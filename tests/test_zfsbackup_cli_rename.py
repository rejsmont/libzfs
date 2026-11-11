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
import threading
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
    fake_daemon,
    fake_zfs,
    generation,
    host_client_id,
    inherit_command,
    inherit_property,
    interleaved,
    isolated,
    live_daemon,
    make_db,
    plant_destination_name,
    run,
    scratch_tmp,
    seed_client_id,
    stderr_lines,
    store_key,
    stored,
    stub,
    unsignalable,
    warnings_in,
    write_yaml,
)
from zfsbackup import daemonlock
from zfsbackup.cli import dotpath, editor, rename
from zfsbackup.cli import write as cli_write
from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    ConfigPathError,
    SchemaError,
    dispose_all,
    read_config,
    resolve_config_path,
    write_config,
    writer_lock,
    writer_lock_path,
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
def rich(run, db, fake_zfs):
    made = make_db(run, db, RENAME_YAML)
    # RENAME_YAML has a recursive dataset, so the import's backup-copy check (A2d) looked at the fake
    # (`zfs list tank/data`): the tests below count the calls the RENAME makes.
    fake_zfs.clear_calls()
    return made


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
        assert f"zfs inherit -S {ANCHOR}offsite tank/vm.01" in out and f"zfs inherit {CONFIG_PROP} tank/vm.01" in out
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
        # the server-side path before and after: the client's pool is part of it
        assert "<server target>/<client-id>/tank/data" in err
        assert "<server target>/<client-id>/pool2/other" in err
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

    def test_a_pool_only_rename_changes_the_server_path_and_says_a_full_send_is_likely(self, run, rich):
        """The pool is part of the server path (`<target>/<id>/<pool>/...`), so moving a dataset to
        another pool is a new server dataset like any other rename."""
        result = run("rename", "dataset", "tank/data", "pool2/data", "-c", rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "<server target>/<client-id>/tank/data" in err
        assert "<server target>/<client-id>/pool2/data" in err
        assert "likely a full send unless the server already has the dataset under the new path" in err
        assert "orphaned" in err
        assert "--force" in err and "Nothing was changed" in err
        # the retired "same path" branch
        assert "does not change" not in err and "incremental sends can continue" not in err
        assert "only the pool part" not in err

    def test_a_zfs_renamed_pool_only_rename_still_needs_force_and_reports_the_new_path(
        self, run, rich, fake_zfs
    ):
        fake_zfs.add("pool2/data", {ANCHOR + "offsite": "snap1"})
        result = run("rename", "dataset", "tank/data", "pool2/data", "-c", rich)
        assert result.exit_code == 1
        assert "--force" in result.stderr
        assert "<server target>/<client-id>/pool2/data" in result.stderr
        assert "likely a full send" in result.stderr
        assert "the anchor properties came along with the dataset" not in result.stderr
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


class TestRenameServerNoteClientId:
    """The server-side path in the `--force` notes uses this host's client id from
    the config database (`ConfigSnapshot.client_id`, read in the same snapshot as
    the config), or the placeholder `<client-id>` when none exists yet. Nothing
    reads a file any more (`client_id_file` was retired)."""

    CID = "backup-host-1a2b3c4d"

    def test_without_a_stored_id_the_placeholder_is_used(self, run, rich):
        assert host_client_id(rich) is None
        result = run("rename", "dataset", "tank/data", "pool2/other", "-c", rich)
        assert result.exit_code == 1
        assert "<server target>/<client-id>/tank/data" in result.stderr
        assert "<server target>/<client-id>/pool2/other" in result.stderr

    def test_a_stored_id_replaces_the_placeholder_in_the_refusal(self, run, rich):
        seed_client_id(rich, self.CID)
        state = db_state(rich)
        result = run("rename", "dataset", "tank/data", "pool2/other", "-c", rich)
        assert result.exit_code == 1
        assert f"<server target>/{self.CID}/tank/data" in result.stderr
        assert f"<server target>/{self.CID}/pool2/other" in result.stderr
        assert "<client-id>" not in result.stderr
        assert db_state(rich) == state

    def test_a_stored_id_is_used_in_the_force_warning(self, run, rich):
        seed_client_id(rich, self.CID)
        result = run("rename", "dataset", "tank/data", "pool2/other", "--force", "-c", rich)
        assert result.exit_code == 0, result.stderr
        (warned,) = [w for w in warnings_in(result) if w.startswith("warning: --force:")]
        assert f"<server target>/{self.CID}/tank/data" in warned
        assert f"<server target>/{self.CID}/pool2/other" in warned
        assert "<client-id>" not in warned

    def test_a_pool_only_rename_uses_the_stored_id(self, run, rich):
        seed_client_id(rich, self.CID)
        result = run("rename", "dataset", "tank/data", "pool2/data", "-c", rich)
        assert result.exit_code == 1
        assert f"<server target>/{self.CID}/tank/data" in result.stderr
        assert f"<server target>/{self.CID}/pool2/data" in result.stderr
        assert "does not change" not in result.stderr and "<client-id>" not in result.stderr

    def test_a_dataset_that_sends_nowhere_needs_no_note(self, run, rich):
        seed_client_id(rich, self.CID)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert "<server target>" not in result.stderr + result.stdout

    def test_the_stored_id_survives_the_rename_write(self, run, rich):
        seed_client_id(rich, self.CID)
        gen = generation(rich)
        assert run("rename", "dataset", "tank/data", "pool2/other", "--force", "-c", rich).exit_code == 0
        assert generation(rich) == gen + 1
        assert host_client_id(rich) == self.CID
        assert "pool2/other" in names(rich)

    def test_a_destination_rename_does_not_touch_the_stored_id(self, run, rich):
        seed_client_id(rich, self.CID)
        result = run("rename", "destination", "offsite", "offsite2", "--skip-missing", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert "offsite2" in stored(rich).destinations
        assert host_client_id(rich) == self.CID

    def test_a_destination_rename_carries_the_pin_along(self, run, db):
        fp = "ab" * 32
        make_db(run, db, RENAME_YAML.replace(
            '    url: "https://offsite:8080"\n',
            f'    url: "https://offsite:8080"\n    server_fingerprint: "{fp}"\n',
        ))
        result = run("rename", "destination", "offsite", "offsite2", "--skip-missing", "-c", db)
        assert result.exit_code == 0, result.stderr
        config = stored(db)
        assert "offsite" not in config.destinations
        assert config.destinations["offsite2"].server_fingerprint == fp
        assert [w for w in warnings_in(result) if "server_fingerprint" in w] == []

    @pytest.mark.parametrize("stored_id, expected", [(CID, CID), (None, "<client-id>"), ("", "<client-id>")])
    def test_client_id_prefers_the_snapshot_and_falls_back_to_the_placeholder(self, stored_id, expected):
        from types import SimpleNamespace

        assert rename._client_id(SimpleNamespace(client_id=stored_id)) == expected

    def test_client_id_reads_no_file(self, monkeypatch):
        from pathlib import Path
        from types import SimpleNamespace

        def boom(*args, **kwargs):
            raise AssertionError("rename must not read a client id file")

        monkeypatch.setattr(Path, "read_text", boom)
        monkeypatch.setattr(Path, "open", boom)
        assert rename._client_id(SimpleNamespace(client_id=None)) == "<client-id>"

    def test_server_note_takes_the_snapshot_not_the_config(self):
        from types import SimpleNamespace

        snapshot = SimpleNamespace(client_id=self.CID)
        moved = rename._server_note(snapshot, "tank/data", "pool2/other")
        assert f"<server target>/{self.CID}/tank/data" in moved
        assert f"<server target>/{self.CID}/pool2/other" in moved
        pooled = rename._server_note(snapshot, "tank/data", "pool2/data")      # the pool alone differs
        assert f"<server target>/{self.CID}/tank/data" in pooled
        assert f"<server target>/{self.CID}/pool2/data" in pooled
        assert "orphaned" in pooled and "does not change" not in pooled
        bare = rename._server_note(SimpleNamespace(client_id=None), "tank/data", "pool2/other")
        assert "<server target>/<client-id>/tank/data" in bare


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
        assert [inherit_property(c) for c in self.inherits(stale)] == self.ORDER

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
            assert inherit_command(prop, "tank/vm.01") in err
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
        real_gate = rename._clear_gate

        def spying(resolved, *args, **kwargs):
            order.append("daemon check")
            return real_gate(resolved, *args, **kwargs)

        monkeypatch.setattr(rename, "_clear_gate", spying)
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
        assert all(inherit_command(p, "tank/vm.01") in err for p in self.ORDER)
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
        assert "a daemon is running" in result.stdout
        assert (
            "after the write the clear happens only if the daemon is verified and applies the "
            "rename within --reload-wait, else it is skipped and the zfs inherit commands "
            "printed instead"
        ) in result.stdout
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
        assert f"zfs inherit -S {ANCHOR}offsite tank/vm.01" in err and f"zfs inherit {CONFIG_PROP} tank/vm.01" in err
        assert f"zfs inherit -S {ANCHOR}dc.two" not in err
        assert "tank/vm.02" in names(rich)
        assert stale.props("tank/vm.01") == {
            CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1", "compression": "lz4",
        }
        # it stopped at the first failure
        assert [inherit_property(c) for c in self.inherits(stale)] == [ANCHOR + "dc.two", ANCHOR + "offsite"]

    def test_a_failure_on_the_first_property_says_none_were_cleared(self, run, rich, stale):
        stale.fail("inherit", "tank/vm.01", ANCHOR + "dc.two")
        err = self.clear(run, rich).stderr
        assert "Cleared so far: none" in err
        assert all(inherit_command(p, "tank/vm.01") in err for p in self.ORDER)

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


@pytest.fixture
def slept(monkeypatch):
    """Every `time.sleep` the CLI's ack loop makes (still really slept). Tests of "it did not
    sit out `--reload-wait`" assert on the SUM of these rather than on wall-clock time, which
    flaked under load (imports, SQLite and the fake `zfs` count against a clock)."""
    seen = []
    real = cli_write.time.sleep
    monkeypatch.setattr(cli_write.time, "sleep", lambda s: (seen.append(s), real(s))[1])
    return seen


class TestRenameDatasetClearWithAVerifiedDaemon:
    """`--clear-stale-props` while a daemon runs: the properties are cleared only
    if `request_reload` VERIFIED the daemon (`signalled` -- and nothing else:
    `already-applied` is decided before its proofs, so it refuses like
    `unverified`) and it reports the rename's `(store id, generation)` as
    `applied` within `--reload-wait`; then, under the writer lock, nothing may
    have been written since and OLD must not be configured again. Everything
    else keeps today's refusal. Without a daemon (or when it exits during the
    wait) the gate also refuses while daemon workers survive, and always takes
    the writer lock and re-reads the store.

    `request_reload` cannot return `signalled` on macOS (no lock table), so the
    tests replace `rename.begin_reload` with a fake that returns the outcome
    they want and lets the fake daemon "apply" the key as a side effect. The
    daemon is a `FakeDaemon` (pid file + held lock, no process); the zfs is the
    fake one (`fake_zfs`).
    """

    PROPS = TestRenameDatasetClearStale.PROPS
    ORDER = TestRenameDatasetClearStale.ORDER
    WAIT = "0.5"
    REFUSED = "the configuration was renamed, but the stale properties on 'tank/vm.01' were NOT cleared"

    @pytest.fixture
    def stale(self, fake_zfs):
        fake_zfs.add("tank/vm.01", self.PROPS)
        fake_zfs.add("tank/vm.02", {})
        return fake_zfs

    @pytest.fixture
    def daemon(self, rich, db, fake_daemon):
        """A daemon that is on the pre-rename generation."""
        return fake_daemon(db, applied=store_key(db))

    @pytest.fixture
    def requests(self):
        return []

    @pytest.fixture
    def verify(self, monkeypatch, requests):
        """`verify(d, outcome, on_request)`: `rename.begin_reload` answers
        `outcome` for daemon `d`; `on_request(key)` runs first (the daemon
        applying the rename, a concurrent writer, ...). Records each key."""

        def install(d, outcome="signalled", on_request=None):
            def fake(resolved, generation, store_id=None):
                key = (store_id, generation)
                requests.append(key)
                if on_request is not None:
                    on_request(key)
                return daemonlock.ReloadRequest(outcome, pid=d.pid)

            monkeypatch.setattr(rename, "begin_reload", fake)

        return install

    @pytest.fixture
    def answers(self, monkeypatch):
        """Every `_await_answer` call (the wait for the daemon), as `(wait, applied_only)`."""
        seen = []
        real = cli_write._await_answer

        def spy(resolved, key, wait, **kwargs):
            seen.append((wait, kwargs.get("applied_only", False)))
            return real(resolved, key, wait, **kwargs)

        monkeypatch.setattr(cli_write, "_await_answer", spy)
        return seen

    def clear(self, run, rich, *extra, wait=None):
        return run(
            "-c", rich, "--reload-wait", wait or self.WAIT,
            "rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", *extra,
        )

    def inherits(self, fake):
        return [c for c in fake.calls if c[0] == "inherit"]

    def assert_refused(self, result, stale, rich, why, renamed=True):
        assert result.exit_code == 1, (result.stdout, result.stderr)
        err = result.stderr
        assert self.REFUSED in err and why in err
        assert "Once it is stopped, run:" in err
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in err
        assert stale.mutations == [] and stale.props("tank/vm.01") == self.PROPS
        if renamed:
            assert "tank/vm.02" in names(rich)                   # the rename itself stands

    def assert_cleared(self, result, stale, rich):
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert "Cleared on 'tank/vm.01':" in result.stdout
        assert stale.props("tank/vm.01") == {"compression": "lz4"}
        assert [inherit_property(c) for c in self.inherits(stale)] == self.ORDER
        assert "tank/vm.02" in names(rich)

    # -- 1, 2: the daemon applied the rename -----------------------------

    def test_signalled_and_applied_in_time_clears_the_properties(
        self, run, rich, stale, daemon, verify, requests
    ):
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))
        result = self.clear(run, rich)
        self.assert_cleared(result, stale, rich)
        out = result.stdout
        assert out.index("Renamed dataset") < out.index("Cleared on 'tank/vm.01':")
        assert f"reload requested from daemon pid {daemon.pid}: it is now on generation {requests[0][1]}" in out
        assert "not yet acknowledged" not in out

    def test_a_daemon_that_applies_while_we_wait_is_waited_for(
        self, run, rich, stale, daemon, verify, requests
    ):
        """The signal is sent, but the apply lands a little later: the gate polls."""
        timer = []

        def later(key):
            timer.append(threading.Timer(0.25, daemon.set, kwargs={"applied": key}))
            timer[0].start()

        verify(daemon, "signalled", later)
        try:
            result = self.clear(run, rich, wait="5")
        finally:
            timer[0].cancel()
            timer[0].join()
        self.assert_cleared(result, stale, rich)

    def test_a_pre_applied_daemon_through_the_real_request_reload_is_refused(
        self, run, rich, db, stale, live_daemon, unsignalable, answers
    ):
        """No patching: the daemon (a live pid with its real start token) already
        reports the rename's key, so `request_reload` answers `already-applied`
        -- decided before any lock-holder proof, so on a host that cannot verify
        lock holders (and for a forged `applied=` line) it must not clear."""
        sid, gen = store_key(db)
        d = live_daemon(db, applied=(sid, gen + 1))
        result = self.clear(run, rich)
        assert f"is already on generation {gen + 1}" in result.stdout     # it WAS already-applied
        assert result.exit_code == 1
        assert (
            f"{self.REFUSED}: a daemon is running (pid {d.pid}). Once it is stopped, run:\n"
        ) in result.stderr
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in result.stderr
        assert stale.mutations == [] and stale.props("tank/vm.01") == self.PROPS
        assert "tank/vm.02" in names(rich)
        assert answers == []                      # and it did not wait for anything

    def test_a_daemon_on_a_later_stored_generation_is_enough(
        self, run, rich, db, stale, daemon, verify
    ):
        """Someone wrote again right after the rename and the daemon is on THAT
        generation (`newer`): it has the rename in it, and the stored key equals
        its applied key, so nothing newer waits to be loaded."""

        def write_again_and_apply(key):
            current = read_config(resolve_config_path(str(db)))
            current.config.api_port = 9999
            write_config(
                resolve_config_path(str(db)), current.config, expected_generation=current.generation
            )
            dispose_all()
            daemon.set(applied=store_key(db))

        verify(daemon, "signalled", write_again_and_apply)
        self.assert_cleared(self.clear(run, rich), stale, rich)

    def test_a_daemon_claiming_an_unstored_generation_is_refused(
        self, run, rich, db, stale, daemon, verify
    ):
        """`applied` is later than the rename but is not what is stored: the
        stored-key comparison, not the wait's answer, decides."""
        seen = []

        def claim(key):
            seen.append(key)
            daemon.set(applied=(key[0], key[1] + 5))

        verify(daemon, "signalled", claim)
        result = self.clear(run, rich)
        gen = seen[0][1]
        self.assert_refused(
            result, stale, rich,
            f"it reports generation {gen + 5} as applied, which is not the stored generation {gen}",
        )
        assert "changed again" not in result.stderr

    # -- 3, 4: not applied ---------------------------------------------------

    @pytest.mark.parametrize("state", ["nothing", "accepted"])
    def test_not_applied_in_time_refuses_with_the_inherit_commands(
        self, run, rich, stale, daemon, verify, answers, state
    ):
        def on_request(key):
            if state == "accepted":
                daemon.set(target=key)             # accepted, still switching over

        verify(daemon, "signalled", on_request)
        started = time.monotonic()
        result = self.clear(run, rich)
        elapsed = time.monotonic() - started
        self.assert_refused(
            result, stale, rich,
            f"a daemon is running (pid {daemon.pid}); it has not applied the rename within --reload-wait",
        )
        assert elapsed >= 0.45                      # it waited the whole --reload-wait
        assert "config updated" in result.stdout    # the daemon notice is still printed

    def test_the_wait_is_not_repeated_by_the_notice(
        self, run, rich, stale, daemon, verify, answers
    ):
        """One wait only: the gate's (applied-only, `--reload-wait`); the notice
        that follows a failed gate does not wait for the same answer again."""
        verify(daemon, "signalled")
        result = self.clear(run, rich)
        assert result.exit_code == 1
        assert answers == [(0.5, True)]          # one wait, however long the subprocesses take
        assert "not yet acknowledged" not in result.stdout
        assert "reload requested from daemon" in result.stdout

    def test_a_cleared_rename_waits_only_once_too(self, run, rich, stale, daemon, verify, answers):
        """After a successful gate the notice reads the answer it already
        waits for (instantly: the daemon is on the key)."""
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))
        assert self.clear(run, rich).exit_code == 0
        assert answers[0] == (0.5, True)
        assert all(applied_only is False for _w, applied_only in answers[1:])

    DEFERRED_REASON = "the dataset 'tank/vm.02' does not exist"

    @pytest.mark.parametrize("reason", [DEFERRED_REASON, None], ids=["reason", "no-reason"])
    def test_a_deferred_rename_at_the_deadline_is_refused_with_the_reason(
        self, run, rich, stale, daemon, verify, answers, reason
    ):
        verify(
            daemon, "signalled",
            lambda key: daemon.set(deferred=key, deferred_reason=reason),
        )
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich,
            f"a daemon is running (pid {daemon.pid}); it has not applied the rename within "
            f"--reload-wait (it deferred it: {reason or 'see the daemon log'}; it re-checks "
            "every poll, 10 s)",
        )
        assert answers == [(0.5, True)]               # waited once; the notice does not wait again
        assert "not yet acknowledged" not in result.stdout
        assert "it rejected" not in result.stderr

    def test_a_deferred_rename_that_is_then_applied_clears(
        self, run, rich, stale, daemon, verify, answers
    ):
        timer = []

        def deferred_then_applied(key):
            daemon.set(deferred=key, deferred_reason=self.DEFERRED_REASON)
            timer.append(threading.Timer(0.3, daemon.set, kwargs={"applied": key, "deferred": None}))
            timer[0].start()

        verify(daemon, "signalled", deferred_then_applied)
        try:
            result = self.clear(run, rich, wait="5")
        finally:
            timer[0].cancel()
            timer[0].join()
        self.assert_cleared(result, stale, rich)

    def test_a_deferred_rename_that_is_then_rejected_is_refused_as_rejected(
        self, run, rich, stale, daemon, verify
    ):
        timer = []

        def deferred_then_rejected(key):
            daemon.set(deferred=key, deferred_reason=self.DEFERRED_REASON)
            timer.append(threading.Timer(0.3, daemon.set, kwargs={"rejected": key, "deferred": None}))
            timer[0].start()

        verify(daemon, "signalled", deferred_then_rejected)
        try:
            started = time.monotonic()
            result = self.clear(run, rich, wait="10")
        finally:
            timer[0].cancel()
            timer[0].join()
        assert time.monotonic() - started < 6        # rejection ended the wait
        self.assert_refused(
            result, stale, rich, "it rejected the new configuration and keeps running the old one"
        )

    @pytest.mark.parametrize(
        "other",
        [lambda key: (key[0], key[1] - 1), lambda key: (key[0] + 1, key[1] + 1)],
        ids=["older", "other-store"],
    )
    def test_a_deferral_of_an_unrelated_key_is_just_not_applied(
        self, run, rich, stale, daemon, verify, other
    ):
        verify(daemon, "signalled", lambda key: daemon.set(deferred=other(key)))
        result = self.clear(run, rich)
        self.assert_refused(result, stale, rich, "it has not applied the rename within --reload-wait")
        assert "deferred it" not in result.stderr and "later configuration" not in result.stderr

    @pytest.mark.parametrize("reason", [DEFERRED_REASON, None], ids=["reason", "no-reason"])
    def test_a_later_deferred_generation_is_refused_with_its_number_and_reason(
        self, run, rich, stale, daemon, verify, answers, reason
    ):
        seen = []

        def later_deferred(key):
            seen.append(key)
            daemon.set(deferred=(key[0], key[1] + 1), deferred_reason=reason)

        verify(daemon, "signalled", later_deferred)
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich,
            f"a daemon is running (pid {daemon.pid}); it has not applied a later configuration "
            f"yet (generation {seen[0][1] + 1} is deferred: {reason or 'see the daemon log'}; "
            "it re-checks every poll, 10 s)",
        )
        assert answers == [(0.5, True)]                 # waited once, not twice
        assert "it rejected" not in result.stderr

    def test_a_later_deferred_generation_that_resolves_to_applied_clears(
        self, run, rich, stale, daemon, verify
    ):
        timer = []

        def later_then_applied(key):
            daemon.set(deferred=(key[0], key[1] + 1), deferred_reason=self.DEFERRED_REASON)
            timer.append(threading.Timer(0.3, daemon.set, kwargs={"applied": key, "deferred": None}))
            timer[0].start()

        verify(daemon, "signalled", later_then_applied)
        try:
            result = self.clear(run, rich, wait="5")
        finally:
            timer[0].cancel()
            timer[0].join()
        self.assert_cleared(result, stale, rich)

    def test_a_rejected_rename_is_refused_at_once(
        self, run, rich, stale, daemon, verify, answers, slept
    ):
        """Hardened (a wall-clock `< 2.5 s` bound on the whole in-process run flaked under load:
        imports, SQLite and the fake `zfs` processes all count against it). What must not
        happen is SITTING OUT the 5 s `--reload-wait`, so the time the ack loop actually
        sleeps is measured instead -- it is zero (or one poll) however slow the machine."""
        verify(daemon, "signalled", lambda key: daemon.set(rejected=key))
        result = self.clear(run, rich, wait="5")
        assert sum(slept) < 1.0, slept              # did not sit out the 5 s
        self.assert_refused(
            result, stale, rich,
            "it rejected the new configuration and keeps running the old one",
        )
        assert "rejected generation" in result.stderr or "rejected" in result.stderr

    def test_a_later_rejected_generation_is_refused_as_superseded(
        self, run, rich, stale, daemon, verify
    ):
        verify(daemon, "signalled", lambda key: daemon.set(rejected=(key[0], key[1] + 1)))
        result = self.clear(run, rich, wait="5")
        self.assert_refused(
            result, stale, rich, "it rejected a later configuration and keeps running the old one"
        )

    def test_a_newer_target_alone_is_not_applied(self, run, rich, stale, daemon, verify):
        verify(daemon, "signalled", lambda key: daemon.set(target=(key[0], key[1] + 1)))
        result = self.clear(run, rich)
        self.assert_refused(result, stale, rich, "it has not applied the rename within --reload-wait")

    def test_an_unreadable_pid_file_while_waiting_is_refused(
        self, run, rich, stale, daemon, verify, monkeypatch
    ):
        verify(daemon, "signalled")
        real = daemonlock.probe
        calls = []

        def flaky(resolved):
            calls.append(1)
            if len(calls) >= 2:                      # the first look is the gate's own
                raise OSError(errno.EIO, "Input/output error")
            return real(resolved)

        monkeypatch.setattr(daemonlock, "probe", flaky)
        result = self.clear(run, rich)
        self.assert_refused(result, stale, rich, "its state file could not be read while waiting")

    # -- 5: not verified -----------------------------------------------------

    def test_an_unverified_daemon_keeps_the_plain_refusal(
        self, run, rich, stale, daemon_lock_holder, unsignalable, answers
    ):
        holder = daemon_lock_holder(rich, generation=0)
        result = self.clear(run, rich)
        assert result.exit_code == 1
        assert result.stderr.count(self.REFUSED) == 1
        assert (
            f"{self.REFUSED}: a daemon is running (pid {holder.pid}). Once it is stopped, run:\n"
        ) in result.stderr
        assert stale.mutations == [] and answers == []   # and it did not wait

    @pytest.mark.parametrize(
        "outcome",
        ["already-applied", "unverified", "not-permitted", "supervisor-gone", "legacy", "error"],
    )
    def test_no_other_outcome_trusts_the_pid_file_even_when_it_says_applied(
        self, run, rich, stale, daemon, verify, answers, outcome
    ):
        """The daemon's own file says `applied` -- but nothing proves the file
        belongs to the daemon, so only a verified outcome can clear."""
        verify(daemon, outcome, lambda key: daemon.set(applied=key))
        result = self.clear(run, rich)
        assert result.exit_code == 1
        assert (
            f"{self.REFUSED}: a daemon is running (pid {daemon.pid}). Once it is stopped, run:\n"
        ) in result.stderr
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in result.stderr
        assert stale.mutations == [] and stale.props("tank/vm.01") == self.PROPS
        assert "tank/vm.02" in names(rich)
        assert [w for w in answers if w[1]] == []         # never waited for `applied`

    # -- 6, 7: the world moved on after the daemon answered ----------------

    def test_a_write_after_the_daemon_applied_is_refused(
        self, run, rich, db, stale, daemon, verify, monkeypatch
    ):
        """The daemon applied the rename; a moment later someone wrote again, so
        the daemon has not loaded the newest configuration: not safe to clear."""
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))
        real = rename.await_applied

        def then_a_write(resolved, key, wait=None):
            answer = real(resolved, key, wait)
            current = read_config(resolve_config_path(str(db)))
            current.config.api_port = 9999
            write_config(
                resolve_config_path(str(db)), current.config, expected_generation=current.generation
            )
            dispose_all()
            return answer

        monkeypatch.setattr(rename, "await_applied", then_a_write)
        result = self.clear(run, rich)
        self.assert_refused(result, stale, rich, "the configuration was changed again since the rename")
        assert stored(db).api_port == 9999

    def test_a_dataset_configured_again_is_refused_even_if_the_daemon_has_it(
        self, run, rich, db, stale, daemon, verify, monkeypatch
    ):
        """A concurrent rename back, which the daemon already applied too: the
        old dataset's properties are live again."""
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))
        real = rename.await_applied

        def then_a_rename_back(resolved, key, wait=None):
            answer = real(resolved, key, wait)
            current = read_config(resolve_config_path(str(db)))
            for entry in current.config.datasets:
                if entry.name == "tank/vm.02":
                    entry.name = "tank/vm.01"
            write_config(
                resolve_config_path(str(db)), current.config, expected_generation=current.generation
            )
            dispose_all()
            daemon.set(applied=store_key(db))
            return answer

        monkeypatch.setattr(rename, "await_applied", then_a_rename_back)
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich, "'tank/vm.01' is a configured dataset again", renamed=False
        )
        assert "tank/vm.02" not in names(rich) and "tank/vm.01" in names(rich)

    def test_the_stored_key_is_read_under_the_writer_lock(
        self, run, rich, db, stale, daemon, verify, monkeypatch
    ):
        lock_path = writer_lock_path(resolve_config_path(str(db)))
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))
        seen = []
        real = rename.read_snapshot

        def spying(resolved):
            seen.append(_try_exclusive_lock(lock_path))
            return real(resolved)

        monkeypatch.setattr(rename, "read_snapshot", spying)
        assert self.clear(run, rich).exit_code == 0
        assert seen == ["blocked"]

    # -- 8, 9: the writer lock -----------------------------------------------

    def test_a_busy_writer_lock_is_exit_75_with_the_inherit_commands(
        self, run, rich, stale, daemon, verify, monkeypatch
    ):
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))

        def busy(*args, **kwargs):
            raise ConfigBusy("the config writer lock is busy")

        monkeypatch.setattr(rename, "writer_lock", busy)
        result = self.clear(run, rich)
        assert result.exit_code == 75, (result.stdout, result.stderr)
        err = result.stderr
        assert "the configuration was renamed, but the stale properties on 'tank/vm.01' were NOT cleared" in err
        assert "the writer lock was not free" in err
        assert "Re-running the rename cannot help" in err and "run this again" not in err
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in err
        assert stale.mutations == [] and "tank/vm.02" in names(rich)

    def test_a_real_holder_of_the_writer_lock_is_exit_75(
        self, run, rich, db, stale, daemon, verify, monkeypatch
    ):
        """Another writer (a thread here: the lock is per open file) holds the
        lock between the rename and the gate; the gate gives up after its own
        short timeout instead of waiting 10 s."""
        resolved = resolve_config_path(str(db))
        held, release, done = threading.Event(), threading.Event(), threading.Event()

        def holder():
            with writer_lock(resolved):
                held.set()
                release.wait(30)
            done.set()

        thread = threading.Thread(target=holder, daemon=True)

        def on_request(key):
            daemon.set(applied=key)
            thread.start()
            assert held.wait(10)

        verify(daemon, "signalled", on_request)
        real = rename.writer_lock
        monkeypatch.setattr(rename, "writer_lock", lambda r, **k: real(r, timeout=0.2))
        try:
            result = self.clear(run, rich)
        finally:
            release.set()
            thread.join(10)
        assert result.exit_code == 75, (result.stdout, result.stderr)
        assert "the writer lock was not free" in result.stderr
        assert f"zfs inherit -S {ANCHOR}offsite tank/vm.01" in result.stderr
        assert stale.mutations == [] and "tank/vm.02" in names(rich)
        assert done.is_set()

    def test_a_second_writer_gets_config_busy_while_the_properties_are_cleared(
        self, run, rich, db, stale, daemon, verify, monkeypatch
    ):
        import zfsbackup.backup_manager as bm

        resolved = resolve_config_path(str(db))
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))
        outcomes = []
        real = bm.clear_zfsbackup_properties

        def probing(*args, **kwargs):
            def second_writer():
                try:
                    with writer_lock(resolved, timeout=0.05):
                        outcomes.append("acquired")
                except ConfigBusy:
                    outcomes.append("busy")

            thread = threading.Thread(target=second_writer)
            thread.start()
            thread.join(10)
            return real(*args, **kwargs)

        monkeypatch.setattr(bm, "clear_zfsbackup_properties", probing)
        result = self.clear(run, rich)
        self.assert_cleared(result, stale, rich)
        assert outcomes == ["busy"] * 3
        assert _try_exclusive_lock(writer_lock_path(resolved)) == "acquired"   # released after

    def test_the_writer_lock_is_released_after_a_failed_clear(
        self, run, rich, db, stale, daemon, verify
    ):
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))
        stale.fail("inherit", "tank/vm.01", ANCHOR + "offsite")
        result = self.clear(run, rich)
        assert result.exit_code == 1 and "Cleared so far" in result.stderr
        assert _try_exclusive_lock(writer_lock_path(resolve_config_path(str(db)))) == "acquired"

    def test_the_no_daemon_clear_holds_the_writer_lock_throughout(self, run, rich, db, stale, monkeypatch):
        """No daemon is the same gate as a gone one: the writer lock is taken
        (so a concurrent rename back cannot slip in) and held through every clear."""
        import zfsbackup.backup_manager as bm

        lock_path = writer_lock_path(resolve_config_path(str(db)))
        states = []
        real = bm.clear_zfsbackup_properties

        def probing(*args, **kwargs):
            states.append(_try_exclusive_lock(lock_path))
            return real(*args, **kwargs)

        monkeypatch.setattr(bm, "clear_zfsbackup_properties", probing)
        self.assert_cleared(self.clear(run, rich), stale, rich)
        assert states == ["blocked"] * 3
        assert _try_exclusive_lock(lock_path) == "acquired"

    # -- 10: the daemon goes away ------------------------------------------

    def test_a_daemon_that_exits_while_we_wait_is_the_no_daemon_case(
        self, run, rich, stale, daemon, verify, monkeypatch, slept
    ):
        verify(daemon, "signalled")                # never applies: it exits instead
        real = daemonlock.probe
        calls = []

        def exiting(resolved):
            calls.append(1)
            if len(calls) == 3:                    # gate, first wait look, second wait look
                daemon.close()
            return real(resolved)

        monkeypatch.setattr(daemonlock, "probe", exiting)
        result = self.clear(run, rich, wait="5")
        assert sum(slept) < 1.0, slept             # it did not sit out the 5 s
        self.assert_cleared(result, stale, rich)
        assert "the daemon exited before acknowledging" in result.stdout

    def test_a_daemon_that_exits_and_is_replaced_is_still_a_daemon(
        self, run, rich, db, stale, daemon, verify, monkeypatch, fake_daemon
    ):
        verify(daemon, "signalled")
        real = daemonlock.probe
        calls = []
        replacement = []

        def exiting(resolved):
            calls.append(1)
            if len(calls) == 3:                    # the wait sees nobody: "gone"
                daemon.close()
            elif len(calls) == 4:                  # ...and a new one starts before the re-check
                replacement.append(fake_daemon(db, pid=5678, applied=(store_key(db)[0], 0)))
            return real(resolved)

        monkeypatch.setattr(daemonlock, "probe", exiting)
        result = self.clear(run, rich, wait="5")
        self.assert_refused(result, stale, rich, "a daemon is running (pid 5678)")

    # -- the daemon is gone: workers, writer lock, the store re-read ------------

    def exit_during_the_wait(self, daemon, monkeypatch, *, also=None):
        """The daemon's lock disappears on the third probe (the wait's second
        look); `also()` runs at that moment (a concurrent writer, ...)."""
        real = daemonlock.probe
        calls = []

        def exiting(resolved):
            calls.append(1)
            if len(calls) == 3:
                daemon.close()
                if also is not None:
                    also()
            return real(resolved)

        monkeypatch.setattr(daemonlock, "probe", exiting)

    def rename_back(self, db):
        current = read_config(resolve_config_path(str(db)))
        for entry in current.config.datasets:
            if entry.name == "tank/vm.02":
                entry.name = "tank/vm.01"
        write_config(
            resolve_config_path(str(db)), current.config, expected_generation=current.generation
        )
        dispose_all()

    def test_gone_with_surviving_workers_is_refused(
        self, run, rich, stale, daemon, verify, monkeypatch
    ):
        verify(daemon, "signalled")
        self.exit_during_the_wait(daemon, monkeypatch)
        monkeypatch.setattr(daemonlock, "workers_present", lambda resolved: True)
        result = self.clear(run, rich, wait="5")
        self.assert_refused(result, stale, rich, "NOT cleared: daemon workers are still running")

    def test_gone_with_an_unexaminable_workers_lock_is_refused(
        self, run, rich, stale, daemon, verify, monkeypatch
    ):
        verify(daemon, "signalled")
        self.exit_during_the_wait(daemon, monkeypatch)

        def broken(resolved):
            raise daemonlock.DaemonLockError(errno.EACCES, "denied", "workers.lock")

        monkeypatch.setattr(daemonlock, "workers_present", broken)
        result = self.clear(run, rich, wait="5")
        self.assert_refused(
            result, stale, rich, "NOT cleared: cannot tell whether daemon workers are still running"
        )

    def test_gone_and_the_old_dataset_configured_again_is_refused(
        self, run, rich, db, stale, daemon, verify, monkeypatch
    ):
        verify(daemon, "signalled")
        self.exit_during_the_wait(daemon, monkeypatch, also=lambda: self.rename_back(db))
        result = self.clear(run, rich, wait="5")
        self.assert_refused(
            result, stale, rich, "'tank/vm.01' is a configured dataset again", renamed=False
        )
        assert "tank/vm.01" in names(rich) and "tank/vm.02" not in names(rich)

    def test_gone_and_clean_clears_with_the_writer_lock_held(
        self, run, rich, db, stale, daemon, verify, monkeypatch
    ):
        import zfsbackup.backup_manager as bm

        verify(daemon, "signalled")
        self.exit_during_the_wait(daemon, monkeypatch)
        lock_path = writer_lock_path(resolve_config_path(str(db)))
        states = []
        real = bm.clear_zfsbackup_properties

        def probing(*args, **kwargs):
            states.append(_try_exclusive_lock(lock_path))
            return real(*args, **kwargs)

        monkeypatch.setattr(bm, "clear_zfsbackup_properties", probing)
        self.assert_cleared(self.clear(run, rich, wait="5"), stale, rich)
        assert states == ["blocked"] * 3

    # -- no daemon at all: the same gate ----------------------------------------

    def test_orphaned_workers_without_a_daemon_are_refused(self, run, rich, db, stale):
        """A killed supervisor's spawn/forkserver workers hold the workers lock
        (shared) but not the presence lock: a real `LOCK_SH`, a real probe."""
        path = daemonlock.workers_lock_path(resolve_config_path(str(db)))
        fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o660)
        try:
            fcntl.flock(fd, fcntl.LOCK_SH)
            result = self.clear(run, rich)
        finally:
            os.close(fd)
        self.assert_refused(result, stale, rich, "NOT cleared: daemon workers are still running")
        assert "Once it is stopped, run:" in result.stderr

    def test_no_workers_lock_file_is_no_workers(self, run, rich, db, stale):
        assert not daemonlock.workers_lock_path(resolve_config_path(str(db))).exists()
        self.assert_cleared(self.clear(run, rich), stale, rich)

    def test_an_unexaminable_workers_lock_without_a_daemon_is_refused(
        self, run, rich, stale, monkeypatch
    ):
        def broken(resolved):
            raise daemonlock.DaemonLockError(errno.EACCES, "denied", "workers.lock")

        monkeypatch.setattr(daemonlock, "workers_present", broken)
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich, "NOT cleared: cannot tell whether daemon workers are still running"
        )

    def test_a_plain_oserror_from_the_workers_lock_is_refused_too(self, run, rich, stale, monkeypatch):
        def broken(resolved):
            raise OSError(errno.EIO, "Input/output error")

        monkeypatch.setattr(daemonlock, "workers_present", broken)
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich,
            "cannot tell whether daemon workers are still running (Input/output error)",
        )

    def test_no_daemon_and_the_old_dataset_configured_again_is_refused(
        self, run, rich, db, stale, monkeypatch
    ):
        """A rename back committed between our write and the gate."""

        def back(resolved, generation, store_id=None):
            self.rename_back(db)
            return daemonlock.ReloadRequest("no-daemon")

        monkeypatch.setattr(rename, "begin_reload", back)
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich, "NOT cleared: 'tank/vm.01' is a configured dataset again",
            renamed=False,
        )

    def test_no_daemon_and_a_busy_writer_lock_is_exit_75_with_the_note(
        self, run, rich, stale, monkeypatch
    ):
        def busy(*args, **kwargs):
            raise ConfigBusy("the config writer lock is busy")

        monkeypatch.setattr(rename, "writer_lock", busy)
        result = self.clear(run, rich)
        assert result.exit_code == 75, (result.stdout, result.stderr)
        err = result.stderr
        assert "the writer lock was not free" in err
        assert "Re-running the rename cannot help" in err
        assert "run this again" not in err
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in err
        assert stale.mutations == [] and "tank/vm.02" in names(rich)

    # -- errors from the gate itself ---------------------------------------------

    @pytest.mark.parametrize(
        "error, code",
        [
            (OSError(errno.EIO, "Input/output error"), 1),
            (ConfigPathError("the config database is gone"), 78),
            (SchemaError("the config schema is out of date"), 78),
        ],
        ids=["oserror", "config-path", "schema"],
    )
    def test_a_store_error_in_the_gate_is_not_reported_as_a_daemon_probe(
        self, run, rich, stale, monkeypatch, error, code
    ):
        def unreadable(resolved):
            raise error

        monkeypatch.setattr(rename, "read_snapshot", unreadable)
        result = self.clear(run, rich)
        assert result.exit_code == code, (result.stdout, result.stderr)
        err = result.stderr
        assert "cannot tell whether a daemon is running" not in err
        assert "the configuration was renamed, but clearing the stale properties on 'tank/vm.01' failed" in err
        assert "Cleared so far: none" in err
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in err
        assert stale.mutations == [] and "tank/vm.02" in names(rich)

    def test_a_probe_error_after_the_writer_lock_is_still_cannot_tell(
        self, run, rich, stale, monkeypatch
    ):
        real = daemonlock.probe
        calls = []

        def second_fails(resolved):
            calls.append(1)
            if len(calls) == 2:                  # the gate's re-probe under the lock
                raise OSError(errno.EIO, "Input/output error")
            return real(resolved)

        monkeypatch.setattr(daemonlock, "probe", second_fails)
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich,
            "NOT cleared: cannot tell whether a daemon is running (Input/output error)",
        )
        assert len(calls) == 2

    # -- the fresh probe under the writer lock: per-case wording --------------

    def start_a_daemon_after_the_store_is_read(self, monkeypatch, db, fake_daemon, **state):
        """No daemon at the first look; one takes the presence lock after the
        gate has taken the writer lock and read the store."""
        real = rename.read_snapshot
        started = []

        def reading(resolved):
            snap = real(resolved)
            started.append(fake_daemon(db, pid=5678, **state))
            return snap

        monkeypatch.setattr(rename, "read_snapshot", reading)
        return started

    def test_a_daemon_that_started_without_publishing_applied_is_refused(
        self, run, rich, db, stale, fake_daemon, monkeypatch
    ):
        self.start_a_daemon_after_the_store_is_read(monkeypatch, db, fake_daemon, applied=None)
        gen = store_key(db)[1] + 1                 # the rename's generation
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich,
            "NOT cleared: a daemon has started; it has not published which configuration it "
            f"applied yet (the stored one is generation {gen})",
        )

    def test_a_daemon_that_started_on_another_store_is_refused(
        self, run, rich, db, stale, fake_daemon, monkeypatch
    ):
        sid, gen = store_key(db)
        gen += 1                                   # the rename's generation
        self.start_a_daemon_after_the_store_is_read(
            monkeypatch, db, fake_daemon, applied=(sid + 1000, 3)
        )
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich,
            f"a daemon has started; the stored configuration (store {sid}, generation {gen}) is "
            f"not the one it applied (store {sid + 1000}, generation 3)",
        )

    def test_an_unstable_state_file_under_the_lock_is_refused(
        self, run, rich, stale, monkeypatch
    ):
        real = daemonlock.probe
        calls = []

        def torn(resolved):
            calls.append(1)
            if len(calls) == 2:                  # the gate's probe under the writer lock
                return daemonlock.DaemonInfo(
                    pid=5678, generation=None, start="t", applied=(1, 1), stable=False
                )
            return real(resolved)

        monkeypatch.setattr(daemonlock, "probe", torn)
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich,
            "NOT cleared: a daemon has started; its state file could not be read consistently, "
            "so what it applied is unknown",
        )

    def test_a_verified_daemon_that_goes_unstable_keeps_its_lead(
        self, run, rich, stale, daemon, verify, monkeypatch
    ):
        verify(daemon, "signalled", lambda key: daemon.set(applied=key))
        real = daemonlock.probe

        def torn_after_the_wait(resolved):
            info = real(resolved)
            if torn_after_the_wait.armed and info is not None:
                return daemonlock.DaemonInfo(
                    pid=info.pid, generation=None, start="t", applied=info.applied, stable=False
                )
            return info

        torn_after_the_wait.armed = False
        real_read = rename.read_snapshot

        def arm(resolved):
            snap = real_read(resolved)
            torn_after_the_wait.armed = True      # only the probe that follows is torn
            return snap

        monkeypatch.setattr(rename, "read_snapshot", arm)
        monkeypatch.setattr(daemonlock, "probe", torn_after_the_wait)
        result = self.clear(run, rich)
        self.assert_refused(
            result, stale, rich,
            f"a daemon is running (pid {daemon.pid}); its state file could not be read "
            "consistently",
        )

    def test_a_store_error_that_is_a_locked_database_is_exit_75(
        self, run, rich, stale, monkeypatch
    ):
        from sqlalchemy.exc import OperationalError

        def locked(resolved):
            raise OperationalError("SELECT 1", {}, Exception("database is locked"))

        monkeypatch.setattr(rename, "read_snapshot", locked)
        result = self.clear(run, rich)
        assert result.exit_code == 75, (result.stdout, result.stderr)
        err = result.stderr
        assert "clearing the stale properties on 'tank/vm.01' failed" in err
        assert "Cleared so far: none" in err
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in err
        assert stale.mutations == [] and "tank/vm.02" in names(rich)

    def test_an_existing_but_unheld_workers_lock_is_no_workers(self, run, rich, db, stale):
        """A host where a daemon once ran: the file stays, nobody holds it."""
        path = daemonlock.workers_lock_path(resolve_config_path(str(db)))
        path.write_text("")
        assert path.exists() and not daemonlock.workers_present(resolve_config_path(str(db)))
        self.assert_cleared(self.clear(run, rich), stale, rich)

    # -- 11: dry run ---------------------------------------------------------

    def test_a_dry_run_says_when_the_clear_would_happen(self, run, rich, db, stale, daemon):
        state = db_state(db)
        result = self.clear(run, rich, "-d")
        assert result.exit_code == 0, result.stderr
        assert "would clear on 'tank/vm.01'" in result.stdout
        assert (
            f"a daemon is running (pid {daemon.pid}); after the write the clear happens only if the "
            "daemon is verified and applies the rename within --reload-wait, else it is skipped "
            "and the zfs inherit commands printed instead"
        ) in result.stdout
        assert db_state(db) == state and stale.mutations == []


class TestClearStaleKeptExclusion:
    """`_clear_stale` failure path: the names that are only RECEIVED on the old dataset (`kept`: `inherit -S`
    leaves them) get no `zfs inherit` command in the "to clear by hand" list, and when nothing is left the
    message says so instead of printing an empty list."""

    @pytest.fixture
    def old_dataset(self, fake_zfs):
        fake_zfs.add("tank/vm.01", {})
        fake_zfs.set_prop("tank/vm.01", ANCHOR + "dc.two", "RCV", "received")     # kept: received only
        fake_zfs.set_prop("tank/vm.01", ANCHOR + "offsite", "LOCAL")              # cleared, or fails
        fake_zfs.set_prop("tank/vm.01", CONFIG_PROP, "cfg")
        fake_zfs.add("tank/vm.02", {})
        return fake_zfs

    def clear(self, run, rich):
        return run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", "-c", rich)

    def test_a_received_only_name_gets_no_command_when_a_later_clear_fails(self, run, rich, old_dataset):
        old_dataset.fail("inherit", "tank/vm.01", ANCHOR + "offsite")
        result = self.clear(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "Cleared so far: none" in err
        left = err.split("Still set, to clear by hand:")[1]
        assert inherit_command(ANCHOR + "offsite", "tank/vm.01") in left
        assert inherit_command(CONFIG_PROP, "tank/vm.01") in left
        assert inherit_command(ANCHOR + "dc.two", "tank/vm.01") not in left       # received only: nothing to run
        assert "Nothing else needs a command." not in err
        assert old_dataset.value("tank/vm.01", ANCHOR + "dc.two") == "RCV"

    def test_nothing_left_says_so_instead_of_an_empty_command_list(self, run, rich, fake_zfs, monkeypatch):
        import contextlib

        fake_zfs.add("tank/vm.01", {})
        fake_zfs.set_prop("tank/vm.01", ANCHOR + "dc.two", "RCV", "received")     # the only stale property
        fake_zfs.add("tank/vm.02", {})
        real = rename._clear_gate

        @contextlib.contextmanager
        def failing_late(*args, **kwargs):
            with real(*args, **kwargs):
                yield
            raise RuntimeError("late failure")             # after every property was dealt with

        monkeypatch.setattr(rename, "_clear_gate", failing_late)
        result = self.clear(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "clearing the stale properties on 'tank/vm.01' failed: late failure" in err
        assert "Cleared so far: none. Nothing else needs a command." in err
        assert "Still set, to clear by hand" not in err and "zfs inherit" not in err
        assert "tank/vm.02" in names(rich)

    def test_a_busy_gate_lists_every_name_because_none_was_examined_yet(self, run, rich, old_dataset, monkeypatch):
        import contextlib

        @contextlib.contextmanager
        def busy(*args, **kwargs):
            raise ConfigBusy("writer lock held")
            yield

        monkeypatch.setattr(rename, "_clear_gate", busy)
        result = self.clear(run, rich)
        assert result.exit_code == 75
        assert inherit_command(ANCHOR + "dc.two", "tank/vm.01") in result.stderr   # nothing was examined: all listed

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
        """Three real CLI processes with NO `zfs` anywhere (`ZFS_CMD` unset, a `PATH` of the
        interpreter's directory only).

        Hardened after a one-off failure in a full run that never reproduced alone or under CPU
        load: the processes import the live source tree, so a module another session is
        mid-way through saving fails them for reasons that have nothing to do with ZFS. Every
        failure now carries the child's stdout and stderr (the traceback names the module);
        the child writes no `.pyc` and has its own `HOME`; and the test first proves that its
        environment really cannot reach a `zfs` (it must never touch a real one).
        """
        import shutil

        make_db(run, db, RENAME_YAML)
        home = tmp_path / "home"
        home.mkdir()
        env = {k: v for k, v in os.environ.items() if k not in ("ZFS_CMD", "ZPOOL_CMD")}
        env["PATH"] = os.path.dirname(sys.executable)  # no zfs / zpool anywhere
        env["PYTHONPATH"] = REPO
        env["HOME"] = str(home)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        assert shutil.which("zfs", path=env["PATH"]) is None, "a zfs is on the interpreter's PATH"
        assert shutil.which("zpool", path=env["PATH"]) is None

        def cli(*args):
            done = subprocess.run(
                [sys.executable, "-m", "zfsbackup.cli", *args, "-c", str(db)],
                capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120,
            )
            done.report = f"exit {done.returncode}\n--- stdout ---\n{done.stdout}\n--- stderr ---\n{done.stderr}"
            return done

        done = cli("rename", "dataset", "tank/vm.01", "tank/vm.02")
        assert done.returncode == 0, done.report
        assert "ZFS was not consulted" in done.stderr, done.report
        assert "tank/vm.02" in names(db)
        refused = cli("rename", "dataset", "tank/vm.02", "tank/vm.03", "--clear-stale-props")
        assert refused.returncode == 1 and "needs ZFS" in refused.stderr, refused.report
        no_anchors = cli("rename", "destination", "offsite", "offsite2")
        assert no_anchors.returncode == 1 and "cannot be used here" in no_anchors.stderr, no_anchors.report


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
        assert config.destinations["offsite2"].url == "https://offsite:8080"
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
        assert f"zfs inherit -S {ANCHOR}offsite tank/data" in out
        assert f"zfs inherit -S {ANCHOR}offsite tank/other" in out

    def test_only_datasets_that_have_an_old_anchor_get_an_inherit_hint(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {})                                   # nothing to copy
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "x"})
        out = self.rename(run, rich).stdout
        hints = [l for l in out.splitlines() if l.strip().startswith("zfs inherit")]
        assert hints == [f"  zfs inherit -S {ANCHOR}offsite tank/other"]
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
            ("x" * 232, None),
            ("x" * 233, "it is too long"),
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
        # measured against the longer `inflight.` pin property, 255 characters at most
        limit = 255 - len("org.zfsbackup:inflight.")
        assert limit == 232
        assert dotpath.destination_name_problem("x" * limit) is None
        assert "too long" in dotpath.destination_name_problem("x" * (limit + 1))

    def test_the_longest_name_is_renamed_to_and_one_more_is_refused(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {})
        fake_zfs.add("tank/other", {})
        state = db_state(rich)
        refused = run("rename", "destination", "offsite", "y" * 233, "-c", rich)
        assert refused.exit_code == 1 and "it is too long" in refused.stderr
        assert db_state(rich) == state
        assert run("rename", "destination", "offsite", "y" * 232, "-c", rich).exit_code == 0
        assert "y" * 232 in stored(rich).destinations

    def test_a_stored_unusable_name_nothing_refers_to_is_renamed_to_a_usable_one(self, run, rich):
        """The way out: read-only commands and `rename destination BAD GOOD` work on a bad stored name
        (planted with SQL, as an older release could have stored it)."""
        plant_destination_name(rich, "spare", "bad name")
        gen = generation(rich)
        assert run("list", "destinations", "-c", rich).exit_code == 0
        assert run("export", "-c", rich).exit_code == 0
        result = run("rename", "destination", "bad name", "spare", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert generation(rich) == gen + 1
        assert sorted(stored(rich).destinations) == ["dc.two", "offsite", "spare"]

    BAD = "bad name"
    NOTE = (
        "note: the old name 'bad name' could not hold anchors or send pins "
        f"({dotpath.destination_name_problem('bad name')}), "
        "so there is nothing to copy; the first send to 'offsite' negotiates its base"
    )

    def planted(self, rich, fake_zfs):
        """`tank/data` (recursive) and `tank/other` send to the stored unusable name `bad name`; the
        fake pool holds what a real one can: no property of that name (it cannot exist)."""
        fake_zfs.add("tank/data", {ANCHOR + "dc.two": "snapB"})
        fake_zfs.add("tank/other", {})
        plant_destination_name(rich, "offsite", self.BAD)
        fake_zfs.clear_calls()
        assert self.BAD in stored(rich).destinations
        return generation(rich)

    @staticmethod
    def mentioning(fake_zfs, text="bad name"):
        return [c for c in fake_zfs.calls if text in " ".join(c)]

    def test_a_stored_unusable_name_a_dataset_sends_to_is_renamed_to_a_usable_one(self, run, rich, fake_zfs):
        gen = self.planted(rich, fake_zfs)
        result = run("rename", "destination", "bad name", "offsite", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert generation(rich) == gen + 1
        config = stored(rich)
        assert sorted(config.destinations) == ["dc.two", "offsite", "spare"]
        assert [r.destination for d in config.datasets if d.name == "tank/other" for r in d.remote] == ["offsite"]
        # nothing was copied: no anchor or pin of the new name appeared anywhere
        for dataset in ("tank/data", "tank/other"):
            props = fake_zfs.props(dataset)
            assert ANCHOR + "offsite" not in props and INFLIGHT + "offsite" not in props
        assert result.stdout.count(self.NOTE) == 1
        # and no zfs get/set/inherit of any property containing the bad name was ever run
        assert self.mentioning(fake_zfs) == []
        assert fake_zfs.mutations == []

    def test_the_note_follows_the_renamed_line_and_is_printed_once(self, run, rich, fake_zfs):
        self.planted(rich, fake_zfs)
        result = run("rename", "destination", "bad name", "offsite", "-c", rich)
        lines = result.stdout.splitlines()
        assert lines[0] == "Renamed destination 'bad name' -> 'offsite' in the configuration."
        assert lines.index(self.NOTE) > 0
        assert (result.stdout + result.stderr).count("could not hold anchors") == 1

    def test_a_stored_unusable_name_makes_no_zfs_call_and_loads_no_backup_manager(
        self, run, rich, fake_zfs, monkeypatch
    ):
        self.planted(rich, fake_zfs)
        loaded = []
        monkeypatch.setattr(rename, "_load_backup_manager", lambda: loaded.append(1) or (None, "spy"))
        result = run("rename", "destination", "bad name", "offsite", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert loaded == []
        assert fake_zfs.calls == []
        assert "ZFS" not in result.stderr and "must be copied" not in result.stderr

    def test_a_stored_unusable_name_is_renamed_with_zfs_unavailable(self, run, rich, fake_zfs, monkeypatch):
        """Before the fix a dataset sending to the name made `rename` abort 'ZFS cannot be used here'."""
        self.planted(rich, fake_zfs)
        monkeypatch.setattr(rename, "_load_backup_manager", lambda: (None, "libzfseasy: no zfs binary here"))
        result = run("rename", "destination", "bad name", "offsite", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert "cannot be used here" not in result.stderr
        assert "offsite" in stored(rich).destinations and self.BAD not in stored(rich).destinations
        assert result.stdout.count(self.NOTE) == 1
        assert fake_zfs.calls == []

    @pytest.mark.integration
    def test_in_a_process_with_no_zfs_at_all_the_unusable_name_is_renamed_and_no_zfs_module_loads(
        self, tmp_path, run, rich, fake_zfs
    ):
        self.planted(rich, fake_zfs)
        env = {k: v for k, v in os.environ.items() if k not in ("ZFS_CMD", "ZPOOL_CMD")}
        env["PATH"] = os.path.dirname(sys.executable)
        env["PYTHONPATH"] = REPO
        code = (
            "import sys\n"
            "from zfsbackup.cli.main import cli\n"
            "try:\n"
            f"    cli.main(args={['rename', 'destination', 'bad name', 'offsite', '-c', str(rich)]!r}, standalone_mode=False)\n"
            "except SystemExit as exc:\n"
            "    print('EXIT', exc.code)\n"
            "bad = sorted(m for m in sys.modules if m.split('.')[0] == 'libzfseasy' "
            "or m in ('zfsbackup.backup_manager', 'zfsbackup.daemon'))\n"
            "print('LOADED', bad)\n"
        )
        done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                              env=env, cwd=str(tmp_path), timeout=120)
        report = f"{done.stdout}\n{done.stderr}"
        assert done.returncode == 0, report
        assert "LOADED []" in done.stdout and "EXIT" not in done.stdout, report
        assert "could not hold anchors" in done.stdout, report
        assert "offsite" in stored(rich).destinations and self.BAD not in stored(rich).destinations

    def test_skip_missing_is_irrelevant_for_an_unusable_name(self, run, rich, fake_zfs):
        """No existence check is made, so a dataset missing from ZFS neither blocks nor warns."""
        gen = self.planted(rich, fake_zfs)
        fake_zfs.state_path.write_text("{}")      # neither dataset exists in ZFS
        for extra in ((), ("--skip-missing",)):
            state = db_state(rich)
            result = run("rename", "destination", "bad name", "offsite", *extra, "-d", "-c", rich)
            assert result.exit_code == 0, result.stderr
            assert "does not exist in ZFS" not in result.stderr and warnings_in(result) == []
            assert db_state(rich) == state
        result = run("rename", "destination", "bad name", "offsite", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert warnings_in(result) == [] and generation(rich) == gen + 1
        assert fake_zfs.calls == []

    def test_a_recursive_dataset_sending_to_an_unusable_name_is_not_expanded(self, run, rich, fake_zfs):
        """`tank/data` is `recursive: true`: no descendant listing, no descendant anchor read or copy."""
        self.planted(rich, fake_zfs)
        fake_zfs.add("tank/data/child", {ANCHOR + "dc.two": "c1"})
        fake_zfs.clear_calls()
        result = run("rename", "destination", "bad name", "offsite", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.calls == []
        assert ANCHOR + "offsite" not in fake_zfs.props("tank/data/child")
        assert warnings_in(result) == []

    def test_dry_run_prints_the_prefixed_note_before_would_rename_and_writes_nothing(
        self, run, rich, fake_zfs
    ):
        gen = self.planted(rich, fake_zfs)
        state = db_state(rich)
        result = run("rename", "destination", "bad name", "offsite", "--dry-run", "-c", rich)
        assert result.exit_code == 0, result.stderr
        lines = result.stdout.splitlines()
        noted = [i for i, l in enumerate(lines) if l == "[dry run] " + self.NOTE]
        would = [i for i, l in enumerate(lines) if l.startswith("[dry run] would rename destination 'bad name'")]
        assert len(noted) == 1 and len(would) == 1 and noted[0] < would[0]
        assert "tank/data" in lines[would[0]] and "tank/other" in lines[would[0]]
        assert result.stdout.count("could not hold anchors") == 1
        assert db_state(rich) == state and generation(rich) == gen
        assert fake_zfs.calls == []

    def test_a_generation_conflict_still_exits_75_and_prints_no_note(self, run, rich, fake_zfs, monkeypatch):
        self.planted(rich, fake_zfs)
        # another writer commits between the snapshot and the write. `write_config` would refuse (the
        # stored name is unusable), so bump the generation with SQL, as that writer's commit would.
        real = rename.commit_config
        done = []

        def commit(resolved, snapshot, config, **kwargs):
            from sqlalchemy import text

            from zfsbackup.config.store import db as store_db

            try:
                with store_db.session_scope(store_db.url_for_path(rich)) as session:
                    session.execute(text("UPDATE global_settings SET generation = generation + 1"))
            finally:
                dispose_all()
            done.append(True)
            return real(resolved, snapshot, config, **kwargs)

        monkeypatch.setattr(rename, "commit_config", commit)
        result = run("rename", "destination", "bad name", "offsite", "-c", rich)
        assert done == [True]
        assert result.exit_code == 75
        assert "config changed underneath you" in result.stderr
        assert "could not hold anchors" not in result.stdout
        assert self.BAD in stored(rich).destinations and "offsite" not in stored(rich).destinations
        assert fake_zfs.calls == []

    def test_a_usable_old_name_prints_no_note(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {})
        fake_zfs.add("tank/other", {})
        result = run("rename", "destination", "offsite", "offsite2", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert "could not hold anchors" not in result.stdout + result.stderr

    def test_renaming_a_stored_unusable_name_to_another_unusable_one_is_refused(self, run, rich, fake_zfs):
        plant_destination_name(rich, "offsite", "bad name")
        state = db_state(rich)
        result = run("rename", "destination", "bad name", "also bad", "-c", rich)
        assert result.exit_code == 1 and "cannot be used as a destination name" in result.stderr
        assert db_state(rich) == state

    def test_another_edit_on_a_database_holding_an_unusable_name_is_refused_until_it_is_renamed(
        self, run, rich
    ):
        plant_destination_name(rich, "offsite", "bad name")
        state = db_state(rich)
        result = run("rename", "destination", "spare", "spare2", "-c", rich)
        assert result.exit_code == 1
        assert "destinations[bad name]: the name cannot form the ZFS properties" in result.stderr
        assert db_state(rich) == state

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
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/data" in err
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
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/data" in err
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/other" in err
        assert db_state(rich) == state

    def test_an_interrupt_on_the_first_copy_restores_only_the_one_in_flight(self, run, rich, anchors, monkeypatch):
        import zfsbackup.backup_manager as bm

        def interrupting(ds, *args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(bm, "copy_anchor_property", interrupting)
        err = self.rename(run, rich).stderr
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/data" in err
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
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/other" in err         # was absent
        assert "offsite2" not in stored(rich).destinations

    def test_a_conflict_raised_by_the_commit_is_75_with_the_restore_commands(
        self, run, rich, anchors, monkeypatch
    ):
        def conflict(*args, **kwargs):
            raise ConfigConflict(0, 1)

        monkeypatch.setattr(rename, "commit_config", conflict)
        result = self.rename(run, rich)
        assert result.exit_code == 75
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/data" in result.stderr
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/other" in result.stderr

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


INFLIGHT = "org.zfsbackup:inflight."


class TestRenameDestinationInflight:
    """`rename destination` moves `inflight.OLD` (the pin on the two snapshots of
    a send that may still be resumed, `<from>,<to>`) exactly like `anchor.OLD`:
    copied to `inflight.NEW` before the write, a differing value overwritten,
    the old one kept and listed for the operator to clear."""

    @pytest.fixture
    def pins(self, fake_zfs):
        fake_zfs.add("tank/data", {
            ANCHOR + "offsite": "snapA", INFLIGHT + "offsite": "snapA,snapB",
            INFLIGHT + "dc.two": "x,y",
        })
        fake_zfs.add("tank/other", {INFLIGHT + "offsite": "s1,s2"})      # a pin, no anchor
        return fake_zfs

    def rename(self, run, rich, *extra, new="offsite2", old="offsite"):
        return run("rename", "destination", old, new, *extra, "-c", rich)

    def test_the_pin_is_copied_before_the_write_and_the_old_one_kept(self, run, rich, pins, monkeypatch):
        seen = {}
        real = rename.commit_config

        def commit(*args, **kwargs):
            seen["sets"] = [c for c in pins.calls if c[0] == "set"]
            return real(*args, **kwargs)

        monkeypatch.setattr(rename, "commit_config", commit)
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert sorted(seen["sets"]) == sorted([
            ["set", f"{ANCHOR}offsite2=snapA", "tank/data"],
            ["set", f"{INFLIGHT}offsite2=snapA,snapB", "tank/data"],
            ["set", f"{INFLIGHT}offsite2=s1,s2", "tank/other"],
        ])
        assert pins.props("tank/data")[INFLIGHT + "offsite"] == "snapA,snapB"   # copied, not moved
        assert pins.props("tank/data")[INFLIGHT + "offsite2"] == "snapA,snapB"
        assert pins.props("tank/other")[INFLIGHT + "offsite2"] == "s1,s2"
        assert INFLIGHT + "dc.two2" not in pins.props("tank/data")        # the other destination's pin
        assert pins.calls_of("inherit") == []

    def test_the_output_names_each_pin_copy(self, run, rich, pins):
        out = self.rename(run, rich).stdout
        assert "tank/data: inflight.offsite2 set to snapA,snapB (copied from inflight.offsite)" in out
        assert "tank/other: inflight.offsite2 set to s1,s2 (copied from inflight.offsite)" in out
        assert "tank/data: anchor.offsite2 set to snapA (copied from anchor.offsite)" in out
        assert "tank/other: has no anchor.offsite to copy" in out

    def test_no_source_pin_says_nothing_about_inflight(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "inflight" not in result.stdout
        assert not any(INFLIGHT in " ".join(c) for c in fake_zfs.mutations)

    def test_a_stray_new_pin_without_a_source_is_left_alone(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {INFLIGHT + "offsite2": "keep,me"})
        fake_zfs.add("tank/other", {})
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "inflight" not in result.stdout
        assert fake_zfs.mutations == []
        assert fake_zfs.props("tank/data")[INFLIGHT + "offsite2"] == "keep,me"

    def test_a_differing_new_pin_is_overwritten_and_both_values_are_shown(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {INFLIGHT + "offsite": "n1,n2", INFLIGHT + "offsite2": "o1,o2"})
        fake_zfs.add("tank/other", {})
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert (
            "tank/data: inflight.offsite2 was o1,o2, now n1,n2 (copied from inflight.offsite); "
            "snapshot o1,o2 is no longer pinned by inflight.offsite2"
        ) in result.stdout
        assert fake_zfs.props("tank/data")[INFLIGHT + "offsite2"] == "n1,n2"
        assert fake_zfs.props("tank/data")[INFLIGHT + "offsite"] == "n1,n2"

    def test_an_equal_new_pin_is_already_current_and_not_set_again(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {INFLIGHT + "offsite": "a,b", INFLIGHT + "offsite2": "a,b"})
        fake_zfs.add("tank/other", {})
        result = self.rename(run, rich)
        assert "tank/data: inflight.offsite2 is already current (a,b)" in result.stdout
        assert fake_zfs.mutations == []

    def test_dry_run_says_what_would_happen_and_changes_nothing(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {INFLIGHT + "offsite": "n1,n2", INFLIGHT + "offsite2": "o1,o2"})
        fake_zfs.add("tank/other", {INFLIGHT + "offsite": "s1,s2"})
        state = db_state(rich)
        result = self.rename(run, rich, "-d")
        assert result.exit_code == 0, result.stderr
        out = result.stdout
        assert (
            "[dry run] tank/data: inflight.offsite2 would be overwritten: was o1,o2, would "
            "become n1,n2 (copied from inflight.offsite)"
        ) in out
        assert "[dry run] tank/other: inflight.offsite2 would be set to s1,s2 (copied from inflight.offsite)" in out
        assert fake_zfs.mutations == [] and db_state(rich) == state

    def test_dry_run_with_an_equal_pin_is_already_current(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {INFLIGHT + "offsite": "a,b", INFLIGHT + "offsite2": "a,b"})
        fake_zfs.add("tank/other", {})
        out = self.rename(run, rich, "-d").stdout
        assert "[dry run] tank/data: inflight.offsite2 is already current (a,b)" in out

    def test_the_stale_commands_include_the_inflight_inherit(self, run, rich, pins):
        out = self.rename(run, rich).stdout
        assert f"zfs inherit -S {INFLIGHT}offsite tank/data" in out
        assert f"zfs inherit -S {INFLIGHT}offsite tank/other" in out
        assert f"zfs inherit -S {ANCHOR}offsite tank/data" in out
        assert f"zfs inherit -S {ANCHOR}offsite tank/other" not in out        # no anchor there
        assert "inflight.offsite (the pin on the snapshots of a send to 'offsite'" in out
        assert f"zfs inherit -S {INFLIGHT}dc.two" not in out                   # not this destination's

    def test_only_a_pin_alone_still_gets_the_inherit_hint(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {})
        fake_zfs.add("tank/other", {INFLIGHT + "offsite": "s1,s2"})
        out = self.rename(run, rich).stdout
        hints = [l.strip() for l in out.splitlines() if l.strip().startswith("zfs inherit")]
        assert hints == [f"zfs inherit -S {INFLIGHT}offsite tank/other"]

    def test_a_dotted_destination_pin(self, run, rich, pins):
        result = self.rename(run, rich, old="dc.two", new="dc.three")
        assert result.exit_code == 0, result.stderr
        assert pins.props("tank/data")[INFLIGHT + "dc.three"] == "x,y"

    # -- failures ---------------------------------------------------------------

    def test_a_commit_failure_after_the_copy_prints_the_pin_rollback_too(
        self, run, rich, fake_zfs, monkeypatch
    ):
        fake_zfs.add("tank/data", {
            ANCHOR + "offsite": "snapA", INFLIGHT + "offsite": "n1,n2", INFLIGHT + "offsite2": "o1,o2",
        })
        fake_zfs.add("tank/other", {INFLIGHT + "offsite": "s1,s2"})

        def busy(*args, **kwargs):
            raise ConfigBusy("database is locked by another process")

        monkeypatch.setattr(rename, "commit_config", busy)
        result = self.rename(run, rich)
        assert result.exit_code == 75
        err = result.stderr
        assert "To put the previous values back run:" in err
        assert f"  zfs set {INFLIGHT}offsite2=o1,o2 tank/data" in err        # was overwritten
        assert f"  zfs inherit -S {INFLIGHT}offsite2 tank/other" in err          # was absent
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/data" in err             # the anchor, as before
        assert f"{INFLIGHT}offsite2 was overwritten on 'tank/data'" in err
        assert f"{INFLIGHT}offsite2 was newly set on 'tank/other'" in err
        assert "which copies anchor.offsite and inflight.offsite again" in err
        assert "offsite2" not in stored(rich).destinations

    def test_the_restore_note_without_pins_is_unchanged(self, run, rich, fake_zfs, monkeypatch):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        fake_zfs.add("tank/other", {})

        def busy(*args, **kwargs):
            raise ConfigBusy("locked")

        monkeypatch.setattr(rename, "commit_config", busy)
        err = self.rename(run, rich).stderr
        assert "which copies anchor.offsite again" in err and "inflight" not in err

    def test_the_pin_restore_commands_really_restore(self, run, rich, fake_zfs, monkeypatch):
        fake_zfs.add("tank/data", {INFLIGHT + "offsite": "n1,n2", INFLIGHT + "offsite2": "o1,o2"})
        fake_zfs.add("tank/other", {INFLIGHT + "offsite": "s1,s2"})

        def busy(*args, **kwargs):
            raise ConfigBusy("locked")

        monkeypatch.setattr(rename, "commit_config", busy)
        err = self.rename(run, rich).stderr
        commands = [
            l.strip() for l in err.split("To put the previous values back run:")[1].splitlines()
            if l.strip().startswith("zfs ")
        ]
        assert sorted(commands) == sorted([
            f"zfs set {INFLIGHT}offsite2=o1,o2 tank/data",
            f"zfs inherit -S {INFLIGHT}offsite2 tank/other",
        ])
        for command in commands:
            subprocess.run([str(fake_zfs.wrapper), *shlex.split(command)[1:]], check=True, env=dict(os.environ))
        assert fake_zfs.props("tank/data")[INFLIGHT + "offsite2"] == "o1,o2"
        assert INFLIGHT + "offsite2" not in fake_zfs.props("tank/other")

    def test_a_failing_pin_copy_aborts_before_the_write_and_rolls_back_the_anchor(self, run, rich, pins):
        pins.fail("set", "tank/data", INFLIGHT + "offsite2")
        state = db_state(rich)
        result = self.rename(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert f"copying {INFLIGHT}offsite to inflight.offsite2 on 'tank/data' failed" in err
        assert "Nothing was written to the configuration" in err
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/data" in err           # the anchor that landed
        assert f"zfs inherit -S {INFLIGHT}offsite2 tank/data" not in err      # the pin that did not
        assert db_state(rich) == state

    def test_an_interrupt_during_a_pin_copy_covers_it_in_the_note(self, run, rich, pins, monkeypatch):
        def interrupting(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(rename, "_copy_inflight_property", interrupting)
        state = db_state(rich)
        result = self.rename(run, rich)
        assert result.exit_code == 130
        err = result.stderr
        assert f"interrupted while copying {INFLIGHT}offsite to inflight.offsite2 on 'tank/data'" in err
        assert "Nothing was written to the configuration" in err
        assert f"  zfs inherit -S {ANCHOR}offsite2 tank/data" in err           # finished anchor copy
        assert f"  zfs inherit -S {INFLIGHT}offsite2 tank/data" in err         # the one in flight
        assert db_state(rich) == state

    # -- --skip-missing ---------------------------------------------------------

    def test_skip_missing_datasets_get_no_pin_copy(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {INFLIGHT + "offsite": "a,b"})          # tank/other does not exist
        result = self.rename(run, rich, "--skip-missing")
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.props("tank/data")[INFLIGHT + "offsite2"] == "a,b"
        assert all(c[-1] != "tank/other" for c in fake_zfs.mutations)
        assert "tank/other:" not in result.stdout
        assert f"zfs inherit -S {INFLIGHT}offsite tank/other" not in result.stdout


class TestRenameDatasetInflight:
    """`rename dataset`: `inflight.*` are stale properties of the old dataset,
    cleared (sorted, one `zfs inherit` each) with `anchor.*` and the config."""

    PROPS = {
        CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1", ANCHOR + "dc.two": "snap2",
        INFLIGHT + "offsite": "a,b", INFLIGHT + "dc.two": "c,d", "compression": "lz4",
    }
    ORDER = [
        ANCHOR + "dc.two", ANCHOR + "offsite", CONFIG_PROP,
        INFLIGHT + "dc.two", INFLIGHT + "offsite",
    ]

    @pytest.fixture
    def stale(self, fake_zfs):
        fake_zfs.add("tank/vm.01", self.PROPS)
        fake_zfs.add("tank/vm.02", {})
        return fake_zfs

    def clear(self, run, rich, *extra):
        return run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", *extra, "-c", rich)

    def inherits(self, fake):
        return [c for c in fake.calls if c[0] == "inherit"]

    def test_clear_stale_props_inherits_the_pins_in_sorted_order(self, run, rich, stale):
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        assert [inherit_property(c) for c in self.inherits(stale)] == self.ORDER
        assert all(c[-1] == "tank/vm.01" for c in self.inherits(stale))
        assert stale.props("tank/vm.01") == {"compression": "lz4"}
        assert "tank/vm.02" in names(rich)
        cleared = [l for l in result.stdout.splitlines() if l.startswith("Cleared on 'tank/vm.01':")]
        assert cleared == ["Cleared on 'tank/vm.01': " + ", ".join(self.ORDER)]

    def test_a_pin_inherit_is_a_one_property_inherit_on_the_old_dataset(self, run, rich, stale):
        self.clear(run, rich)
        pin_calls = [c for c in self.inherits(stale) if inherit_property(c).startswith(INFLIGHT)]
        assert [(inherit_property(c), c[-1]) for c in pin_calls] == [(INFLIGHT + "dc.two", "tank/vm.01"),
                                                       (INFLIGHT + "offsite", "tank/vm.01")]

    def test_a_pin_is_cleared_with_inherit_dash_S_like_the_advice_says(self, run, rich, stale):
        self.clear(run, rich)
        pin_calls = [c for c in self.inherits(stale) if inherit_property(c).startswith(INFLIGHT)]
        # libzfseasy writes the options before the property: `zfs inherit -S PROP DATASET`
        assert pin_calls == [["inherit", "-S", INFLIGHT + "dc.two", "tank/vm.01"],
                             ["inherit", "-S", INFLIGHT + "offsite", "tank/vm.01"]]

    def test_clearing_leaves_a_received_only_anchor_in_place(self, run, rich, fake_zfs):
        """`-S` leaves a received-only anchor alone (the pruner keeps protecting it); what the
        command says about it is pinned in `TestRenameDatasetLeftInPlaceReceived`."""
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg"})
        fake_zfs.set_prop("tank/vm.01", ANCHOR + "offsite", "RCV", "received")
        fake_zfs.add("tank/vm.02", {})
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.value("tank/vm.01", ANCHOR + "offsite") == "RCV"
        assert fake_zfs.source("tank/vm.01", ANCHOR + "offsite") == "received"
        assert fake_zfs.value("tank/vm.01", CONFIG_PROP) is None


    def test_a_failing_pin_inherit_reports_cleared_so_far_and_what_is_left(self, run, rich, stale):
        stale.fail("inherit", "tank/vm.01", INFLIGHT + "offsite")
        result = self.clear(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "clearing the stale properties on 'tank/vm.01' failed" in err
        assert "Cleared so far: " + ", ".join(self.ORDER[:4]) in err
        left = err.split("Still set, to clear by hand:")[1]
        assert f"zfs inherit -S {INFLIGHT}offsite tank/vm.01" in left
        for prop in self.ORDER[:4]:
            assert inherit_command(prop, "tank/vm.01") not in left
        assert stale.props("tank/vm.01") == {
            INFLIGHT + "offsite": "a,b", "compression": "lz4",
        }
        assert [inherit_property(c) for c in self.inherits(stale)] == self.ORDER       # it stopped at the failure

    def test_a_failing_first_pin_leaves_both_pins(self, run, rich, stale):
        stale.fail("inherit", "tank/vm.01", INFLIGHT + "dc.two")
        err = self.clear(run, rich).stderr
        assert "Cleared so far: " + ", ".join(self.ORDER[:3]) + "." in err
        left = err.split("Still set, to clear by hand:")[1]
        assert f"zfs inherit -S {INFLIGHT}dc.two tank/vm.01" in left
        assert f"zfs inherit -S {INFLIGHT}offsite tank/vm.01" in left
        assert stale.props("tank/vm.01")[INFLIGHT + "offsite"] == "a,b"

    def test_an_interrupt_before_a_pin_lists_the_pins_that_may_remain(self, run, rich, stale, monkeypatch):
        import zfsbackup.backup_manager as bm

        real = bm.zfs.inherit

        def interrupting(ds, prop, *args, **kwargs):
            if prop == INFLIGHT + "dc.two":
                raise KeyboardInterrupt
            return real(ds, prop, *args, **kwargs)

        monkeypatch.setattr(bm.zfs, "inherit", interrupting)
        result = self.clear(run, rich)
        assert result.exit_code == 130
        assert (
            f"Properties that may remain: {INFLIGHT}dc.two, {INFLIGHT}offsite"
        ) in result.stderr

    def test_the_plain_hint_prints_the_pin_inherit_commands(self, run, rich, stale):
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        out = result.stdout
        assert "The stale properties on 'tank/vm.01' were left in place" in out
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in out
        assert stale.mutations == []

    def test_the_warning_names_the_pins_among_the_stale_properties(self, run, rich, stale):
        err = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich).stderr
        assert INFLIGHT + "offsite" in err and INFLIGHT + "dc.two" in err

    def test_a_dry_run_lists_the_pins_under_would_clear(self, run, rich, stale):
        state = db_state(rich)
        result = self.clear(run, rich, "-d")
        assert result.exit_code == 0, result.stderr
        assert f"would clear on 'tank/vm.01': {', '.join(self.ORDER)}" in result.stdout
        assert db_state(rich) == state and stale.mutations == []

    def test_a_dataset_with_only_a_pin_is_cleared(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {INFLIGHT + "offsite": "a,b"})
        fake_zfs.add("tank/vm.02", {})
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.props("tank/vm.01") == {}
        assert "Cleared on 'tank/vm.01': " + INFLIGHT + "offsite" in result.stdout

    def test_a_pin_on_the_old_dataset_alone_is_not_cleared_without_the_flag(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {INFLIGHT + "offsite": "a,b"})
        fake_zfs.add("tank/vm.02", {})
        assert run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich).exit_code == 0
        assert fake_zfs.props("tank/vm.01") == {INFLIGHT + "offsite": "a,b"}

    def test_a_running_daemon_still_means_the_pins_stay(
        self, run, rich, stale, daemon_lock_holder, unsignalable
    ):
        daemon_lock_holder(rich, generation=0)
        result = self.clear(run, rich)
        assert result.exit_code == 1
        for prop in self.ORDER:
            assert inherit_command(prop, "tank/vm.01") in result.stderr
        assert stale.mutations == [] and stale.props("tank/vm.01") == self.PROPS


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
        assert body == ['url: "https://dc2:8080"']
        assert "rename destination <old> <new>" in stub.log[0]["before"]

    def test_a_destination_url_edit_saves(self, run, ready, stub):
        stub.plan([{"replace": ["https://dc2:8080", "https://dc3:9090"]}])
        result = self.edit(run, stub, ready, "destination", "dc.two", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert "destinations changed: dc.two" in result.stdout
        assert stored(ready).destinations["dc.two"].url == "https://dc3:9090"

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

    def test_a_loaded_libzfseasy_never_prints_commands_to_stdout(self, run, db, fake_zfs):
        """libzfseasy logs the commands it runs (DEBUG, `libzfseasy` logger); the
        old `zfs.debug` flag is kept but ignored, so setting it prints nothing."""
        make_db(run, db, RENAME_YAML)
        import libzfseasy.zfs as zfs_module

        before = zfs_module.debug
        zfs_module.debug = True
        try:
            fake_zfs.add("tank/vm.01", {CONFIG_PROP: "x"})
            out = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", db)
        finally:
            zfs_module.debug = before
        assert out.exit_code == 0, out.stderr
        assert "Executing command" not in out.stdout
        assert "Executing command" not in out.stderr          # and not without -v

    def test_the_cli_does_not_touch_the_zfs_debug_flag(self, run, db, fake_zfs):
        make_db(run, db, RENAME_YAML)
        import libzfseasy.zfs as zfs_module

        before = zfs_module.debug
        zfs_module.debug = True
        try:
            fake_zfs.add("tank/vm.01", {CONFIG_PROP: "x"})
            run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", db)
            assert zfs_module.debug is True                    # the old workaround flipped it
        finally:
            zfs_module.debug = before


class TestZfsCommandsAreLogged:
    """`libzfseasy` logs `Executing command: <shlex.join(cmd)>` at DEBUG; the CLI
    shows it as a `debug:` line on stderr under `-v` and drops it otherwise."""

    @pytest.fixture
    def stale(self, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "x", ANCHOR + "offsite": "snap1"})
        fake_zfs.add("tank/vm.02", {})
        return fake_zfs

    def rename(self, run, rich, *flags, clear=False):
        args = ["-c", rich, *flags, "rename", "dataset", "tank/vm.01", "tank/vm.02"]
        if clear:
            args.append("--clear-stale-props")
        return run(*args)

    def test_with_v_the_commands_are_debug_lines_on_stderr_only(self, run, rich, stale):
        result = self.rename(run, rich, "-v")
        assert result.exit_code == 0, result.stderr
        lines = [l for l in result.stderr.splitlines() if "Executing command:" in l]
        assert lines and all(l.startswith("debug: Executing command: ") for l in lines)
        assert any(str(stale.wrapper) in l and " list " in l for l in lines)   # the real argv
        assert "Executing command" not in result.stdout

    def test_with_v_the_clear_commands_are_logged_too(self, run, rich, stale):
        result = self.rename(run, rich, "-v", clear=True)
        assert result.exit_code == 0, result.stderr
        assert sum(
            1 for l in result.stderr.splitlines()
            if l.startswith("debug: Executing command: ") and " inherit " in l
        ) == 2
        assert "Executing command" not in result.stdout

    def test_without_v_neither_stream_has_them(self, run, rich, stale):
        result = self.rename(run, rich, clear=True)
        assert result.exit_code == 0, result.stderr
        assert "Executing command" not in result.stdout + result.stderr
        assert "debug:" not in result.stderr
        assert stale.calls                                       # (commands did run)

    def test_the_subcommand_level_v_works_too(self, run, rich, stale):
        result = run("-c", rich, "rename", "dataset", "tank/vm.01", "tank/vm.02", "-v")
        assert result.exit_code == 0, result.stderr
        assert "debug: Executing command: " in result.stderr
        assert "Executing command" not in result.stdout

    def test_remove_logging_leaves_the_libzfseasy_logger_as_it_was(self, run, rich, stale):
        import logging

        loggers = [logging.getLogger(n) for n in ("libzfseasy", "zfsbackup")]
        saved = [l.level for l in loggers]
        for logger in loggers:
            logger.setLevel(logging.WARNING)          # a known level, whatever ran before
        try:
            self.check_no_leak(run, rich, stale, loggers)
        finally:
            for logger, level in zip(loggers, saved):
                logger.setLevel(level)

    def check_no_leak(self, run, rich, stale, loggers):
        before = [(list(l.handlers), l.level) for l in loggers]
        first = self.rename(run, rich, "-v")
        assert first.exit_code == 0 and "debug: Executing command" in first.stderr
        assert [(list(l.handlers), l.level) for l in loggers] == before

        stale.add("tank/vm.02", {})
        second = run("-c", rich, "rename", "dataset", "tank/vm.02", "tank/vm.03")
        assert second.exit_code == 0, second.stderr
        assert "Executing command" not in second.stdout + second.stderr     # no leak from -v
        assert [(list(l.handlers), l.level) for l in loggers] == before

    def test_the_handler_is_on_both_loggers_while_running_and_removed_after(self, run, rich, stale, monkeypatch):
        import logging

        seen = {}
        real = rename.commit_config

        def commit(*args, **kwargs):
            for name in ("libzfseasy", "zfsbackup"):
                logger = logging.getLogger(name)
                seen[name] = ([type(h).__name__ for h in logger.handlers], logger.level)
            return real(*args, **kwargs)

        monkeypatch.setattr(rename, "commit_config", commit)
        assert self.rename(run, rich, "-v").exit_code == 0
        for name in ("libzfseasy", "zfsbackup"):
            assert "_LogHandler" in seen[name][0], name
            assert seen[name][1] == 10, name                     # DEBUG under -v
            assert not any(
                type(h).__name__ == "_LogHandler" for h in logging.getLogger(name).handlers
            )

    def test_state_remove_logging_restores_the_levels(self):
        import logging

        from zfsbackup.cli import main as cli_main

        zfs_logger = logging.getLogger("libzfseasy")
        original = zfs_logger.level
        zfs_logger.setLevel(logging.ERROR)
        try:
            state = cli_main.State(verbose=True)
            state.install_logging()
            assert zfs_logger.level == logging.DEBUG and state.handler in zfs_logger.handlers
            state.remove_logging()
            assert zfs_logger.level == logging.ERROR
            assert not any(isinstance(h, cli_main._LogHandler) for h in zfs_logger.handlers)
            assert state.handler is None
        finally:
            zfs_logger.setLevel(original)


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
            assert inherit_command(prop, "tank/vm.01") in err
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


# --------------------------------------------------------------------------
# A4: recursive datasets. The descendants of an enabled `recursive: true`
# dataset that sends to OLD are sent as copies of their own, each with its own
# LOCAL `anchor.OLD` / `inflight.OLD`, so `rename destination` copies those too.
# --------------------------------------------------------------------------

#: One recursive dataset sending to `offsite`, a second recursive one sending only
#: to `spare`, and room for per-test extra datasets (`{extra}`).
TREE_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/tree"
    frequency: "1h"
    recursive: true
    retention:
      "1d": "30d"
    remote:
      - destination: "offsite"
{extra}destinations:
  offsite:
    url: "https://offsite:8080"
  spare:
    url: "https://spare:8080"
"""


def dataset_block(name, *, recursive=False, enabled=True, remote=("offsite",)):
    lines = [f'  - name: "{name}"', '    frequency: "1h"', '    retention:', '      "1d": "30d"']
    if recursive:
        lines.insert(2, "    recursive: true")
    if not enabled:
        lines.insert(2, "    enabled: false")
    if remote:
        lines.append("    remote:")
        lines += [f'      - destination: "{d}"' for d in remote]
    else:
        lines.append("    remote: []")
    return "\n".join(lines) + "\n"


def tree_db(run, db, *blocks):
    # `--force`: several tests plant backup copies below the recursive parent BEFORE the import, which the
    # import's backup-copy check (A2d) would refuse; what they test is what the RENAME does about them.
    made = make_db(run, db, TREE_YAML.format(extra="".join(blocks)), force=True)
    # That check also looked at ZFS for the recursive datasets; the tests count what the RENAME reads. (The fake is the module's autouse one: `$FAKEZFS_LOG` names its log.)
    open(os.environ["FAKEZFS_LOG"], "w").close()
    return made


def grow_tree(fake):
    """The ZFS side: the parent and four descendants with a mix of state."""
    fake.add("tank/tree", {ANCHOR + "offsite": "P0"})
    fake.add("tank/tree/a", {ANCHOR + "offsite": "A1", INFLIGHT + "offsite": "a1,a2"})
    fake.add("tank/tree/b", {ANCHOR + "offsite": "B1"})
    fake.add("tank/tree/b/deep", {INFLIGHT + "offsite": "d1,d2"})     # a pin and no anchor
    fake.add("tank/tree/empty", {})                                   # never sent
    return fake


def recursive_calls(fake, command="list"):
    return [c for c in fake.calls_of(command) if "-r" in c]


class TestRenameDestinationRecursive:
    @pytest.fixture
    def tree(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        return tree_db(run, db)

    def rename(self, run, database, *extra, old="offsite", new="offsite2"):
        return run("rename", "destination", old, new, *extra, "-c", database)

    def test_the_fake_walks_the_tree_and_finds_no_copy_root(self, fake_zfs):
        grow_tree(fake_zfs)
        listed = subprocess.run(
            [str(fake_zfs.wrapper), "list", "-H", "-r", "-t", "filesystem,volume", "-o", "name,type", "tank/tree"],
            capture_output=True, text=True, env=dict(os.environ),
        )
        assert listed.stdout.splitlines() == [
            "tank/tree\tfilesystem", "tank/tree/a\tfilesystem", "tank/tree/b\tfilesystem",
            "tank/tree/b/deep\tfilesystem", "tank/tree/empty\tfilesystem",
        ]

    # -- what is copied ---------------------------------------------------

    def test_every_descendant_with_something_to_copy_gets_its_own_copy_before_the_write(
        self, run, tree, fake_zfs, monkeypatch
    ):
        seen = {}
        real = rename.commit_config

        def commit(*args, **kwargs):
            seen["sets"] = [c for c in fake_zfs.calls if c[0] == "set"]
            return real(*args, **kwargs)

        monkeypatch.setattr(rename, "commit_config", commit)
        gen = generation(tree)
        result = self.rename(run, tree)
        assert result.exit_code == 0, result.stderr
        assert sorted(seen["sets"]) == sorted([
            ["set", f"{ANCHOR}offsite2=P0", "tank/tree"],
            ["set", f"{ANCHOR}offsite2=A1", "tank/tree/a"],
            ["set", f"{INFLIGHT}offsite2=a1,a2", "tank/tree/a"],
            ["set", f"{ANCHOR}offsite2=B1", "tank/tree/b"],
            ["set", f"{INFLIGHT}offsite2=d1,d2", "tank/tree/b/deep"],
        ])
        for name, prop, value in [
            ("tank/tree/a", ANCHOR, "A1"), ("tank/tree/b", ANCHOR, "B1"),
            ("tank/tree/a", INFLIGHT, "a1,a2"), ("tank/tree/b/deep", INFLIGHT, "d1,d2"),
        ]:
            assert fake_zfs.props(name)[prop + "offsite2"] == value
            assert fake_zfs.props(name)[prop + "offsite"] == value                # copied, never moved
        assert ANCHOR + "offsite2" not in fake_zfs.props("tank/tree/empty")
        assert INFLIGHT + "offsite2" not in fake_zfs.props("tank/tree/empty")
        assert fake_zfs.props("tank/tree/a")[ANCHOR + "offsite"] == "A1"       # copied, never moved
        assert fake_zfs.calls_of("inherit") == []
        assert generation(tree) == gen + 1
        assert "offsite2" in stored(tree).destinations and "offsite" not in stored(tree).destinations

    def test_the_output_names_each_child_copy_and_summarises_the_parent_once(self, run, tree):
        out = self.rename(run, tree).stdout
        assert "tank/tree: anchor.offsite2 set to P0 (copied from anchor.offsite)" in out
        assert "tank/tree/a: anchor.offsite2 set to A1 (copied from anchor.offsite)" in out
        assert "tank/tree/a: inflight.offsite2 set to a1,a2 (copied from inflight.offsite)" in out
        assert "tank/tree/b: anchor.offsite2 set to B1 (copied from anchor.offsite)" in out
        assert "tank/tree/b/deep: inflight.offsite2 set to d1,d2 (copied from inflight.offsite)" in out
        summaries = [l for l in out.splitlines() if "descendant(s)" in l]
        assert summaries == [
            "4 descendant(s) of 'tank/tree' send to 'offsite'; 3 had an anchor.offsite or inflight.offsite to copy"
        ]

    def test_a_child_with_nothing_to_copy_prints_nothing(self, run, tree):
        out = self.rename(run, tree).stdout
        assert "tank/tree/empty" not in out
        before_summary = out.split("descendant(s)")[0]
        # a child with an anchor and no pin (or a pin and no anchor) is reported for what it has, as a configured one is
        assert [l for l in before_summary.splitlines() if "tank/tree/b:" in l] == [
            "tank/tree/b: anchor.offsite2 set to B1 (copied from anchor.offsite)"
        ]
        assert "tank/tree: has no" not in out

    def test_a_configured_dataset_with_nothing_to_copy_still_says_so(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree", {})
        tree_db(run, db)
        out = self.rename(run, db).stdout
        assert "tank/tree: has no anchor.offsite to copy" in out

    def test_the_old_properties_are_listed_for_clearing_children_included_and_never_run(self, run, tree, fake_zfs):
        out = self.rename(run, tree).stdout
        anchors = [l.strip() for l in out.splitlines() if l.strip().startswith(f"zfs inherit -S {ANCHOR}offsite ")]
        assert anchors == [
            f"zfs inherit -S {ANCHOR}offsite tank/tree",
            f"zfs inherit -S {ANCHOR}offsite tank/tree/a",
            f"zfs inherit -S {ANCHOR}offsite tank/tree/b",
        ]
        pins = [l.strip() for l in out.splitlines() if l.strip().startswith(f"zfs inherit -S {INFLIGHT}offsite ")]
        assert pins == [
            f"zfs inherit -S {INFLIGHT}offsite tank/tree/a",
            f"zfs inherit -S {INFLIGHT}offsite tank/tree/b/deep",
        ]
        assert "tank/tree/empty" not in out.split("A running daemon reloads")[1]
        assert fake_zfs.calls_of("inherit") == []              # the tool never clears the old ones

    def test_a_differing_local_new_value_on_a_child_is_overwritten_and_restorable(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree/a", {
            ANCHOR + "offsite": "A1", ANCHOR + "offsite2": "A0", INFLIGHT + "offsite": "a1,a2",
        })
        fake_zfs.fail("set", "tank/tree/b")
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 1
        err = result.stderr
        assert "copying org.zfsbackup:anchor.offsite to anchor.offsite2 on 'tank/tree/b' failed" in err
        tail = err.split("To put the previous values back run:")[1]
        assert f"zfs set {ANCHOR}offsite2=A0 tank/tree/a" in tail          # overwritten: set the old value back
        assert f"zfs inherit -S {ANCHOR}offsite2 tank/tree\n" in tail                # the parent: newly set
        assert f"zfs inherit -S {INFLIGHT}offsite2 tank/tree/a" in tail      # newly set pin
        assert "tank/tree/b" not in tail and "deep" not in tail             # not reached
        assert "Nothing was written to the configuration" in err
        assert "offsite" in stored(db).destinations and "offsite2" not in stored(db).destinations

    def test_the_restore_commands_after_a_child_failure_really_restore(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.fail("set", "tank/tree/b")
        tree_db(run, db)
        err = self.rename(run, db).stderr
        commands = [
            l.strip() for l in err.split("To put the previous values back run:")[1].splitlines()
            if l.strip().startswith("zfs ")
        ]
        assert sorted(commands) == sorted([
            f"zfs inherit -S {ANCHOR}offsite2 tank/tree",
            f"zfs inherit -S {ANCHOR}offsite2 tank/tree/a",
            f"zfs inherit -S {INFLIGHT}offsite2 tank/tree/a",
        ])
        for command in commands:
            subprocess.run([str(fake_zfs.wrapper), *shlex.split(command)[1:]], check=True, env=dict(os.environ))
        for name in ("tank/tree", "tank/tree/a"):
            assert not any(p.endswith("offsite2") for p in fake_zfs.props(name))

    def test_a_commit_that_fails_after_the_copies_gives_the_children_restore_commands(
        self, run, tree, fake_zfs, monkeypatch
    ):
        def busy(*args, **kwargs):
            raise ConfigBusy("database is locked by another process")

        monkeypatch.setattr(rename, "commit_config", busy)
        result = self.rename(run, tree)
        assert result.exit_code == 75
        tail = result.stderr.split("To put the previous values back run:")[1]
        for name in ("tank/tree/a", "tank/tree/b", "tank/tree/b/deep"):
            assert name in tail
        assert "empty" not in tail
        assert "offsite2" not in stored(tree).destinations

    # -- dry run ----------------------------------------------------------

    def test_a_dry_run_reads_the_children_and_changes_nothing(self, run, tree, fake_zfs):
        state, gen = db_state(tree), generation(tree)
        result = self.rename(run, tree, "--dry-run")
        assert result.exit_code == 0, result.stderr
        assert "[dry run] tank/tree/a: anchor.offsite2 would be set to A1 (copied from anchor.offsite)" in result.stdout
        assert "[dry run] tank/tree/b/deep: inflight.offsite2 would be set to d1,d2" in result.stdout
        assert (
            "[dry run] 4 descendant(s) of 'tank/tree' send to 'offsite'; 3 had an anchor.offsite or "
            "inflight.offsite to copy"
        ) in result.stdout
        assert db_state(tree) == state and generation(tree) == gen
        assert fake_zfs.mutations == []

    # -- a parent whose listing failed -----------------------------------

    @pytest.mark.parametrize("failing", ["list", "get"])
    def test_a_failed_listing_does_not_stop_the_rename_and_leaves_the_commands_and_a_reminder(
        self, run, db, fake_zfs, failing
    ):
        grow_tree(fake_zfs)
        fake_zfs.fail(failing, "tank/tree", "-r")
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert "offsite2" in stored(db).destinations                     # the rename is done
        assert fake_zfs.props("tank/tree")[ANCHOR + "offsite2"] == "P0"    # the parent itself was copied
        assert not any(p.endswith("offsite2") for n in ("tank/tree/a", "tank/tree/b") for p in fake_zfs.props(n))
        (warned,) = [w for w in warnings_in(result) if "could not be listed" in w]
        assert "the descendants of 'tank/tree' could not be listed" in warned
        err = result.stderr
        assert f"zfs get -r -H -s local -o name,property,value {ANCHOR}offsite,{INFLIGHT}offsite tank/tree" in err
        assert f"{ANCHOR}offsite2=<value>" in err and f"{INFLIGHT}offsite2=<value>" in err
        # the reminder comes after the commit message
        shown = interleaved(result)
        reminder = shown.index("NOT handled: the anchors of the descendants of 'tank/tree'")
        assert shown.index("Renamed destination 'offsite' -> 'offsite2'") < reminder
        assert "the warning above has the commands" in shown[reminder:]

    def test_a_failed_listing_is_reminded_in_a_dry_run_too(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.fail("list", "tank/tree", "-r")
        tree_db(run, db)
        state = db_state(db)
        result = self.rename(run, db, "-d")
        assert result.exit_code == 0, result.stderr
        assert any("could not be listed" in w for w in warnings_in(result))
        assert "[dry run] NOT handled: the anchors of the descendants of 'tank/tree'" in result.stderr
        assert "[dry run] would rename destination 'offsite' -> 'offsite2'" in result.stdout
        assert "descendant(s)" not in result.stdout
        assert db_state(db) == state and fake_zfs.mutations == []

    def test_a_failed_listing_of_one_parent_leaves_the_other_parents_children_copied(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/two", {ANCHOR + "offsite": "T0"})
        fake_zfs.add("tank/two/c", {ANCHOR + "offsite": "C1"})
        fake_zfs.fail("list", "tank/two", "-r")
        tree_db(run, db, dataset_block("tank/two", recursive=True))
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.props("tank/tree/a")[ANCHOR + "offsite2"] == "A1"
        assert ANCHOR + "offsite2" not in fake_zfs.props("tank/two/c")
        assert "could not be listed" in result.stderr and "'tank/two'" in result.stderr
        assert "'tank/tree'" not in result.stderr.split("NOT handled")[-1]

    # -- who owns which name ----------------------------------------------

    def test_a_disabled_configured_subtree_is_not_copied_from_the_parent(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree/off", {ANCHOR + "offsite": "O0"})            # configured, disabled: R-1
        fake_zfs.add("tank/tree/off/x", {ANCHOR + "offsite": "X1"})
        fake_zfs.add("tank/tree/off/x/y", {INFLIGHT + "offsite": "y1,y2"})
        tree_db(run, db, dataset_block("tank/tree/off", enabled=False, remote=()))
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        for name in ("tank/tree/off", "tank/tree/off/x", "tank/tree/off/x/y"):
            assert not any(p.endswith("offsite2") for p in fake_zfs.props(name)), name
        assert "tank/tree/off" not in result.stdout
        assert fake_zfs.props("tank/tree/a")[ANCHOR + "offsite2"] == "A1"     # the rest still copied
        assert "4 descendant(s) of 'tank/tree'" in result.stdout

    def test_a_nested_configured_dataset_is_copied_once_as_itself_not_as_an_expanded_child(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree/cfg", {ANCHOR + "offsite": "C0"})
        tree_db(run, db, dataset_block("tank/tree/cfg"))
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.props("tank/tree/cfg")[ANCHOR + "offsite2"] == "C0"
        assert len([c for c in fake_zfs.calls_of("set") if c[-1] == "tank/tree/cfg"]) == 1
        assert result.stdout.count("tank/tree/cfg: anchor.offsite2 set to C0") == 1
        assert "4 descendant(s) of 'tank/tree'" in result.stdout          # a..empty, not cfg

    def test_a_nested_enabled_recursive_dataset_with_no_remote_ends_the_walk(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree/own", {ANCHOR + "offsite": "W0"})
        fake_zfs.add("tank/tree/own/z", {ANCHOR + "offsite": "Z1"})
        fake_zfs.add("tank/tree/own/z/zz", {INFLIGHT + "offsite": "q1,q2"})
        tree_db(run, db, dataset_block("tank/tree/own", recursive=True, remote=()))
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        for name in ("tank/tree/own", "tank/tree/own/z", "tank/tree/own/z/zz"):
            assert not any(p.endswith("offsite2") for p in fake_zfs.props(name)), name
        assert "4 descendant(s) of 'tank/tree'" in result.stdout

    def test_a_nested_recursive_dataset_sending_elsewhere_owns_its_subtree(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree/elsewhere", {})
        fake_zfs.add("tank/tree/elsewhere/z", {ANCHOR + "offsite": "Z1"})
        tree_db(run, db, dataset_block("tank/tree/elsewhere", recursive=True, remote=("spare",)))
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert ANCHOR + "offsite2" not in fake_zfs.props("tank/tree/elsewhere/z")
        assert {c[-1] for c in recursive_calls(fake_zfs)} == {"tank/tree"}     # never listed from `elsewhere`

    def test_a_nested_recursive_dataset_that_sends_to_old_is_its_own_parent_with_its_own_summary(
        self, run, db, fake_zfs
    ):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree/sub", {ANCHOR + "offsite": "S0"})
        fake_zfs.add("tank/tree/sub/m", {ANCHOR + "offsite": "M1"})
        fake_zfs.add("tank/tree/sub/n", {})
        tree_db(run, db, dataset_block("tank/tree/sub", recursive=True))
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.props("tank/tree/sub/m")[ANCHOR + "offsite2"] == "M1"
        assert fake_zfs.props("tank/tree/sub")[ANCHOR + "offsite2"] == "S0"         # configured: copied once
        summaries = [l for l in result.stdout.splitlines() if "descendant(s)" in l]
        assert summaries == [
            "4 descendant(s) of 'tank/tree' send to 'offsite'; 3 had an anchor.offsite or inflight.offsite to copy",
            "2 descendant(s) of 'tank/tree/sub' send to 'offsite'; 1 had an anchor.offsite or inflight.offsite to copy",
        ]
        assert len([c for c in fake_zfs.calls_of("set") if c[-1] == "tank/tree/sub/m"]) == 1

    def test_a_recursive_dataset_that_does_not_send_to_old_is_not_listed(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/spare", {})
        fake_zfs.add("tank/spare/c", {ANCHOR + "offsite": "stray"})
        tree_db(run, db, dataset_block("tank/spare", recursive=True, remote=("spare",)))
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert {c[-1] for c in recursive_calls(fake_zfs, "list")} == {"tank/tree"}
        assert {c[-1] for c in recursive_calls(fake_zfs, "get")} == {"tank/tree"}
        assert ANCHOR + "offsite2" not in fake_zfs.props("tank/spare/c")

    def test_a_disabled_recursive_dataset_sending_to_old_is_not_listed(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/dis", {ANCHOR + "offsite": "D0"})
        fake_zfs.add("tank/dis/c", {ANCHOR + "offsite": "C1"})
        tree_db(run, db, dataset_block("tank/dis", recursive=True, enabled=False))
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert {c[-1] for c in recursive_calls(fake_zfs, "list")} == {"tank/tree"}
        assert ANCHOR + "offsite2" not in fake_zfs.props("tank/dis/c")

    # -- --skip-missing ---------------------------------------------------

    def test_a_missing_recursive_parent_is_not_expanded_under_skip_missing(self, run, db, fake_zfs):
        fake_zfs.add("tank/tree/a", {ANCHOR + "offsite": "A1"})              # no tank/tree itself
        tree_db(run, db)
        state = db_state(db)
        refused = self.rename(run, db)
        assert refused.exit_code == 1 and "'tank/tree' send(s) to 'offsite' but does not exist" in refused.stderr
        assert db_state(db) == state and fake_zfs.mutations == []
        result = self.rename(run, db, "--skip-missing")
        assert result.exit_code == 0, result.stderr
        assert any("dataset 'tank/tree' does not exist in ZFS" in w for w in warnings_in(result))
        assert recursive_calls(fake_zfs, "list") == [] and recursive_calls(fake_zfs, "get") == []
        assert fake_zfs.mutations == []
        assert "descendant(s)" not in result.stdout
        assert "offsite2" in stored(db).destinations

    def test_skip_missing_still_copies_the_children_of_a_parent_that_exists(self, run, db, fake_zfs):
        grow_tree(fake_zfs)                                                    # tank/other is missing
        tree_db(run, db, dataset_block("tank/other"))
        result = self.rename(run, db, "--skip-missing")
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.props("tank/tree/a")[ANCHOR + "offsite2"] == "A1"
        assert any("dataset 'tank/other' does not exist" in w for w in warnings_in(result))

    # -- backup copies, received and inherited values ----------------------

    @pytest.mark.parametrize("source", ["local", "received"])
    @pytest.mark.parametrize("prop", ["org.zfsbackup:client_id", "org.zfsbackup:source_dataset"])
    def test_a_backup_copy_below_the_parent_is_not_touched(self, run, db, fake_zfs, source, prop):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree/copy", {ANCHOR + "offsite": "K0"})
        fake_zfs.set_prop("tank/tree/copy", prop, "host-9" if "client" in prop else "tank/src", source)
        fake_zfs.add("tank/tree/copy/x", {ANCHOR + "offsite": "K1", INFLIGHT + "offsite": "k1,k2"})
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        for name in ("tank/tree/copy", "tank/tree/copy/x"):
            assert not any(p.endswith("offsite2") for p in fake_zfs.props(name)), name
        assert "tank/tree/copy" not in result.stdout
        assert fake_zfs.props("tank/tree/a")[ANCHOR + "offsite2"] == "A1"     # its siblings still are
        assert "4 descendant(s) of 'tank/tree'" in result.stdout

    def test_a_parent_inside_a_copy_expands_to_nothing(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.set_prop("tank/tree", "org.zfsbackup:client_id", "host-9", "inherited from tank")
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.props("tank/tree/a").get(ANCHOR + "offsite2") is None
        assert "descendant(s)" not in result.stdout

    def test_received_or_inherited_anchors_and_pins_are_not_copied(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree/rcv", {})
        fake_zfs.set_prop("tank/tree/rcv", ANCHOR + "offsite", "R1", "received")
        fake_zfs.set_prop("tank/tree/rcv", INFLIGHT + "offsite", "r1,r2", "received")
        fake_zfs.add("tank/tree/inh", {})
        fake_zfs.set_prop("tank/tree/inh", ANCHOR + "offsite", "I1", "inherited from tank/tree")
        fake_zfs.set_prop("tank/tree/inh", INFLIGHT + "offsite", "i1,i2", "inherited from tank/tree")
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        for name in ("tank/tree/rcv", "tank/tree/inh"):
            assert not any(c[-1] == name for c in fake_zfs.calls_of("set")), name
            assert not any(p.endswith("offsite2") for p in fake_zfs.props(name)), name
        assert "tank/tree/rcv" not in result.stdout and "tank/tree/inh" not in result.stdout
        assert "6 descendant(s) of 'tank/tree' send to 'offsite'; 3 had" in result.stdout

    def test_a_received_anchor_on_a_configured_dataset_is_not_copied(self, run, db, fake_zfs):
        """The intended local-only `copy_anchor_property` (A2b): a received anchor is another
        host's progress and a rename must not make it this host's own evidence."""
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree", {})
        fake_zfs.set_prop("tank/tree", ANCHOR + "offsite", "RCV", "received")
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert not any(c[-1] == "tank/tree" for c in fake_zfs.calls_of("set"))
        assert "tank/tree: has no anchor.offsite to copy" in result.stdout

    def test_a_received_pin_on_a_configured_dataset_is_not_copied(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.add("tank/tree", {})
        fake_zfs.set_prop("tank/tree", INFLIGHT + "offsite", "r1,r2", "received")
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert not any(c[-1] == "tank/tree" for c in fake_zfs.calls_of("set"))
        assert "tank/tree: inflight" not in result.stdout

    def test_a_received_new_value_is_not_an_overwrite_and_a_local_one_replaces_it(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.set_prop("tank/tree/a", ANCHOR + "offsite2", "RCV", "received")
        fake_zfs.set_prop("tank/tree/a", INFLIGHT + "offsite2", "x,y", "received")
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        # not "was RCV, now A1": the received value is not the value being replaced
        assert "tank/tree/a: anchor.offsite2 set to A1 (copied from anchor.offsite)" in result.stdout
        assert "was RCV" not in result.stdout and "was x,y" not in result.stdout
        assert fake_zfs.value("tank/tree/a", ANCHOR + "offsite2") == "A1"
        assert fake_zfs.source("tank/tree/a", ANCHOR + "offsite2") == "local"

    def test_a_received_new_value_is_restored_by_inherit_dash_S_and_it_really_comes_back(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.set_prop("tank/tree/a", ANCHOR + "offsite2", "RCV", "received")
        fake_zfs.fail("set", "tank/tree/b")
        tree_db(run, db)
        err = self.rename(run, db).stderr
        tail = err.split("To put the previous values back run:")[1]
        assert f"zfs inherit -S {ANCHOR}offsite2 tank/tree/a" in tail
        assert "zfs set" not in tail
        # the rename's own copy replaced the received value with a local one (the received one stays underneath) ...
        assert fake_zfs.value("tank/tree/a", ANCHOR + "offsite2") == "A1"
        assert fake_zfs.source("tank/tree/a", ANCHOR + "offsite2") == "local"
        for line in tail.splitlines():
            if line.strip().startswith("zfs "):
                subprocess.run(
                    [str(fake_zfs.wrapper), *shlex.split(line.strip())[1:]], check=True, env=dict(os.environ)
                )
        # ... and the printed commands bring the RECEIVED value back (a plain `inherit` would have hidden it)
        assert fake_zfs.value("tank/tree/a", ANCHOR + "offsite2") == "RCV"
        assert fake_zfs.source("tank/tree/a", ANCHOR + "offsite2") == "received"
        assert fake_zfs.value("tank/tree", ANCHOR + "offsite2") is None            # newly set: gone

    # -- the writer lock and the generation check, unchanged ---------------

    def test_the_writer_lock_is_held_during_every_copy_children_included(self, run, tree, monkeypatch):
        import zfsbackup.backup_manager as bm

        lock_path = writer_lock_path(resolve_config_path(str(tree)))
        probes = []
        real = bm.copy_anchor_property

        def probing(*args, **kwargs):
            probes.append(_try_exclusive_lock(lock_path))
            return real(*args, **kwargs)

        monkeypatch.setattr(bm, "copy_anchor_property", probing)
        assert self.rename(run, tree).exit_code == 0
        assert probes == ["blocked"] * 5                      # the parent and its four children
        assert _try_exclusive_lock(lock_path) == "acquired"

    def test_the_commit_gets_the_held_writer_lock_and_the_lock_is_released_after(self, run, tree, monkeypatch):
        seen = {}
        real = rename.commit_config

        def spy(resolved, snapshot, config, **kwargs):
            seen["lock"] = kwargs.get("lock")
            return real(resolved, snapshot, config, **kwargs)

        monkeypatch.setattr(rename, "commit_config", spy)
        assert self.rename(run, tree).exit_code == 0
        assert seen["lock"] == writer_lock_path(resolve_config_path(str(tree)))
        assert run("set", "check_interval", "10m", "-c", tree).exit_code == 0

    def test_a_generation_that_moved_before_the_lock_is_75_and_no_child_is_touched(
        self, run, tree, fake_zfs, monkeypatch
    ):
        done = move_generation_before_the_lock(monkeypatch, tree)
        result = self.rename(run, tree)
        assert done == [True]
        assert result.exit_code == 75, result.stderr
        assert "config changed underneath you" in result.stderr
        assert fake_zfs.mutations == []
        assert "To put the previous values back" not in result.stderr
        assert "offsite2" not in stored(tree).destinations

    def test_the_children_are_read_before_the_lock_and_nothing_is_written_by_a_listing(self, run, tree, fake_zfs):
        """The expansion is name-only reads: no `set`/`inherit` outside the copy, and no value read
        with `-s` other than the two exact LOCAL reads per property."""
        assert self.rename(run, tree, "-d").exit_code == 0
        assert fake_zfs.mutations == []
        local_reads = [c for c in fake_zfs.calls_of("get") if c[:5] == ["get", "-H", "-o", "value", "-s"]]
        assert local_reads and all(c[5] == "local" for c in local_reads)

    def test_a_non_recursive_configuration_makes_no_recursive_call(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        yaml_text = RENAME_YAML.replace("    recursive: true\n", "")
        make_db_path = rich.parent / "plain.db"
        make_db(run, make_db_path, yaml_text, name="plain.yaml")
        result = run("rename", "destination", "offsite", "offsite2", "-c", make_db_path)
        assert result.exit_code == 0, result.stderr
        assert recursive_calls(fake_zfs, "list") == [] and recursive_calls(fake_zfs, "get") == []
        assert "descendant(s)" not in result.stdout


# --------------------------------------------------------------------------
# A4: `rename dataset` notes for an enabled recursive dataset's descendants
# --------------------------------------------------------------------------

CID_NOTE = "host-1"

LOCAL_TREE_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/tree"
    frequency: "1h"
    recursive: true
    retention:
      "1d": "30d"
    remote:
      - destination: "loc"
  - name: "tank/flat"
    frequency: "1h"
    retention:
      "1d": "30d"
    remote:
      - destination: "loc"
      - destination: "offsite"
  - name: "tank/off"
    enabled: false
    frequency: "1h"
    recursive: true
    retention:
      "1d": "30d"
    remote:
      - destination: "offsite"
destinations:
  loc:
    url: "zfs:backup/local"
  offsite:
    url: "https://offsite:8080"
"""


class TestFakeZfsInheritDashS:
    """The fake's `inherit [-r] [-S]` (the CLI's restore and clearing advice is executed against it)."""

    def zfs(self, fake, *argv):
        subprocess.run([str(fake.wrapper), *argv], check=True, env=dict(os.environ))

    def test_set_over_a_received_value_keeps_it_underneath(self, fake_zfs):
        fake_zfs.set_prop("tank/x", ANCHOR + "d", "RCV", "received")
        self.zfs(fake_zfs, "set", f"{ANCHOR}d=LOC", "tank/x")
        assert (fake_zfs.value("tank/x", ANCHOR + "d"), fake_zfs.source("tank/x", ANCHOR + "d")) == ("LOC", "local")

    def test_dash_S_reverts_a_local_value_to_the_received_one(self, fake_zfs):
        fake_zfs.set_prop("tank/x", ANCHOR + "d", "LOC", received="RCV")
        self.zfs(fake_zfs, "inherit", "-S", ANCHOR + "d", "tank/x")
        assert (fake_zfs.value("tank/x", ANCHOR + "d"), fake_zfs.source("tank/x", ANCHOR + "d")) == ("RCV", "received")

    def test_dash_S_leaves_a_received_only_value_alone(self, fake_zfs):
        fake_zfs.set_prop("tank/x", ANCHOR + "d", "RCV", "received")
        self.zfs(fake_zfs, "inherit", "-S", ANCHOR + "d", "tank/x")
        assert (fake_zfs.value("tank/x", ANCHOR + "d"), fake_zfs.source("tank/x", ANCHOR + "d")) == ("RCV", "received")

    def test_dash_S_without_a_received_value_is_a_plain_inherit(self, fake_zfs):
        fake_zfs.set_prop("tank/x", ANCHOR + "d", "LOC")
        self.zfs(fake_zfs, "inherit", "-S", ANCHOR + "d", "tank/x")
        assert fake_zfs.value("tank/x", ANCHOR + "d") is None

    def test_a_plain_inherit_hides_the_received_value(self, fake_zfs):
        fake_zfs.set_prop("tank/x", ANCHOR + "d", "LOC", received="RCV")
        self.zfs(fake_zfs, "inherit", ANCHOR + "d", "tank/x")
        assert fake_zfs.value("tank/x", ANCHOR + "d") is None

    def test_the_flags_may_come_before_or_after_the_property(self, fake_zfs):
        for name in ("tank/a", "tank/b"):
            fake_zfs.set_prop(name, ANCHOR + "d", "LOC", received="RCV")
        self.zfs(fake_zfs, "inherit", "-S", ANCHOR + "d", "tank/a")
        self.zfs(fake_zfs, "inherit", ANCHOR + "d", "-S", "tank/b")
        assert fake_zfs.value("tank/a", ANCHOR + "d") == fake_zfs.value("tank/b", ANCHOR + "d") == "RCV"

    def test_dash_r_dash_S_reaches_every_descendant_and_only_them(self, fake_zfs):
        fake_zfs.set_prop("tank/r", ANCHOR + "d", "R0")
        fake_zfs.set_prop("tank/r/c", ANCHOR + "d", "LOC", received="RCV")
        fake_zfs.set_prop("tank/r/c/e", ANCHOR + "d", "RCV2", "received")
        fake_zfs.set_prop("tank/r/c/e/f", ANCHOR + "d", "F0")
        fake_zfs.set_prop("tank/rr", ANCHOR + "d", "NEIGHBOUR")           # a name-prefix sibling, not a descendant
        self.zfs(fake_zfs, "inherit", "-r", "-S", ANCHOR + "d", "tank/r")
        assert fake_zfs.value("tank/r", ANCHOR + "d") is None
        assert (fake_zfs.value("tank/r/c", ANCHOR + "d"), fake_zfs.source("tank/r/c", ANCHOR + "d")) == ("RCV", "received")
        assert fake_zfs.value("tank/r/c/e", ANCHOR + "d") == "RCV2"
        assert fake_zfs.value("tank/r/c/e/f", ANCHOR + "d") is None
        assert fake_zfs.value("tank/rr", ANCHOR + "d") == "NEIGHBOUR"

    def test_the_recursive_property_listing_prints_only_the_requested_properties(self, fake_zfs):
        fake_zfs.set_prop("tank/r", "org.zfsbackup:client_id", "h", "received")
        fake_zfs.set_prop("tank/r", ANCHOR + "d", "x")
        got = subprocess.run(
            [str(fake_zfs.wrapper), "get", "-H", "-o", "name,property,source", "-s", "local,received,inherited",
             "-r", "-t", "filesystem,volume", "org.zfsbackup:client_id,org.zfsbackup:source_dataset", "tank/r"],
            capture_output=True, text=True, env=dict(os.environ), check=True,
        ).stdout.splitlines()
        assert got == ["tank/r\torg.zfsbackup:client_id\treceived"]


class TestRenameDestinationExpansionRecords:
    """`_expansion_records_as_warnings`: `expand_recursive`'s daemon-worded log records, re-voiced."""

    LOGGER = "zfsbackup.backup_manager"

    @pytest.fixture
    def tree(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        return tree_db(run, db)

    def rename(self, run, database, *extra):
        return run("rename", "destination", "offsite", "offsite2", *extra, "-c", database)

    @pytest.fixture
    def logger(self):
        import logging

        log = logging.getLogger(self.LOGGER)
        before = (log.level, list(log.filters), list(log.handlers))
        log.setLevel(logging.DEBUG)

        class Collect(logging.Handler):
            def __init__(self):
                super().__init__()
                self.records = []

            def emit(self, record):
                self.records.append((record.levelname, record.getMessage()))

        handler = Collect()
        log.addHandler(handler)
        log.handler = handler
        yield log
        log.removeHandler(handler)
        log.setLevel(before[0])
        assert list(log.filters) == before[1]            # nothing may leak out of a test either

    # -- the CLI end to end ------------------------------------------------

    def copy_below(self, fake_zfs, *names):
        for name in names:
            fake_zfs.add(name, {ANCHOR + "offsite": "K0"})
            fake_zfs.set_prop(name, "org.zfsbackup:client_id", "host-9")

    def test_a_skipped_backup_copy_is_a_warning_naming_the_consequence_never_an_error(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        self.copy_below(fake_zfs, "tank/tree/copy")
        tree_db(run, db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        lines = stderr_lines(result)
        (shown,) = [l for l in lines if l.startswith("warning: while looking for the descendants that send to 'offsite': ")]
        assert "tank/tree/copy" in shown
        assert shown.endswith(
            "this rename copies no anchor.offsite or inflight.offsite (to 'offsite2') for the datasets skipped this way"
        )
        assert not [l for l in lines if l.startswith("error:")]
        assert not any("Cannot list" in l for l in lines)
        assert "warning: tank/tree/copy" not in result.stderr

    def test_each_skipped_copy_root_is_reported_once(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        self.copy_below(fake_zfs, "tank/tree/copy", "tank/tree/copy2")
        tree_db(run, db)
        result = self.rename(run, db)
        shown = [l for l in stderr_lines(result) if "while looking for the descendants" in l]
        assert len(shown) == 2 and not [l for l in stderr_lines(result) if l.startswith("error:")]

    @pytest.mark.parametrize("failing", ["list", "get"])
    def test_a_failed_listing_is_reported_exactly_once_and_as_no_error(self, run, db, fake_zfs, failing):
        grow_tree(fake_zfs)
        fake_zfs.fail(failing, "tank/tree", "-r")
        tree_db(run, db)
        result = self.rename(run, db)
        lines = stderr_lines(result)
        assert result.exit_code == 0, result.stderr
        assert count_containing(lines, "could not be listed") == 1                   # the `_unlisted_text` one
        assert count_containing(lines, "Cannot list the descendants") == 0         # the daemon's ERROR is dropped
        assert count_containing(lines, "while looking for the descendants") == 0
        assert count_containing(lines, "are not sent until this passes") == 0
        assert not [l for l in lines if l.startswith("error:")]

    def test_the_failed_listing_outcome_is_not_handled_and_still_exits_zero(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.fail("list", "tank/tree", "-r")
        tree_db(run, db)
        gen = generation(db)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr                                  # the configuration rename committed
        assert "NOT handled: the anchors of the descendants of 'tank/tree'" in result.stderr
        assert generation(db) == gen + 1 and "offsite2" in stored(db).destinations

    def test_the_filter_is_gone_after_the_command_so_a_later_error_is_printed_normally(self, run, db, fake_zfs, logger):
        grow_tree(fake_zfs)
        fake_zfs.fail("list", "tank/tree", "-r")
        tree_db(run, db)
        assert self.rename(run, db).exit_code == 0
        assert logger.filters == []
        logger.error("a later problem")
        assert ("ERROR", "a later problem") in logger.handler.records

    # -- the context manager itself ---------------------------------------

    def test_inside_the_block_warning_and_error_records_go_to_warn_and_not_to_the_handlers(self, logger):
        import zfsbackup.backup_manager as bm

        warned = []
        with rename._expansion_records_as_warnings(bm, "old", "new", warned.append):
            logger.error("x is at or below t, which receives copies")
            logger.warning("w problem")
            logger.info("fine")
            logger.debug("detail")
        assert warned == [
            "while looking for the descendants that send to 'old': x is at or below t, which receives copies. "
            "this rename copies no anchor.old or inflight.old (to 'new') for the datasets skipped this way",
            "while looking for the descendants that send to 'old': w problem. "
            "this rename copies no anchor.old or inflight.old (to 'new') for the datasets skipped this way",
        ]
        assert logger.handler.records == [("INFO", "fine"), ("DEBUG", "detail")]      # lower levels untouched

    def test_the_listing_failure_error_is_dropped_without_a_warn(self, logger):
        import zfsbackup.backup_manager as bm

        warned = []
        with rename._expansion_records_as_warnings(bm, "old", "new", warned.append):
            logger.error("Cannot list the descendants of tank/tree, so they are not sent until this passes (X: y)")
        assert warned == [] and logger.handler.records == []

    def test_the_filter_is_removed_afterwards_even_when_the_block_raises(self, logger):
        import zfsbackup.backup_manager as bm

        with pytest.raises(RuntimeError):
            with rename._expansion_records_as_warnings(bm, "old", "new", lambda m: None):
                assert len(logger.filters) == 1
                raise RuntimeError("boom")
        assert logger.filters == []
        logger.error("after")
        assert logger.handler.records == [("ERROR", "after")]

    def test_the_filter_only_touches_the_backup_managers_logger(self, logger):
        import logging
        import zfsbackup.backup_manager as bm

        other = logging.getLogger("zfsbackup.other")
        with rename._expansion_records_as_warnings(bm, "old", "new", lambda m: None):
            assert other.filters == [] and logging.getLogger("zfsbackup").filters == []

    def test_warn_none_is_a_no_op(self, logger):
        import zfsbackup.backup_manager as bm

        with rename._expansion_records_as_warnings(bm, "old", "new", None):
            assert logger.filters == []
            logger.error("still printed")
        assert logger.handler.records == [("ERROR", "still printed")]

    def test_sending_descendants_without_warn_leaves_the_records_alone(self, db, fake_zfs, run, logger):
        import zfsbackup.backup_manager as bm

        grow_tree(fake_zfs)
        self.copy_below(fake_zfs, "tank/tree/copy")
        path = tree_db(run, db)
        config = stored(path)
        pairs, failed = rename._sending_descendants(bm, config, "offsite")
        assert failed == {} and "tank/tree/copy" not in {c for _, c in pairs}
        assert any(level == "ERROR" and "tank/tree/copy" in msg for level, msg in logger.handler.records)


class TestUnlistedTextCaveats:
    """What `rename._unlisted_text` tells the operator who must carry the anchors over by hand."""

    TEXT = rename._unlisted_text("old", "new", "tank/my data", "RuntimeError: boom")

    def test_the_inherit_advice_clears_only_a_local_value(self):
        assert "`zfs inherit -S org.zfsbackup:anchor.old <ds>` (`-S org.zfsbackup:inflight.old`; `-S` clears only a local value and leaves a received one alone)" in self.TEXT
        assert "zfs inherit org.zfsbackup:anchor.old" not in self.TEXT.replace("zfs inherit -S", "")

    def test_the_listing_is_said_to_include_backup_copies_to_skip(self):
        assert (
            "The listing also shows the backup copies below 'tank/my data': skip any dataset that is a "
            "backup copy, i.e. one that carries org.zfsbackup:client_id or org.zfsbackup:source_dataset"
        ) in self.TEXT

    def test_the_listing_command_quotes_the_parent_and_names_both_properties(self):
        assert "zfs get -r -H -s local -o name,property,value org.zfsbackup:anchor.old,org.zfsbackup:inflight.old 'tank/my data'" in self.TEXT
        assert "anchor.new=<value>" in self.TEXT and "inflight.new=<value>" in self.TEXT
        assert "(RuntimeError: boom)" in self.TEXT

    def test_the_caveats_reach_the_operator_through_the_cli(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.fail("list", "tank/tree", "-r")
        tree_db(run, db)
        err = run("rename", "destination", "offsite", "offsite2", "-c", db).stderr
        assert "`-S` clears only a local value and leaves a received one alone" in err
        assert "skip any dataset that is a backup copy" in err


class TestRenameDatasetRecursiveNotes:
    @pytest.fixture
    def local_tree(self, run, db):
        make_db(run, db, LOCAL_TREE_YAML)
        seed_client_id(db, CID_NOTE)
        return db

    def refusal(self, run, database, old, new):
        result = run("rename", "dataset", old, new, "-c", database)
        assert result.exit_code == 1, result.stdout
        assert "Nothing was changed." in result.stderr
        return " ".join(result.stderr.split())

    def test_a_server_note_says_the_children_move_with_the_parent_copy_and_need_relabelling(self, run, rich):
        seed_client_id(rich, CID_NOTE)
        err = self.refusal(run, rich, "tank/data", "tank/data2")
        assert f"the server keeps this dataset at <server target>/{CID_NOTE}/tank/data;" in err
        assert (
            f"its descendants are sent as copies of their own, below <server target>/{CID_NOTE}/tank/data/, "
            "so on the receiver they move with the parent copy's `zfs rename` (they are below it)"
        ) in err
        assert "org.zfsbackup:source_dataset=tank/data/<child>, which must read tank/data2/<child> there" in err

    def test_a_local_note_with_the_same_parent_says_to_relabel_every_child(self, run, local_tree):
        err = self.refusal(run, local_tree, "tank/tree", "tank/tree2")
        assert (
            "its descendants are sent to 'loc' as copies of their own, below "
            f"backup/local/{CID_NOTE}/tank/tree/, so they move with the parent copy's `zfs rename` "
            "(they are below it); then re-label each one, for every descendant <child> (the part of its "
            "name after tank/tree2/): `zfs set org.zfsbackup:source_dataset=tank/tree2/<child> "
            f"backup/local/{CID_NOTE}/tank/tree2/<child>`"
        ) in err
        assert f"`zfs rename backup/local/{CID_NOTE}/tank/tree backup/local/{CID_NOTE}/tank/tree2`" in err
        assert "<server target>" not in err

    def test_a_local_note_with_a_changed_parent_says_the_children_are_resent_in_full(self, run, local_tree):
        err = self.refusal(run, local_tree, "tank/tree", "pool2/tree")
        assert (
            "its descendants are sent to 'loc' as copies of their own, below "
            f"backup/local/{CID_NOTE}/tank/tree/; they are sent in full into "
            f"backup/local/{CID_NOTE}/pool2/tree/ and the copies below backup/local/{CID_NOTE}/tank/tree/ "
            "stay with the orphaned parent copy"
        ) in err
        assert "re-label" not in err and "zfs rename" not in err

    def test_a_dataset_with_both_kinds_of_destination_gets_the_children_note_per_kind(self, run, db):
        make_db(run, db, LOCAL_TREE_YAML.replace(
            '  - name: "tank/flat"\n', '  - name: "tank/flat"\n    recursive: true\n'))
        seed_client_id(db, CID_NOTE)
        err = self.refusal(run, db, "tank/flat", "tank/flat2")
        assert err.count("its descendants are sent") == 2
        assert "below <server target>" in err and "below backup/local/" in err

    def test_a_non_recursive_dataset_has_no_children_note(self, run, local_tree):
        err = self.refusal(run, local_tree, "tank/flat", "tank/flat2")
        assert "its descendants are sent" not in err
        assert "local destination 'loc'" in err and "<server target>" in err

    def test_a_disabled_recursive_dataset_has_no_children_note(self, run, local_tree):
        result = run("rename", "dataset", "tank/off", "tank/off2", "--force", "-c", local_tree)
        assert result.exit_code == 0, result.stderr
        assert "its descendants are sent" not in result.stderr
        assert "tank/off2" in names(local_tree)

    def test_force_carries_the_children_note_in_the_warning(self, run, local_tree):
        result = run("rename", "dataset", "tank/tree", "tank/tree2", "--force", "-c", local_tree)
        assert result.exit_code == 0, result.stderr
        (warned,) = [w for w in warnings_in(result) if w.startswith("warning: --force:")]
        assert "its descendants are sent to 'loc' as copies of their own" in warned
        assert "tank/tree2" in names(local_tree)


class TestRenameDatasetLeftInPlaceReceived:
    """`--clear-stale-props`: an anchor or pin with no LOCAL value (received only) is another host's
    progress, which the pruner still honours. It is not inherited (no `zfs inherit` runs for it),
    not reported as cleared, and is listed on its own `Left in place on ...` line; a LOCAL value (also
    one with a received value underneath) goes through `inherit -S`; `config` stays a plain inherit."""

    LEFT = ("Left in place on 'tank/vm.01' because they are received (another host's values, "
            "which the pruner still honours): ")

    @pytest.fixture
    def pool(self, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg", "compression": "lz4"})
        fake_zfs.add("tank/vm.02", {})
        return fake_zfs

    def clear(self, run, rich, *extra):
        return run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", *extra, "-c", rich)

    def inherited(self, fake):
        return [inherit_property(c) for c in fake.calls if c[0] == "inherit"]

    def test_a_received_only_anchor_and_pin_are_listed_and_never_inherited(self, run, rich, pool):
        pool.set_prop("tank/vm.01", ANCHOR + "offsite", "RA", "received")
        pool.set_prop("tank/vm.01", INFLIGHT + "offsite", "r1,r2", "received")
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        assert self.inherited(pool) == [CONFIG_PROP]                       # not even attempted
        assert [pool.value("tank/vm.01", p) for p in (ANCHOR + "offsite", INFLIGHT + "offsite")] == ["RA", "r1,r2"]
        assert [pool.source("tank/vm.01", p) for p in (ANCHOR + "offsite", INFLIGHT + "offsite")] == ["received"] * 2
        lines = result.stdout.splitlines()
        assert "Cleared on 'tank/vm.01': " + CONFIG_PROP in lines
        assert self.LEFT + f"{ANCHOR}offsite, {INFLIGHT}offsite" in lines

    def test_the_left_in_place_line_follows_the_cleared_line(self, run, rich, pool):
        pool.set_prop("tank/vm.01", ANCHOR + "offsite", "RA", "received")
        lines = self.clear(run, rich).stdout.splitlines()
        cleared = next(i for i, l in enumerate(lines) if l.startswith("Cleared on 'tank/vm.01':"))
        assert lines[cleared + 1] == self.LEFT + f"{ANCHOR}offsite"

    def test_only_received_properties_leave_nothing_cleared_but_still_say_so(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {})
        fake_zfs.set_prop("tank/vm.01", ANCHOR + "offsite", "RA", "received")
        fake_zfs.add("tank/vm.02", {})
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "Cleared on 'tank/vm.01': (nothing set)" in result.stdout
        assert self.LEFT + f"{ANCHOR}offsite" in result.stdout
        assert self.inherited(fake_zfs) == []

    def test_a_local_value_is_inherited_with_dash_S_and_the_received_value_comes_back(self, run, rich, pool):
        pool.set_prop("tank/vm.01", ANCHOR + "offsite", "LOCAL", received="RA")
        pool.set_prop("tank/vm.01", INFLIGHT + "offsite", "l1,l2", received="r1,r2")
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        calls = [c for c in pool.calls if c[0] == "inherit" and inherit_property(c) != CONFIG_PROP]
        assert calls == [["inherit", "-S", ANCHOR + "offsite", "tank/vm.01"],
                         ["inherit", "-S", INFLIGHT + "offsite", "tank/vm.01"]]
        for prop, value in ((ANCHOR + "offsite", "RA"), (INFLIGHT + "offsite", "r1,r2")):
            assert (pool.value("tank/vm.01", prop), pool.source("tank/vm.01", prop)) == (value, "received")
        # a local value was there, so both are "cleared" and nothing is "left in place"
        assert f"Cleared on 'tank/vm.01': {ANCHOR}offsite, {CONFIG_PROP}, {INFLIGHT}offsite" in result.stdout
        assert "Left in place" not in result.stdout

    def test_the_config_property_is_a_plain_inherit(self, run, rich, pool):
        pool.set_prop("tank/vm.01", ANCHOR + "offsite", "L")
        self.clear(run, rich)
        (config_call,) = [c for c in pool.calls if c[0] == "inherit" and inherit_property(c) == CONFIG_PROP]
        assert config_call == ["inherit", CONFIG_PROP, "tank/vm.01"]

    def test_a_mix_clears_the_local_and_keeps_the_received(self, run, rich, pool):
        pool.set_prop("tank/vm.01", ANCHOR + "dc.two", "L2")
        pool.set_prop("tank/vm.01", ANCHOR + "offsite", "RA", "received")
        pool.set_prop("tank/vm.01", INFLIGHT + "dc.two", "r1,r2", "received")
        pool.set_prop("tank/vm.01", INFLIGHT + "offsite", "l1,l2")
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        assert f"Cleared on 'tank/vm.01': {ANCHOR}dc.two, {CONFIG_PROP}, {INFLIGHT}offsite" in result.stdout
        assert self.LEFT + f"{ANCHOR}offsite, {INFLIGHT}dc.two" in result.stdout
        assert self.inherited(pool) == [ANCHOR + "dc.two", CONFIG_PROP, INFLIGHT + "offsite"]
        assert pool.props("tank/vm.01").keys() == {
            "compression", ANCHOR + "offsite", INFLIGHT + "dc.two"}

    def test_nothing_received_means_no_left_in_place_line(self, run, rich, pool):
        pool.set_prop("tank/vm.01", ANCHOR + "offsite", "L")
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "Left in place" not in result.stdout

    def test_the_local_read_is_one_exact_get_per_anchor_or_pin_and_none_for_config(self, run, rich, pool):
        pool.set_prop("tank/vm.01", ANCHOR + "offsite", "L")
        pool.set_prop("tank/vm.01", INFLIGHT + "offsite", "a,b", "received")
        self.clear(run, rich)
        reads = [c for c in pool.calls if c[0] == "get" and "-s" in c and "local" in c[c.index("-s") + 1:c.index("-s") + 2]
                 and "value" in c]
        asked = [c[-2] for c in reads]
        assert ANCHOR + "offsite" in asked and INFLIGHT + "offsite" in asked
        assert CONFIG_PROP not in asked

    def test_a_dry_run_changes_nothing_and_prints_no_left_in_place_line(self, run, rich, pool):
        pool.set_prop("tank/vm.01", ANCHOR + "offsite", "RA", "received")
        result = self.clear(run, rich, "-d")
        assert result.exit_code == 0, result.stderr
        assert pool.mutations == [] and "Left in place" not in result.stdout


class TestRenameDestinationNotHandledIsAWarning:
    """Both "NOT handled" reminders (the dry-run one and the one after the commit) are warnings:
    `warning: ...` on stderr, exit status 0."""

    @pytest.fixture
    def broken(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        fake_zfs.fail("list", "tank/tree", "-r")
        return tree_db(run, db)

    def rename(self, run, database, *extra):
        return run("rename", "destination", "offsite", "offsite2", *extra, "-c", database)

    def reminders(self, result):
        return [l for l in stderr_lines(result) if "NOT handled" in l]

    def test_after_the_commit_the_reminder_is_a_warning_line_and_the_exit_is_zero(self, run, broken):
        result = self.rename(run, broken)
        assert result.exit_code == 0, result.stderr
        (line,) = self.reminders(result)
        assert line.startswith("warning: NOT handled: the anchors of the descendants of 'tank/tree'")
        assert not line.startswith("error:")
        assert "offsite2" in stored(broken).destinations

    def test_in_a_dry_run_the_reminder_is_a_warning_line_with_the_prefix_and_the_exit_is_zero(self, run, broken):
        result = self.rename(run, broken, "--dry-run")
        assert result.exit_code == 0, result.stderr
        (line,) = self.reminders(result)
        assert line.startswith("warning: [dry run] NOT handled: the anchors of the descendants of 'tank/tree'")
        assert "offsite2" not in stored(broken).destinations

    def test_there_is_no_reminder_without_a_failed_listing(self, run, db, fake_zfs):
        grow_tree(fake_zfs)
        tree = tree_db(run, db)
        assert self.reminders(self.rename(run, tree, "--dry-run")) == []
        assert self.reminders(self.rename(run, tree)) == []

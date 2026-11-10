"""C0 (decisions X3/C6): a configured dataset may not overlap `remote_backup.target_dataset`.

`zfsbackup/config/store/mapper.py::_require_outside_target`, called from
`validate_config`. The server treats everything under the target as received
backups, so a configured dataset AT or BELOW it would be snapshotted and
pruned inside received data, and a `recursive: true` strict ANCESTOR reaches
the same datasets through `zfs snapshot -r` / `zfs destroy -r`.

Sections:

- ``TestOverlapTable`` -- the decision table, through `validate_config`;
- ``TestComponentsNotStringPrefix`` -- the comparison is by path component
  (the mutation that compares strings fails these);
- ``TestOnlyAnUnsetTargetImposesNothing`` -- `remote_backup` absent or with no
  target (a DISABLED receiver still counts: its former copies stay on disk);
- ``TestWording`` -- what the operator is told;
- ``TestThroughTheCli`` -- `import` exits 1 and writes nothing, for a disabled receiver too;
  `set remote_backup.target_dataset` onto a dataset is refused;
- ``TestThroughTheDaemon`` -- `--test-config` and startup reject it (a database
  that was edited behind the CLI's back, or written before C0), and a
  non-overlapping real config is accepted.

The live-reload rejection lives with the other rejection cases in
`test_zfsbackup_reload.py::TestRejection`.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests
in this file plus `test_zfsbackup_reload.py::TestRejection` and the render
round-trip/through-the-database tests): comparing by STRING prefix instead of
path components, at/below arm 6, ancestor arm 4; the recursive-ancestor arm
removed 8; the "at or below" arm removed 20; the check skipped for a disabled
dataset 3; the call removed from `validate_config` 28. The target counting although
`remote_backup` is disabled (a former server): the check gated on `enabled` again fails
`TestOnlyAnUnsetTargetImposesNothing` and the disabled-receiver tests (re-counted in the
second pass of A2, see `test_zfsbackup_recursive_sends.py`).
"""

import logging
import sqlite3
from unittest.mock import patch

import pytest
from _cli_support import Cli, db, db_state, generation, isolated, stored  # noqa: F401

from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteServerConfig
from zfsbackup.config.store import (
    ConfigSnapshot,
    dispose_all,
    import_yaml,
    resolve_config_path,
    validate_config,
)
from zfsbackup.config.store.mapper import _require_outside_target
from zfsbackup.daemon import main


def config(*datasets, target="tank/backups", enabled=True) -> BackupConfig:
    """`datasets` are `(name, recursive, enabled)` triples (or bare names)."""
    built = []
    for item in datasets:
        name, recursive, ds_enabled = (item, False, True) if isinstance(item, str) else item
        built.append(DatasetConfig(name=name, recursive=recursive, enabled=ds_enabled))
    remote = None if target is None else RemoteServerConfig(target, enabled=enabled)
    return BackupConfig(datasets=built, remote_backup=remote)


def refused(cfg: BackupConfig) -> str:
    with pytest.raises(ValueError) as excinfo:
        validate_config(cfg)
    return str(excinfo.value)


@pytest.mark.unit
class TestOverlapTable:
    @pytest.mark.parametrize(
        "name, recursive, ds_enabled, target",
        [
            pytest.param("tank/backups", False, True, "tank/backups", id="equal"),
            pytest.param("tank/backups/a", False, True, "tank/backups", id="below"),
            pytest.param("tank/backups/a/b/c", False, True, "tank/backups", id="deeply-below"),
            pytest.param("tank/backups/a", False, False, "tank/backups", id="below-and-disabled"),
            pytest.param("tank/backups", False, False, "tank/backups", id="equal-and-disabled"),
            pytest.param("tank/backups/a", True, True, "tank/backups", id="below-and-recursive"),
            pytest.param("tank", True, True, "tank/backups", id="recursive-ancestor"),
            pytest.param("tank", True, False, "tank/backups", id="recursive-ancestor-disabled"),
            pytest.param("tank/x", True, True, "tank/x/y/z", id="recursive-ancestor-two-levels"),
            pytest.param("tank/backups", False, True, "tank", id="target-is-the-pool"),
        ],
    )
    def test_refused(self, name, recursive, ds_enabled, target):
        message = refused(config((name, recursive, ds_enabled), target=target))
        assert message.startswith(f"datasets[{name}]: ")

    @pytest.mark.parametrize(
        "name, recursive, ds_enabled, target",
        [
            pytest.param("tank", False, True, "tank/backups", id="non-recursive-ancestor"),
            pytest.param("tank", False, False, "tank/backups", id="non-recursive-ancestor-disabled"),
            pytest.param("tank/x", False, True, "tank/x/y/z", id="non-recursive-grandparent"),
            pytest.param("tank/other", True, True, "tank/backups", id="recursive-sibling"),
            pytest.param("tank/other/a", True, True, "tank/backups", id="recursive-cousin"),
            pytest.param("rpool", True, True, "tank/backups", id="other-pool"),
            pytest.param("tank/backups-old", False, True, "tank/backups", id="dash-sibling"),
            pytest.param("tank/backup", False, True, "tank/backups", id="name-prefix-of-target"),
            pytest.param("tank/backup", True, True, "tank/backups", id="name-prefix-of-target-recursive"),
            pytest.param("tank/backups", False, True, "tank/backup", id="target-is-a-name-prefix"),
            pytest.param("tank/backups/a", True, True, "tank/backup", id="below-a-name-prefix-sibling"),
            pytest.param("tank/b", True, True, "tank/backups", id="recursive-tank-b"),
            pytest.param("ta", True, True, "tank/backups", id="recursive-pool-name-prefix"),
        ],
    )
    def test_allowed(self, name, recursive, ds_enabled, target):
        validate_config(config((name, recursive, ds_enabled), target=target))  # does not raise

    def test_one_overlapping_dataset_among_valid_ones_is_found(self):
        message = refused(config("tank/a", "tank/b", "tank/backups/x", "tank/c"))
        assert message.startswith("datasets[tank/backups/x]: ")

    def test_a_trailing_slash_is_ignored(self):
        assert refused(config("tank/backups/", target="tank/backups"))
        assert refused(config("tank/backups/a", target="tank/backups/"))

    def test_a_non_string_name_is_not_this_checks_business(self):
        # Type problems are reported by the dataset checks that run before it.
        _require_outside_target(
            DatasetConfig(name=None), "datasets[?]", RemoteServerConfig("tank/backups")
        )


@pytest.mark.unit
class TestComponentsNotStringPrefix:
    """Path components, never string prefixes: the mutation that uses
    `startswith` refuses every one of these."""

    @pytest.mark.parametrize(
        "name, target",
        [
            ("tank/backup", "tank/backups"),
            ("tank/backups", "tank/backup"),
            ("tank/backups2", "tank/backups"),
            ("tank/backupsx/y", "tank/backups"),
            ("tankx/backups", "tank/backups"),
            ("tank", "tankx/backups"),       # recursive ancestor, a string prefix only
            ("a", "ab/c"),
            ("t", "tank/backups"),
        ],
    )
    def test_a_sibling_whose_name_shares_a_string_prefix_is_allowed(self, name, target):
        validate_config(config((name, True, True), target=target))

    def test_an_ancestor_is_matched_per_component(self):
        # `tank/b` is a string prefix of `tank/backups` but not its ancestor.
        validate_config(config(("tank/b", True, True), target="tank/backups"))
        assert refused(config(("tank", True, True), target="tank/backups"))


@pytest.mark.unit
class TestOnlyAnUnsetTargetImposesNothing:
    """The target counts whenever `remote_backup.target_dataset` is set, ENABLED OR NOT: a host that
    stopped being a server still has other clients' received copies under it, which a snapshot, a prune
    or a recursive send must never touch."""

    def test_remote_backup_absent(self):
        validate_config(config(("tank/backups", True, True), target=None))

    @pytest.mark.parametrize("name, recursive", [
        ("tank/backups", False), ("tank/backups/a", False), ("tank", True),
    ])
    def test_a_disabled_receiver_refuses_the_same_datasets_with_a_note(self, name, recursive):
        message = refused(config((name, recursive, True), target="tank/backups", enabled=False))
        assert "remote_backup is disabled, but the target still counts" in message
        assert "Remove or move remote_backup.target_dataset" in message

    @pytest.mark.parametrize("name, recursive", [
        ("tank/backups", False), ("tank/backups/a", False), ("tank", True),
    ])
    def test_the_note_appears_only_when_it_is_disabled(self, name, recursive):
        enabled = refused(config((name, recursive, True), target="tank/backups", enabled=True))
        disabled = refused(config((name, recursive, True), target="tank/backups", enabled=False))
        assert "remote_backup is disabled" not in enabled
        assert disabled.startswith(enabled.rstrip()), "the same refusal, plus the note"

    def test_enabled_and_disabled_refuse_exactly_the_same_datasets(self):
        for name, recursive in [("tank/backups", False), ("tank/backups/a", False), ("tank", True),
                                ("tank/b", True), ("tank/backups-old", False), ("tank", False)]:
            outcomes = []
            for enabled in (True, False):
                try:
                    validate_config(config((name, recursive, True), target="tank/backups", enabled=enabled))
                    outcomes.append(False)
                except ValueError:
                    outcomes.append(True)
            assert outcomes[0] == outcomes[1], (name, recursive)

    def test_a_disabled_receiver_with_a_non_overlapping_dataset_is_accepted(self):
        validate_config(config("tank/data", ("tank/backup", True, True), target="tank/backups", enabled=False))

    def test_a_disabled_dataset_over_a_disabled_target_is_refused_too(self):
        refused(config(("tank/backups/a", False, False), target="tank/backups", enabled=False))

    def test_a_local_destination_equal_inside_or_above_a_disabled_remote_target_is_still_accepted(self):
        # `_require_disjoint_receive_targets` and `shared_receive_target` still need the receiver enabled
        for local in ("tank/backups", "tank/backups/vault", "tank"):
            cfg = config("other/data", target="tank/backups", enabled=False)
            cfg.destinations = {"local": Destination(url=f"zfs:{local}")}
            validate_config(cfg)

    @pytest.mark.parametrize("target", ["", None])
    def test_no_target_means_nothing_to_overlap(self, target):
        # Called directly: `validate_config` has its own (earlier) refusal for
        # an empty target, which is not this check's business.
        ds = DatasetConfig(name="tank/backups", recursive=True)
        remote = RemoteServerConfig(target, enabled=True)
        _require_outside_target(ds, "datasets[tank/backups]", remote)

    def test_validate_config_reports_an_empty_target_in_its_own_words(self):
        message = refused(config("tank/a", target=""))
        assert "target_dataset" in message and "received" not in message

    def test_no_remote_backup_object_at_all(self):
        _require_outside_target(DatasetConfig(name="tank/a"), "datasets[tank/a]", None)


@pytest.mark.unit
class TestWording:
    def test_at_or_below_the_target(self):
        equal = refused(config("tank/backups"))
        below = refused(config("tank/backups/a"))
        for message in (equal, below):
            assert "remote_backup.target_dataset 'tank/backups'" in message
            assert "received" in message
            assert "ancestor" not in message
        assert "the dataset is remote_backup.target_dataset" in equal
        assert "the dataset is below remote_backup.target_dataset" in below
        assert "Move the target" in below  # a way out

    def test_recursive_ancestor(self):
        message = refused(config(("tank", True, True)))
        assert message.startswith("datasets[tank]: ")
        assert "recursive" in message and "ancestor" in message
        assert "'tank/backups'" in message
        assert "received" in message
        assert "non-recursive" in message  # the way out

    def test_the_message_is_a_plain_value_error_with_no_traceback_text(self):
        assert "Traceback" not in refused(config("tank/backups"))


@pytest.mark.unit
class TestLocalDestinationTargets:
    """Item 2.2: the same rule for the target of a `zfs:` (local) destination. The full matrix,
    the target-versus-target rules and the wording live in
    `test_zfsbackup_store_local_dest.py`; this is the C0 table's local twin."""

    @staticmethod
    def with_local(*datasets, local_target="tank/backups", remote=None) -> BackupConfig:
        cfg = config(*datasets, target=remote)
        cfg.destinations = {"local": Destination(url=f"zfs:{local_target}")}
        return cfg

    @pytest.mark.parametrize(
        "name, recursive, ds_enabled",
        [
            ("tank/backups", False, True),
            ("tank/backups/a", False, True),
            ("tank/backups/a", False, False),
            ("tank", True, True),
            ("tank", True, False),
        ],
    )
    def test_refused_like_the_remote_target(self, name, recursive, ds_enabled):
        message = refused(self.with_local((name, recursive, ds_enabled)))
        assert message.startswith(f"datasets[{name}]: ")
        assert "local destination 'local'" in message

    @pytest.mark.parametrize(
        "name, recursive, target",
        [
            ("tank", False, "tank/backups"),
            ("tank/backup", True, "tank/backups"),
            ("tank/backups-old", True, "tank/backups"),
            ("tank/b", True, "tank/backups"),
            ("ta", True, "tank/backups"),
            ("tank/backups", False, "tank/backup"),
        ],
    )
    def test_components_not_string_prefix(self, name, recursive, target):
        validate_config(self.with_local((name, recursive, True), local_target=target))

    def test_remote_backup_being_disabled_does_not_switch_the_local_check_off(self):
        cfg = self.with_local("tank/backups/a", remote="tank/other")
        cfg.remote_backup.enabled = False
        refused(cfg)

    def test_the_remote_only_helper_cannot_see_local_destinations(self):
        _require_outside_target(
            DatasetConfig(name="tank/backups"), "datasets[tank/backups]", None
        )


@pytest.mark.integration
class TestLocalTargetThroughTheDaemon:
    def test_test_config_rejects_a_target_moved_onto_a_dataset(self, tmp_path, caplog):
        path = tmp_path / "daemon" / "config.db"
        source = tmp_path / "ok.yaml"
        source.write_text(
            'snapshot_prefix: "autosnap"\ndatasets:\n  - name: "tank/data"\n    frequency: "1h"\n'
            'destinations:\n  local:\n    url: "zfs:tank/backups"\n'
        )
        import_yaml(source, resolve_config_path(str(path)))
        dispose_all()
        conn = sqlite3.connect(str(path))
        conn.execute("UPDATE destinations SET url = 'zfs:tank'")
        conn.commit()
        conn.close()
        with patch("sys.argv", ["daemon", "--test-config", "-c", str(path)]):
            with caplog.at_level(logging.ERROR):
                result = main()
        dispose_all()
        assert result == 1
        assert "is invalid" in caplog.text
        assert "datasets[tank/data]" in caplog.text and "local destination 'local'" in caplog.text


# --------------------------------------------------------------------------
# Through the CLI
# --------------------------------------------------------------------------

OVERLAP = """\
snapshot_prefix: "autosnap"
remote_backup:
  enabled: {enabled}
  target_dataset: "{target}"
datasets:
{datasets}"""


def yaml_text(datasets, target="tank/backups", enabled="true") -> str:
    rows = "".join(
        f'  - name: "{name}"\n    recursive: {str(recursive).lower()}\n    frequency: "1h"\n'
        for name, recursive in datasets
    )
    return OVERLAP.format(enabled=enabled, target=target, datasets=rows)


@pytest.mark.integration
class TestThroughTheCli:
    @pytest.mark.parametrize(
        "datasets, word",
        [
            ([("tank/backups", False)], "is remote_backup.target_dataset"),
            ([("tank/backups/a", False)], "is below remote_backup.target_dataset"),
            ([("tank", True)], "ancestor"),
        ],
    )
    def test_import_exits_one_and_writes_nothing(self, tmp_path, db, datasets, word):
        source = tmp_path / "overlap.yaml"
        source.write_text(yaml_text(datasets))
        before = sorted(p.name for p in tmp_path.rglob("*"))
        result = Cli()("import", source, "-c", db)
        assert result.exit_code == 1, result.stderr
        assert word in result.stderr and "received" in result.stderr
        assert not db.exists()
        assert sorted(p.name for p in tmp_path.rglob("*")) == before

    def test_import_over_an_existing_database_leaves_it_untouched(self, tmp_path, db):
        run = Cli()
        good = tmp_path / "good.yaml"
        good.write_text(yaml_text([("tank/a", False)]))
        assert run("import", good, "-c", db).exit_code == 0
        dispose_all()
        before, gen, before_cfg = db_state(db), generation(db), stored(db)

        bad = tmp_path / "bad.yaml"
        bad.write_text(yaml_text([("tank/backups/x", False)]))
        result = run("import", bad, "-c", db)
        assert result.exit_code == 1, result.stderr
        dispose_all()
        assert db_state(db) == before
        assert generation(db) == gen and stored(db) == before_cfg

    def test_the_allowed_neighbours_import_fine(self, tmp_path, db):
        source = tmp_path / "ok.yaml"
        source.write_text(yaml_text([("tank", False), ("tank/backup", True), ("tank/b", True)]))
        result = Cli()("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr

    def test_a_disabled_receiver_over_an_overlapping_dataset_is_refused_at_import(self, tmp_path, db):
        source = tmp_path / "off.yaml"
        source.write_text(yaml_text([("tank/backups/a", False)], enabled="false"))
        result = Cli()("import", source, "-c", db)
        assert result.exit_code == 1, result.stderr
        assert "remote_backup is disabled, but the target still counts" in result.stderr
        assert not db.exists()

    def test_a_disabled_receiver_beside_unrelated_datasets_imports_and_can_be_enabled(self, tmp_path, db):
        run = Cli()
        source = tmp_path / "off.yaml"
        source.write_text(yaml_text([("tank/data", False)], enabled="false"))
        assert run("import", source, "-c", db).exit_code == 0
        dispose_all()
        assert run("set", "remote_backup.enabled", "true", "-c", db).exit_code == 0

    def test_a_dataset_moved_onto_a_disabled_target_is_refused(self, tmp_path, db):
        run = Cli()
        source = tmp_path / "off.yaml"
        source.write_text(yaml_text([("tank/data", False)], enabled="false"))
        assert run("import", source, "-c", db).exit_code == 0
        dispose_all()
        before, gen = db_state(db), generation(db)
        result = run("rename", "dataset", "tank/data", "tank/backups/data", "-c", db)
        assert result.exit_code == 1, result.stderr
        assert "remote_backup is disabled, but the target still counts" in result.stderr
        dispose_all()
        assert db_state(db) == before and generation(db) == gen

    def test_moving_the_target_onto_a_dataset_is_refused(self, tmp_path, db):
        run = Cli()
        source = tmp_path / "ok.yaml"
        source.write_text(yaml_text([("tank/data", False)], target="tank/backups"))
        assert run("import", source, "-c", db).exit_code == 0
        dispose_all()
        before, gen = db_state(db), generation(db)

        result = run("set", "remote_backup.target_dataset", "tank/data", "-c", db)
        assert result.exit_code == 1, result.stderr
        dispose_all()
        assert db_state(db) == before and generation(db) == gen


# --------------------------------------------------------------------------
# Through the daemon
# --------------------------------------------------------------------------


@pytest.fixture
def overlapping_db(tmp_path):
    """A real database that holds an ENABLED receiver over a configured dataset: imported without the
    overlap (the import refuses it, even for a disabled receiver), then the dataset is renamed onto the
    target behind the CLI's back -- the state a database written before C0 can be in."""
    path = tmp_path / "daemon" / "config.db"
    source = tmp_path / "ok.yaml"
    source.write_text(yaml_text([("tank/other", False), ("tank/data", False)]))
    resolved = resolve_config_path(str(path))
    import_yaml(source, resolved)
    dispose_all()
    conn = sqlite3.connect(str(path))
    conn.execute("UPDATE datasets SET name = 'tank/backups/a' WHERE name = 'tank/other'")
    conn.commit()
    conn.close()
    return path


@pytest.mark.integration
class TestThroughTheDaemon:
    def test_test_config_rejects_it(self, overlapping_db, caplog):
        with patch("sys.argv", ["daemon", "--test-config", "-c", str(overlapping_db)]):
            with caplog.at_level(logging.ERROR):
                result = main()
        dispose_all()
        assert result == 1
        assert "is invalid" in caplog.text
        assert "datasets[tank/backups/a]" in caplog.text
        assert "received" in caplog.text
        assert "Traceback" not in caplog.text

    def test_startup_rejects_it_before_any_worker(self, overlapping_db, caplog, mocker):
        mocker.patch("zfsbackup.daemon.BackupDaemon", side_effect=AssertionError("daemon built"))
        with patch("sys.argv", ["daemon", "-c", str(overlapping_db)]):
            with caplog.at_level(logging.ERROR):
                result = main()
        dispose_all()
        assert result == 1
        assert "is invalid" in caplog.text

    def test_a_config_that_does_not_overlap_is_accepted(self, tmp_path, caplog):
        path = tmp_path / "ok" / "config.db"
        source = tmp_path / "ok.yaml"
        source.write_text(yaml_text([("tank/data", False), ("tank", False)]))
        import_yaml(source, resolve_config_path(str(path)))
        dispose_all()
        with patch("sys.argv", ["daemon", "--test-config", "-c", str(path)]):
            with caplog.at_level(logging.INFO):
                assert main() == 0
        dispose_all()
        assert "Configuration is valid!" in caplog.text

    def test_the_mocked_load_path_uses_the_real_validator(self, mocker, tmp_path, caplog):
        """A guard on the idiom: the load is faked, `validate_config` is not."""
        mocker.patch("zfsbackup.daemon.check_config_db")
        mocker.patch(
            "zfsbackup.daemon.load_runtime_config",
            return_value=ConfigSnapshot(config(("tank", True, True)), 4, "deadbeef"),
        )
        with patch("sys.argv", ["daemon", "--test-config", "-c", str(tmp_path / "c.db")]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        assert "configuration generation 4 is invalid" in caplog.text
        assert "ancestor" in caplog.text

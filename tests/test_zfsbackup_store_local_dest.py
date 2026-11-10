"""Item 2.2 of docs/local_destination_plan.md: local destinations (`url: zfs:<dataset>`) in
the config store.

A local destination is an ordinary `destinations` row whose `url` is `zfs:<dataset>`; there
is no schema change. `zfsbackup/config/store/mapper.py` changed in two places:

- `validate_config` judges every destination with `config.destination_problem` (the URL
  grammar for both kinds, plus "a `zfs:` destination has no server, so no
  `server_fingerprint`");
- `_require_outside_target` became `_require_outside_receive_targets`: a configured dataset
  may not be at or below, nor a recursive ancestor of, the remote target OR the target of any
  declared local destination (used or not), and `_require_disjoint_receive_targets` refuses
  receive targets that overlap one another.

Sections:

- ``TestRoundTrip`` -- `save_config` -> `load_config`, a dataset `remote` entry that uses the
  destination, an https destination next to it, an https fingerprint unchanged, and the YAML
  `import` -> `export` loop;
- ``TestUrlAndFingerprintRefusals`` -- malformed `zfs:` URLs, whitespace, a fingerprint on a
  `zfs:` destination (in code, in YAML, in a hand-edited database), `save_config` raising
  before any DELETE;
- ``TestDatasetOverlap`` -- the dataset-versus-local-target matrix and its wording;
- ``TestTargetsDisjoint`` -- local-local and local-remote target overlap;
- ``TestOrderAndCompatibility`` -- which error wins, the remote messages byte for byte,
  `_require_outside_target` keeping its signature;
- ``TestThroughTheCli`` / ``TestThroughTheDaemon`` -- `import` refuses and writes nothing;
  `--test-config`, startup and the mocked load path refuse a database that was edited behind
  the CLI's back.

The engine is always built through `store.db.make_engine` (foreign keys ON). Nothing here
reaches a real `zfs`.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests in this file
plus `test_zfsbackup_store_target_overlap.py`): see the report that accompanied this file.
"""

import inspect
import logging
import sqlite3
from unittest.mock import patch

import pytest
from _cli_support import Cli, db, db_state, generation, isolated, stored  # noqa: F401
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
    RemoteServerConfig,
    RetentionRule,
)
from zfsbackup.config.store import (
    ConfigSnapshot,
    dispose_all,
    import_yaml,
    read_config,
    resolve_config_path,
    validate_config,
)
from zfsbackup.config.store import models
from zfsbackup.config.store.db import make_engine
from zfsbackup.config.store.mapper import (
    _require_outside_target,
    load_config,
    save_config,
)
from zfsbackup.daemon import main

FP = "ab" * 32  # a canonical 64-hex fingerprint


# --------------------------------------------------------------------------
# Builders
# --------------------------------------------------------------------------


@pytest.fixture
def engine():
    eng = make_engine("sqlite:///:memory:", foreign_keys=True)
    models.Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session(engine):
    with Session(engine) as s:
        yield s


def local(target: str) -> Destination:
    return Destination(url=f"zfs:{target}")


def cfg(
    dataset: str = "scratch/data",
    *,
    recursive: bool = False,
    enabled: bool = True,
    locals_: dict = None,
    remote_target: str = None,
    remote_enabled: bool = True,
    use: tuple = (),
    extra_destinations: dict = None,
) -> BackupConfig:
    """One dataset; `locals_` maps destination name -> local target dataset (declared, used
    only if named in `use`); optional enabled/disabled remote_backup target."""
    destinations = {name: local(target) for name, target in (locals_ or {}).items()}
    destinations.update(extra_destinations or {})
    return BackupConfig(
        datasets=[
            DatasetConfig(
                name=dataset,
                recursive=recursive,
                enabled=enabled,
                remote=[RemoteDatasetConfig(destination=name) for name in use],
            )
        ],
        destinations=destinations,
        remote_backup=(
            None if remote_target is None else RemoteServerConfig(remote_target, enabled=remote_enabled)
        ),
    )


def refused(config: BackupConfig) -> str:
    with pytest.raises(ValueError) as excinfo:
        validate_config(config)
    return str(excinfo.value)


def yaml_with(url_block: str, dataset: str = "tank/data", remote: str = "") -> str:
    return (
        'snapshot_prefix: "autosnap"\n'
        "datasets:\n"
        f'  - name: "{dataset}"\n'
        '    frequency: "1h"\n'
        f"{remote}"
        "destinations:\n"
        f"{url_block}"
    )


# --------------------------------------------------------------------------
# Round trip
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestRoundTrip:
    def test_save_then_load_returns_an_equal_config(self, session):
        config = cfg(locals_={"local": "tank/backups"}, use=("local",))
        save_config(session, config)
        session.commit()
        loaded = load_config(session)
        assert loaded.destinations == config.destinations
        assert loaded.destinations["local"] == Destination(url="zfs:tank/backups")
        assert loaded.destinations["local"].is_local
        assert loaded.destinations["local"].target_dataset == "tank/backups"
        assert loaded == config

    def test_the_url_is_stored_verbatim_in_the_existing_column(self, session):
        save_config(session, cfg(locals_={"local": "tank/backups"}))
        session.commit()
        row = session.scalars(select(models.Destination)).one()
        assert (row.name, row.url, row.server_fingerprint) == ("local", "zfs:tank/backups", None)

    def test_usable_as_a_dataset_remote_destination_with_overrides(self, session):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/data",
                    frequency=Duration("1h"),
                    retention_rules=[RetentionRule(age=Duration("1d"), keep_for=Duration("30d"))],
                    remote=[
                        RemoteDatasetConfig(
                            destination="local",
                            frequency=Duration("4h"),
                            retention_rules=[
                                RetentionRule(age=Duration("1d"), keep_for=Duration("7d"))
                            ],
                            raw=True,
                        ),
                        RemoteDatasetConfig(destination="offsite"),
                    ],
                )
            ],
            destinations={
                "local": local("tank/backups"),
                "offsite": Destination(url="https://offsite:8080", server_fingerprint=FP),
            },
        )
        save_config(session, config)
        session.commit()
        loaded = load_config(session)
        assert loaded == config
        assert [r.destination for r in loaded.datasets[0].remote] == ["local", "offsite"]
        assert loaded.datasets[0].remote[0].raw is True
        assert [r.keep_for.total_seconds() for r in loaded.datasets[0].remote[0].retention_rules] == [
            7 * 86400
        ]

    def test_an_https_destination_with_a_fingerprint_is_unchanged(self, session):
        config = cfg(
            extra_destinations={"offsite": Destination(url="https://offsite:8080", server_fingerprint=FP)}
        )
        assert validate_config(config) is not None
        save_config(session, config)
        session.commit()
        got = load_config(session).destinations["offsite"]
        assert got == Destination(url="https://offsite:8080", server_fingerprint=FP)
        assert not got.is_local and got.target_dataset is None

    def test_a_dotted_destination_name_and_a_colon_in_the_dataset(self, session):
        config = cfg(locals_={"dc.local": "tank/a:b"}, use=("dc.local",))
        save_config(session, config)
        session.commit()
        assert load_config(session) == config

    def test_a_second_save_replaces_the_local_destination(self, session):
        save_config(session, cfg(locals_={"local": "tank/one"}))
        session.commit()
        save_config(session, cfg(locals_={"local": "tank/two"}))
        session.commit()
        assert load_config(session).destinations["local"].url == "zfs:tank/two"
        assert session.scalar(select(func.count()).select_from(models.Destination)) == 1


@pytest.mark.integration
class TestYamlRoundTrip:
    YAML = yaml_with(
        '  local:\n    url: "zfs:tank/backups"\n  offsite:\n    url: "https://offsite:8080"\n',
        remote='    remote:\n      - destination: "local"\n      - destination: "offsite"\n',
    )

    def test_import_then_export_reproduces_the_url(self, tmp_path, db):
        source = tmp_path / "in.yaml"
        source.write_text(self.YAML)
        run = Cli()
        assert run("import", source, "-c", db).exit_code == 0
        dispose_all()
        assert stored(db).destinations["local"] == Destination(url="zfs:tank/backups")

        exported = run("export", "-c", db)
        assert exported.exit_code == 0, exported.stderr
        assert "url: zfs:tank/backups" in exported.stdout or '"zfs:tank/backups"' in exported.stdout
        # And the exported text is itself importable to the same configuration.
        again = tmp_path / "again.yaml"
        again.write_text(exported.stdout)
        other = tmp_path / "other" / "config.db"
        assert run("import", again, "-c", other).exit_code == 0
        dispose_all()
        assert stored(other) == stored(db)

    def test_from_file_equals_the_database_config(self, tmp_path, db):
        """The mapper-equivalence contract for a local destination."""
        source = tmp_path / "in.yaml"
        source.write_text(self.YAML)
        assert Cli()("import", source, "-c", db).exit_code == 0
        dispose_all()
        expected = BackupConfig.from_file(source)
        assert stored(db) == expected
        assert expected.destinations["local"].is_local

    def test_the_local_destination_is_used_by_the_dataset(self, tmp_path):
        source = tmp_path / "in.yaml"
        source.write_text(self.YAML)
        config = BackupConfig.from_file(source)
        assert [r.destination for r in config.datasets[0].remote] == ["local", "offsite"]


# --------------------------------------------------------------------------
# URL and fingerprint refusals
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestUrlAndFingerprintRefusals:
    @pytest.mark.parametrize(
        "url, word",
        [
            pytest.param("zfs:", "names no dataset", id="empty"),
            pytest.param("zfs:/x", "leading '/'", id="leading-slash"),
            pytest.param("zfs://x", "no '//'", id="url-style"),
            pytest.param("zfs:a@b", "not a snapshot", id="snapshot"),
            pytest.param("zfs:a#b", "bookmark", id="bookmark"),
            pytest.param("ZFS:x", "exact lowercase", id="uppercase-prefix"),
            pytest.param("Zfs:x", "exact lowercase", id="mixed-prefix"),
            pytest.param("zfs:tank/", "not a valid dataset name", id="trailing-slash"),
            pytest.param("zfs:tank//x", "not a valid dataset name", id="doubled-slash"),
            pytest.param("zfs:1pool/x", "not a valid dataset name", id="pool-starts-with-digit"),
            pytest.param("zfs:tank/é", "not a valid dataset name", id="non-ascii"),
            pytest.param("zfs:tank/a\nb", "control character", id="newline"),
            pytest.param("zfs:tank/a\x00b", "control character", id="nul"),
        ],
    )
    def test_malformed_local_url_is_refused_with_the_destination_prefix(self, url, word):
        message = refused(cfg(extra_destinations={"bad": Destination(url=url)}))
        assert message.startswith("destinations[bad]: ")
        assert word in message
        # An unprintable character is never echoed raw into the message.
        assert "\x00" not in message
        if "\n" in url:
            assert "\n" not in message

    @pytest.mark.parametrize("url", [" zfs:tank/x", "zfs:tank/x ", "  zfs:tank/x\t"])
    def test_leading_or_trailing_whitespace_is_refused(self, url):
        message = refused(cfg(extra_destinations={"ws": Destination(url=url)}))
        assert message.startswith("destinations[ws].url ")
        assert "whitespace" in message

    def test_a_fingerprint_on_a_local_destination_is_refused_in_code(self):
        config = cfg(extra_destinations={"local": Destination(url="zfs:tank/backups", server_fingerprint=FP)})
        message = refused(config)
        assert message.startswith("destinations[local]: ")
        assert "server_fingerprint" in message and "no server" in message

    def test_a_fingerprint_on_a_local_destination_is_refused_in_yaml(self, tmp_path):
        source = tmp_path / "fp.yaml"
        source.write_text(
            yaml_with(f'  local:\n    url: "zfs:tank/backups"\n    server_fingerprint: "{FP}"\n')
        )
        with pytest.raises(ValueError) as excinfo:
            BackupConfig.from_file(source)
        assert "destinations[local]: " in str(excinfo.value)
        assert "server_fingerprint" in str(excinfo.value)

    def test_a_malformed_local_url_is_refused_in_yaml_with_the_prefix(self, tmp_path):
        source = tmp_path / "bad.yaml"
        source.write_text(yaml_with('  local:\n    url: "ZFS:tank/x"\n'))
        with pytest.raises(ValueError, match=r"destinations\[local\]: .*exact lowercase"):
            BackupConfig.from_file(source)

    def test_save_config_raises_before_any_delete(self, session):
        good = cfg(locals_={"local": "tank/backups"}, use=("local",))
        save_config(session, good)
        session.commit()
        gen = session.scalar(select(models.GlobalSettings.generation))

        bad = cfg(
            "tank/other",
            extra_destinations={"local": Destination(url="zfs:tank/backups", server_fingerprint=FP)},
        )
        with pytest.raises(ValueError, match=r"destinations\[local\]"):
            save_config(session, bad)
        # Nothing was deleted, flushed or bumped.
        assert not session.dirty and not session.new and not session.deleted
        session.rollback()
        assert load_config(session) == good
        assert session.scalar(select(models.GlobalSettings.generation)) == gen

    def test_save_config_refuses_a_malformed_url_before_any_delete(self, session):
        good = cfg(locals_={"local": "tank/backups"})
        save_config(session, good)
        session.commit()
        with pytest.raises(ValueError, match=r"destinations\[local\]: .*names no dataset"):
            save_config(session, cfg(extra_destinations={"local": Destination(url="zfs:")}))
        session.rollback()
        assert load_config(session) == good

    def test_the_generic_scheme_refusal_points_at_both_kinds(self):
        message = refused(cfg(extra_destinations={"h": Destination(url="ftp://h:21")}))
        assert message.startswith("destinations[h]: ")
        assert "https://" in message and "zfs:<dataset>" in message


@pytest.mark.integration
class TestImportRefusals:
    def test_import_refuses_a_fingerprint_on_a_local_destination(self, tmp_path, db):
        source = tmp_path / "fp.yaml"
        source.write_text(
            yaml_with(f'  local:\n    url: "zfs:tank/backups"\n    server_fingerprint: "{FP}"\n')
        )
        result = Cli()("import", source, "-c", db)
        assert result.exit_code == 1, result.stderr
        assert "destinations[local]" in result.stderr and "server_fingerprint" in result.stderr
        assert not db.exists()

    def test_import_over_an_existing_database_leaves_it_untouched(self, tmp_path, db):
        run = Cli()
        good = tmp_path / "good.yaml"
        good.write_text(yaml_with('  local:\n    url: "zfs:tank/backups"\n'))
        assert run("import", good, "-c", db).exit_code == 0
        dispose_all()
        before, gen, before_cfg = db_state(db), generation(db), stored(db)

        bad = tmp_path / "bad.yaml"
        bad.write_text(yaml_with('  local:\n    url: "zfs:a@b"\n'))
        result = run("import", bad, "-c", db)
        assert result.exit_code == 1, result.stderr
        dispose_all()
        assert db_state(db) == before
        assert generation(db) == gen and stored(db) == before_cfg

    def test_a_database_edited_to_carry_one_loads_but_does_not_validate(self, tmp_path, db):
        """Load is permissive (a legacy row can be fixed with the CLI); validate is not."""
        source = tmp_path / "ok.yaml"
        source.write_text(yaml_with('  local:\n    url: "zfs:tank/backups"\n'))
        assert Cli()("import", source, "-c", db).exit_code == 0
        dispose_all()
        conn = sqlite3.connect(str(db))
        conn.execute("UPDATE destinations SET server_fingerprint = ?", (FP,))
        conn.commit()
        conn.close()
        snapshot = read_config(resolve_config_path(str(db)))
        dispose_all()
        assert snapshot.config.destinations["local"].server_fingerprint == FP
        message = refused(snapshot.config)
        assert message.startswith("destinations[local]: ")


# --------------------------------------------------------------------------
# Dataset vs local target
# --------------------------------------------------------------------------

T = "bk/A"


@pytest.mark.unit
class TestDatasetOverlap:
    @pytest.mark.parametrize(
        "name, recursive, target",
        [
            pytest.param("tank/data", False, "tank/backups", id="disjoint"),
            pytest.param("tank/data", True, "tank/backups", id="disjoint-recursive"),
            pytest.param("bk/Ax", False, "bk/A", id="name-prefix-sibling"),
            pytest.param("bk/Ax", True, "bk/A", id="name-prefix-sibling-recursive"),
            pytest.param("bk/A", False, "bk/Ax", id="target-has-longer-name"),
            pytest.param("bk", False, "bk/a", id="non-recursive-ancestor"),
            pytest.param("b", True, "bk/a", id="recursive-string-prefix-only"),
            pytest.param("bk/a", True, "bk/ab", id="recursive-name-prefix-sibling"),
            pytest.param("bk/ab", True, "bk/a", id="recursive-longer-sibling"),
            pytest.param("rpool", True, "tank/backups", id="other-pool"),
        ],
    )
    def test_allowed(self, name, recursive, target):
        validate_config(cfg(name, recursive=recursive, locals_={"local": target}))

    @pytest.mark.parametrize(
        "name, recursive, ds_enabled, target, how",
        [
            pytest.param("tank/backups", False, True, "tank/backups", "is the target", id="equal"),
            pytest.param("tank/backups", False, False, "tank/backups", "is the target", id="equal-disabled"),
            pytest.param("tank/backups/a", False, True, "tank/backups", "is below the target", id="below"),
            pytest.param("tank/backups/a/b/c", False, True, "tank/backups", "is below the target", id="deeply-below"),
            pytest.param("tank/backups/a", False, False, "tank/backups", "is below the target", id="below-disabled"),
            pytest.param("tank/backups/a", True, True, "tank/backups", "is below the target", id="below-recursive"),
            pytest.param("bk", True, True, "bk/a", "ancestor", id="recursive-ancestor"),
            pytest.param("bk", True, False, "bk/a", "ancestor", id="recursive-ancestor-disabled"),
            pytest.param("tank", True, True, "tank/x/y/z", "ancestor", id="recursive-ancestor-two-levels"),
            pytest.param("tank/backups", False, True, "tank", "is below the target", id="target-is-the-pool"),
        ],
    )
    def test_refused(self, name, recursive, ds_enabled, target, how):
        message = refused(
            cfg(name, recursive=recursive, enabled=ds_enabled, locals_={"local": target})
        )
        assert message.startswith(f"datasets[{name}]: ")
        assert how in message
        assert "local destination 'local'" in message
        assert f"{target!r}" in message

    @pytest.mark.parametrize("use", [(), ("local",)])
    def test_a_declared_destination_counts_whether_or_not_a_dataset_uses_it(self, use):
        # The unused case is the one a "check only used destinations" mutation lets through.
        message = refused(cfg("tank/backups/a", locals_={"local": "tank/backups"}, use=use))
        assert message.startswith("datasets[tank/backups/a]: ")

    def test_enabling_a_later_remote_entry_cannot_open_the_hole(self):
        """Refused while the destination is unused, so a `set datasets.N.remote` later is safe."""
        refused(cfg("tank/backups", locals_={"unused": "tank/backups"}))

    def test_every_declared_local_target_is_checked(self):
        config = cfg("tank/second/x", locals_={"first": "tank/first", "second": "tank/second"})
        message = refused(config)
        assert "local destination 'second'" in message and "'tank/second'" in message

    def test_the_wording_names_the_destination_and_the_target_and_a_way_out(self):
        below = refused(cfg("tank/backups/a", locals_={"mirror": "tank/backups"}))
        assert "the dataset is below the target 'tank/backups' of local destination 'mirror'" in below
        assert "received" in below and "dedicated target" in below
        equal = refused(cfg("tank/backups", locals_={"mirror": "tank/backups"}))
        assert "the dataset is the target 'tank/backups' of local destination 'mirror'" in equal
        recursive = refused(cfg("tank", recursive=True, locals_={"mirror": "tank/backups"}))
        assert "recursive" in recursive and "ancestor" in recursive
        assert "'tank/backups'" in recursive and "'mirror'" in recursive
        assert "non-recursive" in recursive
        for message in (below, equal, recursive):
            assert "remote_backup" not in message
            assert "Traceback" not in message

    def test_one_overlapping_dataset_among_valid_ones_is_found(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name=n) for n in ("tank/a", "tank/backups/x", "tank/c")],
            destinations={"local": local("tank/backups")},
        )
        assert refused(config).startswith("datasets[tank/backups/x]: ")


# --------------------------------------------------------------------------
# Targets disjoint
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestTargetsDisjoint:
    @pytest.mark.parametrize(
        "a, b",
        [
            ("bk/a", "bk/ab"),
            ("bk/ab", "bk/a"),
            ("bk/Ax", "bk/A"),
            ("pool1/x", "pool2/x"),
            ("bk/a", "bk/b"),
            ("b", "bk/a"),
        ],
    )
    def test_disjoint_local_targets_are_allowed(self, a, b):
        validate_config(cfg(locals_={"one": a, "two": b}))

    def test_equal_local_targets_are_refused(self):
        message = refused(cfg(locals_={"one": "bk/a", "two": "bk/a"}))
        assert "is the same dataset as" in message
        assert "local destination 'one'" in message and "local destination 'two'" in message
        assert "'bk/a'" in message
        assert "disjoint" in message

    def test_a_local_target_inside_another_is_refused(self):
        message = refused(cfg(locals_={"outer": "bk/a", "inner": "bk/a/b"}))
        assert "the target 'bk/a' of local destination 'outer' is an ancestor of the target 'bk/a/b'" in message
        assert "'inner'" in message and "'outer'" in message

    def test_the_other_declaration_order_says_below(self):
        message = refused(cfg(locals_={"inner": "bk/a/b", "outer": "bk/a"}))
        assert "the target 'bk/a/b' of local destination 'inner' is below the target 'bk/a'" in message

    def test_three_targets_one_pair_overlapping(self):
        message = refused(cfg(locals_={"x": "t/x", "y": "t/y", "z": "t/y/z"}))
        assert "'t/y'" in message and "'t/y/z'" in message

    @pytest.mark.parametrize(
        "local_target, remote_target, how",
        [
            ("tank/x/y", "tank/x", "is below"),
            ("tank", "tank/x", "is an ancestor of"),
        ],
    )
    def test_overlap_with_an_enabled_remote_target_is_refused(self, local_target, remote_target, how):
        message = refused(
            cfg(locals_={"local": local_target}, remote_target=remote_target, remote_enabled=True)
        )
        assert how in message
        assert "local destination 'local'" in message and "remote_backup" in message

    @pytest.mark.parametrize(
        "local_target, remote_target",
        [("tank/x", "tank/x"), ("tank/x/y", "tank/x"), ("tank", "tank/x")],
    )
    def test_overlap_with_a_disabled_remote_target_is_allowed(self, local_target, remote_target):
        validate_config(
            cfg(locals_={"local": local_target}, remote_target=remote_target, remote_enabled=False)
        )

    def test_a_disjoint_enabled_remote_target_is_allowed(self):
        validate_config(cfg(locals_={"local": "tank/x"}, remote_target="tank/xy"))
        validate_config(cfg(locals_={"local": "tank/xy"}, remote_target="tank/x"))

    def test_an_unused_local_target_still_collides(self):
        # A nested target is refused even when no dataset sends to it: declared is enough.
        refused(cfg(locals_={"local": "tank/x/y"}, remote_target="tank/x", use=()))
        refused(cfg(locals_={"local": "tank"}, remote_target="tank/x", use=()))

    @pytest.mark.parametrize("use", [(), ("local",)])
    def test_a_target_equal_to_the_enabled_remote_target_is_accepted(self, use):
        """The shared target (docs/local_destination_plan.md addendum): equality only,
        whether or not any dataset sends to the destination."""
        validate_config(cfg(locals_={"local": "tank/x"}, remote_target="tank/x", use=use))

    def test_an_https_destination_has_no_target(self):
        config = cfg(
            locals_={"local": "tank/x"},
            extra_destinations={"offsite": Destination(url="https://tank/x")},
        )
        validate_config(config)


# --------------------------------------------------------------------------
# A local target shared with the server (docs/local_destination_plan.md addendum)
# --------------------------------------------------------------------------

ONLY_THE_SERVERS_TARGET = "Only the server's target itself may be shared"


@pytest.mark.unit
class TestSharedTarget:
    """A local target EQUAL to the enabled remote target is accepted; everything else stays refused.

    Mutation-checked (scratch copy): removing the equal-pair `continue` in `_require_disjoint_receive_targets`
    fails 11 tests in this file; treating nested pairs as disjoint fails 12.
    """

    def test_equal_to_the_enabled_remote_target_validates(self):
        validate_config(cfg(locals_={"local": "tank/x"}, remote_target="tank/x", use=("local",)))

    def test_a_disabled_remote_with_an_equal_target_validates(self):
        validate_config(cfg(locals_={"local": "tank/x"}, remote_target="tank/x", remote_enabled=False))

    def test_a_second_local_destination_on_the_shared_target_is_refused_as_local_local(self):
        message = refused(
            cfg(locals_={"one": "tank/x", "two": "tank/x"}, remote_target="tank/x")
        )
        assert "is the same dataset as" in message
        assert "local destination 'one'" in message and "local destination 'two'" in message
        assert "remote_backup" not in message
        assert "disjoint" in message and ONLY_THE_SERVERS_TARGET not in message

    def test_the_second_local_is_refused_whatever_the_declaration_order(self):
        message = refused(
            cfg(locals_={"zzz": "tank/x", "aaa": "tank/x"}, remote_target="tank/x")
        )
        assert "local destination 'zzz'" in message and "local destination 'aaa'" in message

    def test_a_third_target_nested_in_the_shared_one_is_refused(self):
        message = refused(
            cfg(locals_={"shared": "tank/x", "inner": "tank/x/y"}, remote_target="tank/x")
        )
        assert "tank/x/y" in message

    @pytest.mark.parametrize(
        "local_target, how",
        [("tank/x/y", "is below"), ("tank", "is an ancestor of")],
    )
    def test_nested_local_remote_refusals_carry_the_only_the_servers_target_wording(
        self, local_target, how
    ):
        message = refused(cfg(locals_={"local": local_target}, remote_target="tank/x"))
        assert how in message
        assert ONLY_THE_SERVERS_TARGET in message
        assert "local destination 'local'" in message and "remote_backup" in message

    def test_local_local_refusals_keep_the_dedicated_and_disjoint_wording(self):
        message = refused(cfg(locals_={"outer": "bk/a", "inner": "bk/a/b"}))
        assert "dedicated and disjoint" in message and ONLY_THE_SERVERS_TARGET not in message

    @pytest.mark.parametrize(
        "name, recursive, ds_enabled, how",
        [
            pytest.param("tank/x", False, True, "is remote_backup.target_dataset", id="at"),
            pytest.param("tank/x", False, False, "is remote_backup.target_dataset", id="at-disabled"),
            pytest.param("tank/x/a", False, True, "is below remote_backup.target_dataset", id="below"),
            pytest.param("tank/x/a", True, False, "is below remote_backup.target_dataset",
                         id="below-disabled-recursive"),
            pytest.param("tank", True, True, "remote_backup.target_dataset", id="recursively-above"),
        ],
    )
    def test_a_configured_dataset_at_below_or_above_the_shared_target_is_refused(
        self, name, recursive, ds_enabled, how
    ):
        message = refused(
            cfg(name, recursive=recursive, enabled=ds_enabled,
                locals_={"local": "tank/x"}, remote_target="tank/x")
        )
        assert message.startswith(f"datasets[{name}]: ") and how in message

    def test_a_non_recursive_dataset_above_the_shared_target_is_allowed(self):
        validate_config(cfg("tank", locals_={"local": "tank/x"}, remote_target="tank/x"))

    def test_save_then_load_round_trips_a_shared_config(self, session):
        config = cfg(locals_={"local": "tank/x"}, remote_target="tank/x", use=("local",))
        save_config(session, config)
        session.commit()
        loaded = load_config(session)
        assert loaded == config
        validate_config(loaded)
        from zfsbackup.config import shared_receive_target

        assert shared_receive_target(loaded) == "tank/x"

    def test_import_accepts_a_shared_target_and_export_round_trips(self, tmp_path, db):
        source = tmp_path / "shared.yaml"
        source.write_text(
            overlap_yaml([("tank/data", False)], [("local", "bk/store")]).replace(
                "destinations:", "remote_backup:\n  target_dataset: \"bk/store\"\n  enabled: true\ndestinations:", 1
            )
        )
        run = Cli()
        result = run("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr
        dispose_all()
        stored_config = stored(db)
        assert stored_config.remote_backup.target_dataset == "bk/store"
        assert stored_config.destinations["local"] == Destination(url="zfs:bk/store")
        exported = run("export", "-c", db)
        again = tmp_path / "again.yaml"
        again.write_text(exported.stdout)
        other = tmp_path / "other" / "config.db"
        assert run("import", again, "-c", other).exit_code == 0
        dispose_all()
        assert stored(other) == stored_config

    def test_import_refuses_a_nested_local_target_with_the_new_wording(self, tmp_path, db):
        source = tmp_path / "nested.yaml"
        source.write_text(
            overlap_yaml([("tank/data", False)], [("local", "bk/store/inner")]).replace(
                "destinations:", "remote_backup:\n  target_dataset: \"bk/store\"\n  enabled: true\ndestinations:", 1
            )
        )
        result = Cli()("import", source, "-c", db)
        assert result.exit_code == 1 and ONLY_THE_SERVERS_TARGET in result.stderr
        assert not db.exists()


# --------------------------------------------------------------------------
# Order and compatibility
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestOrderAndCompatibility:
    def test_a_destination_url_error_wins_over_an_overlap_error(self):
        config = cfg(
            "tank/backups/a",
            locals_={"local": "tank/backups"},
            extra_destinations={"zbad": Destination(url="zfs:a@b")},
        )
        assert refused(config).startswith("destinations[zbad]: ")

    def test_a_fingerprint_error_wins_over_an_overlap_error(self):
        config = cfg(
            "tank/backups/a",
            extra_destinations={"local": Destination(url="zfs:tank/backups", server_fingerprint=FP)},
        )
        assert refused(config).startswith("destinations[local]: ")

    def test_a_target_overlap_is_reported_before_a_dataset_overlap(self):
        config = cfg(
            "tank/x/a",
            locals_={"one": "tank/x", "two": "tank/x"},
        )
        message = refused(config)
        assert "disjoint" in message and not message.startswith("datasets[")

    REMOTE_ONLY = [
        ("tank/backups", False),
        ("tank/backups/a", False),
        ("tank", True),
    ]

    @pytest.mark.parametrize("name, recursive", REMOTE_ONLY)
    def test_remote_target_messages_are_byte_identical_with_local_destinations_present(
        self, name, recursive
    ):
        plain = refused(cfg(name, recursive=recursive, remote_target="tank/backups"))
        with_local = refused(
            cfg(
                name,
                recursive=recursive,
                remote_target="tank/backups",
                locals_={"elsewhere": "other/backups"},
                extra_destinations={"offsite": Destination(url="https://h:1")},
            )
        )
        assert with_local == plain
        assert "remote_backup.target_dataset" in plain and "local destination" not in plain

    def test_the_remote_check_runs_before_the_local_one(self):
        # Disjoint targets, and a dataset that sits under the REMOTE one only.
        message = refused(
            cfg("tank/backups/a", remote_target="tank/backups", locals_={"local": "other/backups"})
        )
        assert "remote_backup.target_dataset" in message

    def test_require_outside_target_keeps_its_signature_and_remote_behaviour(self):
        params = list(inspect.signature(_require_outside_target).parameters)
        assert params == ["ds", "key", "remote_backup"]
        with pytest.raises(ValueError, match="is remote_backup.target_dataset 'tank/backups'"):
            _require_outside_target(
                DatasetConfig(name="tank/backups"), "datasets[tank/backups]", RemoteServerConfig("tank/backups")
            )
        _require_outside_target(
            DatasetConfig(name="tank/backups"),
            "datasets[tank/backups]",
            RemoteServerConfig("tank/backups", enabled=False),
        )
        _require_outside_target(DatasetConfig(name="tank/backups"), "datasets[tank/backups]", None)

    def test_require_outside_target_knows_nothing_of_local_destinations(self):
        # It is the remote-only half: a config with a local overlap goes through validate_config.
        _require_outside_target(DatasetConfig(name="tank/backups"), "k", None)
        refused(cfg("tank/backups", locals_={"local": "tank/backups"}))

    def test_a_config_with_no_local_destination_is_validated_as_before(self):
        validate_config(cfg("tank/data", remote_target="tank/backups"))
        validate_config(cfg("tank/data"))

    def test_is_local_and_target_dataset_are_derived_from_the_url(self):
        d = Destination(url="zfs:tank/x")
        assert d.is_local and d.target_dataset == "tank/x"
        h = Destination(url="https://h:1")
        assert not h.is_local and h.target_dataset is None
        assert Destination(url="ZFS:tank/x").is_local is False


# --------------------------------------------------------------------------
# Through the CLI
# --------------------------------------------------------------------------

OVERLAP = """\
snapshot_prefix: "autosnap"
datasets:
{datasets}destinations:
{destinations}"""


def overlap_yaml(datasets, destinations) -> str:
    rows = "".join(
        f'  - name: "{name}"\n    recursive: {str(recursive).lower()}\n    frequency: "1h"\n'
        for name, recursive in datasets
    )
    dests = "".join(f'  {name}:\n    url: "zfs:{target}"\n' for name, target in destinations)
    return OVERLAP.format(datasets=rows, destinations=dests)


@pytest.mark.integration
class TestThroughTheCli:
    @pytest.mark.parametrize(
        "datasets, destinations, word",
        [
            ([("tank/backups", False)], [("local", "tank/backups")], "is the target"),
            ([("tank/backups/a", False)], [("local", "tank/backups")], "is below the target"),
            ([("tank", True)], [("local", "tank/backups")], "ancestor"),
            ([("tank/data", False)], [("one", "bk/a"), ("two", "bk/a/b")], "disjoint"),
        ],
    )
    def test_import_exits_one_and_writes_nothing(self, tmp_path, db, datasets, destinations, word):
        source = tmp_path / "overlap.yaml"
        source.write_text(overlap_yaml(datasets, destinations))
        before = sorted(p.name for p in tmp_path.rglob("*"))
        result = Cli()("import", source, "-c", db)
        assert result.exit_code == 1, result.stderr
        assert word in result.stderr and "local destination" in result.stderr
        assert not db.exists()
        assert sorted(p.name for p in tmp_path.rglob("*")) == before

    def test_the_allowed_neighbours_import_fine(self, tmp_path, db):
        source = tmp_path / "ok.yaml"
        source.write_text(
            overlap_yaml([("bk", False), ("bk/Ax", True), ("b", True)], [("local", "bk/A")])
        )
        result = Cli()("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr


# --------------------------------------------------------------------------
# Through the daemon
# --------------------------------------------------------------------------


@pytest.fixture
def edited_db(tmp_path):
    """A real database whose local target was moved onto a configured dataset behind the CLI's
    back (a hand-edited or pre-2.2 database)."""
    path = tmp_path / "daemon" / "config.db"
    source = tmp_path / "ok.yaml"
    source.write_text(overlap_yaml([("tank/data", False)], [("local", "tank/backups")]))
    import_yaml(source, resolve_config_path(str(path)))
    dispose_all()
    conn = sqlite3.connect(str(path))
    conn.execute("UPDATE destinations SET url = 'zfs:tank/data'")
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def fingerprinted_db(tmp_path):
    path = tmp_path / "daemon" / "config.db"
    source = tmp_path / "ok.yaml"
    source.write_text(overlap_yaml([("tank/data", False)], [("local", "tank/backups")]))
    import_yaml(source, resolve_config_path(str(path)))
    dispose_all()
    conn = sqlite3.connect(str(path))
    conn.execute("UPDATE destinations SET server_fingerprint = ?", (FP,))
    conn.commit()
    conn.close()
    return path


@pytest.mark.integration
class TestThroughTheDaemon:
    def test_read_config_loads_it_and_validate_config_refuses(self, edited_db):
        snapshot = read_config(resolve_config_path(str(edited_db)))
        dispose_all()
        assert snapshot.config.destinations["local"].url == "zfs:tank/data"
        assert refused(snapshot.config).startswith("datasets[tank/data]: ")

    def test_test_config_rejects_it(self, edited_db, caplog):
        with patch("sys.argv", ["daemon", "--test-config", "-c", str(edited_db)]):
            with caplog.at_level(logging.ERROR):
                result = main()
        dispose_all()
        assert result == 1
        assert "is invalid" in caplog.text
        assert "datasets[tank/data]" in caplog.text
        assert "local destination 'local'" in caplog.text
        assert "Traceback" not in caplog.text

    def test_startup_rejects_it_before_any_worker(self, edited_db, caplog, mocker):
        mocker.patch("zfsbackup.daemon.BackupDaemon", side_effect=AssertionError("daemon built"))
        with patch("sys.argv", ["daemon", "-c", str(edited_db)]):
            with caplog.at_level(logging.ERROR):
                result = main()
        dispose_all()
        assert result == 1
        assert "is invalid" in caplog.text

    def test_a_fingerprint_on_a_local_destination_is_rejected_by_test_config(
        self, fingerprinted_db, caplog
    ):
        with patch("sys.argv", ["daemon", "--test-config", "-c", str(fingerprinted_db)]):
            with caplog.at_level(logging.ERROR):
                result = main()
        dispose_all()
        assert result == 1
        assert "destinations[local]" in caplog.text and "server_fingerprint" in caplog.text

    def test_a_config_that_does_not_overlap_is_accepted(self, tmp_path, caplog):
        path = tmp_path / "ok" / "config.db"
        source = tmp_path / "ok.yaml"
        source.write_text(overlap_yaml([("tank/data", False), ("tank", False)], [("local", "tank/backups")]))
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
            return_value=ConfigSnapshot(cfg("tank", recursive=True, locals_={"local": "tank/backups"}), 4, "deadbeef"),
        )
        with patch("sys.argv", ["daemon", "--test-config", "-c", str(tmp_path / "c.db")]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        assert "configuration generation 4 is invalid" in caplog.text
        assert "local destination 'local'" in caplog.text


# --------------------------------------------------------------------------
# The copy-name length check (`mapper._local_copy_name_findings`)
# --------------------------------------------------------------------------
#
# The copy of a snapshot on a local destination is
#   <target>/<client_id>/<dataset>@<prefix>_<14 digits>
# and ZFS refuses a full name over 255 characters. The client id (1-64 characters) is not known to
# the pure `validate_config`: refused when even a 1-character id is too long, warned about when only
# a longer id would be.

from zfsbackup.config import model as _model  # noqa: E402
from zfsbackup.config.store import mapper as _mapper  # noqa: E402

TARGET_LEN, DATASET_LEN, PREFIX_LEN = 20, 30, 7   # 76 characters with a 1-character id
SLACK_ONE = 255 - (TARGET_LEN + DATASET_LEN + PREFIX_LEN + 19)   # growth that still passes at id length 1


def shaped(
    target_len: int = TARGET_LEN,
    dataset_len: int = DATASET_LEN,
    prefix_len: int = PREFIX_LEN,
    *,
    enabled: bool = True,
    dest_url=None,
    use: bool = True,
    declared: bool = True,
) -> BackupConfig:
    """One dataset of `dataset_len` characters copied to a destination whose target has `target_len`
    characters, with a `prefix_len` snapshot prefix. Full copy name with an id of N characters:
    `target_len + dataset_len + prefix_len + 18 + N`."""
    target = "b" * target_len
    dataset = "tank/" + "d" * (dataset_len - 5)
    destinations = {}
    if declared:
        destinations["here"] = Destination(url=dest_url or f"zfs:{target}")
    return BackupConfig(
        snapshot_prefix="p" * prefix_len,
        datasets=[DatasetConfig(
            name=dataset,
            enabled=enabled,
            remote=[RemoteDatasetConfig(destination="here")] if use and declared else [],
        )],
        destinations=destinations,
    )


def length_warnings(warnings):
    return [w for w in warnings if "snapshot names" in w]


@pytest.mark.unit
class TestCopyNameRefusalBoundary:
    """Even a 1-character client id: 255 characters passes, 256 raises."""

    @pytest.mark.parametrize("which", ["target", "dataset", "prefix"])
    def test_exactly_255_with_a_one_character_id_is_accepted(self, which):
        sizes = {"target": TARGET_LEN, "dataset": DATASET_LEN, "prefix": PREFIX_LEN}
        sizes[which] += SLACK_ONE
        # The one-character id makes exactly 255; a longer id would not fit, so a warning (not an error).
        warnings = validate_config(shaped(sizes["target"], sizes["dataset"], sizes["prefix"]))
        found = length_warnings(warnings)
        assert len(found) == 1 and "only client ids up to 1 characters fit" in found[0]

    @pytest.mark.parametrize("which", ["target", "dataset", "prefix"])
    def test_256_with_a_one_character_id_is_refused(self, which):
        sizes = {"target": TARGET_LEN, "dataset": DATASET_LEN, "prefix": PREFIX_LEN}
        sizes[which] += SLACK_ONE + 1
        message = refused(shaped(sizes["target"], sizes["dataset"], sizes["prefix"]))
        assert "256 characters" in message and "over ZFS's 255" in message
        assert "even with a 1-character client id" in message
        assert "shorten the target, the dataset name or snapshot_prefix" in message
        assert "local destination 'here'" in message

    def test_the_message_names_the_dataset_the_destination_and_the_prefix(self):
        message = refused(shaped(TARGET_LEN + SLACK_ONE + 1))
        assert message.startswith("datasets[tank/")
        assert ".remote: " in message
        assert "@" + "p" * PREFIX_LEN + "_<14 digits>" in message
        assert "(target " in message

    def test_a_longer_overshoot_reports_its_own_length(self):
        assert "300 characters" in refused(shaped(TARGET_LEN + 300 - 76))

    def test_a_custom_snapshot_prefix_is_counted(self):
        assert length_warnings(validate_config(shaped(prefix_len=8))) == []
        # Same target and dataset: a long prefix alone tips it over.
        assert "over ZFS's 255" in refused(shaped(prefix_len=PREFIX_LEN + SLACK_ONE + 1))
        assert length_warnings(validate_config(shaped(prefix_len=PREFIX_LEN + SLACK_ONE)))

    def test_the_default_prefix_autosnap_is_counted(self):
        # With "autosnap" (8 characters) this is exactly 255 at a 1-character id; one more is refused.
        config = shaped(TARGET_LEN + SLACK_ONE - 1)
        config.snapshot_prefix = "autosnap"
        assert len(length_warnings(validate_config(config))) == 1
        config = shaped(TARGET_LEN + SLACK_ONE)
        config.snapshot_prefix = "autosnap"
        assert "256 characters" in refused(config)


@pytest.mark.unit
class TestCopyNameWarningBoundary:
    """A 64-character id: 255 characters fits, 256 warns (the id is not known here)."""

    # with a 64-character id the full name is `t + d + p + 82`; 173 is the last one that fits
    FITS = 173

    def _config(self, total: int) -> BackupConfig:
        extra = total - (TARGET_LEN + DATASET_LEN + PREFIX_LEN)
        return shaped(TARGET_LEN + extra)

    def test_255_with_a_64_character_id_does_not_warn(self):
        assert length_warnings(validate_config(self._config(self.FITS))) == []

    def test_256_warns_with_the_margin_and_the_ids_that_fit(self):
        warnings = length_warnings(validate_config(self._config(self.FITS + 1)))
        assert len(warnings) == 1
        warning = warnings[0]
        assert "with a 64-character client id" in warning
        assert "would be 256 characters, 1 over ZFS's 255" in warning
        assert "only client ids up to 63 characters fit" in warning
        assert "children have longer names" in warning

    @pytest.mark.parametrize("over", [1, 2, 10, 40, 63])
    def test_the_margin_and_the_largest_id_that_fits(self, over):
        warning = length_warnings(validate_config(self._config(self.FITS + over)))[0]
        assert f"would be {255 + over} characters, {over} over ZFS's 255" in warning
        assert f"only client ids up to {64 - over} characters fit" in warning

    def test_one_more_and_not_even_a_one_character_id_fits(self):
        # FITS + 63 is the longest that still allows a 1-character id; one more is a refusal.
        assert length_warnings(validate_config(self._config(self.FITS + 63)))
        assert "over ZFS's 255" in refused(self._config(self.FITS + 64))

    def test_a_warning_is_keyed_by_the_dataset_and_the_remote(self):
        warning = length_warnings(validate_config(self._config(self.FITS + 1)))[0]
        assert warning.startswith("datasets[tank/")
        assert ".remote: dataset 'tank/" in warning
        assert "local destination 'here'" in warning

    def test_a_warning_is_not_a_refusal_and_the_config_stays_storable(self, session):
        config = self._config(self.FITS + 1)
        assert length_warnings(validate_config(config))
        save_config(session, config)
        session.commit()
        assert load_config(session) == config


@pytest.mark.unit
class TestCopyNameNoFinding:
    HUGE = 220   # a long dataset that would never fit under a local target

    def test_an_https_destination_is_not_checked(self):
        config = shaped(dataset_len=self.HUGE,
                        dest_url="https://backup.example.com")
        assert length_warnings(validate_config(config)) == []

    def test_an_https_destination_with_a_pinned_fingerprint_is_not_checked(self):
        config = shaped(dataset_len=self.HUGE)
        config.destinations["here"] = Destination(url="https://backup.example.com",
                                                  server_fingerprint=FP)
        assert length_warnings(validate_config(config)) == []

    def test_an_unused_local_destination_is_not_checked(self):
        config = shaped(target_len=TARGET_LEN + 300 - 76, use=False)
        assert length_warnings(validate_config(config)) == []

    def test_a_dataset_without_remote_is_not_checked(self):
        config = shaped(dataset_len=self.HUGE, declared=False)
        assert length_warnings(validate_config(config)) == []
        assert length_warnings(validate_config(shaped(dataset_len=self.HUGE, use=False))) == []

    def test_a_short_name_has_no_finding(self):
        assert length_warnings(validate_config(shaped())) == []
        assert _mapper._local_copy_name_findings("tank/x", "autosnap", "here", "bk/o") == (None, None)


@pytest.mark.unit
class TestCopyNameDisabledDataset:
    def test_a_disabled_over_long_dataset_warns_and_does_not_raise(self):
        config = shaped(TARGET_LEN + SLACK_ONE + 1, enabled=False)
        warnings = validate_config(config)
        found = length_warnings(warnings)
        assert len(found) == 1
        assert "over ZFS's 255" in found[0] and "even with a 1-character client id" in found[0]
        assert "the dataset is disabled so it is ignored, but enabling it will be refused" in found[0]

    def test_the_same_dataset_enabled_raises(self):
        assert "even with a 1-character client id" in refused(shaped(TARGET_LEN + SLACK_ONE + 1))

    def test_a_disabled_dataset_that_might_be_long_gets_the_ordinary_warning(self):
        config = shaped(TARGET_LEN + SLACK_ONE, enabled=False)
        found = length_warnings(validate_config(config))
        assert len(found) == 1 and "disabled" not in found[0]


@pytest.mark.unit
class TestCopyNameIndependentDatasets:
    def _two(self, first_extra: int, second_extra: int) -> BackupConfig:
        """Two datasets on one local destination; the first has `first_extra` more characters."""
        def dataset(tag: str, length: int) -> DatasetConfig:
            return DatasetConfig(
                name=f"tank/{tag}" + "d" * (length - 5 - 1),
                remote=[RemoteDatasetConfig(destination="here")],
            )
        return BackupConfig(
            snapshot_prefix="p" * PREFIX_LEN,
            datasets=[dataset("a", DATASET_LEN + first_extra), dataset("b", DATASET_LEN + second_extra)],
            destinations={"here": Destination(url="zfs:" + "b" * TARGET_LEN)},
        )

    def test_each_dataset_is_judged_on_its_own_length(self):
        # 173 is the last total that fits with a 64-character id: 30 + 20 + 7 = 57, so +116.
        fits = 173 - (TARGET_LEN + DATASET_LEN + PREFIX_LEN)
        warnings = length_warnings(validate_config(self._two(fits + 5, fits)))
        assert len(warnings) == 1
        assert warnings[0].startswith("datasets[tank/a")
        assert "5 over ZFS's 255" in warnings[0] and "up to 59 characters fit" in warnings[0]

    def test_both_warn_independently_with_their_own_margin(self):
        fits = 173 - (TARGET_LEN + DATASET_LEN + PREFIX_LEN)
        warnings = length_warnings(validate_config(self._two(fits + 5, fits + 9)))
        assert len(warnings) == 2
        first, second = warnings
        assert first.startswith("datasets[tank/a") and "5 over" in first and "up to 59" in first
        assert second.startswith("datasets[tank/b") and "9 over" in second and "up to 55" in second

    def test_one_over_long_dataset_refuses_the_whole_configuration(self):
        fits = 173 - (TARGET_LEN + DATASET_LEN + PREFIX_LEN)
        message = refused(self._two(fits, fits + 64))
        assert message.startswith("datasets[tank/b")


@pytest.mark.unit
class TestCopyNameFindingsDirect:
    def test_the_formula_is_target_id_dataset_prefix_and_fourteen_digits(self):
        # target/<1>/dataset@prefix_<14>
        target, dataset, prefix = "bk/out", "tank/data", "snap"
        one = len(target) + 1 + 1 + 1 + len(dataset) + 1 + len(prefix) + 1 + 14
        assert one == len("bk/out/x/tank/data@snap_" + "0" * 14)
        error, warning = _mapper._local_copy_name_findings(dataset, prefix, "here", target)
        assert (error, warning) == (None, None)

    def test_the_error_reports_the_one_character_length_exactly(self):
        target = "b" * 100
        dataset = "tank/" + "d" * 100
        prefix = "p" * 40
        total = len(target) + 1 + 1 + 1 + len(dataset) + 1 + len(prefix) + 1 + 14
        assert total == 264
        error, warning = _mapper._local_copy_name_findings(dataset, prefix, "here", target)
        assert warning is None and f"would be {total} characters" in error

    @pytest.mark.parametrize("name", ["tank/a\nb", "tank/\x1b[31m", "tank/\x00", "tank/\t"])
    def test_a_control_character_is_never_shown_raw(self, name):
        error, warning = _mapper._local_copy_name_findings(name, "autosnap", "here\n", "bk/\x1b")
        assert (error, warning) == (None, None)
        error, _ = _mapper._local_copy_name_findings(name + "d" * 250, "autosnap", "he\nre", "b\x1bk")
        assert error is not None
        assert not [c for c in error if ord(c) < 0x20 or ord(c) == 0x7F]
        _, warning = _mapper._local_copy_name_findings(name + "d" * 160, "autosnap", "he\nre", "b\x1bk")
        assert warning is not None
        assert not [c for c in warning if ord(c) < 0x20 or ord(c) == 0x7F]

    def test_a_destination_name_with_a_control_character_is_shown_escaped(self):
        error, _ = _mapper._local_copy_name_findings("tank/" + "d" * 250, "autosnap", "a\nb", "bk")
        assert "'a\\nb'" in error


@pytest.mark.unit
class TestCopyNameConstantsMatchTheirSources:
    def test_the_full_name_limit_is_libzfseasy_s_max_name_len(self):
        from libzfseasy.types import Validate

        assert _mapper._MAX_FULL_NAME_LEN == Validate.max_name_len == 255

    def test_the_client_id_bounds_match_the_models_grammar(self):
        assert _mapper._CLIENT_ID_MIN_LEN == 1
        assert _mapper._CLIENT_ID_MAX_LEN == 64
        pattern = _model._CLIENT_ID_RE
        assert pattern.fullmatch("a" * _mapper._CLIENT_ID_MIN_LEN)
        assert pattern.fullmatch("a" * _mapper._CLIENT_ID_MAX_LEN)
        assert not pattern.fullmatch("a" * (_mapper._CLIENT_ID_MAX_LEN + 1))
        assert not pattern.fullmatch("")
        assert _model.client_id_problem("a" * _mapper._CLIENT_ID_MAX_LEN) is None
        assert _model.client_id_problem("a" * (_mapper._CLIENT_ID_MAX_LEN + 1)) is not None
        assert _model.client_id_problem("a" * _mapper._CLIENT_ID_MIN_LEN) is None

    def test_a_generated_client_id_never_exceeds_the_maximum(self):
        assert len(_model.generate_client_id()) <= _mapper._CLIENT_ID_MAX_LEN

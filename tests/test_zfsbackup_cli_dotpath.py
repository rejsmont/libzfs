"""`zfsbackup-config get / set / unset` and `zfsbackup/cli/dotpath.py` (plan
Phase 2, item 11), plus the single-source-of-warnings contract they rely on.

Sections:

- Resolution (pure, `BackupConfig` in, segments out): ``TestDotpathResolve``,
  ``TestDotpathAmbiguity``, ``TestDotpathUnknownPaths``, ``TestDotpathBrackets``,
  ``TestDotpathNotAddressable``, ``TestDotpathFormat``.
- Through the CLI: ``TestDotpathGet``, ``TestDotpathSet``, ``TestDotpathUnset``,
  ``TestDotpathNoOp``, ``TestDotpathRetention``, ``TestDotpathRemoteOverrides``,
  ``TestDotpathRemoteBackup``, ``TestDotpathValues``, ``TestDotpathRefusals``
  (the database file's sha256 is unchanged after every refusal),
  ``TestDotpathConflict`` (a racing write -> exit 75), ``TestDotpathBaseline``
  (warnings already true of the stored configuration are not repeated),
  ``TestDotpathTempFiles``.
- ``TestSingleSourceOfWarnings``: `from_file` is silent about the three semantic
  conditions, `validate_config` is their one source (exact texts), every one
  appears exactly once on `import` stderr, `load_config(warn_never_prune=False)`
  and a re-import over a never-prune configuration.

Every database is created by `import` and every edit goes through the real
CLI, store and writer lock. `foreign_keys=ON` and the retention FK therefore
apply as in production.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests
across `test_zfsbackup_cli_dotpath.py`, `test_zfsbackup_cli_list.py`,
`test_zfsbackup_config_audit.py` and `test_zfsbackup_cli.py::{TestWarnings,
TestSkeleton,TestImport}`): ambiguity guessed instead of refused 5; dotted
names not tried as prefixes 93; rule matched by literal on get 1 / on put 3;
first remote override starts from the inherited set 2; last dataset rule ->
default instead of forever 2; last remote override rule left an empty override
1; `remote_backup` created disabled 4 / no-auth warning dropped on create 2 /
on false->true 1; string type not checked 5; no-op decided by dataclass `==`
(literal blind) 4 / never detected 17; temp file 0644 3 / temp dir 0755 3 /
temp dir kept 3; baseline suppression off 13; compare-and-swap off 4; `list`
not quiet 5 / `get` not quiet 6 / quiet also drops errors 1; destinations
`--enabled-only` counting disabled senders 1 / not hiding unused ones 2;
datasets sorted 5; `never` shown as a count 2; `inherit (X)` without X 3;
mapper `warn_never_prune` ignored 4; importer outgoing load warns 3;
`from_file` warning for forever 7 / `_` prefix 5 / port 0 4; `validate_config`
port text reverted 6 / forever text reverted 4; name editable 3; sibling list
uncut 1; `derived` without the interval 8; `prune_interval` not a tri-state 4;
empty value not "unset" 1; names always dotted 62; snapshot re-read after the
edit (CAS defeated) 4.
"""

import logging
import os
import stat
from pathlib import Path

import pytest
import yaml

from _cli_support import (
    AMBIGUOUS_YAML,
    NEVER_PRUNE_TEXT,
    NO_AUTH_TEXT,
    PLAIN_YAML,
    PORT_TEXT,
    PREFIX_TEXT,
    RICH_YAML,
    WARNY_YAML,
    Cli,
    count_containing,
    db,
    db_state,
    generation,
    isolated,
    make_db,
    run,
    stderr_lines,
    stored,
    warnings_in,
    write_yaml,
)
from zfsbackup.cli import dotpath
from zfsbackup.cli import main as cli_main
from zfsbackup.cli.dotpath import PathError
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    dispose_all,
    import_yaml,
    load_config,
    open_config_session,
    read_config,
    resolve_config_path,
    validate_config,
    write_config,
)

# No module-level tier marker: conftest marks every test that declares none as
# `unit`, and the few that spawn a process are marked `integration` individually
# (a test carrying both would be double-counted; see test_marker_tiers.py).

# ---------------------------------------------------------------------------
# Pure resolution
# ---------------------------------------------------------------------------


@pytest.fixture
def rich_config(tmp_path) -> BackupConfig:
    return BackupConfig.from_file(write_yaml(tmp_path, RICH_YAML))


@pytest.fixture
def ambiguous_config(tmp_path) -> BackupConfig:
    return BackupConfig.from_file(write_yaml(tmp_path, AMBIGUOUS_YAML))


def segs_of(text, config):
    return dotpath.resolve(text, config)


#: (spelling the operator may type, canonical segments)
RESOLVES = [
    ("snapshot_prefix", ("snapshot_prefix",)),
    ("check_interval", ("check_interval",)),
    ("prune_interval", ("prune_interval",)),
    ("api_host", ("api_host",)),
    ("api_port", ("api_port",)),
    ("dry_run", ("dry_run",)),
    ("client_id_file", ("client_id_file",)),
    ("datasets", ("datasets",)),
    ("destinations", ("destinations",)),
    ("remote_backup", ("remote_backup",)),
    ("remote_backup.enabled", ("remote_backup", "enabled")),
    ("remote_backup.target_dataset", ("remote_backup", "target_dataset")),
    # dataset names with a slash and with a dot, dotted and bracketed
    ("datasets.tank/data", ("datasets", "tank/data")),
    ("datasets[tank/data]", ("datasets", "tank/data")),
    ("datasets.tank/data.enabled", ("datasets", "tank/data", "enabled")),
    ("datasets.tank/data.recursive", ("datasets", "tank/data", "recursive")),
    ("datasets.tank/data.frequency", ("datasets", "tank/data", "frequency")),
    ("datasets.tank/vm.01", ("datasets", "tank/vm.01")),
    ("datasets.tank/vm.01.frequency", ("datasets", "tank/vm.01", "frequency")),
    ("datasets[tank/vm.01].frequency", ("datasets", "tank/vm.01", "frequency")),
    ("datasets.tank/vm.01.retention", ("datasets", "tank/vm.01", "retention")),
    ("datasets.tank/vm.01.name", ("datasets", "tank/vm.01", "name")),
    # retention rules: dotted, bracketed, either spelling of the age
    ("datasets.tank/data.retention", ("datasets", "tank/data", "retention")),
    ("datasets.tank/data.retention.1d", ("datasets", "tank/data", "retention", "1d")),
    ("datasets.tank/data.retention[1d]", ("datasets", "tank/data", "retention", "1d")),
    ("datasets[tank/data].retention[1w]", ("datasets", "tank/data", "retention", "1w")),
    # remote destinations, including a dotted destination name
    ("datasets.tank/data.remote", ("datasets", "tank/data", "remote")),
    ("datasets.tank/data.remote.offsite", ("datasets", "tank/data", "remote", "offsite")),
    ("datasets.tank/data.remote.offsite.frequency",
     ("datasets", "tank/data", "remote", "offsite", "frequency")),
    ("datasets.tank/data.remote.dc.two", ("datasets", "tank/data", "remote", "dc.two")),
    ("datasets.tank/data.remote.dc.two.frequency",
     ("datasets", "tank/data", "remote", "dc.two", "frequency")),
    ("datasets.tank/data.remote.dc.two.retention",
     ("datasets", "tank/data", "remote", "dc.two", "retention")),
    ("datasets.tank/data.remote.dc.two.retention.1d",
     ("datasets", "tank/data", "remote", "dc.two", "retention", "1d")),
    ("datasets.tank/data.remote[dc.two].retention[1d]",
     ("datasets", "tank/data", "remote", "dc.two", "retention", "1d")),
    ("datasets[tank/data].remote[dc.two].frequency",
     ("datasets", "tank/data", "remote", "dc.two", "frequency")),
    ("datasets[tank/data].remote[dc.two].retention.1d",
     ("datasets", "tank/data", "remote", "dc.two", "retention", "1d")),
    # destinations, including a dotted one
    ("destinations.offsite", ("destinations", "offsite")),
    ("destinations.offsite.url", ("destinations", "offsite", "url")),
    ("destinations.dc.two", ("destinations", "dc.two")),
    ("destinations.dc.two.url", ("destinations", "dc.two", "url")),
    ("destinations[dc.two].url", ("destinations", "dc.two", "url")),
    ("destinations.dc.two.name", ("destinations", "dc.two", "name")),
]


class TestDotpathResolve:
    @pytest.mark.parametrize("text, expected", RESOLVES)
    def test_resolves(self, rich_config, text, expected):
        assert segs_of(text, rich_config) == expected

    @pytest.mark.parametrize("text, expected", RESOLVES)
    def test_the_printed_form_is_the_bracket_form_and_round_trips(self, rich_config, text, expected):
        printed = dotpath.format_path(expected)
        assert segs_of(printed, rich_config) == expected
        # ...and printing is idempotent: the printed form prints as itself.
        assert dotpath.format_path(segs_of(printed, rich_config)) == printed

    @pytest.mark.parametrize("text, expected", RESOLVES)
    def test_the_bracket_form_never_contains_a_dotted_name_unbracketed(self, rich_config, text, expected):
        printed = dotpath.format_path(expected)
        for name in ("tank/vm.01", "dc.two"):
            if name in printed:
                assert f"[{name}]" in printed

    def test_dotted_names_in_every_position(self, rich_config):
        # dataset (collection level), dataset (field level), destination under
        # remote., destination under destinations. -- all contain a '.'.
        assert dotpath.format_path(segs_of("datasets.tank/vm.01.enabled", rich_config)) == \
            "datasets[tank/vm.01].enabled"
        assert dotpath.format_path(segs_of("datasets.tank/data.remote.dc.two.frequency", rich_config)) == \
            "datasets[tank/data].remote[dc.two].frequency"
        assert dotpath.format_path(segs_of("destinations.dc.two.url", rich_config)) == \
            "destinations[dc.two].url"

    def test_a_name_that_is_a_prefix_of_another_is_not_mistaken_for_it(self, tmp_path):
        text = (
            'destinations:\n  dc:\n    url: "http://dc"\n  dc.two:\n    url: "http://two"\n'
            'datasets:\n  - name: "tank/x"\n    remote:\n'
            '      - destination: "dc"\n      - destination: "dc.two"\n'
        )
        config = BackupConfig.from_file(write_yaml(tmp_path, text))
        assert segs_of("datasets.tank/x.remote.dc.frequency", config) == \
            ("datasets", "tank/x", "remote", "dc", "frequency")
        assert segs_of("datasets.tank/x.remote.dc.two.frequency", config) == \
            ("datasets", "tank/x", "remote", "dc.two", "frequency")
        assert segs_of("destinations.dc.url", config) == ("destinations", "dc", "url")
        assert segs_of("destinations.dc.two.url", config) == ("destinations", "dc.two", "url")

    def test_a_name_containing_a_closing_bracket_is_printed_and_resolved_dotted(self, tmp_path):
        text = 'datasets:\n  - name: "weird]name"\n'
        config = BackupConfig.from_file(write_yaml(tmp_path, text))
        segs = segs_of("datasets.weird]name.frequency", config)
        assert segs == ("datasets", "weird]name", "frequency")
        printed = dotpath.format_path(segs)
        assert printed == "datasets.weird]name.frequency"
        assert segs_of(printed, config) == segs

    def test_resolution_does_not_modify_the_config(self, rich_config):
        before = dotpath.to_dict(rich_config)
        for text, _ in RESOLVES:
            segs_of(text, rich_config)
        assert dotpath.to_dict(rich_config) == before


class TestDotpathAmbiguity:
    """Two existing names can both prefix the remainder; both readings valid ->
    an error asking for the bracket form, never a guess."""

    def test_dataset_a_versus_dataset_a_dot_retention(self, ambiguous_config):
        with pytest.raises(PathError) as info:
            segs_of("datasets.a.retention", ambiguous_config)
        message = str(info.value)
        assert "ambiguous" in message
        assert "datasets[a].retention" in message and "datasets[a.retention]" in message
        assert "bracket form" in message

    def test_bracket_form_disambiguates_both_ways(self, ambiguous_config):
        assert segs_of("datasets[a].retention", ambiguous_config) == ("datasets", "a", "retention")
        assert segs_of("datasets[a.retention]", ambiguous_config) == ("datasets", "a.retention")
        assert segs_of("datasets[a.retention].retention", ambiguous_config) == \
            ("datasets", "a.retention", "retention")

    def test_a_longer_remainder_that_only_one_reading_accepts_resolves(self, ambiguous_config):
        # `a.retention` has no setting called `1d`, so only `a` + `retention.1d`
        # is a valid path: no ambiguity to report.
        assert segs_of("datasets.a.retention.1d", ambiguous_config) == \
            ("datasets", "a", "retention", "1d")

    def test_dotted_destination_under_remote(self, ambiguous_config):
        # destination `dc` + setting `retention`  vs  destination `dc.retention`
        with pytest.raises(PathError) as info:
            segs_of("datasets.a.remote.dc.retention", ambiguous_config)
        message = str(info.value)
        assert "ambiguous" in message
        assert "remote[dc].retention" in message and "remote[dc.retention]" in message

    def test_dotted_destination_bracket_disambiguation(self, ambiguous_config):
        assert segs_of("datasets[a].remote[dc].retention", ambiguous_config) == \
            ("datasets", "a", "remote", "dc", "retention")
        assert segs_of("datasets[a].remote[dc.retention]", ambiguous_config) == \
            ("datasets", "a", "remote", "dc.retention")
        assert segs_of("datasets.a.remote.dc.retention.1d", ambiguous_config) == \
            ("datasets", "a", "remote", "dc", "retention", "1d")

    def test_ambiguous_destinations_collection(self, tmp_path):
        text = (
            'destinations:\n  d:\n    url: "http://d"\n  d.url:\n    url: "http://x"\n'
            'datasets:\n  - name: "tank/x"\n'
        )
        config = BackupConfig.from_file(write_yaml(tmp_path, text))
        with pytest.raises(PathError, match="ambiguous"):
            segs_of("destinations.d.url", config)
        assert segs_of("destinations[d].url", config) == ("destinations", "d", "url")
        assert segs_of("destinations[d.url]", config) == ("destinations", "d.url")

    def test_ambiguity_is_reported_for_get_set_and_unset_alike(self, run, db):
        make_db(run, db, AMBIGUOUS_YAML)
        before = db_state(db)
        for args in (
            ("get", "datasets.a.retention"),
            ("set", "datasets.a.retention", "forever"),
            ("unset", "datasets.a.retention"),
        ):
            result = run(*args, "-c", db)
            assert result.exit_code == 1, args
            assert "ambiguous" in result.stderr and "bracket form" in result.stderr
        assert db_state(db) == before

    def test_a_reading_with_an_impossible_age_does_not_count(self, ambiguous_config):
        # `retention.1d` would be a rule age of "retention.1d" for dataset `a`
        # (not a duration); the only usable reading is dataset `a.retention`.
        assert segs_of("datasets.a.retention.retention.1d", ambiguous_config) == \
            ("datasets", "a.retention", "retention", "1d")

    def test_a_possible_age_still_makes_a_real_ambiguity(self, tmp_path):
        # dataset `a` with rule age 1d, and dataset `a.retention.1d`'s own
        # reading `datasets[a.retention].1d` is not a setting: no ambiguity...
        text = 'datasets:\n  - name: "a"\n  - name: "a.retention.1d"\n'
        config = BackupConfig.from_file(write_yaml(tmp_path, text))
        with pytest.raises(PathError, match="ambiguous"):
            segs_of("datasets.a.retention.1d", config)
        assert segs_of("datasets[a].retention.1d", config) == ("datasets", "a", "retention", "1d")
        assert segs_of("datasets[a.retention.1d]", config) == ("datasets", "a.retention.1d")

    @pytest.mark.parametrize("age", ["soon", "1x", "retention.1d", "-1d", "1.5.5d"])
    def test_an_age_that_is_not_a_duration_never_produces_an_ambiguity(self, ambiguous_config, age):
        try:
            segs_of(f"datasets.a.retention.{age}", ambiguous_config)
        except PathError as exc:
            assert "ambiguous" not in str(exc)


class TestDotpathUnknownPaths:
    """An unknown path lists the valid siblings at the deepest level that failed."""

    def fails(self, config, text):
        with pytest.raises(PathError) as info:
            dotpath.resolve(text, config)
        return str(info.value)

    def test_root(self, rich_config):
        message = self.fails(rich_config, "bogus")
        for name in (
            "snapshot_prefix", "check_interval", "prune_interval", "api_host", "api_port",
            "dry_run", "client_id_file", "datasets", "destinations", "remote_backup",
        ):
            assert name in message

    def test_a_per_dataset_setting_at_the_root_points_at_datasets(self, rich_config):
        message = self.fails(rich_config, "frequency")
        assert "datasets[NAME]" in message

    def test_empty_path(self, rich_config):
        assert "valid:" in self.fails(rich_config, "")

    def test_global_with_sub_setting(self, rich_config):
        assert "api_port has no sub-settings" in self.fails(rich_config, "api_port.x")

    def test_dataset_level_lists_the_dataset_names(self, rich_config):
        message = self.fails(rich_config, "datasets.nosuch")
        assert "no dataset matches 'nosuch'" in message
        for name in ("tank/data", "tank/vm.01", "a", "a.retention"):
            assert name in message
        assert "datasets[NAME]" in message  # the hint for dotted names

    def test_bracketed_unknown_dataset(self, rich_config):
        message = self.fails(rich_config, "datasets[nosuch]")
        assert "no dataset named 'nosuch'" in message and "tank/data" in message

    def test_dataset_setting_level_lists_the_settings(self, rich_config):
        for text in ("datasets[tank/data].bogus", "datasets.tank/data.bogus"):
            message = self.fails(rich_config, text)
            assert "datasets[tank/data] has no setting 'bogus'" in message
            for name in ("enabled", "recursive", "frequency", "retention", "remote"):
                assert name in message

    def test_a_scalar_dataset_setting_has_no_sub_settings(self, rich_config):
        assert "frequency has no sub-settings" in self.fails(rich_config, "datasets.tank/data.frequency.x")
        assert "enabled has no sub-settings" in self.fails(rich_config, "datasets.tank/data.enabled.x")

    def test_remote_level_lists_only_the_attached_destinations(self, rich_config):
        message = self.fails(rich_config, "datasets.tank/data.remote.nosuch.frequency")
        assert "offsite" in message and "dc.two" in message
        assert "spare" not in message  # a declared destination, but not attached

        message = self.fails(rich_config, "datasets[tank/data].remote[nosuch]")
        assert "no attached destination named 'nosuch'" in message

    def test_remote_level_with_nothing_attached(self, rich_config):
        message = self.fails(rich_config, "datasets[tank/vm.01].remote[x].frequency")
        assert "(none)" in message

    def test_remote_destination_setting_level(self, rich_config):
        message = self.fails(rich_config, "datasets.tank/data.remote.offsite.bogus")
        assert "frequency" in message and "retention" in message
        assert "remote[offsite] has no setting 'bogus'" in message

    def test_destination_level_lists_the_destination_names(self, rich_config):
        message = self.fails(rich_config, "destinations.nosuch.url")
        for name in ("dc.two", "offsite", "spare"):
            assert name in message
        assert "destinations[NAME]" in message

    def test_destination_setting_level(self, rich_config):
        message = self.fails(rich_config, "destinations.offsite.bogus")
        assert "destinations[offsite] has no setting 'bogus'" in message and "url" in message
        assert "url has no sub-settings" in self.fails(rich_config, "destinations.offsite.url.x")

    def test_remote_backup_level_lists_its_settings(self, rich_config):
        message = self.fails(rich_config, "remote_backup.bogus")
        assert "target_dataset" in message and "enabled" in message

    def test_long_name_lists_are_cut_and_counted(self, tmp_path):
        names = [f"tank/d{i:02}" for i in range(15)]
        text = "datasets:\n" + "".join(f'  - name: "{n}"\n' for n in names)
        config = BackupConfig.from_file(write_yaml(tmp_path, text))
        message = self.fails(config, "datasets.nosuch")
        assert "tank/d00" in message and "tank/d11" in message
        assert "tank/d12" not in message
        assert "(3 more)" in message

    def test_the_cli_prints_the_siblings_and_exits_1(self, run, db):
        make_db(run, db)
        before = db_state(db)
        result = run("get", "datasets.nosuch", "-c", db)
        assert result.exit_code == 1
        assert result.stderr.startswith("error: unknown path 'datasets.nosuch'")
        assert "tank/vm.01" in result.stderr
        assert db_state(db) == before


class TestDotpathBrackets:
    @pytest.mark.parametrize(
        "text",
        [
            "datasets[tank/data",            # unterminated
            "datasets[",
            "datasets[tank/data].retention[1d",
            "datasets[tank/data].remote[offsite",
            "destinations[offsite",
        ],
    )
    def test_unterminated_bracket(self, rich_config, text):
        with pytest.raises(PathError) as info:
            dotpath.resolve(text, rich_config)
        message = str(info.value)
        assert "unterminated" in message or "unexpected" in message
        assert "ambiguous" not in message

    @pytest.mark.parametrize(
        "text",
        [
            "datasets[tank/data]x",           # trailing junk after the bracket
            "datasets[tank/data]]",
            "datasets[tank/data].retention[1d]x",
            "datasets[tank/data].retention[1d]]",
            "datasets[tank/data].remote[offsite]x",
            "destinations[offsite]x",
        ],
    )
    def test_trailing_text_after_a_bracket(self, rich_config, text):
        with pytest.raises(PathError, match="unexpected"):
            dotpath.resolve(text, rich_config)

    def test_trailing_dot_on_a_dataset_or_age(self, rich_config):
        with pytest.raises(PathError, match="no setting"):
            dotpath.resolve("datasets[tank/data].", rich_config)
        with pytest.raises(PathError, match="needs a rule age"):
            dotpath.resolve("datasets[tank/data].retention.", rich_config)

    def test_empty_brackets(self, rich_config):
        with pytest.raises(PathError, match="no dataset named ''"):
            dotpath.resolve("datasets[]", rich_config)
        with pytest.raises(PathError, match="needs a rule age|unexpected"):
            dotpath.resolve("datasets[tank/data].retention[]", rich_config)

    def test_a_bracketed_name_with_a_slash_and_dots(self, rich_config):
        assert dotpath.resolve("datasets[tank/vm.01]", rich_config) == ("datasets", "tank/vm.01")

    def test_unterminated_bracket_through_the_cli(self, run, db):
        make_db(run, db)
        before = db_state(db)
        for args in (
            ("get", "datasets[tank/data"),
            ("set", "datasets[tank/data", "x"),
            ("unset", "datasets[tank/data].retention[1d"),
        ):
            result = run(*args, "-c", db)
            assert result.exit_code == 1, args
            assert "unterminated" in result.stderr or "unexpected" in result.stderr
        assert db_state(db) == before


class TestDotpathNotAddressable:
    """Names and membership resolve (so `get` can say why not) but cannot be
    edited: the error points at `rename` / `edit`."""

    MEMBERSHIP = [
        "datasets",
        "datasets[tank/data]",
        "datasets[tank/data].name",
        "datasets[tank/data].remote",
        "datasets[tank/data].remote[offsite]",
        "datasets[tank/data].remote[offsite].destination",
        "destinations",
        "destinations[offsite]",
        "destinations[offsite].name",
    ]

    @pytest.mark.parametrize("path", MEMBERSHIP)
    def test_set_and_unset_refuse(self, run, db, path):
        make_db(run, db)
        before = db_state(db)
        for verb in (("set", path, "x"), ("unset", path)):
            result = run(*verb, "-c", db)
            assert result.exit_code == 1, verb
            assert "rename" in result.stderr or "edit" in result.stderr, result.stderr
            assert "config updated" not in result.stdout
        assert db_state(db) == before

    def test_names_point_at_rename(self, run, db):
        make_db(run, db)
        for path, noun in (
            ("datasets[tank/data].name", "dataset"),
            ("destinations[offsite].name", "destination"),
        ):
            for verb in ("get", "set", "unset"):
                args = (verb, path) if verb != "set" else (verb, path, "x")
                result = run(*args, "-c", db)
                assert result.exit_code == 1
                assert f"rename {noun}" in result.stderr

    def test_membership_points_at_edit(self, run, db):
        make_db(run, db)
        assert "edit" in run("set", "datasets[a]", "x", "-c", db).stderr
        assert "edit" in run("unset", "destinations[spare]", "-c", db).stderr
        assert "edit" in run("unset", "datasets[tank/data].remote[offsite]", "-c", db).stderr

    def test_get_datasets_points_at_list(self, run, db):
        make_db(run, db)
        result = run("get", "datasets", "-c", db)
        assert result.exit_code == 1
        assert "list datasets" in result.stderr
        assert "list destinations" in run("get", "destinations", "-c", db).stderr
        assert "list datasets" in run("get", "datasets[tank/data].remote", "-c", db).stderr

    def test_a_scalar_cannot_be_set_on_a_structure(self, run, db):
        make_db(run, db)
        result = run("set", "remote_backup", "x", "-c", db)
        assert result.exit_code == 1
        assert "remote_backup.target_dataset" in result.stderr


class TestDotpathFormat:
    def test_collections_and_scalars(self):
        assert dotpath.format_path(("api_port",)) == "api_port"
        assert dotpath.format_path(("datasets",)) == "datasets"
        assert dotpath.format_path(("datasets", "a.b")) == "datasets[a.b]"
        assert dotpath.format_path(("datasets", "a.b", "remote")) == "datasets[a.b].remote"
        assert dotpath.format_path(("datasets", "a", "remote", "x.y", "retention", "1d")) == \
            "datasets[a].remote[x.y].retention.1d"
        assert dotpath.format_path(("destinations", "d.e", "url")) == "destinations[d.e].url"
        assert dotpath.format_path(("remote_backup", "enabled")) == "remote_backup.enabled"


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


@pytest.fixture
def rich(run, db):
    return make_db(run, db)


def ok(run, database, *args):
    result = run(*args, "-c", database)
    assert result.exit_code == 0, f"{args}: {result.stderr}"
    return result


def got(run, database, path):
    return ok(run, database, "get", path).stdout.splitlines()


class TestDotpathGet:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("snapshot_prefix", ["autosnap"]),
            ("check_interval", ["5m"]),
            ("prune_interval", ["derived (5m)"]),
            ("api_host", ["127.0.0.1"]),
            ("api_port", ["8080"]),
            ("dry_run", ["false"]),
            ("client_id_file", ["derived ($HOME/.config/zfsbackup/client_id)"]),
            ("datasets.tank/data.enabled", ["true"]),
            ("datasets.tank/vm.01.enabled", ["false"]),
            ("datasets.tank/data.recursive", ["true"]),
            ("datasets.tank/data.frequency", ["1h"]),
            ("datasets[tank/vm.01].frequency", ["15m"]),
            ("datasets.tank/data.retention", ["1d: 30d", "1w: 3M"]),
            ("datasets.tank/vm.01.retention", ["forever"]),
            ("datasets.tank/data.retention.1d", ["30d"]),
            ("datasets.tank/data.retention.1w", ["3M"]),
            ("datasets.tank/data.remote.offsite.frequency", ["inherit (1h)"]),
            ("datasets.tank/data.remote.dc.two.frequency", ["4h"]),
            ("datasets.tank/data.remote.offsite.retention", ["inherit (1d: 30d, 1w: 3M)"]),
            ("datasets.tank/data.remote.dc.two.retention", ["1d: 7d"]),
            ("datasets.tank/data.remote.dc.two.retention.1d", ["7d"]),
            ("destinations.offsite.url", ["http://offsite:8080"]),
            ("destinations.dc.two.url", ["http://dc2:8080"]),
        ],
    )
    def test_get(self, run, rich, path, expected):
        assert got(run, rich, path) == expected

    def test_a_dataset_prints_its_yaml_subtree(self, run, rich):
        out = run("get", "datasets[tank/data]", "-c", rich).stdout
        parsed = yaml.safe_load(out)
        assert parsed["name"] == "tank/data"
        assert parsed["retention"] == {"1d": "30d", "1w": "3M"}
        assert parsed["remote"] == [
            {"destination": "offsite"},
            {"destination": "dc.two", "frequency": "4h", "retention": {"1d": "7d"}},
        ]

    def test_a_remote_and_a_destination_print_their_subtrees(self, run, rich):
        assert yaml.safe_load(run("get", "datasets[tank/data].remote[dc.two]", "-c", rich).stdout) == {
            "destination": "dc.two", "frequency": "4h", "retention": {"1d": "7d"},
        }
        assert yaml.safe_load(run("get", "destinations[dc.two]", "-c", rich).stdout) == {
            "url": "http://dc2:8080"
        }

    def test_the_retention_rule_is_matched_by_seconds(self, run, rich):
        assert got(run, rich, "datasets[tank/data].retention.7d") == ["3M"]   # stored as 1w
        assert got(run, rich, "datasets[tank/data].retention.24h") == ["30d"]  # stored as 1d
        assert got(run, rich, "datasets[tank/data].retention[7d]") == ["3M"]

    def test_a_missing_rule_lists_the_rules_that_exist(self, run, rich):
        result = run("get", "datasets[tank/data].retention.3d", "-c", rich)
        assert result.exit_code == 1
        assert "no retention rule with age 3d" in result.stderr
        assert "1d: 30d" in result.stderr and "1w: 3M" in result.stderr

    def test_a_bad_age_is_not_a_duration(self, run, rich):
        result = run("get", "datasets[tank/data].retention.xyz", "-c", rich)
        assert result.exit_code == 1 and "not a duration" in result.stderr

    def test_a_rule_on_an_inheriting_destination_explains_it(self, run, rich):
        result = run("get", "datasets[tank/data].remote[offsite].retention.1d", "-c", rich)
        assert result.exit_code == 1
        assert "inherits" in result.stderr and "remote[offsite].retention" in result.stderr

    def test_get_with_no_remote_backup_section(self, run, rich):
        result = run("get", "remote_backup", "-c", rich)
        assert result.exit_code == 1 and "no remote_backup section" in result.stderr
        assert run("get", "remote_backup.enabled", "-c", rich).exit_code == 1

    def test_get_remote_backup(self, run, db):
        make_db(run, db, RICH_YAML + 'remote_backup:\n  enabled: true\n  target_dataset: "tank/recv"\n')
        assert got(run, db, "remote_backup.enabled") == ["true"]
        assert got(run, db, "remote_backup.target_dataset") == ["tank/recv"]
        assert yaml.safe_load(run("get", "remote_backup", "-c", db).stdout) == {
            "enabled": True, "target_dataset": "tank/recv",
        }

    def test_prune_interval_and_client_id_file_when_set(self, run, db):
        make_db(run, db, RICH_YAML.replace(
            'check_interval: "5m"\n',
            'check_interval: "5m"\nprune_interval: "2h"\nclient_id_file: "/var/lib/zfsbackup/client_id"\n',
        ))
        assert got(run, db, "prune_interval") == ["2h"]
        assert got(run, db, "client_id_file") == ["/var/lib/zfsbackup/client_id"]

    def test_durations_are_shown_as_typed(self, run, db):
        make_db(run, db, PLAIN_YAML.replace('"5m"', '"300s"').replace('"1h"', '"60m"'))
        assert got(run, db, "check_interval") == ["300s"]
        assert got(run, db, "datasets[tank/a].frequency") == ["60m"]

    def test_get_writes_nothing(self, run, rich):
        before, gen = db_state(rich), generation(rich)
        for path in ("api_port", "datasets[tank/data]", "datasets.tank/data.retention"):
            ok(run, rich, "get", path)
        assert db_state(rich) == before and generation(rich) == gen

    def test_bracket_and_dotted_spellings_print_the_same(self, run, rich):
        assert got(run, rich, "datasets.tank/vm.01.frequency") == got(run, rich, "datasets[tank/vm.01].frequency")


# ---------------------------------------------------------------------------
# set / unset
# ---------------------------------------------------------------------------

#: (path, value, printed "PATH: old -> new" line, `get` of the printed path afterwards)
SETS = [
    ("snapshot_prefix", "snap", "snapshot_prefix: autosnap -> snap", ["snap"]),
    ("check_interval", "10m", "check_interval: 5m -> 10m", ["10m"]),
    ("prune_interval", "2h", "prune_interval: derived (5m) -> 2h", ["2h"]),
    ("api_host", "0.0.0.0", "api_host: 127.0.0.1 -> 0.0.0.0", ["0.0.0.0"]),
    ("api_port", "9090", "api_port: 8080 -> 9090", ["9090"]),
    ("dry_run", "true", "dry_run: false -> true", ["true"]),
    ("client_id_file", "/var/lib/zfsbackup/client_id",
     "client_id_file: derived ($HOME/.config/zfsbackup/client_id) -> /var/lib/zfsbackup/client_id",
     ["/var/lib/zfsbackup/client_id"]),
    ("datasets.tank/data.enabled", "false", "datasets[tank/data].enabled: true -> false", ["false"]),
    ("datasets.tank/vm.01.enabled", "true", "datasets[tank/vm.01].enabled: false -> true", ["true"]),
    ("datasets.tank/data.recursive", "false", "datasets[tank/data].recursive: true -> false", ["false"]),
    ("datasets.tank/data.frequency", "30m", "datasets[tank/data].frequency: 1h -> 30m", ["30m"]),
    ("datasets.tank/vm.01.frequency", "20m", "datasets[tank/vm.01].frequency: 15m -> 20m", ["20m"]),
    ("datasets.tank/data.retention.1d", "14d",
     "datasets[tank/data].retention: 1d: 30d, 1w: 3M -> 1d: 14d, 1w: 3M", ["1d: 14d", "1w: 3M"]),
    ("datasets.tank/data.retention.2w", "6M",
     "datasets[tank/data].retention: 1d: 30d, 1w: 3M -> 1d: 30d, 1w: 3M, 2w: 6M",
     ["1d: 30d", "1w: 3M", "2w: 6M"]),
    ("datasets.tank/data.retention", "{1h: 1d, 1w: 1M}",
     "datasets[tank/data].retention: 1d: 30d, 1w: 3M -> 1h: 1d, 1w: 1M", ["1h: 1d", "1w: 1M"]),
    ("datasets.tank/data.remote.offsite.frequency", "2h",
     "datasets[tank/data].remote[offsite].frequency: inherit (1h) -> 2h", ["2h"]),
    ("datasets.tank/data.remote.dc.two.frequency", "2h",
     "datasets[tank/data].remote[dc.two].frequency: 4h -> 2h", ["2h"]),
    ("datasets.tank/data.remote.dc.two.retention.1d", "9d",
     "datasets[tank/data].remote[dc.two].retention: 1d: 7d -> 1d: 9d", ["1d: 9d"]),
    ("datasets.tank/data.remote.dc.two.retention", "{1h: 1d, 1w: 1M}",
     "datasets[tank/data].remote[dc.two].retention: 1d: 7d -> 1h: 1d, 1w: 1M", ["1h: 1d", "1w: 1M"]),
    ("destinations.offsite.url", "http://new:1", "destinations[offsite].url: http://offsite:8080 -> http://new:1",
     ["http://new:1"]),
    ("destinations.dc.two.url", "http://new:2", "destinations[dc.two].url: http://dc2:8080 -> http://new:2",
     ["http://new:2"]),
]


def shown(path):
    """What `set` prints after the settings path (the bracket form of `path`)."""
    return path


class TestDotpathSet:
    @pytest.mark.parametrize("path, value, line, after", SETS)
    def test_set(self, run, rich, path, value, line, after):
        gen = generation(rich)
        result = run("set", path, value, "-c", rich)
        assert result.exit_code == 0, result.stderr
        out = result.stdout.splitlines()
        assert out[0] == line
        # then the daemon notice (the generation the write produced)
        assert out[1].startswith(f"config updated (generation {gen + 1})")
        assert generation(rich) == gen + 1
        # what was stored is what `get` now says (print path = the first segment of `line`)
        printed_path = line.split(":", 1)[0] if not line.startswith("datasets[") else line.split(": ", 1)[0]
        assert got(run, rich, printed_path) == after

    @pytest.mark.parametrize("path, value, line, after", SETS)
    def test_the_stored_config_changes_in_exactly_that_setting(self, run, rich, path, value, line, after):
        before = dotpath.to_dict(stored(rich))
        ok(run, rich, "set", path, value)
        changed = dotpath.to_dict(stored(rich))
        assert changed != before
        # Only one top-level section moved (an omitted tri-state counts as a key).
        moved = {k for k in set(before) | set(changed) if before.get(k) != changed.get(k)}
        assert len(moved) == 1, moved

    def test_set_prints_the_bracket_form_for_a_dotted_input(self, run, rich):
        result = ok(run, rich, "set", "datasets.tank/vm.01.frequency", "20m")
        assert result.stdout.startswith("datasets[tank/vm.01].frequency: ")
        result = ok(run, rich, "set", "datasets.tank/data.remote.dc.two.frequency", "3h")
        assert result.stdout.startswith("datasets[tank/data].remote[dc.two].frequency: ")

    def test_set_null_on_a_dataset_retention_restores_the_default(self, run, rich):
        result = ok(run, rich, "set", "datasets[tank/vm.01].retention", "null")
        assert result.stdout.splitlines()[0] == "datasets[tank/vm.01].retention: forever -> 1d: 30d"

    def test_a_dotted_destination_edit_leaves_its_neighbour_alone(self, run, rich):
        ok(run, rich, "set", "datasets.tank/data.remote.dc.two.frequency", "3h")
        assert got(run, rich, "datasets[tank/data].remote[offsite].frequency") == ["inherit (1h)"]
        assert got(run, rich, "datasets[tank/data].remote[dc.two].retention") == ["1d: 7d"]

    def test_set_works_with_c_before_the_subcommand(self, run, rich):
        result = run("-c", rich, "set", "api_port", "9091")
        assert result.exit_code == 0
        assert got(run, rich, "api_port") == ["9091"]

    def test_set_requires_a_value(self, run, rich):
        assert run("set", "api_port", "-c", rich).exit_code == 2

    def test_the_daemon_notice_follows_every_write(self, run, rich):
        out = ok(run, rich, "set", "api_port", "9092").stdout
        assert "no running daemon detected" in out


class TestDotpathUnset:
    def test_prune_interval_tri_state(self, run, rich):
        ok(run, rich, "set", "prune_interval", "2h")
        assert got(run, rich, "prune_interval") == ["2h"]
        result = ok(run, rich, "unset", "prune_interval")
        assert result.stdout.splitlines()[0] == "prune_interval: 2h -> derived (5m)"
        assert got(run, rich, "prune_interval") == ["derived (5m)"]
        assert stored(rich).prune_interval is None

    def test_derived_prune_interval_follows_check_interval(self, run, rich):
        ok(run, rich, "set", "check_interval", "10m")
        assert got(run, rich, "prune_interval") == ["derived (10m)"]

    def test_client_id_file_tri_state(self, run, rich):
        ok(run, rich, "set", "client_id_file", "/x/id")
        result = ok(run, rich, "unset", "client_id_file")
        assert result.stdout.splitlines()[0] == (
            "client_id_file: /x/id -> derived ($HOME/.config/zfsbackup/client_id)"
        )
        assert stored(rich).client_id_file is None

    @pytest.mark.parametrize("empty", ["null", "''", "~"])
    def test_setting_an_empty_value_on_a_tri_state_unsets_it(self, run, rich, empty):
        ok(run, rich, "set", "prune_interval", "2h")
        result = ok(run, rich, "set", "prune_interval", empty)
        assert result.stdout.splitlines()[0] == "prune_interval: 2h -> derived (5m)"
        ok(run, rich, "set", "client_id_file", "/x/id")
        ok(run, rich, "set", "client_id_file", empty)
        assert stored(rich).client_id_file is None

    def test_dataset_retention_returns_to_the_default(self, run, rich):
        result = ok(run, rich, "unset", "datasets[tank/data].retention")
        assert result.stdout.splitlines()[0] == (
            "datasets[tank/data].retention: 1d: 30d, 1w: 3M -> 1d: 30d"
        )
        forever = ok(run, rich, "unset", "datasets[tank/vm.01].retention")
        assert forever.stdout.splitlines()[0] == "datasets[tank/vm.01].retention: forever -> 1d: 30d"
        assert [(r.age.literal, r.keep_for.literal)
                for r in stored(rich).datasets[1].retention_rules] == [("1d", "30d")]

    def test_one_retention_rule(self, run, rich):
        result = ok(run, rich, "unset", "datasets[tank/data].retention.1w")
        assert result.stdout.splitlines()[0] == "datasets[tank/data].retention: 1d: 30d, 1w: 3M -> 1d: 30d"
        assert got(run, rich, "datasets[tank/data].retention") == ["1d: 30d"]

    def test_a_retention_rule_is_found_by_its_length(self, run, rich):
        result = ok(run, rich, "unset", "datasets[tank/data].retention.7d")  # stored as 1w
        assert "-> 1d: 30d" in result.stdout.splitlines()[0]

    def test_a_missing_rule_is_an_error_listing_the_rules(self, run, rich):
        before = db_state(rich)
        result = run("unset", "datasets[tank/data].retention.3d", "-c", rich)
        assert result.exit_code == 1
        assert "no retention rule with age 3d" in result.stderr and "1w: 3M" in result.stderr
        assert db_state(rich) == before

    def test_remote_frequency_returns_to_inherit(self, run, rich):
        result = ok(run, rich, "unset", "datasets[tank/data].remote[dc.two].frequency")
        assert result.stdout.splitlines()[0] == (
            "datasets[tank/data].remote[dc.two].frequency: 4h -> inherit (1h)"
        )
        assert stored(rich).datasets[0].remote[1].frequency is None

    def test_remote_retention_returns_to_inherit(self, run, rich):
        result = ok(run, rich, "unset", "datasets[tank/data].remote[dc.two].retention")
        assert result.stdout.splitlines()[0] == (
            "datasets[tank/data].remote[dc.two].retention: 1d: 7d -> inherit (1d: 30d, 1w: 3M)"
        )
        assert stored(rich).datasets[0].remote[1].retention_rules == []

    @pytest.mark.parametrize(
        "path",
        [
            "snapshot_prefix", "check_interval", "api_host", "api_port", "dry_run",
            "datasets[tank/data].enabled", "datasets[tank/data].recursive",
            "datasets[tank/data].frequency", "destinations[offsite].url",
        ],
    )
    def test_settings_that_always_have_a_value_cannot_be_unset(self, run, rich, path):
        before = db_state(rich)
        result = run("unset", path, "-c", rich)
        assert result.exit_code == 1
        assert "cannot be unset" in result.stderr and "set" in result.stderr
        assert db_state(rich) == before

    def test_remote_backup_fields_cannot_be_unset_alone(self, run, db):
        make_db(run, db, RICH_YAML + 'remote_backup:\n  enabled: true\n  target_dataset: "tank/recv"\n')
        before = db_state(db)
        for field in ("enabled", "target_dataset"):
            result = run("unset", f"remote_backup.{field}", "-c", db)
            assert result.exit_code == 1 and "unset remote_backup" in result.stderr
        assert db_state(db) == before


class TestDotpathNoOp:
    """A no-op is judged on the rendered YAML, so a change of literal is a real
    edit; a true no-op prints one line, exits 0 and does not write."""

    @pytest.mark.parametrize(
        "args, line",
        [
            (("set", "check_interval", "5m"), "check_interval is already 5m; nothing changed"),
            (("set", "api_port", "8080"), "api_port is already 8080; nothing changed"),
            (("set", "dry_run", "false"), "dry_run is already false; nothing changed"),
            (("set", "snapshot_prefix", "autosnap"), "snapshot_prefix is already autosnap; nothing changed"),
            (("set", "datasets.tank/data.frequency", "1h"),
             "datasets[tank/data].frequency is already 1h; nothing changed"),
            (("set", "datasets.tank/data.retention.1d", "30d"),
             "datasets[tank/data].retention is already 1d: 30d, 1w: 3M; nothing changed"),
            (("set", "datasets.tank/vm.01.retention", "forever"),
             "datasets[tank/vm.01].retention is already forever; nothing changed"),
            (("set", "datasets.tank/data.remote.dc.two.frequency", "4h"),
             "datasets[tank/data].remote[dc.two].frequency is already 4h; nothing changed"),
            (("set", "destinations.offsite.url", "http://offsite:8080"),
             "destinations[offsite].url is already http://offsite:8080; nothing changed"),
            (("unset", "prune_interval"),
             "prune_interval is already derived (5m); nothing changed"),
            (("unset", "client_id_file"),
             "client_id_file is already derived ($HOME/.config/zfsbackup/client_id); nothing changed"),
            (("set", "client_id_file", "null"),
             "client_id_file is already derived ($HOME/.config/zfsbackup/client_id); nothing changed"),
            (("unset", "datasets.tank/data.remote.offsite.frequency"),
             "datasets[tank/data].remote[offsite].frequency is already inherit (1h); nothing changed"),
            (("unset", "datasets.tank/data.remote.offsite.retention"),
             "datasets[tank/data].remote[offsite].retention is already inherit (1d: 30d, 1w: 3M); "
             "nothing changed"),
            (("unset", "remote_backup"), "remote_backup is not set; nothing changed"),
        ],
    )
    def test_noop(self, run, rich, args, line):
        before, gen = db_state(rich), generation(rich)
        result = run(*args, "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert result.stdout == line + "\n"  # one line: no daemon notice either
        assert db_state(rich) == before
        assert generation(rich) == gen

    @pytest.mark.parametrize(
        "args, line, check_path, check_value",
        [
            (("set", "check_interval", "300s"), "check_interval: 5m -> 300s", "check_interval", ["300s"]),
            (("set", "datasets.tank/data.frequency", "60m"),
             "datasets[tank/data].frequency: 1h -> 60m", "datasets[tank/data].frequency", ["60m"]),
            # the age 7d is the same length as the stored 1w: the rule is
            # replaced and the NEW literal is what is stored
            (("set", "datasets.tank/data.retention.7d", "3M"),
             "datasets[tank/data].retention: 1d: 30d, 1w: 3M -> 1d: 30d, 7d: 3M",
             "datasets[tank/data].retention", ["1d: 30d", "7d: 3M"]),
            # same length keep_for, different literal
            (("set", "datasets.tank/data.retention.1d", "720h"),
             "datasets[tank/data].retention: 1d: 30d, 1w: 3M -> 1d: 720h, 1w: 3M",
             "datasets[tank/data].retention", ["1d: 720h", "1w: 3M"]),
            # an explicit frequency equal to the inherited one is still a change
            (("set", "datasets.tank/data.remote.offsite.frequency", "1h"),
             "datasets[tank/data].remote[offsite].frequency: inherit (1h) -> 1h",
             "datasets[tank/data].remote[offsite].frequency", ["1h"]),
        ],
    )
    def test_a_literal_only_change_is_a_real_edit(self, run, rich, args, line, check_path, check_value):
        gen = generation(rich)
        result = run(*args, "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert result.stdout.splitlines()[0] == line
        assert generation(rich) == gen + 1
        assert got(run, rich, check_path) == check_value

    def test_a_noop_does_not_even_touch_the_daemon_probe(self, run, rich, monkeypatch):
        called = []
        monkeypatch.setattr(cli_main, "notify_daemon", lambda *a, **k: called.append(a))
        ok(run, rich, "set", "check_interval", "5m")
        assert called == []

    def test_a_noop_still_reports_a_refusal_of_a_bad_value(self, run, rich):
        assert run("set", "check_interval", "bogus", "-c", rich).exit_code == 1


class TestDotpathRetention:
    def test_replacing_a_rule_stores_the_literal_as_typed(self, run, rich):
        ok(run, rich, "set", "datasets[tank/data].retention.168h", "2M")
        rules = stored(rich).datasets[0].retention_rules
        assert [(r.age.literal, r.keep_for.literal) for r in rules] == [("1d", "30d"), ("168h", "2M")]

    def test_removing_the_last_rule_is_forever_with_one_warning(self, run, db):
        make_db(run, db, PLAIN_YAML)
        result = ok(run, db, "unset", "datasets[tank/a].retention.1d")
        assert result.stdout.splitlines()[0] == "datasets[tank/a].retention: 1d: 30d -> forever"
        assert count_containing(warnings_in(result), "never be pruned") == 1
        assert count_containing(warnings_in(result), "tank/a].retention is empty") == 1
        assert stored(db).datasets[0].retention_rules == []
        assert got(run, db, "datasets[tank/a].retention") == ["forever"]

    def test_set_forever_is_never_prune_with_one_warning(self, run, db):
        make_db(run, db, PLAIN_YAML)
        result = ok(run, db, "set", "datasets[tank/a].retention", "forever")
        assert result.stdout.splitlines()[0] == "datasets[tank/a].retention: 1d: 30d -> forever"
        assert len(warnings_in(result)) == 1
        assert "never be pruned" in warnings_in(result)[0]

    def test_a_keep_for_collision_is_refused_before_writing(self, run, rich):
        before = db_state(rich)
        # 3 days and 3 days... `1d: 30d` exists; `7d: 30d` collides on keep_for
        result = run("set", "datasets[tank/data].retention.2d", "30d", "-c", rich)
        assert result.exit_code == 1
        assert "same duration" in result.stderr
        assert db_state(rich) == before

    def test_an_age_collision_replaces_instead_of_colliding(self, run, rich):
        # 24h == 1d: matched by seconds, so this REPLACES `1d: 30d`.
        ok(run, rich, "set", "datasets[tank/data].retention.24h", "20d")
        rules = stored(rich).datasets[0].retention_rules
        assert [(r.age.literal, r.keep_for.literal) for r in rules] == [("1w", "3M"), ("24h", "20d")] or \
            [(r.age.literal, r.keep_for.literal) for r in rules] == [("24h", "20d"), ("1w", "3M")]

    def test_the_effective_set_is_shown_after_a_rule_edit(self, run, rich):
        line = ok(run, rich, "set", "datasets[tank/data].retention.1h", "1d").stdout.splitlines()[0]
        assert line == "datasets[tank/data].retention: 1d: 30d, 1w: 3M -> 1h: 1d, 1d: 30d, 1w: 3M"

    def test_a_non_duration_age_or_keep_for_is_refused(self, run, rich):
        before = db_state(rich)
        for args in (
            ("set", "datasets[tank/data].retention.soon", "30d"),
            ("set", "datasets[tank/data].retention.1h", "forever"),
            ("set", "datasets[tank/data].retention.1h", "0d"),
        ):
            assert run(*args, "-c", rich).exit_code == 1, args
        assert db_state(rich) == before


class TestDotpathRemoteOverrides:
    def test_the_first_override_rule_replaces_the_inherited_set(self, run, rich):
        # `offsite` inherits `1d: 30d, 1w: 3M`; its FIRST override rule starts
        # from nothing, it does not append to the inherited rules.
        result = ok(run, rich, "set", "datasets[tank/data].remote[offsite].retention.1h", "2d")
        assert result.stdout.splitlines()[0] == (
            "datasets[tank/data].remote[offsite].retention: inherit (1d: 30d, 1w: 3M) -> 1h: 2d"
        )
        rules = stored(rich).datasets[0].remote[0].retention_rules
        assert [(r.age.literal, r.keep_for.literal) for r in rules] == [("1h", "2d")]
        # the dataset's own rules are untouched
        assert got(run, rich, "datasets[tank/data].retention") == ["1d: 30d", "1w: 3M"]

    def test_the_second_rule_adds_to_the_override(self, run, rich):
        ok(run, rich, "set", "datasets[tank/data].remote[offsite].retention.1h", "2d")
        result = ok(run, rich, "set", "datasets[tank/data].remote[offsite].retention.2d", "9d")
        assert result.stdout.splitlines()[0].endswith("1h: 2d -> 1h: 2d, 2d: 9d")

    def test_removing_the_last_override_rule_inherits(self, run, rich):
        result = ok(run, rich, "unset", "datasets[tank/data].remote[dc.two].retention.1d")
        assert result.stdout.splitlines()[0] == (
            "datasets[tank/data].remote[dc.two].retention: 1d: 7d -> inherit (1d: 30d, 1w: 3M)"
        )
        assert stored(rich).datasets[0].remote[1].retention_rules == []
        assert got(run, rich, "datasets[tank/data].remote[dc.two].retention") == ["inherit (1d: 30d, 1w: 3M)"]

    def test_removing_one_of_two_override_rules_keeps_the_override(self, run, rich):
        ok(run, rich, "set", "datasets[tank/data].remote[dc.two].retention.1w", "1M")
        result = ok(run, rich, "unset", "datasets[tank/data].remote[dc.two].retention.1w")
        assert result.stdout.splitlines()[0].endswith("1d: 7d, 1w: 1M -> 1d: 7d")

    def test_unset_a_rule_of_an_inheriting_destination_is_an_error(self, run, rich):
        before = db_state(rich)
        result = run("unset", "datasets[tank/data].remote[offsite].retention.1d", "-c", rich)
        assert result.exit_code == 1 and "no retention override" in result.stderr
        assert db_state(rich) == before

    def test_an_empty_override_mapping_is_refused(self, run, rich):
        before = db_state(rich)
        result = run("set", "datasets[tank/data].remote[dc.two].retention", "{}", "-c", rich)
        assert result.exit_code == 1 and "empty" in result.stderr
        assert db_state(rich) == before

    def test_set_null_on_remote_retention_inherits(self, run, rich):
        result = ok(run, rich, "set", "datasets[tank/data].remote[dc.two].retention", "null")
        assert result.stdout.splitlines()[0].endswith("1d: 7d -> inherit (1d: 30d, 1w: 3M)")

    def test_remote_frequency_zero_means_inherit(self, run, rich):
        result = ok(run, rich, "set", "datasets[tank/data].remote[dc.two].frequency", "0m")
        assert result.stdout.splitlines()[0].endswith("4h -> inherit (1h)")
        assert "'0m'" in result.stderr  # from_file's own warning survives

    def test_inherit_shows_the_effective_dataset_frequency(self, run, rich):
        ok(run, rich, "set", "datasets[tank/data].frequency", "20m")
        assert got(run, rich, "datasets[tank/data].remote[offsite].frequency") == ["inherit (20m)"]


class TestDotpathRemoteBackup:
    def test_target_dataset_creates_the_role_enabled_with_a_no_auth_warning(self, run, rich):
        result = ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv")
        assert result.stdout.splitlines()[0] == "remote_backup.target_dataset: (not set) -> tank/recv"
        assert count_containing(warnings_in(result), NO_AUTH_TEXT) == 1
        assert "127.0.0.1:8080" in result.stderr
        server = stored(rich).remote_backup
        assert server.enabled is True and server.target_dataset == "tank/recv"

    def test_enabled_cannot_create_the_section(self, run, rich):
        before = db_state(rich)
        result = run("set", "remote_backup.enabled", "true", "-c", rich)
        assert result.exit_code == 1 and "target_dataset" in result.stderr
        assert db_state(rich) == before

    def test_false_then_true_warns_again_only_for_true(self, run, rich):
        ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv")
        off = ok(run, rich, "set", "remote_backup.enabled", "false")
        assert off.stdout.splitlines()[0] == "remote_backup.enabled: true -> false"
        assert count_containing(warnings_in(off), NO_AUTH_TEXT) == 0
        on = ok(run, rich, "set", "remote_backup.enabled", "true")
        assert on.stdout.splitlines()[0] == "remote_backup.enabled: false -> true"
        assert count_containing(warnings_in(on), NO_AUTH_TEXT) == 1

    def test_true_when_already_true_is_a_silent_noop(self, run, rich):
        ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv")
        before = db_state(rich)
        result = ok(run, rich, "set", "remote_backup.enabled", "true")
        assert result.stdout == "remote_backup.enabled is already true; nothing changed\n"
        assert count_containing(warnings_in(result), NO_AUTH_TEXT) == 0
        assert db_state(rich) == before

    def test_changing_the_target_of_an_existing_role_does_not_warn(self, run, rich):
        ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv")
        result = ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv2")
        assert result.stdout.splitlines()[0] == "remote_backup.target_dataset: tank/recv -> tank/recv2"
        assert warnings_in(result) == []

    def test_changing_the_target_of_a_disabled_role_keeps_it_disabled(self, run, rich):
        ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv")
        ok(run, rich, "set", "remote_backup.enabled", "false")
        result = ok(run, rich, "set", "remote_backup.target_dataset", "tank/other")
        assert warnings_in(result) == []
        assert stored(rich).remote_backup.enabled is False

    def test_unset_removes_the_role(self, run, rich):
        ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv")
        result = ok(run, rich, "unset", "remote_backup")
        assert result.stdout.splitlines()[0] == (
            "remote_backup: enabled: true, target_dataset: tank/recv -> (not set)"
        )
        assert stored(rich).remote_backup is None
        assert run("get", "remote_backup", "-c", rich).exit_code == 1

    def test_the_warning_uses_the_configured_api_address(self, run, rich):
        ok(run, rich, "set", "api_host", "10.1.2.3")
        ok(run, rich, "set", "api_port", "9999")
        result = ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv")
        assert "10.1.2.3:9999" in result.stderr

    def test_a_non_string_target_is_refused_with_a_quoting_hint(self, run, rich):
        before = db_state(rich)
        result = run("set", "remote_backup.target_dataset", "123", "-c", rich)
        assert result.exit_code == 1
        assert "must be a string" in result.stderr and "quote" in result.stderr
        assert db_state(rich) == before


class TestDotpathValues:
    @pytest.mark.parametrize(
        "path",
        ["snapshot_prefix", "api_host", "client_id_file", "destinations[offsite].url"],
    )
    def test_string_settings_reject_non_string_yaml_with_a_quoting_hint(self, run, rich, path):
        before = db_state(rich)
        for value in ("123", "true", "1.5", "[a, b]", "{a: b}"):
            result = run("set", path, value, "-c", rich)
            assert result.exit_code == 1, (path, value)
            assert "must be a string" in result.stderr
            assert "quote" in result.stderr and '"' in result.stderr
        assert db_state(rich) == before

    def test_quoted_text_is_accepted(self, run, rich):
        ok(run, rich, "set", "snapshot_prefix", '"123"')
        assert got(run, rich, "snapshot_prefix") == ["123"]
        ok(run, rich, "set", "api_host", '"1"')
        assert stored(rich).api_host == "1"

    def test_null_for_snapshot_prefix_is_rejected_by_the_model(self, run, rich):
        before = db_state(rich)
        result = run("set", "snapshot_prefix", "null", "-c", rich)
        assert result.exit_code == 1 and "snapshot_prefix" in result.stderr
        assert db_state(rich) == before

    def test_values_are_read_as_yaml(self, run, rich):
        ok(run, rich, "set", "dry_run", "yes")
        assert got(run, rich, "dry_run") == ["true"]
        ok(run, rich, "set", "api_port", '"9090"')  # digit strings are accepted
        assert got(run, rich, "api_port") == ["9090"]

    def test_integer_booleans_are_accepted_with_a_warning(self, run, rich):
        result = ok(run, rich, "set", "datasets[tank/vm.01].enabled", "1")
        assert result.stdout.splitlines()[0] == "datasets[tank/vm.01].enabled: false -> true"
        assert count_containing(warnings_in(result), "integer 1") == 1

    def test_a_yaml_syntax_error_in_the_value_is_refused(self, run, rich):
        before = db_state(rich)
        result = run("set", "snapshot_prefix", "[unclosed", "-c", rich)
        assert result.exit_code == 1
        assert "cannot read the value" in result.stderr and "quote" in result.stderr
        assert db_state(rich) == before

    def test_negative_values_need_double_dash(self, run, rich):
        before = db_state(rich)
        # Without `--` Click takes `-5m` for an option: a usage error.
        assert run("set", "check_interval", "-5m", "-c", rich).exit_code == 2
        # With it the value reaches the model, which refuses it.
        result = run("-c", rich, "set", "--", "check_interval", "-5m")
        assert result.exit_code == 1 and "positive" in result.stderr
        assert db_state(rich) == before

    @pytest.mark.parametrize(
        "path, value",
        [
            ("check_interval", "0m"),
            ("prune_interval", "0m"),
            ("datasets[tank/data].frequency", "0m"),
            ("datasets[tank/data].frequency", "soon"),
            ("api_port", "70000"),
            ("api_port", "-1"),
            ("api_port", "true"),
            ("api_port", "80.5"),
            ("api_port", "null"),
            ("dry_run", "maybe"),
            ("dry_run", "2"),
            ("datasets[tank/data].enabled", "null_or_not"),
        ],
    )
    def test_bad_values_are_refused_and_name_the_setting(self, run, rich, path, value):
        before = db_state(rich)
        result = run("-c", rich, "set", "--", path, value)
        assert result.exit_code == 1, result.stdout
        assert result.stderr.startswith("error: ")
        assert "Traceback" not in result.stderr
        assert db_state(rich) == before


class TestDotpathRefusals:
    """Whatever the reason, a refusal exits non-zero WITHOUT touching the file:
    sha256 of the database and the set of sidecar files are unchanged."""

    REFUSALS = [
        # (args, exit code, stderr fragment)
        (("set", "bogus", "1"), 1, "unknown path"),
        (("unset", "bogus"), 1, "unknown path"),
        (("set", "datasets.nosuch.frequency", "1h"), 1, "no dataset matches"),
        (("set", "datasets[tank/data].remote[nosuch].frequency", "1h"), 1, "no attached destination"),
        (("set", "destinations.nosuch.url", "http://x"), 1, "no destination matches"),
        (("set", "check_interval", "not-a-duration"), 1, "Invalid duration"),
        (("set", "api_port", "99999"), 1, "api_port"),
        (("set", "datasets[tank/data].frequency", "0m"), 1, "frequency"),
        (("set", "datasets[tank/data].retention.2d", "30d"), 1, "same duration"),   # keep_for collision
        (("set", "datasets[tank/data].remote[dc.two].retention", "{}"), 1, "empty"),
        (("set", "datasets[tank/data].retention", "[1, 2]"), 1, "retention"),
        (("set", "datasets[tank/data].name", "x"), 1, "rename dataset"),
        (("unset", "datasets[tank/data]"), 1, "edit"),
        (("unset", "check_interval"), 1, "cannot be unset"),
        (("set", "snapshot_prefix", "123"), 1, "must be a string"),
        (("set", "remote_backup.enabled", "true"), 1, "target_dataset"),
        (("unset", "datasets[tank/data].remote[offsite].retention.1d"), 1, "no retention override"),
        (("unset", "datasets[tank/data].retention.9y"), 1, "no retention rule"),
        (("set", "check_interval"), 2, "Missing argument"),
    ]

    @pytest.mark.parametrize("args, code, fragment", REFUSALS)
    def test_refusal_writes_nothing(self, run, rich, args, code, fragment):
        before, gen = db_state(rich), generation(rich)
        result = run(*args, "-c", rich)
        assert result.exit_code == code, result.stdout + result.stderr
        assert fragment in result.stderr
        assert "config updated" not in result.stdout
        assert "Traceback" not in result.stderr
        assert db_state(rich) == before
        assert generation(rich) == gen

    def test_a_validation_failure_after_the_edit_writes_nothing(self, run, rich, monkeypatch):
        # The edit parses; `validate_config` (run by `commit_config` on the new
        # configuration) is what refuses. Provoke it with a keep_for collision
        # whose ages differ, which only the strict validator catches.
        before = db_state(rich)
        result = run("set", "datasets[tank/data].retention.1h", "30d", "-c", rich)
        assert result.exit_code == 1 and "same duration" in result.stderr
        assert db_state(rich) == before

    def test_no_backup_or_temp_files_are_left_beside_the_database(self, run, rich):
        run("set", "check_interval", "bogus", "-c", rich)
        ok(run, rich, "set", "check_interval", "10m")
        assert sorted(p.name for p in rich.parent.iterdir()) == sorted(
            p.name for p in rich.parent.iterdir() if not p.name.startswith(".")
            and ".tmp" not in p.name and "pre-import" not in p.name
        )

    def test_a_missing_database_is_never_created_by_set(self, run, db):
        result = run("set", "api_port", "1", "-c", db)
        assert result.exit_code == 78
        assert not db.exists() and not db.parent.exists()


def racing_write(monkeypatch, database, *, port=9999):
    """After `set` / `unset` has read its snapshot and computed its change,
    another writer commits: the compare-and-swap must lose."""
    done = []

    def interleave():
        resolved = resolve_config_path(str(database))
        snapshot = read_config(resolved)
        snapshot.config.api_port = port
        write_config(resolved, snapshot.config, expected_generation=snapshot.generation)
        dispose_all()
        done.append(True)

    real_set, real_unset = dotpath.set_value, dotpath.unset_value

    def set_value(config, segs, text):
        change = real_set(config, segs, text)
        interleave()
        return change

    def unset_value(config, segs):
        change = real_unset(config, segs)
        interleave()
        return change

    monkeypatch.setattr(dotpath, "set_value", set_value)
    monkeypatch.setattr(dotpath, "unset_value", unset_value)
    return done


class TestDotpathConflict:
    @pytest.mark.parametrize(
        "args",
        [
            ("set", "check_interval", "10m"),
            ("set", "datasets[tank/data].retention.2d", "20d"),
            ("unset", "datasets[tank/data].retention.1w"),
            ("set", "remote_backup.target_dataset", "tank/recv"),
        ],
    )
    def test_a_racing_write_is_exit_75_and_the_edit_is_not_applied(self, run, rich, monkeypatch, args):
        done = racing_write(monkeypatch, rich)
        result = run(*args, "-c", rich)
        assert done == [True]
        assert result.exit_code == 75, result.stderr
        assert "config changed underneath you" in result.stderr
        assert "Nothing was written" in result.stderr
        assert "config updated" not in result.stdout
        config = stored(rich)
        assert config.api_port == 9999         # the racer's write survived
        assert config.check_interval.literal == "5m"
        assert [r.age.literal for r in config.datasets[0].retention_rules] == ["1d", "1w"]
        assert config.remote_backup is None

    def test_the_conflict_does_not_retry(self, run, rich, monkeypatch):
        done = racing_write(monkeypatch, rich)
        run("set", "check_interval", "10m", "-c", rich)
        assert done == [True]  # one attempt, one racing write

    def test_a_held_writer_lock_is_75_too(self, run, rich, short_writer_lock):
        from test_zfsbackup_cli import _hold_writer_lock, _resolved

        release = _hold_writer_lock(_resolved(rich))
        try:
            before = db_state(rich)
            result = run("set", "check_interval", "10m", "-c", rich)
            assert result.exit_code == 75, result.stderr
            assert db_state(rich) == before
        finally:
            release()


from test_zfsbackup_cli import short_writer_lock  # noqa: E402,F401  (fixture re-export)


class TestDotpathBaseline:
    """`set` / `unset` re-validate the whole configuration; warnings that were
    already true of the stored one are not repeated, only the new ones."""

    @pytest.fixture
    def warny(self, run, db):
        make_db(run, db, WARNY_YAML)
        return db

    def test_the_fixture_has_all_three_standing_warnings(self, run, db):
        # Import is the one command that shows them (and each exactly once).
        make_db(run, db, WARNY_YAML)
        result = run("export", "-c", db)
        assert "never be pruned" in result.stderr  # the stored config is warny

    def test_an_unrelated_edit_prints_none_of_them(self, run, warny):
        result = ok(run, warny, "set", "check_interval", "10m")
        assert warnings_in(result) == []
        assert result.stderr == ""

    @pytest.mark.parametrize(
        "args",
        [
            ("set", "datasets[tank/ok].frequency", "30m"),
            ("set", "dry_run", "true"),
            ("unset", "prune_interval"),
            ("set", "api_host", "0.0.0.0"),
            ("set", "datasets[tank/forever].frequency", "2h"),  # edits the warned-about dataset itself
        ],
    )
    def test_standing_warnings_are_not_repeated(self, run, warny, args):
        result = ok(run, warny, *args)
        assert warnings_in(result) == [], result.stderr

    def test_only_the_new_warning_prints(self, run, warny):
        result = ok(run, warny, "set", "datasets[tank/ok].retention", "forever")
        lines = warnings_in(result)
        assert len(lines) == 1
        assert "tank/ok].retention is empty" in lines[0]
        # the standing ones stayed silent
        assert "tank/forever" not in result.stderr
        assert "snapshot_prefix" not in result.stderr and "api_port" not in result.stderr

    def test_a_new_unset_warning_prints_alone(self, run, warny):
        result = ok(run, warny, "unset", "datasets[tank/ok].retention.1d")
        lines = warnings_in(result)
        assert len(lines) == 1 and "tank/ok].retention is empty" in lines[0]

    def test_a_changed_warning_text_is_new(self, run, warny):
        result = ok(run, warny, "set", "snapshot_prefix", "other_x")
        lines = warnings_in(result)
        assert len(lines) == 1 and "'other_x' contains '_'" in lines[0]

    def test_fixing_a_standing_condition_is_silent(self, run, warny):
        assert warnings_in(ok(run, warny, "set", "snapshot_prefix", "clean")) == []
        assert warnings_in(ok(run, warny, "set", "api_port", "8080")) == []

    def test_ending_a_standing_never_prune_warns_that_pruning_starts_not_that_it_stops(self, run, warny):
        # Giving the never-pruned dataset rules removes the standing warning
        # (silent) but has a consequence of its own (L10): the pruner starts
        # destroying old snapshots. That is new information, not a repeat.
        result = ok(run, warny, "set", "datasets[tank/forever].retention", "{1d: 30d}")
        lines = warnings_in(result)
        assert len(lines) == 1
        assert "datasets[tank/forever] was never pruned; it now has retention rules" in lines[0]
        assert "never be pruned" not in lines[0]

    def test_a_standing_warning_that_comes_back_is_new_after_it_was_fixed(self, run, warny):
        ok(run, warny, "set", "api_port", "8080")
        result = ok(run, warny, "set", "api_port", "0")
        assert [w for w in warnings_in(result) if "api_port is 0" in w] != []

    def test_the_baseline_belongs_to_one_invocation(self, run, warny):
        # The suppression is not sticky state: a later import prints them again.
        ok(run, warny, "set", "check_interval", "10m")
        source = write_yaml(warny.parent.parent, WARNY_YAML, "again.yaml")
        result = run("import", source, "-c", warny, "--no-backup")
        assert result.exit_code == 0
        assert count_containing(warnings_in(result), "never be pruned") == 1

    def test_the_no_auth_warning_is_not_repeated_by_an_unrelated_edit(self, run, rich):
        ok(run, rich, "set", "remote_backup.target_dataset", "tank/recv")
        result = ok(run, rich, "set", "check_interval", "10m")
        assert warnings_in(result) == []

    def test_baseline_warnings_are_hidden_for_a_noop_too(self, run, warny):
        result = ok(run, warny, "set", "check_interval", "5m")
        assert result.stderr == ""

    def test_a_refused_edit_still_reports_its_error_only(self, run, warny):
        result = run("set", "api_port", "99999", "-c", warny)
        assert result.exit_code == 1
        assert warnings_in(result) == []


class TestDotpathTempFiles:
    """`set` hands the edited YAML to `BackupConfig.from_file` through a private
    temporary file (0700 directory, 0600 file) that is gone afterwards."""

    @pytest.fixture
    def spy(self, monkeypatch):
        seen = []
        original = BackupConfig.__dict__["from_file"].__func__

        def from_file(cls, path, *args, **kwargs):
            path = Path(path)
            seen.append((
                path,
                stat.S_IMODE(path.parent.stat().st_mode),
                stat.S_IMODE(path.stat().st_mode),
            ))
            return original(cls, path, *args, **kwargs)

        monkeypatch.setattr(BackupConfig, "from_file", classmethod(from_file))
        return seen

    def test_modes_and_removal_on_success(self, run, rich, spy):
        ok(run, rich, "set", "check_interval", "10m")
        assert spy, "set must parse through from_file"
        for path, dir_mode, file_mode in spy:
            assert dir_mode == 0o700 and file_mode == 0o600
            assert not path.exists() and not path.parent.exists()

    def test_removed_when_the_value_is_refused(self, run, rich, spy):
        assert run("set", "check_interval", "bogus", "-c", rich).exit_code == 1
        assert spy
        for path, dir_mode, file_mode in spy:
            assert dir_mode == 0o700 and file_mode == 0o600
            assert not path.parent.exists()

    def test_removed_for_a_noop(self, run, rich, spy):
        ok(run, rich, "set", "check_interval", "5m")
        assert spy and all(not p.parent.exists() for p, _, _ in spy)

    def test_the_file_is_not_world_readable_even_under_a_permissive_umask(self, run, rich, spy):
        old = os.umask(0)
        try:
            ok(run, rich, "set", "check_interval", "10m")
        finally:
            os.umask(old)
        assert all(d == 0o700 and f == 0o600 for _, d, f in spy)

    def test_non_ascii_names_round_trip(self, run, db):
        make_db(run, db, 'datasets:\n  - name: "tank/caf\u00e9"\n    frequency: "1h"\n')
        result = ok(run, db, "set", "datasets[tank/caf\u00e9].frequency", "30m")
        assert result.stdout.splitlines()[0] == "datasets[tank/caf\u00e9].frequency: 1h -> 30m"
        assert stored(db).datasets[0].name == "tank/caf\u00e9"


# ---------------------------------------------------------------------------
# Warnings: one source
# ---------------------------------------------------------------------------


def warning_texts(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.fixture
def warny_file(tmp_path):
    return write_yaml(tmp_path, WARNY_YAML, "warny.yaml")


class TestSingleSourceOfWarnings:
    def test_from_file_is_silent_about_the_three_semantic_conditions(self, warny_file, caplog):
        with caplog.at_level(logging.DEBUG):
            BackupConfig.from_file(warny_file)
        assert warning_texts(caplog) == []

    def test_validate_config_is_the_source_with_these_exact_texts(self, warny_file):
        config = BackupConfig.from_file(warny_file)
        texts = validate_config(config)
        assert texts == [
            PORT_TEXT,
            PREFIX_TEXT,
            f"datasets[tank/forever].{NEVER_PRUNE_TEXT}",
        ] or sorted(texts) == sorted([
            PORT_TEXT, PREFIX_TEXT, f"datasets[tank/forever].{NEVER_PRUNE_TEXT}",
        ])

    def test_import_prints_each_exactly_once(self, run, db, tmp_path):
        source = write_yaml(tmp_path, WARNY_YAML, "warny.yaml")
        result = run("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr
        lines = stderr_lines(result)
        assert count_containing(lines, f"datasets[tank/forever].{NEVER_PRUNE_TEXT}") == 1
        assert count_containing(lines, PREFIX_TEXT) == 1
        assert count_containing(lines, PORT_TEXT) == 1
        # ...and the old parse-time wordings are gone
        assert count_containing(lines, "retention is 'forever'") == 0
        assert count_containing(lines, "has no dataset-level retention rules") == 0

    @pytest.mark.parametrize(
        "yaml_text, fragment",
        [
            ('datasets:\n  - name: "t/a"\n    retention: forever\n', "datasets[t/a].retention is empty"),
            ('snapshot_prefix: "a_b"\ndatasets:\n  - name: "t/a"\n', "snapshot_prefix 'a_b' contains '_'"),
            ('api_port: 0\ndatasets:\n  - name: "t/a"\n', "api_port is 0"),
        ],
    )
    def test_each_condition_alone_warns_once_on_import(self, run, db, tmp_path, yaml_text, fragment):
        result = run("import", write_yaml(tmp_path, yaml_text), "-c", db)
        assert result.exit_code == 0
        assert count_containing(stderr_lines(result), fragment) == 1

    def test_a_plain_import_is_silent_about_all_three(self, run, db, tmp_path):
        result = run("import", write_yaml(tmp_path, PLAIN_YAML), "-c", db)
        assert warnings_in(result) == []

    def test_reimport_over_a_never_prune_config_logs_no_outgoing_warning(self, run, db, tmp_path):
        forever = write_yaml(tmp_path, WARNY_YAML, "forever.yaml")
        assert run("import", forever, "-c", db).exit_code == 0
        plain = write_yaml(tmp_path, PLAIN_YAML, "plain.yaml")
        result = run("import", plain, "-c", db)
        assert result.exit_code == 0, result.stderr
        # The replace warning is the CLI's own; nothing about the OUTGOING
        # configuration's never-prune dataset.
        assert "never be pruned" not in result.stderr
        assert "no dataset-level retention rules" not in result.stderr
        assert any(line.startswith("warning: replaced the stored configuration") for line in stderr_lines(result))

    def test_reimporting_a_never_prune_config_warns_once_for_the_new_one(self, run, db, tmp_path):
        forever = write_yaml(tmp_path, WARNY_YAML, "forever.yaml")
        run("import", forever, "-c", db)
        result = run("import", forever, "-c", db)
        assert count_containing(stderr_lines(result), "never be pruned") == 1

    def test_the_backup_of_a_never_prune_config_is_still_written(self, run, db, tmp_path):
        run("import", write_yaml(tmp_path, WARNY_YAML, "forever.yaml"), "-c", db)
        run("import", write_yaml(tmp_path, PLAIN_YAML, "plain.yaml"), "-c", db)
        backups = sorted(db.parent.glob("config.db.pre-import-*.yaml"))
        assert len(backups) == 1
        assert "retention: forever" in backups[0].read_text()

    def test_load_config_warns_by_default_and_not_when_asked(self, run, db, tmp_path, caplog):
        run("import", write_yaml(tmp_path, WARNY_YAML, "forever.yaml"), "-c", db)
        resolved = resolve_config_path(str(db))
        caplog.clear()  # the import above logged its own (validate_config) warnings
        with open_config_session(resolved, readonly=True) as session:
            with caplog.at_level(logging.WARNING):
                load_config(session)
            default = [m for m in warning_texts(caplog) if "never be pruned" in m]
            assert len(default) == 1 and "tank/forever" in default[0]

            caplog.clear()
            with caplog.at_level(logging.WARNING):
                config = load_config(session, warn_never_prune=False)
            assert warning_texts(caplog) == []
            # the flag silences the log line only: the value is the same
            assert [ds.retention_rules for ds in config.datasets][0] == []

    def test_the_default_of_warn_never_prune_is_true(self):
        import inspect

        assert inspect.signature(load_config).parameters["warn_never_prune"].default is True

    def test_import_yaml_backup_load_passes_false(self, tmp_path, caplog):
        # The importer's outgoing-config read is `load_config(..., warn_never_prune=False)`.
        resolved = resolve_config_path(str(tmp_path / "store" / "config.db"))
        forever = write_yaml(tmp_path, WARNY_YAML, "forever.yaml")
        import_yaml(forever, resolved)
        caplog.clear()
        plain = write_yaml(tmp_path, PLAIN_YAML, "plain.yaml")
        with caplog.at_level(logging.WARNING):
            import_yaml(plain, resolved, backup=lambda config: tmp_path / "backup.yaml")
        assert [m for m in warning_texts(caplog) if "no dataset-level retention rules" in m] == []

    def test_set_and_unset_warn_through_validate_config_not_from_file(self, run, db, tmp_path):
        make_db(run, db, PLAIN_YAML)
        result = ok(run, db, "set", "api_port", "0")
        assert warnings_in(result) == [f"warning: {PORT_TEXT}"]
        result = ok(run, db, "set", "snapshot_prefix", "a_b")
        assert len(warnings_in(result)) == 1 and "'a_b' contains '_'" in warnings_in(result)[0]


class TestDotpathRefusalsNameTheSetting:
    """A value the model refuses is reported as `<path>: <reason>` (L3), the
    path in the bracket form this tool prints."""

    @pytest.mark.parametrize(
        "args, printed",
        [
            (("set", "check_interval", "bogus"), "check_interval"),
            (("set", "datasets.tank/data.frequency", "bogus"), "datasets[tank/data].frequency"),
            (("set", "datasets.tank/vm.01.frequency", "bogus"), "datasets[tank/vm.01].frequency"),
            (("set", "datasets[tank/data].remote[dc.two].frequency", "bogus"),
             "datasets[tank/data].remote[dc.two].frequency"),
            (("set", "datasets[tank/data].remote[dc.two].retention", "{}"),
             "datasets[tank/data].remote[dc.two].retention"),
            (("set", "api_port", "70000"), "api_port"),
            (("set", "dry_run", "maybe"), "dry_run"),
            (("set", "datasets.tank/data.retention.1h", "bogus"), "datasets[tank/data].retention.1h"),
        ],
    )
    def test_the_error_starts_with_the_printed_path(self, run, rich, args, printed):
        before = db_state(rich)
        result = run(*args, "-c", rich)
        assert result.exit_code == 1, result.stdout
        assert result.stderr.startswith(f"error: {printed}: "), result.stderr
        assert "Traceback" not in result.stderr
        assert db_state(rich) == before

    def test_the_message_is_not_doubled(self, run, rich):
        err = run("set", "check_interval", "bogus", "-c", rich).stderr
        assert err.count("check_interval") == 1
        assert err.count("Invalid duration format") == 1

    def test_unset_refusals_name_the_setting_too(self, run, rich):
        err = run("unset", "check_interval", "-c", rich).stderr
        assert err.startswith("error: check_interval ")


class TestDotpathPruningStarts:
    """Giving a never-pruned dataset retention rules starts the pruner on its
    existing snapshots (L10): said once, only for that transition."""

    WARNING = "was never pruned; it now has retention rules, so the pruning worker will start destroying"

    @pytest.mark.parametrize(
        "args",
        [
            ("set", "datasets[tank/vm.01].retention", "{1d: 30d}"),
            ("set", "datasets[tank/vm.01].retention.1h", "1d"),
            ("set", "datasets[tank/vm.01].retention", "null"),
            ("unset", "datasets[tank/vm.01].retention"),
        ],
    )
    def test_warns_when_a_never_pruned_dataset_gets_rules(self, run, rich, args):
        result = ok(run, rich, *args)
        lines = [w for w in warnings_in(result) if self.WARNING in w]
        assert len(lines) == 1
        assert lines[0].startswith("warning: datasets[tank/vm.01] ")
        assert "snapshots that are older than they allow" in lines[0]
        assert len(warnings_in(result)) == 1  # and nothing else

    def test_the_warning_is_printed_before_the_write_is_reported(self, run, rich):
        result = ok(run, rich, "set", "datasets[tank/vm.01].retention", "{1d: 30d}")
        assert self.WARNING in result.stderr
        assert result.stdout.splitlines()[0].startswith("datasets[tank/vm.01].retention: forever -> ")

    @pytest.mark.parametrize(
        "args",
        [
            ("set", "datasets[tank/data].retention.2d", "20d"),     # already has rules
            ("set", "datasets[tank/data].retention", "{1d: 10d}"),
            ("unset", "datasets[tank/data].retention.1w"),           # still has a rule left
            ("set", "datasets[tank/vm.01].frequency", "20m"),        # not a retention edit
            ("set", "datasets[tank/vm.01].enabled", "true"),
        ],
    )
    def test_no_warning_otherwise(self, run, rich, args):
        assert [w for w in warnings_in(ok(run, rich, *args)) if self.WARNING in w] == []

    def test_staying_never_pruned_does_not_say_pruning_starts(self, run, rich):
        result = ok(run, rich, "set", "datasets[tank/vm.01].retention", "forever")
        assert result.stdout == "datasets[tank/vm.01].retention is already forever; nothing changed\n"
        assert [w for w in warnings_in(result) if self.WARNING in w] == []


    def test_the_removal_direction_warns_about_never_prune_not_pruning_starts(self, run, db):
        make_db(run, db, PLAIN_YAML)
        result = ok(run, db, "unset", "datasets[tank/a].retention.1d")
        assert [w for w in warnings_in(result) if self.WARNING in w] == []
        assert count_containing(warnings_in(result), "never be pruned") == 1

    def test_the_edit_that_added_rules_is_stored(self, run, rich):
        ok(run, rich, "set", "datasets[tank/vm.01].retention", "{1d: 30d}")
        assert [(r.age.literal, r.keep_for.literal) for r in stored(rich).datasets[1].retention_rules] == [("1d", "30d")]

"""`zfsbackup-config list datasets|destinations` (plan Phase 2, item 13).

Both views are read-only functions of the stored configuration. Covered:

- ``TestListDatasets``       columns, stored order, `never` / rule count, the
                             `inherit (X)` remote frequency, durations as typed,
                             `--enabled-only`, `--json` shape and types.
- ``TestListDestinations``   name order, URL, referencing datasets,
                             `--enabled-only` (counts only enabled senders and
                             hides unused destinations), `--json`.
- ``TestListIsReadOnly``     no write, no generation bump, no sidecars left, a
                             missing database is not created.
- ``TestListQuiet``          `list` and `get` print no warnings about the stored
                             configuration (errors still show); `export` is
                             unchanged and still does.

Mutation counts (scratch copy, one revert at a time; failing tests across this
file, `test_zfsbackup_cli_dotpath.py` and `test_zfsbackup_cli.py`) are recorded
in `test_zfsbackup_cli_dotpath.py`'s docstring.
"""

import json
import logging

import pytest

from _cli_support import (
    NEVER_PRUNE_TEXT,
    PLAIN_YAML,
    RICH_YAML,
    WARNY_YAML,
    Cli,
    db,
    db_state,
    generation,
    isolated,
    make_db,
    run,
    stderr_lines,
)
from zfsbackup.cli import main as cli_main

# No module-level tier marker: conftest marks every test that declares none as
# `unit`, and the few that spawn a process are marked `integration` individually
# (a test carrying both would be double-counted; see test_marker_tiers.py).


def table(result):
    """The stdout table as a list of rows of cells (columns split on 2+ spaces)."""
    import re

    return [re.split(r"\s{2,}", line.strip()) for line in result.stdout.splitlines()]


@pytest.fixture
def rich(run, db):
    return make_db(run, db)


class TestListDatasets:
    def test_columns_and_stored_order(self, run, rich):
        result = run("list", "datasets", "-c", rich)
        assert result.exit_code == 0, result.stderr
        header, *rows = result.stdout.splitlines()
        assert header.split() == [
            "NAME", "ENABLED", "FREQUENCY", "RECURSIVE", "RETENTION", "REMOTE", "DESTINATIONS"
        ]
        # Stored (written) order, NOT sorted: `tank/...` sorts after `a`.
        assert [r.split()[0] for r in rows] == ["tank/data", "tank/vm.01", "a", "a.retention"]

    def test_rows(self, run, rich):
        rows = table(run("list", "datasets", "-c", rich))[1:]
        assert rows == [
            ["tank/data", "yes", "1h", "yes", "2", "offsite: inherit (1h), dc.two: 4h"],
            ["tank/vm.01", "no", "15m", "no", "never", "-"],
            ["a", "yes", "1h", "no", "1", "-"],
            ["a.retention", "yes", "1h", "no", "1", "-"],
        ]

    def test_no_trailing_whitespace(self, run, rich):
        lines = run("list", "datasets", "-c", rich).stdout.splitlines()
        assert lines and all(line == line.rstrip() for line in lines)

    def test_retention_is_never_only_for_zero_rules(self, run, db):
        make_db(run, db, RICH_YAML)
        cells = {r[0]: r[4] for r in table(run("list", "datasets", "-c", db))[1:]}
        assert cells["tank/vm.01"] == "never"
        assert cells["tank/data"] == "2"

    def test_inherit_shows_the_datasets_own_frequency(self, run, db, tmp_path):
        text = RICH_YAML.replace('- name: "tank/data"\n    frequency: "1h"', '- name: "tank/data"\n    frequency: "30m"')
        make_db(run, db, text)
        remote = table(run("list", "datasets", "-c", db))[1][5]
        assert remote == "offsite: inherit (30m), dc.two: 4h"

    def test_durations_are_shown_as_typed(self, run, db):
        text = (
            'datasets:\n  - name: "tank/x"\n    frequency: "60m"\n'
            '    retention:\n      "7d": "30d"\n'
        )
        make_db(run, db, text)
        out = run("list", "datasets", "-c", db, "--json").stdout
        entry = json.loads(out)[0]
        assert entry["frequency"] == "60m"  # not canonicalised to 1h
        assert entry["retention_rules"] == [{"age": "7d", "keep_for": "30d"}]

    def test_enabled_only(self, run, rich):
        result = run("list", "datasets", "--enabled-only", "-c", rich)
        names = [r[0] for r in table(result)[1:]]
        assert names == ["tank/data", "a", "a.retention"]
        assert "tank/vm.01" not in result.stdout

    def test_enabled_only_json(self, run, rich):
        data = json.loads(run("list", "datasets", "--json", "--enabled-only", "-c", rich).stdout)
        assert [d["name"] for d in data] == ["tank/data", "a", "a.retention"]

    def test_empty_enabled_set_says_so(self, run, db):
        make_db(run, db, 'datasets:\n  - name: "tank/x"\n    enabled: false\n')
        assert run("list", "datasets", "--enabled-only", "-c", db).stdout == "(no enabled datasets)\n"
        assert json.loads(run("list", "datasets", "--enabled-only", "--json", "-c", db).stdout) == []

    def test_json_shape_and_types(self, run, rich):
        result = run("list", "datasets", "--json", "-c", rich)
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)  # a bare array
        assert len(data) == 4
        for entry in data:
            assert set(entry) == {
                "name", "enabled", "frequency", "recursive", "never_prune",
                "retention_rules", "remotes",
            }
            assert isinstance(entry["name"], str)
            assert isinstance(entry["enabled"], bool)
            assert isinstance(entry["recursive"], bool)
            assert isinstance(entry["never_prune"], bool)
            assert isinstance(entry["frequency"], str)

    def test_json_values(self, run, rich):
        data = {d["name"]: d for d in json.loads(run("list", "datasets", "--json", "-c", rich).stdout)}
        main = data["tank/data"]
        assert main["enabled"] is True and main["recursive"] is True
        assert main["never_prune"] is False
        assert main["retention_rules"] == [
            {"age": "1d", "keep_for": "30d"},
            {"age": "1w", "keep_for": "3M"},
        ]
        assert main["remotes"] == [
            {
                "destination": "offsite", "frequency": None,
                "effective_frequency": "1h", "retention_override": None,
            },
            {
                "destination": "dc.two", "frequency": "4h",
                "effective_frequency": "4h",
                "retention_override": [{"age": "1d", "keep_for": "7d"}],
            },
        ]
        forever = data["tank/vm.01"]
        assert forever["enabled"] is False
        assert forever["never_prune"] is True
        assert forever["retention_rules"] == []
        assert forever["remotes"] == []

    def test_json_is_pure_stdout_json(self, run, rich):
        result = run("list", "datasets", "--json", "-c", rich)
        assert result.stdout.endswith("\n")
        json.loads(result.stdout)
        assert result.stderr == ""

    def test_json_retention_rules_ordered_by_age(self, run, db):
        text = (
            'datasets:\n  - name: "tank/x"\n    retention:\n'
            '      "1M": "1y"\n      "1h": "1d"\n      "1w": "3M"\n'
        )
        make_db(run, db, text)
        rules = json.loads(run("list", "datasets", "--json", "-c", db).stdout)[0]["retention_rules"]
        assert [r["age"] for r in rules] == ["1h", "1w", "1M"]

    def test_remote_order_is_the_configured_order(self, run, db):
        text = (
            'destinations:\n  b:\n    url: "http://b"\n  a:\n    url: "http://a"\n'
            'datasets:\n  - name: "tank/x"\n    remote:\n'
            '      - destination: "b"\n      - destination: "a"\n'
        )
        make_db(run, db, text)
        assert table(run("list", "datasets", "-c", db))[1][5] == "b: inherit (1h), a: inherit (1h)"

    def test_help_documents_the_options(self, run):
        out = run("list", "datasets", "--help").stdout
        assert "--json" in out and "--enabled-only" in out
        assert "-c, --config" in out

    def test_options_accepted_before_and_after_the_subcommand(self, run, rich):
        a = run("-c", rich, "list", "datasets", "--json").stdout
        b = run("list", "datasets", "--json", "-c", rich).stdout
        assert a == b


class TestListDestinations:
    def test_rows_sorted_by_name(self, run, rich):
        result = run("list", "destinations", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert result.stdout.splitlines()[0].split() == ["NAME", "URL", "DATASETS"]
        assert table(result)[1:] == [
            ["dc.two", "http://dc2:8080", "tank/data"],
            ["offsite", "http://offsite:8080", "tank/data"],
            ["spare", "http://spare:8080", "-"],
        ]

    def test_sorted_by_name_not_by_file_order(self, run, db):
        text = (
            'destinations:\n  zeta:\n    url: "http://z"\n  alpha:\n    url: "http://a"\n'
            'datasets:\n  - name: "tank/x"\n'
        )
        make_db(run, db, text)
        assert [r[0] for r in table(run("list", "destinations", "-c", db))[1:]] == ["alpha", "zeta"]

    def test_several_datasets_listed_in_stored_order(self, run, db):
        text = (
            'destinations:\n  d:\n    url: "http://d"\n'
            'datasets:\n'
            '  - name: "tank/z"\n    remote:\n      - destination: "d"\n'
            '  - name: "tank/a"\n    remote:\n      - destination: "d"\n'
        )
        make_db(run, db, text)
        assert table(run("list", "destinations", "-c", db))[1] == ["d", "http://d", "tank/z, tank/a"]

    def test_enabled_only_counts_only_enabled_senders_and_hides_unused(self, run, db):
        text = (
            'destinations:\n  both:\n    url: "http://both"\n'
            '  only_off:\n    url: "http://off"\n  unused:\n    url: "http://unused"\n'
            'datasets:\n'
            '  - name: "tank/on"\n    remote:\n      - destination: "both"\n'
            '  - name: "tank/off"\n    enabled: false\n    remote:\n'
            '      - destination: "both"\n      - destination: "only_off"\n'
        )
        make_db(run, db, text)
        everything = table(run("list", "destinations", "-c", db))[1:]
        assert [r[0] for r in everything] == ["both", "only_off", "unused"]
        assert everything[0][2] == "tank/on, tank/off"

        enabled = table(run("list", "destinations", "--enabled-only", "-c", db))[1:]
        # `both` now lists the enabled sender only; the destinations nothing
        # enabled sends to are hidden.
        assert enabled == [["both", "http://both", "tank/on"]]

    def test_enabled_only_json(self, run, rich):
        data = json.loads(run("list", "destinations", "--enabled-only", "--json", "-c", rich).stdout)
        assert [d["name"] for d in data] == ["dc.two", "offsite"]

    def test_json_shape(self, run, rich):
        result = run("list", "destinations", "--json", "-c", rich)
        data = json.loads(result.stdout)
        assert data == [
            {"name": "dc.two", "url": "http://dc2:8080", "datasets": ["tank/data"]},
            {"name": "offsite", "url": "http://offsite:8080", "datasets": ["tank/data"]},
            {"name": "spare", "url": "http://spare:8080", "datasets": []},
        ]
        assert result.stderr == ""

    def test_no_destinations(self, run, db):
        make_db(run, db, PLAIN_YAML)
        assert run("list", "destinations", "-c", db).stdout == "(no destinations)\n"
        assert json.loads(run("list", "destinations", "--json", "-c", db).stdout) == []

    def test_help_documents_the_options(self, run):
        out = run("list", "destinations", "--help").stdout
        assert "--json" in out and "--enabled-only" in out


class TestListIsReadOnly:
    @pytest.mark.parametrize(
        "args",
        [
            ("list", "datasets"),
            ("list", "datasets", "--json"),
            ("list", "destinations"),
            ("list", "destinations", "--enabled-only", "--json"),
            ("get", "snapshot_prefix"),
        ],
    )
    def test_nothing_is_written(self, run, rich, args):
        before, gen = db_state(rich), generation(rich)
        assert run(*args, "-c", rich).exit_code == 0
        assert db_state(rich) == before
        assert generation(rich) == gen

    def test_missing_database_is_reported_not_created(self, run, db):
        result = run("list", "datasets", "-c", db)
        assert result.exit_code == 78
        assert not db.exists() and not db.parent.exists()

    def test_list_requires_a_subcommand(self, run):
        result = run("list")
        assert "datasets" in result.stdout + result.stderr
        assert "destinations" in result.stdout + result.stderr

    def test_unknown_list_target_is_a_usage_error(self, run, rich):
        assert run("list", "snapshots", "-c", rich).exit_code == 2


class TestListQuiet:
    """`list` and `get` do not comment on the stored configuration: a
    never-prune dataset is the operator's choice, and the warning belongs to the
    command that wrote it (and to `export`, which is unchanged)."""

    @pytest.fixture
    def warny(self, run, db):
        make_db(run, db, WARNY_YAML)
        return db

    @pytest.mark.parametrize(
        "args",
        [
            ("list", "datasets"),
            ("list", "datasets", "--json"),
            ("list", "destinations"),
            ("get", "snapshot_prefix"),
            ("get", "datasets[tank/forever].retention"),
            ("get", "datasets[tank/forever]"),
            ("get", "api_port"),
        ],
    )
    def test_no_stored_config_warnings(self, run, warny, args):
        result = run(*args, "-c", warny)
        assert result.exit_code == 0, result.stderr
        assert result.stderr == ""

    def test_the_fixture_really_does_warn_on_a_plain_load(self, run, warny):
        # Guards the tests above against passing because nothing would have
        # been said anyway: `export` reads the same snapshot and still warns.
        result = run("export", "-c", warny)
        assert result.exit_code == 0
        assert "never be pruned" in result.stderr

    def test_export_is_unchanged(self, run, warny):
        lines = stderr_lines(run("export", "-c", warny))
        assert sum("never be pruned" in line for line in lines) == 1
        assert all(line.startswith("warning: ") for line in lines)

    def test_errors_are_still_shown(self, run, warny, monkeypatch):
        real = cli_main.read_snapshot

        def noisy(resolved):
            log = logging.getLogger("zfsbackup.config.store.mapper")
            log.warning("a warning about the stored config")
            log.error("an error about the stored config")
            return real(resolved)

        monkeypatch.setattr(cli_main, "read_snapshot", noisy)
        for args in (("list", "datasets"), ("get", "api_port")):
            result = run(*args, "-c", warny)
            assert "error: an error about the stored config" in result.stderr
            assert "a warning about the stored config" not in result.stderr
            assert result.exit_code == 0

    def test_verbose_does_not_resurrect_warnings_for_list(self, run, warny):
        # -v lowers the level to DEBUG; "quiet" is about WARNING records.
        result = run("list", "datasets", "-v", "-c", warny)
        assert "never be pruned" not in result.stderr

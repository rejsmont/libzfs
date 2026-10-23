"""Milestone B (decision B3), CLI half: the per-destination `raw` setting.

`RemoteDatasetConfig.raw` is `None` (auto: raw if and only if the dataset is
encrypted), `True` (raw required) or `False` (never raw). The CLI spells it
`true` / `false` / the absent key (`auto`).

- ``TestRender``        `render_yaml` writes `raw: true|false` as a bare boolean
                        and OMITS the key for None (never `raw: null`); round
                        trip, fixed point, literals, combined with a
                        frequency and a retention override.
- ``TestExportImport``  export -> import -> export through the database is stable.
- ``TestGet``           dot and bracket forms on a dotted dataset
                        (`tank/vm.01`) and a dotted destination (`dc.two`); the
                        sibling list names `frequency, raw, retention`.
- ``TestSet``           the value matrix (true/false/yes/no/auto/null accepted;
                        1, 0, 'true', maybe, [] refused with exit 1 and the
                        database file byte-identical), unattached destinations,
                        no-op sets, the committed value (NULL / 0 / 1) and that
                        neighbouring settings are left alone.
- ``TestUnset``         `unset` is auto, also when it already is.
- ``TestList``          text (` raw=true|false` only when set) and `--json`
                        (`"raw"` always present); `list destinations` is unchanged.
- ``TestEdit``          whole-config and single-dataset buffers: adding and
                        removing `raw: true`, an unchanged `raw` is "no changes",
                        `raw: 1` / `raw: "true"` take the Retry/Abort path naming
                        the key, a raw-only difference is not "rename-shaped",
                        and a typo such as `rwa:` is still an unknown key.
- ``TestMigrateCheck``  `migrate --check` reports `5d2f8a60c1e4`; `migrate`
                        upgrades a TLS-revision database and keeps its rows.

Every database is created by `import` and every change goes through the real
CLI, store and writer lock, so `foreign_keys=ON` and the retention FK apply.

Mutation checks (scratch copy, one revert at a time; failing tests across this
file, `test_zfsbackup_store_raw.py`, `test_zfsbackup_config.py` and the existing
CLI / store files): the mapper dropping `raw` 93 / 91; `render` emitting
`raw: null` for auto 76 (25 here) / dropping a False 26 / (quoting is covered
by `test_a_set_value_is_a_bare_boolean_scalar`); dotpath `set` accepting an
integer 6; dotpath `unset` of `raw` doing nothing 6; `editor._REMOTE_KEYS`
without `raw` 21; `listing` text always printing ` raw=` 5 / JSON omitting the
key for auto 17; the model accepting integers 19 / strings 29 for `raw`;
`raw` leaking into `to_property` 4; the migration's downgrade a no-op 5.

Raw-change warning round: the old/new comparison dropped (every existing entry
warns) 33 here + 26 in `test_zfsbackup_cli_dotpath.py`; new entries warned on
8; a truthiness comparison (false == auto) 7; the call missing from `set` 9 /
from `unset` 2 / from the editor's chain 6.
"""

import json
import re

import pytest
import yaml

import _cli_support
from _cli_support import (
    RICH_YAML,
    buffers_in,
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
    write_yaml,
)
from _raw_support import (
    RAW_REVISION,
    drop_server_fingerprint,
    drop_stamp,
    make_headstamped_without_raw,
    stamp,
    TLS_REVISION,
    TLS_ROWS,
    make_tls_head_db,
    q,
    raw_cells,
    table_rows,
)
from zfsbackup.cli import dotpath, editor
from zfsbackup.cli.render import render_yaml
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
    RetentionRule,
)
from zfsbackup.config.store import dispose_all, upgrade_schema

# No module-level tier marker: conftest marks every test that declares none as `unit`.

RAW_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/vm.01"
    frequency: "1h"
    remote:
      - destination: "offsite"
        raw: true
      - destination: "dc.two"
        raw: false
        frequency: "4h"
        retention:
          "1d": "7d"
      - destination: "spare"
  - name: "tank/data"
destinations:
  offsite:
    url: "https://offsite:8080"
  dc.two:
    url: "https://dc2:8080"
  spare:
    url: "https://spare:8080"
  unused:
    url: "https://unused:8080"
"""

DS = "tank/vm.01"
OFFSITE = "datasets[tank/vm.01].remote[offsite].raw"       # true in RAW_YAML
DC_TWO = "datasets[tank/vm.01].remote[dc.two].raw"         # false
SPARE = "datasets[tank/vm.01].remote[spare].raw"           # auto
D = Duration


@pytest.fixture
def rdb(run, db):
    """A database holding `RAW_YAML`: raw true / false / auto on three remotes."""
    return make_db(run, db, RAW_YAML)


def ok(run, database, *args, **kw):
    result = run(*args, "-c", database, **kw)
    assert result.exit_code == 0, f"{args}: {result.stderr}"
    return result


def raw_of(database, dataset=DS):
    """`{destination: raw}` of `dataset`, as the store hands it back."""
    ds = next(d for d in stored(database).datasets if d.name == dataset)
    return {r.destination: r.raw for r in ds.remote}


# ===========================================================================
# render
# ===========================================================================


def config_with(raw, *, extras=False) -> BackupConfig:
    remote = RemoteDatasetConfig(destination="dc.two", raw=raw)
    if extras:
        remote = RemoteDatasetConfig(
            destination="dc.two", raw=raw, frequency=D("90m"),
            retention_rules=[RetentionRule(D("1d"), D("7d")), RetentionRule(D("2w"), D("30d"))],
        )
    return BackupConfig(
        snapshot_prefix="autosnap",
        datasets=[DatasetConfig(
            name="tank/vm.01", frequency=D("1h"),
            retention_rules=[RetentionRule(D("1d"), D("30d"))], remote=[remote],
        )],
        destinations={"dc.two": Destination("https://dc2:8080")},
    )


def reload(text, tmp_path) -> BackupConfig:
    return BackupConfig.from_file(write_yaml(tmp_path, text, "rendered.yaml"))


def remote_entries(text):
    return yaml.safe_load(text)["datasets"][0]["remote"]


class TestRender:
    @pytest.mark.parametrize("extras", [False, True], ids=["bare", "frequency+retention"])
    @pytest.mark.parametrize("raw", [True, False, None], ids=["true", "false", "auto"])
    def test_round_trip_and_fixed_point(self, tmp_path, raw, extras):
        config = config_with(raw, extras=extras)
        text = render_yaml(config)
        back = reload(text, tmp_path)
        assert back == config
        assert back.datasets[0].remote[0].raw is raw
        assert render_yaml(back) == text
        if extras:
            remote = back.datasets[0].remote[0]
            assert remote.frequency.literal == "90m"
            assert [(r.age.literal, r.keep_for.literal) for r in remote.retention_rules] == [
                ("1d", "7d"), ("2w", "30d")
            ]

    @pytest.mark.parametrize("raw, word", [(True, "true"), (False, "false")])
    def test_a_set_value_is_a_bare_boolean_scalar(self, raw, word):
        text = render_yaml(config_with(raw))
        assert re.search(rf"^\s+raw: {word}$", text, re.MULTILINE)
        assert f'raw: "{word}"' not in text and f"raw: '{word}'" not in text
        entry, = remote_entries(text)
        assert entry["raw"] is raw                        # a real YAML bool, not a string

    def test_false_is_written_not_dropped(self):
        entry, = remote_entries(render_yaml(config_with(False)))
        assert "raw" in entry and entry["raw"] is False

    @pytest.mark.parametrize("extras", [False, True], ids=["bare", "frequency+retention"])
    def test_auto_omits_the_key_entirely(self, extras):
        text = render_yaml(config_with(None, extras=extras))
        assert not re.search(r"\braw\b", text)
        assert not re.search(r"\bnull\b|: auto\b", text)        # neither `raw: null` nor `raw: auto`
        assert "raw" not in remote_entries(text)[0]

    def test_each_remote_has_its_own_state(self, tmp_path):
        config = config_with(True)
        config.datasets[0].remote = [
            RemoteDatasetConfig(destination=n, raw=r)
            for n, r in (("a", True), ("b", None), ("c", False))
        ]
        config.destinations = {n: Destination(f"https://{n}.example:8443") for n in "abc"}
        entries = remote_entries(render_yaml(config))
        assert entries == [
            {"destination": "a", "raw": True},
            {"destination": "b"},
            {"destination": "c", "raw": False},
        ]
        assert reload(render_yaml(config), tmp_path) == config


# ===========================================================================
# export / import through the database
# ===========================================================================


class TestExportImport:
    def test_export_import_export_is_stable(self, run, rdb, tmp_path):
        first = ok(run, rdb, "export").stdout
        assert "raw: true" in first and "raw: false" in first
        copy = tmp_path / "copy" / "config.db"
        source = write_yaml(tmp_path, first, "exported.yaml")
        assert run("import", source, "-c", copy).exit_code == 0
        second = ok(run, copy, "export").stdout
        assert second == first

    def test_the_values_survive_the_database(self, rdb):
        assert raw_of(rdb) == {"offsite": True, "dc.two": False, "spare": None}
        assert raw_cells(rdb) == [
            ("offsite", 1, "integer"), ("dc.two", 0, "integer"), ("spare", None, "null")
        ]

    def test_export_writes_each_spelling_as_the_import_read_it(self, run, rdb):
        entries = yaml.safe_load(ok(run, rdb, "export").stdout)["datasets"][0]["remote"]
        assert [(e["destination"], e.get("raw", "absent")) for e in entries] == [
            ("offsite", True), ("dc.two", False), ("spare", "absent")
        ]

    def test_a_second_import_of_the_export_changes_nothing(self, run, rdb, tmp_path):
        exported = ok(run, rdb, "export").stdout
        source = write_yaml(tmp_path, exported, "again.yaml")
        assert run("import", source, "-c", rdb).exit_code == 0
        assert raw_of(rdb) == {"offsite": True, "dc.two": False, "spare": None}
        assert ok(run, rdb, "export").stdout == exported


# ===========================================================================
# get
# ===========================================================================


class TestGet:
    @pytest.mark.parametrize("path, expected", [
        ("datasets[tank/vm.01].remote[offsite].raw", "true"),
        ("datasets[tank/vm.01].remote[dc.two].raw", "false"),
        ("datasets[tank/vm.01].remote[spare].raw", "auto"),
        ("datasets.tank/vm.01.remote.offsite.raw", "true"),
        ("datasets.tank/vm.01.remote.dc.two.raw", "false"),
        ("datasets.tank/vm.01.remote.spare.raw", "auto"),
        ("datasets[tank/vm.01].remote.dc.two.raw", "false"),
        ("datasets.tank/vm.01.remote[dc.two].raw", "false"),
    ], ids=lambda v: v if v in ("true", "false", "auto") else None)
    def test_get(self, run, rdb, path, expected):
        result = ok(run, rdb, "get", path)
        assert result.stdout == expected + "\n"
        assert result.stderr == ""

    def test_get_never_changes_the_database(self, run, rdb):
        state = db_state(rdb)
        for path in (OFFSITE, DC_TWO, SPARE):
            ok(run, rdb, "get", path)
        assert db_state(rdb) == state

    def test_the_remote_subtree_prints_raw(self, run, rdb):
        assert yaml.safe_load(ok(run, rdb, "get", "datasets[tank/vm.01].remote[offsite]").stdout) == {
            "destination": "offsite", "raw": True,
        }
        assert yaml.safe_load(ok(run, rdb, "get", "datasets[tank/vm.01].remote[spare]").stdout) == {
            "destination": "spare",
        }

    def test_the_dataset_subtree_prints_raw(self, run, rdb):
        remote = yaml.safe_load(ok(run, rdb, "get", "datasets[tank/vm.01]").stdout)["remote"]
        assert remote[0] == {"destination": "offsite", "raw": True}
        assert remote[1]["raw"] is False and remote[1]["frequency"] == "4h"
        assert "raw" not in remote[2]

    def test_the_unknown_sibling_message_lists_the_remote_settings(self, run, rdb):
        state = db_state(rdb)
        result = run("get", "datasets[tank/vm.01].remote[offsite].bogus", "-c", rdb)
        assert result.exit_code == 1
        assert "remote[offsite] has no setting 'bogus'; valid: frequency, raw, retention" in result.stderr
        assert db_state(rdb) == state

    def test_raw_has_no_sub_settings(self, run, rdb):
        result = run("get", OFFSITE + ".x", "-c", rdb)
        assert result.exit_code == 1
        assert "raw has no sub-settings" in result.stderr

    def test_a_destination_that_is_not_attached_is_unknown(self, run, rdb):
        result = run("get", "datasets[tank/data].remote[offsite].raw", "-c", rdb)
        assert result.exit_code == 1
        assert "no attached destination named 'offsite'" in result.stderr
        result = run("get", "datasets[tank/vm.01].remote[unused].raw", "-c", rdb)
        assert result.exit_code == 1
        assert "no attached destination named 'unused'" in result.stderr
        assert "offsite, dc.two, spare" in result.stderr

    def test_the_resolved_path_is_the_bracket_form(self):
        assert dotpath.format_path(("datasets", DS, "remote", "dc.two", "raw")) == \
            "datasets[tank/vm.01].remote[dc.two].raw"


# ===========================================================================
# set
# ===========================================================================


class TestSet:
    @pytest.mark.parametrize("text, expected", [
        ("true", True), ("false", False),
        ("yes", True), ("no", False), ("on", True), ("off", False),   # YAML booleans
        ("True", True), ("False", False),
        ("auto", None), ("null", None), ("~", None),
    ])
    def test_accepted_values(self, run, rdb, text, expected):
        # Start from the OTHER state so that every case is a real change.
        start = "false" if expected is True else "true"
        ok(run, rdb, "set", SPARE, start)
        gen = generation(rdb)
        result = ok(run, rdb, "set", SPARE, text)
        assert raw_of(rdb)["spare"] is expected
        assert generation(rdb) == gen + 1
        assert f"{SPARE}: {start} -> {'auto' if expected is None else str(expected).lower()}" \
            in result.stdout

    @pytest.mark.parametrize("text, cell", [
        ("true", (1, "integer")), ("false", (0, "integer")), ("auto", (None, "null")),
    ])
    def test_the_committed_value_lands_in_the_database_as_null_or_a_bool(self, run, rdb, text, cell):
        ok(run, rdb, "set", OFFSITE, text)
        assert raw_cells(rdb)[0] == ("offsite",) + cell
        # The neighbours were not touched.
        assert raw_cells(rdb)[1:] == [("dc.two", 0, "integer"), ("spare", None, "null")]

    @pytest.mark.parametrize("text", [
        "1", "0", "2", "1.0", "'true'", '"true"', "'false'", "maybe", "TRUEISH",
        "[]", "[true]", "{}", "{a: 1}",
    ])
    def test_refused_values_leave_the_database_byte_identical(self, run, rdb, text):
        state, gen = db_state(rdb), generation(rdb)
        result = run("set", OFFSITE, text, "-c", rdb)
        assert result.exit_code == 1, result.stdout
        assert "datasets[tank/vm.01].remote[offsite].raw must be true, false or auto" in result.stderr
        assert db_state(rdb) == state
        assert generation(rdb) == gen
        assert raw_of(rdb)["offsite"] is True

    @pytest.mark.parametrize("text, shown", [
        ("1", "int 1"), ("0", "int 0"), ("'true'", "str 'true'"), ("maybe", "str 'maybe'"),
    ])
    def test_the_refusal_says_what_was_given(self, run, rdb, text, shown):
        result = run("set", SPARE, text, "-c", rdb)
        assert result.exit_code == 1
        assert shown in result.stderr
        assert stderr_lines(result)[0].startswith("error: ")

    def test_a_refusal_is_not_a_conflict_or_a_usage_error(self, run, rdb):
        assert run("set", OFFSITE, "1", "-c", rdb).exit_code == 1       # not 2 (usage), 75, 78

    def test_setting_the_value_it_already_has_changes_nothing(self, run, rdb):
        state, gen = db_state(rdb), generation(rdb)
        result = ok(run, rdb, "set", OFFSITE, "true")
        assert "already true; nothing changed" in result.stdout
        assert db_state(rdb) == state and generation(rdb) == gen
        result = ok(run, rdb, "set", SPARE, "auto")
        assert "already auto; nothing changed" in result.stdout
        assert db_state(rdb) == state and generation(rdb) == gen

    def test_an_empty_value_means_auto(self, run, rdb):
        ok(run, rdb, "set", OFFSITE, "")
        assert raw_of(rdb)["offsite"] is None

    @pytest.mark.parametrize("path, fragment", [
        ("datasets[tank/data].remote[offsite].raw", "no attached destination named 'offsite'"),
        ("datasets[tank/vm.01].remote[unused].raw", "no attached destination named 'unused'"),
        ("datasets[tank/vm.01].remote[nosuch].raw", "no attached destination named 'nosuch'"),
    ])
    def test_an_unattached_destination_is_refused(self, run, rdb, path, fragment):
        state = db_state(rdb)
        result = run("set", path, "true", "-c", rdb)
        assert result.exit_code == 1
        assert fragment in result.stderr
        assert db_state(rdb) == state

    def test_setting_raw_leaves_the_frequency_and_the_override_alone(self, run, rdb):
        ok(run, rdb, "set", DC_TWO, "true")
        ds = next(d for d in stored(rdb).datasets if d.name == DS)
        remote = next(r for r in ds.remote if r.destination == "dc.two")
        assert remote.raw is True
        assert remote.frequency.literal == "4h"
        assert [(r.age.literal, r.keep_for.literal) for r in remote.retention_rules] == [("1d", "7d")]

    def test_setting_a_frequency_leaves_raw_alone(self, run, rdb):
        ok(run, rdb, "set", "datasets[tank/vm.01].remote[offsite].frequency", "2h")
        ok(run, rdb, "set", "datasets[tank/vm.01].remote[dc.two].retention.1d", "9d")
        assert raw_of(rdb) == {"offsite": True, "dc.two": False, "spare": None}

    def test_unsetting_a_frequency_leaves_raw_alone(self, run, rdb):
        ok(run, rdb, "unset", "datasets[tank/vm.01].remote[dc.two].frequency")
        assert raw_of(rdb) == {"offsite": True, "dc.two": False, "spare": None}

    def test_a_dotted_dataset_and_a_dotted_destination_in_the_dot_form(self, run, rdb):
        ok(run, rdb, "set", "datasets.tank/vm.01.remote.dc.two.raw", "true")
        assert raw_of(rdb)["dc.two"] is True

    def test_the_change_is_reported_with_a_daemon_notice(self, run, rdb):
        result = ok(run, rdb, "set", SPARE, "true")
        assert "config updated (generation" in result.stdout

    def test_set_on_the_second_dataset_is_independent(self, run, rdb):
        ok(run, rdb, "set", OFFSITE, "false")
        assert raw_of(rdb, "tank/data") == {}
        assert raw_of(rdb)["offsite"] is False


# ===========================================================================
# unset
# ===========================================================================


class TestUnset:
    @pytest.mark.parametrize("path", [OFFSITE, DC_TWO])
    def test_unset_means_auto(self, run, rdb, path):
        gen = generation(rdb)
        result = ok(run, rdb, "unset", path)
        assert result.stdout.startswith(f"{path}: ")
        assert "-> auto" in result.stdout
        assert generation(rdb) == gen + 1
        assert ok(run, rdb, "get", path).stdout == "auto\n"
        cell = raw_cells(rdb)[0 if path == OFFSITE else 1]
        assert cell[1:] == (None, "null")                 # NULL, not 0

    def test_unset_when_already_auto_changes_nothing(self, run, rdb):
        state, gen = db_state(rdb), generation(rdb)
        result = ok(run, rdb, "unset", SPARE)
        assert "already auto; nothing changed" in result.stdout
        assert db_state(rdb) == state and generation(rdb) == gen

    def test_unset_twice(self, run, rdb):
        ok(run, rdb, "unset", OFFSITE)
        gen = generation(rdb)
        assert "nothing changed" in ok(run, rdb, "unset", OFFSITE).stdout
        assert generation(rdb) == gen

    def test_unset_on_an_unattached_destination_is_refused(self, run, rdb):
        state = db_state(rdb)
        result = run("unset", "datasets[tank/vm.01].remote[unused].raw", "-c", rdb)
        assert result.exit_code == 1 and "no attached destination" in result.stderr
        assert db_state(rdb) == state

    def test_unset_is_the_same_as_set_auto(self, run, rdb, tmp_path):
        other = make_db(run, tmp_path / "other" / "config.db", RAW_YAML)
        ok(run, rdb, "unset", OFFSITE)
        ok(run, other, "set", OFFSITE, "auto")
        assert ok(run, rdb, "export").stdout == ok(run, other, "export").stdout

    def test_unset_leaves_the_rest_of_the_remote_alone(self, run, rdb):
        ok(run, rdb, "unset", DC_TWO)
        ds = next(d for d in stored(rdb).datasets if d.name == DS)
        remote = next(r for r in ds.remote if r.destination == "dc.two")
        assert remote.raw is None and remote.frequency.literal == "4h"
        assert len(remote.retention_rules) == 1


# ===========================================================================
# list
# ===========================================================================


def table_cells(result):
    return [re.split(r"\s{2,}", line.strip()) for line in result.stdout.splitlines()]


class TestList:
    def test_text_appends_raw_only_when_it_is_set(self, run, rdb):
        rows = table_cells(ok(run, rdb, "list", "datasets"))[1:]
        assert rows[0] == ["tank/vm.01", "yes", "1h", "no", "1",
                           "offsite: inherit (1h) raw=true, dc.two: 4h raw=false, spare: inherit (1h)"]
        assert rows[1] == ["tank/data", "yes", "1h", "no", "1", "-"]

    def test_auto_shows_nothing_not_raw_auto_or_raw_none(self, run, rdb):
        ok(run, rdb, "unset", OFFSITE)
        ok(run, rdb, "unset", DC_TWO)
        text = ok(run, rdb, "list", "datasets").stdout
        assert "raw" not in text
        assert "offsite: inherit (1h), dc.two: 4h, spare: inherit (1h)" in text

    def test_text_has_no_trailing_whitespace(self, run, rdb):
        lines = ok(run, rdb, "list", "datasets").stdout.splitlines()
        assert lines and all(line == line.rstrip() for line in lines)

    def test_json_always_has_the_raw_key(self, run, rdb):
        data = json.loads(ok(run, rdb, "list", "datasets", "--json").stdout)
        remotes = {r["destination"]: r for r in data[0]["remotes"]}
        assert remotes["offsite"]["raw"] is True
        assert remotes["dc.two"]["raw"] is False
        assert "raw" in remotes["spare"] and remotes["spare"]["raw"] is None
        for entry in data:
            for remote in entry["remotes"]:
                assert "raw" in remote
                assert remote["raw"] is None or isinstance(remote["raw"], bool)

    def test_json_keeps_every_older_key(self, run, rdb):
        remote = json.loads(ok(run, rdb, "list", "datasets", "--json").stdout)[0]["remotes"][1]
        assert remote == {
            "destination": "dc.two", "frequency": "4h", "effective_frequency": "4h",
            "retention_override": [{"age": "1d", "keep_for": "7d"}], "raw": False,
        }

    def test_json_follows_a_change(self, run, rdb):
        ok(run, rdb, "set", SPARE, "false")
        ok(run, rdb, "unset", OFFSITE)
        remotes = json.loads(ok(run, rdb, "list", "datasets", "--json").stdout)[0]["remotes"]
        assert [(r["destination"], r["raw"]) for r in remotes] == [
            ("offsite", None), ("dc.two", False), ("spare", False)
        ]

    def test_enabled_only_json_carries_it_too(self, run, rdb):
        data = json.loads(ok(run, rdb, "list", "datasets", "--json", "--enabled-only").stdout)
        assert data[0]["remotes"][0]["raw"] is True

    def test_list_destinations_is_unchanged_by_raw(self, run, rdb, tmp_path):
        with_raw = ok(run, rdb, "list", "destinations")
        with_raw_json = ok(run, rdb, "list", "destinations", "--json").stdout
        for path in (OFFSITE, DC_TWO):
            ok(run, rdb, "unset", path)
        assert ok(run, rdb, "list", "destinations").stdout == with_raw.stdout
        assert ok(run, rdb, "list", "destinations", "--json").stdout == with_raw_json
        assert "raw" not in with_raw.stdout and "raw" not in with_raw_json

    def test_list_does_not_write(self, run, rdb):
        state = db_state(rdb)
        ok(run, rdb, "list", "datasets")
        ok(run, rdb, "list", "datasets", "--json")
        assert db_state(rdb) == state


# ===========================================================================
# edit
# ===========================================================================


@pytest.fixture
def tty(monkeypatch):
    monkeypatch.setattr(editor, "_stdin_is_tty", lambda: True)


def edit(run, database, stub, *args, input=None):
    return run("edit", *args, "-c", database, env=stub.env(database), input=input)


def replace(old, new):
    return {"replace": [old, new]}


OFFSITE_LINE = '      - destination: "offsite"\n        raw: true\n'
NO_CHANGES = "No changes; the stored configuration was not touched.\n"


class TestEdit:
    # ---- whole-config buffer -------------------------------------------------

    def test_the_buffer_carries_raw(self, run, rdb, stub, scratch_tmp):
        stub.plan([])
        edit(run, rdb, stub, "--no-prompt")
        before = stub.log[0]["before"]
        assert '- destination: "offsite"\n        raw: true' in before
        assert "raw: false" in before

    def test_adding_raw_true(self, run, rdb, stub, scratch_tmp):
        gen = generation(rdb)
        stub.plan([replace('      - destination: "spare"', '      - destination: "spare"\n        raw: true')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert "Configuration saved (datasets changed: tank/vm.01)." in result.stdout
        assert raw_of(rdb)["spare"] is True
        assert generation(rdb) == gen + 1
        assert raw_cells(rdb)[2] == ("spare", 1, "integer")
        assert buffers_in(scratch_tmp) == []

    def test_removing_raw_true_returns_to_auto(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('        raw: true\n', '')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb) == {"offsite": None, "dc.two": False, "spare": None}
        assert raw_cells(rdb)[0] == ("offsite", None, "null")

    def test_changing_true_to_false(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('        raw: true', '        raw: false')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb)["offsite"] is False
        assert raw_cells(rdb)[0] == ("offsite", 0, "integer")

    def test_raw_auto_in_the_buffer_is_the_absent_key(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('        raw: true', '        raw: auto')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb)["offsite"] is None

    def test_an_unchanged_raw_is_no_changes(self, run, rdb, stub, scratch_tmp):
        state, gen = db_state(rdb), generation(rdb)
        stub.plan([])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0
        assert result.stdout == NO_CHANGES
        assert db_state(rdb) == state and generation(rdb) == gen

    def test_respelling_raw_without_changing_it_is_no_changes(self, run, rdb, stub, scratch_tmp):
        state = db_state(rdb)
        stub.plan([replace('        raw: true', '        raw: yes'),
                   replace('        raw: false', '        raw: no  # never raw')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert result.stdout == NO_CHANGES
        assert db_state(rdb) == state

    @pytest.mark.parametrize("value, shown", [("1", "int 1"), ('"true"', "str 'true'"),
                                              ("0", "int 0"), ("maybe", "str 'maybe'")])
    def test_a_refused_value_names_the_key_and_writes_nothing(self, run, rdb, stub, scratch_tmp, value, shown):
        state = db_state(rdb)
        stub.plan([replace('        raw: true', f'        raw: {value}')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "datasets[tank/vm.01].remote[offsite].raw must be true, false or auto" in result.stderr
        assert shown in result.stderr
        assert db_state(rdb) == state
        assert len(buffers_in(scratch_tmp)) == 1            # the operator's text is kept

    def test_a_refused_value_takes_the_retry_path_and_a_fixed_buffer_saves(self, run, rdb, stub, scratch_tmp, tty):
        gen = generation(rdb)
        stub.plan(
            [replace('        raw: true', '        raw: 1')],
            [replace('        raw: 1', '        raw: false')],
        )
        result = edit(run, rdb, stub, input="r\n")
        assert result.exit_code == 0, result.stderr
        assert "[r]etry editing or [a]bort" in result.stderr
        assert "remote[offsite].raw must be true, false or auto" in result.stderr
        assert stub.runs == 2
        assert 'raw: 1' in stub.log[1]["before"]            # the operator's own text, preserved
        assert raw_of(rdb)["offsite"] is False
        assert generation(rdb) == gen + 1

    def test_a_refused_value_then_abort_keeps_the_buffer_and_writes_nothing(self, run, rdb, stub, scratch_tmp, tty):
        state = db_state(rdb)
        stub.plan([replace('        raw: true', '        raw: "true"')])
        result = edit(run, rdb, stub, input="a\n")
        assert result.exit_code == 1
        assert "remote[offsite].raw" in result.stderr
        assert db_state(rdb) == state
        assert len(buffers_in(scratch_tmp)) == 1
        assert "your edit is kept in" in result.stderr

    def test_a_typo_is_still_an_unknown_key(self, run, rdb, stub, scratch_tmp):
        state = db_state(rdb)
        stub.plan([replace('        raw: true', '        rwa: true')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "datasets[tank/vm.01].remote[offsite]: unknown key 'rwa' (did you mean 'raw'?)" in result.stderr
        assert "valid keys: destination, frequency, retention, raw" in result.stderr
        assert db_state(rdb) == state

    def test_raw_is_not_an_unknown_key(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('        raw: true', '        raw: true\n        frequency: "2h"')])
        assert edit(run, rdb, stub, "--no-prompt").exit_code == 0
        assert raw_of(rdb)["offsite"] is True

    # ---- the rename detector -------------------------------------------------

    def test_a_rename_alone_is_still_refused_when_raw_is_unchanged(self, run, rdb, stub, scratch_tmp):
        state = db_state(rdb)
        stub.plan([replace('- name: "tank/vm.01"', '- name: "tank/vm.02"')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "looks like a rename (dataset 'tank/vm.01' -> 'tank/vm.02')" in result.stderr
        assert db_state(rdb) == state

    def test_a_raw_only_difference_is_not_rename_shaped(self, run, rdb, stub, scratch_tmp):
        """Two entries identical but for `raw` are different: renaming the dataset
        AND changing its raw is a delete + create, not a bare rename."""
        stub.plan([
            replace('- name: "tank/vm.01"', '- name: "tank/vm.02"'),
            replace('        raw: true', '        raw: false'),
        ])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert "looks like a rename" not in result.stderr
        assert [d.name for d in stored(rdb).datasets] == ["tank/vm.02", "tank/data"]
        assert raw_of(rdb, "tank/vm.02")["offsite"] is False

    def test_adding_raw_while_renaming_is_not_rename_shaped_either(self, run, rdb, stub, scratch_tmp):
        stub.plan([
            replace('- name: "tank/vm.01"', '- name: "tank/vm.02"'),
            replace('      - destination: "spare"', '      - destination: "spare"\n        raw: true'),
        ])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb, "tank/vm.02")["spare"] is True

    def test_the_rename_check_compares_raw_in_the_entries(self):
        a = DatasetConfig(name="x", remote=[RemoteDatasetConfig(destination="d", raw=True)])
        b = DatasetConfig(name="y", remote=[RemoteDatasetConfig(destination="d", raw=True)])
        c = DatasetConfig(name="y", remote=[RemoteDatasetConfig(destination="d", raw=False)])
        d = DatasetConfig(name="y", remote=[RemoteDatasetConfig(destination="d")])
        dests = {"d": Destination("https://d.example:8443")}

        def check(old, new):
            return editor._check_rename(
                BackupConfig(datasets=[old], destinations=dests),
                BackupConfig(datasets=[new], destinations=dests),
            )

        assert check(a, b) is not None                       # a bare rename
        assert check(a, c) is None                           # raw true -> false
        assert check(a, d) is None                           # raw true -> auto

    # ---- single-dataset buffers ----------------------------------------------

    def test_the_dataset_buffer_carries_raw(self, run, rdb, stub, scratch_tmp):
        stub.plan([])
        result = edit(run, rdb, stub, "dataset", DS, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert result.stdout == NO_CHANGES
        before = stub.log[0]["before"]
        assert "raw: true" in before and "raw: false" in before

    def test_dataset_buffer_adding_raw_true(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('- destination: "spare"', '- destination: "spare"\n    raw: true')])
        result = edit(run, rdb, stub, "dataset", DS, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb) == {"offsite": True, "dc.two": False, "spare": True}

    def test_dataset_buffer_removing_raw(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('    raw: true\n', '')])
        result = edit(run, rdb, stub, "dataset", DS, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb) == {"offsite": None, "dc.two": False, "spare": None}

    def test_dataset_buffer_refused_value_names_the_key(self, run, rdb, stub, scratch_tmp):
        state = db_state(rdb)
        stub.plan([replace('    raw: true', '    raw: 1')])
        result = edit(run, rdb, stub, "dataset", DS, "--no-prompt")
        assert result.exit_code == 1
        assert "remote[offsite].raw must be true, false or auto" in result.stderr
        assert db_state(rdb) == state

    def test_dataset_buffer_typo(self, run, rdb, stub, scratch_tmp):
        state = db_state(rdb)
        stub.plan([replace('    raw: true', '    rwa: true')])
        result = edit(run, rdb, stub, "dataset", DS, "--no-prompt")
        assert result.exit_code == 1
        assert "datasets[tank/vm.01].remote[offsite]: unknown key 'rwa' (did you mean 'raw'?)" in result.stderr
        assert db_state(rdb) == state

    def test_dataset_buffer_retry_after_a_refused_value(self, run, rdb, stub, scratch_tmp, tty):
        stub.plan(
            [replace('    raw: true', '    raw: "true"')],
            [replace('    raw: "true"', '    raw: false')],
        )
        result = edit(run, rdb, stub, "dataset", DS, input="r\n")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb)["offsite"] is False


# ===========================================================================
# migrate
# ===========================================================================


class TestMigrateCheck:
    def test_check_reports_the_raw_revision(self, run, rdb):
        result = run("migrate", "--check", "-c", rdb)
        assert result.exit_code == 0, result.stderr
        assert result.stdout.strip() == f"Schema is up to date (revision {RAW_REVISION})."
        assert RAW_REVISION == "5d2f8a60c1e4"

    def test_import_stamps_the_raw_revision(self, rdb):
        assert q(rdb, "SELECT version_num FROM alembic_version") == [(RAW_REVISION,)]

    def test_check_on_a_tls_revision_database_says_it_is_behind(self, run, tmp_path):
        resolved, path = make_tls_head_db(tmp_path)
        state = db_state(path)
        result = run("migrate", "--check", "-c", path)
        assert result.exit_code == 1
        assert result.stderr.startswith("error: the schema is not up to date: ")
        assert f"is at schema revision {TLS_REVISION}" in result.stderr
        assert f"this package expects {RAW_REVISION}" in result.stderr
        assert "zfsbackup-config migrate" in result.stderr
        assert db_state(path) == state

    def test_other_commands_refuse_a_tls_revision_database_until_it_is_migrated(self, run, tmp_path):
        _resolved, path = make_tls_head_db(tmp_path)
        state = db_state(path)
        for args in (["export"], ["list", "datasets"], ["get", "snapshot_prefix"]):
            result = run(*args, "-c", path)
            assert result.exit_code == 78, (args, result.stderr)
            assert "migrate" in result.stderr
        assert db_state(path) == state

    def test_migrate_upgrades_a_tls_revision_database_and_keeps_its_rows(self, run, tmp_path):
        _resolved, path = make_tls_head_db(tmp_path)
        result = run("migrate", "-c", path)
        assert result.exit_code == 0, result.stderr
        assert TLS_REVISION in result.stdout and RAW_REVISION in result.stdout
        assert q(path, "SELECT version_num FROM alembic_version") == [(RAW_REVISION,)]
        assert table_rows(path, "retention_rules") == TLS_ROWS["retention_rules"]
        assert q(path, "PRAGMA foreign_key_check") == []
        assert [c[1] for c in raw_cells(path)] == [None, None, None]
        assert run("migrate", "--check", "-c", path).exit_code == 0
        dispose_all()

    def test_a_migrated_database_takes_a_raw_setting(self, run, tmp_path):
        _resolved, path = make_tls_head_db(tmp_path)
        assert run("migrate", "-c", path).exit_code == 0
        gen = generation(path)
        result = run("set", "datasets[tank/a].remote[offsite].raw", "true", "-c", path)
        assert result.exit_code == 0, result.stderr
        assert generation(path) == gen + 1
        assert raw_cells(path)[0] == ("offsite", 1, "integer")
        assert run("get", "datasets[tank/a].remote[offsite].raw", "-c", path).stdout == "true\n"


class TestStampDisagreesWithTheTables:
    """The CLI face of `SchemaColumnsMissing` / the split-brain advice: exit 78
    (the database is unusable), the remedy in the message, the file untouched."""

    def test_head_stamp_without_the_column_is_exit_78_everywhere(self, run, tmp_path):
        _resolved, path = make_headstamped_without_raw(tmp_path)
        state = db_state(path)
        for args in (["migrate", "--check"], ["migrate"], ["export"], ["list", "datasets"],
                     ["get", "snapshot_prefix"], ["set", "snapshot_prefix", "x"]):
            result = run(*args, "-c", path)
            assert result.exit_code == 78, (args, result.stdout, result.stderr)
            assert "is stamped at schema revision" in result.stderr
            assert "dataset_remotes.raw" in result.stderr
            assert f"`alembic stamp {TLS_REVISION}`" in result.stderr
            assert "`zfsbackup-config migrate`" in result.stderr
            assert "OperationalError" not in result.stderr and "Traceback" not in result.stderr
            assert result.stdout == ""
        assert db_state(path) == state

    def test_following_the_advice_repairs_it(self, run, tmp_path):
        _resolved, path = make_headstamped_without_raw(tmp_path)
        stamp(path, TLS_REVISION)
        result = run("migrate", "-c", path)
        assert result.exit_code == 0, result.stderr
        assert run("migrate", "--check", "-c", path).exit_code == 0
        assert run("get", "datasets[tank/a].remote[offsite].raw", "-c", path).stdout == "auto\n"
        dispose_all()

    def test_unstamped_tls_tables_without_the_column_are_told_the_older_revision(self, run, tmp_path):
        _resolved, path = make_tls_head_db(tmp_path)
        drop_stamp(path)
        state = db_state(path)
        for args in (["migrate", "--check"], ["migrate"], ["export"]):
            result = run(*args, "-c", path)
            assert result.exit_code == 78, (args, result.stderr)
            assert f"`alembic stamp {TLS_REVISION}`" in result.stderr
            assert "alembic stamp head" not in result.stderr
        assert db_state(path) == state

    def test_the_older_revision_advice_works_end_to_end(self, run, tmp_path):
        _resolved, path = make_tls_head_db(tmp_path)
        drop_stamp(path)
        stamp(path, TLS_REVISION)
        assert run("migrate", "-c", path).exit_code == 0
        assert run("export", "-c", path).exit_code == 0
        assert [c[1] for c in raw_cells(path)] == [None, None, None]
        dispose_all()


# ===========================================================================
# the warning when an existing entry's raw changes
# ===========================================================================


def expected_warning(dataset, dest, old, new):
    return (
        f"datasets[{dataset}].remote[{dest}].raw changes from {old} to {new}: if destination "
        f"{dest!r} already holds a copy of {dataset!r}, the next backup to it will be refused "
        "until that server copy is renamed aside, after which a full send follows"
    )


def raw_warnings(result):
    """The raw-change warnings on stderr, each without its `warning: ` prefix.
    Found by pattern, not by line: after an interactive prompt (which ends
    without a newline) the next warning shares its line."""
    return re.findall(r"warning: (datasets\[[^\n]*? changes from [^\n]*?full send follows)", result.stderr)


def cfg_with_remotes(*pairs, dataset="tank/a.b") -> BackupConfig:
    """One dataset sending to each `(destination, raw)` pair."""
    return BackupConfig(
        snapshot_prefix="autosnap",
        datasets=[DatasetConfig(
            name=dataset, frequency=D("1h"),
            retention_rules=[RetentionRule(D("1d"), D("30d"))],
            remote=[RemoteDatasetConfig(destination=d, raw=r) for d, r in pairs],
        )],
        destinations={d: Destination(f"https://{d}.example:8443") for d, _ in pairs},
    )


class TestRawChangeWarningsUnit:
    STATES = [None, True, False]

    @pytest.mark.parametrize("old", STATES, ids=repr)
    @pytest.mark.parametrize("new", STATES, ids=repr)
    def test_matrix(self, old, new):
        got = dotpath.raw_change_warnings(
            cfg_with_remotes(("offsite", old)), cfg_with_remotes(("offsite", new))
        )
        if old is new:
            assert got == []
        else:
            text = lambda v: "auto" if v is None else str(v).lower()
            assert got == [expected_warning("tank/a.b", "offsite", text(old), text(new))]

    def test_false_to_auto_is_a_change_not_a_no_op(self):
        # False == None is False in Python, but `not False` / `not None` are the
        # same: a truthiness comparison would swallow this one.
        assert len(dotpath.raw_change_warnings(
            cfg_with_remotes(("o", False)), cfg_with_remotes(("o", None)))) == 1

    def test_a_new_entry_is_not_warned_about(self):
        old = cfg_with_remotes(("a", None))
        new = cfg_with_remotes(("a", None), ("b", True))
        assert dotpath.raw_change_warnings(old, new) == []

    def test_a_removed_entry_is_not_warned_about(self):
        old = cfg_with_remotes(("a", True), ("b", False))
        new = cfg_with_remotes(("a", True))
        assert dotpath.raw_change_warnings(old, new) == []

    def test_a_new_dataset_and_a_removed_dataset_are_not_warned_about(self):
        old = cfg_with_remotes(("a", True), dataset="tank/x")
        new = cfg_with_remotes(("a", False), dataset="tank/y")
        assert dotpath.raw_change_warnings(old, new) == []

    def test_the_same_destination_under_another_dataset_is_a_different_entry(self):
        old = cfg_with_remotes(("a", True), dataset="tank/x")
        new = cfg_with_remotes(("a", True), dataset="tank/x")
        new.datasets.append(DatasetConfig(
            name="tank/y", frequency=D("1h"),
            retention_rules=[RetentionRule(D("1d"), D("30d"))],
            remote=[RemoteDatasetConfig(destination="a", raw=False)]))
        assert dotpath.raw_change_warnings(old, new) == []

    def test_one_warning_per_changed_entry_in_the_new_configs_order(self):
        old = cfg_with_remotes(("a", None), ("b", True), ("c", False))
        new = cfg_with_remotes(("a", True), ("b", True), ("c", None))
        got = dotpath.raw_change_warnings(old, new)
        assert got == [
            expected_warning("tank/a.b", "a", "auto", "true"),
            expected_warning("tank/a.b", "c", "false", "auto"),
        ]

    def test_a_dotted_dataset_and_destination_are_printed_in_bracket_form(self):
        got = dotpath.raw_change_warnings(
            cfg_with_remotes(("dc.two", True), dataset="tank/vm.01"),
            cfg_with_remotes(("dc.two", False), dataset="tank/vm.01"),
        )
        assert got and got[0].startswith("datasets[tank/vm.01].remote[dc.two].raw changes from")

    def test_it_does_not_modify_either_configuration(self):
        old, new = cfg_with_remotes(("a", True)), cfg_with_remotes(("a", False))
        dotpath.raw_change_warnings(old, new)
        assert old.datasets[0].remote[0].raw is True and new.datasets[0].remote[0].raw is False


class TestRawChangeWarningThroughSetAndUnset:
    @pytest.mark.parametrize("path, value, dest, old, new", [
        (SPARE, "true", "spare", "auto", "true"),
        (OFFSITE, "false", "offsite", "true", "false"),
        (DC_TWO, "true", "dc.two", "false", "true"),
        (DC_TWO, "auto", "dc.two", "false", "auto"),
        (DC_TWO, "", "dc.two", "false", "auto"),            # an empty value is auto
        (OFFSITE, "auto", "offsite", "true", "auto"),
    ])
    def test_each_change_warns_once_naming_the_dataset_and_destination(
        self, run, rdb, path, value, dest, old, new
    ):
        result = ok(run, rdb, "set", path, value)
        assert raw_warnings(result) == [expected_warning(DS, dest, old, new)]
        assert result.stderr.count(" changes from ") == 1

    @pytest.mark.parametrize("path, value", [(OFFSITE, "true"), (DC_TWO, "false"), (SPARE, "auto"), (SPARE, "")])
    def test_a_same_value_set_does_not_warn(self, run, rdb, path, value):
        result = ok(run, rdb, "set", path, value)
        assert raw_warnings(result) == []
        assert "changes from" not in result.stdout + result.stderr

    @pytest.mark.parametrize("path, dest, old", [(OFFSITE, "offsite", "true"), (DC_TWO, "dc.two", "false")])
    def test_unset_warns_to_auto(self, run, rdb, path, dest, old):
        result = ok(run, rdb, "unset", path)
        assert raw_warnings(result) == [expected_warning(DS, dest, old, "auto")]

    def test_unset_on_auto_does_not_warn(self, run, rdb):
        result = ok(run, rdb, "unset", SPARE)
        assert raw_warnings(result) == []

    def test_a_dotted_dataset_and_destination_appear_in_bracket_form(self, run, rdb):
        result = ok(run, rdb, "set", "datasets.tank/vm.01.remote.dc.two.raw", "true")
        assert raw_warnings(result) == [
            "datasets[tank/vm.01].remote[dc.two].raw changes from false to true: if destination "
            "'dc.two' already holds a copy of 'tank/vm.01', the next backup to it will be refused "
            "until that server copy is renamed aside, after which a full send follows"
        ]

    def test_setting_something_else_does_not_warn(self, run, rdb):
        result = ok(run, rdb, "set", "datasets[tank/vm.01].remote[offsite].frequency", "2h")
        assert raw_warnings(result) == []
        result = ok(run, rdb, "set", "check_interval", "10m")
        assert raw_warnings(result) == []

    def test_a_refused_value_gives_an_error_and_no_raw_warning(self, run, rdb):
        result = run("set", OFFSITE, "1", "-c", rdb)
        assert result.exit_code == 1
        assert raw_warnings(result) == []

    def test_the_warning_precedes_the_confirmation(self, run, rdb):
        result = ok(run, rdb, "set", OFFSITE, "false")
        out = result.output
        assert out.index("changes from") < out.index("config updated")

    def test_the_warning_is_printed_before_the_commit(self, run, rdb, monkeypatch):
        # A racing write makes the commit lose (exit 75): the warning was already out.
        from test_zfsbackup_cli_dotpath import racing_write

        done = racing_write(monkeypatch, rdb)
        result = run("set", OFFSITE, "false", "-c", rdb)
        assert done == [True]
        assert result.exit_code == 75, result.stderr
        assert raw_warnings(result) == [expected_warning(DS, "offsite", "true", "false")]
        assert raw_of(rdb)["offsite"] is True                 # and nothing was written

    def test_import_does_not_warn(self, run, rdb, tmp_path):
        flipped = RAW_YAML.replace('- destination: "offsite"\n        raw: true',
                                   '- destination: "offsite"\n        raw: false')
        assert flipped != RAW_YAML
        source = write_yaml(tmp_path, flipped, "flipped.yaml")
        result = run("import", source, "-c", rdb)
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb)["offsite"] is False                # the import did change it
        assert "changes from" not in result.stdout + result.stderr


class TestRawChangeWarningThroughEdit:
    DC_TWO_BLOCK = (
        '      - destination: "dc.two"\n        frequency: "4h"\n        retention:\n'
        '          "1d": "7d"\n        raw: false\n'
    )

    def test_whole_config_changing_an_entry_warns_once(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('        raw: true', '        raw: false')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_warnings(result) == [expected_warning(DS, "offsite", "true", "false")]
        assert result.stderr.count(" changes from ") == 1
        assert result.output.index("changes from") < result.output.index("Configuration saved")

    def test_each_changed_entry_warns_once(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('        raw: true', '        raw: false'),
                   replace('      - destination: "spare"', '      - destination: "spare"\n        raw: true')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_warnings(result) == [
            expected_warning(DS, "offsite", "true", "false"),
            expected_warning(DS, "spare", "auto", "true"),
        ]

    def test_single_dataset_changing_an_entry_warns_once(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('    raw: false', '    raw: true')])
        result = edit(run, rdb, stub, "dataset", DS, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_warnings(result) == [expected_warning(DS, "dc.two", "false", "true")]

    def test_removing_raw_from_an_entry_warns_to_auto(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('        raw: true\n', '')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert raw_warnings(result) == [expected_warning(DS, "offsite", "true", "auto")]

    def test_adding_a_new_entry_with_raw_does_not_warn_whole_config(self, run, rdb, stub, scratch_tmp):
        # Give `tank/data` a brand-new remote that carries raw.
        text_edit = {"replace": ['  - name: "tank/data"\n    enabled: true\n    frequency: "1h"\n'
                                 '    recursive: false\n    retention:\n      "1d": "30d"\n',
                                 '  - name: "tank/data"\n    enabled: true\n    frequency: "1h"\n'
                                 '    recursive: false\n    retention:\n      "1d": "30d"\n'
                                 '    remote:\n      - destination: "spare"\n        raw: true\n']}
        stub.plan([text_edit])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb, "tank/data") == {"spare": True}
        assert raw_warnings(result) == []

    def test_adding_a_new_entry_with_raw_does_not_warn_single_dataset(self, run, rdb, stub, scratch_tmp):
        stub.plan([{"write": 'remote:\n  - destination: "offsite"\n    raw: false\n'}])
        result = edit(run, rdb, stub, "dataset", "tank/data", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb, "tank/data") == {"offsite": False}
        assert raw_warnings(result) == []

    def test_removing_an_entry_gives_no_raw_warning(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace(self.DC_TWO_BLOCK, '')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_of(rdb) == {"offsite": True, "spare": None}
        assert raw_warnings(result) == []

    def test_an_unchanged_raw_does_not_warn(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('check_interval: "5m"', 'check_interval: "6m"')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_warnings(result) == []

    def test_a_rename_with_a_raw_flip_is_delete_and_create_so_no_raw_warning(self, run, rdb, stub, scratch_tmp):
        stub.plan([replace('- name: "tank/vm.01"', '- name: "tank/vm.02"'),
                   replace('        raw: true', '        raw: false')])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert raw_warnings(result) == []

    def test_a_retry_does_not_duplicate_the_warning(self, run, rdb, stub, scratch_tmp, tty):
        stub.plan(
            [replace('        raw: true', '        raw: 1')],
            [replace('        raw: 1', '        raw: false')],
        )
        result = edit(run, rdb, stub, input="r\n")
        assert result.exit_code == 0, result.stderr
        assert stub.runs == 2
        assert raw_warnings(result) == [expected_warning(DS, "offsite", "true", "false")]

    def test_the_warning_is_printed_before_the_commit(self, run, rdb, stub, scratch_tmp):
        # A concurrent `set` from inside the editor makes the commit lose (exit 75).
        stub.plan([replace('        raw: true', '        raw: false'),
                   {"cli": ["set", "check_interval", "10m", "-c", str(rdb)]}])
        result = edit(run, rdb, stub, "--no-prompt")
        assert result.exit_code == 75, result.stderr
        assert raw_warnings(result) == [expected_warning(DS, "offsite", "true", "false")]
        assert raw_of(rdb)["offsite"] is True


class TestTlsColumnMissingThroughTheCli:
    def test_head_stamp_without_server_fingerprint_is_exit_78_with_the_backup_remedy(self, run, tmp_path):
        resolved, path = make_tls_head_db(tmp_path)
        upgrade_schema(resolved)
        dispose_all()
        drop_server_fingerprint(path)
        state = db_state(path)
        for args in (["migrate", "--check"], ["migrate"], ["export"], ["list", "datasets"]):
            result = run(*args, "-c", path)
            assert result.exit_code == 78, (args, result.stdout, result.stderr)
            assert "destinations.server_fingerprint" in result.stderr
            assert "Restore the database from a backup." in result.stderr
            assert "alembic stamp" not in result.stderr
            assert "Traceback" not in result.stderr and "OperationalError" not in result.stderr
        assert db_state(path) == state

    def test_unstamped_tables_without_the_tls_columns_are_not_a_config_database(self, run, tmp_path):
        _resolved, path = make_tls_head_db(tmp_path)
        drop_server_fingerprint(path)
        drop_stamp(path)
        state = db_state(path)
        for args in (["migrate", "--check"], ["migrate"], ["export"]):
            result = run(*args, "-c", path)
            assert result.exit_code == 78, (args, result.stderr)
            assert "not a zfsbackup config database" in result.stderr
        assert db_state(path) == state

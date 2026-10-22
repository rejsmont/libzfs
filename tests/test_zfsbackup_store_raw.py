"""Milestone B (decision B3), store half: `dataset_remotes.raw`.

`RemoteDatasetConfig.raw` (`None` = auto, `True` = raw required, `False` =
never raw) is stored in `dataset_remotes.raw`, a nullable BOOLEAN with NO
default: NULL is the meaningful third state, and a default would silently turn
"auto" into a hard stream-format choice. Alembic revision `5d2f8a60c1e4`
(revising the TLS revision `7c3e9d51b2a8`) adds the column.

Covered:

- ``TestUpgradeFromTheTlsRevision``  2 -> 3 on a populated database: every row
  NULL, FK check clean, `retention_rules` (incl. the per-destination overrides
  behind the composite FK) intact, the composite FK still cascading, the
  schema equal to `create_all`, the column nullable with no default.
- ``TestDowngradeToTheTlsRevision``  3 -> 2: the column goes, rows and rules
  survive, a later upgrade brings the column back as NULL (the values are gone).
- ``TestMapperRoundTrip``            {True, False, None} with and without a
  per-destination retention override; None never becomes False; 0/1 read back
  as real bools; the rows hold 1 / 0 / NULL with the right storage class.
- ``TestImportStoresRaw``            each YAML spelling lands as 1/0/NULL/NULL; a
  refused value creates no database.
- ``TestRawIsPartOfTheStoredConfig`` a change of `raw` alone changes `same_as`
  and bumps the generation; an unchanged `raw` is `same_as`.
- ``TestSchemaStamp``                `EXPECTED_HEAD` / `KNOWN_REVISIONS` for the
  three-revision chain; the unstamped-database advice per case (the older
  revision's `alembic stamp 7c3e9d51b2a8` without `raw`, `stamp head` with it)
  and that following it ends with a readable database.
- ``TestHeadStampWithoutTheColumn``  a head-stamped database lacking `raw`:
  `SchemaColumnsMissing` (not OperationalError) from `check_schema`,
  `read_config` and `upgrade_schema`, file unchanged, the advice repairs it.
- ``TestColumnRequirementHelpers`` / ``TestEveryColumnOnlyRevisionIsRegistered``
  `_columns_required_by`, `_revision_before`, and the GUARD that every
  column-only revision has a `_COLUMNS_ADDED_AT_REVISION` entry.

Every engine is built through `store.db` (`make_engine`, `foreign_keys=True`),
so the composite retention FK is really enforced.

Mutation checks (scratch copy, one revert at a time; failing tests across this
file, `test_zfsbackup_cli_raw.py`, `test_zfsbackup_config.py` and the existing
store / CLI files): the mapper dropping `raw` on save 93 (18 here) / on load 91
(16 here); `None` coerced to `False` on load 70; the migration's downgrade a
no-op 5 (4 here, 1 in `test_zfsbackup_store_tls_schema.py`); a
`server_default='0'` on the model column 14 (8 here) / in the migration 8 (7
here, 1 in the CLI file).

Stamp/column round (same method): the column requirement ignored when matching
an unstamped database 2 (1 here, 1 in the CLI file); `check_schema` without
the column check 5 (3 here, 1 CLI, 1 in `test_zfsbackup_store.py`'s WAL-gate
agreement test); `_COLUMNS_ADDED_AT_REVISION` emptied 10 (8 here, of which the
guard `test_every_added_column_has_an_entry_and_every_entry_is_real` is one).

Registry round (after the TLS columns were registered; re-measured): the `raw`
entry deleted 11; the whole TLS entry deleted 10; one TLS column deleted 3 (the
guard, the helper test and the missing-column test); the column requirement
ignored when matching 5; `check_schema` without the column check 8;
`_stamp_back_is_safe` always True 4.
"""

import sqlite3
from pathlib import Path

import pytest
import yaml
from sqlalchemy import inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from _raw_support import (
    BASELINE_REVISION,
    RAW_REVISION,
    TLS_REVISION,
    TLS_ROWS,
    USER_VERSION,
    alembic_run,
    column_names,
    drop_server_fingerprint,
    drop_stamp,
    make_headstamped_without_raw,
    make_tls_head_db,
    q,
    raw_cells,
    schema_snapshot,
    stamp,
    table_rows,
)
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
    RetentionRule,
)
from zfsbackup.config.store import (
    Base,
    NotAConfigDatabase,
    SchemaColumnsMissing,
    SchemaError,
    SchemaOutOfDate,
    SchemaSplitBrain,
    check_schema,
    dispose_all,
    import_yaml,
    load_config,
    make_engine,
    open_config_connection,
    read_config,
    resolve_config_path,
    save_config,
    upgrade_schema,
    write_config,
)
from zfsbackup.config.store import migrate as migrate_module
from zfsbackup.config.store import models

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _dispose():
    yield
    dispose_all()


@pytest.fixture
def old(tmp_path):
    """A populated database at the TLS revision (no `raw` column yet)."""
    return make_tls_head_db(tmp_path)


def fk_engine(db_path: Path):
    """The store's own engine over the file, FK enforcement on."""
    return make_engine(f"sqlite:///{db_path}", foreign_keys=True)


# ===========================================================================
# the migration, up
# ===========================================================================


class TestUpgradeFromTheTlsRevision:
    def test_the_fixture_is_really_the_tls_revision(self, old):
        _resolved, db = old
        assert q(db, "SELECT version_num FROM alembic_version") == [(TLS_REVISION,)]
        assert "raw" not in column_names(db, "dataset_remotes")
        assert q(db, "PRAGMA user_version") == [(USER_VERSION,)]
        assert len(table_rows(db, "dataset_remotes")) == 3
        assert len(table_rows(db, "retention_rules")) == 4

    def test_check_schema_reports_it_out_of_date_naming_both_revisions(self, old):
        resolved, _db = old
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaOutOfDate) as excinfo:
                check_schema(conn)
        assert TLS_REVISION in str(excinfo.value) and RAW_REVISION in str(excinfo.value)
        assert excinfo.value.recorded_revisions == (TLS_REVISION,)

    def test_upgrade_reaches_the_head_and_stamps_it(self, old):
        resolved, db = old
        result = upgrade_schema(resolved)
        assert result.changed
        assert result.from_revs == (TLS_REVISION,) and result.to_rev == RAW_REVISION
        assert q(db, "SELECT version_num FROM alembic_version") == [(RAW_REVISION,)]

    def test_every_existing_row_comes_out_null_not_false(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        assert raw_cells(db) == [
            ("offsite", None, "null"), ("dc2", None, "null"), ("offsite", None, "null"),
        ]
        assert q(db, "SELECT count(*) FROM dataset_remotes WHERE raw IS NOT NULL") == [(0,)]

    def test_the_foreign_key_check_is_clean(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        assert q(db, "PRAGMA foreign_key_check") == []

    def test_retention_rules_survive_including_the_per_destination_overrides(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        assert table_rows(db, "retention_rules") == TLS_ROWS["retention_rules"]
        assert [r for r in table_rows(db, "retention_rules") if r[2] is not None] == [
            TLS_ROWS["retention_rules"][1], TLS_ROWS["retention_rules"][3],
        ]

    def test_the_other_tables_and_the_store_id_are_untouched(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        assert table_rows(db, "datasets") == TLS_ROWS["datasets"]
        assert table_rows(db, "remote_server") == TLS_ROWS["remote_server"]
        assert [r[:3] for r in table_rows(db, "host_identity")] == [(1, "host-1", 1.0)]
        assert q(db, "PRAGMA user_version") == [(USER_VERSION,)]
        # The five old columns of every remote row are what they were.
        assert table_rows(db, "dataset_remotes", "id, dataset_id, destination_name, "
                                                 "frequency_seconds, frequency_literal") == \
            TLS_ROWS["dataset_remotes"]

    def test_the_column_is_a_nullable_boolean_with_no_default(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        (cid, name, ctype, notnull, default, pk) = [
            r for r in q(db, "PRAGMA table_info(dataset_remotes)") if r[1] == "raw"
        ][0]
        assert ctype.upper() == "BOOLEAN"
        assert notnull == 0
        assert default is None
        assert pk == 0

    def test_a_row_inserted_without_raw_is_null_not_zero(self, old):
        # A server_default of 0 would turn every unspecified row into "never raw".
        resolved, db = old
        upgrade_schema(resolved)
        conn = sqlite3.connect(str(db))
        try:
            conn.execute("INSERT INTO dataset_remotes (dataset_id, destination_name) "
                         "VALUES (2, 'dc2')")
            conn.commit()
        finally:
            conn.close()
        assert raw_cells(db)[-1] == ("dc2", None, "null")

    def test_the_rebuild_kept_the_constraints_of_dataset_remotes(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        (sql,) = q(db, "SELECT sql FROM sqlite_master WHERE name='dataset_remotes'")[0]
        assert "ck_dataset_remotes_frequency_positive" in sql
        assert "uq_dataset_remotes_dataset_destination" in sql
        assert "uq_dataset_remotes_dataset_id_id" in sql
        assert "REFERENCES destinations" in sql
        assert "_alembic_tmp" not in " ".join(r[0] or "" for r in q(db, "SELECT sql FROM sqlite_master"))

    def test_the_composite_foreign_key_still_points_at_the_rebuilt_table(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        refs = q(db, "PRAGMA foreign_key_list(retention_rules)")
        composite = [r for r in refs if r[2] == "dataset_remotes"]
        assert sorted((r[3], r[4]) for r in composite) == [
            ("dataset_id", "dataset_id"), ("dataset_remote_id", "id"),
        ]
        assert {r[6] for r in composite} == {"CASCADE"}

    def test_deleting_a_remote_still_cascades_to_its_scoped_rules(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        engine = fk_engine(db)
        try:
            with engine.begin() as conn:
                assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
                conn.execute(text("DELETE FROM dataset_remotes WHERE id = 1"))
        finally:
            engine.dispose()
        ids = [r[0] for r in table_rows(db, "retention_rules")]
        assert ids == [1, 3, 4]                # rule 2 (scoped to remote 1) went with it

    def test_the_upgraded_store_loads_with_raw_auto_everywhere(self, old):
        resolved, _db = old
        upgrade_schema(resolved)
        snapshot = read_config(resolved)
        dispose_all()
        assert snapshot.schema_rev == RAW_REVISION
        remotes = [r for ds in snapshot.config.datasets for r in ds.remote]
        assert [(r.destination, r.raw) for r in remotes] == [
            ("offsite", None), ("dc2", None), ("offsite", None),
        ]
        assert all(r.raw is None for r in remotes)
        # ... and the overrides are still attached to the right remote.
        first, second = snapshot.config.datasets[0].remote
        assert [(r.age.literal, r.keep_for.literal) for r in first.retention_rules] == [("1h", "1d")]
        assert second.retention_rules == []
        assert [(r.age.literal, r.keep_for.literal)
                for r in snapshot.config.datasets[1].remote[0].retention_rules] == [("2m", "20m")]

    def test_the_upgraded_schema_equals_create_all(self, old, tmp_path):
        resolved, db = old
        upgrade_schema(resolved)
        ref = make_engine(f"sqlite:///{tmp_path / 'ref.db'}")
        Base.metadata.create_all(ref)
        mig = make_engine(f"sqlite:///{db}")
        try:
            a, b = inspect(mig), inspect(ref)
            assert sorted(set(a.get_table_names()) - {"alembic_version"}) == sorted(b.get_table_names())
            for table in sorted(Base.metadata.tables):
                got = {c["name"]: (str(c["type"]), c["nullable"], c["default"])
                       for c in a.get_columns(table)}
                want = {c["name"]: (str(c["type"]), c["nullable"], c["default"])
                        for c in b.get_columns(table)}
                assert got == want, table
                assert {c["name"] for c in a.get_check_constraints(table)} == \
                       {c["name"] for c in b.get_check_constraints(table)}, table
                assert {i["name"] for i in a.get_indexes(table)} == \
                       {i["name"] for i in b.get_indexes(table)}, table
                assert {u["name"] for u in a.get_unique_constraints(table)} == \
                       {u["name"] for u in b.get_unique_constraints(table)}, table
        finally:
            ref.dispose()
            mig.dispose()

    def test_upgrading_again_is_a_no_op(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        before = q(db, "SELECT * FROM sqlite_master ORDER BY name")
        again = upgrade_schema(resolved)
        assert not again.changed
        assert q(db, "SELECT * FROM sqlite_master ORDER BY name") == before

    def test_the_migration_is_one_step_not_a_rebuild_of_the_chain(self, old):
        resolved, db = old
        assert alembic_run(resolved, "upgrade", RAW_REVISION) == []
        assert q(db, "SELECT version_num FROM alembic_version") == [(RAW_REVISION,)]
        assert "raw" in column_names(db, "dataset_remotes")


# ===========================================================================
# the migration, down
# ===========================================================================


class TestDowngradeToTheTlsRevision:
    @pytest.fixture
    def upgraded(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        # Distinctive post-upgrade values the downgrade is allowed to discard.
        conn = sqlite3.connect(str(db))
        try:
            conn.execute("UPDATE dataset_remotes SET raw = 1 WHERE id = 1")
            conn.execute("UPDATE dataset_remotes SET raw = 0 WHERE id = 3")
            conn.commit()
        finally:
            conn.close()
        return resolved, db

    def test_the_setup_holds_real_values(self, upgraded):
        _resolved, db = upgraded
        assert raw_cells(db) == [
            ("offsite", 1, "integer"), ("dc2", None, "null"), ("offsite", 0, "integer"),
        ]

    def test_the_column_is_gone_and_the_version_is_the_tls_revision(self, upgraded):
        resolved, db = upgraded
        assert alembic_run(resolved, "downgrade", TLS_REVISION) == []
        assert "raw" not in column_names(db, "dataset_remotes")
        assert q(db, "SELECT version_num FROM alembic_version") == [(TLS_REVISION,)]

    def test_rows_and_rules_survive_the_downgrade(self, upgraded):
        resolved, db = upgraded
        assert alembic_run(resolved, "downgrade", TLS_REVISION) == []
        assert table_rows(db, "dataset_remotes") == TLS_ROWS["dataset_remotes"]
        assert table_rows(db, "retention_rules") == TLS_ROWS["retention_rules"]
        assert table_rows(db, "datasets") == TLS_ROWS["datasets"]
        assert table_rows(db, "remote_server") == TLS_ROWS["remote_server"]
        assert [r[:3] for r in table_rows(db, "host_identity")] == [(1, "host-1", 1.0)]
        assert q(db, "PRAGMA user_version") == [(USER_VERSION,)]
        assert q(db, "PRAGMA foreign_key_check") == []

    def test_the_downgraded_table_keeps_its_constraints_and_the_composite_fk(self, upgraded):
        resolved, db = upgraded
        alembic_run(resolved, "downgrade", TLS_REVISION)
        (sql,) = q(db, "SELECT sql FROM sqlite_master WHERE name='dataset_remotes'")[0]
        assert "ck_dataset_remotes_frequency_positive" in sql
        assert "uq_dataset_remotes_dataset_id_id" in sql
        refs = [r for r in q(db, "PRAGMA foreign_key_list(retention_rules)") if r[2] == "dataset_remotes"]
        assert len(refs) == 2
        assert "_alembic_tmp" not in " ".join(r[0] or "" for r in q(db, "SELECT sql FROM sqlite_master"))

    def test_the_downgraded_store_is_out_of_date_for_this_code(self, upgraded):
        resolved, _db = upgraded
        alembic_run(resolved, "downgrade", TLS_REVISION)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaOutOfDate) as excinfo:
                check_schema(conn)
        assert excinfo.value.recorded_revisions == (TLS_REVISION,)

    def test_upgrading_after_the_downgrade_gives_null_again_not_the_old_values(self, upgraded):
        resolved, db = upgraded
        alembic_run(resolved, "downgrade", TLS_REVISION)
        assert alembic_run(resolved, "upgrade", RAW_REVISION) == []
        assert raw_cells(db) == [
            ("offsite", None, "null"), ("dc2", None, "null"), ("offsite", None, "null"),
        ]
        assert table_rows(db, "retention_rules") == TLS_ROWS["retention_rules"]
        assert q(db, "PRAGMA foreign_key_check") == []

    def test_the_whole_chain_down_to_the_baseline_and_back_up(self, upgraded):
        resolved, db = upgraded
        assert alembic_run(resolved, "downgrade", BASELINE_REVISION) == []
        assert q(db, "SELECT version_num FROM alembic_version") == [(BASELINE_REVISION,)]
        assert "raw" not in column_names(db, "dataset_remotes")
        assert table_rows(db, "dataset_remotes") == TLS_ROWS["dataset_remotes"]
        assert alembic_run(resolved, "upgrade", RAW_REVISION) == []
        assert "raw" in column_names(db, "dataset_remotes")
        assert table_rows(db, "retention_rules") == TLS_ROWS["retention_rules"]


# ===========================================================================
# the mapper
# ===========================================================================


@pytest.fixture
def session():
    engine = make_engine("sqlite:///:memory:", foreign_keys=True)
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        assert s.execute(text("PRAGMA foreign_keys")).scalar() == 1   # the store's engine, FKs ON
        yield s
    engine.dispose()


def _override(n: int = 1):
    return [RetentionRule(Duration("1d"), Duration("7d"))] + (
        [RetentionRule(Duration("1w"), Duration("3M"))] if n > 1 else []
    )


def config_with(raws, *, override=False) -> BackupConfig:
    """One dataset sending to `d0..dN`, destination `i` carrying `raws[i]`."""
    names = [f"d{i}" for i in range(len(raws))]
    return BackupConfig(
        snapshot_prefix="autosnap",
        datasets=[DatasetConfig(
            name="tank/data",
            frequency=Duration("1h"),
            retention_rules=[RetentionRule(Duration("1d"), Duration("30d"))],
            remote=[
                RemoteDatasetConfig(
                    destination=n,
                    frequency=Duration("4h") if override else None,
                    retention_rules=_override() if override else [],
                    raw=r,
                )
                for n, r in zip(names, raws)
            ],
        )],
        destinations={n: Destination(f"https://{n}.example:8443") for n in names},
    )


def roundtrip(session, config: BackupConfig) -> BackupConfig:
    save_config(session, config)
    session.commit()
    session.expire_all()
    return load_config(session)


class TestMapperRoundTrip:
    @pytest.mark.parametrize("override", [False, True], ids=["inherit", "override"])
    @pytest.mark.parametrize("raw", [True, False, None], ids=["true", "false", "auto"])
    def test_one_value(self, session, raw, override):
        config = config_with([raw], override=override)
        loaded = roundtrip(session, config)
        assert loaded == config
        assert loaded.same_as(config)
        remote = loaded.datasets[0].remote[0]
        assert remote.raw is raw
        # The override travels with the remote, whatever raw is.
        assert remote.retention_rules == (_override() if override else [])

    def test_none_never_becomes_false(self, session):
        remote = roundtrip(session, config_with([None])).datasets[0].remote[0]
        assert remote.raw is None
        assert remote.raw is not False

    def test_false_does_not_become_none(self, session):
        remote = roundtrip(session, config_with([False])).datasets[0].remote[0]
        assert remote.raw is False

    @pytest.mark.parametrize("override", [False, True], ids=["inherit", "override"])
    def test_all_three_states_in_one_dataset_keep_their_own_remote(self, session, override):
        loaded = roundtrip(session, config_with([True, None, False], override=override))
        assert [(r.destination, r.raw) for r in loaded.datasets[0].remote] == [
            ("d0", True), ("d1", None), ("d2", False),
        ]

    def test_two_datasets_can_disagree_about_the_same_destination(self, session):
        config = config_with([True])
        config.datasets.append(DatasetConfig(
            name="tank/other", frequency=Duration("1h"),
            retention_rules=[RetentionRule(Duration("1d"), Duration("30d"))],
            remote=[RemoteDatasetConfig(destination="d0", raw=False)],
        ))
        loaded = roundtrip(session, config)
        assert [d.remote[0].raw for d in loaded.datasets] == [True, False]

    @pytest.mark.parametrize("raw, stored, storage_class", [
        (True, 1, "integer"), (False, 0, "integer"), (None, None, "null"),
    ])
    def test_the_row_holds_1_0_or_null(self, session, raw, stored, storage_class):
        roundtrip(session, config_with([raw]))
        assert session.execute(text("SELECT raw, typeof(raw) FROM dataset_remotes")).all() == [
            (stored, storage_class)
        ]
        row = session.query(models.DatasetRemote).one()
        assert row.raw is raw

    @pytest.mark.parametrize("stored, expected", [(1, True), (0, False), (None, None)])
    def test_integers_in_the_row_come_back_as_real_bools(self, session, stored, expected):
        roundtrip(session, config_with([None]))
        session.execute(text("UPDATE dataset_remotes SET raw = :v"), {"v": stored})
        session.commit()
        session.expire_all()
        raw = load_config(session).datasets[0].remote[0].raw
        assert raw is expected                  # `is`: True/False, never 1/0

    def test_a_resave_replaces_the_value(self, session):
        roundtrip(session, config_with([True]))
        loaded = roundtrip(session, config_with([False]))
        assert loaded.datasets[0].remote[0].raw is False
        loaded = roundtrip(session, config_with([None]))
        assert loaded.datasets[0].remote[0].raw is None
        assert session.execute(text("SELECT count(*) FROM dataset_remotes")).scalar() == 1

    def test_cascade_through_a_loaded_remote_keeps_the_scoped_rules_consistent(self, session):
        # Traverse the relationship BEFORE deleting (the item-3 bug was invisible without it).
        roundtrip(session, config_with([True, False], override=True))
        dataset = session.query(models.Dataset).one()
        remotes = list(dataset.remotes)
        assert [len(r.retention_rules) for r in remotes] == [1, 1]
        session.delete(remotes[0])
        session.commit()
        session.expire_all()
        loaded = load_config(session).datasets[0].remote
        assert [(r.destination, r.raw) for r in loaded] == [("d1", False)]
        assert session.query(models.RetentionRule).filter(
            models.RetentionRule.dataset_remote_id.isnot(None)).count() == 1


# ===========================================================================
# import
# ===========================================================================

RAW_YAML = """\
snapshot_prefix: "autosnap"
datasets:
  - name: "tank/data"
    frequency: "1h"
    remote:
      - destination: "a"
        raw: true
      - destination: "b"
        raw: false
      - destination: "c"
        raw: auto
      - destination: "d"
      - destination: "e"
        raw: yes
        frequency: "4h"
        retention:
          "1d": "7d"
destinations:
  a: {url: "https://a.example:8443"}
  b: {url: "https://b.example:8443"}
  c: {url: "https://c.example:8443"}
  d: {url: "https://d.example:8443"}
  e: {url: "https://e.example:8443"}
"""


class TestImportStoresRaw:
    def test_each_spelling_lands_as_1_0_null_null(self, tmp_path):
        source = tmp_path / "in.yaml"
        source.write_text(RAW_YAML)
        resolved = resolve_config_path(str(tmp_path / "cfg" / "config.db"))
        import_yaml(source, resolved)
        dispose_all()
        assert raw_cells(resolved.resolved_path) == [
            ("a", 1, "integer"),
            ("b", 0, "integer"),
            ("c", None, "null"),
            ("d", None, "null"),
            ("e", 1, "integer"),               # YAML `yes`
        ]

    def test_the_stored_configuration_equals_from_file(self, tmp_path):
        source = tmp_path / "in.yaml"
        source.write_text(RAW_YAML)
        resolved = resolve_config_path(str(tmp_path / "cfg" / "config.db"))
        import_yaml(source, resolved)
        dispose_all()
        stored = read_config(resolved).config
        dispose_all()
        parsed = BackupConfig.from_file(source)
        assert stored == parsed
        assert [r.raw for r in stored.datasets[0].remote] == [True, False, None, None, True]
        assert [r.raw for r in parsed.datasets[0].remote] == [True, False, None, None, True]

    def test_a_reimport_replaces_the_values(self, tmp_path):
        source = tmp_path / "in.yaml"
        source.write_text(RAW_YAML)
        resolved = resolve_config_path(str(tmp_path / "cfg" / "config.db"))
        import_yaml(source, resolved)
        dispose_all()
        source.write_text(RAW_YAML.replace("raw: true", "raw: false").replace("raw: auto", "raw: true"))
        import_yaml(source, resolved)
        dispose_all()
        assert [c[1] for c in raw_cells(resolved.resolved_path)] == [0, 0, 1, None, 1]

    @pytest.mark.parametrize("bad", ["1", "0", '"true"', "maybe"])
    def test_a_refused_value_is_refused_by_the_import_and_creates_no_database(self, tmp_path, bad):
        source = tmp_path / "in.yaml"
        source.write_text(RAW_YAML.replace("raw: true", f"raw: {bad}"))
        target = tmp_path / "cfg" / "config.db"
        resolved = resolve_config_path(str(target))
        with pytest.raises(ValueError, match=r"remote\[a\]\.raw"):
            import_yaml(source, resolved)
        dispose_all()
        assert not target.exists() or target.stat().st_size == 0


# ===========================================================================
# the stored configuration: raw counts
# ===========================================================================


@pytest.fixture
def stored_pair(tmp_path):
    """A file database holding `raw: true` on `d0`; returns the resolved path
    and the snapshot read back."""
    source = tmp_path / "in.yaml"
    source.write_text(yaml.safe_dump({
        "snapshot_prefix": "autosnap",
        "datasets": [{"name": "tank/data", "frequency": "1h",
                      "remote": [{"destination": "d0", "raw": True},
                                 {"destination": "d1"}]}],
        "destinations": {"d0": {"url": "https://d0.example:8443"},
                         "d1": {"url": "https://d1.example:8443"}},
    }, sort_keys=False))
    resolved = resolve_config_path(str(tmp_path / "cfg" / "config.db"))
    import_yaml(source, resolved)
    dispose_all()
    snapshot = read_config(resolved)
    dispose_all()
    return resolved, snapshot


class TestRawIsPartOfTheStoredConfig:
    @pytest.mark.parametrize("new", [False, None])
    def test_changing_raw_alone_changes_same_as_and_bumps_the_generation(self, stored_pair, new):
        resolved, before = stored_pair
        edited = read_config(resolved).config
        dispose_all()
        edited.datasets[0].remote[0].raw = new
        assert not edited.same_as(before.config)           # the only difference is `raw`
        result = write_config(resolved, edited, expected_generation=before.generation)
        dispose_all()
        assert result.generation == before.generation + 1
        after = read_config(resolved)
        dispose_all()
        assert after.generation == before.generation + 1
        assert after.config.datasets[0].remote[0].raw is new
        assert not after.config.same_as(before.config)
        assert after.config.same_as(edited)

    def test_raw_set_on_the_other_remote_changes_it_too(self, stored_pair):
        resolved, before = stored_pair
        edited = read_config(resolved).config
        dispose_all()
        edited.datasets[0].remote[1].raw = True
        assert not edited.same_as(before.config)
        write_config(resolved, edited, expected_generation=before.generation)
        dispose_all()
        after = read_config(resolved).config
        dispose_all()
        assert [r.raw for r in after.datasets[0].remote] == [True, True]

    def test_an_unchanged_raw_is_the_same_config(self, stored_pair):
        resolved, before = stored_pair
        again = read_config(resolved).config
        dispose_all()
        assert again.same_as(before.config)
        assert again == before.config


# ===========================================================================
# schema stamp bookkeeping
# ===========================================================================


class TestSchemaStamp:
    def test_the_head_and_the_known_revisions_are_the_three_revision_chain(self):
        assert migrate_module.EXPECTED_HEAD == RAW_REVISION
        assert migrate_module.KNOWN_REVISIONS == frozenset(
            {BASELINE_REVISION, TLS_REVISION, RAW_REVISION}
        )

    def test_the_table_set_list_has_an_entry_per_revision_and_the_head_comes_first(self):
        revisions = [rev for rev, _tables in migrate_module._TABLES_AT_REVISION]
        assert revisions == [RAW_REVISION, TLS_REVISION, BASELINE_REVISION]
        assert {rev for rev in revisions} == set(migrate_module.KNOWN_REVISIONS)
        sets = dict(migrate_module._TABLES_AT_REVISION)
        # `raw` is a column, not a table: the TLS and head table sets are the same.
        assert sets[RAW_REVISION] == sets[TLS_REVISION] == frozenset(Base.metadata.tables)

    def test_the_scripts_match_the_constants(self):
        migrate_module._assert_scripts_match_constants()

    def test_an_unstamped_database_with_the_tls_tables_and_no_raw_is_told_to_stamp_the_older_revision(self, old):
        """The head's table set equals the TLS revision's (the head only added a
        column), so the match needs the columns too: a database without `raw` is
        the TLS revision's, never "already at the head". Nothing is changed."""
        resolved, db = old
        drop_stamp(db)
        before = schema_snapshot(db)
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(SchemaSplitBrain) as excinfo:
                check_schema(c)
        message = str(excinfo.value)
        assert not isinstance(excinfo.value, NotAConfigDatabase)
        assert "older schema revision" in message
        assert f"`alembic stamp {TLS_REVISION}`" in message
        assert "`zfsbackup-config migrate`" in message
        assert message.index(f"alembic stamp {TLS_REVISION}") < message.index("zfsbackup-config migrate")
        assert TLS_REVISION in message
        assert "alembic stamp head" not in message
        assert "all of this package's tables already exist" not in message
        assert "Nothing was changed" in message
        assert schema_snapshot(db) == before

    def test_an_unstamped_database_with_the_tls_tables_and_raw_is_told_to_stamp_the_head(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        drop_stamp(db)
        before = schema_snapshot(db)
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(SchemaSplitBrain) as excinfo:
                check_schema(c)
        message = str(excinfo.value)
        assert "all of this package's tables already exist" in message
        assert "alembic stamp head" in message
        assert repr(RAW_REVISION) in message
        assert f"alembic stamp {TLS_REVISION}" not in message
        assert "Nothing was changed" in message
        assert schema_snapshot(db) == before

    def test_following_the_older_revision_advice_ends_with_a_readable_database(self, old):
        resolved, db = old
        drop_stamp(db)
        stamp(db, TLS_REVISION)                              # `alembic stamp 7c3e9d51b2a8`
        result = upgrade_schema(resolved)                    # `zfsbackup-config migrate`
        assert result.changed and result.to_rev == RAW_REVISION
        snapshot = read_config(resolved)
        dispose_all()
        assert snapshot.schema_rev == RAW_REVISION
        assert [r.raw for ds in snapshot.config.datasets for r in ds.remote] == [None, None, None]
        assert table_rows(db, "retention_rules") == TLS_ROWS["retention_rules"]
        assert q(db, "PRAGMA foreign_key_check") == []

    def test_following_the_head_advice_with_the_column_present_is_readable(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        drop_stamp(db)
        stamp(db, RAW_REVISION)                              # `alembic stamp head`
        assert read_config(resolved).schema_rev == RAW_REVISION
        dispose_all()

    def test_the_same_advice_for_a_create_all_database_with_no_stamp(self, tmp_path):
        engine = make_engine(f"sqlite:///{tmp_path / 'noreg.db'}")
        Base.metadata.create_all(engine)
        try:
            with engine.connect() as conn:
                with pytest.raises(SchemaSplitBrain) as excinfo:
                    check_schema(conn)
        finally:
            engine.dispose()
        assert "alembic stamp head" in str(excinfo.value)
        assert repr(RAW_REVISION) in str(excinfo.value)


# ===========================================================================
# a stamp that disagrees with the tables: the column requirement
# ===========================================================================


class TestHeadStampWithoutTheColumn:
    """A database stamped at the head that lacks `dataset_remotes.raw` (an old
    hand-made `alembic stamp head`): `check_schema` refuses it as
    `SchemaColumnsMissing`, a `SchemaError`, instead of saying "up to date" and
    letting every read die with `OperationalError: no such column`."""

    @pytest.fixture
    def broken(self, tmp_path):
        return make_headstamped_without_raw(tmp_path)

    def test_the_fixture_is_the_damage(self, broken):
        _resolved, db = broken
        assert q(db, "SELECT version_num FROM alembic_version") == [(RAW_REVISION,)]
        assert "raw" not in column_names(db, "dataset_remotes")

    def test_check_schema_raises_columns_missing(self, broken):
        resolved, db = broken
        before = schema_snapshot(db)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaColumnsMissing) as excinfo:
                check_schema(conn)
        assert isinstance(excinfo.value, SchemaError)
        assert not isinstance(excinfo.value, OperationalError)
        message = str(excinfo.value)
        assert "stamped at schema revision" in message and repr(RAW_REVISION) in message
        assert "dataset_remotes.raw" in message
        assert f"`alembic stamp {TLS_REVISION}`" in message
        assert "`zfsbackup-config migrate`" in message
        assert message.index(f"alembic stamp {TLS_REVISION}") < message.index("zfsbackup-config migrate")
        assert "Nothing was changed" in message
        assert schema_snapshot(db) == before

    def test_read_config_raises_the_same_error_not_an_operational_error(self, broken):
        resolved, db = broken
        before = schema_snapshot(db)
        with pytest.raises(SchemaColumnsMissing):
            read_config(resolved)
        dispose_all()
        assert schema_snapshot(db) == before

    def test_upgrade_schema_refuses_it_too_and_changes_nothing(self, broken):
        resolved, db = broken
        before = schema_snapshot(db)
        stamp_row = q(db, "SELECT * FROM alembic_version")
        with pytest.raises(SchemaColumnsMissing):
            upgrade_schema(resolved)
        dispose_all()
        assert schema_snapshot(db) == before
        assert q(db, "SELECT * FROM alembic_version") == stamp_row

    def test_following_the_advice_repairs_it(self, broken):
        resolved, db = broken
        stamp(db, TLS_REVISION)
        result = upgrade_schema(resolved)
        assert result.changed and result.from_revs == (TLS_REVISION,) and result.to_rev == RAW_REVISION
        assert "raw" in column_names(db, "dataset_remotes")
        assert read_config(resolved).schema_rev == RAW_REVISION
        dispose_all()

    def test_with_the_column_the_check_passes(self, old):
        resolved, _db = old
        upgrade_schema(resolved)
        with open_config_connection(resolved, readonly=True) as conn:
            assert check_schema(conn) == RAW_REVISION

    def test_a_missing_table_is_not_reported_as_a_missing_column(self, tmp_path):
        # `_missing_columns` owns columns only: a database with no `dataset_remotes`
        # at all is some other damage.
        engine = make_engine("sqlite:///:memory:")
        try:
            with engine.connect() as conn:
                assert migrate_module._missing_columns(conn, RAW_REVISION) == []
        finally:
            engine.dispose()

    def test_missing_columns_reports_adding_revision_table_and_column(self, broken):
        resolved, _db = broken
        with open_config_connection(resolved, readonly=True) as conn:
            assert migrate_module._missing_columns(conn, RAW_REVISION) == [
                (RAW_REVISION, "dataset_remotes", "raw")
            ]
            assert migrate_module._missing_columns(conn, TLS_REVISION) == []
            assert migrate_module._missing_columns(conn, "deadbeef0001") == []


# ===========================================================================
# the helpers, and the guard against the next column-only migration
# ===========================================================================


class TestColumnRequirementHelpers:
    TLS_COLUMNS = (
        (TLS_REVISION, "destinations", "server_fingerprint"),
        (TLS_REVISION, "global_settings", "client_cert_file"),
        (TLS_REVISION, "global_settings", "client_key_file"),
    )

    def test_columns_required_by_the_head_are_its_own_and_every_older_ones(self):
        assert migrate_module._columns_required_by(RAW_REVISION) == (
            (RAW_REVISION, "dataset_remotes", "raw"),
            *self.TLS_COLUMNS,
        )

    def test_the_tls_revision_requires_its_three_columns(self):
        assert migrate_module._columns_required_by(TLS_REVISION) == self.TLS_COLUMNS

    def test_the_baseline_requires_nothing(self):
        assert migrate_module._columns_required_by(BASELINE_REVISION) == ()

    @pytest.mark.parametrize("revision", ["deadbeef0001", "", "head"])
    def test_an_unknown_revision_requires_nothing(self, revision):
        assert migrate_module._columns_required_by(revision) == ()

    def test_the_requirement_is_cumulative_and_newest_first(self, monkeypatch):
        monkeypatch.setattr(migrate_module, "_COLUMNS_ADDED_AT_REVISION", {
            RAW_REVISION: (("dataset_remotes", "raw"),),
            TLS_REVISION: (("destinations", "pin"),),
            BASELINE_REVISION: (("datasets", "note"), ("datasets", "tag")),
        })
        assert migrate_module._columns_required_by(RAW_REVISION) == (
            (RAW_REVISION, "dataset_remotes", "raw"),
            (TLS_REVISION, "destinations", "pin"),
            (BASELINE_REVISION, "datasets", "note"),
            (BASELINE_REVISION, "datasets", "tag"),
        )
        assert migrate_module._columns_required_by(TLS_REVISION) == (
            (TLS_REVISION, "destinations", "pin"),
            (BASELINE_REVISION, "datasets", "note"),
            (BASELINE_REVISION, "datasets", "tag"),
        )
        assert migrate_module._columns_required_by(BASELINE_REVISION) == (
            (BASELINE_REVISION, "datasets", "note"),
            (BASELINE_REVISION, "datasets", "tag"),
        )

    def test_revision_before(self):
        assert migrate_module._revision_before(RAW_REVISION) == TLS_REVISION
        assert migrate_module._revision_before(TLS_REVISION) == BASELINE_REVISION

    def test_stamping_back_is_safe_only_for_a_column_only_revision(self):
        # `raw` came with no new table: stamp the TLS revision and re-migrate.
        assert migrate_module._stamp_back_is_safe(RAW_REVISION) is True
        # The TLS revision also created tables a database like this already holds:
        # replaying it would fail with "table already exists".
        assert migrate_module._stamp_back_is_safe(TLS_REVISION) is False


def _columns_by_revision(tmp_path):
    """`[(revision, {table: {columns}}), ...]` oldest first, taken from a
    database brought up one real migration at a time."""
    from alembic.script import ScriptDirectory

    db_path = tmp_path / "chain" / "config.db"
    resolved = resolve_config_path(str(db_path))
    from zfsbackup.config.store import paths as store_paths

    store_paths.ensure_config_dir(db_path.parent)
    store_paths.create_config_db_file(db_path)
    with open_config_connection(resolved, readonly=False, foreign_keys=False, allow_new=True) as conn:
        script = ScriptDirectory.from_config(migrate_module._build_config(conn))
        chain = [rev.revision for rev in reversed(list(script.walk_revisions()))]
    out = []
    for revision in chain:
        assert alembic_run(resolved, "upgrade", revision) == []
        tables = [r[0] for r in q(db_path, "SELECT name FROM sqlite_master WHERE type='table' "
                                           "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
                                           "AND name != 'alembic_version'")]
        out.append((revision, {t: set(column_names(db_path, t)) for t in tables}))
    return out


class TestEveryColumnOnlyRevisionIsRegistered:
    """GUARD. Every column a revision adds to an existing table must be in
    `_COLUMNS_ADDED_AT_REVISION`, exactly. A column-only revision shares its
    predecessor's table set, so without the entry the unstamped-database advice
    and the head-stamp check silently ignore the column (the bug this file's
    earlier version pinned as "documented"); a table-creating revision needs
    it for the head-stamp check. The next such migration must fail HERE."""

    def test_every_added_column_has_an_entry_and_every_entry_is_real(self, tmp_path):
        history = _columns_by_revision(tmp_path)
        assert [rev for rev, _ in history] == [BASELINE_REVISION, TLS_REVISION, RAW_REVISION]
        registered = migrate_module._COLUMNS_ADDED_AT_REVISION
        for (prev_rev, before), (rev, after) in zip(history, history[1:]):
            added = {
                (table, column)
                for table, columns in after.items() if table in before
                for column in columns - before[table]
            }
            registered_here = set(registered.get(rev, ()))
            assert added == registered_here, (
                f"revision {rev} adds {sorted(added)} to existing tables but "
                f"_COLUMNS_ADDED_AT_REVISION has {sorted(registered_here)}: add the entry "
                f"(see _refuse_unrecorded_tables / check_schema). Required for EVERY "
                f"revision, table-creating ones too: without it a head-stamped database "
                f"that lacks the column passes check_schema"
            )
        # The baseline is the start of the chain: nothing can have been added to it.
        assert BASELINE_REVISION not in registered

    def test_every_known_revision_is_in_the_table_set_list(self):
        listed = {rev for rev, _tables in migrate_module._TABLES_AT_REVISION}
        assert listed == set(migrate_module.KNOWN_REVISIONS), (
            "a revision missing from _TABLES_AT_REVISION is invisible to "
            "_columns_required_by, which then requires nothing of it"
        )
        assert migrate_module._TABLES_AT_REVISION[0][0] == migrate_module.EXPECTED_HEAD

    def test_every_registered_column_is_in_the_models(self):
        for revision, pairs in migrate_module._COLUMNS_ADDED_AT_REVISION.items():
            assert revision in migrate_module.KNOWN_REVISIONS
            for table, column in pairs:
                assert column in Base.metadata.tables[table].columns, (revision, table, column)

    def test_the_head_requirement_covers_every_registered_column(self):
        registered = {
            (table, column)
            for pairs in migrate_module._COLUMNS_ADDED_AT_REVISION.values()
            for table, column in pairs
        }
        required = {(t, c) for _rev, t, c in migrate_module._columns_required_by(RAW_REVISION)}
        assert required == registered


class TestTlsColumnsAreRequiredToo:
    """The TLS revision's columns are registered as well: a head-stamped database
    missing one is refused, but with a different remedy -- that revision also
    created tables, so stamping back and re-running `migrate` would fail with
    "table already exists" and the only way out is a backup."""

    @pytest.fixture
    def no_fingerprint(self, old):
        resolved, db = old
        upgrade_schema(resolved)                 # head, with `raw`
        drop_server_fingerprint(db)
        return resolved, db

    def test_the_fixture_lacks_only_server_fingerprint(self, no_fingerprint):
        resolved, db = no_fingerprint
        assert "server_fingerprint" not in column_names(db, "destinations")
        assert "raw" in column_names(db, "dataset_remotes")
        assert q(db, "SELECT version_num FROM alembic_version") == [(RAW_REVISION,)]
        with open_config_connection(resolved, readonly=True) as conn:
            assert migrate_module._missing_columns(conn, RAW_REVISION) == [
                (TLS_REVISION, "destinations", "server_fingerprint")
            ]

    def test_a_head_stamped_database_missing_it_is_columns_missing_with_the_backup_remedy(self, no_fingerprint):
        resolved, db = no_fingerprint
        before = schema_snapshot(db)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaColumnsMissing) as excinfo:
                check_schema(conn)
        message = str(excinfo.value)
        assert "destinations.server_fingerprint" in message
        assert f"revision {TLS_REVISION!r} adds" in message
        assert "Restore the database from a backup." in message
        assert "alembic stamp" not in message          # replaying the TLS revision cannot work
        assert "zfsbackup-config migrate" not in message
        assert "Nothing was changed" in message
        assert schema_snapshot(db) == before

    def test_read_config_and_upgrade_schema_refuse_it_too(self, no_fingerprint):
        resolved, db = no_fingerprint
        before = schema_snapshot(db)
        for call in (read_config, upgrade_schema):
            with pytest.raises(SchemaColumnsMissing):
                call(resolved)
            dispose_all()
        assert schema_snapshot(db) == before

    def test_several_missing_columns_are_all_listed_and_the_oldest_revision_decides(self, old):
        resolved, db = old
        # Both `raw` and `server_fingerprint` are missing; the OLDEST adding revision
        # decides the remedy, and its tables exist here, so only a backup helps.
        upgrade_schema(resolved)
        drop_server_fingerprint(db)
        conn = sqlite3.connect(str(db))
        try:
            conn.execute("UPDATE alembic_version SET version_num = ?", (RAW_REVISION,))
            conn.execute("CREATE TABLE dataset_remotes_rebuilt AS "
                         "SELECT id, dataset_id, destination_name, frequency_seconds, "
                         "frequency_literal FROM dataset_remotes")
            conn.execute("DROP TABLE dataset_remotes")
            conn.execute("ALTER TABLE dataset_remotes_rebuilt RENAME TO dataset_remotes")
            conn.commit()
        finally:
            conn.close()
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(SchemaColumnsMissing) as excinfo:
                check_schema(c)
        message = str(excinfo.value)
        assert "destinations.server_fingerprint" in message and "dataset_remotes.raw" in message
        assert f"revision {TLS_REVISION!r} adds" in message       # the OLDEST missing one
        assert "Restore the database from a backup." in message

    def test_an_unstamped_database_with_every_table_but_without_the_tls_columns_is_not_ours(self, old):
        resolved, db = old
        drop_server_fingerprint(db)
        drop_stamp(db)
        before = schema_snapshot(db)
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(c)
        assert not isinstance(excinfo.value, SchemaSplitBrain)
        assert "not a zfsbackup config database" in str(excinfo.value)
        assert "nothing was changed" in str(excinfo.value)
        assert schema_snapshot(db) == before

    def test_the_full_tls_columns_without_raw_is_still_the_older_revisions_split_brain(self, old):
        resolved, db = old
        drop_stamp(db)
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(SchemaSplitBrain) as excinfo:
                check_schema(c)
        assert f"`alembic stamp {TLS_REVISION}`" in str(excinfo.value)

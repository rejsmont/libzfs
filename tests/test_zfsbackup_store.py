"""Tests for the SQLAlchemy ORM schema in `zfsbackup/store/models.py` (item 3
of docs/config_db_cli_plan.md).

This is schema-only coverage: `zfsbackup/store/models.py` defines tables and
constraints and is not wired into the daemon, workers, or CLI yet, and there
is no mapper (item 4) or engine/session module (item 5) yet either. Item 9
will extend this file with mapper round-trip and fork-safety coverage once
those land.

**`PRAGMA foreign_keys=ON` is per-connection in SQLite and off by default.**
Item 5's engine setup (which will apply this pragma for real) does not exist
yet, so every fixture here enables it explicitly via a `connect` event
listener. Without it, the cascade/no-cascade tests below would silently pass
without actually exercising cascading behaviour at all.

**Each test gets a fresh in-memory database** (function-scoped `engine`/
`session` fixtures) rather than sharing one across the module. Sharing an
engine let rows accumulate across test cases during development, which made
a genuine `UNIQUE` violation look like a scope-mixing bug -- the fresh-DB
discipline avoids that class of false failure entirely.
"""

import logging
from datetime import timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, delete, event, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Duration,
    RemoteDatasetConfig,
    RemoteServerConfig,
)
from zfsbackup.config import Destination as ConfigDestination
from zfsbackup.config import RetentionRule as ConfigRetentionRule
from zfsbackup.store.mapper import (
    _assert_scope_integrity,
    _dataset_level_rules,
    _dataset_to_dataclass,
    _dedupe_exact_duplicates,
    _duration_from_row,
    _scoped_rules,
    load_config,
    save_config,
)
from zfsbackup.store.migrate import (
    SchemaSplitBrain,
    SchemaVersionMismatch,
    _build_config,
    ensure_schema,
)
from zfsbackup.store.models import (
    Base,
    Dataset,
    DatasetRemote,
    Destination,
    GlobalSettings,
    RemoteServer,
    RetentionRule,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    """A fresh in-memory SQLite engine, per test, with FK enforcement on.

    `StaticPool` + `check_same_thread=False` is the combination item 5's plan
    text calls out for in-memory test engines -- without it, SQLite's default
    per-connection `:memory:` semantics mean a second connection from the
    pool would see an *empty* database, not the one `create_all` populated.
    """
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _enable_foreign_keys(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session(engine):
    with Session(engine) as s:
        yield s


@pytest.fixture
def engine_no_fk():
    """A fresh in-memory SQLite engine with FK enforcement left OFF.

    Only for tests that must deliberately construct a DB state the
    composite FK (`fk_retention_rules_dataset_remote`) would otherwise make
    impossible -- see `_assert_scope_integrity`'s docstring in
    `zfsbackup/store/mapper.py` for why that mapper-side check exists as
    the belt to this braces. SQLite's default is FK-off, so this is simply
    the `engine` fixture minus the `connect` listener.
    """
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session_no_fk(engine_no_fk):
    with Session(engine_no_fk) as s:
        yield s


def make_dataset(session, name="tank/data", **kwargs):
    ds = Dataset(
        name=name,
        frequency_seconds=kwargs.pop("frequency_seconds", 3600),
        frequency_literal=kwargs.pop("frequency_literal", "1h"),
        **kwargs,
    )
    session.add(ds)
    session.commit()
    return ds


def make_destination(session, name="offsite", url="ssh://offsite/pool"):
    dest = Destination(name=name, url=url)
    session.add(dest)
    session.commit()
    return dest


def make_rule(
    dataset_id,
    dataset_remote_id=None,
    age_seconds=3600,
    age_literal="1h",
    keep_for_seconds=86400,
    keep_for_literal="1d",
):
    return RetentionRule(
        dataset_id=dataset_id,
        dataset_remote_id=dataset_remote_id,
        age_seconds=age_seconds,
        age_literal=age_literal,
        keep_for_seconds=keep_for_seconds,
        keep_for_literal=keep_for_literal,
    )


def make_dataset_remote(
    session, dataset_id, destination_name="offsite", frequency_seconds=None,
    frequency_literal=None,
):
    remote = DatasetRemote(
        dataset_id=dataset_id,
        destination_name=destination_name,
        frequency_seconds=frequency_seconds,
        frequency_literal=frequency_literal,
    )
    session.add(remote)
    session.commit()
    return remote


def make_global_settings(**overrides):
    fields = dict(
        check_interval_seconds=60,
        check_interval_literal="1m",
        prune_interval_seconds=3600,
        prune_interval_literal="1h",
        client_id_file="/var/lib/zfsbackup/client_id",
    )
    fields.update(overrides)
    return GlobalSettings(**fields)


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSchemaCreation:
    def test_create_all_succeeds_against_in_memory_sqlite(self, engine):
        # The fixture already ran create_all(); just confirm it produced
        # exactly the tables item 3 specifies -- no more, no fewer.
        inspector = inspect(engine)
        assert set(inspector.get_table_names()) == {
            "global_settings",
            "datasets",
            "destinations",
            "dataset_remotes",
            "retention_rules",
            "remote_server",
        }

    def test_create_all_is_idempotent(self, engine):
        # create_all() must be safe to call again (e.g. daemon startup on an
        # already-initialized DB) without raising "table already exists".
        Base.metadata.create_all(engine)


# ---------------------------------------------------------------------------
# datasets / destinations basic constraints
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatasetsAndDestinations:
    def test_dataset_name_unique(self, session):
        make_dataset(session, name="tank/data")
        session.add(
            Dataset(name="tank/data", frequency_seconds=7200, frequency_literal="2h")
        )
        with pytest.raises(IntegrityError):
            session.commit()

    def test_dataset_name_not_unique_across_different_names(self, session):
        make_dataset(session, name="tank/data")
        session.add(
            Dataset(name="tank/other", frequency_seconds=7200, frequency_literal="2h")
        )
        session.commit()  # must not raise
        assert session.query(Dataset).count() == 2

    def test_destination_name_is_the_primary_key(self, session):
        inspector = inspect(session.bind)
        pk = inspector.get_pk_constraint("destinations")
        assert pk["constrained_columns"] == ["name"]

    def test_duplicate_destination_name_rejected(self, session):
        make_destination(session, name="offsite")
        session.add(Destination(name="offsite", url="ssh://other/pool"))
        with pytest.raises(IntegrityError):
            session.commit()


# ---------------------------------------------------------------------------
# retention_rules CHECK constraints (age_seconds > 0, keep_for_seconds > 0)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRetentionRuleCheckConstraints:
    """Item 2b's data-loss bug (`age: 0` is a divisor at prune time --
    `interval_secs` in `backup_manager.py`, a latent `ZeroDivisionError`) is
    rejected at config-load in the dataclass layer; the schema enforces the
    same invariant independently so both layers stay covered.
    """

    @pytest.fixture
    def dataset_id(self, session):
        return make_dataset(session).id

    @pytest.mark.parametrize("age_seconds", [0, -1, -3600])
    def test_age_seconds_must_be_positive(self, session, dataset_id, age_seconds):
        session.add(make_rule(dataset_id, age_seconds=age_seconds))
        with pytest.raises(IntegrityError):
            session.commit()

    @pytest.mark.parametrize("keep_for_seconds", [0, -1, -86400])
    def test_keep_for_seconds_must_be_positive(
        self, session, dataset_id, keep_for_seconds
    ):
        session.add(make_rule(dataset_id, keep_for_seconds=keep_for_seconds))
        with pytest.raises(IntegrityError):
            session.commit()

    def test_positive_values_are_accepted(self, session, dataset_id):
        session.add(make_rule(dataset_id, age_seconds=1, keep_for_seconds=1))
        session.commit()  # must not raise


# ---------------------------------------------------------------------------
# retention_rules uniqueness -- the NULL-scope regression coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRetentionRuleUniqueness:
    """SQLite treats NULLs as distinct in UNIQUE constraints. A plain
    `UNIQUE(dataset_remote_id, age_seconds)` therefore does NOT constrain
    the dataset-level rows (`dataset_remote_id IS NULL`) at all -- every
    NULL compares unequal to every other NULL, so unlimited duplicate
    dataset-level rules would be silently accepted. That is exactly the
    scope that governs local pruning, so a defect here is the worst place
    for it to be.

    `zfsbackup/store/models.py` closes this with partial unique indexes
    (`sqlite_where=text("dataset_remote_id IS NULL")`) in addition to the
    ordinary `UniqueConstraint`s. The `..._null_scope_...` tests below are
    the ones that actually discriminate between "partial index present" and
    "partial index silently dropped in a future simplification" -- a test
    that only exercised the non-NULL (per-destination) scope would keep
    passing even if those indexes were removed, since the plain
    `UniqueConstraint` already covers that scope on its own. Do not "clean
    up" these NULL-scope cases away.
    """

    @pytest.fixture
    def dataset_id(self, session):
        return make_dataset(session, name="tank/data").id

    @pytest.fixture
    def second_dataset_id(self, session):
        return make_dataset(session, name="tank/other").id

    @pytest.fixture
    def two_destinations(self, session, dataset_id):
        make_destination(session, name="offsite")
        make_destination(session, name="dc2")
        offsite = make_dataset_remote(session, dataset_id, destination_name="offsite")
        dc2 = make_dataset_remote(session, dataset_id, destination_name="dc2")
        return {"offsite": offsite.id, "dc2": dc2.id}

    # -- the discriminating cases: NULL-scope (dataset-level) duplicates --

    def test_duplicate_null_scope_age_rejected(self, session, dataset_id):
        session.add(make_rule(dataset_id, None, age_seconds=3600, keep_for_seconds=86400))
        session.commit()
        session.add(
            make_rule(
                dataset_id, None, age_seconds=3600,
                keep_for_seconds=172800, keep_for_literal="2d",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    def test_duplicate_null_scope_keep_for_rejected(self, session, dataset_id):
        session.add(make_rule(dataset_id, None, age_seconds=3600, keep_for_seconds=86400))
        session.commit()
        session.add(
            make_rule(
                dataset_id, None, age_seconds=7200, age_literal="2h",
                keep_for_seconds=86400,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    # -- the per-destination scope, covered by the plain UniqueConstraint --

    def test_duplicate_destination_scope_age_rejected(
        self, session, dataset_id, two_destinations
    ):
        session.add(
            make_rule(
                dataset_id, two_destinations["offsite"],
                age_seconds=3600, keep_for_seconds=86400,
            )
        )
        session.commit()
        session.add(
            make_rule(
                dataset_id, two_destinations["offsite"], age_seconds=3600,
                keep_for_seconds=172800, keep_for_literal="2d",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    def test_duplicate_destination_scope_keep_for_rejected(
        self, session, dataset_id, two_destinations
    ):
        session.add(
            make_rule(
                dataset_id, two_destinations["offsite"],
                age_seconds=3600, keep_for_seconds=86400,
            )
        )
        session.commit()
        session.add(
            make_rule(
                dataset_id, two_destinations["offsite"], age_seconds=7200, age_literal="2h",
                keep_for_seconds=86400,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    # -- distinct scopes must never collide with each other --

    def test_same_age_null_scope_and_destination_scope_both_accepted(
        self, session, dataset_id, two_destinations
    ):
        # Dataset-level and per-destination rules are independent scopes;
        # a rule at age=1h locally and a *different* rule at age=1h for
        # "offsite" must coexist.
        session.add(
            make_rule(dataset_id, None, age_seconds=3600, keep_for_seconds=86400)
        )
        session.add(
            make_rule(
                dataset_id, two_destinations["offsite"], age_seconds=3600,
                keep_for_seconds=172800, keep_for_literal="2d",
            )
        )
        session.commit()  # must not raise
        assert session.query(RetentionRule).count() == 2

    def test_same_age_two_different_destinations_both_accepted(
        self, session, dataset_id, two_destinations
    ):
        session.add(
            make_rule(
                dataset_id, two_destinations["offsite"],
                age_seconds=3600, keep_for_seconds=86400,
            )
        )
        session.add(
            make_rule(
                dataset_id, two_destinations["dc2"], age_seconds=3600,
                keep_for_seconds=172800, keep_for_literal="2d",
            )
        )
        session.commit()  # must not raise
        assert session.query(RetentionRule).count() == 2

    def test_same_age_different_dataset_accepted(
        self, session, dataset_id, second_dataset_id
    ):
        session.add(
            make_rule(dataset_id, None, age_seconds=3600, keep_for_seconds=86400)
        )
        session.add(
            make_rule(
                second_dataset_id, None, age_seconds=3600,
                keep_for_seconds=172800, keep_for_literal="2d",
            )
        )
        session.commit()  # must not raise
        assert session.query(RetentionRule).count() == 2


# ---------------------------------------------------------------------------
# Cascades
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCascades:
    """Requires PRAGMA foreign_keys=ON (see the `engine` fixture) -- without
    it SQLite ignores ON DELETE clauses entirely and these tests would pass
    for the wrong reason (nothing gets deleted, but nothing errors either).
    """

    def test_deleting_dataset_cascades_retention_rules_and_remotes(self, session):
        dest = make_destination(session, name="offsite")
        ds = make_dataset(session, name="tank/data")
        session.add(make_rule(ds.id, None))
        session.add(
            DatasetRemote(
                dataset_id=ds.id,
                destination_name="offsite",
                frequency_seconds=None,
                frequency_literal=None,
            )
        )
        session.commit()
        assert session.query(RetentionRule).count() == 1
        assert session.query(DatasetRemote).count() == 1

        session.delete(ds)
        session.commit()

        assert session.query(RetentionRule).count() == 0
        assert session.query(DatasetRemote).count() == 0
        # The destination itself is a separate row and must survive.
        assert session.query(Destination).filter_by(name="offsite").count() == 1

    def test_deleting_destination_cascades_its_scoped_rules_only(self, session):
        # Rewritten for the dataset_remote_id scoping (see module docstring
        # and the item-3-review finding-1 bug this schema fixes): a
        # Destination no longer owns retention rows directly, and it can no
        # longer be deleted while a DatasetRemote still references it -- the
        # flow is now two steps, delete the DatasetRemote first (which
        # cascades its scoped rules), then the now-unreferenced Destination.
        make_destination(session, name="offsite")
        ds = make_dataset(session, name="tank/data")
        remote = make_dataset_remote(session, ds.id, destination_name="offsite")
        dataset_level = make_rule(ds.id, None, age_seconds=3600, keep_for_seconds=86400)
        destination_scoped = make_rule(
            ds.id, remote.id, age_seconds=7200, age_literal="2h",
            keep_for_seconds=172800, keep_for_literal="2d",
        )
        session.add_all([dataset_level, destination_scoped])
        session.commit()
        dataset_level_id = dataset_level.id

        # Step 1: deleting the still-referenced Destination is rejected.
        dest = session.query(Destination).filter_by(name="offsite").one()
        session.delete(dest)
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

        # Step 2: delete the DatasetRemote first -- this cascades its scoped
        # retention rule but must leave the dataset-level (NULL-scope) rule,
        # which governs local pruning, untouched.
        remote = session.get(DatasetRemote, remote.id)
        session.delete(remote)
        session.commit()

        remaining = session.query(RetentionRule).all()
        assert [r.id for r in remaining] == [dataset_level_id]
        assert remaining[0].dataset_remote_id is None

        # Step 3: the Destination is now unreferenced and deletes cleanly.
        dest = session.query(Destination).filter_by(name="offsite").one()
        session.delete(dest)
        session.commit()  # must not raise
        assert session.query(Destination).count() == 0


# ---------------------------------------------------------------------------
# Regression coverage for the item-3-review HIGH-severity bug
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatasetRemoteDeletionDoesNotPromoteRules:
    """The bug this class exists to catch: the previous schema scoped
    `RetentionRule` by a nullable `destination_name` FK straight to
    `Destination`, with `passive_deletes=True` on
    `Destination.retention_rules` but no delete cascade on that FK. SQLAlchemy's
    default behaviour for an unloaded-then-loaded, cascade-less collection on
    delete is to *disassociate* children by nulling their FK -- and
    `destination_name IS NULL` is exactly the dataset-level scope that
    governs local pruning. Verified before the fix:

        delete WITHOUT traversing dest.retention_rules -> [(1, None, 3600.0)]
        delete WITH    traversing dest.retention_rules -> [(1, None, 3600.0), (1, None, 7200.0)]

    The `7200` override -- a per-destination retention tier nobody
    configured at the dataset level -- survived the delete as a
    newly-promoted dataset-level rule. The old test in this file passed only
    because it never accessed `dest.retention_rules`, leaving the
    collection unloaded so `passive_deletes` deferred entirely to the DB;
    it passed for the wrong reason.

    The fix (`RetentionRule.dataset_remote_id`, a real FK to
    `dataset_remotes.id` with `ON DELETE CASCADE`, and no FK from
    `RetentionRule` to `Destination` at all) makes the promotion
    structurally unavailable rather than merely untriggered. The test below
    still deliberately forces the same traversal that used to expose the
    bug, so it keeps discriminating "fixed" from "reverted to a bare
    nullable destination_name column" rather than just re-testing the happy
    path the old, misleading test already covered.
    """

    def test_traversing_relationship_before_delete_does_not_promote_rules(
        self, session
    ):
        make_destination(session, name="offsite")
        ds = make_dataset(session, name="tank/data")
        remote = make_dataset_remote(session, ds.id, destination_name="offsite")
        dataset_level = make_rule(ds.id, None, age_seconds=3600, keep_for_seconds=86400)
        destination_scoped = make_rule(
            ds.id, remote.id, age_seconds=7200, age_literal="2h",
            keep_for_seconds=172800, keep_for_literal="2d",
        )
        session.add_all([dataset_level, destination_scoped])
        session.commit()

        dr = session.get(DatasetRemote, remote.id)
        # Deliberate traversal: force the scoped rule into the session
        # before deleting. This access is the entire difference between
        # this test catching the promotion bug and missing it -- do NOT
        # remove it as "redundant" with the DB-level cascade test above,
        # which never loads the collection and would keep passing even if
        # this relationship regressed to the old disassociate-on-delete
        # behaviour.
        _ = dr.retention_rules

        session.delete(dr)
        session.commit()

        # Assert the full surviving tuple set, not a count: a count alone
        # cannot distinguish "the override was deleted" from "the override
        # was promoted to a dataset-level rule", since both leave exactly
        # one row behind.
        surviving = {
            (r.dataset_id, r.dataset_remote_id, r.age_seconds)
            for r in session.query(RetentionRule).all()
        }
        assert surviving == {(ds.id, None, 3600.0)}

    def test_deleting_dataset_remote_cascades_its_scoped_rule(self, session):
        # Standalone version of the cascade half of the above, without the
        # promotion-detecting traversal noise -- covered separately since
        # Part 3 of the plan calls it out on its own.
        make_destination(session, name="offsite")
        ds = make_dataset(session, name="tank/data")
        remote = make_dataset_remote(session, ds.id, destination_name="offsite")
        session.add(make_rule(ds.id, remote.id, age_seconds=3600, keep_for_seconds=86400))
        session.commit()
        assert session.query(RetentionRule).count() == 1

        session.delete(session.get(DatasetRemote, remote.id))
        session.commit()

        assert session.query(RetentionRule).count() == 0


# ---------------------------------------------------------------------------
# New constraints from the item-3 review
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRetentionRuleScopeIntegrity:
    """The composite FK `(dataset_id, dataset_remote_id) ->
    dataset_remotes(dataset_id, id)` (see `RetentionRule`'s docstring)
    closes two states the old bare-`destination_name` column could not
    prevent: an override with no owning `DatasetRemote` row at all, and a
    scoped rule whose `dataset_id` disagrees with its remote's actual
    dataset. Neither is representable in `BackupConfig`, so a mapper
    (item 4) round-tripping either would have to silently drop or
    misattribute the row.
    """

    def test_orphan_dataset_remote_id_rejected(self, session):
        ds = make_dataset(session)
        session.add(
            make_rule(
                ds.id, dataset_remote_id=999999,
                age_seconds=3600, keep_for_seconds=86400,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    def test_scoped_rule_dataset_id_mismatch_rejected(self, session):
        make_destination(session, name="offsite")
        ds1 = make_dataset(session, name="tank/data")
        ds2 = make_dataset(session, name="tank/other")
        remote = make_dataset_remote(session, ds1.id, destination_name="offsite")

        # dataset_remote_id belongs to ds1's DatasetRemote row, but this
        # rule claims dataset_id=ds2 -- the composite FK must reject the
        # mismatch even though dataset_remote_id alone is a valid row.
        session.add(
            make_rule(ds2.id, remote.id, age_seconds=3600, keep_for_seconds=86400)
        )
        with pytest.raises(IntegrityError):
            session.commit()


@pytest.mark.unit
class TestDatasetRemoteUniqueness:
    def test_duplicate_dataset_destination_pair_rejected(self, session):
        make_destination(session, name="offsite")
        ds = make_dataset(session)
        make_dataset_remote(session, ds.id, destination_name="offsite")

        session.add(
            DatasetRemote(
                dataset_id=ds.id,
                destination_name="offsite",
                frequency_seconds=None,
                frequency_literal=None,
            )
        )
        # A second row for the same (dataset_id, destination_name) pair
        # would be silently-dead config: effective_retention_rules and
        # RemoteBackupManager both resolve a destination by first match.
        with pytest.raises(IntegrityError):
            session.commit()


@pytest.mark.unit
class TestNamingConvention:
    """`Base.metadata`'s `naming_convention` must actually name every
    implicitly-named constraint (bare PKs, FKs, `Dataset.name`'s
    column-level `unique=True`) -- an unnamed constraint breaks SQLite's
    `batch_alter_table` when Alembic (item 7) needs to alter it, and that
    failure surfaces far from this file, in a migration, not here. Only a
    light non-empty-name check: the naming convention's exact string
    format is an implementation detail, not something worth pinning.
    """

    def test_primary_and_foreign_keys_are_named(self, engine):
        inspector = inspect(engine)
        for table_name in inspector.get_table_names():
            pk = inspector.get_pk_constraint(table_name)
            if pk.get("constrained_columns"):
                assert pk.get(
                    "name"
                ), f"{table_name} has an unnamed primary key: {pk}"
            for fk in inspector.get_foreign_keys(table_name):
                assert fk.get(
                    "name"
                ), f"{table_name} has an unnamed foreign key: {fk}"
            for uq in inspector.get_unique_constraints(table_name):
                assert uq.get(
                    "name"
                ), f"{table_name} has an unnamed unique constraint: {uq}"


# ---------------------------------------------------------------------------
# dataset_remotes.destination_name -- deliberately NOT cascading
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatasetRemoteDestinationNoCascade:
    """`dataset_remotes.destination_name` has no ON DELETE clause on
    purpose: a destination still referenced by a dataset's remote config
    must not silently vanish when the destination row is deleted. Pinning
    the actual (default SQLite FK) behaviour here: deleting a referenced
    destination is rejected, and both rows survive.
    """

    def test_deleting_referenced_destination_via_orm_is_rejected(self, session):
        dest = make_destination(session, name="offsite")
        ds = make_dataset(session, name="tank/data")
        session.add(
            DatasetRemote(
                dataset_id=ds.id,
                destination_name="offsite",
                frequency_seconds=None,
                frequency_literal=None,
            )
        )
        session.commit()

        session.delete(dest)
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

        assert session.query(Destination).filter_by(name="offsite").count() == 1
        assert session.query(DatasetRemote).count() == 1

    def test_deleting_referenced_destination_via_core_delete_is_rejected(
        self, session
    ):
        # The ORM-level test above triggers SQLAlchemy's own FK-nulling
        # cascade (which fails on the NOT NULL column before the DB's FK
        # check is even reached). Issue a raw DELETE too, to confirm the
        # DB-level constraint itself -- not just ORM relationship
        # management -- is what is actually preventing this.
        make_destination(session, name="offsite")
        ds = make_dataset(session, name="tank/data")
        session.add(
            DatasetRemote(
                dataset_id=ds.id,
                destination_name="offsite",
                frequency_seconds=None,
                frequency_literal=None,
            )
        )
        session.commit()

        with pytest.raises(IntegrityError):
            session.execute(delete(Destination).where(Destination.name == "offsite"))
        session.rollback()

        assert session.query(Destination).filter_by(name="offsite").count() == 1

    def test_deleting_unreferenced_destination_succeeds(self, session):
        make_destination(session, name="unused")
        session.delete(session.query(Destination).filter_by(name="unused").one())
        session.commit()  # must not raise
        assert session.query(Destination).count() == 0


# ---------------------------------------------------------------------------
# Singleton tables
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGlobalSettingsSingleton:
    def test_single_row_accepted(self, session):
        session.add(make_global_settings())
        session.commit()  # must not raise
        assert session.query(GlobalSettings).count() == 1

    def test_second_row_with_id_1_rejected_by_primary_key(self, session):
        session.add(make_global_settings(id=1))
        session.commit()
        session.add(make_global_settings(id=1))
        with pytest.raises(IntegrityError):
            session.commit()

    def test_second_row_with_different_id_rejected_by_check_constraint(self, session):
        session.add(make_global_settings(id=1))
        session.commit()
        session.add(make_global_settings(id=2))
        with pytest.raises(IntegrityError):
            session.commit()

    def test_generation_defaults_to_zero(self, session):
        gs = make_global_settings()
        session.add(gs)
        session.commit()
        session.refresh(gs)
        assert gs.generation == 0


@pytest.mark.unit
class TestGlobalSettingsPruneIntervalNullable:
    """Item 3c.3: `prune_interval_seconds`/`_literal` are nullable, meaning
    "derive from check_interval at read time" (models.py's module
    docstring). The `prune_literal_requires_seconds` CHECK constraint rules
    out the one incoherent combination: a literal on record with no seconds
    value for it to agree or disagree with.
    """

    def test_both_null_is_a_valid_unset_state(self, session):
        session.add(make_global_settings(
            prune_interval_seconds=None, prune_interval_literal=None,
        ))
        session.commit()  # must not raise

        session.expire_all()
        row = session.get(GlobalSettings, 1)
        assert row.prune_interval_seconds is None
        assert row.prune_interval_literal is None

    def test_seconds_present_literal_null_is_valid(self, session):
        # A real value on record with no literal is the ordinary
        # "re-synthesize on read" case, unrelated to the unset marker.
        session.add(make_global_settings(
            prune_interval_seconds=3600, prune_interval_literal=None,
        ))
        session.commit()  # must not raise

    def test_null_seconds_with_literal_rejected_by_check_constraint(self, session):
        session.add(make_global_settings(
            prune_interval_seconds=None, prune_interval_literal="1h",
        ))
        with pytest.raises(IntegrityError):
            session.commit()


@pytest.mark.unit
class TestRemoteServerSingleton:
    def test_single_row_accepted(self, session):
        session.add(RemoteServer(id=1, target_dataset="tank/received"))
        session.commit()  # must not raise
        assert session.query(RemoteServer).count() == 1

    def test_second_row_with_id_1_rejected_by_primary_key(self, session):
        session.add(RemoteServer(id=1, target_dataset="tank/received"))
        session.commit()
        session.add(RemoteServer(id=1, target_dataset="tank/other"))
        with pytest.raises(IntegrityError):
            session.commit()

    def test_second_row_with_different_id_rejected_by_check_constraint(self, session):
        session.add(RemoteServer(id=1, target_dataset="tank/received"))
        session.commit()
        session.add(RemoteServer(id=2, target_dataset="tank/other"))
        with pytest.raises(IntegrityError):
            session.commit()


# ---------------------------------------------------------------------------
# Literal round-tripping
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLiteralRoundTrip:
    """The `_literal` column is load-bearing: item 8 makes the DB canonical
    and regenerates every edit buffer from it, so a dropped or overwritten
    literal would silently rewrite a user's `30d` as `1M` on the next edit.
    Every case here uses a *non-canonical* literal for a value that has a
    shorter canonical form (`30d` rather than `1M`, `365d` rather than `1y`)
    -- a test using only canonical literals would pass even if the column
    were silently dropped and reconstructed from `_seconds` at read time.
    """

    def _reload_dataset(self, session, dataset_id):
        session.expire_all()
        return session.get(Dataset, dataset_id)

    def test_dataset_frequency_literal_round_trips_noncanonical(self, session):
        ds = Dataset(
            name="tank/data",
            frequency_seconds=timedelta(days=30).total_seconds(),
            frequency_literal="30d",
        )
        session.add(ds)
        session.commit()
        dataset_id = ds.id

        reloaded = self._reload_dataset(session, dataset_id)
        assert reloaded.frequency_seconds == timedelta(days=30).total_seconds()
        assert reloaded.frequency_literal == "30d"  # not rewritten to "1M"

    def test_retention_rule_literals_round_trip_noncanonical(self, session):
        ds = make_dataset(session)
        rule = make_rule(
            ds.id,
            None,
            age_seconds=timedelta(days=365).total_seconds(),
            age_literal="365d",
            keep_for_seconds=timedelta(days=30).total_seconds(),
            keep_for_literal="30d",
        )
        session.add(rule)
        session.commit()
        rule_id = rule.id

        session.expire_all()
        reloaded = session.get(RetentionRule, rule_id)
        assert reloaded.age_seconds == timedelta(days=365).total_seconds()
        assert reloaded.age_literal == "365d"  # not rewritten to "1y"
        assert reloaded.keep_for_seconds == timedelta(days=30).total_seconds()
        assert reloaded.keep_for_literal == "30d"  # not rewritten to "1M"

    def test_null_retention_rule_literals_round_trip(self, session):
        # `*_literal` columns are nullable: a NULL means "no literal on
        # record, re-synthesize a best-effort one from `*_seconds` on read"
        # (module docstring) -- not an error state, and not something this
        # schema-only layer resynthesizes itself (that is item 4's mapper).
        # A NULL must round-trip as NULL, not get coerced into "" or raise.
        ds = make_dataset(session)
        rule = make_rule(
            ds.id,
            None,
            age_seconds=1800,
            age_literal=None,
            keep_for_seconds=3600,
            keep_for_literal=None,
        )
        session.add(rule)
        session.commit()
        rule_id = rule.id

        session.expire_all()
        reloaded = session.get(RetentionRule, rule_id)
        assert reloaded.age_seconds == 1800
        assert reloaded.age_literal is None
        assert reloaded.keep_for_seconds == 3600
        assert reloaded.keep_for_literal is None

    def test_global_settings_interval_literals_round_trip_noncanonical(self, session):
        gs = make_global_settings(
            check_interval_seconds=timedelta(days=30).total_seconds(),
            check_interval_literal="30d",
            prune_interval_seconds=timedelta(days=365).total_seconds(),
            prune_interval_literal="365d",
        )
        session.add(gs)
        session.commit()

        session.expire_all()
        reloaded = session.get(GlobalSettings, 1)
        assert reloaded.check_interval_literal == "30d"
        assert reloaded.prune_interval_literal == "365d"

    def test_dataset_remote_frequency_literal_round_trips_noncanonical(self, session):
        make_destination(session, name="offsite")
        ds = make_dataset(session)
        remote = DatasetRemote(
            dataset_id=ds.id,
            destination_name="offsite",
            frequency_seconds=timedelta(days=30).total_seconds(),
            frequency_literal="30d",
        )
        session.add(remote)
        session.commit()
        remote_id = remote.id

        session.expire_all()
        reloaded = session.get(DatasetRemote, remote_id)
        assert reloaded.frequency_seconds == timedelta(days=30).total_seconds()
        assert reloaded.frequency_literal == "30d"


# ---------------------------------------------------------------------------
# dataset_remotes NULL frequency ("inherit")
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatasetRemoteInheritFrequency:
    def test_null_frequency_is_valid_and_means_inherit(self, session):
        make_destination(session, name="offsite")
        ds = make_dataset(session)
        remote = DatasetRemote(
            dataset_id=ds.id,
            destination_name="offsite",
            frequency_seconds=None,
            frequency_literal=None,
        )
        session.add(remote)
        session.commit()  # must not raise

        session.expire_all()
        reloaded = session.get(DatasetRemote, remote.id)
        assert reloaded.frequency_seconds is None
        assert reloaded.frequency_literal is None

    def test_non_null_frequency_overrides_inherit(self, session):
        make_destination(session, name="offsite")
        ds = make_dataset(session)
        remote = DatasetRemote(
            dataset_id=ds.id,
            destination_name="offsite",
            frequency_seconds=14400,
            frequency_literal="4h",
        )
        session.add(remote)
        session.commit()  # must not raise
        assert remote.frequency_seconds == 14400


# ---------------------------------------------------------------------------
# Item 4 -- the mapper layer (zfsbackup/store/mapper.py)
# ---------------------------------------------------------------------------
#
# Selectable via `pytest -k mapper`. Covers `_duration_from_row` (Q2),
# retention scope partitioning (Q4 -- the highest-risk part of the mapper),
# `load_config`'s no-silent-drops table (Q7), `save_config` (Q5/Q6), list
# ordering, and the `==` + literal-projection equivalence contract (Q8).
# See scratchpad item4_plan.md section 4.7 for the matrix this follows.


# `None` in the raw `.literal` projection is ambiguous three ways: a plain
# `timedelta` (no `.literal` attribute at all), a `Duration` with a
# sub-second value (no representable literal in the grammar), and -- since
# item 3c.3 -- `BackupConfig.prune_interval is None` ("unset/derived", not a
# duration value at all). `Duration(...).literal` can never legitimately be
# this sentinel, so substituting it for the "unset" case keeps that state
# distinguishable from the other two `None`s the projection already carries.
_UNSET = object()


def _literal_projection(config: BackupConfig) -> list:
    """Walk a `BackupConfig` and collect every duration's `.literal` (or
    `None` for a plain `timedelta`/inherited value, or `_UNSET` for
    `prune_interval is None`), in the same order `==` would traverse the
    underlying lists.

    Structural `==` is literal-blind (`Duration('30d') == Duration('1M')`
    is `True`), so it alone cannot catch a mapper that reconstructs every
    `Duration` from `*_seconds` and drops every stored `*_literal`. This
    projection is the second half of the equivalence contract -- see Q8 in
    scratchpad item4_plan.md. It must also not collapse "derives from
    check_interval" and "a literal-less stored duration" into the same
    `None` -- see `_UNSET` above -- or the equivalence contract would stop
    distinguishing a config that derives `prune_interval` from one that
    stores a literal-less duration for it, silently, with no test failing.
    """
    literals = [
        getattr(config.check_interval, "literal", None),
        _UNSET if config.prune_interval is None
        else getattr(config.prune_interval, "literal", None),
    ]
    for ds in config.datasets:
        literals.append(getattr(ds.frequency, "literal", None))
        for rule in ds.retention_rules:
            literals.append(getattr(rule.age, "literal", None))
            literals.append(getattr(rule.keep_for, "literal", None))
        for remote in ds.remote:
            literals.append(
                None if remote.frequency is None
                else getattr(remote.frequency, "literal", None)
            )
            for rule in remote.retention_rules:
                literals.append(getattr(rule.age, "literal", None))
                literals.append(getattr(rule.keep_for, "literal", None))
    return literals


def _minimal_backup_config(**dataset_kwargs) -> BackupConfig:
    """One dataset, one dataset-level retention rule -- the minimum shape
    `save_config` can write without raising.
    """
    kwargs = dict(
        name="tank/a",
        frequency=Duration("1h"),
        retention_rules=[ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))],
    )
    kwargs.update(dataset_kwargs)
    return BackupConfig(datasets=[DatasetConfig(**kwargs)])


@pytest.mark.unit
class TestLiteralProjectionUnsetSentinel:
    """`_literal_projection`'s `prune_interval` slot must distinguish three
    states that all reach it as a bare `None` if left unguarded: a plain
    `timedelta` (no `.literal`), a sub-second `Duration` with no
    representable literal, and `prune_interval is None` ("unset/derived",
    since item 3c.3). Only the last gets `_UNSET`; the pin below is that an
    unset config and a literal-less-duration config must project
    differently, even though both durations compare unequal to each other's
    duration too -- the point is `_literal_projection` alone, not `==`.
    """

    def test_unset_prune_interval_projects_as_unset_sentinel(self):
        config = _minimal_backup_config()
        assert config.prune_interval is None
        projection = _literal_projection(config)
        assert projection[1] is _UNSET

    def test_unset_and_literal_less_duration_project_differently(self):
        unset_config = _minimal_backup_config()
        unset_config.prune_interval = None

        literal_less_config = _minimal_backup_config()
        # A sub-second Duration has a real, present value with no
        # representable literal -- `.literal` is None, same as an absent
        # `.literal` attribute, but NOT the same as "unset/derived".
        literal_less_config.prune_interval = Duration(seconds=0.5)

        assert _literal_projection(unset_config)[1] is _UNSET
        assert _literal_projection(literal_less_config)[1] is None
        assert (
            _literal_projection(unset_config)[1]
            != _literal_projection(literal_less_config)[1]
        )


# ---------------------------------------------------------------------------
# 4.1 -- _duration_from_row, all four branches (Q2)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMapperDurationFromRow:
    """The four branches from item4_plan.md Q2. The exact-match and
    literal-None cases are trivial round-trips; the two "literal disagrees
    with seconds" cases are the ones the plan explicitly calls a regression
    class ("reading `*_seconds` alone silently rewrites a user's `30d` as
    `1M`") -- both must discard the stored literal, keep the seconds value,
    and log a WARNING naming the key.
    """

    def test_exact_match_literal_is_preserved(self):
        d = _duration_from_row(2592000.0, "30d", "check_interval")
        assert d.literal == "30d"  # NOT re-synthesized to "1M"
        assert d.total_seconds() == 2592000.0

    def test_literal_none_synthesizes_canonical_form(self):
        d = _duration_from_row(2592000.0, None, "check_interval")
        assert d.literal == "1M"  # synthesized, NOT "30d"
        assert d.total_seconds() == 2592000.0

    def test_literal_none_sub_second_value_has_no_literal(self):
        d = _duration_from_row(0.5, None, "check_interval")
        assert d.literal is None
        assert d.total_seconds() == 0.5

    def test_unparseable_literal_falls_back_to_seconds_and_warns(self, caplog):
        with caplog.at_level(logging.WARNING, logger="zfsbackup.store.mapper"):
            d = _duration_from_row(3600.0, "nonsense", "datasets[tank/a].frequency")
        assert d.total_seconds() == 3600.0
        assert d.literal == "1h"  # re-synthesized, the bad literal is gone
        assert Duration(d.literal) == d
        assert any(
            "datasets[tank/a].frequency" in r.message and "nonsense" in r.message
            for r in caplog.records
        )

    def test_disagreeing_literal_falls_back_to_seconds_and_warns(self, caplog):
        # "2h" parses fine but is 7200s, not the 3600s this row claims.
        with caplog.at_level(logging.WARNING, logger="zfsbackup.store.mapper"):
            d = _duration_from_row(3600.0, "2h", "datasets[tank/a].frequency")
        assert d.total_seconds() == 3600.0
        assert d.literal == "1h"  # seconds wins, "2h" is discarded
        assert Duration(d.literal) == d
        assert any(
            "datasets[tank/a].frequency" in r.message and "2h" in r.message
            for r in caplog.records
        )


# ---------------------------------------------------------------------------
# 4.2 -- the scope filter (Q4). The single highest-value regression test.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMapperScopeFilter:
    """`Dataset.retention_rules` mixes dataset-level rows with every
    per-destination override for that dataset (it joins on `dataset_id`
    alone). A mapper that forgets `if r.dataset_remote_id is None` promotes
    every override into local pruning -- no error, no log, just a dataset
    pruned on tiers nobody configured at the dataset level. This is item
    4's #1 ranked risk; build BOTH scopes, never just one.
    """

    def _seed(self, session):
        session.add(make_global_settings(id=1))
        make_destination(session, name="offsite")
        ds = make_dataset(session, name="tank/data")
        remote = make_dataset_remote(session, ds.id, destination_name="offsite")
        session.add(
            make_rule(
                ds.id, None, age_seconds=3600, age_literal="1h",
                keep_for_seconds=86400, keep_for_literal="1d",
            )
        )
        session.add(
            make_rule(
                ds.id, remote.id, age_seconds=7200, age_literal="2h",
                keep_for_seconds=172800, keep_for_literal="2d",
            )
        )
        session.commit()
        return ds, remote

    def test_load_config_never_promotes_or_drops(self, session):
        self._seed(session)
        config = load_config(session)
        ds_config = config.datasets[0]

        # Never 2 and 0 -- exactly one rule per scope.
        assert len(ds_config.retention_rules) == 1
        assert len(ds_config.remote[0].retention_rules) == 1
        assert ds_config.retention_rules == [
            ConfigRetentionRule(age=Duration("1h"), keep_for=Duration("1d"))
        ]
        assert ds_config.remote[0].retention_rules == [
            ConfigRetentionRule(age=Duration("2h"), keep_for=Duration("2d"))
        ]

    def test_dataset_level_rules_helper_filters_scoped_rows(self, session):
        ds, _ = self._seed(session)
        session.expire_all()
        reloaded = session.get(Dataset, ds.id)
        rules = _dataset_level_rules(reloaded)
        assert len(rules) == 1
        assert rules[0] == ConfigRetentionRule(age=Duration("1h"), keep_for=Duration("1d"))

    def test_scoped_rules_helper_excludes_dataset_level_rows(self, session):
        _, remote = self._seed(session)
        session.expire_all()
        reloaded = session.get(DatasetRemote, remote.id)
        rules = _scoped_rules(reloaded)
        assert len(rules) == 1
        assert rules[0] == ConfigRetentionRule(age=Duration("2h"), keep_for=Duration("2d"))


# ---------------------------------------------------------------------------
# 4.4 -- load_config's Q7 no-silent-drops table
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMapperLoadConfigQ7:
    def test_global_settings_absent_raises(self, session):
        make_dataset(session)  # a dataset alone is not enough
        session.commit()
        with pytest.raises(ValueError, match="uninitialized"):
            load_config(session)

    def test_remote_server_absent_yields_remote_backup_none(self, session):
        session.add(make_global_settings(id=1))
        ds = make_dataset(session)
        session.add(make_rule(ds.id, None))
        session.commit()

        config = load_config(session)
        assert config.remote_backup is None

    def test_remote_server_present_is_not_none(self, session):
        # Deliberately contrasted with the test above: GlobalSettings
        # absent is always an error, RemoteServer absent is not -- a
        # mapper that special-cased the wrong table would fail one of
        # these two and pass the other.
        session.add(make_global_settings(id=1))
        ds = make_dataset(session)
        session.add(make_rule(ds.id, None))
        session.add(RemoteServer(id=1, target_dataset="tank/received", enabled=True))
        session.commit()

        config = load_config(session)
        assert config.remote_backup == RemoteServerConfig(
            target_dataset="tank/received", enabled=True
        )

    def test_zero_datasets_raises(self, session):
        session.add(make_global_settings(id=1))
        session.commit()
        with pytest.raises(ValueError, match="No datasets configured"):
            load_config(session)

    def test_zero_dataset_level_retention_rows_applies_default_and_warns(
        self, session, caplog
    ):
        session.add(make_global_settings(id=1))
        make_dataset(session, name="tank/empty")
        session.commit()

        with caplog.at_level(logging.WARNING, logger="zfsbackup.store.mapper"):
            config = load_config(session)

        assert config.datasets[0].retention_rules == [
            ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
        ]
        assert any("tank/empty" in r.message for r in caplog.records)

    def test_unreferenced_destination_still_appears(self, session):
        session.add(make_global_settings(id=1))
        make_destination(session, name="unused", url="ssh://unused/pool")
        ds = make_dataset(session)
        session.add(make_rule(ds.id, None))
        session.commit()

        config = load_config(session)
        assert config.destinations["unused"] == ConfigDestination(url="ssh://unused/pool")

    def test_scoped_rule_dataset_id_mismatch_raises_with_fk_off(self, session_no_fk):
        # The composite FK normally makes this state impossible, but only
        # with PRAGMA foreign_keys=ON, which item 5 has not yet applied
        # anywhere. Build it directly with FK off to exercise the mapper's
        # own belt-and-braces check.
        session = session_no_fk
        session.add(Destination(name="offsite", url="ssh://offsite/pool"))
        ds1 = Dataset(name="tank/a", frequency_seconds=3600, frequency_literal="1h")
        ds2 = Dataset(name="tank/b", frequency_seconds=3600, frequency_literal="1h")
        session.add_all([ds1, ds2])
        session.commit()
        remote = DatasetRemote(
            dataset_id=ds1.id, destination_name="offsite",
            frequency_seconds=None, frequency_literal=None,
        )
        session.add(remote)
        session.commit()

        # dataset_remote_id belongs to ds1's remote, but dataset_id claims
        # ds2 -- rejected by the composite FK when it's on; here it isn't.
        bad_rule = RetentionRule(
            dataset_id=ds2.id, dataset_remote_id=remote.id,
            age_seconds=3600, age_literal="1h",
            keep_for_seconds=86400, keep_for_literal="1d",
        )
        session.add(bad_rule)
        session.commit()  # only succeeds because FK enforcement is off here

        session.expire_all()
        reloaded_ds2 = session.get(Dataset, ds2.id)
        with pytest.raises(ValueError, match="dataset_remote_id"):
            _assert_scope_integrity(reloaded_ds2)

        with pytest.raises(ValueError):
            _dataset_to_dataclass(reloaded_ds2, {"offsite": ConfigDestination(url="x")})

    def test_undeclared_destination_reference_raises_with_fk_off(self, session_no_fk):
        # Also normally impossible with the FK on (destination_name FKs to
        # destinations.name); re-checked anyway per config.py:715-728.
        session = session_no_fk
        session.add(make_global_settings(id=1))
        ds = make_dataset(session, name="tank/a")
        session.add(make_rule(ds.id, None))
        session.add(
            DatasetRemote(
                dataset_id=ds.id, destination_name="ghost",
                frequency_seconds=None, frequency_literal=None,
            )
        )
        session.commit()

        with pytest.raises(ValueError, match="ghost"):
            load_config(session)


@pytest.mark.unit
class TestMapperPruneIntervalClientIdFileUnset:
    """Item 3c.3: `GlobalSettings.prune_interval_seconds`/`_literal` and
    `.client_id_file` are nullable specifically so `BackupConfig`'s "derive
    this elsewhere" state (`prune_interval is None` /
    `client_id_file is None`) is representable in the DB, rather than
    flattened into a concrete value by whichever process happens to write
    it (see models.py's module docstring for the `sudo`/`$HOME` bug this
    fixes). `load_config`/`save_config` must pass `NULL` through as `None`
    on both sides without ever deriving a value themselves.
    """

    def test_save_then_load_both_none_round_trips_as_null(self, session, mocker):
        from zfsbackup.store import mapper as mapper_module

        real_duration_from_row = mapper_module._duration_from_row
        spy = mocker.patch.object(
            mapper_module, "_duration_from_row", side_effect=real_duration_from_row
        )

        config = _minimal_backup_config()
        assert config.prune_interval is None
        assert config.client_id_file is None

        save_config(session, config)
        session.commit()

        # The row itself: NULL, not a materialised value derived from
        # whichever process ran save_config.
        session.expire_all()
        row = session.get(GlobalSettings, 1)
        assert row.prune_interval_seconds is None
        assert row.prune_interval_literal is None
        assert row.client_id_file is None

        spy.reset_mock()
        loaded = load_config(session)
        assert loaded.prune_interval is None
        assert loaded.client_id_file is None

        # _duration_from_row must never be reached for prune_interval --
        # reaching it with seconds=None would raise, and reaching it at all
        # would mean load_config tried to synthesize a value instead of
        # passing NULL through as None.
        prune_interval_calls = [
            call for call in spy.call_args_list if call.args[2] == "prune_interval"
        ]
        assert prune_interval_calls == []
        # check_interval is never nullable, so the spy must still have run
        # at least once -- otherwise this assertion would be vacuous.
        assert spy.call_count >= 1

    def test_save_then_load_explicit_values_round_trip(self, session, tmp_path):
        # Contrast case: explicit, non-None values must NOT round-trip as
        # NULL -- only the deliberate None/derive state does.
        config = _minimal_backup_config()
        config.prune_interval = Duration("2h")
        config.client_id_file = tmp_path / "client_id"

        save_config(session, config)
        session.commit()

        session.expire_all()
        row = session.get(GlobalSettings, 1)
        assert row.prune_interval_seconds == 7200
        assert row.prune_interval_literal == "2h"
        assert row.client_id_file == str(tmp_path / "client_id")

        loaded = load_config(session)
        assert loaded.prune_interval == Duration("2h")
        assert loaded.client_id_file == tmp_path / "client_id"

    def test_save_then_load_zero_prune_interval_is_not_treated_as_unset(self, session):
        # Duration("0m") and its stored seconds value (0.0) are both falsy
        # in Python, but "configured to zero" is a real, present value --
        # distinct from `prune_interval is None` ("unset/derive"). Both
        # save_config (mapper.py's `prune_interval_seconds=`/`_literal=`)
        # and load_config (mapper.py's `prune_interval=`) must branch on
        # `is None`, not truthiness: a truthiness-based guard on either
        # side would silently fold a configured "0m" into "derive from
        # check_interval", returning check_interval's seconds (e.g. 300)
        # from effective_prune_interval instead of 0 -- breaking
        # `load_config(save_config(cfg)) == cfg` with nothing going red.
        config = _minimal_backup_config()
        config.prune_interval = Duration("0m")
        assert config.prune_interval is not None  # sanity: falsy, not None

        save_config(session, config)
        session.commit()

        session.expire_all()
        row = session.get(GlobalSettings, 1)
        assert row.prune_interval_seconds == 0  # NOT NULL
        assert row.prune_interval_literal == "0m"

        loaded = load_config(session)
        assert loaded.prune_interval is not None
        assert loaded.prune_interval.total_seconds() == 0
        assert loaded.effective_prune_interval.total_seconds() == 0


# ---------------------------------------------------------------------------
# 4.5 -- save_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMapperSaveConfig:
    def _two_dest_config(self) -> BackupConfig:
        return BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/a",
                    frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                    remote=[RemoteDatasetConfig(destination="offsite")],
                ),
            ],
            destinations={"offsite": ConfigDestination(url="ssh://offsite/pool")},
        )

    def test_repeat_save_does_not_raise_on_delete_order(self, session):
        # Regression for the delete-order dependency (Q6): datasets must be
        # deleted before destinations, since dataset_remotes.destination_name
        # has no ON DELETE clause (deliberate RESTRICT). The first save
        # against an empty DB can't discriminate order (every DELETE is a
        # no-op); the second save, against a now-populated DB, can.
        config = self._two_dest_config()
        save_config(session, config)
        session.commit()

        save_config(session, config)  # must not raise IntegrityError
        session.commit()

        loaded = load_config(session)
        assert loaded.destinations["offsite"] == ConfigDestination(url="ssh://offsite/pool")

    def test_exact_duplicate_retention_rules_are_deduped(self, session):
        config = _minimal_backup_config(
            retention_rules=[
                ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d")),
                ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d")),
            ]
        )
        save_config(session, config)
        session.commit()
        assert session.query(RetentionRule).count() == 1

    def test_genuine_retention_collision_raises(self, session):
        # Seed a good config FIRST and assert it survives the failed call --
        # a bare `assert session.query(Dataset).count() == 0` against a
        # fresh DB (the original form of this test) is vacuous, because the
        # count is already 0 before `save_config` ever runs; it cannot tell
        # "validated before the DELETE" from "validated after". See
        # `test_undeclared_destination_raises_before_any_delete` above for
        # the same pattern, which this test now mirrors for the retention
        # half of the invariant.
        good = _minimal_backup_config(name="tank/good")
        save_config(session, good)
        session.commit()

        config = _minimal_backup_config(
            name="tank/bad",
            retention_rules=[
                ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d")),
                ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("60d")),
            ]
        )
        with pytest.raises(ValueError):
            save_config(session, config)

        session.expire_all()
        surviving = session.query(Dataset).all()
        assert [d.name for d in surviving] == ["tank/good"]

    def test_undeclared_destination_raises_before_any_delete(self, session):
        good = _minimal_backup_config(name="tank/a")
        save_config(session, good)
        session.commit()

        bad = _minimal_backup_config(
            name="tank/b", remote=[RemoteDatasetConfig(destination="ghost")]
        )
        with pytest.raises(ValueError, match="ghost"):
            save_config(session, bad)

        # The pre-existing good config must survive the failed call
        # untouched -- validation runs before the first DELETE.
        session.expire_all()
        surviving = session.query(Dataset).all()
        assert [d.name for d in surviving] == ["tank/a"]

    def test_generation_increments_and_does_not_reset_on_existing_db(self, session):
        config = _minimal_backup_config()

        save_config(session, config)
        session.commit()
        assert session.get(GlobalSettings, 1).generation == 0

        save_config(session, config)
        session.commit()
        session.expire_all()
        assert session.get(GlobalSettings, 1).generation == 1

        save_config(session, config)
        session.commit()
        session.expire_all()
        assert session.get(GlobalSettings, 1).generation == 2

    def test_plain_timedelta_backup_config_round_trips_without_attributeerror(
        self, session, sample_backup_config
    ):
        # `getattr(d, 'literal', None)` is what makes this work: a plain
        # `timedelta` (tests/conftest.py's sample_backup_config fixture)
        # has no `.literal` attribute at all.
        save_config(session, sample_backup_config)
        session.commit()  # must not raise AttributeError

        loaded = load_config(session)
        assert loaded.datasets[0].name == "pool/data"
        # No literal was on record, so it's re-synthesized from the seconds
        # value on read -- 3600s -> "1h".
        assert loaded.datasets[0].frequency.literal == "1h"


# ---------------------------------------------------------------------------
# List ordering -- surrogate id order, not alphabetical
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMapperListOrdering:
    def test_datasets_preserve_insertion_order(self, session):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name=name, frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                )
                for name in ["zeta", "alpha", "mike"]
            ]
        )
        save_config(session, config)
        session.commit()

        loaded = load_config(session)
        assert [d.name for d in loaded.datasets] == ["zeta", "alpha", "mike"]

    def test_remote_entries_preserve_insertion_order(self, session):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/a", frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                    remote=[
                        RemoteDatasetConfig(destination="zzz"),
                        RemoteDatasetConfig(destination="aaa"),
                    ],
                ),
            ],
            destinations={
                "zzz": ConfigDestination(url="ssh://zzz/pool"),
                "aaa": ConfigDestination(url="ssh://aaa/pool"),
            },
        )
        save_config(session, config)
        session.commit()

        loaded = load_config(session)
        assert [r.destination for r in loaded.datasets[0].remote] == ["zzz", "aaa"]


# ---------------------------------------------------------------------------
# Q8 -- the equivalence contract (== plus a literal projection)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMapperEquivalenceContract:
    """`load_config(session_from(save_config(config))) == config` is the
    mapper's headline property, but `==` alone is literal-blind (Q8):
    `Duration('30d') == Duration('1M')` is `True`, so a mapper that dropped
    every stored literal and rebuilt from `*_seconds` would pass `==` on
    every case here. Both assertions in each test below are load-bearing.

    Neither shipped reference YAML has any `destinations`/`remote` entries,
    so neither alone exercises item 3b (per-destination overrides) at all
    -- the synthetic fixture below is what stands in for item 8's YAML
    importer, which does not exist yet.
    """

    @pytest.fixture
    def synthetic_yaml(self, tmp_path):
        # Deliberately covers all four remote shapes the plan calls out:
        # tank/apps->offsite inherits both frequency and retention;
        # tank/db->offsite overrides frequency only; tank/db->dc2 overrides
        # retention only (inherits frequency). Every literal below is
        # non-canonical ("30d" not "1M", "3650d" not "10y") so the
        # projection assertion is meaningful, not vacuously true.
        content = """
snapshot_prefix: "custom"
check_interval: "5m"
prune_interval: "30d"
api_host: "0.0.0.0"
api_port: 9090
dry_run: true
client_id_file: "/var/lib/zfsbackup/client_id"

destinations:
  offsite:
    url: "http://offsite.example.com:8080"
  dc2:
    url: "http://dc2.example.com:8080"

datasets:
  - name: "tank/apps"
    frequency: "30d"
    retention:
      "1d": "30d"
      "1w": "3M"
    remote:
      - destination: offsite

  - name: "tank/db"
    frequency: "15m"
    retention:
      "1h": "1d"
      "1d": "3650d"
    remote:
      - destination: offsite
        frequency: "4h"
      - destination: dc2
        retention:
          "1h": "6h"

remote_backup:
  target_dataset: "tank/received"
  enabled: true
"""
        p = tmp_path / "synthetic.yaml"
        p.write_text(content)
        return p

    def test_synthetic_config_with_destinations_and_overrides_round_trips(
        self, session, synthetic_yaml
    ):
        expected = BackupConfig.from_file(synthetic_yaml)
        save_config(session, expected)
        session.commit()

        loaded = load_config(session)

        assert loaded == expected
        assert _literal_projection(loaded) == _literal_projection(expected)

    @pytest.mark.parametrize("yaml_name", ["config.example.yaml", "config.test.yaml"])
    def test_reference_yaml_round_trips(self, session, yaml_name):
        # Neither reference config has destinations/remote entries -- they
        # do not exercise the scope-filter half of the contract at all; see
        # the synthetic test above for that.
        #
        # They also do NOT meaningfully exercise the literal-preservation
        # half, despite appearances: every literal in both files ("1M",
        # "10y", "15m", ...) is already canonical for its value --
        # `Duration._synthesize`'s greedy largest-unit-first decomposition
        # reconstructs the exact same string from `*_seconds` alone. The
        # `_literal_projection` assertion below is therefore vacuous here: a
        # mapper mutated to always rebuild every `Duration` from
        # `*_seconds` (dropping every stored `*_literal`) still passes both
        # parametrizations of this test, verified by mutation. Only
        # `TestMapperEquivalenceContract::test_synthetic_config_with_destinations_and_overrides_round_trips`
        # (whose fixture deliberately uses non-canonical literals such as
        # "30d" where "1M" is the shorter form) carries the literal half of
        # the contract. This test remains valid coverage of the *structural*
        # round-trip against real, shipped config files -- keep it for that.
        yaml_path = Path(__file__).resolve().parent.parent / "zfsbackup" / yaml_name
        expected = BackupConfig.from_file(yaml_path)
        save_config(session, expected)
        session.commit()

        loaded = load_config(session)

        assert loaded == expected
        assert _literal_projection(loaded) == _literal_projection(expected)


# ---------------------------------------------------------------------------
# item-3-review-pass fixes (item 9 regression coverage)
#
# Findings 1 and 2 (TestMapperWipeReinsertRegression,
# TestMapperScopedRulesForeignDatasetRegression) live under `PRAGMA
# foreign_keys=OFF`, which is SQLite's default and which nothing outside
# this file's fixtures turns on until item 5 (engine/session setup) lands.
# Their fix-pinning tests run against `session_no_fk` / `engine_no_fk`
# deliberately -- against the FK-on `session` fixture the DB's own
# cascade/FK behaviour papers over the bug and the test would pass for the
# wrong reason (or pass either way), pinning nothing.
#
# Findings 3 and 4 (TestMapperSaveConfigStep1ValidationRegression) are NOT
# FK-dependent -- the defects they pin are a plain-Python duplicate check,
# `UniqueConstraint`/`CHECK`/`NOT NULL` violations, and an empty-list guard,
# none of which involve `dataset_remote_id`/cascade behaviour at all. Their
# tests are fixture-agnostic and correctly use the ordinary FK-on `session`
# fixture; do not move them onto `session_no_fk`.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMapperWipeReinsertRegression:
    """Finding 1 (HIGH): `save_config`'s wipe step used to be a bulk
    `delete(Dataset)` / `delete(Destination)`, relying on `ON DELETE
    CASCADE` to take `dataset_remotes`/`retention_rules` down with it. A
    bulk `delete()` issued through the ORM never applies relationship
    cascades -- only the DB's own `ON DELETE CASCADE` does that, and only
    when `PRAGMA foreign_keys=ON`. With the pragma off, the child rows
    survived the wipe and the next `save_config` call's freshly-inserted
    dataset got handed the SAME rowid (SQLite reuses the lowest available
    rowid once a table is emptied) -- silently re-adopting the previous
    config's stale retention tiers and remote overrides into an unrelated
    dataset, with no error and no log.

    The fix deletes every child table explicitly, in dependency order
    (`retention_rules -> dataset_remotes -> datasets -> destinations ->
    global_settings -> remote_server`), which is correct whether the pragma
    is on or off -- see `save_config`'s docstring, step 4.
    """

    def test_second_save_does_not_re_adopt_first_saves_orphans(self, session_no_fk):
        session = session_no_fk
        cfg1 = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/a",
                    frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                    remote=[
                        RemoteDatasetConfig(
                            destination="offsite",
                            retention_rules=[
                                ConfigRetentionRule(age=Duration("1h"), keep_for=Duration("7d"))
                            ],
                        ),
                    ],
                ),
            ],
            destinations={"offsite": ConfigDestination(url="ssh://offsite/pool")},
        )
        save_config(session, cfg1)
        session.commit()

        # cfg2 reuses none of cfg1's shape: a different dataset name, a
        # different (dataset-level-only) retention tier, and no remotes or
        # destinations at all -- so anything cfg1-shaped that survives into
        # the reload is unambiguously an orphan re-adoption, not a
        # coincidence of matching values.
        cfg2 = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/b",
                    frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("2d"), keep_for=Duration("60d"))
                    ],
                ),
            ],
        )
        save_config(session, cfg2)
        session.commit()

        loaded = load_config(session)

        assert [d.name for d in loaded.datasets] == ["tank/b"]
        assert loaded.datasets[0].retention_rules == [
            ConfigRetentionRule(age=Duration("2d"), keep_for=Duration("60d"))
        ]
        # No re-adopted "offsite" remote or its 1h->7d override.
        assert loaded.datasets[0].remote == []
        # No re-adopted stale destination either -- cfg2 declared none.
        assert loaded.destinations == {}


@pytest.mark.unit
class TestMapperScopedRulesForeignDatasetRegression:
    """Finding 2: `DatasetRemote.retention_rules` (`models.py`) joins on
    `dataset_remote_id` alone via a narrowed `primaryjoin`, and never
    re-checks the row's own `dataset_id`. A row whose `dataset_remote_id`
    correctly names a real remote but whose `dataset_id` names NO dataset
    at all is therefore invisible to `Dataset.retention_rules`'s ordinary
    join (which matches on `dataset_id`, so a nonexistent `dataset_id`
    never appears in any real dataset's collection) -- before the fix, such
    a row sailed straight through `_scoped_rules` and `load_config` silently
    returned it as a genuine override of the remote's owning dataset.

    `_assert_scope_integrity`'s SECOND loop (walking `ds.remotes[*]
    .retention_rules`, not just `ds.retention_rules`) is what closes this --
    see that function's docstring for why two independent loops are
    required, not one.
    """

    def test_scoped_rules_rejects_rule_owned_by_nonexistent_dataset(
        self, session_no_fk
    ):
        session = session_no_fk
        session.add(make_global_settings(id=1))
        session.add(Destination(name="offsite", url="ssh://offsite/pool"))
        ds = Dataset(name="tank/a", frequency_seconds=3600, frequency_literal="1h")
        session.add(ds)
        session.commit()
        session.add(
            make_rule(
                ds.id, None, age_seconds=86400, age_literal="1d",
                keep_for_seconds=2592000, keep_for_literal="30d",
            )
        )
        remote = DatasetRemote(
            dataset_id=ds.id, destination_name="offsite",
            frequency_seconds=None, frequency_literal=None,
        )
        session.add(remote)
        session.commit()

        # dataset_remote_id correctly names ds's own "offsite" remote, but
        # dataset_id=999 names no dataset at all.
        orphan_rule = RetentionRule(
            dataset_id=999, dataset_remote_id=remote.id,
            age_seconds=3600, age_literal="1h",
            keep_for_seconds=604800, keep_for_literal="7d",
        )
        session.add(orphan_rule)
        session.commit()  # only succeeds because FK enforcement is off here

        session.expire_all()
        reloaded = session.get(Dataset, ds.id)

        # Direction 2 specifically: the mismatch is only visible by walking
        # ds.remotes[*].retention_rules, since ds.retention_rules (joined on
        # dataset_id alone) never contains this row at all -- dataset_id=999
        # matches no real dataset, including this one.
        with pytest.raises(ValueError, match="dataset_id"):
            _assert_scope_integrity(reloaded)

        with pytest.raises(ValueError):
            load_config(session)


@pytest.mark.unit
class TestMapperSaveConfigStep1ValidationRegression:
    """Finding 3 (plus one step-1 check that landed after the original four,
    `remote_backup.target_dataset`): each input below previously either
    reached the DB as a bare `IntegrityError` / `CHECK constraint failed` /
    `NOT NULL constraint failed`, raised from inside the insert loop with
    every `DELETE` already flushed, or -- for the empty-string cases --
    inserted silently with no error at all. `save_config` now validates
    every one of them before any `DELETE` runs and raises a named
    `ValueError` instead.

    Every case here asserts BOTH the named raise AND that a pre-seeded good
    config survives the failed call untouched. Asserting only the raise (as
    `test_genuine_retention_collision_raises` originally did -- finding 5)
    cannot distinguish "validated before the DELETE" from "validated
    after"; "cannot wipe a good DB" is the actual contract under test.
    """

    @pytest.fixture(autouse=True)
    def _seed_good_config(self, session):
        good = _minimal_backup_config(name="tank/good")
        save_config(session, good)
        session.commit()

    def _assert_good_survives(self, session):
        session.expire_all()
        surviving = session.query(Dataset).all()
        assert [d.name for d in surviving] == ["tank/good"]

    def test_duplicate_dataset_name_raises_before_delete(self, session):
        bad = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/dup", frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                ),
                DatasetConfig(
                    name="tank/dup", frequency=Duration("2h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("2d"), keep_for=Duration("60d"))
                    ],
                ),
            ]
        )
        with pytest.raises(ValueError, match="duplicate dataset name"):
            save_config(session, bad)
        self._assert_good_survives(session)

    def test_duplicate_remote_destination_on_one_dataset_raises_before_delete(
        self, session
    ):
        # `DatasetConfig.from_dict` (`config.py:624-626`) never checks this
        # -- a YAML `from_file` accepts it cleanly and only fails at insert,
        # against `dataset_remotes`'s composite unique constraint, as a bare
        # IntegrityError. `save_config` must catch it before the wipe since
        # a direct `BackupConfig` (bypassing `from_dict`, e.g. a future CLI
        # importer) is never guaranteed to pass through that check.
        bad = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/bad",
                    frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                    remote=[
                        RemoteDatasetConfig(destination="offsite"),
                        RemoteDatasetConfig(destination="offsite", frequency=Duration("4h")),
                    ],
                ),
            ],
            destinations={"offsite": ConfigDestination(url="ssh://offsite/pool")},
        )
        with pytest.raises(ValueError, match="duplicate"):
            save_config(session, bad)
        self._assert_good_survives(session)

    @pytest.mark.parametrize("field_name", ["age", "keep_for"])
    def test_non_positive_dataset_level_retention_raises_before_delete(
        self, session, field_name
    ):
        rule_kwargs = {"age": Duration("1d"), "keep_for": Duration("30d")}
        rule_kwargs[field_name] = timedelta(seconds=0)
        bad = _minimal_backup_config(
            name="tank/bad", retention_rules=[ConfigRetentionRule(**rule_kwargs)]
        )
        with pytest.raises(ValueError, match="must be positive"):
            save_config(session, bad)
        self._assert_good_survives(session)

    def test_non_positive_remote_scoped_retention_raises_before_delete(self, session):
        # The dataset-level scope in this config is valid on its own -- only
        # the per-destination override is non-positive, covering the "at
        # either scope" half `_assert_positive_duration_rule` mirrors for
        # remote-scoped rules specifically (not just dataset-level ones).
        bad = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/bad",
                    frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                    remote=[
                        RemoteDatasetConfig(
                            destination="offsite",
                            retention_rules=[
                                ConfigRetentionRule(
                                    age=timedelta(seconds=-1), keep_for=Duration("7d")
                                )
                            ],
                        ),
                    ],
                ),
            ],
            destinations={"offsite": ConfigDestination(url="ssh://offsite/pool")},
        )
        with pytest.raises(ValueError, match="must be positive"):
            save_config(session, bad)
        self._assert_good_survives(session)

    @pytest.mark.parametrize("bad_url", [None, ""])
    def test_destination_missing_url_raises_before_delete(self, session, bad_url):
        bad = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/bad",
                    frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                ),
            ],
            destinations={"offsite": ConfigDestination(url=bad_url)},
        )
        with pytest.raises(ValueError, match="requires 'url'"):
            save_config(session, bad)
        self._assert_good_survives(session)

    @pytest.mark.parametrize("bad_target_dataset", [None, ""])
    def test_remote_backup_missing_target_dataset_raises_before_delete(
        self, session, bad_target_dataset
    ):
        # Mirrors the `Destination.url` case above (`config.py:710-712`) for
        # `remote_backup.target_dataset` (`config.py:735-737`). The two
        # params matter for different reasons: `None` would fail anyway, as
        # a bare `NOT NULL constraint failed` from `remote_server
        # .target_dataset` (`models.py`), but loudly and late, after the
        # wipe. `""` is the one that actually mattered before this check
        # existed -- the NOT NULL column happily accepts an empty string,
        # so it inserted silently and `load_config` loaded it back as
        # `target_dataset=''`, a config `BackupConfig.from_file` can never
        # produce (`config.py:735-737` rejects an empty value at the YAML
        # boundary too). `not target_dataset` rejects both the same way,
        # before any DELETE runs.
        bad = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/bad",
                    frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                ),
            ],
            remote_backup=RemoteServerConfig(target_dataset=bad_target_dataset),
        )
        with pytest.raises(ValueError, match="requires 'target_dataset'"):
            save_config(session, bad)
        self._assert_good_survives(session)

    def test_empty_datasets_raises_before_delete(self, session):
        # Finding 4: `save_config(session, BackupConfig(datasets=[]))` used
        # to succeed and leave a store `load_config` refuses to read at all
        # ("No datasets configured"). It now raises up front instead.
        #
        # Deliberately NOT covered here (and must not be "fixed" later as a
        # bug): a `DatasetConfig` with `retention_rules=[]` still writes
        # zero retention rows for that dataset and still loads back as the
        # 1d->30d default with a WARNING (`_dataset_level_rules`). That
        # asymmetry is the user's binding decision -- refusing an
        # empty-retention dataset is the CLI's job later, not this store
        # layer's.
        with pytest.raises(ValueError, match="No datasets configured"):
            save_config(session, BackupConfig(datasets=[]))
        self._assert_good_survives(session)


@pytest.mark.unit
class TestMapperDetachedSafety:
    """Flagged by the reviewer as unpinned: `load_config` builds every
    nested dataclass eagerly, before it returns (module docstring), so the
    resulting `BackupConfig` must stay fully usable -- including every
    nested retention rule and remote -- long after the session that
    produced it is closed. This is structurally guaranteed today because
    everything returned is a plain `@dataclass`, but that guarantee is the
    entire reason the two-layer architecture (`store/__init__.py`'s
    docstring) exists, so it is pinned directly here rather than left to
    hold by accident.
    """

    def test_returned_config_is_fully_usable_after_session_close(self, engine):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="tank/a",
                    frequency=Duration("1h"),
                    retention_rules=[
                        ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
                    ],
                    remote=[
                        RemoteDatasetConfig(
                            destination="offsite",
                            frequency=Duration("4h"),
                            retention_rules=[
                                ConfigRetentionRule(age=Duration("1h"), keep_for=Duration("7d"))
                            ],
                        ),
                    ],
                ),
            ],
            destinations={"offsite": ConfigDestination(url="ssh://offsite/pool")},
            remote_backup=RemoteServerConfig(target_dataset="tank/received"),
        )

        with Session(engine) as session:
            save_config(session, config)
            session.commit()
            loaded = load_config(session)
        # `session` is closed now (the `with` block's __exit__) -- every
        # attribute access below must be a plain dataclass access, not an
        # ORM lazy-load, or this raises DetachedInstanceError.

        assert loaded.datasets[0].name == "tank/a"
        assert loaded.datasets[0].retention_rules == [
            ConfigRetentionRule(age=Duration("1d"), keep_for=Duration("30d"))
        ]
        assert loaded.datasets[0].remote[0].destination == "offsite"
        assert loaded.datasets[0].remote[0].frequency == Duration("4h")
        assert loaded.datasets[0].remote[0].retention_rules == [
            ConfigRetentionRule(age=Duration("1h"), keep_for=Duration("7d"))
        ]
        assert loaded.destinations["offsite"] == ConfigDestination(
            url="ssh://offsite/pool"
        )
        assert loaded.remote_backup == RemoteServerConfig(target_dataset="tank/received")


# ---------------------------------------------------------------------------
# Migrations (item 7: zfsbackup/store/migrations/, zfsbackup/store/migrate.py)
# ---------------------------------------------------------------------------


def _schema_projection(bind):
    """Order-insensitive, name-keyed reflection of a SQLite schema's
    columns, primary key, foreign keys, unique constraints, check
    constraints, and indexes -- one entry per table, `alembic_version`
    excluded.

    Deliberately NOT a `sqlite_master.sql` text compare: the item-7 plan
    measured that `upgrade head` and `Base.metadata.create_all` emit
    byte-different `CREATE TABLE` SQL for two tables in this schema, purely
    from constraint-clause *reordering* -- the constraint sets are
    identical, only their textual order differs. A raw string diff would
    fail on both DDL paths this suite needs to agree, for no real
    difference. Reflecting via `Inspector` and sorting every constraint
    list makes that ordering irrelevant.

    Two reflected values are stringified before comparison, because
    neither defines a value-based `__eq__`:

    - `get_indexes()`'s `dialect_options['sqlite_where']` is a `TextClause`;
      two instances wrapping the identical SQL default-`repr()` to
      different memory addresses, so leaving them as objects produces a
      spurious failure on a schema that is actually identical -- the exact
      trap the plan called out.
    - `get_columns()`'s `'type'` is a `TypeEngine` instance (e.g.
      `INTEGER()`); SQLAlchemy does not give these an `__eq__` either, so
      two separately-constructed instances of the same type compare
      unequal by identity.
    """
    insp = inspect(bind)
    projection = {}
    for table in sorted(insp.get_table_names()):
        if table == "alembic_version":
            continue

        columns = sorted(
            (col["name"], str(col["type"]), col["nullable"], bool(col["primary_key"]))
            for col in insp.get_columns(table)
        )

        pk = insp.get_pk_constraint(table)
        pk_projection = (
            pk.get("name"),
            tuple(sorted(pk.get("constrained_columns") or [])),
        )

        foreign_keys = sorted(
            (
                fk["name"],
                tuple(fk["constrained_columns"]),
                fk["referred_table"],
                tuple(fk["referred_columns"]),
                (fk.get("options") or {}).get("ondelete"),
            )
            for fk in insp.get_foreign_keys(table)
        )

        unique_constraints = sorted(
            (uq["name"], tuple(sorted(uq["column_names"])))
            for uq in insp.get_unique_constraints(table)
        )

        check_constraints = sorted(
            (ck["name"], ck["sqltext"]) for ck in insp.get_check_constraints(table)
        )

        indexes = []
        for ix in insp.get_indexes(table):
            where = (ix.get("dialect_options") or {}).get("sqlite_where")
            indexes.append(
                (
                    ix["name"],
                    tuple(ix["column_names"]),
                    bool(ix["unique"]),
                    str(where) if where is not None else None,
                )
            )
        indexes.sort()

        projection[table] = {
            "columns": columns,
            "pk": pk_projection,
            "foreign_keys": foreign_keys,
            "unique_constraints": unique_constraints,
            "check_constraints": check_constraints,
            "indexes": indexes,
        }
    return projection


def _code_head(engine):
    """This installed package's Alembic head revision id, resolved via
    `ScriptDirectory` alone -- reads the migration scripts on disk, never
    the database, so it does not require (and does not perform) any
    migration against `engine`. Used by tests that need to assert a
    `SchemaSplitBrain`/`SchemaVersionMismatch` message names the right
    revision without disturbing the database state under test to get it.
    """
    with engine.connect() as conn:
        return ScriptDirectory.from_config(_build_config(conn)).get_heads()[0]


@pytest.mark.unit
class TestMigrations:
    """Coverage for `zfsbackup/store/migrate.py` and
    `zfsbackup/store/migrations/` (item 7).

    Two DDL paths coexist in this codebase: `Base.metadata.create_all`
    (every fixture above this class) and `alembic upgrade head`
    (production, via `ensure_schema`). Assertion 1
    (`test_upgrade_head_matches_create_all`) is the only thing keeping
    those two paths honest with each other -- it must never be deleted in
    favour of assertion 2 alone.

    `Config` objects here are built via `zfsbackup.store.migrate.
    _build_config` -- the same private helper `ensure_schema` itself calls
    -- rather than by reading `alembic.ini`, which `ensure_schema` never
    does. Using anything else (a hand-rolled `Config()`, or one that reads
    the repo's `alembic.ini`) would test a code path production never
    takes.
    """

    # -- assertion 1 / 1b: upgrade head == create_all -----------------

    def test_upgrade_head_matches_create_all(self, tmp_path):
        head_db = tmp_path / "head.db"
        create_all_db = tmp_path / "create_all.db"

        head_engine = create_engine(f"sqlite:///{head_db}")
        with head_engine.connect() as conn:
            ensure_schema(conn)
            conn.commit()

        create_all_engine = create_engine(f"sqlite:///{create_all_db}")
        Base.metadata.create_all(create_all_engine)

        try:
            assert _schema_projection(head_engine) == _schema_projection(
                create_all_engine
            )
        finally:
            head_engine.dispose()
            create_all_engine.dispose()

    def test_upgrade_head_spot_checks(self, tmp_path):
        """Three targeted checks so a failure names the culprit directly,
        rather than only reporting "the two dicts differ somewhere" from
        the previous, broader test.
        """
        db_path = tmp_path / "spot_check.db"
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            with engine.connect() as conn:
                ensure_schema(conn)
                conn.commit()

            projection = _schema_projection(engine)

            partial_index_names = {
                "uq_retention_rules_dataset_age_null_remote",
                "uq_retention_rules_dataset_keep_for_null_remote",
            }
            found_partial = {
                name: where
                for name, _cols, _unique, where in projection["retention_rules"][
                    "indexes"
                ]
                if name in partial_index_names
            }
            assert found_partial == {
                "uq_retention_rules_dataset_age_null_remote": "dataset_remote_id IS NULL",
                "uq_retention_rules_dataset_keep_for_null_remote": "dataset_remote_id IS NULL",
            }

            check_constraints = dict(projection["global_settings"]["check_constraints"])
            assert (
                check_constraints["ck_global_settings_prune_literal_requires_seconds"]
                == "prune_interval_seconds IS NOT NULL OR prune_interval_literal IS NULL"
            )

            composite_fks = [
                fk
                for fk in projection["retention_rules"]["foreign_keys"]
                if fk[0] == "fk_retention_rules_dataset_remote"
            ]
            assert composite_fks == [
                (
                    "fk_retention_rules_dataset_remote",
                    ("dataset_id", "dataset_remote_id"),
                    "dataset_remotes",
                    ("dataset_id", "id"),
                    "CASCADE",
                )
            ]
        finally:
            engine.dispose()

    # -- assertion 2: no-drift guard -----------------------------------

    def test_upgrade_head_has_no_autogenerate_drift(self, tmp_path):
        """`compare_metadata` reports zero diffs between a migrated DB and
        `Base.metadata` -- i.e. a fresh `alembic revision --autogenerate`
        against this DB would produce an empty `upgrade()`.

        Does NOT replace `test_upgrade_head_matches_create_all`:
        `compare_metadata` does not compare CHECK constraints at all (a
        `CheckConstraint` dropped from `models.py`'s `__table_args__` but
        left in the migration would report zero diffs here), which is
        exactly why the reflected-projection test above is kept alongside
        this one rather than being replaced by it.
        """
        db_path = tmp_path / "no_drift.db"
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            with engine.connect() as conn:
                ensure_schema(conn)
                conn.commit()

            with engine.connect() as conn:
                migration_context = MigrationContext.configure(conn)
                diff = compare_metadata(migration_context, Base.metadata)
            assert diff == []
        finally:
            engine.dispose()

    # -- assertion 3 & 4: downgrade base, and the upgrade/downgrade/upgrade round trip --

    def test_downgrade_base_leaves_only_alembic_version(self, tmp_path):
        db_path = tmp_path / "downgrade.db"
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            with engine.connect() as conn:
                ensure_schema(conn)
                conn.commit()

            with engine.connect() as conn:
                command.downgrade(_build_config(conn), "base")
                conn.commit()

            insp = inspect(engine)
            assert insp.get_table_names() == ["alembic_version"]
        finally:
            engine.dispose()

    def test_upgrade_downgrade_upgrade_round_trip_reproduces_schema(self, tmp_path):
        """Exercises the generated `downgrade()` body for real: nothing
        else in this suite ever runs it. `upgrade head` -> `downgrade
        base` -> `upgrade head` must reproduce the same reflected
        projection as a single `upgrade head` (assertion 1's projection).
        """
        db_path = tmp_path / "round_trip.db"
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            with engine.connect() as conn:
                ensure_schema(conn)
                conn.commit()
            first_projection = _schema_projection(engine)

            with engine.connect() as conn:
                command.downgrade(_build_config(conn), "base")
                conn.commit()
            insp = inspect(engine)
            assert insp.get_table_names() == ["alembic_version"]

            with engine.connect() as conn:
                ensure_schema(conn)
                conn.commit()
            second_projection = _schema_projection(engine)

            assert second_projection == first_projection
        finally:
            engine.dispose()

    # -- assertions 5-7: in-memory, exercised through ensure_schema() ----
    #
    # These three physically cannot pass unless `env.py`'s
    # `run_migrations_online()` honours `config.attributes["connection"]`
    # ahead of building its own engine from a URL (env.py's module
    # docstring, point 1) -- an in-memory `:memory:` database is not
    # reachable by URL at all from a second connection without it. They
    # therefore double as the acceptance test for that requirement.

    @pytest.fixture
    def empty_memory_engine(self):
        """A fresh, empty (no `create_all`) in-memory SQLite engine.

        `StaticPool` + `check_same_thread=False` for the same reason the
        module-level `engine` fixture above uses it: without it, a second
        connection drawn from the pool sees an empty, distinct
        `:memory:` database rather than the one the first connection
        already migrated.
        """
        eng = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        yield eng
        eng.dispose()

    def test_ensure_schema_raises_on_unrecognised_revision(self, empty_memory_engine):
        with empty_memory_engine.connect() as conn:
            code_head = ensure_schema(conn)
            conn.commit()

        with empty_memory_engine.begin() as conn:
            conn.execute(
                text("UPDATE alembic_version SET version_num='deadbeefcafe'")
            )

        with empty_memory_engine.connect() as conn:
            with pytest.raises(SchemaVersionMismatch) as excinfo:
                ensure_schema(conn)

        message = str(excinfo.value)
        assert "deadbeefcafe" in message
        assert code_head in message
        # `SchemaVersionMismatch` wraps the underlying Alembic error via
        # `raise ... from`, so the original CommandError is still reachable.
        assert excinfo.value.__cause__ is not None
        assert "deadbeefcafe" in str(excinfo.value.__cause__)

    def test_ensure_schema_is_idempotent(self, empty_memory_engine):
        with empty_memory_engine.connect() as conn:
            first_head = ensure_schema(conn)
            conn.commit()

        with empty_memory_engine.connect() as conn:
            second_head = ensure_schema(conn)
            conn.commit()

        assert second_head == first_head

        with empty_memory_engine.connect() as conn:
            assert not conn.closed
            third_head = ensure_schema(conn)
            assert third_head == first_head
            assert not conn.closed

    def test_ensure_schema_never_commits_the_callers_pending_work(
        self, empty_memory_engine
    ):
        """Pins the actual "never commits ... the caller owns the
        transaction" contract from `ensure_schema`'s docstring, via real
        database state rather than a mock.

        This replaces an earlier version of this test that spied on
        `Connection.commit` (`patch.object(type(conn), "commit")`) and was
        close to vacuous for two reasons a review caught: (1) that spy only
        sees `Connection.commit()` itself -- Alembic's own
        `begin_transaction()` path can reach `Connection._commit_impl()`
        through a `_ProxyTransaction`/`RootTransaction` object without ever
        calling `Connection.commit()`, so a mutant that committed the
        caller's work through a transaction object would still have passed
        the spy; and (2) the spy was only exercised on a third call against
        an already-current DB, where `command.upgrade` runs no migrations
        at all -- nothing was ever going to commit there, in any
        implementation, mutated or not. Verified directly: adding an
        explicit `connection.commit()` to the end of `ensure_schema` is
        caught by this test (the caller's row survives the rollback below)
        but was not caught by the old spy-based version on its own second
        call (the spy's call site).

        Scoped to `ensure_schema`'s *second*, idempotent (already-at-head)
        call rather than its first (schema-creating) call: on a fresh,
        schema-less database, how a caller's already-open transaction
        interacts with the first call's DDL/DML is a separate, real hazard
        the store team is addressing independently (a corrected `migrate.py`
        docstring and a named error for the resulting split-brain state),
        and is expected to change out from under this file -- this test
        should not pin exact pre-fix mechanics there. The already-at-head
        no-op path is stable regardless of how that fix lands: a database
        already at head runs no DDL at all on a second `ensure_schema`
        call, so "does this disturb a caller's already-pending write" has
        one well-defined answer independent of that other, unrelated fix.
        """
        with empty_memory_engine.connect() as conn:
            ensure_schema(conn)
            conn.commit()

        with empty_memory_engine.connect() as conn:
            # Caller-side write: a real domain row, not a scratch table --
            # possible here only because the schema above is already
            # committed. Left uncommitted deliberately.
            conn.execute(
                text(
                    "INSERT INTO global_settings "
                    "(id, snapshot_prefix, check_interval_seconds, "
                    "api_host, api_port, dry_run, generation) "
                    "VALUES (1, 'autosnap', 3600.0, '127.0.0.1', 8080, 0, 0)"
                )
            )
            assert (
                conn.execute(text("SELECT COUNT(*) FROM global_settings")).scalar()
                == 1
            )

            ensure_schema(conn)  # idempotent no-op: already at head

            # Still uncommitted after ensure_schema returns: it must not
            # have committed the caller's pending write on its behalf.
            assert not conn.closed
            assert (
                conn.execute(text("SELECT COUNT(*) FROM global_settings")).scalar()
                == 1
            )

            conn.rollback()

            # The caller's own uncommitted write is gone...
            assert (
                conn.execute(text("SELECT COUNT(*) FROM global_settings")).scalar()
                == 0
            )
            # ...but the schema itself -- already committed before this
            # block began -- is untouched by rolling back a transaction
            # that only ever contained the caller's own pending INSERT.
            insp = inspect(empty_memory_engine)
            assert "global_settings" in insp.get_table_names()

    @pytest.fixture
    def mapper_logger_state(self):
        """Snapshot and restore `zfsbackup.store.mapper`'s `level` and
        `disabled` attributes around a test.

        Both are mutable global `logging` state, not per-test state --
        several tests below run a real `logging.config.fileConfig()` (via
        Alembic, positive and negative) against this exact logger, and
        several *other* tests elsewhere in this file assert against it via
        `caplog`. An earlier version of these tests restored `disabled` in
        one case and nothing at all in another, leaking mutated state into
        the rest of the pytest session regardless of which test ran first.
        """
        logger = logging.getLogger("zfsbackup.store.mapper")
        original_level = logger.level
        original_disabled = logger.disabled
        yield logger
        logger.level = original_level
        logger.disabled = original_disabled

    def test_ensure_schema_does_not_disable_application_logging(
        self, empty_memory_engine, mapper_logger_state
    ):
        """Guards against the stock `alembic init` template's
        `logging.config.fileConfig()` default of `disable_existing_loggers=
        True`. Measured directly on this repo's own `zfsbackup.store.
        mapper` logger, which several other tests in this file rely on via
        `caplog`.
        """
        mapper_logger_state.disabled = False
        mapper_logger_state.setLevel(logging.DEBUG)

        with empty_memory_engine.connect() as conn:
            ensure_schema(conn)
            conn.commit()

        assert mapper_logger_state.disabled is False
        assert mapper_logger_state.isEnabledFor(logging.WARNING) is True

    def test_configure_logger_guard_reachable_via_real_ini(
        self, empty_memory_engine, mapper_logger_state
    ):
        """`_build_config` (the helper `ensure_schema` itself uses) never
        sets `config_file_name` -- `Config()` is built with no `file_`
        argument -- so `env.py`'s `config.config_file_name is not None and
        ...` guard is *never reached* from `ensure_schema`'s own call path;
        the two `ensure_schema`-level tests in this class pass regardless
        of whether the trailing `config.attributes.get("configure_logger",
        True)` half of that `and` is present at all, because the leading
        `config_file_name is not None` half is already `False` for them.

        The `configure_logger` half only has an observable effect for a
        caller that *does* supply a config file -- the repo's own
        `alembic.ini`, exactly as a bare `alembic` CLI invocation would --
        which is what this test builds directly, matching env.py's own
        module docstring (point 2) and 7.1(d)'s stated requirement.

        Both halves of the contract are pinned here, not just the
        opt-out's happy path: `configure_logger=False` must protect the
        logger (first block), AND omitting it entirely -- the bare
        `alembic` CLI's own default -- must actually reproduce the hazard
        the guard exists for (second block). Without the second half, this
        test is a tautology about a flag nothing currently sets: it would
        pass identically whether `fileConfig` does anything at all when
        `configure_logger` is left unset, and would not have told anyone
        the guard's *absence* is actually harmful. This repo's own
        `alembic.ini` `[loggers]` section declares only `root, sqlalchemy,
        alembic`, so `fileConfig`'s default `disable_existing_loggers=True`
        disables every other already-configured logger, including this
        one, when nothing opts out.
        """
        mapper_logger_state.disabled = False
        mapper_logger_state.setLevel(logging.DEBUG)

        ini_path = Path(__file__).resolve().parent.parent / "alembic.ini"
        assert ini_path.is_file()
        # Note: building a `Config` from this real ini and running it
        # through `command.upgrade` causes `ScriptDirectory.from_config` to
        # honour `alembic.ini`'s `prepend_sys_path = .`, which permanently
        # prepends `'.'` to `sys.path` for the rest of the interpreter --
        # an unreverted global mutation, same class of leak as the logger
        # state this fixture restores. Harmless today (pytest's CWD is the
        # repo root, already importable), so not worth guarding here, but
        # worth flagging rather than leaving implicit.

        # Opt-out present: the logger must survive.
        with empty_memory_engine.connect() as conn:
            cfg = Config(str(ini_path))
            cfg.attributes["connection"] = conn
            cfg.attributes["configure_logger"] = False
            command.upgrade(cfg, "head")
            conn.commit()

        assert mapper_logger_state.disabled is False
        assert mapper_logger_state.isEnabledFor(logging.WARNING) is True

        # Opt-out absent (the bare `alembic` CLI's own default): the same
        # ini-driven run must actually reproduce the hazard `env.py`'s
        # `configure_logger` guard exists to prevent -- otherwise the
        # block above is not proving the guard does anything.
        mapper_logger_state.disabled = False
        mapper_logger_state.setLevel(logging.DEBUG)

        with empty_memory_engine.connect() as conn:
            cfg = Config(str(ini_path))
            cfg.attributes["connection"] = conn
            # No `configure_logger` attribute set at all.
            command.upgrade(cfg, "head")  # already at head: DDL-free no-op
            conn.commit()

        assert mapper_logger_state.disabled is True

    # -- SchemaSplitBrain -------------------------------------------------

    def test_ensure_schema_raises_schema_split_brain_via_create_all(
        self, empty_memory_engine
    ):
        """Primary reproduction of the split-brain guard: build all six
        application tables directly via `Base.metadata.create_all`,
        bypassing Alembic entirely, so `alembic_version` never exists at
        all. Preferred over a transaction-mechanics-dependent repro (see
        the two tests below) because it does not depend on any pysqlite/
        SQLAlchemy/Alembic transaction-boundary behaviour -- just the
        `_has_existing_schema` table-name check `ensure_schema` runs
        before attempting `command.upgrade`.

        `ensure_schema` must detect this and raise `SchemaSplitBrain`
        naming the code's head revision, rather than attempting
        `command.upgrade` and failing with a bare, unrecoverable
        `OperationalError: table ... already exists` on every retry.
        """
        Base.metadata.create_all(empty_memory_engine)
        code_head = _code_head(empty_memory_engine)

        with empty_memory_engine.connect() as conn:
            with pytest.raises(SchemaSplitBrain) as excinfo:
                ensure_schema(conn)

        assert code_head in str(excinfo.value)

    def test_ensure_schema_raises_schema_split_brain_via_rollback(
        self, empty_memory_engine
    ):
        """A second, more "realistic" way to reach the same split-brain
        state as the test above -- not by bypassing Alembic, but by
        hitting the documented transaction hazard directly (see
        `test_ensure_schema_first_call_on_clean_connection_leaves_split_
        brain_on_rollback` below, which this setup is shared with): a
        clean-connection `ensure_schema` call followed by a rollback
        leaves the six application tables committed but `alembic_version`
        empty. A second `ensure_schema` call against that same database
        must then raise `SchemaSplitBrain`, naming the code's head
        revision, rather than attempting `command.upgrade` again.
        """
        code_head = _code_head(empty_memory_engine)

        with empty_memory_engine.connect() as conn:
            ensure_schema(conn)
            conn.rollback()

        with empty_memory_engine.connect() as conn:
            with pytest.raises(SchemaSplitBrain) as excinfo:
                ensure_schema(conn)

        assert code_head in str(excinfo.value)

    # -- the transaction-boundary hazard (migrate.py's module docstring) --
    #
    # These two characterize documented, verified pysqlite/SQLAlchemy/
    # Alembic mechanics -- not a decision this package's own code makes.
    # They are pinned so that a future SQLAlchemy or Alembic upgrade that
    # silently changes this transaction behaviour fails loudly here first,
    # reading as "the platform changed" rather than "our code broke".
    # Neither asserts `conn.in_transaction()` as the thing under test --
    # each asserts observable database state after a rollback, the same
    # pattern `test_ensure_schema_never_commits_the_callers_pending_work`
    # already established.

    def test_ensure_schema_first_call_inside_callers_open_transaction_loses_everything_on_rollback(
        self, empty_memory_engine
    ):
        """Mechanism 1: when a caller already has an open transaction (any
        prior DML on the same connection) at the time `ensure_schema`
        performs the *first*, schema-creating migration, Alembic's DDL
        runs inside that ambient transaction rather than opening its own.
        A rollback triggered by something with nothing to do with the
        migration itself (a bad YAML file, an `IntegrityError` from
        `save_config`, an operator `^C`) then discards not just the
        caller's own pending write but the entire migration: all six
        application tables AND `alembic_version` are gone. Measured: only
        the caller's own scratch table survives (its `CREATE TABLE` ran --
        and autocommitted -- before any transaction was open at all).

        This is exactly the scenario `migrate.py`'s module docstring
        warns callers away from: never call `ensure_schema` on a
        connection that already has pending work on it.
        """
        with empty_memory_engine.connect() as conn:
            conn.execute(text("CREATE TABLE caller_marker (id INTEGER)"))
            conn.execute(text("INSERT INTO caller_marker (id) VALUES (1)"))
            ensure_schema(conn)
            conn.rollback()

        insp = inspect(empty_memory_engine)
        assert set(insp.get_table_names()) == {"caller_marker"}

    def test_ensure_schema_first_call_on_clean_connection_leaves_split_brain_on_rollback(
        self, empty_memory_engine
    ):
        """Mechanism 2: the one item 8's importer will actually hit, since
        it calls `ensure_schema` on a freshly opened connection with no
        prior work. The six `CREATE TABLE` statements autocommit
        individually (no ambient transaction is open yet), but the final
        `INSERT INTO alembic_version` does open one -- the same open
        transaction `test_ensure_schema_never_commits_the_callers_pending_
        work` already pins as still open when `ensure_schema` returns. A
        rollback -- or simply never calling `commit()` at all, which is
        what an operator's `^C` between `ensure_schema` and the caller's
        own commit produces -- discards only that one `INSERT`, leaving
        exactly the split-brain state `SchemaSplitBrain` exists to detect
        on any subsequent call: full schema, empty `alembic_version`.
        """
        with empty_memory_engine.connect() as conn:
            ensure_schema(conn)
            conn.rollback()

        insp = inspect(empty_memory_engine)
        expected_tables = {
            "datasets",
            "destinations",
            "global_settings",
            "remote_server",
            "dataset_remotes",
            "retention_rules",
            "alembic_version",
        }
        assert expected_tables <= set(insp.get_table_names())

        with empty_memory_engine.connect() as conn:
            row_count = conn.execute(
                text("SELECT COUNT(*) FROM alembic_version")
            ).scalar()
        assert row_count == 0

    # -- happy path: commit immediately, as the docstring now instructs --

    def test_committing_immediately_after_ensure_schema_stamps_alembic_version_at_head(
        self, empty_memory_engine
    ):
        """The one thing `migrate.py`'s docstring now tells every caller
        to actually do -- call `ensure_schema`, then `commit()`
        immediately, before anything else touches the connection. Nothing
        else in this class pins that doing so produces a correctly
        stamped `alembic_version` row (as opposed to merely "did not
        raise") -- this is that pin.
        """
        with empty_memory_engine.connect() as conn:
            code_head = ensure_schema(conn)
            conn.commit()

        with empty_memory_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).fetchall()

        assert rows == [(code_head,)]

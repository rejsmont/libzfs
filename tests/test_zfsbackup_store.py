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

from datetime import timedelta

import pytest
from sqlalchemy import create_engine, delete, event, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

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

"""Tests for the SQLAlchemy ORM schema in `zfsbackup/config/store/models.py` (item 3
of docs/config_db_cli_plan.md), the mapper (item 4), migrations (item 7),
and the engine/session module (item 5, `zfsbackup/config/store/db.py`).

This is store-layer coverage: `zfsbackup/config/store/` defines the schema, the
dataclass<->ORM mapper, Alembic migrations, and engine/session management,
and is not wired into the daemon, workers, or CLI yet.

**Every fixture below goes through `zfsbackup.config.store.db`** (`make_engine`,
`get_engine`, `session_for_engine`, `session_scope`) rather than hand-rolling
`create_engine`/pragma listeners. `foreign_keys` is a real, named parameter
on `make_engine`, so `engine_no_fk`/`session_no_fk` below are simply
`make_engine(url, foreign_keys=False)` -- that fixture is **not** a
workaround for item 5's absence (item 5 has landed). It is the fixture for a
mode that still exists after item 5: Alembic's SQLite `batch_alter_table`
migrations must run FK-off (see `db.py`'s note for item 8's `ensure_schema`
wrapper), and the FK-off regressions pinned by
`TestMapperWipeReinsertRegression` /
`TestMapperScopedRulesForeignDatasetRegression` below would pass for the
wrong reason -- or not discriminate anything at all -- under an FK-on
fixture, since the DB's own cascade/FK behaviour would paper over exactly
the bug those tests exist to catch.

**Each test gets a fresh in-memory database** (function-scoped `engine`/
`session` fixtures) rather than sharing one across the module. Sharing an
engine let rows accumulate across test cases during development, which made
a genuine `UNIQUE` violation look like a scope-mixing bug -- the fresh-DB
discipline avoids that class of false failure entirely.

**`TestDb*` classes near the end of this file are item 5's own coverage**
(`pytest -k "TestDbEngine or TestDbFork or TestDbWal or TestDbReadOnly or
TestDbCrash or TestDbDispose or TestDbPragma or TestDbPool"`), including a
fork-safety test pinned to `multiprocessing.get_context("fork")` and its
negative control -- see that section's module docstring for why the
positive test alone would pass vacuously on this machine (Python 3.14 on
darwin, `spawn`-default).
"""

import json
import logging
import multiprocessing
import os
import sqlite3
import threading
import time
from datetime import timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, delete, event, inspect, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import QueuePool, SingletonThreadPool, StaticPool

from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Duration,
    RemoteDatasetConfig,
    RemoteServerConfig,
)
from zfsbackup.config import Destination as ConfigDestination
from zfsbackup.config import RetentionRule as ConfigRetentionRule
from zfsbackup.config.store import db as store_db
from zfsbackup.config.store.db import (
    ReadOnlySessionError,
    _is_memory_url,
    dispose_all,
    get_engine,
    make_engine,
    session_for_engine,
    session_scope,
    url_for_path,
)
from zfsbackup.config.store.mapper import (
    _assert_scope_integrity,
    _dataset_level_rules,
    _dataset_to_dataclass,
    _dedupe_exact_duplicates,
    _duration_from_row,
    _scoped_rules,
    load_config,
    save_config,
)
from zfsbackup.config.store.migrate import (
    SchemaSplitBrain,
    SchemaVersionMismatch,
    _build_config,
    ensure_schema,
)
from zfsbackup.config.store.models import (
    Base,
    Dataset,
    DatasetRemote,
    Destination,
    GlobalSettings,
    RemoteServer,
    RetentionRule,
)

# `multiprocessing`'s default start method is platform- and version-
# dependent (spawn on this darwin/3.14 machine), so every fork-safety test
# below pins `multiprocessing.get_context("fork")` explicitly rather than
# relying on the default -- an unpinned fork test would pass here while
# testing nothing (`db.py`'s module docstring). Skip cleanly, never error,
# where "fork" is not an available start method at all.
FORK_AVAILABLE = "fork" in multiprocessing.get_all_start_methods()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    """A fresh in-memory SQLite engine, per test, with FK enforcement on.

    Built via `make_engine` (item 5) rather than a hand-rolled
    `create_engine`/pragma listener -- `StaticPool` + `check_same_thread=
    False` for `:memory:` URLs, and `PRAGMA foreign_keys=ON`, are both
    `db.py`'s job now. Without `StaticPool`, SQLite's default
    per-connection `:memory:` semantics mean a second connection from the
    pool would see an *empty* database, not the one `create_all` populated.
    """
    eng = make_engine("sqlite:///:memory:", foreign_keys=True)
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session(engine):
    """A plain, directly-owned `Session` -- deliberately NOT
    `session_for_engine`/`session_scope`.

    Those context managers commit on clean exit, which does not match this
    suite's long-standing contract: the majority of the `pytest.raises(
    IntegrityError)` tests below call `session.commit()` themselves inside
    the `raises` block and never roll back afterwards, leaving the session
    in SQLAlchemy's "previous exception during flush" state on return. A
    `session_for_engine`-based fixture would then attempt its own
    `commit()` at teardown against that same aborted transaction and raise
    `PendingRollbackError` from every one of those tests, as a fixture
    teardown error rather than the intended, already-asserted
    `IntegrityError` -- verified directly by swapping this fixture to
    `session_for_engine` and rerunning the file: 20 of these tests turn from
    passing into `ERROR` (`PendingRollbackError` at fixture teardown), and
    the count is exact, not estimated. `db.py`'s own read/write and
    read-only session behaviour is covered directly by the `TestDb*` session
    classes near the end of this file, via `session_for_engine`/
    `session_scope` themselves; this fixture only needs the pragma-carrying
    `Engine` `make_engine` builds.
    """
    with Session(engine) as s:
        yield s


@pytest.fixture
def engine_no_fk():
    """A fresh in-memory SQLite engine with FK enforcement left OFF.

    `make_engine(url, foreign_keys=False)` -- a real, named opt-out (item
    5), not a workaround for item 5's absence. Only for tests that must
    deliberately construct a DB state the composite FK
    (`fk_retention_rules_dataset_remote`) would otherwise make impossible --
    see `_assert_scope_integrity`'s docstring in `zfsbackup/config/store/mapper.py`
    for why that mapper-side check exists as the belt to this braces.
    """
    eng = make_engine("sqlite:///:memory:", foreign_keys=False)
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session_no_fk(engine_no_fk):
    with Session(engine_no_fk) as s:
        yield s


@pytest.fixture
def file_db_url(tmp_path):
    """A `tmp_path`-backed file SQLite URL.

    The in-memory fixtures above cannot exercise WAL, sidecar files, fork,
    or cross-process concurrency at all -- `:memory:` has no file on disk
    for a second connection, let alone a second process, to open.
    """
    db_path = tmp_path / "store.db"
    return f"sqlite:///{db_path}"


@pytest.fixture
def file_engine(file_db_url):
    """A schema-initialized, pragma-carrying file engine for `file_db_url`,
    built via `make_engine` (not the pid-keyed `get_engine` cache -- tests
    that specifically need the cache use `get_engine` directly).
    """
    eng = make_engine(file_db_url)
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture(autouse=True)
def _clean_db_engine_cache():
    """Disposes every engine `get_engine()` has cached, before and after
    every test in this file.

    `db.py`'s pid-keyed `_ENGINES` cache is module-global state shared by
    the whole pytest process; without this, a test that calls `get_engine`
    would leak a live engine (and its open DBAPI connection/pool) into the
    next test, which is exactly the class of hazard `db.py` exists to
    prevent process-wide, not just across a fork. Autouse and file-scoped
    (not just on the `TestDb*` classes) because the cache is a single piece
    of global state regardless of which test populated it.
    """
    dispose_all()
    yield
    dispose_all()


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

    `zfsbackup/config/store/models.py` closes this with partial unique indexes
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
# Item 4 -- the mapper layer (zfsbackup/config/store/mapper.py)
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
        with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.mapper"):
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
        with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.mapper"):
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

        with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.mapper"):
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
        # destinations.name); re-checked anyway per config/model.py:715-728.
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
        from zfsbackup.config.store import mapper as mapper_module

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
        # `DatasetConfig.from_dict` (`config/model.py:624-626`) never checks this
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
        # Mirrors the `Destination.url` case above (`config/model.py:710-712`) for
        # `remote_backup.target_dataset` (`config/model.py:735-737`). The two
        # params matter for different reasons: `None` would fail anyway, as
        # a bare `NOT NULL constraint failed` from `remote_server
        # .target_dataset` (`models.py`), but loudly and late, after the
        # wipe. `""` is the one that actually mattered before this check
        # existed -- the NOT NULL column happily accepts an empty string,
        # so it inserted silently and `load_config` loaded it back as
        # `target_dataset=''`, a config `BackupConfig.from_file` can never
        # produce (`config/model.py:735-737` rejects an empty value at the YAML
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
# Migrations (item 7: zfsbackup/config/store/migrations/, zfsbackup/config/store/migrate.py)
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
    """Coverage for `zfsbackup/config/store/migrate.py` and
    `zfsbackup/config/store/migrations/` (item 7).

    Two DDL paths coexist in this codebase: `Base.metadata.create_all`
    (every fixture above this class) and `alembic upgrade head`
    (production, via `ensure_schema`). Assertion 1
    (`test_upgrade_head_matches_create_all`) is the only thing keeping
    those two paths honest with each other -- it must never be deleted in
    favour of assertion 2 alone.

    `Config` objects here are built via `zfsbackup.config.store.migrate.
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
        """Snapshot and restore `zfsbackup.config.store.mapper`'s `level` and
        `disabled` attributes around a test.

        Both are mutable global `logging` state, not per-test state --
        several tests below run a real `logging.config.fileConfig()` (via
        Alembic, positive and negative) against this exact logger, and
        several *other* tests elsewhere in this file assert against it via
        `caplog`. An earlier version of these tests restored `disabled` in
        one case and nothing at all in another, leaking mutated state into
        the rest of the pytest session regardless of which test ran first.
        """
        logger = logging.getLogger("zfsbackup.config.store.mapper")
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
        True`. Measured directly on this repo's own `zfsbackup.config.store.
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


@pytest.mark.unit
class TestMigrationsOverDbWriterConnection:
    """`ensure_schema` atomicity through a `zfsbackup.config.store.db` WRITER
    engine, not the bare `empty_memory_engine` (plain `create_engine`,
    pysqlite's legacy implicit-transaction handling) every test above
    uses.

    `migrate.py`'s own module docstring calls this out explicitly: "Item 5
    changes which of the two cases applies, not the advice." A `Connection`
    from a file-backed writer engine runs with `isolation_level = None`
    and an explicit `BEGIN IMMEDIATE` at the first statement
    (`_install_begin_immediate`) -- so `migrate.py`'s "case 2" (a clean
    connection, where the six `CREATE TABLE`s autocommit individually and
    only the final `INSERT INTO alembic_version` opens a real transaction)
    collapses into "case 1" (an already-open transaction, where NOTHING
    autocommits and the whole migration lives or dies with one commit).
    Concretely: over a db.py writer connection, `ensure_schema` +
    `rollback()` must leave ZERO tables -- not `TestMigrations`'
    `SchemaSplitBrain` state (full schema, empty `alembic_version`), which
    is specific to the bare-connection "case 2" mechanics. This is the one
    thing nothing in `TestMigrations` (deliberately built on a bare engine
    to characterise the *platform* mechanics independent of this store's
    own pragmas) can pin.
    """

    @pytest.fixture
    def writer_engine(self, file_db_url):
        eng = make_engine(file_db_url)
        yield eng
        eng.dispose()

    def test_ensure_schema_then_rollback_leaves_zero_tables(self, writer_engine):
        with writer_engine.connect() as conn:
            ensure_schema(conn)
            conn.rollback()

        insp = inspect(writer_engine)
        # Unlike the bare-connection case (`TestMigrations.test_ensure_
        # schema_first_call_on_clean_connection_leaves_split_brain_on_
        # rollback`), NO application table survives -- the six `CREATE
        # TABLE`s never auto-committed themselves, because
        # `isolation_level=None` + `BEGIN IMMEDIATE` mean this connection
        # never runs in pysqlite's legacy autocommit-between-statements
        # mode at all.
        assert insp.get_table_names() == []

    def test_ensure_schema_then_rollback_does_not_raise_schema_split_brain_on_retry(
        self, writer_engine
    ):
        # The direct behavioural contrast with `TestMigrations.test_
        # ensure_schema_raises_schema_split_brain_via_rollback`: over a
        # writer connection, a rolled-back first call leaves a genuinely
        # clean database, so a SECOND `ensure_schema` call on the same
        # engine must succeed normally rather than raising
        # `SchemaSplitBrain`.
        with writer_engine.connect() as conn:
            ensure_schema(conn)
            conn.rollback()

        with writer_engine.connect() as conn:
            code_head = ensure_schema(conn)  # must not raise
            conn.commit()

        with writer_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).fetchall()
        assert rows == [(code_head,)]

    def test_ensure_schema_then_commit_stamps_alembic_version_and_creates_tables(
        self, writer_engine
    ):
        with writer_engine.connect() as conn:
            code_head = ensure_schema(conn)
            conn.commit()

        insp = inspect(writer_engine)
        assert {
            "datasets", "destinations", "global_settings", "remote_server",
            "dataset_remotes", "retention_rules", "alembic_version",
        } <= set(insp.get_table_names())

        with writer_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).fetchall()
        assert rows == [(code_head,)]


# ---------------------------------------------------------------------------
# Item 5 -- zfsbackup/config/store/db.py (engine/session, WAL, fork safety)
# ---------------------------------------------------------------------------
#
# `pytest -k "TestDbEngine or TestDbPool or TestDbPragma or TestDbFork or
# TestDbWal or TestDbReadOnly or TestDbCrash or TestDbDispose"` selects just
# this section.
#
# `_clean_db_engine_cache` (file-scoped autouse fixture, above) disposes
# every engine `get_engine()` has cached before and after each test here, so
# nothing in this section leaks a live engine into the next test's process.


@pytest.mark.unit
class TestDbEngineValidation:
    """`make_engine`'s two rejections (`db.py`): a non-SQLite URL, and a
    `mode=ro` SQLite URL. Both are `ValueError`, both fail before any
    `Engine` is constructed.
    """

    def test_non_sqlite_url_raises_value_error(self):
        with pytest.raises(ValueError, match="SQLite"):
            make_engine("postgresql://user@host/db")

    def test_mode_ro_query_param_raises_value_error(self, tmp_path):
        db_path = tmp_path / "ro.db"
        with pytest.raises(ValueError, match="mode=ro"):
            make_engine(f"sqlite:///{db_path}?mode=ro")

    def test_mode_ro_raw_string_form_raises_value_error(self, tmp_path):
        # `db.py` checks the raw string in addition to the parsed query
        # dict specifically so an odd spelling of the same option cannot
        # slip past `make_url`'s parsing. This URL is ALSO shared-cache
        # (`cache=shared`) -- it doubles as the ordering pin between the
        # two rejections in `TestDbSharedCacheRejection`: `mode=ro` is
        # checked first in `make_engine`, so a URL matching both reasons
        # must report the `mode=ro` one, not the shared-cache one.
        db_path = tmp_path / "ro2.db"
        with pytest.raises(ValueError, match="mode=ro"):
            make_engine(f"sqlite:///{db_path}?mode=ro&cache=shared")

    def test_bare_filesystem_path_raises_value_error_naming_url_for_path(
        self, tmp_path
    ):
        # The likely mistake once a path-taking caller owns resolution: a
        # filesystem path passed where a URL is wanted. `make_url` raises a
        # bare `ArgumentError` that says nothing about the fix; `db.py`
        # catches it and re-raises naming `url_for_path` explicitly.
        db_path = tmp_path / "plain_path.db"
        with pytest.raises(ValueError, match="url_for_path"):
            make_engine(str(db_path))


@pytest.mark.unit
class TestDbSharedCacheRejection:
    """`make_engine` refuses shared-cache in-memory SQLite URLs outright.

    Not merely unsupported -- rejected, because it is the one URL shape on
    which a two-writer guard test would demonstrate the pre-`BEGIN
    IMMEDIATE` lost update *through the public API* while reporting green:
    two engines on one `mode=memory&cache=shared` database are two real
    connections onto one database (unlike a private `:memory:` engine,
    where `StaticPool` hands out the SAME connection to everyone). And it
    cannot be fixed by simply installing `BEGIN IMMEDIATE` there the way
    the file case was: shared-cache mode uses table-level locking that
    raises `SQLITE_LOCKED`, which the busy handler does not retry, so
    every session -- including an uninvolved one -- would fail with
    `database table is locked` instead. Refusing the URL is what makes a
    future two-writer test against this shape structurally impossible to
    write and have it silently pass.
    """

    def test_query_param_form_rejected(self):
        with pytest.raises(ValueError, match="shared-cache"):
            make_engine("sqlite:///:memory:?cache=shared")

    def test_mode_memory_cache_shared_form_rejected(self):
        with pytest.raises(ValueError, match="shared-cache"):
            make_engine("sqlite:///?mode=memory&cache=shared")

    def test_private_memory_url_is_not_rejected(self):
        # Negative control for the two tests above: a PRIVATE in-memory
        # URL (no `cache=shared`) must keep working -- this class is
        # pinning the shared-cache predicate specifically, not merely
        # "any URL containing 'memory' is now rejected".
        eng = make_engine("sqlite:///:memory:")
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("SELECT 1").scalar() == 1
        finally:
            eng.dispose()


@pytest.mark.unit
class TestDbUrlForPath:
    """`url_for_path` builds the canonical SQLite URL for a filesystem
    path -- `resolve()`'d, so a relative path and an equivalent absolute
    or symlinked path produce the IDENTICAL cache key `get_engine` uses.
    Skipping this normalisation is the one thing the store's single-writer
    design rules out: `sqlite:///config.db` and
    `sqlite:////abs/path/config.db` for the same file would be two cache
    entries with two separate pools -- two writers racing each other on
    one database.
    """

    def test_builds_canonical_absolute_url(self, tmp_path):
        db_path = tmp_path / "canon.db"
        url = url_for_path(db_path)
        assert url == f"sqlite:///{db_path.resolve()}"
        assert url.startswith("sqlite:////")  # absolute: 3 scheme slashes + leading /

    def test_relative_path_resolves_to_the_same_url_as_absolute(self, tmp_path, monkeypatch):
        db_path = tmp_path / "canon2.db"
        monkeypatch.chdir(tmp_path)
        relative_url = url_for_path("canon2.db")
        absolute_url = url_for_path(db_path)
        assert relative_url == absolute_url

    def test_symlinked_path_resolves_to_the_real_file_url(self, tmp_path):
        real = tmp_path / "real.db"
        real.write_text("")
        link = tmp_path / "link.db"
        link.symlink_to(real)
        assert url_for_path(link) == url_for_path(real)


@pytest.mark.unit
class TestDbPoolSelection:
    """5a: `StaticPool` for `:memory:` (a second pooled connection must see
    the *same* database), plain `QueuePool` defaults for a file URL (the
    pysqlite dialect already passes `check_same_thread=False` for file
    URLs, so `make_engine` must not override the pool class there).
    """

    def test_memory_url_uses_static_pool(self):
        eng = make_engine("sqlite:///:memory:")
        try:
            assert isinstance(eng.pool, StaticPool)
        finally:
            eng.dispose()

    def test_file_url_uses_queue_pool(self, file_db_url):
        eng = make_engine(file_db_url)
        try:
            assert isinstance(eng.pool, QueuePool)
        finally:
            eng.dispose()

    def test_memory_url_second_checkout_sees_same_database(self):
        # The actual reason StaticPool is load-bearing: without it, a
        # second pooled connection to `:memory:` would see an *empty*
        # database rather than the one the first connection populated.
        #
        # This must be done from a SECOND THREAD, not a second sequential
        # checkout on the main thread. A single-threaded second checkout
        # does not discriminate `StaticPool` from SQLAlchemy's fallback
        # `SingletonThreadPool` (the pool `create_engine` would choose for
        # a `:memory:` URL with no `poolclass=` override at all):
        # `SingletonThreadPool` also reuses one connection *per thread*, so
        # a single-threaded test passes identically whether
        # `poolclass=StaticPool` is present or was deleted from
        # `make_engine` -- verified directly: removing `poolclass=
        # StaticPool` from `make_engine` failed only the `isinstance(eng.
        # pool, StaticPool)` check in `test_memory_url_uses_static_pool`,
        # not this test, before this fix. A cross-thread checkout is also
        # the real hazard this pool choice exists for: `ApiWorker`
        # (`workers.py`) runs the Flask app in a thread inside a worker
        # process, so a second thread reaching the same in-memory engine is
        # not a hypothetical.
        eng = make_engine("sqlite:///:memory:")
        try:
            with eng.connect() as conn:
                conn.exec_driver_sql("CREATE TABLE t (id INTEGER)")
                conn.exec_driver_sql("INSERT INTO t (id) VALUES (1)")
                conn.commit()

            result = {}

            def _read_from_other_thread():
                try:
                    with eng.connect() as conn:
                        result["rows"] = conn.exec_driver_sql(
                            "SELECT id FROM t"
                        ).fetchall()
                except Exception as exc:  # noqa: BLE001
                    result["error"] = exc

            thread = threading.Thread(target=_read_from_other_thread)
            thread.start()
            thread.join(timeout=10)
            assert not thread.is_alive()

            assert "error" not in result, result.get("error")
            assert result["rows"] == [(1,)]
        finally:
            eng.dispose()


@pytest.mark.unit
class TestDbIsMemoryUrl:
    """`_is_memory_url` is decided from the **parsed** URL, never a raw
    substring test -- see its docstring for the `'sqlite://'` false
    negative a substring test produced. Table-driven over every spelling
    the docstring names, plus the file-URL cases that must NOT be treated
    as in-memory.
    """

    @pytest.mark.parametrize(
        "url, expected",
        [
            ("sqlite://", True),
            ("sqlite:///", True),
            ("sqlite:///:memory:", True),
            ("sqlite:///?mode=memory&cache=shared", True),
            ("sqlite:///relative.db", False),
            ("sqlite:////abs/path.db", False),
        ],
    )
    def test_is_memory_url_table(self, url, expected):
        assert _is_memory_url(make_url(url)) is expected

    def test_bare_sqlite_scheme_cross_thread_read_sees_same_database(self):
        """The regression this table exists to catch: before the parsed-URL
        fix, `'sqlite://'` (no path, no `:memory:` substring, no `mode=
        memory`) took the FILE branch -- losing `StaticPool` and the
        explicit `check_same_thread=False` override -- so a second thread
        reading through the same engine saw an empty database (`no such
        table`). `make_engine("sqlite://")` must behave exactly like
        `make_engine("sqlite:///:memory:")` here.
        """
        eng = make_engine("sqlite://")
        try:
            assert isinstance(eng.pool, StaticPool)
            with eng.connect() as conn:
                conn.exec_driver_sql("CREATE TABLE t (id INTEGER)")
                conn.exec_driver_sql("INSERT INTO t (id) VALUES (1)")
                conn.commit()

            result = {}

            def _read_from_other_thread():
                try:
                    with eng.connect() as conn:
                        result["rows"] = conn.exec_driver_sql(
                            "SELECT id FROM t"
                        ).fetchall()
                except Exception as exc:  # noqa: BLE001
                    result["error"] = exc

            thread = threading.Thread(target=_read_from_other_thread)
            thread.start()
            thread.join(timeout=10)
            assert not thread.is_alive()

            assert "error" not in result, result.get("error")
            assert result["rows"] == [(1,)]
        finally:
            eng.dispose()


@pytest.mark.unit
class TestDbPragmaPersistence:
    """5a: pragmas are set once by a `connect` event listener and survive
    pool checkin/checkout -- not reissued per checkout. Pinned across
    **three** sequential checkouts of the same engine (not just two), per
    the plan's probe, and the connect-event count is asserted directly
    rather than merely inferred from pragma values still being correct.
    """

    def test_pragmas_survive_three_sequential_checkouts(self, file_db_url):
        eng = make_engine(file_db_url)
        connect_events = []

        @event.listens_for(eng, "connect")
        def _count_connect(dbapi_connection, connection_record):
            connect_events.append(1)

        try:
            for _ in range(3):
                with eng.connect() as conn:
                    fk = conn.exec_driver_sql("PRAGMA foreign_keys").scalar()
                    jm = conn.exec_driver_sql("PRAGMA journal_mode").scalar()
                    assert fk == 1
                    assert jm == "wal"
            # One real DBAPI connection served all three sequential
            # checkouts (QueuePool reuse), so the connect event -- and
            # therefore every pragma registered in it -- fired exactly
            # once, not three times.
            assert len(connect_events) == 1
        finally:
            eng.dispose()


@pytest.mark.unit
class TestDbForeignKeysParameter:
    def test_foreign_keys_false_yields_zero(self, tmp_path):
        db_path = tmp_path / "fk_off.db"
        eng = make_engine(f"sqlite:///{db_path}", foreign_keys=False)
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 0
        finally:
            eng.dispose()

    def test_foreign_keys_true_yields_one(self, tmp_path):
        db_path = tmp_path / "fk_on.db"
        eng = make_engine(f"sqlite:///{db_path}", foreign_keys=True)
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        finally:
            eng.dispose()

    def test_foreign_keys_off_engine_does_not_leak_onto_fk_on_engine_same_url(
        self, tmp_path
    ):
        # Two SEPARATE `make_engine` calls for the same URL get separate
        # pools/connections -- an FK-off engine existing must not affect a
        # later FK-on engine for the identical file.
        db_path = tmp_path / "shared.db"
        url = f"sqlite:///{db_path}"
        fk_off = make_engine(url, foreign_keys=False)
        fk_on = make_engine(url, foreign_keys=True)
        try:
            with fk_on.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
            # Re-check fk_off AFTER fk_on was built and used: still 0.
            with fk_off.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 0
        finally:
            fk_off.dispose()
            fk_on.dispose()


@pytest.mark.unit
class TestDbReaderWriterSeparatePools:
    """5c: a read-only and a read-write engine for the same URL are
    separate `Engine`/pool objects -- sharing one would leak `query_only=
    ON` onto the writer's connections.
    """

    def test_writer_engine_unaffected_by_existing_reader_engine_same_url(
        self, file_engine, file_db_url
    ):
        # `file_engine` (fixture) has already run create_all against
        # `file_db_url` and owns its own disposal -- reuse it to create the
        # file rather than building and leaking a second, throwaway engine.
        reader = make_engine(file_db_url, readonly=True)
        try:
            with reader.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA query_only").scalar() == 1

            writer = make_engine(file_db_url, readonly=False)
            try:
                with writer.connect() as conn:
                    assert conn.exec_driver_sql("PRAGMA query_only").scalar() == 0
            finally:
                writer.dispose()
        finally:
            reader.dispose()


@pytest.mark.unit
class TestDbWriterTransactionSetup:
    """5b/generation-race fix: a file-backed WRITER engine gets a `begin`
    listener (`_install_begin_immediate`'s `_begin_immediate`, which issues
    `BEGIN IMMEDIATE`) and disables pysqlite's legacy implicit `BEGIN`
    (`isolation_level = None`) so that listener is the only thing that ever
    starts a transaction. A READER engine gets neither -- see the module
    docstring's "Never adopt SQLAlchemy's 'BEGIN on connect' recipe for
    READER engines" warning.
    """

    def test_writer_engine_has_begin_listener_and_null_isolation_level(
        self, file_db_url
    ):
        eng = make_engine(file_db_url, readonly=False)
        try:
            assert len(list(eng.dispatch.begin)) == 1
            with eng.connect() as conn:
                assert conn.connection.dbapi_connection.isolation_level is None
        finally:
            eng.dispose()

    def test_reader_engine_has_no_begin_listener_and_default_isolation_level(
        self, file_engine, file_db_url
    ):
        eng = make_engine(file_db_url, readonly=True)
        try:
            assert len(list(eng.dispatch.begin)) == 0
            with eng.connect() as conn:
                # pysqlite's own legacy default (an empty string, not
                # `None`) -- readers must not opt into the writer's
                # "BEGIN on connect" recipe.
                assert conn.connection.dbapi_connection.isolation_level is not None
        finally:
            eng.dispose()

    def test_memory_writer_engine_has_no_begin_listener(self):
        # `_install_begin_immediate` is scoped to writer engines on pools
        # that hand each checkout a distinct connection (the predicate is
        # the POOL class, not the URL shape -- see `_SHARED_CONNECTION_
        # POOLS`) -- `:memory:` uses `StaticPool`, which hands the same
        # DBAPI connection to every checkout, so a second `Session` would
        # hit `OperationalError: cannot start a transaction within a
        # transaction` (see `_install_begin_immediate`'s docstring).
        eng = make_engine("sqlite:///:memory:", readonly=False)
        try:
            assert isinstance(eng.pool, StaticPool)
            assert len(list(eng.dispatch.begin)) == 0
        finally:
            eng.dispose()

    def test_bare_sqlite_scheme_writer_engine_has_no_begin_listener(self):
        # `sqlite://` is also an in-memory URL (`_is_memory_url`) and also
        # lands on `StaticPool` via `make_engine`'s memory branch -- same
        # exclusion as `sqlite:///:memory:`, pinned separately because this
        # is exactly the URL spelling that used to slip through
        # `_is_memory_url`'s pre-fix raw-substring check (see that
        # function's own docstring).
        eng = make_engine("sqlite://", readonly=False)
        try:
            assert isinstance(eng.pool, StaticPool)
            assert len(list(eng.dispatch.begin)) == 0
        finally:
            eng.dispose()

    def test_singleton_thread_pool_predicate_would_exclude_it_too(self):
        """Direct pin of the SECOND member of `_SHARED_CONNECTION_POOLS`.

        `make_engine` itself never actually builds a `SingletonThreadPool`
        engine today -- its memory branch forces `poolclass=StaticPool`
        explicitly. The predicate still names `SingletonThreadPool`
        defensively: it is SQLAlchemy's own DEFAULT pool for a bare
        `sqlite://`/`:memory:` URL when `poolclass` is left unset -- i.e.
        exactly what `make_engine` would fall back to if a future change
        ever dropped the explicit `poolclass=StaticPool` override (the
        same shape as the pre-`_is_memory_url`-fix history the module
        docstring measures: `'sqlite://' pool=SingletonThreadPool`). It
        shares the same hazard `StaticPool` does -- one DBAPI connection
        per thread, shared across every concurrent checkout in that
        thread -- so a second `Session` on it would hit the identical
        `OperationalError: cannot start a transaction within a
        transaction`. This test builds a `SingletonThreadPool` engine
        directly (bypassing `make_engine`, since it never produces one)
        and evaluates `make_engine`'s own exclusion predicate --
        `isinstance(engine.pool, store_db._SHARED_CONNECTION_POOLS)` --
        against it, referencing the REAL tuple `db.py` uses rather than a
        copy, so removing `SingletonThreadPool` from that tuple fails this
        test.
        """
        eng = create_engine(
            "sqlite://",
            poolclass=SingletonThreadPool,
            connect_args={"check_same_thread": False},
        )
        try:
            assert isinstance(eng.pool, SingletonThreadPool)
            assert isinstance(eng.pool, store_db._SHARED_CONNECTION_POOLS)
        finally:
            eng.dispose()


@pytest.mark.unit
class TestDbPragmaConnectListenerInternals:
    """Whitebox coverage of `_install_pragmas`'s `connect` listener via a
    stub DBAPI connection, so the WARNING branch (SQLite silently refusing
    WAL) and the exact pragma statement order can be pinned without
    depending on a filesystem or platform that actually refuses WAL.
    """

    @pytest.fixture(autouse=True)
    def _db_logger_state(self):
        """Snapshot and restore `zfsbackup.config.store.db`'s logger state.

        `TestMigrations.test_configure_logger_guard_reachable_via_real_ini`
        runs a real `logging.config.fileConfig()` against this repo's
        `alembic.ini`, whose `[loggers]` section names only `root,
        sqlalchemy, alembic` -- `fileConfig`'s default
        `disable_existing_loggers=True` therefore disables every other
        already-configured logger for the rest of the pytest session,
        `zfsbackup.config.store.db` included. Without restoring it here, this
        class's `caplog`-based tests pass in isolation but fail when the
        full file runs (the disabled logger drops every `.warning()` call
        before it ever reaches `caplog`'s handler) -- exactly the kind of
        cross-test leak `TestMigrations.mapper_logger_state` guards against
        for the mapper's own logger.
        """
        logger = logging.getLogger("zfsbackup.config.store.db")
        original_level = logger.level
        original_disabled = logger.disabled
        logger.disabled = False
        yield
        logger.level = original_level
        logger.disabled = original_disabled

    class _StubCursor:
        def __init__(self, calls, journal_mode_row):
            self._calls = calls
            self._journal_mode_row = journal_mode_row

        def execute(self, sql, *args):
            self._calls.append(sql)

        def fetchone(self):
            if self._calls and "JOURNAL_MODE" in self._calls[-1].upper():
                return self._journal_mode_row
            return None

        def close(self):
            pass

    class _StubDbapiConnection:
        def __init__(self, calls, journal_mode_row=("wal",)):
            self._calls = calls
            self._journal_mode_row = journal_mode_row

        def cursor(self):
            return TestDbPragmaConnectListenerInternals._StubCursor(
                self._calls, self._journal_mode_row
            )

        # The pysqlite dialect's own `on_connect` touches a few attributes
        # (e.g. `isolation_level`) ahead of our listener; accept anything.
        def __getattr__(self, name):
            return lambda *a, **k: None

    def _build_stub_engine(self, journal_mode_row):
        calls = []

        def _creator():
            return self._StubDbapiConnection(calls, journal_mode_row)

        eng = create_engine("sqlite://", creator=_creator, poolclass=StaticPool)
        store_db._install_pragmas(
            eng, readonly=False, foreign_keys=True, memory=False
        )
        return eng, calls

    def test_wal_refusal_logs_warning_naming_the_unchanged_mode(self, caplog):
        # Simulates a read-only file or a filesystem without WAL support:
        # SQLite does not raise when it cannot switch journal modes -- it
        # returns the UNCHANGED mode, silently, unless something checks it.
        eng, calls = self._build_stub_engine(journal_mode_row=("delete",))
        try:
            with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.db"):
                with eng.connect():
                    pass
        finally:
            eng.dispose()

        assert any(
            "SQLite refused WAL journal mode" in record.message
            and "'delete'" in record.message
            for record in caplog.records
        )

    def test_wal_success_logs_no_warning(self, caplog):
        eng, calls = self._build_stub_engine(journal_mode_row=("wal",))
        try:
            with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.db"):
                with eng.connect():
                    pass
        finally:
            eng.dispose()

        assert not any(
            "SQLite refused WAL journal mode" in record.message
            for record in caplog.records
        )

    def test_busy_timeout_is_the_first_pragma_issued(self):
        # "FIRST, before any statement that can contend for a lock" --
        # `_install_pragmas`'s own comment. Filter out the pysqlite
        # dialect's own connect-time housekeeping (e.g. `PRAGMA
        # read_uncommitted`), which runs through a separate cursor before
        # our listener fires and is not part of the sequence under test.
        eng, calls = self._build_stub_engine(journal_mode_row=("wal",))
        try:
            with eng.connect():
                pass
        finally:
            eng.dispose()

        our_calls = [c for c in calls if "PRAGMA" in c.upper()]
        pragma_order = [
            c for c in our_calls
            if any(
                name in c.upper()
                for name in (
                    "BUSY_TIMEOUT", "JOURNAL_MODE", "SYNCHRONOUS",
                    "FOREIGN_KEYS", "QUERY_ONLY",
                )
            )
        ]
        assert pragma_order[0] == "PRAGMA busy_timeout=5000"
        assert pragma_order == [
            "PRAGMA busy_timeout=5000",
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=FULL",
            "PRAGMA foreign_keys=ON",
        ]


@pytest.mark.unit
class TestDbPragmaValues:
    """5a/D2: `journal_mode` is `wal` for a file URL, `memory` for
    `:memory:`, neither raising. `synchronous` is FULL (2) for the writer
    engine and NORMAL (1) for the reader engine (D2's binding decision).
    """

    def test_journal_mode_wal_for_file_url(self, file_db_url):
        eng = make_engine(file_db_url)
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
        finally:
            eng.dispose()

    def test_journal_mode_memory_for_in_memory_url_does_not_raise(self):
        eng = make_engine("sqlite:///:memory:")
        try:
            with eng.connect() as conn:
                # Cosmetic per the plan, not a correctness requirement --
                # issuing `journal_mode=WAL` against `:memory:` must not
                # raise, and simply returns 'memory'.
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "memory"
        finally:
            eng.dispose()

    def test_synchronous_full_for_writer_engine(self, file_db_url):
        eng = make_engine(file_db_url, readonly=False)
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA synchronous").scalar() == 2
        finally:
            eng.dispose()

    def test_synchronous_normal_for_reader_engine(self, file_engine, file_db_url):
        eng = make_engine(file_db_url, readonly=True)
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA synchronous").scalar() == 1
        finally:
            eng.dispose()

    def test_busy_timeout_is_5000(self, file_db_url):
        # Every other pragma `_install_pragmas` sets has a direct value
        # assertion somewhere in this class; `busy_timeout` did not, so
        # deleting the `PRAGMA busy_timeout=5000` line failed zero tests.
        # 5000ms is also what makes `TestDbGenerationRace`'s "blocked then
        # fails" test's short override observable as a deliberate outlier
        # rather than the default.
        eng = make_engine(file_db_url)
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 5000
        finally:
            eng.dispose()


@pytest.mark.unit
class TestDbDisposeAll:
    def test_idempotent_and_safe_on_empty_cache(self):
        dispose_all()
        dispose_all()  # must not raise the second time either
        assert store_db._ENGINES == {}

    def test_disposes_every_cached_engine_and_empties_the_cache(self, file_db_url):
        get_engine(file_db_url)
        get_engine(file_db_url, readonly=True)
        assert len(store_db._ENGINES) == 2

        dispose_all()

        assert store_db._ENGINES == {}


@pytest.mark.unit
class TestDbDisposeAllPidGuard:
    """`dispose_all()`'s own pid guard (`db.py`'s docstring: "Pid-guarded,
    like `get_engine`"), pinned without an actual fork -- the same
    whitebox technique `TestDbGetEngineStalePidHandling` uses for
    `get_engine()`'s branch, applied to `dispose_all`'s separate one.

    Not a redundant echo of that other class: earlier, `dispose_all`
    unpacked `owner_pid` and then ignored it, always passing `close=True`.
    A forked child calling it (daemon pre-fork hygiene, a worker's own
    teardown) would run the close path on connections the PARENT still
    believes it owns and exit 0 with no warning -- probed directly (see
    `db.py`'s docstring) and confirmed as a real, silent hazard on this
    function's own code path, not merely inherited from `get_engine`'s.
    `TestDbForkSafety.
    test_dispose_all_in_forked_child_does_not_disturb_parents_connection`
    below is the same claim through a genuine fork.
    """

    def test_foreign_owner_pid_entry_disposed_with_close_false(
        self, file_db_url, mocker
    ):
        stale_engine = make_engine(file_db_url)
        fake_pid = os.getpid() + 1  # guaranteed to differ from this process's pid
        key = (file_db_url, False, True)
        store_db._ENGINES[key] = (fake_pid, stale_engine)

        dispose_spy = mocker.spy(stale_engine, "dispose")

        dispose_all()

        dispose_spy.assert_called_once_with(close=False)
        assert store_db._ENGINES == {}

        # close=False did not actually close it (nobody forked this
        # single-process test) -- close it for real so it does not leak.
        stale_engine.dispose()

    def test_own_pid_entry_disposed_with_close_true(self, file_db_url, mocker):
        get_engine(file_db_url)  # cached under THIS process's pid
        key = (file_db_url, False, True)
        owner_pid, engine = store_db._ENGINES[key]
        assert owner_pid == os.getpid()

        dispose_spy = mocker.spy(engine, "dispose")

        dispose_all()

        dispose_spy.assert_called_once_with(close=True)
        assert store_db._ENGINES == {}


@pytest.mark.unit
class TestDbGetEngineCaching:
    """`get_engine()`'s cache-HIT path: the same `(url, readonly,
    foreign_keys)` key returns the SAME `Engine` object on a second call
    from the same process. `TestDbDisposeAll.
    test_disposes_every_cached_engine_and_empties_the_cache` only counts
    `len(_ENGINES)`, which an always-rebuild implementation (`if owner_pid
    == pid: return engine` replaced with `if False:`) satisfies just as
    well -- two calls still populate two cache entries, one per key, even
    if each call silently replaces its entry with a fresh `Engine`. That
    mutation opens a fresh connection plus every `connect`-event pragma
    round-trip on EVERY `get_engine()` call -- a real cost in a worker's
    poll loop -- and means `dispose_all()` no longer disposes what any
    caller still holds a reference to. Only an identity check on repeated
    calls catches it.
    """

    def test_same_key_returns_the_same_engine_object(self, file_db_url):
        first = get_engine(file_db_url)
        second = get_engine(file_db_url)
        assert first is second

    def test_readonly_and_readwrite_are_different_cached_engines(self, file_db_url):
        writer = get_engine(file_db_url, readonly=False)
        reader = get_engine(file_db_url, readonly=True)
        assert writer is not reader
        # ...and each is still itself stable across a repeat call.
        assert get_engine(file_db_url, readonly=False) is writer
        assert get_engine(file_db_url, readonly=True) is reader


# ---------------------------------------------------------------------------
# Fork safety (5b) -- the highest-risk item in the plan.
#
# These helpers are module-level so a `multiprocessing.get_context("fork")`
# child can call them directly. Results cross the process boundary via a
# plain JSON file rather than a `multiprocessing.Queue`, to keep the IPC
# mechanism itself out of the way of what is under test.
# ---------------------------------------------------------------------------


def _fork_child_get_engine(url, result_path):
    """Runs inside a forked child. Calls the real, guarded `get_engine()` --
    the pid check must notice this process did not build the cached entry
    and discard-and-rebuild rather than reuse the parent's pool.
    """
    engine = get_engine(url)
    with engine.connect() as conn:
        dbapi_id = id(conn.connection.dbapi_connection)
        names = [
            row[0]
            for row in conn.exec_driver_sql(
                "SELECT name FROM datasets ORDER BY id"
            ).fetchall()
        ]
        integrity = conn.exec_driver_sql("PRAGMA integrity_check").scalar()
    result = {
        "pid": os.getpid(),
        "engine_id": id(engine),
        "dbapi_id": dbapi_id,
        "dataset_names": names,
        "integrity_check": integrity,
    }
    with open(result_path, "w") as fh:
        json.dump(result, fh)


def _fork_child_use_inherited_engine_directly(url, result_path):
    """The negative control: bypasses `get_engine()` entirely and reaches
    into `db._ENGINES` for the raw, inherited `(owner_pid, engine)` tuple --
    exactly what a forked child would see the instant after fork, before
    anyone calls `get_engine()`. Demonstrates the hazard 5b exists to
    prevent: no exception of any kind, and the same DBAPI connection object
    the parent still holds gets used to read (and, in the real bug, could
    be used to write) from the child.
    """
    key = (url, False, True)  # (url, readonly=False, foreign_keys=True)
    owner_pid, inherited_engine = store_db._ENGINES[key]
    with inherited_engine.connect() as conn:
        dbapi_id = id(conn.connection.dbapi_connection)
        names = [
            row[0]
            for row in conn.exec_driver_sql(
                "SELECT name FROM datasets ORDER BY id"
            ).fetchall()
        ]
    result = {
        "pid": os.getpid(),
        "owner_pid_seen": owner_pid,
        "engine_id": id(inherited_engine),
        "dbapi_id": dbapi_id,
        "dataset_names": names,
    }
    with open(result_path, "w") as fh:
        json.dump(result, fh)


def _fork_child_dispose_all(url, result_path):
    """Runs inside a forked child. Calls the real, guarded `dispose_all()`
    directly -- no `get_engine()` call first -- so the ONLY cache entry
    that exists at all is the one inherited from the parent (`owner_pid ==
    parent's pid`). `dispose_all()`'s own pid check must discard it with
    `close=False`, exactly as `get_engine()` would, rather than the
    unconditional `close=True` an earlier, pid-unaware implementation
    passed regardless of ownership.
    """
    dispose_all()
    result = {
        "pid": os.getpid(),
        "cache_emptied": store_db._ENGINES == {},
    }
    with open(result_path, "w") as fh:
        json.dump(result, fh)


@pytest.mark.unit
class TestDbGetEngineStalePidHandling:
    """Whitebox pin for `get_engine()`'s discard-and-rebuild branch,
    without an actual fork -- runs unconditionally (no `FORK_AVAILABLE`
    skip), since it needs no subprocess at all.

    The real fork tests below (`TestDbForkSafety`) cannot, on their own,
    tell "closed with `close=False`" from "closed with a bare `dispose()`"
    apart: either way, the CHILD calls `sqlite3.connect()` fresh for its
    rebuilt engine, so the child's own read/integrity-check assertions pass
    identically under both. The actual difference `close=False` protects
    against is what happens to the file descriptor the PARENT still
    believes it owns -- an effect on a different process's kernel-level fd
    state, which is unreliable and slow to assert directly in a unit test.
    This test instead manufactures the exact precondition a forked child
    inherits (a cache entry whose `owner_pid` is not this process's own)
    without forking at all, and pins the *call itself*: `Engine.dispose`
    must be invoked with `close=False`, not merely "invoked".
    """

    def test_stale_pid_entry_is_discarded_with_close_false_and_rebuilt(
        self, file_db_url, mocker
    ):
        stale_engine = make_engine(file_db_url)

        # Behavioural half of the pin (not just "dispose was called with
        # close=False"): grab a live reference to the pooled DBAPI
        # connection BEFORE the discard-and-rebuild runs. Holding this
        # reference from Python is what makes `close=False` observable
        # in-process at all -- see the sibling test below for the
        # `close=True` contrast that proves this isn't just "nothing
        # happened to be called".
        with stale_engine.connect() as conn:
            dbapi_conn = conn.connection.dbapi_connection

        fake_pid = os.getpid() + 1  # guaranteed to differ from this process's pid
        key = (file_db_url, False, True)
        store_db._ENGINES[key] = (fake_pid, stale_engine)

        dispose_spy = mocker.spy(stale_engine, "dispose")

        rebuilt = get_engine(file_db_url)

        # A genuinely different Engine object was built for THIS process...
        assert rebuilt is not stale_engine
        # ...and the stale one was dropped via close=False, never a bare
        # dispose() -- "the single most important line" per db.py.
        dispose_spy.assert_called_once_with(close=False)
        # The cache now attributes the entry to this process, not the fake
        # one, and holds the rebuilt engine.
        assert store_db._ENGINES[key] == (os.getpid(), rebuilt)

        # THE BEHAVIOURAL ASSERTION: `close=False` leaves the connection
        # we still hold a reference to genuinely usable -- it was never
        # explicitly closed, only de-referenced by the pool. Contrast with
        # `test_bare_dispose_close_true_closes_a_still_referenced_connection`
        # below, where the identical setup DOES raise. This was previously
        # asserted only via the `dispose_spy` call args -- true, but the
        # actual claim ("the connection is left usable") is observable
        # in-process in microseconds, not "unreliable and slow to assert
        # directly" as an earlier version of this test's rationale claimed.
        dbapi_conn.execute("SELECT 1")
        dbapi_conn.close()  # hygiene: avoid leaking this fd past the test

        # dispose(close=False) deliberately does not close the real
        # connection (that is the whole point for an actually-forked
        # child, which must not touch the parent's fd) -- but nobody else
        # owns `stale_engine` in this single-process test, so it must be
        # closed for real here or its connection leaks for the rest of the
        # suite.
        stale_engine.dispose()

    def test_bare_dispose_close_true_closes_a_still_referenced_connection(
        self, file_db_url
    ):
        """The contrast that makes the test above meaningful: with the
        default `close=True`, `Engine.dispose()` closes pooled connections
        for real, even one this test still holds a Python reference to.
        Without this negative half, `dbapi_conn.execute("SELECT 1")`
        succeeding above could just as easily mean "the assertion doesn't
        discriminate anything" as "close=False genuinely didn't close it".
        """
        eng = make_engine(file_db_url)
        with eng.connect() as conn:
            dbapi_conn = conn.connection.dbapi_connection

        eng.dispose()  # close=True, the default

        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            dbapi_conn.execute("SELECT 1")


@pytest.mark.skipif(
    not FORK_AVAILABLE,
    reason="'fork' multiprocessing start method unavailable on this platform",
)
@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestDbForkSafety:
    """5b, pinned with `multiprocessing.get_context("fork")` explicitly --
    this machine's default start method is `spawn` (Python 3.14 / darwin),
    under which every process gets its own fresh interpreter and no
    inherited file descriptor exists to corrupt anything. An unpinned test
    here would pass while testing nothing.

    Marked `integration`/`slow`, not `unit`: every test here forks a real
    OS process and joins it with a 30s timeout. `pytest -m unit` should
    mean "no subprocess, no real fork, fast" -- this class was previously
    marked `unit` despite doing exactly the opposite.

    The `filterwarnings` marker silences CPython's own `DeprecationWarning`
    ("this process is multi-threaded, use of fork() may lead to
    deadlocks") that `multiprocessing.Process.start()` raises on every test
    here. It is expected, not a bug to fix: pytest's own process is
    multi-threaded (coverage, capture, etc.), and forking a multi-threaded
    process safely is *the exact subject matter this class exists to
    test* -- `db.py`'s `_reinit_lock_after_fork` (`os.register_at_fork`)
    is the module's own answer to that same hazard for its `_ENGINES_LOCK`.
    Silencing it here, narrowly, is preferable to a global filter (which
    would hide it for a genuinely new, unrelated multi-threaded-fork site
    elsewhere) or leaving three identical, permanently-unactionable
    warnings in every test run.
    """

    def _seed(self, url):
        engine = get_engine(url)
        Base.metadata.create_all(engine)
        with session_for_engine(engine) as session:
            session.add(
                Dataset(name="tank/fork", frequency_seconds=3600, frequency_literal="1h")
            )
        return engine

    def test_child_rebuilds_a_fresh_engine_and_connection_after_fork(
        self, file_db_url, tmp_path
    ):
        parent_engine = self._seed(file_db_url)
        with parent_engine.connect() as conn:
            parent_dbapi_id = id(conn.connection.dbapi_connection)

        result_path = tmp_path / "fork_positive.json"
        ctx = multiprocessing.get_context("fork")
        proc = ctx.Process(
            target=_fork_child_get_engine, args=(file_db_url, str(result_path))
        )
        proc.start()
        proc.join(timeout=30)
        assert proc.exitcode == 0

        result = json.loads(result_path.read_text())

        # The child's engine is not the parent's object...
        assert result["engine_id"] != id(parent_engine)
        # ...and its DBAPI connection is not the parent's either.
        #
        # KNOWN FLAKE RISK, documented rather than "fixed" with a more
        # elaborate discriminator (a review pass looked for one and did not
        # find a cheap one): `id()` is only guaranteed unique among objects
        # simultaneously alive within ONE process's address space. Inside
        # the child, `get_engine()`'s discard-and-rebuild path frees the
        # INHERITED (stale) `dbapi_connection` wrapper -- via `dispose(
        # close=False)` dropping the pool's last reference, then CPython's
        # allocator reclaiming it -- strictly before the rebuilt engine's
        # own first connect allocates its replacement. The replacement can
        # therefore legally land on the just-freed block and get the SAME
        # `id()`, which would fail this specific assertion despite the
        # rebuild being entirely correct: a false-failure (flaky red), not
        # a false-pass. Not reproduced in 25 forced trials on this
        # platform/Python version, so left in place as the cheapest
        # available corroborating signal -- `engine_id`, `dataset_names`,
        # and `integrity_check` above are what actually prove freshness
        # and correctness and do not share this risk (a brand-new `Engine`
        # Python object is not freed-and-reallocated mid-call the way the
        # short-lived DBAPI connection wrapper is). If this line ever
        # flakes, that is the signal to replace it with a discriminator
        # that does not depend on CPython's allocator reuse behaviour
        # (e.g. a monotonic counter tagged onto each DBAPI connection via
        # its own `connect` listener) rather than to loosen the test.
        assert result["dbapi_id"] != parent_dbapi_id
        # The child reads correct data through its rebuilt connection.
        assert result["dataset_names"] == ["tank/fork"]
        # ...and the rebuilt connection is genuinely sound, not merely
        # usable.
        assert result["integrity_check"] == "ok"

        # The parent still works after the child forked, discarded the
        # inherited pool, and rebuilt its own.
        with parent_engine.connect() as conn:
            names = [
                row[0]
                for row in conn.exec_driver_sql("SELECT name FROM datasets").fetchall()
            ]
        assert names == ["tank/fork"]

    def test_negative_control_bypassing_pid_check_reuses_parents_connection(
        self, file_db_url, tmp_path
    ):
        """Without 5b's pid check, a forked child that reaches the
        engine/pool at all reuses the PARENT's engine object and the exact
        same DBAPI connection object -- with no error of any kind. This is
        what makes the positive test above meaningful: it demonstrates the
        actual hazard `get_engine()`'s pid check exists to prevent, rather
        than merely asserting a property that happened to hold anyway.
        """
        parent_engine = self._seed(file_db_url)
        with parent_engine.connect() as conn:
            parent_dbapi_id = id(conn.connection.dbapi_connection)

        result_path = tmp_path / "fork_negative_control.json"
        ctx = multiprocessing.get_context("fork")
        proc = ctx.Process(
            target=_fork_child_use_inherited_engine_directly,
            args=(file_db_url, str(result_path)),
        )
        proc.start()
        proc.join(timeout=30)
        # No error of any kind -- the corruption hazard is silent, not an
        # exception.
        assert proc.exitcode == 0

        result = json.loads(result_path.read_text())

        # The cached tuple's owner_pid is still the PARENT's pid -- the
        # child never updated it, because it never went through
        # get_engine().
        assert result["owner_pid_seen"] == os.getpid()
        # Same engine object (literally, not just equal) --...
        assert result["engine_id"] == id(parent_engine)
        # ...and the same underlying DBAPI connection, reused silently.
        assert result["dbapi_id"] == parent_dbapi_id
        # The child can still read through it -- nothing about this fails
        # loudly, which is precisely the danger.
        assert result["dataset_names"] == ["tank/fork"]

    def test_dispose_all_in_forked_child_does_not_disturb_parents_connection(
        self, file_db_url, tmp_path
    ):
        """`dispose_all()`'s own pid guard, through a genuine fork rather
        than the whitebox manufactured-pid technique
        `TestDbDisposeAllPidGuard` uses. The child calls `dispose_all()`
        DIRECTLY, with no prior `get_engine()` call of its own -- the only
        cache entry it sees at all is the one inherited from the parent.
        An earlier, pid-unaware `dispose_all` always passed `close=True`;
        run against an inherited entry in a real forked child, that closes
        a DBAPI connection the parent still holds and is using.
        """
        parent_engine = self._seed(file_db_url)

        result_path = tmp_path / "fork_dispose_all.json"
        ctx = multiprocessing.get_context("fork")
        proc = ctx.Process(
            target=_fork_child_dispose_all, args=(file_db_url, str(result_path))
        )
        proc.start()
        proc.join(timeout=30)
        assert proc.exitcode == 0

        result = json.loads(result_path.read_text())
        # The child's own (inherited-then-discarded) cache is empty...
        assert result["cache_emptied"] is True

        # ...and the PARENT's engine/connection is unaffected: if the
        # child's dispose_all() had run the close path against the
        # inherited entry, this read would fail with
        # `ProgrammingError: Cannot operate on a closed database`.
        with parent_engine.connect() as conn:
            names = [
                row[0]
                for row in conn.exec_driver_sql("SELECT name FROM datasets").fetchall()
            ]
        assert names == ["tank/fork"]


@pytest.mark.unit
class TestDbWalConcurrency:
    def test_concurrent_write_while_reader_session_open_succeeds(self, file_db_url):
        writer_engine = make_engine(file_db_url)
        Base.metadata.create_all(writer_engine)
        with session_for_engine(writer_engine) as session:
            session.add(
                Dataset(name="tank/a", frequency_seconds=3600, frequency_literal="1h")
            )

        reader_engine = make_engine(file_db_url, readonly=True)
        try:
            with session_for_engine(reader_engine, readonly=True) as reader_session:
                # A live read, held open for the duration of the block
                # below -- this is the state under WAL that must not block
                # a concurrent writer.
                _ = reader_session.query(Dataset).all()

                writer2 = make_engine(file_db_url)
                try:
                    with session_for_engine(writer2) as writer_session:
                        writer_session.add(
                            Dataset(
                                name="tank/b",
                                frequency_seconds=7200,
                                frequency_literal="2h",
                            )
                        )
                finally:
                    writer2.dispose()
        finally:
            reader_engine.dispose()

        with writer_engine.connect() as conn:
            names = sorted(
                row[0] for row in conn.exec_driver_sql("SELECT name FROM datasets").fetchall()
            )
        assert names == ["tank/a", "tank/b"]
        writer_engine.dispose()

    def test_reader_reads_fine_while_a_writer_holds_the_begin_immediate_lock(
        self, file_db_url
    ):
        """The other direction from the test above: a WRITER holds its
        `BEGIN IMMEDIATE` write lock open (uncommitted), and a concurrent
        reader must still succeed -- proving `_install_begin_immediate`
        (the generation-race fix) did not regress "readers never block" for
        writers, only serialise writers against each other.
        """
        writer_engine = make_engine(file_db_url)
        Base.metadata.create_all(writer_engine)
        with session_for_engine(writer_engine) as session:
            session.add(
                Dataset(name="tank/a", frequency_seconds=3600, frequency_literal="1h")
            )
        writer_engine.dispose()

        held_writer = make_engine(file_db_url)
        reader_engine = make_engine(file_db_url, readonly=True)
        try:
            with Session(held_writer) as holding_session:
                # First statement on a file-backed writer engine issues
                # `BEGIN IMMEDIATE` -- takes SQLite's write lock -- and it
                # stays open because this session is never committed.
                holding_session.execute(
                    text("INSERT INTO datasets "
                         "(name, recursive, frequency_seconds, enabled) "
                         "VALUES ('tank/held', 0, 60.0, 1)")
                )

                # A concurrent reader must complete without waiting on
                # `busy_timeout` at all -- readers under WAL are never
                # blocked by a live writer transaction.
                start = time.monotonic()
                with session_for_engine(reader_engine, readonly=True) as reader_session:
                    names = sorted(d.name for d in reader_session.query(Dataset).all())
                elapsed = time.monotonic() - start

                # Only the already-committed row is visible -- the
                # writer's uncommitted INSERT is invisible to the reader,
                # as WAL snapshot isolation requires.
                assert names == ["tank/a"]
                # Generous bound: a genuinely non-blocking read is
                # near-instant; a busy_timeout wait would be seconds.
                assert elapsed < 1.0

                holding_session.rollback()
        finally:
            held_writer.dispose()
            reader_engine.dispose()


def _run_and_capture(target, *args, **kwargs):
    """Runs `target` in a new `threading.Thread`, capturing any exception
    it raises so the calling test can re-raise it on the main thread
    (pytest never sees an exception raised inside a bare `threading.Thread`
    -- it would just print to stderr and the test would pass regardless).
    Returns the started, not-yet-joined `Thread`.
    """
    outcome = {}

    def _wrapper():
        try:
            target(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            outcome["exception"] = exc

    thread = threading.Thread(target=_wrapper)
    thread.outcome = outcome
    thread.start()
    return thread


def _join_and_reraise(thread, timeout=10):
    thread.join(timeout=timeout)
    assert not thread.is_alive(), "thread did not finish within timeout"
    if "exception" in thread.outcome:
        raise thread.outcome["exception"]


@pytest.mark.integration
class TestDbGenerationRace:
    """The generation-race fix: `_install_begin_immediate` makes every
    file-backed writer transaction start as `BEGIN IMMEDIATE`, closing a
    silent lost-update on `GlobalSettings.generation` (`db.py`'s own
    docstring has the full measured writeup). Two concurrent
    `zfsbackup-config` writers both reading generation N and both writing
    N+1 means items 15/17 -- which poll `generation` to detect "config
    changed" -- never notice the second save happened at all.

    Every test here uses a FILE URL with two independent writer
    connections/engines -- an in-memory engine uses `StaticPool`, which
    hands the same DBAPI connection to every checkout, so a second
    concurrent writer session on `:memory:` cannot exist as SQLite would
    reject it (`cannot start a transaction within a transaction`) and there
    would be nothing to serialise there anyway (one connection, one
    process). A guard test built on `:memory:` would pass vacuously.
    """

    def _seed(self, url, name="tank/base"):
        engine = make_engine(url)
        try:
            Base.metadata.create_all(engine)
            with session_for_engine(engine) as session:
                save_config(session, _minimal_backup_config(name=name))
        finally:
            engine.dispose()

    def _read_generation(self, url):
        """A throwaway `make_engine()` call used only to read the current
        generation, disposed explicitly. Anonymous `make_engine(url)`
        calls that are never disposed are exactly the leak class this
        method exists to avoid -- `engine.dispose()` releases the pooled
        DBAPI connection deterministically; relying on garbage collection
        to do it eventually is what produces the `ResourceWarning:
        unclosed database` that can surface in an unrelated, later test's
        output once the GC finally runs.
        """
        engine = make_engine(url)
        try:
            with session_for_engine(engine) as s:
                return s.get(GlobalSettings, 1).generation
        finally:
            engine.dispose()

    def test_serialized_interleave_second_writer_reads_incremented_generation(
        self, file_db_url
    ):
        """Two writers, deliberately interleaved via `threading.Event`s so
        writer A's transaction is still open (holding SQLite's write lock)
        when writer B starts. With `BEGIN IMMEDIATE` in force, B's own
        first statement blocks until A commits, then B reads the
        POST-A-commit generation -- N+1, never the stale N -- and writes
        N+2. The final generation is base+2, proving no update was lost.
        """
        self._seed(file_db_url, name="tank/base")
        base_generation = self._read_generation(file_db_url)

        a_holds_lock = threading.Event()
        b_read_generation = {}

        def writer_a():
            with session_scope(file_db_url) as s:
                save_config(s, _minimal_backup_config(name="tank/a"))
                # The DELETE/INSERT sequence above has already forced
                # BEGIN IMMEDIATE to fire -- the write lock is held from
                # here until this `with` block exits (commits).
                a_holds_lock.set()
                time.sleep(0.3)

        def writer_b():
            a_holds_lock.wait(timeout=5)
            with session_scope(file_db_url) as s:
                # Blocks here (busy_timeout, default 5000ms > A's 0.3s
                # hold) until A commits, then reads A's committed value.
                b_read_generation["value"] = s.get(GlobalSettings, 1).generation
                save_config(s, _minimal_backup_config(name="tank/b"))

        thread_a = _run_and_capture(writer_a)
        thread_b = _run_and_capture(writer_b)
        _join_and_reraise(thread_a)
        _join_and_reraise(thread_b)

        assert b_read_generation["value"] == base_generation + 1

        with session_scope(file_db_url, readonly=True) as s:
            final = load_config(s)
            final_generation = s.get(GlobalSettings, 1).generation

        assert final.datasets[0].name == "tank/b"  # B committed last
        assert final_generation == base_generation + 2

    def test_second_writer_fails_with_database_is_locked_past_busy_timeout(
        self, file_db_url
    ):
        """The other observable half of the fix: if the first writer holds
        the lock past the second's `busy_timeout`, the second fails LOUDLY
        with `database is locked` rather than silently interleaving.
        Overrides writer B's `busy_timeout` down to 100ms (via the raw
        DBAPI cursor, bypassing SQLAlchemy's autobegin so the override
        itself does not trigger `BEGIN IMMEDIATE` before it can take
        effect) so this test does not need to wait out the real 5000ms
        default.
        """
        self._seed(file_db_url, name="tank/base")

        engine_a = make_engine(file_db_url)
        engine_b = make_engine(file_db_url)
        try:
            conn_a = engine_a.connect()
            # Any statement begins the SQLAlchemy-level transaction, which
            # fires the `begin` listener -> `BEGIN IMMEDIATE` -> takes the
            # write lock.
            conn_a.execute(text("SELECT 1"))

            conn_b = engine_b.connect()
            raw_cursor = conn_b.connection.dbapi_connection.cursor()
            raw_cursor.execute("PRAGMA busy_timeout=100")
            raw_cursor.close()

            with pytest.raises(OperationalError, match="database is locked"):
                conn_b.execute(text("SELECT 1"))

            conn_a.rollback()
            conn_b.close()
            conn_a.close()
        finally:
            engine_a.dispose()
            engine_b.dispose()

    def test_negative_control_without_begin_immediate_loses_an_update(
        self, file_db_url, mocker
    ):
        """Patches `_install_begin_immediate` out to a no-op BEFORE either
        writer engine is built, reproducing the pre-fix race the two tests
        above guard against: both writers read the SAME (stale) generation
        and both commit `previous + 1` -- the final generation reflects
        only ONE increment despite TWO writes, which is exactly the silent
        lost update items 15/17 would never notice. Without this negative
        control, the guard tests above could be passing for the wrong
        reason (or testing nothing) rather than actually depending on
        `_install_begin_immediate` -- the same reasoning that makes
        `TestDbForkSafety`'s negative control load-bearing.
        """
        mocker.patch.object(store_db, "_install_begin_immediate")

        self._seed(file_db_url, name="tank/base")
        base_generation = self._read_generation(file_db_url)

        a_read_generation = {}
        b_read_generation = {}
        a_has_read = threading.Event()
        b_has_read = threading.Event()

        def writer_a():
            with session_scope(file_db_url) as s:
                a_read_generation["value"] = s.get(GlobalSettings, 1).generation
                a_has_read.set()
                # Wait for B to also read before either of us writes --
                # this is the race window `BEGIN IMMEDIATE` exists to
                # close. Without it, a bare SELECT takes no lock and no
                # snapshot, so both threads can freely interleave here.
                b_has_read.wait(timeout=5)
                save_config(s, _minimal_backup_config(name="tank/a"))

        def writer_b():
            a_has_read.wait(timeout=5)
            with session_scope(file_db_url) as s:
                b_read_generation["value"] = s.get(GlobalSettings, 1).generation
                b_has_read.set()
                save_config(s, _minimal_backup_config(name="tank/b"))

        thread_a = _run_and_capture(writer_a)
        thread_b = _run_and_capture(writer_b)
        _join_and_reraise(thread_a)
        _join_and_reraise(thread_b)

        # Both writers saw the same, stale generation -- the race
        # precondition the guard exists to prevent.
        assert a_read_generation["value"] == base_generation
        assert b_read_generation["value"] == base_generation

        with session_scope(file_db_url, readonly=True) as s:
            final_generation = s.get(GlobalSettings, 1).generation

        # THE BUG: only ONE increment recorded despite TWO writes -- a
        # generation-based poller (items 15/17) would never notice the
        # second config was ever saved. Contrast with the positive guard
        # test above, where the equivalent final value is base+2.
        assert final_generation == base_generation + 1


@pytest.mark.integration
@pytest.mark.slow
class TestDbNestedWriterDeadlock:
    """`_install_begin_immediate`'s write lock is taken at the FIRST
    STATEMENT of any transaction -- not the first write -- so it is held
    by a writer session that never writes anything at all, and a SECOND
    writer connection opened while the first is still live cannot make
    progress: no thread is waiting to release anything, so this is a
    guaranteed `busy_timeout` stall then a hard failure, not a race (see
    `session_for_engine`'s docstring). Single-threaded throughout --
    deadlock, not a race, needs no `threading`.

    `busy_timeout` is shrunk to 200ms via a fixture-installed `connect`
    listener, registered AFTER `make_engine`'s own so it overrides the
    5000ms default on every connection the engine hands out -- without
    this, each `OperationalError`-raising test here would cost the real
    5s default. Marked `integration`/`slow` regardless: these are
    deliberately slow-by-construction (~200ms of guaranteed blocking
    each), not fast unit tests.
    """

    @pytest.fixture
    def fast_writer_engine(self, file_db_url):
        # Bootstrap the schema via `ensure_schema` (sequential, its own
        # phase, committed and closed before anything else touches the
        # file) rather than `Base.metadata.create_all` -- the latter
        # leaves `alembic_version` missing, which would make the FK-off
        # nested test below hit `SchemaSplitBrain` for a reason that has
        # nothing to do with the lock contention it exists to pin. This
        # way the database is genuinely at head, and a nested `ensure_
        # schema` call inside a writer session is a legitimate "already
        # migrated, no-op" case that STILL takes the write lock at its
        # very first statement (reading `alembic_version`) -- exactly the
        # "fires at the first statement, not the first write" behaviour
        # under test.
        bootstrap = make_engine(file_db_url, foreign_keys=False)
        try:
            with bootstrap.connect() as conn:
                ensure_schema(conn)
                conn.commit()
        finally:
            bootstrap.dispose()

        eng = make_engine(file_db_url)

        @event.listens_for(eng, "connect")
        def _shrink_busy_timeout(dbapi_connection, connection_record):  # noqa: ANN001
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA busy_timeout=200")
            cur.close()

        yield eng
        eng.dispose()

    def test_nested_writer_session_self_deadlocks(self, fast_writer_engine):
        with session_for_engine(fast_writer_engine) as outer:
            outer.add(
                Dataset(name="tank/outer", frequency_seconds=60, frequency_literal="1m")
            )
            # `add()` alone only stages the object in memory -- it emits
            # no SQL and therefore begins no transaction. `flush()` is
            # what actually issues the outer session's first statement,
            # which is what takes the write lock this test needs live.
            outer.flush()
            with pytest.raises(OperationalError, match="database is locked"):
                with session_for_engine(fast_writer_engine) as inner:
                    # ANY statement -- including a read -- begins the
                    # transaction and triggers BEGIN IMMEDIATE, which
                    # blocks on the outer session's still-open write lock.
                    inner.query(Dataset).all()

    def test_nested_reader_inside_writer_succeeds_immediately(
        self, fast_writer_engine, file_db_url
    ):
        # The asymmetry: a nested READER is fine even while the writer
        # genuinely holds the lock (via the same `flush()` as above),
        # because reader engines never take the write lock at all.
        reader_engine = make_engine(file_db_url, readonly=True)
        try:
            with session_for_engine(fast_writer_engine) as outer:
                outer.add(
                    Dataset(
                        name="tank/outer", frequency_seconds=60, frequency_literal="1m"
                    )
                )
                outer.flush()
                start = time.monotonic()
                with session_for_engine(reader_engine, readonly=True) as inner:
                    inner.query(Dataset).all()
                elapsed = time.monotonic() - start
            assert elapsed < 0.1
        finally:
            reader_engine.dispose()

    def test_nested_fk_off_engine_self_deadlocks(self, fast_writer_engine, file_db_url):
        # The FK-off Alembic migration engine (`get_engine(url,
        # foreign_keys=False)`) is a second WRITER engine on the same
        # file -- different cache key, own pool, same write lock. Nesting
        # it inside a live writer session self-deadlocks exactly like a
        # second full writer session would. This is what `store/
        # __init__.py`'s item-8 note instructs against. The database is
        # already at head (`fast_writer_engine`'s own bootstrap), so this
        # nested `ensure_schema` call would otherwise be a harmless no-op
        # -- it still deadlocks, because `MigrationContext.configure`'s
        # own read of `alembic_version` is itself the first statement
        # that takes the lock.
        fk_off_engine = make_engine(file_db_url, foreign_keys=False)

        @event.listens_for(fk_off_engine, "connect")
        def _shrink_busy_timeout(dbapi_connection, connection_record):  # noqa: ANN001
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA busy_timeout=200")
            cur.close()

        try:
            with session_for_engine(fast_writer_engine) as outer:
                outer.add(
                    Dataset(
                        name="tank/outer", frequency_seconds=60, frequency_literal="1m"
                    )
                )
                outer.flush()
                with pytest.raises(OperationalError, match="database is locked"):
                    with fk_off_engine.connect() as conn:
                        ensure_schema(conn)
        finally:
            fk_off_engine.dispose()

    def test_sequential_fk_off_engine_then_writer_session_succeeds(self, file_db_url):
        # The documented fix: run ensure_schema + commit + close as a
        # strictly sequential phase BEFORE opening any writer session --
        # never nested. No deadlock, no shrunk timeout needed.
        fk_off_engine = make_engine(file_db_url, foreign_keys=False)
        try:
            with fk_off_engine.connect() as conn:
                ensure_schema(conn)
                conn.commit()
        finally:
            fk_off_engine.dispose()

        writer_engine = make_engine(file_db_url)
        try:
            with session_for_engine(writer_engine) as s:
                save_config(s, _minimal_backup_config(name="tank/after-migrate"))
        finally:
            writer_engine.dispose()

        reader_engine = make_engine(file_db_url, readonly=True)
        try:
            with session_for_engine(reader_engine, readonly=True) as s:
                loaded = load_config(s)
        finally:
            reader_engine.dispose()
        assert loaded.datasets[0].name == "tank/after-migrate"


@pytest.mark.integration
class TestDbReadOnlyWorkloadSerialization:
    """`BEGIN IMMEDIATE` fires at the first statement of ANY transaction,
    so a writer session that only READS still takes the exclusive write
    lock and holds it until commit/close (`session_scope`'s docstring).
    Two read-only workloads through the DEFAULT (read-write) form
    therefore serialise on that lock; the identical workloads through
    `readonly=True` do not, because reader engines never take it at all.
    """

    def _seed(self, url):
        engine = make_engine(url)
        try:
            Base.metadata.create_all(engine)
            with session_for_engine(engine) as s:
                save_config(s, _minimal_backup_config(name="tank/a"))
        finally:
            engine.dispose()

    def test_two_readonly_workloads_serialise_through_readwrite_form(
        self, file_db_url
    ):
        self._seed(file_db_url)

        a_holds = threading.Event()
        timings = {}

        def workload_a():
            with session_scope(file_db_url) as s:
                s.query(GlobalSettings).all()  # read-only WORK, writer FORM
                a_holds.set()
                time.sleep(0.3)

        def workload_b():
            a_holds.wait(timeout=5)
            start = time.monotonic()
            with session_scope(file_db_url) as s:
                s.query(GlobalSettings).all()
            timings["b_elapsed"] = time.monotonic() - start

        thread_a = _run_and_capture(workload_a)
        thread_b = _run_and_capture(workload_b)
        _join_and_reraise(thread_a)
        _join_and_reraise(thread_b)

        # B waited out (most of) A's 0.3s hold -- the two read-only
        # workloads serialised on the write lock the read-write form
        # takes even though neither one writes anything.
        assert timings["b_elapsed"] >= 0.2

    def test_two_readonly_workloads_do_not_serialise_through_readonly_true(
        self, file_db_url
    ):
        self._seed(file_db_url)

        a_holds = threading.Event()
        timings = {}

        def workload_a():
            with session_scope(file_db_url, readonly=True) as s:
                s.query(GlobalSettings).all()
                a_holds.set()
                time.sleep(0.3)

        def workload_b():
            a_holds.wait(timeout=5)
            start = time.monotonic()
            with session_scope(file_db_url, readonly=True) as s:
                s.query(GlobalSettings).all()
            timings["b_elapsed"] = time.monotonic() - start

        thread_a = _run_and_capture(workload_a)
        thread_b = _run_and_capture(workload_b)
        _join_and_reraise(thread_a)
        _join_and_reraise(thread_b)

        # B did NOT wait for A's 0.3s hold -- readers under WAL are never
        # blocked, contrasting directly with the read-write form above.
        assert timings["b_elapsed"] < 0.1


@pytest.mark.integration
class TestDbWalSidecars:
    def test_sidecars_present_while_open_and_gone_after_dispose_all(self, tmp_path):
        db_path = tmp_path / "sidecar.db"
        url = f"sqlite:///{db_path}"
        wal_path = db_path.with_name(db_path.name + "-wal")
        shm_path = db_path.with_name(db_path.name + "-shm")

        engine = get_engine(url)
        Base.metadata.create_all(engine)
        with session_for_engine(engine) as session:
            session.add(
                Dataset(name="tank/a", frequency_seconds=3600, frequency_literal="1h")
            )

        assert wal_path.exists()
        assert shm_path.exists()

        dispose_all()

        assert db_path.exists()
        assert not wal_path.exists()
        assert not shm_path.exists()


@pytest.mark.integration
class TestDbCrashDurability:
    def test_abandoning_a_connection_mid_save_leaves_previous_config_intact(
        self, file_db_url
    ):
        """Simulates an operator `^C` / process kill between `save_config`
        returning and the caller's own `commit()`: `save_config` is a
        wipe-and-reinsert that owns no transaction policy of its own
        (`session_scope`'s docstring), so abandoning the connection before
        any commit must leave the store exactly as it was before the call,
        not half-written.
        """
        engine = make_engine(file_db_url)
        Base.metadata.create_all(engine)

        good = _minimal_backup_config(name="tank/good")
        with session_for_engine(engine) as session:
            save_config(session, good)

        # A bare Session, NOT session_for_engine -- nothing here commits or
        # rolls back on our behalf; the connection is abandoned exactly as
        # a crash would abandon it.
        crashing_session = Session(engine)
        bad = _minimal_backup_config(name="tank/bad")
        save_config(crashing_session, bad)  # wipes + reinserts, no commit
        crashing_session.close()  # abandoned: implicit rollback, no commit

        with session_for_engine(engine, readonly=False) as session:
            loaded = load_config(session)

        assert loaded.datasets[0].name == "tank/good"
        engine.dispose()


@pytest.mark.unit
class TestDbSessionForEngineRollback:
    """`session_for_engine`'s writer contract: "rolls back on any
    exception" (its own docstring). `Session.close()` also rolls back
    implicitly -- which is exactly why deleting the explicit `except
    Exception: session.rollback(); raise` block from `session_for_engine`
    failed zero tests before this class existed: every behavioural
    assertion "nothing was committed" still held via `close()`'s own
    fallback. The spy below pins the CALL itself, not just its (already
    guaranteed by SQLAlchemy) side effect, so a future refactor that
    removes the explicit rollback is caught here even though its
    observable behaviour would not change today.
    """

    def test_rollback_is_called_when_the_body_raises(self, engine, mocker):
        rollback_spy = mocker.spy(Session, "rollback")

        class _Boom(Exception):
            pass

        with pytest.raises(_Boom):
            with session_for_engine(engine) as session:
                session.add(
                    Dataset(
                        name="tank/never-committed",
                        frequency_seconds=60,
                        frequency_literal="1m",
                    )
                )
                raise _Boom("simulated failure mid-transaction")

        assert rollback_spy.call_count >= 1

    def test_body_raising_leaves_nothing_committed(self, engine):
        # The behavioural half -- true regardless of the explicit
        # `rollback()` call (see class docstring), but still the actual
        # contract `session_for_engine`'s docstring promises and worth
        # pinning alongside the call-site spy above.
        class _Boom(Exception):
            pass

        with pytest.raises(_Boom):
            with session_for_engine(engine) as session:
                session.add(
                    Dataset(
                        name="tank/never-committed",
                        frequency_seconds=60,
                        frequency_literal="1m",
                    )
                )
                session.flush()
                raise _Boom("simulated failure mid-transaction")

        # A SEPARATE Session/connection, not the aborted one -- proves the
        # row never reached the database, not merely that this particular
        # Session object forgot about it.
        with Session(engine) as verify:
            assert verify.query(Dataset).count() == 0


@pytest.mark.unit
class TestDbSessionScopeWriterPath:
    """`session_scope`'s DEFAULT (`readonly=False`) branch had never been
    exercised on its own anywhere in this file -- every existing
    `session_scope` call elsewhere passes `readonly=True`. This is the
    CLI's entry point (item 10) and the only path that combines "resolve
    the engine through the pid-keyed `get_engine()` cache" with "commit
    exactly once", both at once; nothing else pins that combination.
    """

    def test_writer_session_scope_commits_and_is_visible_from_a_separate_readonly_scope(
        self, file_db_url
    ):
        setup_engine = make_engine(file_db_url)
        Base.metadata.create_all(setup_engine)
        setup_engine.dispose()

        with session_scope(file_db_url) as session:
            save_config(
                session, _minimal_backup_config(name="tank/via-session-scope")
            )

        # A SEPARATE session_scope call, resolving its OWN readonly=True
        # engine through get_engine() -- exactly how a worker process
        # would read what the CLI just wrote.
        with session_scope(file_db_url, readonly=True) as session:
            loaded = load_config(session)

        assert loaded.datasets[0].name == "tank/via-session-scope"


@pytest.mark.unit
class TestDbReadOnlySessions:
    """5c, both enforcement layers: `PRAGMA query_only=ON` (layer 1, SQLite
    itself) and the `before_flush`/`do_orm_execute` `ReadOnlySessionError`
    guard (layer 2, diagnosis). Every write attempt below also asserts the
    database file's bytes are unchanged, not just that an exception was
    raised.
    """

    @pytest.fixture
    def seeded_file_db(self, tmp_path):
        db_path = tmp_path / "seeded.db"
        url = f"sqlite:///{db_path}"
        engine = make_engine(url)
        Base.metadata.create_all(engine)
        with session_for_engine(engine) as session:
            save_config(session, _minimal_backup_config(name="tank/a"))
        engine.dispose()
        return url, db_path

    def test_orm_flush_raises_readonly_session_error(self, seeded_file_db):
        url, db_path = seeded_file_db
        before = db_path.read_bytes()

        reader_engine = make_engine(url, readonly=True)
        try:
            with pytest.raises(ReadOnlySessionError):
                with session_for_engine(reader_engine, readonly=True) as session:
                    dataset = session.query(Dataset).first()
                    dataset.frequency_seconds = 999
                    session.flush()
        finally:
            reader_engine.dispose()

        assert db_path.read_bytes() == before

    def test_bulk_delete_raises_readonly_session_error(self, seeded_file_db):
        url, db_path = seeded_file_db
        before = db_path.read_bytes()

        reader_engine = make_engine(url, readonly=True)
        try:
            with pytest.raises(ReadOnlySessionError):
                with session_for_engine(reader_engine, readonly=True) as session:
                    session.execute(delete(Dataset))
        finally:
            reader_engine.dispose()

        assert db_path.read_bytes() == before

    def test_bulk_update_raises_readonly_session_error(self, seeded_file_db):
        url, db_path = seeded_file_db
        before = db_path.read_bytes()

        reader_engine = make_engine(url, readonly=True)
        try:
            with pytest.raises(ReadOnlySessionError):
                with session_for_engine(reader_engine, readonly=True) as session:
                    session.execute(update(Dataset).values(frequency_seconds=1))
        finally:
            reader_engine.dispose()

        assert db_path.read_bytes() == before

    def test_raw_text_update_raises_operational_error(self, seeded_file_db):
        # Layer 2 (`ReadOnlySessionError`) does not recognise a raw
        # `text()` statement as DML -- only Core `update()`/`delete()`
        # constructs set `is_update`/`is_delete` on the event state. Layer
        # 1 (`PRAGMA query_only=ON`) is what actually stops this one, and
        # it surfaces as a bare `OperationalError`.
        url, db_path = seeded_file_db
        before = db_path.read_bytes()

        reader_engine = make_engine(url, readonly=True)
        try:
            with pytest.raises(OperationalError):
                with session_for_engine(reader_engine, readonly=True) as session:
                    session.execute(text("UPDATE datasets SET frequency_seconds=999"))
        finally:
            reader_engine.dispose()

        assert db_path.read_bytes() == before

    def test_readonly_session_still_serves_load_config(self, seeded_file_db):
        # The regression guard against over-blocking: a read-only session
        # must still be able to do the one thing it exists for. Uses
        # `session_scope` (not `session_for_engine`) deliberately -- this
        # is the entry point a real worker process would call, and it
        # resolves its own `readonly=True` engine through `get_engine()`
        # rather than being handed one.
        url, _db_path = seeded_file_db

        with session_scope(url, readonly=True) as session:
            loaded = load_config(session)

        assert loaded.datasets[0].name == "tank/a"


@pytest.mark.integration
class TestDbChmodReadOnlyDatabase:
    """Item 6's permissions requirement: `readonly=True` is an accident
    guard on database *content* (`PRAGMA query_only=ON`), not a statement
    that this process needs no filesystem write access at all -- see the
    `db.py` module docstring's "Read-only is an accident guard, not a
    privilege boundary" section. Touches real file permissions, hence
    `integration` rather than `unit`.
    """

    def test_chmod_444_before_first_wal_open_fails_even_readonly(self, tmp_path):
        """The realistic item-6 shape: a `config.db` provisioned with the
        wrong permissions before it has EVER been opened in WAL mode (the
        journal mode this module always tries to set). SQLite's
        `PRAGMA journal_mode=WAL` has to actually rewrite the on-disk
        journal-mode header the first time, which needs write access to
        the main db file -- and fails loudly even though the caller only
        asked to read.
        """
        db_path = tmp_path / "never_written.db"
        # A bare sqlite3 connection, bypassing db.py entirely, so the file
        # is created with SQLite's own default (non-WAL) journal mode --
        # this module has never touched it.
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.commit()
        assert conn.execute("PRAGMA journal_mode").fetchone() == ("delete",)
        conn.close()

        os.chmod(db_path, 0o444)
        try:
            url = f"sqlite:///{db_path}"
            reader = make_engine(url, readonly=True)

            # SQLAlchemy's pool has no cleanup path for a raw DBAPI
            # connection whose FIRST-EVER `connect` event listener raises:
            # `_ConnectionRecord.__init__`'s own `__connect()` call (which
            # is where a brand-new pool creates its first connection) is
            # NOT wrapped by `_ConnectionRecord.checkout()`'s try/except --
            # that only wraps an ALREADY-CONSTRUCTED record's
            # `get_connection()`. Confirmed empirically: neither the
            # pool's `invalidate` nor `close` events fire on this path, so
            # `reader.dispose()` alone does not close the connection this
            # test's `db.py` pragma listener opened just before raising --
            # it leaks as a bare `ResourceWarning: unclosed database`,
            # attributed by Python's GC to whatever test happens to be
            # running when the cyclic collector eventually reclaims it
            # (observed landing on an unrelated sibling test). Registered
            # with `insert=True` so this listener runs BEFORE `db.py`'s
            # own pragma listener and can capture the raw connection while
            # it is still reachable, then close it explicitly below.
            opened_connections = []

            @event.listens_for(reader, "connect", insert=True)
            def _capture_raw_connection(dbapi_connection, connection_record):  # noqa: ANN001
                opened_connections.append(dbapi_connection)

            try:
                with pytest.raises(
                    OperationalError, match="readonly database"
                ):
                    with reader.connect():
                        pass
            finally:
                reader.dispose()
                for raw_conn in opened_connections:
                    raw_conn.close()
        finally:
            os.chmod(db_path, 0o644)  # so tmp_path cleanup can remove it

    def test_chmod_444_after_wal_already_set_opens_fine_readonly(self, tmp_path):
        """The documented exception to the rule above, pinned so nobody
        "fixes" the docstring's precondition note back into an
        unconditional claim: a file already migrated to WAL mode by a
        writer BEFORE the `chmod` opens fine read-only even at `chmod
        444`, because re-affirming an already-set `journal_mode=WAL`
        requires no write.
        """
        db_path = tmp_path / "already_wal.db"
        url = f"sqlite:///{db_path}"
        writer = make_engine(url)
        Base.metadata.create_all(writer)
        writer.dispose()

        os.chmod(db_path, 0o444)
        try:
            reader = make_engine(url, readonly=True)
            try:
                with reader.connect() as conn:
                    assert (
                        conn.exec_driver_sql("PRAGMA journal_mode").scalar()
                        == "wal"
                    )
            finally:
                reader.dispose()
        finally:
            os.chmod(db_path, 0o644)

"""TLS/mTLS plan M5 + M6 (store half): schema, migration and mapper.

M5 added the identity and trust tables (`host_identity`, `clients`,
`trusted_clients`, `enrollment_token`, `trust_state`), a per-destination
`server_fingerprint`, the operator's `client_cert_file`/`client_key_file`, and
dropped `global_settings.client_id_file`, all in Alembic revision
`7c3e9d51b2a8` (revising `a1c28e1b3f34`). M6 taught the mapper to load/save the
new fields, `validate_config` to refuse what the schema cannot hold,
`ConfigSnapshot` to carry `client_id`, and `save_config` to wipe ONLY the six
config tables so an `import` keeps the client id and every pinned certificate.

What is covered here (the M4 helper matrix is in `test_zfsbackup_config.py`):

- every new CHECK constraint, one accept and one reject case, run against BOTH
  `create_all` and the real migration (the migration hand-adds two CHECKs, so a
  drift between the two would otherwise go unseen);
- the `trusted_clients` -> `clients` foreign key, and `client_id` being
  non-unique across fingerprints;
- the upgrade from `a1c28e1b3f34` with real data, the downgrade and a second
  upgrade, FK-clean and `user_version`-preserving;
- the mapper round trip, each new `validate_config` rejection,
  `ConfigSnapshot.client_id`;
- `import` and `write_config` leaving `host_identity` and the trust tables
  intact, and the `CONFIG_TABLES` / `PRESERVED_TABLES` classification.

Mutation checks (reverting the fix fails the named count in THIS file; each was
run by patching the product at pytest start-up, never by editing it):

- `save_config` also deleting `HostIdentity`: 8 tests fail (6 behavioural, 2
  classification); also deleting all six preserved tables: 13.
- the hand-added `ck_destinations_server_fingerprint_hex` missing from the
  migration script: 17 fail (all `migrated` variants plus the upgrade tests;
  the `create_all` variants stay green, which is the point of running both).
- the hand-added `ck_global_settings_client_cert_key_together` missing: 8 fail.
- `validate_config` no longer refusing non-https URLs: 9 fail.
- `load_config_snapshot` not returning `client_id`: 4 fail.
"""

import subprocess
import sys
import sqlite3
from pathlib import Path

import pytest
import yaml
from alembic import command
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
    RetentionRule,
    client_id_problem,
    normalize_fingerprint,
)
from zfsbackup.config.store import (
    Base,
    ConfigSnapshot,
    dispose_all,
    ensure_schema,
    import_yaml,
    load_config,
    load_config_snapshot,
    make_engine,
    open_config_connection,
    open_config_session,
    read_config,
    resolve_config_path,
    save_config,
    upgrade_schema,
    validate_config,
    write_config,
)
from zfsbackup.config.store import migrate as migrate_module
from zfsbackup.config.store import models
from zfsbackup.config.store import paths as store_paths
from zfsbackup.config.store.mapper import CONFIG_TABLES, PRESERVED_TABLES

pytestmark = pytest.mark.unit

BASELINE = "a1c28e1b3f34"
TLS_HEAD = "7c3e9d51b2a8"

HEX = "0123456789abcdef" * 4          # a canonical 64-hex value
HEX2 = "fedcba9876543210" * 4
HEX3 = "ab" * 32
NEW_TABLES = ("host_identity", "clients", "trusted_clients", "revoked_fingerprints",
              "enrollment_token", "trust_state")


# ---------------------------------------------------------------------------
# fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture(params=["create_all", "migrated"])
def schema_engine(request, tmp_path):
    """An FK-enforcing engine over the schema built BOTH ways: `create_all`
    from the ORM models and the real Alembic migration chain. Every CHECK test
    runs against each, so a constraint that exists in `models.py` but is
    missing from the migration script (or the reverse) fails here."""
    url = f"sqlite:///{tmp_path / 'schema.db'}"
    if request.param == "create_all":
        engine = make_engine(url)
        Base.metadata.create_all(engine)
    else:
        raw = make_engine(url, foreign_keys=False)
        with raw.connect() as conn:
            ensure_schema(conn)
            conn.commit()
        raw.dispose()
        engine = make_engine(url, foreign_keys=True)
    yield engine
    engine.dispose()


@pytest.fixture
def mem_session():
    engine = make_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def run(engine, sql, **params):
    with engine.begin() as conn:
        conn.execute(text(sql), params)


def accepted(engine, sql, **params):
    run(engine, sql, **params)


def rejected(engine, sql, constraint, **params):
    """The statement is refused BY the named constraint (not merely refused)."""
    with pytest.raises(IntegrityError, match=constraint):
        run(engine, sql, **params)


INS_HOST = "INSERT INTO host_identity (id, client_id, created_at) VALUES (:id, :cid, 1.0)"
INS_CLIENT = "INSERT INTO clients (client_id, created_at) VALUES (:cid, 1.0)"
INS_TRUSTED = (
    "INSERT INTO trusted_clients (fingerprint, client_id, cert_pem, not_after, "
    "enrolled_at, enrolled_via, predecessor_fingerprint) "
    "VALUES (:fp, :cid, 'PEM', 9e9, 1.0, :via, :pred)"
)
INS_REVOKED = (
    "INSERT INTO revoked_fingerprints (fingerprint, client_id, revoked_at, reason) "
    "VALUES (:fp, :cid, 1.0, :reason)"
)
INS_REVOKED_SUPERSEDED = (
    "INSERT INTO revoked_fingerprints "
    "(fingerprint, client_id, revoked_at, reason, superseded_by) "
    "VALUES (:fp, :cid, 1.0, :reason, :sup)"
)
INS_TOKEN = (
    "INSERT INTO enrollment_token (id, secret_sha256, server_fingerprint, "
    "created_at, expires_at, client_id, replace, consumed_fingerprint) "
    "VALUES (:id, :secret, :sfp, :created, :expires, :cid, :replace, :cfp)"
)
INS_DEST = "INSERT INTO destinations (name, url, server_fingerprint) VALUES (:n, 'https://h', :fp)"
INS_SETTINGS = (
    "INSERT INTO global_settings (id, snapshot_prefix, check_interval_seconds, "
    "api_host, api_port, dry_run, client_cert_file, client_key_file, generation) "
    "VALUES (1, 'autosnap', 60, '127.0.0.1', 8080, 0, :cert, :key, 0)"
)


def token_params(**over):
    base = dict(id=1, secret=HEX, sfp=HEX2, created=100.0, expires=200.0,
                cid=None, replace=0, cfp=None)
    base.update(over)
    return base


def revoked_params(**over):
    base = dict(fp=HEX, cid="alice", reason="revoked")
    base.update(over)
    return base


def superseded_params(**over):
    base = dict(fp=HEX, cid="alice", reason="retired", sup=HEX2)
    base.update(over)
    return base


def trusted_params(**over):
    base = dict(fp=HEX, cid="alice", via="token", pred=None)
    base.update(over)
    return base


def minimal_config(**over) -> BackupConfig:
    cfg = BackupConfig(
        datasets=[DatasetConfig(
            name="tank/data", frequency=Duration("1h"),
            retention_rules=[RetentionRule(Duration("1d"), Duration("30d"))],
        )],
        snapshot_prefix="autosnap",
    )
    for key, value in over.items():
        setattr(cfg, key, value)
    return cfg


def tls_config() -> BackupConfig:
    cfg = minimal_config()
    cfg.datasets[0].remote = [RemoteDatasetConfig(destination="offsite")]
    cfg.destinations = {
        "offsite": Destination("https://offsite.example:8443", server_fingerprint=HEX),
        "unpinned": Destination("https://other.example"),
    }
    cfg.client_cert_file = Path("/etc/zfsbackup/client.crt")
    cfg.client_key_file = Path("/etc/zfsbackup/client.key")
    return cfg


# ===========================================================================
# M5: CHECK constraints (accept + reject, on create_all AND the migration)
# ===========================================================================

GOOD_IDS = ["a", "A", "0", "0a", "host-1234abcd", "a.b_c-d", "a.", "a-", "a_", "x" * 64]
BAD_IDS = [
    ("", "empty"), ("x" * 65, "65 chars"), (".a", "leading dot"), (".", "dot"),
    ("..", "dotdot"), ("a/b", "slash"), ("a b", "space"), ("a@b", "at"),
    ("a#b", "hash"), ("a\nb", "newline"), ("abc\n", "trailing newline"),
    ("café", "non-ascii"), ("日本", "cjk"), ("a:b", "colon"),
    ("a%b", "percent"), ("a*b", "star"),
    # the first character must be [A-Za-z0-9]: '-' (an option when it reaches a command
    # line) and '_' are refused as well as '.'
    ("-a", "leading hyphen"), ("_a", "leading underscore"), ("-", "hyphen"),
    ("_", "underscore"), ("--x", "double hyphen"), (".-a", "dot then hyphen"),
]


class TestHostIdentityChecks:
    def test_singleton_accepts_id_1(self, schema_engine):
        accepted(schema_engine, INS_HOST, id=1, cid="host-1")

    @pytest.mark.parametrize("bad_id", [0, 2, -1, 100])
    def test_singleton_rejects_any_other_id(self, schema_engine, bad_id):
        rejected(schema_engine, INS_HOST, "ck_host_identity_singleton", id=bad_id, cid="host-1")

    def test_a_second_row_is_refused_by_the_primary_key(self, schema_engine):
        accepted(schema_engine, INS_HOST, id=1, cid="host-1")
        with pytest.raises(IntegrityError, match="UNIQUE|PRIMARY"):
            run(schema_engine, INS_HOST, id=1, cid="host-2")

    @pytest.mark.parametrize("cid", GOOD_IDS)
    def test_grammar_accepts(self, schema_engine, cid):
        accepted(schema_engine, INS_HOST, id=1, cid=cid)

    @pytest.mark.parametrize("cid, why", BAD_IDS, ids=[w for _c, w in BAD_IDS])
    def test_grammar_rejects(self, schema_engine, cid, why):
        rejected(schema_engine, INS_HOST, "ck_host_identity_client_id_grammar", id=1, cid=cid)

    def test_client_id_is_not_null(self, schema_engine):
        with pytest.raises(IntegrityError, match="NOT NULL"):
            run(schema_engine, "INSERT INTO host_identity (id, client_id, created_at) VALUES (1, NULL, 1.0)")

    def test_updated_at_is_optional(self, schema_engine):
        accepted(schema_engine, INS_HOST, id=1, cid="host-1")
        with schema_engine.connect() as conn:
            assert conn.execute(text("SELECT updated_at FROM host_identity")).scalar() is None

    def test_updating_to_a_bad_id_is_refused_too(self, schema_engine):
        accepted(schema_engine, INS_HOST, id=1, cid="host-1")
        rejected(schema_engine, "UPDATE host_identity SET client_id = '.evil'",
                 "ck_host_identity_client_id_grammar")


class TestClientsChecks:
    @pytest.mark.parametrize("cid", GOOD_IDS)
    def test_grammar_accepts(self, schema_engine, cid):
        accepted(schema_engine, INS_CLIENT, cid=cid)

    @pytest.mark.parametrize("cid, why", BAD_IDS, ids=[w for _c, w in BAD_IDS])
    def test_grammar_rejects(self, schema_engine, cid, why):
        rejected(schema_engine, INS_CLIENT, "ck_clients_client_id_grammar", cid=cid)

    def test_a_duplicate_id_is_refused(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")
        with pytest.raises(IntegrityError, match="UNIQUE|PRIMARY"):
            run(schema_engine, INS_CLIENT, cid="alice")

    def test_the_tombstone_and_label_are_optional(self, schema_engine):
        accepted(schema_engine, "INSERT INTO clients (client_id, label, created_at, revoked_at) "
                                "VALUES ('a', 'lbl', 1.0, 2.0)")
        accepted(schema_engine, INS_CLIENT, cid="b")


class TestTrustedClientsChecks:
    @pytest.fixture(autouse=True)
    def _client(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")

    def test_accepts_a_well_formed_row(self, schema_engine):
        accepted(schema_engine, INS_TRUSTED, **trusted_params())

    @pytest.mark.parametrize("fp, why", [
        (HEX[:-1], "63 chars"), (HEX + "0", "65 chars"), (HEX.upper(), "uppercase"),
        ("g" * 64, "non-hex"), (":".join(HEX[i:i + 2] for i in range(0, 64, 2)), "colon form"),
        ("", "empty"), (" " + HEX[1:], "leading space"), (HEX[:63] + "\n", "newline"),
    ])
    def test_fingerprint_must_be_64_lowercase_hex(self, schema_engine, fp, why):
        rejected(schema_engine, INS_TRUSTED, "ck_trusted_clients_fingerprint_hex",
                 **trusted_params(fp=fp))

    @pytest.mark.parametrize("via", ["token", "rotate"])
    def test_enrolled_via_accepts_the_two_paths(self, schema_engine, via):
        accepted(schema_engine, INS_TRUSTED, **trusted_params(via=via))

    @pytest.mark.parametrize("via", ["admin", "", "TOKEN", "enroll", "Rotate"])
    def test_enrolled_via_rejects_anything_else(self, schema_engine, via):
        rejected(schema_engine, INS_TRUSTED, "ck_trusted_clients_enrolled_via_known",
                 **trusted_params(via=via))

    def test_predecessor_is_optional_and_may_be_well_formed(self, schema_engine):
        accepted(schema_engine, INS_TRUSTED, **trusted_params(fp=HEX, pred=None))
        accepted(schema_engine, INS_TRUSTED, **trusted_params(fp=HEX2, via="rotate", pred=HEX))

    @pytest.mark.parametrize("pred", [HEX[:-1], HEX.upper(), "zz" * 32, ""])
    def test_a_malformed_predecessor_is_refused(self, schema_engine, pred):
        rejected(schema_engine, INS_TRUSTED, "ck_trusted_clients_predecessor_fingerprint_hex",
                 **trusted_params(via="rotate", pred=pred))

    def test_a_predecessor_need_not_be_a_stored_row(self, schema_engine):
        # No FK on it: the predecessor is retired (deleted) after first use.
        accepted(schema_engine, INS_TRUSTED, **trusted_params(fp=HEX2, via="rotate", pred=HEX3))


class TestTrustedClientsForeignKeyAndIndex:
    def test_a_pin_for_an_unknown_client_is_refused(self, schema_engine):
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            run(schema_engine, INS_TRUSTED, **trusted_params(cid="ghost"))

    def test_a_pin_for_a_known_client_is_accepted(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")
        accepted(schema_engine, INS_TRUSTED, **trusted_params())

    def test_deleting_a_client_that_still_has_pins_is_refused(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")
        accepted(schema_engine, INS_TRUSTED, **trusted_params())
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            run(schema_engine, "DELETE FROM clients WHERE client_id = 'alice'")

    def test_there_is_no_cascade_a_client_can_go_once_its_pins_are_gone(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")
        accepted(schema_engine, INS_TRUSTED, **trusted_params())
        run(schema_engine, "DELETE FROM trusted_clients")
        run(schema_engine, "DELETE FROM clients WHERE client_id = 'alice'")

    def test_the_foreign_key_targets_clients_and_has_no_action(self, schema_engine):
        fks = inspect(schema_engine).get_foreign_keys("trusted_clients")
        assert len(fks) == 1
        fk = fks[0]
        assert fk["referred_table"] == "clients"
        assert fk["constrained_columns"] == ["client_id"]
        assert fk["referred_columns"] == ["client_id"]
        assert not fk.get("options", {}).get("ondelete")

    def test_one_client_id_may_hold_several_fingerprints(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")
        for fp in (HEX, HEX2, HEX3):
            accepted(schema_engine, INS_TRUSTED, **trusted_params(fp=fp))
        with schema_engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM trusted_clients")).scalar() == 3

    def test_a_fingerprint_belongs_to_exactly_one_client(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")
        accepted(schema_engine, INS_CLIENT, cid="bob")
        accepted(schema_engine, INS_TRUSTED, **trusted_params(fp=HEX, cid="alice"))
        with pytest.raises(IntegrityError, match="UNIQUE|PRIMARY"):
            run(schema_engine, INS_TRUSTED, **trusted_params(fp=HEX, cid="bob"))

    def test_client_id_is_indexed_but_not_unique(self, schema_engine):
        indexes = {ix["name"]: ix for ix in inspect(schema_engine).get_indexes("trusted_clients")}
        assert "ix_trusted_clients_client_id" in indexes
        assert indexes["ix_trusted_clients_client_id"]["column_names"] == ["client_id"]
        assert not indexes["ix_trusted_clients_client_id"]["unique"]

    def test_not_after_and_cert_pem_are_required(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")
        with pytest.raises(IntegrityError, match="NOT NULL"):
            run(schema_engine, "INSERT INTO trusted_clients (fingerprint, client_id, cert_pem, "
                               "not_after, enrolled_at, enrolled_via) "
                               f"VALUES ('{HEX}', 'alice', NULL, 1.0, 1.0, 'token')")


class TestRevokedFingerprintsChecks:
    """`revoked_fingerprints`: the permanent record that keeps a revoked key from being
    re-enrolled. Every check runs against `create_all` AND the migration."""

    @pytest.fixture(autouse=True)
    def _client(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")

    @pytest.mark.parametrize(
        "reason", ["revoked", "successor_revoked", "client_revoked", "replaced", "retired"])
    def test_accepts_every_known_reason(self, schema_engine, reason):
        accepted(schema_engine, INS_REVOKED, **revoked_params(reason=reason))

    def test_the_reason_list_in_the_database_is_the_models_list(self, schema_engine):
        assert models.REVOCATION_REASONS == (
            "revoked", "successor_revoked", "client_revoked", "replaced", "retired")
        # The CHECK, built both ways, accepts exactly that list: the migration's hand-written
        # copy cannot drift from the model's.
        for index, reason in enumerate(models.REVOCATION_REASONS):
            accepted(schema_engine, INS_REVOKED,
                     **revoked_params(fp=f"{index:064x}", reason=reason))
        for reason in ("revoke", "successor", "succesor_revoked", "client", "replace"):
            rejected(schema_engine, INS_REVOKED, "ck_revoked_fingerprints_reason_known",
                     **revoked_params(fp="f" * 64, reason=reason))

    @pytest.mark.parametrize("reason", ["", "REVOKED", "revoke", "expired", "Retired", "retired "])
    def test_an_unknown_reason_is_refused_by_the_named_check(self, schema_engine, reason):
        rejected(schema_engine, INS_REVOKED, "ck_revoked_fingerprints_reason_known",
                 **revoked_params(reason=reason))

    def test_a_null_reason_is_refused(self, schema_engine):
        with pytest.raises(IntegrityError, match="NOT NULL"):
            run(schema_engine, INS_REVOKED, **revoked_params(reason=None))

    @pytest.mark.parametrize("fp, why", [
        (HEX[:-1], "63 chars"), (HEX + "0", "65 chars"), (HEX.upper(), "uppercase"),
        ("g" * 64, "non-hex"), ("", "empty"), (HEX + "\x00junk", "nul tail"),
        (HEX[:63] + "\x00", "nul swapped in"),
    ])
    def test_fingerprint_must_be_64_lowercase_hex_nul_safe(self, schema_engine, fp, why):
        rejected(schema_engine, INS_REVOKED, "ck_revoked_fingerprints_fingerprint_hex",
                 **revoked_params(fp=fp))

    def test_a_revocation_for_an_unknown_client_is_refused(self, schema_engine):
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            run(schema_engine, INS_REVOKED, **revoked_params(cid="ghost"))

    def test_deleting_a_client_that_has_revocations_is_refused(self, schema_engine):
        accepted(schema_engine, INS_REVOKED, **revoked_params())
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            run(schema_engine, "DELETE FROM clients WHERE client_id = 'alice'")

    def test_a_fingerprint_is_recorded_once(self, schema_engine):
        accepted(schema_engine, INS_REVOKED, **revoked_params())
        with pytest.raises(IntegrityError, match="UNIQUE|PRIMARY"):
            run(schema_engine, INS_REVOKED, **revoked_params(reason="retired"))

    def test_client_id_is_indexed_but_not_unique(self, schema_engine):
        indexes = {ix["name"]: ix for ix in inspect(schema_engine).get_indexes("revoked_fingerprints")}
        assert indexes["ix_revoked_fingerprints_client_id"]["column_names"] == ["client_id"]
        assert not indexes["ix_revoked_fingerprints_client_id"]["unique"]
        for fp in (HEX, HEX2, HEX3):
            accepted(schema_engine, INS_REVOKED, **revoked_params(fp=fp))

    def test_a_fingerprint_may_be_both_pinned_and_recorded_at_the_schema_level(self, schema_engine):
        """The two sets are disjoint by `trust.py`, not by the schema (the reason `read_trust`
        filters the revoked ones out as a backstop)."""
        accepted(schema_engine, INS_TRUSTED, **trusted_params())
        accepted(schema_engine, INS_REVOKED, **revoked_params())

    def test_superseded_by_is_nullable_and_defaults_to_null(self, schema_engine):
        accepted(schema_engine, INS_REVOKED, **revoked_params())
        with schema_engine.connect() as conn:
            assert conn.execute(text("SELECT superseded_by FROM revoked_fingerprints")).scalar() is None
        col = {c["name"]: c for c in inspect(schema_engine).get_columns("revoked_fingerprints")}
        assert col["superseded_by"]["nullable"]

    def test_superseded_by_accepts_a_canonical_fingerprint(self, schema_engine):
        accepted(schema_engine, INS_REVOKED_SUPERSEDED, **superseded_params())
        with schema_engine.connect() as conn:
            assert conn.execute(text("SELECT superseded_by FROM revoked_fingerprints")).scalar() == HEX2

    @pytest.mark.parametrize("sup, why", [
        (HEX2[:-1], "63 chars"), (HEX2 + "0", "65 chars"), (HEX2.upper(), "uppercase"),
        ("g" * 64, "non-hex"), ("", "empty"), (HEX2 + "\x00junk", "nul tail"),
        (HEX2[:63] + "\x00", "nul swapped in"),
    ])
    def test_superseded_by_must_be_64_lowercase_hex_nul_safe(self, schema_engine, sup, why):
        rejected(schema_engine, INS_REVOKED_SUPERSEDED,
                 "ck_revoked_fingerprints_superseded_by_hex", **superseded_params(sup=sup))

    def test_superseded_by_is_not_a_foreign_key(self, schema_engine):
        """The superseder is routinely absent from `trusted_clients` (retired in its own
        turn, or revoked): the column records a link, it does not reference a row."""
        fks = inspect(schema_engine).get_foreign_keys("revoked_fingerprints")
        assert [(fk["constrained_columns"], fk["referred_table"]) for fk in fks] == [
            (["client_id"], "clients")]
        accepted(schema_engine, INS_REVOKED_SUPERSEDED, **superseded_params(sup="9" * 64))

    def test_superseded_by_is_checked_on_update_too(self, schema_engine):
        accepted(schema_engine, INS_REVOKED_SUPERSEDED, **superseded_params())
        rejected(schema_engine, "UPDATE revoked_fingerprints SET superseded_by = 'abc'",
                 "ck_revoked_fingerprints_superseded_by_hex")
        rejected(schema_engine, "UPDATE revoked_fingerprints SET superseded_by = fingerprint || char(0)",
                 "ck_revoked_fingerprints_superseded_by_hex")

    def test_the_updates_are_checked_too(self, schema_engine):
        accepted(schema_engine, INS_REVOKED, **revoked_params())
        rejected(schema_engine, "UPDATE revoked_fingerprints SET reason = 'x'",
                 "ck_revoked_fingerprints_reason_known")
        rejected(schema_engine, "UPDATE revoked_fingerprints SET fingerprint = 'abc'",
                 "ck_revoked_fingerprints_fingerprint_hex")


class TestEnrollmentTokenChecks:
    def test_accepts_an_unscoped_token(self, schema_engine):
        accepted(schema_engine, INS_TOKEN, **token_params())

    @pytest.mark.parametrize("bad_id", [0, 2])
    def test_singleton(self, schema_engine, bad_id):
        rejected(schema_engine, INS_TOKEN, "ck_enrollment_token_singleton", **token_params(id=bad_id))

    @pytest.mark.parametrize("value", [HEX[:-1], HEX.upper(), "x" * 64, ""])
    def test_secret_sha256_must_be_64_lowercase_hex(self, schema_engine, value):
        rejected(schema_engine, INS_TOKEN, "ck_enrollment_token_secret_sha256_hex",
                 **token_params(secret=value))

    @pytest.mark.parametrize("value", [HEX[:-1], HEX.upper(), "x" * 64, ""])
    def test_server_fingerprint_must_be_64_lowercase_hex(self, schema_engine, value):
        rejected(schema_engine, INS_TOKEN, "ck_enrollment_token_server_fingerprint_hex",
                 **token_params(sfp=value))

    def test_expiry_must_follow_creation(self, schema_engine):
        accepted(schema_engine, INS_TOKEN, **token_params(created=100.0, expires=100.001))

    @pytest.mark.parametrize("created, expires", [(100.0, 100.0), (100.0, 99.0), (100.0, -5.0)])
    def test_expiry_not_after_creation_is_refused(self, schema_engine, created, expires):
        rejected(schema_engine, INS_TOKEN, "ck_enrollment_token_expiry_after_creation",
                 **token_params(created=created, expires=expires))

    def test_replace_with_a_client_id_is_accepted(self, schema_engine):
        accepted(schema_engine, INS_TOKEN, **token_params(cid="alice", replace=1))

    def test_replace_without_a_client_id_is_refused(self, schema_engine):
        rejected(schema_engine, INS_TOKEN, "ck_enrollment_token_replace_requires_client_id",
                 **token_params(cid=None, replace=1))

    @pytest.mark.parametrize("cid, replace", [(None, 0), ("alice", 0)])
    def test_no_replace_is_fine_with_or_without_a_client_id(self, schema_engine, cid, replace):
        accepted(schema_engine, INS_TOKEN, **token_params(cid=cid, replace=replace))

    def test_replace_defaults_to_false(self, schema_engine):
        run(schema_engine, "INSERT INTO enrollment_token (id, secret_sha256, server_fingerprint, "
                           "created_at, expires_at) "
                           f"VALUES (1, '{HEX}', '{HEX2}', 1.0, 2.0)")
        with schema_engine.connect() as conn:
            assert conn.execute(text("SELECT replace FROM enrollment_token")).scalar() == 0

    def test_consumed_fingerprint_is_optional_and_well_formed(self, schema_engine):
        accepted(schema_engine, INS_TOKEN, **token_params(cfp=HEX3))

    @pytest.mark.parametrize("value", [HEX[:-1], HEX.upper(), "", "g" * 64])
    def test_a_malformed_consumed_fingerprint_is_refused(self, schema_engine, value):
        rejected(schema_engine, INS_TOKEN, "ck_enrollment_token_consumed_fingerprint_hex",
                 **token_params(cfp=value))

    def test_the_token_client_id_is_not_a_foreign_key(self, schema_engine):
        # A scoped token may be minted for an id that is revoked/absent later;
        # the store API (M7) checks existence, not the schema.
        accepted(schema_engine, INS_TOKEN, **token_params(cid="no-such-client"))


class TestTrustStateChecks:
    def test_accepts_id_1(self, schema_engine):
        accepted(schema_engine, "INSERT INTO trust_state (id, generation) VALUES (1, 0)")

    @pytest.mark.parametrize("bad_id", [0, 2, -1])
    def test_singleton(self, schema_engine, bad_id):
        rejected(schema_engine, "INSERT INTO trust_state (id, generation) VALUES (:i, 0)",
                 "ck_trust_state_singleton", i=bad_id)

    def test_generation_is_required(self, schema_engine):
        with pytest.raises(IntegrityError, match="NOT NULL"):
            run(schema_engine, "INSERT INTO trust_state (id, generation) VALUES (1, NULL)")


class TestDestinationFingerprintCheck:
    def test_null_is_not_enrolled(self, schema_engine):
        accepted(schema_engine, INS_DEST, n="d", fp=None)

    def test_canonical_hex_is_accepted(self, schema_engine):
        accepted(schema_engine, INS_DEST, n="d", fp=HEX)

    @pytest.mark.parametrize("fp, why", [
        (HEX.upper(), "uppercase"), (HEX[:-1], "63"), (HEX + "0", "65"), ("", "empty"),
        ("z" * 64, "non-hex"), (":".join(HEX[i:i + 2] for i in range(0, 64, 2)), "colon"),
        (" " + HEX[1:], "space"),
    ])
    def test_anything_else_is_refused(self, schema_engine, fp, why):
        rejected(schema_engine, INS_DEST, "ck_destinations_server_fingerprint_hex", n="d", fp=fp)

    def test_it_is_enforced_on_update_too(self, schema_engine):
        accepted(schema_engine, INS_DEST, n="d", fp=None)
        rejected(schema_engine, "UPDATE destinations SET server_fingerprint = 'abc'",
                 "ck_destinations_server_fingerprint_hex")


class TestGlobalSettingsCertKeyCheck:
    @pytest.mark.parametrize("cert, key", [(None, None), ("/etc/c", "/etc/k")])
    def test_both_or_neither_is_accepted(self, schema_engine, cert, key):
        accepted(schema_engine, INS_SETTINGS, cert=cert, key=key)

    @pytest.mark.parametrize("cert, key", [("/etc/c", None), (None, "/etc/k")])
    def test_exactly_one_is_refused(self, schema_engine, cert, key):
        rejected(schema_engine, INS_SETTINGS, "ck_global_settings_client_cert_key_together",
                 cert=cert, key=key)

    def test_updating_to_a_half_pair_is_refused(self, schema_engine):
        accepted(schema_engine, INS_SETTINGS, cert="/etc/c", key="/etc/k")
        rejected(schema_engine, "UPDATE global_settings SET client_key_file = NULL",
                 "ck_global_settings_client_cert_key_together")

    def test_client_id_file_is_gone(self, schema_engine):
        cols = {c["name"] for c in inspect(schema_engine).get_columns("global_settings")}
        assert "client_id_file" not in cols
        assert {"client_cert_file", "client_key_file"} <= cols

    def test_the_old_checks_survived_the_table_rebuild(self, schema_engine):
        names = {c["name"] for c in inspect(schema_engine).get_check_constraints("global_settings")}
        assert {
            "ck_global_settings_singleton",
            "ck_global_settings_check_interval_positive",
            "ck_global_settings_prune_interval_positive",
            "ck_global_settings_prune_literal_requires_seconds",
            "ck_global_settings_client_cert_key_together",
        } <= names


class TestSchemaShape:
    def test_the_new_tables_exist(self, schema_engine):
        assert set(NEW_TABLES) <= set(inspect(schema_engine).get_table_names())

    def test_every_new_check_is_named(self, schema_engine):
        insp = inspect(schema_engine)
        names = {t: {c["name"] for c in insp.get_check_constraints(t)} for t in NEW_TABLES}
        assert names["host_identity"] == {
            "ck_host_identity_singleton", "ck_host_identity_client_id_grammar"}
        assert names["clients"] == {"ck_clients_client_id_grammar"}
        assert names["trusted_clients"] == {
            "ck_trusted_clients_fingerprint_hex", "ck_trusted_clients_enrolled_via_known",
            "ck_trusted_clients_predecessor_fingerprint_hex"}
        assert names["enrollment_token"] == {
            "ck_enrollment_token_singleton", "ck_enrollment_token_secret_sha256_hex",
            "ck_enrollment_token_server_fingerprint_hex",
            "ck_enrollment_token_expiry_after_creation",
            "ck_enrollment_token_replace_requires_client_id",
            "ck_enrollment_token_consumed_fingerprint_hex"}
        assert names["revoked_fingerprints"] == {
            "ck_revoked_fingerprints_fingerprint_hex", "ck_revoked_fingerprints_reason_known",
            "ck_revoked_fingerprints_superseded_by_hex"}
        assert names["trust_state"] == {"ck_trust_state_singleton"}
        assert "ck_destinations_server_fingerprint_hex" in {
            c["name"] for c in insp.get_check_constraints("destinations")}

    def test_no_new_table_starts_with_rows(self, schema_engine):
        with schema_engine.connect() as conn:
            for table in NEW_TABLES:
                assert conn.execute(text(f"SELECT count(*) FROM {table}")).scalar() == 0, table


class TestGrammarParityWithPython:
    """`client_id_problem` and the DB CHECK are two statements of one grammar
    (A7). They must agree on everything, an embedded NUL included (SQLite's
    `length()` and `GLOB` stop at a NUL; each CHECK also compares
    `length(CAST(col AS BLOB))` with `length(col)`)."""

    SAMPLES = [
        "a", "A", "0", "-", "_", "-a", "_a", "-x", "--", "a-", "a_", "0-", ".-", "a.", "a..b", "x" * 64, "x" * 65, "", ".", "..", ".a",
        "a b", "a/b", "a@b", "a#b", "a:b", "a%b", "a\nb", "abc\n", "\nabc", "a\tb",
        "café", "日本", "á", "Ａ", "a​b", "My-Host.example-0a1b2c3d",
    ]

    @pytest.mark.parametrize("cid", SAMPLES, ids=repr)
    def test_python_and_database_agree(self, schema_engine, cid):
        python_ok = client_id_problem(cid) is None
        try:
            run(schema_engine, INS_CLIENT, cid=cid)
            db_ok = True
        except IntegrityError:
            db_ok = False
        assert db_ok == python_ok, cid

    @pytest.mark.parametrize("cid", ["a\x00b", "a\x00", "ok\x00/../x"])
    def test_an_embedded_nul_is_refused_by_the_database(self, schema_engine, cid):
        assert client_id_problem(cid) is not None
        with pytest.raises(IntegrityError):
            run(schema_engine, INS_CLIENT, cid=cid)

    @pytest.mark.parametrize("fp", [HEX, HEX.upper(), HEX[:-1], "z" * 64, ""])
    def test_fingerprint_check_accepts_exactly_the_canonical_form(self, schema_engine, fp):
        try:
            canonical = normalize_fingerprint(fp) == fp
        except ValueError:
            canonical = False
        try:
            run(schema_engine, INS_DEST, n="d", fp=fp)
            db_ok = True
        except IntegrityError:
            db_ok = False
        assert db_ok == canonical

    def test_a_fingerprint_with_a_nul_tail_is_refused_by_the_database(self, schema_engine):
        with pytest.raises(IntegrityError):
            run(schema_engine, INS_DEST, n="d", fp=HEX + "\x00junk")

    # Every grammar / hex CHECK carries the `length(CAST(col AS BLOB)) =
    # length(col)` term, not only the two sampled above. One row per column,
    # each built so the NUL is the ONLY thing wrong with it.
    NUL_SITES = {
        "host_identity.client_id": (
            INS_HOST, lambda v: dict(id=1, cid=v), "ck_host_identity_client_id_grammar", "ok"),
        "clients.client_id": (
            INS_CLIENT, lambda v: dict(cid=v), "ck_clients_client_id_grammar", "ok"),
        "revoked_fingerprints.fingerprint": (
            INS_REVOKED, lambda v: revoked_params(fp=v),
            "ck_revoked_fingerprints_fingerprint_hex", HEX),
        "revoked_fingerprints.superseded_by": (
            INS_REVOKED_SUPERSEDED, lambda v: superseded_params(sup=v),
            "ck_revoked_fingerprints_superseded_by_hex", HEX2),
        "trusted_clients.fingerprint": (
            INS_TRUSTED, lambda v: trusted_params(fp=v),
            "ck_trusted_clients_fingerprint_hex", HEX),
        "trusted_clients.predecessor_fingerprint": (
            INS_TRUSTED, lambda v: trusted_params(fp=HEX2, via="rotate", pred=v),
            "ck_trusted_clients_predecessor_fingerprint_hex", HEX),
        "enrollment_token.secret_sha256": (
            INS_TOKEN, lambda v: token_params(secret=v),
            "ck_enrollment_token_secret_sha256_hex", HEX),
        "enrollment_token.server_fingerprint": (
            INS_TOKEN, lambda v: token_params(sfp=v),
            "ck_enrollment_token_server_fingerprint_hex", HEX),
        "enrollment_token.consumed_fingerprint": (
            INS_TOKEN, lambda v: token_params(cfp=v),
            "ck_enrollment_token_consumed_fingerprint_hex", HEX),
        "destinations.server_fingerprint": (
            INS_DEST, lambda v: dict(n="d", fp=v),
            "ck_destinations_server_fingerprint_hex", HEX),
    }

    @pytest.mark.parametrize("site", sorted(NUL_SITES))
    def test_every_grammar_check_refuses_an_embedded_nul(self, schema_engine, site):
        sql, params, constraint, good = self.NUL_SITES[site]
        if site.startswith(("trusted", "revoked")):
            accepted(schema_engine, INS_CLIENT, cid="alice")
        # Control: the well-formed value is accepted (the row is otherwise valid) ...
        run(schema_engine, sql, **params(good))
        with schema_engine.begin() as conn:
            conn.execute(text("DELETE FROM " + site.split(".")[0]))
        # ... and the same value with a NUL tail, or a NUL swapped in for the last
        # character (so the length is unchanged), is refused BY the named CHECK.
        for bad in (good + "\x00junk", good[:-1] + "\x00", "\x00" + good[1:]):
            with pytest.raises(IntegrityError, match=constraint):
                run(schema_engine, sql, **params(bad))

    def test_a_nul_is_refused_on_update_too(self, schema_engine):
        accepted(schema_engine, INS_CLIENT, cid="alice")
        accepted(schema_engine, INS_TRUSTED, **trusted_params())
        rejected(schema_engine,
                 "UPDATE trusted_clients SET predecessor_fingerprint = :v",
                 "ck_trusted_clients_predecessor_fingerprint_hex", v=HEX + "\x00")


# ===========================================================================
# M5: the migration itself
# ===========================================================================

BASELINE_ROWS = {
    "global_settings": [(1, "autosnap", 300.0, "5m", 3600.0, "1h", "127.0.0.1", 8080, 0, 7)],
    "datasets": [(1, "tank/a", 0, 3600.0, "1h", 1), (2, "tank/b", 1, 60.0, None, 1)],
    # A legacy plaintext URL and a non-https scheme: the migration must keep them
    # (loadable, then fixable); `validate_config` is what refuses them.
    "destinations": [("dc2", "ssh://x.example/pool"), ("offsite", "http://old.example:8080")],
    "dataset_remotes": [(1, 1, "offsite", 7200.0, "2h"), (2, 1, "dc2", None, None)],
    "retention_rules": [
        (1, 1, None, 86400.0, "1d", 2592000.0, "30d"),
        (2, 1, 1, 3600.0, "1h", 86400.0, "1d"),
        (3, 2, None, 60.0, None, 600.0, None),
    ],
    "remote_server": [(1, "tank/recv", 1)],
}
#: `global_settings` columns that exist in BOTH revisions, in baseline order.
SETTINGS_COMMON = ("id", "snapshot_prefix", "check_interval_seconds", "check_interval_literal",
                   "prune_interval_seconds", "prune_interval_literal", "api_host", "api_port",
                   "dry_run", "generation")
OLD_USER_VERSION = 424242


def _alembic(resolved, direction, revision):
    """Run `alembic upgrade|downgrade <revision>` on the file the way
    `upgrade_schema` does (FK enforcement off, one explicit transaction), and
    return the FK-check result taken before the commit."""
    with open_config_connection(resolved, readonly=False, foreign_keys=False,
                                allow_new=True) as conn:
        trans = conn.begin()
        cfg = migrate_module._build_config(conn)
        getattr(command, direction)(cfg, revision)
        violations = conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
        trans.commit()
    dispose_all()
    return violations


def make_baseline_db(tmp_path):
    """A real database at `a1c28e1b3f34`, built by the real baseline migration
    (never by `create_all`), holding rows in every table, with a distinctive
    `user_version`. Returns `(resolved, db_path)`."""
    db_path = tmp_path / "old" / "config.db"
    resolved = resolve_config_path(str(db_path))
    store_paths.ensure_config_dir(db_path.parent)
    store_paths.create_config_db_file(db_path)
    assert _alembic(resolved, "upgrade", BASELINE) == []
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            "INSERT INTO global_settings (id, snapshot_prefix, check_interval_seconds, "
            "check_interval_literal, prune_interval_seconds, prune_interval_literal, api_host, "
            "api_port, dry_run, client_id_file, generation) VALUES (1,'autosnap',300,'5m',3600,'1h',"
            "'127.0.0.1',8080,0,'/home/old/.config/zfsbackup/client_id',7)")
        conn.executemany("INSERT INTO datasets VALUES (?,?,?,?,?,?)", BASELINE_ROWS["datasets"])
        conn.executemany("INSERT INTO destinations VALUES (?,?)", BASELINE_ROWS["destinations"])
        conn.executemany("INSERT INTO dataset_remotes VALUES (?,?,?,?,?)",
                         BASELINE_ROWS["dataset_remotes"])
        conn.executemany("INSERT INTO retention_rules VALUES (?,?,?,?,?,?,?)",
                         BASELINE_ROWS["retention_rules"])
        conn.executemany("INSERT INTO remote_server VALUES (?,?,?)", BASELINE_ROWS["remote_server"])
        conn.execute(f"PRAGMA user_version = {OLD_USER_VERSION}")
        conn.commit()
    finally:
        conn.close()
    return resolved, db_path


def q(db_path, statement, *params):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(statement, params).fetchall()
    finally:
        conn.close()


def table_rows(db_path, table, columns="*"):
    return q(db_path, f"SELECT {columns} FROM {table} ORDER BY 1")


def table_names(db_path):
    return {r[0] for r in q(db_path, "SELECT name FROM sqlite_master WHERE type='table'")}


def column_names(db_path, table):
    return [r[1] for r in q(db_path, f"PRAGMA table_info({table})")]


def check_names(db_path, table):
    (sql,) = q(db_path, "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table)[0]
    import re
    return set(re.findall(r"CONSTRAINT\s+(\w+)\s+CHECK", sql))


def assert_old_rows_intact(db_path):
    assert table_rows(db_path, "global_settings", ", ".join(SETTINGS_COMMON)) == [
        BASELINE_ROWS["global_settings"][0]]
    assert table_rows(db_path, "datasets") == BASELINE_ROWS["datasets"]
    assert table_rows(db_path, "destinations", "name, url") == BASELINE_ROWS["destinations"]
    assert table_rows(db_path, "dataset_remotes") == BASELINE_ROWS["dataset_remotes"]
    assert table_rows(db_path, "retention_rules") == BASELINE_ROWS["retention_rules"]
    assert table_rows(db_path, "remote_server") == BASELINE_ROWS["remote_server"]


class TestUpgradeFromTheBaselineWithData:
    @pytest.fixture
    def old(self, tmp_path):
        return make_baseline_db(tmp_path)

    def test_the_fixture_is_really_the_baseline(self, old):
        _resolved, db = old
        assert q(db, "SELECT version_num FROM alembic_version") == [(BASELINE,)]
        assert "client_id_file" in column_names(db, "global_settings")
        assert not (set(NEW_TABLES) & table_names(db))
        assert q(db, "PRAGMA user_version") == [(OLD_USER_VERSION,)]

    def test_check_schema_reports_it_out_of_date_not_unknown(self, old):
        from zfsbackup.config.store import SchemaOutOfDate, check_schema

        resolved, _db = old
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaOutOfDate) as excinfo:
                check_schema(conn)
        assert BASELINE in str(excinfo.value) and TLS_HEAD in str(excinfo.value)
        assert excinfo.value.recorded_revisions == (BASELINE,)

    def test_upgrade_reaches_the_new_head_and_keeps_every_row(self, old):
        resolved, db = old
        result = upgrade_schema(resolved)
        dispose_all()
        assert result.changed and result.from_revs == (BASELINE,) and result.to_rev == TLS_HEAD
        assert q(db, "SELECT version_num FROM alembic_version") == [(TLS_HEAD,)]
        assert_old_rows_intact(db)

    def test_user_version_survives_the_table_rebuilds(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        assert q(db, "PRAGMA user_version") == [(OLD_USER_VERSION,)]

    def test_the_foreign_key_check_is_clean_and_references_still_resolve(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        assert q(db, "PRAGMA foreign_key_check") == []
        (remotes_sql,) = q(db, "SELECT sql FROM sqlite_master WHERE name='dataset_remotes'")[0]
        assert "REFERENCES destinations" in remotes_sql
        assert "_alembic_tmp" not in " ".join(r[0] or "" for r in q(db, "SELECT sql FROM sqlite_master"))

    def test_global_settings_lost_client_id_file_and_gained_the_pair_as_null(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        cols = column_names(db, "global_settings")
        assert "client_id_file" not in cols
        assert table_rows(db, "global_settings", "client_cert_file, client_key_file") == [(None, None)]

    def test_destinations_gained_a_null_fingerprint(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        assert table_rows(db, "destinations", "name, server_fingerprint") == [
            ("dc2", None), ("offsite", None)]

    def test_new_tables_exist_and_are_empty(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        assert set(NEW_TABLES) <= table_names(db)
        for table in NEW_TABLES:
            assert table_rows(db, table) == [], table

    def test_the_hand_added_checks_are_present_after_the_upgrade(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        assert "ck_destinations_server_fingerprint_hex" in check_names(db, "destinations")
        assert "ck_global_settings_client_cert_key_together" in check_names(db, "global_settings")
        # ... and the rebuilt tables kept their old ones.
        assert "ck_global_settings_singleton" in check_names(db, "global_settings")

    def test_the_upgraded_store_loads_and_the_legacy_urls_are_refused_not_lost(self, old):
        resolved, _db = old
        upgrade_schema(resolved)
        dispose_all()
        snapshot = read_config(resolved)
        dispose_all()
        assert snapshot.client_id is None
        assert not hasattr(snapshot.config, "client_id_file")
        assert snapshot.config.destinations["offsite"].url == "http://old.example:8080"
        with pytest.raises(ValueError, match=r"destinations\[dc2\].*scheme 'ssh'"):
            validate_config(snapshot.config)
        del snapshot.config.destinations["dc2"]
        with pytest.raises(ValueError, match="plaintext http is not supported"):
            validate_config(snapshot.config)

    def test_upgrading_again_is_a_no_op(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        before = [q(db, "SELECT * FROM sqlite_master ORDER BY name")]
        again = upgrade_schema(resolved)
        dispose_all()
        assert not again.changed
        assert [q(db, "SELECT * FROM sqlite_master ORDER BY name")] == before

    def test_downgrade_then_upgrade_again_round_trips_with_a_clean_fk_check(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        # Post-upgrade writes that the downgrade is allowed to discard.
        conn = sqlite3.connect(str(db))
        conn.execute("INSERT INTO host_identity (id, client_id, created_at) VALUES (1,'h-1',1.0)")
        conn.execute("INSERT INTO clients (client_id, created_at) VALUES ('alice', 1.0)")
        conn.execute(f"UPDATE destinations SET server_fingerprint = '{HEX}' WHERE name = 'offsite'")
        conn.execute("UPDATE global_settings SET client_cert_file='/c', client_key_file='/k'")
        conn.commit()
        conn.close()

        assert _alembic(resolved, "downgrade", BASELINE) == []
        assert q(db, "SELECT version_num FROM alembic_version") == [(BASELINE,)]
        assert not (set(NEW_TABLES) & table_names(db))
        assert "client_id_file" in column_names(db, "global_settings")
        assert "client_cert_file" not in column_names(db, "global_settings")
        assert "server_fingerprint" not in column_names(db, "destinations")
        # Documented loss: the old client_id_file value is not recoverable.
        assert table_rows(db, "global_settings", "client_id_file") == [(None,)]
        assert_old_rows_intact(db)
        assert q(db, "PRAGMA user_version") == [(OLD_USER_VERSION,)]
        assert q(db, "PRAGMA foreign_key_check") == []

        assert _alembic(resolved, "upgrade", TLS_HEAD) == []
        assert q(db, "SELECT version_num FROM alembic_version") == [(TLS_HEAD,)]
        assert set(NEW_TABLES) <= table_names(db)
        for table in NEW_TABLES:
            assert table_rows(db, table) == [], table      # the discarded rows stay gone
        assert table_rows(db, "destinations", "name, server_fingerprint") == [
            ("dc2", None), ("offsite", None)]
        assert_old_rows_intact(db)
        assert q(db, "PRAGMA user_version") == [(OLD_USER_VERSION,)]
        assert q(db, "PRAGMA foreign_key_check") == []

    def test_the_round_tripped_schema_still_enforces_every_new_check(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        _alembic(resolved, "downgrade", BASELINE)
        _alembic(resolved, "upgrade", TLS_HEAD)
        engine = make_engine(f"sqlite:///{db}")
        try:
            rejected(engine, INS_DEST, "ck_destinations_server_fingerprint_hex", n="x", fp="bad")
            rejected(engine, "UPDATE global_settings SET client_cert_file = '/c'",
                     "ck_global_settings_client_cert_key_together")
            with pytest.raises(IntegrityError, match="FOREIGN KEY"):
                run(engine, INS_TRUSTED, **trusted_params(cid="ghost"))
        finally:
            engine.dispose()

    def test_the_upgrade_creates_superseded_by_with_its_named_check(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        assert column_names(db, "revoked_fingerprints")[-1] == "superseded_by"
        assert "ck_revoked_fingerprints_superseded_by_hex" in check_names(db, "revoked_fingerprints")

    def test_the_round_tripped_schema_still_enforces_the_superseded_by_check(self, old):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        _alembic(resolved, "downgrade", BASELINE)
        _alembic(resolved, "upgrade", TLS_HEAD)
        engine = make_engine(f"sqlite:///{db}")
        try:
            accepted(engine, INS_CLIENT, cid="alice")
            accepted(engine, INS_REVOKED_SUPERSEDED, **superseded_params())
            rejected(engine, INS_REVOKED_SUPERSEDED, "ck_revoked_fingerprints_superseded_by_hex",
                     **superseded_params(fp=HEX3, sup=HEX2 + "\x00junk"))
        finally:
            engine.dispose()

    def test_the_round_tripped_schema_matches_create_all(self, old, tmp_path):
        resolved, db = old
        upgrade_schema(resolved)
        dispose_all()
        _alembic(resolved, "downgrade", BASELINE)
        _alembic(resolved, "upgrade", TLS_HEAD)
        ref_engine = make_engine(f"sqlite:///{tmp_path / 'ref.db'}")
        Base.metadata.create_all(ref_engine)
        mig_engine = make_engine(f"sqlite:///{db}")
        try:
            ref, mig = inspect(ref_engine), inspect(mig_engine)
            for table in sorted(Base.metadata.tables):
                # Set, not list: `batch.add_column` appends after `generation`.
                assert {c["name"] for c in mig.get_columns(table)} == \
                       {c["name"] for c in ref.get_columns(table)}, table
                assert {c["name"] for c in mig.get_check_constraints(table)} == \
                       {c["name"] for c in ref.get_check_constraints(table)}, table
                assert {i["name"] for i in mig.get_indexes(table)} == \
                       {i["name"] for i in ref.get_indexes(table)}, table
        finally:
            ref_engine.dispose()
            mig_engine.dispose()


# ===========================================================================
# M6: mapper load/save of the new fields
# ===========================================================================


class TestMapperRoundTrip:
    def test_server_fingerprint_and_cert_key_round_trip(self, mem_session):
        cfg = tls_config()
        save_config(mem_session, cfg)
        mem_session.commit()
        mem_session.expire_all()
        loaded = load_config(mem_session)
        assert loaded == cfg
        assert loaded.destinations["offsite"].server_fingerprint == HEX
        assert loaded.destinations["unpinned"].server_fingerprint is None
        assert loaded.client_cert_file == Path("/etc/zfsbackup/client.crt")
        assert loaded.client_key_file == Path("/etc/zfsbackup/client.key")
        assert isinstance(loaded.client_cert_file, Path)

    def test_the_rows_hold_plain_strings_and_canonical_hex(self, mem_session):
        save_config(mem_session, tls_config())
        mem_session.commit()
        mem_session.expire_all()
        gs = mem_session.get(models.GlobalSettings, 1)
        assert gs.client_cert_file == "/etc/zfsbackup/client.crt"
        assert gs.client_key_file == "/etc/zfsbackup/client.key"
        assert mem_session.get(models.Destination, "offsite").server_fingerprint == HEX
        assert mem_session.get(models.Destination, "unpinned").server_fingerprint is None

    def test_a_colon_form_pin_is_stored_canonical(self, mem_session):
        cfg = tls_config()
        cfg.destinations["offsite"] = Destination(
            "https://o", server_fingerprint=":".join(HEX.upper()[i:i + 2] for i in range(0, 64, 2)))
        save_config(mem_session, cfg)
        mem_session.commit()
        assert load_config(mem_session).destinations["offsite"].server_fingerprint == HEX

    def test_none_round_trips_as_null_for_every_new_column(self, mem_session):
        save_config(mem_session, minimal_config())
        mem_session.commit()
        mem_session.expire_all()
        gs = mem_session.get(models.GlobalSettings, 1)
        assert gs.client_cert_file is None and gs.client_key_file is None
        loaded = load_config(mem_session)
        assert loaded.client_cert_file is None and loaded.client_key_file is None
        assert not hasattr(loaded, "client_id_file")

    def test_a_second_save_can_clear_the_pins_and_the_paths(self, mem_session):
        save_config(mem_session, tls_config())
        mem_session.commit()
        cleared = tls_config()
        cleared.destinations["offsite"] = Destination("https://offsite.example:8443")
        cleared.client_cert_file = cleared.client_key_file = None
        save_config(mem_session, cleared)
        mem_session.commit()
        mem_session.expire_all()
        loaded = load_config(mem_session)
        assert loaded.destinations["offsite"].server_fingerprint is None
        assert loaded.client_cert_file is None and loaded.client_key_file is None

    def test_load_never_materialises_client_id_file(self, mem_session):
        save_config(mem_session, tls_config())
        mem_session.commit()
        assert not hasattr(load_config(mem_session), "client_id_file")

    def test_loading_does_not_touch_home(self, mem_session, monkeypatch):
        def boom(*_a, **_k):
            raise AssertionError("load_config touched $HOME")

        save_config(mem_session, tls_config())
        mem_session.commit()
        monkeypatch.setattr(Path, "home", boom)
        load_config(mem_session)

    def test_the_load_is_equal_by_value_and_by_same_as(self, mem_session):
        cfg = tls_config()
        save_config(mem_session, cfg)
        mem_session.commit()
        assert load_config(mem_session).same_as(cfg)

    def test_a_pin_change_alone_is_a_same_as_difference(self, mem_session):
        a, b = tls_config(), tls_config()
        b.destinations["offsite"].server_fingerprint = HEX2
        assert not a.same_as(b)


# ===========================================================================
# M6: validate_config
# ===========================================================================


def with_destinations(**dests) -> BackupConfig:
    cfg = minimal_config()
    cfg.destinations = dict(dests)
    return cfg


class TestValidateConfigTls:
    def test_the_good_config_has_no_warnings(self):
        assert validate_config(tls_config()) == []

    # -- destination url --------------------------------------------------

    @pytest.mark.parametrize("url, text", [
        ("http://h", "plaintext http is not supported"),
        ("HTTP://h", "plaintext http is not supported"),
        ("ssh://h/pool", "scheme 'ssh'"),
        ("backup.example", "no scheme"),
        ("https://", "no host"),
        ("https://h:99999", "not a valid URL"),
        ("https://h:abc", "not a valid URL"),
        ("https://h:0", "port 0"),
        ("https://h:", "empty port"),
        ("https://[::1]:", "empty port"),
        ("https://u@h:", "empty port"),
    ])
    def test_non_https_urls_are_refused_naming_the_destination(self, url, text):
        with pytest.raises(ValueError, match=r"destinations\[d\]: ") as excinfo:
            validate_config(with_destinations(d=Destination(url)))
        assert text in str(excinfo.value)

    def test_an_empty_url_still_reports_requires_url(self):
        with pytest.raises(ValueError, match="Destination 'd' requires 'url'"):
            validate_config(with_destinations(d=Destination("")))

    @pytest.mark.parametrize("url", ["HTTPS://H", "https://h:8443/x", "https://[::1]:1"])
    def test_https_in_any_case_is_accepted(self, url):
        assert validate_config(with_destinations(d=Destination(url))) == []

    @pytest.mark.parametrize("url", [
        "  https://h ", " https://h", "https://h ", "https://h\n", "\thttps://h", "https://h\t ",
    ], ids=["both", "leading", "trailing", "newline", "tab", "tab-space"])
    def test_a_url_with_surrounding_whitespace_is_refused(self, url):
        # `destination_url_problem` judges the stripped url, so the https scheme
        # itself is fine; the store refuses to save a value `from_file` would
        # have stripped (so `load_config(save_config(c)) == c`).
        with pytest.raises(ValueError, match=r"destinations\[d\]\.url") as excinfo:
            validate_config(with_destinations(d=Destination(url)))
        message = str(excinfo.value)
        assert "has leading or trailing whitespace" in message
        assert "expected 'https://h'" in message

    def test_the_stripped_form_of_that_url_is_accepted(self):
        assert validate_config(with_destinations(d=Destination("https://h"))) == []

    def test_a_whitespace_url_is_never_written(self, tmp_path):
        # validate_config runs before the wipe in `save_config`'s caller path;
        # prove the refusal leaves the saved configuration untouched.
        engine = make_engine(f"sqlite:///{tmp_path / 'ws.db'}")
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            with pytest.raises(ValueError, match="leading or trailing whitespace"):
                save_config(session, with_destinations(d=Destination(" https://h")))
            session.rollback()
            assert session.execute(text("SELECT count(*) FROM destinations")).scalar() == 0
        engine.dispose()

    # -- fingerprints -----------------------------------------------------

    @pytest.mark.parametrize("value", [HEX.upper(), ":".join(HEX[i:i + 2] for i in range(0, 64, 2)), f" {HEX}"])
    def test_a_non_canonical_pin_is_refused_with_the_canonical_form(self, value):
        dest = Destination("https://h")
        dest.server_fingerprint = value            # bypass __post_init__ normalisation
        with pytest.raises(ValueError, match=r"destinations\[d\]\.server_fingerprint") as excinfo:
            validate_config(with_destinations(d=dest))
        assert "not canonical" in str(excinfo.value) and HEX in str(excinfo.value)

    @pytest.mark.parametrize("value", ["", "abc", HEX[:-1], 5, b"x" * 64, ["x"]])
    def test_a_malformed_pin_is_refused(self, value):
        dest = Destination("https://h")
        dest.server_fingerprint = value
        with pytest.raises(ValueError, match=r"destinations\[d\]\.server_fingerprint"):
            validate_config(with_destinations(d=dest))

    def test_no_pin_and_a_canonical_pin_are_fine(self):
        assert validate_config(with_destinations(
            a=Destination("https://a"), b=Destination("https://b", HEX))) == []

    # -- cert / key -------------------------------------------------------

    @pytest.mark.parametrize("cert, key", [
        (Path("/etc/c"), None), (None, Path("/etc/k")), ("/etc/c", None),
    ])
    def test_cert_without_key_or_the_reverse_is_refused(self, cert, key):
        with pytest.raises(ValueError, match="must be set together"):
            validate_config(minimal_config(client_cert_file=cert, client_key_file=key))

    @pytest.mark.parametrize("cert, key, field", [
        (Path("c.pem"), Path("/k"), "client_cert_file"),
        (Path("/c"), Path("rel/k.pem"), "client_key_file"),
        ("c.pem", "/k", "client_cert_file"),
    ])
    def test_relative_paths_are_refused_naming_the_field(self, cert, key, field):
        with pytest.raises(ValueError, match=f"{field}.*absolute"):
            validate_config(minimal_config(client_cert_file=cert, client_key_file=key))

    @pytest.mark.parametrize("cert, key", [(5, Path("/k")), (Path("/c"), 5), (b"/c", b"/k")])
    def test_a_non_path_is_refused(self, cert, key):
        with pytest.raises(ValueError, match="client_(cert|key)_file must be a path"):
            validate_config(minimal_config(client_cert_file=cert, client_key_file=key))

    @pytest.mark.parametrize("cert, key", [(None, None), (Path("/c"), Path("/k")), ("/c", "/k")])
    def test_none_or_an_absolute_pair_is_fine(self, cert, key):
        assert validate_config(minimal_config(client_cert_file=cert, client_key_file=key)) == []

    # -- it is the write-side gate ---------------------------------------

    @pytest.mark.parametrize("mutate", [
        lambda c: c.destinations.update(d=Destination("http://h")),
        lambda c: setattr(c, "client_key_file", None),
        lambda c: setattr(c.destinations["offsite"], "server_fingerprint", HEX.upper()),
    ], ids=["http", "half-pair", "uppercase-pin"])
    def test_save_config_refuses_before_wiping_anything(self, mem_session, mutate):
        save_config(mem_session, tls_config())
        mem_session.commit()
        bad = tls_config()
        mutate(bad)
        with pytest.raises(ValueError):
            save_config(mem_session, bad)
        assert not mem_session.new and not mem_session.deleted
        mem_session.rollback()
        assert load_config(mem_session) == tls_config()


# ===========================================================================
# M6: ConfigSnapshot.client_id
# ===========================================================================


@pytest.fixture
def stored(tmp_path):
    """A schema-current file database holding `tls_config()`."""
    resolved = resolve_config_path(str(tmp_path / "store" / "config.db"))
    src = tmp_path / "in.yaml"
    src.write_text(yaml.safe_dump(YAML_DICT, sort_keys=False))
    import_yaml(src, resolved)
    dispose_all()
    return resolved


YAML_DICT = {
    "snapshot_prefix": "zb",
    "datasets": [{"name": "tank/a", "frequency": "1h",
                  "remote": [{"destination": "offsite"}]}],
    "destinations": {"offsite": {"url": "https://offsite.example:8443",
                                 "server_fingerprint": HEX}},
    "client_cert_file": "/etc/zfsbackup/client.crt",
    "client_key_file": "/etc/zfsbackup/client.key",
}
OTHER_YAML = {"snapshot_prefix": "other", "datasets": [{"name": "tank/x", "frequency": "30m"}]}


def write_yaml(tmp_path, data, name="other.yaml"):
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


def sql_exec(resolved, statements):
    conn = sqlite3.connect(str(resolved.resolved_path))
    try:
        for stmt in statements:
            conn.execute(*stmt) if isinstance(stmt, tuple) else conn.execute(stmt)
        conn.commit()
    finally:
        conn.close()


class TestConfigSnapshotClientId:
    def test_the_dataclass_carries_client_id_defaulting_to_none(self):
        snap = ConfigSnapshot(config=minimal_config(), generation=0, schema_rev=TLS_HEAD, store_id=0)
        assert snap.client_id is None
        with pytest.raises(Exception):
            snap.client_id = "x"          # frozen

    def test_no_row_means_none(self, stored):
        snap = read_config(stored)
        dispose_all()
        assert snap.client_id is None

    def test_a_row_is_returned(self, stored):
        sql_exec(stored, [("INSERT INTO host_identity (id, client_id, created_at) "
                           "VALUES (1, 'laptop-0a1b2c3d', 1.0)",)])
        snap = read_config(stored)
        dispose_all()
        assert snap.client_id == "laptop-0a1b2c3d"

    def test_the_snapshot_still_carries_the_rest(self, stored):
        sql_exec(stored, [("INSERT INTO host_identity (id, client_id, created_at) VALUES (1,'h',1.0)",)])
        snap = read_config(stored)
        dispose_all()
        assert snap.config.snapshot_prefix == "zb" and snap.schema_rev == TLS_HEAD
        assert snap.config.destinations["offsite"].server_fingerprint == HEX
        assert snap.config.client_cert_file == Path("/etc/zfsbackup/client.crt")
        assert not hasattr(snap.config, "client_id_file")

    def test_the_session_level_snapshot_reads_it_too(self, mem_session):
        save_config(mem_session, minimal_config())
        mem_session.commit()
        # create_all leaves no alembic stamp; hand the revision in, as a caller
        # that already ran `check_schema` on this transaction does.
        assert load_config_snapshot(mem_session, schema_rev=TLS_HEAD).client_id is None
        mem_session.add(models.HostIdentity(id=1, client_id="h-1", created_at=1.0))
        mem_session.commit()
        assert load_config_snapshot(mem_session, schema_rev=TLS_HEAD).client_id == "h-1"

    def test_the_id_is_not_part_of_the_config(self, stored):
        import dataclasses

        assert "client_id" not in {f.name for f in dataclasses.fields(BackupConfig)}
        before = read_config(stored).config
        dispose_all()
        sql_exec(stored, [("INSERT INTO host_identity (id, client_id, created_at) VALUES (1,'h',1.0)",)])
        after = read_config(stored).config
        dispose_all()
        assert before.same_as(after)               # an id change is not a config difference

    def test_a_changed_id_is_visible_on_the_next_read(self, stored):
        sql_exec(stored, [("INSERT INTO host_identity (id, client_id, created_at) VALUES (1,'one',1.0)",)])
        assert read_config(stored).client_id == "one"
        dispose_all()
        sql_exec(stored, [("UPDATE host_identity SET client_id = 'two'",)])
        assert read_config(stored).client_id == "two"
        dispose_all()


# ===========================================================================
# M6: import / write_config keep the identity and the trust tables
# ===========================================================================

SEEDS = {
    "host_identity": [("INSERT INTO host_identity (id, client_id, created_at, updated_at) "
                       "VALUES (1, 'laptop-0a1b2c3d', 100.0, 150.0)",)],
    "clients": [("INSERT INTO clients (client_id, label, created_at, revoked_at) "
                 "VALUES ('alice', 'Alice', 100.0, NULL)",),
                ("INSERT INTO clients (client_id, label, created_at, revoked_at) "
                 "VALUES ('bob', NULL, 100.0, 200.0)",)],
    "trusted_clients": [
        (f"INSERT INTO trusted_clients VALUES ('{HEX}', 'alice', 'PEM-A', 9e9, 100.0, 'token', NULL)",),
        (f"INSERT INTO trusted_clients VALUES ('{HEX2}', 'alice', 'PEM-B', 9e9, 120.0, 'rotate', '{HEX}')",)],
    "enrollment_token": [(f"INSERT INTO enrollment_token (id, secret_sha256, server_fingerprint, "
                          f"created_at, expires_at, client_id, replace) "
                          f"VALUES (1, '{HEX3}', '{HEX2}', 100.0, 400.0, 'alice', 1)",)],
    "revoked_fingerprints": [
        (f"INSERT INTO revoked_fingerprints VALUES ('{HEX3}', 'alice', 130.0, 'revoked', NULL)",),
        (f"INSERT INTO revoked_fingerprints VALUES ('{'cd' * 32}', 'bob', 140.0, 'client_revoked', NULL)",),
        # A retired row keeps the successor that retired it: `superseded_by` must survive too.
        (f"INSERT INTO revoked_fingerprints VALUES ('{'ef' * 32}', 'alice', 150.0, 'retired', '{HEX2}')",)],
    "trust_state": [("INSERT INTO trust_state (id, generation) VALUES (1, 5)",)],
}
INSERT_ORDER = ("host_identity", "clients", "trusted_clients", "revoked_fingerprints",
                "enrollment_token", "trust_state")


def seed_preserved(resolved):
    sql_exec(resolved, [stmt for table in INSERT_ORDER for stmt in SEEDS[table]])


def preserved_snapshot(resolved):
    conn = sqlite3.connect(str(resolved.resolved_path))
    try:
        return {t: conn.execute(f"SELECT * FROM {t} ORDER BY 1, 2").fetchall()
                for t in sorted(PRESERVED_TABLES)}
    finally:
        conn.close()


class TestImportAndWriteKeepIdentityAndTrust:
    @pytest.fixture
    def seeded(self, stored):
        seed_preserved(stored)
        return stored

    def test_the_seed_is_complete_and_nonempty(self, seeded):
        snap = preserved_snapshot(seeded)
        assert set(snap) == set(PRESERVED_TABLES) == set(NEW_TABLES)
        assert all(snap[t] for t in snap), snap

    def test_import_over_the_database_leaves_every_preserved_row(self, seeded, tmp_path):
        before = preserved_snapshot(seeded)
        gen = read_config(seeded).generation
        dispose_all()
        import_yaml(write_yaml(tmp_path, OTHER_YAML), seeded)
        dispose_all()
        assert preserved_snapshot(seeded) == before
        snap = read_config(seeded)
        dispose_all()
        assert snap.config.snapshot_prefix == "other"        # the config really was replaced
        assert snap.generation == gen + 1
        assert snap.client_id == "laptop-0a1b2c3d"

    def test_repeated_imports_and_a_backup_callback_keep_them_too(self, seeded, tmp_path):
        before = preserved_snapshot(seeded)
        for data in (OTHER_YAML, YAML_DICT, OTHER_YAML):
            import_yaml(write_yaml(tmp_path, data), seeded, backup=lambda cfg: tmp_path / "bk.yaml")
            dispose_all()
        assert preserved_snapshot(seeded) == before

    def test_write_config_leaves_every_preserved_row(self, seeded):
        before = preserved_snapshot(seeded)
        snap = read_config(seeded)
        dispose_all()
        edited = snap.config
        edited.snapshot_prefix = "edited"
        edited.datasets[0].frequency = Duration("2h")
        result = write_config(seeded, edited, expected_generation=snap.generation)
        dispose_all()
        assert result.generation == snap.generation + 1
        assert preserved_snapshot(seeded) == before
        after = read_config(seeded)
        dispose_all()
        assert after.config.snapshot_prefix == "edited" and after.client_id == "laptop-0a1b2c3d"

    def test_write_config_that_drops_every_destination_keeps_the_trust(self, seeded):
        before = preserved_snapshot(seeded)
        snap = read_config(seeded)
        dispose_all()
        edited = snap.config
        edited.datasets[0].remote = []
        edited.destinations = {}
        edited.client_cert_file = edited.client_key_file = None
        write_config(seeded, edited, expected_generation=snap.generation)
        dispose_all()
        assert preserved_snapshot(seeded) == before

    def test_a_direct_save_config_session_keeps_them(self, seeded):
        before = preserved_snapshot(seeded)
        with open_config_session(seeded, readonly=False) as session:
            save_config(session, minimal_config())
        dispose_all()
        assert preserved_snapshot(seeded) == before

    def test_a_refused_import_changes_nothing_at_all(self, seeded, tmp_path):
        before = preserved_snapshot(seeded)
        bad = dict(OTHER_YAML, destinations={"d": {"url": "http://plain.example"}})
        with pytest.raises(ValueError, match="plaintext http"):
            import_yaml(write_yaml(tmp_path, bad), seeded)
        dispose_all()
        assert preserved_snapshot(seeded) == before
        assert read_config(seeded).config.snapshot_prefix == "zb"
        dispose_all()

    def test_the_trust_generation_is_not_a_config_generation(self, seeded, tmp_path):
        import_yaml(write_yaml(tmp_path, OTHER_YAML), seeded)
        dispose_all()
        assert q(seeded.resolved_path, "SELECT generation FROM trust_state") == [(5,)]

    def test_the_foreign_keys_between_preserved_rows_stay_valid(self, seeded, tmp_path):
        import_yaml(write_yaml(tmp_path, OTHER_YAML), seeded)
        dispose_all()
        assert q(seeded.resolved_path, "PRAGMA foreign_key_check") == []

    def test_a_first_import_creates_no_identity_or_trust_rows(self, tmp_path):
        resolved = resolve_config_path(str(tmp_path / "fresh" / "config.db"))
        import_yaml(write_yaml(tmp_path, OTHER_YAML), resolved)
        dispose_all()
        assert all(rows == [] for rows in preserved_snapshot(resolved).values())
        assert read_config(resolved).client_id is None
        dispose_all()

    def test_each_preserved_table_is_individually_protected(self, seeded, tmp_path):
        # One parametrised failure message per table if a wipe is ever widened.
        before = preserved_snapshot(seeded)
        import_yaml(write_yaml(tmp_path, OTHER_YAML), seeded)
        dispose_all()
        after = preserved_snapshot(seeded)
        for table in sorted(PRESERVED_TABLES):
            assert after[table] == before[table], table


# ===========================================================================
# M6: CONFIG_TABLES / PRESERVED_TABLES
# ===========================================================================


class TestTableClassification:
    def test_the_two_sets_partition_every_table(self):
        assert set(CONFIG_TABLES).isdisjoint(PRESERVED_TABLES)
        assert set(CONFIG_TABLES) | set(PRESERVED_TABLES) == set(Base.metadata.tables)

    def test_config_tables_is_exactly_the_six_in_a_fixed_order(self):
        assert CONFIG_TABLES == (
            "retention_rules", "dataset_remotes", "datasets",
            "destinations", "global_settings", "remote_server")
        assert len(set(CONFIG_TABLES)) == len(CONFIG_TABLES)

    def test_preserved_tables_is_exactly_the_six_new_ones(self):
        assert PRESERVED_TABLES == frozenset(NEW_TABLES)
        assert isinstance(PRESERVED_TABLES, frozenset)

    def test_a_table_added_without_classifying_it_fails_the_import_time_assert(self):
        """The guard is an `assert` at import of `mapper.py`; prove it fires by
        registering an extra table and re-importing the module in a child process
        (reloading it in-process would split the class identities every other
        test holds)."""
        from _hermetic import hermetic_env

        code = (
            "import sqlalchemy as sa\n"
            "from zfsbackup.config.store import models\n"
            "sa.Table('surprise', models.Base.metadata, sa.Column('id', sa.Integer, primary_key=True))\n"
            "try:\n"
            "    import importlib, zfsbackup.config.store.mapper as m\n"
            "    importlib.reload(m)\n"
            "except AssertionError as exc:\n"
            "    print('GUARD', exc)\n"
            "else:\n"
            "    print('NO-GUARD')\n"
        )
        root = Path(__file__).resolve().parent.parent
        out = subprocess.run(
            [sys.executable, "-c", code], cwd=root, env=hermetic_env(PYTHONPATH=str(root)),
            capture_output=True, text=True, timeout=60,
        )
        assert out.stdout.startswith("GUARD"), (out.stdout, out.stderr)
        assert "surprise" in out.stdout and "CONFIG_TABLES" in out.stdout

    def test_delete_order_is_children_first(self):
        position = {name: i for i, name in enumerate(CONFIG_TABLES)}
        for name in CONFIG_TABLES:
            for fk in Base.metadata.tables[name].foreign_keys:
                parent = fk.column.table.name
                assert parent in position, f"{name} -> {parent} leaves the config tables"
                assert position[parent] > position[name], (
                    f"{parent} must be deleted AFTER {name} (it references it)")

    def test_no_foreign_key_crosses_between_the_groups(self):
        for name in PRESERVED_TABLES:
            for fk in Base.metadata.tables[name].foreign_keys:
                assert fk.column.table.name in PRESERVED_TABLES, (name, fk)
        for name in CONFIG_TABLES:
            for fk in Base.metadata.tables[name].foreign_keys:
                assert fk.column.table.name in CONFIG_TABLES, (name, fk)

    def test_the_delete_models_are_the_config_tables_in_order(self):
        from zfsbackup.config.store import mapper

        assert tuple(m.__table__.name for m in mapper._CONFIG_MODELS) == CONFIG_TABLES
        assert not ({m.__table__.name for m in mapper._CONFIG_MODELS} & set(PRESERVED_TABLES))

    @pytest.mark.parametrize("table", sorted(PRESERVED_TABLES))
    def test_a_save_config_never_deletes_from_a_preserved_table(self, mem_session, table):
        """Behavioural twin of the classification: seed one row per preserved
        table, save twice, and every row is still there."""
        for stmt in [s for t in INSERT_ORDER for s in SEEDS[t]]:
            mem_session.execute(text(stmt[0]))
        mem_session.commit()
        for prefix in ("a", "b"):
            save_config(mem_session, minimal_config(snapshot_prefix=prefix))
            mem_session.commit()
        count = mem_session.execute(text(f"SELECT count(*) FROM {table}")).scalar()
        assert count == sum(1 for t in [table] for _ in SEEDS[t])

    def test_every_config_table_is_replaced_not_accumulated(self, mem_session):
        for _ in range(3):
            save_config(mem_session, tls_config())
            mem_session.commit()
        counts = {t: mem_session.execute(text(f"SELECT count(*) FROM {t}")).scalar()
                  for t in CONFIG_TABLES}
        assert counts["datasets"] == 1 and counts["destinations"] == 2
        assert counts["dataset_remotes"] == 1 and counts["global_settings"] == 1

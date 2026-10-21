"""TLS/mTLS plan M7 / M16: `zfsbackup.config.store.trust` and the ephemeral-engine
plumbing in `store/db.py` and `store/paths.py`.

`trust.py` is the only code that reads or writes the six non-configuration tables
(`host_identity`, `clients`, `trusted_clients`, `revoked_fingerprints`, `enrollment_token`,
`trust_state`).
What is covered here, by class:

- `TestHostIdentity`      `read_client_id` / `ensure_client_id` / `set_client_id`: creation,
                          the config-generation bump, the compare-and-swap order
                          (grammar -> schema -> store id -> generation), the no-op case;
- `TestIdentityVersusConfigWrites`  `import_yaml` / `write_config` keep the id and every trust
                          table, `export` (the renderer) emits neither, the id shares a snapshot
                          with the config;
- `TestTokens`            `create_token` validation, TTL, scoping, the previous-status result,
                          `token_status` (boundaries, never the hash), `revoke_token`;
- `TestExpiryLatch`       the first redemption attempt that sees an expired token retires it
                          durably, so a clock stepping back cannot revive it; a row with ANY
                          `consumed_*` column set is spent;
- `TestEnrollOutcomes`    every `enroll_client` outcome, in the plan's check order, and what
                          each leaves behind (token consumed or not, pins, generation);
- `TestRevocation`        `revoke_pin`, `revoke_client` (tombstone, second revoke), dangling
                          predecessors;
- `TestEveryUnpinPathRecordsTheFingerprint`, `TestRevokedFingerprintReads`,
  `TestARevokedFingerprintIsNeverEnrolledAgain`, `TestRevokedRefusalTakesNoWriteLock`,
  `TestTokenPrecheckWithAFingerprint`, `TestRotationAndRevokedFingerprints`,
  `TestReplaceWithAnAlreadyPinnedCertificate`, `TestRevocationSurvivesImportAndWrites`
                          `revoked_fingerprints`: one record per unpinned fingerprint with its
                          reason, and a recorded fingerprint is refused by enroll and rotate (token
                          untouched, no write lock, re-checked under the lock);
- `TestRevokePinCascade`  `revoke_pin` also unpins the rotation successors, transitively, in one
                          transaction (`successor_revoked`, `PinRevocation.cascaded`), one bump; `chain()` plants the second link, since
                          `rotate_client` no longer builds a two-hop chain of live pins;
                          retire / `--replace` / `revoke_client` do not cascade; the thief scenario;
- `TestRevokePinRetiredChain`  `revoke_pin` of a key already RETIRED by rotation follows the
                          record's `superseded_by` (transitively, through retired intermediates and
                          pending successors) to every live descendant and returns a
                          `SupersededRevocation`; a dead chain is a `LookupError` with no write;
                          cycles terminate; `RevokedPin.superseded_by` on the reads;
- `TestRotateRetiresTheGrandparent`  `rotate_client(C -> D)` retires C's still-pinned predecessor
                          A (`superseded_by=C`) in the same transaction and bump; refused / already
                          / stale-field / other-client cases retire nothing;
- `TestRotation`          `rotate_client` (`rotated` / `already` / `refused`),
                          `retire_predecessor`;
- `TestTrustReads`        `read_trust` / `list_clients` / `TrustSnapshot.key`;
- `TestGenerations`       which operations bump the trust generation (and that none of them
                          touches the config generation), the store key and a replaced file;
- `TestTokenPrecheck`     `token_precheck`: the True/False matrix, read-only (fast under a held
                          `BEGIN IMMEDIATE`, no `_API_WRITE_LOCK`, no write, no latch), the
                          constant-time comparison (both digests always computed);
- `TestRefusedEnrollNeverTakesTheWriteLock`  a refused enroll never enters `_write_tx`
                          (spy AND a held raw write lock);
- `TestUnauthenticatedCallersCannotBurnAToken`  credentials are checked before the clock, so
                          only the secret's holder can latch an expired token;
- `TestCallableClock`     `now=` as a callable: read again under the write lock, that reading
                          drives every decision and timestamp;
- `TestOneSuccessorPerPredecessor`  `rotate_client` refuses a second, different successor;
                          `retire_predecessor` with nothing to retire takes no write lock;
- `TestApiWriteLock`      `_API_WRITE_LOCK`: bounded wait is `ConfigBusy` "nothing was written",
                          released on every path, replaced in a forked child;
- `TestErrorPaths`        old schema (every function), `ConfigBusy` (a raw `BEGIN IMMEDIATE`
                          holder, a held writer lock), missing database, uninitialised store;
- `TestLockContract`      the `lock=` argument of the CLI writers;
- `TestEphemeralEngine`   `db.ephemeral_engine` / `paths.open_config_session(ephemeral=True)`;
- `TestResourceHygiene`   no fd, no WAL read mark, no cached engine after many threaded calls;
  `dispose_all()` in another thread is harmless;
- `TestRaces` (integration)  enroll / retire / `ensure_client_id` races between threads and
                          between processes under fork, spawn and forkserver.

Hermetic: every database is a real file under `tmp_path`, always opened through
`store.db` (so `foreign_keys=ON`, WAL and `BEGIN IMMEDIATE` are the project's). Raw
`sqlite3` is used only to inspect the tables or to hold a lock on purpose. No zfs. Every
child process is joined, and killed if it does not finish, in a `finally`.

Helpers are local on purpose (a shared fixture module is not part of this item).
"""

import contextlib
import dataclasses
import hashlib
import multiprocessing
import os
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from zfsbackup.cli.render import render_yaml
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Duration,
    RetentionRule,
    client_id_problem,
)
from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    ConfigDbNotFound,
    EXPECTED_HEAD,
    ReadOnlySessionError,
    SchemaOutOfDate,
    SchemaVersionMismatch,
    StoreIdConflict,
    dispose_all,
    import_yaml,
    open_config_session,
    read_config,
    read_generation,
    resolve_config_path,
    write_config,
    writer_lock,
    writer_lock_path,
)
from zfsbackup.config.store import db as store_db
from zfsbackup.config.store import trust
from zfsbackup.config.store.trust import (
    TOKEN_MAX_TTL_SECONDS,
    ClientIdUpdate,
    ClientRecord,
    EnrollOutcome,
    NewPin,
    RotateOutcome,
    TokenState,
    TokenStatus,
    PinRevocation,
    RevokedPin,
    SupersededRevocation,
    TrustedPin,
    TrustKey,
    TrustSnapshot,
    create_token,
    enroll_client,
    ensure_client_id,
    list_clients,
    list_revoked,
    read_client_id,
    read_trust,
    read_trust_key,
    retire_predecessor,
    revoke_token,
    rotate_client,
    set_client_id,
    token_precheck,
    token_status,
)

BASELINE = "a1c28e1b3f34"           # the pre-M5 revision: no identity / trust tables

NOW = 1_700_000_000.0
SECRET = hashlib.sha256(b"enrollment secret").hexdigest()
OTHER_SECRET = hashlib.sha256(b"another secret").hexdigest()
SERVER = hashlib.sha256(b"server certificate").hexdigest()
OTHER_SERVER = hashlib.sha256(b"another server").hexdigest()

START_METHODS = [m for m in ("fork", "spawn", "forkserver")
                 if m in multiprocessing.get_all_start_methods()]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def revoke_pin(resolved, fingerprint, *, lock=None, **kwargs):
    """`trust.revoke_pin` with `lock=None` ("take the writer lock for this call") as the test
    default; `lock` is keyword-only and REQUIRED in the product (`TestLockContract`)."""
    return trust.revoke_pin(resolved, fingerprint, lock=lock, **kwargs)


def revoke_client(resolved, client_id, *, lock=None, **kwargs):
    """As `revoke_pin`, for `trust.revoke_client`."""
    return trust.revoke_client(resolved, client_id, lock=lock, **kwargs)


def fp(n: int) -> str:
    """A well-formed, distinct fingerprint per `n`."""
    return hashlib.sha256(f"client certificate {n}".encode()).hexdigest()


def newpin(n: int, not_after: float = NOW + 10_000_000) -> NewPin:
    return NewPin(fp(n), f"-----BEGIN CERTIFICATE-----\ncert-{n}\n-----END CERTIFICATE-----\n",
                  not_after)


def mktoken(resolved, *, client_id=None, replace=False, created=NOW, ttl=3600.0,
            secret=SECRET, server=SERVER):
    return create_token(
        resolved, secret_sha256=secret, server_fingerprint=server, created_at=created,
        expires_at=created + ttl, client_id=client_id, replace=replace, lock=None,
    )


def enroll(resolved, n=1, client_id="alpha", *, secret=SECRET, server=SERVER, now=NOW + 10,
           exists=False, label=None):
    return enroll_client(
        resolved, secret_sha256=secret, server_fingerprint=server, client_id=client_id,
        pin=newpin(n), label=label, now=now, dataset_exists=exists,
    )


def seed(resolved, client_id="alpha", *ns, label=None):
    """`client_id` known with pins `fp(n)` for each n: the first through an unscoped token, the
    rest through scoped ones (several fingerprints per id)."""
    for index, n in enumerate(ns):
        scoped = client_id if index else None
        mktoken(resolved, client_id=scoped)
        assert enroll(resolved, n, client_id, label=label if not index else None) \
            is EnrollOutcome.ENROLLED


def rotate(resolved, old, new, now=NOW + 20):
    return rotate_client(resolved, old_fp=fp(old), new_pin=newpin(new), now=now)


def tgen(resolved) -> int:
    return read_trust_key(resolved).generation


def cgen(resolved) -> int:
    """The config generation. `read_generation` is the config side's (cached-engine) read, so
    the cache is emptied again: the trust assertions about the cache must see only trust calls."""
    try:
        return read_generation(resolved)
    finally:
        dispose_all()


def pin_map(resolved):
    """{fingerprint: TrustedPin} of every live pin."""
    return {p.fingerprint: p for p in read_trust(resolved).pins}


def sql(db_path, statement, *params):
    """One raw statement (FK enforcement on), committed; rows returned."""
    conn = sqlite3.connect(str(db_path), timeout=10)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        rows = conn.execute(statement, params).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()


def token_row_consumed(resolved) -> bool:
    return token_status(resolved, NOW + 10).state is TokenState.CONSUMED


def token_row(db_path):
    rows = sql(db_path, "SELECT consumed_at, consumed_fingerprint, consumed_client_id "
                        "FROM enrollment_token")
    return rows[0] if rows else None


def assert_cache_empty():
    assert store_db._ENGINES == {}, "a trust call left an engine in the pid-keyed cache"


def open_fds_on(*paths) -> int:
    """How many of this process's descriptors refer to any of `paths` (inode compare)."""
    wanted = set()
    for path in paths:
        try:
            st = os.stat(str(path))
        except OSError:
            continue
        wanted.add((st.st_dev, st.st_ino))
    count = 0
    for entry in os.listdir("/dev/fd"):
        try:
            st = os.fstat(int(entry))
        except (OSError, ValueError):
            continue
        if (st.st_dev, st.st_ino) in wanted:
            count += 1
    return count


def fd_count() -> int:
    return len(os.listdir("/dev/fd"))


def checkpoint_busy(db_path) -> int:
    """`wal_checkpoint(TRUNCATE)`'s `busy` column: nonzero while ANY connection holds a WAL read
    mark (or a write lock)."""
    conn = sqlite3.connect(str(db_path), timeout=0.3, isolation_level=None)
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        return conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]
    finally:
        conn.close()


@contextlib.contextmanager
def write_lock_held(db_path):
    """A raw connection that owns SQLite's write lock (`BEGIN IMMEDIATE`) until the block ends."""
    conn = sqlite3.connect(str(db_path), timeout=0, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
    finally:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        conn.close()


def run_threads(count, target):
    """Run `target(index)` on `count` threads released together; return their results in index
    order. A worker's exception is returned as a value so the test can assert on it."""
    barrier = threading.Barrier(count)
    results = [None] * count

    def body(index):
        try:
            barrier.wait(30)
            results[index] = target(index)
        except BaseException as exc:  # noqa: BLE001 - reported, never swallowed
            results[index] = exc

    threads = [threading.Thread(target=body, args=(i,)) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert not any(t.is_alive() for t in threads), "a worker thread hung"
    return results


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

MINIMAL_YAML = "datasets:\n  - name: tank/x\n    frequency: 1h\n"


def minimal_config(name="tank/x") -> BackupConfig:
    return BackupConfig(
        snapshot_prefix="auto",
        datasets=[DatasetConfig(
            name=name, frequency=Duration("1h"),
            retention_rules=[RetentionRule(age=Duration("1d"), keep_for=Duration("30d"))],
        )],
    )


def make_store(directory: Path, *, template: Path = None):
    """An imported config database in `directory` (created); its `ResolvedConfigPath`.

    With `template` (a checkpointed, sidecar-free copy of a freshly imported database) the file
    is copied instead of imported again: the import costs ~50 ms and most tests need only an
    initialised store. The copy shares the template's `store_id`, which no test relies on being
    unique except `test_replacing_the_database_file...`, which imports for real."""
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "config.db"
    if template is not None:
        shutil.copyfile(template, target)
        os.chmod(target, 0o640)
        return resolve_config_path(str(target))
    yaml_path = directory / "config.yaml"
    yaml_path.write_text(MINIMAL_YAML)
    resolved = resolve_config_path(str(target))
    import_yaml(yaml_path, resolved)
    dispose_all()
    return resolved


@pytest.fixture(scope="module")
def template_db(tmp_path_factory):
    directory = tmp_path_factory.mktemp("trust_template")
    resolved = make_store(directory)
    conn = sqlite3.connect(str(resolved.path), isolation_level=None)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
    finally:
        conn.close()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(resolved.path) + suffix)
        if sidecar.exists():
            sidecar.unlink()
    return resolved.path


@pytest.fixture(autouse=True)
def _clean_engine_cache():
    dispose_all()
    yield
    dispose_all()


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "store" / "config.db"


@pytest.fixture
def resolved(template_db, db_path):
    return make_store(db_path.parent, template=template_db)


@pytest.fixture
def yaml_path(resolved, db_path):
    path = db_path.parent / "config.yaml"
    path.write_text(MINIMAL_YAML)
    return path


@pytest.fixture
def short_busy(monkeypatch):
    """Cap SQLite's busy timeout so a contended write fails in ~0.1 s instead of 5 s."""
    monkeypatch.setattr(store_db, "_BUSY_TIMEOUT_MS", 100)


# ===========================================================================
# Host identity
# ===========================================================================


@pytest.mark.unit
class TestHostIdentity:
    def test_a_fresh_store_has_no_id(self, resolved):
        assert read_client_id(resolved) is None

    def test_ensure_creates_once_and_bumps_the_config_generation_once(self, resolved):
        before = cgen(resolved)
        client_id, created = ensure_client_id(resolved, lock=None)
        assert created is True
        assert client_id_problem(client_id) is None
        assert cgen(resolved) == before + 1
        assert read_client_id(resolved) == client_id

        again, created_again = ensure_client_id(resolved, lock=None)
        assert (again, created_again) == (client_id, False)
        assert cgen(resolved) == before + 1          # nothing written, nothing bumped
        assert_cache_empty()

    def test_ensure_never_touches_the_trust_generation(self, resolved):
        ensure_client_id(resolved, lock=None)
        assert tgen(resolved) == 0

    def test_ensure_works_under_a_lock_the_caller_already_holds(self, resolved):
        with writer_lock(resolved) as held:
            client_id, created = ensure_client_id(resolved, lock=held)
            assert created
            assert ensure_client_id(resolved, lock=held) == (client_id, False)

    def test_ensure_is_a_value_error_on_an_uninitialised_store(self, resolved, db_path):
        sql(db_path, "DELETE FROM global_settings")
        with pytest.raises(ValueError, match="uninitialized"):
            ensure_client_id(resolved, lock=None)
        assert read_client_id(resolved) is None

    def test_set_creates_the_row_and_bumps(self, resolved):
        generation = cgen(resolved)
        update = set_client_id(resolved, "web-1", expected_generation=generation, lock=None)
        assert update == ClientIdUpdate("web-1", None, generation + 1, True)
        assert read_client_id(resolved) == "web-1"
        assert cgen(resolved) == generation + 1

    def test_set_replaces_the_id_records_the_previous_one_and_stamps_updated_at(
        self, resolved, db_path
    ):
        set_client_id(resolved, "web-1", expected_generation=None, lock=None)
        generation = cgen(resolved)
        update = set_client_id(resolved, "web-2", expected_generation=generation, lock=None)
        assert (update.client_id, update.previous, update.changed) == ("web-2", "web-1", True)
        assert update.generation == generation + 1 == cgen(resolved)
        created_at, updated_at = sql(db_path, "SELECT created_at, updated_at FROM host_identity")[0]
        assert updated_at is not None and updated_at >= created_at

    def test_setting_the_id_it_already_has_is_a_no_op(self, resolved):
        set_client_id(resolved, "web-1", expected_generation=None, lock=None)
        generation = cgen(resolved)
        update = set_client_id(resolved, "web-1", expected_generation=generation, lock=None)
        assert update == ClientIdUpdate("web-1", "web-1", generation, False)
        assert cgen(resolved) == generation              # no bump, no reload

    def test_a_stale_generation_is_a_conflict_and_changes_nothing(self, resolved):
        generation = cgen(resolved)
        set_client_id(resolved, "web-1", expected_generation=generation, lock=None)
        with pytest.raises(ConfigConflict) as info:
            set_client_id(resolved, "web-2", expected_generation=generation, lock=None)
        assert (info.value.expected, info.value.actual) == (generation, generation + 1)
        assert read_client_id(resolved) == "web-1"
        assert cgen(resolved) == generation + 1

    def test_the_generation_is_compared_before_the_no_op_shortcut(self, resolved):
        generation = cgen(resolved)
        set_client_id(resolved, "web-1", expected_generation=generation, lock=None)
        with pytest.raises(ConfigConflict):
            set_client_id(resolved, "web-1", expected_generation=generation, lock=None)

    def test_none_means_unconditional(self, resolved):
        set_client_id(resolved, "web-1", expected_generation=None, lock=None)
        set_client_id(resolved, "web-2", expected_generation=None, lock=None)
        assert read_client_id(resolved) == "web-2"

    def test_a_replaced_database_is_a_store_id_conflict(self, resolved):
        key = read_trust_key(resolved)
        with pytest.raises(StoreIdConflict) as info:
            set_client_id(resolved, "web-1", expected_generation=cgen(resolved),
                          expected_store_id=key.store_id + 1, lock=None)
        assert info.value.expected_store_id == key.store_id + 1
        assert info.value.actual_store_id == key.store_id
        assert isinstance(info.value, ConfigConflict)
        assert read_client_id(resolved) is None

    def test_a_matching_or_zero_store_id_is_accepted(self, resolved):
        key = read_trust_key(resolved)
        set_client_id(resolved, "web-1", expected_generation=None,
                      expected_store_id=key.store_id, lock=None)
        set_client_id(resolved, "web-2", expected_generation=None, expected_store_id=0, lock=None)
        assert read_client_id(resolved) == "web-2"

    def test_the_store_id_is_checked_before_the_generation(self, resolved):
        key = read_trust_key(resolved)
        with pytest.raises(StoreIdConflict):
            set_client_id(resolved, "web-1", expected_generation=cgen(resolved) + 99,
                          expected_store_id=key.store_id + 1, lock=None)

    @pytest.mark.parametrize("bad", [
        "", ".hidden", "a/b", "a b", "café", "x" * 65, "a\n", "..", None, 7, b"abc",
        "-", "-a", "_", "_a",
    ])
    def test_the_grammar_is_enforced_before_anything_is_opened(self, tmp_path, bad):
        # No database exists at all: a ValueError (not ConfigDbNotFound) proves the grammar check
        # runs first.
        missing = resolve_config_path(str(tmp_path / "nowhere" / "config.db"))
        with pytest.raises(ValueError):
            set_client_id(missing, bad, expected_generation=0, lock=None)
        assert not (tmp_path / "nowhere").exists()

    @pytest.mark.parametrize("good", ["a", "x" * 64, "web-1.example_org", "A.b-C_9"])
    def test_valid_ids_are_stored_verbatim(self, resolved, good):
        set_client_id(resolved, good, expected_generation=None, lock=None)
        assert read_client_id(resolved) == good

    def test_set_is_a_value_error_on_an_uninitialised_store(self, resolved, db_path):
        sql(db_path, "DELETE FROM global_settings")
        with pytest.raises(ValueError, match="uninitialized"):
            set_client_id(resolved, "web-1", expected_generation=None, lock=None)

    def test_the_schema_is_checked_before_the_store_id_and_the_generation(self, resolved, db_path):
        sql(db_path, "UPDATE alembic_version SET version_num = ?", BASELINE)
        with pytest.raises(SchemaOutOfDate):
            set_client_id(resolved, "web-1", expected_generation=999,
                          expected_store_id=123, lock=None)

    def test_no_engine_is_left_in_the_cache(self, resolved):
        ensure_client_id(resolved, lock=None)
        set_client_id(resolved, "web-1", expected_generation=None, lock=None)
        read_client_id(resolved)
        assert_cache_empty()


# ===========================================================================
# Import / write_config / export versus the identity and trust tables
# ===========================================================================


@pytest.mark.unit
class TestIdentityVersusConfigWrites:
    def _populate(self, resolved):
        client_id, _ = ensure_client_id(resolved, lock=None)
        seed(resolved, "alpha", 1, 2, label="the first")
        mktoken(resolved, client_id="alpha")
        revoke_client(resolved, "alpha", now=NOW + 5)
        seed(resolved, "beta", 3)
        return client_id

    def _state(self, resolved):
        return (
            read_client_id(resolved),
            list_clients(resolved),
            read_trust(resolved).pins,
            token_status(resolved, NOW + 50),
            tgen(resolved),
        )

    def test_a_full_import_keeps_the_id_and_every_trust_table(self, resolved, yaml_path):
        self._populate(resolved)
        before = self._state(resolved)
        import_yaml(yaml_path, resolved)
        dispose_all()
        assert self._state(resolved) == before
        assert before[0] is not None and len(before[1]) == 2 and before[4] > 0

    def test_write_config_keeps_the_id_and_every_trust_table(self, resolved):
        self._populate(resolved)
        before = self._state(resolved)
        write_config(resolved, minimal_config("tank/y"), expected_generation=cgen(resolved))
        dispose_all()
        assert self._state(resolved) == before

    def test_a_trust_write_never_moves_the_config_generation(self, resolved):
        generation = cgen(resolved)
        seed(resolved, "alpha", 1, 2)
        rotate(resolved, 1, 3)
        retire_predecessor(resolved, fp(3))
        revoke_pin(resolved, fp(2))
        revoke_client(resolved, "alpha")
        mktoken(resolved)
        revoke_token(resolved, lock=None)
        assert cgen(resolved) == generation

    def test_a_snapshot_read_before_ensure_cannot_write_afterwards(self, resolved):
        snapshot = read_config(resolved)
        dispose_all()
        ensure_client_id(resolved, lock=None)
        with pytest.raises(ConfigConflict):
            write_config(resolved, snapshot.config, expected_generation=snapshot.generation)
        dispose_all()

    def test_the_id_is_read_in_the_same_snapshot_as_the_config(self, resolved):
        first = read_config(resolved)
        dispose_all()
        assert first.client_id is None
        client_id, _ = ensure_client_id(resolved, lock=None)
        second = read_config(resolved)
        dispose_all()
        assert second.client_id == client_id
        assert second.generation == first.generation + 1
        assert second.config.same_as(first.config)      # the id is not part of the config
        set_client_id(resolved, "renamed", expected_generation=second.generation, lock=None)
        third = read_config(resolved)
        dispose_all()
        assert (third.client_id, third.generation) == ("renamed", second.generation + 1)

    def test_export_emits_neither_the_id_nor_any_trust_data(self, resolved):
        client_id = self._populate(resolved)
        snapshot = read_config(resolved)
        dispose_all()
        rendered = render_yaml(snapshot.config)
        assert client_id not in rendered
        for needle in ("client_id", "alpha", "beta", fp(1), "host_identity", "trusted",
                       "enrollment"):
            assert needle not in rendered


# ===========================================================================
# Tokens
# ===========================================================================


@pytest.mark.unit
class TestTokens:
    def test_no_token_is_state_none(self, resolved):
        assert token_status(resolved, NOW) == TokenStatus(TokenState.NONE)

    def test_create_stores_an_unscoped_active_token(self, resolved):
        previous = mktoken(resolved)
        assert previous.state is TokenState.NONE
        status = token_status(resolved, NOW + 1)
        assert status == TokenStatus(
            TokenState.ACTIVE, created_at=NOW, expires_at=NOW + 3600, server_fingerprint=SERVER,
            client_id=None, replace=False)

    def test_the_status_never_carries_the_hash(self, resolved):
        mktoken(resolved)
        status = token_status(resolved, NOW + 1)
        assert not [f.name for f in dataclasses.fields(TokenStatus)
                    if "secret" in f.name or "sha" in f.name or "hash" in f.name]
        assert SECRET not in repr(status) and SECRET not in repr(dataclasses.astuple(status))

    def test_create_returns_the_previous_status_evaluated_at_the_new_creation_time(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        assert mktoken(resolved, created=NOW + 50, ttl=100).state is TokenState.ACTIVE
        # the second token runs NOW+50 .. NOW+150
        previous = mktoken(resolved, created=NOW + 500, ttl=100)
        assert previous.state is TokenState.EXPIRED
        assert (previous.created_at, previous.expires_at) == (NOW + 50, NOW + 150)

    def test_create_reports_a_consumed_token_as_replaced(self, resolved):
        mktoken(resolved)
        assert enroll(resolved) is EnrollOutcome.ENROLLED
        previous = mktoken(resolved, secret=OTHER_SECRET)
        assert previous.state is TokenState.CONSUMED
        assert previous.consumed_client_id == "alpha"
        assert token_status(resolved, NOW + 1).state is TokenState.ACTIVE

    def test_a_new_token_replaces_the_old_one(self, resolved):
        mktoken(resolved)
        mktoken(resolved, secret=OTHER_SECRET)
        assert enroll(resolved, secret=SECRET) is EnrollOutcome.REFUSED      # the old secret
        assert enroll(resolved, secret=OTHER_SECRET) is EnrollOutcome.ENROLLED

    def test_the_fingerprint_is_normalised(self, resolved):
        coloned = ":".join(SERVER[i:i + 2].upper() for i in range(0, 64, 2))
        mktoken(resolved, server=coloned)
        assert token_status(resolved, NOW + 1).server_fingerprint == SERVER

    def test_a_ttl_of_exactly_seven_days_is_accepted(self, resolved):
        mktoken(resolved, ttl=TOKEN_MAX_TTL_SECONDS)
        assert token_status(resolved, NOW + 1).expires_at == NOW + TOKEN_MAX_TTL_SECONDS

    @pytest.mark.parametrize("ttl", [TOKEN_MAX_TTL_SECONDS + 1, TOKEN_MAX_TTL_SECONDS * 2, 1e12])
    def test_a_ttl_over_seven_days_is_refused(self, resolved, ttl):
        with pytest.raises(ValueError, match="7 days"):
            mktoken(resolved, ttl=ttl)
        assert token_status(resolved, NOW).state is TokenState.NONE

    @pytest.mark.parametrize("ttl", [0, -1])
    def test_expiry_must_follow_creation(self, resolved, ttl):
        with pytest.raises(ValueError):
            mktoken(resolved, ttl=ttl)

    @pytest.mark.parametrize("bad", [
        "", "abc", "A" * 64, "g" * 64, "a" * 63, "a" * 65, None, 123, b"a" * 64, SECRET + "\n",
    ])
    def test_a_bad_secret_hash_is_refused(self, resolved, bad):
        with pytest.raises(ValueError):
            mktoken(resolved, secret=bad)
        assert token_status(resolved, NOW).state is TokenState.NONE

    @pytest.mark.parametrize("bad", ["", "xyz", "a" * 63, None])
    def test_a_bad_server_fingerprint_is_refused(self, resolved, bad):
        with pytest.raises(ValueError):
            mktoken(resolved, server=bad)

    @pytest.mark.parametrize("created", [float("nan"), float("inf"), "now", True])
    def test_non_finite_or_non_numeric_times_are_refused(self, resolved, created):
        with pytest.raises(ValueError):
            create_token(resolved, secret_sha256=SECRET, server_fingerprint=SERVER,
                         created_at=created, expires_at=NOW + 10, lock=None)

    def test_replace_without_a_client_id_is_refused(self, resolved):
        with pytest.raises(ValueError, match="requires client_id"):
            mktoken(resolved, replace=True)

    def test_replace_must_be_a_bool(self, resolved):
        seed(resolved, "alpha", 1)
        with pytest.raises(ValueError):
            create_token(resolved, secret_sha256=SECRET, server_fingerprint=SERVER,
                         created_at=NOW, expires_at=NOW + 10, client_id="alpha",
                         replace=1, lock=None)

    def test_a_bad_client_id_grammar_is_refused(self, resolved):
        with pytest.raises(ValueError):
            mktoken(resolved, client_id="../etc")

    @pytest.mark.parametrize("bad", ["-a", "_a", "-", "_", ".a"])
    def test_a_client_id_must_start_with_a_letter_or_digit(self, resolved, bad):
        with pytest.raises(ValueError, match="start with an ASCII letter or digit"):
            mktoken(resolved, client_id=bad)
        with pytest.raises(ValueError, match="start with an ASCII letter or digit"):
            enroll(resolved, 1, bad)

    def test_a_scoped_token_needs_a_known_client(self, resolved):
        with pytest.raises(LookupError):
            mktoken(resolved, client_id="ghost")
        assert token_status(resolved, NOW).state is TokenState.NONE

    def test_a_failed_create_keeps_the_previous_token(self, resolved):
        mktoken(resolved)
        with pytest.raises(LookupError):
            mktoken(resolved, client_id="ghost", secret=OTHER_SECRET)
        assert enroll(resolved) is EnrollOutcome.ENROLLED               # the original secret

    def test_a_scoped_token_records_its_client_and_replace_flag(self, resolved):
        seed(resolved, "alpha", 1)
        mktoken(resolved, client_id="alpha", replace=True)
        status = token_status(resolved, NOW + 1)
        assert (status.client_id, status.replace) == ("alpha", True)

    def test_a_scoped_token_may_name_a_revoked_client(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_client(resolved, "alpha")
        mktoken(resolved, client_id="alpha")
        assert token_status(resolved, NOW + 1).client_id == "alpha"

    def test_status_boundaries(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        assert token_status(resolved, NOW - 0.001).state is TokenState.EXPIRED   # clock behind
        assert token_status(resolved, NOW).state is TokenState.ACTIVE            # inclusive start
        assert token_status(resolved, NOW + 99.999).state is TokenState.ACTIVE
        assert token_status(resolved, NOW + 100).state is TokenState.EXPIRED     # exclusive end

    def test_reading_the_status_does_not_retire_an_expired_token(self, resolved, db_path):
        mktoken(resolved, created=NOW, ttl=100)
        assert token_status(resolved, NOW + 500).state is TokenState.EXPIRED
        assert token_row(db_path) == (None, None, None)
        assert token_status(resolved, NOW + 5).state is TokenState.ACTIVE

    def test_status_of_a_non_finite_now_is_refused(self, resolved):
        with pytest.raises(ValueError):
            token_status(resolved, float("nan"))

    def test_revoke_token_reports_whether_a_row_was_removed(self, resolved):
        assert revoke_token(resolved, lock=None) is False
        mktoken(resolved)
        assert revoke_token(resolved, lock=None) is True
        assert token_status(resolved, NOW + 1).state is TokenState.NONE
        assert revoke_token(resolved, lock=None) is False

    def test_revoke_token_removes_a_consumed_token_too(self, resolved):
        mktoken(resolved)
        enroll(resolved)
        assert revoke_token(resolved, lock=None) is True
        assert token_status(resolved, NOW + 1).state is TokenState.NONE

    def test_a_revoked_token_can_no_longer_be_redeemed(self, resolved):
        mktoken(resolved)
        revoke_token(resolved, lock=None)
        assert enroll(resolved) is EnrollOutcome.REFUSED

    def test_token_operations_do_not_bump_the_trust_generation(self, resolved):
        mktoken(resolved)
        revoke_token(resolved, lock=None)
        mktoken(resolved, ttl=10)
        assert tgen(resolved) == 0


# ===========================================================================
# The expiry latch and "spent" rows
# ===========================================================================


@pytest.mark.unit
class TestExpiryLatch:
    def test_the_first_enroll_that_sees_it_expired_retires_it_durably(self, resolved, db_path):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, now=NOW + 200) is EnrollOutcome.REFUSED
        consumed_at, consumed_fp, consumed_cid = token_row(db_path)
        assert consumed_at == NOW + 200 and consumed_fp is None and consumed_cid is None
        assert token_status(resolved, NOW + 200).state is TokenState.EXPIRED

    def test_stepping_the_clock_back_cannot_revive_it(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, now=NOW + 200) is EnrollOutcome.REFUSED
        assert enroll(resolved, now=NOW + 50) is EnrollOutcome.REFUSED
        assert token_status(resolved, NOW + 50).state is TokenState.EXPIRED
        assert read_trust(resolved).pins == ()
        assert tgen(resolved) == 0

    def test_without_a_failed_redemption_the_clock_back_is_still_valid(self, resolved):
        """The latch is set by a redemption ATTEMPT, not by the passage of time or by a read."""
        mktoken(resolved, created=NOW, ttl=100)
        assert token_status(resolved, NOW + 200).state is TokenState.EXPIRED
        assert enroll(resolved, now=NOW + 50) is EnrollOutcome.ENROLLED

    def test_the_end_of_the_window_is_exclusive_and_latches(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, now=NOW + 99.999) is EnrollOutcome.ENROLLED
        mktoken(resolved, created=NOW, ttl=100, secret=OTHER_SECRET)
        assert enroll(resolved, 2, secret=OTHER_SECRET, now=NOW + 100) is EnrollOutcome.REFUSED
        assert token_status(resolved, NOW + 50).state is TokenState.EXPIRED

    def test_the_start_of_the_window_is_inclusive(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, now=NOW) is EnrollOutcome.ENROLLED

    def test_a_clock_before_creation_is_refused_but_not_latched(self, resolved, db_path):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, now=NOW - 5) is EnrollOutcome.REFUSED
        assert token_row(db_path) == (None, None, None)
        assert enroll(resolved, now=NOW + 5) is EnrollOutcome.ENROLLED

    def test_a_row_with_consumed_at_set_and_no_fingerprint_is_refused(self, resolved, db_path):
        mktoken(resolved)
        sql(db_path, "UPDATE enrollment_token SET consumed_at = ?", NOW + 1)
        assert enroll(resolved, now=NOW + 10) is EnrollOutcome.REFUSED
        assert read_trust(resolved).pins == ()
        assert token_status(resolved, NOW + 10).state is TokenState.EXPIRED

    def test_a_row_with_only_a_consumed_fingerprint_is_spent(self, resolved, db_path):
        mktoken(resolved)
        sql(db_path, "UPDATE enrollment_token SET consumed_fingerprint = ?", fp(9))
        assert enroll(resolved) is EnrollOutcome.REFUSED
        assert token_status(resolved, NOW + 1).state is TokenState.CONSUMED

    def test_a_row_with_only_a_consumed_client_id_is_spent(self, resolved, db_path):
        mktoken(resolved)
        sql(db_path, "UPDATE enrollment_token SET consumed_client_id = 'zed'")
        assert enroll(resolved) is EnrollOutcome.REFUSED
        assert token_status(resolved, NOW + 1).state is TokenState.CONSUMED

    def test_the_latch_does_not_overwrite_an_earlier_retirement(self, resolved, db_path):
        mktoken(resolved, created=NOW, ttl=100)
        enroll(resolved, now=NOW + 200)
        enroll(resolved, now=NOW + 300)
        assert token_row(db_path)[0] == NOW + 200

    def test_a_fresh_token_replaces_a_latched_one(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        enroll(resolved, now=NOW + 200)
        previous = mktoken(resolved, created=NOW + 300, ttl=100)
        assert previous.state is TokenState.EXPIRED
        assert enroll(resolved, now=NOW + 310) is EnrollOutcome.ENROLLED

    def test_the_latch_does_not_bump_the_trust_generation(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        enroll(resolved, now=NOW + 200)
        assert tgen(resolved) == 0


# ===========================================================================
# enroll_client outcomes
# ===========================================================================


@pytest.mark.unit
class TestEnrollOutcomes:
    def test_an_unscoped_token_enrolls_a_new_client(self, resolved):
        mktoken(resolved)
        assert enroll(resolved, label="laptop") is EnrollOutcome.ENROLLED
        (record,) = list_clients(resolved)
        assert (record.client_id, record.label, record.revoked) == ("alpha", "laptop", False)
        assert record.created_at == NOW + 10
        (pin,) = record.pins
        assert (pin.fingerprint, pin.client_id, pin.enrolled_via, pin.predecessor) == (
            fp(1), "alpha", "token", None)
        assert pin.enrolled_at == NOW + 10 and pin.not_after == newpin(1).not_after
        assert pin.cert_pem == newpin(1).cert_pem
        assert tgen(resolved) == 1
        assert_cache_empty()

    def test_enrolling_consumes_the_token_and_records_who(self, resolved):
        mktoken(resolved)
        enroll(resolved, 4, "gamma")
        status = token_status(resolved, NOW + 11)
        assert status.state is TokenState.CONSUMED
        assert (status.consumed_fingerprint, status.consumed_client_id) == (fp(4), "gamma")
        assert status.consumed_at == NOW + 10

    def test_a_token_is_good_for_exactly_one_enrollment(self, resolved):
        mktoken(resolved)
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.ENROLLED
        assert enroll(resolved, 2, "beta") is EnrollOutcome.REFUSED
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED     # even the same client
        assert [c.client_id for c in list_clients(resolved)] == ["alpha"]
        assert tgen(resolved) == 1

    @pytest.mark.parametrize("what", [
        "no_token", "wrong_secret", "wrong_server", "expired", "before_creation", "consumed",
        "malformed_hash_short", "malformed_hash_upper", "malformed_hash_none",
        "malformed_hash_bytes",
    ])
    def test_every_token_failure_is_the_same_refusal_and_enrolls_nothing(self, resolved, what):
        if what != "no_token":
            mktoken(resolved, created=NOW, ttl=100)
        kwargs = {"now": NOW + 10}
        if what == "wrong_secret":
            kwargs["secret"] = OTHER_SECRET
        elif what == "wrong_server":
            kwargs["server"] = OTHER_SERVER
        elif what == "expired":
            kwargs["now"] = NOW + 100
        elif what == "before_creation":
            kwargs["now"] = NOW - 1
        elif what == "consumed":
            assert enroll(resolved, 9, "other") is EnrollOutcome.ENROLLED
        elif what == "malformed_hash_short":
            kwargs["secret"] = SECRET[:-1]
        elif what == "malformed_hash_upper":
            kwargs["secret"] = SECRET.upper()
        elif what == "malformed_hash_none":
            kwargs["secret"] = None
        elif what == "malformed_hash_bytes":
            kwargs["secret"] = SECRET.encode()
        generation = tgen(resolved)
        assert enroll(resolved, 1, "alpha", **kwargs) is EnrollOutcome.REFUSED
        assert fp(1) not in pin_map(resolved)
        assert "alpha" not in [c.client_id for c in list_clients(resolved)]
        assert tgen(resolved) == generation

    def test_a_refused_attempt_does_not_consume_a_live_token(self, resolved):
        mktoken(resolved)
        assert enroll(resolved, secret=OTHER_SECRET) is EnrollOutcome.REFUSED
        assert enroll(resolved, server=OTHER_SERVER) is EnrollOutcome.REFUSED
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE
        assert enroll(resolved) is EnrollOutcome.ENROLLED

    def test_the_server_fingerprint_may_be_given_in_any_accepted_spelling(self, resolved):
        mktoken(resolved)
        coloned = ":".join(SERVER[i:i + 2].upper() for i in range(0, 64, 2))
        assert enroll(resolved, server=coloned) is EnrollOutcome.ENROLLED

    def test_both_secrets_are_always_compared(self, resolved, monkeypatch):
        """No early exit tells a fingerprint mismatch from a secret mismatch."""
        mktoken(resolved)
        calls = []
        real = trust.hmac.compare_digest

        def counting(a, b):
            calls.append((a, b))
            return real(a, b)

        monkeypatch.setattr(trust.hmac, "compare_digest", counting)
        assert enroll(resolved, server=OTHER_SERVER) is EnrollOutcome.REFUSED
        assert len(calls) == 2
        calls.clear()
        assert enroll(resolved, secret="not hex") is EnrollOutcome.REFUSED
        assert len(calls) == 2

    def test_programming_errors_raise_value_error(self, resolved):
        mktoken(resolved)
        with pytest.raises(ValueError):
            enroll(resolved, client_id="not/valid")
        with pytest.raises(ValueError):
            enroll(resolved, server="short")
        with pytest.raises(ValueError):
            enroll(resolved, now=float("nan"))
        with pytest.raises(ValueError):
            enroll(resolved, label=5)
        with pytest.raises(ValueError):
            enroll_client(resolved, secret_sha256=SECRET, server_fingerprint=SERVER,
                          client_id="alpha", pin=fp(1), now=NOW, dataset_exists=False)
        assert token_status(resolved, NOW + 1).state is TokenState.ACTIVE

    def test_the_new_pin_validates_its_inputs(self):
        assert NewPin(fp(1).upper(), "pem", 5).fingerprint == fp(1)       # normalised
        with pytest.raises(ValueError):
            NewPin("short", "pem", 5.0)
        with pytest.raises(ValueError):
            NewPin(fp(1), "   ", 5.0)
        with pytest.raises(ValueError):
            NewPin(fp(1), "pem", float("inf"))
        with pytest.raises(ValueError):
            NewPin(fp(1), "pem", "tomorrow")
        with pytest.raises(dataclasses.FrozenInstanceError):
            newpin(1).fingerprint = fp(2)

    # --- already_trusted / cert_bound_elsewhere (check 2) -------------------------------------

    def test_the_same_certificate_for_the_same_id_is_already_trusted_and_leaves_the_token(
        self, resolved
    ):
        seed(resolved, "alpha", 1)
        generation = tgen(resolved)
        mktoken(resolved)
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.ALREADY_TRUSTED
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE
        assert tgen(resolved) == generation
        assert len(pin_map(resolved)) == 1

    def test_a_certificate_pinned_to_another_id_is_bound_elsewhere(self, resolved):
        seed(resolved, "alpha", 1)
        generation = tgen(resolved)
        mktoken(resolved)
        assert enroll(resolved, 1, "beta") is EnrollOutcome.CERT_BOUND_ELSEWHERE
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE
        assert [c.client_id for c in list_clients(resolved)] == ["alpha"]
        assert tgen(resolved) == generation

    def test_a_scoped_token_does_not_override_a_binding_elsewhere(self, resolved):
        seed(resolved, "alpha", 1)
        seed(resolved, "beta", 2)
        mktoken(resolved, client_id="beta")
        assert enroll(resolved, 1, "beta") is EnrollOutcome.CERT_BOUND_ELSEWHERE
        assert pin_map(resolved)[fp(1)].client_id == "alpha"

    def test_already_trusted_is_decided_before_dataset_exists_is_considered(self, resolved):
        seed(resolved, "alpha", 1)
        mktoken(resolved)
        assert enroll(resolved, 1, "alpha", exists=True) is EnrollOutcome.ALREADY_TRUSTED

    def test_the_token_is_checked_before_the_certificate(self, resolved):
        """An already-pinned certificate is not told so unless the token is good."""
        seed(resolved, "alpha", 1)
        mktoken(resolved)
        assert enroll(resolved, 1, "alpha", secret=OTHER_SECRET) is EnrollOutcome.REFUSED
        assert enroll(resolved, 1, "beta", secret=OTHER_SECRET) is EnrollOutcome.REFUSED

    def test_a_pin_of_a_revoked_client_fails_closed(self, resolved, db_path):
        """Revoking deletes the pins, so this row cannot arise through the API. If one is
        planted, it must never read as `already_trusted`."""
        seed(resolved, "alpha", 1)
        sql(db_path, "UPDATE clients SET revoked_at = ? WHERE client_id = 'alpha'", NOW + 1)
        mktoken(resolved)
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE

    # --- client_id_taken (check 4) ------------------------------------------------------------

    def test_an_unscoped_token_cannot_claim_an_existing_row(self, resolved):
        seed(resolved, "alpha", 1)
        generation = tgen(resolved)
        mktoken(resolved)
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.CLIENT_ID_TAKEN
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE
        assert list(pin_map(resolved)) == [fp(1)]
        assert tgen(resolved) == generation

    def test_an_unscoped_token_cannot_claim_a_tombstone(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_client(resolved, "alpha")
        generation = tgen(resolved)
        mktoken(resolved)
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.CLIENT_ID_TAKEN
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE
        assert read_trust(resolved).pins == ()
        assert list_clients(resolved)[0].revoked
        assert tgen(resolved) == generation

    def test_an_unscoped_token_cannot_claim_an_id_whose_dataset_exists(self, resolved):
        mktoken(resolved)
        assert enroll(resolved, 1, "alpha", exists=True) is EnrollOutcome.CLIENT_ID_TAKEN
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE
        assert list_clients(resolved) == ()           # no row was created for the squatter
        assert tgen(resolved) == 0

    def test_after_client_id_taken_the_same_token_still_works_for_a_free_id(self, resolved):
        seed(resolved, "alpha", 1)
        mktoken(resolved)
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.CLIENT_ID_TAKEN
        assert enroll(resolved, 2, "beta") is EnrollOutcome.ENROLLED

    # --- scoped tokens (check 3) --------------------------------------------------------------

    def test_a_scoped_token_adds_a_pin_to_its_client(self, resolved):
        seed(resolved, "alpha", 1, label="orig")
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.ENROLLED
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2)])
        (record,) = list_clients(resolved)
        assert record.label == "orig" and len(record.pins) == 2   # label kept when none is given
        assert token_status(resolved, NOW + 11).state is TokenState.CONSUMED

    def test_a_scoped_token_ignores_dataset_exists(self, resolved):
        seed(resolved, "alpha", 1)
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 2, "alpha", exists=True) is EnrollOutcome.ENROLLED

    def test_a_scoped_token_refuses_any_other_id_and_is_not_consumed(self, resolved):
        seed(resolved, "alpha", 1)
        generation = tgen(resolved)
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 2, "beta") is EnrollOutcome.REFUSED
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE
        assert [c.client_id for c in list_clients(resolved)] == ["alpha"]
        assert tgen(resolved) == generation

    def test_a_scoped_token_updates_the_label_only_when_one_is_given(self, resolved):
        seed(resolved, "alpha", 1, label="orig")
        mktoken(resolved, client_id="alpha")
        enroll(resolved, 2, "alpha", label="renamed")
        assert list_clients(resolved)[0].label == "renamed"

    def test_a_scoped_token_clears_a_tombstone(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_client(resolved, "alpha", now=NOW + 5)
        assert read_trust(resolved).pins == ()
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.ENROLLED
        (record,) = list_clients(resolved)
        assert not record.revoked and record.revoked_at is None
        assert [p.fingerprint for p in read_trust(resolved).pins] == [fp(2)]

    def test_a_scoped_token_recreates_a_vanished_client_row(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha")
        # the revocation row of fp(1) references the client (FK): drop it first
        sql(db_path, "DELETE FROM revoked_fingerprints")
        sql(db_path, "DELETE FROM clients WHERE client_id = 'alpha'")
        assert enroll(resolved, 2, "alpha", label="back") is EnrollOutcome.ENROLLED
        (record,) = list_clients(resolved)
        assert (record.client_id, record.label) == ("alpha", "back")

    def test_replace_deletes_the_other_pins_of_the_client(self, resolved):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 5)
        mktoken(resolved, client_id="alpha", replace=True)
        generation = tgen(resolved)
        assert enroll(resolved, 3, "alpha") is EnrollOutcome.ENROLLED
        assert sorted(pin_map(resolved)) == sorted([fp(3), fp(5)])    # beta untouched
        assert tgen(resolved) == generation + 1                       # one bump for the whole swap

    def test_without_replace_the_other_pins_stay(self, resolved):
        seed(resolved, "alpha", 1, 2)
        mktoken(resolved, client_id="alpha", replace=False)
        enroll(resolved, 3, "alpha")
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2), fp(3)])

    def test_replace_leaves_no_dangling_predecessor(self, resolved):
        seed(resolved, "alpha", 1)
        assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED
        mktoken(resolved, client_id="alpha", replace=True)
        assert enroll(resolved, 3, "alpha") is EnrollOutcome.ENROLLED
        assert list(pin_map(resolved)) == [fp(3)]
        assert pin_map(resolved)[fp(3)].predecessor is None

    def test_replace_with_an_already_pinned_certificate_drops_the_other_pins(self, resolved):
        """A client re-presenting its CURRENT certificate with a scoped `replace` token drops the
        lost sibling key: ENROLLED, one bump, the token consumed, the dropped pin recorded."""
        seed(resolved, "alpha", 1, 2)
        mktoken(resolved, client_id="alpha", replace=True)
        generation = tgen(resolved)
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.ENROLLED
        assert list(pin_map(resolved)) == [fp(1)]
        assert tgen(resolved) == generation + 1
        assert token_row_consumed(resolved) is True
        assert {r.fingerprint: r.reason for r in list_revoked(resolved)} == {fp(2): "replaced"}

    # --- several fingerprints per id ----------------------------------------------------------

    def test_one_id_may_hold_several_fingerprints_but_a_fingerprint_has_one_id(self, resolved):
        seed(resolved, "alpha", 1, 2, 3)
        (record,) = list_clients(resolved)
        assert [p.fingerprint for p in record.pins] == sorted(
            [fp(1), fp(2), fp(3)], key=lambda f: (pin_map(resolved)[f].enrolled_at, f))
        assert {p.client_id for p in read_trust(resolved).pins} == {"alpha"}
        mktoken(resolved)
        assert enroll(resolved, 2, "beta") is EnrollOutcome.CERT_BOUND_ELSEWHERE

    def test_only_a_successful_enrollment_bumps_the_trust_generation(self, resolved):
        mktoken(resolved)
        gens = [tgen(resolved)]
        enroll(resolved, 1, "alpha", secret=OTHER_SECRET)                  # refused
        gens.append(tgen(resolved))
        enroll(resolved, 1, "alpha")                                       # enrolled
        gens.append(tgen(resolved))
        mktoken(resolved)
        enroll(resolved, 1, "alpha")                                       # already trusted
        enroll(resolved, 1, "beta")                                        # bound elsewhere
        enroll(resolved, 2, "alpha")                                       # id taken
        gens.append(tgen(resolved))
        assert gens == [0, 0, 1, 1]


# ===========================================================================
# Revocation
# ===========================================================================


@pytest.mark.unit
class TestRevocation:
    def test_revoking_one_pin_returns_it_and_keeps_the_client_and_other_pins(self, resolved):
        seed(resolved, "alpha", 1, 2)
        generation = tgen(resolved)
        removed = revoke_pin(resolved, fp(1))
        assert isinstance(removed, TrustedPin) and removed.fingerprint == fp(1)
        assert removed.client_id == "alpha"
        assert list(pin_map(resolved)) == [fp(2)]
        (record,) = list_clients(resolved)
        assert not record.revoked
        assert tgen(resolved) == generation + 1

    def test_revoke_pin_accepts_any_accepted_fingerprint_spelling(self, resolved):
        seed(resolved, "alpha", 1)
        coloned = ":".join(fp(1)[i:i + 2].upper() for i in range(0, 64, 2))
        assert revoke_pin(resolved, coloned).fingerprint == fp(1)

    def test_revoking_an_unknown_pin_is_a_lookup_error_and_does_not_bump(self, resolved):
        seed(resolved, "alpha", 1)
        generation = tgen(resolved)
        with pytest.raises(LookupError):
            revoke_pin(resolved, fp(9))
        with pytest.raises(ValueError):
            revoke_pin(resolved, "abc")
        assert tgen(resolved) == generation

    def test_revoking_the_last_pin_does_not_make_the_client_a_tombstone(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved)
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.CLIENT_ID_TAKEN   # still known

    def test_revoking_a_client_removes_every_pin_and_sets_the_tombstone(self, resolved):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 3)
        generation = tgen(resolved)
        record = revoke_client(resolved, "alpha", now=NOW + 99)
        assert isinstance(record, ClientRecord)
        assert record.revoked and record.revoked_at == NOW + 99
        assert sorted(p.fingerprint for p in record.pins) == sorted([fp(1), fp(2)])
        assert list(pin_map(resolved)) == [fp(3)]
        by_id = {c.client_id: c for c in list_clients(resolved)}
        assert by_id["alpha"].revoked and by_id["alpha"].pins == ()
        assert by_id["alpha"].revoked_at == NOW + 99
        assert not by_id["beta"].revoked
        assert tgen(resolved) == generation + 1

    def test_a_second_revoke_keeps_the_original_stamp_and_does_not_bump(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_client(resolved, "alpha", now=NOW + 99)
        generation = tgen(resolved)
        record = revoke_client(resolved, "alpha", now=NOW + 500)
        assert record.revoked_at == NOW + 99
        assert record.pins == ()
        assert list_clients(resolved)[0].revoked_at == NOW + 99
        assert tgen(resolved) == generation

    def test_revoking_a_client_without_pins_sets_the_tombstone_without_a_bump(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        generation = tgen(resolved)
        record = revoke_client(resolved, "alpha", now=NOW + 7)
        assert record.revoked and record.pins == ()
        assert tgen(resolved) == generation

    def test_revoking_an_unknown_client_is_a_lookup_error(self, resolved):
        with pytest.raises(LookupError):
            revoke_client(resolved, "ghost")
        with pytest.raises(ValueError):
            revoke_client(resolved, 5)
        with pytest.raises(ValueError):
            revoke_client(resolved, "alpha", now=float("nan"))

    def test_the_default_stamp_is_the_current_time(self, resolved):
        seed(resolved, "alpha", 1)
        before = time.time()
        record = revoke_client(resolved, "alpha")
        assert before <= record.revoked_at <= time.time()

    def test_a_revoked_clients_certificate_is_not_trusted_and_is_refused_for_ever(
        self, resolved, db_path
    ):
        """The client id can come back (scoped token) but only with a NEW certificate."""
        seed(resolved, "alpha", 1)
        revoke_client(resolved, "alpha")
        assert fp(1) not in pin_map(resolved)
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED       # the same cert
        assert fp(1) not in pin_map(resolved)
        assert token_row(db_path) == (None, None, None)                     # token untouched
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.ENROLLED       # a new cert works
        assert list(pin_map(resolved)) == [fp(2)]

    # --- dangling predecessors ----------------------------------------------------------------

    def test_revoking_a_pin_takes_its_rotation_successor_with_it(self, resolved):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        assert pin_map(resolved)[fp(2)].predecessor == fp(1)
        revoked = revoke_pin(resolved, fp(1))
        assert pin_map(resolved) == {}
        assert [p.fingerprint for p in revoked.cascaded] == [fp(2)]

    def test_a_replaced_old_fingerprint_is_not_retired_by_the_stale_reference(
        self, resolved, db_path
    ):
        """The reason `_unpin` sweeps dangling links: a pin's `predecessor` must not keep naming a
        fingerprint that is gone, in case that fingerprint comes back under ANOTHER id. Neither
        `revoke_pin` (it cascades) nor re-enrolment (refused) can recreate that state, so the
        stale link and the returning fingerprint are planted; the next pin deletion sweeps the
        link, and the successor's request then retires nothing."""
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 9)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(9), fp(2))
        assert pin_map(resolved)[fp(2)].predecessor == fp(9)        # a cross-client link
        assert retire_predecessor(resolved, fp(2)) is False          # never deletes another client's pin
        assert fp(9) in pin_map(resolved)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(7), fp(2))                                            # a link to a fingerprint nobody holds
        revoke_pin(resolved, fp(1))                                  # any deletion sweeps it
        assert pin_map(resolved)[fp(2)].predecessor is None
        seed(resolved, "gamma", 7)                                   # the fingerprint now appears
        assert retire_predecessor(resolved, fp(2)) is False
        assert pin_map(resolved)[fp(7)].client_id == "gamma"

    def test_revoking_a_client_clears_a_reference_into_it(self, resolved, db_path):
        """A (planted) cross-client reference to a pin that revoke_client deletes is cleared."""
        seed(resolved, "alpha", 1)
        seed(resolved, "beta", 2)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(1), fp(2))
        revoke_client(resolved, "alpha")
        assert pin_map(resolved)[fp(2)].predecessor is None

    def test_read_trust_reports_only_live_predecessors(self, resolved, db_path):
        """`TrustedPin.predecessor` is `None` unless the predecessor is still pinned: a stale
        field (planted below, naming a pin that stays pinned) is cleared by the next deletion of
        the pin it names -- here a `--replace` enrollment, which does not cascade."""
        seed(resolved, "alpha", 1, 2)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(1), fp(2))
        assert pin_map(resolved)[fp(2)].predecessor == fp(1)
        mktoken(resolved, client_id="alpha", replace=True)
        enroll(resolved, 2, "alpha")                  # drops fp(1) only
        assert list(pin_map(resolved)) == [fp(2)]
        assert pin_map(resolved)[fp(2)].predecessor is None

    def test_read_trust_never_returns_the_pins_of_a_revoked_client(self, resolved, db_path):
        """Backstop: plant a pin under a revoked client."""
        seed(resolved, "alpha", 1)
        sql(db_path, "UPDATE clients SET revoked_at = ? WHERE client_id = 'alpha'", NOW)
        assert read_trust(resolved).pins == ()
        (record,) = list_clients(resolved)
        assert record.revoked and [p.fingerprint for p in record.pins] == [fp(1)]


# ===========================================================================
# revoked_fingerprints: a revocation is permanent
# ===========================================================================


def revoked_rows(db_path):
    """{fingerprint: (client_id, revoked_at, reason)} straight from the table."""
    return {r[0]: tuple(r[1:]) for r in sql(
        db_path, "SELECT fingerprint, client_id, revoked_at, reason FROM revoked_fingerprints")}


def revoked_links(db_path):
    """{fingerprint: (reason, superseded_by)} straight from the table."""
    return {r[0]: tuple(r[1:]) for r in sql(
        db_path, "SELECT fingerprint, reason, superseded_by FROM revoked_fingerprints")}


def token_untouched(db_path) -> bool:
    return token_row(db_path) == (None, None, None)


@pytest.fixture
def revoked_hidden_until_locked(monkeypatch):
    """`trust._is_revoked` answers False everywhere EXCEPT inside `_write_tx`: the read-only
    pre-check (and the pre-check inside `enroll_client`) sees nothing, so only the re-check
    under the write lock can refuse a revoked fingerprint."""
    state = {"locked": False}
    real_is_revoked = trust._is_revoked
    real_tx = trust._write_tx

    @contextlib.contextmanager
    def tx(resolved):
        with real_tx(resolved) as session:
            state["locked"] = True
            try:
                yield session
            finally:
                state["locked"] = False

    def is_revoked(session, fingerprint):
        return state["locked"] and real_is_revoked(session, fingerprint)

    monkeypatch.setattr(trust, "_write_tx", tx)
    monkeypatch.setattr(trust, "_is_revoked", is_revoked)


@pytest.mark.unit
class TestEveryUnpinPathRecordsTheFingerprint:
    """`_unpin` is the only code that deletes from `trusted_clients`, and each caller passes its
    own reason. Exactly one row per unpinned fingerprint, with the client, the stamp and the
    reason; nothing else ever writes the table."""

    def test_revoke_pin(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        revoke_pin(resolved, fp(1), now=NOW + 50)
        assert revoked_rows(db_path) == {fp(1): ("alpha", NOW + 50, "revoked")}

    def test_revoke_client_records_every_pin_with_the_client_reason(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 3)
        revoke_client(resolved, "alpha", now=NOW + 60)
        assert revoked_rows(db_path) == {
            fp(1): ("alpha", NOW + 60, "client_revoked"),
            fp(2): ("alpha", NOW + 60, "client_revoked"),
        }

    def test_a_second_revoke_client_records_nothing_more(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        revoke_client(resolved, "alpha", now=NOW + 60)
        revoke_client(resolved, "alpha", now=NOW + 90)
        assert revoked_rows(db_path) == {fp(1): ("alpha", NOW + 60, "client_revoked")}

    def test_revoke_client_keeps_an_earlier_single_revocation_reason(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        revoke_pin(resolved, fp(1), now=NOW + 50)
        revoke_client(resolved, "alpha", now=NOW + 60)
        assert revoked_rows(db_path) == {
            fp(1): ("alpha", NOW + 50, "revoked"),
            fp(2): ("alpha", NOW + 60, "client_revoked"),
        }

    def test_a_replace_enrollment_records_the_dropped_pins(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 5)
        mktoken(resolved, client_id="alpha", replace=True)
        assert enroll(resolved, 3, "alpha", now=NOW + 70) is EnrollOutcome.ENROLLED
        assert revoked_rows(db_path) == {
            fp(1): ("alpha", NOW + 70, "replaced"),
            fp(2): ("alpha", NOW + 70, "replaced"),
        }                                              # fp(3) is live, beta's pin untouched

    def test_a_replace_enrollment_with_nothing_to_drop_records_nothing(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        sql(db_path, "DELETE FROM revoked_fingerprints")
        mktoken(resolved, client_id="alpha", replace=True)
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.ENROLLED
        assert revoked_rows(db_path) == {}

    def test_retire_predecessor(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        assert revoked_rows(db_path) == {}             # a pending rotation records nothing
        assert retire_predecessor(resolved, fp(2), now=NOW + 80) is True
        assert revoked_rows(db_path) == {fp(1): ("alpha", NOW + 80, "retired")}

    def test_retire_predecessor_stamps_the_current_time_by_default(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        before = time.time()
        retire_predecessor(resolved, fp(2))
        (row,) = revoked_rows(db_path).values()
        assert before <= row[1] <= time.time() and row[2] == "retired"

    def test_revoke_pin_stamps_the_current_time_by_default(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        before = time.time()
        revoke_pin(resolved, fp(1))
        (row,) = revoked_rows(db_path).values()
        assert before <= row[1] <= time.time() and row[2] == "revoked"

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), "now"])
    def test_a_bad_now_is_a_value_error_and_changes_nothing(self, resolved, db_path, bad):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        generation = tgen(resolved)
        with pytest.raises(ValueError):
            revoke_pin(resolved, fp(1), now=bad)
        with pytest.raises(ValueError):
            retire_predecessor(resolved, fp(2), now=bad)
        assert revoked_rows(db_path) == {} and set(pin_map(resolved)) == {fp(1), fp(2)}
        assert tgen(resolved) == generation

    def test_the_operations_that_unpin_nothing_record_nothing(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.ENROLLED          # a plain add
        assert rotate(resolved, 1, 3) is RotateOutcome.ROTATED
        assert rotate(resolved, 1, 3) is RotateOutcome.ALREADY
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.REFUSED           # token spent
        assert retire_predecessor(resolved, fp(2)) is False
        assert revoke_token(resolved, lock=None) is True
        with pytest.raises(LookupError):
            revoke_pin(resolved, fp(9))
        with pytest.raises(LookupError):
            revoke_client(resolved, "ghost")
        read_trust(resolved), list_clients(resolved), list_revoked(resolved)
        assert revoked_rows(db_path) == {}

    def test_revoking_a_client_with_no_pins_records_nothing(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1), now=NOW + 5)
        revoke_client(resolved, "alpha", now=NOW + 6)
        assert revoked_rows(db_path) == {fp(1): ("alpha", NOW + 5, "revoked")}

    def test_a_revoked_pin_and_its_record_never_coexist_after_any_path(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2, 3)
        rotate(resolved, 1, 4)
        revoke_pin(resolved, fp(2))
        retire_predecessor(resolved, fp(4))
        revoke_client(resolved, "alpha")
        live = {r[0] for r in sql(db_path, "SELECT fingerprint FROM trusted_clients")}
        assert live == set() and set(revoked_rows(db_path)) == {fp(1), fp(2), fp(3), fp(4)}

    def test_only_unpin_deletes_pins(self):
        """The invariant behind all of the above, checked on the source: `_unpin` holds the
        only `delete(_TC)` in trust.py."""
        source = Path(trust.__file__).read_text()
        assert source.count("delete(_TC)") == 1
        assert source.index("delete(_TC)") > source.index("def _unpin(")
        assert source.index("delete(_TC)") < source.index("def _clear_dangling_predecessors(")


@pytest.mark.unit
class TestRevokedFingerprintReads:
    def test_read_trust_excludes_a_pin_that_is_also_recorded(self, resolved, db_path):
        """The backstop: the sets are kept disjoint by `_unpin`, but a planted overlap (a crash
        can't produce one, a hand edit can) never reaches the TLS pin set."""
        seed(resolved, "alpha", 1, 2)
        sql(db_path, "INSERT INTO revoked_fingerprints (fingerprint, client_id, revoked_at, reason) VALUES (?, 'alpha', ?, 'revoked')",
            fp(1), NOW)
        assert list(pin_map(resolved)) == [fp(2)]

    def test_the_backstop_applies_to_every_reason(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2, 3, 4)
        for n, reason in ((1, "revoked"), (2, "client_revoked"), (3, "replaced"), (4, "retired")):
            sql(db_path, "INSERT INTO revoked_fingerprints (fingerprint, client_id, revoked_at, reason) VALUES (?, 'alpha', ?, ?)",
                fp(n), NOW, reason)
        assert read_trust(resolved).pins == ()

    def test_list_revoked_is_empty_on_a_fresh_store(self, resolved):
        assert list_revoked(resolved) == ()

    def test_list_revoked_returns_frozen_records_ordered_by_time_then_fingerprint(
        self, resolved
    ):
        seed(resolved, "alpha", 1, 2, 3)
        revoke_pin(resolved, fp(3), now=NOW + 5)
        revoke_pin(resolved, fp(1), now=NOW + 9)
        revoke_pin(resolved, fp(2), now=NOW + 9)
        listed = list_revoked(resolved)
        assert all(isinstance(r, RevokedPin) for r in listed)
        assert [r.fingerprint for r in listed] == [fp(3)] + sorted([fp(1), fp(2)])
        assert listed[0] == RevokedPin(fp(3), "alpha", NOW + 5, "revoked")
        with pytest.raises(dataclasses.FrozenInstanceError):
            listed[0].reason = "x"
        assert_cache_empty()

    def test_a_client_record_carries_its_revoked_pins(self, resolved):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 3)
        revoke_pin(resolved, fp(2), now=NOW + 5)
        by_id = {c.client_id: c for c in list_clients(resolved)}
        assert by_id["alpha"].revoked_pins == (RevokedPin(fp(2), "alpha", NOW + 5, "revoked"),)
        assert not by_id["alpha"].revoked                   # one revoked pin is not a revoked client
        assert [p.fingerprint for p in by_id["alpha"].pins] == [fp(1)]
        assert by_id["beta"].revoked_pins == ()

    def test_a_client_records_revoked_pins_are_ordered(self, resolved):
        seed(resolved, "alpha", 1, 2, 3)
        revoke_pin(resolved, fp(2), now=NOW + 9)
        revoke_pin(resolved, fp(3), now=NOW + 5)
        revoke_pin(resolved, fp(1), now=NOW + 9)
        (record,) = list_clients(resolved)
        assert [r.fingerprint for r in record.revoked_pins] == [fp(3)] + sorted([fp(1), fp(2)])

    def test_revoke_client_returns_the_revoked_pins_too(self, resolved):
        seed(resolved, "alpha", 1, 2)
        revoke_pin(resolved, fp(1), now=NOW + 5)
        record = revoke_client(resolved, "alpha", now=NOW + 8)
        assert [p.fingerprint for p in record.pins] == [fp(2)]          # what THIS call removed
        assert [(r.fingerprint, r.reason) for r in record.revoked_pins] == [
            (fp(1), "revoked"), (fp(2), "client_revoked")]

    def test_revoke_client_returns_only_its_own_clients_revocations(self, resolved):
        seed(resolved, "alpha", 1)
        seed(resolved, "beta", 2)
        revoke_pin(resolved, fp(2), now=NOW + 5)
        record = revoke_client(resolved, "alpha", now=NOW + 8)
        assert [r.fingerprint for r in record.revoked_pins] == [fp(1)]

    def test_a_revoked_client_keeps_listing_its_revoked_pins(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_client(resolved, "alpha", now=NOW + 8)
        (record,) = list_clients(resolved)
        assert record.revoked and record.pins == ()
        assert [r.reason for r in record.revoked_pins] == ["client_revoked"]

    def test_revoking_an_already_revoked_pin_says_so(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1), now=NOW + 5)
        generation = tgen(resolved)
        with pytest.raises(LookupError, match="is already revoked") as excinfo:
            revoke_pin(resolved, fp(1), now=NOW + 99)
        assert fp(1) in str(excinfo.value)
        assert tgen(resolved) == generation
        assert [(r.revoked_at, r.reason) for r in list_revoked(resolved)] == [(NOW + 5, "revoked")]

    @pytest.mark.parametrize("how", ["client_revoked", "replaced", "retired"])
    def test_every_reason_reads_as_already_revoked(self, resolved, how):
        seed(resolved, "alpha", 1)
        if how == "client_revoked":
            revoke_client(resolved, "alpha")
        elif how == "replaced":
            mktoken(resolved, client_id="alpha", replace=True)
            enroll(resolved, 2, "alpha")
        else:
            rotate(resolved, 1, 2)
            retire_predecessor(resolved, fp(2))
            # A retired key with a live successor is not "already revoked": it follows
            # `superseded_by` to the successor (`TestRevokePinRetiredChain`).
            result = revoke_pin(resolved, fp(1))
            assert isinstance(result, SupersededRevocation) and result.already_retired
            assert [p.fingerprint for p in result.cascaded] == [fp(2)]
            with pytest.raises(LookupError, match="is already revoked"):
                revoke_pin(resolved, fp(1))
            return
        with pytest.raises(LookupError, match="is already revoked"):
            revoke_pin(resolved, fp(1))

    def test_an_unknown_fingerprint_is_not_reported_as_revoked(self, resolved):
        with pytest.raises(LookupError, match="no pinned certificate") as excinfo:
            revoke_pin(resolved, fp(9))
        assert "already revoked" not in str(excinfo.value)

    def test_list_revoked_refuses_an_old_schema(self, resolved, db_path):
        sql(db_path, "UPDATE alembic_version SET version_num = ?", BASELINE)
        with pytest.raises(SchemaOutOfDate):
            list_revoked(resolved)
        assert_cache_empty()


@pytest.mark.unit
class TestARevokedFingerprintIsNeverEnrolledAgain:
    """Revoke F, then present F again: REFUSED, whatever the token, the scope or the id, and the
    token is left untouched (neither consumed nor latched)."""

    @pytest.mark.parametrize("how", ["pin", "client", "replaced", "retired"])
    @pytest.mark.parametrize("scope, target", [
        (None, "alpha"), ("alpha", "alpha"), (None, "gamma"), ("alpha", "gamma"),
        ("alpha", "beta"), (None, "beta"),
    ], ids=["unscoped_same_id", "scoped_same_id", "unscoped_new_id", "scoped_other_id_gamma",
            "scoped_other_id_beta", "unscoped_other_known_id"])
    def test_refused_for_every_route_and_token(self, resolved, db_path, how, scope, target):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 7)
        if how == "pin":
            revoke_pin(resolved, fp(1))
        elif how == "client":
            revoke_client(resolved, "alpha")
        elif how == "replaced":
            mktoken(resolved, client_id="alpha", replace=True)
            assert enroll(resolved, 3, "alpha") is EnrollOutcome.ENROLLED
        else:
            rotate(resolved, 2, 4)
            retire_predecessor(resolved, fp(4))
        gone = fp(1) if how != "retired" else fp(2)
        assert gone in revoked_rows(db_path)
        n = 1 if how != "retired" else 2
        mktoken(resolved, client_id=scope)
        before = (table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints"), tgen(resolved))
        assert enroll(resolved, n, target) is EnrollOutcome.REFUSED
        assert (table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints"), tgen(resolved)) == before
        assert token_untouched(db_path)
        assert gone not in pin_map(resolved)

    def test_the_untouched_token_still_enrolls_a_different_certificate(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED      # and again
        assert enroll(resolved, 5, "alpha") is EnrollOutcome.ENROLLED
        assert list(pin_map(resolved)) == [fp(5)]

    def test_an_expired_token_is_not_latched_by_a_revoked_fingerprint(self, resolved, db_path):
        """The refusal comes before the token is looked at, so a stolen key cannot be used to
        burn an expired-but-valid token either."""
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha", created=NOW, ttl=100)
        assert enroll(resolved, 1, "alpha", now=NOW + 500) is EnrollOutcome.REFUSED
        assert token_untouched(db_path)

    def test_the_recorded_fingerprint_is_matched_in_any_accepted_spelling(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved)
        coloned = ":".join(fp(1)[i:i + 2].upper() for i in range(0, 64, 2))
        outcome = enroll_client(
            resolved, secret_sha256=SECRET, server_fingerprint=SERVER, client_id="gamma",
            pin=NewPin(coloned, "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n", NOW),
            now=NOW + 10, dataset_exists=False)
        assert outcome is EnrollOutcome.REFUSED

    def test_a_key_that_was_never_pinned_is_not_affected(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved)
        assert enroll(resolved, 2, "gamma") is EnrollOutcome.ENROLLED

    def test_the_check_is_repeated_under_the_write_lock(
        self, resolved, db_path, revoked_hidden_until_locked
    ):
        seed(resolved, "alpha", 1)
        sql(db_path, "DELETE FROM trusted_clients")
        sql(db_path, "INSERT INTO revoked_fingerprints (fingerprint, client_id, revoked_at, reason) VALUES (?, 'alpha', ?, 'revoked')",
            fp(1), NOW)
        mktoken(resolved, client_id="alpha")
        assert trust.token_precheck(
            resolved, secret_sha256=SECRET, server_fingerprint=SERVER, now=NOW + 10,
            fingerprint=fp(1)) is True              # the (blinded) filter lets it through ...
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED    # ... the lock refuses it
        assert token_untouched(db_path) and pin_map(resolved) == {}

    def test_a_revocation_racing_the_filter_is_caught_for_an_expired_token_too(
        self, resolved, db_path, revoked_hidden_until_locked
    ):
        seed(resolved, "alpha", 1)
        sql(db_path, "DELETE FROM trusted_clients")
        sql(db_path, "INSERT INTO revoked_fingerprints (fingerprint, client_id, revoked_at, reason) VALUES (?, 'alpha', ?, 'revoked')",
            fp(1), NOW)
        mktoken(resolved, client_id="alpha", created=NOW, ttl=100)
        assert enroll(resolved, 1, "alpha", now=NOW + 500) is EnrollOutcome.REFUSED
        assert token_untouched(db_path)             # check 0 precedes the expiry latch


@pytest.mark.unit
class TestRevokedRefusalTakesNoWriteLock:
    def test_enroll_decides_on_a_snapshot(self, resolved, db_path, write_tx_calls):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha")
        write_tx_calls.clear()
        with write_lock_held(db_path):
            started = time.monotonic()
            assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
            assert time.monotonic() - started < 1.0
        assert write_tx_calls == []

    def test_enroll_does_not_wait_for_the_in_process_lock(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha")
        assert trust._API_WRITE_LOCK.acquire(timeout=1)
        try:
            started = time.monotonic()
            assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
            assert time.monotonic() - started < 1.0
        finally:
            trust._API_WRITE_LOCK.release()

    def test_rotate_decides_on_a_snapshot(self, resolved, db_path, write_tx_calls):
        seed(resolved, "alpha", 1, 2)
        revoke_pin(resolved, fp(2))
        write_tx_calls.clear()
        with write_lock_held(db_path):
            started = time.monotonic()
            assert rotate(resolved, 1, 2) is RotateOutcome.REFUSED
            assert time.monotonic() - started < 1.0
        assert write_tx_calls == []

    def test_control_an_unrevoked_fingerprint_does_reach_the_write_transaction(
        self, resolved, write_tx_calls
    ):
        seed(resolved, "alpha", 1)
        write_tx_calls.clear()
        assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED
        assert write_tx_calls == [1]


@pytest.mark.unit
class TestTokenPrecheckWithAFingerprint:
    def pre(self, resolved, *, fingerprint=None, secret=SECRET, now=NOW + 10):
        return token_precheck(resolved, secret_sha256=secret, server_fingerprint=SERVER,
                              now=now, fingerprint=fingerprint)

    def test_a_recorded_fingerprint_is_false_and_others_are_true(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha")
        assert self.pre(resolved) is True                             # no fingerprint given
        assert self.pre(resolved, fingerprint=fp(1)) is False
        assert self.pre(resolved, fingerprint=fp(1).upper()) is False  # any spelling
        assert self.pre(resolved, fingerprint=fp(2)) is True
        assert_cache_empty()

    @pytest.mark.parametrize("how", ["pin", "client", "replaced", "retired"])
    def test_every_reason_is_false(self, resolved, how):
        seed(resolved, "alpha", 1)
        if how == "pin":
            revoke_pin(resolved, fp(1))
        elif how == "client":
            revoke_client(resolved, "alpha")
        elif how == "replaced":
            mktoken(resolved, client_id="alpha", replace=True)
            enroll(resolved, 2, "alpha")
        else:
            rotate(resolved, 1, 2)
            retire_predecessor(resolved, fp(2))
        mktoken(resolved, client_id="alpha")
        assert self.pre(resolved, fingerprint=fp(1)) is False       # fp(1) is the one unpinned

    def test_a_pinned_but_unrecorded_fingerprint_is_not_refused_by_the_filter(self, resolved):
        seed(resolved, "alpha", 1)
        mktoken(resolved, client_id="alpha")
        assert self.pre(resolved, fingerprint=fp(1)) is True

    def test_a_malformed_fingerprint_is_a_value_error(self, resolved):
        mktoken(resolved)
        for bad in ("nope", "", "ab" * 31, 5):
            with pytest.raises(ValueError):
                self.pre(resolved, fingerprint=bad)

    def test_it_stays_false_without_a_matching_token(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        assert self.pre(resolved, fingerprint=fp(2)) is False        # no token at all
        mktoken(resolved)
        assert self.pre(resolved, fingerprint=fp(2), secret=OTHER_SECRET) is False

    @pytest.mark.parametrize("fingerprint", [None, "revoked", "other"])
    def test_the_constant_time_comparison_always_runs(self, resolved, monkeypatch, fingerprint):
        """Both credentials are compared (twice, no early exit) BEFORE the revoked lookup can
        short-circuit: a revoked fingerprint must not answer faster than a clean one."""
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha")
        calls = []
        real = trust.hmac.compare_digest
        monkeypatch.setattr(trust.hmac, "compare_digest",
                            lambda a, b: calls.append((a, b)) or real(a, b))
        chosen = {None: None, "revoked": fp(1), "other": fp(2)}[fingerprint]
        self.pre(resolved, fingerprint=chosen)
        assert len(calls) == 2

    def test_a_wrong_secret_with_a_revoked_fingerprint_still_compares_both(
        self, resolved, monkeypatch
    ):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha")
        calls = []
        real = trust.hmac.compare_digest
        monkeypatch.setattr(trust.hmac, "compare_digest",
                            lambda a, b: calls.append((a, b)) or real(a, b))
        assert self.pre(resolved, fingerprint=fp(1), secret=OTHER_SECRET) is False
        assert len(calls) == 2

    def test_it_is_read_only(self, resolved, db_path, write_tx_calls):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha", created=NOW, ttl=100)
        before = table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints")
        write_tx_calls.clear()
        with write_lock_held(db_path):
            assert self.pre(resolved, fingerprint=fp(1), now=NOW + 500) is False
            assert self.pre(resolved, fingerprint=fp(2), now=NOW + 500) is True
        assert write_tx_calls == []
        assert table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints") == before

    def test_enroll_client_passes_the_certificate_fingerprint_to_it(self, resolved, monkeypatch):
        mktoken(resolved)
        seen = []
        real = trust.token_precheck
        monkeypatch.setattr(trust, "token_precheck",
                            lambda *a, **kw: seen.append(kw.get("fingerprint")) or real(*a, **kw))
        assert enroll(resolved, 4, "alpha") is EnrollOutcome.ENROLLED
        assert seen == [fp(4)]


@pytest.mark.unit
class TestRotationAndRevokedFingerprints:
    def test_a_retired_predecessor_is_refused_for_enroll_and_rotate(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        assert retire_predecessor(resolved, fp(2)) is True
        assert list(pin_map(resolved)) == [fp(2)]
        generation = tgen(resolved)
        assert rotate(resolved, 2, 1) is RotateOutcome.REFUSED       # a rotation is no back door
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
        assert token_untouched(db_path)
        assert tgen(resolved) == generation and list(pin_map(resolved)) == [fp(2)]

    @pytest.mark.parametrize("how", ["pin", "client", "replaced", "retired"])
    def test_a_revoked_successor_is_refused_whatever_revoked_it(self, resolved, how):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 6)
        if how == "pin":
            revoke_pin(resolved, fp(2))
        elif how == "client":
            revoke_client(resolved, "beta")
        elif how == "replaced":
            mktoken(resolved, client_id="alpha", replace=True)
            enroll(resolved, 3, "alpha")
        else:
            rotate(resolved, 1, 8)
            retire_predecessor(resolved, fp(8))
        gone = {"pin": 2, "client": 6, "replaced": 2, "retired": 1}[how]
        generation = tgen(resolved)
        live = {"pin": 1, "client": 1, "replaced": 3, "retired": 8}[how]
        assert rotate(resolved, live, gone) is RotateOutcome.REFUSED
        assert tgen(resolved) == generation and fp(gone) not in pin_map(resolved)

    def test_a_pending_successor_the_operator_revoked_cannot_be_rotated_in_again(self, resolved):
        seed(resolved, "alpha", 1)
        assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED
        revoke_pin(resolved, fp(2))
        assert rotate(resolved, 1, 2) is RotateOutcome.REFUSED       # same key: a new one needed
        assert rotate(resolved, 1, 3) is RotateOutcome.ROTATED

    def test_a_revoked_old_pin_cannot_rotate(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        assert rotate(resolved, 1, 2) is RotateOutcome.REFUSED

    def test_the_new_pin_check_is_repeated_under_the_write_lock(
        self, resolved, db_path, revoked_hidden_until_locked
    ):
        seed(resolved, "alpha", 1)
        sql(db_path, "INSERT INTO revoked_fingerprints (fingerprint, client_id, revoked_at, reason) VALUES (?, 'alpha', ?, 'revoked')",
            fp(2), NOW)
        generation = tgen(resolved)
        assert rotate(resolved, 1, 2) is RotateOutcome.REFUSED
        assert list(pin_map(resolved)) == [fp(1)] and tgen(resolved) == generation

    def test_retiring_clears_the_successors_predecessor_reference(
        self, resolved
    ):
        """`retire_predecessor` clears the field before the sweep, so the successor does not
        keep pointing at the recorded key."""
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2))
        assert pin_map(resolved)[fp(2)].predecessor is None


@pytest.mark.unit
class TestReplaceWithAnAlreadyPinnedCertificate:
    """A scoped `replace` token presented with a certificate already pinned to that client."""

    def test_enrolled_drops_the_others_consumes_the_token_and_bumps_once(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2, 3)
        seed(resolved, "beta", 9)
        mktoken(resolved, client_id="alpha", replace=True)
        generation = tgen(resolved)
        assert enroll(resolved, 2, "alpha", now=NOW + 40) is EnrollOutcome.ENROLLED
        assert sorted(pin_map(resolved)) == sorted([fp(2), fp(9)])
        assert tgen(resolved) == generation + 1
        assert token_row(db_path) == (NOW + 40, fp(2), "alpha")
        assert revoked_rows(db_path) == {
            fp(1): ("alpha", NOW + 40, "replaced"), fp(3): ("alpha", NOW + 40, "replaced")}
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.REFUSED      # spent

    def test_the_pinned_row_itself_is_untouched(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        before = sql(db_path, "SELECT * FROM trusted_clients WHERE fingerprint = ?", fp(1))
        mktoken(resolved, client_id="alpha", replace=True)
        enroll(resolved, 1, "alpha")
        assert sql(db_path, "SELECT * FROM trusted_clients WHERE fingerprint = ?", fp(1)) == before

    def test_it_bumps_even_when_there_is_nothing_to_drop(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        mktoken(resolved, client_id="alpha", replace=True)
        generation = tgen(resolved)
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.ENROLLED
        assert tgen(resolved) == generation + 1
        assert list(pin_map(resolved)) == [fp(1)] and revoked_rows(db_path) == {}
        assert token_row(db_path)[0] is not None

    def test_the_label_is_set_when_given_and_kept_otherwise(self, resolved):
        seed(resolved, "alpha", 1, 2, label="first")
        mktoken(resolved, client_id="alpha", replace=True)
        enroll(resolved, 1, "alpha")
        assert list_clients(resolved)[0].label == "first"
        mktoken(resolved, client_id="alpha", replace=True)
        enroll(resolved, 1, "alpha", label="second")
        assert list_clients(resolved)[0].label == "second"

    def test_an_unscoped_token_is_still_already_trusted_and_not_consumed(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        mktoken(resolved)
        generation = tgen(resolved)
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.ALREADY_TRUSTED
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2)])
        assert token_untouched(db_path) and revoked_rows(db_path) == {}
        assert tgen(resolved) == generation

    def test_a_scoped_token_without_replace_is_still_already_trusted(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        mktoken(resolved, client_id="alpha", replace=False)
        assert enroll(resolved, 1, "alpha", label="ignored") is EnrollOutcome.ALREADY_TRUSTED
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2)])
        assert token_untouched(db_path) and revoked_rows(db_path) == {}
        assert list_clients(resolved)[0].label is None

    def test_a_replace_token_scoped_to_another_client_is_already_trusted(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 5)
        mktoken(resolved, client_id="beta", replace=True)
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.ALREADY_TRUSTED
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2), fp(5)])
        assert token_untouched(db_path)

    def test_a_certificate_pinned_elsewhere_is_still_bound_elsewhere(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 5, 6)
        mktoken(resolved, client_id="alpha", replace=True)
        assert enroll(resolved, 5, "alpha") is EnrollOutcome.CERT_BOUND_ELSEWHERE
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2), fp(5), fp(6)])
        assert token_untouched(db_path) and revoked_rows(db_path) == {}

    def test_presenting_a_pending_successor_drops_the_predecessor(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        mktoken(resolved, client_id="alpha", replace=True)
        generation = tgen(resolved)
        assert enroll(resolved, 2, "alpha", now=NOW + 40) is EnrollOutcome.ENROLLED
        assert list(pin_map(resolved)) == [fp(2)]
        assert pin_map(resolved)[fp(2)].predecessor is None        # no dangling reference
        assert revoked_rows(db_path) == {fp(1): ("alpha", NOW + 40, "replaced")}
        assert tgen(resolved) == generation + 1
        assert retire_predecessor(resolved, fp(2)) is False        # nothing left to retire

    def test_presenting_the_predecessor_drops_the_pending_successor(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        mktoken(resolved, client_id="alpha", replace=True)
        assert enroll(resolved, 1, "alpha", now=NOW + 40) is EnrollOutcome.ENROLLED
        assert list(pin_map(resolved)) == [fp(1)]
        assert revoked_rows(db_path) == {fp(2): ("alpha", NOW + 40, "replaced")}
        # the dropped successor can never come back, neither by enroll nor by rotation
        assert rotate(resolved, 1, 2) is RotateOutcome.REFUSED
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.REFUSED

    def test_a_pending_rotation_of_the_presented_certificate_survives_only_as_a_record(
        self, resolved
    ):
        seed(resolved, "alpha", 1, 2)
        rotate(resolved, 2, 3)                      # fp(3) pending for fp(2)
        mktoken(resolved, client_id="alpha", replace=True)
        enroll(resolved, 1, "alpha")
        assert list(pin_map(resolved)) == [fp(1)]
        assert {r.fingerprint for r in list_revoked(resolved)} == {fp(2), fp(3)}

    def test_a_revoked_client_cannot_replace_its_way_back_with_an_old_pin(self, resolved):
        seed(resolved, "alpha", 1, 2)
        revoke_client(resolved, "alpha")
        mktoken(resolved, client_id="alpha", replace=True)
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
        assert enroll(resolved, 5, "alpha") is EnrollOutcome.ENROLLED


@pytest.mark.unit
class TestRevocationSurvivesImportAndWrites:
    def test_import_keeps_the_revocations_and_they_still_refuse(self, resolved, yaml_path, db_path):
        seed(resolved, "alpha", 1, 2)
        revoke_pin(resolved, fp(1), now=NOW + 5)
        revoke_client(resolved, "alpha", now=NOW + 6)
        before = sql(db_path, "SELECT * FROM revoked_fingerprints ORDER BY 1")
        assert len(before) == 2
        import_yaml(yaml_path, resolved)
        dispose_all()
        assert sql(db_path, "SELECT * FROM revoked_fingerprints ORDER BY 1") == before
        assert [r.fingerprint for r in list_revoked(resolved)] == [fp(1), fp(2)]
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
        assert token_untouched(db_path)

    def test_write_config_keeps_them(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        before = sql(db_path, "SELECT * FROM revoked_fingerprints")
        snap = read_config(resolved)
        dispose_all()
        snap.config.snapshot_prefix = "changed"
        write_config(resolved, snap.config, expected_generation=snap.generation)
        dispose_all()
        assert sql(db_path, "SELECT * FROM revoked_fingerprints") == before

    def test_a_revocation_does_not_move_the_config_generation(self, resolved):
        seed(resolved, "alpha", 1)
        before = cgen(resolved)
        revoke_pin(resolved, fp(1))
        assert cgen(resolved) == before

    def test_a_revoke_that_cannot_take_the_lock_records_and_unpins_nothing(self, resolved, db_path):
        """Atomic: no lock, no unpin and no record."""
        seed(resolved, "alpha", 1)
        with write_lock_held(db_path):
            with pytest.raises(ConfigBusy):
                trust.revoke_pin(resolved, fp(1), lock=None)
        assert revoked_rows(db_path) == {} and list(pin_map(resolved)) == [fp(1)]


@pytest.mark.unit
class TestRevokePinCascade:
    """`revoke_pin(A)` also unpins every rotation successor of A, transitively, in the same
    transaction (reason `successor_revoked`): whoever rotated A -> C proved possession of A's
    key, so revoking A as stolen must not leave the key its holder authorised."""

    @staticmethod
    def chain(resolved, links=((1, 3), (3, 4))):
        """alpha holds fp(1) -> fp(3) -> fp(4), all three LIVE: a chain of pending pins.

        `rotate_client` can build only the first link: rotating 3 -> 4 now retires fp(1) (3's
        pinned predecessor) in the same transaction. So the first link is a real rotation and
        each later one is planted exactly as `rotate_client` would have written it
        (`enrolled_via='rotate'`, `predecessor` = the pin before), one second apart. The
        rotation-built chain, and the retirements, are `TestRevokePinRetiredChain` and
        `TestRotateRetiresTheGrandparent`."""
        seed(resolved, "alpha", 1)
        for step, (old, new) in enumerate(links):
            if step == 0:
                assert rotate(resolved, old, new, now=NOW + 20) is RotateOutcome.ROTATED
            else:
                sql(resolved.resolved_path,
                    "INSERT INTO trusted_clients VALUES (?, 'alpha', ?, ?, ?, 'rotate', ?)",
                    fp(new), newpin(new).cert_pem, newpin(new).not_after, NOW + 20 + step, fp(old))

    def test_a_transitive_chain_goes_with_its_root(self, resolved, db_path):
        self.chain(resolved)
        assert pin_map(resolved)[fp(4)].predecessor == fp(3)
        revoke_pin(resolved, fp(1), now=NOW + 99)
        assert pin_map(resolved) == {}
        assert revoked_rows(db_path) == {
            fp(1): ("alpha", NOW + 99, "revoked"),
            fp(3): ("alpha", NOW + 99, "successor_revoked"),
            fp(4): ("alpha", NOW + 99, "successor_revoked"),
        }

    def test_the_result_is_a_pin_revocation_with_the_cascaded_pins(self, resolved):
        self.chain(resolved)
        result = revoke_pin(resolved, fp(1))
        assert isinstance(result, PinRevocation) and isinstance(result, TrustedPin)
        assert (result.fingerprint, result.client_id, result.enrolled_via) == (fp(1), "alpha", "token")
        assert result.predecessor is None
        assert isinstance(result.cascaded, tuple)
        assert [p.fingerprint for p in result.cascaded] == [fp(3), fp(4)]
        assert all(type(p) is TrustedPin for p in result.cascaded)
        assert [(p.client_id, p.enrolled_via) for p in result.cascaded] == [
            ("alpha", "rotate"), ("alpha", "rotate")]
        assert result.cascaded[0].cert_pem == newpin(3).cert_pem
        with pytest.raises(dataclasses.FrozenInstanceError):
            result.cascaded = ()

    def test_cascaded_predecessors_are_the_links_as_they_stood_before_the_call(self, resolved):
        """Every group is read before anything is written (`_unpin_all`): unpinning the root
        first used to clear fp(3)'s link, so the same result showed the chain two ways."""
        self.chain(resolved)
        result = revoke_pin(resolved, fp(1))
        assert [(p.fingerprint, p.predecessor) for p in result.cascaded] == [
            (fp(3), fp(1)), (fp(4), fp(3))]
        assert result.already_retired is False

    def test_without_a_successor_nothing_cascades(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        result = revoke_pin(resolved, fp(1))
        assert isinstance(result, PinRevocation) and result.cascaded == ()
        assert list(pin_map(resolved)) == [fp(2)]
        assert set(revoked_rows(db_path)) == {fp(1)}

    def test_cascaded_is_ordered_by_enrollment_time_then_fingerprint(self, resolved, db_path):
        self.chain(resolved)
        # the grandchild enrolled BEFORE the child (planted): time, not chain depth, orders them
        sql(db_path, "UPDATE trusted_clients SET enrolled_at = ? WHERE fingerprint = ?", NOW + 1, fp(4))
        assert [p.fingerprint for p in revoke_pin(resolved, fp(1)).cascaded] == [fp(4), fp(3)]

    def test_equal_enrollment_times_fall_back_to_the_fingerprint(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        low, high = sorted([fp(5), fp(6)])
        for fingerprint in (high, low):
            sql(db_path, "INSERT INTO trusted_clients VALUES (?, 'alpha', 'PEM', 9e9, ?, 'rotate', ?)",
                fingerprint, NOW + 5, fp(1))
        assert [p.fingerprint for p in revoke_pin(resolved, fp(1)).cascaded] == [low, high]

    def test_one_generation_bump_for_the_whole_cascade(self, resolved):
        self.chain(resolved)
        generation = tgen(resolved)
        revoke_pin(resolved, fp(1))
        assert tgen(resolved) == generation + 1

    def test_the_cascade_is_all_or_nothing(self, resolved, db_path, monkeypatch):
        self.chain(resolved)
        before = table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints")

        def boom(session):
            raise RuntimeError("injected")

        monkeypatch.setattr(trust, "_bump_trust_generation", boom)
        with pytest.raises(RuntimeError, match="injected"):
            revoke_pin(resolved, fp(1))
        assert table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints") == before
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(3), fp(4)])

    def test_every_cascaded_fingerprint_is_refused_afterwards(self, resolved, db_path):
        self.chain(resolved)
        revoke_pin(resolved, fp(1))
        mktoken(resolved, client_id="alpha")
        for n in (1, 3, 4):
            assert enroll(resolved, n, "alpha") is EnrollOutcome.REFUSED
        assert token_untouched(db_path)
        for n in (3, 4):
            with pytest.raises(LookupError, match="is already revoked"):
                revoke_pin(resolved, fp(n))

    def test_the_cascade_stays_inside_one_clients_chain(self, resolved):
        self.chain(resolved)
        seed(resolved, "beta", 7)
        rotate(resolved, 7, 8)
        revoke_pin(resolved, fp(1))
        assert sorted(pin_map(resolved)) == sorted([fp(7), fp(8)])

    def test_siblings_of_the_root_are_not_touched(self, resolved):
        seed(resolved, "alpha", 1, 2)
        rotate(resolved, 1, 3)
        revoke_pin(resolved, fp(1))
        assert list(pin_map(resolved)) == [fp(2)]

    def test_revoking_the_middle_of_a_chain_keeps_the_root_and_takes_the_tail(
        self, resolved, db_path
    ):
        self.chain(resolved)
        result = revoke_pin(resolved, fp(3), now=NOW + 99)
        assert [p.fingerprint for p in result.cascaded] == [fp(4)]
        assert list(pin_map(resolved)) == [fp(1)]
        assert {k: v[2] for k, v in revoked_rows(db_path).items()} == {
            fp(3): "revoked", fp(4): "successor_revoked"}

    def test_revoking_a_successor_leaves_its_predecessor_alone(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        result = revoke_pin(resolved, fp(2))
        assert result.cascaded == ()
        assert list(pin_map(resolved)) == [fp(1)]
        assert {k: v[2] for k, v in revoked_rows(db_path).items()} == {fp(2): "revoked"}
        assert rotate(resolved, 1, 3) is RotateOutcome.ROTATED        # the way is free again

    def test_a_link_cycle_terminates(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(2), fp(1))
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(1), fp(2))
        result = guarded_call(lambda: revoke_pin(resolved, fp(1)))
        assert [p.fingerprint for p in result.cascaded] == [fp(2)]
        assert pin_map(resolved) == {}

    def test_a_successor_listed_with_the_client_shows_its_reason(self, resolved):
        self.chain(resolved)
        revoke_pin(resolved, fp(1), now=NOW + 99)
        (record,) = list_clients(resolved)
        assert [(r.fingerprint, r.reason) for r in record.revoked_pins] == sorted(
            [(fp(1), "revoked"), (fp(3), "successor_revoked"), (fp(4), "successor_revoked")])
        assert [r.reason for r in list_revoked(resolved)] == [
            r.reason for r in sorted(list_revoked(resolved), key=lambda r: (r.revoked_at, r.fingerprint))]

    # --- the cascade runs in one direction, and only for revoke_pin ---------------------------

    def test_retiring_a_predecessor_does_not_cascade(self, resolved, db_path):
        self.chain(resolved)
        assert retire_predecessor(resolved, fp(3), now=NOW + 80) is True
        assert sorted(pin_map(resolved)) == sorted([fp(3), fp(4)])    # the pending tail stays
        assert pin_map(resolved)[fp(4)].predecessor == fp(3)
        assert revoked_rows(db_path) == {fp(1): ("alpha", NOW + 80, "retired")}

    def test_replace_does_not_cascade_and_records_replaced(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        rotate(resolved, 2, 3)
        mktoken(resolved, client_id="alpha", replace=True)
        assert enroll(resolved, 1, "alpha", now=NOW + 70) is EnrollOutcome.ENROLLED
        assert revoked_rows(db_path) == {
            fp(2): ("alpha", NOW + 70, "replaced"), fp(3): ("alpha", NOW + 70, "replaced")}

    def test_replace_keeping_a_successor_does_not_touch_the_rest(self, resolved, db_path):
        seed(resolved, "alpha", 1, 2)
        rotate(resolved, 2, 3)
        mktoken(resolved, client_id="alpha", replace=True)
        assert enroll(resolved, 3, "alpha", now=NOW + 70) is EnrollOutcome.ENROLLED
        assert list(pin_map(resolved)) == [fp(3)]
        assert {v[2] for v in revoked_rows(db_path).values()} == {"replaced"}

    def test_revoking_a_client_records_client_revoked_for_a_chain(self, resolved, db_path):
        self.chain(resolved)
        revoke_client(resolved, "alpha", now=NOW + 60)
        assert {v[2] for v in revoked_rows(db_path).values()} == {"client_revoked"}
        assert set(revoked_rows(db_path)) == {fp(1), fp(3), fp(4)}

    # --- the thief scenario --------------------------------------------------------------------

    def test_a_thief_who_rotated_before_the_revoke_is_locked_out_at_every_door(
        self, resolved, db_path
    ):
        """Key A is stolen; the thief rotates A -> C with a key of their own before the operator
        notices. `trust revoke A` must take C with it: C's later /rotate, /enroll and
        first-request retirement all fail."""
        seed(resolved, "alpha", 1)
        assert rotate(resolved, 1, 9) is RotateOutcome.ROTATED          # the thief's rotation
        revoke_pin(resolved, fp(1))
        generation = tgen(resolved)
        assert read_trust(resolved).pins == ()                          # C is not trusted
        assert rotate(resolved, 9, 10) is RotateOutcome.REFUSED         # C cannot rotate on
        assert rotate(resolved, 1, 10) is RotateOutcome.REFUSED         # nor can A
        assert retire_predecessor(resolved, fp(9)) is False
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 9, "alpha") is EnrollOutcome.REFUSED    # nor re-enroll C
        mktoken(resolved)
        assert enroll(resolved, 9, "gamma") is EnrollOutcome.REFUSED    # under any id
        assert token_untouched(db_path)
        assert tgen(resolved) == generation
        # the legitimate client enrols a NEW key with a fresh scoped token
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 11, "alpha") is EnrollOutcome.ENROLLED
        assert list(pin_map(resolved)) == [fp(11)]

    def test_the_same_thief_after_the_legitimate_successor_retired_the_old_key(self, resolved):
        """Control: once the real client's successor has retired A, A is not pinned at all, so
        the thief's key (never rotated in) gets nothing, and no cascade is involved."""
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2))
        assert rotate(resolved, 1, 9) is RotateOutcome.REFUSED
        assert list(pin_map(resolved)) == [fp(2)]


@pytest.mark.unit
class TestRevokePinRetiredChain:
    """`revoke_pin` of a key that was already RETIRED by rotation. One request with the
    successor retires the old key (reason `retired`) and clears the successor's `predecessor`,
    which used to leave `trust revoke A` finding A "already revoked" while the thief's C stayed
    trusted. A `retired` record now remembers `superseded_by`, and the revoke follows it."""

    def retired_pair(self, resolved, now=NOW + 30):
        """alpha: 1 -> 2 rotated, then 2's first request retired 1. Only fp(2) is pinned."""
        seed(resolved, "alpha", 1)
        assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED
        assert retire_predecessor(resolved, fp(2), now=now) is True

    # --- the thief scenario ----------------------------------------------------------------

    def test_the_retire_records_the_successor_that_retired_the_key(self, resolved, db_path):
        self.retired_pair(resolved)
        assert revoked_links(db_path) == {fp(1): ("retired", fp(2))}

    def test_revoking_a_retired_key_unpins_the_successor_that_retired_it(self, resolved, db_path):
        self.retired_pair(resolved, now=NOW + 30)
        generation = tgen(resolved)
        result = revoke_pin(resolved, fp(1), now=NOW + 40)
        assert isinstance(result, SupersededRevocation) and isinstance(result, RevokedPin)
        assert not isinstance(result, TrustedPin)               # the retired pin's cert is gone
        assert result.already_retired is True
        assert (result.fingerprint, result.client_id) == (fp(1), "alpha")
        assert (result.reason, result.revoked_at, result.superseded_by) == (
            "retired", NOW + 30, fp(2))                         # A's record, as it stood
        assert [p.fingerprint for p in result.cascaded] == [fp(2)]
        assert type(result.cascaded[0]) is TrustedPin
        assert pin_map(resolved) == {}
        assert revoked_rows(db_path) == {
            fp(1): ("alpha", NOW + 30, "retired"),              # untouched: first reason, first time
            fp(2): ("alpha", NOW + 40, "successor_revoked")}
        assert revoked_links(db_path)[fp(2)] == ("successor_revoked", None)
        assert tgen(resolved) == generation + 1                 # one transaction, one bump

    def test_the_thief_is_locked_out_at_every_door_after_the_retire_too(self, resolved, db_path):
        """The thief rotated A -> C and made one request with C (which retired A). The operator
        then revokes A. C's enroll, rotate and retire are all refused."""
        self.retired_pair(resolved)
        revoke_pin(resolved, fp(1))
        generation = tgen(resolved)
        assert read_trust(resolved).pins == ()
        assert rotate(resolved, 2, 9) is RotateOutcome.REFUSED
        assert rotate(resolved, 1, 9) is RotateOutcome.REFUSED
        assert retire_predecessor(resolved, fp(2)) is False
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 2, "alpha") is EnrollOutcome.REFUSED
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
        assert token_untouched(db_path)
        assert tgen(resolved) == generation
        assert pin_map(resolved) == {}

    def test_a_second_revoke_of_the_same_retired_key_says_already_revoked(self, resolved, db_path):
        self.retired_pair(resolved)
        revoke_pin(resolved, fp(1))
        generation, before = tgen(resolved), table_dump(db_path, "revoked_fingerprints")
        with pytest.raises(LookupError, match="is already revoked"):
            revoke_pin(resolved, fp(1))
        assert tgen(resolved) == generation
        assert table_dump(db_path, "revoked_fingerprints") == before

    def test_the_default_stamp_is_the_wall_clock(self, resolved, db_path):
        self.retired_pair(resolved)
        before = time.time()
        revoke_pin(resolved, fp(1))
        assert before <= revoked_rows(db_path)[fp(2)][1] <= time.time()

    def test_it_does_not_touch_the_clients_unrelated_pins_or_the_config_generation(
        self, resolved
    ):
        seed(resolved, "alpha", 1, 7)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2))
        config_generation = cgen(resolved)
        revoke_pin(resolved, fp(1))
        assert list(pin_map(resolved)) == [fp(7)]
        assert cgen(resolved) == config_generation

    # --- A -> C -> D ------------------------------------------------------------------------

    def test_a_chain_of_retirements_is_followed_to_the_live_end_and_its_pending_tail(
        self, resolved, db_path
    ):
        """A=1 -> C=2 (retired A) -> D=3 (retired C) -> E=4 (pending). Revoking A reaches D
        and E although neither names A, and C is gone from `trusted_clients`."""
        seed(resolved, "alpha", 1)
        seed(resolved, "beta", 9)                                # an unrelated client's pin
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2), now=NOW + 30)
        rotate(resolved, 2, 3, now=NOW + 31)
        retire_predecessor(resolved, fp(3), now=NOW + 32)
        rotate(resolved, 3, 4, now=NOW + 33)
        assert revoked_links(db_path) == {fp(1): ("retired", fp(2)), fp(2): ("retired", fp(3))}
        generation = tgen(resolved)
        result = revoke_pin(resolved, fp(1), now=NOW + 40)
        assert isinstance(result, SupersededRevocation)
        assert [p.fingerprint for p in result.cascaded] == [fp(3), fp(4)]
        assert result.cascaded[1].predecessor == fp(3)          # as it stood before the call
        assert list(pin_map(resolved)) == [fp(9)]
        assert {k: v[2] for k, v in revoked_rows(db_path).items()} == {
            fp(1): "retired", fp(2): "retired",
            fp(3): "successor_revoked", fp(4): "successor_revoked"}
        assert tgen(resolved) == generation + 1

    def test_c_that_never_made_a_request_of_its_own_still_leads_to_d(self, resolved, db_path):
        """A -> C, then C rotates to D BEFORE any request of C's own retired A: the rotate
        retires A (`TestRotateRetiresTheGrandparent`), so revoking A reaches D both while C is
        still pinned and after D has retired C."""
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        rotate(resolved, 2, 3, now=NOW + 25)
        assert revoked_links(db_path) == {fp(1): ("retired", fp(2))}
        # (a) C is still pinned: A leads to C, and C's pending D
        result = revoke_pin(resolved, fp(1), now=NOW + 40)
        assert [(p.fingerprint, p.predecessor) for p in result.cascaded] == [
            (fp(2), None), (fp(3), fp(2))]
        assert pin_map(resolved) == {}

    def test_c_that_never_made_a_request_of_its_own_still_leads_to_d_after_d_retired_c(
        self, resolved, db_path
    ):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        rotate(resolved, 2, 3, now=NOW + 25)
        assert retire_predecessor(resolved, fp(3), now=NOW + 30) is True    # D retires C
        assert list(pin_map(resolved)) == [fp(3)]
        assert revoked_links(db_path) == {fp(1): ("retired", fp(2)), fp(2): ("retired", fp(3))}
        result = revoke_pin(resolved, fp(1), now=NOW + 40)
        assert isinstance(result, SupersededRevocation)
        assert [p.fingerprint for p in result.cascaded] == [fp(3)]      # C is not pinned: skipped
        assert pin_map(resolved) == {}

    def test_revoking_a_middle_retired_key_follows_only_its_own_successors(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2), now=NOW + 30)
        rotate(resolved, 2, 3, now=NOW + 31)
        retire_predecessor(resolved, fp(3), now=NOW + 32)
        rotate(resolved, 3, 4, now=NOW + 33)
        result = revoke_pin(resolved, fp(2), now=NOW + 40)             # the middle one
        assert [p.fingerprint for p in result.cascaded] == [fp(3), fp(4)]

    # --- nothing live: LookupError, nothing written -----------------------------------------

    @pytest.mark.parametrize("how", ["revoked", "client_revoked"])
    def test_a_dead_retired_chain_is_an_error_with_no_write_and_no_bump(
        self, resolved, db_path, how
    ):
        self.retired_pair(resolved)
        if how == "revoked":
            revoke_pin(resolved, fp(2), now=NOW + 35)                    # the successor, directly
        else:
            revoke_client(resolved, "alpha", now=NOW + 35)
        generation = tgen(resolved)
        before = table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints")
        with pytest.raises(LookupError) as excinfo:
            revoke_pin(resolved, fp(1), now=NOW + 99)
        assert "is already revoked" in str(excinfo.value)
        assert "retired by rotation; nothing that superseded it is still pinned" in str(
            excinfo.value)
        assert fp(1) in str(excinfo.value)
        assert tgen(resolved) == generation
        assert table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints") == before

    def test_only_a_retired_record_gets_the_by_rotation_detail(self, resolved):
        seed(resolved, "alpha", 1)
        revoke_pin(resolved, fp(1))
        with pytest.raises(LookupError) as excinfo:
            revoke_pin(resolved, fp(1))
        assert "is already revoked" in str(excinfo.value)
        assert "retired by rotation" not in str(excinfo.value)

    def test_a_retired_record_from_before_the_column_existed_is_a_dead_chain(
        self, resolved, db_path
    ):
        """`superseded_by` NULL on a `retired` row (nothing to follow)."""
        self.retired_pair(resolved)
        sql(db_path, "UPDATE revoked_fingerprints SET superseded_by = NULL")
        generation = tgen(resolved)
        with pytest.raises(LookupError, match="is already revoked"):
            revoke_pin(resolved, fp(1))
        assert list(pin_map(resolved)) == [fp(2)]                # C is NOT reachable: stays
        assert tgen(resolved) == generation

    def test_only_a_retired_record_is_followed(self, resolved, db_path):
        """`superseded_by` on any other reason (a planted row; the schema allows it) is not a
        link: following it would let a stray value unpin a live key."""
        seed(resolved, "alpha", 1, 2)
        sql(db_path, "INSERT INTO revoked_fingerprints "
                     "(fingerprint, client_id, revoked_at, reason, superseded_by) "
                     "VALUES (?, 'alpha', 1.0, 'revoked', ?)", fp(8), fp(2))
        generation = tgen(resolved)
        with pytest.raises(LookupError, match="is already revoked"):
            revoke_pin(resolved, fp(8))
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2)])
        assert tgen(resolved) == generation

    def test_a_non_retired_record_in_the_middle_of_a_chain_is_not_followed(self, resolved, db_path):
        """A (retired) -> B, but B's own record is `revoked` (it carries a stray
        `superseded_by` -> the live pin 5): only `retired` records are links, so the walk
        stops at B and 5 stays pinned."""
        seed(resolved, "alpha", 5)
        for fingerprint, reason, link in ((1, "retired", 2), (2, "revoked", 5)):
            sql(db_path, "INSERT INTO revoked_fingerprints "
                         "(fingerprint, client_id, revoked_at, reason, superseded_by) "
                         "VALUES (?, 'alpha', 1.0, ?, ?)", fp(fingerprint), reason, fp(link))
        generation = tgen(resolved)
        with pytest.raises(LookupError, match="is already revoked"):
            revoke_pin(resolved, fp(1))
        assert list(pin_map(resolved)) == [fp(5)] and tgen(resolved) == generation

    def test_an_unknown_fingerprint_is_still_not_pinned(self, resolved):
        self.retired_pair(resolved)
        with pytest.raises(LookupError, match="no pinned certificate"):
            revoke_pin(resolved, fp(99))

    # --- cycles and planted rows --------------------------------------------------------------

    def test_a_superseded_by_cycle_terminates(self, resolved, db_path):
        seed(resolved, "alpha", 5)
        for fingerprint, other in ((1, 2), (2, 1)):
            sql(db_path, "INSERT INTO revoked_fingerprints "
                         "(fingerprint, client_id, revoked_at, reason, superseded_by) "
                         "VALUES (?, 'alpha', 1.0, 'retired', ?)", fp(fingerprint), fp(other))
        generation = tgen(resolved)
        with pytest.raises(LookupError, match="is already revoked"):
            guarded_call(lambda: revoke_pin(resolved, fp(1)))
        assert list(pin_map(resolved)) == [fp(5)]
        assert tgen(resolved) == generation

    def test_a_cycle_through_a_live_pin_terminates_and_returns_each_pin_once(
        self, resolved, db_path
    ):
        """A -> B -> A through `superseded_by`, B also naming the live pin 5 as a pending
        successor, and 5 naming B back: every fingerprint is visited once."""
        seed(resolved, "alpha", 5)
        for fingerprint, other in ((1, 2), (2, 1)):
            sql(db_path, "INSERT INTO revoked_fingerprints "
                         "(fingerprint, client_id, revoked_at, reason, superseded_by) "
                         "VALUES (?, 'alpha', 1.0, 'retired', ?)", fp(fingerprint), fp(other))
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(2), fp(5))
        result = guarded_call(lambda: revoke_pin(resolved, fp(1)))
        assert isinstance(result, SupersededRevocation)
        assert [p.fingerprint for p in result.cascaded] == [fp(5)]
        assert pin_map(resolved) == {}

    def test_a_pending_successor_of_a_retired_intermediate_is_reached(self, resolved, db_path):
        seed(resolved, "alpha", 5)
        sql(db_path, "INSERT INTO revoked_fingerprints "
                     "(fingerprint, client_id, revoked_at, reason) VALUES (?, 'alpha', 1.0, 'retired')",
            fp(1))
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(1), fp(5))
        result = revoke_pin(resolved, fp(1))
        assert [p.fingerprint for p in result.cascaded] == [fp(5)]

    def test_a_superseded_by_naming_an_unknown_fingerprint_is_just_dead(self, resolved, db_path):
        seed(resolved, "alpha", 5)
        sql(db_path, "INSERT INTO revoked_fingerprints "
                     "(fingerprint, client_id, revoked_at, reason, superseded_by) "
                     "VALUES (?, 'alpha', 1.0, 'retired', ?)", fp(1), fp(77))
        with pytest.raises(LookupError, match="is already revoked"):
            revoke_pin(resolved, fp(1))
        assert list(pin_map(resolved)) == [fp(5)]

    def test_the_hex_check_refuses_a_nul_tailed_link(self, resolved, db_path):
        seed(resolved, "alpha", 5)
        with pytest.raises(sqlite3.IntegrityError, match="ck_revoked_fingerprints_superseded_by_hex"):
            sql(db_path, "INSERT INTO revoked_fingerprints "
                         "(fingerprint, client_id, revoked_at, reason, superseded_by) "
                         "VALUES (?, 'alpha', 1.0, 'retired', ?)", fp(1), fp(2) + "\x00junk")
        assert revoked_links(db_path) == {}

    # --- bookkeeping ----------------------------------------------------------------------------

    def test_the_retired_path_is_all_or_nothing(self, resolved, db_path, monkeypatch):
        self.retired_pair(resolved)
        before = table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints")

        def boom(session):
            raise RuntimeError("injected")

        monkeypatch.setattr(trust, "_bump_trust_generation", boom)
        with pytest.raises(RuntimeError, match="injected"):
            revoke_pin(resolved, fp(1))
        monkeypatch.undo()
        assert table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints") == before
        assert list(pin_map(resolved)) == [fp(2)]

    def test_the_retired_path_is_one_write_transaction(self, resolved, write_tx_calls):
        self.retired_pair(resolved)
        del write_tx_calls[:]
        revoke_pin(resolved, fp(1))
        assert write_tx_calls == [1]

    def test_the_live_path_is_still_a_pin_revocation_that_is_not_already_retired(self, resolved):
        seed(resolved, "alpha", 1)
        result = revoke_pin(resolved, fp(1))
        assert type(result) is PinRevocation and result.already_retired is False
        assert result.cascaded == ()

    # --- reads --------------------------------------------------------------------------------

    def test_superseded_by_is_exposed_by_list_revoked_and_list_clients(self, resolved):
        seed(resolved, "alpha", 1, 7)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2), now=NOW + 30)
        revoke_pin(resolved, fp(7), now=NOW + 31)
        listed = {r.fingerprint: r for r in list_revoked(resolved)}
        assert listed[fp(1)] == RevokedPin(fp(1), "alpha", NOW + 30, "retired", fp(2))
        assert listed[fp(1)].superseded_by == fp(2)
        assert listed[fp(7)].superseded_by is None             # only a retired record has one
        (record,) = list_clients(resolved)
        assert {r.fingerprint: r.superseded_by for r in record.revoked_pins} == {
            fp(1): fp(2), fp(7): None}

    def test_a_cascaded_record_carries_no_link_and_a_dataclass_default_is_none(self, resolved):
        self.retired_pair(resolved)
        revoke_pin(resolved, fp(1))
        listed = {r.fingerprint: r for r in list_revoked(resolved)}
        assert listed[fp(2)].reason == "successor_revoked" and listed[fp(2)].superseded_by is None
        assert RevokedPin(fp(1), "alpha", 1.0, "revoked").superseded_by is None
        with pytest.raises(dataclasses.FrozenInstanceError):
            listed[fp(1)].superseded_by = None

    def test_revoke_client_reports_the_link_on_its_revoked_pins(self, resolved):
        self.retired_pair(resolved)
        record = revoke_client(resolved, "alpha", now=NOW + 40)
        assert {r.fingerprint: (r.reason, r.superseded_by) for r in record.revoked_pins} == {
            fp(1): ("retired", fp(2)), fp(2): ("client_revoked", None)}

    def test_retire_predecessor_records_the_link_and_the_stamp(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2), now=NOW + 44)
        assert sql(db_path, "SELECT revoked_at, reason, superseded_by FROM revoked_fingerprints") == [
            (NOW + 44, "retired", fp(2))]


@pytest.mark.unit
class TestRotateRetiresTheGrandparent:
    """`rotate_client(old_fp=C, new_pin=D)` where C is itself a rotation successor whose own
    first request has not retired its predecessor A yet: A is retired (same transaction, same
    bump), recording `superseded_by=C`, so `revoke_pin(A)` still reaches D."""

    def a_to_c(self, resolved):
        seed(resolved, "alpha", 1)
        assert rotate(resolved, 1, 2, now=NOW + 20) is RotateOutcome.ROTATED

    def test_a_is_retired_with_the_link_the_stamp_and_one_bump(self, resolved, db_path):
        self.a_to_c(resolved)
        generation = tgen(resolved)
        assert rotate(resolved, 2, 3, now=NOW + 25) is RotateOutcome.ROTATED
        assert tgen(resolved) == generation + 1
        pins = pin_map(resolved)
        assert {k: v.predecessor for k, v in pins.items()} == {fp(2): None, fp(3): fp(2)}
        assert (pins[fp(3)].client_id, pins[fp(3)].enrolled_via, pins[fp(3)].enrolled_at) == (
            "alpha", "rotate", NOW + 25)
        assert revoked_rows(db_path) == {fp(1): ("alpha", NOW + 25, "retired")}
        assert revoked_links(db_path) == {fp(1): ("retired", fp(2))}

    def test_the_stamp_is_the_clock_read_under_the_write_lock(self, resolved, db_path):
        self.a_to_c(resolved)
        assert rotate_client(resolved, old_fp=fp(2), new_pin=newpin(3),
                             now=lambda: NOW + 61) is RotateOutcome.ROTATED
        assert revoked_rows(db_path)[fp(1)][1] == NOW + 61
        assert pin_map(resolved)[fp(3)].enrolled_at == NOW + 61

    def test_a_chain_of_live_pending_pins_cannot_be_built_through_rotate(self, resolved):
        self.a_to_c(resolved)
        rotate(resolved, 2, 3, now=NOW + 25)
        assert fp(1) not in pin_map(resolved)
        assert rotate(resolved, 1, 9, now=NOW + 26) is RotateOutcome.REFUSED   # A is gone

    def test_the_retired_key_is_refused_for_re_enrollment(self, resolved, db_path):
        self.a_to_c(resolved)
        rotate(resolved, 2, 3, now=NOW + 25)
        mktoken(resolved, client_id="alpha")
        assert enroll(resolved, 1, "alpha") is EnrollOutcome.REFUSED
        assert token_untouched(db_path)

    def test_a_repeat_of_the_rotation_is_already_and_retires_nothing_more(self, resolved, db_path):
        self.a_to_c(resolved)
        rotate(resolved, 2, 3, now=NOW + 25)
        generation, before = tgen(resolved), table_dump(db_path, "revoked_fingerprints")
        assert rotate(resolved, 2, 3, now=NOW + 99) is RotateOutcome.ALREADY
        assert tgen(resolved) == generation
        assert table_dump(db_path, "revoked_fingerprints") == before

    def test_already_retires_nothing_while_the_predecessor_is_still_pinned(self, resolved, db_path):
        """D is already C's successor but C's predecessor A is still pinned (planted: only a
        rotate that ran before this change could have left it so). ALREADY writes nothing."""
        self.a_to_c(resolved)
        sql(db_path, "INSERT INTO trusted_clients VALUES (?, 'alpha', ?, ?, ?, 'rotate', ?)",
            fp(3), newpin(3).cert_pem, newpin(3).not_after, NOW + 21, fp(2))
        generation, before = tgen(resolved), table_dump(db_path, "revoked_fingerprints",
                                                         "trusted_clients")
        assert rotate(resolved, 2, 3, now=NOW + 99) is RotateOutcome.ALREADY
        assert tgen(resolved) == generation
        assert table_dump(db_path, "revoked_fingerprints", "trusted_clients") == before
        assert fp(1) in pin_map(resolved)

    @pytest.mark.parametrize("case", [
        "new_pin_revoked", "second_successor", "new_pin_bound_elsewhere", "new_is_old",
        "new_pinned_without_predecessor", "old_unknown",
    ])
    def test_a_refused_rotation_retires_nothing(self, resolved, db_path, case):
        self.a_to_c(resolved)
        seed(resolved, "beta", 5)
        if case == "new_pin_revoked":
            revoke_pin(resolved, fp(5))
            fp_old, new = 2, 5
        elif case == "second_successor":
            sql(db_path, "INSERT INTO trusted_clients VALUES (?, 'alpha', 'PEM', 9e9, ?, 'rotate', ?)",
                fp(8), NOW + 22, fp(2))
            fp_old, new = 2, 3
        elif case == "new_pin_bound_elsewhere":
            fp_old, new = 2, 5
        elif case == "new_is_old":
            fp_old, new = 2, 2
        elif case == "new_pinned_without_predecessor":
            fp_old, new = 2, 1
        else:
            fp_old, new = 99, 3
        pins, generation = pin_map(resolved), tgen(resolved)
        revoked = table_dump(db_path, "revoked_fingerprints")
        assert rotate(resolved, fp_old, new, now=NOW + 99) is RotateOutcome.REFUSED
        assert fp(1) in pin_map(resolved)                    # A, C's predecessor, still pinned
        assert pin_map(resolved) == pins and tgen(resolved) == generation
        assert table_dump(db_path, "revoked_fingerprints") == revoked

    def test_a_stale_predecessor_field_is_only_cleared(self, resolved, db_path):
        seed(resolved, "alpha", 2)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ?", fp(1))     # not pinned
        generation = tgen(resolved)
        assert rotate(resolved, 2, 3) is RotateOutcome.ROTATED
        assert pin_map(resolved)[fp(2)].predecessor is None
        assert pin_map(resolved)[fp(3)].predecessor == fp(2)
        assert revoked_rows(db_path) == {}                  # nothing is retired
        assert tgen(resolved) == generation + 1

    def test_a_predecessor_pinned_to_a_different_client_is_not_retired(self, resolved, db_path):
        seed(resolved, "alpha", 2)
        seed(resolved, "beta", 1)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(1), fp(2))
        assert rotate(resolved, 2, 3) is RotateOutcome.ROTATED
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2), fp(3)])
        assert pin_map(resolved)[fp(2)].predecessor is None
        assert revoked_rows(db_path) == {}

    def test_a_successor_without_a_predecessor_retires_nothing(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2), now=NOW + 30)
        before = table_dump(db_path, "revoked_fingerprints")
        assert rotate(resolved, 2, 3, now=NOW + 31) is RotateOutcome.ROTATED
        assert table_dump(db_path, "revoked_fingerprints") == before

    def test_the_rotation_is_all_or_nothing(self, resolved, db_path, monkeypatch):
        self.a_to_c(resolved)
        before = table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints")

        def boom(session):
            raise RuntimeError("injected")

        monkeypatch.setattr(trust, "_bump_trust_generation", boom)
        with pytest.raises(RuntimeError, match="injected"):
            rotate(resolved, 2, 3, now=NOW + 25)
        monkeypatch.undo()
        assert table_dump(db_path, *TRUST_TABLES, "revoked_fingerprints") == before
        assert fp(1) in pin_map(resolved) and fp(3) not in pin_map(resolved)

    def test_revoking_a_first_leaves_nothing_for_the_rotation_to_retire_or_pin(
        self, resolved, db_path
    ):
        """The other serialisation of the race: revoke A, then C -> D finds C unpinned."""
        self.a_to_c(resolved)
        revoke_pin(resolved, fp(1))
        generation = tgen(resolved)
        assert rotate(resolved, 2, 3, now=NOW + 25) is RotateOutcome.REFUSED
        assert pin_map(resolved) == {} and tgen(resolved) == generation
        assert set(revoked_rows(db_path)) == {fp(1), fp(2)}

    def test_one_write_transaction(self, resolved, write_tx_calls):
        self.a_to_c(resolved)
        del write_tx_calls[:]
        rotate(resolved, 2, 3)
        assert write_tx_calls == [1]


def guarded_call(fn, timeout=20):
    """Run `fn` on a thread so a regression that loops fails instead of hanging the suite."""
    out = {}

    def body():
        try:
            out["v"] = fn()
        except BaseException as exc:  # noqa: BLE001
            out["v"] = exc

    t = threading.Thread(target=body, daemon=True)
    t.start()
    t.join(timeout)
    assert not t.is_alive(), "the call did not finish"
    if isinstance(out["v"], BaseException):
        raise out["v"]
    return out["v"]



# ===========================================================================
# Rotation
# ===========================================================================


@pytest.mark.unit
class TestRotation:
    def test_rotating_pins_a_successor_for_the_same_client(self, resolved):
        seed(resolved, "alpha", 1)
        generation = tgen(resolved)
        assert rotate(resolved, 1, 2, now=NOW + 33) is RotateOutcome.ROTATED
        pins = pin_map(resolved)
        assert sorted(pins) == sorted([fp(1), fp(2)])          # the old pin stays until retired
        new = pins[fp(2)]
        assert (new.client_id, new.enrolled_via, new.predecessor, new.enrolled_at) == (
            "alpha", "rotate", fp(1), NOW + 33)
        assert pins[fp(1)].predecessor is None
        assert tgen(resolved) == generation + 1

    def test_a_repeat_is_already_and_does_not_bump(self, resolved):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        generation = tgen(resolved)
        assert rotate(resolved, 1, 2) is RotateOutcome.ALREADY
        assert tgen(resolved) == generation

    @pytest.mark.parametrize("case", [
        "unknown_old", "revoked_client", "new_bound_elsewhere", "new_is_old",
        "new_pinned_without_predecessor", "new_pinned_with_other_predecessor",
    ])
    def test_the_refused_cases_change_nothing(self, resolved, case):
        seed(resolved, "alpha", 1, 2)
        seed(resolved, "beta", 5)
        rotate(resolved, 2, 3)                             # fp(3) is a successor of fp(2)
        before = (pin_map(resolved), tgen(resolved))
        if case == "unknown_old":
            outcome = rotate(resolved, 99, 4)
        elif case == "revoked_client":
            revoke_client(resolved, "beta")
            before = (pin_map(resolved), tgen(resolved))
            outcome = rotate(resolved, 5, 4)
        elif case == "new_bound_elsewhere":
            outcome = rotate(resolved, 1, 5)
        elif case == "new_is_old":
            outcome = rotate(resolved, 1, 1)
        elif case == "new_pinned_without_predecessor":
            outcome = rotate(resolved, 1, 2)               # fp(2) is alpha's, but a token pin
        else:
            outcome = rotate(resolved, 1, 3)               # fp(3) succeeds fp(2), not fp(1)
        assert outcome is RotateOutcome.REFUSED
        assert (pin_map(resolved), tgen(resolved)) == before

    def test_chains_keep_their_predecessors(self, resolved):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        assert retire_predecessor(resolved, fp(2)) is True
        assert rotate(resolved, 2, 3) is RotateOutcome.ROTATED
        pins = pin_map(resolved)
        assert sorted(pins) == sorted([fp(2), fp(3)])
        assert pins[fp(3)].predecessor == fp(2)

    def test_rotation_validates_its_arguments(self, resolved):
        with pytest.raises(ValueError):
            rotate_client(resolved, old_fp="nope", new_pin=newpin(2), now=NOW)
        with pytest.raises(ValueError):
            rotate_client(resolved, old_fp=fp(1), new_pin=fp(2), now=NOW)
        with pytest.raises(ValueError):
            rotate_client(resolved, old_fp=fp(1), new_pin=newpin(2), now=float("inf"))

    def test_the_old_fingerprint_may_be_given_in_any_spelling(self, resolved):
        seed(resolved, "alpha", 1)
        assert rotate_client(resolved, old_fp=fp(1).upper(), new_pin=newpin(2),
                             now=NOW) is RotateOutcome.ROTATED

    # --- retire_predecessor -------------------------------------------------------------------

    def test_retiring_deletes_the_predecessor_clears_the_field_and_bumps(self, resolved):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        generation = tgen(resolved)
        assert retire_predecessor(resolved, fp(2)) is True
        pins = pin_map(resolved)
        assert list(pins) == [fp(2)] and pins[fp(2)].predecessor is None
        assert tgen(resolved) == generation + 1

    def test_retiring_is_idempotent(self, resolved):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2))
        generation = tgen(resolved)
        assert retire_predecessor(resolved, fp(2)) is False
        assert tgen(resolved) == generation

    def test_nothing_to_retire_is_false_and_writes_nothing(self, resolved):
        seed(resolved, "alpha", 1)
        generation = tgen(resolved)
        assert retire_predecessor(resolved, fp(1)) is False        # no predecessor
        assert retire_predecessor(resolved, fp(9)) is False        # unknown pin
        assert tgen(resolved) == generation
        with pytest.raises(ValueError):
            retire_predecessor(resolved, "xyz")

    def test_a_predecessor_that_is_gone_is_cleared_without_a_bump(self, resolved, db_path):
        seed(resolved, "alpha", 2)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ?", fp(1))   # stale
        generation = tgen(resolved)
        assert retire_predecessor(resolved, fp(2)) is False
        assert pin_map(resolved)[fp(2)].predecessor is None
        assert tgen(resolved) == generation

    def test_a_predecessor_belonging_to_another_client_is_never_deleted(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        seed(resolved, "beta", 2)
        sql(db_path, "UPDATE trusted_clients SET predecessor_fingerprint = ? WHERE fingerprint = ?",
            fp(1), fp(2))
        generation = tgen(resolved)
        assert retire_predecessor(resolved, fp(2)) is False
        assert sorted(pin_map(resolved)) == sorted([fp(1), fp(2)])
        assert pin_map(resolved)[fp(2)].predecessor is None
        assert tgen(resolved) == generation

    def test_retiring_the_old_pin_leaves_the_clients_other_pins(self, resolved):
        seed(resolved, "alpha", 1, 7)
        rotate(resolved, 1, 2)
        retire_predecessor(resolved, fp(2))
        assert sorted(pin_map(resolved)) == sorted([fp(2), fp(7)])


# ===========================================================================
# Reads
# ===========================================================================


@pytest.mark.unit
class TestTrustReads:
    def test_an_empty_store(self, resolved):
        snapshot = read_trust(resolved)
        assert isinstance(snapshot, TrustSnapshot)
        assert snapshot.pins == () and snapshot.generation == 0
        assert snapshot.key == read_trust_key(resolved)
        assert list_clients(resolved) == ()

    def test_the_snapshot_key_matches_the_poll_key(self, resolved):
        seed(resolved, "alpha", 1)
        snapshot = read_trust(resolved)
        key = read_trust_key(resolved)
        assert isinstance(key, TrustKey) and snapshot.key == key == (key.store_id, key.generation)
        assert key.generation == 1 and key.store_id != 0

    def test_pins_are_ordered_by_client_then_enrollment_then_fingerprint(self, resolved):
        seed(resolved, "beta", 3)
        seed(resolved, "alpha", 5)
        mktoken(resolved, client_id="alpha")
        enroll(resolved, 1, "alpha", now=NOW + 30)
        mktoken(resolved, client_id="alpha")
        enroll(resolved, 2, "alpha", now=NOW + 20)
        assert [(p.client_id, p.fingerprint) for p in read_trust(resolved).pins] == [
            ("alpha", fp(5)), ("alpha", fp(2)), ("alpha", fp(1)), ("beta", fp(3))]

    def test_clients_are_ordered_by_id_and_include_revoked_ones(self, resolved):
        seed(resolved, "zed", 1)
        seed(resolved, "alpha", 2)
        seed(resolved, "mid", 3)
        revoke_client(resolved, "mid")
        assert [(c.client_id, c.revoked) for c in list_clients(resolved)] == [
            ("alpha", False), ("mid", True), ("zed", False)]

    def test_records_and_pins_are_frozen(self, resolved):
        seed(resolved, "alpha", 1)
        (record,) = list_clients(resolved)
        with pytest.raises(dataclasses.FrozenInstanceError):
            record.label = "x"
        with pytest.raises(dataclasses.FrozenInstanceError):
            record.pins[0].client_id = "x"
        with pytest.raises(dataclasses.FrozenInstanceError):
            read_trust(resolved).pins = ()

    def test_nothing_returned_is_an_orm_object(self, resolved):
        seed(resolved, "alpha", 1)
        mktoken(resolved)
        results = [
            read_client_id(resolved), read_trust(resolved), read_trust_key(resolved),
            list_clients(resolved), token_status(resolved, NOW + 1),
            revoke_pin(resolved, fp(1)),
        ]
        for result in results:
            assert type(result).__module__ != "zfsbackup.config.store.models"
            assert not hasattr(result, "_sa_instance_state")

    def test_a_missing_trust_state_row_reads_as_generation_zero(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        sql(db_path, "DELETE FROM trust_state")
        assert tgen(resolved) == 0
        mktoken(resolved)
        enroll(resolved, 2, "beta")
        assert tgen(resolved) == 1                  # the row is recreated by the next bump


# ===========================================================================
# Generations and the store key
# ===========================================================================


@pytest.mark.unit
class TestGenerations:
    def test_the_trust_generation_moves_exactly_when_the_set_of_pins_changes(self, resolved):
        trail = []

        def step(label, action):
            before = (tgen(resolved), cgen(resolved))
            action()
            after = (tgen(resolved), cgen(resolved))
            trail.append((label, after[0] - before[0], after[1] - before[1]))

        step("create_token", lambda: mktoken(resolved))
        step("enroll refused", lambda: enroll(resolved, secret=OTHER_SECRET))
        step("enroll", lambda: enroll(resolved, 1))
        step("create_token 2", lambda: mktoken(resolved))
        step("enroll already", lambda: enroll(resolved, 1))
        step("enroll taken", lambda: enroll(resolved, 2))
        step("revoke_token", lambda: revoke_token(resolved, lock=None))
        step("rotate", lambda: rotate(resolved, 1, 2))
        step("rotate already", lambda: rotate(resolved, 1, 2))
        step("rotate refused", lambda: rotate(resolved, 9, 3))
        step("retire", lambda: retire_predecessor(resolved, fp(2)))
        step("retire again", lambda: retire_predecessor(resolved, fp(2)))
        step("revoke_pin", lambda: revoke_pin(resolved, fp(2)))
        step("revoke_client (no pins)", lambda: revoke_client(resolved, "alpha"))
        step("revoke_client again", lambda: revoke_client(resolved, "alpha"))
        step("ensure_client_id", lambda: ensure_client_id(resolved, lock=None))
        step("ensure_client_id again", lambda: ensure_client_id(resolved, lock=None))
        step("set_client_id", lambda: set_client_id(resolved, "newid", expected_generation=None,
                                                    lock=None))
        step("set_client_id same", lambda: set_client_id(resolved, "newid",
                                                         expected_generation=None, lock=None))
        assert trail == [
            ("create_token", 0, 0), ("enroll refused", 0, 0), ("enroll", 1, 0),
            ("create_token 2", 0, 0), ("enroll already", 0, 0), ("enroll taken", 0, 0),
            ("revoke_token", 0, 0), ("rotate", 1, 0), ("rotate already", 0, 0),
            ("rotate refused", 0, 0), ("retire", 1, 0), ("retire again", 0, 0),
            ("revoke_pin", 1, 0), ("revoke_client (no pins)", 0, 0),
            ("revoke_client again", 0, 0),
            ("ensure_client_id", 0, 1), ("ensure_client_id again", 0, 0),
            ("set_client_id", 0, 1), ("set_client_id same", 0, 0),
        ]

    def test_replacing_the_database_file_changes_the_key_even_at_the_same_generation(
        self, tmp_path, resolved, db_path
    ):
        seed(resolved, "alpha", 1)
        before = read_trust_key(resolved)
        assert before.generation == 1

        other = make_store(tmp_path / "other")
        seed(other, "alpha", 1)                              # the same trust generation
        assert tgen(other) == 1
        for path in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")):
            if path.exists():
                path.unlink()
        # fold the other file's WAL into the main file, then drop its sidecars
        sqlite3.connect(str(tmp_path / "other" / "config.db")).execute(
            "PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(tmp_path / "other" / "config.db") + suffix)
            if sidecar.exists():
                sidecar.unlink()
        os.replace(tmp_path / "other" / "config.db", db_path)

        after = read_trust_key(resolve_config_path(str(db_path)))
        assert after.generation == before.generation
        assert after.store_id != before.store_id
        assert after != before

    def test_the_key_is_stable_across_reads(self, resolved):
        assert read_trust_key(resolved) == read_trust_key(resolved)


# ===========================================================================
# The API writers' read-only filter, clock and in-process lock (review fixes)
# ===========================================================================


@pytest.fixture
def write_tx_calls(monkeypatch):
    """Every entry into `trust._write_tx` (the only way an API writer takes SQLite's write
    lock), counted; the real one still runs."""
    calls = []
    real = trust._write_tx

    @contextlib.contextmanager
    def spy(resolved):
        calls.append(1)
        with real(resolved) as session:
            yield session

    monkeypatch.setattr(trust, "_write_tx", spy)
    return calls


def sequence_clock(*values):
    """A zero-argument clock that returns `values` in turn and records each reading."""
    pending = list(values)
    seen = []

    def clock():
        value = pending.pop(0)
        seen.append(value)
        return value

    clock.seen = seen
    return clock


def write_lock_is_free(db_path) -> bool:
    """Could another connection take SQLite's write lock right now? (Rolled back at once.)"""
    conn = sqlite3.connect(str(db_path), timeout=0, isolation_level=None)
    try:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            assert "locked" in str(exc)
            return False
        conn.execute("ROLLBACK")
        return True
    finally:
        conn.close()


def table_dump(db_path, *tables):
    return {t: sql(db_path, f"SELECT * FROM {t} ORDER BY 1") for t in tables}


TRUST_TABLES = ("enrollment_token", "trust_state", "trusted_clients", "clients")


@pytest.mark.unit
class TestTokenPrecheck:
    """`token_precheck`: True only for an existing, unspent token, with `now >= created_at` and
    both credentials matching. Read-only."""

    def pre(self, resolved, *, secret=SECRET, server=SERVER, now=NOW + 10):
        return token_precheck(resolved, secret_sha256=secret, server_fingerprint=server, now=now)

    CASES = [
        # id, how to prepare, kwargs, expected
        ("live_and_matching", "token", {}, True),
        ("exactly_at_creation", "token", {"now": NOW}, True),
        ("one_tick_before_creation", "token", {"now": NOW - 0.001}, False),
        ("far_before_creation", "token", {"now": NOW - 1e6}, False),
        ("expired_with_matching_credentials", "token", {"now": NOW + 500}, True),
        ("exactly_at_expiry", "token", {"now": NOW + 100}, True),
        ("wrong_secret", "token", {"secret": OTHER_SECRET}, False),
        ("wrong_server", "token", {"server": OTHER_SERVER}, False),
        ("both_wrong", "token", {"secret": OTHER_SECRET, "server": OTHER_SERVER}, False),
        ("wrong_secret_and_expired", "token", {"secret": OTHER_SECRET, "now": NOW + 500}, False),
        ("wrong_secret_and_early", "token", {"secret": OTHER_SECRET, "now": NOW - 5}, False),
        ("secret_too_short", "token", {"secret": SECRET[:-1]}, False),
        ("secret_uppercase", "token", {"secret": SECRET.upper()}, False),
        ("secret_none", "token", {"secret": None}, False),
        ("secret_bytes", "token", {"secret": SECRET.encode()}, False),
        ("secret_int", "token", {"secret": 5}, False),
        ("secret_non_ascii", "token", {"secret": "é" * 64}, False),
        ("secret_with_nul_tail", "token", {"secret": SECRET + "\x00"}, False),
        ("server_in_any_spelling", "token", {"server": SERVER.upper()}, True),
        ("no_token", "none", {}, False),
        ("redeemed", "redeemed", {}, False),
        ("retired_by_the_latch", "latched", {}, False),
        ("only_consumed_fingerprint", "cfp", {}, False),
        ("only_consumed_client_id", "ccid", {}, False),
        ("scoped_token_matching", "scoped", {}, True),
    ]

    @pytest.mark.parametrize("prep, kwargs, expected",
                             [c[1:] for c in CASES], ids=[c[0] for c in CASES])
    def test_the_matrix(self, resolved, db_path, prep, kwargs, expected):
        if prep == "scoped":
            seed(resolved, "alpha", 1)
            mktoken(resolved, created=NOW, ttl=100, client_id="alpha")
        elif prep != "none":
            mktoken(resolved, created=NOW, ttl=100)
        if prep == "redeemed":
            assert enroll(resolved, 9, "other") is EnrollOutcome.ENROLLED
        elif prep == "latched":
            sql(db_path, "UPDATE enrollment_token SET consumed_at = ?", NOW + 1)
        elif prep == "cfp":
            sql(db_path, "UPDATE enrollment_token SET consumed_fingerprint = ?", fp(9))
        elif prep == "ccid":
            sql(db_path, "UPDATE enrollment_token SET consumed_client_id = 'zed'")
        assert self.pre(resolved, **kwargs) is expected
        assert_cache_empty()

    @pytest.mark.parametrize("server", ["nope", "", "ab" * 31, "zz" * 32, None])
    def test_a_bad_server_fingerprint_is_a_value_error(self, resolved, server):
        mktoken(resolved)
        with pytest.raises(ValueError):
            self.pre(resolved, server=server)

    @pytest.mark.parametrize("now", [float("inf"), float("nan"), "x", None, True])
    def test_a_bad_clock_is_a_value_error(self, resolved, now):
        mktoken(resolved)
        with pytest.raises(ValueError):
            self.pre(resolved, now=now)

    def test_a_callable_clock_is_read_exactly_once(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        clock = sequence_clock(NOW + 5)
        assert self.pre(resolved, now=clock) is True
        assert clock.seen == [NOW + 5]

    def test_it_is_bool_typed_not_truthy(self, resolved):
        mktoken(resolved)
        assert self.pre(resolved) is True
        assert self.pre(resolved, secret=OTHER_SECRET) is False

    # --- read-only ----------------------------------------------------------------------------

    def test_it_answers_quickly_under_a_held_write_lock_without_config_busy(
        self, resolved, db_path
    ):
        # Deliberately NOT `short_busy`: a call that tried the write lock would wait ~5 s.
        mktoken(resolved, created=NOW, ttl=100)
        with write_lock_held(db_path):
            started = time.monotonic()
            assert self.pre(resolved) is True
            assert self.pre(resolved, secret=OTHER_SECRET) is False
            assert self.pre(resolved, now=NOW + 500) is True
            assert time.monotonic() - started < 1.0
        assert_cache_empty()

    @pytest.mark.parametrize("kwargs", [
        {}, {"now": NOW + 500}, {"secret": OTHER_SECRET}, {"now": NOW - 5},
    ], ids=["live", "expired_matching", "wrong_secret", "early"])
    def test_it_changes_nothing(self, resolved, db_path, kwargs):
        mktoken(resolved, created=NOW, ttl=100)
        before, generation = table_dump(db_path, *TRUST_TABLES), tgen(resolved)
        self.pre(resolved, **kwargs)
        assert table_dump(db_path, *TRUST_TABLES) == before
        assert tgen(resolved) == generation
        assert token_row(db_path) == (None, None, None)     # an expired token is NOT latched here
        assert token_status(resolved, NOW + 50).state is TokenState.ACTIVE

    def test_it_neither_takes_the_api_write_lock_nor_opens_a_write_transaction(
        self, resolved, write_tx_calls
    ):
        mktoken(resolved)
        write_tx_calls.clear()          # `create_token` above is a CLI writer
        assert trust._API_WRITE_LOCK.acquire(timeout=1)
        try:
            started = time.monotonic()
            assert self.pre(resolved) is True
            assert time.monotonic() - started < 1.0
        finally:
            trust._API_WRITE_LOCK.release()
        assert write_tx_calls == []

    def test_it_leaves_no_engine_descriptor_or_read_mark(self, resolved, db_path):
        sidecars = [db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")]
        mktoken(resolved)
        self.pre(resolved)
        baseline = fd_count()
        for _ in range(5):
            self.pre(resolved)
            self.pre(resolved, secret=OTHER_SECRET)
        assert_cache_empty()
        assert fd_count() == baseline
        assert open_fds_on(*sidecars) == 0
        assert checkpoint_busy(db_path) == 0

    def test_it_is_refused_on_an_old_or_unknown_schema(self, resolved, db_path):
        mktoken(resolved)
        sql(db_path, "UPDATE alembic_version SET version_num = ?", BASELINE)
        with pytest.raises(SchemaOutOfDate):
            self.pre(resolved)
        sql(db_path, "UPDATE alembic_version SET version_num = 'deadbeef0000'")
        with pytest.raises(SchemaVersionMismatch):
            self.pre(resolved)
        assert_cache_empty()

    def test_enroll_client_filters_with_it(self, resolved, monkeypatch):
        """`enroll_client` calls the public function (the API relies on both agreeing)."""
        mktoken(resolved)
        seen = []
        real = trust.token_precheck

        def spy(*args, **kwargs):
            seen.append(kwargs)
            return real(*args, **kwargs)

        monkeypatch.setattr(trust, "token_precheck", spy)
        assert enroll(resolved) is EnrollOutcome.ENROLLED
        (call,) = seen
        assert call["secret_sha256"] == SECRET and call["server_fingerprint"] == SERVER

    # --- the credential comparison itself -----------------------------------------------------

    @pytest.fixture
    def digest_calls(self, monkeypatch):
        calls = []
        real = trust.hmac.compare_digest

        def spy(a, b):
            calls.append((a, b))
            return real(a, b)

        monkeypatch.setattr(trust.hmac, "compare_digest", spy)
        return calls

    @pytest.mark.parametrize("secret, server, expected", [
        (SECRET, SERVER, True),
        (OTHER_SECRET, SERVER, False),
        (SECRET, OTHER_SERVER, False),
        (OTHER_SECRET, OTHER_SERVER, False),
        (None, SERVER, False),
        (SECRET[:-1], SERVER, False),
    ], ids=["match", "bad_secret", "bad_server", "both_bad", "malformed_secret", "short_secret"])
    def test_both_credentials_are_always_compared_in_constant_time(
        self, digest_calls, secret, server, expected
    ):
        row = SimpleNamespace(server_fingerprint=SERVER, secret_sha256=SECRET)
        assert trust._token_credentials_match(
            row, server_fp=server, secret_sha256=secret) is expected
        assert len(digest_calls) == 2          # no early exit: a mismatch costs the same

    def test_a_malformed_hash_is_refused_even_if_the_dummy_would_compare_equal(self):
        row = SimpleNamespace(server_fingerprint=SERVER, secret_sha256="0" * 64)
        assert trust._token_credentials_match(
            row, server_fp=SERVER, secret_sha256=None) is False
        assert trust._token_credentials_match(
            row, server_fp=SERVER, secret_sha256="0" * 64) is True


@pytest.mark.unit
class TestRefusedEnrollNeverTakesTheWriteLock:
    """The read-only pre-check: a refusal that can be decided on a snapshot never reaches
    `_write_tx`. Proven twice: the spy counts zero entries, AND the call returns fast while a
    raw connection holds `BEGIN IMMEDIATE` (a call that did try would wait the 5 s busy timeout
    and raise `ConfigBusy`)."""

    CASES = [
        ("no_token", {}),
        ("wrong_secret", {"secret": OTHER_SECRET}),
        ("wrong_server_fingerprint", {"server": OTHER_SERVER}),
        ("both_wrong", {"secret": OTHER_SECRET, "server": OTHER_SERVER}),
        ("clock_behind_creation", {"now": NOW - 1}),
        ("spent_by_a_redemption", {"spent": "redeemed"}),
        ("spent_by_the_latch", {"spent": "latched"}),
        ("expired_with_a_wrong_secret", {"now": NOW + 500, "secret": OTHER_SECRET}),
        ("malformed_hash", {"secret": SECRET[:-1]}),
        ("non_ascii_hash", {"secret": "é" * 64}),
        ("callable_clock_behind_creation", {"now": "callable_early"}),
    ]

    @pytest.mark.parametrize("opts", [c[1] for c in CASES], ids=[c[0] for c in CASES])
    def test_refusals_decided_on_a_snapshot(self, resolved, db_path, write_tx_calls, opts):
        opts = dict(opts)
        if opts.get("now") == "callable_early":
            opts["now"] = sequence_clock(NOW - 1)
        # `no_token` is the only case with an empty option set: no token row at all.
        if opts:
            mktoken(resolved, created=NOW, ttl=100)
        spent = opts.pop("spent", None)
        if spent == "redeemed":
            assert enroll(resolved, 9, "other") is EnrollOutcome.ENROLLED
        elif spent == "latched":
            sql(db_path, "UPDATE enrollment_token SET consumed_at = ?", NOW + 1)
        write_tx_calls.clear()
        before, generation = table_dump(db_path, *TRUST_TABLES), tgen(resolved)
        with write_lock_held(db_path):
            started = time.monotonic()
            assert enroll(resolved, 1, "alpha", **{"now": NOW + 10, **opts}) \
                is EnrollOutcome.REFUSED
            assert time.monotonic() - started < 1.0
        assert write_tx_calls == []
        assert table_dump(db_path, *TRUST_TABLES) == before
        assert tgen(resolved) == generation

    def test_a_refused_callable_clock_is_read_once_and_only_for_the_filter(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        clock = sequence_clock(NOW - 1)
        assert enroll(resolved, now=clock) is EnrollOutcome.REFUSED
        assert clock.seen == [NOW - 1]

    def test_a_refusal_does_not_wait_for_the_in_process_lock_either(self, resolved):
        mktoken(resolved)
        assert trust._API_WRITE_LOCK.acquire(timeout=1)
        try:
            started = time.monotonic()
            assert enroll(resolved, secret=OTHER_SECRET) is EnrollOutcome.REFUSED
            assert time.monotonic() - started < 1.0
        finally:
            trust._API_WRITE_LOCK.release()

    def test_control_a_matching_token_does_reach_the_write_transaction(
        self, resolved, write_tx_calls
    ):
        mktoken(resolved, created=NOW, ttl=100)
        write_tx_calls.clear()
        assert enroll(resolved, now=NOW + 10) is EnrollOutcome.ENROLLED
        assert write_tx_calls == [1]

    def test_control_an_expired_token_with_the_right_secret_reaches_it_and_is_latched(
        self, resolved, db_path, write_tx_calls
    ):
        mktoken(resolved, created=NOW, ttl=100)
        write_tx_calls.clear()
        assert enroll(resolved, now=NOW + 500) is EnrollOutcome.REFUSED
        assert write_tx_calls == [1]
        assert token_row(db_path)[0] == NOW + 500


@pytest.mark.unit
class TestUnauthenticatedCallersCannotBurnAToken:
    """`_token_redeemable` checks the credentials BEFORE the clock, so only a holder of the
    secret can latch an expired token."""

    @pytest.mark.parametrize("opts", [
        {"secret": OTHER_SECRET}, {"server": OTHER_SERVER},
        {"secret": OTHER_SECRET, "server": OTHER_SERVER}, {"secret": SECRET.upper()},
        {"secret": None},
    ], ids=["wrong_secret", "wrong_server", "both", "uppercase", "none"])
    def test_after_expiry_wrong_credentials_do_not_latch(self, resolved, db_path, opts):
        mktoken(resolved, created=NOW, ttl=100)
        for _ in range(3):
            assert enroll(resolved, now=NOW + 500, **opts) is EnrollOutcome.REFUSED
        assert token_row(db_path) == (None, None, None)
        # The attack this prevents: a forward clock step, then back into the window.
        assert enroll(resolved, now=NOW + 50) is EnrollOutcome.ENROLLED

    def test_after_expiry_the_right_secret_does_latch(self, resolved, db_path):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, secret=OTHER_SECRET, now=NOW + 500) is EnrollOutcome.REFUSED
        assert token_row(db_path) == (None, None, None)
        assert enroll(resolved, now=NOW + 500) is EnrollOutcome.REFUSED
        assert token_row(db_path) == (NOW + 500, None, None)
        assert enroll(resolved, now=NOW + 50) is EnrollOutcome.REFUSED      # latched for good
        assert token_status(resolved, NOW + 50).state is TokenState.EXPIRED

    def test_the_write_transaction_alone_also_checks_credentials_before_the_clock(
        self, resolved, db_path
    ):
        """`_token_redeemable` directly (the pre-check is only a filter, never the decision)."""
        mktoken(resolved, created=NOW, ttl=100)
        with trust._write_tx(resolved) as session:
            row = trust._select_token(session)
            assert trust._token_redeemable(
                session, row, server_fp=SERVER, secret_sha256=OTHER_SECRET, now=NOW + 500) is False
        assert token_row(db_path) == (None, None, None)
        with trust._write_tx(resolved) as session:
            row = trust._select_token(session)
            assert trust._token_redeemable(
                session, row, server_fp=SERVER, secret_sha256=SECRET, now=NOW + 500) is False
        assert token_row(db_path) == (NOW + 500, None, None)


@pytest.mark.unit
class TestCallableClock:
    def test_a_number_still_works_and_a_callable_is_read_twice_for_enroll(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        clock = sequence_clock(NOW + 10, NOW + 20)
        assert enroll(resolved, now=clock) is EnrollOutcome.ENROLLED
        assert clock.seen == [NOW + 10, NOW + 20]

    def test_the_second_reading_drives_every_timestamp(self, resolved, db_path):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, now=sequence_clock(NOW + 10, NOW + 20)) is EnrollOutcome.ENROLLED
        (record,) = list_clients(resolved)
        assert record.created_at == NOW + 20
        assert record.pins[0].enrolled_at == NOW + 20
        assert token_row(db_path)[0] == NOW + 20

    def test_the_clock_is_read_under_the_write_lock(self, resolved, db_path):
        """The first reading (the filter) sees the write lock free; the second is taken after
        `BEGIN IMMEDIATE`, so it sees it HELD. This is what stops lock waiting from stretching a
        token's life."""
        mktoken(resolved, created=NOW, ttl=100)
        observed = []

        def clock():
            observed.append(write_lock_is_free(db_path))
            return NOW + 10

        assert enroll(resolved, now=clock) is EnrollOutcome.ENROLLED
        assert observed == [True, False]

    def test_valid_at_the_filter_but_expired_at_the_second_reading_is_refused_and_latched(
        self, resolved, db_path
    ):
        mktoken(resolved, created=NOW, ttl=100)
        clock = sequence_clock(NOW + 10, NOW + 200)
        assert enroll(resolved, now=clock) is EnrollOutcome.REFUSED
        assert clock.seen == [NOW + 10, NOW + 200]
        assert token_row(db_path) == (NOW + 200, None, None)
        assert read_trust(resolved).pins == ()
        assert enroll(resolved, now=NOW + 50) is EnrollOutcome.REFUSED     # stays latched
        assert tgen(resolved) == 0

    def test_expired_at_the_filter_but_valid_at_the_second_reading_is_enrolled(self, resolved):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, now=sequence_clock(NOW + 200, NOW + 10)) \
            is EnrollOutcome.ENROLLED

    def test_behind_creation_at_the_second_reading_is_refused_and_not_latched(
        self, resolved, db_path
    ):
        mktoken(resolved, created=NOW, ttl=100)
        assert enroll(resolved, now=sequence_clock(NOW + 10, NOW - 5)) is EnrollOutcome.REFUSED
        assert token_row(db_path) == (None, None, None)
        assert enroll(resolved, now=NOW + 10) is EnrollOutcome.ENROLLED

    def test_a_bad_second_reading_raises_writes_nothing_and_releases_the_lock(
        self, resolved, db_path
    ):
        mktoken(resolved, created=NOW, ttl=100)
        with pytest.raises(ValueError, match="finite"):
            enroll(resolved, now=sequence_clock(NOW + 10, float("inf")))
        assert not trust._API_WRITE_LOCK.locked()
        assert write_lock_is_free(db_path)
        assert token_row(db_path) == (None, None, None)
        assert_cache_empty()
        assert enroll(resolved, now=NOW + 10) is EnrollOutcome.ENROLLED

    def test_rotate_reads_the_clock_once_under_the_write_lock(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        observed = []

        def clock():
            observed.append(write_lock_is_free(db_path))
            return NOW + 33

        assert rotate_client(resolved, old_fp=fp(1), new_pin=newpin(2), now=clock) \
            is RotateOutcome.ROTATED
        assert observed == [False]
        assert pin_map(resolved)[fp(2)].enrolled_at == NOW + 33

    def test_a_rotate_refused_by_its_filter_never_reads_the_clock(self, resolved):
        seed(resolved, "alpha", 1)
        clock = sequence_clock()
        assert rotate_client(resolved, old_fp=fp(99), new_pin=newpin(2), now=clock) \
            is RotateOutcome.REFUSED
        assert clock.seen == []

    def test_a_non_number_non_callable_is_a_value_error_before_anything_is_opened(
        self, resolved, write_tx_calls
    ):
        for now in ("x", None, True, float("nan")):
            with pytest.raises(ValueError):
                rotate_client(resolved, old_fp=fp(1), new_pin=newpin(2), now=now)
            with pytest.raises(ValueError):
                enroll(resolved, now=now)
        assert write_tx_calls == []


@pytest.mark.unit
class TestOneSuccessorPerPredecessor:
    def test_a_second_different_successor_is_refused_and_changes_nothing(self, resolved):
        seed(resolved, "alpha", 1)
        assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED
        before, generation = pin_map(resolved), tgen(resolved)
        assert rotate(resolved, 1, 3) is RotateOutcome.REFUSED
        assert pin_map(resolved) == before and fp(3) not in pin_map(resolved)
        assert tgen(resolved) == generation

    def test_the_same_successor_again_is_already(self, resolved):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        rotate(resolved, 1, 3)                                   # refused, in between
        generation = tgen(resolved)
        assert rotate(resolved, 1, 2) is RotateOutcome.ALREADY
        assert tgen(resolved) == generation

    def test_after_the_successor_retires_the_predecessor_it_can_rotate_on(self, resolved):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        assert rotate(resolved, 1, 3) is RotateOutcome.REFUSED
        assert retire_predecessor(resolved, fp(2)) is True
        assert rotate(resolved, 2, 3) is RotateOutcome.ROTATED
        pins = pin_map(resolved)
        assert sorted(pins) == sorted([fp(2), fp(3)])
        assert pins[fp(3)].predecessor == fp(2) and pins[fp(2)].predecessor is None
        # and A can no longer rotate at all: it is gone
        assert rotate(resolved, 1, 4) is RotateOutcome.REFUSED

    def test_the_operator_unpinning_the_pending_successor_lets_the_client_rotate_again(
        self, resolved
    ):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        assert rotate(resolved, 1, 3) is RotateOutcome.REFUSED
        revoke_pin(resolved, fp(2))
        assert rotate(resolved, 1, 3) is RotateOutcome.ROTATED

    def test_a_refusal_for_a_pending_successor_is_per_predecessor(self, resolved):
        seed(resolved, "alpha", 1, 5)                            # two independent pins
        assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED
        assert rotate(resolved, 5, 3) is RotateOutcome.ROTATED

    def test_the_filter_refuses_an_unpinned_or_revoked_old_pin_without_the_write_lock(
        self, resolved, db_path, write_tx_calls
    ):
        seed(resolved, "alpha", 1)
        seed(resolved, "beta", 5)
        revoke_client(resolved, "beta")
        write_tx_calls.clear()
        with write_lock_held(db_path):
            started = time.monotonic()
            assert rotate(resolved, 99, 2) is RotateOutcome.REFUSED
            assert rotate(resolved, 5, 3) is RotateOutcome.REFUSED
            assert time.monotonic() - started < 1.0
        assert write_tx_calls == []

    def test_retire_with_no_predecessor_takes_no_write_lock(self, resolved, db_path, write_tx_calls):
        seed(resolved, "alpha", 1)
        generation = tgen(resolved)
        write_tx_calls.clear()
        with write_lock_held(db_path):
            started = time.monotonic()
            assert retire_predecessor(resolved, fp(1)) is False      # no predecessor
            assert retire_predecessor(resolved, fp(9)) is False      # unknown pin
            assert time.monotonic() - started < 1.0
        assert write_tx_calls == []
        assert tgen(resolved) == generation
        assert_cache_empty()

    def test_retire_with_a_predecessor_does_need_it(self, resolved, db_path, short_busy):
        seed(resolved, "alpha", 1)
        rotate(resolved, 1, 2)
        with write_lock_held(db_path):
            with pytest.raises(ConfigBusy, match="nothing was written"):
                retire_predecessor(resolved, fp(2))
        assert retire_predecessor(resolved, fp(2)) is True


@pytest.mark.integration
class TestRotationRace:
    def test_two_regenerated_keys_racing_leave_exactly_one_successor(self, resolved):
        seed(resolved, "alpha", 1)
        results = run_threads(
            8, lambda i: rotate_client(resolved, old_fp=fp(1), new_pin=newpin(2 + i % 2),
                                       now=time.time))
        assert not [r for r in results if isinstance(r, BaseException)], results
        assert results.count(RotateOutcome.ROTATED) == 1
        assert set(results) <= {RotateOutcome.ROTATED, RotateOutcome.ALREADY, RotateOutcome.REFUSED}
        successors = [p for p in pin_map(resolved).values() if p.predecessor == fp(1)]
        assert len(successors) == 1
        assert len(pin_map(resolved)) == 2
        assert_cache_empty()


@pytest.mark.unit
class TestApiWriteLock:
    @pytest.fixture
    def short_lock_wait(self, monkeypatch):
        monkeypatch.setattr(trust, "_API_WRITE_LOCK_TIMEOUT_S", 0.05)

    def test_the_wait_is_bounded_by_the_busy_timeout(self):
        assert trust._API_WRITE_LOCK_TIMEOUT_S == store_db._BUSY_TIMEOUT_MS / 1000.0

    @pytest.mark.parametrize("name", ["enroll_client", "rotate_client", "retire_predecessor"])
    def test_a_lock_wait_timeout_is_config_busy_and_nothing_is_written(
        self, resolved, db_path, short_lock_wait, name
    ):
        seed(resolved, "alpha", 1)
        mktoken(resolved)
        rotate(resolved, 1, 2)                       # a pending successor for `retire`
        call = {
            "enroll_client": lambda: enroll(resolved, 5, "beta"),
            "rotate_client": lambda: rotate(resolved, 2, 3),
            "retire_predecessor": lambda: retire_predecessor(resolved, fp(2)),
        }[name]
        before, generation = table_dump(db_path, *TRUST_TABLES), tgen(resolved)
        assert trust._API_WRITE_LOCK.acquire(timeout=1)
        try:
            started = time.monotonic()
            with pytest.raises(ConfigBusy, match="nothing was written"):
                call()
            assert time.monotonic() - started < 2.0
        finally:
            trust._API_WRITE_LOCK.release()
        assert table_dump(db_path, *TRUST_TABLES) == before and tgen(resolved) == generation
        assert_cache_empty()
        # the failed wait left the lock free, and the same call now works
        assert not trust._API_WRITE_LOCK.locked()
        assert call() in (EnrollOutcome.ENROLLED, RotateOutcome.ROTATED, True)

    def test_the_lock_is_released_when_the_write_fails(self, resolved, db_path, short_busy):
        mktoken(resolved)
        with write_lock_held(db_path):
            with pytest.raises(ConfigBusy):
                enroll(resolved)
        assert not trust._API_WRITE_LOCK.locked()

    def test_the_lock_is_released_after_every_outcome(self, resolved):
        seed(resolved, "alpha", 1)
        mktoken(resolved)
        rotate(resolved, 1, 2)
        for call in (
            lambda: enroll(resolved, 5, "beta"),
            lambda: enroll(resolved, 6, "gamma"),                     # token spent: filter
            lambda: rotate(resolved, 2, 3),
            lambda: rotate(resolved, 2, 3),                           # ALREADY
            lambda: retire_predecessor(resolved, fp(2)),
            lambda: retire_predecessor(resolved, fp(2)),
        ):
            call()
            assert not trust._API_WRITE_LOCK.locked()

    def test_the_in_process_lock_is_taken_before_any_sqlite_write_lock(self, resolved):
        """A thread waiting for `_API_WRITE_LOCK` holds no SQLite lock, so a CLI writer is not
        delayed by the queue of API requests."""
        mktoken(resolved)
        outcome = []
        assert trust._API_WRITE_LOCK.acquire(timeout=1)
        thread = threading.Thread(target=lambda: outcome.append(enroll(resolved)), daemon=True)
        try:
            thread.start()
            time.sleep(0.3)
            assert thread.is_alive(), "the enroller should be queued on the in-process lock"
            started = time.monotonic()
            ensure_client_id(resolved, lock=None)
            assert time.monotonic() - started < 2.0
        finally:
            trust._API_WRITE_LOCK.release()
            thread.join(15)
        assert outcome == [EnrollOutcome.ENROLLED]

    def test_the_reinit_hook_replaces_the_lock_with_a_fresh_unlocked_one(self, monkeypatch):
        stuck = threading.Lock()
        stuck.acquire()
        monkeypatch.setattr(trust, "_API_WRITE_LOCK", stuck)
        trust._reinit_api_lock_after_fork()
        assert trust._API_WRITE_LOCK is not stuck
        assert not trust._API_WRITE_LOCK.locked()

    @pytest.mark.skipif("fork" not in START_METHODS, reason="needs the fork start method")
    @pytest.mark.filterwarnings("ignore:.*multi-threaded.*fork.*:DeprecationWarning")
    def test_a_fork_while_another_thread_holds_the_lock_gives_the_child_a_fresh_one(
        self, resolved, monkeypatch
    ):
        monkeypatch.setattr(trust, "_API_WRITE_LOCK_TIMEOUT_S", 0.5)
        seed(resolved, "alpha", 1)
        held, release = threading.Event(), threading.Event()

        def holder():
            with trust._API_WRITE_LOCK:
                held.set()
                release.wait(30)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        ctx = multiprocessing.get_context("fork")
        reader, writer = ctx.Pipe(duplex=False)

        def child(conn):
            try:
                at_start = trust._API_WRITE_LOCK.locked()
                outcome = rotate(resolved, 1, 2)
                conn.send(("ok", at_start, outcome.value))
            except BaseException as exc:  # noqa: BLE001
                conn.send(("error", repr(exc)))
            finally:
                conn.close()

        proc = None
        try:
            assert held.wait(10)
            assert trust._API_WRITE_LOCK.locked()
            proc = ctx.Process(target=child, args=(writer,))
            proc.start()
            writer.close()
            assert reader.poll(30), "the forked child did not answer"
            message = reader.recv()
            proc.join(10)
            assert proc.exitcode == 0
            # the child's lock was NOT the stuck one, and its API write went through
            assert message == ("ok", False, "rotated")
            # the parent's lock is untouched by the child
            assert trust._API_WRITE_LOCK.locked()
        finally:
            release.set()
            thread.join(10)
            if proc is not None:
                if proc.is_alive():
                    proc.kill()
                proc.join(10)
            reader.close()
        assert pin_map(resolved)[fp(2)].predecessor == fp(1)


# ===========================================================================
# Error paths
# ===========================================================================


def _every_function():
    """(name, callable(resolved)) for every public function, with arguments that would otherwise
    succeed (or be a plain refusal) on a healthy store."""
    return [
        ("read_client_id", lambda r: read_client_id(r)),
        ("read_trust_key", lambda r: read_trust_key(r)),
        ("read_trust", lambda r: read_trust(r)),
        ("list_clients", lambda r: list_clients(r)),
        ("token_status", lambda r: token_status(r, NOW)),
        ("ensure_client_id", lambda r: ensure_client_id(r, lock=None)),
        ("set_client_id", lambda r: set_client_id(r, "web-1", expected_generation=None, lock=None)),
        ("create_token", lambda r: mktoken(r)),
        ("revoke_token", lambda r: revoke_token(r, lock=None)),
        ("revoke_pin", lambda r: revoke_pin(r, fp(1), lock=None)),
        ("revoke_client", lambda r: revoke_client(r, "alpha", lock=None)),
        ("token_precheck", lambda r: token_precheck(
            r, secret_sha256=SECRET, server_fingerprint=SERVER, now=NOW)),
        ("enroll_client", lambda r: enroll(r)),
        ("rotate_client", lambda r: rotate(r, 1, 2)),
        ("retire_predecessor", lambda r: retire_predecessor(r, fp(1))),
    ]


FUNCTION_IDS = [name for name, _ in _every_function()]
FUNCTIONS = dict(_every_function())


@pytest.mark.unit
class TestErrorPaths:
    @pytest.mark.parametrize("name", FUNCTION_IDS)
    def test_a_pre_tls_schema_is_refused_by_every_function(self, resolved, db_path, name):
        sql(db_path, "UPDATE alembic_version SET version_num = ?", BASELINE)
        with pytest.raises(SchemaOutOfDate):
            FUNCTIONS[name](resolved)
        assert_cache_empty()

    @pytest.mark.parametrize("name", FUNCTION_IDS)
    def test_an_unknown_schema_revision_is_refused_by_every_function(self, resolved, db_path, name):
        sql(db_path, "UPDATE alembic_version SET version_num = 'deadbeef0000'")
        with pytest.raises(SchemaVersionMismatch):
            FUNCTIONS[name](resolved)

    def test_a_refused_schema_writes_nothing(self, resolved, db_path):
        sql(db_path, "UPDATE alembic_version SET version_num = ?", BASELINE)
        for name in ("create_token", "ensure_client_id", "enroll_client"):
            with pytest.raises(SchemaOutOfDate):
                FUNCTIONS[name](resolved)
        sql(db_path, "UPDATE alembic_version SET version_num = ?", EXPECTED_HEAD)
        assert token_status(resolved, NOW).state is TokenState.NONE
        assert read_client_id(resolved) is None
        assert tgen(resolved) == 0

    @pytest.mark.parametrize("name", [
        "enroll_client", "rotate_client", "retire_predecessor", "create_token", "revoke_token",
        "revoke_pin", "revoke_client", "ensure_client_id", "set_client_id",
    ])
    def test_a_locked_database_is_config_busy_for_every_writer(
        self, resolved, db_path, short_busy, name
    ):
        # Each API writer first filters on a read-only snapshot, so it must be given a call that
        # passes that filter to reach the write lock at all: a live token for `enroll_client`, a
        # pin that names a predecessor for `retire_predecessor` (`rotate_client`'s old pin is live).
        seed(resolved, "alpha", 1)
        if name == "enroll_client":
            mktoken(resolved)
        elif name == "retire_predecessor":
            assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED
        call = (
            (lambda: retire_predecessor(resolved, fp(2))) if name == "retire_predecessor"
            else (lambda: FUNCTIONS[name](resolved))
        )
        before, generation = pin_map(resolved), tgen(resolved)
        with write_lock_held(db_path):
            started = time.monotonic()
            with pytest.raises(ConfigBusy):
                call()
            assert time.monotonic() - started < 5
        assert_cache_empty()
        # Nothing was written, and the store is usable once the lock is released.
        assert pin_map(resolved) == before
        assert tgen(resolved) == generation

    def test_config_busy_writes_nothing_and_a_retry_succeeds(self, resolved, db_path, short_busy):
        mktoken(resolved)
        with write_lock_held(db_path):
            with pytest.raises(ConfigBusy):
                enroll(resolved)
        assert token_status(resolved, NOW + 11).state is TokenState.ACTIVE
        assert read_trust(resolved).pins == ()
        assert enroll(resolved) is EnrollOutcome.ENROLLED

    @pytest.mark.parametrize("name", ["enroll_client", "rotate_client", "retire_predecessor"])
    def test_the_busy_message_says_nothing_was_written_for_writers(self, resolved, db_path,
                                                                    short_busy, name):
        seed(resolved, "alpha", 1)
        mktoken(resolved)                                  # a live token: enroll reaches the writer
        rotate(resolved, 1, 2)                             # a predecessor: retire reaches it
        call = {
            "enroll_client": lambda: enroll(resolved, 5, "beta"),
            "rotate_client": lambda: rotate(resolved, 2, 3),
            "retire_predecessor": lambda: retire_predecessor(resolved, fp(2)),
        }[name]
        with write_lock_held(db_path):
            with pytest.raises(ConfigBusy, match="nothing was written"):
                call()

    def test_readers_are_not_blocked_by_a_writer(self, resolved, db_path):
        seed(resolved, "alpha", 1)
        with write_lock_held(db_path):
            assert list(pin_map(resolved)) == [fp(1)]
            assert read_trust_key(resolved).generation == 1
            assert read_client_id(resolved) is None
            assert [c.client_id for c in list_clients(resolved)] == ["alpha"]
            assert token_status(resolved, NOW).state is TokenState.CONSUMED

    @pytest.mark.parametrize("name", [
        "create_token", "revoke_token", "revoke_pin", "revoke_client", "ensure_client_id",
        "set_client_id",
    ])
    def test_a_held_writer_lock_is_config_busy_for_the_cli_writers(self, resolved, name):
        """`lock=None` takes the flock for the call; another holder makes it time out."""
        seed(resolved, "alpha", 1)
        ready, leave = threading.Event(), threading.Event()

        def holder():
            with writer_lock(resolved):
                ready.set()
                leave.wait(30)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            calls = {
                "create_token": lambda: create_token(
                    resolved, secret_sha256=SECRET, server_fingerprint=SERVER, created_at=NOW,
                    expires_at=NOW + 10, lock=None, lock_timeout=0.2),
                "revoke_token": lambda: revoke_token(resolved, lock=None, lock_timeout=0.2),
                "revoke_pin": lambda: revoke_pin(resolved, fp(1), lock=None, lock_timeout=0.2),
                "revoke_client": lambda: revoke_client(resolved, "alpha", lock=None,
                                                       lock_timeout=0.2),
                "ensure_client_id": lambda: ensure_client_id(resolved, lock=None,
                                                             lock_timeout=0.2),
                "set_client_id": lambda: set_client_id(resolved, "web-1", expected_generation=None,
                                                       lock=None, lock_timeout=0.2),
            }
            with pytest.raises(ConfigBusy):
                calls[name]()
        finally:
            leave.set()
            thread.join(10)
        assert list(pin_map(resolved)) == [fp(1)] and read_client_id(resolved) is None

    def test_the_api_writers_do_not_take_the_flock(self, resolved):
        seed(resolved, "alpha", 1)
        mktoken(resolved)
        ready, leave = threading.Event(), threading.Event()

        def holder():
            with writer_lock(resolved):
                ready.set()
                leave.wait(30)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            assert enroll(resolved, 2, "beta") is EnrollOutcome.ENROLLED
            assert rotate(resolved, 2, 3) is RotateOutcome.ROTATED
            assert retire_predecessor(resolved, fp(3)) is True
        finally:
            leave.set()
            thread.join(10)

    @pytest.mark.parametrize("name", FUNCTION_IDS)
    def test_a_missing_database_is_refused_and_nothing_is_created(self, tmp_path, name):
        missing = resolve_config_path(str(tmp_path / "nowhere" / "config.db"))
        with pytest.raises(ConfigDbNotFound):
            FUNCTIONS[name](missing)
        assert not (tmp_path / "nowhere").exists()

    @pytest.mark.parametrize("name", ["enroll_client", "rotate_client", "retire_predecessor"])
    def test_the_api_functions_work_on_an_uninitialised_config(self, resolved, db_path, name):
        """They never read GlobalSettings, so a store without config still serves trust."""
        sql(db_path, "DELETE FROM global_settings")
        mktoken(resolved)
        FUNCTIONS[name](resolved)       # no exception


# ===========================================================================
# The lock= contract of the CLI writers
# ===========================================================================


def _cli_writers(lock):
    return {
        "ensure_client_id": lambda r: ensure_client_id(r, lock=lock),
        "set_client_id": lambda r: set_client_id(r, "web-1", expected_generation=None, lock=lock),
        "create_token": lambda r: create_token(
            r, secret_sha256=SECRET, server_fingerprint=SERVER, created_at=NOW,
            expires_at=NOW + 10, lock=lock),
        "revoke_token": lambda r: revoke_token(r, lock=lock),
        "revoke_pin": lambda r: revoke_pin(r, fp(1), lock=lock),
        "revoke_client": lambda r: revoke_client(r, "alpha", lock=lock),
    }


CLI_WRITER_IDS = list(_cli_writers(None))


@pytest.mark.unit
class TestLockContract:
    @pytest.mark.parametrize("name", CLI_WRITER_IDS)
    def test_the_lock_keyword_is_required(self, resolved, name):
        functions = {
            "ensure_client_id": lambda: ensure_client_id(resolved),
            "set_client_id": lambda: set_client_id(resolved, "web-1", expected_generation=None),
            "create_token": lambda: create_token(
                resolved, secret_sha256=SECRET, server_fingerprint=SERVER, created_at=NOW,
                expires_at=NOW + 10),
            "revoke_token": lambda: revoke_token(resolved),
            "revoke_pin": lambda: trust.revoke_pin(resolved, fp(1)),
            "revoke_client": lambda: trust.revoke_client(resolved, "alpha"),
        }
        with pytest.raises(TypeError):
            functions[name]()

    @pytest.mark.parametrize("name", CLI_WRITER_IDS)
    @pytest.mark.parametrize("kind", ["str", "unheld_path", "stale", "other_database", "other_thread"])
    def test_a_lock_that_is_not_held_by_this_thread_for_this_database_is_refused(
        self, tmp_path, resolved, name, kind
    ):
        seed(resolved, "alpha", 1)
        before = (read_trust(resolved).pins, cgen(resolved), tgen(resolved))
        other = make_store(tmp_path / "elsewhere")
        holder_thread = None
        leave = threading.Event()
        if kind == "str":
            lock = str(writer_lock_path(resolved))
        elif kind == "unheld_path":
            lock = writer_lock_path(resolved)
        elif kind == "stale":
            with writer_lock(resolved) as lock:
                pass
        elif kind == "other_database":
            with writer_lock(other) as foreign:
                lock = foreign
                with pytest.raises(ValueError):
                    _cli_writers(lock)[name](resolved)
            lock = None
        else:
            box = []
            ready = threading.Event()

            def holder():
                with writer_lock(resolved) as held:
                    box.append(held)
                    ready.set()
                    leave.wait(30)

            holder_thread = threading.Thread(target=holder, daemon=True)
            holder_thread.start()
            assert ready.wait(10)
            lock = box[0]
        try:
            if lock is not None:
                with pytest.raises(ValueError):
                    _cli_writers(lock)[name](resolved)
        finally:
            leave.set()
            if holder_thread is not None:
                holder_thread.join(10)
        assert (read_trust(resolved).pins, cgen(resolved), tgen(resolved)) == before
        assert read_client_id(resolved) is None

    def test_a_held_lock_serves_a_whole_multi_step_edit(self, resolved):
        with writer_lock(resolved) as held:
            client_id, _ = ensure_client_id(resolved, lock=held)
            create_token(resolved, secret_sha256=SECRET, server_fingerprint=SERVER, created_at=NOW,
                         expires_at=NOW + 10, lock=held)
            revoke_token(resolved, lock=held)
            set_client_id(resolved, "web-1", expected_generation=None, lock=held)
        assert read_client_id(resolved) == "web-1" != client_id

    def test_lock_none_takes_and_releases_the_flock_itself(self, resolved):
        ensure_client_id(resolved, lock=None)
        with writer_lock(resolved, timeout=1):           # free again
            pass

    def test_the_preflight_runs_before_the_lock_file_is_touched(self, tmp_path):
        missing = resolve_config_path(str(tmp_path / "nowhere" / "config.db"))
        with pytest.raises(ConfigDbNotFound):
            ensure_client_id(missing, lock=None)
        assert not (tmp_path / "nowhere").exists()


# ===========================================================================
# db.ephemeral_engine / paths.open_config_session(ephemeral=True)
# ===========================================================================


@pytest.mark.unit
class TestEphemeralEngine:
    @pytest.mark.parametrize("url", ["sqlite:///:memory:", "sqlite://", "sqlite:///file::memory:"])
    @pytest.mark.parametrize("readonly", [False, True])
    def test_in_memory_urls_are_refused(self, url, readonly):
        with pytest.raises(ValueError):
            with store_db.ephemeral_engine(url, readonly=readonly):
                pytest.fail("an in-memory ephemeral engine must not be built")
        assert_cache_empty()

    def test_each_call_builds_a_fresh_unpooled_uncached_engine(self, resolved):
        url = resolved.url()
        with store_db.ephemeral_engine(url) as first, store_db.ephemeral_engine(url) as second:
            assert first is not second
            assert isinstance(first.pool, NullPool)
            assert store_db._ENGINES == {}
        assert store_db._ENGINES == {}

    def test_an_ephemeral_writer_holds_the_write_lock_from_its_first_statement(
        self, resolved, db_path, short_busy
    ):
        with store_db.ephemeral_engine(resolved.url()) as engine:
            with store_db.session_for_engine(engine) as session:
                session.execute(text("SELECT 1"))               # first statement: BEGIN IMMEDIATE
                probe = sqlite3.connect(str(db_path), timeout=0, isolation_level=None)
                try:
                    with pytest.raises(sqlite3.OperationalError, match="locked"):
                        probe.execute("BEGIN IMMEDIATE")
                finally:
                    probe.close()

    def test_the_connection_pragmas_match_the_cached_engines(self, resolved):
        with store_db.ephemeral_engine(resolved.url()) as engine:
            with store_db.session_for_engine(engine) as session:
                conn = session.connection()
                assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
                assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == \
                    store_db._BUSY_TIMEOUT_MS
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"

    def test_foreign_keys_are_enforced_on_a_writer_session(self, resolved):
        with pytest.raises(Exception, match="FOREIGN KEY"):
            with open_config_session(resolved, readonly=False, ephemeral=True) as session:
                session.execute(text(
                    "INSERT INTO trusted_clients (fingerprint, client_id, cert_pem, not_after, "
                    "enrolled_at, enrolled_via) VALUES (:fp, 'nobody', 'p', 1, 1, 'token')"),
                    {"fp": fp(1)})

    def test_an_ephemeral_reader_cannot_write(self, resolved):
        with pytest.raises((ReadOnlySessionError, OperationalError)):
            with open_config_session(resolved, readonly=True, ephemeral=True) as session:
                session.execute(text("INSERT INTO trust_state (id, generation) VALUES (1, 5)"))
        assert tgen(resolved) == 0

    def test_open_config_session_preflights_and_caches_nothing(self, resolved, tmp_path):
        with open_config_session(resolved, readonly=True, ephemeral=True) as session:
            assert session.execute(text("SELECT count(*) FROM clients")).scalar() == 0
            assert store_db._ENGINES == {}
        missing = resolve_config_path(str(tmp_path / "nowhere" / "config.db"))
        with pytest.raises(ConfigDbNotFound):
            with open_config_session(missing, readonly=True, ephemeral=True):
                pass
        assert not (tmp_path / "nowhere").exists()

    def test_the_session_commits_on_success_and_rolls_back_on_error(self, resolved):
        with open_config_session(resolved, readonly=False, ephemeral=True) as session:
            session.execute(text("INSERT INTO trust_state (id, generation) VALUES (1, 41)"))
        assert tgen(resolved) == 41
        with pytest.raises(RuntimeError):
            with open_config_session(resolved, readonly=False, ephemeral=True) as session:
                session.execute(text("UPDATE trust_state SET generation = 99"))
                raise RuntimeError("boom")
        assert tgen(resolved) == 41


# ===========================================================================
# Resource hygiene
# ===========================================================================


def _mixed_calls(resolved, index):
    """One thread's worth of trust traffic: reads, a refused enroll, an enroll under its own
    token-less id (refused), pin churn on its own client."""
    own = f"t{index}"
    for _ in range(3):
        read_trust(resolved)
        read_trust_key(resolved)
        list_clients(resolved)
        token_status(resolved, NOW)
        read_client_id(resolved)
        enroll_client(resolved, secret_sha256=SECRET, server_fingerprint=SERVER, client_id=own,
                      pin=newpin(1000 + index), now=NOW, dataset_exists=False)
        retire_predecessor(resolved, fp(2000 + index))
        rotate_client(resolved, old_fp=fp(3000 + index), new_pin=newpin(4000 + index), now=NOW)


@pytest.mark.unit
class TestResourceHygiene:
    def test_no_descriptor_or_read_mark_survives_many_threaded_calls(self, resolved, db_path):
        sidecars = [db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")]
        seed(resolved, "alpha", 1)
        # warm up once so one-time process descriptors (logging, locale, ...) are not counted
        _mixed_calls(resolved, 0)
        baseline_fds, baseline_db = fd_count(), open_fds_on(*sidecars)

        results = run_threads(8, lambda i: _mixed_calls(resolved, i))

        assert results == [None] * 8, results
        assert fd_count() == baseline_fds
        assert open_fds_on(*sidecars) == baseline_db == 0
        assert_cache_empty()
        assert checkpoint_busy(db_path) == 0

    def test_the_checkpoint_probe_does_detect_an_open_reader(self, resolved, db_path):
        """Control for the test above: a reader that is still open makes the probe report busy."""
        seed(resolved, "alpha", 1)
        reader = sqlite3.connect(str(db_path), timeout=10, isolation_level=None)
        try:
            reader.execute("BEGIN")
            reader.execute("SELECT count(*) FROM clients").fetchall()
            write_row = sql(db_path, "SELECT count(*) FROM trusted_clients")
            assert write_row == [(1,)]
            # a write after the reader's snapshot leaves frames the reader pins
            mktoken(resolved)
            assert checkpoint_busy(db_path) == 1
        finally:
            reader.close()
        assert checkpoint_busy(db_path) == 0

    def test_a_failing_call_releases_everything_too(self, resolved, db_path, short_busy):
        sidecars = [db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")]
        mktoken(resolved)             # live, so `enroll` passes its read-only filter and is busy
        baseline = fd_count()
        with write_lock_held(db_path):
            for _ in range(3):
                with pytest.raises(ConfigBusy):
                    enroll(resolved)
        with pytest.raises(LookupError):
            revoke_pin(resolved, fp(9), lock=None)
        with pytest.raises(ValueError):
            revoke_pin(resolved, "bad", lock=None)
        assert fd_count() == baseline
        assert open_fds_on(*sidecars) == 0
        assert checkpoint_busy(db_path) == 0

    def test_the_engine_cache_stays_empty_across_every_function(self, resolved):
        seed(resolved, "alpha", 1)
        for name, call in _every_function():
            try:
                call(resolved)
            except (LookupError, ValueError, ConfigBusy):
                pass
            assert store_db._ENGINES == {}, name

    def test_dispose_all_in_another_thread_is_harmless(self, resolved):
        stop = threading.Event()
        failures = []

        def disposer():
            # A real cached engine each round, so `dispose_all` has something to dispose while the
            # trust calls run on their own engines.
            while not stop.is_set():
                try:
                    store_db.get_engine(resolved.url(), readonly=True)
                    dispose_all()
                except BaseException as exc:  # noqa: BLE001
                    failures.append(exc)
                    return
                time.sleep(0.001)

        thread = threading.Thread(target=disposer, daemon=True)
        thread.start()
        try:
            results = run_threads(6, lambda i: _mixed_calls(resolved, i))
        finally:
            stop.set()
            thread.join(10)
        assert failures == []
        assert results == [None] * 6, results
        assert not thread.is_alive()

    def test_dispose_all_does_not_disturb_a_call_in_flight(self, resolved, db_path):
        """A writer holding its transaction open while `dispose_all()` runs still commits."""
        mktoken(resolved)
        with store_db.ephemeral_engine(resolved.url()) as engine:
            with store_db.session_for_engine(engine) as session:
                session.execute(text("INSERT INTO trust_state (id, generation) VALUES (1, 7)"))
                dispose_all()
        assert tgen(resolved) == 7

    def test_a_cached_engine_is_untouched_by_ephemeral_use(self, resolved):
        """The supervisor's cached engine and the API worker's per-call engines are separate
        worlds: using the trust API neither adds to nor removes from the cache."""
        engine = store_db.get_engine(resolved.url(), readonly=True)
        keys = set(store_db._ENGINES)
        seed(resolved, "alpha", 1)
        read_trust(resolved)
        assert set(store_db._ENGINES) == keys
        assert store_db.get_engine(resolved.url(), readonly=True) is engine


# ===========================================================================
# Races
# ===========================================================================


def _run_op(db_path, op, payload):
    """One racing operation, as a plain picklable value. Module level so that spawn and
    forkserver children can import it by reference."""
    resolved = resolve_config_path(db_path)
    try:
        if op == "enroll":
            outcome = enroll_client(
                resolved, secret_sha256=SECRET, server_fingerprint=SERVER,
                client_id=payload["client_id"], pin=newpin(payload["n"]), now=NOW + 10,
                dataset_exists=False)
            return outcome.value
        if op == "ensure":
            return ensure_client_id(resolved, lock=None, lock_timeout=60)
        if op == "retire":
            return retire_predecessor(resolved, fp(payload["n"]))
        if op == "rotate":
            return rotate_client(
                resolved, old_fp=fp(payload["old"]), new_pin=newpin(payload["new"]),
                now=NOW + 50).value
        if op == "revoke":
            try:
                result = trust.revoke_pin(resolved, fp(payload["n"]), lock=None, now=NOW + 60)
            except LookupError as exc:
                return ("lookup_error", str(exc))
            return (type(result).__name__, [p.fingerprint for p in result.cascaded])
        raise AssertionError(op)
    finally:
        dispose_all()


def _race_child(db_path, op, payload, barrier, out):
    try:
        barrier.wait(60)
        out.put(("ok", _run_op(db_path, op, payload)))
    except BaseException as exc:  # noqa: BLE001 - reported, never swallowed
        out.put(("error", repr(exc)))


def run_process_race(method, db_path, op, payloads):
    """One process per payload, released together. Every child is joined, and killed if it does
    not finish. Returns (results, exit codes)."""
    dispose_all()
    mp = multiprocessing.get_context(method)
    barrier = mp.Barrier(len(payloads))
    out = mp.Queue()
    ops = [op] * len(payloads) if isinstance(op, str) else list(op)     # one op, or one each
    procs = [mp.Process(target=_race_child, args=(str(db_path), one_op, payload, barrier, out),
                        daemon=True) for one_op, payload in zip(ops, payloads)]
    results = []
    try:
        for proc in procs:
            proc.start()
        for _ in procs:
            results.append(out.get(timeout=120))
        for proc in procs:
            proc.join(timeout=60)
    finally:
        for proc in procs:
            if proc.is_alive():
                proc.kill()
            proc.join(timeout=10)
        out.close()
        out.join_thread()
    return results, [proc.exitcode for proc in procs]


@pytest.mark.integration
class TestRaces:
    # --- enrollment: exactly one winner ------------------------------------------------------

    def _assert_one_winner(self, resolved, outcomes, count):
        assert sorted(outcomes) == sorted(["enrolled"] + ["refused"] * (count - 1)), outcomes
        assert len(read_trust(resolved).pins) == 1
        assert tgen(resolved) == 1
        status = token_status(resolved, NOW + 11)
        assert status.state is TokenState.CONSUMED
        (pin,) = read_trust(resolved).pins
        assert status.consumed_fingerprint == pin.fingerprint
        assert status.consumed_client_id == pin.client_id
        assert [c.client_id for c in list_clients(resolved)] == [pin.client_id]

    def test_threads_racing_one_token_produce_exactly_one_client(self, resolved):
        mktoken(resolved)
        outcomes = run_threads(8, lambda i: enroll(resolved, 100 + i, f"c{i}").value)
        self._assert_one_winner(resolved, outcomes, 8)

    def test_threads_racing_one_token_for_the_same_id_produce_one_pin(self, resolved):
        mktoken(resolved)
        outcomes = run_threads(8, lambda i: enroll(resolved, 100 + i, "alpha").value)
        self._assert_one_winner(resolved, outcomes, 8)

    def test_threads_racing_a_scoped_replace_token_leave_exactly_one_replacement(self, resolved):
        seed(resolved, "alpha", 1, 2)
        mktoken(resolved, client_id="alpha", replace=True)
        outcomes = run_threads(8, lambda i: enroll(resolved, 100 + i, "alpha").value)
        assert sorted(outcomes) == sorted(["enrolled"] + ["refused"] * 7)
        (pin,) = read_trust(resolved).pins
        assert pin.fingerprint in {fp(100 + i) for i in range(8)}

    @pytest.mark.parametrize("method", START_METHODS)
    def test_processes_racing_one_token_produce_exactly_one_client(
        self, resolved, db_path, method
    ):
        mktoken(resolved)
        payloads = [{"n": 100 + i, "client_id": f"c{i}"} for i in range(4)]
        results, exit_codes = run_process_race(method, db_path, "enroll", payloads)
        assert exit_codes == [0] * 4
        assert [r[0] for r in results] == ["ok"] * 4, results
        self._assert_one_winner(resolved, [r[1] for r in results], 4)

    # --- ensure_client_id converges -----------------------------------------------------------

    def test_threads_calling_ensure_client_id_converge_on_one_id(self, resolved):
        generation = cgen(resolved)
        results = run_threads(8, lambda i: ensure_client_id(resolved, lock=None, lock_timeout=60))
        assert all(isinstance(r, tuple) for r in results), results
        assert len({r[0] for r in results}) == 1
        assert sum(1 for r in results if r[1]) == 1
        assert read_client_id(resolved) == results[0][0]
        assert cgen(resolved) == generation + 1

    @pytest.mark.parametrize("method", START_METHODS)
    def test_processes_calling_ensure_client_id_converge_on_one_id(
        self, resolved, db_path, method
    ):
        generation = cgen(resolved)
        results, exit_codes = run_process_race(method, db_path, "ensure", [{}] * 4)
        assert exit_codes == [0] * 4
        assert [r[0] for r in results] == ["ok"] * 4, results
        values = [r[1] for r in results]
        assert len({v[0] for v in values}) == 1
        assert sum(1 for v in values if v[1]) == 1
        assert read_client_id(resolved) == values[0][0]
        assert cgen(resolved) == generation + 1

    # --- retire_predecessor: a retire racing another retire is safe ---------------------------

    def _rotated(self, resolved):
        seed(resolved, "alpha", 1)
        assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED
        return tgen(resolved)

    def test_threads_racing_a_retire_retire_once(self, resolved):
        generation = self._rotated(resolved)
        results = run_threads(8, lambda i: retire_predecessor(resolved, fp(2)))
        assert sorted(results) == [False] * 7 + [True], results
        assert list(pin_map(resolved)) == [fp(2)]
        assert pin_map(resolved)[fp(2)].predecessor is None
        assert tgen(resolved) == generation + 1

    @pytest.mark.parametrize("method", START_METHODS)
    def test_processes_racing_a_retire_retire_once(self, resolved, db_path, method):
        generation = self._rotated(resolved)
        results, exit_codes = run_process_race(method, db_path, "retire", [{"n": 2}] * 3)
        assert exit_codes == [0] * 3
        assert [r[0] for r in results] == ["ok"] * 3, results
        assert sorted(r[1] for r in results) == [False, False, True]
        assert list(pin_map(resolved)) == [fp(2)]
        assert tgen(resolved) == generation + 1

    def test_a_retire_racing_a_revoke_leaves_a_consistent_store(self, resolved, db_path):
        """Two valid serialisations, ONE end state: nothing pinned. Revoke first: A is
        `revoked`, the cascade takes the successor (`successor_revoked`) and the retire finds
        no pin. Retire first: A is `retired` (with `superseded_by`), and the revoke of the
        retired key follows that link to the successor (`successor_revoked`) and returns a
        `SupersededRevocation`."""
        self._rotated(resolved)
        results = run_threads(2, lambda i: (
            retire_predecessor(resolved, fp(2)) if i == 0
            else revoke_pin(resolved, fp(1), lock=None)))
        assert not any(isinstance(r, BaseException) for r in results), results
        assert pin_map(resolved) == {}
        reasons = {fingerprint: row[2] for fingerprint, row in revoked_rows(db_path).items()}
        if reasons[fp(1)] == "retired":                       # retire won
            assert reasons == {fp(1): "retired", fp(2): "successor_revoked"}
            assert results[0] is True
            assert isinstance(results[1], SupersededRevocation) and results[1].already_retired
            assert [p.fingerprint for p in results[1].cascaded] == [fp(2)]
        else:                                                 # revoke won
            assert reasons == {fp(1): "revoked", fp(2): "successor_revoked"}
            assert results[0] is False
            assert isinstance(results[1], PinRevocation) and not results[1].already_retired

    # --- rotate C -> D racing revoke A: nothing that descends from A may stay live ------------

    def _a_to_c(self, resolved):
        seed(resolved, "alpha", 1)
        assert rotate(resolved, 1, 2) is RotateOutcome.ROTATED

    def _assert_nothing_descending_from_a_is_live(self, resolved, db_path, rotate_result, revoke_result):
        """Either order ends with no pin at all. Rotate first: A is `retired` (`superseded_by`
        C) and the revoke follows the link to C and D. Revoke first: A is `revoked`, C is
        `successor_revoked` and the rotate finds C unpinned."""
        assert pin_map(resolved) == {}
        links = revoked_links(db_path)
        if rotate_result == "rotated":
            assert links == {fp(1): ("retired", fp(2)), fp(2): ("successor_revoked", None),
                             fp(3): ("successor_revoked", None)}
            assert revoke_result[0] == "SupersededRevocation"
            assert revoke_result[1] == [fp(2), fp(3)]
        else:
            assert rotate_result == "refused"
            assert links == {fp(1): ("revoked", None), fp(2): ("successor_revoked", None)}
            assert revoke_result == ("PinRevocation", [fp(2)])
        # and nothing can be pinned back through any of them
        mktoken(resolved, client_id="alpha")
        for n in (1, 2, 3):
            if fp(n) in links:                      # D was never pinned when the rotate lost
                assert enroll(resolved, n, "alpha") is EnrollOutcome.REFUSED
        assert rotate(resolved, 2, 4) is RotateOutcome.REFUSED

    def test_threads_a_rotate_racing_a_revoke_of_the_predecessor(self, resolved, db_path):
        self._a_to_c(resolved)
        results = run_threads(2, lambda i: (
            rotate(resolved, 2, 3, now=NOW + 50) if i == 0
            else revoke_pin(resolved, fp(1), lock=None, now=NOW + 60)))
        assert not any(isinstance(r, BaseException) for r in results), results
        self._assert_nothing_descending_from_a_is_live(
            resolved, db_path, results[0].value,
            (type(results[1]).__name__, [p.fingerprint for p in results[1].cascaded]))

    @pytest.mark.parametrize("method", START_METHODS)
    def test_processes_a_rotate_racing_a_revoke_of_the_predecessor(
        self, resolved, db_path, method
    ):
        self._a_to_c(resolved)
        results, exit_codes = run_process_race(
            method, db_path, ["rotate", "revoke"], [{"old": 2, "new": 3}, {"n": 1}])
        assert exit_codes == [0, 0]
        assert sorted(r[0] for r in results) == ["ok", "ok"], results
        by_op = {("rotate" if isinstance(r[1], str) else "revoke"): r[1] for r in results}
        self._assert_nothing_descending_from_a_is_live(
            resolved, db_path, by_op["rotate"], by_op["revoke"])

    def test_a_rotate_racing_a_revoke_client_never_pins_a_revoked_client(self, resolved):
        seed(resolved, "alpha", 1)
        results = run_threads(2, lambda i: (
            rotate(resolved, 1, 2) if i == 0 else revoke_client(resolved, "alpha", now=NOW + 1)))
        assert not any(isinstance(r, BaseException) for r in results), results
        assert read_trust(resolved).pins == ()
        assert list_clients(resolved)[0].revoked

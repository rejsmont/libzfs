"""SQLite-backed config store for zfsbackup.

This package is a **separate persistence layer** from the runtime config
dataclasses in `zfsbackup.config` (`BackupConfig`, `DatasetConfig`,
`RetentionRule`, ...). It does not replace them, and nothing in `zfsbackup/`
imports the ORM classes defined in here.

Why a separate layer rather than making the dataclasses declarative models
(see docs/config_db_cli_plan.md, "Recommended architecture"):

1. `DatasetConfig.from_property()` builds instances from *remote server
   data* with no DB behind them at all. If `DatasetConfig` were itself a
   declarative model, the same class would be simultaneously persistent,
   transient, and detached depending on where an instance came from.
2. Config objects outlive any plausible DB session -- held for the life of
   `BackupDaemon`, `DatasetManager`, `RemoteBackupManager`, and
   `create_app()`. A detached ORM instance would raise
   `DetachedInstanceError` lazily, deep inside a worker loop, the first time
   a relationship attribute is touched after the session that loaded it
   closed.
3. Several existing tests construct `BackupConfig(...)` directly with no DB
   at all; keeping the dataclasses pure keeps all of them working unchanged.

`zfsbackup/config/store/models.py` defines the ORM schema (`Base` plus one class
per table). `zfsbackup/config/store/mapper.py` converts between ORM rows and the
dataclasses: `validate_config(config) -> list[str]` is the pure, session-free
write-side check (warnings returned, `ValueError` raised) that `save_config`
runs first and the importer runs before it touches the target;
`load_config(session) -> BackupConfig` builds a fully detached
`BackupConfig` (replicating `BackupConfig.from_file`'s defaulting exactly,
except `prune_interval`'s fallback to `check_interval` --
`GlobalSettings.prune_interval_seconds` is nullable precisely so "derive this
elsewhere" is representable as `NULL` rather than flattened into a stored
value by whichever process writes it, and `load_config` passes a `NULL` row
through as `None` rather than resolving it; the client id is not a
`BackupConfig` field at all, see below), and `save_config(session, config)` does
a full wipe-and-reinsert of the store from a `BackupConfig` (no
diff-and-merge, no internal `commit()`/`begin()` -- the caller owns the
transaction).

`zfsbackup/config/store/migrate.py` drives Alembic: `ensure_schema(connection)`
upgrades an already-open `Connection` to this package's migration head (see
that module for the forward-compatibility guard it applies first, and for a
binding constraint on the caller's transaction handling) and is
deliberately not named `db.py`: that filename belongs to engine/session
setup, which has now landed as `zfsbackup/config/store/db.py` (item 5).
`ensure_schema` itself still creates no `Engine` or `Connection` -- it takes
one, never a URL.

`zfsbackup/config/store/db.py` is the ONLY module in this package that may create
an `Engine`. It builds them lazily, per-process, from a pid-keyed cache
(`get_engine`), discarding and rebuilding with `dispose(close=False)` any
engine inherited across a `multiprocessing` fork -- an inherited SQLite file
descriptor corrupts the database silently rather than raising. It also owns
the `connect`-event pragmas (WAL, `synchronous`, `busy_timeout`,
`foreign_keys`, and `query_only` for the read-only worker engines) and the
context-managed session factories `session_scope` / `session_for_engine`;
no public function there hands out a bare long-lived `Session`, because a
session checked out before a fork is invisible to the cache. Nothing is
constructed at import time -- importing this package creates no engine, no
connection, and no file. One import-time side effect does exist and is
deliberate: `db.py` calls `os.register_at_fork(after_in_child=...)` to
replace its cache lock in a forked child (a fork taken while another thread
held that lock would leave the child deadlocked), and `_liveconn.py` does the
same for its raw-fd guard (fresh lock, inherited parked header fds
closed). Both are process-global and cannot be
unregistered; neither touches an engine or a connection.

`zfsbackup/config/store/migrations/env.py`'s `run_migrations_online()` does have an
`engine_from_config` fallback (for the bare `alembic` CLI, used by
developers running migrations by hand outside any zfsbackup process), but
that branch is unreachable from `ensure_schema`, which always injects a
`Connection` -- `run_migrations_online()` returns before ever reaching it.
That fallback engine is also function-scoped (`NullPool`, opened and
`dispose()`d within the one call, never module-level state), so even a
developer running the bare CLI cannot leak it across a fork. **The eventual
URL-taking wrapper around `ensure_schema` (item 8) must inject a
`Connection` opened from `db.py`'s pid-keyed cached engine -- with
`foreign_keys=False`, since SQLite batch migrations must run FK-off -- and
not a URL** -- setting `sqlalchemy.url` or `ZFSBACKUP_DB_URL` instead would
let `env.py` build its own second, uncached `NullPool` engine that bypasses
every `connect`-event pragma `db.py` installs (`foreign_keys`, WAL,
`busy_timeout`) silently, with no error, and would also reintroduce a second
engine that is not fork-safe by construction. That wrapper must also run as
a **strictly sequential phase before any writer session is opened** --
`ensure_schema`, `commit()`, close, and only then `session_scope(url)` for
`save_config` -- because the FK-off engine is a second *writer* engine on
the same file and `db.py` takes SQLite's write lock at the start of every
writer transaction. Nesting the two self-deadlocks even in a single thread
(measured: `database is locked` after 5.2s, on a fresh database,
deterministically). That wrapper now exists as `importer.py`'s
`import_yaml`. The daemon's supervisor reads this package exactly once, at
startup, through `zfsbackup/runtime_config.py` -- a thin wrapper over
`access.read_config` (read-only). Worker processes never open the database
for configuration: they are handed the already-loaded config (and the client
id). The one exception is the API worker's access to the TRUST tables, which
goes only through `trust.py` on per-call engines (see below).

`mapper.py` is a new, deliberately one-directional coupling: it imports
`zfsbackup.config`, so `store` now depends on `config`, but `config/model.py`
still imports nothing from `store`. Do not let that direction reverse.

`Base.metadata` carries an explicit `naming_convention` (see
`models.NAMING_CONVENTION`), landed ahead of item 7's initial Alembic
revision so every constraint got a deterministic, convention-derived name
from the start. `zfsbackup/config/store/migrations/env.py`'s `target_metadata =
Base.metadata` picks it up automatically; the item-7 initial revision
(`zfsbackup/config/store/migrations/versions/`) was generated by autogenerate
against that metadata and reproduces every name the convention derives --
verified directly, not merely intended. Every constraint this package's
tables gain from now on should still let the convention name it rather
than hand-picking names for primary keys, foreign keys, or plain
`unique=True` columns, so future autogenerated revisions keep matching.

Per-destination retention overrides (`RetentionRule`) are scoped by
`dataset_remote_id`, an FK to `DatasetRemote`, not by a `destination_name`
column on `RetentionRule` itself -- see `RetentionRule`'s docstring in
`models.py` for why a direct `Destination` reference was rejected.

`zfsbackup/config/store/paths.py` (item 6a) owns config-database path
resolution (`resolve_config_path`/`resolve_config_url`: explicit `-c` >
`ZFSBACKUP_CONFIG` > `/var/lib/zfsbackup/config.db`), the pre-engine
existence/permission preflight (`check_config_db`,
`diagnose_open_failure`) that a `readonly=True` `db.py` engine cannot
itself provide (it would silently create a zero-byte file at a missing
path instead of refusing), and the creation-policy constants and helpers
(`ensure_config_dir`, `create_config_db_file`, `ensure_config_db_mode`)
that item 10's `import` must call, in that order, before opening the
database's first connection. Its only sanctioned ways to reach a live
`Session`/`Connection` -- `open_config_session`/`open_config_connection`
-- are context managers that preflight first and never hand back a bare,
storable `Engine`: `db.py`'s pid-keyed fork-safety cache depends on
nothing capturing a long-lived handle outside a `with` block, and an
`Engine` returned by value from this module would be exactly such a
handle. Only the daemon supervisor reads the configuration: at startup, on
every 10 s generation poll (`access.read_store_state`) and on every reload,
each through `zfsbackup/runtime_config.py` -> `access.read_config` (which
uses these two functions) and disposing its engines after each read;
`workers.py` never opens it for configuration (the API worker reaches the
trust tables only through `trust.py`, with `ephemeral=True`). Neither module
builds an engine, opens a connection, or touches an ORM class directly;
`BackupConfig.from_file` is gone from both.

`zfsbackup/config/store/importer.py` (item 8, phase A / 8b) is this
package's one caller of `paths.py`'s `allow_new` escape hatch, and the
only place in the codebase that drives `ensure_schema` and `save_config`
back to back in the mandated sequence: `import_yaml(yaml_path, resolved)`
parses a YAML config (`BackupConfig.from_file`, never `from_dict`/
`from_property` -- see `mapper.py`'s module docstring), then migrates the
target database to head over its own connection, commits, closes it, and
only then opens a fresh writer session to replace the store's contents
wholesale via `save_config`. `zfsbackup-config import` (`zfsbackup/cli/`)
calls `import_yaml` directly; the module has no command line of its own.

`migrate.py`'s `upgrade_schema(resolved) -> SchemaUpgrade` (item 10.1) is the
in-place, atomic, foreign-key-verified migration `zfsbackup-config migrate`
calls; the daemon only ever uses `check_schema`, which decides from
`EXPECTED_HEAD`/`KNOWN_REVISIONS` and reads no migration scripts.

`access.py` (item 15.1) is the whole-configuration entry point for every tool:
`read_config(resolved) -> ConfigSnapshot(config, generation, schema_rev,
store_id, client_id)` reads
the schema check, the rows and the generation in ONE read snapshot (the daemon
and the CLI's `export` / `migrate --check` / the read half of every edit), and
`write_config(resolved, config, *, expected_generation) -> WriteResult` is the
single compare-and-swap write: preflight, `validate_config`, the writer lock, a
`BEGIN IMMEDIATE` session, the schema check again, the generation compare, then
`save_config`. `ConfigSnapshot.client_id` is this host's client id
(`host_identity`; `None` until one is generated), read in that same snapshot;
it is not part of `BackupConfig`, and `save_config` / `write_config` /
`import_yaml` wipe only the six config tables (`mapper.CONFIG_TABLES`) and
leave `host_identity` and the trust tables untouched. A stale generation is
`ConfigConflict`; a lock that stays held
(the `<db>.writer.lock` flock, or SQLite's `busy_timeout`) is `ConfigBusy`.
Both live in `errors.py`, deliberately outside `SchemaError`/`ConfigPathError`
(retry, not "unusable database"). `messages.py` holds the operator wording
shared by the daemon and the CLI (`config_error_message`,
`describe_config_source`). `paths.writer_lock` (item 15.4) serialises every
zfsbackup-config (CLI) writer, `import_yaml` included -- NOT the API worker's
trust writers, see below; it adds one more file next to
the database (`<db>.writer.lock`, besides WAL's `-wal`/`-shm`) that is never
deleted and should not be archived as state.

`trust.py` (TLS/mTLS plan M7) is the only code that touches the six
non-config tables (`mapper.PRESERVED_TABLES`), apart from
`load_config_snapshot` reading `host_identity.client_id` into the config
snapshot. **Who writes what:** the CLI is the only writer of configuration
**and of `host_identity`** (`ensure_client_id` / `set_client_id`, under the
writer lock; any change to the stored id bumps `GlobalSettings.generation` in
the same transaction, so the daemon reloads). **The trust tables have exactly
two writers, both going through `store.trust`:** the CLI (`create_token`,
`revoke_token`, `revoke_pin`, `revoke_client`, under the writer lock) and the
API worker (`enroll_client`, `rotate_client`, `retire_predecessor`, no flock:
they serialise against the CLI on SQLite's write lock alone, and against each
other on an in-process lock in `trust.py`; each first decides read-only, so a
refused or no-op call -- every unauthenticated wrong `/enroll` -- never takes
the write lock). A request thread must never call one of them while it holds
any other writer session or connection (`db.session_for_engine`: at most one
writer connection per file per process). Trust writes bump only
`trust_state.generation`, and only when the set of pins changes (or an
enrollment returns `ENROLLED`); they never touch the config generation and
never cause a reload. **Revocation is permanent:** every path that unpins a
certificate (`revoke_pin`, which also unpins the rotation successors the revoked
key authorised and returns them as `PinRevocation.cascaded` -- for a key already
retired by rotation it follows `RevokedPin.superseded_by` to the live successor
and returns a `SupersededRevocation`; `revoke_client`, a
`--replace` enrollment, and `retire_predecessor`) records its fingerprint in `revoked_fingerprints` in
the same transaction, `enroll_client` / `rotate_client` refuse a recorded
fingerprint (`REFUSED`, token untouched, decided read-only where possible),
and `read_trust` never returns one. Every `trust.py` call
runs on its own fresh, unpooled engine (`db.ephemeral_engine`) that is
disposed before it returns: nothing is cached or shared between threads, so
the API worker may call it from any request thread, and the supervisor's
`dispose_all()` never interacts with it.

Public API
----------
Tool-level surface -- what the daemon and `zfsbackup-config` are meant to call:

- trust and host identity (`trust.py`): `read_client_id`, `ensure_client_id`,
  `set_client_id` / `ClientIdUpdate`; `read_trust_key` / `TrustKey`,
  `read_trust` / `TrustSnapshot` / `TrustedPin`, `list_clients` /
  `ClientRecord`, `list_revoked` / `RevokedPin`, `token_status` /
  `TokenStatus` / `TokenState`;
  `token_precheck` (read-only enrollment filter: the API calls it first in
  `/enroll`, before any `zfs` call; `enroll_client` re-checks everything);
  `create_token`, `revoke_token`, `revoke_pin` / `PinRevocation` /
  `SupersededRevocation`, `revoke_client`;
  `enroll_client` / `EnrollOutcome` / `NewPin`, `rotate_client` /
  `RotateOutcome`, `retire_predecessor`; `TOKEN_MAX_TTL_SECONDS`;

- whole-configuration access (`access.py`): `read_config`, `write_config`,
  `read_generation`, `read_store_state` / `StoreState` (the poll's cheap
  `(store_id, generation)` read), `check_config_schema`, `WriteResult`,
  `ConfigSnapshot`;
  `import_yaml` (`importer.py`) and `upgrade_schema` / `check_schema`
  (`migrate.py`) for creating and migrating a database;
- paths and the writer lock (`paths.py`): `resolve_config_path`,
  `ResolvedConfigPath`, `writer_lock` (to hold the lock across a multi-step
  edit, passing it on as `write_config(..., lock=held)`), the `ConfigPath*` /
  `ConfigDb*` errors;
- errors: `ConfigConflict`, `StoreIdConflict` (a `ConfigConflict` subclass:
  the database file was replaced; `expected_store_id` / `actual_store_id`,
  with `expected` / `actual` still the generations), `ConfigBusy`, `SchemaError` and subclasses;
- messages: `config_error_message`, `describe_config_source`;
- pure checks on dataclasses: `validate_config`, `destination_name_problem`.

Internal -- not for tools, even though they are importable: the ORM classes
(`Base`, `Dataset`, `Destination`, ...), the engine and session factories
(`get_engine`, `make_engine`, `session_scope`, `session_for_engine`,
`dispose_all` outside a fork point), and the session-level `save_config`,
`load_config`, `load_config_snapshot` and `ensure_schema`. The session-level
functions bypass the writer lock, the schema re-check and (for
`save_config` without `expected_generation`) the compare-and-swap; only
`write_config` and `import_yaml` compose them safely.
"""

from zfsbackup.config.store.models import (
    Base,
    Dataset,
    DatasetRemote,
    Destination,
    GlobalSettings,
    NAMING_CONVENTION,
    RemoteServer,
    RetentionRule,
    RevokedFingerprint,
)
from zfsbackup.config.store.db import (
    ReadOnlySessionError,
    dispose_all,
    get_engine,
    make_engine,
    session_for_engine,
    session_scope,
    url_for_path,
)
from zfsbackup.config.store.errors import ConfigBusy, ConfigConflict
from zfsbackup.config.store.mapper import (
    ConfigSnapshot,
    destination_name_problem,
    load_config,
    StoreIdConflict,
    load_config_snapshot,
    save_config,
    validate_config,
)
from zfsbackup.config.store.migrate import (
    EXPECTED_HEAD,
    KNOWN_REVISIONS,
    MigrationPackagingError,
    NotAConfigDatabase,
    SchemaColumnsMissing,
    SchemaError,
    SchemaIntegrityError,
    SchemaOutOfDate,
    SchemaSplitBrain,
    SchemaUpgrade,
    SchemaVersionMismatch,
    check_schema,
    ensure_schema,
    upgrade_schema,
)
from zfsbackup.config.store.paths import (
    CONFIG_DB_MODE,
    CONFIG_DIR_MODE,
    CONFIG_GROUP,
    CONFIG_OWNER,
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    DEFAULT_WRITER_LOCK_TIMEOUT,
    WRITER_LOCK_SUFFIX,
    ConfigDbCreatedConcurrently,
    ConfigDbIsYaml,
    ConfigDbNotADatabase,
    ConfigDbNotAFile,
    ConfigDbNotFound,
    ConfigDbOrphanedSidecars,
    ConfigDbPathUnrepresentable,
    ConfigDbPermissionError,
    ConfigPathEnvError,
    ConfigPathError,
    ResolvedConfigPath,
    check_config_db,
    check_config_db_suffix,
    create_config_db_file,
    diagnose_open_failure,
    ensure_config_db_mode,
    ensure_config_dir,
    open_config_connection,
    open_config_session,
    require_writer_lock_held,
    resolve_config_path,
    resolve_config_url,
    writer_lock,
    writer_lock_path,
)
from zfsbackup.config.store.messages import config_error_message, describe_config_source
# Imported last: depends on mapper, migrate, AND paths all being importable
# (it uses save_config, ensure_schema/SchemaError, and the paths helpers
# together to drive the three-phase import in importer.py's own docstring).
from zfsbackup.config.store.importer import (
    ImportResult,
    OutgoingConfigUnreadable,
    import_yaml,
)
from zfsbackup.config.store.access import (
    StoreState,
    WriteResult,
    check_config_schema,
    read_config,
    read_generation,
    read_store_state,
    write_config,
)
from zfsbackup.config.store.backup import BackupError, BackupInfo, backup_database
from zfsbackup.config.store.trust import (
    TOKEN_MAX_TTL_SECONDS,
    ClientIdUpdate,
    ClientRecord,
    EnrollOutcome,
    NewPin,
    PinRevocation,
    RevokedPin,
    RotateOutcome,
    SupersededRevocation,
    TokenState,
    TokenStatus,
    TrustKey,
    TrustSnapshot,
    TrustedPin,
    create_token,
    enroll_client,
    ensure_client_id,
    list_clients,
    list_revoked,
    read_client_id,
    read_trust,
    read_trust_key,
    retire_predecessor,
    revoke_client,
    revoke_pin,
    revoke_token,
    rotate_client,
    set_client_id,
    token_precheck,
    token_status,
)

__all__ = [
    "backup_database",
    "BackupError",
    "BackupInfo",
    "Base",
    "check_config_db",
    "check_config_db_suffix",
    "check_config_schema",
    "check_schema",
    "ClientIdUpdate",
    "ClientRecord",
    "CONFIG_DB_MODE",
    "CONFIG_DIR_MODE",
    "config_error_message",
    "CONFIG_GROUP",
    "CONFIG_OWNER",
    "CONFIG_PATH_ENV",
    "ConfigBusy",
    "ConfigConflict",
    "ConfigDbCreatedConcurrently",
    "ConfigDbIsYaml",
    "ConfigDbNotADatabase",
    "ConfigDbNotAFile",
    "ConfigDbNotFound",
    "ConfigDbOrphanedSidecars",
    "ConfigDbPathUnrepresentable",
    "ConfigDbPermissionError",
    "ConfigPathEnvError",
    "ConfigPathError",
    "ConfigSnapshot",
    "create_config_db_file",
    "create_token",
    "Dataset",
    "DatasetRemote",
    "DEFAULT_CONFIG_DB",
    "DEFAULT_WRITER_LOCK_TIMEOUT",
    "describe_config_source",
    "Destination",
    "destination_name_problem",
    "diagnose_open_failure",
    "dispose_all",
    "enroll_client",
    "EnrollOutcome",
    "ensure_client_id",
    "ensure_config_db_mode",
    "ensure_config_dir",
    "ensure_schema",
    "EXPECTED_HEAD",
    "get_engine",
    "GlobalSettings",
    "import_yaml",
    "ImportResult",
    "KNOWN_REVISIONS",
    "list_clients",
    "list_revoked",
    "load_config",
    "load_config_snapshot",
    "make_engine",
    "MigrationPackagingError",
    "NAMING_CONVENTION",
    "NewPin",
    "PinRevocation",
    "SupersededRevocation",
    "NotAConfigDatabase",
    "open_config_connection",
    "open_config_session",
    "OutgoingConfigUnreadable",
    "read_client_id",
    "read_config",
    "read_generation",
    "read_store_state",
    "read_trust",
    "read_trust_key",
    "ReadOnlySessionError",
    "RemoteServer",
    "require_writer_lock_held",
    "resolve_config_path",
    "resolve_config_url",
    "ResolvedConfigPath",
    "RetentionRule",
    "retire_predecessor",
    "RevokedFingerprint",
    "RevokedPin",
    "revoke_client",
    "revoke_pin",
    "revoke_token",
    "rotate_client",
    "RotateOutcome",
    "save_config",
    "SchemaColumnsMissing",
    "SchemaError",
    "SchemaIntegrityError",
    "SchemaOutOfDate",
    "SchemaSplitBrain",
    "SchemaUpgrade",
    "SchemaVersionMismatch",
    "session_for_engine",
    "session_scope",
    "set_client_id",
    "StoreIdConflict",
    "StoreState",
    "TOKEN_MAX_TTL_SECONDS",
    "token_precheck",
    "token_status",
    "TokenState",
    "TokenStatus",
    "TrustedPin",
    "TrustKey",
    "TrustSnapshot",
    "upgrade_schema",
    "url_for_path",
    "validate_config",
    "write_config",
    "writer_lock",
    "writer_lock_path",
    "WRITER_LOCK_SUFFIX",
    "WriteResult",
]

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
dataclasses: `load_config(session) -> BackupConfig` builds a fully detached
`BackupConfig` (replicating `BackupConfig.from_file`'s defaulting exactly,
except `prune_interval`'s fallback to `check_interval` and the
`client_id_file` default -- `GlobalSettings.prune_interval_seconds` and
`.client_id_file` are nullable precisely so "derive this elsewhere" is
representable as `NULL` rather than flattened into a stored value by
whichever process writes it, and `load_config` passes a `NULL` row through
as `None` rather than resolving it), and `save_config(session, config)` does
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
held that lock would leave the child deadlocked). It is process-global and
cannot be unregistered; it touches no engine and no connection.

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
deterministically). This package is not yet wired
into the daemon, workers, or CLI -- no caller of `ensure_schema` exists yet.

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
handle. It has no caller in this repo yet; `daemon.py` still loads YAML,
and wiring it into the daemon and CLI is item 8's job.
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
from zfsbackup.config.store.mapper import load_config, save_config
from zfsbackup.config.store.migrate import (
    SchemaError,
    SchemaOutOfDate,
    SchemaSplitBrain,
    SchemaVersionMismatch,
    check_schema,
    ensure_schema,
)
from zfsbackup.config.store.paths import (
    CONFIG_DB_MODE,
    CONFIG_DIR_MODE,
    CONFIG_OWNER,
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    ConfigDbIsYaml,
    ConfigDbNotADatabase,
    ConfigDbNotAFile,
    ConfigDbNotFound,
    ConfigDbPathUnrepresentable,
    ConfigDbPermissionError,
    ConfigPathEnvError,
    ConfigPathError,
    ResolvedConfigPath,
    check_config_db,
    create_config_db_file,
    diagnose_open_failure,
    ensure_config_db_mode,
    ensure_config_dir,
    open_config_connection,
    open_config_session,
    resolve_config_path,
    resolve_config_url,
)

__all__ = [
    "Base",
    "CONFIG_DB_MODE",
    "CONFIG_DIR_MODE",
    "CONFIG_OWNER",
    "CONFIG_PATH_ENV",
    "ConfigDbIsYaml",
    "ConfigDbNotADatabase",
    "ConfigDbNotAFile",
    "ConfigDbNotFound",
    "ConfigDbPathUnrepresentable",
    "ConfigDbPermissionError",
    "ConfigPathEnvError",
    "ConfigPathError",
    "Dataset",
    "DatasetRemote",
    "DEFAULT_CONFIG_DB",
    "Destination",
    "GlobalSettings",
    "NAMING_CONVENTION",
    "ReadOnlySessionError",
    "RemoteServer",
    "ResolvedConfigPath",
    "RetentionRule",
    "SchemaError",
    "SchemaOutOfDate",
    "SchemaSplitBrain",
    "SchemaVersionMismatch",
    "check_config_db",
    "check_schema",
    "create_config_db_file",
    "diagnose_open_failure",
    "dispose_all",
    "ensure_config_db_mode",
    "ensure_config_dir",
    "ensure_schema",
    "get_engine",
    "load_config",
    "make_engine",
    "open_config_connection",
    "open_config_session",
    "resolve_config_path",
    "resolve_config_url",
    "save_config",
    "session_for_engine",
    "session_scope",
    "url_for_path",
]

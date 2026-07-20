"""SQLite-backed config store for zfsbackup.

This package is a **separate persistence layer** from the runtime config
dataclasses in `zfsbackup.config` (`BackupConfig`, `DatasetConfig`,
`RetentionRule`, ...). It does not replace them and nothing in `zfsbackup/`
imports the dataclasses in here or vice versa at this stage.

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

`zfsbackup/store/models.py` defines the ORM schema (`Base` plus one class
per table). A mapper module (`zfsbackup/store/mapper.py`, a later item)
converts between ORM rows and the dataclasses; engine/session setup
(`zfsbackup/store/db.py`) and Alembic migrations are later items too. This
package currently defines schema only -- nothing here is wired into the
daemon, workers, or CLI.

`Base.metadata` already carries an explicit `naming_convention` (see
`models.NAMING_CONVENTION`), deliberately landed ahead of item 7's initial
Alembic revision -- Alembic's `env.py` will pick it up automatically via
`target_metadata = Base.metadata`, so nothing further is needed here for
that, but it means every constraint this package's tables gain from now on
should let the convention name it rather than hand-picking names for
primary keys, foreign keys, or plain `unique=True` columns.

Per-destination retention overrides (`RetentionRule`) are scoped by
`dataset_remote_id`, an FK to `DatasetRemote`, not by a `destination_name`
column on `RetentionRule` itself -- see `RetentionRule`'s docstring in
`models.py` for why a direct `Destination` reference was rejected.
"""

from zfsbackup.store.models import (
    Base,
    Dataset,
    DatasetRemote,
    Destination,
    GlobalSettings,
    NAMING_CONVENTION,
    RemoteServer,
    RetentionRule,
)

__all__ = [
    "Base",
    "Dataset",
    "DatasetRemote",
    "Destination",
    "GlobalSettings",
    "NAMING_CONVENTION",
    "RemoteServer",
    "RetentionRule",
]

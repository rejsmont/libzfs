"""Configuration model for zfsbackup, and its persistence layer.

This package holds two distinct things:

- `zfsbackup.config.model` -- the runtime configuration *value types*
  (`BackupConfig`, `DatasetConfig`, `Duration`, ...). They are plain
  dataclasses with no persistence behaviour. `DatasetConfig` is also a
  remote wire format, reconstructed by `from_property()` from data that
  has no database behind it at all.
- `zfsbackup.config.store` -- the SQLite/SQLAlchemy persistence layer for
  those value types: ORM rows, the ORM<->dataclass mapper, engine and
  session management, and the Alembic migrations.

The store is a *separate layer* from the model rather than the same
classes made persistent, because config objects outlive any session and a
detached ORM instance raises `DetachedInstanceError` lazily, deep inside a
worker loop, long after the session that loaded it closed. See
`zfsbackup.config.store`'s docstring for the full reasoning.

Note the deliberate distinction between this package and `zfsbackup.config.store`
(reserved, not yet present), which is for *backup data* destinations --
object storage, cloud targets and the like. This package is about the
configuration itself, not about where backups are written.

Every public name of `model` is re-exported here, so
`from zfsbackup.config import BackupConfig` is the supported spelling and
importing from `zfsbackup.config.model` directly is not necessary.
"""

from zfsbackup.config.model import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
    RemoteServerConfig,
    RetentionRule,
    parse_time_duration,
    validate_retention_uniqueness,
    # Not public, but imported across module boundaries by backup_manager
    # and remote at prune time. Re-exported so the move of `config/model.py` into
    # this package did not silently change their import path.
    _collapse_retention_rules,
)

__all__ = [
    "BackupConfig",
    "DatasetConfig",
    "Destination",
    "Duration",
    "RemoteDatasetConfig",
    "RemoteServerConfig",
    "RetentionRule",
    "parse_time_duration",
    "validate_retention_uniqueness",
]

"""The daemon's thin wrapper around the config store's shared read path.

The reading itself lives in `zfsbackup.config.store` (item 15.1):
`read_config(resolved)` is one read-only connection and one read snapshot --
`BEGIN`, `check_schema`, `load_config_snapshot`, `COMMIT` -- so the schema
check and the rows are the same database state, and a concurrent
`zfsbackup-config import` is never read as a config assembled from two
generations. `describe_config_source` and the bulk of `config_error_message`
live there too, shared with the CLI. What stays here is what only the daemon
needs:

- `load_runtime_config`, the supervisor's one read of the config database,
  returning a `ConfigSnapshot` (config, generation, schema revision);
- `config_error_message`, the store's wording plus the daemon's own closing
  sentence for a stale schema;
- re-exports of the store's `check_config_schema` and
  `describe_config_source`, so there is one implementation of each.

Who may read the database
-------------------------
**The supervisor, once, before the first fork.** Workers never open the config
database (item 15.2(b), decision Q4): they are handed the supervisor's
detached `BackupConfig` -- by inheritance under `fork`, by pickling under
`spawn`/`forkserver` -- so every worker, and every crash-restarted
replacement, runs the same generation as the one the supervisor logged and
recorded in the presence lock. A crash restart reuses that object; it does
not re-read the database. Live reload is Phase 3.

The rules this wrapper still obeys
----------------------------------
1. **Read-only, always** (`read_config` opens `readonly=True`).
2. **The daemon never creates and never migrates.** `check_schema`, never
   `ensure_schema`; `zfsbackup-config` is the only writer. `SchemaOutOfDate`
   exists so a daemon facing a stale database refuses to start rather than
   reading old rows through current ORM models.
3. **Never catch bare `RuntimeError`.** `SchemaError` is a `RuntimeError`
   subclass, but the store also has a bare-`RuntimeError` family outside it
   (`MigrationPackagingError`, a packaging defect in the installed
   zfsbackup). An `except RuntimeError` would absorb a real bug into the
   "log an operator message and exit cleanly" path.
4. **No connection survives the load into a fork.** `read_config` closes its
   session, but the reader engine's pooled connection stays open; the
   supervisor calls `dispose_all()` before every fork (`BackupDaemon._spawn`)
   and right after this load.

Why this is not in `zfsbackup/config/model.py`: `store/mapper.py` imports
`zfsbackup.config`, so a store-aware `BackupConfig.from_db()` would invert that
one-directional dependency.
"""

from typing import Optional

from zfsbackup.config.store import (
    ConfigSnapshot,
    ResolvedConfigPath,
    SchemaOutOfDate,
    check_config_schema,
    describe_config_source,
    read_config,
)
from zfsbackup.config.store import config_error_message as _store_config_error_message

__all__ = [
    "check_config_schema",
    "config_error_message",
    "describe_config_source",
    "load_runtime_config",
]

_SCHEMA_OUT_OF_DATE_DAEMON_NOTE = (
    "The daemon only checks the schema and never migrates it; run the "
    "command above, then start the daemon again."
)


def load_runtime_config(resolved: ResolvedConfigPath) -> ConfigSnapshot:
    """Read the configuration the daemon will run, with the generation and
    schema revision it was read at -- all from one read snapshot.

    A straight delegation to the store's `read_config`: the schema check is
    inside the snapshot, nothing is created or migrated, and the returned
    `ConfigSnapshot.config` is a fully detached `BackupConfig` that owns no
    session or ORM state, so it is safe to hold for the life of the process
    and to hand to child processes.

    Called once, by the supervisor, before the first fork. Workers never call
    it. The pooled reader connection stays open afterwards: the caller must
    `dispose_all()` before forking.

    Raises `ConfigPathError`, `SchemaError`, `ValueError` (the store holds no
    usable configuration) or `sqlalchemy.exc.DatabaseError`;
    `config_error_message` turns any of them into an operator-grade line.
    """
    return read_config(resolved)


def config_error_message(
    resolved: Optional[ResolvedConfigPath], exc: Exception
) -> str:
    """The store's operator-facing message for `exc`, plus the daemon's own
    sentence after a `SchemaOutOfDate`.

    The store's text is neutral because the CLI shares it and it ends at the
    command to run. For a stale schema the daemon appends "never migrates;
    run the command above, then start the daemon again" -- appended, never
    prefixed, so the pasteable command stays where it was. Everything else is
    the store's wording verbatim.
    """
    message = _store_config_error_message(resolved, exc)
    if isinstance(exc, SchemaOutOfDate):
        return f"{message}\n{_SCHEMA_OUT_OF_DATE_DAEMON_NOTE}"
    return message

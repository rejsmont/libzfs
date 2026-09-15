"""Alembic environment for the zfsbackup config store.

Deviates from the stock `alembic init` template in the two ways the item-7
plan measured against it:

1. The stock template's `run_migrations_online()` always builds an `Engine`
   from `sqlalchemy.url` via `engine_from_config`, ignoring any connection
   the caller already has open. `ensure_schema()` (see
   `zfsbackup/store/migrate.py`) -- and every in-memory test -- inject a
   live `Connection` via `config.attributes["connection"]` instead, because
   the DB they are targeting (an in-memory `StaticPool` engine, or one
   opened by a caller that must not be handed a second, independent
   connection) is not reachable by URL alone. `run_migrations_online()`
   below honours that injected connection *before* falling back to
   building an engine from a resolved URL. Skipping this branch does not
   error -- it silently migrates whatever `sqlalchemy.url`/`-x db_url=`/
   `ZFSBACKUP_DB_URL` resolves to instead, which in practice means
   migrating the wrong database while the intended target stays untouched.
   The injected value is type-checked (`TypeError`, not silently ignored)
   rather than merely `isinstance`-tested and falling through -- a
   mistyped injection (an `Engine`, a `Session`, a test double) must not be
   able to fall through to the URL-based fallback and migrate whatever
   `ZFSBACKUP_DB_URL` happens to be set to in the caller's shell.

2. The stock template already guards `fileConfig(config.config_file_name)`
   with `if config.config_file_name is not None:` -- but its call, when it
   does fire, defaults to `disable_existing_loggers=True`. Since the
   generated `alembic.ini` only declares `root`, `sqlalchemy`, and
   `alembic` in `[loggers]`, every other already-configured logger (all of
   `zfsbackup.*`, including anything a caller's `caplog` fixture is
   listening to) gets disabled as a side effect of running a migration.
   `ensure_schema()` never has a `config_file_name` at all (see
   `migrate.py`'s `_build_config`, which never reads `alembic.ini`), so the
   pre-existing `config_file_name is not None` check alone already skips
   `fileConfig()` on that path -- that is the load-bearing half; do not
   remove it. `config.attributes.get("configure_logger", True)` is an
   additional, independent guard kept for a bare `alembic` CLI invocation
   that does have a config file (a future consumer might want to run
   migrations with an ini file present but application logging still
   protected); it plays no role in `ensure_schema()`'s own path.

Deliberately does NOT resolve any zfsbackup application path (no
`/var/lib/zfsbackup/config.db`, no `ZFSBACKUP_CONFIG`) -- that is item 6's
job, and its output must reach this file only as an already-open
`Connection`, never as a path this module goes looking for itself.
"""

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import Connection

from alembic import context

from zfsbackup.store.models import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging, unless the caller asked us
# not to (see module docstring, point 2) or there is no ini file at all
# (ensure_schema() builds a bare `Config()` with no config_file_name).
if config.config_file_name is not None and config.attributes.get(
    "configure_logger", True
):
    fileConfig(config.config_file_name)

# Picks up NAMING_CONVENTION (see models.py) automatically -- Base.metadata
# carries it, and autogenerate/compare_metadata consult target_metadata.
target_metadata = Base.metadata


def _resolve_url() -> str:
    """Resolve a database URL for a plain engine, when no connection was
    injected. Order: `-x db_url=...` > `ZFSBACKUP_DB_URL` env var >
    `sqlalchemy.url` in alembic.ini. Errors naming all three sources when
    none is set, so a bare `alembic` invocation with no target fails loudly
    rather than silently no-op-ing against an empty URL.
    """
    x_args = context.get_x_argument(as_dictionary=True)
    db_url = x_args.get("db_url")
    if db_url:
        return db_url

    env_url = os.environ.get("ZFSBACKUP_DB_URL")
    if env_url:
        return env_url

    ini_url = config.get_main_option("sqlalchemy.url")
    if ini_url:
        return ini_url

    raise RuntimeError(
        "No database URL configured for Alembic. Set one of: "
        "-x db_url=<url>, the ZFSBACKUP_DB_URL environment variable, or "
        "sqlalchemy.url in alembic.ini."
    )


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL, no live connection)."""
    context.configure(
        url=_resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    Honours an injected `Connection` (see module docstring, point 1) ahead
    of building an `Engine` from a resolved URL. When an engine is built
    here, it is scoped to this function -- opened, used, and disposed
    before returning -- never leaked into the caller or created ahead of a
    `multiprocessing` fork (item 5's concern; this module is never on that
    path, since `ensure_schema()` always injects a connection).

    Checks `is not None` rather than `isinstance(..., Connection)` and
    falling through on a mismatch: anything injected under the
    `"connection"` key is presumed intentional, so a wrong type (an
    `Engine`, a `Session`, a test double) must fail loudly with `TypeError`
    here rather than silently falling through to the URL-based fallback
    below and migrating a different database than the caller meant to
    target.
    """
    injected = config.attributes.get("connection")
    if injected is not None:
        if not isinstance(injected, Connection):
            raise TypeError(
                "config.attributes['connection'] must be a "
                f"sqlalchemy.engine.Connection, got {injected!r}"
            )
        context.configure(
            connection=injected,
            target_metadata=target_metadata,
            render_as_batch=True,
        )
        with context.begin_transaction():
            context.run_migrations()
        return

    connectable_config = config.get_section(config.config_ini_section, {})
    connectable_config["sqlalchemy.url"] = _resolve_url()
    connectable = engine_from_config(
        connectable_config,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    try:
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                render_as_batch=True,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

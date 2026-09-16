"""Loading the daemon's runtime configuration out of the SQLite config store.

This module is the daemon core's *only* door into `zfsbackup.config.store`.
`daemon.py` and `workers.py` call `load_runtime_config()`; neither of them
opens a connection, builds an engine, or touches an ORM class directly.

Why it lives here and not in `zfsbackup/config/model.py`
--------------------------------------------------------
`store/mapper.py` imports `zfsbackup.config` (it builds the dataclasses).
Putting a store-aware `BackupConfig.from_db()` on the model would invert
that one-directional dependency -- see `zfsbackup.config.store`'s docstring,
which states plainly that `config/model.py` must keep importing nothing from
`store`. A separate module, imported by `daemon.py` (which already imports
`workers.py`) and by `workers.py`, gives the same convenience with the
dependency arrow pointing the same way it did before.

The five constraints every function here obeys
----------------------------------------------
1. **Read-only, always.** `readonly=True` on every open. A writer session
   that only reads still takes SQLite's exclusive write lock at
   `BEGIN IMMEDIATE`, so two daemon processes reading through the writer
   form would serialise against each other (and against the CLI) for no
   reason at all.
2. **Sequential, never nested.** At most one connection to the config
   database is live in this process at any moment. `db.py` fires
   `BEGIN IMMEDIATE` at the first statement of a writer transaction and
   pins a WAL read snapshot for the reader form; a second handle opened
   inside the first self-deadlocks for `busy_timeout` and then fails with
   `database is locked` (measured at 5.2s in the store's own notes). This
   is why `check_config_schema`'s connection is fully closed before
   `load_runtime_config` opens its session, rather than the two being
   nested in one `with`.
3. **The daemon never creates and never migrates.** `check_schema`, never
   `ensure_schema`; the CLI (`zfsbackup-config`) is the only writer. That
   asymmetry is what the store's fork-safety and WAL design rest on, and
   `SchemaOutOfDate` exists precisely so a daemon facing a stale database
   refuses to start rather than reading old rows through current ORM
   models.
4. **`require_writable` stays `True`.** A `readonly=True` SQLAlchemy engine
   still opens the file read-write at the OS level and still needs a
   writable containing directory for the `-shm` sidecar, so the preflight
   must keep checking for that -- see `paths.open_config_session`. The same
   value is forwarded to `diagnose_open_failure`, so a post-hoc diagnosis
   re-runs exactly the check the open itself performed.
5. **Never catch bare `RuntimeError`.** `SchemaError` is a `RuntimeError`
   subclass, but `check_schema` also raises a *bare* `RuntimeError` for a
   multi-head migration history -- a packaging defect in the installed
   zfsbackup, deliberately left outside the `SchemaError` hierarchy. An
   `except RuntimeError` here would absorb a real bug into the
   "log an operator message and exit cleanly" path.
6. **One scoped read snapshot per load.** `load_config` is many statements,
   not one, and a reader engine begins no transaction of its own, so the
   whole of it runs inside an explicit `BEGIN`/`COMMIT` pair -- otherwise a
   concurrent `zfsbackup-config import` is read as a coherent config
   assembled from two different generations (measured: two datasets' whole
   retention policies swapped, silently). Scoped to the call, never to the
   connection's lifetime; see `load_runtime_config` for why that
   distinction is exactly what `db.py`'s reader rule is about.
"""

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    ConfigPathError,
    ResolvedConfigPath,
    SchemaError,
    check_schema,
    diagnose_open_failure,
    load_config,
    open_config_connection,
    open_config_session,
)

logger = logging.getLogger(__name__)

#: Exit code a worker child uses when the configuration database itself is
#: unusable (a stale schema, a missing or malformed file, a permission
#: problem) -- i.e. a failure that restarting cannot possibly fix. Chosen to
#: match BSD `sysexits.h`'s `EX_CONFIG`. `BackupDaemon._check_workers`
#: treats this one code as permanent and stops respawning that worker;
#: every other non-zero exit stays restartable, which is what keeps a
#: transient `database is locked` (an `OperationalError`, never a
#: `SchemaError`) from being mistaken for a permanent config fault.
EX_CONFIG = 78

__all__ = [
    "EX_CONFIG",
    "check_config_schema",
    "config_error_message",
    "describe_config_source",
    "load_runtime_config",
]

_SOURCE_DESCRIPTIONS = {
    "--config": "via -c/--config",
    CONFIG_PATH_ENV: f"via ${CONFIG_PATH_ENV}",
    "default": "default path",
}


def describe_config_source(resolved: ResolvedConfigPath) -> str:
    """One human-readable line naming the config database and *why* that
    file was chosen, e.g.::

        /var/lib/zfsbackup/config.db (via -c/--config)

    Leads with `ResolvedConfigPath.path` -- the path as the operator
    actually spelled it -- never `resolved_path` alone. The resolved form
    is appended only when it differs *and* the path came from `-c` or
    `$ZFSBACKUP_CONFIG`, where the operator supplied something that may be
    relative or a symlink and genuinely benefits from being told which
    file was opened.

    For `source="default"` the resolved form is deliberately suppressed,
    matching `paths._not_found_message`: the default is a compiled-in
    constant nobody typed, and on macOS resolving it rewrites
    `/var/lib/zfsbackup/config.db` to `/private/var/lib/zfsbackup/config.db`
    -- a path that differs across platforms for an input the operator
    never chose.
    """
    where = _SOURCE_DESCRIPTIONS.get(resolved.source, resolved.source)
    shown = str(resolved.path)
    real = str(resolved.resolved_path)
    if resolved.source != "default" and shown != real:
        return f"{shown} -> {real} ({where})"
    return f"{shown} ({where})"


def check_config_schema(resolved: ResolvedConfigPath) -> str:
    """Verify the config database is at this package's Alembic head and
    return that head revision's id.

    Opens exactly one **read-only** connection, runs `check_schema`, and
    closes it before returning -- holding it open would pin a WAL read
    snapshot and stop the CLI's checkpoints from passing (see
    `check_schema`'s own docstring).

    Raises `SchemaError` (`SchemaOutOfDate`, `SchemaVersionMismatch`,
    `SchemaSplitBrain`) for a database this daemon must refuse, and
    `ConfigPathError` for a path/permission problem the preflight catches.
    It never migrates: fixing a `SchemaOutOfDate` is `zfsbackup-config`'s
    job, deliberately, because reading an older schema through current ORM
    models silently mis-reads config, and a daemon that refuses to start
    beats a daemon that starts wrong.
    """
    with open_config_connection(resolved, readonly=True) as connection:
        return check_schema(connection)


def load_runtime_config(
    resolved: ResolvedConfigPath, *, verify_schema: bool = True
) -> BackupConfig:
    """Load a fully detached `BackupConfig` from the config database.

    Two handles, strictly sequential, both read-only, both closed before
    this returns: `check_config_schema`'s connection first (closed at its
    `with` exit), then a read-only session for `load_config`. Never
    nested -- see this module's docstring, constraint 2.

    `verify_schema=False` skips the schema connection for a caller that has
    already run `check_config_schema` itself in the same sequence. It is
    not an escape hatch for skipping the check altogether: every process
    that reads this database -- the supervisor at startup *and* each worker
    child, including one respawned hours later against a database the CLI
    migrated in the meantime -- verifies the schema exactly once before
    reading rows through the ORM.

    The returned `BackupConfig` owns no session and no ORM state, so it is
    safe to hold for the life of the process and to hand to
    `DatasetManager`/`create_app()`; that is the whole reason the store
    maps rows into dataclasses instead of exposing the ORM objects.

    Raises `ConfigPathError`, `SchemaError`, or `sqlalchemy.exc.
    OperationalError`; `config_error_message` turns any of the three into
    an operator-grade line.
    """
    if verify_schema:
        check_config_schema(resolved)

    with open_config_session(resolved, readonly=True) as session:
        # ONE read snapshot for the whole of `load_config`, explicitly
        # begun and explicitly ended.
        #
        # `load_config` issues five or more independent statements
        # (`GlobalSettings`, `Destination`, `Dataset`, then a lazy
        # `retention_rules`/`remotes` load per dataset, then
        # `RemoteServer`). `db.py` deliberately installs no `begin`
        # handler on reader engines, and pysqlite emits no `BEGIN` for a
        # bare `SELECT`, so without this each of those statements is its
        # own read transaction against whatever the database contains at
        # that instant. A `zfsbackup-config import` committing in the
        # middle is not seen as an error; it is seen as a *different
        # config*, half from before the commit and half from after.
        #
        # Measured, with a concurrent import that merely reorders two
        # datasets in the YAML: the two datasets' retention policies came
        # back swapped, so a dataset configured `1d -> 10y` was handed
        # `1h -> 2h` and the PruningWorker built from it would destroy
        # ten-year archive snapshots after two hours -- no exception, no
        # warning. A reader/writer soak hit 15.6% torn loads with zero
        # exceptions of any kind. The window is ~1.6ms, but 8h reaches it
        # on every worker start and every crash restart, forever.
        #
        # This does NOT contradict `db.py`'s "never adopt BEGIN-on-connect
        # for reader engines" rule, and must not be "fixed" back to match
        # it. That rule forbids pinning a WAL read snapshot for a
        # connection's entire life, which for the long-lived worker
        # readers would stop checkpoints passing and grow `-wal` without
        # bound. This snapshot is scoped to one `load_config` call --
        # milliseconds -- and is committed before the session closes.
        #
        # Residual, deliberately not addressed here: the schema check
        # (`check_config_schema`, its own earlier connection) is outside
        # this snapshot, so a migration landing between the two is still
        # possible. That is a far narrower window than the multi-statement
        # one above, and the real answer to it is the `generation` counter
        # items 15/17 own, not a longer-lived reader.
        session.execute(text("BEGIN"))
        import os as _os
        _raw = session.connection().connection.dbapi_connection
        _in1 = _raw.in_transaction
        try:
            config = load_config(session)
            _raw2 = session.connection().connection.dbapi_connection
            if not (_in1 and _raw2.in_transaction and _raw is _raw2):
                with open("/tmp/snapdbg.log", "a") as _f:
                    _f.write(f"VIOLATION pid={_os.getpid()} after_begin={_in1} after_load={_raw2.in_transaction} same_conn={_raw is _raw2}\n")
        except BaseException:
            try:
                session.execute(text("ROLLBACK"))
            except Exception:  # pragma: no cover - the close below is the
                pass           # real guarantee; never mask the original
            raise
        session.execute(text("COMMIT"))
        return config


def config_error_message(
    resolved: Optional[ResolvedConfigPath], exc: Exception
) -> str:
    """The single translation policy from a config-load failure to the one
    line an operator should see.

    `ConfigPathError` and `SchemaError` messages are already operator-grade
    -- they name the file, the three candidate sources, and in
    `ConfigDbIsYaml`'s case a copy-pasteable `zfsbackup-config import
    <path>` command. They are returned **verbatim**. Prefixing them with
    something like "Failed to load configuration: " would wrap a multi-line
    message that ends in a command the operator is meant to paste, which is
    exactly how that command stops being pasteable.

    An `OperationalError` means the preflight passed but the real open
    still failed -- routine when the daemon runs as root, since
    `os.access` is uid-based, blind to ACLs, and permissive for root. It
    goes through `diagnose_open_failure` with the same
    `require_writable=True` the open used (forwarding a different value
    would produce a confidently wrong diagnosis of an access mode the open
    never requested). If the re-run preflight finds nothing wrong, the
    SQLite error is reported as-is rather than guessed at.

    `resolved` is `Optional` because `resolve_config_path` can itself raise
    a `ConfigPathError` (a symlink loop, an unlinked cwd) before any
    `ResolvedConfigPath` exists -- and that is exactly the branch that
    needs none of it.
    """
    if isinstance(exc, (ConfigPathError, SchemaError)):
        return str(exc)

    where = (
        f" ({describe_config_source(resolved)})" if resolved is not None else ""
    )

    if isinstance(exc, OperationalError) and resolved is not None:
        diagnosed = diagnose_open_failure(resolved, exc, require_writable=True)
        if diagnosed is not None:
            return str(diagnosed)
        return f"Could not open the config database{where}: {exc}"

    return f"Failed to load configuration{where}: {exc}"

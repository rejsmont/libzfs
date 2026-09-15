"""Alembic driver for the zfsbackup config store.

Deliberately not named `db.py` -- item 5 owns that filename for
engine/session setup (lazy, per-process, pid-keyed, created only after any
`multiprocessing` fork). This module creates no `Engine` and no
`Connection`; `ensure_schema()` takes an already-open `Connection` and does
nothing else. That is what keeps this module fork-safe by construction and
lets it land ahead of item 5 without any risk of colliding with its
pid-keyed engine cache -- there is no engine here to collide with.

No caller is wired up yet. Per the item-7 plan: the CLI's `import` (item 8)
will call `ensure_schema()` as part of bringing a fresh or existing DB up
to date; the daemon will only ever *check* the current revision against the
package's head and refuse to start on a mismatch, never write -- "the CLI
is the only writer" is what item 5's fork-safety and WAL design depend on.

**Binding note for item 8, verified against SQLAlchemy 2.0.51 / Alembic
1.18.5 / pysqlite:** call `ensure_schema(connection)` as a standalone unit
of work, then `connection.commit()` immediately -- before doing anything
else on that connection, and even if the connection was completely unused
before the call. There are two distinct, independently-verified ways of
losing work if that commit does not happen, and they fail differently:

1. **The connection already had an open transaction before `ensure_schema`
   was called** (e.g. an earlier `execute()` on the same connection
   autobegan one). Alembic detects the ambient transaction and does not
   open (or commit) one of its own -- the whole migration, `CREATE TABLE`s
   and the `alembic_version` stamp alike, runs inside it. A later
   `rollback()` -- for any reason, including one with nothing to do with
   the migration (a bad YAML file, an `IntegrityError` from `save_config`,
   an operator `^C`) -- discards the entire migration, schema included,
   with no error at the time. Re-running `ensure_schema` against the same,
   now fully unmigrated database just redoes the work; nothing is stuck.

2. **The connection was clean when `ensure_schema` was called** -- no
   prior statement, no open transaction. This is the more dangerous case,
   because it looks safe and is not. `ensure_schema` itself reads
   `alembic_version` (via `MigrationContext.configure`) before running
   anything, and that read is what leaves the connection primed: pysqlite
   auto-commits a DDL statement immediately, as its own implicit
   transaction, whenever no transaction is already open when it runs --
   which is the case for the six `CREATE TABLE`s here, so they land
   durably and immediately, before `ensure_schema` even returns. The
   `INSERT INTO alembic_version` that stamps the migration is the one
   plain-DML statement in the whole sequence, and it is what actually
   opens a real, pending transaction (pysqlite auto-begins one before DML,
   not before DDL or `SELECT`) -- one `ensure_schema` never commits itself
   (see below). If that transaction is later rolled back, or the
   connection is simply dropped without an explicit commit at all (an
   unhandled exception, a `^C`, a crash) -- the version stamp is what is
   lost. The six tables remain. This is `SchemaSplitBrain`'s target case:
   full schema, no recorded revision, and every future `ensure_schema`
   call against that database raises rather than hitting a bare
   `OperationalError: table ... already exists` from `command.upgrade`.

Both cases are the same underlying advice from the caller's side: nothing
`ensure_schema` does is durable until the caller commits, and that commit
must happen immediately, on its own, before the connection does anything
else. Never commits or closes `connection` itself -- the caller owns the
transaction, the same contract `mapper.save_config` already follows -- so
this is not something `ensure_schema` can fix from inside; only discipline
at the call site can.
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic.util.exc import CommandError
from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from zfsbackup.store.models import Base

# zfsbackup/store/migrations/, not alembic.ini's script_location -- this
# path is fixed at install time and does not depend on `alembic.ini`
# existing at all (see `_build_config`'s docstring).
_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class SchemaVersionMismatch(RuntimeError):
    """Raised by `ensure_schema` when a database's recorded Alembic
    revision is not one the installed package's migration scripts know
    about.

    This is the forward-compatibility guard: an older zfsbackup package
    opening a database that a newer package already migrated must not
    silently `upgrade head` against an unrecognised history (there is
    nothing sane to autogenerate against a revision graph this package has
    never seen), and must not silently proceed as if the DB were already
    current either. Both would corrupt or misinterpret the schema. The fix
    is always on the operator's side: install a zfsbackup version whose
    migrations include the DB's revision.
    """


class SchemaSplitBrain(RuntimeError):
    """Raised by `ensure_schema` when a database has no recorded Alembic
    revision (`alembic_version` missing or empty) but one or more tables
    this package's schema defines already exist.

    Not merely a defensive, hard-to-reach check -- this is the
    **routinely** reachable outcome of the module docstring's item-8
    binding note, case 2: call `ensure_schema` on a clean connection and
    let the caller's process exit, crash, or simply forget to
    `connection.commit()` before that same call returns, and the database
    is left in exactly this state (verified directly: the six tables
    commit durably as `ensure_schema` runs, the `alembic_version` stamp
    does not, until the caller commits). It can also result from a rolled-
    back caller transaction that had already begun before `ensure_schema`
    was called (case 1 there), a future multi-revision upgrade interrupted
    partway through, or manual intervention on the database file -- this
    check does not need to know which. Without it, `command.upgrade(cfg,
    "head")` would attempt to `CREATE TABLE` a table that is already there
    and fail with a bare, unrecoverable `OperationalError: table ...
    already exists` -- forever, on every retry, since nothing about that
    error tells the operator what happened or how to fix it. The fix
    always requires a human decision (`alembic stamp head` if the existing
    tables are in fact already at head, or dropping/renaming the database
    and starting over if they are not), so this raises rather than
    guessing.
    """


def _has_existing_schema(connection: Connection) -> bool:
    """True if any table `Base.metadata` defines already exists on
    `connection`. Used only to detect the split-brain state described by
    `SchemaSplitBrain` -- a normal brand-new database has no current
    Alembic heads *and* none of these tables, which is the expected state
    `ensure_schema` performs the initial migration against.
    """
    existing = set(inspect(connection).get_table_names())
    expected = set(Base.metadata.tables.keys())
    return bool(existing & expected)


def _build_config(connection: Connection) -> Config:
    """Build an Alembic `Config` programmatically, without reading
    `alembic.ini`.

    `alembic.ini` is a developer/repo artifact (used by the bare `alembic`
    CLI during development) and is not part of an installed deployment --
    nothing guarantees it exists, or that the process's current working
    directory is the repo root, wherever `ensure_schema` is called from. The
    migration scripts themselves (`zfsbackup/store/migrations/`) are
    resolved relative to this file instead, which ships with the package
    regardless of how it is installed.

    `attributes["connection"]` is what `env.py`'s `run_migrations_online()`
    honours ahead of building its own engine (see that module's docstring)
    -- this is the one load-bearing setting here. `attributes
    ["configure_logger"] = False` is a second, independent guard against
    `env.py` calling `logging.config.fileConfig()` (see that module's
    docstring, point 2), but on this path it is belt-and-braces, not the
    load-bearing half: `Config()` here is never given a `config_file_name`
    at all (no `alembic.ini` is read), and `env.py`'s own `if
    config.config_file_name is not None` check already skips `fileConfig()`
    for that reason alone. Set for defence in depth and to keep the two
    `Config` construction paths (this one, and a bare `alembic` CLI's)
    behaving consistently if this function is ever pointed at a real ini
    file, not because it is the thing actually preventing the call today.
    """
    cfg = Config()
    cfg.set_main_option("script_location", str(_MIGRATIONS_DIR))
    cfg.attributes["connection"] = connection
    cfg.attributes["configure_logger"] = False
    return cfg


def ensure_schema(connection: Connection) -> str:
    """Bring `connection`'s database up to this installed package's
    Alembic head revision, in place, and return that head revision's id.

    Idempotent: a database already at head is left untouched (Alembic's
    `upgrade head` against a current DB runs no migrations). Never commits
    or closes `connection` -- the caller owns the transaction, the same
    contract `mapper.save_config` already follows. **`connection.commit()`
    immediately after this call is not optional, even on a connection that
    had no prior activity at all** -- see this module's docstring for the
    two independently-verified ways of losing work otherwise (an already-
    open caller transaction destroys the whole migration on rollback; a
    clean connection still leaves the `alembic_version` stamp -- not the
    schema -- uncommitted and at risk, because reading it is itself the
    first thing this function does).

    Before upgrading, checks that every revision the database currently
    records (`MigrationContext.get_current_heads()`) is one this package's
    migration scripts recognise (`ScriptDirectory.get_revision`). A
    database migrated by a *newer* zfsbackup package -- one whose
    migrations this installed package has never seen -- fails this check
    and raises `SchemaVersionMismatch` naming both the database's revision
    and this package's head, rather than either refusing to progress with
    no explanation or, worse, attempting `upgrade head` against a history
    Alembic cannot make sense of. A brand new (empty) database has no
    current heads at all, so the loop below is a no-op for it and
    `command.upgrade` performs the full initial migration.

    A database can also have no current heads *and* already contain one or
    more of this package's tables -- `alembic_version` missing or emptied
    while the application tables it should describe remain. This is not a
    remote edge case: it is exactly what an earlier `ensure_schema` call
    whose caller never committed (or rolled back) leaves behind -- see
    `SchemaSplitBrain`'s docstring and this module's item-8 binding note.
    It is detected (`_has_existing_schema`) and raised as `SchemaSplitBrain`
    before attempting `command.upgrade`, which would otherwise fail with a
    bare, unrecoverable `OperationalError: table ... already exists` on
    every retry.
    """
    cfg = _build_config(connection)
    script = ScriptDirectory.from_config(cfg)
    code_heads = script.get_heads()
    if len(code_heads) != 1:
        # get_current_head() (singular) raises a bare
        # `CommandError: The script directory has multiple heads` for this
        # case instead of something ensure_schema's own error handling
        # would catch -- guard explicitly so a branched migration history
        # (which should never happen for this package -- a single linear
        # chain is the whole point of the item-7 baseline) fails with a
        # named, explained error rather than an opaque one escaping ahead
        # of the checks below.
        raise RuntimeError(
            f"zfsbackup's installed migration scripts have "
            f"{len(code_heads)} heads {code_heads!r}; ensure_schema only "
            f"supports a single, linear migration history. This is a "
            f"packaging defect, not something fixable by an operator."
        )
    code_head = code_heads[0]

    db_heads = MigrationContext.configure(connection).get_current_heads()
    if not db_heads and _has_existing_schema(connection):
        raise SchemaSplitBrain(
            f"Database has no recorded Alembic revision (alembic_version "
            f"missing or empty) but one or more of this package's tables "
            f"already exist. Refusing to attempt the initial migration, "
            f"which would fail with 'table already exists'. If the "
            f"existing tables are already at revision {code_head!r}, fix "
            f"this by stamping them (`alembic stamp head`); otherwise "
            f"restore the database from a backup or start over with a "
            f"fresh file."
        )

    for revision in db_heads:
        try:
            script.get_revision(revision)
        except CommandError as exc:
            raise SchemaVersionMismatch(
                f"Database schema is at revision {revision!r}, which this "
                f"installed zfsbackup package (migrations head "
                f"{code_head!r}) does not recognise. This usually means "
                f"the database was created or migrated by a newer "
                f"zfsbackup package. Upgrade the zfsbackup package "
                f"installed on this host before opening this database "
                f"again."
            ) from exc

    command.upgrade(cfg, "head")
    return code_head

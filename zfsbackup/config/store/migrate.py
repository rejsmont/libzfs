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

**`check_schema(connection)` (item 6a) is that check.** It is the
never-writes half of what used to be `ensure_schema`'s single monolithic
body -- see its own docstring -- and it is what makes the daemon's
"only checks, never writes" promise above an actual function the daemon
can call rather than just a sentence in this docstring. `ensure_schema`
is now defined in terms of it (`check_schema` + `command.upgrade`) so the
two cannot drift apart: a database `check_schema` accepts needs nothing
done to it, and one it refuses is, by construction, exactly the state
`ensure_schema`'s `except SchemaOutOfDate` branch migrates.

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

**Item 5 changes which of the two cases applies, not the advice.** A
`Connection` opened from a file-backed writer engine in `zfsbackup/config/store/
db.py` runs with pysqlite's implicit transaction handling disabled and an
explicit `BEGIN IMMEDIATE` at the first statement, so case 2 collapses into
case 1: the `CREATE TABLE`s no longer auto-commit themselves, and the whole
migration -- schema and version stamp alike -- lives or dies with the
caller's commit. That is strictly better (a rolled-back migration leaves an
untouched database rather than a `SchemaSplitBrain`), and it makes the
commit below non-optional rather than merely advisable. Do not rely on the
old auto-commit behaviour for a connection that came from `db.py`; do not
assume the new behaviour for one that did not.

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

from zfsbackup.config.store.models import Base

# zfsbackup/config/store/migrations/, not alembic.ini's script_location -- this
# path is fixed at install time and does not depend on `alembic.ini`
# existing at all (see `_build_config`'s docstring).
_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class SchemaError(RuntimeError):
    """Base for the three schema-state refusals `check_schema`/
    `ensure_schema` can raise: `SchemaSplitBrain`, `SchemaVersionMismatch`,
    `SchemaOutOfDate`.

    Deliberately **not** the base of the bare `RuntimeError` `check_schema`
    raises when its own installed migration scripts have more than one
    head (see that function) -- that is a packaging defect in this
    installed zfsbackup package, not an operator-fixable database state,
    and the two must not share a catchable base. Item 8's daemon half is
    expected to catch `SchemaError` specifically, mirroring `ConfigPathError`
    in `paths.py`: an `except RuntimeError` there would also silently
    absorb the multi-head packaging bug into the same "clean refusal,
    log and exit" handling meant for a stale-but-fixable schema, instead
    of letting it crash loudly as a bug report.
    """


class SchemaOutOfDate(SchemaError):
    """Raised by `check_schema` when a database's recorded Alembic
    revision(s) are ones this installed package's migration scripts DO
    recognise, but are not the package's head -- i.e. the database predates
    an upgrade this package already knows how to perform.

    This is the daemon's refusal case (item 6a / item 8; `migrate.py`'s
    module docstring and `docs/config_db_cli_plan.md:821-823` both promise
    "the daemon only *checks* and refuses to start on a mismatch, never
    write"). The distinction from `SchemaVersionMismatch` matters: that
    exception is a revision this package has never heard of (newer package
    wrote it); this one is a revision this package HAS heard of and could
    upgrade -- `ensure_schema` would happily run `command.upgrade(cfg,
    "head")` and fix it. The daemon must not do that itself. Reading an
    older schema through the current ORM models risks silently
    misinterpreting a column that migrated meaning, or missing one that was
    added -- e.g. reading pre-retention-scoping rows as if
    `dataset_remote_id` already existed. Data loss (a daemon that refuses to
    start) beats data corruption (a daemon that starts and mis-reads
    config). The fix is always `zfsbackup-config import`/the CLI's
    migration path, i.e. `ensure_schema`, run deliberately by an operator or
    the CLI -- never automatically by a process that only ever opens the
    database read-only.
    """


class SchemaVersionMismatch(SchemaError):
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


class SchemaSplitBrain(SchemaError):
    """Raised by `ensure_schema` when a database has no recorded Alembic
    revision (`alembic_version` missing or empty) but one or more tables
    this package's schema defines already exist.

    **How reachable this is depends on where the connection came from, and
    that changed with item 5.** Over a connection from `store/db.py`'s
    writer engine -- the sanctioned item-8 path -- it is *not* routinely
    reachable: that connection runs under an explicit `BEGIN IMMEDIATE`
    with pysqlite's implicit transaction handling disabled, so the
    `CREATE TABLE`s no longer auto-commit ahead of the stamp and a missing
    commit discards the whole migration instead of half of it (verified
    both ways: rollback leaves zero tables; commit leaves seven tables and
    the stamp). Over any *other* connection -- a hand-built engine, the
    `sqlite3` CLI, a pre-item-5 caller -- it remains the **routinely**
    reachable outcome of the module docstring's item-8 binding note, case
    2: call `ensure_schema` on a clean connection and let the caller's
    process exit, crash, or simply forget to `connection.commit()` before
    that same call returns, and the database is left in exactly this state
    (verified directly: the six tables commit durably as `ensure_schema`
    runs, the `alembic_version` stamp does not, until the caller commits).
    Either way this check stays -- it is cheap, and the states below do not
    care how they arose. It can also result from a rolled-
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
    migration scripts themselves (`zfsbackup/config/store/migrations/`) are
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


def check_schema(connection: Connection) -> str:
    """Check `connection`'s database against this installed package's
    Alembic head revision and return that head revision's id. **Never
    writes anything** -- no DDL, no DML, not even `alembic_version`.

    That is a statement about writes, not about transactions: `Migration
    Context.configure(connection).get_current_heads()` and
    `_has_existing_schema`'s `inspect(connection)` both issue `SELECT`s,
    and on a file-backed connection from `store/db.py`'s reader engine a
    bare `SELECT` still opens (or extends) a real SQLite read transaction
    that pins a WAL read snapshot for as long as it stays open. The
    caller does not need to `commit()` after `check_schema` -- there is
    nothing to commit -- but a caller that keeps `connection` open and
    idle afterwards is still holding that snapshot open, and under WAL an
    open reader snapshot is exactly what prevents a checkpoint from
    passing, letting the `-wal` file grow without bound (see `db.py`'s
    module docstring, "Transaction start: readers and writers differ
    deliberately"). Close or roll back `connection` once you are done
    with the value `check_schema` returned, the same discipline
    `session_for_engine`'s reader form already applies. This is the half of the old,
    monolithic `ensure_schema` that item 6a's daemon-refusal contract
    (`migrate.py`'s own module docstring, `docs/config_db_cli_plan.md:
    821-823`) actually needs: "the daemon only *checks* and refuses to
    start on a mismatch, never write". `ensure_schema` below is now
    `check_schema` plus `command.upgrade` and nothing else, specifically so
    the two functions cannot drift apart -- a case `check_schema` accepts
    is by construction a case `ensure_schema` treats as already done, and a
    case it refuses is by construction one only `ensure_schema` (an
    operator-driven migration) can fix.

    Raises, in order:

    - `RuntimeError` if this package's own migration scripts have more than
      one head -- a packaging defect, never an operator-fixable state (see
      inline comment below).
    - `SchemaSplitBrain` if the database has no recorded Alembic revision
      (`alembic_version` missing or empty) but one or more of this
      package's tables already exist -- `alembic_version` lost to an
      uncommitted `ensure_schema` call, or the tables created some other
      way. See that exception's docstring.
    - `SchemaVersionMismatch` if the database records a revision this
      installed package's migration scripts do not recognise -- written by
      a *newer* zfsbackup package. See that exception's docstring.
    - `SchemaOutOfDate` if the database's recorded revision(s) are ones
      this package *does* recognise but are not its head -- including a
      brand-new or empty database with no recorded revision at all (no
      `alembic_version`, no tables): `check_schema` cannot itself perform
      the initial migration, so that case is a refusal here, not the
      silent "create it" that `ensure_schema` alone used to provide. See
      that exception's docstring for why the daemon's refusal, not a
      log-and-continue, is the deliberate behaviour.

    Returns the code head with no exception only when the database's
    recorded heads are exactly `{code_head}`.
    """
    cfg = _build_config(connection)
    script = ScriptDirectory.from_config(cfg)
    code_heads = script.get_heads()
    if len(code_heads) != 1:
        # get_current_head() (singular) raises a bare
        # `CommandError: The script directory has multiple heads` for this
        # case instead of something this function's own error handling
        # would catch -- guard explicitly so a branched migration history
        # (which should never happen for this package -- a single linear
        # chain is the whole point of the item-7 baseline) fails with a
        # named, explained error rather than an opaque one escaping ahead
        # of the checks below.
        raise RuntimeError(
            f"zfsbackup's installed migration scripts have "
            f"{len(code_heads)} heads {code_heads!r}; check_schema/"
            f"ensure_schema only support a single, linear migration "
            f"history. This is a packaging defect, not something fixable "
            f"by an operator."
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

    if set(db_heads) != {code_head}:
        if db_heads:
            current = f"revision(s) {sorted(db_heads)!r}"
        else:
            current = (
                "no recorded Alembic revision at all (alembic_version "
                "missing or empty, and no application tables exist -- a "
                "brand-new or empty database)"
            )
        raise SchemaOutOfDate(
            f"Database schema is at {current}, which is behind this "
            f"installed zfsbackup package's migration head {code_head!r}. "
            f"check_schema() only checks -- it never migrates the "
            f"database, so this is a refusal, not an automatic upgrade. "
            f"Run `zfsbackup-config import` (or otherwise invoke "
            f"ensure_schema()) to bring the database up to date, then "
            f"start the daemon again."
        )

    return code_head


def ensure_schema(connection: Connection) -> str:
    """Bring `connection`'s database up to this installed package's
    Alembic head revision, in place, and return that head revision's id.

    **Reimplemented as `check_schema(connection)` plus `command.upgrade`,
    so the two can never drift.** `check_schema` alone decides whether the
    database needs anything done to it; this function's only addition is
    performing that migration when `check_schema` says `SchemaOutOfDate`,
    then re-checking to confirm and return the head. Every other exception
    `check_schema` can raise (`RuntimeError`, `SchemaSplitBrain`,
    `SchemaVersionMismatch`) is not something a migration can fix, so it
    propagates unchanged -- `ensure_schema` catches `SchemaOutOfDate`
    specifically, nothing broader.

    Idempotent: a database already at head is left untouched --
    `check_schema` returns immediately with no exception and no write at
    all (not even a no-op `command.upgrade` call), which is a strictly
    stronger guarantee than the previous implementation's "Alembic's
    `upgrade head` against a current DB runs no migrations" (that version
    still opened and read `alembic_version` via `command.upgrade` on every
    call; this one does not call into `command.upgrade` at all once the
    database is current). Never commits or closes `connection` -- the
    caller owns the transaction, the same contract `mapper.save_config`
    already follows. **`connection.commit()` immediately after this call
    is not optional, even on a connection that had no prior activity at
    all** -- see this module's docstring for the two independently-
    verified ways of losing work otherwise (an already-open caller
    transaction destroys the whole migration on rollback; a clean
    connection still leaves the `alembic_version` stamp -- not the schema
    -- uncommitted and at risk, because reading it is itself the first
    thing `check_schema` does).

    A brand new (empty) database has no current heads at all, so
    `check_schema` raises `SchemaOutOfDate` for it just like any other
    behind-head database, and the migration below performs the full
    initial migration via `command.upgrade`.

    **`command.upgrade` deliberately runs OUTSIDE the `except` block**,
    not nested inside it. An earlier version of this function called it
    from within `except SchemaOutOfDate:`, which is correct in outcome
    but wrong in presentation: had `command.upgrade` itself failed, Python
    would report it chained as "During handling of the above exception,
    another exception occurred" underneath `SchemaOutOfDate`'s own "run
    the CLI to migrate" text -- telling the reader to run the very
    command whose failure they are looking at, and burying the real
    traceback under an exception that was never the problem. Capturing
    "does this need a migration" as a plain boolean first, and running
    `command.upgrade` only after leaving the `except` block entirely,
    keeps a genuine upgrade failure an unchained, first-class exception.
    """
    needs_migration = False
    try:
        head = check_schema(connection)
    except SchemaOutOfDate:
        needs_migration = True

    if not needs_migration:
        return head

    cfg = _build_config(connection)
    command.upgrade(cfg, "head")
    # Re-derive rather than trust anything computed above: this confirms
    # the upgrade actually landed (a mutant that silently swallowed
    # `command.upgrade`'s effect would fail this line, not just look
    # correct), and it is the reason `ensure_schema` and `check_schema`
    # cannot silently drift apart -- `ensure_schema` never computes "is
    # this database current" by any means other than calling
    # `check_schema`.
    return check_schema(connection)

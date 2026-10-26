"""Alembic driver for the zfsbackup config store.

Deliberately not named `db.py` -- item 5 owns that filename for
engine/session setup (lazy, per-process, pid-keyed, created only after any
`multiprocessing` fork). `check_schema()` and `ensure_schema()` create no
`Engine` and no `Connection`; they take an already-open `Connection` and do
nothing else, which is what keeps the daemon's path through this module
fork-safe by construction. The one exception is `upgrade_schema()` (item
10.1), the CLI's `zfsbackup-config migrate`: it opens its own connection, but
only through `paths.open_config_connection` -> `db.get_engine`, i.e. the same
pid-keyed cache every other caller uses, and it is never reachable from the
daemon (the daemon only ever *checks*) or a worker (which never opens the
store).

Callers: `importer.py`'s `import_yaml` calls `ensure_schema()` as part of
bringing a fresh or existing DB up to date; `zfsbackup-config migrate` calls
`upgrade_schema()`; the daemon supervisor only ever *checks* the current
revision against the package's head (`check_schema`, inside `read_config`,
via `zfsbackup/runtime_config.py`) and refuses to start on a mismatch, never
write; workers never open the database at all -- "the CLI is the only writer" is what item 5's fork-safety and WAL
design depend on.

**`EXPECTED_HEAD` / `KNOWN_REVISIONS` (item 10.1, B1).** `check_schema`
decides from these two module constants and reads no `ScriptDirectory` at
all, so the daemon's startup check does no filesystem walk of the migrations
package and cannot be broken by a packaging problem in it. The price is that
the constants must track the scripts; the *upgrade* path pays for that by
asserting, before it touches the database, that the scripts' head equals
`EXPECTED_HEAD` and that their revision set equals `KNOWN_REVISIONS` (a
`MigrationPackagingError`, deliberately not a `SchemaError`: a packaging
defect, not an operator-fixable database state). **Adding a migration
therefore means editing three things together: the new revision file,
`EXPECTED_HEAD`, and `KNOWN_REVISIONS`** (see `migrations/README`).
Forgetting any of them fails loudly at `migrate` / `import` time and in the
test suite (`TestExpectedHeadConstants`), never silently at daemon start.

**Never modify a database that is not ours (item 10.4).** A file with no
recorded revision but with tables in it is not something this package may
migrate: `check_schema`/`upgrade_schema` raise `NotAConfigDatabase` (or
`SchemaSplitBrain` when the tables are exactly those of one of our
revisions) and write nothing.
`migrate` only upgrades a *recorded, known* older revision; creating a
database is `zfsbackup-config import`'s job alone. The writer engine
cooperates by not switching such a file to WAL on connect (`db.py`).

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

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from zfsbackup.config.store.models import Base
from zfsbackup.config.store.paths import ResolvedConfigPath, open_config_connection

# Alembic's per-call INFO lines are silenced for `check_schema` only, by a
# per-thread flag and one permanent filter on `alembic.runtime.migration`.
# The filter is DEFINED in `_alembic_quiet.py`, not here, on purpose: a
# filter defined in this module pinned this module's globals from the
# process-lifetime logger, and through them every cached engine, so pooled
# connections survived interpreter exit and a clean `import` left `-wal`/
# `-shm` behind. See that module's docstring before moving it back.
from zfsbackup.config.store._alembic_quiet import (  # noqa: F401 (re-exported)
    QuietWhileChecking as _QuietWhileChecking,
    quiet as _quiet,
    quiet_alembic as _quiet_alembic,
)


# zfsbackup/config/store/migrations/, not alembic.ini's script_location -- this
# path is fixed at install time and does not depend on `alembic.ini`
# existing at all (see `_build_config`'s docstring).
_MIGRATIONS_DIR = Path(__file__).parent / "migrations"

# The single head of `migrations/versions/`, and every revision id in that
# history. `check_schema` decides from these and reads no scripts (item 10.1,
# B1) -- see the module docstring. They MUST be edited together with any new
# revision file; `_assert_scripts_match_constants` (run first on every upgrade
# path, and by the test suite) fails loudly if they drift.
EXPECTED_HEAD = "5d2f8a60c1e4"
KNOWN_REVISIONS = frozenset({"a1c28e1b3f34", "7c3e9d51b2a8", "5d2f8a60c1e4"})

# The tables (`alembic_version` excluded) that a database AT each known
# revision holds, newest first. Only used to diagnose a database whose stamp
# was lost: with no recorded revision, a table set equal to one of these is
# ours (`SchemaSplitBrain`, which offers recovery), anything else is
# `NotAConfigDatabase`. The baseline entry matters because an interrupted
# FIRST `ensure_schema` over a connection with pysqlite's implicit DDL
# autocommit (anything not from `db.py`'s writer engine) leaves exactly the
# baseline's six tables: their `CREATE TABLE`s autocommit, the baseline's
# `alembic_version` INSERT opens a transaction, and every later revision's
# DDL runs inside it and is rolled back with the stamp. Matching only the
# head's set misreported that state as someone else's database. Add an entry
# whenever a revision changes the table set (the head's is `Base.metadata`).
_TABLES_AT_REVISION: Tuple[Tuple[str, frozenset], ...] = (
    (EXPECTED_HEAD, frozenset(Base.metadata.tables)),
    # 5d2f8a60c1e4 only added a column (`dataset_remotes.raw`), so the TLS
    # revision's table set equals the head's. Table names alone cannot tell
    # the two apart; `_COLUMNS_ADDED_AT_REVISION` below does.
    ("7c3e9d51b2a8", frozenset(Base.metadata.tables)),
    ("a1c28e1b3f34", frozenset({
        "global_settings", "datasets", "destinations", "dataset_remotes",
        "retention_rules", "remote_server",
    })),
)
_OUR_TABLE_NAMES = frozenset().union(*(t for _, t in _TABLES_AT_REVISION))

# Columns a revision ADDED to an existing table, as (table, column) pairs.
# A revision that changes only columns leaves `_TABLES_AT_REVISION` unable to
# distinguish it from its predecessor, and the first (newest) table-set match
# would always win: a database at the older revision that lost its
# `alembic_version` was told `alembic stamp head`, which makes `check_schema`
# say "up to date" while every read fails with `OperationalError: no such
# column: dataset_remotes.raw`. So a `_TABLES_AT_REVISION` entry matches only
# if its table set is equal AND every column added at that revision or any
# OLDER one is present. Add an entry here whenever a revision adds a column
# (a table-creating revision needs only its `_TABLES_AT_REVISION` entry). The
# same data backs `check_schema`'s defence in depth: a database STAMPED at
# head but missing such a column is refused as `SchemaColumnsMissing`.
_COLUMNS_ADDED_AT_REVISION: Dict[str, Tuple[Tuple[str, str], ...]] = {
    "7c3e9d51b2a8": (
        ("destinations", "server_fingerprint"),
        ("global_settings", "client_cert_file"),
        ("global_settings", "client_key_file"),
    ),
    "5d2f8a60c1e4": (("dataset_remotes", "raw"),),
}


def _columns_required_by(revision: str) -> Tuple[Tuple[str, str, str], ...]:
    """`(adding_revision, table, column)` for every column a database at
    `revision` must hold: those added at `revision` and at every older one
    (those later in `_TABLES_AT_REVISION`, which is newest first)."""
    revisions = [rev for rev, _names in _TABLES_AT_REVISION]
    if revision not in revisions:
        return ()  # nothing known about it (tests substitute a fake head)
    older_or_same = revisions[revisions.index(revision):]
    return tuple(
        (rev, table, column)
        for rev in older_or_same
        for table, column in _COLUMNS_ADDED_AT_REVISION.get(rev, ())
    )


def _missing_columns(connection: Connection, revision: str) -> list:
    """The `_columns_required_by(revision)` entries that `connection`'s
    database does not hold, as `(adding_revision, table, column)`. A table
    that is not there at all is not reported: that is a different damage
    (the table-set checks and the read's own "no such table" own it), and a
    column cannot be "added" to a table that does not exist."""
    insp = inspect(connection)
    present: Dict[str, set] = {}
    missing = []
    for rev, table, column in _columns_required_by(revision):
        if table not in present:
            present[table] = (
                {c["name"] for c in insp.get_columns(table)}
                if insp.has_table(table) else None
            )
        if present[table] is not None and column not in present[table]:
            missing.append((rev, table, column))
    return missing


def _revision_before(revision: str) -> str:
    """The revision that precedes `revision` in `_TABLES_AT_REVISION`: the one
    to stamp a database that lacks the columns `revision` added, so that
    `zfsbackup-config migrate` applies `revision` for real."""
    revisions = [rev for rev, _names in _TABLES_AT_REVISION]
    return revisions[revisions.index(revision) + 1]


def _stamp_back_is_safe(revision: str) -> bool:
    """Whether stamping the revision before `revision` and re-running
    `migrate` can work for a database that has all of the tables: only if
    `revision` created none (a column-only revision). The TLS revision
    created tables as well as columns, so replaying it over a database that
    already holds them fails with 'table already exists'; the only remedy
    there is a backup."""
    revisions = [rev for rev, _names in _TABLES_AT_REVISION]
    tables = dict(_TABLES_AT_REVISION)
    return tables[revision] == tables[revisions[revisions.index(revision) + 1]]


def _unknown_revisions(revisions) -> list:
    """The members of `revisions` this package does not recognise, sorted.

    The ONE place "is this recorded revision one of ours" is decided. Both
    `check_schema` (to refuse a newer/foreign history) and
    `may_switch_to_wal` (to decide whether the writer engine may rewrite the
    file header) call it, so the two cannot disagree about which databases
    are ours (item 10.5, M-1R). Reads `KNOWN_REVISIONS` at call time.
    """
    return sorted(r for r in revisions if r not in KNOWN_REVISIONS)


def may_switch_to_wal(cursor) -> bool:  # noqa: ANN001
    """Writer-engine connect hook: may this file be switched to WAL, i.e. is
    it either brand new or already recognisably ours? Called by `db.py` on a
    raw DBAPI `cursor`, before any schema check can run; only ever SELECTs.

    `PRAGMA journal_mode=WAL` rewrites the file header (bytes 18-19), so it
    must not be issued against a database `check_schema` is about to refuse
    -- `migrate`/`import` pointed at a foreign file promise "nothing was
    changed". The bug this prevents (item 10.5, M-1R): the old rule was "an
    `alembic_version` table exists", but a foreign Alembic-managed database
    has one too (holding the other application's revision), as does a
    foreign file with an EMPTY one; both were switched, then refused.

    True when:

    - the file holds no tables at all (a brand-new/zero-byte file; `import`
      creates the schema in it, so it must be WAL before any reader
      attaches); or
    - `alembic_version` holds at least one row and EVERY row is in
      `KNOWN_REVISIONS` (the test `check_schema` applies).

    False for everything else: foreign tables with a missing, empty or
    unrecognised `alembic_version`, an `alembic_version` that is not an
    Alembic version table -- and an EMPTY `alembic_version`, whatever else
    the file holds (item 10.6). A stamp-only file is refused by `migrate`
    (`SchemaOutOfDate`: no recorded revision, nothing to migrate), which
    promises it left the file unchanged; switching it here would rewrite the
    header first. `import` still works on one: its migration runs on the
    unswitched file, and the next engine, now seeing a known revision,
    switches it.

    Only "no such table"/"no such column" `OperationalError`s are read as
    "not an Alembic version table". Anything else ("database is locked",
    "database disk image is malformed", ...) propagates: guessing False
    there would silently leave a file of ours in rollback-journal mode.
    """
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'"
    )
    # `fetchone` loops rather than `fetchall`: only the minimal DBAPI cursor
    # surface is assumed (the connect-hook tests stub exactly that).
    tables = set()
    while (row := cursor.fetchone()) is not None:
        tables.add(row[0])
    if not tables:
        return True
    if "alembic_version" not in tables:
        return False
    revisions: list = []
    try:
        cursor.execute("SELECT version_num FROM alembic_version")
        while (row := cursor.fetchone()) is not None:
            revisions.append(row[0])
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if "no such column" in message or "no such table" in message:
            return False  # an `alembic_version` that is not Alembic's
        raise
    if not revisions:
        return False
    return not _unknown_revisions(revisions)


class SchemaError(RuntimeError):
    """Base for the schema-state refusals `check_schema`/`ensure_schema`/
    `upgrade_schema` can raise: `SchemaSplitBrain`, `SchemaVersionMismatch`,
    `SchemaOutOfDate`, `NotAConfigDatabase`, `SchemaIntegrityError`.

    Deliberately **not** the base of `MigrationPackagingError`, which
    `_assert_scripts_match_constants` raises when the installed migration
    scripts disagree with `EXPECTED_HEAD`/`KNOWN_REVISIONS` or have more than
    one head -- that is a packaging defect in this installed zfsbackup
    package, not an operator-fixable database state, and the two must not
    share a catchable base. The daemon catches `SchemaError` specifically,
    mirroring `ConfigPathError` in `paths.py`: an `except RuntimeError` there
    would also silently absorb the packaging bug into the same "clean
    refusal, log and exit" handling meant for a stale-but-fixable schema,
    instead of letting it crash loudly as a bug report.
    """


class MigrationPackagingError(RuntimeError):
    """The installed migration scripts disagree with `EXPECTED_HEAD` /
    `KNOWN_REVISIONS` (head, revision set, or more than one head). A defect in
    the installed zfsbackup package -- nothing an operator can fix by
    changing the database. Deliberately a `RuntimeError` but **not** a
    `SchemaError`; see `SchemaError`."""


class NotAConfigDatabase(SchemaError):
    """The file is an SQLite database that holds tables but records no
    zfsbackup schema revision, and not only zfsbackup's own tables: it belongs
    to something else (or was never initialised by zfsbackup). Raised by
    `check_schema` and `upgrade_schema` (hence `import` over an existing file)
    *before* anything is written. Grafting zfsbackup's schema into someone
    else's database -- and, for `migrate`, stamping it -- is the bug this
    exists to prevent (item 10.4; reproduced with a foreign `orders` table)."""


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
    config). The fix is always `zfsbackup-config migrate` -- which
    upgrades in place and keeps the stored configuration -- run deliberately
    by an operator, never automatically by a process that only ever opens
    the database read-only. (`zfsbackup-config import` also migrates, but it
    is a destructive full replace of the stored configuration, so it is not
    the remedy for "my schema is old".)

    `recorded_revisions` is the tuple of revision id(s) found in
    `alembic_version`, sorted, or `()` when none is recorded -- an empty or
    tableless file, or a stamp-less one `import` may still adopt. It lets a
    caller tell the two situations this one class covers apart without parsing
    the message: a non-empty tuple is a database `migrate` can upgrade; `()`
    is a file with nothing to migrate (`migrate` refuses it, `import` creates
    into it). It defaults to `()` so a bare `SchemaOutOfDate("text")` keeps
    working.
    """

    def __init__(self, *args: object, recorded_revisions: Tuple[str, ...] = ()) -> None:
        super().__init__(*args)
        self.recorded_revisions: Tuple[str, ...] = tuple(recorded_revisions)

    def __reduce__(self):  # keep the keyword-only attribute across pickle/copy
        return (_rebuild_out_of_date, (self.args, self.recorded_revisions))


def _rebuild_out_of_date(args: tuple, recorded_revisions: Tuple[str, ...]) -> SchemaOutOfDate:
    return SchemaOutOfDate(*args, recorded_revisions=recorded_revisions)


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

    The same state is what a foreign Alembic-managed database looks like
    (another application's `alembic_version`), so the message says both
    "newer zfsbackup package" and "not a zfsbackup config database"
    (item 10.5).
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

    "This package's tables" means the exact table set of ANY known revision
    (`_TABLES_AT_REVISION`), not only the head's. Since the second revision,
    case 2 above interrupted on a fresh file leaves the BASELINE's six tables
    (the later revision's DDL ran inside the stamp's transaction and was
    rolled back with it); that state is ours too, and the message names the
    revision to stamp before `zfsbackup-config migrate`.
    """


class SchemaIntegrityError(SchemaError):
    """Raised when a migration leaves rows that violate a foreign key
    (`PRAGMA foreign_key_check` returned any row) -- item 10.1, B2.

    Why this exists: Alembic's SQLite batch migrations recreate tables and
    must run with `PRAGMA foreign_keys=OFF` (it cannot be changed inside a
    transaction, so the connection is FK-off from the start). With
    enforcement off, a migration that moves or rewrites rows can leave an
    orphan that nothing complains about, and the first `foreign_keys=ON`
    writer to touch the table afterwards fails with a bare `FOREIGN KEY
    constraint failed` -- long after the migration "succeeded". SQLite's own
    documented recipe for table rebuilds is to run `PRAGMA foreign_key_check`
    before committing; this is that step. The migration is rolled back
    (`upgrade_schema` always; `ensure_schema`'s caller owns the transaction
    and must not commit after this is raised), so the database is exactly as
    it was before the attempt.
    """


class SchemaColumnsMissing(SchemaError):
    """Raised by `check_schema` when a database is stamped at the head but
    lacks a column that a revision at or below the head added
    (`_COLUMNS_ADDED_AT_REVISION`).

    How it arises: an unstamped database at an older, column-only revision
    that was stamped `head` by hand (the advice an earlier version gave), or
    a hand-edited file. Without this check `check_schema` and `migrate
    --check` say "up to date" while every read fails with a bare
    `OperationalError: no such column: ...`, which is not an exit-78 error
    and names no remedy. The message gives the remedy: stamp the revision
    before the one that added the column, then `zfsbackup-config migrate`
    adds it for real.
    """


@dataclass(frozen=True)
class SchemaUpgrade:
    """What `upgrade_schema` did. `from_revs` is the sorted tuple of
    revision ids the database recorded before (never empty: a database with
    no recorded revision is refused, not migrated); `to_rev` is the head it is at now; `changed` is False
    iff the database was already at head and nothing was written."""

    from_revs: Tuple[str, ...]
    to_rev: str
    changed: bool


def _db_label(connection: Connection) -> str:
    """The database path for messages (`:memory:` for an in-memory one)."""
    return connection.engine.url.database or ":memory:"


def _unrecorded_tables(connection: Connection) -> list:
    """Sorted names of the tables on `connection`, ignoring SQLite's internal
    `sqlite_*` tables (SQLAlchemy already omits them) and `alembic_version`
    (the stamp table, not application data: an empty one alone is just an
    unfinished initial migration). Only meaningful when no revision is
    recorded -- see `_refuse_unrecorded_tables`.
    """
    return sorted(
        t for t in inspect(connection).get_table_names() if t != "alembic_version"
    )


def _refuse_unrecorded_tables(connection: Connection) -> None:
    """With NO recorded revision, a database that already holds tables is not
    ours to migrate: raise instead of grafting the schema in (item 10.4).

    - The tables are EXACTLY the set some known revision leaves
      (`_TABLES_AT_REVISION`: the head's, or the baseline's six) and hold
      every column that revision and its predecessors added
      (`_COLUMNS_ADDED_AT_REVISION`; so a column-only difference gets the
      older revision's remedy, never `alembic stamp head`) ->
      `SchemaSplitBrain` (our tables, the stamp lost; the message names the
      revision they match and how to recover). Exactly, not "a subset"
      (item 10.5): a foreign file whose only table is called `datasets` must
      not be told to `alembic stamp` a schema it does not have.
    - Anything else -- a set no revision leaves, alone or alongside foreign
      tables -> `NotAConfigDatabase`. Its message lists the tables that are
      NOT ours first: with eleven tables of ours, a sorted list truncated at
      six could summarise the one foreign table away ("and N more").
    - No tables (empty or brand-new file) -> return; that is the legitimate
      state a first `import` migrates.
    """
    tables = _unrecorded_tables(connection)
    if not tables:
        return
    found = set(tables)
    # Table names AND columns: a column-only revision shares its predecessor's
    # table set, and the first match must not claim a database that lacks the
    # columns of the revision it names (`_COLUMNS_ADDED_AT_REVISION`).
    matches = [
        rev for rev, names in _TABLES_AT_REVISION
        if found == names and not _missing_columns(connection, rev)
    ]
    if matches:
        revision = matches[0]
        if revision == EXPECTED_HEAD:
            what = "all of this package's tables already exist"
            remedy = (
                f"If the existing tables are already at revision "
                f"{EXPECTED_HEAD!r}, fix this by stamping them (`alembic "
                f"stamp head`)"
            )
        else:
            what = (
                f"its tables are exactly those of this package's older "
                f"schema revision {revision!r} (a first import interrupted "
                f"before it committed, or an older database whose stamp "
                f"was lost)"
            )
            remedy = (
                f"If the existing tables are at revision {revision!r}, fix "
                f"this by stamping them (`alembic stamp {revision}`) and then "
                f"running `zfsbackup-config migrate` (head {EXPECTED_HEAD!r})"
            )
        raise SchemaSplitBrain(
            f"{_db_label(connection)} has no recorded Alembic revision "
            f"(alembic_version missing or empty) but {what}. Refusing to "
            f"attempt the initial migration, which would fail with 'table "
            f"already exists'. {remedy}; otherwise restore the database from "
            f"a backup or start over with a fresh file. Nothing was changed."
        )
    foreign = sorted(t for t in tables if t not in _OUR_TABLE_NAMES)
    listed = foreign + sorted(t for t in tables if t in _OUR_TABLE_NAMES)
    shown = ", ".join(listed[:6])
    more = f" and {len(listed) - 6} more" if len(listed) > 6 else ""
    raise NotAConfigDatabase(
        f"{_db_label(connection)} is an SQLite database but not a zfsbackup "
        f"config database (tables: {shown}{more}); nothing was changed. "
        f"Point -c at the config database, or create one with "
        f"`zfsbackup-config import`."
    )


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


def _assert_scripts_match_constants() -> None:
    """Fail loudly, before any database is touched, if the installed
    migration scripts disagree with `EXPECTED_HEAD` / `KNOWN_REVISIONS`.

    This is the other half of "`check_schema` reads no scripts": the
    constants are only trustworthy if something checks them against the
    scripts, and this runs first on every path that is about to *write*
    (`ensure_schema`'s upgrade branch, hence `upgrade_schema` and `import`).
    Raises `MigrationPackagingError`, not a `SchemaError`: a packaging defect
    (a new revision added without updating the constants, or a branched
    history),
    never an operator-fixable database state -- the same reasoning as
    `SchemaError`'s docstring.
    """
    cfg = Config()
    cfg.set_main_option("script_location", str(_MIGRATIONS_DIR))
    script = ScriptDirectory.from_config(cfg)
    code_heads = script.get_heads()
    if len(code_heads) != 1:
        # get_current_head() (singular) raises a bare
        # `CommandError: The script directory has multiple heads` for this
        # case instead of something with a named explanation.
        raise MigrationPackagingError(
            f"zfsbackup's installed migration scripts have "
            f"{len(code_heads)} heads {code_heads!r}; the config store only "
            f"supports a single, linear migration history. This is a "
            f"packaging defect, not something fixable by an operator."
        )
    if code_heads[0] != EXPECTED_HEAD:
        raise MigrationPackagingError(
            f"zfsbackup's installed migration scripts have head "
            f"{code_heads[0]!r} but migrate.EXPECTED_HEAD is "
            f"{EXPECTED_HEAD!r}. A migration was added or removed without "
            f"updating EXPECTED_HEAD/KNOWN_REVISIONS in migrate.py. This is "
            f"a packaging defect, not something fixable by an operator."
        )
    revisions = {rev.revision for rev in script.walk_revisions()}
    if revisions != set(KNOWN_REVISIONS):
        raise MigrationPackagingError(
            f"zfsbackup's installed migration scripts define revisions "
            f"{sorted(revisions)!r} but migrate.KNOWN_REVISIONS is "
            f"{sorted(KNOWN_REVISIONS)!r}. A migration was added or removed "
            f"without updating KNOWN_REVISIONS in migrate.py. This is a "
            f"packaging defect, not something fixable by an operator."
        )


def check_migration_packaging() -> None:
    """Public, read-only: raise `MigrationPackagingError` if the installed
    migration scripts disagree with `EXPECTED_HEAD` / `KNOWN_REVISIONS`;
    return `None` otherwise.

    It is the exact check `upgrade_schema` runs first (and for the same
    reasons: no database, no lock, no writes), exposed so a caller that does
    something irreversible BEFORE `upgrade_schema` -- the pre-migration
    backup -- can refuse a broken install first. Without it a deterministic
    packaging failure under a restart loop writes a fresh backup every cycle.
    """
    _assert_scripts_match_constants()


def _assert_foreign_keys_intact(connection: Connection) -> None:
    """`PRAGMA foreign_key_check`; raise `SchemaIntegrityError` if it returns
    any row. See that exception for why. Valid on an FK-off connection (the
    pragma reports violations regardless of the enforcement setting) and
    inside a transaction. Read-only."""
    rows = connection.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
    if not rows:
        return
    shown = "; ".join(
        f"table {r[0]!r} rowid {r[1]!r} -> missing parent in {r[2]!r}"
        for r in rows[:5]
    )
    more = f" (and {len(rows) - 5} more)" if len(rows) > 5 else ""
    raise SchemaIntegrityError(
        f"The migration left {len(rows)} foreign-key violation(s): "
        f"{shown}{more}. The migration was not committed; the database is "
        f"unchanged. This is a defect in the migration, not in your data."
    )


def check_schema(connection: Connection) -> str:
    """Check `connection`'s database against this installed package's
    Alembic head revision and return that head revision's id. **Never
    writes anything** -- no DDL, no DML, not even `alembic_version`.

    Decides from `EXPECTED_HEAD` / `KNOWN_REVISIONS` and reads **no migration
    scripts** (item 10.1, B1): the only things it touches are the database's
    `alembic_version` and, when none is recorded, its table list. The
    constants are checked against the scripts on the write paths
    (`_assert_scripts_match_constants`), not here.

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

    - `NotAConfigDatabase` if the database records no revision but holds
      tables that are not exactly the set some known revision leaves --
      someone else's SQLite database. Nothing is written, here or by any
      caller that gets this far.
    - `SchemaSplitBrain` if it records no revision and its tables are
      exactly the set of a known revision (the head's, or the baseline's
      six: `_TABLES_AT_REVISION`) -- `alembic_version` lost to an
      uncommitted `ensure_schema` call, or the tables created some other
      way. See that exception's docstring.
    - `SchemaVersionMismatch` if the database records a revision not in
      `KNOWN_REVISIONS` -- written by a *newer* zfsbackup package. See that
      exception's docstring.
    - `SchemaOutOfDate` if the database's recorded revision(s) are ones
      this package *does* recognise but are not `EXPECTED_HEAD`, or if it is
      an empty/brand-new file with no recorded revision and no tables
      (`check_schema` cannot perform the initial migration; the text points
      at `zfsbackup-config import`, not `migrate`). The text is neutral
      about who is asking -- the daemon appends its own sentence.
    - `SchemaColumnsMissing` if it is stamped at `EXPECTED_HEAD` but lacks a
      column a revision added (a wrong hand-made stamp); the message names
      the revision to stamp before `zfsbackup-config migrate`.

    Returns `EXPECTED_HEAD` with no exception only when the database's
    recorded heads are exactly `{EXPECTED_HEAD}`.
    """
    with _quiet_alembic():
        db_heads = MigrationContext.configure(connection).get_current_heads()
    if not db_heads:
        _refuse_unrecorded_tables(connection)

    unknown = _unknown_revisions(db_heads)
    if unknown:
        revision = unknown[0]
        raise SchemaVersionMismatch(
            f"Database schema is at revision {revision!r}, which this "
            f"installed zfsbackup package (migrations head "
            f"{EXPECTED_HEAD!r}) does not recognise. {_db_label(connection)} "
            f"was written by a newer zfsbackup package, or is not a "
            f"zfsbackup config database; nothing was changed. If it is "
            f"ours, upgrade the zfsbackup package installed on this host "
            f"before opening it again; otherwise point -c at the config "
            f"database."
        )

    if set(db_heads) != {EXPECTED_HEAD}:
        if db_heads:
            # Neutral text: the daemon appends its own restart sentence and
            # the CLI adds nothing; `migrate` is the remedy for both.
            word = "revision" if len(db_heads) == 1 else "revisions"
            raise SchemaOutOfDate(
                f"{_db_label(connection)} is at schema {word} "
                f"{', '.join(sorted(db_heads))}; this package expects "
                f"{EXPECTED_HEAD}. Run `zfsbackup-config migrate`.",
                recorded_revisions=tuple(sorted(db_heads)),
            )
        # No recorded revision and no tables: an empty or brand-new file.
        # `migrate` does not create databases, so the remedy is `import`.
        raise SchemaOutOfDate(
            f"{_db_label(connection)} has no recorded schema revision (it "
            f"holds no configuration tables); this package expects "
            f"{EXPECTED_HEAD}. Create "
            f"the configuration database with `zfsbackup-config import`.",
            recorded_revisions=(),
        )

    # Stamped at head: confirm the columns the revisions added are really
    # there (see `SchemaColumnsMissing`). One PRAGMA per table involved.
    missing = _missing_columns(connection, EXPECTED_HEAD)
    if missing:
        oldest = missing[-1][0]  # `_columns_required_by` is newest first
        listed = ", ".join(f"{table}.{column}" for _rev, table, column in missing)
        if _stamp_back_is_safe(oldest):
            remedy = (
                f"Fix this by stamping the revision before it (`alembic "
                f"stamp {_revision_before(oldest)}`) and then running "
                f"`zfsbackup-config migrate`; otherwise restore the "
                f"database from a backup."
            )
        else:
            # Revision `oldest` also created tables, which this database
            # holds: replaying it would fail with 'table already exists'.
            remedy = "Restore the database from a backup."
        raise SchemaColumnsMissing(
            f"{_db_label(connection)} is stamped at schema revision "
            f"{EXPECTED_HEAD!r} but lacks {listed}, which revision "
            f"{oldest!r} adds; the stamp does not match the tables. "
            f"{remedy} Nothing was changed."
        )

    return EXPECTED_HEAD


def ensure_schema(connection: Connection) -> str:
    """Bring `connection`'s database up to this installed package's
    Alembic head revision, in place, and return that head revision's id.

    **Reimplemented as `check_schema(connection)` plus `command.upgrade`,
    so the two can never drift.** `check_schema` alone decides whether the
    database needs anything done to it; this function's only additions are
    performing that migration when `check_schema` says `SchemaOutOfDate`
    (after `_assert_scripts_match_constants`), verifying it with `PRAGMA
    foreign_key_check` (`SchemaIntegrityError`), then re-checking to confirm
    and return the head. Every other exception `check_schema` can raise
    (`SchemaSplitBrain`, `SchemaVersionMismatch`) is not something a
    migration can fix, so it propagates unchanged -- `ensure_schema`
    catches `SchemaOutOfDate` specifically, nothing broader.

    **Migrations must never use `autocommit_block()`.** It commits the
    caller's transaction mid-migration, so the `PRAGMA foreign_key_check`
    below can no longer roll the migration back and a failure leaves a
    half-applied schema (`SchemaIntegrityError`'s "database is unchanged"
    promise is void). Say so in any revision that is tempted to.

    **If this raises after `command.upgrade` ran (including
    `SchemaIntegrityError`), the caller must roll back, not commit** -- the
    migration is sitting uncommitted in the caller's transaction on a
    db.py writer connection. `upgrade_schema` and `import_yaml` both do.

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

    A brand new (empty, tableless) database has no current heads at all, so
    `check_schema` raises `SchemaOutOfDate` for it just like any other
    behind-head database, and the migration below performs the full
    initial migration via `command.upgrade` -- this is `import`'s first-import
    path. A database that holds tables but records no revision never reaches
    the migration: `check_schema` raises `NotAConfigDatabase` /
    `SchemaSplitBrain`, which propagate unchanged.

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

    # Before any write: the constants `check_schema` just decided from are
    # only trustworthy if the scripts agree with them.
    _assert_scripts_match_constants()

    # `check_schema` above autobegan a transaction, so this holds on every
    # supported path; it is here so a refactor that hands Alembic a
    # transaction-less connection fails loudly instead of letting the DDL,
    # the version stamp and the FK check below commit piecemeal.
    if not connection.in_transaction():
        raise RuntimeError(
            "ensure_schema must run inside the caller's transaction; "
            "refusing to hand Alembic a connection with none open."
        )
    cfg = _build_config(connection)
    command.upgrade(cfg, "head")
    # B2: still inside the caller's transaction, before it can commit.
    _assert_foreign_keys_intact(connection)
    # Re-derive rather than trust anything computed above: this confirms
    # the upgrade actually landed (a mutant that silently swallowed
    # `command.upgrade`'s effect would fail this line, not just look
    # correct), and it is the reason `ensure_schema` and `check_schema`
    # cannot silently drift apart -- `ensure_schema` never computes "is
    # this database current" by any means other than calling
    # `check_schema`.
    return check_schema(connection)


def upgrade_schema(resolved: ResolvedConfigPath) -> SchemaUpgrade:
    """Migrate the EXISTING config database `resolved` addresses to head, in
    place, atomically, and report what happened (item 10.1; the CLI's
    `zfsbackup-config migrate`). Never creates a database -- a missing or
    zero-byte target is refused by `open_config_connection`'s preflight
    (`ConfigDbNotFound` / `ConfigDbNotADatabase`); only `import` may create.
    It only upgrades a database with a *recorded, known* older revision: one
    with no recorded revision is refused (`NotAConfigDatabase` /
    `SchemaSplitBrain` if it holds tables, `SchemaOutOfDate` pointing at
    `import` if it is empty) and left unchanged (item 10.4).

    **Callers must hold `writer_lock(resolved)`** (the CLI's `migrate` does):
    this function takes SQLite's write lock but not the cross-process writer
    lock that serialises it against `import` and `write_config`, and does not
    acquire it itself.

    Owns its connection end to end, so no caller can get the sequencing
    wrong (cf. `ensure_schema`'s binding note):

    - The connection comes from `open_config_connection(readonly=False,
      foreign_keys=False)`: FK enforcement OFF from the first byte (the
      pragma cannot be changed inside a transaction, and Alembic's SQLite
      batch migrations rebuild tables), and `db.py`'s writer-engine setup,
      i.e. pysqlite's implicit `BEGIN` disabled (`isolation_level = None`,
      SQLAlchemy's documented recipe for transactional DDL on pysqlite) and
      `BEGIN IMMEDIATE` as the transaction start.
    - `connection.begin()` is called explicitly, first, so `BEGIN
      IMMEDIATE` -- the write lock -- is taken *before* `check_schema` reads
      `alembic_version`. Two concurrent `migrate` runs therefore serialise:
      the second waits (`busy_timeout`), then sees the first's head and
      reports `changed=False` instead of racing it into `table already
      exists`. The DDL, the version stamp and the foreign-key check are all
      inside that one transaction: any failure rolls back to an untouched
      database.
    - `PRAGMA foreign_key_check` runs before the commit (via
      `ensure_schema`); any row -> rollback and `SchemaIntegrityError`.
    - `commit()` is the last statement; the connection is closed before this
      returns. A database already at head is rolled back, not committed
      (nothing was written).

    Raises `NotAConfigDatabase` / `SchemaSplitBrain` /
    `SchemaVersionMismatch` unchanged (a migration cannot fix them),
    `SchemaIntegrityError`, the `MigrationPackagingError` of
    `_assert_scripts_match_constants` (before the database is opened at
    all), `ConfigPathError` subclasses from the preflight, and
    `sqlalchemy.exc.OperationalError` ("database is locked") if the write
    lock could not be had within `busy_timeout`. Does not dispose the engine
    cache; the caller's process-exit (or `dispose_all()`) does.
    """
    # Cheap, needs no database, and must precede any lock: a script/constant
    # mismatch is a packaging bug to report before touching the file.
    _assert_scripts_match_constants()

    with open_config_connection(
        resolved, readonly=False, foreign_keys=False
    ) as conn:
        trans = conn.begin()  # db.py's `begin` listener issues BEGIN IMMEDIATE
        try:
            from_revs = tuple(
                sorted(MigrationContext.configure(conn).get_current_heads())
            )
            if not from_revs:
                # Not a "known recorded revision": never migrate (and so
                # never stamp) a database we cannot show is ours.
                _refuse_unrecorded_tables(conn)
                raise SchemaOutOfDate(
                    f"{_db_label(conn)} has no recorded schema revision "
                    f"(it holds no configuration tables), so there is nothing to "
                    f"migrate. "
                    f"Create the configuration database with "
                    f"`zfsbackup-config import`.",
                    recorded_revisions=(),
                )
            to_rev = ensure_schema(conn)
        except BaseException:
            trans.rollback()
            raise
        changed = from_revs != (to_rev,)
        if changed:
            trans.commit()  # LAST statement on this connection
        else:
            trans.rollback()
    return SchemaUpgrade(from_revs=from_revs, to_rev=to_rev, changed=changed)

"""Engine and session management for the zfsbackup config store (item 5).

This module owns every `Engine` the store ever creates, and it is the only
place in `zfsbackup/config/store/` allowed to create one. `models.py` defines the
schema, `mapper.py` converts rows to dataclasses, `migrate.py` takes an
already-open `Connection` -- none of them builds an engine. Keeping engine
construction in exactly one module is what makes the fork guarantee below
checkable by reading one file.

Fork safety is the reason this module exists
--------------------------------------------
`daemon.py` is a `multiprocessing` supervisor: it spawns 3-4 worker
processes in `BackupDaemon._start_workers` and re-spawns any that die from
`BackupDaemon._check_workers` -- the second fork point happens arbitrarily
far into the supervisor's life, so a one-time "dispose before `run()`"
ordering rule is *structurally insufficient*: **every** fork point must
call `dispose_all()` first, and `BackupDaemon._spawn` does, before every
`Process.start()`. That call is **mandatory**, not belt-and-braces.

The pid-keyed cache in `get_engine()` is the backstop for a fork point that
forgot, and ON ITS OWN IT IS NOT ENOUGH. Measured (a parent holding a pooled
reader connection at fork, a child that merely called `get_engine()` and
kept its new connection open, then the parent closing everything): the
parent's last close believed it was the last connection to the database,
checkpointed, and DELETED `-wal`/`-shm` under the connected child. The cause
is not file descriptors being shared but SQLite's per-PROCESS bookkeeping:
the child inherits, by `fork`, the parent's in-memory record of which locks
the connection holds and how many connections exist. A child connection
opened while that inherited record is still alive reuses it without taking
the kernel locks, so the parent can no longer see the child at all. (The
child closing the inherited connection is harmless in itself -- `fcntl`
locks are per-process, so it releases nothing the parent holds, and a
pooled connection was already rolled back at check-in. What is harmful is
the inherited record staying ALIVE while the child opens its own
connection.) `get_engine()` therefore drops EVERY inherited engine on its
first cache miss in a child, then runs `gc.collect()` once, before building
anything: `dispose(close=False)` leaves the old pool and its connection
records in a reference cycle, so the inherited `sqlite3.Connection` -- and
with it the inherited bookkeeping -- would otherwise live until the garbage
collector next happened to run. That guarantee holds for every cache key
unless something calls `gc.freeze()` (frozen objects are invisible to the
collector): any future `gc.freeze()` must come after `dispose_all()`.

On platforms where the `multiprocessing` start method is `fork` (the default
on Linux for CPython < 3.14, and still the default for the `fork` context
everywhere it exists), the child inherits every open file descriptor. A
SQLite connection inherited across a fork and then used by both processes
**corrupts the database without raising anything**. Measured, with a
deliberately pre-forked engine:

    parent pool: Connections in pool: 1
      child inherited pool: Connections in pool: 1
      child read via INHERITED connection: [(1,)]
      child: no error -- inherited fd used silently
      after dispose(close=False), child has fresh connection: [(1,)]
    parent still works: [(1,)]

Note also that SQLAlchemy 2.0's default pool for a *file* SQLite URL is
`QueuePool`, which **retains** connections after use: one `load_config()`
call before a fork leaves a live fd in the pool for every subsequent child
to inherit. (Under SQLAlchemy 1.x's `NullPool` default this was
self-healing; it is not any more.)

Pragmas and why a single `connect` listener is enough
-----------------------------------------------------
All pragmas are issued from one `connect` event listener, which fires once
per real DBAPI connection -- not once per `engine.connect()` checkout.
SQLite pragmas are connection-scoped state that survives pool
checkin/checkout, because the pool's reset-on-return is a `rollback()` and a
rollback does not reset pragmas. Measured over three sequential checkouts of
one engine:

    journal_mode -> ('wal',)
    0 fk: 1 jm: wal
    1 fk: 1 jm: wal
    2 fk: 1 jm: wal
    connect events: 1

- `journal_mode=WAL` is **persistent DB-file state**, not per-connection, so
  re-issuing it is a cheap no-op. Only WRITER engines issue it, and only
  against a file that is brand new or recognisably ours --
  `migrate.may_switch_to_wal`: no tables, or an `alembic_version` whose rows
  are all in `KNOWN_REVISIONS` (a database that is not ours must not be
  modified before a schema check can refuse it; item 10.5).
  Reader engines only *query* `PRAGMA journal_mode` and log a WARNING, once
  per engine, if it is not `wal` -- a reader that switched the mode rewrote
  the header of whatever file it was pointed at (item 10.4). Both are
  skipped for in-memory URLs, where the mode is `('memory',)`.
- `synchronous`, `busy_timeout`, `foreign_keys` and `query_only` are
  **per-connection** and must be issued on every new connection, which is
  exactly what the `connect` event does.
- Neither `journal_mode` nor `foreign_keys` can be changed inside a
  transaction. The `connect` event fires before any transaction can have
  begun, which is the other reason the pragmas live here and nowhere else.

Transaction start: readers and writers differ deliberately
-----------------------------------------------------------
**Never adopt SQLAlchemy's "BEGIN on connect" recipe for READER engines.**
This is not a blanket rule, and reading it as one would forbid the only fix
for the writer-side race described below -- so be exact about which side is
which. Readers get no `begin` handler and no `isolation_level` change.
Writers (file-backed ones) do: see `_install_begin_immediate`, which makes
every writer transaction start as `BEGIN IMMEDIATE` so that
`mapper.save_config`'s read of `GlobalSettings.generation` happens under the
write lock. Without it two concurrent writers silently commit two different
configs carrying the same generation number, and items 15/17 -- which poll
that counter to detect "config changed" -- never notice the second one.

The reader rule, measured: with a reader session open and a second engine
writing concurrently,

    select: [(1,)]
    in_transaction (sqlalchemy): True | pysqlite in_transaction: False
    concurrent write while reader session open: OK

SQLAlchemy reports `in_transaction() == True`, but the DBAPI connection is
*not* in a real SQLite transaction, because pysqlite does not emit `BEGIN`
for a bare `SELECT`. That discrepancy is precisely what makes "readers never
block and are never blocked" true under WAL. If a future change adds the
documented `BEGIN`-on-connect recipe (for serializable isolation), every
reader session pins a WAL read snapshot for its whole lifetime, and the
`-wal` file grows without bound because a checkpoint cannot pass a live
reader. Any long-lived reader (the supervisor's later config polling, if
it is added) would make this a slow-motion disk exhaustion bug, not a test
failure. The writer side does not have that problem: writer sessions are short-lived CLI commands, and a write
transaction has to hold the lock anyway.

WAL sidecars
------------
WAL creates `-wal` and `-shm` files next to the database on first write, and
checkpoints them away when the last connection closes cleanly:

    sidecars: ['p.db', 'p.db-shm', 'p.db-wal']
    sidecars after close: ['p.db']

Consequences for anyone packaging, permissioning, copying or cleaning up a
config DB:

- A WAL database needs **write permission on the containing directory**
  whenever the `-shm` has to be (re)created -- the first-reader-after-writer-
  exit and post-crash cases. Measured: `-wal` + `-shm` both present, with
  read-only files in a read-only directory, opens fine even `mode=ro`;
  already-WAL with `-shm` missing (checkpointed away by a prior clean close)
  in a read-only directory fails -- with `OperationalError: attempt to
  write a readonly database`, **not** the `unable to open database file`
  this docstring previously claimed. Never diagnose this case by matching
  SQLite's message text: item 6's preflight (`config/store/paths.py`)
  works from `stat` and the file header instead, precisely because this
  message is not the stable, self-explanatory one it looks like.
- Any copy or backup of the DB must take **all three** files, or use
  `VACUUM INTO`. Copying `config.db` alone while a `-wal` is pending
  silently loses the most recent commits.
- `scenarios/` cleanup and `.gitignore` must cover `*-wal` / `*-shm`.

Read-only is an accident guard, not a privilege boundary
--------------------------------------------------------
See `session_scope`/`session_for_engine`. `PRAGMA query_only=ON` is
reversible by the very connection that set it (measured: yes), so it stops
mistakes, not malice. It also governs changes to database *content* only: a
"read-only" engine still opens the file read-write. Since item 10.4 it no
longer switches the journal mode, though: it only QUERIES `PRAGMA
journal_mode` and warns (once per process and database path) if the file is not in WAL, because
a reader that issued `journal_mode=WAL` rewrote the header of any
rollback-journal database it was pointed at -- including one that is not a
zfsbackup database (measured with `-c other.db export`). The first WRITE by
`zfsbackup-config` switches the file to WAL.

History, kept because `paths.check_config_db` still encodes the conservative
half of it: before 10.4 a `chmod 444 config.db` that had never been opened in
WAL mode could not be opened even by a `readonly=True` engine (the pragma had
to CHANGE the header, which needs write access, and raised `OperationalError:
attempt to write a readonly database` at connect). That failure is gone for
readers, but the preflight still refuses a non-writable, not-yet-WAL file for
the daemon: such a file is a provisioning mistake (the daemon would run on a
rollback-journal database, where it and the CLI writer block each other),
and refusing early is cheaper than diagnosing it later.

Note for item 8's `ensure_schema` wrapper
-----------------------------------------
When a URL-taking wrapper around `migrate.ensure_schema()` is written, it
must open a `Connection` from `get_engine(...)` and **inject** it into
Alembic (`config.attributes["connection"]`). Setting `sqlalchemy.url` (or
`ZFSBACKUP_DB_URL`) instead would send `migrations/env.py` down its
`engine_from_config` fallback, building a second, uncached engine that
bypasses every pragma installed here -- no WAL, no `busy_timeout`, no
`foreign_keys` -- silently and with no error. That wrapper should pass
`foreign_keys=False`: SQLite `batch_alter_table` migrations recreate tables
and must run with FK enforcement off, which is the only reason the
`foreign_keys` parameter exists on `make_engine`/`get_engine` today.

**Sequence it, do not nest it.** That FK-off engine is a second *writer*
engine on the same file -- different cache key, own pool, same write lock --
and writer transactions here open with `BEGIN IMMEDIATE`. Opening it inside
a live writer session self-deadlocks in a single thread: measured, a
`get_engine(url, foreign_keys=False).connect()` + `ensure_schema` nested in
a `session_scope(url)` block fails after 5.2s with `database is locked`, on
a fresh database, deterministically. Run `ensure_schema` + `commit()` +
close first, as its own phase, and only then open the `save_config` session
-- or do both over one connection. See `session_for_engine` for the general
rule (at most one live writer connection per file per process).
"""

from __future__ import annotations

import gc
import logging
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, Tuple, Union

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, URL, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session
from sqlalchemy.pool import SingletonThreadPool, StaticPool

logger = logging.getLogger(__name__)

# Pools that hand the SAME DBAPI connection to every concurrent checkout in
# a thread. `_install_begin_immediate` must not be installed on these: a
# second `Session` would issue `BEGIN IMMEDIATE` on a connection already in
# a transaction (`cannot start a transaction within a transaction`).
# Measured on both. Only in-memory URLs reach them.
_SHARED_CONNECTION_POOLS = (StaticPool, SingletonThreadPool)

__all__ = [
    "ReadOnlySessionError",
    "dispose_all",
    "get_engine",
    "make_engine",
    "session_for_engine",
    "session_scope",
    "url_for_path",
]


class ReadOnlySessionError(RuntimeError):
    """Raised when a write is attempted through a read-only session.

    Layer 1 of read-only enforcement is `PRAGMA query_only=ON`, which SQLite
    itself enforces -- but it surfaces as `OperationalError: attempt to
    write a readonly database`, which reads like a disk-permissions or
    mounted-read-only problem and sends the reader looking at the
    filesystem. This exception exists so that the *first* symptom of a
    coding error (a worker trying to write) names the offending ORM
    entities and the fact that the session was deliberately read-only.
    """


def _is_memory_url(parsed: URL) -> bool:
    """True for SQLite URLs that address a transient in-memory database.

    Decided from the **parsed** URL, never from the raw string. `sqlite://`
    and `sqlite:///` are standard spellings of an in-memory database and
    contain neither `:memory:` nor `mode=memory`; a raw-substring test sent
    them down the file branch, which loses `StaticPool` *and* the explicit
    `check_same_thread=False` (the pysqlite dialect supplies `True` for
    memory URLs and `False` only for file URLs, so that override is
    load-bearing here, not redundant). Measured before the fix:

        'sqlite://'  _is_memory=False  pool=SingletonThreadPool
        sqlite:// from another thread -> OperationalError: no such table: t

    -- the second thread sees an *empty* database, which is exactly the
    hazard `StaticPool` exists to prevent, in the `ApiWorker`-Flask-thread
    scenario this module reasons about elsewhere.

    `parsed.database` is `None` for `sqlite://`, `''` for `sqlite:///`, and
    `':memory:'` for the explicit spelling; `mode=memory` (with or without
    `cache=shared`) is the URI form.
    """
    database = parsed.database
    if database is None or database == "" or ":memory:" in database:
        return True
    return parsed.query.get("mode") == "memory" or "mode=memory" in str(parsed)


def url_for_path(path: Union[str, Path]) -> str:
    """Build the canonical SQLite URL for a filesystem path.

    Use this rather than hand-formatting: `make_engine("/var/lib/zfsbackup/
    config.db")` (a path where a URL is wanted -- the likely mistake once
    item 6 owns path resolution) raises SQLAlchemy's `ArgumentError` from
    inside `make_url`, and the cache key in `get_engine` is the raw string,
    so `sqlite:///config.db` and `sqlite:////abs/config.db` for one and the
    same file are two entries with two pools -- two writers racing each
    other on one database, which is the one thing the store's single-writer
    design rules out. `resolve()` here is what makes the key canonical, and
    it must keep working on a path that does not exist yet (`Path.resolve()`
    is non-strict) -- both the default-path error message (item 6) and
    item 10's `import` on a brand-new file depend on that.

    **Round-tripped through `make_url` and checked**, because a `?` in the
    path is silently swallowed rather than rejected: SQLAlchemy's URL
    grammar splits the string there and treats everything after it as query
    parameters, so `url_for_path("/tmp/a?b.db")` used to build a URL whose
    `.database` was `/tmp/a` -- a different, and possibly pre-existing,
    file. Measured:

        make_url("sqlite:////tmp/a?b.db").database == "/tmp/a"

    `#` and spaces round-trip correctly and are accepted. This module
    deliberately does **not** percent-encode a `?` to make it round-trip
    instead of rejecting it -- a config DB path containing `?` is a mistake
    to catch, not a use case to support, and silently re-encoding would
    reintroduce the two-spellings-one-file problem `get_engine`'s docstring
    describes, just one layer up (a percent-encoded and a literal spelling
    of the same path would again be two cache entries for one file).
    """
    resolved = Path(path).resolve()
    url = f"sqlite:///{resolved}"
    parsed = make_url(url)
    if parsed.database != str(resolved):
        raise ValueError(
            f"path {str(path)!r} cannot be represented as a SQLite URL "
            f"without ambiguity: building {url!r} and re-parsing it with "
            f"make_url() yields database={parsed.database!r}, not "
            f"{str(resolved)!r}. This means the path contains a character "
            "-- most likely '?' -- that SQLAlchemy's URL grammar treats as "
            "a delimiter, so the resulting URL silently addresses a "
            "different file. Rename the file; this function deliberately "
            "does not percent-encode paths to work around it."
        )
    return url


def _switch_to_wal(cursor, engine: Engine) -> None:  # noqa: ANN001
    """Writer engines only: `PRAGMA journal_mode=WAL`, and complain loudly if
    SQLite leaves the mode unchanged."""
    # Persistent DB-file state, so this is a no-op after the first time.
    # `journal_mode` RETURNS A ROW; leaving it unfetched leaves the cursor in
    # an odd state, so fetch it.
    cursor.execute("PRAGMA journal_mode=WAL")
    row = cursor.fetchone()
    # ...and CHECK it. For SOME ways WAL can fail to engage, SQLite does not
    # raise -- it returns the UNCHANGED mode (measured: a filesystem without
    # the shared-memory primitives WAL needs, e.g. some network mounts). This
    # branch exists for those.
    #
    # It is NOT reached for a never-yet-WAL, permission-denied file. An
    # earlier version of this comment claimed otherwise; corrected during
    # item 6a's review, measured: `PRAGMA journal_mode=WAL` against such a
    # file raises `OperationalError: attempt to write a readonly database` at
    # the `execute()` above, before this `fetchone()` is ever reached. That
    # case is instead refused before any engine is even built, by
    # `config/store/paths.py`'s `check_config_db` (item 6a) -- see that module
    # for the exact rule.
    #
    # Every claim this module rests on -- readers never block, the single
    # writer serialises on the write lock, the busy_timeout story -- silently
    # evaporates in the case this branch DOES catch, so say so loudly rather
    # than degrading in silence.
    if row is None or str(row[0]).lower() != "wal":
        logger.warning(
            "SQLite refused WAL journal mode for %s (still %r). "
            "Readers will now block writers and vice versa, and "
            "the store's concurrency guarantees do not hold. "
            "This usually means the database file is read-only "
            "or lives on a filesystem without WAL support "
            "(some network mounts).",
            engine.url.render_as_string(hide_password=True),
            None if row is None else row[0],
        )


def _install_pragmas(
    engine: Engine, *, readonly: bool, foreign_keys: bool, memory: bool
) -> None:
    """Attach the one `connect` listener that configures every connection.

    Fires once per real DBAPI connection; see the module docstring for the
    measurement showing the pragmas stay in force across pool checkouts.
    Registered on the `Engine`, so `Engine.dispose(close=False)` carries it
    over to the rebuilt pool automatically (SQLAlchemy passes
    `_dispatch=self.dispatch` to `Pool.recreate()`). **Never re-register
    after a dispose** -- that would double-fire every pragma below.
    """

    # The "not in WAL mode" warning is latched per (process, path) in
    # `_NOT_WAL_WARNED`, not per engine or pooled connection.

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, connection_record):  # noqa: ANN001
        # SQLAlchemy 2.0's `_ConnectionRecord.__connect` never closes the
        # DBAPI connection when a `connect` listener raises (measured on
        # 2.0.51, pool/base.py): the connection is left open inside a
        # reference cycle that neither the pool nor `dispose_all()` can see,
        # one leaked fd per failed connect until the cyclic GC happens to run.
        # A valid-header/corrupt-body file makes the pragmas below raise
        # ("database disk image is malformed"), so close it ourselves. This
        # matters most for a process that loads config and then forks.
        try:
            _apply_sqlite_pragmas(dbapi_connection)
        except BaseException:
            try:
                dbapi_connection.close()
            except Exception:
                pass
            raise

    def _apply_sqlite_pragmas(dbapi_connection):  # noqa: ANN001
        cursor = dbapi_connection.cursor()
        try:
            # FIRST, before any statement that can contend for a lock.
            # Under WAL a reader is never blocked, but two writers still
            # serialise on the write lock (all the more so now that writer
            # transactions open with `BEGIN IMMEDIATE`), and waiting beats
            # raising `database is locked` instantly.
            # pysqlite's own `timeout=5.0` default already sets
            # busy_timeout=5000 at connect, so today this line is
            # confirmatory rather than causative -- but the moment anyone
            # passes `connect_args={"timeout": ...}`, or a future dialect
            # stops defaulting it, every statement issued before this one
            # would run with a zero busy timeout and fail instantly with
            # `database is locked` instead of waiting.
            cursor.execute("PRAGMA busy_timeout=5000")
            if not memory and readonly:
                # Readers OBSERVE the journal mode; they never set it. A
                # read-only engine that issued `journal_mode=WAL` rewrote the
                # file header (bytes 18-19) of any rollback-journal database
                # it was pointed at -- including one that is not a zfsbackup
                # config database at all (`zfsbackup-config -c other.db
                # export` converted it; measured). "Read-only" has to mean
                # the file is not modified, so a non-WAL file is only
                # reported. A writer engine (below) switches it on the next
                # write -- but only if it is recognisably ours, so the
                # warning must not promise a switch (a foreign file never
                # gets one).
                cursor.execute("PRAGMA journal_mode")
                row = cursor.fetchone()
                mode = None if row is None else str(row[0]).lower()
                warn_key = (os.getpid(), str(engine.url.database))
                if mode == "wal":
                    # Back in WAL: forget the latch so a later switch away
                    # from it is reported again (cheap flip-flop handling).
                    _NOT_WAL_WARNED.pop(warn_key, None)
                elif _first_not_wal_warning(warn_key):
                    logger.warning(
                        "%s is not in WAL mode (journal_mode=%s); readers and "
                        "writers will block each other. (A zfsbackup-config "
                        "write switches a zfsbackup config database to WAL.)",
                        engine.url.database or engine.url.render_as_string(hide_password=True),
                        "unknown" if mode is None else mode,
                    )
            elif not memory:
                # Never switch a database that is not ours. `migrate`/`import`
                # pointed at a foreign SQLite file must leave it
                # byte-for-byte alone until they have decided to refuse it,
                # and `journal_mode=WAL` rewrites the header (bytes 18-19) --
                # before any schema check can run. "Ours" is decided by
                # `migrate.may_switch_to_wal`: a file with no tables, or one
                # whose `alembic_version` rows are all in `KNOWN_REVISIONS`
                # -- the rule `check_schema` uses. A stamp-only file (an
                # EMPTY `alembic_version`) is not switched either (item
                # 10.6): `migrate` refuses it and promises to leave it
                # unchanged; `import` migrates it and the next engine
                # switches it. Merely HAVING an `alembic_version` is not enough (item
                # 10.5, M-1R): a foreign Alembic-managed database has one, so
                # does a foreign file with an empty one, and both were
                # switched and then refused with a false "nothing was
                # changed". Imported lazily: `migrate` imports `paths`, which
                # imports this module.
                from zfsbackup.config.store.migrate import may_switch_to_wal

                if may_switch_to_wal(cursor):
                    _switch_to_wal(cursor, engine)
                else:
                    logger.debug(
                        "not switching %s to WAL: it is not a recognisable "
                        "zfsbackup config database",
                        engine.url.render_as_string(hide_password=True),
                    )
            # D2: FULL for the writer, NORMAL for readers. Under NORMAL a
            # power loss can silently discard a commit the CLI already
            # reported as successful -- one extra fsync on a
            # human-driven command is a fair price for not lying to the
            # operator. Readers durability-wise have nothing to lose.
            cursor.execute(
                "PRAGMA synchronous=NORMAL" if readonly else "PRAGMA synchronous=FULL"
            )
            # Per-connection and OFF by default in SQLite. Every FK in
            # `models.py` -- notably the composite
            # `fk_retention_rules_dataset_remote` -- is inert without this.
            cursor.execute(
                "PRAGMA foreign_keys=ON" if foreign_keys else "PRAGMA foreign_keys=OFF"
            )
            if readonly:
                # Layer 1 of read-only enforcement (layer 2 is
                # `ReadOnlySessionError`, see `_install_readonly_guard`).
                # Issued LAST, defensively, so it cannot interfere with the
                # pragmas above. Measured on SQLite 3.53.4:
                # `journal_mode=WAL` still succeeds with `query_only=ON`
                # (it governs changes to database *content*, not to
                # file-format state) -- which is why readers must not be
                # trusted to leave the mode alone and simply never issue
                # it; the ordering here is hygiene.
                # Also measured: `query_only` is reversible by the very
                # connection that set it. This is an accident guard, not a
                # privilege boundary.
                cursor.execute("PRAGMA query_only=ON")
        finally:
            cursor.close()


def _install_begin_immediate(engine: Engine) -> None:
    """Make every transaction on a **file-backed writer** engine start as
    `BEGIN IMMEDIATE`, i.e. take SQLite's write lock up front.

    This exists to close a lost-update race on `GlobalSettings.generation`
    (`models.py`, consumed by `mapper.save_config`). Without it, and
    *because* of the very property that makes readers non-blocking, two
    concurrent writers silently produce two different configs carrying the
    same generation number: a bare `SELECT` takes no snapshot and no lock,
    `session_for_engine` opens no transaction on entry, and pysqlite emits
    the real `BEGIN` only at the first DML -- which in `save_config` is the
    `DELETE`, several steps *after* it reads the counter. Measured, two
    `session_scope(url)` writers on one file:

        A read generation: 5 | pysqlite in_transaction: False
        B read generation: 5 | pysqlite in_transaction: False
        A committed generation 6
        B committed generation 6 -- NO conflict raised
        final (generation, prefix): (6, 'from-B')

    Items 15/17 poll `generation` to detect "config changed", so a worker
    that already saw 6 never reloads B's config: the operator sees B in the
    database and the daemon runs A forever, with no error anywhere. With
    `BEGIN IMMEDIATE` the second writer instead waits on the write lock for
    `busy_timeout` and then fails loudly with `database is locked`:

        B blocked then failed after 5.2s: database is locked
        B after A committed reads gen = 6

    Two deliberate scope limits:

    - **Writers only.** Readers must keep their current behaviour; see the
      module docstring's BEGIN-on-connect section for why pinning a WAL
      read snapshot for the life of a long-lived worker session would let
      the `-wal` grow without bound.
    - **Only on pools that hand out a distinct connection per checkout**
      (`make_engine` decides; the predicate is the pool class, not the URL
      shape). `StaticPool` and `SingletonThreadPool` both return the *same*
      DBAPI connection to every concurrent checkout in a thread, so a
      second `Session` on such an engine would run `BEGIN IMMEDIATE` on a
      connection already inside a transaction: `OperationalError: cannot
      start a transaction within a transaction` (measured, both pools).
      In-memory databases are the only thing that lands on those pools --
      and `make_engine` rejects the one in-memory spelling where that
      would hide a real race (`cache=shared`; see there).

      This predicate used to read "file URLs only, because an in-memory
      database has one connection and nothing to serialise". The second
      half of that was false: `mode=memory&cache=shared` is also an
      in-memory URL, and two engines on one are two real connections onto
      one database. Narrowing on the pool says what is actually true.

    `isolation_level = None` is required with this: it turns off pysqlite's
    legacy implicit `BEGIN` so ours is the only one. It is SQLAlchemy's
    documented recipe for exactly this, and it also means Alembic DDL run
    over such a connection is transactional (see `migrate.py`).
    """

    @event.listens_for(engine, "connect")
    def _disable_pysqlite_implicit_begin(  # noqa: ANN001
        dbapi_connection, connection_record
    ):
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def _begin_immediate(conn):  # noqa: ANN001
        conn.exec_driver_sql("BEGIN IMMEDIATE")


def make_engine(
    url: str, *, readonly: bool = False, foreign_keys: bool = True
) -> Engine:
    """Build a new, **uncached** SQLite `Engine` with the store's pragmas.

    For a config database resolved from `-c`/`ZFSBACKUP_CONFIG`/the
    default path, prefer `config/store/paths.py`'s `open_config_session`
    / `open_config_connection` instead of calling this (or `get_engine`)
    directly on a hand-built URL -- they run `check_config_db`'s preflight
    first, which this function cannot: `readonly=True` on a URL for a path that does not
    yet exist silently creates a zero-byte file (see that module's
    docstring for the measurement).

    Prefer `get_engine()`: this function has no pid awareness, so an engine
    it returns must not be allowed to cross a fork. It exists for tests and
    for callers that genuinely want an isolated, short-lived engine.

    `readonly=True` adds `PRAGMA query_only=ON` (see `session_for_engine`).
    `foreign_keys=False` is a deliberate, named opt-out for Alembic batch
    migrations, which recreate tables and must run with FK enforcement off;
    nothing else should pass it.

    `mode=ro` URIs are rejected. They interact badly with WAL: a read-only
    *open* cannot create the `-shm` file, so the first reader to arrive
    after a writer exited (or after a crash) fails with a bare `unable to
    open database file`. Read-only-ness here is a pragma on a normally
    opened database, not a different open mode.
    """
    try:
        parsed = make_url(url)
    except ArgumentError as exc:
        # The likely mistake once item 6 owns path resolution is passing a
        # *path* where a URL is wanted. SQLAlchemy's ArgumentError escapes
        # ahead of this function's own ValueErrors and says nothing about
        # the fix, so name it.
        raise ValueError(
            f"not a SQLAlchemy URL: {url!r}. If this is a filesystem path, "
            "use zfsbackup.config.store.db.url_for_path(path) to build the URL."
        ) from exc

    backend = parsed.get_backend_name()
    if backend != "sqlite":
        # Everything below -- the pool choice, every pragma, and therefore
        # BOTH layers of read-only enforcement -- is SQLite-specific. A
        # non-SQLite URL would either blow up on the first `PRAGMA` with a
        # confusing dialect error, or (worse, if someone "fixed" that by
        # making the pragmas conditional) hand back a `readonly=True`
        # session with nothing enforcing it. Fail here, plainly. A future
        # Postgres move goes through `mapper.py`'s dataclass boundary and
        # gets its own engine module; it does not go through this function.
        raise ValueError(
            f"zfsbackup.config.store.db only supports SQLite URLs, got {backend!r} "
            f"({url!r})"
        )

    # Both forms: SQLAlchemy parses `?mode=ro` into `.query`, but check the
    # raw string too so an odd spelling cannot slip past the parser.
    if parsed.query.get("mode") == "ro" or "mode=ro" in url:
        raise ValueError(
            f"refusing SQLite URL with mode=ro: {url!r}. A read-only open "
            "cannot create the WAL -shm file, so the first reader after a "
            "writer exits (or after a crash) fails with 'unable to open "
            "database file'. Use readonly=True instead, which sets PRAGMA "
            "query_only=ON on a normally opened database."
        )

    memory = _is_memory_url(parsed)
    if memory and (parsed.query.get("cache") == "shared" or "cache=shared" in url):
        # Rejected rather than merely unsupported, because this is the one
        # URL shape on which a two-writer test would demonstrate a real
        # lost update while reporting green. Two engines on one shared-
        # cache in-memory database are two genuine connections, so the
        # pre-`BEGIN IMMEDIATE` race is reproducible there -- measured:
        #
        #     reads: {'A': 5, 'B': 5}  final generation: 6   <- update lost
        #
        # And it cannot be fixed the way the file case was. Shared-cache
        # mode uses table-level locking that raises SQLITE_LOCKED, which
        # the busy handler does not retry, so installing `BEGIN IMMEDIATE`
        # here turns every contended statement into an immediate `database
        # table is locked` -- measured on two sessions of ONE engine, and
        # on an uninvolved third connection afterwards. Neither behaviour
        # is one this store should offer, so the URL does not build.
        raise ValueError(
            f"refusing shared-cache in-memory SQLite URL: {url!r}. Two "
            "engines on one shared-cache in-memory database are two real "
            "connections with no write-lock serialisation between them "
            "(writes are silently lost), and BEGIN IMMEDIATE cannot fix "
            "it there -- shared-cache locking raises SQLITE_LOCKED, which "
            "busy_timeout does not retry. Use a private in-memory URL "
            "('sqlite:///:memory:') for single-connection tests, or a "
            "real file (url_for_path) when more than one connection is "
            "involved."
        )

    if memory:
        # Without StaticPool a second pooled connection to `:memory:` sees
        # an *empty* database rather than the one `create_all` populated.
        engine = create_engine(
            url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    else:
        # Deliberately all-defaults: the pysqlite dialect already passes
        # `check_same_thread=False` for file URLs (verified:
        # `create_connect_args -> {'check_same_thread': False}`), so
        # passing it again is redundant, and `QueuePool` is the right pool
        # for a file DB. This is also why item 8's `ApiWorker` Flask thread
        # will not hit "SQLite objects created in a thread...".
        engine = create_engine(url)

    _install_pragmas(
        engine, readonly=readonly, foreign_keys=foreign_keys, memory=memory
    )
    # Writers, on pools that give each checkout its own connection. See
    # `_install_begin_immediate` for both scope limits, the lost-update
    # race this closes, and why the predicate is the POOL and not the URL.
    if not readonly and not isinstance(engine.pool, _SHARED_CONNECTION_POOLS):
        _install_begin_immediate(engine)
    return engine


# --------------------------------------------------------------------------
# Pid-keyed engine cache (item 5b) -- the fork guarantee.
# --------------------------------------------------------------------------
# Key `(url, readonly, foreign_keys)`, value `(owner_pid, engine)`.
#
# `readonly` is part of the KEY, not just a pragma: a read-only and a
# read-write engine for the same URL must be separate engines with separate
# pools, or `query_only=ON` leaks onto the writer's connections through a
# shared pool. `foreign_keys` likewise -- an Alembic FK-off connection must
# never be handed to application code.
_ENGINES: Dict[Tuple[str, bool, bool], Tuple[int, Engine]] = {}

# Guards `_ENGINES` only. `ApiWorker` runs Flask in a thread inside a worker
# process (`workers.py`), so two threads can reach `get_engine()` at once;
# without this, both could build an engine and one would be leaked
# unreferenced with an open fd.
#
# A RLOCK, deliberately. `get_engine` and `dispose_all` both run their
# `gc.collect()` INSIDE the lock: `get_engine` must collect before it builds
# anything (moving the collect out would hand the new engine out before the
# inherited connection is gone, or let another thread build in between --
# exactly the window the collect exists to close), and `dispose_all` must not
# return -- or let another thread's `get_engine` repopulate the cache -- while
# inherited connections it just dropped are still alive. A collection runs
# arbitrary finalizers on this thread, and one that reached
# `get_engine`/`dispose_all` would self-deadlock on a plain lock; the
# re-entrant lock makes that merely re-entrant. (Both functions leave
# `_ENGINES` consistent before they collect, so re-entry is safe.)
_ENGINES_LOCK = threading.RLock()


# "Not in WAL mode" is logged once per (process, database path), NOT once per
# engine: the daemon disposes every engine after each poll (every 10 s), and a
# per-engine latch re-warned on every new engine -- about 8,640 identical
# WARNINGs a day for a database someone switched to `journal_mode=DELETE`.
# Keyed by pid so a forked child (which inherits the dict) logs for itself
# rather than inheriting the parent's "already said it"; the at-fork hook below
# also clears it so the child does not carry the parent's keys around.
_NOT_WAL_WARNED: Dict[Tuple[int, str], object] = {}


def _first_not_wal_warning(key: Tuple[int, str]) -> bool:
    """True exactly once per `key` until it is popped. `setdefault` is atomic
    under the GIL, so two threads' first connections cannot both win."""
    token = object()
    return _NOT_WAL_WARNED.setdefault(key, token) is token


def _reinit_lock_after_fork() -> None:
    """Replace `_ENGINES_LOCK` in a freshly forked child.

    A fork that happens while another thread holds the lock leaves the child
    with a lock nobody will ever release. This handler ONLY replaces the
    lock; it deliberately does not touch `_ENGINES`, because disposing an
    engine from a fork handler would run connection teardown at an
    arbitrary point. The pid check in `get_engine()` owns the engines.
    """
    global _ENGINES_LOCK
    _ENGINES_LOCK = threading.RLock()
    _NOT_WAL_WARNED.clear()


if hasattr(os, "register_at_fork"):  # POSIX only
    os.register_at_fork(after_in_child=_reinit_lock_after_fork)


def get_engine(
    url: str, *, readonly: bool = False, foreign_keys: bool = True
) -> Engine:
    """Return the cached `Engine` for `(url, readonly, foreign_keys)`,
    building it lazily **in the calling process**.

    This is the fork *backstop*. Nothing in this module builds an engine at
    import time, and no engine is reachable from module or class state
    except through this cache, which is keyed by owning pid. If the cached
    engine was built by a different process -- i.e. this process inherited
    it across a fork -- then on the first cache miss in this process EVERY
    inherited entry, whatever its key, is discarded (not just the requested
    key's: a child that asks for a different `(url, readonly,
    foreign_keys)` than the parent cached would otherwise leave the
    inherited engine strongly referenced forever, so the collect below could
    never free its connection -- reproduced on 3.11-3.14), followed by one
    `gc.collect()` before anything is built. So an accidental pre-fork
    engine is survivable for every key -- **unless something calls
    `gc.freeze()` between filling the cache and forking**, which hides the
    inherited connection from the collector (reproduced); any `gc.freeze()`
    must come after `dispose_all()`. It is **not** a substitute for `dispose_all()` at the fork
    point, which every fork site must still call: the supervisor forks in
    `BackupDaemon._start_workers` *and* in `BackupDaemon._check_workers`
    (on every worker crash, arbitrarily late in its life, potentially after
    it has itself read the DB), and `_spawn` runs `dispose_all()` before
    each. The cache and the gc exist for the day a fork site forgets.

    Discard-and-rebuild, never warn-and-continue: a warning would imply the
    caller may proceed with an inherited connection, which is the exact
    thing that corrupts the file.

    **The cache key is the URL string as given**, not a canonicalised form,
    so `sqlite:///config.db` and `sqlite:////abs/config.db` for one file are
    two entries with two pools -- i.e. two writers racing on one database.
    Callers must funnel paths through `url_for_path()` rather than
    hand-formatting. `get_engine` deliberately does not canonicalise for
    them: not every SQLite URL is a path (`:memory:`, `file:...?uri=true`),
    and silently resolving symlinks at this layer would surprise -- the
    normalisation belongs where a path is turned into a URL, once.

    What this cannot fix: a `Connection` or `Session` that was already
    checked out before the fork holds its DBAPI connection directly, and
    `dispose(close=False)` does not touch checked-out connections. That is
    prevented by API shape instead -- no public function here returns a bare
    long-lived `Session`; sessions only exist inside a context manager.
    """
    key = (url, readonly, foreign_keys)
    pid = os.getpid()

    with _ENGINES_LOCK:
        cached = _ENGINES.get(key)
        if cached is not None and cached[0] == pid:
            return cached[1]
        cached = None

        # Everything left in the cache that this process did not build was
        # inherited across a fork. Purge ALL of it, not just `key`'s entry:
        # an inherited engine under any other key stays strongly referenced
        # by `_ENGINES`, so no collection could ever free its connection.
        # Own-pid entries cannot coexist with inherited ones (the first miss
        # in this process purges before it builds), so a hit above is safe.
        purged = False
        for stale_key, (owner_pid, engine) in list(_ENGINES.items()):
            if owner_pid == pid:
                continue
            # ------------------------------------------------------------
            # THE SINGLE MOST IMPORTANT LINE IN THIS MODULE.
            # ------------------------------------------------------------
            # `close=False`. We are in a forked child holding a pool of
            # connections whose file descriptors are *shared* with the
            # parent. A plain `dispose()` explicitly runs pysqlite's close
            # path -- rollback, lock release, `sqlite3_close_v2()` -- and
            # fires the pool's `close` event hooks, against a database the
            # parent believes it exclusively owns.
            #
            # Be precise about what `close=False` does and does not buy,
            # because the difference licenses different things. It replaces
            # the pool (`Pool.recreate()`) and de-references the old one;
            # it does NOT keep the old connections open. Dropping the last
            # reference frees the old pool's `_ConnectionRecord`s, and
            # CPython's `sqlite3.Connection` deallocator then runs
            # `sqlite3_close_v2()` on the inherited fd anyway. Traced in a
            # real fork child:
            #
            #   [child] calling get_engine() ...
            #   [pid 97379] sqlite3.Connection DEALLOC
            #               (sqlite3_close_v2 runs on the INHERITED fd)
            #
            # BUT "frees ... when the last reference drops" is the catch:
            # `dispose(close=False)` leaves the old pool and its connection
            # records in a reference cycle, so the inherited
            # `sqlite3.Connection` is NOT freed here -- it lives until the
            # garbage collector next runs, possibly long after the child
            # has opened its own connection. In that window SQLite's
            # inherited per-process lock bookkeeping is shared by the old
            # and new connections, so the child's connection skips the
            # kernel locks the parent then cannot see, and the parent's
            # last close checkpoints and DELETES `-wal`/`-shm` under the
            # connected child (reproduced; see the module docstring).
            # Hence the explicit `gc.collect()` below: it forces the
            # inherited connection to be closed NOW, before the rebuilt
            # engine can open anything. Closing an inherited connection
            # releases no lock the parent holds (`fcntl` locks are
            # per-process), and a pooled connection was already rolled back
            # at check-in, so the close itself is harmless -- the harm is the
            # inherited in-memory lock bookkeeping outliving it. `close=False` remains
            # correct (SQLAlchemy's documented post-fork idiom; it skips the
            # explicit close path and the `close` hooks, which is the part
            # that would actively interfere).
            #
            # What this must NOT be read as permitting: forking from a
            # thread while another thread is inside a writer
            # `session_scope` block with a live `BEGIN IMMEDIATE`. That
            # connection is checked out, so `dispose(close=False)` does not
            # touch it at all, and the child inherits a connection that
            # believes it holds the write lock. Fork only from a quiescent
            # process, after `dispose_all()`.
            #
            # The `connect` listener survives the rebuild: `Engine.dispose`
            # passes the old pool's dispatch into `Pool.recreate()`, so
            # pragmas keep firing on the new pool and must NOT be
            # re-registered (that would double-fire them).
            engine.dispose(close=False)
            del _ENGINES[stale_key]
            purged = True
        # Drop every remaining reference (the loop variables) and collect:
        # see the comment above for why the inherited connections must be
        # closed before this function can hand out -- or even build --
        # another one. Inside the lock and before the build, on purpose:
        # see `_ENGINES_LOCK`.
        stale_key = owner_pid = engine = None  # noqa: F841
        if purged:
            gc.collect()

        engine = make_engine(url, readonly=readonly, foreign_keys=foreign_keys)
        _ENGINES[key] = (pid, engine)
        return engine


def dispose_all() -> None:
    """Dispose every cached engine and empty the cache.

    For test teardown, so no test leaks an engine into another test's
    process, and for item 8's pre-fork hygiene in the supervisor.

    **This is the mandatory fork-point call.** Every site that forks while
    this module's engines may exist must call it first (the daemon's
    `_spawn` does, before every `Process.start()`); `get_engine`'s pid check
    is only the backstop. Calling it in the PARENT, with the engines the
    parent built, closes them for real -- which is what keeps the child from
    ever inheriting a live connection.

    **Pid-guarded, like `get_engine`.** Each entry is closed for real only
    when this process is the one that built it; an entry inherited across a
    fork is dropped with `close=False` and then `gc.collect()`ed, exactly
    as `get_engine` would -- without the collect the inherited connection
    would linger in a reference cycle and, in a child that goes on to open
    its own connection, defeat SQLite's lock bookkeeping (see `get_engine`).
    Earlier this function ignored the `owner_pid` it was unpacking and
    always passed `close=True`; probed, a forked child calling it ran the
    close path on the parent's connections and exited 0 with no warning.
    Since this is public, re-exported, used in test teardown, and earmarked
    for the *supervisor* -- i.e. it will be called near a fork point by
    design -- its guarantee has to be structural and not a sentence in this
    docstring. Entries are dropped either way.

    A `gc.freeze()` before a fork defeats the collect (frozen objects are
    ignored by the collector), so any future `gc.freeze()` must come AFTER
    this function.
    """
    pid = os.getpid()
    inherited = False
    with _ENGINES_LOCK:
        for owner_pid, engine in list(_ENGINES.values()):
            engine.dispose(close=(owner_pid == pid))
            inherited = inherited or owner_pid != pid
        _ENGINES.clear()
        # Unbind the loop variables so no engine stays reachable from this
        # frame when the collector runs.
        owner_pid = engine = None  # noqa: F841
        if inherited:
            # An inherited engine was disposed with close=False, which leaves
            # its connections in a reference cycle; see `get_engine`. Inside
            # the (re-entrant) lock, like `get_engine`: otherwise another
            # thread could build a fresh engine and open its own connection
            # while the inherited one is still alive.
            gc.collect()


def _install_readonly_guard(session: Session) -> None:
    """Attach layer 2 of read-only enforcement to one `Session` instance.

    Layer 1 (`PRAGMA query_only=ON`) is the enforcement and catches
    everything, including raw `session.execute(text("UPDATE ..."))`, because
    SQLite itself refuses the write. Layer 2 is *diagnosis*: it turns the
    common ORM cases into a `ReadOnlySessionError` naming the entities,
    instead of an `OperationalError` that reads like a filesystem problem.

    Listeners are attached to this session INSTANCE, never to the `Session`
    class -- a class-level listener would silently apply to the CLI's writer
    sessions too.
    """

    @event.listens_for(session, "before_flush")
    def _block_flush(sess, flush_context, instances):  # noqa: ANN001
        changed = list(sess.new) + list(sess.dirty) + list(sess.deleted)
        if changed:
            names = sorted({type(obj).__name__ for obj in changed})
            raise ReadOnlySessionError(
                "write attempted through a read-only session (pending "
                f"changes to: {', '.join(names)}). Read-only sessions "
                "never write; the CLI is the only writer."
            )

    @event.listens_for(session, "do_orm_execute")
    def _block_orm_dml(orm_execute_state):  # noqa: ANN001
        if (
            orm_execute_state.is_insert
            or orm_execute_state.is_update
            or orm_execute_state.is_delete
        ):
            raise ReadOnlySessionError(
                "bulk INSERT/UPDATE/DELETE attempted through a read-only "
                f"session: {orm_execute_state.statement!r}"
            )


@contextmanager
def session_for_engine(engine: Engine, *, readonly: bool = False) -> Iterator[Session]:
    """Context-managed `Session` bound to an existing `Engine`.

    Writer (`readonly=False`): commits **exactly once**, on clean exit;
    rolls back on any exception; always closes. On a file-backed writer
    engine the transaction begins as `BEGIN IMMEDIATE` at the first
    statement -- including a `SELECT` -- so the whole block runs under
    SQLite's write lock (`_install_begin_immediate`).

    **At most one writer connection to a given file may be live in this
    process at a time.** Not "one concurrent writer per thread", and not
    "one writer *transaction*": the lock is taken at the first statement of
    any transaction, so a writer session that only reads still holds it,
    and a second writer connection opened while the first is live cannot
    make progress -- no thread is waiting to release anything. Measured,
    single-threaded:

        nested session_scope(url) inside session_scope(url)
          -> OperationalError after 5.2s: database is locked
             [SQL: BEGIN IMMEDIATE]

    That is a guaranteed `busy_timeout` stall followed by a hard failure
    whose message names a concurrent process that does not exist. A nested
    *reader* is fine (measured: 0.00s), so the failure is asymmetric and
    easy to miss. "Writer connection" includes the FK-off migration engine
    from `get_engine(url, foreign_keys=False)`: different cache key, own
    pool, same file, same lock. So item 8 must run `ensure_schema` +
    `commit()` + close as a strictly sequential phase **before** opening
    the `save_config` session -- or do both over one connection -- never
    nested. Measured for the nested shape:

        with session_scope(url) as s:
            conn = get_engine(url, foreign_keys=False).connect()
            ensure_schema(conn)
        -> FAILED after 5.2s: database is locked

    i.e. a deterministically wedged-then-failing `zfsbackup-config import`
    on a fresh database. Keep writer blocks short for the same family of
    reasons, and do not wrap a long ZFS operation in one.

    That single commit is the
    whole durability story for `mapper.save_config`, which is a
    wipe-and-reinsert that owns no transaction policy of its own: because
    the deletes and the inserts land in one transaction with one commit, a
    crash mid-save leaves the **previous** config intact rather than a
    half-written one (WAL recovery is atomic at the commit boundary and
    never replays uncommitted frames). It is also what prevents item 7's
    split-brain case, where a missing commit loses the `alembic_version`
    stamp while the created tables persist.

    Reader (`readonly=True`): never commits, always closes, and gets the
    `ReadOnlySessionError` guard. **The engine must itself have been built
    with `readonly=True`** for `PRAGMA query_only=ON` to be in force --
    `get_engine`/`session_scope` arrange that; this function cannot, since
    the engine already exists. Passing a read-write engine here gets you
    layer 2 (diagnosis) without layer 1 (enforcement).

    No public function in this module returns a bare, long-lived `Session`.
    That is deliberate: a `Session` checked out before a fork holds its
    DBAPI connection directly and is invisible to the pid-keyed cache, so
    the only durable fix is for sessions not to outlive a `with` block.
    """
    session = Session(engine)
    if readonly:
        _install_readonly_guard(session)
    try:
        yield session
        if not readonly:
            session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def session_scope(url: str, *, readonly: bool = False) -> Iterator[Session]:
    """Context-managed `Session` for a URL, resolving the engine per call.

    The engine is looked up through `get_engine()` **on every call**, inside
    the context manager. There is deliberately no module-level
    `sessionmaker(bind=engine)`: a bound `sessionmaker` holds a hard
    reference to its engine, so it would survive the pid-keyed cache's
    discard-and-rebuild entirely and keep handing out sessions on the
    inherited, pre-fork pool. The session factory must resolve its engine
    per call, and that is what this function is.

    `readonly=True` selects the separately-keyed read-only engine (its own
    pool, `PRAGMA query_only=ON`, `synchronous=NORMAL`) and installs the
    `ReadOnlySessionError` guard.

    **Who passes what.** Every reader (the daemon supervisor's one startup
    read, via `access.read_config`; worker processes never open the store):
    always `readonly=True`. CLI:
    `readonly=True` for every command that only reads (`show`, `validate`,
    `export`, ...) and the default read-write form only for commands that
    mutate. That is not a stylistic preference. Since writer transactions
    open with `BEGIN IMMEDIATE`, a block that only `SELECT`s through the
    read-write form still takes the exclusive write lock at its first
    statement and holds it until commit. Measured, two threads doing
    read-only work through the read-write form:

        time to first statement: A=0.00s  B=0.90s
          (B waited out A's 1.0s READ-ONLY block)

    ...against A=0.00s B=0.00s for the same work with `readonly=True`. Left
    unqualified, two concurrent `zfsbackup-config show` invocations would
    serialise, and a slow `show` could make a concurrent `import` fail with
    `database is locked` -- a write rejected by a reader. This is a
    behaviour change introduced together with `BEGIN IMMEDIATE`; before it,
    a `SELECT` on a writer engine took no lock at all.
    """
    engine = get_engine(url, readonly=readonly)
    with session_for_engine(engine, readonly=readonly) as session:
        yield session

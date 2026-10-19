"""The store's read and write entry points for whole-configuration access
(plan item 15.1/15.4): `read_config` and `write_config`.

Every tool that reads the configuration -- the daemon at startup, and
`zfsbackup-config` for `export`, `migrate --check` and the read half of every
edit -- goes through `read_config`; every `zfsbackup-config` edit goes
through `write_config` (and `import` through `importer.import_yaml`, which
shares the same writer lock). Nothing in here returns a session, a
connection or an engine: each function opens what it needs, finishes, and
closes before returning, so the callers stay clear of the fork-safety rules
in `db.py`. Neither function ever creates a database -- creating is
`import_yaml`'s monopoly (`allow_new`).

Read path
---------
`read_config(resolved)` is ONE read-only connection and ONE read snapshot:
an explicit `BEGIN`, then `check_schema` and `load_config_snapshot` on that
same connection, then `COMMIT`, then close. Putting `check_schema` inside the
snapshot is the point. Run on a connection of its own, ahead of the load, it
leaves a window in which a `migrate` (or an `import` that migrates) can land
between "the schema is at head" and "read the rows through head's ORM
models", and the rows are then read through models that no longer describe
them. Inside the snapshot the check and the rows are the same database
state, by construction. The `BEGIN` is scoped to the call (milliseconds) and
committed before the connection closes, so it does not contradict `db.py`'s
rule against pinning a WAL read snapshot for a connection's lifetime.

The same `BEGIN` is what makes the config itself coherent. `load_config` is
five or more independent statements, and a reader engine begins no
transaction of its own (pysqlite emits no `BEGIN` for a bare `SELECT`), so
without it a concurrent `zfsbackup-config import` is read as a config
assembled from two commits. Measured, with an import that merely reordered
two datasets: their whole retention policies came back swapped, silently,
and a reader/writer soak hit 15.6% torn loads with no exception of any kind.

Write path
----------
`write_config(resolved, config, *, expected_generation)` is: preflight (no
create) -> `validate_config` -> `writer_lock` -> writer session (`BEGIN
IMMEDIATE`) -> `check_schema` -> compare-and-swap -> `save_config` -> commit.
The schema check repeats inside the write transaction because the read that
produced `config` may be minutes old (an operator in `$EDITOR`) and a newer
zfsbackup can have migrated the file since. "database is locked" after
`busy_timeout`, and a writer lock that stays held, both surface as
`ConfigBusy`; a stale `expected_generation` as `ConfigConflict`.

A multi-step edit holds the writer lock across its side effects with
`with writer_lock(resolved) as held: gen = read_generation(resolved); ...;
write_config(resolved, cfg, expected_generation=gen, lock=held)`; see
`write_config` for the exact contract.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from zfsbackup.config import BackupConfig
from zfsbackup.config.store.db import dispose_all
from zfsbackup.config.store.errors import ConfigBusy
from zfsbackup.config.store.mapper import (
    ConfigSnapshot,
    load_config_snapshot,
    read_store_id,
    save_config,
    validate_config,
)
from zfsbackup.config.store.migrate import check_schema
from zfsbackup.config.store.paths import (
    DEFAULT_WRITER_LOCK_TIMEOUT,
    ResolvedConfigPath,
    open_config_connection,
    open_config_session,
    require_writer_lock_held,
    writer_lock,
)

__all__ = [
    "StoreState",
    "WriteResult",
    "check_config_schema",
    "read_config",
    "read_generation",
    "read_store_state",
    "write_config",
]

_LOCKED_MARKERS = ("database is locked", "database table is locked")


def check_config_schema(resolved: ResolvedConfigPath) -> str:
    """Verify the config database is at this package's Alembic head and
    return that head revision's id.

    Opens exactly one **read-only** connection, runs `check_schema`, and
    closes it before returning -- holding it open would pin a WAL read
    snapshot and stop the CLI's checkpoints from passing (see
    `check_schema`'s own docstring). Prefer `read_config` when the rows are
    going to be read next: this standalone check is outside any snapshot, so
    a migration can land between it and a later read.

    Raises `SchemaError` (`SchemaOutOfDate`, `SchemaVersionMismatch`,
    `SchemaSplitBrain`) for a database this process must refuse, and
    `ConfigPathError` for a path/permission problem the preflight catches.
    It never migrates: fixing a `SchemaOutOfDate` is `zfsbackup-config`'s
    job, deliberately, because reading an older schema through current ORM
    models silently mis-reads config, and a daemon that refuses to start
    beats a daemon that starts wrong.
    """
    with open_config_connection(resolved, readonly=True) as connection:
        return check_schema(connection)


def read_config(
    resolved: ResolvedConfigPath, *, warn_never_prune: bool = True
) -> ConfigSnapshot:
    """Read the whole configuration, its generation, its store id and its
    schema revision from one read snapshot. See this module's docstring.

    `warn_never_prune=False` suppresses `load_config`'s per-dataset "never be
    pruned" log lines: a caller that reads repeatedly (a reload) logs them
    once, at startup, and not again on every read.

    Read-only throughout (the `query_only` reader engine plus the
    `ReadOnlySessionError` guard), never creates or migrates anything, and
    has closed its session when it returns -- the returned `ConfigSnapshot`
    owns no ORM state. The cached reader engine's pooled connection does
    stay open, as for any `db.py` session; a caller that is about to fork
    (the daemon supervisor) or to block on a human (`$EDITOR`) calls
    `dispose_all()` itself. This is deliberate and differs from
    `write_config`, which disposes on every exit: a read is the half of an
    edit that is followed by the human wait, so the caller already has the
    obvious place to dispose, and a reader that disposed here would defeat
    callers that make several reads back to back.

    Raises `ConfigPathError` (preflight), `SchemaError` (wrong schema),
    `ValueError` (`load_config`: uninitialised store, "No datasets
    configured", a bad stored duration) and `sqlalchemy.exc.OperationalError`
    / `DatabaseError` for storage failures. `messages.config_error_message`
    turns any of these into an operator-grade line.
    """
    with open_config_session(resolved, readonly=True) as session:
        session.execute(text("BEGIN"))
        try:
            schema_rev = check_schema(session.connection())
            snapshot = load_config_snapshot(
                session, schema_rev=schema_rev, warn_never_prune=warn_never_prune
            )
        except BaseException:
            try:
                session.execute(text("ROLLBACK"))
            except Exception:  # pragma: no cover - the close is the real
                pass           # guarantee; never mask the original error
            raise
        session.execute(text("COMMIT"))
        return snapshot


def read_generation(resolved: ResolvedConfigPath) -> int:
    """The stored `GlobalSettings.generation`, and nothing else.

    One read-only connection: `BEGIN`, a single `SELECT generation`, `COMMIT`,
    close -- no config load, NO schema check (a caller that needs one has
    `read_config` / `check_config_schema`; `write_config` re-checks the schema
    under its own lock anyway), nothing left open (no transaction, no WAL read
    mark; the cached reader engine's pooled connection stays, as for
    `read_config`, and `write_config`'s `dispose_all()` drops it).

    Its job is the multi-step edit: under `writer_lock(resolved)` no other
    zfsbackup-config writer can move the generation, so a value read here is
    the one `write_config(..., lock=held)` will compare against. See
    `write_config` for the full sequence.

    Raises `ConfigPathError` (preflight), `ValueError` (the store holds no
    `GlobalSettings` row, i.e. is uninitialised -- the same condition
    `load_config` reports) and `sqlalchemy.exc.OperationalError` /
    `DatabaseError` for storage failures (an unmigrated database lacking the
    table is one of those; run `read_config` or `check_config_schema` first
    when that distinction matters).
    """
    with open_config_connection(resolved, readonly=True) as connection:
        connection.execute(text("BEGIN"))
        try:
            row = connection.execute(
                text("SELECT generation FROM global_settings WHERE id = 1")
            ).fetchone()
        except BaseException:
            try:
                connection.execute(text("ROLLBACK"))
            except Exception:  # pragma: no cover - never mask the original
                pass
            raise
        connection.execute(text("COMMIT"))
    if row is None:
        raise ValueError(
            "config store is uninitialized: no GlobalSettings row (id=1) "
            "-- was it created by import?"
        )
    return int(row[0])


@dataclass(frozen=True)
class StoreState:
    """The two cheap facts a periodic poll compares: the file's store id
    (`PRAGMA user_version`; 0 = a legacy database no save has stamped yet) and
    `GlobalSettings.generation`. `(store_id, generation)` is the reload key.
    """

    store_id: int
    generation: int


def read_store_state(resolved: ResolvedConfigPath) -> StoreState:
    """`(store_id, generation)` from one read snapshot, and nothing else.

    The poll's read: one read-only connection, `BEGIN`; `PRAGMA user_version`;
    `SELECT generation`; `COMMIT`; close. The pragma is the FIRST statement
    so it opens the read snapshot and the `SELECT` reads the same one -- the
    pair cannot straddle a commit. NO schema check (the poll only asks "did
    the key change"; a changed key is followed by a full `read_config`, which
    checks), no config load, no ORM, and it does NOT dispose: like
    `read_generation` it leaves the cached reader engine's pooled connection
    in place (idle, in no transaction, so it holds no WAL read mark and
    `PRAGMA wal_checkpoint` is not blocked by it) and the caller disposes --
    the supervisor does so after every poll.

    Raises what `read_generation` raises: `ConfigPathError` (preflight),
    `ValueError` (no `GlobalSettings` row, i.e. uninitialised) and
    `sqlalchemy.exc.OperationalError` / `DatabaseError` (storage failures; an
    unmigrated database lacking the table is one). `read_generation` itself
    is unchanged.
    """
    with open_config_connection(resolved, readonly=True) as connection:
        connection.execute(text("BEGIN"))
        try:
            store_id = connection.exec_driver_sql("PRAGMA user_version").scalar()
            row = connection.execute(
                text("SELECT generation FROM global_settings WHERE id = 1")
            ).fetchone()
        except BaseException:
            try:
                connection.execute(text("ROLLBACK"))
            except Exception:  # pragma: no cover - never mask the original
                pass
            raise
        connection.execute(text("COMMIT"))
    if row is None:
        raise ValueError(
            "config store is uninitialized: no GlobalSettings row (id=1) "
            "-- was it created by import?"
        )
    return StoreState(store_id=int(store_id or 0), generation=int(row[0]))


@dataclass(frozen=True)
class WriteResult:
    """What a successful `write_config` reports.

    `generation` is the new `GlobalSettings.generation`, i.e. the value to
    pass as `expected_generation` for a follow-up edit without re-reading.
    `warnings` are `validate_config`'s non-fatal findings for the config that
    was written (never-prune, duplicate rules, ...): the store logs nothing,
    so showing them is the caller's job. A caller that wants them printed
    BEFORE the write can call `validate_config` itself first; they are the
    same strings. `store_id` is the file's store id after the write (stamped
    by this write if the database was a legacy, never-stamped one): with
    `generation` it is the reload key of the config just written.
    """

    generation: int
    warnings: Tuple[str, ...] = ()
    store_id: int = 0


def _is_locked(exc: OperationalError) -> bool:
    reason = str(getattr(exc, "orig", None) or exc).lower()
    return any(marker in reason for marker in _LOCKED_MARKERS)


def write_config(
    resolved: ResolvedConfigPath,
    config: BackupConfig,
    *,
    expected_generation: Optional[int],
    expected_store_id: Optional[int] = None,
    lock_timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT,
    lock: Optional[Path] = None,
) -> WriteResult:
    """Replace the stored configuration with `config`, if the store is still
    at `expected_generation`. See this module's docstring for the sequence.

    `expected_generation` is required (keyword-only) so no caller can write
    unconditionally by omission: pass the `ConfigSnapshot.generation` the edit
    started from, or `None` to deliberately overwrite whatever is there.

    Never creates a database: a missing or zero-byte file is refused by the
    preflight (`ConfigDbNotFound` / `ConfigDbNotADatabase`) before the writer
    lock file is even created. Never returns or holds a session.

    `expected_store_id` (default `None` = not compared) is the
    `ConfigSnapshot.store_id` / `StoreState.store_id` the edit started from.
    It is compared inside the write transaction, before the generation, when
    it is non-zero (0 = "read a legacy database"; if that has been stamped
    since, the generation compare reports it): a
    database file that was deleted and re-imported since (same generation
    numbers, different file) is refused with `ConfigConflict` (a
    `StoreIdConflict`) and nothing is written. A database never stamped
    (id 0) is stamped by this write. `WriteResult.store_id` is the id after
    the write.

    **Holding the lock across a multi-step operation.** By default this call
    takes `writer_lock(resolved)` itself, so the lock covers only the write.
    An edit with a side effect that must not outlive a refused write (e.g.
    `rename destination` copying ZFS anchors first) takes the lock itself and
    passes it in::

        with writer_lock(resolved) as held:
            gen = read_generation(resolved)   # stable: no other writer can move it
            ...side effects that depend on `gen`...
            write_config(resolved, cfg, expected_generation=gen, lock=held)

    Contract for `lock=`:

    - it must be exactly the object `writer_lock(resolved)` yielded, for the
      SAME `resolved` database, and the calling thread must still be inside
      that `with` block. Anything else (None aside) -- a stale handle, another
      database's lock, another thread's -- raises `ValueError` before anything
      is opened or written, rather than degrading to an unlocked write;
    - `write_config` then does NOT re-acquire (a plain second `writer_lock`
      in the same thread still raises `RuntimeError`, by design: that is the
      unaware caller) and does NOT release; the caller's `with` block does;
    - `lock_timeout` is ignored (nothing is waited for; the caller chose its
      timeout when it took the lock);
    - everything else is unchanged: preflight, `validate_config`, `BEGIN
      IMMEDIATE`, the schema re-check, the compare-and-swap, `ConfigConflict`
      / `ConfigBusy` semantics (`ConfigBusy` here can only be SQLite's
      `busy_timeout`, e.g. a non-honouring process holding the database), and
      the `dispose_all()` on every exit. A refusal leaves the lock held for the
      caller to release (or to retry under).

    Raises `ValueError` (invalid `config`, or an invalid `lock=`; nothing
    written), `ConfigPathError` / `SchemaError` (unusable database),
    `ConfigConflict` (the generation moved on, or the store id no longer
    matches `expected_store_id`; nothing written), `ConfigBusy`
    (writer lock or SQLite lock not obtained in time; nothing written; safe to
    retry as is). A `ValueError` always means nothing was written. It does not
    mean nothing was opened: an invalid `config` is detected AFTER the schema
    pre-check, which opens (and closes) one read-only connection, so a
    `ConfigPathError` / `SchemaError` for an unusable database wins over a
    `ValueError` for a bad `config`. An invalid `lock=` is detected before
    anything is opened.

    **Leaves no engine behind.** Whatever the outcome (success, `ConfigConflict`,
    `ConfigBusy`, any other error) every pooled connection this call opened --
    the preflight's reader engine and the writer engine -- is disposed before
    it returns or raises, via `dispose_all()` in a `finally`. A caller that
    loops (the editor's retry after a `ConfigConflict`) therefore never carries
    an open connection, a WAL read mark or `-shm` handle across `$EDITOR` or a
    fork. `dispose_all()` is process-global: it also drops any other engine
    this process had cached, which is harmless (they are rebuilt lazily) and is
    the same call every fork point already makes.
    """
    try:
        return _write_config(
            resolved, config, expected_generation, expected_store_id,
            lock_timeout, lock,
        )
    finally:
        dispose_all()


def _write_config(
    resolved: ResolvedConfigPath,
    config: BackupConfig,
    expected_generation: Optional[int],
    expected_store_id: Optional[int],
    lock_timeout: float,
    held_lock: Optional[Path] = None,
) -> WriteResult:
    # A claimed-held lock is verified first: cheap, and it must not degrade
    # into an unlocked write.
    if held_lock is not None:
        require_writer_lock_held(resolved, held_lock)
    # Preflight AND schema check BEFORE the lock: a missing database, or one
    # that is not ours (item 10.4), must not get a `<db>.writer.lock` created
    # beside it, and this also guarantees the lock's directory exists. Every
    # refusal here would be raised under the lock too (`SchemaOutOfDate`
    # included: a write never migrates), so there is no case this check turns
    # away that the locked check below would have let through. The locked
    # check stays as the authority -- the file can change in between.
    check_config_schema(resolved)
    warnings = tuple(validate_config(config))

    with contextlib.nullcontext(held_lock) if held_lock is not None else writer_lock(
        resolved, timeout=lock_timeout
    ):
        try:
            with open_config_session(resolved, readonly=False) as session:
                # First statement of the session: `BEGIN IMMEDIATE` (db.py),
                # so the schema check, the generation compare and the wipe-
                # and-reinsert all see one frozen state.
                check_schema(session.connection())
                generation = save_config(
                    session,
                    config,
                    expected_generation=expected_generation,
                    expected_store_id=expected_store_id,
                )
                # Same transaction, after the stamp: the id this write
                # commits.
                store_id = read_store_id(session)
        except OperationalError as exc:
            if _is_locked(exc):
                raise ConfigBusy(
                    f"the config database {resolved.path} is locked by another "
                    "process (the database stayed locked); nothing was "
                    "written. Retry in a moment."
                ) from exc
            raise
    return WriteResult(generation=generation, warnings=warnings, store_id=store_id)

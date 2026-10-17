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
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from zfsbackup.config import BackupConfig
from zfsbackup.config.store.db import dispose_all
from zfsbackup.config.store.errors import ConfigBusy
from zfsbackup.config.store.mapper import (
    ConfigSnapshot,
    load_config_snapshot,
    save_config,
    validate_config,
)
from zfsbackup.config.store.migrate import check_schema
from zfsbackup.config.store.paths import (
    DEFAULT_WRITER_LOCK_TIMEOUT,
    ResolvedConfigPath,
    open_config_connection,
    open_config_session,
    writer_lock,
)

__all__ = ["WriteResult", "check_config_schema", "read_config", "write_config"]

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


def read_config(resolved: ResolvedConfigPath) -> ConfigSnapshot:
    """Read the whole configuration, its generation and its schema revision
    from one read snapshot. See this module's docstring.

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
            snapshot = load_config_snapshot(session, schema_rev=schema_rev)
        except BaseException:
            try:
                session.execute(text("ROLLBACK"))
            except Exception:  # pragma: no cover - the close is the real
                pass           # guarantee; never mask the original error
            raise
        session.execute(text("COMMIT"))
        return snapshot


@dataclass(frozen=True)
class WriteResult:
    """What a successful `write_config` reports.

    `generation` is the new `GlobalSettings.generation`, i.e. the value to
    pass as `expected_generation` for a follow-up edit without re-reading.
    `warnings` are `validate_config`'s non-fatal findings for the config that
    was written (never-prune, duplicate rules, ...): the store logs nothing,
    so showing them is the caller's job. A caller that wants them printed
    BEFORE the write can call `validate_config` itself first; they are the
    same strings.
    """

    generation: int
    warnings: Tuple[str, ...] = ()


def _is_locked(exc: OperationalError) -> bool:
    reason = str(getattr(exc, "orig", None) or exc).lower()
    return any(marker in reason for marker in _LOCKED_MARKERS)


def write_config(
    resolved: ResolvedConfigPath,
    config: BackupConfig,
    *,
    expected_generation: Optional[int],
    lock_timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT,
) -> WriteResult:
    """Replace the stored configuration with `config`, if the store is still
    at `expected_generation`. See this module's docstring for the sequence.

    `expected_generation` is required (keyword-only) so no caller can write
    unconditionally by omission: pass the `ConfigSnapshot.generation` the edit
    started from, or `None` to deliberately overwrite whatever is there.

    Never creates a database: a missing or zero-byte file is refused by the
    preflight (`ConfigDbNotFound` / `ConfigDbNotADatabase`) before the writer
    lock file is even created. Never returns or holds a session.

    Raises `ValueError` (invalid `config`; nothing opened), `ConfigPathError`
    / `SchemaError` (unusable database), `ConfigConflict` (the generation moved
    on; nothing written), `ConfigBusy` (writer lock or SQLite lock not
    obtained in time; nothing written; safe to retry as is).

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
        return _write_config(resolved, config, expected_generation, lock_timeout)
    finally:
        dispose_all()


def _write_config(
    resolved: ResolvedConfigPath,
    config: BackupConfig,
    expected_generation: Optional[int],
    lock_timeout: float,
) -> WriteResult:
    # Preflight AND schema check BEFORE the lock: a missing database, or one
    # that is not ours (item 10.4), must not get a `<db>.writer.lock` created
    # beside it, and this also guarantees the lock's directory exists. Every
    # refusal here would be raised under the lock too (`SchemaOutOfDate`
    # included: a write never migrates), so there is no case this check turns
    # away that the locked check below would have let through. The locked
    # check stays as the authority -- the file can change in between.
    check_config_schema(resolved)
    warnings = tuple(validate_config(config))

    with writer_lock(resolved, timeout=lock_timeout):
        try:
            with open_config_session(resolved, readonly=False) as session:
                # First statement of the session: `BEGIN IMMEDIATE` (db.py),
                # so the schema check, the generation compare and the wipe-
                # and-reinsert all see one frozen state.
                check_schema(session.connection())
                generation = save_config(
                    session, config, expected_generation=expected_generation
                )
        except OperationalError as exc:
            if _is_locked(exc):
                raise ConfigBusy(
                    f"the config database {resolved.path} is locked by another "
                    "process (SQLite busy_timeout expired); nothing was "
                    "written. Retry in a moment."
                ) from exc
            raise
    return WriteResult(generation=generation, warnings=warnings)

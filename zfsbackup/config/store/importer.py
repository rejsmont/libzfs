"""YAML -> SQLite config store import (plan item 8, phase A / 8b).

`import_yaml(yaml_path, resolved, *, create=True) -> ImportResult` is the
one supported way to bring a `BackupConfig.from_file`-shaped YAML config
into the store this package defines. It is a **destructive full replace**:
every dataset, destination, retention rule, and global setting already in
the target database is discarded and replaced by the YAML's contents (see
`mapper.save_config`'s own docstring for why wipe-and-reinsert, not
diff-and-merge, is the only sane semantics for a config with no surrogate
ids of its own).

Three phases, strictly sequential, never nested -- see `db.py`'s "Sequence
it, do not nest it" and `migrate.py`'s binding note, both measured
directly against this repo's own engine -- plus one guard in front of
them, not a fourth phase:

0. Parse the YAML (`BackupConfig.from_file`) and run the session-free
   `mapper.validate_config` on the result (its warnings are logged).
   **Nothing touches the filesystem target before both succeed.** This
   covers every refusal `save_config` would make later too -- a
   `{24h: 1M, 7d: 30d}` keep_for collision, a duplicate remote destination,
   a non-boolean flag -- which used to be found only in phase 3, after
   phases 1-2 had already created, migrated and stamped the target. A bad
   YAML never creates, truncates, or wipes anything: this ordering is the
   entire safety property of a destructive full replace.

   The property is *not* absolute for failures that can only be discovered
   later (disk full, a locked database, a SQLAlchemy error in phase 2 or 3).
   If THIS call created the target (`created=True`) and a later phase fails,
   `_remove_created_target` tries to put the filesystem back -- but it only
   ever deletes a database it can show nobody else is using, because "the
   inode I made is still there" proves only that it was not replaced, not
   that it was not adopted: a concurrent `import` of the same path takes the
   re-import branch, commits, and would otherwise have its committed
   configuration unlinked (and been told it succeeded); a connected reader
   would have the DB and its sidecars deleted under it (both measured).
   It therefore removes the file only if, after this process has closed
   every engine, (1) the path is still the inode this call created, (2) no
   `-wal`/`-shm`/`-journal` exists (in WAL mode the last connection to close
   deletes them, so a survivor means another connection is attached), and
   (3) a raw `sqlite3` probe under `BEGIN IMMEDIATE` finds `global_settings`
   absent or empty (it is written in the same transaction as all data, so
   it is the commit marker; a zero-byte file skips the probe -- nobody can
   have written to one), and (4) an `lstat` fingerprint `(st_ino, st_size,
   st_mtime_ns, st_ctime_ns)`, taken at the top of the cleanup BEFORE the
   probe, is unchanged at the `unlink`. Otherwise it removes NOTHING and logs
   one WARNING naming the database, the reason and the next step. It never
   unlinks a sidecar: the connection that created them removes them by path
   when it closes, and deleting them by hand under a live process is how a WAL
   database is corrupted. The probe's verdict goes stale the moment its lock is
   released -- a whole concurrent import can run between the probe's ROLLBACK
   and the `unlink` (measured with a forced stall: it commits, exits and
   checkpoints, leaving no sidecar for check (2) to see). The fingerprint
   catches that, and is deliberately NOT re-taken after the probe: a retake
   would adopt whatever a writer did in the gap as the new baseline. The probe
   itself does not change the fingerprint (step (2) guarantees no WAL at
   entry, so its last close checkpoints nothing; it only reads and rolls
   back -- measured). What remains is (a) a writer that has attached but run
   no statement yet, and (b) on a non-empty file, a writer whose whole commit
   and checkpoint lands within one filesystem timestamp tick of the baseline
   `lstat` (inode and size do not change; on Linux mtime/ctime come from a
   coarse 1-10 ms clock). A zero-byte file is not affected: any write grows
   it. Item 15.4's writer lock, which the CLI will hold across the whole
   import, closes both. A failure over a pre-existing target
   leaves its stored configuration exactly as it was -- phase 3 rolls back.
   Phase 2 commits before phase 3 starts, so an existing database may
   already have been migrated to the current schema (its content is
   unchanged); phase 2's own failures roll back entirely.

   A first import (the target does not exist) is also refused, creating
   nothing and touching nothing, if `-wal`/`-shm`/`-journal` files already
   exist next to it (`ConfigDbOrphanedSidecars`): SQLite would replay an
   orphaned `-wal`/`-journal` into the brand-new file, and a live orphan's
   `-shm` is shared by path.
   If the database has appeared by the time the sidecars are found (two first
   imports racing: the other one's live sidecars look identical), it raises
   `ConfigDbCreatedConcurrently` instead -- retry, and never remove them.

   **The same "misuse creates nothing" property extends to a bad
   *target*, not just a bad source.** Immediately after phase 0,
   `check_config_db_suffix(resolved)` (`paths.py`) raises `ConfigDbIsYaml`
   before phase 1 runs at all, for a `.yaml`/`.yml`-suffixed `-c`. Without
   this guard, an operator typo'ing `-c config.yaml` instead of `-c
   config.db` would still get refused -- but only after phase 1 had
   already created a stray zero-byte file at that path (`check_config_db`
   itself refuses it, from inside `open_config_connection`, in phase 2 --
   too late to prevent phase 1's side effect). That stray file is
   indistinguishable from the zero-byte artifact `paths.py`'s own
   docstring describes elsewhere, so it would also confuse the NEXT
   preflight against the same path. `check_config_db_suffix` is exactly
   `check_config_db`'s own step-3 suffix decision, extracted so this
   guard and that check share one decision site and cannot drift apart --
   see its docstring for why it needs no I/O and is therefore safe to
   call before the target exists at all.
1. Bring the target file into existence (or, for an existing file on the
   DEFAULT path only, fix its mode -- any other existing path is left
   alone and only warned about), via `paths.py`'s
   `create_config_db_file`/`ensure_config_db_mode` -- never a raw
   `open()`/`os.chmod()` here. The `create=False` refusal
   (`ConfigDbNotFound`) is decided FIRST, before this phase touches
   anything, for the same "misuse creates nothing" reason as the suffix
   guard above: a `create=False` call against a missing target must not
   leave a freshly-`mkdir`'d directory behind either. The directory itself
   is only created/chmod'd by `_prepare_target_dir` on the branch that
   will actually create the file -- unlike `paths.py`'s `ensure_config_dir`
   (which fixes the mode of an existing directory, correct for the one
   directory this package owns, `DEFAULT_CONFIG_DB.parent`), this
   module must not silently widen the permissions of an arbitrary
   operator-supplied `-c`'s containing directory just because it already
   existed -- see `_prepare_target_dir`'s own docstring for the measured
   privilege-escalation shape that guards against.
2. **Schema phase, its own connection, closed before phase 3 opens
   anything.** `open_config_connection(resolved, readonly=False,
   foreign_keys=False, allow_new=created)` (the `allow_new=True` case is
   for a database this call just created at step 1 -- it is exactly zero
   bytes until Alembic's first `CREATE TABLE`, which `check_config_db`
   would otherwise refuse as `ConfigDbNotADatabase`; see `paths.py`).
   `ensure_schema(conn)` migrates to head, and `conn.commit()` is the
   **last statement issued on this connection** -- nothing else runs on
   it afterwards. `ensure_schema` raises `NotAConfigDatabase` for an existing
   file that holds tables but records no zfsbackup revision (item 10.4): a
   foreign database is refused, not grafted into, and a first import onto a
   zero-byte or tableless file still works. Both hazards this ordering
   avoids are measured, not
   theoretical:

   - Nesting this inside the phase-3 writer session self-deadlocks: the
     FK-off migration engine is a second *writer* connection on the same
     file, and `BEGIN IMMEDIATE` fires at the first statement of any
     transaction on either one -- measured, `database is locked` after a
     deterministic 5.2s `busy_timeout` stall.
   - Skipping or reordering `commit()` away from being the last statement
     leaves a split-brain database: pysqlite auto-commits each `CREATE
     TABLE` immediately (no transaction open when DDL runs), but the
     `alembic_version` stamp is the one DML statement in the sequence and
     is what actually opens a transaction -- so a missing/early commit can
     land full schema with an empty version table, and every subsequent
     `ensure_schema` call then fails forever with a bare `table datasets
     already exists`. `SchemaSplitBrain` detects that state but cannot
     undo it.
3. **Data phase, a brand new writer session, opened only after phase 2's
   connection is fully closed.** `open_config_session(resolved,
   readonly=False)`. Reads the outgoing row counts on this same session
   (before anything is wiped, so the WARNING it logs is accurate), then
   `save_config(session, config)`. `session_for_engine` commits exactly
   once, on clean exit; any exception rolls the whole phase back, leaving
   whatever was there before this call intact. Duration literals survive:
   `save_config` writes `(total_seconds, literal)` pairs, so `"1M"` in the
   YAML round-trips as `"1M"`, not a re-synthesized `"30d"` -- verified
   against `zfsbackup/config.example.yaml`.

   **Pre-import backup (item 10.1, D-5).** With `backup=` given, phase 3
   loads the OUTGOING configuration (`load_config`) on that same session,
   after the counts and before `save_config`, and hands it to the callback,
   which writes it somewhere and returns the path
   (`ImportResult.backup_path`). Because the writer session's transaction is
   `BEGIN IMMEDIATE` from its first statement, the config backed up is
   exactly the config replaced -- no other writer can commit in between. A
   callback that raises rolls the whole phase back. An existing config that
   cannot be loaded raises `OutgoingConfigUnreadable` unless
   `allow_unreadable_outgoing`. Phase 2's `ensure_schema` also verifies the
   migration with `PRAGMA foreign_key_check` (`SchemaIntegrityError`), and a
   skipped first-import cleanup attaches its WARNING text to the in-flight
   exception (`add_note`) as well as logging it.

**Never route through `DatasetConfig.from_dict` or `.from_property`.**
See `mapper.py`'s module docstring, invariant 3 -- this module calls
`BackupConfig.from_file` exactly once, at phase 0, and every dataclass
downstream of that point is already a `BackupConfig`; nothing here
re-parses anything.

This module has exactly one caller of `check_config_db`'s `allow_new`
escape hatch (via `open_config_connection`, phase 2 above) in the entire
codebase. It must stay that way -- `allow_new` must never be reachable
from the daemon; see `paths.py`'s docstring.

8d -- interim CLI
------------------
`python -m zfsbackup.config.store.importer <yaml> [-c PATH]` (the `_main`
function below) is a deliberately throwaway ~25-line argparse wrapper.
**Item 10 deletes it** and replaces it with `zfsbackup-config import`, an
operator-facing CLI entry point (`zfsbackup/cli/`, not owned by this
package) that must call `import_yaml` directly -- exactly as this
function does -- rather than reimplementing any of its phases. This
exists only so the item-8 commit does not leave a tree with a daemon that
refuses to start (per item 6a's hard YAML refusal) and no way at all to
make it a database, which would break `scenarios/test_two_vm_backup.sh`
and the smoke commands in `CLAUDE.md`.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import stat as stat_module
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

import yaml
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from zfsbackup.config import BackupConfig
from zfsbackup.config.store.db import dispose_all
from zfsbackup.config.store.mapper import load_config, save_config, validate_config
from zfsbackup.config.store.migrate import SchemaError, ensure_schema
from zfsbackup.config.store.models import Dataset, Destination, GlobalSettings
from zfsbackup.config.store.paths import (
    CONFIG_DIR_MODE,
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    ConfigDbCreatedConcurrently,
    ConfigDbNotFound,
    ConfigDbOrphanedSidecars,
    ConfigPathError,
    ResolvedConfigPath,
    check_config_db_suffix,
    create_config_db_file,
    ensure_config_db_mode,
    ensure_config_dir,
    is_default_config_dir,
    open_config_connection,
    open_config_session,
    resolve_config_path,
)

logger = logging.getLogger(__name__)

__all__ = ["ImportResult", "OutgoingConfigUnreadable", "import_yaml"]


@dataclass(frozen=True)
class ImportResult:
    """The outcome of one `import_yaml` call.

    `created` is True only when phase 1 had to create the target file
    itself (a brand-new database); False for a re-import over an existing
    one. `head_revision` is the Alembic head the schema phase left the
    database at. `replaced_datasets`/`replaced_destinations` are the row
    counts phase 3 observed BEFORE wiping them -- both are 0 for a
    brand-new (`created=True`) database. `backup_path` is whatever the
    `backup` callback returned (`None` when no callback was given, there was
    no outgoing configuration to back up, or the outgoing one was unreadable
    and `allow_unreadable_outgoing` let the import proceed).
    """

    created: bool
    head_revision: str
    replaced_datasets: int
    replaced_destinations: int
    backup_path: Optional[Path] = None


class OutgoingConfigUnreadable(ConfigPathError):
    """The configuration already stored in the target database could not be
    loaded, so the pre-import backup the caller asked for cannot be taken --
    and `import_yaml` refuses to destroy a configuration it could not back
    up. The underlying error is `__cause__`. Pass
    `allow_unreadable_outgoing=True` (the CLI's `--no-backup`) to replace it
    anyway. A `ConfigPathError` so that the same `except` the daemon and CLI
    already have for "this config database is unusable" covers it.
    """


def _target_exists(path: Path) -> bool:
    """`os.stat(path)`-based existence check, so a symlink to a real
    target reads as "exists" the same way `check_config_db` itself
    decides existence (`paths.py`'s step 1) -- never `Path.exists()`,
    which folds every `OSError` (not just `ENOENT`) into `False`. Any
    `OSError` other than "missing" is a real problem this function is not
    positioned to diagnose (that is `check_config_db`'s job, reached a few
    lines later via `open_config_connection`'s own preflight), so it is
    left to propagate rather than guessed at here.
    """
    try:
        os.stat(path)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False


def _prepare_target_dir(directory: Path) -> None:
    """Create `directory` (at `CONFIG_DIR_MODE`) if it does not exist;
    otherwise touch nothing but a log line.

    `paths.py`'s `ensure_config_dir` fixes the mode of an existing
    directory (only when it differs and is owned by the caller -- see
    `_ensure_mode`) -- correct for the one directory this package actually
    owns, `DEFAULT_CONFIG_DB.parent` (`/var/lib/zfsbackup`), where "fix a
    stale mode on re-import" is exactly the intended behaviour. It is the
    wrong behaviour for an arbitrary
    operator-supplied `-c` path: measured, `python -m zfsbackup.config.
    store.importer config.yaml -c ~/x.db` against an existing `$HOME`
    (mode `0755`) silently widened it to `0770` -- a privilege escalation
    (group-writable `$HOME`, on many systems a *shared* group like
    `staff`/`users`) and a world-read drop, from a command whose stated
    job is "import a YAML file".

    So: the owned default directory is always created/fixed, matching
    `ensure_config_dir`'s own re-import-friendly intent (this call did
    not necessarily create it, but this package DOES own it). Anything
    else is only created (and therefore only chmod'd) when it does not
    already exist -- this call is then the one bringing it into being, so
    setting its mode is this call's own business. An existing, non-default
    directory is left untouched; if its mode does not already satisfy
    what a WAL sidecar needs, this logs a WARNING naming the actual vs.
    target mode instead of silently changing permissions on a directory
    this call did not create -- `check_config_db` (reached a few lines
    later, via `open_config_connection`) is what actually enforces
    writability, and gives the operator-actionable failure if the
    mismatch turns out to matter.
    """
    # Compared by (st_dev, st_ino), not by path: `target.parent` comes from
    # the RESOLVED path (`/private/var/lib/...` on macOS, a symlinked
    # `/var/lib` on some Linux layouts) while `DEFAULT_CONFIG_DB.parent` is
    # the bare constant, so `==` failed safe -- and wrongly -- on both.
    if is_default_config_dir(directory) or not _target_exists(directory):
        ensure_config_dir(directory)
        return

    try:
        actual_mode = stat_module.S_IMODE(os.stat(directory).st_mode)
    except OSError:
        # Let the real preflight (`check_config_db`, reached a few lines
        # later) diagnose this properly -- it already turns a stat
        # failure into a named ConfigPathError subclass.
        return
    if actual_mode != CONFIG_DIR_MODE:
        logger.warning(
            "%s already exists (mode %04o) and was not created by this "
            "import -- leaving its permissions unchanged rather than "
            "widening them. A WAL config database needs %04o (group-write "
            "and group-search) on its containing directory to (re)create "
            "its -wal/-shm sidecars; fix this yourself if the import "
            "below, or a later daemon start, fails because of it.",
            directory, actual_mode, CONFIG_DIR_MODE,
        )


_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


def _file_id(path: Path) -> Optional[Tuple[int, int]]:
    """`(st_dev, st_ino)` of `path` via `lstat` (a symlink is identified as
    itself, never followed), or `None` if nothing is there."""
    try:
        st = os.lstat(path)
    except OSError:
        return None
    return (st.st_dev, st.st_ino)


def _fingerprint(path: Path) -> Optional[Tuple[int, int, int, int]]:
    """`(st_ino, st_size, st_mtime_ns, st_ctime_ns)` of `path` via `lstat`, or
    `None` if it cannot be read. A concurrent import that commits and
    checkpoints moves mtime/ctime (size does not necessarily change), unless
    it lands within one filesystem timestamp tick of the baseline."""
    try:
        st = os.lstat(path)
    except OSError:
        return None
    return (st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def _sidecars_present(target: Path) -> List[Path]:
    """The `-wal`/`-shm`/`-journal` files next to `target` that exist, by
    `lstat`. Only a positive ENOENT/ENOTDIR counts as "absent": any other
    error (EACCES, ELOOP, ...) is "cannot tell", which every caller must
    treat as present -- both callers refuse to act on it."""
    found = []
    for sfx in _SIDECAR_SUFFIXES:
        path = Path(f"{target}{sfx}")
        try:
            os.lstat(path)
        except (FileNotFoundError, NotADirectoryError):
            continue
        except OSError:
            pass
        found.append(path)
    return found


def _refuse_orphaned_sidecars(target: Path) -> None:
    """A-11.2: raise `ConfigDbOrphanedSidecars` if any sidecar exists next to
    a database that does not (or `ConfigDbCreatedConcurrently` if the database
    has appeared in the meantime). Creates and changes nothing."""
    orphans = _sidecars_present(target)
    if not orphans:
        return
    # The sidecars are only "orphaned" if the database is still absent. A
    # concurrent first import (which created the database after our existence
    # check, and is holding its sidecars open right now) looks identical a
    # moment ago, and the removal advice below would be wrong -- and harmful:
    # deleting a live process's -wal/-shm corrupts its database. Look again.
    names = ", ".join(str(p) for p in orphans)
    if _target_exists(target):
        raise ConfigDbCreatedConcurrently(
            f"{target} was created concurrently by another process while "
            f"this import was starting ({names} are in use, not "
            "orphaned). Nothing was created or changed here; retry the "
            "import once the other process has finished."
        )
    raise ConfigDbOrphanedSidecars(
        f"Refusing to create {target}: {names} already exist(s) but the "
        "database does not. Creating a database here would make SQLite "
        "replay an orphaned -wal/-journal into it, and a still-running "
        "process's -shm is shared by path, so a new database would share "
        "lock state with whatever still holds the old one. Remove them "
        "once no process has them open (check with `fuser` or `lsof`), "
        "then retry. Nothing was created or changed."
    )


def _probe_config_empty(target: Path) -> bool:
    """True iff `target` provably holds no imported configuration.

    A raw `sqlite3` connection -- deliberately NOT `get_engine`, so it
    cannot create the file (`mode=rw`), takes no pragmas and leaves nothing
    in the engine cache. Under `BEGIN IMMEDIATE` (the write lock, so no
    import can commit between the check and our verdict), "empty" means
    `global_settings` is absent or has zero rows: `save_config` writes that
    row in the same transaction as all the data, so it is the commit marker.
    The transaction is rolled back and the connection closed before
    returning. Busy (`timeout=1`), any `sqlite3.Error`, or a row present
    all mean "cannot prove it is empty" -- the first two raise, the last
    returns False.

    The verdict holds only while the write lock is held: once the connection
    closes, another process may import into the file. That is why the caller
    fingerprints the file BEFORE calling this and re-checks it before
    unlinking (`_fingerprint`); the verdict alone is not sufficient. This
    function does not change the fingerprint: it only reads and rolls back,
    and the caller has already established that no WAL exists.
    """
    conn = sqlite3.connect(
        target.as_uri() + "?mode=rw", uri=True, timeout=1, isolation_level=None
    )
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type = 'table' AND name = 'global_settings'"
            ).fetchone()
            if table is None:
                return True
            return conn.execute("SELECT 1 FROM global_settings LIMIT 1").fetchone() is None
        finally:
            conn.execute("ROLLBACK")
    finally:
        conn.close()


def _remove_created_target(
    target: Path,
    created_id: Optional[Tuple[int, int]],
    error: Optional[BaseException] = None,
) -> None:
    """Undo the file creation of a failed first import -- but never delete a
    database another process may be using.

    Without any cleanup, a failure after phase 1 (a disk-full, a locked
    database, anything the up-front validation could not predict) leaves a
    stamped-but-empty database: `alembic_version` at head and no
    `global_settings` row. The daemon's schema check passes it and then
    `load_config` fails "uninitialized"; and a retried `import` sees an
    existing file, takes the re-import path, and never again gets the
    "created" treatment (`allow_new`). Removing what this call made puts the
    filesystem back to what it was -- when that is safe. The module
    docstring explains why "the inode is still ours" is NOT enough; this is
    the algorithm it summarises:

    1. `dispose_all()` -- this process's pooled connections hold the file,
       and a retry here would otherwise be handed a pooled connection to
       the unlinked inode. If it raises, remove nothing.
    2. The path must still be the inode this call created (`created_id`).
    3. No `-wal`/`-shm`/`-journal` may exist. After our own dispose, SQLite's
       last-closer-deletes-the-sidecars rule means a survivor is another
       connection.
    4. If the file is non-empty, `_probe_config_empty` must say it holds no
       configuration (a zero-byte file skips the probe: nobody can have
       written to one).
    5. The probe's connection is closed BEFORE the unlink (its last close
       deletes the sidecars by path -- after an unlink that name may belong
       to a new database). A fingerprint `(st_ino, st_size, st_mtime_ns,
       st_ctime_ns)` is taken by `lstat` at the top of `_remove_if_unused`,
       BEFORE the probe (where the file's size is measured once), and 2 and 3
       are re-checked, together with that fingerprint, immediately before
       `os.unlink`. It is never re-taken after the probe: the probe's verdict
       is stale once its lock is released, a whole concurrent import can run
       before the `unlink` (measured), and a retake would adopt that import's
       changes as the baseline. The probe does not itself change the
       fingerprint (no WAL exists at entry, so its close checkpoints nothing).
       The window that remains is (a) a writer that has attached but run no
       statement and created no sidecar yet, and (b) on a non-empty file, a
       writer whose whole commit and checkpoint lands within one filesystem
       timestamp tick of the baseline `lstat` (coarse 1-10 ms clock on Linux;
       inode and size do not change). Zero-byte files are not affected. Only
       item 15.4's writer lock, held by the CLI across the whole import,
       closes both.
    6. A sidecar is NEVER unlinked.
    7. Success logs INFO; any skip logs one WARNING (see
       `_cleanup_skipped`) and, when `error` (the in-flight exception) is
       given, attaches the same text to it with `error.add_note` so the CLI
       can show it next to the import error without capturing log records.
       Errors are logged, never raised -- this runs while another exception
       is propagating and must not mask it.
    """
    try:
        dispose_all()
    except Exception:  # noqa: BLE001 - cleanup must not mask the real error
        logger.exception("could not dispose engines while cleaning up %s", target)
        note = _cleanup_skipped(
            target, "could not close this process's database connections", "unknown"
        )
        if error is not None:
            error.add_note(note)
        return

    try:
        # A KeyboardInterrupt (any non-`Exception`) can land AFTER phase 3's
        # COMMIT returned, so "this import was NOT applied" may be false.
        if error is not None and not isinstance(error, Exception):
            note = _remove_if_unused(target, created_id, interrupted=True)
        else:
            note = _remove_if_unused(target, created_id)
    except Exception as cleanup_exc:  # noqa: BLE001 - must not mask the real error
        logger.exception("unexpected error while cleaning up %s", target)
        note = (
            f"Failed first import: cleanup of {target} failed unexpectedly "
            f"({cleanup_exc!r}); {target} may still exist. Never delete its "
            "-wal/-shm files on their own."
        )
    if note is not None and error is not None:
        error.add_note(note)


def _cleanup_skipped(
    target: Path, reason: str, kind: str, interrupted: bool = False
) -> str:
    """The one WARNING every skipped cleanup logs, and the same text returned
    so `_remove_created_target` can attach it to the in-flight exception
    (`add_note`) -- the CLI shows it with the import error without having to
    capture log records. `kind` selects the
    next step: `"zero-byte"` (the file is empty, so a re-import cannot adopt
    it), `"schema"` (a schema-only database, which a re-run of the import
    reuses), `"imported"` (another process has imported a configuration),
    `"changed"` (its fingerprint moved between the check and the unlink) or
    `"replaced"`/`"unknown"`. `interrupted` (the in-flight exception was a
    `KeyboardInterrupt`-style `BaseException`, which can land after the commit)
    stops the `"imported"` text from claiming this import was not applied."""
    if kind == "imported" and interrupted:
        step = (
            f"{target} holds a configuration and was left intact; the import "
            "was interrupted, so it may or may not have been applied (run "
            "`zfsbackup-config export` to check)."
        )
    elif kind == "imported":
        step = (
            "Another process has since imported a configuration into "
            f"{target}; it was left intact; this import was NOT applied."
        )
    elif kind == "zero-byte":
        step = f"Remove the empty {target} once nothing has it open, then retry."
    elif kind == "schema":
        step = "If it holds no configuration, re-running the import reuses it."
    elif kind == "changed":
        step = (
            f"{target} was left intact. If it holds no configuration, "
            "re-running the import reuses it."
        )
    elif kind == "replaced":
        step = f"{target} is no longer the file this import created; it was left untouched."
    else:
        step = f"{target} was left in place."
    text = (
        f"Failed first import: {target} was NOT removed ({reason}). {step} "
        "Never delete its -wal/-shm files on their own."
    )
    logger.warning("%s", text)
    return text


def _remove_if_unused(
    target: Path,
    created_id: Optional[Tuple[int, int]],
    interrupted: bool = False,
) -> Optional[str]:
    """Steps 2-6 of `_remove_created_target`. Returns the WARNING text when
    cleanup was SKIPPED (see `_cleanup_skipped`), else `None` (removed, or
    already gone)."""
    # Measured once, BEFORE the probe: size/kind, and the fingerprint step 5
    # compares against (never re-taken -- see the module docstring).
    fingerprint = _fingerprint(target)
    size = fingerprint[1] if fingerprint is not None else -1
    kind = "zero-byte" if size == 0 else "schema"

    # Step 2: replaced?
    if created_id is None or _file_id(target) != created_id:
        if _file_id(target) is None and created_id is not None:
            return  # already gone -- nothing to clean up
        return _cleanup_skipped(target, "it is not the file this import created", "replaced")

    # Step 3: another connection attached?
    live = _sidecars_present(target)
    if live:
        return _cleanup_skipped(
            target,
            ", ".join(p.name for p in live) + " exist (another process may have it open)",
            kind,
        )

    # Step 4: provably no configuration inside? (non-empty files only)
    if size != 0:
        try:
            empty = _probe_config_empty(target)
        except sqlite3.Error as exc:
            return _cleanup_skipped(
                target, f"could not confirm it is empty ({exc})", kind
            )
        if not empty:
            return _cleanup_skipped(
                target, "it now holds a configuration", "imported", interrupted
            )
        # NOTE: no fingerprint retake here. The probe's lock is released, so
        # its verdict is stale; step 5 compares against the fingerprint taken
        # BEFORE the probe, or a writer landing in this gap would become the
        # baseline. See the module docstring.

    # Step 5: the probe's connection is closed; re-check, then unlink.
    if _file_id(target) != created_id:
        return _cleanup_skipped(target, "it was replaced during cleanup", "replaced")
    if _fingerprint(target) != fingerprint:
        return _cleanup_skipped(
            target,
            "it changed since it was checked -- another process may have "
            "written to it",
            "changed",
        )
    live = _sidecars_present(target)
    if live:
        return _cleanup_skipped(
            target,
            "another process opened it during cleanup: "
            + ", ".join(p.name for p in live) + " appeared",
            kind,
        )
    try:
        os.unlink(target)
    except FileNotFoundError:
        return
    except OSError as exc:
        return _cleanup_skipped(target, f"could not unlink it: {exc}", kind)
    logger.info("removed %s, created by this failed import", target)


def _backup_outgoing(
    session,  # noqa: ANN001 - a writer Session, mid-transaction
    backup: Callable[[BackupConfig], Path],
    allow_unreadable: bool,
    target: Path,
) -> Optional[Path]:
    """Phase 3's pre-import backup step. See `import_yaml`'s `backup`.

    "There is an outgoing configuration" means `GlobalSettings` row 1
    exists: `save_config` writes it in the same transaction as all the data,
    so its absence is exactly a fresh or schema-only database (nothing to
    lose, no backup). Anything that goes wrong *loading* an existing one --
    `load_config`'s own refusals (no datasets, a bad duration row, an orphan
    scope) and any storage error -- is "unreadable", deliberately one
    category: the question the caller is asking is only "can I snapshot what
    I am about to destroy". A failure of `backup` itself is NOT caught here:
    it propagates, and phase 3 rolls back.
    """
    try:
        has_config = session.get(GlobalSettings, 1) is not None
        outgoing = load_config(session) if has_config else None
    except Exception as exc:  # noqa: BLE001 - any failure to read is "unreadable"
        if not allow_unreadable:
            raise OutgoingConfigUnreadable(
                f"The configuration currently stored in {target} cannot be "
                f"read ({type(exc).__name__}: {exc}), so it cannot be backed "
                "up before being replaced. Nothing was changed."
            ) from exc
        logger.warning(
            "The configuration currently stored in %s cannot be read (%s: "
            "%s); replacing it WITHOUT a backup.",
            target, type(exc).__name__, exc,
        )
        return None
    if outgoing is None:
        return None
    return backup(outgoing)


def import_yaml(
    yaml_path: Union[str, Path],
    resolved: ResolvedConfigPath,
    *,
    create: bool = True,
    backup: Optional[Callable[[BackupConfig], Path]] = None,
    allow_unreadable_outgoing: bool = False,
) -> ImportResult:
    """Import `yaml_path` (a `BackupConfig.from_file`-shaped YAML config)
    into the config database `resolved` addresses, replacing its entire
    contents. See this module's docstring for the full phase breakdown
    and the two nesting/commit hazards phases 2 and 3 avoid.

    `create=True` (the default): create the target file (and its
    containing directory, at `CONFIG_DIR_MODE`) if nothing exists there
    yet. `create=False`: require an existing target and raise
    `ConfigDbNotFound` if there is none -- for a caller that wants
    "re-import only", never "import or create".

    `backup` (item 10.1, D-5): called with the OUTGOING configuration -- the
    one this import is about to destroy -- and must write it somewhere and
    return the path (`ImportResult.backup_path`). It runs in phase 3, inside
    the same `BEGIN IMMEDIATE` as `save_config`, so the config it saves is
    exactly the one that gets replaced: nothing can commit between the two.
    If `backup` raises, phase 3 rolls back and the exception propagates; the
    stored configuration is unchanged (the schema may already have been
    upgraded by phase 2). If a LATER step fails after `backup` succeeded
    (the file is already written), the exception gets a note naming it.
    It is not called when there is nothing to back up (a database this call
    just created, or one that holds no configuration yet), and it is not
    called -- and the outgoing config is not even loaded -- when `backup` is
    `None`.

    An outgoing configuration that exists but cannot be loaded raises
    `OutgoingConfigUnreadable` (the stored configuration is not replaced) unless
    `allow_unreadable_outgoing` is True, in which case a WARNING is logged,
    no backup is taken, and the import proceeds. Only meaningful with
    `backup`; without one the outgoing config is never read.
    """
    # Phase 0 -- parse first. Nothing below this line runs if the YAML
    # itself is invalid, so a bad YAML never touches the filesystem
    # target at all.
    config = BackupConfig.from_file(Path(yaml_path))
    # ...and validate it with the same session-free check `save_config` runs,
    # so nothing `save_config` would refuse in phase 3 can first have
    # created, migrated and stamped the target. Warnings are logged here;
    # `save_config` validates again in phase 3 but never logs, so they are
    # not printed twice.
    for message in validate_config(config):
        logger.warning("%s: %s", yaml_path, message)

    # Guard, ahead of phase 1: a `.yaml`/`.yml`-suffixed TARGET is the
    # same class of misuse as a bad source YAML, just reached the other
    # way around -- an operator typo'ing `-c config.yaml` instead of
    # `-c config.db` must get refused before anything is created, not
    # after phase 1 has already left a stray zero-byte file at the path
    # they will likely retry (which would then look exactly like the
    # zero-byte artifact `check_config_db`'s own docstring describes,
    # confusing the NEXT preflight too). `check_config_db_suffix` is the
    # same decision `check_config_db`'s step 3 makes -- see its docstring
    # for why this is safe to call before the target exists, and why it
    # is not a duplicate check: it is the one place both this guard and
    # `check_config_db` call, so they cannot drift apart. Phases 1-3 keep
    # their exact sequence below; this is a guard in front of them, not a
    # change to their order.
    check_config_db_suffix(resolved)

    # Phase 1 -- bring the target file into existence, or fix its mode.
    # The create=False refusal is decided FIRST, before anything below it
    # touches the filesystem: a `create=False` call against a missing
    # target (and a missing containing directory) must leave no directory
    # behind either -- the same "misuse creates nothing" property the
    # suffix guard above gives for a `.yaml` target. Measured before this
    # ordering: `create=False` against `<tmp>/nodir/config.db` raised
    # `ConfigDbNotFound` correctly but still left `<tmp>/nodir` created at
    # `0770` -- `ensure_config_dir` had already run on the branch that was
    # about to fail anyway.
    target = resolved.resolved_path
    existing = _target_exists(target)

    if not existing and not create:
        raise ConfigDbNotFound(
            f"No config database exists at {target}, and create=False was "
            "passed to import_yaml() -- this call only replaces an "
            "EXISTING database. Pass create=True (the default) to create "
            "one, or point at an existing database to re-import over it."
        )

    # A-11.2: orphaned `-wal`/`-shm`/`-journal` next to an ABSENT database
    # are refused before anything is created, chmod'd or otherwise touched
    # (the directory included -- a directory holding sidecars exists, so
    # this cannot be reached with a missing one). See the module docstring.
    if not existing:
        _refuse_orphaned_sidecars(target)

    # Only the branch that will actually create/touch the target may also
    # create/touch its containing directory -- see _prepare_target_dir's
    # own docstring for why that call is gated the way it is.
    _prepare_target_dir(target.parent)

    if existing:
        ensure_config_db_mode(target)
        created = False
        created_id = None
    else:
        # Remember which inode the new file got, so the cleanup below only
        # ever considers the file THIS call created.
        create_config_db_file(target)
        created = True
        created_id = _file_id(target)

    backup_path: Optional[Path] = None
    try:
        # Phase 2 -- schema, its own connection, committed and closed before
        # phase 3 opens anything. `allow_new=created`: only a database THIS
        # call just created is the zero-byte file `check_config_db` would
        # otherwise refuse; an existing target re-imported over is already
        # a real SQLite file and must still be refused if it is not one.
        with open_config_connection(
            resolved, readonly=False, foreign_keys=False, allow_new=created
        ) as conn:
            head_revision = ensure_schema(conn)
            # LAST statement on this connection. See module docstring.
            conn.commit()

        # Phase 3 -- data, in a brand-new writer session opened only now
        # that phase 2's connection has fully closed.
        with open_config_session(resolved, readonly=False) as session:
            # First statement of the session: `BEGIN IMMEDIATE` (db.py), so
            # everything below -- the counts, the outgoing config handed to
            # `backup`, and `save_config`'s wipe -- sees one frozen state.
            replaced_datasets = len(session.scalars(select(Dataset)).all())
            replaced_destinations = len(session.scalars(select(Destination)).all())
            if backup is not None:
                backup_path = _backup_outgoing(
                    session, backup, allow_unreadable_outgoing, target
                )
                # No `session.expunge_all()` here: `save_config`'s bulk
                # `delete()`s (default `synchronize_session="auto"`) already
                # evict every row `load_config` loaded, and the re-inserted
                # rows reusing the same primary keys then flush cleanly
                # (checked: re-import with a backup callback, twice, with
                # and without an explicit expunge -- identical). An earlier
                # `expunge_all()` guarded nothing observable.
            if replaced_datasets or replaced_destinations:
                logger.warning(
                    "Replacing existing config store at %s: %d dataset(s) and "
                    "%d destination(s) will be discarded and replaced by the "
                    "contents of %s",
                    target, replaced_datasets, replaced_destinations, yaml_path,
                )
            save_config(session, config)
    except BaseException as exc:
        if backup_path is not None:
            if isinstance(exc, Exception):
                outcome = "the stored configuration was not replaced."
            else:
                # KeyboardInterrupt etc. can land after phase 3's COMMIT
                # returned (post-commit bookkeeping, session close), so
                # neither outcome may be asserted.
                outcome = (
                    "the import was interrupted; it may or may not have "
                    "been applied (run `zfsbackup-config export` to check)."
                )
            exc.add_note(
                f"A pre-import backup of the outgoing configuration was "
                f"already written to {backup_path}; {outcome}"
            )
        if created:
            _remove_created_target(target, created_id, exc)
        raise

    return ImportResult(
        created=created,
        head_revision=head_revision,
        replaced_datasets=replaced_datasets,
        replaced_destinations=replaced_destinations,
        backup_path=backup_path,
    )


def _main(argv: Optional[List[str]] = None) -> int:
    """See this module's "8d -- interim CLI" docstring section."""
    parser = argparse.ArgumentParser(
        prog="python -m zfsbackup.config.store.importer",
        description="INTERIM tool, deleted by item 10 -- import a YAML "
        "zfsbackup config into the SQLite config store.",
    )
    parser.add_argument("yaml_path", type=Path, help="YAML config to import.")
    # Deliberately NOT type=Path: str(Path("")) == ".", which would turn
    # `-c ""` into "the current directory" and defeat resolve_config_path's
    # documented fallthrough to ZFSBACKUP_CONFIG. See paths.py's F3.
    parser.add_argument(
        "-c", "--config", default=None,
        help=f"Config database path (default: ${CONFIG_PATH_ENV}, or {DEFAULT_CONFIG_DB}).",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    try:
        resolved = resolve_config_path(args.config)
        result = import_yaml(args.yaml_path, resolved)
    except (
        ConfigPathError,
        SchemaError,
        ValueError,
        TypeError,
        FileNotFoundError,
        yaml.YAMLError,
        # Interim, until item 10.3 deletes this wrapper: a storage-level
        # failure (locked database, disk full, constraint) is an operator
        # message, not a traceback.
        SQLAlchemyError,
    ) as exc:
        # ValueError/TypeError/yaml.YAMLError: BackupConfig.from_file's own
        # failure modes for a malformed YAML -- a bad parse
        # (yaml.YAMLError), a value of the wrong shape rejected by
        # config/model.py's own guards (ValueError), or something coerced
        # with the wrong type, e.g. `int(api_port)` against a mapping/list
        # (TypeError). A malformed YAML is *the* likely error for an
        # import tool, so these must produce this tool's own clean
        # "error: ..." line, not a bare traceback.
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Imported {args.yaml_path} into {resolved.resolved_path}: "
        f"created={result.created} head={result.head_revision} "
        f"replaced {result.replaced_datasets} dataset(s), "
        f"{result.replaced_destinations} destination(s)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(_main())

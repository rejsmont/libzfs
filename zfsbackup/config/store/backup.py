"""Online backup of the config database (plan item C8, S-11a).

`export` is not a backup: it omits the trust tables and `host_identity`, and
a plain file copy of `config.db` while a `-wal` is pending silently loses the
most recent commits (see `db.py`, "WAL sidecars"). `backup_database` takes a
consistent copy of everything -- the six config tables, the trust tables,
`host_identity`, `alembic_version` and the `PRAGMA user_version` store id --
using SQLite's online backup API while the daemon and the CLI keep running.

How it stays safe
-----------------
- **No writer lock, one short read snapshot.** The source is a fresh,
  unpooled, read-only store connection (`open_config_connection(readonly=True,
  ephemeral=True)`: a pooled one could be holding an older snapshot), and the
  backup API copies every page in a single step (`pages=-1`) into an
  IN-MEMORY database. Under WAL a writer committing mid-copy is neither
  blocked by it nor visible in it, so the copy is one commit's state, never a
  mix. Everything slow (hashing, writing, fsync) happens AFTER the source
  connection is closed, so the WAL read mark is never held across disk I/O at
  the destination. Because it takes no lock, it can run while the caller
  already holds the writer lock (the pre-migration backup, C12).
- **SQLite never opens anything at the destination.** The copy is checked
  (`PRAGMA quick_check`) and serialised in memory (`Connection.serialize`),
  and the bytes are written with plain `os.write` to an `O_EXCL`, mode-0600
  file. SQLite resolves its `-wal`/`-shm`/`-journal` sidecars BY NAME, and the
  copy's header carries the WAL flag; had SQLite opened the temporary file, an
  attacker able to plant `<tmp>-wal` in a shared-writable destination (macOS,
  or a Linux without `fs.protected_regular`) would have had that "WAL" replayed
  into the published backup with every check passing. The in-memory step also
  means the copy has no sidecars to clean up.
- **No raw handle on the DB file.** The source is read through the store's
  own tracked connection (`_liveconn`); this module never `open()`s or
  `close()`s `config.db`, so it cannot drop a live connection's POSIX locks.
- **Bounded.** A config database is a few hundred kilobytes; a source larger
  than `MAX_BACKUP_BYTES` (256 MiB, held in memory once for the copy and once
  serialised) is refused. A source that is not in WAL mode is refused up
  front (the store always uses WAL): CPython's `Connection.backup` retries
  BUSY forever inside C, ignoring signals, so a rollback-journal source with a
  writer could hang a backup for good. As defence in depth the backup's
  `progress` callback, which CPython calls after every step including each
  BUSY retry, raises once `BACKUP_DEADLINE_SECONDS` (30 s) have passed.
- **Atomic, private, durable, never clobbering.** Bytes are written to
  `.<name>.tmp-<hex>` (0600), fsynced, then HARD-LINKED to the final name
  (`link` fails if the name exists, so a concurrent or earlier backup is never
  replaced; a filesystem without hard links is refused rather than falling
  back to an overwriting rename), and the directory is fsynced. If the
  directory fsync fails the published name is removed again: nothing is left
  under the final name unless it is durable. A crash leaves at worst a
  `.<name>.tmp-*` file with partial bytes, never a partial file under the final
  name; a crash between `link` and the temp's `unlink` leaves two names for
  one complete file. Mode 0600 because the copy holds pinned fingerprints and
  enrollment tokens.
- **Needs write access to the source.** The source connection is opened with
  the store's usual preflight (`require_writable=True`): a reader that lacks
  write access but can create files would leave its own `-wal`/`-shm`
  sidecars behind, which the stuck-sidecar guard exists to prevent. A backup
  is therefore a writer-or-root operation (the CLI's group operators and root).
  The schema is NOT required to be current: the point of a pre-migration
  backup is to capture a database that is behind.
- **Faithful.** The copy is page for page, so `user_version` (the store id),
  `alembic_version` and the WAL flag in the header come across unchanged.

Restoring a backup is NOT a no-op for security state (read this before
writing a runbook)
---------------------------------------------------------------------
A backup is a snapshot in time, and it includes the trust tables. Restoring
one brings back every enrollment token consumed since, and un-revokes every
fingerprint revoked or retired since: it breaks "revocation is permanent" for
the window between the backup and the restore. After restoring, re-check
`trust list` and re-revoke what was revoked. Likewise, API-worker trust
writes (`/enroll`, `/rotate`) can land between a pre-migration backup and the
upgrade that follows it, so rolling back to that backup loses them (a client
enrolled in that window must enroll again).
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import secrets
import sqlite3
import stat
import time
from pathlib import Path
from typing import NamedTuple, Optional, Union

from zfsbackup.config.store.paths import (
    ConfigDbPermissionError,
    ResolvedConfigPath,
    open_config_connection,
)

__all__ = [
    "BackupBusy",
    "BackupError",
    "BackupInfo",
    "BackupPublishError",
    "BackupSourceError",
    "backup_database",
]

#: Name used when `dst` is an existing directory.
DEFAULT_BACKUP_NAME = "config.db"

#: Largest source database accepted (config databases are KiB..MiB).
MAX_BACKUP_BYTES = 256 * 1024 * 1024

#: Wall-clock budget for the page copy (a defence in depth; see the module
#: docstring).
BACKUP_DEADLINE_SECONDS = 30.0

_BACKUP_MODE = 0o600


class BackupError(RuntimeError):
    """The backup could not be made, and nothing was published.

    Callers branch on the subclasses, never on the message text.
    """


class BackupBusy(BackupError):
    """The source stayed locked past the deadline; retry later (CLI: 75)."""


class BackupSourceError(BackupError):
    """The source database cannot be backed up as it is: not in WAL mode,
    oversized, failing `quick_check`, or unreadable mid-copy (CLI: 78)."""


class BackupPublishError(BackupError):
    """The copy was made but could not be published durably: no hard links on
    the destination filesystem, or the directory fsync failed (CLI: 1)."""


class BackupInfo(NamedTuple):
    """What a backup recorded, for the CLI's MANIFEST."""

    store_id: int              # PRAGMA user_version (0 = never stamped)
    generation: int            # global_settings.generation (0 = no such row)
    schema_rev: Optional[str]  # alembic_version.version_num, None if absent
    sha256: str                # hex digest of the backup file


def _scalar(conn: sqlite3.Connection, sql: str):
    try:
        row = conn.execute(sql).fetchone()
    except sqlite3.OperationalError:
        return None  # table absent (a legacy / never-stamped database)
    return row[0] if row else None


def _lstat_or_none(path: Path) -> Optional[os.stat_result]:
    """`lstat`, with "absent" meaning ONLY `FileNotFoundError`; any other
    error (EACCES on the directory, ELOOP, ...) is raised, never read as
    "nothing there"."""
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None


def _is_directory(path: Path) -> bool:
    try:
        return stat.S_ISDIR(os.stat(path).st_mode)
    except FileNotFoundError:
        return False


def _exists_message(target: Path) -> str:
    return f"{target} already exists; refusing to overwrite a backup"


def _fsync_dir(directory: Path) -> None:
    """fsync `directory`; raises `OSError` -- a rename that may not survive a
    crash is not a durable backup, so the caller must know."""
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def _snapshot_to_memory(source: sqlite3.Connection, where: Path) -> sqlite3.Connection:
    """Copy `source` into a new in-memory database in one backup step, with
    the refusals described in the module docstring."""
    mode = source.execute("PRAGMA journal_mode").fetchone()[0]
    if str(mode).lower() != "wal":
        raise BackupSourceError(
            f"{where} is not in WAL journal mode (it is {mode!r}); refusing "
            "to back it up online, because a writer could make the copy wait "
            "without end. The store switches a database to WAL on its first "
            "write by zfsbackup-config; run any write (or `migrate`) once and "
            "retry."
        )
    pages = source.execute("PRAGMA page_count").fetchone()[0]
    size = pages * source.execute("PRAGMA page_size").fetchone()[0]
    if size > MAX_BACKUP_BYTES:
        raise BackupSourceError(
            f"{where} is {size} bytes, larger than the {MAX_BACKUP_BYTES} "
            "byte limit for an online backup; this is not a plausible "
            "zfsbackup config database"
        )
    deadline = time.monotonic() + BACKUP_DEADLINE_SECONDS

    def _progress(status: int, remaining: int, total: int) -> None:
        if time.monotonic() > deadline:
            raise BackupBusy(
                f"backup of {where} did not finish within "
                f"{BACKUP_DEADLINE_SECONDS:.0f}s (the database stayed locked)"
            )

    memory = sqlite3.connect(":memory:")
    try:
        # One step: one read snapshot of the whole file. `sleep` is how long
        # CPython waits between BUSY retries; `_progress` runs after each.
        source.backup(memory, pages=-1, progress=_progress, sleep=0.05)
    except BaseException:
        memory.close()
        raise
    return memory


def backup_database(
    resolved: ResolvedConfigPath, dst: Union[str, "os.PathLike[str]"]
) -> BackupInfo:
    """Write a consistent online copy of the config database and describe it.

    `dst` is the destination FILE; if it is an existing directory the copy is
    written as `dst/config.db`. The parent directory must exist.

    Raises `FileExistsError` if the destination is already there (a backup is
    never overwritten); `ConfigPathError` subclasses for a source
    path/permission problem (the store's preflight, which requires write
    access to the database and its directory) or a destination that cannot be
    written (`ConfigDbPermissionError`); `BackupError` subclasses: `BackupSourceError` (non-WAL or
    oversized source, failed `PRAGMA quick_check`, unreadable mid-copy),
    `BackupBusy` (the copy exceeded its deadline, or the source stayed locked),
    `BackupPublishError` (a filesystem without hard links, or a failure to make
    the result durable). Nothing is left under the final name on failure.

    Returns `BackupInfo(store_id, generation, schema_rev, sha256)`, read from
    the COPY (so they describe exactly the bytes written and hashed). See the
    module docstring for the locking and durability contract.

    **A backup includes the trust tables, and restoring it is not neutral.**
    It brings back every enrollment token consumed, and un-revokes every
    fingerprint revoked or retired, since the backup was taken (this breaks
    "revocation is permanent" for that window); and API-worker trust writes
    can land between a pre-migration backup and the upgrade, so restoring that
    backup loses them. Re-check `trust list` after a restore.
    """
    target = Path(dst)
    if _is_directory(target):
        target = target / DEFAULT_BACKUP_NAME
    directory = target.parent
    if _lstat_or_none(target) is not None:
        raise FileExistsError(_exists_message(target))

    tmp = directory / f".{target.name}.tmp-{secrets.token_hex(6)}"
    published = False
    created = False
    try:
        # Preflight AND snapshot first, so a source problem never leaves a
        # file behind; the destination is only touched once the bytes exist.
        with open_config_connection(
            resolved, readonly=True, ephemeral=True
        ) as connection:
            source = connection.connection.driver_connection
            try:
                memory = _snapshot_to_memory(source, resolved.path)
            except sqlite3.OperationalError as exc:
                # SQLITE_BUSY (5) / SQLITE_LOCKED (6), primary codes.
                busy = (getattr(exc, "sqlite_errorcode", 0) or 0) & 0xFF in (5, 6)
                cls = BackupBusy if busy else BackupSourceError
                raise cls(f"backup of {resolved.path} failed: {exc}") from exc
        # The source connection (and its WAL read mark) is gone from here on.
        try:
            check = _scalar(memory, "PRAGMA quick_check")
            if check != "ok":
                raise BackupSourceError(
                    f"backup of {resolved.path} failed quick_check: {check}"
                )
            store_id = int(_scalar(memory, "PRAGMA user_version") or 0)
            generation = int(
                _scalar(memory, "SELECT generation FROM global_settings WHERE id = 1") or 0
            )
            schema_rev = _scalar(memory, "SELECT version_num FROM alembic_version")
            data = memory.serialize()
        finally:
            memory.close()
        digest = hashlib.sha256(data).hexdigest()

        try:
            # O_EXCL + 0600 from the first byte: the copy holds trust material.
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _BACKUP_MODE)
        except PermissionError as exc:
            raise ConfigDbPermissionError(
                f"Cannot write the backup into {directory}: {exc.strerror or exc}. "
                "The directory must be writable and searchable by the user "
                "running the backup."
            ) from exc
        created = True
        try:
            os.fchmod(fd, _BACKUP_MODE)  # exact, whatever the umask
            _write_all(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)

        try:
            # `link` is an atomic no-clobber publish.
            os.link(tmp, target)
        except FileExistsError:
            raise FileExistsError(_exists_message(target)) from None
        except OSError as exc:
            raise BackupPublishError(
                f"cannot publish the backup as {target}: {exc.strerror or exc}. "
                "The destination filesystem must support hard links (a plain "
                "rename could overwrite a concurrent backup)."
            ) from exc
        published = True
        # Only the unlink of the temp name and the directory fsync follow.
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        try:
            _fsync_dir(directory)
        except OSError as exc:
            raise BackupPublishError(
                f"could not make the backup {target} durable (fsync of "
                f"{directory} failed: {exc.strerror or exc}); it was removed"
            ) from exc
        published = False  # durable: from here nothing may undo it
        return BackupInfo(
            store_id,
            generation,
            None if schema_rev is None else str(schema_rev),
            digest,
        )
    except BaseException:
        if published:
            with contextlib.suppress(OSError):
                os.unlink(target)
        if created:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
        raise

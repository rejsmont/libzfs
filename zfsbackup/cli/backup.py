"""`zfsbackup-config backup DIR [--no-keys]` -- a consistent backup of this
host's configuration database and TLS identities (plan item C11, S-11c).

`export` is not a backup: it omits the trust tables, `host_identity` (the
client id) and the keys. This command writes, under `DIR`, a directory

    zfsbackup-<host>-<UTC time>/        mode 0700
        config.db                       store.backup_database (C8), 0600
        tls/                            identity.copy_tls_dir (C4), 0700
        MANIFEST                        what was captured, written last

A restore is "put `config.db` at the configured path and `tls/` next to it as
`<db>.tls`"; a client restored from a backup that includes the keys keeps its
client id and its pinned identity and does not enroll again.

Contract:

- **What is and is not included.** The database, and the files of `<db>.tls/`
  (`server.*`, `client.*`, `client.next.*`). A `client_cert_file` /
  `client_key_file` configured OUTSIDE `<db>.tls/` is the operator's own and is
  NOT copied: back those up separately, or a restore will not have them.
- **Read-only towards the host.** No writer lock, no signal to the daemon
  (the store backup is one read snapshot; `copy_tls_dir` takes
  `identity.lock` itself). `backup_database` needs write access to the
  database (root or the operator group), which the group operator has, but
  not the root-owned 0600 private keys: that case is refused with a pointer to `--no-keys`, never
  silently downgraded -- a backup that quietly lacks the keys would be a
  trap at restore time.
- **All or nothing.** The directory is built in place and `MANIFEST` is its
  last file, so a directory with a MANIFEST is complete. Any failure, ^C or
  SIGTERM removes the partial directory again (with SIGINT and SIGTERM blocked while it
  does, MANIFEST removed first, so a cut cleanup never leaves a MANIFEST beside
  missing files). A `DIR` this command
  had to create is removed too if nothing else is in it.
- **Never overwrites.** A name taken within the same second gets a `-N`
  suffix.

Exit codes are the CLI's (`errors.py`), mapped from the store's exception
classes (`BackupBusy` 75, `BackupPublishError` 1, `BackupSourceError` or any
other `BackupError` 78, `ConfigPathError` 78): 1 for a refusal (unreadable keys,
an unusable destination), 78 when the database is unusable, 75 when it is
busy, 130/143 for ^C / SIGTERM (`main._Group`).
"""

import contextlib
import errno
import json
import os
import re
import shutil
import signal
import socket
import sqlite3
import stat
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

import click

from zfsbackup import identity
from zfsbackup.cli import tlsfiles
from zfsbackup.cli.errors import CliError, ConfigUnusable, RetryLater
from zfsbackup.config.store import (
    BackupBusy,
    BackupError,
    BackupInfo,
    BackupPublishError,
    ResolvedConfigPath,
    backup_database,
    dispose_all,
    read_store_state,
)

__all__ = [
    "register", "run_backup", "backup_before_migrate", "BackupResult", "PreMigrationBackup",
    "MANIFEST_NAME",
]

MANIFEST_NAME = "MANIFEST"
CONFIG_NAME = "config.db"
TLS_NAME = "tls"
_DIR_MODE = 0o700
_FILE_MODE = 0o600

_HOST_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


class BackupResult:
    """What `run_backup` wrote."""

    def __init__(
        self,
        path: Path,
        info: BackupInfo,
        tls_files: Optional[List[str]],
        keys_included: bool,
        created_utc: datetime,
        version: str,
        host: str,
        source: Path,
    ):
        self.path = path
        self.info = info
        #: Names copied into `tls/`; `None` when there is no `<db>.tls`.
        self.tls_files = tls_files
        self.keys_included = keys_included
        self.created_utc = created_utc
        self.version = version
        self.host = host
        self.source = source

    def as_json(self) -> Dict[str, Any]:
        return {
            "path": str(self.path),
            "version": self.version,
            "created": _iso(self.created_utc),
            "host": self.host,
            "source": str(self.source),
            "config_db": {
                "file": CONFIG_NAME,
                "schema_revision": self.info.schema_rev,
                "store_id": self.info.store_id,
                "generation": self.info.generation,
                "sha256": self.info.sha256,
            },
            "tls": None if self.tls_files is None else {
                "dir": TLS_NAME,
                "files": list(self.tls_files),
            },
            "keys_included": self.keys_included,
        }

    def summary(self) -> str:
        if self.tls_files is None:
            tls = "no TLS directory"
        elif self.keys_included:
            tls = f"{len(self.tls_files)} TLS file(s) incl. private keys"
        else:
            tls = f"{len(self.tls_files)} TLS file(s), NO private keys"
        rev = self.info.schema_rev or "none"
        return (
            f"config.db (schema {rev}, generation {self.info.generation}, "
            f"store id {self.info.store_id}), {tls}"
        )


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _host_label() -> str:
    """The host name as one safe path component."""
    label = _HOST_UNSAFE.sub("-", socket.gethostname() or "").strip("-.")
    return label or "host"


_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
#: How a directory this module CREATED is opened: never through a symlink.
_NEW_DIR_FLAGS = os.O_RDONLY | _O_CLOEXEC | _O_DIRECTORY | _O_NOFOLLOW


def _mkdir_private(name: str, dir_fd: Optional[int] = None) -> int:
    """Create the directory `name` (relative to `dir_fd` if given) and return a
    descriptor for it, held for as long as the caller works inside.

    The new directory is opened `O_DIRECTORY|O_NOFOLLOW` right after `mkdir`,
    checked to be ours (owned by this euid), and `fchmod`ed to exactly 0700 --
    never `chmod` by path, which a symlink swapped into the name in between
    would redirect (this runs as root in a parent someone else may write to).
    Raises `FileExistsError` if the name is taken, `OSError` otherwise; on any
    failure after the `mkdir` the new directory is removed again."""
    os.mkdir(name, _DIR_MODE, dir_fd=dir_fd)
    try:
        fd = os.open(name, _NEW_DIR_FLAGS, dir_fd=dir_fd)
    except BaseException:
        with contextlib.suppress(OSError):
            os.rmdir(name, dir_fd=dir_fd)  # refuses a symlink
        raise
    try:
        if os.fstat(fd).st_uid != os.geteuid():
            raise OSError(errno.EPERM, "the new directory is not owned by this user")
        os.fchmod(fd, _DIR_MODE)  # exactly, whatever the umask or a setgid parent
    except BaseException:
        os.close(fd)
        with contextlib.suppress(OSError):
            os.rmdir(name, dir_fd=dir_fd)
        raise
    return fd


def _same_directory(parent_fd: Optional[int], name: str, fd: int) -> bool:
    """Does `name` (in `parent_fd`, or a path) still lead to the directory `fd`
    is open on, and not to a symlink or another directory put in its place?"""
    try:
        now = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return False
    held = os.fstat(fd)
    return (now.st_dev, now.st_ino) == (held.st_dev, held.st_ino) and stat.S_ISDIR(now.st_mode)


def _require_same(parent_fd: Optional[int], name: str, fd: int, shown: Path) -> None:
    if not _same_directory(parent_fd, name, fd):
        raise CliError(f"{shown} was replaced while the backup was being written; nothing was kept")


def _open_dest(dest_dir: Path) -> Tuple[int, Optional[Path]]:
    """`(fd, created)` for the destination directory `dest_dir`: an existing one
    (the operator's own argument, symlinks followed) or one created here, 0700
    (`created` is then its path, so a failure can remove it again)."""
    if not os.path.lexists(dest_dir):
        try:
            fd = _mkdir_private(str(dest_dir))  # the parent must exist
        except OSError as exc:
            raise CliError(f"cannot create {dest_dir}: {exc.strerror or exc}") from exc
        return fd, dest_dir
    try:
        fd = os.open(dest_dir, os.O_RDONLY | _O_CLOEXEC | _O_DIRECTORY)
    except NotADirectoryError:
        raise CliError(f"{dest_dir} is not a directory") from None
    except OSError as exc:
        raise CliError(f"cannot use {dest_dir}: {exc.strerror or exc}") from exc
    return fd, None


def _make_backup_dir(parent_fd: int, parent: Path, stem: str) -> Tuple[str, int]:
    """Create `parent/stem` (0700, exclusively; `-N` if the name is taken) ->
    `(name, fd)`."""
    for attempt in range(1, 100):
        name = stem if attempt == 1 else f"{stem}-{attempt}"
        try:
            return name, _mkdir_private(name, parent_fd)
        except FileExistsError:
            continue
        except OSError as exc:
            raise CliError(f"cannot create a backup directory in {parent}: {exc.strerror or exc}") from exc
    raise CliError(f"no free backup directory name for {stem} in {parent}")


def _manifest_text(result: BackupResult) -> str:
    info = result.info
    lines = [
        "zfsbackup configuration backup",
        f"version: {result.version}",
        f"created: {_iso(result.created_utc)}",
        f"host: {result.host}",
        f"source: {result.source}",
        f"schema_revision: {info.schema_rev if info.schema_rev is not None else 'none'}",
        f"store_id: {info.store_id}",
        f"generation: {info.generation}",
        f"config.db sha256: {info.sha256}",
        f"keys_included: {'yes' if result.keys_included else 'no'}",
    ]
    if result.tls_files is None:
        lines.append("tls: none (no TLS directory existed)")
    else:
        lines.append(f"tls: {len(result.tls_files)} file(s) in {TLS_NAME}/")
        lines.extend(f"  {TLS_NAME}/{name}" for name in result.tls_files)
    lines.append("")
    return "\n".join(lines)


def _write_manifest(directory: Path, text: str, dir_fd: Optional[int] = None) -> None:
    """Create MANIFEST in `directory` (through `dir_fd` when given, so a swapped
    path cannot redirect it), 0600, exclusively, fsynced."""
    target = MANIFEST_NAME if dir_fd is not None else directory / MANIFEST_NAME
    fd = os.open(
        target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC | _O_NOFOLLOW, _FILE_MODE,
        dir_fd=dir_fd,
    )
    try:
        os.fchmod(fd, _FILE_MODE)
        data = text.encode("utf-8")
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view):]
        os.fsync(fd)
    finally:
        os.close(fd)


def _remove_partial(
    parent_fd: Optional[int], name: Optional[str], fd: Optional[int], shown: Optional[Path],
    created_parent: Optional[Path],
) -> Optional[str]:
    """Remove the unfinished backup directory `name` (in `parent_fd`; `fd` is
    open on it) and a `DIR` we created if it is now empty. -> a note if
    something was left behind.

    MANIFEST goes first and the directory is fsynced, so a cleanup that is cut
    short (a crash, a SIGKILL) never leaves a MANIFEST beside missing files: a
    directory without one is plainly not a backup. SIGINT and SIGTERM are
    blocked meanwhile (they are delivered afterwards), so neither can stop the
    cleanup half-way."""
    try:
        old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
    except (ValueError, OSError):  # no such facility here: nothing to block with
        old_mask = None
    try:
        if name is not None:
            if fd is not None:
                with contextlib.suppress(OSError):
                    os.unlink(MANIFEST_NAME, dir_fd=fd)
                    os.fsync(fd)
            try:
                # A symlink put in the directory's place is unlinked, never
                # followed.
                if stat.S_ISLNK(os.stat(name, dir_fd=parent_fd, follow_symlinks=False).st_mode):
                    os.unlink(name, dir_fd=parent_fd)
                else:
                    shutil.rmtree(name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
            except OSError as exc:
                return (
                    f"the partial backup {shown} could not be removed ({exc.strerror or exc}); "
                    "delete it by hand, it is not a usable backup"
                )
        if created_parent is not None:
            with contextlib.suppress(OSError):
                os.rmdir(created_parent)  # only if empty
        return None
    finally:
        if old_mask is not None:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)


def _backup_db(resolved: ResolvedConfigPath, target: Path) -> BackupInfo:
    """`backup_database` to the file `target`, its errors mapped to the CLI's
    messages and exit codes."""
    try:
        return backup_database(resolved, target)
    except sqlite3.Error as exc:
        # The store wraps its mid-copy errors in `BackupError`; anything raw
        # that still escapes is the database, not the operator's input.
        raise ConfigUnusable(f"cannot back up {resolved.resolved_path}: {exc}") from exc
    except FileExistsError as exc:
        # Our own fresh directory has the name: a concurrent backup.
        raise CliError(str(exc)) from exc
    except BackupBusy as exc:
        raise RetryLater(f"{exc}; retry in a moment") from exc
    except BackupPublishError as exc:
        raise CliError(str(exc)) from exc
    except BackupError as exc:
        # BackupSourceError, and any other (unexpected) BackupError: the
        # database cannot be backed up as it is.
        raise ConfigUnusable(str(exc)) from exc


def _backup_tls(resolved: ResolvedConfigPath, directory: Path, keys: bool) -> Optional[List[str]]:
    source = tlsfiles.tls_directory(resolved)
    if not os.path.lexists(source):
        return None  # no identity was ever generated here
    try:
        return identity.copy_tls_dir(source, directory / TLS_NAME, include_keys=keys)
    except identity.KeyUnreadable as exc:
        raise CliError(
            f"cannot read the TLS private key {exc.filename}: permission denied. "
            "The keys are readable only by the user that owns the TLS directory "
            "(normally root, not a group operator). Run this as that user, or pass "
            "--no-keys to back up the database and the certificates only. A backup "
            "without keys cannot restore this host's TLS identity: it would have to "
            "enroll again."
        ) from None
    except identity.IdentityError as exc:
        raise CliError(f"cannot back up the TLS directory {source}: {exc}") from exc


def run_backup(resolved: ResolvedConfigPath, dest_dir: Path, *, keys: bool = True) -> BackupResult:
    """Write the backup under `dest_dir` and return what was captured; on any
    failure (including ^C) nothing is left behind. See the module docstring."""
    from zfsbackup.version import installed_version

    dest_fd, created_parent = _open_dest(dest_dir)
    name: Optional[str] = None
    fd: Optional[int] = None
    now = datetime.now(timezone.utc)
    host = _host_label()
    stem = f"zfsbackup-{host}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    path: Optional[Path] = None
    try:
        if created_parent is not None:
            _require_same(None, str(dest_dir), dest_fd, dest_dir)
        name, fd = _make_backup_dir(dest_fd, dest_dir, stem)
        path = dest_dir / name
        _require_same(dest_fd, name, fd, path)
        info = _backup_db(resolved, path / CONFIG_NAME)
        _require_same(dest_fd, name, fd, path)
        tls_files = _backup_tls(resolved, path, keys)
        _require_same(dest_fd, name, fd, path)
        result = BackupResult(
            path, info, tls_files, keys and tls_files is not None
            and any(f.endswith(".key") for f in tls_files),
            now, installed_version(), host, Path(resolved.resolved_path),
        )
        try:
            _write_manifest(path, _manifest_text(result), fd)
        except OSError as exc:
            raise CliError(f"cannot write {path / MANIFEST_NAME}: {exc.strerror or exc}") from exc
        for descriptor in (fd, dest_fd):
            with contextlib.suppress(OSError):  # a filesystem that cannot fsync a directory
                os.fsync(descriptor)
        return result
    except BaseException as exc:
        note = _remove_partial(dest_fd, name, fd, path, created_parent)
        if note and hasattr(exc, "add_note"):
            exc.add_note(note)
        raise
    finally:
        if fd is not None:
            os.close(fd)
        os.close(dest_fd)


class PreMigrationBackup(NamedTuple):
    """What `backup_before_migrate` left in place. `reused` is True when an
    identical backup (same revisions, store id and generation) already existed
    and nothing was written."""

    path: Path
    store_id: int
    generation: int
    schema_rev: Optional[str]
    reused: bool


def _backup_state(path: Path) -> Optional[Tuple[int, int, frozenset]]:
    """`(store_id, generation, {revisions})` of an existing backup FILE, or
    `None` if it cannot be read as one. Opened read-only and immutable (it is
    a finished copy, never the live database), so nothing is written next to it."""
    uri = "file:" + urllib.parse.quote(str(path)) + "?mode=ro&immutable=1"
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.Error:
        return None
    try:
        store_id = int(conn.execute("PRAGMA user_version").fetchone()[0] or 0)
        generation = conn.execute("SELECT generation FROM global_settings WHERE id = 1").fetchone()
        revs = frozenset(str(r[0]) for r in conn.execute("SELECT version_num FROM alembic_version"))
    except (sqlite3.Error, TypeError, ValueError):
        return None
    finally:
        conn.close()
    if generation is None:
        return None
    return store_id, int(generation[0]), revs


def _identical_backup(
    dir_fd: int, directory: Path, prefix: str, live: Tuple[int, int], from_revs: Tuple[str, ...]
) -> Optional[Path]:
    """The newest `<prefix>-<UTC>` backup in `directory` if it holds the same
    store id, generation and revisions as the live database (`live`), else
    `None`. A restart loop whose migration keeps failing after the backup would
    otherwise write an identical copy every time."""
    pattern = re.compile(re.escape(prefix) + r"-(\d{8}T\d{6}Z)(?:-(\d+))?")
    found_names = []
    for entry in os.listdir(dir_fd):
        match = pattern.fullmatch(entry)
        if match:
            # Newest = latest stamp, then the highest numeric `-N` suffix (a
            # plain name is the first of its second): "-10" is after "-9".
            found_names.append(((match.group(1), int(match.group(2) or 1)), entry))
    if not found_names:
        return None
    newest = directory / max(found_names)[1]
    found = _backup_state(newest)
    if found is not None and found == (live[0], live[1], frozenset(from_revs)):
        return newest
    return None


def backup_before_migrate(
    resolved: ResolvedConfigPath, directory: Path, from_revs: Tuple[str, ...]
) -> PreMigrationBackup:
    """The pre-migration backup (C12): the database only, as
    `DIRECTORY/config.db.pre-<from revision>-<UTC time>` (0600; `-N` if the
    name is taken -- a backup is never overwritten). `DIRECTORY` is created
    (0700, its parent must exist) when missing, and removed again if the backup
    then fails and left it empty. The caller holds the writer lock; this takes
    none (`backup_database` is a read snapshot). The TLS keys are not touched
    by a migration, so they are not copied.

    **No second copy of the same thing.** If the newest backup of this
    revision in `DIRECTORY` already holds the live database's store id and
    generation, it is kept as the backup and nothing is written
    (`reused=True`). The unit retries `migrate` every minute, and a migration
    that fails deterministically after its backup (a packaging mismatch, a
    foreign-key violation, a full disk) would otherwise fill the disk with
    identical files.

    Every failure -- an unwritable `DIRECTORY`, a source the store refuses to
    back up -- is a `CliError` (exit 1) saying nothing was migrated; the
    caller must not migrate after it."""
    dir_fd: Optional[int] = None
    created: Optional[Path] = None
    try:
        dir_fd, created = _open_dest(directory)
        if created is not None:
            _require_same(None, str(directory), dir_fd, directory)
        revs = "+".join(from_revs) or "unknown"
        prefix = f"{CONFIG_NAME}.pre-{revs}"
        try:
            live = read_store_state(resolved)
        except Exception:  # noqa: BLE001 - only an optimisation; the backup below reports real trouble
            live = None
        finally:
            dispose_all()
        if live is not None:
            same = _identical_backup(dir_fd, directory, prefix, (live.store_id, live.generation), from_revs)
            if same is not None:
                return PreMigrationBackup(same, live.store_id, live.generation, ", ".join(from_revs), True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        stem = f"{prefix}-{stamp}"
        for attempt in range(1, 100):
            target = directory / (stem if attempt == 1 else f"{stem}-{attempt}")
            try:
                info = backup_database(resolved, target)
            except FileExistsError:
                continue
            return PreMigrationBackup(target, info.store_id, info.generation, info.schema_rev, False)
        raise CliError(f"no free backup file name for {stem} in {directory}")
    except CliError:
        _discard_created(created)
        raise
    except Exception as exc:
        _discard_created(created)
        raise CliError(
            f"the pre-migration backup of {resolved.resolved_path} into {directory} failed: "
            f"{str(exc).rstrip('. ')}. The schema was NOT migrated."
        ) from exc
    except BaseException:
        _discard_created(created)
        raise
    finally:
        if dir_fd is not None:
            os.close(dir_fd)


def _discard_created(directory: Optional[Path]) -> None:
    if directory is not None:
        with contextlib.suppress(OSError):
            os.rmdir(directory)  # only if empty


def register(
    cli: click.Group,
    common: Callable[[Callable], Callable],
    get_state: Callable[[Optional[str], bool], Any],
) -> None:
    """Add `backup` to `cli`. `common` is `main.py`'s `-c`/`-v` decorator and
    `get_state(config_path, verbose)` its `_state` (this module does not
    import `main`)."""

    @cli.command(
        "backup",
        help=(
            "Back up the configuration database and this host's TLS identities.\n\n"
            "Writes DIR/zfsbackup-<host>-<UTC time>/ (mode 0700) holding config.db "
            "(a consistent copy of everything: configuration, host identity, trust "
            "tables), tls/ (the certificates and, by default, the PRIVATE KEYS of "
            "<db>.tls/) and a MANIFEST. A client_cert_file / client_key_file "
            "configured outside <db>.tls/ is NOT included. DIR is created if it "
            "does not exist. "
            "Nothing is signalled or locked, so it is safe while the daemon runs. "
            "A group operator cannot read the root-owned keys: the backup is "
            "refused (exit 1) unless --no-keys is given, and a backup without keys "
            "cannot restore this host's TLS identity (it would have to enroll again)."
        ),
    )
    @click.argument(
        "directory", metavar="DIR",
        type=click.Path(file_okay=False, path_type=Path),
    )
    @click.option(
        "--no-keys", "no_keys", is_flag=True, default=False,
        help="Do not copy the TLS private keys (certificates only).",
    )
    @click.option(
        "--json", "as_json", is_flag=True, default=False,
        help="Machine-readable JSON (stable shape; see zfsbackup/cli/backup.py).",
    )
    @common
    def backup_cmd(
        directory: Path, no_keys: bool, as_json: bool,
        config_path: Optional[str], verbose: bool,
    ) -> None:
        state = get_state(config_path, verbose)
        result = run_backup(state.resolve(), directory, keys=not no_keys)
        if as_json:
            click.echo(json.dumps(result.as_json(), indent=2, sort_keys=True))
            return
        click.echo(f"Backup written to {result.path}")
        click.echo(result.summary())
        if no_keys and result.tls_files:
            click.echo(
                "note: the private keys were not copied (--no-keys); restoring this "
                "backup will not restore the TLS identity.",
                err=True,
            )

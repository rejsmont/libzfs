"""Config database path resolution, creation policy, and permissions
(plan item 6a).

Scope: a `zfsbackup/config/store/` change. Nothing here builds an `Engine`
or opens a `Connection` except `open_config_session`/
`open_config_connection`, the sanctioned, preflighted, **context-managed**
ways to get one -- see their docstrings and review finding 3 below; both
hand back a `Session`/`Connection` scoped to a `with` block, never a bare
`Engine` a caller could stash across a `fork`. `check_config_db` does
only `stat`/`open`-for-header-bytes (never a SQLite open), and
`ensure_config_dir`/`ensure_config_db_mode`/`create_config_db_file` are
`mkdir`/`chmod`/`open(O_CREAT)` only.

**Resolving a config path is not filesystem-I/O-free, even though
choosing *which* of `-c`/`ZFSBACKUP_CONFIG`/the default wins is.**
`resolve_config_path` constructs a `ResolvedConfigPath` immediately (so,
in practice, at argparse time), and doing so calls `Path.resolve()` to
walk the path's components with `lstat`/`readlink` -- see
`ResolvedConfigPath`'s docstring and review finding 1. An earlier version
of this module's docstrings claimed the opposite; do not reintroduce that
claim.

The daemon (`daemon.py`/`workers.py`, item 8 Phase B) now reaches this
module exclusively through `zfsbackup/runtime_config.py`'s
`load_runtime_config()`, which sequences `open_config_connection`
(`readonly=True`, for `check_schema`) and `open_config_session`
(`readonly=True`, for `load_config`) as two separate, fully-closed
handles -- never nested, and never the `allow_new`-carrying writer path
`importer.py` uses. `BackupConfig.from_file` is gone from both
`daemon.py` and `workers.py`.

Why the existence check has to be a `stat`, not a caught exception
--------------------------------------------------------------------
`db.py`'s `make_engine(url, readonly=True)` does not, and structurally
cannot, refuse to create a missing file: `PRAGMA query_only=ON` is issued
*after* SQLite has already opened (and, for a missing path, created) the
database file. Measured directly against this repo's own `make_engine`:

    make_engine(url_for_path(<path that does not exist>), readonly=True).connect()

...succeeds, and leaves a zero-byte file behind. A typo'd `-c`, or the
default path on a host that was never `import`-ed, would otherwise
auto-create an empty config -- "back up nothing", silently. This is why
`check_config_db` takes a `ResolvedConfigPath`, not a URL: it must run
before `url_for_path` is even called, let alone `make_engine`.

The preflight and the actual open must agree on ONE file
------------------------------------------------------------
Two rounds of code review of this module found six issues, all now fixed
and worth recording so they are not reintroduced:

1. **Resolution itself must not escape `ConfigPathError`, on every
   Python version this package supports.** `ResolvedConfigPath` resolves
   `path` to an absolute, symlink-followed form **exactly once, at
   construction time** (`resolved_path`), via `_resolve_or_raise` --
   never a bare `Path.resolve()`. `pyproject.toml` declares
   `python = "^3.11"`, and on 3.11/3.12 (verified directly; 3.10 behaved
   the same but is no longer supported) `Path.resolve(strict=False)` turns a symlink loop into a bare
   `RuntimeError`, not `OSError` -- so `ZFSBACKUP_CONFIG` pointing at a
   loop used to crash `resolve_config_path` itself, before
   `check_config_db` ever ran, with an exception item 8's single `except
   ConfigPathError` does not catch. An unlinked current working
   directory (`FileNotFoundError` from the `os.getcwd()` a relative path
   needs) is the same hazard, version-independently. See
   `_resolve_or_raise`.
2. **The preflight and the actual open must resolve to the same file.**
   Every filesystem check in `check_config_db`, and `.url()` itself, read
   the one `resolved_path` value computed in (1) rather than
   re-resolving `path` independently. Measured before this fix: two
   `os.chdir()` calls between constructing one frozen `ResolvedConfigPath`
   and calling `.url()` twice produced two different URLs from the same
   "immutable" object -- and, separately, `check_config_db`'s
   directory-writability step used the *symlink's* parent directory
   (`path.parent`) while SQLite itself creates `-wal`/`-shm` next to the
   *resolved target* (`/etc/zfsbackup/config.db ->
   /var/lib/zfsbackup/config.db`, non-group-writable `/etc/zfsbackup`):
   the preflight rejected a deployment that opens fine, and the mirror
   case -- symlink directory writable, target directory read-only --
   passed a preflight that then failed, undiagnosed, at actual open.
   Resolving once, into one field every consumer shares, closes both.
3. **A database file that is not writable, and already in WAL mode, is
   now always refused -- there is no warn-and-continue arm.** An earlier
   version of step 6 warned instead of failing when the `-wal`/`-shm`
   sidecars happened to both already exist and be writable (matching
   measurement B: such a file opens fine). That "safe" state is
   transient in normal operation -- sidecars exist only while some other
   connection holds the database open, and are checkpointed away on its
   clean close -- so a preflight that ran while they happened to be
   present was a TOCTOU window into exactly the permanent-wedge state
   the rest of step 6 exists to prevent: this process's own (lazily
   opened) connection can be the one that arrives after the sidecars are
   gone, recreating them at this file's own unwritable mode, permanently.
   For a non-writable database file there is no stable safe state to
   warn about; a caller that genuinely only needs read access should
   pass `require_writable=False` instead.
4. **`open_config_session`/`open_config_connection`, not a function
   returning a bare `Engine`.** An earlier version exposed
   `open_config_db(...) -> Engine`, which reopened, by API shape, the
   exact fork hole `db.py`'s own module docstring closes: no public
   function there returns a bare, long-lived handle, because one
   captured before a `multiprocessing` fork is invisible to `get_engine`'s
   pid-keyed cache. An `Engine` stashed as `self._engine` during a
   supervisor's startup, with a live pooled connection from an earlier
   `load_config`, is inherited by every forked worker without any of
   them ever calling `get_engine` again -- the corruption hazard that
   cache exists for. This module's exports are context managers instead,
   for the same reason `session_scope`/`session_for_engine` are in
   `db.py`.
5. **The `.yaml`/`.yml` decision reads the resolved target, not the
   given spelling.** `check_config_db` decides this from `real.suffix`
   (the fully resolved path), not `resolved.path.suffix`. An operator
   keeping `/etc/zfsbackup/config.yaml` as a compatibility symlink to a
   fully migrated `/var/lib/zfsbackup/config.db` must not get a hard
   `ConfigDbIsYaml` telling them to import a working SQLite database
   into itself just because of the symlink's own name. The message text
   still names `resolved.path` (what the operator typed), via
   `_describe_real`.
6. **Sidecar existence is checked with `os.lstat` in a `try`, never
   `Path.exists()`.** `Path.exists()` returns `False` for *any* `OSError`
   -- not just `ENOENT` -- so it silently turns "could not determine"
   (permission denied, a symlink loop in a sidecar's own path) into "does
   not exist", which then reaches step 6's most permissive-looking arm on
   a wrong premise. Step 5's directory check now also requires `os.X_OK`
   (search permission), not only `os.W_OK`: creating a file inside a
   directory needs both, and checking only one lets a directory missing
   the other through.

Two env vars, two different things, never conflated
-----------------------------------------------------
`ZFSBACKUP_CONFIG` (`CONFIG_PATH_ENV`, this module) holds a filesystem
**path** to the config database file. `ZFSBACKUP_DB_URL`
(`migrations/env.py`, `alembic.ini`) holds a SQLAlchemy **URL** for the
bare `alembic` developer CLI. This module never reads `ZFSBACKUP_DB_URL`.
A `ZFSBACKUP_CONFIG` value that looks like a URL (`sqlite:` prefix, or
`://` anywhere in it) is rejected with `ConfigPathEnvError` naming both
variables and which takes which.

Permissions: the `-shm`-creation rule, not "read-only can't open WAL"
------------------------------------------------------------------------
An earlier draft of this plan claimed "a read-only user cannot open a WAL
database at all". **Measured false**: with both `-wal` and `-shm` already
present and writable, a read-only open succeeds even on a `chmod 444`
file in a read-only directory. The rule that is actually true, and that
`check_config_db` and the `0770` directory mode below both encode, is
narrower: **a WAL database needs write permission on its containing
directory whenever the `-shm` file must be (re)created**. Never diagnose
any of this by matching SQLite's exception text -- `check_config_db` and
`diagnose_open_failure` both work from `os.stat` and the file's own
16-byte SQLite magic / journal-mode header bytes instead.

Every exception this module's own filesystem calls can raise is caught
and re-raised as a `ConfigPathError` subclass -- including the residual
`OSError` arm for a symlink loop (`ELOOP`) or an overlong path
(`ENAMETOOLONG`), and `ConfigDbPathUnrepresentable` for a path
`url_for_path` cannot turn into an unambiguous URL (a `?`, see `db.py`).
This is deliberate: item 8's daemon half is built around a single
`except ConfigPathError`, and any exception that escapes that hierarchy
from inside this module is a bug in this module, not a documented
possibility for the caller to handle separately.

Creation policy
----------------
This module creates nothing at import time and nothing on its own. The
daemon must never auto-create a config database; the CLI creates one only
via an explicit `zfsbackup-config import` (item 10; it calls `zfsbackup/
config/store/importer.py`'s `import_yaml`), which must
call, in order: `ensure_config_dir`, `create_config_db_file` (creates the
file AND fixes its mode as one step, so the mandated create-then-chmod-
before-first-connection ordering is not something item 10's
implementation can get backwards), then open its first connection, then
run `ensure_schema`. `ensure_config_db_mode` remains for the case of
*fixing* the mode of the DEFAULT database on a re-import -- and only that:
a file at any other path is never chmod'd, and nothing (database or
sidecar) is chmod'd unless this process owns it and its mode differs. See
`_ensure_mode` and the "Mode/ownership policy" section below for the
group-operator rules (setgid directory, no chmod by a non-owner, no
touching sidecars owned by the daemon user).

That mandated sequence -- `create_config_db_file`, then the first
connection -- runs straight into this module's own preflight:
`create_config_db_file` leaves a file at exactly 0 bytes, and
`check_config_db` (below) refuses that as `ConfigDbNotADatabase` by
default (no SQLite magic to match). `check_config_db`'s `allow_new`
parameter, plumbed through `open_config_connection`/`open_config_session`,
is the narrow fix: it skips only the magic-bytes/truncation comparison,
and only when `os.stat` measures the file at exactly 0 bytes -- every
other check (the `.yaml`/`.yml` refusal, readability, directory
`W_OK`/`X_OK`, file writability) still runs. See `check_config_db`'s own
docstring for the exact rule. It has exactly one sanctioned caller,
`importer.py`'s schema phase, and must never be reachable from the
daemon.

"The daemon only reads" describes config *content*, not the *file*: the
daemon's read-only engine still opens the database file read-write at the
OS level and needs a writable containing directory for the `-shm` reason
above. (Until item 10.4 it also issued `PRAGMA journal_mode=WAL` on every
connection; it now only queries the mode and warns if it is not WAL -- see
`db.py` -- so the writable-file requirement below is a provisioning check
for the CLI's first write, not a requirement of the reader's connect.)
"""

from __future__ import annotations

import errno
import fcntl
import logging
import os
import stat as stat_module
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Literal, Mapping, Optional, Set, Tuple, Union

from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from zfsbackup.config.store.db import (
    ephemeral_engine,
    get_engine,
    session_scope,
    url_for_path,
)
from zfsbackup.config.store.errors import ConfigBusy

logger = logging.getLogger(__name__)

__all__ = [
    "CONFIG_DB_MODE",
    "CONFIG_DIR_MODE",
    "CONFIG_OWNER",
    "CONFIG_PATH_ENV",
    "ConfigDbCreatedConcurrently",
    "ConfigDbIsYaml",
    "ConfigDbNotADatabase",
    "ConfigDbNotAFile",
    "ConfigDbNotFound",
    "ConfigDbOrphanedSidecars",
    "ConfigDbPathUnrepresentable",
    "ConfigDbPermissionError",
    "ConfigPathEnvError",
    "ConfigPathError",
    "DEFAULT_CONFIG_DB",
    "DEFAULT_WRITER_LOCK_TIMEOUT",
    "ResolvedConfigPath",
    "WRITER_LOCK_SUFFIX",
    "check_config_db",
    "check_config_db_suffix",
    "create_config_db_file",
    "diagnose_open_failure",
    "ensure_config_db_mode",
    "ensure_config_dir",
    "is_default_config_dir",
    "is_default_config_path",
    "open_config_connection",
    "open_config_session",
    "resolve_config_path",
    "resolve_config_url",
    "require_writer_lock_held",
    "writer_lock",
    "writer_lock_path",
]

# --------------------------------------------------------------------------
# Resolution constants
# --------------------------------------------------------------------------

#: Per user decision (item 6a plan). `config.example.yaml` already
#: documents `/var/lib/zfsbackup/` as the daemon's state directory.
DEFAULT_CONFIG_DB = Path("/var/lib/zfsbackup/config.db")

#: Holds a PATH, never a URL. See module docstring for the sibling
#: `ZFSBACKUP_DB_URL` (Alembic, `migrations/env.py`) and why the two names
#: must never be conflated.
CONFIG_PATH_ENV = "ZFSBACKUP_CONFIG"

# Named only for use in this module's own error text (the "which takes
# which" guard) -- this module must never read it.
_DB_URL_ENV_NAME = "ZFSBACKUP_DB_URL"

# --------------------------------------------------------------------------
# Layout constants (6a-4) -- recorded here, not created at import time.
# --------------------------------------------------------------------------

#: `mkdir`'s own `mode=` argument is masked by umask, so `ensure_config_dir`
#: chmods explicitly to this value rather than relying on it. Group-write
#: is not optional: whichever of the daemon or the CLI opens the database
#: *first* is the one that has to create `-wal`/`-shm` in this directory,
#: and which one that is is genuinely unpredictable -- sidecars are
#: checkpointed away on every clean close, so it is never simply "the
#: writer creates them once".
#:
#: The setgid bit (`2` in `2770`) is what makes that unpredictability
#: harmless for operators who are NOT the daemon user: on Linux a file
#: created in a setgid directory inherits the DIRECTORY's group instead of
#: its creator's primary group. Without it, an operator (user `alice`, in
#: group `zfsbackup`) whose CLI is the first opener creates `-wal`/`-shm`
#: as `alice:alice`; SQLite copies the DB file's mode onto them but never
#: its group, so the de-rooted daemon is locked out of the sidecars and
#: gets an undiagnosable "unable to open database file". (macOS assigns a
#: new file the directory's group regardless, so this cannot be reproduced
#: there -- it is mocked in the unit tests and proven on Linux by the
#: `test_config_permissions.sh` scenario.) Note that `chmod` by an owner
#: who is not a member of the directory's group silently DROPS the setgid
#: bit on Linux -- `_ensure_mode` re-stats and warns when that happens.
CONFIG_DIR_MODE = 0o2770

#: Group-write for the CLI (the only writer of config *content*);
#: owner-write for the daemon even though it writes no config content --
#: its read-only engine still opens the file read-write at the OS level.
#: (It no longer flips a not-yet-WAL file into WAL itself -- item 10.4 -- but
#: the preflight still wants the file writable; see `check_config_db` step 6.)
CONFIG_DB_MODE = 0o660

#: The service user and group the config store is meant to belong to.
#: Creating them is a packaging concern (`sysusers.d`/`tmpfiles.d`, or a
#: systemd unit's `User=`/`Group=`), not something this module can or
#: should do -- they must already exist before `import` runs. This module
#: changes ownership in exactly one case (Q7, `_chgrp_to_service_group`):
#: when ROOT creates the default directory or database it `chgrp`s it to
#: the GROUP half of this value (and warns if that group does not exist).
#: The USER half is never applied -- nothing here ever `chown`s a user --
#: and it is also used to build the "who should run chmod" advice in error
#: messages.
CONFIG_OWNER = "zfsbackup:zfsbackup"

_SQLITE_MAGIC = b"SQLite format 3\x00"

#: `os.access` normally consults the REAL uid/gid, not the effective
#: ones -- harmless while the daemon runs as root without ever dropping
#: privileges (today), wrong the moment a `seteuid`-based de-rooting
#: lands (a documented future direction). `effective_ids=True` asks for
#: the effective check instead, wherever the platform supports it
#: (`os.supports_effective_ids`; verified True on Linux and macOS,
#: possibly not on other platforms) -- falls back to the (still correct
#: today) default silently where it is not.
_ACCESS_KWARGS = {"effective_ids": True} if os.access in os.supports_effective_ids else {}


def _can_access(path: Path, mode: int) -> bool:
    return os.access(path, mode, **_ACCESS_KWARGS)


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------


class ConfigPathError(Exception):
    """Base class for every error this module raises.

    One base class is deliberate: item 8's daemon half needs a single
    `except ConfigPathError` to turn any of these into its refusal-to-start
    path, without enumerating every subclass at the call site. Every
    filesystem call this module makes is wrapped so nothing escapes this
    hierarchy -- including a residual `OSError` arm (`ConfigDbPermission
    Error`) for cases like a symlink loop or an overlong path, and
    `ConfigDbPathUnrepresentable` for a path `url_for_path` cannot
    represent unambiguously as a URL.
    """


class ConfigPathEnvError(ConfigPathError):
    """`ZFSBACKUP_CONFIG` holds something this module cannot use as a
    path -- today, only "looks like a URL" (see module docstring).
    """


class ConfigDbPathUnrepresentable(ConfigPathError):
    """The resolved path cannot be turned into an unambiguous SQLite URL
    by `url_for_path` -- e.g. it contains a `?`, which SQLAlchemy's URL
    grammar treats as a delimiter (see `db.py:url_for_path`). Raised by
    `ResolvedConfigPath.url()`, and checked by `check_config_db` as its
    first step (cheap, no I/O) so this -- not a bare `ValueError` from a
    different exception hierarchy -- is what a caller of the preflight
    actually sees.
    """


class ConfigDbNotFound(ConfigPathError):
    """Nothing exists at the resolved path.

    Deliberately raised from a `stat`, not from a caught `OperationalError`
    -- see this module's docstring for why a caught exception is too late:
    `make_engine(url, readonly=True)` would already have created the file
    by the time anything could observe that it did not previously exist.
    """


class ConfigDbNotADatabase(ConfigPathError):
    """The resolved path exists, is a regular file, and does not have a
    `.yaml`/`.yml` suffix, but its content is not a SQLite database
    either (bad magic bytes, or a header shorter than a truncated file
    could plausibly be). Most commonly the zero-byte artifact a
    `readonly=True` engine leaves behind at a path that did not
    previously exist (this module's whole reason to exist), or a
    corrupted/truncated database.

    Deliberately **not** the same exception, and does not carry the same
    "run `zfsbackup-config import <this file>`" advice, as `ConfigDbIsYaml`
    below: telling an operator to import a zero-byte or corrupted file
    into itself is actively wrong, and was exactly what an earlier
    version of this module's shared message text produced for this case.
    """


class ConfigDbIsYaml(ConfigDbNotADatabase):
    """The resolved path has a `.yaml`/`.yml` suffix -- the one case
    where "run `zfsbackup-config import <path>`" is the actually-correct
    next step, which its message says verbatim and copy-pasteably. A
    subclass of `ConfigDbNotADatabase` (rather than a sibling) so a
    caller that only wants "is this usable as a database at all" can
    catch the parent, while item 8 -- which is expected to name this
    exact class in its own messaging -- keeps a stable, specific type to
    reference.
    """


class ConfigDbNotAFile(ConfigPathError):
    """The resolved path exists but is not a regular file (a directory,
    a socket, a device, ...).
    """


class ConfigDbPermissionError(ConfigPathError):
    """The resolved path (or its containing directory, or a WAL sidecar)
    exists and is the right kind of thing, but this process cannot use it
    as the permissions require, or a filesystem-level error other than a
    simple permission denial prevented checking it at all (a symlink
    loop, an unlinked current working directory, an overlong path). Kept
    as one exception type because the fix is usually the same shape
    (permissions/group membership, or removing a broken symlink) for all
    of its sub-cases -- the message itself, not the exception class, says
    which sub-case it was.
    """


class ConfigDbOrphanedSidecars(ConfigPathError):
    """A first import was refused because `-wal`/`-shm`/`-journal` files
    already exist next to a config database that does not.

    Creating the database there would make SQLite replay an orphaned `-wal`
    or `-journal` into the brand-new file (resurrecting somebody else's
    pages), and a live process's `-shm` is shared by PATH, so a new database
    would silently share lock state with whatever still has the old one
    open. The importer therefore creates nothing and touches nothing; the
    operator removes the files once no process has them open
    (`fuser`/`lsof`), then retries.
    """


class ConfigDbCreatedConcurrently(ConfigPathError):
    """A first import found `-wal`/`-shm`/`-journal` files next to a database
    that did not exist when it first looked, but does exist now: another
    process created it in the meantime and has it open.

    Distinct from `ConfigDbOrphanedSidecars` because the advice is the
    opposite -- those sidecars are live, must NOT be removed, and the right
    action is simply to retry once the other import has finished.
    """


# --------------------------------------------------------------------------
# Small helpers shared between resolution and the preflight.
# --------------------------------------------------------------------------


def _residual_os_error_message(action: str, exc: Exception) -> str:
    """Format a filesystem-level failure that is not a simple permission
    denial. Deliberately typed to accept more than `OSError`: see
    `_resolve_or_raise` below for why -- on Python 3.11-3.12,
    `Path.resolve()` can raise a bare `RuntimeError` (not `OSError`) for
    a symlink loop, so this cannot assume `.errno`/`.strerror` exist.
    """
    strerror = getattr(exc, "strerror", None)
    errno_value = getattr(exc, "errno", None)
    if strerror is not None and errno_value is not None:
        detail = f"{strerror} (errno {errno_value})"
    else:
        detail = str(exc)
    return (
        f"Cannot {action}: {detail}. This is usually a filesystem-level "
        "problem -- a symlink loop, an unlinked current working "
        "directory, a path exceeding the filesystem's name-length "
        "limit, or similar -- rather than a simple permissions issue."
    )


def _resolve_or_raise(path: Path) -> Path:
    """`Path(path).resolve()`, with every failure it can raise turned
    into a `ConfigDbPermissionError`.

    `Path.resolve()` looks like a pure, in-memory string operation, and
    is not one: it issues `lstat`/`readlink` per path component to walk
    symlinks, which is exactly why it can fail the way `os.stat` can --
    and two ways it does are neither obvious nor version-stable, found in
    review:

    - A symlink loop. **On Python 3.11 and 3.12** (this package
      declares `python = "^3.11"` in `pyproject.toml`, so both are in
      scope), `Path.resolve(strict=False)` performs a final re-`stat`
      of its own result and converts the underlying `OSError(ELOOP)`
      into a bare, undocumented `RuntimeError` ("Symlink loop from...");
      verified directly on 3.11 and 3.12. Python 3.13+ raises
      `OSError` for the identical input instead. Both are caught here.
    - An unlinked current working directory: resolving a *relative* path
      needs `os.getcwd()`, which raises `FileNotFoundError` if the
      directory the calling shell is sitting in has since been removed.
      Version-independent.

    Called from `ResolvedConfigPath.__post_init__` -- i.e. from inside
    `resolve_config_path` itself. Without this wrapper, either failure
    mode escaped *resolution*, before `check_config_db` ever ran, as an
    exception item 8's single `except ConfigPathError` does not catch:
    `ZFSBACKUP_CONFIG` pointing at a symlink loop used to crash
    `resolve_config_path` itself with a bare `RuntimeError` on 3.11-3.12.
    This is also why `check_config_db`'s own residual `except OSError`
    arm on its `os.stat` step is defence in depth rather than the only
    guard against ELOOP/ENAMETOOLONG-shaped failures: on 3.11-3.12 a
    symlink loop is caught here, at construction, before `check_config_db`
    is ever reached.
    """
    try:
        return Path(path).resolve()
    except (OSError, RuntimeError) as exc:
        raise ConfigDbPermissionError(
            _residual_os_error_message(f"resolve {path}", exc)
        ) from exc


# --------------------------------------------------------------------------
# Resolution (6a-2)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedConfigPath:
    """The result of resolving `-c`/`ZFSBACKUP_CONFIG`/the default, with
    enough provenance for `check_config_db` to produce a source-specific
    error message.

    `path` is deliberately **not** resolved, and stays exactly as given.
    Measured: `Path("/var/lib/zfsbackup/config.db").resolve()` becomes
    `/private/var/lib/zfsbackup/config.db` on macOS (a `/var` ->
    `/private/var` symlink); naming a path in an error message that the
    operator never typed, and that differs across platforms for the exact
    same input, is worse than the unresolved original.

    `resolved_path` (a cached property backed by a field computed once in
    `__post_init__`, **not** lazily) is the fully cwd- and
    symlink-resolved form, computed exactly once, at construction time.
    Every actual filesystem operation -- `check_config_db`'s checks and
    `.url()`'s `url_for_path` call alike -- reads this one value rather
    than re-resolving `path` independently. This is load-bearing, not
    tidiness: measured, two `os.chdir()` calls between constructing one
    frozen `ResolvedConfigPath` and calling `.url()` twice produced two
    different URLs from the same object, which means a preflight
    performed before a `chdir` and an actual open performed after it (or
    vice versa) could silently address two different files -- exactly
    the soundness gap the preflight exists to prevent elsewhere. A
    consequence worth naming: if the path is a symlink whose target
    changes after this object is constructed, `resolved_path` does not
    track that -- by design, for the same reason a single, stable
    resolution is what makes "the preflight checked the file the caller
    will actually open" true at all.

    Computing `resolved_path` is **not** filesystem-I/O-free, even though
    resolution of *which* path wins (explicit/env/default) is: `Path.
    resolve()` walks the path component by component with `lstat`/
    `readlink` to follow symlinks, and can therefore fail the way
    `os.stat` can (a symlink loop, an unlinked cwd -- see
    `_resolve_or_raise`) or block (a hung NFS mount). Constructing a
    `ResolvedConfigPath` -- which `resolve_config_path` does immediately,
    so effectively at argparse time -- can do either. Every failure
    `_resolve_or_raise` can hit is translated to `ConfigDbPermissionError`
    before it leaves `__post_init__`, so this is still exception-safe
    within the `ConfigPathError` hierarchy; it is specifically *not*
    I/O-free, and this class's docstring and `resolve_config_path`'s used
    to claim otherwise.

    **Equality and hashing include `resolved_path`, deliberately.** The
    auto-generated `__eq__`/`__hash__` compare every field that does not
    opt out with `compare=False`; `resolved_path` is not one of them. Two
    `ResolvedConfigPath` objects built from the identical `path` string
    under two different working directories address two different real
    files and must not compare equal or collide in a hash-keyed
    structure -- excluding `resolved_path` from comparison (an earlier
    version of this class did, deliberately, for the wrong reason) made
    exactly that pair equal: the precise inverse of the chdir/two-URLs
    bug `resolved_path` itself exists to fix. Nothing in this repository
    hash-keys on a `ResolvedConfigPath` today, but the type would be a
    natural dict/set key for a future caller, and getting this backwards
    fails in the dangerous direction -- silently merging two different
    databases into one identity -- rather than the safe one.
    """

    path: Path
    source: Literal["--config", "ZFSBACKUP_CONFIG", "default"]
    _resolved: Path = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.source not in ("--config", CONFIG_PATH_ENV, "default"):
            raise ValueError(
                f"invalid ResolvedConfigPath.source: {self.source!r}; "
                f"must be '--config', {CONFIG_PATH_ENV!r}, or 'default'"
            )
        object.__setattr__(self, "_resolved", _resolve_or_raise(self.path))

    @property
    def resolved_path(self) -> Path:
        """`path`, fully resolved (cwd + symlinks), computed once at
        construction time. Use this for any actual filesystem operation;
        use `path` only for error-message text.
        """
        return self._resolved

    def url(self) -> str:
        """The canonical SQLite URL for `resolved_path`, via
        `url_for_path`. Wraps `url_for_path`'s own `ValueError` (raised
        for a path `url_for_path` cannot represent unambiguously, e.g.
        one containing `?`) into `ConfigDbPathUnrepresentable`, so a
        direct caller of this method -- not just one that goes through
        `check_config_db` first -- still gets a `ConfigPathError`
        subclass rather than a bare `ValueError` from a different
        hierarchy.
        """
        try:
            return url_for_path(self._resolved)
        except ValueError as exc:
            raise ConfigDbPathUnrepresentable(str(exc)) from exc


def resolve_config_path(
    explicit: Union[str, Path, None] = None,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> ResolvedConfigPath:
    """Pick the config database path.

    Order: `explicit` (the CLI's `-c/--config`, already parsed by the
    caller) > `ZFSBACKUP_CONFIG` > `DEFAULT_CONFIG_DB`.

    **Selection among the three sources is pure** -- which one wins never
    depends on the filesystem, so *that* decision is testable with no
    `tmp_path` fixture. Existence is deliberately factored out into
    `check_config_db` (which knows the `source` this function decided on,
    and can list all three candidates in `source="default"`'s error), so
    every "not found" message has exactly one producer.

    **The `ResolvedConfigPath` this returns is not filesystem-I/O-free,
    though.** Constructing it resolves the winning path (`Path.resolve()`,
    walking each component with `lstat`/`readlink` to follow symlinks) so
    that this function's result and the same object's later `.url()`/
    `check_config_db` calls all agree on one real file -- see
    `ResolvedConfigPath`'s own docstring. That resolution can itself fail
    (a symlink loop, an unlinked cwd) or block (a hung NFS mount) --
    every failure is translated to `ConfigDbPermissionError` before it
    escapes this function, so callers still only ever need `except
    ConfigPathError`, but "no filesystem I/O at all" would be wrong to
    claim about this function as a whole; it is true only of the
    three-way selection itself.

    An `explicit` value of `""` (an empty string -- `--config ""`, the
    shape argparse produces for an explicitly-blank argument) is treated
    as not given, the same as `None`, and falls through to
    `ZFSBACKUP_CONFIG`/the default -- exactly matching `ZFSBACKUP_CONFIG
    =""`'s own treatment below. Without this, `--config ""` would resolve
    to `Path("")`, i.e. the current directory, and fail later with a
    confusing "the current directory is not a regular file" instead of
    behaving like `-c` was never passed.

    `ZFSBACKUP_CONFIG=""` is treated as unset, matching the truthiness
    check `migrations/env.py`'s own `_resolve_url()` already uses for
    `ZFSBACKUP_DB_URL`.

    A relative `ZFSBACKUP_CONFIG` (or `-c`) is accepted and resolved
    against the current working directory -- but only inside
    `ResolvedConfigPath.resolved_path`/`.url()`; see that class's
    docstring for why the attribute itself stays unresolved and why
    resolution happens exactly once.

    **Binding on item 8**: whatever parses `-c` must default it to `None`,
    never to a path (`daemon.py`'s current
    `default=Path('/etc/zfsbackup/config.yaml')` is exactly the shape that
    breaks this). With any non-`None` default, `explicit` here is never
    `None` or `""`, `ZFSBACKUP_CONFIG` can never be reached, and step 2 of
    the resolution order becomes dead code.
    """
    if explicit is not None and str(explicit) != "":
        return ResolvedConfigPath(path=Path(explicit), source="--config")

    env = os.environ if environ is None else environ
    raw = env.get(CONFIG_PATH_ENV, "")
    if raw:
        if raw.startswith("sqlite:") or "://" in raw:
            raise ConfigPathEnvError(
                f"{CONFIG_PATH_ENV}={raw!r} looks like a database URL, "
                f"not a filesystem path. {CONFIG_PATH_ENV} must hold the "
                f"PATH to the config database file (e.g. "
                f"{CONFIG_PATH_ENV}=/var/lib/zfsbackup/config.db). A URL "
                f"is what {_DB_URL_ENV_NAME} holds instead -- that "
                "variable configures Alembic's standalone developer CLI "
                "(see alembic.ini) and is read only by "
                "zfsbackup/config/store/migrations/env.py, never by this "
                "module."
            )
        return ResolvedConfigPath(path=Path(raw), source=CONFIG_PATH_ENV)

    return ResolvedConfigPath(path=DEFAULT_CONFIG_DB, source="default")


def resolve_config_url(
    explicit: Union[str, Path, None] = None,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """`resolve_config_path(...).url()` -- the convenience form for a
    caller that only wants the URL.

    **This bypasses `check_config_db` by construction.** Prefer
    `open_config_session`/`open_config_connection`, which preflight
    before building anything; calling this function directly and handing
    the result straight to `get_engine`/`session_scope` reintroduces the
    exact hazard this module exists to remove -- `readonly=True` on a URL
    for a path that does not exist fails with a bare "unable to open database
    file" (a reader never creates it; a writer engine still would).
    Kept public for a caller that genuinely only needs the URL string and
    performs its own preflight (or genuinely wants no preflight at all,
    e.g. a diagnostic tool) -- nothing in this repository is such a
    caller yet.
    """
    return resolve_config_path(explicit, environ=environ).url()


# --------------------------------------------------------------------------
# Message builders -- kept separate from the checks so each message's
# required content (see the plan's table) is visible in one place.
# --------------------------------------------------------------------------


def _describe_real(given: Path, real: Path) -> str:
    """Format `real` (a fully resolved path an actual filesystem check
    targeted) for an error message, also naming `given` (the path as
    supplied -- possibly relative, possibly a symlink to somewhere else)
    whenever the two differ. Covers both D3 (a relative `ZFSBACKUP_CONFIG`
    resolved against cwd) and a symlinked config path resolving to a
    different directory in one comparison, since `real` already
    incorporates both kinds of resolution.
    """
    if str(given) == str(real):
        return str(real)
    return f"{real} (given as: {given})"


def _not_found_message(resolved: ResolvedConfigPath) -> str:
    if resolved.source == "default":
        # Deliberately the bare, unresolved default -- never `.resolve()`d
        # -- see ResolvedConfigPath.path's docstring: resolving it here
        # would rewrite it to a path the operator never typed (and never
        # will type, since this is a compiled-in constant, not something
        # they entered) on macOS.
        shown = str(resolved.path)
        return (
            f"No config database found at the default path {shown} "
            f"(no -c/--config was given, and {CONFIG_PATH_ENV} is not "
            "set). zfsbackup no longer reads /etc/zfsbackup/config.yaml "
            "as a config source. Create a database from an existing YAML "
            "config with: zfsbackup-config import <path-to-yaml.yml>"
        )
    shown = _describe_real(resolved.path, resolved.resolved_path)
    if resolved.source == "--config":
        # No mention of the default: it is not what will be tried next,
        # and implying a fallback that does not exist is worse than no
        # explanation at all.
        return f"No config database found at {shown} (given via -c/--config)."
    return (
        f"No config database found at {shown} "
        f"({CONFIG_PATH_ENV}={str(resolved.path)!r})."
    )


def _yaml_message(resolved: ResolvedConfigPath) -> str:
    given = str(resolved.path)
    shown = _describe_real(resolved.path, resolved.resolved_path)
    # Two steps, because "import" alone leaves the daemon still pointed at
    # the YAML it just refused: the import writes a NEW database, and `-c`
    # (or ZFSBACKUP_CONFIG) must then be changed to name it. Suggested name:
    # the YAML's own path with a `.db` suffix, which cannot collide with it.
    db = str(resolved.path.with_suffix(".db"))
    return (
        f"{shown} has a .yaml/.yml extension. zfsbackup's config store "
        "is a SQLite database, not YAML. To migrate an existing YAML "
        "config: (1) import it into a database with: "
        f"zfsbackup-config import {given} -c {db}  "
        f"and (2) point the daemon at that database instead of the YAML "
        f"(-c {db}, or {CONFIG_PATH_ENV}={db})."
    )


def check_config_db_suffix(resolved: ResolvedConfigPath) -> None:
    """Raise `ConfigDbIsYaml` if `resolved`'s fully resolved target has a
    `.yaml`/`.yml` suffix -- **without requiring the target to exist**.

    This is `check_config_db`'s own step 3 suffix decision (decided from
    `resolved.resolved_path.suffix`, never `resolved.path.suffix` -- see
    that function's docstring for why: a `/etc/zfsbackup/config.yaml`
    compatibility symlink pointing at a real, migrated `config.db` must
    not be refused just because of its own name), extracted into its own
    function and called from there, so this remains the **one** place
    that decides what "looks like YAML" means and builds the message --
    `check_config_db` calls this too, rather than duplicating the
    comparison, so the two can never drift apart.

    Unlike the rest of `check_config_db`, this needs no I/O at all:
    `resolved.resolved_path` is already computed, non-strictly, at
    `ResolvedConfigPath` construction time (`Path.resolve(strict=False)`
    does not require the path to exist). That is what makes this safe to
    call from `importer.py`'s `import_yaml` **before** phase 1 creates
    anything -- a `.yaml`-suffixed `-c` typo must be refused before a
    stray zero-byte file is created at that path, the same
    create-nothing-on-misuse property phase 0's "parse the YAML before
    touching the target" ordering already gives for a bad *source* YAML.
    Refusing an existing YAML target still goes through
    `check_config_db`'s full step 3 as before; this function only adds
    the pre-existence guard, it does not replace the in-preflight check.
    """
    if resolved.resolved_path.suffix in (".yaml", ".yml"):
        raise ConfigDbIsYaml(_yaml_message(resolved))


def _not_a_database_message(resolved: ResolvedConfigPath, real: Path) -> str:
    shown = _describe_real(resolved.path, real)
    return (
        f"{shown} exists but is not a SQLite database (its header does "
        "not match SQLite's file-format magic bytes). This can be a "
        "zero-byte file left behind by an accidental readonly=True open "
        "of a path that did not previously exist, a truncated or "
        "corrupted database, or a YAML config saved under a name "
        "without a .yaml/.yml extension. Inspect it before doing "
        "anything else: if it turns out to be a YAML config, rename it "
        "and run `zfsbackup-config import <that path>`; otherwise remove "
        "it (it is not usable as-is), or restore config.db from backup."
    )


def _truncated_message(resolved: ResolvedConfigPath, real: Path, length: int) -> str:
    shown = _describe_real(resolved.path, real)
    return (
        f"{shown} starts with SQLite's format magic but is only "
        f"{length} bytes long -- far short of SQLite's minimum page size "
        "(512 bytes). This is a truncated or corrupted file, not a "
        "usable database. Restore it from backup, or remove it and "
        "re-run `zfsbackup-config import` if you meant to import a YAML "
        "config."
    )


def _dir_not_writable_message(resolved: ResolvedConfigPath, directory: Path) -> str:
    shown = _describe_real(resolved.path.parent, directory)
    return (
        f"Directory {shown} is not writable and/or not searchable "
        "(execute permission) by this process. A WAL-mode SQLite "
        "database (re)creates its -wal and -shm sidecar files in its "
        "containing directory -- on the first open after every previous "
        "writer has closed cleanly, and after a crash -- and creating a "
        "file inside a directory needs both write AND search permission "
        "on it, so the directory itself must have both, not only the "
        f"database file. Target: {directory} should be "
        f"{CONFIG_DIR_MODE:04o} owned by {CONFIG_OWNER} (group-writable "
        "and group-searchable, because whichever of the daemon or the "
        "CLI opens the database first is the one that has to create the "
        "sidecars)."
    )


def _file_not_writable_message(resolved: ResolvedConfigPath, real: Path) -> str:
    shown = _describe_real(resolved.path, real)
    return (
        f"{shown} is not writable by this process, and is not yet in "
        "WAL journal mode. The store's writer engine switches the file to "
        "WAL on its first connection (zfsbackup.config.store.db), which "
        "rewrites the file's journal-mode header and needs write access; a "
        "read-only engine does not switch it (it only warns), but a "
        "non-WAL database would make readers and the CLI writer block each "
        f"other. Target: {real} should be "
        f"{CONFIG_DB_MODE:04o} owned by {CONFIG_OWNER}."
    )


def _sidecars_would_be_created_readonly_message(
    resolved: ResolvedConfigPath, real: Path
) -> str:
    shown = _describe_real(resolved.path, real)
    return (
        f"{shown} is not writable by this process, and its WAL sidecars "
        "(-wal/-shm) do not both already exist. Opening it now would "
        "let SQLite create the missing one(s) -- as THIS process, not "
        "necessarily the database file's owner. A newly created sidecar "
        "is OWNED by whichever process creates it, with mode bits "
        "derived from the database file's own mode (independent of "
        "umask; see ensure_config_db_mode) -- not from who is currently "
        "allowed to write the database file. For a database owned by "
        "one user with a second user (e.g. the daemon) only in its "
        "group, this process creating the sidecars locks the FIRST user "
        "out of them, even though that user can write the database file "
        "itself -- the opposite of harmless, and permanent, since an "
        "existing sidecar is never re-chmod'd (see ensure_config_db_mode). "
        "If this process only needs read access, pass "
        "require_writable=False to check_config_db (see "
        "open_config_session/open_config_connection) instead of "
        f"widening {real}'s permissions to let THIS process create the "
        "sidecars -- widening them is very likely the opposite of what "
        "provisioning the file this way intended."
    )


def _stuck_sidecars_message(
    resolved: ResolvedConfigPath, real: Path, stuck: List[Path]
) -> str:
    shown = _describe_real(resolved.path, real)
    names = ", ".join(str(p) for p in stuck)
    return (
        f"{shown} is already in WAL mode and is not writable, but its "
        f"WAL sidecar(s) {names} are ALSO not writable -- most likely "
        "because the database file was chmod'd after these sidecars "
        "already existed, and an existing sidecar is never re-chmod'd. "
        "This process cannot write through this database until the "
        f"sidecars themselves are fixed: chmod {CONFIG_DB_MODE:04o} "
        f"{names}. Never blindly `rm` the -wal file (it can hold "
        "committed data not yet checkpointed into the main file); the "
        "-shm file is safe to remove only when no connection has the "
        "database open."
    )


def _transient_sidecars_message(resolved: ResolvedConfigPath, real: Path) -> str:
    shown = _describe_real(resolved.path, real)
    return (
        f"{shown} is not writable by this process. It is already in WAL "
        "mode and its -wal/-shm sidecars currently both exist and are "
        "writable -- but that state is transient in normal operation: "
        "the sidecars exist only because some other connection "
        "currently has the database open, and are checkpointed away the "
        "moment that connection closes cleanly. If this process's own "
        "connection (opened lazily, on first use -- not necessarily "
        "right now) is the one that reopens the database after that "
        "happens, it can be the one that (re)creates the sidecars at "
        "this file's own unwritable mode, permanently -- exactly the "
        "stuck-sidecar state this check exists to refuse elsewhere. "
        "There is no stable safe state to continue past here for a "
        "non-writable database file. If this process genuinely only "
        "needs read access and can tolerate the database being briefly "
        "unopenable during a checkpoint window, pass "
        "require_writable=False to check_config_db (see "
        "open_config_session/open_config_connection) instead of "
        "widening this file's permissions -- widening them is very "
        "likely the opposite of what was intended by making it "
        "not-writable-by-this-process in the first place."
    )


def _sidecar_unusable_message(
    resolved: ResolvedConfigPath, real: Path, sidecar: Path, st: os.stat_result
) -> str:
    shown = _describe_real(resolved.path, real)
    owner = _username(st.st_uid)
    return (
        f"{shown} is writable, but its WAL sidecar {sidecar} (owner="
        f"{owner} group={_groupname(st.st_gid)} mode="
        f"{stat_module.S_IMODE(st.st_mode):04o}) is not readable and "
        "writable by this process. SQLite would fail to open the database "
        "with a bare 'unable to open database file'. The usual cause is a "
        "process of another user having been the first to open the "
        "database and exited uncleanly. Fix: the owner "
        f"({owner}) or root must run: chmod {CONFIG_DB_MODE:04o} {sidecar}; "
        f"chgrp {CONFIG_OWNER.split(':', 1)[1]} {sidecar}. Never blindly "
        "`rm` a -wal file (it can hold committed data not yet "
        "checkpointed); the -shm is safe to remove only when no process "
        "has the database open."
    )


def _username(uid: int) -> str:
    try:
        import pwd

        return pwd.getpwuid(uid).pw_name
    except (ImportError, KeyError):
        return str(uid)


def _groupname(gid: int) -> str:
    try:
        import grp

        return grp.getgrgid(gid).gr_name
    except (ImportError, KeyError):
        return str(gid)


def _unreadable_message(
    resolved: ResolvedConfigPath, real: Path, st: os.stat_result
) -> str:
    shown = _describe_real(resolved.path, real)
    owner = _username(st.st_uid)
    group = _groupname(st.st_gid)
    mode = stat_module.S_IMODE(st.st_mode)
    target_group = CONFIG_OWNER.split(":", 1)[1]
    return (
        f"{shown} is not readable by this process. You may not be in "
        f"the '{target_group}' group. Actual: owner={owner} "
        f"group={group} mode={mode:04o}. Target: {CONFIG_DB_MODE:04o} "
        f"owned by {CONFIG_OWNER}."
    )


def _permission_failure_message(action: str, context_path: Path, exc: OSError) -> str:
    """Format an `OSError` from a create/chmod operation with the same
    actual-vs-target detail `_unreadable_message` uses, so a packaging or
    first-run permission failure in `ensure_config_dir`/
    `ensure_config_db_mode`/`create_config_db_file` is diagnosable the
    same way a preflight failure is, instead of surfacing as a bare
    `PermissionError: [Errno 13] Permission denied`.
    """
    try:
        st = os.stat(context_path)
        owner = _username(st.st_uid)
        group = _groupname(st.st_gid)
        mode = stat_module.S_IMODE(st.st_mode)
        actual = f"{context_path}: owner={owner} group={group} mode={mode:04o}"
    except OSError:
        actual = f"{context_path}: <could not stat -- missing or inaccessible>"
    target_group = CONFIG_OWNER.split(":", 1)[1]
    return (
        f"Cannot {action}: {exc.strerror or exc} (errno {exc.errno}). "
        f"You may not be in the '{target_group}' group, or may not have "
        f"the permissions this needs. Actual: {actual}. Target: owned "
        f"by {CONFIG_OWNER}."
    )


def _is_wal_header(header: bytes) -> bool:
    """True if the SQLite file-format header (bytes 18/19: file-format
    write/read version) records WAL journal mode. `1` = legacy rollback
    journal, `2` = WAL -- verified directly against a real WAL and a real
    rollback-journal database. By the time this is called from
    `check_config_db`, `header` has already been confirmed at least 100
    bytes long (see the truncation check there), so the `len(header) >=
    20` guard here is defence in depth, not the primary safeguard.
    """
    return len(header) >= 20 and header[18] == 2 and header[19] == 2


def _sidecar_lstat(path: Path) -> Optional[os.stat_result]:
    """`os.lstat(path)`, returning `None` only for `ENOENT`/
    `NotADirectoryError` -- unlike `Path.exists()`, which swallows EVERY
    `OSError` (`EACCES`, `ELOOP`, ...) into `False`, silently turning
    "could not determine whether this exists" into "does not exist".
    That distinction matters here: `check_config_db`'s step 6 reads
    "does not exist" as "opening the database would let SQLite create
    this sidecar", which is a very different, and much more dangerous to
    get wrong, conclusion than "exists, but this process cannot see it".
    Any other `OSError` is re-raised as `ConfigDbPermissionError` instead
    of being silently folded into either reading.

    Uses `lstat`, not `stat`, deliberately: a WAL sidecar that is itself
    a symlink is not a configuration this store needs to support, so
    this reports "this IS a symlink" (a regular file per `S_ISREG` would
    be false) rather than silently following it to whatever it points at.
    """
    try:
        return os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as exc:
        raise ConfigDbPermissionError(
            _residual_os_error_message(f"check {path}", exc)
        ) from exc


# --------------------------------------------------------------------------
# Mode/ownership policy (A-4). Group operators -- user `alice` in group
# `zfsbackup` editing a database owned by user `zfsbackup` -- are in scope,
# which changes three things about what this module may do to a file:
#
# - `chmod` by a non-owner is EPERM even when the mode would not change, so
#   "always chmod" turns every non-owner re-import into an error;
# - `chmod` of a mode the file already has is pointless work that can only
#   fail or (for a directory) strip the setgid bit; and
# - a file this process did not create and does not own must never be
#   chmod'd at all (a sidecar owned by the daemon user, say).
#
# `_ensure_mode` is the single place all three rules live, for the
# directory, the database, and the sidecars alike.
# --------------------------------------------------------------------------


def _is_owner(st: os.stat_result) -> bool:
    """True if this process may chmod the file `st` describes. Root counts as
    an owner of everything (it can chmod anything), which also means a root
    import never takes the "not owned" branch for a daemon-owned file."""
    euid = os.geteuid()
    return euid == 0 or st.st_uid == euid


def _ensure_mode(path: Path, mode: int, *, access: int, what: str) -> None:
    """Make `path`'s permission bits equal `mode`, without ever fighting the
    file's owner.

    1. `stat` (following symlinks -- callers hand in resolved paths, and
       sidecar callers have already `lstat`-rejected symlinks). A missing
       path is not this function's concern. Mode already equal: do
       NOTHING (no chmod syscall, so no EPERM for a non-owner, and no
       setgid stripping for an owner). For a DIRECTORY the comparison
       ignores the sticky bit (`S_ISVTX`) and a chmod preserves it: an
       admin who set `3770` on the config directory (so group members
       cannot delete each other's files) must not have it silently stripped
       to `2770` by a re-import. Nothing is ever widened -- the sticky bit
       only restricts. Files keep the exact comparison.
    2. Owned (`_is_owner`: the file belongs to this process's effective
       uid, OR this process is root, which can chmod anything and so never
       takes the "not owned" branch): `chmod`, then re-`stat`. Linux
       silently drops the setgid bit when the chmod-ing user is not a member
       of the file's group, so a mode that did not stick is a WARNING naming
       the difference, not an error -- the operation otherwise succeeded.
       A path that vanishes between the `stat` and the `chmod` (a WAL
       sidecar checkpointed away by another connection's last close) is
       simply gone, not an error.
    3. Not owned but `access` (a bitmask of `os.R_OK`/`W_OK`/`X_OK`) works
       for this process: WARNING naming the owner, the actual mode and the
       exact command the owner should run -- the store is usable, so
       refusing would turn a cosmetic mismatch into an outage. If the
       access check fails, the path is stat'ed once more: only a positive
       ENOENT/ENOTDIR there means "vanished mid-check" (return quietly);
       a path that still exists and is still unusable raises.
    4. Not owned and not usable: `ConfigDbPermissionError` saying the owner
       (or root) must run `chmod`.
    """
    try:
        st = os.stat(path)
    except (FileNotFoundError, NotADirectoryError):
        return
    except OSError as exc:
        raise ConfigDbPermissionError(
            _residual_os_error_message(f"check {path}", exc)
        ) from exc

    actual = stat_module.S_IMODE(st.st_mode)
    if stat_module.S_ISDIR(st.st_mode):
        mode |= actual & stat_module.S_ISVTX
    if actual == mode:
        return

    if _is_owner(st):
        try:
            os.chmod(path, mode)
        except FileNotFoundError:
            # A-10: the `stat` above saw it, then it went away -- the
            # normal fate of a `-wal`/`-shm` whose last connection just
            # closed. Root always takes this branch, so without this a
            # root re-import racing a daemon exit failed on a file that no
            # longer exists. Every other errno (EPERM, EROFS, ...) is a
            # real problem and still raises below.
            logger.debug("%s %s vanished before it could be chmod'd", what, path)
            return
        except OSError as exc:
            raise ConfigDbPermissionError(
                _permission_failure_message(
                    f"chmod {path} to {mode:04o}", path, exc
                )
            ) from exc
        try:
            after = stat_module.S_IMODE(os.stat(path).st_mode)
        except OSError:
            return
        if after != mode:
            logger.warning(
                "chmod %04o %s left it at %04o (%s). Linux drops the setgid "
                "bit when the chmod-ing user is not a member of the file's "
                "group; new files will not inherit group %r until someone "
                "who is runs: chmod %04o %s",
                mode, path, after, what,
                _groupname(st.st_gid), mode, path,
            )
        return

    owner = _username(st.st_uid)
    usable = _can_access(path, access)
    if not usable:
        # A-10: `os.access` is False for a path that has just been deleted,
        # too, and "deleted" is not "unusable" -- a sidecar checkpointed
        # away by another connection's last close, between the `stat`
        # above and here. A skip needs a POSITIVE ENOENT/ENOTDIR from a
        # fresh `stat` (follow-symlink semantics kept, like the first one);
        # anything else -- the path still there, or some other errno -- is
        # a real problem and falls through to the raise. One re-check, no
        # loop. (The caller has already proved the directory searchable, so
        # ENOENT here means the entry itself is gone.)
        try:
            st = os.stat(path)
        except (FileNotFoundError, NotADirectoryError):
            return
        except OSError:
            pass
        else:
            actual = stat_module.S_IMODE(st.st_mode)
            owner = _username(st.st_uid)
            usable = _can_access(path, access)
    if usable:
        logger.warning(
            "%s %s is owned by %s (group %s) with mode %04o, not the %04o "
            "this store expects, and you are not its owner so its mode was "
            "left alone. It is usable by this process as it is; if other "
            "group members are locked out, ask %s (or root) to run: "
            "chmod %04o %s",
            what, path, owner, _groupname(st.st_gid), actual, mode,
            owner, mode, path,
        )
        return

    raise ConfigDbPermissionError(
        f"{what} {path} is owned by {owner} (group {_groupname(st.st_gid)}) "
        f"with mode {actual:04o}, is not usable by this process, and you "
        f"are not its owner so it cannot be fixed from here. {owner} (or "
        f"root) must run: chmod {mode:04o} {path}"
    )


def _same_location(path: Union[str, Path], reference: Path) -> bool:
    """True if `path` and `reference` name the same filesystem object --
    by `(st_dev, st_ino)` when both exist (survives symlinks and macOS's
    `/var` -> `/private/var`), else by comparing resolved paths."""
    try:
        a = os.stat(path)
        b = os.stat(reference)
        return (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino)
    except OSError:
        pass
    try:
        return Path(path).resolve() == reference.resolve()
    except (OSError, RuntimeError):
        return False


def is_default_config_dir(path: Union[str, Path]) -> bool:
    """True if `path` is the directory holding the default config database
    (`DEFAULT_CONFIG_DB.parent`) -- the one directory this package owns."""
    return _same_location(path, DEFAULT_CONFIG_DB.parent)


def is_default_config_path(path: Union[str, Path]) -> bool:
    """True if `path` is the default config database file, however spelled."""
    return _same_location(path, DEFAULT_CONFIG_DB)


def _chgrp_to_service_group(path: Path, what: str, *, fd: Optional[int] = None) -> None:
    """(Q7) When ROOT has just CREATED the default directory/database, give
    it to the `zfsbackup` group, so group operators and the de-rooted daemon
    can use what root made. Does nothing for a non-root caller (a plain
    `chgrp` to a group you are not in is EPERM, and an operator-created
    file already carries a group they chose).

    If the group does not exist this WARNS rather than fails -- creating
    users and groups is a packaging concern -- but says plainly that
    operators who are not root will be locked out until it is fixed.
    `fd` (for a freshly `os.open`ed file) avoids a path race.
    """
    if os.geteuid() != 0:
        return
    group = CONFIG_OWNER.split(":", 1)[1]
    try:
        import grp

        gid = grp.getgrnam(group).gr_gid
    except (ImportError, KeyError):
        logger.warning(
            "created %s %s as root, but the %r group does not exist, so it "
            "keeps root's group and non-root operators will be locked out "
            "of it. Create the group (packaging: sysusers.d) and run: "
            "chgrp %s %s",
            what, path, group, group, path,
        )
        return
    try:
        if fd is not None:
            os.fchown(fd, -1, gid)
        else:
            os.chown(path, -1, gid)
    except OSError as exc:
        raise ConfigDbPermissionError(
            _permission_failure_message(f"chgrp {path} to {group}", path, exc)
        ) from exc


# --------------------------------------------------------------------------
# Preflight (6a-3)
# --------------------------------------------------------------------------


def check_config_db(
    resolved: ResolvedConfigPath,
    *,
    require_writable: bool = True,
    allow_new: bool = False,
) -> None:
    """Raise a `ConfigPathError` subclass if `resolved.path` cannot be
    used as the config database, without ever building a SQLite engine.

    **Must run before any engine is built.** `make_engine(url,
    readonly=True).connect()` on a path that does not yet exist used to
    silently create a zero-byte file; reader engines now open `mode=rw`
    (db.py) and fail instead, but a writer engine still creates. A
    `stat`-based check performed first is the only way to observe "did not
    previously exist" at all, and the only source of a precise error; see
    module docstring.

    **Every actual filesystem check below targets `resolved.resolved_path`**
    -- the same fully (cwd- and symlink-) resolved path `resolved.url()`
    hands to SQLite -- never `resolved.path` directly. `resolved.path` is
    used only to build message text. See `ResolvedConfigPath`'s docstring
    for why using two different resolutions for the preflight and the
    actual open would be unsound.

    Checks, in order, each a `stat`/header inspection rather than an
    attempted open:

    0. `resolved.url()` -- cheap, no I/O. Raises `ConfigDbPathUnrepresentable`
       for a path `url_for_path` cannot turn into an unambiguous URL (a
       `?`), so that failure surfaces from this hierarchy rather than a
       bare `ValueError` reaching the caller later, from `.url()` itself,
       through a code path this preflight never touched.
    1. `os.stat` -- missing (or a path component that is not a directory)
       -> `ConfigDbNotFound`; any other `OSError` (a symlink loop, an
       overlong path) -> `ConfigDbPermissionError`, residual sub-case.
       Deliberately `os.stat`, not `os.lstat`: this predicts whether
       *opening* the path would work, and a symlinked config database is
       a legitimate setup that `os.lstat` would misreport as "not a
       regular file"; a *broken* symlink correctly reads as "missing"
       under `os.stat`.
    2. Not a regular file -> `ConfigDbNotAFile`.
    3. **`real.suffix`** (the resolved target's own name -- **not**
       `resolved.path.suffix`) is `.yaml`/`.yml` -> `ConfigDbIsYaml` (the
       one case that names `zfsbackup-config import <path>` as the fix,
       using `resolved.path` -- what the operator typed -- in that
       message). Deciding from the resolved target, not the given
       spelling, matters for a symlink: `/etc/zfsbackup/config.yaml`
       kept as a compatibility symlink to a fully migrated
       `/var/lib/zfsbackup/config.db` must not be refused as YAML just
       because of the symlink's own name. Otherwise, the first 16 bytes
       are read and compared against SQLite's format magic; a mismatch,
       or a match on a file shorter than 100 bytes (too short to even be
       a truncated database, let alone meet SQLite's minimum page size of
       512 bytes) -> `ConfigDbNotADatabase` -- **not** `ConfigDbIsYaml`:
       telling an operator to import a zero-byte or corrupted file into
       itself is actively wrong advice, and this is exactly the case
       measurement E's zero-byte artifact produces. A `PermissionError`
       from the header read is deliberately *not* turned into "not a
       database" here -- it falls through to step 4, which diagnoses it
       correctly as a permission problem instead of a wrong file type;
       any other `OSError` (a TOCTOU unlink between step 1 and here, an
       `EIO`) is turned into `ConfigDbNotFound`/`ConfigDbPermissionError`
       respectively.
    4. Not readable (`os.access(path, os.R_OK, effective_ids=True)` where
       supported, corroborated by the header read's own success/failure
       above) -> `ConfigDbPermissionError`, "file not readable" sub-case.
    5. **Containing directory not writable, or not searchable** ->
       `ConfigDbPermissionError`, "directory not writable" sub-case.
       Checks both `os.W_OK` and `os.X_OK`: creating a file inside a
       directory needs search (execute) permission on it as well as
       write permission, and checking only one lets a directory missing
       the other through undetected. Hard failure regardless of the
       file's own mode: this is what `-wal`/`-shm` (re)creation needs.
    6. **File not writable.** In order:
       - Not yet in WAL mode (header bytes 18/19 != 2) -> hard fail.
         The store's WRITER engine issues `journal_mode=WAL` on connect,
         which must rewrite the header and cannot without write access
         (measurement A). Read-only engines no longer switch the mode
         (item 10.4), so this refusal is now a provisioning check rather
         than a prediction that a reader's connect would fail: a non-WAL
         file the daemon cannot write is refused early because the CLI
         writer and the daemon's readers would block each other on it.
       - Already WAL, but its `-wal`/`-shm` sidecars do not **both**
         already exist (checked with `os.lstat`, not `Path.exists()` --
         see below) -> hard fail. Opening now would let SQLite create
         the missing one(s) with this file's own (unwritable) mode,
         permanently, since an existing sidecar is never re-chmod'd --
         see module docstring, point 2. **This deliberately refuses a
         case that `open()` itself would succeed at** (measurement B).
       - Already WAL, sidecars both present, but one or both of *them*
         are not writable -> hard fail, naming the stuck sidecar(s)
         directly and how to fix them (`chmod`, never a blind `rm` of
         `-wal`). This is the H' state already reached.
       - Already WAL, sidecars both present **and** writable -> **also a
         hard failure**, not a warning. An earlier version of this
         function warned and let this case through, on the reasoning
         that measurement B shows such a file opens fine -- true, and
         beside the point: that state is transient in normal operation
         (the sidecars exist only because some other connection
         currently has the database open, and are checkpointed away on
         its clean close), so a preflight that observed them present was
         a TOCTOU window into exactly the permanent-wedge state the
         first two sub-cases above exist to refuse -- this process's own
         (lazily opened) connection can be the one that arrives after
         the sidecars are gone. There is no stable safe state to
         continue past here for a non-writable database file; a caller
         that genuinely only needs read access should pass
         `require_writable=False` instead of relying on this arm.

       `os.lstat`, not `Path.exists()`, decides whether each sidecar
       exists: `Path.exists()` returns `False` for *any* `OSError` it
       hits, not only `ENOENT` (permission denied on the sidecar itself,
       a symlink loop in its path, ...), silently turning "could not
       determine" into "does not exist" -- which would otherwise steer
       this ambiguous state into the "would be created read-only" arm on
       a wrong premise instead of surfacing the real problem.

    7. **Existing `-wal`/`-shm` sidecars** (writable-database path only):
       each one that exists (`lstat`) must be readable and writable by this
       process, else `ConfigDbPermissionError` -- SQLite would otherwise
       fail with a bare "unable to open database file". A group that
       differs from the database's is a WARNING. A sidecar that vanishes
       between the `lstat` and the access check (another connection's last
       close) counts as absent: a skip needs a positive ENOENT from a
       fresh `lstat`, an existing-but-unusable sidecar still raises.

    `require_writable=False` skips steps 5, 6 and 7 -- for a hypothetical
    future caller that genuinely only needs read access (nothing in this
    repository is such a caller yet).

    `allow_new=True` is a narrow escape hatch for item 8's importer, for
    exactly the one moment `create_config_db_file` leaves a file this
    preflight would otherwise refuse: a freshly created, **exactly
    zero-byte** file, which has no SQLite magic to check at all. It is
    never "skip the magic check" -- it skips step 3's magic-bytes and
    truncation comparisons **only when `os.stat` already measured the file
    at exactly 0 bytes**; a non-empty file that fails the magic-bytes
    check (a 1-byte file, a corrupted database, a YAML file saved without
    a `.yaml` suffix) still raises `ConfigDbNotADatabase` exactly as
    without this flag. Every other check still runs unchanged: the
    `.yaml`/`.yml` refusal (step 3, ahead of the magic-bytes check, so a
    zero-byte file that happens to be *named* `.yaml` is still refused as
    YAML, not silently admitted), readability (step 4), the containing
    directory's `W_OK`/`X_OK` (step 5), and file writability (step 6) --
    a zero-byte file that is also unwritable still hits step 6's
    not-yet-WAL hard fail, correctly, since `_is_wal_header` can never be
    true for a header this short. **Never reachable from the daemon**:
    nothing in `daemon.py`/`workers.py`/`runtime_config.py` passes this;
    it exists solely for `zfsbackup/config/store/importer.py`'s schema
    phase, immediately after `create_config_db_file`.

    Returns `None` on success.
    """
    # Step 0.
    resolved.url()

    real = resolved.resolved_path

    try:
        st = os.stat(real)
    except (FileNotFoundError, NotADirectoryError):
        raise ConfigDbNotFound(_not_found_message(resolved)) from None
    except OSError as exc:
        raise ConfigDbPermissionError(
            _residual_os_error_message(f"check {real}", exc)
        ) from exc

    if not stat_module.S_ISREG(st.st_mode):
        raise ConfigDbNotAFile(
            f"{real} exists but is not a regular file "
            f"(mode={oct(stat_module.S_IFMT(st.st_mode))}). Expected a "
            "SQLite database file."
        )

    # Delegates to check_config_db_suffix -- the one place that decides
    # what "looks like YAML" means, so this and importer.py's pre-phase-1
    # guard can never drift apart. See that function's docstring for why
    # it is decided from the RESOLVED target's own suffix, not
    # resolved.path's.
    check_config_db_suffix(resolved)

    # The `allow_new` gate reads the `os.stat` size from step 1, not the
    # header length read below -- st.st_size is the authoritative "exactly
    # 0 bytes" measurement `create_config_db_file` produces, and deciding
    # from it (rather than `len(header) == 0`) keeps this exact regardless
    # of how much of the file the header read below actually returns.
    skip_magic_check = allow_new and st.st_size == 0

    header: Optional[bytes]
    try:
        with open(real, "rb") as fh:
            header = fh.read(100)
    except FileNotFoundError:
        # TOCTOU: existed for the os.stat() above, gone by the time this
        # opened it. Reported the same way an ordinary "missing" is.
        raise ConfigDbNotFound(_not_found_message(resolved)) from None
    except PermissionError:
        header = None
    except OSError as exc:
        raise ConfigDbPermissionError(
            _residual_os_error_message(f"open {real}", exc)
        ) from exc
    else:
        if not skip_magic_check:
            if not header.startswith(_SQLITE_MAGIC):
                raise ConfigDbNotADatabase(_not_a_database_message(resolved, real))
            if len(header) < 100:
                raise ConfigDbNotADatabase(
                    _truncated_message(resolved, real, len(header))
                )

    if header is None or not _can_access(real, os.R_OK):
        raise ConfigDbPermissionError(_unreadable_message(resolved, real, st))

    if not require_writable:
        return

    directory = real.parent
    if not _can_access(directory, os.W_OK) or not _can_access(directory, os.X_OK):
        raise ConfigDbPermissionError(_dir_not_writable_message(resolved, directory))

    if not _can_access(real, os.W_OK):
        if not _is_wal_header(header):
            raise ConfigDbPermissionError(_file_not_writable_message(resolved, real))

        wal_path = Path(str(real) + "-wal")
        shm_path = Path(str(real) + "-shm")
        wal_state = _sidecar_lstat(wal_path)
        shm_state = _sidecar_lstat(shm_path)
        sidecars_exist = wal_state is not None and shm_state is not None

        if not sidecars_exist:
            raise ConfigDbPermissionError(
                _sidecars_would_be_created_readonly_message(resolved, real)
            )

        stuck = [p for p in (wal_path, shm_path) if not _can_access(p, os.W_OK)]
        if stuck:
            raise ConfigDbPermissionError(
                _stuck_sidecars_message(resolved, real, stuck)
            )

        # Sidecars present and writable right now -- but that state is
        # transient (see the docstring's step 6, last sub-case): refuse
        # rather than let this process's own later, lazy connection be
        # the one that reopens after a checkpoint removes them.
        raise ConfigDbPermissionError(
            _transient_sidecars_message(resolved, real)
        )

    # Step 7: existing sidecars, whether or not the DB file is writable (the
    # branch above always raises for an unwritable DB, so reaching here
    # means it IS writable -- and that says nothing about the sidecars). An
    # operator's CLI that was the first opener and died (SIGHUP while in
    # $EDITOR) leaves `-wal`/`-shm` owned by that operator in the
    # operator's group; the DB file itself is perfectly writable by the
    # daemon, so a check that only looked at the DB passed, and SQLite then
    # failed with an undiagnosable "unable to open database file" on every
    # respawn. Checked with `lstat` for existence, `os.access` for use.
    for sidecar_suffix in ("-wal", "-shm"):
        sidecar = Path(str(real) + sidecar_suffix)
        sidecar_st = _sidecar_lstat(sidecar)
        if sidecar_st is None:
            continue
        if not _can_access(sidecar, os.R_OK | os.W_OK):
            # A-10: a sidecar is transient by design -- another connection's
            # last close checkpoints and unlinks it, and `os.access` on a
            # path that has just vanished is False, indistinguishable from
            # "present but unusable". Raising here stopped the whole daemon
            # (EX_CONFIG -> exit 1) when two worker schema checks, a respawn
            # and a CLI exit, or a `systemctl stop` overlapped. So re-stat:
            # a POSITIVE ENOENT means it vanished (nothing to be unusable);
            # a sidecar that is still there gets one more access check and,
            # if that still fails, raises from the FRESH stat. A real
            # permission problem always still raises; no loop.
            sidecar_st = _sidecar_lstat(sidecar)
            if sidecar_st is None:
                continue
            if not _can_access(sidecar, os.R_OK | os.W_OK):
                raise ConfigDbPermissionError(
                    _sidecar_unusable_message(resolved, real, sidecar, sidecar_st)
                )
        if sidecar_st.st_gid != st.st_gid:
            logger.warning(
                "%s has group %s but the database %s has group %s: a "
                "process in only one of those groups may be locked out. "
                "Fix: chgrp %s %s (the directory should be setgid, mode "
                "%04o, so new sidecars inherit its group).",
                sidecar, _groupname(sidecar_st.st_gid), real,
                _groupname(st.st_gid), _groupname(st.st_gid), sidecar,
                CONFIG_DIR_MODE,
            )


def diagnose_open_failure(
    resolved: ResolvedConfigPath, exc: Exception, *, require_writable: bool = True
) -> Optional[ConfigPathError]:
    """Re-run the `check_config_db` diagnosis after an actual SQLite open
    already failed, for item 8's and item 10's `except OperationalError`.

    Exists because `os.access` -- which `check_config_db` uses for its
    writability checks -- is uid-based (partially mitigated by
    `effective_ids=True` where supported), blind to POSIX ACLs, and
    always permissive for root. The daemon runs as root today, so
    `check_config_db` can pass and a real open can still fail; this
    function gives that failure a diagnosis by re-running the same
    `stat`/header checks (**never** by inspecting `exc`'s message text)
    rather than leaving a raw, opaque exception to propagate.

    `require_writable` is forwarded to `check_config_db` unchanged --
    passing `False` here when the original open used `readonly=True`
    (i.e. did not itself require write access) avoids a confidently
    wrong "directory not writable" diagnosis for an open that never
    needed the directory to be writable in the first place. Forgetting
    to forward it, and always re-checking at the default `True`, is
    exactly the earlier version of this function's bug.

    Returns the `ConfigPathError` `check_config_db` would have raised, with
    its `__cause__` set to `exc` so the original SQLite error stays
    reachable for debugging. Returns `None` if `check_config_db` finds
    nothing wrong, in which case the caller must re-raise `exc` itself;
    this function never raises.
    """
    try:
        check_config_db(resolved, require_writable=require_writable)
    except ConfigPathError as diagnosed:
        diagnosed.__cause__ = exc
        return diagnosed
    return None


# --------------------------------------------------------------------------
# The sanctioned, preflighted ways to reach a Session/Connection (review
# item 9). Both are context managers -- neither hands back a bare, storable
# `Engine`. `resolve_config_url` stays available for a caller with its own
# reason to bypass the preflight entirely.
# --------------------------------------------------------------------------


@contextmanager
def open_config_session(
    resolved: ResolvedConfigPath,
    *,
    readonly: bool,
    require_writable: bool = True,
    allow_new: bool = False,
    ephemeral: bool = False,
) -> Iterator[Session]:
    """`check_config_db(resolved)`, then a `Session` from `db.py`'s
    `session_scope(resolved.url(), readonly=readonly)`, scoped to this
    `with` block. The primary sanctioned entrypoint for item 8's
    `load_config`/`save_config` use of `mapper.py`.

    `ephemeral=True` (TLS/mTLS plan M7, `store/trust.py`): the session runs
    on a fresh, unpooled, uncached engine that is disposed when the block
    exits (`db.ephemeral_engine`) instead of the pid-keyed cache. Safe to
    use from any thread of a long-lived process; leaves no connection
    behind. Same preflight, pragmas and `BEGIN IMMEDIATE` for writers.

    **Deliberately a context manager, not a function returning an
    `Engine`.** An earlier version of this module exposed
    `open_config_db(...) -> Engine`. That reopened, by API shape, the
    exact fork hole `db.py`'s own module docstring closes: nothing there
    returns a bare, long-lived `Session` or `Engine`, specifically
    because a handle captured before a `multiprocessing.Process` fork is
    invisible to `get_engine`'s pid-keyed cache and gets inherited by
    every child. A supervisor that did `self._engine =
    open_config_db(...)` during startup, ran one `load_config` through
    it (leaving a live pooled connection), and then forked workers at
    `_start_workers()` would hand every child a shared file descriptor on
    the same SQLite database -- the corruption hazard `get_engine`'s
    cache exists to prevent, reached without ever calling `get_engine`
    again, because the child has no reason to. `session_scope` resolves
    its engine through that cache **on every call, inside its own `with`
    block** -- the exact discipline `db.py`'s module docstring describes
    -- and this function does nothing but preflight in front of it.

    `require_writable` defaults to `True` regardless of `readonly`,
    matching `db.py`'s own design: even a read-only *session* opens the
    underlying file read-write at the OS level and needs a writable
    containing directory (see this module's docstring's closing
    paragraph).

    `allow_new` is forwarded to `check_config_db` unchanged -- see its
    docstring. Plumbed here for symmetry with `open_config_connection`;
    item 8's importer opens its data phase (`save_config`) through this
    function only *after* the schema phase has already run, so by the time
    it does the file is no longer zero-byte and `allow_new` is left at its
    default here in practice.
    """
    check_config_db(resolved, require_writable=require_writable, allow_new=allow_new)
    with session_scope(
        resolved.url(), readonly=readonly, ephemeral=ephemeral
    ) as session:
        yield session


@contextmanager
def open_config_connection(
    resolved: ResolvedConfigPath,
    *,
    readonly: bool = False,
    foreign_keys: bool = True,
    require_writable: bool = True,
    allow_new: bool = False,
    ephemeral: bool = False,
) -> Iterator[Connection]:
    """`check_config_db(resolved)`, then a raw `Connection` from
    `get_engine(resolved.url(), readonly=readonly, foreign_keys=
    foreign_keys)`, scoped to this `with` block. For item 8's
    `check_schema`/`ensure_schema` (`migrate.py`), which take a
    `Connection`, never a `Session` or a URL.

    Same context-manager discipline as `open_config_session`, and for the
    same reason -- see that function's docstring. The `Connection` this
    yields must not be extracted and kept past the `with` block for the
    identical reason a `Session` must not be.

    `foreign_keys=False` remains the deliberate, named opt-out
    `get_engine` itself defines, for Alembic's batch migrations, which
    recreate tables and must run with FK enforcement off; nothing else
    should pass it. See `db.py`'s notes on sequencing this ahead of any
    writer session -- opening this nested inside a live
    `open_config_session` write self-deadlocks on SQLite's write lock.

    `allow_new` is forwarded to `check_config_db` unchanged -- see its
    docstring. This is the parameter item 8's importer actually needs: its
    schema phase opens the connection that runs `ensure_schema` against a
    file `create_config_db_file` just created, which is exactly zero bytes
    until Alembic's first `CREATE TABLE`.

    `ephemeral=True`: as for `open_config_session` -- a fresh, unpooled
    engine for this block only (`db.ephemeral_engine`), never cached.
    """
    check_config_db(resolved, require_writable=require_writable, allow_new=allow_new)
    if ephemeral:
        with ephemeral_engine(
            resolved.url(), readonly=readonly, foreign_keys=foreign_keys
        ) as engine:
            with engine.connect() as connection:
                yield connection
        return
    engine = get_engine(
        resolved.url(), readonly=readonly, foreign_keys=foreign_keys
    )
    with engine.connect() as connection:
        yield connection


# --------------------------------------------------------------------------
# Creation policy (6a-4) -- called by `importer.import_yaml` (the only
# sanctioned caller of the creation helpers).
# --------------------------------------------------------------------------


def ensure_config_dir(path: Union[str, Path]) -> None:
    """Create `path` as a directory (with any missing parents) if needed and
    bring it to `CONFIG_DIR_MODE` (setgid, group-writable), explicitly.

    The explicit chmod after `mkdir` is not redundant: `Path.mkdir`'s own
    `mode=` argument is masked by the process umask, so `mkdir(...,
    mode=0o2770)` under a umask of `0o022` would actually create a `0o2750`
    directory -- silently missing the group-write bit every WAL sidecar
    creation depends on. Chmod-ing explicitly makes the result independent
    of whatever umask the calling process happens to have.

    The chmod goes through `_ensure_mode`, so it is **not** unconditional
    (it used to be, and that was the bug): a directory that already has the
    right mode is not touched, a directory owned by someone else (the
    normal case for a group operator: the directory belongs to user
    `zfsbackup`) is never chmod'd -- that was EPERM -- and the setgid bit
    surviving the chmod is verified rather than assumed. See `_ensure_mode`.

    Idempotent (`exist_ok=True`): re-running `import` against an existing,
    correctly-laid-out directory is a no-op.

    Both the `mkdir` and the chmod are wrapped: an unprivileged operator
    running `import` against a missing or unwritable `/var/lib/zfsbackup`
    gets a `ConfigDbPermissionError` naming the actual owner/group/mode of
    the parent directory and the target, rather than a bare
    `PermissionError: [Errno 13]`.

    **Ownership.** Creating the `zfsbackup` user and group is a packaging
    concern -- a `sysusers.d`/`tmpfiles.d` snippet, or a systemd unit's
    `User=`/`Group=`. The one thing this function does about it: when run
    as ROOT and it CREATES the default directory (`DEFAULT_CONFIG_DB.parent`),
    it `chgrp`s it to `zfsbackup` if that group exists (and warns loudly if
    not) -- see `_chgrp_to_service_group`. It never changes the group of a
    directory it did not create.
    """
    directory = Path(path)
    try:
        existed = os.path.isdir(directory)
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigDbPermissionError(
            _permission_failure_message(
                f"create directory {directory}", directory.parent, exc
            )
        ) from exc
    if not existed and is_default_config_dir(directory):
        _chgrp_to_service_group(directory, "config directory")
    _ensure_mode(
        directory, CONFIG_DIR_MODE, access=os.W_OK | os.X_OK, what="config directory"
    )


def create_config_db_file(path: Union[str, Path]) -> None:
    """Create an empty file at `path`, atomically fixed to `CONFIG_DB_MODE`,
    as the one step item 10's `import` should use to bring a brand-new
    database file into existence.

    Exists so the mandated ordering -- fix the mode *before* the first
    SQLite connection, never after (see `ensure_config_db_mode`'s
    docstring for why an after-the-fact chmod is not equivalent) -- is not
    something item 10's implementation can get backwards by writing the
    natural-looking "connect, then chmod" sequence instead. `os.open`'s
    own `mode=` argument is, like `mkdir`'s, masked by the process umask,
    so an explicit `os.fchmod` after the `open()` is required here too,
    on the same file descriptor, before anything else can observe the
    file.

    Uses `O_CREAT | O_EXCL` so this fails loudly (`ConfigDbPermissionError`
    wrapping `FileExistsError`) rather than silently truncating an
    existing database -- `import` against an existing file is a
    different, explicit operation this function deliberately does not
    perform.

    (Q7) When run as ROOT on the DEFAULT path, the new file is `fchown`ed to
    the `zfsbackup` group if it exists (warning if not), before the mode is
    fixed -- the mode is set last because a chown can clear mode bits.

    **Atomic: it either fully succeeds or leaves nothing behind.** If
    anything raises after `O_EXCL` has created the file -- the Q7 `fchown`
    (NFS `root_squash` makes that realistic), the `fchmod`, or a
    `KeyboardInterrupt` -- the half-made file is unlinked again, so a retry
    gets the first-import treatment (`created=True`) instead of finding a
    zero-byte file, taking the re-import path, and being refused as
    `ConfigDbNotADatabase` until someone deletes it by hand. The unlink
    only happens when the open descriptor (`fstat`) and the path (`lstat`)
    still name the same inode, so a file that was swapped in meanwhile is
    never removed; it is done while the descriptor is still open and is best
    effort (failure is logged, never raised over the original error). This
    lives here, not in the importer, so every caller (the `import` CLI too)
    gets it.
    """
    target = Path(path)
    try:
        fd = os.open(
            str(target), os.O_CREAT | os.O_EXCL | os.O_WRONLY, CONFIG_DB_MODE
        )
    except OSError as exc:
        raise ConfigDbPermissionError(
            _permission_failure_message(f"create {target}", target.parent, exc)
        ) from exc
    try:
        try:
            if is_default_config_path(target):
                _chgrp_to_service_group(target, "config database", fd=fd)
            os.fchmod(fd, CONFIG_DB_MODE)
        except OSError as exc:
            raise ConfigDbPermissionError(
                _permission_failure_message(
                    f"chmod {target} to {CONFIG_DB_MODE:04o}", target, exc
                )
            ) from exc
    except BaseException:
        _unlink_if_same_file(fd, target)
        raise
    finally:
        os.close(fd)


def _unlink_if_same_file(fd: int, path: Path) -> None:
    """Best-effort `os.unlink(path)` of the file `fd` was just created as,
    but only if `path` still names the same inode (`fstat(fd)` vs
    `lstat(path)`). Never raises: it runs while the real exception is
    propagating (see `create_config_db_file`)."""
    try:
        opened = os.fstat(fd)
        named = os.lstat(path)
        if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
            return
        os.unlink(path)
    except OSError as exc:
        logger.warning(
            "could not remove the partially created %s after a failure: %s "
            "-- remove it by hand before retrying the import", path, exc,
        )


def ensure_config_db_mode(path: Union[str, Path]) -> None:
    """Fix the permission bits of an EXISTING default config database and the
    `-wal`/`-shm` sidecars next to it -- and nothing else.

    **Only the default path is adjusted** (`DEFAULT_CONFIG_DB`, however
    spelled). An existing file at any other path (`-c ~/x.db`, a test
    fixture, a deliberately private `0600` file) is NEVER chmod'd: an
    earlier version widened an operator-supplied `0600` file to `0660` on
    every re-import. For such a file this only logs a WARNING if its group
    cannot read and write it, since then group operators and a de-rooted
    daemon will be locked out -- the operator chose that, but should know.

    For the default path, everything goes through `_ensure_mode`: chmod
    only when the mode differs AND the file is owned by this process (its
    effective uid owns it, or this process is root -- root can chmod
    anything, so it also brings a daemon-owned database or sidecar to
    `CONFIG_DB_MODE`); otherwise warn (usable) or raise (unusable) without
    touching it. Nothing is ever widened beyond `CONFIG_DB_MODE`. In
    particular a **sidecar this process does not own is never chmod'd** (it
    belongs to whichever process created it, typically the daemon user's
    pooled connection), and sidecars are examined with `lstat` so a symlink
    is skipped rather than followed. `-journal` is never touched: it is a
    transient rollback-journal file that only exists during a transaction.

    **Must be called before the first SQLite connection is opened against
    the database** -- prefer `create_config_db_file` for a brand-new file,
    which makes that ordering structural. This function remains for the
    re-import case.

    This is not cosmetic. Measured: SQLite copies the *database file's*
    mode onto `-wal`/`-shm` the moment it creates them -- independent of
    the process's umask (tested under `0o022`, `0o007` and `0o077`: every
    umask produced `0o660` sidecars once the DB file itself was `0o660`
    before the first connection) -- but it never re-chmods a sidecar that
    already exists, and never copies the group. Hence the setgid directory
    (`CONFIG_DIR_MODE`) for the group, and re-fixing the mode of sidecars
    we own here for the bits.

    Every `chmod` is wrapped: a failure surfaces as `ConfigDbPermissionError`
    with actual-vs-target detail, not a bare `PermissionError`.
    """
    target = Path(path)
    if not is_default_config_path(target):
        try:
            mode = stat_module.S_IMODE(os.stat(target).st_mode)
        except OSError:
            return  # the real preflight (check_config_db) diagnoses this
        if mode & 0o060 != 0o060:
            logger.warning(
                "%s has mode %04o and is not at the default path, so its "
                "permissions were left alone. Its group cannot read and "
                "write it, so group operators and a daemon running as "
                "another user are locked out unless they own it.",
                target, mode,
            )
        return

    _ensure_mode(
        target, CONFIG_DB_MODE, access=os.R_OK | os.W_OK, what="config database"
    )
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(target) + suffix)
        sidecar_st = _sidecar_lstat(sidecar)
        if sidecar_st is None:
            continue
        if stat_module.S_ISLNK(sidecar_st.st_mode):
            logger.warning(
                "%s is a symlink; leaving it alone (SQLite sidecars are "
                "never expected to be links).", sidecar,
            )
            continue
        _ensure_mode(
            sidecar, CONFIG_DB_MODE, access=os.R_OK | os.W_OK,
            what="WAL sidecar",
        )


# --------------------------------------------------------------------------
# CLI writer lock (item 15.4)
# --------------------------------------------------------------------------

#: Appended to the resolved database path: `config.db` -> `config.db.writer.lock`.
#: NOT `<db>.lock`: that is the name SQLite's `unix-dotfile` VFS uses for its
#: own locking, and a file we flock'd under that name could be mistaken for
#: (or interfere with) SQLite's.
WRITER_LOCK_SUFFIX = ".writer.lock"

#: How long `writer_lock` waits for another zfsbackup-config writer before
#: giving up with `ConfigBusy`. Longer than `busy_timeout` (5 s) on purpose: a
#: second writer queueing behind an `import` that is mid-migration should
#: normally just wait its turn rather than fail.
DEFAULT_WRITER_LOCK_TIMEOUT = 10.0

#: `(st_dev, st_ino)` of the lock files this THREAD currently holds. `flock`
#: locks belong to the open file description, so a second `writer_lock` in the
#: same thread would open a new description and wait on itself for the whole
#: timeout, then report a "busy" that names a writer that does not exist.
#: Refusing loudly is the honest failure. Other threads are not affected:
#: they legitimately contend and get `ConfigBusy` on timeout.
_writer_locks_held = threading.local()


def writer_lock_path(resolved: ResolvedConfigPath) -> Path:
    """The lock file beside `resolved`'s database. Uses `resolved_path`, so
    two spellings of one database (a symlink, a relative path) share a lock."""
    return Path(str(resolved.resolved_path) + WRITER_LOCK_SUFFIX)


def require_writer_lock_held(resolved: ResolvedConfigPath, lock: object) -> None:
    """Raise `ValueError` unless `lock` is the object `writer_lock(resolved)`
    yielded AND the calling thread still holds it.

    This is what lets `write_config(..., lock=held)` skip re-acquiring without
    trusting an arbitrary argument: a `lock=` that is a stale handle (the
    `with` block already exited), belongs to another database, or was taken by
    another thread would otherwise turn "I hold the lock" into a silent
    unlocked write. Checks the lock file's (device, inode) against this
    thread's held set -- the same key `writer_lock` uses for its re-entry
    guard -- so two spellings of one database agree. Never blocks and takes
    nothing.
    """
    expected = writer_lock_path(resolved)
    not_ours = ValueError(
        f"lock={lock!r} is not the writer lock of {resolved.path} "
        f"(expected the object `writer_lock(resolved)` yielded, {expected})"
    )
    if not isinstance(lock, Path):
        raise not_ours
    try:
        st = os.stat(str(expected))
    except OSError as exc:
        raise ValueError(f"the writer lock {expected} cannot be inspected: {exc}") from exc
    if lock != expected:
        # A different spelling of the same database (relative vs absolute,
        # a symlinked directory...) names the same lock file: compare files,
        # not strings.
        try:
            lst = os.stat(str(lock))
        except OSError:
            raise not_ours from None
        if (lst.st_dev, lst.st_ino) != (st.st_dev, st.st_ino):
            raise not_ours
    held = getattr(_writer_locks_held, "keys", None) or set()
    if (st.st_dev, st.st_ino) not in held:
        raise ValueError(
            f"this thread does not hold the writer lock {expected} (a handle "
            "from a `with writer_lock(...)` block that has exited, or from "
            "another thread); refusing to write unlocked"
        )


def _open_writer_lock_fd(lock: Path, *, default_db: bool) -> int:
    """Open (creating if missing) the lock file and return an fd, never
    following a symlink. Follows `create_config_db_file`'s and
    `_ensure_mode`'s rules: a file THIS call creates is brought to
    `CONFIG_DB_MODE` on its own fd before anything else can see it (and, for
    root on the default path, given to the service group); an existing file is
    only ever chmod'd on the default path and only by its owner (`_ensure_mode`).
    The file is never deleted -- an unlink would let one writer lock the old
    inode while another creates and locks a new one, i.e. two holders."""
    base = os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    last_error: Optional[OSError] = None
    for _ in range(3):
        try:
            fd = os.open(
                str(lock), base | os.O_CREAT | os.O_EXCL | os.O_RDWR, CONFIG_DB_MODE
            )
        except FileExistsError:
            pass
        except OSError as exc:
            raise ConfigDbPermissionError(
                _permission_failure_message(
                    f"create the writer lock {lock}", lock.parent, exc
                )
            ) from exc
        else:
            try:
                if default_db:
                    _chgrp_to_service_group(lock, "writer lock file", fd=fd)
                os.fchmod(fd, CONFIG_DB_MODE)
            except OSError as exc:
                os.close(fd)
                raise ConfigDbPermissionError(
                    _permission_failure_message(
                        f"chmod {lock} to {CONFIG_DB_MODE:04o}", lock, exc
                    )
                ) from exc
            except BaseException:
                os.close(fd)
                raise
            return fd

        # Exists. flock(2) needs no write access, so an operator who can only
        # read a root-made lock file can still take the lock.
        for access in (os.O_RDWR, os.O_RDONLY):
            try:
                fd = os.open(str(lock), base | access)
            except FileNotFoundError:
                break  # vanished (hand-deleted); try creating again
            except PermissionError as exc:
                last_error = exc
                continue
            except OSError as exc:
                raise ConfigDbPermissionError(
                    _permission_failure_message(
                        f"open the writer lock {lock}", lock, exc
                    )
                ) from exc
            if default_db:
                try:
                    _ensure_mode(
                        lock, CONFIG_DB_MODE, access=os.R_OK, what="writer lock file"
                    )
                except BaseException:
                    os.close(fd)
                    raise
            return fd
        else:
            raise ConfigDbPermissionError(
                _permission_failure_message(
                    f"open the writer lock {lock}", lock, last_error  # type: ignore[arg-type]
                )
            ) from last_error
    raise ConfigDbPermissionError(
        f"Cannot open the writer lock {lock}: it keeps appearing and "
        "disappearing. Something is deleting it; it must never be removed."
    )


@contextmanager
def writer_lock(
    resolved: ResolvedConfigPath, *, timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT
) -> Iterator[Path]:
    """Hold an exclusive `flock` on `<db>.writer.lock` for the `with` block.

    Serialises every `zfsbackup-config` WRITER (`write_config`, `import_yaml`)
    against every other, across processes. SQLite's own write lock already
    serialises individual transactions; this one covers what SQLite cannot see:
    a writer's decisions *before* it opens a transaction. Without it two
    imports racing on a missing database both decide "I create it", and
    `import_yaml`'s failed-first-import cleanup has to guess -- from sidecar
    files and an `lstat` fingerprint -- whether anybody else has attached
    (see `importer.py`'s A-11 notes). Under this lock, that question has an
    answer: nobody who honours the lock.

    **Scope.** It binds only openers that take it. A process that opens the
    database directly (the `sqlite3` shell, a script, a different program) is
    outside it, and the daemon never takes it -- the daemon only reads, and
    under WAL a reader neither blocks nor is blocked by this lock's holder.

    Blocks for up to `timeout` seconds (polling a non-blocking `flock`, so a
    stuck holder cannot hang the CLI forever), then raises `ConfigBusy`.
    `timeout=0` tries exactly once. Not re-entrant: taking it twice in one
    thread raises `RuntimeError` instead of waiting on itself.

    The lock file is created if missing (0660, in the setgid config directory,
    group = the directory's, or the service group for root on the default path)
    and NEVER deleted. The directory must already exist and be searchable --
    this function creates no directories, so a caller that may be the first
    to run (`import_yaml`) must create it first. It is released when the block
    exits, by any means, and by the kernel if the process dies.

    The lock file adds one more file next to the database, besides WAL's
    `-wal`/`-shm`: it matters for packaging, backups (do not archive it as
    state), and `scenarios/` cleanup.
    """
    lock = writer_lock_path(resolved)
    fd = _open_writer_lock_fd(
        lock, default_db=is_default_config_path(resolved.resolved_path)
    )
    try:
        st = os.fstat(fd)
        key = (st.st_dev, st.st_ino)
        held: Set[Tuple[int, int]] = getattr(_writer_locks_held, "keys", None) or set()
        _writer_locks_held.keys = held
        if key in held:
            raise RuntimeError(
                f"writer_lock({lock}) is not re-entrant: this thread already "
                "holds it. Take the lock once, at the outermost write."
            )

        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                    raise ConfigDbPermissionError(
                        _permission_failure_message(
                            f"lock {lock}", lock, exc
                        )
                    ) from exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ConfigBusy(
                        f"another zfsbackup-config write to {resolved.path} is "
                        f"still in progress (waited {timeout:g}s for {lock}); "
                        "retry in a moment."
                    ) from None
                time.sleep(min(0.05, remaining))

        held.add(key)
        try:
            yield lock
        finally:
            held.discard(key)
    finally:
        # Closing releases the flock once the last descriptor on the open file
        # description is gone. An explicit LOCK_UN ends it now even if a forked
        # child still holds a duplicate of the description (flock locks belong
        # to the description, which a fork shares): the block is over, so the
        # lock is too.
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(fd)

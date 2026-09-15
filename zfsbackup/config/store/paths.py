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

This module has no daemon caller yet: `daemon.py` still loads YAML
(`BackupConfig.from_file`), and wiring the daemon to `check_schema`/
`open_config_session` is item 8's job, not this one's.

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
   `python = "^3.10"`, and on 3.10/3.11/3.12 (verified directly on all
   three) `Path.resolve(strict=False)` turns a symlink loop into a bare
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
via an explicit `zfsbackup-config import` (item 10), which must call, in
order: `ensure_config_dir`, `create_config_db_file` (creates the file AND
fixes its mode as one step, so the mandated create-then-chmod-before-
first-connection ordering is not something item 10's implementation can
get backwards), then open its first connection, then run
`ensure_schema`. `ensure_config_db_mode` remains for the case of *fixing*
the mode of a database this module did not itself create (an existing
file handed to `import`, or a re-import) -- it also re-chmods any
`-wal`/`-shm` that already exist next to it, since those are exactly as
likely to be stuck at a stale mode as the case `ensure_config_db_mode`'s
own docstring already warns about for the database file itself.

"The daemon only reads" describes config *content*, not the *file*: the
daemon's read-only engine still opens the database file read-write at the
OS level (it issues `PRAGMA journal_mode=WAL` on every connection, which
must be able to rewrite the header the first time) and needs a writable
containing directory for the `-shm` reason above.
"""

from __future__ import annotations

import logging
import os
import stat as stat_module
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Literal, Mapping, Optional, Union

from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from zfsbackup.config.store.db import get_engine, session_scope, url_for_path

logger = logging.getLogger(__name__)

__all__ = [
    "CONFIG_DB_MODE",
    "CONFIG_DIR_MODE",
    "CONFIG_OWNER",
    "CONFIG_PATH_ENV",
    "ConfigDbIsYaml",
    "ConfigDbNotADatabase",
    "ConfigDbNotAFile",
    "ConfigDbNotFound",
    "ConfigDbPathUnrepresentable",
    "ConfigDbPermissionError",
    "ConfigPathEnvError",
    "ConfigPathError",
    "DEFAULT_CONFIG_DB",
    "ResolvedConfigPath",
    "check_config_db",
    "create_config_db_file",
    "diagnose_open_failure",
    "ensure_config_db_mode",
    "ensure_config_dir",
    "open_config_connection",
    "open_config_session",
    "resolve_config_path",
    "resolve_config_url",
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
CONFIG_DIR_MODE = 0o770

#: Group-write for the CLI (the only writer of config *content*);
#: owner-write for the daemon even though it writes no config content --
#: its read-only engine still opens the file read-write at the OS level
#: and needs write access to flip a not-yet-WAL file into WAL mode on its
#: very first connection.
CONFIG_DB_MODE = 0o660

#: `chown` needs root, which the CLI operator invoking `import` is not.
#: Creating this user/group is a packaging concern (`sysusers.d`/
#: `tmpfiles.d`, or a systemd unit's `User=`/`Group=`), not something this
#: module can or should do -- it must already exist before `import` runs.
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


# --------------------------------------------------------------------------
# Small helpers shared between resolution and the preflight.
# --------------------------------------------------------------------------


def _residual_os_error_message(action: str, exc: Exception) -> str:
    """Format a filesystem-level failure that is not a simple permission
    denial. Deliberately typed to accept more than `OSError`: see
    `_resolve_or_raise` below for why -- on Python 3.10-3.12,
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

    - A symlink loop. **On Python 3.10, 3.11, and 3.12** (this package
      declares `python = "^3.10"` in `pyproject.toml`, so all three are
      in scope), `Path.resolve(strict=False)` performs a final re-`stat`
      of its own result and converts the underlying `OSError(ELOOP)`
      into a bare, undocumented `RuntimeError` ("Symlink loop from...");
      verified directly on 3.10, 3.11, and 3.12. Python 3.13+ raises
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
    `resolve_config_path` itself with a bare `RuntimeError` on 3.10-3.12.
    This is also why `check_config_db`'s own residual `except OSError`
    arm on its `os.stat` step is defence in depth rather than the only
    guard against ELOOP/ENAMETOOLONG-shaped failures: on 3.10-3.12 a
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
    for a path that does not exist silently creates a zero-byte database.
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
    return (
        f"{shown} has a .yaml/.yml extension. zfsbackup's config store "
        "is a SQLite database, not YAML. To migrate an existing YAML "
        f"config, run: zfsbackup-config import {given}"
    )


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
        "WAL journal mode. Every engine this store builds -- including "
        "read-only ones -- issues `PRAGMA journal_mode=WAL` on its "
        "first connection (zfsbackup.config.store.db), which must "
        "rewrite the file's journal-mode header the first time, and "
        f"that needs write access. Target: {real} should be "
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
# Preflight (6a-3)
# --------------------------------------------------------------------------


def check_config_db(
    resolved: ResolvedConfigPath, *, require_writable: bool = True
) -> None:
    """Raise a `ConfigPathError` subclass if `resolved.path` cannot be
    used as the config database, without ever building a SQLite engine.

    **Must run before any engine is built.** `make_engine(url,
    readonly=True).connect()` on a path that does not yet exist silently
    creates a zero-byte file. A `stat`-based check performed first is the
    only way to observe "did not previously exist" at all; see module
    docstring.

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
         Every engine this store builds re-issues `journal_mode=WAL` on
         connect, which must rewrite the header and cannot without write
         access (measurement A).
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

    `require_writable=False` skips steps 5 and 6 -- for a hypothetical
    future caller that genuinely only needs read access (nothing in this
    repository is such a caller yet).

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

    # Decided from the RESOLVED target's own suffix, not resolved.path's
    # -- a `/etc/zfsbackup/config.yaml` compatibility symlink pointing at
    # a real, fully migrated `config.db` must not be refused just
    # because of its own name.
    if real.suffix in (".yaml", ".yml"):
        raise ConfigDbIsYaml(_yaml_message(resolved))

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
    resolved: ResolvedConfigPath, *, readonly: bool, require_writable: bool = True
) -> Iterator[Session]:
    """`check_config_db(resolved)`, then a `Session` from `db.py`'s
    `session_scope(resolved.url(), readonly=readonly)`, scoped to this
    `with` block. The primary sanctioned entrypoint for item 8's
    `load_config`/`save_config` use of `mapper.py`.

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
    """
    check_config_db(resolved, require_writable=require_writable)
    with session_scope(resolved.url(), readonly=readonly) as session:
        yield session


@contextmanager
def open_config_connection(
    resolved: ResolvedConfigPath,
    *,
    readonly: bool = False,
    foreign_keys: bool = True,
    require_writable: bool = True,
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
    """
    check_config_db(resolved, require_writable=require_writable)
    engine = get_engine(
        resolved.url(), readonly=readonly, foreign_keys=foreign_keys
    )
    with engine.connect() as connection:
        yield connection


# --------------------------------------------------------------------------
# Creation policy (6a-4) -- record-only. Nothing here is called yet; item
# 10's `import` is the sanctioned caller.
# --------------------------------------------------------------------------


def ensure_config_dir(path: Union[str, Path]) -> None:
    """Create `path` as a directory (with any missing parents) and set it
    to `CONFIG_DIR_MODE`, explicitly.

    The explicit `os.chmod` after `mkdir` is not redundant: `Path.mkdir`'s
    own `mode=` argument is masked by the process umask, so `mkdir(...,
    mode=0o770)` under a umask of `0o022` would actually create a `0o750`
    directory -- silently missing the group-write bit every WAL sidecar
    creation depends on. Chmod-ing explicitly makes the result
    independent of whatever umask the calling process happens to have.

    Idempotent (`exist_ok=True`): re-running `import` against an existing,
    correctly-laid-out directory must not fail, and the chmod is applied
    unconditionally so a directory that exists but is not yet `0o770` is
    corrected rather than silently left alone.

    Both the `mkdir` and the `chmod` are wrapped: an unprivileged
    operator running `import` against a missing or unwritable
    `/var/lib/zfsbackup` gets a `ConfigDbPermissionError` naming the
    actual owner/group/mode of the parent directory and the target,
    rather than a bare `PermissionError: [Errno 13]`.

    **Does not `chown`.** Creating the `zfsbackup` user and group is a
    packaging concern -- a `sysusers.d`/`tmpfiles.d` snippet, or a systemd
    unit's `User=`/`Group=` -- and doing it here would need root, which
    the CLI operator invoking `import` is not expected to have.
    """
    directory = Path(path)
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigDbPermissionError(
            _permission_failure_message(
                f"create directory {directory}", directory.parent, exc
            )
        ) from exc
    try:
        os.chmod(directory, CONFIG_DIR_MODE)
    except OSError as exc:
        raise ConfigDbPermissionError(
            _permission_failure_message(
                f"chmod {directory} to {CONFIG_DIR_MODE:04o}", directory, exc
            )
        ) from exc


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
        os.fchmod(fd, CONFIG_DB_MODE)
    finally:
        os.close(fd)


def ensure_config_db_mode(path: Union[str, Path]) -> None:
    """`os.chmod(path, CONFIG_DB_MODE)`, and the same for any `-wal`/
    `-shm` that already exist next to it.

    **Must be called immediately after the database file is created, and
    before the first SQLite connection is opened against it** -- prefer
    `create_config_db_file` for a brand-new file, which makes that
    ordering structural rather than a prose requirement an implementation
    can violate. This function remains for *fixing* the mode of a
    database this module did not itself create (an existing file handed
    to `import`, or a re-import).

    This is not cosmetic. Measured: SQLite copies the *database file's*
    mode onto `-wal`/`-shm` the moment it creates them -- independent of
    the process's umask (tested under `0o022`, `0o007` and `0o077`: every
    umask produced `0o660` sidecars once the DB file itself was `0o660`
    before the first connection) -- but it never re-chmods a sidecar that
    already exists. So if `import` instead let SQLite create `config.db`
    first (inheriting whatever mode the process's umask produces) and
    only chmod'd it to `0o660` afterwards, the DB file itself would end
    up `0o660`, but **every `-wal`/`-shm` it ever creates for the rest of
    that file's life would still come out at the earlier mode** (measured:
    sidecars created after such a late chmod stayed at `0o644`), and a
    second group member -- or a de-rooted daemon -- would be unable to
    write them.

    For the same reason, a **re-import** against a database that already
    has `-wal`/`-shm` sidecars (created by a scenario harness, a plain
    `cp` that brought them along, or a bare `sqlite3` invocation) must
    re-chmod those sidecars too, not just the database file -- an
    existing sidecar left at a stale mode is exactly the H' state
    `check_config_db`'s step 6 detects and refuses at open time, and this
    function existing at all is supposed to prevent reaching that state
    in the first place.

    Every `chmod` here is wrapped: a failure surfaces as
    `ConfigDbPermissionError` with actual-vs-target detail, not a bare
    `PermissionError`.
    """
    target = Path(path)
    try:
        os.chmod(target, CONFIG_DB_MODE)
    except OSError as exc:
        raise ConfigDbPermissionError(
            _permission_failure_message(
                f"chmod {target} to {CONFIG_DB_MODE:04o}", target, exc
            )
        ) from exc

    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(target) + suffix)
        if sidecar.exists():
            try:
                os.chmod(sidecar, CONFIG_DB_MODE)
            except OSError as exc:
                raise ConfigDbPermissionError(
                    _permission_failure_message(
                        f"chmod {sidecar} to {CONFIG_DB_MODE:04o}", sidecar, exc
                    )
                ) from exc

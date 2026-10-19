"""Operator-facing wording for config-store failures, shared by every tool
that opens the store (the daemon and `zfsbackup-config`).

Moved here from `zfsbackup/runtime_config.py` (item 15.1) so the two tools
describe the same failure with the same sentence. The one deliberate
difference between them is the daemon's closing "start the daemon again"
advice for a stale schema: that belongs to the daemon alone, so
`config_error_message` returns the store's neutral text and the daemon's
wrapper appends its own sentence.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy.exc import DatabaseError, OperationalError

from zfsbackup.config.store.errors import ConfigBusy, ConfigConflict
from zfsbackup.config.store.migrate import SchemaError
from zfsbackup.config.store.paths import (
    CONFIG_PATH_ENV,
    ConfigPathError,
    ResolvedConfigPath,
    diagnose_open_failure,
)

__all__ = ["config_error_message", "describe_config_source"]

_SOURCE_DESCRIPTIONS = {
    "--config": "via -c/--config",
    CONFIG_PATH_ENV: f"via ${CONFIG_PATH_ENV}",
    "default": "default path",
}


def describe_config_source(resolved: ResolvedConfigPath) -> str:
    """One human-readable line naming the config database and *why* that
    file was chosen, e.g.::

        /var/lib/zfsbackup/config.db (via -c/--config)

    Leads with `ResolvedConfigPath.path` -- the path as the operator
    actually spelled it -- never `resolved_path` alone. The resolved form
    is appended only when it differs *and* the path came from `-c` or
    `$ZFSBACKUP_CONFIG`, where the operator supplied something that may be
    relative or a symlink and genuinely benefits from being told which
    file was opened.

    For `source="default"` the resolved form is deliberately suppressed,
    matching `paths._not_found_message`: the default is a compiled-in
    constant nobody typed, and on macOS resolving it rewrites
    `/var/lib/zfsbackup/config.db` to `/private/var/lib/zfsbackup/config.db`
    -- a path that differs across platforms for an input the operator
    never chose.
    """
    where = _SOURCE_DESCRIPTIONS.get(resolved.source, resolved.source)
    shown = str(resolved.path)
    real = str(resolved.resolved_path)
    if resolved.source != "default" and shown != real:
        return f"{shown} -> {real} ({where})"
    return f"{shown} ({where})"


def _first_line(value: object) -> str:
    lines = str(value).strip().splitlines()
    return lines[0] if lines else ""


def config_error_message(
    resolved: Optional[ResolvedConfigPath], exc: Exception
) -> str:
    """The single translation policy from a config-load failure to the one
    line an operator should see.

    `ConfigPathError` and `SchemaError` messages are already operator-grade
    -- they name the file, the three candidate sources, and in
    `ConfigDbIsYaml`'s case a copy-pasteable `zfsbackup-config import
    <path>` command. They are returned **verbatim** (every `SchemaError`
    subclass, `SchemaOutOfDate` and `NotAConfigDatabase` included; the
    daemon appends its own "start the daemon again" sentence to the former
    itself). Prefixing them with something like "Failed to load
    configuration: " would wrap a multi-line message that ends in a command
    the operator is meant to paste, which is exactly how that command stops
    being pasteable. `ConfigConflict` / `ConfigBusy` are likewise already
    complete sentences.

    A plain `DatabaseError` (exactly that class -- SQLAlchemy's subclasses
    are statements somebody built wrongly, not a bad file) means the FILE is
    bad: "file is not a database", "database disk image is malformed". It
    gets one clean line from SQLite's own words, not the SQL statement and
    the sqlalche.me link `str(exc)` carries, and says what to do.

    An `OperationalError` means the preflight passed but the real open
    still failed -- routine when the daemon runs as root, since
    `os.access` is uid-based, blind to ACLs, and permissive for root. It
    goes through `diagnose_open_failure` with the same
    `require_writable=True` the open used (forwarding a different value
    would produce a confidently wrong diagnosis of an access mode the open
    never requested). If the re-run preflight finds nothing wrong, the
    SQLite error is reported as-is rather than guessed at -- worded "Could
    not open" only when SQLite itself reported `unable to open database`,
    and "Failed to read" for any other `OperationalError` (e.g. `no such
    table`, `database is locked`).

    `resolved` is `Optional` because `resolve_config_path` can itself raise
    a `ConfigPathError` (a symlink loop, an unlinked cwd) before any
    `ResolvedConfigPath` exists -- and that is exactly the branch that
    needs none of it.
    """
    if isinstance(exc, (ConfigPathError, SchemaError, ConfigConflict, ConfigBusy)):
        return str(exc)

    where = (
        f" ({describe_config_source(resolved)})" if resolved is not None else ""
    )

    if type(exc) is DatabaseError:
        reason = _first_line(getattr(exc, "orig", None) or exc) or type(exc).__name__
        subject = (
            "the config database"
            if resolved is None
            else f"the config database {describe_config_source(resolved)}"
        )
        return (
            f"{subject} is damaged or not an SQLite database: {reason}; "
            "restore it from a backup."
        )

    if isinstance(exc, OperationalError) and resolved is not None:
        diagnosed = diagnose_open_failure(resolved, exc, require_writable=True)
        if diagnosed is not None:
            return str(diagnosed)
        # "Could not open" is only true when SQLite said so. Anything else
        # (`no such table`, `database is locked`, a read-only database) means
        # the file opened fine and the *read* failed; mislabelling it sent
        # operators hunting for a permissions problem that did not exist.
        reason = str(getattr(exc, "orig", None) or exc)
        if "unable to open database" in reason:
            return f"Could not open the config database{where}: {exc}"
        return f"Failed to read the config database{where}: {exc}"

    return f"Failed to load configuration{where}: {exc}"

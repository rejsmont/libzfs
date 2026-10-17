"""The one place `zfsbackup-config` turns an exception into a message and an
exit code.

Exit codes (docs/config_db_cli_plan.md, item 10.2):

====  =============================================================
0     success
1     invalid input or a refused operation (including `migrate
      --check` finding the schema behind)
2     usage error (bad flags or arguments; Click reports these itself)
75    conflict or busy -- retryable (`EX_TEMPFAIL`)
78    the config database is unusable (`EX_CONFIG`)
====  =============================================================

`78` is the same code the daemon's workers use for a configuration database
they cannot read (`zfsbackup.runtime_config.EX_CONFIG`), so one number means
"the DB, not your input" across both tools.

Messages from `ConfigPathError`, `SchemaError` and `OperationalError` already
are operator-grade (they name the file, the source of the path, and the next
command to run) and are shown verbatim through the daemon's own
`config_error_message`, so both tools word the same failure the same way.
Notes attached with `add_note` -- the store uses them for "the cleanup was
skipped" and "a backup was already written" -- are printed after the message:
they are often the part that says what state the filesystem was left in.
"""

import sys
import traceback
from typing import List, Optional, TextIO

import click
import yaml
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from zfsbackup.config.store import (
    ConfigDbCreatedConcurrently,
    ConfigPathError,
    OutgoingConfigUnreadable,
    ResolvedConfigPath,
    SchemaError,
)
from zfsbackup.runtime_config import EX_CONFIG, config_error_message

__all__ = [
    "EXIT_OK",
    "EXIT_FAILURE",
    "EXIT_USAGE",
    "EXIT_BUSY",
    "EXIT_CONFIG",
    "CliError",
    "BackupFailed",
    "exit_code_for",
    "format_error",
    "report",
]

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_BUSY = 75
EXIT_CONFIG = EX_CONFIG  # 78

_LOCKED_MARKERS = ("database is locked", "database table is locked", "busy")


class CliError(Exception):
    """An error the CLI itself raises, with a message fit to print as is."""

    exit_code = EXIT_FAILURE


class BackupFailed(CliError):
    """The pre-import backup of the outgoing configuration could not be
    written. The import is rolled back; nothing was replaced."""


def _is_locked(exc: OperationalError) -> bool:
    reason = str(getattr(exc, "orig", None) or exc).lower()
    return any(marker in reason for marker in _LOCKED_MARKERS)


def exit_code_for(exc: BaseException) -> int:
    """The exit code `exc` maps to."""
    if isinstance(exc, CliError):
        return exc.exit_code
    # Someone else holds the file or is creating it: retry, do not give up.
    if isinstance(exc, ConfigDbCreatedConcurrently):
        return EXIT_BUSY
    # An outgoing config that cannot be read is a refusal the operator can
    # lift (--no-backup); it is not "the database is unusable".
    if isinstance(exc, OutgoingConfigUnreadable):
        return EXIT_FAILURE
    if isinstance(exc, (ConfigPathError, SchemaError)):
        return EXIT_CONFIG
    # A bare RuntimeError is the store's packaging-defect signal (migration
    # scripts that disagree with EXPECTED_HEAD/KNOWN_REVISIONS, or a branched
    # history): the installation is unusable, not the operator's input.
    if type(exc) is RuntimeError:
        return EXIT_CONFIG
    if isinstance(exc, OperationalError):
        return EXIT_BUSY if _is_locked(exc) else EXIT_CONFIG
    if isinstance(exc, click.UsageError):
        return EXIT_USAGE
    # Everything else -- a bad YAML, a value the model refuses, an OSError
    # writing an output file, a constraint the mapper reports -- is the
    # operator's input or a refused operation.
    return EXIT_FAILURE


def format_error(exc: BaseException, resolved: Optional[ResolvedConfigPath]) -> List[str]:
    """The lines to print for `exc`: its message, then its notes."""
    if isinstance(exc, (ConfigPathError, SchemaError, OperationalError)):
        message = config_error_message(resolved, exc)
    elif isinstance(exc, yaml.YAMLError):
        message = f"invalid YAML: {exc}"
    elif isinstance(exc, SQLAlchemyError):
        message = f"database error: {exc}"
    else:
        message = str(exc) or type(exc).__name__
    if isinstance(exc, OperationalError) and exit_code_for(exc) == EXIT_BUSY:
        message += "\nThe database is busy; retry in a moment."
    lines = [message]
    lines.extend(getattr(exc, "__notes__", ()))
    return lines


def report(
    exc: BaseException,
    resolved: Optional[ResolvedConfigPath],
    *,
    verbose: bool = False,
    stream: Optional[TextIO] = None,
) -> int:
    """Print `exc` for a human on `stream` (stderr) and return its exit code.

    No traceback unless `verbose`: this tool's failures are expected and
    explained. A bare `RuntimeError` -- the store's packaging-defect signal,
    deliberately outside `SchemaError` -- is reported as an installation error
    (exit 78). Any exception this module does not know is a bug, not an
    operator problem, so it is labelled as an internal error (with a pointer
    to `-v`) rather than dressed as a config problem.
    """
    out = stream if stream is not None else sys.stderr
    packaging = type(exc) is RuntimeError
    internal = not packaging and not isinstance(
        exc,
        (
            CliError, ConfigPathError, SchemaError, SQLAlchemyError, ValueError,
            TypeError, KeyError, OSError, yaml.YAMLError,
        ),
    )
    lines = format_error(exc, resolved)
    if packaging:
        prefix = "installation error: "
        lines.append(
            "The installed zfsbackup package is inconsistent (its migration "
            "scripts do not match the schema revision it expects). The "
            "database was not touched; reinstall zfsbackup."
        )
    else:
        prefix = "internal error: " if internal else "error: "
    click.echo(prefix + lines[0], file=out)
    for line in lines[1:]:
        click.echo(line, file=out)
    if verbose:
        click.echo("".join(traceback.format_exception(exc)).rstrip(), file=out)
    elif internal:
        click.echo("re-run with -v for a traceback", file=out)
    return exit_code_for(exc)

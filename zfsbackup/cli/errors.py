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

`78` is sysexits.h's `EX_CONFIG`, used here as this CLI's own "the config
database is unusable (the DB, not your input)" code. The daemon does not share
it, and the CLI imports nothing from the daemon.

`ConfigConflict` and `ConfigBusy` (a compare-and-swap that lost, a writer lock
that stayed held) are 75 and are checked FIRST: they are "retry", not "the
database is unusable".

Messages from `ConfigPathError` and `SchemaError` already are operator-grade
(they name the file, the source of the path, and the next command to run) and
are shown verbatim; `OperationalError` and a plain `DatabaseError` (a file that
is not SQLite, a valid header over a corrupt body) go through the store's own
`config_error_message`, so the daemon and this tool word the same failure the
same way (the store's `SchemaOutOfDate` text is neutral; the daemon appends its
own restart sentence, which would be wrong advice here).
Notes attached with `add_note` -- the store uses them for "the cleanup was
skipped" and "a backup was already written" -- are printed after the message:
they are often the part that says what state the filesystem was left in.
"""

import sys
import traceback
from typing import List, Optional, TextIO

import click
import yaml
from sqlalchemy.exc import DatabaseError, IntegrityError, OperationalError, SQLAlchemyError

from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    ConfigDbCreatedConcurrently,
    ConfigPathError,
    MigrationPackagingError,
    OutgoingConfigUnreadable,
    ResolvedConfigPath,
    SchemaError,
    config_error_message,
)

__all__ = [
    "EXIT_OK",
    "EXIT_FAILURE",
    "EXIT_USAGE",
    "EXIT_BUSY",
    "EXIT_CONFIG",
    "CliError",
    "BackupFailed",
    "is_database_unusable",
    "exit_code_for",
    "format_error",
    "report",
]

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_BUSY = 75
EXIT_CONFIG = 78  # sysexits.h EX_CONFIG: the config database is unusable

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


def is_database_unusable(exc: BaseException) -> bool:
    """A `DatabaseError` that says the FILE is bad -- "file is not a database",
    "database disk image is malformed" (a valid header over a corrupt body) --
    as opposed to an `IntegrityError` (a constraint, i.e. the input) or an
    `OperationalError` (busy, no space, cannot open: handled on its own)."""
    # Exactly `DatabaseError`: SQLAlchemy raises its subclasses
    # (`ProgrammingError`, `DataError`, `InternalError`, ...) for statements
    # this tool built wrongly, which is a bug, not a damaged file.
    return type(exc) is DatabaseError


def exit_code_for(exc: BaseException) -> int:
    """The exit code `exc` maps to."""
    if isinstance(exc, CliError):
        return exc.exit_code
    # Before ConfigPathError/SchemaError below: these mean "look again and
    # retry", never "the database is unusable".
    if isinstance(exc, (ConfigConflict, ConfigBusy)):
        return EXIT_BUSY
    # Someone else holds the file or is creating it: retry, do not give up.
    if isinstance(exc, ConfigDbCreatedConcurrently):
        return EXIT_BUSY
    # An outgoing config that cannot be read is a refusal the operator can
    # lift (--no-backup); it is not "the database is unusable".
    if isinstance(exc, OutgoingConfigUnreadable):
        return EXIT_FAILURE
    if isinstance(exc, (ConfigPathError, SchemaError)):
        return EXIT_CONFIG
    # Migration scripts that disagree with EXPECTED_HEAD/KNOWN_REVISIONS, or a
    # branched history: the installation is unusable, not the operator's
    # input. Only this class says so -- a bare RuntimeError is a bug.
    if isinstance(exc, MigrationPackagingError):
        return EXIT_CONFIG
    if isinstance(exc, OperationalError):
        return EXIT_BUSY if _is_locked(exc) else EXIT_CONFIG
    if is_database_unusable(exc):
        return EXIT_CONFIG
    if isinstance(exc, click.UsageError):
        return EXIT_USAGE
    # Everything else -- a bad YAML, a value the model refuses, an OSError
    # writing an output file, a constraint the mapper reports -- is the
    # operator's input or a refused operation.
    return EXIT_FAILURE


def _first_line(value: object) -> str:
    lines = str(value).strip().splitlines()
    return lines[0] if lines else ""


def _strip_sqlalchemy(exc: BaseException) -> str:
    """`str(exc)` without the `[SQL: ...]`, `[parameters: ...]` and
    `(Background on this error at: ...)` parts SQLAlchemy appends."""
    text = str(exc)
    for marker in ("\n[SQL:", " [SQL:", "\n[parameters:", "\n(Background on this error"):
        text = text.split(marker, 1)[0]
    return text


def _is_internal_sqlalchemy(exc: BaseException) -> bool:
    return isinstance(exc, SQLAlchemyError) and not isinstance(
        exc, (IntegrityError, OperationalError)
    ) and not is_database_unusable(exc)


def format_error(exc: BaseException, resolved: Optional[ResolvedConfigPath]) -> List[str]:
    """The lines to print for `exc`: its message, then its notes."""
    if isinstance(exc, ConfigConflict):
        actual = "no configuration" if exc.actual is None else f"generation {exc.actual}"
        return [
            f"config changed underneath you (generation {exc.expected}, now {actual})",
            "Nothing was written. Re-run the command to start from the current configuration.",
            *getattr(exc, "__notes__", ()),
        ]
    if isinstance(exc, ConfigBusy):
        # The store's text already says what is busy and to retry; use it as
        # the one line, and only fall back to a generic one without it.
        return [
            str(exc) or "another write is in progress; retry",
            *getattr(exc, "__notes__", ()),
        ]
    if is_database_unusable(exc) or isinstance(
        exc, (ConfigPathError, SchemaError, OperationalError)
    ):
        # The store's one wording (see its `config_error_message`): verbatim
        # for path/schema errors, one clean line for a damaged file.
        message = config_error_message(resolved, exc)
    elif isinstance(exc, yaml.YAMLError):
        message = f"invalid YAML: {exc}"
    elif isinstance(exc, IntegrityError):
        message = f"database error: {exc}"
    elif isinstance(exc, SQLAlchemyError):
        # Any other SQLAlchemy error (ProgrammingError, DataError,
        # InternalError, ...) is a statement this tool built wrongly: a bug,
        # not an operator problem. One line, no SQL, no documentation link.
        message = f"{type(exc).__name__}: {_first_line(getattr(exc, 'orig', None) or _strip_sqlalchemy(exc))}"
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
    explained. `MigrationPackagingError` -- the store's packaging-defect
    signal, deliberately outside `SchemaError` -- is reported as an
    installation error (exit 78); a bare `RuntimeError` is not. Any exception this module does not know is a bug, not an
    operator problem, so it is labelled as an internal error (with a pointer
    to `-v`) rather than dressed as a config problem.
    """
    out = stream if stream is not None else sys.stderr
    packaging = isinstance(exc, MigrationPackagingError)
    sqlalchemy_bug = _is_internal_sqlalchemy(exc)
    internal = sqlalchemy_bug or (not packaging and not isinstance(
        exc,
        (
            CliError, ConfigPathError, SchemaError, SQLAlchemyError, ValueError,
            TypeError, KeyError, OSError, yaml.YAMLError, ConfigConflict, ConfigBusy,
        ),
    ))
    lines = format_error(exc, resolved)
    if packaging:
        prefix = "installation error: "
        lines.append(
            "The installed zfsbackup package is inconsistent (its migration "
            "scripts do not match the schema revision it expects). Reinstall "
            "zfsbackup."
        )
    else:
        prefix = "internal error: " if internal else "error: "
    if sqlalchemy_bug and not verbose:
        lines[0] += " (re-run with -v for details)"
    click.echo(prefix + lines[0], file=out)
    for line in lines[1:]:
        click.echo(line, file=out)
    if verbose:
        click.echo("".join(traceback.format_exception(exc)).rstrip(), file=out)
    elif internal and not sqlalchemy_bug:
        click.echo("re-run with -v for a traceback", file=out)
    return exit_code_for(exc)

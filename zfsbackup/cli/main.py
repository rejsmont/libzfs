"""`zfsbackup-config` -- the command-line front end to the config database.

This is the only writer of the SQLite config store (the daemon and its workers
only ever read it). It reaches the database exclusively through
`zfsbackup.config.store`'s public API (path resolution, schema migration,
`import_yaml`) and `zfsbackup.runtime_config`'s read-only loader -- never an
ORM model, never a query -- so every schema invariant stays behind the mapper.

Commands (this module is plan item 10.2; `get`/`set`/`edit`/`list`/`rename`
follow in items 11-14 and are deliberately not stubbed here):

- `import YAML`  -- replace the stored configuration with a YAML file.
- `export`       -- print the stored configuration as YAML.
- `migrate`      -- bring the database schema up to this version.

Path resolution is the daemon's: `-c/--config`, then `$ZFSBACKUP_CONFIG`, then
the default path. Only `import` may create a database; every other command
reports a missing one.

`-c` and `-v` are accepted both before the subcommand and after it, because
the store's own "this is a YAML file, import it" message tells the operator to
write `zfsbackup-config import <yaml> -c <db>`, and that must work verbatim.

Errors never traceback at the user: `errors.py` maps every failure to a
message on stderr and an exit code (0 ok, 1 invalid/refused, 2 usage, 75
conflict/busy, 78 config database unusable).
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import click
import yaml

from zfsbackup.cli.errors import BackupFailed, CliError, report
from zfsbackup.cli.render import render_yaml
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    ConfigPathError,
    OutgoingConfigUnreadable,
    ResolvedConfigPath,
    SchemaError,
    SchemaOutOfDate,
    dispose_all,
    import_yaml,
    resolve_config_path,
    upgrade_schema,
)
from zfsbackup.runtime_config import check_config_schema, load_runtime_config

#: Mode of the pre-import backup: same as the database (`CONFIG_DB_MODE`), so
#: the operators who may replace the configuration may read what it replaced.
BACKUP_MODE = 0o660

_LOGGER_NAME = "zfsbackup"

_REPLACE_NOTE = (
    "`import` always replaces the whole stored configuration with the YAML; "
    "a retry replaces whatever is in the database at that time."
)


class _LogHandler(logging.Handler):
    """Prints the library's log records as CLI messages.

    Warnings from `BackupConfig.from_file`, `validate_config`, `load_config`
    and the store all arrive here as `logging` records. Surfacing them through
    one handler is what keeps them in one list and prints each distinct message
    once: `from_file` and `validate_config` are both reached from more than
    one place (the importer parses and validates, and a later phase may
    validate again), and the same text must not be shown twice.
    """

    def __init__(self, level: int) -> None:
        super().__init__(level)
        self.seen: set = set()
        self.drop_prefixes: Tuple[str, ...] = ()

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if self.drop_prefixes and message.startswith(self.drop_prefixes):
            return
        if record.levelno >= logging.WARNING:
            if message in self.seen:
                return
            self.seen.add(message)
            label = "error" if record.levelno >= logging.ERROR else "warning"
        else:
            label = record.levelname.lower()
        click.echo(f"{label}: {message}", err=True)


@dataclass
class State:
    """What the group callback hands to the subcommands (`ctx.obj`)."""

    config_path: Optional[str] = None
    verbose: bool = False
    resolved: Optional[ResolvedConfigPath] = None
    handler: Optional[_LogHandler] = None
    _restore_level: Optional[int] = field(default=None, repr=False)

    def install_logging(self) -> None:
        logger = logging.getLogger(_LOGGER_NAME)
        self._restore_level = logger.level
        self.handler = _LogHandler(logging.WARNING)
        logger.addHandler(self.handler)
        if self.verbose:
            self.set_verbose()

    def remove_logging(self) -> None:
        logger = logging.getLogger(_LOGGER_NAME)
        if self.handler is not None:
            logger.removeHandler(self.handler)
            self.handler = None
        if self._restore_level is not None:
            logger.setLevel(self._restore_level)

    def set_verbose(self) -> None:
        self.verbose = True
        if self.handler is not None:
            self.handler.setLevel(logging.DEBUG)
            logging.getLogger(_LOGGER_NAME).setLevel(logging.DEBUG)

    def resolve(self) -> ResolvedConfigPath:
        """The config database path, by the daemon's order. Cached so that
        error reporting can name the file that was actually chosen."""
        if self.resolved is None:
            self.resolved = resolve_config_path(self.config_path)
        return self.resolved


class _Group(click.Group):
    """Maps every exception a command lets escape to a message and an exit
    code, so no command repeats a try/except and none can leak a traceback."""

    def invoke(self, ctx: click.Context):
        try:
            return super().invoke(ctx)
        except (click.ClickException, click.exceptions.Exit, click.Abort):
            raise
        except KeyboardInterrupt:
            click.echo("interrupted", err=True)
            ctx.exit(130)
        except Exception as exc:  # noqa: BLE001 - this is the catch-all by design
            state = ctx.obj if isinstance(ctx.obj, State) else None
            code = report(
                exc,
                state.resolved if state else None,
                verbose=bool(state and state.verbose),
            )
            ctx.exit(code)


def _common(func: Callable) -> Callable:
    """`-c`/`-v` on a subcommand as well as on the group."""
    func = click.option(
        "-v", "--verbose", is_flag=True, default=False,
        help="Verbose (debug) output.",
    )(func)
    func = click.option(
        "-c", "--config", "config_path", default=None, metavar="DB",
        help="Config database (same as before the subcommand).",
    )(func)
    return func


def _state(config_path: Optional[str], verbose: bool) -> State:
    state = click.get_current_context().find_object(State)
    if config_path:
        state.config_path = config_path
    if verbose:
        state.set_verbose()
    return state


# `type=str`, not `Path`: `str(Path("")) == "."` would turn `-c ""` into "the
# current directory" and defeat `resolve_config_path`'s fallthrough to
# $ZFSBACKUP_CONFIG (same reasoning as the daemon's `-c`).
@click.group(
    cls=_Group,
    context_settings={"help_option_names": ["-h", "--help"]},
    help=(
        "Manage the zfsbackup configuration database.\n\n"
        "Exit codes: 0 ok, 1 invalid input or refused, 2 usage error, "
        "75 conflict or busy (retry), 78 config database unusable."
    ),
)
@click.option(
    "-c", "--config", "config_path", default=None, metavar="DB",
    help=(
        f"Path to the config DATABASE (SQLite). Defaults to ${CONFIG_PATH_ENV} "
        f"if set, otherwise {DEFAULT_CONFIG_DB}."
    ),
)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Verbose (debug) output.")
@click.pass_context
def cli(ctx: click.Context, config_path: Optional[str], verbose: bool) -> None:
    state = State(config_path=config_path, verbose=verbose)
    ctx.obj = state
    state.install_logging()
    # Closing the group's context is the last thing every invocation does,
    # including a failing one: drop this process's engines so the last
    # connection closes now and SQLite removes -wal/-shm while this process
    # still owns them (the daemon does the same at exit), and detach the
    # log handler so repeated in-process invocations do not stack.
    ctx.call_on_close(dispose_all)
    ctx.call_on_close(state.remove_logging)


# --------------------------------------------------------------------------
# import
# --------------------------------------------------------------------------


def _create_exclusive(directory: Path, name_stem: str, suffix: str) -> Tuple[int, Path]:
    """Create `<directory>/<name_stem><suffix>` with `O_EXCL` (and `-N` before
    the suffix if the name is taken, e.g. two imports in the same second)."""
    for attempt in range(1, 100):
        tail = "" if attempt == 1 else f"-{attempt}"
        path = directory / f"{name_stem}{tail}{suffix}"
        try:
            return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, BACKUP_MODE), path
        except FileExistsError:
            continue
    raise FileExistsError(f"no free backup file name for {name_stem}{suffix} in {directory}")


def _backup_writer(resolved: ResolvedConfigPath) -> Callable[[BackupConfig], Path]:
    """The `backup=` callback for `import_yaml`: renders the outgoing
    configuration to `<dbdir>/<dbname>.pre-import-<UTC>.yaml` (`O_EXCL`, 0660)
    and returns the path. `import_yaml` calls it inside the transaction that
    replaces the configuration, so any failure here rolls the import back."""
    database = resolved.resolved_path

    def write_backup(config: BackupConfig) -> Path:
        try:
            text = render_yaml(config)
        except ValueError as exc:
            raise BackupFailed(
                f"cannot write the pre-import backup: {exc}. Nothing was "
                "replaced. Re-run with --no-backup to replace the "
                "configuration without a backup."
            ) from exc
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path: Optional[Path] = None
        try:
            fd, path = _create_exclusive(
                database.parent, f"{database.name}.pre-import-{stamp}", ".yaml"
            )
            # The creation mode is masked by the umask; the file must be
            # exactly group-writable like the database it came from.
            os.fchmod(fd, BACKUP_MODE)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            if path is not None:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            raise BackupFailed(
                f"cannot write the pre-import backup in {database.parent}: {exc}. "
                "Nothing was replaced. Re-run with --no-backup to replace the "
                "configuration without a backup."
            ) from exc
        return path

    return write_backup


# Failures raised before the import has touched anything (a bad YAML, a bad
# path): the "a retry replaces ..." reminder would be noise on those.
_PRE_WRITE_ERRORS = (ValueError, TypeError, yaml.YAMLError, FileNotFoundError, ConfigPathError, SchemaError)


@cli.command(
    "import",
    help=(
        "Replace the stored configuration with the contents of YAML.\n\n"
        "An import is always a FULL replace: every dataset, destination and "
        "setting in the database is discarded and replaced by the file, and a "
        "retry replaces whatever is in the database at that time. It creates "
        "the database if it does not exist. Before replacing an existing "
        "configuration it writes a YAML backup next to the database "
        "(<db>.pre-import-<UTC time>.yaml)."
    ),
)
@click.argument(
    "yaml_file", metavar="YAML",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option(
    "--no-backup", is_flag=True, default=False,
    help=(
        "Do not back up the outgoing configuration. Also the only way to "
        "replace a stored configuration that cannot be read."
    ),
)
@_common
def import_cmd(yaml_file: Path, no_backup: bool, config_path: Optional[str], verbose: bool) -> None:
    state = _state(config_path, verbose)
    resolved = state.resolve()
    existed = _exists(resolved.resolved_path)

    # `import_yaml` logs its own "Replacing existing config store" warning
    # before the result is known; the warning below says the same thing and
    # also names the backup, so only that one is shown.
    if state.handler is not None:
        state.handler.drop_prefixes = ("Replacing existing config store",)

    try:
        result = import_yaml(
            yaml_file,
            resolved,
            # With no callback the outgoing configuration is never read, so
            # --no-backup is also what lets an unreadable one be replaced.
            backup=None if no_backup else _backup_writer(resolved),
        )
    except OutgoingConfigUnreadable as exc:
        exc.add_note(
            "To replace it anyway, discarding it without a backup, re-run "
            "with --no-backup. " + _REPLACE_NOTE
        )
        raise
    except Exception as exc:
        if existed and not isinstance(exc, _PRE_WRITE_ERRORS):
            exc.add_note(_REPLACE_NOTE)
        raise

    where = resolved.resolved_path
    if result.created:
        click.echo(f"Imported {yaml_file} into new database {where}.")
    else:
        if result.backup_path is not None:
            saved = f"The previous configuration was saved to {result.backup_path}."
        elif no_backup:
            saved = "The previous configuration was NOT backed up (--no-backup)."
        else:
            saved = "The database held no configuration, so there was nothing to back up."
        click.echo(
            f"warning: replaced the stored configuration in {where} "
            f"({result.replaced_datasets} dataset(s), "
            f"{result.replaced_destinations} destination(s)) with the contents of "
            f"{yaml_file}; nothing of the previous configuration was kept. {saved}",
            err=True,
        )
        click.echo(f"Imported {yaml_file} into {where}.")
    click.echo(f"Schema revision: {result.head_revision}")


def _exists(path: Path) -> bool:
    try:
        os.stat(path)
        return True
    except OSError:
        return False


# --------------------------------------------------------------------------
# export
# --------------------------------------------------------------------------


@cli.command(
    "export",
    help=(
        "Print the stored configuration as YAML that `import` accepts. "
        "Writes to stdout unless -o is given."
    ),
)
@click.option(
    "-o", "--output", "output", default=None, metavar="FILE",
    help="Write to FILE instead of stdout ('-' means stdout).",
)
@click.option(
    "--force", is_flag=True, default=False,
    help="Overwrite FILE if it already exists.",
)
@_common
def export_cmd(output: Optional[str], force: bool, config_path: Optional[str], verbose: bool) -> None:
    state = _state(config_path, verbose)
    resolved = state.resolve()
    # Render before opening the output, so a failure leaves no half-written
    # or truncated file behind.
    try:
        config = load_runtime_config(resolved)
    except ValueError as exc:
        # `load_config` refuses a stored configuration it cannot make sense of
        # (no datasets, a bad duration row); say which stage failed.
        raise CliError(f"the stored configuration cannot be loaded: {exc}") from exc
    text = render_yaml(config)

    if output is None or output == "-":
        click.echo(text, nl=False)
        return

    flags = os.O_WRONLY | os.O_CREAT | (os.O_TRUNC if force else os.O_EXCL)
    try:
        fd = os.open(output, flags, 0o666)
    except FileExistsError:
        raise CliError(f"{output} already exists; use --force to overwrite it") from None
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    click.echo(f"Exported the configuration in {resolved.resolved_path} to {output}.", err=True)


# --------------------------------------------------------------------------
# migrate
# --------------------------------------------------------------------------


@cli.command(
    "migrate",
    help=(
        "Bring the database schema up to the version this zfsbackup expects. "
        "With --check, only report whether it is current (exit 1 if behind)."
    ),
)
@click.option(
    "--check", is_flag=True, default=False,
    help="Do not change anything; exit 1 if the schema is behind.",
)
@_common
def migrate_cmd(check: bool, config_path: Optional[str], verbose: bool) -> None:
    state = _state(config_path, verbose)
    resolved = state.resolve()

    if check:
        try:
            revision = check_config_schema(resolved)
        except SchemaOutOfDate as exc:
            raise CliError(str(exc)) from exc
        click.echo(f"Schema is up to date (revision {revision}).")
        return

    upgrade = upgrade_schema(resolved)
    if not upgrade.changed:
        click.echo(f"Schema is up to date (revision {upgrade.to_rev}).")
        return
    before = ", ".join(upgrade.from_revs) or "none (no recorded revision)"
    click.echo(f"Migrated the schema of {resolved.resolved_path} from {before} to {upgrade.to_rev}.")
    click.echo(
        "Restart a running zfsbackup daemon: it checks the schema when it "
        "starts and will not pick up the new one until then."
    )


def main(argv: Optional[List[str]] = None) -> None:
    """Console-script entry point (`zfsbackup-config`). Exits the process with
    the command's exit code."""
    cli.main(args=argv, prog_name="zfsbackup-config", standalone_mode=True)


if __name__ == "__main__":  # pragma: no cover
    main()

"""`zfsbackup-config` -- the command-line front end to the config database.

This is the only writer of the SQLite config store (the daemon and its workers
only ever read it). It reaches the database exclusively through
`zfsbackup.config.store`'s public API (path resolution, `read_config`,
`write_config`, schema migration, `import_yaml`) -- never an ORM model, never
a query -- so every schema invariant stays behind the mapper. It imports
nothing of the daemon (`runtime_config`, `daemon`, `workers`, ...) at import
time; the one daemon-lane module it uses is `daemonlock`, to ask the running
daemon to reload after a write and to report on it. The single exception is `rename.py`,
which imports `zfsbackup.backup_manager` (and with it `libzfseasy`) LAZILY, only
inside `rename dataset|destination`, because the ZFS user-property helpers live
there; nothing else loads it, and a host without ZFS can still run every other
command. Every write follows `write.py`'s discipline.

Commands:

- `import YAML`  -- replace the stored configuration with a YAML file.
- `export`       -- print the stored configuration as YAML.
- `migrate`      -- bring the database schema up to this version.
- `check [--json|--prometheus|--nagios] [--output FILE]` -- is the backup healthy?
  (`check.py`; read-only: `zfs list`/`zfs get`, no lock, never signals.)
- `backup DIR [--no-keys]` -- a consistent backup of the database and the TLS
  identities (`backup.py`; no lock, never signals the daemon).
- `list datasets|destinations` -- read-only tables (`listing.py`).
- `get PATH` / `set PATH VALUE` / `unset PATH` -- one setting at a time
  (`dotpath.py`).
- `edit`         -- the whole configuration in `$EDITOR` (`editor.py`);
  `edit dataset|destination NAME` edits one entry.
- `rename dataset|destination OLD NEW` (`rename.py`).
- `status [--wait S] [--json]` -- is the running daemon on the stored
  configuration? (`status.py`; read-only, never signals.)
- `trust token|list|revoke` and `identity show|set-client-id` -- enrollment
  tokens and pinned client certificates (server side) and this host's client
  id and certificates (`trust.py`; only `identity set-client-id` is a config
  write and signals the daemon).
- `enroll DESTINATION` and `identity rotate` -- the client side of trust
  (`enroll.py`): enroll with a token, and replace the client certificate on
  every enrolled server. `enroll` commits up to two config writes (the client
  id, then the destination's url and pin) and signals the daemon once, after
  the last; `identity rotate` signals only when it updates configured paths.

After every committed write the CLI asks the running daemon to reload
(`write.notify_daemon`; global `--reload-wait SECONDS`).

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

import errno
import logging
import os
import signal
import stat
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import click
import yaml

from zfsbackup import daemonlock
from zfsbackup.cli import backup, check as check_view, dotpath, editor, enroll, rename, status as status_view, trust
from zfsbackup.cli.errors import BackupFailed, CliError, RetryLater, is_database_unusable, report
from zfsbackup.cli.listing import render_datasets, render_destinations
from zfsbackup.cli.render import render_yaml
from zfsbackup.cli.write import (
    DEFAULT_RELOAD_WAIT,
    COMMITTED_MARK,
    begin_reload,
    commit_config,
    notify_daemon,
    read_snapshot,
    where_flag,
)
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    ConfigPathError,
    OutgoingConfigUnreadable,
    ResolvedConfigPath,
    SchemaError,
    SchemaOutOfDate,
    check_config_schema,
    check_migration_packaging,
    dispose_all,
    import_yaml,
    read_config,
    resolve_config_path,
    upgrade_schema,
    validate_config,
    writer_lock,
)

#: Mode of the pre-import backup: same as the database (`CONFIG_DB_MODE`), so
#: the operators who may replace the configuration may read what it replaced.
BACKUP_MODE = 0o660

_LOGGER_NAME = "zfsbackup"
#: `libzfseasy` logs the `zfs` commands it runs at DEBUG. Under `-v` they are
#: shown like the other debug records (on stderr, via `_LogHandler`); without
#: `-v` they are dropped. Nothing from it ever reaches stdout.
_ZFS_LOGGER_NAME = "libzfseasy"
_LOGGER_NAMES = (_LOGGER_NAME, _ZFS_LOGGER_NAME)

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
        #: Warning texts not to show: what the stored configuration already
        #: warned about before this command changed anything (see `_edit`).
        self.suppress: set = set()
        #: Show nothing at all (while a baseline is being collected).
        self.muted = False
        #: Drop WARNING records (keep errors): read-only commands do not
        #: comment on the stored configuration.
        self.quiet_warnings = False

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if self.muted:
            return
        if self.quiet_warnings and logging.WARNING <= record.levelno < logging.ERROR:
            return
        if self.drop_prefixes and message.startswith(self.drop_prefixes):
            return
        if record.levelno >= logging.WARNING:
            if message in self.seen or message in self.suppress:
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
    #: `--reload-wait`: seconds `notify_daemon` waits for the daemon's
    #: acknowledgement after a SIGHUP (0 = do not wait).
    reload_wait: float = DEFAULT_RELOAD_WAIT
    #: `migrate` has reached the point where it may change the schema (set
    #: under the writer lock): an interrupt then cannot say whether it did.
    migrating: bool = False
    _restore_levels: Dict[str, int] = field(default_factory=dict, repr=False)

    def install_logging(self) -> None:
        self._restore_levels = {}
        self.handler = _LogHandler(logging.WARNING)
        for name in _LOGGER_NAMES:
            logger = logging.getLogger(name)
            self._restore_levels[name] = logger.level
            logger.addHandler(self.handler)
        if self.verbose:
            self.set_verbose()

    def remove_logging(self) -> None:
        for name in _LOGGER_NAMES:
            logger = logging.getLogger(name)
            if self.handler is not None:
                logger.removeHandler(self.handler)
            if name in self._restore_levels:
                logger.setLevel(self._restore_levels[name])
        self.handler = None

    def set_verbose(self) -> None:
        self.verbose = True
        if self.handler is not None:
            self.handler.setLevel(logging.DEBUG)
            for name in _LOGGER_NAMES:
                logging.getLogger(name).setLevel(logging.DEBUG)

    def quiet(self) -> None:
        """Read-only command: show no warnings about the stored configuration
        (errors are still shown)."""
        if self.handler is not None:
            self.handler.quiet_warnings = True

    def warn(self, message: str) -> None:
        """Print `message` as a warning, once per distinct text per invocation
        (the same dedupe as warnings arriving as log records)."""
        if self.handler is not None:
            if message in self.handler.seen or message in self.handler.suppress:
                return
            self.handler.seen.add(message)
        click.echo(f"warning: {message}", err=True)

    def resolve(self) -> ResolvedConfigPath:
        """The config database path, by the daemon's order. Cached so that
        error reporting can name the file that was actually chosen."""
        if self.resolved is None:
            self.resolved = resolve_config_path(self.config_path)
        return self.resolved


EXIT_SIGINT = 130
EXIT_SIGTERM = 143

_APPLIED_CLAIM = "may or may not have been applied"


class _Terminated(KeyboardInterrupt):
    """SIGTERM, delivered as a `KeyboardInterrupt` so `kill` unwinds exactly as
    ^C does: temp and backup files are removed, the transaction rolls back."""


def _without_applied_claim(note: str) -> str:
    """`note` without its "may or may not have been applied" clause, or "" if
    nothing else is left. The CLI states that verdict itself, once. Only the
    clause goes (from its leading ";" through its closing ")." or end of
    sentence); whatever follows it, such as a "never delete its -wal/-shm"
    warning, is kept."""
    at = note.find(_APPLIED_CLAIM)
    if at < 0:
        return note
    end = note.find(").", at)
    if end >= 0:
        end += 2
    else:
        stop = note.find(".", at)
        end = len(note) if stop < 0 else stop + 1
    tail = note[end:].strip()
    cut = note.rfind(";", 0, at)
    if cut < 0:
        return tail
    head = note[:cut]
    # "...written to X; the import was interrupted; it may or may not ..."
    if "interrupted" not in note[cut:at]:
        before, sep, last = head.rpartition(";")
        if sep and "interrupted" in last:
            head = before
    head = head.rstrip() + "."
    return f"{head} {tail}" if tail else head


class _Group(click.Group):
    """Maps every exception a command lets escape to a message and an exit
    code, so no command repeats a try/except and none can leak a traceback."""

    def invoke(self, ctx: click.Context):
        try:
            return super().invoke(ctx)
        except (click.ClickException, click.exceptions.Exit, click.Abort):
            raise
        except KeyboardInterrupt as exc:
            state = ctx.obj if isinstance(ctx.obj, State) else None
            terminated = isinstance(exc, _Terminated)
            word = "terminated" if terminated else "interrupted"
            notes = list(getattr(exc, "__notes__", ()))
            if getattr(exc, COMMITTED_MARK, False):
                # Interrupted while waiting for the daemon to acknowledge a
                # write that is already durable (`write.notify_daemon`): the
                # exit code (130/143) reflects the interrupt, not the write.
                click.echo(f"{word} (the change is saved)", err=True)
            elif ctx.invoked_subcommand == "import":
                # ^C can land before, inside or just after the commit; the
                # store's notes say what it knows, but only the database says
                # which it was.
                where = where_flag(state.resolved) if state is not None and state.resolved is not None else ""
                click.echo(
                    f"{word} -- the import may or may not have been applied; "
                    f"run `zfsbackup-config{where} export` to check",
                    err=True,
                )
                # Say that once: keep what else the notes know (the backup's
                # path, a skipped cleanup), drop their copy of the verdict.
                notes = [n for n in map(_without_applied_claim, notes) if n]
            elif state is not None and state.migrating:
                # ^C can land before, inside or just after the schema commit.
                where = where_flag(state.resolved) if state.resolved is not None else ""
                click.echo(
                    f"{word} -- the migration may or may not have been applied; "
                    f"run `zfsbackup-config{where} migrate --check` to check",
                    err=True,
                )
            else:
                click.echo(word, err=True)
            for note in notes:
                click.echo(note, err=True)
            ctx.exit(EXIT_SIGTERM if terminated else EXIT_SIGINT)
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


def _check_seconds(ctx, param, value):
    """`--reload-wait` / `--wait`: a finite number of seconds, 0 or more. (`nan`
    would compare false against every deadline and wait forever.) `None` (not
    given) passes through."""
    if value is None:
        return None
    if value != value or value in (float("inf"), float("-inf")) or value < 0:
        raise click.BadParameter("must be a finite number of seconds, 0 or more")
    return value


def _print_version(ctx: click.Context, _param: click.Parameter, value: bool) -> None:
    """Eager `--version`: print the one version line and exit 0, before `-c` is
    looked at, so it needs no database. `zfsbackup.version` is imported here,
    not at module level, so ordinary invocations do not pay for it (its schema
    head import is the heavy part)."""
    if not value or ctx.resilient_parsing:
        return
    from zfsbackup.version import version_string

    click.echo(version_string("zfsbackup-config"))
    ctx.exit(0)


# `type=str`, not `Path`: `str(Path("")) == "."` would turn `-c ""` into "the
# current directory" and defeat `resolve_config_path`'s fallthrough to
# $ZFSBACKUP_CONFIG (same reasoning as the daemon's `-c`).
@click.group(
    cls=_Group,
    context_settings={"help_option_names": ["-h", "--help"]},
    help=(
        "Manage the zfsbackup configuration database.\n\n"
        "Exit codes: 0 ok, 1 invalid input or refused, 2 usage error, "
        "75 conflict or busy (retry), 78 config database unusable.\n\n"
        "After every committed write the running daemon is asked to reload. "
        "--reload-wait is a global option and goes BEFORE the subcommand, "
        "e.g. `zfsbackup-config --reload-wait 10 set check_interval 5m`."
    ),
)
@click.option(
    "--version", is_flag=True, is_eager=True, expose_value=False,
    callback=_print_version,
    help="Show the version, wire protocol and config schema, and exit.",
)
@click.option(
    "-c", "--config", "config_path", default=None, metavar="DB",
    help=(
        f"Path to the config DATABASE (SQLite). Defaults to ${CONFIG_PATH_ENV} "
        f"if set, otherwise {DEFAULT_CONFIG_DB}."
    ),
)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Verbose (debug) output.")
@click.option(
    "--reload-wait", "reload_wait", type=float, default=DEFAULT_RELOAD_WAIT,
    show_default=True, callback=_check_seconds, metavar="SECONDS",
    help=(
        "Global option: goes before the subcommand. After a write that "
        "signalled the running daemon to reload, wait up to SECONDS for it to "
        "acknowledge (0 = do not wait). It never changes the exit code."
    ),
)
@click.pass_context
def cli(
    ctx: click.Context, config_path: Optional[str], verbose: bool, reload_wait: float
) -> None:
    state = State(config_path=config_path, verbose=verbose, reload_wait=reload_wait)
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


# A directory fsync some filesystems cannot do (FUSE, CIFS, vboxsf mounts):
# the entry is as durable as that filesystem makes it, so carry on.
_FSYNC_UNSUPPORTED = (errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP)


def _fsync_directory(directory) -> None:
    """Flush `directory`'s entries (a created or renamed file) to disk."""
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        try:
            os.fsync(dir_fd)
        except OSError as exc:
            if exc.errno not in _FSYNC_UNSUPPORTED:
                raise
            logging.getLogger(_LOGGER_NAME).debug(
                "cannot fsync directory %s (%s); continuing", directory, exc.strerror
            )
    finally:
        os.close(dir_fd)


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
        fd: Optional[int] = None
        path: Optional[Path] = None
        try:
            fd, path = _create_exclusive(
                database.parent, f"{database.name}.pre-import-{stamp}", ".yaml"
            )
            # The creation mode is masked by the umask; the file must be
            # exactly group-writable like the database it came from.
            os.fchmod(fd, BACKUP_MODE)
            # Clear `fd` BEFORE `fdopen`: once the file object exists it owns
            # the descriptor, and a ^C between the two statements must not
            # close that number a second time (it may be reused by then).
            raw, fd = fd, None
            handle = os.fdopen(raw, "w", encoding="utf-8")
            with handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            # The file is durable; make its directory entry durable too, or a
            # crash could lose the only copy of the outgoing configuration
            # after the import has gone on to replace it.
            _fsync_directory(database.parent)
        except BaseException as exc:
            # Includes KeyboardInterrupt: a half-written "backup" must never
            # be left looking like one.
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if path is not None:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            if isinstance(exc, OSError):
                raise BackupFailed(
                    f"cannot write the pre-import backup in {database.parent}: {exc}. "
                    "Nothing was replaced. Re-run with --no-backup to replace the "
                    "configuration without a backup."
                ) from exc
            raise
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
    # also names the backup, so only that one is shown. Likewise a skipped
    # first-import cleanup is logged ("Failed first import: ...") AND attached
    # to the exception as a note, which `errors.report` prints; show it once.
    if state.handler is not None:
        state.handler.drop_prefixes = (
            "Replacing existing config store",
            "Failed first import",
        )

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
        if existed and not isinstance(exc, _PRE_WRITE_ERRORS) and not is_database_unusable(exc):
            exc.add_note(_REPLACE_NOTE)
        raise

    # The import is committed: ask the daemon to reload BEFORE printing
    # anything, so a closed stdout cannot cost the signal.
    request = begin_reload(resolved, result.generation, result.store_id)
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
    notify_daemon(resolved, result.generation, result.store_id, request=request)


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
    help=(
        "Overwrite FILE if it already exists. A regular file is replaced, not "
        "rewritten (other hard links to it keep the old content), which needs "
        "a writable directory; a FIFO or device is written to in place."
    ),
)
@_common
def export_cmd(output: Optional[str], force: bool, config_path: Optional[str], verbose: bool) -> None:
    state = _state(config_path, verbose)
    resolved = state.resolve()
    # Render before opening the output, so a failure leaves no half-written
    # or truncated file behind.
    # `read_snapshot` is the store's `read_config` (one read transaction, the
    # schema checked inside it); it says which stage failed when `load_config`
    # refuses a stored configuration (no datasets, a bad duration row).
    text = render_yaml(read_snapshot(resolved).config)

    if output is None or output == "-":
        click.echo(text, nl=False)
        return

    try:
        _write_output(output, text, force)
    except OSError as exc:
        # `exc` may name the temp file; the operator asked for `output`.
        raise CliError(
            f"cannot write {output}: {exc.strerror or exc}. "
            "Any existing file was left untouched."
        ) from exc
    click.echo(f"Exported the configuration in {resolved.resolved_path} to {output}.", err=True)


def _write_output(output: str, text: str, force: bool) -> None:
    """Write `text` to `output` through a temp file in the same directory and
    rename it into place, so a failure (disk full, ^C) leaves an existing
    target exactly as it was and never a truncated one.

    Without `force` an existing `output` is refused (nothing is clobbered even
    if it appears while this runs: the final step is a hard link, which fails
    if the name exists). With `force` a regular file is replaced; a symlink is
    followed, as the open-and-truncate this replaces did, and the existing
    file's permission bits (and, where allowed, owner) are kept. Anything that
    is not a regular file -- a FIFO, a device such as /dev/null -- is never
    renamed over: it is opened and written in place.
    """
    target = os.path.realpath(output) if force else os.path.abspath(output)
    directory = os.path.dirname(target)
    existing: Optional[os.stat_result] = None
    if force:
        try:
            existing = os.stat(target)
        except OSError:
            existing = None
    elif os.path.lexists(output):
        raise CliError(f"{output} already exists; use --force to overwrite it")

    if existing is not None and not stat.S_ISREG(existing.st_mode):
        _write_in_place(output, target, text)
        return

    fd, temp = tempfile.mkstemp(dir=directory, prefix=".zfsbackup-export.")
    try:
        if existing is not None:
            # chown first: it may clear set-id bits that the chmod then sets.
            try:
                os.fchown(fd, existing.st_uid, existing.st_gid)
            except PermissionError:
                pass  # not ours to give away; the replacement is ours
            os.fchmod(fd, existing.st_mode & 0o7777)
        else:
            # New files get the usual 0666-minus-umask, as `open()` would.
            umask = os.umask(0)
            try:  # restore the umask even if a signal unwinds us here
                os.fchmod(fd, 0o666 & ~umask)
            finally:
                os.umask(umask)
        # Clear `fd` BEFORE `fdopen` (see `_backup_writer`).
        raw, fd = fd, None
        handle = os.fdopen(raw, "w", encoding="utf-8")
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        if force:
            os.replace(temp, target)
        else:
            try:
                os.link(temp, target)
            except FileExistsError:
                raise CliError(f"{output} already exists; use --force to overwrite it") from None
            except OSError:
                # No hard links here (some network or FAT filesystems): fall
                # back to rename, accepting the narrow check-then-rename race.
                if os.path.lexists(target):
                    raise CliError(f"{output} already exists; use --force to overwrite it") from None
                os.replace(temp, target)
            else:
                os.unlink(temp)
        temp = None
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if temp is not None:
            try:
                os.unlink(temp)
            except OSError:
                pass
    try:
        _fsync_directory(directory)
    except OSError as exc:
        raise CliError(
            f"{output} was written, but flushing its directory failed: "
            f"{exc.strerror or exc}. It may not survive a crash."
        ) from exc


def _write_in_place(output: str, target: str, text: str) -> None:
    """`--force` onto something that is not a regular file: open it and write,
    with no temp file and no rename -- replacing a FIFO or /dev/null with a
    regular file would silently break whatever reads or relies on it."""
    try:
        # No O_CREAT, no O_TRUNC: it exists, and truncation means nothing
        # to a pipe or a device.
        fd = os.open(target, os.O_WRONLY)
        raw, fd = fd, None
        with os.fdopen(raw, "w", encoding="utf-8") as handle:
            handle.write(text)
    except OSError as exc:
        raise CliError(f"cannot write {output}: {exc.strerror or exc}.") from exc


# --------------------------------------------------------------------------
# migrate
# --------------------------------------------------------------------------


@cli.command(
    "migrate",
    help=(
        "Bring the database schema up to the version this zfsbackup expects. "
        "With --check, only report whether it is current (exit 1 if behind). "
        "A schema that is behind is only migrated with the daemon stopped: "
        "refused (exit 75) while a daemon or any of its workers run. "
        "With --backup-dir DIR, a schema that is behind is first backed up "
        "(see there)."
    ),
)
@click.option(
    "--check", is_flag=True, default=False,
    help="Do not change anything; exit 1 if the schema is behind.",
)
@click.option(
    "--backup-dir", "backup_dir", default=None, metavar="DIR",
    type=click.Path(file_okay=False, path_type=Path),
    help=(
        "Only when the schema is behind: first write a copy of the database "
        "to DIR/config.db.pre-<revision>-<UTC time> (never overwritten; DIR is "
        "created if missing), and migrate only if that succeeded (exit 1 "
        "otherwise). An identical backup already in DIR (same revision, store id "
        "and generation) is kept instead of writing another, so a start-up retry "
        "loop cannot fill the disk. Nothing is written when the schema is "
        "current. Does not back up the TLS keys (a migration does not touch them)."
    ),
)
@_common
def migrate_cmd(
    check: bool, backup_dir: Optional[Path], config_path: Optional[str], verbose: bool
) -> None:
    state = _state(config_path, verbose)
    resolved = state.resolve()

    if check:
        try:
            revision = check_config_schema(resolved)
        except SchemaOutOfDate as exc:
            # One line, this command's own verdict. The store's text already
            # names the database, both revisions and the remedy (`migrate`,
            # or `import` for a file with no recorded revision); the
            # daemon's "start the daemon again" sentence is deliberately not
            # part of it.
            raise CliError(f"the schema is not up to date: {exc}") from exc
        click.echo(f"Schema is up to date (revision {revision}).")
        return

    # Refuse a database that is not ours BEFORE the writer lock puts a new file
    # next to it (10.4: never touch a foreign database). Only "behind" (a
    # recorded, older revision) goes on to the upgrade; every other schema
    # error propagates -- including a SchemaOutOfDate with no recorded
    # revision (a tableless file: migrate cannot help, `import` can). Not a substitute for the check under the lock:
    # `upgrade_schema` repeats it inside its own transaction.
    try:
        current = check_config_schema(resolved)
    except SchemaOutOfDate as exc:
        if not exc.recorded_revisions:
            raise
    else:
        if backup_dir is not None:
            # Current: nothing to back up and nothing to migrate. No lock and
            # no file written -- the unit runs this on every start.
            click.echo(f"Schema is up to date (revision {current}).")
            return

    # Serialise with `import` and every other write: the same writer lock they
    # take (a concurrent migrate that won the race reports `changed=False`).
    # `check_config_schema` above is also the preflight, so a missing database
    # leaves no lock file behind. `ConfigBusy` -> exit 75.
    with writer_lock(resolved):
        _guard_and_backup_if_behind(resolved, backup_dir)
        # Only from here can the schema change: an interrupt after this point
        # cannot say whether it did (`_Group.invoke`); before it, nothing was.
        state.migrating = True
        upgrade = upgrade_schema(resolved)
    if not upgrade.changed:
        click.echo(f"Schema is up to date (revision {upgrade.to_rev}).")
        return
    before = ", ".join(upgrade.from_revs)
    click.echo(f"Migrated the schema of {resolved.resolved_path} from {before} to {upgrade.to_rev}.")
    click.echo(
        "Start the daemon to run this schema (a daemon refuses to start on an older "
        "one, and `migrate` refuses while one runs, so none is running now)."
    )


def _guard_and_backup_if_behind(resolved: ResolvedConfigPath, backup_dir: Optional[Path]) -> None:
    """`migrate`, under the writer lock: if the schema is (still) behind,
    refuse while a daemon or any worker runs (75), then -- with `--backup-dir`
    -- write the pre-migration backup. Any failure raises and the caller never
    migrates. A schema that a concurrent migrate brought to head meanwhile
    needs neither. Every other schema error (a NEWER database, an unknown
    history) propagates exactly as before, with no backup.

    With `--backup-dir` the migration scripts are checked against the schema
    constants FIRST (`check_migration_packaging`): a packaging defect fails
    `upgrade_schema` every time, and a backup written before it would be
    written again on every retry."""
    if backup_dir is not None:
        check_migration_packaging()
    try:
        check_config_schema(resolved)
        return
    except SchemaOutOfDate as exc:
        if not exc.recorded_revisions:
            raise
        from_revs = exc.recorded_revisions
    # Old code must not run against a new schema, so nothing may be running at
    # all -- not a previous daemon's survivors (a `KillMode=process` stop), and
    # not the running daemon or its workers either: `migrate` needs the daemon
    # stopped. The presence lock covers a live supervisor between worker
    # restarts or before its first worker; the workers lock covers the workers.
    # A lock that cannot be examined raises (`DaemonLockError`, exit 1): not
    # knowing is not permission.
    if daemonlock.probe(resolved) is not None or daemonlock.workers_present(resolved):
        raise RetryLater(
            "the schema is behind and a zfsbackup daemon or its workers are running; "
            "refusing to migrate the schema under them (a migration needs the daemon "
            "stopped, including its own workers). Stop the daemon (or wait for the "
            "workers to exit) and run again."
        )
    if backup_dir is None:
        return
    saved = backup.backup_before_migrate(resolved, backup_dir, from_revs)
    if saved.reused:
        click.echo(f"an identical pre-migration backup already exists: {saved.path}")
    else:
        click.echo(
            f"Backed up {resolved.resolved_path} (schema {saved.schema_rev or 'none'}, "
            f"generation {saved.generation}) to {saved.path}."
        )


# --------------------------------------------------------------------------
# status
# --------------------------------------------------------------------------


_JSON_OPTION_STATUS = click.option(
    "--json", "as_json", is_flag=True, default=False,
    help="Machine-readable JSON (stable shape; see zfsbackup/cli/status.py).",
)


@cli.command(
    "status",
    help=(
        "Is the running daemon on the stored configuration? Prints the stored "
        "key (store id and generation), the daemon's pid, what it has applied, "
        "is applying (target), rejected or deferred (refused for now, re-checked "
        "every poll), whether it supports live reload, "
        "and a verdict. Read-only: it never signals the daemon. Exit 0; 1 if "
        "the daemon's lock file cannot be read; 78 if the database is unusable "
        "or its schema is out of date (the daemon state is printed first). With "
        "--wait SECONDS, wait for the daemon to catch up: exit 0 once it is in "
        "sync (or if no daemon is running), 1 if it rejected the stored "
        "configuration or can never catch up (a daemon that predates live "
        "reload, or whose supervisor is gone), 75 if the time ran out (also while "
        "the daemon is still deferred)."
    ),
)
@click.option(
    "--wait", "wait", type=float, default=None, metavar="SECONDS",
    callback=_check_seconds,
    help="Wait up to SECONDS for the daemon to be in sync; the exit code reports the outcome.",
)
@_JSON_OPTION_STATUS
@_common
def status_cmd(
    wait: Optional[float], as_json: bool, config_path: Optional[str], verbose: bool
) -> None:
    state = _state(config_path, verbose)
    state.quiet()
    resolved = state.resolve()
    if wait is None:
        current = status_view.collect(resolved)
    else:
        current = status_view.wait_for(resolved, wait)
    # The workers' last outcomes (S-7b): read once, after any wait, and never
    # part of the verdict or the exit code.
    workers = status_view.collect_workers(resolved, daemon_running=current.daemon is not None)
    if as_json:
        click.echo(status_view.render_json(current, workers), nl=False)
    else:
        click.echo(status_view.render_text(current, workers), nl=False)
    if current.schema_error is not None:
        # What could be said has been; now fail like any unusable database
        # (exit 78, the store's own wording).
        raise current.schema_error
    if wait is None:
        return
    code = status_view.wait_exit_code(current)
    if code == 75:
        extra = (
            "" if "daemon log" in current.message or "after the restart" in current.message
            else "; if it does not catch up, see the daemon log"
        )
        click.echo(
            f"error: the daemon is not yet in sync after {wait:g}s: {current.message}{extra}",
            err=True,
        )
    elif current.verdict == status_view.REJECTED:
        click.echo(
            "error: the daemon rejected the stored configuration and keeps "
            "running an older one; see its log",
            err=True,
        )
    elif current.will_not_catch_up:
        if current.verdict == status_view.SUPERVISOR_GONE:
            click.echo(
                "error: the daemon's supervisor is gone, so nothing will pick up the "
                "stored configuration until the daemon is restarted",
                err=True,
            )
        else:
            click.echo(
                "error: the running daemon predates live reload and will not pick up "
                "the stored configuration; restart it",
                err=True,
            )
    if code:
        click.get_current_context().exit(code)


# --------------------------------------------------------------------------
# list
# --------------------------------------------------------------------------


@cli.group(
    "list",
    help="Show the stored datasets or destinations (read-only).",
)
@_common
def list_group(config_path: Optional[str], verbose: bool) -> None:
    _state(config_path, verbose)


_JSON_OPTION = click.option(
    "--json", "as_json", is_flag=True, default=False,
    help="Machine-readable JSON (stable shape; see zfsbackup/cli/listing.py).",
)


@list_group.command(
    "datasets",
    help=(
        "One row per dataset, in stored order: name, enabled, frequency, "
        "recursive, RETENTION (the number of rules, or `never` for a dataset "
        "that is never pruned) and its remote destinations (`inherit (X)` = the "
        "dataset's own frequency X)."
    ),
)
@_JSON_OPTION
@click.option("--enabled-only", is_flag=True, default=False, help="Only enabled datasets.")
@_common
def list_datasets_cmd(
    as_json: bool, enabled_only: bool, config_path: Optional[str], verbose: bool
) -> None:
    state = _state(config_path, verbose)
    state.quiet()
    config = read_snapshot(state.resolve()).config
    click.echo(render_datasets(config, enabled_only=enabled_only, as_json=as_json), nl=False)


@list_group.command(
    "destinations",
    help="One row per destination, by name: its URL and the datasets that send to it.",
)
@_JSON_OPTION
@click.option(
    "--enabled-only", is_flag=True, default=False,
    help=(
        "Count only enabled datasets as senders: a destination used only by "
        "disabled datasets (or by none) is hidden."
    ),
)
@_common
def list_destinations_cmd(
    as_json: bool, enabled_only: bool, config_path: Optional[str], verbose: bool
) -> None:
    state = _state(config_path, verbose)
    state.quiet()
    config = read_snapshot(state.resolve()).config
    click.echo(render_destinations(config, enabled_only=enabled_only, as_json=as_json), nl=False)


# --------------------------------------------------------------------------
# get / set / unset
# --------------------------------------------------------------------------

_PATH_HELP = (
    "PATH is a setting such as `check_interval`, `datasets.tank/data.frequency`, "
    "`datasets.tank/data.retention.1d`, `datasets.tank/data.remote.offsite.frequency`, "
    "`destinations.offsite.url`, `destinations.offsite.server_fingerprint` or `remote_backup.enabled`. A name containing dots "
    "is matched against the names that exist; the bracket form "
    "`datasets[tank/vm.01].frequency` is always accepted (quote it in the shell) and "
    "is the form this tool prints. An ambiguous path is an error."
)


@cli.command(
    "get",
    help=f"Print one setting as stored (a duration as it was written). {_PATH_HELP}",
)
@click.argument("path", metavar="PATH")
@_common
def get_cmd(path: str, config_path: Optional[str], verbose: bool) -> None:
    state = _state(config_path, verbose)
    state.quiet()
    config = read_snapshot(state.resolve()).config
    segs = dotpath.resolve(path, config)
    for line in dotpath.get_lines(config, segs):
        click.echo(line)


class _CollectWarnings(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.messages: set = set()

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.add(record.getMessage())


def _read_with_baseline(state: State, resolved: ResolvedConfigPath):
    """`read_snapshot`, plus the set of warnings the stored configuration
    already carries (a `forever` dataset, a prefix with `_`, ...), collected
    without printing them, and installed on the log handler as "do not show".

    `set` and `unset` re-parse and re-validate the WHOLE configuration, so
    without this every edit would repeat the warnings of settings it did not
    touch; only the warnings the edit introduces are shown. A baseline that
    cannot be built is smaller (more is shown), never an error."""
    logger = logging.getLogger(_LOGGER_NAME)
    collector = _CollectWarnings()
    if state.handler is not None:
        state.handler.muted = True
    logger.addHandler(collector)
    try:
        snapshot = read_snapshot(resolved)
        baseline = set(collector.messages)
        try:
            baseline |= set(validate_config(snapshot.config))
            dotpath.parse_dict(dotpath.to_dict(snapshot.config))
        except Exception:  # noqa: BLE001
            pass
        baseline |= collector.messages
    finally:
        logger.removeHandler(collector)
        if state.handler is not None:
            state.handler.muted = False
    if state.handler is not None:
        state.handler.suppress = baseline
    return snapshot


def _edit(
    state: State,
    path: str,
    apply: Callable[[BackupConfig, dotpath.Segs], "dotpath.Change"],
) -> None:
    """The shared flow of `set` and `unset`: snapshot, resolve, change in
    memory, validate, compare-and-swap write. The database is opened for
    reading only until `commit_config`, and nothing is written when the path,
    the value or the resulting configuration is refused."""
    resolved = state.resolve()
    snapshot = _read_with_baseline(state, resolved)
    config = snapshot.config
    segs = dotpath.resolve(path, config)
    change = apply(config, segs)
    for message in change.warnings:
        state.warn(message)

    shown = change.scope or segs
    if render_yaml(change.config) == render_yaml(config):
        current = dotpath.compact(config, shown)
        state_text = "not set" if current == dotpath.NOT_SET else f"already {current}"
        click.echo(f"{dotpath.format_path(shown)} is {state_text}; nothing changed")
        return

    result = commit_config(resolved, snapshot, change.config, warn=state.warn, announce=False)
    # Request the reload BEFORE printing anything: a closed stdout must not
    # cost the signal.
    request = begin_reload(resolved, result.generation, result.store_id)
    click.echo(
        f"{dotpath.format_path(shown)}: {dotpath.compact(config, shown)} -> "
        f"{dotpath.compact(change.config, shown)}"
    )
    notify_daemon(resolved, result.generation, result.store_id, request=request)


@cli.command(
    "set",
    help=(
        "Set one setting. VALUE is read as YAML, exactly as in a config file "
        "(`true`, `5m`, `8080`, `forever`; quote text that YAML would read as "
        "something else, e.g. '\"123\"'; `''` is an empty value). A duration is "
        "stored as typed. The whole resulting configuration is validated before "
        f"anything is written. {_PATH_HELP}"
    ),
)
@click.argument("path", metavar="PATH")
@click.argument("value", metavar="VALUE")
@_common
def set_cmd(path: str, value: str, config_path: Optional[str], verbose: bool) -> None:
    state = _state(config_path, verbose)
    _edit(state, path, lambda config, segs: dotpath.set_value(config, segs, value))


@cli.command(
    "unset",
    help=(
        "Revert one setting: prune_interval goes back to deriving its value; "
        "client_cert_file and client_key_file (unset together) go back to the generated "
        "client identity; a destination's server_fingerprint is removed (it is no longer "
        "enrolled); datasets.N.retention to the default 1d->30d; "
        "retention.AGE removes that rule (the last one leaves the dataset "
        "never pruned); a destination's remote frequency or retention goes "
        "back to inheriting the dataset's; `remote_backup` removes the receive "
        f"role. {_PATH_HELP}"
    ),
)
@click.argument("path", metavar="PATH")
@_common
def unset_cmd(path: str, config_path: Optional[str], verbose: bool) -> None:
    state = _state(config_path, verbose)
    _edit(state, path, dotpath.unset_value)


# --------------------------------------------------------------------------
# edit
# --------------------------------------------------------------------------


_EDIT_META = "zfsbackup.edit"

_EDIT_OPTIONS = (
    click.option(
        "--editor", "editor_cmd", default=None, metavar="CMD",
        help="Editor command (overrides $VISUAL and $EDITOR); split like a shell would.",
    ),
    click.option(
        "--no-prompt", is_flag=True, default=False,
        help=(
            "Never prompt and do not require a terminal (for scripts and tests): "
            "a rejected buffer abandons the edit, exit 1, and is kept in a file."
        ),
    ),
)


def _edit_options(func: Callable) -> Callable:
    for option in reversed(_EDIT_OPTIONS):
        func = option(func)
    return func


@cli.group(
    "edit",
    invoke_without_command=True,
    help=(
        "Edit the configuration in $VISUAL / $EDITOR (default vi). Without a "
        "subcommand, the whole configuration is rendered as YAML (as `export` "
        "prints it); `edit dataset NAME` and `edit destination NAME` edit one "
        "entry, whose buffer has no name. When you save, the result is "
        "validated and written, but only if nobody else changed the "
        "configuration meanwhile (otherwise exit 75 and your edit is kept in a "
        "file). Saving a buffer with no real change writes nothing. A rejected "
        "buffer can be retried or abandoned. Dataset and destination names are "
        "identity: in the whole-config buffer a new name creates one and a "
        "deleted name deletes one, and a diff that looks like a rename is "
        "refused (use `rename`, or --allow-recreate). Needs a terminal unless "
        "--no-prompt is given."
    ),
)
@click.option(
    "--allow-recreate", is_flag=True, default=False,
    help="Whole-config edit: accept a rename-shaped edit as the delete + create it is.",
)
@_edit_options
@_common
@click.pass_context
def edit_group(
    ctx: click.Context, allow_recreate: bool, editor_cmd: Optional[str], no_prompt: bool,
    config_path: Optional[str], verbose: bool,
) -> None:
    state = _state(config_path, verbose)
    if ctx.invoked_subcommand is not None:
        if allow_recreate:
            raise click.UsageError("--allow-recreate applies to the whole-config edit only")
        ctx.meta[_EDIT_META] = (editor_cmd, no_prompt)
        return
    resolved = state.resolve()
    snapshot = _read_with_baseline(state, resolved)
    editor.edit_config(
        resolved, snapshot,
        warn=state.warn, editor=editor_cmd,
        allow_recreate=allow_recreate, prompt=not no_prompt,
    )


def _entry_edit(
    kind: str, name: str, editor_cmd: Optional[str], no_prompt: bool,
    config_path: Optional[str], verbose: bool,
) -> None:
    state = _state(config_path, verbose)
    inherited_editor, inherited_no_prompt = click.get_current_context().meta.get(
        _EDIT_META, (None, False)
    )
    resolved = state.resolve()
    snapshot = _read_with_baseline(state, resolved)
    run = editor.edit_dataset if kind == "dataset" else editor.edit_destination
    run(
        resolved, snapshot, name,
        warn=state.warn, editor=editor_cmd or inherited_editor,
        prompt=not (no_prompt or inherited_no_prompt),
    )


@edit_group.command(
    "dataset",
    help=(
        "Edit one dataset's settings in $EDITOR. The buffer holds enabled, "
        "frequency, recursive, retention and remote but NOT the name (a name: "
        "key is rejected; use `rename dataset`). Omitted keys take their defaults."
    ),
)
@click.argument("name", metavar="NAME")
@_edit_options
@_common
def edit_dataset_cmd(
    name: str, editor_cmd: Optional[str], no_prompt: bool,
    config_path: Optional[str], verbose: bool,
) -> None:
    _entry_edit("dataset", name, editor_cmd, no_prompt, config_path, verbose)


@edit_group.command(
    "destination",
    help="Edit one destination's url in $EDITOR (the buffer has no name; use `rename destination`).",
)
@click.argument("name", metavar="NAME")
@_edit_options
@_common
def edit_destination_cmd(
    name: str, editor_cmd: Optional[str], no_prompt: bool,
    config_path: Optional[str], verbose: bool,
) -> None:
    _entry_edit("destination", name, editor_cmd, no_prompt, config_path, verbose)


# --------------------------------------------------------------------------
# rename
# --------------------------------------------------------------------------


@cli.group("rename", help="Rename a dataset or a destination in the configuration.")
@_common
def rename_group(config_path: Optional[str], verbose: bool) -> None:
    _state(config_path, verbose)


_DRY_RUN = click.option(
    "-d", "--dry-run", is_flag=True, default=False,
    help="Print the ZFS property actions and the configuration change without doing them.",
)


@rename_group.command(
    "dataset",
    help=(
        "Rename dataset OLD to NEW in the configuration. A dataset with remote "
        "destinations needs --force: the server-side copy would move, so the "
        "next backup is a full send and the old server copy is orphaned. The "
        "org.zfsbackup:* properties on the old ZFS dataset do not follow; "
        "--clear-stale-props removes them (refused while a daemon is running). "
        "This does not rename the ZFS dataset."
    ),
)
@click.argument("old", metavar="OLD")
@click.argument("new", metavar="NEW")
@click.option(
    "--force", is_flag=True, default=False,
    help="Rename a dataset that has remote destinations (next backup is a full send).",
)
@click.option(
    "--clear-stale-props", is_flag=True, default=False,
    help="Clear org.zfsbackup:config and anchor.* on the old dataset after the rename.",
)
@_DRY_RUN
@_common
def rename_dataset_cmd(
    old: str, new: str, force: bool, clear_stale_props: bool, dry_run: bool,
    config_path: Optional[str], verbose: bool,
) -> None:
    state = _state(config_path, verbose)
    resolved = state.resolve()
    snapshot = _read_with_baseline(state, resolved)
    rename.rename_dataset(
        resolved, snapshot, old, new,
        force=force, clear_stale_props=clear_stale_props, dry_run=dry_run, warn=state.warn,
    )


@rename_group.command(
    "destination",
    help=(
        "Rename destination OLD to NEW in the configuration and in every "
        "dataset's remote: list. Each referencing dataset's "
        "org.zfsbackup:anchor.OLD is copied to anchor.NEW first (copied, never "
        "moved; a differing existing anchor.NEW is overwritten and both values "
        "are printed); the old properties are left in place."
    ),
)
@click.argument("old", metavar="OLD")
@click.argument("new", metavar="NEW")
@click.option(
    "--skip-missing", is_flag=True, default=False,
    help=(
        "Proceed when a dataset that sends to OLD does not exist in ZFS, copying no "
        "anchor for it (default: refuse)."
    ),
)
@_DRY_RUN
@_common
def rename_destination_cmd(
    old: str, new: str, skip_missing: bool, dry_run: bool,
    config_path: Optional[str], verbose: bool,
) -> None:
    state = _state(config_path, verbose)
    resolved = state.resolve()
    snapshot = _read_with_baseline(state, resolved)
    rename.rename_destination(
        resolved, snapshot, old, new,
        dry_run=dry_run, skip_missing=skip_missing, warn=state.warn,
    )


# --------------------------------------------------------------------------
# trust / identity (TLS enrollment; the commands live in trust.py)
# --------------------------------------------------------------------------

trust.register(cli, _common, _state)
# After trust.register: `identity rotate` is added to its `identity` group.
enroll.register(cli, _common, _state)
backup.register(cli, _common, _state)
check_view.register(cli, _common, _state)


_sigterm_seen = False


def _on_sigterm(signum, frame) -> None:
    global _sigterm_seen
    _sigterm_seen = True
    # A second `kill` while the first is unwinding must not interrupt the
    # cleanup; SIGKILL remains for a command that hangs.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise _Terminated()


def main(argv: Optional[List[str]] = None) -> None:
    """Console-script entry point (`zfsbackup-config`). Exits the process with
    the command's exit code.

    SIGTERM is turned into a `KeyboardInterrupt` for the duration, so `kill`
    unwinds like ^C (temp and backup files removed, the transaction rolled
    back); the exit code is 143 instead of 130."""
    global _sigterm_seen
    _sigterm_seen = False
    previous = None
    try:
        previous = signal.signal(signal.SIGTERM, _on_sigterm)
    except ValueError:  # not the main thread: leave the signal alone
        pass
    try:
        try:
            cli.main(args=argv, prog_name="zfsbackup-config", standalone_mode=True)
        except SystemExit as exc:
            # A signal that landed outside the command (while Click parsed
            # arguments) comes back as Click's generic Abort, exit 1.
            if _sigterm_seen and exc.code in (1, EXIT_SIGINT):
                raise SystemExit(EXIT_SIGTERM) from None
            raise
    finally:
        if previous is not None:
            if argv is None:
                # The console script: the process is about to exit. Keep SIGTERM
                # handled (ignored) rather than back at its default, which
                # would kill a command that has already committed while it
                # flushes and exits. SIGINT likewise: a Ctrl-C here would
                # raise KeyboardInterrupt mid-flush and print a traceback over
                # a result that is already saved.
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                signal.signal(signal.SIGINT, signal.SIG_IGN)
            else:
                signal.signal(signal.SIGTERM, previous)  # embedded (tests)


if __name__ == "__main__":  # pragma: no cover
    main()

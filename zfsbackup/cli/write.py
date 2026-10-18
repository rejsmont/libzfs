"""The write discipline every `zfsbackup-config` edit shares (plan item 15.3).

One sequence, so `set`, `edit`, `rename` and the rest cannot each get it
slightly wrong::

    snapshot = read_snapshot(resolved)          # (config, generation), then dispose
    new = <mutate a copy / parse the edited YAML>
    # ... $EDITOR or a prompt may happen here: nothing is open meanwhile ...
    result = commit_config(resolved, snapshot, new, warn=state.warn)

- `read_snapshot` is the store's `read_config` (one read transaction: schema
  check, rows and generation agree) followed by `dispose_all()`, so the process
  holds no connection while a human is thinking. A caller that is about to run
  `$EDITOR` or prompt calls `release_database()` again if it opened anything
  since.
- `commit_config` validates the whole new configuration in memory FIRST
  (`validate_config`: a `ValueError` here means the database was never opened
  for write; its warnings are shown once), then `write_config`, which takes the
  writer lock itself (never wrap it in `writer_lock`: that lock is not
  re-entrant), compares the generation inside `BEGIN IMMEDIATE`, and writes in
  one transaction. `ConfigConflict` (somebody wrote since the snapshot) and
  `ConfigBusy` propagate; `errors.py` maps both to exit 75 with their messages.
  Nothing here ever retries: a conflict means the edit was made against a
  configuration that no longer exists.
- After a successful write `notify_daemon` says whether a daemon is running and
  that it will not apply the change until restarted. The CLI never signals it.

This module reaches the database only through `zfsbackup.config.store`'s
public API (and `zfsbackup.daemonlock`, which has no daemon imports).
"""

import shlex
from pathlib import Path
from typing import Callable, Optional, Tuple

import click

from zfsbackup import daemonlock
from zfsbackup.cli.errors import CliError
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    ConfigSnapshot,
    ResolvedConfigPath,
    WriteResult,
    dispose_all,
    read_config,
    validate_config,
    write_config,
)

__all__ = [
    "commit_config",
    "notify_daemon",
    "read_snapshot",
    "release_database",
    "where_flag",
]


def _echo_warning(message: str) -> None:
    click.echo(f"warning: {message}", err=True)


def release_database() -> None:
    """Drop this process's cached engines and pooled connections. Call before
    launching `$EDITOR` or prompting (a human may take minutes; an open WAL
    read snapshot would pin the checkpoint) and before anything that may
    fork."""
    dispose_all()


def read_snapshot(resolved: ResolvedConfigPath) -> ConfigSnapshot:
    """The stored configuration with the generation it was read at, from one
    read transaction, with nothing left open afterwards.

    A stored configuration `load_config` cannot make sense of (no datasets, a
    bad duration row) is a `CliError` that says which stage failed; path,
    schema and storage errors propagate for `errors.report` to word.
    """
    try:
        return read_config(resolved)
    except ValueError as exc:
        raise CliError(f"the stored configuration cannot be loaded: {exc}") from exc
    finally:
        release_database()


def commit_config(
    resolved: ResolvedConfigPath,
    snapshot: ConfigSnapshot,
    config: BackupConfig,
    *,
    warn: Callable[[str], None] = _echo_warning,
    announce: bool = True,
    lock: Optional[Path] = None,
) -> WriteResult:
    """Validate `config`, then replace the stored configuration with it iff the
    store is still at `snapshot.generation`; then tell the operator about the
    daemon (unless `announce=False`, for a caller that wants to print its own
    result line first and call `notify_daemon` itself).

    `warn` receives each `validate_config` warning once. Raises `ValueError`
    for an invalid `config` (nothing opened), `ConfigConflict` / `ConfigBusy`
    (nothing written), and the store's path/schema/storage errors.
    """
    for message in validate_config(config):
        warn(message)
    release_database()
    try:
        # `lock` is a writer lock the caller already holds (`paths.writer_lock`);
        # `write_config` then reuses it instead of taking it again (the flock
        # is not re-entrant).
        result = write_config(
            resolved, config, expected_generation=snapshot.generation, lock=lock
        )
    finally:
        # Whatever the write opened, nothing may stay pooled: the caller's
        # retry loop may run `$EDITOR` next.
        release_database()
    if announce:
        notify_daemon(resolved, result.generation)
    return result


def where_flag(resolved) -> str:
    """` -c <db>` for a command hint, or "" when the database is the default
    one. The one place that decides how a hint names the database."""
    if resolved.source == "default":
        return ""
    return f" -c {shlex.quote(str(resolved.resolved_path))}"


def _daemon_notice(
    resolved: ResolvedConfigPath, generation: Optional[int]
) -> Tuple[str, Optional[str]]:
    """(stdout line, stderr warning or None) for the post-write notice."""
    updated = "config updated" if generation is None else f"config updated (generation {generation})"
    try:
        info = daemonlock.probe(resolved)
    except OSError as exc:  # DaemonLockError is an OSError
        return updated, (
            f"warning: could not check whether a daemon is running for "
            f"{resolved.resolved_path}: {exc.strerror or exc}"
        )
    if info is None:
        return f"{updated}; no running daemon detected for {resolved.resolved_path}", None
    who = f"pid {info.pid}" if info.pid is not None else "(pid unknown)"
    if info.pid is not None and info.pid_alive is False:
        return (
            f"{updated}; "
            + daemonlock.stale_supervisor_message(daemonlock.lock_path(resolved), info.pid)
        ), None
    if generation is not None and info.generation == generation:
        return f"{updated}; running daemon {who} is already on generation {generation}", None
    on = f"generation {info.generation}" if info.generation is not None else "an unknown generation"
    return (
        f"{updated}; running daemon {who} is on {on} and will not apply "
        "this until restarted"
    ), None


def notify_daemon(resolved: ResolvedConfigPath, generation: Optional[int]) -> None:
    """Say, after a successful write, whether a daemon is running against this
    database and what that means. Never raises: a failure to *check*, or to
    *print* (a closed pipe), is at most a warning -- the write already
    committed and must not turn into a non-zero exit.

    `generation` is the one just written (`None` if it is not known).
    """
    try:
        line, warning = _daemon_notice(resolved, generation)
    except Exception as exc:  # noqa: BLE001 - never fail a committed write
        line, warning = "config updated", f"warning: could not check for a running daemon: {exc}"
    try:
        click.echo(line)
        if warning:
            click.echo(warning, err=True)
    except OSError:  # BrokenPipeError, EIO, ENOSPC on the output stream
        pass

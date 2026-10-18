"""The write discipline every `zfsbackup-config` edit shares (plan items 15.3, 19.1).

One sequence, so `set`, `edit`, `rename` and the rest cannot each get it
slightly wrong::

    snapshot = read_snapshot(resolved)          # (config, generation, store id), then dispose
    new = <mutate a copy / parse the edited YAML>
    # ... $EDITOR or a prompt may happen here: nothing is open meanwhile ...
    result = commit_config(resolved, snapshot, new, warn=state.warn)

- `read_snapshot` is the store's `read_config` (one read transaction: schema
  check, rows, generation and store id agree) followed by `dispose_all()`, so
  the process holds no connection while a human is thinking. A caller that is
  about to run `$EDITOR` or prompt calls `release_database()` again if it
  opened anything since.
- `commit_config` validates the whole new configuration in memory FIRST
  (`validate_config`: a `ValueError` here means the database was never opened
  for write; its warnings are shown once), then `write_config`, which takes the
  writer lock itself (never wrap it in `writer_lock`: that lock is not
  re-entrant), compares the store id and the generation inside `BEGIN
  IMMEDIATE`, and writes in one transaction. `ConfigConflict` (somebody wrote
  since the snapshot; `StoreIdConflict` when the database file was replaced)
  and `ConfigBusy` propagate; `errors.py` maps both to exit 75 with their
  messages. Nothing here ever retries: a conflict means the edit was made
  against a configuration that no longer exists.
- After EVERY committed write `notify_daemon` asks the running daemon to
  reload, exactly once (`daemonlock.request_reload`: it verifies the daemon is
  the lock holder before signalling it with SIGHUP, and never signals
  otherwise), then says what happened. Where the daemon cannot be signalled
  (another user's, or on a host where it cannot be verified) it still applies
  the change itself within `daemonlock.RELOAD_POLL_SECONDS`; only a daemon that
  predates live reload must be restarted. After a signal the CLI waits up to
  `--reload-wait` seconds (default 3; 0 = do not wait) for the daemon to name
  the new generation in its pid file; the wait runs after the write and holds
  no lock. None of this changes the exit code: the write is already durable.

This module reaches the database only through `zfsbackup.config.store`'s
public API (and `zfsbackup.daemonlock`, which has no daemon imports).
"""

import shlex
import sys
import time
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
    "begin_reload",
    "notify_daemon",
    "COMMITTED_MARK",
    "read_snapshot",
    "release_database",
    "where_flag",
    "DEFAULT_RELOAD_WAIT",
]

#: `--reload-wait` default: how long, after a SIGHUP, the CLI waits for the
#: daemon to name the new generation in its pid file.
DEFAULT_RELOAD_WAIT = 3.0
#: How often that wait reads the pid file.
ACK_POLL_SECONDS = 0.1


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
    store is still the file and the generation `snapshot` was read at; then
    ask the daemon to reload and tell the operator (unless `announce=False`,
    for a caller that wants to print its own result line first and call
    `notify_daemon` itself).

    `warn` receives each `validate_config` warning once. Raises `ValueError`
    for an invalid `config` (nothing opened), `ConfigConflict` /
    `StoreIdConflict` / `ConfigBusy` (nothing written), and the store's
    path/schema/storage errors.

    `announce=True` with a caller-held `lock` is refused: the notice may wait
    for the daemon, and nothing may wait while the writer lock is held.
    """
    if announce and lock is not None:
        raise ValueError("announce=True cannot be combined with a caller-held writer lock")
    for message in validate_config(config):
        warn(message)
    release_database()
    try:
        # `lock` is a writer lock the caller already holds (`paths.writer_lock`);
        # `write_config` then reuses it instead of taking it again (the flock
        # is not re-entrant).
        result = write_config(
            resolved,
            config,
            expected_generation=snapshot.generation,
            expected_store_id=snapshot.store_id,
            lock=lock,
        )
    finally:
        # Whatever the write opened, nothing may stay pooled: the caller's
        # retry loop may run `$EDITOR` next.
        release_database()
    if announce:
        notify_daemon(resolved, result.generation, result.store_id)
    return result


def where_flag(resolved) -> str:
    """` -c <db>` for a command hint, or "" when the database is the default
    one. The one place that decides how a hint names the database."""
    if resolved.source == "default":
        return ""
    return f" -c {shlex.quote(str(resolved.resolved_path))}"


def _reload_wait() -> float:
    """`--reload-wait` of the running invocation (the group option), or the
    default when there is no Click context (a library caller, a test)."""
    ctx = click.get_current_context(silent=True)
    value = getattr(ctx.obj, "reload_wait", None) if ctx is not None else None
    if value is None:
        return DEFAULT_RELOAD_WAIT
    return float(value)


def _newer(found: Optional[Tuple[int, int]], key: Tuple[int, int]) -> bool:
    """Is `found` a later generation of the SAME store as `key`? Keys of
    different stores are not ordered (a re-import picks a random store id), so
    they are never "newer"."""
    return found is not None and found[0] == key[0] and found[1] > key[1]


def _answer(info: daemonlock.DaemonInfo, key: Tuple[int, int]) -> Optional[str]:
    """How the daemon's pid file answers our write, or `None` for "not yet":
    `applied` (it is on our generation), `accepted` (it is switching to it),
    `rejected`, `newer` (it is on, or applying, a later generation of this
    store: `target` can sit on an older key indefinitely while in-flight work
    finishes, and a later write replaces it, so our key may never appear) or
    `superseded` (it REJECTED a later generation of this store, so ours was
    folded into that one and will never be named on its own).
    Keys of different stores are not ordered and never answer. A read that was
    torn (`info.stable` is False) answers nothing: its fields may be a mix of
    two states."""
    if not info.stable:
        return None
    if info.applied == key:
        return "applied"
    if _newer(info.applied, key) or _newer(info.target, key):
        return "newer"
    if info.target == key:
        return "accepted"
    if info.rejected == key:
        return "rejected"
    if _newer(info.rejected, key):
        return "superseded"
    return None


def _await_answer(
    resolved: ResolvedConfigPath, key: Tuple[int, int], wait: float
) -> Tuple[Optional[str], Optional[daemonlock.DaemonInfo]]:
    """Poll the daemon's pid file every `ACK_POLL_SECONDS` until it names `key`
    (or a newer one) in `applied`, `target` or `rejected`, or `wait` seconds
    pass. -> `(answer, last stable info read)`.

    `answer` is one of `_answer`'s words, `"gone"` (`probe` finds nobody
    holding the lock any more: the daemon exited), `"unreadable"` (`probe`
    raised `OSError`), or `None` (the deadline passed). A torn read is "no
    answer yet" and polling continues.

    Holds no lock and no database handle: it only reads the pid file, which
    `probe` does on a fresh descriptor under a non-blocking shared flock. Always
    bounded by the deadline, so a signalled supervisor that never answers (a
    zombie, a wedged process) costs at most `wait` seconds.
    """
    deadline = time.monotonic() + wait
    last: Optional[daemonlock.DaemonInfo] = None
    while True:
        try:
            info = daemonlock.probe(resolved)
        except OSError:
            return "unreadable", last
        if info is None:
            return "gone", last
        if info.stable:
            last = info
        answer = _answer(info, key)
        if answer is not None:
            return answer, info
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None, last
        time.sleep(min(ACK_POLL_SECONDS, remaining))


def _unverified_line(updated: str, req: daemonlock.ReloadRequest) -> str:
    poll = daemonlock.RELOAD_POLL_SECONDS
    detail = req.detail or "no reason given"
    who = f"pid {req.pid}" if req.pid is not None else "an unknown pid"
    lowered = detail.lower()
    code = req.reason_code
    platform = code == "not-linux" or (
        not code
        and sys.platform != "linux"
        and ("only on linux" in lowered or "this platform" in lowered)
    )
    if platform:
        # Not a doubt about this daemon: this host cannot prove any daemon's
        # identity, so it never signals one. (On Linux the daemon's own detail
        # says what failed, and is shown instead.)
        return (
            f"{updated}; running daemon {who} was not signalled (a daemon can be verified "
            f"only on Linux); it picks up the change within {poll}s"
        )
    if req.pid == 1 and code in ("", "refused-pid"):
        return (
            f"{updated}; running daemon pid 1 was not signalled (pid 1 is never signalled "
            f"from outside; probably a container); it picks up the change within {poll}s"
        )
    return (
        f"{updated}; could not verify that {who} is the daemon ({detail}), so it "
        f"was not signalled; it picks up the change within {poll}s"
    )


def begin_reload(
    resolved: ResolvedConfigPath, generation: Optional[int], store_id: Optional[int] = None
) -> daemonlock.ReloadRequest:
    """Ask the running daemon to reload, NOW. Call it immediately after a
    commit, before printing anything: a closed stdout must not cost the signal.
    Pass the result to `notify_daemon(..., request=...)` to report it. Never
    raises (a failure is an `error` outcome)."""
    key = (store_id, generation) if store_id is not None and generation is not None else None
    try:
        return daemonlock.request_reload(resolved, skip_if_applied=key)
    except Exception as exc:  # noqa: BLE001 - the write is already durable
        return daemonlock.ReloadRequest("error", detail=f"{type(exc).__name__}: {exc}")


def _daemon_notice(
    resolved: ResolvedConfigPath,
    generation: Optional[int],
    store_id: Optional[int] = None,
    wait: float = DEFAULT_RELOAD_WAIT,
    request: Optional[daemonlock.ReloadRequest] = None,
) -> Tuple[str, Optional[str]]:
    """(stdout line, stderr warning or None) for the post-write notice.

    Uses `request` (from `begin_reload`) or, without one, calls
    `daemonlock.request_reload` itself -- exactly once either way. Only the
    `signalled` outcome waits (up to `wait` seconds, see `_await_answer`).
    """
    updated = "config updated" if generation is None else f"config updated (generation {generation})"
    key = (store_id, generation) if store_id is not None and generation is not None else None
    req = request if request is not None else begin_reload(resolved, generation, store_id)
    outcome = req.outcome
    pid = req.pid
    who = f"pid {pid}" if pid is not None else "(pid unknown)"
    poll = daemonlock.RELOAD_POLL_SECONDS
    info = req.info

    if outcome == "no-daemon":
        return f"{updated}; no running daemon detected for {resolved.resolved_path}", None
    if outcome == "already-applied":
        return f"{updated}; running daemon {who} is already on generation {generation}", None
    if outcome == "signalled":
        return _signalled_notice(resolved, updated, who, key, wait)
    if outcome == "not-permitted":
        return (
            f"{updated}; daemon {who} runs as another user and was not signalled; "
            f"it picks up the change within {poll}s"
        ), None
    if outcome == "unverified":
        return _unverified_line(updated, req), None
    if outcome == "supervisor-gone":
        code = req.reason_code
        if code in ("zombie", "no-such-process", "exited"):
            gone = True
        elif code in ("not-lock-holder", "reused"):
            gone = False
        else:  # no reason code: fall back to asking about the pid itself
            gone = pid is not None and daemonlock.pid_is_alive(pid) is False
        if pid is not None and gone:
            return f"{updated}; " + daemonlock.stale_supervisor_message(
                daemonlock.lock_path(resolved), pid
            ), None
        if pid is not None:
            return (
                f"{updated}; {who} is not the process holding the daemon lock, so it was "
                f"not signalled; a running daemon picks up the change within {poll}s"
            ), None
        return f"{updated}; the daemon lock is held but its supervisor is gone", None
    if outcome == "legacy":
        running = info.generation if info is not None else None
        on = f"generation {running}" if running is not None else "an unknown generation"
        return (
            f"{updated}; running daemon {who} is on {on} and predates live "
            "reload; restart it to apply this"
        ), None
    # "error", or an outcome this CLI does not know: the write is durable, we
    # just could not say anything about the daemon.
    return updated, (
        f"warning: could not check whether a daemon is running for "
        f"{resolved.resolved_path}: {req.detail or outcome}"
    )


#: Set (to True) on a `KeyboardInterrupt`/`SystemExit` that interrupted the
#: post-commit acknowledgement wait: the write is durable, so handlers must not
#: say it "may or may not have been applied".
COMMITTED_MARK = "zfsbackup_write_committed"


def _signalled_notice(
    resolved: ResolvedConfigPath,
    updated: str,
    who: str,
    key: Optional[Tuple[int, int]],
    wait: float,
) -> Tuple[str, Optional[str]]:
    poll = daemonlock.RELOAD_POLL_SECONDS
    requested = f"{updated}; reload requested from daemon {who}"
    waited = key is not None and wait > 0
    answer: Optional[str] = None
    info: Optional[daemonlock.DaemonInfo] = None
    if waited:
        try:
            answer, info = _await_answer(resolved, key, wait)
        except (KeyboardInterrupt, SystemExit) as exc:
            # ^C / SIGTERM while waiting: the write and the signal are done;
            # say so before the interrupt message, never leave the operator
            # guessing whether the change was saved.
            try:
                click.echo(
                    f"{updated}; interrupted while waiting for daemon {who} to acknowledge "
                    f"(the change is saved; it picks up the change within {poll}s)"
                )
            except OSError:
                pass
            try:
                setattr(exc, COMMITTED_MARK, True)
            except Exception:  # noqa: BLE001
                pass
            raise
    if answer == "applied":
        return f"{requested}: it is now on generation {key[1]}", None
    if answer == "accepted":
        return (
            f"{requested}: it accepted generation {key[1]} and is switching its "
            "workers over (in-flight transfers finish first)"
        ), None
    if answer == "rejected":
        applied = info.applied if info is not None else None
        running = applied[1] if applied is not None else (info.generation if info is not None else None)
        keeps = f"generation {running}" if running is not None else "its previous configuration"
        return requested, (
            f"warning: daemon {who} rejected generation {key[1]} and keeps running "
            f"{keeps}; see its log"
        )
    if answer == "newer":
        newest = max(
            (k[1] for k in (info.applied, info.target) if _newer(k, key)),
        )
        return (
            f"{requested}: it is already on (or applying) newer generation {newest}"
        ), None
    if answer == "superseded":
        return (
            f"{requested}; superseded by generation {info.rejected[1]}, which daemon "
            f"{who} rejected; see its log"
        ), None
    if answer == "gone":
        return (
            f"{requested}; the daemon exited before acknowledging; the next daemon "
            f"start reads generation {key[1]}"
        ), None
    if answer == "unreadable":
        return (
            f"{requested}; could not read its pid file while waiting; it picks up "
            f"the change within {poll}s if it is running"
        ), None
    line = f"{requested} (SIGHUP)"
    if waited:
        line += (
            f"; not yet acknowledged; it picks up the change at its next check "
            f"(within {poll}s); if it does not, see the daemon log"
        )
    return line, None


def notify_daemon(
    resolved: ResolvedConfigPath,
    generation: Optional[int],
    store_id: Optional[int] = None,
    *,
    reload_wait: Optional[float] = None,
    request: Optional[daemonlock.ReloadRequest] = None,
) -> None:
    """After a successful write: ask the running daemon (if any, and only
    after `request_reload` has verified it) to reload, wait briefly for its
    acknowledgement, and say what happened. Call it exactly once per committed
    write, after the writer lock is released. Never raises for a failure to
    *check*, or to *print* (a closed pipe) -- the write already committed and
    must not turn into a non-zero exit -- but an interrupt (^C, SIGTERM) during
    the acknowledgement wait propagates, as it does anywhere (the exception is
    marked with `COMMITTED_MARK`; the exit code reflects the interrupt, the
    write is durable regardless).

    `generation` and `store_id` are the ones just written (`None` if not
    known; without both there is no key to skip on or to wait for).
    `reload_wait` overrides the `--reload-wait` option (seconds; 0 = do not
    wait). `request` is the outcome of an earlier `begin_reload`: the caller
    that prints other lines first requests the reload right after the commit
    (so a closed stdout cannot cost the signal) and passes it here; the
    request is then NOT made again.
    """
    wait = _reload_wait() if reload_wait is None else reload_wait
    try:
        line, warning = _daemon_notice(resolved, generation, store_id, wait, request)
    except Exception as exc:  # noqa: BLE001 - never fail a committed write
        line, warning = "config updated", f"warning: could not check for a running daemon: {exc}"
    try:
        click.echo(line)
        if warning:
            click.echo(warning, err=True)
    except OSError:  # BrokenPipeError, EIO, ENOSPC on the output stream
        pass

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
- A configuration that contains a local (`zfs:`) destination needs this host's
  client id (its copies live under `<target>/<client id>/`). When the snapshot
  has none, `commit_config` stores one first -- `generate_client_id()` through
  the store's `set_client_id(expected_generation=..., expected_store_id=...)`,
  which bumps the config generation by one -- and then writes with the new
  generation, both under ONE writer lock hold, so no other writer can slip in
  between and the original snapshot is still the compare-and-swap basis. The
  id is announced on stdout ("generated this host's client id: ...") by the
  next `begin_reload` for that generation -- after the writer lock is released
  and after the daemon was asked to reload, like every other line printed
  after a commit. On success the caller's single `begin_reload` covers both
  generations and prints it; if the write that follows fails, the id stays
  (it is committed and harmless), `commit_config` itself calls `begin_reload`
  for the id's generation (so the daemon is asked once and the line is
  printed) and the error propagates. An interrupt (^C) anywhere between the
  id's commit and the config write takes that same path, because the id's
  generation is recorded the moment `set_client_id` returns; the one window
  that cannot be guarded is an interrupt INSIDE `set_client_id` after its
  commit but before it returns, and there the daemon's 10 s poll of the
  generation applies the id instead of a signal. A caller-held `lock` (never
  the case for the id-less store this is for, in practice) gets the signal but
  not the line: that waits for the next `begin_reload`.
- A configuration that shares a backup target with the server (a local
  destination whose target is exactly `remote_backup.target_dataset`,
  `config.shared_receive_target`) reserves this host's own client id on the
  server. When that id is also bound in the trust tables (a loopback setup:
  this host enrolled with its own server) the server refuses its receives, and
  `commit_config` queues that check; `notify_daemon` prints the warning after its notice
  for that generation (`shared_target_warning`; the trust tables are read only
  through `zfsbackup.config.store`'s `list_clients`, and only there: after the
  reload was requested and the writer lock released, never in between).
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
from typing import Callable, List, Optional, Tuple

import click

from zfsbackup import daemonlock
from zfsbackup.cli.errors import CliError
from zfsbackup.config import BackupConfig, generate_client_id, shared_receive_target
from zfsbackup.config.store import (
    ConfigSnapshot,
    ResolvedConfigPath,
    WriteResult,
    dispose_all,
    list_clients,
    read_config,
    set_client_id,
    validate_config,
    write_config,
    writer_lock,
)

__all__ = [
    "commit_config",
    "begin_reload",
    "await_applied",
    "notify_daemon",
    "COMMITTED_MARK",
    "ID_COMMITTED_MARK",
    "read_snapshot",
    "release_database",
    "where_flag",
    "shared_local_destinations",
    "own_id_bound",
    "shared_target_warning",
    "stored_shared_target_warning",
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


#: Set (to True) on an exception that left `commit_config` after a fresh client
#: id had been stored: the configuration was not written, the id was.
ID_COMMITTED_MARK = "zfsbackup_client_id_committed"

#: `(store_id, generation, client_id)` of a client id this process just stored
#: and has not announced yet. `begin_reload` prints it when it is called for
#: that very key, then forgets it.
_pending_announcement: Optional[Tuple[Optional[int], int, str]] = None


def _announce_client_id(client_id: str) -> None:
    """Say the id was generated. Never raises: the id is already stored."""
    try:
        click.echo(f"generated this host's client id: {client_id}")
    except OSError:  # BrokenPipeError, EIO, ENOSPC on the output stream
        pass


def _flush_announcement(generation: Optional[int], store_id: Optional[int]) -> None:
    """Print the pending client-id line iff it belongs to the write
    `(store_id, generation)` that `begin_reload` was just called for. Anything
    else pending is stale (its caller never reloaded) and dropped."""
    global _pending_announcement
    pending, _pending_announcement = _pending_announcement, None
    if pending is not None and pending[0] == store_id and pending[1] == generation:
        _announce_client_id(pending[2])


class _IdProgress:
    """What `_write_with_client_id` committed before it stopped (the id's
    `ClientIdUpdate`), so `commit_config` can still ask for the reload once."""

    update = None


def _write_with_client_id(
    resolved: ResolvedConfigPath,
    snapshot: ConfigSnapshot,
    config: BackupConfig,
    held: Path,
    progress: _IdProgress,
) -> WriteResult:
    """Store a fresh client id, then `config`, under the writer lock `held`.

    The id goes in with `set_client_id(expected_generation=snapshot's,
    expected_store_id=snapshot's)`: a moved generation or a replaced file is
    a `ConfigConflict` / `StoreIdConflict` before anything is written, and the
    returned generation (the snapshot's plus one) is what the config write then
    compares against. Nothing else can write in between (`held`). `progress`
    records the committed id for the caller; nothing is printed here (the
    lock is held)."""
    progress.update = set_client_id(
        resolved,
        generate_client_id(),
        expected_generation=snapshot.generation,
        expected_store_id=snapshot.store_id,
        lock=held,
    )
    release_database()
    return write_config(
        resolved,
        config,
        expected_generation=progress.update.generation,
        expected_store_id=snapshot.store_id,
        lock=held,
    )


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
    path/schema/storage errors. "Nothing written" holds except for one case:
    when the snapshot had no client id and `config` has a local destination, a
    fresh id is committed first (its own generation bump), so a later failure
    of the config write leaves the id stored. Then the daemon has already been
    asked to reload (and the id line printed) before the exception propagates;
    the exception then carries `ID_COMMITTED_MARK = True` so a caller that would say
    "nothing was written" can say what is true.

    `announce=True` with a caller-held `lock` is refused: the notice may wait
    for the daemon, and nothing may wait while the writer lock is held.
    """
    if announce and lock is not None:
        raise ValueError("announce=True cannot be combined with a caller-held writer lock")
    for message in validate_config(config):
        warn(message)
    release_database()
    needs_client_id = (
        getattr(snapshot, "client_id", None) is None
        and any(d.is_local for d in config.destinations.values())
    )
    progress = _IdProgress()
    try:
        try:
            if needs_client_id and lock is None:
                with writer_lock(resolved) as held:
                    result = _write_with_client_id(resolved, snapshot, config, held, progress)
            elif needs_client_id:
                result = _write_with_client_id(resolved, snapshot, config, lock, progress)
            else:
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
    except BaseException as exc:
        update = progress.update
        if update is not None:
            # The id is committed (the generation moved), the config was not:
            # the writer lock is released now (unless the caller holds it), so
            # ask the daemon to reload once -- which also prints the id line --
            # and let the error through.
            _stash_announcement(snapshot.store_id, update.generation, update.client_id)
            begin_reload(
                resolved, update.generation, snapshot.store_id,
                announce_pending=lock is None,
            )
            try:
                setattr(exc, ID_COMMITTED_MARK, True)
            except Exception:  # noqa: BLE001
                pass
        raise
    if progress.update is not None:
        # One reload covers both generations: the line belongs to the write's.
        _stash_announcement(result.store_id, result.generation, progress.update.client_id)
    global _pending_notice
    _pending_notice = None
    if shared_receive_target(config) is not None:
        # Only queued: the trust tables are read after the reload was requested
        # and the writer lock is released (`notify_daemon`), never before.
        _pending_notice = (
            result.store_id, result.generation, resolved, config,
            getattr(snapshot, "client_id", None),
        )
    if announce:
        notify_daemon(resolved, result.generation, result.store_id)
    return result


def _stash_announcement(store_id: Optional[int], generation: int, client_id: str) -> None:
    global _pending_announcement
    _pending_announcement = (store_id, generation, client_id)


def shared_local_destinations(config: BackupConfig) -> Tuple[Optional[str], List[str]]:
    """`(target, names)`: the server's target a local destination shares
    (`shared_receive_target`, `None` when nothing is shared) and the sorted
    names of the local destinations on it."""
    target = shared_receive_target(config)
    if target is None:
        return None, []
    wanted = target.split("/")
    names = sorted(
        name for name, dest in config.destinations.items()
        if dest.is_local and dest.target_dataset.strip().rstrip("/").split("/") == wanted
    )
    return target, names


def own_id_bound(resolved: ResolvedConfigPath, client_id: Optional[str]) -> bool:
    """Is `client_id` bound in the trust tables to a live pin (a client of this
    server already uses it)? Best effort, used to word notes and to warn: an
    unreadable trust table is "no"."""
    if not client_id:
        return False
    try:
        clients = list_clients(resolved)
    except Exception:  # noqa: BLE001 - cosmetic; never fail a committed write
        return False
    finally:
        release_database()
    return any(c.client_id == client_id and c.pins for c in clients)


def shared_target_warning(
    resolved: ResolvedConfigPath, config: BackupConfig, client_id: Optional[str]
) -> Optional[str]:
    """The loopback warning: `config` shares a target with the server while this
    host's own `client_id` is bound in the trust tables, so the server refuses
    that client's receives. `None` when either half is absent."""
    target, names = shared_local_destinations(config)
    if target is None or not own_id_bound(resolved, client_id):
        return None
    listed = ", ".join(repr(n) for n in names)
    return (
        f"local destination(s) {listed} share the server's target {target!r}, and this host's "
        f"own client id {client_id!r} is enrolled with this server: the server now refuses "
        f"receives from client {client_id!r}. Finish moving to the local destination with "
        f"`zfsbackup-config{where_flag(resolved)} trust revoke --client {client_id}`"
    )


def stored_shared_target_warning(resolved: ResolvedConfigPath) -> Optional[str]:
    """`shared_target_warning` for the configuration now stored (what `import`
    committed). Best effort: `None` when it cannot be read."""
    try:
        snapshot = read_config(resolved)
    except Exception:  # noqa: BLE001 - cosmetic; the write already happened
        return None
    finally:
        release_database()
    return shared_target_warning(resolved, snapshot.config, snapshot.client_id)


#: `(store_id, generation, resolved, config, client_id)` of a write that left
#: the configuration sharing a target with the server; `notify_daemon` decides
#: and prints the loopback warning for it (`shared_target_warning`) after its
#: notice, when called for that very key.
_pending_notice: Optional[Tuple[Optional[int], int, ResolvedConfigPath, BackupConfig, Optional[str]]] = None


def _flush_notice(generation: Optional[int], store_id: Optional[int]) -> None:
    global _pending_notice
    pending, _pending_notice = _pending_notice, None
    if pending is None or pending[0] != store_id or pending[1] != generation:
        return
    try:
        text = shared_target_warning(*pending[2:])
        if text is not None:
            _echo_warning(text)
    except Exception:  # noqa: BLE001 - cosmetic; the write is committed and signalled
        pass


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
    `rejected`, `deferred` (refused for now for a reason that can pass later;
    NOT final, the daemon re-checks it every poll), `superseded-deferred` (a
    LATER generation of this store is deferred, so ours is folded into it; also
    not final), `newer` (it is on, or applying, a later generation of this
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
    if info.deferred == key:
        return "deferred"
    if _newer(info.deferred, key):
        return "superseded-deferred"
    return None


#: Answers that mean "refused for now, re-checked every poll": never final.
_DEFERRED_ANSWERS = ("deferred", "superseded-deferred")


def _await_answer(
    resolved: ResolvedConfigPath, key: Tuple[int, int], wait: float, *,
    applied_only: bool = False,
) -> Tuple[Optional[str], Optional[daemonlock.DaemonInfo]]:
    """Poll the daemon's pid file every `ACK_POLL_SECONDS` until it names `key`
    (or a newer one) in `applied`, `target` or `rejected`, or `wait` seconds
    pass. -> `(answer, last stable info read)`.

    `deferred` is not final: polling goes on (the daemon re-checks it every poll
    and may apply it), and only if the deadline passes while the last stable
    read still said `deferred` (or `superseded-deferred`, the same for a later
    generation) is that word returned (with that info, whose
    `deferred_reason` the caller can show) instead of `None`.

    `answer` is one of `_answer`'s words (except that `deferred` only comes at
    the deadline), `"gone"` (`probe` finds nobody
    holding the lock any more: the daemon exited), `"unreadable"` (`probe`
    raised `OSError`), or `None` (the deadline passed). A torn read is "no
    answer yet" and polling continues.

    Holds no lock and no database handle: it only reads the pid file, which
    `probe` does on a fresh descriptor under a non-blocking shared flock. Always
    bounded by the deadline, so a signalled supervisor that never answers (a
    zombie, a wedged process) costs at most `wait` seconds.

    `applied_only=True` keeps polling through `accepted` (and through a newer
    `target` alone): it settles only on `applied` / a newer `applied` (answered
    as `newer`), on `rejected` / `superseded` (the key will never be applied),
    or on `gone` / `unreadable`. `await_applied` is its public spelling.
    """
    # Nothing pooled may stay open across the wait, whichever command got here.
    release_database()
    deadline = time.monotonic() + wait
    last: Optional[daemonlock.DaemonInfo] = None
    deferred: Optional[daemonlock.DaemonInfo] = None
    deferred_answer: Optional[str] = None
    while True:
        try:
            info = daemonlock.probe(resolved)
        except OSError:
            return "unreadable", last
        if info is None:
            return "gone", last
        answer = _answer(info, key)
        if info.stable:
            last = info
            deferred = info if answer in _DEFERRED_ANSWERS else None
            deferred_answer = answer if deferred is not None else None
        if answer in _DEFERRED_ANSWERS:
            answer = None  # not final: keep waiting within the deadline
        if (
            applied_only
            and answer in ("accepted", "newer")
            and not (info.applied == key or _newer(info.applied, key))
        ):
            answer = None
        if answer is not None:
            return answer, info
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if deferred is not None:
                return deferred_answer, deferred
            return None, last
        time.sleep(min(ACK_POLL_SECONDS, remaining))


def await_applied(
    resolved: ResolvedConfigPath, key: Tuple[int, int], wait: Optional[float] = None
) -> Tuple[Optional[str], Optional[daemonlock.DaemonInfo]]:
    """Wait up to `wait` seconds (default: `--reload-wait`) until the daemon's
    pid file says it has APPLIED `key` -- every worker runs it -- or a later
    generation of the same store. -> `(answer, last stable info)`: `applied`,
    `newer` (applied is later than `key`), `rejected`, `superseded`, `gone`,
    `unreadable`, `superseded-deferred` (as `deferred`, for a later generation),
    `deferred` (still deferred -- refused for now, re-checked every
    poll -- when the wait ran out; not final), or `None` (not applied in time,
    including "accepted but still switching"). Reads the pid file only; holds no lock."""
    return _await_answer(
        resolved, key, _reload_wait() if wait is None else wait, applied_only=True
    )


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
    resolved: ResolvedConfigPath,
    generation: Optional[int],
    store_id: Optional[int] = None,
    *,
    announce_pending: bool = True,
) -> daemonlock.ReloadRequest:
    """Ask the running daemon to reload, NOW. Call it immediately after a
    commit, before printing anything: a closed stdout must not cost the signal.
    Pass the result to `notify_daemon(..., request=...)` to report it. Never
    raises (a failure is an `error` outcome).

    The only line this prints is "generated this host's client id: ..." when
    `commit_config` just stored one for this very write (so it comes after the
    signal and after the writer lock is released); `announce_pending=False`
    leaves it for the next call."""
    key = (store_id, generation) if store_id is not None and generation is not None else None
    try:
        return daemonlock.request_reload(resolved, skip_if_applied=key)
    except Exception as exc:  # noqa: BLE001 - the write is already durable
        return daemonlock.ReloadRequest("error", detail=f"{type(exc).__name__}: {exc}")
    finally:
        if announce_pending:
            _flush_announcement(generation, store_id)


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
        elif code in ("not-lock-holder", "reused", "file-not-open"):
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
    if answer in ("deferred", "superseded-deferred") and info is not None and info.draining is not None:
        # S-8: it reloads nothing while it drains for an upgrade; the restarted
        # daemon loads the stored key. Not a re-check.
        newest = info.deferred[1] if info.deferred is not None else key[1]
        return (
            f"{requested}; the daemon is draining for an upgrade and does not reload: "
            f"generation {newest} is applied after it restarts, not by a re-check"
        ), None
    if answer == "deferred":
        reason = (info.deferred_reason if info is not None else None) or "see the daemon log"
        return (
            f"{requested}; the daemon has not applied generation {key[1]} yet: {reason}; "
            f"it re-checks every poll ({poll} s)"
        ), None
    if answer == "superseded-deferred":
        reason = (info.deferred_reason if info is not None else None) or "see the daemon log"
        return (
            f"{requested}; superseded by generation {info.deferred[1]}, which the daemon "
            f"has not applied yet: {reason}; it re-checks every poll ({poll} s)"
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
        release_database()  # defence in depth: no pooled connection across the wait
    except Exception:  # noqa: BLE001 - never fail a committed write
        pass
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
    _flush_notice(generation, store_id)

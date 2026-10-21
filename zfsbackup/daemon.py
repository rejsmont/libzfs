#!/usr/bin/env python3
"""ZFS Backup Daemon - Main entry point."""

import signal

# FIRST, before any other import: in the daemon process itself (`python -m
# zfsbackup.daemon`), ignore SIGHUP. Its default action is terminate, and the
# imports below (SQLAlchemy, Alembic, libzfseasy, ...) plus argparse take a
# few hundred milliseconds before `_StartupHup.install()` can catch it -- so
# `systemctl reload` straight after `start`, or a CLI write racing the start,
# killed the daemon (exit 129, which `Restart=on-failure` does not restart).
# Ignoring is lossless: the startup read happens later and sees every write
# committed before it. `_StartupHup.install()` replaces this with its flag
# handler, and `_StartupHup.restore()` puts it back. Only for `__main__`, so an
# importer (the test suite, the CLI) keeps its own disposition -- and SIG_IGN,
# unlike a handler, would survive into every process it execs.
#
# Accepted residual: the interpreter's own start-up before this line (exec,
# `site`, runpy, the trivial `zfsbackup/__init__.py`; ~25 ms) cannot be closed
# from Python -- a SIGHUP there still kills the process. The systemd unit
# covers it with `RestartForceExitStatus=SIGHUP` (a SIGHUP death is
# restarted, and the restart's startup read sees every committed write).
if __name__ == '__main__':  # pragma: no cover - the real process only
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

import argparse  # noqa: E402
import errno  # noqa: E402
import logging  # noqa: E402
import multiprocessing  # noqa: E402
import os  # noqa: E402
import socket  # noqa: E402
import stat  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy.exc import DatabaseError, OperationalError

from zfsbackup.config import BackupConfig
from zfsbackup.config.model import _same
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    ConfigPathError,
    ResolvedConfigPath,
    SchemaError,
    check_config_db,
    dispose_all,
    read_store_state,
    resolve_config_path,
    validate_config,
)
from zfsbackup.runtime_config import (
    config_error_message,
    describe_config_source,
    load_runtime_config,
)
from zfsbackup import daemonlock
from zfsbackup import identity
from zfsbackup.backup_manager import DatasetManager
from zfsbackup import procident
from zfsbackup import sdnotify
from zfsbackup.stopflag import ReloadEpoch, StopFlag
from zfsbackup.workers import (
    WORKER_EXIT_RELOAD,
    ApiWorker,
    PruningWorker,
    RemoteBackupWorker,
    SnapshotWorker,
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

#: Base of the per-worker restart backoff: the delay before the n-th
#: consecutive restart is `WORKER_RESTART_DELAY * 2**(n-1)`, capped at
#: `WORKER_RESTART_MAX_DELAY`. See `BackupDaemon._record_worker_death`.
#:
#: The 300s cap is currently unreachable: with
#: `WORKER_MAX_CONSECUTIVE_FAILURES = 6` the daemon gives up after the sixth
#: death, so the largest delay ever applied is 5 * 2**4 = 80s. The cap is kept
#: deliberately, as a guard should the failure limit ever be raised.
WORKER_RESTART_DELAY = 5
WORKER_RESTART_MAX_DELAY = 300
#: A worker that stayed up at least this long (seconds) before dying is
#: considered to have recovered: its consecutive-failure count restarts at 1.
WORKER_STABLE_UPTIME = 600
#: This many consecutive deaths of one worker (none of them after a
#: `WORKER_STABLE_UPTIME` run) is fatal to the daemon: it stops and exits 1
#: instead of respawning forever.
WORKER_MAX_CONSECUTIVE_FAILURES = 6
SUPERVISOR_POLL = 5

#: What `multiprocessing.popen_forkserver.Popen.poll` reports for every worker
#: once the forkserver process is gone (it cannot read the real status).
_FORKSERVER_LOST_EXITCODE = 255

#: Longest slice `_supervisor_sleep` sleeps for before re-reading the stop
#: flag. This bounds one thing only: how long the supervisor stays in its
#: idle sleep after the flag is raised. It is **not** the daemon's
#: signal-to-shutdown latency, and `TimeoutStopSec` must not be sized from
#: it. The real worst case is the sum of three terms:
#:
#:   1. this slice, 0.25s, if the signal lands in the idle sleep;
#:   2. nothing from the restart path: `_check_workers` no longer sleeps.
#:      A crashed worker's restart is a per-worker `restart_at` deadline
#:      checked once per poll, so backoff can never delay a shutdown;
#:   3. `_shutdown_workers`' escalation, up to `timeout` + 5s terminate-join
#:      + 5s kill-join per worker that will not exit. The 30s budget is now
#:      a single deadline shared by all workers rather than per-worker, so
#:      four workers stuck in a long `zfs send` cost up to 30 + 4x10 = 70s
#:      there, not 4x40 = 160s.
#:
#: So roughly 0.25 + 70 ~= 70s worst case, under systemd's default
#: `TimeoutStopSec=90s`. On the healthy path -- workers parked in
#: `stop_event.wait()` -- the tail is one worker's `stopflag.STOP_POLL`
#: slice, not this one; measured end-to-end shutdowns land at 0.265-0.554s.
#:
#: A fourth term since live reload (item 17.2): a SIGTERM that lands while
#: the supervisor is inside `_reload_tick` waits for that tick to finish. That
#: tick is not only a SQLite read (up to `busy_timeout`, 5s): a candidate is
#: also run through the B3 checks -- `getaddrinfo` for an `api_host` given by
#: name (resolver timeout x retries; why it should be an address) and the
#: client TLS identity files the remote-backup advisory reads -- on every
#: reload, and on every poll while a deferred reload is pending; and `notify.stopping()` may block up to its 1s send timeout. None
#: of it is bounded by `busy_timeout` alone, so the term is the sum, with no
#: hard cap. The supervisor itself makes **no `zfs` call**
#: (B1): the existence check and the property sync run in
#: `SnapshotWorker._before_loop`, where a hung pool stalls only that worker
#: (stop/RELOAD end its sync at the next dataset, and `_shutdown_workers`
#: escalates past it). Nothing is forked after the flag is raised, so the term
#: only delays the start of `_shutdown_workers`; size `TimeoutStopSec` with it.
SUPERVISOR_SLEEP_SLICE = 0.25

#: How often the supervisor checks the config database for a committed write
#: it was not signalled about (decision D2). Shared with the CLI's wording.
RELOAD_POLL_SECONDS = daemonlock.RELOAD_POLL_SECONDS
#: A worker still finishing work begun under an older generation is reported
#: at WARNING once this long after the reload was accepted (decision D5: it is
#: never killed for a reload) ...
RELOAD_DEFER_NOTICE = 60
#: ... and again every this many seconds for as long as it keeps going.
RELOAD_DEFER_REPEAT = 900


@dataclass(frozen=True)
class Handoff:
    """What every worker is born with, as one object: the config it runs, the
    reload epoch that config belongs to, the `(store_id, generation)` key
    it was read at (`None` when not known -- a daemon built without one, see
    `BackupDaemon.__init__`), and this host's client id (`ConfigSnapshot.
    client_id`, read in the same snapshot as the config; `None` until
    `zfsbackup-config enroll` generates one).

    The supervisor replaces the whole object on an accepted reload and only
    then bumps the shared `ReloadEpoch`; `_new_worker` reads it exactly once,
    so a worker can never be given one generation's config with another
    generation's `born_epoch` -- the pairing that would make it either never
    switch or switch forever -- nor one generation's config with another's
    client id. The client id is not part of `BackupConfig` (it is host
    identity, not configuration: never imported or exported), so a reload
    compares it separately (`_try_reload`); a changed id is a real switch.
    """

    config: BackupConfig
    epoch: int
    key: Optional[Tuple[int, int]]
    client_id: Optional[str] = None


def _gen(key: Optional[Tuple[int, int]]) -> str:
    """The generation half of a reload key, for log lines."""
    return "unknown" if key is None else str(key[1])


#: Config-load failures that are already diagnosed by `config_error_message`
#: and are logged without a traceback. Anything else -- including a bare
#: `RuntimeError` such as `MigrationPackagingError` -- is a bug and is logged
#: with one. `OperationalError` is a `DatabaseError` subclass.
_EXPECTED_CONFIG_ERRORS = (ConfigPathError, SchemaError, ValueError, DatabaseError)


def _build_manager(config: BackupConfig) -> DatasetManager:
    """Build the `DatasetManager` and its dataset list for `config` exactly as
    every worker does first thing in `run()`, so a config no worker could
    start with is refused by the supervisor -- before any worker gets it --
    instead of crash-looping every worker. Pure: no `zfs` call. Raises what
    the build raises (`ValueError` for a name `libzfseasy` refuses, e.g.
    `pool/has space`)."""
    manager = DatasetManager(config)
    manager.datasets  # noqa: B018 -- the property builds and validates the list
    return manager


def _remote_worker_needed(config: BackupConfig) -> bool:
    """Whether `config` runs the remote backup worker (some enabled dataset
    has a `remote:` destination). The one definition behind
    `BackupDaemon._active_worker_names` and the pre-acceptance checks."""
    return any(ds.remote for ds in config.enabled_datasets)


class _BindProblem(str):
    """Why a bind probe failed (the message), plus `busy`: `True` only for
    `EADDRINUSE`, the one failure that can clear by itself (the port is held
    by something that will exit). Everything else -- an address that is not
    ours (`EADDRNOTAVAIL`), a privileged port (`EACCES`), a name that does not
    resolve (`gaierror`), a malformed host (`ValueError`/`UnicodeError`) --
    is the operator's to fix and is rejected for good, not re-probed (a
    re-resolve of a dead name would stall the supervisor loop at every
    poll). A `str` subclass so plain-string consumers keep working; a plain
    `str` counts as not busy."""

    busy: bool = False

    def __new__(cls, message: str, busy: bool = False):
        obj = super().__new__(cls, message)
        obj.busy = busy
        return obj


class _Refusal(str):
    """A `_candidate_refusal` result: the message, plus `deferrable` -- every
    problem in it can clear without any write (`_BindProblem.busy`)."""

    deferrable: bool = False

    def __new__(cls, message: str, deferrable: bool = False):
        obj = super().__new__(cls, message)
        obj.deferrable = deferrable
        return obj


def _probe_api_bind(host: str, port: int) -> Optional[str]:
    """Try to bind `(host, port)` the way the API worker will; return the
    reason it cannot (a `_BindProblem`; `.busy` only for "address in use"),
    or `None`. The socket is closed before returning, in a
    `finally`, so the supervisor never carries a probe descriptor into a fork
    (also `SOCK_CLOEXEC` where the platform has it).

    The address family and sockaddr come from Werkzeug's own helpers, the ones
    `make_server` uses, so a host the server would bind (IPv6 literal, name)
    is probed identically. `SO_REUSEADDR` matches `http.server.HTTPServer`.
    Bind only, no `listen`: that already fails for a port another socket
    holds. A `unix://` host is not probed (Werkzeug unlinks a stale path
    before binding it).

    **Remaining gap**: the port can be taken between this probe and the new
    API worker's own bind (or a name can resolve differently); that worker
    then exits 1 and the supervisor's restart backoff applies (D9). The same
    holds for a host-only change on a port the running API worker holds: the
    reload probes `(new_host, 0)` (see `_candidate_refusal`), so a third
    party already holding `new_host:port` is not detected here either. A host
    given by name is resolved here, in the supervisor, so a hung resolver
    would stall it -- the same exposure the worker's bind has, and why
    `api_host` should be an address.
    """
    from werkzeug.serving import get_sockaddr, select_address_family

    try:
        family = select_address_family(host, port)
    except (OSError, ValueError) as e:
        return _BindProblem(str(e))
    if family == getattr(socket, 'AF_UNIX', None):
        return None
    sock = None
    try:
        sockaddr = get_sockaddr(host, int(port), family)
        sock = socket.socket(
            family, socket.SOCK_STREAM | getattr(socket, 'SOCK_CLOEXEC', 0)
        )
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(sockaddr)
    except (OSError, ValueError) as e:
        # `ValueError` includes `UnicodeError`: an IDNA label over 63
        # characters, an empty label -- what a bind of that name would hit.
        return _BindProblem(
            str(e), busy=isinstance(e, OSError) and e.errno == errno.EADDRINUSE
        )
    finally:
        if sock is not None:
            sock.close()
    return None


#: A client certificate expiring within this many days is warned about
#: (`_client_tls_problem`).
CLIENT_CERT_EXPIRY_WARN_DAYS = 30


def _tls_dir(resolved) -> Optional[Path]:
    """`<db>.tls` for a `ResolvedConfigPath` (or a plain path); `None` for
    `None`. Derived from the *resolved* database path, like
    `daemonlock.workers_lock_path`, so a symlinked `-c` and its target name
    the same directory."""
    if resolved is None:
        return None
    return identity.tls_dir(getattr(resolved, 'resolved_path', resolved))


def _client_identity_paths(config: BackupConfig, tls: Optional[Path]):
    """`(cert, key, configured)`: the client TLS identity the remote worker
    presents -- `client_cert_file`/`client_key_file` when set (both or
    neither, `validate_config`), else `<db>.tls/client.{crt,key}`."""
    cert = getattr(config, 'client_cert_file', None)
    key = getattr(config, 'client_key_file', None)
    if cert is not None and key is not None:
        return Path(cert), Path(key), True
    if tls is None:
        return None, None, False
    cert, key = identity.identity_paths(tls, identity.CLIENT_NAME)
    return cert, key, False


def _not_regular(*paths) -> Optional[str]:
    """A message if one of `paths` exists but is not a regular file (after
    following symlinks), else `None`. Checked BEFORE `identity` opens them:
    its reads use a blocking `open`, and a FIFO there (an operator-supplied
    `client_cert_file`, say) would hang the single-threaded supervisor with
    SIGTERM unanswered (PEP 475 retries the interrupted `open`) -- the hazard
    the old client-id-file reader guarded against. A path swapped between
    this `stat` and the `open` is not covered; nor is a hung network
    filesystem."""
    for path in paths:
        try:
            mode = os.stat(path).st_mode
        except OSError:
            continue  # missing or unreadable: the loader says which
        if not stat.S_ISREG(mode):
            return f"{path} is not a regular file"
    return None


#: How long `_client_tls_problem` waits for `<db>.tls/identity.lock` (held by
#: a CLI creating or promoting an identity) before reporting the identity as
#: unusable for now. Advisory only, so it must not stall the supervisor.
ADVISORY_IDENTITY_LOCK_TIMEOUT = 0.5


def _remote_destinations(config: BackupConfig) -> List[str]:
    """The destination names the enabled datasets send to, sorted."""
    return sorted({
        r.destination for ds in config.enabled_datasets for r in (ds.remote or [])
    })


def _client_tls_problem(
    config: BackupConfig,
    client_id: Optional[str],
    tls: Optional[Path],
    *,
    now: Optional[datetime] = None,
) -> Tuple[List[str], Optional['identity.Identity']]:
    """Why remote backups may not work for `config` and this host's client
    id, as advisory messages, plus the client identity when it loaded.
    Never a refusal, never creates anything: remote backup is the only thing
    affected -- the remote worker skips a destination it cannot use (M11) --
    so callers WARN and accept the configuration. `([], None)` when no
    enabled dataset has a remote destination.

    Checked: a client id exists (`enroll` generates it); the client
    certificate and key (`_client_identity_paths`) load as a `clientAuth`
    identity -- readable, matching, unexpired, usable EKU -- and do not
    expire within `CLIENT_CERT_EXPIRY_WARN_DAYS`; every destination in use
    is enrolled (has a `server_fingerprint`). The daemon runs as root, so
    the key is read with `load_identity`; a non-root `--test-config` may
    report it unreadable."""
    if not _remote_worker_needed(config):
        return [], None
    problems: List[str] = []
    if client_id is None:
        problems.append(
            "this host has no client id yet; run `zfsbackup-config enroll "
            "DESTINATION --url https://... --token-file FILE` (it generates the "
            "id and the client certificate)"
        )
    loaded = None
    cert, key, configured = _client_identity_paths(config, tls)
    if cert is None:
        problems.append("the client TLS identity cannot be located (no config database path)")
    elif not configured and not os.path.lexists(cert) and not os.path.lexists(key):
        problems.append(
            f"there is no client TLS identity in {cert.parent} yet; "
            "`zfsbackup-config enroll` creates it"
        )
    elif (odd := _not_regular(cert, key)) is not None:
        problems.append(f"the client TLS identity cannot be used: {odd}")
    else:
        try:
            # Advisory, and run in the supervisor's loop: never wait out a
            # CLI `identity rotate` holding `identity.lock` (up to 30 s).
            loaded = identity.load_identity(
                cert, key, identity.ROLE_CLIENT, now=now,
                lock_timeout=ADVISORY_IDENTITY_LOCK_TIMEOUT,
            )
        except identity.IdentityError as e:
            problems.append(f"the client TLS identity cannot be used: {e}")
        else:
            moment = now or datetime.now(timezone.utc)
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            left = loaded.not_after - moment
            if left < timedelta(days=CLIENT_CERT_EXPIRY_WARN_DAYS):
                problems.append(
                    f"the client certificate {cert} expires on "
                    f"{loaded.not_after:%Y-%m-%d %H:%M} UTC (in {max(left.days, 0)} "
                    "day(s)); rotate it: `zfsbackup-config identity rotate`"
                )
    unenrolled = [
        name for name in _remote_destinations(config)
        if (dest := config.destinations.get(name)) is not None
        and getattr(dest, 'server_fingerprint', None) is None
    ]
    if unenrolled:
        problems.append(
            f"destination(s) {', '.join(unenrolled)} not enrolled (no "
            "server_fingerprint), so backups to them are skipped; run "
            "`zfsbackup-config enroll DESTINATION ...` for each"
        )
    return problems, loaded


def _tls_inputs(config: BackupConfig, client_id: Optional[str]) -> tuple:
    """What `_client_tls_problem`'s verdict depends on in the configuration,
    so a reload re-checks only when one of these changed."""
    return (
        _remote_worker_needed(config),
        client_id,
        getattr(config, 'client_cert_file', None),
        getattr(config, 'client_key_file', None),
        tuple(
            (name, getattr(config.destinations.get(name), 'server_fingerprint', None))
            for name in _remote_destinations(config)
        ),
    )


def _candidate_refusal(
    config: BackupConfig,
    previous: Optional[BackupConfig] = None,
    *,
    probe_api: bool = True,
    live_api: Sequence[Tuple[str, int]] = (),
) -> Optional[str]:
    """Why the API worker could not start with `config`, or `None`: it binds
    its address first and would otherwise crash-loop on it (D9). The only
    refusal left; a remote-backup problem is advisory (`_candidate_warnings`).
    The result is a `_Refusal`: `.deferrable` is true only when the address
    is merely IN USE (`EADDRINUSE`, which can clear by itself); an address
    that cannot exist here, a privileged port or a name that does not resolve
    is final.

    `previous` is the running config of a reload: only what changed is
    checked. `live_api` is every `(host, port)` a live API worker -- of any
    epoch, a draining one included -- is bound to, because the running config
    says what the NEXT API worker will bind, not what is bound now. The
    candidate's API address is checked only when it differs from `previous`'s
    (port 0 means "any free port"):

    - an address a live API worker already holds is not probed: reverting a
      change the old worker never gave up must not collide with that worker's
      own socket;
    - the same nonzero port on another host (a host-only change) is probed
      as `(host, 0)`: the port is held by the old worker until the turnover,
      but an address that does not exist here or a name that does not
      resolve is caught;
    - anything else is bound for real.

    `previous=None` (startup) checks everything. Creates nothing."""
    problems = []
    deferrable = True
    addr = (config.api_host, config.api_port)
    if (
        probe_api
        and (previous is None or addr != (previous.api_host, previous.api_port))
        and not (config.api_port != 0 and addr in live_api)
    ):
        port = config.api_port
        held = port != 0 and any(p == port for _, p in live_api)
        err = _probe_api_bind(config.api_host, 0 if held else port)
        if err is not None:
            busy = bool(getattr(err, 'busy', False))
            tail = ""
            if previous is not None:
                tail = "; the API keeps listening on its old address"
                if not busy:
                    tail += (
                        "; this is not retried until the configuration "
                        "changes (fix api_host/api_port)"
                    )
            problems.append(
                f"api_host:api_port {config.api_host}:{port} "
                + ("is in use" if busy else "cannot be bound")
                + f": {err}{tail}"
            )
            deferrable = deferrable and busy

    if not problems:
        return None
    return _Refusal("\n".join(problems), deferrable=deferrable)


def _candidate_warnings(
    config: BackupConfig,
    previous: Optional[BackupConfig] = None,
    *,
    client_id: Optional[str] = None,
    previous_client_id: Optional[str] = None,
    tls: Optional[Path] = None,
) -> List[str]:
    """Advisory findings about `config` and the client id read with it:
    never a refusal. Today: whether remote backups can work
    (`_client_tls_problem` -- client id, client certificate, enrolled
    destinations). Evaluated at startup (`previous=None`) and on a reload
    only when one of its inputs changed (`_tls_inputs`), so an unchanged
    problem is not repeated on every `set`. That breaks remote backup only
    -- the remote worker skips what it cannot use -- so the candidate is
    accepted, with a WARNING."""
    if previous is not None and (
        _tls_inputs(config, client_id) == _tls_inputs(previous, previous_client_id)
    ):
        return []
    problems, _ = _client_tls_problem(config, client_id, tls)
    return [
        f"remote backup: {problem}; local snapshots and pruning are unaffected"
        for problem in problems
    ]


#: Top-level `BackupConfig` settings compared by a reload's summary line. The
#: client id is not a `BackupConfig` setting; `_try_reload` adds `client_id`
#: to the summary itself when it changed.
_SETTINGS = (
    'snapshot_prefix', 'check_interval', 'prune_interval', 'api_host',
    'api_port', 'dry_run', 'destinations', 'remote_backup',
    'client_cert_file', 'client_key_file',
)


def _changed_settings(old: BackupConfig, new: BackupConfig) -> List[str]:
    """Names of the top-level settings that differ between two configs."""
    # `_same`, not `!=`: a literal-only edit (`1h` -> `60m`) is a difference
    # (it turns workers over, B2) that `==` on a `Duration` does not see.
    return [
        n for n in _SETTINGS
        if not _same(getattr(old, n, None), getattr(new, n, None))
    ]


def _describe_dataset(ds) -> str:
    return (
        f"frequency {ds.frequency}, recursive {ds.recursive}, "
        f"{len(ds.retention_rules)} retention rule(s), "
        f"{len(getattr(ds, 'remote', None) or [])} remote(s)"
    )


def _signal_verified(pid: int, token: Optional[str], sig: int) -> str:
    """Signal `pid` only if it is still the process that had start token
    `token`. Returns `'sent'`, `'gone'` (no such process, a zombie, or a
    reused pid), `'unverified'` (identity cannot be established: nothing was
    sent) or `'error'`.

    Where the platform has pidfds (Linux 5.3+, Python 3.9+) the descriptor is
    opened FIRST, the identity checked, and the signal sent through the
    descriptor: it names the process that held the pid at open time, so a pid
    recycled between the check and the kill (an orphan reaped by init) can
    never be hit. Elsewhere it is check-then-`kill`, with the race that
    implies."""
    if not token or not isinstance(pid, int) or pid <= 1:
        return 'unverified'
    pidfd = None
    opener = getattr(os, 'pidfd_open', None)
    sender = getattr(signal, 'pidfd_send_signal', None)
    if opener is not None and sender is not None:
        try:
            pidfd = opener(pid)
        except ProcessLookupError:
            return 'gone'
        except OSError:
            pidfd = None  # not permitted / not supported: plain path below
    try:
        ident = procident.identity(pid, token)
        if ident in ('gone', 'mismatch'):
            return 'gone'
        if ident != 'match':
            return 'unverified'
        try:
            if pidfd is not None:
                sender(pidfd, sig)
            else:
                os.kill(pid, sig)
        except ProcessLookupError:
            return 'gone'
        except OSError:
            return 'error'
        return 'sent'
    finally:
        if pidfd is not None:
            os.close(pidfd)


class BackupDaemon:
    """Supervisor process: starts, monitors and reconfigures the snapshot,
    pruning, API and remote workers.

    One main thread, single-threaded by design: the signal handlers only
    store flags, and every decision -- restart, reload, shutdown -- is taken
    by `run()`'s poll loop (`_supervisor_sleep` -> `_reload_tick` ->
    `_check_workers`, with the stop flag re-checked between steps).

    **Live reload (item 17.2).** Two triggers feed one code path: SIGHUP
    (`zfsbackup-config` sends it after every committed write, once it has
    verified this process) and a `RELOAD_POLL_SECONDS` poll of the store's
    `(store_id, generation)` key. A changed key is read in full, validated
    and, if accepted, becomes the new `Handoff`; the shared `ReloadEpoch` is
    then bumped, and each worker leaves at its own next safe point with
    `WORKER_EXIT_RELOAD` and is respawned on the new config by
    `_check_workers` (a staggered turnover; in-flight work is never killed).
    A rejected candidate leaves every worker running the old generation.
    Progress is published in the presence lock's pid file
    (`applied=`/`target=`/`rejected=`).
    """

    def __init__(
        self,
        config: BackupConfig,
        resolved_config: ResolvedConfigPath,
        verbose: bool = False,
        *,
        key: Optional[Tuple[int, int]] = None,
        presence_lock=None,
        dry_run_override: bool = False,
        workers_lock=None,
        notifier: Optional[sdnotify.Notifier] = None,
        client_id: Optional[str] = None,
    ):
        """`key` is the `(store_id, generation)` that `config` was read at,
        and `client_id` this host's client id from the same snapshot
        (`ConfigSnapshot.client_id`; handed to the remote worker).
        `_run_startup` always passes it. A daemon built without one (unit
        tests, embedded use) cannot tell a changed store from its own, so
        live reload is off for it: `_reload_tick` consumes a SIGHUP, logs
        that it was ignored and never reads the database.

        `presence_lock` is the held `daemonlock.DaemonLock` whose pid file
        records reload progress (`None` = record nothing).
        `dry_run_override` is `--dry-run`, re-applied to every config a
        reload reads. `workers_lock` is the `<db>.workers.lock` path every
        worker holds `LOCK_SH` on (default: derived from `resolved_config`).
        `notifier` reports readiness to systemd (`zfsbackup.sdnotify`); the
        default is a no-op, and `_run_startup` passes the one it built from
        `$NOTIFY_SOCKET`. Only `run()` and `_reload_tick` use it -- never a
        signal handler.
        """
        #: Config, epoch, key and client id, swapped as one object by
        #: `_try_reload` and read once per spawn by `_new_worker`. See
        #: `Handoff`.
        self._handoff = Handoff(config, 0, key, client_id)
        #: The config *database* path, with its provenance. Read by the
        #: supervisor: at startup, by every poll and by every reload
        #: (`_reload_tick`). The API worker is given it for TLS only (the
        #: trust tables, through `store.trust`; never the configuration). Renamed from
        #: `config_path` rather than merely retyped, so a bare `Path` cannot
        #: sneak back in unnoticed.
        self.resolved_config = resolved_config
        self.verbose = verbose
        self._presence_lock = presence_lock
        self._dry_run_override = dry_run_override
        self._workers_lock = workers_lock
        self._notify = notifier if notifier is not None else sdnotify.Notifier()
        #: The last STATUS= text sent, so the 10 s poll sends one only when
        #: it changed.
        self._status_sent: Optional[str] = None
        #: Not a `multiprocessing.Event`. `Event.set()` blocks forever --
        #: holding the Condition's lock, which then also freezes every
        #: `is_set()` in every process sharing it -- once any worker has
        #: been killed while parked in `wait()`, which is routine here
        #: (`_shutdown_workers` escalates to SIGKILL, and `SIG_DFL`
        #: children die abruptly on SIGTERM/SIGINT). See
        #: `zfsbackup.stopflag` for the mechanism and the measurements.
        self._stop_event = StopFlag()
        #: The reload epoch shared with every worker (`zfsbackup.stopflag`):
        #: created once, never recreated, never touched from a signal
        #: handler. Bumped by `_try_reload` only, always AFTER the matching
        #: `Handoff` is in place; always equal to `self._handoff.epoch`
        #: once that call returns. A supervisor-local counter, never the DB
        #: generation.
        self._reload_epoch = ReloadEpoch()
        self._workers: Dict[str, multiprocessing.Process] = {}
        #: The epoch each live (or not yet classified) worker was born under,
        #: and the generation of that epoch's config (for log lines only).
        #: Set by `_spawn`, dropped with the rest of a worker's bookkeeping.
        self._worker_epoch: Dict[str, int] = {}
        self._worker_generation: Dict[str, str] = {}
        #: The last key a reload refused for good; the poll does not retry it
        #: (a SIGHUP does). Published as `rejected=`, which is final.
        #: `_rejection_logged` dedupes the ERROR / WARNING line by
        #: `(key, message)`.
        self._rejected: Optional[Tuple[int, int]] = None
        #: The key refused for a reason that can pass later with the stored
        #: config unchanged (B3: the API address is busy): the poll
        #: re-evaluates it at every poll. Published as `deferred=` (plus a
        #: one-line `deferred_reason=`), never as `rejected=`; at most one of
        #: `_rejected` / `_deferred` is set. Cleared on acceptance, when a
        #: newer key supersedes it, or when it becomes a final rejection.
        self._deferred: Optional[Tuple[int, int]] = None
        self._deferred_reason: Optional[str] = None
        #: `(key, message)` of every advisory WARNING (`_candidate_warnings`)
        #: logged for the candidate being evaluated, so a candidate that is
        #: re-evaluated (a deferred key, at every poll) warns once.
        self._warned: Set[tuple] = set()
        self._rejection_logged: Optional[tuple] = None
        #: Poll-only marker: the stored key a poll saw when the full read it
        #: triggered failed before its own key was known (a stale schema, a
        #: damaged file, an unloadable row). The poll skips that key until it
        #: changes, so a broken store is not fully re-read every
        #: `RELOAD_POLL_SECONDS`. Never published as `rejected=`, never set
        #: for a transient `OperationalError` (locked/busy) or a
        #: `SchemaError` (stale schema: the check is cheap, and `migrate`
        #: changes no key) -- the next poll retries those -- and a SIGHUP
        #: still forces a read.
        self._poll_skip: Optional[Tuple[int, int]] = None
        #: The epoch every live worker has switched to; equal to the current
        #: epoch when no turnover is in progress.
        self._applied_epoch = 0
        #: Wall-clock "HH:MM:SS" of the last accepted reload, for the D9
        #: crash-loop message.
        self._accepted_at: Optional[str] = None
        self._next_poll_at = self._monotonic() + RELOAD_POLL_SECONDS
        #: `_monotonic()` when the current reload was accepted; `None` before
        #: the first one. Drives the deferral WARNINGs.
        self._reload_started_at: Optional[float] = None
        self._defer_logged_at: Dict[str, float] = {}
        #: The operator message of the poll failure currently in effect, or
        #: `None` when the last poll succeeded. See `_note_poll_problem`.
        self._poll_problem: Optional[str] = None
        #: Workers that crashed `WORKER_MAX_CONSECUTIVE_FAILURES` times in a
        #: row and will never be respawned. Any entry stops the daemon.
        self._crash_looped: Set[str] = set()
        #: Per-worker restart bookkeeping, all on the `time.monotonic()`
        #: clock and all touched only by the supervisor's main thread.
        #: `_restart_at[name]` exists exactly while `name` is dead and
        #: waiting out its backoff; `_started_at[name]` is when the current
        #: incarnation was forked; `_failures[name]` counts consecutive
        #: deaths (reset by a run of >= `WORKER_STABLE_UPTIME`).
        self._restart_at: Dict[str, float] = {}
        self._started_at: Dict[str, float] = {}
        self._failures: Dict[str, int] = {}
        #: Forkserver orphans. Under `forkserver` (CPython 3.14: the Linux
        #: default) `Popen.poll()` answers 255 for EVERY worker once the
        #: forkserver process is gone, although the workers are alive and
        #: running (they leave at their next safe point -- possibly hours into
        #: a `zfs send`). `_worker_token[name]` is the worker's
        #: `procident` start token, recorded by `_spawn` (only under
        #: `forkserver`); `_lost` holds the workers `_check_workers` found
        #: "dead" by `poll()` whose identity still matches. A lost worker
        #: stays in `_workers`, counts no failure (D9), is never respawned
        #: while its pid is alive, and is replaced at once -- no backoff --
        #: when it has exited.
        self._worker_token: Dict[str, Optional[str]] = {}
        self._lost: Set[str] = set()
        #: `(pid, start token)` of the forkserver that forked each worker,
        #: recorded by `_spawn`. `poll() == 255` while that forkserver is no
        #: longer the same live process means "orphaned, outcome unknown",
        #: not "crashed" -- even when the worker has meanwhile exited (an
        #: idle orphan leaves within one `STOP_POLL` slice, long before the
        #: next 5 s `_check_workers`). An exit code 255 beside a healthy
        #: forkserver is a genuine crash.
        self._worker_fs: Dict[str, Optional[Tuple[int, Optional[str]]]] = {}
        #: `(api_host, api_port)` each worker was forked with (the address an
        #: `api` worker binds). `_try_reload` must not probe an address a
        #: live, possibly draining, API worker already holds.
        self._worker_addr: Dict[str, Tuple[str, int]] = {}
        #: Workers that survived SIGKILL and were left behind by
        #: `_shutdown_workers`. `main()` hard-exits when this is non-empty:
        #: multiprocessing's atexit handler would otherwise `join()` them
        #: with no timeout.
        self.abandoned_workers: List[str] = []
        #: Set by `_sighup_handler` (one plain store); makes
        #: `_supervisor_sleep` return early and is consumed -- cleared before
        #: the read it forces -- by `_reload_tick`.
        self._hup_requested = False
        #: Set by `_signal_handler` (one plain store), read and logged by
        #: `run()`. `None` means the flag was raised by `_check_workers`,
        #: not by a signal.
        self._stop_signum = None
        self._stop_reported = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGHUP, self._sighup_handler)

    def _sighup_handler(self, signum, frame):
        """Record that SIGHUP arrived and return -- nothing else.

        SIGHUP means "the configuration database was written; apply it now"
        (`zfsbackup-config` sends it after every committed write). The
        handler is one plain store. `_supervisor_sleep` notices the flag
        within one `SUPERVISOR_SLEEP_SLICE` and returns, and `_reload_tick`
        clears it and forces a full read on the main thread. Same rules as
        `_signal_handler`: no logging, no lock, and above all **no database
        access** -- `db._ENGINES_LOCK` is an RLock, so a handler that
        interrupts the main thread while it holds the lock would not
        deadlock; it would re-enter and see `_ENGINES` half-mutated. Its
        default action (terminate) would orphan every worker, which is the
        other reason it is handled at all.
        """
        self._hup_requested = True

    def _signal_handler(self, signum, frame):
        """Record the request and return; `run()`'s poll loop acts on it.

        Two stores, and **deliberately no logging**. Both are single stores
        into memory -- no lock, no condition variable, nothing that can be
        half-done when the handler returns to whatever it interrupted. The
        operator-facing "Received SIGTERM" line is emitted by
        `_report_stop_signal` from the poll loop instead, where taking a
        lock is legal.

        **A signal handler must not call anything that takes a lock.** A
        handler runs on the main thread at an arbitrary instruction,
        including one inside a critical section that thread already owns.
        The rule the `zfsbackup.stopflag` module docstring states for
        `Event.set()` applies verbatim to `logger.info()`, and this handler
        used to do exactly that, *before* raising the flag:

        - **Blocking I/O.** `logger.info` takes `logging._lock` and the
          handler's `Handler.lock`, then performs a blocking `write()` to
          fd 1 -- under systemd, a journald socket. A stalled journald or a
          full pipe buffer blocks that write, so `set()` is never reached.
          Measured with nothing changed but the statement order, against a
          supervisor whose stdout pipe was full:

              ORDER=log-first  ->  WORKER NEVER SAW STOP
              ORDER=set-first  ->  WORKER SAW STOP

          The daemon then ignores SIGTERM *and* every worker keeps running,
          so systemd SIGKILLs the whole cgroup at `TimeoutStopSec` --
          tearing `RemoteBackupWorker` out of a `zfs send`. Setting the
          flag first does not unblock the supervisor (it stays in the
          handler until the write drains) but it does let every worker
          reach a safe point and exit, which is the difference between a
          clean stop and a torn stream.

        - **Re-entrancy.** If a signal lands while the main thread is
          already inside `logging` -- `_spawn`'s `logger.info`,
          `_check_workers`' `logger.error` -- the handler re-enters
          `logging` on that same thread. With today's `StreamHandler` this
          only interleaves output, because `logging._lock` and
          `Handler.lock` are `RLock`s. Add a `SysLogHandler`,
          `SocketHandler` or `QueueHandler` and it becomes a hard
          self-deadlock, with zero dead workers and on every start method.

        `self._stop_event.set()` itself is a `StopFlag` store for the same
        reason at one more remove: `Event.set()` is `with self._cond:`
        around `notify_all()`, a non-recursive lock plus a primitive that
        is not async-signal-safe, so it was never legal in a handler even
        with zero dead sleepers.
        """
        self._stop_signum = signum
        self._stop_event.set()

    def _report_stop_signal(self) -> None:
        """Log the shutdown line `_signal_handler` must not log itself.

        Called from `run()` as soon as the poll loop observes the flag, so
        the message still appears exactly once and in the same place in the
        log as before -- just from a context where blocking on a journald
        socket is harmless. Silent when the flag was raised by
        `_check_workers` (a configuration fault), which logs its own
        explanation.
        """
        signum = self._stop_signum
        if signum is None or self._stop_reported:
            return
        self._stop_reported = True
        sig_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        logger.info(f"Received {sig_name}, shutting down gracefully...")

    @property
    def config(self) -> BackupConfig:
        """The config new workers are given: the current `Handoff`'s.
        Read-only -- a reload replaces the whole `Handoff`, never this alone
        (see `_try_reload`)."""
        return self._handoff.config

    def _new_worker(self, name: str) -> multiprocessing.Process:
        cls = {
            'snapshot': SnapshotWorker,
            'pruning': PruningWorker,
            'api': ApiWorker,
            'remote': RemoteBackupWorker,
        }[name]
        # ONE read of the hand-over: config and `born_epoch` come from the
        # same `Handoff`, never `self.config` plus a separate
        # `self._reload_epoch.get()` (a reload between the two would pair a
        # config with the wrong epoch). Never a path to re-read: under `fork`
        # the config is inherited; under `spawn` and `forkserver` it is
        # pickled, and `Duration.__reduce__` carries the operator's literals
        # across, so a respawned worker sees exactly the config its siblings
        # on the same epoch run. `dry_run` is already applied to it.
        #
        # The client id rides the same `Handoff` (a changed id is a reload,
        # never a separate value read here). The API worker gets the database
        # path for its TLS identity and trust view only; the remote worker
        # gets the TLS directory (its default client identity) and the id.
        h = self._handoff
        extra = {}
        if name == 'api':
            extra['config_db'] = self.resolved_config
        elif name == 'remote':
            extra['tls_dir'] = _tls_dir(self.resolved_config)
            extra['client_id'] = h.client_id
        return cls(
            h.config,
            self._stop_event,
            self.verbose,
            reload_epoch=self._reload_epoch,
            born_epoch=h.epoch,
            workers_lock=self._workers_lock_path(),
            **extra,
        )

    def _workers_lock_path(self):
        """Path of `<db>.workers.lock` for every worker to hold `LOCK_SH` on
        (item 18.5): the one `_run_startup` passed, else derived from the
        database path, else `None`. A path, never an open descriptor: the
        supervisor keeps no `workers.lock` fd across a fork -- each worker
        opens its own, `O_CLOEXEC`."""
        if self._workers_lock is not None:
            return self._workers_lock
        if self.resolved_config is None:
            return None
        return daemonlock.workers_lock_path(self.resolved_config)

    def _active_worker_names(self) -> list:
        names = ['snapshot', 'pruning', 'api']
        if _remote_worker_needed(self.config):
            names.append('remote')
        return names

    def _spawn(self, name: str, verb: str = 'Started') -> multiprocessing.Process:
        """The **single fork point** for every worker, at startup and at
        every crash restart alike.

        `dispose_all()` first, always, and it is **mandatory**, not
        belt-and-braces. The supervisor read the config database before the
        first fork, and SQLAlchemy 2.0's default `QueuePool` *retains* the
        connection it used -- so without this the child would inherit a live
        SQLite file descriptor on the config database. Workers never use
        it, but an inherited descriptor is not harmless merely because it is
        unused: the child's garbage collector may finalise the unreferenced
        pooled connection, and SQLite's close of a connection it believes is
        the last one checkpoints and deletes the `-wal`/`-shm` sidecars out
        from under the parent, while its per-process inode lock bookkeeping
        no longer matches the kernel's. `store/db.py`'s pid-keyed engine
        cache alone is **not** a guarantee: it hands a child a fresh engine,
        but the inherited connection is merely unreferenced, not closed.
        Only `dispose_all()` at the fork point, in the process that owns the
        connections, closes them before the fork.

        This is a choke point on purpose. `_check_workers` re-forks on
        every worker crash, arbitrarily late in the daemon's life; an
        earlier version of this design cleaned up only in
        `_start_workers`, which left exactly that path uncovered.

        The supervisor must be quiescent here: `dispose_all()` is
        pid-guarded and cannot help a connection that is *checked out*, so
        no session may be open across this call. Every database read the
        supervisor makes (`_run_startup`, `_reload_tick`, `_try_reload`)
        disposes in a `finally` straight after it, so none is.

        The worker's epoch is recorded from the same `Handoff` `_new_worker`
        reads: only this (main) thread ever replaces `self._handoff`, and
        not between these two lines.
        """
        dispose_all()
        h = self._handoff
        worker = self._new_worker(name)
        worker.start()
        # Recorded straight after `start()`, before anything that can raise
        # (the log line): an untracked live child would never be joined or
        # terminated at shutdown.
        self._workers[name] = worker
        self._worker_epoch[name] = h.epoch
        self._worker_generation[name] = _gen(h.key)
        self._started_at[name] = self._monotonic()
        self._lost.discard(name)
        self._worker_addr[name] = (h.config.api_host, h.config.api_port)
        if self._tracks_orphans():
            self._worker_token[name] = procident.start_token(worker.pid)
            self._worker_fs[name] = self._current_forkserver()
        else:
            self._worker_token[name] = None
            self._worker_fs[name] = None
        on = f" on generation {_gen(h.key)}" if verb == 'Reloaded' else ""
        logger.info(f"{verb} {name} worker (pid={worker.pid}){on}")
        self._restart_at.pop(name, None)
        return worker

    def _forget_worker(self, name: str, *, keep_failures: bool = True) -> None:
        """Drop the bookkeeping of a worker that is gone for good (a turnover
        about to be respawned, or a worker the new config no longer needs).
        `_failures` survives a turnover, so a worker that crash-loops across
        a reload still reaches the A-5 limit (D9)."""
        self._workers.pop(name, None)
        self._restart_at.pop(name, None)
        self._started_at.pop(name, None)
        self._worker_epoch.pop(name, None)
        self._worker_generation.pop(name, None)
        self._defer_logged_at.pop(name, None)
        self._worker_token.pop(name, None)
        self._worker_fs.pop(name, None)
        self._worker_addr.pop(name, None)
        self._lost.discard(name)
        if not keep_failures:
            self._failures.pop(name, None)

    @staticmethod
    def _tracks_orphans() -> bool:
        """Only `forkserver` can report a live worker as dead (its `poll()`
        loses the child when the forkserver exits). Under `fork` and `spawn`
        the supervisor is the parent and `waitpid` is authoritative, so no
        identity bookkeeping (and no way to misclassify a real death)."""
        try:
            return multiprocessing.get_start_method() == 'forkserver'
        except Exception:
            return False

    @staticmethod
    def _current_forkserver() -> Optional[Tuple[int, Optional[str]]]:
        """`(pid, start token)` of the forkserver multiprocessing is using,
        or `None` if that cannot be read (a private attribute: any failure
        means "unknown", never an exception)."""
        try:
            from multiprocessing import forkserver
            pid = forkserver._forkserver._forkserver_pid
        except Exception:
            return None
        if not isinstance(pid, int) or pid <= 1:
            return None
        return pid, procident.start_token(pid)

    def _forkserver_state(self, name: str) -> str:
        """`'healthy'` (the forkserver that forked `name` is still that live
        process), `'gone'` (exited, a zombie, or its pid reused) or
        `'unknown'` (not recorded, or unreadable)."""
        rec = self._worker_fs.get(name)
        if rec is None or not rec[1]:
            return 'unknown'
        ident = procident.identity(rec[0], rec[1])
        if ident == 'match':
            return 'healthy'
        if ident in ('gone', 'mismatch'):
            return 'gone'
        return 'unknown'

    def _worker_running(self, name: str, worker: multiprocessing.Process) -> bool:
        """Is the process `_spawn` started still there? Errs towards `True`
        (never replace a worker beside a possibly live one): identity
        `'match'` is yes, `'gone'`/`'mismatch'` is no, and `'unknown'`
        (unreadable, e.g. `hidepid`) falls back to `kill(pid, 0)` -- alive or
        `EPERM` is yes, only `ESRCH` is no. Without a recorded token (the
        worker died before it could be read) the answer is no."""
        token = self._worker_token.get(name)
        if not token or worker.pid is None:
            return False
        ident = procident.identity(worker.pid, token)
        if ident == 'match':
            return True
        if ident in ('gone', 'mismatch'):
            return False
        try:
            os.kill(worker.pid, 0)
        except ProcessLookupError:
            return False
        except Exception:
            return True
        return True

    def _orphaned(self, name: str, worker: multiprocessing.Process) -> bool:
        """`True` if `worker`, which `poll()` calls dead, is still running:
        it is a forkserver orphan (see `_lost`)."""
        return self._worker_running(name, worker)

    def _lost_running(self, name: str, worker: multiprocessing.Process) -> bool:
        """Is a lost worker's process still there? See `_worker_running`."""
        return self._worker_running(name, worker)

    def _monotonic(self) -> float:
        """The supervisor's clock for restart deadlines: a named seam so
        tests can advance time without patching the process-global
        `time.monotonic` (which `_supervisor_sleep` also reads)."""
        return time.monotonic()

    def _start_workers(self) -> None:
        names = self._active_worker_names()
        for i, name in enumerate(names):
            # Re-checked before every fork, not once: a SIGTERM arriving
            # between spawns would otherwise still fork the remaining
            # workers only for them to be stopped a moment later.
            if self._stop_event.is_set():
                logger.info(
                    "Shutdown requested while starting workers; not starting: "
                    f"{', '.join(names[i:])}"
                )
                return
            self._spawn(name)

    def _record_worker_death(self, name: str, worker: multiprocessing.Process) -> bool:
        """Account for one dead worker and schedule its restart.

        Returns `True` if the worker has now died
        `WORKER_MAX_CONSECUTIVE_FAILURES` times in a row, in which case no
        restart is scheduled and the caller must stop the daemon.

        Backoff is `WORKER_RESTART_DELAY * 2**(n-1)` seconds before the n-th
        consecutive restart, capped at `WORKER_RESTART_MAX_DELAY`. A
        worker that stayed up at least `WORKER_STABLE_UPTIME` before dying
        has recovered, so its count restarts at 1 -- a worker that crashes
        once a day should never accumulate towards the limit. The delay is a
        `restart_at` deadline, not a sleep: `_check_workers` returns
        immediately and re-checks the deadline on the next poll, so a
        backoff can never hold up a shutdown.
        """
        now = self._monotonic()
        uptime = now - self._started_at.get(name, now)
        if uptime >= WORKER_STABLE_UPTIME:
            self._failures[name] = 0
        failures = self._failures.get(name, 0) + 1
        self._failures[name] = failures

        if failures >= WORKER_MAX_CONSECUTIVE_FAILURES:
            logger.error(
                f"{name} worker (pid={worker.pid}) exited with code "
                f"{worker.exitcode} for the {failures}th consecutive time "
                f"(it never stayed up {WORKER_STABLE_UPTIME}s); giving up "
                "instead of restarting it forever."
            )
            return True

        delay = min(WORKER_RESTART_DELAY * 2 ** (failures - 1), WORKER_RESTART_MAX_DELAY)
        self._restart_at[name] = now + delay
        logger.error(
            f"{name} worker (pid={worker.pid}) exited with code "
            f"{worker.exitcode} after {uptime:.0f}s (consecutive failure "
            f"{failures}/{WORKER_MAX_CONSECUTIVE_FAILURES}), restarting in {delay}s"
        )
        return False

    def _check_workers(self) -> None:
        """Restart workers that died -- except a worker that keeps crashing,
        which stops the whole daemon.

        Never sleeps. A dead worker gets a `restart_at` deadline (see
        `_record_worker_death`) and is forked again by whichever later poll
        finds the deadline passed, so one call takes microseconds however
        many workers are down, and the stop flag is honoured at the next
        `_supervisor_sleep` slice.

        A crash restart hands the replacement the supervisor's current
        `Handoff` (see `_new_worker`); it does not re-read the config
        database, so a crash can never move one worker onto a different
        generation than its siblings born on the same epoch. There is no
        configuration failure left to classify here: a worker never opens
        the database, so a bad one is refused by `_run_startup` before any worker
        exists (or, on a reload, by `_try_reload` while the old workers keep
        running). A worker that dies is restarted with exponential backoff
        and escalated to the fatal treatment below after
        `WORKER_MAX_CONSECUTIVE_FAILURES` consecutive deaths.

        **Turnover (item 17.2).** A worker that exits `WORKER_EXIT_RELOAD`
        while its `born_epoch` is below the current epoch did exactly what a
        reload asked: it is respawned at once on the current `Handoff`
        ("Reloaded"), with no failure count and no backoff -- or retired, if
        the current config no longer needs it. A code 3 from a worker born
        on the current epoch cannot be a reload, so it takes the crash path;
        during a shutdown any code 3 is just a normal exit. Respawning only
        a worker that is already dead is also what keeps the API port free:
        a replacement `ApiWorker` never exists beside the old one.

        **Lost workers (forkserver death).** Under `forkserver` a dead
        forkserver makes `poll()` report every worker dead while they still
        run. A "dead" worker whose `procident` identity still matches is
        *lost*, not crashed: ERROR once, no failure toward D9, no respawn
        while its pid lives (two remote workers on one dataset, an API that
        cannot bind), and an immediate respawn -- no backoff -- once it has
        exited. Lost workers count like live ones on their recorded epoch in
        `_maybe_complete` / `_log_deferred_switches`.

        **One crash-looping worker is fatal to the daemon, not just to that
        worker.** A daemon that keeps three of four workers alive after the
        fourth gave up -- say, taking no snapshots while the API keeps
        answering and pruning keeps deleting, with one ERROR line long since
        scrolled away -- is partial degradation with no liveness surface,
        which is worse for a backup daemon than an exit the service manager
        can see, restart, and eventually report as failed.
        """
        # Two passes on purpose: classify every dead worker first, so that
        # a worker that has given up is known before anything is respawned.
        # A single pass would fork a replacement for an unrelated crash and
        # then immediately shut it down.
        #
        # Workers already waiting out a backoff are skipped: they are still
        # in `_workers` (so `_shutdown_workers` reaps them) but have been
        # accounted for.
        h = self._handoff
        active: Optional[Set[str]] = None
        turnovers: List[Tuple[str, multiprocessing.Process]] = []

        # Lost (forkserver-orphaned) workers first: an orphan that has now
        # exited is replaced exactly like a turnover (no failure counted, no
        # backoff); one that is still alive is left alone. A lost worker is
        # never in `dead` below.
        for name in sorted(self._lost):
            worker = self._workers.get(name)
            if worker is None:
                self._lost.discard(name)
            elif not self._lost_running(name, worker):
                logger.info(
                    f"orphaned {name} worker (pid={worker.pid}) has exited"
                )
                turnovers.append((name, worker))

        dead = [
            (name, w) for name, w in self._workers.items()
            if name not in self._restart_at and name not in self._lost
            and not w.is_alive()
        ]

        lost_now = []
        orphan_gone: Set[str] = set()
        for name, worker in dead:
            if self._orphaned(name, worker):
                self._lost.add(name)
                lost_now.append(f"{name} (pid={worker.pid})")
            elif (
                worker.exitcode == _FORKSERVER_LOST_EXITCODE
                # Only a worker whose identity was recorded: without a token
                # we cannot tell an exit from a still-running orphan, so it
                # takes the crash path (backoff) rather than a replacement
                # that might run beside it.
                and self._worker_token.get(name)
                and self._forkserver_state(name) == 'gone'
            ):
                # The orphan has already exited (an idle one leaves within a
                # `STOP_POLL` slice): its outcome is unknown and `poll()`'s
                # 255 is the forkserver's death, not the worker's. No
                # failure, no backoff -- replace it now, like a turnover.
                turnovers.append((name, worker))
                orphan_gone.add(name)
                lost_now.append(f"{name} (pid={worker.pid}, already exited)")
        if lost_now:
            if len(orphan_gone) == len(lost_now):
                outcome = "they have already exited; replacing them now"
            else:
                outcome = "replacements start once they have exited"
            logger.error(
                "forkserver exited; "
                f"{len(lost_now)} worker(s) orphaned: {', '.join(lost_now)}; "
                f"{outcome}"
            )
        dead = [
            (n, w) for n, w in dead
            if n not in self._lost and n not in orphan_gone
        ]

        for name, worker in dead:
            if worker.exitcode == WORKER_EXIT_RELOAD:
                if self._stop_event.is_set():
                    # It saw RELOAD just before STOP: a normal exit, left in
                    # `_workers` for `_shutdown_workers` to reap.
                    continue
                if self._worker_epoch.get(name, h.epoch) < h.epoch:
                    turnovers.append((name, worker))
                    continue
            if active is None:
                active = set(self._active_worker_names())
            if name not in active:
                # Draining after a reload that no longer needs it, and it
                # crashed instead of switching: nothing to restart.
                logger.error(
                    f"{name} worker (pid={worker.pid}) exited with code "
                    f"{worker.exitcode}; not restarting it: not needed by "
                    f"generation {_gen(h.key)}"
                )
                self._forget_worker(name, keep_failures=False)
                continue
            if self._record_worker_death(name, worker):
                self._crash_looped.add(name)
                del self._workers[name]

        if not self._crash_looped:
            if turnovers and active is None:
                active = set(self._active_worker_names())
            for i, (name, worker) in enumerate(turnovers):
                # Forking into a shutdown is never right.
                if self._stop_event.is_set():
                    logger.info(
                        "Shutdown requested; not respawning remaining "
                        f"worker(s): {', '.join(n for n, _ in turnovers[i:])}"
                    )
                    return
                retired = name not in active
                verb = (
                    'Reloaded' if self._worker_epoch.get(name, h.epoch) < h.epoch
                    else 'Restarted'
                )
                # The same recovery rule a death applies
                # (`_record_worker_death`): an incarnation that stayed up
                # `WORKER_STABLE_UPTIME` has recovered, so a turnover must
                # not carry its earlier crashes forward.
                now = self._monotonic()
                if now - self._started_at.get(name, now) >= WORKER_STABLE_UPTIME:
                    self._failures[name] = 0
                self._forget_worker(name, keep_failures=not retired)
                if retired:
                    logger.info(
                        f"{name} worker retired: not needed by generation {_gen(h.key)}"
                    )
                else:
                    self._spawn(name, verb=verb)

            now = self._monotonic()
            due = [n for n, at in sorted(self._restart_at.items(), key=lambda kv: kv[1]) if at <= now]
            for i, name in enumerate(due):
                # Forking into a shutdown is never right.
                if self._stop_event.is_set():
                    logger.info(
                        "Shutdown requested; not restarting remaining "
                        f"worker(s): {', '.join(due[i:])}"
                    )
                    return
                self._spawn(name, verb='Restarted')

        if self._crash_looped:
            # D9: no rollback. After a reload the operator needs to know
            # which generation the crash loop happened on.
            running = ""
            if h.epoch > 0:
                running = (
                    f" The daemon is running configuration generation "
                    f"{_gen(h.key)} (accepted at {self._accepted_at})."
                )
            logger.error(
                "Worker(s) crashed repeatedly and were not restarted: "
                f"{', '.join(sorted(self._crash_looped))}. The daemon "
                "cannot run degraded -- shutting down the remaining "
                f"workers ({', '.join(sorted(self._workers)) or 'none'}) "
                f"and exiting non-zero.{running} See the worker errors above."
            )
            # A visible, non-zero exit instead of a spin: this is precisely
            # the "workers have died" case `StopFlag` (see
            # `zfsbackup.stopflag`) was built for, and `set()` cannot wedge
            # on a ghost sleeper the way `multiprocessing.Event` could. If
            # this is ever reverted to `Event`, that guarantee is lost.
            self._stop_event.set()

        if self._stop_event.is_set():
            return
        self._log_deferred_switches()
        self._maybe_complete()

    def _log_deferred_switches(self) -> None:
        """D5: a reload never kills in-flight work, so a worker busy with a
        long transfer can stay on the old generation indefinitely. Say so:
        WARNING per such worker once `RELOAD_DEFER_NOTICE` has passed since
        the reload was accepted, then every `RELOAD_DEFER_REPEAT`."""
        h = self._handoff
        if self._reload_started_at is None or self._applied_epoch >= h.epoch:
            return
        now = self._monotonic()
        if now - self._reload_started_at < RELOAD_DEFER_NOTICE:
            return
        for name, worker in self._workers.items():
            if name in self._restart_at or self._worker_epoch.get(name, h.epoch) >= h.epoch:
                continue
            if name not in self._lost and not worker.is_alive():
                continue  # classified by the next `_check_workers`
            last = self._defer_logged_at.get(name)
            if last is not None and now - last < RELOAD_DEFER_REPEAT:
                continue
            self._defer_logged_at[name] = now
            logger.warning(
                f"{name} worker (pid={worker.pid}) is still finishing work begun "
                f"under generation {self._worker_generation.get(name, 'unknown')}; "
                f"it switches to {_gen(h.key)} at its next safe point "
                "(in-flight transfers are never interrupted)"
            )

    def _maybe_complete(self) -> None:
        """Record a reload as applied once every live worker runs the
        current epoch. A worker waiting out a crash backoff counts as
        switched: it will be respawned on the current `Handoff`."""
        h = self._handoff
        if self._applied_epoch >= h.epoch:
            return
        for name in self._workers:
            if name in self._restart_at:
                continue
            if self._worker_epoch.get(name, h.epoch) < h.epoch:
                return
        self._applied_epoch = h.epoch
        if self._presence_lock is not None:
            self._presence_lock.set_state(applied=h.key, target=None)
        logger.info(f"generation {_gen(h.key)} applied by all workers")

    def _shutdown_workers(self, timeout: int = 30) -> None:
        """Join every worker, escalating to SIGTERM then SIGKILL.

        `timeout` is a **single budget shared by all workers**, not a
        per-worker one: the deadline is computed once and each join gets
        whatever is left of it. The joins are sequential, so a per-worker
        30s meant four workers stuck in a long `zfs send` -- the case the
        30s exists for -- could cost 4 x (30 + 5 + 5) = 160s *after* the
        poll loop exited, against systemd's default `TimeoutStopSec=90s`.
        An operator sizing `TimeoutStopSec` from the documented bound got
        the cgroup SIGKILLed mid-stream, which is exactly what the
        escalation below is here to prevent. Shared, the graceful phase is
        capped at `timeout` in total and the escalation adds at most 10s
        per stubborn worker; see `SUPERVISOR_SLEEP_SLICE` for the
        end-to-end sum.
        """
        deadline = time.monotonic() + timeout
        lost: List[str] = []
        for name, worker in self._workers.items():
            if name in self._lost:
                lost.append(name)
                continue
            worker.join(timeout=max(0.0, deadline - time.monotonic()))
            if not worker.is_alive():
                # A forkserver that died since the last poll makes every
                # worker look dead (see `_lost`): do not walk away from live
                # orphans. A real exit leaves no matching process.
                if self._orphaned(name, worker):
                    self._lost.add(name)
                    lost.append(name)
                continue
            logger.warning(
                f"{name} worker did not exit within the {timeout}s shutdown "
                f"budget, terminating"
            )
            worker.terminate()
            worker.join(timeout=5)
            if not worker.is_alive():
                continue
            # SIGKILL backstop. `terminate()` sends SIGTERM, which a child
            # that has a SIGTERM *handler* installed does not have to obey
            # -- and under `fork` a child inherited this supervisor's own
            # handler until `workers._init_child_process` started resetting
            # it to SIG_DFL (measured before that fix: `alive after
            # terminate+join(5): True exitcode None`). This loop used to
            # fall through silently at that point, leaving a live child and
            # no log line; the interpreter's own exit handler then called
            # `join()` with NO timeout, so `systemctl stop` hung until
            # TimeoutStopSec and systemd SIGKILLed the whole cgroup --
            # possibly mid-`zfs send`. Escalating here keeps that decision
            # ours, and keeps it logged.
            logger.error(
                f"{name} worker (pid={worker.pid}) ignored SIGTERM; sending SIGKILL"
            )
            worker.kill()
            worker.join(timeout=5)
            if worker.is_alive():
                logger.error(
                    f"{name} worker (pid={worker.pid}) survived SIGKILL "
                    "(uninterruptible sleep?); abandoning it"
                )
                self.abandoned_workers.append(name)
        for name in lost:
            self._shutdown_lost(name, self._workers[name], timeout, deadline)

    def _shutdown_lost(
        self, name: str, worker: multiprocessing.Process, timeout: int, deadline: float
    ) -> None:
        """Wait for an orphan (no `Process` handle works any more) within the
        shared stop budget, then SIGTERM and SIGKILL it by pid. Every signal
        is preceded by a fresh identity check: a pid that is gone, or now
        belongs to another process, is never signalled. Shutdown path only:
        blocking here is fine."""
        pid = worker.pid
        token = self._worker_token.get(name)

        def wait_gone(limit: float) -> bool:
            while self._lost_running(name, worker):
                if time.monotonic() >= limit:
                    return False
                time.sleep(0.1)
            return True

        if wait_gone(deadline):
            return
        logger.warning(
            f"orphaned {name} worker (pid={pid}) did not exit within the "
            f"{timeout}s shutdown budget, terminating"
        )
        for sig, label, grace in (
            (signal.SIGTERM, 'SIGTERM', 5), (signal.SIGKILL, 'SIGKILL', 5),
        ):
            outcome = _signal_verified(pid, token, sig)
            if outcome == 'gone':
                return
            if outcome == 'error':
                logger.error(f"cannot send {label} to orphaned {name} worker (pid={pid})")
                break
            if outcome == 'unverified':
                if wait_gone(time.monotonic()):
                    return
                break  # cannot verify identity: never signal blindly
            if wait_gone(time.monotonic() + grace):
                return
        logger.error(
            f"orphaned {name} worker (pid={pid}) survived SIGKILL or could not "
            "be verified; abandoning it"
        )
        self.abandoned_workers.append(name)

    def _supervisor_sleep(self, seconds: float) -> None:
        """Sleep up to `seconds`, returning as soon as the stop flag is set
        or a SIGHUP is pending (`_hup_requested`, one attribute read per
        slice), so a reload request waits at most one
        `SUPERVISOR_SLEEP_SLICE` before `_reload_tick` acts on it.

        Called exactly once per poll iteration, in place of a bare
        `time.sleep(SUPERVISOR_POLL)`.

        A single `time.sleep(5)` is **not** interruptible by a signal:
        per PEP 475 it is retried with the time remaining once the handler
        returns (measured: handler at 0.50s, `time.sleep(5.0)` still
        returned at 5.01s). So `_signal_handler` setting the flag promptly
        bought nothing -- the supervisor still took up to `SUPERVISOR_POLL`
        seconds to reach `_shutdown_workers`, every time, on the healthy
        path. Slicing the sleep bounds that by `SUPERVISOR_SLEEP_SLICE`
        instead.

        This is a named method rather than an inlined loop so that tests
        have one stable seam to patch: patching `zfsbackup.daemon.time.sleep`
        would now see an *unbounded* number of calls per iteration instead
        of one -- a patched `time.sleep` does not sleep, so the loop below
        is bounded only by real `time.monotonic()` and spins a CPU for the
        whole five seconds. (`_check_workers` no longer sleeps at all; its
        restart backoff is a deadline on `_monotonic()`.)

        Why this and not `self._stop_event.wait(seconds)`, which it
        otherwise duplicates: only the return value and the slice size
        differ, and the slice difference is real but narrow.
        `stopflag.STOP_POLL` (0.5s) governs how fast a *worker* notices the
        flag, and that is what bounds the daemon's healthy-path shutdown --
        the supervisor cannot finish until the workers exit, and the
        measured 0.265-0.554s tails are a worker's slice, not this one.
        `SUPERVISOR_SLEEP_SLICE` (0.25s) governs only how fast the
        supervisor leaves its idle sleep, where the finer slice buys
        promptness on the *restart* path (the flag can also be raised by
        `_check_workers`, with no worker involved) at negligible cost: 20
        wakeups per idle poll in one process. If the two ever need to move
        together, collapse them -- they are not independent knobs, just
        deliberately different ones.
        """
        deadline = time.monotonic() + seconds
        while not self._stop_event.is_set():
            if self._hup_requested:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(SUPERVISOR_SLEEP_SLICE, remaining))

    def run(self) -> int:
        """Run the supervisor until the stop event is set. Returns the
        daemon's process exit code: 0 for a clean shutdown, 1 if **any**
        worker crash-looped -- in which case `_check_workers` has already
        stopped the whole daemon rather than let it run degraded. Returning
        a code at all, rather than always 0, is what lets a service manager
        see that a daemon that gave up is not a successful run.
        """
        logger.info("=" * 60)
        logger.info("ZFS Backup Daemon Starting")
        logger.info("=" * 60)

        # The supervisor makes no `zfs` call (B1): a hung pool inside one
        # would stall SIGTERM, SIGHUP and every worker restart, because
        # `Popen.wait` is retried after a signal (PEP 475). The existence
        # check and the config-property sync are the snapshot worker's
        # first act (`SnapshotWorker._before_loop`); this is pure logging.
        manager = DatasetManager(self.config)
        manager.dataset_report()

        # A SIGTERM can still land in the logging above, or in the
        # presence-lock write of the first `_reload_tick`; forking into a
        # shutdown is never right (`_check_workers` says the same thing about
        # the restart path, and guards it the same way). Structured as a
        # branch rather than an early return so that the shutdown tail
        # below stays the single exit: `_shutdown_workers` over an empty
        # `_workers` is a no-op, and the log ends with "Daemon stopped"
        # either way.
        #
        # The whole of it sits in a try/finally: an exception escaping the
        # poll loop (`worker.start()` failing with EAGAIN under a tight
        # TasksMax, say) used to skip `_shutdown_workers` entirely, and
        # multiprocessing's exit handler then SIGTERMed workers with the
        # default disposition mid-`zfs send`. The `finally` raises the stop
        # flag first, so workers can reach a safe point on their own, then
        # runs the usual escalating join; the exception still propagates to
        # `_run`, which logs it and exits 1.
        try:
            if not self._stop_event.is_set():
                # Before the first spawn: a SIGHUP carried over from
                # `_StartupHup`, or a write that landed since the startup
                # read, is applied now -- onto workers that do not exist yet,
                # so there is no turnover and no double cycle.
                self._reload_tick(startup=True)

            if self._stop_event.is_set():
                # Idempotent, and called here too only so the "Received SIGTERM"
                # line precedes its consequence in the log.
                self._report_stop_signal()
                logger.info("Shutdown requested during startup; no workers started")
            else:
                logger.info("=" * 60)
                logger.info("Starting workers...")
                logger.info("=" * 60)

                self._start_workers()
                # READY=1 means "started": every worker is forked (the
                # workers' own first act, e.g. the dataset check, is not
                # waited for). Not sent into a shutdown.
                if not self._stop_event.is_set():
                    self._notify_ready()

                while not self._stop_event.is_set():
                    self._supervisor_sleep(SUPERVISOR_POLL)
                    if self._stop_event.is_set():
                        break
                    self._reload_tick()
                    if self._stop_event.is_set():
                        break
                    self._check_workers()
        finally:
            # First, before the (up to ~70 s) worker shutdown, so `systemctl
            # status` shows "deactivating" with the right reason. Sent here,
            # on the main thread, never from `_signal_handler`.
            #
            # The stop flag first: `notify.stopping()` can block for up to its
            # send timeout (a full systemd queue), and the workers only see
            # the stop once the flag is up.
            self._stop_event.set()
            self._notify.stopping()
            # The line `_signal_handler` deliberately does not log itself. Here
            # it is an ordinary call on the main thread, not a handler, so
            # blocking on a journald socket is merely slow instead of fatal.
            self._report_stop_signal()
            logger.info("Shutting down workers...")
            self._shutdown_workers()
        logger.info("Daemon stopped")
        return 1 if self._crash_looped else 0

    # -- live reload (item 17.2) -------------------------------------------

    def _reload_tick(self, startup: bool = False) -> None:
        """Decide whether to read the configuration again, and do it.

        Called from `run()`'s loop between `_supervisor_sleep` and
        `_check_workers`, and once with `startup=True` before the first
        spawn. Two triggers, one path (D1):

        - **SIGHUP** (`_hup_requested`) forces a full read. The flag is
          cleared BEFORE the read: a SIGHUP that lands later forces another
          tick, and the write behind this one was committed before its
          signal was sent, so the read below sees it.
        - **The poll**, every `RELOAD_POLL_SECONDS` (and always at startup):
          `read_store_state`, and a full read only when the stored key
          differs from both the running key and the last rejected one.

        Every database read is followed by `dispose_all()` in a `finally`,
        including a read that raises, so no pooled connection exists at the
        next fork. Stop beats everything: nothing is read once the flag is
        up.
        """
        forced = self._hup_requested
        if forced:
            self._hup_requested = False
        if startup or not forced:
            try:
                self._reload_tick_body(startup, forced)
            finally:
                # A poll-triggered accept or reject is only a STATUS= change
                # (and the startup tick sends none: READY=1 follows it).
                if not startup:
                    self._notify_status()
            return
        # A SIGHUP-forced tick is what `systemctl reload` waits on
        # (`Type=notify-reload` sends the SIGHUP itself; older units use
        # `ExecReload=kill -HUP`). RELOADING=1 first, READY=1 on EVERY outcome
        # -- unchanged, identical, rejected, live reload off, an exception --
        # because READY means "the reload was decided", not "the turnover
        # finished": D5 lets a worker finish a transfer for hours, and waiting
        # for that would hang `systemctl reload`. Not for the startup tick
        # (READY=1 has not been sent yet; RELOADING before it is meaningless).
        if self._stop_event.is_set():
            # A SIGTERM that raced the SIGHUP: RELOADING=1 -> READY=1 ->
            # STOPPING=1 would tell systemd a reload finished on a daemon
            # that is going down (the body would read nothing anyway).
            return
        self._notify.reloading()
        try:
            self._reload_tick_body(startup, forced)
        finally:
            self._notify_ready()

    def _status_text(self) -> str:
        """The `systemctl status` line: the running generation, plus the
        candidate the last reload refused (if it is still the stored one)."""
        text = f"generation {_gen(self._handoff.key)}"
        if self._rejected is not None:
            text += f"; generation {_gen(self._rejected)} rejected"
        elif self._deferred is not None:
            text += f"; generation {_gen(self._deferred)} deferred"
        return text

    def _notify_status(self) -> None:
        text = self._status_text()
        if text != self._status_sent:
            self._status_sent = text
            self._notify.status(text)

    def _notify_ready(self) -> None:
        text = self._status_text()
        self._status_sent = text
        self._notify.ready(text)

    def _reload_tick_body(self, startup: bool, forced: bool) -> None:
        """`_reload_tick` minus the notification bracket; `forced` is the
        SIGHUP flag it already consumed."""
        h = self._handoff
        if h.key is None:
            if forced:
                logger.warning(
                    "Received SIGHUP, but this daemon was started without a "
                    "configuration key, so live reload is off; restart it to "
                    "apply configuration changes"
                )
            return
        now = self._monotonic()
        if not (forced or startup or now >= self._next_poll_at):
            return
        self._next_poll_at = now + RELOAD_POLL_SECONDS
        if self._stop_event.is_set():
            return
        if not forced:
            try:
                try:
                    state = read_store_state(self.resolved_config)
                finally:
                    dispose_all()
            except Exception as e:
                self._note_poll_problem(e)
                return
            self._note_poll_problem(None)
            stored = (state.store_id, state.generation)
            # A final rejection is skipped; a deferred key is not (it is
            # re-evaluated at every poll, see `_deferred`).
            if stored in (h.key, self._rejected, self._poll_skip):
                return
            # The poll can block for up to `busy_timeout`: a stop that landed
            # meanwhile must not start a full read and a reload.
            if self._stop_event.is_set():
                return
            self._try_reload('poll', startup=startup, polled=stored)
        else:
            self._try_reload('SIGHUP', startup=startup)

    def _describe_failure(self, exc: Exception) -> str:
        """`config_error_message` with the reload wording, never raising."""
        try:
            return config_error_message(self.resolved_config, exc, reloading=True)
        except Exception:  # pragma: no cover - the message must never kill the loop
            return f"{type(exc).__name__}: {exc}"

    def _note_poll_problem(self, exc: Optional[Exception]) -> None:
        """Log a poll failure once per state change: WARNING when a problem
        starts (or its message changes), INFO when it clears, DEBUG while it
        persists. The daemon keeps running its current generation either
        way."""
        running = _gen(self._handoff.key)
        if exc is None:
            if self._poll_problem is not None:
                self._poll_problem = None
                logger.info(
                    "the configuration database can be checked for changes "
                    f"again; running generation {running}"
                )
            return
        message = self._describe_failure(exc)
        # The store's message goes LAST and verbatim: its last line may be a
        # command meant to be pasted into a shell.
        line = (
            "cannot check the configuration database for changes; still "
            f"running generation {running}:\n{message}"
        )
        if message != self._poll_problem:
            self._poll_problem = message
            # Diagnosed failures carry no traceback; anything else is a bug
            # and is undiagnosable without one.
            logger.warning(line, exc_info=not isinstance(exc, _EXPECTED_CONFIG_ERRORS))
        else:
            logger.debug(line)

    def _reject(
        self,
        trigger: str,
        key: Optional[Tuple[int, int]],
        exc: Exception,
        *,
        polled: Optional[Tuple[int, int]] = None,
        message: Optional[str] = None,
        retry: bool = False,
    ) -> None:
        """A candidate that could not be read, failed `validate_config`, or
        cannot be built (`_build_manager`): log it (deduplicated by
        `(key, message)`), and when its key is known remember it so the poll
        does not retry it, and publish it as `rejected=` in the pid file.
        When the key is unknown but a poll triggered the read, `polled` is
        remembered as the poll-only `_poll_skip` marker instead (not for a
        transient `OperationalError`). The workers keep running the current
        generation. `message` replaces `config_error_message`'s wording.
        `retry` marks a refusal that may pass later without any new write
        (see `_deferred`): it is logged as "deferred", published as
        `deferred=` -- NOT `rejected=`, which stays final -- and the poll
        re-evaluates the key."""
        if message is None:
            message = self._describe_failure(exc)
        # The clause first, the store's message LAST and verbatim: its last
        # line may be a command meant to be pasted into a shell.
        if retry and key is not None:
            line = (
                f"configuration generation {_gen(key)} deferred (trigger: "
                f"{trigger}); still running generation {_gen(self._handoff.key)}: "
                f"{message}; re-checked every poll"
            )
        else:
            line = (
                f"configuration generation {_gen(key)} rejected (trigger: {trigger}); "
                f"still running generation {_gen(self._handoff.key)}:\n{message}"
            )
        if (key, message) != self._rejection_logged:
            self._rejection_logged = (key, message)
            if retry and key is not None:
                logger.warning(line)
            else:
                # Diagnosed failures carry no traceback; anything else is a
                # bug and is undiagnosable without one.
                logger.error(line, exc_info=not isinstance(exc, _EXPECTED_CONFIG_ERRORS))
        else:
            logger.debug(line)
        if key is not None and retry:
            reason = message.splitlines()[0] if message else None
            if key != self._deferred or reason != self._deferred_reason:
                self._deferred = key
                self._deferred_reason = reason
                changed = True
            else:
                changed = False
            if self._rejected is not None:
                # Superseded: the pid file shows one refusal at a time.
                self._rejected = None
                changed = True
            if changed and self._presence_lock is not None:
                self._presence_lock.set_state(
                    deferred=key, deferred_reason=reason, rejected=None
                )
        elif key is not None:
            had_deferred = self._deferred is not None
            self._deferred = None
            self._deferred_reason = None
            if key != self._rejected or had_deferred:
                self._rejected = key
                if self._presence_lock is not None:
                    # `deferred=None` only when one was published: the common
                    # rejection keeps its single `rejected=` write.
                    self._presence_lock.set_state(
                        rejected=key, **({'deferred': None} if had_deferred else {})
                    )
        elif polled is not None and not isinstance(exc, (OperationalError, SchemaError)):
            # Not for a transient storage failure, and not for a schema
            # failure: `check_schema` runs first in `read_config`, before any
            # row, so a retry costs one tiny read transaction, and `migrate`
            # keeps the store id and generation -- only a retry picks the
            # migrated store up without a SIGHUP. The ERROR above is logged
            # once per `(key, message)`; each retry logs at DEBUG.
            self._poll_skip = polled

    def _try_reload(
        self,
        trigger: str,
        *,
        startup: bool = False,
        polled: Optional[Tuple[int, int]] = None,
    ) -> None:
        """Read, validate, build and -- if it is a different, usable
        configuration -- switch to it. The key recorded is the one
        `read_config`'s snapshot returned, never the poll's (`polled` is only
        a `_poll_skip` marker for a read that failed before its key was
        known): a write between the poll and this read is simply part of
        what is applied.

        Refused, with the old workers left running, when the read fails,
        `validate_config` raises, or the dataset list cannot be built
        (`_build_manager`, the first thing every worker does): a config that
        would crash-loop a worker is never accepted -- including one whose
        changed API address cannot be bound (`_candidate_refusal`, B3). When the
        address is merely IN USE (`EADDRINUSE`) that is *deferred*, not
        rejected: it may pass later with nothing written (the port is
        freed), so it is re-checked at every poll and published as
        `deferred=`. Any other bind failure (not our address, privileged
        port, unresolvable name) is a final rejection: re-probing it would
        only repeat the failure and re-resolve the name on the supervisor
        loop at every poll. A remote-backup problem (client id, client
        certificate, un-enrolled destinations) only warns
        (`_candidate_warnings`). A candidate identical to the running config
        (`BackupConfig.same_as`, after the `--dry-run` override) *with the
        same client id* only moves the key (`_accept_identical`, B2); a
        changed client id is a real switch (epoch bump).
        The supervisor makes
        no `zfs` call (B1): `verify_datasets` and the property sync are the
        respawned snapshot worker's job. Stop beats all of it: nothing is
        accepted, recorded or switched once the flag is up.

        Order matters and is the contract workers rely on: the new `Handoff`
        is stored BEFORE `ReloadEpoch.set()`, so any worker spawned after a
        worker could observe the new epoch is born with the new config and
        the new epoch together.

        The catch is deliberately broad (`Exception`, bare `RuntimeError`
        included, unlike startup's): a running daemon keeps its current
        generation rather than dying over a candidate. Undiagnosed types are
        logged with a traceback (`_EXPECTED_CONFIG_ERRORS`).
        """
        h = self._handoff
        key: Optional[Tuple[int, int]] = None
        try:
            try:
                snap = load_runtime_config(self.resolved_config)
                key = (snap.store_id, snap.generation)
                warnings = validate_config(snap.config)
            finally:
                dispose_all()
        except Exception as e:
            self._reject(trigger, key, e, polled=polled)
            return

        if key == h.key:
            if trigger == 'SIGHUP':
                logger.info(f"SIGHUP: configuration unchanged (generation {_gen(key)})")
            return

        config = snap.config
        try:
            manager = _build_manager(config)
        except Exception as e:
            self._reject(
                trigger, key, e, message=f"its dataset list cannot be used: {e}"
            )
            return
        if self._stop_event.is_set():
            return

        # `--dry-run` is a property of this run, never of the stored
        # configuration: it survives every reload. Applied before the
        # comparison below, so a stored `dry_run: false` copy of a
        # `--dry-run` daemon's config is still identical.
        if self._dry_run_override:
            config.dry_run = True

        # B2: a rewrite that changed nothing (a re-import, an `edit` that
        # saved the same text) bumps the generation but must not turn every
        # worker over. `same_as` is exact: a literal-only edit (`1h` ->
        # `60m`) is a difference and does turn workers over. The client id
        # is compared too: `same_as` ignores it (it is not configuration),
        # but `identity set-client-id` / `enroll` change it with an
        # otherwise identical config, and the remote worker must turn over
        # to pick it up -- an id read here and then dropped would leave it
        # sending under the old one.
        client_id = snap.client_id
        if config.same_as(h.config) and client_id == h.client_id:
            self._accept_identical(key)
            return

        # B3: refuse what no worker could start with before accepting it.
        # Broad catch, like the rest of this method: a bug in a check must
        # not take the supervisor down.
        try:
            refusal = _candidate_refusal(
                config, h.config, live_api=self._live_api_addrs()
            )
        except Exception as e:
            self._reject(
                trigger, key, e,
                message=(
                    "the pre-start check (API bind) failed to run: "
                    f"{type(e).__name__}: {e}"
                ),
            )
            return
        if refusal is not None:
            self._reject(
                trigger, key, ValueError(refusal), message=refusal,
                retry=bool(getattr(refusal, 'deferrable', False)),
            )
            return

        for warning in warnings:
            logger.warning(f"configuration generation {_gen(key)}: {warning}")
        # Advisory (remote backup: client id, client certificate,
        # enrollment): never a refusal. Broad catch, a bug in a
        # check must not take the supervisor down; once per `(key, message)`.
        try:
            advice = _candidate_warnings(
                config, h.config, client_id=client_id,
                previous_client_id=h.client_id, tls=_tls_dir(self.resolved_config),
            )
        except Exception as e:
            advice = [f"the remote backup check failed to run: {type(e).__name__}: {e}"]
        self._warned = {w for w in self._warned if w[0] == key}
        for warning in advice:
            if (key, warning) not in self._warned:
                self._warned.add((key, warning))
                logger.warning(f"configuration generation {_gen(key)}: {warning}")
        logger.info(
            f"configuration generation {_gen(key)} accepted "
            f"(was {_gen(h.key)}; trigger: {trigger})"
        )
        if self._presence_lock is not None:
            self._presence_lock.set_state(
                target=key, rejected=None,
                **({'deferred': None} if self._deferred is not None else {}),
            )

        old = {ds.name: ds for ds in h.config.datasets}
        new = {ds.name: ds for ds in config.datasets}
        added = sorted(new.keys() - old.keys())
        removed = sorted(old.keys() - new.keys())
        changed = sorted(n for n in new.keys() & old.keys() if not _same(new[n], old[n]))
        settings = _changed_settings(h.config, config)
        if client_id != h.client_id:
            settings.append('client_id')
        if logger.isEnabledFor(logging.DEBUG):
            manager.dataset_report()

        # No `zfs` call here (B1): the existence check and the property sync
        # for this generation run in the respawned snapshot worker.
        if self._stop_event.is_set():
            # Shutdown proceeds; `target=` stays recorded, nothing switches.
            return

        epoch = h.epoch + 1
        self._handoff = Handoff(config, epoch, key, client_id)
        self._reload_epoch.set(epoch)
        self._rejected = None
        self._deferred = None
        self._deferred_reason = None
        self._rejection_logged = None
        self._poll_skip = None
        self._reload_started_at = self._monotonic()
        self._defer_logged_at.clear()
        self._accepted_at = time.strftime("%H:%M:%S")

        how = (
            "no workers are running yet" if startup
            else "workers switch over at their next safe point"
        )
        what = f"+{len(added)}, -{len(removed)}, ~{len(changed)} datasets"
        if settings:
            what += f"; settings changed: {', '.join(settings)}"
        logger.info(
            f"Reloading: generation {_gen(h.key)} -> {_gen(key)} ({what}); {how}"
        )
        # Logged only now, after the stop check above: a reload interrupted
        # by a shutdown must not describe a switch that never happened. Only
        # what changed at INFO; the full per-dataset block (which startup
        # prints) only under --verbose, not on every `set`.
        for name in added:
            logger.info(f"  added dataset {name}: {_describe_dataset(new[name])}")
        for name in changed:
            logger.info(f"  changed dataset {name}: {_describe_dataset(new[name])}")
        for name in removed:
            logger.info(f"  removed dataset {name}")
        if 'client_id' in settings:
            logger.info(
                f"  client id: {h.client_id or '(none)'} -> {client_id or '(none)'}"
            )
        if 'snapshot_prefix' in settings:
            logger.warning(
                f"snapshot_prefix changed from {h.config.snapshot_prefix} to "
                f"{config.snapshot_prefix}: snapshots named "
                f"{h.config.snapshot_prefix}_* are no longer managed or pruned"
            )

        if not startup:
            self._adjust_worker_set()
        self._maybe_complete()

    def _accept_identical(self, key: Tuple[int, int]) -> None:
        """B2: `key` holds exactly the configuration already running. Adopt
        the new key without a turnover: the `Handoff` keeps its config object
        and epoch (so no worker sees a change and none restarts), only the
        key moves. Rejection state is cleared -- a candidate equal to the
        running config is no longer rejected. With no turnover in flight the
        new key is published as `applied=` at once; mid-turnover it becomes
        the `target=` and `_maybe_complete` publishes it as `applied=` when
        the turnover finishes (it reads `h.key`)."""
        h = self._handoff
        clear = {'deferred': None} if self._deferred is not None else {}
        self._handoff = Handoff(h.config, h.epoch, key, h.client_id)
        self._rejected = None
        self._deferred = None
        self._deferred_reason = None
        self._rejection_logged = None
        self._poll_skip = None
        if self._presence_lock is not None:
            if self._applied_epoch == h.epoch:
                self._presence_lock.set_state(
                    applied=key, target=None, rejected=None, **clear
                )
            else:
                self._presence_lock.set_state(target=key, rejected=None, **clear)
        self._maybe_complete()
        logger.info(
            f"configuration generation {_gen(key)} is identical to running "
            f"generation {_gen(h.key)}; no worker restarts"
        )

    def _live_api_addrs(self) -> List[Tuple[str, int]]:
        """The `(host, port)` every live API worker is bound to -- a draining
        one from an older epoch included, a forkserver orphan included. Not
        `self.config`'s: after an accepted API-address change the old worker
        keeps its socket until it reaches a safe point (hours, if a receive is
        in flight), and a config reverting that change must not be refused
        for colliding with it."""
        w = self._workers.get('api')
        addr = self._worker_addr.get('api')
        if w is None or addr is None or 'api' in self._restart_at:
            return []
        if 'api' in self._lost or w.is_alive() or self._orphaned('api', w):
            return [addr]
        return []

    def _adjust_worker_set(self) -> None:
        """After a reload: retire workers in backoff that the new config no
        longer needs, and start the ones it newly needs. A name still in
        `_workers` -- live, draining, or dead and not yet classified -- is
        left to `_check_workers`, which respawns it only once it has exited:
        never a second instance beside the first (the API port)."""
        if self._crash_looped:
            return
        h = self._handoff
        active = self._active_worker_names()
        for name in sorted(self._restart_at):
            if name not in active:
                self._forget_worker(name, keep_failures=False)
                logger.info(f"{name} worker retired: not needed by generation {_gen(h.key)}")
        missing = [n for n in active if n not in self._workers]
        for i, name in enumerate(missing):
            # Forking into a shutdown is never right.
            if self._stop_event.is_set():
                logger.info(
                    "Shutdown requested; not starting worker(s): "
                    f"{', '.join(missing[i:])}"
                )
                return
            self._spawn(name)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='ZFS Backup Daemon - Automated snapshot management with retention policies'
    )
    # Deliberately `default=None` and deliberately NOT `type=Path`.
    #
    # `default=None` is what makes step 2 of `resolve_config_path`'s order
    # (`ZFSBACKUP_CONFIG`) reachable at all: with any non-None default,
    # `explicit` is never None and the environment variable becomes dead
    # code. The old `default=Path('/etc/zfsbackup/config.yaml')` was
    # exactly that shape.
    #
    # No `type=Path` because `str(Path("")) == "."`: `-c ""` would arrive
    # as `PosixPath('.')` and `resolve_config_path`'s documented
    # "empty means not given" fallthrough would silently become "use the
    # current directory", failing later with a confusing message about a
    # directory the operator never named. Pass the raw string through.
    parser.add_argument(
        '-c', '--config',
        default=None,
        help=(
            'Path to the config DATABASE (SQLite). Defaults to '
            f'${CONFIG_PATH_ENV} if set, otherwise {DEFAULT_CONFIG_DB}. '
            'YAML is no longer a config source: create a database from an '
            'existing YAML config with `zfsbackup-config import <path.yaml>`.'
        )
    )
    parser.add_argument(
        '-d', '--dry-run',
        action='store_true',
        help='Dry run mode - do not create or destroy snapshots'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose (debug) logging'
    )
    parser.add_argument(
        '--test-config',
        action='store_true',
        help='Test configuration and exit'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    abandoned: List[str] = []
    try:
        code = _run(args, abandoned)
    finally:
        # Every return path -- `--test-config`, a config refusal, a
        # supervisor crash, a clean shutdown -- drops this process's
        # engines before returning, so the last connection to the config
        # database closes here rather than whenever the interpreter
        # happens to finalise a pooled connection (a CPython detail this
        # module does not control, and one that differs between 3.11 and
        # 3.14).
        #
        # That determinism is the point, and the hazard is concrete:
        # SQLite deletes `-wal`/`-shm` when the LAST connection closes
        # cleanly, and `paths.py`'s step 6 documents that a sidecar is
        # owned by whichever process created it and is never re-chmod'd
        # afterwards. A `--test-config` run as root that leaves sidecars
        # behind is exactly the lockout `_stuck_sidecars_message` has to
        # refuse later -- root-owned sidecars next to a database the
        # zfsbackup user is supposed to open.
        #
        # This is also the same discipline `_spawn` applies at every fork
        # point, now applied at every process exit.
        dispose_all()

    if abandoned:
        # A worker survived SIGKILL (uninterruptible kernel sleep, typically
        # a hung `zfs` call). Returning normally would hand control to
        # multiprocessing's atexit handler, which `join()`s every child with
        # NO timeout and so hangs until systemd's `TimeoutStopSec` SIGKILLs
        # the cgroup. The orderly shutdown has already happened, so leave
        # right now -- after `dispose_all()` above, and after flushing the
        # logs that `os._exit` would otherwise discard.
        _hard_exit(code, abandoned)
    return code


def _hard_exit(code: int, abandoned: List[str]) -> None:  # pragma: no cover - exits
    """Flush logging and leave without running atexit handlers."""
    logger.error(
        f"Exiting without waiting for abandoned worker(s): {', '.join(abandoned)}"
    )
    logging.shutdown()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    # Workers were abandoned, so this was not a clean stop: never report 0.
    os._exit(code or 1)


#: Seams for the startup waits: tests advance time without sleeping.
_startup_sleep = time.sleep
_startup_monotonic = time.monotonic
#: Where the startup waits report `EXTEND_TIMEOUT_USEC`. A module global (like
#: the two seams above) so the waits keep their one-argument signature; `_run`
#: replaces it with the real notifier and puts the no-op back on the way out.
_startup_notifier = sdnotify.Notifier()

#: Poll interval of the startup waits, and how often their WARNING repeats.
_STARTUP_WAIT_POLL = 1.0
_STARTUP_WARN_EVERY = 60.0
#: Sent as `EXTEND_TIMEOUT_USEC` on every poll of a startup wait, so a unit's
#: `TimeoutStartSec` cannot kill a start that is legitimately waiting out a
#: predecessor's transfer. Far above the 1 s poll; if the supervisor stalls,
#: systemd's timeout still fires 30 s later.
_STARTUP_EXTEND_USEC = 30_000_000
#: How long ONE startup wait keeps extending the unit's start timeout. An
#: orphan stuck in uninterruptible sleep holds `<db>.workers.lock` forever and
#: would otherwise keep the unit "activating" for ever; after this the wait
#: goes on, but no more extensions are sent, so systemd fails the start about
#: `_STARTUP_EXTEND_USEC` (30 s) after the last one.
_STARTUP_EXTEND_MAX = 3600.0


class _ExtendBudget:
    """`EXTEND_TIMEOUT_USEC` for one startup wait, for at most
    `_STARTUP_EXTEND_MAX` seconds; one ERROR when the cap is reached."""

    def __init__(self, what: str) -> None:
        self._what = what
        self._began = _startup_monotonic()
        self._capped = False

    def extend(self) -> None:
        waited = _startup_monotonic() - self._began
        if waited >= _STARTUP_EXTEND_MAX:
            if not self._capped:
                self._capped = True
                logger.error(
                    f"still waiting for {self._what} after {waited / 60:.0f} "
                    "minutes; no longer extending the systemd start timeout "
                    "(systemd fails the start about 30 s after the last extension). Find the stuck "
                    "process(es) and stop them."
                )
            return
        _startup_notifier.extend_timeout(
            _STARTUP_EXTEND_USEC,
            "waiting for the previous daemon's workers to exit",
        )


def _acquire_presence_lock(resolved):
    """`daemonlock.acquire(resolved)`, except that a lock held by the fork
    orphans of a verifiably dead supervisor is waited out rather than refused.

    `DaemonAlreadyRunning` is re-raised unless the recorded supervisor is
    `'gone'` or `'mismatch'` by `procident.identity(e.pid, e.start)`: then the
    holders are workers of a daemon that was `kill -9`'d, which exit at their
    next safe point (a transfer in progress is never interrupted). Retried
    every second, with a WARNING at the start and every 60s. `'match'` (a live
    daemon) and `'unknown'` (legacy file, no token, unreadable) are refused as
    before. SIGINT/SIGTERM keep their default dispositions here, so the wait
    stays killable.
    """
    last_warned = None
    budget = _ExtendBudget("the previous daemon's workers to exit")
    while True:
        try:
            return daemonlock.acquire(resolved)
        except daemonlock.DaemonAlreadyRunning as e:
            if procident.identity(e.pid, e.start) not in ('gone', 'mismatch'):
                raise
            now = _startup_monotonic()
            if last_warned is None or now - last_warned >= _STARTUP_WARN_EVERY:
                last_warned = now
                logger.warning(
                    f"{daemonlock.stale_supervisor_message(e.path, e.pid)}; "
                    "waiting for them to exit (they stop at their next safe "
                    "point and a transfer in progress is never interrupted)"
                )
            budget.extend()
            _startup_sleep(_STARTUP_WAIT_POLL)


def _wait_for_previous_workers(resolved) -> None:
    """Block while workers of a previous daemon still hold
    `<db>.workers.lock` (item 18.5). Under `spawn`/`forkserver` such survivors
    hold no presence lock, so without this a restarted daemon could overlap
    with a worker finishing its last dataset. Called right after the presence
    lock is taken and before the config is read.

    A detection aid, not a gate: if the lock file cannot be created or
    inspected, log a WARNING and carry on. SIGINT/SIGTERM keep their default
    dispositions during the wait (no workers exist yet; stay killable).
    """
    try:
        path = daemonlock.ensure_workers_lock(resolved)
        present = daemonlock.workers_present(resolved)
    except (daemonlock.DaemonLockError, OSError) as e:
        logger.warning(f"cannot check for workers of a previous daemon: {e}")
        return
    last_warned = None
    budget = _ExtendBudget(f"workers of a previous daemon to release {path}")
    while present:
        now = _startup_monotonic()
        if last_warned is None or now - last_warned >= _STARTUP_WARN_EVERY:
            last_warned = now
            logger.warning(
                f"workers of a previous daemon still hold {path}; waiting for "
                "them to finish -- they exit at their next safe point and a "
                f"transfer in progress is never interrupted. Find them with `fuser {path}`"
            )
        budget.extend()
        _startup_sleep(_STARTUP_WAIT_POLL)
        try:
            present = daemonlock.workers_present(resolved)
        except (daemonlock.DaemonLockError, OSError) as e:
            logger.warning(f"cannot check for workers of a previous daemon: {e}")
            return


def _ensure_server_identity(resolved, dry_run: bool) -> bool:
    """Make sure the API server's TLS identity (`<db>.tls/server.{crt,key}`)
    exists and is usable, before the first fork; -> `False` to refuse the
    start (the reason is logged). Plan A4: the supervisor creates it at
    startup (or `zfsbackup-config trust token create` did earlier); the API
    worker only loads it.

    `--dry-run` (the flag, `dry_run`) creates nothing: a key is a persistent
    change. An existing identity is still validated exactly as the API worker
    will load it, so a dry run refuses what a real start would; a missing
    (or half-written) one is left missing and the API worker serves with a
    throwaway identity for that run (`workers._server_identity`). A stored
    `dry_run: true` is configuration, not this run's flag: the identity is
    created then, so a later reload to `dry_run: false` finds it."""
    tls = _tls_dir(resolved)
    cert, key = identity.identity_paths(tls, identity.SERVER_NAME)
    odd = _not_regular(cert, key)
    if odd is not None:
        logger.error(
            f"cannot set up the API server's TLS identity: {odd}; remove it "
            "(the daemon then generates a new identity, and every client must "
            "re-enroll) or put the right file in its place"
        )
        return False
    try:
        if dry_run:
            if not (os.path.lexists(cert) and os.path.lexists(key)):
                logger.warning(
                    f"dry run: there is no server TLS identity in {tls}, and a dry "
                    "run does not create one; the API worker serves with a "
                    "throwaway identity for this run (no client can use it)"
                )
                return True
            server = identity.load_identity(
                cert, key, identity.ROLE_SERVER, allow_expired=True
            )
        else:
            server = identity.ensure_identity(tls, identity.ROLE_SERVER)
    except identity.IdentityError as e:
        logger.error(
            f"cannot set up the API server's TLS identity: {e}. The API serves "
            "HTTPS only and cannot start without it; fix this and start the "
            "daemon again"
        )
        return False
    except Exception as e:
        logger.error(
            f"cannot set up the API server's TLS identity in {tls}: "
            f"{type(e).__name__}: {e}",
            exc_info=True,
        )
        return False
    logger.info(
        f"server TLS identity {server.cert_path} (fingerprint {server.fingerprint})"
    )
    return True


def _report_client_tls(
    config: BackupConfig, client_id: Optional[str], tls: Optional[Path], generation
) -> None:
    """Startup (and `--test-config`): log this host's client id and client
    certificate fingerprint at INFO, and every `_client_tls_problem` at
    WARNING. Advisory only; never raises."""
    if client_id is not None:
        logger.info(f"client id: {client_id}")
    else:
        logger.info("client id: not set yet (`zfsbackup-config enroll` generates it)")
    try:
        problems, client = _client_tls_problem(config, client_id, tls)
    except Exception as e:
        logger.warning(
            f"configuration generation {generation}: the remote backup check "
            f"failed to run: {type(e).__name__}: {e}"
        )
        return
    if client is not None:
        logger.info(
            f"client TLS identity {client.cert_path} (fingerprint {client.fingerprint})"
        )
    for problem in problems:
        logger.warning(
            f"configuration generation {generation}: remote backup: {problem}; "
            "local snapshots and pruning are unaffected"
        )


def _report_tls_state(resolved, config: BackupConfig, client_id: Optional[str]) -> None:
    """`--test-config`: the TLS state, read-only -- nothing is created, no
    key is needed for the server's fingerprint (`peek_identity`)."""
    tls = _tls_dir(resolved)
    try:
        server = identity.peek_identity(tls, identity.SERVER_NAME)
    except identity.IdentityError as e:
        logger.warning(f"Server TLS identity: unusable: {e}")
    else:
        if server is None:
            logger.info(
                f"Server TLS identity: none yet in {tls} (the daemon creates it "
                "when it starts)"
            )
        else:
            logger.info(
                f"Server TLS identity: fingerprint {server.fingerprint}, expires "
                f"{server.not_after:%Y-%m-%d}"
            )
    _report_client_tls(config, client_id, tls, "under test")


def _release_refused(lock) -> None:
    """A startup refusal after the presence lock was taken (the config failed
    to load): remove the pid file and drop the lock, so a refused start leaves
    nothing behind and does not look like a running daemon to `probe()`."""
    if lock is not None:
        lock.release()


class _StartupHup:
    """A flag-only SIGHUP handler for the window before `BackupDaemon` exists.

    The presence lock is taken at the top of `_run`, i.e. the daemon is
    already visible to `probe()` -- and so to `zfsbackup-config`'s
    auto-reload SIGHUP -- long before `BackupDaemon.__init__` installs the
    real handler. In that window SIGHUP has its default action, terminate,
    so a config edit racing a daemon start killed the starting daemon
    (measured 8/8). This handler does one plain store and returns; `_run`
    carries the result into the daemon's own `_hup_requested` (set-only), and
    `run()`'s startup `_reload_tick` -- before the first spawn -- forces the
    read, so a write that raced the start is applied with no worker turnover.

    Deliberately SIGHUP only. SIGINT/SIGTERM keep their default
    dispositions until `BackupDaemon.__init__`: before the first fork there
    are no workers to orphan, the kernel drops the lock, a stale pid file is
    harmless, and a startup stuck in slow I/O must stay killable. After the
    daemon exists the three signals are all handled by `BackupDaemon`.
    """

    def __init__(self) -> None:
        self.pending = False
        #: Set once `BackupDaemon` owns the signals; `restore` is then a no-op.
        self.handed_over = False
        self._previous = None
        self._installed = False

    def _handler(self, signum, frame) -> None:
        self.pending = True

    def install(self) -> None:
        try:
            self._previous = signal.signal(signal.SIGHUP, self._handler)
            self._installed = True
        except ValueError:  # not the main thread (embedded/test use)
            pass

    def restore(self) -> None:
        """Put back the previous disposition on a path that never created a
        daemon (refusal, `--test-config`); never after hand-over."""
        if self._installed and not self.handed_over:
            self._installed = False
            signal.signal(signal.SIGHUP, self._previous)


def _run(args: argparse.Namespace, abandoned: Optional[List[str]] = None) -> int:
    """Install the startup SIGHUP handler (see `_StartupHup`), then
    `_run_startup`. Installed before anything else -- in particular before the
    presence lock -- and left in place until `BackupDaemon` replaces it.
    """
    # Every full read runs Alembic's schema check, which logs two routine
    # INFO lines ("Context impl SQLiteImpl", "Will assume non-transactional
    # DDL"). With live reload the supervisor reads on every accepted change,
    # so keep them out of the log unless something is actually wrong --
    # except under --verbose, which shows them as before.
    if not getattr(args, 'verbose', False):
        logging.getLogger('alembic').setLevel(logging.WARNING)
    # Read once and REMOVED from the environment before anything can fork,
    # start a forkserver or exec `zfs`: neither workers nor `zfs` inherit it.
    # Consumed even under `--test-config`, which then sends nothing.
    global _startup_notifier
    notifier = sdnotify.Notifier.from_environ()
    if args.test_config:
        notifier = sdnotify.Notifier()
    _startup_notifier = notifier
    early_hup = _StartupHup()
    if not args.test_config:  # `--test-config` takes no lock and is never probed
        early_hup.install()
    try:
        return _run_startup(args, abandoned, early_hup, notifier)
    finally:
        _startup_notifier = sdnotify.Notifier()
        # Drop the engines BEFORE SIGHUP gets its default action (terminate)
        # back: a refusal path still holds a pooled connection here, and an
        # auto-reload SIGHUP landing between a restore and `main()`'s own
        # `dispose_all()` would kill the process with it open. Idempotent,
        # so `main()` repeating it is harmless.
        dispose_all()
        early_hup.restore()


def _run_startup(
    args: argparse.Namespace,
    abandoned: Optional[List[str]],
    early_hup: _StartupHup,
    notifier: Optional[sdnotify.Notifier] = None,
) -> int:
    """`main()`'s body, minus argument parsing and the `dispose_all()` that
    must run on every return path. Split out so that cleanup is a single
    `finally` rather than something each `return` has to remember.

    Startup order: resolve the path -> preflight (`check_config_db`, creates
    nothing) -> take the presence lock -> wait out a predecessor's workers ->
    read the config (and the client id, one snapshot) -> `validate_config`
    -> the API bind probe -> the server TLS identity (`_ensure_server_identity`;
    nothing is created under `--dry-run`) -> the remote-backup advisory ->
    record its key as `applied=` in the lock -> `BackupDaemon.run`.
    `--test-config` skips the lock, the probe and the identity creation but
    not the validation, and reports the TLS state read-only. Any refusal
    after the lock was taken releases it.
    """
    resolved = None
    lock = None
    try:
        # Inside the `try`: resolution itself performs filesystem I/O
        # (`Path.resolve()` walks each component) and can raise
        # `ConfigDbPermissionError` on a symlink loop or an unlinked cwd.
        resolved = resolve_config_path(args.config)
        logger.info(f"Loading configuration from: {describe_config_source(resolved)}")

        # Presence lock (item 15.2(c)), taken BEFORE the config is read.
        # Order: resolve path -> preflight -> acquire -> read -> record the
        # generation. Were the read first, a `zfsbackup-config` write plus
        # its `probe()` landing between the read and the lock would print
        # "no running daemon detected" while this daemon went on to run the
        # OLD generation. With the lock first, any probe after this point
        # finds a daemon, and a write that lands before the read is simply
        # the generation this daemon loads.
        #
        # The preflight (`check_config_db`, stat-based, builds no engine and
        # creates nothing) comes first so a missing database or directory is
        # refused with the store's own message, never by creating a lock file
        # beside a database that does not exist. Not taken for
        # `--test-config`, which must work beside a running daemon. A second
        # daemon on the same database stops here. The descriptor is held for
        # the life of the process via `lock`; workers inherit it under
        # `fork` (see `zfsbackup.daemonlock`).
        if not args.test_config:
            check_config_db(resolved)
            try:
                lock = _acquire_presence_lock(resolved)
            except daemonlock.DaemonAlreadyRunning as e:
                logger.error(f"{e}. Refusing to start a second daemon.")
                return 1
            except daemonlock.DaemonLockError as e:
                logger.error(f"Cannot take the daemon lock: {e}")
                return 1
            # Survivors of a previous daemon (item 18.5), before the config
            # is read and long before the first fork.
            _wait_for_previous_workers(resolved)

        # The daemon's startup read of the config database: a single read
        # snapshot in which the schema check and the rows are the same
        # database state. The supervisor hands the resulting detached
        # `BackupConfig` to its workers, which never open the database
        # (item 15.2(b)); later reads are the supervisor's reload polls.
        # Disposed in a `finally`, so not even a failed read leaves a pooled
        # reader connection to sit on through the long dataset verification
        # below or to carry towards the first fork. `_spawn` repeats the
        # dispose at every fork.
        try:
            snapshot = load_runtime_config(resolved)
        finally:
            dispose_all()
        config = snapshot.config
        generation = snapshot.generation
        key = (snapshot.store_id, generation)
        logger.info(
            f"Configuration generation {generation}, "
            f"schema revision {snapshot.schema_rev}"
        )
        # D10: the same check every reload applies, so a configuration this
        # daemon would refuse on a reload is never run at startup either. A
        # `ValueError` is an operator-grade message: no traceback.
        try:
            warnings = validate_config(config)
        except ValueError as e:
            logger.error(f"configuration generation {generation} is invalid: {e}")
            _release_refused(lock)
            return 1
        for warning in warnings:
            logger.warning(f"configuration generation {generation}: {warning}")
        # And the build every worker does first (`_build_manager`): a
        # dataset list no worker could start with -- an invalid ZFS name --
        # is refused here, cleanly, rather than by six crash-loops per worker
        # and a "Daemon failed" exit minutes later. Same refusal as above.
        try:
            _build_manager(config)
        except Exception as e:
            logger.error(
                f"configuration generation {generation} cannot be used: "
                f"its dataset list cannot be built: {e}",
                exc_info=not isinstance(e, ValueError),
            )
            _release_refused(lock)
            return 1
        # B3, D10: nor may a config be run at startup that a reload would
        # refuse for what the API worker does first -- the bind (a D9 crash
        # loop). A remote-backup problem (client id, client certificate,
        # enrollment) only warns: it breaks remote backup, not the daemon.
        # Not for `--test-config`: it must work beside a running daemon
        # (which holds the API port) and under any user.
        if not args.test_config:
            try:
                refusal = _candidate_refusal(config)
            except Exception as e:
                # The read succeeded: not a "Failed to load configuration".
                logger.error(
                    f"configuration generation {generation}: the pre-start "
                    f"check (API bind) failed to run: "
                    f"{type(e).__name__}: {e}",
                    exc_info=True,
                )
                _release_refused(lock)
                return 1
            if refusal is not None:
                logger.error(
                    f"configuration generation {generation} cannot be used: {refusal}"
                )
                _release_refused(lock)
                return 1
            # The API server's TLS identity (plan A4), after the presence
            # lock and before the first fork: the API worker only loads it.
            # A failure refuses the start (HTTPS only; it would crash-loop).
            if not _ensure_server_identity(resolved, bool(args.dry_run)):
                _release_refused(lock)
                return 1
            _report_client_tls(
                config, snapshot.client_id, _tls_dir(resolved), generation
            )
        # The key recorded in the lock is the one the workers are about to
        # run, which is what `zfsbackup-config` reports to an operator after
        # an edit (`applied=` also rewrites line 2 with the generation).
        # Until this line the file's generation is empty ("not known yet").
        if lock is not None:
            lock.set_state(applied=key)

        # In memory only, and BEFORE the config is handed to any worker.
        # `--dry-run` is a property of this run, not of the stored
        # configuration, and the daemon never writes to the database. Every
        # config a reload reads gets it again (`dry_run_override`).
        if args.dry_run:
            config.dry_run = True

        if args.test_config:
            logger.info("Configuration is valid!")
            # Proof that the schema was actually verified, not merely that
            # some rows could be read: `schema_rev` is what the check inside
            # the snapshot returned. Nothing was migrated or created.
            logger.info(f"Schema revision: {snapshot.schema_rev}")
            logger.info(f"Datasets: {len(config.datasets)}")
            for ds in config.datasets:
                logger.info(f"  - {ds.name} (enabled={ds.enabled})")
            if config.destinations:
                logger.info(f"Destinations: {list(config.destinations)}")
            if config.remote_backup:
                logger.info(f"Remote backup target: {config.remote_backup.target_dataset}")
            _report_tls_state(resolved, config, snapshot.client_id)
            return 0

    except (ConfigPathError, SchemaError) as e:
        # Logged VERBATIM and with no traceback. These messages are already
        # operator-grade -- they name the file, the candidate sources, and
        # in `ConfigDbIsYaml`'s case a copy-pasteable
        # `zfsbackup-config import <path>`. A "Failed to load
        # configuration: " prefix would wrap a multi-line message whose
        # last line is meant to be pasted into a shell.
        logger.error(config_error_message(resolved, e))
        _release_refused(lock)
        return 1
    except Exception as e:
        # Also `config_error_message`, not a bare f-string, and this arm is
        # the only way an `OperationalError` is diagnosed at all: the
        # preflight can pass and the real `engine.connect()` still fail,
        # routinely, because the daemon runs as root and `os.access` is
        # uid-based, permissive for root, and blind to POSIX ACLs. A
        # directory ACL, SELinux, or a read-only bind mount lands here.
        # Without this, the operator got "(sqlite3.OperationalError) unable
        # to open database file" and a SQLAlchemy documentation link;
        # `config_error_message` re-runs the preflight via
        # `diagnose_open_failure` and names the actual problem.
        # `exc_info` for everything that is not a database failure (already
        # diagnosed, with its cause, by `config_error_message`; a plain
        # `DatabaseError` is a damaged file and gets its own one-liner): a
        # bare `KeyError` or `ValueError` from the mapper is otherwise
        # undiagnosable from the log.
        logger.error(
            config_error_message(resolved, e),
            exc_info=not (isinstance(e, OperationalError) or type(e) is DatabaseError),
        )
        _release_refused(lock)
        return 1

    daemon = None
    clean = False
    try:
        daemon = BackupDaemon(
            config,
            resolved,
            verbose=args.verbose,
            key=key,
            presence_lock=lock,
            dry_run_override=bool(args.dry_run),
            workers_lock=daemonlock.workers_lock_path(resolved),
            notifier=notifier,
            client_id=snapshot.client_id,
        )
        # `BackupDaemon.__init__` has replaced the startup handler; a SIGHUP
        # that landed before that is carried over. A SIGHUP landing between
        # the replacement and this line went to the daemon's own handler.
        early_hup.handed_over = True
        # Only ever SET the flag: a read-modify-write here could overwrite a
        # SIGHUP that the daemon's handler recorded between the read and the
        # store.
        if early_hup.pending:
            daemon._hup_requested = True
        code = daemon.run()
        clean = True
        return code
    except Exception as e:
        logger.error(f"Daemon failed: {e}", exc_info=True)
        return 1
    finally:
        # Reported on every path out, including a crashed `run()`: its
        # `finally` has already run `_shutdown_workers`, which is what
        # records the abandoned ones.
        if abandoned is not None and daemon is not None:
            abandoned.extend(daemon.abandoned_workers)
        # Remove the pid file only on a clean stop with no abandoned
        # workers. After a crash, or when workers survived SIGKILL (the
        # `_hard_exit` path -- under fork they still hold the inherited
        # lock, correctly: they are still running), leave the file: the
        # kernel drops our lock on exit, and a stale file is harmless
        # because "running" means "lock held", never "file exists".
        if clean and not (daemon is not None and daemon.abandoned_workers):
            lock.release()


def _process_main() -> int:  # pragma: no cover - the real process only
    """`main()` for the daemon process itself: SIGHUP is ignored again on the
    way out, whatever path `main()` took, so a SIGHUP during interpreter
    shutdown (multiprocessing's atexit joins, module teardown) cannot turn an
    exit status into a signal death. Not part of `main()`, so embedded and
    test callers keep their own disposition (`_StartupHup.restore()`)."""
    try:
        return main()
    finally:
        signal.signal(signal.SIGHUP, signal.SIG_IGN)


if __name__ == '__main__':  # pragma: no cover
    sys.exit(_process_main())

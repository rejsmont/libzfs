"""The item-8x regression test: a real `SIGTERM`/`SIGINT` sent to a real
`python -m zfsbackup.daemon` process, after one of its real workers has
been `SIGKILL`ed while parked in `stop_event.wait()` -- the exact scenario
that permanently wedges `multiprocessing.Event.set()` (see
`zfsbackup/stopflag.py`'s module docstring and the approved item-8x plan).

This is the strongest regression test item 8x can have: every other test
either exercises `StopFlag` in isolation (`test_zfsbackup_stopflag.py`) or
drives `BackupDaemon`/`BaseWorker` with mocked workers
(`test_zfsbackup_daemon.py`, `test_zfsbackup_workers.py`). Nothing else
launches the actual CLI entry point, kills a real OS process abruptly, and
asserts the supervisor still answers a real signal.

**Marked `integration`, not `real_zfs`.** A `--dry-run` daemon starts all
three default workers (snapshot, pruning, api) against a fixture SQLite
config database that names a dataset which does not exist on this host --
`DatasetManager.verify_datasets()` logs an error for it and does not abort,
and `BaseWorker.run()` catches any exception a per-dataset ZFS call raises
around `_process_dataset`. No pool, and no `zpool`/delegated `zfs allow`,
is required -- only the `zfs` CLI binary itself, which `verify_datasets()`
and `needs_snapshot()`/`prune_snapshots()` still shell out to even in dry
run.

**No longer skips when `zfs` is absent.** The previous version of this
module skipped entirely on a host with no `zfs` binary on `PATH` and no
`$ZFS_CMD` -- which meant this module's headline regression (and its own
sensitivity control) never ran in CI at all: `.github/workflows/tests.yml`
installs only Python and Poetry on both `ubuntu-latest` and `macos-latest`,
neither of which ships `zfsutils`. `libzfseasy._zfs_cmd()` re-reads
`ZFS_CMD` at call time (not just at import), so `_resolve_zfs_cmd` below
writes a tiny stub `zfs` script -- exit 0, empty stdout, enough for
`zfs.exists()` to see zero results and report "dataset does not exist"
exactly as `verify_datasets()` does today, and enough for the `zfs.set()`/
`zfs.list()` calls `sync_all_config_properties()`/`needs_snapshot()`/
`prune_snapshots()` make in `--dry-run` mode (never `zfs.snapshot()` or
`zfs.destroy()`, which dry-run skips before ever building a command) to
succeed as no-ops -- and passes it via `ZFS_CMD` in `_spawn_daemon`'s
`env`. A real `zfs` on `PATH` (or an already-set `$ZFS_CMD`) is still
preferred when available, so this changes nothing on a dev machine that
already has ZFS installed; it only removes the CI blind spot.

**The trap this test is designed around:** waiting for the supervisor's
own `Started {name} worker (pid=...)` line is NOT sufficient readiness --
that line is logged the instant `Process.start()` returns, before the
worker has even reached `_init_child_process`, let alone its first
`stop_event.wait()`. Killing at that point can SIGKILL a worker that was
never registered as an `Event` sleeper at all, which would make the
regression test pass whether or not the fix is present -- proving nothing.
Readiness here is keyed off each *worker's own* log line instead:

    snapshot/pruning: INFO "{worker-name} started" -- the worker's own
        first log line, proved (by measurement in this fixture) to be
        followed by a single, sub-50ms dataset-processing pass and then
        `stop_event.wait(timeout=interval)` -- plus `KILL_SETTLE` margin
        sized well above that.
    api:              INFO "API listening on {host}:{port}" (emitted the
        statement immediately before the untimed `stop_event.wait()`)

Not the `BaseWorker.run()` DEBUG "{name} cycle" line the loop body logs
immediately before dataset processing, which would be the tighter marker
in-process tests can use (see `test_zfsbackup_workers.py`): `-v`'s
`logging.getLogger().setLevel(logging.DEBUG)` in `main()` only raises the
level in the **supervisor's own** process. Because this daemon is launched
via `python -m zfsbackup.daemon`, `multiprocessing`'s `spawn` start method
re-imports `zfsbackup.daemon` in every child to reconstruct `__main__` --
running its module-level `logging.basicConfig(level=logging.INFO, ...)`
again before `workers._init_child_process`'s own
`_setup_logging(verbose=True)` gets a chance, and `logging.basicConfig` is
a no-op once handlers already exist. Measured: workers never emit a single
DEBUG line under this launch shape, `-v` and all. Real, but orthogonal to
item 8x, and not this module's to fix -- the INFO markers above hold
regardless.

**Two more flakiness hazards, both designed around:**
- `communicate()` can hang forever once the supervisor is dead, because an
  orphaned worker still holds the write end of the stdout pipe. This
  module never uses a `PIPE` at all -- the daemon's stdout/stderr are
  redirected straight to a file, tailed by polling `Path.read_text()`, and
  every assertion is on `Popen.wait(timeout=...)`, never `communicate()`.
- Teardown unconditionally does
  `os.killpg(os.getpgid(proc.pid), SIGKILL)` in a `finally` --
  `start_new_session=True` is what makes that a distinct process group in
  the first place. Without it, a regression that leaves the supervisor (or
  a worker) alive leaks a live orphan into the pytest host.

Assertions are on **events** -- exit code 0, `"Daemon stopped"` in the log
-- never on measured durations. The reference measurements after the fix
(killing snapshot, pruning, or the api worker, then SIGTERM or SIGINT) were
0.265-0.495s in all six combinations; the timeout budget here is 30s, pure
headroom.

**Mutation-check / sensitivity control**
(`TestSensitivityControlDaemonWithRawEvent`): proves this harness can
actually detect the defect it exists to catch, exactly as
`test_zfsbackup_stopflag.py::TestSensitivityControlMpEventWedges` does for
`StopFlag` in isolation. Re-running the *production* `python -m
zfsbackup.daemon` entry point against the pre-8x `Event`-based mechanism
would require either reverting committed-but-superseded source or adding a
test-only environment hook to `daemon.py` purely to make that possible --
both worse than the alternative used here: an **in-process canary** that
constructs the real, current `BackupDaemon`, then substitutes a bare
`multiprocessing.Event()` for `self._stop_event` *before* calling
`.run()`. Every other line that executes -- `_spawn`, `_check_workers`,
`_supervisor_sleep`, `_shutdown_workers`, `run()`, and every worker in
`workers.py` -- is the exact, unmodified, currently-installed source;
`workers.StopEvent`'s duck-typed `Union[StopFlag, Event]` is what makes
the substitution possible without touching a single call site. Only the
object behind `.set()`/`.is_set()`/`.wait()` differs, which is the entire
defect under test. This control is expected to leave its own harness
process genuinely wedged, so its teardown kills it unconditionally too.
"""

import multiprocessing
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parent.parent

#: How long to wait for the supervisor's "Started ... worker" line and for
#: the worker's own "genuinely parked" marker after that.
READY_TIMEOUT = 20.0
#: Settle between the worker's readiness marker appearing and the SIGKILL,
#: mirroring `test_zfsbackup_stopflag.py`'s `KILL_SETTLE` -- long enough
#: that the worker has actually reached `stop_event.wait()` (registered as
#: an `Event` sleeper, where relevant), not merely logged the line before
#: it. Generous relative to the measured sub-50ms gap between a worker's
#: own "started" line and its first `stop_event.wait()` in this fixture
#: (one dataset, dry run, no existing snapshots).
KILL_SETTLE = 1.0
#: Pure headroom: measured post-fix latency was 0.265-0.495s.
SHUTDOWN_TIMEOUT = 30.0
#: Deliberately short for the sensitivity control -- it only has to prove
#: the harness is STILL ALIVE (wedged), not that it stays wedged forever.
SENSITIVITY_TIMEOUT = 5.0

_STARTED_RE = re.compile(r"Started (\w+) worker \(pid=(\d+)\)")

#: The log line that proves each worker type has gone all the way round
#: to (or into, for the API worker) its blocking wait -- not merely that
#: `Process.start()` returned. See module docstring.
_PARKED_MARKER = {
    'snapshot': 'snapshot-worker started',
    'pruning': 'pruning-worker started',
    'api': 'API listening on',
}


_STUB_ZFS_SCRIPT = "#!/bin/sh\n# stub zfs: always succeeds, empty stdout\nexit 0\n"


def _resolve_zfs_cmd(tmp_path: Path) -> str:
    """Return the `zfs` binary this module's daemon subprocess should use.

    Prefers whatever is already resolvable -- an explicit `$ZFS_CMD` or a
    real `zfs` on `PATH` -- unchanged, so a dev machine with ZFS installed
    exercises the genuine binary exactly as before. Only when NEITHER is
    present does it fall back to a freshly written stub script, which is
    what keeps this module out of CI's blind spot (see module docstring):
    the stub's `verify_datasets()`/`needs_snapshot()`/`prune_snapshots()`
    calls all succeed as no-ops against it in `--dry-run` mode, which is
    all this module's `--dry-run` daemon ever needs from `zfs`.
    """
    existing = os.environ.get('ZFS_CMD') or shutil.which('zfs')
    if existing:
        return existing
    stub_path = tmp_path / 'zfs'
    stub_path.write_text(_STUB_ZFS_SCRIPT)
    stub_path.chmod(0o755)
    return str(stub_path)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _build_config_db(tmp_path: Path):
    """A minimal, importable YAML config naming a dataset that cannot
    exist on this host, imported into a fresh SQLite store -- exactly the
    shape `zfsbackup-config import` / `daemon.py -c <db>` expects.
    """
    from zfsbackup.config.store import import_yaml, resolve_config_path

    dataset_name = f"zfsbackup_stopsig_test_{uuid.uuid4().hex[:8]}/nonexistent"
    api_port = _free_port()
    cfg_path = tmp_path / 'config.yaml'
    cfg_path.write_text(
        "datasets:\n"
        f"  - name: {dataset_name}\n"
        "api_host: 127.0.0.1\n"
        f"api_port: {api_port}\n"
    )
    db_path = tmp_path / 'config.db'
    resolved = resolve_config_path(str(db_path))
    import_yaml(cfg_path, resolved)
    return resolved


def _read_log(log_path: Path) -> str:
    try:
        return log_path.read_text(errors='replace')
    except FileNotFoundError:
        return ''


def _wait_for(log_path: Path, predicate, timeout: float):
    """Poll *log_path* until *predicate(text)* returns a truthy value (in
    which case it is returned) or *timeout* elapses (returns None).
    """
    deadline = time.monotonic() + timeout
    while True:
        text = _read_log(log_path)
        result = predicate(text)
        if result:
            return result
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.05)


def _worker_pid_from_text(text: str, worker_name: str):
    for m in _STARTED_RE.finditer(text):
        if m.group(1) == worker_name:
            return int(m.group(2))
    return None


def _wait_for_worker_parked(log_path: Path, worker_name: str, timeout: float):
    """Blocks until BOTH the supervisor's "Started {worker_name} worker
    (pid=...)" line and that worker's own readiness marker (see
    `_PARKED_MARKER`) are present, then returns the worker's pid.

    Deliberately two separate checks rather than one combined regex: the
    supervisor's line can appear well before the worker's own marker (the
    child still has to reset its signal dispositions, configure logging,
    and load its own config -- see `workers._init_child_process` and
    `workers._load_config`), and keying readiness off the supervisor's
    line alone is exactly the trap this module's docstring records.
    """
    marker = _PARKED_MARKER[worker_name]

    def _ready(text):
        pid = _worker_pid_from_text(text, worker_name)
        if pid is None:
            return None
        if marker not in text:
            return None
        return pid

    return _wait_for(log_path, _ready, timeout)


def _killpg_quiet(pid: int, sig: int = signal.SIGKILL) -> None:
    """Best-effort process-group kill. Never raises -- this runs from a
    `finally`, possibly after the target has already exited cleanly (in
    which case its pid, and any pgid lookup on it, may already be gone).
    """
    try:
        pgid = os.getpgid(pid)
    except (ProcessLookupError, PermissionError):
        return
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError):
        pass


@pytest.mark.unit
class TestResolveZfsCmd:
    """`_resolve_zfs_cmd` is the seam that keeps this module's headline
    regression (and its own sensitivity control) out of CI's blind spot --
    see the module docstring. A dev machine with ZFS installed must see
    NO change in behaviour (the real binary), and a machine with neither
    `$ZFS_CMD` nor `zfs` on `PATH` must get a working stub instead of a
    skip. Covered directly here, not only through the effect of
    `TestDaemonRealSignalShutdown` always running.
    """

    def test_prefers_existing_zfs_cmd_env_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv('ZFS_CMD', '/some/real/zfs')
        assert _resolve_zfs_cmd(tmp_path) == '/some/real/zfs'
        # And it must not have written a stub nobody asked for.
        assert not (tmp_path / 'zfs').exists()

    def test_prefers_path_zfs_when_zfs_cmd_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv('ZFS_CMD', raising=False)
        monkeypatch.setattr(
            shutil, 'which', lambda name: '/usr/bin/zfs' if name == 'zfs' else None
        )
        assert _resolve_zfs_cmd(tmp_path) == '/usr/bin/zfs'
        assert not (tmp_path / 'zfs').exists()

    def test_writes_a_working_executable_stub_when_neither_is_present(
        self, tmp_path, monkeypatch
    ):
        """The actual CI-fix path: no `$ZFS_CMD`, no `zfs` on `PATH` --
        exactly the state of both GitHub Actions jobs in
        `.github/workflows/tests.yml` today. The stub itself must be a
        real, executable file that exits 0 with empty stdout, since that
        is what makes `verify_datasets()`'s `zfs list` call report a
        (correctly) missing dataset instead of raising.
        """
        monkeypatch.delenv('ZFS_CMD', raising=False)
        monkeypatch.setattr(shutil, 'which', lambda name: None)

        cmd = _resolve_zfs_cmd(tmp_path)

        assert cmd == str(tmp_path / 'zfs')
        assert os.access(cmd, os.X_OK)
        result = subprocess.run(
            [cmd, 'list', '-H', 'some/nonexistent/dataset'],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert result.stdout == ''
        assert result.stderr == ''


@pytest.fixture
def stop_signal_config_db(tmp_path, monkeypatch):
    # `monkeypatch.setenv` (not a plain `os.environ[...] = ...`) so this is
    # automatically reverted at the end of the test -- both `_spawn_daemon`
    # (which copies `os.environ` wholesale into the real subprocess's env)
    # and the sensitivity control's `ctx.Process(...)` (a `spawn`-context
    # child, which inherits the parent's environment at `start()` time, not
    # an explicit `env=` mapping) pick this up without any further plumbing.
    monkeypatch.setenv('ZFS_CMD', _resolve_zfs_cmd(tmp_path))
    return _build_config_db(tmp_path)


def _spawn_daemon(db_path: Path, log_path: Path):
    """Starts the real CLI entry point in its own process group (never a
    `PIPE`, see module docstring) and returns the `Popen` handle. The
    caller is responsible for tearing it down via `_killpg_quiet`.
    """
    env = dict(os.environ)
    # Logging module handlers flush per record, but the underlying file
    # object Python opens for a non-tty stdout is still block-buffered by
    # default -- without this, `_wait_for_worker_parked` can spin for a
    # full OS buffer flush interval waiting for bytes that were written
    # long ago.
    env['PYTHONUNBUFFERED'] = '1'
    log_fh = open(log_path, 'wb')
    proc = subprocess.Popen(
        [sys.executable, '-m', 'zfsbackup.daemon', '-c', str(db_path), '--dry-run', '-v'],
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        cwd=str(REPO_ROOT),
        env=env,
        start_new_session=True,
    )
    return proc, log_fh


# ---------------------------------------------------------------------------
# The regression proper.
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestDaemonRealSignalShutdown:
    """Kill one real worker abruptly, then send the supervisor a real
    signal, and assert it still answers -- the production-path analogue of
    `test_zfsbackup_stopflag.py`'s child-process canaries.

    Parametrised over all three default `--dry-run` workers (snapshot,
    pruning, and the API worker's distinctive *untimed* `wait()`) crossed
    with both signals the daemon actually installs handlers for.
    """

    @pytest.mark.parametrize('worker_name', ['snapshot', 'pruning', 'api'])
    @pytest.mark.parametrize(
        'stop_signal', [signal.SIGTERM, signal.SIGINT], ids=lambda s: s.name
    )
    def test_stop_after_worker_killed_mid_wait(
        self, tmp_path, stop_signal_config_db, worker_name, stop_signal
    ):
        log_path = tmp_path / 'daemon.log'
        proc, log_fh = _spawn_daemon(stop_signal_config_db.path, log_path)
        try:
            worker_pid = _wait_for_worker_parked(log_path, worker_name, READY_TIMEOUT)
            assert worker_pid is not None, (
                f"{worker_name} worker never reported parked in wait() -- log:\n"
                f"{_read_log(log_path)}"
            )

            # Let it actually settle into the blocking wait, not merely
            # have logged the line immediately before it.
            time.sleep(KILL_SETTLE)

            # THE THING NO PRE-8x TEST COULD DO: SIGKILL a worker parked
            # in stop_event.wait(), producing exactly one ghost sleeper.
            os.kill(worker_pid, signal.SIGKILL)

            # THE ASSERTION: the supervisor must still answer a real
            # signal after that, and exit cleanly.
            os.kill(proc.pid, stop_signal)

            returncode = proc.wait(timeout=SHUTDOWN_TIMEOUT)
            log_text = _read_log(log_path)
            assert returncode == 0, (
                f"daemon did not exit 0 after {signal.Signals(stop_signal).name} "
                f"following a killed {worker_name} worker -- log:\n{log_text}"
            )
            assert 'Daemon stopped' in log_text
        finally:
            log_fh.close()
            _killpg_quiet(proc.pid)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass


# ---------------------------------------------------------------------------
# Sensitivity control -- proves the harness above can tell "the fix works"
# apart from "the harness never reproduced the defect". See module
# docstring for why this is an in-process canary against a substituted
# `multiprocessing.Event()` rather than a second real CLI subprocess.
# ---------------------------------------------------------------------------


def _mp_event_daemon_harness(resolved_config, log_path: str) -> None:
    """Runs in a freshly spawned process. Detaches into its own process
    group (mirroring the real subprocess test's `start_new_session=True`,
    so the whole tree -- this process and every worker it forks -- can be
    torn down with a single `killpg`), redirects the real OS-level stdout
    /stderr fds to *log_path* (a plain `sys.stdout = ...` reassignment
    would not reach `workers.py`'s own child processes, which under
    `spawn`/`forkserver` inherit fd 1 directly rather than a Python
    object), then builds and runs a real `BackupDaemon` with its
    `_stop_event` swapped for a bare `multiprocessing.Event()` --
    reproducing the pre-8x defect via dependency substitution on the
    CURRENT source, not by reverting or duplicating any file.
    """
    os.setpgrp()
    log_fh = open(log_path, 'w', buffering=1)
    os.dup2(log_fh.fileno(), 1)
    os.dup2(log_fh.fileno(), 2)

    from zfsbackup.daemon import BackupDaemon
    from zfsbackup.runtime_config import load_runtime_config

    config = load_runtime_config(resolved_config)
    config.dry_run = True
    daemon = BackupDaemon(config, resolved_config, verbose=True)
    daemon._stop_event = multiprocessing.Event()
    daemon.run()


@pytest.mark.slow
class TestSensitivityControlDaemonWithRawEvent:
    """Same shape as `TestDaemonRealSignalShutdown` -- wait for a worker to
    report parked, `SIGKILL` it, then signal the supervisor -- but with the
    daemon's `_stop_event` forced back to a raw `multiprocessing.Event()`.
    The control passes if, and only if, the harness process is STILL ALIVE
    after `SENSITIVITY_TIMEOUT`: proof that this methodology really can
    distinguish the fix from a defect, rather than passing regardless of
    what the daemon does.
    """

    def test_harness_wedges_with_raw_event(self, tmp_path, stop_signal_config_db):
        log_path = tmp_path / 'sensitivity.log'
        ctx = multiprocessing.get_context('spawn')
        harness = ctx.Process(
            target=_mp_event_daemon_harness,
            args=(stop_signal_config_db, str(log_path)),
        )
        try:
            harness.start()

            worker_pid = _wait_for_worker_parked(log_path, 'snapshot', READY_TIMEOUT)
            assert worker_pid is not None, (
                "snapshot worker never reported parked in wait() -- sensitivity "
                f"control cannot proceed -- log:\n{_read_log(log_path)}"
            )
            time.sleep(KILL_SETTLE)

            os.kill(worker_pid, signal.SIGKILL)
            os.kill(harness.pid, signal.SIGTERM)

            harness.join(timeout=SENSITIVITY_TIMEOUT)

            # THE CONTROL: the harness must still be running -- a raw
            # Event's set() really is wedged inside this daemon's own
            # `_check_workers`/`run()` loop, proving this harness can tell
            # "returns promptly" (the StopFlag-backed fix) apart from
            # "never returns" (the pre-8x defect) rather than passing
            # vacuously.
            assert harness.is_alive(), (
                "sensitivity control failed: the raw-Event harness exited -- "
                "this test methodology cannot detect the item-8x defect at all, "
                "so every pass in TestDaemonRealSignalShutdown proves nothing"
            )
        finally:
            _killpg_quiet(harness.pid)
            harness.join(timeout=5)
            if harness.is_alive():
                harness.terminate()
                harness.join(timeout=5)

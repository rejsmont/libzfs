"""Pytest configuration and shared fixtures for libzfseasy tests."""

import os
import shutil

# FIRST, before anything imports `libzfseasy` (it resolves $ZFS_CMD / $ZPOOL_CMD at
# import time): a bare `pytest` on a host with ZFS installed must never end up
# pointing the suite -- or the subprocesses it starts, which inherit this
# environment -- at the real binaries. The REAL tier is opt-in (ZFSB_REAL_ZFS=1):
# without it nothing real is reachable and every `real_zfs` test is skipped. See
# tests/_hermetic.py.
from _hermetic import (
    REAL_TIER_REASON,
    apply_stub_policy,
    fork_record_dir,
    find_orphans,
    hermetic_env,
    installed_zfs_binaries,
    is_real_zfs,
    real_tier_enabled,
    register_fork_recorder,
    session_marker,
)

apply_stub_policy()
INSTALLED_ZFS = installed_zfs_binaries()    # from the ORIGINAL PATH, once

#: Marks every process this pytest session starts (children inherit the
#: environment); `pytest_sessionfinish` hunts for survivors by it. Always a fresh
#: value, except in an xdist worker of the controller that minted it (one run = one
#: marker): an inherited or exported value is never reused.
SESSION_MARKER = session_marker(os.environ, os.getppid())
#: Children forked WITHOUT exec carry the exec-time environment (no marker); their
#: pids are recorded here by an at-fork hook so the net can still find them.
FORK_DIR = fork_record_dir(SESSION_MARKER)
os.makedirs(FORK_DIR, exist_ok=True)
register_fork_recorder(FORK_DIR)

import pytest
from unittest.mock import Mock, MagicMock
from libzfseasy.types import Dataset, Filesystem, Volume, Snapshot, Bookmark, Property


@pytest.fixture
def mock_subprocess(mocker):
    """Mock subprocess.Popen to avoid actual ZFS commands."""
    mock_popen = mocker.patch('subprocess.Popen')
    
    # Queue to hold multiple mock configurations
    mock_queue = []
    
    def setup_mock(stdout='', stderr='', returncode=0):
        """Helper to configure mock subprocess behavior for a single call."""
        process_mock = MagicMock()
        
        # Setup stdout
        if isinstance(stdout, list):
            process_mock.stdout.readline.side_effect = stdout + ['']
        else:
            process_mock.stdout.readline.return_value = stdout
        
        # Setup stderr
        if isinstance(stderr, list):
            process_mock.stderr.readline.side_effect = stderr + ['']
        elif stderr == '':
            # Empty string already signals EOF on the very first read.
            process_mock.stderr.readline.return_value = ''
        else:
            # libzfseasy drains stderr on a dedicated background thread that
            # loops "while readline() != ''". A constant non-empty
            # return_value would make that thread (and any stderr_thread.join())
            # spin/hang forever, so terminate after a single line.
            process_mock.stderr.readline.side_effect = [stderr, '']
        
        # Setup return code
        process_mock.poll.side_effect = [None] * len(stdout) + [returncode] if isinstance(stdout, list) else [returncode]

        # Command._exec_capture() now falls back to process.wait() when
        # stdout hits EOF but poll() still returns None (rather than
        # spin-looping). Stub it to resolve to the same returncode that
        # poll() would eventually report, so that fallback path works too.
        process_mock.wait.return_value = returncode

        # Set as return_value and also add to queue
        mock_popen.return_value = process_mock
        mock_queue.append(process_mock)
        return process_mock
    
    def setup_multi(*configs):
        """Helper to configure multiple subprocess calls in sequence.
        
        Args:
            *configs: Tuples of (stdout, stderr, returncode) for each subprocess call
        """
        mock_queue.clear()
        mock_popen.side_effect = popen_side_effect  # Enable side_effect for multi-call
        for config in configs:
            if len(config) == 1:
                setup_mock(stdout=config[0])
            elif len(config) == 2:
                setup_mock(stdout=config[0], stderr=config[1])
            else:
                setup_mock(stdout=config[0], stderr=config[1], returncode=config[2])
    
    # Return mocks from queue in order
    def popen_side_effect(*args, **kwargs):
        if mock_queue:
            return mock_queue.pop(0)
        # Fallback - return a mock with poll() returning 0 immediately
        fallback = MagicMock()
        fallback.stdout.readline.return_value = ''
        fallback.stderr.readline.return_value = ''
        fallback.poll.return_value = 0
        return fallback
    
    mock_popen.setup = setup_mock
    mock_popen.setup_multi = setup_multi
    return mock_popen


@pytest.fixture
def sample_pool():
    """Return a sample pool name for testing."""
    return 'testpool'


@pytest.fixture
def sample_dataset(sample_pool):
    """Return a sample dataset object."""
    return Dataset(f'{sample_pool}/dataset')


@pytest.fixture
def sample_filesystem(sample_pool):
    """Return a sample filesystem object."""
    return Filesystem(f'{sample_pool}/filesystem')


@pytest.fixture
def sample_volume(sample_pool):
    """Return a sample volume object."""
    return Volume(f'{sample_pool}/volume')


@pytest.fixture
def sample_snapshot(sample_filesystem):
    """Return a sample snapshot object."""
    return Snapshot(sample_filesystem, 'snap1')


@pytest.fixture
def sample_bookmark(sample_filesystem):
    """Return a sample bookmark object."""
    return Bookmark(sample_filesystem, 'bookmark1')


@pytest.fixture
def mock_zfs_list_output():
    """Mock output from 'zfs list' command."""
    return [
        'testpool/filesystem\tfilesystem\n',
        'testpool/volume\tvolume\n',
        'testpool/filesystem@snap1\tsnapshot\n',
    ]


@pytest.fixture
def mock_zfs_get_output():
    """Mock output from 'zfs get' command."""
    return [
        'testpool/filesystem\tname\ttestpool/filesystem\t-\t-\n',
        'testpool/filesystem\ttype\tfilesystem\t-\t-\n',
        'testpool/filesystem\tcompression\tlz4\t-\tlocal\n',
        'testpool/filesystem\tmountpoint\t/mnt/test\t-\tlocal\n',
    ]


@pytest.fixture
def sample_properties():
    """Return a sample properties dictionary."""
    return {
        'compression': 'lz4',
        'mountpoint': '/mnt/test',
        'quota': '10G'
    }


@pytest.fixture
def dataset_properties():
    """Return properties valid for generic Dataset objects (no fs/volume extras)."""
    return {
        'compression': 'lz4',
        'reservation': '1G'
    }


@pytest.fixture
def sample_property_objects():
    """Return sample Property objects."""
    return {
        'compression': Property('lz4', source='local'),
        'mountpoint': Property('/mnt/test', source='local'),
        'quota': Property('10G', source='inherited')
    }


@pytest.fixture
def sample_backup_config():
    """Minimal in-memory BackupConfig for zfsbackup tests."""
    from zfsbackup.config import BackupConfig, DatasetConfig, RetentionRule
    from datetime import timedelta
    return BackupConfig(
        datasets=[
            DatasetConfig(
                name='pool/data',
                frequency=timedelta(hours=1),
                retention_rules=[RetentionRule(timedelta(days=1), timedelta(days=30))],
            ),
        ],
        snapshot_prefix='autosnap',
    )


@pytest.fixture
def config_with_remote(tmp_path):
    """BackupConfig with remote_backup enabled, writing config to a temp YAML file."""
    from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig
    from datetime import timedelta
    cfg_file = tmp_path / 'config.yaml'
    cfg_file.write_text(
        "datasets:\n"
        "  - name: pool/data\n"
        "remote_backup:\n"
        "  target_dataset: pool/backups\n"
        "  enabled: true\n"
    )
    return BackupConfig.from_file(cfg_file)


@pytest.fixture
def config_yaml_path(tmp_path):
    """Write a minimal valid YAML config to a temp file and return the Path.

    Kept exactly as it was before the daemon started reading the SQLite
    config store (item 8): this is now **importer input**, not daemon
    input -- `imported_config_db`/`sqlite_config_db` below are what daemon
    and worker tests should construct `BackupDaemon`/`BaseWorker` subclasses
    with.
    """
    cfg_file = tmp_path / 'config.yaml'
    cfg_file.write_text(
        "datasets:\n"
        "  - name: pool/data\n"
        "    frequency: 1h\n"
        "    retention:\n"
        "      1d: 30d\n"
    )
    return cfg_file


@pytest.fixture
def real_api_probe():
    """Opt-out marker fixture for `_default_api_port_is_not_probed`: a test
    (or class, via `usefixtures`) that exercises `daemon._probe_api_bind`
    itself requests this to get the real function even for port 8080."""


@pytest.fixture(autouse=True)
def _default_api_port_is_not_probed(request, monkeypatch):
    """Startup binds the API address once to refuse a port nobody can use
    (`daemon._probe_api_bind`, B3). Many tests import configs with the default
    `api_port` (8080) and go through startup, and would fail on any host where
    something listens on 127.0.0.1:8080. For the in-process `zfsbackup.daemon`
    only, port 8080 answers "bindable"; every other port is probed for real
    (or by the test's own patch). Narrow on purpose: request `real_api_probe`
    to keep the real function for 8080 too. Subprocess daemons are unaffected
    (the tests that start one pick a free port)."""
    if "real_api_probe" in request.fixturenames:
        return
    # Imported here, not looked up in `sys.modules`: a test that imports the
    # daemon lazily would otherwise probe 127.0.0.1:8080 for real.
    import zfsbackup.daemon as daemon

    real = daemon._probe_api_bind
    monkeypatch.setattr(
        daemon, "_probe_api_bind",
        lambda host, port: None if port == 8080 else real(host, port),
    )


@pytest.fixture(autouse=True)
def _clean_db_engine_cache():
    """Disposes every engine `zfsbackup.config.store.get_engine()` has
    cached, before and after every test in the whole suite.

    Moved here from `tests/test_zfsbackup_store.py` (item 8, sub-item 8j)
    and made autouse **session-wide** rather than file-scoped, and moved
    first among the fixtures this file defines. `db.py`'s pid-keyed
    `_ENGINES` cache is module-global state shared by the whole pytest
    process; the daemon and worker tests added alongside this fixture
    build engines through `zfsbackup.config.store.db` too (via
    `load_runtime_config`), so without this move a daemon/worker test run
    anywhere in the suite would leak a live engine (and its open DBAPI
    connection/pool) into an unrelated test -- exactly the class of hazard
    `db.py` exists to prevent process-wide, not just across a fork. Must
    land before any test that touches the config store, which is why it
    is a prerequisite for 8k.

    Imports `zfsbackup.config.store` lazily, inside the fixture body, so
    that `libzfseasy`-only test runs pay no cost for a package they never
    touch.
    """
    from zfsbackup.config.store import dispose_all

    dispose_all()
    yield
    dispose_all()


@pytest.fixture(autouse=True)
def _restore_store_logger_filters():
    """Strips `zfsbackup.api._RepeatFilter` from the config store's loggers
    after every test (and before, for a filter a non-test import left behind).

    WORKAROUND for product global state: `api.dedupe_store_warnings()` (called
    by `ApiWorker.run`, `workers.py`) permanently installs a `_RepeatFilter` on
    the process-wide `zfsbackup.config.store.paths` / `.db` loggers, "once per
    process", because in production `run()` is the body of a worker process.
    A test that calls `ApiWorker.run()` inline therefore installs it in the
    pytest process for the rest of the session, and the filter drops any later
    WARNING whose text was already seen within an hour -- so a later test that
    asserts "this warning was logged N times" sees fewer (observed:
    `TestJournalModeByEngineRole::test_every_flip_flop_is_reported_once`,
    `..._a_forked_child_warns_on_its_own...`, and
    `TestBusyTimeoutPerAttempt::test_a_failing_restore_does_not_stop_the_busy_retries`
    in the full suite, never alone). Generic on purpose: every current and
    future inline `ApiWorker.run()` test is covered, not just the ones that
    were found polluting. Looks `zfsbackup.api` up in `sys.modules` so
    libzfseasy-only runs pay nothing.
    """
    import logging
    import sys

    def strip():
        api = sys.modules.get("zfsbackup.api")
        if api is None:
            return
        for name in api._STORE_LOGGERS:
            lg = logging.getLogger(name)
            for f in [f for f in lg.filters if isinstance(f, api._RepeatFilter)]:
                lg.removeFilter(f)

    strip()
    yield
    strip()


@pytest.fixture(autouse=True)
def _never_run_a_real_zfs(request, monkeypatch):
    """Fails any test (outside the `real_zfs` tier) whose code starts a process
    that is a real `zfs` or `zpool`: a compiled program named `zfs` / `zpool`, or
    one that resolves to the binary found on `PATH` under those names (fake
    scripts named `zfs`, which start with `#!`, are fine).

    **This covers only the pytest process.** It wraps `Popen.__init__` here, so it
    sees what the code under test runs in-process; it cannot see what a
    subprocess runs. Subprocess safety comes from the ENVIRONMENT: `ZFS_CMD` /
    `ZPOOL_CMD` are set to a stub for the whole session when unset (top of this
    file), and the tests that start real daemons or CLIs build their environment
    with `_hermetic.hermetic_env()` (stubs, restricted `PATH`, never
    `which("zfs")`).

    The mocked tiers either patch `subprocess.Popen` wholesale (which this guard
    never sees) or point `ZFS_CMD` at a stub or a fake script
    (`tests/_fake_zfs.py`); a code path that slips past both and reaches a real
    binary would run against whatever pool the host happens to have. Wraps
    `Popen.__init__` in place, so the class identity (and every `isinstance`
    check) is unchanged.
    """
    if request.node.get_closest_marker("real_zfs"):
        yield
        return
    import shlex
    import subprocess

    real_init = subprocess.Popen.__init__
    installed = INSTALLED_ZFS

    def guarded(self, args, *a, **kw):
        if isinstance(args, bytes):
            args = os.fsdecode(args)
        if isinstance(args, str):
            try:
                program = (shlex.split(args) or [""])[0]
            except ValueError:
                program = args.split()[0] if args.split() else ""
        elif isinstance(args, os.PathLike):
            program = os.fspath(args)
        else:
            program = os.fsdecode(args[0]) if args else ""
        if program and is_real_zfs(program, installed):
            raise AssertionError(
                f"this test started a real {os.path.basename(program)!r} ({program}); use "
                "mock_subprocess or a fake script (tests/_fake_zfs.py) -- see _never_run_a_real_zfs"
            )
        return real_init(self, args, *a, **kw)

    monkeypatch.setattr(subprocess.Popen, "__init__", guarded)
    yield


def pytest_sessionfinish(session, exitstatus):
    """Safety net against leaked processes: after the last test, hunt for
    processes this session started -- carrying its `ZFSB_TEST_SESSION` marker (exec'd
    children inherit it), or recorded by the at-fork hook (children forked without
    exec) -- that are not live descendants of this process, i.e. left running after
    their test ended (reparented to init or a subreaper), such as a daemon's workers
    once the supervisor was SIGKILLed. They are killed, a loud warning is printed, and the
    session FAILS (set `ZFSB_ALLOW_ORPHANS=1` to downgrade that to the warning).
    Runs once, in the pytest-xdist controller or the plain process."""
    if hasattr(session.config, "workerinput"):
        return
    import signal

    try:
        leftover = find_orphans(SESSION_MARKER, fork_dir=FORK_DIR)
    finally:
        shutil.rmtree(FORK_DIR, ignore_errors=True)
    if not leftover:
        return
    for pid, _ppid, _state, _text in leftover:
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    lines = ["", "!" * 70, f"LEAKED PROCESSES: {len(leftover)} process(es) started by this test session were left running (now killed):"]
    for pid, ppid, state, text in leftover:
        command = text.split(" ZFSB_TEST_SESSION=")[0][:150]
        lines.append(f"  pid {pid} (ppid {ppid}, {state}): {command}")
    lines += ["A test must kill its whole process group (os.killpg(pid, SIGKILL), pgid == pid under",
              "start_new_session=True) independently of reaping the leader.", "!" * 70]
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    for line in lines:
        if reporter is not None:
            reporter.write_line(line, red=True, bold=True)
        else:
            print(line)
    if not os.environ.get("ZFSB_ALLOW_ORPHANS") and session.exitstatus == 0:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


_MISSING = object()
_LIBZFSEASY_BEFORE = {}


def _libzfseasy_state():
    """`(module attributes, {command singleton name: (singleton, its instance attributes)})`."""
    import libzfseasy
    from libzfseasy.zfs import Command

    module = dict(vars(libzfseasy))
    instances = {n: (o, dict(vars(o))) for n, o in module.items() if isinstance(o, Command)}
    return module, instances


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_setup(item):
    """Remember `libzfseasy`'s attributes before ANY fixture of the test runs."""
    _LIBZFSEASY_BEFORE[item.nodeid] = _libzfseasy_state()
    yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(item, nextitem):
    """Fails the test that leaves `libzfseasy` patched, instead of a random later one.

    `libzfseasy`'s command singletons (`zfs.exists`, `zfs.list`, `zfs.get.value`, ...) are shared
    by every test in the process, and a patch that is not undone silently changes what the next
    test calls. The classic way to leak one is two undo stacks over one attribute: `mocker.patch`
    and `monkeypatch.setattr` unwind in the order the FIXTURES were first requested, so when
    `monkeypatch` came before `mocker` the later `monkeypatch` undo put the earlier mock back
    (`libzfseasy.exists` stayed a `MagicMock` after `test_zfsbackup_api_protocol`'s websocket
    tests, which made two `test_zfsbackup_prune_descendants` tests fail only in multi-file runs).

    A hook wrapper, not a fixture, so the comparison happens after EVERY fixture finalizer has
    run. It restores whatever differs (one leak must not cascade) and then fails the test."""
    outcome = yield
    before = _LIBZFSEASY_BEFORE.pop(item.nodeid, None)
    if before is None:
        return
    import libzfseasy

    module_before, instances_before = before
    module = vars(libzfseasy)
    leaked = []
    for name, value in module_before.items():
        if not name.startswith("__") and module.get(name, _MISSING) is not value:
            leaked.append(f"libzfseasy.{name}")
            module[name] = value
    for name, (obj, attrs) in instances_before.items():
        now = vars(obj)
        for attr in set(now) | set(attrs):
            if not attr.startswith("__") and now.get(attr, _MISSING) is not attrs.get(attr, _MISSING):
                leaked.append(f"libzfseasy.{name}.{attr}")
                if attr in attrs:
                    now[attr] = attrs[attr]
                else:
                    del now[attr]
    if leaked and outcome.excinfo is None:
        raise pytest.fail.Exception(
            "this test left libzfseasy patched (undone now): " + ", ".join(sorted(leaked)), pytrace=False
        )


@pytest.fixture(autouse=True)
def _restore_signal_handlers():
    """Saves `SIGINT`/`SIGTERM`/`SIGHUP` dispositions before each test and restores
    them after, process-wide.

    `BackupDaemon.__init__` (`zfsbackup/daemon.py`) installs process-wide
    `signal.signal(SIGINT, ...)`/`signal.signal(SIGTERM, ...)` handlers as
    a plain construction side effect, and nearly every test in
    `tests/test_zfsbackup_daemon.py` constructs one. Without this fixture,
    the LAST `BackupDaemon` constructed anywhere in the whole pytest
    process silently becomes the live SIGINT/SIGTERM handler for the rest
    of the session -- bound to a `StopFlag` (`zfsbackup/stopflag.py`)
    whose owning daemon is long since garbage collected. Since item 8x
    replaced `BackupDaemon`'s stop mechanism with `StopFlag`, a stale
    handler here is genuinely inert: `self._stop_event.set()` is a plain
    store into shared memory nobody is left to read, not the
    `multiprocessing.Event.set()` that pre-8x could wedge the whole
    process on a ghost sleeper (see `tests/test_zfsbackup_stop_signals.py`
    for the regression test that proves that mechanism is fixed). This
    fixture still earns its place regardless -- leaving any stale handler
    bound to a dead daemon's flag is wrong on principle, and cheap
    (`getsignal`/`signal` are O(1) syscalls) to prevent. Autouse and
    session-wide, not just for `test_zfsbackup_daemon.py`: it protects any
    future test that touches signal dispositions the same way, not only
    the ones that exist today.
    """
    import signal

    # SIGHUP too (A-6.2): `BackupDaemon.__init__` installs the supervisor's
    # stopgap handler and `workers._init_child_process` sets SIG_IGN, and
    # either would otherwise outlive the test that triggered it.
    saved = {
        sig: signal.getsignal(sig)
        for sig in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGHUP", None))
        if sig is not None
    }
    yield
    for sig, handler in saved.items():
        if handler is not None:
            signal.signal(sig, handler)


@pytest.fixture
def imported_config_db(tmp_path, config_yaml_path):
    """Import `config_yaml_path` into a fresh SQLite config store under
    `tmp_path` and return the `ResolvedConfigPath` addressing it.

    This is the one fixture daemon/worker/basic tests should build
    `BackupDaemon`/`BaseWorker` subclasses with now that both read the
    config database, never a YAML file, at construction time (`daemon.py`,
    `workers.py`) -- passing a bare `Path` where a `ResolvedConfigPath` is
    required fails with `'PosixPath' object has no attribute 'url'` the
    first time anything actually opens the store.

    Goes through `import_yaml` (the sanctioned importer, item 8 sub-item
    8b) exactly as an operator running `zfsbackup-config import` would --
    never a raw `save_config`/`ensure_schema` call -- so the resulting
    database is schema-current and holds the same config
    `BackupConfig.from_file(config_yaml_path)` would produce.
    """
    from zfsbackup.config.store import import_yaml, resolve_config_path

    db_path = tmp_path / 'config.db'
    resolved = resolve_config_path(str(db_path))
    import_yaml(config_yaml_path, resolved)
    return resolved


#: A config whose Duration literals are deliberately NOT canonical (`60m`, not
#: `1h`; `600s`; `90m`; a per-destination `120m` and `52w`), so that a
#: handoff which re-synthesised literals from the numeric value would be
#: visible. See `tests/_config_handoff.py::config_signature`.
LITERAL_CONFIG_YAML = (
    "snapshot_prefix: zb\n"
    "check_interval: 600s\n"
    "prune_interval: 90m\n"
    "datasets:\n"
    "  - name: pool/data\n"
    "    frequency: 60m\n"
    "    retention:\n"
    "      1d: 4w\n"
    "      1w: 1M\n"
    "    remote:\n"
    "      - destination: offsite\n"
    "        frequency: 120m\n"
    "        retention:\n"
    "          1d: 52w\n"
    "  - name: pool/other\n"
    "    frequency: 1d\n"
    "destinations:\n"
    "  offsite:\n"
    "    url: https://backup.example.com:8080\n"
)


@pytest.fixture
def literal_config_db(tmp_path):
    """A schema-current config database imported from `LITERAL_CONFIG_YAML`,
    returned as a `ResolvedConfigPath`. Its dataset `pool/data` has a remote,
    so a daemon built from it runs all four workers."""
    from zfsbackup.config.store import import_yaml, resolve_config_path

    src = tmp_path / "literal.yaml"
    src.write_text(LITERAL_CONFIG_YAML)
    resolved = resolve_config_path(str(tmp_path / "literal.db"))
    import_yaml(src, resolved)
    return resolved


@pytest.fixture
def worker_config(sqlite_config_db):
    """The detached `BackupConfig` a worker is handed now (item 15.2(b)):
    read once from the imported database exactly as the supervisor does
    (`load_runtime_config`), with `--dry-run` applied in memory, and with the
    reader engine disposed so nothing in the test process still holds the
    database open. Workers are constructed with this -- never with a path.
    """
    from zfsbackup.config.store import dispose_all
    from zfsbackup.runtime_config import load_runtime_config

    config = load_runtime_config(sqlite_config_db).config
    dispose_all()
    config.dry_run = True
    return config


@pytest.fixture
def sqlite_config_db(imported_config_db):
    """Alias for `imported_config_db`, for daemon/worker test sites that
    read more naturally naming *what* the fixture is (a SQLite config
    database) rather than *how* it got there.
    """
    return imported_config_db


@pytest.fixture
def tls_config_db(sqlite_config_db):
    """`sqlite_config_db` plus the server TLS identity the supervisor would have
    created at startup (`<db>.tls/server.{crt,key}`): what a real `ApiWorker`
    needs next to its `config_db=`. Returns the `ResolvedConfigPath`. TLS
    helpers live in `tests/_tls_support.py`."""
    from _tls_support import ensure_server_identity

    ensure_server_identity(sqlite_config_db)
    return sqlite_config_db


def _auto_zfs_pool_local(tmp_path_factory):
    """Local pool creation path: file-backed pool via passwordless sudo."""
    import subprocess
    import os
    import stat
    from libzfseasy.zfs import ZFS_BIN, ZPOOL_BIN

    def _has_zfs():
        try:
            subprocess.run([ZFS_BIN, 'list'], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    if not _has_zfs():
        yield None
        return

    pool_name = f'zfsbackup_pytest_{os.getpid()}'
    disk_dir = tmp_path_factory.mktemp('zfs_pool')
    disk_image = str(disk_dir / 'disk.img')

    subprocess.run(['truncate', '-s', '512m', disk_image], check=True)

    result = subprocess.run(
        ['sudo', '-n', 'zpool', 'create', pool_name, disk_image],
        capture_output=True,
    )
    if result.returncode != 0:
        yield None
        return

    current_user = subprocess.run(
        ['id', '-un'], capture_output=True, text=True
    ).stdout.strip()
    perms = (
        'create,destroy,snapshot,clone,rename,mount,bookmark,'
        'receive,userprop,inherit,'
        'compression,mountpoint,quota,reservation,volsize,volblocksize'
    )
    # Delegation is best-effort; some platforms don't support zfs allow on pool root.
    subprocess.run(
        ['sudo', '-n', 'zfs', 'allow', '-u', current_user, perms, pool_name],
        capture_output=True,
    )

    # Check whether delegation actually worked; if not, install sudo wrapper scripts
    # so that _zfs_cmd()/_zpool_cmd() transparently prefix every command with sudo.
    _probe = f'{pool_name}/_probe'
    try:
        subprocess.run([ZFS_BIN, 'create', _probe], check=True, capture_output=True)
        subprocess.run([ZFS_BIN, 'destroy', _probe], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        script_dir = tmp_path_factory.mktemp('sudo_wrappers')
        for tool, bin_path in [('zfs', ZFS_BIN), ('zpool', ZPOOL_BIN)]:
            script = script_dir / tool
            script.write_text(f'#!/bin/bash\nexec sudo {bin_path} "$@"\n')
            script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        os.environ['ZFS_CMD'] = str(script_dir / 'zfs')
        os.environ['ZPOOL_CMD'] = str(script_dir / 'zpool')

    from libzfseasy.types import Dataset
    try:
        yield Dataset.from_name(pool_name)
    finally:
        subprocess.run(
            ['sudo', '-n', 'zpool', 'destroy', '-f', pool_name],
            capture_output=True,
        )
        os.environ.pop('ZFS_CMD', None)
        os.environ.pop('ZPOOL_CMD', None)


def _auto_zfs_pool_multipass(vm_name, tmp_path_factory):
    """Multipass pool creation path: pool lives inside a Linux VM."""
    import subprocess
    import os
    import stat

    # Start VM if suspended/stopped (best-effort; no-op if already running)
    subprocess.run(['multipass', 'start', vm_name], capture_output=True)

    result = subprocess.run(
        ['multipass', 'info', vm_name], capture_output=True, text=True
    )
    if 'Running' not in result.stdout:
        yield None
        return

    # Auto-install ZFS inside the VM if absent
    r = subprocess.run(
        ['multipass', 'exec', vm_name, '--', 'which', 'zfs'],
        capture_output=True,
    )
    if r.returncode != 0:
        subprocess.run(
            ['multipass', 'exec', vm_name, '--',
             'sudo', 'apt-get', 'install', '-y', 'zfsutils-linux'],
            check=True,
        )

    pool_name = f'zfsbackup_pytest_{os.getpid()}'
    disk_image = f'/tmp/{pool_name}.img'

    subprocess.run(
        ['multipass', 'exec', vm_name, '--', 'truncate', '-s', '512m', disk_image],
        check=True,
    )

    r = subprocess.run(
        ['multipass', 'exec', vm_name, '--',
         'sudo', 'zpool', 'create', pool_name, disk_image],
        capture_output=True,
    )
    if r.returncode != 0:
        subprocess.run(
            ['multipass', 'exec', vm_name, '--', 'rm', '-f', disk_image],
            capture_output=True,
        )
        yield None
        return

    # Create wrapper scripts so libzfseasy routes all zfs/zpool calls into the VM
    script_dir = tmp_path_factory.mktemp('multipass_wrappers')
    for tool in ('zfs', 'zpool'):
        script = script_dir / tool
        script.write_text(
            f'#!/bin/bash\nexec multipass exec {vm_name} -- sudo /usr/sbin/{tool} "$@"\n'
        )
        script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    # _zfs_cmd() / _zpool_cmd() re-read these env vars at call time
    os.environ['ZFS_CMD'] = str(script_dir / 'zfs')
    os.environ['ZPOOL_CMD'] = str(script_dir / 'zpool')

    from libzfseasy.types import Dataset
    try:
        yield Dataset.from_name(pool_name)
    finally:
        subprocess.run(
            ['multipass', 'exec', vm_name, '--',
             'sudo', 'zpool', 'destroy', '-f', pool_name],
            capture_output=True,
        )
        subprocess.run(
            ['multipass', 'exec', vm_name, '--', 'rm', '-f', disk_image],
            capture_output=True,
        )
        os.environ.pop('ZFS_CMD', None)
        os.environ.pop('ZPOOL_CMD', None)


@pytest.fixture(scope='session')
def auto_zfs_pool(tmp_path_factory):
    """Auto-create and destroy a ZFS pool for real_zfs integration tests.

    Without MULTIPASS_VM: creates a local file-backed pool via passwordless sudo.
    With MULTIPASS_VM=<name>: creates the pool inside the named Multipass Linux VM,
    routing all zfs/zpool commands through it for the duration of the session.
    Returns None if setup fails; dependent fixtures call pytest.skip().
    """
    import os
    vm = os.environ.get('MULTIPASS_VM')
    if vm:
        yield from _auto_zfs_pool_multipass(vm, tmp_path_factory)
    else:
        yield from _auto_zfs_pool_local(tmp_path_factory)


# Test markers
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "subprocess: mark test as mocking subprocess"
    )


# Q8 (Checkpoint A): every mocked test belongs to exactly one of the two
# selectable tiers, so `pytest -m unit` + `pytest -m integration` together
# cover the whole mocked suite. Most test modules never carried a marker at
# all (only some classes were decorated), which made `-m unit` silently skip
# the majority of the suite. Anything that declares none of the three tier
# markers is a unit test.
_TIER_MARKERS = ("unit", "integration", "real_zfs")


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """Mark every test lacking a `unit`/`integration`/`real_zfs` marker as
    `unit`.

    `tryfirst` matters: pytest's own `-m` deselection runs in the same hook,
    and a marker added after it would be invisible to `-m unit`.
    """
    skip_real = None if real_tier_enabled() else pytest.mark.skip(reason=REAL_TIER_REASON)
    for item in items:
        if not any(item.get_closest_marker(name) for name in _TIER_MARKERS):
            item.add_marker(pytest.mark.unit)
        if skip_real is not None and item.get_closest_marker("real_zfs"):
            # opt-in tier: never run (and never set up its pool fixture) by accident
            item.add_marker(skip_real)


# ---------------------------------------------------------------------------
# Daemon presence lock (item 15.2(c)): a real second process holding it.
# ---------------------------------------------------------------------------

_LOCK_HOLDER_SCRIPT = r"""
import os, sys, time
from zfsbackup import daemonlock

path, gen, mode = sys.argv[1], sys.argv[2], sys.argv[3]
lock = daemonlock.acquire(path, generation=int(gen) if gen else None)
orphan = 0
if mode == "fork":
    orphan = os.fork()
    if orphan == 0:
        # Inherits the descriptor (and so, under fork, the lock itself).
        # Bounded: never outlives a forgotten teardown by more than a minute.
        time.sleep(60)
        os._exit(0)
print("ready", os.getpid(), orphan, flush=True)
time.sleep(60)
"""


class DaemonLockHolder:
    """A real subprocess that has taken `daemonlock.acquire(path)`."""

    def __init__(self, proc, pid, orphan_pid):
        self.proc = proc
        self.pid = pid
        self.orphan_pid = orphan_pid or None

    def kill9(self):
        """`kill -9` the holder and reap it. An orphan (fork mode) lives on."""
        import signal as _signal

        self.proc.send_signal(_signal.SIGKILL)
        self.proc.wait(timeout=10)

    def kill_orphan(self, timeout=10):
        import signal as _signal
        import time as _time

        if not self.orphan_pid:
            return
        import os as _os

        try:
            _os.kill(self.orphan_pid, _signal.SIGKILL)
        except ProcessLookupError:
            return
        # A killed orphan is reaped by init; a zombie has already closed its
        # descriptors, so the lock is gone whether or not the pid lingers.
        deadline = _time.monotonic() + timeout
        while _time.monotonic() < deadline:
            try:
                _os.kill(self.orphan_pid, 0)
            except ProcessLookupError:
                return
            _time.sleep(0.05)

    def stop(self):
        import signal as _signal

        if self.proc.poll() is None:
            self.proc.send_signal(_signal.SIGKILL)
            try:
                self.proc.wait(timeout=10)
            except Exception:
                pass
        try:
            self.proc.stdout.close()
        except Exception:
            pass
        self.kill_orphan()


@pytest.fixture
def daemon_lock_holder():
    """Factory: `holder = daemon_lock_holder(db_path, generation=None, fork=False)`.

    Starts `python -c <acquire>` against `db_path`, waits (hard 30s timeout)
    for it to report that it holds the lock, and returns a `DaemonLockHolder`.
    Every holder (and, in fork mode, its orphan child) is SIGKILLed at
    teardown whatever the test did.
    """
    import os
    import select
    import shutil
    import subprocess
    import sys
    from pathlib import Path

    repo_root = Path(__file__).resolve().parent.parent
    holders = []

    def start(db_path, generation=None, fork=False):
        env = hermetic_env()
        proc = subprocess.Popen(
            [
                sys.executable, "-c", _LOCK_HOLDER_SCRIPT,
                str(db_path), "" if generation is None else str(generation),
                "fork" if fork else "plain",
            ],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            cwd=str(repo_root), env=env,
        )
        holder = DaemonLockHolder(proc, proc.pid, 0)
        holders.append(holder)
        ready, _, _ = select.select([proc.stdout], [], [], 30)
        if not ready:
            raise RuntimeError("lock holder subprocess never reported ready")
        line = proc.stdout.readline().split()
        if not line or line[0] != "ready":
            raise RuntimeError(f"lock holder failed to start: {line!r}")
        holder.pid = int(line[1])
        holder.orphan_pid = int(line[2]) or None
        return holder

    yield start
    for h in holders:
        h.stop()


@pytest.fixture
def lock_spy(mocker):
    """Wraps `daemonlock.acquire` so tests can inspect the `DaemonLock` that
    `daemon._run` took. A lock `_run` deliberately did NOT release holds its
    raw descriptor until the process exits (that is the point); this closes
    any such descriptor at teardown so one test cannot leak a held lock into
    the next."""
    import os

    from zfsbackup import daemon as daemon_module
    from zfsbackup import daemonlock

    locks = []
    real = daemonlock.acquire

    def spy(*args, **kwargs):
        lock = real(*args, **kwargs)
        locks.append(lock)
        return lock

    mocker.patch.object(daemon_module.daemonlock, 'acquire', side_effect=spy)
    yield locks
    for lock in locks:
        fd, lock._fd = lock._fd, None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass


#: What the contention window is during the suite unless a test opts back in
#: to the real one with `real_contention_window`. `daemonlock.acquire` keeps
#: retrying a contended lock for a whole second before refusing; the dozens
#: of tests that merely need "a second daemon is refused" would each pay it.
_TEST_CONTENTION_WINDOW = 0.05


@pytest.fixture(autouse=True)
def _short_daemonlock_contention_window(monkeypatch):
    """Shrinks `daemonlock._CONTENTION_WINDOW` to 50 ms for every test (it
    only matters to a test that contends for the lock). The tests that pin
    the real window -- the transient-holder success and the persistent-holder
    refusal -- request `real_contention_window` instead, and a separate test
    asserts the real constants are 1 s / 25 ms. Subprocess daemons are not
    affected: they import the module themselves. Yields the production value.
    """
    import sys

    # Only if some test module already imported it: libzfseasy-only runs
    # (and the real-ZFS ones) must not pay for importing zfsbackup here.
    daemonlock = sys.modules.get("zfsbackup.daemonlock")
    if daemonlock is None:
        yield None
        return
    production = daemonlock._CONTENTION_WINDOW
    monkeypatch.setattr(daemonlock, "_CONTENTION_WINDOW", _TEST_CONTENTION_WINDOW)
    yield production


@pytest.fixture
def real_contention_window(monkeypatch, _short_daemonlock_contention_window):
    """Restore the production contention window (1 s) for this test. Depends
    on the autouse shrink, so it runs after it and wins."""
    from zfsbackup import daemonlock

    monkeypatch.setattr(daemonlock, "_CONTENTION_WINDOW", _short_daemonlock_contention_window)
    return _short_daemonlock_contention_window


@pytest.fixture
def forbid_db(mocker):
    """Arms every way the config database could be opened from the process
    under test to raise `DbTouched`, and records each attempt in the returned
    list (which a test asserts is empty, in case something caught the raise).

    Arm it AFTER the test has finished writing its fixture database: it
    patches `sqlite3.connect` itself, so nothing -- not `read_config`, not an
    ORM session, not the importer -- can open any SQLite file. Under `fork`
    a child inherits the patched module, so a worker that opened the database
    would explode there too.
    """
    from _config_handoff import DbTouched

    touched = []

    def boom(name):
        def _raise(*args, **kwargs):
            touched.append(name)
            raise DbTouched(name)
        return _raise

    for target in (
        "zfsbackup.runtime_config.load_runtime_config",
        "zfsbackup.runtime_config.read_config",
        "zfsbackup.config.store.access.read_config",
        "zfsbackup.config.store.access.open_config_session",
        "zfsbackup.config.store.paths.get_engine",
        "zfsbackup.config.store.db.get_engine",
        "sqlite3.connect",
    ):
        mocker.patch(target, side_effect=boom(target))
    return touched

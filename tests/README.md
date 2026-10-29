# Testing Guide for libzfseasy and zfsbackup

This directory contains the test suite for both packages of the `zfsbackup` distribution: the `libzfseasy` bindings
and the `zfsbackup` daemon and CLI.

## Overview

The test suite is organized into three tiers:

- **Unit tests** (`-m unit`) - individual components, with mocked subprocess calls, fake `zfs` scripts or stubs
- **Integration tests** (`-m integration`) - complete workflows: mocked `zfs`, but real daemons, workers, CLIs, TLS
  servers and SQLite databases in real subprocesses
- **Real ZFS tests** (`-m real_zfs`: `test_real_zfs.py`, `test_zfsbackup_real.py`) - actual ZFS commands against a
  throwaway pool; opt-in

Shared fixtures and the suite's safety nets are in `conftest.py` and `_hermetic.py`; the other `_*.py` modules are
shared helpers (see [Helper modules](#helper-modules)). What each `test_*.py` file covers is listed in the "Test
layout" table of [CLAUDE.md](../CLAUDE.md).

## Installation

First, install the development dependencies:

```bash
poetry install --with dev
```

Or if using pip (the dev dependencies are a Poetry group, not an extra):

```bash
pip install -e .
pip install pytest pytest-mock pytest-cov pytest-xdist requests-mock
```

## Running Tests

### Run all tests
```bash
pytest
```

This runs the mocked tiers (unit and integration). `pytest.ini`'s `addopts` deselect `real_zfs`
(`-m "not real_zfs"`; a `-m` on the command line replaces it), and add `-v`, `--strict-markers`, `--tb=short` and
coverage of both packages (`--cov=libzfseasy --cov=zfsbackup`, reports in the terminal, `htmlcov/index.html` and
`coverage.xml`). `--no-cov` turns coverage off.

### Run specific test categories

Run only unit tests:
```bash
pytest -m unit
```

Run only integration tests:
```bash
pytest -m integration
```

Run tests that mock subprocess:
```bash
pytest -m subprocess
```

`./run_tests.sh` is a thin wrapper (`--unit`, `--integration`, `-p`/`--parallel`, `-v`; `--help` lists its flags).

### Real ZFS Integration Tests

**WARNING: These tests run actual ZFS commands and create a pool.**

**The real tier is opt-in: set `ZFSB_REAL_ZFS=1` (`1`, `true`, `yes` or `on`).** Without it, `pytest -m real_zfs`
SKIPS every real test (with that reason) and sets up no pool -- the pool fixture would
otherwise run `sudo -n zpool create` -- and, in every run, an unset `ZFS_CMD` / `ZPOOL_CMD`
is pointed at a stub so that nothing in the mocked tiers (including the real
daemons and CLIs they start in subprocesses) can resolve the host's `zfs`/`zpool`:

```bash
pytest                              # mocked tiers; real_zfs tests are deselected
pytest -m real_zfs                  # all real_zfs tests SKIPPED (no ZFSB_REAL_ZFS)
ZFSB_REAL_ZFS=1 pytest -m real_zfs  # the real tier: nothing is stubbed, real zfs/zpool run
```

Set it per command; never export it in a shared shell profile.

With `ZFSB_REAL_ZFS=1` the real binaries must be resolvable (`ZFS_CMD`/`ZPOOL_CMD` or `PATH`).
The mocked tests in the same session are still guarded: the in-process guard
(`_never_run_a_real_zfs` in `conftest.py`) fails any non-`real_zfs` test that starts a
real `zfs`/`zpool` itself, and test helpers that start subprocesses build their environment
with `tests/_hermetic.py::hermetic_env()` (stub binaries, a private `PATH`).
The guard covers only the pytest process; subprocess safety comes from that environment.

Leaked processes: every session gets a fresh `ZFSB_TEST_SESSION` marker, and
`conftest.pytest_sessionfinish` kills -- and FAILS the run for -- any process of the session
(exec'd children are found by the marker in their environment, children forked without exec by
an at-fork record) that outlives its tests and is no longer a descendant of pytest. Set
`ZFSB_ALLOW_ORPHANS=1` to downgrade that failure to a warning.

#### The test pool

You do not create a pool yourself: the session fixture `auto_zfs_pool` (`conftest.py`) does.

- **Locally** (default): a 512 MiB sparse image in pytest's temp directory and a pool named
  `zfsbackup_pytest_<pid>`, created with `sudo -n zpool create`, so it needs **passwordless `sudo`** for `zpool` and
  `zfs`. It delegates permissions to the current user with `zfs allow`; where delegation is not enough it routes every
  `zfs`/`zpool` call through `sudo` wrapper scripts (`ZFS_CMD`/`ZPOOL_CMD`).
- **In a Multipass VM**: `MULTIPASS_VM=<vm>` creates the pool inside that VM (installing `zfsutils-linux` there if
  needed) and routes every `zfs`/`zpool` call through `multipass exec`.

The pool is destroyed at the end of the session; each test also removes the datasets it creates. If the pool
cannot be created (no ZFS, no passwordless `sudo`), the real tests SKIP rather than fail.
`scenarios/setup_test_pool.sh` makes a separate pool (`testpool`) for manual experiments; the tests do not use it.

### Run specific test files

```bash
pytest tests/test_types.py
pytest tests/test_commands.py
pytest tests/test_integration.py
```

### Run specific test classes or functions

```bash
pytest tests/test_types.py::TestValidate
pytest tests/test_types.py::TestValidate::test_zfsname_valid
```

### Run in parallel (faster)

```bash
pytest -n auto
```

This uses all available CPU cores. The workers of one run share its session marker, so the orphan check still
covers them.

### Run with verbose output

```bash
pytest -vv
```

(`-v` is already in `addopts`.)

### Run and stop at first failure

```bash
pytest -x
```

### In a git worktree

```bash
PYTHONPATH=<worktree> <venv python> -m pytest
```

Without it the editable install imports the main tree's packages, not the worktree's.

## Environment variables

| Variable | Effect |
|---|---|
| `ZFSB_REAL_ZFS` | `1`/`true`/`yes`/`on` opts into the real tier; otherwise `real_zfs` tests are skipped and `ZFS_CMD`/`ZPOOL_CMD` are stubbed |
| `ZFS_CMD`, `ZPOOL_CMD` | the `zfs`/`zpool` binaries `libzfseasy` runs; when unset (and the real tier is off) the suite sets them to a stub that exits 0 with no output. `hermetic_env()` replaces a value that names a real binary |
| `MULTIPASS_VM` | real tier only: create the test pool inside this Multipass VM |
| `ZFSB_ALLOW_ORPHANS` | any non-empty value: leaked processes are still killed and reported, but no longer fail the run |
| `ZFSB_TEST_SESSION`, `ZFSB_TEST_SESSION_OWNER` | set BY the suite (the session marker and the pid that minted it); an exported value is never reused |

## Safety nets

- **Stub policy** (`_hermetic.apply_stub_policy`, at the top of `conftest.py`, before `libzfseasy` is imported, since
  it resolves the binaries at import time): unset `ZFS_CMD`/`ZPOOL_CMD` point at a stub unless the real tier is on.
- **Hermetic subprocesses** (`_hermetic.hermetic_env(**extra)`): the environment for any subprocess that could reach
  `zfs`: stub `ZFS_CMD`/`ZPOOL_CMD` and a private `PATH` holding only `zfs`/`zpool` stubs, `sh`, `true`, `false`,
  `sleep`, `cat`, `echo` and the interpreter's directory. `/usr/bin` is deliberately not on it. `nested_env()` is the
  environment for a pytest session started from a test (no inherited session identity).
- **In-process guard** (`_never_run_a_real_zfs`, autouse): a non-`real_zfs` test that starts a real `zfs`/`zpool`
  (the binary on the original `PATH`, or any compiled file of that name; `#!` fake scripts are fine) fails.
- **Orphan check** (`pytest_sessionfinish`, see above), proven by `test_orphan_safety_net.py`.
- **Patched `libzfseasy`**: a test that leaves a `libzfseasy` attribute or command singleton patched after its
  fixtures have been torn down fails (the patch is undone first, so it cannot cascade).
- **Tier markers**: a test with none of `unit`/`integration`/`real_zfs` is marked `unit` at collection, so `-m unit`
  plus `-m integration` cover the whole mocked suite (`test_marker_tiers.py`).

Other autouse fixtures restore the SIGINT/SIGTERM/SIGHUP dispositions and the logging set up by
`zfsbackup.logsetup` after each test, dispose every cached config-store engine, shrink the daemon lock's contention
window to 50 ms (`real_contention_window` restores the real one), and make the in-process daemon's API bind probe
treat port 8080 as free (`real_api_probe` opts out).

## Test Structure

### test_types.py

Tests for the `types` module including:
- `Validate` - Input validation functions
- `Property` - Property value objects
- `Dataset`, `Filesystem`, `Volume` - Dataset type classes
- `Snapshot`, `SnapshotRange`, `Bookmark` - Snapshot and bookmark classes

### test_commands.py / test_commands_part2.py

Tests for all ZFS command classes (`test_commands_part2.py` takes `AllowCommand` through `MountCommand` further, and
the streaming send/receive handles):
- `ListCommand` - Listing datasets
- `CreateCommand` - Creating filesystems and volumes
- `SnapshotCommand` - Creating snapshots
- `BookmarkCommand` - Creating bookmarks
- `DestroyCommand` - Destroying datasets
- `RenameCommand` - Renaming datasets
- `CloneCommand` - Cloning snapshots
- `GetCommand` - Getting properties
- `SetCommand` - Setting properties
- `InheritCommand` - Inheriting properties
- `SendCommand` - Sending snapshots
- `ReceiveCommand` - Receiving snapshots
- `AllowCommand`, `UnAllowCommand` - Permission delegation
- `LoadKeyCommand`, `UnLoadKeyCommand`, `ChangeKeyCommand` - Encryption keys
- `MountCommand` - Mounting filesystems
- `UnMountCommand` - Unmounting filesystems
- the exec core (`_exec_lines`, `_exec_out`, the streaming variants) with mocks and with real child processes

### test_integration.py

End-to-end workflow tests with mocked subprocess calls:
- Basic create/list/destroy workflows
- Snapshot creation and cloning
- Incremental snapshots
- Bookmark management
- Property management (get/set/inherit)
- Send/receive operations
- Mount/unmount operations
- Complex multi-step workflows
- Error handling

### test_real_zfs.py

**Real ZFS integration tests that run actual ZFS commands:**
- Filesystem creation, listing, and destruction
- Property get/set/inherit operations
- Snapshot creation and management
- Recursive snapshots
- Clone operations
- Bookmark creation and management
- Rename operations
- Volume creation and management
- Send/receive streams, `-H` framing, `get` rows and property sources against real output

`test_zfsbackup_real.py` runs the daemon against the same pool (end-to-end mTLS send/receive, plain and raw chains,
pruning of held and cloned snapshots, ...).

**Requirements:**
- `ZFSB_REAL_ZFS=1`
- ZFS utilities must be installed
- Passwordless `sudo` for `zpool`/`zfs` (or `MULTIPASS_VM`); the pool is created for you
- Tests skip if no pool can be created
- Tests clean up after themselves

### The zfsbackup tests

`test_zfsbackup_*.py` cover the daemon, workers, retention, remote transfer and API, the config store and the CLI;
`test_openzfs_formats.py` replays captured OpenZFS output (below). See the table in [CLAUDE.md](../CLAUDE.md).

### conftest.py

Shared test fixtures and configuration:
- `mock_subprocess` - Mock subprocess calls
- Sample ZFS objects (`sample_pool`, `sample_dataset`, `sample_filesystem`, `sample_volume`, `sample_snapshot`,
  `sample_bookmark`) and properties
- Mock command outputs (`mock_zfs_list_output`, `mock_zfs_get_output`)
- zfsbackup configs: `sample_backup_config`, `config_with_remote`, `config_yaml_path` (importer input),
  `imported_config_db` / `sqlite_config_db` (a schema-current config database), `literal_config_db`,
  `worker_config` (the detached config a worker is handed), `tls_config_db` (plus the server TLS identity)
- `daemon_lock_holder` (a real process holding the daemon lock), `lock_spy`, `forbid_db` (fails any config DB open)
- `auto_zfs_pool` - the real tier's pool
- The safety nets and autouse fixtures above, and the `unit` auto-marker

## Test Markers

Tests are marked with custom markers for categorization (registered in `pytest.ini`; `--strict-markers` refuses
any other):

- `@pytest.mark.unit` - Unit tests that test individual functions/methods (the default for an unmarked test)
- `@pytest.mark.integration` - Integration tests that test complete workflows
- `@pytest.mark.subprocess` - Tests that mock subprocess calls
- `@pytest.mark.slow` - Tests that take longer to run
- `@pytest.mark.real_zfs` - Real ZFS integration tests (opt-in with `ZFSB_REAL_ZFS=1`)

## Helper modules

Importable from any test module (`tests/` is on `sys.path`); none is collected as a test file.

| Module | What it provides |
|---|---|
| `_hermetic.py` | stub policy, `hermetic_env()`, `nested_env()`, the real-`zfs` definition and the orphan hunt |
| `_tls_support.py` | real generated identities, a scripted `StubTrust`, `make_gateway`, `pin_client`/`unpin_client`, `https_get`/`tls_probe` |
| `_api_support.py` | client certificates, `FakeGateway`, `Authed`, `RealTrust`, `/enroll` and `/rotate` bodies |
| `_remote_support.py` | `make_transport`, `fake_connect_pinned`, `client_tls_dir`, `whoami_json` for `RemoteBackupManager` tests |
| `_enroll_support.py` | `RealServer` (real app, gateway and TLS listener) for the `enroll` / `identity rotate` CLI tests |
| `_session_support.py` | `StreamRig`, `ChildWriter` and process rigs for the mid-stream revocation tests |
| `_stream_support.py` | synthetic and captured `DRR_BEGIN` stream headers for the server receive path |
| `_get_support.py` | realistic `zfs get -H`/`zfs list -H` bytes, `ZfsWorld` (a `Popen` router by argv), `GetReader` |
| `_snapshot_support.py` | snapshots and `zfs list` rows with the six listing columns, a `frozen_clock` fixture |
| `_prune_support.py` | `PrunePool`, a stateful fake of list/get/destroy for the pruner, and the `prune_pool` fixture |
| `_raw_support.py` | databases built through the real migrations at a given revision, stamp helpers |
| `_openzfs_fixtures.py` | `load_section` / `load_m1_summary` over `fixtures/openzfs/` |
| `_config_handoff.py` | `config_signature` (literal-preserving config comparison), recording workers, `DbTouched` |
| `_cli_support.py` | fixtures for CLI tests that build a real database through the CLI |
| `_click_compat.py` | `make_runner()` (a `CliRunner` with separate stdout/stderr and EOF-at-a-prompt = abort on click 8.1 and >= 8.2 alike), `interleaved(result)` for ordering across the two streams; every CLI test builds its runner through it (`test_click_compat.py`) |
| `_worker_status_support.py` | worker status files written with the real `health` code |
| `_reload_workers.py`, `_skew_workers.py`, `_status_workers.py` | picklable worker doubles for spawn/forkserver tests |
| `_fake_pids.py` | pids guaranteed not to belong to a real process, for fake workers |
| `_clock_support.py`, `_timer_support.py` | a fake monotonic clock; `timer_stretch()` for real-clock bounds |
| `_fake_zfs.py`, `_fake_zfs_check.py` | fake `zfs` executables (JSON state) for the `rename` and `check` tests |
| `_editor_stub.py`, `_slow_editor.py`, `_pty_driver.py` | scripted `$EDITOR`s and a pty prompt driver for the CLI tests |
| `_leak_probe.py` | leaks a process on purpose, in a nested session, for `test_orphan_safety_net.py` |

## Data fixtures

- `fixtures/openzfs/<2.1.5|2.2.2|2.4.1>/` - real `zfs`/`zpool` output captured on each supported OpenZFS version,
  loaded with `_openzfs_fixtures.py`; see [fixtures/openzfs/README.md](fixtures/openzfs/README.md).
- `fixtures/get_regression_pre_d5.json` - what `zfs.get()` returned (objects, warnings, skipped-row counts, argv)
  before the D5 parser change, the regression baseline of `test_framing.py::TestGetUnchangedByRows`.

## Writing New Tests

### Unit Test Example

```python
import pytest
from libzfseasy.types import Dataset

class TestMyFeature:
    @pytest.mark.unit
    def test_dataset_creation(self, sample_pool):
        """Test creating a dataset."""
        ds = Dataset(f'{sample_pool}/test')
        assert ds.name == f'{sample_pool}/test'
```

### Integration Test Example

```python
import pytest
import libzfseasy as zfs

class TestMyWorkflow:
    @pytest.mark.integration
    @pytest.mark.subprocess
    def test_create_and_destroy(self, mock_subprocess, sample_pool):
        """Test creating and destroying a filesystem."""
        # Setup mock
        mock_subprocess.setup()
        
        # Create filesystem
        fs = zfs.create.filesystem(f'{sample_pool}/test')
        assert fs.name == f'{sample_pool}/test'
        
        # Setup mock for destroy
        mock_subprocess.setup(stdout=['destroy\ttestpool/test\n'])
        
        # Destroy filesystem
        result = zfs.destroy.dataset(fs, destroy=True)
        assert len(result) == 1
```

A test that starts a real subprocess which could reach `zfs` (a daemon, the CLI) must pass `env=hermetic_env(...)`
and kill its whole process group when it is done.

## Mocking Subprocess

The `mock_subprocess` fixture provides an easy way to mock ZFS commands:

```python
def test_with_mock(mock_subprocess):
    # Setup mock to return specific output
    mock_subprocess.setup(
        stdout=['line1\n', 'line2\n'],
        stderr='',
        returncode=0
    )
    
    # Your test code here
    # The mocked subprocess will return the configured output
```

For a sequence of calls, `mock_subprocess.setup_multi((stdout, stderr, returncode), ...)` queues one configuration
per `Popen`.

## Continuous Integration

`.github/workflows/tests.yml` runs the mocked suite on pushes to `master` and on pull requests, and
`latest-deps.yml` runs it weekly against the newest dependencies; both set explicit `ZFS_CMD`/`ZPOOL_CMD` stubs:

- No ZFS utilities, pools or root privileges are needed
- `zfs` is mocked, stubbed or a fake script; daemons, CLIs and TLS servers run for real in subprocesses on `127.0.0.1`

The `debian-floors` job in `tests.yml` runs the whole mocked suite on Python 3.13 with Debian 13's archive versions of
every runtime dependency (click 8.1.8 included; Ubuntu 26.04 ships the same click), no lock, both projects installed
with `--no-deps`. The `.deb` depends on those packages, so this is the supported floor; reproduce it locally with a
venv of the pins listed in the job. `test_ci_workflows.py` keeps the pins inside `pyproject.toml`'s ranges.

`.github/workflows/real-zfs.yml` runs `ZFSB_REAL_ZFS=1 pytest -m real_zfs` nightly (and on manual dispatch) on
`master` only, never for a pull request (`test_ci_workflows.py` pins this).

## Coverage Goals

Target coverage goals:
- Overall: >90%
- Critical paths: 100%
- Error handling: >85%

View current coverage:
```bash
pytest
```

(`addopts` already report the missed lines of both packages.)

## Troubleshooting

### Tests fail with "fixture not found"

Make sure you're running pytest from the project root:
```bash
cd /path/to/libzfs
pytest
```

### Import errors

Ensure the package is installed in development mode:
```bash
poetry install --with dev
```

### Subprocess mocking issues

If tests hang or fail with subprocess errors, ensure you're using the `mock_subprocess` fixture and calling `.setup()` before each command.

### "this test started a real 'zfs'"

The in-process guard caught a code path that bypassed the mocks: use `mock_subprocess` or a fake script.

### LEAKED PROCESSES at the end of the run

A test left a process running. Kill its whole process group (`os.killpg(pid, SIGKILL)` with
`start_new_session=True`), not just the leader.

## Contributing

When adding new features:

1. Write unit tests for new functions/classes
2. Write integration tests for new workflows
3. Ensure all tests pass: `pytest`
4. Check coverage: `pytest --cov`
5. Run with multiple markers to verify categorization
6. Add a new test file to the "Test layout" table in CLAUDE.md

## Additional Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-mock documentation](https://pytest-mock.readthedocs.io/)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [pytest-xdist documentation](https://pytest-xdist.readthedocs.io/)

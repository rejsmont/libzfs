# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Install dependencies:**
```bash
poetry install --with dev
```

**Run tests (mocked, no ZFS required):**
```bash
pytest
```

**Run a single test file or test:**
```bash
pytest tests/test_types.py
pytest tests/test_commands.py::TestListCommand::test_list_basic
```

**Run by marker:**
```bash
pytest -m unit
pytest -m integration
```

**Run real ZFS integration tests (requires a test pool):**
```bash
./scenarios/setup_test_pool.sh        # create file-backed pool
pytest -m real_zfs
./scenarios/cleanup_test_pool.sh      # destroy pool when done
```

**Run the backup daemon:**
```bash
python -m zfsbackup.daemon -c zfsbackup/config.example.yaml --dry-run
python -m zfsbackup.daemon --test-config -c zfsbackup/config.example.yaml
```

## Architecture

This repo contains two packages:

### `libzfseasy` — ZFS Python bindings

Wraps the `zfs` and `zpool` CLI utilities (or optionally TrueNAS `libzfs` bindings) as Python objects.

**[libzfseasy/types.py](libzfseasy/types.py)** defines the type hierarchy:
- `ZFS` (base) → `Dataset` → `Filesystem`, `Volume`
- `ZFS` → `Snapshot`, `Bookmark`
- `SnapshotRange` — represents a `dataset@snap1%snap2` range
- `Property` — wraps a value with `source` and `received` metadata
- `Validate` — static validators for names, types, property names

Properties are stored internally as integer-indexed lists (using `_prop_names` class attrs) for efficiency; user-defined properties (containing `:`) are stored separately in `_user_props`.

**[libzfseasy/zfs.py](libzfseasy/zfs.py)** defines command classes (`ListCommand`, `CreateCommand`, `SnapshotCommand`, etc.), each wrapping a `zfs` subcommand. `Command._exec` / `_exec_out` drive `subprocess.Popen` and stream stdout line-by-line. `SendCommand` and `ReceiveCommand` use binary streaming variants (`_exec_stream` / `_exec_stream_in`) that return `BufferedReader`/`BufferedWriter`.

**[libzfseasy/\_\_init\_\_.py](libzfseasy/__init__.py)** instantiates one singleton per command class and exports them as module-level names (`zfs.list`, `zfs.create`, `zfs.snapshot`, etc.). This is the public API.

`ZFS_CMD` and `ZPOOL_CMD` environment variables override the resolved binary paths.

### `zfsbackup` — Automated snapshot daemon

**[zfsbackup/config.py](zfsbackup/config.py)** — YAML config loader. `BackupConfig.from_file()` parses global settings and a list of `DatasetConfig` objects. Each dataset has a `frequency` (how often to snapshot) and tiered `retention` rules (`RetentionRule(age, keep_for)`). Time durations use a custom format: `m`=minutes, `h`=hours, `d`=days, `w`=weeks, `M`=months (~30d), `y`=years (~365d).

**[zfsbackup/backup_manager.py](zfsbackup/backup_manager.py)** — Core logic:
- `DatasetManager` — owns the list of `DatasetInfo` objects, checks whether a snapshot is due (`needs_snapshot`), creates snapshots via `libzfseasy`, and lists existing ones
- `DatasetInfo` — pairs a `libzfseasy.Dataset` with its `DatasetConfig`; computes the aligned reference time for period boundaries
- `SnapshotInfo` — wraps a `libzfseasy.Snapshot`; parses the `{prefix}_{YYYYMMDDHHMMSS}` name to get a timestamp and calculates age
- Retention/destruction lives on `DatasetManager` too — the tiered slot-based `needs_prunning` (keeps the newest snapshot per time slot, always keeps `snapshots[0]`, skips anchored snapshots) and `prune_snapshots` (destroys in batches, honors `dry_run`). Anchor state is stored in the `org.zfsbackup:anchor.*` user properties.

**[zfsbackup/daemon.py](zfsbackup/daemon.py)** — `BackupDaemon` is a **multiprocessing supervisor**, not a single-process loop: it owns a shared `multiprocessing.Event` stop flag, installs `SIGINT`/`SIGTERM` handlers, spawns the worker processes, then polls and restarts any that die. Entry point: `python -m zfsbackup.daemon` (flags `-c/--config`, `-d/--dry-run`, `-v/--verbose`, `--test-config`).

**[zfsbackup/workers.py](zfsbackup/workers.py)** — the per-dataset work runs here. `BaseWorker.run` builds a `DatasetManager`, iterates its datasets calling an abstract `_process_dataset`, and sleeps via `stop_event.wait(timeout=interval)`. Subclasses: `SnapshotWorker`, `PruningWorker`, `RemoteBackupWorker` (lazy-imports `RemoteBackupManager`), `ApiWorker` (runs the Flask app in a thread).

**[zfsbackup/remote.py](zfsbackup/remote.py)** / **[zfsbackup/api.py](zfsbackup/api.py)** — remote send/receive transfer (`RemoteBackupManager`) and the HTTP control API (`create_app`). Remote backup is **not deployment-ready** (no auth/TLS) — see [docs/production_readiness_report.md](docs/production_readiness_report.md).

### Test layout

| File | Coverage |
|---|---|
| `tests/test_types.py` | `Validate`, `Property`, type classes |
| `tests/test_commands.py` | Each command class, mocked subprocess |
| `tests/test_integration.py` | End-to-end workflows, mocked subprocess |
| `tests/test_real_zfs.py` | Real ZFS commands, marked `real_zfs` |
| `zfsbackup/test_basic.py` | `zfsbackup` package tests |

The `mock_subprocess` fixture in `conftest.py` patches `subprocess.Popen`. Use `mock_subprocess.setup(stdout=[...], stderr='', returncode=0)` for a single call and `mock_subprocess.setup_multi(...)` for sequences.

## Agentic development workflow

This repo ships a project-scoped agent team under [.claude/agents/](.claude/agents/). Non-trivial
changes go through a review → plan → implement → test → review cycle rather than ad-hoc edits.
**[.claude/agents/README.md](.claude/agents/README.md) is the authoritative description of the pipeline,
its stop conditions, and loop behavior** — this section is a summary. The main Claude session
orchestrates the cycle: subagents cannot call each other, so the main loop drives every handoff.

| Agent | Model / effort | Scope |
|---|---|---|
| `zfs-code-reviewer` | opus / high (read-only) | Reviews the current diff for both packages; reports findings |
| `libzfseasy-implementation-planner` | opus / high (read-only) | Turns findings into an ordered plan for `libzfseasy/` |
| `zfsbackup-implementation-planner` | opus / high (read-only) | Turns findings into an ordered plan for `zfsbackup/` |
| `libzfseasy-developer` | sonnet / high | Implements plans in `libzfseasy/types.py`, `zfs.py` |
| `zfsbackup-developer` | sonnet / high | Implements plans in `zfsbackup/` (config, retention, workers, daemon, remote, api) |
| `pytest-test-author` | sonnet / high | Owns **all** pytest tests + `conftest.py` fixtures |
| `real-zfs-scenario-dev` | sonnet / high | Owns the shell scenarios under `scenarios/` |

**One cycle:** `zfs-code-reviewer` finds issues → the matching planner produces an ordered plan (a
proposal) → **user approves** → the matching developer implements → `pytest-test-author` (and
`real-zfs-scenario-dev` for shell coverage) add tests → `zfs-code-reviewer` re-reviews. A clean review
ends the cycle; surviving findings loop back to the planner.

**Loop-style development (`/loop`):** planners tag each plan item `low-risk` or `needs-approval`. In an
interactive run the whole plan waits for approval. Under `/loop`, `low-risk` items proceed
automatically — the review pass and the test suite are the safety net — while `needs-approval` items
(exec/stream-contract or breaking public-API changes in `libzfseasy`; retention, multiprocessing/IPC,
or remote/API/security changes in `zfsbackup`) pause and surface to the user.

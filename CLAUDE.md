# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Install dependencies:**
```bash
poetry install --with dev
```

**Run tests (mocked — no ZFS *pool* required, but see below):**
```bash
pytest
```

`libzfseasy/zfs.py` resolves the `zfs`/`zpool` binaries at **import** time. `tests/conftest.py`
therefore points `ZFS_CMD`/`ZPOOL_CMD` at a harmless stub before importing it whenever they are unset,
so a bare `pytest` never runs a real `zfs` (the developer's Mac has one), and every test subprocess gets
a hermetic environment (`tests/_hermetic.py::hermetic_env()`: a private PATH with stub `zfs`/`zpool`
and no `/usr/bin`). The mocked suite must never touch real ZFS; the `real_zfs` tier is the opt-in
exception (below). `.github/workflows/tests.yml` sets explicit stubs.

The suite also fails loudly if any process it started outlives it (a `ZFSB_TEST_SESSION` marker plus an
orphan check at session end that also tracks fork-without-exec children; `ZFSB_ALLOW_ORPHANS=1`
downgrades that to a warning).

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
ZFSB_REAL_ZFS=1 pytest -m real_zfs
./scenarios/cleanup_test_pool.sh      # destroy pool when done
```

The real tier is **opt-in**: without `ZFSB_REAL_ZFS=1`, `pytest -m real_zfs` skips every real test
(and an unset `ZFS_CMD`/`ZPOOL_CMD` is stubbed for the whole session). Set it per command; never export
it in a shared shell profile.

**Run the backup daemon:**

The daemon reads its config **only** from the SQLite config store; a `.yaml` passed to `-c` is refused.
Import YAML first with the config CLI (a destructive full replace of whatever the DB holds; the
outgoing config is backed up next to the DB unless `--no-backup`). `python -m zfsbackup.cli` works
without an installed entry point.

```bash
zfsbackup-config -c /tmp/zfsbackup/config.db import zfsbackup/config.example.yaml
zfsbackup-config -c /tmp/zfsbackup/config.db export          # DB -> YAML on stdout
zfsbackup-config -c /tmp/zfsbackup/config.db migrate --check # schema up to date?
python -m zfsbackup.daemon --test-config -c /tmp/zfsbackup/config.db
python -m zfsbackup.daemon -c /tmp/zfsbackup/config.db --dry-run
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

**[zfsbackup/config/model.py](zfsbackup/config/model.py)** — YAML config loader. `BackupConfig.from_file()` parses global settings and a list of `DatasetConfig` objects. Each dataset has a `frequency` (how often to snapshot) and tiered `retention` rules (`RetentionRule(age, keep_for)`). Time durations use a custom format: `m`=minutes, `h`=hours, `d`=days, `w`=weeks, `M`=months (~30d), `y`=years (~365d).

**[zfsbackup/backup_manager.py](zfsbackup/backup_manager.py)** — Core logic:
- `DatasetManager` — owns the list of `DatasetInfo` objects, checks whether a snapshot is due (`needs_snapshot`), creates snapshots via `libzfseasy`, and lists existing ones
- `DatasetInfo` — pairs a `libzfseasy.Dataset` with its `DatasetConfig`; computes the aligned reference time for period boundaries
- `SnapshotInfo` — wraps a `libzfseasy.Snapshot`; parses the `{prefix}_{YYYYMMDDHHMMSS}` name to get a timestamp and calculates age
- Retention/destruction lives on `DatasetManager` too — the tiered slot-based `needs_prunning` (keeps the newest snapshot per time slot, always keeps `snapshots[0]`, skips anchored snapshots) and `prune_snapshots` (destroys in batches, honors `dry_run`). Anchor state is stored in the `org.zfsbackup:anchor.*` user properties.

**[zfsbackup/daemon.py](zfsbackup/daemon.py)** — `BackupDaemon` is a **multiprocessing supervisor**, not a single-process loop: it owns a shared **`zfsbackup.stopflag.StopFlag`** (a lock-free shared byte — **not** a `multiprocessing.Event`; see below), installs `SIGINT`/`SIGTERM` handlers, spawns the worker processes, then polls and restarts any that die. Entry point: `python -m zfsbackup.daemon` (flags `-c/--config`, `-d/--dry-run`, `-v/--verbose`, `--test-config`).

**[zfsbackup/workers.py](zfsbackup/workers.py)** — the per-dataset work runs here. `BaseWorker.run` builds a `DatasetManager`, iterates its datasets calling an abstract `_process_dataset`, and sleeps in short slices (`_wait`), checking at every safe point (loop top, between datasets, each slice) whether to stop or whether its supervisor has died (`_SupervisorWatch` — an orphaned worker exits on its own, never mid-transfer). Workers receive the supervisor's config object and never open the config DB. Subclasses: `SnapshotWorker`, `PruningWorker`, `RemoteBackupWorker` (lazy-imports `RemoteBackupManager`), `ApiWorker` (binds the HTTP server in its main thread and exits 1 if it cannot, so the supervisor's backoff applies).

**[zfsbackup/remote.py](zfsbackup/remote.py)** / **[zfsbackup/api.py](zfsbackup/api.py)** — remote send/receive transfer (`RemoteBackupManager`) and the HTTP control API (`create_app`). Remote backup is **not deployment-ready** (no auth/TLS) — see [docs/production_readiness_report.md](docs/production_readiness_report.md).

**[zfsbackup/config/](zfsbackup/config/)** — the configuration package. `model.py` holds the runtime
value types (`BackupConfig`, `DatasetConfig`, `Duration`, …) and `__init__.py` re-exports them, so
`from zfsbackup.config import BackupConfig` is the supported spelling and importing from
`zfsbackup.config.model` directly is unnecessary.

**[zfsbackup/config/store/](zfsbackup/config/store/)** — the SQLAlchemy config store, and the daemon's
**only** config source (plan item 8; see [docs/config_db_cli_plan.md](docs/config_db_cli_plan.md)).
`models.py` holds the ORM schema, `mapper.py` converts ORM rows ⇄ the `config/model.py` dataclasses,
`db.py` owns engine/session management (fork-safe engine cache, WAL pragmas, read-only sessions),
`paths.py` resolves and preflights the DB path (`-c` > `$ZFSBACKUP_CONFIG` >
`/var/lib/zfsbackup/config.db`; the daemon never creates it) and owns the CLI writer lock,
`access.py` is the read/write API (`read_config` — one snapshot incl. the schema check — and
`write_config` — writer lock, generation compare-and-swap), `messages.py` the shared error wording,
`errors.py` `ConfigConflict`/`ConfigBusy`, `importer.py` the YAML → DB full replace, and `migrate.py`
plus `migrations/` the Alembic baseline (pinned head; `upgrade_schema`). It is a **separate layer** from
the dataclasses, not a replacement — `DatasetConfig` is also a remote wire format with no DB behind it,
and config objects outlive any session. YAML survives only as an import format and (from Phase 2) the
`$EDITOR` buffer; `BackupConfig.from_file` remains the single YAML parser.

**[zfsbackup/runtime_config.py](zfsbackup/runtime_config.py)** — a thin daemon wrapper over the
store's `read_config` that adds the daemon's own error wording. **Only the supervisor reads the config
DB, once, at startup**; it then disposes every engine and hands its detached `BackupConfig` to every
worker, including respawns (decision Q4) — workers never open the DB, so all workers always run the
same generation. A running daemon does not pick up config edits until it is restarted (live reload is
Phase 3).

**[zfsbackup/daemonlock.py](zfsbackup/daemonlock.py)** — the daemon presence lock: `flock` on
`<db>.daemon.pid` (`pid` and config generation inside). A second daemon on the same DB refuses to
start; the CLI `probe()`s it to report whether a running daemon is on an older generation. "Running"
means the lock is held, never that a pid exists, so a file left after a crash is harmless.

Files next to the config DB: `config.db` (+ `-wal`/`-shm` while open), `config.db.writer.lock` (CLI
writer lock — empty, never deleted), `config.db.daemon.pid` (daemon presence lock), and
`config.db.pre-import-<UTC>.yaml` backups written by `import`.

**[zfsbackup/cli/](zfsbackup/cli/)** — the `zfsbackup-config` Click application (Phase 2):
`import`, `export`, `migrate`, `list`, `get`/`set`/`unset` (dot paths, `dotpath.py`), `edit`
(whole config or one dataset/destination in `$EDITOR`, `editor.py`) and `rename dataset|destination`
(`rename.py`, which also handles the ZFS anchor/property side effects); `render.py` turns a `BackupConfig` back into YAML that
`from_file` reproduces exactly, including duration literals; `write.py` is the shared read → edit →
`write_config(expected_generation)` helper for the editing commands, with the post-write daemon notice.
It is the only writer of the config DB. Exit codes: 0 ok, 1 invalid/refused, 2 usage, 75 retry
(conflict/busy), 78 config DB unusable, 130/143 interrupted/terminated.

> **`zfsbackup/store/` is reserved and deliberately empty.** It is for *backup data* destinations —
> object storage, cloud targets and the like. Configuration persistence lives under
> `zfsbackup/config/store/`. Do not put config code in `zfsbackup/store/`, and do not move the config
> store back out of `zfsbackup/config/`.

### Test layout

| File | Coverage |
|---|---|
| `tests/test_types.py` | `Validate`, `Property`, type classes |
| `tests/test_commands.py` | Each command class, mocked subprocess |
| `tests/test_integration.py` | End-to-end workflows, mocked subprocess |
| `tests/test_real_zfs.py` | Real ZFS commands, marked `real_zfs` |
| `tests/test_commands_part2.py` | Command classes `AllowCommand` through `MountCommand` |
| `tests/test_zfsbackup_basic.py` | Package smoke tests |
| `tests/test_zfsbackup_manager.py` | `DatasetManager`, retention/pruning |
| `tests/test_zfsbackup_snapshot_info.py` | `SnapshotInfo` / `DatasetInfo` time computation |
| `tests/test_zfsbackup_daemon.py` | `BackupDaemon` supervisor: single config read, handoff to workers, presence lock |
| `tests/test_zfsbackup_workers.py` | Worker loop and interval behaviour; config pickling across start methods |
| `tests/test_zfsbackup_daemonlock.py` | Daemon presence lock: held/stale/contention, permissions, fork vs spawn |
| `tests/test_zfsbackup_runtime_config.py` | The daemon wrapper over `read_config`; torn-read protection |
| `tests/test_zfsbackup_store_access.py` | `read_config` snapshot, `write_config` compare-and-swap, `ConfigBusy`, the writer lock |
| `tests/test_zfsbackup_signals.py` | Fork-pinned SIGINT/SIGTERM reset in worker children |
| `tests/test_zfsbackup_stopflag.py` | `StopFlag` semantics |
| `tests/test_zfsbackup_stop_signals.py` | Real daemon: kill a worker, then signal the supervisor — must exit, not wedge |
| `tests/test_zfsbackup_api.py` | HTTP control API |
| `tests/test_zfsbackup_remote.py` | `RemoteBackupManager`, `ClientIdentity` |
| `tests/test_zfsbackup_config.py` | Config loading, retention collapse, per-destination rules |
| `tests/test_zfsbackup_duration.py` | The `Duration` type and its literal preservation |
| `tests/test_zfsbackup_store.py` | SQLAlchemy schema, constraints, mapper, YAML import, baseline revision |
| `tests/test_zfsbackup_config_audit.py` | Empty-value / type audit through `from_file` and import, `validate_config` |
| `tests/test_zfsbackup_importer_cleanup.py` | Failed-first-import cleanup never deletes a DB another process uses; orphaned-sidecar refusal |
| `tests/test_zfsbackup_store_permissions.py` | Group-operator permission model (mocked Linux ownership) |
| `tests/test_zfsbackup_daemon_resilience.py` | Restart backoff, shutdown `finally`, abandoned workers, SIGHUP stopgap |
| `tests/test_zfsbackup_fork_shm.py` | Fork child must not let the parent delete `-wal`/`-shm` (runs in a timed subprocess) |
| `tests/test_zfsbackup_store_upgrade.py` | Pinned schema head, `upgrade_schema` (transactional, FK check, concurrency), import backup hook |
| `tests/test_zfsbackup_cli.py` | `zfsbackup-config` commands, path resolution, exit codes, import backups, conflict/busy, daemon notice |
| `tests/test_zfsbackup_cli_render.py` | YAML renderer round trip (values and duration literals) |
| `tests/test_zfsbackup_cli_list.py` | `list datasets|destinations`, text and `--json` |
| `tests/test_zfsbackup_cli_dotpath.py` | `get`/`set`/`unset` dot paths, coercion, warnings |
| `tests/test_zfsbackup_cli_edit.py` | `edit` in `$EDITOR`: buffers, retry/abort, rename detection |
| `tests/test_zfsbackup_cli_edit_signals.py` | Signal handling around the `$EDITOR` child (subprocess tests) |
| `tests/test_zfsbackup_cli_rename.py` | `rename dataset|destination` and their ZFS anchor/property effects (fake `zfs`) |
| `tests/test_zfsbackup_cli_closeout.py` | `-c`/`-v` on `list`/`rename` groups, `where_flag`, console-script signal disposition, whole-config prefix-change warning |
| `tests/test_zfsbackup_daemon_closeout.py` | Startup SIGHUP handler, supervisor-death watch and sliced waits, ApiWorker failures, real `kill -9` of the supervisor under fork/spawn/forkserver |
| `tests/test_zfsbackup_store_heldlock.py` | `read_generation`, `write_config(lock=held)`/`require_writer_lock_held`, destination-name and never-prune warnings |
| `tests/test_orphan_safety_net.py` | The orphan net and test hygiene: fork-without-exec, subreapers, fresh session marker, real-tier opt-in skip, hermetic env, guard PATH |
| `tests/test_no_real_zfs_guard.py` | The conftest guard that fails any test reaching a real `zfs`/`zpool` |
| `tests/test_marker_tiers.py` | `unit` and `integration` partition the mocked suite |
| `tests/test_zfsbackup_real.py` | Daemon against a real pool, marked `real_zfs` |

Tests without a `unit`/`integration`/`real_zfs` marker are marked `unit` by a hook in `conftest.py`.

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
| `concurrency-reviewer` | opus / high (read-only) | Depth review of fork-safety, SQLite/WAL, IPC and signal hazards only |
| `libzfseasy-implementation-planner` | opus / high (read-only) | Turns findings into an ordered plan for `libzfseasy/` |
| `zfsbackup-implementation-planner` | opus / high (read-only) | Turns findings into an ordered plan for `zfsbackup/` |
| `libzfseasy-developer` | sonnet / high | Implements plans in `libzfseasy/types.py`, `zfs.py` |
| `zfsbackup-developer` | sonnet / high | Daemon core: `config/model.py`, `backup_manager.py`, `daemon.py`, `workers.py`, `remote.py`, `api.py` |
| `zfsbackup-store-developer` | sonnet / high | `zfsbackup/config/store/**` (incl. `store/migrations/`), `alembic.ini` — ORM models, mapper, engine/session, migrations |
| `zfsbackup-cli-developer` | sonnet / high | `zfsbackup/cli/**` — the `zfsbackup-config` Click application |
| `pytest-test-author` | sonnet / high | Owns **all** pytest tests + `conftest.py` fixtures |
| `real-zfs-scenario-dev` | sonnet / high | Owns the shell scenarios under `scenarios/` |

**File ownership inside `zfsbackup/` is exclusive** — no file is co-edited. A plan item spanning two
owners is split by the planner into sequenced sub-items. Daemon and CLI reach the config store only
through the public API of `zfsbackup.config.store` (its `__all__`), never through ORM models; the CLI is
the only writer.

**One cycle:** `zfs-code-reviewer` finds issues → the matching planner produces an ordered plan (a
proposal) → **user approves** → the owning developer implements → `pytest-test-author` (and
`real-zfs-scenario-dev` for shell coverage) add tests → `zfs-code-reviewer` re-reviews, with
`concurrency-reviewer` in parallel on fork/WAL/IPC/signal items. A clean review ends the cycle;
surviving findings loop back to the planner.

**Loop-style development (`/loop`):** planners tag each plan item `low-risk` or `needs-approval`. In an
interactive run the whole plan waits for approval. Under `/loop`, `low-risk` items proceed
automatically — the review pass and the test suite are the safety net — while `needs-approval` items
(exec/stream-contract or breaking public-API changes in `libzfseasy`; retention, multiprocessing/IPC,
remote/API/security, schema-migration/config-contract, or config-write changes in `zfsbackup`) pause
and surface to the user.

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

**[zfsbackup/config/model.py](zfsbackup/config/model.py)** — YAML config loader. `BackupConfig.from_file()` parses global settings and a list of `DatasetConfig` objects. Each dataset has a `frequency` (how often to snapshot) and tiered `retention` rules (`RetentionRule(age, keep_for)`). Time durations use a custom format: `m`=minutes, `h`=hours, `d`=days, `w`=weeks, `M`=months (~30d), `y`=years (~365d). Destination URLs must be `https://` with a non-zero port (`Destination.server_fingerprint` is the pinned server certificate, set by `enroll`); `client_cert_file`/`client_key_file` are optional absolute overrides (both or neither). The client id is **not** configuration: its grammar (`[A-Za-z0-9][A-Za-z0-9._-]{0,63}`, `client_id_problem`) and generator live here, but the value lives in the `host_identity` table, and `from_file` refuses the old file-based setting with a pointer to `identity set-client-id`.

**[zfsbackup/backup_manager.py](zfsbackup/backup_manager.py)** — Core logic:
- `DatasetManager` — owns the list of `DatasetInfo` objects, checks whether a snapshot is due (`needs_snapshot`), creates snapshots via `libzfseasy`, and lists existing ones
- `DatasetInfo` — pairs a `libzfseasy.Dataset` with its `DatasetConfig`; computes the aligned reference time for period boundaries
- `SnapshotInfo` — wraps a `libzfseasy.Snapshot`; parses the `{prefix}_{YYYYMMDDHHMMSS}` name to get a timestamp and calculates age
- Retention/destruction lives on `DatasetManager` too — the tiered slot-based `needs_prunning` (keeps the newest snapshot per time slot, always keeps `snapshots[0]`, skips anchored snapshots) and `prune_snapshots` (destroys in batches, honors `dry_run`). Anchor state is stored in the `org.zfsbackup:anchor.*` user properties. A recursive prune (`zfs destroy -r`) never destroys a name a descendant protects — its local/received anchors (one `zfs get -r`) or an enabled configured descendant's own retention — and skips the dataset if those cannot be read. Received datasets (server side) are always pruned non-recursively: the client never sends `-R`, so each received child is its own client dataset with its own retention.

**[zfsbackup/daemon.py](zfsbackup/daemon.py)** — `BackupDaemon` is a **multiprocessing supervisor**, not a single-process loop: it owns a shared **`zfsbackup.stopflag.StopFlag`** (a lock-free shared byte — **not** a `multiprocessing.Event`; see below), installs `SIGINT`/`SIGTERM`/`SIGHUP` handlers, spawns the worker processes, then polls and restarts any that die. It also runs the live-reload state machine: a SIGHUP or the 10 s poll leads to read, validate, build and the pre-start checks; then the `Handoff` swap and a `ReloadEpoch` bump; then a staggered turnover (see Live reload below). The supervisor itself makes **no `zfs` call** (a hung pool must not stall signals or restarts). Under the `forkserver` start method (the Linux default from Python 3.14) a dead forkserver makes `poll()` report every worker as exit 255 while they live: such workers are classified *lost* by process identity (start token), counted toward no restart limit, and replaced only once the orphan has exited. Entry point: `python -m zfsbackup.daemon` (flags `-c/--config`, `-d/--dry-run`, `-v/--verbose`, `--test-config`).

**[zfsbackup/workers.py](zfsbackup/workers.py)** — the per-dataset work runs here. `BaseWorker.run` builds a `DatasetManager`, iterates its datasets calling an abstract `_process_dataset`, and sleeps in short slices (`_wait`), checking at every safe point (loop top, between datasets and remote destinations, each slice) for an `ExitReason`: STOP, then SUPERVISOR_GONE, then RELOAD. RELOAD means the shared `ReloadEpoch` moved past the worker's `born_epoch`, and the worker exits with code 3 (`WORKER_EXIT_RELOAD`) to be respawned on the new config (`_SupervisorWatch` — an orphaned worker exits on its own, never mid-transfer). Workers receive the supervisor's config object (and the client id) and never open the config DB, with one exception: the API worker, which reaches the trust tables only through `zfsbackup.config.store.trust` (see "Who reads and writes what"). Subclasses: `SnapshotWorker` (first, once per incarnation, checks the datasets exist and syncs their `org.zfsbackup:config` property — work the supervisor used to do), `PruningWorker`, `RemoteBackupWorker` (lazy-imports `RemoteBackupManager`; gets the `client_id` and the `<db>.tls/` path), `ApiWorker` (HTTPS only; binds the server in its main thread and exits 1 if it cannot, so the supervisor's backoff applies; on reload it drains in-flight receives through `api.StreamGate`, answering 503 + retry meanwhile, then calls `server.shutdown()`; see the TLS paragraph below). Every worker holds `<db>.workers.lock` `LOCK_SH` for life.

**[zfsbackup/remote.py](zfsbackup/remote.py)** / **[zfsbackup/api.py](zfsbackup/api.py)** — remote send/receive transfer (`RemoteBackupManager`) and the HTTP control API (`create_app`). Transport is TLS 1.3 with mutual authentication and fingerprint pinning (B1 is resolved); the residual caveats are in [docs/production_readiness_report.md](docs/production_readiness_report.md) (werkzeug's development server, no mid-stream revocation, no rate limiting on `/enroll`).
Resume: the client fetches the server's `receive_resume_token`, decodes it with `zfs.send.resume_info`
(`zfs send -nv -t`) and resumes only a managed local snapshot of the same dataset matched **by guid**
with no send flags; definitive staleness → a fresh send, anything uncertain → skip the cycle. `/stream`
takes `?mode=fresh|resume`; on `fresh` the server aborts stale partial state (`zfs receive -A`). The anchor
records the snapshot actually sent (never backwards) and a resumed older snapshot is followed by a send to
`latest` in the same cycle. `org.zfsbackup:inflight.<dest>` = `<from>,<to>` pins snapshots while they are
sent (the pruner protects them like anchors; `rename` copies/clears them like anchors). Two client
datasets mapping to one server dataset are refused (409); a concurrent stream for the same server dataset
gets a retryable `busy`. Client and server must be upgraded together (an old server's `/resume_token` 500 makes a
new client skip; an old client's `/stream` without `mode` is refused).

**TLS, authentication and enrollment** (plan: [docs/tls_mtls_plan.md](docs/tls_mtls_plan.md); wire contract at its end).
- **[zfsbackup/identity.py](zfsbackup/identity.py)** is the one implementation of the crypto plumbing shared by the
  daemon and the CLI: generated identities (ECDSA P-256, 10 years), SHA-256 fingerprints, `server_context` /
  `client_context`, `connect_pinned` / `pinned_session`, enrollment tokens (`zfsb1.<server fp>.<secret>`),
  proof-of-possession payloads and `check_client_cert`. It imports only the stdlib, `cryptography` and
  `zfsbackup.config`; the CLI imports it, so it must never import the daemon.
- **`<db>.tls/`** (mode 0750, the DB file's group) holds `server.crt`/`server.key`, `client.crt`/`client.key`,
  the pending successor `client.next.crt`/`client.next.key` (rotation, `enroll --new-key`) and `identity.lock`
  (the flock that guards creating or moving files). Keys are 0600; certificates and the lock file 0640. Keys are
  never stored in the DB. The supervisor creates the server identity at startup (or `trust token create` does
  earlier); `enroll`/`identity rotate` create the client one. `--dry-run` creates no TLS files: an API worker
  without a server identity serves with a throwaway one.
- **Server (`workers.py` `_api_server_class`, `api.py`).** HTTPS only, no plaintext mode, TLS 1.3 minimum. The
  handshake runs on each connection's own thread (30 s timeout) with the context the `TrustGateway` holds at that
  moment, so one silent peer cannot stall the accept loop or `shutdown()`. Caps: 64 connections in all, 16 of
  them without a pinned client certificate; the request line and headers have an absolute 30 s deadline. A
  dedicated thread polls the trust generation every 2 s, so a revocation or an enrollment applies without a
  restart; every `store.trust` call in the API worker is serialised in-process. The API worker drains and
  exits 1 (so the supervisor restarts it) only for a confirmed on-disk server-identity change (`IdentityChanged`).
- **Authentication.** Every endpoint except `/health` and `/enroll` needs a client certificate pinned in the
  trust tables (401 without one, 403 if unknown or expired). The client id a request acts as is the one the
  certificate is **bound** to; the `<client_id>` in `/backup/<client_id>/…` is compared with it (403 on a
  mismatch) and never used. `PROP_CLIENT_ID` is written from the binding. `GET /whoami` returns the bound id.
  `POST /enroll` (no client certificate; token + proof of possession of the key; a read-only token pre-check runs
  before any `zfs` call) and `POST /rotate` (authenticated with the current certificate) are the only trust
  writes over HTTP. `/backup/register` no longer exists.
- **Client (`remote.py`).** One pinned `requests` session per destination and a pinned websocket: the server
  certificate fingerprint is checked right after the handshake, before any HTTP byte. `/whoami` runs once per
  cycle per destination, and a destination that is not enrolled, whose server does not trust this host, or whose
  binding differs from the local client id is skipped before any `zfs send`. The client identity is reloaded when
  its files change. There is no `http://`/`ws://` path.

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
`access.py` is the read/write API (`read_config` — one snapshot incl. the schema check —,
`write_config` — writer lock, `(store_id, generation)` compare-and-swap — and `read_store_state`, the
daemon's cheap poll read), `messages.py` the shared error wording, `errors.py` `ConfigConflict`/`ConfigBusy`
(`StoreIdConflict`, "the database was replaced", lives in `mapper.py`; the store id is `PRAGMA
user_version`, stamped on first save), `importer.py` the YAML → DB full replace, and `migrate.py`
plus `migrations/` the Alembic baseline (pinned head; `upgrade_schema`). It is a **separate layer** from
the dataclasses, not a replacement — `DatasetConfig` is also a remote wire format with no DB behind it,
and config objects outlive any session. YAML survives only as an import format and (from Phase 2) the
`$EDITOR` buffer; `BackupConfig.from_file` remains the single YAML parser.

**[zfsbackup/runtime_config.py](zfsbackup/runtime_config.py)** — a thin daemon wrapper over the
store's `read_config` that adds the daemon's own error wording. **Only the supervisor reads the config
and the client id** — at startup and on every reload, in one snapshot — and disposes every engine after each
read. Each worker receives its config, its epoch and the client id together from one `Handoff`; no worker
opens the DB for configuration.

**Who reads and writes what.** Three groups of tables, three rules:
- **Configuration** (the six config tables) **and the host identity** (`host_identity.client_id`): the CLI is
  the only writer. `host_identity` is outside the six config tables, so `import`/`export` never touch it (an
  `import` keeps the id), and a change to it bumps the config generation so a running daemon reloads and hands
  the new id to the remote worker. `client_id` is a stable host id that the server binds to verified
  certificate fingerprints and never takes from the request.
- **Trust** (`clients`, `trusted_clients`, `revoked_fingerprints`, `enrollment_token`, `trust_state`): two
  writers, both through `zfsbackup.config.store.trust` — the CLI (`trust …`) and the API worker (`/enroll`,
  `/rotate`, retiring superseded pins). Trust writes never bump the config generation and never cause a
  reload; they bump `trust_state.generation`, which the API worker polls. Revocation is permanent: every
  unpinned fingerprint is recorded in `revoked_fingerprints` (a `retired` row keeps `superseded_by`) and
  refused for ever on enroll and rotate. `revoke_pin` revokes every live descendant, through pending
  rotation links and `superseded_by`, so a thief's used successor goes with the stolen key; `/rotate`
  retires the authenticating pin's still-pinned predecessor in the same transaction.
- **Reads:** only the supervisor reads configuration and the client id. The API worker opens the DB only
  through `store.trust`, on a per-call engine (never cached, disposed on exit, so the supervisor's
  `dispose_all()` neither sees nor affects it). No other worker opens it.

**Live reload (Phase 3).** The supervisor reloads on SIGHUP and on a 10 s poll of `read_store_state()`
(`(store_id, generation)`, the store id being `PRAGMA user_version`). Neither trigger kills work:
- **Checks before a candidate is accepted:** it is read, run through `validate_config` and its dataset list
  is built. A candidate that fails any of these is rejected (final); `rejected=` is recorded and the old
  workers keep running. A changed API address (probed unless a live API worker holds it) that is *in use*
  (`EADDRINUSE`) is **deferred**, not rejected: `deferred=` is recorded and the poll re-checks it every
  10 s until it passes; any other bind failure (unresolvable host, `EADDRNOTAVAIL`, `EACCES`) is a final
  rejection. Client TLS problems only warn (`_client_tls_problem`: unreadable, mismatched, expired or
  wrong-EKU client certificate, expiry within 30 days, destinations not enrolled, a remote worker needed but no
  client id).
- **Identical candidate:** a new key whose config is `same_as` the running one (duration literals count) **and
  whose client id is unchanged** is adopted without an epoch bump — no worker restarts. A changed client id is a
  real switch.
- **Switching an accepted candidate:** the supervisor bumps a shared `ReloadEpoch`. Each worker exits with
  code 3 at its next safe point (between datasets or destinations, never mid-transfer) and is respawned on
  the new config. The API worker first drains in-flight receives.
- **Reporting:** progress is published in the pid file as `applied=`, `target=`, `rejected=` and
  `deferred=` (+ `deferred_reason=`); `applied=` means every worker has turned over.
`zfsbackup-config` sends the SIGHUP itself after every committed write, but only to a verified supervisor
(see daemonlock). Otherwise the poll applies the change.

**[zfsbackup/daemonlock.py](zfsbackup/daemonlock.py)** — the daemon presence lock: `flock` on
`<db>.daemon.pid`. Pid file v2: lines 1–2 are pid and generation, then `start=`, `applied=`, `target=`,
`rejected=`, `deferred=` and `deferred_reason=` lines (readers ignore unknown keys), written padded so a reader never sees a torn tail. A second daemon on the same DB refuses
to start, and waits instead if the holder is a verified-dead supervisor's orphans. "Running" means the lock
is held, never that a pid exists. `request_reload()` is the only SIGHUP sender. It signals only after
checking:
- the pid is in `/proc/locks` as the FLOCK WRITE holder of that exact device+inode (Linux only);
- the pid file is owned by root, by the CLI's own euid, or by the target's uid;
- the pid + start-time identity matches, and the process is not a zombie.
Anything it cannot verify is reported (`reason_code`) and left to the poll; macOS therefore never
signals. [zfsbackup/procident.py](zfsbackup/procident.py) provides the start-time, zombie and lock-holder
primitives (stdlib only).

**[zfsbackup/sdnotify.py](zfsbackup/sdnotify.py)** — systemd `sd_notify` (stdlib only). `_run` pops
`NOTIFY_SOCKET` before any fork or `zfs` exec, so only the supervisor notifies (`NotifyAccess=main`):
`READY=1` after the workers start, `RELOADING=1` … `READY=1` around every SIGHUP reload (READY means the
reload was *decided*, accepted or rejected, not that the turnover finished), `STATUS=generation N`,
`EXTEND_TIMEOUT_USEC` during startup waits, `STOPPING=1` on exit. The README unit is `Type=notify-reload`.

Files next to the config DB: `config.db` (+ `-wal`/`-shm` while open), `config.db.writer.lock` (CLI
writer lock — empty, never deleted), `config.db.daemon.pid` (daemon presence lock), `config.db.workers.lock`
(held `LOCK_SH` by every worker, so a new daemon waits for a previous daemon's surviving workers),
`config.db.pre-import-<UTC>.yaml` backups written by `import`, and the `config.db.tls/` directory (see above).

**[zfsbackup/cli/](zfsbackup/cli/)** — the `zfsbackup-config` Click application (Phases 2–3):
`import`, `export`, `migrate`, `list`, `get`/`set`/`unset` (dot paths, `dotpath.py`), `edit`
(whole config or one dataset/destination in `$EDITOR`, `editor.py`) and `rename dataset|destination`
(`rename.py`, which also handles the ZFS anchor/property side effects); the TLS commands (`trust.py`,
`enroll.py`, `tlsfiles.py`):
- server: `trust token create [--ttl 1h] [--client-id ID [--replace]] [--json]`, `trust token show|revoke`,
  `trust list [--json]`, `trust revoke FINGERPRINT` or `trust revoke --client ID` (never signals the daemon: the
  API picks trust changes up within ~2 s);
- client: `enroll DESTINATION [--url URL] [--label L] [--token-file PATH | --token -] [--new-key [--cert --key]]`,
  `identity show [--json]`, `identity set-client-id ID [--force]`, `identity rotate [--cert --key]`;

`render.py` turns a `BackupConfig` back into YAML that
`from_file` reproduces exactly, including duration literals; `write.py` is the shared read → edit →
`write_config(expected_generation)` helper for the editing commands. After every committed write it calls
`begin_reload`, which is the one `request_reload` call, before any output; it then waits up to
`--reload-wait` (a global option, default 3 s) for the daemon to answer and prints the outcome.
`status [--wait] [--json]` (`status.py`) reports the stored key vs the daemon's applied, target,
rejected and deferred keys, and never signals (`--wait` on a still-deferred key exits 75, never 1).
`rename dataset --clear-stale-props` clears the old name's properties only with no daemon and no surviving
workers, or once a *signalled* (verified, so never on macOS) daemon has applied the rename. The CLI is the only writer of the configuration and of the host identity (`host_identity.client_id`); the trust tables have a second writer, the API worker (see "Who reads and writes what"). Exit codes: 0 ok, 1 invalid/refused, 2 usage, 75 retry
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
| `tests/test_zfsbackup_prune_descendants.py` | Recursive prune honours descendants' anchors and configured retention; received datasets pruned non-recursively |
| `tests/test_zfsbackup_snapshot_info.py` | `SnapshotInfo` / `DatasetInfo` time computation |
| `tests/test_zfsbackup_daemon.py` | `BackupDaemon` supervisor: single config read, handoff to workers, presence lock |
| `tests/test_zfsbackup_workers.py` | Worker loop and interval behaviour; config pickling across start methods |
| `tests/test_zfsbackup_daemonlock.py` | Daemon presence lock: held/stale/contention, permissions, fork vs spawn |
| `tests/test_zfsbackup_runtime_config.py` | The daemon wrapper over `read_config`; torn-read protection |
| `tests/test_zfsbackup_store_access.py` | `read_config` snapshot, `write_config` compare-and-swap, `ConfigBusy`, the writer lock |
| `tests/test_zfsbackup_signals.py` | Fork-pinned SIGINT/SIGTERM reset in worker children |
| `tests/test_zfsbackup_stopflag.py` | `StopFlag` semantics |
| `tests/test_zfsbackup_stop_signals.py` | Real daemon: kill a worker, then signal the supervisor — must exit, not wedge |
| `tests/test_zfsbackup_api.py` | HTTP control API behind a stubbed trust gateway (stream modes, abort, 409 collision, busy lock, receive error reporting) |
| `tests/test_zfsbackup_api_auth.py` | Authentication and authorization: 401/403, the client-id binding (cert B cannot touch A's data), `/whoami`, the `/enroll` and `/rotate` matrices against a real trust store, predecessor retirement, `TrustGateway` |
| `tests/test_zfsbackup_api_tls.py` | API worker TLS: per-connection handshake, TLS 1.3 only, connection caps and deadlines, revocation/addition with no restart, real `ApiWorker` under fork/spawn/forkserver, dry-run throwaway identity |
| `tests/test_zfsbackup_api_bind.py` | API server binds without a reverse DNS lookup |
| `tests/test_zfsbackup_remote.py` | `RemoteBackupManager`: negotiation, resume protocol, transfer (the pinned transport is faked) |
| `tests/test_zfsbackup_config.py` | Config loading, retention collapse, per-destination rules |
| `tests/test_zfsbackup_duration.py` | The `Duration` type and its literal preservation |
| `tests/test_zfsbackup_store_tls_schema.py` | Trust/identity schema and migration `7c3e9d51b2a8`, new `validate_config` rules, `ConfigSnapshot.client_id`, `import` keeps the id and the trust tables |
| `tests/test_zfsbackup_store_trust.py` | `store.trust`: host identity, tokens, `enroll_client` outcomes, revocation (permanent), rotation, trust generation, ephemeral engines |
| `tests/test_zfsbackup_identity.py` | `zfsbackup.identity`: fingerprints, tokens, proof of possession, `check_client_cert`, `ensure_identity` races and modes, pinned connections |
| `tests/test_zfsbackup_tls_platform.py` | Characterization spikes S0–S10 (ssl, werkzeug, flask-sock, websocket-client, requests) the TLS design rests on |
| `tests/test_zfsbackup_store.py` | SQLAlchemy schema, constraints, mapper, YAML import, baseline revision |
| `tests/test_zfsbackup_config_audit.py` | Empty-value / type audit through `from_file` and import, `validate_config` |
| `tests/test_zfsbackup_importer_cleanup.py` | Failed-first-import cleanup never deletes a DB another process uses; orphaned-sidecar refusal |
| `tests/test_zfsbackup_store_permissions.py` | Group-operator permission model (mocked Linux ownership) |
| `tests/test_zfsbackup_daemon_resilience.py` | Restart backoff, shutdown `finally`, abandoned workers |
| `tests/test_zfsbackup_reload.py` | Live reload state machine: triggers, rejection, turnover, deferral, real-daemon reloads |
| `tests/test_zfsbackup_procident.py` | Process identity (start time, zombies) and `/proc/locks` lock-holder parsing |
| `tests/test_zfsbackup_reload_workers.py` | Worker exit protocol (STOP > SUPERVISOR_GONE > RELOAD), safe points, workers lock — real workers under fork/spawn/forkserver |
| `tests/test_zfsbackup_stream_gate.py` | API worker drain: `StreamGate`, 503 while draining, server receive timeout |
| `tests/test_zfsbackup_remote_tls.py` | Pinned-mTLS client: every check runs before any send (no id, not enrolled, not trusted, id mismatch), `/whoami` heuristics, identity reload, real end-to-end TLS |
| `tests/test_zfsbackup_remote_timeouts.py` | Client websocket timeouts; a failed connect never leaves `zfs send` running; 503 skips the cycle |
| `tests/test_zfsbackup_startup_waits.py` | Startup waits for a previous daemon's survivors / dead supervisor |
| `tests/test_zfsbackup_cli_reload.py` | CLI signals after every committed write: once-only matrix, ack loop, notice wording |
| `tests/test_zfsbackup_cli_status.py` | `zfsbackup-config status`: verdicts, JSON, `--wait` exit codes |
| `tests/test_zfsbackup_fork_shm.py` | Fork child must not let the parent delete `-wal`/`-shm` (runs in a timed subprocess) |
| `tests/test_zfsbackup_store_upgrade.py` | Pinned schema head, `upgrade_schema` (transactional, FK check, concurrency), import backup hook |
| `tests/test_zfsbackup_cli_trust.py` | `trust token create/show/revoke`, `trust list`, `trust revoke` (fingerprint or `--client`); no daemon signal |
| `tests/test_zfsbackup_cli_identity.py` | `identity show` and `identity set-client-id` (one SIGHUP, refused while enrolled, `import` keeps the id) |
| `tests/test_zfsbackup_cli_enroll.py` | `enroll` against a real in-process TLS server: idempotency, pin mismatch, `--new-key`, revoked certificates, SIGHUP-once |
| `tests/test_zfsbackup_cli_rotate.py` | `identity rotate` against real API servers: all-or-nothing swap, partial failure, configured paths |
| `tests/test_zfsbackup_cli.py` | `zfsbackup-config` commands, path resolution, exit codes, import backups, conflict/busy, daemon notice |
| `tests/test_zfsbackup_cli_render.py` | YAML renderer round trip (values and duration literals) |
| `tests/test_zfsbackup_cli_list.py` | `list datasets|destinations`, text and `--json` |
| `tests/test_zfsbackup_cli_dotpath.py` | `get`/`set`/`unset` dot paths, coercion, warnings |
| `tests/test_zfsbackup_cli_edit.py` | `edit` in `$EDITOR`: buffers, retry/abort, rename detection |
| `tests/test_zfsbackup_cli_edit_signals.py` | Signal handling around the `$EDITOR` child (subprocess tests) |
| `tests/test_zfsbackup_cli_rename.py` | `rename dataset|destination` and their ZFS anchor/property effects (fake `zfs`) |
| `tests/test_zfsbackup_cli_closeout.py` | `-c`/`-v` on `list`/`rename` groups, `where_flag`, console-script signal disposition, whole-config prefix-change warning |
| `tests/test_zfsbackup_daemon_closeout.py` | Startup SIGHUP handler, supervisor-death watch and sliced waits, ApiWorker failures, real `kill -9` of the supervisor under fork/spawn/forkserver |
| `tests/test_zfsbackup_forkserver_death.py` | Forkserver death: lost/orphaned workers, no restart-limit count, no overlap, identity-checked (pidfd) shutdown signals — real forkserver processes |
| `tests/test_zfsbackup_sdnotify.py` | systemd `sd_notify`: `NOTIFY_SOCKET` handling, READY/RELOADING/STOPPING/STATUS/EXTEND_TIMEOUT sequence, real daemon |
| `tests/test_zfsbackup_store_sqla_compat.py` | SQLAlchemy 2.0/2.1 differences: failed-connect close probe, `%` in DB paths (CI also runs a 2.1 job) |
| `tests/test_zfsbackup_store_wal_switch.py` | WAL-switch busy retry: wall-clock budget, per-attempt busy_timeout, real lock holders |
| `tests/test_zfsbackup_store_heldlock.py` | `read_generation`, `write_config(lock=held)`/`require_writer_lock_held`, destination-name and never-prune warnings |
| `tests/test_orphan_safety_net.py` | The orphan net and test hygiene: fork-without-exec, subreapers, fresh session marker, real-tier opt-in skip, hermetic env, guard PATH |
| `tests/test_no_real_zfs_guard.py` | The conftest guard that fails any test reaching a real `zfs`/`zpool` |
| `tests/test_marker_tiers.py` | `unit` and `integration` partition the mocked suite |
| `tests/test_zfsbackup_real.py` | Daemon against a real pool, marked `real_zfs` |

Tests without a `unit`/`integration`/`real_zfs` marker are marked `unit` by a hook in `conftest.py`.

Shared TLS test helpers (importable from any test module; `tests/` is on `sys.path`; none reaches a real `zfs`, every socket is `127.0.0.1`):
- `tests/_tls_support.py` — real generated identities, a scripted `StubTrust`, `make_gateway`, `pin_client`/`unpin_client` through the real trust writers, `https_get`/`tls_probe`.
- `tests/_api_support.py` — client certificates, `FakeGateway`, `Authed` (a Flask app presenting `SSL_CLIENT_CERT`), `RealTrust`, the `/enroll` and `/rotate` request bodies.
- `tests/_remote_support.py` — `make_transport`, `fake_connect_pinned`, `client_tls_dir`, `whoami_json` for `RemoteBackupManager` tests.
- `tests/_enroll_support.py` — `RealServer` (real app + real gateway + real TLS listener) for the `enroll`/`identity rotate` CLI tests; `tests/_pty_driver.py` answers a prompt on a real pty.

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
| `zfsbackup-developer` | sonnet / high | Daemon core: `config/model.py`, `backup_manager.py`, `daemon.py`, `workers.py`, `remote.py`, `api.py`, `identity.py` |
| `zfsbackup-store-developer` | sonnet / high | `zfsbackup/config/store/**` (incl. `store/migrations/`), `alembic.ini` — ORM models, mapper, engine/session, migrations |
| `zfsbackup-cli-developer` | sonnet / high | `zfsbackup/cli/**` — the `zfsbackup-config` Click application |
| `pytest-test-author` | sonnet / high | Owns **all** pytest tests + `conftest.py` fixtures |
| `real-zfs-scenario-dev` | sonnet / high | Owns the shell scenarios under `scenarios/` |

**File ownership inside `zfsbackup/` is exclusive** — no file is co-edited. A plan item spanning two
owners is split by the planner into sequenced sub-items. Daemon and CLI reach the config store only
through the public API of `zfsbackup.config.store` (its `__all__`), never through ORM models; the CLI is
the only writer of configuration and of the host identity (the API worker also writes the trust tables, through
`zfsbackup.config.store.trust`).

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

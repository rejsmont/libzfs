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
python -m zfsbackup.daemon --test-config -c /tmp/zfsbackup/config.db   # or: zfsbackupd --test-config ...
python -m zfsbackup.daemon -c /tmp/zfsbackup/config.db --dry-run
zfsbackup-config -c /tmp/zfsbackup/config.db check                     # health: 0 ok, 1 warn/crit
zfsbackup-config -c /tmp/zfsbackup/config.db backup /tmp/bk           # DB + <db>.tls + MANIFEST
```

The distribution is named `zfsbackup` (it ships both packages); `zfsbackup/version.py` is the single version
source (`installed_version()`, uncached), and `zfsbackupd --version` / `zfsbackup-config --version` print it with
the wire protocol and schema head. systemd units, sysusers and tmpfiles live in `packaging/` (sdist only, never
the wheel; never put a `.py` file there). The runbook (install, upgrade, restore, monitoring) is
[zfsbackup/README.md](zfsbackup/README.md).

## Architecture

This repo contains two packages:

### `libzfseasy` — ZFS Python bindings

Wraps the `zfs` and `zpool` CLI utilities as Python objects.

**[libzfseasy/types.py](libzfseasy/types.py)** defines the type hierarchy:
- `ZFS` (base) → `Dataset` → `Filesystem`, `Volume`
- `ZFS` → `Snapshot`, `Bookmark`
- `SnapshotRange` — represents a `dataset@snap1%snap2` range
- `Property` — wraps a value with `source` and `received` metadata
- `Validate` — static validators for names, types, property names

Properties are stored internally as integer-indexed lists (using `_prop_names` class attrs) for efficiency; user-defined properties (containing `:`) are stored separately in `_user_props`.

**[libzfseasy/zfs.py](libzfseasy/zfs.py)** defines command classes (`ListCommand`, `CreateCommand`, `SnapshotCommand`, etc.), each wrapping a `zfs` subcommand. `Command._exec` / `_exec_out` drive `subprocess.Popen` and stream stdout line-by-line. `SendCommand` and `ReceiveCommand` use binary streaming variants (`_exec_stream` / `_exec_stream_in`) that return `BufferedReader`/`BufferedWriter`.
`-H` output framing is a trust boundary: every write refuses values containing `\n`, `\r`, `\t` or `\0`
(`Validate.value`, `InvalidValueError`) before running; `list`/`get` read bytes (`_exec_lines`, surrogateescape,
split on `\n` only) and fail closed per row and per object (wrong column count, out-of-scope or duplicate names,
non-contiguous groups, and the "lead rule": an object's `get` rows must start with `type` for `all`, or
`name`,`type` for an explicit list, so `GetCommand` must never pass `-s`). `zfs.get.value(ds, prop, sources=…)`
reads one value exactly.

**[libzfseasy/\_\_init\_\_.py](libzfseasy/__init__.py)** instantiates one singleton per command class and exports them as module-level names (`zfs.list`, `zfs.create`, `zfs.snapshot`, etc.). This is the public API.

`ZFS_CMD` and `ZPOOL_CMD` environment variables override the resolved binary paths.

### `zfsbackup` — Automated snapshot daemon

**[zfsbackup/config/model.py](zfsbackup/config/model.py)** — YAML config loader. `BackupConfig.from_file()` parses global settings and a list of `DatasetConfig` objects. Each dataset has a `frequency` (how often to snapshot) and tiered `retention` rules (`RetentionRule(age, keep_for)`). Time durations use a custom format: `m`=minutes, `h`=hours, `d`=days, `w`=weeks, `M`=months (~30d), `y`=years (~365d). Destination URLs must be `https://` with a non-zero port (`Destination.server_fingerprint` is the pinned server certificate, set by `enroll`); `client_cert_file`/`client_key_file` are optional absolute overrides (both or neither). The client id is **not** configuration: its grammar (`[A-Za-z0-9][A-Za-z0-9._-]{0,63}`, `client_id_problem`) and generator live here, but the value lives in the `host_identity` table, and `from_file` refuses the old file-based setting with a pointer to `identity set-client-id`. `RemoteDatasetConfig.raw` (`Optional[bool]`: `auto`/omitted = None = raw iff the dataset is encrypted, `true`, `false`) is per dataset × destination, client-side only (`to_property` never carries it). `duration_ok`/`wire_config_problem` (positive durations, config shape) are shared by the server's negotiate and pruner.

**[zfsbackup/backup_manager.py](zfsbackup/backup_manager.py)** — Core logic:
- `DatasetManager` — owns the list of `DatasetInfo` objects, checks whether a snapshot is due (`needs_snapshot`), creates snapshots via `libzfseasy`, and lists existing ones
- `DatasetInfo` — pairs a `libzfseasy.Dataset` with its `DatasetConfig`; computes the aligned reference time for period boundaries
- `SnapshotInfo` — wraps a `libzfseasy.Snapshot`; a snapshot is managed only if its name is exactly `{prefix}_{14 digits}` (a suffix makes it unmanaged); carries `createtxg`, `guid`, `userrefs`, `clones`, `encrypted`, `key_loaded` from the one listing (`SNAPSHOT_LIST_PROPERTIES`), sorted newest first by `createtxg` (`snapshot_order_key`; `snapshot_newer` is False when either txg is unknown); a snapshot without `createtxg` is dropped with one WARNING. Names give slot and age only; every newest/older comparison uses `createtxg`. A newest name dated beyond `max(5 min, check_interval)` in the future → ERROR once ("check NTP") and the dataset is due; a generated name that already exists → WARNING once, not due (backwards clock).
- Retention/destruction lives on `DatasetManager` too — the tiered slot-based `needs_prunning` (keeps the oldest snapshot per time slot, always keeps the newest by `createtxg`, skips anchored snapshots) and `prune_snapshots` (batches of 20, honors `dry_run`). Held (`userrefs > 0`) or cloned candidates are skipped up front; a failed batch is retried one snapshot at a time; a name that still won't go is remembered in `_prune_stuck` (WARNING once, tried alone each cycle, INFO when it finally goes). Anchors, inflight pins and descendants' protection are re-read right before every destroy call (`_still_prunable`, the dataset's own properties read last; a failed re-read destroys nothing), because a chain step may become anchored during the pass. Anchor state is stored in the `org.zfsbackup:anchor.*` user properties. A recursive prune (`zfs destroy -r`) never destroys a name a descendant protects — its local/received anchors (one `zfs get -r`) or an enabled configured descendant's own retention — and skips the dataset if those cannot be read. Received datasets (server side) are always pruned non-recursively: the client never sends `-R`, so each received child is its own client dataset with its own retention.

**[zfsbackup/daemon.py](zfsbackup/daemon.py)** — `BackupDaemon` is a **multiprocessing supervisor**, not a single-process loop: it owns a shared **`zfsbackup.stopflag.StopFlag`** (a lock-free shared byte — **not** a `multiprocessing.Event`; see below), installs `SIGINT`/`SIGTERM`/`SIGHUP` handlers, spawns the worker processes, then polls and restarts any that die. It also runs the live-reload state machine: a SIGHUP or the 10 s poll leads to read, validate, build and the pre-start checks; then the `Handoff` swap and a `ReloadEpoch` bump; then a staggered turnover (see Live reload below). The supervisor itself makes **no `zfs` call** (a hung pool must not stall signals or restarts). Under the `forkserver` start method (the Linux default from Python 3.14) a dead forkserver makes `poll()` report every worker as exit 255 while they live: such workers are classified *lost* by process identity (start token), counted toward no restart limit, and replaced only once the orphan has exited. Entry points: the `zfsbackupd` console script (`zfsbackup/daemon_entry.py`: ignores SIGHUP before importing the daemon, so a reload signal right after exec cannot kill it) and `python -m zfsbackup.daemon` (flags `-c/--config`, `-d/--dry-run`, `-v/--verbose`, `--log-level`, `--test-config`, `--version`). Logging is set up by `zfsbackup/logsetup.py` in `main()` and in every worker child (never at import): under systemd, when `$JOURNAL_STREAM` matches stdout's dev:ino, every physical line carries a `<N>` journald priority (NUL escaped, lines wrapped below journald's LineMax); werkzeug's access lines are DEBUG for 1xx–3xx, INFO 4xx, WARNING 5xx, with the path clipped. Forking a worker while other threads exist logs an ERROR.

**[zfsbackup/workers.py](zfsbackup/workers.py)** — the per-dataset work runs here. `BaseWorker.run` builds a `DatasetManager`, iterates its datasets calling an abstract `_process_dataset`, and sleeps in short slices (`_wait`), checking at every safe point (loop top, between datasets and remote destinations, each slice) for an `ExitReason`: STOP, then SUPERVISOR_GONE, then RELOAD. RELOAD means the shared `ReloadEpoch` moved past the worker's `born_epoch`, and the worker exits with code 3 (`WORKER_EXIT_RELOAD`) to be respawned on the new config (`_SupervisorWatch` — an orphaned worker exits on its own, never mid-transfer). Workers receive the supervisor's config object (and the client id) and never open the config DB, with one exception: the API worker, which reaches the trust tables only through `zfsbackup.config.store.trust` (see "Who reads and writes what"). Subclasses: `SnapshotWorker` (first, once per incarnation, checks the datasets exist and syncs their `org.zfsbackup:config` property — work the supervisor used to do), `PruningWorker`, `RemoteBackupWorker` (lazy-imports `RemoteBackupManager`; gets the `client_id` and the `<db>.tls/` path; an anchor dated beyond the clock tolerance makes the destination due, ERROR once), `ApiWorker` (HTTPS only; binds the server in its main thread and exits 1 if it cannot, so the supervisor's backoff applies; on reload it drains in-flight receives through `api.StreamGate`, answering 503 + retry meanwhile, then calls `server.shutdown()`; see the TLS paragraph below). Every worker holds `<db>.workers.lock` `LOCK_SH` for life.

**[zfsbackup/remote.py](zfsbackup/remote.py)** / **[zfsbackup/api.py](zfsbackup/api.py)** — remote send/receive transfer (`RemoteBackupManager`) and the HTTP control API (`create_app`). Transport is TLS 1.3 with mutual authentication and fingerprint pinning (B1 is resolved); the residual caveats are in [docs/production_readiness_report.md](docs/production_readiness_report.md) (werkzeug's development server, no mid-stream revocation, no rate limiting on `/enroll`).
Send flags are frozen (OpenZFS 2.1/2.2/2.4 captures in `tests/fixtures/openzfs/`): plain `-L -c`, raw `-w`, a
resume is `zfs send -t` with no flags; `-L` is permanent once received. The mode is decided once per dataset ×
destination from the newest snapshot's encryption/keystatus and `raw` (`raw: true` on an unencrypted dataset, or
plain with the key unloaded → one ERROR, nothing sent); negotiate reports `server_copy: none|plain|raw` and a
mismatch is refused with one ERROR naming the `zfs rename` fix (the client never touches server data).
Intermediates go as a chain of `-i` steps: every managed snapshot newer by `createtxg` than the common one,
oldest first; per step pin (`org.zfsbackup:inflight.<dest>` = `<from>,<to>`), send, ack, anchor, then check the
sent snapshot still exists by guid (gone → anchor back to the base, pin kept, chain stops). The pin stays until
the next step overwrites it and is cleared after the last step. A first full sends the newest only; a stop
between steps pauses the chain; a vanished member is rebuilt around; a named common snapshot that is not managed
skips the cycle; the anchor never moves backwards. Resume: the client fetches the server's
`receive_resume_token`, decodes it with `zfs.send.resume_info` (`zfs send -nv -t`), and accepts it only for a
managed local snapshot matched **by guid** whose flags satisfy `required <= flags <= allowed` for the mode (plain:
∅ ⊆ flags ⊆ {compressok, largeblockok}; raw: {rawok} ⊆ flags ⊆ {rawok, compressok, largeblockok}; never
embedok/savedok); definitive staleness → a fresh send, anything uncertain → skip the cycle. A resume runs as
step 0, then one re-negotiate and the rest of the chain. `/stream` takes `?mode=fresh|resume`; on `fresh` the
server aborts stale partial state (`zfs receive -A`). The pruner protects pinned snapshots like anchors; `rename`
copies/clears them like anchors. The remote worker sends each client's datasets parents first.
Negotiate holds the server dataset's claim (a concurrent negotiate or stream gets the retryable `busy` 503) and,
for a missing dataset, creates only its parent chain and keeps the canonical config in an in-memory pending map
(TTL 600 s, refreshed by each negotiate, capped at 4096, lost on restart); the receive itself creates the leaf
(raw streams and encrypted targets need that), so `-F` is passed only when the dataset exists. Two client
datasets mapping to one server dataset are refused (409), including against a live pending entry or an unowned
placeholder; a child whose ancestor is claimed, pending, or a new partial receive gets 503 retry; configs over
8 KiB are refused (400). The server releases a stream's claim before any reply, so chain steps run back to back. A connection failure
before any stream byte on a step after a successful one in the same cycle (typically the server's API worker
exiting for a reload) is retried for the connect only (`CONNECT_RETRY_DELAYS`, ~7 s, stop-aware), then stops the
chain quietly (INFO, `_STOP`); on the first stream of a cycle it is an ERROR as before.
Wire protocol (`zfsbackup/protocol.py`): every request carries `X-Zfsbackup-Protocol: 1`; anything else gets
426 (after authentication, so an untrusted caller still sees 401/403 first) on every endpoint except `/health`, and `/whoami`, `/enroll` and `/rotate` report `protocol`. Client and
server must be upgraded together.
Trust boundary on the server (one client's values must never reach another client's data): negotiate stores
only a canonical re-encoding of the client's config; every receive passes `client_id` (from the certificate
binding), `source_dataset` and that config (the dataset's local value, else this client dataset's live pending
entry) as `-o` (`SERVER_OWNED_PROPERTIES`; `-x` does not protect local values on a full `-F` receive of a `-p`
stream), and a stream with neither is refused ("negotiate again", retryable). `received_datasets()` lists
names only, then reads each dataset's `client_id`/`config` with `zfs.get.value(sources='local')` and skips (one
WARNING) anything outside `<target>/<client_id>/`, with an invalid id, or with a config that is missing,
undecodable or only received. The client's sent values survive as hidden `received` values under the server's
locals, so never `zfs inherit -S` them. `/config`, `/datasets` and `/datasets/<n>/snapshots` no longer exist.

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

**[zfsbackup/health.py](zfsbackup/health.py)** (stdlib only) / **[zfsbackup/health_collect.py](zfsbackup/health_collect.py)** — the health check behind `zfsbackup-config check`: the collector reads ZFS only (newest managed snapshot by `createtxg`, anchors per destination, received datasets on a server) and `evaluate` grades lag at 2× (WARN) and 4× (CRIT) of the expected interval; JSON (`"schema": 1`, additive), Prometheus exposition and an atomic file writer. `check` adds daemon-presence, applied-config and client-certificate entries and supports `--nagios` (0/1/2, 3 = UNKNOWN).

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
user_version`, stamped on first save), `importer.py` the YAML → DB full replace, `backup.py`
`backup_database` (a one-snapshot copy staged in memory and published with a no-clobber link — SQLite never opens
a file at the destination; writer-or-root; `BackupBusy`/`BackupSourceError`/`BackupPublishError`), and `migrate.py`
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
`validate_config` also refuses a configured dataset at, below, or recursively above `remote_backup.target_dataset`
(the target must be dedicated). Schema checks are column-aware (`_COLUMNS_ADDED_AT_REVISION`): a head-stamped DB
missing a column fails with `SchemaColumnsMissing` (78). Ownership advice names the `zfsbackup` group only.

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
- operations: `backup DIR [--no-keys] [--json]` (`backup.py`: `DIR/zfsbackup-<host>-<UTC>/` with `config.db`, `tls/`
  via `identity.copy_tls_dir` under a shared `identity.lock`, and a `MANIFEST` written last; never the writer lock,
  never a signal), `check [--json|--prometheus|--nagios] [--output FILE]` (`check.py`), `migrate --backup-dir DIR`
  (backs up only when the schema is behind, inside the writer lock; when the schema is behind, `migrate` refuses
  with 75 while a daemon or any worker is running, so stop the daemon first), `--version`;

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
| `tests/test_framing.py` | `-H` output framing: unframeable values refused on write, fail-closed `list`/`get` row parsing (forged/duplicate/out-of-scope rows), `get.value`, `_exec_lines` |
| `tests/test_libzfseasy_argv_hygiene.py` | Property names starting with `-` or over 255 characters refused, `receive` name validation, `_redact` clipping |
| `tests/test_integration.py` | End-to-end workflows, mocked subprocess |
| `tests/test_real_zfs.py` | Real ZFS commands, marked `real_zfs` |
| `tests/test_commands_part2.py` | Command classes `AllowCommand` through `MountCommand` |
| `tests/test_zfsbackup_basic.py` | Package smoke tests |
| `tests/test_zfsbackup_manager.py` | `DatasetManager`, retention/pruning; the one six-column listing and txg order; B-4 prune fallback, held/cloned pre-skip, stuck names, protection re-read before every destroy |
| `tests/test_zfsbackup_prune_descendants.py` | Recursive prune honours descendants' anchors and configured retention; received datasets pruned non-recursively |
| `tests/test_openzfs_formats.py` | Captured OpenZFS 2.1.5/2.2.2/2.4.1 output (`tests/fixtures/openzfs/`, loader `tests/_openzfs_fixtures.py`) replayed through `resume_info`, destroy, list/get and zfsbackup's stale-token and `receive -A` classifiers |
| `tests/test_zfsbackup_store_raw.py` | `dataset_remotes.raw` migration up/down, mapper round trip, column-aware stamp advice and `SchemaColumnsMissing`, the column-registry guard |
| `tests/test_zfsbackup_cli_raw.py` | `raw` through render/export/import, get/set/unset, list, edit, and the warning on a change |
| `tests/test_zfsbackup_prune_forged_bytes.py` | B-1 end to end: forged `zfs -H` bytes through the real libzfseasy parsers into `received_datasets()`/`prune_snapshots()`; another client's snapshots are never destroyed (`bk/A` vs `bk/Ax`) |
| `tests/test_zfsbackup_snapshot_info.py` | `SnapshotInfo` / `DatasetInfo` time computation; strict name parse, `snapshot_order_key`/`snapshot_newer`, clock rules |
| `tests/test_zfsbackup_daemon.py` | `BackupDaemon` supervisor: single config read, handoff to workers, presence lock |
| `tests/test_zfsbackup_workers.py` | Worker loop and interval behaviour; config pickling across start methods |
| `tests/test_zfsbackup_daemonlock.py` | Daemon presence lock: held/stale/contention, permissions, fork vs spawn |
| `tests/test_zfsbackup_runtime_config.py` | The daemon wrapper over `read_config`; torn-read protection |
| `tests/test_zfsbackup_store_access.py` | `read_config` snapshot, `write_config` compare-and-swap, `ConfigBusy`, the writer lock |
| `tests/test_zfsbackup_signals.py` | Fork-pinned SIGINT/SIGTERM reset in worker children |
| `tests/test_zfsbackup_stopflag.py` | `StopFlag` semantics |
| `tests/test_zfsbackup_stop_signals.py` | Real daemon: kill a worker, then signal the supervisor — must exit, not wedge |
| `tests/test_zfsbackup_api.py` | HTTP control API behind a stubbed trust gateway (stream modes, abort, 409 collision, busy lock, receive error reporting, the claim released before every reply, the receive creating the leaf) |
| `tests/test_zfsbackup_api_negotiate.py` | `/negotiate`: body shape, canonical config (positive durations, 8 KiB cap), client dataset names, 400 before any write; parent-only creation, pending map (TTL, bounds, refresh), `server_copy`, placeholder ownership (D7), the negotiate claim, unsettled ancestors |
| `tests/test_zfsbackup_api_protocol.py` | Wire protocol header: 426 matrix (`/health` exempt, `/enroll` first), `protocol` in `/whoami`/`/enroll`/`/rotate`, per-peer refusal log cap |
| `tests/test_zfsbackup_api_receive.py` | Receive ownership: `-o` of `config`/`client_id`/`source_dataset` on fresh and resume, config from the pending entry, refusal without one, `-F` only when the dataset exists, the placeholder-overwrite message |
| `tests/test_zfsbackup_api_auth.py` | Authentication and authorization: 401/403, the client-id binding (cert B cannot touch A's data), `/whoami`, the `/enroll` and `/rotate` matrices against a real trust store, predecessor retirement, `TrustGateway` |
| `tests/test_zfsbackup_api_tls.py` | API worker TLS: per-connection handshake, TLS 1.3 only, connection caps and deadlines, revocation/addition with no restart, real `ApiWorker` under fork/spawn/forkserver, dry-run throwaway identity |
| `tests/test_zfsbackup_api_wssession.py` | Websocket `_SessionSocket`: fd never closed while a reader is inside it, serialised writes, `end()`/hand-over close, no response after the upgrade, `ws_end_timeout` validation |
| `tests/test_zfsbackup_api_drain.py` | Post-response drain: client teardown after a response or websocket session is quiet (DEBUG, no traceback), bounded drain deadline, post-websocket 200 never written, websocket reader joined before `/stream` returns |
| `tests/test_zfsbackup_api_bind.py` | API server binds without a reverse DNS lookup |
| `tests/test_zfsbackup_remote.py` | `RemoteBackupManager`: negotiation, the `-i` chain (order, pins kept between steps, anchors, stop, rebuild, backstop), resume as step 0, per-mode token matrix, send modes and `server_copy` mismatch (the pinned transport is faked) |
| `tests/test_zfsbackup_config.py` | Config loading, retention collapse, per-destination rules |
| `tests/test_zfsbackup_duration.py` | The `Duration` type and its literal preservation |
| `tests/test_zfsbackup_store_tls_schema.py` | Trust/identity schema and migration `7c3e9d51b2a8`, new `validate_config` rules, `ConfigSnapshot.client_id`, `import` keeps the id and the trust tables |
| `tests/test_zfsbackup_store_liveconn.py` | Raw DB fds never drop a live connection's POSIX locks (`_liveconn` fd-table guard): lock survives preflight, zombie connections, fail-closed listings, fork, tracking coverage |
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
| `tests/test_zfsbackup_real.py` | Daemon against a real pool, marked `real_zfs`: end-to-end mTLS send/receive, plain and raw chains, R0 layouts (raw/plain under encrypted and unencrypted targets), held/cloned prune, txg order, the user-property size limit |
| `tests/test_zfsbackup_version.py` | Single version source (uncached), lazy `__version__`, import weight, `daemon_entry`, pyproject pins and the built wheel/sdist contents |
| `tests/test_zfsbackup_daemon_entry.py` | `zfsbackupd --version`, SIGHUP ignored before the daemon import, the generated console-script wrapper under fork/spawn/forkserver (READY, reload, SIGHUP storm), fork-with-threads ERROR |
| `tests/test_zfsbackup_logsetup.py` | journald detection by dev:ino, `<N>` on every line, NUL escaping and wrapping, `--log-level`, worker children, `log_request` levels and clipping |
| `tests/test_zfsbackup_identity_copy.py` | `identity.copy_tls_dir`: modes, no overwrite, `KeyUnreadable`, shared lock, lock appearing mid-read, consistency against a real rotation |
| `tests/test_zfsbackup_health.py` | `health.evaluate` boundaries, JSON and Prometheus serialisers (a strict exposition parser), `write_text_atomic` |
| `tests/test_zfsbackup_health_collect.py` | The read-only collector over a fake `zfs`: observations, `createtxg` order, errors, received datasets |
| `tests/test_zfsbackup_store_backup.py` | `backup_database`: one snapshot under concurrent writers, planted-sidecar attack, refusals, no clobber, durable publish |
| `tests/test_zfsbackup_store_target_overlap.py` | C0: datasets at, below or recursively above the backup target refused (validate, import, reload, startup) |
| `tests/test_zfsbackup_store_config_group.py` | Ownership advice names the `zfsbackup` group only |
| `tests/test_zfsbackup_cli_version.py` | `zfsbackup-config --version` (eager, no DB, no daemon import) |
| `tests/test_zfsbackup_cli_backup.py` | `zfsbackup-config backup`: layout, modes, MANIFEST, `--no-keys`, exit mapping, signals, restore round trip |
| `tests/test_zfsbackup_cli_migrate_backup.py` | `migrate --backup-dir`: behind/at head/newer, backup failures never migrate, workers-present refusal, concurrent migrates |
| `tests/test_zfsbackup_cli_check.py` | `zfsbackup-config check`: formats, exit codes incl. Nagios UNKNOWN, escaping, atomic output, daemon and certificate entries |
| `tests/test_zfsbackup_platform_contracts.py` | Contract tests for the werkzeug, requests/urllib3 and simple-websocket internals zfsbackup overrides |
| `tests/test_ci_workflows.py` | The CI workflows: the real-ZFS job runs only on schedule/dispatch on `master`, stubs stay in the mocked jobs |

Tests without a `unit`/`integration`/`real_zfs` marker are marked `unit` by a hook in `conftest.py`.

Shared TLS test helpers (importable from any test module; `tests/` is on `sys.path`; none reaches a real `zfs`, every socket is `127.0.0.1`):
- `tests/_tls_support.py` — real generated identities, a scripted `StubTrust`, `make_gateway`, `pin_client`/`unpin_client` through the real trust writers, `https_get`/`tls_probe`.
- `tests/_api_support.py` — client certificates, `FakeGateway`, `Authed` (a Flask app presenting `SSL_CLIENT_CERT`), `RealTrust`, the `/enroll` and `/rotate` request bodies.
- `tests/_remote_support.py` — `make_transport`, `fake_connect_pinned`, `client_tls_dir`, `whoami_json` for `RemoteBackupManager` tests.
- `tests/_enroll_support.py` — `RealServer` (real app + real gateway + real TLS listener) for the `enroll`/`identity rotate` CLI tests; `tests/_pty_driver.py` answers a prompt on a real pty.

Other shared helpers:
- `tests/_snapshot_support.py` — `listed_snapshot`/`listed_info`/`list_row` (snapshots and `zfs list` rows with the six listing columns; `txg=` to make txg order differ from name order) and a `frozen_clock` fixture.
- `tests/_prune_support.py` — `PrunePool`, a stateful fake of list/get/destroy with OpenZFS's all-or-nothing destroy and the captured per-version stderr; `before_destroy` hooks inject races; the `prune_pool` fixture fails a test that destroys an empty list.
- `tests/_raw_support.py` — databases built through the real migrations at a given revision, stamp helpers, `stub_table_ddl()` (stub tables carrying every registered column).
- `tests/_fake_zfs_check.py` — a read-only fake `zfs` for the `check` tests (snapshot listings and `get -H -o value` from a JSON spec; anything else exits 2).
- `tests/_openzfs_fixtures.py` — `load_section(version, section)` / `load_m1_summary(version)` over `tests/fixtures/openzfs/{2.1.5,2.2.2,2.4.1}/` (regenerate with `scenarios/test_openzfs_matrix.sh --captures`).
- When verifying a commit in a `git worktree`, run `PYTHONPATH=<worktree> <venv python> -m pytest`: the editable install otherwise imports the main tree.

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

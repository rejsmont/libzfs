# Local destination — implementation plan

**Status: decisions settled 2026-11-08; implementation not started.** Written against master 727e3e3
(v1.0.0rc3); line references point there.

Goal: back configured datasets up to another dataset or pool on the **same host** with a direct
`zfs send | zfs receive`, without HTTP, TLS or enrollment. Today the same result needs one daemon
enrolled with its own API over loopback.

## Decisions (settled 2026-11-08)

1. **Config shape: a `zfs:` URL scheme.** A local destination is `url: zfs:<dataset>` (for example
   `url: zfs:backup/zfsbackup`); the kind follows from the scheme. No schema change: the `url` column keeps
   its type and stays non-null. The dataset is everything after the first `zfs:` — no `//`, no host, no
   port, no query — and is checked by the dataset-name grammar, never by URL parsing, so a `:` inside it is
   fine. A `zfs:` URL never carries a `server_fingerprint`. (The alternative, a separate `target_dataset`
   key, was declined: it needed a SQLite table rebuild.)
2. **Layout on the target.** `<target>/<client_id>/<pool>/<path>`, the server's layout
   (`api.py:2856`). Discovery and pruning work unchanged (home check `backup_manager.py:960`), two hosts
   can share a target pool, and a loopback setup migrates with one `zfs rename`. A host that never
   enrolled has no client id, so the CLI creates one (`store.trust.ensure_client_id`, `trust.py:806`)
   when a write adds the first local destination; a later `identity set-client-id` orphans copies under
   the old id (the CLI prints the `zfs rename`).
3. **Retention of the copies.** The config stamped on each copy (`org.zfsbackup:config` =
   `to_property(destination)`, set at every receive and cycle), pruned by the existing received-dataset
   pruner — one hardened code path; copies of a removed dataset keep being pruned to their last rules.
4. **Lock-down of the copies.** The server's full forced set (`readonly=on`, `canmount=noauto`,
   `mountpoint=none`, `volmode=none`, share/setuid/exec/devices off; `api.py:206`, `:226`) plus `-h` and
   `-u`. Restore by clone or by setting `mountpoint`/`canmount` (runbook).
5. **Code sharing.** A no-behaviour-change refactor first (a `ChainSender` base class for the send side;
   receive helpers out of the `create_app` closures), local as a thin subclass; acceptance bar: the remote
   and API suites pass with no test file edited.
6. **Killing a stalled stream.** Yes: an additive `StreamHandle.terminate()` in libzfseasy (item 1.3).

Assumptions: a local target may not sit at, below or above `remote_backup.target_dataset`, a configured
dataset, or another local target (the `_require_outside_target` rule, `mapper.py:906`, generalised); the
operator creates the target dataset; `raw` keeps its meaning per dataset × destination.

## Why not `zfsbackup/store/`

That directory is reserved for backends that keep byte streams as files (object storage, cloud) and will
prune stream files, not snapshots. A local ZFS destination is the remote exchange minus the network: it
shares the send chain with `remote.py` and the receive decisions with `api.py`. New modules go next to
them, owned by `zfsbackup-developer`:

- `zfsbackup/chain.py` — the transport-independent send chain;
- `zfsbackup/receiver.py` — shared receive-side helpers;
- `zfsbackup/local.py` — the local manager and the in-process pump.

The `chain.py` interface (negotiate, resume token, stream one step) is the seam a future `store/` backend
would implement.

## Phase 1 — refactor, no behaviour change

**1.1 Split the chain out of `remote.py`** — `zfsbackup-developer`; `chain.py` (new), `remote.py`.
*needs-approval.*
- The chain logic already passes an opaque transport around: `backup_dataset` :699, `_send_mode` :787,
  `_server_copy_matches` :818, `_rollback_permit` :846, `_transfer` :1465, `_plan_chain` :1654,
  `_start_fresh` :1662, `_settle_up_to_date` :2012, `_resume_target` :2144, `_try_resume` :2207. HTTP lives
  only in `_negotiate` :1358, `_fetch_resume_token` :2076, the websocket half of `_push_step`
  (:1726–1923) and `_ready_transport` :995.
- `ChainSender(config, local_manager, *, client_id)` takes the generic methods and their shared state with
  **unchanged names and signatures** (tests patch them: `_negotiate` 31×, `_transfer` 36×, `_try_resume`
  14×, `_start_fresh` 9×, `_push_step` 6×). `_push_step` splits into `_stream_step` and a generic
  `_record_step` (anchor, sent-snapshot check, pin; :1924–1992). Wording hooks keep remote messages
  byte-identical. `RemoteBackupManager(ChainSender)` keeps TLS and identity; `chain.py` imports none of
  `requests`, `websocket`, `identity`.
- Verify: `tests/test_zfsbackup_remote*.py`, `test_zfsbackup_workers.py`, `test_zfsbackup_status_workers.py`,
  `test_zfsbackup_version_skew.py` with no test file changed; then the full suite.
- Risk: drift in wording or ordering; the `should_stop` points between steps must not move.

**1.2 Receive helpers out of `create_app`** — `zfsbackup-developer`; `receiver.py` (new), `api.py`.
*needs-approval.* Parallel with 1.1.
- Closures that depend only on the `_zfs` budget wrapper: `_server_dataset` :2856, `_server_copy_kind`
  :2877, `_resume_token` :2913, `_owner_problem` :2940, `_ownership_conflict` :2981, the
  ancestor-resume-token check in `_ancestors_state` :3794, ancestor creation (:4038–4047), abort-if-token
  (:4612–4667), receive assembly (:4711–4740). Each becomes a plain function taking `zfs_call`. Replies,
  claims, the pending map, session cuts and log memories stay in `api.py`. Already module-level helpers
  (`parse_stream_header`, `stream_base_problem`, `canonical_dataset_config`, `server_snapshots`,
  `decide_receive`, `newer_than`, the property constants) stay and are imported.
- Verify: `tests/test_zfsbackup_api*.py`, `test_zfsbackup_stream_gate.py`, `test_openzfs_formats.py`
  unchanged; new direct tests in `tests/test_zfsbackup_receiver.py`.
- Risk: check order is a security property — ownership → owned values → header → `-F` decision → abort →
  spawn. Both reviewers.

**1.3 `StreamHandle.terminate()` / `.pid`** — `libzfseasy-developer`;
`libs/libzfseasy/libzfseasy/zfs.py`. *needs-approval;* route through `libzfseasy-implementation-planner`.
- Additive: TERM, KILL after 5 s, abandon in D state as `_exec_lines` does; `close()` (:1163) stays the only
  reaper. Lets the pump kill a stalled pair without touching `StreamHandle._process` (:1146).

## Phase 2 — config shape (inert at runtime)

**2.1 Model and daemon checks** — `zfsbackup-developer`; `config/model.py`, `config/__init__.py`,
`daemon.py`, `workers.py`. *needs-approval.*
- `destination_url_problem` (:572) accepts `zfs:<dataset>` beside `https://`: the dataset part must pass
  the dataset-name grammar (no `@`/`#`, no leading `/`, not empty). `Destination` (:612–629) gains derived
  `is_local` and `target_dataset` properties (the field set and positional signature are unchanged);
  `from_file` (:1150–1156) refuses a `server_fingerprint` on a `zfs:` URL. A pure `destination_problem()`
  the store and CLI reuse.
- `daemon.py` `_remote_destinations` :402 and `_client_tls_problem` :408/:464 count URL destinations only;
  a warning when a local destination exists without a client id.
- `workers.py` `_send_to_destinations` :1005 skips a local destination with one WARNING (temporary guard,
  replaced in 3.1).
- Verify: `tests/test_zfsbackup_config*.py`, `test_zfsbackup_daemon.py`, `test_zfsbackup_reload.py`.

**2.2 Mapper and validation (no schema change)** — `zfsbackup-store-developer`; `store/mapper.py`
:572/:950. *needs-approval.* After 2.1.
- No migration: `zfs:` URLs are stored in the existing `destinations.url` column. `validate_config` uses
  `destination_problem()` and refuses a fingerprint on a `zfs:` URL (an imported or hand-edited DB).
- `_require_outside_target` (:906) becomes `_require_outside_receive_targets` over the remote target and
  every local target: no configured dataset at or below one, no recursive ancestor of one, targets
  pairwise disjoint.
- Verify: new `tests/test_zfsbackup_store_local_dest.py` (round trip, refusals), extended
  `test_zfsbackup_store_target_overlap.py`, `tests/test_zfsbackup_store*.py`.

**2.3 CLI** — `zfsbackup-cli-developer`; `zfsbackup/cli/`. *needs-approval.* After 2.2.
- `render.py` and `editor.py` need no change (the URL round-trips as is). `dotpath.py` :132: `set
  destinations.N.url` between `https://` and `zfs:` switches the kind — refuse while a
  `server_fingerprint` is set (unset it first); changing a `zfs:` target warns about a full send and
  orphaned copies. `listing.py` :177: a kind column and `"kind"`/`"target_dataset"` in JSON.
- `enroll` refuses a local destination; `check.py` :167–174 grades the client certificate only with URL
  destinations; `rename.py` :212–226 prints the copy path and `zfs rename`; `identity set-client-id` warns
  when local destinations exist.
- `write.py`/`import` call `ensure_client_id` under the writer lock when a local destination is added and
  the store has no id.
- Verify: `tests/test_zfsbackup_cli*.py`, new `tests/test_zfsbackup_cli_local_dest.py`.

**2.4 Tests for Phase 2** — `pytest-test-author`. *low-risk.*

## Phase 3 — runtime

**3.1 `LocalBackupManager` and the pump** — `zfsbackup-developer`; `local.py` (new), `workers.py`.
*needs-approval;* both reviewers. After 1.1, 1.2, 2.1 (and 1.3 if approved).
- `_ready_transport`: a `_LocalTarget(target, client_id)`; no client id or a missing target skips with one
  message per episode.
- `_negotiate` with the 1.2 helpers: ownership check; ancestors created one level at a time
  (`canmount=noauto`, local `client_id`, no `-p`); a child skipped while an ancestor holds a resume token;
  owned properties set on an existing copy; a strict listing filling `common`, `server_newer` (or unknown)
  and `server_copy` as remote negotiate does. `_fetch_resume_token` uses `_resume_token`.
- `_stream_step`: read the 312-byte header from our own `zfs send` (kind, compound refusal, base check);
  type check against an existing copy; `decide_receive`; `receive -A` on a fresh step with a token; spawn
  `zfs receive -s -u -h -o <owned + forced>` (`-F` only when allowed); feed the header, then copy in
  chunks of at least 1 MiB. A failed receive closes the send and raises the receive's error; a failed send
  closes the receive (`-s` keeps partial state) and raises the send's error; both stderrs logged; both
  children reaped on every path.
- Receive-side pre-checks run under `libzfseasy.timeout(≈60 s)` so a suspended target pool skips that
  destination instead of wedging the worker; the stream itself is never timed, and a watchdog kills both
  children after 600 s without progress (1.3). Stops only between steps. Dry run logs
  `[DRY RUN] … -> local <target>`.
- Wiring: the existing `RemoteBackupWorker` (:886) builds both managers lazily and dispatches on
  `dest.is_local` — same parents-first order (:952), interval (:926), status records and upgrade drain
  (:891); `check`/`status` unchanged. Local and remote sends run one after another.
- Verify: new `tests/test_zfsbackup_local.py` (mocked) and `tests/test_zfsbackup_local_pump.py` (real
  stub children: stall, kill, stderrs, no orphans); extended worker, status and version-skew tests.

**3.2 Pruning the copies** — `zfsbackup-developer`; `backup_manager.py`, `workers.py`, `daemon.py`.
*needs-approval.* After 2.1 and 3.1.
- Pull the body of `received_datasets()` (:744) into `_discover_under(target, only_client_id=None)`;
  `received_datasets()` and `health_collect.py:99` unchanged. Add `local_copies(client_id, should_stop)`
  covering only `<target>/<own id>` of each local target, so another host's copies are never touched.
- `PruningWorker._get_datasets` (:827) appends them; `_note_received` (:838) writes one `discover` record
  per local target; the pruning worker gets `client_id` from the `Handoff` (`daemon.py:195`) —
  `concurrency-reviewer`. Never recursive; the newest copy always kept.
- Verify: extended `test_zfsbackup_prune_descendants.py`, `test_zfsbackup_prune_forged_bytes.py`,
  `test_zfsbackup_status_workers.py`, `test_zfsbackup_daemon_resilience.py`.

**3.3 Real-ZFS tests** — `pytest-test-author`; `tests/test_zfsbackup_real.py`, a second pool in
`auto_zfs_pool` (`tests/conftest.py:647`). *low-risk.* Plain and raw chains same-pool and cross-pool,
encrypted source with raw auto, plain into an encrypted target, a worker killed mid-stream then resume
and a fresh receive with `receive -A`, rollback refusal and backstop, non-recursive pruning, an exported
target pool skipped then recovered.

**3.4 Scenario** — `real-zfs-scenario-dev`; `scenarios/test_local_destination.sh`, one VM, two pools.
*low-risk.* Import → daemon → a chain of at least 3 steps, `kill -9` mid-stream then resume, target
`zpool export` then recovery, a prune cycle, `check` grading, no zfs processes left.

## Phase 4 — docs (*low-risk*)

- `zfsbackup/README.md` runbook: creating the target (`zfs create -o canmount=off`), layout, restore,
  moving from loopback, USB pools; `config.example.yaml`; CLAUDE.md; `CHANGELOG.md`.
- `.claude/agents/README.md` ownership table gains `chain.py`, `receiver.py`, `local.py` (main session).

## Order

1.1 ∥ 1.2 ∥ 1.3 ∥ 2.1; 2.1 → 2.2 → 2.3 → 2.4; {1.1, 1.2, 2.1, (1.3)} → 3.1 → 3.2 → 3.3 ∥ 3.4 → 4.
Phase 1 is pure refactor and Phase 2 is inert, so each phase merges on its own.

## Deferred

- **Kernel-level piping between the children.** `_exec_stream` peeks into a Python buffer (`zfs.py:2992`)
  and `_exec_stream_in` always pipes stdin (`:3168`); it needs new libzfseasy API on both sides and loses
  stall detection. Revisit if the pump costs real CPU.
- **A separate local worker** for parallel local and remote sends: a second writer of anchors and pins,
  another drain stage and status file.
- **Health checks reading the copies directly**: the per dataset × destination anchor lag already covers
  it.
- **`rename dataset` renaming local copies itself**: it prints the command for now.

# Sender rework: recursive sends, then parallel sender threads

Approved 2026-11-10. Two tracks:

- **Track A** adds recursive sends and fixes the inherited-anchor bug found while planning. It runs on today's serial
  remote worker and ships first.
- **Track B** replaces the serial loop with one short-lived sender thread per due (dataset, destination) pair. It builds
  on Track A.

Owners and roles are in `.claude/agents/README.md`. Items tagged `needs-approval` were approved with the plan but still
pass `zfs-code-reviewer` (and `concurrency-reviewer` for B1.3 and B3) before they land.

## Decisions (user, 2026-11-10)

| Topic | Decision |
|---|---|
| Sender model | One short-lived **thread** per running send, inside the one long-lived `RemoteBackupWorker` process. No sender processes. |
| Start time | A stable per-pair offset, hashed from client id, dataset and destination, inside the snapshot frequency, after the aligned UTC boundary (formula in Track B). |
| Retries | Soon, but capped. An overrun gets one follow-up. An outage gets one catch-up within 5 min. A retryable outcome is retried after 60, 120 and 300 s. Any other failure is retried within `min(next slot, 1 h)`. A retryable outcome is recorded only once its retries are used up. |
| Same dataset, several destinations | Allowed to run concurrently. |
| Caps | Constants: `MAX_SENDERS = 4`, `MAX_SENDERS_PER_DESTINATION = 2`. No config knobs. |
| Reload | No new sends start while an older generation's sends finish (as today). |
| Recursive sends | Each descendant of an enabled recursive dataset is sent as its own plain or raw `-i` chain into its own copy. **Never** `send -R`, `-p` or `-I`: the server refuses compound streams (B-2a). |
| R-1 Disabled descendant | A configured descendant with `enabled: false` excludes **its whole subtree** from sending. Its snapshots are still taken and pruned with the parent. |
| R-2 Parent first | A child pair waits for its ancestor pair's first **successful** send to that destination. One WARNING per episode names the parent. |
| R-3 Clones | Clone children are sent as ordinary datasets (a full independent copy). |

## Background (verified in the code)

- **Recursive snapshots, non-recursive sends.**
  - `recursive: true` gives `zfs snapshot -r` (`backup_manager.py:1547`).
  - The recursive prune protects descendants' anchors and pins (`_descendant_anchor_protection`).
  - Nothing in `workers.py`, `chain.py`, `remote.py` or `local.py` reads `recursive`.
  - `DatasetManager._get_datasets` builds only configured entries, and the remote worker sends exactly those
    (`workers.py:1248-1262`).
- **Inherited anchors (bug).**
  - `get_anchor` and `get_inflight` read the *effective* value (`_get_prop`, `backup_manager.py:663-681`), so a child
    with no local `anchor.<d>` reports its parent's.
  - The due check (`workers.py:1353-1370`) finds that name among the child's snapshots, because a recursive snapshot
    or an aligned same-frequency name has it. Parents go first, so the parent's anchor is always fresh, and the child
    is "not due" for ever.
  - `_record_step` (`chain.py:~1003`) and `_settle_up_to_date` (`chain.py:~1104`, pin `~1130`) compare against the
    inherited value.
  - `health_collect` (`:145-150`) grades a never-sent child as fresh: a false OK from `check`.
  - Already correct: `_rollback_permit` (`get_local_anchor`) and the pruner's local/received protect-set reader. The
    pruner's inherited `get_anchor` values only widen protection and stay.

## Track A — recursive sends (serial worker)

### Semantics

- **What is sent.** Every filesystem and volume strictly below an **enabled recursive** configured dataset that has
  `remote:` entries. It goes to every destination of that dataset, with that dataset's per-destination `frequency`,
  retention and `raw`.
- **Explicit config wins.** A descendant configured explicitly uses its own entry and is not expanded twice. Further
  descendants expand from the **nearest** enabled recursive configured ancestor. A configured **disabled** entry
  excludes its subtree (R-1). Pairs are unique by (dataset, destination).
- **Stamped config.** Each child's stamped config is `dataclasses.replace(parent_cfg, name=child, recursive=False)`, so
  `to_property(dest)` carries the child's name, the parent's effective retention for that destination, the frequency
  and `recursive=False`.
- **Send mode.** `raw: auto` is evaluated per child (`_send_mode` already decides per `dsi`).
- **Discovery** is a name-only `zfs list -r -t filesystem,volume` of the parent. It is **untimed**, because source
  datasets are never timed (a stuck timed call would make every later timed call fail fast). It is scope-checked
  strictly below the parent.
  - A name libzfseasy cannot represent is skipped (with its WARNING).
  - A name at or below a local target or the server target is skipped with one ERROR (defence in depth; validation
    already refuses this).
- **Clones** are sent as ordinary datasets (R-3).
- **Order.** In the serial worker, children join the existing depth sort, after their parent. R-2 (wait for the
  parent's first success) is enforced by the Track B scheduler. In Track A, a child pair whose ancestor pair on the
  same destination has never succeeded (no `last_success` in the status records and none this incarnation) is skipped
  for that cycle with one WARNING per episode.
- **Lifecycle.**
  - A child created later is discovered the next cycle. It gets a full send once its first recursive snapshot exists.
  - A child destroyed on the source drops out of the pairs. Its status records go at `retain()`, except while that
    parent's discovery is failing. Its copy stays, and the receiver prunes it under its stamped retention (the newest
    is always kept).
  - A renamed child gets a full send under its new name. Its stale local anchor pins one snapshot until the first
    success overwrites it.
- **Children are never handed to the snapshot or pruning workers.** Client pruning is unchanged. The parent's recursive
  destroy already protects names pinned by descendants' local or received anchors and pins. A lagging child therefore
  retains its anchored snapshot across the tree; document this.
- **Copy-name length.** A copy whose full snapshot name would exceed 255 characters is refused at send time, with one
  ERROR per pair. `validate_config` can only check configured names.
- **Status cap.** When the pair count exceeds `STATUS_MAX_RECORDS` (`health.py:~507`), log one WARNING.

### Items

| # | Owner | Risk | What |
|---|---|---|---|
| A0 | pytest-test-author | low-risk | Characterization tests, `xfail(strict=True)` until the fix lands: (1) a configured parent and child share a destination and frequency, and the child has no local anchor → it stays "not due" across cycles; (2) `check` grades that never-sent child as fresh; (3) the descendants of a recursive dataset are never sent. |
| A1 | zfsbackup-developer | needs-approval | Add `DatasetManager.get_own_anchor` / `get_own_inflight` (`zfs.get.value(..., sources='local')`). Unreadable → `None` with a DEBUG line, so the pair is due and negotiate decides. Use them in the due check (`workers.py:1353`), `_record_step` (`chain.py:~1003`), `_settle_up_to_date` (`chain.py:~1104`, `~1130`) and `health_collect` (`:145`, `:150`). Leave `get_anchor`/`get_anchors` for the pruner. Fix the `_get_prop` docstring. Tests: A0 (1) and (2) flip; a matrix of {local, inherited, received, unset, unreadable} × {due, record, settle, health}; the pruner's protect set is unchanged. |
| A2 | zfsbackup-developer | needs-approval | `DatasetManager.expand_recursive(should_stop) -> Expansion(children, failed)` with the semantics above (`DatasetInfo.expanded_from`). `RemoteBackupWorker._get_datasets` appends the children before the depth sort. `_status_keys` covers the expanded pairs and keeps a failing parent's children's records. Discovery errors: one ERROR per parent per episode, INFO on recovery. The R-2 skip in the serial worker. The copy-name length check. The status-cap WARNING. Tests: kinds and deep trees, explicit override, nearest ancestor, disabled subtree excluded, skipped names, failed discovery, depth order, stamped config per destination, per-child `raw: auto`, no snapshots yet, vanished and renamed children, over-long name, R-2 skip. |
| A3 | zfsbackup-developer | low-risk | `health_collect`: `expand_recursive` plus one anchor observation per child × destination (own reads, the parent's expected interval). A failed discovery gives one WARN entry for that subtree. Also (A1 review L2): an unreadable own anchor is reported as an observation error (the raising `get_local_anchor` → `Observation(error=...)`, as the listing failure does), not as "never sent". Tests: `test_zfsbackup_health_collect.py`, `test_zfsbackup_cli_check.py` (JSON, Prometheus, Nagios). |
| A4 | zfsbackup-cli-developer | needs-approval | `rename destination` copies and clears `anchor.<old>`/`inflight.<old>` on the descendants of recursive datasets too (local values only) and prints the undo/inherit commands. `rename dataset` notes that children's copies move with the parent copy's `zfs rename`. Also (A1 review L3): `copy_anchor_property` (`backup_manager.py:~321`) and `_copy_inflight_property` (`cli/rename.py:~167`) copy only LOCAL values, never a received one: after A1 a received anchor is not evidence, and a rename must not turn it into a local one. Tests: `test_zfsbackup_cli_rename.py`, using a recursive tree in `tests/_fake_zfs.py`. |
| A5 | zfsbackup-developer (README), main session (CLAUDE.md) | low-risk | README "Recursive datasets" section; fix README ~953; update the CLAUDE.md `backup_manager.py`/`workers.py` paragraphs and the tests table. |
| A6a | pytest-test-author | low-risk | Real ZFS, using a tree of: an unencrypted parent; a child that is its own encryption root (raw); a grandchild inheriting that encryption; a zvol child; an explicitly configured child. Cover parent-first fulls, incrementals, a child created mid-run, the recursive prune keeping children's anchored snapshots, the inherited-anchor regression, and receiver pruning by stamped config. |
| A6b | real-zfs-scenario-dev | low-risk | `test_two_vm_backup.sh`, `test_local_destination.sh`: the same tree, plus a child created then destroyed mid-run. Never launch two VMs at once. |

Order: A0 → A1 → A2 → (A3 ∥ A4) → (A5 ∥ A6a ∥ A6b). The tests author follows each item.

## Track B — parallel sender threads

### Thread model

`RemoteBackupWorker` keeps everything the supervisor relies on:
- its name and its process;
- `drains_for_upgrade` / epoch −1;
- exit codes 3, 0, 4 and 1;
- `<db>.workers.lock`;
- its one status file.

Its main thread becomes the **scheduler**:
- **No `zfs` call.** Recursive discovery runs on discovery threads: one per parent at a time, keeping the last known
  child set, abandoned after a 5 s bounded join on exit.
- **One sender thread per due pair.** It starts `threading.Thread(name="send:<ds>-><dest>", daemon=True)` per due
  pair, never two for one pair, within the caps.
- **Each sender** builds **fresh** `DatasetManager`, `DatasetInfo` and `Remote`/`LocalBackupManager` objects with the
  shared `SendMemory`. It runs `send_pair`, puts its `PairOutcome` on a `queue.SimpleQueue`, and ends.
- **Imports up front.** `zfsbackup.remote`/`zfsbackup.local` are imported once after the version check, never mid-run.

**Stopping senders.**
- Only the scheduler calls `_exit_reason()`, once per `STOP_POLL` slice. On any reason it sets a `leave` Event.
- A sender's `should_stop` is `stop_event.is_set() or leave.is_set()`, polled between chain steps, never mid-stream.

**Exit paths.**
- **Reload or stage-1 drain.** The scheduler stops dispatching, sets `leave`, joins every sender (in slices, recording
  outcomes), runs the existing `_wait_for_stuck_writes`, then exits 3 or 0. `applied=`, the receive-writer gate and
  `_DRAIN_WORKERS` keep their meaning.
- **Stop.** The same, minus the stuck wait. After the supervisor's 30 s budget, `terminate()` kills the process and so
  every thread: today's semantics, with up to 4 torn `-s` streams instead of 1.
- **A scheduler exception.** `try/finally` sets `leave` and joins the senders. `run()` never returns with a live
  sender.
- **A sender exception** becomes a `failed` outcome.

**Shared state.**
- The scheduler thread is the **only** `StatusWriter` user. It records from the queue and flushes a heartbeat at least
  every `check_interval`.
- Everything that outlives a send, or is shared between pairs, lives in one `SendMemory` with one lock:
  - the once-per-episode latches;
  - the token-skip and negotiate-`zfs_timeout` counters;
  - the `_identity_error`, `_no_client_id_logged`, `_clock_noted` and `_no_txg_noted` latches;
  - the local `_pending` map with its TTL;
  - a `claim_expiry_check()` for the daily certificate reminder.
- Decisions are made under the lock; logging happens outside it.
- Why fresh managers per send:
  - `_server_*` and `_call_note` are per-call state stored on the instance;
  - `RemoteBackupManager._client_identity`/`_identity_failed` call `close()`, which would cut other threads' sessions;
  - a `requests.Session` must not be shared;
  - `DatasetInfo.snapshots` is written by every listing.
- `/whoami`, the identity reload and the local target check run once per send.

### Scheduling

- **Notation:** `F` = snapshot frequency; `P` = `remote.frequency or F`; `W = min(P, F)`;
  `lo = min(check_interval + 60 s, W/2)`.
- **Offset:** `frac = int(sha256(client_id ‖ dataset ‖ destination)[:8]) / 2^64`; `offset = lo + frac·(W − lo)`.
- **Slots:** `s_k = k·P + offset`, aligned to UTC.
- **The anchor-age due check stays in the sender,** on the own anchor from A1.
- **Catch-up:** if the status record shows `last_attempt < s_prev`, run within `frac·min(W, 300 s)`.
- **Overrun:** one coalesced follow-up.
- **Clock jumps:** recompute from wall-clock time and never re-run an attempted slot `k`.
- **Caps and order:**
  - 4 in total, 2 per destination.
  - A pair never runs while an ancestor or descendant pair on the same destination runs or is due ahead of it. The
    ancestor goes first, then by due time, depth and config order.
  - A blocked pair does not block unrelated pairs.
  - R-2 wait as above.
  - The same dataset may go to several destinations at once.

### Items

| # | Owner | Risk | What |
|---|---|---|---|
| B1.1 | zfsbackup-developer | low-risk | Extract `send_pair(manager, dsi, remote_cfg, sender, should_stop, exit_pending) -> PairOutcome` from `workers.py:1285-1376` (due check, `backup_dataset`, `_record_send` classification, per-destination exception path). The serial worker calls it; existing tests pass with no edits. |
| B1.2 | zfsbackup-developer | low-risk | `ChainSender.last_retryable(ds, dest)` in `PairOutcome`. True for busy, reloading, an unsettled ancestor, a connect failure before any byte, a stop between steps. False for `zfs_timeout`, token skips, refusals. |
| B1.3 | zfsbackup-developer | low-risk | `SendMemory` (locked, atomic note/clear/incr/pending/claim). Route the latches and counters of `ChainSender`, `RemoteBackupManager`, `LocalBackupManager` and `DatasetManager` through it. `last_problem` iterates a snapshot copy. The serial worker shares one memory; managers built without one get a private instance. Stress tests: 32 threads × 1000 operations. |
| B2 | zfsbackup-developer | low-risk | `SendSchedule`, a pure scheduling core with an injected clock: slots, catch-up, overrun, retries, the record-when-retries-are-spent rule, dispatch under the caps, exclusion and order, the R-2 wait, discovery timing. Table tests (F=P=1h → :33 for `frac`=0.5; F=1h P=1d → 00:33; F=5m → 3m45; F=1d P=1h → hourly; `frac`=0 → `lo`), plus catch-up, overrun, clock back, retry recording, caps, ordering, stable offsets. |
| B3 | zfsbackup-developer | needs-approval | The scheduler loop with sender threads, fresh managers, the `leave` Event, the outcome queue and discovery threads. Remove `_get_interval`/`_get_datasets`/`begin_cycle` use from the remote worker. Docstring updates in `daemon.py` (stop budget, gate residuals). Tests: real threads with fake senders; real worker processes under fork/spawn/forkserver for reload, the epoch −1 drain, stop with escalation, supervisor `kill -9`, forkserver death, `_wait_for_stuck_writes`, status counters across reload and the heartbeat; 4 concurrent `_Pump`s (every receive sees EOF while other threads spawn children: `close_fds`); the pruner's two-step read racing two pin writers. Use `timer_stretch()` and no tight wall-clock bounds. |
| B4 | various | low-risk | README (lines ~922, 975, 1429) and CLAUDE.md; a `check`/`status` regression with many pairs; scenarios (overlapping sends, reload during a long send, concurrent local sends, hung target pool, throughput at 1, 2 and 4 senders); real-ZFS concurrent chains from one dataset to two destinations. |

Order: B1.1 → B1.2 → B1.3 → B2 (they can run alongside A3–A6), then B3 after A2 and B2, then B4.

## Known trade-offs of the thread model

- A crash in a C extension, or an OOM kill, ends every in-flight send (up to 4), not one.
- The stuck-child registry is process-wide, so one hung target pool skips every local destination (as today).
  Per-pool admission in libzfseasy is deferred.
- The GIL may limit aggregate throughput (`_Pump` and websocket framing run Python code). B4 measures it.

## Out of scope

- `zfs send -R`/`-p`/`-I`;
- bandwidth limits;
- a server-side cap on concurrent receives;
- per-pool stuck-child admission;
- persisting stuck children across worker incarnations;
- handing sends over between reload generations;
- config knobs for the caps;
- per-child snapshot observations in `check`;
- `zfsbackup/store/`.

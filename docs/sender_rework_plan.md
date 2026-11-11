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
- **Backup copies are never sources.** Expansion skips, with its whole subtree, every dataset that carries
  `org.zfsbackup:client_id` or `source_dataset` as a local, received or inherited value. One value-free
  `property_sources` read per parent finds them, and a failed read fails the parent's expansion. The server target
  counts as a receive target whether or not `remote_backup` is enabled, and `validate_config` agrees.
- **Order.** In the serial worker, children join the existing depth sort, after their parent. R-2: a child pair waits
  until its nearest ancestor pair on the same destination has been **confirmed in this incarnation**, meaning its
  negotiate reported an existing copy or a stream to it committed. An anchor alone is not evidence, because it is keyed
  by destination name and can outlive the copy. An unconfirmed ancestor pair with waiting children is treated as due,
  so it negotiates once per incarnation. One WARNING per (ancestor, destination) per episode, with a count. A child
  with no snapshots is not held. In a dry run, the ancestor's `backup_dataset` returning True passes.
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
| A2b | zfsbackup-developer | needs-approval (retention; a safety fix found in the A2 review) | The recursive prune must never destroy snapshots inside a backup copy below the recursive dataset, for example the copies of a `zfs:` destination later removed from the config, or a `send -R` replica. `zfs destroy -r pool@snap` also destroys `copy@snap` when names match. Feed the copy-root detection (local, received or inherited `client_id`/`source_dataset`) into `_descendant_anchor_protection` / `_still_prunable`: a copy below the dataset protects every snapshot name it holds, so the recursive destroy of that name is skipped, with one ERROR per episode naming the copy and the remedy (move it out of the recursive tree). An unreadable copy read protects everything (no recursive destroy that pass). Tests in `test_zfsbackup_prune_descendants.py` style. |
| A2c | zfsbackup-developer | needs-approval (user decision 2026-11-10: "skip copies") | The snapshot worker never snapshots into a backup copy below a recursive dataset. When `_copy_roots` finds a copy strictly below a recursive dataset, take ONE atomic `zfs snapshot a@s b@s …` over the dataset and its source descendants (a name-only `zfs list -r -t filesystem,volume`, minus every copy subtree) instead of `snapshot -r`, with one ERROR per (dataset, copy root) per episode naming the copy and the remedy. No copy → plain `snapshot -r` as today. If the copy read or the listing FAILS, fall back to plain `snapshot -r` with an ERROR. This is deliberate: a stray snapshot in a copy is recoverable, a missed source snapshot is not. A child created between the listing and the snapshot misses one snapshot (next period catches up). Check libzfseasy's `zfs.snapshot` accepts several datasets in one call (it must be one command, so the snapshot is atomic). |
| A2d | zfsbackup-cli-developer | needs-approval (user decision 2026-11-10: refuse, `--force` overrides) | Prevention, so plain `snapshot -r` stays the normal path. `validate_config` already refuses DECLARED targets at, below or recursively above configured datasets. The CLI additionally reads ZFS before committing (`set`/`unset`/`edit`/`import`, through the shared write path): (1) a dataset that becomes `recursive: true`, or is added as recursive, with a backup copy below it (`DatasetManager._copy_tops`: markers local/received/inherited, plus receive targets) → refuse, exit 1, naming the copy and the `zfs rename` to move it out; (2) removing a `zfs:` destination, or the server `target_dataset`, while copies under that target would be left below a recursive configured dataset → refuse with "move them first". `--force` overrides both (a warning is still printed). When zfs cannot be read (no zfs on this host, pool not imported, timeout), WARN only and commit. The daemon keeps A2c as the backstop for copies no config check sees (outside replicas). |
| A3 | zfsbackup-developer | low-risk | `health_collect`: `expand_recursive` plus one anchor observation per child × destination (own reads, the parent's expected interval). A failed discovery gives one WARN entry for that subtree. Also (A1 review L2): an unreadable own anchor is reported as an observation error (the raising `get_local_anchor` → `Observation(error=...)`, as the listing failure does), not as "never sent". Tests: `test_zfsbackup_health_collect.py`, `test_zfsbackup_cli_check.py` (JSON, Prometheus, Nagios). |
| A4 | zfsbackup-cli-developer | needs-approval | `rename destination` copies and clears `anchor.<old>`/`inflight.<old>` on the descendants of recursive datasets too (local values only) and prints the undo/inherit commands. `rename dataset` notes that children's copies move with the parent copy's `zfs rename`. Also (A1 review L3): `copy_anchor_property` (`backup_manager.py:~321`) and `_copy_inflight_property` (`cli/rename.py:~167`) copy only LOCAL values, never a received one: after A1 a received anchor is not evidence, and a rename must not turn it into a local one. Tests: `test_zfsbackup_cli_rename.py`, using a recursive tree in `tests/_fake_zfs.py`. |
| A5 | zfsbackup-developer (README), main session (CLAUDE.md) | low-risk | README "Recursive datasets" section. Also from the A2 review: (a) anchors are keyed by destination NAME, so after a URL or `zfs:` target switch a dataset's stale fresh anchor reads as "sent" and it waits up to one period (R-2 is NOT affected: since the A2 re-review it uses `copy_confirmed`, never the anchor, and a parent with waiting children is due once per daemon start), so document it and have A4/`set`/`edit` warn; (b) a child without a readable own anchor has an unprotected common snapshot until its pin is written (same as configured children); (c) `raw: true` on a parent applies to every child, and an unencrypted child is refused with the existing ERROR; (d) dropped children (names libzfseasy cannot represent) leave only libzfseasy's WARNING; (e) a child's encoded config near the 8 KiB cap is refused at negotiate. Contents: fix README ~953; update the CLAUDE.md `backup_manager.py`/`workers.py` paragraphs and the tests table. |
| A6a | pytest-test-author | low-risk | Real ZFS, using a tree of: an unencrypted parent; a child that is its own encryption root (raw); a grandchild inheriting that encryption; a zvol child; an explicitly configured child. Cover parent-first fulls, incrementals, a child created mid-run, the recursive prune keeping children's anchored snapshots, the inherited-anchor regression, and receiver pruning by stamped config. |
| A6b | real-zfs-scenario-dev | low-risk | `test_two_vm_backup.sh`, `test_local_destination.sh`: the same tree, plus a child created then destroyed mid-run. Never launch two VMs at once. |

Order: A0 → A1 → A2 → A2b → A2c → (A2d ∥ A3 ∥ A4) → (A5 ∥ A6a ∥ A6b). The tests author follows each item.

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

Carried over from A2 (the serial worker's state that B must move or rework):
- `_waiting_noted`, `_pairs_over_cap_noted`, `_expand_*_noted`, `_dry_run_sent` and the `_dependents` map are plain
  per-worker state. They belong in `SendMemory`, or in the scheduler thread only.
- `copy_confirmed` is done (B1.3): it lives in `SendMemory` (`COPY_CONFIRMED`), keyed by (dataset, destination), so
  confirmation survives fresh managers within the incarnation.
- `_waits_for_ancestor` lists each held child's snapshots every cycle while its ancestor is unconfirmed. Under the
  scheduler, hold children without listing, and decide "no snapshots" in the sender.

### Scheduling

- **Notation:** `F` = snapshot frequency; `P` = `remote.frequency or F`; `W = min(P, F)`;
  `lo = min(check_interval + 60 s, W/2)`.
- **Offset:** `frac = (int(sha256(client_id \0 dataset \0 destination)[:8]) >> 11) / 2^53` (exactly representable, so
  always in [0, 1)); `offset = lo + frac·(W − lo)`.
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
  - R-2 wait as above, applied to EVERY child pair, configured or expanded (A2 applies it to expanded children only;
    a configured child whose parent never negotiated can still create a placeholder parent copy until B lands).
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

### What B3 must handle (from the B1/B2 reviews, 2026-11-11)

- **One sender factory.**
  - Every per-send manager (`DatasetManager`, `RemoteBackupManager`, `LocalBackupManager`) is built in ONE factory
    that passes `memory=self._mem()` and asserts the result shares it.
  - A missing memory silently loses `COPY_CONFIRMED`, and R-2 then holds the children for ever.
  - Each sender gets its own `DatasetManager` and `DatasetInfo`, never the scheduler's, and calls `close()` in a
    `finally`.
  - Latch the per-send "client TLS identity loaded" INFO (by fingerprint) and the "cannot reach" ERROR in the memory,
    so they stay once per episode rather than once per send.
- **Local negotiate claim.**
  - Two sibling pairs on one `zfs:` target can both try to create the same missing ancestor. The second gets "dataset
    already exists": an ERROR and a non-retryable failure.
  - Fix: add a claim in `SendMemory` (`claim(name)`/`release(name)`) over the copy and its missing ancestors. A
    claimed ancestor is a retryable `NegotiationFailed`, like the server's 503 for an unsettled ancestor. A per-target
    lock held for the whole local `_negotiate` (bounded by `_Budget`) also works.
- **Per-parent discovery.**
  - It needs a per-parent entry point to `expand_recursive`.
  - Its episode forgetting (`retain`, `reset_latches`) must be scoped to that parent, so one parent's discovery never
    clears another root's latches.
- **The daily certificate reminder** runs from `begin_cycle` today. If B3 drops `begin_cycle`, call
  `claim_expiry_check` from the scheduler.
- **Stopping.** Senders never call `_exit_reason()`: `should_stop` and `exit_pending` are "stop_event or leave".
- **Invariants that `SendMemory` users rely on,** to be tested under real threads:
  - one thread per pair;
  - no ancestor and descendant pairs running at once on the same destination.

  The `COPY_CONFIRMED` clear/note, the token counters and `AncestorRule.hold`'s confirmed-then-latch are
  check-then-act across two lock acquisitions, and are correct only under these invariants.
- **R-2 backstop for configured children.** B2 holds configured children too; `AncestorRule.hold` covers only
  expanded ones. Extend the sender-side backstop to configured children, or rely on the schedule plus
  `PairOutcome.confirmed`/`SendSchedule.unconfirmed`. Test either way.
- **libzfseasy (low, libzfseasy-developer).** `_name_warnings` is a global before/after counter. Another thread's
  WARNING can turn this listing's summary into a WARNING. Return the count from the build, or make it thread-local.
- **Document:** after a reload, every unconfirmed parent with children negotiates at once. The caps bound this, but it
  bypasses the stagger.

### Decisions taken while building B2 (`zfsbackup/sendschedule.py`, 2026-11-11)

- **Hash input.** It is NUL-joined (`client_id \0 dataset \0 destination`), so ("a","bc/d") and ("ab","c/d") differ.
- **Clock steps back.** This deviates from "never re-run an attempted slot k".
  - Whenever `attempted > slot_index(now)`, the attempted index is clamped to `slot_index(now)`. The next regular run
    is then at most P away, whatever the pair's history.
  - The cost is at most one extra run per step back, and the sender's anchor-age check turns it into a cheap
    `NOT_DUE`.
  - Pending catch-up, retry, R-2 confirmation and discovery times are capped by distance (`CATCH_UP_CAP`,
    `FAILURE_RETRY_CAP`, now, the discovery interval).
  - Why: the literal rule stalls every pair for the size of the step, which can be days on a host that boots before
    NTP.
- **Retries.** An "other failure" is retried at `min(next slot, now + 1 h)`, with no jitter. A retryable burst
    (60/120/300 s) that is used up is recorded once, and the burst then resets.
- **Immediate runs.**
  - The overrun follow-up and the once-per-incarnation R-2 confirmation are due immediately.
  - Children released by R-2 are not staggered; only the caps limit them.
- **Additions to the plan.**
  - `MAX_DISCOVERIES = 4` concurrent parent discoveries.
  - `LATE_SLACK = 60 s`: a live idle pair that first sees its slot later than this gets one catch-up.
- **Order.** A pair ranks by the earliest due time among itself and its due descendants, so an ancestor never ranks
  behind its child. `dispatch` reserves nothing: the caller calls `started` for each pair it returns.

## Follow-ups found during Track A (not yet scheduled)

- **`check` docs and residuals, from the last collector review (low; fold into A5):**
  - `--per-call-timeout` help: "a hung pool then costs this, not the whole --time-limit" is no longer true for the
    batched reads (a slow or killable-hung pool can spend the time left there). The help also omits the expansion and
    `_copy_roots` from the batched reads.
  - Docstrings: the M1 example "an umlaut in its name" is wrong (zfs names are ASCII; the real triggers are names zfs
    allows and libzfseasy refuses, e.g. a component starting with `.`/`-`); the overshoot text says "up to 2 s to
    join the stderr reader" (it is 0.05 s).
  - M1 relies on libzfseasy's error text ("…the first: libzfseasy cannot represent the name"). It is pinned by
    `test_framing.py` today, but not across repositories once libzfseasy moves. Better: `zfs.list(strict=True,
    skip_unrepresentable=True)` in libzfseasy (an API addition, needs approval), which also avoids listing twice.
  - Optional: skip a parent's batched reads when its own read just timed out. Optional: re-read the properties when
    an anchor name is missing from the listing (a send plus a prune inside the batch window).

- **From the final A2d/A2e review (all low):**
  1. The boundary retry gives up inside the `-r` FALLBACK. That fallback runs when an unrepresentable name sits below
     the dataset, or when the argv bound is hit. The retry's strict re-listing, or `many`'s bound, fails again, so the
     rest of the tree misses that period, with an ERROR. Use the non-strict listing for the `-r` retry, so only the
     unrepresentable dataset itself misses `@N`.
  2. The `_snapshot_tree` docstring still says a failure of `many` is not caught. It is now retried once.
  3. `copies.py` given-up-target loop: the target's existence is cached before `pool_absent` runs, so a second label
     for the same target can be skipped silently. The exception texts use `{target}` instead of `{label} ({target})`.
  4. libzfseasy fork guard residuals (unreachable from zfsbackup): `watchdog.cancel()` in a fork takes an `Event`
     lock the parent's watchdog thread may have held. A generator RESUMED to completion in a fork sees ECHILD →
     returncode 0, a false success. Document it, or raise `RuntimeError` on resume in a fork; concurrency-reviewer.
  5. `import` with an unreadable stored config runs the copies check before `import_yaml`'s own refusal (cosmetic).
  6. CLAUDE.md prose (not the tests table) does not yet describe `cli/copies.py` and the `--force`/`--force-copies`
     flags, the snapshot worker's depth order, ride and holder retry, or the libzfseasy fork guard. These belong to A5.

- **A failed snapshot listing reads as "no snapshots" in `needs_snapshot` (pre-existing).** The dataset counts as due,
  `create_snapshot` then fails ("already exists"), and the status record shows a creation failure instead of the
  real cause, the listing. Make `needs_snapshot` report "unknown" on a list failure (no attempt, one ERROR naming the
  listing).
- **The A2e retry's holder listing runs on every failed tree command.** With a persistent failure (pool out of space)
  that is one full recursive snapshot listing per cycle. It is acceptable, but could be skipped when the error is
  clearly not a collision. Also, the retry error repeats itself when both attempts fail the same way (cosmetic).

- **A configured child under a recursive parent, snapshotted first, blocks the parent's tree (pre-existing).** The
  snapshot worker walks datasets in config order. A configured enabled child with the same frequency as its recursive
  parent, listed before it, takes `@N` first. The parent's `snapshot -r` (or the A2c atomic list) then fails with
  "already exists" every period, so the parent's whole tree is never snapshotted. Validation does not refuse this
  layout. **Decided 2026-11-10 (item A2e): parents first, ride along.** The snapshot worker snapshots enabled recursive
  parents before their configured descendants (a depth sort, as the remote worker does). A configured child whose
  `@N` was just created by an enabled recursive ancestor's snapshot (`snapshot -r` or the A2c atomic list) counts that
  as its own snapshot for that period: no "already exists" WARNING, not a failure. It keeps its own retention and
  destinations, and takes extra snapshots only when its own frequency is higher. Tests: config order child-before-parent,
  equal and different frequencies, the parent's atomic snapshot covering the child, and the child's own pruning.
  **Review follow-up (implemented):** a period boundary falling between the parent's and a configured child's turn
  in one cycle, or one failed parent command, lets the child take `@N` alone. The parent's tree command then
  collides. On ANY failure of the tree command, one name-only snapshot listing finds the source datasets that
  already hold `@N`, and the command is retried ONCE as an atomic `many` over the rest. Those holders already have
  their snapshot for the period, and they are not covered by the ride. No holders means the original error. A failed
  retry fails the cycle, with no loop. A child-side skip was deliberately NOT done: the retry closes every miss, and
  a skip could starve a child. The cost is that the rest of the tree gets `@N` one cycle later than the child, not
  in the same atomic command.

- **libzfseasy option order.** `zfs.inherit` emits `zfs inherit <prop> -S <ds>` (and `-r` likewise) with the option AFTER
  the property. glibc getopt permutes, so this works on Linux, but a BSD or illumos getopt stops at the first operand.
  Move the options before the property (libzfseasy-developer). Check the other commands for the same pattern.

- **The cost of the copy read (A2/A2b review).**
  `zfs get -H -o name,property,source -s local,received,inherited -r … all <ds>` returns a row for every inherited
  native property of every dataset in the tree. It runs once per expansion (each remote cycle) and once per destroy
  call in `_still_prunable`.
  - **Done:** `zfs.get.property_sources(..., properties=[...])` takes an explicit property list and `_copy_roots` passes
    `org.zfsbackup:client_id,org.zfsbackup:source_dataset`, so the read is `zfs get -H -o name,property,source
    -s local,received,inherited -r -t filesystem,volume org.zfsbackup:client_id,org.zfsbackup:source_dataset <ds>`
    (still value-free). What remains is the call count.
  - Or share the discovery with the descendant-anchor read.
- **Copies without markers** (a manual `send | recv` with no `-o`, a syncoid replica without properties), not under a
  known receive target, are indistinguishable from source children. They are sent as children (A2) and pruned
  recursively. A6a: also add a `real_zfs` test proving `zfs destroy -r R@x` keeps a MARKED copy's `@x`.
- **libzfseasy at-fork hook (low, libzfseasy-developer).**
  - The gap: a process forked while a `StreamHandle` write stream holds unflushed bytes, which never calls `close()`,
    flushes those bytes into the parent's `zfs receive` when GC finalises its `BufferedWriter`.
  - Fix: register an `os.register_at_fork(after_in_child=...)` hook that `dup2`s `/dev/null` over every live
    handle's inherited pipe fds (a `WeakSet` of handles and `_exec_*` `Popen`s).
  - Not reachable from zfsbackup: no worker forks with a live stream.
  - Pinned by a strict xfail in `test_libzfseasy_pipe_leaks.py`.
- **Found by A6 (2026-11-11; no code bug: the real tier passed on 2.1.5/2.2.2/2.3.9/2.4.1, the recursive scenarios
  152/152 local and 70/70 two-VM):**
  - `scenarios/test_openzfs_matrix.sh`: install `samba` on jammy so the 2.1 raw-receive `sharesmb` tests run instead
    of skipping (they pass with it), and put a `timeout` around `add-apt-repository` (Launchpad was unreachable and
    it hung for over 10 minutes before the `uv` fallback).
  - `test_zfsbackup_real.py::…test_a_modified_server_head_with_the_base_the_newest_snapshot_still_succeeds` always
    skips: the copy has the forced `mountpoint=none`; set a mountpoint before mounting it.
  - The real tier has no recursive-SEND test (only prune/snapshot); the scenarios cover it. Add one.
  - The multipass wedge also hits a long `log_since … | grep | cut | tail` pipeline in scenario 5 of
    `test_local_destination.sh`; filter in-guest.

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

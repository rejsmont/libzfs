# Parallel Sender Threads — Real-World Scenario Report

**Date:** 2026-11-12
**Harness:** [`scenarios/test_parallel_senders.sh`](../scenarios/test_parallel_senders.sh) (sender rework, Track B, item B4 of
[`sender_rework_plan.md`](sender_rework_plan.md))
**Tree under test:** `git archive HEAD` = `c5908ca` for the main run (the working-tree re-run is in "Re-run after B4 fixes") ("Sender rework B3: scheduler loop with parallel sender threads"), shipped to both VMs. Uncommitted work
in the repository (other agents' B4 edits) is **not** in what was tested.
**Scope:** the remote worker's scheduler with up to 4 concurrent sender threads (2 per destination) on real ZFS pools and a real
network, with the real daemon under systemd (`KillMode=mixed`, `TimeoutStopSec=90`, as the shipped unit).

---

## Result

| # | Scenario | Result | Assertions |
|---|---|---|---|
| 1 | Overlapping sends, 6 datasets x (remote over mTLS, local `zfs:`) | **pass** | 21 / 21 |
| 2 | Reload during a long send (3 incremental steps in flight) | **pass** | 24 / 24 |
| 3 | Concurrent first sends into a missing container tree (siblings, cousins), 5 rounds | **pass on data, FAIL on a bug** (see B1) | 42 pass, 4 fail |
| 4 | Hung local target pool (dm-linear device frozen with `dmsetup suspend`) | **pass** | 34 / 34 (30 / 30 in the full run, 34 after adding a healthy second local destination) |
| 5 | Throughput at 1, 2, 4 concurrent senders vs raw `zfs send \| zfs receive` | **measured** | 76 / 76 |

Full run (one invocation, setup to teardown, about 55 minutes): **215 passed, 4 failed**. All four failures are the same defect (B1, round 1, 2, 3 and 5 of
scenario 3). Scenario 4 was re-run alone afterwards with a healthy second local destination added (34 / 34); every other number below is from
the full run. Every copy of every dataset on every destination ended complete and guid-identical in every scenario, with no torn stream left behind.

**Bugs found:** B1 (fixed in B4, see below), a race between two sender threads on a freshly created local container, logged as a non-retryable ERROR with
misleading advice (self-healing, but noisy and delaying). Details, log excerpts and root cause below. **Environment limit:** the 1 GiB `zfsb-server` VM is
OOM-killed by two concurrent large receives (F1).

---

## Environment and method

* **VMs (existing; none launched or deleted):** sender `zfsb-local` (Ubuntu 24.04, OpenZFS 2.2.2, 2 vCPU, 2 GiB; Python 3.12, workers by `fork`) and server
  `zfsb-server` (Ubuntu 26.04, OpenZFS 2.4.1, 1 vCPU, 1 GiB; Python 3.14, workers by `forkserver`). `zfsb-client` was not needed. All pools are file-backed
  images in `/var/tmp` with `autotrim=on` (the images otherwise grow to their full size and filled the sender's disk). The hang pool is a loop device under a
  device-mapper `linear` target (`failmode=wait`).
* **Enrollment as an operator does it:** `trust token create --json` on the server, the token delivered as a file, `enroll remote1 --url https://<ip>:8080
  --token-file ...`; client id `psend1`. The sender's datasets go to `remote1` (HTTPS, pinned mTLS) and to local `zfs:` destinations (`local1`, `local2`,
  `hung1`) at the same time.
* **Evidence is process state, not log wording.** A guest-side sampler records every change of the live `zfs send` / `zfs receive` processes (0.1 s) on both VMs,
  so overlap and the caps are read from the process table; the daemon log is a second, independent witness (a send runs from its `Sending ...` line to its
  `Backup complete` line). Copies are judged by `(snapshot, guid)` sets, anchors, `receive_resume_token`.
* **Start alignment.** The scheduler staggers pairs by a hash fraction (`sendschedule.pair_fraction`), which would spread small sends over seconds. For the
  scenarios that need several pairs due at the same instant (2, 3, 5) the harness picks *dataset names* whose fractions (for every destination used) lie in a
  window of 0.002-0.02 (40 ms-0.4 s at `W` = 20 s). Nothing in the daemon is changed; scenario 1 uses ordinary names.
* **Run-time tuning of the small VMs (reset at the end):** `zfs_arc_max` 768 MiB (sender) / 256 MiB (server), the server's `zfs_dirty_data_max` 64 MiB and
  `vm.dirty_bytes` 96 MiB. Scenarios 1, 2 and 4 run the unit with `CPUQuota=40%` so sends last seconds (uncapped, 192 MiB takes 0.3 s locally); 3 and 5 are uncapped;
  scenario 5 runs the daemon without `-v`.

---

## Scenario 1 — overlapping sends

Six datasets of 192 MiB, each to `remote1` and `local1` (12 pairs), one pre-taken snapshot each, the snapshot worker quiet (frequency 1d).

| Measure | Sampler (process table) | Daemon log |
|---|---|---|
| Most `zfs send` alive at once (cap 4) | **4** | **4** |
| Most local receives at once (cap 2 per destination) | **2** | 2 (local sends) |
| Most receives at once on the server (cap 2) | **2** | 2 (remote sends) |
| Seconds with >= 2 / 3 / 4 sends alive | 22.1 / 10.1 / 3.9 | 22.8 / 11.8 / 4.8 |
| Window for 12 sends | 24.7 s | |

All 12 pairs completed; the 12 copies on each side are guid-equal; the status file holds 12 `ok` records with no consecutive failure; no ERROR and no Traceback on
either VM. Log excerpt (sender; the remote pair of `p4` and `p5` start 1 s apart, `p4` local joins 3 s later):

```
01:08:05,164 remote INFO Sending full snapshot zpsrc/p4@autosnap_20261112000758
01:08:06,183 remote INFO Sending full snapshot zpsrc/p5@autosnap_20261112000758
01:08:09,317 local  INFO Sending full snapshot zpsrc/p4@autosnap_20261112000758
01:08:11,792 local  INFO Backup complete: zpsrc/p4@... (full) -> local zptgt/a
01:08:11,851 remote INFO Backup complete: zpsrc/p4@... (full) -> https://192.168.252.97:8080
```

## Scenario 2 — reload during a long send

Two datasets (start-aligned) x (`remote1`, `local1`), a baseline, then a backlog of three snapshots with 100 MiB of new data each. The moment three incremental
`zfs send` processes were alive (guest-side trigger, 50 ms poll), `zfsbackup-config set prune_interval 90s` was run.

```
T_TRIGGER  01:09:35.5   INFLIGHT zfs send -c -L -i zpsrc/r2x43231@..0846 zpsrc/r2x43231@..0920   (x2: remote and local)
                        INFLIGHT zfs send -c -L -i zpsrc/r1x78113@..0846 zpsrc/r1x78113@..0920
           reload requested ... "it accepted generation 5 and is switching its workers over (in-flight transfers finish first)"
01:09:37-39  all four first steps "Backup complete"            <- every in-flight step FINISHED
01:09:41,240 __main__ INFO Reloaded remote worker (pid=414935) on generation 5   (old pid 411938)
01:09:55-10:04  the chain continues from the anchor: 0920->0923, 0923->0925 on both destinations
```

Verified: every in-flight step was logged complete *before* the worker was replaced; no `receive_resume_token` on either side, no `Resumed`, no `Sending full` after
the baseline; exactly **12** incremental steps sent and 12 acknowledged (2 datasets x 2 destinations x 3 steps: none twice, none missing); `status --wait` exits 0
with `applied` = stored generation (5) and the pid file shows `applied=` with no `target=`/`deferred=`; all 8 (snapshot, guid) pairs equal on both destinations.
Cost of the reload as observed: 6 s from the request to the new worker, then the pairs wait their catch-up delay (up to `frac x 20 s`, 14 s here) before the next step.

## Scenario 3 — concurrent local sends into a missing container tree

Five rounds, each on a fresh target tree: four start-aligned datasets to one local target, two siblings (`zpsrc/gN/a..`, `zpsrc/gN/b..`; container `zpsrc/gN`
missing) and two cousins (`zpsrc/hN/x/c..`, `zpsrc/hN/y/d..`; containers `hN`, `hN/x`, `hN/y` missing), plus `<target>/psend1` and `<target>/psend1/zpsrc`, all created
by the first senders.

Data result: in all 5 rounds all 4 copies were present and guid-equal, no resume tokens, every pair's last status record `ok` with 0 consecutive failures, nothing
busy in the last 60 log lines (6-30 s to complete). The negotiate claim was exercised: **10** `local target zptgt/a is busy: another send is creating
zptgt/a/psend1[/zpsrc]; skipping <ds> this cycle` INFO messages over the 5 rounds, every loser completed later.

**But** 4 of the 5 rounds logged an ERROR (see B1), so the harness reports 4 failures here.

## Scenario 4 — hung local target pool

`remote1`, `hung1` (`zfs:zphang/zfsbackup`, on a dm-linear device) and (final run) a healthy `local1`, three datasets, a 256 MiB backlog per dataset. The device is
frozen with `dmsetup suspend` 0.4 s after the first `zfs receive` into the pool appears (so the stream is cut mid-flight), and the daemon runs on.

| Check | Result |
|---|---|
| The frozen pool really blocks: receive into it blocked in the kernel (D, or waiting on the txg) | yes (1 process) |
| `remote1` receives the whole backlog for all 3 datasets while `hung1` is frozen | yes; `hung1` did not advance |
| A snapshot taken during the hang reaches `remote1`; so does another after the abandon | yes, both |
| Caps during the hang | <= 3 sends in all, 1 receive into the frozen pool |
| First *timed* zfs call on the pool (a negotiate `zfs set`) runs out its 60 s budget and its D-state child is abandoned | logged **68 s** after the freeze: `zfs set (pid N) did not exit after SIGTERM and SIGKILL (uninterruptible, likely a hung pool); calls under a timeout are refused until it exits` |
| Local destination skipped, one message per episode | 1 x `... hung1: zfs did not answer within 60 s on this host; skipping this destination ...` (WARNING) and 1 x `local backup to hung1 skipped: 1 zfs process(es) of this worker did not exit after SIGKILL ...` (WARNING); the repeats (every 20 s) are DEBUG; no repeat in the next 100 s |
| The stuck-child registry is per process (documented trade-off): the **healthy** `local1` is skipped too | confirmed: `local1` stayed behind `remote1` after the abandon |
| Stop with the pool still frozen | SIGTERM at 01:34:47.5; at 01:35:17.7 `WARNING remote worker did not exit within the 30s shutdown budget, terminating`; supervisor gone **33 s** after the request (unit budget 90 s); no daemon/worker process left; 1 `zfs` child stuck in the kernel outlived its worker |
| `dmsetup resume` | the stray exits within seconds; `zpool status -x`: healthy |
| Restarted daemon | `Resumed zpsrc/h1@... (from ...) to hung1` (a cut receive left a token), then the chain; all `hung1`, `local1`, `remote1` copies complete and guid-equal, no tokens |

The stop path is the plan's "stop = escalation" case: a sender thread inside its pump is never cut mid-stream, so the remote worker does not leave by itself and is
terminated after the supervisor's 30 s budget (one WARNING); the other workers stop in under a second. The pump's own 600 s stall bound was **not** reached: the
first timed call on the pool fires at ~60 s, long before it. (The pool did not wedge the kernel; no `multipass restart` was needed.)

## Scenario 5 — throughput

Datasets of 512 MiB (local destinations) or 160 MiB (when `remote1` is a destination: the 1 GiB server cannot take two concurrent 600 MiB receives, F1), plain
`zfs send -L -c`. Containers are pre-created (the first sends would otherwise queue on the negotiate claim). "Raw" is the same data, same concurrency, started at
once: `zfs send | zfs receive` for local legs, `zfs send | nc -N | nc -l | zfs receive` for the network leg. Daemon window = first `zfs send` alive to last exit
(sampler); two repetitions each, data written just before each run.

| Config (senders) | Raw MiB/s (runs) | Daemon MiB/s (runs) | Daemon / raw | Worker Python CPU | Python ms per MiB | Children CPU |
|---|---|---|---|---|---|---|
| L1: 1 pair -> local1 (1) | 580 (711, 449) | **792** (966, 617) | 1.36 | 0.32 s | 0.63 | 0.24 s |
| L2: 2 pairs -> local1 (2) | 378 (347, 410) | **399** (427, 371) | 1.05 | 0.64 s | 0.62 | 0.54 s |
| L4: 2 pairs -> local1, local2 (4) | 374 (443, 305) | **387** (439, 335) | 1.03 | 1.35 s | 0.66 | 1.11 s |
| N1: 1 pair -> remote1 (1) | 258 (267, 250) | **69** (67, 71) | 0.27 | 0.84 s | 5.28 | 0.17 s |
| N2: 2 pairs -> remote1 (2) | 207 (193, 221) | **88** (96, 79) | 0.42 | 1.94 s | 6.08 | 0.28 s |
| N4: 2 pairs -> remote1, local1 (4; 3 alive at once in one run) | 440 (413, 467) | **224** (207, 242) | 0.51 | 1.71 s | 2.66 | 0.49 s |

Windows are short (0.5-6 s) and the sender's pools and page cache serve most reads, so single runs vary by +-30 % (raw L1: 711 vs 449); read the means.

**Does the GIL limit the aggregate?**

* **Local legs: no.** The daemon is within noise of raw at 1, 2 and 4 concurrent senders (ratio 1.36 / 1.05 / 1.03). The Python in the worker costs 0.6-0.7 ms per MiB
  (the 1 MiB `_Pump` copy loop), about 0.25 of one core at 390 MiB/s; a single GIL could carry roughly 1,500 MiB/s. The aggregate stops scaling after one sender
  for raw and daemon alike (580 -> 378 -> 374): the 2-vCPU VM and its file-backed pools are the limit (0.9-1.7 of 2 cores busy), not Python.
* **Remote legs: the daemon reaches 27-51 % of a raw `nc` pipe, but the sender's GIL is not what stops it.** The websocket path costs 5.3-6.1 ms of sender Python per
  MiB (`websocket-client` framing and masking), so one sender *process* tops out near 165-190 MiB/s however many threads it has (they share one GIL); observed
  69-88 MiB/s uses 0.35-0.55 of that core. The binding resource here is the **server's single vCPU**: its API worker spends 7.5-9 ms of Python per MiB
  (1.4 s for 160 MiB; 2.4-2.7 s for 320 MiB) and the VM is 0.75-0.99 busy, so 1 -> 2 senders gives only x1.27 (69 -> 88 MiB/s). With two cores on the server the
  next ceiling would be the sender's ~170-190 MiB/s per worker process (and the server API worker's ~110-130 MiB/s per process). Parallel sender threads
  therefore buy overlap and fairness, not remote bandwidth beyond one process's Python budget.
* N4 mixes two local legs (finished in ~0.3 s) with two remote ones; its aggregate is skewed by that, and only 3 of 4 sends were alive at once in one repetition.

---

## Defects and findings

### B1 — a sender that finds a container another sender has just created refuses it as "not recorded as a backup" (ERROR, non-retryable) — real defect

**Where:** `zfsbackup/local.py` `LocalBackupManager._negotiate` -> `settled_ancestors()` -> `_ancestors_state` (the pre-claim look at the container chain) and
`receiver.local_client_id`. **Seen:** scenario 3 in 7 of 18 rounds across four executions (1/3, 1/5, 1/5, 4/5; more often the tighter the alignment).

```
01:10:22,773 DEBUG Executing command: zfs create -u -o canmount=noauto -o org.zfsbackup:client_id=psend1 zptgt/a/psend1      <- sender A creates the container
01:10:22,786 DEBUG Executing command: zfs get -H -o value -s local org.zfsbackup:client_id zptgt/a/psend1                    <- sender B reads its owner, 13 ms later
01:10:22,790 ERROR zpsrc/g1/b38380 -> local1: zptgt/a/psend1 already exists on this host and is not recorded as a backup
     (org.zfsbackup:client_id is not set on it); refusing to adopt it. If it is meant to hold this host's backups, adopt it on purpose:
     zfs set org.zfsbackup:client_id=psend1 zptgt/a/psend1
```

(Seen for `<target>/psend1` and for `<target>/psend1/zpsrc`.)

**Root cause, reproduced without the daemon** (`visprobe.sh`, printed by the scenario): on OpenZFS 2.2.2 a dataset made with `zfs create -u -o org.zfsbackup:client_id=X`
is visible to `zfs list` before `zfs get -s local org.zfsbackup:client_id` can read the property (40 of 40 creates: the first `get` returned nothing and the
second, 10 ms later, returned `X`). Sender B's look at the chain (`exists` true, owner "not set") happens before sender A's `claim` matters: the claim only covers the
containers that were *missing* when B looked, not an existing-but-not-yet-readable one.

**Impact:** the loser's pair gets an ERROR with advice (`zfs set ... client_id`) that is wrong for a race (following it is harmless but pointless), a failed outcome and a
retry at its next slot (6-30 s here); in every round all copies still completed. It is the first-send-into-an-empty-target case, i.e. a new `zfs:` destination with
several datasets, or a new client id.

**Fixed in B4 (2026-11-12, `zfsbackup/local.py` `_recheck_unowned`):** an existing container that shows no local `client_id` is re-checked before any
refusal: claimed by another sender → the retryable `busy` (INFO `is busy: another send is creating ...`, which already worked: 10 hits, all recovered);
otherwise its owner is re-read under a non-blocking claim, and only a re-read that still shows no owner (an operator-made dataset) or another client's id
is refused. The `partial` check needs no such treatment: containers come from `zfs create`, never from a receive, so they never hold a resume token.

**Verified against the fixed working tree (2026-11-12, see "Re-run after B4 fixes"):** 18 rounds of scenario 3, **0** ownership ERRORs (HEAD: 7 in 18 rounds); in
those rounds 108 containers were created, 69 owner reads fell inside the creation race window (within 0.1 s after the `zfs create` of the container started), and the
busy path (INFO `local target ... is busy: another send is creating ...`) was taken **41** times, so about 28 of the window reads were resolved by the re-read under the
claim (the re-read path itself logs nothing, so that figure is a difference, not a count). Every round's 4 copies completed in 6-13 s, where HEAD needed up to 30 s.

### F1 — the 1 GiB `zfsb-server` is OOM-killed by two concurrent large receives (environment limit, observed 4 times)

With two concurrent uncapped receives of 600 MiB (first attempt at scenario 4) or 192 MiB (a throughput round) the kernel OOM killer killed the server daemon's API
worker (`anon-rss` 209-292 MiB; MemAvailable was 270 MiB at idle after the daemon started: five Python processes of ~100 MiB each, `forkserver` on Python 3.14) and the
sender saw `Connection to remote host was lost` / `socket is already closed` followed by `cannot reach ...: Connection refused`. A single 600 MiB stream is fine
(worker RSS +10 MiB). Mitigations used: ARC and dirty-data caps on the server, remote streams of 160-256 MiB, a fresh server daemon before every remote round, a
liveness guard in every wait (a dead server fails the wait in seconds and is reported as a finding). A server with more RAM would remove it; I did not resize or
launch a VM.

### Observations (no action required)

* **First sends into a fresh target queue on the claim:** the loser gets the INFO `is busy ...` and gets a retryable failure, retried after 60 s or at its next slot
  if that is sooner (scenario 3: 6-30 s for 4 tiny datasets with short periods). Pre-creating the owned containers (as scenario 5 does) avoids it.
* **The abandoned `zfs set` is a D-state process that outlives the daemon** until the pool resumes; the next start after `dmsetup resume` was clean.
* **Reload and stop never cut a stream:** a reload waits for the in-flight steps (scenario 2); a stop terminates the worker only after the 30 s budget (scenario 4).
* The status file's `last_attempt` is the time the outcome was recorded, not the start, so overlap cannot be read from it; the sampler and the log are used instead.

---

## Re-run after B4 fixes (working tree, 2026-11-12)

Scenarios 1, 2 and 3 were re-run against the **working tree** with the uncommitted B4 fixes (`SRC_REF=WORKTREE`: `zfsbackup/local.py` `_recheck_unowned`, the scheduler
now deciding when a pair sends, `incomplete: true` status records, the successor's start-up catch-up). Same VMs, same rules; both are left clean (no pools, daemons or
images). The sender received tree `d4ce8e5`, the server `f61d9ab` (two throwaway commits of the working tree made a few seconds apart while others were editing it).

| # | Scenario | Result |
|---|---|---|
| 1 | Overlap and send counts | **pass**, 24 / 24 |
| 2 | Reload during a long send | **pass**, 24 / 24 (twice) |
| 2b | Reload between chain steps, destination frequency 5 min > dataset 30 s | **pass**, 15 / 15 |
| 3 | 18 rounds of concurrent first sends into a missing container tree | **pass**, 182 / 182 (a 3-round repeat with the final harness: 32 / 32) |

**1. Regression check of the forcing change.** At most **4** `zfs send` alive at once (sampler and log), at most **2** local receives and 2 receives on the server, seconds with
>= 2 / 3 / 4 sends alive 22.8 / 11.2 / 4.3; all 12 copies guid-equal; 12 `ok` status records. New: the daemon is watched for 60 s after the sends. Per pair (window 83.7 s,
period 20 s, so at most `83.7 / 20 + 2` = 6 attempts):

```
pair zpsrc/p1 local1  sends=1 nothing=4 attempts=5      pair zpsrc/p1 remote1 sends=1 nothing=3 attempts=4
pair zpsrc/p2 local1  sends=1 nothing=4 attempts=5      pair zpsrc/p2 remote1 sends=1 nothing=4 attempts=5
   ... p3, p4, p6 the same (5 attempts each), p5 remote1 4
```

Every pair sent **exactly once** (12 sends for 12 pairs; each later attempt is a `Nothing to send ... already on the server` DEBUG line, one per 20 s slot), none attempted more than 5 times;
nothing re-sends and nothing runs more often than its period.

**2. Reload mid-send (unchanged expectations).** Three incremental sends in flight at the reload; every in-flight step completed before the worker was replaced
(pid 520320 -> 523738); no torn stream, no `Resumed`, no `Sending full`; exactly 12 incremental steps sent and 12 acknowledged; `applied=` = stored generation; copies equal.

**2b. Destination frequency longer than the dataset's.** Dataset frequency 30 s (the snapshot worker makes a snapshot every 30 s while 24 MiB are appended every 15 s), destination
frequency 5 min, so each pair's chain starts at its 5-minute slot. The reload was issued the moment three incremental sends were in flight (`T_TRIGGER`; steps
`..011500 -> ..011530` on both destinations):

```
02:45:2x  slot: 3 incremental sends in flight, `zfsbackup-config set prune_interval 90s` (generation 7), RC=0
          statuswatch (50 ms poll of remote-backup-worker.json):  incomplete=True  for all four pairs, written by the OLD worker (pid 620134), 1.3 s after the request
02:45:33  Reloaded remote worker (pid=630804) on generation 7
02:45:57  successor's first step (24 s later: its catch-up delay frac x 30 s plus start-up) ... the chain runs back to back
          both destinations hold every snapshot up to the cut point 41 s after the replacement was seen (the next regular slot is 300 s after the first)
```

The status file showed `incomplete: true` for all four pairs, written by the old worker, before the replacement; the successor flipped them to `incomplete=False` as it finished
(`statuswatch` lines at pid 630804); no step was sent twice (same transport, dataset, from -> to), no `Sending full` after the baseline, no `receive_resume_token` once the sends
were idle, no ERROR on either VM. A first attempt of this variant (harness defects only: it required the never-sent baseline snapshot on the copies and read resume tokens while the next
slot's chain was already running) showed the same behaviour: successor first step 24 s after `Reloaded`.

**3. B1 re-verified.** 18 rounds, 4 start-aligned datasets each (siblings and cousins, six fresh containers per round): **0** ownership ERRORs; every round's 4 copies complete
(6-13 s) with every pair's last status record `ok/0`, no resume token, no ERROR or Traceback. Busy path: 41 INFO `is busy: another send is creating <container>` over the 18 rounds
(2-3 per round); reads inside the creation window: 69 of 108 creates (median 50 ms after the create started, max 99 ms), which is exactly the window the bug lived in
(HEAD: 4 of 5 rounds logged the ERROR in the last run). The 3-round repeat: 7 busy hits, 12 window reads, 0 ERRORs.

**New defects:** none. **Harness:** scenario 3 now fails a round on any `not recorded as a backup` line (`R3_ROUNDS` default 18); scenario 1 checks per-pair send and attempt counts;
scenario 2b is new. The re-read counter first written for scenario 3 (container owners read twice within 0.1 s) was dropped as unreliable (8 hits per round even without contention); the
window-read count above replaces it.

## What was not covered

* The pump's 600 s no-byte stall bound (scenario 4 is decided at ~60 s by the timed negotiate call) and a hung *source* pool.
* More than 2 vCPUs on either side, and a server that is not CPU-bound: the remote ceiling per process (~165-190 MiB/s sender, ~110-130 MiB/s server) is derived
  from CPU seconds per MiB, not measured at saturation.
* `zfsb-client` (unused: the sender role was given to `zfsb-local`, the only VM a pool may be hung on).

## Files

* New: `scenarios/test_parallel_senders.sh` (about 1,400 lines, `bash -n` clean; `--only-N`, `--no-N`, `--keep`, `--keep-pools`, `--reuse`).
* Changed: `scenarios/README.md` (new table row, caveat list, shipped-tree list).
* Report: this file. No change to `zfsbackup/`, `libs/`, `tests/`, `CLAUDE.md`; nothing committed.
* Raw material of the full run (logs, samples, analyses, `throughput.tsv`) is written to `PS_LOG_DIR` (default a `mktemp` directory).

## How to run

```
./scenarios/test_parallel_senders.sh                 # everything (about 55 min with the VMs provisioned), VMs untouched, pools and daemons removed
./scenarios/test_parallel_senders.sh --only-4        # one scenario
./scenarios/test_parallel_senders.sh --keep          # then --reuse for a quick second scenario
SRC_REF=WORKTREE ./scenarios/test_parallel_senders.sh   # ship the working tree instead of HEAD
```

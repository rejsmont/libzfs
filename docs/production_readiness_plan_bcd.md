# Production readiness: milestones B, C and D (proposal)

Status: **proposal, awaiting approval** (drafted 2026-10-22). Nothing here is implemented.

This continues [production_readiness_plan.md](production_readiness_plan.md). That file holds the item
definitions (B-4, S-1, …), the decisions already taken (Q1–Q15), and milestone A. This file merges five planner
reports into one ordered plan, settles where they disagreed, and lists the decisions still open. The detailed
reports are kept as appendices: they give the file:line basis, edge cases and test tables for each item.

The plans are written against branch `prod-trust-boundary` with milestone A (trust boundary) complete but
uncommitted. Line numbers in the appendices refer to that tree.

---

## 1. Decisions

Each decision has a recommendation. **"Accept all"** takes every recommendation. Otherwise answer by number. The
ones that cannot be changed after the first deployment are marked **(irreversible)**.

### Across milestones

| # | Decision | Recommendation |
|---|---|---|
| X1 | Branching and the wire protocol | Commit milestone A and open its PR first. Milestone B goes on a branch stacked on it (`prod-send-correctness`). **No release until B has landed**, so B's additions (`server_copy` in negotiate) stay inside protocol v1 and `PROTOCOL_VERSION` stays 1. |
| X2 | Pull two libzfseasy items forward from D into B | Yes. (a) Run every `zfs` child under `LC_ALL=C` (lib S1a): `exists()` already matches English text and breaks on a non-English host, and B-4 and the format captures should see the locale the library will use. (b) `destroy.snapshots([])` raises `ValueError` instead of `AttributeError`, before B-4's one-at-a-time retry. |
| X3 | Pull D-0 forward into C | Yes. `validate_config` refuses a configured dataset equal to or below `remote_backup.target_dataset`, or a `recursive` one above it. Such a config today snapshots and prunes inside received datasets, and it is the forging path milestone A's review found. Owner: `zfsbackup-store-developer` (`validate_config` lives in `config/store/mapper.py:897`). |
| X4 | Where B-2a (received datasets `canmount=noauto`, `readonly=on`, `volmode=none`, no holds) goes | Keep it in D. The restore runbook (B-9, milestone C) is written to be correct before and after it: operators never mount or modify a received dataset and restore only from clones and sends. |
| X5 | Encrypted server target pools | Support them. A plain full stream with `-F` onto a pre-created dataset under an encrypted target is expected to fail (to be confirmed by capture Z1), so item R0 (the receive creates the leaf dataset) is needed even without raw sends. |
| X6 | Timeout API (libzfseasy S2b, used by zfsbackup S-14) | A context manager, `with zfs.timeout(30):`, held in a contextvar and applied per command to non-streaming commands only. S-14 wraps each API handler body in it and maps `ZFSTimeoutError` to 503 `{"retry": true}`. A per-call `timeout=` keyword can be added later without breaking anything. |
| X7 | Commit the OpenZFS format captures as test fixtures | Yes: `tests/fixtures/openzfs/{2.1.5,2.2.2,2.4.1}/`. The original T2 captures were lost with a crashed session's temp folder. Without committed fixtures, the next "identical across versions" claim has nothing to check against. |
| X8 | Ownership of new files | `zfsbackup-developer`: `zfsbackup/protocol.py` (new in A), `version.py`, `logsetup.py`, `health.py`, `health_collect.py`, `daemon_entry.py`, `packaging/**`, `pyproject.toml`, `poetry.lock`. `pytest-test-author`: `.github/workflows/**` (Q14). Recorded in `.claude/agents/README.md`. |

### Milestone B: snapshot and send correctness

| # | Decision | Recommendation |
|---|---|---|
| B1 | The frozen send-flag set **(irreversible per server dataset)** | Plain mode: `-L -c`. Raw mode: `-w` (libzfs adds `-L -e -c` itself). No `-e` in plain mode: it saves almost nothing, needs `embedded_data` on the server pool, and its token flag depends on pool state. Resumed sends: `zfs send -t <token>` with no flags, because the token carries its own. Freeze only after captures M1 and M6 confirm this on 2.1, 2.2 and 2.4. |
| B2 | Resume-token check | Per mode, a required and an allowed set of token flags. Plain: required none, allowed `{compressok, largeblockok}`. Raw: required `{rawok}`, allowed `{rawok, compressok, largeblockok}`. A token is raw if and only if it holds `rawok`, and that must match the dataset's current mode. `embedok` and `savedok` are never accepted. This replaces today's equality check, which would refuse valid tokens once `-L` is added: `largeblockok` is only recorded when the dataset's `large_blocks` feature is active. |
| B3 | The `raw` setting **(irreversible per server dataset)** | `raw: auto` (the default), `true` or `false` on each dataset's remote entry (`RemoteDatasetConfig`, the `dataset_remotes` table). `auto`: raw if and only if the dataset is encrypted. `true`: raw required, an unencrypted dataset is refused. `false`: never raw, needs the key loaded. Not sent on the wire. |
| B4 | Switching an existing server copy between raw and plain | The client refuses, logs one ERROR naming the fix (rename the server copy aside; the next cycle sends a full stream) and sends nothing. It never destroys or renames server data itself. |
| B5 | Where the canonical config lives before the server dataset exists (R0) | Negotiate creates only the parent and keeps the canonical config in a lock-guarded in-memory map in the API worker (on the dataset itself once it exists). The stream's `-o` values come from the local property, else from that map, else the stream is refused (milestone A's rule). No wire change. Alternative: send `config` on `/stream` (a protocol change). |
| B6 | The first full send | The newest snapshot only, as today. History before the first backup is not backfilled. |
| B7 | A chain member is deleted between steps | Re-list, rebuild the rest of the chain from the current base, and continue in the same cycle (one rebuild per missing member). Any other failure stops the chain until the next cycle. That snapshot is never replicated. |
| B8 | Held and cloned snapshots in a prune batch | Skip them up front from `userrefs`/`clones` (also in dry run, because a cloned snapshot fails even `destroy -n`), and fall back to one snapshot at a time when a batch fails. No deferred destroy (`-d`). No structured `DestroyError` in libzfseasy. |
| B9 | Retention order after S-1 | Unchanged: slots and ages use name timestamps, and the oldest snapshot per slot is kept. Only "always keep the newest" and every newest/older/newer comparison switch to `createtxg`. |
| B10 | Filter the chain by the destination's retention | No (deferred). Every managed snapshot is sent; the server prunes. |
| B11 | Tolerance for future-dated names | `max(5 min, check_interval)` for the newest snapshot and for anchors. Beyond it: ERROR once ("system clock is behind the newest snapshot; check NTP") and treat the dataset as due. |

### Milestone C: install, operate, restore

| # | Decision | Recommendation |
|---|---|---|
| C1 | Packaging format | Wheel and sdist. Units live in `packaging/`, ship in the sdist only, never in the wheel. A `.deb` is deferred. |
| C2 | Distribution name | Rename the dist from `libzfseasy` to `zfsbackup`, which still ships `libzfseasy` inside it. One wheel per host; `importlib.metadata.version("zfsbackup")` is the single version source. |
| C3 | Supported Python | 3.11–3.14, plus 3.15 in CI (declared supported once green). No upper cap. Ubuntu 22.04's `python3` is 3.10, so the runbook uses a uv-managed or deadsnakes 3.11+ there. |
| C4 | Prometheus output | `check --prometheus` to stdout, or to a file with `--output FILE` (written atomically, for node_exporter's textfile collector). An optional, disabled `zfsbackup-check.timer`. No HTTP `/metrics`. |
| C5 | Daemon command name | `zfsbackupd`. |
| C6 | Pre-migration backup (Q11) | `migrate --backup-dir DIR` writes a backup only when the schema is actually behind, inside the writer lock. A backup on every start would fill the disk under restart loops. |
| C7 | Keys in backups | `zfsbackup-config backup DIR` includes the `<db>.tls` keys by default; `--no-keys` leaves them out (a group operator cannot read them and is refused without it). Restore is a documented procedure, not a command. |
| C8 | Service account | sysusers creates the `zfsbackup` group only; the daemon stays root. tmpfiles creates `/var/lib/zfsbackup` as `2770 root:zfsbackup`. The store's advice messages name the group only. |
| C9 | Health thresholds | WARN at 2× and CRIT at 4× the expected interval: the dataset's `frequency` for local snapshots; `max(dataset, destination frequency)` for the remote anchor; on the server, the largest of the client's frequencies. A destination not sent yet is WARN until its oldest snapshot passes the CRIT threshold. |
| C10 | Real-pool tests in CI | First try hosted `ubuntu-24.04` with `zfsutils-linux`. If that fails, a self-hosted runner on `zfsb-ci` (which has OpenZFS 2.2), triggered only by schedule and manual dispatch on `master`, **never** by pull requests (it would run PR code). |
| C11 | Supported systemd | 249 and later. Below 253 a shipped drop-in switches `Type=notify-reload` to `Type=notify` with an `ExecReload`. |
| C12 | Upper version caps | `werkzeug >=3.1,<3.2`, `urllib3 >=2,<3`, `requests >=2.31,<3`, `sqlalchemy >=2.0,<2.2`. `cryptography` uncapped, covered by a weekly latest-dependencies job. |

### Milestone D: hardening for 1.0

| # | Decision | Recommendation |
|---|---|---|
| D1 | B-2 (receive hardening) under Q1 | Do B-2a (no mounting, no holds, no zvol devices for received data) and B-2c (stop adopting an unowned dataset whose name matches a client id) at 1.0, because they guard against accidents too. B-2b (`-F` only onto a dataset with no snapshots) as a separately approvable item; if it fails on any OpenZFS version, document it instead. The `-x` list must never name the three properties milestone A passes as `-o`. |
| D2 | B-3 (server retention floor) | Defer the full floor past 1.0 and document it. Ship B-3-lite: negotiate logs a WARNING when a client's new config shortens its retention horizon. |
| D3 | Scenario L2 (hostile streams) | With B-2, in D, reusing L1's modified-client harness. |
| D4 | Which sessions S-2 cuts mid-stream | Cut on `revoked`, `successor_revoked`, `client_revoked` and `replaced`. Do not cut on `retired` (a rotation) or on certificate expiry mid-stream. Stalled writers are cut after `WS_IDLE_TIMEOUT`. |
| D5 | S-3 flood-control limits | Constants, no config: per-IP token bucket (burst 60, 10/s, LRU of 4096 IPs); `/enroll` 5 failures per 15 min gives 429 with `Retry-After: 900`; at most 2 `/enroll` in flight; refusal logs one WARNING per IP per hour plus an hourly count. |
| D6 | S-8 version-skew guard | Compare the installed version plus the dist-info `RECORD` file's identity. On a change: a cooperative stop, then exit code 76, which the unit lists in `RestartForceExitStatus=`. No `execv`. |
| D7 | S-14 scope | Only the API handlers' non-streaming `zfs` calls get a timeout (30 s, giving 503 with `retry`). Workers get the watchdog only. Running transfers are never cut (Q7). |
| D8 | S-7 status files | `<db>.status/<worker>.json`, written atomically by each worker; nothing under `--dry-run`. `check` (milestone C) reads them once they exist, and their absence is "no data", never CRIT. |
| D9 | libzfseasy exec core | Rebuild `_exec` and `_exec_out` on `_exec_lines` (bytes, surrogateescape, bounded stderr, child reaped). Two behaviour changes: an interrupt during a write command now terminates the child (TERM, then KILL after 5 s), and `\r` is no longer a line break in `_exec_out`. |
| D10 | Calls refused while a timed-out child is still alive | Only calls made under a timeout, and they fail fast. Calls without a timeout behave as today. |
| D11 | New exception types | `ZFSTimeoutError` and `ZFSOutputError`, both plain `Exception` subclasses (not `ValueError`, `OSError` or `TimeoutError`). |
| D12 | `zfs.get.rows()` result (lib S3, for zfsbackup B-1d) | A list of `GetRow(name, property, value, received, source)` with `.complete`, `.dropped` and `.skipped`; `strict=True` raises instead. `verify_names=True` cross-checks every name against a name-only `zfs list`, which cannot be forged, closing the forged-whole-object gap. Safety decisions use `.complete`. Optional exact-sequence check (`name, type, p1, p2, …`), gated on a real-tier check that zfs prints every requested property for every object. |
| D13 | `StreamHandle.close(timeout)` / `abort()` | Defer; nothing would use them under Q7. |
| D14 | `load_key` / `unload_key` | Fix the docstrings and keep `ds` required. A default of `None` would turn a bare call into `-a` (every dataset). |
| D15 | `zfs list -j` / `zfs get -j` (OpenZFS ≥ 2.3) | Probe once with `zfs version -j`, cached per binary path; a module setting `zfs.json_output = 'auto' | True | False`; fall back to `-H` with one WARNING on an unknown `output_version`. |

---

## 2. Where the plans disagreed, and how this merge settles it

- **Timeouts:** the zfsbackup plan assumed `timeout=30` per call; the libzfseasy plan proposed a context manager. Settled by X6 (context manager).
- **D-0's owner:** one plan named `config/model.py`, the other `store/mapper.py`. `validate_config` is in `store/mapper.py:897`, so the store developer owns it (X3).
- **`zfsb-ci`:** the milestone B plan says it has no `zfs`. It does: Ubuntu 24.04.5 with OpenZFS 2.2, checked 2026-10-22. B-4's real tests run on `zfsb-zfs21/22/24` anyway, for the version spread.
- **The locale:** the milestone B plan keys B-4 on `userrefs`/`clones` rather than stderr because stderr is not yet in the C locale. With X2, stderr will be in the C locale too; B-4 still prefers the listing (a hold and a send in progress print the same "busy").
- **The `-x`/`-o` overlap:** zfs refuses the same property in both (usage error on all three versions). libzfseasy refuses it before running (lib L1), and milestone D's B-2 keeps the three ownership properties out of its `-x` list.
- **CLAUDE.md:** it says the pruner "keeps the newest snapshot per time slot". It keeps the oldest per slot (see B9). Corrected in milestone A's commit.
- **Carried into milestone B from milestone A's re-review (low-risk):**
  - rename `api.RECEIVE_EXCLUDED_PROPERTIES` (now passed with `-o`, not `-x`) to `SERVER_OWNED_PROPERTIES`, with R0;
  - the server pruner applies the positive-duration check itself (`_duration_ok` and the shape check move to `config/model.py`, and `_received_dataset_info` calls them), with S1-a;
  - the `conftest.py` leak guard also snapshots `Command` subclass dicts and `libzfseasy.zfs` globals, and becomes a new-style `wrapper=True` hook that reports a leak even when teardown already failed (`pytest-test-author`);
  - `.claude/agents/zfs-code-reviewer.md` still says "newest-per-slot" (main session, with the user's agreement, since it is an agent definition).

---

## 3. Milestone B: work items in order

Lanes run in parallel; each lane is serial. Every item is needs-approval unless marked low-risk. "CR" means the
concurrency reviewer also reviews the item.

1. **B0. Commit milestone A, open its PR, branch from it** (main session, low-risk).
2. **SP. OpenZFS capture spike** (`real-zfs-scenario-dev`, low-risk). It gates B5-a, R0 and S6-d/e. On `zfsb-zfs21/22/24`, extending `scenarios/test_openzfs_matrix.sh`:
   - M1: token flags per send-flag set, at `recordsize` 128K and 1M, with lz4 and with compression off, full and incremental;
   - M2: stale tokens;
   - M3 / Z8: destroy failures (held, cloned, busy; batch and single; recursive child hold);
   - M4: the listing with `createtxg`, `guid`, `userrefs` and `clones`;
   - M5 / Z1: raw receive, a full raw with `-F` onto a pre-created unencrypted dataset, and a plain full onto a pre-created dataset under an encrypted target;
   - M6: `-L` mismatch in both directions;
   - Z2: `-o` ownership properties on raw full, incremental and resumed receives;
   - Z3: a partial receive of a new dataset with `-s`;
   - Z4, Z5: sends with the key unloaded, and after `change-key`;
   - Z6, Z7: the `server_copy` predicate, and pruning a raw copy with no key.
   Captures become fixtures (X7) through `pytest-test-author` (M8).
3. **libzfseasy lane** (`libzfseasy-developer`):
   - S1a: `LC_ALL=C` for every child (X2; CR light);
   - L2: `destroy.snapshots([])` raises `ValueError`;
   - L1: refuse a name in both `props` and `reset`;
   - L3: docstrings (lazy destroy on failure, token flags are a subset);
   - L5 (optional): `send.resume` applies the token check;
   - L4: fix whatever the captures show to differ between versions (needs-approval when it arises).
4. **zfsbackup core lane** (`zfsbackup-developer`):
   1. S6-a: model field `raw`;
   2. S1-a: one snapshot listing carrying `createtxg`, `guid`, `userrefs`, `clones`, `encryption` and `keystatus`; newest by `createtxg`;
   3. S1-b: strict name parse (Q13);
   4. B4-a: per-snapshot fallback with a stuck-snapshot memory;
   5. B4-b: pre-skip held and cloned snapshots;
   6. S1-c: the future-dated rule and the backwards-clock collision;
   7. S1-d: client comparisons on `createtxg`;
   8. R1: release the server's per-dataset claim before the final ack (CR);
   9. after M1/M6 and lib L4: B5-a, the frozen flags plus the per-mode token check (shipped together);
   10. B5-b: the `-i` chain (CR);
   11. after M5 and Z1–Z7: R0, the receive creates the leaf (CR);
   12. S6-d: negotiate reports `server_copy`;
   13. S6-e: client mode selection.
5. **Store lane** (`zfsbackup-store-developer`): S6-b, the `dataset_remotes.raw` column and Alembic revision 3.
6. **CLI lane** (`zfsbackup-cli-developer`): S6-c, `render`, dot paths and `list` for `raw`.
7. **Tests** (`pytest-test-author`): tables T-S1, T-B4, T-B5 and T-S6 (Appendix B1), the R1–R6 real tests (Appendix B2), mocked and `real_zfs` on all three versions.
8. **Scenarios** (`real-zfs-scenario-dev`):
   - scenario B extended (every managed snapshot reaches the server with a matching GUID);
   - J rerun with the new flags and a resume mid-chain;
   - new scenario M (encrypted datasets and raw sends);
   - L1 rerun.
9. **Review:** `zfs-code-reviewer` on everything; `concurrency-reviewer` on S1a, R0, R1 and B5-b.

## 4. Milestone C: work items in order

1. **Wave 1** (no milestone B dependency):
   - C1: one version source, dependency pins, the dist rename (core);
   - C8 / C9: online DB backup and the advice wording (store);
   - C19 / C21-spike: contract tests and the CI real-tier spike (tests);
   - C24-ownership rows (main session).
2. **Wave 2:**
   - C0 (= D-0, X3, store);
   - C2: daemon entry point that ignores SIGHUP before any import, plus `--version` (CR);
   - C3: journald priorities (CR);
   - C4: TLS-directory copy under `identity.lock` (CR);
   - C10: CLI `--version`;
   - C11: `zfsbackup-config backup`;
   - C20: the CI package job.
3. **Wave 3:**
   - C12: `migrate --backup-dir` (CR);
   - C5: `health.py` (stdlib only) and `health_collect.py`, **after milestone B's S-1**;
   - C13: `zfsbackup-config check`;
   - C6: the shipped units;
   - C14 (optional): journald priority for CLI errors.
4. **Wave 4:**
   - C22: install scenario on fresh `zfsb-inst22`/`zfsb-inst24`, including an upgrade across a schema revision;
   - C7: the `zfsbackup/README.md` pass (install, upgrade runbook, server rules from milestone A, restore, logging, known limitations);
   - C23: two-VM scenario R (restore), after B-5 (and S-6);
   - C24: release checklist and report rewrites.

**Carried into C7's restore runbook (from the C8 concurrency review):** restoring a config-DB backup brings back
consumed enrollment tokens and revoked or retired fingerprints, which breaks "revocation is permanent": after a
restore, re-apply every revocation made since the backup (`trust list` on the live DB first, then `trust revoke`),
or restore only on a host whose trust state has not changed. API-worker trust writes (`/enroll`, `/rotate`) can
also land between a pre-migration backup and the upgrade. `zfsbackup-config backup` is a writer-or-root
operation.

Appendix C1 has the full items; Appendix C2 has the changes made after the milestone D input (restore that
holds before and after B-2a, the S-8 contract, B-6 first without status files, C0).

## 5. Milestone D: work items in order

1. **libzfseasy lane:**
   1. D1 (S5 small fixes, low-risk);
   2. D3 (one exec core, CR);
   3. D4 (name-limit counts, low-risk);
   4. D5 (`zfs.get.rows`, low-risk);
   5. D6 (slow-command watchdog, CR);
   6. D7 (opt-in timeouts, CR);
   7. D8 (`-j` JSON, after a capture on 2.4).
   D2 (S1a) has moved to milestone B (X2).
2. **zfsbackup core lane:**
   1. K-5 and S-12 (low-risk);
   2. B-2a, B-2c, B-2b and B-3-lite;
   3. B-1d, after lib D5;
   4. S-2 (CR);
   5. S-3 (CR);
   6. S-7a (CR);
   7. S-8, after C1 and C6 (CR);
   8. S-14, after lib D7 (CR).
3. **Store lane:** D-0 has moved to milestone C (X3).
4. **CLI lane:** S-7b (`status`/`check` read the status files), after S-7a.
5. **Scenarios:**
   - L2 (hostile streams);
   - the K5 rewrite (revocation cuts a running stream);
   - the S-8 upgrade-under-a-running-daemon step;
   - the D7 hung-pool test on a disposable `zfsb-hang` VM;
   - L1's stale "receive -x" wording.
6. **Carried in from milestone B's final review (2026-10-22):**
   - **RESOLVED in milestone C (2026-10-22): B-2b was pulled forward** after scenario R2c showed the data loss;
     the backstop stays recoverable through the client's `discard` permit (CLAUDE.md, remote paragraph).
     Original note: **B-2b must keep the chain recoverable.** The milestone-B anchor backstop puts the anchor back on the base
     when a just-sent snapshot vanished on the client, while the server keeps it; the next `-i base …` works only
     because `-F` rolls that server-only snapshot back. With "`-F` only onto a dataset with no snapshots" that
     becomes a permanent "does not match incremental source" stall, so B-2b needs a rule for it (e.g. allow `-F`
     when the server's newest snapshot is newer than the incremental's base and was never anchored by the client).
     A full send when no common snapshot exists still passes `-F` onto an existing server copy, but zfs refuses it
     ("destination has snapshots … must destroy them to overwrite it", checked on OpenZFS 2.4.1, 2026-10-22), so
     history is never wiped; the client is refused each cycle until the operator renames the copy aside.
   - **S-3: the R0 pending map is evicted globally** (LRU, 4096 entries): an enrolled client negotiating thousands
     of names can evict other clients' entries (their next stream is refused with "negotiate again", retryable).
     Out of scope under Q1, but cap it per client id when S-3 adds per-client limits.
   - **S-14 inputs from the D6/D7 concurrency review (2026-10-25):** the stuck-child registry is per process, so
     one hung pool makes every timed call in that API worker fail fast (503 retry) until its stuck child exits,
     healthy pools included (DD4 as designed); a respawned API worker starts with an empty registry, so each
     incarnation can add up to its concurrency in new D-state `zfs` processes; the budget is per command, so a
     handler making N calls can take N × (30 + 4) s: give each handler one overall budget if that matters.
     `send.resume_info` and `StreamHandle.close` are not bounded by `timeout()`.
   - **S-3: fold in S-12's validity WARNING** (from the S-12 review, 2026-10-25): `_ValidityWarnThrottle` bounds
     memory (256 peers) but not log volume; a holder of a former client's expired pinned certificate (its key is
     not needed: the certificate is checked before the possession proof) cycling through source addresses logs one
     WARNING per connection. Expired pins stay in the trust store. Give it S-3's global per-interval cap.
   - **Deferred from the B-2b reviews (2026-10-22):** (a) `-F` is decided from a listing but applied by the kernel at
     the end of the receive, so a snapshot that appears on the server copy mid-receive (an operator's `zfs snapshot
     -r`, or an orphaned `zfs receive` of a previous API worker committing late) is destroyed unpermitted; fix by
     passing `-F` only when needed (the permitted discard, or a head modified since the base) and verify a resume
     without `-F` first (M7 U2 shows it fails when newer snapshots exist); README documents the residual. (b) Check
     the stream's DRR_BEGIN `drr_fromguid` against the declared `from_guid` before spawning `zfs receive`. (c) An
     exiting API worker should reap its `zfs receive` children before it exits.
   - **A child backed up before its parent** is handled by the remote worker sending parents first (milestone B);
     a parent added to the config after its child has been backed up still meets the placeholder refusal, which
     needs the operator's rename (documented in the runbook, milestone C's B-9).
   - `.claude/agents/zfs-code-reviewer.md` still says "newest-per-slot" (the pruner keeps the oldest per slot):
     an agent definition, left for the user to approve.

---

## Appendices: the planner reports

These are the five planner reports as delivered, plus the milestone C addendum. Their decision numbers are
local to each report; section 1 above renumbers them.

- [B1. zfsbackup, milestone B](#appendix-b1-zfsbackup-milestone-b)
- [B2. libzfseasy, milestone B (B4)](#appendix-b2-libzfseasy-milestone-b-b4)
- [C1. Milestone C](#appendix-c1-milestone-c)
- [C2. Milestone C addendum](#appendix-c2-milestone-c-addendum)
- [D1. zfsbackup, milestone D](#appendix-d1-zfsbackup-milestone-d)
- [D2. libzfseasy, milestone D](#appendix-d2-libzfseasy-milestone-d)

---

## Appendix B1. zfsbackup, milestone B

### Milestone B plan (snapshot and send correctness): proposal, nothing implemented

**Base.** This plan is written against the current working tree of `prod-trust-boundary`, with milestone A uncommitted. I made no edits.

**Branching.** Stacking milestone B on milestone A's PR is fine, with two conditions:
- Commit A first, once the test author is done, so that B has a clean base.
- Item **R0** reworks A's newly verified negotiate/receive flow (`api.py:1943-1952`, `:2155-2169`). If A's review changes those same lines, the two branches will conflict. Land R0 as B's first api.py commit, so it can be reviewed on its own.

**Input from the libzfseasy B4 planner.** I agree with points 1, 2, 3, 5 and 6. I reached the same conclusions independently from the code (details under each item). Where I differ or add:
- **(1)** "rawok only for `-w` on an encrypted dataset" is exactly why I propose that `raw: true` refuses an unencrypted dataset (D-B3). Otherwise a `-w` send of an unencrypted dataset would produce a token without `rawok`, which is a third case the check would have to handle.
- **(3)** The same libzfs check, as I read it (`(raw && !encrypted) || encrypted` with `-F` on a new-fs stream), has a wider consequence. A plain full stream with `-F` onto a pre-created dataset under an **encrypted server target** should also fail. If so, milestone A's flow already breaks on encrypted backup pools, raw or not. Matrix capture Z1 must cover this. R0 below is the reconciliation.
- **(4)** This constrains milestone D's B-2: its `reset` (`-x`) list must never name the three properties that A now passes as `-o`.
- **(5)** A cloned snapshot fails even a dry run, so the B-4 pre-skip must also apply in dry run.

---

#### Decisions (each has a recommendation; "accept the recommendation" is fine)

| # | Decision | Recommendation |
|---|---|---|
| D-B1 | The frozen send-flag set | **Plain mode:** `-L -c`. **Raw mode:** `-w`, which implies `-L -e -c` in libzfs. **No `-e` in plain mode:** it saves almost nothing, needs `embedded_data` on the server pool, and its token flag depends on pool state. **Resumed sends:** `zfs send -t <token>` with no flags (`zfs.send.resume(token)` unchanged), because the token carries its own flags. Freeze this only after captures M1 and M6. |
| D-B2 | The resume-token check (replaces `info.flags != _EXPECTED_RESUME_FLAGS`, `remote.py:319`, `:1254`) | Each mode has a required set and an allowed set. A token is accepted only if every required flag is present and all its flags are within the allowed set. **Plain:** required ∅; allowed `{compressok, largeblockok}`. **Raw:** required `{rawok}`; allowed `{rawok, compressok, largeblockok}`. The token's mode is "raw" if and only if it contains `rawok`, and that must equal the dataset's current mode. `embedok` and `savedok` are never accepted. Widen a set only if M1 shows an extra flag on 2.1, 2.2 or 2.4. The empty set (tokens from today's flagless sends) stays acceptable in plain mode. |
| D-B3 | What the `raw` values mean, and where the setting lives | It goes on `RemoteDatasetConfig` (the `dataset_remotes` table, one per dataset × destination), stored as `Optional[bool]`. **`auto`** (None, the default): raw if and only if the dataset is encrypted. **`true`**: raw is required, and an unencrypted dataset is refused (ERROR once). **`false`**: never raw, which needs the key loaded; otherwise skip with ERROR once. No destination-level default. Not put on the wire: `to_property` stays unchanged. |
| D-B4 | A switch between raw and plain on an existing server copy | Negotiate reports `server_copy: none\|plain\|raw` (S6-d). On a mismatch the client refuses: ERROR once, no send, and the message names the fix ("on the server, `zfs rename <ds> <ds>.plain-<date>`; the next cycle sends a full raw stream"). The client never destroys or renames server data automatically. |
| D-B5 | Where the canonical config lives before the server dataset exists (R0) | Negotiate creates **only the parent**. It records the canonical config in an in-memory, lock-guarded `pending_configs[server_dataset]` in the API worker, and on the dataset itself when the dataset exists. The stream's `-o` values come from the local property if present, otherwise from the pending entry; with neither, the stream is refused (A's rule is kept). This needs no wire change: the client always negotiates right before it streams. Alternative: send `config` on `/stream`, which changes the protocol. |
| D-B6 | Protocol version | Keep `PROTOCOL_VERSION = 1` and define v1 to include `server_copy`, provided A and B ship together (nothing is deployed). Bump to 2 only if A is released alone. |
| D-B7 | The first full send | Send the newest snapshot only, as today. History from before the first backup is not backfilled. |
| D-B8 | A chain member is deleted between steps | Re-list, rebuild the rest of the chain from the current base, and continue in the same cycle; one rebuild per missing member. Any other failure stops the chain until the next cycle. |
| D-B9 | B-4 pre-skip, and `destroy -d` | Pre-skip **yes**: `userrefs` and `clones` come from the listing S-1 already extends. Deferred destroy (`-d`) **no**: a snapshot would disappear later, when the user releases the hold. |
| D-B10 | Retention order after S-1 | Slots and ages stay on name timestamps, and `needs_prunning` still walks oldest-name-first, so the **oldest** snapshot per slot is kept, exactly as today. Only "always keep the newest" switches to `createtxg`. (CLAUDE.md wrongly says "keeps the newest snapshot per time slot"; it is stale.) |
| D-B11 | Should the chain be filtered by the destination's retention? | No, deferred. Send every managed snapshot; the server prunes. |
| D-B12 | Tolerance for future-dated names | `max(5m, check_interval)`, the same for the newest snapshot and for anchors. |

---

#### Ordered work items

##### Phase 0 (now)

**B0. Commit milestone A, open its PR, branch `prod-send-correctness` from it.** Owner: main session. Tier: low-risk.

**SP. OpenZFS matrix captures (spike).** Owner: `real-zfs-scenario-dev`. Tier: low-risk (no product code), but it gates B5-a, R0 and S6-d/e.
- **Where:** extend `scenarios/test_openzfs_matrix.sh` on zfsb-zfs21, zfsb-zfs22 and zfsb-zfs24.
- **The lib planner's captures:** M1 (token flags per flag set: recordsize 128K vs 1M, lz4 vs off, full and incremental), M5 (raw full with `-F` onto a pre-created unencrypted dataset) and M6 (`-L` mismatch in both directions). Plus these zfsbackup captures:
  - **Z1:** a plain full with `-F` onto a pre-created dataset under an **encrypted** target; a raw full into a non-existent leaf under an unencrypted parent and under an encrypted one.
  - **Z2:** `-o org.zfsbackup:{config,client_id,source_dataset}=…` on a raw full, a raw incremental, a resumed raw full and a resumed raw incremental. Also record whether B-2's later `-o readonly/canmount/volmode` are accepted on raw.
  - **Z3:** a partial **new-dataset** receive with `-s`. Does the dataset exist, does `zfs list` see it, which dataset holds the token, are the `-o` values present while it is partial, and what does `receive -A` leave behind?
  - **Z4:** `zfs snapshot` and `zfs send -w` with the key unloaded; a plain send with the key unloaded (the error text).
  - **Z5:** a raw incremental after `zfs change-key`, and after `change-key -i`.
  - **Z6:** the `server_copy` predicate. Print `encryption` and `encryptionroot` for a raw-received dataset, a raw child under a raw parent, plain under an encrypted target, and plain under an unencrypted target.
  - **Z7:** on a raw-received dataset with no key, server-side `zfs get -s local org.zfsbackup:*` and `zfs destroy` of a snapshot.
  - **Z8:** `zfs destroy` stderr for a held snapshot, a cloned one, one being sent, and a recursive destroy with a child hold. Confirm that a failed batch destroys nothing.
- **libzfseasy B4 coordination:** if `send -nv -t` prints the `*ok` keys differently on 2.1 or 2.2 (stdout vs stderr, naming), the parser fix goes through `libzfseasy-implementation-planner` → `libzfseasy-developer` (`_parse_resume_contents`, `zfs.py:~1690-1711`). It is not planned here. B5-a depends on it.

##### Phase 1 (start now, in parallel with SP)

`zfsbackup-developer` runs this lane serially. **S6-a goes first** so the store and CLI lanes can start.

**S6-a. Model field `raw`.** Owner: `zfsbackup-developer`. Tier: needs-approval (config round-trip).
- **Basis:** Q10; `RemoteDatasetConfig` (`model.py:675-682`).
- **Change:** `raw: Optional[bool] = None`. `from_file` accepts `true`, `false`, `auto` or an omitted key; anything else is an error naming the key. `same_as` covers it, and `to_property` is unchanged.
- **Verify:** `tests/test_zfsbackup_config.py` (parse matrix, `same_as`).

**S6-b. Store column and migration.** Owner: `zfsbackup-store-developer`. Tier: needs-approval (schema). Runs after S6-a.
- **Change:** `dataset_remotes.raw`, a nullable BOOLEAN (NULL = auto); Alembic revision 3 on top of `7c3e9d51b2a8`, with downgrade; mapper round-trip; `EXPECTED_HEAD`/`KNOWN_REVISIONS`. B-3 is deferred, so nothing is bundled with it.
- **Verify:** `test_zfsbackup_store.py`, and `test_zfsbackup_store_upgrade.py` (2→3 and back).

**S6-c. CLI.** Owner: `zfsbackup-cli-developer`. Tier: needs-approval (writes config). Runs after S6-b.
- **Change:** `render.py` emits `raw: true|false` and omits the key for auto. `dotpath` supports `datasets.<ds>.remote.<dest>.raw` set/unset. `list --json` shows it.
- **Verify:** `test_zfsbackup_cli_render.py`, `test_zfsbackup_cli_dotpath.py`, `test_zfsbackup_cli_list.py`.

**S1-a. One snapshot listing that carries everything.** Owner: `zfsbackup-developer`. Tier: needs-approval (retention ordering).
- **Basis:** `backup_manager.py:768-783` lists `creation` only and sorts by name timestamp (`:782`); `remote.py:1172-1180` runs a second listing for `guid`.
- **Listing:** `_list_snapshots_strict` requests `properties=['createtxg', 'guid', 'userrefs', 'clones', 'encryption', 'keystatus']`, in the same single call.
- **`SnapshotInfo`:** gains `createtxg`, `guid`, `userrefs`, `clones`, `encrypted` and `key_loaded`.
- **Sort:** newest first by `createtxg`, with name timestamp and then name as tie-breakers. Two snapshots of one dataset can share a txg (`zfs snapshot a b`).
- **Missing `createtxg`:** the snapshot is dropped from the managed list with one WARNING. It can then never be pruned or treated as newest, which is the safe direction.
- **Retention walk:** `needs_prunning` (`:744`) sorts its walk explicitly by name timestamp, then `createtxg`, oldest first (D-B10). `keep` (`:742`) is the newest by `createtxg`.
- **`create_snapshot`'s `insert(0)` (`:812`):** the inserted object has `createtxg=None`. Treat None as newest, or re-list; the second is preferred.
- **Verify:** table T-S1 (below).

**S1-b. Strict name parse (Q13).** Owner: `zfsbackup-developer`. Tier: needs-approval.
- **Basis:** `:953-967`; `split('_', 1)` at `:960` accepts `{prefix}_{ts}_anything`.
- **Change:** a full match of `re.escape(prefix) + '_([0-9]{14})'` (ASCII digits only), plus the `strptime` validity check already there.
- **What changes classification:** only `{prefix}_{14 digits}_<anything>`, which becomes unmanaged: never pruned, sent or treated as newest. Sanoid's `autosnap_YYYY-MM-DD_HH:MM:SS_x` names are already unmanaged today.
- **Nothing shipped generates suffixes:** `_generate_snapshot_name` (`:793-799`) has none, and neither does the CLI or any scenario.
- **Tests that flip:** `test_zfsbackup_snapshot_info.py:201`, `:226` and `:319`. Check `test_zfsbackup_remote_timeouts.py:1388`, which uses a suffixed base only for URL quoting.

**B4-a. Prune fallback, one snapshot at a time.** Owner: `zfsbackup-developer`. Tier: needs-approval (retention).
- **Basis:** `backup_manager.py:928-940`. X3, and lib input point 5: list mode raises without returning the `destroy` lines.
- **Change:**
  - When a batch fails, retry each member alone. Count only calls that succeed; never count `-v` output.
  - Keep a memory `_prune_stuck[(ds, snap)] = reason`. Stuck names are tried alone on later cycles, so they never fail a batch of 19 others. An entry is dropped once the snapshot is destroyed, has disappeared, or is no longer a candidate.
  - Log WARNING once per stuck episode (DEBUG on repeats) and INFO when a stuck snapshot finally goes.
  - The reason comes from `userrefs`/`clones`, not from stderr: "busy" is ambiguous (lib point 5), and stderr is not yet forced to `LC_ALL=C` (lib S1).
  - A recursive child hold is only ever seen through the fallback; a recursive destroy is atomic per call.
  - Transient EBUSY (a send in progress) is simply retried next cycle.

**B4-b. Pre-skip held and cloned snapshots (D-B9).** Owner: `zfsbackup-developer`. Tier: needs-approval.
- **Change:** remove candidates with `userrefs > 0` or a non-empty `clones` before batching. WARNING once, naming `zfs holds` or the clone. This also applies in dry run.
- **Verify (B-4):** T-B4, plus `real_zfs` with a real `zfs hold` and `zfs clone` on zfsb-zfs21/22/24. (The original plan says zfsb-ci, but that VM has no `zfs`; that doc line is stale.)

**S1-c. Future-dated newest snapshot or anchor.** Owner: `zfsbackup-developer`. Tier: needs-approval (timeslot logic).
- **`needs_snapshot` (`:698-708`):**
  - The newest snapshot is now the newest by `createtxg`.
  - If its name is more than the tolerance (D-B12) in the future: ERROR once ("system clock is behind the newest snapshot; check NTP") and the dataset is due.
  - Before creating, if the generated name already exists in the listing: WARNING once per name, not due, and no `zfs` call. This is the backwards-clock collision.
- **Remote "not due" (`workers.py:642-651`):** the same tolerance rule for the anchor's age.

**S1-d. Client comparisons in `remote.py` switch to `createtxg`.** Owner: `zfsbackup-developer`. Tier: needs-approval (send logic).
- **Comparisons:** "never move the anchor backwards" (`:1062-1071`), `_settle_up_to_date` "newer" (`:1134-1140`), and the resume check that the source is older than the target (`:1278-1282`).
- **Follow automatically from the sort:** `latest` (`:465`, `:1155`) and the order of negotiate's list (`:844`). The server picks the first common name in the client's order (`api.py:1976-1979`), so that becomes the newest common snapshot by txg.
- **GUIDs:** `_snapshot_guids` (`:1172`) is replaced by `SnapshotInfo.guid` from S1-a. This is equivalent, because only managed matches are ever accepted.
- **Verify (S-1):** T-S1 with a frozen clock (`test_zfsbackup_snapshot_info.py`, `_manager.py`, `_remote.py`, `_workers.py`). `real_zfs`: a hand-made `prefix_20990101000000`, and a later-created `prefix_20200101000000` (txg order ≠ name order).

**R1. Release the server's per-dataset claim before the final ack.** Owner: `zfsbackup-developer`. Tier: needs-approval (API threads).
- **Basis:** `_receive_stream_locked` sends `ok` (`api.py:2237`) before `_receive_stream`'s `finally` releases the claim (`:2050-2053`). A client's immediate next stream (B-5 steps; today already `_continue_after_resume`) can therefore get `busy`.
- **Change:** release after `writer.close()` succeeds, then send `ok`.
- **Client side:** a `busy` on chain step 2 or later still just stops the chain.
- **Verify:** `test_zfsbackup_api.py` / `test_zfsbackup_stream_gate.py`, with an immediate back-to-back stream. Concurrency-reviewer.

##### Phase 2 (after SP M1 and M6, and libzfseasy B4)

**B5-a. Frozen flags and the per-mode token check (D-B1, D-B2), shipped together.** Owner: `zfsbackup-developer`. Tier: needs-approval.
- **Change:**
  - `_get_resume_stream` (`remote.py:1352-1363`) sends with `large_blocks=True, compressed=True`, or `raw=True` once S6-e lands.
  - `_EXPECTED_RESUME_FLAGS` becomes `_RESUME_FLAGS = {'plain': (frozenset(), frozenset({'compressok','largeblockok'})), 'raw': (frozenset({'rawok'}), frozenset({'rawok','compressok','largeblockok'}))}` with the rule from D-B2.
  - Error messages keep naming the unexpected flags.
- **What is irreversible after the first deployment, per server dataset:**
  - `-L`: changing it in either direction is unsafe (M6 records which direction zfs refuses).
  - Raw vs plain: switching needs a full re-send.
  - `-c`, `-e` and chain vs `-I` can be changed later.
- **Effect on the server:** nothing new is needed on receive. With `-c` the server stores blocks with the client's compression, so the server pool needs the matching feature, for example `zstd`.
- **Test pools:** today's two-VM server datasets were received without `-L`. Recreate them; J already uses fresh pools.
- **Tests that change:** `test_zfsbackup_remote.py:594-605` (`compressok`/`largeblockok` become acceptable in plain mode; `rawok` only in raw mode).

**B5-b. Intermediate snapshots as a chain of `-i` steps (Q9).** Owner: `zfsbackup-developer`. Tier: needs-approval. Runs after S1-d, B5-a and R1.
- **Basis:** `remote.py:1356-1358` sends `latest` with `-i common`, so intermediate snapshots never reach the server.
- **Design:**
  - **Planning:** one negotiate per cycle. The chain is the managed snapshots newer by txg than `common`, oldest first. If `common` is named but missing locally, skip the cycle; never fall back to a full send.
  - **Before the first step:** the resume token is fetched once, as today. A resumable token runs as step 0. Then one re-negotiate, and the chain continues from the new common snapshot. This replaces `_continue_after_resume` and `followup`.
  - **Each step:**
    - `should_stop` is checked (a safe point between steps);
    - `set_inflight(prev, next)` is written before `zfs send`;
    - the stream is sent with `mode=fresh`, then acked;
    - `set_anchor(next)`, which is strictly newer by txg, so the anchor never moves backwards;
    - `clear_inflight`.
  - **Failures:**
    - A failed anchor write stops the chain, and the pin stays.
    - `busy` or reloading stops the chain; the next cycle continues from the anchor.
    - A step whose snapshot has vanished is handled per D-B8.
  - **Stop and dry run:** a stop mid-chain returns True and logs "continues next cycle", as the resume follow-up does today. Dry run logs the planned chain.
  - **Pins:** only the current step's two snapshots are pinned. Members not yet sent may be pruned locally; they are then never replicated. This is accepted (D-B8).
  - **Server side:** each step claims and releases the per-dataset lock separately (R1). The reload gate refuses new steps while the server drains.

##### Phase 3 (after SP M5 and Z1–Z7)

**R0. The server receive creates the leaf dataset (D-B5).** Owner: `zfsbackup-developer`. Tier: needs-approval (receive/API, changes A's contract). Concurrency-reviewer reviews (the shared map).
- **Basis:** `api.py:1943-1952` (negotiate pre-creates the dataset), `:2155-2169` (recreated after an abort), `:2083-2115` (refused without a local config), `:2184-2188` (`-F`). If M5/Z1 confirm the hazard, raw streams and encrypted server targets cannot work on this flow.
- **Change:**
  - **Negotiate:** if the dataset does not exist, create only its parent (at least `<target>/<id>`) and record the pending config. If it exists, set the properties as today; the D7 check is unchanged.
  - **Stream:** take `owned` from the local value, otherwise from the pending entry, otherwise refuse.
  - **Abort:** after a `fresh` abort that destroyed the dataset, do not recreate it.
  - Keep `-F` for now; B-2 revisits it.
  - The map is keyed by server dataset, bounded by the number of datasets, guarded by a lock, and dropped once a local config exists.
  - Z3 decides how a partial new dataset is resumed.
- **Edge case (documented, with a clear error):** a raw parent whose server copy was already created as a placeholder for a child negotiated earlier cannot then receive its own full raw stream.
- **Tests:** many milestone-A tests (`test_zfsbackup_api.py` `zfs.create` mocks at `:158`, `:223`, `:318`, …) change. Coordinate with the test author's open A work.

**S6-d. Negotiate reports `server_copy` (D-B4).** Owner: `zfsbackup-developer`. Tier: needs-approval. Runs after R0.
- **Change:** `none` if the dataset is missing or has no snapshots. `raw` if `encryption != off` and `encryptionroot` is the dataset itself or lies under `<target>/<id>/` (predicate confirmed by Z6). Otherwise `plain`.

**S6-e. Client mode selection.** Owner: `zfsbackup-developer`. Tier: needs-approval (remote/security). Runs after S6-a, S6-d and B5-b.
- **Change:**
  - The mode is decided once per `backup_dataset`, from the newest snapshot's `encrypted` and `key_loaded`, with no extra `zfs` call, and is fixed for the whole chain.
  - `raw: true` on an unencrypted dataset is refused, and so is plain mode with the key unloaded; each logs an ERROR once and nothing is sent.
  - A `server_copy` that does not match the mode is refused (D-B4).
  - Raw sends use `raw=True` only.
- **Edge cases:**
  - Encryption cannot change on an existing dataset. A "change" means the dataset was replaced, and negotiate handles that like any new lineage.
  - Key rotation (`change-key`) depends on Z5.
  - Restoring a raw backup needs the client's key material. This goes into the B-9 runbook.

##### Tests and scenarios (alongside each item)

**`pytest-test-author`** (mocked tests per item, plus `real_zfs` on zfsb-zfs21/22/24):

**T-S1:**
- normal order;
- a future-dated newest: a snapshot is taken, and the next cycle is not due;
- a past-dated name with a newer txg: due once;
- a backwards clock with a name collision: logged once, no `zfs` call, no loop;
- txg order ≠ name order: newest, resume, settle and anchor-not-backwards all follow txg;
- equal txg;
- a future-dated anchor: due;
- the strict parse table, suffixes included;
- retention keeps the oldest snapshot per slot, unchanged.

**T-B4:**
- one held snapshot in a batch of 20: 19 destroyed, the held one pre-skipped, WARNING once;
- a hold appearing between listing and destroy: fallback, 19 destroyed;
- a clone, including in dry run;
- a recursive child hold: fallback, only that name stays;
- EBUSY: retried next cycle, no duplicate WARNING;
- once stuck, never back in a batch;
- a failed call's lines never counted.

**T-B5:**
- the steps and their order;
- a failure, a stop, `busy` and an anchor-write failure, each mid-chain;
- a member vanishing between steps (rebuild);
- resume followed by the rest of the chain;
- a missing `common`;
- `-L -c` in argv;
- the token-flag matrix per mode.

**T-S6:**
- the auto/true/false × encrypted/unencrypted × key loaded/unloaded matrix;
- the `server_copy` mismatch, both ways;
- `-w` argv;
- R0: no pre-create, pending config, no recreate after an abort, refused with neither config source.

**`real-zfs-scenario-dev`** (two-VM, zfsb-client/zfsb-server):
- **B extended:** a backlog of N snapshots, then every managed client snapshot is on the server with a matching GUID, and no unmanaged one is.
- **J rerun** with the new flags, plus a resume mid-chain.
- **New scenario M (encrypted):**
  - a passphrase-encrypted client dataset sent with `raw: auto`;
  - the server shows `keystatus=unavailable` and matching GUIDs;
  - the server pruner works with no key;
  - sends continue with the client key unloaded;
  - `raw: false` with the key loaded, then switched to `auto`, gives the mismatch ERROR; after the runbook rename, a full raw send;
  - plain mode into an encrypted server target (R0).

**Docs** (main session): fix CLAUDE.md "newest per time slot", the original plan's "real_zfs on zfsb-ci" for B-4, and its "-L after the first receive" wording to match M6. Add raw mode and the D-B4 runbook step to the README and the B-9 runbook.

---

#### Order and parallelism

1. **B0**, then **SP** starts immediately. In parallel, `zfsbackup-developer` runs S6-a → S1-a → S1-b → B4-a → B4-b → S1-c → S1-d → R1.
2. **S6-b**, then **S6-c**, run on the store and CLI lanes as soon as S6-a lands.
3. **libzfseasy B4 fixes** run on the lib lane, in parallel.
4. **After M1/M6 and lib B4:** B5-a → B5-b.
5. **After M5 and Z1–Z7:** R0 → S6-d → S6-e.
6. **Tests** follow each item. Scenarios B, J and M run last.
7. **Reviews:** `zfs-code-reviewer` on every item; `concurrency-reviewer` on R0, R1 and B5-b (safe points).

**Every item is needs-approval** except B0, SP, tests and docs. `zfsbackup-developer` remains the bottleneck.

#### Deferred (not dropped)
- **Negotiate by name only.** A replaced dataset whose snapshot names collide gets a false "common" snapshot. Match by GUID: S-5 or milestone D.
- **The server pruner classifies received snapshots with the server's own `snapshot_prefix`** (`received_datasets`, `_received_dataset_info`). A client with another prefix is never pruned on the server; this fails safe (keeps everything). The wire config would need a prefix field.
- **Filtering the chain by retention** (D-B11), **`-e` in plain mode**, **bookmarks as incremental base** (K-7), **automatic re-send on a mode switch**, **B-2's `-o`/`-x` set** (milestone D; Z2 collects its raw facts now), **S-7 outcome recording**.

---

#### For your approval

**Proposed:**
- B-4: a per-snapshot fallback plus a held/cloned pre-skip.
- S-1: one listing carrying `createtxg`, `guid`, `userrefs`, `clones` and encryption; every newest/older/newer comparison on txg; the future-dated rule; the strict parse.
- B-5: a managed `-i` chain with a frozen `-L -c` set and a per-mode token check.
- S-6: `raw: auto|true|false` on each dataset's remote entry, through model, store (revision 3) and CLI, with server-copy mismatch detection.
- R0/R1: the server creates the leaf on receive, and releases its claim before the ack.
- A matrix spike that gates the flag freeze and the R0/S-6 design.

**Assumptions to confirm:**
- Encrypted server target pools must be supported. This makes R0 needed even without raw.
- A and B ship together, so protocol v1 is unchanged.
- The `raw: true` semantics in D-B3.

Approve, adjust, reprioritise or drop any item or decision. Nothing is implemented before your sign-off.

**Key files:**
- /Users/rejsmont/src/libzfs/zfsbackup/backup_manager.py
- /Users/rejsmont/src/libzfs/zfsbackup/remote.py
- /Users/rejsmont/src/libzfs/zfsbackup/api.py
- /Users/rejsmont/src/libzfs/zfsbackup/workers.py
- /Users/rejsmont/src/libzfs/zfsbackup/config/model.py
- /Users/rejsmont/src/libzfs/zfsbackup/config/store/models.py
- /Users/rejsmont/src/libzfs/zfsbackup/config/store/migrations/versions/
- /Users/rejsmont/src/libzfs/zfsbackup/cli/render.py
- /Users/rejsmont/src/libzfs/zfsbackup/cli/dotpath.py
- /Users/rejsmont/src/libzfs/zfsbackup/protocol.py
- /Users/rejsmont/src/libzfs/libzfseasy/zfs.py
- /Users/rejsmont/src/libzfs/scenarios/test_openzfs_matrix.sh
- /Users/rejsmont/src/libzfs/scenarios/test_two_vm_backup.sh
- /Users/rejsmont/src/libzfs/tests/test_zfsbackup_snapshot_info.py
- /Users/rejsmont/src/libzfs/tests/test_zfsbackup_remote.py
- /Users/rejsmont/src/libzfs/tests/test_zfsbackup_api.py
- /Users/rejsmont/src/libzfs/docs/production_readiness_plan.md
- /Users/rejsmont/src/libzfs/CLAUDE.md

---

## Appendix B2. libzfseasy, milestone B (B4)

### libzfseasy plan for milestone B: item B4 (OpenZFS 2.1, 2.2 and 2.4 output formats)

This is a proposal; nothing has been edited. It is planned against the working tree on `prod-trust-boundary`.

**Main conclusions**
- None of the current formats needs a libzfseasy fix.
- What milestone B still needs is mostly evidence: send flags, failed destroys, encryption and the new listing columns are not yet checked on 2.1, 2.2 or 2.4.
- Two findings change what zfsbackup can do:
  - A resume token records feature flags that depend on the dataset's data, not just on the send flags.
  - A full raw stream probably cannot be received with `-F` into the pre-created, unencrypted server dataset.

#### 0. What T2 covered (the evidence)

**The captured format output is not in the repo.** T2 sent it to a `mktemp` log directory in the session that crashed, and it is gone. What survives:
- the T2 summary line in the old transcript (formats identical on 2.1.5, 2.2.2 and 2.4.1, except `-j`);
- the `-x`/`-o` probes in `/private/tmp/claude-501/-Users-rejsmont-src-libzfs/d470aa33-2b0d-4e69-8da0-6c3355d7a80d/scratchpad/out2{1,2,4}.txt` and `o2_zfs2{1,2,4}.txt`;
- the pytest logs in `.../scratchpad/matrix-logs/`, `mx1/` and `mx2/`.

**Covered on all three versions:**
- `formats.sh` (`scenarios/test_openzfs_matrix.sh:276-332`):
  - `send -nv -t` on a full plain token, a stale token (target destroyed) and a corrupt token;
  - `receive -A` with saved state, already clean, a dataset that does not exist, and an existing dataset with an interrupted incremental;
  - `destroy -n/-v -p`: single, range, batch, range plus batch, real batch, `-r`, and a missing snapshot;
  - the `received` column, before and after a local override.
- pytest: `TestRealFraming` and `TestRealGetLeadRows` (71 passed on each version in `mx2`).
- `tests/test_zfsbackup_real.py::TestRealSendReceive` (`:969-1035`): full-stream kill, resume and a stale target token. It ran on 2.1.5 and 2.4.1 (`matrix-logs`) and on 2.2.2 (`zfsb-ci`, B-7).
  - The one failure, `test_negotiate_reports_the_common_snapshot` (426), was a missing S-5 protocol header. It appears fixed in the working tree (`:1085`); confirm on the next run.

**Not covered on any version:**
- tokens made with `-L`, `-c`, `-e` or `-w`;
- incremental tokens, including a token whose incremental source is gone, and a snapshot destroyed and recreated with the same name;
- destroy failures (held, cloned or busy snapshot; batch vs single; `-r` with a held child snapshot);
- `createtxg`, `userrefs` and `clones` in a listing;
- encryption and `keystatus`;
- raw send and receive.

The T1 list from milestone A (`exists`, `abort`, `send -i`, `resume_info`/`resume` in one pool, batch destroy) is also still undone in `tests/test_real_zfs.py`.

#### 1. B4 answers

- **`resume_info`** (`zfs.py:1828-1877`; parser `:1674-1711`):
  - The contents block is on stdout on all three versions, so the stderr fallback stays as a guard.
  - Not yet checked:
    - the token flags (`:1709` accepts only keys ending in `ok` with value `= 1`);
    - incremental tokens (`fromguid`);
    - the wording of a stale incremental source. `remote.py:329-340` matches "incremental source … no longer exists", "Invalid cross-device link" and "no longer the same snapshot"; these were checked on 2.4.1 only.
- **`destroy -v -p`:**
  - Both the source (`zfs_main.c:1491, 1572, 1789`, copy in the scratchpad) and T2 agree: the `destroy\t<name>` lines are printed while the snapshots are gathered, **before** the destroy is attempted.
  - Expected from the source, not yet checked on our VMs:
    - **held or busy snapshot:** destroy lines, then `reclaim`, then "cannot destroy snapshot X: dataset is busy" and rc 1.
    - **cloned snapshot:** destroy lines, **no** `reclaim` line, "…has dependent clones / use '-R'…" plus one stderr line per clone, rc 1.
    - A dry run (`-n`) **succeeds** on a held snapshot but fails on a cloned one.
  - A hold and a transient busy (a `zfs send` in progress) give the same text. Only `userrefs` tells them apart.
  - libzfseasy already never counts the lines of a failed call in list mode: `_exec_out` raises at EOF (`zfs.py:547`), so the list comprehension at `:1095-1100` never returns. **Lazy mode does yield the lines before it raises.** zfsbackup uses list mode (`backup_manager.py:937`).
- **`receive -A`:** the wording is identical on all three. It is matched by substring (`api.py:96-107`), so the extra "For further help…" footer that 2.2+ adds to *usage* errors does not affect it.
- **`received` column:** identical, and covered by pytest on all three versions.
- **Reclaim bytes:** only the number in `reclaim\t<N>` differs, because space accounting differs between versions. Nothing parses it: `_destroy_lines` drops the line (`zfs.py:1052`), nothing in zfsbackup reads it, and the mocked tests only assert that it is dropped (`test_commands_part2.py:2087-2110`). No action needed.

#### 2. What milestone B's zfsbackup items need from libzfseasy

| Need | Supported today? | Gap |
|---|---|---|
| **B-5:** `-L`, `-i` per step, `-c`, `-e` | Yes. `send.snapshot(large_blocks, compressed, embed, since)`, `zfs.py:1727-1758`, flags at `:1897-1929`. `intermediate` (`-I`) exists but Q9 rules it out. `since` must be a Snapshot (`:1922`), which is fine for a chain over managed snapshots. | None in the code. |
| **B-5:** resumed sends | `send.resume(token, embed)` (`:1813-1825`). `zfs send -t` takes its flags from the token (`zfs_main.c:4935-4947`). | Keep `resume()` plain; don't add `-L`/`-c`/`-w` to it. |
| **B-5:** token flag names (`largeblockok`, `compressok`, `embedok`, `rawok`) | The parser collects every key that ends in `ok` with `= 1`. | **Unverified on every version for any flag.** From the OpenZFS send code (from memory, to be confirmed): `compressok` is recorded for `-c` or `-w`; `largeblockok` only if the dataset's `large_blocks` feature is *active*; `embedok` only if `embedded_data` is active and the dataset is not encrypted; `rawok` only for `-w` on an encrypted dataset. **So `remote.py:1254`'s equality check against one fixed set (`:319`) would refuse valid tokens once `-L` is added.** It needs a subset rule (decision DB1). |
| **S-6:** `raw` (`-w`) on send | Yes (`:1907`). | None. |
| **S-6:** `-o`/`-x` on a raw receive | Yes (`ReceiveCommand._get_options`, `:2074-2105`). | Unverified. The `-o` user properties that B-1 now passes are expected to be allowed; `-o`/`-x encryption` is expected to be refused. **Expected hazard:** a full raw stream with `-F` onto the pre-created *unencrypted* server dataset fails ("zfs receive -F cannot be used to destroy an encrypted filesystem or overwrite an unencrypted one with an encrypted one"). That affects S-6's design (DB5). |
| **S-6:** sending with the key unloaded | `send(raw=True)`. `unload_key` needs `ds` (S5 note, `:2204`). | Unverified: the error text of a non-raw send with the key unloaded. |
| **S-6:** reading `keystatus` and `encryption` | Yes, both are known property names (`types.py:16-18` datasets, `:38-42` snapshots). Read with `zfs.get.value` (`zfs.py:1367`); `-` becomes `None`. | Unverified whether an unencrypted dataset reports `keystatus` as `-` or as empty. |
| **S-1:** `createtxg` in one listing | Yes. `zfs.list(roots, types='snapshot', recursive=..., properties=['createtxg', 'guid', ...])` is one call (`zfs.py:750-802`), and `sort='createtxg'` passes `_check_sort_key`. Values are raw strings, so the caller converts with `int(...)`. zfsbackup currently makes two listings (`backup_manager.py:770` creation, `remote.py:1176` guid) and can merge them. | None. (`creation` without `-p` has only minute resolution; it is not used for ordering.) |
| **B-4:** `userrefs` and `clones` in a listing | Yes, in the snapshot property names (`types.py:38-41`). | Unverified: what `clones` prints when there are none (`''` or `-`→`None`), and the comma-joined form when there are several. |
| **B-4:** error text and rc for held, cloned and busy, batch vs single | Every failure raises the bare `Exception(stderr)`; rc is not exposed. `zfs` returns rc 1 in all of these cases. | The wording needs checking on each version (M3). Optional `DestroyError` (L6, recommended not to do). `snapshots([])` raises `AttributeError` (`zfs.py:1089`, S5). |
| **Related, found in the probes:** `-o P=v` together with `-x P` | Passed through to zfs. | On all three versions zfs fails with a usage error, rc 2: "property … specified multiple times" (`out2{1,2,4}.txt`). Refuse it before running (L1). |

#### 3. Work items (each can be approved on its own)

##### Matrix captures: `real-zfs-scenario-dev`, all low-risk. They gate the zfsbackup work.

Each capture adds a section to `formats.sh` in `scenarios/test_openzfs_matrix.sh` and writes one file per section into a persistent `MATRIX_LOG_DIR`.

- **M1. Resume-token flags.**
  - Run every send flag set (none, `-L`, `-c`, `-e`, `-L -c`, `-w` on an unencrypted dataset, `-w` on an encrypted one) against:
    - a dataset with `recordsize=128K` and small data;
    - a dataset with `recordsize=1M` and a large file (so `large_blocks` is active);
    - compression `lz4` and `off`.
  - For each, capture a full and an incremental interrupted `receive -s`, then `send -nv -t` (stdout, stderr and rc separately), then `send -t | receive -s` to completion.
- **M2. Stale tokens:** incremental source destroyed; target destroyed and recreated under the same name; the sender's dataset renamed.
- **M3. Destroy failures:**
  - Batch and single destroy of a held snapshot (`zfs hold`), a cloned one, and a busy one (`zfs send … | sleep 30`).
  - `-r` with a hold on a child's snapshot.
  - A dry run of each.
  - For each: the destroy lines, whether a `reclaim` line appears, stderr, rc, and `zfs list` afterwards (to show nothing was destroyed).
- **M4. Listing:** `zfs list -H -r -t snapshot -o name,type,createtxg,guid,userrefs,clones` with no clones, one clone and two clones, plus `-s createtxg`.
- **M5. Encryption and raw:**
  - A passphrase dataset (`keylocation=file://`). Read `encryption`, `keystatus`, `encryptionroot`, `keyformat` and `keylocation` on a filesystem and on a snapshot, before and after `unload-key`; also `keystatus` on an unencrypted dataset.
  - Non-raw send with the key unloaded (expected to fail) and raw send with the key unloaded.
  - Raw receive:
    - into a dataset that does not exist yet, with `-o org.x:y=v`, `-x org.x:z`, `-o canmount=noauto`, `-o readonly=on`;
    - **full raw with `-F` onto a pre-created unencrypted dataset**;
    - an incremental raw;
    - a raw resume, with the key unloaded on both sides;
    - `-o encryption=…` (expected to be refused).
- **M6. Freezing `-L`:** a full receive with `-L`, then an incremental without it, and the reverse. Capture the error wording.
- **M7. Format captures become fixtures:** write each section so it can be extracted mechanically, so the main session can commit the captures (M8).

Verification: `./scenarios/test_openzfs_matrix.sh --formats-only` on `zfsb-zfs21`, `zfsb-zfs22` and `zfsb-zfs24` (these VMs only).

##### Tests: `pytest-test-author`, low-risk

- **M8. Fixtures.**
  - Commit `tests/fixtures/openzfs/{2.1.5,2.2.2,2.4.1}/*.txt` from the M1–M6 captures.
  - Add mocked tests that replay them through `resume_info` (`mock_subprocess` with the real stdout and stderr) and `_destroy_lines`.
  - This gives regression coverage without a VM. Run with `pytest tests/test_commands.py tests/test_commands_part2.py`.
- **R1–R5.** Add these to `tests/test_real_zfs.py`. The assertions must hold on every version; where a value differs by version, pin it per version.
  - **R1 `TestRealResumeInfo`:**
    - full and incremental tokens: `toguid` matches the snapshot's guid, `fromguid` is right, `error` is `None`;
    - the flag sets from the M1 table;
    - `send.resume(token)` into `receive(save=True)` completes;
    - stale target and stale source set `.error`;
    - a corrupt token raises `ResumeTokenError`.
  - **R2 `TestRealReceiveAbort`:** with saved state, already clean, and a dataset that does not exist. The stderr must contain one of `api._ABORT_ALREADY_CLEAN`.
  - **R3 `TestRealDestroyBatch`:**
    - a dry run returns the names without `reclaim`; a range works;
    - a failed batch (hold or clone) raises, destroys nothing, and list mode returns nothing;
    - the busy, clone and child-hold wording that B-4 will match;
    - lazy mode yields lines and then raises (documents the hazard).
  - **R4 `TestRealSnapshotListing`:** one recursive `zfs.list` with `createtxg`, `guid`, `userrefs` and `clones`; txg order matches creation order; a held snapshot has `userrefs == '1'`; `clones` is pinned for none and for several.
  - **R5 `TestRealEncryption`:** `get.value` of `encryption`/`keystatus`; a raw send with the key unloaded works and a plain one raises; a raw receive with `-o` user properties.
  - **Extend `tests/conftest.py:633-643` delegation** with `send,hold,release,load-key,change-key,encryption,keyformat,keylocation,recordsize,readonly,canmount`. Assumption: on the Linux VMs the sudo-wrapper fallback already runs everything as root, so this only matters on a host where delegation works.
- **R6.** Mocked argv tests for L1 and L2 in `tests/test_commands_part2.py`.
- **Verify:** `./scenarios/test_openzfs_matrix.sh --extra tests/test_zfsbackup_real.py` passes on all three versions.
  - Mock tests cannot prove any pipe behaviour; none of these items needs that.

##### libzfseasy code: `libzfseasy-developer`

- **L1** (low-risk). `ReceiveCommand._get_options` (`zfs.py:2079-2089`) raises `ValueError` when a name appears in both `props` and `reset`.
  - Basis: zfs answers with a usage error (rc 2) on all three versions.
  - Relevant to S-6 and B-2, which combine `-o` and `-x`.
- **L2** (low-risk). `DestroyCommand.snapshots([])` raises `ValueError` (`:1078-1089`).
  - S5 moved forward: B-4's one-at-a-time retry and its pre-filter can produce an empty batch.
- **L3** (low-risk, docstrings only).
  - `DestroyCommand`: lazy mode on failure, and the fact that a dry run passes a held snapshot.
  - `ResumeToken.flags` (`:1653`): flags depend on the dataset's feature state, so check them as a subset.
- **L4** (needs-approval, only if the evidence calls for it). Fix whatever M1–M6 or R1–R5 show to differ between versions, for example `*ok` keys that 2.1 prints differently, or a token block on stderr. The change and its exact diff come back for approval.
- **L5** (low-risk, optional). `send.resume` applies `resume_info`'s token check (`:1843-1845` vs `:1824`; S5).
- **L6** (needs-approval; recommended **not** to do). A `DestroyError(Exception)` carrying the busy and cloned names parsed from stderr. It would change the error convention.

Verification for L1–L3 and L5: `pytest tests/test_commands.py tests/test_commands_part2.py tests/test_framing.py`.

#### 4. Order and parallelism

1. **Now, in parallel:**
   - M1–M7 (scenario developer, which owns `scenarios/`);
   - L1, L2, L3, L5 (libzfseasy developer);
   - R6 and the conftest permissions (test author, once it finishes its milestone-A edits).
   - zfsbackup **S-1 can start now**: it needs nothing new from libzfseasy.
   - **B-4 can start now** with the 2.4.1 facts from X3 (all-or-nothing batch, retry one at a time), but its error-text matching waits for M3.
2. **After M1/M6:** decide DB1 and DB2. Then zfsbackup **B-5** (flag set, subset check in `remote.py:1254`).
3. **After M5:** decide DB5. Then zfsbackup **S-6** design.
4. **After the captures:** M8 and R1–R5 (test author), then L4 if needed.
5. **Last:**
   - a full matrix run with `--extra tests/test_zfsbackup_real.py`;
   - two-VM `--only-j`, scenario B and scenario J with the new flags;
   - `zfs-code-reviewer` on the result. `concurrency-reviewer` is not needed unless L4 touches the exec path.

#### 5. Risks

- M3's "busy" fixture uses a send piped into `sleep`, which can be timing-sensitive. Clean up with `zpool destroy -f` on the matrix VMs only.
- Adding `-L` before DB1 lands turns every partial `-L` resume into a fresh send. Partial progress is lost but no data is. B-5 must ship the flag set and the token check together.
- If DB5 is confirmed, S-6 conflicts with the milestone-A receive design: the server's local properties set with `-o`, and the refusal of a stream with no stored config, both assume a pre-created dataset.
- L1 refuses input that zfs already rejects, so no caller that works today breaks.

#### 6. Deferred, with reasons

- `-I` (`intermediate`) coverage: Q9 chose a chain of `-i` sends.
- A `-p` (parsable) option on `ListCommand`: `createtxg` and `userrefs` are already numbers.
- `hold`/`release` commands (C7): the tests call `zfs hold` directly.
- `-j` (S4, milestone D).
- 2.3 coverage (Q5).
- The non-B4 parts of the T1 remainder: `exists`, renaming a snapshot or volume, names with inner spaces, unrepresentable names, `get all` on each type. These are low-risk and can go to the test author at any time.

#### 7. Decisions (each with a recommendation; "accept the recommendation" is enough)

- **DB1. Resume-token check in zfsbackup** (handed to its planner).
  - **Recommendation:** accept a token whose flags are a subset of the flags our send set can record. Use the M1 table rather than one fixed set.
- **DB2. Frozen send-flag set.**
  - **Recommendation:** `-L -c` for non-raw sends and `-w` for raw ones. Leave out `-e`, whose token flag depends on the pool's state.
  - Freeze it only after M1 and M6 confirm the behaviour on all three versions.
- **DB3. Structured destroy errors (L6).**
  - **Recommendation:** no. B-4 retries one snapshot at a time and uses `userrefs`/`clones` to skip held and cloned snapshots up front.
- **DB4. Commit the captures as fixtures (M8).**
  - **Recommendation:** yes. Otherwise the next "identical across versions" claim has nothing to check it against.
- **DB5. Raw receive target.**
  - **Recommendation:** run M5 before S-6 is designed. If `-F` onto a pre-created unencrypted dataset fails as expected, S-6 must receive the first full raw stream into a dataset that does not exist yet, with the ownership properties set by `-o`.

#### For your approval

I propose:
- matrix captures M1–M7;
- fixtures and real-pool tests M8 and R1–R6;
- four small low-risk libzfseasy changes (L1–L3, optional L5);
- one conditional needs-approval fix slot (L4);
- dropping L6.

No current format needs a fix. The real work is the evidence that B-5 and S-6 need before their flag sets and receive design are frozen.

Assumptions to confirm:
1. The token-flag behaviour and the raw `-F` refusal come from my reading of the OpenZFS send and receive code. They are not yet checked on our VMs.
2. The sudo-wrapper fallback is what makes the Linux matrix run as root.

Approve, adjust, reprioritize or drop any item.

---

## Appendix C1. Milestone C

### Milestone C implementation plan (install, operate, restore): proposal

**Base:** branch `prod-trust-boundary`, with milestone A uncommitted in the tree (`git diff HEAD`: B-1a–c, S-4, S-5 in `api.py`, `backup_manager.py`, `remote.py`, `protocol.py`). I made no edits. Line numbers are from the current tree.

**Stale citations I found:**
- The plan's `workers.py:614, :1643` (lazy imports) are now `:621, :1650`. Its `:648` (`backup_dataset` result dropped) is now `:655`.
- The README unit is now `zfsbackup/README.md:441-489`.
- `.claude/agents/README.md:38` lists neither `protocol.py` (new in A) nor the new modules below.

---

#### Decisions to take first

Each has a recommendation. Reply "accept all", or answer by number.

| # | Decision | Recommendation | Alternatives |
|---|---|---|---|
| **C-Q1** | Packaging format | **Wheel + sdist.** Units live in `packaging/` and ship in the sdist only, never the wheel. Defer a `.deb`. | deb only; both |
| **C-Q2** | Distribution name. `pyproject.toml:2` calls the one dist `libzfseasy`, but it ships both packages. | **Rename the dist to `zfsbackup`** and keep shipping `libzfseasy` inside it. One wheel per host, and `importlib.metadata.version("zfsbackup")` is the single version source. | keep `libzfseasy`; split into two dists (more work, path deps) |
| **C-Q3** | Supported Python | **3.11–3.14 supported.** Add 3.15 to CI now (it should be out by today; verify on setup-python) and declare it supported once green. No `Requires-Python` upper cap. Jammy's `python3` is 3.10, so on Ubuntu 22.04 the runbook uses a deadsnakes or uv-managed 3.11+, as `scenarios/test_openzfs_matrix.sh` already does. | lower the floor to 3.10 (no: code risk); cap `<3.16` |
| **C-Q4** | Where Prometheus output goes | **`check --prometheus` writes to stdout, or with `--output FILE` does an atomic temp+rename** (node_exporter's textfile collector reads half-written files otherwise). Ship an optional, disabled `zfsbackup-check.service`/`.timer` (oneshot every 5 min, output path set by `Environment=`). No HTTP endpoint (K-9). | stdout only; HTTP `/metrics` |
| **C-Q5** | Daemon console-script name | **`zfsbackupd`** | `zfsbackup-daemon` |
| **C-Q6** | When the Q11 pre-migration backup runs | **`migrate --backup-dir DIR` writes the backup only when the schema is actually behind**, inside the writer lock and before `upgrade_schema`. A separate `ExecStartPre=backup` would write a backup on every start, and `Restart=on-failure` loops would fill the disk. | a backup on every start |
| **C-Q7** | Key handling in `backup` / restore | **`zfsbackup-config backup DIR` includes `<db>.tls` keys by default.** Without read access to the keys (a group operator; keys are 0600, owned by the root daemon) it refuses with exit 1 and points to `--no-keys`. Restore is **documented only**: stop the daemon, `install -m0660 -g zfsbackup`, migrate, start. No `restore` command in C. | add a `restore` command now |
| **C-Q8** | Service account | **sysusers creates the group only** (`g zfsbackup -`). The daemon stays root (K-8). tmpfiles: `d /var/lib/zfsbackup 2770 root zfsbackup -`. Reword the store's "owned by zfsbackup:zfsbackup" advice (`paths.py:328`, used at `:872, :889, :1016, :1041`) to name the group only, since the user half is never applied (`paths.py:318-327`). | also create a `zfsbackup` system user now (future de-root) |
| **C-Q9** | Health semantics | WARN at 2× and CRIT at 4× of: local = `frequency`; remote = `max(dataset.frequency, destination.frequency)`, applied to the anchor's age; server = `max(client frequency, every client destination frequency)`, because the server cannot know which of the client's destinations it is. A destination with no anchor yet is WARN "not yet sent" until the oldest managed snapshot is older than the CRIT threshold, then CRIT. A dataset with an in-flight pin and a fresh anchor is OK. | per-item overrides in config (a schema change; defer) |
| **C-Q10** | Real-tier CI | **Spike** `apt install zfsutils-linux && modprobe zfs` on hosted `ubuntu-24.04`. If that fails: a self-hosted runner on `zfsb-ci`, limited to `schedule` + `workflow_dispatch` on `master` and **never** `pull_request` (a self-hosted runner on a public repo would run PR code). | self-hosted straight away; no real-tier CI |
| **C-Q11** | Supported systemd | **≥ 249.** Ubuntu 22.04 (249) and Debian 12 (252) use the shipped `<253` drop-in; Ubuntu 24.04 (255) uses the base unit. | ≥ 253 only |
| **C-Q12** | Upper caps beyond Q12's werkzeug | `werkzeug >=3.1,<3.2`, `urllib3 >=2,<3`, `requests >=2.31,<3`, `sqlalchemy >=2.0,<2.2` (2.0 and 2.1 are tested). `cryptography` stays uncapped and is covered by the weekly latest-deps job. | cap cryptography too |

---

#### Work items (clustered by owner and subsystem)

##### Lane 1: daemon core (`zfsbackup-developer`)

**C1. One version source and dependency pins (B-8a part 1, S-9a)**
- **Tier:** low-risk. **Size:** S.
- **Basis:**
  - The version is duplicated: `pyproject.toml:3` and `zfsbackup/__init__.py:3`.
  - The dist name is `libzfseasy` (`pyproject.toml:2`).
  - werkzeug and urllib3 are undeclared (`pyproject.toml:16-30`), yet `ApiServer` subclasses werkzeug internals (`workers.py:783` on), and `pinned_session` overrides requests/urllib3 internals.
- **Change:**
  - new stdlib-only `zfsbackup/version.py`: `installed_version()` via `importlib.metadata`, falling back to `"0+unknown"` on `PackageNotFoundError` (e.g. a `git archive` tree run without install);
  - `zfsbackup/__init__.py` exposes `__version__` through a lazy module `__getattr__`, so the package import stays trivial (see C2);
  - `pyproject.toml`: the C-Q2 rename, the C-Q12 pins, the `zfsbackupd` script, and sdist-only `include` of `packaging/**`; re-lock `poetry.lock`.
- **Edge cases:**
  - `packaging/` shares its name with PyPA's `packaging`, which pytest and pip import. It must never get an `__init__.py` or Python files, and must never go into the wheel, where it would install as a top-level `packaging` and shadow theirs.
  - S-8 (milestone D) will re-read `installed_version()` from disk, so it must not cache.
- **Verify:** mocked tests (C1T); the CI package job (C20).

**C2. Daemon entry point and `--version` (B-8a part 2)**
- **Tier:** needs-approval (startup signal disposition). **Size:** S. concurrency-reviewer reviews.
- **Basis:** `daemon.py:4-24` ignores SIGHUP only under `if __name__ == '__main__'`. A console script imports `zfsbackup.daemon` as a normal module, so `zfsbackupd = "zfsbackup.daemon:main"` would silently bring back the exit-129 SIGHUP death during the few hundred ms of imports. No `--version` exists (`daemon.py:2181-2225`).
- **Change:**
  - new tiny `zfsbackup/daemon_entry.py`: `main()` does `signal.signal(SIGHUP, SIG_IGN)` first, then imports and calls `zfsbackup.daemon.main`;
  - add `--version` to the argparse parser. It prints `zfsbackupd <ver> (wire protocol N, config schema <EXPECTED_HEAD>)`, which feeds the upgrade-together check in the runbook;
  - `python -m zfsbackup.daemon` keeps working.
- **Edge cases:**
  - `RestartForceExitStatus=SIGHUP` stays.
  - Under spawn/forkserver the main module is the pip wrapper script (no `.py`), so check that `__mp_main__` re-import and forkserver preload still work (3.14's Linux default is forkserver).
- **Verify:**
  - C2T: the generated-wrapper form under fork/spawn/forkserver reaches READY and turns over a reload; a SIGHUP sent right after exec does not kill it (mirrors the existing CLI console-script disposition test).

**C3. journald-friendly logging (S-13a, S-13b)**
- **Tier:** low-risk. **Size:** S.
- **Basis:**
  - `daemon.py:76-80` (`basicConfig` at import) and `workers.py:93-98` log to stdout with no priority, so `journalctl -p err` finds nothing.
  - The only level control is `-v`.
  - werkzeug's per-request INFO access line is not overridden in `_api_server_class`.
- **Change:**
  - new stdlib-only `zfsbackup/logsetup.py` with `configure(level)`, called from `daemon.main()` (moved out of import time) and from `_setup_logging` in workers, so spawn/forkserver children behave the same as fork;
  - it is journald-aware **only when `$JOURNAL_STREAM` equals the handler stream's `fstat` dev:ino**, not merely when the variable is set (it is inherited by children whose stdout is elsewhere);
  - under journald: `<N>` sd-daemon priorities and no `asctime`;
  - **every line of a multi-line record gets the prefix**, or a traceback's continuation lines would land at info;
  - `--log-level debug|info|warning|error`, with `-v` = debug, passed to workers in place of `verbose`;
  - `log_request` override: 1xx–3xx at DEBUG, 4xx at INFO, 5xx at WARNING. Floods of 401/403 are left to S-3.
- **Edge cases:** `_init_child_process` ordering ("nothing may precede it") is unchanged: logging stays after the signal resets.
- **Verify:** C3T (mocked formatter tests including the dev:ino mismatch and a traceback); the install scenario (`journalctl -p err` shows a forced ERROR).

**C4. TLS-directory snapshot for backups (S-11b)**
- **Tier:** needs-approval (private keys). **Size:** S.
- **Basis:**
  - `identity.py:35, :202-233`: `<db>.tls/` holds the keys, and `identity.lock` guards creates and moves.
  - A plain copy can tear a cert/key pair mid-rotation.
- **Change:**
  - `identity.copy_tls_dir(src, dst)` takes `identity.lock` (exclusive, briefly) and copies `server.*`, `client.*` and `client.next.*`;
  - the destination directory is 0700, keys 0600, certificates 0640;
  - it raises a distinct `PermissionError` subclass when a key is unreadable (C-Q7);
  - stdlib + `cryptography` only, never the daemon.
- **Verify:** C4T, including a concurrent `rotate` racing the copy.

**C5. `lag_report` (B-6a)**
- **Tier:** low-risk (read-only). **Size:** M.
- **Basis:** M6. `workers.py:655` drops `backup_dataset()`'s result, and nothing reports whether backups happen.
- **Change:**
  - new `zfsbackup/health.py` (not `backup_manager.py`, to stay out of milestone B's files; S-7 extends it later);
  - a pure `evaluate(config, observations, now, warn, crit) -> list[LagEntry]`, plus a thin collector over `DatasetManager`: newest managed snapshot, `get_anchor` per destination, `get_inflight`, and `received_datasets()` (B-1c is already done, `backup_manager.py:546+`);
  - semantics as in C-Q9; disabled datasets are skipped, and `dry_run` gives a WARN line.
- **Depends on milestone B:**
  - "newest" must use S-1's createtxg ordering and its future-dated-name rule; a name in the future is reported CRIT "clock behind newest snapshot". **Land C5 after S-1.**
  - If S-6 lands, add a `raw` reason only if S-6 leaves a readable marker for a raw/non-raw mismatch. Otherwise it shows up as lag only, until S-7.
- **Test table (C5T):**
  - fresh;
  - lag at 1.9× / 2.1× / 4.1×;
  - no snapshots;
  - no anchor, young and old;
  - in-flight pin;
  - anchor == newest;
  - snapshot 1d / remote 1h (the max() rule);
  - server side with two client destination frequencies;
  - a future-dated newest snapshot;
  - a disabled dataset.

**C6. Shipped units (B-8a part 3)**
- **Tier:** low-risk, except the `ExecStartPre` line (needs-approval, Q11).
- **Basis:**
  - The README unit is inline only (`README.md:441-489`): `/usr/bin/python3 -m …`, only `After=zfs-import.target`.
  - Nothing creates the group or `/var/lib/zfsbackup` at mode 2770 (`paths.py:309`).
- **Files:**
  - `packaging/systemd/zfsbackup.service`:
    - `Type=notify-reload`, `NotifyAccess=main`, `KillMode=mixed`, `TimeoutStopSec=90`, `Restart=on-failure`, `RestartForceExitStatus=SIGHUP`, `RestartSec=60` (as today);
    - `Wants=`/`After=network-online.target zfs-mount.service`, plus `After=zfs-import.target`;
    - `RequiresMountsFor=/var/lib/zfsbackup`;
    - `AssertPathExists=/var/lib/zfsbackup/config.db`, so a host with no `import` yet fails clearly instead of looping. The install scenario confirms that `Restart=` does not re-fire on an assert failure;
    - `ExecStartPre=/opt/zfsbackup/venv/bin/zfsbackup-config -c /var/lib/zfsbackup/config.db migrate --backup-dir /var/lib/zfsbackup/backups`;
    - `ExecStart=/opt/zfsbackup/venv/bin/zfsbackupd -c /var/lib/zfsbackup/config.db`;
  - `packaging/systemd/zfsbackup.service.d/pre-253.conf`: `Type=notify` plus `ExecReload=/bin/kill -HUP $MAINPID`;
  - `packaging/sysusers.d/zfsbackup.conf` and `packaging/tmpfiles.d/zfsbackup.conf` (C-Q8);
  - the optional `zfsbackup-check.{service,timer}` (C-Q4).
- **Comment in the unit:** no `ProtectSystem`/`PrivateTmp`, because a private mount namespace hides `zfs create` mounts from the host.
- **Hazard:** under the documented `KillMode=process` alternative, surviving old workers would see `migrate` run under them; C12 refuses that case. ExecStartPre runs as root, and the store already handles root-created files (setgid directory, `_chgrp_to_service_group`).
- **Depends on:** C2 (script name) and C12 (the `--backup-dir` contract).
- **Verify:** the install scenario (C22).

**C7. `zfsbackup/README.md`, one consolidated pass (B-8c, B-9 docs, S-11 docs, S-13 docs, milestone-A facts, known limitations)**
- **Tier:** low-risk. **Size:** M. It lands last in this lane, in one edit sequence, because the README has a single owner.
- **Install:**
  - venv at `/opt/zfsbackup/venv` from the wheel (needs `python3-venv`; Python per C-Q3);
  - copy units from the sdist, then `systemd-sysusers` and `systemd-tmpfiles --create`;
  - first-run order: import, then trust/enroll, then `enable --now`;
  - NTP required;
  - firewall;
  - a per-client `zfs set quota` on `<target>/<id>`.
- **Upgrade runbook:**
  - read the release notes for a protocol or schema bump;
  - `zfsbackup-config backup`;
  - **`systemctl stop`, install, `systemctl start`.** Never `pip install --upgrade` under a running daemon: workers lazily import `remote`/`api` from disk (`workers.py:621, :1650`), and the S-8 guard does not exist yet. A versioned-venv symlink flip does not avoid this, because the venv prefix is resolved through the symlink;
  - ExecStartPre migrates and keeps the pre-migration backup;
  - downgrade = old wheel plus that backup (an old package refuses a newer schema);
  - **client and server must be upgraded together**: a protocol mismatch is 426, and remote backups pause (local snapshots and pruning continue) until both sides match; `check` shows the lag;
  - changing the frozen send flags or `raw` on an existing destination needs a full re-send (from milestone B).
- **Server rules (from milestone A):**
  - `remote_backup.target_dataset` must be a **dedicated** dataset;
  - the three properties `config`/`client_id`/`source_dataset` are the server's. A hidden *received* copy of each sits under the server's `-o` local value (`api.py:2185-2186`), so **never `zfs inherit -S`** them (it reverts to the client's copy), and never hand-edit them.
- **Restore (B-9):**
  - **File-level:** on the server, `zfs clone -o readonly=on -o canmount=noauto -o mountpoint=…` of a server snapshot **outside the target tree**, then copy files back and destroy the clone. While the clone exists, its origin snapshot cannot be pruned; before B-4 that blocks the whole prune batch, after B-4 only that snapshot. `.zfs/snapshot` works only while the received dataset is mounted, and that changes with B-2 (milestone D).
  - **Full:** out-of-band `zfs send` server → client (no `-p`/`-R`, with the frozen B-5 flags such as `-L`), received with `-u`. It can be restored in place to "seed and rejoin": negotiation matches by guid, so the next cycle is incremental.
  - **No common snapshot:** today the client fails each cycle. Either seed from the server as above, or `zfs rename` the server copy **out of** the target tree. Inside the tree its `source_dataset` would get the new send refused with 409, and it would keep being pruned.
  - **Raw (if S-6 lands):** `zfs send -w`, then `zfs load-key` with the original key. The server never has the key, so keep a copy of the key off the client.
  - **Host recovery:** a client restored from an S-11 backup keeps its client id and pin (no re-enroll). Otherwise `identity set-client-id ID`, then `enroll` with a server-side `trust token create --client-id ID --replace`. A lost server `.tls` means every client re-enrolls (K-4).
- **Logging:** rewrite the stale section (`README.md:529-537`) for C3.
- **Known limitations:** K-1 (pinned werkzeug, and stop calling it "the development server"), K-2–K-14, and the operator-visible libzfseasy C2–C7 (pool names, `longname`, `zpool` required at import, a `-` value reads back as None, Ctrl-C reaching `zfs` children, no hold/rollback). Also: no restore over the API.
- **Verify:** the install scenario and scenario R follow the README verbatim. Fix up the README after they run.

##### Lane 2: store (`zfsbackup-store-developer`)

**C8. `backup_database(resolved, dst)` (S-11a)**
- **Tier:** needs-approval (store contract). **Size:** S/M.
- **Basis:**
  - `export` omits trust, `host_identity` and keys;
  - `db.py:150-152` says a copy must take all three files or use `VACUUM INTO`;
  - the `_liveconn` guard means a raw `open()`/`close()` of the DB file must never drop a live connection's POSIX locks.
- **Change:**
  - SQLite online backup API (or `VACUUM INTO`) from a read-only connection;
  - written to a temp file in `dst`, then fsync and rename; mode 0600;
  - it must preserve `user_version` (the store id) and `alembic_version`;
  - it takes no writer lock and holds only a read transaction;
  - export it in `__all__`.
  - Optionally return `(store_id, generation, schema_rev, sha256)` for the CLI's MANIFEST.
- **Verify:** C8T:
  - consistent while a writer commits mid-backup (WAL);
  - `user_version`/revision preserved;
  - the copy opens and passes `check_config_db`.

**C9. CONFIG_OWNER advice wording (C-Q8)**
- **Tier:** low-risk. **Size:** XS. Can run in parallel with C8.
- **Change:** the messages at `paths.py:872, :889, :1016, :1041` name group `zfsbackup` and no longer imply a `zfsbackup` user.
- **Verify:** update the existing message assertions (C9T).

##### Lane 3: CLI (`zfsbackup-cli-developer`)

**C10. `zfsbackup-config --version` (B-8b)**
- **Tier:** low-risk. **Size:** XS. After C1.
- **Change:** the same string as C2, via `zfsbackup.version`, which is stdlib-only and so safe for the CLI.
- **Verify:** C10T.

**C11. `zfsbackup-config backup DIR [--no-keys]` (S-11c)**
- **Tier:** needs-approval (keys). **Size:** S. After C8 and C4.
- **Change:**
  - writes `DIR/zfsbackup-<host>-<UTC>/` with `config.db` (C8), `tls/` (C4) and a `MANIFEST` (version, schema, store id, generation, sha256);
  - directory 0700;
  - exit 1 on unreadable keys without `--no-keys`, 78 if the DB is unusable;
  - never signals the daemon, takes no writer lock, and cleans up a partial directory on `^C` (the existing interrupt discipline).
- **Verify:** C11T, including running as a group operator (refusal, then `--no-keys` works).

**C12. `migrate --backup-dir DIR` (Q11 / D4)**
- **Tier:** needs-approval. **Size:** S. After C11.
- **Basis:** `cli/main.py:749-805` upgrades with no backup.
- **Change:**
  - only when the schema is behind and inside the existing `writer_lock`, write a C8 backup (DB only; keys are not touched by a migration), then `upgrade_schema`;
  - if the backup fails, refuse with exit 1 and never migrate;
  - refuse with exit 75 while `daemonlock.workers_present()` (`daemonlock.py:1222`) and the schema is behind: survivors of a `KillMode=process` stop would otherwise run old code against a new schema. The unit's restart retries after 60 s;
  - a DB already at head writes nothing. A newer DB keeps today's error (`SchemaVersionMismatch`, 78).
- **Verify:** C12T matrix (behind / at head / newer / backup failure / workers present / concurrent migrate); the install scenario's upgrade step.

**C13. `zfsbackup-config check` (B-6b)**
- **Tier:** low-risk (read-only). **Size:** M. After C5.
- **Options:** `check [--json] [--prometheus] [--output FILE] [--nagios] [--warn-factor 2] [--crit-factor 4]`.
- **Behaviour:**
  - lazy import of `zfsbackup.health` (the `rename.py` precedent);
  - reads config through `read_config`, never signals, takes no lock;
  - also reports daemon presence and applied vs stored by reusing `status.collect`; daemon not running = CRIT;
  - client certificate expiry within 30 days = WARN, via `identity.check_client_cert`.
- **Exit codes:** 0 healthy, 1 unhealthy, 78 DB unusable. `--nagios`: 0/1/2/3, where **usage errors and 78 map to 3 (UNKNOWN), not 2**.
- **Prometheus:** names `zfsbackup_{snapshot,anchor,received}_age_seconds`, `_threshold_seconds`, `_check_state`, `zfsbackup_check_success`, `zfsbackup_build_info{version}`. Label values are escaped per the exposition format. `--output` is an atomic write.
- **Verify:** C13T (each output format, exit-code matrix, escaping); a real_zfs smoke test; two-VM check after scenario C: remote goes CRIT, then OK.

**C14. (optional) journald priority for CLI errors (S-13c)**
- **Tier:** low-risk. **Size:** XS.
- **Basis:** an ExecStartPre `migrate` failure lands at info.
- **Change:** a `<3>` prefix on error lines, only when `$JOURNAL_STREAM` matches stderr's dev:ino.

##### Lane 4: tests and workflows (`pytest-test-author`)

- **Per-item tests (C1T–C13T)** as listed above, each landing right after its item.
- **C19. Contract tests and latest-deps job (S-9b, Q12)**
  - Low-risk.
  - `inspect.signature` and behaviour checks for the werkzeug internals `_api_server_class` overrides, requests/urllib3 `cert_verify`/`init_poolmanager`, and simple-websocket 1.1's socket surface (extend `test_zfsbackup_tls_platform.py`);
  - a weekly unlocked "latest deps" job;
  - 3.15 in the matrix (C-Q3).
- **C20. CI package job**
  - Low-risk.
  - `poetry build`; assert the wheel contains `config/store/migrations/versions/*.py` and has no `packaging/` or `tests/`;
  - install into a clean venv without dev dependencies;
  - run `zfsbackupd --version` and `zfsbackup-config --version`, `import config.example.yaml`, `migrate --check`, and `--test-config` with stub `zfs`.
- **C21. Real-tier CI (S-10a)**
  - Spike per C-Q10, then nightly plus on-demand `ZFSB_REAL_ZFS=1 pytest -m real_zfs` on a file-backed pool.
  - Low-risk if hosted. A self-hosted runner is needs-approval.

##### Lane 5: scenarios (`real-zfs-scenario-dev`, zfsb-* VMs only)

**C22. `scenarios/test_install.sh` (B-8 verification)**
- Fresh VMs `zfsb-inst22` (22.04: systemd 249, drop-in, uv/deadsnakes 3.11) and `zfsb-inst24` (24.04: systemd 255); `--purge` deletes only those two.
- Build wheel and sdist on the host; install the venv, units, sysusers/tmpfiles; file-backed pool.
- Checks:
  - start before `import` fails cleanly (assert, no loop);
  - `import`, then `enable --now` reaches READY;
  - `reload`;
  - `--version` from both CLIs;
  - `journalctl -p err` shows a forced ERROR;
  - `check` exit codes and `--prometheus --output`;
  - `backup` as root and as a group operator;
  - clean `stop`;
  - **upgrade:** a wheel built at schema `a1c28e1b3f34` (`git` ref before `915642a`) upgraded to the current tree (head `7c3e9d51b2a8`, `migrate.py:157-158`): restart, pre-migration backup present, READY. If milestone B adds a schema revision (S-6b), use A-tree → B-tree instead.
- Reuse `test_config_permissions.sh` group (h)'s systemd helpers.

**C23. Two-VM scenario R (B-9)**
- In `test_two_vm_backup.sh`, `--only-r`:
  - **R1** file restore from an out-of-tree read-only clone (sha match); the clone pins only its snapshot (after B-4);
  - **R2** full restore in place, guids equal, then the next cycle is incremental (seed and rejoin);
  - **R3** no common snapshot: the documented rename-aside, then a full send succeeds. This may expose an unclear client error, which would be a follow-up code item;
  - **R4** raw restore with the key (only if S-6);
  - **R5** client DB+`.tls` restored from a C11 backup: same client id, no re-enroll.
- Also: the B-6 check after scenario C.

##### Lane 6: main session (docs outside `zfsbackup/`, S-15, S-10b)

**C24. Docs and ownership**
- Low-risk.
- `docs/release_checklist.md`:
  - mocked CI green;
  - OpenZFS matrix 21/22/24;
  - two-VM A–L1 + R;
  - `test_config_permissions.sh`;
  - the install scenario on both VMs;
  - version bump;
  - a protocol/schema bump note in the release notes;
  - record results in `two_vm_backup_report.md`.
- Rewrite `production_readiness_report.md` against this plan (H3, M6/M8 line refs, bind check, "development server" wording); `two_vm_backup_report.md` H5; the `tls_mtls_plan.md` status note.
- Drop the TrueNAS claim (C1) from `README.md:4-5` and `CLAUDE.md:72`.
- Update the CLAUDE.md package and test tables for the new modules, `packaging/` and `zfsbackupd`.
- Add ownership rows to `.claude/agents/README.md`: `packaging/**`, `pyproject.toml`, `poetry.lock` → zfsbackup-developer; `.github/workflows/**` → pytest-test-author; `protocol.py`, `version.py`, `logsetup.py`, `health.py`, `daemon_entry.py` → zfsbackup-developer.

---

#### Order and parallelism

1. **Wave 1, now (no milestone B dependency):**
   - C1 (core);
   - C8 ∥ C9 (store);
   - C19 and C21 spike (tests);
   - C24 ownership rows (main session).
   - Note: zfsbackup-developer is also milestone B's executor. C1–C4 touch no file B touches except `workers.py` (C3, `_setup_logging` and the handler class only), so they can be interleaved after B's core PR is frozen for review. I recommend not editing `workers.py` while B-5 is mid-edit.
2. **Wave 2:**
   - C2, C3, C4 (core, serial);
   - C10 after C1;
   - C11 after C8 + C4;
   - C20 after C1/C2/C10.
3. **Wave 3:**
   - C12 after C11;
   - C5 after milestone B's S-1 lands, then C13;
   - C6 after C2 + C12;
   - C14 optional.
4. **Wave 4:**
   - C22 after C6/C12/C13/C11/C3;
   - C7 README draft, then C23 (after B-5, and S-6 if it lands), then README fix-ups;
   - C24 reports.
5. **Review:**
   - zfs-code-reviewer on everything;
   - concurrency-reviewer on C2 (startup SIGHUP and the console script under fork/spawn/forkserver), C3 (child logging init), C8 (WAL backup), C4 (identity.lock), C12 (lock and workers-present ordering).

#### Where C depends on milestone B
- **C5/C13:** S-1's "newest" (createtxg) and its future-date rule; the S-6 raw-mismatch reason, if S-6 provides a marker.
- **C7 docs and runbook:** B-5's frozen flag set (`-L`) for restore and re-send; S-6 raw restore and key escrow; B-4 for clone-pinning behaviour; "changing send flags or `raw` needs a full re-send".
- **C22:** the upgrade test uses B's schema revision if S-6b exists.
- **C23:** R4 exists only if S-6 lands, and R2/R3 use B-5 flags.
- **C24:** H5 status in `two_vm_backup_report.md` after B-5.

#### Not doing now
- `.deb` (C-Q1).
- HTTP `/metrics` (K-9).
- A `restore` command (C-Q7).
- A shipped DB-backup timer: the docs show a cron/timer example and say to store the copy off-host.
- An API liveness probe inside `check` (daemon presence covers it).
- S-7 status file and S-8 skew guard: milestone D. The runbook's stop-before-install covers S-8 until then.
- A `zfsbackup` service user (C-Q8).
- Mount-namespace hardening: documented as why not.

---

#### For your approval

**Proposal:** 24 units in six lanes:
- daemon core C1–C7;
- store C8–C9;
- CLI C10–C14;
- tests C19–C21 plus the per-item tests;
- scenarios C22–C23;
- main-session docs C24.

**needs-approval:** C2 (entry-point signal handling), C4, C8, C11 (keys and store contract), C12 (Q11 migrate in ExecStartPre), the `ExecStartPre` line in C6, and a self-hosted runner in C21. Everything else is low-risk.

**Confirm:**
- C-Q1–C-Q12, above all C-Q2 (renaming the dist to `zfsbackup`) and C-Q6 (backup only when the schema is behind);
- that C5 waits for S-1;
- the new-module ownership rows.

Approve, adjust, reprioritise or drop any item; nothing starts before your sign-off.

**Key files:**
- /Users/rejsmont/src/libzfs/docs/production_readiness_plan.md
- /Users/rejsmont/src/libzfs/pyproject.toml
- /Users/rejsmont/src/libzfs/zfsbackup/__init__.py
- /Users/rejsmont/src/libzfs/zfsbackup/daemon.py
- /Users/rejsmont/src/libzfs/zfsbackup/workers.py
- /Users/rejsmont/src/libzfs/zfsbackup/identity.py
- /Users/rejsmont/src/libzfs/zfsbackup/backup_manager.py
- /Users/rejsmont/src/libzfs/zfsbackup/protocol.py
- /Users/rejsmont/src/libzfs/zfsbackup/README.md
- /Users/rejsmont/src/libzfs/zfsbackup/cli/main.py
- /Users/rejsmont/src/libzfs/zfsbackup/cli/status.py
- /Users/rejsmont/src/libzfs/zfsbackup/config/store/paths.py
- /Users/rejsmont/src/libzfs/zfsbackup/config/store/db.py
- /Users/rejsmont/src/libzfs/zfsbackup/config/store/migrate.py
- /Users/rejsmont/src/libzfs/zfsbackup/daemonlock.py
- /Users/rejsmont/src/libzfs/.github/workflows/tests.yml
- /Users/rejsmont/src/libzfs/scenarios/test_two_vm_backup.sh
- /Users/rejsmont/src/libzfs/scenarios/test_config_permissions.sh
- /Users/rejsmont/src/libzfs/scenarios/test_openzfs_matrix.sh
- /Users/rejsmont/src/libzfs/.claude/agents/README.md
- /Users/rejsmont/src/libzfs/CLAUDE.md
- /Users/rejsmont/src/libzfs/README.md

---

## Appendix C2. Milestone C addendum

### Milestone C plan: addendum on the milestone-D planner's input

This changes items in my plan (the report I sent before this one). It replaces nothing else, and I made no edits. The two headline answers: keep B-2a in D (B-9 is written so it holds before and after it), and pull D-0 forward into C.

#### 1. B-2a (`canmount=noauto`, `readonly=on`, `volmode=none`, `-h`, the `-x` resets): leave it in D

B-9 can be written so it is correct whether or not B-2a has landed. That keeps the restore docs and scenario R independent of the receive flags, so B-2a can stay in D. The one rule that makes this work: **operators never mount, `zfs set`, `zfs inherit` or modify a received dataset.** They work only from clones and sends.

**Changes to the B-9 part of C7 (`zfsbackup/README.md`, `zfsbackup-developer`):**

- **Reading a server copy (file-level restore):**
  - Use `zfs clone -o readonly=on -o canmount=on -o mountpoint=/mnt/restore/<x> <target>/<id>/<ds>@<snap> <pool>/restore/<x>`.
  - The clone must be **outside the target tree**. A clone takes its properties from its new parent, not from the origin, so it does not pick up `canmount=noauto`, the reset `mountpoint` or the server's local `org.zfsbackup:*` values.
  - For a zvol (`volmode=none` on the server): add `-o volmode=dev` to the clone and read `/dev/zvol/<pool>/restore/<x>`.
  - Destroy the clone when done. While it exists its origin snapshot cannot be pruned: after B-4 the pruner skips only that snapshot, before B-4 its whole batch.
  - `.zfs/snapshot` browsing on the received dataset itself is **not** a documented route. After B-2a the dataset is not mounted, and an explicit `zfs mount` of it would race the receives.
- **Full restore back to a client:**
  - On the server, `zfs send` **without `-p` and without `-R`**, using the frozen B-5 flags (`-L`, and `-w` for raw once S-6 lands). Pipe it to `zfs receive -u <pool>/<ds>` on the client.
  - Without `-p`, none of the server's local `readonly`/`canmount`/`volmode` values or the `org.zfsbackup:*` values travel.
  - If an operator used `-p` anyway, the client dataset arrives `readonly=on`/`canmount=noauto`/`volmode=none` as *received* values. The fix is `zfs inherit readonly` and `zfs inherit volmode` (plain inherit, which hides the received value), and `zfs set canmount=on`. Better: receive with `-x readonly -x canmount -x volmode -x org.zfsbackup:config -x org.zfsbackup:client_id -x org.zfsbackup:source_dataset`.
  - Raw restores: `zfs load-key` with the client's original key, then `zfs mount`.
- **Seed and rejoin (needs verifying in R2):**
  - An in-place restore lets the client resume incrementals only if it restores the **server's newest** snapshot.
  - If it restores an older one, the next client → server incremental has a base that is not the server's most recent snapshot. (Correction, 2026-10-22: this did NOT fail before B-2b. `-F` on a `-i` receive destroyed every server snapshot newer than the base, on 2.1, 2.2 and 2.4; captures M7. Since B-2b the server refuses it.) The runbook says to rename the server copy aside (see 2), or destroy the newer snapshots if they are expendable.
- **Holds:** after B-2a the server has none (`-h`), so restore needs no `zfs release` step.

**Changes to scenario R (C23, `real-zfs-scenario-dev`):**

- It asserts whichever semantics are present, detected from `zfs get -o value,source canmount,readonly` on the server copy.
- R1 adds: after the clone is destroyed, the next incremental receive still succeeds.
- R2 adds the "older snapshot cannot rejoin" negative case.
- R2 also checks that a `-p` restore without `-x` gives a read-only client dataset, as documented.
- The release checklist (C24) reruns R after D lands B-2.

#### 2. B-2b (`-F` only when the server dataset has no snapshots)

A full stream onto a server dataset that already has snapshots fails today ("destination has snapshots") and will still fail after B-2b. The runbook's "no common snapshot" recovery in C7 already handles this: seed from the server, or `zfs rename` the server copy **out of** the target tree.

**Additions to C7:**
- State explicitly that this case refuses rather than overwriting, both now and after B-2b.
- Name the client-side symptom (repeated per-cycle refusal; `check` shows remote lag going CRIT).

**Possible follow-up:** if R3 shows the client's error for this case is unclear, a clearer ERROR goes to `zfsbackup-developer` in D, alongside B-2b.

#### 3. S-8 needs from B-8a: provided as follows

- **C1 (`zfsbackup/version.py`, stdlib only):**
  - `installed_version()` re-reads `importlib.metadata.version(DIST_NAME)` on every call, with no module-level cache. `importlib.metadata`'s own path cache is keyed on directory mtime, so it sees a changed install.
  - It exports `DIST_NAME` (`"zfsbackup"` if C-Q2's rename is accepted) and `EXIT_CODE_VERSION_CHANGED = 76`. I checked that 76 is unused anywhere in `zfsbackup/` today; the CLI uses 75 and 78.
  - S-8 imports both rather than defining its own.
- **C6 (the unit):** `RestartForceExitStatus=SIGHUP 76`, with a unit comment saying what 76 means. The `pre-253.conf` drop-in does not override that line.
- **C2:** the daemon never exits with 76 for any other reason, and `--version` uses `installed_version()`.
- **C7 (runbook):** until S-8 lands, the rule is "`systemctl stop`, install, `systemctl start`". After S-8 an in-place install is safe: S-8 stops the daemon cooperatively and exits with 76, systemd restarts it, and ExecStartPre migrates. D's S-15 pass rewrites that runbook paragraph.

#### 4. S-7 status files and B-6 `check`: B-6 first, without the files

**Recommendation: B-6 goes first, in C, without reading status files.** S-7a then adds them in D.

**Reasons:**
- B-6 is a production blocker. S-7a is needs-approval (writes from workers) and has to go past the concurrency reviewer.
- Defining a file format in C with no writer would be speculative.

**Changes to C5 so D can extend it without rework:**

- **C5 is split in two:**
  - `zfsbackup/health.py`, **stdlib only** (the constraint S-7b needs). It holds the `LagEntry`/`CheckEntry` types and a pure `evaluate(observations, now, warn, crit)` over plain values (seconds and names, no `BackupConfig`). It also holds the JSON and Prometheus serialisers. S-7a later adds the `<db>.status/<worker>.json` path helper, the schema, and an atomic writer and reader to the same module.
  - `zfsbackup/health_collect.py`. This is the ZFS-reading collector over `DatasetManager` and `received_datasets()`, and it imports `libzfseasy` and `zfsbackup.config`. The `check` command imports it lazily.
- **C13 (`check`) contract:**
  - JSON carries `"schema": 1`, and changes to it are additive only.
  - The overall state is the worst over all entries, so S-7b's worker-outcome entries can only make a verdict worse.
  - Prometheus metric names are prefixed by source (`zfsbackup_snapshot_*`, `zfsbackup_anchor_*`, `zfsbackup_received_*`). S-7b adds `zfsbackup_worker_*` without renaming anything.
  - When S-7b lands, a missing `<db>.status/` directory is "no data" (no entry), not CRIT. An older daemon writes none.

#### 5. S-13 and the S-3/S-12 throttled WARNINGs

C3's formatter assigns the priority per record from its level, so any `logger.warning` gets `<4>` automatically, from any thread. That covers the per-connection handshake threads in the API worker.

**Additions to C3 (`zfsbackup-developer`):**
- In the `logsetup.py` docstring, state the contract for S-3 and S-12: emit through `logging` at WARNING (the periodic summary count at INFO or WARNING, S-3's choice); never `print`; never through the werkzeug access logger. C3's `log_request` demotion applies only to werkzeug's per-request line.
- Use the dedicated logger name `zfsbackup.api.peer` for per-peer throttled messages, so operators can filter them without losing priorities.

**C3T adds:** a WARNING logged from a non-main thread in an API-worker-shaped child (fork and spawn) arrives with `<4>` when the journald stream matches.

#### 6. D-0 (refuse a configured dataset equal to, below, or a recursive ancestor of the target): pull it forward into C as C0

**Recommendation: pull it forward into C, as C0.** The C runbook states the rule, and C is the first milestone that installs on real hosts. The hazard is concrete today, not only after B-2b:
- A `recursive: true` configured ancestor of the target runs `zfs snapshot -r` over every received dataset, putting server-prefixed snapshots onto them. After B-2b (no `-F`) the next incremental for every client then fails.
- Its `zfs destroy -r ancestor@name` also reaches received snapshots that have the same name.

**C0:** `zfsbackup-developer`, `config/model.py` `validate_config`, needs-approval (config contract), size S. It lands at the start of wave 2.

**C0 test table (C0T, `pytest-test-author`):**
- a dataset equal to the target;
- a child of the target;
- a recursive ancestor (refused);
- a non-recursive ancestor (allowed);
- a sibling sharing the target's name prefix (`tank/backup` vs `tank/backups`, allowed);
- remote backup disabled (no check).

`import` and reload reject such a config through the existing paths.

**If you keep D-0 in D:** C7 still documents the rule, worded as "not yet enforced", and D-0 must land before or together with B-2b.

**Docs (C7):**
- "The target must be a dedicated dataset" now spells out what that means: no configured dataset at, below, or recursively above it, and nothing that is not zfsbackup's placed under it.
- Add the reasons: the server enumerates everything under the target, and the snapshot and prune interaction above.

#### Net effect on the plan

- **New item:** C0 (needs-approval, if you accept the pull-forward).
- **Changed items:** C1 (`DIST_NAME`, exit code 76, no cache), C3, C5 (split into `health.py` and `health_collect.py`), C6 (`RestartForceExitStatus=SIGHUP 76`), C7 (B-9 text above), C13 (the JSON and metric contract), C23 (R1/R2 additions).
- **Order:** unchanged, except C0 joins wave 2 ahead of C5.
- **Two choices for you to confirm:** B-2a stays in D; D-0 moves into C.

Key files:
- /Users/rejsmont/src/libzfs/zfsbackup/config/model.py
- /Users/rejsmont/src/libzfs/zfsbackup/README.md
- /Users/rejsmont/src/libzfs/zfsbackup/api.py
- /Users/rejsmont/src/libzfs/zfsbackup/workers.py
- /Users/rejsmont/src/libzfs/zfsbackup/daemon.py
- /Users/rejsmont/src/libzfs/scenarios/test_two_vm_backup.sh

---

## Appendix D1. zfsbackup, milestone D

### zfsbackup implementation plan: milestone D (hardening for 1.0)

This is a proposal and nothing has been edited. It is planned against the uncommitted milestone-A tree on `prod-trust-boundary` (`git diff HEAD`). Line numbers below are from that tree.

#### Open decisions (recommendation first; answer "accept" to take all of them)

| # | Decision | Recommendation |
|---|---|---|
| **DD1** | What B-2 does at 1.0, given Q1 (a malicious enrolled client is out of scope) | **Do B-2a and B-2c, and do B-2b as a separately approvable item.** B-2a matters even without malice. Today server datasets are created mountable (`api.py:1946`, `:2163`), so `zfs mount -a` at boot mounts every client's data. Client uids then map to server uids, so the data is readable on the server, and mountpoint clashes can fail `zfs-mount.service`. A client zvol would also be probed by udev/LVM on the server (a VG name collision is possible). B-2b (stop using `-F` on incrementals) mainly defends against a malicious `-R -I` stream. It also stops `-F` from quietly rolling back and destroying server snapshots. If B-2b fails on any OpenZFS version, document it as a limitation instead. Accept? |
| **DD2** | B-3 (server retention floor) | **Defer the full floor (M/L size, schema revision 3, CLI) to after 1.0 and document it.** Instead, ship **B-3-lite**: negotiate logs a WARNING when the incoming config shortens the stored retention horizon. This covers the realistic accidental case (a client admin's typo) at S size. Q2 stays deferred. Accept? |
| **DD3** | Where L2 (hostile streams) goes | **With B-2, in this milestone.** The `real_zfs` tier is required. The two-VM L2 reuses L1's `sitecustomize` attacker harness. L2 tests exactly B-2's flags, and it was never started in A. Accept? |
| **DD4** | New item, my judgement (**D-0**): refuse a configured dataset that overlaps `remote_backup.target_dataset` | **Refuse it in `validate_config`.** This covers a dataset equal to or below the target, and a `recursive` dataset above it. Such a config today: `zfs snapshot -r` puts server snapshots into received datasets (the next incremental then fails, or `-F` rolls it back); the recursive prune `zfs destroy -r` removes received snapshots that have the same `{prefix}_{ts}` name (aligned timestamps make that likely); and it is the forging path the milestone-A review found for B-1d. Nothing is deployed, so there is no compatibility cost. The alternative is a WARNING only. Accept? |
| **DD5** | S-2: which sessions get cut | **Cut on `revoked`, `successor_revoked`, `client_revoked` and `replaced`. Do not cut on `retired` (a rotation) or on certificate expiry mid-stream.** The write-stall bound is `WS_IDLE_TIMEOUT`. Accept? |
| **DD6** | S-3: limits as constants or as config | **Constants, so there is no schema or config change.** Per IP on accept: burst 60, refill 10/s, bounded LRU of 4096 IPs. `/enroll`: 5 failures per 15 min gives 429 with `Retry-After: 900`. At most 2 `/enroll` requests in flight; beyond that, 503 with `retry`. Refusal logs: one WARNING per IP per hour plus an hourly count. Accept? |
| **DD7** | S-8: how skew is detected, and the exit code | **Compare the distribution version (`importlib.metadata`, from B-8a) plus `(st_ino, st_mtime_ns)` of the dist-info `RECORD`.** An editable or poetry install falls back to the package `__init__.py`. On a change: a cooperative stop, then exit with a dedicated code (proposed **76**) listed in `RestartForceExitStatus=`. No `execv`. Accept? |
| **DD8** | S-14: who adopts the libzfseasy timeouts | **Only the API request handlers' non-streaming calls, with a 30 s timeout giving 503 `{"retry": true, "reason": "zfs_timeout"}`.** The client already skips the cycle on any 503 carrying `retry` (`remote.py:101-108`), so the client needs no change. Workers get the watchdog only: no timeouts and no `StreamHandle.abort()`. That follows Q7 and the rule that running transfers are never cut. Accept? |
| **DD9** | S-7: where status lives | **`<db>.status/<worker>.json`.** The supervisor creates the directory (0750, the DB group); files are 0640 and written atomically (tmp file plus `rename`). No status is written under `--dry-run`. The new `zfsbackup/health.py` imports only the stdlib, so the CLI can import it. It goes to `zfsbackup-developer`; please confirm ownership of that new file, and of `protocol.py`. Accept? |

#### Work items

All items go to `zfsbackup-developer` unless marked otherwise. "CR" means `concurrency-reviewer` must also review the item. `zfs-code-reviewer` reviews every item.

##### D-0. Refuse a configured dataset that overlaps the remote-backup target (DD4)
- **Basis:** my own judgement (see DD4). The recursive descendant read at `backup_manager.py:100-116` and `:826` crosses into received datasets, and that is where the B-1d forging path comes from.
- **Owner:** `zfsbackup-store-developer` (`store/mapper.py:897` `validate_config`, next to the target checks at `:1092-1108`). After it, `pytest-test-author`.
- **Change:** an error for a configured dataset (enabled or not) that is equal to or below the target. An error for a `recursive: true` dataset that is a strict ancestor of the target. Compare by path components (`tank/b` must not match `tank/backup`).
- **Tier:** needs-approval (config load contract). **CR:** no.
- **Verify:** a table in `test_zfsbackup_config_audit.py` (equal, below, recursive ancestor, non-recursive ancestor allowed, sibling with a shared prefix allowed). Also `import` is refused with exit 1 and nothing is written (`test_zfsbackup_cli.py`), and the daemon's `--test-config` refuses it.

##### B-2. Server receive hardening (DD1)
`api.py` only. libzfseasy already supports `props`, `reset` and `holds` (`zfs.py:2000-2093`).

- **B-2a. Neutralise the received dataset.**
  - **Basis:** `api.py:2185-2188` receives with `props=owned` and `force=True`, with holds on by default; `zfs.create` at `:1946` and `:2163` creates mountable datasets.
  - **Change:**
    - add `holds=False` (`-h`);
    - extend `props` with `canmount=noauto` and `readonly=on`, plus `volmode=none` if the real tier accepts it on a filesystem receive. Otherwise set `volmode=none` once on the `<target>/<id>` container and let it inherit;
    - add `reset=[mountpoint, sharenfs, sharesmb, setuid, exec, devices]`. The three owned names stay `-o`: zfs refuses the same property in both `-o` and `-x`;
    - create server datasets with `canmount=noauto`;
    - resume must use the same flag set (one call already serves both modes, `:2183`).
- **B-2c. Stop adopting unowned datasets** (residual from A).
  - **Basis:** `_ownership_conflict` (`api.py:1178-1214`) treats a dataset with no properties as "not a conflict". An operator's existing `<target>/<id>` (or a dataset below it) is therefore adopted, and the full `-F` receive overwrites it.
  - **Change:**
    - negotiate creates the missing ancestors one level at a time (no `parents=True`) and stamps local `org.zfsbackup:client_id` on each one it creates;
    - an existing dataset with no local `client_id` gets 409, with a hint naming the `zfs set` the operator can run to adopt it on purpose;
    - `received_datasets` treats "client_id set, no config" as a container and logs it at DEBUG, not WARNING (`backup_manager.py:663-666`). This is the same owner, in the same pass.
- **B-2b. Use `-F` only when the server dataset has no snapshots.**
  - **Basis:** `force=True` is unconditional (`:2187`).
  - **Change:** count the server snapshots inside the per-dataset claim (`_claim_dataset`, `:2044`), so two streams cannot race. The pruner always keeps the newest snapshot, so the count never drops to zero underneath. A full stream onto a dataset that has snapshots then fails, as it should; that goes into the B-9 runbook.
  - **Edge cases:** a resumed full receive (the dataset still has no snapshots, so `-F` again, consistent with the first attempt); `mode=fresh` plus `receive -A` is unchanged; each B-5 chain step is an incremental, so it never gets `-F`.
- **B-3-lite** (DD2), `api.py` negotiate (around `:1916-1952`):
  - before overwriting `PROP_CONFIG`, decode the stored canonical config and compare the longest effective `keep_for` horizon;
  - if it shrinks, log one WARNING per (dataset, old horizon to new horizon);
  - it never refuses.
- **Tier:** needs-approval (remote/API). **CR:** no.
- **Depends on:**
  - S-6 (milestone B, raw sends). `-o` and `-x` must be checked on a raw receive. If S-6 has not landed, record that as an open check.
  - B-5 (`-F` against chain steps).
  - B-9 (milestone C): its restore docs must assume `canmount=noauto` and `readonly=on`. Either write B-9 against B-2a's semantics or pull B-2a forward; flag this to the C planner.
- **Verify:**
  - `pytest-test-author`, mocked: argv for fresh, resume and full-into-empty, and incremental without `-F`; the B-2c 409 matrix; the B-3-lite WARNING table (shrink, grow, equal, undecodable stored config). Files: `test_zfsbackup_api_receive.py`, `test_zfsbackup_api.py`.
  - `real_zfs` on 2.1, 2.2 and 2.4 (`zfsb-ci` plus the matrix VMs). Feed crafted streams: a `-p` stream with `mountpoint=/etc`, `canmount=on`, `setuid=on`, `sharenfs=on`; then run `zfs mount -a` and assert nothing is mounted or shared. A `-R -I` stream with a deleted snapshot must leave server snapshots intact. A `send -h` stream must leave `userrefs=0`. A zvol stream must end with `volmode=none` and no `/dev/zd*` partitions. Also a resumed full and a resumed incremental, and a raw stream if S-6 has landed.
  - `real-zfs-scenario-dev`: two-VM **L2**, reusing the L1 attacker. Also fix L1a's stale wording: `scenarios/test_two_vm_backup.sh:1631, :1909` and `scenarios/README.md` say "receive -x", but the code uses `-o`. The check `-s received` cannot see a received value hidden under a local one. Assert on the `received` column (`zfs get -o received`) and record the hidden value as expected.

##### B-1d. Move the property readers onto the framing-safe row API (X4)
- **Basis:** `backup_manager.py:45-116`. `_parse_zfsbackup_get` uses the private `_zfs_cmd` and `zfs.get._exec_out`, and passes `-s local,received`. That `-s` drops the `type` rows the lead-rule parser needs (`zfs.py:1292-1296`). It also reads the `received` column, which holds raw client bytes (the hidden-received residual from A). A forged row can overwrite a real anchor and so shrink a protect-set (fail-open). The same parser is behind `get_anchors` (`:513`), `_descendant_anchor_protection` (`:826`), `_ownership_conflict` (`api.py:1198`) and CLI rename (`cli/rename.py:159, 171, 874`).
- **Owner:** `zfsbackup-developer` (`backup_manager.py`), then `pytest-test-author`. CLI code is unchanged.
- **Change:**
  - `read_zfsbackup_properties(ds)`: `zfs.get.rows(ds, 'all')` with no `-s`; filter `local`/`received` in Python; use only `value` and `source`. If the object's group was dropped, or any row in scope was skipped, **raise**. Every caller already treats a raise as "unknown, do not act" (`:877-880`).
  - `read_descendant_zfsbackup_properties`: the same over `-r -t filesystem,volume`. A dropped descendant group raises, so the ancestor is skipped this cycle (fail-safe). D-0 removes the server-side case where this would happen.
  - `received_datasets` batching (the review's question):
    - replace the 2-3 `get.value` calls per dataset (`:643-655`) with one `zfs get -r -t filesystem name,type,client_id,config <target>`, without the `received` column;
    - take only `local` values; a `received`-source config with no local one gives the existing WARNING;
    - **fall back to today's per-dataset `get.value` path only for names the `zfs list` enumeration returned but the batch dropped**, so a forged group cannot make a victim's dataset vanish from pruning;
    - B-1c's checks (A7 grammar, `<target>/<id>` tree) are unchanged.
  - Delete `_parse_zfsbackup_get` and the `_zfs_cmd` import.
- **Contract this needs from lib S3** (send to `libzfseasy-implementation-planner`):
  1. Non-lazy, per-object grouped output with the lead rule, the contiguity rule (`twice`) and duplicate-property detection, exactly as `GetCommand._lines_to_objects`.
  2. With an explicit property list, a row for a property that was not requested is forged, so the group is dropped. Today it is accepted.
  3. A way to omit the `received` column (`-o name,property,value,source`).
  4. Skips reported to the caller: a `strict=True` that raises, or a returned skip set naming the dropped objects.

  If (3) is refused, B-1d still fails closed through (1), (2) and (4).
- **Tier:** needs-approval (the retention protect-set). **CR:** no.
- **Verify:**
  - Mocked, in `test_zfsbackup_manager.py`, `test_zfsbackup_prune_descendants.py`, `test_zfsbackup_prune_forged_bytes.py` and `test_framing.py`. Forged-row table: a received `anchor.*` with `\n` forging an earlier or later sibling, a duplicate anchor of the dataset itself, a forged `type` lead, a tab in the `received` column. Each must give "raise and skip prune" or "dropped group falls back per dataset", never a smaller protect-set.
  - The batch costs one exec for N datasets.
  - Update the fake-`zfs` argv expectations in `test_zfsbackup_cli_rename.py` and the three files that match on `local,received`.
  - `real_zfs`: a `send -p` stream carrying a newline `anchor.*` into a received dataset, with a recursive client-side ancestor.
  - Rerun two-VM L1 (`--only-l1`).

##### S-2. Mid-stream revocation and the write-stall bound (DD5)
- **Basis:**
  - A revocation only refuses new connections; the K5 scenario asserts that a running stream finishes.
  - A peer sending one frame every less than `WS_IDLE_TIMEOUT` (`api.py:2196`) holds `_claim_dataset` (`:1101-1110`) for ever.
  - `_SessionSocket.sendall` (`workers.py:992-1002`) blocks on a peer that does not read, while it holds `_write_lock`.
- **Owner:** `zfsbackup-developer` (`api.py` plus `workers.py`, same owner, one pass). Then `pytest-test-author` and `real-zfs-scenario-dev` (K5).
- **Change:**
  - a session registry in `create_app` keyed by the authenticated fingerprint (registered in `backup_stream`, unregistered in `finally`);
  - after each trust-view swap, the poll thread (`workers.py:1573`) asks `store.trust.list_revoked` (public, no store change) only about the fingerprints of live sessions that left the view;
  - `force_eof()` on the DD5 reasons;
  - `_SessionSocket` records when a write started (monotonic, under the write lock), and the same thread force-EOFs a write blocked longer than `WS_IDLE_TIMEOUT`;
  - `save=True` keeps the resumable state, and the claim is released in the existing `finally`.
- **Tier:** needs-approval. **CR:** yes (cross-thread `force_eof`, registry lifetime, `_store_call` lock).
- **Verify:**
  - Mocked: registry and reason matrix (`retired` is not cut).
  - Real TLS: a pinned client that does not read is cut within the bound (`test_zfsbackup_api_wssession.py`, `test_zfsbackup_api_tls.py`); the drain completes (`test_zfsbackup_stream_gate.py`).
  - Two-VM K5 becomes: the stream is cut within about 4 s, the token is kept, and the re-enrolled client resumes.

##### S-3. Anonymous flood control and log throttling (DD6)
- **Basis:**
  - Only concurrency caps exist (`workers.py:660-699`, and `process_request` at `:1269`), so handshakes per second are unbounded.
  - Every refusal logs a WARNING: `api.py:1618` (enroll), `:1811` (rotate), `:1341` and `:1358` (auth).
  - `/enroll` pre-checks go through `_store_call` (`api.py:361`, `:604`) together with the trust poll. Verify first whether a flood delays revocation pickup.
- **Owner:** `zfsbackup-developer`.
- **Change:**
  - a per-IP token bucket in `process_request`: O(1), never blocks, bounded LRU;
  - a per-IP failed-`/enroll` counter giving 429 with `Retry-After`;
  - an in-flight cap on `/enroll`;
  - a shared `_PerPeerThrottle` for the refusal logs plus an hourly count.
- **Tier:** needs-approval. **CR:** yes (accept thread, shared limiter state).
- **Verify:** mocked clock (bucket, 429, log counts); a real-socket flood on `zfsb-ci`; while the flood runs, a revocation is still picked up within about 2 s.

##### S-7. Record worker outcomes; surface resume-token failures (DD9)
- **Basis:**
  - `workers.py:655` drops the result of `backup_dataset()`; prune and snapshot errors only log.
  - `remote.py:1311-1318` skips the cycle with a WARNING when the token check fails, for ever.
- **S-7a** (`zfsbackup-developer`): the new `zfsbackup/health.py`. The supervisor creates `<db>.status/` and passes its path in the `Handoff` (`daemon.py`). Each worker writes last attempt, last success, last error and the consecutive-failure count per dataset and destination, plus the last prune result. After 3 consecutive skips caused by the resume token, log an ERROR once per (dataset, destination) until the next success.
- **S-7b** (`zfsbackup-cli-developer`, after S-7a and B-6b): `status` and `check` read the files through `health.py`'s reader.
- **Tier:** needs-approval. **CR:** yes (writers in several processes, atomic replace, permissions, fork/spawn/forkserver).
- **Verify:** `test_zfsbackup_workers.py` and a new `test_zfsbackup_health.py` (atomicity, mode and group, no files in dry-run, under all three start methods); CLI `status`/`check` tables; an ERROR-once test with a fake token failure.

##### S-8. Guard against code-version skew after an in-place upgrade (DD7)
- **Basis:** the lazy imports at `workers.py:621`, `:1583` and `:1650`. Spawn and forkserver import from disk.
- **Owner:** `zfsbackup-developer` (`daemon.py`, `workers.py`). Depends on B-8a (milestone C: one version source, and the unit's `RestartForceExitStatus=` in `packaging/**`).
- **Change:**
  - the supervisor records the signature at startup and re-checks it on its 10 s poll. That is a file read, with no `zfs` call;
  - on a change: a cooperative stop, then exit 76;
  - each worker compares the signature it imported with the `Handoff`'s at start. On a mismatch it exits with a new worker code, which counts toward no restart limit, and the supervisor starts the same stop.
- **Tier:** needs-approval (supervisor). **CR:** yes.
- **Verify:** mocked signature changes (supervisor and worker paths, not counted as restarts, transfers not cut); a real daemon whose signature file is touched mid-run exits 76. `real-zfs-scenario-dev` extends B-8's install scenario: upgrade the wheel under a running daemon, and systemd restarts it once.

##### S-12. Make TLS validity failures visible
- **Basis:**
  - A failed handshake is DEBUG only (`workers.py:1309`).
  - The client expiry check runs only at start and reload (`daemon.py:355`).
  - `trust list` already shows `not_after` (`cli/trust.py:67`), so that bullet only needs verifying.
- **Owner:** `zfsbackup-developer`.
- **Change:**
  - a WARNING when `SSLCertVerificationError.verify_code` is 10 (expired) or 9 (not yet valid), rate-limited per IP with its own small bounded map. S-3 can fold that map into `_PerPeerThrottle` later;
  - move the expiry check into `identity.py` and call it from the remote worker's `begin_cycle` (`remote.py:413`) once every 24 h;
  - document NTP.
- **Tier:** low-risk. **CR:** no.
- **Verify:** `test_zfsbackup_api_tls.py` (an expired client certificate gives one WARNING per IP); `test_zfsbackup_remote_tls.py` (a frozen clock gives one daily warning).

##### S-14. Adopt the libzfseasy timeouts (DD8)
- **Basis:** Q7 and X5. API handlers call `zfs` synchronously in `negotiate`, `resume_token`, `_ownership_conflict` and the stream pre-checks (`api.py:1925-1981`, `:2063-2169`, `:2254-2269`).
- **Owner:** `zfsbackup-developer`. **Depends on lib S2** (its exception type and API).
- **Change:** pass `timeout=30` to those non-streaming calls; map the timeout to 503 `{retry: true}`. The `zfs receive` stream itself gets no timeout. Workers get nothing beyond the watchdog's logging.
- **Tier:** needs-approval. **CR:** yes (a killed child versus a handler thread, no orphans).
- **Verify:** mocked timeouts give 503 with `retry`, and the client skips the cycle (`test_zfsbackup_remote_timeouts.py`); the orphan net stays green; optionally a real hang on the disposable `zfsb-hang` VM.

##### K-5. Remove the global filter `dedupe_store_warnings` installs
- **Basis:** `api.py:943-951` installs a filter on the store's loggers for the life of the process, and `tests/conftest.py:322-350` works around it.
- **Change:** return an uninstall handle, which `ApiWorker.run` (`workers.py:1650`) keeps. Then `pytest-test-author` removes the conftest workaround.
- **Tier:** low-risk. **CR:** no.
- **Verify:** `TestStoreWarningDedupe` passes with no conftest help.

#### Order and parallelism
- **`zfsbackup-developer` works serially**, because `api.py` and `workers.py` are shared:
  1. K-5, then S-12 (both low-risk; can auto-run under `/loop`);
  2. B-2a, B-2c, B-2b and B-3-lite;
  3. B-1d, after lib S3;
  4. S-2, then S-3 (both touch `ApiServer`);
  5. S-7a;
  6. S-8, after B-8a;
  7. S-14, after lib S2.
- **Running alongside:**
  - the store lane does D-0 right away. It should land before B-1d, because it removes the server-side descendant case;
  - the CLI lane does S-7b after B-6b;
  - `pytest-test-author` builds the L2 real-tier fixtures and the forged-row tables early;
  - `real-zfs-scenario-dev` does L2, the L1a wording fix, the K5 rewrite and the S-8 upgrade step.
- **Dependencies on the other planners:**
  - lib S3 is needed by B-1d (the contract above);
  - lib S2 is needed by S-14;
  - lib S1 (`LC_ALL=C`) must keep the English substrings of `_ABORT_ALREADY_CLEAN` working. B-2b decides `-F` from a snapshot listing, not from error text;
  - milestone B: S-6 (raw `-o`/`-x`), B-5 (`-F` against chain steps), B-4 (holds fallback; B-2a's `-h` mostly removes the cause);
  - milestone C: B-8a is needed by S-8; B-6b by S-7b; B-9 must assume `canmount=noauto`; S-13's journald priorities apply to the S-3 and S-12 WARNINGs;
  - B-7 gains the B-2 and B-1d assertions.

#### Deliberately not done or deferred
- **The full B-3 floor:** deferred (DD2). Document it: "the server applies the client's retention verbatim from the next negotiate".
- **Clearing the hidden `received` values left under `-o`:** no receive flag does it (`-x` cannot be combined with `-o` on the same property). The values are inert: B-1c reads local only, and B-1d stops reading the `received` column. Document it next to K-13.
- **Timeouts in the workers, and `abort()` on transfers:** not doing them (Q7, DD8).
- **Dynamic `anchor.*`/`inflight.*` names arriving in `-p` streams:** they can still pin snapshots (K-13, disk growth only). Unchanged.

#### For your approval
This proposes 11 items: D-0 (new, my own judgement), B-2a/b/c with B-3-lite, B-1d, S-2, S-3, S-7a/b, S-8, S-12, S-14 and K-5. Two of them are low-risk (S-12, K-5); the rest need approval. The concurrency reviewer must review S-2, S-3, S-7, S-8 and S-14. Under Q1, B-2 is kept because most of it also guards against non-malicious exposure (mounts, zvols, holds). The full B-3 is deferred behind a WARNING.

Please confirm or change:
- DD1–DD9 (or "accept all");
- ownership of the new `zfsbackup/health.py` (and the existing `protocol.py`) by `zfsbackup-developer`;
- that the S3 contract (four points) goes to `libzfseasy-implementation-planner`;
- that B-9 (milestone C) is written against B-2a's `canmount=noauto` and `readonly=on`.

Approve, adjust, reorder or drop any item; each one stands alone.

**Key files:**
- `/Users/rejsmont/src/libzfs/zfsbackup/api.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/backup_manager.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/workers.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/remote.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/daemon.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/identity.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/store/mapper.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/store/trust.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/rename.py`
- `/Users/rejsmont/src/libzfs/libzfseasy/zfs.py`
- `/Users/rejsmont/src/libzfs/tests/conftest.py`
- `/Users/rejsmont/src/libzfs/scenarios/test_two_vm_backup.sh`
- `/Users/rejsmont/src/libzfs/scenarios/README.md`
- `/Users/rejsmont/src/libzfs/docs/production_readiness_plan.md`

---

## Appendix D2. libzfseasy, milestone D

### libzfseasy milestone D (hardening for 1.0): implementation plan, for your approval

This is a proposal. I edited no files. The baseline is `prod-trust-boundary` with milestone A uncommitted (`git diff HEAD -- libzfseasy/`). All line numbers below refer to that working tree. I checked every "already done" claim in the code itself rather than taking the list you sent.

#### 0. What milestone A already did (checked in the code)

| Plan item | Status | Evidence |
|---|---|---|
| S1: `_exec` has no try/finally | **open** | `zfs.py:278-287` |
| S1: unbounded `stderr_thread.join()` | **open** | `zfs.py:322` (`_exec_out`) and `:546` (`_exec_capture`). `_exec_lines` already uses the bounded join (`:351,:362`). |
| S1: the text stderr reader is unbounded and dies on a decode error | **open** | `_start_stderr_reader` `:438-452`, used by `_exec`, `_exec_out` and `_exec_collect` (`:418`). The stdout of `_exec`/`_exec_out` is also strict-decoded in the locale's encoding (`:281,:292`). |
| S1 / Q6: `LC_ALL=C` everywhere | **open** | Only `resume_info` sets it (`:1850`). Two tests currently *require* that nothing else gets an env: `tests/test_commands.py:3520-3546`. |
| S1 / Q6: stdout decoded with `surrogateescape` | **partly done** | Done in `_exec_lines` (`:325-365`, used by `list`/`get`/`get.value`). Not done in `_exec`, `_exec_out` or `_exec_collect`. |
| S1: message when a failure has no stderr | **open** | Empty message at `:365`, `:548`, `:1983`, `:637`. `_exec_stream_in` (`:2136`) and `resume_info` (`:1874`) already fall back to `Command failed with exit code N`. |
| S5: `_send` returns None for a Bookmark | open | `:1720-1724` |
| S5: `destroy.snapshots([])` raises `AttributeError` | open | `:1078-1089` (`ds.name` on None) |
| S5: `exists()` rejects a Bookmark | open | `__init__.py:47-54` goes through `_dslist_to_list` (`zfs.py:686-696`) |
| S5: `load_key`/`unload_key` docstrings | open | `:2145-2171`, `:2190-2213` |
| S5: `send.resume` has no token check | open | `:1812-1825`; `resume_info` checks at `:1843-1845` |
| S5: `Validate.source` says "type" | open | `types.py:131-133` |
| S5: `_lines_to_objects` uses `name`, not `dsname` | **fixed by A** | `:1508`, `:1537` |
| S6: warn once per process | **mostly fixed by A** | The registry is now cleared hourly (`_WARNED_NAMES_TTL`, `:58-88`). Remaining gap: unrepresentable-name skips are not counted in the per-listing `_SkippedRows` summary (`:874-876`, `:1571-1573`). |
| Lead rule, no `-s` in `GetCommand`, `get.value`, `-` prefix and 255-character caps on property names, `_redact` clip, receive name validation | done by A | `:1292-1296`, `:1474-1539`, `:1366-1413`, `types.py:141-152`, `:217-233`, `:2079-2089` |

#### 1. Open decisions (each has a recommendation)

- **DD1. Move S1's `LC_ALL=C` part (S1a) into milestone B, ahead of lib B4 and zb B-4.**
  - Why: `exists()` already matches the English "dataset does not exist" (`__init__.py:51`). A systemd service inherits the system locale, so on a non-English host `exists()` raises today instead of returning False.
  - B-4 will match `destroy` failure text, and B4 should capture formats under the locale the library will actually use.
  - Recommendation: yes. **Accept the recommendation: [ ]**

- **DD2. Rebuild `_exec` and `_exec_out` on top of `_exec_lines`.**
  - The plan text said "have `_exec` consume `_exec_out`". `_exec_lines` is the better base: it already reads bytes, decodes with surrogateescape, keeps a bounded error list, joins the stderr reader with a bound, and terminates and reaps the child in its finally block.
  - Behaviour change 1: an interrupt during a *write* command (`set`, `destroy`, `create`) now terminates the child with TERM, then KILL after 5 s. Today the child is left running. This matches what the CLI's Ctrl-C already does (C6).
  - Behaviour change 2: `\r` stops being a line break inside `_exec_out` (universal newlines are gone).
  - Recommendation: yes. **Accept: [ ]**

- **DD3. Shape of the timeout API (S2b).**
  - (A) A context manager `with zfs.timeout(30):`, stored in a contextvar. It is read when each child starts, applies per command, and covers non-streaming commands only. No command signature changes.
  - (B) A `timeout=` keyword on every non-streaming command, about 19 signatures.
  - The zfsbackup planner's S-14 currently assumes `timeout=30` per call. With (A), S-14 wraps each handler body in the context manager instead.
  - Recommendation: (A); (B) can be added later without breaking anything. **Accept (A): [ ]**

- **DD4. Which calls are refused while a timed-out child is still alive (D state).**
  - Only calls made under a timeout are refused, and they fail fast. Calls without a timeout behave exactly as today, as Q7's "off by default" requires.
  - Recommendation: yes. **Accept: [ ]**

- **DD5. New exception types.**
  - `ZFSTimeoutError(Exception)` for S2, and `ZFSOutputError(Exception)` for strict `rows()`.
  - Neither subclasses `ValueError`, `OSError` or the built-in `TimeoutError`, so existing `except Exception` handlers still catch them and no `except OSError` catches them by accident.
  - Recommendation: as stated. **Accept: [ ]**

- **DD6. How `rows()` reports skipped output (S3; this answers B-1d's point 4).**
  - It returns `GetRows`, a list of `GetRow(name, property, value, received, source)` with three attributes: `.complete` (bool), `.dropped` (frozenset of names) and `.skipped` (int).
  - `strict=True` raises `ZFSOutputError` instead.
  - `verify_names=True` is the default. It cross-checks every name against a `zfs list` with the same roots, `-t`, `-r` and `-d` (§3, S3).
  - Recommendation: as stated. **Accept: [ ]**

- **DD7. Defer `StreamHandle.close(timeout)` and `abort()` (D2(c)).**
  - Q7 says no abort of running transfers, and nothing would use them.
  - Recommendation: defer. **Accept: [ ]**

- **DD8. `load_key`/`unload_key` (S5): fix the docstrings and keep `ds` required.**
  - A `None` default would turn a bare `zfs.load_key()` or `zfs.unload_key()` into `-a`, which acts on every dataset.
  - Recommendation: fix the docstrings only. **Accept: [ ]**

- **DD9. How S4 detects JSON support, and a way to turn it off.**
  - Probe once with `zfs version -j`, cached per resolved binary path (`ZFS_CMD` can change at runtime). Add a module setting `zfs.json_output = 'auto' | True | False`.
  - Use JSON only when `output_version` is one we recognise; otherwise fall back to `-H` with a single WARNING.
  - Recommendation: as stated. **Accept: [ ]**

#### 2. Library work milestone B needs first

- **S1a (`LC_ALL=C`)** comes before zb B-4 (destroy failure text) and before the lib B4 format fixes; this is DD1.
- **S5's `destroy.snapshots([])` → `ValueError`** comes before zb B-4's one-snapshot-at-a-time retry, so that an empty batch fails clearly.
- **Nothing else.**
  - zb S-1 can already list and sort by `createtxg` (`types.py:15,39`).
  - zb B-5 and S-6 use send options that already exist (`zfs.py:1896-1929`); the resume-token flag names (`largeblockok`) belong to B4.
- **`receive -A`:** with `LC_ALL=C` zfs prints its untranslated English messages, so the "already clean" match in `api.py:93-96` gets *more* reliable, not less. A `real_zfs` test of `receive.abort` in both states under the new environment proves it on 2.1, 2.2 and 2.4 (T1 or B4).

#### 3. Work items, in order

`libzfseasy-developer` works serially, one commit per item: everything is in `zfs.py`. `pytest-test-author` writes tests in parallel against each agreed contract. That author is editing tests right now, so the S1 test changes below must be coordinated with them.

##### D1. S5: small correctness fixes. low-risk; no concurrency review.
- **Basis:** the S5 rows of §0. One of them (`dsname`) is already fixed and is dropped.
- **Changes:**
  - `_send` raises `ValueError` for anything that is not a Dataset or Snapshot (`:1720-1724`).
  - `destroy.snapshots` raises `ValueError` on an empty iterable (`:1078-1089`).
  - `exists()` handles a Bookmark through `zfs.get.value(bm, 'type')`; every other type keeps the `list` path (`__init__.py:47-54`).
  - `send.resume` applies the same token check as `resume_info`, through a shared `_check_resume_token` (`:1812-1825`).
  - `Validate.source` says "source" in its message (`types.py:133`).
  - Docstrings: `load_key`/`unload_key` (DD8) and `change_key` (it says `format`, the parameter is `fmt`; `:2223-2234`).
  - My own addition: validate `iterations` as an int (`:2269-2270`). It is the one `-o` value that skips B1's check.
- **Verify:** mocked; Popen is never called on bad input. `pytest tests/test_types.py tests/test_commands.py tests/test_commands_part2.py`. A `real_zfs` test of bookmark `exists()` on any zfsb VM.

##### D2. S1a: locale, decoding and failure messages. needs-approval (exec contract); concurrency review: yes (light). Lands in milestone B per DD1.
- **Basis:** the S1 rows of §0, and Q6.
- **Changes:**
  - Add one `_child_env()` helper returning `dict(os.environ, LC_ALL='C', LANG='C')`. Pass it to every Popen: `_exec_lines`, `_exec_collect` when `env` is None, `_exec_stream` (`:1936`), `_exec_stream_in` (`:2127`), plus `_exec`/`_exec_out` until D3 replaces them.
  - Add one `_failure_text(errors, rc)` helper, used only when stderr is empty: `Command failed with exit code N`, or `Command killed by signal SIGPIPE` when rc < 0. This keeps the wording already pinned at `tests/test_commands.py:2931,3303`. Apply it at `:365`, `:548`, `:1983` and `:637`.
- **Edge cases:**
  - A caller's own `LC_ALL` is overridden; document this.
  - zfs prints raw bytes under `C`. The B2 `\xff` real test must still pass.
- **Tests to change:** `tests/test_commands.py:3520-3546` (they assert no env) are inverted.
- **Verify:**
  - Mocked: the env reaches every Popen.
  - Real child, no pool: a `sys.executable` child that is killed by SIGPIPE.
  - Real tier on zfsb-zfs21/22/24: the existing T1 and B2 framing tests, plus a non-ASCII user-property round trip.

##### D3. S1b: one exec core and bounded stderr. needs-approval (process cleanup); concurrency review: yes.
- **Basis:** `_exec` has no cleanup (`:278-287`); unbounded joins (`:322,:546`); the text stderr reader (`:438-452`). Approach per DD2.
- **Changes:**
  - `_exec` drains `_exec_lines`.
  - `_exec_out` yields the stripped, non-empty lines of `_exec_lines`. zfsbackup still calls it directly (X4), so its contract stays: stripped, non-empty lines.
  - `_exec_collect` uses `_start_stderr_reader_lines` in its non-raw mode. Its stdout keeps `errors='replace'`, which is pinned at `tests/test_commands.py:3499-3503`; only `resume_info` uses it and the token contents are ASCII.
  - `_exec_capture` and `_start_stderr_reader` are retired, or reduced to shims if the test author prefers. Five references in `tests/test_commands.py` and the ones at `:1171` and `:1319` move to the new path.
- **Verify:** real-child tests (no pool):
  - invalid UTF-8 on stdout and on stderr;
  - 200 KB of stderr;
  - KeyboardInterrupt during `_exec`: the child is reaped and nothing is orphaned;
  - an abandoned `_exec_out` generator.
  - The suite's orphan net must stay green.
  - Mocks cannot prove the pipe behaviour; these must be real children.

##### D4. S6: name-limit warnings. low-risk.
- **Basis:** the hourly TTL from A already fixes "goes quiet". The remaining gap is that unrepresentable names are not part of each listing's count.
- **Change:** pass `skips` into the `InvalidNameError` paths (`:874-876`, `:1571-1573`) and have `_SkippedRows.done()` add "N names libzfseasy cannot represent". The per-name warning dedup stays as it is.
- **Verify:** mocked, `tests/test_framing.py`.

##### D5. S3: `zfs.get.rows(…)`. low-risk (new API; the shared refactor must not change behaviour). Concurrency review: no.
- **Basis:** zfsbackup calls the private `_exec_out` and `_zfs_cmd` and runs its own forgeable parser (`backup_manager.py:15, 45-116`), and that parser passes `-s local,received` (`:94,:110`), which breaks the lead rule.
- **Design:**
  - Split `_lines_to_objects` (`:1474-1539`) into a shared group iterator: scope, lead rule, contiguity (`twice`), duplicate property, type row. `get()` builds objects from those groups; `rows()` flattens them.
  - `rows()` **never passes `-s`**; sources are filtered in Python, with `inherited` also matching `inherited from …` as `get()` does today.
  - It is non-lazy only. Values come back raw: `-` is not mapped to None, and invalid bytes are kept through surrogateescape.
  - **It does not refuse names libzfseasy cannot represent.** No object is built, and dropping such a descendant would hide its anchors from the protect set, which fails open.
- **Answer on the residual you asked about:** yes, `verify_names` closes it.
  - A forged whole object that zfs never printed (a child filtered out by `-t`, or a self-contained forged group with nothing real to collide with) is not in the name-only `zfs list -H -o name,type`. Names cannot contain a tab or newline, so that listing cannot be forged.
  - A forged group under a real name is already dropped by the contiguity rule.
  - **What remains:** with `'all'`, a value can still forge *extra* rows inside its own object's group. That only affects the forger's own object, never another tenant's. With an explicit property list it is closed too (see point 2 below). On 2.3+ S4 closes it completely.
  - TOCTOU: a dataset created between the two calls is dropped and `complete` is False, so the caller skips one cycle. That fails safe.
- **Must verify on the real tier:** that `zfs list` and `zfs get` iterate the same set for the argument shapes zfsbackup uses (`-r -t filesystem,volume all ds`, a single dataset with `all`, and the batched explicit list), on 2.1, 2.2 and 2.4.

**The B-1d contract, point by point:**
1. **Accepted.** `rows()` uses the same group iterator, so it applies the lead rule, the contiguity rule and duplicate-property detection exactly as `get()` does, non-lazily.
2. **Accepted.** With an explicit list, a group that contains a property that was not requested is dropped. Combined with the duplicate rule, this closes in-group forging for explicit lists: a forged copy of a requested property becomes a duplicate, and any other property is unrequested.
   - I propose one further check behind a real-tier gate: require the exact sequence `name, type, p1, p2, …`. That depends on zfs printing every requested property for every object (`-` when unset or not applicable), which is unverified on 2.1, 2.2 and 2.4.
   - The same check goes into `get()` too, since it is the same iterator. Mocked tests in `test_framing.py` need an "unrequested property" case.
3. **Accepted**, as `received=False`, which sends `-o name,property,value,source`.
   - Then `GetRow.received` is None.
   - Tab recovery also becomes unambiguous, because there is only one middle column (`_split_row`, `:1450-1472`).
   - Caveat: this only hides payloads that sit in the received column. When the source *is* received, the value column carries the payload itself.
4. **Accepted**, per DD6.
   - `.dropped` cannot always name the object: a short fragment row identifies no object it can be blamed on. So **safety decisions must use `.complete`** (or `strict=True`), and `.dropped` only to target re-reads.
   - B-1d's planned fallback (re-read with `get.value` any name the `zfs list` enumeration returned but the batch dropped) is correct. For that batch, B-1d may pass `verify_names=False`, since it does its own listing.
- **Verify:**
  - Mocked: the byte sequences in `tests/test_framing.py`, plus a forged whole object, an unrequested-property row, received-only and local-over-received values, an unrepresentable name kept, and `complete`/`strict`.
  - Real tier on zfsb-zfs21/22/24: a crafted `zfs send -p` stream whose `org.zfsbackup:*` values contain newlines and tabs, on a descendant, with `-r -t filesystem,volume`.

##### D6. S2a: slow-command watchdog. needs-approval; concurrency review: yes.
- **Basis:** nothing in the exec path is bounded: the `readline` and `wait` in `_exec_lines` (`:341,:350`) and the `wait()` in `StreamHandle.close` (`:608`).
- **Change:**
  - In the single core from D3, arm a `threading.Timer` per call. It logs a WARNING at `zfs.slow_command_warning` seconds (default 60; None turns it off), with the redacted argv, pid and elapsed time, and re-arms at each further multiple. It is cancelled in the finally block.
  - Behaviour does not change.
  - It also covers the post-EOF `wait()` in `StreamHandle.close`, **never the transfer itself**.
- **Review points:** a Timer thread across fork (it does not survive in the child; logging's fork handlers reset the logging locks), and the cost of one thread per call.
- **Verify:** real-child tests, with a threshold of 0.2 s: a child that sleeps logs the warning once per interval and the timer is cancelled on exit.

##### D7. S2b: opt-in timeouts. needs-approval (pipes, fds, process cleanup); concurrency review: yes.
- **Design (DD3–DD5):**
  - While a timeout is active, stdout is read with `selectors` plus `os.read` against the deadline. A blocking `readline` cannot be interrupted, and a D-state child (or a grandchild holding the pipe) never delivers EOF.
  - On expiry: TERM, wait up to 2 s, KILL, wait up to 2 s.
  - If the child is still alive, it goes into a locked `_stuck` registry. Its stdout read end is closed: this thread is its only reader. **Never close stderr**: the daemon reader thread may be blocked on it, and closing a busy fd risks fd reuse (the same class as the websocket bug). Then raise `ZFSTimeoutError` with "timed out after Ns; outcome unknown".
  - Admission: a call under a timeout first sweeps `_stuck` (poll, then close the pipes of children that have exited). If any remain, it raises immediately without starting a child.
  - `os.register_at_fork(after_in_child=…)` clears the registry in children.
  - Streaming commands ignore the timeout.
- **Verify:**
  - Real-child tests: a child that never exits; a child that ignores SIGTERM; a child whose grandchild holds stdout open (the call must return at the deadline; the test kills the grandchild itself); concurrent timed calls from several threads.
  - Registry and admission logic: a fake process object, since D state cannot be simulated in a test.
  - Real hang: `real-zfs-scenario-dev`, on a **disposable `zfsb-hang`** VM: a pool on a dm device with `failmode=wait`, then `dmsetup suspend`. Time `zfs snapshot`/`zfs set` (they wait on txg sync); `zfs list` may not hang. The VM may need a reboot. Never `foam` or `zfs-test`.

##### D8. S4: `zfs list -j` / `zfs get -j` (OpenZFS ≥ 2.3). needs-approval; concurrency review: no.
- **S4a (`real-zfs-scenario-dev`):** extend the format capture in `scenarios/test_openzfs_matrix.sh` (`:272-274`) to save, on zfsb-zfs24:
  - `zfs version -j`;
  - `list -j` and `get -j` with values containing newlines, tabs, `\xff` and `"`;
  - every source type (local, inherited, received, default, none);
  - `-s` sort order;
  - whether `get -j` has an equivalent of the `received` column.
  - On 2.1 and 2.2, record the usage text and rc that make the probe fall back.
  - I do not have the T2 logs locally. **Assumption:** the developer works from these captures, not from my notes.
- **S4b (developer):**
  - Probe per DD9.
  - Decode with surrogateescape, then `json.loads` with an `object_pairs_hook` that rejects duplicate keys (fail closed).
  - Map types from upper case and sources to their `-H` strings (`INHERITED` + data → `inherited from X`).
  - Feed both `_list` (`:750-802`) and the S3 group iterator, keeping the scope and type checks as defence in depth. Do not use `-p`, so values match the `-H` path.
  - Lazy mode parses the whole document, then yields.
- **Verify:** a parity test (the same pool read through `-j` and `-H` gives the same objects and rows) on zfsb-zfs24; the fallback on zfsb-zfs21/22; mocked tests from the captured documents.
- **Decided 2026-10-25: deferred past 1.0, no `-j` code.** The S4a captures (`tests/fixtures/openzfs/*/M9.txt`,
  section M9 of `scenarios/test_openzfs_matrix.sh`) refute the premise that `-j` is a safer path:
  1. on 2.1/2.2 `zfs version -j` exits 0 and prints plain text, so a probe must parse stdout, not trust the rc;
  2. on 2.4.1 `-j` output is silently truncated (rc 0, empty stderr, invalid JSON) at the first value it cannot
     encode: any `\xff` in every locale, and any non-ASCII value under `LC_ALL=C`, which libzfseasy always sets;
  3. `--json-int` clamps guids at or above 2^63 to 9223372036854775807;
  4. there is no equivalent of the `received` column (a `RECEIVED` source shows its value only with
     `-o received`, and a locally overridden received value is not shown at all);
  5. a multi-target `-j` call with one missing target prints nothing for the others (rc 1).
  The only gain would be ASCII values containing a newline or tab, which the `-H` path already refuses fail-closed.

#### 4. Deferred (on purpose)
- D2(c) `StreamHandle.close(timeout)` / `abort()` (DD7).
- `-j` for `get.value`: it is already exact.
- A `timeout=` keyword on each command (DD3 (B)), unless S-14 insists.
- C2 (pool-name regex, `types.py:94`) and C4 (lazy binary resolution, `zfs.py:18-32`): document only.

#### 5. "Can ship without": rechecked
- C1–C8 still hold: TrueNAS claim at `README.md:5` and `CLAUDE.md:72`; `types.py:94`; the 255 cap; `zfs.py:26-32`; `-` → None at `:869`; C6; `__init__.py:30-45`; C8.
- Add three D limitations to the same doc pass:
  - timeouts free threads, not D-state processes;
  - on 2.1/2.2 a `rows(...,'all')` value can forge rows inside its own object's group;
  - `LC_ALL=C` overrides the caller's locale.
- Owner: the main session.

#### 6. Notes for the zfsbackup planner
- X1 and X2 are done in A (B-1a–c). X3 → B-4. X4 → B-1d, which needs D5. X5 → S-14, which needs D7.
- **B-1d:**
  - Treat `complete=False` as an unknown protect set and skip the recursive prune.
  - Never pass `-s`.
  - Expect unrepresentable names as raw strings.
  - Also move `read_zfsbackup_properties` (CLI rename) onto `rows()`.
- **S-14:** wrap API handler bodies in `zfs.timeout(30)` (DD3) and map `ZFSTimeoutError` to 503 with Retry-After. A write that times out has an unknown outcome. Never wrap receive streams.
- **B-4:** after S1a, error text is always in the C locale.

#### 7. Owners, review and parallel work
- `libzfseasy-developer`, serially: D1 → D2 (in milestone B) → D3 → D4 → D5 → D6 → D7 → D8.
- `pytest-test-author`: works in parallel per item, after syncing with the ongoing test edits (D2 flips the env tests; D3 retires the `_exec_capture` tests).
- `real-zfs-scenario-dev`: D7 hang scenario, D8a capture, and the matrix reruns.
- `concurrency-reviewer`: D2, D3, D6, D7.
- `zfs-code-reviewer`: everything.

#### For your approval
I propose eight items:
- **Low-risk:** S5 and S6, plus S3 (new API).
- **Needs approval:** S1 in two parts (S1a moved into milestone B), S2 in two parts, and S4.
- S3 accepts all four B-1d contract points, with a proposed exact-sequence check that depends on real-tier verification. Its `verify_names` cross-check closes the forged-whole-object residual; what remains is a value forging rows inside its own object's group with `'all'` on 2.1/2.2.

Please confirm DD1–DD9. DD3 is the one that needs reconciling with the zfsbackup S-14 plan (a context manager vs `timeout=30` per call). One assumption: the T2 `-j` captures do not exist locally yet, so S4 starts with S4a.

Approve, adjust, reprioritize or drop any item; each stands on its own.

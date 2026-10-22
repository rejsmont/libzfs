# Production-readiness plan (libzfseasy + zfsbackup)

**Status:** proposal, nothing implemented. Drafted 2026-10-21 against `master` at `609df66`, after PR #4
(milestone 3, TLS/mTLS) was merged. It covers both packages. Part 1 combines the two planners' output: the
decisions you need to make, the blockers and the order of work. Parts 2 and 3 are the full per-package plans,
each item with file references, owner, risk tier, size and how it will be checked.

## Headline finding

Every row of `zfs list -H` / `zfs get -H` output that libzfseasy parses can be **forged by an enrolled client**.
ZFS accepts newlines, tabs and invalid UTF-8 in user-property values, and `-H` prints them raw. The server
stores the client-supplied `org.zfsbackup:config` unvalidated, and the same values also arrive as received
properties in a `zfs send -p` stream. A crafted value adds a fake row to the server's received-dataset listing,
and the server pruner then applies the attacker's retention to **another client's** backups. A malformed row
also currently crashes the listing, which stops server pruning for every client. This was reproduced on
OpenZFS 2.4.1. It is the top blocker, and it needs fixes in both packages: libzfseasy B1–B3, then zfsbackup B-1.

## Decisions taken (2026-10-21)

- **Q1: no.** A compromised or malicious enrolled client is **out of scope**. So zfsbackup **B-2 (receive hardening)
  and B-3 (server retention floor) move from blockers to "should fix before 1.0"**. B-1 stays a blocker, because
  the injection is cross-tenant and can also be triggered by accident (a newline in a config value).
  Q2 (the floor's default) is therefore deferred together with B-3.
- **Q5:** recommendation accepted. Test OpenZFS 2.1, 2.2 and 2.4; 2.3 is best-effort.
- **Q9:** accepted. Intermediate snapshots go as a chain of `-i` sends over managed snapshots; the send-flag set is frozen before the first deployment.
- **Q10:** accepted. Per-remote `raw: auto`.
- **Q11:** accepted. The systemd unit runs `zfsbackup-config migrate` in `ExecStartPre`, after a DB backup.
- **Q3, Q4, Q6, Q7, Q8, Q12, Q13, Q14, Q15:** the user did not object, so the recommendations stand.

## Decisions needed (each with a recommendation)

| # | Decision | Recommendation | Affects |
|---|---|---|---|
| Q1 | Threat model: is a compromised or malicious **enrolled** client in scope (ransomware on a client)? | **Yes.** That makes B-2 (receive hardening) and B-3 (server retention floor) blockers. B-1 is a blocker either way, because it is cross-tenant. | zb B-2, B-3 |
| Q2 | Retention floor default | An explicit `remote_backup.min_retention` setting with no implicit default, plus a WARNING at server start when remote backup is on and no floor is set. | zb B-3 |
| Q3 | Property values libzfseasy refuses on write | Refuse `\n \r \t \0` in every value passed to zfs, raising `ValueError` before the command runs. | lib B1 |
| Q4 | Malformed `-H` rows on read | Skip the row with a warning, never abort the listing. Drop both rows when a name appears twice. Skipping fails safe for pruning: it keeps more. | lib B2 |
| Q5 | Supported OpenZFS versions | Test 2.1 (Ubuntu 22.04), 2.2 (24.04) and 2.4 (26.04). 2.3 is best-effort and untested. macOS is dev-only; FreeBSD is unsupported. | lib B4/T2 |
| Q6 | Run every `zfs` command under `LC_ALL=C`, with stdout decoded as UTF-8 `surrogateescape` | Yes. zfsbackup already matches English error text. | lib S1 |
| Q7 | Hung pool | A slow-command WARNING watchdog, plus opt-in timeouts for non-streaming commands (off by default; used by API request handlers). **No** abort of running transfers. | lib S2, zb S-14 |
| Q8 | `zfs list -j` JSON parsing (OpenZFS ≥ 2.3) for 1.0 | Yes, as should-fix rather than a blocker, keeping the `-H` fallback for 2.1/2.2. | lib S4 |
| Q9 | How intermediate snapshots reach the server | A chain of `-i` sends over **managed** snapshots only, not `-I` (which also ships unmanaged snapshots that the server never prunes). Also freeze the send-flag set (`-L`) before the first deployment. | zb B-5 |
| Q10 | Raw sends (`-w`) for encrypted datasets | A per-remote `raw: auto` setting (raw if and only if the dataset is encrypted). Decide before the first real deployment, because switching later needs a full re-send. | zb S-6 |
| Q11 | Schema migration on package upgrade | The shipped systemd unit runs `zfsbackup-config migrate` in `ExecStartPre`, after a DB backup. | zb B-8 |
| Q12 | werkzeug's server | Keep it, pin the minor version, and add contract tests. Its known weaknesses are already handled in `ApiServer`; replacing it is a large rewrite with no drop-in alternative. | zb K-1, S-9 |
| Q13 | Strict snapshot-name parse (`{prefix}_{14 digits}` exactly) | Yes. Today a manual `autosnap_<ts>_keep` counts as managed and gets pruned. | zb S-1 |
| Q14 | Ownership of new `packaging/**` and `.github/workflows/**` files | `packaging/**` to zfsbackup-developer; workflows to pytest-test-author. | process |
| Q15 | The old libzfseasy decisions (sort `-S`, comma roots, `debug`, send stderr, `-v` deadlock) | Close them. All five are already fixed (verified). | — |

## Production blockers (must be fixed before any real deployment)

zb B-2 and B-3 are not in this table: after Q1 they are should-fix items (see Part 3).


| ID | Item | Package | Size |
|---|---|---|---|
| lib B1 | Refuse property values that can't be framed, on every write path | libzfseasy | S |
| lib B2 | `list`/`get` parsers fail closed per row (skip and warn, drop duplicate names, keep empty trailing columns) | libzfseasy | M |
| lib B3 | Framing-safe single-property read `zfs.get.value()` | libzfseasy | S |
| lib B4 | Check the zfs output formats zfsbackup depends on (resume token, `destroy -v`, `receive -A`) on every supported OpenZFS version | libzfseasy | M |
| lib T1/T2 | Real-pool tests for the commands zfsbackup uses; OpenZFS version-matrix scenario | tests/scenarios | M–L |
| zb B-1 | Cross-tenant property injection: validate and canonicalise the client's config, `-x` the zfsbackup properties on receive, re-read each received dataset's properties individually | zfsbackup | M |
| zb B-4 | Prune batch fails entirely on one held or cloned snapshot; fall back to one snapshot at a time | zfsbackup | S |
| zb B-5 | Intermediate snapshots never reach the server; freeze the send flags (`-L`) | zfsbackup | M |
| zb B-6 | A backup health check for alerting (`zfsbackup-config check`, with Nagios/Prometheus output) | zfsbackup + CLI | S/M |
| zb B-7 | Real-pool end-to-end test in pytest (rewrite the skipped `TestRealSendReceive`) | tests | M |
| zb B-8 | Installable package: daemon entry point, `--version`, shipped systemd/sysusers/tmpfiles, upgrade runbook | zfsbackup + CLI | M |
| zb B-9 | Documented and tested restore (file-level and full) | docs + scenarios | S/M |

## Recommended order

1. **Decisions** Q1–Q15. Q3, Q4, Q5 and Q6 gate libzfseasy; Q1, Q2, Q9, Q10 and Q11 gate zfsbackup.
2. **Milestone A: trust boundary.**
   - libzfseasy B1, B2 and B3, with tests T1 alongside, and the T2 matrix scenario built in parallel.
   - Then zfsbackup B-1, S-4 (remove the cross-client info endpoints) and S-5 (wire protocol version).
   - Two-VM scenarios L1 (malicious client tries to drive pruning of another client) and L2 (hostile streams).
   - B-7 base rewrite in parallel.
3. **Milestone B: snapshot and send correctness.**
   - zfsbackup B-4, S-1 (order by `createtxg`, survive clock jumps), B-5, and S-6 if Q10 says "now".
   - libzfseasy B4: run the OpenZFS matrix and fix whatever differs.
   - Rerun two-VM scenarios B and J.
4. **Milestone C: install, operate, restore.**
   - zfsbackup: B-8, B-6, B-9, S-13 (journald priorities), S-9 (dependency pins), S-11 (backup of the DB and keys), S-15 (docs).
   - S-10: CI for the real-pool tier, plus a release checklist.
5. **Milestone D: hardening for 1.0.**
   - libzfseasy: S1 (exec hygiene), S5, S6, then S3 (public row API), which enables zb B-1d. S2 (timeouts), which enables zb S-14. S4 (JSON).
   - zfsbackup: B-2 and B-3 (now should-fix after Q1), S-2 (mid-stream revocation, stalled-writer bound), S-3 (flood control), S-7 (worker outcome status), S-8 (code-version skew guard), S-12 (TLS validity visibility), K-5.

All Linux-sensitive work (sockets, TLS, flock, SQLite, systemd, ZFS) is checked on the `zfsb-ci` VM, using the
system libraries that match CI, or on the zfsb VMs with real pools; never on macOS alone. The concurrency
reviewer covers the exec, concurrency and supervisor items.

---

# Part 2: libzfseasy plan (full)


**Baseline.** PR #4 is no longer open: `gh` shows it merged on 2026-10-21 as `609df66` on `origin/master`. `libzfseasy/` is byte-identical between `23f36b5` and `origin/master`, so this plan applies unchanged. Implementation should go on a new branch from `master`, for example `libzfseasy-prod`.

The mocked libzfseasy files pass (types, commands, commands_part2, integration: 1000 passed), and the working tree is clean.

**What I checked against real ZFS.** I ran short probes on a throwaway file-backed pool `pprobe` on `zfsb-client` (OpenZFS 2.4.1), created and destroyed each time. I never touched `foam` or `zfs-test`.

**Main finding.** The pre-existing item-1 bugs are all fixed. The real remaining blocker is **row framing in `-H` output**. ZFS accepts newlines, tabs and invalid UTF-8 in user-property values, and libzfseasy's `list`/`get` parsers then either crash or **accept forged rows**. One enrolled client can use this to make the server prune another client's backups. That is a cross-tenant data-loss path.

---

## Open decisions (each has a recommendation)

**D0. The old pending decisions from item 1.** All five are implemented (PR #3, verified in the code; see Appendix). Recommendation: confirm they are closed. Three small leftovers:
- (a) `zfs.debug` is kept but does nothing (`zfs.py:50-52`). Keep it as a no-op through 1.0 and remove it later.
- (b) Exposing `zfs send -v/-P` progress is now deadlock-safe, but nothing needs it. Do not expose it now.
- (c) The streaming stderr cap is 200 head + 200 tail lines, 64 KiB per line (`zfs.py:81-84`). Keep it.

**D1. Framing policy for property values (drives B1 and B2).**
- **Writes:** I recommend refusing `\n`, `\r`, `\t` and `\0` in every property value libzfseasy passes to zfs. That means `set`, plus the `-o` options of `create`, `clone`, `snapshot` and `receive`. It raises `ValueError` before the command runs.
  - Alternative: refuse only `\n\r\0`, since a tab can be recovered in the last column only.
- **Reads:** I recommend that a malformed row is **skipped with a warning**, the same policy as R3, and never raises for the whole command. A malformed row means: wrong column count, unknown type, a name outside the requested roots, or a duplicate name.
  - Reason: skipping fails safe for pruning (it keeps more), and one bad dataset cannot stop pruning for every client.
  - Alternative: raise for the whole listing. That is safer against forged rows but lets any one client stop all server pruning.

**D2. Hung pool.** I recommend three parts:
- (a) A slow-command watchdog: log a WARNING when a `zfs` command is still running after N seconds (default 60). Behaviour does not change.
- (b) An opt-in `timeout=` for non-streaming commands, **off by default**. On timeout: TERM, then KILL, a bounded wait, then raise. While an earlier timed-out child is still alive (D state), refuse to start new ones, so a hung pool cannot pile up unkillable processes. Callers must treat a timeout as "outcome unknown".
- (c) `StreamHandle.close(timeout=None)` and `StreamHandle.abort()`.

Whether the workers turn (b) or (c) on is the zfsbackup planner's call.

**D3. JSON output (`zfs list -j` / `zfs get -j`, OpenZFS 2.3 and later; verified on 2.4.1).** I recommend using it for `list`/`get` when it is available (detected once per process) and keeping `-H` for 2.1/2.2. It is the only fully framing-safe parse. It belongs in "should fix before 1.0", not the blockers, because B1–B3 plus the zfsbackup changes close the hole on every version.

**D4. Supported OpenZFS versions.** I recommend:
- Test on 2.1 (Ubuntu 22.04), 2.2 (Ubuntu 24.04) and 2.4 (Ubuntu 26.04, verified 2.4.1).
- Treat 2.3 as best-effort and untested. `multipass find` offers no image that ships 2.3; the only option is a VM built from source (size L).
- Treat macOS OpenZFS as development-only and FreeBSD as unsupported.

If you claim 2.1/2.2 support, B4 is a blocker.

**D5. Locale and decoding for every `zfs` call.** I recommend:
- Run every command with `LC_ALL=C`. `resume_info` already does (`zfs.py:1356`), and zfsbackup matches English messages such as "dataset does not exist" and "does not have any resumable receive state".
- Decode stdout as UTF-8 with `surrogateescape`, which round-trips the bytes, and stderr with `replace`.

---

## Production blockers

### B1. Refuse un-frameable property values on write
- **Tier:** needs-approval (changes what `Validate` accepts). **Size:** S. **Owner:** `libzfseasy-developer`, then `pytest-test-author`.
- **Basis (re-verified):**
  - `SetCommand._set` (`zfs.py:1090-1100`) passes any value through.
  - The server stores the client-supplied `config` string with `zfs.set`, with no validation (`zfsbackup/api.py:1712, 1733-1742`).
  - On 2.4.1, `zfs set org.t:v=$'line1\nline2'` and values containing a tab both succeed. `zfs list -H` then prints the newline raw inside the row.
  - With the config as the last column, a value of `X\n<victim>\tfilesystem\t<id>\t<cfg>` produces a well-formed forged row in `received_datasets()` (`backup_manager.py:537-577`). The server pruner then applies the attacker's retention to another client's dataset.
- **Fix:**
  - Add `Validate.value(s)` per D1.
  - Call it in `SetCommand`, `CreateCommand.filesystem/volume`, `CloneCommand`, `SnapshotCommand`, `ReceiveCommand._get_options(props)` and `MountCommand` `-o`.
  - It runs before any exec. The exec contract is otherwise unchanged.
- **Verify (mocked):** each command raises `ValueError` and never calls Popen. Run `pytest tests/test_types.py tests/test_commands.py tests/test_commands_part2.py`.
- **Real pool:** not needed.
- **Hand-off:** zfsbackup must map this `ValueError` to a 400 response (X1).

### B2. Make the `list`/`get` parsers fail closed per row
- **Tier:** needs-approval (parser behaviour, and touches the shared exec read path). **Size:** M. **Owner:** `libzfseasy-developer`, then `pytest-test-author`.
- **Basis:** the write guard does not cover values that arrive any other way. On 2.4.1, a `zfs send -p` stream whose `org.zfsbackup:config` holds a newline was received, and `zfs list -H -r` printed a forged `pprobe/victim\tfilesystem\tFORGED` row. A client can deliver such a stream itself (X2). Reproduced on the current code:
  - `ListCommand._line_to_object('line2\tc1\tc2', …)` raises `ValueError: c1 is not a valid ZFS type`, and a one-column fragment raises "not enough values to unpack" (`zfs.py:577-591`). Either one kills the whole listing, so `received_datasets` returns `[]` and server pruning stops for every client.
  - In `GetCommand._lines_to_objects` (`zfs.py:1046`), a 5-way unpack crashes on any value containing a tab or newline.
  - A tab in a middle column silently shifts values into the wrong properties (reproduced).
  - `_exec_capture` returns `stdout.strip()` (`zfs.py:323`). This drops a trailing empty column (2.4.1 prints `name\t\n` for an empty value) and trailing spaces in values.
- **Fix:**
  - `list`/`get` read through a new private line helper that strips only `\n`. Keep `_exec_out` unchanged, because zfsbackup calls it directly (X4).
  - **`list`:** a row is skipped with a warning when any of these holds:
    - the column count is not `len(properties)`;
    - the type is not filesystem, volume, snapshot or bookmark;
    - the name fails to parse (today only `InvalidNameError` is caught);
    - the name is outside the requested roots/depth;
    - **the name is a duplicate.** Drop both rows. This defeats forging the name of a dataset that exists. Non-lazy mode only; document that lazy mode gets per-row checks only.
  - **`get`:** recover the value when it is unambiguous (`received` is `-`, or source is `received` and the two halves match); otherwise skip with a warning.
  - `_skip_invalid_name` should log the name with `%r`.
- **Verify:**
  - Mocked: feed the exact VM byte sequences above, including the forged-row, empty-column and `\xff` cases.
  - Real pool: one `real_zfs` test that sets newline/tab values and a received `-p` property, then asserts no crash and no forged object. This can run on any `zfsb-*` VM.

### B3. A framing-safe single-property read: `zfs.get.value(ds, prop, sources=…)`
- **Tier:** low-risk (new API only). **Size:** S. **Owner:** `libzfseasy-developer`, then `pytest-test-author`.
- **Basis:** on 2.1/2.2 (no `-j`), `-H` output for many objects cannot be framed safely. For one object and one property, `zfs get -H -o value [-s …] <prop> <name>` is unambiguous: all of stdout is the value. zfsbackup needs this to re-read the server-side config per dataset instead of trusting rows from the listing (X2).
- **Fix:**
  - Build it on `_exec_collect` (`zfs.py:175-220`), which keeps blank lines and strips only `\n`.
  - Join the lines with `\n`. Map `-` to `None`, or provide a raw variant.
  - Validate the property name strictly. `sources` uses `Validate.source`.
- **Verify:** mocked tests, plus one `real_zfs` test with a multi-line and a tab value.

### B4. Check the output formats zfsbackup depends on across OpenZFS versions
- **Tier:** needs-approval (fixes depend on what the matrix finds). **Size:** M, plus fixes. **Owners:** `real-zfs-scenario-dev` (matrix), `pytest-test-author` (tests), then `libzfseasy-developer` (fixes).
- **Basis:** these were only ever checked on 2.4.1:
  - `resume_info` parses `zfs send -nv -t` from stdout, falling back to stderr (`zfs.py:1353-1363`).
  - The `destroy\t`/`reclaim\t` lines of `destroy -v -p` (`zfs.py:746-754`).
  - `receive -A` "already clean" wording (`api.py:93-96`).
  - The `received` column of `get`.

  On a version where `resume_info` cannot parse the output, the client treats the token as uncertain and **skips every cycle** until an operator runs `zfs receive -A`. Backup stops quietly for that dataset.
- **Fix:** run T2 on 2.1 and 2.2, then fix whatever differs.
- **Verify:** T1 passes on `zfsb-zfs21`, `zfsb-zfs22` and `zfsb-zfs24`. Then re-run `scenarios/test_two_vm_backup.sh --only-j` (`zfsb-client`/`zfsb-server`).
- **Real pool:** yes.
- **Condition:** this is a blocker only if D4 includes 2.1/2.2.

### T1. `real_zfs` coverage for the commands zfsbackup uses
- **Tier:** low-risk. **Size:** M–L. **Owner:** `pytest-test-author`.
- **Basis:** `tests/test_real_zfs.py` (lines 78-521) has no real-pool tests for:
  - `exists()` (missing dataset, missing pool);
  - user-property `set`/`get`/`inherit` with `-s local,received`;
  - `receive_resume_token` on a filesystem and a volume;
  - `send -i`/`-I`;
  - `send.resume_info` and `send.resume(token)` against a genuinely interrupted `receive -s` in a single pool;
  - `receive.abort` in both states;
  - `receive -F` onto a diverged target;
  - batch and range destroy, dry run plus `reclaim`;
  - `rename` of a snapshot or volume;
  - names with inner spaces;
  - unrepresentable names (`pool/-x`, `@ manual`; verified that 2.4.1 accepts both) being skipped with a warning;
  - `get all` on each object type.

  The daemon-level `TestRealSendReceive` is skipped (readiness report, line 57).
- **Note:** pipe and deadlock assertions must run with **pytest inside the VM**, not through the `MULTIPASS_VM` wrapper. The `multipass exec` layer adds its own buffering, and stdin=DEVNULL hits the known Multipass wedge.

### T2. OpenZFS matrix scenario
- **Tier:** low-risk. **Size:** M. **Owner:** `real-zfs-scenario-dev`.
- Write `scenarios/test_openzfs_matrix.sh`:
  - Provision disposable `zfsb-zfs21` (jammy), `zfsb-zfs22` (noble) and `zfsb-zfs24` (resolute). `zfsb-ci` is noble but has no `zfs` installed.
  - Ship the tree and log `zfs version`.
  - Create a file pool under `/var/tmp`.
  - Run `ZFSB_REAL_ZFS=1 pytest -m real_zfs tests/test_real_zfs.py` inside each VM.
  - Run `--purge` on its own VMs only.

---

## Should fix before 1.0

### S1. Exec-path hygiene
- **Tier:** needs-approval (exec/process cleanup). **Size:** M. **Owner:** `libzfseasy-developer`, then `pytest-test-author`.
- **Changes:**
  - `_exec` (`zfs.py:127-136`) has no try/finally; an interrupt leaves the child running. Have it consume `_exec_out` so it gets that cleanup.
  - Bound the unbounded `stderr_thread.join()` calls (`zfs.py:171, 330`) using `_join_stderr_reader`.
  - The text stderr reader (`zfs.py:222-236`) keeps an unbounded list and dies on a decode error, which stops draining the pipe and can deadlock on more than 64 KiB of stderr. Make it use `_BoundedErrors` and survive decode errors.
  - Apply the D5 encoding and `LC_ALL=C` to all commands.
  - When a command fails with no stderr, the error message is empty (`zfs.py:332, 1489, 421`; for example a `zfs send` killed by SIGPIPE). Fall back to "zfs <sub> exited with code N / signal SIGPIPE" only when stderr is empty, so substring matching is unaffected.
- **Verify:** real-child tests (`sys.executable` children, no pool needed): invalid UTF-8 on stdout and stderr, 200 KB of stderr, an interrupt during `_exec`, and the empty-stderr message. The suite's orphan net must stay green.
- **Extra review:** `concurrency-reviewer`.

### S2. Hung-pool support (per D2)
- **Tier:** needs-approval. **Size:** M–L. **Owner:** `libzfseasy-developer`, `pytest-test-author`, `real-zfs-scenario-dev`.
- **Basis:** nothing in the exec path has a timeout. `_exec_capture`'s `readline`/`wait` and `StreamHandle.close()`'s `wait()` (`zfs.py:392`) block forever.
- **Context:** stop still works, because workers have SIGTERM=SIG_DFL (`workers.py:180-182`). A reload never turns over a stuck worker.
- **Verify:**
  - Real-child tests: a child that ignores SIGTERM, and a child that never exits.
  - Real hang: suspend a pool (`failmode=wait` on a `dmsetup suspend`ed device) on a **disposable** `zfsb-hang` VM, since it may need a reboot.
- **Extra review:** `concurrency-reviewer`.

### S3. Public raw-row API: `zfs.get.rows(…)`
- **Tier:** low-risk. **Size:** S–M. **Owner:** `libzfseasy-developer`.
- **Basis:** zfsbackup calls private `zfs.get._exec_out` and `_zfs_cmd` (`backup_manager.py:15, 94, 112`) and has its own tab-recovery parser.
- **Fix:** yield `(name, prop, value, received, source)` tuples using B2's rules. zfsbackup migrates afterwards (X4).

### S4. JSON parse path (per D3)
- **Tier:** needs-approval. **Size:** L. **Owner:** `libzfseasy-developer`, `pytest-test-author`.
- **Notes from 2.4.1:** type is upper-case (`"FILESYSTEM"`), and raw `\xff` bytes appear inside the JSON, so decode with `surrogateescape` before `json.loads`.
- **Verify:** on `zfsb-zfs24`, plus the fallback on `zfsb-zfs22`.

### S5. Small correctness fixes
- **Tier:** low-risk. **Size:** S. **Owner:** `libzfseasy-developer`, then `pytest-test-author`.
- **Changes:**
  - `SendCommand._send` returns `None` for a Bookmark or any other object (`zfs.py:1225-1230`); make it raise `ValueError`.
  - `DestroyCommand.snapshots([])` raises `AttributeError` (`zfs.py:789`); make it raise `ValueError`.
  - `exists()` rejects a Bookmark (`__init__.py:47-54` goes through `_dslist_to_list`).
  - `load_key`/`unload_key` require `ds` although the docstring says it defaults to `None` (`zfs.py:1662, 1706`).
  - `send.resume` does not apply the token check that `resume_info` does (`zfs.py:1318-1331`).
  - `Validate.source` says "type" in its error message (`types.py:122`).
  - `_lines_to_objects` uses `name` instead of `dsname` (`zfs.py:1052`).
- **Verify:** mocked tests only.

### S6. Name-limit warnings
- **Tier:** low-risk. **Size:** S. **Owner:** `libzfseasy-developer`.
- **Change:** make the "skip unrepresentable name" warning count-based rather than strictly once per process. For example, log a summary line on every listing that skipped rows.
- **Basis:** `_warned_names` (`zfs.py:56-75`) warns once per process, so a repeating skip goes quiet after the first worker incarnation.

---

## Can ship without (document these)

- **C1. TrueNAS `libzfs` backend.** It does not exist: the dependency is commented out (`pyproject.toml:19`), but `README.md:5` and `CLAUDE.md:72` still advertise it. Drop the claim. Owner: main session, since docs have no owning agent.
- **C2. Pool names.** `Validate.pool` (`types.py:83`) refuses `:` and inner spaces, which ZFS allows. Such a pool fails config validation with a clear message. Document it, or make the optional needs-approval regex fix.
- **C3. `longname` (2.3+).** Names over 255 bytes (`types.py:96`) are skipped with a warning. Document.
- **C4.** `zpool` is required at import but never used (`zfs.py:24-30`), and binaries are resolved at import time. Document, or later make resolution lazy.
- **C5.** A user-property value of literally `-` reads back as `None` (`zfs.py:580`). Document.
- **C6.** In an interactive CLI, `zfs` children share the terminal's process group, so Ctrl-C also reaches them. Document.
- **C7.** Not implemented: hold/release/rollback/promote/diff (`__init__.py:30-45`). zfsbackup uses `inflight.*` properties instead of holds. Document.
- **C8.** Pickles are not stable across versions (already documented at the top of `types.py`).

### What happens today with names ZFS accepts but `Validate` refuses (item 2)
- **A configured dataset** with such a name is refused at config validation (`config/store/mapper.py:160-202`). This is a clear error, not a silent failure.
- **Snapshots, descendants and received children** with such names are skipped with a warning (R3, `zfs.py:584-588, 1076-1080`). Pruning continues for everything else (`backup_manager.py:651-665`). `zfs destroy -r` still reaches those descendants. No worker crashes.
- **R3 holds everywhere objects are built from zfs output.** `SnapshotCommand` (recursive) and `CloneCommand` go through `ListCommand`.
- **The real crash or stall path is malformed rows, not names** (B2).

---

## Notes for the zfsbackup planner (not libzfseasy work; listed so nothing is dropped)

- **X1.** `api.py:1712-1742`: decode the client's `config` with `from_property` and re-encode it in canonical form before `zfs.set`. Turn B1's `ValueError` into a 400.
- **X2.** `received_datasets` trusts rows from `zfs list -r`. A **received** property can forge rows (verified on 2.4.1), and a client that sends an empty `config` leaves the received value in effect.
  - Re-read config and client id per dataset with B3.
  - Check that the path is under `target/<client_id>`.
  - Consider `zfs.receive(..., reset=[org.zfsbackup:*])`, which maps to `-x`, for the zfsbackup properties.
- **X3.** Verified on 2.4.1: destroying a batch of snapshots is all-or-nothing. One cloned or held snapshot fails the whole batch (rc=1, nothing destroyed), and `-v -p` still prints `destroy` lines before failing. Prune's batch of 20 (`backup_manager.py:811-820`) then blocks every cycle; retry the batch one snapshot at a time.
- **X4.** Migrate off `zfs.get._exec_out`/`_zfs_cmd` once S3 lands. Until then B2 keeps `_exec_out` as it is.
- **X5.** Decide whether the workers adopt S2's timeouts and `StreamHandle.abort()`.

---

## Recommended order

1. **Decisions** D1, D4 and D5 first (they gate B1, B2, B4 and S1); then D2 and D3.
2. **libzfseasy-developer, one pass, separate commits:** B1, then B2, then B3. In parallel, `pytest-test-author` writes the B1–B3 mocked tests and T1 against the agreed contracts, and `real-zfs-scenario-dev` builds T2.
3. **Hand B3 to the zfsbackup planner** for X1/X2. That is where the cross-tenant fix actually closes.
4. **B4:** run T2 on 2.1, 2.2 and 2.4, fix what it finds, then run two-VM `--only-j`.
5. **S1**, then S5 and S6. S5 and S6 are low-risk and could run automatically under `/loop` at any point.
6. **S3** (then X4), then **S2**, then **S4**. Docs C1–C8 can be done at any time.
7. **Review:** `zfs-code-reviewer` on everything; `concurrency-reviewer` on S1 and S2.

---

## Appendix: already fixed (verified in the current code)

- **Sort `-s`/`-S`:** `asc` is honoured for strings and lists, and per key for a dict (`zfs.py:542-552`).
- **Comma-joined roots:** each root is now its own argv entry (`zfs.py:561-562`).
- **`debug=True`:** kept but ignored. Commands are logged at DEBUG with credentials redacted (`zfs.py:50-52, 87-100`).
- **Send stderr truncation and the `-v` deadlock:** a binary reader thread drains stderr from the start (`zfs.py:239-270`, started at 1446 and 1633), with `_BoundedErrors` (103-122). `close()` joins it (395-402).
- **Abandoned `_exec_out` generator:** the child is reaped (`zfs.py:153-172`).
- **Busy-spin at stdout EOF:** text path (`zfs.py:327-328`) and binary path (1481-1483).
- **Send/receive failures were silent:** `StreamHandle.close()` now raises (`zfs.py:380-423`), and a receive that fails at startup raises (1634-1638).
- **Set/Inherit changed the object before the command ran:** names are validated first and the object is updated only after success (`zfs.py:1094-1098, 1125-1131`).
- **Name regexes:** `fullmatch` with ASCII only, so a trailing `\n` or non-ASCII is refused (`types.py:76-77, 111, 156`). Inner spaces are allowed (`types.py:84, 89`).
- **R3 skip and warn:** `InvalidNameError` (`types.py:69-72`), `zfs.py:62-75`.
- **OpenZFS 2.4 property names, and lenient native names** (`types.py:15-60, 139-147, 274-306`).
- **`receive_resume_token` on volumes** (`types.py:37`).
- **`resume_info` under `LC_ALL=C`** (`zfs.py:1333-1383`).
- **`-t ''` in `get`** (`zfs.py:1023-1025`).
- **`reclaim` line dropped from destroy output** (`zfs.py:746-754`).
- **Real-pipe tests without a pool** (`tests/test_commands.py:1379`; `tests/test_commands_part2.py:732, 1038, 1562`).

Still open from the earlier deferred list, now carried in S1/S5: `_exec` without try/finally, `_send` returning `None`, `destroy.snapshots([])`, and the `load_key` docstring.

---

## For your approval

I propose four blockers:
- **B1–B3:** close the `-H` framing hole, which lets an enrolled client forge rows and drive server pruning of another client's data. B1 and B2 need approval; B3 is low-risk.
- **B4:** check the output formats on the OpenZFS versions you will claim to support.

Plus real-ZFS coverage (T1/T2), six should-fix items (S1–S6) and eight documentation-only items.

Questions I need answered:
1. Which characters to refuse in values (D1), and should malformed rows be skipped or abort the listing?
2. Which OpenZFS versions to support (D4): is 2.3 untested acceptable?
3. The hung-pool approach (D2): watchdog only, or also opt-in timeouts and `abort()`?
4. Should `LC_ALL=C` and `surrogateescape` apply to every command (D5)?
5. JSON parsing for 1.0, yes or no (D3)?

Approve, adjust, reprioritize or drop any item; each one stands on its own. One assumption: the X-notes go to the zfsbackup planner rather than into this plan.

---

# Part 3: zfsbackup plan (full)


**Base:** PR #4 is merged to `master` at `609df66`, so all work goes on a new branch from `master`. File references were re-checked against that tree; line numbers are approximate after the merge. The libzfseasy planner's handoff (X1–X5, B4) is folded in, and its cross-tenant items are production blockers.

**Ownership.** The ownership table has no owner for packaging files, `pyproject.toml` or `.github/workflows/`. Earlier milestones give `pyproject.toml` to `zfsbackup-developer`. I assume new `packaging/**` files go to `zfsbackup-developer` too, and `.github/workflows/**` to `pytest-test-author`. Please confirm.

---

## Open decisions (each has a recommendation)

- **D1. Is a compromised or malicious enrolled client in the threat model?**
  - **Recommendation: yes.** The point of an off-host backup is to survive a compromised client (ransomware).
  - If yes, B-2 (receive hardening) and B-3 (retention floor) are blockers. If no, they become "should fix", but B-1 stays a blocker because it is cross-tenant.
  - Sub-question: should the retention floor have a default? I recommend an explicit setting, and a server-start WARNING when remote backup is on and no floor is set.
- **D2. How should intermediate snapshots reach the server (H5)?**
  - Option A: one `zfs send -I` stream (standard). But `-I` also sends every *unmanaged* snapshot in the range (manual or zfs-auto-snap ones). The server pruner never touches unmanaged snapshots, so they pile up there for ever.
  - Option B: a chain of `-i` sends over managed snapshots only. Each step is its own stream with its own pin, and the anchor advances after each step.
  - **Recommendation: B.** The server history is then exactly the client's managed snapshots, and an interruption loses only one step.
- **D3. Raw sends (`-w`) for encrypted datasets: decide before the first real deployment.**
  - Switching a dataset between raw and non-raw later needs a full re-send.
  - **Recommendation:** a per-remote `raw: auto|true|false`, default `auto` (raw if and only if the dataset is encrypted).
  - Implemented as S-6. If you have no encrypted client datasets, S-6 can wait.
- **D4. Schema migration on package upgrade.**
  - Today the daemon refuses an out-of-date schema until someone runs `zfsbackup-config migrate`.
  - **Recommendation:** the shipped unit runs `ExecStartPre=<venv>/bin/zfsbackup-config -c DB migrate`, with a pre-migration backup from S-11's backup primitive. The alternative is a manual step in the upgrade runbook.
- **D5. werkzeug's server.**
  - **Recommendation: keep it** (details in K-1). Pin werkzeug's minor version and add contract tests (S-9) instead of replacing it.

---

## 1. Production blockers

### B-1. Cross-tenant property injection in received datasets (X1, X2, part of X4)
- **Tier:** needs-approval. **Size:** M.
- **Basis:**
  - Verified by the libzfseasy planner on OpenZFS 2.4.1: user-property values may contain `\n`, `\t` and invalid UTF-8, and `zfs list -H` prints them raw.
  - `api.py:1710-1743` (negotiate) does `zfs.set(PROP_CONFIG: ds_config_encoded)` with the client's string unvalidated. `request.get_json(force=True)` is also unchecked for shape.
  - `backup_manager.py:537-577` `received_datasets()` trusts every row of one `zfs list -r -o …client_id,config`.
  - A forged row (`X\n<victim>\tfilesystem\t<id>\t<cfg>`) makes the server pruner apply the attacker's retention to another client's dataset: cross-tenant data loss.
  - The same value can arrive as a *received* property in a `zfs send -p` stream; local wins only if a local value exists. A client that sends an empty `config` (`api.py:1733`) leaves a received one in effect.
  - A malformed row currently crashes the parse, so `received_datasets` returns `[]` and server pruning stops for every client.
- **Dependency:** libzfseasy B1 (refuse values that can't be framed), B2 (fail closed per row, drop duplicate names) and B3 (`zfs.get.value(ds, prop, sources=…)`) land first. Coordinate with `libzfseasy-implementation-planner`.
- **Fix:**
  - **B-1a** (`zfsbackup-developer`, `api.py`), X1:
    - Negotiate requires a JSON object.
    - `config` must decode with `DatasetConfig.from_property`. Re-encode it canonically (`to_property()`) and store only that.
    - Undecodable input → 400. libzfseasy's new `ValueError` from `zfs.set` → 400.
    - An empty or missing `config` → 400. This changes the old "skip the set" behaviour: there is no deployed client to stay compatible with.
  - **B-1b** (`zfsbackup-developer`, `api.py`):
    - receive with `reset=['org.zfsbackup:config', 'org.zfsbackup:client_id', 'org.zfsbackup:source_dataset']` (`-x`), so received copies of the fixed-name properties never take effect.
    - Dynamic names (`anchor.*`, `inflight.*`) cannot be `-x`'d. This is an accepted residual: they can only *pin* snapshots (disk growth, bounded by quota), never delete them.
  - **B-1c** (`zfsbackup-developer`, `backup_manager.py`), X2. In `received_datasets()`, use `zfs list` only to enumerate names. Then, for each dataset:
    - re-read `client_id` and `config` with B3's single-property read, `sources=local` only;
    - require the name to be under `<target>/<client_id>/`, with `client_id` matching the A7 grammar;
    - skip with a once-per-dataset WARNING on any mismatch;
    - make sure one bad dataset never empties the list.
  - **B-1d** (`zfsbackup-developer`, `backup_manager.py`), X4: move `read_zfsbackup_properties` and `read_descendant_zfsbackup_properties` (`:15, :91-112`) off `_zfs_cmd` / `zfs.get._exec_out` onto libzfseasy S3 `zfs.get.rows(…)` once it lands. This can trail B-1a–c.
- **Verification:**
  - `pytest-test-author`, mocked:
    - a forged-row table: `\n`/`\t` in config, client_id or source_dataset values, a received-only config, an empty config, and a dataset outside `<target>/<id>`;
    - the pruner never gets a victim `DatasetInfo`;
    - one bad dataset doesn't stop the others.
  - `real_zfs` on `zfsb-ci`: `zfs set` a newline value directly, plus a crafted `zfs send -p` stream.
  - `real-zfs-scenario-dev`: two-VM scenario L1, where a malicious client B tries to drive pruning of A's dataset; A's snapshots and GUIDs must be unchanged.

### B-2. Server receive hardening: properties, holds, `-F`, zvols (needs D1)
- **Tier:** needs-approval. **Size:** M.
- **Basis:**
  - `api.py:1909`: `zfs.receive.filesystem(server_fs, force=True, save=True, mount=False)`. libzfseasy receives holds by default, `-F` is always on, and no `-o`/`-x` is passed.
  - A trusted but malicious client can therefore:
    - (a) use a `-p` stream with `mountpoint=/etc`, `canmount=on`, `sharenfs=on`, `setuid=on` or `exec=on`. It mounts at the next `zfs mount -a`/boot, which gives a path to root on the backup server;
    - (b) use a `-R -I` stream, where `-F` destroys every server snapshot "not on the sending side", wiping history without touching retention;
    - (c) send holds (`send -h`), which block pruning (see B-4);
    - (d) send a zvol stream that udev/LVM/md may probe on the server.
  - The datasets are also created mountable: `zfs.create(server_fs, parents=True)` at `api.py:1736` and `:1894`.
- **Fix** (`zfsbackup-developer`, `api.py` only; libzfseasy already supports `props`, `reset` and `holds`):
  - receive with `holds=False` (`-h`);
  - `props={'canmount': 'noauto', 'readonly': 'on', 'volmode': 'none'}` (`-o`);
  - `reset=[mountpoint, sharenfs, sharesmb, setuid, exec, devices]` plus B-1b's names (`-x`);
  - create server datasets with `canmount=noauto`;
  - use `force=True` only for a full stream into a server dataset that has no snapshots (the first receive into the pre-created dataset), and never for incrementals. `readonly=on` + `canmount=noauto` keep the target unmodified, so incrementals don't need `-F`.
  - The exact property set must be checked per OpenZFS version: `-o volmode` on a filesystem receive, `-o` combined with `-s` resume, and `-o`/`-x` on a raw stream (S-6).
- **Edge cases:**
  - Resume (`mode=resume`) must use the identical flag set.
  - `mode=fresh` + `receive -A` is unchanged.
  - A full send onto a dataset that already has snapshots fails, as it does today; that goes into the runbook (B-9).
- **Verification:**
  - Mocked argv tests (`pytest-test-author`).
  - `real_zfs` on **two OpenZFS versions**: 2.4.1 (zfsb VMs) and 2.2.x (Ubuntu 24.04, also the CI target in S-10). Crafted `-p`, `-R -I`-with-a-deleted-snapshot, `-h` and zvol streams. Assert nothing gets mounted or shared, no server snapshot is destroyed, holds are absent, and `volmode=none`.
  - Two-VM scenario L2.

### B-3. Server-side retention floor for received datasets (M8; needs D1)
- **Tier:** needs-approval (retention, schema, config contract). **Size:** M/L.
- **Basis:**
  - The readiness report's M8.
  - `received_datasets()` builds retention from the client-supplied `PROP_CONFIG` (`backup_manager.py:557`).
  - A compromised client can set `retention` to keep only the newest snapshot, and the server deletes the client's whole history within one prune interval. Authentication alone does not help here.
- **Fix** (simplest guarantee): a new server setting `remote_backup.min_retention: <Duration>`. A received snapshot younger than this is never pruned. The client's rules apply only to older snapshots.
  - **B-3a** (`zfsbackup-developer`, `config/model.py`): field, parse/validate (positive Duration, literal kept), `same_as` and `validate_config`. Lands first.
  - **B-3b** (`zfsbackup-store-developer`): nullable column, Alembic revision 3, mapper round-trip, and `EXPECTED_HEAD`/`KNOWN_REVISIONS`. Consumes B-3a's field.
  - **B-3c** (`zfsbackup-cli-developer`): `render.py` round-trip, `dotpath` (`remote_backup.min_retention`), and `list`/`export`. After B-3b. Parallel with B-3d.
  - **B-3d** (`zfsbackup-developer`, `backup_manager.py`): `received_datasets()` attaches the floor. `needs_prunning` drops from `to_prune` anything with `age < floor`, for received datasets only (local pruning unchanged). Add the D1 server-start WARNING.
- **Test table** (`pytest-test-author`); floor 30d, client rule "keep newest only":
  - age 1d → kept;
  - age 29d23h → kept;
  - age 30d+1s → pruned by the client's rule;
  - a future-dated name → kept;
  - no floor → today's behaviour.
- **Verification:** mocked unit tests, a store upgrade test (revision 2→3 and downgrade), a CLI round-trip, and `real_zfs` with a hostile config.

### B-4. Prune batch is all-or-nothing on holds and clones (X3)
- **Tier:** needs-approval (retention). **Size:** S.
- **Basis:**
  - `backup_manager.py:811-823` destroys in batches of 20 with one `zfs destroy ds@a,b,…`.
  - Verified on 2.4.1 by the libzfseasy planner: one held or cloned snapshot fails the whole batch, and `-v -p` prints destroy lines before failing. The same batch is rebuilt every cycle, so up to 19 other snapshots are never pruned and the pool fills.
  - A recursive prune also fails on a hold on any descendant's `@snap`.
- **Fix** (`zfsbackup-developer`):
  - when a batch fails, retry it one snapshot at a time;
  - log each per-snapshot failure once (deduplicated like `_descendant_protect_noted`);
  - never count the `-v` destroy lines of a failed call as success.
  - Optional, same item: list with `userrefs,clones` and skip held/cloned snapshots up front with a once-WARNING. The fallback alone covers recursive holds.
- **Verification:**
  - Mocked: one held snapshot in a batch means the other 19 are destroyed; a recursive child hold; a clone; transient EBUSY (a `zfs send` in progress) is retried next cycle.
  - `real_zfs` on `zfsb-ci` with a real `zfs hold` and `zfs clone`.

### B-5. Send semantics: intermediates (H5) and a frozen send-flag set (needs D2)
- **Tier:** needs-approval. **Size:** M.
- **Basis:**
  - `remote.py:1301-1313` sends `zfs.send.snapshot(latest, since=common)`, which is `-i`. Snapshots in between never reach the server, as live-confirmed in the two-VM report.
  - No `-L`/`-c`/`-e` either. Adding `-L` after the first receive breaks incrementals ("incremental send stream requires -L"), so the flag set must be fixed before the first deployment.
  - `_EXPECTED_RESUME_FLAGS` (`remote.py:~1203`) has to match whatever flags are chosen.
- **Fix** (`zfsbackup-developer`, `remote.py`; libzfseasy already supports `intermediate` and `large_blocks`):
  - **D2=B:** iterate common → s1 → … → latest over managed snapshots. Each step: pin (`set_inflight`), fresh stream, ack, `set_anchor`. Stop at the first failure or when `should_stop` fires; the next cycle continues from the anchor. This reuses `_continue_after_resume`. **D2=A:** `intermediate=True`.
  - Add `large_blocks=True` from day one, and update `_EXPECTED_RESUME_FLAGS` (`largeblockok`) to match. Resume-token flag naming has only been checked on 2.4.1 (libzfseasy B4), so coordinate with that planner.
- **Edge cases:**
  - The client pruner may delete a chain member between steps. The step then fails and the next cycle restarts from the anchor; that snapshot is simply never replicated.
  - The anchor never moves backwards.
  - A resumed older step is followed by the rest of the chain.
- **Verification:**
  - Mocked: the step sequence, a failure mid-chain, a stop mid-chain, and `-L` in argv.
  - `real_zfs` (B-7).
  - Two-VM scenario B extended: every managed client snapshot is on the server with a matching GUID. Rerun J (resume) with the new flags.

### B-6. Backup health check usable for alerting (part of M6)
- **Tier:** low-risk. **Size:** S/M.
- **Basis:**
  - M6: a failed backup is only a log line; `workers.py:648` ignores `backup_dataset()`'s result.
  - Nothing reports whether backups are actually happening. For a backup system, silent failure is a deployment blocker.
- **Fix:** a check derived purely from ZFS state, with no daemon change.
  - **B-6a** (`zfsbackup-developer`): a read-only helper, `backup_manager.lag_report(config) -> list[LagEntry]`. It reports:
    - per dataset, the age of the newest managed snapshot vs `frequency`;
    - per destination, the age of the anchor snapshot vs the remote frequency;
    - on a server, per received dataset, the newest snapshot vs the client's frequency.
  - **B-6b** (`zfsbackup-cli-developer`): `zfsbackup-config check [--json] [--prometheus] [--warn-factor 2 --crit-factor 4]`.
    - Exit codes: 0 healthy, 1 unhealthy, 78 DB unusable. Optional `--nagios` maps to 0/1/2/3.
    - The `--prometheus` output (textfile collector) is also the metrics answer (K-9).
- **Verification:** mocked tables (fresh, lagging, missing anchor, no snapshots, server side); a `real_zfs` smoke test; a two-VM check after scenario C (server down) shows remote lag going CRITICAL, then back to OK.

### B-7. Real-pool end-to-end in pytest: rewrite `TestRealSendReceive` (follow-up 7)
- **Tier:** low-risk. **Size:** M.
- **Basis:** `tests/test_zfsbackup_real.py:440-446` skips the class pending an enrolled-mTLS rewrite. Today only the shell harness exercises a real send/receive.
- **Fix** (`pytest-test-author`):
  - an in-process `ApiServer` on loopback with a real server identity;
  - a client identity pinned through `store.trust`, and a client config whose destination carries the server fingerprint;
  - source and target datasets on the test pool.
  - It covers: full, incremental (B-5), resume after a killed stream, B-1/B-2 property neutralisation, and the B-3 floor.
  - Land the basic rewrite first, then add assertions as B-1…B-5 land.
- **Verification:** `ZFSB_REAL_ZFS=1 pytest -m real_zfs` on `zfsb-ci` (Ubuntu, older OpenSSL 3.0 / SQLite), and on the CI job if the S-10 spike works.

### B-8. Minimum installable package, service and upgrade path
- **Tier:** needs-approval only for the D4 `ExecStartPre` part; low-risk otherwise. **Size:** M.
- **Basis:**
  - `pyproject.toml` has only `zfsbackup-config` as a script.
  - The version is duplicated (`pyproject` 0.1.0 and `zfsbackup/__init__.py:3`), and neither CLI has `--version` (`daemon.py:2181-2225`).
  - The README unit (`zfsbackup/README.md:441-479`) uses `/usr/bin/python3 -m zfsbackup.daemon`, has only `After=zfs-import.target` (no `network-online`, no `zfs-mount.service` / `RequiresMountsFor=/var/lib/zfsbackup`), and isn't shipped as a file.
  - Nothing creates the `zfsbackup` group or `/var/lib/zfsbackup` with mode `2770` (`paths.py:309`, `CONFIG_OWNER`).
  - Installation is documented only as `poetry install`.
  - Upgrade hazard: workers import `zfsbackup.remote` and `zfsbackup.api` lazily (`workers.py:614, :1643`), so a package upgraded under a running daemon mixes code versions in respawned workers. The runbook must say "restart after upgrade" (the guard is S-8).
- **Fix:**
  - **B-8a** (`zfsbackup-developer`):
    - a daemon console script (e.g. `zfsbackupd`) and `--version`;
    - one version source (`importlib.metadata`);
    - `packaging/systemd/zfsbackup.service` plus a `<253` drop-in variant, with `After=`/`Wants=network-online.target zfs-mount.service`, `RequiresMountsFor=`, venv `ExecStart`, and the D4 `ExecStartPre`;
    - `packaging/sysusers.d` (group `zfsbackup`) and `packaging/tmpfiles.d` (`d /var/lib/zfsbackup 2770 root zfsbackup`);
    - no mount-namespace hardening (`ProtectSystem`/`PrivateTmp` would hide `zfs create` mounts from the host). Document why.
  - **B-8b** (`zfsbackup-cli-developer`): `zfsbackup-config --version`.
  - **B-8c** (`zfsbackup-developer`, `zfsbackup/README.md`):
    - install into `/opt/zfsbackup/venv` from a wheel;
    - first-run order (import, then trust/enroll, then start);
    - upgrade runbook (install → migrate → restart);
    - supported matrix (Python 3.11–3.14, OpenZFS ≥ 2.1 for `receive -h`, systemd);
    - NTP required;
    - firewall, and a per-client `zfs set quota` on `<target>/<id>`.
- **Verification:** `real-zfs-scenario-dev` adds an install scenario on a fresh zfsb VM. It builds the wheel, installs into a venv, installs the unit/sysusers/tmpfiles, and checks:
  - `systemctl start` reaches READY;
  - `reload` works;
  - an upgrade to a wheel with a newer schema succeeds (D4);
  - `--version`;
  - journald output.

### B-9. Documented and tested restore
- **Tier:** low-risk. **Size:** S/M.
- **Basis:** neither the README nor the scenarios say how to get data back from the server. A backup without a tested restore is not production-ready.
- **Fix:**
  - Docs (`zfsbackup-developer`, README), covering:
    - file-level restore: clone a server snapshot with `-o mountpoint=… -o readonly=on`; must work with B-2's `canmount=noauto`;
    - full restore: `zfs send` from the server back to the client, out of band;
    - "no common snapshot" recovery;
    - an encrypted (raw) dataset, with the key needed.
  - `real-zfs-scenario-dev`: two-VM scenario R covering a file restore and a full dataset restore with GUID/content checks.
- **Verification:** two-VM harness.

---

## 2. Should fix before 1.0

### S-1. Order snapshots by `createtxg`; survive clock jumps
- **Tier:** needs-approval (retention/timeslot). **Size:** M. **Owner:** `zfsbackup-developer`, then tests. Use opus.
- **Basis:**
  - `backup_manager.py:665` sorts by the name timestamp.
  - `needs_snapshot` (`:581-591`) uses `latest.age`. After the clock jumps forward and then back, a future-dated newest snapshot makes `age` negative, so **no new snapshot is taken until real time passes it**. Remote "not due" (`workers.py:636-644`) stalls the same way.
  - The resume ordering check (`remote.py:1227-1231`) and `_settle_up_to_date` (`:1084-1088`) also compare name timestamps.
- **Fix:**
  - fetch `createtxg` and use it for "newest", for the remote `latest`, and for every older/newer comparison;
  - keep name timestamps for retention slots and age;
  - a newest snapshot whose name is more than `max(5m, check_interval)` in the future → ERROR once ("system clock behind the newest snapshot; check NTP") and treat it as due;
  - the same rule for a future-dated anchor.
- **Assumption to confirm:** the parser accepts `{prefix}_{ts}_anything` as managed (`:843`, F1). Nothing generates suffixes, so a manual `autosnap_<ts>_keep` gets pruned. I recommend a strict full match, which makes such snapshots unmanaged (the safe direction), unless F1 intended otherwise.
- **Test table:**
  - normal order;
  - a future-dated newest (snapshot is taken, the next cycle is not due);
  - a backwards clock with a name collision (logged, no loop);
  - txg order differing from name order for resume and settle.
- **Verification:** mocked tests with a frozen clock; `real_zfs` with a hand-made `prefix_20990101000000`.

### S-2. Mid-stream revocation and a write-stall bound for sessions (follow-ups 3 and 8b)
- **Tier:** needs-approval (API, concurrency). **Size:** M. **Owner:** `zfsbackup-developer` (`api.py`, `workers.py`). Use opus; concurrency-reviewer reviews.
- **Basis:**
  - Revocation refuses only new connections; a running stream finishes (the K5 scenario asserts this).
  - A stolen key that sends one frame every <600 s holds the per-dataset claim (`api.py:975-988`) for ever. The legitimate client, re-enrolled under the same id, then gets `busy` indefinitely, and the reload drain never completes.
  - 8b: a pinned peer that pings but never reads blocks the reader in `_SessionSocket.sendall` (`workers.py:985-995`) while it holds the write lock.
- **Fix:**
  - register each stream session with its fingerprint;
  - on every trust refresh, `force_eof` any session whose fingerprint is now `revoked`, `successor_revoked`, `client_revoked` or `replaced`. Not `retired`: rotation is legitimate;
  - the same thread force-EOFs a session whose `sendall` has been blocked longer than `WS_IDLE_TIMEOUT`;
  - `zfs receive -s` keeps resumable state.
- **Verification:** mocked registry tests; a real-TLS test with a non-reading pinned client; two-VM K5 changed to "stream cut within ~4 s, resumable state kept, the re-enrolled client resumes".

### S-3. Anonymous flood control and log throttling (follow-up 2)
- **Tier:** needs-approval. **Size:** M. **Owner:** `zfsbackup-developer`. Concurrency-reviewer reviews (accept-thread path).
- **Basis:**
  - Only concurrency caps exist (`workers.py:657-692`). The handshake rate is unbounded (CPU).
  - Every refused `/enroll` logs a WARNING (`api.py:~1414`), unthrottled.
  - `/enroll` pre-checks share the in-process `_store_call` lock with the trust poll, so a flood can delay revocation pickup. This is an inference; verify it.
- **Fix:**
  - a per-IP token bucket on accept, before the handshake;
  - a per-IP failed-`/enroll` counter, giving 429 + `Retry-After` after N failures in T;
  - one refusal WARNING per IP per hour, plus a periodic count.
- **Verification:** mocked clock; a real-socket flood test on Linux (`zfsb-ci`).

### S-4. Remove the cross-client info endpoints
- **Tier:** needs-approval. **Size:** S. **Owner:** `zfsbackup-developer`, then tests.
- **Basis:** `api.py:1656-1699` (`/config`, `/datasets`, `/datasets/<name>/snapshots`) shows any enrolled client the server's own config, datasets and snapshots. No client or scenario uses them (grep).
- **Fix:** remove them.

### S-5. Wire protocol version
- **Tier:** needs-approval. **Size:** S/M. **Owner:** `zfsbackup-developer` (`api.py` + `remote.py`).
- **Basis:** "client and server must be upgraded together". Today a mismatch shows up as a 500 or refusal, and the client keeps skipping cycles. Nothing exists to negotiate future changes such as raw sends.
- **Fix:**
  - `/whoami` returns `protocol: N`;
  - the client sends `X-Zfsbackup-Protocol`;
  - either side refuses an incompatible peer with one clear ERROR (server: 426).
- Land it in the same milestone as B-1/B-2.

### S-6. Raw sends (D3; follow-up 5)
- **Tier:** needs-approval. **Size:** L.
- **Basis:** confirmed: `remote.py` never passes `raw`, so encrypted datasets go out decrypted, and a locked dataset cannot be sent at all.
- **Split:**
  - S-6a: model field `raw` (`zfsbackup-developer`).
  - S-6b: store column + migration. Bundle with B-3b if both are approved.
  - S-6c: CLI render/dotpath.
  - S-6d: `remote.py` flag selection, plus per-mode `_EXPECTED_RESUME_FLAGS` (`rawok`).
  - Also confirm which B-2 `-o`/`-x` are permitted on a raw receive.
- **Verification:** `real_zfs` with a passphrase-encrypted dataset, sending with the key unloaded; two-VM scenario: the server has no key and the GUIDs match.

### S-7. Record worker outcomes (rest of M6) and surface resume-token parse failures (libzfseasy B4)
- **Tier:** needs-approval (writes from workers). **Size:** M. Concurrency-reviewer reviews.
- **Basis:**
  - `workers.py:648` drops the result; prune and snapshot errors only log.
  - `remote.py:1258-1266`: an undecodable resume token gives a WARNING and a skip every cycle, for ever. On untested OpenZFS versions this is effectively silent.
- **Fix:**
  - `zfsbackup-developer`: a new `zfsbackup/health.py` with an atomic per-worker JSON status file next to the DB (`0640`, DB group), holding last attempt/success/error and consecutive failures per dataset+destination, and the last prune result. Workers write it; ERROR once after N consecutive resume skips.
  - `zfsbackup-cli-developer`: `status` and `check` (B-6) show it.

### S-8. Guard against code-version skew after an in-place upgrade
- **Tier:** needs-approval (supervisor). **Size:** M. **Owner:** `zfsbackup-developer`. Use opus; concurrency-reviewer reviews.
- **Basis:** the lazy imports at `workers.py:614, :1643`; spawn/forkserver import from disk.
- **Fix:**
  - the supervisor records the distribution version and re-reads it on its 10 s poll (a file read, no `zfs`);
  - on a change: a cooperative stop (transfers finish), then exit with a dedicated code listed in `RestartForceExitStatus=`;
  - workers also check against the `Handoff` version at start.

### S-9. Dependency pins and the CI matrix
- **Tier:** low-risk. **Size:** S.
- **Basis:**
  - `ApiServer` subclasses werkzeug internals (`run_wsgi`, `make_environ`, `send_response`), yet werkzeug isn't pinned (lock: 3.1.8).
  - `pinned_session` overrides requests/urllib3 internals (`cert_verify`, `init_poolmanager`), and neither is capped.
  - Python 3.15 is not in CI (there are local 3.15 pycaches).
- **Fix:**
  - `zfsbackup-developer`: `werkzeug >=3.1,<3.2`, `urllib3 >=2,<3`, `requests <3`.
  - `pytest-test-author`: a weekly unlocked "latest deps" CI job, and 3.15 in the matrix once released.

### S-10. CI for the real tier and a release checklist (follow-up 11)
- **Tier:** low-risk. **Size:** S (spike) / M.
- **Fix:**
  - `pytest-test-author`: spike `apt install zfsutils-linux && modprobe zfs` on `ubuntu-24.04` (OpenZFS 2.2). If it works, run a file-backed pool plus `ZFSB_REAL_ZFS=1 pytest -m real_zfs` nightly and on demand. If not, a self-hosted runner on `zfsb-ci` (needs your OK).
  - The two-VM harness stays a manual **release checklist** (docs: run `test_two_vm_backup.sh`, `test_config_permissions.sh` and the B-8 install scenario on zfsb-* VMs, and record results in `two_vm_backup_report.md`). Nested Multipass in CI isn't worth it.

### S-11. Back up the config DB and `<db>.tls`
- **Tier:** needs-approval (handles private keys). **Size:** M.
- **Basis:**
  - `export` omits the trust tables, `host_identity` and keys.
  - The README warns against copying a DB into place under a running daemon.
  - Losing the server's `<db>.tls` means every client must re-enroll; losing a client's DB loses its client id.
- **Split:**
  - `zfsbackup-store-developer`: a `backup_database(dst)` using SQLite's online backup API (consistent under WAL, keeps `user_version`).
  - `zfsbackup-cli-developer`: `zfsbackup-config backup DIR`, writing the DB copy plus `.tls` (dir `0700`, keys `0600`). It is reused by D4.
  - Docs: restore = stop the daemon, copy, start.

### S-12. Make TLS validity failures visible (follow-up 10)
- **Tier:** low-risk. **Size:** S. **Owner:** `zfsbackup-developer`.
- **Basis:**
  - Server handshake failures are DEBUG only (`workers.py:1302`), so a pinned client whose certificate is expired or not yet valid is invisible on the server.
  - The client's 30-day expiry warning runs only at start/reload (`daemon.py:294-417`).
  - Clock skew: `server_context` lets OpenSSL check `notBefore` with only the 5-minute backdate (`identity.py:214`). A client more than 5 minutes ahead fails handshakes briefly after enroll or rotate. This is transient; document NTP.
- **Fix:**
  - a WARNING (rate-limited per IP) when the verify code is "expired" or "not yet valid";
  - the remote worker re-checks client certificate expiry daily;
  - make sure `trust list` shows `not_after`.

### S-13. Logging that works with journald
- **Tier:** low-risk. **Size:** S. **Owner:** `zfsbackup-developer`.
- **Basis:**
  - `daemon.py:76-80` and `workers.py:93-98` log everything to stdout with no priority, so journald records every line at info and `journalctl -p err` (or alerting on priority) finds nothing.
  - werkzeug writes an INFO access line for every request, including anonymous scans.
  - Only `-v` controls the level.
- **Fix:**
  - when `JOURNAL_STREAM` is set, prefix lines with `<N>` sd-daemon priorities and drop `asctime`;
  - add `--log-level`;
  - log routine 2xx access lines at DEBUG.

### S-14. Adopt libzfseasy timeouts (X5)
- **Tier:** needs-approval. **Size:** S/M. **Depends on:** libzfseasy S2.
- **Recommendation:**
  - use the timeouts for non-streaming `zfs` calls in API request handlers, so a hung pool gives 503 instead of a stuck handler thread holding a slot;
  - optionally for list/get in the workers;
  - do **not** add `StreamHandle.abort()` to transfers; that keeps D5's never-cut rule.
  - Note: a process in D-state cannot be killed anyway, so this frees threads, not processes.

### S-15. Documentation refresh
- **Tier:** low-risk. **Size:** S.
- **Owners:** main session for `docs/` and `CLAUDE.md`; `zfsbackup-developer` for `zfsbackup/README.md`.
- **Scope:** fix the stale entries listed in the appendix, and rewrite the readiness report against this plan.

---

## 3. Can ship without (document as known limitations)

- **K-1. werkzeug's server (D5).**
  - The usual objections to the development server are each already addressed in `ApiServer`:
    - per-connection handshake with a timeout;
    - 64/16 connection caps;
    - an absolute header deadline;
    - a drain deadline;
    - a sessioned websocket socket.
  - Replacing it is an L-size rewrite with nothing equivalent available:
    - gunicorn's prefork model conflicts with the supervisor and the live pin-context swap;
    - cheroot/waitress don't provide `werkzeug.socket` for flask-sock, and waitress has no TLS;
    - asyncio means rewriting every route and spike.
  - Keep it, pin it (S-9), and stop calling it "the development server" in the docs.
- **K-2. Writes inside `SSL_read` are not under the write lock (8a).**
  - Only a pinned peer can trigger it, by requesting a TLS 1.3 KeyUpdate, and the server's route thread writes almost nothing during a stream.
  - Optional S-size probe (`pytest-test-author`, `zfsb-ci`): send `K` from `openssl s_client` mid-session under load.
- **K-3.** `require_ca` mode is not a blocker.
- **K-4.** Server certificate re-keying is unsupported. The certificate is self-signed for 10 years and clients ignore its expiry by design; a compromised server key means re-enrolling every client.
- **K-5. `dedupe_store_warnings` installs a global filter** (`api.py:892-899`).
  - Harmless in production, because the API worker is its own process.
  - Optional low-risk S cleanup: return an uninstall handle and drop the conftest workaround.
- **K-6. M7 (no coordination between workers) is mitigated.**
  - The inflight pin is written before `zfs send`, the pruner always keeps the newest snapshot and anchors, and ZFS itself refuses to destroy a snapshot being sent (EBUSY, handled by B-4's retry).
  - Residual: one failed cycle. Recommend a mocked race test; no code change.
- **K-7.** No bookmarks. A removed destination's stale `anchor.*` pins one snapshot per dataset (the protect-all decision).
- **K-8.** The daemon runs as root (an earlier deferral).
- **K-9.** No metrics endpoint; B-6 `--prometheus` covers metrics.
- **K-10.** No bandwidth limit and no automatic per-client quota (document `zfs set quota`). Remote sends are serial per host.
- **K-11.** `--test-config` doesn't check that datasets exist.
- **K-12.** A trusted peer can still postpone a reload by actively streaming (D5). S-2 removes the abuse cases.
- **K-13.** Received-source `anchor.*`/`inflight.*` properties can pin snapshots on the server (disk growth only, after B-1/B-2).
- **K-14.** The CLI never sends SIGHUP on macOS; the 10 s poll applies the change.

---

## 4. Recommended order (milestones and PRs)

1. **PR 1: server trust boundary (blockers).**
   - Sequence: B-1 (after libzfseasy B1–B3) → B-2 → B-3 (a → b → c ∥ d) → S-4 → S-5.
   - Test lanes (B-7 assertions; scenarios L1 and L2) run alongside.
   - Re-review: zfs-code-reviewer. Verification on `zfsb-ci` with OpenZFS 2.4.1 and 2.2.
2. **PR 2: snapshot and send correctness.**
   - B-4 → S-1 → B-5 (and S-6 if D3 = now).
   - All are retention/send items: needs-approval, and opus for S-1.
   - Rerun two-VM scenarios B and J.
3. **PR 3: real-pool verification** (starts in parallel with PR 1). B-7 base rewrite, then the S-10 spike. Assertions are added as PRs 1–2 land.
4. **PR 4: install, operate, restore.**
   - B-8 (a ∥ b, then c and the install scenario), B-6 (a → b), B-9, S-13, S-9, S-11, S-15.
   - Mostly low-risk; the store/CLI/scenario lanes run in parallel with the daemon-core lane.
5. **PR 5: hardening for 1.0.**
   - S-2, S-3, S-7, S-8, S-12, S-14, B-1d (X4), K-5.
   - Concurrency-reviewer on S-2, S-3, S-7 and S-8.

`zfsbackup-developer` is the serial bottleneck (PRs 1, 2, 5 and the core half of 4). The store, CLI, test and scenario lanes run beside it. Every Linux-sensitive item (TLS, sockets, flock, systemd, ZFS) is verified on `zfsb-ci` or the zfsb VMs, never only on macOS.

---

## Appendix: re-verified as fixed (dropped) and stale docs

**Fixed:**
- B1 (TLS/mTLS) and B2 (`requests`).
- H3: `pytest.ini` now has `--cov=zfsbackup`. No threshold is enforced.
- H4 resume.
- Reload-time API bind check; systemd unit; restart backoff.
- Anchor repair when common == latest after a lost ack: `_settle_up_to_date`, `remote.py:1068`.
- Follow-up 8c: a websocket route that raises after the upgrade has its 500 discarded and still logged (`workers.py:1128-1147`; tests in `test_zfsbackup_api_wssession.py` ~:1656/:1715).
- The client certificate expiry warning exists (start/reload only; see S-12).

**Stale docs:**
- `production_readiness_report.md`:
  - H3 is listed as open;
  - M6 cites `workers.py:171` (now `:648`);
  - M8 cites `backup_manager.py:168` (now `:537-577`);
  - remediation step 5 still lists the bind check, which is done;
  - "werkzeug development server" wording (see K-1).
- `two_vm_backup_report.md`: H5 cites `remote.py:198` (now `:1301-1313`).
- `tls_mtls_plan.md` status note: says the M17 run "is still to be recorded"; it was recorded on 2026-10-21 (231/0).

---

## For your approval

**What this proposes:**
- **Nine blockers:**
  - cross-tenant property injection (B-1);
  - receive hardening (B-2);
  - a server retention floor (B-3);
  - prune batch fallback (B-4);
  - `-I` or chained sends plus frozen send flags (B-5);
  - a health check (B-6);
  - a real-pool pytest end-to-end test (B-7);
  - install/upgrade packaging (B-8);
  - a tested restore (B-9).
- **Fifteen should-fix items** for 1.0.
- **Fourteen documented limitations.**

**Confirm or change:**
- D1–D5 above.
- The ownership assumptions for `packaging/**` and `.github/workflows/**`.
- The strict snapshot-name parse in S-1.
- That B-1 waits for libzfseasy B1–B3.

**Tiers:** everything is `needs-approval` except B-6, B-7, B-8 (apart from D4), B-9, S-9, S-10, S-12, S-13, S-15 and K-5.

Approve, adjust, reprioritise or drop any item. Nothing is implemented before your sign-off.

**Key files:**
- `/Users/rejsmont/src/libzfs/zfsbackup/api.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/backup_manager.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/remote.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/workers.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/daemon.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/identity.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/model.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/store/`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/`
- `/Users/rejsmont/src/libzfs/zfsbackup/README.md`
- `/Users/rejsmont/src/libzfs/pyproject.toml`
- `/Users/rejsmont/src/libzfs/.github/workflows/tests.yml`
- `/Users/rejsmont/src/libzfs/tests/test_zfsbackup_real.py`
- `/Users/rejsmont/src/libzfs/docs/production_readiness_report.md`
- `/Users/rejsmont/src/libzfs/docs/two_vm_backup_report.md`
- `/Users/rejsmont/src/libzfs/docs/tls_mtls_plan.md`

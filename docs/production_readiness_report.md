# zfsbackup — Production Readiness Report

**Date:** 2026-07-07 (updated 2026-10-20: B1 resolved by the TLS/mTLS milestone)
**Scope:** `zfsbackup` (remote backup daemon) and `libzfseasy`, focused on the
client↔server remote-backup feature.
**Verdict:** **The security blocker (B1) is resolved; the remaining items below are residual risks and
completeness gaps, not blockers.** The local snapshot + retention core is solid and anchor-aware; the
remote-backup transport is now TLS 1.3 with pinned mutual authentication.

The transfer *happy path* is verified working end-to-end over two real VMs — see
[`two_vm_backup_report.md`](two_vm_backup_report.md). A green functional run is **not** a security or
completeness sign-off; read the residuals under B1 before exposing the API beyond a trusted network.

---

## Blockers

### ~~B1 — Server API has no authentication, authorization, or TLS~~ — **RESOLVED**
Originally `zfsbackup/api.py` exposed the whole ingest surface with no credential: registration was
open, the WebSocket handler piped arbitrary client bytes into `zfs receive -F` under any
`target/<client_id>`, and the client itself downgraded `https`/`wss` to `http`/`ws`.

**Resolved** by the TLS/mTLS milestone ([tls_mtls_plan.md](tls_mtls_plan.md)):

- **Transport.** HTTPS only, TLS 1.3 minimum, no plaintext mode on either side (the client has no
  `http://`/`ws://` path, and an `http://` destination is refused at load). The client pins the server's
  certificate by SHA-256 fingerprint and checks it before sending a byte; the server pins client
  certificates as exact trust anchors (`zfsbackup/identity.py`).
- **Authentication and authorization.** Every endpoint except `/health` and `/enroll` needs a pinned
  client certificate. The client id a request acts as is the id the certificate is bound to, never the
  one in the URL (403 on a mismatch), so one client cannot touch another's datasets; `/backup/register`
  was removed.
- **Enrollment.** A one-time token (1 h by default, 7 d at most, 256-bit, shown once, stored hashed)
  created on the server with `zfsbackup-config trust token create`; the client proves possession of its
  key (`enroll`). Client certificates rotate (`identity rotate`) and can be revoked.
- **Revocation is permanent.** Revoked, replaced and retired fingerprints are kept in
  `revoked_fingerprints` and refused for ever on enroll and rotate. Revoking a key also revokes every
  certificate that descends from it by rotation, used or not.
- **The reload-postponement item below is narrowed:** only trusted senders can hold a stream open.
- Both sides were hardened against unauthenticated peers: a per-connection handshake with a 30 s timeout,
  at most 64 connections (16 without a client certificate), an absolute 30 s deadline for request
  headers, and a read-only token pre-check before `/enroll` reaches any `zfs` call.

**Residual risks** (none blocks a deployment on a network you control; all are open):

- **werkzeug's development server** serves the API (see Operations). No production WSGI server.
- **No mid-stream revocation.** A revoked client's stream already in flight finishes; revocation refuses
  new connections within about 2 s.
- **No rate limiting on `/enroll`.** It is reachable without a certificate and is bounded only by the
  connection caps; a wrong token enrolls nothing, but it can be guessed at the speed the caps allow
  (256-bit secret) and it costs the server a request each. Restrict the port to your clients by firewall.
- **Client-driven server retention (M8)** is unchanged, but now only authenticated clients can drive it.
- **A trusted peer can postpone a reload.** On a reload the API worker waits for in-flight receives and
  never cuts them (decision D5); the only per-stream bound is the 600 s websocket idle timeout, so a
  trusted client that sends a frame every ~10 minutes keeps the old configuration running on the server
  side. Deliberately not capped; before B1 this was open to any peer.
- **`TestRealSendReceive`** (`tests/test_zfsbackup_real.py`, `real_zfs` tier) is skipped pending a rewrite
  around an enrolled client.

Path traversal via `client_id`/`dataset` is *mitigated* (the `Validate` name regex rejects `..` and shell
metacharacters; the client-id grammar is `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`); a validation failure now
answers 400.

### ~~B2 — `requests` is imported but is not a declared dependency~~ — **RESOLVED**
`zfsbackup/remote.py:11` does `import requests`, but `pyproject.toml` declared only
`pyyaml`, `flask`, `flask-sock`, `websocket-client`. `requests` was present only transitively
via the **dev**-only `requests-mock`. A clean `poetry install --without dev` yielded an
environment where the entire client-side remote-backup path died with `ModuleNotFoundError`
on first use. (The two-VM harness worked around this by `pip install requests` explicitly.)

**Resolved** by item 1 of [config_db_cli_plan.md](config_db_cli_plan.md): `requests = "^2.31"` is now a
declared direct dependency. `click` was promoted from transitive-via-Flask to direct in the same change,
closing the identical latent gap before it could bite.

---

## High

- **H3 — Coverage does not measure `zfsbackup`.** `pytest.ini` uses `--cov=libzfseasy`, and
  the real end-to-end tests are `-m "not real_zfs"` by default, so the WebSocket `/stream`
  receive path has no coverage in a normal run. Any quoted coverage number describes
  `libzfseasy` only.
- **H4 — Resume.** *(Resolved 2026-10-19 on branch `libzfseasy-fixes`.)* Before this, the server's
  `/resume_token` always returned 500 (libzfseasy refused the `receive_resume_token` property), so resume
  never ran, and nothing ever called `zfs receive -A`: one interrupted transfer could block a dataset until
  an operator cleared it. Now:
  - **The client checks the token before using it:** `zfs send -nv -t` decodes it; the target must match a
    managed local snapshot of the same dataset by GUID (a token's `toname` goes stale after a rename); the
    source must be older; the token's flags must fit the send mode (milestone B: plain `-L -c` accepts
    ∅ ⊆ flags ⊆ {compressok, largeblockok}, raw `-w` requires rawok and allows compressok/largeblockok).
  - **Only definitive staleness leads to a fresh send.** Anything uncertain skips the cycle rather than
    discard the server's partial progress.
  - **`/stream?mode=fresh|resume`:** on `fresh`, the server discards stale partial state with an
    idempotent `zfs receive -A`.
  - **Recording and catch-up:** the anchor records the snapshot actually sent, never backwards. A resume
    of an older snapshot continues to `latest` in the same cycle.
  - **In-flight pin:** `org.zfsbackup:inflight.<dest>` protects snapshots being sent from pruning.
  - **Collisions:** two client datasets that map to the same server dataset are refused with 409.
  - **Per-dataset lock:** a second stream for the same server dataset gets a retryable "busy".
  - **Covered by:** two-VM scenario J (interrupt, resume, stale token, pin, collision).
  - **Remaining caveats:**
    - Client and server must be upgraded together. A new client treats an old server's `/resume_token`
      500 as uncertain and skips every cycle; an old client's `/stream` without `?mode=` is refused by a
      new server.
    - The "definitively stale" check matches zfs's English messages. `resume_info` runs with
      `LC_ALL=C` so localized hosts still match, but any token problem that isn't on the definitive list
      (an unfamiliar wording from another OpenZFS version, EIO) skips the cycle until the server operator
      runs `zfs receive -A`. That favours keeping partial progress over making progress.
    - Resume-token checks happen only on the client; the server relies on the authenticated client id
      and the per-dataset lock.
    - *(Resolved in milestone B, S-6.)* Encrypted datasets are sent raw (`-w`) under the per-destination
      `raw: auto` default, so the server never holds the key; a raw/plain mismatch with an existing server
      copy is refused with the rename fix.
    - Resume within the milestone-B `-i` chain is covered (a resume runs as step 0, then the rest of the chain).
- **H5 — Incremental sends use `-i`, not `-I`.** *(Resolved in milestone B, B5-b.)* Every managed snapshot now
  reaches the server as a chain of `-i` steps, oldest first by `createtxg`; unmanaged snapshots are never
  sent. Before, `RemoteBackupManager._transfer` emitted a single increment and the server missed the
  intermediate snapshots (live-confirmed in the two-VM run).

---

## Medium

- **M6 — Silent backup failures.** `RemoteBackupWorker._process_dataset` ignores the
  boolean return of `backup_dataset()` (`workers.py:171`). A server that is down produces a
  single log line — no retry, backoff, alert, or "last backup succeeded" signal.
- **M7 — No inter-worker coordination.** Snapshot, pruning, and remote workers are separate
  processes with no locking. Retention shorter than remote frequency can prune a snapshot
  before it is sent (mitigated by always keeping the newest + excluding anchors, but
  unguarded and untested).
- **M8 — Server retention is driven by client-supplied config.** `received_datasets()`
  loads retention rules from a base64 property the client wrote (`backup_manager.py:168`),
  and there is no server-side anchor protection, so a misconfigured/malicious client can
  drive deletion of its own server-side history down to the newest snapshot.
  *(Update 2026-10-18: the pruner now protects every local or received `org.zfsbackup:anchor.*`
  snapshot. Conversely, because `zfs receive` does not strip user properties, a modified client could
  send a `-p` stream carrying `anchor.*` properties that pin snapshots on the server indefinitely — no
  new exposure, since `retention: forever` in the client-supplied config already does the same.
  Since B1 was resolved only an authenticated (enrolled) client can do either.)*

---

## Operations

- *(Updated 2026-10-19.)* A documented systemd unit ships in `zfsbackup/README.md`: `Type=notify-reload`
  (systemd ≥ 253; `Type=notify` + `ExecReload` for older), `NotifyAccess=main`, `KillMode=mixed` and
  `TimeoutStopSec`. The daemon sends `READY`/`RELOADING`/`STOPPING`/`STATUS` via `sd_notify`, holds a
  presence lock and pid file (`<db>.daemon.pid`), and reloads its configuration live on SIGHUP and on a
  10 s poll.
- Worker restarts use exponential backoff (5 s to 80 s). After 6 consecutive short-lived crashes the
  daemon exits 1 and names the configuration generation (A-5, D9). Workers orphaned by a dead forkserver
  are not counted.
- *(Resolved 2026-10-19.)* A reload bind-probes a changed API address; one that cannot be bound is
  *deferred* (re-checked every poll) instead of crash-looping the API worker into D9. Residual: a port
  taken between the probe and the bind, or a third party already on `new_host:port` after a host-only
  change, still ends in D9. The supervisor makes no `zfs` call, so a hung pool no longer stalls reloads,
  signals or restarts.
- `--test-config` validates the config and builds the dataset list. It does not check that datasets
  exist or that destinations resolve.
- Werkzeug's development server (`make_server`, with a per-connection TLS handshake on top) is used in
  the `ApiWorker` — not a production WSGI server. Residual risk listed under B1.

**Good:** graceful `SIGINT`/`SIGTERM` shutdown via a shared `StopFlag` (was a `multiprocessing.Event`, which wedged the supervisor whenever a worker died mid-`wait()` — see plan item 8x); anchor-aware retention
math is correct under the real_zfs tests; dataset-name validation blocks injection.

---

## Note on stale docs

`CLAUDE.md`'s "SnapshotManager partially wired in / daemon loop in progress" is **stale** —
the daemon loop, workers, and pruning are implemented and functioning. The security
layer (TLS and mutual authentication, B1) was finished 2026-10-20; resume was finished 2026-10-19.

---

## Suggested remediation order

1. ~~B2 (declare `requests`)~~ — done.
2. ~~B1 (auth + TLS on the API; validate/authorize `client_id`)~~ — done; residuals under B1.
3. H5 (`-i` → `-I`) — correctness/completeness. (H4, resume, is done.)
4. M6 (surface backup failures) and M7/M8 (pruning safety) — reliability hardening.
5. Ops: production WSGI, `/enroll` rate limiting, and the reload-time API bind check (the systemd unit and restart backoff are
   done).

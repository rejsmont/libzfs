# zfsbackup — Production Readiness Report

**Date:** 2026-07-07
**Scope:** `zfsbackup` (remote backup daemon) and `libzfseasy`, focused on the
client↔server remote-backup feature.
**Verdict:** **NOT ready for real-life deployment.** The local snapshot + retention core
is solid and anchor-aware; the remote-backup feature has security and completeness blockers.

The transfer *happy path* is verified working end-to-end over two real VMs — see
[`two_vm_backup_report.md`](two_vm_backup_report.md) (15/15). A green functional run is
**not** a security or completeness sign-off; the issues below are what block deployment.

---

## Blockers (must fix before any non-loopback deployment)

### B1 — Server API has no authentication, authorization, or TLS
`zfsbackup/api.py` exposes the entire ingest surface with no credential of any kind.

- Registration is open: `backup_register` accepts any `client_id` and creates a dataset
  (`zfs.create(Filesystem(client_root), parents=True)`).
- The WebSocket handler pipes arbitrary client bytes straight into
  `zfs.receive.filesystem(server_fs, force=True, save=True, mount=False)` (`api.py:171`).
  `force=True` lets any caller overwrite/roll back existing datasets under
  `target/<client_id>`, with no check that the caller "owns" that path.
- No TLS. The client even downgrades the scheme itself:
  `ws_base = base_url.replace('https://','wss://',1).replace('http://','ws://',1)`
  (`remote.py:139`). Combined with non-raw sends, encrypted datasets go over the wire in
  cleartext.
- Default `api_host` is `127.0.0.1` (safe but useless for remote backup). The moment you
  bind `0.0.0.0` to make the feature work, it is wide open to anything routable.

Path traversal via `client_id`/`dataset` is *mitigated* (the `Validate` name regex rejects
`..` and shell metacharacters) but handled by raising an uncaught `ValueError` → HTTP 500
rather than a clean rejection.

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
- **H4 — Resume is half-built and untested.** The client sends `?from=<snap>`
  (`remote.py:141`) but the server never reads that query parameter (`api.py`, `/stream`
  handler) — dead code. No interrupt-then-resume test exists; the resume probe swallows all
  exceptions.
- **H5 — Incremental sends use `-i`, not `-I`.** `remote.py:198` emits a single increment,
  so snapshots created *between* two backups are never replicated; the server's history
  diverges from the client's. **Live-confirmed** in the two-VM run: the server was missing
  intermediate snapshots the client had.

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

---

## Operations

- No systemd unit / daemonization / PID file; the daemon runs foreground only.
- Worker auto-restart loops every 5s indefinitely with no backoff cap (`daemon.py`).
- `--test-config` validates parsing only — not that datasets exist or destinations resolve.
- Flask's dev server (`app.run`) is used in the `ApiWorker` — not a production WSGI server.

**Good:** graceful `SIGINT`/`SIGTERM` shutdown via a shared `Event`; anchor-aware retention
math is correct under the real_zfs tests; dataset-name validation blocks injection.

---

## Note on stale docs

`CLAUDE.md`'s "SnapshotManager partially wired in / daemon loop in progress" is **stale** —
the daemon loop, workers, and pruning are implemented and functioning. The genuinely
unfinished areas are the security layer and resume.

---

## Suggested remediation order

1. B2 (declare `requests`) — trivial, unblocks clean installs.
2. B1 (auth + TLS on the API; validate/authorize `client_id`) — the real deployment gate.
3. H5 (`-i` → `-I`) and H4 (finish or remove resume) — correctness/completeness.
4. M6 (surface backup failures) and M7/M8 (pruning safety) — reliability hardening.
5. Ops (systemd unit, production WSGI, restart backoff).

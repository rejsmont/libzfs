# Changelog

## Unreleased

### Packaging

- **Two distributions.** `libzfseasy` is now its own wheel (`libs/libzfseasy/`, standard library only) and
  `zfsbackup` depends on exactly the same version. **Upgrading a 1.0.0rc1 virtualenv: `pip uninstall -y zfsbackup`
  first**, then install both wheels; a plain `pip install` over the old single wheel deletes `libzfseasy`. The
  version guard now watches both installs.
- **Debian packages** for Debian 13 and Ubuntu 26.04 (`python3-libzfseasy`, `zfsbackup`), built on the
  distribution's own Python libraries and attached to the release. Units are installed disabled; an upgrade stops
  the daemon and starts it again if it is enabled; an upgrade of a Python library it uses restarts it (OpenSSL:
  `needrestart`, recommended); purge keeps `/var/lib/zfsbackup`.
- **OpenZFS 2.3** (Debian 13, 2.3.9) is a tested version: in the OpenZFS matrix (a Debian 13 VM) with format captures.
- `flask-sock` 0.7.0 is vendored (it is in no Debian or Ubuntu archive). The `flask` and `websocket-client`
  floors are lowered to Debian 13's versions (3.1.1, 1.8.0).

## 1.0.0rc1 (2026-10-27)

First release candidate of `zfsbackup` (the distribution ships both `zfsbackup` and the `libzfseasy` bindings). It is
the production-readiness plan, milestones A to D, complete. It has been tested on VMs only: run it on a real client and
server before relying on it. 1.0.0 will be this code unless running it finds a problem.

### Compatibility

- **Wire protocol 1** (`X-Zfsbackup-Protocol: 1`). Clients and servers must run the same protocol; a mismatch is
  refused with 426 and remote backups pause.
- **Config schema head `5d2f8a60c1e4`.** The systemd unit migrates the config database in `ExecStartPre`, after a
  backup.
- **Frozen send flags:** plain sends `-L -c`, raw sends `-w` (per remote: `raw: auto|true|false`). A later change to
  these would need every server copy re-sent.
- **Supported:** OpenZFS 2.1, 2.2 and 2.4 (2.3 best-effort); Python 3.11 to 3.15; Ubuntu 22.04 (with a newer
  Python and the systemd < 253 drop-in) and 24.04 or later. macOS is supported for development only.

### What it does

- **Local snapshots and retention.** Scheduled snapshots per dataset, tiered slot-based retention, ordered by
  `createtxg` and safe across clock jumps; held and cloned snapshots are skipped, a failed batch is retried one
  snapshot at a time.
- **Remote backup** to a zfsbackup server over TLS 1.3 with mutual authentication and pinned certificates
  (`enroll`, `identity rotate`, `trust …`). Every managed snapshot is sent, as a chain of incremental streams,
  resumable after an interruption; encrypted datasets can be sent raw.
- **The server never rolls back its history**, receives only plain single-snapshot streams it has checked, keeps
  received copies inert (never mounted, shared or exposed as devices), writes only into datasets it created, and
  never lets one client's values reach another client's data.
- **Pruning fails closed:** ambiguous `zfs` output stops pruning of that dataset (one ERROR naming the fix); it never
  shrinks the set of protected snapshots.
- **Operations:** a SQLite config store edited with `zfsbackup-config` (`get`/`set`/`edit`/`rename`/`import`/
  `export`), live reload without interrupting transfers, `zfsbackup-config check` for monitoring (Nagios and
  Prometheus output), `zfsbackup-config backup` of the database and keys, systemd units in the sdist.
- **Hardening:** a revoked client's running stream is cut within seconds; per-address flood control and an
  `/enroll` lockout; per-request time limits on the server's `zfs` calls; worker outcomes recorded in status files
  that `check` grades; an in-place upgrade under a running daemon is detected, transfers drain, and the daemon exits
  76 so systemd restarts it once.

### Installing and upgrading

Follow `zfsbackup/README.md` (install, enroll, upgrade, restore, monitoring). This is the first release, so there is
nothing to upgrade from. Stop, install, start remains the recommended upgrade path.

### Known limitations

Listed in `zfsbackup/README.md` ("Known limitations") and `docs/production_readiness_report.md`. The main ones:
werkzeug's server serves the API (hardened and pinned); a compromised or malicious enrolled client is out of scope
(it controls its own retention on the server); the daemon runs as root; no bandwidth limit, per-client quota
(use `zfs set quota`) or bookmarks; a dependency-only upgrade is not detected by the version guard (restart the
daemon).

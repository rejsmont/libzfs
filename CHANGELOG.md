# Changelog

## 1.0.0rc5 (2026-11-09)

One backup dataset for remote clients and this host's own local copies.

### Compatibility

- **Wire protocol 1** and the **config schema head** are unchanged: no migration. A configuration whose local
  destination targets the server's own `remote_backup.target_dataset` is refused by rc4 (move the local target
  elsewhere before going back).
- A server whose local destination shares its target now answers 409 `reserved_client_id` to a client bound to
  the server host's own client id (`/enroll`, `/negotiate`, `/stream`, `/resume_token`). Without a shared target
  nothing changes.

### Changes

- **A local destination may share the server's target.** On a host that is also a backup server, `url: "zfs:T"`
  may name the server's own `remote_backup.target_dataset` `T` (inside or above it stays refused), so one dataset
  holds the remote clients' copies and this host's own, at `T/<client_id>/…` like any client's. While shared, the
  server reserves this host's client id: enrolling it and receives from a client bound to it are refused (409,
  `reserved_client_id`), and the CLI refuses `trust token create --client-id` and `identity set-client-id` that
  would collide. Moving from a loopback setup into the same `T` is now a URL change plus `trust revoke --client`,
  with no `zfs rename`. See `zfsbackup/README.md`, "Sharing the server's target".
- **`zfsbackup-config enroll`** explains a 409 `reserved_client_id` (the token is not used; give this host another
  client id and enroll again).
- **`zfsbackup-config list destinations`** marks a shared target (`local (shared)`, JSON `"shared"`).

## 1.0.0rc4 (2026-11-09)

Local destinations: back a dataset up to another dataset or pool on the same host, with no server.

### Compatibility

- **Wire protocol 1** and the **config schema head** are unchanged: no migration. A `zfs:` URL is stored in the
  existing `destinations.url` column. An rc3 daemon or CLI refuses a database that holds one (its destinations
  must be `https://`), so remove local destinations before going back to rc3.
- **Send flags, `raw` semantics and the server's dataset layout** unchanged.

### Changes

- **Local destinations: `url: zfs:<dataset>`.** A destination whose URL is `zfs:backup/zfsbackup` is sent to with a
  direct `zfs send | zfs receive` on this host: no HTTP, no TLS, no enrollment, no fingerprint. It is the same
  chain as a remote backup (incremental `-i` steps, resumable receive, raw or plain per dataset and destination,
  anchors and pins, the same rollback and ownership refusals), laid out the server's way,
  `<target>/<client_id>/<pool>/<path>`, with the server's forced properties on every copy (`readonly=on`,
  `canmount=noauto`, `mountpoint=none`, `volmode=none`, share/setuid/exec/devices off, `-u`, `-h`). The remote
  worker sends to local and remote destinations one after another. The copies are pruned by the retention stamped
  on each (this host's own client id only, never recursively, newest always kept). An exported or hung target pool
  skips the destination with one message per episode instead of stalling the worker (non-streaming `zfs` calls
  have a 60 s budget, a stream that moves no byte for 600 s is stopped and resumed later). The target dataset is
  created by the operator; see `zfsbackup/README.md`, "Local destinations", for the setup, restore, removable
  pools and the move from a loopback setup (one `zfs rename` of `<target>/<client_id>`).
- **New validation rules.** A local target must be disjoint from every configured dataset (at or below it, or a
  `recursive` one above it), from every other local target and from an enabled `remote_backup.target_dataset`;
  `url` is `zfs:` plus a valid dataset name (exact lowercase prefix; no `//`, `/`, `@` or `#`) and a `zfs:`
  destination never has a `server_fingerprint`; a copy's snapshot name must fit ZFS's 255 characters (an error
  for an enabled dataset, a warning when only a long client id would not fit).
- **`zfsbackup-config`.** `list destinations` shows the kind (`--json`: `kind`, `target_dataset`); `enroll` refuses
  a local destination; `identity rotate` skips it; `set`/`edit` warn when a destination's target or kind changes
  and refuse a kind switch while a `server_fingerprint` is set; `rename dataset` prints the copy's path and, when
  its parent is unchanged, the commands that move it; `identity set-client-id` warns that local copies are orphaned; `check` grades the
  client certificate only when an `https://` destination exists. An `import`, `set` or `edit` that adds the first
  local destination to a host without a client id generates one (`generated this host's client id: …`).
- **Fix: raw copies on OpenZFS 2.1 could inherit `sharesmb`.** On 2.1 a raw `zfs receive` skips the forced
  `-o sharesmb=off` (it prints "cannot receive sharesmb property … must be upgraded" and still succeeds), so a raw
  server copy inherited the target's `sharesmb`. After every successful raw receive the server (and a local
  destination) now sets any forced property that did not land locally. Plain receives and 2.2+ were not affected.
- **A failure at one destination no longer abandons the dataset's other destinations.** An exception from one
  `remote:` entry is logged, recorded against that destination in the status file and `check`, and the rest of
  the dataset's destinations still get their turn in that cycle (remote destinations too).
- **libzfseasy: `StreamHandle.terminate()`, `.poll()` and `.pid`.** `terminate()` stops a stream's child (SIGTERM,
  SIGKILL after 5 s, bounded) without closing the stream, abandons a child that survives SIGKILL in
  `stuck_children()` so that a later `close()` does not block on it; `poll()` is a non-blocking status check
  for a bounded close. `close()` still collects stderr and the exit status.
- **libzfseasy: `zfs.send.snapshot/dataset/redact/resume/partial(..., peek=False)`** (keyword-only, default
  `True` = unchanged) returns the handle right after spawning, without the blocking read of the first byte, for
  callers that bound the first read themselves; an immediate failure then shows as EOF and is reported by
  `close()`.
- Internal: the send chain moved out of `remote.py` into `zfsbackup/chain.py` (`ChainSender`) and the receive-side
  helpers out of `api.py` into `zfsbackup/receiver.py`; the remote client and the API behave as before.

### Known limitations

New in `zfsbackup/README.md` ("Known limitations", "Local destinations"): local and remote sends share one
worker; a `zfs receive` stuck on a hung pool is not remembered across worker restarts (ZFS refuses the overlap,
nothing is corrupted); an exported target pool makes `check` WARN then CRIT until it is imported again.

## 1.0.0rc3 (2026-11-08)

Third release candidate: one change to the server's dataset layout and one fix.

### Compatibility

- **Wire protocol 1**, unchanged: rc2 and rc3 clients and servers work together. The server alone decides where a
  copy is stored, so the layout below follows the **server's** version.
- **Config schema head `5d2f8a60c1e4`**, unchanged: no migration from rc2.
- **Send flags and `raw` semantics** unchanged.
- **The server dataset layout changed** (below). A server upgraded from rc1/rc2 does not continue the copies it
  already holds: each client dataset is sent in full again to its new path, and the old copies stay where they
  are (still pruned by their own retention, newest snapshot always kept) until you destroy them. **Before the
  upgrade, rename a client's old pool-root copy (`<target>/<client_id>` itself, when it holds one) out of the
  target tree**: if it is encrypted (raw), the server cannot create the new `<target>/<client_id>/<pool>` level
  under it and no dataset of that client is backed up until it is moved (`zfsbackup/README.md`, "Layout").

### Changes

- **Server copies keep the client's pool name.** A client's `tank/data` is stored as
  `<target>/<client_id>/tank/data` (it was `<target>/<client_id>/data`), and a pool root `tank` as
  `<target>/<client_id>/tank` (it was `<target>/<client_id>` itself). Datasets of the same name in two pools of one
  client (`tank/data`, `fast/data`) are now kept apart instead of the second being refused with 409. Moving a
  dataset to another pool now gives it a new server path, so its next backup is a full send; `zfsbackup-config
  rename dataset` says so.

### Fixes

- **`trust token create` run twice at once on a host without a server TLS identity** could fail one of them with
  "certificate …/server.crt does not exist" while the other created it. The losing run now reads the certificate
  the other one wrote.

## 1.0.0rc2 (2026-10-29)

Second release candidate. The code is rc1's with three fixes; what is new is the packaging: `libzfseasy` is its own
wheel, and there are Debian packages for Debian 13 and Ubuntu 26.04.

### Compatibility

- **Wire protocol 1**, unchanged: an rc1 client and an rc2 server (or the reverse) work together.
- **Config schema head `5d2f8a60c1e4`**, unchanged: no migration from rc1.
- **Send flags and `raw` semantics** unchanged.
- **Upgrading an rc1 virtualenv needs `pip uninstall -y zfsbackup` first** (below, and `zfsbackup/README.md`,
  "Upgrading and downgrading").

### Fixes

- **A stop under Python 3.14 no longer ends with a SIGKILL.** With `forkserver` (3.14's Linux default) the
  supervisor now stops the multiprocessing forkserver before it exits (bounded at 3 s, one WARNING if something still
  holds it), so systemd no longer kills a leftover process on every stop, restart and upgrade exit.
- **libzfseasy: a second Ctrl-C during the cleanup of an interrupted command no longer leaves the `zfs` child
  running.** The cleanup now terminates and reaps the child before it stops the slow-command watchdog; an interrupt
  arriving while the watchdog's thread was joined used to skip the SIGTERM/SIGKILL.
- `trust revoke` now says that running streams are cut within ~2 s (rc1 already cut them; the text said they finish).

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

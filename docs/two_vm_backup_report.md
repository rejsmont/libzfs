# Two-VM Backup Transfer — Reliability Report

**Date:** 2026-07-07
**Harness:** [`test_two_vm_backup.sh`](../scenarios/test_two_vm_backup.sh)
**Scope:** End-to-end verification of `zfsbackup` snapshot transfer from a client machine
to a separate backup server, over the real network path (two Multipass VMs, real IP,
`0.0.0.0` bind, WebSocket over the Multipass bridge) — not the single-VM `zfs`-CLI routing
used by the pytest suite.

---

## Result: 15 / 15 passed — the transfer path works reliably

A clean end-to-end run verified all of the following against real ZFS pools on two VMs
(`zfsb-server`, `zfsb-client`), same daemon binary on both, role decided purely by config:

| Scenario | Verified behaviour |
|---|---|
| A — full initial backup | Snapshot transfers to server; **byte-identical** (guid match on the anchor snapshot) |
| B — incremental backup | Second snapshot transfers incrementally; anchor advances |
| C — server-down resilience | Client keeps snapshotting locally; logs the failure gracefully; anchor does **not** advance; daemon survives; **catches up** once the server returns |
| D — client restart | `client_id` stable across restart; next send is incremental (no full re-send) |

Integrity is checked via ZFS `guid` equality across the two VMs (preserved by
`zfs send`/`receive`), evaluated on the **anchor** snapshot (protected from pruning), so
it does not flake under the aggressive test schedule.

### Schedule used (deliberately short / human-followable)
- Client: `check_interval 15s`, snapshot `frequency 30s`, remote push `30s`, `prune_interval 1m`, retention `1m→10m`, `5m→1h`
- Server: `check_interval 1m`, `prune_interval 5m`

---

## Environment gotcha: `multipass exec` `/dev/null` wedge (macOS)

Most of the debugging time went to a **Multipass-on-macOS** bug, reproduced
deterministically (12/12):

> `multipass exec` busy-spins a host CPU core **forever** when the remote command writes
> to **stdout** and the **host** redirects that stdout to **`/dev/null`**
> (e.g. `multipass exec vm -- cmd >/dev/null`). The remote command exits; the host client
> never notices and spins.

| Host-side stream handling | Wedges? |
|---|---|
| `>/dev/null` or `>/dev/null 2>&1` (stdout with content) | **Yes, 100%** |
| `\| tr` / `\| sed` (host pipe) | No |
| `$(...)` command substitution | No |
| redirect **inside** the guest (`sh -c '... >/dev/null 2>&1'`, `curl -o /dev/null`) | No |

**Rule for any Multipass automation on macOS:** never send an `exec`'s *host* stdout to
`/dev/null` — discard output *in the guest* instead. This single bug produced all three
"hangs" seen while building the harness (config `tee >/dev/null`, daemon-start
`… & echo started >/dev/null`, health `curl … >/dev/null 2>&1`). Related, understood
constraints the harness also handles: `multipass exec` only returns when the remote
command exits (so the daemon is launched via `systemd-run`, which returns immediately),
and host stdin proxying is unreliable (so configs/source are delivered via
`multipass transfer`, not piped heredocs).

---

## Deployment readiness: NOT ready

A green harness run confirms the **happy path**, not production-readiness. Open blockers
(see also memory `readiness_zfsbackup.md`):

- **Blocker — no auth/authz/TLS on the server API** (`zfsbackup/api.py`): any host that can
  reach the port can `POST /backup/register` and stream into `zfs receive` (`force=True`).
  Binding `0.0.0.0` (required for remote backup) exposes it to anything routable.
- **Blocker — `requests` imported but undeclared** (`zfsbackup/remote.py:11` vs
  `pyproject.toml`): a clean `poetry install --without dev` cannot run the client. The
  harness works around this by `pip install requests` explicitly.
- **High — resume is dead code**: client sends `?from=<snap>`; server never reads it
  (`zfsbackup/api.py`). Not tested by the harness for that reason.
- **High — incrementals use `-i`, not `-I`** (`zfsbackup/remote.py:198`): snapshots created
  *between* two backups are never replicated. **Live-confirmed** in this run — the server
  was missing intermediate snapshots (e.g. `…114800`, `…114900`) that the client had.

The local snapshot + retention core is solid and anchor-aware; the gaps are the remote
(server) feature's security and completeness.

---

## Running it

```bash
./scenarios/test_two_vm_backup.sh              # run all scenarios; reuse VMs; recreate pools
./scenarios/test_two_vm_backup.sh --keep       # leave daemons + pools up for inspection
./scenarios/test_two_vm_backup.sh --keep-pools # destroy daemons, keep pools
./scenarios/test_two_vm_backup.sh --purge      # delete the VMs entirely at the end
```

Requires `multipass` on the host. VMs are persistent and reused across runs; pools are
recreated each run. Inspect preserved state after `--keep`:

```bash
multipass exec zfsb-server -- sudo zfs list -t snapshot -r zbserver/backups
multipass exec zfsb-client -- sudo tail -f /var/log/zfsbackup.log
```

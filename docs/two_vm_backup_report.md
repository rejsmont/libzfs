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

## Milestone 3: the harness over TLS / mutual TLS (plan item M17)

The API is now HTTPS only and every endpoint except `/health` and `/enroll` needs a pinned client
certificate, so the harness no longer starts a client against an open server. It enrolls the client the
way an operator would, and a new scenario K exercises the trust model itself.

**Enrollment in the harness.** After the server daemon is up (it creates `<db>.tls/server.{key,crt}` at
startup) the harness runs `zfsbackup-config trust token create --json` on the server, moves the token to
the client as a root-owned `0600` **file** (`multipass transfer`; never a command line, never piped
stdin), and runs `zfsbackup-config enroll server1 --url https://<server-ip>:8080 --token-file ...` as root
on the client. `CLIENT_ID` is read from `identity show --json` (the id lives in the client's config DB;
`client_id_file` is gone). Health checks and the J4 409 probe use `curl -k` (the J4 probe also presents the
client certificate); `-k` is acceptable because the pin is verified by zfsbackup's own client, not by curl.
Every `import` the harness makes carries the destination's `server_fingerprint` (an import is a full
replace and would otherwise un-enroll the destination); `identity` and trust data are outside it.

**Scenario K (fresh pools, client snapshot frequency 1d, a second client identity "B" with its own DB on
the client VM):**

| Case | Checks |
|---|---|
| K1 — not enrolled | no client id: one ERROR "remote backup skipped: no client id"; an id but no pinned server: one ERROR "destination server1 is not enrolled"; in both: no `zfs send`, no HTTP request in the server log, nothing under the target dataset, trust list empty |
| K2 — token consumed | enroll with the running daemon (it sends the baseline, full, byte-identical); the same, now spent, token for B: exit 1 "refused the token", still one trusted client |
| K3 — unscoped token, known id | B claiming A's id: exit 1, "already knows a client with id", the `--client-id` hint, "token was not used and is still valid"; token state stays `active`; the SAME token then enrolls B under its own id |
| K4 — certificate isolation | B's cert on A's URL: 403 `client_id mismatch` naming B's id; A's cert on B's URL: 403; no cert: 401; `/backup/register`: 404; A's server data (names and guids) unchanged; no dataset for B |
| K5 — revoke mid-flight | `trust revoke <A's fingerprint>` while a throttled 400 MiB stream runs: the stream still completes and is byte-identical; the next cycle logs one ERROR "does not trust this client", sends nothing, the anchor stays |
| K6 — re-enroll | `trust token create --client-id A --replace` + a new certificate: same client id, one pin (the new one), the pending snapshot goes out as `incremental from <anchor>`, same server dataset guid, same old-snapshot guid |
| K7 — rotate | `identity rotate`: two pins, the successor naming its predecessor; the next cycle succeeds incrementally and retires the old pin (one pin, no predecessor) |
| K8 — import | `import` of a YAML leaves `identity show --json` byte-identical; the destination keeps working |
| K9 — set-client-id | refused while a destination is enrolled; the id is unchanged |
| K10 — http:// | import of a config with an `http://` destination: exit 1 "plaintext http is not supported"; the stored config (`export`) is unchanged |

`test_config_permissions.sh` gained group (i): `<db>.tls` is `0750` in the DB file's group, keys `0600`,
certificates `0640`, `identity.lock` `0640`; a group operator can read `server.crt` (and issue a token) but
not `server.key`; root-created client identity files keep the DB's group.

### Results of the TLS run

_Pending: filled in after the first real run of the milestone-3 harness (`./scenarios/test_two_vm_backup.sh`
and `./scenarios/test_config_permissions.sh`) on the committed tree. Record here: date, commit, pass/fail
counts per scenario (A-K), anything that was flaky, and any code defect the run found._

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
  (`zfsbackup/api.py`). Not tested by the harness for that reason. *(Superseded 2026-10-19: resume was
  finished on branch `libzfseasy-fixes` and is covered by scenario J — interrupt/resume, stale token,
  in-flight pin, dataset collision.)*
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

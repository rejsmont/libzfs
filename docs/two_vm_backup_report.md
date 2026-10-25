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
| K5 — revoke mid-stream | `trust revoke <A's fingerprint>` while a throttled 400 MiB stream runs: the server cuts the stream within ~4 s (WARNING "cutting the running stream of client ...", INFO "was cut; zfs receive reaped, the partial receive state is kept"), no `zfs receive` is left, the server holds a `receive_resume_token`, the client's `zfs send` ends; the client logs one ERROR "does not trust this client", sends nothing more, the anchor and the in-flight pin stay |
| K6 — re-enroll | `trust token create --client-id A --replace` + `enroll --new-key` (a new certificate): same client id, one pin (the new one); the cut stream is RESUMED (`Resumed <ds>@<snap> (from <anchor>)`, server `mode=resume`, token cleared, matching guids), then the pending snapshot goes out as `incremental from <resumed snapshot>`; same server dataset guid |
| K7 — rotate | `identity rotate`: two pins, the successor naming its predecessor; the next cycle succeeds incrementally and retires the old pin (one pin, no predecessor) |
| K8 — import | `import` of a YAML leaves `identity show --json` byte-identical; the destination keeps working |
| K9 — set-client-id | refused while a destination is enrolled; the id is unchanged |
| K10 — http:// | import of a config with an `http://` destination: exit 1 "plaintext http is not supported"; the stored config (`export`) is unchanged |
| K11 — rotation does not cut | `identity rotate` during a throttled stream and the old pin's retirement (the successor's first request) leave the stream running (no "cutting" line); revoking the SUCCESSOR key then cuts it within ~4 s ("retired by a rotation and its successor ... was revoked"), the token is kept |

`test_config_permissions.sh` gained group (i): `<db>.tls` is `0750` in the DB file's group, keys `0600`,
certificates `0640`, `identity.lock` `0640`; a group operator can read `server.crt` (and issue a token) but
not `server.key`; root-created client identity files keep the DB's group.

### Results of the TLS run

**Run:** 2026-10-21, `915642a` plus the final drain fix (`zfsbackup/workers.py` suppresses werkzeug's post-websocket
200; `zfsbackup/api.py` `backup_stream` waits for the websocket reader thread before returning), shipped as the
uncommitted tree `SRC_REF=d32ab94e3bc6a304a36267880ffa89fd1ed58769` (`git stash create`); `zfsb-server` +
`zfsb-client` (and `zfsb-perm` for the permissions script). Harness: **231 passed, 0 failed**;
`test_config_permissions.sh`: **321 passed, 0 failed** (run on `915642a`, before the fix; it does not touch the
changed code path).

Server log, counted from this run's start only (27 `stream?mode=` requests in that window): **0**
ERROR/CRITICAL/Traceback lines. The client log has 11, all caused by scenarios: 4 "supervisor ... is gone"
(scenario G's `kill -9`), 3 "cannot reach" (scenario C's server outage), and 1 each of J4's 409 "refusing to
mix in", K5's "does not trust this client", K1a's "no client id", K1b's "not enrolled".

History: an earlier run on plain `915642a` was 230 passed, 1 failed: scenario I's "no Failed/traceback in the
server log" check caught two `werkzeug - ERROR - Error on request` tracebacks (`ssl.SSLError:
RECORD_LAYER_FAILURE` from werkzeug's post-handler `rfile.read(10_000_000)` through `workers.py
_DeadlineReader.readinto`) right after successful streams. A first fix gave 231/0; the final fix above is 231/0
as well.

| Scenario | Pass / fail |
|---|---|
| provisioning + enrollment (TLS identity created, one pin) | 2 / 0 |
| A full, B incremental | 3 / 0, 3 / 0 |
| C server-down + catch-up (incl. server certificate/pin survives the restart) | 8 / 0 |
| D client restart (client id and certificate stable via `identity show`) | 4 / 0 |
| E reload client during a 2 GiB send, F reload server during a receive, G `kill -9` supervisor | 13 / 0, 11 / 0, 11 / 0 |
| H `rename --clear-stale-props` | 10 / 0 |
| I non-recursive pruning of received datasets (incl. the server-log traceback check, which failed before the fix) | 7 / 0 |
| J resume (J0 3, J1 15, J2 12, J3 14, J4 9; J4's 409 probe is an authenticated curl) | 53 / 0 |
| K TLS enrollment and trust (K1-K10) | 106 / 0 |
| `test_config_permissions.sh`, groups (a)-(h) unchanged, (g) over HTTPS, new group (i) | 321 / 0 |

Notes:
- Every K case passed first time on the real VMs, including the ones that depend on server behaviour
  (revoke while a throttled 400 MiB stream is in flight: the stream completes and is byte-identical, the next
  cycle logs one ERROR "does not trust this client ... (TLS alert ...)" and sends nothing; `enroll --new-key`
  with a `--client-id A --replace` token: same client id, same server dataset guid, the pending snapshot goes
  out incrementally; `identity rotate`: two pins, then one after the first authenticated request).
- Script fixes made while running (harness only): `cryptography` is now `pip install`ed in the guests (the
  source tree runs from `/opt/zfsbackup-src`, not a pip install), `tar -m` on extraction (the VMs' clocks lag the
  host), K6 uses `enroll --new-key`.

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

## Deployment readiness

A green harness run confirms the **happy path** and the trust model, not production-readiness. Status of the
original 2026-07 findings:

- ~~**Blocker — no auth/authz/TLS on the server API**~~ **Resolved by milestone 3** (commit `915642a`): the API
  is HTTPS only (TLS 1.3), every endpoint except `/health` and `/enroll` needs a pinned client certificate, the
  client id is bound to the verified fingerprint (a mismatch is 403), `/backup/register` is gone, admission is by
  one-time token, and revocation and rotation are covered by scenario K. See
  [`production_readiness_report.md`](production_readiness_report.md) for the residuals (werkzeug's development
  server, no rate limiting on `/enroll`, no revocation of a stream already running).
- ~~**Blocker — `requests` imported but undeclared**~~ **Resolved:** `requests` (and `cryptography`) are declared
  in `pyproject.toml`; the harness still installs them explicitly because the guests run from a source tree.
- **High — resume is dead code**: client sends `?from=<snap>`; server never reads it
  (`zfsbackup/api.py`). Not tested by the harness for that reason. *(Superseded 2026-10-19: resume was
  finished on branch `libzfseasy-fixes` and is covered by scenario J — interrupt/resume, stale token,
  in-flight pin, dataset collision.)*
- **High — incrementals use `-i`, not `-I`** (`zfsbackup/remote.py:198`): snapshots created
  *between* two backups are never replicated. **Live-confirmed** in this run — the server
  was missing intermediate snapshots (e.g. `…114800`, `…114900`) that the client had. *(Resolved in production-readiness
  milestone B, 2026-10-22: every managed snapshot is sent as a chain of `-i` steps, oldest first by `createtxg`;
  scenario B2 asserts a six-snapshot backlog arrives with matching GUIDs and no unmanaged snapshot is sent.)*

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

# Milestone 3 plan (final): TLS and mutual-TLS client authentication for the remote-backup API (closes B1)

## Status / implementation notes (2026-10-20)

**Status 2026-10-22:** merged to master as PR #4 (`609df66`); scenario K passed in the two-VM harness. Later
changes to the wire contract (the `X-Zfsbackup-Protocol` header and 426, `protocol` in `/whoami`/`/enroll`/`/rotate`,
the removed info endpoints) are in [production_readiness_plan.md](production_readiness_plan.md) milestone A.
Milestone C (B-2b, additive, protocol still 1): negotiate answers `server_newer` and `server_newer_count` (or `server_newer_unknown: true`); `/stream`
takes `from_guid` and `discard`, and refuses with `reason` `server_newer`, `base_missing`, `no_common` or `listing` (see CLAUDE.md).

**Implemented**: M1–M16 and M18 on branch `tls-mtls`, uncommitted at the time of writing. M17 (the two-VM
harness over TLS, scenario K) is written; its first real run is still to be recorded in
[two_vm_backup_report.md](two_vm_backup_report.md). B1 is marked resolved in
[production_readiness_report.md](production_readiness_report.md). The body below is the plan as approved;
where it differs from the code, **the code and this section win**. The wire contract is at the end of the
file, and the user-facing procedures are in `zfsbackup/README.md` ("TLS and enrollment").

### Decisions taken during implementation (final)

- **Revocation is permanent.**
  - A new table, `revoked_fingerprints`, records every fingerprint that was unpinned, with a reason:
    `revoked`, `successor_revoked`, `client_revoked`, `replaced` or `retired`. A `retired` row also records
    the successor that retired it (`superseded_by`).
  - Enroll and rotate refuse a recorded fingerprint for ever, before the token is looked at, so a stolen
    key cannot be re-enrolled with a fresh token.
  - `trust revoke FP` revokes every live descendant of FP, transitively, reaching it through pending
    rotation links and through the `superseded_by` of retired records (reason `successor_revoked`). If FP
    was already retired by rotation, the live successor chain is still revoked ("was already retired by its
    successor C; revoked C ..."). Revoking a stolen key A therefore also removes a thief's C, even after C
    has been used. A retired rotation predecessor is refused on enroll and rotate too.
  - A `/rotate` authenticated with C retires C's still-pinned predecessor in the same transaction, so
    rotating twice (A to C to D) always leaves a recorded chain.
  - `import` does not clear the table (it is one of the preserved tables).
- **`enroll --new-key [--cert --key]`** is the recovery path for a lost, stolen or revoked key.
  - It generates a fresh key as `client.next`, enrolls it, and promotes it once the server accepted it.
  - A pending `client.next` is reused only if the server at this destination already trusts it; otherwise a
    new one is made. A pin mismatch during that probe aborts without discarding anything.
  - With several destinations, run `--new-key` once, then plain `enroll` for each other server, each with
    a `trust token create --client-id ID --replace` token.
- **A `--replace` token** presented with an already-pinned certificate performs the replace (the id's
  other pins are dropped and recorded as `replaced`). The plan answered `already_trusted` here.
- **Client-id grammar** is `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` (A7 was tightened; a leading `.` or `-` is out).
- **Destination URLs** must be `https://`; port 0 or an empty port is refused (no port means 443).
- **`client_id` lives in the `host_identity` table**, not in a file. `client_id_file` and
  `effective_client_id_file` are gone, and `from_file` refuses the key.
- **API server:**
  - TLS 1.3 only, with a per-connection handshake (30 s);
  - at most 64 connections in all and 16 anonymous ones (no pinned client certificate);
  - an absolute 30 s deadline for the request line and headers;
  - a trust poll every 2 s in a dedicated thread;
  - every `store.trust` call in the API worker serialised in-process;
  - the API worker restarts (drain, exit 1) only for a confirmed on-disk server-identity change
    (`identity.IdentityChanged`).
- **`/enroll`** runs a read-only token pre-check (`token_precheck`) before any `zfs` call.
- **Dry-run** creates no TLS files and serves with a throwaway identity.

### Deviations from the plan

- **Revoked-fingerprint table** (above); the plan only had a `clients.revoked_at` tombstone and deleted pins.
  `store.trust` also exports `list_revoked`, and the tables outside the config set are named
  `mapper.PRESERVED_TABLES` (a module-level assertion classifies every table).
- **Cascade on revoke** (every live descendant, through pending links and `superseded_by`), retirement of a
  successor's predecessor at `/rotate`, and permanent refusal of retired predecessors (M7 said only "delete pins").
- **`enroll --new-key`** and the `--replace`-with-a-pinned-certificate rule (not in M7, M13).
- **Refresh after a trust write:** the plan said `refresh()` then respond. The code first tries a bounded
  refresh (up to 2 s) before the response; if that cannot run in time, it is retried right after the
  response is sent (`call_on_close`). The trust poll is the backstop, and a client connecting in that
  window may see one refusal and should retry.
- **Anonymous connection cap (16)** and the **absolute header deadline** (the plan had only the total cap of
  64 and the handshake timeout).
- **`IdentityChanged`:** a failed gateway rebuild fails closed for removals; only a confirmed change of the
  server certificate on disk restarts the API worker (the plan had no restart path).
- **`identity.py` additions:** `peek_identity`, `promote_identity`/`complete_promotion` (a crash between
  the two renames is rolled forward, never reported as damage), redacting `SecretStr`/`SecretBytes`,
  `is_pin_mismatch`/`is_tls_alert_rejection`.
- **`token_precheck`, `list_revoked`** in the store API; the store's API writers take no flock and queue on
  an in-process lock, and a locked database becomes `ConfigBusy`.
- **Client rejection detection:** `/whoami` is retried once after a reset straight after the handshake
  (see "M11 rejection detection" below).
- **`PROP_CLIENT_ID` and `_server_dataset`:** a validation failure answers 400 as planned; a dataset whose
  recorded id differs from the binding answers 409.

### Deferred (beyond the plan's own list)

- The plan's deferred items stand (`require_ca`, `zfs receive -x`, `trust add`/`trust forget`, re-keying
  the server certificate, a production WSGI server, raw sends, reporting the applied trust generation).
- No mid-stream revocation, and no rate limiting on `/enroll`.
- `TestRealSendReceive` (`tests/test_zfsbackup_real.py`, `real_zfs` tier) is skipped pending a rewrite around
  an enrolled client.
- The M17 run record (see above).

---

This is the final proposal. It includes your answers on the revised plan and replaces `/Users/rejsmont/src/libzfs/docs/tls_mtls_plan.md` in full. Implementation starts only after you sign off.

## Changes since the previous revision

- **D2 changed: `client_id` now lives in the config database.**
  - It is stored in a new one-row table, `host_identity`, outside the six config tables. So `import` and `save_config` never touch it and `export` never emits it.
  - The CLI generates it the first time `enroll` needs it. The daemon only reads it.
  - The supervisor reads it in the **same read snapshot** as the config (`ConfigSnapshot.client_id`) and hands it to the workers through the `Handoff`.
  - Changing it bumps the config generation, so a running daemon reloads (see M6, M7 and M9).
  - Removed: `client_id_file`, `effective_client_id_file`, both copies of `_read_id_file`, the file logic in `ClientIdentity`, and `_client_id_problem`. A8 is obsolete.
- **The staged removal of `client_id_file` is back**, spread across the model, store, CLI and daemon owners: M4 → M6 → M8 → M9/M11 → M15.
  - Test churn is about 370 references in roughly 25 files.
- **Decisions recorded:** D1-A1, D3 as recommended, D4 with `/rotate` included, D5 (keep the URL id; 403 on mismatch), D6, D7 and A1–A7.
  - M14 is now in scope. Its store half moves into M7.
- **New CLI commands:**
  - `identity show` displays the client id.
  - `identity set-client-id` changes it. It checks the A7 grammar, warns, and refuses while destinations are enrolled unless `--force`.
- **`rename.py` server-path notes** now use the id from the DB.

## Open decisions

None. For reference, these are the recorded decisions the plan is built on:
- **D1-A1:** trust tables live in the config DB. Only `store/trust.py` touches them; the API worker writes enroll, rotate and retire; the API worker polls a trust generation; trust changes cause no reload or SIGHUP.
- **D2 (revised):** `client_id` lives in the DB in `host_identity`, generated by the CLI.
- **D3:** one client id may have several fingerprints, and a fingerprint belongs to exactly one id. A known id gets 409 unless the operator issues a `--client-id X [--replace]` token.
- **D4:** token re-enrollment, plus `/rotate`. The old pin is retired on the successor's first authenticated request.
- **D5:** keep `/backup/<client_id>/…`; the server returns 403 on a mismatch.
- **D6:** the TLS handshake runs in each connection's own thread, with the current context.
- **D7:** the defaults:
  - token lifetime 1 h by default, 7 d maximum; 256-bit secret;
  - TLS 1.3 only; HTTPS always;
  - every endpoint except `/health` and `/enroll` needs a pinned certificate;
  - expired client certificates are refused;
  - the client ignores the server certificate's expiry;
  - a dry-run server refuses `/enroll` and `/rotate`;
  - revoke takes a fingerprint, or `--client ID`.
- **A1–A7:** accepted.

Two design choices follow from D2. Both are spelled out in M6, M7 and M9, and you can veto either:

- **The client id is in the config read snapshot and bumps the generation when it changes.**
  - The alternative is a separate table read alongside the snapshot. With that, a running daemon would only see a changed id after a restart. It would also need a second reload key.
  - Bumping the shared generation reuses the existing poll, SIGHUP and compare-and-swap machinery. The trade-off is that a concurrent CLI edit gets a retryable conflict.
- **`enroll` does two committed writes** (the id if absent, then the destination pin) **and sends exactly one SIGHUP**, after its last committed write. It does this on the failure path too, if anything was committed.

## Facts from the source that drive the plan

- **werkzeug does the TLS handshake on its accept thread.** It wraps the *listening* socket (`werkzeug/serving.py:806`), so every handshake runs on the serving thread with no timeout.
  - One silent peer stalls all new connections and hangs `server.shutdown()`. That breaks the reload drain.
  - D6 fixes this.
- **`save_config` wipes and reinserts only the six config tables** (`zfsbackup/config/store/mapper.py:1282-1292`).
  - Tables outside that set (trust and `host_identity`) survive `import`, as long as the mapper never names them.
- **The supervisor reads the config in one snapshot**, and its reload logic adopts an identical candidate with no epoch bump (`same_as`).
  - Any non-config value the workers need (the client id) has to be part of both the snapshot and that identity check. Otherwise a change to it would be read and then silently ignored.

## Stated assumptions (accepted)

- **A1. The client pins only the server fingerprint.** It connects with `CERT_NONE` and checks the fingerprint before sending any HTTP byte:
  - for HTTP, through a `requests` adapter using `assert_fingerprint`;
  - for the websocket, by wrapping the socket first and passing it as `websocket.connect(..., socket=)`.
  - If spike S7 or S8 fails, the fallback is to store the server certificate PEM instead.
- **A2. TLS files live in `<db>.tls/`.**
  - The directory is mode 0750, with the DB file's group.
  - Keys are 0600; certificates are 0640.
  - Keys are not stored in the DB.
  - `client_cert_file` and `client_key_file` are optional overrides: absolute paths, both or neither.
  - Replacing the DB file keeps the TLS identity but loses the pins **and the client id**.
- **A3. Generated certificates:**
  - ECDSA P-256 with SHA-256, valid 10 years, `notBefore` set 5 minutes early;
  - `CA:FALSE`, `digitalSignature`, and an EKU for the role (`serverAuth` or `clientAuth`).
- **A4. Who generates what:**
  - The server TLS identity: the supervisor at startup, or `trust token create`.
  - The client TLS identity and the client id: `enroll` or `identity rotate`, both CLI.
  - Creating files in the TLS directory is guarded by an flock on `<db>.tls/identity.lock`.
- **A5. Tokens are entered with `--token-file`, `--token -` (stdin) or a hidden prompt.** They travel only in the request body and are never logged.
- **A6. `/whoami` returns `{client_id, fingerprint}`.** The remote worker calls it once per cycle per destination, in place of `/register`. `enroll` uses it to be idempotent.
- **A7. Client id grammar: `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`** (first character a letter or digit; tightened after review because `libzfseasy` rejects a leading `-`).
  - Generated ids are `<sanitised hostname, truncated to 55>-<uuid8>`.
  - The grammar is enforced by the model helper, store CHECK constraints, the server and the CLI.

---

## Ordered work items

Legend:
- **Owner** is the one agent that edits the files.
- **Tier** is `low-risk` or `needs-approval`.
- **CR** means concurrency-reviewer also reviews it, alongside zfs-code-reviewer.
- **opus** means pass `model: opus`.

### Wave 0: prerequisites

**M1. Add the `cryptography` dependency.**
- Owner: zfsbackup-developer. Tier: low-risk.
- Basis: the brief.
- Files: `/Users/rejsmont/src/libzfs/pyproject.toml` and `poetry.lock`.
  - Add `cryptography = ">=43"` as a runtime dependency.
  - Run `poetry lock` and `poetry install --with dev`.
- Confirm the CI workflow, including the SQLAlchemy 2.1 job, needs no change.
- Verify: `import cryptography` works, and a bare `pytest` stays green.

**M2. Spike tests that gate M3, M9, M10 and M11.**
- Owner: pytest-test-author. Tier: low-risk.
- Basis: the NOT-verified list in decision 2, plus D6 and A1.
- New file: `tests/test_zfsbackup_tls_platform.py`, marked `integration`.
  - Certificates are generated in `tmp_path`, servers bind `127.0.0.1:0`, no zfs, hermetic.
  - These tests stay in the suite as regression guards.
- **S1.** A pinned but expired certificate is rejected at the handshake under `CERT_OPTIONAL` + `VERIFY_X509_PARTIAL_CHAIN`. So is a not-yet-valid one.
- **S2.** With a context swapped between connections in a `ThreadedWSGIServer` subclass that wraps each connection itself:
  - a revoked certificate is rejected and a newly pinned one is accepted, with no restart;
  - record the `sni_callback` variant as the fallback.
- **S3.** A silent peer does not block other clients, and `server.shutdown()` returns in under 2 s.
- **S4.** A client limited to TLS 1.2 is refused when the server's minimum is 1.3.
- **S5.** `SSL_CLIENT_CERT` is present on a plain route and on a `flask_sock` websocket route. It is absent for a client with no certificate, and that client still completes the handshake.
- **S6.** A wss connection in both directions on one `SSLSocket`: `simple_websocket`'s background `recv` thread runs while the handler does its final `send`.
  - 64 MiB × 20 runs with no deadlock and no corruption.
- **S7.** The `requests` pinned adapter with a client certificate and `assert_fingerprint` (requests 2.32 / urllib3 2.6):
  - a wrong fingerprint fails before the server sees any request;
  - neither `verify=False` nor certifi overrides it.
- **S8.** `websocket.connect("wss://…", socket=<already-wrapped SSLSocket>)` uses that socket as it is.
- **S9.** When `before_request` returns 401 or 403 on a `flask_sock` route, websocket-client raises `WebSocketBadStatusException` with that status.
- **S10.** Record the exception a TLS 1.3 client sees when the server rejects its certificate, for M11's error mapping.
- If S1, S2, S6, S7 or S8 fails, work goes back to me before M3 starts.
- Verify: `pytest tests/test_zfsbackup_tls_platform.py`.

### Wave 1 (parallel once M2 is green)

**M3. New `zfsbackup/identity.py`: TLS identity, fingerprints, token, and proof that the client holds its key.**
- Owner: zfsbackup-developer. Tier: needs-approval. CR (race when creating identity files).
- Basis: decisions 1, 2 and 4; D3, D4; A1–A4.
- It may import only the stdlib, `cryptography` and `zfsbackup.config`. It must not import the daemon, because the CLI uses it.
- With the D2 revision it holds no client-id-file logic: the id grammar and the generator live in `config/model.py` (M4).
- **TLS identity and contexts:**
  - `tls_dir(db_path)`.
  - Fingerprints: `fingerprint_der`, `fingerprint_pem`, `normalize_fingerprint`.
  - `ensure_identity(dir, role)`: takes the flock, writes the key and certificate as `O_EXCL` 0600 temp files, fsyncs, renames key then certificate, and sets modes and group.
  - `load_identity(cert, key)` refuses a key that others can write or read and warns if its group can read it. It then checks:
    - the key and certificate match;
    - the certificate has not expired;
    - its EKU fits the role.
  - `server_context(identity, pinned_pems)`:
    - `CERT_OPTIONAL`, `VERIFY_X509_PARTIAL_CHAIN`, minimum TLS 1.3;
    - calls `load_verify_locations(cadata=…)` only when there is at least one pin.
  - `client_context(identity|None)`: `check_hostname=False`, `CERT_NONE`, minimum TLS 1.3.
  - `connect_pinned(host, port, ctx, expected_fp, timeout)`: raises `PinMismatch` before any byte is sent.
- **Token and proof of possession:**
  - Tokens: `make_token`, giving `zfsb1.<64-hex server fingerprint>.<base64url of 32 bytes>`. `parse_token` is strict. `secret_hash` is SHA-256.
  - `enrollment_payload(server_fp, client_id, client_fp, secret_sha256)` builds
    `b"zfsbackup-enroll-v1\nserver=<fp>\nclient_id=<id>\nclient=<fp>\ntoken=<sha256>\n"`.
  - `rotation_payload(server_fp, client_id, old_fp, new_fp)` builds the equivalent `zfsbackup-rotate-v1` payload.
  - Signing and verification: ECDSA-SHA256 for EC keys, PKCS#1 v1.5-SHA256 for RSA, raw signing for Ed25519.
- **`check_client_cert(pem, now)`** accepts a certificate only if:
  - it is a single PEM of at most 16 KiB, X.509 v3;
  - it is within its validity window (5 minutes of slack);
  - its EKU is absent or includes `clientAuth`;
  - it is not a CA;
  - its key is RSA ≥ 2048, EC P-256/384/521, or Ed25519.
- Verify: `tests/test_zfsbackup_identity.py` (M16).

**M4. `config/model.py`: new fields, https only, and the client id grammar (first step of the staged removal).**
- Owner: zfsbackup-developer. Tier: needs-approval (config round-trip format).
- Basis: decision 4; the brief's "operator-supplied paths"; D2; A7.
- **New fields and URL rule:**
  - `Destination(url, server_fingerprint: Optional[str] = None)`. The fingerprint is parsed, normalised and validated.
  - URLs must be `https://` with a host. `http://` raises `ValueError`.
  - `BackupConfig.client_cert_file` and `client_key_file`: `Optional[Path]`, absolute, both or neither.
- **Shared client id helpers**, used by the store, the CLI and the server:
  - `client_id_problem(text) -> Optional[str]` checks the A7 grammar;
  - `generate_client_id()` returns `<sanitised hostname>-<uuid8>`, always valid under A7.
- **`client_id_file` stays in this step** (parsed and kept), so the daemon and remote code keep working until M9/M11.
- **Update `/Users/rejsmont/src/libzfs/zfsbackup/config.example.yaml`:**
  - https URLs and a `server_fingerprint` comment;
  - remove the `client_id_file` block and replace it with a note that the client id lives in the DB (`zfsbackup-config identity show`).
- Verify: `pytest tests/test_zfsbackup_config.py tests/test_zfsbackup_config_audit.py tests/test_zfsbackup_duration.py`, after M16a.

**M5. Store schema and one new Alembic revision.**
- Owner: zfsbackup-store-developer. Tier: needs-approval. Runs in parallel with M4.
- Basis: decision 5, D1–D4, and your requirement for a proper new revision.
- Files:
  - `store/models.py`;
  - `store/migrations/versions/2026xxxx_<id>_tls_trust_and_host_identity.py`;
  - `EXPECTED_HEAD` and `KNOWN_REVISIONS` in `migrate.py`.
- **`host_identity`** (new, one row, `CHECK id=1`):
  - `client_id` TEXT NOT NULL;
  - CHECK `length BETWEEN 1 AND 64 AND client_id NOT GLOB '*[^A-Za-z0-9._-]*' AND substr(client_id,1,1) <> '.'`;
  - `created_at` REAL NOT NULL;
  - `updated_at` REAL NULL.
  - No row means the id has not been generated yet.
- **`clients`:**
  - `client_id` TEXT primary key, with the same CHECK;
  - `label` NULL; `created_at`;
  - `revoked_at` NULL (a tombstone).
- **`trusted_clients`:**
  - `fingerprint` TEXT primary key (64 hex);
  - `client_id` NOT NULL, foreign key to `clients`, indexed but not unique;
  - `cert_pem`, `not_after`, `enrolled_at`;
  - `enrolled_via` in (`token`, `rotate`);
  - `predecessor_fingerprint` NULL.
- **`enrollment_token`** (one row):
  - `secret_sha256`, `server_fingerprint`, `created_at`, `expires_at`;
  - `client_id` NULL and `replace` BOOL, with CHECK `replace → client_id IS NOT NULL`;
  - `consumed_at`, `consumed_fingerprint`, `consumed_client_id`.
- **`trust_state`** (one row): `generation` INTEGER NOT NULL.
- **`destinations`:** add `server_fingerprint` TEXT NULL, CHECK 64 hex.
- **`global_settings`:**
  - add `client_cert_file` and `client_key_file` NULL, with a both-or-neither CHECK;
  - **drop `client_id_file`**.
- Build and checks:
  - Generate with autogenerate, using `batch_alter_table` for the two altered tables.
  - No rows are seeded.
  - The naming convention names every constraint, and the `Float()` pin is kept.
  - The drift check passes on SQLAlchemy 2.0 and 2.1.
  - Downgrade restores the column. The old values are lost, which is acceptable because nothing is deployed.
- Verify:
  - `pytest tests/test_zfsbackup_store.py tests/test_zfsbackup_store_upgrade.py tests/test_zfsbackup_store_sqla_compat.py`;
  - `zfsbackup-config migrate` from `a1c28e1b3f34` reaches the new head and passes the FK check.

### Wave 2

**M6. Mapper, `validate_config` and the snapshot (second step of the staged removal).**
- Owner: zfsbackup-store-developer. Tier: needs-approval (config load/write contract). Depends on M4 and M5.
- Basis:
  - M4 and M5;
  - D2: the client id is read in the same snapshot as the config;
  - the wipe-and-reinsert in `save_config` must spare trust and identity tables.
- **Load, save and validate:**
  - Load and save `server_fingerprint`, `client_cert_file` and `client_key_file`.
  - `client_id_file` is no longer persisted. `validate_config` rejects a non-`None` value: "client_id_file was removed; the client id is stored in the database (`zfsbackup-config identity show` / `identity set-client-id`)".
  - `validate_config` also enforces https URLs, canonical fingerprints, and absolute certificate and key paths.
- **Snapshot:**
  - `load_config_snapshot` reads `host_identity` in the same `BEGIN` snapshot.
  - It returns `ConfigSnapshot.client_id: Optional[str]`.
  - `BackupConfig` does not carry the id, so YAML, `render` and `same_as` stay config-only.
- **What import and export do:**
  - `save_config` deletes only the six config tables; a guard and a comment enforce this.
  - `import` therefore keeps the client id and all trust tables, and `export` emits neither.
- Verify: `pytest tests/test_zfsbackup_store*.py tests/test_zfsbackup_importer_cleanup.py tests/test_zfsbackup_runtime_config.py`, plus M16.

**M7. `store/trust.py`: trust and host-identity API, plus a per-call engine helper in `db.py`. Includes the store half of `/rotate`.**
- Owner: zfsbackup-store-developer. Tier: needs-approval. CR. **opus.** Depends on M5 and M6.
- Basis: D1-A1, D2–D4.
- **`db.py` / `paths.py`:** an `ephemeral=True` session variant.
  - Same pragmas and the same `BEGIN IMMEDIATE` behaviour for writers.
  - It builds a new engine on each call: no connection pool, never cached, disposed on exit.
  - `db.py` stays the only module that creates engines.
- **Return types:** frozen dataclasses only (`TrustedPin`, `ClientRecord`, `TrustSnapshot(store_id, generation, pins)`, `TokenStatus`). No ORM objects leave the module.
- **Host identity (CLI writers only):**
  - `read_client_id(resolved) -> Optional[str]`.
  - `ensure_client_id(resolved, *, lock) -> (client_id, created: bool)`. This runs under the writer lock and `BEGIN IMMEDIATE`:
    - if there is no row, it inserts one with `generate_client_id()` and **bumps `GlobalSettings.generation`** in the same transaction;
    - otherwise it returns the stored id.
  - `set_client_id(resolved, new_id, *, expected_generation, lock)`:
    - checks the grammar, compares the generation (compare-and-swap), then upserts and bumps the generation;
    - a stale generation raises `ConfigConflict`.
  - Both raise `ValueError` for an uninitialised store (no `GlobalSettings` row).
  - **Rule: any change to the stored id bumps the config generation.** The daemon's existing poll and SIGHUP path then picks it up (M9).
- **Trust reads** (read-only; `BEGIN`, `check_schema`, `SELECT`, `COMMIT`):
  - `read_trust_key`, `read_trust` (never returns pins of revoked clients), `list_clients`, `token_status`.
- **Trust writes** (`BEGIN IMMEDIATE`, schema check inside, bump `trust_state.generation` only when pins change):
  - `create_token(…, client_id=None, replace=False, lock)`: a scoped token requires the client to exist.
  - `revoke_token`, `revoke_pin(fp)`, `revoke_client(id)` (deletes all its pins and sets `revoked_at`).
- **`enroll_client(…, client_id, pin, label, now, dataset_exists)`** checks, in this order:
  1. **The token itself.** If there is none, or it is expired or consumed, or the server fingerprint differs, or the hash differs (`hmac.compare_digest`), the result is `refused`. Every one of these failures gives the same answer.
  2. **The certificate's fingerprint is already pinned:**
     - to the same id: `already_trusted`, and the token is not consumed;
     - to a different id: `cert_bound_elsewhere`.
  3. **The token is scoped to `X`:**
     - a proposed id other than `X` is `refused`;
     - otherwise the pin is added to `X`, the tombstone is cleared, and with `replace` the other pins of `X` are deleted.
  4. **The token is unscoped:**
     - a known id (a `clients` row exists, or `dataset_exists`) gives `client_id_taken`, and the token is **not** consumed;
     - otherwise the new client is inserted with its pin.
  5. On success the token is consumed and the trust generation is bumped.
- **Rotation (store half of D4):**
  - `rotate_client(old_fp, new_pin, now)` returns `rotated`, `already` or `refused`.
  - `retire_predecessor(fp)` is idempotent and same-client only.
- **Errors:** "database is locked" becomes `ConfigBusy`. Export the new names in `store/__init__.py` and update the docstring invariants.
- **Edge cases:**
  - The trust key includes `store_id`, so a replaced DB file is detected.
  - Old schema: callers refuse.
  - Concurrent enrollers serialise; exactly one wins.
  - Two `ensure_client_id` calls converge on one id.
  - A retire racing another retire is safe.
- Verify: `tests/test_zfsbackup_store_trust.py`.

**M8. CLI config surface (third step of the staged removal).**
- Owner: zfsbackup-cli-developer. Tier: needs-approval. Depends on M4 and M6.
- Basis: the render round-trip invariant; D2; the coordinator's `rename.py` note.
- **Render, edit and list:**
  - `render.py` emits `server_fingerprint`, `client_cert_file` and `client_key_file`. It no longer emits `client_id_file`.
  - `dotpath.py` supports get/set/unset for the three new fields and drops `client_id_file` (including its `_TRI_STATE`/`_TEXT_SETTINGS` entries).
  - `editor.py`: add `server_fingerprint` to `_DESTINATION_KEYS` and drop `client_id_file`.
  - `listing.py`: `list destinations` shows each destination's enrollment.
- **`rename.py`:**
  - `_client_id` now returns `ConfigSnapshot.client_id` (rename already reads a snapshot), or the placeholder `<client-id>` when it is unset.
  - It no longer reads files.
- Verify: `pytest tests/test_zfsbackup_cli*.py`.

### Wave 3 (daemon core runs serially M9 → M10 → M11 → M14b; the CLI items M12, M13, M14c run in parallel lanes)

**M9. Daemon and workers: HTTPS-only API server, per-connection handshake, trust gateway, server identity, client id in the `Handoff`. Fourth step of the staged removal (daemon side).**
- Owner: zfsbackup-developer. Tier: needs-approval (multiprocessing, reload state machine, security). CR. **opus.** Depends on M2, M3, M6 and M7.
- Basis: decision 2; D1, D2, D6; the accept-thread DoS.
- **`workers.py` (API server):**
  - Werkzeug never wraps the listening socket, and `self.ssl_context` is set after construction.
  - `process_request` takes a non-blocking semaphore (cap 64).
  - `process_request_thread` wraps the socket with `gateway.current_context()` and runs the handshake with a 30 s timeout.
  - There is no plaintext mode.
  - `ApiWorker` gains a keyword-only `config_db`, used only for trust. It loads the server identity (exits 1 on failure), builds a `TrustGateway`, and polls trust every 2 s.
  - `RemoteBackupWorker` gains keyword-only `tls_dir` and `client_id`.
- **`api.py` `TrustGateway`:**
  - It holds an immutable `TrustView(key, pins: {fp: (client_id, not_after, predecessor)}, context)`, swapped atomically.
  - It has `poll()` and `refresh()`, and fails closed.
- **`daemon.py`:**
  - `Handoff` gains `client_id` (from `ConfigSnapshot.client_id`). `_new_worker` reads it from the same single `Handoff` read.
  - **Reload:** the identical-candidate test becomes `config.same_as(current) and snapshot.client_id == current.client_id`. A changed id is a real switch (epoch bump; the remote worker turns over), and `_changed_settings` reports `client_id`.
  - `_run_startup` ensures the server identity after the presence lock and before the first fork. On failure it returns 1 with a reason.
  - `--test-config` reports the identity state and the client id, and creates nothing.
  - **Removals:** `_read_id_file`, `_ID_FILE_MAX`, `_client_id_problem` and their `_candidate_warnings` branch. `_SETTINGS` gains `client_cert_file` and `client_key_file` and loses `client_id_file`.
  - New advisory `_client_tls_problem`. It only warns (never refuses) about:
    - certificate or key unreadable, mismatched, expired, or the wrong EKU;
    - expiry within 30 days;
    - destinations not enrolled;
    - remote worker needed but `client_id is None` ("run zfsbackup-config enroll").
  - Log the server and client fingerprints and the client id at INFO.
- **`runtime_config.py`:** update "Who may read the database".
- **Edge cases:**
  - A changed id arriving during a turnover follows the normal epoch rules.
  - The `Handoff` pickles under fork, spawn and forkserver.
  - `shutdown()` works with 64 stalled handshakes.
  - A forkserver-lost API worker behaves as before.
- Verify: `pytest tests/test_zfsbackup_workers.py tests/test_zfsbackup_api_bind.py tests/test_zfsbackup_stream_gate.py tests/test_zfsbackup_reload*.py tests/test_zfsbackup_daemon*.py tests/test_zfsbackup_forkserver_death.py`, plus `test_zfsbackup_api_tls.py`.

**M10. `api.py`: authentication, the binding, `/enroll`, `/whoami`, removing `/backup/register`.**
- Owner: zfsbackup-developer. Tier: needs-approval. CR. **opus.** Depends on M9.
- Basis: decisions 1 and 4; B1's "any caller can `zfs receive -F`…"; D3, D5, D7.
- **`create_app(config, trust, server_fingerprint, *, clock)`.** There is no switch to turn authentication off.
- **`before_request`** (`/health` and `/enroll` are exempt):
  1. No certificate: 401.
  2. Unknown or expired fingerprint: 403 `client not trusted`.
  3. Otherwise `g.client_id` comes from the **binding** and `g.fingerprint` from the certificate.
  4. If the URL's `client_id` differs from the binding: 403 `{"error":"client_id mismatch","bound":…}`.
  5. If the pin has a predecessor still pinned, call `retire_predecessor` and `refresh()`. A failure is logged and retried later.
  - These checks run on the websocket upgrade too, at request or stream start only. Streams already in flight finish.
- **Routes:**
  - Remove `/backup/register`.
  - Add `/whoami`.
  - `negotiate`, `stream` and `resume_token` use `g.client_id` only.
  - A `_server_dataset` validation failure returns 400 instead of 500.
- **`PROP_CLIENT_ID`:** written from the binding. An existing dataset whose recorded id differs from the binding gets 409. `received_datasets()` is unchanged.
- **`POST /enroll`:**
  - Gating:
    - it is gated (503 with `retry` while draining);
    - 503 without retry when the server is disabled or in dry-run;
    - the body is limited to 64 KiB (413).
  - The body is `{version, client_id, label?, token_secret, cert_pem, signature}`.
  - Checks, in order:
    1. the A7 grammar (400);
    2. `check_client_cert` (422);
    3. the signature over `enrollment_payload(own_fp, client_id, fp, sha256)` (403);
    4. `zfs.exists(target/<client_id>)` (503 on error);
    5. `enroll_client`.
  - Responses from `enroll_client`:
    - `refused`: 403 with an identical body every time;
    - `client_id_taken`: 409, with a hint to ask for a `--client-id` token; the token is still usable;
    - `cert_bound_elsewhere`: 409;
    - `already_trusted`: 200;
    - `enrolled`: `refresh()` then 200 `{status, client_id, fingerprint}`;
    - `ConfigBusy`: 503 with `retry`;
    - store schema or path errors: 503 plus an ERROR log.
  - The token is never logged.
- `MAX_CONTENT_LENGTH` is 1 MiB.
- Verify: `pytest tests/test_zfsbackup_api.py tests/test_zfsbackup_stream_gate.py`, plus `test_zfsbackup_api_auth.py`.

**M11. `remote.py`: pinned mTLS client; plaintext downgrade and `ClientIdentity` removed (fifth step of the staged removal).**
- Owner: zfsbackup-developer. Tier: needs-approval. Depends on M3, M4, M9 and M10.
- Basis: decision 4 (remove the `ws://` downgrade at `remote.py:452`); D2; A1; A6.
- **Removals:** `ClientIdentity`, `_read_id_file`, `_ID_FILE_MAX` and `_register`.
- **`RemoteBackupManager(config, local_manager, *, client_id, tls_dir)`:**
  - `client_id` comes from the `Handoff`.
  - When it is `None`, every destination is skipped with one ERROR ("no client id; run `zfsbackup-config enroll`").
- **TLS identity:** reloaded each cycle when the certificate file's mtime or inode changes, so a re-enroll or `identity rotate` takes effect without a restart.
- **Transport:** one pinned `requests.Session` per destination. The websocket uses `connect_pinned` and `ws.connect(..., socket=)`. The scheme-replacement line is deleted.
- **`_whoami`, once per cycle per destination:**
  - 403 "not trusted", 403 "mismatch" (naming the bound and local ids), `PinMismatch` and the S10 alert are each logged once at ERROR, deduplicated by `_refusal_logged`;
  - in all those cases the destination is skipped **before** any `zfs send`.
- **Destination checks:** a non-https or un-enrolled destination gets one ERROR and is skipped.
- **URLs** keep `/backup/<client_id>/…`.
- **Edge case:** a pin mismatch on the websocket reaps the send, through the existing `entered=False` path.
- Verify: `pytest tests/test_zfsbackup_remote.py tests/test_zfsbackup_remote_timeouts.py`, plus `test_zfsbackup_remote_tls.py`.

**M12. CLI `trust …` and `identity show` / `identity set-client-id`.**
- Owner: zfsbackup-cli-developer. Tier: needs-approval. Depends on M3 and M7. Runs in parallel with M9–M11.
- Basis: the brief's CLI surface; D2, D3, D7.
- **`trust` group (`cli/trust.py`):**
  - `token create [--ttl 1h] [--client-id ID [--replace]] [--json]`:
    - generates the server identity if missing and permitted;
    - a scoped token requires a known id;
    - warns when replacing a token or when `remote_backup` is disabled;
    - prints the token to stdout only.
  - `token show [--json]` and `token revoke`.
  - `list [--json]`: grouped by client id, showing label, active or revoked, and the fingerprints (with how each was enrolled, `not_after` and predecessor).
  - `revoke FINGERPRINT` (a unique prefix of at least 8 hex characters is accepted) and `revoke --client ID`.
  - None of these send a SIGHUP.
- **`identity` group:**
  - `identity show [--json]`: the client id (or "not set — generated by `enroll`"), the client and server fingerprints, and the paths. It is read-only and never generates anything.
  - `identity set-client-id ID [--force]`:
    - checks the A7 grammar;
    - **refuses while any destination has a `server_fingerprint`**, unless `--force`. Each server binds this host's certificate to the OLD id, so they will answer 403 mismatch until re-enrolled. The message names the fix: revoke on the server, then re-enroll with a new certificate, or re-enroll with a `--client-id NEW` token;
    - always warns that servers will see a new client and a full send;
    - uses `set_client_id` under the writer lock with compare-and-swap. Because this is a committed generation bump, it calls `begin_reload` exactly once.
- **Exit codes:**
  - 0 ok;
  - 1 for unknown or ambiguous inputs, a bad grammar, no server identity, or refusal while enrolled;
  - 2 usage;
  - 75 for busy or conflict;
  - 78 when the DB is unusable.
- Verify: `tests/test_zfsbackup_cli_trust.py` and `tests/test_zfsbackup_cli_identity.py`.

**M13. CLI `enroll`.**
- Owner: zfsbackup-cli-developer. Tier: needs-approval. Depends on M8, M10 and M12.
- Basis: decision 4; D2 (the id is generated here); A5; A6.
- **Command:** `enroll DESTINATION [--url URL] [--label L] (--token-file PATH | --token - | prompt)`.
- **Steps:**
  1. Read the snapshot. `--url` is required for a new destination and must be https.
  2. `ensure_client_id`, under the writer lock, released immediately afterwards.
  3. Parse the token. Ensure or load the client TLS identity.
  4. If the destination already has a pin, call `/whoami` with the client certificate:
     - same id: skip to step 6 without spending the token;
     - mismatch: exit 1, naming both ids.
  5. `connect_pinned` without a client certificate. A mismatch exits 1 and the token is never sent. Then `POST /enroll`.
  6. `commit_config` sets the `url` and `server_fingerprint`.
- **SIGHUP rule:** `begin_reload` is called **exactly once**, after the last committed write. That includes the failure path where only step 2 committed.
- **Exit codes:**
  - 0 ok;
  - 1 for refused, pin mismatch, `client_id_taken` (with the operator hint), `cert_bound_elsewhere`, a bad token or another 4xx;
  - 75 for unreachable, timeout, 503 with `retry`, or a conflict or busy DB. A conflict after the server already enrolled the client prints "re-run enroll; the token is not needed again".
  - 78 when the DB is unusable.
- Verify: `tests/test_zfsbackup_cli_enroll.py`, against an in-process TLS server; the once-only SIGHUP matrix in `test_zfsbackup_cli_reload.py`.

**M14. Self-service rotation, `/rotate` (approved under D4). The store half is in M7.**
- **M14b. API.**
  - Owner: zfsbackup-developer. Tier: needs-approval. CR. **opus.** Depends on M10.
  - `POST /rotate` authenticates with the old certificate.
  - The body is `{successor_cert_pem, signature}`, where `signature` is the successor key's signature over `rotation_payload(own_fp, g.client_id, g.fingerprint, new_fp)`.
  - It is gated, and refused in dry-run.
  - Responses:
    - 200 for `rotated` or `already`;
    - 403 for `refused`;
    - 422 for a bad certificate;
    - 503 with `retry` when busy.
  - The old pin is retired on the successor's first use (`before_request`, step 5 in M10).
  - Verify: `test_zfsbackup_api_auth.py`, rotation cases.
- **M14c. CLI `identity rotate [--cert NEW --key NEW]`.**
  - Owner: zfsbackup-cli-developer. Tier: needs-approval. Depends on M13 and M14b.
  - Steps:
    1. Create the successor identity as `client.next.{crt,key}`, or take the operator's files.
    2. `/rotate` on **every** enrolled destination.
    3. Swap the files atomically only if every destination succeeded.
  - A re-run reuses `next` and gets `already` from servers that succeeded before.
  - Configured paths are updated through `commit_config`, which sends one SIGHUP. Generated default paths need no signal, because M11 reloads them on file change.
  - Exit codes: 0, 1 (some destination refused), 75 (some unreachable or busy).
  - Verify: `test_zfsbackup_cli_rotate.py`.

**M15. Remove `client_id_file` from the model (final step of the staged removal).**
- Owner: zfsbackup-developer. Tier: needs-approval. Depends on M6, M8, M9 and M11.
- Basis: D2.
- Remove the `client_id_file` field and `effective_client_id_file`. `from_file` raises a clear error for the YAML key: "removed; the client id is in the database".
- Verify: `grep -rn "client_id_file\|effective_client_id_file\|ClientIdentity\|_read_id_file" zfsbackup/` returns nothing, and the mocked suite is green.

### Wave 4

**M16. Tests.**
- Owner: pytest-test-author. Tier: low-risk. Each part lands alongside its item.
- Hermetic: stubbed zfs, 127.0.0.1 only, and the orphan net.
- **M16a. Fixture sweep, timed with M4/M6/M9/M11/M15.** About 370 references in roughly 25 files:
  - about 144 `http://` occurrences in 22 files;
  - 47 for `/backup/register` in 3 files;
  - 177 for `client_id_file`, `ClientIdentity` and `_client_id_problem` in 12 files. The heaviest are `test_zfsbackup_remote.py`, `test_zfsbackup_config_audit.py`, `test_zfsbackup_reload.py` and `test_zfsbackup_store.py`.
  - These move to DB-held ids.
  - Add conftest fixtures:
    - a certificate factory with session-cached keys;
    - an authenticated app fixture (a `TrustGateway` stub, `environ_base={"SSL_CLIENT_CERT": pem}`) for `test_zfsbackup_api.py` (124 tests) and `test_zfsbackup_stream_gate.py`;
    - a helper that seeds `host_identity`.
- **`test_zfsbackup_identity.py`** (M3/M4):
  - The A7 grammar and the generator (long hostnames, non-ASCII, a leading `.`).
  - Fingerprints and tokens.
  - The proof-of-possession checks fail for: relay to another server, a rewritten `client_id`, a swapped certificate, and a token-hash mismatch.
  - The `check_client_cert` matrix.
  - The `ensure_identity` race, and key permissions.
- **`test_zfsbackup_store_trust.py`** (M7):
  - Host identity:
    - `ensure_client_id` creates once, two concurrent calls converge, and it bumps the generation;
    - `set_client_id` uses compare-and-swap and checks the grammar;
    - **`import` and `write_config` keep the id; `export` omits it**;
    - the id is read in the same snapshot as the config.
  - Tokens:
    - scoped and unscoped, plus their time-to-live;
    - in an enrollment race, exactly one client wins;
    - a token can be used only once.
  - Enrollment outcomes:
    - `client_id_taken` for an existing row, a tombstone, or `dataset_exists`; the token is not consumed;
    - a scoped token adds a pin; `--replace` removes the other pins;
    - `cert_bound_elsewhere`;
    - several fingerprints per id.
  - Revoking one pin, and revoking a whole client.
  - Rotation, `retire_predecessor`, and concurrent retire.
  - The trust generation bumps only when pins change; a replaced file changes the key.
  - The old schema is refused; a locked DB raises `ConfigBusy`; no pooled connection is left behind.
- **`test_zfsbackup_api_auth.py`** (M10/M14b):
  - 401, 403 and expired certificates; `/health` and `/enroll` stay open.
  - **Certificate B cannot touch A's data:** a URL with id A gets 403 mismatch, and a dataset whose `PROP_CLIENT_ID` differs gets 409.
  - The websocket gets 403.
  - `/register` returns 404.
  - `/whoami`.
  - The `/enroll` matrix, including squatting an id, an existing `target/<id>` with no row, and the 409s.
  - `/rotate`, and first-use retirement.
  - The token never appears in logs.
- **`test_zfsbackup_api_tls.py`** (M9), with real handshakes and a real `ApiWorker`:
  - pinned and unpinned clients, and clients with no certificate;
  - **revocation and addition without a restart**, while a stream that already started completes;
  - stalled handshakes plus drain and `shutdown()`;
  - TLS 1.2 is refused;
  - under fork, spawn and forkserver.
- **Reload** (`test_zfsbackup_reload.py`, M9):
  - a changed `host_identity` with an unchanged config is not "identical"; it bumps the epoch, and the remote worker receives the new id;
  - an identical config with the same id causes no turnover.
- **`test_zfsbackup_remote_tls.py`** (M11):
  - an end-to-end negotiate plus a wss stream;
  - a pin mismatch reaps the send;
  - a missing id, an un-enrolled destination and an id mismatch are each skipped before any send and logged once;
  - the identity reloads when its file changes.
- **CLI:** `test_zfsbackup_cli_trust.py`, `_identity.py`, `_enroll.py` (including the SIGHUP-once rule with two commits) and `_rotate.py`. Also the `rename` notes using the DB id.
- Real-daemon `/health` probes move to `https` with verification off.
- Verify: `pytest`, and `pytest -m integration`.

**M17. Two-VM harness over TLS.**
- Owner: real-zfs-scenario-dev. Tier: low-risk. Only the `zfsb-*` VMs. Depends on M13 and M14c.
- Basis: the brief.
- **`scenarios/test_two_vm_backup.sh`:**
  - remove `CLIENT_ID_FILE`, the waits on it, and the `client_id_file` setting;
  - the server creates a token (`trust token create --json`) and transfers it as a file;
  - the client runs `enroll offsite --url https://… --token-file …`;
  - `CLIENT_ID` is read from `identity show --json`;
  - "client_id stable across restart" is now compared through `identity show`;
  - health checks use `curl -k https://`;
  - the 409 probe becomes an authenticated `curl --cert/--key -k` against `/backup/${CLIENT_ID}/…`.
- **Scenario K:**
  1. A client that is not enrolled is refused.
  2. A consumed token is refused for a second client.
  3. An unscoped token naming an existing id gets 409, and the token stays usable.
  4. **Certificate B cannot reach A's data** (403).
  5. A fingerprint revoked mid-run: the stream in flight completes and the next cycle is refused.
  6. Re-enroll with a `--client-id A --replace` token and a new certificate: the backup stays **incremental**.
  7. `identity rotate`: the next cycle succeeds, the old pin is retired, and the backup stays incremental.
  8. `import` of a YAML file leaves `identity show` unchanged.
  9. `identity set-client-id` is refused while enrolled.
  10. Importing an `http://` destination is refused.
- **`test_config_permissions.sh`:** `<db>.tls` is 0750, keys are 0600.
- Verify: the harness passes; record the run in `docs/two_vm_backup_report.md`.

**M18. Docs and wording of the invariants.**
- Owner: zfsbackup-developer. Store and CLI docstrings are updated in M7 and M12. Tier: low-risk.
- Basis: the brief; D1; D2.
- **`/Users/rejsmont/src/libzfs/CLAUDE.md`:**
  - The CLI is now described as the only writer of configuration **and of the host identity** (`host_identity.client_id`). That table is outside the six config tables: `import` and `export` never touch it, and a change bumps the config generation.
  - The trust tables have two writers, both going through `zfsbackup.config.store.trust`:
    - the CLI (`trust …`);
    - the API worker (`/enroll`, `/rotate`, retiring superseded pins).
  - Trust writes never bump the config generation and never cause a reload.
  - Reads:
    - only the supervisor reads configuration and the client id, in one snapshot, handed to workers through the `Handoff`;
    - the API worker opens the DB only through `store.trust`, on a per-call engine;
    - no other worker opens it.
  - `client_id` is a stable host id that the server binds to verified certificate fingerprints and never takes from the request.
  - Also update: `identity.py`, `<db>.tls/`, the new CLI commands, the new test files, the TLS text for the API and remote, and remove "not deployment-ready (no auth/TLS)".
- **`.claude/agents/README.md`:** add `zfsbackup/identity.py` to zfsbackup-developer, and reword "The CLI is the only writer…" the same way.
- **`zfsbackup/README.md`:** TLS setup, an enrollment walkthrough, `identity show` / `set-client-id`, re-enrolling and rotating, and what revocation covers.
- **`docs/production_readiness_report.md`:** B1 resolved. What remains:
  - werkzeug's development server;
  - no revocation of streams already running;
  - M8 retention is still driven by the client, but now only by authenticated clients;
  - "a peer can postpone a reload" now applies only to trusted senders.

---

## Order and what can run in parallel

```
M1 → M2 ─┬─ M3 (dev) ──────────────────┐
         ├─ M4 (dev) ─┐                │
         └─ M5 (store)┴─ M6 (store) → M7 (store)
                       └─ M8 (cli, after M6)
M3+M6+M7 → M9 → M10 → M11 → M14b (dev, serial)
M3+M7 → M12 (cli);  M8+M10+M12 → M13 (cli);  M13+M14b → M14c (cli)
M6+M8+M9+M11 → M15 (dev);  M16 alongside every item;  M17 after M14c;  M18 last
```

- **In parallel:**
  - M3 and M4 alongside M5;
  - M8 alongside M7;
  - M12 alongside M9–M11.
- **Must stay serial:**
  - M5 → M6 → M7;
  - M9 → M10 → M11 → M14b;
  - M13 → M14c.
- **Staged removal of `client_id_file` (each step keeps the suite testable):**
  1. M4 adds the replacements and keeps the field.
  2. M6 stops persisting it (rejected on write).
  3. M8 removes it from the CLI.
  4. M9 and M11 remove the daemon and remote readers.
  5. M15 deletes the field.
- **concurrency-reviewer** reviews M3, M7, M9 (`Handoff` and reload compare), M10 and M14b.

## Risks

- **Authorization:**
  - The binding is the only source of `client_id`. The URL id is compared, never trusted.
  - `PROP_CLIENT_ID` is written from the binding.
  - Squatting an id is blocked: rows, tombstones and existing `target/<id>` datasets all count as "known".
- **Host identity in the DB (M6, M7, M9):**
  - **A lost or replaced DB means a new client id.** Servers then see a new client and do a full send. Documented: back up the DB, or restore with `identity set-client-id`.
  - **A changed id has to reach the daemon.** That is why it rides the config generation and the identical check now compares it. A missed comparison would leave the remote worker on a stale id. M9 is marked CR and opus for this.
  - **Rejected alternative:** sharing the generation with config edits means an identity write makes a concurrent `edit` get a retryable 75. That is acceptable, and better than a second reload key.
- **Trust writes from the API worker** (M7, M10, M14b):
  - Each call builds its own engine, takes `BEGIN IMMEDIATE`, and checks the schema inside the transaction.
  - It never calls `dispose_all()` from a thread.
  - `zfs receive` runs as fork+exec with `close_fds`.
  - Retirement on first use writes at most once per rotation, and repeating it is harmless.
- **Threads before authentication:** capped at 64 with a 30 s handshake timeout. There is no rate limiting (deferred).
- **Duplex TLS on one socket (S6):** if the spike fails, serialise the websocket's sends and receives before M10.
- **No plaintext fallback, by construction:**
  - the server class refuses to run without TLS;
  - the client has no `ws://` or `http://` path;
  - validation refuses `http://`.
- **Compatibility:** client and server must move together. Nothing is deployed, so there is no migration of existing installs, and the Alembic revision is reversible (except for the dropped `client_id_file` values).
- **Retention:** untouched. `received_datasets` is unchanged.

## Deferred (not in this milestone)

- **`require_ca` mode:** deferred by your decision 6. The pin check in the app layer is already separate from the trust store, so it can be added later.
- **`zfs receive -x org.zfsbackup:{client_id,source_dataset,config}`:** defence in depth; server-local properties already take precedence.
- **More trust commands:**
  - `trust add <cert.pem> --client-id` for air-gapped enrollment;
  - `trust forget --client ID` to clear a tombstone.
- **Server identity:**
  - re-keying the server certificate (it is valid 10 years; re-keying means re-enrolling every client);
  - server-side path overrides for the certificate and key.
- **Revocation and enrollment limits:**
  - revoking a stream mid-flight;
  - rate-limiting failed enrollments.
- **Client-side automation:**
  - recording the fingerprint as a ZFS property;
  - automatic rotation when an operator renews the certificate outside the CLI (use `identity rotate --cert/--key`).
- **Operations:**
  - a production WSGI server;
  - raw sends for encrypted datasets;
  - reporting the applied trust generation.

---

## For your approval

The plan is 18 items in 5 waves, with no open decisions left:

- **Foundations:**
  - add the dependency, then spike the TLS behaviour;
  - the shared `identity` module;
  - the new config fields and client id helpers;
  - one Alembic revision: `host_identity`, the trust tables, the new columns, and dropping `client_id_file`.
- **Store:**
  - the snapshot carries `client_id`;
  - the trust and host-identity API, including the store half of `/rotate`;
  - the CLI config surface.
- **Daemon and server:**
  - an HTTPS-only API server with per-connection handshakes;
  - `client_id` passed through the `Handoff`, and compared on reload;
  - authentication with the binding as the only source of `client_id`;
  - the pinned client.
- **CLI:** `trust`, `identity`, `enroll`, and `/rotate` with its CLI.
- **Finish:** the final `client_id_file` removal, about 370 test references, two-VM scenario K, and docs.

Everything except M1, M2, M16, M17 and M18 is `needs-approval`.

Please confirm the two choices that follow from the revised D2:
- the client id is in the config snapshot and bumps the generation when it changes;
- `enroll` does two commits and sends one SIGHUP.

Beyond that, approve, adjust, reprioritise or drop items as you like. Implementation starts only after your sign-off. If a spike in M2 fails, it comes back to me first.

Key files:
- `/Users/rejsmont/src/libzfs/docs/tls_mtls_plan.md` (replace with this text)
- `/Users/rejsmont/src/libzfs/zfsbackup/api.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/remote.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/workers.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/daemon.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/runtime_config.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/model.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/store/models.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/store/mapper.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/store/access.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/store/db.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config/store/migrate.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/main.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/write.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/render.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/dotpath.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/editor.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/listing.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/cli/rename.py`
- `/Users/rejsmont/src/libzfs/zfsbackup/config.example.yaml`
- `/Users/rejsmont/src/libzfs/scenarios/test_two_vm_backup.sh`
- `/Users/rejsmont/src/libzfs/docs/production_readiness_report.md`
- `/Users/rejsmont/src/libzfs/CLAUDE.md`
- `/Users/rejsmont/src/libzfs/.claude/agents/README.md`
- `/Users/rejsmont/src/libzfs/pyproject.toml`

---

## M2 results (2026-10-20): all spikes S0–S10 pass, no fallback needed

Recorded in `tests/test_zfsbackup_tls_platform.py` (45 tests, `integration`). Facts later items depend on:

- **M9 server hook:** wrap each connection in `process_request_thread` (runs on the connection's thread,
  before `finish_request`; owns `shutdown_request` in its `finally`). `get_request`/`process_request` run on
  the accept thread and would reintroduce the stall. Construct werkzeug without `ssl_context`, then assign
  `self.ssl_context` so the environ reports `https` and sets `SSL_CLIENT_CERT`. Stock werkzeug
  `ssl_context` + one silent peer stalls the next client (confirmed).
- **M11 `requests` pinning (S7):** an `HTTPAdapter` whose `init_poolmanager` passes
  `ssl_context=<CERT_NONE, check_hostname=False, client cert>` and `assert_fingerprint=<sha256 hex>` **must
  also override `cert_verify` to force `verify=False`**: urllib3 re-applies `cert_reqs` from requests'
  `verify` on every connection, so the default `verify=True` turns the context back into CERT_REQUIRED +
  certifi. With the override, session/request `verify` and `REQUESTS_CA_BUNDLE`/`CURL_CA_BUNDLE` change
  nothing. A wrong fingerprint raises `requests.exceptions.SSLError` before any HTTP request is sent.
- **M11 websocket (S8):** `WebSocket().connect("wss://…", socket=<SSLSocket>)` reuses the socket as-is; the
  URL host and `sslopt` are ignored.
- **M11 error mapping (S10):** under TLS 1.3 a rejected client certificate surfaces after the handshake, on
  first read or write: raw `ssl` → plain `ssl.SSLError` (reason `TLS_ALERT_*` on OpenSSL 4.0,
  `TLSV1_ALERT_*`/`SSLV3_ALERT_*` on older builds — match both); `requests` → `requests.exceptions.SSLError`
  (alert in `str(exc)`); websocket-client → the raw `ssl.SSLError` escapes `connect()` (not a
  `WebSocketException`). A `before_request` 401/403 on a `flask_sock` route → `WebSocketBadStatusException`
  with that `.status_code` (S9).
- **S6:** duplex wss on one `SSLSocket` is clean (64 MiB, hashes match). `simple_websocket` uses
  `sock.send`, not `sendall`; no partial-write problem was observed.
- **M11 rejection detection (from `test_zfsbackup_identity.py`):** with `pinned_session`, a client
  certificate the server rejects usually surfaces as `requests.SSLError` carrying the TLS alert, but in about
  1 run in 30 requests has already written its request and gets `requests.ConnectionError(BrokenPipeError)`
  or a reset instead. M11 must not treat "got an alert" as the only sign of rejection: a reset straight after
  a completed handshake on a destination that used to work should be reported as a possible rejection, and
  `/whoami` should be retried once before concluding.

## M10/M14b wire contract (as implemented, 2026-10-20)

Every endpoint except `/health` and `/enroll` needs a pinned client certificate. No certificate → 401
`{"error":"client certificate required"}` (also for unknown URLs); unpinned → normally a TLS alert
(`*_ALERT_UNKNOWN_CA`), 403 `{"error":"client not trusted"}` if it reaches the app; URL `<client_id>` ≠ binding →
403 `{"error":"client_id mismatch","bound":"<id>"}` (websocket upgrade too). `/enroll` and `/rotate` error bodies
carry a stable `reason`.

- `GET /whoami` → 200 `{"client_id": <bound id>, "fingerprint": <this cert, 64 hex>}`.
- `POST /enroll` (no client cert), JSON ≤ 64 KiB:
  `{"version":1, "client_id", "label"? (≤128, no control chars), "token_secret": <the WHOLE token zfsb1.<fp>.<secret>>,
  "cert_pem", "signature": base64(sign(cert key, enrollment_payload(server_fp, client_id, cert_fp, secret_hash(secret))))}`.
  Order: shape 400 → grammar 400 → label 400 → token parse 400 → token fp ≠ own 403 refused → `check_client_cert`
  422 → signature 403 `bad_signature` → token pre-check 403 `refused` (read-only `token_precheck`, same body as a
  store refusal; never reaches `zfs`) → `zfs.exists(target/<id>)` 503 → store (`enroll_client`, the authority).
  200 `enrolled` | 200 `already_trusted` (live token only, not spent) | 403 `refused` (identical for every token
  problem) | 409 `client_id_taken` (+`hint`; token not spent) | 409 `cert_bound_elsewhere` | 413 | 422
  `bad_certificate` | 503 `retry:true` (`busy`, `zfs`, draining) | 503 no retry (`remote_backup disabled`, `dry_run`,
  `unavailable`). Idempotency for a client that is already enrolled comes from `/whoami`, not `/enroll`.
- `POST /rotate` (current cert), JSON ≤ 64 KiB: `{"version":1, "successor_cert_pem", "signature":
  base64(sign(SUCCESSOR key, rotation_payload(server_fp, bound_id, current_fp, new_fp)))}` → 200
  `{"status":"rotated"|"already","client_id","fingerprint":<new>}`; 403 `bad_signature` / `refused`; 400/413/422/503
  as above. The old pin is retired on the successor's first authenticated request.

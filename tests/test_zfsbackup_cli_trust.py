"""`zfsbackup-config trust ...` (TLS/mTLS plan M12, D3, D7, A2, A5).

The server side of enrollment: the one enrollment token and the pinned client
certificates. Every database here is built by `import` and read back through the
store's public API; pins are seeded by redeeming a real token through
`store.enroll_client` (real self-signed certificates for the ordinary cases, fake
64-hex pins where a test needs a fingerprint it can control, e.g. a shared
prefix). Nothing here reads or writes an ORM object, and nothing reaches a real
`zfs` (the one subprocess test uses `_hermetic.hermetic_env()`).

Covered:

- ``TestTokenCreate``            output, JSON shape, the stored row, warnings, scoping
- ``TestTokenCreateTtl``         accepted / rejected lifetimes
- ``TestTokenCreateErrors``      usage, grammar, unknown id, nothing created on a refusal
- ``TestTokenCreateIdentity``    server identity: generated, absent and unwritable,
                                 existing (peek only, an unreadable key does not matter)
- ``TestTokenSecrecy``           the token only ever on stdout, on every path
- ``TestTokenShow``              none / active / expired / consumed, text and JSON,
                                 `server_certificate_matches`
- ``TestTokenRevoke``            with and without a token
- ``TestTrustList``              grouping, revoked, expired, predecessor, empty, JSON
- ``TestTrustRevokeFingerprint`` prefix grammar, ambiguity, the notes
- ``TestTrustRevokeRetired``     `trust revoke` of a fingerprint a rotation retired: the chain is
                                 followed to the live successors, nothing-live-left, plain wording
                                 for every other record
- ``TestTrustRevokeAmbiguousRevoked`` a prefix matching several revoked records is listed, not revoked
- ``TestTrustListRetired``       `retired (replaced by C)` in the text, `superseded_by` in JSON
- ``TestTrustRevokeClient``      unknown, already revoked (no write), success
- ``TestNoDaemonSignal``         no trust command calls `request_reload`
- ``TestMissingDatabase``        exit 78 and nothing created
- ``TestGroupOperator``          a non-owner of the TLS files (ownership mocked)
- ``TestOptionPlacement``        `-c` / `-v` before and after every subcommand
- ``TestConcurrentTokenCreate``  two real processes racing (integration)

Mutation counts (scratch copy of the tree, one revert at a time against
`zfsbackup/cli/trust.py`; failing tests across THIS file and its sibling
`test_zfsbackup_cli_identity.py`): active-token check widened to any previous
row 2; MIN_PREFIX 8 -> 4 9; ambiguity check removed 2; "last pin" note
unconditional 1; `--client` without the already-revoked return 1;
`server_certificate_matches` for any state 1; token echoed on stderr under -v 4;
`ensure_identity` instead of the peek 10; `trust revoke` signalling the daemon 22;
`set-client-id` without the enrolled refusal 3; two signals 7; refusal even with
no id stored 4; `--replace` without `--client-id` allowed 1; `7d` refused 2; the
TLS dir created before the preflight 5; `identity show` creating the TLS dir 1;
`key_present` always true 4; `replaced_previous` always false 1; predecessor
column blank 1; signal after the output 2; token in the unknown-scope error 2;
`--ttl 0m` accepted 2; no colon normalisation 2; unknown-id listing blank 2; a
same-id set not a no-op 2; the `--force` warning dropped 1; the NEW-client
warning dropped 2.
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from _cli_support import (
    PLAIN_YAML,
    Cli,
    db,
    db_state,
    fake_daemon,
    generation,
    isolated,
    make_db as _make_db,
)
from zfsbackup import daemonlock, identity
from zfsbackup.cli import trust as trust_cli
from zfsbackup.config.store import (
    ConfigBusy,
    EnrollOutcome,
    NewPin,
    create_token,
    dispose_all,
    enroll_client,
    list_clients,
    read_trust_key,
    resolve_config_path,
    retire_predecessor,
    revoke_client,
    revoke_pin,
    rotate_client,
    token_status,
)

# No module-level tier marker: conftest marks a test that declares none as
# `unit`; the subprocess test is `integration` on its own.

REPO = Path(__file__).resolve().parent.parent

REMOTE_ON = PLAIN_YAML + 'remote_backup:\n  enabled: true\n  target_dataset: "tank/backups"\n'
REMOTE_OFF = PLAIN_YAML + 'remote_backup:\n  enabled: false\n  target_dataset: "tank/backups"\n'

#: The server fingerprint tokens carry when no real server certificate matters.
SERVER_FP = "ab" * 32
TOKEN_RE = re.compile(r"zfsb1\.[0-9a-f]{64}\.[A-Za-z0-9_-]{43}")
ISO_RE = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ")


# --------------------------------------------------------------------------
# Fixtures and helpers
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def no_reload(monkeypatch):
    """Every test here runs with `request_reload` replaced by a counter, and fails
    at teardown if anything called it: NO trust command signals the daemon (plan
    M12). Tests that want the list read it; the assertion is implicit."""
    calls = []

    def recorder(resolved, *, skip_if_applied=None):
        calls.append(skip_if_applied)
        return daemonlock.ReloadRequest("no-daemon")

    monkeypatch.setattr(daemonlock, "request_reload", recorder)
    yield calls
    assert calls == [], f"a trust command asked the daemon to reload: {calls}"


@pytest.fixture
def run(no_reload) -> Cli:
    cli = Cli()
    cli.no_reload = no_reload
    return cli


def make_db(run, database, text=REMOTE_ON) -> Path:
    """`import` the database (which, being a config write, DOES ask the daemon to
    reload) and forget those calls: only the commands under test are counted."""
    _make_db(run, database, text)
    run.no_reload.clear()
    return database


@pytest.fixture
def server(run, db) -> Path:
    """A database with `remote_backup` enabled."""
    return make_db(run, db, REMOTE_ON)


@pytest.fixture
def resolved(server):
    return resolve_config_path(str(server))


def tls_of(database: Path) -> Path:
    return Path(str(database) + ".tls")


def fingerprints(items) -> list:
    return [p.fingerprint for c in items for p in c.pins]


_KEY = None


def make_cert(cn: str = "client", *, valid_from=None, valid_to=None):
    """A real self-signed certificate -> `(pem, fingerprint, not_after_epoch)`.
    One EC key is reused (generating keys is the slow part); the serial and CN
    make every certificate, and so every fingerprint, distinct."""
    global _KEY
    if _KEY is None:
        _KEY = ec.generate_private_key(ec.SECP256R1())
    now = datetime.now(timezone.utc)
    valid_from = valid_from or now - timedelta(minutes=5)
    valid_to = valid_to or now + timedelta(days=365)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name).public_key(_KEY.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(valid_from).not_valid_after(valid_to)
        .sign(_KEY, hashes.SHA256())
    )
    pem = cert.public_bytes(serialization.Encoding.PEM).decode("ascii")
    return pem, identity.fingerprint_pem(pem), valid_to.timestamp()


def fake_pin(prefix: str, fill: str, *, not_after: float = None) -> NewPin:
    """A pin with a chosen fingerprint: `prefix` then `fill` repeated to 64 hex."""
    fp = (prefix + fill * 64)[:64]
    return NewPin(
        fp, "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n",
        not_after if not_after is not None else time.time() + 86400 * 365,
    )


def real_pin(cn: str = "client", **kw) -> NewPin:
    pem, fp, not_after = make_cert(cn, **kw)
    return NewPin(fp, pem, not_after)


def enroll(resolved, client_id, pin, *, label=None, scoped=False, replace=False,
           now=None, server_fp=SERVER_FP) -> None:
    """Pin `pin` for `client_id` by redeeming a real token (created through the
    store with a window around `now`). `scoped` makes it a `--client-id` token."""
    now = time.time() if now is None else now
    _token, digest = identity.make_token(server_fp)
    create_token(
        resolved, secret_sha256=digest, server_fingerprint=server_fp,
        created_at=now - 1, expires_at=now + 3600,
        client_id=client_id if scoped else None, replace=replace, lock=None,
    )
    outcome = enroll_client(
        resolved, secret_sha256=digest, server_fingerprint=server_fp,
        client_id=client_id, pin=pin, label=label, now=now, dataset_exists=False,
    )
    assert outcome == EnrollOutcome.ENROLLED, outcome


def stored_token_hash(database: Path):
    """The `secret_sha256` column, read straight from SQLite (the store API never
    returns it); `None` when there is no token row."""
    conn = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        rows = conn.execute("SELECT secret_sha256 FROM enrollment_token").fetchall()
    finally:
        conn.close()
    assert len(rows) <= 1
    return rows[0][0] if rows else None


def token_of(result) -> str:
    """The one token on stdout (text or JSON output)."""
    found = TOKEN_RE.findall(result.stdout)
    assert len(found) == 1, result.stdout
    return found[0]


def hash_of(token: str) -> str:
    _fp, secret = identity.parse_token(token)
    return identity.secret_hash(bytes(secret))


def jout(result):
    assert result.exit_code == 0, result.stderr
    return json.loads(result.stdout)


def create(run, database, *args):
    return run("trust", "token", "create", "-c", database, *args)


def list_json(run, database):
    return jout(run("trust", "list", "--json", "-c", database))


def tree(root: Path) -> list:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


def stderr_has(result, fragment: str) -> bool:
    return fragment in result.stderr


def server_identity(database: Path):
    return identity.ensure_identity(tls_of(database), identity.ROLE_SERVER)


# --------------------------------------------------------------------------
# trust token create
# --------------------------------------------------------------------------


class TestTokenCreate:
    def test_text_output_has_the_token_on_stdout_only(self, run, server):
        result = create(run, server)
        assert result.exit_code == 0, result.stderr
        token = token_of(result)
        assert token not in result.stderr
        fp = identity.parse_token(token)[0]
        assert f"server fingerprint: {fp}" in result.stdout
        assert "valid for:          1h" in result.stdout
        assert "usable by:          any new client id" in result.stdout
        assert "zfsbackup-config enroll NAME --url https://<this-host>:8080 --token-file FILE" in result.stdout

    def test_the_stored_row_is_the_hash_of_the_printed_token(self, run, server, resolved):
        token = token_of(create(run, server))
        assert stored_token_hash(server) == hash_of(token)
        status = token_status(resolved, time.time())
        assert status.state == "active"
        assert status.server_fingerprint == identity.parse_token(token)[0]
        assert status.client_id is None and status.replace is False
        assert status.expires_at - status.created_at == pytest.approx(3600, abs=1)

    def test_the_first_run_generates_the_server_identity_and_says_so(self, run, server):
        assert not tls_of(server).exists()
        result = create(run, server)
        cert = tls_of(server) / "server.crt"
        key = tls_of(server) / "server.key"
        assert cert.is_file() and key.is_file()
        fp = identity.fingerprint_pem(cert.read_text())
        assert f"warning: generated a new server TLS identity in {tls_of(server)} (fingerprint {fp})" in result.stderr
        assert identity.parse_token(token_of(result))[0] == fp
        assert result.exit_code == 0

    def test_the_second_run_reuses_it_without_a_warning(self, run, server):
        first = create(run, server)
        second = create(run, server)
        assert "generated a new server TLS identity" not in second.stderr
        assert identity.parse_token(token_of(first))[0] == identity.parse_token(token_of(second))[0]
        assert token_of(first) != token_of(second)

    def test_json_shape_and_values(self, run, server, resolved):
        before = time.time()
        data = jout(create(run, server, "--json"))
        assert set(data) == {
            "token", "server_fingerprint", "created_at", "expires_at",
            "client_id", "replace", "replaced_previous",
        }
        assert TOKEN_RE.fullmatch(data["token"])
        assert data["server_fingerprint"] == identity.parse_token(data["token"])[0]
        assert re.fullmatch(r"[0-9a-f]{64}", data["server_fingerprint"])
        assert ISO_RE.fullmatch(data["created_at"]) and ISO_RE.fullmatch(data["expires_at"])
        created = datetime.strptime(data["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        expires = datetime.strptime(data["expires_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        assert (expires - created).total_seconds() == pytest.approx(3600, abs=1)
        assert abs(created.timestamp() - before) < 30
        assert data["client_id"] is None
        assert data["replace"] is False
        assert data["replaced_previous"] is False

    def test_json_stdout_is_only_the_json(self, run, server):
        result = create(run, server, "--json")
        assert json.loads(result.stdout)["token"] not in result.stderr
        assert result.stdout.endswith("}\n")

    def test_the_warning_about_a_disabled_remote_backup(self, run, db):
        make_db(run, db, REMOTE_OFF)
        result = create(run, db)
        assert result.exit_code == 0, result.stderr
        assert "warning: remote_backup is not enabled in the stored configuration" in result.stderr
        assert "set remote_backup.enabled true" in result.stderr
        assert token_of(result)  # still issued

    def test_the_warning_when_there_is_no_remote_backup_section(self, run, db):
        make_db(run, db, PLAIN_YAML)
        result = create(run, db)
        assert result.exit_code == 0, result.stderr
        assert "remote_backup is not enabled" in result.stderr

    def test_no_such_warning_when_remote_backup_is_enabled(self, run, server):
        assert "remote_backup is not enabled" not in create(run, server).stderr

    def test_the_hint_names_the_database_when_it_is_not_the_default(self, run, db):
        make_db(run, db, PLAIN_YAML)
        assert f"zfsbackup-config -c {db} set remote_backup.enabled true" in create(run, db).stderr

    def test_the_port_comes_from_the_stored_configuration(self, run, db):
        make_db(run, db, REMOTE_ON + "api_port: 9443\n")
        assert "https://<this-host>:9443" in create(run, db).stdout

    # ---- replacing a token ----

    def test_replacing_an_active_token_warns_and_invalidates_it(self, run, server):
        first = token_of(create(run, server))
        result = create(run, server, "--json")
        data = json.loads(result.stdout)
        assert data["replaced_previous"] is True
        assert "warning: replaced the previous enrollment token, which was still active" in result.stderr
        assert "it can no longer be used" in result.stderr
        assert stored_token_hash(server) == hash_of(data["token"]) != hash_of(first)

    def test_an_expired_previous_token_is_replaced_silently(self, run, server, resolved):
        now = time.time()
        _t, digest = identity.make_token(SERVER_FP)
        create_token(resolved, secret_sha256=digest, server_fingerprint=SERVER_FP,
                     created_at=now - 7200, expires_at=now - 3600, lock=None)
        result = create(run, server, "--json")
        assert json.loads(result.stdout)["replaced_previous"] is False
        assert "replaced the previous enrollment token" not in result.stderr

    def test_a_consumed_previous_token_is_replaced_silently(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        result = create(run, server, "--json")
        assert json.loads(result.stdout)["replaced_previous"] is False
        assert "replaced the previous enrollment token" not in result.stderr

    # ---- scoping ----

    def test_a_scoped_token_for_a_known_id(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        result = create(run, server, "--client-id", "c1", "--json")
        data = jout(result)
        assert data["client_id"] == "c1" and data["replace"] is False
        status = token_status(resolved, time.time())
        assert status.client_id == "c1" and status.replace is False
        assert "REPLACES" not in result.stderr

    def test_the_text_output_names_a_scoped_id(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        assert "usable by:          client id c1\n" in create(run, server, "--client-id", "c1").stdout

    def test_replace_warns_and_is_recorded(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        result = create(run, server, "--client-id", "c1", "--replace", "--json")
        data = jout(result)
        assert data["replace"] is True and data["client_id"] == "c1"
        assert "warning: enrolling with this token REPLACES every certificate pinned for client id 'c1'" in result.stderr
        status = token_status(resolved, time.time())
        assert status.client_id == "c1" and status.replace is True

    def test_the_text_output_says_replacing(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        out = create(run, server, "--client-id", "c1", "--replace").stdout
        assert "usable by:          client id c1 (replacing its certificates)" in out

    def test_a_replace_token_tells_the_operator_the_client_needs_new_key(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        result = create(run, server, "--client-id", "c1", "--replace")
        assert "enroll NAME --url https://<this-host>:" in result.stdout
        assert "--token-file FILE --new-key" in result.stdout
        assert "a plain `enroll` would find the host already enrolled and not use the token" in result.stdout
        # the warning (stderr) says the same, and the token stays alone on stdout
        assert "`zfsbackup-config enroll NAME --new-key`" in result.stderr
        assert "--new-key" not in token_of(result)

    def test_replace_warns_about_the_cost_for_a_client_enrolled_elsewhere(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        warning = " ".join(create(run, server, "--client-id", "c1", "--replace").stderr.split())
        assert "The cost:" in warning
        assert "a fresh key" in warning
        assert "trust token create --client-id c1 --replace` token from each of them" in warning
        assert "plain `enroll` there" in warning

    def test_a_scoped_token_mentions_new_key_for_a_revoked_certificate(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        out = create(run, server, "--client-id", "c1").stdout
        assert "If this client's certificate was revoked here, add --new-key" in out
        assert "--token-file FILE\n" in out or "--token-file FILE " in out    # no --new-key in the command

    def test_an_unscoped_token_has_neither_hint(self, run, server):
        out = create(run, server).stdout
        assert "--new-key" not in out

    def test_a_revoked_id_may_be_the_scope(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        revoke_client(resolved, "c1", lock=None)
        assert create(run, server, "--client-id", "c1").exit_code == 0

    def test_creating_a_token_pins_nothing_and_does_not_move_the_trust_generation(self, run, server, resolved):
        before = read_trust_key(resolved)
        create(run, server)
        create(run, server)
        assert read_trust_key(resolved) == before
        assert list_clients(resolved) == ()

    def test_it_does_not_bump_the_config_generation(self, run, server):
        gen = generation(server)
        create(run, server)
        assert generation(server) == gen


class TestTokenCreateTtl:
    @pytest.mark.parametrize("ttl, seconds", [
        ("30m", 1800), ("1h", 3600), ("2d", 172800), ("6d12h", 561600),
        ("7d", 604800), ("1w", 604800), ("1h30m", 5400),
    ])
    def test_accepted(self, run, server, resolved, ttl, seconds):
        assert create(run, server, "--ttl", ttl).exit_code == 0
        status = token_status(resolved, time.time())
        assert status.expires_at - status.created_at == pytest.approx(seconds, abs=1)

    def test_the_default_is_one_hour(self, run, server, resolved):
        create(run, server)
        status = token_status(resolved, time.time())
        assert status.expires_at - status.created_at == pytest.approx(3600, abs=1)

    @pytest.mark.parametrize("ttl", ["7d1s", "7d1m", "8d", "2w", "1M"])
    def test_longer_than_seven_days_is_refused(self, run, server, resolved, ttl):
        result = create(run, server, "--ttl", ttl)
        assert result.exit_code == 1
        assert "maximum token lifetime of 7 days" in result.stderr
        assert token_status(resolved, time.time()).state == "none"
        assert TOKEN_RE.search(result.stdout + result.stderr) is None

    @pytest.mark.parametrize("ttl", ["0m", "0h", "--ttl=-1h", "--ttl=-30m"])
    def test_zero_and_negative_are_refused(self, run, server, resolved, ttl):
        args = (ttl,) if ttl.startswith("--ttl=") else ("--ttl", ttl)
        result = create(run, server, *args)
        assert result.exit_code == 1
        assert "must be positive" in result.stderr
        assert token_status(resolved, time.time()).state == "none"

    @pytest.mark.parametrize("ttl", ["0", "abc", "", "1x", "1.5h", " 1h", "1h1h", "h"])
    def test_garbage_is_refused(self, run, server, resolved, ttl):
        result = create(run, server, "--ttl", ttl)
        assert result.exit_code == 1
        assert f"invalid --ttl {ttl!r}" in result.stderr
        assert "examples: 30m, 1h, 2d" in result.stderr
        assert token_status(resolved, time.time()).state == "none"

    def test_a_refused_ttl_creates_nothing_beside_the_database(self, run, server):
        before = tree(server.parent)
        assert create(run, server, "--ttl", "9d").exit_code == 1
        assert tree(server.parent) == before


class TestTokenCreateErrors:
    def test_replace_needs_a_client_id_and_is_a_usage_error(self, run, server):
        before = tree(server.parent)
        result = create(run, server, "--replace")
        assert result.exit_code == 2
        assert "--replace needs --client-id" in result.stderr
        assert tree(server.parent) == before  # no identity was generated

    @pytest.mark.parametrize("bad", [".hidden", "a b", "a/b", "é", "x" * 65, "a;b", "a\nb"])
    def test_a_bad_client_id_grammar_is_refused(self, run, server, resolved, bad):
        before = tree(server.parent)
        result = create(run, server, "--client-id", bad)
        assert result.exit_code == 1
        assert "invalid --client-id" in result.stderr
        assert tree(server.parent) == before
        assert token_status(resolved, time.time()).state == "none"

    def test_an_empty_client_id_is_refused(self, run, server):
        result = create(run, server, "--client-id", "")
        assert result.exit_code == 1
        assert "must not be empty" in result.stderr

    def test_an_unknown_scoped_id_is_refused_with_a_hint(self, run, db):
        make_db(run, db, REMOTE_ON)
        resolved = resolve_config_path(str(db))
        result = create(run, db, "--client-id", "nobody")
        assert result.exit_code == 1
        assert "unknown client id 'nobody'" in result.stderr
        assert f"See `zfsbackup-config -c {db} trust list`" in result.stderr
        assert "omit --client-id" in result.stderr
        assert token_status(resolved, time.time()).state == "none"
        assert TOKEN_RE.search(result.stdout + result.stderr) is None

    def test_an_unknown_scoped_id_leaves_an_existing_token_alone(self, run, server, resolved):
        token = token_of(create(run, server))
        assert create(run, server, "--client-id", "nobody").exit_code == 1
        assert stored_token_hash(server) == hash_of(token)

    def test_a_busy_database_is_exit_75(self, run, server, monkeypatch):
        def busy(*a, **k):
            raise ConfigBusy("the config database is locked")

        monkeypatch.setattr(trust_cli, "create_token", busy)
        assert create(run, server).exit_code == 75

    def test_an_unloadable_stored_configuration_stops_before_anything_is_generated(
        self, run, server, monkeypatch
    ):
        def broken(resolved):
            raise trust_cli.CliError("the stored configuration cannot be loaded: nope")

        monkeypatch.setattr(trust_cli, "read_snapshot", broken)
        result = create(run, server)
        assert result.exit_code == 1
        assert not tls_of(server).exists()


class TestTokenCreateIdentity:
    def test_no_identity_and_no_way_to_make_one_is_exit_1_with_the_advice(self, run, server, monkeypatch):
        def refuse(directory, role, **kw):
            raise identity.IdentityError(f"cannot write the server identity in {directory}: permission denied")

        monkeypatch.setattr(identity, "ensure_identity", refuse)
        result = create(run, server)
        assert result.exit_code == 1
        assert "this host has no server TLS identity yet and it could not be created" in result.stderr
        assert "permission denied" in result.stderr
        assert "Start the daemon once (not with --dry-run: it creates" in result.stderr
        assert "at its first start)" in result.stderr
        assert "run this command as root" in result.stderr
        assert str(tls_of(server) / "server.crt") in result.stderr
        assert result.stdout == ""
        assert stored_token_hash(server) is None

    def test_an_oserror_from_the_generation_is_worded_the_same_way(self, run, server, monkeypatch):
        def refuse(directory, role, **kw):
            raise PermissionError(13, "Permission denied", str(directory))

        monkeypatch.setattr(identity, "ensure_identity", refuse)
        result = create(run, server)
        assert result.exit_code == 1
        assert "could not be created: Permission denied" in result.stderr
        assert "Start the daemon once" in result.stderr
        assert stored_token_hash(server) is None

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can write any directory")
    def test_a_real_unwritable_tls_directory(self, run, server):
        tls = tls_of(server)
        tls.mkdir(mode=0o750)
        os.chmod(tls, 0o550)
        try:
            result = create(run, server)
        finally:
            os.chmod(tls, 0o750)
        assert result.exit_code == 1
        assert "this host has no server TLS identity yet and it could not be created" in result.stderr
        assert "Start the daemon once (not with --dry-run: it creates" in result.stderr
        assert "at its first start)" in result.stderr
        assert not (tls / "server.crt").exists()
        assert stored_token_hash(server) is None

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads any file")
    def test_an_existing_identity_is_only_peeked_an_unreadable_key_does_not_matter(self, run, server):
        ident = server_identity(server)
        cert, key = ident.cert_path, ident.key_path
        os.chmod(cert, 0o640)
        os.chmod(key, 0o000)
        os.chmod(tls_of(server), 0o550)  # and nothing could be written either
        try:
            result = create(run, server, "--json")
        finally:
            os.chmod(tls_of(server), 0o750)
            os.chmod(key, 0o600)
        assert result.exit_code == 0, result.stderr
        assert json.loads(result.stdout)["server_fingerprint"] == ident.fingerprint
        assert "generated a new server TLS identity" not in result.stderr

    def test_an_existing_identity_is_never_loaded_or_regenerated(self, run, server, monkeypatch):
        ident = server_identity(server)

        def forbidden(*a, **k):
            raise AssertionError("a certificate that exists must only be peeked")

        monkeypatch.setattr(identity, "ensure_identity", forbidden)
        monkeypatch.setattr(identity, "load_identity", forbidden)
        result = create(run, server, "--json")
        assert result.exit_code == 0, result.stderr
        assert json.loads(result.stdout)["server_fingerprint"] == ident.fingerprint

    def test_an_unparseable_certificate_is_exit_1_and_is_not_replaced(self, run, server):
        tls = tls_of(server)
        tls.mkdir(mode=0o750)
        (tls / "server.crt").write_text("this is not a certificate\n")
        result = create(run, server)
        assert result.exit_code == 1
        assert "does not contain a PEM certificate" in result.stderr
        assert (tls / "server.crt").read_text() == "this is not a certificate\n"
        assert not (tls / "server.key").exists()
        assert stored_token_hash(server) is None

    def test_an_expired_server_certificate_still_issues_tokens(self, run, server):
        tls = tls_of(server)
        tls.mkdir(mode=0o750)
        pem, fp, _ = make_cert(
            "server", valid_from=datetime.now(timezone.utc) - timedelta(days=30),
            valid_to=datetime.now(timezone.utc) - timedelta(days=1),
        )
        (tls / "server.crt").write_text(pem)
        result = create(run, server, "--json")
        assert result.exit_code == 0, result.stderr
        assert json.loads(result.stdout)["server_fingerprint"] == fp


# --------------------------------------------------------------------------
# The token is a secret
# --------------------------------------------------------------------------


@pytest.fixture
def made_tokens(monkeypatch):
    """Record every token `make_token` returns (the real function still runs), so
    a test knows the secret to look for even on a path that never prints it."""
    seen = []
    real = identity.make_token

    def spy(server_fp):
        token, digest = real(server_fp)
        seen.append(str(token))
        return token, digest

    monkeypatch.setattr(identity, "make_token", spy)
    return seen


class TestTokenSecrecy:
    def test_only_stdout_has_it(self, run, server, made_tokens):
        result = create(run, server)
        assert made_tokens and made_tokens[0] in result.stdout
        assert made_tokens[0] not in result.stderr

    def test_verbose_output_never_has_it_on_stderr_or_in_the_logs(self, run, server, made_tokens, caplog):
        caplog.set_level(0)
        result = create(run, server, "-v")
        assert result.exit_code == 0, result.stderr
        (token,) = made_tokens
        assert token in result.stdout and token not in result.stderr
        secret = token.rsplit(".", 1)[1]
        assert secret not in result.stderr and secret not in caplog.text
        assert token not in caplog.text

    def test_verbose_json_output(self, run, server, made_tokens):
        result = create(run, server, "--json", "-v")
        (token,) = made_tokens
        assert json.loads(result.stdout)["token"] == token
        assert token not in result.stderr

    def test_a_failed_write_prints_no_token_anywhere(self, run, server, made_tokens, monkeypatch):
        def busy(*a, **k):
            raise ConfigBusy("the config database is locked by another process")

        monkeypatch.setattr(trust_cli, "create_token", busy)
        result = create(run, server)
        assert result.exit_code == 75
        assert made_tokens, "the token was generated before the write failed"
        for stream in (result.stdout, result.stderr):
            assert made_tokens[0] not in stream
            assert made_tokens[0].rsplit(".", 1)[1] not in stream
        assert stored_token_hash(server) is None

    def test_a_failed_write_under_verbose_prints_no_token_either(self, run, server, made_tokens, monkeypatch):
        def broken(*a, **k):
            raise RuntimeError("storage exploded")

        monkeypatch.setattr(trust_cli, "create_token", broken)
        result = create(run, server, "-v")
        assert result.exit_code == 1
        assert "storage exploded" in result.stderr
        (token,) = made_tokens
        assert token not in result.stdout + result.stderr
        assert token.rsplit(".", 1)[1] not in result.stdout + result.stderr

    def test_the_unknown_scope_error_generates_and_prints_no_token(
        self, run, server, made_tokens
    ):
        # M12 fix: an unknown --client-id is refused BEFORE anything is generated, so
        # there is no token to leak, and no server identity (`.tls` directory) either.
        result = create(run, server, "--client-id", "nobody", "-v")
        assert result.exit_code == 1
        assert made_tokens == []                      # make_token was never called
        assert "zfsb1." not in result.stdout + result.stderr
        assert not tls_of(server).exists()
        assert stored_token_hash(server) is None      # and no token row was written

    def test_the_secret_is_not_in_the_database_only_its_hash(self, run, server):
        token = token_of(create(run, server))
        secret = token.rsplit(".", 1)[1]
        raw = server.read_bytes()
        assert secret.encode() not in raw and token.encode() not in raw
        assert hash_of(token) == stored_token_hash(server)

    def test_show_and_list_never_print_it(self, run, server, resolved, made_tokens):
        create(run, server)
        secret = made_tokens[0].rsplit(".", 1)[1]
        for args in (("trust", "token", "show"), ("trust", "token", "show", "--json"),
                     ("trust", "list"), ("trust", "list", "--json")):
            result = run(*args, "-c", server)
            assert secret not in result.stdout + result.stderr


# --------------------------------------------------------------------------
# trust token show
# --------------------------------------------------------------------------


def add_token(resolved, *, created, expires, server_fp=SERVER_FP, client_id=None, replace=False):
    _t, digest = identity.make_token(server_fp)
    create_token(resolved, secret_sha256=digest, server_fingerprint=server_fp,
                 created_at=created, expires_at=expires, client_id=client_id,
                 replace=replace, lock=None)
    return digest


class TestTokenShow:
    def test_none(self, run, server):
        result = run("trust", "token", "show", "-c", server)
        assert result.exit_code == 0
        assert result.stdout.startswith("state: none")
        assert "zfsbackup-config trust token create" in result.stdout

    def test_none_json(self, run, server):
        data = jout(run("trust", "token", "show", "--json", "-c", server))
        assert data == {
            "state": "none", "created_at": None, "expires_at": None,
            "server_fingerprint": None, "server_certificate_matches": None,
            "client_id": None, "replace": False, "consumed_at": None,
            "consumed_fingerprint": None, "consumed_client_id": None,
        }

    def test_active(self, run, server):
        server_identity(server)
        create(run, server)
        result = run("trust", "token", "show", "-c", server)
        assert result.exit_code == 0
        lines = dict(line.split(":", 1) for line in result.stdout.splitlines() if ":" in line)
        assert lines["state"].strip() == "active"
        assert re.fullmatch(r"[0-9a-f]{64}", lines["server fingerprint"].strip())
        assert "(in 59m" in result.stdout or "(in 1h" in result.stdout
        assert "usable by:          any new client id" in result.stdout
        assert "consumed" not in result.stdout

    def test_active_json(self, run, server):
        ident = server_identity(server)
        create(run, server)
        data = jout(run("trust", "token", "show", "--json", "-c", server))
        assert data["state"] == "active"
        assert data["server_fingerprint"] == ident.fingerprint
        assert data["server_certificate_matches"] is True
        assert ISO_RE.fullmatch(data["created_at"]) and ISO_RE.fullmatch(data["expires_at"])
        assert data["consumed_at"] is None and data["consumed_fingerprint"] is None

    def test_active_scoped_text(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        now = time.time()
        add_token(resolved, created=now - 5, expires=now + 600, client_id="c1")
        assert "usable by:          client id c1 (adding a certificate)" in run("trust", "token", "show", "-c", server).stdout
        add_token(resolved, created=now - 5, expires=now + 600, client_id="c1", replace=True)
        assert "usable by:          client id c1 (replacing its certificates)" in run("trust", "token", "show", "-c", server).stdout
        data = jout(run("trust", "token", "show", "--json", "-c", server))
        assert data["client_id"] == "c1" and data["replace"] is True

    def test_expired(self, run, server, resolved):
        now = time.time()
        add_token(resolved, created=now - 7200, expires=now - 3600)
        result = run("trust", "token", "show", "-c", server)
        assert result.exit_code == 0
        assert "state:              expired" in result.stdout
        assert "(1h ago)" in result.stdout

    def test_expired_json(self, run, server, resolved):
        now = time.time()
        add_token(resolved, created=now - 7200, expires=now - 3600)
        data = jout(run("trust", "token", "show", "--json", "-c", server))
        assert data["state"] == "expired"
        assert data["consumed_at"] is None
        assert data["server_fingerprint"] == SERVER_FP

    def test_a_token_retired_by_an_expired_redemption_attempt_is_expired_not_consumed(self, run, server, resolved):
        now = time.time()
        digest = add_token(resolved, created=now - 7200, expires=now - 3600)
        outcome = enroll_client(
            resolved, secret_sha256=digest, server_fingerprint=SERVER_FP, client_id="late",
            pin=real_pin("late"), now=now, dataset_exists=False,
        )
        assert outcome == EnrollOutcome.REFUSED
        data = jout(run("trust", "token", "show", "--json", "-c", server))
        assert data["state"] == "expired" and data["consumed_at"] is not None
        assert data["consumed_fingerprint"] is None

    def test_consumed(self, run, server, resolved):
        pin = real_pin("c1")
        enroll(resolved, "c1", pin)
        result = run("trust", "token", "show", "-c", server)
        assert result.exit_code == 0
        assert "state:              consumed" in result.stdout
        assert f"by fingerprint:   {pin.fingerprint}" in result.stdout
        assert "as client id:     c1" in result.stdout
        assert re.search(r"consumed:\s+\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC", result.stdout)

    def test_consumed_json(self, run, server, resolved):
        pin = real_pin("c1")
        enroll(resolved, "c1", pin)
        data = jout(run("trust", "token", "show", "--json", "-c", server))
        assert data["state"] == "consumed"
        assert data["consumed_fingerprint"] == pin.fingerprint
        assert data["consumed_client_id"] == "c1"
        assert ISO_RE.fullmatch(data["consumed_at"])
        assert data["server_certificate_matches"] is None

    def test_a_match_is_only_reported_for_an_active_token(self, run, server, resolved):
        ident = server_identity(server)
        now = time.time()
        add_token(resolved, created=now - 7200, expires=now - 3600, server_fp=ident.fingerprint)
        data = jout(run("trust", "token", "show", "--json", "-c", server))
        assert data["state"] == "expired"
        assert data["server_certificate_matches"] is None

    def test_a_replaced_server_certificate_is_reported_and_warned_about(self, run, server):
        old = server_identity(server)
        create(run, server)
        for name in ("server.crt", "server.key"):
            (tls_of(server) / name).unlink()
        new = server_identity(server)
        assert new.fingerprint != old.fingerprint
        result = run("trust", "token", "show", "--json", "-c", server)
        data = jout(result)
        assert data["server_certificate_matches"] is False
        assert data["server_fingerprint"] == old.fingerprint
        assert f"issued for server fingerprint {old.fingerprint}" in result.stderr
        assert f"server certificate is now {new.fingerprint}" in result.stderr
        assert "Create a new token." in result.stderr

    def test_the_mismatch_warning_in_text_mode_and_exit_zero(self, run, server):
        create(run, server)
        for name in ("server.crt", "server.key"):
            (tls_of(server) / name).unlink()
        server_identity(server)
        result = run("trust", "token", "show", "-c", server)
        assert result.exit_code == 0
        assert "warning: the token was issued for server fingerprint" in result.stderr
        assert "state:              active" in result.stdout

    def test_a_match_prints_no_warning(self, run, server):
        create(run, server)
        result = run("trust", "token", "show", "-c", server)
        assert result.stderr == ""

    def test_no_server_certificate_at_all_is_unknown(self, run, server, resolved):
        now = time.time()
        add_token(resolved, created=now - 5, expires=now + 600)
        result = run("trust", "token", "show", "--json", "-c", server)
        assert jout(result)["server_certificate_matches"] is None
        assert result.stderr == ""
        assert not tls_of(server).exists()  # a read-only command creates nothing

    def test_an_unreadable_server_certificate_is_unknown_not_an_error(self, run, server, resolved):
        now = time.time()
        add_token(resolved, created=now - 5, expires=now + 600)
        tls_of(server).mkdir(mode=0o750)
        (tls_of(server) / "server.crt").write_text("garbage")
        result = run("trust", "token", "show", "--json", "-c", server)
        assert jout(result)["server_certificate_matches"] is None

    def test_show_changes_nothing(self, run, server, resolved):
        create(run, server)
        before = (db_state(server), tree(server.parent), read_trust_key(resolved))
        run("trust", "token", "show", "-c", server)
        run("trust", "token", "show", "--json", "-c", server)
        assert (db_state(server), tree(server.parent), read_trust_key(resolved)) == before


# --------------------------------------------------------------------------
# trust token revoke
# --------------------------------------------------------------------------


class TestTokenRevoke:
    def test_with_a_token(self, run, server, resolved):
        create(run, server)
        result = run("trust", "token", "revoke", "-c", server)
        assert result.exit_code == 0
        assert "Revoked the enrollment token; it can no longer be used." in result.stdout
        assert token_status(resolved, time.time()).state == "none"
        assert stored_token_hash(server) is None

    def test_without_a_token(self, run, server):
        result = run("trust", "token", "revoke", "-c", server)
        assert result.exit_code == 0
        assert "There is no enrollment token; nothing changed." in result.stdout

    def test_twice(self, run, server):
        create(run, server)
        assert "Revoked" in run("trust", "token", "revoke", "-c", server).stdout
        assert "no enrollment token" in run("trust", "token", "revoke", "-c", server).stdout

    def test_a_consumed_token_is_removed_too(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        assert "Revoked the enrollment token" in run("trust", "token", "revoke", "-c", server).stdout
        assert token_status(resolved, time.time()).state == "none"

    def test_pins_are_left_alone(self, run, server, resolved):
        pin = real_pin("c1")
        enroll(resolved, "c1", pin)
        before = read_trust_key(resolved)
        run("trust", "token", "revoke", "-c", server)
        assert read_trust_key(resolved) == before
        assert fingerprints(list_clients(resolved)) == [pin.fingerprint]

    def test_a_revoked_token_cannot_be_redeemed(self, run, server, resolved):
        _t, digest = identity.make_token(SERVER_FP)
        now = time.time()
        create_token(resolved, secret_sha256=digest, server_fingerprint=SERVER_FP,
                     created_at=now - 1, expires_at=now + 600, lock=None)
        run("trust", "token", "revoke", "-c", server)
        outcome = enroll_client(
            resolved, secret_sha256=digest, server_fingerprint=SERVER_FP, client_id="c1",
            pin=real_pin("c1"), now=now, dataset_exists=False,
        )
        assert outcome == EnrollOutcome.REFUSED

    def test_revoke_does_not_generate_an_identity(self, run, server):
        run("trust", "token", "revoke", "-c", server)
        assert not tls_of(server).exists()


# --------------------------------------------------------------------------
# trust list
# --------------------------------------------------------------------------


def cells(line: str) -> list:
    return re.split(r"\s{2,}", line.strip())


class TestTrustList:
    def test_an_empty_store(self, run, server):
        result = run("trust", "list", "-c", server)
        assert result.exit_code == 0
        assert result.stdout == "(no clients)\n"
        assert result.stderr == ""

    def test_an_empty_store_json(self, run, server):
        result = run("trust", "list", "--json", "-c", server)
        assert result.exit_code == 0
        assert json.loads(result.stdout) == []

    def test_grouping_by_client_id(self, run, server, resolved):
        a1, a2, b1 = real_pin("a1"), real_pin("a2"), real_pin("b1")
        now = time.time()
        enroll(resolved, "alpha", a1, label="laptop", now=now - 300)
        enroll(resolved, "alpha", a2, scoped=True, now=now - 200)
        enroll(resolved, "beta", b1, now=now - 100)
        result = run("trust", "list", "-c", server)
        assert result.exit_code == 0
        lines = result.stdout.splitlines()
        assert cells(lines[0]) == [
            "CLIENT ID", "STATUS", "LABEL", "FINGERPRINT", "VIA", "ENROLLED", "NOT AFTER", "PREDECESSOR",
        ]
        rows = lines[1:]
        assert len(rows) == 3
        assert cells(rows[0])[:5] == ["alpha", "active", "laptop", a1.fingerprint[:16], "token"]
        # the second pin of the same client: no repeated id / status / label
        assert rows[1].startswith(" " * 8) or cells(rows[1])[0] == a2.fingerprint[:16]
        assert a2.fingerprint[:16] in rows[1] and "alpha" not in rows[1]
        assert cells(rows[2])[:3] == ["beta", "active", "-"]
        assert b1.fingerprint[:16] in rows[2]
        assert all(a.fingerprint[16:] not in result.stdout for a in (a1, a2, b1))

    def test_the_shortening_note_goes_to_stderr(self, run, server, resolved):
        enroll(resolved, "alpha", real_pin("a"))
        result = run("trust", "list", "-c", server)
        assert "fingerprints shortened to 16 characters" in result.stderr
        assert "fingerprints shortened" not in result.stdout

    def test_a_revoked_client(self, run, server, resolved):
        pin = real_pin("gone")
        enroll(resolved, "gone", pin)
        revoke_client(resolved, "gone", lock=None)
        result = run("trust", "list", "-c", server)
        main_table, _, revoked_table = result.stdout.partition("\nrevoked certificates")
        (row,) = main_table.splitlines()[1:]
        parts = cells(row)
        assert parts[0] == "gone"
        assert parts[1].startswith("revoked 20") and parts[1].endswith("UTC")
        assert parts[3:] == ["-", "-", "-", "-", "-"]  # no pin rows
        # ...but the pin it had is listed as revoked, for ever
        assert "(refused for ever; a client must enroll a new key):" in revoked_table
        header, revoked_row = revoked_table.splitlines()[1:3]
        assert header.split("  ")[0] == "FINGERPRINT" and "CLIENT ID" in header
        assert header.split()[-2:] == ["REVOKED", "REASON"]
        rparts = cells(revoked_row)
        assert rparts[0] == pin.fingerprint[:16] and rparts[1] == "gone"
        assert rparts[-1] == "client_revoked"
        assert pin.fingerprint[:16] not in main_table

    def test_a_revoked_pin_of_an_active_client_is_listed_in_the_revoked_table(self, run, server, resolved):
        keep, drop = real_pin("keep"), real_pin("drop")
        enroll(resolved, "c1", keep)
        enroll(resolved, "c1", drop, scoped=True)
        assert run("trust", "revoke", drop.fingerprint, "-c", server).exit_code == 0
        result = run("trust", "list", "-c", server)
        main_table, _, revoked_table = result.stdout.partition("\nrevoked certificates")
        assert keep.fingerprint[:16] in main_table and drop.fingerprint[:16] not in main_table
        assert drop.fingerprint[:16] in revoked_table and keep.fingerprint[:16] not in revoked_table
        (entry,) = list_json(run, server)
        assert [r["fingerprint"] for r in entry["revoked_pins"]] == [drop.fingerprint]
        assert entry["revoked_pins"][0]["reason"] == "revoked"
        assert ISO_RE.fullmatch(entry["revoked_pins"][0]["revoked_at"])

    def test_no_revoked_table_when_nothing_was_revoked(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        assert "revoked certificates" not in run("trust", "list", "-c", server).stdout
        assert list_json(run, server)[0]["revoked_pins"] == []

    def test_a_revoked_client_json(self, run, server, resolved):
        enroll(resolved, "gone", real_pin("gone"))
        revoke_client(resolved, "gone", lock=None)
        (entry,) = list_json(run, server)
        assert entry["client_id"] == "gone"
        assert entry["status"] == "revoked"
        assert ISO_RE.fullmatch(entry["revoked_at"])
        assert entry["pins"] == []

    def test_an_expired_pin(self, run, server, resolved):
        enroll(resolved, "old", fake_pin("c0ffee00", "1", not_after=time.time() - 3600))
        result = run("trust", "list", "-c", server)
        assert "(expired)" in result.stdout
        (entry,) = list_json(run, server)
        assert entry["pins"][0]["expired"] is True
        assert entry["status"] == "active"  # expiry does not revoke by itself

    def test_a_valid_pin_is_not_marked_expired(self, run, server, resolved):
        enroll(resolved, "ok", real_pin("ok"))
        assert "(expired)" not in run("trust", "list", "-c", server).stdout
        assert list_json(run, server)[0]["pins"][0]["expired"] is False

    def test_the_predecessor_column(self, run, server, resolved):
        old, new = real_pin("old"), real_pin("new")
        enroll(resolved, "c1", old)
        assert rotate_client(resolved, old_fp=old.fingerprint, new_pin=new, now=time.time() + 1) == "rotated"
        result = run("trust", "list", "-c", server)
        rows = result.stdout.splitlines()[1:]
        assert len(rows) == 2
        new_row = next(r for r in rows if new.fingerprint[:16] in r)
        old_row = next(r for r in rows if old.fingerprint[:16] in r)
        assert cells(new_row)[-1] == old.fingerprint[:16]
        assert " rotate " in new_row and " token " in old_row
        assert cells(old_row)[-1] == "-"

    def test_the_predecessor_in_json(self, run, server, resolved):
        old, new = real_pin("old"), real_pin("new")
        enroll(resolved, "c1", old)
        rotate_client(resolved, old_fp=old.fingerprint, new_pin=new, now=time.time() + 1)
        (entry,) = list_json(run, server)
        by_fp = {p["fingerprint"]: p for p in entry["pins"]}
        assert by_fp[new.fingerprint]["predecessor"] == old.fingerprint
        assert by_fp[new.fingerprint]["enrolled_via"] == "rotate"
        assert by_fp[old.fingerprint]["predecessor"] is None
        assert by_fp[old.fingerprint]["enrolled_via"] == "token"

    def test_json_has_full_fingerprints_and_the_documented_keys(self, run, server, resolved):
        pin = real_pin("c1")
        enroll(resolved, "c1", pin, label="web 1")
        result = run("trust", "list", "--json", "-c", server)
        assert result.stderr == ""
        (entry,) = json.loads(result.stdout)
        assert set(entry) == {"client_id", "label", "status", "created_at", "revoked_at", "pins",
                              "revoked_pins"}
        assert entry["revoked_pins"] == []
        assert entry["client_id"] == "c1" and entry["label"] == "web 1"
        assert entry["status"] == "active" and entry["revoked_at"] is None
        assert ISO_RE.fullmatch(entry["created_at"])
        (p,) = entry["pins"]
        assert set(p) == {"fingerprint", "enrolled_via", "enrolled_at", "not_after", "expired", "predecessor"}
        assert p["fingerprint"] == pin.fingerprint and len(p["fingerprint"]) == 64
        assert ISO_RE.fullmatch(p["enrolled_at"]) and ISO_RE.fullmatch(p["not_after"])

    def test_clients_are_ordered_by_id(self, run, server, resolved):
        for name in ("zeta", "alpha", "mid"):
            enroll(resolved, name, real_pin(name))
        assert [e["client_id"] for e in list_json(run, server)] == ["alpha", "mid", "zeta"]

    def test_a_client_that_lost_every_pin_but_is_not_revoked(self, run, server, resolved):
        pin = real_pin("c1")
        enroll(resolved, "c1", pin)
        revoke_pin(resolved, pin.fingerprint, lock=None)
        (entry,) = list_json(run, server)
        assert entry["status"] == "active" and entry["pins"] == []
        row = run("trust", "list", "-c", server).stdout.splitlines()[1]
        assert cells(row)[:2] == ["c1", "active"]

    def test_list_is_read_only(self, run, server, resolved):
        enroll(resolved, "c1", real_pin("c1"))
        before = (db_state(server), tree(server.parent), read_trust_key(resolved))
        run("trust", "list", "-c", server)
        run("trust", "list", "--json", "-c", server)
        assert (db_state(server), tree(server.parent), read_trust_key(resolved)) == before


# --------------------------------------------------------------------------
# trust revoke FINGERPRINT
# --------------------------------------------------------------------------


@pytest.fixture
def pins(server, resolved):
    """Three pins: two of `c1` sharing a 10-hex prefix, one of `c2`."""
    p1 = fake_pin("abcdef0123", "1")
    p2 = fake_pin("abcdef0123", "2")
    p3 = fake_pin("77aa88bb", "3")
    now = time.time()
    enroll(resolved, "c1", p1, now=now - 30)
    enroll(resolved, "c1", p2, scoped=True, now=now - 20)
    enroll(resolved, "c2", p3, now=now - 10)
    return p1, p2, p3


def revoke(run, database, *args):
    return run("trust", "revoke", *args, "-c", database)


class TestTrustRevokeFingerprint:
    def test_a_unique_prefix_echoes_the_full_fingerprint(self, run, server, resolved, pins):
        p1, p2, p3 = pins
        result = revoke(run, server, p3.fingerprint[:8])
        assert result.exit_code == 0, result.stderr
        assert f"Revoked certificate {p3.fingerprint} of client id c2." in result.stdout
        assert fingerprints(list_clients(resolved)) == [p1.fingerprint, p2.fingerprint]

    def test_the_full_fingerprint(self, run, server, resolved, pins):
        p1, p2, _ = pins
        result = revoke(run, server, p1.fingerprint)
        assert f"Revoked certificate {p1.fingerprint} of client id c1." in result.stdout
        assert p1.fingerprint not in fingerprints(list_clients(resolved))

    def test_the_prefix_is_normalised(self, run, server, resolved, pins):
        _, _, p3 = pins
        shown = p3.fingerprint[:12]
        colon = ":".join(shown[i:i + 2] for i in range(0, len(shown), 2)).upper()
        assert re.fullmatch(r"([0-9A-F]{2}:){5}[0-9A-F]{2}", colon)
        result = revoke(run, server, colon)
        assert result.exit_code == 0, result.stderr
        assert f"Revoked certificate {p3.fingerprint} " in result.stdout

    def test_an_uppercase_prefix(self, run, server, pins):
        p1, p2, p3 = pins
        result = revoke(run, server, p3.fingerprint[:20].upper())
        assert result.exit_code == 0, result.stderr
        assert p3.fingerprint in result.stdout

    def test_a_full_colon_separated_uppercase_fingerprint(self, run, server, resolved, pins):
        _, _, p3 = pins
        colon = ":".join(p3.fingerprint[i:i + 2] for i in range(0, 64, 2)).upper()
        assert revoke(run, server, colon).exit_code == 0
        assert p3.fingerprint not in fingerprints(list_clients(resolved))

    def test_surrounding_whitespace_is_ignored(self, run, server, pins):
        assert revoke(run, server, f"  {pins[2].fingerprint[:10]}  ").exit_code == 0

    @pytest.mark.parametrize("bad", ["", "abc", "1234567", "a" * 7, "zzzzzzzzzz", "ab:cd:ef:0", "12 345678", "g" * 8])
    def test_a_bad_prefix_is_exit_1_and_changes_nothing(self, run, server, resolved, pins, bad):
        key = read_trust_key(resolved)
        result = revoke(run, server, bad)
        assert result.exit_code == 1
        assert "is not a certificate fingerprint" in result.stderr
        assert "at least 8 and at most 64 hexadecimal characters" in result.stderr
        assert read_trust_key(resolved) == key
        assert len(fingerprints(list_clients(resolved))) == 3

    def test_too_long_is_refused(self, run, server, resolved, pins):
        result = revoke(run, server, pins[2].fingerprint + "0")
        assert result.exit_code == 1
        assert "is not a certificate fingerprint" in result.stderr

    def test_eight_characters_is_the_minimum(self, run, server, pins):
        assert revoke(run, server, pins[2].fingerprint[:7]).exit_code == 1
        assert revoke(run, server, pins[2].fingerprint[:8]).exit_code == 0

    def test_no_match(self, run, server, resolved, pins):
        key = read_trust_key(resolved)
        result = revoke(run, server, "deadbeef")
        assert result.exit_code == 1
        assert "no pinned certificate has a fingerprint starting with deadbeef" in result.stderr
        assert "trust list" in result.stderr
        assert read_trust_key(resolved) == key

    def test_no_match_in_an_empty_store(self, run, server):
        assert revoke(run, server, "deadbeef").exit_code == 1

    def test_an_ambiguous_prefix_lists_every_candidate(self, run, server, resolved, pins):
        p1, p2, _ = pins
        key = read_trust_key(resolved)
        result = revoke(run, server, "abcdef01")
        assert result.exit_code == 1
        assert "matches 2 pinned certificates; give more characters" in result.stderr
        assert f"{p1.fingerprint}  (client id c1)" in result.stderr
        assert f"{p2.fingerprint}  (client id c1)" in result.stderr
        assert read_trust_key(resolved) == key
        assert len(fingerprints(list_clients(resolved))) == 3

    def test_an_ambiguous_prefix_across_clients_names_both_ids(self, run, server, resolved):
        enroll(resolved, "left", fake_pin("11223344", "a"))
        enroll(resolved, "right", fake_pin("11223344", "b"))
        result = revoke(run, server, "11223344")
        assert result.exit_code == 1
        assert "(client id left)" in result.stderr and "(client id right)" in result.stderr

    def test_a_longer_prefix_resolves_the_ambiguity(self, run, server, resolved, pins):
        p1, p2, _ = pins
        result = revoke(run, server, p2.fingerprint[:11])
        assert result.exit_code == 0, result.stderr
        assert f"Revoked certificate {p2.fingerprint} " in result.stdout
        assert fingerprints(list_clients(resolved)) == [p1.fingerprint, pins[2].fingerprint]

    def test_a_revoke_bumps_the_trust_generation_once(self, run, server, resolved, pins):
        before = read_trust_key(resolved)
        revoke(run, server, pins[2].fingerprint[:8])
        after = read_trust_key(resolved)
        assert (after.store_id, after.generation) == (before.store_id, before.generation + 1)

    def test_it_does_not_bump_the_config_generation(self, run, server, pins):
        gen = generation(server)
        revoke(run, server, pins[2].fingerprint[:8])
        assert generation(server) == gen

    def test_the_client_stays_known_and_active(self, run, server, resolved, pins):
        revoke(run, server, pins[2].fingerprint[:8])
        c2 = next(e for e in list_json(run, server) if e["client_id"] == "c2")
        assert c2["status"] == "active" and c2["pins"] == []

    # ---- notes ----

    def test_the_last_pin_of_a_client_adds_a_note(self, run, server, pins):
        result = revoke(run, server, pins[2].fingerprint[:8])
        assert "client id c2 has no other pinned certificate, so it can no longer connect" in result.stdout
        assert "`trust revoke --client` also retires the id" in result.stdout

    def test_a_client_with_another_pin_gets_no_such_note(self, run, server, pins):
        result = revoke(run, server, pins[0].fingerprint)
        assert result.exit_code == 0
        assert "no other pinned certificate" not in result.stdout

    def test_the_closing_note_names_the_target_dataset(self, run, server, pins):
        result = revoke(run, server, pins[2].fingerprint[:8])
        assert "New connections are refused and streams already in flight are cut within ~2 s" in result.stdout
        assert "data under tank/backups/c2 is left in place." in result.stdout

    def test_the_closing_note_with_remote_backup_disabled_still_names_it(self, run, db):
        make_db(run, db, REMOTE_OFF)
        resolved = resolve_config_path(str(db))
        enroll(resolved, "c2", fake_pin("77aa88bb", "3"))
        assert "data under tank/backups/c2 is left in place." in revoke(run, db, "77aa88bb").stdout

    def test_the_closing_note_without_remote_backup_uses_a_placeholder(self, run, db):
        make_db(run, db, PLAIN_YAML)
        resolved = resolve_config_path(str(db))
        enroll(resolved, "c2", fake_pin("77aa88bb", "3"))
        result = revoke(run, db, "77aa88bb")
        assert result.exit_code == 0
        assert "data under <target>/c2 is left in place." in result.stdout

    def test_an_unreadable_configuration_never_blocks_the_revoke(self, run, server, resolved, pins, monkeypatch):
        def unreadable(_resolved):
            raise RuntimeError("cannot load")

        monkeypatch.setattr(trust_cli, "read_snapshot", unreadable)
        result = revoke(run, server, pins[2].fingerprint[:8])
        assert result.exit_code == 0, result.stderr
        assert "data under <target>/c2 is left in place." in result.stdout
        assert pins[2].fingerprint not in fingerprints(list_clients(resolved))

    def test_revoking_a_predecessor_cascades_to_its_successor(self, run, server, resolved):
        """The store revokes the key the holder authorised with the one that was revoked, so a
        thief who rotated before the operator reacted does not keep the client's access."""
        old, new = real_pin("old"), real_pin("new")
        enroll(resolved, "c1", old)
        rotate_client(resolved, old_fp=old.fingerprint, new_pin=new, now=time.time() + 1)
        assert revoke(run, server, old.fingerprint).exit_code == 0
        (entry,) = list_json(run, server)
        assert entry["pins"] == []
        reasons = {r["fingerprint"]: r["reason"] for r in entry["revoked_pins"]}
        assert reasons == {old.fingerprint: "revoked", new.fingerprint: "successor_revoked"}

    def test_the_revoke_output_reports_the_cascade(self, run, server, resolved):
        old, new = real_pin("old"), real_pin("new")
        enroll(resolved, "c1", old)
        rotate_client(resolved, old_fp=old.fingerprint, new_pin=new, now=time.time() + 1)
        result = revoke(run, server, old.fingerprint)
        assert result.exit_code == 0, result.stderr
        assert (f"also revoked {new.fingerprint} (client id c1; registered by the revoked "
                "certificate via /rotate)") in result.stdout
        # the successor does not count as "another pinned certificate": the client is locked out
        assert "has no other pinned certificate" in result.stdout

    def test_the_list_explains_a_cascaded_reason_and_json_keeps_the_raw_one(self, run, server, resolved):
        old, new = real_pin("old"), real_pin("new")
        enroll(resolved, "c1", old)
        rotate_client(resolved, old_fp=old.fingerprint, new_pin=new, now=time.time() + 1)
        assert revoke(run, server, old.fingerprint).exit_code == 0
        text = run("trust", "list", "-c", server).stdout
        assert "successor_revoked (registered by a revoked certificate via /rotate)" in text
        (entry,) = list_json(run, server)
        assert {r["reason"] for r in entry["revoked_pins"]} == {"revoked", "successor_revoked"}

    def test_revoking_a_successor_does_not_cascade_to_its_predecessor_or_say_so(self, run, server, resolved):
        old, new = real_pin("old"), real_pin("new")
        enroll(resolved, "c1", old)
        rotate_client(resolved, old_fp=old.fingerprint, new_pin=new, now=time.time() + 1)
        result = revoke(run, server, new.fingerprint)
        assert "also revoked" not in result.stdout
        (entry,) = list_json(run, server)
        assert [p["fingerprint"] for p in entry["pins"]] == [old.fingerprint]

    def test_revoking_an_already_revoked_fingerprint_is_a_no_op(self, run, server, resolved):
        pin = real_pin("again")
        enroll(resolved, "c1", pin)
        assert run("trust", "revoke", pin.fingerprint, "-c", server).exit_code == 0
        state = db_state(server)
        result = run("trust", "revoke", pin.fingerprint[:12], "-c", server)
        assert result.exit_code == 0, result.stderr
        assert f"certificate {pin.fingerprint} (client id c1) is already revoked" in result.stdout
        assert "nothing changed" in result.stdout
        assert db_state(server) == state

    def test_the_revoke_note_says_a_revoked_certificate_needs_a_new_key(self, run, server, resolved):
        pin = real_pin("note")
        enroll(resolved, "c1", pin)
        result = run("trust", "revoke", pin.fingerprint, "-c", server)
        assert "A revoked certificate can never be enrolled again" in result.stdout
        assert "enroll NAME --new-key" in result.stdout


# --------------------------------------------------------------------------
# trust revoke / trust list: a certificate a rotation already retired
# --------------------------------------------------------------------------


def rotate_to(resolved, old, new, step: int = 1) -> None:
    """`/rotate` with `old` registering `new` (pending: `old` stays pinned)."""
    outcome = rotate_client(resolved, old_fp=old.fingerprint, new_pin=new, now=time.time() + step)
    assert outcome == "rotated", outcome


def first_request(resolved, pin) -> None:
    """The successor's first authenticated request: it retires its predecessor."""
    assert retire_predecessor(resolved, pin.fingerprint) is True


def revoked_rows(run, database):
    (entry,) = list_json(run, database)
    return {r["fingerprint"]: r for r in entry["revoked_pins"]}


class TestTrustRevokeRetired:
    """The thief rotates A -> C and uses C once, which retires A. `trust revoke A` must still
    take C away, and the output must say what happened rather than "already revoked"."""

    @pytest.fixture
    def chain1(self, resolved):
        """A retired by C; C live and alone for `c1`."""
        a, c = real_pin("a"), real_pin("c")
        enroll(resolved, "c1", a)
        rotate_to(resolved, a, c)
        first_request(resolved, c)
        assert fingerprints(list_clients(resolved)) == [c.fingerprint]
        return a, c

    def test_a_chain_of_one_revokes_the_live_successor_and_says_so(self, run, server, resolved, chain1):
        a, c = chain1
        result = revoke(run, server, a.fingerprint[:12])
        assert result.exit_code == 0, result.stderr
        out = result.stdout
        assert (f"certificate {a.fingerprint} (client id c1) was already retired by its "
                f"successor {c.fingerprint}; revoked {c.fingerprint}.") in out
        assert "and its successors" not in out
        assert "already revoked" not in out and "nothing changed" not in out
        assert fingerprints(list_clients(resolved)) == []

    def test_the_output_order_is_the_chain_the_lockout_line_and_the_note(self, run, server, resolved, chain1):
        a, c = chain1
        out = revoke(run, server, a.fingerprint).stdout
        chain = out.index("was already retired by its successor")
        lockout = out.index("client id c1 has no other pinned certificate, so it can no longer connect")
        note = out.index("New connections are refused and streams already in flight are cut within ~2 s")
        assert chain < lockout < note
        assert "(the id itself stays known; `trust revoke --client` also retires the id)" in out
        assert "data under tank/backups/c1 is left in place." in out

    def test_the_store_state_after_it(self, run, server, resolved, chain1):
        a, c = chain1
        before = revoked_rows(run, server)[a.fingerprint]
        key = read_trust_key(resolved)
        assert revoke(run, server, a.fingerprint).exit_code == 0
        rows = revoked_rows(run, server)
        assert rows[a.fingerprint] == before                  # A's own record is left as it was
        assert rows[a.fingerprint]["reason"] == "retired"
        assert rows[a.fingerprint]["superseded_by"] == c.fingerprint
        assert rows[c.fingerprint]["reason"] == "successor_revoked"
        assert "superseded_by" not in rows[c.fingerprint]
        assert read_trust_key(resolved) != key                # the trust generation moved

    def test_no_lockout_line_when_the_client_keeps_another_pin(self, run, server, resolved, chain1):
        a, c = chain1
        keep = real_pin("keep")
        enroll(resolved, "c1", keep, scoped=True)
        out = revoke(run, server, a.fingerprint).stdout
        assert f"revoked {c.fingerprint}." in out
        assert "has no other pinned certificate" not in out
        assert fingerprints(list_clients(resolved)) == [keep.fingerprint]

    def test_a_live_successor_with_its_own_pending_successor(self, run, server, resolved):
        """A -> C (A retired), then C -> D registered and not yet used: C and D are both live."""
        a, c, d = real_pin("a"), real_pin("c"), real_pin("d")
        enroll(resolved, "c1", a)
        rotate_to(resolved, a, c, 1)
        first_request(resolved, c)
        rotate_to(resolved, c, d, 2)
        out = revoke(run, server, a.fingerprint).stdout
        assert (f"was already retired by its successor {c.fingerprint}; revoked {c.fingerprint} "
                f"(and its successors {d.fingerprint}).") in out
        assert fingerprints(list_clients(resolved)) == []
        rows = revoked_rows(run, server)
        assert rows[c.fingerprint]["reason"] == rows[d.fingerprint]["reason"] == "successor_revoked"

    def test_the_named_successor_is_reported_first_whatever_the_enrollment_order(self, run, server, resolved):
        """The store lists the cascade by enrollment time; a pending successor stamped EARLIER
        than the live C (a clock that stepped back) must not be reported as the direct one."""
        a, c, d = real_pin("a"), real_pin("c"), real_pin("d")
        enroll(resolved, "c1", a)
        rotate_to(resolved, a, c, 50)
        first_request(resolved, c)
        rotate_to(resolved, c, d, 10)                                   # D enrolled before C
        pins = {p.fingerprint: p.enrolled_at for p in list_clients(resolved)[0].pins}
        assert pins[d.fingerprint] < pins[c.fingerprint]
        out = revoke(run, server, a.fingerprint).stdout
        assert (f"was already retired by its successor {c.fingerprint}; revoked {c.fingerprint} "
                f"(and its successors {d.fingerprint}).") in out

    def test_a_chain_through_a_retired_intermediate_lists_every_live_successor(self, run, server, resolved):
        """A -> C -> D -> E: A is retired by C, C by D; D is live with E pending. The named
        successor C is gone, so the first live key is the one reported as revoked."""
        a, c, d, e = real_pin("a"), real_pin("c"), real_pin("d"), real_pin("e")
        enroll(resolved, "c1", a)
        rotate_to(resolved, a, c, 1)
        first_request(resolved, c)
        rotate_to(resolved, c, d, 2)
        first_request(resolved, d)
        rotate_to(resolved, d, e, 3)
        assert fingerprints(list_clients(resolved)) == [d.fingerprint, e.fingerprint]
        out = revoke(run, server, a.fingerprint).stdout
        assert (f"certificate {a.fingerprint} (client id c1) was already retired by its "
                f"successor {c.fingerprint}; revoked {d.fingerprint} "
                f"(and its successors {e.fingerprint}).") in out
        assert "has no other pinned certificate" in out
        assert fingerprints(list_clients(resolved)) == []

    def test_a_prefix_of_the_retired_fingerprint_resolves_it(
        self, run, server, resolved, chain1
    ):
        """A prefix of the RETIRED fingerprint resolves it (the pinned successor does not match)."""
        a, c = chain1
        assert not c.fingerprint.startswith(a.fingerprint[:8])
        result = revoke(run, server, a.fingerprint[:8])
        assert result.exit_code == 0 and "was already retired by its successor" in result.stdout

    def test_the_same_command_twice_the_second_finds_nothing_live(self, run, server, resolved, chain1):
        a, c = chain1
        assert revoke(run, server, a.fingerprint).exit_code == 0
        state = db_state(server)
        again = revoke(run, server, a.fingerprint)
        assert again.exit_code == 0, again.stderr
        assert (f"certificate {a.fingerprint} (client id c1) is already revoked (retired (replaced "
                f"by {c.fingerprint[:16]}), ") in again.stdout
        assert "; nothing that superseded it is still pinned; nothing changed." in again.stdout
        assert db_state(server) == state

    def test_nothing_live_left_changes_nothing(self, run, server, resolved, chain1):
        """C was revoked on its own, so nothing that superseded A is pinned any more."""
        a, c = chain1
        assert revoke(run, server, c.fingerprint).exit_code == 0
        state = db_state(server)
        key = read_trust_key(resolved)
        result = revoke(run, server, a.fingerprint)
        assert result.exit_code == 0, result.stderr
        assert re.search(
            rf"certificate {a.fingerprint} \(client id c1\) is already revoked "
            rf"\(retired \(replaced by {c.fingerprint[:16]}\), \d{{4}}-\d\d-\d\d[^)]*\); "
            r"nothing that superseded it is still pinned; nothing changed\.",
            result.stdout,
        ), result.stdout
        assert "nothing that superseded it is still pinned; nothing changed." in result.stdout
        assert "was already retired" not in result.stdout
        assert "has no other pinned certificate" not in result.stdout
        assert "New connections are refused" not in result.stdout     # no revoke note: nothing revoked
        assert db_state(server) == state
        assert read_trust_key(resolved) == key

    def test_nothing_live_left_after_a_client_revoke(self, run, server, resolved, chain1):
        a, c = chain1
        assert run("trust", "revoke", "--client", "c1", "-c", server).exit_code == 0
        state = db_state(server)
        result = revoke(run, server, a.fingerprint[:10])
        assert result.exit_code == 0, result.stderr
        assert "nothing that superseded it is still pinned; nothing changed." in result.stdout
        assert db_state(server) == state

    def test_a_record_that_is_not_retired_keeps_the_plain_wording(self, run, server, resolved):
        pin = real_pin("plain")
        enroll(resolved, "c1", pin)
        assert revoke(run, server, pin.fingerprint).exit_code == 0
        out = revoke(run, server, pin.fingerprint).stdout
        assert "is already revoked (revoked, " in out and out.rstrip().endswith("; nothing changed.")
        assert "retired" not in out and "superseded" not in out

    def test_a_successor_revoked_record_keeps_the_plain_wording(self, run, server, resolved):
        old, new = real_pin("old"), real_pin("new")
        enroll(resolved, "c1", old)
        rotate_to(resolved, old, new)
        assert revoke(run, server, old.fingerprint).exit_code == 0     # new: successor_revoked
        state = db_state(server)
        out = revoke(run, server, new.fingerprint).stdout
        assert (f"certificate {new.fingerprint} (client id c1) is already revoked "
                "(successor_revoked (registered by a revoked certificate via /rotate), ") in out
        assert "superseded it" not in out and out.rstrip().endswith("; nothing changed.")
        assert db_state(server) == state

    def test_a_retired_record_without_a_recorded_successor_keeps_the_plain_wording(
        self, run, server, resolved, chain1
    ):
        """An old row: `retired` with `superseded_by` NULL (nothing to follow)."""
        a, c = chain1
        conn = sqlite3.connect(server)
        try:
            conn.execute("UPDATE revoked_fingerprints SET superseded_by = NULL WHERE fingerprint = ?",
                         (a.fingerprint,))
            conn.commit()
        finally:
            conn.close()
        state = db_state(server)
        result = revoke(run, server, a.fingerprint)
        assert result.exit_code == 0, result.stderr
        assert f"certificate {a.fingerprint} (client id c1) is already revoked (retired, " in result.stdout
        assert "superseded it" not in result.stdout and "replaced by" not in result.stdout
        assert result.stdout.rstrip().endswith("; nothing changed.")
        assert db_state(server) == state
        assert fingerprints(list_clients(resolved)) == [c.fingerprint]

    def test_it_does_not_touch_other_clients(self, run, server, resolved, chain1):
        a, c = chain1
        other = real_pin("other")
        enroll(resolved, "c2", other)
        assert revoke(run, server, a.fingerprint).exit_code == 0
        assert fingerprints(list_clients(resolved)) == [other.fingerprint]


class TestTrustRevokeAmbiguousRevoked:
    @pytest.fixture
    def two_revoked(self, run, server, resolved):
        p1, p2 = fake_pin("5a5a5a5a5a", "1"), fake_pin("5a5a5a5a5a", "2")
        now = time.time()
        enroll(resolved, "c1", p1, now=now - 30)
        enroll(resolved, "c2", p2, now=now - 20)
        assert revoke(run, server, p1.fingerprint).exit_code == 0
        assert revoke(run, server, p2.fingerprint).exit_code == 0
        return p1, p2

    def test_a_prefix_across_revoked_records_lists_them_and_changes_nothing(
        self, run, server, resolved, two_revoked
    ):
        p1, p2 = two_revoked
        state = db_state(server)
        result = revoke(run, server, "5a5a5a5a")
        assert result.exit_code == 1
        assert "'5a5a5a5a' matches 2 revoked certificates; give more characters:" in result.stderr
        assert f"{p1.fingerprint}  (client id c1, revoked)" in result.stderr
        assert f"{p2.fingerprint}  (client id c2, revoked)" in result.stderr
        assert "already revoked" not in result.stdout
        assert db_state(server) == state

    def test_a_longer_prefix_resolves_it(self, run, server, resolved, two_revoked):
        p1, p2 = two_revoked
        result = revoke(run, server, p2.fingerprint[:12])
        assert result.exit_code == 0, result.stderr
        assert f"certificate {p2.fingerprint} (client id c2) is already revoked" in result.stdout

    def test_two_revoked_records_of_one_retired_chain_are_still_ambiguous(self, run, server, resolved):
        """The check counts distinct fingerprints, whatever the reason."""
        a, c = fake_pin("77ab77ab77", "1"), fake_pin("77ab77ab77", "2")
        enroll(resolved, "c1", a)
        rotate_to(resolved, a, c)
        first_request(resolved, c)
        assert revoke(run, server, c.fingerprint).exit_code == 0       # a: retired, c: revoked
        result = revoke(run, server, "77ab77ab")
        assert result.exit_code == 1
        assert "matches 2 revoked certificates" in result.stderr
        assert f"{a.fingerprint}  (client id c1, revoked)" in result.stderr

    def test_a_pinned_match_wins_over_a_revoked_one(self, run, server, resolved):
        gone, live = fake_pin("3c3c3c3c3c", "1"), fake_pin("3c3c3c3c3c", "2")
        enroll(resolved, "c1", gone)
        assert revoke(run, server, gone.fingerprint).exit_code == 0
        enroll(resolved, "c2", live)
        result = revoke(run, server, "3c3c3c3c")
        assert result.exit_code == 0, result.stderr
        assert f"Revoked certificate {live.fingerprint} of client id c2." in result.stdout


class TestTrustListRetired:
    @pytest.fixture
    def chain(self, resolved):
        a, c = real_pin("a"), real_pin("c")
        enroll(resolved, "c1", a)
        rotate_to(resolved, a, c)
        first_request(resolved, c)
        return a, c

    def test_text_shows_the_successor(self, run, server, chain):
        a, c = chain
        out = run("trust", "list", "-c", server).stdout
        _, _, revoked_table = out.partition("\nrevoked certificates")
        row = next(r for r in revoked_table.splitlines() if r.startswith(a.fingerprint[:16]))
        assert cells(row)[-1] == f"retired (replaced by {c.fingerprint[:16]})"
        assert cells(row)[1] == "c1"

    def test_json_carries_superseded_by_only_for_a_retired_record(self, run, server, resolved, chain):
        a, c = chain
        extra_old, extra_new = real_pin("x"), real_pin("y")
        enroll(resolved, "c2", extra_old)
        rotate_to(resolved, extra_old, extra_new, 2)
        assert revoke(run, server, extra_old.fingerprint).exit_code == 0        # revoked + successor_revoked
        entries = {e["client_id"]: e for e in list_json(run, server)}
        (retired,) = entries["c1"]["revoked_pins"]
        assert retired["fingerprint"] == a.fingerprint and retired["reason"] == "retired"
        assert retired["superseded_by"] == c.fingerprint and len(retired["superseded_by"]) == 64
        assert set(retired) == {"fingerprint", "revoked_at", "reason", "superseded_by"}
        by_reason = {r["reason"]: r for r in entries["c2"]["revoked_pins"]}
        assert set(by_reason) == {"revoked", "successor_revoked"}
        for plain in by_reason.values():
            assert set(plain) == {"fingerprint", "revoked_at", "reason"}

    def test_a_retired_record_without_a_successor_has_no_key_and_plain_text(
        self, run, server, resolved, chain
    ):
        a, c = chain
        conn = sqlite3.connect(server)
        try:
            conn.execute("UPDATE revoked_fingerprints SET superseded_by = NULL WHERE fingerprint = ?",
                         (a.fingerprint,))
            conn.commit()
        finally:
            conn.close()
        (entry,) = list_json(run, server)
        assert "superseded_by" not in entry["revoked_pins"][0]
        out = run("trust", "list", "-c", server).stdout
        assert "retired (replaced by" not in out
        row = next(r for r in out.splitlines() if r.startswith(a.fingerprint[:16]))
        assert cells(row)[-1] == "retired"

    def test_list_is_still_read_only(self, run, server, chain):
        state = db_state(server)
        run("trust", "list", "-c", server)
        run("trust", "list", "--json", "-c", server)
        assert db_state(server) == state


# --------------------------------------------------------------------------
# trust revoke: arguments
# --------------------------------------------------------------------------


class TestTrustRevokeArguments:
    def test_neither_is_a_usage_error(self, run, server):
        result = revoke(run, server)
        assert result.exit_code == 2
        assert "give exactly one of FINGERPRINT or --client ID" in result.stderr

    def test_both_is_a_usage_error_and_changes_nothing(self, run, server, resolved, pins):
        key = read_trust_key(resolved)
        result = revoke(run, server, pins[2].fingerprint, "--client", "c2")
        assert result.exit_code == 2
        assert "give exactly one of FINGERPRINT or --client ID" in result.stderr
        assert read_trust_key(resolved) == key
        assert len(fingerprints(list_clients(resolved))) == 3

    def test_two_fingerprints_is_a_usage_error(self, run, server, pins):
        assert revoke(run, server, pins[0].fingerprint, pins[1].fingerprint).exit_code == 2

    def test_a_usage_error_does_not_open_the_database(self, run, db):
        before = tree(db.parent) if db.parent.exists() else None
        assert revoke(run, db).exit_code == 2
        assert (tree(db.parent) if db.parent.exists() else None) == before


# --------------------------------------------------------------------------
# trust revoke --client
# --------------------------------------------------------------------------


class TestTrustRevokeClient:
    def test_success_removes_every_pin_and_retires_the_id(self, run, server, resolved, pins):
        p1, p2, p3 = pins
        result = revoke(run, server, "--client", "c1")
        assert result.exit_code == 0, result.stderr
        assert "Revoked client id c1: 2 pinned certificate(s) removed" in result.stdout
        assert f"  {p1.fingerprint}" in result.stdout and f"  {p2.fingerprint}" in result.stdout
        assert "data under tank/backups/c1 is left in place." in result.stdout
        assert fingerprints(list_clients(resolved)) == [p3.fingerprint]
        c1 = next(e for e in list_json(run, server) if e["client_id"] == "c1")
        assert c1["status"] == "revoked" and c1["pins"] == []
        assert ISO_RE.fullmatch(c1["revoked_at"])

    def test_the_trust_generation_moves_once(self, run, server, resolved, pins):
        before = read_trust_key(resolved)
        revoke(run, server, "--client", "c1")
        assert read_trust_key(resolved).generation == before.generation + 1

    def test_the_config_generation_does_not_move(self, run, server, pins):
        gen = generation(server)
        revoke(run, server, "--client", "c1")
        assert generation(server) == gen

    def test_an_unknown_id_lists_the_known_ones(self, run, server, resolved, pins):
        key = read_trust_key(resolved)
        result = revoke(run, server, "--client", "nobody")
        assert result.exit_code == 1
        assert "unknown client id 'nobody'; known client ids: c1, c2" in result.stderr
        assert read_trust_key(resolved) == key

    def test_an_unknown_id_in_an_empty_store(self, run, server):
        result = revoke(run, server, "--client", "nobody")
        assert result.exit_code == 1
        assert "known client ids: (none)" in result.stderr

    def test_known_ids_include_revoked_ones(self, run, server, resolved, pins):
        revoke(run, server, "--client", "c1")
        result = revoke(run, server, "--client", "nobody")
        assert "known client ids: c1, c2" in result.stderr

    def test_an_already_revoked_id_is_exit_0_and_writes_nothing(self, run, server, resolved, pins):
        revoke(run, server, "--client", "c1")
        before = (read_trust_key(resolved), list_json(run, server), db_state(server))
        result = revoke(run, server, "--client", "c1")
        assert result.exit_code == 0
        assert "client id c1 is already revoked; nothing changed." in result.stdout
        assert "left in place" not in result.stdout
        assert (read_trust_key(resolved), list_json(run, server), db_state(server)) == before

    def test_an_id_with_no_pins_that_is_not_revoked_is_retired(self, run, server, resolved):
        pin = real_pin("c1")
        enroll(resolved, "c1", pin)
        revoke_pin(resolved, pin.fingerprint, lock=None)
        before = read_trust_key(resolved)
        result = revoke(run, server, "--client", "c1")
        assert result.exit_code == 0
        assert "0 pinned certificate(s) removed" in result.stdout
        assert read_trust_key(resolved) == before  # no pin changed
        assert list_json(run, server)[0]["status"] == "revoked"

    def test_a_revoked_id_can_be_enrolled_again_with_a_scoped_token(self, run, server, resolved, pins):
        revoke(run, server, "--client", "c1")
        assert create(run, server, "--client-id", "c1").exit_code == 0
        enroll(resolved, "c1", real_pin("again"), scoped=True)
        c1 = next(e for e in list_json(run, server) if e["client_id"] == "c1")
        assert c1["status"] == "active" and len(c1["pins"]) == 1

    def test_the_dotted_and_dashed_ids_are_exact_matches(self, run, server, resolved):
        enroll(resolved, "web.1-a", fake_pin("aa00bb11", "1"))
        enroll(resolved, "web", fake_pin("cc00dd22", "2"))
        assert revoke(run, server, "--client", "web.1-a").exit_code == 0
        assert [e["status"] for e in list_json(run, server)] == ["active", "revoked"]


# --------------------------------------------------------------------------
# No trust command signals the daemon
# --------------------------------------------------------------------------


class TestNoDaemonSignal:
    def test_every_trust_command(self, run, server, resolved, no_reload, pins):
        p1, p2, p3 = pins
        commands = [
            ("trust", "token", "create"),
            ("trust", "token", "create", "--json"),
            ("trust", "token", "create", "--client-id", "c1", "--replace"),
            ("trust", "token", "show"),
            ("trust", "token", "show", "--json"),
            ("trust", "token", "revoke"),
            ("trust", "list"),
            ("trust", "list", "--json"),
            ("trust", "revoke", p3.fingerprint[:8]),
            ("trust", "revoke", "--client", "c1"),
            ("trust", "revoke", "--client", "c1"),   # already revoked
            ("trust", "revoke", "--client", "nobody"),
        ]
        for command in commands:
            run(*command, "-c", server)
        assert no_reload == []

    def test_a_trust_write_prints_no_daemon_notice(self, run, server, pins):
        result = revoke(run, server, "--client", "c1")
        assert "config updated" not in result.stdout
        assert "daemon" not in result.stdout + result.stderr

    def test_a_live_daemon_is_left_alone(self, run, server, pins, fake_daemon, monkeypatch):
        """With a (fake) daemon holding the lock the commands still never ask it
        anything, and never wait for an answer."""
        def tripwire(*a, **k):
            raise AssertionError("a trust command touched the daemon")

        monkeypatch.setattr(daemonlock, "request_reload", tripwire)
        monkeypatch.setattr(daemonlock, "probe", tripwire)
        fake_daemon(server)
        for command in (("trust", "token", "create"), ("trust", "list"),
                        ("trust", "revoke", "--client", "c1"), ("trust", "token", "revoke")):
            assert run(*command, "-c", server).exit_code == 0


# --------------------------------------------------------------------------
# A missing database
# --------------------------------------------------------------------------


class TestMissingDatabase:
    COMMANDS = [
        ("trust", "token", "create"),
        ("trust", "token", "create", "--json"),
        ("trust", "token", "show"),
        ("trust", "token", "revoke"),
        ("trust", "list"),
        ("trust", "revoke", "deadbeef"),
        ("trust", "revoke", "--client", "x"),
    ]

    @pytest.mark.parametrize("command", COMMANDS, ids=[" ".join(c[1:]) for c in COMMANDS])
    def test_exit_78_and_nothing_is_created(self, run, tmp_path, command):
        before = tree(tmp_path)
        result = run(*command, "-c", tmp_path / "nowhere" / "config.db")
        assert result.exit_code == 78, result.stderr
        assert tree(tmp_path) == before
        assert not any(".tls" in line or "lock" in line for line in tree(tmp_path))

    def test_token_create_in_an_existing_directory_creates_no_tls_dir_or_lock_file(self, run, tmp_path):
        directory = tmp_path / "here"
        directory.mkdir()
        result = create(run, directory / "config.db")
        assert result.exit_code == 78
        assert os.listdir(directory) == []
        assert TOKEN_RE.search(result.stdout + result.stderr) is None

    def test_a_zero_byte_file_is_not_a_database(self, run, tmp_path):
        empty = tmp_path / "config.db"
        empty.write_bytes(b"")
        result = create(run, empty)
        assert result.exit_code == 78
        assert sorted(os.listdir(tmp_path)) == ["config.db"]


# --------------------------------------------------------------------------
# Group operators (ownership mocked, as tests/test_zfsbackup_store_permissions.py)
# --------------------------------------------------------------------------


@pytest.fixture
def foreign_euid(monkeypatch):
    """This process looks like a user who owns none of tmp_path's files."""
    fake = os.getuid() + 4242
    monkeypatch.setattr(os, "geteuid", lambda: fake)
    return fake


class TestGroupOperator:
    """A group operator is not the owner of the database or of `<db>.tls/`; the
    server key is root-owned and unreadable to them. Everything they run here
    must work from the world-readable certificate alone."""

    def test_every_trust_command_works_as_a_non_owner(self, run, server, resolved, foreign_euid):
        ident = server_identity(server)
        enroll(resolved, "c1", fake_pin("77aa88bb", "3"))
        result = create(run, server, "--json")
        assert result.exit_code == 0, result.stderr
        assert json.loads(result.stdout)["server_fingerprint"] == ident.fingerprint
        assert run("trust", "token", "show", "--json", "-c", server).exit_code == 0
        assert run("trust", "list", "--json", "-c", server).exit_code == 0
        assert revoke(run, server, "77aa88bb").exit_code == 0
        assert revoke(run, server, "--client", "c1").exit_code == 0
        assert run("trust", "token", "revoke", "-c", server).exit_code == 0

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads any file")
    def test_token_create_with_the_key_hidden_and_the_directory_read_only(self, run, server, foreign_euid):
        ident = server_identity(server)
        os.chmod(ident.key_path, 0)
        os.chmod(tls_of(server), 0o550)
        try:
            result = create(run, server, "--json")
        finally:
            os.chmod(tls_of(server), 0o750)
            os.chmod(ident.key_path, 0o600)
        assert result.exit_code == 0, result.stderr
        assert json.loads(result.stdout)["server_fingerprint"] == ident.fingerprint

    def test_without_an_identity_a_non_owner_gets_the_advice(self, run, server, foreign_euid, monkeypatch):
        def refuse(directory, role, **kw):
            raise identity.IdentityError("permission denied")

        monkeypatch.setattr(identity, "ensure_identity", refuse)
        result = create(run, server)
        assert result.exit_code == 1
        assert "Start the daemon once (not with --dry-run: it creates" in result.stderr
        assert "at its first start)" in result.stderr


# --------------------------------------------------------------------------
# -c / -v placement
# --------------------------------------------------------------------------


class TestOptionPlacement:
    @pytest.mark.parametrize("position", ["root", "trust", "token", "leaf"])
    def test_c_and_v_at_every_level(self, run, server, resolved, position):
        enroll(resolved, "c1", real_pin("c1"))
        if position == "root":
            args = ["-c", server, "-v", "trust", "token", "show"]
        elif position == "trust":
            args = ["trust", "-c", server, "-v", "token", "show"]
        elif position == "token":
            args = ["trust", "token", "-c", server, "-v", "show"]
        else:
            args = ["trust", "token", "show", "-c", server, "-v"]
        result = run(*args)
        assert result.exit_code == 0, result.stderr
        assert "state:              consumed" in result.stdout

    def test_list_and_revoke_take_them_after_the_subcommand(self, run, server, resolved):
        enroll(resolved, "c1", fake_pin("77aa88bb", "3"))
        assert run("trust", "list", "-v", "-c", server).exit_code == 0
        assert run("trust", "revoke", "77aa88bb", "-v", "-c", server).exit_code == 0

    def test_the_help_lists_the_subcommands(self, run):
        trust = run("trust", "--help")
        assert trust.exit_code == 0
        for word in ("token", "list", "revoke"):
            assert word in trust.stdout
        token = run("trust", "token", "--help")
        for word in ("create", "show", "revoke"):
            assert word in token.stdout
        create_help = run("trust", "token", "create", "--help").stdout
        for option in ("--ttl", "--client-id", "--replace", "--json"):
            assert option in create_help


# --------------------------------------------------------------------------
# Racing token creators (real processes)
# --------------------------------------------------------------------------


@pytest.mark.integration
class TestConcurrentTokenCreate:
    TIMEOUT = 120

    def _spawn(self, database: Path, *extra):
        from _hermetic import hermetic_env

        env = hermetic_env(PYTHONPATH=str(REPO))
        env.pop("ZFSBACKUP_CONFIG", None)
        return subprocess.Popen(
            [sys.executable, "-m", "zfsbackup.cli", "-c", str(database),
             "trust", "token", "create", "--json", *extra],
            cwd=str(REPO), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

    def _finish(self, procs):
        results = []
        try:
            for proc in procs:
                out, err = proc.communicate(timeout=self.TIMEOUT)
                results.append((proc.returncode, out, err))
        finally:
            for proc in procs:
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate()
        return results

    def test_two_creators_leave_exactly_one_token(self, run, server):
        dispose_all()
        results = self._finish([self._spawn(server), self._spawn(server)])
        codes = [code for code, _, _ in results]
        assert all(code in (0, 75) for code in codes), results
        assert 0 in codes, results
        tokens = [json.loads(out)["token"] for code, out, _ in results if code == 0]
        for code, out, err in results:
            if code == 75:
                assert TOKEN_RE.search(out + err) is None
        # One row, and it is the hash of the token of the last writer to commit.
        stored = stored_token_hash(server)
        assert stored in {hash_of(t) for t in tokens}
        # Both processes agree on the one server identity (generated once under the identity lock).
        fps = {json.loads(out)["server_fingerprint"] for code, out, _ in results if code == 0}
        assert len(fps) == 1
        assert identity.fingerprint_pem((tls_of(server) / "server.crt").read_text()) in fps
        # Nothing was left behind in the TLS directory but the finished identity.
        names = sorted(p.name for p in tls_of(server).iterdir())
        assert names == ["identity.lock", "server.crt", "server.key"], names

    def test_a_creator_racing_a_revoker(self, run, server, resolved):
        from _hermetic import hermetic_env

        creator = self._spawn(server)
        env = hermetic_env(PYTHONPATH=str(REPO))
        env.pop("ZFSBACKUP_CONFIG", None)
        revoker = subprocess.Popen(
            [sys.executable, "-m", "zfsbackup.cli", "-c", str(server), "trust", "token", "revoke"],
            cwd=str(REPO), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        results = self._finish([creator, revoker])
        assert all(code in (0, 75) for code, _, _ in results), results
        # either order is legal; the row, if any, is the creator's
        stored = stored_token_hash(server)
        if stored is not None and results[0][0] == 0:
            assert stored == hash_of(json.loads(results[0][1])["token"])

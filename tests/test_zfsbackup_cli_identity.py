"""`zfsbackup-config identity show` and `identity set-client-id` (TLS/mTLS plan
M12, D2, A2, A7).

The client side of identity: this host's client id (stored in the config
database, `host_identity`) and its TLS certificates. `identity show` is read-only;
`identity set-client-id` is a CONFIG write (the store bumps the config
generation with the id) that signals the daemon exactly once. Every database is
built by `import`; the client id is read back through the store's public API.

Covered:

- ``TestIdentityShow``          text and JSON, the configured/default source, a missing
                                / expired / unreadable / garbage certificate, a missing
                                key; always exit 0; nothing is created
- ``TestSetClientIdGrammar``    the A7 grammar, refused before anything is read
- ``TestSetClientId``           first set, change, no-op, refusal while enrolled,
                                `--force`, the warning, output
- ``TestSetClientIdSignal``     exactly one `request_reload`, before any output
- ``TestSetClientIdConflicts``  CAS conflict, replaced file, busy: exit 75, no signal
- ``TestSetClientIdPreservation`` `import` keeps the id and the trust tables
- ``TestMissingDatabase``       exit 78, nothing created
- ``TestGroupOperator``         a non-owner (ownership mocked)
- ``TestOptionPlacement``       `-c` / `-v` at every level
- ``TestConcurrentSetClientId`` two real processes racing (integration)

Mutation counts (scratch copy of the tree, one revert at a time against
`zfsbackup/cli/trust.py`; failing tests across THIS file and its sibling
`test_zfsbackup_cli_trust.py`): active-token check widened to any previous
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

import dataclasses
import json
import os
import re
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
    generation,
    host_client_id,
    isolated,
    store_key,
    write_yaml,
)
from _cli_support import make_db as _make_db
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
    read_config,
    read_trust_key,
    resolve_config_path,
    token_status,
    write_config,
)

# No module-level tier marker (conftest marks a test that declares none as
# `unit`); the process-spawning test is `integration` on its own.

REPO = Path(__file__).resolve().parent.parent

FP = "ab" * 32
ISO_RE = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ")

#: Two destinations pinned to a server fingerprint (`ENROLLED`) and one not.
ENROLLED_YAML = PLAIN_YAML + (
    "destinations:\n"
    '  "offsite":\n'
    '    url: "https://offsite:8080"\n'
    f'    server_fingerprint: "{FP}"\n'
    '  "dc.two":\n'
    '    url: "https://dc2:8080"\n'
    f'    server_fingerprint: "{"cd" * 32}"\n'
    '  "spare":\n'
    '    url: "https://spare:8080"\n'
)
#: Destinations, none enrolled.
UNENROLLED_YAML = PLAIN_YAML + (
    "destinations:\n"
    '  "spare":\n'
    '    url: "https://spare:8080"\n'
)

BAD_IDS = [
    "", ".hidden", ".", "_lead", "a b", "a/b", "a\\b", "é", "ünï", "x" * 65, "a;b", "a\nb",
    "a:b", "a@b", "a%b", "a*b", "tab\tid", "../etc",
]
NEW_CLIENT_WARNING = "every server will see this host as a NEW client"


# --------------------------------------------------------------------------
# Fixtures and helpers
# --------------------------------------------------------------------------


@pytest.fixture
def reload_calls(monkeypatch):
    """`request_reload` replaced by a recorder (each call's `skip_if_applied`
    key, and the key the DATABASE holds at that moment)."""
    calls = []

    def recorder(resolved, *, skip_if_applied=None):
        calls.append(skip_if_applied)
        return daemonlock.ReloadRequest("no-daemon")

    monkeypatch.setattr(daemonlock, "request_reload", recorder)
    return calls


@pytest.fixture
def run(reload_calls) -> Cli:
    cli = Cli()
    cli.reload_calls = reload_calls
    return cli


def make_db(run, database, text=PLAIN_YAML) -> Path:
    """`import` the database (a config write, so it asks the daemon to reload)
    and forget those calls: only the commands under test are counted."""
    _make_db(run, database, text)
    run.reload_calls.clear()
    return database


@pytest.fixture
def plain(run, db) -> Path:
    return make_db(run, db, PLAIN_YAML)


@pytest.fixture
def enrolled(run, db) -> Path:
    return make_db(run, db, ENROLLED_YAML)


def tls_of(database: Path) -> Path:
    return Path(str(database) + ".tls")


def tree(root: Path) -> list:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


_KEY = None


def make_cert(cn="client", *, valid_from=None, valid_to=None):
    """A real self-signed certificate and the key it was made with ->
    `(cert_pem, key_pem, fingerprint, not_after)`. One EC key is reused."""
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
    key_pem = _KEY.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("ascii")
    return pem, key_pem, identity.fingerprint_pem(pem), valid_to.replace(microsecond=0)


def configured_yaml(cert: Path, key: Path) -> str:
    return PLAIN_YAML + f'client_cert_file: "{cert}"\nclient_key_file: "{key}"\n'


def show_json(run, database):
    result = run("identity", "show", "--json", "-c", database)
    assert result.exit_code == 0, result.stderr
    return json.loads(result.stdout)


def set_id(run, database, *args):
    return run("identity", "set-client-id", *args, "-c", database)


def enroll_pin(resolved, client_id="c1"):
    now = time.time()
    _t, digest = identity.make_token(FP)
    create_token(resolved, secret_sha256=digest, server_fingerprint=FP,
                 created_at=now - 1, expires_at=now + 3600, lock=None)
    pin = NewPin("77aa88bb" + "3" * 56, "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n",
                 now + 86400)
    outcome = enroll_client(resolved, secret_sha256=digest, server_fingerprint=FP,
                            client_id=client_id, pin=pin, now=now, dataset_exists=False)
    assert outcome == EnrollOutcome.ENROLLED
    return pin


def without_sqlite_sidecars(root: Path) -> list:
    """The tree, minus the `-wal`/`-shm` files SQLite itself maintains while any
    connection is open (a read opens one; neither is something the tool made)."""
    return [p for p in tree(root) if not p.endswith(("-wal", "-shm"))]


# --------------------------------------------------------------------------
# identity show
# --------------------------------------------------------------------------


CERT_KEYS = {"path", "key_path", "source", "exists", "fingerprint", "not_after",
             "expired", "key_present", "error"}


class TestIdentityShow:
    def test_no_client_id_and_no_certificates_text(self, run, plain):
        result = run("identity", "show", "-c", plain)
        assert result.exit_code == 0, result.stderr
        out = result.stdout
        assert out.startswith("client id: not set -- generated by `zfsbackup-config enroll`\n")
        assert "client certificate:\n  none (generated by `zfsbackup-config enroll`)" in out
        assert f"  file:        {tls_of(plain) / 'client.crt'}\n" in out
        assert "server certificate:\n  none (created when the daemon first starts (not with --dry-run), or by `trust token create`)" in out
        assert f"  file:        {tls_of(plain) / 'server.crt'}" in out
        assert result.stderr == ""

    def test_the_json_shape_with_nothing_present(self, run, plain):
        data = show_json(run, plain)
        assert set(data) == {"client_id", "client_certificate", "server_certificate"}
        assert data["client_id"] is None
        client, server = data["client_certificate"], data["server_certificate"]
        assert set(client) == CERT_KEYS and set(server) == CERT_KEYS
        assert client == {
            "path": str(tls_of(plain) / "client.crt"), "key_path": str(tls_of(plain) / "client.key"),
            "source": "default", "exists": False, "fingerprint": None, "not_after": None,
            "expired": False, "key_present": False, "error": None,
        }
        assert server == {
            "path": str(tls_of(plain) / "server.crt"), "key_path": str(tls_of(plain) / "server.key"),
            "source": "default", "exists": False, "fingerprint": None, "not_after": None,
            "expired": False, "key_present": False, "error": None,
        }

    def test_the_client_id_is_shown_once_set(self, run, plain):
        set_id(run, plain, "web-1")
        assert show_json(run, plain)["client_id"] == "web-1"
        assert run("identity", "show", "-c", plain).stdout.startswith("client id: web-1\n")

    def test_generated_default_identities_are_described(self, run, plain):
        server = identity.ensure_identity(tls_of(plain), identity.ROLE_SERVER)
        client = identity.ensure_identity(tls_of(plain), identity.ROLE_CLIENT)
        data = show_json(run, plain)
        for entry, ident, name in ((data["server_certificate"], server, "server"),
                                   (data["client_certificate"], client, "client")):
            assert entry["source"] == "default"
            assert entry["exists"] is True and entry["key_present"] is True
            assert entry["fingerprint"] == ident.fingerprint
            assert ISO_RE.fullmatch(entry["not_after"])
            assert entry["not_after"] == ident.not_after.strftime("%Y-%m-%dT%H:%M:%SZ")
            assert entry["expired"] is False and entry["error"] is None
            assert entry["path"] == str(tls_of(plain) / f"{name}.crt")
        text = run("identity", "show", "-c", plain).stdout
        assert f"  fingerprint: {server.fingerprint}" in text
        assert f"  fingerprint: {client.fingerprint}" in text
        assert "(EXPIRED)" not in text and "MISSING" not in text

    # ---- configured paths ----

    def test_a_configured_source(self, run, db, tmp_path):
        files = tmp_path / "pki"
        files.mkdir()
        pem, key_pem, fp, not_after = make_cert()
        (files / "c.crt").write_text(pem)
        (files / "c.key").write_text(key_pem)
        make_db(run, db, configured_yaml(files / "c.crt", files / "c.key"))
        data = show_json(run, db)
        client = data["client_certificate"]
        assert client["source"] == "configured"
        assert client["path"] == str(files / "c.crt") and client["key_path"] == str(files / "c.key")
        assert client["exists"] is True and client["key_present"] is True
        assert client["fingerprint"] == fp
        assert client["not_after"] == not_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        assert client["expired"] is False and client["error"] is None
        # the server certificate is always the generated one
        assert data["server_certificate"]["source"] == "default"
        text = run("identity", "show", "-c", db).stdout
        assert f"  file:        {files / 'c.crt'} (configured)" in text
        assert f"  fingerprint: {fp}" in text

    def test_the_default_path_is_not_looked_at_when_configured(self, run, db, tmp_path):
        files = tmp_path / "pki"
        files.mkdir()
        make_db(run, db, configured_yaml(files / "c.crt", files / "c.key"))
        identity.ensure_identity(tls_of(db), identity.ROLE_CLIENT)  # present, but not the configured one
        client = show_json(run, db)["client_certificate"]
        assert client["source"] == "configured" and client["exists"] is False
        assert client["fingerprint"] is None

    def test_a_missing_configured_certificate(self, run, db, tmp_path):
        make_db(run, db, configured_yaml(tmp_path / "nope.crt", tmp_path / "nope.key"))
        result = run("identity", "show", "-c", db)
        assert result.exit_code == 0
        assert "none (generated by `zfsbackup-config enroll`)" in result.stdout
        client = show_json(run, db)["client_certificate"]
        assert client["exists"] is False and client["key_present"] is False and client["error"] is None
        assert not (tmp_path / "nope.crt").exists()  # the operator's files are never created

    # ---- the certificate's own state ----

    @pytest.fixture
    def cert_dir(self, tmp_path):
        directory = tmp_path / "pki"
        directory.mkdir()
        return directory

    def configure(self, run, db, cert_dir) -> Path:
        make_db(run, db, configured_yaml(cert_dir / "c.crt", cert_dir / "c.key"))
        return cert_dir / "c.crt"

    def test_an_expired_certificate(self, run, db, cert_dir):
        now = datetime.now(timezone.utc)
        pem, key_pem, fp, _ = make_cert(valid_from=now - timedelta(days=30), valid_to=now - timedelta(days=1))
        (cert_dir / "c.crt").write_text(pem)
        (cert_dir / "c.key").write_text(key_pem)
        self.configure(run, db, cert_dir)
        client = show_json(run, db)["client_certificate"]
        assert client["expired"] is True and client["fingerprint"] == fp and client["error"] is None
        result = run("identity", "show", "-c", db)
        assert result.exit_code == 0
        assert re.search(r"expires:\s+\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ \(EXPIRED\)", result.stdout)

    def test_a_missing_key(self, run, db, cert_dir):
        pem, _key, fp, _ = make_cert()
        (cert_dir / "c.crt").write_text(pem)
        self.configure(run, db, cert_dir)
        client = show_json(run, db)["client_certificate"]
        assert client["exists"] is True and client["key_present"] is False
        assert client["fingerprint"] == fp and client["error"] is None
        result = run("identity", "show", "-c", db)
        assert result.exit_code == 0
        assert f"  key:         MISSING ({cert_dir / 'c.key'})" in result.stdout

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads any file")
    def test_an_unreadable_certificate(self, run, db, cert_dir):
        pem, key_pem, _fp, _ = make_cert()
        cert = self.configure(run, db, cert_dir)
        cert.write_text(pem)
        (cert_dir / "c.key").write_text(key_pem)
        os.chmod(cert, 0)
        try:
            data = show_json(run, db)
            text = run("identity", "show", "-c", db)
        finally:
            os.chmod(cert, 0o644)
        client = data["client_certificate"]
        assert client["exists"] is True and client["error"] == "permission denied"
        assert client["fingerprint"] is None and client["not_after"] is None
        assert client["expired"] is False
        assert text.exit_code == 0
        assert "  unreadable: permission denied" in text.stdout

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads any file")
    def test_an_unreadable_key_is_still_present(self, run, db, cert_dir):
        pem, key_pem, fp, _ = make_cert()
        cert = self.configure(run, db, cert_dir)
        cert.write_text(pem)
        (cert_dir / "c.key").write_text(key_pem)
        os.chmod(cert_dir / "c.key", 0)
        try:
            result = run("identity", "show", "--json", "-c", db)
        finally:
            os.chmod(cert_dir / "c.key", 0o600)
        assert result.exit_code == 0
        client = json.loads(result.stdout)["client_certificate"]
        assert client["key_present"] is True and client["fingerprint"] == fp

    @pytest.mark.parametrize("content, fragment", [
        (b"this is not a certificate\n", "not a PEM certificate"),
        (b"", "not a PEM certificate"),
        (b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n", "not a PEM certificate"),
        (b"\x00\xff\x00binary", "not a PEM certificate"),
        (b"x" * (1024 * 1024 + 2), "too large to be a certificate"),
    ], ids=["text", "empty", "bad-pem", "binary", "oversize"])
    def test_a_garbage_certificate_is_described_not_an_error(self, run, db, cert_dir, content, fragment):
        cert = self.configure(run, db, cert_dir)
        cert.write_bytes(content)
        result = run("identity", "show", "--json", "-c", db)
        assert result.exit_code == 0, result.stderr
        client = json.loads(result.stdout)["client_certificate"]
        assert client["exists"] is True and fragment in client["error"]
        assert client["fingerprint"] is None and client["not_after"] is None
        text = run("identity", "show", "-c", db)
        assert text.exit_code == 0 and f"  unreadable: {client['error']}" in text.stdout

    def test_a_directory_in_place_of_the_certificate(self, run, db, cert_dir):
        cert = self.configure(run, db, cert_dir)
        cert.mkdir()
        result = run("identity", "show", "--json", "-c", db)
        assert result.exit_code == 0, result.stderr
        client = json.loads(result.stdout)["client_certificate"]
        assert client["exists"] is True and client["error"]
        assert client["fingerprint"] is None

    def test_a_broken_server_certificate_does_not_hide_the_client_one(self, run, db, cert_dir):
        pem, key_pem, fp, _ = make_cert()
        cert = self.configure(run, db, cert_dir)
        cert.write_text(pem)
        (cert_dir / "c.key").write_text(key_pem)
        tls_of(db).mkdir(mode=0o750)
        (tls_of(db) / "server.crt").write_text("garbage")
        data = show_json(run, db)
        assert data["client_certificate"]["fingerprint"] == fp
        assert "not a PEM certificate" in data["server_certificate"]["error"]

    def test_an_expired_and_keyless_server_certificate(self, run, plain):
        now = datetime.now(timezone.utc)
        pem, _k, fp, _ = make_cert("server", valid_from=now - timedelta(days=30), valid_to=now - timedelta(days=1))
        tls_of(plain).mkdir(mode=0o750)
        (tls_of(plain) / "server.crt").write_text(pem)
        server = show_json(run, plain)["server_certificate"]
        assert server["expired"] is True and server["key_present"] is False and server["fingerprint"] == fp
        text = run("identity", "show", "-c", plain).stdout
        assert "(EXPIRED)" in text
        assert f"key:         MISSING ({tls_of(plain) / 'server.key'})" in text

    # ---- read-only, never key material ----

    def test_nothing_is_created_or_changed(self, run, plain):
        before = (without_sqlite_sidecars(plain.parent), db_state(plain)[0], generation(plain))
        for args in (("identity", "show"), ("identity", "show", "--json")):
            assert run(*args, "-c", plain).exit_code == 0
        assert (without_sqlite_sidecars(plain.parent), db_state(plain)[0], generation(plain)) == before
        assert not tls_of(plain).exists()
        assert not (plain.parent / (plain.name + ".daemon.pid")).exists()

    def test_nothing_is_created_beside_an_existing_identity_either(self, run, plain):
        identity.ensure_identity(tls_of(plain), identity.ROLE_SERVER)
        before = tree(tls_of(plain))
        run("identity", "show", "-c", plain)
        assert tree(tls_of(plain)) == before

    def test_no_identity_is_generated_or_loaded(self, run, plain, monkeypatch):
        def forbidden(*a, **k):
            raise AssertionError("identity show must only describe certificate files")

        for name in ("ensure_identity", "load_identity", "promote_identity", "peek_identity"):
            monkeypatch.setattr(identity, name, forbidden)
        assert run("identity", "show", "-c", plain).exit_code == 0
        assert run("identity", "show", "--json", "-c", plain).exit_code == 0

    def test_a_private_key_is_never_printed(self, run, plain):
        identity.ensure_identity(tls_of(plain), identity.ROLE_SERVER)
        key_text = (tls_of(plain) / "server.key").read_text()
        body = "".join(key_text.splitlines()[1:-1])
        for args in (("identity", "show"), ("identity", "show", "--json")):
            result = run(*args, "-c", plain)
            assert "PRIVATE KEY" not in result.stdout + result.stderr
            assert body[:40] not in result.stdout + result.stderr

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads any file")
    def test_the_key_is_not_opened(self, run, plain):
        ident = identity.ensure_identity(tls_of(plain), identity.ROLE_SERVER)
        os.chmod(ident.key_path, 0)
        try:
            result = run("identity", "show", "--json", "-c", plain)
        finally:
            os.chmod(ident.key_path, 0o600)
        assert result.exit_code == 0
        assert json.loads(result.stdout)["server_certificate"]["key_present"] is True

    def test_show_does_not_signal_the_daemon(self, run, plain, reload_calls):
        run("identity", "show", "-c", plain)
        run("identity", "show", "--json", "-c", plain)
        assert reload_calls == []

    def test_show_does_not_bump_the_generation(self, run, plain):
        gen = generation(plain)
        run("identity", "show", "-c", plain)
        assert generation(plain) == gen


# --------------------------------------------------------------------------
# identity set-client-id
# --------------------------------------------------------------------------


class TestSetClientIdGrammar:
    @pytest.mark.parametrize("bad", BAD_IDS, ids=[repr(b)[:20] for b in BAD_IDS])
    def test_a_bad_id_is_exit_1_and_nothing_changes(self, run, plain, reload_calls, bad):
        before = (generation(plain), db_state(plain)[0], without_sqlite_sidecars(plain.parent))
        result = set_id(run, plain, bad)
        assert result.exit_code == 1, result.stderr
        assert f"invalid client id {bad!r}" in result.stderr
        assert result.stdout == ""
        assert reload_calls == []
        assert host_client_id(plain) is None
        assert (generation(plain), db_state(plain)[0], without_sqlite_sidecars(plain.parent)) == before

    def test_the_grammar_is_checked_before_the_database_is_read(self, run, tmp_path):
        before = tree(tmp_path)
        result = set_id(run, tmp_path / "nowhere" / "config.db", ".bad")
        assert result.exit_code == 1  # not 78
        assert tree(tmp_path) == before

    @pytest.mark.parametrize("good", [
        "host-1", "a", "A", "0", "a.b_c-d", "x" * 64, "web.example.com-1a2b3c4d", "a..b", "x.", "0lead", "9-",
    ])
    def test_a_valid_id_is_accepted(self, run, plain, good):
        result = run("identity", "set-client-id", "-c", plain, "--", good)
        assert result.exit_code == 0, result.stderr
        assert host_client_id(plain) == good

    def test_sixty_five_characters_is_one_too_many(self, run, plain):
        assert set_id(run, plain, "x" * 65).exit_code == 1
        assert "the maximum is 64" in set_id(run, plain, "x" * 65).stderr

    @pytest.mark.parametrize("bad, char", [(".hidden", "."), ("-lead", "-"), ("_lead", "_")])
    def test_a_bad_first_character_message(self, run, plain, bad, char):
        # Grammar A7: the first character is a letter or digit (`<target>/<id>` must be a
        # valid dataset name).
        result = run("identity", "set-client-id", "-c", plain, "--", bad)
        assert result.exit_code == 1
        assert f"client id must start with an ASCII letter or digit, not {char!r}" in result.stderr
        assert "must not start with '.'" not in result.stderr
        assert host_client_id(plain) is None

    def test_the_help_states_the_first_character_rule(self, run):
        out = " ".join(run("identity", "set-client-id", "--help").stdout.split())
        assert "starting with a letter or digit" in out


class TestSetClientId:
    def test_the_first_id(self, run, plain, reload_calls):
        gen = generation(plain)
        result = set_id(run, plain, "web-1")
        assert result.exit_code == 0, result.stderr
        assert host_client_id(plain) == "web-1"
        assert generation(plain) == gen + 1
        assert "client id: (not set) -> web-1\n" in result.stdout
        assert f"config updated (generation {gen + 1})" in result.stdout

    def test_changing_the_id(self, run, plain):
        set_id(run, plain, "old-id")
        gen = generation(plain)
        result = set_id(run, plain, "new-id")
        assert result.exit_code == 0, result.stderr
        assert host_client_id(plain) == "new-id"
        assert generation(plain) == gen + 1
        assert "client id: old-id -> new-id\n" in result.stdout

    def test_the_new_client_warning_is_always_given(self, run, plain):
        first = set_id(run, plain, "one")
        assert f"warning: changing the client id to 'one': {NEW_CLIENT_WARNING}" in first.stderr
        assert "FULL send to <target>/one" in first.stderr
        assert "data under the old id on the servers is left in place" in first.stderr
        second = set_id(run, plain, "two")
        assert f"warning: changing the client id from 'one' to 'two': {NEW_CLIENT_WARNING}" in second.stderr
        assert "FULL send to <target>/two" in second.stderr

    def test_the_warning_is_on_stderr_only(self, run, plain):
        result = set_id(run, plain, "one")
        assert "NEW client" not in result.stdout

    def test_the_same_id_is_a_no_op(self, run, plain, reload_calls):
        set_id(run, plain, "same")
        reload_calls.clear()
        gen = generation(plain)
        state = db_state(plain)
        result = set_id(run, plain, "same")
        assert result.exit_code == 0
        assert result.stdout == "client id is already same; nothing changed\n"
        assert "NEW client" not in result.stderr
        assert reload_calls == []
        assert generation(plain) == gen
        assert db_state(plain)[0] == state[0]

    def test_a_no_op_is_not_refused_while_enrolled(self, run, enrolled):
        set_id(run, enrolled, "same")
        assert set_id(run, enrolled, "same").exit_code == 0

    def test_it_creates_no_tls_files(self, run, plain):
        set_id(run, plain, "web-1")
        assert not tls_of(plain).exists()

    def test_it_does_not_touch_the_trust_tables(self, run, plain):
        resolved = resolve_config_path(str(plain))
        enroll_pin(resolved)
        before = (read_trust_key(resolved), list_clients(resolved))
        set_id(run, plain, "web-1")
        assert (read_trust_key(resolved), list_clients(resolved)) == before

    def test_the_stored_configuration_is_unchanged(self, run, enrolled):
        resolved = resolve_config_path(str(enrolled))
        before = read_config(resolved).config
        set_id(run, enrolled, "web-1")
        assert read_config(resolved).config == before
        dispose_all()

    def test_the_id_is_in_the_config_snapshot(self, run, plain):
        set_id(run, plain, "web-1")
        assert read_config(resolve_config_path(str(plain))).client_id == "web-1"
        dispose_all()

    # ---- enrolled destinations ----

    def test_with_no_id_stored_nothing_is_refused_even_when_destinations_are_enrolled(self, run, enrolled):
        result = set_id(run, enrolled, "first")
        assert result.exit_code == 0, result.stderr
        assert host_client_id(enrolled) == "first"
        assert "--force" not in result.stderr  # no force warning either

    def test_a_change_is_refused_while_a_destination_is_enrolled(self, run, enrolled, reload_calls):
        set_id(run, enrolled, "old")
        reload_calls.clear()
        gen = generation(enrolled)
        result = set_id(run, enrolled, "new")
        assert result.exit_code == 1
        err = result.stderr
        assert "refusing to change the client id while destination(s) 'dc.two', 'offsite' are enrolled" in err
        assert "'spare'" not in err  # not enrolled
        assert "bound to the OLD id 'old'" in err
        assert "403 (mismatch)" in err
        assert "`zfsbackup-config trust revoke --client old`" in err
        assert "NEW client certificate" in err
        assert "`trust token create`" in err
        assert "`--client-id new --replace`" in err
        # the key is changed ONCE: --new-key on the first server, a plain enroll on the others
        assert "--new-key --token-file FILE` for the first server" in err
        assert "a plain `zfsbackup-config enroll NAME --token-file FILE`" in err
        assert "409 client_id_taken" in err and "without --replace the old certificate stays pinned" in err
        assert "Re-run with --force" in err
        assert result.stdout == ""
        assert host_client_id(enrolled) == "old"
        assert generation(enrolled) == gen
        assert reload_calls == []
        assert "FULL send" not in err  # the warning belongs to a change that happens

    def test_force_proceeds_with_a_warning(self, run, enrolled, reload_calls):
        set_id(run, enrolled, "old")
        reload_calls.clear()
        result = set_id(run, enrolled, "new", "--force")
        assert result.exit_code == 0, result.stderr
        assert host_client_id(enrolled) == "new"
        assert ("warning: --force: destination(s) 'dc.two', 'offsite' stay enrolled under the old "
                "client id 'old' and will refuse this host (403 mismatch) until it is enrolled again") in result.stderr
        assert NEW_CLIENT_WARNING in result.stderr
        assert len(reload_calls) == 1

    def test_force_when_nothing_is_enrolled_adds_no_warning(self, run, db):
        make_db(run, db, UNENROLLED_YAML)
        set_id(run, db, "old")
        result = set_id(run, db, "new", "--force")
        assert result.exit_code == 0
        assert "--force:" not in result.stderr

    def test_unenrolled_destinations_do_not_block(self, run, db):
        make_db(run, db, UNENROLLED_YAML)
        set_id(run, db, "old")
        assert set_id(run, db, "new").exit_code == 0
        assert host_client_id(db) == "new"

    def test_no_destinations_at_all(self, run, plain):
        set_id(run, plain, "old")
        assert set_id(run, plain, "new").exit_code == 0

    def test_a_single_enrolled_destination_is_named(self, run, db):
        make_db(run, db, PLAIN_YAML + 'destinations:\n  "only":\n    url: "https://o:1"\n'
                f'    server_fingerprint: "{FP}"\n')
        set_id(run, db, "old")
        assert "destination(s) 'only' are enrolled" in set_id(run, db, "new").stderr

    def test_a_refusal_leaves_the_database_byte_identical(self, run, enrolled):
        set_id(run, enrolled, "old")
        state = db_state(enrolled)
        assert set_id(run, enrolled, "new").exit_code == 1
        assert db_state(enrolled) == state

    def test_the_dotted_destination_name_is_quoted_verbatim(self, run, enrolled):
        set_id(run, enrolled, "old")
        assert "'dc.two'" in set_id(run, enrolled, "new").stderr


class TestSetClientIdSignal:
    def test_exactly_one_request_after_the_commit_with_the_written_key(self, run, plain, db, reload_calls, monkeypatch):
        seen = []

        def recorder(resolved, *, skip_if_applied=None):
            seen.append((skip_if_applied, store_key(db)))
            return daemonlock.ReloadRequest("no-daemon")

        monkeypatch.setattr(daemonlock, "request_reload", recorder)
        before = store_key(db)
        assert set_id(run, plain, "web-1").exit_code == 0
        assert len(seen) == 1
        (skip, stored), = seen
        assert skip == stored == (before[0], before[1] + 1)

    def test_it_is_requested_before_any_output(self, run, plain, monkeypatch):
        import click

        events = []
        real_echo = click.echo

        def echo(message=None, *args, **kwargs):
            if not kwargs.get("err"):
                events.append(("stdout", message))
            return real_echo(message, *args, **kwargs)

        def recorder(resolved, *, skip_if_applied=None):
            events.append(("reload", skip_if_applied))
            return daemonlock.ReloadRequest("no-daemon")

        monkeypatch.setattr(click, "echo", echo)
        monkeypatch.setattr(daemonlock, "request_reload", recorder)
        assert set_id(run, plain, "web-1").exit_code == 0
        kinds = [kind for kind, _ in events]
        assert kinds.count("reload") == 1
        assert kinds.index("reload") < kinds.index("stdout")

    def test_a_closed_stdout_does_not_cost_the_signal(self, run, plain, reload_calls, monkeypatch):
        import click

        real_echo = click.echo

        def echo(message=None, *args, **kwargs):
            if isinstance(message, str) and message.startswith("client id:") and not kwargs.get("err"):
                raise BrokenPipeError(32, "Broken pipe")
            return real_echo(message, *args, **kwargs)

        monkeypatch.setattr(click, "echo", echo)
        set_id(run, plain, "web-1")
        assert len(reload_calls) == 1
        assert host_client_id(plain) == "web-1"  # and the write is durable

    def test_the_notice_is_printed_once(self, run, plain):
        result = set_id(run, plain, "web-1")
        assert result.stdout.count("config updated (generation") == 1

    @pytest.mark.parametrize("outcome, fragment", [
        ("no-daemon", "no running daemon detected"),
        ("already-applied", "is already on generation"),
        ("unverified", "was not signalled"),
    ])
    def test_every_outcome_is_exit_0_and_is_reported(self, run, plain, monkeypatch, outcome, fragment):
        kw = {"pid": 4321} if outcome != "no-daemon" else {}
        if outcome == "unverified":
            kw["detail"] = "could not verify"
        monkeypatch.setattr(daemonlock, "request_reload", lambda r, **k: daemonlock.ReloadRequest(outcome, **kw))
        result = set_id(run, plain, "web-1")
        assert result.exit_code == 0, result.stderr
        assert fragment in result.stdout

    def test_verbose_does_not_signal_twice(self, run, plain, reload_calls):
        assert set_id(run, plain, "web-1", "-v").exit_code == 0
        assert len(reload_calls) == 1

    def test_a_force_change_signals_once(self, run, enrolled, reload_calls):
        set_id(run, enrolled, "old")
        reload_calls.clear()
        set_id(run, enrolled, "new", "--force")
        assert len(reload_calls) == 1


class TestSetClientIdConflicts:
    def test_a_stale_generation_is_exit_75_with_no_signal(self, run, plain, reload_calls, monkeypatch):
        real = trust_cli.read_snapshot
        resolved = resolve_config_path(str(plain))

        def stale(_resolved):
            snapshot = real(_resolved)
            # a concurrent writer commits between this read and our write
            write_config(_resolved, snapshot.config, expected_generation=snapshot.generation, lock=None)
            dispose_all()
            return snapshot

        monkeypatch.setattr(trust_cli, "read_snapshot", stale)
        before = generation(plain)
        result = set_id(run, plain, "web-1")
        assert result.exit_code == 75, result.stderr
        assert "config changed underneath you" in result.stderr
        assert "Nothing was written" in result.stderr
        assert result.stdout == ""
        assert reload_calls == []
        assert host_client_id(plain) is None
        assert generation(plain) == before + 1  # only the concurrent writer's bump
        assert resolved  # the same database

    def test_a_replaced_database_file_is_exit_75_with_no_signal(self, run, plain, reload_calls, monkeypatch):
        real = trust_cli.read_snapshot

        def replaced(_resolved):
            snapshot = real(_resolved)
            return dataclasses.replace(snapshot, store_id=snapshot.store_id + 1000)

        monkeypatch.setattr(trust_cli, "read_snapshot", replaced)
        result = set_id(run, plain, "web-1")
        assert result.exit_code == 75, result.stderr
        assert "the configuration database was replaced since it was read" in result.stderr
        assert reload_calls == []
        assert host_client_id(plain) is None

    def test_a_busy_database_is_exit_75_with_no_signal(self, run, plain, reload_calls, monkeypatch):
        def busy(*a, **k):
            raise ConfigBusy("the config database is locked by another process")

        monkeypatch.setattr(trust_cli, "set_client_id", busy)
        result = set_id(run, plain, "web-1")
        assert result.exit_code == 75
        assert "locked by another process" in result.stderr
        assert reload_calls == []

    def test_an_unloadable_stored_configuration_is_exit_1_with_no_signal(self, run, plain, reload_calls, monkeypatch):
        def broken(_resolved):
            raise trust_cli.CliError("the stored configuration cannot be loaded: nope")

        monkeypatch.setattr(trust_cli, "read_snapshot", broken)
        assert set_id(run, plain, "web-1").exit_code == 1
        assert reload_calls == []

    def test_a_concurrent_set_to_the_same_id_is_a_no_op_for_the_loser(self, run, plain, reload_calls, monkeypatch):
        """The other writer already stored the id we want: the compare-and-swap
        still loses (the generation moved) -- retry semantics, exit 75."""
        real = trust_cli.read_snapshot

        def stale(_resolved):
            snapshot = real(_resolved)
            from zfsbackup.config.store import set_client_id as store_set

            store_set(_resolved, "web-1", expected_generation=snapshot.generation, lock=None)
            dispose_all()
            return snapshot

        monkeypatch.setattr(trust_cli, "read_snapshot", stale)
        result = set_id(run, plain, "web-1")
        assert result.exit_code == 75
        assert reload_calls == []
        assert host_client_id(plain) == "web-1"


class TestSetClientIdPreservation:
    def test_import_afterwards_keeps_the_id_and_the_trust_tables(self, run, plain, tmp_path, reload_calls):
        resolved = resolve_config_path(str(plain))
        assert set_id(run, plain, "web-1").exit_code == 0
        pin = enroll_pin(resolved)
        trust_before = (read_trust_key(resolved), list_clients(resolved), token_status(resolved, time.time()))
        source = write_yaml(tmp_path, UNENROLLED_YAML.replace("tank/a", "tank/other"), "again.yaml")
        result = run("import", source, "-c", plain)
        assert result.exit_code == 0, result.stderr
        assert host_client_id(plain) == "web-1"
        trust_after = (read_trust_key(resolved), list_clients(resolved), token_status(resolved, time.time()))
        assert trust_after == trust_before
        assert [p.fingerprint for c in list_clients(resolved) for p in c.pins] == [pin.fingerprint]
        assert read_config(resolved).client_id == "web-1"
        dispose_all()

    def test_export_never_emits_the_id(self, run, plain):
        set_id(run, plain, "web-1-unique")
        exported = run("export", "-c", plain)
        assert exported.exit_code == 0
        assert "web-1-unique" not in exported.stdout
        assert "client_id" not in exported.stdout

    def test_a_config_edit_keeps_the_id(self, run, plain):
        set_id(run, plain, "web-1")
        assert run("set", "check_interval", "10m", "-c", plain).exit_code == 0
        assert host_client_id(plain) == "web-1"

    def test_the_id_survives_an_export_import_round_trip(self, run, plain, tmp_path):
        set_id(run, plain, "web-1")
        exported = run("export", "-c", plain).stdout
        source = write_yaml(tmp_path, exported, "round.yaml")
        assert run("import", source, "-c", plain).exit_code == 0
        assert host_client_id(plain) == "web-1"
        assert show_json(run, plain)["client_id"] == "web-1"


# --------------------------------------------------------------------------
# A missing database
# --------------------------------------------------------------------------


class TestMissingDatabase:
    @pytest.mark.parametrize("command", [
        ("identity", "show"),
        ("identity", "show", "--json"),
        ("identity", "set-client-id", "web-1"),
        ("identity", "set-client-id", "web-1", "--force"),
    ], ids=lambda c: " ".join(c[1:]))
    def test_exit_78_and_nothing_is_created(self, run, tmp_path, reload_calls, command):
        before = tree(tmp_path)
        result = run(*command, "-c", tmp_path / "nowhere" / "config.db")
        assert result.exit_code == 78, result.stderr
        assert tree(tmp_path) == before
        assert reload_calls == []

    def test_a_zero_byte_file_is_not_a_database(self, run, tmp_path):
        empty = tmp_path / "config.db"
        empty.write_bytes(b"")
        assert set_id(run, empty, "web-1").exit_code == 78
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
    def test_show_and_set_work_as_a_non_owner(self, run, plain, foreign_euid, reload_calls):
        identity.ensure_identity(tls_of(plain), identity.ROLE_SERVER)
        assert run("identity", "show", "-c", plain).exit_code == 0
        result = set_id(run, plain, "web-1")
        assert result.exit_code == 0, result.stderr
        assert host_client_id(plain) == "web-1"
        assert len(reload_calls) == 1

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads any file")
    def test_show_with_a_root_only_key_and_a_read_only_directory(self, run, plain, foreign_euid):
        ident = identity.ensure_identity(tls_of(plain), identity.ROLE_SERVER)
        os.chmod(ident.key_path, 0)
        os.chmod(tls_of(plain), 0o550)
        try:
            result = run("identity", "show", "--json", "-c", plain)
        finally:
            os.chmod(tls_of(plain), 0o750)
            os.chmod(ident.key_path, 0o600)
        assert result.exit_code == 0, result.stderr
        server = json.loads(result.stdout)["server_certificate"]
        assert server["fingerprint"] == ident.fingerprint and server["key_present"] is True


# --------------------------------------------------------------------------
# -c / -v placement
# --------------------------------------------------------------------------


class TestOptionPlacement:
    @pytest.mark.parametrize("position", ["root", "identity", "leaf"])
    def test_c_and_v_at_every_level(self, run, plain, position):
        if position == "root":
            args = ["-c", plain, "-v", "identity", "show"]
        elif position == "identity":
            args = ["identity", "-c", plain, "-v", "show"]
        else:
            args = ["identity", "show", "-c", plain, "-v"]
        result = run(*args)
        assert result.exit_code == 0, result.stderr
        assert result.stdout.startswith("client id: not set")

    @pytest.mark.parametrize("position", ["root", "identity", "leaf"])
    def test_set_client_id_at_every_level(self, run, plain, position):
        if position == "root":
            args = ["-c", plain, "identity", "set-client-id", "web-1"]
        elif position == "identity":
            args = ["identity", "-c", plain, "set-client-id", "web-1"]
        else:
            args = ["identity", "set-client-id", "web-1", "-c", plain]
        assert run(*args).exit_code == 0
        assert host_client_id(plain) == "web-1"

    def test_the_help_lists_the_subcommands_and_options(self, run):
        group = run("identity", "--help")
        assert group.exit_code == 0
        assert "show" in group.stdout and "set-client-id" in group.stdout
        assert "--force" in run("identity", "set-client-id", "--help").stdout
        assert "--json" in run("identity", "show", "--help").stdout

    def test_set_client_id_needs_an_argument(self, run, plain):
        assert run("identity", "set-client-id", "-c", plain).exit_code == 2


# --------------------------------------------------------------------------
# Racing writers (real processes)
# --------------------------------------------------------------------------


@pytest.mark.integration
class TestConcurrentSetClientId:
    TIMEOUT = 120

    def _spawn(self, database: Path, new_id: str):
        from _hermetic import hermetic_env

        env = hermetic_env(PYTHONPATH=str(REPO))
        env.pop("ZFSBACKUP_CONFIG", None)
        return subprocess.Popen(
            [sys.executable, "-m", "zfsbackup.cli", "-c", str(database),
             "identity", "set-client-id", new_id],
            cwd=str(REPO), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

    def test_two_writers_one_wins_or_both_apply_in_turn(self, run, plain):
        gen = generation(plain)
        dispose_all()
        procs = [self._spawn(plain, "writer-a"), self._spawn(plain, "writer-b")]
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
        codes = [code for code, _, _ in results]
        assert all(code in (0, 75) for code in codes), results
        assert 0 in codes, results
        winners = sum(code == 0 for code in codes)
        final = host_client_id(plain)
        assert final in ("writer-a", "writer-b")
        # every success moved the generation exactly once; a loser wrote nothing
        assert generation(plain) == gen + winners
        for code, out, err in results:
            if code == 75:
                assert "config changed underneath you" in err
                assert out == ""


class TestDescribeCertificateOnSpecialFiles:
    """`tlsfiles.describe_certificate` is called by `identity show` and `enroll`; a FIFO at
    the certificate path must be reported, not read (opening one blocks for ever)."""

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs mkfifo")
    def test_a_fifo_returns_immediately_with_an_error(self, tmp_path):
        import threading

        from zfsbackup.cli import tlsfiles

        fifo = tmp_path / "client.crt"
        os.mkfifo(fifo)
        box = []
        worker = threading.Thread(target=lambda: box.append(tlsfiles.describe_certificate(fifo)),
                                  daemon=True)
        worker.start()
        worker.join(10)
        if worker.is_alive():                       # unblock a reader stuck in open(), then fail
            fd = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
            os.close(fd)
            worker.join(5)
            pytest.fail("describe_certificate blocked on a FIFO")
        (summary,) = box
        assert summary.exists is True and summary.fingerprint is None
        assert summary.error == "not a regular file"

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs mkfifo")
    def test_identity_show_reports_a_fifo_certificate_instead_of_hanging(self, run, db, tmp_path):
        fifo = tmp_path / "client.crt"
        os.mkfifo(fifo)
        _make_db(run, db, configured_yaml(fifo, tmp_path / "client.key"))
        data = show_json(run, db)
        assert "not a regular file" in json.dumps(data)
        assert not any(isinstance(v, str) and len(v) == 64 and set(v) <= set("0123456789abcdef")
                       for v in json.dumps(data).split('"'))   # no fingerprint was read

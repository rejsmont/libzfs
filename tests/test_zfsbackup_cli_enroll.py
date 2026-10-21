"""`zfsbackup-config enroll` (TLS/mTLS plan M13, D2, A5, A6, wire contract M10).

The client side of enrollment. Almost everything here runs the real CLI (`CliRunner`)
against a REAL API server in this process (`tests/_enroll_support.py::RealServer`: the
real Flask app, the real `TrustGateway`, a real trust database, a real TLS 1.3 listener
on 127.0.0.1) with real certificates and pins; the few answers the real app never gives
(a 502, a reset after the handshake, a hang) are scripted on that same server. `zfs`
is never reached (`zfs.exists` is an in-memory stub).

- ``TestTokenInput``            `--token-file`, `--token -`, the prompt, bad usage
- ``TestInputsAreCheckedFirst`` url / label / name problems exit before anything is
                                generated, written or sent
- ``TestEnroll``                the happy path: what is stored on each side
- ``TestIdempotency``           the `/whoami` probe: a re-run does not spend the token; id
                                mismatch; a token for another server; stale pins
- ``TestPinMismatch``           a server that is not the one in the token: the token is
                                NEVER sent
- ``TestServerAnswers``         every answer mapped to an exit code and a message
- ``TestTransport``             unreachable, timeout, a plaintext peer, resets
- ``TestSslEof``               a close with no TLS alert (`ssl.SSLEOFError`) is a reset, never a
                                local verification failure: `_classify` / `_is_reset`, and `enroll`
                                against a server that closes that way
- ``TestSecrecy``               the token appears only in the request body
- ``TestPermissions``           an identity problem exits 1 with no database write
- ``TestFailureAfterTheClientId`` the generated id is committed first: one reload on failure
- ``TestConflictThenRerun``     step 6 conflicts after the server enrolled us; the re-run
- ``TestNewKey``                `enroll --new-key`: fresh key, replace tokens, promotion,
                                configured paths, the warning about other servers
- ``TestRevokedCertificate``    a revoked certificate is refused for ever; `--new-key` recovers
- ``TestPromptOnATty``          a real pty and a real process (integration)

Every failing case runs with the client id already stored (the `client` fixture), so
"the database is byte-identical and nothing asked the daemon to reload" is checkable.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests in this
file plus `test_zfsbackup_cli_reload.py`'s enroll/rotate cases): see the hand-off report.
"""

import json
import os
import re
import ssl
import stat
import subprocess
import sys
import types
from pathlib import Path

import pytest
import requests
import urllib3

from _cli_support import (
    PLAIN_YAML,
    Cli,
    db,
    db_state,
    generation,
    host_client_id,
    isolated,
    make_db,
    seed_client_id,
    stored,
    store_key,
)
from _enroll_support import (
    CID,
    HANG,
    RESET,
    Reply,
    client,
    client_cert,
    client_with,
    closed_port,
    dest,
    do_enroll,
    everything,
    make_foreign_token,
    make_server,
    plain_http_server,
    reloads,
    secret_of,
    silent_server,
    tls_of,
    token_file,
    untouched,
    zfs_stub,
)
from _hermetic import hermetic_env
from zfsbackup import daemonlock, identity
from zfsbackup.cli import enroll as enroll_cli
from zfsbackup.cli import write as cli_write
from zfsbackup.config.store import ConfigBusy, ConfigConflict, dispose_all, resolve_config_path, revoke_pin

REPO = Path(__file__).resolve().parent.parent

#: Every test here talks TLS to a real API server on 127.0.0.1.
pytestmark = pytest.mark.integration
BOGUS_FP = "cd" * 32
TOKEN_RE = re.compile(r"zfsb1\.[0-9a-f]{64}\.[A-Za-z0-9_-]{43}")


# --------------------------------------------------------------------------
# Fixtures and helpers (shared ones live in _enroll_support.py)
# --------------------------------------------------------------------------


@pytest.fixture
def run(reloads) -> Cli:
    return Cli()


class TestHelp:
    def test_enroll_help_lists_every_option(self, run):
        result = run("enroll", "--help")
        assert result.exit_code == 0
        out = " ".join(result.stdout.split())
        for option in ("--url", "--label", "--token-file", "--token", "--new-key", "--cert",
                       "--key", "-c, --config", "-v, --verbose"):
            assert option in out, option
        assert "never on the command line" in out

    def test_enroll_help_describes_the_discard_and_the_rotation_successor_caveat(self, run):
        out = " ".join(run("enroll", "--help").stdout.split())
        assert ("A pending client.next that no server trusts is discarded and replaced by a "
                "fresh key (it is reused only when the server at this destination already "
                "trusts it; another server that accepted it as a rotation successor must be "
                "enrolled again).") in out

    def test_enroll_is_in_the_top_level_help(self, run):
        assert "enroll" in run("--help").stdout


# --------------------------------------------------------------------------
# The token
# --------------------------------------------------------------------------


class TestTokenInput:
    def test_a_token_file(self, run, client, make_server):
        srv = make_server()
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 0, result.stderr

    def test_a_token_file_with_no_trailing_newline_or_with_spaces(self, run, client, make_server):
        srv = make_server()
        path = client.parent.parent / "bare"
        path.write_text(f"  {srv.token()}  ")
        path.chmod(0o600)
        result = run("enroll", "offsite", "--url", srv.url, "--token-file", path, "-c", client)
        assert result.exit_code == 0, result.stderr

    def test_a_token_file_readable_by_everyone_warns_and_still_works(self, run, client, make_server):
        srv = make_server()
        result = do_enroll(run, client, srv, srv.token(), name="offsite", fname="open")
        assert result.exit_code == 0
        assert "readable by everyone" not in result.stderr          # the default is 0600
        srv2 = make_server()
        path = token_file(client.parent.parent / "tokens", srv2.token(), mode=0o644, name="open2")
        result = run("enroll", "other", "--url", srv2.url, "--token-file", path, "-c", client)
        assert result.exit_code == 0, result.stderr
        assert result.stderr.count("readable by everyone") == 1
        assert str(path) in result.stderr

    def test_token_dash_reads_stdin(self, run, client, make_server):
        srv = make_server()
        token = srv.token()
        result = run("enroll", "offsite", "--url", srv.url, "--token", "-", "-c", client,
                     input=token + "\n")
        assert result.exit_code == 0, result.stderr
        assert dest(client).server_fingerprint == srv.fp
        assert token not in everything(result)

    def test_token_with_any_other_value_is_a_usage_error_that_does_not_echo_it(
        self, run, client, reloads
    ):
        token = make_foreign_token(types.SimpleNamespace(fp="ab" * 32))
        before = db_state(client)
        result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "--token", token,
                     "-c", client)
        assert result.exit_code == 2
        assert token not in everything(result) and secret_of(token) not in everything(result)
        assert "--token takes only '-'" in result.stderr
        untouched(client, before, reloads)
        assert not tls_of(client).exists()

    def test_both_inputs_are_a_usage_error(self, run, client, reloads):
        path = token_file(client.parent.parent, "x")
        before = db_state(client)
        result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "--token-file", path,
                     "--token", "-", "-c", client)
        assert result.exit_code == 2 and "not both" in result.stderr
        untouched(client, before, reloads)

    def test_no_option_and_no_terminal_is_a_usage_error(self, run, client, reloads):
        before = db_state(client)
        result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "-c", client)
        assert result.exit_code == 2
        assert "no enrollment token" in result.stderr and "no terminal" in result.stderr
        untouched(client, before, reloads)
        assert not tls_of(client).exists()

    @pytest.mark.parametrize("extra", [(), ("--token", "-")], ids=["prompt", "token-dash"])
    def test_on_a_terminal_it_prompts_and_does_not_echo(self, run, client, make_server,
                                                       monkeypatch, extra):
        srv = make_server()
        token = srv.token()
        monkeypatch.setattr(
            enroll_cli, "sys", types.SimpleNamespace(stdin=types.SimpleNamespace(isatty=lambda: True))
        )
        asked = []
        real_prompt = enroll_cli.click.prompt

        def prompt(*args, **kwargs):
            asked.append(kwargs)
            return real_prompt(*args, **kwargs)

        monkeypatch.setattr(enroll_cli.click, "prompt", prompt)
        result = run("enroll", "offsite", "--url", srv.url, *extra, "-c", client,
                     input=token + "\n")
        assert result.exit_code == 0, result.stderr
        (kwargs,) = asked
        assert kwargs["hide_input"] is True and kwargs["err"] is True
        assert "Enrollment token" in result.stderr
        assert token not in everything(result)

    def test_empty_stdin_is_a_malformed_token(self, run, client, reloads):
        before = db_state(client)
        result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "--token", "-",
                     "-c", client, input="")
        assert result.exit_code == 1 and "malformed enrollment token" in result.stderr
        untouched(client, before, reloads)

    @pytest.mark.parametrize("text, fragment", [
        ("hello world", "not a zfsbackup enrollment token"),
        ("zfsb1.short.PLANTED-secret-text", "malformed enrollment token"),
        ("zfsb1." + "A" * 64 + "." + "B" * 43, "malformed enrollment token"),    # fp not hex-lower
    ])
    def test_a_bad_token_is_refused_without_repeating_it(self, run, client, reloads, text, fragment):
        before = db_state(client)
        result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "--token", "-",
                     "-c", client, input=text + "\n")
        assert result.exit_code == 1 and fragment in result.stderr
        for piece in ("PLANTED-secret-text", "hello world", "B" * 43):
            assert piece not in everything(result)
        untouched(client, before, reloads)
        assert not tls_of(client).exists()

    def test_a_too_long_stdin_is_refused(self, run, client, reloads):
        before = db_state(client)
        result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "--token", "-",
                     "-c", client, input="x" * 5000)
        assert result.exit_code == 1 and "far too long" in result.stderr
        untouched(client, before, reloads)

    def test_a_missing_token_file(self, run, client, reloads):
        before = db_state(client)
        result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "--token-file",
                     client.parent / "nope", "-c", client)
        assert result.exit_code == 1 and "cannot read the token file" in result.stderr
        untouched(client, before, reloads)

    def test_a_directory_is_not_a_token_file(self, run, client, reloads):
        before = db_state(client)
        result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "--token-file",
                     client.parent, "-c", client)
        assert result.exit_code == 1 and "not a regular file" in result.stderr
        untouched(client, before, reloads)

    def test_a_huge_or_binary_token_file_is_refused(self, run, client, reloads):
        before = db_state(client)
        huge = client.parent.parent / "huge"
        huge.write_bytes(b"x" * 5000)
        binary = client.parent.parent / "binary"
        binary.write_bytes(b"\xff\xfe\x00zfsb1")
        for path, fragment in ((huge, "far too large"), (binary, "does not hold an enrollment token")):
            result = run("enroll", "offsite", "--url", "https://127.0.0.1:1", "--token-file",
                         path, "-c", client)
            assert result.exit_code == 1 and fragment in result.stderr
        untouched(client, before, reloads)


# --------------------------------------------------------------------------
# Inputs are checked before anything is generated, written or sent
# --------------------------------------------------------------------------


class TestInputsAreCheckedFirst:
    @pytest.fixture
    def fresh(self, run, db, reloads, make_server):
        """No client id stored: anything that got past the checks would generate one."""
        make_db(run, db, PLAIN_YAML)
        reloads.clear()
        srv = make_server()
        return types.SimpleNamespace(db=db, srv=srv, token=srv.token(), before=db_state(db),
                                     reloads=reloads)

    def check_nothing_happened(self, f):
        assert db_state(f.db) == f.before
        assert host_client_id(f.db) is None
        assert not tls_of(f.db).exists()
        assert f.reloads == []
        assert f.srv.log == []

    def test_a_new_destination_needs_a_url(self, run, fresh):
        result = do_enroll(run, fresh.db, fresh.srv, fresh.token, url=False)
        assert result.exit_code == 2 and "pass --url https://HOST:PORT" in result.stderr
        self.check_nothing_happened(fresh)

    @pytest.mark.parametrize("url, fragment", [
        ("http://127.0.0.1:8080", "plaintext http"),
        ("ftp://host", "destinations must be https:// URLs"),
        ("https://", "no host"),
        ("host:8080", "destinations must be https:// URLs"),
        ("https://h:0", "has port 0; use a port from 1 to 65535"),
        ("https://h:", "has an empty port after ':'"),
        ("https://h:99999", "is not a valid URL"),
        ("https://h:abc", "is not a valid URL"),
    ])
    def test_a_url_that_is_not_https(self, run, fresh, url, fragment):
        result = do_enroll(run, fresh.db, url, fresh.token)
        assert result.exit_code == 1 and fragment in result.stderr
        self.check_nothing_happened(fresh)

    @pytest.mark.parametrize("label", ["bad\x07label", "line\nbreak", "x" * 129])
    def test_a_bad_label(self, run, fresh, label):
        result = do_enroll(run, fresh.db, fresh.srv, fresh.token, "--label", label)
        assert result.exit_code == 1 and "--label must be printable text" in result.stderr
        self.check_nothing_happened(fresh)

    def test_a_label_of_exactly_128_characters_is_accepted(self, run, fresh):
        result = do_enroll(run, fresh.db, fresh.srv, fresh.token, "--label", "L" * 128)
        assert result.exit_code == 0, result.stderr
        (record,) = fresh.srv.clients().values()
        assert record.label == "L" * 128


# --------------------------------------------------------------------------
# The happy path
# --------------------------------------------------------------------------


class TestEnroll:
    def test_a_new_destination_end_to_end(self, run, client, make_server, reloads):
        srv = make_server()
        token = srv.token()
        gen = generation(client)
        result = do_enroll(run, client, srv, token)
        assert result.exit_code == 0, result.stderr
        # what the operator is told
        assert f"enrolled destination 'offsite' ({srv.url}) as client id {CID}" in result.stdout
        ours = client_cert(client)
        assert f"client certificate: {ours.fingerprint}" in result.stdout
        assert f"server fingerprint: {srv.fp}" in result.stdout
        assert "Traceback" not in everything(result)
        # what the client stored
        d = dest(client)
        assert d.url == srv.url and d.server_fingerprint == srv.fp
        assert generation(client) == gen + 1 and host_client_id(client) == CID
        # what the server stored: one client, one pin (our certificate), the token spent
        (record,) = srv.clients().values()
        assert record.client_id == CID
        assert [p.fingerprint for p in record.pins] == [ours.fingerprint]
        assert srv.token_state() != "active"
        # what crossed the wire: one anonymous POST, the whole token and nothing else secret
        assert srv.paths() == ["/enroll"]
        (seen,) = srv.log
        assert seen.method == "POST" and seen.cert is False
        body = seen.json
        assert body["version"] == 1 and body["client_id"] == CID
        assert body["token_secret"] == token and body["cert_pem"] == ours.cert_pem
        assert "label" not in body
        assert set(body) == {"version", "client_id", "token_secret", "cert_pem", "signature"}
        # one committed write, one reload request, at the written key
        assert len(reloads) == 1
        skip, held = reloads[0]
        assert skip == held == store_key(client)

    def test_the_client_identity_is_generated_with_a_private_key(self, run, client, make_server):
        srv = make_server()
        assert not tls_of(client).exists()
        assert do_enroll(run, client, srv, srv.token()).exit_code == 0
        key = tls_of(client) / "client.key"
        assert stat.S_IMODE(key.stat().st_mode) == 0o600
        assert (tls_of(client) / "client.crt").exists()
        assert "BEGIN" not in everything(do_enroll(run, client, srv, srv.token(), fname="t2"))

    def test_a_generated_client_id_is_stored_and_one_reload_covers_both_writes(
        self, run, db, reloads, make_server
    ):
        make_db(run, db, PLAIN_YAML)
        reloads.clear()
        assert host_client_id(db) is None
        srv = make_server()
        gen = generation(db)
        result = do_enroll(run, db, srv, srv.token())
        assert result.exit_code == 0, result.stderr
        generated = host_client_id(db)
        assert generated and identity.client_id_problem(generated) is None
        assert f"as client id {generated}" in result.stdout
        assert f"(generated this host's client id {generated})" in result.stdout
        assert generation(db) == gen + 2                 # the id, then the destination
        assert len(reloads) == 1                         # ...announced once, for the last write
        skip, held = reloads[0]
        assert skip == held == store_key(db)
        assert result.stdout.count("config updated (generation") == 1
        (record,) = srv.clients().values()
        assert record.client_id == generated

    def test_a_label_reaches_the_server(self, run, client, make_server):
        srv = make_server()
        assert do_enroll(run, client, srv, srv.token(), "--label", "laptop of ann").exit_code == 0
        assert srv.clients()[CID].label == "laptop of ann"
        assert srv.log[0].json["label"] == "laptop of ann"

    def test_an_existing_destination_needs_no_url(self, run, db, reloads, make_server):
        srv = make_server()
        client_with(run, db, reloads, {"offsite": (srv.url, None)})
        result = do_enroll(run, db, srv, srv.token(), url=False)
        assert result.exit_code == 0, result.stderr
        assert dest(db).url == srv.url and dest(db).server_fingerprint == srv.fp

    def test_a_url_option_replaces_the_stored_url(self, run, db, reloads, make_server):
        srv = make_server()
        client_with(run, db, reloads, {"offsite": ("https://127.0.0.1:1", None)})
        result = do_enroll(run, db, srv, srv.token())
        assert result.exit_code == 0, result.stderr
        assert dest(db).url == srv.url

    def test_other_destinations_and_datasets_are_left_alone(self, run, db, reloads, make_server):
        srv = make_server()
        client_with(run, db, reloads, {"spare": ("https://spare:8080", None)})
        before = stored(db)
        assert do_enroll(run, db, srv, srv.token()).exit_code == 0
        after = stored(db)
        assert after.destinations["spare"] == before.destinations["spare"]
        assert [d.name for d in after.datasets] == [d.name for d in before.datasets]
        assert set(after.destinations) == {"spare", "offsite"}

    def test_two_servers_share_one_client_certificate_and_id(self, run, client, make_server):
        a, b = make_server(), make_server()
        assert do_enroll(run, client, a, a.token(), name="a").exit_code == 0
        first = client_cert(client).fingerprint
        assert do_enroll(run, client, b, b.token(), name="b").exit_code == 0
        assert client_cert(client).fingerprint == first       # not regenerated
        for srv in (a, b):
            (record,) = srv.clients().values()
            assert record.client_id == CID and [p.fingerprint for p in record.pins] == [first]
        assert {d.server_fingerprint for d in stored(client).destinations.values()} == {a.fp, b.fp}

    def test_configured_certificate_files_are_used_and_never_generated(
        self, run, db, reloads, make_server, tmp_path
    ):
        mine = tmp_path / "mine"
        ours = identity.ensure_identity(mine, identity.ROLE_CLIENT)
        make_db(run, db, PLAIN_YAML + f'client_cert_file: "{ours.cert_path}"\n'
                                      f'client_key_file: "{ours.key_path}"\n')
        seed_client_id(db, CID)
        reloads.clear()
        srv = make_server()
        result = do_enroll(run, db, srv, srv.token())
        assert result.exit_code == 0, result.stderr
        assert [p.fingerprint for p in srv.clients()[CID].pins] == [ours.fingerprint]
        assert not tls_of(db).exists()                         # nothing generated for the default dir

    def test_a_dry_run_server_refuses_with_its_own_wording(self, run, client, make_server):
        srv = make_server(dry_run=True)
        before = db_state(client)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 1
        assert "dry-run server" in result.stderr
        assert db_state(client) == before

    def test_a_server_with_remote_backup_off_refuses(self, run, client, make_server):
        srv = make_server(remote=False)
        before = db_state(client)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 1
        assert "cannot do /enroll right now (HTTP 503)" in result.stderr
        assert db_state(client) == before


# --------------------------------------------------------------------------
# Idempotency: GET /whoami first
# --------------------------------------------------------------------------


class TestIdempotency:
    @pytest.fixture
    def enrolled(self, run, client, make_server, reloads):
        srv = make_server()
        token = srv.token()
        assert do_enroll(run, client, srv, token).exit_code == 0
        reloads.clear()
        srv.log.clear()
        return types.SimpleNamespace(srv=srv, token=token, db=client, reloads=reloads)

    def test_a_rerun_with_the_same_token_skips_the_enroll_and_changes_nothing(self, run, enrolled):
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token)
        assert result.exit_code == 0, result.stderr
        assert "already enrolled" in result.stdout and "the token was not used" in result.stdout
        assert "destination 'offsite' is already recorded; nothing to change" in result.stdout
        assert enrolled.srv.paths() == ["/whoami"]                 # and no /enroll
        assert enrolled.srv.log[0].cert is True and enrolled.srv.log[0].body == b""
        untouched(enrolled.db, before, enrolled.reloads)

    def test_a_rerun_with_a_fresh_token_does_not_spend_it(self, run, enrolled):
        fresh = enrolled.srv.token()
        assert enrolled.srv.token_state() == "active"
        result = do_enroll(run, enrolled.db, enrolled.srv, fresh, fname="fresh")
        assert result.exit_code == 0, result.stderr
        assert enrolled.srv.token_state() == "active"             # still usable by someone else
        assert enrolled.srv.paths() == ["/whoami"]
        assert fresh not in everything(result)

    def test_a_second_name_for_the_same_server_is_recorded_without_the_token(self, run, enrolled):
        before = generation(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token, name="mirror")
        assert result.exit_code == 0, result.stderr
        assert "the token was not used" in result.stdout
        assert "already recorded" not in result.stdout            # it IS a change
        assert dest(enrolled.db, "mirror").server_fingerprint == enrolled.srv.fp
        assert generation(enrolled.db) == before + 1
        assert len(enrolled.reloads) == 1
        assert enrolled.srv.paths() == ["/whoami"]

    def test_a_different_client_id_names_both_ids_and_changes_nothing(self, run, enrolled):
        seed_client_id(enrolled.db, "other-1")
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token)
        assert result.exit_code == 1
        assert f"enrolled as client id '{CID}'" in result.stderr
        assert "this host's client id is 'other-1'" in result.stderr
        assert "Nothing was changed" in result.stderr
        assert enrolled.srv.paths() == ["/whoami"]
        untouched(enrolled.db, before, enrolled.reloads)

    def test_a_token_for_another_server_than_the_one_that_trusts_us(self, run, enrolled, make_server):
        other = make_server()
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, other.token(), url=False)
        assert result.exit_code == 1
        assert f"fingerprint {enrolled.srv.fp}" in result.stderr and other.fp in result.stderr
        assert "issued for a different server" in result.stderr
        assert other.log == []                                    # the other server was never asked
        assert enrolled.srv.paths() == ["/whoami"]
        untouched(enrolled.db, before, enrolled.reloads)

    def test_a_stale_pin_with_a_valid_token_warns_and_repins(self, run, db, reloads, make_server):
        srv = make_server()
        client_with(run, db, reloads, {"offsite": (srv.url, BOGUS_FP)})
        result = do_enroll(run, db, srv, srv.token(), url=False)
        assert result.exit_code == 0, result.stderr
        warned = [l for l in result.stderr.splitlines() if "Re-pinning" in l]
        assert len(warned) == 1 and BOGUS_FP in warned[0] and srv.fp in warned[0]
        assert dest(db).server_fingerprint == srv.fp
        assert srv.paths() == ["/enroll"]

    def test_a_stale_pin_on_an_already_enrolled_host_is_repinned_without_the_token(
        self, run, enrolled
    ):
        assert run("set", "destinations.offsite.server_fingerprint", BOGUS_FP,
                   "-c", enrolled.db).exit_code == 0
        enrolled.reloads.clear()
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token)
        assert result.exit_code == 0, result.stderr
        assert dest(enrolled.db).server_fingerprint == enrolled.srv.fp
        assert enrolled.srv.paths() == ["/whoami"]
        assert len(enrolled.reloads) == 1

    # (revoked certificate + unscoped/scoped token: pending the store's revoked-fingerprint
    #  memory and `--replace` changes; added once the final behaviour is announced)

    @pytest.mark.parametrize("effect, code, fragment", [
        (Reply(500, {"error": "boom"}), 1, "answered HTTP 500 to /whoami: boom"),
        (Reply(503, {"error": "busy", "retry": True}), 75, "busy (HTTP 503)"),
        (Reply(404, {}), 1, "does not look like a zfsbackup server"),
        (Reply(200, {"client_id": 7}), 1, "unexpected body"),
        (Reply(200, {"client_id": CID, "fingerprint": "ee" * 32}), 1, "reports certificate"),
        (Reply(200, "<html>nope</html>"), 1, "unexpected body"),
    ], ids=["500", "503-retry", "404", "bad-body", "wrong-fingerprint", "html"])
    def test_a_whoami_answer_that_is_not_trust_or_distrust(self, run, enrolled, effect, code, fragment):
        enrolled.srv.on("/whoami", effect)
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token)
        assert result.exit_code == code, result.stderr
        assert fragment in result.stderr
        assert enrolled.srv.seen("/enroll") == []                 # the token is not risked
        untouched(enrolled.db, before, enrolled.reloads)

    @pytest.mark.parametrize("status", [401, 403])
    def test_a_whoami_401_or_403_means_not_trusted_so_the_token_is_used(self, run, enrolled, status):
        # the real app answers /enroll: the certificate is bound under HOST-1 already, the
        # token is unscoped -> `cert_bound_elsewhere`? No: same id, so ALREADY_TRUSTED.
        enrolled.srv.on("/whoami", Reply(status, {"error": "client not trusted"}))
        fresh = enrolled.srv.token()
        result = do_enroll(run, enrolled.db, enrolled.srv, fresh, fname="fresh")
        assert result.exit_code == 0, result.stderr
        assert [e.path for e in enrolled.srv.log] == ["/whoami", "/enroll"]
        assert enrolled.srv.token_state() == "active"            # already_trusted: not spent

    def test_a_reset_right_after_the_handshake_is_retried_once(self, run, enrolled):
        enrolled.srv.on("/whoami", RESET)                         # first request only
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token)
        assert result.exit_code == 0, result.stderr
        assert [e.path for e in enrolled.srv.log] == ["/whoami", "/whoami"]
        assert "the token was not used" in result.stdout
        untouched(enrolled.db, before, enrolled.reloads)

    def test_two_resets_in_a_row_are_read_as_untrusted_and_the_enroll_decides(self, run, enrolled):
        """A TLS 1.3 rejection sometimes surfaces as a reset: one that survives the retry is
        taken as "this certificate is not trusted", and `/enroll` decides. The server here DOES
        trust us, so a live token is answered `already_trusted` and is not spent."""
        enrolled.srv.on("/whoami", RESET, RESET)
        fresh = enrolled.srv.token()
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, fresh, fname="fresh")
        assert result.exit_code == 0, result.stderr
        assert [e.path for e in enrolled.srv.log] == ["/whoami", "/whoami", "/enroll"]
        assert enrolled.srv.token_state() == "active"
        assert "the token was not used" in result.stdout
        untouched(enrolled.db, before, enrolled.reloads)

    def test_two_resets_with_a_spent_token_end_in_the_refusal_not_in_a_silent_success(
        self, run, enrolled
    ):
        enrolled.srv.on("/whoami", RESET, RESET)
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token)
        assert result.exit_code == 1
        assert "refused the token" in result.stderr
        untouched(enrolled.db, before, enrolled.reloads)

    def test_a_server_that_is_really_down_fails_at_enroll_with_the_token_unspent(
        self, run, enrolled
    ):
        enrolled.srv.on("/whoami", RESET, RESET).on("/enroll", RESET)
        fresh = enrolled.srv.token()
        result = do_enroll(run, enrolled.db, enrolled.srv, fresh, fname="fresh")
        assert result.exit_code == 75, result.stderr
        assert enrolled.srv.token_state() == "active"

    def test_a_first_time_enroll_is_not_stopped_by_a_whoami_that_always_resets(
        self, run, client, make_server, monkeypatch
    ):
        srv = make_server()
        real = enroll_cli._Server.request

        def request(self, method, path, body=None):
            if path == "/whoami":
                raise enroll_cli._TransportError("unreachable", "reset", reset=True)
            return real(self, method, path, body)

        monkeypatch.setattr(enroll_cli._Server, "request", request)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 0, result.stderr
        assert srv.paths() == ["/enroll"]
        assert dest(client).server_fingerprint == srv.fp


# --------------------------------------------------------------------------
# The token is never sent to a server that is not the one it names
# --------------------------------------------------------------------------


class TestPinMismatch:
    WRONG = "ee" * 32

    def wrong_token(self):
        token, _ = identity.make_token(self.WRONG)
        return str(token)

    def test_a_token_for_another_server_is_not_sent(self, run, client, reloads, make_server):
        srv = make_server()
        before = db_state(client)
        token = self.wrong_token()
        result = do_enroll(run, client, srv, token)
        assert result.exit_code == 1
        assert self.WRONG in result.stderr and srv.fp in result.stderr          # both named
        assert "The token was NOT sent." in result.stderr
        assert srv.log == [], f"the server saw {srv.log}"                       # not one HTTP byte
        assert token not in everything(result) and secret_of(token) not in everything(result)
        untouched(client, before, reloads)

    def test_a_stale_stored_pin_equal_to_the_token_fingerprint_is_the_same_refusal(
        self, run, db, reloads, make_server
    ):
        srv = make_server()
        client_with(run, db, reloads, {"offsite": (srv.url, self.WRONG)})
        before = db_state(db)
        result = do_enroll(run, db, srv, self.wrong_token(), url=False)
        assert result.exit_code == 1 and "The token was NOT sent." in result.stderr
        assert srv.log == []
        untouched(db, before, reloads)

    def test_the_destination_is_not_recorded_with_the_wrong_fingerprint(self, run, client, make_server):
        srv = make_server()
        do_enroll(run, client, srv, self.wrong_token())
        assert dest(client) is None

    def test_a_mismatch_with_an_unknown_presented_fingerprint_still_names_the_expected_one(
        self, run, client, make_server, monkeypatch
    ):
        srv = make_server()
        monkeypatch.setattr(enroll_cli, "_probe_presented", lambda host, port: None)
        monkeypatch.setattr(enroll_cli, "_presented_fingerprint", lambda exc: None)
        result = do_enroll(run, client, srv, self.wrong_token())
        assert result.exit_code == 1
        assert "a different certificate" in result.stderr and self.WRONG in result.stderr


# --------------------------------------------------------------------------
# Every answer of the server
# --------------------------------------------------------------------------


class TestServerAnswers:
    def test_an_unknown_token_is_refused(self, run, client, make_server, reloads):
        srv = make_server()
        before = db_state(client)
        result = do_enroll(run, client, srv, make_foreign_token(srv))
        assert result.exit_code == 1
        assert "refused the token" in result.stderr
        assert "zfsbackup-config trust token create" in result.stderr
        assert srv.paths() == ["/enroll"]
        untouched(client, before, reloads)

    def test_an_expired_token_is_refused_the_same_way(self, run, client, make_server):
        srv = make_server()
        result = do_enroll(run, client, srv, srv.token(ttl=0.5))
        assert result.exit_code == 1 and "refused the token" in result.stderr

    def test_a_used_token_is_refused_for_another_host(self, run, client, make_server, db, tmp_path,
                                                      reloads):
        srv = make_server()
        token = srv.token()
        assert do_enroll(run, client, srv, token).exit_code == 0
        second = tmp_path / "second" / "config.db"
        make_db(run, second, PLAIN_YAML)
        seed_client_id(second, "host-2")
        reloads.clear()
        result = do_enroll(run, second, srv, token)
        assert result.exit_code == 1 and "refused the token" in result.stderr
        assert dest(second) is None

    def test_a_token_scoped_to_another_id_is_refused(self, run, client, make_server):
        srv = make_server()
        srv.pin(identity.ensure_identity(client.parent.parent / "someone.tls", "client"), "someone")
        result = do_enroll(run, client, srv, srv.token(client_id="someone"))
        assert result.exit_code == 1 and "refused the token" in result.stderr

    def test_client_id_taken_explains_the_fix_and_keeps_the_token_valid(
        self, run, client, make_server, reloads, tmp_path
    ):
        srv = make_server()
        srv.pin(identity.ensure_identity(tmp_path / "other.tls", "client"), CID)   # CID is taken
        token = srv.token()
        before = db_state(client)
        result = do_enroll(run, client, srv, token)
        assert result.exit_code == 1
        assert f"already knows a client with id '{CID}'" in result.stderr
        assert "the token was not used and is still valid" in result.stderr
        assert f"zfsbackup-config trust token create --client-id {CID}" in result.stderr
        assert "--replace" in result.stderr
        assert srv.token_state() == "active"
        untouched(client, before, reloads)
        # the operator's fix: a token scoped to the id, replacing the other certificate
        fixed = srv.token(client_id=CID, replace=True)
        assert do_enroll(run, client, srv, fixed, fname="fixed").exit_code == 0

    def test_a_certificate_bound_to_another_id_says_how_to_resolve_it(
        self, run, client, make_server, reloads
    ):
        srv = make_server()
        ours = identity.ensure_identity(tls_of(client), identity.ROLE_CLIENT)
        srv.pin(ours, "elsewhere")                                   # OUR certificate, another id
        token = srv.token()
        srv.on("/whoami", Reply(403, {"error": "client not trusted"}))   # hide it from the probe
        before = db_state(client)
        result = do_enroll(run, client, srv, token)
        assert result.exit_code == 1
        assert "already has this host's certificate enrolled under a different client id" in result.stderr
        assert "identity set-client-id" in result.stderr and "trust revoke" in result.stderr
        assert "enroll <destination> --new-key" in result.stderr           # revoke, then a NEW key
        assert "a revoked certificate cannot be enrolled again" in result.stderr
        untouched(client, before, reloads)

    SCRIPTED = [
        # (id, reply, exit code, fragments that must be in stderr)
        ("bad_signature", Reply(403, {"error": "sig", "reason": "bad_signature"}), 1,
         ["could not verify this host's proof of possession", "not a problem with the token"]),
        ("400", Reply(400, {"error": "client_id missing"}), 1,
         ["rejected the enrollment request (HTTP 400): client_id missing"]),
        ("413", Reply(413, {"error": "too big"}), 1, ["(HTTP 413): too big"]),
        ("422", Reply(422, {"error": "certificate not acceptable: expired", "reason": "bad_certificate"}), 1,
         ["(HTTP 422): certificate not acceptable: expired"]),
        ("404", Reply(404, {}), 1, ["answered HTTP 404 for /enroll", "does not look like a zfsbackup server"]),
        ("405", Reply(405, {}), 1, ["answered HTTP 405 for /enroll"]),
        ("401", Reply(401, {"error": "client certificate required"}), 1, ["refused /enroll (HTTP 401)"]),
        ("500", Reply(500, {"error": "boom"}), 1, ["answered HTTP 500 to /enroll: boom"]),
        ("503-retry", Reply(503, {"error": "busy", "retry": True, "reason": "busy"}), 75,
         ["is busy (HTTP 503)", "retry in a moment"]),
        ("503-no-retry", Reply(503, {"error": "off", "reason": "unavailable"}), 1,
         ["cannot do /enroll right now (HTTP 503)"]),
        ("503-dry-run", Reply(503, {"error": "dry", "reason": "dry_run"}), 1,
         ["is a dry-run server and does not accept /enroll"]),
        ("502", Reply(502, "<html>bad gateway</html>"), 75, ["not reachable through its proxy (HTTP 502)"]),
        ("504", Reply(504, ""), 75, ["not reachable through its proxy (HTTP 504)"]),
        ("302", Reply(302, "", headers={"Location": "http://elsewhere/enroll"}), 1,
         ["answered HTTP 302 to /enroll"]),
        ("html-body", Reply(418, "<html>teapot</html>"), 1, ["answered HTTP 418 to /enroll"]),
        ("json-list", Reply(409, ["x"]), 1, ["answered HTTP 409 to /enroll"]),
        ("200-other-client", Reply(200, {"status": "enrolled", "client_id": "intruder",
                                          "fingerprint": "ab" * 32}), 1,
         ["not this host's; nothing was recorded"]),
        ("200-unknown-status", Reply(200, {"status": "maybe"}), 1, ["answered HTTP 200 to /enroll"]),
    ]

    @pytest.mark.parametrize("reply, code, fragments", [s[1:] for s in SCRIPTED],
                             ids=[s[0] for s in SCRIPTED])
    def test_each_scripted_answer(self, run, client, make_server, reloads, reply, code, fragments):
        srv = make_server().on("/enroll", reply)
        before = db_state(client)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == code, result.stderr
        for fragment in fragments:
            assert fragment in result.stderr, result.stderr
        assert len(srv.seen("/enroll")) == 1                          # never retried: POST
        assert dest(client) is None                                   # nothing recorded
        untouched(client, before, reloads)

    def test_server_text_is_sanitised_redacted_and_capped(self, run, client, make_server):
        srv = make_server()
        token = srv.token()
        evil = f"bad \x1b[31mred\x07 line1\nline2 {token} and {secret_of(token)} " + "z" * 4000
        srv.on("/enroll", Reply(400, {"error": evil}))
        result = do_enroll(run, client, srv, token)
        assert result.exit_code == 1
        out = everything(result)
        assert "\x1b" not in out and "\x07" not in out
        assert token not in out and secret_of(token) not in out
        assert "<redacted>" in out
        error_line = next(l for l in result.stderr.splitlines() if "rejected the enrollment" in l)
        assert "line2" in error_line                         # newline became a space, one line
        assert len(error_line) < 500

    def test_a_server_that_enrolled_us_but_names_another_certificate_records_nothing(
        self, run, client, make_server
    ):
        srv = make_server().on("/enroll", Reply(200, {"status": "enrolled", "client_id": CID,
                                                      "fingerprint": "ab" * 32}))
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 1 and "not this host's" in result.stderr
        assert dest(client) is None


# --------------------------------------------------------------------------
# The connection
# --------------------------------------------------------------------------


class TestTransport:
    def test_an_unreachable_server_is_retryable(self, run, client, reloads, make_server):
        srv = make_server()
        token = srv.token()
        before = db_state(client)
        result = do_enroll(run, client, f"https://127.0.0.1:{closed_port()}", token)
        assert result.exit_code == 75
        assert "cannot reach https://127.0.0.1:" in result.stderr
        untouched(client, before, reloads)

    def test_a_server_that_does_not_answer_in_time_is_retryable(self, run, client, reloads,
                                                                make_server, monkeypatch):
        monkeypatch.setattr(enroll_cli, "_TIMEOUT", (2.0, 0.5))
        srv = make_server().on("/enroll", HANG)
        before = db_state(client)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 75 and "did not answer in time" in result.stderr
        assert len(srv.seen("/enroll")) == 1
        untouched(client, before, reloads)

    def test_a_peer_that_never_completes_the_handshake_is_retryable(self, run, client, reloads,
                                                                    monkeypatch):
        monkeypatch.setattr(enroll_cli, "_TIMEOUT", (0.5, 0.5))
        token, _ = identity.make_token("ab" * 32)
        before = db_state(client)
        with silent_server(hold=2.0) as peer:
            result = do_enroll(run, client, peer.url, token)
        assert result.exit_code == 75, result.stderr
        untouched(client, before, reloads)

    def test_a_plaintext_http_server_is_not_a_zfsbackup_api(self, run, client, reloads):
        token, _ = identity.make_token("ab" * 32)
        before = db_state(client)
        with plain_http_server() as peer:
            result = do_enroll(run, client, peer.url, token)
        assert result.exit_code == 1, result.stderr
        assert "TLS failure" in result.stderr and "is it a zfsbackup HTTPS API?" in result.stderr
        assert str(token) not in everything(result)
        untouched(client, before, reloads)

    def test_a_reset_on_the_enroll_post_is_never_retried(self, run, client, make_server, reloads):
        srv = make_server().on("/enroll", RESET)
        before = db_state(client)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 75, result.stderr
        assert len(srv.seen("/enroll")) == 1                    # the token may have been spent
        untouched(client, before, reloads)

    def test_an_http_url_never_reaches_the_transport(self, run, client, reloads):
        token, _ = identity.make_token("ab" * 32)
        before = db_state(client)
        result = do_enroll(run, client, "http://127.0.0.1:1", token)
        assert result.exit_code == 1 and "plaintext http" in result.stderr
        untouched(client, before, reloads)


# --------------------------------------------------------------------------
# A close without a TLS alert is a reset, not a TLS failure
# --------------------------------------------------------------------------


def _eof() -> ssl.SSLEOFError:
    return ssl.SSLEOFError(8, "EOF occurred in violation of protocol (_ssl.c:1016)")


def _verify_failure() -> ssl.SSLCertVerificationError:
    return ssl.SSLCertVerificationError(1, "certificate verify failed: self-signed certificate")


def _alert(name: str) -> ssl.SSLError:
    err = ssl.SSLError(1, f"[SSL: {name}] ssl/tls alert")
    err.reason = name
    return err


def _max_retry(reason) -> urllib3.exceptions.MaxRetryError:
    return urllib3.exceptions.MaxRetryError(None, "/whoami", reason=reason)


def _requests_ssl_error(inner) -> requests.exceptions.SSLError:
    return requests.exceptions.SSLError(inner)


def _caused_by(outer: BaseException, cause: BaseException) -> BaseException:
    outer.__cause__ = cause
    return outer


def _in_context(outer: BaseException, inner: BaseException) -> BaseException:
    """`outer`, raised while `inner` was being handled (so `inner` is its `__context__`)."""
    try:
        try:
            raise inner
        except ssl.SSLError:
            raise outer
    except BaseException as exc:  # noqa: BLE001 - the chain is the point
        return exc


#: The same SSLEOFError, reached the ways requests / urllib3 / the stdlib carry it.
EOF_CHAINS = {
    "bare": lambda: _eof(),
    "requests-args": lambda: _requests_ssl_error(_eof()),
    "requests-maxretry-reason-urllib3-args": lambda: _requests_ssl_error(
        _max_retry(urllib3.exceptions.SSLError(_eof()))
    ),
    "maxretry-reason-is-the-eof": lambda: _requests_ssl_error(_max_retry(_eof())),
    "urllib3-ssl-error-args": lambda: _requests_ssl_error(urllib3.exceptions.SSLError(_eof())),
    "cause": lambda: _caused_by(requests.exceptions.SSLError("wrapped"), _eof()),
    "context": lambda: _in_context(requests.exceptions.SSLError("wrapped"), _eof()),
    "oserror-with-cause": lambda: _caused_by(OSError("connection problem"), _eof()),
}


class TestSslEof:
    @pytest.mark.parametrize("name", list(EOF_CHAINS))
    def test_classify_reads_an_ssl_eof_as_a_reset(self, name):
        failure = enroll_cli._classify(EOF_CHAINS[name]())
        assert (failure.kind, failure.reset) == ("unreachable", True)

    @pytest.mark.parametrize("name", list(EOF_CHAINS))
    def test_is_ssl_eof_and_is_reset_both_see_it(self, name):
        exc = EOF_CHAINS[name]()
        assert enroll_cli._is_ssl_eof(exc) is True
        assert enroll_cli._is_reset(exc) is True

    def test_the_eof_is_checked_before_the_generic_tls_branch(self):
        exc = _requests_ssl_error(_max_retry(urllib3.exceptions.SSLError(_eof())))
        assert isinstance(exc, requests.exceptions.SSLError)        # it WOULD have been "tls"
        assert enroll_cli._classify(exc).kind == "unreachable"

    def test_the_reason_text_is_kept_for_the_message(self):
        failure = enroll_cli._classify(_requests_ssl_error(_eof()))
        assert "EOF occurred in violation of protocol" in failure.detail

    @pytest.mark.parametrize("build", [
        lambda: _requests_ssl_error(_max_retry(urllib3.exceptions.SSLError(_verify_failure()))),
        lambda: _requests_ssl_error(_verify_failure()),
        lambda: _verify_failure(),
        # an eof is somewhere in the chain as well: the local verification failure wins
        lambda: _caused_by(_requests_ssl_error(_verify_failure()), _eof()),
        lambda: _caused_by(_requests_ssl_error(_eof()), _verify_failure()),
        lambda: _caused_by(_requests_ssl_error(_max_retry(urllib3.exceptions.SSLError(_eof()))),
                           _verify_failure()),
    ], ids=["maxretry-verify", "args-verify", "bare-verify", "verify-over-eof-cause",
            "eof-over-verify-cause", "maxretry-eof-over-verify-cause"])
    def test_a_local_verification_failure_is_never_a_reset(self, build):
        exc = build()
        assert enroll_cli._is_ssl_eof(exc) is False
        assert enroll_cli._is_reset(exc) is False
        failure = enroll_cli._classify(exc)
        assert failure.reset is False
        if isinstance(exc, requests.exceptions.SSLError):
            assert failure.kind == "tls"

    def test_a_plain_ssl_error_without_an_eof_is_still_tls(self):
        exc = _requests_ssl_error(ssl.SSLError(1, "[SSL: WRONG_VERSION_NUMBER] wrong version number"))
        failure = enroll_cli._classify(exc)
        assert (failure.kind, failure.reset) == ("tls", False)
        assert enroll_cli._is_ssl_eof(exc) is False and enroll_cli._is_reset(exc) is False

    def test_a_pin_mismatch_keeps_its_classification(self):
        exc = _requests_ssl_error(_max_retry(urllib3.exceptions.SSLError(
            "Fingerprints did not match. Expected 'aa', got \"" + "ab" * 32 + "\""
        )))
        failure = enroll_cli._classify(exc)
        assert failure.kind == "pin" and failure.actual == "ab" * 32

    def test_a_pin_mismatch_wins_over_an_eof_in_the_same_chain(self):
        exc = _caused_by(identity.PinMismatch("aa" * 32, "bb" * 32), _eof())
        assert enroll_cli._classify(exc).kind == "pin"

    @pytest.mark.parametrize("alert", [
        "TLSV1_ALERT_UNKNOWN_CA", "TLS_ALERT_UNKNOWN_CA", "SSLV3_ALERT_BAD_CERTIFICATE",
        "TLSV1_ALERT_CERTIFICATE_EXPIRED", "TLSV1_ALERT_UNSUPPORTED_CERTIFICATE",
    ])
    def test_a_tls_alert_rejection_keeps_its_classification(self, alert):
        failure = enroll_cli._classify(_requests_ssl_error(_max_retry(_alert(alert))))
        assert (failure.kind, failure.reset) == ("rejected", False)

    def test_a_tls_alert_wins_over_an_eof_in_the_same_chain(self):
        exc = _caused_by(_requests_ssl_error(_alert("TLSV1_ALERT_UNKNOWN_CA")), _eof())
        assert enroll_cli._classify(exc).kind == "rejected"

    @pytest.mark.parametrize("exc", [
        ConnectionResetError(104, "Connection reset by peer"),
        BrokenPipeError(32, "Broken pipe"),
        _requests_ssl_error(_max_retry(urllib3.exceptions.ProtocolError(
            "Connection aborted.", ConnectionResetError(104, "reset")))),
    ], ids=["reset", "pipe", "wrapped-reset"])
    def test_the_old_resets_are_still_resets(self, exc):
        assert enroll_cli._is_reset(exc) is True
        assert enroll_cli._is_ssl_eof(exc) is False

    def test_other_errors_are_not_resets(self):
        assert enroll_cli._is_reset(requests.exceptions.ConnectTimeout("slow")) is False
        assert enroll_cli._is_reset(ConnectionRefusedError(111, "refused")) is False

    # -- the whole command, with the transport failing the way CI saw it. A real server cannot
    #    be made to fail like this on demand: whether the rejection arrives as an alert, a reset
    #    or an SSLEOFError is a race inside OpenSSL, so the adapter raises what requests raised.

    @pytest.fixture
    def eof_on(self, monkeypatch):
        """`eof_on(path, times)`: the first `times` requests for `path` fail with the exception
        requests raised on CI (SSLError > MaxRetryError > urllib3 SSLError > SSLEOFError) before
        any byte is sent. Returns the list of paths it failed."""
        import requests.adapters

        real = requests.adapters.HTTPAdapter.send
        plan = {}
        failed = []

        def send(self, request, *args, **kwargs):
            path = request.path_url
            if plan.get(path, 0) > 0:
                plan[path] -= 1
                failed.append(path)
                raise requests.exceptions.SSLError(
                    _max_retry(urllib3.exceptions.SSLError(_eof())), request=request
                )
            return real(self, request, *args, **kwargs)

        monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)

        def arm(path, times):
            plan[path] = times
            return failed

        return arm

    def test_a_first_enroll_whose_whoami_ends_in_ssl_eof_goes_on_as_untrusted(
        self, run, client, reloads, make_server, eof_on
    ):
        """The CI failure: the server turns the unpinned certificate away, the client sees
        SSLEOFError instead of an alert, and `enroll` used to exit 1."""
        srv = make_server()
        failed = eof_on("/whoami", 99)
        token = srv.token()
        gen = generation(client)
        result = do_enroll(run, client, srv, token)
        assert result.exit_code == 0, result.stderr
        assert failed == ["/whoami", "/whoami"]                       # one retry, then untrusted
        assert srv.paths() == ["/enroll"]
        assert dest(client).server_fingerprint == srv.fp
        assert generation(client) == gen + 1 and len(reloads) == 1
        assert secret_of(token) not in everything(result)

    def test_one_ssl_eof_on_whoami_is_retried_and_the_second_answer_counts(
        self, run, client, reloads, make_server, eof_on
    ):
        srv = make_server()
        assert do_enroll(run, client, srv, srv.token()).exit_code == 0
        reloads.clear()
        srv.log.clear()
        failed = eof_on("/whoami", 1)
        fresh = srv.token()
        before = db_state(client)
        result = do_enroll(run, client, srv, fresh, fname="fresh")
        assert result.exit_code == 0, result.stderr
        assert failed == ["/whoami"] and srv.paths() == ["/whoami"]   # the retry saw "trusted"
        assert "the token was not used" in result.stdout
        untouched(client, before, reloads)

    def test_two_ssl_eofs_with_a_trusting_server_end_in_already_trusted_not_in_a_spent_token(
        self, run, client, reloads, make_server, eof_on
    ):
        srv = make_server()
        assert do_enroll(run, client, srv, srv.token()).exit_code == 0
        reloads.clear()
        srv.log.clear()
        failed = eof_on("/whoami", 2)
        fresh = srv.token()
        before = db_state(client)
        result = do_enroll(run, client, srv, fresh, fname="fresh")
        assert result.exit_code == 0, result.stderr
        assert failed == ["/whoami", "/whoami"] and srv.paths() == ["/enroll"]
        assert srv.token_state() == "active"                          # already_trusted: not spent
        untouched(client, before, reloads)

    def test_ssl_eof_everywhere_is_retryable_with_the_token_unspent(
        self, run, client, reloads, make_server, eof_on
    ):
        srv = make_server()
        failed = eof_on("/whoami", 99)
        eof_on("/enroll", 99)
        before = db_state(client)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 75, result.stderr
        assert failed.count("/whoami") == 2 and failed.count("/enroll") == 1   # POST: no retry
        assert srv.log == [] and srv.token_state() == "active"
        untouched(client, before, reloads)

    def test_a_local_verification_failure_on_whoami_is_neither_retried_nor_ignored(
        self, run, client, reloads, make_server, monkeypatch
    ):
        import requests.adapters

        srv = make_server()
        real = requests.adapters.HTTPAdapter.send
        calls = []

        def send(self, request, *args, **kwargs):
            if request.path_url == "/whoami":
                calls.append(1)
                raise requests.exceptions.SSLError(
                    _max_retry(urllib3.exceptions.SSLError(_caused_by(_verify_failure(), _eof()))),
                    request=request,
                )
            return real(self, request, *args, **kwargs)

        monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
        before = db_state(client)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 1, result.stderr
        assert "TLS failure" in result.stderr
        assert calls == [1]                                           # no retry
        assert srv.paths() == []                                      # and the token was not sent
        untouched(client, before, reloads)


# --------------------------------------------------------------------------
# The token never leaves the request body
# --------------------------------------------------------------------------


class TestSecrecy:
    def assert_clean(self, text, token):
        assert token not in text, "the token leaked"
        assert secret_of(token) not in text, "the token secret leaked"
        assert not TOKEN_RE.search(text), "something shaped like a token was printed"

    @pytest.mark.parametrize("verbose", [(), ("-v",)], ids=["quiet", "verbose"])
    def test_success(self, run, client, make_server, caplog, verbose):
        caplog.set_level(0)
        srv = make_server()
        token = srv.token()
        result = do_enroll(run, client, srv, token, *verbose)
        assert result.exit_code == 0, result.stderr
        self.assert_clean(everything(result), token)
        self.assert_clean(caplog.text, token)
        # at rest on the client: not in the database (nor its sidecars), nor the TLS files
        for path in [p for p in client.parent.iterdir() if p.is_file()]:
            self.assert_clean(path.read_bytes().decode("latin-1"), token)
        for path in tls_of(client).iterdir():
            self.assert_clean(path.read_bytes().decode("latin-1"), token)
        # on the wire: exactly one place, the body of the one POST /enroll
        self.assert_clean(srv.raw_text(), token)
        bodies = [e.body.decode() for e in srv.log if token in e.body.decode()]
        assert len(bodies) == 1 and srv.log[0].path == "/enroll"

    @pytest.mark.parametrize("effect", [
        Reply(400, {"error": "x"}), Reply(403, {"reason": "refused"}), Reply(500, "boom"),
        Reply(503, {"retry": True}), RESET,
    ], ids=["400", "refused", "500", "503", "reset"])
    def test_failures_with_verbose(self, run, client, make_server, caplog, effect):
        caplog.set_level(0)
        srv = make_server().on("/enroll", effect)
        token = srv.token()
        result = do_enroll(run, client, srv, token, "-v")
        assert result.exit_code != 0
        self.assert_clean(everything(result), token)
        self.assert_clean(caplog.text, token)
        self.assert_clean(srv.raw_text(), token)

    def test_a_pin_mismatch_with_verbose_prints_no_token(self, run, client, make_server):
        srv = make_server()
        token, _ = identity.make_token("ee" * 32)
        result = do_enroll(run, client, srv, str(token), "-v")
        assert result.exit_code == 1
        self.assert_clean(everything(result), str(token))

    def test_the_exception_text_of_a_failure_has_no_token(self, run, client, make_server, monkeypatch):
        """Nothing from the HTTP stack is carried into a message: call `enroll()` itself and
        look at the exception and its whole chain."""
        srv = make_server().on("/enroll", RESET)
        token = srv.token()
        path = token_file(client.parent.parent, token)
        state = types.SimpleNamespace(
            resolve=lambda: resolve_config_path(str(client)),
            warn=lambda m: None,
        )
        with pytest.raises(Exception) as info:
            enroll_cli.enroll(state, "offsite", srv.url, None, str(path), None)
        seen, exc = [], info.value
        while exc is not None:
            seen.append(f"{exc!r} {exc}")
            exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)
        self.assert_clean("\n".join(seen), token)
        assert info.value.__cause__ is None                  # `from None`

    def test_the_secret_string_type_hides_the_token(self):
        token, _ = identity.make_token("ab" * 32)
        assert str(token) not in repr(token) and str(token) not in f"{token!r}"


# --------------------------------------------------------------------------
# Identity problems stop the run before any database write
# --------------------------------------------------------------------------


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
class TestPermissions:
    @pytest.fixture
    def fresh(self, run, db, reloads, make_server):
        make_db(run, db, PLAIN_YAML)                                  # NO client id yet
        reloads.clear()
        srv = make_server()
        return types.SimpleNamespace(db=db, srv=srv, token=srv.token(), before=db_state(db))

    def test_an_unwritable_tls_directory_stops_before_any_write(self, run, fresh, reloads):
        tls = tls_of(fresh.db)
        tls.mkdir(mode=0o750)
        tls.chmod(0o500)
        try:
            result = do_enroll(run, fresh.db, fresh.srv, fresh.token)
        finally:
            tls.chmod(0o750)
        assert result.exit_code == 1, result.stderr
        assert "no usable client identity" in result.stderr
        assert "run as root or as the owner" in result.stderr
        assert db_state(fresh.db) == fresh.before
        assert host_client_id(fresh.db) is None                       # even the id was not made
        assert reloads == [] and fresh.srv.log == []
        assert not any(p.name.startswith("client") for p in tls.iterdir())

    def test_an_unreadable_private_key_stops_before_any_write(self, run, fresh, reloads):
        ident = identity.ensure_identity(tls_of(fresh.db), identity.ROLE_CLIENT)
        ident.key_path.chmod(0o000)
        try:
            result = do_enroll(run, fresh.db, fresh.srv, fresh.token)
        finally:
            ident.key_path.chmod(0o600)
        assert result.exit_code == 1, result.stderr
        assert "error:" in result.stderr
        assert db_state(fresh.db) == fresh.before and host_client_id(fresh.db) is None
        assert reloads == [] and fresh.srv.log == []

    def test_configured_files_that_are_missing_are_never_generated(self, run, db, reloads, make_server,
                                                                   tmp_path):
        cert, key = tmp_path / "gone" / "c.crt", tmp_path / "gone" / "c.key"
        make_db(run, db, PLAIN_YAML + f'client_cert_file: "{cert}"\nclient_key_file: "{key}"\n')
        reloads.clear()
        srv = make_server()
        before = db_state(db)
        result = do_enroll(run, db, srv, srv.token())
        assert result.exit_code == 1
        assert "so no identity is generated for them" in result.stderr
        assert "unset client_cert_file" in result.stderr
        assert not cert.parent.exists()
        assert db_state(db) == before and host_client_id(db) is None
        assert reloads == [] and srv.log == []


# --------------------------------------------------------------------------
# The generated id is committed before anything can fail
# --------------------------------------------------------------------------


class TestFailureAfterTheClientId:
    @pytest.fixture
    def fresh(self, run, db, reloads, make_server):
        make_db(run, db, PLAIN_YAML)
        reloads.clear()
        return db

    @pytest.fixture
    def notified(self, monkeypatch):
        calls = []
        real = cli_write.notify_daemon

        def spy(resolved, generation, store_id=None, *, reload_wait=None, request=None):
            calls.append({"generation": generation, "store_id": store_id,
                          "reload_wait": reload_wait, "request": request})
            return real(resolved, generation, store_id, reload_wait=reload_wait, request=request)

        monkeypatch.setattr(enroll_cli, "notify_daemon", spy)
        return calls

    @pytest.mark.parametrize("how", ["refused", "unreachable", "pin", "server-error"])
    def test_one_reload_for_the_id_and_a_note_naming_it(self, run, fresh, reloads, make_server,
                                                        notified, how):
        srv = make_server()
        gen = generation(fresh)
        if how == "refused":
            url, token = srv.url, make_foreign_token(srv)
        elif how == "unreachable":
            url, token = f"https://127.0.0.1:{closed_port()}", srv.token()
        elif how == "pin":
            url, token = srv.url, str(identity.make_token("ee" * 32)[0])
        else:
            srv.on("/enroll", Reply(500, "x"))
            url, token = srv.url, srv.token()
        result = do_enroll(run, fresh, url, token)
        assert result.exit_code in (1, 75)
        generated = host_client_id(fresh)
        assert generated is not None
        assert generation(fresh) == gen + 1
        assert len(reloads) == 1
        skip, held = reloads[0]
        assert skip == held == store_key(fresh)
        assert f"generated this host's client id: {generated}" in result.stdout
        (call,) = notified
        assert call["reload_wait"] == 0 and call["request"] is not None
        assert (call["store_id"], call["generation"]) == store_key(fresh)
        assert result.stdout.count("config updated (generation") == 1
        assert dest(fresh) is None

    def test_nothing_committed_means_no_reload_and_no_note(self, run, client, reloads, notified):
        result = do_enroll(run, client, f"https://127.0.0.1:{closed_port()}", "zfsb1." + "0" * 64 + "." + "A" * 43)
        assert result.exit_code == 75
        assert reloads == [] and notified == []
        assert "generated this host's client id" not in everything(result)

    def test_the_reload_comes_before_the_failure_message_and_the_id_note(self, run, fresh, monkeypatch):
        import click

        events = []
        real_echo = click.echo

        def echo(message=None, *args, **kwargs):
            events.append(("err" if kwargs.get("err") else "out", str(message)))
            return real_echo(message, *args, **kwargs)

        def recorder(resolved, *, skip_if_applied=None):
            events.append(("reload", skip_if_applied))
            return daemonlock.ReloadRequest("no-daemon")

        monkeypatch.setattr(click, "echo", echo)
        monkeypatch.setattr(daemonlock, "request_reload", recorder)
        result = do_enroll(run, fresh, f"https://127.0.0.1:{closed_port()}",
                           "zfsb1." + "0" * 64 + "." + "A" * 43)
        assert result.exit_code == 75
        kinds = [k for k, _ in events]
        assert kinds.count("reload") == 1
        assert kinds.index("reload") < min(i for i, (k, m) in enumerate(events)
                                           if k == "out" and "generated this host's" in m)

    def test_a_success_notifies_once_with_the_default_wait(self, run, fresh, make_server, notified):
        srv = make_server()
        assert do_enroll(run, fresh, srv, srv.token()).exit_code == 0
        (call,) = notified
        assert call["reload_wait"] is None                       # waits for the ack, as every write


# --------------------------------------------------------------------------
# Step 6 loses a race after the server enrolled us
# --------------------------------------------------------------------------


class TestConflictThenRerun:
    @pytest.fixture
    def competing_write(self, monkeypatch, db):
        """Make another writer commit right after the server answered /enroll (before the
        CLI's own commit): a genuine `ConfigConflict`. Armed once."""
        state = {"armed": True}
        real = enroll_cli._enroll_with_token

        def wrapper(*args, **kwargs):
            real(*args, **kwargs)
            if state["armed"]:
                state["armed"] = False
                done = Cli()("set", "check_interval", "10m", "-c", db)
                assert done.exit_code == 0, done.stderr

        monkeypatch.setattr(enroll_cli, "_enroll_with_token", wrapper)
        return state

    def test_conflict_is_a_retry_with_a_note_and_the_rerun_completes_via_whoami(
        self, run, client, make_server, reloads, competing_write
    ):
        srv = make_server()
        token = srv.token()
        result = do_enroll(run, client, srv, token)
        assert result.exit_code == 75, result.stderr
        assert "re-run `zfsbackup-config enroll`" in result.stderr
        assert "the token is not needed again" in result.stderr
        assert srv.token_state() != "active"                        # the server DID enroll us
        assert dest(client) is None
        assert len(reloads) == 1                                    # only the competing set's
        reloads.clear()
        srv.log.clear()
        # the very same command: completes through /whoami, never POSTs again
        again = do_enroll(run, client, srv, token)
        assert again.exit_code == 0, again.stderr
        assert "the token was not used" in again.stdout
        assert srv.paths() == ["/whoami"]
        assert dest(client).server_fingerprint == srv.fp
        assert len(reloads) == 1

    def test_a_conflict_after_the_generated_id_still_reloads_once(
        self, run, db, reloads, make_server, competing_write
    ):
        make_db(run, db, PLAIN_YAML)
        reloads.clear()
        srv = make_server()

        # the competing `set` goes through the same request_reload recorder: tell them apart
        result = do_enroll(run, db, srv, srv.token())
        assert result.exit_code == 75, result.stderr
        assert "the token is not needed again" in result.stderr
        generated = host_client_id(db)
        assert f"generated this host's client id: {generated}" in result.stdout
        # two reload requests in all: the competing `set`, and enroll's one for the generated id
        assert len(reloads) == 2

    def test_a_busy_database_at_step_6_is_also_a_retry_with_the_note(
        self, run, client, make_server, reloads, monkeypatch
    ):
        srv = make_server()

        def busy(*args, **kwargs):
            raise ConfigBusy("the configuration database is busy")

        monkeypatch.setattr(enroll_cli, "commit_config", busy)
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 75
        assert "the token is not needed again" in result.stderr
        assert reloads == []
        assert srv.token_state() != "active"

    def test_the_conflict_is_not_swallowed_into_exit_zero(self, run, client, make_server, monkeypatch):
        srv = make_server()
        monkeypatch.setattr(enroll_cli, "commit_config",
                            lambda *a, **k: (_ for _ in ()).throw(ConfigConflict(1, 2)))
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 75


# --------------------------------------------------------------------------
# --new-key: a fresh key, enrolled with the token, promoted on success
# --------------------------------------------------------------------------


def next_files(database: Path):
    return sorted(p.name for p in tls_of(database).glob("client.next.*"))


class TestNewKey:
    @pytest.fixture
    def enrolled(self, run, client, make_server, reloads):
        srv = make_server("a")
        token = srv.token()
        assert do_enroll(run, client, srv, token).exit_code == 0
        reloads.clear()
        srv.log.clear()
        return types.SimpleNamespace(srv=srv, token=token, db=client, reloads=reloads,
                                     old=client_cert(client))

    def test_a_replace_token_enrolls_the_fresh_key_and_replaces_the_old_one(self, run, enrolled):
        srv = enrolled.srv
        scoped = srv.token(client_id=CID, replace=True)
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, srv, scoped, "--new-key", fname="scoped")
        assert result.exit_code == 0, result.stderr
        new = client_cert(enrolled.db)
        assert new.fingerprint != enrolled.old.fingerprint
        assert next_files(enrolled.db) == []                           # promoted
        assert "new client key installed" in result.stdout
        assert f"client certificate: {new.fingerprint}" in result.stdout
        # the server: only the new certificate remains under the id
        assert {p.fingerprint for p in srv.clients()[CID].pins} == {new.fingerprint}
        # the wire: the OLD certificate is never asked about (the question is about the new
        # key), the token is sent once, anonymously
        assert [e.path for e in srv.log] == ["/enroll"] and srv.log[0].cert is False
        assert srv.log[0].json["cert_pem"] == new.cert_pem
        # the destination is unchanged, so nothing is written and nobody is signalled (the
        # daemon follows the file)
        untouched(enrolled.db, before, enrolled.reloads)

    def test_it_skips_the_whoami_shortcut_even_when_the_old_key_is_trusted(self, run, enrolled):
        """Plain `enroll` with this token would stop at /whoami (the old key is trusted) and a
        `--replace` token would never be sent; `--new-key` sends it."""
        scoped = enrolled.srv.token(client_id=CID, replace=True)
        plain = do_enroll(run, enrolled.db, enrolled.srv, scoped, fname="plain")
        assert plain.exit_code == 0 and "the token was not used" in plain.stdout
        assert "enroll --new-key" in plain.stdout                      # ...and says what to do
        assert enrolled.srv.token_state() == "active"
        assert enrolled.srv.seen("/enroll") == []

    def test_a_token_that_is_not_a_replace_token_meets_client_id_taken(self, run, enrolled):
        unscoped = enrolled.srv.token()
        result = do_enroll(run, enrolled.db, enrolled.srv, unscoped, "--new-key", fname="u")
        assert result.exit_code == 1
        assert "already knows a client with id" in result.stderr
        assert client_cert(enrolled.db).fingerprint == enrolled.old.fingerprint   # nothing promoted
        assert enrolled.srv.token_state() == "active"

    def test_a_failure_leaves_the_old_key_in_place_and_an_untrusted_successor_is_discarded(
        self, run, enrolled
    ):
        enrolled.srv.on("/enroll", Reply(500, "x"))
        scoped = enrolled.srv.token(client_id=CID, replace=True)
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 1
        assert client_cert(enrolled.db).fingerprint == enrolled.old.fingerprint
        assert len(next_files(enrolled.db)) == 2
        left = identity.fingerprint_pem((tls_of(enrolled.db) / "client.next.crt").read_text())
        untouched(enrolled.db, before, enrolled.reloads)
        # No server trusts the leftover (and a copied directory could have brought it along):
        # the re-run throws it away and enrolls a fresh key.
        enrolled.srv.on("/enroll", None)
        again = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert again.exit_code == 0, again.stderr
        assert f"discarding the unused new client key {left}" in again.stderr
        assert "Generating a fresh one" in again.stderr
        assert client_cert(enrolled.db).fingerprint not in (left, enrolled.old.fingerprint)
        assert next_files(enrolled.db) == []

    def test_a_successor_a_server_already_trusts_is_reused_and_the_token_is_not_sent(
        self, run, enrolled
    ):
        """An earlier run got `client.next` enrolled and then failed (promotion, commit): the
        server trusts it, so the re-run keeps it, does not send the token and finishes."""
        nxt = identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                       name=identity.CLIENT_NEXT_NAME)
        enrolled.srv.pin_additional(nxt, CID)
        spare = enrolled.srv.token(client_id=CID, replace=True)
        result = do_enroll(run, enrolled.db, enrolled.srv, spare, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        assert "reusing the new client key" in result.stderr and "already trusts it" in result.stderr
        assert "the token was not used" in result.stdout
        assert enrolled.srv.seen("/enroll") == [] and enrolled.srv.token_state() == "active"
        assert client_cert(enrolled.db).fingerprint == nxt.fingerprint
        assert next_files(enrolled.db) == []

    def test_a_successor_trusted_under_another_id_is_refused(self, run, enrolled):
        nxt = identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                       name=identity.CLIENT_NEXT_NAME)
        enrolled.srv.pin(nxt, "other-client")
        scoped = enrolled.srv.token(client_id=CID, replace=True)
        result = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 1
        assert "other-client" in result.stderr and CID in result.stderr
        assert enrolled.srv.seen("/enroll") == []

    def test_a_host_with_no_certificate_yet_just_enrolls(self, run, client, make_server):
        srv = make_server()
        result = do_enroll(run, client, srv, srv.token(), "--new-key")
        assert result.exit_code == 0, result.stderr
        assert "--new-key changes nothing" in result.stderr
        assert next_files(client) == []
        assert srv.clients()[CID].pins[0].fingerprint == client_cert(client).fingerprint

    def test_other_enrolled_servers_are_warned_about(self, run, enrolled, make_server):
        b = make_server("b")
        assert do_enroll(run, enrolled.db, b, b.token(), name="second", fname="tb").exit_code == 0
        enrolled.reloads.clear()
        scoped = enrolled.srv.token(client_id=CID, replace=True)
        result = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        assert "also enrolled at 'second'" in result.stderr
        assert "will refuse the new one until it is enrolled there too" in result.stderr
        assert f"trust token create --client-id {CID} --replace" in result.stderr
        assert "unscoped token is refused with 409 client_id_taken" in result.stderr

    def test_the_other_server_is_brought_over_with_a_plain_enroll_and_a_replace_token(
        self, run, enrolled, make_server
    ):
        b = make_server("b")
        assert do_enroll(run, enrolled.db, b, b.token(), name="second", fname="tb").exit_code == 0
        scoped_a = enrolled.srv.token(client_id=CID, replace=True)
        assert do_enroll(run, enrolled.db, enrolled.srv, scoped_a, "--new-key", fname="sa").exit_code == 0
        new = client_cert(enrolled.db)
        # B still has the old certificate; its operator creates a scoped replace token, and a
        # PLAIN enroll (the new certificate is this host's own now) replaces it
        scoped_b = b.token(client_id=CID, replace=True)
        result = do_enroll(run, enrolled.db, b, scoped_b, name="second", url=False, fname="sb")
        assert result.exit_code == 0, result.stderr
        assert {p.fingerprint for p in b.clients()[CID].pins} == {new.fingerprint}
        assert client_cert(enrolled.db).fingerprint == new.fingerprint     # not regenerated

    def test_the_old_key_is_not_trusted_by_the_replaced_server_any_more(self, run, enrolled):
        scoped = enrolled.srv.token(client_id=CID, replace=True)
        assert do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s").exit_code == 0
        assert enrolled.old.fingerprint not in {p.fingerprint for p in enrolled.srv.pins().values()}

    @pytest.mark.parametrize("args", [
        ("--cert", "/x/c.crt"), ("--key", "/x/c.key"),
        ("--cert", "/x/c.crt", "--key", "/x/c.key"),             # not with --new-key absent
    ], ids=["cert-only", "key-only", "no-new-key"])
    def test_cert_and_key_usage(self, run, enrolled, args):
        extra = ("--new-key",) if "no-new-key" not in args and len(args) == 2 else ()
        if len(args) == 4:
            extra = ()
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token, *args, *extra, fname="u")
        assert result.exit_code == 2, result.stderr
        assert enrolled.srv.log == []
        untouched(enrolled.db, before, enrolled.reloads)

    def test_cert_and_key_with_generated_paths_are_a_usage_error(self, run, enrolled):
        result = do_enroll(run, enrolled.db, enrolled.srv, enrolled.token, "--new-key",
                           "--cert", "/x/c.crt", "--key", "/x/c.key", fname="u")
        assert result.exit_code == 2
        assert "enroll NAME --new-key" in result.stderr
        assert enrolled.srv.log == []

    def test_a_swap_that_cannot_be_installed_says_so_and_names_the_rerun(self, run, enrolled, monkeypatch):
        real = os.rename

        def flaky(src, dst, *a, **kw):
            if Path(dst).name == "client.crt" and Path(src).name == "client.next.crt":
                raise OSError(5, "simulated crash")
            return real(src, dst, *a, **kw)

        scoped = enrolled.srv.token(client_id=CID, replace=True)
        monkeypatch.setattr(os, "rename", flaky)
        result = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 1
        assert "the server enrolled the new certificate, but it could not be installed" in result.stderr
        assert "--new-key" in result.stderr

    def test_the_rerun_after_an_interrupted_promotion_completes_without_the_token(
        self, run, enrolled, monkeypatch
    ):
        """The first run's server enrolled the new key, but the promotion died between its two
        renames; the message promises that re-running the same command needs no token."""
        real = os.rename
        state = {"fail": True}

        def flaky(src, dst, *a, **kw):
            if state["fail"] and Path(dst).name == "client.crt" and Path(src).name == "client.next.crt":
                raise OSError(5, "simulated crash")
            return real(src, dst, *a, **kw)

        scoped = enrolled.srv.token(client_id=CID, replace=True)
        monkeypatch.setattr(os, "rename", flaky)
        first = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert first.exit_code == 1
        enrolled_fp = identity.fingerprint_pem((tls_of(enrolled.db) / "client.next.crt").read_text())
        state["fail"] = False
        enrolled.srv.log.clear()
        again = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert again.exit_code == 0, again.stderr
        assert "finished an interrupted promotion" in again.stderr
        assert "discarding" not in again.stderr                         # it is the key the server has
        # the run became a PLAIN enroll of the promoted identity: it says so, the token is not
        # spent, and the plain-enroll `--replace` hint is suppressed (this was --new-key)
        assert "finished an interrupted key promotion" in again.stdout
        assert "finished an interrupted `enroll --new-key`" not in again.stdout
        assert "the server already trusts it, so this run did not use the token" in again.stdout
        assert "the token was not used" in again.stdout
        assert "If this was a `--replace` token" not in again.stdout
        assert enrolled.srv.seen("/enroll") == []                        # (the first run spent it)
        assert client_cert(enrolled.db).fingerprint == enrolled_fp       # the key the server has
        assert enrolled.srv.seen("/enroll") == []                         # the token is not sent again

    # -- a pending client.next
    def test_debris_in_client_next_is_repaired_without_a_discard_warning(self, run, enrolled):
        d = tls_of(enrolled.db)
        (d / "client.next.crt").write_text("not a certificate\n")
        (d / "client.next.key").write_text("not a key\n")
        scoped = enrolled.srv.token(client_id=CID, replace=True)
        result = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        assert "discarding" not in result.stderr and "reusing" not in result.stderr
        assert client_cert(enrolled.db).fingerprint != enrolled.old.fingerprint
        assert next_files(enrolled.db) == []

    def test_an_unreachable_server_during_the_probe_keeps_the_pending_key(self, run, enrolled):
        """With the server down a pending `client.next` cannot be told from a trusted one:
        exit 75 and nothing is deleted or generated."""
        pending = identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                           name=identity.CLIENT_NEXT_NAME)
        files = {p.name: p.read_bytes() for p in tls_of(enrolled.db).glob("client.*")}
        before = db_state(enrolled.db)
        token = str(identity.make_token(enrolled.srv.fp)[0])
        result = do_enroll(run, enrolled.db, f"https://127.0.0.1:{closed_port()}", token,
                           "--new-key", fname="s")
        assert result.exit_code == 75, result.stderr
        assert "cannot reach" in result.stderr and "discarding" not in result.stderr
        assert {p.name: p.read_bytes() for p in tls_of(enrolled.db).glob("client.*")} == files
        assert identity.fingerprint_pem((tls_of(enrolled.db) / "client.next.crt").read_text()) == pending.fingerprint
        untouched(enrolled.db, before, enrolled.reloads)

    def test_the_discard_warning_says_which_server_does_not_trust_it_and_names_the_caveat(
        self, run, enrolled
    ):
        pending = identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                           name=identity.CLIENT_NEXT_NAME)
        scoped = enrolled.srv.token(client_id=CID, replace=True)
        result = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        flat = " ".join(result.stderr.split())
        assert (f"discarding the unused new client key {pending.fingerprint} (client.next.* in "
                f"{tls_of(enrolled.db)}) left by an earlier `enroll --new-key` or `identity "
                "rotate`: the server at this destination does not trust it, and the directory "
                "may have been copied. Generating a fresh one. Any other server that accepted "
                "it as a pending rotation successor will need this host enrolled there again "
                "(see `identity rotate`)") in flat
        assert "the server does not trust it," not in flat       # the old, unqualified wording

    def test_every_probe_failing_the_pin_check_raises_the_pin_error_and_keeps_client_next(
        self, run, enrolled, make_server
    ):
        """The destination now answers with ANOTHER certificate than the one pinned (a
        re-keyed, replaced or impostor server): no server presenting the expected
        certificate has said "not trusted", so the pending key is NOT known to be stale
        and a discard (irreversible) must not happen."""
        impostor = make_server("impostor")
        pending = identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                           name=identity.CLIENT_NEXT_NAME)
        files = {p.name: p.read_bytes() for p in tls_of(enrolled.db).glob("client.*")}
        scoped = enrolled.srv.token(client_id=CID, replace=True)    # names srv "a", not the impostor
        before = db_state(enrolled.db)
        result = do_enroll(run, enrolled.db, impostor, scoped, "--new-key", fname="s")
        assert result.exit_code == 1, result.stderr
        assert (f"the server at {impostor.url} presents certificate {impostor.fp}, not the "
                f"expected {enrolled.srv.fp}. Refusing to talk to it") in " ".join(result.stderr.split())
        assert "discarding" not in result.stderr and "Generating a fresh one" not in result.stderr
        # nothing was deleted, replaced, generated or sent
        assert {p.name: p.read_bytes() for p in tls_of(enrolled.db).glob("client.*")} == files
        assert identity.peek_identity(tls_of(enrolled.db), identity.CLIENT_NEXT_NAME).fingerprint \
            == pending.fingerprint
        assert client_cert(enrolled.db).fingerprint == enrolled.old.fingerprint
        assert impostor.log == [] and enrolled.srv.seen("/enroll") == []
        assert enrolled.srv.token_state() == "active"
        untouched(enrolled.db, before, enrolled.reloads)
        # ...and once the real server answers (it does not trust the pending key) the same
        # command discards it as before.
        again = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert again.exit_code == 0, again.stderr
        assert f"discarding the unused new client key {pending.fingerprint}" in again.stderr
        assert client_cert(enrolled.db).fingerprint not in (pending.fingerprint, enrolled.old.fingerprint)

    def test_the_pin_error_names_the_tokens_fingerprint_not_a_stored_one(
        self, run, enrolled, make_server
    ):
        """With a stored pin that differs from the token's, two pins are tried; if both fail
        the check the error names the token's (the server the operator meant)."""
        impostor = make_server("impostor")
        wanted = make_server("wanted")
        identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                 name=identity.CLIENT_NEXT_NAME)
        token = wanted.token()                       # a third server's fingerprint; stored pin is srv a's
        result = do_enroll(run, enrolled.db, impostor, token, "--new-key", fname="s")
        assert result.exit_code == 1
        assert f"not the expected {wanted.fp}" in " ".join(result.stderr.split())
        assert len(next_files(enrolled.db)) == 2 and "discarding" not in result.stderr
        assert impostor.log == []

    def test_one_pin_failing_while_another_answers_not_trusted_still_discards(
        self, run, enrolled, make_server
    ):
        """The destination is re-pointed at a NEW server (stored pin: srv a, token: the new
        server). The stored pin fails the check but the token's pin reaches a server that says
        "not trusted": that is a real answer, so the stale key is discarded and replaced."""
        new_srv = make_server("b")
        pending = identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                           name=identity.CLIENT_NEXT_NAME)
        result = do_enroll(run, enrolled.db, new_srv, new_srv.token(), "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        assert f"discarding the unused new client key {pending.fingerprint}" in result.stderr
        assert "does not trust it" in result.stderr
        assert client_cert(enrolled.db).fingerprint not in (pending.fingerprint, enrolled.old.fingerprint)
        assert next_files(enrolled.db) == []
        assert dest(enrolled.db).server_fingerprint == new_srv.fp
        assert [p.fingerprint for p in new_srv.clients()[CID].pins] == [client_cert(enrolled.db).fingerprint]

    def test_a_server_that_trusts_the_pending_key_keeps_it_even_beside_a_failing_pin(
        self, run, enrolled, make_server
    ):
        """A pin failure on the stored pin does not stop the token's pin from finding a server
        that already trusts the pending key (the idempotent re-run)."""
        new_srv = make_server("b")
        nxt = identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                       name=identity.CLIENT_NEXT_NAME)
        new_srv.pin(nxt, CID)
        spare = new_srv.token(client_id=CID, replace=True)
        result = do_enroll(run, enrolled.db, new_srv, spare, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        assert "reusing the new client key" in result.stderr and "discarding" not in result.stderr
        assert client_cert(enrolled.db).fingerprint == nxt.fingerprint

    def test_a_refused_new_key_names_the_fingerprint_and_how_to_get_another(self, run, enrolled):
        foreign = make_foreign_token(enrolled.srv)
        result = do_enroll(run, enrolled.db, enrolled.srv, foreign, "--new-key", fname="f")
        assert result.exit_code == 1
        successor = identity.peek_identity(tls_of(enrolled.db), identity.CLIENT_NEXT_NAME)
        assert f"the server REVOKED the new client certificate {successor.fingerprint}" in result.stderr
        flat = " ".join(result.stderr.split())
        assert ("re-run `zfsbackup-config enroll offsite --new-key` (a new client key that no "
                "server trusts is discarded and replaced by a fresh one)") in flat
        assert "delete " not in flat                                   # no manual file surgery
        assert "The token is not spent when the server refuses it." in result.stderr
        assert client_cert(enrolled.db).fingerprint == enrolled.old.fingerprint

    def test_the_other_destinations_are_listed_with_the_token_they_need(self, run, enrolled, make_server):
        b = make_server("b")
        assert do_enroll(run, enrolled.db, b, b.token(), name="second", fname="tb").exit_code == 0
        scoped = enrolled.srv.token(client_id=CID, replace=True)
        result = do_enroll(run, enrolled.db, enrolled.srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        old = enrolled.old.fingerprint
        assert f"this host no longer holds the key of {old}" in result.stdout
        # not knowing which pins /enroll dropped, the revoke advice is conditional
        assert f"still trusts {old} unless the token was created with" in result.stdout
        assert f"`zfsbackup-config trust revoke {old}`" in result.stdout
        warning = " ".join(result.stderr.split())
        assert "also enrolled at 'second'" in warning
        assert f"trust token create --client-id {CID} --replace" in warning
        assert "without --replace the old, possibly stolen, certificate stays pinned there" in warning

    # -- discarding the stale successor (identity.discard_identity)
    @pytest.fixture
    def stale_successor(self, enrolled):
        identity.ensure_identity(tls_of(enrolled.db), identity.ROLE_CLIENT,
                                 name=identity.CLIENT_NEXT_NAME)
        enrolled.scoped = enrolled.srv.token(client_id=CID, replace=True)
        return enrolled

    def test_a_pair_that_changed_since_the_probe_stops_the_run(self, run, stale_successor, monkeypatch):
        e = stale_successor
        calls = []
        monkeypatch.setattr(identity, "discard_identity",
                            lambda *a, **k: (calls.append((a, k)), False)[1])
        before = db_state(e.db)
        result = do_enroll(run, e.db, e.srv, e.scoped, "--new-key", fname="s")
        assert result.exit_code == 1
        assert f"client.next in {tls_of(e.db)} changed while this command was running" in result.stderr
        assert "another `enroll --new-key` or `identity rotate`?" in result.stderr
        assert "nothing was discarded. Re-run the same command." in result.stderr
        (args, kwargs), = calls
        assert args[1] == identity.CLIENT_NEXT_NAME and len(kwargs["expected_fingerprint"]) == 64
        assert e.srv.seen("/enroll") == [] and e.srv.token_state() == "active"
        untouched(e.db, before, e.reloads)

    def test_an_identity_error_from_the_discard_is_exit_1(self, run, stale_successor, monkeypatch):
        e = stale_successor

        def refuse(*a, **k):
            raise identity.IdentityError("cannot lock the identity directory: timed out", Path("/x"))

        monkeypatch.setattr(identity, "discard_identity", refuse)
        result = do_enroll(run, e.db, e.srv, e.scoped, "--new-key", fname="s")
        assert result.exit_code == 1
        assert "cannot lock the identity directory: timed out" in result.stderr
        assert "Traceback" not in everything(result)
        assert e.srv.seen("/enroll") == []

    def test_what_the_discard_does_to_the_files(self, run, stale_successor):
        e = stale_successor
        left = identity.peek_identity(tls_of(e.db), identity.CLIENT_NEXT_NAME).fingerprint
        old_pair = (tls_of(e.db) / "client.crt").read_bytes()
        result = do_enroll(run, e.db, e.srv, e.scoped, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        # the stale pair is gone for good, the old pair was never touched until the promotion
        assert client_cert(e.db).fingerprint not in (left, e.old.fingerprint)
        assert next_files(e.db) == []
        assert e.srv.seen("/enroll") and left not in e.srv.seen("/enroll")[0].json["cert_pem"]
        assert (tls_of(e.db) / "client.crt").read_bytes() != old_pair

    # -- configured paths
    @pytest.fixture
    def conf(self, run, db, reloads, make_server, tmp_path):
        srv = make_server("a")
        old = identity.ensure_identity(tmp_path / "mine", identity.ROLE_CLIENT)
        new = identity.ensure_identity(tmp_path / "next", identity.ROLE_CLIENT)
        make_db(run, db, PLAIN_YAML + f'client_cert_file: "{old.cert_path}"\n'
                                      f'client_key_file: "{old.key_path}"\n')
        seed_client_id(db, CID)
        assert do_enroll(run, db, srv, srv.token()).exit_code == 0
        reloads.clear()
        srv.log.clear()
        return types.SimpleNamespace(
            db=db, srv=srv, old=old, new=new, reloads=reloads,
            args=("--new-key", "--cert", str(new.cert_path), "--key", str(new.key_path)))

    def test_configured_paths_are_updated_in_the_same_commit_with_one_reload(self, run, conf):
        gen = generation(conf.db)
        scoped = conf.srv.token(client_id=CID, replace=True)
        result = do_enroll(run, conf.db, conf.srv, scoped, *conf.args, fname="s")
        assert result.exit_code == 0, result.stderr
        config = stored(conf.db)
        assert config.client_cert_file == conf.new.cert_path and config.client_key_file == conf.new.key_path
        assert generation(conf.db) == gen + 1
        assert len(conf.reloads) == 1
        assert conf.reloads[0][0] == conf.reloads[0][1] == store_key(conf.db)
        assert conf.old.cert_path.exists() and conf.old.key_path.exists()      # left in place
        assert {p.fingerprint for p in conf.srv.clients()[CID].pins} == {conf.new.fingerprint}
        assert "client_cert_file / client_key_file now point at" in result.stdout

    def test_configured_paths_need_cert_and_key(self, run, conf):
        result = do_enroll(run, conf.db, conf.srv, conf.srv.token(), "--new-key", fname="s")
        assert result.exit_code == 2 and "pass --cert NEW --key NEW" in result.stderr
        assert conf.srv.log == [] and conf.reloads == []

    def test_the_configured_files_themselves_are_refused(self, run, conf):
        result = do_enroll(run, conf.db, conf.srv, conf.srv.token(), "--new-key", "--cert",
                           str(conf.old.cert_path), "--key", str(conf.old.key_path), fname="s")
        assert result.exit_code == 1 and "name the currently configured files" in result.stderr
        assert conf.srv.log == []

    def test_a_refused_pair_says_to_use_a_different_one(self, run, conf):
        result = do_enroll(run, conf.db, conf.srv, make_foreign_token(conf.srv), *conf.args, fname="f")
        assert result.exit_code == 1
        assert f"the server REVOKED the new client certificate {conf.new.fingerprint}" in result.stderr
        assert "use a different pair" in result.stderr
        assert f"the files {conf.new.cert_path} and {conf.new.key_path} are yours" in result.stderr
        assert conf.new.cert_path.exists() and conf.new.key_path.exists()      # never deleted
        assert stored(conf.db).client_cert_file == conf.old.cert_path

    def test_a_conflict_at_the_commit_says_to_rerun_the_same_command(self, run, conf, monkeypatch):
        scoped = conf.srv.token(client_id=CID, replace=True)
        real = enroll_cli.commit_config
        state = {"armed": True}

        def competing(*args, **kwargs):
            if state["armed"]:
                state["armed"] = False
                assert Cli()("set", "check_interval", "10m", "-c", conf.db).exit_code == 0
            return real(*args, **kwargs)

        monkeypatch.setattr(enroll_cli, "commit_config", competing)
        result = do_enroll(run, conf.db, conf.srv, scoped, *conf.args, fname="s")
        assert result.exit_code == 75
        assert "ALREADY enrolled the new certificate" in result.stderr
        conf.reloads.clear()
        again = do_enroll(run, conf.db, conf.srv, scoped, *conf.args, fname="s")
        assert again.exit_code == 0, again.stderr
        assert stored(conf.db).client_cert_file == conf.new.cert_path
        assert len(conf.reloads) == 1


# --------------------------------------------------------------------------
# A revoked certificate can never be enrolled again
# --------------------------------------------------------------------------


class TestRevokedCertificate:
    @pytest.fixture
    def revoked(self, run, client, make_server, reloads):
        srv = make_server()
        assert do_enroll(run, client, srv, srv.token()).exit_code == 0
        old = client_cert(client)
        revoke_pin(srv.resolved, old.fingerprint, lock=None)
        dispose_all()
        srv.refresh()
        srv.log.clear()
        reloads.clear()
        return types.SimpleNamespace(srv=srv, db=client, old=old, reloads=reloads)

    def test_the_refusal_points_at_new_key(self, run, revoked):
        scoped = revoked.srv.token(client_id=CID, replace=True)
        before = db_state(revoked.db)
        result = do_enroll(run, revoked.db, revoked.srv, scoped)
        assert result.exit_code == 1
        assert "refused the token" in result.stderr
        assert f"enroll offsite --new-key" in result.stderr
        untouched(revoked.db, before, revoked.reloads)

    def test_new_key_recovers(self, run, revoked):
        scoped = revoked.srv.token(client_id=CID, replace=True)
        result = do_enroll(run, revoked.db, revoked.srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        new = client_cert(revoked.db)
        assert new.fingerprint != revoked.old.fingerprint
        assert {p.fingerprint for p in revoked.srv.clients()[CID].pins} == {new.fingerprint}


# --------------------------------------------------------------------------
# A real terminal
# --------------------------------------------------------------------------


@pytest.mark.integration
class TestPromptOnATty:
    def drive(self, tmp_path, argv, send, prompt="Enrollment token"):
        env = hermetic_env(HOME=str(tmp_path / "home"), PYTHONPATH=str(REPO))
        (tmp_path / "home").mkdir(exist_ok=True)
        done = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "_pty_driver.py"), prompt, send, "60",
             "--", sys.executable, "-m", "zfsbackup.cli", *argv],
            capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120,
        )
        assert done.returncode == 0, done.stderr
        import base64
        data = json.loads(done.stdout)
        return base64.b64decode(data["output"]).decode(errors="replace"), data["status"], data["prompted"]

    def test_the_prompt_is_hidden_and_the_token_works(self, run, client, make_server, tmp_path):
        srv = make_server()
        token = srv.token()
        output, status, prompted = self.drive(
            tmp_path, ["enroll", "offsite", "--url", srv.url, "-c", str(client)], token
        )
        assert prompted and status == 0, output
        assert "Enrollment token" in output
        assert token not in output and secret_of(token) not in output     # the terminal did not echo it
        assert dest(client).server_fingerprint == srv.fp
        assert srv.paths() == ["/enroll"]

    def test_token_dash_on_a_terminal_prompts_too(self, run, client, make_server, tmp_path):
        srv = make_server()
        token = srv.token()
        output, status, prompted = self.drive(
            tmp_path, ["enroll", "offsite", "--url", srv.url, "--token", "-", "-c", str(client)], token
        )
        assert prompted and status == 0, output
        assert token not in output

    def test_a_wrong_token_typed_at_the_prompt_is_not_echoed_in_the_error(self, run, client, tmp_path):
        output, status, _ = self.drive(
            tmp_path,
            ["enroll", "offsite", "--url", "https://127.0.0.1:1", "-c", str(client)],
            "zfsb1.short.TYPED-SECRET-TEXT",
        )
        assert status == 1
        assert "TYPED-SECRET-TEXT" not in output
        assert "malformed enrollment token" in output

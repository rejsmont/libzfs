"""`zfsbackup-config identity rotate` (TLS/mTLS plan M14c; server half M14b `POST /rotate`).

Replace this host's client certificate without re-enrolling: a successor
(`client.next.*`, or the operator's `--cert/--key`) is registered with EVERY enrolled
destination, authenticated with the CURRENT certificate, and the files are swapped only
if every server accepted it. Like ``test_zfsbackup_cli_enroll.py`` this drives the real
CLI against real API servers in this process (`tests/_enroll_support.py`), with the
answers a healthy server never gives scripted on the same server.

- ``TestRotate``                 one and two destinations, what each side holds afterwards,
                                 the table, the old pin retired on first use, no signal
- ``TestPartialFailure``         busy / unreachable / refused: exit 75 vs 1, NOTHING
                                 swapped, `client.next` kept, the re-run (`already` + `rotated`)
                                 a refusal beside a server that accepted the successor names that
                                 server and the two ways out (revoke the successor, or a --replace token)
- ``TestServerAnswers``          every answer of `/whoami` and `/rotate` mapped to a RESULT
- ``TestResets``                 a reset right after the handshake during `/whoami` is
                                 retried once; `/rotate` is never retried
- ``TestInterruptedSwap``        a swap that died between its two renames is finished first
- ``TestConfiguredPaths``        `--cert/--key`: one `commit_config`, exactly one reload
- ``TestUsage``                  exit 2 and exit 1 matrices; nothing created on a refusal

Mutation counts: see the hand-off report.
"""

import os
import shutil
import types
from pathlib import Path

import pytest

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
    do_enroll,
    everything,
    make_server,
    reloads,
    tls_of,
    untouched,
    zfs_stub,
)
from zfsbackup import daemonlock, identity
from zfsbackup.cli import enroll as enroll_cli
from zfsbackup.config.store import dispose_all, revoke_pin


#: Every test here talks TLS to a real API server on 127.0.0.1.
pytestmark = pytest.mark.integration

@pytest.fixture
def run(reloads) -> Cli:
    return Cli()


def rotate(run, database, *extra):
    return run("identity", "rotate", *extra, "-c", database)


def pair_bytes(database: Path):
    d = tls_of(database)
    return (d / "client.crt").read_bytes(), (d / "client.key").read_bytes()


def next_files(database: Path):
    return sorted(p.name for p in tls_of(database).glob("client.next.*"))


def table_rows(result):
    """The rows of the DESTINATION RESULT URL DETAIL table, as `{destination: result}`."""
    lines = [l for l in result.stdout.splitlines() if l.strip()]
    start = next(i for i, l in enumerate(lines) if l.split()[:4] == ["DESTINATION", "RESULT", "URL", "DETAIL"])
    rows = {}
    for line in lines[start + 1:]:
        if not line.split() or line.startswith("client certificate"):
            break
        parts = line.split()
        rows[parts[0]] = parts[1]
    return rows


@pytest.fixture
def one(run, client, make_server, reloads):
    """Enrolled at one server (the client certificate is generated)."""
    srv = make_server("a")
    token = srv.token()
    assert do_enroll(run, client, srv, token).exit_code == 0
    reloads.clear()
    srv.log.clear()
    return types.SimpleNamespace(db=client, srv=srv, token=token, reloads=reloads,
                                 old=client_cert(client))


@pytest.fixture
def two(run, one, make_server, reloads):
    """Enrolled at two servers, `a` (destination `offsite`) and `b` (destination `second`)."""
    b = make_server("b")
    assert do_enroll(run, one.db, b, b.token(), name="second", fname="tb").exit_code == 0
    reloads.clear()
    b.log.clear()
    one.b = b
    return one


# --------------------------------------------------------------------------
# The healthy cases
# --------------------------------------------------------------------------


class TestRotate:
    def test_one_destination(self, run, one):
        before = db_state(one.db)
        old = one.old
        result = rotate(run, one.db)
        assert result.exit_code == 0, result.stderr
        # the table, then the swap line
        out = result.stdout
        assert out.splitlines()[0].split() == ["DESTINATION", "RESULT", "URL", "DETAIL"]
        assert table_rows(result) == {"offsite": "rotated"}
        assert one.srv.url in out.splitlines()[1]
        new = client_cert(one.db)
        assert new.fingerprint != old.fingerprint
        assert f"client certificate rotated: {old.fingerprint} -> {new.fingerprint}" in out
        # files: swapped in, no successor left behind, the new key is private
        assert next_files(one.db) == []
        assert (tls_of(one.db) / "client.key").stat().st_mode & 0o777 == 0o600
        # the wire: /whoami then /rotate, both with the CURRENT certificate
        assert [(e.method, e.path, e.cert) for e in one.srv.log] == [
            ("GET", "/whoami", True), ("POST", "/rotate", True)]
        body = one.srv.seen("/rotate")[0].json
        assert set(body) == {"version", "successor_cert_pem", "signature"}
        assert identity.fingerprint_pem(body["successor_cert_pem"]) == new.fingerprint
        assert "PRIVATE KEY" not in everything(result)
        # the server pinned the successor beside the old certificate
        assert {p.fingerprint for p in one.srv.clients()[CID].pins} == {old.fingerprint, new.fingerprint}
        # the database is untouched and the daemon is not signalled (generated paths
        # are reloaded by the remote worker on file change)
        untouched(one.db, before, one.reloads)

    def test_the_old_pin_is_retired_on_the_first_request_with_the_new_certificate(self, run, one):
        old = one.old
        assert rotate(run, one.db).exit_code == 0
        new = client_cert(one.db)
        again = do_enroll(run, one.db, one.srv, one.token)            # /whoami with the NEW certificate
        assert again.exit_code == 0 and "the token was not used" in again.stdout
        assert {p.fingerprint for p in one.srv.clients()[CID].pins} == {new.fingerprint}
        assert old.fingerprint not in {p.fingerprint for p in one.srv.pins().values()}

    def test_the_first_request_after_a_rotation_retires_the_old_pin_in_the_chain_trust_revoke_follows(
        self, run, one
    ):
        """A -> B -> C by two `identity rotate`s. The second one's /whoami (with B) retires A and
        its /rotate (with B, authenticated) leaves C pending. The server operator who revokes
        the long-gone A (a stolen first key) must still reach B and C: `trust revoke` follows
        the recorded supersession A -> B and takes B's pending successor with it."""
        a = one.old
        assert rotate(run, one.db).exit_code == 0
        b = client_cert(one.db)
        assert rotate(run, one.db).exit_code == 0
        c = client_cert(one.db)
        assert len({a.fingerprint, b.fingerprint, c.fingerprint}) == 3
        # the second rotation authenticated B: A is gone from the pins, B and C remain
        assert {p.fingerprint for p in one.srv.clients()[CID].pins} == {b.fingerprint, c.fingerprint}
        server_db = str(one.srv.resolved.path)
        listed = run("trust", "list", "-c", server_db)
        assert f"retired (replaced by {b.fingerprint[:16]})" in listed.stdout
        result = run("trust", "revoke", a.fingerprint, "-c", server_db)
        assert result.exit_code == 0, result.stderr
        assert (f"certificate {a.fingerprint} (client id {CID}) was already retired by its "
                f"successor {b.fingerprint}; revoked {b.fingerprint} (and its successors "
                f"{c.fingerprint}).") in result.stdout
        assert f"client id {CID} has no other pinned certificate" in result.stdout
        one.srv.refresh()
        assert one.srv.clients()[CID].pins == ()

    def test_two_destinations_are_both_rotated_to_the_same_certificate(self, run, two):
        result = rotate(run, two.db)
        assert result.exit_code == 0, result.stderr
        assert table_rows(result) == {"offsite": "rotated", "second": "rotated"}
        new = client_cert(two.db).fingerprint
        for srv in (two.srv, two.b):
            assert new in {p.fingerprint for p in srv.clients()[CID].pins}
            assert [e.path for e in srv.log] == ["/whoami", "/rotate"]
        names = [l.split()[0] for l in result.stdout.splitlines()[1:3]]
        assert names == sorted(names)                                  # sorted by name

    def test_rotating_twice_gives_a_third_certificate(self, run, one):
        first = client_cert(one.db).fingerprint
        assert rotate(run, one.db).exit_code == 0
        second = client_cert(one.db).fingerprint
        assert rotate(run, one.db).exit_code == 0
        third = client_cert(one.db).fingerprint
        assert len({first, second, third}) == 3

    def test_only_enrolled_destinations_are_asked(self, run, db, reloads, make_server):
        srv = make_server()
        client_with(run, db, reloads, {"unenrolled": ("https://127.0.0.1:1", None)})
        assert do_enroll(run, db, srv, srv.token()).exit_code == 0
        reloads.clear()
        result = rotate(run, db)
        assert result.exit_code == 0, result.stderr
        assert table_rows(result) == {"offsite": "rotated"}

    def test_the_configuration_is_not_otherwise_touched(self, run, one):
        before = stored(one.db)
        assert rotate(run, one.db).exit_code == 0
        assert stored(one.db) == before


# --------------------------------------------------------------------------
# One destination fails: nothing is swapped
# --------------------------------------------------------------------------


class TestPartialFailure:
    def test_busy_then_rerun_gives_already_and_rotated(self, run, two):
        two.b.on("/rotate", Reply(503, {"error": "busy", "retry": True, "reason": "busy"}))
        old_pair = pair_bytes(two.db)
        old = client_cert(two.db)
        first = rotate(run, two.db)
        assert first.exit_code == 75, first.stderr
        assert table_rows(first) == {"offsite": "rotated", "second": "busy"}
        # NOTHING swapped: the same pair, and the successor kept for the re-run
        assert pair_bytes(two.db) == old_pair
        assert len(next_files(two.db)) == 2
        assert "NOTHING was swapped" in first.stderr
        assert f"this host still uses {old.fingerprint}" in first.stderr
        assert "re-run `zfsbackup-config" in first.stderr and "identity rotate" in first.stderr
        assert "client certificate rotated" not in first.stdout
        successor = identity.load_identity(
            tls_of(two.db) / "client.next.crt", tls_of(two.db) / "client.next.key", "client")
        # the re-run reuses it (with a warning), servers that took it say `already`
        second = rotate(run, two.db)
        assert second.exit_code == 0, second.stderr
        assert table_rows(second) == {"offsite": "already", "second": "rotated"}
        assert "reusing the successor certificate" in second.stderr
        assert second.stderr.count("reusing the successor certificate") == 1
        assert client_cert(two.db).fingerprint == successor.fingerprint
        assert next_files(two.db) == []
        assert f"{old.fingerprint} -> {successor.fingerprint}" in second.stdout

    def test_a_successor_revoked_on_a_server_is_refused_for_ever_with_the_remedy(self, run, two):
        """B is busy, so the successor stays pending; the operator of A then revokes it (an
        earlier rotation undone on the server). The store refuses a revoked fingerprint for
        ever, so the re-run is refused and the message says how to get a new successor."""
        two.b.on("/rotate", Reply(503, {"retry": True}), then=None)
        assert rotate(run, two.db).exit_code == 75
        nxt_crt = tls_of(two.db) / "client.next.crt"
        successor = identity.fingerprint_pem(nxt_crt.read_text())
        revoke_pin(two.srv.resolved, successor, lock=None)
        dispose_all()
        two.srv.refresh()
        before = pair_bytes(two.db)
        result = rotate(run, two.db)
        assert result.exit_code == 1, result.stderr
        assert table_rows(result)["offsite"] == "refused"
        assert "If a server revoked this successor it can never be accepted: delete" in result.stderr
        assert str(nxt_crt) in result.stderr and str(tls_of(two.db) / "client.next.key") in result.stderr
        assert pair_bytes(two.db) == before

    def successor_of(self, database) -> str:
        return identity.peek_identity(tls_of(database), identity.CLIENT_NEXT_NAME).fingerprint

    def test_a_refusal_beside_an_accepting_server_names_it_and_the_two_ways_out(self, run, two):
        """A accepted the successor, B refused it: A now refuses any DIFFERENT successor for
        the current certificate, so deleting `client.next` and re-running would be refused
        there. The message says to revoke the successor on A first, or to enroll the new
        certificate there with a `--client-id ID --replace` token."""
        two.b.on("/rotate", Reply(403, {"error": "no", "reason": "refused"}))
        before = pair_bytes(two.db)
        result = rotate(run, two.db)
        assert result.exit_code == 1, result.stderr
        assert table_rows(result) == {"offsite": "rotated", "second": "refused"}
        successor = self.successor_of(two.db)
        flat = " ".join(result.stderr.split())
        assert (f"Destination(s) 'offsite' already accepted the successor {successor} (answered "
                "rotated/already) and will refuse a different successor for the current "
                "certificate: on each of those servers first run `zfsbackup-config trust revoke "
                f"{successor}`, or enroll the new certificate there with a token created by "
                "`trust token create --client-id ID --replace`.") in flat
        assert "'second'" not in flat.split("Destination(s)")[1].split("already accepted")[0]
        # the generated-files advice comes first, the caution after it
        assert flat.index("delete ") < flat.index("Destination(s) 'offsite' already accepted")
        assert flat.index("to generate a new one.") < flat.index("Destination(s) 'offsite'")
        assert pair_bytes(two.db) == before                     # nothing swapped

    def test_an_already_answer_counts_as_accepted_and_every_accepting_destination_is_named(
        self, run, two, make_server
    ):
        # round 1: B busy (the successor is pending at A); round 2: A says `already`, B and
        # the new C refuse
        two.b.on("/rotate", Reply(503, {"retry": True}), then=Reply(403, {"reason": "refused"}))
        c = make_server("c")
        assert do_enroll(run, two.db, c, c.token(), name="third", fname="tc").exit_code == 0
        first = rotate(run, two.db)
        assert first.exit_code == 75
        assert table_rows(first) == {"offsite": "rotated", "second": "busy", "third": "rotated"}
        assert "trust revoke" not in first.stderr               # a retry, not a refusal: no caution
        second = rotate(run, two.db)
        assert second.exit_code == 1, second.stderr
        assert table_rows(second) == {"offsite": "already", "second": "refused", "third": "already"}
        successor = self.successor_of(two.db)
        flat = " ".join(second.stderr.split())
        assert (f"Destination(s) 'offsite', 'third' already accepted the successor {successor}") in flat
        assert f"`zfsbackup-config trust revoke {successor}`" in flat

    def test_no_caution_when_nobody_accepted_the_successor(self, run, one):
        one.srv.on("/rotate", Reply(403, {"reason": "refused"}))
        result = rotate(run, one.db)
        assert result.exit_code == 1
        assert "If a server revoked this successor it can never be accepted" in result.stderr
        assert "already accepted the successor" not in result.stderr
        assert "trust revoke" not in result.stderr
        assert "--client-id ID --replace" not in result.stderr

    def test_no_caution_when_the_other_destination_is_only_unreachable(self, run, two):
        """Refusal wins over unreachable, but only a destination that ANSWERED rotated/already
        is named."""
        two.b.on("/rotate", Reply(403, {"reason": "refused"}))
        two.srv.stop()
        result = rotate(run, two.db)
        assert result.exit_code == 1
        assert table_rows(result) == {"offsite": "unreachable", "second": "refused"}
        assert "already accepted the successor" not in result.stderr

    def test_an_unreachable_server_is_retryable_and_swaps_nothing(self, run, two):
        two.b.stop()
        before = pair_bytes(two.db)
        result = rotate(run, two.db)
        assert result.exit_code == 75, result.stderr
        assert table_rows(result) == {"offsite": "rotated", "second": "unreachable"}
        assert "cannot reach" in result.stdout
        assert pair_bytes(two.db) == before and len(next_files(two.db)) == 2

    def test_refused_is_exit_1_and_wins_over_unreachable(self, run, two, make_server):
        two.b.on("/rotate", Reply(403, {"error": "no", "reason": "refused"}))
        c = make_server("c")
        assert do_enroll(run, two.db, c, c.token(), name="third", fname="tc").exit_code == 0
        c.stop()
        before = pair_bytes(two.db)
        result = rotate(run, two.db)
        assert result.exit_code == 1, result.stderr
        assert table_rows(result) == {"offsite": "rotated", "second": "refused", "third": "unreachable"}
        assert "2 of 3 destination(s) did not accept" in result.stderr
        assert pair_bytes(two.db) == before

    def test_only_unreachable_and_busy_is_75(self, run, two, make_server):
        two.b.on("/rotate", Reply(503, {"retry": True}))
        two.srv.stop()
        result = rotate(run, two.db)
        assert result.exit_code == 75
        assert table_rows(result) == {"offsite": "unreachable", "second": "busy"}

    def test_a_failure_changes_neither_the_database_nor_the_daemon(self, run, two):
        two.b.on("/rotate", Reply(403, {"reason": "refused"}))
        before = db_state(two.db)
        assert rotate(run, two.db).exit_code == 1
        untouched(two.db, before, two.reloads)


# --------------------------------------------------------------------------
# Each answer
# --------------------------------------------------------------------------


class TestServerAnswers:
    CASES = [
        # (id, path, reply, result, exit code, detail fragment)
        ("bad_signature", "/rotate", Reply(403, {"error": "x", "reason": "bad_signature"}),
         "refused", 1, "rejected the proof of possession"),
        ("refused", "/rotate", Reply(403, {"error": "x", "reason": "refused"}),
         "refused", 1, "refused the rotation"),
        ("400", "/rotate", Reply(400, {"error": "bad shape"}), "refused", 1, "(HTTP 400): bad shape"),
        ("413", "/rotate", Reply(413, {"error": "big"}), "refused", 1, "(HTTP 413): big"),
        ("422", "/rotate", Reply(422, {"error": "cert not acceptable"}), "refused", 1, "(HTTP 422)"),
        ("404", "/rotate", Reply(404, {}), "refused", 1, "does not look like a zfsbackup server"),
        ("500", "/rotate", Reply(500, {"error": "boom"}), "refused", 1, "answered HTTP 500"),
        ("503-retry", "/rotate", Reply(503, {"retry": True, "error": "busy"}), "busy", 75, "busy (HTTP 503)"),
        ("503-final", "/rotate", Reply(503, {"error": "dry", "reason": "dry_run"}), "refused", 1, "dry-run server"),
        ("502", "/rotate", Reply(502, ""), "unreachable", 75, "HTTP 502"),
        ("504", "/rotate", Reply(504, ""), "unreachable", 75, "HTTP 504"),
        ("200-other-cert", "/rotate", Reply(200, {"status": "rotated", "fingerprint": "ab" * 32}),
         "refused", 1, "pinned a different certificate"),
        ("200-unknown-status", "/rotate", Reply(200, {"status": "maybe", "fingerprint": "ab" * 32}),
         "refused", 1, "answered HTTP 200 to /rotate"),
        ("whoami-500", "/whoami", Reply(500, {"error": "boom"}), "refused", 1, "answered HTTP 500"),
        ("whoami-503", "/whoami", Reply(503, {"retry": True}), "busy", 75, "busy"),
        ("whoami-other-id", "/whoami", Reply(200, {"client_id": "someone", "fingerprint": None}),
         "refused", 1, "unexpected body"),
        ("whoami-not-trusted", "/whoami", Reply(403, {"error": "client not trusted"}),
         "refused", 1, "does not trust this host's current certificate"),
    ]

    @pytest.mark.parametrize("path, reply, result_word, code, fragment",
                             [c[1:] for c in CASES], ids=[c[0] for c in CASES])
    def test_scripted(self, run, one, path, reply, result_word, code, fragment):
        one.srv.on(path, reply)
        before = pair_bytes(one.db)
        result = rotate(run, one.db)
        assert result.exit_code == code, result.stderr
        assert table_rows(result) == {"offsite": result_word}
        assert fragment in result.stdout
        assert pair_bytes(one.db) == before and len(next_files(one.db)) == 2
        assert "NOTHING was swapped" in result.stderr

    def test_a_server_that_knows_the_certificate_under_another_id(self, run, one):
        one.srv.on("/whoami", Reply(200, {"client_id": "someone-else", "fingerprint": one.old.fingerprint}))
        result = rotate(run, one.db)
        assert result.exit_code == 1
        assert table_rows(result) == {"offsite": "refused"}
        assert "knows this certificate as client id 'someone-else', not 'host-1'" in result.stdout

    def test_a_revoked_current_certificate_is_refused_with_advice(self, run, one):
        revoke_pin(one.srv.resolved, one.old.fingerprint, lock=None)
        dispose_all()
        one.srv.refresh()
        result = rotate(run, one.db)
        assert result.exit_code == 1
        assert table_rows(result) == {"offsite": "refused"}
        assert "does not trust this host's current certificate" in result.stdout
        assert "enroll again" in result.stdout

    def test_a_timeout_is_unreachable(self, run, one, monkeypatch):
        monkeypatch.setattr(enroll_cli, "_TIMEOUT", (2.0, 0.5))
        one.srv.on("/rotate", HANG)
        result = rotate(run, one.db)
        assert result.exit_code == 75
        assert table_rows(result) == {"offsite": "unreachable"}
        assert "did not answer in time" in result.stdout

    def test_a_pin_mismatch_is_refused_and_nothing_is_sent_to_the_wrong_server(self, run, one):
        assert run("set", "destinations.offsite.server_fingerprint", "ee" * 32,
                   "-c", one.db).exit_code == 0
        one.reloads.clear()
        result = rotate(run, one.db)
        assert result.exit_code == 1
        assert table_rows(result) == {"offsite": "refused"}
        assert "presents certificate" in result.stdout
        assert one.srv.log == []

    def test_a_server_response_cannot_inject_control_characters(self, run, one):
        one.srv.on("/rotate", Reply(400, {"error": "x\x1b[31m\x07y\nZZZ"}))
        result = rotate(run, one.db)
        assert "\x1b" not in everything(result) and "\x07" not in everything(result)
        assert "ZZZ" in result.stdout.splitlines()[1]                 # one table row


# --------------------------------------------------------------------------
# Resets
# --------------------------------------------------------------------------


class TestResets:
    def test_a_reset_during_whoami_is_retried_once(self, run, one):
        one.srv.on("/whoami", RESET)
        result = rotate(run, one.db)
        assert result.exit_code == 0, result.stderr
        assert [e.path for e in one.srv.log] == ["/whoami", "/whoami", "/rotate"]

    def test_two_resets_during_whoami_are_unreachable(self, run, one):
        one.srv.on("/whoami", RESET, RESET)
        before = pair_bytes(one.db)
        result = rotate(run, one.db)
        assert result.exit_code == 75
        assert table_rows(result) == {"offsite": "unreachable"}
        assert [e.path for e in one.srv.log] == ["/whoami", "/whoami"]
        assert pair_bytes(one.db) == before

    def test_a_reset_on_the_rotate_post_is_not_retried_and_the_rerun_says_already(self, run, one):
        one.srv.on("/rotate", RESET)                                   # first only
        result = rotate(run, one.db)
        assert result.exit_code == 75
        assert len(one.srv.seen("/rotate")) == 1
        again = rotate(run, one.db)
        assert again.exit_code == 0, again.stderr
        assert table_rows(again) == {"offsite": "rotated"}      # the reset hit before the app


# --------------------------------------------------------------------------
# A swap that died between its two renames
# --------------------------------------------------------------------------


class TestInterruptedSwap:
    @pytest.fixture
    def half(self, run, one, monkeypatch):
        """Run a rotation whose second rename (certificate) fails: the servers all accepted the
        successor, the key has moved, the certificate has not."""
        real = os.rename
        state = {"fail": True}

        def flaky(src, dst, *a, **kw):
            if state["fail"] and Path(dst).name == "client.crt" and Path(src).name == "client.next.crt":
                raise OSError(5, "simulated crash")
            return real(src, dst, *a, **kw)

        monkeypatch.setattr(os, "rename", flaky)
        old = one.old
        result = rotate(run, one.db)
        state["fail"] = False
        assert result.exit_code == 1, result.stderr
        assert "every server accepted the successor, but it could not be installed here" in result.stderr
        assert (tls_of(one.db) / "client.next.crt").exists() and not (tls_of(one.db) / "client.next.key").exists()
        one.successor_fp = identity.fingerprint_pem((tls_of(one.db) / "client.next.crt").read_text())
        one.old_fp = old.fingerprint
        one.srv.log.clear()
        return one

    def test_the_rerun_finishes_it_without_asking_any_server(self, run, half):
        result = rotate(run, half.db)
        assert result.exit_code == 0, result.stderr
        assert "finished an interrupted rotation" in result.stdout
        assert half.successor_fp in result.stdout
        assert "Every server had already accepted it" in result.stdout
        assert half.srv.log == []                                      # not one request
        assert client_cert(half.db).fingerprint == half.successor_fp
        assert next_files(half.db) == []
        assert half.reloads == []

    def test_after_that_the_next_run_rotates_again(self, run, half):
        assert rotate(run, half.db).exit_code == 0
        result = rotate(run, half.db)
        assert result.exit_code == 0, result.stderr
        assert table_rows(result) == {"offsite": "rotated"}
        assert client_cert(half.db).fingerprint not in (half.successor_fp, half.old_fp)

    def test_nothing_to_finish_means_the_normal_path(self, run, one):
        result = rotate(run, one.db)
        assert result.exit_code == 0 and "finished an interrupted" not in result.stdout


# --------------------------------------------------------------------------
# Configured certificate paths
# --------------------------------------------------------------------------


class TestConfiguredPaths:
    @pytest.fixture
    def conf(self, run, db, reloads, make_server, tmp_path):
        srv = make_server("a")
        old = identity.ensure_identity(tmp_path / "mine", identity.ROLE_CLIENT)
        new = identity.ensure_identity(tmp_path / "next", identity.ROLE_CLIENT)
        make_db(run, db, PLAIN_YAML + f'client_cert_file: "{old.cert_path}"\n'
                                      f'client_key_file: "{old.key_path}"\n')
        seed_client_id(db, CID)
        token = srv.token()
        assert do_enroll(run, db, srv, token).exit_code == 0
        reloads.clear()
        srv.log.clear()
        return types.SimpleNamespace(db=db, srv=srv, old=old, new=new, reloads=reloads, token=token)

    def args(self, conf):
        return ("--cert", str(conf.new.cert_path), "--key", str(conf.new.key_path))

    def test_one_commit_and_exactly_one_reload(self, run, conf):
        gen = generation(conf.db)
        result = rotate(run, conf.db, *self.args(conf))
        assert result.exit_code == 0, result.stderr
        config = stored(conf.db)
        assert config.client_cert_file == conf.new.cert_path
        assert config.client_key_file == conf.new.key_path
        assert generation(conf.db) == gen + 1
        assert len(conf.reloads) == 1
        skip, held = conf.reloads[0]
        assert skip == held == store_key(conf.db)
        assert result.stdout.count("config updated (generation") == 1
        assert f"client certificate rotated: {conf.old.fingerprint} -> {conf.new.fingerprint}" in result.stdout
        assert "client_cert_file / client_key_file now point at" in result.stdout
        # the old files are left alone; the server pinned the new one
        assert conf.old.cert_path.exists() and conf.old.key_path.exists()
        assert conf.new.fingerprint in {p.fingerprint for p in conf.srv.clients()[CID].pins}
        assert not tls_of(conf.db).exists()                              # nothing generated

    def test_the_signal_comes_before_any_output(self, run, conf, monkeypatch):
        import click

        events = []
        real_echo = click.echo

        def echo(message=None, *a, **kw):
            events.append(("out", str(message)))
            return real_echo(message, *a, **kw)

        def recorder(resolved, *, skip_if_applied=None):
            events.append(("reload", skip_if_applied))
            return daemonlock.ReloadRequest("no-daemon")

        monkeypatch.setattr(click, "echo", echo)
        monkeypatch.setattr(daemonlock, "request_reload", recorder)
        assert rotate(run, conf.db, *self.args(conf)).exit_code == 0
        kinds = [k for k, _ in events]
        assert kinds.count("reload") == 1
        rotated = next(i for i, (k, m) in enumerate(events) if "certificate rotated" in m)
        assert kinds.index("reload") < rotated

    def test_relative_paths_are_stored_absolute(self, run, conf, tmp_path):
        rel_cert = os.path.relpath(conf.new.cert_path, tmp_path)
        rel_key = os.path.relpath(conf.new.key_path, tmp_path)
        result = rotate(run, conf.db, "--cert", rel_cert, "--key", rel_key)
        assert result.exit_code == 0, result.stderr
        assert stored(conf.db).client_cert_file == conf.new.cert_path

    def test_a_failure_commits_nothing_and_asks_for_no_reload(self, run, conf):
        conf.srv.on("/rotate", Reply(403, {"reason": "refused"}))
        before = db_state(conf.db)
        result = rotate(run, conf.db, *self.args(conf))
        assert result.exit_code == 1
        assert f"--cert {conf.new.cert_path} --key {conf.new.key_path}" in result.stderr
        untouched(conf.db, before, conf.reloads)

    def test_a_conflict_at_the_commit_is_75_and_the_rerun_completes(self, run, conf, monkeypatch):
        real = enroll_cli.commit_config
        state = {"armed": True}

        def competing(*args, **kwargs):
            if state["armed"]:
                state["armed"] = False
                assert Cli()("set", "check_interval", "10m", "-c", conf.db).exit_code == 0
            return real(*args, **kwargs)

        monkeypatch.setattr(enroll_cli, "commit_config", competing)
        first = rotate(run, conf.db, *self.args(conf))
        assert first.exit_code == 75, first.stderr
        assert "Every server accepted the successor but the configuration was not updated" in first.stderr
        assert stored(conf.db).client_cert_file == conf.old.cert_path
        conf.reloads.clear()
        second = rotate(run, conf.db, *self.args(conf))
        assert second.exit_code == 0, second.stderr
        assert table_rows(second) == {"offsite": "already"}
        assert stored(conf.db).client_cert_file == conf.new.cert_path
        assert len(conf.reloads) == 1

    def test_a_refused_successor_with_configured_paths_says_to_use_another_pair(self, run, conf):
        conf.srv.on("/rotate", Reply(403, {"reason": "refused"}))
        result = rotate(run, conf.db, *self.args(conf))
        assert result.exit_code == 1
        assert "re-run with a different --cert/--key pair" in result.stderr

    def test_a_refusal_beside_an_accepting_server_has_the_caution_after_the_other_pair_advice(
        self, run, conf, make_server
    ):
        b = make_server("b")
        assert do_enroll(run, conf.db, b, b.token(), name="second", fname="tb").exit_code == 0
        conf.reloads.clear()
        b.on("/rotate", Reply(403, {"reason": "refused"}))
        result = rotate(run, conf.db, *self.args(conf))
        assert result.exit_code == 1
        assert table_rows(result) == {"offsite": "rotated", "second": "refused"}
        flat = " ".join(result.stderr.split())
        successor = conf.new.fingerprint
        assert "re-run with a different --cert/--key pair." in flat
        assert f"Destination(s) 'offsite' already accepted the successor {successor}" in flat
        assert f"first run `zfsbackup-config trust revoke {successor}`" in flat
        assert flat.index("different --cert/--key pair.") < flat.index("Destination(s) 'offsite'")
        assert "delete " not in flat                            # configured files are the operator's
        assert stored(conf.db).client_cert_file == conf.old.cert_path
        assert conf.reloads == []

    def test_mixed_rerun_with_configured_paths(self, run, conf, make_server):
        b = make_server("b")
        assert do_enroll(run, conf.db, b, b.token(), name="second", fname="tb").exit_code == 0
        conf.reloads.clear()
        b.on("/rotate", Reply(503, {"retry": True}))
        first = rotate(run, conf.db, *self.args(conf))
        assert first.exit_code == 75
        assert stored(conf.db).client_cert_file == conf.old.cert_path
        assert conf.reloads == []
        second = rotate(run, conf.db, *self.args(conf))
        assert second.exit_code == 0 and table_rows(second) == {"offsite": "already", "second": "rotated"}
        assert len(conf.reloads) == 1

    def test_the_half_promotion_check_does_not_apply_to_configured_paths(self, run, conf):
        # a stray half-promoted generated directory next to the database is not this host's identity
        result = rotate(run, conf.db, *self.args(conf))
        assert "finished an interrupted" not in result.stdout


# --------------------------------------------------------------------------
# Usage and refusals
# --------------------------------------------------------------------------


class TestUsage:
    def untouched_state(self, database, srv=None):
        return db_state(database)

    @pytest.mark.parametrize("flag", ["--cert", "--key"])
    def test_one_of_cert_and_key_is_a_usage_error(self, run, one, flag):
        before = db_state(one.db)
        result = rotate(run, one.db, flag, str(one.db.parent / "x"))
        assert result.exit_code == 2 and "--cert and --key go together" in result.stderr
        assert one.srv.log == [] and next_files(one.db) == []
        untouched(one.db, before, one.reloads)

    def test_configured_paths_need_cert_and_key(self, run, db, reloads, make_server, tmp_path):
        old = identity.ensure_identity(tmp_path / "mine", identity.ROLE_CLIENT)
        make_db(run, db, PLAIN_YAML + f'client_cert_file: "{old.cert_path}"\n'
                                      f'client_key_file: "{old.key_path}"\n')
        reloads.clear()
        before = db_state(db)
        result = rotate(run, db)
        assert result.exit_code == 2
        assert "so the successor must be yours: pass --cert NEW --key NEW" in result.stderr
        untouched(db, before, reloads)

    def test_cert_and_key_with_generated_paths_are_a_usage_error(self, run, one):
        before = db_state(one.db)
        result = rotate(run, one.db, "--cert", "/x/c.crt", "--key", "/x/c.key")
        assert result.exit_code == 2
        assert "for installations that set client_cert_file/client_key_file" in result.stderr
        assert one.srv.log == [] and next_files(one.db) == []
        untouched(one.db, before, one.reloads)

    def test_the_configured_files_themselves_are_refused(self, run, db, reloads, tmp_path, make_server):
        old = identity.ensure_identity(tmp_path / "mine", identity.ROLE_CLIENT)
        make_db(run, db, PLAIN_YAML + f'client_cert_file: "{old.cert_path}"\n'
                                      f'client_key_file: "{old.key_path}"\n')
        seed_client_id(db, CID)
        srv = make_server()
        assert do_enroll(run, db, srv, srv.token()).exit_code == 0
        reloads.clear()
        srv.log.clear()
        before = db_state(db)
        for cert, key in ((old.cert_path, old.key_path),
                          (old.cert_path, tmp_path / "other.key"),
                          (tmp_path / "other.crt", old.key_path)):
            result = rotate(run, db, "--cert", str(cert), "--key", str(key))
            assert result.exit_code == 1, result.stderr
            assert "name the currently configured files" in result.stderr
        assert srv.log == []
        untouched(db, before, reloads)

    def test_a_successor_that_is_the_same_certificate_is_refused(self, run, db, reloads, tmp_path, make_server):
        old = identity.ensure_identity(tmp_path / "mine", identity.ROLE_CLIENT)
        copy = tmp_path / "copy"
        copy.mkdir()
        shutil.copy(old.cert_path, copy / "c.crt")
        shutil.copy(old.key_path, copy / "c.key")
        (copy / "c.key").chmod(0o600)
        make_db(run, db, PLAIN_YAML + f'client_cert_file: "{old.cert_path}"\n'
                                      f'client_key_file: "{old.key_path}"\n')
        seed_client_id(db, CID)
        srv = make_server()
        assert do_enroll(run, db, srv, srv.token()).exit_code == 0
        reloads.clear()
        srv.log.clear()
        result = rotate(run, db, "--cert", str(copy / "c.crt"), "--key", str(copy / "c.key"))
        assert result.exit_code == 1
        assert "the successor certificate is the current certificate" in result.stderr
        assert srv.log == []

    def test_a_missing_successor_file_is_refused(self, run, db, reloads, tmp_path, make_server):
        old = identity.ensure_identity(tmp_path / "mine", identity.ROLE_CLIENT)
        make_db(run, db, PLAIN_YAML + f'client_cert_file: "{old.cert_path}"\n'
                                      f'client_key_file: "{old.key_path}"\n')
        seed_client_id(db, CID)
        srv = make_server()
        assert do_enroll(run, db, srv, srv.token()).exit_code == 0
        reloads.clear()
        srv.log.clear()
        result = rotate(run, db, "--cert", str(tmp_path / "gone.crt"), "--key", str(tmp_path / "gone.key"))
        assert result.exit_code == 1 and "error:" in result.stderr
        assert srv.log == []

    def test_no_enrolled_destination(self, run, client, reloads):
        before = db_state(client)
        result = rotate(run, client)
        assert result.exit_code == 1
        assert "no destination is enrolled, so there is nothing to rotate" in result.stderr
        assert "enroll NAME --url URL" in result.stderr
        assert not tls_of(client).exists()                               # nothing generated
        untouched(client, before, reloads)

    def test_a_destination_without_a_pin_does_not_count_as_enrolled(self, run, db, reloads):
        client_with(run, db, reloads, {"offsite": ("https://127.0.0.1:1", None)})
        result = rotate(run, db)
        assert result.exit_code == 1 and "nothing to rotate" in result.stderr
        assert not tls_of(db).exists()

    def test_no_client_id(self, run, db, reloads):
        make_db(run, db, PLAIN_YAML + 'destinations:\n  offsite:\n    url: "https://127.0.0.1:1"\n'
                                      f'    server_fingerprint: "{"ab" * 32}"\n')
        reloads.clear()
        assert host_client_id(db) is None
        result = rotate(run, db)
        assert result.exit_code == 1 and "nothing to rotate" in result.stderr
        assert not tls_of(db).exists()

    def test_the_current_certificate_missing_is_an_error(self, run, db, reloads):
        client_with(run, db, reloads, {"offsite": ("https://127.0.0.1:1", "ab" * 32)})
        result = rotate(run, db)
        assert result.exit_code == 1 and "error:" in result.stderr
        assert not any(tls_of(db).glob("client.next.*")) if tls_of(db).exists() else True

    def test_the_help_lists_the_command(self, run):
        result = run("identity", "--help")
        assert "rotate" in result.stdout
        result = run("identity", "rotate", "--help")
        assert result.exit_code == 0
        assert "-c, --config" in result.stdout and "-v, --verbose" in result.stdout
        assert "--cert" in result.stdout and "--key" in result.stdout

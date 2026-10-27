"""Plan S-2: a RUNNING `/stream` is cut when its client certificate is revoked, and a write to a
client that stopped reading is bounded.

Before S-2 a revocation reached new connections only: a stream already running finished (a
stolen key kept its transfer, and the dataset claim and gate slot with it), and a peer that
completed the handshake and never read could block a write for ever. `api.SessionRegistry`
(`app.extensions['zfsbackup.sessions']`) knows the running sessions; the API worker's trust-poll
thread calls `enforce(gateway)` after every poll.

Layout
------

* `TestCutReasons` / `TestRevocationChains` / `TestCheckCaching` / `TestListRevokedFailure` /
  `TestRegistryBookkeeping` / `TestWriteStallCheck` -- `SessionRegistry` on fakes (no TLS, no
  database): which records cut, the `superseded_by` chain walk, the per-view-key cache, the
  failed read, registration, the cut racing `unregister`, the stall check.
* `TestGatewayListRevoked` -- `TrustGateway.list_revoked` (its seam, the store lock, the real
  store).
* `TestRouteRegistration` / `TestRouteCut` -- the `/stream` route on a mock websocket:
  registered at start and unregistered on every exit path, after `_end_websocket`; a session
  cut mid-receive stops writing, reaps once, sends no ack and releases the claim and the gate.
* `TestRevocationOverRealTls` -- the real route behind the real TLS server, with a REAL receive
  child process, the real `store.trust` writers and the real trust-poll thread: every cutting
  reason, a rotation that does not cut, and the retired key that IS cut when a successor is
  revoked.
* `TestWriteStallOverRealTls` -- a client that pings and never reads blocks the server's pong;
  the stall bound cuts it, the route unwinds, the gate drains.
* `TestSessionsInARealApiWorker` -- a real `ApiWorker` process under fork, spawn and forkserver.

Hygiene: sockets are `127.0.0.1` port 0, the `zfs` calls are patched (`_session_support`), the
receive children are killed in a finalizer, and every wait is generous (a loaded host).
"""

import contextlib
import json
import logging
import multiprocessing
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from simple_websocket import ConnectionClosed

from _api_support import Authed, CLIENT_ID, streams_in_progress
from _session_support import (
    CLIENT,
    DATASET,
    SERVER_DATASET,
    ProcessRig,
    StreamRig,
    eventually,
    finish,
    ping_flood,
    remote_config,
    zfs_answers,
)
from _stream_support import DONE, HEADER
from _tls_support import client_identity, make_gateway, pin_client
from zfsbackup import api
from zfsbackup.api import (
    CUT_REVOCATION_REASONS,
    NOT_CUT_REVOCATION_REASONS,
    PinInfo,
    SessionRegistry,
    StreamSession,
    _ws_force_eof,
)
from zfsbackup.config import store
from zfsbackup.config.store import NewPin, RevokedPin
from zfsbackup.config.store import trust as store_trust
from zfsbackup.config.store.models import REVOCATION_REASONS
from zfsbackup.remote import WS_IDLE_TIMEOUT

API_LOGGER = "zfsbackup.api"
CUTTING = "cutting the running stream"
SERVER = "pool/backups/alice/data"

START_METHODS = [
    m for m in ("fork", "spawn", "forkserver") if m in multiprocessing.get_all_start_methods()
]


# ---------------------------------------------------------------------------
# Fakes for the registry
# ---------------------------------------------------------------------------


def fp(letter: str) -> str:
    """A well-formed fingerprint made of one hex digit."""
    return letter * 64


class FakeSock:
    """What `SessionRegistry` asks of `ws.sock`: `force_eof` and the stall check."""

    def __init__(self, stalled=None):
        self.stalled = stalled
        self.limits = []
        self.eof = 0

    def force_eof(self):
        self.eof += 1

    def force_eof_if_write_stalled(self, limit):
        self.limits.append(limit)
        return self.stalled


class FakeWs:
    def __init__(self, sock=None):
        self.sock = sock if sock is not None else FakeSock()


class FakeTrust:
    """The gateway as `enforce` sees it: `view.key`, `view.pins` and `list_revoked()`."""

    def __init__(self, records=(), pins=(), key=(7, 1)):
        self.view = SimpleNamespace(
            key=key, pins={p: PinInfo("alice", 4e12, None) for p in pins}
        )
        self.records = list(records)
        self.calls = 0
        self.fail = None
        self.on_read = None

    def list_revoked(self):
        self.calls += 1
        if self.on_read is not None:
            self.on_read()
        if self.fail is not None:
            raise self.fail
        return tuple(self.records)

    def bump(self, *, pins=None):
        """A new trust generation (a new view; the real one is immutable too)."""
        store_id, generation = self.view.key
        self.view = SimpleNamespace(
            key=(store_id, generation + 1),
            pins=self.view.pins if pins is None else {
                p: PinInfo("alice", 4e12, None) for p in pins
            },
        )


def rec(fingerprint, reason, superseded_by=None, client_id="alice"):
    return RevokedPin(fingerprint, client_id, 1.0, reason, superseded_by)


def make_session(fingerprint=fp("a"), *, sock=None, client_id="alice", server_dataset=SERVER):
    session = StreamSession(FakeWs(sock), fingerprint, client_id, DATASET)
    session.server_dataset = server_dataset
    return session


@pytest.fixture
def registry():
    return SessionRegistry()


def warnings_of(caplog, needle=CUTTING):
    return [r for r in caplog.records
            if r.name == API_LOGGER and r.levelno == logging.WARNING and needle in r.getMessage()]


@pytest.fixture
def log(caplog):
    caplog.set_level(logging.DEBUG, logger=API_LOGGER)
    return caplog


# ---------------------------------------------------------------------------
# Which records cut
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCutReasons:
    def test_the_reasons_known_today_and_the_one_exception(self):
        assert CUT_REVOCATION_REASONS == {
            "revoked", "successor_revoked", "client_revoked", "replaced",
        }
        assert NOT_CUT_REVOCATION_REASONS == {"retired"}
        # every reason the store can record is one or the other
        assert set(REVOCATION_REASONS) == CUT_REVOCATION_REASONS | NOT_CUT_REVOCATION_REASONS

    @pytest.mark.parametrize("reason", sorted(set(REVOCATION_REASONS) - {"retired"}))
    def test_the_rule_is_a_denylist_every_reason_but_retired_cuts(self, reason):
        assert SessionRegistry._revocation(fp("a"), {fp("a"): rec(fp("a"), reason)}) is not None
        assert SessionRegistry._revocation(fp("a"), {fp("a"): rec(fp("a"), "retired")}) is None

    @pytest.mark.parametrize("reason", sorted(CUT_REVOCATION_REASONS))
    def test_each_cutting_reason_cuts_the_session_once(self, registry, log, reason):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), reason)])
        assert registry.enforce(trust) == 1
        assert session.ws.sock.eof == 1
        assert session.cut_reason is not None and reason in session.cut_reason
        (warning,) = warnings_of(log)
        text = warning.getMessage()
        assert "alice" in text and SERVER in text and reason in text

    def test_the_log_names_the_client_id_and_dataset_of_the_session(self, registry, log):
        session = make_session(client_id="host-7", server_dataset="pool/backups/host-7/x")
        registry.register(session)
        registry.enforce(FakeTrust(records=[rec(fp("a"), "revoked", client_id="host-7")]))
        (warning,) = warnings_of(log)
        assert "host-7" in warning.getMessage() and "pool/backups/host-7/x" in warning.getMessage()

    def test_before_the_server_dataset_is_known_the_client_dataset_is_named(self, registry, log):
        session = make_session(server_dataset=None)
        registry.register(session)
        registry.enforce(FakeTrust(records=[rec(fp("a"), "revoked")]))
        (warning,) = warnings_of(log)
        assert repr(DATASET) in warning.getMessage()

    def test_retired_alone_does_not_cut(self, registry, log):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "retired")])      # no superseded_by
        assert registry.enforce(trust) == 0
        assert session.ws.sock.eof == 0 and session.cut_reason is None
        assert warnings_of(log) == []
        assert session.checked_key == trust.view.key

    @pytest.mark.parametrize("reason", ["something-new", "compromised", ""])
    def test_an_unknown_reason_cuts_too(self, registry, log, reason):
        """F5: a reason added to the store later cuts by default; only `retired` is exempt."""
        session = make_session()
        registry.register(session)
        assert registry.enforce(FakeTrust(records=[rec(fp("a"), reason)])) == 1
        assert session.cut_reason is not None and session.ws.sock.eof == 1
        assert len(warnings_of(log)) == 1

    def test_no_record_at_all_cuts_nothing(self, registry):
        """E.g. the database file was replaced: the pin is gone from the view and nothing says
        why. That is not a revocation."""
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("b"), "revoked")])      # somebody else's record
        assert registry.enforce(trust) == 0
        assert session.cut_reason is None and session.ws.sock.eof == 0

    def test_only_the_revoked_session_of_several_is_cut(self, registry):
        revoked, healthy, other = make_session(fp("a")), make_session(fp("b")), make_session(fp("c"))
        for s in (revoked, healthy, other):
            registry.register(s)
        trust = FakeTrust(records=[rec(fp("a"), "revoked"), rec(fp("c"), "retired")], pins=[fp("b")])
        assert registry.enforce(trust) == 1
        assert [s.cut_reason is not None for s in (revoked, healthy, other)] == [True, False, False]
        assert trust.calls == 1                       # one read for all of them

    def test_a_session_whose_pin_is_in_the_view_is_never_cut_by_a_record(self, registry):
        """The view is the authority for "still trusted"; a stale or odd record for a key that
        is pinned must not cut it (and costs no store call)."""
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")], pins=[fp("a")])
        assert registry.enforce(trust) == 0
        assert trust.calls == 0
        assert session.cut_reason is None and session.ws.sock.eof == 0

    def test_a_view_that_has_read_nothing_yet_cuts_nothing_and_asks_nothing(self, registry):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")], key=None)
        assert registry.enforce(trust) == 0
        assert trust.calls == 0 and session.cut_reason is None

    def test_a_force_eof_that_raises_still_marks_the_session_cut(self, registry, log):
        sock = FakeSock()

        def boom():
            raise OSError("bad file descriptor")

        sock.force_eof = boom
        session = make_session(sock=sock)
        registry.register(session)
        assert registry.enforce(FakeTrust(records=[rec(fp("a"), "revoked")])) == 1
        assert session.cut_reason is not None
        assert len(warnings_of(log)) == 1

    def test_a_websocket_with_nothing_to_shut_down_is_still_marked_cut(self, registry):
        session = StreamSession(SimpleNamespace(), fp("a"), "alice", DATASET)
        registry.register(session)
        assert registry.enforce(FakeTrust(records=[rec(fp("a"), "revoked")])) == 1
        assert session.cut_reason is not None

    def test_the_stall_limit_default_is_the_idle_timeout(self):
        assert SessionRegistry()._stall_limit == WS_IDLE_TIMEOUT

    def test_every_app_has_its_own_registry(self, mocker):
        a, b = Authed(remote_config()), Authed(remote_config())
        ra, rb = (x.app.extensions["zfsbackup.sessions"] for x in (a, b))
        assert isinstance(ra, SessionRegistry) and ra is not rb
        assert len(ra) == 0


@pytest.mark.unit
class TestWsForceEof:
    def test_the_session_sockets_force_eof_is_preferred(self):
        sock = FakeSock()
        assert _ws_force_eof(FakeWs(sock)) == sock.force_eof

    def test_a_plain_socket_is_shut_down_with_the_os_call(self, mocker):
        import socket

        a, b = socket.socketpair()
        try:
            calls = mocker.patch.object(socket.socket, "shutdown")
            force = _ws_force_eof(SimpleNamespace(sock=a))
            force()
            calls.assert_called_once_with(a, socket.SHUT_RDWR)
        finally:
            a.close()
            b.close()

    @pytest.mark.parametrize("sock", [None, object(), SimpleNamespace(force_eof=None)])
    def test_anything_else_has_no_way_to_shut_down(self, sock):
        assert _ws_force_eof(SimpleNamespace(sock=sock)) is None
        assert _ws_force_eof(SimpleNamespace()) is None


# ---------------------------------------------------------------------------
# Chains: a retired key is followed along superseded_by
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRevocationChains:
    def run(self, registry, records, **kw):
        session = make_session(fp("a"), **kw)
        registry.register(session)
        trust = FakeTrust(records=records)
        return session, registry.enforce(trust)

    @pytest.mark.parametrize("reason", sorted(CUT_REVOCATION_REASONS))
    def test_a_retired_key_is_cut_when_its_successor_has_a_cutting_reason(
        self, registry, log, reason
    ):
        session, cuts = self.run(registry, [rec(fp("a"), "retired", fp("b")), rec(fp("b"), reason)])
        assert cuts == 1
        assert session.ws.sock.eof == 1
        text = session.cut_reason
        assert fp("b") in text and reason in text, "the successor and why are named"
        (warning,) = warnings_of(log)
        assert fp("b") in warning.getMessage()

    def test_two_deep_a_to_b_to_c(self, registry):
        session, cuts = self.run(registry, [
            rec(fp("a"), "retired", fp("b")),
            rec(fp("b"), "retired", fp("c")),
            rec(fp("c"), "revoked"),
        ])
        assert cuts == 1 and fp("c") in session.cut_reason

    def test_three_deep(self, registry):
        session, cuts = self.run(registry, [
            rec(fp("a"), "retired", fp("b")),
            rec(fp("b"), "retired", fp("c")),
            rec(fp("c"), "retired", fp("d")),
            rec(fp("d"), "client_revoked"),
        ])
        assert cuts == 1 and fp("d") in session.cut_reason

    def test_a_cutting_reason_in_the_middle_of_the_chain_cuts_at_that_link(self, registry):
        session, cuts = self.run(registry, [
            rec(fp("a"), "retired", fp("b")),
            rec(fp("b"), "successor_revoked"),
            rec(fp("c"), "revoked"),
        ])
        assert cuts == 1 and fp("b") in session.cut_reason and fp("c") not in session.cut_reason

    @pytest.mark.parametrize("records", [
        pytest.param([rec(fp("a"), "retired", fp("a"))], id="self-loop"),
        pytest.param([rec(fp("a"), "retired", fp("b")), rec(fp("b"), "retired", fp("a"))],
                     id="two-cycle"),
        pytest.param([rec(fp("a"), "retired", fp("b")), rec(fp("b"), "retired", fp("c")),
                      rec(fp("c"), "retired", fp("b"))], id="cycle-not-through-the-start"),
    ])
    def test_a_cycle_terminates_and_cuts_nothing(self, registry, records):
        session, cuts = self.run(registry, records)
        assert cuts == 0 and session.cut_reason is None

    def test_a_chain_ending_at_a_live_pin_cuts_nothing(self, registry):
        """The successor is pinned (so in the view and in no record): the rotation is ordinary."""
        session = make_session(fp("a"))
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "retired", fp("b"))], pins=[fp("b")])
        assert registry.enforce(trust) == 0
        assert session.cut_reason is None

    def test_a_chain_ending_in_no_record_cuts_nothing(self, registry):
        session, cuts = self.run(registry, [rec(fp("a"), "retired", fp("b"))])
        assert cuts == 0 and session.cut_reason is None

    def test_a_retired_link_without_superseded_by_ends_the_chain(self, registry):
        """Written before the column existed."""
        session, cuts = self.run(registry, [
            rec(fp("a"), "retired", fp("b")), rec(fp("b"), "retired", None), rec(fp("c"), "revoked"),
        ])
        assert cuts == 0

    def test_superseded_by_is_followed_only_from_a_retired_record(self, registry):
        """A record with another reason cuts on its own account; its `superseded_by` is not a
        link to follow."""
        session, cuts = self.run(registry, [
            rec(fp("a"), "something-new", fp("b")), rec(fp("b"), "revoked"),
        ])
        assert cuts == 1
        assert "something-new" in session.cut_reason and fp("b") not in session.cut_reason

    def test_an_unknown_reason_down_the_chain_cuts(self, registry):
        session, cuts = self.run(registry, [
            rec(fp("a"), "retired", fp("b")), rec(fp("b"), "something-new"),
        ])
        assert cuts == 1 and fp("b") in session.cut_reason and "something-new" in session.cut_reason

    def test_the_chain_is_read_from_the_records_of_one_listing(self, registry):
        session, cuts = self.run(registry, [
            rec(fp("a"), "retired", fp("b")), rec(fp("b"), "retired", fp("c")),
            rec(fp("c"), "revoked"),
        ])
        # a chain of any depth costs one read
        assert cuts == 1


# ---------------------------------------------------------------------------
# The per-view-key cache
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckCaching:
    def test_an_unchanged_key_asks_the_store_once(self, registry):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "retired", fp("b"))])
        for _ in range(5):
            assert registry.enforce(trust) == 0
        assert trust.calls == 1
        assert session.checked_key == trust.view.key

    def test_a_bumped_key_checks_again_and_cuts_when_the_chain_now_reaches_a_revocation(
        self, registry
    ):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "retired", fp("b"))])
        assert registry.enforce(trust) == 0 and trust.calls == 1
        trust.records.append(rec(fp("b"), "revoked"))        # the successor was revoked later
        assert registry.enforce(trust) == 0 and trust.calls == 1     # same key: not re-read
        trust.bump()
        assert registry.enforce(trust) == 1 and trust.calls == 2
        assert session.cut_reason is not None

    def test_a_bumped_key_without_a_cut_records_the_new_key(self, registry):
        session = make_session()
        registry.register(session)
        trust = FakeTrust()
        registry.enforce(trust)
        old_key = session.checked_key
        trust.bump()
        registry.enforce(trust)
        assert session.checked_key == trust.view.key != old_key
        assert trust.calls == 2

    def test_each_session_is_checked_at_its_own_pace(self, registry):
        early, late = make_session(fp("a")), make_session(fp("b"))
        registry.register(early)
        trust = FakeTrust()
        registry.enforce(trust)
        registry.register(late)
        registry.enforce(trust)                 # only `late` is new at this key
        assert trust.calls == 2 and early.checked_key == late.checked_key == trust.view.key

    def test_a_session_that_returned_to_the_view_costs_nothing_and_is_not_cut(self, registry):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")], pins=[fp("a")])
        for _ in range(3):
            assert registry.enforce(trust) == 0
        assert trust.calls == 0

    def test_no_double_cut(self, registry, log):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")])
        assert registry.enforce(trust) == 1
        trust.bump()
        assert registry.enforce(trust) == 0
        assert registry.enforce(trust) == 0
        assert session.ws.sock.eof == 1
        assert len(warnings_of(log)) == 1
        assert trust.calls == 1, "a cut session is not asked about again"

    def test_a_stalled_session_is_not_cut_again_by_a_revocation(self, registry, log):
        sock = FakeSock(stalled=7.5)
        session = make_session(sock=sock)
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")])
        assert registry.enforce(trust) == 1
        assert len(warnings_of(log)) == 1
        assert trust.calls == 0, "already cut: the revocation pass skips it"


# ---------------------------------------------------------------------------
# A failed list_revoked
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestListRevokedFailure:
    def arm(self, registry):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")])
        trust.fail = store.ConfigBusy("another trust operation is using the database")
        return session, trust

    def test_a_failed_read_cuts_nothing_and_warns_once(self, registry, log):
        session, trust = self.arm(registry)
        for _ in range(4):
            assert registry.enforce(trust) == 0
        assert session.cut_reason is None and session.ws.sock.eof == 0
        failures = warnings_of(log, "cannot check")
        assert len(failures) == 1, "the same failure is not repeated every tick"
        text = failures[0].getMessage()
        assert "1 running stream" in text and "ConfigBusy" in text

    def test_it_is_retried_on_the_same_key_until_it_works(self, registry, log):
        session, trust = self.arm(registry)
        registry.enforce(trust)
        registry.enforce(trust)
        assert trust.calls == 2 and session.checked_key is None
        trust.fail = None
        assert registry.enforce(trust) == 1                  # same key, now it reads
        assert trust.calls == 3 and session.cut_reason is not None

    def test_recovery_is_one_info_line(self, registry, log):
        session, trust = self.arm(registry)
        registry.enforce(trust)
        trust.fail = None
        trust.records = []
        registry.enforce(trust)
        infos = [r for r in log.records if r.levelno == logging.INFO
                 and "can be read again" in r.getMessage()]
        assert len(infos) == 1
        assert session.checked_key == trust.view.key
        registry.enforce(trust)
        assert len([r for r in log.records if "can be read again" in r.getMessage()]) == 1

    def test_a_different_failure_warns_again(self, registry, log):
        session, trust = self.arm(registry)
        registry.enforce(trust)
        trust.fail = RuntimeError("disk I/O error")
        registry.enforce(trust)
        messages = [w.getMessage() for w in warnings_of(log, "cannot check")]
        assert len(messages) == 2 and "RuntimeError" in messages[1]

    def test_a_failure_does_not_stop_the_stall_check(self, registry, log):
        sock = FakeSock(stalled=3.0)
        session = make_session(sock=sock)
        registry.register(session)
        trust = FakeTrust()
        trust.fail = RuntimeError("boom")
        assert registry.enforce(trust) == 1                  # cut for the stall, not the read
        assert "blocked" in session.cut_reason

    def test_enforce_never_raises(self, registry):
        registry.register(make_session())

        class Broken:
            @property
            def view(self):
                raise RuntimeError("no view")

        assert registry.enforce(Broken()) == 0


class BrokenSockWs:
    """A websocket whose `sock` cannot be read (what a per-session failure looks like)."""

    broken = True

    @property
    def sock(self):
        if self.broken:
            raise RuntimeError("the socket is in a state nobody expected")
        return FakeSock()


@pytest.mark.unit
class TestPerSessionIsolation:
    """F3: one session's failure never costs the others their pass, and a persistent
    failure is one WARNING (INFO when it clears)."""

    def test_a_failing_stall_check_does_not_stop_the_next_session(self, registry, log):
        bad = StreamSession(BrokenSockWs(), fp("a"), "alice", DATASET)
        good = make_session(fp("b"), sock=FakeSock(stalled=8.0))
        registry.register(bad)
        registry.register(good)
        assert registry.enforce_stalls() == 1
        assert good.cut_reason is not None and bad.cut_reason is None

    def test_a_persistent_stall_failure_is_one_warning_and_clears_with_one_info(
        self, registry, log
    ):
        ws = BrokenSockWs()
        registry.register(StreamSession(ws, fp("a"), "alice", DATASET))
        for _ in range(4):
            assert registry.enforce_stalls() == 0
        failures = warnings_of(log, "write-stall check")
        assert len(failures) == 1 and "RuntimeError" in failures[0].getMessage()
        ws.broken = False
        registry.enforce_stalls()
        registry.enforce_stalls()
        cleared = [r for r in log.records if "write-stall check works again" in r.getMessage()]
        assert len(cleared) == 1 and cleared[0].levelno == logging.INFO

    def test_a_failing_cut_does_not_stop_the_other_cuts(self, registry, log):
        bad = StreamSession(BrokenSockWs(), fp("a"), "alice", DATASET)
        good = make_session(fp("b"))
        registry.register(bad)
        registry.register(good)
        trust = FakeTrust(records=[rec(fp("a"), "revoked"), rec(fp("b"), "revoked")])
        assert registry.enforce_revocations(trust) == 1
        assert good.cut_reason is not None and good.ws.sock.eof == 1
        failures = warnings_of(log, "revocation check of a running stream failed")
        assert len(failures) == 1 and "RuntimeError" in failures[0].getMessage()

    def test_the_session_whose_own_check_failed_is_not_marked_checked(self, registry):
        bad = StreamSession(BrokenSockWs(), fp("a"), "alice", DATASET)
        quiet = make_session(fp("b"))
        registry.register(bad)
        registry.register(quiet)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")])
        registry.enforce_revocations(trust)
        assert quiet.checked_key == trust.view.key       # nothing cut it: checked
        assert bad.checked_key is None                   # its own work failed

    def test_a_failed_revocation_pass_recovers_with_an_info(self, registry, log):
        registry.register(make_session())

        class Flaky:
            broken = True

            @property
            def view(self):
                if self.broken:
                    raise RuntimeError("no view")
                return FakeTrust().view

            def list_revoked(self):
                return ()

        flaky = Flaky()
        registry.enforce_revocations(flaky)
        registry.enforce_revocations(flaky)
        assert len(warnings_of(log, "revocation check of running streams failed")) == 1
        flaky.broken = False
        registry.enforce_revocations(flaky)
        assert [r for r in log.records if "revocation check works again" in r.getMessage()]


@pytest.mark.unit
class TestTwoPasses:
    """F4: the stall pass needs no database; the two passes are separable."""

    def test_enforce_stalls_needs_no_gateway_and_does_no_revocation_work(self, registry):
        stalled = make_session(fp("a"), sock=FakeSock(stalled=5.0))
        revoked = make_session(fp("b"))
        registry.register(stalled)
        registry.register(revoked)
        assert registry.enforce_stalls() == 1
        assert revoked.cut_reason is None and revoked.ws.sock.eof == 0

    def test_enforce_revocations_does_no_stall_check(self, registry):
        sock = FakeSock(stalled=5.0)
        session = make_session(sock=sock)
        registry.register(session)
        assert registry.enforce_revocations(FakeTrust(pins=[fp("a")])) == 0
        assert sock.limits == [] and session.cut_reason is None

    def test_enforce_is_both_passes_stalls_first(self, registry):
        order = []
        sock = FakeSock()
        sock.force_eof_if_write_stalled = lambda limit: order.append("stall")
        registry.register(make_session(sock=sock))
        trust = FakeTrust()
        trust.list_revoked = lambda: order.append("read") or ()
        registry.enforce(trust)
        assert order == ["stall", "read"]

    def test_a_skipped_read_marks_nothing_checked_and_is_retried_next_tick(self, registry):
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")])
        trust.fail = store.ConfigBusy("another trust operation is still using the database")
        assert registry.enforce_revocations(trust) == 0
        assert session.checked_key is None
        trust.fail = None
        assert registry.enforce_revocations(trust) == 1


# ---------------------------------------------------------------------------
# Registration and the race with unregister
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRegistryBookkeeping:
    def test_register_unregister_and_len(self, registry):
        a, b = make_session(fp("a")), make_session(fp("b"))
        registry.register(a)
        registry.register(b)
        assert len(registry) == 2
        registry.unregister(a)
        assert len(registry) == 1
        registry.unregister(a)                                # idempotent
        registry.unregister(b)
        assert len(registry) == 0

    def test_enforce_with_no_session_asks_the_gateway_nothing(self, registry):
        class Strict:
            @property
            def view(self):
                raise AssertionError("the gateway must not be touched with no session")

        assert registry.enforce(Strict()) == 0

    def test_an_unregistered_session_is_never_cut(self, registry):
        gone, kept = make_session(fp("a")), make_session(fp("b"))
        registry.register(gone)
        registry.register(kept)
        registry.unregister(gone)
        trust = FakeTrust(records=[rec(fp("a"), "revoked"), rec(fp("b"), "revoked")])
        assert registry.enforce(trust) == 1
        assert gone.ws.sock.eof == 0 and gone.cut_reason is None
        assert kept.ws.sock.eof == 1

    def test_a_session_unregistered_after_the_snapshot_is_not_cut(self, registry, log):
        """`enforce` works from a snapshot of the sessions; the route may unregister (and the
        server close its connection, freeing the descriptor number) in between. The cut
        re-checks membership under the registry's lock."""
        session = make_session()
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")])
        trust.on_read = lambda: registry.unregister(session)
        assert registry.enforce(trust) == 0
        assert session.ws.sock.eof == 0 and session.cut_reason is None
        assert warnings_of(log) == []

    def test_a_stall_cut_of_a_session_unregistered_after_the_snapshot_does_nothing(
        self, registry
    ):
        sock = FakeSock(stalled=99.0)
        session = make_session(sock=sock)
        # never registered / already unregistered: the private step must refuse it
        assert registry._cut_if_stalled(session) is False
        assert sock.limits == [] and session.cut_reason is None

    def test_unregister_waits_for_a_cut_in_progress_and_no_cut_follows_it(self, registry):
        """The descriptor rule: werkzeug closes the connection after the route returns, so
        `unregister` (the route's last act) must not return while a cut is still inside
        `shutdown`, and nothing may start one afterwards."""
        entered, release = threading.Event(), threading.Event()
        sock = FakeSock()
        sock.force_eof = lambda: (entered.set(), release.wait(30), setattr(sock, "eof", sock.eof + 1))
        session = make_session(sock=sock)
        registry.register(session)
        trust = FakeTrust(records=[rec(fp("a"), "revoked")])
        enforcer = threading.Thread(target=registry.enforce, args=(trust,), daemon=True)
        enforcer.start()
        assert entered.wait(30)
        returned = threading.Event()
        unregistrar = threading.Thread(
            target=lambda: (registry.unregister(session), returned.set()), daemon=True
        )
        unregistrar.start()
        try:
            assert not returned.wait(0.4), "unregister returned while a cut was still running"
            assert sock.eof == 0
        finally:
            release.set()
        enforcer.join(30)
        unregistrar.join(30)
        assert returned.is_set() and sock.eof == 1
        trust.bump()
        assert registry.enforce(trust) == 0
        assert sock.eof == 1, "a cut ran after unregister returned"

    def test_a_cut_racing_unregister_never_calls_force_eof_after_unregister_returned(
        self, registry
    ):
        late = []
        for i in range(300):
            gone = threading.Event()
            sock = FakeSock()
            sock.force_eof = lambda gone=gone: late.append(i) if gone.is_set() else None
            session = make_session(sock=sock)
            registry.register(session)
            trust = FakeTrust(records=[rec(fp("a"), "revoked")])
            start = threading.Barrier(2)

            def cut():
                start.wait(30)
                registry.enforce(trust)

            def leave():
                start.wait(30)
                registry.unregister(session)
                gone.set()

            threads = [threading.Thread(target=f, daemon=True) for f in (cut, leave)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(30)
            registry.unregister(session)
        assert late == []

    def test_enforce_is_serialised_with_register(self, registry):
        """A thread registering while another enforces never corrupts the list."""
        trust = FakeTrust()
        stop = threading.Event()
        errors = []

        def churn():
            try:
                while not stop.is_set():
                    s = make_session()
                    registry.register(s)
                    registry.unregister(s)
            except BaseException as exc:    # noqa: BLE001
                errors.append(exc)

        t = threading.Thread(target=churn, daemon=True)
        t.start()
        try:
            for _ in range(500):
                registry.enforce(trust)
        finally:
            stop.set()
            t.join(30)
        assert errors == []


# ---------------------------------------------------------------------------
# The stall check
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWriteStallCheck:
    def test_an_age_cuts_with_the_age_in_the_reason(self, registry, log):
        sock = FakeSock(stalled=12.7)
        session = make_session(sock=sock)
        registry.register(session)
        assert registry.enforce(FakeTrust(pins=[fp("a")])) == 1
        assert session.cut_reason == "a write to the client has been blocked for 13s"
        (warning,) = warnings_of(log)
        text = warning.getMessage()
        assert "alice" in text and SERVER in text and "blocked for 13s" in text

    def test_the_registry_does_not_shut_the_socket_itself(self, registry):
        """The socket's own check does the shutdown, atomically with reading the write's age;
        a second `force_eof` from the registry could hit a NEW write."""
        sock = FakeSock(stalled=5.0)
        registry.register(make_session(sock=sock))
        registry.enforce(FakeTrust(pins=[fp("a")]))
        assert sock.eof == 0

    def test_none_means_not_stalled(self, registry, log):
        sock = FakeSock(stalled=None)
        session = make_session(sock=sock)
        registry.register(session)
        assert registry.enforce(FakeTrust(pins=[fp("a")])) == 0
        assert session.cut_reason is None and warnings_of(log) == []
        assert sock.limits == [registry._stall_limit]

    def test_the_registrys_limit_is_what_is_passed(self, log):
        sock = FakeSock()
        registry = SessionRegistry(stall_limit=3.5)
        registry.register(make_session(sock=sock))
        registry.enforce(FakeTrust(pins=[fp("a")]))
        registry.enforce(FakeTrust(pins=[fp("a")]))
        assert sock.limits == [3.5, 3.5]

    def test_every_tick_asks_every_session(self, registry):
        socks = [FakeSock() for _ in range(3)]
        for i, sock in enumerate(socks):
            registry.register(make_session(fp("abc"[i]), sock=sock))
        trust = FakeTrust(pins=[fp("a"), fp("b"), fp("c")])
        registry.enforce(trust)
        registry.enforce(trust)
        assert [len(s.limits) for s in socks] == [2, 2, 2]

    @pytest.mark.parametrize("sock", [None, object(), SimpleNamespace(),
                                      SimpleNamespace(force_eof_if_write_stalled=None)])
    def test_a_socket_without_the_check_is_never_cut_for_a_stall(self, registry, sock):
        session = StreamSession(SimpleNamespace(sock=sock), fp("a"), "alice", DATASET)
        registry.register(session)
        assert registry.enforce(FakeTrust(pins=[fp("a")])) == 0
        assert session.cut_reason is None

    def test_a_stall_cut_is_reported_by_one_warning_even_if_revoked_too(self, registry, log):
        session = make_session(sock=FakeSock(stalled=9.0))
        registry.register(session)
        registry.enforce(FakeTrust(records=[rec(fp("a"), "revoked")]))
        assert len(warnings_of(log)) == 1


# ---------------------------------------------------------------------------
# TrustGateway.list_revoked
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGatewayListRevoked:
    def test_it_is_the_seam_through_the_store_call_and_returns_a_tuple(self, tmp_path):
        seen = []
        records = [rec(fp("a"), "revoked"), rec(fp("b"), "retired", fp("c"))]

        def fake(resolved):
            seen.append(resolved)
            return list(records)

        gateway, _server, _stub = make_gateway(tmp_path / "srv.tls", list_revoked=fake)
        result = gateway.list_revoked()
        assert result == tuple(records) and isinstance(result, tuple)
        assert seen == [gateway._resolved]

    def test_the_default_is_the_store_function(self, tmp_path):
        gateway, _server, _stub = make_gateway(tmp_path / "srv.tls")
        assert gateway._list_revoked is store.list_revoked

    def test_a_failure_propagates_for_enforce_to_handle(self, tmp_path):
        def fake(resolved):
            raise store.ConfigBusy("busy")

        gateway, _server, _stub = make_gateway(tmp_path / "srv.tls", list_revoked=fake)
        with pytest.raises(store.ConfigBusy):
            gateway.list_revoked()

    def test_it_waits_a_bounded_time_for_the_store_lock_then_raises_config_busy(
        self, tmp_path, mocker
    ):
        """F4: a skipped read is safe (nothing is marked checked, the next tick asks again) and
        the poll thread must not sit behind a lock held for long."""
        mocker.patch.object(api, "STORE_CALL_WAIT", 0.3)
        gateway, _server, _stub = make_gateway(
            tmp_path / "srv.tls", list_revoked=lambda resolved: [rec(fp("a"), "revoked")]
        )
        box = SimpleNamespace(value=None, exc=None, elapsed=None)

        def call():
            started = time.monotonic()
            try:
                box.value = gateway.list_revoked()
            except BaseException as exc:    # noqa: BLE001
                box.exc = exc
            box.elapsed = time.monotonic() - started

        api._STORE_LOCK.acquire()
        try:
            t = threading.Thread(target=call, daemon=True)
            t.start()
            t.join(10)                       # a read that waits for ever is a failure, not a hang
            finished = not t.is_alive()
        finally:
            api._STORE_LOCK.release()
        t.join(10)
        assert finished, "list_revoked waited for the store lock without a bound"
        assert isinstance(box.exc, store.ConfigBusy)
        assert 0.25 <= box.elapsed < 5.0, box.elapsed
        assert gateway.list_revoked() == (rec(fp("a"), "revoked"),)

    def test_a_lock_freed_within_the_wait_lets_the_read_through(self, tmp_path, mocker):
        mocker.patch.object(api, "STORE_CALL_WAIT", 30.0)
        gateway, _server, _stub = make_gateway(
            tmp_path / "srv.tls", list_revoked=lambda resolved: [rec(fp("a"), "revoked")]
        )
        box = []
        api._STORE_LOCK.acquire()
        t = threading.Thread(target=lambda: box.append(gateway.list_revoked()), daemon=True)
        t.start()
        try:
            time.sleep(0.4)
            assert box == [] and t.is_alive()
        finally:
            api._STORE_LOCK.release()
        t.join(30)
        assert box == [(rec(fp("a"), "revoked"),)]

    def test_it_reads_the_real_store(self, tls_config_db, tmp_path):
        from _api_support import RealTrust

        trust = RealTrust(tls_config_db)
        ident = client_identity(tmp_path / "alice.tls")
        pin_client(tls_config_db, ident, "alice")
        assert trust.gateway.list_revoked() == ()
        store_trust.revoke_pin(tls_config_db, ident.fingerprint, lock=None)
        (record,) = trust.gateway.list_revoked()
        assert (record.fingerprint, record.reason, record.client_id) == (
            ident.fingerprint, "revoked", "alice"
        )


# ---------------------------------------------------------------------------
# The route on a mock websocket
# ---------------------------------------------------------------------------

class Runaway(BaseException):
    """Escapes the route's `except Exception`: a script that is read for ever is a failure,
    not a hang."""


ROUTE_CLIENT = CLIENT_ID                    # `client1`, the certificate Authed pins
ROUTE_SERVER_DATASET = f"pool/backups/{ROUTE_CLIENT}/data"


class RouteRig:
    """The `/stream` handler of one app driven directly with a mock websocket."""

    def __init__(self, mocker, config=None):
        self.mocker = mocker
        self.authed = Authed(config if config is not None else remote_config())
        self.app = self.authed.app
        self.handler = self.app.view_functions["backup_stream"].__wrapped__
        self.registry = self.app.extensions["zfsbackup.sessions"]
        self.gate = self.authed.gate
        mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
        self.exists = mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        #: Hooks run inside the zfs reads, to land a cut "during the pre-checks".
        self.on_snapshots = None
        self.on_token = None
        self.on_type = None
        self.token = None                 # the server dataset's receive_resume_token, if any
        mocker.patch("zfsbackup.api.zfs.list", side_effect=self._list)
        get_value = zfs_answers(ROUTE_CLIENT)

        def get(ds, prop, *args, **kwargs):
            if prop == "type" and self.on_type is not None:
                self.on_type()
            return get_value(ds, prop, *args, **kwargs)

        mocker.patch("zfsbackup.api.zfs.get.value", side_effect=get)
        self.abort = mocker.patch("zfsbackup.api.zfs.receive.abort")
        self.create = mocker.patch("zfsbackup.api.zfs.create")
        self.set = mocker.patch("zfsbackup.api.zfs.set")
        self.writer = MagicMock()
        self.receive = mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=self.writer)
        self.ws = MagicMock()
        self.ws.sock = FakeSock()
        self.ws.thread = None            # `_end_websocket` has no reader thread to join

    def _list(self, *args, **kwargs):
        if kwargs.get("types") == "snapshot":
            if self.on_snapshots is not None:
                self.on_snapshots()
            return []
        if self.on_token is not None:
            self.on_token()
        return [{"receive_resume_token": self.token or "none"}]

    def cut(self, reason="its client certificate was revoked (revoked)"):
        """What the trust-poll thread does to the one running session."""
        (session,) = self.current()
        session.cut_reason = reason

    def untouched(self):
        """Nothing was destroyed, created or spawned."""
        for mock in (self.abort, self.create, self.set, self.receive):
            mock.assert_not_called()
        self.ws.send.assert_not_called()
        assert self.claimed == set() and self.gate.active == 0 and len(self.registry) == 0

    def run(self, ws=None, *, path_client=ROUTE_CLIENT, dataset=DATASET, handler_client=None,
            **query):
        query.setdefault("mode", "resume")
        query = {k: v for k, v in query.items() if v is not None}
        with self.authed.context(f"/backup/{path_client}/pool/data/stream", query_string=query):
            self.handler(ws if ws is not None else self.ws, handler_client or path_client, dataset)

    @property
    def claimed(self):
        return set(streams_in_progress(self.app))

    def sent(self):
        return [json.loads(c.args[0]) for c in self.ws.send.call_args_list]

    def current(self):
        with self.registry._lock:
            return list(self.registry._sessions)


@pytest.fixture
def route(mocker):
    return RouteRig(mocker)


@pytest.mark.unit
class TestRouteRegistration:
    def test_the_session_is_registered_before_any_work_and_describes_the_request(self, route):
        seen = []
        script = iter([HEADER, DONE])

        def receive(timeout=None):
            seen.append([(s.client_id, s.fingerprint, s.dataset, s.server_dataset, s.ws)
                         for s in route.current()])
            return next(script)

        route.ws.receive.side_effect = receive
        route.run()
        assert seen[0] == [(ROUTE_CLIENT, route.authed.who.fingerprint, DATASET,
                            ROUTE_SERVER_DATASET, route.ws)]
        assert len(route.registry) == 0

    def test_the_session_names_the_bound_id_not_the_url(self, route):
        seen = []
        script = iter([HEADER, DONE])

        def receive(timeout=None):
            seen.extend(s.client_id for s in route.current())
            return next(script)

        route.ws.receive.side_effect = receive
        route.run(handler_client="someone-else")
        assert seen and set(seen) == {ROUTE_CLIENT}

    def test_the_registry_the_route_uses_is_the_one_on_the_app(self, route):
        assert isinstance(route.registry, SessionRegistry)

    def test_unregistered_after_end_websocket_and_still_registered_inside_it(self, route, mocker):
        """Order matters: a route stuck in `_end_websocket` (its close frame waits for a write
        lock held by a blocked pong) must still be reachable by the stall check; and the
        descriptor stays valid until `unregister` has returned."""
        order = []
        real_unregister = route.registry.unregister
        mocker.patch(
            "zfsbackup.api._end_websocket",
            side_effect=lambda ws: order.append(("end_websocket", len(route.registry))),
        )
        route.registry.unregister = lambda s: (order.append(("unregister", len(route.registry))),
                                               real_unregister(s))
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        route.run()
        assert order == [("end_websocket", 1), ("unregister", 1)]
        assert len(route.registry) == 0

    def test_end_websocket_raising_still_unregisters(self, route, mocker):
        mocker.patch("zfsbackup.api._end_websocket", side_effect=RuntimeError("boom"))
        route.ws.receive.side_effect = [HEADER, DONE]
        with pytest.raises(RuntimeError, match="boom"):
            route.run()
        assert len(route.registry) == 0
        assert route.gate.active == 0 and route.claimed == set()

    @pytest.mark.parametrize("scenario", [
        "success", "disabled", "no_mode", "unknown_mode", "bad_base", "draining", "bad_name",
        "busy", "closed_before_header", "receive_cannot_start", "write_error", "ownership_error",
        "ack_send_fails",
    ])
    def test_every_exit_path_unregisters(self, mocker, scenario):
        config = remote_config()
        if scenario == "disabled":
            config.remote_backup = None
        route = RouteRig(mocker, config)
        unregistered = []
        real = route.registry.unregister
        route.registry.unregister = lambda s: (unregistered.append(s), real(s))
        query = {}
        dataset = DATASET
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        if scenario == "no_mode":
            query["mode"] = None
        elif scenario == "unknown_mode":
            query["mode"] = "sideways"
        elif scenario == "bad_base":
            query["from_guid"] = "not-a-guid"
        elif scenario == "draining":
            route.gate.start_draining()
        elif scenario == "bad_name":
            dataset = "pool/bad\nname"
        elif scenario == "busy":
            streams_in_progress(route.app).add(ROUTE_SERVER_DATASET)
        elif scenario == "closed_before_header":
            route.ws.receive.side_effect = ConnectionClosed(None, None)
        elif scenario == "receive_cannot_start":
            route.receive.side_effect = RuntimeError("cannot receive")
        elif scenario == "write_error":
            route.writer.write.side_effect = IOError("disk full")
        elif scenario == "ownership_error":
            route.exists.side_effect = RuntimeError("zfs is gone")
            route.ws.send.side_effect = RuntimeError("socket is gone too")
        elif scenario == "ack_send_fails":
            route.ws.send.side_effect = RuntimeError("socket is gone")
        try:
            if scenario == "ownership_error":
                with pytest.raises(RuntimeError):
                    route.run(dataset=dataset, **query)
            else:
                route.run(dataset=dataset, **query)
        finally:
            streams_in_progress(route.app).discard(ROUTE_SERVER_DATASET)
        assert len(unregistered) == 1 and len(route.registry) == 0
        assert route.gate.active == 0


@pytest.mark.unit
class TestRouteCut:
    @staticmethod
    def cut_at(route, call, then):
        """A `ws.receive` script: the header, then `then` for the data; the session is cut by
        the `call`-th receive (1-based) as the trust-poll thread would."""
        counter = {"n": 0}

        def receive(timeout=None):
            counter["n"] += 1
            if counter["n"] > 25:
                raise Runaway("the route kept reading after the script ended")
            if counter["n"] == call:
                (session,) = route.current()
                session.cut_reason = "its client certificate was revoked (revoked)"
            if counter["n"] == 1:
                return HEADER
            if isinstance(then, BaseException):
                raise then
            return then

        route.ws.receive.side_effect = receive
        return counter

    @pytest.mark.parametrize("data", [b"chunk", DONE, None], ids=["data", "done", "timeout"])
    def test_a_cut_session_writes_nothing_more_and_is_reaped_once(self, route, log, data):
        self.cut_at(route, 2, data)
        route.run()
        assert [c.args[0] for c in route.writer.write.call_args_list] == [HEADER]
        route.writer.close.assert_called_once()
        assert route.ws.send.call_count == 0, f"nothing is sent to a cut client: {route.sent()}"
        assert route.claimed == set()
        assert route.gate.active == 0
        assert len(route.registry) == 0
        (info,) = [r for r in log.records if "was cut" in r.getMessage()]
        assert info.levelno == logging.INFO
        assert ROUTE_SERVER_DATASET in info.getMessage() and "revoked" in info.getMessage()
        assert not [r for r in log.records if r.levelno >= logging.ERROR]

    def test_a_cut_while_the_header_is_read_spawns_nothing_at_all(self, route, log):
        """F1: the header frame was queued before the cut; the route must not carry on to the
        spawn (or the abort) with a session that has been cut."""
        self.cut_at(route, 1, b"x")
        route.run()
        route.untouched()
        route.writer.close.assert_not_called()
        route.writer.write.assert_not_called()
        (info,) = [r for r in log.records if "was cut" in r.getMessage()]
        assert "nothing was changed" in info.getMessage() and "after its header" in info.getMessage()

    def test_the_read_error_of_a_cut_connection_is_the_same_clean_exit(self, route, log):
        """The reader saw the shutdown first: `ConnectionClosed` is the symptom."""
        self.cut_at(route, 2, ConnectionClosed(None, None))
        route.run()
        route.writer.close.assert_called_once()
        route.ws.send.assert_not_called()
        assert route.claimed == set() and route.gate.active == 0
        assert [r for r in log.records if "was cut" in r.getMessage()]

    def test_an_oserror_after_a_cut_still_reaps_once_and_releases_everything(self, route):
        self.cut_at(route, 2, OSError(9, "bad file descriptor"))
        route.run()
        route.writer.close.assert_called_once()
        assert route.claimed == set() and route.gate.active == 0
        assert len(route.registry) == 0

    def test_a_writer_close_failure_while_cutting_is_suppressed(self, route):
        route.writer.close.side_effect = RuntimeError("receive exited 1")
        self.cut_at(route, 2, b"chunk")
        route.run()                                    # does not raise
        route.writer.close.assert_called_once()
        assert route.claimed == set()

    def test_without_a_cut_the_same_script_still_completes_with_an_ack(self, route):
        """The control: it is `cut_reason`, nothing else, that stops this stream."""
        self.cut_at(route, 99, b"chunk")
        route.ws.receive.side_effect = [HEADER, b"chunk", DONE]
        route.run()
        assert [c.args[0] for c in route.writer.write.call_args_list] == [HEADER, b"chunk"]
        assert route.sent() == [{"status": "ok"}]

    def test_a_cut_after_done_does_not_take_the_ack_back(self, route):
        """`done` already received: the receive finished and was reaped, the claim released
        and the ack is on its way; the loop is over, so the cut changes nothing."""
        def receive(timeout=None, _calls=[0]):
            _calls[0] += 1
            return [HEADER, DONE][_calls[0] - 1]

        route.ws.receive.side_effect = receive
        route.run()
        assert route.sent() == [{"status": "ok"}]

    # -- F2: what the log says about the receive's outcome ---------------------------------

    @pytest.mark.parametrize("then", [b"chunk", DONE], ids=["data", "queued-done"])
    def test_the_log_says_partial_state_is_kept_when_zfs_receive_failed(self, route, log, then):
        route.writer.close.side_effect = RuntimeError("zfs receive exited 1")
        self.cut_at(route, 2, then)
        route.run()
        (info,) = [r for r in log.records if "was cut" in r.getMessage()]
        assert "partial receive state is kept for a resume" in info.getMessage()
        assert "completed" not in info.getMessage()
        route.ws.send.assert_not_called()

    @pytest.mark.parametrize("then", [b"chunk", DONE], ids=["data", "queued-done"])
    def test_the_log_says_the_snapshot_was_committed_when_zfs_receive_exited_cleanly(
        self, route, log, then
    ):
        """A `{"done": true}` queued before the cut still lets the receive commit: what is
        reported is `writer.close()`'s outcome, not where the cut landed."""
        self.cut_at(route, 2, then)
        route.run()
        (info,) = [r for r in log.records if "was cut" in r.getMessage()]
        assert "after the receive had completed" in info.getMessage()
        assert "the snapshot was committed" in info.getMessage()
        assert "partial" not in info.getMessage()
        route.ws.send.assert_not_called()

    @pytest.mark.parametrize("error", [ConnectionClosed(None, None), OSError(9, "bad fd")],
                             ids=["closed", "oserror"])
    @pytest.mark.parametrize("close_fails", [False, True], ids=["clean", "failed"])
    def test_every_cut_exit_branch_reports_by_the_close_outcome(self, route, log, error,
                                                                close_fails):
        if close_fails:
            route.writer.close.side_effect = RuntimeError("zfs receive exited 1")
        self.cut_at(route, 2, error)
        route.run()
        (info,) = [r for r in log.records if "was cut" in r.getMessage()]
        expected = "partial receive state is kept" if close_fails else "the snapshot was committed"
        assert expected in info.getMessage()

    def test_nothing_is_said_about_a_cut_when_nothing_cut_the_session(self, route, log):
        route.ws.receive.side_effect = [HEADER, ConnectionClosed(None, None)]
        route.run()
        assert not [r for r in log.records if "was cut" in r.getMessage()]

    @pytest.mark.parametrize("error", [ConnectionClosed(None, None), OSError(32, "broken pipe")],
                             ids=["closed", "oserror"])
    def test_a_cut_during_the_final_ack_is_reported_as_committed(self, route, log, error):
        """The receive finished and was reaped before the ack; the cut only took the ack."""
        route.ws.receive.side_effect = [HEADER, b"c", DONE]

        def send(message):
            route.cut()
            raise error

        route.ws.send.side_effect = send
        route.run()
        (info,) = [r for r in log.records if "was cut" in r.getMessage()]
        assert "the snapshot was committed" in info.getMessage()
        route.writer.close.assert_called_once()
        assert route.claimed == set() and route.gate.active == 0

    # -- F1: a cut before the data loop never runs a destructive step -----------------------

    @pytest.mark.parametrize("mode", ["resume", "fresh"])
    def test_a_cut_while_the_header_is_read_neither_aborts_nor_spawns(self, route, mode):
        route.token = "1-partial-token"            # a fresh stream would `receive -A` it
        self.cut_at(route, 1, b"x")
        route.run(mode=mode)
        route.untouched()

    def test_a_revoke_during_the_snapshot_listing_neither_aborts_nor_spawns(self, route, log):
        """The cut lands in the pre-checks, after the header was read."""
        route.token = "1-partial-token"
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        route.on_snapshots = route.cut
        route.run(mode="fresh")
        route.untouched()
        route.ws.receive.assert_called()
        (info,) = [r for r in log.records if "was cut" in r.getMessage()]
        assert "nothing was changed" in info.getMessage()

    def test_a_revoke_during_the_type_read_neither_aborts_nor_spawns(self, route):
        route.token = "1-partial-token"
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        route.on_type = route.cut
        route.run(mode="fresh")
        route.untouched()

    def test_a_revoke_during_the_snapshot_listing_spawns_nothing_in_resume_mode(self, route):
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        route.on_snapshots = route.cut
        route.run(mode="resume")
        route.untouched()

    def test_a_revoke_during_the_resume_token_lookup_does_not_abort(self, route):
        route.token = "1-partial-token"
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        route.on_token = route.cut
        route.run(mode="fresh")
        route.abort.assert_not_called()
        route.receive.assert_not_called()

    def test_a_revoke_during_the_resume_token_lookup_still_spawns_nothing(self, route):
        route.token = "1-partial-token"
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        route.on_token = route.cut
        route.run(mode="fresh")
        route.receive.assert_not_called()
        route.create.assert_not_called()
        route.ws.send.assert_not_called()
        assert route.claimed == set() and route.gate.active == 0

    def test_a_cut_as_zfs_receive_starts_reaps_it_and_writes_nothing(self, route):
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        route.receive.side_effect = lambda *a, **k: (route.cut(), route.writer)[1]
        route.run(mode="resume")
        route.writer.write.assert_not_called()
        route.writer.close.assert_called_once()
        route.ws.send.assert_not_called()
        assert route.claimed == set() and route.gate.active == 0

    def test_a_session_that_is_not_cut_goes_through_every_step(self, route):
        """The control for the checkpoints: the same fresh stream with a token and no cut
        aborts the partial state, spawns and acks."""
        route.token = "1-partial-token"
        route.ws.receive.side_effect = [HEADER, b"c", DONE]
        route.run(mode="fresh")
        route.abort.assert_called_once()
        route.receive.assert_called_once()
        assert route.sent() == [{"status": "ok"}]

    def test_the_registry_cuts_the_route_end_to_end_on_a_mock_websocket(self, route):
        """`enforce` (not a hand-set `cut_reason`) from a second thread's role: it runs inside
        the receive call, as the trust-poll thread would, and the route reacts."""
        trust = FakeTrust(records=[rec(route.authed.who.fingerprint, "client_revoked",
                                       client_id=ROUTE_CLIENT)])
        calls = {"n": 0}
        results = []

        def receive(timeout=None):
            calls["n"] += 1
            if calls["n"] > 25:
                raise Runaway("the route kept reading after the script ended")
            if calls["n"] == 1:
                return HEADER
            results.append(route.registry.enforce(trust))
            return b"more data that must not be written"

        route.ws.receive.side_effect = receive
        route.run()
        assert results == [1]
        assert route.ws.sock.eof == 1
        assert [c.args[0] for c in route.writer.write.call_args_list] == [HEADER]
        route.writer.close.assert_called_once()
        assert route.sent() == []
        assert route.claimed == set() and route.gate.active == 0 and len(route.registry) == 0


# ---------------------------------------------------------------------------
# Real TLS: revocation of a running stream
# ---------------------------------------------------------------------------


@pytest.fixture
def rig(tls_config_db, tmp_path, mocker):
    rig = StreamRig(tls_config_db, tmp_path, mocker)
    try:
        yield rig
    finally:
        rig.close()


def closed_by_server(ws, within=30.0) -> bool:
    """Does the client's connection end (an error or EOF on read) within `within` seconds?"""
    ws.settimeout(within)
    try:
        while True:
            ws.recv()
    except Exception:
        return True
    return False


def wait_cut(rig, writer, *, within=30.0, others=0):
    """Wait until the route has unwound after a cut (`others` sessions of other streams stay);
    -> the seconds it took."""
    started = time.monotonic()
    assert eventually(lambda: writer.closes == 1 and writer.reaped, within), (
        "the receive child was not reaped after the cut"
    )
    assert eventually(lambda: len(rig.sessions) == others, within), "the session stayed registered"
    assert eventually(lambda: rig.gate.active == others, within), "the gate slot was not released"
    assert eventually(lambda: len(rig.claimed()) == others, within), (
        "the dataset claim was not released"
    )
    return time.monotonic() - started


@pytest.mark.integration
class TestRevocationOverRealTls:
    def revoke(self, rig, how, ident):
        resolved = rig.resolved
        if how == "revoke_pin":
            store_trust.revoke_pin(resolved, ident.fingerprint, lock=None)
            return "revoked"
        if how == "revoke_client":
            store_trust.revoke_client(resolved, CLIENT, lock=None)
            return "client_revoked"
        assert how == "replace"
        new = rig.new_client("replacement", pin=False)
        rig.trust.enroll(new, CLIENT, scope=CLIENT, replace=True)
        return "replaced"

    @pytest.mark.parametrize("how", ["revoke_pin", "revoke_client", "replace"])
    def test_revoking_the_pin_cuts_the_running_stream_and_frees_everything(
        self, rig, caplog, how
    ):
        caplog.set_level(logging.INFO, logger=API_LOGGER)
        alice = rig.new_client("alice")
        ws = rig.connect(alice)
        writer = rig.start_stream(ws)
        session = rig.only_session()
        assert rig.gate.active == 1 and rig.claimed() == {SERVER_DATASET}
        time.sleep(0.6)                                  # several ticks with nothing revoked
        assert writer.proc.poll() is None and session.cut_reason is None
        assert rig.list_revoked_calls == 0, "a pin that is in the view costs no store read"

        reason = self.revoke(rig, how, alice)
        took = wait_cut(rig, writer)
        assert took < 20
        assert reason in session.cut_reason
        assert closed_by_server(ws), "the client's connection was not ended"
        cuts = warnings_of(caplog)
        assert len(cuts) == 1
        text = cuts[0].getMessage()
        assert CLIENT in text and SERVER_DATASET in text and reason in text
        (info,) = [r for r in caplog.records if "was cut" in r.getMessage()]
        assert "partial receive state is kept for a resume" in info.getMessage()
        assert writer.proc.returncode == 1               # a truncated stream: zfs receive fails
        assert writer.closes == 1                        # reaped exactly once
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_a_revoked_stream_never_gets_an_ack(self, rig):
        alice = rig.new_client("alice")
        ws = rig.connect(alice)
        writer = rig.start_stream(ws)
        rig.only_session()
        store_trust.revoke_pin(rig.resolved, alice.fingerprint, lock=None)
        wait_cut(rig, writer)
        replies = []
        ws.settimeout(10)
        with contextlib.suppress(Exception):
            ws.send(json.dumps({"done": True}))
        with contextlib.suppress(Exception):
            while True:
                replies.append(ws.recv())
        assert all('"status": "ok"' not in str(r) for r in replies), replies

    def test_another_clients_stream_is_untouched(self, rig):
        alice = rig.new_client("alice", "alice")
        bob = rig.new_client("bob", "bob")
        ws_a, ws_b = rig.connect(alice, client_id="alice"), rig.connect(bob, client_id="bob")
        writer_a = rig.start_stream(ws_a)
        ws_b.send_binary(HEADER)
        assert eventually(lambda: len(rig.writers) == 2 and rig.writers[1].writes, 30)
        writer_b = rig.writers[1]
        assert eventually(lambda: len(rig.sessions) == 2, 30)
        store_trust.revoke_pin(rig.resolved, alice.fingerprint, lock=None)
        wait_cut(rig, writer_a, others=1)
        assert writer_b.proc.poll() is None
        assert finish(ws_b) == {"status": "ok"}
        assert writer_b.closes == 1 and writer_b.reaped

    def test_a_certificate_that_is_still_pinned_is_not_cut_whatever_else_changes(self, rig):
        """Trust generations move for other reasons (another client revoked, a token made); a
        stream whose pin is in the view is not asked about."""
        alice = rig.new_client("alice", "alice")
        bob = rig.new_client("bob", "bob")
        ws = rig.connect(alice, client_id="alice")
        writer = rig.start_stream(ws)
        rig.only_session()
        store_trust.revoke_pin(rig.resolved, bob.fingerprint, lock=None)
        assert eventually(lambda: bob.fingerprint not in rig.gateway.view.pins, 30)
        time.sleep(0.6)
        assert writer.proc.poll() is None and rig.list_revoked_calls == 0
        assert finish(ws) == {"status": "ok"}

    # -- rotation ----------------------------------------------------------------------

    def rotate(self, rig, old, name):
        """Rotate `old` to a new key and let the new key's first request retire `old`, through
        the real writers. -> the new identity."""
        new = rig.new_client(name, pin=False)
        outcome = store_trust.rotate_client(
            rig.resolved, old_fp=old.fingerprint,
            new_pin=NewPin(new.fingerprint, new.cert_pem, new.not_after.timestamp()),
            now=time.time,
        )
        assert outcome is store_trust.RotateOutcome.ROTATED
        assert store_trust.retire_predecessor(rig.resolved, new.fingerprint) is True
        return new

    def await_checked(self, rig, session, calls):
        assert eventually(lambda: old_gone(rig, session), 30), "the retired pin never left the view"
        assert eventually(lambda: session.checked_key == rig.gateway.view.key, 30)
        assert rig.list_revoked_calls == calls

    def test_a_rotation_that_retires_the_old_pin_does_not_cut_and_the_stream_finishes(
        self, rig, caplog
    ):
        old = rig.new_client("old")
        ws = rig.connect(old)
        writer = rig.start_stream(ws)
        session = rig.only_session()
        self.rotate(rig, old, "new")
        self.await_checked(rig, session, 1)
        time.sleep(0.8)                                   # many ticks at the same key
        assert rig.list_revoked_calls == 1, "the store is asked once per trust change"
        assert session.cut_reason is None and writer.proc.poll() is None
        assert finish(ws) == {"status": "ok"}
        assert writer.closes == 1
        assert warnings_of(caplog) == []

    @pytest.mark.parametrize("what", ["old", "new", "client"])
    def test_after_a_rotation_the_stream_on_the_retired_key_is_cut_by(self, rig, caplog, what):
        old = rig.new_client("old")
        ws = rig.connect(old)
        writer = rig.start_stream(ws)
        session = rig.only_session()
        new = self.rotate(rig, old, "new")
        self.await_checked(rig, session, 1)
        assert session.cut_reason is None

        if what == "old":
            result = store_trust.revoke_pin(rig.resolved, old.fingerprint, lock=None)
            assert result.already_retired and [p.fingerprint for p in result.cascaded] == [new.fingerprint]
            reason = "successor_revoked"
        elif what == "new":
            store_trust.revoke_pin(rig.resolved, new.fingerprint, lock=None)
            reason = "revoked"
        else:
            store_trust.revoke_client(rig.resolved, CLIENT, lock=None)
            reason = "client_revoked"
        wait_cut(rig, writer)
        assert reason in session.cut_reason and new.fingerprint in session.cut_reason
        (warning,) = warnings_of(caplog)
        assert new.fingerprint in warning.getMessage() and CLIENT in warning.getMessage()
        assert rig.list_revoked_calls == 2
        assert closed_by_server(ws)

    def test_two_rotations_deep(self, rig, caplog):
        a = rig.new_client("a")
        ws = rig.connect(a)
        writer = rig.start_stream(ws)
        session = rig.only_session()
        b = self.rotate(rig, a, "b")
        c = self.rotate(rig, b, "c")
        assert eventually(lambda: session.checked_key == rig.gateway.view.key
                          and a.fingerprint not in rig.gateway.view.pins, 30)
        assert session.cut_reason is None and writer.proc.poll() is None
        store_trust.revoke_pin(rig.resolved, c.fingerprint, lock=None)
        wait_cut(rig, writer)
        assert c.fingerprint in session.cut_reason
        assert len(warnings_of(caplog)) == 1


def old_gone(rig, session) -> bool:
    return session.fingerprint not in rig.gateway.view.pins


# ---------------------------------------------------------------------------
# Real TLS: the write-stall bound
# ---------------------------------------------------------------------------


@pytest.fixture
def stall_rig(tls_config_db, tmp_path, mocker):
    rig = StreamRig(tls_config_db, tmp_path, mocker, stall_limit=STALL, small_buffers=True)
    try:
        yield rig
    finally:
        rig.close()


STALL = 2.0                 # the write-stall limit the real-TLS rigs use


def blocked_age(sock) -> float:
    started = sock._write_started
    return 0.0 if started is None else time.monotonic() - started


@pytest.mark.integration
class TestWriteStallOverRealTls:
    def test_a_client_that_pings_and_never_reads_is_cut_within_the_limit_and_a_tick(
        self, stall_rig, caplog
    ):
        rig = stall_rig
        caplog.set_level(logging.INFO, logger=API_LOGGER)
        alice = rig.new_client("alice")
        ws = rig.connect(alice, rcvbuf=4096)
        writer = rig.start_stream(ws)
        session = rig.only_session()
        sock = session.ws.sock
        stop = threading.Event()
        flood = ping_flood(ws, stop=stop)
        try:
            assert eventually(lambda: blocked_age(sock) > 0.3, 60), (
                f"the server's pong write never blocked ({flood.sent} pings sent, "
                f"client error {flood.exc!r})"
            )
            assert rig.gate.active == 1 and session.cut_reason is None, (
                "the stream must still hold its slot while the write is below the limit"
            )
            began = time.monotonic()
            assert eventually(lambda: session.cut_reason is not None, 30), "never cut"
            cut_after = time.monotonic() - began
            assert session.cut_reason.startswith("a write to the client has been blocked for")
            assert cut_after < STALL + 10, cut_after
            wait_cut(rig, writer)
            assert rig.gate.wait_idle(10) is True
        finally:
            stop.set()
        (cut,) = warnings_of(caplog)
        assert CLIENT in cut.getMessage() and SERVER_DATASET in cut.getMessage()
        assert "blocked for" in cut.getMessage()
        infos = [r for r in caplog.records if "was cut" in r.getMessage()]
        assert len(infos) <= 1       # the INFO is missed if the route looked before cut_reason
        assert sock._write_started is None, "the failed write cleared its mark"

    def test_a_client_that_pings_and_reads_is_not_cut(self, tls_config_db, tmp_path, mocker):
        """The control: the same flood from a client that reads its pongs. Writes finish, so
        nothing is ever older than the limit."""
        rig = StreamRig(tls_config_db, tmp_path, mocker, stall_limit=STALL, small_buffers=True)
        try:
            alice = rig.new_client("alice")
            ws = rig.connect(alice, rcvbuf=4096)
            writer = rig.start_stream(ws)
            session = rig.only_session()
            # ONE thread does both: a batch of pings, then read the pongs, repeat. A client SSLSocket must
            # not be read and written from two threads at once (OpenSSL 3.0.13 turns that into a spurious
            # SSLEOFError, websocket-client closes, and the server rightly reaps the stream).
            pongs = []
            payload = b"p" * 125
            batch = 20                                       # ~3 KiB of pongs: fits the 4 KiB client buffer
            ws.settimeout(10)
            began = time.monotonic()
            sent = 0
            while sent < 3000:
                for _ in range(batch):
                    ws.ping(payload)
                    sent += 1
                got = 0
                while got < batch:
                    opcode, _data = ws.recv_data_frame(control_frame=True)
                    if opcode == 0xA:
                        got += 1
                        pongs.append(1)
            assert time.monotonic() - began < 120
            time.sleep(1.5)                                  # past the limit with nothing blocked
            assert len(pongs) >= 500, f"only {len(pongs)} pongs came back: the write path was idle"
            assert session.cut_reason is None and writer.proc.poll() is None
            ws.settimeout(10)
            assert finish(ws) == {"status": "ok"}
        finally:
            rig.close()


# ---------------------------------------------------------------------------
# A real ApiWorker process
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestSessionsInARealApiWorker:
    @pytest.mark.parametrize("method", START_METHODS)
    def test_a_revoked_client_is_cut_mid_stream_with_no_restart(
        self, tls_config_db, tmp_path, method
    ):
        """The registry is wired into the worker's own trust-poll thread, whichever way the
        process was started: the running stream ends after the revocation, the worker stays."""
        from zfsbackup.config.store import dispose_all

        alice = client_identity(tmp_path / "alice.tls")
        pin_client(tls_config_db, alice, CLIENT)
        dispose_all()
        with ProcessRig(tls_config_db, method, tmp_path) as proc:
            proc.ready()
            pid = proc.worker.pid
            ws = proc.connect(alice)
            ws.send_binary(HEADER)
            ws.send_binary(b"x" * 4096)
            assert eventually(lambda: "write 4096" in proc.events(), 40), proc.events()
            assert "close" not in proc.events()

            store_trust.revoke_pin(tls_config_db, alice.fingerprint, lock=None)
            dispose_all()
            assert eventually(lambda: "close" in proc.events(), 60), proc.events()
            assert closed_by_server(ws, 30)
            assert proc.worker.is_alive() and proc.worker.pid == pid
            assert proc.events().count("close") == 1

"""S-3: anonymous flood control and log throttling in the API worker.

What is under test (all in `zfsbackup.api`, wired by `workers._api_server_class` and
`ApiWorker`): nothing here is configuration, every limit is a constant.

* a per-peer token bucket and a per-peer cap on concurrent connections, checked on the ACCEPT
  thread before a thread or a TLS byte is spent (`PeerRateLimiter`, `peer_key`); a peer that
  completed a handshake with a pinned certificate is "vouched" for an hour (no bucket, a higher
  cap);
* a per-peer `/enroll` failure lockout (429 + `Retry-After`) and a cap on `/enroll` requests in
  flight (503 busy) (`EnrollGuard`);
* every refusal log line goes through one throttle (`_PerPeerThrottle`): one line per kind and
  peer per hour, 120 an hour in all, the rest DEBUG and counted into one summary line an hour
  (`PeerLimits.flush_log`, called from the trust-poll thread); `log_request` demotes the repeated
  refusal statuses the same way;
* a per-client cap on the negotiate pending-config map.

Layout
------

* `TestPeerKey`               -- which address counts as "one peer".
* `TestPeerRateLimiter`       -- bucket, refill, caps, vouching, bounds (fake clock, no sockets).
* `TestEnrollGuard`           -- the lockout and the in-flight count (fake clock).
* `TestPerPeerThrottle`       -- the log throttle: per-key hour, global cap, kinds, LRU, summary,
  threads.
* `TestPeerLimits`            -- the bundle, `flush_log`, the constants, the wiring into the app
  and the server.
* `TestEnrollLockoutApp`      -- the lockout and the busy cap through the Flask app: 429 before
  the body is read or the store touched, what is and is not counted, Retry-After.
* `TestEnrollInFlight`        -- 503 busy and the slot's release on every outcome.
* `TestRefusalLogsAreThrottled` -- every refusal site routes through the throttle.
* `TestAccessLineThrottle`    -- `log_request`: 401/403/426/429/503 once per peer, status, hour.
* `TestPendingPerClientCap`   -- the negotiate pending map: one client cannot evict another's.
* `TestAcceptTimeLimits`      -- `ApiServer.process_request` on socketpairs with arbitrary peer
  addresses: dropped before a TLS byte, no slot leaks, another peer unaffected.
* `TestRealSockets`           -- loopback TCP/TLS floods against a real server (and 127.0.0.2
  where the platform has it).
* `TestSummaryFromThePollThread` -- the hourly summary is logged by the trust-poll thread.

Hygiene: no `zfs` (patched), every socket is 127.0.0.1 (a second source address only where the
platform lets us bind it), generous time bounds, every thread joined and every server stopped.
"""

import collections
import http.client
import json
import logging
import socket
import threading
import time
from types import SimpleNamespace

import pytest

from _api_support import (
    CLIENT_ID, SERVER_FP, Authed, _find_in_closures, enroll_body, make_client_cert, pending_api, pending_configs,
    patch_api_clock, pending_by_client, pending_deadlines, pending_needed,
)
from _enroll_support import make_server, zfs_stub  # noqa: F401  (fixtures)
from _tls_support import (
    IO_TIMEOUT, LOOPBACK, client_identity, https_get, insecure_client_context, make_gateway,
    pin_client, raw_connect, trusted_pin, try_get, unpin_client,
)
from zfsbackup import api, identity, workers
from zfsbackup.api import (
    ADMIT_CONCURRENT, ADMIT_RATE, ADMIT_RESERVED, EnrollGuard, PeerLimits, PeerRateLimiter, _PerPeerThrottle,
    peer_key,
)
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig
from zfsbackup.config.store.trust import EnrollOutcome
from zfsbackup.logsetup import PEER_LOGGER
from zfsbackup.protocol import PROTOCOL_HEADER
from zfsbackup.stopflag import StopFlag

TARGET = "pool/backups"


class Clock:
    """A settable monotonic clock to inject as `clock=`."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _config(**kwargs) -> BackupConfig:
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data")],
        remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
        **kwargs,
    )


def _eventually(predicate, timeout=20.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


@pytest.fixture
def zfs(mocker):
    """No `zfs` is reached: every entry point the routes use is a mock (so `calls` can say none
    was made)."""
    mocks = SimpleNamespace(
        exists=mocker.patch("zfsbackup.api.zfs.exists", return_value=False),
        list=mocker.patch("zfsbackup.api.zfs.list", return_value=[]),
        create=mocker.patch("zfsbackup.api.zfs.create"),
        set=mocker.patch("zfsbackup.api.zfs.set"),
        value=mocker.patch("zfsbackup.api.zfs.get.value", return_value=None),
        props=mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={}),
    )
    return mocks


def _peer_records(caplog, *, levels=None, text=None):
    """The records on the S-3 peer logger, optionally filtered by level names / message text."""
    found = [r for r in caplog.records if r.name == PEER_LOGGER]
    if levels is not None:
        found = [r for r in found if r.levelname in levels]
    if text is not None:
        found = [r for r in found if text in r.getMessage()]
    return found


# ===========================================================================
# peer_key
# ===========================================================================


@pytest.mark.unit
class TestPeerKey:
    @pytest.mark.parametrize("address, key", [
        (("203.0.113.7", 40000), "203.0.113.7"),
        (["203.0.113.7", 40000], "203.0.113.7"),
        ("203.0.113.7", "203.0.113.7"),
        (("127.0.0.1", 1), "127.0.0.1"),
        (("0.0.0.0", 1), "0.0.0.0"),
    ])
    def test_an_ipv4_peer_is_its_address(self, address, key):
        assert peer_key(address) == key

    def test_the_port_is_not_part_of_the_peer(self):
        assert peer_key(("203.0.113.7", 1)) == peer_key(("203.0.113.7", 65535))

    @pytest.mark.parametrize("address", [
        ("::ffff:203.0.113.7", 40000, 0, 0),
        ("::ffff:cb00:7107", 40000, 0, 0),
        "::ffff:203.0.113.7",
    ])
    def test_an_ipv4_mapped_ipv6_peer_is_the_ipv4_address(self, address):
        assert peer_key(address) == "203.0.113.7"

    def test_a_mapped_and_a_native_ipv4_connection_are_one_peer(self):
        assert peer_key(("::ffff:198.51.100.9", 1, 0, 0)) == peer_key(("198.51.100.9", 1))

    def test_an_ipv6_peer_is_its_slash_64(self):
        assert peer_key(("2001:db8:1:2:3:4:5:6", 40000, 0, 0)) == "2001:db8:1:2::/64"

    def test_a_whole_slash_64_is_one_peer(self):
        a = peer_key(("2001:db8:1:2::1", 1, 0, 0))
        b = peer_key(("2001:db8:1:2:ffff:ffff:ffff:ffff", 1, 0, 0))
        c = peer_key(("2001:db8:1:2:abcd::", 1, 0, 0))
        assert a == b == c == "2001:db8:1:2::/64"

    def test_neighbouring_slash_64s_are_different_peers(self):
        keys = {peer_key((f"2001:db8:1:{n}::1", 1, 0, 0)) for n in range(5)}
        assert len(keys) == 5
        assert peer_key(("2001:db8:1:2::1", 1)) != peer_key(("2001:db8:2:2::1", 1))

    def test_the_slash_64_is_the_prefix_not_the_suffix(self):
        """Two hosts that differ only in the first 64 bits are two peers (and the lower 64 bits
        are what one host varies for free)."""
        assert peer_key(("2001:db8:0:1::5", 1)) != peer_key(("2001:db8:0:2::5", 1))
        assert peer_key(("2001:db8:0:1::5", 1)) == peer_key(("2001:db8:0:1::6", 1))

    def test_the_ipv6_loopback_is_itself_not_a_slash_64(self):
        assert peer_key(("::1", 1, 0, 0)) == "::1"

    def test_a_zone_id_is_ignored(self):
        assert peer_key(("fe80::1%en0", 1, 0, 0)) == peer_key(("fe80::2", 1, 0, 0)) == "fe80::/64"

    def test_the_same_address_spelled_differently_is_one_peer(self):
        assert peer_key("2001:0db8:0001:0002:0000:0000:0000:0001") == peer_key("2001:db8:1:2::1")

    @pytest.mark.parametrize("address, key", [
        (None, "?"),
        ((), "?"),
        ([], "?"),
        ((None, 1), "?"),
        ("", "?"),
        (("", 1), "?"),
    ])
    def test_nothing_usable_is_one_shared_unknown_peer(self, address, key):
        assert peer_key(address) == key

    def test_unparseable_text_is_the_text_itself_clipped(self):
        assert peer_key("not-an-address") == "not-an-address"
        assert peer_key(("x" * 500, 1)) == "x" * 64

    @pytest.mark.parametrize("address", [
        5, 5.5, b"bytes", object(), ("\xff\xfe", 1), (b"\x00", 1), ("1.2.3", 1), ("999.1.1.1", 1),
        ("::g", 1), ("1" * 1000, 1), ({}, 1), (("nested", 1), 2), -1, True,
    ])
    def test_it_never_raises(self, address):
        key = peer_key(address)
        assert isinstance(key, str) and 0 < len(key) <= 70

    def test_a_hostile_str_does_not_raise(self):
        class Boom:
            def __str__(self):
                raise RuntimeError("boom")

        assert peer_key(Boom()) == "?"
        assert peer_key((Boom(), 1)) == "?"

    def test_it_is_a_pure_function_of_the_address(self):
        """Nothing else (no header, no request) is consulted."""
        assert peer_key(("203.0.113.7", 1)) is not None
        import inspect
        assert list(inspect.signature(peer_key).parameters) == ["client_address"]


# ===========================================================================
# PeerRateLimiter
# ===========================================================================


@pytest.mark.unit
class TestPeerRateLimiter:
    @pytest.fixture
    def clock(self):
        return Clock()

    @pytest.fixture
    def limiter(self, clock):
        return PeerRateLimiter(clock=clock)

    @staticmethod
    def _spend(limiter, key, n):
        """n admit-then-release cycles (a connection that comes and goes); -> the list of
        answers."""
        out = []
        for _ in range(n):
            answer = limiter.admit(key)
            out.append(answer)
            if answer is None:
                limiter.release(key)
        return out

    def test_the_defaults_are_the_documented_constants(self):
        assert (api.API_RATE_BURST, api.API_RATE_REFILL, api.API_RATE_PEERS) == (60, 10.0, 4096)
        assert api.API_MAX_CONNECTIONS_PER_IP == 8
        assert api.API_MAX_CONNECTIONS_PER_VOUCHED_IP == 32
        assert api.API_VOUCH_TTL == 8 * 86400.0 and api.API_VOUCHED_PEERS == 1024
        limiter = PeerRateLimiter()
        assert limiter.burst == 60 and limiter.rate == 10.0
        assert limiter._max_peers == 4096 and limiter._max_per_peer == 8
        assert limiter._max_per_vouched == 32 and limiter._vouch_ttl == 8 * 86400.0
        assert limiter._max_vouched == 1024

    def test_a_burst_of_60_then_refused(self, limiter):
        answers = self._spend(limiter, "203.0.113.7", 70)
        assert answers[:60] == [None] * 60
        assert answers[60:] == [ADMIT_RATE] * 10

    def test_the_refusal_names_the_rate(self, limiter):
        self._spend(limiter, "p", 60)
        assert limiter.admit("p") == ADMIT_RATE == "rate"

    def test_refill_is_ten_per_second(self, limiter, clock):
        self._spend(limiter, "p", 60)
        assert limiter.admit("p") == ADMIT_RATE
        clock.advance(0.1)                                   # one token
        assert limiter.admit("p") is None
        limiter.release("p")
        assert limiter.admit("p") == ADMIT_RATE
        clock.advance(1.0)                                   # ten tokens
        assert self._spend(limiter, "p", 12) == [None] * 10 + [ADMIT_RATE] * 2

    def test_part_of_a_token_accumulates_across_refusals(self, limiter, clock):
        self._spend(limiter, "p", 60)
        clock.advance(0.0625)
        assert limiter.admit("p") == ADMIT_RATE              # 0.625 of a token
        clock.advance(0.0625)
        assert limiter.admit("p") is None                    # and 0.625 more: 1.25

    def test_the_bucket_never_holds_more_than_the_burst(self, limiter, clock):
        self._spend(limiter, "p", 5)
        clock.advance(100_000)
        assert self._spend(limiter, "p", 70) == [None] * 60 + [ADMIT_RATE] * 10

    def test_a_peer_nobody_has_seen_has_a_full_bucket(self, limiter):
        self._spend(limiter, "a", 60)
        assert limiter.admit("a") == ADMIT_RATE
        assert limiter.admit("b") is None

    def test_peers_have_independent_buckets(self, limiter):
        self._spend(limiter, "203.0.113.7", 61)
        assert self._spend(limiter, "203.0.113.8", 60) == [None] * 60

    def test_an_ipv6_slash_64_shares_one_bucket(self, limiter):
        keys = [peer_key((f"2001:db8:1:2::{n:x}", 1, 0, 0)) for n in range(1, 100)]
        answers = [self._spend(limiter, k, 1)[0] for k in keys]
        assert answers.count(None) == 60 and answers.count(ADMIT_RATE) == 39

    def test_a_refused_admit_takes_no_slot(self, limiter):
        self._spend(limiter, "p", 60)
        assert limiter.admit("p") == ADMIT_RATE
        assert limiter._active == {}

    def test_an_admit_takes_a_slot_and_a_release_gives_it_back(self, limiter):
        assert limiter.admit("p") is None
        assert limiter._active == {"p": 1}
        limiter.release("p")
        assert limiter._active == {}

    def test_release_balance_with_many_slots(self, limiter):
        for _ in range(5):
            assert limiter.admit("p") is None
        assert limiter._active == {"p": 5}
        for left in (4, 3, 2, 1):
            limiter.release("p")
            assert limiter._active == {"p": left}
        limiter.release("p")
        assert limiter._active == {}

    def test_releasing_a_key_that_holds_nothing_is_harmless(self, limiter):
        limiter.release("never-seen")
        limiter.release("never-seen")
        assert limiter._active == {}
        # and it did not give the peer extra room: still capped at 8
        answers = [limiter.admit("never-seen") for _ in range(9)]
        assert answers == [None] * 8 + [ADMIT_CONCURRENT]

    def test_a_double_release_never_takes_another_connections_slot(self, limiter):
        assert limiter.admit("p") is None
        limiter.release("p")
        limiter.release("p")
        limiter.release("p")
        answers = [limiter.admit("p") for _ in range(9)]
        assert answers == [None] * 8 + [ADMIT_CONCURRENT]

    def test_eight_at_once_then_concurrent(self, limiter):
        answers = [limiter.admit("p") for _ in range(10)]
        assert answers == [None] * 8 + [ADMIT_CONCURRENT] * 2
        assert ADMIT_CONCURRENT == "concurrent"

    def test_a_release_makes_room_for_one(self, limiter):
        for _ in range(8):
            limiter.admit("p")
        assert limiter.admit("p") == ADMIT_CONCURRENT
        limiter.release("p")
        assert limiter.admit("p") is None
        assert limiter.admit("p") == ADMIT_CONCURRENT

    def test_the_cap_is_per_peer(self, limiter):
        for _ in range(8):
            limiter.admit("a")
        assert limiter.admit("a") == ADMIT_CONCURRENT
        assert limiter.admit("b") is None

    def test_the_concurrency_check_comes_first_and_costs_no_token(self, limiter):
        for _ in range(8):
            assert limiter.admit("p") is None                # 8 tokens spent, 52 left
        assert [limiter.admit("p") for _ in range(200)] == [ADMIT_CONCURRENT] * 200
        for _ in range(8):
            limiter.release("p")
        assert self._spend(limiter, "p", 55) == [None] * 52 + [ADMIT_RATE] * 3

    def test_a_peer_with_slots_free_but_no_tokens_is_refused_for_rate(self, limiter):
        self._spend(limiter, "p", 60)
        assert limiter._active == {}
        assert limiter.admit("p") == ADMIT_RATE

    # -- vouching -----------------------------------------------------------------------------

    @staticmethod
    def _vouch_and_end(limiter, key):
        """A pinned connection that came and went: the TTL starts now."""
        limiter.vouch(key)
        limiter.unvouch(key)

    def test_a_vouched_peer_gets_32_concurrent(self, limiter):
        limiter.vouch("p")
        answers = [limiter.admit("p") for _ in range(34)]
        assert answers == [None] * 32 + [ADMIT_CONCURRENT] * 2

    def test_a_vouched_peer_skips_the_bucket(self, limiter):
        limiter.vouch("p")
        assert self._spend(limiter, "p", 1000) == [None] * 1000

    def test_vouching_does_not_help_another_peer(self, limiter):
        limiter.vouch("a")
        assert [limiter.admit("b") for _ in range(9)] == [None] * 8 + [ADMIT_CONCURRENT]
        for _ in range(8):
            limiter.release("b")
        assert self._spend(limiter, "b", 70)[52:] == [ADMIT_RATE] * 18

    def test_vouching_an_exhausted_peer_lets_it_in_at_once(self, limiter):
        self._spend(limiter, "p", 61)
        assert limiter.admit("p") == ADMIT_RATE
        limiter.vouch("p")
        assert limiter.admit("p") is None

    def test_a_peer_already_over_the_unvouched_cap_can_grow_to_the_vouched_one(self, limiter):
        for _ in range(8):
            limiter.admit("p")
        assert limiter.admit("p") == ADMIT_CONCURRENT
        limiter.vouch("p")
        for _ in range(24):
            assert limiter.admit("p") is None
        assert limiter.admit("p") == ADMIT_CONCURRENT

    def test_is_vouched(self, limiter):
        assert limiter.is_vouched("p") is False
        limiter.vouch("p")
        assert limiter.is_vouched("p") is True

    def test_the_vouch_lasts_eight_days_from_the_end_of_the_last_pinned_connection(self, limiter, clock):
        self._vouch_and_end(limiter, "p")
        clock.advance(api.API_VOUCH_TTL - 0.001)
        assert limiter.is_vouched("p") is True
        assert limiter.admit("p") is None
        limiter.release("p")
        clock.advance(0.001)                                  # exactly API_VOUCH_TTL
        assert limiter.is_vouched("p") is False

    def test_a_peer_vouched_a_day_ago_is_still_admitted_into_the_reserve(self, limiter, clock):
        """A weekly or daily client arriving during a flood must not be dropped before it can
        present its certificate."""
        limiter.configure_capacity(64)
        limiter.vouch("weekly")
        limiter.unvouch("weekly")
        self._fill_with_anonymous(limiter, 48)
        clock.advance(23 * 3600)
        assert limiter.admit("weekly") is None
        clock.advance(7 * 86400)                                  # 7 d 23 h: still inside
        assert limiter.admit("weekly") is None
        clock.advance(3600)                                       # 8 d 0 h: out
        assert limiter.admit("weekly") == ADMIT_RESERVED

    def test_a_live_pinned_connection_is_vouched_however_long_it_runs(self, limiter, clock):
        """A NAT with long streams open must always be able to open another connection."""
        limiter.vouch("p")                                    # a stream that runs for days
        clock.advance(10 * api.API_VOUCH_TTL)
        assert limiter.is_vouched("p") is True
        assert self._spend(limiter, "p", 500) == [None] * 500   # no bucket
        assert [limiter.admit("p") for _ in range(33)].count(ADMIT_CONCURRENT) == 1

    def test_the_ttl_starts_when_the_last_pinned_connection_ends(self, limiter, clock):
        limiter.vouch("p")
        clock.advance(5 * api.API_VOUCH_TTL)
        limiter.unvouch("p")
        clock.advance(api.API_VOUCH_TTL - 1)
        assert limiter.is_vouched("p") is True
        clock.advance(1)
        assert limiter.is_vouched("p") is False

    def test_one_of_two_pinned_connections_ending_keeps_the_peer_vouched(self, limiter, clock):
        limiter.vouch("p")
        limiter.vouch("p")
        limiter.unvouch("p")
        clock.advance(10 * api.API_VOUCH_TTL)
        assert limiter.is_vouched("p") is True
        limiter.unvouch("p")
        clock.advance(api.API_VOUCH_TTL)
        assert limiter.is_vouched("p") is False

    def test_eight_long_streams_do_not_lock_a_nat_out(self, limiter, clock):
        """The eight streams hold the unvouched cap; being pinned, the ninth still gets in."""
        for _ in range(8):
            assert limiter.admit("nat") is None
            limiter.vouch("nat")
        clock.advance(3 * api.API_VOUCH_TTL)
        assert limiter.admit("nat") is None

    def test_unvouching_a_peer_that_was_never_vouched_is_harmless(self, limiter):
        limiter.unvouch("nobody")
        assert limiter.is_vouched("nobody") is False
        assert limiter._pinned_open == {}

    def test_an_extra_unvouch_never_takes_another_connections_vouch(self, limiter, clock):
        limiter.vouch("p")
        limiter.unvouch("p")
        limiter.unvouch("p")
        limiter.vouch("p")
        clock.advance(10 * api.API_VOUCH_TTL)
        assert limiter.is_vouched("p") is True

    def test_an_expired_vouch_puts_the_peer_back_under_the_bucket_and_the_cap(self, limiter, clock):
        self._vouch_and_end(limiter, "p")
        assert self._spend(limiter, "p", 200) == [None] * 200      # no bucket while vouched
        clock.advance(api.API_VOUCH_TTL)
        answers = [limiter.admit("p") for _ in range(10)]
        assert answers == [None] * 8 + [ADMIT_CONCURRENT] * 2      # the unvouched cap again
        for _ in range(8):
            limiter.release("p")
        assert "p" not in limiter._vouched                          # and the entry was dropped

    def test_a_new_pinned_connection_renews_the_vouch(self, limiter, clock):
        self._vouch_and_end(limiter, "p")
        clock.advance(api.API_VOUCH_TTL - 10)
        self._vouch_and_end(limiter, "p")
        clock.advance(api.API_VOUCH_TTL - 10)
        assert limiter.is_vouched("p") is True
        clock.advance(10)
        assert limiter.is_vouched("p") is False

    def test_the_vouched_table_is_bounded_and_forgets_the_oldest(self, clock):
        limiter = PeerRateLimiter(max_vouched=3, clock=clock)
        for key in "abcd":
            self._vouch_and_end(limiter, key)
            clock.advance(1)
        assert len(limiter._vouched) == 3
        assert [limiter.is_vouched(k) for k in "abcd"] == [False, True, True, True]

    def test_vouching_again_makes_a_peer_the_newest(self, clock):
        limiter = PeerRateLimiter(max_vouched=3, clock=clock)
        for key in "abc":
            self._vouch_and_end(limiter, key)
        self._vouch_and_end(limiter, "a")                           # now the newest
        self._vouch_and_end(limiter, "d")                           # evicts "b"
        assert [limiter.is_vouched(k) for k in "abcd"] == [True, False, True, True]

    def test_a_flood_of_vouches_never_grows_the_table(self, clock):
        limiter = PeerRateLimiter(max_vouched=16, clock=clock)
        for n in range(1000):
            self._vouch_and_end(limiter, f"10.0.{n // 256}.{n % 256}")
            assert len(limiter._vouched) <= 16

    # -- the reserve for vouched peers -----------------------------------------------------------

    def test_the_reserve_constant_and_the_rule(self):
        assert api.API_VOUCHED_RESERVE == 16 and ADMIT_RESERVED == "reserved"
        for cap, reserve in ((64, 16), (8, 2), (4, 1), (3, 0), (1, 0), (200, 16)):
            limiter = PeerRateLimiter()
            assert limiter.reserve == 0                          # until configured
            limiter.configure_capacity(cap)
            assert limiter.reserve == reserve, cap

    def _fill_with_anonymous(self, limiter, n):
        """n connections from n/8 distinct unvouched peers (8 each)."""
        for i in range(n):
            assert limiter.admit(f"10.0.0.{i // 8}") is None

    def test_anonymous_peers_cannot_take_the_reserved_slots(self, limiter):
        limiter.configure_capacity(64)                           # 48 for anonymous peers
        self._fill_with_anonymous(limiter, 48)
        assert limiter.admit("10.0.9.9") == ADMIT_RESERVED
        assert limiter.admit("10.0.9.10") == ADMIT_RESERVED

    def test_a_vouched_peer_uses_the_reserve(self, limiter):
        limiter.configure_capacity(64)
        self._fill_with_anonymous(limiter, 48)
        limiter.vouch("pinned")
        answers = [limiter.admit("pinned") for _ in range(16)]
        assert answers == [None] * 16                            # the 16 reserved slots
        assert limiter._active_total == 64

    def test_a_peer_with_a_live_pinned_connection_uses_the_reserve_after_the_ttl(self, limiter, clock):
        limiter.configure_capacity(64)
        self._fill_with_anonymous(limiter, 48)
        limiter.vouch("pinned")
        clock.advance(10 * api.API_VOUCH_TTL)
        assert limiter.admit("pinned") is None

    def test_a_refused_reserve_costs_no_token_and_no_slot(self, limiter):
        limiter.configure_capacity(64)
        self._fill_with_anonymous(limiter, 48)
        before = limiter._active_total
        for _ in range(500):
            assert limiter.admit("fresh") == ADMIT_RESERVED
        assert limiter._active_total == before and "fresh" not in limiter._active
        limiter.release("10.0.0.0")
        assert self._spend(limiter, "fresh", 60) == [None] * 60   # the full bucket is still there

    def test_the_per_peer_cap_is_checked_before_the_reserve(self, limiter):
        limiter.configure_capacity(64)
        self._fill_with_anonymous(limiter, 48)
        assert limiter.admit("10.0.0.0") == ADMIT_CONCURRENT     # 8 already, and the pool is full

    def test_releases_reopen_the_anonymous_pool(self, limiter):
        limiter.configure_capacity(64)
        self._fill_with_anonymous(limiter, 48)
        limiter.release("10.0.0.0")
        assert limiter.admit("10.0.9.9") is None
        assert limiter.admit("10.0.9.9") == ADMIT_RESERVED

    def test_without_a_capacity_there_is_no_reserve(self, limiter):
        for i in range(100):
            assert limiter.admit(f"10.0.{i // 8}.1") is None

    def test_reserve_zero_means_no_reserve_at_all(self, clock):
        limiter = PeerRateLimiter(reserve=0, clock=clock)
        limiter.configure_capacity(64)
        assert limiter.reserve == 0
        for i in range(64):
            assert limiter.admit(f"10.0.{i // 8}.1") is None

    def test_the_total_tracks_admits_and_releases(self, limiter):
        for key in ("a", "a", "b"):
            limiter.admit(key)
        assert limiter._active_total == 3
        limiter.release("a")
        limiter.release("never")
        limiter.release("a")
        limiter.release("a")
        assert limiter._active_total == 1 and limiter._active == {"b": 1}

    # -- bounds -------------------------------------------------------------------------------

    def test_the_bucket_table_is_bounded_and_forgets_the_least_recently_used(self, clock):
        limiter = PeerRateLimiter(burst=2, max_peers=2, clock=clock)
        assert self._spend(limiter, "a", 3) == [None, None, ADMIT_RATE]   # "a" is exhausted
        self._spend(limiter, "b", 1)
        self._spend(limiter, "c", 1)                                      # evicts "a", the oldest
        assert len(limiter._buckets) == 2
        assert self._spend(limiter, "a", 3) == [None, None, ADMIT_RATE]   # a forgotten bucket is full

    def test_touching_a_peer_keeps_it_out_of_the_eviction(self, clock):
        limiter = PeerRateLimiter(burst=2, max_peers=2, clock=clock)
        self._spend(limiter, "a", 3)                                      # exhausted
        self._spend(limiter, "b", 1)
        assert limiter.admit("a") == ADMIT_RATE                           # a refused touch counts
        self._spend(limiter, "c", 1)                                      # evicts "b"
        assert limiter.admit("a") == ADMIT_RATE                           # "a" is still remembered
        assert limiter._buckets.keys() == {"a", "c"}

    def test_a_flood_of_addresses_never_grows_the_table(self, clock):
        limiter = PeerRateLimiter(max_peers=64, clock=clock)
        for n in range(2000):
            limiter.admit(f"10.0.{n // 256}.{n % 256}")
            assert len(limiter._buckets) <= 64
        assert len(limiter._buckets) == 64

    def test_the_concurrency_table_has_only_peers_with_open_connections(self, limiter):
        for n in range(100):
            key = f"10.0.0.{n}"
            assert limiter.admit(key) is None
            limiter.release(key)
        assert limiter._active == {}

    # -- time source ---------------------------------------------------------------------------

    def test_without_a_clock_it_uses_time_monotonic_looked_up_at_each_call(self, mocker):
        limiter = PeerRateLimiter()
        fake = Clock(5000.0)
        mocker.patch.object(api.time, "monotonic", fake)
        self._spend(limiter, "p", 60)
        assert limiter.admit("p") == ADMIT_RATE
        fake.advance(0.1)
        assert limiter.admit("p") is None

    # -- threads --------------------------------------------------------------------------------

    def test_threads_hammering_one_peer_never_exceed_the_cap_and_leave_no_slots(self):
        limiter = PeerRateLimiter(burst=10 ** 9, rate=1.0)
        held = [0]
        peak = [0]
        guard = threading.Lock()
        errors = []

        def hammer():
            try:
                for _ in range(1500):
                    if limiter.admit("203.0.113.7") is None:
                        with guard:
                            held[0] += 1
                            peak[0] = max(peak[0], held[0])
                            assert held[0] <= 8
                        with guard:
                            held[0] -= 1
                        limiter.release("203.0.113.7")
            except BaseException as exc:                    # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=hammer) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert not any(t.is_alive() for t in threads) and errors == []
        assert limiter._active == {}
        assert 1 <= peak[0] <= 8

    def test_threads_spending_one_bucket_admit_exactly_the_burst(self):
        clock = Clock()                                     # frozen: no refill
        limiter = PeerRateLimiter(max_per_peer=10 ** 9, clock=clock)
        results = []
        barrier = threading.Barrier(16)

        def go():
            barrier.wait(10)
            for _ in range(20):
                results.append(limiter.admit("p"))

        threads = [threading.Thread(target=go) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        assert results.count(None) == 60 and results.count(ADMIT_RATE) == 16 * 20 - 60

    def test_it_has_a_lock_and_holds_it_for_each_operation(self, limiter):
        assert hasattr(limiter._guard, "acquire")
        done = threading.Event()
        with limiter._guard:
            t = threading.Thread(target=lambda: (limiter.admit("p"), done.set()), daemon=True)
            t.start()
            assert not done.wait(0.3)
        assert done.wait(10)
        t.join(10)


# ===========================================================================
# EnrollGuard
# ===========================================================================


@pytest.mark.unit
class TestEnrollGuard:
    @pytest.fixture
    def clock(self):
        return Clock()

    @pytest.fixture
    def guard(self, clock):
        return EnrollGuard(clock=clock)

    def test_the_defaults_are_the_documented_constants(self):
        assert api.ENROLL_FAILURES_MAX == 5 and api.ENROLL_FAILURE_WINDOW == 900.0
        assert api.ENROLL_MAX_IN_FLIGHT == 2 and api.ENROLL_FAILURE_PEERS == 4096
        assert api.ENROLL_MAX_IN_FLIGHT_PER_PEER == 1
        g = EnrollGuard()
        assert (g.max_failures, g.window, g.max_in_flight, g._max_peers) == (5, 900.0, 2, 4096)
        assert g.max_per_peer == 1

    def test_a_peer_nobody_has_seen_is_not_locked(self, guard):
        assert guard.retry_after("p") == 0

    def test_the_fifth_failure_locks_the_peer_out(self, guard):
        assert [guard.record_failure("p") for _ in range(4)] == [False] * 4
        assert guard.retry_after("p") == 0
        assert guard.record_failure("p") is True
        assert guard.retry_after("p") > 0

    def test_retry_after_is_900_right_after_the_lockout_and_counts_down(self, guard, clock):
        for _ in range(5):
            guard.record_failure("p")
        assert guard.retry_after("p") == 900
        clock.advance(100)
        assert guard.retry_after("p") == 800
        clock.advance(799.5)                                  # 0.5 s left: rounded UP, never 0
        assert guard.retry_after("p") == 1
        clock.advance(0.5)
        assert guard.retry_after("p") == 0

    def test_the_lockout_lasts_exactly_the_window(self, guard, clock):
        for _ in range(5):
            guard.record_failure("p")
        clock.advance(899.999)
        assert guard.retry_after("p") >= 1
        clock.advance(0.001)
        assert guard.retry_after("p") == 0

    def test_after_the_lockout_the_peer_starts_again_from_zero(self, guard, clock):
        for _ in range(5):
            guard.record_failure("p")
        clock.advance(900)
        assert [guard.record_failure("p") for _ in range(4)] == [False] * 4
        assert guard.retry_after("p") == 0
        assert guard.record_failure("p") is True

    def test_failures_older_than_the_window_do_not_count(self, guard, clock):
        for _ in range(4):
            assert guard.record_failure("p") is False
        clock.advance(900)                                     # all four are now out of the window
        assert guard.record_failure("p") is False
        assert guard.retry_after("p") == 0

    def test_a_failure_just_inside_the_window_still_counts(self, guard, clock):
        for _ in range(4):
            guard.record_failure("p")
        clock.advance(899.9)
        assert guard.record_failure("p") is True

    def test_slow_failures_never_lock(self, guard, clock):
        """One failure every 300 s: at most three are ever inside the 900 s window."""
        for _ in range(40):
            assert guard.record_failure("p") is False
            clock.advance(300)
        assert guard.retry_after("p") == 0

    def test_the_window_slides(self, guard, clock):
        for offset in (0, 200, 400, 600):
            clock.now = 1000.0 + offset
            assert guard.record_failure("p") is False
        clock.now = 1000.0 + 899
        assert guard.record_failure("p") is True               # 0, 200, 400, 600 and 899

    def test_peers_are_independent(self, guard):
        for _ in range(5):
            guard.record_failure("a")
        assert guard.retry_after("a") == 900 and guard.retry_after("b") == 0
        assert [guard.record_failure("b") for _ in range(4)] == [False] * 4

    def test_success_forgets_the_failures(self, guard):
        for _ in range(4):
            guard.record_failure("p")
        guard.forget("p")
        assert [guard.record_failure("p") for _ in range(4)] == [False] * 4
        assert guard.record_failure("p") is True

    def test_forgetting_a_locked_peer_lifts_the_lockout(self, guard):
        for _ in range(5):
            guard.record_failure("p")
        guard.forget("p")
        assert guard.retry_after("p") == 0

    def test_forgetting_a_stranger_is_harmless(self, guard):
        guard.forget("nobody")
        assert guard.retry_after("nobody") == 0

    def test_the_peer_table_is_bounded_and_forgets_the_least_recently_failed(self, clock):
        guard = EnrollGuard(max_peers=2, clock=clock)
        for _ in range(4):
            guard.record_failure("a")                           # one short of a lockout
        guard.record_failure("b")
        guard.record_failure("c")                               # evicts "a"
        assert len(guard._peers) == 2
        assert [guard.record_failure("a") for _ in range(4)] == [False] * 4   # starts over
        assert guard.record_failure("a") is True

    def test_a_failure_makes_a_peer_the_newest(self, clock):
        guard = EnrollGuard(max_peers=2, clock=clock)
        for _ in range(4):
            guard.record_failure("a")
        guard.record_failure("b")
        guard.record_failure("a")                               # the 5th: locked, and the newest
        guard.record_failure("c")                               # evicts "b", not "a"
        assert guard.retry_after("a") == 900

    def test_a_flood_of_addresses_never_grows_the_table(self, clock):
        guard = EnrollGuard(max_peers=32, clock=clock)
        for n in range(1000):
            guard.record_failure(f"10.0.{n // 256}.{n % 256}")
            assert len(guard._peers) <= 32

    def test_an_evicted_lockout_is_lifted(self, clock):
        """The price of the bound, documented: forgetting a peer forgets its lockout."""
        guard = EnrollGuard(max_peers=1, clock=clock)
        for _ in range(5):
            guard.record_failure("a")
        assert guard.retry_after("a") == 900
        guard.record_failure("b")
        assert guard.retry_after("a") == 0

    # -- in flight -----------------------------------------------------------------------------

    def test_two_in_flight_and_the_third_is_refused(self, guard):
        assert guard.try_enter() is True and guard.try_enter() is True
        assert guard.in_flight == 2
        assert guard.try_enter() is False
        assert guard.in_flight == 2

    def test_leaving_makes_room(self, guard):
        guard.try_enter()
        guard.try_enter()
        guard.leave()
        assert guard.in_flight == 1
        assert guard.try_enter() is True
        assert guard.try_enter() is False

    def test_leave_never_goes_below_zero(self, guard):
        guard.leave()
        guard.leave()
        assert guard.in_flight == 0
        assert [guard.try_enter() for _ in range(3)] == [True, True, False]

    def test_without_a_key_only_the_global_cap_counts(self, guard):
        assert guard.try_enter() and guard.try_enter()
        assert guard.try_enter() is False

    def test_one_in_flight_per_peer(self, guard):
        assert guard.try_enter("a") is True
        assert guard.try_enter("a") is False                   # the same peer's second
        assert guard.in_flight == 1                            # and it took nothing
        assert guard.try_enter("b") is True

    def test_the_global_cap_still_applies_across_peers(self, guard):
        assert guard.try_enter("a") and guard.try_enter("b")
        assert guard.try_enter("c") is False
        assert guard.in_flight == 2 and guard._in_flight_by_peer == {"a": 1, "b": 1}

    def test_a_peer_refused_for_the_global_cap_keeps_no_count(self, guard):
        guard.try_enter("a")
        guard.try_enter("b")
        assert guard.try_enter("c") is False
        guard.leave("a")
        assert guard.try_enter("c") is True                    # nothing was left behind by the refusal

    def test_leaving_by_key_frees_that_peers_slot_and_the_global_one(self, guard):
        guard.try_enter("a")
        guard.leave("a")
        assert guard.in_flight == 0 and guard._in_flight_by_peer == {}
        assert guard.try_enter("a") is True

    def test_leaving_the_wrong_peer_does_not_free_this_ones_slot(self, guard):
        guard.try_enter("a")
        guard.leave("b")
        assert guard.try_enter("a") is False

    def test_the_per_peer_limit_is_the_ipv6_slash_64_when_callers_pass_peer_keys(self, guard):
        k1 = peer_key(("2001:db8:1:2::1", 1, 0, 0))
        k2 = peer_key(("2001:db8:1:2::2", 1, 0, 0))
        assert guard.try_enter(k1) and guard.try_enter(k2) is False

    def test_a_larger_per_peer_limit_is_honoured(self, clock):
        g = EnrollGuard(max_in_flight=5, max_per_peer=3, clock=clock)
        assert [g.try_enter("a") for _ in range(4)] == [True, True, True, False]

    def test_threads_never_get_more_than_two_in_flight(self):
        guard = EnrollGuard()
        inside = [0]
        peak = [0]
        lock = threading.Lock()
        errors = []

        def go():
            try:
                for _ in range(1000):
                    if guard.try_enter():
                        with lock:
                            inside[0] += 1
                            peak[0] = max(peak[0], inside[0])
                            assert inside[0] <= 2
                            inside[0] -= 1
                        guard.leave()
            except BaseException as exc:                    # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=go) for _ in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert errors == [] and not any(t.is_alive() for t in threads)
        assert guard.in_flight == 0 and peak[0] <= 2

    def test_without_a_clock_it_uses_time_monotonic(self, mocker):
        guard = EnrollGuard()
        fake = Clock(7000.0)
        mocker.patch.object(api.time, "monotonic", fake)
        for _ in range(5):
            guard.record_failure("p")
        assert guard.retry_after("p") == 900
        fake.advance(900)
        assert guard.retry_after("p") == 0


# ===========================================================================
# _PerPeerThrottle
# ===========================================================================


@pytest.mark.unit
class TestPerPeerThrottle:
    @pytest.fixture
    def clock(self):
        return Clock()

    def make(self, clock, **kwargs):
        kwargs.setdefault("global_max", 10 ** 9)
        return _PerPeerThrottle(clock=clock, **kwargs)

    def test_the_defaults_are_the_documented_constants(self):
        assert (api.PEER_LOG_EVERY, api.PEER_LOG_KEYS, api.PEER_LOG_GLOBAL_MAX) == (3600.0, 4096, 120)
        assert api.PEER_LOG_ACCESS_MAX == 120
        t = _PerPeerThrottle()
        assert (t._every, t._max, t._global_max, t._access_max) == (3600.0, 4096, 120, 120)

    def test_which_kinds_are_flood_kinds(self):
        flood = ("rate", "conn-cap", "conn-reserve", "enroll-busy", "store-busy",
                 "access-401", "access-403", "access-426", "access-429", "access-503")
        other = ("auth", "enroll", "enroll-locked", "rotate", "protocol", "tls-validity",
                 "pending")
        assert all(api.is_flood_log_kind(k) for k in flood)
        assert not any(api.is_flood_log_kind(k) for k in other)
        assert api.FLOOD_LOG_KINDS == frozenset(
            {"rate", "conn-cap", "conn-reserve", "enroll-busy", "store-busy"})

    @pytest.mark.parametrize("kind, peer, klass", [
        ("rate", "203.0.113.7", "flood"), ("access-401", "203.0.113.7", "flood"),
        ("conn-cap", "203.0.113.7", "flood"), ("enroll-busy", "203.0.113.7", "flood"),
        ("enroll", "203.0.113.7", "anonymous"), ("enroll-locked", "203.0.113.7", "anonymous"),
        ("protocol", "203.0.113.7", "anonymous"), ("protocol", "2001:db8:1:2::/64", "anonymous"),
        ("protocol", "host-1@203.0.113.7", "holder"),
        ("auth", "203.0.113.7", "holder"), ("auth", "host-1@203.0.113.7", "holder"),
        ("rotate", "host-1@203.0.113.7", "holder"), ("pending", "host-1", "holder"),
        ("tls-validity", "203.0.113.7", "holder"),
    ])
    def test_the_budget_class_of_a_line(self, kind, peer, klass):
        assert api.log_class(kind, peer) == klass
        assert (api.LOG_CLASS_FLOOD, api.LOG_CLASS_ANONYMOUS, api.LOG_CLASS_HOLDER) == (
            "flood", "anonymous", "holder")

    def test_the_defaults_of_the_three_budgets(self):
        t = _PerPeerThrottle()
        assert (t._access_max, t._anon_max, t._global_max) == (120, 120, 120)
        assert api.PEER_LOG_ANON_MAX == 120

    HOLDER = ("auth", "rotate", "tls-validity", "pending")

    def test_access_and_flood_lines_cannot_silence_anyone_else(self, clock):
        t = _PerPeerThrottle(clock=clock)
        assert [t.allow("access-401", f"p{n}") for n in range(300)].count(True) == 120
        assert t.allow("access-403", "new") is False           # the flood budget is spent ...
        for kind in ("enroll", "enroll-locked", "protocol") + self.HOLDER:
            assert t.allow(kind, "victim") is True, kind       # ... the others are whole

    def test_anonymous_lines_cannot_silence_the_certificate_holder_kinds(self, clock):
        """120 anonymous protocol lines (a 426 needs no certificate), enroll refusals and
        lockouts are spent; auth, tls-validity, rotate and pending still get their lines."""
        t = _PerPeerThrottle(clock=clock)
        assert [t.allow("protocol", f"10.0.{n // 256}.{n % 256}") for n in range(300)].count(True) == 120
        assert t.allow("protocol", "10.9.9.9") is False
        assert t.allow("enroll", "10.9.9.9") is False              # enroll shares the anonymous budget
        assert t.allow("enroll-locked", "10.9.9.8") is False
        for kind in self.HOLDER:
            assert t.allow(kind, "victim") is True, kind
        assert t.allow("protocol", "host-1@10.9.9.9") is True      # an authenticated 426 too

    def test_holder_lines_cannot_silence_the_anonymous_kinds(self, clock):
        t = _PerPeerThrottle(clock=clock)
        assert [t.allow("auth", f"p{n}") for n in range(300)].count(True) == 120
        assert t.allow("enroll", "a") and t.allow("protocol", "a") and t.allow("enroll-locked", "a")
        assert t.allow("rate", "a") and t.allow("access-401", "a")

    def test_each_budget_resets_with_the_hour(self, clock):
        t = _PerPeerThrottle(clock=clock, global_max=2, access_max=3, anon_max=1)
        assert [t.allow("auth", f"p{n}") for n in range(4)] == [True, True, False, False]
        assert [t.allow("rate", f"p{n}") for n in range(5)] == [True] * 3 + [False] * 2
        assert [t.allow("enroll", f"p{n}") for n in range(3)] == [True, False, False]
        clock.advance(3600)
        assert t.allow("auth", "x") and t.allow("rate", "x") and t.allow("enroll", "x")

    def test_emit_writes_only_the_admitted_line(self, clock, caplog):
        t = self.make(clock)
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            results = [t.emit("rate", "p", logging.WARNING, "dropped %s", n) for n in range(3)]
        assert results == [True, False, False]
        assert [(r.levelname, r.getMessage()) for r in _peer_records(caplog)] == [
            ("WARNING", "dropped 0")]                          # no DEBUG line per refusal

    def test_emit_does_not_format_a_suppressed_line(self, clock):
        class Boom:
            def __str__(self):
                raise AssertionError("formatted")

        t = self.make(clock)
        t.emit("rate", "p", logging.WARNING, "x")
        t.emit("rate", "p", logging.WARNING, "%s", Boom())     # would raise if it were formatted

    def test_emit_counts_what_it_suppresses(self, clock):
        t = self.make(clock)
        for _ in range(4):
            t.emit("conn-cap", "p", logging.WARNING, "x")
        clock.advance(3600)
        assert "conn-cap 3" in t.take_summary()

    def test_one_line_per_kind_and_peer(self, clock):
        t = self.make(clock)
        assert [t.allow("enroll", "p") for _ in range(4)] == [True, False, False, False]

    def test_the_hour_boundary_is_exact(self, clock):
        t = self.make(clock)
        assert t.allow("enroll", "p") is True
        clock.advance(3599.999)
        assert t.allow("enroll", "p") is False
        clock.advance(0.001)                                    # exactly 3600 s after the line
        assert t.allow("enroll", "p") is True

    def test_a_suppressed_line_does_not_extend_the_hour(self, clock):
        t = self.make(clock)
        t.allow("enroll", "p")
        clock.advance(3000)
        assert t.allow("enroll", "p") is False
        clock.advance(600)
        assert t.allow("enroll", "p") is True

    def test_kinds_are_independent(self, clock):
        t = self.make(clock)
        assert t.allow("enroll", "p") and t.allow("auth", "p") and t.allow("protocol", "p")
        assert not t.allow("enroll", "p") and not t.allow("auth", "p")

    def test_peers_are_independent(self, clock):
        t = self.make(clock)
        assert [t.allow("enroll", p) for p in ("a", "b", "a", "c", "b")] == [
            True, True, False, True, False]

    def test_at_most_120_lines_an_hour_over_all_peers(self, clock):
        t = _PerPeerThrottle(clock=clock)
        answers = [t.allow("enroll", f"10.0.{n // 256}.{n % 256}") for n in range(300)]
        assert answers.count(True) == 120 and answers[:120] == [True] * 120
        assert t.allow("enroll-locked", "someone-new") is False   # the cap is over the kinds of its class

    def test_the_global_cap_resets_with_the_next_hour(self, clock):
        t = _PerPeerThrottle(clock=clock, global_max=3)
        assert [t.allow("k", f"p{n}") for n in range(5)] == [True] * 3 + [False] * 2
        clock.advance(3600)
        assert [t.allow("k", f"q{n}") for n in range(5)] == [True] * 3 + [False] * 2

    def test_a_suppressed_line_by_the_global_cap_does_not_remember_the_peer(self, clock):
        t = _PerPeerThrottle(clock=clock, global_max=1)
        assert t.allow("k", "a") is True
        assert t.allow("k", "b") is False
        assert ("k", "b") not in t._last
        clock.advance(3600)
        assert t.allow("k", "b") is True

    def test_the_key_table_is_bounded_and_evicts_the_oldest_logged(self, clock):
        t = self.make(clock, max_keys=3)
        for peer in "abc":
            assert t.allow("k", peer) is True
            clock.advance(1)
        assert t.allow("k", "d") is True                       # evicts "a"
        assert len(t._last) == 3
        assert [t.allow("k", p) for p in "bcd"] == [False] * 3
        assert t.allow("k", "a") is True                       # forgotten: logged again, early

    def test_eviction_is_by_last_logged_not_by_last_seen(self, clock):
        t = self.make(clock, max_keys=2)
        t.allow("k", "a")
        clock.advance(1)
        t.allow("k", "b")
        assert t.allow("k", "a") is False                      # seen again, not logged
        t.allow("k", "c")                                      # so "a" goes
        assert t.allow("k", "b") is False and t.allow("k", "c") is False
        assert t.allow("k", "a") is True

    def test_a_flood_of_keys_never_grows_the_table(self, clock):
        t = self.make(clock, max_keys=16)
        for n in range(1000):
            t.allow("k", f"p{n}")
            assert len(t._last) <= 16

    # -- log() -----------------------------------------------------------------------------------

    def test_log_writes_the_admitted_line_at_its_level_and_the_rest_at_debug(self, clock, caplog):
        t = self.make(clock)
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            results = [t.log("enroll", "p", logging.WARNING, "refused %s", n) for n in range(3)]
        assert results == [True, False, False]
        assert [(r.levelname, r.getMessage()) for r in _peer_records(caplog)] == [
            ("WARNING", "refused 0"), ("DEBUG", "refused 1"), ("DEBUG", "refused 2")]

    def test_log_uses_the_callers_level(self, clock, caplog):
        t = self.make(clock)
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            t.log("protocol", "p", logging.ERROR, "x")
            t.log("auth", "p", logging.WARNING, "y")
        assert [r.levelname for r in _peer_records(caplog)] == ["ERROR", "WARNING"]

    def test_log_is_on_the_dedicated_peer_logger_only(self, clock, caplog):
        t = self.make(clock)
        with caplog.at_level(logging.DEBUG):
            t.log("enroll", "p", logging.WARNING, "x")
        assert [r.name for r in caplog.records if r.getMessage() == "x"] == [PEER_LOGGER]

    def test_suppressed_lines_are_hidden_at_info(self, clock, caplog):
        t = self.make(clock)
        with caplog.at_level(logging.INFO, logger=PEER_LOGGER):
            for _ in range(5):
                t.log("enroll", "p", logging.WARNING, "x")
        assert len(_peer_records(caplog)) == 1

    # -- the summary -----------------------------------------------------------------------------

    def test_no_summary_without_suppression(self, clock):
        t = self.make(clock)
        assert t.take_summary() is None
        t.allow("k", "p")
        clock.advance(7200)
        assert t.take_summary() is None

    def test_the_summary_comes_when_the_interval_ends_not_before(self, clock):
        t = self.make(clock)
        for _ in range(4):
            t.allow("enroll", "p")                             # 3 suppressed
        clock.advance(3599.999)
        assert t.take_summary() is None
        clock.advance(0.001)
        summary = t.take_summary()
        assert summary is not None and "3 refusal log line(s) suppressed" in summary
        assert "enroll 3" in summary

    def test_the_summary_is_returned_once(self, clock):
        t = self.make(clock)
        for _ in range(3):
            t.allow("enroll", "p")
        clock.advance(3600)
        assert t.take_summary() is not None
        assert t.take_summary() is None
        clock.advance(3600)
        assert t.take_summary() is None

    def test_the_summary_counts_the_kinds(self, clock):
        t = self.make(clock)
        for _ in range(4):
            t.allow("enroll", "p")                             # 3
        for _ in range(3):
            t.allow("auth", "p")                               # 2
        t.allow("protocol", "p")                               # 0
        clock.advance(3600)
        summary = t.take_summary()
        assert "5 refusal log line(s)" in summary
        assert "auth 2, enroll 3" in summary and "protocol" not in summary

    def test_the_summary_counts_the_global_cap_suppression_too(self, clock):
        t = _PerPeerThrottle(clock=clock, global_max=2)
        for n in range(10):
            t.allow("auth", f"p{n}")
        clock.advance(3600)
        assert "8 refusal log line(s)" in t.take_summary()

    def test_intervals_are_not_mixed(self, clock):
        t = self.make(clock)
        for _ in range(3):
            t.allow("enroll", "p")                             # 2 in interval one
        clock.advance(3600)
        t.allow("auth", "q")
        t.allow("auth", "q")                                   # 1 in interval two (rolls one)
        first = t.take_summary()
        assert "enroll 2" in first and "auth" not in first or "auth 1" not in first.split("(")[0]
        clock.advance(3600)
        second = t.take_summary()
        assert second is not None and "auth 1" in second and "enroll" not in second

    def test_the_interval_rolls_even_if_nobody_asks(self, clock):
        t = _PerPeerThrottle(clock=clock, global_max=1)
        t.allow("k", "a")
        assert t.allow("k", "b") is False
        clock.advance(3600)
        assert t.allow("k", "b") is True                       # allow() rolled the window itself

    def test_the_wording_names_the_hour(self, clock):
        t = self.make(clock)
        t.allow("k", "p")
        t.allow("k", "p")
        clock.advance(3600)
        assert "in the last hour" in t.take_summary()
        t2 = self.make(clock, every=90)
        t2.allow("k", "p")
        t2.allow("k", "p")
        clock.advance(90)
        assert "in the last 90 seconds" in t2.take_summary()

    # -- threads ---------------------------------------------------------------------------------

    def test_exactly_one_of_many_simultaneous_lines_is_admitted(self):
        t = _PerPeerThrottle()
        barrier = threading.Barrier(32)
        results = []

        def go():
            barrier.wait(10)
            results.append(t.allow("enroll", "203.0.113.7"))

        threads = [threading.Thread(target=go) for _ in range(32)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(10)
        assert sorted(results) == [False] * 31 + [True]

    def test_threads_hammering_keep_the_bounds_and_count_every_suppression(self):
        clock = Clock()
        t = _PerPeerThrottle(clock=clock, max_keys=16, global_max=40)
        admitted = []
        errors = []

        def hammer(seed):
            try:
                for i in range(500):
                    if t.allow(f"kind{i % 3}", f"p{(seed * 7 + i) % 90}"):
                        admitted.append(1)
                    with t._guard:
                        assert len(t._last) <= 16
            except BaseException as exc:                    # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=hammer, args=(n,)) for n in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(60)
        assert errors == [] and not any(th.is_alive() for th in threads)
        assert len(admitted) == 40                              # the global cap, exactly
        clock.advance(3600)
        summary = t.take_summary()
        assert f"{8 * 500 - 40} refusal log line(s) suppressed" in summary

    def test_it_has_a_lock(self):
        t = _PerPeerThrottle()
        assert hasattr(t._guard, "acquire") and hasattr(t._guard, "release")


# ===========================================================================
# The bundle, the constants and the wiring
# ===========================================================================


@pytest.mark.unit
class TestPeerLimits:
    def test_a_default_bundle_has_all_three(self):
        limits = PeerLimits()
        assert isinstance(limits.rate, PeerRateLimiter)
        assert isinstance(limits.enroll, EnrollGuard)
        assert isinstance(limits.log, _PerPeerThrottle)

    def test_the_clock_reaches_all_three(self):
        clock = Clock()
        limits = PeerLimits(clock=clock)
        assert limits.rate._clock is clock and limits.enroll._clock is clock
        assert limits.log._clock is clock

    def test_parts_can_be_supplied(self):
        rate, enroll, log = PeerRateLimiter(), EnrollGuard(), _PerPeerThrottle()
        limits = PeerLimits(rate=rate, enroll=enroll, log=log)
        assert (limits.rate, limits.enroll, limits.log) == (rate, enroll, log)

    def test_flush_log_writes_the_summary_as_a_warning_once(self, caplog):
        clock = Clock()
        limits = PeerLimits(clock=clock)
        for _ in range(4):
            limits.log.allow("enroll", "p")
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            limits.flush_log()
            assert _peer_records(caplog) == []                  # the hour is not over
            clock.advance(3600)
            limits.flush_log()
            limits.flush_log()
        (record,) = _peer_records(caplog)
        assert record.levelname == "WARNING" and record.name == PEER_LOGGER
        assert record.getMessage().startswith("API: 3 refusal log line(s) suppressed")

    def test_flush_log_never_raises(self, caplog):
        limits = PeerLimits()
        limits.log.take_summary = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
        limits.flush_log()

    def test_the_old_per_feature_names_are_gone(self):
        assert not hasattr(api, "PROTOCOL_ERROR_LINES_PER_PEER")
        assert not hasattr(api, "PROTOCOL_LOGGED_PEERS")
        assert not hasattr(workers, "_ValidityWarnThrottle")

    def test_the_throttled_access_statuses(self):
        assert api.THROTTLED_ACCESS_STATUSES == frozenset({401, 403, 426, 429, 503})

    def test_the_per_client_pending_cap_constant(self):
        assert api.PENDING_CONFIGS_PER_CLIENT == 256
        assert api.PENDING_CONFIGS_PER_CLIENT < api.PENDING_CONFIGS_MAX

    def test_create_app_publishes_the_limits_it_was_given(self):
        limits = PeerLimits()
        authed = Authed(_config(), peer_limits=limits)
        assert authed.app.extensions["zfsbackup.peer_limits"] is limits

    def test_create_app_makes_its_own_when_none_is_given(self):
        a, b = Authed(_config()), Authed(_config())
        assert isinstance(a.limits, PeerLimits) and a.limits is not b.limits

    def test_the_server_uses_the_apps_limits(self, tmp_path):
        authed = Authed(_config())
        gateway, _ident, _trust = make_gateway(tmp_path / "srv.tls")
        server = workers._bind_api_server(authed.app, LOOPBACK, 0, gateway)
        try:
            assert server.limits is authed.limits
        finally:
            server.server_close()

    def test_an_explicit_limits_argument_wins_over_the_apps(self, tmp_path):
        authed = Authed(_config())
        mine = PeerLimits()
        gateway, _ident, _trust = make_gateway(tmp_path / "srv.tls")
        server = workers._api_server_class()(LOOPBACK, 0, authed.app, gateway, limits=mine)
        try:
            assert server.limits is mine
        finally:
            server.server_close()

    def test_a_server_over_a_bare_wsgi_app_gets_a_private_bundle(self, tmp_path):
        gateway, _ident, _trust = make_gateway(tmp_path / "srv.tls")
        server = workers._api_server_class()(LOOPBACK, 0, lambda e, s: [], gateway)
        try:
            assert isinstance(server.limits, PeerLimits)
        finally:
            server.server_close()


# ===========================================================================
# /enroll through the app
# ===========================================================================


_UNSET = object()


def _post(authed, path, *, addr=None, protocol=_UNSET, headers=None, wsgi_input=None, **kwargs):
    """POST as an anonymous caller from `addr` (REMOTE_ADDR); `protocol=None`: no protocol
    header, another value: that one, unset: this code's."""
    environ = {} if addr is None else {"REMOTE_ADDR": addr}
    if wsgi_input is not None:           # a body that never arrives, announced as 100 bytes
        environ.update({"wsgi.input": wsgi_input, "CONTENT_LENGTH": "100",
                        "CONTENT_TYPE": "application/json"})
    client = authed.client(None) if protocol is _UNSET else authed.client(None, protocol=protocol)
    return client.post(path, environ_overrides=environ, headers=headers or {}, **kwargs)


def bad_enroll(authed, *, addr=None, **kw):
    """A malformed `/enroll` (no client id): a 400 the peer is charged for."""
    return _post(authed, "/enroll", addr=addr, json={"version": 1}, **kw)


def good_body(authed, client_id="host-z", cert=None):
    cert = cert or make_client_cert()
    token = str(identity.make_token(authed.server_fp)[0])
    return enroll_body(authed.server_fp, client_id, cert, token)


class _Stalled:
    """A request body stream that never delivers: `read` waits for `release` (forever by
    default), then raises `raises` -- what a peer that sent headers and a Content-Length but no
    body looks like to the app."""

    def __init__(self, raises=None, release=None):
        self.raises = raises if raises is not None else TimeoutError("timed out")
        self.release = release
        self.entered = threading.Event()

    def read(self, *args):
        self.entered.set()
        if self.release is not None:
            self.release.wait(30)
        raise self.raises

    readline = read

    def __iter__(self):
        return self

    def __next__(self):
        self.read()


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def limits(clock):
    return PeerLimits(clock=clock)


@pytest.fixture
def authed(limits, zfs):
    return Authed(_config(), peer_limits=limits)


@pytest.mark.integration
class TestEnrollLockoutApp:
    def test_five_failures_then_429(self, authed, zfs):
        assert [bad_enroll(authed).status_code for _ in range(5)] == [400] * 5
        resp = bad_enroll(authed)
        assert resp.status_code == 429

    def test_the_429_body_and_retry_after(self, authed):
        for _ in range(5):
            bad_enroll(authed)
        resp = bad_enroll(authed)
        body = resp.get_json()
        assert body["reason"] == "rate_limited" and body["retry_after"] == 900
        assert "900 seconds" in body["error"]
        assert resp.headers["Retry-After"] == "900"
        assert resp.headers[PROTOCOL_HEADER] if PROTOCOL_HEADER in resp.headers else True

    def test_retry_after_counts_down(self, authed, clock):
        for _ in range(5):
            bad_enroll(authed)
        assert bad_enroll(authed).headers["Retry-After"] == "900"
        clock.advance(100)
        assert bad_enroll(authed).headers["Retry-After"] == "800"
        clock.advance(799.5)
        resp = bad_enroll(authed)
        assert resp.status_code == 429 and resp.headers["Retry-After"] == "1"
        clock.advance(0.5)
        assert bad_enroll(authed).status_code == 400            # the lockout is over

    def test_a_locked_peer_is_refused_without_reading_the_body_or_touching_the_store(
        self, authed, zfs
    ):
        for _ in range(5):
            bad_enroll(authed)
        big = b'{"pad": "' + b"x" * (2 * 1024 * 1024) + b'"}'
        resp = _post(authed, "/enroll", data=big, content_type="application/json")
        assert resp.status_code == 429                          # 413 would mean the body was read
        resp = _post(authed, "/enroll", json=good_body(authed))
        assert resp.status_code == 429
        gw = authed.gateway
        assert gw.precheck_calls == [] and gw.enroll_calls == [] and gw.refresh_calls == 0
        zfs.exists.assert_not_called()

    def test_a_locked_peer_cannot_enroll_even_with_a_valid_request(self, authed):
        for _ in range(5):
            bad_enroll(authed)
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 429
        assert authed.gateway.enroll_calls == []

    def test_the_lockout_is_for_the_peer_not_for_the_server(self, authed):
        for _ in range(5):
            bad_enroll(authed)
        assert bad_enroll(authed).status_code == 429
        assert bad_enroll(authed, addr="10.0.0.5").status_code == 400
        resp = _post(authed, "/enroll", addr="10.0.0.5", json=good_body(authed))
        assert resp.status_code == 200

    def test_ipv6_addresses_in_one_slash_64_share_a_lockout(self, authed):
        for n in range(1, 6):
            assert bad_enroll(authed, addr=f"2001:db8:1:2::{n:x}").status_code == 400
        assert bad_enroll(authed, addr="2001:db8:1:2:ffff::9").status_code == 429
        assert bad_enroll(authed, addr="2001:db8:1:3::1").status_code == 400

    def test_a_mapped_ipv6_address_is_the_ipv4_peer(self, authed):
        for _ in range(5):
            bad_enroll(authed, addr="203.0.113.7")
        assert bad_enroll(authed, addr="::ffff:203.0.113.7").status_code == 429

    def test_x_forwarded_for_neither_dodges_nor_frames(self, authed):
        for n in range(5):
            bad_enroll(authed, headers={"X-Forwarded-For": f"198.51.100.{n}"})
        assert bad_enroll(authed, headers={"X-Forwarded-For": "198.51.100.200"}).status_code == 429
        # a spoofed header naming somebody else locks nobody else out
        assert bad_enroll(authed, addr="198.51.100.0").status_code == 400

    def test_a_pinned_clients_other_requests_are_unaffected_by_a_lockout(self, authed, zfs):
        for _ in range(5):
            bad_enroll(authed)
        assert bad_enroll(authed).status_code == 429
        assert authed.client().get("/whoami").status_code == 200
        assert authed.client(None).get("/health").status_code == 200
        resp = authed.client().post(
            "/rotate", json={"version": 1}, environ_overrides={"REMOTE_ADDR": "127.0.0.1"})
        assert resp.status_code != 429

    def test_a_426_comes_before_the_lockout(self, authed):
        for _ in range(5):
            bad_enroll(authed)
        resp = _post(authed, "/enroll", protocol=None, json={})
        assert resp.status_code == 426                            # not 429

    def test_a_426_is_not_a_failure(self, authed):
        for _ in range(4):
            bad_enroll(authed)
        for _ in range(30):
            assert _post(authed, "/enroll", protocol="2", json={}).status_code == 426
        assert bad_enroll(authed).status_code == 400              # the 5th failure, not the 35th
        assert bad_enroll(authed).status_code == 429

    def test_a_success_forgets_the_failures(self, authed):
        for _ in range(4):
            bad_enroll(authed)
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 200
        for _ in range(4):
            assert bad_enroll(authed).status_code == 400
        assert bad_enroll(authed).status_code == 400              # the 5th: locks only now
        assert bad_enroll(authed).status_code == 429

    def test_already_trusted_also_forgets(self, authed):
        authed.gateway.enroll_effect = EnrollOutcome.ALREADY_TRUSTED
        for _ in range(4):
            bad_enroll(authed)
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 200
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0
        for _ in range(4):
            assert bad_enroll(authed).status_code == 400

    def test_a_refusal_does_not_forget(self, authed):
        authed.gateway.enroll_effect = EnrollOutcome.CLIENT_ID_TAKEN
        for _ in range(4):
            bad_enroll(authed)
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 409   # the 5th failure
        assert bad_enroll(authed).status_code == 429

    def test_the_lockout_ends_and_the_window_slides(self, authed, clock):
        for _ in range(5):
            bad_enroll(authed)
        clock.advance(900)
        assert [bad_enroll(authed).status_code for _ in range(5)] == [400] * 5
        assert bad_enroll(authed).status_code == 429

    # -- what is a failure ---------------------------------------------------------------------

    def _case(self, authed, name):
        """-> the request that fails in way `name`, as a callable returning a response."""
        cert = make_client_cert()
        base = good_body(authed, cert=cert)

        def post(body):
            return _post(authed, "/enroll", json=body)

        gw = authed.gateway
        if name == "unreadable body":
            return lambda: _post(authed, "/enroll", wsgi_input=_Stalled(raises=TimeoutError("t"))), 400
        if name == "short body":
            import io
            return lambda: _post(authed, "/enroll", wsgi_input=io.BytesIO(b'{"version": 1, "cl')), 400
        if name == "client disconnected":
            from werkzeug.exceptions import ClientDisconnected

            return lambda: _post(authed, "/enroll", wsgi_input=_Stalled(raises=ClientDisconnected())), 400
        if name == "malformed json":
            return lambda: _post(authed, "/enroll", data=b"\xff not json",
                                 content_type="application/json"), 400
        if name == "not an object":
            return lambda: _post(authed, "/enroll", json=[1, 2]), 400
        if name == "wrong version":
            return lambda: post({**base, "version": 99}), 400
        if name == "too large":
            return lambda: _post(authed, "/enroll", data=b"x" * (70 * 1024),
                                 content_type="application/json"), 413
        if name == "bad client id":
            return lambda: post({**base, "client_id": "!!"}), 400
        if name == "bad label":
            return lambda: post({**base, "label": 5}), 400
        if name == "incomplete":
            return lambda: post({k: v for k, v in base.items() if k != "signature"}), 400
        if name == "malformed token":
            return lambda: post({**base, "token_secret": "garbage"}), 400
        if name == "other server":
            other = str(identity.make_token("cd" * 32)[0])
            return lambda: post({**base, "token_secret": other}), 403
        if name == "bad certificate":
            expired = make_client_cert(
                not_before=__import__("datetime").datetime.now(__import__("datetime").timezone.utc)
                - __import__("datetime").timedelta(days=9),
                not_after=__import__("datetime").datetime.now(__import__("datetime").timezone.utc)
                - __import__("datetime").timedelta(days=1))
            return lambda: post(good_body(authed, cert=expired)), 422
        if name == "bad signature":
            return lambda: post({**base, "signature": "QUJD"}), 403
        if name == "precheck refuses":
            gw.precheck_effect = False
            return lambda: post(base), 403
        if name == "client id taken":
            gw.enroll_effect = EnrollOutcome.CLIENT_ID_TAKEN
            return lambda: post(base), 409
        if name == "cert bound elsewhere":
            gw.enroll_effect = EnrollOutcome.CERT_BOUND_ELSEWHERE
            return lambda: post(base), 409
        if name == "token refused":
            gw.enroll_effect = EnrollOutcome.REFUSED
            return lambda: post(base), 403
        raise AssertionError(name)

    FAILURES = [
        "unreadable body", "client disconnected", "short body", "malformed json", "not an object", "wrong version", "too large", "bad client id",
        "bad label", "incomplete", "malformed token", "other server", "bad certificate",
        "bad signature", "precheck refuses", "client id taken",
        "cert bound elsewhere", "token refused",
    ]

    @pytest.mark.parametrize("name", FAILURES)
    def test_every_failure_path_counts_towards_the_lockout(self, authed, name):
        request, status = self._case(authed, name)
        assert [request().status_code for _ in range(5)] == [status] * 5
        assert request().status_code == 429

    @pytest.mark.parametrize("name", FAILURES)
    def test_four_of_them_are_not_yet_a_lockout(self, authed, name):
        request, status = self._case(authed, name)
        assert [request().status_code for _ in range(4)] == [status] * 4
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_503_from_the_store_is_not_a_failure(self, authed):
        from zfsbackup.config.store import ConfigBusy

        authed.gateway.precheck_effect = ConfigBusy("busy")
        body = good_body(authed)
        for _ in range(20):
            assert _post(authed, "/enroll", json=body).status_code == 503
        authed.gateway.precheck_effect = True
        for _ in range(4):
            assert bad_enroll(authed).status_code == 400
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_store_that_is_down_is_not_a_failure(self, authed):
        authed.gateway.precheck_effect = RuntimeError("disk gone")
        body = good_body(authed)
        for _ in range(20):
            assert _post(authed, "/enroll", json=body).status_code == 503
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_failing_zfs_is_not_a_failure(self, authed, zfs):
        zfs.exists.side_effect = RuntimeError("zfs hung")
        body = good_body(authed)
        for _ in range(20):
            assert _post(authed, "/enroll", json=body).status_code == 503
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_draining_server_is_not_a_failure(self, authed):
        authed.gate.start_draining()
        for _ in range(20):
            assert _post(authed, "/enroll", json=good_body(authed)).status_code == 503
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_disabled_remote_backup_is_not_a_failure(self, limits, zfs):
        off = Authed(BackupConfig(datasets=[DatasetConfig(name="pool/data")]), peer_limits=limits)
        for _ in range(20):
            assert _post(off, "/enroll", json={"version": 1}).status_code == 503
        assert limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_dry_run_server_is_not_a_failure(self, limits, zfs):
        config = _config()
        config.dry_run = True
        dry = Authed(config, peer_limits=limits)
        for _ in range(20):
            assert _post(dry, "/enroll", json={"version": 1}).status_code == 503
        assert limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_lockout_is_logged_once_and_the_429s_are_quiet(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for _ in range(5):
                bad_enroll(authed)
            for _ in range(20):
                assert bad_enroll(authed).status_code == 429
        warnings = [r for r in _peer_records(caplog, levels=("WARNING",))]
        texts = [r.getMessage() for r in warnings]
        assert len(warnings) == 2, texts
        assert any("refused: malformed request" in t or "refused:" in t for t in texts)
        assert any("locked out of /enroll for 900 seconds" in t for t in texts)

    def test_an_enroll_to_a_locked_peer_is_not_charged_again(self, authed, clock):
        for _ in range(5):
            bad_enroll(authed)
        for _ in range(50):
            bad_enroll(authed)
        clock.advance(900)
        assert bad_enroll(authed).status_code == 400              # one failure, not 55


# ===========================================================================
# Every refusal site goes through the throttle
# ===========================================================================


class _Recording(_PerPeerThrottle):
    """A throttle that remembers `(kind, peer, level)` of every `log` call."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.calls = []

    def log(self, kind, peer, level, msg, *args):
        self.calls.append((kind, peer, level))
        return super().log(kind, peer, level, msg, *args)

    def kinds(self):
        return [c[0] for c in self.calls]


@pytest.mark.integration
class TestRefusalLogsAreThrottled:
    @pytest.fixture
    def rec(self, clock):
        return _Recording(clock=clock, global_max=10 ** 9)

    @pytest.fixture
    def app(self, rec, clock, zfs):
        return Authed(_config(), peer_limits=PeerLimits(clock=clock, log=rec))

    @staticmethod
    def _no_direct_warnings(caplog):
        """No WARNING/ERROR is written on the plain `zfsbackup.api` logger for a refusal."""
        return [r for r in caplog.records
                if r.name == "zfsbackup.api" and r.levelno >= logging.WARNING]

    def _repeat(self, fn, n=3):
        for _ in range(n):
            fn()

    def test_enroll_refusals(self, app, rec, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            self._repeat(lambda: bad_enroll(app))
        assert rec.kinds() == ["enroll"] * 3
        assert [r.levelname for r in _peer_records(caplog)] == ["WARNING", "DEBUG", "DEBUG"]
        assert self._no_direct_warnings(caplog) == []

    def test_enroll_lockout(self, app, rec, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for _ in range(5):
                bad_enroll(app)
            self._repeat(lambda: bad_enroll(app))
        assert "enroll-locked" in rec.kinds()
        assert [c for c in rec.calls if c[0] == "enroll-locked"][0][2] == logging.WARNING
        assert self._no_direct_warnings(caplog) == []
        assert len(_peer_records(caplog, levels=("WARNING",), text="locked out")) == 1

    def test_rotate_refusals(self, app, rec, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            self._repeat(lambda: app.client().post("/rotate", json={"version": 1}))
        assert rec.kinds() == ["rotate"] * 3
        assert rec.calls[0][1] == f"{CLIENT_ID}@127.0.0.1"
        assert [r.levelname for r in _peer_records(caplog)] == ["WARNING", "DEBUG", "DEBUG"]
        assert self._no_direct_warnings(caplog) == []

    def test_an_unknown_certificate(self, app, rec, caplog):
        stranger = make_client_cert(cn="stranger")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for _ in range(3):
                assert app.client(stranger).get("/whoami").status_code == 403
        assert rec.kinds() == ["auth"] * 3 and rec.calls[0][1] == "127.0.0.1"
        assert [r.levelname for r in _peer_records(caplog)] == ["WARNING", "DEBUG", "DEBUG"]
        assert self._no_direct_warnings(caplog) == []

    def test_a_client_id_mismatch(self, app, rec, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for _ in range(3):
                assert app.client().get("/backup/someone-else/pool/data/resume_token").status_code == 403
        assert rec.kinds() == ["auth"] * 3
        assert rec.calls[0][1] == f"{CLIENT_ID}@127.0.0.1"
        assert [r.levelname for r in _peer_records(caplog)] == ["WARNING", "DEBUG", "DEBUG"]

    def test_protocol_refusals(self, app, rec, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            self._repeat(lambda: app.client(protocol="2").get("/whoami"))
            self._repeat(lambda: _post(app, "/enroll", protocol="2", json={}))
        assert rec.kinds() == ["protocol"] * 6
        assert [r.levelname for r in _peer_records(caplog)] == [
            "ERROR", "DEBUG", "DEBUG", "ERROR", "DEBUG", "DEBUG"]    # client@peer, then peer
        assert self._no_direct_warnings(caplog) == []

    def test_a_busy_config_database(self, app, rec, caplog):
        from zfsbackup.config.store import ConfigBusy

        app.gateway.precheck_effect = ConfigBusy("busy")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for _ in range(3):
                assert _post(app, "/enroll", json=good_body(app)).status_code == 503
        assert rec.kinds() == ["store-busy"] * 3
        assert [r.levelname for r in _peer_records(caplog)] == ["WARNING", "DEBUG", "DEBUG"]
        assert self._no_direct_warnings(caplog) == []

    def test_the_throttle_is_the_one_in_the_bundle(self, app, rec):
        assert app.limits.log is rec

    def test_anonymous_426s_cannot_silence_auth_or_pending_lines(self, limits, zfs, caplog):
        authed = Authed(_config(), peer_limits=limits)
        stranger = make_client_cert(cn="stranger")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for n in range(300):
                _post(authed, "/enroll", addr=f"10.1.{n // 256}.{n % 256}", protocol="2", json={})
            assert len(_peer_records(caplog, levels=("ERROR",))) == 120
            caplog.clear()
            assert authed.client(stranger).get("/whoami").status_code == 403       # auth
            assert authed.client(protocol="2").get("/whoami").status_code == 426   # client@peer
            limits.log.log("pending", "client1", logging.WARNING, "pending")
            limits.log.log("tls-validity", "203.0.113.9", logging.WARNING, "tls")
        assert [r.levelname for r in _peer_records(caplog)] == ["WARNING", "ERROR", "WARNING", "WARNING"]

    def test_a_flood_of_refusals_from_many_peers_is_bounded_by_the_global_cap(
        self, limits, zfs, caplog
    ):
        authed = Authed(_config(), peer_limits=limits)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for n in range(300):
                _post(authed, "/enroll", addr=f"10.1.{n // 256}.{n % 256}", protocol="2", json={})
        assert len(_peer_records(caplog, levels=("ERROR",))) == 120


# ===========================================================================
# log_request: the access line for repeated refusals
# ===========================================================================


@pytest.fixture
def handler_for(tmp_path):
    """-> `make(limits, address)`: a bare request handler whose server carries `limits`."""
    gateway, _ident, _trust = make_gateway(tmp_path / "srv.tls")
    made = []

    def make(limits, address=("203.0.113.7", 4242), path="/whoami"):
        server = workers._api_server_class()(LOOPBACK, 0, lambda e, s: [], gateway, limits=limits)
        made.append(server)
        h = server.RequestHandlerClass.__new__(server.RequestHandlerClass)
        h.server = server
        h.command, h.path, h.request_version = "GET", path, "HTTP/1.1"
        h.requestline = f"GET {path} HTTP/1.1"
        h.client_address = address
        return h

    yield make
    for server in made:
        server.server_close()


@pytest.mark.unit
class TestAccessLineThrottle:
    def _levels(self, caplog):
        return [r.levelno for r in caplog.records if r.name == "werkzeug"]

    @pytest.mark.parametrize("status", [401, 403, 426, 429, 503])
    def test_a_repeated_refusal_is_logged_once_an_hour_per_peer(
        self, handler_for, clock, caplog, status
    ):
        h = handler_for(PeerLimits(clock=clock))
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            for _ in range(4):
                h.log_request(status, 1)
        want = logging.WARNING if status >= 500 else logging.INFO
        assert self._levels(caplog) == [want, logging.DEBUG, logging.DEBUG, logging.DEBUG]

    @pytest.mark.parametrize("status", [400, 404, 409, 500, 502])
    def test_other_statuses_are_not_throttled(self, handler_for, clock, caplog, status):
        h = handler_for(PeerLimits(clock=clock))
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            for _ in range(3):
                h.log_request(status, 1)
        want = logging.WARNING if status >= 500 else logging.INFO
        assert self._levels(caplog) == [want] * 3

    def test_success_stays_debug(self, handler_for, clock, caplog):
        h = handler_for(PeerLimits(clock=clock))
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            h.log_request(200, 1)
        assert self._levels(caplog) == [logging.DEBUG]

    def test_the_key_is_peer_and_status(self, handler_for, clock, caplog):
        limits = PeerLimits(clock=clock)
        a, b = handler_for(limits), handler_for(limits, ("203.0.113.8", 1))
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            a.log_request(401, 1)
            a.log_request(403, 1)                       # another status of the same peer
            b.log_request(401, 1)                       # the same status of another peer
            a.log_request(401, 1)
        assert self._levels(caplog) == [logging.INFO, logging.INFO, logging.INFO, logging.DEBUG]

    def test_the_port_does_not_make_a_new_peer(self, handler_for, clock, caplog):
        limits = PeerLimits(clock=clock)
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            for port in (1, 2, 3):
                handler_for(limits, ("203.0.113.7", port)).log_request(401, 1)
        assert self._levels(caplog) == [logging.INFO, logging.DEBUG, logging.DEBUG]

    def test_an_hour_later_the_peer_is_logged_again(self, handler_for, clock, caplog):
        limits = PeerLimits(clock=clock)
        h = handler_for(limits)
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            h.log_request(401, 1)
            clock.advance(3599)
            h.log_request(401, 1)
            clock.advance(1)
            h.log_request(401, 1)
        assert self._levels(caplog) == [logging.INFO, logging.DEBUG, logging.INFO]

    def test_the_suppressed_lines_are_counted_by_kind(self, handler_for, clock):
        limits = PeerLimits(clock=clock)
        h = handler_for(limits)
        for _ in range(4):
            h.log_request(401, 1)
        clock.advance(3600)
        assert "access-401 3" in limits.log.take_summary()

    def test_the_line_keeps_its_format(self, handler_for, clock, caplog):
        h = handler_for(PeerLimits(clock=clock))
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            h.log_request(401, 12)
        (rec,) = [r for r in caplog.records if r.name == "werkzeug"]
        assert '"GET /whoami HTTP/1.1" 401 12' in rec.getMessage()

    def test_a_handler_without_limits_still_logs(self, handler_for, clock, caplog):
        h = handler_for(PeerLimits(clock=clock))
        del h.server
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            h.log_request(401, 1)
            h.log_request(401, 1)
        assert self._levels(caplog) == [logging.INFO, logging.INFO]


@pytest.mark.integration
class TestEnrollInFlight:
    """The in-flight cap is taken AFTER the body was read and the cheap checks passed, right
    before the token pre-check (the first store call): 2 at once in all, 1 per peer, 503 busy
    otherwise (never a failure)."""

    def _hold(self, authed, *keys):
        for key in keys:
            assert authed.limits.enroll.try_enter(key)

    def test_two_in_flight_elsewhere_make_a_third_enrollment_503_busy(self, authed):
        self._hold(authed, "198.51.100.1", "198.51.100.2")
        resp = _post(authed, "/enroll", json=good_body(authed))
        assert resp.status_code == 503
        assert resp.get_json() == {
            "error": "the server is busy with other enrollments; retry in a moment",
            "reason": "busy", "retry": True,
        }
        assert resp.headers["Retry-After"] == "2"

    def test_a_busy_refusal_reaches_neither_the_store_nor_zfs(self, authed, zfs):
        self._hold(authed, "198.51.100.1", "198.51.100.2")
        _post(authed, "/enroll", json=good_body(authed))
        gw = authed.gateway
        assert gw.precheck_calls == [] and gw.enroll_calls == [] and gw.refresh_calls == 0
        zfs.exists.assert_not_called()

    def test_a_busy_refusal_leaves_the_count_alone(self, authed):
        self._hold(authed, "198.51.100.1", "198.51.100.2")
        _post(authed, "/enroll", json=good_body(authed))
        assert authed.limits.enroll.in_flight == 2              # it did not leave a slot it never took

    def test_busy_is_not_a_failure(self, authed):
        self._hold(authed, "198.51.100.1", "198.51.100.2")
        for _ in range(20):
            assert _post(authed, "/enroll", json=good_body(authed)).status_code == 503
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0

    def test_once_a_slot_is_free_the_same_request_succeeds(self, authed):
        self._hold(authed, "198.51.100.1", "198.51.100.2")
        body = good_body(authed)
        assert _post(authed, "/enroll", json=body).status_code == 503
        authed.limits.enroll.leave("198.51.100.1")
        assert _post(authed, "/enroll", json=body).status_code == 200

    def test_one_in_flight_per_peer(self, authed):
        self._hold(authed, "127.0.0.1")
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 503
        # another peer is served (one slot is free globally)
        assert _post(authed, "/enroll", addr="10.0.0.5", json=good_body(authed)).status_code == 200

    def test_a_peer_is_its_ipv6_slash_64_for_the_in_flight_count_too(self, authed):
        self._hold(authed, peer_key(("2001:db8:1:2::1", 1, 0, 0)))
        resp = _post(authed, "/enroll", addr="2001:db8:1:2::99", json=good_body(authed))
        assert resp.status_code == 503

    def test_the_cheap_checks_come_before_the_cap(self, authed):
        """A malformed request is a 400 (and a failure) even when the server is busy: it never
        wanted a slot."""
        self._hold(authed, "198.51.100.1", "198.51.100.2")
        assert bad_enroll(authed).status_code == 400
        assert authed.limits.enroll.in_flight == 2

    def test_the_lockout_comes_before_the_cap(self, authed):
        for _ in range(5):
            bad_enroll(authed)
        self._hold(authed, "198.51.100.1", "198.51.100.2")
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 429

    def test_the_protocol_check_comes_before_the_cap(self, authed):
        self._hold(authed, "198.51.100.1", "198.51.100.2")
        assert _post(authed, "/enroll", protocol="2", json=good_body(authed)).status_code == 426

    # -- the slot is given back on every outcome -----------------------------------------------

    def test_released_after_a_success(self, authed):
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 200
        assert authed.limits.enroll.in_flight == 0
        assert authed.limits.enroll._in_flight_by_peer == {}

    def test_released_after_a_refusal_past_the_slot(self, authed):
        authed.gateway.precheck_effect = False
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 403
        assert authed.limits.enroll.in_flight == 0

    @pytest.mark.parametrize("outcome", [
        EnrollOutcome.CLIENT_ID_TAKEN, EnrollOutcome.CERT_BOUND_ELSEWHERE, EnrollOutcome.REFUSED,
        EnrollOutcome.ALREADY_TRUSTED,
    ])
    def test_released_after_every_store_outcome(self, authed, outcome):
        authed.gateway.enroll_effect = outcome
        _post(authed, "/enroll", json=good_body(authed))
        assert authed.limits.enroll.in_flight == 0

    def test_released_after_a_store_error(self, authed):
        authed.gateway.precheck_effect = RuntimeError("down")
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 503
        assert authed.limits.enroll.in_flight == 0

    def test_released_after_a_zfs_error(self, authed, zfs):
        zfs.exists.side_effect = RuntimeError("hung")
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 503
        assert authed.limits.enroll.in_flight == 0

    def test_released_when_the_handler_blows_up(self, authed, zfs):
        zfs.exists.side_effect = KeyboardInterrupt()           # not an Exception: nothing catches it
        with pytest.raises(KeyboardInterrupt):
            _post(authed, "/enroll", json=good_body(authed))
        assert authed.limits.enroll.in_flight == 0
        assert authed.limits.enroll._in_flight_by_peer == {}
        zfs.exists.side_effect = None
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 200

    def test_a_request_that_never_got_a_slot_gives_none_back(self, authed):
        self._hold(authed, "198.51.100.1")
        for _ in range(3):
            bad_enroll(authed)                                  # 400 before the slot
        for _ in range(3):
            _post(authed, "/enroll", protocol="2", json={})     # 426
        assert authed.limits.enroll.in_flight == 1

    # -- the stalled-body attack -----------------------------------------------------------------

    def test_stalled_bodies_hold_no_slot_and_do_not_make_a_third_peer_503(self, authed):
        """Two connections that send headers and a Content-Length and then nothing sit in the
        body read. They must not hold the in-flight slots: a third peer enrolls fine."""
        release = threading.Event()
        stalls = [_Stalled(release=release) for _ in range(2)]
        results = []

        def attacker(n):
            results.append(_post(authed, "/enroll", addr=f"198.51.100.{n}", wsgi_input=stalls[n]))

        threads = [threading.Thread(target=attacker, args=(n,), daemon=True) for n in range(2)]
        for t in threads:
            t.start()
        try:
            assert all(st.entered.wait(10) for st in stalls)
            assert authed.limits.enroll.in_flight == 0
            resp = _post(authed, "/enroll", addr="203.0.113.9", json=good_body(authed))
            assert resp.status_code == 200
            assert authed.gateway.enroll_calls                      # it really reached the store
        finally:
            release.set()
            for t in threads:
                t.join(10)
        assert not any(t.is_alive() for t in threads)
        assert [r.status_code for r in results] == [400, 400]

    def test_stalled_bodies_count_towards_the_lockout(self, authed):
        for _ in range(5):
            resp = _post(authed, "/enroll", wsgi_input=_Stalled(raises=TimeoutError("read deadline")))
            assert resp.status_code == 400
        assert authed.limits.enroll.retry_after("127.0.0.1") == 900
        assert _post(authed, "/enroll", json=good_body(authed)).status_code == 429

    def test_the_stalled_peer_alone_is_locked_out(self, authed):
        for _ in range(5):
            _post(authed, "/enroll", addr="198.51.100.1",
                  wsgi_input=_Stalled(raises=TimeoutError("read deadline")))
        assert _post(authed, "/enroll", addr="203.0.113.9", json=good_body(authed)).status_code == 200


# ===========================================================================
# Negotiate's pending map: a per-client cap
# ===========================================================================


def _ds(client, name):
    return f"{TARGET}/{client}/{name}"


@pytest.mark.integration
class TestPendingPerClientCap:
    CFG = "cfg"

    @pytest.fixture
    def app(self, limits, zfs):
        return Authed(_config(), peer_limits=limits)

    @staticmethod
    def _claim(app, client, name, config="cfg"):
        claim = pending_api(app.app)[0]
        return claim(_ds(client, name), config, f"pool/{name}")

    def _in_step(self, app):
        """The three maps agree: every entry has a deadline and an owner, nothing else does."""
        configs = pending_configs(app.app)
        by_client = pending_by_client(app.app)
        owned = [name for per in by_client.values() for name in per]
        assert sorted(owned) == sorted(configs)
        assert sorted(pending_deadlines(app.app)) == sorted(configs)
        assert all(per for per in by_client.values()), "an empty owner list was left behind"
        for client, per in by_client.items():
            assert all(name.startswith(f"{TARGET}/{client}/") for name in per)

    def test_the_constant(self):
        assert api.PENDING_CONFIGS_PER_CLIENT == 256

    def test_a_client_with_300_names_keeps_its_newest_256(self, app):
        for n in range(300):
            assert self._claim(app, "client1", f"d{n}") is None
        mine = list(pending_by_client(app.app)["client1"])
        assert len(mine) == 256
        assert mine == [_ds("client1", f"d{n}") for n in range(44, 300)]
        assert sorted(pending_configs(app.app)) == sorted(mine)
        self._in_step(app)

    def test_another_clients_entries_are_untouched(self, app):
        for n in range(10):
            self._claim(app, "client2", f"b{n}")
        for n in range(300):
            self._claim(app, "client1", f"d{n}")
        theirs = [_ds("client2", f"b{n}") for n in range(10)]
        assert list(pending_by_client(app.app)["client2"]) == theirs
        assert all(name in pending_configs(app.app) for name in theirs)
        self._in_step(app)

    def test_a_client_at_its_cap_cannot_push_another_one_out(self, app):
        """The old behaviour: one global LRU of 4096 let a single client evict everybody."""
        for n in range(5):
            self._claim(app, "client2", f"b{n}")
        for n in range(1000):
            self._claim(app, "client1", f"d{n}")
        assert len(pending_by_client(app.app)["client2"]) == 5
        assert len(pending_configs(app.app)) == 256 + 5

    def test_the_cap_is_the_patched_value(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        for n in range(5):
            self._claim(app, "client1", f"d{n}")
        assert list(pending_by_client(app.app)["client1"]) == [_ds("client1", f"d{n}") for n in (2, 3, 4)]

    def test_the_oldest_is_evicted_first(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        for name in "abc":
            self._claim(app, "client1", name)
        self._claim(app, "client1", "d")
        assert list(pending_by_client(app.app)["client1"]) == [_ds("client1", n) for n in "bcd"]
        assert _ds("client1", "a") not in pending_configs(app.app)
        assert _ds("client1", "a") not in pending_deadlines(app.app)

    def test_renegotiating_refreshes_an_entry_so_it_is_evicted_last(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        for name in "abc":
            self._claim(app, "client1", name)
        self._claim(app, "client1", "a")                        # now the newest
        self._claim(app, "client1", "d")                        # evicts "b"
        assert list(pending_by_client(app.app)["client1"]) == [_ds("client1", n) for n in "cad"]

    def test_refreshing_an_entry_at_the_cap_evicts_nothing(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        for name in "abc":
            self._claim(app, "client1", name)
        for _ in range(5):
            assert self._claim(app, "client1", "b") is None
        assert sorted(pending_by_client(app.app)["client1"]) == sorted(_ds("client1", n) for n in "abc")

    def test_an_expired_entry_goes_before_an_older_live_one(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        for name in "abc":
            self._claim(app, "client1", name)
        pending_deadlines(app.app)[_ds("client1", "b")] = 0.0   # long expired
        self._claim(app, "client1", "d")
        assert list(pending_by_client(app.app)["client1"]) == [_ds("client1", n) for n in "acd"]
        self._in_step(app)

    def test_an_ancestor_of_another_pending_name_is_not_evicted(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        self._claim(app, "client1", "p")                        # the oldest, and a parent of ...
        self._claim(app, "client1", "p/x")
        self._claim(app, "client1", "y")
        assert self._claim(app, "client1", "z") is None
        assert list(pending_by_client(app.app)["client1"]) == [
            _ds("client1", n) for n in ("p", "y", "z")]         # "p/x" went, not "p"

    def test_an_ancestor_of_a_claimed_name_is_not_evicted(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        self._claim(app, "client1", "p")
        self._claim(app, "client1", "q")
        streams_in_progress_set = _find_streams(app)
        streams_in_progress_set.add(_ds("client1", "p/child"))  # a stream is working below "p"
        try:
            assert self._claim(app, "client1", "z") is None
        finally:
            streams_in_progress_set.discard(_ds("client1", "p/child"))
        assert list(pending_by_client(app.app)["client1"]) == [_ds("client1", n) for n in ("p", "z")]

    def test_an_ancestor_of_the_new_name_is_not_evicted(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        self._claim(app, "client1", "p")
        self._claim(app, "client1", "w")
        assert self._claim(app, "client1", "p/new") is None
        assert list(pending_by_client(app.app)["client1"]) == [_ds("client1", n) for n in ("p", "p/new")]

    def test_when_every_entry_is_needed_the_new_name_is_refused_and_nothing_changes(
        self, app, mocker
    ):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        self._claim(app, "client1", "p")
        self._claim(app, "client1", "p/q")
        before = dict(pending_configs(app.app))
        assert self._claim(app, "client1", "p/q/r") is api.PENDING_FULL
        assert dict(pending_configs(app.app)) == before
        assert _ds("client1", "p/q/r") not in pending_deadlines(app.app)
        self._in_step(app)

    def test_a_refused_client_does_not_touch_another_clients_entries(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        self._claim(app, "client2", "b")
        self._claim(app, "client1", "p")
        self._claim(app, "client1", "p/q")
        assert self._claim(app, "client1", "p/q/r") is api.PENDING_FULL
        assert list(pending_by_client(app.app)["client2"]) == [_ds("client2", "b")]

    def test_the_rival_check_still_comes_first(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 1)
        claim = pending_api(app.app)[0]
        assert claim(_ds("client1", "a"), "cfg", "pool/one") is None
        assert claim(_ds("client1", "a"), "cfg", "pool/other") == "pool/one"
        assert list(pending_by_client(app.app)["client1"]) == [_ds("client1", "a")]

    # -- forgetting, expiry and the global backstop ------------------------------------------------

    def test_forgetting_and_unclaiming_keep_the_maps_in_step(self, app):
        claim, lookup, forget = pending_api(app.app)
        for n in range(4):
            claim(_ds("client1", f"d{n}"), "cfg", f"pool/d{n}")
        forget(_ds("client1", "d1"))
        self._in_step(app)
        for n in (0, 2, 3):
            forget(_ds("client1", f"d{n}"))
        assert pending_by_client(app.app) == {}                   # the empty owner is dropped
        self._in_step(app)

    def test_an_expired_read_drops_the_owner_entry_too(self, app, mocker):
        clock = patch_api_clock(mocker)
        claim, lookup, forget = pending_api(app.app)
        claim(_ds("client1", "a"), "cfg", "pool/a")
        clock.advance(api.PENDING_CONFIG_TTL + 1)
        assert lookup(_ds("client1", "a")) is None
        assert pending_by_client(app.app) == {}
        self._in_step(app)

    def test_the_global_cap_is_still_a_backstop(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_MAX", 5)
        for client in ("client1", "client2", "client3"):
            for n in range(3):
                self._claim(app, client, f"d{n}")
        assert len(pending_configs(app.app)) == 5
        self._in_step(app)
        # the oldest overall went: client1's first four, client2's first
        assert _ds("client1", "d0") not in pending_configs(app.app)
        assert _ds("client3", "d2") in pending_configs(app.app)

    def test_the_global_cap_exceeds_the_per_client_cap(self):
        assert api.PENDING_CONFIGS_PER_CLIENT * 2 <= api.PENDING_CONFIGS_MAX

    def test_names_outside_the_target_have_a_shared_empty_owner(self, app):
        claim = pending_api(app.app)[0]
        assert claim("other/pool/x", "cfg", "pool/x") is None
        self._in_step_loose(app)

    def _in_step_loose(self, app):
        owned = [n for per in pending_by_client(app.app).values() for n in per]
        assert sorted(owned) == sorted(pending_configs(app.app))

    # -- a refused child keeps its pending parent ------------------------------------------------------------

    def _negotiate_as(self, app, dataset, client_id=CLIENT_ID, who=None):
        client = app.client(who) if who is not None else app.client()
        return self._negotiate(client, client_id, dataset)

    def test_a_child_refused_for_a_pending_parent_refreshes_the_parent(self, app, mocker, zfs):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        assert self._negotiate_as(app, "pool/p").status_code == 200       # P pending, the oldest
        assert self._negotiate_as(app, "pool/q").status_code == 200
        resp = self._negotiate_as(app, "pool/p/c")
        assert resp.status_code == 503 and resp.get_json()["reason"] == "unsettled_ancestor"
        assert list(pending_by_client(app.app)[CLIENT_ID]) == [
            _ds(CLIENT_ID, "q"), _ds(CLIENT_ID, "p")]                      # P is the newest now
        assert _ds(CLIENT_ID, "p") in pending_needed(app.app)
        assert self._negotiate_as(app, "pool/r").status_code == 200       # evicts q, not p
        assert list(pending_by_client(app.app)[CLIENT_ID]) == [
            _ds(CLIENT_ID, "p"), _ds(CLIENT_ID, "r")]
        self._in_step(app)

    def test_without_the_refused_child_the_oldest_parent_goes(self, app, mocker, zfs):
        """The control: the same sequence without the child's retry evicts P."""
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        self._negotiate_as(app, "pool/p")
        self._negotiate_as(app, "pool/q")
        self._negotiate_as(app, "pool/r")
        assert list(pending_by_client(app.app)[CLIENT_ID]) == [
            _ds(CLIENT_ID, "q"), _ds(CLIENT_ID, "r")]

    def test_a_marked_entry_is_never_evicted_while_the_mark_lives(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        self._claim(app, CLIENT_ID, "p")
        self._claim(app, CLIENT_ID, "q")
        pending_needed(app.app)[_ds(CLIENT_ID, "p")] = time.monotonic() + 100
        pending_needed(app.app)[_ds(CLIENT_ID, "q")] = time.monotonic() + 100
        before = dict(pending_configs(app.app))
        assert self._claim(app, CLIENT_ID, "z") is api.PENDING_FULL
        assert dict(pending_configs(app.app)) == before

    def test_an_expired_mark_no_longer_protects(self, app, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        self._claim(app, CLIENT_ID, "p")
        self._claim(app, CLIENT_ID, "q")
        pending_needed(app.app)[_ds(CLIENT_ID, "p")] = time.monotonic() - 1
        assert self._claim(app, CLIENT_ID, "z") is None
        assert _ds(CLIENT_ID, "p") not in pending_configs(app.app)
        assert _ds(CLIENT_ID, "p") not in pending_needed(app.app)    # the mark went with the entry

    def test_the_mark_lapses_after_the_pending_ttl(self, app, mocker, zfs):
        clock = patch_api_clock(mocker)
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        self._negotiate_as(app, "pool/p")
        self._negotiate_as(app, "pool/p/c")                                # refused, marks p
        mark = pending_needed(app.app)[_ds(CLIENT_ID, "p")]
        assert mark == pytest.approx(clock.now + api.PENDING_CONFIG_TTL)
        clock.advance(api.PENDING_CONFIG_TTL - 1)
        self._negotiate_as(app, "pool/p/c")                                # a retry refreshes the mark
        assert pending_needed(app.app)[_ds(CLIENT_ID, "p")] > mark

    def test_marking_does_nothing_for_an_entry_that_is_gone(self, app, mocker, zfs):
        self._negotiate_as(app, "pool/p")
        pending_api(app.app)[2](_ds(CLIENT_ID, "p"))                      # the first receive finished
        mark = _find_in_closures(app.app, "_mark_pending_needed")
        mark(_ds(CLIENT_ID, "p"))
        assert pending_needed(app.app) == {}

    def test_a_claimed_parent_is_not_marked(self, app, zfs):
        """A parent that is busy (claimed) is not 'pending': nothing to keep."""
        streams = _find_streams(app)
        streams.add(_ds(CLIENT_ID, "p"))
        try:
            resp = self._negotiate_as(app, "pool/p/c")
        finally:
            streams.discard(_ds(CLIENT_ID, "p"))
        assert resp.status_code == 503
        assert pending_needed(app.app) == {}

    # -- the warning ---------------------------------------------------------------------------------

    def test_the_eviction_is_one_warning_per_client_and_hour(self, app, mocker, caplog):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for n in range(10):
                self._claim(app, "client1", f"d{n}")
            for n in range(5):
                self._claim(app, "client2", f"d{n}")
        warnings = _peer_records(caplog, levels=("WARNING",), text="negotiated and not yet received")
        assert len(warnings) == 2
        assert "client client1 has more than 3 datasets" in warnings[0].getMessage()
        assert "client client2" in warnings[1].getMessage()
        assert "other clients are not affected" in warnings[0].getMessage()

    def test_the_warning_comes_back_after_an_hour(self, app, mocker, caplog, clock):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 1)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            self._claim(app, "client1", "a")
            self._claim(app, "client1", "b")
            clock.advance(3600)
            self._claim(app, "client1", "c")
        assert len(_peer_records(caplog, levels=("WARNING",))) == 2

    def test_a_refusal_is_a_warning_naming_the_remedy(self, app, mocker, caplog):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 2)
        self._claim(app, "client1", "p")
        self._claim(app, "client1", "p/q")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            assert self._claim(app, "client1", "p/q/r") is api.PENDING_FULL
        (record,) = _peer_records(caplog, levels=("WARNING",))
        assert "refusing another negotiate (503, retry)" in record.getMessage()

    def test_no_eviction_means_no_warning(self, app, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup"):
            for n in range(256):
                self._claim(app, "client1", f"d{n}")
        assert _peer_records(caplog) == []

    # -- through the route -----------------------------------------------------------------------------

    @staticmethod
    def _negotiate(client, client_id, dataset):
        import base64

        config = base64.b64encode(json.dumps({
            "name": dataset, "recursive": False, "frequency": 3600.0, "enabled": True,
            "retention_rules": [{"age": 86400.0, "keep_for": 2592000.0}],
            "remote": [{"destination": "offsite", "frequency": None}],
        }).encode()).decode()
        return client.post(f"/backup/{client_id}/{dataset}/negotiate",
                           json={"snapshots": [], "config": config})

    def test_negotiate_evicts_only_the_callers_own_oldest(self, app, mocker, zfs):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_PER_CLIENT", 3)
        other = app.cert_for("client2")
        mine, theirs = app.client(), app.client(other)
        assert self._negotiate(theirs, "client2", "pool/t0").status_code == 200
        for n in range(5):
            assert self._negotiate(mine, CLIENT_ID, f"pool/d{n}").status_code == 200
        assert list(pending_by_client(app.app)[CLIENT_ID]) == [
            _ds(CLIENT_ID, f"d{n}") for n in (2, 3, 4)]
        assert list(pending_by_client(app.app)["client2"]) == [_ds("client2", "t0")]
        self._in_step(app)


def _find_streams(app):
    from _api_support import streams_in_progress

    return streams_in_progress(app.app)


# ===========================================================================
# ApiServer.process_request: the accept-time limits
# ===========================================================================


class Feeder:
    """Feeds connections with ARBITRARY peer addresses to a server that is not serving
    (`process_request` over a socketpair), like `TestHandshakeValidityWarning._feed`; the
    client ends are kept so a held connection stays silent until `close`."""

    def __init__(self, server):
        self.server = server
        self.clients = []

    def open(self, address):
        server_side, client_side = socket.socketpair()
        client_side.settimeout(IO_TIMEOUT)
        self.server.process_request(server_side, address)
        self.clients.append(client_side)
        return client_side

    @staticmethod
    def dropped(sock, within=3.0) -> bool:
        """Whether the server closed `sock` without sending a single byte."""
        sock.settimeout(within)
        try:
            return sock.recv(1) == b""
        except socket.timeout:
            return False
        except OSError:
            return True

    def close(self):
        for sock in self.clients:
            sock.close()
        self.clients = []


@pytest.fixture
def feed_server(tmp_path):
    """-> `make(limits=None, **server_kwargs) -> (server, Feeder)`; everything is closed and
    the servers closed at teardown."""
    made = []

    def make(limits=None, **kwargs):
        gateway, _ident, trust = make_gateway(tmp_path / f"srv{len(made)}.tls")
        cls = workers._api_server_class()
        kwargs.setdefault("handshake_timeout", 20.0)
        server = cls(LOOPBACK, 0, echo_app_flood, gateway,
                     limits=limits if limits is not None else PeerLimits(), **kwargs)
        feeder = Feeder(server)
        made.append((server, feeder))
        server.gateway_trust = trust
        return server, feeder

    yield make
    for server, feeder in made:
        feeder.close()
        _eventually(lambda s=server: s._slots._value == s.max_connections, 15)
        server.server_close()


def echo_app_flood(environ, start_response):
    body = b"ok"
    start_response("200 OK", [("Content-Length", str(len(body)))])
    return [body]


def _unlimited_rate(**kw):
    kw.setdefault("burst", 10 ** 9)
    kw.setdefault("rate", 1e9)
    kw.setdefault("max_per_peer", 10 ** 6)
    kw.setdefault("max_per_vouched", 10 ** 6)
    kw.setdefault("reserve", 0)
    return PeerRateLimiter(**kw)


@pytest.mark.integration
class TestAcceptTimeLimits:
    A = ("198.51.100.1", 40000)
    B = ("198.51.100.2", 40000)

    def test_over_the_bucket_a_connection_is_closed_before_a_tls_byte(self, feed_server):
        rate = _unlimited_rate(burst=3, rate=0.0)
        server, feeder = feed_server(PeerLimits(rate=rate))
        held = [feeder.open(self.A) for _ in range(3)]
        over = [feeder.open(self.A) for _ in range(5)]
        assert all(feeder.dropped(c) for c in over)
        assert not any(feeder.dropped(c, 0.2) for c in held)       # the admitted are waiting
        assert rate._active == {"198.51.100.1": 3}
        assert server._slots._value == server.max_connections - 3  # nothing was taken for the dropped

    def test_over_the_concurrency_cap_a_ninth_connection_is_dropped(self, feed_server):
        server, feeder = feed_server()
        held = [feeder.open(self.A) for _ in range(8)]
        ninth = feeder.open(self.A)
        assert feeder.dropped(ninth)
        assert not any(feeder.dropped(c, 0.1) for c in held)
        assert server.limits.rate._active == {"198.51.100.1": 8}

    def test_another_peer_is_unaffected_by_a_flooding_one(self, feed_server):
        server, feeder = feed_server()
        for _ in range(8):
            feeder.open(self.A)
        assert all(feeder.dropped(feeder.open(self.A)) for _ in range(20))
        other = feeder.open(self.B)
        assert not feeder.dropped(other, 0.3)
        assert server.limits.rate._active == {"198.51.100.1": 8, "198.51.100.2": 1}

    def test_a_closed_connection_gives_its_peer_slot_back(self, feed_server):
        server, feeder = feed_server()
        held = [feeder.open(self.A) for _ in range(8)]
        assert feeder.dropped(feeder.open(self.A))
        held[0].close()                                            # the client hangs up mid-handshake
        assert _eventually(lambda: server.limits.rate._active.get("198.51.100.1") == 7, 15)
        again = feeder.open(self.A)
        assert not feeder.dropped(again, 0.3)

    def test_all_slots_come_back_when_every_client_leaves(self, feed_server):
        server, feeder = feed_server()
        for n in range(6):
            for _ in range(4):
                feeder.open((f"198.51.100.{n + 1}", 1))
        feeder.close()
        assert _eventually(lambda: server.limits.rate._active == {}, 20)
        assert server.limits.rate._active_total == 0
        assert _eventually(lambda: server._slots._value == server.max_connections, 20)

    def test_a_failed_handshake_gives_the_peer_slot_back(self, feed_server):
        server, feeder = feed_server(handshake_timeout=0.3, reject_linger=0.0)
        for _ in range(8):
            feeder.open(self.A)                                    # say nothing: dropped at 0.3 s
        assert _eventually(lambda: server.limits.rate._active == {}, 15)

    def test_the_peer_slot_is_released_when_the_global_cap_refuses(self, feed_server):
        limits = PeerLimits(rate=_unlimited_rate())
        server, feeder = feed_server(limits, max_connections=2)
        feeder.open(self.A)
        feeder.open(self.A)
        refused = feeder.open(self.B)                              # the global cap, not the peer's
        assert feeder.dropped(refused)
        assert limits.rate._active == {"198.51.100.1": 2}          # B kept nothing
        assert limits.rate._active_total == 2

    def test_the_peer_and_global_slots_are_released_when_the_thread_cannot_start(
        self, feed_server, mocker
    ):
        import socketserver

        limits = PeerLimits(rate=_unlimited_rate())
        server, feeder = feed_server(limits)
        mocker.patch.object(socketserver.ThreadingMixIn, "process_request",
                            side_effect=RuntimeError("can't start new thread"))
        server_side, client_side = socket.socketpair()
        try:
            with pytest.raises(RuntimeError):
                server.process_request(server_side, self.A)
        finally:
            server_side.close()
            client_side.close()
        assert limits.rate._active == {} and limits.rate._active_total == 0
        assert server._slots._value == server.max_connections

    def test_ipv6_peers_of_one_slash_64_share_the_cap(self, feed_server):
        server, feeder = feed_server()
        for n in range(8):
            feeder.open((f"2001:db8:1:2::{n + 1:x}", 1, 0, 0))
        assert feeder.dropped(feeder.open(("2001:db8:1:2:ffff::1", 1, 0, 0)))
        assert not feeder.dropped(feeder.open(("2001:db8:1:3::1", 1, 0, 0)), 0.2)

    def test_a_mapped_ipv4_peer_counts_as_the_ipv4_address(self, feed_server):
        server, feeder = feed_server()
        for _ in range(8):
            feeder.open(("::ffff:198.51.100.1", 1, 0, 0))
        assert feeder.dropped(feeder.open(self.A))

    def test_a_malformed_address_is_one_shared_unknown_peer_not_a_crash(self, feed_server):
        server, feeder = feed_server()
        for _ in range(8):
            feeder.open(None)
        assert feeder.dropped(feeder.open(()))
        assert server.limits.rate._active == {"?": 8}

    # -- the reserve ----------------------------------------------------------------------------------

    def test_the_server_tells_the_limiter_its_capacity(self, feed_server):
        server, _ = feed_server()
        assert server.max_connections == 64 and server.limits.rate.reserve == 16
        server, _ = feed_server(max_connections=8)
        assert server.limits.rate.reserve == 2
        server, _ = feed_server(max_connections=4)
        assert server.limits.rate.reserve == 1
        server, _ = feed_server(max_connections=2)
        assert server.limits.rate.reserve == 0

    def test_anonymous_peers_from_many_addresses_cannot_take_the_reserve(self, feed_server):
        server, feeder = feed_server()
        clients = [feeder.open((f"203.0.113.{n}", 1)) for n in range(1, 61)]
        admitted = [c for c in clients if not feeder.dropped(c, 0.05)]
        assert len(admitted) == 48
        assert server.limits.rate._active_total == 48
        assert server._slots._value == 64 - 48

    def test_a_vouched_peer_still_gets_in_when_the_anonymous_pool_is_full(self, feed_server):
        limits = PeerLimits()
        server, feeder = feed_server(limits)
        limits.rate.vouch("198.51.100.77")
        limits.rate.unvouch("198.51.100.77")
        for n in range(1, 61):
            feeder.open((f"203.0.113.{n}", 1))
        assert not feeder.dropped(feeder.open(("198.51.100.77", 1)), 0.3)
        assert limits.rate._active["198.51.100.77"] == 1

    def test_the_reserve_drop_is_logged_once_not_per_connection(self, feed_server, caplog):
        server, feeder = feed_server()
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            for n in range(1, 49):
                feeder.open((f"203.0.113.{n}", 1))
            for n in range(100, 140):
                assert feeder.dropped(feeder.open((f"203.0.113.{n}", 1)))
        records = _peer_records(caplog, text="kept for peers with a pinned client certificate")
        assert len(records) == 40                                  # one per (distinct) peer ...
        assert {r.levelname for r in records} == {"WARNING"}
        assert _peer_records(caplog, levels=("DEBUG",)) == []      # ... and no DEBUG line per drop

    # -- logging ----------------------------------------------------------------------------------------

    def test_a_rate_drop_is_one_warning_per_peer_and_nothing_else(self, feed_server, caplog):
        rate = _unlimited_rate(burst=2, rate=0.0)
        server, feeder = feed_server(PeerLimits(rate=rate))
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            for _ in range(2):
                feeder.open(self.A)
            for _ in range(30):
                assert feeder.dropped(feeder.open(self.A))
        (record,) = _peer_records(caplog)                          # no DEBUG per dropped connection
        assert record.levelname == "WARNING"
        assert "dropping connections from 198.51.100.1" in record.getMessage()
        assert "more than 2 at once" in record.getMessage()

    def test_a_concurrency_drop_is_one_warning_per_peer_and_nothing_else(self, feed_server, caplog):
        server, feeder = feed_server()
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            for _ in range(8):
                feeder.open(self.A)
            for _ in range(30):
                assert feeder.dropped(feeder.open(self.A))
            assert feeder.dropped(feeder.open(self.A))
        (record,) = _peer_records(caplog)
        assert "maximum of simultaneous connections" in record.getMessage()
        assert record.levelname == "WARNING"

    def test_suppressed_drops_are_counted_for_the_hourly_summary(self, feed_server):
        clock = Clock()
        rate = _unlimited_rate(burst=1, rate=0.0, clock=clock)
        limits = PeerLimits(clock=clock, rate=rate)
        server, feeder = feed_server(limits)
        feeder.open(self.A)
        for _ in range(10):
            assert feeder.dropped(feeder.open(self.A))
        clock.advance(3600)
        assert "rate 9" in limits.log.take_summary()

    def test_a_flood_of_drops_cannot_use_the_security_log_budget(self, feed_server):
        clock = Clock()
        rate = _unlimited_rate(burst=0, rate=0.0, clock=clock)
        limits = PeerLimits(clock=clock, rate=rate)
        server, feeder = feed_server(limits)
        for n in range(300):
            server.process_request(socket.socketpair()[0], (f"203.0.113.{n % 250}", n))
        for kind in ("auth", "enroll", "protocol", "tls-validity", "rotate", "pending",
                     "enroll-locked"):
            assert limits.log.allow(kind, "victim") is True, kind

    def test_a_logging_failure_on_the_accept_thread_does_not_leak_or_raise(
        self, feed_server, mocker
    ):
        rate = _unlimited_rate(burst=1, rate=0.0)
        limits = PeerLimits(rate=rate)
        server, feeder = feed_server(limits)
        mocker.patch.object(limits.log, "emit", side_effect=RuntimeError("log broke"))
        feeder.open(self.A)
        assert feeder.dropped(feeder.open(self.A))                 # still dropped, no exception
        assert rate._active == {"198.51.100.1": 1}

    # -- locks -----------------------------------------------------------------------------------------

    def test_the_accept_thread_does_not_wait_for_the_limiter_lock_holders_logging(
        self, feed_server
    ):
        """The limiter's lock is held only for table updates: a thread that holds the THROTTLE's
        lock (it is logging) never blocks the bucket check."""
        server, feeder = feed_server()
        with server.limits.log._guard:
            assert not feeder.dropped(feeder.open(self.A), 0.2)


@pytest.fixture
def pki(tmp_path):
    gateway, server_ident, trust = make_gateway(tmp_path / "srv.tls")
    alice = client_identity(tmp_path / "alice.tls")
    mallory = client_identity(tmp_path / "mallory.tls")
    trust.set_pins([trusted_pin(alice, "alice")], generation=trust.generation + 1)
    gateway.refresh()
    return SimpleNamespace(gateway=gateway, alice=alice, mallory=mallory, trust=trust,
                           server=server_ident)


class _Serving:
    def __init__(self, app, gateway, **kwargs):
        cls = workers._api_server_class()
        self.server = cls(LOOPBACK, 0, app, gateway, **kwargs)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.05}, daemon=True)
        self.port = self.server.server_port
        self.limits = self.server.limits

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.thread.join(10)
        self.server.server_close()
        assert not self.thread.is_alive()


@pytest.mark.integration
class TestPinnedHandshakeVouches:
    def _hold_pinned(self, port, ident):
        raw = socket.create_connection((LOOPBACK, port), timeout=IO_TIMEOUT)
        return insecure_client_context(ident).wrap_socket(raw)

    def test_a_pinned_connection_is_counted_open_and_then_starts_the_ttl(self, pki):
        with _Serving(echo_app_flood, pki.gateway, limits=PeerLimits()) as s:
            tls = self._hold_pinned(s.port, pki.alice)
            try:
                assert _eventually(lambda: s.limits.rate._pinned_open.get("127.0.0.1") == 1, 10)
                assert s.limits.rate.is_vouched("127.0.0.1")
            finally:
                tls.close()
            assert _eventually(lambda: s.limits.rate._pinned_open == {}, 10)
            assert s.limits.rate.is_vouched("127.0.0.1")           # for the TTL now
            assert "127.0.0.1" in s.limits.rate._vouched

    def test_requests_over_pinned_connections_vouch_and_unvouch_in_balance(self, pki):
        with _Serving(echo_app_flood, pki.gateway, limits=PeerLimits()) as s:
            for _ in range(5):
                assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200
            assert _eventually(lambda: s.limits.rate._pinned_open == {} and
                               s.limits.rate._active == {}, 10)

    def test_an_anonymous_connection_does_not_vouch(self, pki):
        with _Serving(echo_app_flood, pki.gateway, limits=PeerLimits()) as s:
            assert https_get(s.port, "/")[0] == 200
            assert _eventually(lambda: s.limits.rate._active == {}, 10)
            assert not s.limits.rate.is_vouched("127.0.0.1") and s.limits.rate._pinned_open == {}

    def test_an_unpinned_certificate_does_not_vouch(self, pki):
        with _Serving(echo_app_flood, pki.gateway, limits=PeerLimits()) as s:
            assert isinstance(try_get(s.port, "/", insecure_client_context(pki.mallory)), Exception)
            assert _eventually(lambda: s.limits.rate._active == {}, 10)
            assert not s.limits.rate.is_vouched("127.0.0.1")

    def test_a_failed_handshake_does_not_vouch(self, pki):
        with _Serving(echo_app_flood, pki.gateway, limits=PeerLimits()) as s:
            with raw_connect(s.port) as sock:
                sock.sendall(b"GET / HTTP/1.1\r\n\r\n")
                sock.settimeout(IO_TIMEOUT)
                try:
                    while sock.recv(4096):
                        pass
                except OSError:
                    pass
            assert _eventually(lambda: s.limits.rate._active == {}, 10)
            assert not s.limits.rate.is_vouched("127.0.0.1")

    def test_a_vouched_peer_is_not_rate_limited_until_the_vouch_runs_out(self, pki):
        """The same address: after a pinned connection the bucket does not apply to anybody
        there; an hour after the last pinned connection ended it does again."""
        clock = Clock()
        rate = _unlimited_rate(burst=2, rate=0.0, max_per_peer=8, max_per_vouched=32, clock=clock)
        limits = PeerLimits(clock=clock, rate=rate)
        with _Serving(echo_app_flood, pki.gateway, limits=limits) as s:
            assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200   # token 1 of 2
            assert _eventually(lambda: rate._pinned_open == {}, 10)
            for _ in range(6):
                assert https_get(s.port, "/")[0] == 200            # vouched: no bucket
            clock.advance(api.API_VOUCH_TTL)
            assert https_get(s.port, "/")[0] == 200                # token 2 of 2
            assert isinstance(try_get(s.port, "/"), Exception)     # dropped at accept


# ===========================================================================
# Real sockets: a real server on 127.0.0.1
# ===========================================================================


def _tls_request(port, method, path, *, body=None, ctx=None, headers=None, timeout=IO_TIMEOUT):
    """-> `(status, headers, parsed JSON or raw bytes)`. Sends the wire protocol header."""
    from zfsbackup.protocol import protocol_headers

    conn = http.client.HTTPSConnection(LOOPBACK, port, timeout=timeout,
                                       context=ctx or insecure_client_context())
    try:
        raw = None if body is None else json.dumps(body)
        hdrs = {**protocol_headers(), **(headers or {})}
        if raw is not None:
            hdrs["Content-Type"] = "application/json"
        conn.request(method, path, body=raw, headers=hdrs)
        resp = conn.getresponse()
        data = resp.read()
        try:
            data = json.loads(data)
        except ValueError:
            pass
        return resp.status, dict(resp.getheaders()), data
    finally:
        conn.close()


def _junk_enroll(port):
    return _tls_request(port, "POST", "/enroll", body={"version": 1})


def _closed(sock, within=0.05) -> bool:
    sock.settimeout(within)
    try:
        return sock.recv(1) == b""
    except socket.timeout:
        return False
    except OSError:
        return True


class CountingRate(PeerRateLimiter):
    """A limiter that remembers every answer `admit` gave."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.answers = collections.Counter()

    def admit(self, key):
        answer = super().admit(key)
        self.answers[answer] += 1
        return answer


def _second_loopback_address():
    """127.0.0.2 as a SOURCE address, or None where the platform cannot bind it (macOS)."""
    try:
        with socket.socket() as s:
            s.bind(("127.0.0.2", 0))
        return "127.0.0.2"
    except OSError:
        return None


@pytest.mark.integration
class TestRealSockets:
    def test_a_silent_flood_from_one_address_holds_at_most_eight_connections(self, make_server):
        srv = make_server("silent")
        limits = srv._server.limits
        socks = []
        try:
            for _ in range(100):
                s = socket.socket()
                s.settimeout(IO_TIMEOUT)
                try:
                    s.connect((LOOPBACK, srv.port))
                except OSError:
                    s.close()
                    continue
                socks.append(s)
            assert len(socks) >= 60
            assert _eventually(lambda: sum(_closed(s) for s in socks) >= len(socks) - 8, 20)
            open_ = [s for s in socks if not _closed(s)]
            assert len(open_) <= 8
            assert limits.rate._active.get("127.0.0.1", 0) <= 8
            assert srv._server._slots._value >= srv._server.max_connections - 8
        finally:
            for s in socks:
                s.close()
        assert _eventually(lambda: limits.rate._active == {}, 20)

    def test_dropped_connections_got_no_tls_bytes(self, make_server):
        srv = make_server("nobytes")
        held = [raw_connect(srv.port) for _ in range(8)]
        try:
            assert _eventually(lambda: srv._server.limits.rate._active.get("127.0.0.1") == 8, 10)
            extra = raw_connect(srv.port)
            try:
                extra.settimeout(IO_TIMEOUT)
                try:
                    assert extra.recv(100) == b""             # no alert, no bytes: just closed
                except ConnectionResetError:
                    pass
            finally:
                extra.close()
        finally:
            for s in held:
                s.close()

    def test_a_tls_client_of_a_flooded_address_fails_fast_instead_of_hanging(self, make_server):
        srv = make_server("fast")
        held = [raw_connect(srv.port) for _ in range(8)]
        try:
            assert _eventually(lambda: srv._server.limits.rate._active.get("127.0.0.1") == 8, 10)
            started = time.monotonic()
            result = try_get(srv.port, "/health", timeout=10)
            assert isinstance(result, Exception)
            assert time.monotonic() - started < 5
        finally:
            for s in held:
                s.close()
        assert _eventually(lambda: not isinstance(try_get(srv.port, "/health"), Exception), 20)

    def test_a_fast_connect_flood_hits_the_bucket(self, make_server, caplog):
        rate = CountingRate(max_per_peer=10 ** 6, reserve=0)
        limits = PeerLimits(rate=rate)
        srv = make_server("bucket", peer_limits=limits)
        n = 300
        started = time.monotonic()
        made = 0
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            for _ in range(n):
                try:
                    s = socket.create_connection((LOOPBACK, srv.port), timeout=IO_TIMEOUT)
                except OSError:
                    continue
                made += 1
                s.close()
            elapsed = time.monotonic() - started
            assert _eventually(lambda: sum(rate.answers.values()) >= made, 30)
        admitted = rate.answers[None]
        assert admitted >= 60
        assert admitted <= 60 + 10 * elapsed + 3
        assert rate.answers[ADMIT_RATE] == made - admitted and rate.answers[ADMIT_RATE] > 0
        lines = _peer_records(caplog, text="dropping connections from 127.0.0.1")
        assert len(lines) == 1 and lines[0].levelname == "WARNING"
        assert _eventually(lambda: rate._active == {}, 20)

    def test_a_vouched_address_is_not_rate_limited_by_a_connect_flood(self, make_server):
        rate = CountingRate(max_per_peer=10 ** 6, reserve=0)
        srv = make_server("vouched-flood", peer_limits=PeerLimits(rate=rate))
        good = client_identity(srv.trust.resolved.resolved_path.parent / "good.tls")
        pin_client(srv.trust.resolved, good, "good")
        srv.refresh()
        ctx = insecure_client_context(good)
        assert _tls_request(srv.port, "GET", "/whoami", ctx=ctx)[0] == 200
        before = rate.answers[ADMIT_RATE]
        made = 0
        for _ in range(300):
            try:
                socket.create_connection((LOOPBACK, srv.port), timeout=IO_TIMEOUT).close()
                made += 1
            except OSError:
                pass
        assert _eventually(lambda: sum(rate.answers.values()) >= made + 1, 30)
        assert rate.answers[ADMIT_RATE] == before == 0
        assert _tls_request(srv.port, "GET", "/whoami", ctx=ctx)[0] == 200

    def test_an_already_vouched_client_gets_through_while_other_addresses_flood(self, make_server):
        srv = make_server("others-flood")
        server = srv._server
        good = client_identity(srv.trust.resolved.resolved_path.parent / "good.tls")
        pin_client(srv.trust.resolved, good, "good")
        srv.refresh()
        ctx = insecure_client_context(good)
        assert _tls_request(srv.port, "GET", "/whoami", ctx=ctx)[0] == 200      # vouches 127.0.0.1
        feeder = Feeder(server)
        try:
            clients = [feeder.open((f"203.0.113.{n}", 1)) for n in range(1, 101)]
            assert _eventually(
                lambda: server.limits.rate._active_total >= 48, 10)
            assert server.limits.rate._active_total <= 48 + 1
            # the anonymous pool is full: a vouched address still gets served
            assert _tls_request(srv.port, "GET", "/whoami", ctx=ctx)[0] == 200
            assert sum(Feeder.dropped(c, 0.05) for c in clients) >= 52
        finally:
            feeder.close()

    def test_a_flood_from_a_second_source_address_leaves_the_first_alone(self, make_server):
        second = _second_loopback_address()
        if second is None:
            pytest.skip("this platform cannot use 127.0.0.2 as a source address")
        srv = make_server("two-sources")
        flooders = []
        try:
            for _ in range(40):
                s = socket.socket()
                s.settimeout(IO_TIMEOUT)
                s.bind((second, 0))
                try:
                    s.connect((LOOPBACK, srv.port))
                except OSError:
                    s.close()
                    continue
                flooders.append(s)
            limits = srv._server.limits
            assert _eventually(lambda: limits.rate._active.get(second, 0) == 8, 10)
            assert limits.rate._active.get(second, 0) == 8
            assert _tls_request(srv.port, "GET", "/health")[0] == 200       # 127.0.0.1 is untouched
        finally:
            for s in flooders:
                s.close()

    # -- /enroll over TLS ------------------------------------------------------------------------------

    def test_a_real_429_with_retry_after_after_five_failures(self, make_server):
        srv = make_server("real-429")
        assert [_junk_enroll(srv.port)[0] for _ in range(5)] == [400] * 5
        status, headers, body = _junk_enroll(srv.port)
        assert status == 429
        assert headers["Retry-After"] in ("900", "899")
        assert body["reason"] == "rate_limited" and body["retry_after"] in (900, 899)
        assert _tls_request(srv.port, "GET", "/health")[0] == 200

    def test_a_pinned_client_is_served_during_a_lockout(self, make_server):
        srv = make_server("lockout-pinned")
        good = client_identity(srv.trust.resolved.resolved_path.parent / "good.tls")
        pin_client(srv.trust.resolved, good, "good")
        srv.refresh()
        for _ in range(6):
            _junk_enroll(srv.port)
        assert _junk_enroll(srv.port)[0] == 429
        assert _tls_request(srv.port, "GET", "/whoami", ctx=insecure_client_context(good))[0] == 200

    def test_a_valid_enrollment_after_failures_below_the_limit_still_works(self, make_server):
        srv = make_server("enroll-ok")
        for _ in range(4):
            assert _junk_enroll(srv.port)[0] == 400
        token = srv.token()
        body = enroll_body(srv.fp, "host-z", make_client_cert(), token)
        assert _tls_request(srv.port, "POST", "/enroll", body=body)[0] == 200
        for _ in range(4):
            assert _junk_enroll(srv.port)[0] == 400          # the success forgot the four

    def test_stalled_bodies_hold_no_slot_and_count_towards_the_lockout(self, make_server):
        """The attack: connections that send headers and a Content-Length and then nothing."""
        srv = make_server("stalled", server_kwargs={"request_idle_timeout": 1.5}, record=False)
        limits = srv._server.limits
        ctx = insecure_client_context()

        def stalled():
            raw = socket.create_connection((LOOPBACK, srv.port), timeout=IO_TIMEOUT)
            tls = ctx.wrap_socket(raw)
            tls.sendall(
                b"POST /enroll HTTP/1.1\r\nHost: x\r\nX-Zfsbackup-Protocol: 1\r\n"
                b"Content-Type: application/json\r\nContent-Length: 100\r\n\r\n")
            return tls

        tls_conns = [stalled(), stalled()]
        try:
            time.sleep(0.4)                                   # both sit in the body read
            assert limits.enroll.in_flight == 0
            token = srv.token()
            body = enroll_body(srv.fp, "host-z", make_client_cert(), token)
            assert _tls_request(srv.port, "POST", "/enroll", body=body)[0] == 200   # not 503 busy
        finally:
            for t in tls_conns:
                t.close()
        # five stalls in all: the peer is locked out once they time out
        more = [stalled() for _ in range(5)]
        try:
            assert _eventually(lambda: limits.enroll.retry_after("127.0.0.1") > 0, 30)
        finally:
            for t in more:
                t.close()
        assert _junk_enroll(srv.port)[0] == 429

    def test_revocation_is_picked_up_in_a_few_seconds_during_an_enroll_flood(
        self, make_server, worker_config, mocker
    ):
        srv = make_server("revoke-flood")
        good = client_identity(srv.trust.resolved.resolved_path.parent / "good.tls")
        pin_client(srv.trust.resolved, good, "good")
        srv.refresh()
        ctx = insecure_client_context(good)
        worker = workers.ApiWorker(worker_config, StopFlag())
        worker._trust = srv.trust.gateway
        worker._peer_limits = srv.app.extensions["zfsbackup.peer_limits"]
        stopped = worker._start_trust_poll(logging.getLogger("flood-test"),
                                           srv.app.extensions["zfsbackup.sessions"])
        stop_flood = threading.Event()
        codes = collections.Counter()

        def flood():
            while not stop_flood.is_set():
                try:
                    codes[_junk_enroll(srv.port)[0]] += 1
                except Exception as exc:                      # noqa: BLE001
                    codes[type(exc).__name__] += 1

        flooders = [threading.Thread(target=flood, daemon=True) for _ in range(6)]
        try:
            assert _tls_request(srv.port, "GET", "/whoami", ctx=ctx)[0] == 200
            for t in flooders:
                t.start()
            time.sleep(0.5)
            for _ in range(5):
                assert _tls_request(srv.port, "GET", "/whoami", ctx=ctx)[0] == 200
            unpin_client(srv.trust.resolved, good)
            revoked_at = time.monotonic()
            deadline = revoked_at + 20
            status = 200
            while time.monotonic() < deadline:
                try:
                    status = _tls_request(srv.port, "GET", "/whoami", ctx=ctx, timeout=5)[0]
                except OSError:
                    status = "refused"
                if status != 200:
                    break
                time.sleep(0.1)
            took = time.monotonic() - revoked_at
            assert status != 200, f"the revoked client was still served after {took:.1f}s"
            assert took < 12, took            # one 2 s poll tick, plus generous slack
        finally:
            stop_flood.set()
            for t in flooders:
                t.join(30)
            stopped.set()
        assert codes[429] > 0                                  # the flood really was being refused
        assert limits_in_flight_zero(srv)


def limits_in_flight_zero(srv) -> bool:
    return _eventually(lambda: srv.app.extensions["zfsbackup.peer_limits"].enroll.in_flight == 0, 10)


# ===========================================================================
# The hourly summary comes from the trust-poll thread
# ===========================================================================


class _PollGateway:
    def __init__(self):
        self.polls = 0
        self.block = None

    def poll(self):
        self.polls += 1
        if self.block is not None:
            self.block.wait(30)
        return False


@pytest.mark.integration
class TestSummaryFromThePollThread:
    @pytest.fixture
    def worker(self, worker_config, mocker):
        mocker.patch.object(api, "TRUST_POLL_SECONDS", 0.03)
        w = workers.ApiWorker(worker_config, StopFlag())
        w._trust = _PollGateway()
        return w

    @staticmethod
    def _suppressed_limits(clock):
        limits = PeerLimits(clock=clock)
        for _ in range(4):
            limits.log.allow("enroll", "203.0.113.7")                  # 3 suppressed
        return limits

    def test_the_class_default_is_no_limits(self):
        assert workers.ApiWorker._peer_limits is None

    def test_the_summary_is_logged_at_warning_once_the_hour_is_over(self, worker, caplog):
        clock = Clock()
        worker._peer_limits = self._suppressed_limits(clock)
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            stopped = worker._start_trust_poll(logging.getLogger("t"))
            try:
                time.sleep(0.3)
                assert _peer_records(caplog, levels=("WARNING",)) == []     # not yet
                clock.advance(3600)
                assert _eventually(lambda: _peer_records(caplog, levels=("WARNING",)), 10)
                time.sleep(0.3)
            finally:
                stopped.set()
        (record,) = _peer_records(caplog, levels=("WARNING",))
        assert "3 refusal log line(s) suppressed in the last hour" in record.getMessage()
        assert "enroll 3" in record.getMessage()

    def test_without_limits_the_poll_is_unchanged(self, worker):
        assert worker._peer_limits is None
        stopped = worker._start_trust_poll(logging.getLogger("t"))
        try:
            assert _eventually(lambda: worker._trust.polls >= 3, 5)
        finally:
            stopped.set()

    def test_the_summary_does_not_wait_for_a_poll_that_never_returns(self, worker, caplog):
        """The flush runs at the top of the tick, before anything that can wait."""
        clock = Clock()
        worker._peer_limits = self._suppressed_limits(clock)
        clock.advance(3600)
        release = threading.Event()
        worker._trust.block = release
        with caplog.at_level(logging.DEBUG, logger=PEER_LOGGER):
            stopped = worker._start_trust_poll(logging.getLogger("t"))
            try:
                assert _eventually(lambda: worker._trust.polls >= 1, 5)
                assert _eventually(lambda: _peer_records(caplog, levels=("WARNING",)), 10)
            finally:
                release.set()
                stopped.set()

    def test_a_failing_flush_does_not_end_the_loop(self, worker):
        limits = PeerLimits()
        limits.log.take_summary = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
        worker._peer_limits = limits
        stopped = worker._start_trust_poll(logging.getLogger("t"))
        try:
            assert _eventually(lambda: worker._trust.polls >= 3, 5)
        finally:
            stopped.set()

    def test_serve_hands_the_apps_limits_to_the_poll_thread(self, worker_config, mocker):
        """`_serve` wires `app.extensions['zfsbackup.peer_limits']` into the worker before the
        poll thread starts."""
        stop = StopFlag()
        worker = workers.ApiWorker(worker_config, stop)
        worker._trust = SimpleNamespace(refresh=lambda **kw: True, restart_reason=None)
        seen = {}

        def fake_start(self_, logger, sessions=None):
            seen["limits"] = self_._peer_limits
            stop.set()
            return threading.Event()

        mocker.patch.object(workers.ApiWorker, "_start_trust_poll", fake_start)
        server = SimpleNamespace(
            serve_forever=lambda: stop.wait(10), shutdown=lambda: None, server_close=lambda: None,
            port=0,
        )
        mocker.patch.object(workers, "_bind_api_server", lambda *a, **k: server)
        made = {}

        def spy_create(*args, **kwargs):
            made["app"] = api.create_app(*args, **kwargs)
            return made["app"]

        worker._serve(worker_config, logging.getLogger("t"), spy_create,
                      SimpleNamespace(fingerprint=SERVER_FP))
        assert seen["limits"] is made["app"].extensions["zfsbackup.peer_limits"]
        assert isinstance(seen["limits"], PeerLimits)

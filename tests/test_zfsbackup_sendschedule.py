"""`zfsbackup.sendschedule.SendSchedule`: the pure scheduling core (Track B, item B2).

Everything here is pure: an injected clock or an explicit ``now``, no subprocess,
no thread, no real sleep. The rules under test are the ones the module's
docstrings state; the plan is ``docs/sender_rework_plan.md`` ("Decisions" and
"Track B -> Scheduling").

Conventions: ``T0`` is a UTC midnight (a multiple of a day, so a slot's wall
clock time is easy to read). With ``frac = 0.5``, ``check_interval = 300`` and
``F = P = 1 h`` the offset is 1980 s, so slots fall at ``T0 + n * H + 1980``.
A pair with ``last=None`` is new and unattempted, hence due by catch-up at
``now + frac * 300``; a pair with the default ``last=FRESH`` already ran in the
current slot, so its next due time is the next regular slot.
"""

import hashlib
import random
from datetime import datetime, timedelta, timezone

import pytest

from zfsbackup import sendschedule as ss
from zfsbackup.sendschedule import (
    CATCH_UP_CAP, DISCOVERY_MAX_INTERVAL, DISCOVERY_RETRY, FAILURE_RETRY_CAP, LATE_SLACK,
    MAX_DISCOVERIES, MAX_SENDERS, MAX_SENDERS_PER_DESTINATION, RETRY_DELAYS, WAIT_RECHECK,
    Outcome, PairKey, PairSpec, SendSchedule, pair_fraction, slot_index, slot_offset,
    slot_time,
)

pytestmark = pytest.mark.unit

M = 60
H = 3600
D = 86400
T0 = 20000 * D               # a UTC midnight
FRESH = T0 - 10              # on the 1 h grid below: inside the current slot, so not overdue
OFFSET = 1980                # frac 0.5, ci 300, W = 1 h
CI = 300                     # `make()`'s check interval
GAP = max(WAIT_RECHECK, CI)  # the re-arm spacing after the last start: max(WAIT_RECHECK, check_interval)


class Clock:
    """The injected clock."""

    def __init__(self, t=T0):
        self.t = t

    def __call__(self):
        return self.t


def make(frac=0.5, ci=300, fracs=None, **kw):
    """A schedule whose per-pair fraction is ``frac`` unless ``fracs`` maps
    (dataset, destination) to its own."""
    fracs = fracs or {}

    def fraction(client, dataset, destination):
        return fracs.get((dataset, destination), frac)

    return SendSchedule('c1', ci, fraction=fraction, **kw)


def sp(dataset, dest='A', F=H, P=None, last=FRESH, order=0, depth=None):
    return PairSpec(dataset, dest, F, P, order, depth, last)


def key(dataset, dest='A'):
    return PairKey(dataset, dest)


def cycle(sched, k, t, outcome=Outcome.OK, retryable=False):
    """start + finish at ``t``; returns ``finished``'s ``record_now``."""
    sched.started(k, now=t)
    return sched.finished(k, outcome, retryable=retryable, now=t)


def utc(t):
    return datetime.fromtimestamp(t, timezone.utc)


# ----------------------------------------------------------------------------
# The plan's worked table
# ----------------------------------------------------------------------------

class TestSlotArithmeticTable:
    @pytest.mark.parametrize('F, P, ci, frac, expected', [
        (H, H, 300, 0.5, 1980),         # :33
        (H, None, 300, 0.5, 1980),      # P defaults to F
        (H, H, 60, 0.5, 1860),          # :31
        (H, D, 300, 0.5, 1980),         # 00:33 every day
        (300, 300, 300, 0.5, 225),      # 3m45
        (D, H, 300, 0.5, 1980),         # hourly at :33
        (H, H, 300, 0, 360),            # frac 0 -> lo
        (300, 300, 300, 0, 150),        # lo capped at W / 2
    ])
    def test_slot_offset_table(self, F, P, ci, frac, expected):
        assert slot_offset(frac, F, P, ci) == expected

    def test_offset_through_the_schedule(self):
        s = make()
        s.update_pairs([sp('a', F=H), sp('b', F=H, P=D), sp('c', F=300), sp('d', F=D, P=H)], now=T0)
        assert [s.offset(('a', 'A')), s.offset(('b', 'A')), s.offset(('c', 'A')),
                s.offset(('d', 'A'))] == [1980, 1980, 225, 1980]

    def test_wall_clock_readings(self):
        # F = P = 1 h -> :33 ; ci 60 -> :31 ; F = 1 h, P = 1 d -> 00:33 ; F = 5 m -> 3m45
        assert utc(slot_time(5, H, slot_offset(0.5, H, H, 300))).strftime('%M:%S') == '33:00'
        assert utc(slot_time(5, H, slot_offset(0.5, H, H, 60))).strftime('%M:%S') == '31:00'
        daily = slot_time(T0 // D, D, slot_offset(0.5, H, D, 300))
        assert utc(daily).strftime('%H:%M:%S') == '00:33:00'
        assert slot_time(7, 300, slot_offset(0.5, 300, 300, 300)) % 300 == 225   # 3m45 past the boundary
        assert utc(T0 + slot_offset(0.5, 300, 300, 300)).strftime('%H:%M:%S') == '00:03:45'

    def test_small_window_caps_lo_at_half(self):
        # F = 60 s: lo = min(360, 30)
        assert slot_offset(0, 60, 60, 300) == 30
        assert slot_offset(0.5, 60, 60, 300) == 45

    def test_offset_stays_in_lo_to_w(self):
        for frac in (0, 0.001, 0.25, 0.5, 0.999999):
            off = slot_offset(frac, H, H, 300)
            assert 360 <= off < H

    def test_window_is_min_of_period_and_frequency(self):
        # W = min(P, F): F=1d,P=1h spreads over 1 h; F=1h,P=1d over 1 h.
        assert slot_offset(0.999, D, H, 300) < H
        assert slot_offset(0.999, H, D, 300) < H

    def test_accepts_timedelta_inputs(self):
        assert slot_offset(0.5, timedelta(hours=1), timedelta(hours=1),
                           timedelta(minutes=5)) == 1980
        assert slot_offset(0.5, timedelta(hours=1), None, timedelta(minutes=5)) == 1980

    def test_slots_are_epoch_aligned(self):
        for k in (-3, 0, 17, 480000):
            assert slot_time(k, H, 1980) % H == 1980
        assert slot_time(T0 // D, D, 1980) == T0 + 1980

    def test_slot_index_is_latest_slot_at_or_before(self):
        k = 1000
        st = slot_time(k, H, 1980)
        assert slot_index(st, H, 1980) == k
        assert slot_index(st - 1, H, 1980) == k - 1
        assert slot_index(st + H - 1, H, 1980) == k
        assert slot_index(st + H, H, 1980) == k + 1
        assert slot_index(slot_time(-5, H, 1980) + 1, H, 1980) == -5

    def test_the_schedule_uses_its_client_id(self):
        s = SendSchedule('c1', 300)
        s.update_pairs([sp('pool/x', 'dst')], now=T0)
        assert s.fraction(('pool/x', 'dst')) == pair_fraction('c1', 'pool/x', 'dst')
        assert s.offset(('pool/x', 'dst')) == slot_offset(
            pair_fraction('c1', 'pool/x', 'dst'), H, H, 300)
        other = SendSchedule('c2', 300)
        other.update_pairs([sp('pool/x', 'dst')], now=T0)
        assert other.fraction(('pool/x', 'dst')) != s.fraction(('pool/x', 'dst'))


# ----------------------------------------------------------------------------
# pair_fraction
# ----------------------------------------------------------------------------

class TestPairFraction:
    @staticmethod
    def _expected(*parts):
        digest = hashlib.sha256(b'\0'.join(p.encode() for p in parts)).digest()
        return (int.from_bytes(digest[:8], 'big') >> 11) / 2 ** 53

    def test_matches_the_documented_formula(self):
        assert pair_fraction('client1', 'pool/data', 'backup1') == \
            self._expected('client1', 'pool/data', 'backup1')

    def test_pinned_known_value(self):
        # sha256(b"client1\0pool/data\0backup1")[:8] = 5937881365760165711; stable across runs
        # and processes (no use of the salted builtin hash()).
        assert pair_fraction('client1', 'pool/data', 'backup1') == (5937881365760165711 >> 11) / 2 ** 53
        assert pair_fraction('client1', 'pool/data', 'backup1') == 0.3218931938359182

    def test_the_top_53_bits_never_round_up_to_one(self):
        """`n / 2**64` in floating point rounds a hash within 2**-54 of the top up to 1.0 (a slot
        offset of exactly W); `(n >> 11) / 2**53` is exact and stays below 1."""
        top = (0xFFFFFFFFFFFFFFFF >> 11) / 2 ** 53
        assert top < 1.0 and top == 1.0 - 2 ** -53
        assert 0xFFFFFFFFFFFFFFFF / 2 ** 64 == 1.0, 'the rounding the shift avoids'

    def test_the_maximum_hash_gives_a_value_below_one(self, monkeypatch):
        class Max:
            def digest(self):
                return b'\xff' * 32

        monkeypatch.setattr(ss.hashlib, 'sha256', lambda data: Max())
        value = pair_fraction('c', 'd', 'e')
        assert value < 1.0 and value == 1.0 - 2 ** -53
        assert slot_offset(value, H, H, 300) < H

    def test_the_minimum_hash_gives_zero(self, monkeypatch):
        class Min:
            def digest(self):
                return b'\x00' * 32

        monkeypatch.setattr(ss.hashlib, 'sha256', lambda data: Min())
        assert pair_fraction('c', 'd', 'e') == 0.0

    def test_deterministic(self):
        assert pair_fraction('a', 'b', 'c') == pair_fraction('a', 'b', 'c')

    def test_nul_separation(self):
        assert pair_fraction('a', 'bc/d', 'x') != pair_fraction('ab', 'c/d', 'x')
        assert pair_fraction('a', 'b', 'cd') != pair_fraction('a', 'bc', 'd')
        assert pair_fraction('a', 'bc/d', 'x') == self._expected('a', 'bc/d', 'x')

    def test_each_part_matters(self):
        base = pair_fraction('c', 'd', 'e')
        assert pair_fraction('c2', 'd', 'e') != base
        assert pair_fraction('c', 'd2', 'e') != base
        assert pair_fraction('c', 'd', 'e2') != base

    def test_in_unit_interval(self):
        values = [pair_fraction('c', f'pool/ds{i}', f'dest{i % 7}') for i in range(2000)]
        assert all(0.0 <= v < 1.0 for v in values)
        # spread over the interval, not clustered
        assert min(values) < 0.05 and max(values) > 0.95

    def test_non_utf8_surrogateescape_names_do_not_raise(self):
        assert 0 <= pair_fraction('c', 'pool/\udcff', 'd') < 1
        assert pair_fraction('c', 'pool/é', 'd') == self._expected('c', 'pool/é', 'd')


# ----------------------------------------------------------------------------
# Catch-up and regular slots
# ----------------------------------------------------------------------------

class TestCatchUp:
    @pytest.mark.parametrize('last', [None, T0 - 7 * D, T0 - 5000])
    def test_new_pair_with_old_or_missing_last_attempt(self, last):
        s = make(frac=0.5)
        s.update_pairs([sp('a', last=last)], now=T0)
        # frac * min(W, 300) = 150 s from now
        assert s.due_time(('a', 'A'), T0) == T0 + 150
        assert s.due(T0 + 149) == []
        assert s.due(T0 + 150) == [key('a')]

    def test_catch_up_delay_is_frac_times_min_w_and_300(self):
        s = make(fracs={('a', 'A'): 0.2, ('b', 'A'): 1 - 1e-9, ('c', 'A'): 0.5})
        s.update_pairs([sp('a', last=None), sp('b', last=None), sp('c', F=60, last=None)],
                       now=T0)
        assert s.due_time(('a', 'A'), T0) == pytest.approx(T0 + 60)
        assert s.due_time(('b', 'A'), T0) < T0 + 300 + 1e-6
        assert s.due_time(('b', 'A'), T0) > T0 + 299
        # W = 60 s: 0.5 * 60
        assert s.due_time(('c', 'A'), T0) == T0 + 30

    def test_catch_up_uses_window_not_period(self):
        s = make(frac=0.5)
        s.update_pairs([sp('a', F=H, P=D, last=None)], now=T0)
        assert s.due_time(('a', 'A'), T0) == T0 + 150
        s2 = make(frac=0.5)
        s2.update_pairs([sp('a', F=D, P=120, last=None)], now=T0)   # W = 120
        assert s2.due_time(('a', 'A'), T0) == T0 + 60

    def test_fresh_last_attempt_waits_for_the_next_regular_slot(self):
        s = make()
        s.update_pairs([sp('a', last=FRESH)], now=T0)
        assert s.due_time(('a', 'A'), T0) == T0 + OFFSET
        assert s.due(T0 + OFFSET - 1) == []
        assert s.due(T0 + OFFSET) == [key('a')]

    def test_last_attempt_exactly_at_the_slot_is_fresh_and_one_second_before_is_not(self):
        slot = T0 - H + OFFSET          # the latest slot at T0
        s = make()
        s.update_pairs([sp('a', last=slot), sp('b', last=slot - 1)], now=T0)
        assert s.due_time(('a', 'A'), T0) == T0 + OFFSET
        assert s.due_time(('b', 'A'), T0) == T0 + 150

    @pytest.mark.parametrize('F, P, now, last, expected', [
        (H, H, T0, T0 - 10, T0 + 1980),                          # :33 hourly
        (H, D, T0 + 5000, T0 + 2000, T0 + D + 1980),             # today's 00:33 done: tomorrow's
        (D, H, T0 + 5000, T0 + 2000, T0 + H + 1980),             # F = 1 d, P = 1 h: next :33
    ])
    def test_fresh_pair_next_slot_per_table_row(self, F, P, now, last, expected):
        s = make()
        s.update_pairs([sp('a', F=F, P=P, last=last)], now=now)
        assert s.due_time(('a', 'A'), now) == expected

    def test_idle_pair_on_time_is_due_now_not_delayed(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        assert s.due_time(('a', 'A'), slot + 5) == slot       # due since the slot
        assert s.due(slot + 5) == [key('a')]
        # the slot is armed on time: seeing it later does not turn it into a catch-up
        assert s.due_time(('a', 'A'), slot + 500) == slot

    def test_late_slack_boundary(self):
        slot = T0 + OFFSET
        s = make(frac=0.5)
        s.update_pairs([sp('a')], now=T0)
        # first looked at exactly LATE_SLACK late: still on time
        assert s.due_time(('a', 'A'), slot + LATE_SLACK) == slot
        s2 = make(frac=0.5)
        s2.update_pairs([sp('a')], now=T0)
        # one second more: an outage, one catch-up delay from now
        t = slot + LATE_SLACK + 1
        assert s2.due_time(('a', 'A'), t) == t + 150

    def test_idle_pair_first_seeing_its_slot_late_gets_one_catch_up(self):
        s = make(frac=0.4)
        s.update_pairs([sp('a')], now=T0)
        off = s.offset(('a', 'A'))
        late = T0 + off + 2 * H + 500         # three slots later, no query in between
        assert s.due(late) == []
        assert s.due_time(('a', 'A'), late) == late + 0.4 * 300
        assert s.due(late + 120) == [key('a')]
        cycle(s, ('a', 'A'), late + 120)
        # the missed slots are coalesced: next is the next regular slot
        nxt = slot_time(slot_index(late, H, off) + 1, H, off)
        assert s.due_time(('a', 'A'), late + 121) == nxt

    def test_catch_up_is_not_repeated_after_it_ran(self):
        s = make()
        s.update_pairs([sp('a', last=None)], now=T0)
        cycle(s, ('a', 'A'), T0 + 150)
        assert s.due(T0 + 151) == []
        assert s.due_time(('a', 'A'), T0 + 151) == T0 + OFFSET


class TestRegularSlots:
    def test_runs_once_per_slot(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        runs = []
        for t in range(T0, T0 + 5 * H, 30):
            for k in s.dispatch(now=t):
                s.started(k, now=t)
                runs.append(t)
                s.finished(k, 'ok', now=t + 5)
        assert len(runs) == 5
        assert all(OFFSET <= r % H < OFFSET + 30 for r in runs)       # at :33, within a tick

    def test_forward_jump_gives_one_run_not_one_per_missed_slot(self):
        s = make(frac=0.5)
        s.update_pairs([sp('a')], now=T0)
        t = T0 + 10 * H + 1234                 # ten slots missed
        runs = []
        for step in range(0, 3 * H, 10):
            now = t + step
            for k in s.dispatch(now=now):
                s.started(k, now=now)
                runs.append(now)
                s.finished(k, 'ok', now=now + 1)
        # one catch-up for the whole gap, then only the regular slots of the next 3 h
        assert runs[0] == t + 150
        # the catch-up, then the slots still ahead at 10:33, 11:33, 12:33 (none of the missed ones)
        assert len(runs) == 4
        assert [(r - T0) // H for r in runs[1:]] == [10, 11, 12]
        assert all(r % H >= OFFSET for r in runs[1:])

    def test_forward_jump_single_catch_up(self):
        s = make(frac=0.5)
        s.update_pairs([sp('a')], now=T0)
        t = T0 + 10 * H + 1234
        assert s.due(t) == []
        due_at = s.due_time(('a', 'A'), t)
        assert due_at == t + 150
        assert s.due(due_at) == [key('a')]
        cycle(s, ('a', 'A'), due_at)
        # nothing owed for the other nine missed slots
        assert s.due(due_at + 1) == []
        assert s.due_time(('a', 'A'), due_at + 1) > due_at + 1


# ----------------------------------------------------------------------------
# Overrun
# ----------------------------------------------------------------------------

class TestOverrun:
    @pytest.mark.parametrize('outcome', ['ok', 'not_due'])
    def test_a_run_spanning_several_slots_owes_exactly_one_follow_up(self, outcome):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        s.started(('a', 'A'), now=slot)
        end = slot + 3 * H + 1800            # three slots passed while it ran
        s.finished(('a', 'A'), outcome, now=end)
        assert s.due(end) == [key('a')]      # follow-up due at once
        assert s.due_time(('a', 'A'), end) <= end
        s.started(('a', 'A'), now=end)
        s.finished(('a', 'A'), 'ok', now=end + 10)
        assert s.due(end + 10) == []
        nxt = slot_time(slot_index(end, H, OFFSET) + 1, H, OFFSET)
        assert s.due_time(('a', 'A'), end + 10) == nxt

    def test_one_slot_overrun_also_follows_up(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        s.started(('a', 'A'), now=slot)
        s.finished(('a', 'A'), 'ok', now=slot + H + 5)
        assert s.due(slot + H + 5) == [key('a')]

    def test_no_follow_up_when_it_finished_in_its_own_slot(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        s.started(('a', 'A'), now=slot)
        s.finished(('a', 'A'), 'ok', now=slot + 100)
        assert s.due(slot + 100) == []
        assert s.due_time(('a', 'A'), slot + 100) == slot + H

    def test_follow_up_is_not_delayed_like_a_catch_up(self):
        s = make(frac=0.9)
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + s.offset(('a', 'A'))
        s.started(('a', 'A'), now=slot)
        end = slot + 2 * H + 3000
        s.finished(('a', 'A'), 'ok', now=end)
        assert s.due_time(('a', 'A'), end) <= end

    def test_failure_covers_the_slots_that_passed(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        s.started(('a', 'A'), now=slot)
        end = slot + 3 * H + 600
        assert s.finished(('a', 'A'), 'failed', now=end) is True
        assert s.due(end) == []              # no immediate follow-up after a failure
        nxt = slot_time(slot_index(end, H, OFFSET) + 1, H, OFFSET)
        assert s.due_time(('a', 'A'), end) == min(nxt, end + FAILURE_RETRY_CAP)


# ----------------------------------------------------------------------------
# Clock steps
# ----------------------------------------------------------------------------

class TestClockSteps:
    def _ran(self, s, t, name='a'):
        """Run the pair at ``t`` and finish OK 10 s later; returns the finish time."""
        cycle(s, (name, 'A'), t)
        return t

    def test_small_back_step_across_a_slot_reruns_it_once_at_the_slot_not_at_the_step(self):
        # Deliberate deviation from the plan's "never re-run an attempted slot" (see the module
        # docstring): the attempted index is clamped to the stepped-back clock's slot.
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        t = slot + 30
        assert s.dispatch(now=t) == [key('a')]
        cycle(s, ('a', 'A'), t)
        back = t - 1800                            # half a period back: the previous slot's period
        assert s.due(back) == []                   # not at the step instant
        assert s.dispatch(now=back) == []
        assert s.due_time(('a', 'A'), back) == slot
        for now in range(back, slot, 60):          # replaying the time before the slot: nothing
            assert s.due(now) == []
            assert s.dispatch(now=now) == []
        assert s.dispatch(now=slot) == [key('a')]  # the re-run, once, at the slot
        cycle(s, ('a', 'A'), slot)
        nxt = slot + H
        assert s.due_time(('a', 'A'), slot + 1) == nxt        # then the normal schedule
        for now in range(slot + 1, nxt, 60):
            assert s.due(now) == []
        assert s.due(nxt) == [key('a')]

    def test_a_start_during_a_back_step_attempts_the_current_slot_of_the_new_clock(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        t = slot + 30
        cycle(s, ('a', 'A'), t)
        back = t - 1800                                   # in the previous slot's period
        cycle(s, ('a', 'A'), back)                        # e.g. a forced start while behind
        assert s.due(back + 1) == []                      # the slot it just covered is not due
        assert s.due_time(('a', 'A'), back + 1) == slot   # the next slot of the new clock
        assert s.due(slot) == [key('a')]

    def test_a_start_never_lowers_the_attempted_slot_while_the_clock_moves_forward(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        t = T0 + OFFSET + 30
        cycle(s, ('a', 'A'), t)
        cycle(s, ('a', 'A'), t + 10)           # a second start in the same slot
        assert s.due_time(('a', 'A'), t + 11) == T0 + H + OFFSET

    def test_back_by_just_under_a_period_reruns_once_at_the_slot_not_at_the_step(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        t = slot + 30
        cycle(s, ('a', 'A'), t)
        back = t - (H - 60)                          # inside the previous slot's period
        assert s.due(back) == []                     # not due at the step instant
        assert s.due_time(('a', 'A'), back) == slot
        assert s.due(slot - 1) == []
        assert s.dispatch(now=slot) == [key('a')]    # the replayed slot time: once
        cycle(s, ('a', 'A'), slot)
        assert s.due(t) == []
        assert s.due_time(('a', 'A'), t) == slot + H

    def test_a_step_back_within_the_same_slot_changes_nothing(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        t = T0 + OFFSET + 1000
        cycle(s, ('a', 'A'), t)
        for now in (t - 1, t - 900):                 # still inside the slot's period
            assert s.due(now) == []
            assert s.due_time(('a', 'A'), now) == T0 + H + OFFSET

    @pytest.mark.parametrize('observed, back_by', [
        (100, H), (100, 2 * H), (4000, 2 * H), (3 * H + 5, 3 * H), (10 * H, 6 * D)
    ])
    def test_a_step_back_long_after_the_last_event_still_clamps(self, observed, back_by):
        # no "newest state change" to measure the step against: the schedule only looks at the
        # attempted index and the clock
        s = make()
        s.update_pairs([sp('a')], now=T0)
        slot = T0 + OFFSET
        t = slot + 30
        cycle(s, ('a', 'A'), t)
        s.due(t + observed)                            # the clock ran on (arms a late slot)
        now = t + observed - back_by
        due = s.due_time(('a', 'A'), now)
        assert now < due <= now + H
        assert s.due(now) == []

    @pytest.mark.parametrize('P, back_by', [(H, 3 * D), (D, 5 * D), (H, H + 5), (H, H)])
    def test_back_by_a_period_or_more_is_due_within_one_period_and_runs_once(self, P, back_by):
        s = make()
        s.update_pairs([sp('a', F=P, last=T0 - 10)], now=T0)
        off = s.offset(('a', 'A'))
        first = T0 + P + off + 5
        s.update_pairs([sp('a', F=P, last=T0 - 10)], now=first)   # no state change (same spec)
        cycle(s, ('a', 'A'), first)
        now = first - back_by              # the newest state change is `back_by` ahead
        due = s.due_time(('a', 'A'), now)
        assert now < due <= now + P
        assert s.due(now) == []
        runs = 0
        t = now
        while t <= due + 2 * P:
            for k in s.dispatch(now=t):
                s.started(k, now=t)
                s.finished(k, 'ok', now=t + 1)
                runs += 1
            t += max(P // 20, 1)
            if runs:
                break
        assert runs == 1
        # and the run happened at the first due time, not later
        assert t - max(P // 20, 1) <= due + max(P // 20, 1)
        # then it is quiet until the next slot
        assert s.due(t + 5) == []

    def test_three_days_back_with_one_hour_period_runs_once(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        cycle(s, ('a', 'A'), T0 + OFFSET + 5)
        now = T0 + OFFSET + 5 - 3 * D
        due = s.due_time(('a', 'A'), now)
        assert 0 < due - now <= H
        assert s.due(due - 1) == []
        assert s.due(due) == [key('a')]
        cycle(s, ('a', 'A'), due)
        assert s.due(due + 1) == []
        assert s.due_time(('a', 'A'), due + 1) == due + H

    def test_five_days_back_with_one_day_period(self):
        s = make()
        s.update_pairs([sp('a', F=D)], now=T0)
        off = s.offset(('a', 'A'))
        t = T0 + D + off + 5
        cycle(s, ('a', 'A'), t)
        now = t - 5 * D
        due = s.due_time(('a', 'A'), now)
        assert now < due <= now + D

    def test_a_pending_retry_is_capped_after_a_big_back_step(self):
        W = 7 * D
        s = make()
        s.update_pairs([sp('a', F=W, last=None)], now=T0)
        t1 = s.due_time(('a', 'A'), T0)
        s.started(('a', 'A'), now=t1)
        assert s.finished(('a', 'A'), 'failed', now=t1) is True    # retry at t1 + 1 h
        assert s.due_time(('a', 'A'), t1) == t1 + FAILURE_RETRY_CAP
        now = t1 - 5 * D
        assert s.due_time(('a', 'A'), now) == now + FAILURE_RETRY_CAP

    def test_a_retryable_backoff_is_capped_after_a_back_step(self):
        W = 7 * D
        s = make()
        s.update_pairs([sp('a', F=W, last=FRESH)], now=T0)
        t1 = T0 + 30 * D
        s.update_pairs([sp('a', F=W, last=FRESH)], now=t1)
        s.started(('a', 'A'), now=t1)
        s.finished(('a', 'A'), 'retryable', now=t1)                # retry at t1 + 60
        now = t1 - 5 * D
        assert s.due_time(('a', 'A'), now) == now + FAILURE_RETRY_CAP

    def test_a_pending_catch_up_is_capped_after_a_back_step(self):
        s = make(frac=0.9)
        s.update_pairs([sp('a', F=D, last=None)], now=T0)
        assert s.due_time(('a', 'A'), T0) == T0 + 270
        now = T0 - 1000
        assert s.due_time(('a', 'A'), now) == now + CATCH_UP_CAP

    def test_a_catch_up_pending_after_an_old_attempt_is_bounded_by_the_period(self):
        # last_attempt 2 d ago: stamp - now >= P after a 3-day step back -> the schedule unstalls.
        s = make(frac=0.5)
        s.update_pairs([sp('a', last=T0 - 2 * D)], now=T0)
        now = T0 - 3 * D
        due = s.due_time(('a', 'A'), now)
        assert now < due <= now + H

    @pytest.mark.parametrize('last', [None, T0 - 10 * D])
    def test_pending_catch_up_after_a_three_day_back_step(self, last):
        # regression: a pair that has not started since it entered the schedule used to stall
        # for the whole step, because the capped catch-up was ignored behind an unclamped index
        s = make(frac=0.5)
        s.update_pairs([sp('a', last=last)], now=T0)
        now = T0 - 3 * D
        due = s.due_time(('a', 'A'), now)
        assert now < due <= now + H

    def test_confirmation_is_capped_after_a_back_step(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH)], now=T0)
        assert s.due(T0) == [key('p')]               # confirmation due now
        now = T0 - 3 * D
        assert s.due(now) == [key('p')]              # not in the future of the new clock
        assert s.due_time(('p', 'A'), now) <= now

    def test_forward_moving_clock_never_shortens_a_legitimate_delay(self):
        # catch-up: the largest legitimate delay is just under 300 s
        s = make(frac=0.999)
        s.update_pairs([sp('a', last=None)], now=T0)
        due = T0 + 0.999 * 300
        for now in (T0, T0 + 1, T0 + 100, T0 + 299):
            assert s.due_time(('a', 'A'), now) == due
        # failure retry: exactly 1 h stays exactly 1 h at every later query
        s = make()
        s.update_pairs([sp('a', F=D)], now=T0)
        t = T0 + s.offset(('a', 'A')) + 10
        cycle(s, ('a', 'A'), t, 'failed')
        for now in (t, t + 1, t + 1800, t + 3599):
            assert s.due_time(('a', 'A'), now) == t + FAILURE_RETRY_CAP
        # the longest retryable backoff
        s = make()
        s.update_pairs([sp('a', F=D)], now=T0)
        t = T0 + s.offset(('a', 'A')) + 10
        for _ in range(2):
            cycle(s, ('a', 'A'), t, 'retryable')
        cycle(s, ('a', 'A'), t, 'retryable')
        for now in (t, t + 100, t + 299):
            assert s.due_time(('a', 'A'), now) == t + 300
        # a confirmation that is due stays at its own time (never moves forward either)
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        for now in (T0, T0 + 100, T0 + 1000):
            assert s.due_time(('p', 'A'), now) == T0

    def test_discovery_time_is_exact_while_the_clock_moves_forward(self):
        s = make(ci=60)
        s.update_pairs([sp('p', F=D, last=FRESH)], now=T0)
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', True, now=T0)          # interval 1 h
        for now in (T0, T0 + 1800, T0 + H - 1):
            assert s.discovery_due(now) == []
        assert s.discovery_due(T0 + H) == ['p']

    # regression: the cap used to slide with `now`, so the parent was never due until the clock
    # got back to the old next_at
    def test_discovery_time_is_capped_after_a_back_step(self):
        s = make(ci=60)
        s.update_pairs([sp('p', F=D, last=FRESH)], now=T0)
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', True, now=T0)          # next at T0 + 1 h
        back = T0 - 3 * D
        assert s.discovery_due(back) == []
        due_by = back + max(60, DISCOVERY_MAX_INTERVAL)
        # the caller polls the clock: it is due within the cap, as the clock moves on
        assert any(s.discovery_due(now) == ['p'] for now in range(int(back), int(due_by) + 1, 60))

    def test_a_long_check_interval_discovery_delay_is_not_shortened(self):
        s = make(ci=7200)
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', True, now=T0)
        assert s.discovery_due(T0 + 7199) == []
        assert s.discovery_due(T0 + 7200) == ['p']

    def test_clock_reads_the_injected_clock(self):
        clock = Clock(T0)
        s = make(clock=clock)
        s.update_pairs([sp('a', last=None)])
        assert s.due() == []
        clock.t = T0 + 150
        assert s.due() == [key('a')]
        assert s.dispatch() == [key('a')]
        s.started(('a', 'A'))
        assert s.running == {key('a')}
        s.finished(('a', 'A'), 'ok')
        assert s.running == set()
        assert s.next_wakeup() == T0 + OFFSET


# ----------------------------------------------------------------------------
# Retries and what is recorded
# ----------------------------------------------------------------------------

class TestRetries:
    def _fresh(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        t = T0 + OFFSET + 10
        return s, t

    def test_retry_delays_constant(self):
        assert RETRY_DELAYS == (60.0, 120.0, 300.0)
        assert FAILURE_RETRY_CAP == 3600.0

    @pytest.mark.parametrize('kind, retryable', [
        ('retryable', False), (Outcome.RETRYABLE, False), ('failed', True),
        (Outcome.FAILED, True), (Outcome.RETRYABLE, True),
    ])
    def test_retryable_backoff_then_recorded(self, kind, retryable):
        s, t = self._fresh()
        for delay in (60, 120, 300):
            s.started(('a', 'A'), now=t)
            assert s.finished(('a', 'A'), kind, retryable=retryable, now=t) is False
            assert s.due_time(('a', 'A'), t) == t + delay
            assert s.due(t + delay - 1) == []
            assert s.due(t + delay) == [key('a')]
            t += delay
        # used up: recorded, and now an "other failure"
        s.started(('a', 'A'), now=t)
        assert s.finished(('a', 'A'), kind, retryable=retryable, now=t) is True
        assert s.due_time(('a', 'A'), t) == min(t + FAILURE_RETRY_CAP,
                                                slot_time(slot_index(t, H, OFFSET) + 1, H, OFFSET))

    def test_retryable_and_failed_retryable_are_equivalent(self):
        a, t = self._fresh()
        b, _ = self._fresh()
        for _ in range(6):
            a.started(('a', 'A'), now=t)
            b.started(('a', 'A'), now=t)
            ra = a.finished(('a', 'A'), 'retryable', now=t)
            rb = b.finished(('a', 'A'), 'failed', retryable=True, now=t)
            assert ra == rb
            assert a.due_time(('a', 'A'), t) == b.due_time(('a', 'A'), t)
            t += 30

    def test_used_up_burst_is_recorded_once_then_resets(self):
        s, t = self._fresh()
        seen = [cycle(s, ('a', 'A'), t + i, 'retryable') for i in range(8)]
        # False, False, False, True, then the burst starts over
        assert seen == [False, False, False, True, False, False, False, True]
        # and the delays restart at 60
        s2, t = self._fresh()
        for _ in range(4):
            cycle(s2, ('a', 'A'), t, 'retryable')
        cycle(s2, ('a', 'A'), t, 'retryable')
        assert s2.due_time(('a', 'A'), t) == t + 60

    def test_other_failure_is_recorded_and_retried_within_the_hour(self):
        s, t = self._fresh()
        assert cycle(s, ('a', 'A'), t, 'failed') is True
        nxt = T0 + H + OFFSET
        # next regular slot (~ 33 min away) preempts the 1 h retry
        assert s.due_time(('a', 'A'), t) == nxt < t + H

    def test_other_failure_retry_is_exactly_one_hour_when_the_slot_is_further(self):
        s = make()
        s.update_pairs([sp('a', F=D)], now=T0)
        off = s.offset(('a', 'A'))
        t = T0 + off + 10
        assert cycle(s, ('a', 'A'), t, 'failed') is True
        assert s.due_time(('a', 'A'), t) == t + FAILURE_RETRY_CAP      # no jitter
        assert s.due(t + H - 1) == []
        assert s.due(t + H) == [key('a')]

    def test_failure_retry_has_no_jitter_for_any_fraction(self):
        for frac in (0, 0.3, 0.99):
            s = make(frac=frac)
            s.update_pairs([sp('a', F=D)], now=T0)
            t = T0 + s.offset(('a', 'A')) + 10
            cycle(s, ('a', 'A'), t, 'failed')
            assert s.due_time(('a', 'A'), t) == t + H

    def test_failed_after_failed_is_recorded_each_time(self):
        s = make()
        s.update_pairs([sp('a', F=D)], now=T0)
        t = T0 + s.offset(('a', 'A')) + 10
        assert [cycle(s, ('a', 'A'), t + i * H, 'failed') for i in range(3)] == [True] * 3

    def test_a_non_retryable_failure_resets_the_burst(self):
        s, t = self._fresh()
        cycle(s, ('a', 'A'), t, 'retryable')
        cycle(s, ('a', 'A'), t, 'retryable')
        assert cycle(s, ('a', 'A'), t, 'failed') is True
        cycle(s, ('a', 'A'), t, 'retryable')
        assert s.due_time(('a', 'A'), t) == t + 60

    def test_ok_records_and_clears_the_burst(self):
        s, t = self._fresh()
        cycle(s, ('a', 'A'), t, 'retryable')
        cycle(s, ('a', 'A'), t, 'retryable')
        assert cycle(s, ('a', 'A'), t + 200, 'ok') is True
        assert s.due(t + 210) == []
        cycle(s, ('a', 'A'), t + 220, 'retryable')
        assert s.due_time(('a', 'A'), t + 220) == t + 220 + 60      # burst restarted

    def test_ok_clears_a_pending_retry(self):
        s, t = self._fresh()
        s.started(('a', 'A'), now=t)
        s.finished(('a', 'A'), 'retryable', now=t)
        s.started(('a', 'A'), now=t + 60)           # the retry
        s.finished(('a', 'A'), 'ok', now=t + 61)
        assert s.due(t + 62) == []
        assert s.due_time(('a', 'A'), t + 62) == T0 + H + OFFSET

    @pytest.mark.parametrize('kind, recorded', [
        ('ok', True), ('not_due', False), ('stopped', False), ('waiting', False),
        ('failed', True), ('retryable', False),
    ])
    def test_record_now_per_outcome(self, kind, recorded):
        s, t = self._fresh()
        assert cycle(s, ('a', 'A'), t, kind) is recorded

    def test_not_due_records_nothing_and_is_not_retried(self):
        s, t = self._fresh()
        assert cycle(s, ('a', 'A'), t, 'not_due') is False
        assert s.due(t + 61) == []
        assert s.due_time(('a', 'A'), t) == T0 + H + OFFSET

    def test_stopped_records_nothing_and_schedules_no_retry(self):
        s, t = self._fresh()
        assert cycle(s, ('a', 'A'), t, 'stopped') is False
        assert s.due(T0 + H + OFFSET - 1) == []              # no soon-retry
        assert s.due_time(('a', 'A'), t) == T0 + H + OFFSET

    def test_waiting_is_asked_again_after_a_minute(self):
        s, t = self._fresh()
        assert cycle(s, ('a', 'A'), t, 'waiting') is False
        assert WAIT_RECHECK == 60.0
        assert s.due_time(('a', 'A'), t) == t + 60
        assert s.due(t + 59) == []
        assert s.due(t + 60) == [key('a')]

    def test_waiting_never_consumes_the_retry_budget(self):
        s, t = self._fresh()
        for i in range(10):
            assert cycle(s, ('a', 'A'), t + i * 60, 'waiting') is False
        t += 600
        for delay in (60, 120, 300):
            assert cycle(s, ('a', 'A'), t, 'retryable') is False
            assert s.due_time(('a', 'A'), t) == t + delay
        assert cycle(s, ('a', 'A'), t, 'retryable') is True

    def test_waiting_does_not_reset_a_retry_burst(self):
        s, t = self._fresh()
        cycle(s, ('a', 'A'), t, 'retryable')
        cycle(s, ('a', 'A'), t + 1, 'waiting')
        cycle(s, ('a', 'A'), t + 2, 'retryable')
        assert s.due_time(('a', 'A'), t + 2) == t + 2 + 120

    def test_string_outcomes_are_accepted_and_unknown_is_refused(self):
        s, t = self._fresh()
        s.started(('a', 'A'), now=t)
        with pytest.raises(ValueError):
            s.finished(('a', 'A'), 'nonsense', now=t)
        assert Outcome('ok') is Outcome.OK
        assert Outcome.OK == 'ok'

    def test_finished_for_a_pair_not_in_the_schedule(self):
        s, t = self._fresh()
        s.started(('a', 'A'), now=t)
        s.update_pairs([], now=t)
        assert s.running == {key('a')}
        assert s.finished(('a', 'A'), 'ok', now=t + 1) is False
        assert s.running == set()
        # an unknown pair that never ran is just ignored
        assert s.finished(('zz', 'A'), 'failed', now=t) is False


# ----------------------------------------------------------------------------
# Caps
# ----------------------------------------------------------------------------

def due_pairs(spec_list, **kw):
    """A schedule of new unattempted pairs and a ``now`` at which all are due."""
    s = make(**kw)
    s.update_pairs(spec_list, now=T0)
    return s, T0 + 400


class TestCaps:
    def test_constants(self):
        assert (MAX_SENDERS, MAX_SENDERS_PER_DESTINATION) == (4, 2)

    def test_four_in_total(self):
        specs = [sp(f'p{i}', dest, last=None, order=i)
                 for i, dest in enumerate(['A', 'A', 'B', 'B', 'C', 'C'])]
        s, now = due_pairs(specs)
        chosen = s.dispatch(now=now)
        assert len(chosen) == MAX_SENDERS
        assert chosen == [key('p0', 'A'), key('p1', 'A'), key('p2', 'B'), key('p3', 'B')]

    def test_two_per_destination(self):
        specs = [sp(f'p{i}', 'A', last=None, order=i) for i in range(4)]
        s, now = due_pairs(specs)
        assert s.dispatch(now=now) == [key('p0'), key('p1')]

    def test_running_pairs_count_against_the_destination_cap(self):
        specs = [sp(f'p{i}', 'A', last=None, order=i) for i in range(3)]
        s, now = due_pairs(specs)
        s.started(('p0', 'A'), now=now)
        assert s.dispatch(now=now) == [key('p1')]
        s.started(('p1', 'A'), now=now)
        assert s.dispatch(now=now) == []

    def test_running_pairs_count_against_the_total_cap(self):
        specs = [sp('r1', 'A', last=None), sp('r2', 'B', last=None), sp('r3', 'C', last=None),
                 sp('d1', 'D', last=None, order=1), sp('d2', 'D', last=None, order=2)]
        s, now = due_pairs(specs)
        for name, dest in (('r1', 'A'), ('r2', 'B'), ('r3', 'C')):
            s.started((name, dest), now=now)
        assert s.dispatch(now=now) == [key('d1', 'D')]       # 3 running + 1 chosen

    def test_chosen_in_the_same_call_count(self):
        specs = [sp('x', 'A', last=None), sp('y', 'A', last=None, order=1),
                 sp('z', 'A', last=None, order=2)]
        s, now = due_pairs(specs)
        assert len(s.dispatch(now=now)) == 2
        # and dispatch reserved nothing: asked again, the same answer
        assert len(s.dispatch(now=now)) == 2

    def test_a_pair_removed_while_running_still_counts(self):
        s, now = due_pairs([sp('a1', last=None), sp('a2', last=None)])
        s.started(('a1', 'A'), now=now)
        s.started(('a2', 'A'), now=now)
        s.update_pairs([sp('a3', last=None), sp('a4', last=None, order=1)], now=now)
        assert s.running == {key('a1'), key('a2')}
        assert s.dispatch(now=now + 400) == []
        assert s.finished(('a1', 'A'), 'ok', now=now + 400) is False
        assert s.dispatch(now=now + 400) == [key('a3')]
        s.finished(('a2', 'A'), 'ok', now=now + 400)
        assert len(s.dispatch(now=now + 400)) == 2

    def test_a_pair_removed_while_running_still_counts_in_the_total(self):
        s, now = due_pairs([sp('a1', 'A', last=None), sp('b1', 'B', last=None),
                            sp('c1', 'C', last=None), sp('d1', 'D', last=None)])
        for n, d in (('a1', 'A'), ('b1', 'B'), ('c1', 'C'), ('d1', 'D')):
            s.started((n, d), now=now)
        s.update_pairs([sp('e1', 'E', last=None)], now=now)
        assert s.dispatch(now=now + 400) == []

    def test_running_argument_is_unioned(self):
        s, now = due_pairs([sp('a', 'A', last=None), sp('b', 'B', last=None, order=1)])
        assert s.dispatch(now=now, running=[('x', 'A'), ('y', 'A')]) == [key('b', 'B')]
        assert s.dispatch(now=now, running=[('a', 'A')]) == [key('b', 'B')]   # not twice
        # the schedule's own and the argument add up
        specs = [sp(f'p{i}', 'A', last=None, order=i) for i in range(4)]
        s, now = due_pairs(specs)
        s.started(('p0', 'A'), now=now)
        assert s.dispatch(now=now, running=[('other', 'A')]) == []
        assert s.dispatch(now=now, running=[('o1', 'B'), ('o2', 'C')]) == [key('p1')]
        assert s.dispatch(now=now, running=[('o1', 'B'), ('o2', 'C'), ('o3', 'D')]) == []

    def test_running_argument_accepts_pairkeys_and_tuples(self):
        s, now = due_pairs([sp('a', 'A', last=None)])
        assert s.dispatch(now=now, running=[PairKey('a', 'A')]) == []
        assert s.dispatch(now=now, running=iter([('a', 'A')])) == []

    def test_custom_caps(self):
        specs = [sp(f'p{i}', f'D{i}', last=None) for i in range(5)]
        s, now = due_pairs(specs, max_senders=2, max_per_destination=1)
        assert len(s.dispatch(now=now)) == 2
        specs = [sp(f'p{i}', 'A', last=None, order=i) for i in range(5)]
        s, now = due_pairs(specs, max_senders=3, max_per_destination=3)
        assert len(s.dispatch(now=now)) == 3

    def test_a_destination_at_its_cap_does_not_block_other_destinations(self):
        specs = [sp('a1', 'A', last=None), sp('a2', 'A', last=None, order=1),
                 sp('a3', 'A', last=None, order=2), sp('b1', 'B', last=None, order=3)]
        s, now = due_pairs(specs)
        assert s.dispatch(now=now) == [key('a1'), key('a2'), key('b1', 'B')]

    def test_dispatch_returns_nothing_that_is_not_due(self):
        s, now = due_pairs([sp('a', last=FRESH)])
        assert s.dispatch(now=T0) == []


# ----------------------------------------------------------------------------
# Exclusion and order
# ----------------------------------------------------------------------------

class TestExclusion:
    def _tree(self, **kw):
        s = make(**kw)
        specs = [sp('p', last=None), sp('p/c', last=None, order=1),
                 sp('p/c/g', last=None, order=2), sp('p/d', last=None, order=3),
                 sp('q', last=None, order=4)]
        s.update_pairs(specs, now=T0)
        for n in ('p', 'p/c'):
            s.confirmed((n, 'A'))               # release the R-2 holds
        return s, T0 + 400

    def test_an_ancestor_that_is_due_goes_first_alone(self):
        s, now = self._tree()
        # p is due: its descendants wait; the unrelated q is not blocked
        assert s.dispatch(now=now) == [key('p'), key('q')]
        assert key('p/c') in s.due(now)              # due, but not dispatched

    def test_descendants_wait_while_the_ancestor_runs(self):
        s, now = self._tree()
        s.started(('p', 'A'), now=now)
        assert s.dispatch(now=now) == [key('q')]
        s.finished(('p', 'A'), 'ok', now=now + 1)
        # the children may go now (2 per destination: the unrelated q takes one slot)
        chosen = s.dispatch(now=now + 1)
        assert key('p') not in chosen
        assert key('p/c') in chosen and set(chosen) <= {key('p/c'), key('p/d'), key('q')}
        assert key('p/c/g') not in chosen            # p/c is due ahead of it

    def test_an_ancestor_waits_while_a_descendant_runs(self):
        s, now = self._tree()
        s.started(('p/c', 'A'), now=now)
        chosen = s.dispatch(now=now)
        assert key('p') not in chosen
        # p/c/g (a descendant of the running p/c) and p/d (its ancestor p is due)
        assert chosen == [key('q')]

    def test_a_deep_descendant_running_blocks_every_ancestor(self):
        s, now = self._tree()
        s.confirmed(('p/c', 'A'))
        s.started(('p/c/g', 'A'), now=now)
        chosen = s.dispatch(now=now)
        assert key('p') not in chosen and key('p/c') not in chosen

    def test_a_due_ancestor_blocked_by_a_running_descendant_still_holds_back_its_other_descendants(self):
        # p/d runs, so p cannot start; p is due, so p/c (also below p) must not overtake it,
        # or a steady stream of children would starve the ancestor.
        s = make()
        s.update_pairs([sp('p', last=None), sp('p/c', last=None, order=1),
                        sp('p/d', last=None, order=2)], now=T0)
        s.confirmed(('p', 'A'))
        now = T0 + 400
        s.started(('p/d', 'A'), now=now)
        assert s.dispatch(now=now) == []
        s.finished(('p/d', 'A'), 'ok', now=now + 1)
        assert s.dispatch(now=now + 1) == [key('p')]

    def test_siblings_run_concurrently(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=None, order=1),
                        sp('p/d', last=None, order=2)], now=T0)
        s.confirmed(('p', 'A'))
        now = T0 + 400
        assert s.dispatch(now=now) == [key('p/c'), key('p/d')]

    def test_unrelated_prefix_names_are_not_related(self):
        s = make()
        s.update_pairs([sp('pool/a', last=None), sp('pool/ab', last=None, order=1)], now=T0)
        assert s.dispatch(now=T0 + 400) == [key('pool/a'), key('pool/ab')]
        s.started(('pool/a', 'A'), now=T0 + 400)
        assert s.dispatch(now=T0 + 400) == [key('pool/ab')]

    def test_exclusion_is_per_destination(self):
        s = make()
        s.update_pairs([sp('p', 'A', last=None), sp('p/c', 'B', last=None, order=1)], now=T0)
        assert s.dispatch(now=T0 + 400) == [key('p', 'A'), key('p/c', 'B')]

    def test_the_same_dataset_to_two_destinations_runs_concurrently(self):
        s = make()
        s.update_pairs([sp('x', 'A', last=None), sp('x', 'B', last=None)], now=T0)
        chosen = s.dispatch(now=T0 + 400)
        assert chosen == [key('x', 'A'), key('x', 'B')]
        s.started(('x', 'A'), now=T0 + 400)
        assert s.dispatch(now=T0 + 400) == [key('x', 'B')]

    def test_a_due_but_cap_blocked_ancestor_blocks_descendants_not_unrelated(self):
        s = make()
        specs = [sp('u1', 'A', last=None), sp('u2', 'A', last=None, order=1),
                 sp('p', 'A', last=None, order=2), sp('p/c', 'A', last=None, order=3),
                 sp('q', 'B', last=None, order=4)]
        s.update_pairs(specs, now=T0)
        s.confirmed(('p', 'A'))
        now = T0 + 400
        s.started(('u1', 'A'), now=now)
        s.started(('u2', 'A'), now=now)               # destination A is at its cap
        chosen = s.dispatch(now=now)
        assert key('p') not in chosen                 # cap-blocked
        assert key('p/c') not in chosen               # blocked behind the due ancestor
        assert chosen == [key('q', 'B')]              # unrelated pair unaffected
        s.finished(('u1', 'A'), 'ok', now=now)
        chosen = s.dispatch(now=now)
        assert key('p') in chosen and key('p/c') not in chosen

    def test_total_cap_blocked_ancestor_blocks_descendants_not_other_pairs(self):
        s = make(max_senders=1)
        s.update_pairs([sp('p', last=None), sp('p/c', last=None, order=1)], now=T0)
        s.confirmed(('p', 'A'))
        now = T0 + 400
        s.update_pairs([sp('p', last=None), sp('p/c', last=None, order=1)], now=now)
        s.confirmed(('p', 'A'))
        s.started(('p', 'A'), now=now)
        assert s.dispatch(now=now) == []
        s.finished(('p', 'A'), 'ok', now=now)
        assert s.dispatch(now=now) == [key('p/c')]


class TestOrder:
    def test_by_due_time(self):
        fr = {('a', 'A'): 0.9, ('b', 'A'): 0.1, ('c', 'A'): 0.5}
        s = make(fracs=fr)
        s.update_pairs([sp('a', last=None), sp('b', last=None, order=1),
                        sp('c', last=None, order=2)], now=T0)
        assert s.due(T0 + 400) == [key('b'), key('c'), key('a')]

    def test_ties_by_depth(self):
        s = make()
        s.update_pairs([sp('x/y/z', last=None, order=0), sp('w', last=None, order=1),
                        sp('v/u', last=None, order=2)], now=T0)
        assert s.due(T0 + 400) == [key('w'), key('v/u'), key('x/y/z')]

    def test_depth_override_is_used(self):
        s = make()
        s.update_pairs([sp('w', last=None, order=0, depth=5), sp('x', last=None, order=1, depth=1)],
                       now=T0)
        assert s.due(T0 + 400) == [key('x'), key('w')]

    def test_ties_by_config_order(self):
        s = make()
        s.update_pairs([sp('b', last=None, order=2), sp('a', last=None, order=5),
                        sp('c', last=None, order=1)], now=T0)
        assert s.due(T0 + 400) == [key('c'), key('b'), key('a')]

    def test_ties_by_name_then_destination(self):
        s = make()
        s.update_pairs([sp('b', 'Z', last=None), sp('b', 'Y', last=None), sp('a', 'Z', last=None)],
                       now=T0)
        assert s.due(T0 + 400) == [key('a', 'Z'), key('b', 'Y'), key('b', 'Z')]

    def test_an_ancestor_never_ranks_behind_its_due_descendant(self):
        fr = {('p', 'A'): 0.9, ('p/c', 'A'): 0.1, ('u', 'A'): 0.3}
        s = make(fracs=fr)
        s.update_pairs([sp('p', last=None), sp('p/c', last=None, order=1),
                        sp('u', last=None, order=2)], now=T0)
        s.confirmed(('p', 'A'))
        # u is due (90 s) after p/c (30 s) but before p (270 s): without the ancestor rule the
        # order would be p/c, u, p. The ancestor takes its due descendant's rank and goes first.
        assert s.due(T0 + 400) == [key('p'), key('p/c'), key('u')]
        assert s.dispatch(now=T0 + 400) == [key('p'), key('u')]

    def test_rank_inheritance_is_per_destination(self):
        fr = {('p', 'A'): 0.9, ('p/c', 'B'): 0.1, ('u', 'A'): 0.3}
        s = make(fracs=fr)
        s.update_pairs([sp('p', 'A', last=None), sp('p/c', 'B', last=None, order=1),
                        sp('u', 'A', last=None, order=2)], now=T0)
        assert s.due(T0 + 400) == [key('p/c', 'B'), key('u', 'A'), key('p', 'A')]


# ----------------------------------------------------------------------------
# R-2
# ----------------------------------------------------------------------------

class TestParentFirst:
    def _pair(self, **kw):
        s = make(**kw)
        s.update_pairs([sp('p', last=None), sp('p/c', last=None, order=1)], now=T0)
        return s

    def test_children_are_held_until_the_ancestor_is_confirmed(self):
        s = self._pair()
        assert s.held_by(('p/c', 'A')) == key('p')
        assert s.held_by(('p', 'A')) is None
        now = T0 + 400
        assert s.due(now) == [key('p')]
        assert s.dispatch(now=now) == [key('p')]
        s.started(('p', 'A'), now=now)
        s.finished(('p', 'A'), 'ok', now=now + 1)           # OK is not "confirmed"
        assert s.held_by(('p/c', 'A')) == key('p')
        assert key('p/c') not in s.due(now + 1)
        s.confirmed(('p', 'A'))
        assert s.held_by(('p/c', 'A')) is None
        assert s.dispatch(now=now + 1) == [key('p/c')]

    def test_every_child_pair_is_held_configured_or_expanded(self):
        # the schedule cannot tell them apart: any pair under another pair is a child
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/a', last=FRESH, order=1),
                        sp('p/b/c', last=FRESH, order=2)], now=T0)
        assert s.held_by(('p/a', 'A')) == key('p')
        assert s.held_by(('p/b/c', 'A')) == key('p')      # no pair p/b: nearest pair is p

    def test_holds_are_transitive(self):
        s = make()
        s.update_pairs([sp('p', last=None), sp('p/a', last=None, order=1),
                        sp('p/a/b', last=None, order=2)], now=T0)
        assert s.held_by(('p/a/b', 'A')) == key('p/a')    # nearest
        assert s.held_by(('p/a', 'A')) == key('p')
        now = T0 + 400
        assert s.due(now) == [key('p')]
        s.confirmed(('p', 'A'))
        assert s.held_by(('p/a', 'A')) is None
        assert s.held_by(('p/a/b', 'A')) == key('p/a')    # still held by the middle pair
        assert s.due(now) == [key('p'), key('p/a')]
        s.confirmed(('p/a', 'A'))
        assert s.held_by(('p/a/b', 'A')) is None
        assert key('p/a/b') in s.due(now)

    def test_nearest_ancestor_pair_is_the_hold_not_a_gap(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/x/y', last=FRESH, order=1)], now=T0)
        assert s.held_by(('p/x/y', 'A')) == key('p')

    def test_other_destinations_do_not_hold(self):
        s = make()
        s.update_pairs([sp('p', 'A', last=FRESH), sp('p/c', 'B', last=FRESH, order=1)], now=T0)
        assert s.held_by(('p/c', 'B')) is None
        assert s.wants_confirmation(('p', 'A')) is False      # no children on A

    def test_held_pairs_are_absent_from_due_dispatch_and_wakeup(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=None, order=1)], now=T0)
        # p/c's catch-up would be at T0+150; p is due now (confirmation) and is running
        s.started(('p', 'A'), now=T0)
        assert s.due(T0 + 400) == []
        assert s.dispatch(now=T0 + 400) == []
        assert s.next_wakeup(T0) is None                       # p runs; p/c is held
        assert s.due_time(('p/c', 'A'), T0) is not None         # the raw due time still exists

    def test_unconfirmed_ancestor_with_children_is_due_once_per_incarnation(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        assert s.wants_confirmation(('p', 'A')) is True
        assert s.due(T0) == [key('p')]               # a fresh anchor, yet due: confirmation
        assert s.dispatch(now=T0) == [key('p')]
        s.started(('p', 'A'), now=T0)
        s.finished(('p', 'A'), 'not_due', now=T0 + 1)
        # asked once; not forced again, but children still wait
        assert s.due(T0 + 2) == []
        assert s.wants_confirmation(('p', 'A')) is True
        assert s.held_by(('p/c', 'A')) == key('p')
        # a later update_pairs (a discovery) does not re-arm the one-shot
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1),
                        sp('p/d', last=FRESH, order=2)], now=T0 + 5)
        assert s.due(T0 + 5) == []
        # the regular slot still comes
        assert s.due(T0 + OFFSET) == [key('p')]
        s.confirmed(('p', 'A'))
        assert s.wants_confirmation(('p', 'A')) is False
        assert s.is_confirmed(('p', 'A')) is True

    def test_wants_confirmation_stays_true_across_failures_until_confirmed(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        cycle(s, ('p', 'A'), T0, 'failed')
        assert s.wants_confirmation(('p', 'A')) is True
        cycle(s, ('p', 'A'), T0 + H, 'ok')
        assert s.wants_confirmation(('p', 'A')) is True
        s.confirmed(('p', 'A'))
        assert s.wants_confirmation(('p', 'A')) is False

    def test_confirmation_is_due_without_a_catch_up_delay(self):
        s = make(frac=0.9)
        s.update_pairs([sp('p', last=None), sp('p/c', last=None, order=1)], now=T0)
        assert s.due_time(('p', 'A'), T0) == T0          # not T0 + 270

    def test_a_pair_without_children_does_not_want_confirmation(self):
        s = make()
        s.update_pairs([sp('a', last=FRESH)], now=T0)
        assert s.wants_confirmation(('a', 'A')) is False
        assert s.due(T0) == []
        assert s.wants_confirmation(('zz', 'A')) is False

    def test_a_leaf_child_does_not_want_confirmation(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        assert s.wants_confirmation(('p/c', 'A')) is False

    def test_a_middle_pair_wants_confirmation_for_its_own_children(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/a', last=FRESH, order=1),
                        sp('p/a/b', last=FRESH, order=2)], now=T0)
        assert s.wants_confirmation(('p/a', 'A')) is True
        assert s.due(T0) == [key('p')]               # p/a is held until p confirms
        s.confirmed(('p', 'A'))
        assert s.due(T0) == [key('p/a')]             # p is settled; p/a is asked, once
        assert s.due_time(('p/a', 'A'), T0) == T0

    def test_children_added_after_the_parent_ran_make_it_due_again_once(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH)], now=T0)
        cycle(s, ('p', 'A'), T0 + OFFSET)              # ran with no children
        assert s.wants_confirmation(('p', 'A')) is False
        t = T0 + OFFSET + 100
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=t)
        assert s.wants_confirmation(('p', 'A')) is True
        assert s.due(t) == [key('p')]
        cycle(s, ('p', 'A'), t)
        assert s.due(t + 1) == []

    def test_waiting_on(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/a', last=FRESH, order=1),
                        sp('p/b', last=FRESH, order=2), sp('q', 'B', last=FRESH, order=3),
                        sp('q/x', 'B', last=FRESH, order=4)], now=T0)
        assert s.waiting_on() == {
            key('p'): [key('p/a'), key('p/b')],
            key('q', 'B'): [key('q/x', 'B')],
        }
        s.confirmed(('p', 'A'))
        assert s.waiting_on() == {key('q', 'B'): [key('q/x', 'B')]}
        s.confirmed(('q', 'B'))
        assert s.waiting_on() == {}

    def test_waiting_on_transitive(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/a', last=FRESH, order=1),
                        sp('p/a/b', last=FRESH, order=2)], now=T0)
        assert s.waiting_on() == {key('p'): [key('p/a')], key('p/a'): [key('p/a/b')]}

    def test_removing_the_ancestor_releases_the_children(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=None, order=1)], now=T0)
        assert s.held_by(('p/c', 'A')) == key('p')
        s.update_pairs([sp('p/c', last=None, order=1)], now=T0)
        assert s.held_by(('p/c', 'A')) is None
        assert s.due_time(('p/c', 'A'), T0) is not None
        assert s.waiting_on() == {}

    def test_removal_drops_the_confirmation(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        s.confirmed(('p', 'A'))
        assert s.held_by(('p/c', 'A')) is None
        s.update_pairs([sp('p/c', last=FRESH, order=1)], now=T0)
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        assert s.is_confirmed(('p', 'A')) is False
        assert s.held_by(('p/c', 'A')) == key('p')
        assert s.wants_confirmation(('p', 'A')) is True

    def test_confirmation_survives_an_unchanged_pair_set_update(self):
        s = make()
        specs = [sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)]
        s.update_pairs(specs, now=T0)
        s.confirmed(('p', 'A'))
        s.update_pairs(specs + [sp('p/d', last=FRESH, order=2)], now=T0 + 5)
        assert s.is_confirmed(('p', 'A'))
        assert s.held_by(('p/d', 'A')) is None

    def test_slash_prefix_trap(self):
        s = make()
        s.update_pairs([sp('pool/a', last=FRESH), sp('pool/ab', last=FRESH, order=1),
                        sp('pool/a/x', last=FRESH, order=2), sp('pool', last=FRESH, order=3)],
                       now=T0)
        assert s.held_by(('pool/ab', 'A')) == key('pool')         # not pool/a
        assert s.held_by(('pool/a', 'A')) == key('pool')
        assert s.held_by(('pool/a/x', 'A')) == key('pool/a')
        assert s.wants_confirmation(('pool/a', 'A')) is True
        s.confirmed(('pool', 'A'))
        assert s.held_by(('pool/ab', 'A')) is None
        assert s.waiting_on() == {key('pool/a'): [key('pool/a/x')]}

    def test_confirmed_is_idempotent_and_ignores_unknown_pairs(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        s.confirmed(('p', 'A'))
        s.confirmed(('p', 'A'))
        s.confirmed(('nope', 'A'))
        assert s.is_confirmed(('p', 'A'))
        assert not s.is_confirmed(('nope', 'A'))
        assert s.held_by(('nope', 'A')) is None

    def test_confirming_while_the_confirmation_runs_releases_children_at_once(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=None, order=1)], now=T0)
        s.started(('p', 'A'), now=T0 + 400)
        s.confirmed(('p', 'A'))
        # the parent still runs: the child is released but excluded by the ancestor running
        assert s.dispatch(now=T0 + 400) == []
        s.finished(('p', 'A'), 'ok', now=T0 + 401)
        assert s.dispatch(now=T0 + 401) == [key('p/c')]

    def test_no_stagger_when_children_are_released(self):
        # a released child is due at its own (already passed) time, not delayed again
        s = make(frac=0.9)
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        now = T0 + s.offset(('p', 'A'))
        s.confirmed(('p', 'A'))
        assert s.due(now) == [key('p'), key('p/c')]
        # released at its own slot time: not pushed back by a catch-up delay
        assert s.due_time(('p/c', 'A'), now) == now


class TestUnconfirmedAndRearm:
    """B1 review fixes: `unconfirmed(key)`, the re-arm of an unconfirmed parent with children after a
    NOT_DUE / STOPPED / WAITING finish (never hot), and the unstaggered release of a long-held child."""

    def _tree(self, **kw):
        s = make(**kw)
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        return s

    @staticmethod
    def _never_started(**kw):
        """A parent with no start on record: a re-armed confirmation is due at once."""
        s = make(**kw)
        s.update_pairs([sp('p', last=None), sp('p/c', last=None, order=1)], now=T0)
        return s

    # -- unconfirmed ---------------------------------------------------------------------------

    def test_unconfirmed_holds_the_children_again(self):
        s = self._tree()
        s.confirmed(('p', 'A'))
        assert s.held_by(('p/c', 'A')) is None
        s.unconfirmed(('p', 'A'), now=T0)
        assert s.is_confirmed(('p', 'A')) is False
        assert s.held_by(('p/c', 'A')) == key('p')
        assert s.waiting_on() == {key('p'): [key('p/c')]}

    def test_unconfirmed_rearms_the_confirmation_of_a_parent_with_children(self):
        s = self._never_started()
        s.confirmed(('p', 'A'))
        assert s.wants_confirmation(('p', 'A')) is False
        s.unconfirmed(('p', 'A'), now=T0)
        assert s.wants_confirmation(('p', 'A')) is True
        assert s.due(T0) == [key('p')], 'due at once: no start on record'

    def test_unconfirmed_after_a_recent_attempt_waits_out_the_recheck_spacing(self):
        s = self._tree()                       # last attempt T0 - 10
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0)
        assert s.due(T0) == [] and s.due(T0 - 10 + GAP - 1) == []
        assert s.due(T0 - 10 + GAP) == [key('p')]

    def test_unconfirmed_is_due_no_earlier_than_the_recheck_gap_after_the_last_start(self):
        """The gap is `max(WAIT_RECHECK, check_interval)`: 300 s here, not 60 s."""
        s = self._tree()
        cycle(s, ('p', 'A'), T0 + 10)
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0 + 20)
        assert s.due(T0 + 20) == [] and s.due(T0 + 10 + WAIT_RECHECK) == []
        assert s.due(T0 + 10 + GAP - 1) == []
        assert s.due(T0 + 10 + GAP) == [key('p')]

    def test_unconfirmed_is_due_no_earlier_than_a_pending_retry(self):
        s = make()
        s.update_pairs([sp('p', F=D, last=FRESH), sp('p/c', F=D, last=FRESH, order=1)], now=T0)
        cycle(s, ('p', 'A'), T0 + 10, 'failed')                  # retry due at +1 h
        retry_at = T0 + 10 + FAILURE_RETRY_CAP
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0 + 20)
        assert s.due(T0 + 100) == [] and s.due(retry_at - 1) == []
        assert s.due_time(('p', 'A'), T0 + 100) == retry_at
        assert s.due(retry_at) == [key('p')]

    def test_unconfirmed_without_children_arms_nothing(self):
        s = make()
        s.update_pairs([sp('a', last=FRESH)], now=T0)
        s.confirmed(('a', 'A'))
        s.unconfirmed(('a', 'A'), now=T0)
        assert s.is_confirmed(('a', 'A')) is False
        assert s.wants_confirmation(('a', 'A')) is False
        assert s.due(T0) == []

    def test_unconfirmed_is_idempotent_and_ignores_unknown_pairs(self):
        s = self._never_started()
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0)
        s.unconfirmed(('p', 'A'), now=T0)
        s.unconfirmed(('nope', 'A'), now=T0)
        assert s.held_by(('p/c', 'A')) == key('p') and s.due(T0) == [key('p')]

    def test_unconfirmed_uses_the_injected_clock_when_no_time_is_given(self):
        clock = Clock(T0 + 500)
        s = SendSchedule('c1', 300, fraction=lambda *a: 0.5, clock=clock)
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'))
        assert s.due() == [key('p')]

    def test_a_reconfirmation_after_unconfirmed_releases_the_children_again(self):
        s = self._tree()
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0)
        s.confirmed(('p', 'A'))
        assert s.held_by(('p/c', 'A')) is None and s.wants_confirmation(('p', 'A')) is False

    # -- finished re-arms an unconfirmed parent with children ---------------------------------------

    @pytest.mark.parametrize('kind, gap', [
        ('ok', GAP), ('not_due', GAP), ('stopped', GAP),
        # WAITING also sets its own retry, `WAIT_RECHECK` after the finish: that one is due first
        ('waiting', WAIT_RECHECK + 1),
    ])
    def test_an_unconfirmed_parent_that_confirmed_nothing_is_asked_again_after_the_gap(self, kind, gap):
        """`max(WAIT_RECHECK, check_interval)` after the start (an OK that confirmed nothing, a run
        that found no snapshot yet, included); a WAITING finish also sets its own recheck, a minute
        after the finish."""
        s = self._tree()
        s.started(('p', 'A'), now=T0)
        assert s.wants_confirmation(('p', 'A')) is True
        s.finished(('p', 'A'), kind, now=T0 + 1)
        assert s.due(T0 + 2) == [], 'not hot'
        assert s.due(T0 + gap - 1) == []
        assert s.due(T0 + gap) == [key('p')]
        assert s.held_by(('p/c', 'A')) == key('p')

    def test_an_ok_that_confirmed_nothing_rearms_the_confirmation(self):
        s = self._tree()
        s.started(('p', 'A'), now=T0)
        assert s.finished(('p', 'A'), 'ok', now=T0 + 1) is True     # OK still records
        assert s.wants_confirmation(('p', 'A')) is True
        assert s.due_time(('p', 'A'), T0 + 2) == T0 + GAP

    def test_an_ok_followed_by_confirmed_leaves_nothing_due_early(self):
        """The caller's `confirmed()` after `finished('ok')` clears the re-arm."""
        s = self._tree()
        s.started(('p', 'A'), now=T0)
        s.finished(('p', 'A'), 'ok', now=T0 + 1)
        s.confirmed(('p', 'A'))
        assert s.wants_confirmation(('p', 'A')) is False
        assert s.held_by(('p/c', 'A')) is None
        for t in (T0 + 2, T0 + WAIT_RECHECK, T0 + GAP, T0 + 2 * GAP):
            assert s.due(t) == [] and key('p') not in s.dispatch(now=t)
        assert s.due_time(('p', 'A'), T0 + 2) == T0 + OFFSET        # only its next regular slot

    @pytest.mark.parametrize('kind', ['ok', 'not_due', 'stopped'])
    def test_with_a_small_check_interval_the_gap_is_still_a_minute(self, kind):
        s = self._tree(ci=10)
        s.started(('p', 'A'), now=T0)
        s.finished(('p', 'A'), kind, now=T0 + 1)
        assert s.due(T0 + WAIT_RECHECK - 1) == []
        assert s.due(T0 + WAIT_RECHECK) == [key('p')]

    @pytest.mark.parametrize('ci', [10, 60, 300, 900])
    def test_the_gap_is_the_larger_of_the_recheck_and_the_check_interval(self, ci):
        s = self._tree(ci=ci)
        s.started(('p', 'A'), now=T0)
        s.finished(('p', 'A'), 'ok', now=T0 + 1)
        assert s.due_time(('p', 'A'), T0 + 2) == T0 + max(WAIT_RECHECK, ci)

    def test_a_rearm_is_no_earlier_than_the_retry_of_a_failed_run_whatever_the_gap(self):
        """A failure's backoff (1 h) still governs a later `unconfirmed`, the gap being far shorter."""
        s = make()
        s.update_pairs([sp('p', F=D, last=FRESH), sp('p/c', F=D, last=FRESH, order=1)], now=T0)
        cycle(s, ('p', 'A'), T0 + 10, 'failed')
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0 + 20)
        assert s.due_time(('p', 'A'), T0 + 20) == T0 + 10 + FAILURE_RETRY_CAP

    @pytest.mark.parametrize('kind', ['ok', 'not_due', 'stopped', 'waiting'])
    def test_it_is_never_a_hot_loop(self, kind):
        """A sender that keeps answering "nothing" is asked at most once per `WAIT_RECHECK`
        (once per `GAP` unless the finish sets its own, shorter retry: WAITING)."""
        s = self._tree()
        starts = []
        now = T0
        for _ in range(6):
            t = s.due_time(('p', 'A'), now)
            now = max(now, t)
            assert s.due(now) == [key('p')]
            s.started(('p', 'A'), now=now)
            starts.append(now)
            s.finished(('p', 'A'), kind, now=now + 1)
            now += 1
        assert all(b - a >= WAIT_RECHECK for a, b in zip(starts, starts[1:]))

    def test_a_confirmed_parent_is_not_rearmed(self):
        s = self._tree()
        s.started(('p', 'A'), now=T0)
        s.confirmed(('p', 'A'))
        s.finished(('p', 'A'), 'not_due', now=T0 + 1)
        assert s.due(T0 + 2 * WAIT_RECHECK) == []

    def test_a_parent_without_children_is_not_rearmed(self):
        s = make()
        s.update_pairs([sp('a', last=FRESH)], now=T0)
        s.started(('a', 'A'), now=T0)
        s.finished(('a', 'A'), 'not_due', now=T0 + 1)
        assert s.due(T0 + 2 * WAIT_RECHECK) == []

    def test_a_waiting_finish_keeps_its_own_recheck_spacing(self):
        s = self._tree()
        s.started(('p', 'A'), now=T0)
        s.finished(('p', 'A'), 'waiting', now=T0 + 30)
        assert s.due(T0 + 30 + WAIT_RECHECK - 1) == []
        assert s.due(T0 + 30 + WAIT_RECHECK) == [key('p')]

    def test_a_failure_is_not_rearmed_by_this_rule_its_backoff_governs(self):
        s = self._tree()
        s.started(('p', 'A'), now=T0)
        assert s.finished(('p', 'A'), 'failed', now=T0 + 1) is True
        assert s.due(T0 + WAIT_RECHECK) == [], 'a failure waits for its own retry'

    def test_the_rearm_survives_the_unstall_that_runs_on_every_query(self):
        """`_unstall` must not cancel the spacing: queried between the finish and the due time, the
        confirmation is still exactly `GAP` after the start."""
        s = self._tree()
        s.started(('p', 'A'), now=T0)
        s.finished(('p', 'A'), 'not_due', now=T0 + 1)
        for t in (T0 + 2, T0 + 10, T0 + 30, T0 + 59, T0 + 299):
            assert s.due_time(('p', 'A'), t) == T0 + GAP
            assert s.due(t) == []

    def test_a_clock_step_back_pulls_a_far_confirmation_to_the_clamped_retry(self):
        """The confirm clamp is `confirm_at - now > FAILURE_RETRY_CAP`; with a retry pending (itself
        clamped to `now + FAILURE_RETRY_CAP` in the same pass) the confirmation goes to that retry,
        so a failure's backoff still governs."""
        s = make()
        s.update_pairs([sp('p', F=D, last=FRESH), sp('p/c', F=D, last=FRESH, order=1)], now=T0)
        cycle(s, ('p', 'A'), T0 + 100, 'failed')                 # retry_at = T0 + 100 + 1 h
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0 + 101)                  # confirm_at = the retry time
        back = T0 + 101 - 2 * H                                   # the clock steps back two hours
        assert s.due_time(('p', 'A'), back) == back + FAILURE_RETRY_CAP
        assert s.due(back) == [] and s.due(back + FAILURE_RETRY_CAP) == [key('p')]

    def test_a_clock_step_back_leaves_the_confirmation_at_the_retry_never_before_now(self):
        """`max(now, retry_at)`: a clamped retry in the future is the bound, never an earlier time."""
        s = make()
        s.update_pairs([sp('p', F=D, last=FRESH), sp('p/c', F=D, last=FRESH, order=1)], now=T0)
        cycle(s, ('p', 'A'), T0 + 100, 'failed')
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0 + 101)
        for back_by in (2 * H, 5 * H, 2 * D):
            probe = T0 + 101 - back_by
            t = s.due_time(('p', 'A'), probe)
            assert t == probe + FAILURE_RETRY_CAP and t > probe

    def test_a_clock_step_back_pulls_a_far_confirmation_to_now_when_no_retry_is_pending(self):
        s = make()
        s.update_pairs([sp('p', F=D, last=FRESH), sp('p/c', F=D, last=FRESH, order=1)], now=T0)
        s.started(('p', 'A'), now=T0 + 100)
        s.finished(('p', 'A'), 'not_due', now=T0 + 101)           # confirm_at = T0 + 100 + GAP, no retry
        assert s.due_time(('p', 'A'), T0 + 102) == T0 + 100 + GAP
        back = T0 + 101 - 2 * H
        assert s.due_time(('p', 'A'), back) == back               # no backoff to honour: due now
        assert s.due(back) == [key('p')]

    def test_a_confirmation_within_the_cap_is_not_shortened_by_a_small_step_back(self):
        s = self._tree()
        s.started(('p', 'A'), now=T0 + 100)
        s.finished(('p', 'A'), 'not_due', now=T0 + 101)
        assert s.due_time(('p', 'A'), T0 + 50) == T0 + 100 + GAP

    def test_a_confirmation_exactly_at_the_cap_ahead_is_not_clamped(self):
        """The test is `confirm_at - now > FAILURE_RETRY_CAP`: equal is a legitimate delay."""
        s = make()
        s.update_pairs([sp('p', F=D, last=FRESH), sp('p/c', F=D, last=FRESH, order=1)], now=T0)
        cycle(s, ('p', 'A'), T0 + 100, 'failed')                  # retry_at = confirm_at = T0 + 3700
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0 + 101)
        assert s.due_time(('p', 'A'), T0 + 100) == T0 + 100 + FAILURE_RETRY_CAP

    def test_a_far_confirmation_end_to_end_with_a_retry_pending_goes_to_the_retry_not_now(self):
        """The release it protects: after a failure and a big step back, the retry is not
        pre-empted by an immediate confirmation."""
        s = make()
        s.update_pairs([sp('p', F=D, last=FRESH), sp('p/c', F=D, last=FRESH, order=1)], now=T0)
        cycle(s, ('p', 'A'), T0 + 100, 'failed')
        s.confirmed(('p', 'A'))
        s.unconfirmed(('p', 'A'), now=T0 + 101)
        back = T0 - 3 * D
        assert key('p') not in s.dispatch(now=back)

    # -- released children are not staggered -----------------------------------------------------

    def test_a_child_released_after_a_long_hold_is_due_at_once(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1),
                        sp('q', last=FRESH, order=2)], now=T0)
        assert s.due(T0 + 10) == [key('p')]               # the hold is seen (and remembered)
        now = T0 + 3 * H + OFFSET + 500                    # three slots later, 500 s past the slot
        s.confirmed(('p', 'A'))
        assert key('p/c') in s.due(now)
        assert s.due_time(('p/c', 'A'), now) <= now
        assert key('p/c') in s.dispatch(now=now)

    def test_a_pair_that_was_never_held_is_still_caught_up_after_an_outage(self):
        """The contrast: the same outage staggers a pair R-2 never held (frac 0.5 -> 150 s)."""
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1),
                        sp('q', last=FRESH, order=2)], now=T0)
        now = T0 + 3 * H + OFFSET + 500
        assert s.due_time(('q', 'A'), now) == now + 0.5 * 300
        assert key('q') not in s.due(now)

    def test_a_hold_that_ends_inside_the_attempted_slot_changes_nothing(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        s.due(T0 + 10)
        s.confirmed(('p', 'A'))
        assert s.due(T0 + 20) == []                        # nothing was missed: its slot is attempted

    def test_a_released_child_is_still_subject_to_the_caps(self):
        s = make(max_senders=1)
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1),
                        sp('u', last=FRESH, order=2)], now=T0)
        s.due(T0 + 10)
        now = T0 + 3 * H + OFFSET + 500
        s.confirmed(('p', 'A'))
        s.started(('u', 'A'), now=now)
        assert s.dispatch(now=now) == []                   # due, but the one sender slot is taken
        s.finished(('u', 'A'), 'ok', now=now + 1)
        assert key('p/c') in s.dispatch(now=now + 1)

    def test_a_child_whose_ancestor_left_the_pair_set_is_released_unstaggered_too(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        s.due(T0 + 10)
        now = T0 + 3 * H + OFFSET + 500
        s.update_pairs([sp('p/c', last=FRESH, order=1)], now=now)
        assert key('p/c') in s.due(now)
        assert s.due_time(('p/c', 'A'), now) <= now


class TestConfirmationRunThatFoundNothing:
    """End to end with the real class: a new parent whose first confirmation run is OK but confirmed
    nothing (no snapshot yet). Before the fix the one-shot confirmation stayed spent and the child
    waited for the parent's next regular slot, up to P (~P - F, 82800 s here)."""

    @staticmethod
    def _world(ci=CI):
        """F = 1 h, P = 1 d, a new parent with one child, first seen 400 s after its first slot.
        Returns (schedule, the time of the parent's first run)."""
        s = make(ci=ci)
        base = T0 + slot_offset(0.5, H, D, ci) + 400
        s.update_pairs([sp('p', F=H, P=D, last=None), sp('p/c', F=H, P=D, last=None, order=1)], now=base)
        return s, base + 300                                     # past the catch-up (frac * 300)

    @staticmethod
    def _run(s, name, now, outcome, confirmed):
        s.started((name, 'A'), now=now)
        s.finished((name, 'A'), outcome, now=now + 1)
        if confirmed:
            s.confirmed((name, 'A'))

    @pytest.mark.parametrize('ci', [10, 60, 300, 900])
    def test_the_child_is_released_within_the_gap_after_a_later_confirmed_run(self, ci):
        s, now = self._world(ci)
        gap = max(WAIT_RECHECK, ci)
        assert s.dispatch(now=now) == [key('p')]                 # the confirmation run
        self._run(s, 'p', now, 'ok', confirmed=False)            # OK, but no snapshot yet
        first_start = now
        assert s.held_by(('p/c', 'A')) == key('p')
        assert key('p/c') not in s.due(now + 2)

        wake = s.next_wakeup(now + 1)
        assert wake is not None and wake - first_start <= gap, (wake, first_start, gap)
        assert s.dispatch(now=wake - 1) == []                    # nothing earlier
        assert s.dispatch(now=wake) == [key('p')]
        self._run(s, 'p', wake, 'ok', confirmed=True)            # now the copy is confirmed

        released = wake + 2
        assert s.held_by(('p/c', 'A')) is None
        assert key('p/c') in s.dispatch(now=released)
        assert released - first_start <= gap + 2, 'not ~P - F later'

    def test_the_wait_the_fix_removes_was_the_regular_slot_about_a_period_away(self):
        """A confirmed run's own next regular slot is far beyond the re-arm gap."""
        s, now = self._world()
        self._run(s, 'p', now, 'ok', confirmed=True)
        assert s.due_time(('p', 'A'), now + 2) - now > 10 * GAP

    def test_the_unconfirmed_parent_is_due_again_at_the_gap_not_at_its_next_slot(self):
        s, now = self._world()
        self._run(s, 'p', now, 'ok', confirmed=False)
        assert s.due_time(('p', 'A'), now + 2) == now + GAP

    def test_it_keeps_asking_at_the_gap_while_nothing_is_confirmed(self):
        s, now = self._world()
        starts = []
        for _ in range(5):
            now = max(now, s.due_time(('p', 'A'), now))
            assert s.dispatch(now=now) == [key('p')]
            starts.append(now)
            self._run(s, 'p', now, 'ok', confirmed=False)
            now += 1
        assert all(b - a == GAP for a, b in zip(starts, starts[1:]))
        assert s.held_by(('p/c', 'A')) == key('p')


class TestRegridKeepsTheRetry:
    def test_a_regrid_keeps_a_pending_retry_and_its_burst_count(self):
        s = make()
        s.update_pairs([sp('a', F=H, last=FRESH)], now=T0)
        s.started(('a', 'A'), now=T0 + 10)
        assert s.finished(('a', 'A'), 'retryable', now=T0 + 10) is False
        retry_at = T0 + 10 + RETRY_DELAYS[0]
        assert s.due_time(('a', 'A'), T0 + 11) == retry_at
        # P changes, so the pair's grid (its offset) does: `_init_state` runs again
        s.update_pairs([sp('a', F=H, P=2 * H, last=FRESH)], now=T0 + 20)
        assert key('a') in s.due(retry_at), 'the retry is still pending'
        # the burst continues where it was: the second retryable failure waits the SECOND delay
        s.started(('a', 'A'), now=retry_at)
        assert s.finished(('a', 'A'), 'retryable', now=retry_at) is False
        assert s.due_time(('a', 'A'), retry_at + 1) == retry_at + RETRY_DELAYS[1]

    def test_the_burst_runs_out_after_the_same_number_of_failures_across_a_regrid(self):
        s = make()
        s.update_pairs([sp('a', F=H, last=FRESH)], now=T0)
        t = T0 + 10
        for i in range(len(RETRY_DELAYS)):
            s.started(('a', 'A'), now=t)
            assert s.finished(('a', 'A'), 'retryable', now=t) is False
            if i == 0:
                s.update_pairs([sp('a', F=H, P=2 * H, last=FRESH)], now=t + 1)
            t = s.due_time(('a', 'A'), t + 1)
        s.started(('a', 'A'), now=t)
        assert s.finished(('a', 'A'), 'retryable', now=t) is True, 'spent: recorded once'


# ----------------------------------------------------------------------------
# Discovery
# ----------------------------------------------------------------------------

class TestDiscovery:
    def test_a_new_parent_is_due_at_once_in_config_order(self):
        s = make()
        s.set_discovery_parents(['p', 'q', 'r'], now=T0)
        assert s.discovery_due(T0) == ['p', 'q', 'r']

    def test_one_in_flight_per_parent(self):
        s = make()
        s.set_discovery_parents(['p', 'q'], now=T0)
        s.discovery_started('p', now=T0)
        assert s.discovery_due(T0) == ['q']
        with pytest.raises(ValueError):
            s.discovery_started('p', now=T0)
        assert s.discovery_due(T0 + 10 * H) == ['q']
        s.discovery_finished('p', True, now=T0 + 10)
        assert 'p' not in s.discovery_due(T0 + 10)

    def test_unknown_parent_start_is_refused(self):
        s = make()
        with pytest.raises(KeyError):
            s.discovery_started('nope')

    def test_max_discoveries_cap(self):
        assert MAX_DISCOVERIES == 4
        s = make()
        names = [f'p{i}' for i in range(7)]
        s.set_discovery_parents(names, now=T0)
        assert s.discovery_due(T0) == names[:4]
        s.discovery_started('p0', now=T0)
        s.discovery_started('p1', now=T0)
        assert s.discovery_due(T0) == ['p2', 'p3']
        s.discovery_started('p2', now=T0)
        s.discovery_started('p3', now=T0)
        assert s.discovery_due(T0) == []
        s.discovery_finished('p0', True, now=T0 + 1)
        assert s.discovery_due(T0 + 1) == ['p4']          # one slot freed

    def test_never_discovered_first_then_longest_overdue(self):
        s = make()
        s.set_discovery_parents(['p', 'q', 'r'], now=T0)
        for n in ('p', 'q', 'r'):
            s.discovery_started(n, now=T0)
        s.discovery_finished('r', True, now=T0 + 100)       # next at 100 + 300
        s.discovery_finished('p', True, now=T0 + 200)       # next at 200 + 300
        s.discovery_finished('q', False, now=T0 + 300)      # retry at 300 + 300 (min(300, 300))
        # all overdue: the one overdue longest first
        assert s.discovery_due(T0 + 2000) == ['r', 'p', 'q']

    def test_a_never_discovered_parent_goes_before_discovered_ones(self):
        s = make()
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', True, now=T0 + 10)
        s.set_discovery_parents(['p', 'q'], now=T0 + 20)     # q is new
        assert s.discovery_due(T0 + 20) == ['q']
        # q (never discovered) has been due since T0 + 20, p only since T0 + 310
        assert s.discovery_due(T0 + 10 * H) == ['q', 'p']

    def test_interval_default_is_the_check_interval(self):
        s = make(ci=300)
        assert s.discovery_interval('p') == 300
        s2 = make(ci=7200)
        assert s2.discovery_interval('p') == 7200

    @pytest.mark.parametrize('ci, P, expected', [
        (300, 1800, 1800),             # min(P, 1 h) above the check interval
        (300, D, DISCOVERY_MAX_INTERVAL),   # capped at 1 h
        (300, 120, 300),                # never more often than the check interval
        (300, 300, 300),
        (60, 120, 120),
        (7200, 1800, 7200),             # check interval above the cap
    ])
    def test_interval_formula(self, ci, P, expected):
        s = make(ci=ci)
        s.update_pairs([sp('p', F=P, last=FRESH)], now=T0)
        assert s.discovery_interval('p') == expected

    def test_interval_uses_the_smallest_period_of_the_parents_own_pairs(self):
        s = make()
        s.update_pairs([sp('p', 'A', F=1800), sp('p', 'B', F=900, order=1),
                        sp('p/c', 'A', F=60, order=2), sp('pp', 'A', F=120, order=3)], now=T0)
        assert s.discovery_interval('p') == 900        # children and look-alikes do not count

    def test_a_success_waits_the_interval(self):
        s = make(ci=60)
        s.update_pairs([sp('p', F=1800, last=FRESH)], now=T0)
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', True, now=T0 + 5)
        assert s.discovery_due(T0 + 5 + 1799) == []
        assert s.discovery_due(T0 + 5 + 1800) == ['p']

    def test_a_failure_retries_after_min_interval_and_300(self):
        s = make(ci=60)
        s.update_pairs([sp('p', F=1800, last=FRESH)], now=T0)
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', False, now=T0 + 5)
        assert DISCOVERY_RETRY == 300
        assert s.discovery_due(T0 + 5 + 299) == []
        assert s.discovery_due(T0 + 5 + 300) == ['p']

    def test_a_failure_never_waits_longer_than_the_interval(self):
        s = make(ci=60)
        s.update_pairs([sp('p', F=120, last=FRESH)], now=T0)       # interval 120
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', False, now=T0 + 5)
        assert s.discovery_due(T0 + 5 + 119) == []
        assert s.discovery_due(T0 + 5 + 120) == ['p']

    def test_finished_clears_the_in_flight_flag_in_both_cases(self):
        for ok in (True, False):
            s = make()
            s.set_discovery_parents(['p'], now=T0)
            s.discovery_started('p', now=T0)
            s.discovery_finished('p', ok, now=T0)
            s.discovery_started('p', now=T0 + 3 * H)       # no ValueError

    def test_finished_for_an_unknown_parent_is_ignored(self):
        s = make()
        s.discovery_finished('gone', True, now=T0)
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.set_discovery_parents([], now=T0)
        s.discovery_finished('p', True, now=T0)
        assert s.discovery_due(T0) == []

    def test_dropped_parents_are_forgotten_and_kept_ones_keep_their_state(self):
        s = make()
        s.set_discovery_parents(['p', 'q'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', True, now=T0)
        s.set_discovery_parents(['q', 'p'], now=T0 + 10)       # reorder; p keeps next_at
        assert s.discovery_due(T0 + 10) == ['q']
        s.set_discovery_parents(['p'], now=T0 + 10)
        assert s.discovery_due(T0 + 10 * H) == ['p']
        s.set_discovery_parents(['q'], now=T0 + 20)
        assert s.discovery_due(T0 + 20) == ['q']               # a re-added parent is new

    def test_discovery_times_feed_next_wakeup(self):
        s = make(ci=60)
        s.update_pairs([sp('p', F=1800, last=FRESH)], now=T0)
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        s.discovery_finished('p', True, now=T0)
        wake = s.next_wakeup(T0)
        # the pair's own slot (T0 + 1980 on a 1800 grid) vs discovery at T0 + 1800
        assert wake == min(T0 + 1800, s.due_time(('p', 'A'), T0))

    def test_in_flight_discovery_is_not_a_wakeup(self):
        s = make()
        s.set_discovery_parents(['p'], now=T0)
        s.discovery_started('p', now=T0)
        assert s.next_wakeup(T0 - 1) is None


# ----------------------------------------------------------------------------
# The pair set
# ----------------------------------------------------------------------------

class TestPairSet:
    def test_add_and_remove(self):
        s = make()
        assert s.keys() == []
        s.update_pairs([sp('a'), sp('b', order=1)], now=T0)
        assert s.keys() == [key('a'), key('b')]
        s.update_pairs([sp('b', order=1), sp('c', order=2)], now=T0)
        assert s.keys() == [key('b'), key('c')]
        assert s.due_time(('a', 'A'), T0) is None
        with pytest.raises(KeyError):
            s.spec(('a', 'A'))
        assert s.spec(('c', 'A')).dataset == 'c'

    def test_keys_follow_the_given_order(self):
        s = make()
        s.update_pairs([sp('z'), sp('a', order=1), sp('m', 'B', order=2)], now=T0)
        assert s.keys() == [key('z'), key('a'), key('m', 'B')]

    def test_a_staying_pair_keeps_its_state(self):
        s = make()
        s.update_pairs([sp('a', F=D)], now=T0)
        t = T0 + s.offset(('a', 'A')) + 10
        cycle(s, ('a', 'A'), t, 'failed')                 # retry_at = t + 1 h
        s.update_pairs([sp('a', F=D, order=9, last=None)], now=t + 5)   # last_attempt ignored
        assert s.spec(('a', 'A')).config_order == 9
        assert s.due_time(('a', 'A'), t + 5) == t + FAILURE_RETRY_CAP

    def test_last_attempt_is_read_only_for_new_pairs(self):
        s = make()
        s.update_pairs([sp('a', last=FRESH)], now=T0)
        s.update_pairs([sp('a', last=None)], now=T0 + 5)
        assert s.due_time(('a', 'A'), T0 + 5) == T0 + OFFSET        # no catch-up appeared

    def test_removal_drops_retry_state_and_a_readd_is_new(self):
        s = make()
        s.update_pairs([sp('a', last=FRESH)], now=T0)
        cycle(s, ('a', 'A'), T0 + OFFSET + 5, 'retryable')
        s.update_pairs([], now=T0 + 3000)
        s.update_pairs([sp('a', last=None)], now=T0 + 3000)
        assert s.due_time(('a', 'A'), T0 + 3000) == T0 + 3000 + 150

    def test_a_changed_period_rederives_the_attempted_slot(self):
        s = make()
        s.update_pairs([sp('a', F=H)], now=T0)
        t = T0 + OFFSET + 20
        cycle(s, ('a', 'A'), t)                               # attempted on the 1 h grid
        # P becomes 1 d (F stays 1 h): offset unchanged, grid changed
        now = T0 + 5000
        s.update_pairs([sp('a', F=H, P=D)], now=now)
        assert s.offset(('a', 'A')) == OFFSET
        # last start (T0 + 2000) is inside today's daily slot (T0 + 1980): next is tomorrow's
        assert s.due_time(('a', 'A'), now) == T0 + D + OFFSET

    def test_a_changed_frequency_rederives_the_offset_and_slot(self):
        s = make()
        s.update_pairs([sp('a', F=H)], now=T0)
        t = T0 + OFFSET + 20
        cycle(s, ('a', 'A'), t)
        now = t + 50
        s.update_pairs([sp('a', F=300)], now=now)                # W = 300: offset 225
        assert s.offset(('a', 'A')) == 225
        k = slot_index(now, 300, 225)
        if t >= slot_time(k, 300, 225):
            assert s.due_time(('a', 'A'), now) == slot_time(k + 1, 300, 225)
        else:
            assert s.due_time(('a', 'A'), now) == now + 0.5 * 300

    def test_a_changed_period_with_an_older_last_start_gets_a_catch_up(self):
        s = make()
        s.update_pairs([sp('a', F=H)], now=T0)
        cycle(s, ('a', 'A'), T0 + OFFSET + 5)                    # ran at 00:33
        now = T0 + 2 * D          # long idle; period change to daily
        s.update_pairs([sp('a', F=H, P=D)], now=now)
        assert s.due_time(('a', 'A'), now) == now + 150

    def test_a_pair_never_started_regrids_with_its_fed_in_last_attempt(self):
        s = make()
        s.update_pairs([sp('a', F=H, last=FRESH)], now=T0)
        now = T0 + 5000
        s.update_pairs([sp('a', F=H, P=D, last=FRESH)], now=now)
        # FRESH (T0 - 10) is before today's daily slot at T0 + 1980: owed a catch-up
        assert s.due_time(('a', 'A'), now) == now + 150

    @pytest.mark.parametrize('kw', [
        dict(F=0), dict(F=-1), dict(F=H, P=0), dict(F=H, P=-5), dict(F=timedelta(0)),
    ])
    def test_non_positive_specs_are_refused(self, kw):
        with pytest.raises(ValueError):
            PairSpec('a', 'A', kw.pop('F'), kw.pop('P', None))

    def test_duplicate_pairs_are_refused(self):
        s = make()
        with pytest.raises(ValueError):
            s.update_pairs([sp('a'), sp('a')], now=T0)
        # same dataset on two destinations, or two datasets on one, is fine
        s.update_pairs([sp('a', 'A'), sp('a', 'B')], now=T0)

    def test_started_on_unknown_or_running_pair(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        with pytest.raises(KeyError):
            s.started(('nope', 'A'), now=T0)
        s.started(('a', 'A'), now=T0)
        with pytest.raises(ValueError):
            s.started(('a', 'A'), now=T0)
        assert s.running == {key('a')}

    def test_started_on_a_removed_running_pair_is_unknown(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        s.started(('a', 'A'), now=T0)
        s.update_pairs([], now=T0)
        with pytest.raises(KeyError):
            s.started(('a', 'A'), now=T0)

    def test_a_running_pair_is_neither_due_nor_a_wakeup(self):
        s = make()
        s.update_pairs([sp('a', last=None)], now=T0)
        s.started(('a', 'A'), now=T0 + 150)
        assert s.due(T0 + 400) == []
        assert s.due_time(('a', 'A'), T0 + 400) is None
        assert s.next_wakeup(T0 + 400) is None

    def test_a_pair_re_added_while_still_running_is_not_dispatched(self):
        s = make()
        s.update_pairs([sp('a', last=None)], now=T0)
        s.started(('a', 'A'), now=T0 + 150)
        s.update_pairs([], now=T0 + 160)
        s.update_pairs([sp('a', last=None)], now=T0 + 160)
        assert s.dispatch(now=T0 + 1000) == []
        s.finished(('a', 'A'), 'ok', now=T0 + 1000)
        assert key('a') in s.due(T0 + 1000 + 150)

    def test_accessors(self):
        s = make(frac=0.25)
        s.update_pairs([sp('a', F=H, P=D, order=3)], now=T0)
        spec = s.spec(('a', 'A'))
        assert (spec.frequency, spec.effective_period, spec.window, spec.config_order) == (H, D, H, 3)
        assert spec.key == key('a')
        assert s.fraction(('a', 'A')) == 0.25
        assert s.offset(('a', 'A')) == 360 + 0.25 * (H - 360)
        assert s.is_confirmed(('a', 'A')) is False

    def test_depth_defaults_to_the_slash_count(self):
        assert PairSpec('a/b/c', 'A', H).depth == 2
        assert PairSpec('a', 'A', H).depth == 0
        assert PairSpec('a', 'A', H, depth=7).depth == 7


# ----------------------------------------------------------------------------
# Inputs
# ----------------------------------------------------------------------------

class TestInputs:
    def test_string_kinds_equal_the_enum(self):
        for text in ('ok', 'not_due', 'failed', 'retryable', 'stopped', 'waiting'):
            s = make()
            s.update_pairs([sp('a')], now=T0)
            t = T0 + OFFSET + 5
            r1 = cycle(s, ('a', 'A'), t, text)
            s2 = make()
            s2.update_pairs([sp('a')], now=T0)
            r2 = cycle(s2, ('a', 'A'), t, Outcome(text))
            assert r1 == r2
            assert s.due_time(('a', 'A'), t) == s2.due_time(('a', 'A'), t)

    def test_timedelta_inputs(self):
        s = SendSchedule('c1', timedelta(minutes=5), fraction=lambda *a: 0.5)
        s.update_pairs([PairSpec('a', 'A', timedelta(hours=1), None, 0, None, None),
                        PairSpec('b', 'A', timedelta(days=1), timedelta(hours=1))], now=T0)
        assert s.offset(('a', 'A')) == 1980
        assert s.offset(('b', 'A')) == 1980
        assert s.spec(('a', 'A')).frequency == 3600.0
        assert s.spec(('b', 'A')).effective_period == 3600.0
        assert s.check_interval == 300.0

    def test_float_and_int_inputs_agree(self):
        a = SendSchedule('c', 300, fraction=lambda *x: 0.5)
        b = SendSchedule('c', 300.0, fraction=lambda *x: 0.5)
        a.update_pairs([PairSpec('x', 'A', 3600)], now=T0)
        b.update_pairs([PairSpec('x', 'A', 3600.0)], now=T0)
        assert a.offset(('x', 'A')) == b.offset(('x', 'A'))

    def test_plain_tuples_and_pairkeys_are_interchangeable(self):
        s = make()
        s.update_pairs([sp('a', last=None)], now=T0)
        s.started(('a', 'A'), now=T0 + 150)
        assert s.running == {PairKey('a', 'A')} == {('a', 'A')}
        s.finished(PairKey('a', 'A'), 'ok', now=T0 + 151)
        assert s.running == set()

    def test_module_exports(self):
        for name in ss.__all__:
            assert hasattr(ss, name)


# ----------------------------------------------------------------------------
# next_wakeup
# ----------------------------------------------------------------------------

class TestNextWakeup:
    def test_none_when_nothing_is_pending(self):
        assert make().next_wakeup(T0) is None

    def test_the_earliest_future_due_time(self):
        fr = {('a', 'A'): 0.2, ('b', 'A'): 0.8}
        s = make(fracs=fr)
        s.update_pairs([sp('a', last=None), sp('b', last=None, order=1)], now=T0)
        assert s.next_wakeup(T0) == T0 + 60
        assert s.next_wakeup(T0 + 60) == T0 + 240                # strictly after now
        # pairs that are due (even if never started) are not wakeups
        assert s.next_wakeup(T0 + 240) is None

    def test_regular_slots(self):
        s = make()
        s.update_pairs([sp('a'), sp('b', F=D, order=1)], now=T0)
        assert s.next_wakeup(T0) == T0 + OFFSET

    def test_due_but_blocked_pairs_are_ignored(self):
        s = make(max_per_destination=1)
        s.update_pairs([sp('a', last=None), sp('b', last=None, order=1),
                        sp('c', last=FRESH, order=2)], now=T0)
        s.started(('a', 'A'), now=T0 + 150)
        # b is due and blocked by the cap: not a wakeup; c's slot is
        assert s.next_wakeup(T0 + 400) == T0 + OFFSET
        s.update_pairs([sp('a', last=None), sp('b', last=None, order=1)], now=T0)
        assert s.next_wakeup(T0 + 400) is None

    def test_running_pairs_are_ignored(self):
        s = make()
        s.update_pairs([sp('a', last=None)], now=T0)
        s.started(('a', 'A'), now=T0)
        assert s.next_wakeup(T0 - 1) is None

    def test_held_pairs_are_ignored(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        s.started(('p', 'A'), now=T0)
        assert s.next_wakeup(T0) is None                       # p runs, p/c is held
        s.finished(('p', 'A'), 'ok', now=T0 + 1)
        s.confirmed(('p', 'A'))                                # a confirming run, as the worker feeds it
        assert s.next_wakeup(T0 + 1) == T0 + OFFSET            # p's next slot, not p/c's

    def test_an_ok_that_confirmed_nothing_wakes_for_its_rearmed_confirmation_not_for_the_child(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        s.started(('p', 'A'), now=T0)
        s.finished(('p', 'A'), 'ok', now=T0 + 1)               # unconfirmed: re-armed at start + gap
        assert s.next_wakeup(T0 + 1) == T0 + GAP

    def test_a_retry_is_a_wakeup(self):
        s = make()
        s.update_pairs([sp('a')], now=T0)
        t = T0 + OFFSET + 5
        cycle(s, ('a', 'A'), t, 'retryable')
        assert s.next_wakeup(t) == t + 60

    def test_a_due_confirmation_is_not_a_wakeup(self):
        s = make()
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1),
                        sp('z', last=FRESH, order=2)], now=T0)
        # p is due now (its confirmation), p/c is held: z's slot is the next wakeup
        assert s.next_wakeup(T0) == T0 + OFFSET
        s.update_pairs([sp('p', last=FRESH), sp('p/c', last=FRESH, order=1)], now=T0)
        assert s.next_wakeup(T0) is None


# ----------------------------------------------------------------------------
# Randomized property test (hypothesis is not a dependency of this repo)
# ----------------------------------------------------------------------------

DATASETS = ['p', 'p/a', 'p/a/b', 'p/c', 'pp', 'q', 'q/a', 'r']
DESTS = ['A', 'B', 'C']


def _related(a, b):
    """True when two datasets are ancestor/descendant (or equal)."""
    return a == b or a.startswith(b + '/') or b.startswith(a + '/')


def _random_specs(rng, now):
    specs = []
    for ds in DATASETS:
        for dest in DESTS:
            if rng.random() < 0.45:
                f = rng.choice([300, 900, H, D])
                p = rng.choice([None, 300, 900, H, D])
                last = rng.choice([None, now - 100, now - 5000, now - 3 * D])
                specs.append(PairSpec(ds, dest, f, p, len(specs), None, last))
    return specs


def _assert_running_invariants(running):
    assert len(running) <= MAX_SENDERS
    per = {}
    for k in running:
        per[k.destination] = per.get(k.destination, 0) + 1
    assert all(n <= MAX_SENDERS_PER_DESTINATION for n in per.values()), per
    ks = sorted(running)
    for i, a in enumerate(ks):
        for b in ks[i + 1:]:
            if a.destination == b.destination:
                assert not _related(a.dataset, b.dataset), (a, b)


class TestRandomizedInvariants:
    @pytest.mark.parametrize('seed', range(40))
    def test_caps_exclusion_and_single_flight_hold(self, seed):
        rng = random.Random(seed)
        t = T0
        s = SendSchedule(f'client{seed}', rng.choice([60, 300]))
        mirror = set()
        s.update_pairs(_random_specs(rng, t), now=t)
        s.set_discovery_parents(['p', 'q'], now=t)
        for step in range(300):
            roll = rng.random()
            if roll < 0.35:
                arg = sorted(mirror) if rng.random() < 0.5 else None
                chosen = s.dispatch(now=t, running=arg)
                assert len(chosen) == len(set(chosen))
                assert not (set(chosen) & mirror)
                assert set(chosen) <= set(s.due(t))
                assert all(s.held_by(c) is None for c in chosen)
                for c in chosen:
                    s.started(c, now=t)             # never raises: not already running
                    mirror.add(c)
                _assert_running_invariants(mirror)
            elif roll < 0.55 and mirror:
                k = rng.choice(sorted(mirror))
                kind = rng.choice(list(Outcome))
                s.finished(k, kind, retryable=rng.random() < 0.5, now=t)
                mirror.discard(k)
            elif roll < 0.72:
                t += rng.choice([1, 30, 300, 1800, H, 6 * H, D, -1, -600, -H, -3 * D])
            elif roll < 0.8:
                ks = s.keys()
                if ks:
                    s.confirmed(rng.choice(ks))
            elif roll < 0.88:
                s.update_pairs(_random_specs(rng, t), now=t)
            else:
                wake = s.next_wakeup(t)
                assert wake is None or wake > t
                for k in s.keys():
                    dt = s.due_time(k, t)
                    assert (dt is None) == (k in mirror)
                for parent in s.discovery_due(t):
                    s.discovery_started(parent, now=t)
                    s.discovery_finished(parent, rng.random() < 0.5, now=t)
            assert s.running == mirror
            _assert_running_invariants(mirror)
            # the unstall rule: whatever the clock did, nothing waits longer than one period
            # (or a capped catch-up) from now
            for k in s.keys():
                if k in mirror:
                    continue
                dt = s.due_time(k, t)
                limit = max(s.spec(k).effective_period, CATCH_UP_CAP)
                assert dt <= t + limit, (k, dt - t)

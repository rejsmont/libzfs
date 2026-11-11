"""`zfsbackup.sendmemory.SendMemory`: what the senders remember between sends (Track B, item B1.3).

One lock, atomic operations; the caller decides from the return value and logs outside the lock.
Pinned here:

* the semantics of every operation (latches, counters, value maps, the pending map, the daily
  expiry claim, the live views), tables addressed by name and independent of each other;
* stress with real threads (32 x 1000 operations over `note` / `incr` / `note_value` / `pending_*` /
  `claim_expiry_check`, a `threading.Barrier` so they really overlap, and a tiny switch interval so
  a missing lock shows): exactly one winner per latch key, exact counter totals and every returned
  count distinct, exactly one expiry claim per window;
* `items()` is a copy, safe to iterate while other threads write;
* pickling (the lock is recreated, the containers are handed over, aliases survive);
* a real `spawn` / `forkserver` / `fork` child that receives a pickled `RemoteBackupWorker` and uses
  its memory.

Nothing reaches `zfs`, a database or a network.

Mutation-checked (scratch copy): replacing the lock of `SendMemory` by a no-op context manager fails
5 of the stress tests here (`note` winners, `incr` totals, `note_value` winners, the expiry window
claims and the mixed run).
"""

import copy
import json
import multiprocessing
import pickle
import sys
import threading

import pytest

from _sendmemory_workers import MemoryProbeWorker, hammer
from zfsbackup.sendmemory import (
    CLOCK, COPY_CONFIRMED, DRY_RUN_SENT, EXPAND_COPY, EXPAND_ERROR, EXPAND_TARGET,
    IDENTITY_ERROR, NEGOTIATE_ZFS_ERROR, NEGOTIATE_ZFS_SKIPS, NO_CLIENT_ID, NO_TXG, PENDING,
    REFUSALS, RETENTION_WARNED, TOKEN_ERROR, TOKEN_SKIPS, WAITING, SendMemory,
)

# No module-level tier marker: the classes that start real processes are `integration`, the rest
# are marked `unit` by the hook in conftest.py (a module-level `unit` would double-count them).


@pytest.fixture
def mem():
    return SendMemory()


# ---------------------------------------------------------------------------------------------
# latches
# ---------------------------------------------------------------------------------------------


class TestLatches:
    def test_note_is_true_once_per_key(self, mem):
        assert mem.note(CLOCK, 'k') is True
        assert mem.note(CLOCK, 'k') is False
        assert mem.note(CLOCK, 'k') is False

    def test_keys_are_independent(self, mem):
        assert mem.note(CLOCK, ('a', 'b')) is True
        assert mem.note(CLOCK, ('a', 'c')) is True
        assert mem.noted(CLOCK, ('a', 'b')) and mem.noted(CLOCK, ('a', 'c'))

    def test_tables_are_independent(self, mem):
        assert mem.note(CLOCK, 'k') is True
        assert mem.note(NO_TXG, 'k') is True
        mem.clear(CLOCK, 'k')
        assert mem.noted(NO_TXG, 'k') is True

    def test_noted_reads_without_setting(self, mem):
        assert mem.noted(CLOCK, 'k') is False
        assert mem.noted(CLOCK, 'k') is False
        assert mem.note(CLOCK, 'k') is True

    def test_clear_says_whether_it_was_set(self, mem):
        assert mem.clear(CLOCK, 'k') is False
        mem.note(CLOCK, 'k')
        assert mem.clear(CLOCK, 'k') is True
        assert mem.clear(CLOCK, 'k') is False
        assert mem.noted(CLOCK, 'k') is False

    def test_a_cleared_latch_can_be_set_again(self, mem):
        mem.note(CLOCK, 'k')
        mem.clear(CLOCK, 'k')
        assert mem.note(CLOCK, 'k') is True

    def test_reset_latches_empties_one_table(self, mem):
        mem.note(CLOCK, 1), mem.note(CLOCK, 2), mem.note(NO_TXG, 1)
        mem.reset_latches(CLOCK)
        assert not mem.noted(CLOCK, 1) and not mem.noted(CLOCK, 2)
        assert mem.noted(NO_TXG, 1)

    def test_reset_latches_on_an_unknown_table_is_harmless(self, mem):
        mem.reset_latches('never-used')

    def test_forget_where_removes_the_matching_keys_and_counts_them(self, mem):
        for key in [('a', 1), ('a', 2), ('b', 1)]:
            mem.note(CLOCK, key)
        assert mem.forget_where(CLOCK, lambda k: k[0] == 'a') == 2
        assert [mem.noted(CLOCK, k) for k in [('a', 1), ('a', 2), ('b', 1)]] == [False, False, True]
        assert mem.forget_where(CLOCK, lambda k: k[0] == 'a') == 0

    def test_retain_keeps_only_the_matching_keys(self, mem):
        for key in [1, 2, 3, 4]:
            mem.note(CLOCK, key)
        mem.retain(CLOCK, lambda k: k % 2 == 0)
        assert [mem.noted(CLOCK, k) for k in [1, 2, 3, 4]] == [False, True, False, True]

    def test_retain_with_nothing_matching_empties_the_table(self, mem):
        mem.note(CLOCK, 1)
        mem.retain(CLOCK, lambda k: False)
        assert mem.latch_view(CLOCK) == set()

    def test_replace_group_makes_the_group_exactly_the_given_keys(self, mem):
        for key in [('d1', 'x'), ('d1', 'y'), ('d2', 'z')]:
            mem.note(NO_TXG, key)
        fresh = mem.replace_group(NO_TXG, lambda k: k[0] == 'd1', [('d1', 'y'), ('d1', 'w')])
        assert fresh == [('d1', 'w')], "only the key that was not set before is new"
        assert mem.latch_view(NO_TXG) == {('d1', 'y'), ('d1', 'w'), ('d2', 'z')}

    def test_replace_group_keeps_the_order_given(self, mem):
        fresh = mem.replace_group(NO_TXG, lambda k: True, [('d', 'c'), ('d', 'a'), ('d', 'b')])
        assert fresh == [('d', 'c'), ('d', 'a'), ('d', 'b')]

    def test_replace_group_with_no_keys_empties_the_group(self, mem):
        mem.note(NO_TXG, ('d1', 'x'))
        mem.note(NO_TXG, ('d2', 'x'))
        assert mem.replace_group(NO_TXG, lambda k: k[0] == 'd1', []) == []
        assert mem.latch_view(NO_TXG) == {('d2', 'x')}

    def test_replace_group_accepts_a_generator(self, mem):
        assert mem.replace_group(NO_TXG, lambda k: True, ((('d', i)) for i in range(2))) == [
            ('d', 0), ('d', 1)]

    def test_replace_group_announces_a_key_again_only_after_it_left(self, mem):
        assert mem.replace_group(NO_TXG, lambda k: k[0] == 'd', [('d', 'x')]) == [('d', 'x')]
        assert mem.replace_group(NO_TXG, lambda k: k[0] == 'd', [('d', 'x')]) == []
        mem.replace_group(NO_TXG, lambda k: k[0] == 'd', [])
        assert mem.replace_group(NO_TXG, lambda k: k[0] == 'd', [('d', 'x')]) == [('d', 'x')]

    def test_a_predicate_runs_under_the_lock(self, mem):
        """`forget_where` / `retain` / `replace_group` predicates run with the lock held: they must
        be pure. A plain `Lock` is not re-entrant, so calling back into the memory deadlocks (the
        documented contract; pinned with a non-blocking probe)."""
        seen = []

        def predicate(key):
            seen.append(mem._lock.acquire(False))
            return True

        mem.note(CLOCK, 'k')
        mem.forget_where(CLOCK, predicate)
        mem.note(CLOCK, 'k')
        mem.retain(CLOCK, predicate)
        mem.replace_group(CLOCK, predicate, ['k'])
        assert seen == [False, False, False]
        assert mem._lock.acquire(False), "released afterwards"
        mem._lock.release()


# ---------------------------------------------------------------------------------------------
# counters
# ---------------------------------------------------------------------------------------------


class TestCounters:
    def test_incr_returns_the_new_count(self, mem):
        assert [mem.incr(TOKEN_SKIPS, 'k') for _ in range(3)] == [1, 2, 3]
        assert mem.count(TOKEN_SKIPS, 'k') == 3

    def test_count_of_an_unknown_key_is_zero_and_does_not_create_it(self, mem):
        assert mem.count(TOKEN_SKIPS, 'k') == 0
        assert mem.incr(TOKEN_SKIPS, 'k') == 1

    def test_keys_and_tables_are_independent(self, mem):
        mem.incr(TOKEN_SKIPS, 'a')
        mem.incr(TOKEN_SKIPS, 'a')
        mem.incr(TOKEN_SKIPS, 'b')
        mem.incr(NEGOTIATE_ZFS_SKIPS, 'a')
        assert (mem.count(TOKEN_SKIPS, 'a'), mem.count(TOKEN_SKIPS, 'b'),
                mem.count(NEGOTIATE_ZFS_SKIPS, 'a')) == (2, 1, 1)

    def test_reset_returns_the_last_value_and_drops_the_counter(self, mem):
        mem.incr(TOKEN_SKIPS, 'k')
        mem.incr(TOKEN_SKIPS, 'k')
        assert mem.reset(TOKEN_SKIPS, 'k') == 2
        assert mem.count(TOKEN_SKIPS, 'k') == 0
        assert mem.incr(TOKEN_SKIPS, 'k') == 1, "a reset counter restarts at one"

    def test_reset_of_a_missing_counter_is_none(self, mem):
        assert mem.reset(TOKEN_SKIPS, 'k') is None

    def test_seed_max_raises_and_never_lowers(self, mem):
        mem.seed_max(TOKEN_SKIPS, 'k', 5)
        assert mem.count(TOKEN_SKIPS, 'k') == 5
        mem.seed_max(TOKEN_SKIPS, 'k', 3)
        assert mem.count(TOKEN_SKIPS, 'k') == 5
        mem.seed_max(TOKEN_SKIPS, 'k', 9)
        assert mem.count(TOKEN_SKIPS, 'k') == 9

    def test_seed_max_then_incr_continues_from_the_seed(self, mem):
        mem.seed_max(TOKEN_SKIPS, 'k', 2)
        assert mem.incr(TOKEN_SKIPS, 'k') == 3

    def test_seed_max_coerces_to_int(self, mem):
        mem.seed_max(TOKEN_SKIPS, 'k', '4')
        assert mem.count(TOKEN_SKIPS, 'k') == 4

    def test_counter_view_is_the_live_dict(self, mem):
        mem.incr(TOKEN_SKIPS, 'k')
        view = mem.counter_view(TOKEN_SKIPS)
        assert view == {'k': 1}
        mem.incr(TOKEN_SKIPS, 'k')
        assert view == {'k': 2}


# ---------------------------------------------------------------------------------------------
# value maps
# ---------------------------------------------------------------------------------------------


class TestValues:
    def test_note_value_is_true_when_the_value_changed(self, mem):
        assert mem.note_value(REFUSALS, 'k', 'm1') is True
        assert mem.note_value(REFUSALS, 'k', 'm1') is False
        assert mem.note_value(REFUSALS, 'k', 'm2') is True
        assert mem.get(REFUSALS, 'k') == 'm2'

    def test_note_value_with_a_none_value_on_a_fresh_key_is_a_change(self, mem):
        assert mem.note_value(REFUSALS, 'k', None) is True
        assert mem.note_value(REFUSALS, 'k', None) is False

    def test_put_get_pop(self, mem):
        mem.put(REFUSALS, 'k', 'v')
        assert mem.get(REFUSALS, 'k') == 'v'
        assert mem.pop(REFUSALS, 'k') == 'v'
        assert mem.get(REFUSALS, 'k') is None
        assert mem.pop(REFUSALS, 'k') is None

    def test_defaults(self, mem):
        assert mem.get(REFUSALS, 'k', 'dflt') == 'dflt'
        assert mem.pop(REFUSALS, 'k', 'dflt') == 'dflt'

    def test_a_popped_value_is_announced_again(self, mem):
        mem.note_value(REFUSALS, 'k', 'm')
        mem.pop(REFUSALS, 'k')
        assert mem.note_value(REFUSALS, 'k', 'm') is True

    def test_pop_returns_a_falsy_stored_value(self, mem):
        mem.put(REFUSALS, 'k', 0)
        assert mem.pop(REFUSALS, 'k', 'dflt') == 0

    def test_items_lists_the_pairs(self, mem):
        mem.put(REFUSALS, 'a', 1)
        mem.put(REFUSALS, 'b', 2)
        assert sorted(mem.items(REFUSALS)) == [('a', 1), ('b', 2)]
        assert mem.items('never-used') == []

    def test_items_is_a_copy(self, mem):
        mem.put(REFUSALS, 'a', 1)
        snapshot = mem.items(REFUSALS)
        mem.put(REFUSALS, 'b', 2)
        mem.pop(REFUSALS, 'a')
        assert snapshot == [('a', 1)]
        snapshot.append(('z', 0))
        assert sorted(mem.items(REFUSALS)) == [('b', 2)], "mutating the copy changes nothing"

    def test_value_view_is_the_live_dict(self, mem):
        view = mem.value_view(REFUSALS)
        mem.put(REFUSALS, 'k', 'v')
        assert view == {'k': 'v'}

    def test_the_three_kinds_of_table_share_no_namespace(self, mem):
        mem.note('x', 'k')
        mem.incr('x', 'k')
        mem.put('x', 'k', 'v')
        assert mem.noted('x', 'k') and mem.count('x', 'k') == 1 and mem.get('x', 'k') == 'v'
        mem.clear('x', 'k')
        mem.reset('x', 'k')
        assert mem.get('x', 'k') == 'v'

    def test_the_table_names_are_distinct_strings(self):
        names = [CLOCK, NO_TXG, EXPAND_ERROR, EXPAND_TARGET, EXPAND_COPY, WAITING, DRY_RUN_SENT,
                 COPY_CONFIRMED, RETENTION_WARNED, TOKEN_ERROR, NEGOTIATE_ZFS_ERROR, NO_CLIENT_ID,
                 TOKEN_SKIPS, NEGOTIATE_ZFS_SKIPS, REFUSALS, PENDING, IDENTITY_ERROR]
        assert all(isinstance(n, str) and n for n in names)
        assert len(set(names)) == len(names)


# ---------------------------------------------------------------------------------------------
# pending
# ---------------------------------------------------------------------------------------------


class TestPending:
    def test_a_put_is_active_for_the_ttl(self, mem):
        mem.pending_put('copy', 100.0)
        assert mem.pending_active('copy', 600, 100.0) is True
        assert mem.pending_active('copy', 600, 699.9) is True

    def test_it_expires_at_exactly_the_ttl_and_is_removed(self, mem):
        mem.pending_put('copy', 100.0)
        assert mem.pending_active('copy', 600, 700.0) is False
        assert 'copy' not in mem.value_view(PENDING), "an expired entry is removed"
        assert mem.pending_active('copy', 600, 100.0) is False, "and stays gone"

    def test_an_unknown_key_is_not_active(self, mem):
        assert mem.pending_active('copy', 600, 1.0) is False

    def test_a_still_active_entry_is_not_removed(self, mem):
        mem.pending_put('copy', 100.0)
        mem.pending_active('copy', 600, 200.0)
        assert mem.value_view(PENDING) == {'copy': 100.0}

    def test_pending_clear(self, mem):
        mem.pending_put('copy', 100.0)
        mem.pending_clear('copy')
        assert mem.pending_active('copy', 600, 101.0) is False
        mem.pending_clear('copy')      # clearing a missing key is harmless

    def test_a_second_put_refreshes_the_ttl(self, mem):
        mem.pending_put('copy', 100.0)
        mem.pending_put('copy', 650.0)
        assert mem.pending_active('copy', 600, 1000.0) is True

    def test_it_is_the_PENDING_value_table(self, mem):
        mem.pending_put('copy', 5.0)
        assert mem.get(PENDING, 'copy') == 5.0
        mem.put(PENDING, 'other', 6.0)
        assert mem.pending_active('other', 10, 7.0) is True

    def test_keys_are_independent(self, mem):
        mem.pending_put('a', 0.0)
        mem.pending_put('b', 0.0)
        assert mem.pending_active('a', 5, 100.0) is False
        assert mem.pending_active('b', 500, 100.0) is True


# ---------------------------------------------------------------------------------------------
# the daily certificate reminder
# ---------------------------------------------------------------------------------------------


class TestClaimExpiryCheck:
    def test_the_first_caller_claims(self, mem):
        assert mem.expiry_checked_at is None
        assert mem.claim_expiry_check(86400.0, 1000.0) is True
        assert mem.expiry_checked_at == 1000.0

    def test_later_callers_inside_the_window_do_not(self, mem):
        mem.claim_expiry_check(86400.0, 1000.0)
        assert mem.claim_expiry_check(86400.0, 1001.0) is False
        assert mem.claim_expiry_check(86400.0, 1000.0 + 86399.0) is False
        assert mem.expiry_checked_at == 1000.0, "a refused claim does not move the window"

    def test_the_window_ends_at_exactly_every(self, mem):
        mem.claim_expiry_check(86400.0, 1000.0)
        assert mem.claim_expiry_check(86400.0, 1000.0 + 86400.0) is True
        assert mem.expiry_checked_at == 1000.0 + 86400.0

    def test_a_claim_starts_a_new_window(self, mem):
        mem.claim_expiry_check(10.0, 0.0)
        mem.claim_expiry_check(10.0, 10.0)
        assert mem.claim_expiry_check(10.0, 15.0) is False
        assert mem.claim_expiry_check(10.0, 20.0) is True

    def test_a_clock_that_reads_earlier_is_not_a_claim(self, mem):
        mem.claim_expiry_check(10.0, 100.0)
        assert mem.claim_expiry_check(10.0, 50.0) is False

    def test_the_first_claim_at_time_zero(self, mem):
        assert mem.claim_expiry_check(10.0, 0.0) is True
        assert mem.expiry_checked_at == 0.0
        assert mem.claim_expiry_check(10.0, 5.0) is False, "0.0 is a claim, not 'unclaimed'"


# ---------------------------------------------------------------------------------------------
# views
# ---------------------------------------------------------------------------------------------


class TestViews:
    def test_a_view_of_a_table_never_used_is_created_empty_and_live(self, mem):
        view = mem.latch_view('fresh')
        assert view == set()
        mem.note('fresh', 1)
        assert view == {1}

    def test_the_same_object_each_time(self, mem):
        assert mem.latch_view(CLOCK) is mem.latch_view(CLOCK)
        assert mem.counter_view(TOKEN_SKIPS) is mem.counter_view(TOKEN_SKIPS)
        assert mem.value_view(REFUSALS) is mem.value_view(REFUSALS)

    def test_the_operations_write_through_to_the_views(self, mem):
        latches, counters, values = (
            mem.latch_view(CLOCK), mem.counter_view(TOKEN_SKIPS), mem.value_view(REFUSALS))
        mem.note(CLOCK, 'a')
        mem.incr(TOKEN_SKIPS, 'a')
        mem.put(REFUSALS, 'a', 'x')
        assert (latches, counters, values) == ({'a'}, {'a': 1}, {'a': 'x'})
        mem.reset_latches(CLOCK)
        mem.reset(TOKEN_SKIPS, 'a')
        mem.pop(REFUSALS, 'a')
        assert (latches, counters, values) == (set(), {}, {})

    def test_replace_group_keeps_the_views_alive(self, mem):
        """The worker's `_waiting_noted` alias must survive `replace_group` / `retain`."""
        view = mem.latch_view(NO_TXG)
        mem.replace_group(NO_TXG, lambda k: True, [('d', 'x')])
        mem.retain(NO_TXG, lambda k: True)
        assert mem.latch_view(NO_TXG) is view and view == {('d', 'x')}

    def test_forget_where_keeps_the_views_alive(self, mem):
        view = mem.latch_view(CLOCK)
        mem.note(CLOCK, 1)
        mem.forget_where(CLOCK, lambda k: True)
        assert mem.latch_view(CLOCK) is view and view == set()


# ---------------------------------------------------------------------------------------------
# pickling
# ---------------------------------------------------------------------------------------------


class TestPickling:
    def populated(self):
        m = SendMemory()
        m.note(CLOCK, ('d', 'what', 's'))
        m.incr(TOKEN_SKIPS, ('d', 'dest'))
        m.incr(TOKEN_SKIPS, ('d', 'dest'))
        m.put(REFUSALS, (None, 'dest'), 'refused')
        m.pending_put('copy', 12.5)
        m.claim_expiry_check(100.0, 50.0)
        return m

    @pytest.mark.parametrize('protocol', range(2, pickle.HIGHEST_PROTOCOL + 1))
    def test_the_state_round_trips(self, protocol):
        back = pickle.loads(pickle.dumps(self.populated(), protocol))
        assert back.noted(CLOCK, ('d', 'what', 's'))
        assert back.count(TOKEN_SKIPS, ('d', 'dest')) == 2
        assert back.get(REFUSALS, (None, 'dest')) == 'refused'
        assert back.pending_active('copy', 100, 20.0) is True
        assert back.expiry_checked_at == 50.0
        assert back.claim_expiry_check(100.0, 60.0) is False, "the claim window survives"

    def test_the_lock_is_recreated_free_and_distinct(self):
        original = self.populated()
        back = pickle.loads(pickle.dumps(original))
        assert back._lock is not original._lock
        assert back._lock.acquire(False)
        back._lock.release()

    def test_the_lock_is_the_only_thing_that_cannot_be_pickled(self):
        with pytest.raises(TypeError):
            pickle.dumps(threading.Lock())
        pickle.dumps(SendMemory())

    def test_an_empty_memory_round_trips_and_works(self):
        back = pickle.loads(pickle.dumps(SendMemory()))
        assert back.note(CLOCK, 'k') is True and back.note(CLOCK, 'k') is False
        assert back.incr(TOKEN_SKIPS, 'k') == 1

    def test_the_unpickled_copy_is_independent_of_the_original(self):
        original = self.populated()
        back = pickle.loads(pickle.dumps(original))
        back.note(CLOCK, 'only-in-back')
        original.note(CLOCK, 'only-in-original')
        assert not original.noted(CLOCK, 'only-in-back')
        assert not back.noted(CLOCK, 'only-in-original')

    def test_a_view_alias_survives_the_round_trip_by_identity(self):
        """The worker keeps `_waiting_noted` as the memory's live latch; pickled together they stay one."""
        mem = SendMemory()
        alias = mem.latch_view(WAITING)
        alias.add(('p', 'dest'))
        holder = pickle.loads(pickle.dumps({'alias': alias, 'memory': mem}))
        assert holder['alias'] is holder['memory'].latch_view(WAITING)
        assert holder['memory'].noted(WAITING, ('p', 'dest'))
        assert holder['memory'].note(WAITING, ('p', 'dest')) is False

    def test_deepcopy_works_and_is_independent(self):
        original = self.populated()
        clone = copy.deepcopy(original)
        clone.incr(TOKEN_SKIPS, ('d', 'dest'))
        assert original.count(TOKEN_SKIPS, ('d', 'dest')) == 2
        assert clone.count(TOKEN_SKIPS, ('d', 'dest')) == 3
        assert clone._lock is not original._lock


# ---------------------------------------------------------------------------------------------
# lock discipline (deterministic: the stress tests below only catch a missing lock by luck)
# ---------------------------------------------------------------------------------------------


class _TrackingLock:
    """A non-reentrant lock that knows who holds it and raises on a nested acquire (a real
    `threading.Lock` would deadlock): what a public operation must never do."""

    def __init__(self):
        self._inner = threading.Lock()
        self.owner = None
        self.acquired = 0

    def acquire(self, blocking=True, timeout=-1):
        if self.owner == threading.get_ident():
            raise AssertionError('a public operation took the lock while holding it')
        got = self._inner.acquire(blocking, timeout)
        if got:
            self.owner = threading.get_ident()
            self.acquired += 1
        return got

    def release(self):
        self.owner = None
        self._inner.release()

    def locked(self):
        return self._inner.locked()

    def held(self):
        return self.owner == threading.get_ident()

    __enter__ = acquire

    def __exit__(self, *exc):
        self.release()
        return False


class _GuardedMemory(SendMemory):
    """A `SendMemory` whose tables may only be reached with the lock held."""

    def __init__(self):
        super().__init__()
        self._lock = _TrackingLock()

    def _set(self, name):
        assert self._lock.held(), f'latch table {name!r} reached without the lock'
        return super()._set(name)

    def _count(self, name):
        assert self._lock.held(), f'counter table {name!r} reached without the lock'
        return super()._count(name)

    def _map(self, name):
        assert self._lock.held(), f'value table {name!r} reached without the lock'
        return super()._map(name)


#: Every public operation, with arguments that make it touch its table.
_OPERATIONS = {
    'note': lambda m: m.note(CLOCK, 'k'),
    'clear': lambda m: m.clear(CLOCK, 'k'),
    'noted': lambda m: m.noted(CLOCK, 'k'),
    'reset_latches': lambda m: m.reset_latches(CLOCK),
    'forget_where': lambda m: m.forget_where(CLOCK, lambda k: True),
    'retain': lambda m: m.retain(CLOCK, lambda k: True),
    'replace_group': lambda m: m.replace_group(CLOCK, lambda k: True, ['a']),
    'incr': lambda m: m.incr(TOKEN_SKIPS, 'k'),
    'reset': lambda m: m.reset(TOKEN_SKIPS, 'k'),
    'seed_max': lambda m: m.seed_max(TOKEN_SKIPS, 'k', 3),
    'count': lambda m: m.count(TOKEN_SKIPS, 'k'),
    'note_value': lambda m: m.note_value(REFUSALS, 'k', 'v'),
    'put': lambda m: m.put(REFUSALS, 'k', 'v'),
    'get': lambda m: m.get(REFUSALS, 'k'),
    'pop': lambda m: m.pop(REFUSALS, 'k'),
    'items': lambda m: m.items(REFUSALS),
    'pending_put': lambda m: m.pending_put('c', 1.0),
    'pending_active': lambda m: m.pending_active('c', 5.0, 2.0),
    'pending_clear': lambda m: m.pending_clear('c'),
    'claim_expiry_check': lambda m: m.claim_expiry_check(10.0, 1.0),
    'latch_view': lambda m: m.latch_view(CLOCK),
    'counter_view': lambda m: m.counter_view(TOKEN_SKIPS),
    'value_view': lambda m: m.value_view(REFUSALS),
}


class TestLockDiscipline:
    @pytest.mark.parametrize('operation', sorted(_OPERATIONS))
    def test_each_operation_takes_the_lock_once_and_releases_it(self, operation):
        mem = _GuardedMemory()
        _OPERATIONS[operation](mem)
        assert mem._lock.acquired >= 1, f'{operation} never took the lock'
        assert not mem._lock.locked(), f'{operation} returned with the lock held'

    @pytest.mark.parametrize('operation', ['forget_where', 'retain', 'replace_group'])
    def test_a_predicate_that_raises_releases_the_lock(self, operation):
        """The predicates of these three run under the lock."""
        mem = _GuardedMemory()
        mem.note(CLOCK, 'k')

        def predicate(key):
            raise RuntimeError('predicate')

        call = getattr(mem, operation)
        with pytest.raises(RuntimeError, match='predicate'):
            call(CLOCK, predicate) if operation != 'replace_group' else call(CLOCK, predicate, ['a'])
        assert not mem._lock.locked()

    def test_the_operations_are_all_covered(self):
        public = {
            name for name, member in vars(SendMemory).items()
            if callable(member) and not name.startswith('_')
        }
        assert public == set(_OPERATIONS)

    def test_the_guard_catches_an_operation_that_forgot_the_lock(self):
        """Not vacuous: reaching a table without the lock trips the guard."""
        mem = _GuardedMemory()
        with pytest.raises(AssertionError, match='without the lock'):
            mem._set(CLOCK)

    def test_the_default_memory_uses_a_real_lock(self):
        assert type(SendMemory()._lock) is type(threading.Lock())


# ---------------------------------------------------------------------------------------------
# stress
# ---------------------------------------------------------------------------------------------

THREADS = 32
OPS = 1000


@pytest.fixture
def tiny_switch_interval():
    """Make the interpreter switch threads as often as it can, so a missing lock shows."""
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-5)
    yield
    sys.setswitchinterval(old)


def run_threads(work, threads=THREADS):
    """Run `work(thread_index)` on `threads` threads that start together (a Barrier); -> the list of
    exceptions (empty when all is well)."""
    barrier = threading.Barrier(threads)
    errors = []

    def body(index):
        try:
            barrier.wait(30)
            work(index)
        except BaseException as e:  # noqa: BLE001
            errors.append(repr(e))

    pool = [threading.Thread(target=body, args=(i,)) for i in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join(120)
    assert not any(t.is_alive() for t in pool), "a stress thread is stuck"
    return errors


@pytest.mark.slow
@pytest.mark.usefixtures('tiny_switch_interval')
class TestStress:
    """32 threads x 1000 operations. The memory's one lock makes every operation atomic: no
    announcement is lost or made twice, no count is skipped."""

    def test_exactly_one_winner_per_latch_key(self, mem):
        winners = [[] for _ in range(THREADS)]

        def work(t):
            for i in range(OPS):
                if mem.note(CLOCK, ('k', i)):
                    winners[t].append(i)

        assert run_threads(work) == []
        won = sorted(i for mine in winners for i in mine)
        assert won == list(range(OPS)), "every key announced exactly once across all threads"
        assert len(mem.latch_view(CLOCK)) == OPS

    def test_exactly_one_clear_per_latch_key(self, mem):
        for i in range(OPS):
            mem.note(CLOCK, ('k', i))
        cleared = [[] for _ in range(THREADS)]

        def work(t):
            for i in range(OPS):
                if mem.clear(CLOCK, ('k', i)):
                    cleared[t].append(i)

        assert run_threads(work) == []
        assert sorted(i for mine in cleared for i in mine) == list(range(OPS))
        assert mem.latch_view(CLOCK) == set()

    def test_counters_are_exact_and_every_returned_count_is_distinct(self, mem):
        returned = [[] for _ in range(THREADS)]

        def work(t):
            for _ in range(OPS):
                returned[t].append(mem.incr(TOKEN_SKIPS, 'shared'))

        assert run_threads(work) == []
        assert mem.count(TOKEN_SKIPS, 'shared') == THREADS * OPS
        assert sorted(c for mine in returned for c in mine) == list(range(1, THREADS * OPS + 1))
        for mine in returned:
            assert mine == sorted(mine), "one thread sees its counts increase"

    def test_one_new_value_announcement_per_value(self, mem):
        """`note_value` of the same message from every thread is announced once per distinct message."""
        announced = [[] for _ in range(THREADS)]

        def work(t):
            for i in range(OPS):
                if mem.note_value(REFUSALS, ('k', i % 50), f'message-{i % 50}'):
                    announced[t].append(i % 50)

        assert run_threads(work) == []
        won = sorted(i for mine in announced for i in mine)
        assert won == list(range(50)), "each (key, message) was announced exactly once"

    def test_a_changing_value_is_announced_once_per_change_seen_by_a_single_writer(self, mem):
        """Different threads write different keys: each key's alternating values are all announced."""
        announced = [0] * THREADS

        def work(t):
            for i in range(OPS):
                if mem.note_value(REFUSALS, ('thread', t), f'v{i % 2}'):
                    announced[t] += 1

        assert run_threads(work) == []
        assert announced == [OPS] * THREADS

    def test_each_thread_owns_its_pending_keys(self, mem):
        def work(t):
            key = ('copy', t)
            for i in range(OPS):
                mem.pending_put(key, float(i))
                assert mem.pending_active(key, 1000.0, float(i) + 1.0) is True
                assert mem.pending_active(key, 0.5, float(i) + 1.0) is False
                assert mem.pending_active(key, 1000.0, float(i) + 1.0) is False, "expired = removed"
                mem.pending_put(key, float(i))
                mem.pending_clear(key)

        assert run_threads(work) == []
        assert mem.value_view(PENDING) == {}

    def test_a_shared_pending_key_is_never_corrupted(self, mem):
        """Threads put, test and clear ONE key at once: no exception, and the table ends in a
        state that is either absent or a float, whatever the interleaving."""
        def work(t):
            for i in range(OPS):
                mem.pending_put('shared', float(i))
                mem.pending_active('shared', 10.0, float(i))
                mem.pending_active('shared', 0.0, float(i))
                if i % 3 == t % 3:
                    mem.pending_clear('shared')

        assert run_threads(work) == []
        assert mem.get(PENDING, 'shared') is None or isinstance(mem.get(PENDING, 'shared'), float)

    def test_exactly_one_expiry_claim_per_window(self, mem):
        """Every thread calls with the SAME `now` once per window (a barrier between windows, so the
        threads are in the same window at once): one winner each, never two."""
        windows = OPS
        barrier = threading.Barrier(THREADS)
        winners = [0] * windows
        guard = threading.Lock()

        def work(t):
            for w in range(windows):
                barrier.wait(30)
                if mem.claim_expiry_check(10.0, 10.0 * w):
                    with guard:
                        winners[w] += 1

        assert run_threads(work) == []
        assert winners == [1] * windows

    def test_the_first_claim_of_an_incarnation_is_unique(self, mem):
        won = []

        def work(t):
            if mem.claim_expiry_check(86400.0, 1000.0 + t * 1e-6):
                won.append(t)

        assert run_threads(work) == []
        assert len(won) == 1

    def test_the_mixed_run(self, mem):
        """Every operation at once, from every thread, over shared and own keys."""
        note_wins = [[] for _ in range(THREADS)]
        incr_seen = [[] for _ in range(THREADS)]
        value_wins = [[] for _ in range(THREADS)]
        claims = [0] * THREADS

        def work(t):
            for i in range(OPS):
                if mem.note(CLOCK, ('mixed', i)):
                    note_wins[t].append(i)
                incr_seen[t].append(mem.incr(TOKEN_SKIPS, 'mixed'))
                if mem.note_value(REFUSALS, ('mixed', i % 20), 'same'):
                    value_wins[t].append(i % 20)
                key = ('own', t)
                mem.pending_put(key, 0.0)
                assert mem.pending_active(key, 5.0, 1.0)
                mem.pending_clear(key)
                if mem.claim_expiry_check(10.0 ** 9, 5.0):
                    claims[t] += 1
                mem.items(REFUSALS)
                mem.noted(WAITING, ('p', i))

        assert run_threads(work) == []
        assert sorted(i for w in note_wins for i in w) == list(range(OPS))
        assert sorted(c for s in incr_seen for c in s) == list(range(1, THREADS * OPS + 1))
        assert sorted(i for w in value_wins for i in w) == list(range(20))
        assert sum(claims) == 1
        assert mem.value_view(PENDING) == {}

    def test_replace_group_hands_each_new_key_to_exactly_one_caller(self, mem):
        """The `list_snapshots` pattern: every thread lists the same dataset's no-`createtxg` names."""
        fresh_total = [0] * THREADS

        def work(t):
            for i in range(200):
                names = [('d', f'snap{i}-{j}') for j in range(3)]
                fresh_total[t] += len(mem.replace_group(NO_TXG, lambda k: k[0] == 'd', names))

        assert run_threads(work) == []
        # Group replacement is atomic: the table holds the LAST call's names, and every call announced
        # only names that the previous call had not left behind. The total can differ per run; what
        # must hold is that no call raised and the final table is one call's complete set.
        final = mem.latch_view(NO_TXG)
        assert len(final) == 3 and len({k[1].split('-')[0] for k in final}) == 1


@pytest.mark.slow
@pytest.mark.usefixtures('tiny_switch_interval')
class TestItemsIsASafeCopy:
    def test_iterating_items_while_other_threads_write_never_raises(self, mem):
        stop = threading.Event()
        problems = []

        def writer(t):
            i = 0
            while not stop.is_set():
                mem.put(REFUSALS, (t, i % 500), i)
                if i % 3 == 0:
                    mem.pop(REFUSALS, (t, (i + 7) % 500))
                i += 1

        def reader():
            try:
                for _ in range(400):
                    for key, value in mem.items(REFUSALS):
                        assert isinstance(key, tuple)
                    sorted(mem.items(REFUSALS), key=lambda item: (item[0][0], item[0][1]))
            except BaseException as e:  # noqa: BLE001
                problems.append(repr(e))
            finally:
                stop.set()

        writers = [threading.Thread(target=writer, args=(t,)) for t in range(4)]
        for w in writers:
            w.start()
        r = threading.Thread(target=reader)
        r.start()
        r.join(120)
        stop.set()
        for w in writers:
            w.join(30)
        assert problems == []
        assert not r.is_alive()

    def test_the_live_view_is_not_safe_to_iterate_and_the_copy_is(self, mem):
        """Why `items()` exists: iterating the live dict while it changes raises."""
        for i in range(5):
            mem.put(REFUSALS, i, i)
        copy_ = mem.items(REFUSALS)
        for key, _ in copy_:
            mem.put(REFUSALS, key + 100, 0)           # growing the table mid-iteration: fine on a copy
        assert len(mem.items(REFUSALS)) == 10
        with pytest.raises(RuntimeError):
            for key in mem.value_view(REFUSALS):
                mem.put(REFUSALS, key + 1000, 0)


# ---------------------------------------------------------------------------------------------
# real child processes
# ---------------------------------------------------------------------------------------------

START_METHODS = [
    pytest.param(
        m,
        marks=pytest.mark.skipif(
            m not in multiprocessing.get_all_start_methods(),
            reason=f'{m!r} start method unavailable on this platform',
        ),
    )
    for m in ('fork', 'spawn', 'forkserver')
]

_POPEN_MODULE = {
    'fork': 'popen_fork',
    'spawn': 'popen_spawn_posix',
    'forkserver': 'popen_forkserver',
}


def _worker_config():
    from zfsbackup.config import BackupConfig, DatasetConfig
    return BackupConfig(datasets=[DatasetConfig(name='pool/x')])


def _run_child(worker, tmp_path, method):
    """Start `worker` in a real child, wait for it, and make sure none is left behind."""
    worker.start_method = method
    worker.record_path = str(tmp_path / f'record-{method}.json')
    worker.start()
    try:
        worker.join(120)
        assert worker.exitcode == 0, f'{method} child exited {worker.exitcode}'
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(30)
    assert type(worker._popen).__module__.endswith(_POPEN_MODULE[method])
    with open(worker.record_path) as fh:
        return json.load(fh)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestMemoryInARealChild:
    """A pickled `RemoteBackupWorker` reaches a spawned / forkserver child (inherited under fork) with
    its `SendMemory` intact, a usable fresh lock, and the old attribute names still aliasing the
    memory's latches; the child then uses it from threads."""

    @staticmethod
    def worker(preseed=True):
        worker = MemoryProbeWorker(_worker_config(), multiprocessing.Event(), 'INFO')
        mem = worker._mem()
        if preseed:
            mem.note(CLOCK, ('pool/a', 'snapshot', 's1'))
            mem.put(REFUSALS, ('pool/a', 'offsite'), 'standing refusal')
            mem.incr(TOKEN_SKIPS, ('pool/a', 'offsite'))
            mem.incr(TOKEN_SKIPS, ('pool/a', 'offsite'))
            mem.claim_expiry_check(100.0, 100.0)
            mem.note(WAITING, ('pool/p', 'offsite'))
        return worker

    @pytest.mark.parametrize('method', START_METHODS)
    def test_the_child_sees_the_memory_and_uses_it(self, tmp_path, method):
        record = _run_child(self.worker(), tmp_path, method)
        assert record['memory_type'] == 'SendMemory'
        # what the parent noted before the start
        assert record['preseeded_latch'] is True
        assert record['preseeded_value'] == 'standing refusal'
        assert record['preseeded_count'] == 2
        assert record['preseeded_expiry'] == 100.0
        assert record['expiry_reclaimed_inside_window'] is False
        assert record['waiting'] == [['pool/p', 'offsite']]
        # the lock and the aliases
        assert record['lock_was_free'] is True
        assert record['waiting_is_the_memorys_latch'] is True
        assert record['dry_run_is_the_memorys_latch'] is True
        assert record['manager_shares_it'] is True
        assert record['manager_clock_alias'] is True
        # and the child can really use it from threads
        assert record['hammer']['errors'] == []
        assert record['hammer']['one_winner_per_key'] is True
        assert record['hammer']['counts_are_exact'] is True
        assert record['hammer']['final_count'] == 8 * 300

    @pytest.mark.parametrize('method', START_METHODS)
    def test_the_childs_writes_do_not_reach_the_parent(self, tmp_path, method):
        worker = self.worker(preseed=False)
        _run_child(worker, tmp_path, method)
        assert worker._mem().count(TOKEN_SKIPS, 'child') == 0
        assert not worker._mem().noted(CLOCK, ('child', 0))

    @pytest.mark.parametrize('method', START_METHODS)
    def test_a_child_of_an_empty_memory_starts_empty(self, tmp_path, method):
        record = _run_child(self.worker(preseed=False), tmp_path, method)
        assert record['preseeded_latch'] is False
        assert record['preseeded_count'] == 0
        assert record['preseeded_expiry'] is None
        assert record['waiting'] == []
        assert record['hammer']['counts_are_exact'] is True

    def test_a_bare_memory_pickled_into_a_pool_of_threads_is_still_atomic(self):
        """In this process: unpickle, then hammer the copy (the child does the same, remotely)."""
        back = pickle.loads(pickle.dumps(SendMemory()))
        result = hammer(back, threads=8, ops=200)
        assert result['errors'] == []
        assert result['one_winner_per_key'] and result['counts_are_exact']

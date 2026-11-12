"""`zfsbackup.sendloop.SendLoop`: the remote worker's scheduler with its sender and discovery threads (plan
Track B, item B3).

REAL threads, FAKE kits: `send_pair` is replaced (by a function of the test's choosing, the same arguments),
the kit factory builds `SenderKit`s over doubles, `expand` is a function, and the schedule is a REAL
`SendSchedule` on a fake clock the test moves (or, for the concurrency invariants, on the real clock with
short periods). Nothing reaches `zfs`, a database or the network. The loop runs on a thread of its own so
that the test body can watch it, move the clock and release the senders; every wait has a hard bound
(`_sendloop_support.bound()`, scaled by `timer_stretch()`), and a fixture stops whatever a test leaves
running.

Pinned here:

* the caps (4 in all, 2 per destination), one thread per pair, no ancestor and descendant pair of one
  destination running together (the invariants `SendMemory`'s check-then-act sequences rely on);
* the outcome mapping, `finished` before `confirmed`/`unconfirmed`, which outcomes are recorded and when
  (a retryable FAILED only once its retries are used up), the kit closed before the outcome is visible;
* a sender exception of any kind, a kit that cannot be built, a thread that cannot be started, a thread that
  dies without an outcome;
* the exit paths: a reload / upgrade drain / stop with a long sender (the loop returns only after it), a
  scheduler exception never leaves a live sender, a discovery that does not return is abandoned after a
  bounded join, the heartbeat during a long drain;
* discovery: one thread per parent, at most 4, `update_pairs` only when the child names changed, a failed
  or crashed discovery keeps the last known children and their status records;
* the R-2 announcement: the WARNING after `WAITING_NOTICE` of a stuck ancestor (never while it is running
  or queued), the INFO that ends it, the latch shared with the senders' `AncestorRule`;
* the sender-side rule's wiring and the latch housekeeping of `_rebuild`.

Mutation-checked (scratch copy; counts in the report of the change that added this file).
"""

import logging
import threading
import time
import zlib
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from _sendloop_support import bound, stretch, wait_for
from zfsbackup import health, sendloop
from zfsbackup.sendloop import SenderKit, SendLoop, schedule_outcome
from zfsbackup.sendpair import send_pair as real_send_pair
from zfsbackup.sendmemory import COPY_CONFIRMED, ORPHAN_HOLD, WAITING, SendMemory
from zfsbackup.sendpair import (
    PAIR_DONE, PAIR_FAILED, PAIR_NOT_DUE, PAIR_STOPPED, PAIR_WAITING, PairOutcome,
    released_message, waiting_message,
)
from zfsbackup.sendschedule import (
    MAX_DISCOVERIES, MAX_SENDERS, MAX_SENDERS_PER_DESTINATION, Outcome, SendSchedule,
)
from zfsbackup.workers import ExitReason

LOG = 'test_sendloop'


class FakeClock:
    """A clock the test moves (a float: read and written without a lock, never torn)."""

    def __init__(self, now=1_800_000_000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def done(key, **kw):
    kw.setdefault('confirmed', True)
    return PairOutcome(key[0], key[1], PAIR_DONE, **kw)


def failed(key, error='boom', **kw):
    return PairOutcome(key[0], key[1], PAIR_FAILED, error=error, **kw)


def dsi_of(name, destinations=('d',), frequency=3600.0, recursive=False, expanded_from=None, period=None):
    """`period`: the destinations' own frequency (P) in seconds; None: the dataset's (F)."""
    return SimpleNamespace(
        name=name, expanded_from=expanded_from,
        config=SimpleNamespace(
            remote=[
                SimpleNamespace(destination=d, frequency=None if period is None else timedelta(seconds=period))
                for d in destinations
            ],
            frequency=timedelta(seconds=frequency), recursive=recursive,
        ),
    )


class Call:
    """What a fake `send_pair` is given."""

    def __init__(self, key, dsi, should_stop, exit_pending, force_confirm, rule):
        self.key, self.dsi = key, dsi
        self.should_stop, self.exit_pending = should_stop, exit_pending
        self.force_confirm, self.rule = force_confirm, rule


class Harness:
    """A `SendLoop` over fakes, run on its own thread."""

    def __init__(
        self, monkeypatch, pairs, *, parents=(), clock=None, expand=None, behaviour=None,
        status=None, discovery_join=0.2, heartbeat_every=None, memory=None, monotonic=None,
        remind=None, schedule_cls=SendSchedule, poll=0.01, caps=None, flush=True, record=True,
        children_of=None, new_kit=None, fraction=None, period=None,
    ):
        """`pairs`: `{dataset: destinations}` of the configured datasets (`parents`: names of the recursive
        ones among them)."""
        monkeypatch.setattr(sendloop, 'STOP_POLL', poll)
        self.clock = clock if clock is not None else FakeClock()
        self.memory = memory if memory is not None else SendMemory()
        self.stop_event = threading.Event()
        self.reason = None
        self.lock = threading.Lock()
        self.behaviour = behaviour or (lambda call: done(call.key))
        self.recorded, self.flushes, self.kits, self.calls = [], [], [], []
        self.release_events = []
        self.error = None
        self.result = 'unset'
        self.returned_at = None
        self.configured = [dsi_of(n, d, recursive=n in parents, period=period) for n, d in pairs.items()]
        config = SimpleNamespace(
            check_interval=timedelta(minutes=5),
            enabled_datasets=[
                SimpleNamespace(name=c.name, recursive=c.config.recursive, remote=c.config.remote)
                for c in self.configured
            ],
        )
        kw = {} if caps is None else {'max_senders': caps[0], 'max_per_destination': caps[1]}
        self.schedule = schedule_cls('cid', 300.0, clock=self.clock, fraction=fraction or (lambda *a: 0.0), **kw)

        def make_kit(template, remote_cfg):
            if new_kit is not None:
                return new_kit(template, remote_cfg)
            kit = SenderKit(MagicMock(name='manager'), template, lambda: None)
            kit.closed = False
            real_close = kit.close

            def close():
                kit.closed = True
                real_close()

            kit.close = close
            self.kits.append(kit)
            return kit

        def fake_send_pair(manager, dsi, remote_cfg, sender, should_stop, exit_pending, *, rule=None,
                           logger=None, label=None, force_confirm=False):
            call = Call((dsi.name, remote_cfg.destination), dsi, should_stop, exit_pending, force_confirm, rule)
            with self.lock:
                self.calls.append(call)
            return self.behaviour(call)

        monkeypatch.setattr(sendloop, 'send_pair', fake_send_pair)

        def do_record(outcome):
            self.recorded.append(outcome)

        def do_flush(force=False, heartbeat=False):
            self.flushes.append((force, heartbeat))

        kwargs = {}
        if monotonic is not None:
            kwargs['monotonic'] = monotonic
        self.loop = SendLoop(
            name='test-worker', config=config, schedule=self.schedule, memory=self.memory,
            exit_reason=lambda: self.reason or (ExitReason.STOP if self.stop_event.is_set() else None),
            stop_event=self.stop_event, configured=self.configured,
            new_kit=make_kit, expand=expand or (lambda parent, should_stop: SimpleNamespace(
                children=[], failed={}, skipped={})),
            record=do_record if record else None, flush=do_flush if flush else None, status=status,
            remind=remind, logger=logging.getLogger(LOG), clock=self.clock,
            heartbeat_every=heartbeat_every, discovery_join=discovery_join, **kwargs,
        )
        self.thread = None
        # Quiescence points: counted AFTER the real handler returned (it pops its entry, joins the
        # thread, reads the clock and rebuilds; an internal flag flips well before it is done). Written by the
        # scheduler thread only.
        self.discoveries_done = 0
        self.senders_done = {}
        real_discovery_done, real_sender_done = self.loop._on_discovery_done, self.loop._on_sender_done

        def on_discovery_done(done_):
            try:
                return real_discovery_done(done_)
            finally:
                self.discoveries_done += 1

        def on_sender_done(key, outcome):
            try:
                return real_sender_done(key, outcome)
            finally:
                self.senders_done[tuple(key)] = self.senders_done.get(tuple(key), 0) + 1

        self.loop._on_discovery_done = on_discovery_done
        self.loop._on_sender_done = on_sender_done

    # -- running ------------------------------------------------------------------------------

    def start(self):
        def body():
            try:
                self.result = self.loop.run()
            except BaseException as e:  # noqa: BLE001
                self.error = e
            finally:
                self.returned_at = time.monotonic()

        self.thread = threading.Thread(target=body, name='scheduler', daemon=True)
        self.thread.start()
        return self

    def on_release(self, event):
        self.release_events.append(event)
        return event

    def finish(self, reason=ExitReason.STOP):
        """End the loop with `reason` and wait for it (bounded); releases every gate first."""
        self.reason = reason
        self.stop_event.set() if reason is ExitReason.STOP else None
        for event in self.release_events:
            event.set()
        self.thread.join(bound())
        assert not self.thread.is_alive(), 'the loop did not return'
        return self

    def close(self):
        if self.thread is not None and self.thread.is_alive():
            self.reason = self.reason or ExitReason.STOP
            self.stop_event.set()
            for event in self.release_events:
                event.set()
            self.thread.join(bound())

    # -- looking ------------------------------------------------------------------------------

    def sent(self):
        return [c.key for c in self.calls]

    def count(self, key):
        return sum(1 for c in self.calls if c.key == key)

    def until(self, predicate, what='the condition'):
        assert wait_for(predicate), f'timed out waiting for {what}'

    def until_discoveries(self, count, what=None):
        """Wait until `count` discoveries have been completely handled by the scheduler (the handler returned)."""
        self.until(lambda: self.discoveries_done >= count, what or f'{count} discovery result(s) handled')

    def until_sender_done(self, key, count=1, what=None):
        """Wait until the scheduler has completely handled `count` outcomes of the pair `key`."""
        self.until(lambda: self.senders_done.get(tuple(key), 0) >= count,
                   what or f'{count} outcome(s) of {tuple(key)} handled')


@pytest.fixture
def harness(monkeypatch):
    made = []

    def build(pairs, **kw):
        h = Harness(monkeypatch, pairs, **kw)
        made.append(h)
        return h

    yield build
    for h in made:
        h.close()
    for h in made:
        assert h.thread is None or not h.thread.is_alive(), 'a loop thread outlived its test'
        assert not any(t.is_alive() for t in h.loop._senders.values()), 'a sender thread outlived its test'


def gated(h, hold=None):
    """A behaviour: every send blocks until the test releases it (`gate`), recording what is active."""
    gate = h.on_release(threading.Event())
    active, peak, per_destination = set(), [0], {}

    def behaviour(call):
        with h.lock:
            active.add(call.key)
            peak[0] = max(peak[0], len(active))
            n = sum(1 for k in active if k[1] == call.key[1])
            per_destination[call.key[1]] = max(per_destination.get(call.key[1], 0), n)
        try:
            gate.wait(bound())
        finally:
            with h.lock:
                active.discard(call.key)
        return done(call.key, confirmed=None)

    h.behaviour = behaviour
    h.gate, h.active, h.peak, h.per_destination = gate, active, peak, per_destination
    return h


# ---------------------------------------------------------------------------------------------
# the outcome mapping
# ---------------------------------------------------------------------------------------------


@pytest.mark.unit
class TestScheduleOutcome:
    @pytest.mark.parametrize('kind, retryable, expected', [
        (PAIR_DONE, False, (Outcome.OK, False)),
        (PAIR_DONE, True, (Outcome.RETRYABLE, True)),
        (PAIR_NOT_DUE, False, (Outcome.NOT_DUE, False)),
        (PAIR_NOT_DUE, True, (Outcome.NOT_DUE, False)),
        (PAIR_WAITING, False, (Outcome.WAITING, False)),
        (PAIR_STOPPED, False, (Outcome.STOPPED, False)),
        (PAIR_STOPPED, True, (Outcome.STOPPED, False)),
        (PAIR_FAILED, False, (Outcome.FAILED, False)),
        (PAIR_FAILED, True, (Outcome.FAILED, True)),
        ('mystery', True, (Outcome.FAILED, False)),
    ])
    def test_the_mapping(self, kind, retryable, expected):
        assert schedule_outcome(PairOutcome('d', 'x', kind, retryable=retryable)) == expected


# ---------------------------------------------------------------------------------------------
# caps, one thread per pair, no ancestor and descendant together
# ---------------------------------------------------------------------------------------------


@pytest.mark.integration
class TestCaps:
    def test_at_most_four_senders_run_at_once(self, harness):
        """Eight pairs on four destinations (two each: the per-destination cap is not what binds)."""
        pairs = {f'p{i}': (f'd{i % 4}',) for i in range(8)}
        h = gated(harness(pairs)).start()
        h.until(lambda: len(h.active) == MAX_SENDERS, 'four senders')
        time.sleep(0.2 * stretch())                 # a fifth would have started by now
        assert len(h.active) == MAX_SENDERS and h.peak[0] == MAX_SENDERS
        h.gate.set()
        h.until(lambda: len(h.recorded) == 8, 'all eight sends')
        assert h.peak[0] == MAX_SENDERS
        h.finish()

    def test_at_most_two_senders_per_destination(self, harness):
        h = gated(harness({f'p{i}': ('d',) for i in range(6)})).start()
        h.until(lambda: len(h.active) == MAX_SENDERS_PER_DESTINATION, 'two senders')
        time.sleep(0.2 * stretch())
        assert len(h.active) == 2
        h.gate.set()
        h.until(lambda: len(h.recorded) == 6, 'all six sends')
        assert h.per_destination == {'d': 2}
        h.finish()

    def test_a_full_destination_does_not_block_another(self, harness):
        pairs = {'a0': ('x',), 'a1': ('x',), 'a2': ('x',), 'b0': ('y',)}
        h = gated(harness(pairs)).start()
        h.until(lambda: {('a0', 'x'), ('a1', 'x'), ('b0', 'y')} <= h.active, 'x twice and y once')
        assert ('a2', 'x') not in h.active
        h.gate.set()
        h.finish()

    def test_the_same_dataset_goes_to_several_destinations_at_once(self, harness):
        h = gated(harness({'p': ('x', 'y', 'z')})).start()
        h.until(lambda: len(h.active) == 3, 'three destinations at once')
        h.gate.set()
        h.finish()


@pytest.mark.integration
class TestInvariantsUnderRealThreads:
    """The invariants `SendMemory`'s check-then-act sequences are correct under: ONE thread per pair, and no
    ancestor and descendant pair of one destination at the same time. Short periods on the real clock, so
    pairs fall due again and again while others are still running."""

    PAIRS = {'p': ('d', 'e'), 'p/a': ('d', 'e'), 'p/a/b': ('d',), 'q': ('d',), 'q/c': ('d', 'e'), 'r': ('f',)}
    #: ONE pair whose send outlasts its period (0.3 s): it falls due again while its own thread is still sending,
    #: the only way "two threads for one pair" could happen. (It has its own destination and no relatives, so it
    #: starves nobody and nobody starves it.)
    SLOW = {('r', 'f'): 0.7}

    def build(self, harness, monkeypatch):
        registry = {'active': set(), 'violations': [], 'counts': {}, 'first': {}}

        def behaviour(call):
            key = call.key
            with registry_lock:
                if key in registry['active']:
                    registry['violations'].append(('twice', key))
                for other in registry['active']:
                    if other[1] == key[1] and (
                        other[0].startswith(key[0] + '/') or key[0].startswith(other[0] + '/')
                    ):
                        registry['violations'].append(('overlap', key, other))
                registry['active'].add(key)
                registry['counts'][key] = registry['counts'].get(key, 0) + 1
                registry['first'].setdefault(key, len(registry['first']))
            time.sleep(self.SLOW.get(key, 0.02 + 0.015 * (zlib.crc32(repr(key).encode()) % 7)))  # overlaps later slots
            with registry_lock:
                registry['active'].discard(key)
            return done(key)

        registry_lock = threading.Lock()
        # each pair's own offset inside the period: a parent falls due while its child is still sending
        h = harness(self.PAIRS, behaviour=behaviour, clock=time.time,
                    fraction=lambda cid, ds, dest: (zlib.crc32((ds + dest).encode()) % 1000) / 1000.0)
        # 0.3 s periods on the real clock: a pair falls due many times while others still run
        for dsi in h.configured:
            dsi.config.frequency = timedelta(seconds=0.3)
        return h, registry

    def test_never_two_threads_for_one_pair_and_never_an_ancestor_with_a_descendant(self, harness, monkeypatch):
        h, registry = self.build(harness, monkeypatch)
        h.start()
        keys = [(d, dest) for d, dests in self.PAIRS.items() for dest in dests]
        h.until(lambda: all(registry['counts'].get(k, 0) >= 3 for k in keys if k not in self.SLOW)
                and all(registry['counts'].get(k, 0) >= 2 for k in self.SLOW), 'every pair three times')
        h.finish()
        assert registry['violations'] == []

    def test_a_parent_is_sent_before_its_descendants(self, harness, monkeypatch):
        h, registry = self.build(harness, monkeypatch)
        h.start()
        wanted = {('p', 'd'), ('p/a', 'd'), ('p/a/b', 'd'), ('p', 'e'), ('p/a', 'e'), ('q', 'd'), ('q/c', 'd')}
        h.until(lambda: all(k in registry['first'] for k in wanted), 'every pair of the three orderings has started once')
        h.finish()
        first = registry['first']
        assert first[('p', 'd')] < first[('p/a', 'd')] < first[('p/a/b', 'd')]
        assert first[('p', 'e')] < first[('p/a', 'e')]
        assert first[('q', 'd')] < first[('q/c', 'd')]


# ---------------------------------------------------------------------------------------------
# outcomes: order of the schedule's calls, what is recorded, retries
# ---------------------------------------------------------------------------------------------


class SpySchedule(SendSchedule):
    """Records the calls the loop makes that report an outcome."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.calls = []

    def finished(self, key, kind, retryable=False, now=None):
        self.calls.append(('finished', tuple(key), Outcome(kind).value, retryable))
        return super().finished(key, kind, retryable, now)

    def confirmed(self, key):
        self.calls.append(('confirmed', tuple(key)))
        return super().confirmed(key)

    def unconfirmed(self, key, now=None):
        self.calls.append(('unconfirmed', tuple(key)))
        return super().unconfirmed(key, now)


@pytest.mark.integration
class TestOutcomes:
    def test_finished_comes_before_confirmed(self, harness):
        h = harness({'p': ('d',)}, schedule_cls=SpySchedule).start()
        h.until(lambda: h.recorded, 'the outcome')
        h.finish()
        assert [c[0] for c in h.schedule.calls] == ['finished', 'confirmed']

    def test_a_copy_found_gone_is_unconfirmed_after_finished(self, harness):
        h = harness({'p': ('d',)}, schedule_cls=SpySchedule,
                    behaviour=lambda call: done(call.key, confirmed=False)).start()
        h.until(lambda: h.recorded, 'the outcome')
        h.finish()
        assert [c[0] for c in h.schedule.calls] == ['finished', 'unconfirmed']

    def test_an_outcome_that_learned_nothing_leaves_the_confirmation_alone(self, harness):
        h = harness({'p': ('d',)}, schedule_cls=SpySchedule,
                    behaviour=lambda call: done(call.key, confirmed=None)).start()
        h.until(lambda: h.recorded, 'the outcome')
        h.finish()
        assert [c[0] for c in h.schedule.calls] == ['finished']

    def test_a_confirmed_parent_releases_its_child(self, harness):
        h = harness({'p': ('d',), 'p/c': ('d',)}).start()
        h.until(lambda: h.count(('p/c', 'd')) == 1, 'the child')
        assert h.sent() == [('p', 'd'), ('p/c', 'd')]
        h.finish()

    def test_an_unconfirmed_parent_holds_its_child(self, harness):
        h = harness({'p': ('d',), 'p/c': ('d',)}, behaviour=lambda call: done(call.key, confirmed=None)).start()
        h.until(lambda: h.recorded, 'the parent')
        time.sleep(0.2 * stretch())
        assert h.sent() == [('p', 'd')]
        h.finish()

    def test_every_dispatch_is_forced_the_first_negotiate_of_a_parent_with_children_included(self, harness):
        h = harness({'p': ('d',), 'p/c': ('d',)}).start()
        h.until(lambda: h.count(('p/c', 'd')) == 1, 'the child')
        h.finish()
        forced = {c.key: c.force_confirm for c in h.calls}
        assert forced == {('p', 'd'): True, ('p/c', 'd'): True}, 'the schedule decides, not the anchor-age check'

    def test_a_done_and_a_failed_pair_are_recorded_the_rest_are_not(self, harness):
        kinds = {'a': PAIR_DONE, 'b': PAIR_FAILED, 'c': PAIR_NOT_DUE, 'd': PAIR_WAITING, 'e': PAIR_STOPPED}
        h = harness({n: ('d',) for n in kinds}, behaviour=lambda call: PairOutcome(
            call.key[0], call.key[1], kinds[call.key[0]], error='why')).start()
        h.until(lambda: sum(h.senders_done.values()) == 5, 'all five outcomes handled')
        h.until(lambda: len(h.recorded) == 2, 'two records')
        h.finish()
        assert sorted((o.dataset, o.kind) for o in h.recorded) == [('a', PAIR_DONE), ('b', PAIR_FAILED)]

    def test_a_done_with_retryable_is_recorded_at_once_and_retried(self, harness):
        """A chain that made progress (cut between steps) is a recorded success; the schedule retries it."""
        h = harness({'p': ('d',)}, behaviour=lambda call: done(call.key, retryable=True)).start()
        h.until(lambda: len(h.recorded) == 1, 'the record')
        h.until(lambda: h.schedule.due_time(('p', 'd')) is not None, 'the retry')
        assert h.schedule.due_time(('p', 'd')) == pytest.approx(h.clock.now + 60.0, abs=1)
        h.finish()

    def test_a_done_chain_cut_by_a_drain_is_recorded(self, harness):
        """A reload's drain cuts a chain between steps: DONE with `retryable`, progress made, a success."""
        h = harness({'p': ('d',)})
        release = h.on_release(threading.Event())

        def behaviour(call):
            release.wait(bound())
            return done(call.key, retryable=True)

        h.behaviour = behaviour
        h.start()
        h.until(lambda: h.calls, 'the sender')
        h.reason = ExitReason.RELOAD
        h.until(lambda: h.loop.leave.is_set(), 'leave')
        release.set()
        h.thread.join(bound())
        assert [(o.kind, o.retryable) for o in h.recorded] == [(PAIR_DONE, True)]

    def test_a_retried_done_is_recorded_each_time_but_a_later_not_due_loses_nothing(self, harness):
        results = [done(('p', 'd'), retryable=True), PairOutcome('p', 'd', PAIR_NOT_DUE)]
        h = harness({'p': ('d',)}, behaviour=lambda call: results.pop(0) if len(results) > 1 else results[0]).start()
        h.until_sender_done(('p', 'd'), 1, 'the first outcome')
        assert len(h.recorded) == 1
        h.clock.advance(61)
        h.until_sender_done(('p', 'd'), 2, 'the retry')
        h.finish()
        assert [o.kind for o in h.recorded] == [PAIR_DONE], 'the not-due retry records nothing and erases nothing'

    def test_a_retryable_failure_is_recorded_only_once_its_retries_are_used_up(self, harness):
        h = harness({'p': ('d',)}, behaviour=lambda call: failed(call.key, retryable=True)).start()
        for attempt, wait in ((1, 61.0), (2, 121.0), (3, 301.0)):
            h.until_sender_done(('p', 'd'), attempt, f'attempt {attempt}')
            assert h.recorded == [], f'attempt {attempt} must not be recorded yet'
            h.clock.advance(wait)
        h.until(lambda: len(h.recorded) == 1, 'the record after the last retry')
        assert h.count(('p', 'd')) == 4 and h.recorded[0].kind == PAIR_FAILED
        h.finish()

    def test_a_plain_failure_is_recorded_at_once_and_retried_within_an_hour(self, harness):
        h = harness({'p': ('d',)}, behaviour=lambda call: failed(call.key)).start()
        h.until(lambda: len(h.recorded) == 1, 'the record')
        h.until_sender_done(('p', 'd'), 1, 'the sender gone')
        assert h.schedule.due_time(('p', 'd')) <= h.clock.now + 3600.0 + 1
        h.finish()

    def test_the_kit_is_closed_before_the_outcome_is_recorded(self, harness):
        seen = []
        h = harness({'p': ('d',)})
        inner = h.loop._record

        def record(outcome):
            seen.append([k.closed for k in h.kits])
            inner(outcome)

        h.loop._record = record
        h.start()
        h.until(lambda: seen, 'the record')
        h.finish()
        assert seen == [[True]]

    def test_every_send_has_a_fresh_kit(self, harness):
        h = harness({'a': ('d',), 'b': ('d',)}).start()
        h.until(lambda: len(h.recorded) == 2, 'both')
        h.finish()
        assert len(h.kits) == 2 and h.kits[0] is not h.kits[1]
        assert all(k.closed for k in h.kits)


# ---------------------------------------------------------------------------------------------
# failures of a sender thread
# ---------------------------------------------------------------------------------------------


@pytest.mark.integration
class TestSenderFailures:
    @pytest.mark.parametrize('error', [
        RuntimeError('boom'), KeyboardInterrupt(), SystemExit(3), GeneratorExit(),
    ], ids=['RuntimeError', 'KeyboardInterrupt', 'SystemExit', 'GeneratorExit'])
    def test_an_exception_of_any_kind_is_a_failed_pair_the_loop_goes_on(self, harness, error, caplog):
        def behaviour(call):
            if call.key[0] == 'a':
                raise error
            return done(call.key)

        h = harness({'a': ('d',), 'b': ('d',)}, behaviour=behaviour)
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.until(lambda: len(h.recorded) == 2, 'both outcomes')
            h.finish()
        by = {o.dataset: o for o in h.recorded}
        assert by['a'].kind == PAIR_FAILED and by['a'].error == f'{type(error).__name__}: {error}'
        assert by['b'].kind == PAIR_DONE
        assert [r for r in caplog.records if 'error on a -> d' in r.getMessage() and r.exc_info]
        assert all(k.closed for k in h.kits)

    def test_an_exception_while_a_stop_is_pending_is_a_stopped_pair_not_recorded(self, harness):
        def behaviour(call):
            h.stop_event.set()
            raise ConnectionError('stream closed')

        h = harness({'a': ('d',)}, behaviour=behaviour)
        h.start()
        h.until(lambda: h.thread is not None and not h.thread.is_alive(), 'the loop to leave')
        assert h.recorded == []

    def test_a_kit_that_cannot_be_built_fails_that_pair_only(self, harness):
        def new_kit(template, remote_cfg):
            if template.name == 'a':
                raise ValueError('no manager')
            return SenderKit(MagicMock(), template, lambda: None)

        h = harness({'a': ('d',), 'b': ('d',)}, new_kit=new_kit).start()
        h.until(lambda: len(h.recorded) == 2, 'both outcomes')
        h.finish()
        by = {o.dataset: o for o in h.recorded}
        assert by['a'].kind == PAIR_FAILED and by['a'].error == 'ValueError: no manager'
        assert by['b'].kind == PAIR_DONE

    def test_a_kit_that_cannot_be_closed_does_not_lose_the_outcome(self, harness):
        def new_kit(template, remote_cfg):
            kit = SenderKit(MagicMock(), template, lambda: None)
            kit.close = MagicMock(side_effect=OSError('close failed'))
            return kit

        h = harness({'a': ('d',)}, new_kit=new_kit).start()
        h.until(lambda: len(h.recorded) == 1, 'the outcome')
        h.finish()
        assert h.recorded[0].kind == PAIR_DONE

    def test_a_thread_that_cannot_be_started_fails_that_pair_alone_and_soon(self, harness, monkeypatch, caplog):
        real = threading.Thread

        class Refusing(real):
            def start(self):
                if self.name == 'send:a->d':
                    raise RuntimeError("can't start new thread")
                super().start()

        monkeypatch.setattr(threading, 'Thread', Refusing)
        h = harness({'a': ('d',), 'b': ('d',)}, schedule_cls=SpySchedule)
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.until(lambda: any(c[0] == 'finished' and c[1] == ('a', 'd') for c in h.schedule.calls), 'the failure')
            h.until(lambda: h.count(('b', 'd')) == 1 and len(h.recorded) == 1, 'the other pair')
            h.finish()
        assert ('a', 'd') not in h.schedule.running and h.loop.sender_threads == {}
        assert ('finished', ('a', 'd'), 'failed', True) in h.schedule.calls, 'transient: a RETRYABLE failure (60 s)'
        assert [o.dataset for o in h.recorded] == ['b'], 'not recorded until the retries are used up'
        assert any('cannot start a sender for a -> d' in r.getMessage() for r in caplog.records)

    def test_a_thread_that_dies_without_an_outcome_frees_its_pair(self, harness, monkeypatch, caplog):
        monkeypatch.setattr(SendLoop, '_sender_main', lambda self_, *a, **k: None)
        h = harness({'a': ('d',)})
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.until(lambda: len(h.recorded) == 1, 'the synthesized outcome')
            h.finish()
        assert h.recorded[0].kind == PAIR_FAILED and 'ended without an outcome' in h.recorded[0].error
        assert ('a', 'd') not in h.schedule.running
        assert any('ended without an outcome' in r.getMessage() for r in caplog.records)

    def test_a_record_callback_that_raises_does_not_stop_the_loop(self, harness, caplog):
        h = harness({'a': ('d',), 'b': ('d',)})
        h.loop._record = MagicMock(side_effect=OSError('disk full'))
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.until(lambda: h.loop._record.call_count == 2, 'both records attempted')
            h.finish()
        assert [r for r in caplog.records if 'cannot record' in r.getMessage()]

    def test_a_flush_callback_that_raises_does_not_stop_the_loop(self, harness, caplog):
        h = harness({'a': ('d',)})
        h.loop._flush = MagicMock(side_effect=OSError('disk full'))
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.until(lambda: h.recorded, 'the outcome')
            h.finish()
        assert [r for r in caplog.records if 'cannot flush' in r.getMessage()]


# ---------------------------------------------------------------------------------------------
# exit paths
# ---------------------------------------------------------------------------------------------


@pytest.mark.integration
class TestExitPaths:
    @pytest.mark.parametrize('reason', [ExitReason.RELOAD, ExitReason.UPGRADE, ExitReason.STOP,
                                        ExitReason.SUPERVISOR_GONE], ids=lambda r: r.name)
    def test_the_loop_returns_only_after_a_long_sender_and_gives_the_first_reason(self, harness, reason):
        h = harness({'p': ('d',)})
        release = h.on_release(threading.Event())
        seen = {}

        def behaviour(call):
            h.until(lambda: h.loop.leave.is_set() or h.stop_event.is_set(), 'leave')
            seen['at_poll'] = call.should_stop()            # the safe point between two chain steps
            release.wait(bound())                           # a stream in hand does not end at once
            return done(call.key)

        h.behaviour = behaviour
        h.start()
        h.until(lambda: h.calls, 'the sender')
        h.reason = reason
        if reason is ExitReason.STOP:
            h.stop_event.set()
        h.until(lambda: seen, 'the sender to see the exit')
        h.thread.join(0.3 * stretch())
        assert h.thread.is_alive(), 'the loop returned while a sender was still running'
        assert seen == {'at_poll': True} and h.loop.leave.is_set()
        release.set()
        h.thread.join(bound())
        assert not h.thread.is_alive() and h.result is reason and h.error is None
        assert [o.kind for o in h.recorded] == [PAIR_DONE], 'the outcome of the drained sender is recorded'
        assert h.loop.sender_threads == {}

    def test_a_stop_flag_is_seen_by_a_sender_before_the_scheduler_looks(self, harness):
        h = harness({'p': ('d',)})
        h.loop._exit_reason = lambda: h.reason     # the scheduler ignores the flag: only the flag can tell the sender
        release = h.on_release(threading.Event())
        seen = []

        def behaviour(call):
            h.stop_event.set()
            seen.append((call.should_stop(), h.loop.leave.is_set()))
            release.wait(bound())
            return done(call.key)

        h.behaviour = behaviour
        h.start()
        h.until(lambda: seen, 'the sender')
        assert seen == [(True, False)], 'through the flag at once; `leave` only when the scheduler looks'
        h.finish()

    def test_no_new_sender_starts_once_the_loop_is_leaving(self, harness):
        h = harness({'p': ('d',), 'q': ('d',)}, caps=(1, 1))
        release = h.on_release(threading.Event())

        def behaviour(call):
            release.wait(bound())
            return done(call.key)

        h.behaviour = behaviour
        h.start()
        h.until(lambda: len(h.calls) == 1, 'the first sender')
        h.reason = ExitReason.RELOAD
        h.until(lambda: h.loop.leave.is_set(), 'leave')
        release.set()
        h.thread.join(bound())
        assert h.sent() == [('p', 'd')], 'the second pair was due but never started'

    def test_a_scheduler_exception_never_leaves_a_live_sender(self, harness, caplog):
        h = harness({'p': ('d',)})
        ended = {}

        def behaviour(call):
            h.until(lambda: call.should_stop(), 'leave')      # set by the scheduler's `finally`
            time.sleep(0.2 * stretch())                       # still draining
            ended['at'] = time.monotonic()
            return done(call.key)

        h.behaviour = behaviour

        def exit_reason():
            if h.calls:
                raise RuntimeError('scheduler bug')
            return None

        h.loop._exit_reason = exit_reason
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.thread.join(bound())
        assert isinstance(h.error, RuntimeError) and 'scheduler bug' in str(h.error)
        assert ended and ended['at'] <= h.returned_at, 'run() raised only after the sender had ended'
        assert h.loop.sender_threads == {}
        text = ' '.join(r.getMessage() for r in caplog.records)
        assert 'the scheduler failed' in text and 'draining 1 running sender' in text
        failure = [r for r in caplog.records if 'the scheduler failed' in r.getMessage()]
        assert len(failure) == 1 and failure[0].exc_info is not None and failure[0].exc_info[0] is RuntimeError, 'with a traceback'

    def test_a_discovery_that_does_not_return_is_abandoned_after_a_bounded_join(self, harness, caplog):
        h = harness({'p': ('d',)}, parents=('p',), discovery_join=0.2)
        release = h.on_release(threading.Event())
        entered = threading.Event()

        def expand(parent, should_stop):
            entered.set()
            release.wait(bound())                  # a `zfs list` that is not coming back
            return SimpleNamespace(children=[], failed={}, skipped={})

        h.loop._expand = expand
        with caplog.at_level(logging.WARNING, logger=LOG):
            h.start()
            assert entered.wait(bound())
            started = time.monotonic()
            h.reason = ExitReason.STOP
            h.thread.join(bound())
            took = time.monotonic() - started
        assert not h.thread.is_alive() and took < 0.2 + 5.0 * stretch()
        assert any('is abandoning the discovery of p' in r.getMessage() for r in caplog.records)
        alive = [t for t in threading.enumerate() if t.name == 'discover:p']
        assert alive, 'the discovery thread was abandoned, not killed'
        release.set()
        for t in alive:
            t.join(bound())

    def test_a_discovery_result_that_arrives_while_leaving_is_ignored(self, harness):
        h = harness({'p': ('d',)}, parents=('p',))
        release = h.on_release(threading.Event())
        entered = threading.Event()

        def expand(parent, should_stop):
            entered.set()
            release.wait(bound())
            return SimpleNamespace(children=[dsi_of('p/c', expanded_from='p')], failed={}, skipped={})

        h.loop._expand = expand
        h.start()
        assert entered.wait(bound())
        h.reason = ExitReason.RELOAD
        h.until(lambda: h.loop.leave.is_set(), 'leave')
        release.set()
        h.thread.join(bound())
        assert h.loop._children == {} and h.loop._failed == {}, 'a list cut short by an exit is no truth'

    def test_a_status_heartbeat_is_written_during_a_long_drain(self, harness):
        h = harness({'p': ('d',)}, heartbeat_every=0.05)
        release = h.on_release(threading.Event())

        def behaviour(call):
            release.wait(bound())
            return done(call.key)

        h.behaviour = behaviour
        h.start()
        h.until(lambda: h.calls, 'the sender')
        h.reason = ExitReason.RELOAD
        h.until(lambda: h.loop.leave.is_set(), 'leave')
        before = len(h.flushes)
        h.until(lambda: len(h.flushes) >= before + 3, 'heartbeats while draining')
        release.set()
        h.thread.join(bound())
        assert all(f == (True, True) for f in h.flushes if f[1])

    def test_the_status_is_flushed_at_the_start_and_then_as_a_heartbeat(self, harness):
        h = harness({'p': ('d',)}, heartbeat_every=0.05).start()
        h.until(lambda: len(h.flushes) >= 4, 'a few heartbeats')
        h.finish()
        assert h.flushes[0] == (True, True)

    def test_the_reminder_runs_every_tick_and_a_failing_one_changes_nothing(self, harness):
        calls = []

        def remind():
            calls.append(1)
            raise RuntimeError('advisory')

        h = harness({'p': ('d',)}, remind=remind).start()
        h.until(lambda: len(calls) >= 5, 'five ticks')
        h.until(lambda: h.recorded, 'the send')
        h.finish()


# ---------------------------------------------------------------------------------------------
# discovery threads
# ---------------------------------------------------------------------------------------------


def expansion(*children, failed=None):
    return SimpleNamespace(children=list(children), failed=failed or {}, skipped={})


class CountingSchedule(SendSchedule):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.updates = 0

    def update_pairs(self, pairs, now=None):
        self.updates += 1
        return super().update_pairs(pairs, now)


class DispatchCountingSchedule(SendSchedule):
    """Counts the scheduler's looks (`dispatch` is called once per tick)."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.dispatches = 0

    def dispatch(self, now=None, running=None):
        self.dispatches += 1
        return super().dispatch(now, running)


@pytest.mark.integration
class TestDiscovery:
    def test_every_parent_is_expanded_once_and_its_children_are_scheduled(self, harness):
        seen = []

        def expand(parent, should_stop):
            seen.append((parent, callable(should_stop)))
            return expansion(dsi_of(f'{parent}/c', expanded_from=parent))

        h = harness({'p': ('d',), 'q': ('d',)}, parents=('p', 'q'), expand=expand).start()
        h.until(lambda: {('p/c', 'd'), ('q/c', 'd')} <= set(h.sent()), 'both children sent')
        h.finish()
        assert sorted(seen) == [('p', True), ('q', True)]

    def test_at_most_four_discoveries_run_at_once(self, harness):
        h = harness({f'p{i}': ('d',) for i in range(6)}, parents=tuple(f'p{i}' for i in range(6)))
        release = h.on_release(threading.Event())
        active, peak = set(), [0]

        def expand(parent, should_stop):
            with h.lock:
                active.add(parent)
                peak[0] = max(peak[0], len(active))
            release.wait(bound())
            with h.lock:
                active.discard(parent)
            return expansion()

        h.loop._expand = expand
        h.start()
        h.until(lambda: len(active) == MAX_DISCOVERIES, 'four discoveries')
        time.sleep(0.2 * stretch())
        assert len(active) == MAX_DISCOVERIES
        release.set()
        h.until_discoveries(6, 'all six done')
        assert peak[0] == MAX_DISCOVERIES
        h.finish()

    def test_a_parent_is_never_expanded_twice_at_once(self, harness):
        h = harness({'p': ('d',)}, parents=('p',))
        release = h.on_release(threading.Event())
        calls = []

        def expand(parent, should_stop):
            calls.append(parent)
            release.wait(bound())
            return expansion()

        h.loop._expand = expand
        h.start()
        h.until(lambda: calls, 'the first discovery')
        h.clock.advance(100_000)                         # long past its next time
        time.sleep(0.2 * stretch())
        assert calls == ['p']
        h.finish()

    def test_the_pairs_are_replaced_only_when_the_child_names_changed(self, harness):
        answers = [expansion(dsi_of('p/c', expanded_from='p')), expansion(dsi_of('p/c', expanded_from='p')),
                   expansion(dsi_of('p/c', expanded_from='p'), dsi_of('p/d', expanded_from='p'))]
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            return answers[min(len(calls), len(answers)) - 1]

        h = harness({'p': ('d',)}, parents=('p',), expand=expand, schedule_cls=CountingSchedule).start()
        h.until_discoveries(1, 'the first discovery')
        assert h.schedule.updates == 2, 'the start, then the first discovery'
        h.clock.advance(3601)
        h.until_discoveries(2, 'the second discovery')
        assert h.schedule.updates == 2, 'the same names: nothing replaced'
        h.clock.advance(3601)
        h.until_discoveries(3, 'the third discovery')
        assert h.schedule.updates == 3
        h.until(lambda: ('p/d', 'd') in h.schedule.keys(), 'the new child')
        h.finish()

    def test_a_failed_discovery_keeps_the_last_known_children(self, harness):
        answers = [expansion(dsi_of('p/c', expanded_from='p')), expansion(failed={'p': 'pool suspended'})]
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            return answers[min(len(calls), 2) - 1]

        h = harness({'p': ('d',)}, parents=('p',), expand=expand, schedule_cls=CountingSchedule).start()
        h.until(lambda: ('p/c', 'd') in h.schedule.keys(), 'the child')
        updates = h.schedule.updates
        h.clock.advance(3601)
        h.until_discoveries(2, 'the failing discovery')
        assert h.loop._failed == {'p': 'pool suspended'}
        assert ('p/c', 'd') in h.schedule.keys() and h.schedule.updates == updates
        h.finish()

    def test_a_failed_discovery_is_retried_within_five_minutes_not_an_hour(self, harness):
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            return expansion(failed={'p': 'boom'})

        h = harness({'p': ('d',)}, parents=('p',), expand=expand).start()
        h.until_discoveries(1, 'the first discovery')
        h.clock.advance(301)
        h.until(lambda: len(calls) == 2, 'the retry')
        h.finish()

    def test_a_parent_that_failed_before_its_discovery_returned_is_not_sent_again_at_once(self, harness):
        """The race the rig's gate hides: the parent is dispatched in the first tick, together with its
        discovery, and fails before the discovery returns. The children then appear and arm the parent's
        once-per-incarnation confirmation: it must wait for the parent's own retry (an hour for a plain
        failure), not fire `now` (a second attempt within seconds, a second failure on the record)."""
        gate = threading.Event()

        def expand(parent, should_stop):
            gate.wait(bound())
            return expansion(dsi_of('p/c', expanded_from='p'))

        def behaviour(call):
            return failed(call.key) if call.key == ('p', 'd') else done(call.key)

        h = harness({'p': ('d',)}, parents=('p',), expand=expand, behaviour=behaviour,
                    schedule_cls=DispatchCountingSchedule)
        h.on_release(gate)
        h.start()
        h.until_sender_done(('p', 'd'), 1, 'the first failure')
        assert h.count(('p', 'd')) == 1 and h.loop._children == {}, 'sent before its children were known'
        retry_at = h.schedule.due_time(('p', 'd'))
        gate.set()
        h.until_discoveries(1, 'the discovery')
        h.until(lambda: ('p/c', 'd') in h.schedule.keys(), 'the child')
        dispatches = h.schedule.dispatches
        h.until(lambda: h.schedule.dispatches >= dispatches + 5, 'five more looks at the schedule')
        assert h.count(('p', 'd')) == 1 and h.count(('p/c', 'd')) == 0
        assert [(o.dataset, o.kind) for o in h.recorded] == [('p', PAIR_FAILED)], 'one failure, recorded once'
        assert h.schedule.due_time(('p', 'd')) == retry_at, 'the retry timer governs the confirmation'
        h.clock.advance(3601)                      # the retry: now the confirmation is asked
        h.until(lambda: h.count(('p', 'd')) == 2, 'the retry')
        h.finish()

    def test_a_parent_whose_discovery_returned_while_it_ran_is_not_sent_again_at_once(self, harness):
        """The other ordering: the parent's send is still RUNNING when its discovery returns (the children appear
        and `_link` arms the confirmation `now`), and only then does the parent fail. `SendSchedule.finished`
        must hold the armed confirmation back to the parent's retry: one send, one failed record, no second
        attempt within seconds."""
        release = threading.Event()

        def expand(parent, should_stop):
            return expansion(dsi_of('p/c', expanded_from='p'))

        def behaviour(call):
            if call.key == ('p', 'd'):
                release.wait(bound())
                return failed(call.key)
            return done(call.key)

        h = harness({'p': ('d',)}, parents=('p',), expand=expand, behaviour=behaviour,
                    schedule_cls=DispatchCountingSchedule)
        h.on_release(release)
        h.start()
        h.until(lambda: h.count(('p', 'd')) == 1, 'the parent running')
        h.until_discoveries(1, 'the discovery handled while the parent runs')
        h.until(lambda: ('p/c', 'd') in h.schedule.keys(), 'the child')
        assert h.recorded == [], 'the parent has not failed yet'
        release.set()
        h.until_sender_done(('p', 'd'), 1, 'the failure')
        dispatches = h.schedule.dispatches
        h.until(lambda: h.schedule.dispatches >= dispatches + 5, 'five more looks at the schedule')
        assert h.count(('p', 'd')) == 1 and h.count(('p/c', 'd')) == 0
        assert [(o.dataset, o.kind) for o in h.recorded] == [('p', PAIR_FAILED)], 'one failure, recorded once'
        h.clock.advance(3601)                      # the retry: now the confirmation is asked
        h.until(lambda: h.count(('p', 'd')) == 2, 'the retry')
        h.finish()

    def test_a_discovery_that_raises_is_a_failed_discovery_and_keeps_the_children(self, harness, caplog):
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError('boom')
            return expansion(dsi_of('p/c', expanded_from='p'))

        h = harness({'p': ('d',)}, parents=('p',), expand=expand)
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.until(lambda: ('p/c', 'd') in h.schedule.keys(), 'the child')
            h.clock.advance(3601)
            h.until_discoveries(2, 'the crashing discovery')
            h.finish()
        assert 'RuntimeError' in h.loop._failed['p']
        assert ('p/c', 'd') in h.schedule.keys()
        assert any('could not expand p' in r.getMessage() for r in caplog.records)

    def test_a_discovery_thread_that_cannot_start_is_a_failed_discovery(self, harness, monkeypatch, caplog):
        real = threading.Thread

        class Refusing(real):
            def start(self):
                if self.name == 'discover:p':
                    raise RuntimeError("can't start new thread")
                super().start()

        monkeypatch.setattr(threading, 'Thread', Refusing)
        h = harness({'p': ('d',)}, parents=('p',))
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.until(lambda: 'p' in h.loop._failed, 'the failure')
            h.until(lambda: h.recorded, 'the parent is sent all the same')
            h.finish()
        assert any('cannot start the discovery of p' in r.getMessage() for r in caplog.records)
        assert 'p' not in h.loop._discoveries

    def test_status_records_of_children_survive_while_their_parent_is_unknown_and_go_with_the_child(
        self, harness, tmp_path
    ):
        (tmp_path / 'config.db').write_bytes(b'')
        directory = tmp_path / 'config.db.status'
        health.ensure_status_dir(directory)
        writer = health.StatusWriter(directory, 'remote-backup-worker')
        for name in ('p/c', 'p/gone', 'other/x'):
            writer.record(health.KIND_SEND, name, 'd', ok=True)
        answers = [expansion(failed={'p': 'boom'}), expansion(dsi_of('p/c', expanded_from='p'))]
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            return answers[min(len(calls), 2) - 1]

        h = harness({'p': ('d',)}, parents=('p',), expand=expand, status=writer).start()
        h.until_discoveries(1, 'the failed discovery')
        keys = {r.dataset for r in writer.records}
        assert {'p/c', 'p/gone'} <= keys, 'unknown, not gone'
        assert 'other/x' not in keys, 'a record that belongs to no parent goes'
        h.clock.advance(301)
        h.until_discoveries(2, 'the good discovery')
        h.until(lambda: 'p/gone' not in {r.dataset for r in writer.records}, 'the vanished child dropped')
        assert 'p/c' in {r.dataset for r in writer.records}
        h.finish()

    def test_a_new_pairs_catch_up_reads_its_status_record(self, harness, tmp_path):
        """A pair attempted a moment ago (its record) is not sent again at once: the restart does not re-send."""
        (tmp_path / 'config.db').write_bytes(b'')
        directory = tmp_path / 'config.db.status'
        health.ensure_status_dir(directory)
        clock = FakeClock()
        writer = health.StatusWriter(directory, 'remote-backup-worker', clock=clock)
        writer.record(health.KIND_SEND, 'recent', 'd', ok=True)
        h = harness({'recent': ('d',), 'fresh': ('d',)}, clock=clock, status=writer).start()
        h.until(lambda: h.count(('fresh', 'd')) == 1, 'the pair without a record')
        time.sleep(0.2 * stretch())
        assert h.count(('recent', 'd')) == 0
        h.finish()


@pytest.mark.integration
class TestClockReads:
    def test_now_is_read_after_the_tick_work_so_started_gets_the_slot_the_sender_serves(self, harness):
        """The tick's work before dispatch (the reminder here) can take long: `dispatch` and `started` must see
        the time AFTER it, not the time the tick began."""
        starts = []

        class Spy(SendSchedule):
            def started(self, key, now=None):
                starts.append((now, h.clock.now))
                return super().started(key, now)

        def remind():
            h.clock.advance(5.0)

        h = harness({'p': ('d',)}, schedule_cls=Spy, remind=remind)
        h.start()
        h.until(lambda: starts, 'the first start')
        h.finish()
        now, clock_then = starts[0]
        assert now == clock_then, 'started() was given a time read before the reminder ran'

    def test_announce_waiting_takes_the_wall_clock_and_the_monotonic_clock(self):
        import inspect
        assert list(inspect.signature(SendLoop._announce_waiting).parameters) == ['self', 'now', 'mono']


# ---------------------------------------------------------------------------------------------
# the R-2 announcement
# ---------------------------------------------------------------------------------------------


class Mono:
    """A monotonic clock the test moves."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.mark.integration
class TestWaitingEpisodes:
    def build(self, harness, outcomes):
        """Parent `p` and child `p/c` (one destination); `outcomes` is the list of the parent's outcomes."""
        mono = Mono()
        script = list(outcomes)

        def behaviour(call):
            if call.key == ('p', 'd'):
                return script.pop(0) if len(script) > 1 else script[0]
            return done(call.key)

        h = harness({'p': ('d',), 'p/c': ('d',)}, behaviour=behaviour, monotonic=mono)
        return h, mono

    @staticmethod
    def words(caplog, level):
        return [r.getMessage() for r in caplog.records if r.name == LOG and r.levelno == level]

    def settle(self, h, count=1):
        h.until_sender_done(('p', 'd'), count, f'the parent attempted {count} time(s) and its outcome handled')
        h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')

    def test_the_warning_comes_after_the_notice_and_once(self, harness, caplog):
        h, mono = self.build(harness, [done(('p', 'd'), confirmed=None)])
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            self.settle(h)
            mono.now += sendloop.WAITING_NOTICE - 1
            time.sleep(0.1 * stretch())
            assert self.words(caplog, logging.WARNING) == []
            mono.now += 2
            h.until(lambda: self.words(caplog, logging.WARNING), 'the WARNING')
            time.sleep(0.1 * stretch())
            h.finish()
        assert self.words(caplog, logging.WARNING) == [waiting_message('p', 'd', 1)]
        assert h.memory.noted(WAITING, ('p', 'd'))

    def test_a_running_ancestor_restarts_the_clock(self, harness, caplog):
        mono = Mono()
        release = threading.Event()

        def behaviour(call):
            if call.key == ('p', 'd'):
                release.wait(bound())
                return done(call.key)
            return done(call.key)

        h = harness({'p': ('d',), 'p/c': ('d',)}, behaviour=behaviour, monotonic=mono)
        h.on_release(release)
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            h.until(lambda: h.calls, 'the parent running')
            mono.now += 10 * sendloop.WAITING_NOTICE
            time.sleep(0.15 * stretch())
            assert self.words(caplog, logging.WARNING) == [], 'a sending ancestor is not a stuck one'
            release.set()
            h.until(lambda: h.count(('p/c', 'd')) == 1, 'the child')
            h.finish()
        assert self.words(caplog, logging.WARNING) == []

    def test_no_warning_and_no_info_when_the_parent_confirms_within_the_notice(self, harness, caplog):
        h, mono = self.build(harness, [done(('p', 'd'))])
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            h.until(lambda: h.count(('p/c', 'd')) == 1, 'the child')
            h.finish()
        assert self.words(caplog, logging.WARNING) == []
        assert not [m for m in self.words(caplog, logging.INFO) if 'confirmed at' in m]

    def test_the_info_ends_an_announced_episode(self, harness, caplog):
        h, mono = self.build(harness, [done(('p', 'd'), confirmed=None), done(('p', 'd'))])
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            self.settle(h)
            mono.now += sendloop.WAITING_NOTICE + 1
            h.until(lambda: self.words(caplog, logging.WARNING), 'the WARNING')
            h.clock.advance(3601)                                  # the parent's retry is due
            h.until(lambda: h.count(('p/c', 'd')) == 1, 'the child after the parent confirmed')
            h.finish()
        assert self.words(caplog, logging.INFO).count(released_message('p', 'd')) == 1
        assert not h.memory.noted(WAITING, ('p', 'd'))

    def test_a_new_episode_is_announced_again(self, harness, caplog):
        h, mono = self.build(harness, [
            done(('p', 'd'), confirmed=None), done(('p', 'd')), done(('p', 'd'), confirmed=False),
        ])
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            self.settle(h)
            mono.now += sendloop.WAITING_NOTICE + 1
            h.until(lambda: len(self.words(caplog, logging.WARNING)) == 1, 'the first WARNING')
            h.clock.advance(3601)
            h.until(lambda: h.count(('p/c', 'd')) == 1, 'released')
            h.clock.advance(3601)
            h.until_sender_done(('p', 'd'), 3, 'the copy found gone')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            mono.now += sendloop.WAITING_NOTICE + 1
            h.until(lambda: len(self.words(caplog, logging.WARNING)) == 2, 'the second WARNING')
            h.finish()

    def test_the_latch_is_shared_with_the_senders_so_an_episode_is_announced_once(self, harness, caplog):
        """`AncestorRule.hold` noted it first (a sender whose child ran): the scheduler adds no second WARNING."""
        h, mono = self.build(harness, [done(('p', 'd'), confirmed=None), done(('p', 'd'))])
        h.memory.note(WAITING, ('p', 'd'))
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            self.settle(h)
            mono.now += sendloop.WAITING_NOTICE + 1
            time.sleep(0.15 * stretch())
            assert self.words(caplog, logging.WARNING) == []
            h.clock.advance(3601)
            h.until(lambda: h.count(('p/c', 'd')) == 1, 'released')
            h.finish()
        assert self.words(caplog, logging.INFO).count(released_message('p', 'd')) == 1

    def test_a_grandchild_is_not_stuck_while_the_root_ancestor_sends(self, harness, caplog):
        """Three levels, one destination: `p` sends (a long chain), `p/c` is held behind it and `p/c/g` behind
        `p/c`. The intermediate ancestor is held too, but what it waits for is the ROOT, and the root is
        working: no WARNING however long the (monotonic) time passes."""
        mono = Mono()
        release = threading.Event()

        def behaviour(call):
            if call.key == ('p', 'd'):
                release.wait(bound())
            return done(call.key)

        h = harness({'p': ('d',), 'p/c': ('d',), 'p/c/g': ('d',)}, behaviour=behaviour, monotonic=mono)
        h.on_release(release)
        looks = [0]
        real_announce = h.loop._announce_waiting

        def counting_announce(now, mono_now):
            real_announce(now, mono_now)
            looks[0] += 1

        h.loop._announce_waiting = counting_announce
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            h.until(lambda: h.calls, 'the root running')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            assert h.schedule.held_by(('p/c/g', 'd')) == ('p/c', 'd'), 'the grandchild is held by the middle pair'
            assert h.schedule.waiting_on().keys() == {('p', 'd'), ('p/c', 'd')}
            mono.now += 10 * sendloop.WAITING_NOTICE
            looked = looks[0]
            h.until(lambda: looks[0] >= looked + 3, 'three more looks after the time moved')
            assert self.words(caplog, logging.WARNING) == [], 'the root is sending'
            release.set()
            h.until(lambda: h.count(('p/c/g', 'd')) == 1, 'the grandchild after the chain')
            h.finish()
        assert self.words(caplog, logging.WARNING) == []

    def test_a_stuck_root_is_announced_once_for_a_three_level_tree(self, harness, caplog):
        """The counterpart: the root is NOT working (sent, copy not confirmed): the walk up the holders ends at it,
        the WARNING names the root."""
        mono = Mono()
        h = harness({'p': ('d',), 'p/c': ('d',), 'p/c/g': ('d',)}, monotonic=mono,
                    behaviour=lambda call: done(call.key, confirmed=None))
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            self.settle(h)
            mono.now += sendloop.WAITING_NOTICE + 1
            h.until(lambda: waiting_message('p', 'd', 1) in self.words(caplog, logging.WARNING), 'the root WARNING')
            # Give a wrong second announcement (the held middle pair, `p/c`) every chance to show up.
            looks = [0]
            real_announce = h.loop._announce_waiting

            def counting_announce(now, mono_now):
                real_announce(now, mono_now)
                looks[0] += 1

            h.loop._announce_waiting = counting_announce
            mono.now += 10 * sendloop.WAITING_NOTICE
            looked = looks[0]
            h.until(lambda: looks[0] >= looked + 3, 'three more looks after the time moved')
            h.finish()
        warnings = self.words(caplog, logging.WARNING)
        assert warnings == [waiting_message('p', 'd', 1)], 'one WARNING in all: the root'
        assert not [w for w in warnings if 'p/c' in w], 'the held middle pair is never announced'
        assert not h.memory.noted(WAITING, ('p/c', 'd'))

    def test_an_episode_announced_while_topmost_still_ends_when_a_parent_appears_above_it(self, harness, caplog):
        """`p/c` holds `p/c/g` and, with no pair above it, is announced (WARNING). A discovery then adds `p`
        above it: `p/c` is itself held now and is never announced again, but the episode it already has must
        not be lost: when `p` and then `p/c` are confirmed the latch is cleared and the episode ends
        (INFO, or DEBUG when the confirmation is not known to the schedule), and `p` raises no WARNING."""
        mono = Mono()
        sends = {}
        discoveries = []

        def behaviour(call):
            sends[call.key] = sends.get(call.key, 0) + 1
            if call.key == ('p/c', 'd') and sends[call.key] == 1:
                return done(call.key, confirmed=None)
            return done(call.key)

        def expand(parent, should_stop):
            discoveries.append(1)
            if len(discoveries) == 1:
                return expansion()
            return expansion(dsi_of('p', expanded_from='q'))

        h = harness({'p/c': ('d',), 'p/c/g': ('d',), 'q': ('d',)}, parents=('q',), expand=expand,
                    behaviour=behaviour, monotonic=mono)
        with caplog.at_level(logging.DEBUG, logger=LOG):
            h.start()
            h.until_sender_done(('p/c', 'd'), 1, 'the first send of p/c')
            h.until_discoveries(1, 'the first discovery of q')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            mono.now += sendloop.WAITING_NOTICE + 1
            h.until(lambda: self.words(caplog, logging.WARNING) == [waiting_message('p/c', 'd', 1)],
                    'the WARNING for p/c')
            h.clock.advance(3601)                                  # q is expanded again: `p` appears
            h.until_discoveries(2, 'the second discovery')
            h.until(lambda: ('p', 'd') in h.schedule.keys(), 'p above p/c')
            h.until(lambda: sends.get(('p/c/g', 'd'), 0) >= 1, 'the chain released all the way down')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            mono.now += 10 * sendloop.WAITING_NOTICE
            h.until(lambda: ('p/c', 'd') not in h.loop._announced, 'the episode of p/c ended')
            h.finish()
        assert self.words(caplog, logging.WARNING) == [waiting_message('p/c', 'd', 1)], 'no second WARNING'
        ended = [m for level in (logging.INFO, logging.DEBUG) for m in self.words(caplog, level)
                 if released_message('p/c', 'd') == m or m.startswith('p/c -> d: no descendant')]
        assert len(ended) == 1, 'the episode ends with one INFO or DEBUG line'
        assert not h.memory.noted(WAITING, ('p/c', 'd'))

    def test_an_announced_episode_confirmed_in_the_batch_of_an_unrelated_rebuild_still_logs_its_info(
        self, harness, caplog
    ):
        """`a` holds its CONFIGURED child `a/k` (so `a` has no expanded dependents), the WARNING was logged. The
        scheduler then handles `a`'s confirmation and, in the very next batch and before it announces anything, an
        unrelated parent's discovery that changed names (a rebuild). The rebuild's latch housekeeping must not
        drop `a`'s WAITING latch: the release INFO depends on it."""
        mono = Mono()
        second_send, second_discovery = threading.Event(), threading.Event()
        park, parked, unpark = threading.Event(), threading.Event(), threading.Event()
        sends, discoveries = [], []

        def behaviour(call):
            if call.key == ('a', 'd'):
                sends.append(1)
                if len(sends) == 1:
                    return done(call.key, confirmed=None)
                second_send.wait(bound())
            return done(call.key)

        def expand(parent, should_stop):
            discoveries.append(1)
            if len(discoveries) > 1:
                second_discovery.wait(bound())
                return expansion(dsi_of('q/x', expanded_from='q'), dsi_of('q/y', expanded_from='q'))
            return expansion(dsi_of('q/x', expanded_from='q'))

        def remind():
            if park.is_set():
                parked.set()
                unpark.wait(bound())

        h = harness({'a': ('d',), 'a/k': ('d',), 'q': ('d',)}, parents=('q',), expand=expand,
                    behaviour=behaviour, monotonic=mono, remind=remind)
        for event in (second_send, second_discovery, unpark):
            h.on_release(event)
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            h.until_sender_done(('a', 'd'), 1, 'the first send of a')
            h.until_discoveries(1, 'the first discovery of q')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            mono.now += sendloop.WAITING_NOTICE + 1
            h.until(lambda: self.words(caplog, logging.WARNING) == [waiting_message('a', 'd', 1)], 'the WARNING')
            h.clock.advance(3601)                      # a is asked again, q's discovery is due again
            h.until(lambda: len(sends) == 2 and len(discoveries) == 2, 'both running')
            park.set()
            h.until(parked.is_set, 'the scheduler parked between two batches')
            second_send.set()                          # a's confirmation reaches the queue first ...
            h.until(lambda: h.loop._outbox.qsize() == 1, 'the confirmation queued')
            second_discovery.set()                     # ... then the names change
            h.until(lambda: h.loop._outbox.qsize() == 2, 'the discovery queued')
            unpark.set()
            h.until_sender_done(('a', 'd'), 2)
            h.until_discoveries(2)
            h.until(lambda: ('q/y', 'd') in h.schedule.keys(), 'the rebuild')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            h.finish()
        assert self.words(caplog, logging.INFO).count(released_message('a', 'd')) == 1
        assert not h.memory.noted(WAITING, ('a', 'd'))

    def test_a_queued_ancestor_is_not_a_stuck_one(self, harness, caplog):
        """The reviewer's repro: the ancestor is DUE but waiting for a free slot (caps), its child is held:
        no WARNING however long the (monotonic) time passes."""
        mono = Mono()
        release = threading.Event()

        def behaviour(call):
            if call.key[0] == 'blocker':
                release.wait(bound())
            return done(call.key)

        h = harness({'blocker': ('d',), 'p': ('d',), 'p/c': ('d',)}, behaviour=behaviour, monotonic=mono, caps=(1, 1))
        h.on_release(release)
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            h.until(lambda: h.sent() == [('blocker', 'd')], 'the blocker running')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            mono.now += 20 * sendloop.WAITING_NOTICE
            time.sleep(0.15 * stretch())
            assert self.words(caplog, logging.WARNING) == [], 'p is queued behind the cap, not stuck'
            release.set()
            h.until(lambda: h.count(('p/c', 'd')) == 1, 'the child')
            h.finish()
        assert self.words(caplog, logging.WARNING) == []

    def test_an_episode_that_ends_without_a_confirmation_is_a_debug_not_an_info(self, harness, caplog):
        """The held child leaves the pair set (its dataset was removed): the latch goes, but "its descendants
        follow" is not true, so no INFO."""
        mono = Mono()
        child = dsi_of('p/c', expanded_from='p')
        answers = [expansion(child), expansion()]
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            return answers[min(len(calls), 2) - 1]

        h = harness({'p': ('d',)}, parents=('p',), expand=expand, monotonic=mono,
                    behaviour=lambda call: done(call.key, confirmed=None))
        with caplog.at_level(logging.DEBUG, logger=LOG):
            h.start()
            h.until_discoveries(1, 'the child known')
            h.until_sender_done(('p', 'd'), 1, 'the held child')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            mono.now += sendloop.WAITING_NOTICE + 1
            h.until(lambda: self.words(caplog, logging.WARNING), 'the WARNING')
            h.clock.advance(3601)                                  # the next discovery: the child is gone
            h.until(lambda: ('p/c', 'd') not in h.schedule.keys(), 'the child gone')
            h.until(lambda: not h.loop._held_dirty, 'the scheduler to have looked')
            h.finish()
        assert released_message('p', 'd') not in self.words(caplog, logging.INFO)
        assert not h.memory.noted(WAITING, ('p', 'd')), 'the rebuild dropped the latch: nobody waits for it'

    def test_nothing_is_announced_for_an_ancestor_the_memory_already_confirmed(self, harness, caplog):
        """The outcome that tells the schedule is still to come: not a stuck ancestor."""
        h, mono = self.build(harness, [done(('p', 'd'), confirmed=None)])
        h.memory.note(COPY_CONFIRMED, ('p', 'd'))
        with caplog.at_level(logging.INFO, logger=LOG):
            h.start()
            self.settle(h)
            mono.now += 10 * sendloop.WAITING_NOTICE
            time.sleep(0.15 * stretch())
            h.finish()
        assert self.words(caplog, logging.WARNING) == []


# ---------------------------------------------------------------------------------------------
# the rule the senders use and the latch housekeeping
# ---------------------------------------------------------------------------------------------


@pytest.mark.integration
class TestRuleWiring:
    def tree(self, harness, **kw):
        child = dsi_of('p/c', expanded_from='p')
        grandchild = dsi_of('p/c/g', expanded_from='p')
        configured = dsi_of('p/k')                             # a configured child: not counted
        h = harness({'p': ('d',), 'p/k': ('d',)}, parents=('p',),
                    expand=lambda parent, should_stop: expansion(child, grandchild), **kw)
        return h, configured

    def test_the_rule_reads_the_pairs_of_this_rebuild_and_the_shared_memory(self, harness):
        h, _ = self.tree(harness)
        h.start()
        h.until(lambda: ('p/c/g', 'd') in h.schedule.keys(), 'the children')
        h.until(lambda: h.recorded, 'a send')
        h.finish()
        rule = h.calls[-1].rule
        assert rule.memory is h.memory
        assert set(rule.pairs) == {'p', 'p/k', 'p/c', 'p/c/g'}
        assert rule.nearest_pair('p/c/g', 'd').name == 'p/c'
        assert rule.nearest_pair('p/c', 'd').name == 'p'

    def test_only_expanded_children_count_as_dependents(self, harness):
        h, _ = self.tree(harness)
        h.start()
        h.until(lambda: ('p/c/g', 'd') in h.schedule.keys(), 'the children')
        h.finish()
        assert h.loop._rule.dependents == {('p', 'd'): 1, ('p/c', 'd'): 1}
        assert ('p/k', 'd') not in h.loop._rule.dependents

    def test_a_rebuild_makes_a_new_rule_and_never_changes_the_old_one(self, harness):
        answers = [expansion(), expansion(dsi_of('p/c', expanded_from='p'))]
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            return answers[min(len(calls), 2) - 1]

        h = harness({'p': ('d',)}, parents=('p',), expand=expand).start()
        h.until_discoveries(1, 'the first discovery')
        old = h.loop._rule
        old_pairs, old_dependents = dict(old.pairs), dict(old.dependents)
        h.clock.advance(3601)
        h.until_discoveries(2, 'the second discovery')
        assert h.loop._rule is not old
        assert old.pairs == old_pairs and old.dependents == old_dependents, 'a rule in use is never mutated'
        assert 'p/c' in h.loop._rule.pairs
        h.finish()

    def test_the_orphan_latch_of_a_pair_that_left_is_dropped(self, harness):
        answers = [expansion(dsi_of('p/c', expanded_from='p')), expansion()]
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            return answers[min(len(calls), 2) - 1]

        h = harness({'p': ('d',)}, parents=('p',), expand=expand).start()
        h.until(lambda: ('p/c', 'd') in h.schedule.keys(), 'the child')
        h.memory.note(ORPHAN_HOLD, ('p/c', 'd'))
        h.memory.note(ORPHAN_HOLD, ('p', 'd'))
        h.clock.advance(3601)
        h.until(lambda: ('p/c', 'd') not in h.schedule.keys(), 'the child gone')
        assert not h.memory.noted(ORPHAN_HOLD, ('p/c', 'd')), 'the pair left: its latch goes'
        assert h.memory.noted(ORPHAN_HOLD, ('p', 'd')), 'a live pair keeps its own'
        h.finish()

    def test_the_waiting_latch_is_kept_only_for_ancestors_that_still_have_dependents(self, harness):
        child, grandchild = dsi_of('p/c', expanded_from='p'), dsi_of('p/c/g', expanded_from='p')
        answers = [expansion(child), expansion(child, grandchild)]
        calls = []

        def expand(parent, should_stop):
            calls.append(1)
            return answers[min(len(calls), 2) - 1]

        h = harness({'p': ('d',)}, parents=('p',), expand=expand).start()
        h.until_discoveries(1, 'the child')
        assert ('p/c', 'd') in h.schedule.keys()
        h.memory.note(WAITING, ('gone', 'd'))
        h.memory.note(WAITING, ('p', 'd'))
        h.clock.advance(3601)                                  # the next discovery finds one more: a rebuild
        h.until(lambda: ('p/c/g', 'd') in h.schedule.keys(), 'the grandchild')
        h.finish()
        assert not h.memory.noted(WAITING, ('gone', 'd')), 'nobody waits for it any more'
        assert h.memory.noted(WAITING, ('p', 'd')), 'its dependents are still there'

    def test_a_pair_that_cannot_be_scheduled_is_one_error_not_a_dead_loop(self, harness, caplog):
        h = harness({'good': ('d',), 'zero': ('d',)})
        for dsi in h.configured:
            if dsi.name == 'zero':
                dsi.config.frequency = timedelta(seconds=0)
        with caplog.at_level(logging.ERROR, logger=LOG):
            h.start()
            h.until(lambda: h.count(('good', 'd')) == 1, 'the good pair')
            h.finish()
        assert h.count(('zero', 'd')) == 0
        assert len([r for r in caplog.records if 'zero -> d is not scheduled' in r.getMessage()]) == 1


# ---------------------------------------------------------------------------------------------
# forced sends: a retry and the continuation of a cut chain skip the anchor-age check
# ---------------------------------------------------------------------------------------------


def stub_records(*rows):
    """`rows`: `(kind, dataset, destination, result, last_attempt, incomplete)`; -> a status stand-in that
    holds `health.OutcomeRecord`s (the loop only reads `.records`)."""
    return SimpleNamespace(records=[
        health.OutcomeRecord(
            kind=kind, dataset=ds, destination=dest, result=result, last_attempt=attempt, incomplete=incomplete,
        )
        for kind, ds, dest, result, attempt, incomplete in rows
    ])


T = FakeClock().now                  # the instant every `Harness` starts at
SEND, SNAP = health.KIND_SEND, health.KIND_SNAPSHOT


@pytest.mark.integration
class TestStatusSeedsOfTheSchedule:
    """`SendLoop._cut_pairs` / `_failed_pairs` read the previous incarnation's send records, and `_rebuild` hands the
    two bits to the schedule as `PairSpec.resume` / `PairSpec.failed`."""

    def loop_over(self, harness, *rows, pairs=None):
        h = harness(pairs or {'a': ('d',)}, status=stub_records(*rows))
        return h, h.loop

    def test_no_status_means_no_seeds(self, harness):
        h = harness({'a': ('d',)})
        assert h.loop._cut_pairs() == set() and h.loop._failed_pairs() == set()

    def test_an_incomplete_send_record_is_a_cut_pair(self, harness):
        _, loop = self.loop_over(harness, (SEND, 'a', 'd', 'ok', T - 5, True))
        assert loop._cut_pairs() == {('a', 'd')} and loop._failed_pairs() == set()

    def test_a_plain_success_is_neither(self, harness):
        _, loop = self.loop_over(harness, (SEND, 'a', 'd', 'ok', T - 5, False))
        assert loop._cut_pairs() == set() and loop._failed_pairs() == set()

    def test_an_error_record_with_a_last_attempt_is_a_failed_pair(self, harness):
        _, loop = self.loop_over(harness, (SEND, 'a', 'd', 'error', T - 5, False))
        assert loop._failed_pairs() == {('a', 'd')} and loop._cut_pairs() == set()

    def test_an_error_record_without_a_last_attempt_is_ignored(self, harness):
        _, loop = self.loop_over(harness, (SEND, 'a', 'd', 'error', None, False))
        assert loop._failed_pairs() == set()

    def test_other_kinds_of_record_are_ignored(self, harness):
        _, loop = self.loop_over(
            harness,
            (SNAP, 'a', None, 'error', T - 5, False), (SNAP, 'b', None, 'ok', T - 5, True),
            (health.KIND_PRUNE, 'c', None, 'error', T - 5, False),
            (health.KIND_DISCOVER, 'bk', 'd', 'error', T - 5, False),
        )
        assert loop._cut_pairs() == set() and loop._failed_pairs() == set()

    def test_last_attempts_leave_a_cut_record_out_and_keep_a_failure(self, harness):
        _, loop = self.loop_over(
            harness,
            (SEND, 'cut', 'd', 'ok', T - 5, True), (SEND, 'bad', 'd', 'error', T - 6, False),
            (SEND, 'good', 'd', 'ok', T - 7, False),
        )
        assert loop._last_attempts() == {('bad', 'd'): T - 6, ('good', 'd'): T - 7}

    def test_rebuild_passes_the_flags_into_the_specs(self, harness):
        h = harness(
            {n: ('d',) for n in ('cut', 'bad', 'good', 'snapfail', 'nodate')},
            status=stub_records(
                (SEND, 'cut', 'd', 'ok', T - 5, True), (SEND, 'bad', 'd', 'error', T - 5, False),
                (SEND, 'good', 'd', 'ok', T - 5, False), (SNAP, 'snapfail', None, 'error', T - 5, False),
                (SEND, 'nodate', 'd', 'error', None, False),
            ),
        )
        h.loop._rebuild(h.clock.now)
        flags = {k.dataset: (h.schedule.spec(k).resume, h.schedule.spec(k).failed) for k in h.schedule.keys()}
        assert flags == {
            'cut': (True, False), 'bad': (False, True), 'good': (False, False),
            'snapfail': (False, False), 'nodate': (False, False),
        }
        assert h.schedule.spec(('cut', 'd')).last_attempt is None, 'a cut record is not an attempt'
        assert h.schedule.spec(('bad', 'd')).last_attempt == T - 5, 'a failure is'

    def test_a_pair_that_appears_later_is_seeded_from_its_record_too(self, harness):
        """A child found by a later discovery: `_rebuild` runs again and the new pair takes its seed."""
        h = harness(
            {'p': ('d',)}, parents=('p',),
            status=stub_records((SEND, 'p/c', 'd', 'ok', T - 5, True), (SEND, 'p/e', 'd', 'error', T - 5, False)),
        )
        h.loop._children['p'] = [dsi_of('p/c', expanded_from='p'), dsi_of('p/e', expanded_from='p')]
        h.loop._rebuild(h.clock.now)
        assert h.schedule.spec(('p/c', 'd')).resume is True and h.schedule.spec(('p/e', 'd')).failed is True
        assert (h.schedule.spec(('p', 'd')).resume, h.schedule.spec(('p', 'd')).failed) == (False, False)


class RealDue:
    """A `Harness` whose senders run the REAL `send_pair` (so the real due check runs) over a manager double
    whose anchor `anchor_age` old is the destination's, for a pair with F = 1 h and P = 1 d. `answers` is the
    script of `backup_dataset` results, `(done, retryable)` each (default: `(True, False)`, a settle). Records
    in `sends` what every send_pair call was given (`force_confirm`) and what it came to; `backups` is every
    call that got as far as the transport manager.

    Fraction 0 and a FakeClock at 08:00 UTC: a new pair is due at once, and its daily slot is at 00:06."""

    PERIOD, FREQUENCY = 86400.0, 3600.0
    KEY = ('p', 'd')

    def __init__(self, harness, monkeypatch, status=None, *, anchor_age=timedelta(days=2), answers=()):
        self.anchor_age = anchor_age
        self.answers = list(answers)
        self.backups, self.sends = [], []
        #: set by a test: `backup_dataset` waits for it (a send that is still running)
        self.gate = None
        manager = MagicMock(name='manager')
        manager.get_own_anchor.side_effect = lambda dsi, destination: 'anchor'
        manager.list_snapshots.side_effect = lambda dsi: [SimpleNamespace(name='anchor', age=self.anchor_age)]
        manager.clock_behind.return_value = False
        sender = MagicMock(name='sender')
        sender.last_problem.return_value = 'the copy is not ours'

        def backup(dsi, remote_cfg, should_stop=None):
            self.backups.append((dsi.name, remote_cfg.destination))
            if self.gate is not None:
                self.gate.wait(bound())
            done, retryable = self.answers.pop(0) if self.answers else (True, False)
            sender.last_retryable.return_value = retryable
            return done

        sender.backup_dataset.side_effect = backup

        def new_kit(template, remote_cfg):
            return SenderKit(manager, template, lambda: sender)

        self.h = harness(
            {'p': ('d',)}, period=self.PERIOD, new_kit=new_kit, schedule_cls=SpySchedule, status=status,
        )
        self.h.configured[0].config.frequency = timedelta(seconds=self.FREQUENCY)

        def spy(manager, dsi, remote_cfg, sender, should_stop, exit_pending, **kw):
            outcome = real_send_pair(manager, dsi, remote_cfg, sender, should_stop, exit_pending, **kw)
            self.sends.append((kw.get('force_confirm'), outcome.kind))
            return outcome

        monkeypatch.setattr(sendloop, 'send_pair', spy)

    def finished(self):
        return [(c[2], c[3]) for c in self.h.schedule.calls if c[0] == 'finished']

    def settle(self, count):
        self.h.until_sender_done(self.KEY, count, f'{count} outcome(s)')


@pytest.fixture
def due(harness, monkeypatch):
    def build(*, status=None, **kw):
        return RealDue(harness, monkeypatch, status, **kw)
    return build


@pytest.mark.integration
class TestForcedThroughTheRealDueCheck:
    """P = 1 d, F = 1 h, and the anchor is younger than P. The schedule decides when a pair sends: every dispatch
    it makes (on-time and late slots, catch-ups, retries, continuations) is FORCED past the sender's anchor-age
    check, which would call it NOT_DUE. The one dispatch that keeps the check is the re-run of an attempted slot
    that a clock step back causes, where NOT_DUE is exactly the point."""

    YOUNG = timedelta(hours=1)
    DAY = 86400.0
    #: the daily slot (fraction 0: offset 360 s) at or before the harness clock (08:00 UTC)
    SLOT = T - T % 86400 + 360

    def seeded(self, tmp_path, *rows):
        """A real status file: `rows` `(result, age_seconds, incomplete)` of the one pair, newest last."""
        (tmp_path / 'config.db').write_bytes(b'')
        directory = tmp_path / 'config.db.status'
        health.ensure_status_dir(directory)
        box = [T]
        writer = health.StatusWriter(directory, 'remote-backup-worker', clock=lambda: box[0])
        for result, age, incomplete in rows:
            box[0] = T - age
            writer.record(SEND, 'p', 'd', ok=result == 'ok', error=None if result == 'ok' else 'earlier', incomplete=incomplete)
        return writer

    def test_a_cut_chain_is_continued_past_a_young_anchor_and_ends_ok(self, due, tmp_path):
        r = due(status=self.seeded(tmp_path, ('ok', 2 * 86400, True)), anchor_age=self.YOUNG)
        r.h.start()
        r.settle(1)
        r.h.finish()
        assert r.sends == [(True, PAIR_DONE)] and r.backups == [r.KEY], 'it reached backup_dataset'
        assert r.finished() == [('ok', False)]

    def test_an_ordinary_catch_up_is_forced_too(self, due, tmp_path):
        """An ordinary success two days old is a missed slot: the catch-up reaches the transport (and settles as
        up to date) instead of being called NOT_DUE by the young anchor."""
        r = due(status=self.seeded(tmp_path, ('ok', 2 * 86400, False)), anchor_age=self.YOUNG)
        r.h.start()
        r.settle(1)
        r.h.finish()
        assert r.sends == [(True, PAIR_DONE)] and r.backups == [r.KEY]

    def test_a_new_pair_with_no_record_is_forced_too(self, due):
        r = due(anchor_age=self.YOUNG)
        r.h.start()
        r.settle(1)
        r.h.finish()
        assert r.sends == [(True, PAIR_DONE)] and r.backups == [r.KEY]

    def test_a_forced_attempt_with_nothing_pending_is_done_and_not_retryable(self, due, tmp_path):
        r = due(status=self.seeded(tmp_path, ('ok', 60, True)), anchor_age=self.YOUNG, answers=[(True, False)])
        r.h.start()
        r.settle(1)
        r.h.finish()
        (outcome,) = r.h.recorded
        assert outcome.kind == PAIR_DONE and outcome.retryable is False, 'so its record is not marked incomplete'
        assert r.finished() == [('ok', False)]
        assert r.h.schedule.wants_anchor_check(r.KEY) is False

    def test_a_forced_attempt_cut_again_stays_a_cut_chain(self, due, tmp_path):
        r = due(status=self.seeded(tmp_path, ('ok', 60, True)), anchor_age=self.YOUNG, answers=[(True, True)])
        r.h.start()
        r.settle(1)
        r.h.finish()
        (outcome,) = r.h.recorded
        assert outcome.kind == PAIR_DONE and outcome.retryable is True
        assert r.h.schedule.wants_anchor_check(r.KEY) is False, 'its 60 s retry continues the chain, forced'

    def test_after_a_failure_whose_attempt_moved_the_anchor_the_retry_reaches_the_transport(self, due):
        """A non-retryable FAILED (some steps went before it failed, so the anchor is young now): the retry, an
        hour later, must reach `backup_dataset` instead of being called NOT_DUE."""
        r = due(answers=[(False, False), (True, False)])           # an old anchor: the first attempt is due
        r.h.start()
        r.settle(1)
        assert r.sends == [(True, PAIR_FAILED)]
        r.anchor_age = self.YOUNG                                   # what the failed chain left behind
        r.h.clock.advance(3601)
        r.settle(2)
        r.h.finish()
        assert r.sends == [(True, PAIR_FAILED), (True, PAIR_DONE)]
        assert r.backups == [r.KEY, r.KEY]
        assert r.finished() == [('failed', False), ('ok', False)]

    def test_an_on_time_slot_after_an_ok_is_forced_and_reaches_the_transport(self, due):
        r = due(answers=[(True, False), (True, False)])
        r.h.start()
        r.settle(1)
        r.anchor_age = self.YOUNG                                   # the OK's anchor: far younger than P
        r.h.clock.now = self.SLOT + self.DAY                        # the next daily slot, exactly on time
        r.settle(2)
        r.h.finish()
        assert r.sends == [(True, PAIR_DONE), (True, PAIR_DONE)]
        assert r.backups == [r.KEY, r.KEY], 'the young anchor did not turn the slot into a NOT_DUE'

    def test_a_late_slot_is_forced_too(self, due):
        r = due(answers=[(True, False), (True, False)])
        r.h.start()
        r.settle(1)
        r.anchor_age = self.YOUNG
        r.h.clock.advance(self.DAY)                                 # 8 h after the slot: a catch-up
        r.settle(2)
        r.h.finish()
        assert r.sends == [(True, PAIR_DONE), (True, PAIR_DONE)]

    def test_the_rerun_of_a_slot_after_a_clock_step_back_keeps_the_check_and_is_not_due(self, due):
        """The one unforced dispatch: the attempted slot runs again after the clock went back past it."""
        r = due(answers=[(True, False), (True, False)])
        r.h.start()
        r.settle(1)
        r.anchor_age = self.YOUNG
        assert r.sends == [(True, PAIR_DONE)]
        r.h.clock.now = self.SLOT - 4 * 3600                        # back before the slot that was served
        r.h.until(lambda: r.h.schedule.wants_anchor_check(r.KEY), 'the schedule to notice the step back')
        r.h.clock.now = self.SLOT                                   # the clock reaches the slot again
        r.settle(2)
        assert r.sends == [(True, PAIR_DONE), (False, PAIR_NOT_DUE)], 'unforced, and a cheap NOT_DUE'
        assert r.backups == [r.KEY], 'the transport was not asked again'
        assert r.h.schedule.wants_anchor_check(r.KEY) is False
        # and the next ordinary slot is forced again
        r.h.clock.now = self.SLOT + self.DAY
        r.settle(3)
        r.h.finish()
        assert r.sends[-1] == (True, PAIR_DONE) and r.backups == [r.KEY, r.KEY]

    def test_a_step_back_during_a_send_is_not_erased_by_that_sends_outcome(self, due):
        """The clock goes back past the slot being served while its send runs: when the send finishes, the next
        dispatch of that slot is the checked re-run (NOT_DUE for a young anchor)."""
        r = due(answers=[(True, False), (True, False)])
        r.gate = r.h.on_release(threading.Event())
        r.h.start()
        r.h.until(lambda: r.backups, 'the send to be running')
        r.anchor_age = self.YOUNG
        r.h.clock.now = self.SLOT - 4 * 3600                        # back before the slot the running send serves
        r.gate.set()
        r.settle(1)
        r.h.until(lambda: r.h.schedule.wants_anchor_check(r.KEY), 'the re-armed check')
        r.h.clock.now = self.SLOT
        r.settle(2)
        r.h.finish()
        assert r.sends == [(True, PAIR_DONE), (False, PAIR_NOT_DUE)]
        assert r.backups == [r.KEY]

    def test_a_late_catch_up_and_a_retry_leave_a_young_anchor_and_the_next_on_time_slot_still_sends(self, due):
        """The gap the anchor-age check used to cause: a catch-up 8 h late (or a retry an hour after a failure)
        leaves an anchor taken AFTER its slot, which the next on-time slot would have found "too young" and
        skipped, a gap approaching 2 P. Every dispatch is forced, so it sends."""
        r = due(answers=[(False, False), (True, False), (True, False)])
        r.h.start()
        r.settle(1)                                                  # the catch-up (8 h late) fails
        r.anchor_age = self.YOUNG
        r.h.clock.advance(3601)                                      # the retry an hour on
        r.settle(2)
        r.h.clock.now = self.SLOT + self.DAY                         # and the next on-time slot
        r.settle(3)
        r.h.finish()
        assert r.sends == [(True, PAIR_FAILED), (True, PAIR_DONE), (True, PAIR_DONE)]
        assert r.backups == [r.KEY] * 3

    def test_a_retryable_failure_burst_is_forced_too(self, due):
        """The 60 s retry of a busy server follows an attempt that may have moved the anchor: forced as well."""
        r = due(answers=[(False, True), (True, False)])
        r.h.start()
        r.settle(1)
        r.anchor_age = self.YOUNG
        r.h.clock.advance(61)
        r.settle(2)
        r.h.finish()
        assert r.sends == [(True, PAIR_FAILED), (True, PAIR_DONE)]
        assert r.finished() == [('failed', True), ('ok', False)]

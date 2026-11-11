"""The remote worker's scheduler loop and its short-lived sender threads (plan
"Track B", item B3 of ``docs/sender_rework_plan.md``).

`SendLoop` is what the remote worker's main thread runs. It is the SCHEDULER:
it makes no ``zfs`` call and no network call, and it is the only owner of the
`sendschedule.SendSchedule`, the `health.StatusWriter` (through the
``record``/``flush`` callbacks), the outcome queue and the table of threads.

Threads and their lifetimes
---------------------------
**scheduler** (the caller's thread, the worker's main thread)
    Lives for `SendLoop.run`. Every ``STOP_POLL`` slice it asks ``exit_reason``
    ONCE; on any reason it sets ``leave`` and never dispatches again.

**sender** (one per due pair, `threading.Thread(name="send:<ds>-><dest>",
daemon=True)`)
    Lives for one `sendpair.send_pair` call: builds FRESH managers
    (``new_kit``), runs the pair, closes them, and puts exactly one
    `PairOutcome` on the queue as its last act. It polls ``should_stop`` (the
    stop flag OR ``leave``) between chain steps, never mid-stream, and never
    calls ``exit_reason``. An exception of any kind becomes a FAILED outcome
    (STOPPED while an exit is pending). At most ``MAX_SENDERS`` run at once and
    never two for one pair (the schedule guarantees it, `started()` refuses it).

**discovery** (one per recursive parent at a time, at most ``MAX_DISCOVERIES``,
daemon threads)
    Lives for one ``expand(parent, should_stop)`` call (a ``zfs list -r`` and
    the copy-root property read; untimed, like every source-side call; it only
    reads). Its result comes back on the
    queue; the scheduler keeps using the parent's last known children meanwhile
    and replaces them only on success. On exit a discovery thread is
    ABANDONED after a bounded join (``DISCOVERY_JOIN``): it may be inside a
    ``zfs`` call that is not coming back, and the process is about to exit.

The queue (``queue.SimpleQueue``) carries `_SenderDone` and `_DiscoveryDone`
items; the scheduler blocks on it for at most the time to the next wake-up, so
an outcome is handled at once.

Exit paths
----------
``run()`` NEVER returns, or raises, with a live sender thread:

* an exit reason (STOP, SUPERVISOR_GONE, RELOAD, UPGRADE): ``leave`` is set, no
  new sender starts, and the loop joins every sender, in slices, recording each
  outcome as it arrives (a send that finished its chain is recorded; one that
  was cut between steps is not a failure); then the discovery threads are
  abandoned; the first reason seen is returned. What follows (the wait for
  stuck children, the exit status) is the worker's;
* an exception in the scheduler: it is logged at ERROR with its traceback at
  once (naming how many senders are being drained), then the same ``finally``
  sets ``leave``, joins the senders and abandons the discoveries, and the
  exception propagates (the worker crashes, exit 1, and the supervisor
  restarts it). The join can be long, a sender stops only at its next chain
  step, so every drain writes the status heartbeat on the normal cadence.

There is no cap on a reload or upgrade drain: a source pool that hangs
mid-send holds its sender slot, and therefore the drain, until STOP (the
supervisor's escalation); one hung pair also keeps one of the 4 slots (2 per
destination) from every other pair until then.

Behaviours worth knowing
------------------------
* **R-2 for every child.** The schedule holds every child pair, configured or
  expanded, behind its nearest ancestor pair until that pair is confirmed in
  this incarnation (`SendSchedule.held_by`); the mirror of the senders' truth
  is the `PairOutcome.confirmed` of each outcome (`confirmed`/`unconfirmed`,
  called after `finished`). `sendpair.AncestorRule` stays in each sender as a
  backstop for EXPANDED children only (it reads the memory itself); a
  configured child relies on the schedule alone.
* **After a reload every unconfirmed parent with children negotiates at once**
  (it is due immediately, `SendSchedule` "once per incarnation"): the caps (4
  in all, 2 per destination) bound the burst, but the per-pair stagger does not
  apply to it.
* **A sender sees a RELOAD or a UPGRADE drain up to one ``STOP_POLL`` slice
  late, plus the tick's own work** (the joins of up to 1 s, a forced status
  flush, the reminder): it polls the stop flag and ``leave``, and the scheduler
  sets ``leave`` when its own once-per-slice ``exit_reason()`` call says so (a
  STOP is seen at once, through the flag). Leaving also joins the discovery
  threads for up to ``DISCOVERY_JOIN`` in all, and each outcome it handles
  joins its thread for up to 1 s (normally at once).
* The first WARNING of an R-2 episode waits ``WAITING_NOTICE`` seconds: a child
  held behind an ancestor that negotiates first is normal for a moment at every
  start. The clock restarts while the ancestor is running or due (queued for a
  slot), and no WARNING is logged for an ancestor the memory already knows to be
  confirmed (its outcome has not been handled yet).
* **A DONE pair is always recorded as a success at once**, even when it is
  retryable (a chain cut between steps, or by a drain): the steps that were
  acknowledged are real progress. Only the retry TIMING is `RETRYABLE`; the final
  outcome of the retries is recorded as usual.

This module imports neither ``zfsbackup.remote`` nor ``zfsbackup.local``
(the worker imports those once, after the start-up version check) and builds
no manager itself: the worker's factory does, through ONE place that passes
the shared `SendMemory` and checks that the result shares it.
"""

import logging
import queue
import threading
import time
from typing import (
    Any, Callable, Dict, Hashable, List, NamedTuple, Optional, Sequence, Set, Tuple,
)

from zfsbackup import health
from zfsbackup.sendmemory import COPY_CONFIRMED, ORPHAN_HOLD, WAITING, SendMemory
from zfsbackup.sendpair import (
    PAIR_DONE, PAIR_FAILED, PAIR_NOT_DUE, PAIR_STOPPED, PAIR_WAITING, AncestorRule, LazySender,
    PairOutcome, released_message, send_pair, waiting_message,
)
from zfsbackup.sendschedule import Outcome, PairKey, PairSpec, SendSchedule
from zfsbackup.stopflag import STOP_POLL

__all__ = ['DISCOVERY_JOIN', 'WAITING_NOTICE', 'SenderKit', 'SendLoop', 'schedule_outcome']

#: Longest the exit waits, in all, for discovery threads before abandoning them.
DISCOVERY_JOIN = 5.0
#: R-2: a child held behind an unconfirmed ancestor is normal for a moment at
#: every start (the ancestor negotiates first), so the one WARNING per
#: (ancestor, destination) episode is logged only once a hold has lasted this
#: long (s). The INFO that ends an episode is logged only if the WARNING was.
WAITING_NOTICE = 60.0
#: Smallest sleep between ticks (s): a due time a hair away must not spin.
MIN_SLICE = 0.01


class SenderKit:
    """What one sender thread works with, built fresh for the send (the
    worker's factory): the `DatasetManager` and `DatasetInfo` of this send only
    (``DatasetInfo.snapshots`` is written by every listing), and ``resolve``,
    which builds the transport manager (`RemoteBackupManager` /
    `LocalBackupManager`) when `send_pair` first needs it (after R-2, so a held
    pair never builds one). `close()` closes the transport manager if one was
    built; the kit is used by one thread only."""

    def __init__(self, manager, dsi, resolve: Callable[[], Any]):
        self.manager = manager
        self.dsi = dsi
        self._resolve = resolve
        self.sender = None

    def resolve(self):
        self.sender = self._resolve()
        return self.sender

    def close(self) -> None:
        sender, self.sender = self.sender, None
        close = getattr(sender, 'close', None)
        if close is not None:
            close()


class _SenderDone(NamedTuple):
    key: PairKey
    outcome: PairOutcome


class _DiscoveryDone(NamedTuple):
    parent: str
    ok: bool
    children: list
    error: Optional[str]


def schedule_outcome(outcome: PairOutcome) -> Tuple[Outcome, bool]:
    """`(sendschedule.Outcome, retryable)` for a `PairOutcome` (the mapping
    the schedule's docstring leaves to the loop). A DONE pair that
    `ChainSender.last_retryable` calls transient (a chain cut between steps
    with work left) is RETRYABLE: retried after 60, 120, 300 s, and recorded
    (as a success) only once those are used up."""
    kind = outcome.kind
    if kind == PAIR_DONE:
        return (Outcome.RETRYABLE if outcome.retryable else Outcome.OK), outcome.retryable
    if kind == PAIR_NOT_DUE:
        return Outcome.NOT_DUE, False
    if kind == PAIR_WAITING:
        return Outcome.WAITING, False
    if kind == PAIR_STOPPED:
        return Outcome.STOPPED, False
    if kind == PAIR_FAILED:
        return Outcome.FAILED, outcome.retryable
    return Outcome.FAILED, False


class SendLoop:
    """See the module docstring.

    * ``config``: the worker's `BackupConfig` (``check_interval``,
      ``enabled_datasets``); ``schedule``: the `SendSchedule` this loop owns
      (injected, so a test gives it a clock); ``memory``: the worker's
      `SendMemory`, the one every manager shares.
    * ``configured``: the enabled datasets with ``remote:`` entries as
      scheduler-side `DatasetInfo` records (never handed to a sender: a sender
      builds its own, ``new_kit``).
    * ``new_kit(template, remote_cfg) -> SenderKit``: the ONE factory of the
      per-send objects. ``expand(parent, should_stop) -> Expansion``: one
      parent's discovery (`DatasetManager.expand_recursive(parent=...)` on a
      fresh manager); runs on a discovery thread.
    * ``exit_reason() -> reason or None``: called only by the scheduler thread,
      once per tick; any truthy value is a reason. ``stop_event``: the stop
      flag a sender also polls.
    * ``record(outcome)``: records a `PairOutcome` the schedule says to record
      (the worker's `_record_outcome`); ``flush(force, heartbeat)``; ``status``:
      the `StatusWriter` or `None` (read for ``last_attempt`` and pruned with
      `retain`). All three are used from the scheduler thread only.
    * ``remind()``: called every tick (the daily certificate reminder claims
      its own once-a-day slot in the memory).
    """

    def __init__(
        self, *, name: str, config, schedule: SendSchedule, memory: SendMemory,
        exit_reason: Callable[[], Optional[Hashable]], stop_event,
        configured: Sequence[Any],
        new_kit: Callable[[Any, Any], SenderKit],
        expand: Callable[[str, Callable[[], bool]], Any],
        record: Optional[Callable[[PairOutcome], None]] = None,
        flush: Optional[Callable[..., None]] = None,
        status: Optional[health.StatusWriter] = None,
        remind: Optional[Callable[[], None]] = None,
        logger: Optional[logging.Logger] = None,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        heartbeat_every: Optional[float] = None,
        discovery_join: float = DISCOVERY_JOIN,
    ):
        self._name = name
        self._config = config
        self._sched = schedule
        self._memory = memory
        self._exit_reason = exit_reason
        self._stop_event = stop_event
        self._configured = list(configured)
        self._new_kit = new_kit
        self._expand = expand
        self._record = record
        self._flush = flush
        self._status = status
        self._remind = remind
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._clock = clock
        self._monotonic = monotonic
        self._discovery_join = discovery_join
        check = config.check_interval.total_seconds()
        #: At least this often the status file is rewritten, so that `check` can
        #: tell a quiet worker from a dead one.
        self._heartbeat_every = (
            heartbeat_every if heartbeat_every is not None else max(check, 1.0)
        )
        #: Set by the scheduler on any exit reason; read by every thread.
        self.leave = threading.Event()
        self._outbox: 'queue.SimpleQueue' = queue.SimpleQueue()
        self._senders: Dict[PairKey, threading.Thread] = {}
        #: Sender threads whose outcome was handled but which were still alive
        #: after a short join (joined at the end).
        self._lingering: List[threading.Thread] = []
        self._discoveries: Dict[str, threading.Thread] = {}
        # Pairs: key -> (scheduler-side DatasetInfo, RemoteDatasetConfig).
        self._pairs: Dict[PairKey, Tuple[Any, Any]] = {}
        self._rule: Optional[AncestorRule] = None
        # Recursive parents (enabled, with `remote:`), config order.
        self._parents: List[str] = [
            c.name for c in config.enabled_datasets if c.recursive and c.remote
        ]
        # Parent -> its last known children (absent: never discovered), and the
        # parents whose LAST discovery failed (-> why).
        self._children: Dict[str, List[Any]] = {}
        self._failed: Dict[str, str] = {}
        # Pairs whose spec could not be built (a hand-edited zero frequency):
        # one ERROR each.
        self._bad_noted: Set[PairKey] = set()
        # R-2 announcements: the ancestors waiting children are held behind
        # (recomputed after every event that can change it), when each was
        # first seen (monotonic), and those announced by the scheduler.
        self._held_dirty = True
        self._held: Dict[PairKey, List[PairKey]] = {}
        self._held_since: Dict[PairKey, float] = {}
        self._announced: Set[PairKey] = set()
        self._pairs_over_cap_noted = False
        self._next_heartbeat = 0.0
        #: The reason that ended the loop (None until it ends).
        self.reason: Optional[Hashable] = None

    # ------------------------------------------------------------------ threads

    def _should_stop(self) -> bool:
        """What a sender and a discovery thread poll (a safe point): the stop
        flag or the scheduler's ``leave``. Never `exit_reason()`."""
        return self._stop_event.is_set() or self.leave.is_set()

    @property
    def sender_threads(self) -> Dict[PairKey, threading.Thread]:
        """The live sender threads by pair (a copy; for tests and diagnostics)."""
        return dict(self._senders)

    @property
    def schedule(self) -> SendSchedule:
        return self._sched

    # --------------------------------------------------------------------- run

    def run(self) -> Optional[Hashable]:
        """The scheduler loop; -> the first exit reason seen. Returns only
        after every sender has ended and the discovery threads were joined
        (bounded) or abandoned; an exception of the scheduler itself leaves
        the same way and then propagates."""
        try:
            self._start()
            while True:
                self.reason = self._exit_reason()
                if self.reason is not None:
                    break
                self._tick()
        except BaseException as e:  # noqa: BLE001 -- say so NOW; the drain below may take long
            live = len(self._senders) + len(self._lingering)
            self._log.error(
                f"{self._name}: the scheduler failed: {e!r}; "
                + (
                    f"draining {live} running sender{'' if live == 1 else 's'} before the "
                    "worker exits (a stream is never cut mid-way; nothing new is started)"
                    if live else "no sender is running; the worker exits"
                ),
                exc_info=True,
            )
            raise
        finally:
            self.leave.set()
            try:
                self._drain()
            finally:
                self._abandon_discoveries()
        return self.reason

    def _start(self) -> None:
        now = self._clock()
        self._rebuild(now)
        self._sched.set_discovery_parents(self._parents, now)
        self._retain_status()
        # This incarnation's `started_at`/`updated_at` are on disk from the
        # first moment, not only after the first outcome or a heartbeat.
        self._call_flush(True, True)
        self._next_heartbeat = self._monotonic() + self._heartbeat_every

    def _tick(self) -> None:
        self._handle_ready()
        self._reap_dead_senders()
        if self._remind is not None:
            try:
                self._remind()
            except Exception as e:  # advisory only
                self._log.debug(f"{self._name}: the reminder failed: {e!r}", exc_info=True)
        # Read AFTER the work above (joins of up to 1 s, forced status flushes,
        # the reminder): `started` must be given the slot the sender will serve.
        now = self._clock()
        for parent in self._sched.discovery_due(now):
            self._start_discovery(parent, now)
        for key in self._sched.dispatch(now):
            self._start_sender(key, now)
        mono = self._monotonic()
        self._announce_waiting(now, mono)
        self._heartbeat(mono)
        self._sleep(now, mono)

    def _heartbeat(self, mono: float) -> None:
        """Rewrite the status file at least every ``heartbeat_every`` (every
        path that can wait for long calls this: the tick and `_drain`)."""
        if mono >= self._next_heartbeat:
            self._next_heartbeat = mono + self._heartbeat_every
            self._call_flush(True, True)

    def _sleep(self, now: float, mono: float) -> None:
        """Block on the queue until an item arrives or the next wake-up:
        the next due time of the schedule, the next heartbeat, at most one
        ``STOP_POLL`` slice (the exit reason is polled once per slice)."""
        timeout = STOP_POLL
        wake = self._sched.next_wakeup(now)
        if wake is not None:
            timeout = min(timeout, wake - now)
        timeout = min(timeout, max(0.0, self._next_heartbeat - mono))
        try:
            item = self._outbox.get(timeout=max(timeout, MIN_SLICE))
        except queue.Empty:
            return
        self._handle(item)

    # --------------------------------------------------------------- the queue

    def _handle_ready(self) -> None:
        while True:
            try:
                item = self._outbox.get_nowait()
            except queue.Empty:
                return
            self._handle(item)

    def _handle(self, item) -> None:
        try:
            if isinstance(item, _SenderDone):
                self._on_sender_done(item.key, item.outcome)
            elif isinstance(item, _DiscoveryDone):
                self._on_discovery_done(item)
        except Exception as e:
            self._log.error(f"{self._name}: cannot process {item!r}: {e!r}", exc_info=True)

    def _on_sender_done(self, key: PairKey, outcome: PairOutcome) -> None:
        thread = self._senders.pop(key, None)
        if thread is not None:
            # The outcome is the thread's last act: this join is immediate.
            thread.join(timeout=1.0)
            if thread.is_alive():
                self._lingering.append(thread)
        kind, retryable = schedule_outcome(outcome)
        # `finished` BEFORE `confirmed`/`unconfirmed` (`SendSchedule` docstring).
        record_now = self._sched.finished(key, kind, retryable, self._clock())
        # A chain that completed steps (DONE, even with `retryable`: cut between
        # steps, a reload's drain, a transient stop) made real progress: record
        # it as a success NOW. Only the retry TIMING is the schedule's
        # (`RETRYABLE`); a later retry that finds nothing due (NOT_DUE) must not
        # lose this success, and the burst's final outcome is recorded as usual.
        if outcome.kind == PAIR_DONE:
            record_now = True
        if record_now and self._record is not None:
            try:
                self._record(outcome)
            except Exception as e:
                self._log.error(
                    f"{self._name} cannot record {key.dataset} -> {key.destination}: {e!r}",
                    exc_info=True,
                )
        if outcome.confirmed is True:
            self._sched.confirmed(key)
        elif outcome.confirmed is False:
            self._sched.unconfirmed(key, self._clock())
        self._held_dirty = True

    def _on_discovery_done(self, done: _DiscoveryDone) -> None:
        parent = done.parent
        thread = self._discoveries.pop(parent, None)
        if thread is not None:
            thread.join(timeout=1.0)
        ok = done.ok and not self.leave.is_set()
        self._sched.discovery_finished(parent, ok, self._clock())
        if not ok:
            if not self.leave.is_set():
                self._failed[parent] = done.error or 'discovery failed'
            return
        self._failed.pop(parent, None)
        old = self._children.get(parent)
        self._children[parent] = list(done.children)
        if old is None or [c.name for c in old] != [c.name for c in done.children]:
            self._rebuild(self._clock())
        # Known now: the parent's vanished children's records may go.
        self._retain_status()

    def _reap_dead_senders(self) -> None:
        """A sender thread that ended without an outcome (it cannot, short of
        the interpreter killing it) must not hold its pair for ever."""
        dead = [k for k, t in self._senders.items() if not t.is_alive()]
        if not dead:
            return
        # The outcome, if any, was put before the thread ended: look again.
        self._handle_ready()
        for key in dead:
            if key in self._senders:
                self._log.error(
                    f"{self._name}: the sender of {key.dataset} -> {key.destination} ended "
                    "without an outcome"
                )
                self._on_sender_done(key, PairOutcome(
                    key.dataset, key.destination, PAIR_FAILED,
                    error='the sender ended without an outcome',
                ))

    # ------------------------------------------------------------------- pairs

    def _last_attempts(self) -> Dict[Tuple[str, Optional[str]], float]:
        if self._status is None:
            return {}
        return {
            (r.dataset, r.destination): r.last_attempt for r in self._status.records
            if r.kind == health.KIND_SEND and r.last_attempt is not None
        }

    def _rebuild(self, now: float) -> None:
        """The pair set: the configured pairs plus each parent's last known
        children (config order, children after the configured ones), handed to
        the schedule. A new pair's catch-up reads its status record's
        ``last_attempt``."""
        entries: List[Tuple[Any, Any]] = []
        for dsi in self._configured:
            entries.extend((dsi, rc) for rc in dsi.config.remote)
        for parent in self._parents:
            for child in self._children.get(parent, ()):
                entries.extend((child, rc) for rc in child.config.remote)
        last = self._last_attempts()
        specs: List[PairSpec] = []
        pairs: Dict[PairKey, Tuple[Any, Any]] = {}
        for order, (dsi, rc) in enumerate(entries):
            key = PairKey(dsi.name, rc.destination)
            if key in pairs:
                continue  # unique by (dataset, destination)
            try:
                spec = PairSpec(
                    dsi.name, rc.destination, frequency=dsi.config.frequency, period=rc.frequency,
                    config_order=order, last_attempt=last.get(key),
                )
            except ValueError as e:
                if key not in self._bad_noted:
                    self._bad_noted.add(key)
                    self._log.error(
                        f"{self._name}: {dsi.name} -> {rc.destination} is not scheduled: {e}"
                    )
                continue
            specs.append(spec)
            pairs[key] = (dsi, rc)
        self._pairs = pairs
        self._rule = self._make_rule(pairs)
        self._sched.update_pairs(specs, now)
        # Per-child latch housekeeping: a pair that left no longer owns one.
        live = set(pairs)
        self._memory.retain(ORPHAN_HOLD, lambda k: PairKey(*k) in live)
        # The R-2 "waiting" latch outlives an episode when its children vanish
        # (a sender may have noted it, `AncestorRule.hold`, with no scheduler
        # announcement to clear it): keep only the ancestors that still have
        # dependents, in the schedule or in the sender-side rule.
        have = {tuple(a) for a in self._sched.waiting_on()}
        have.update(self._rule.dependents)
        # `_announce_waiting` owns clearing the ancestors it announced.
        have.update(tuple(a) for a in self._announced)
        self._memory.retain(WAITING, lambda k: tuple(k) in have)
        self._held_dirty = True

    def _make_rule(self, pairs: Dict[PairKey, Tuple[Any, Any]]) -> AncestorRule:
        """The sender-side R-2 backstop (`AncestorRule`) over this pair set:
        read-only records, replaced, never changed in place, so one rule serves
        every sender thread started under it. The schedule already holds every
        child (configured or expanded) behind an unconfirmed ancestor; the rule
        covers the expanded ones if the memory and the schedule ever disagree."""
        by_name: Dict[str, Any] = {}
        for (dsi, _rc) in pairs.values():
            by_name.setdefault(dsi.name, dsi)
        rule = AncestorRule(by_name, {}, self._memory, lambda manager, destination: None)
        dependents: Dict[Tuple[str, str], int] = {}
        for key, (dsi, _rc) in pairs.items():
            if dsi.expanded_from is None:
                continue
            ancestor = rule.nearest_pair(dsi.name, key.destination)
            if ancestor is not None:
                k = (ancestor.name, key.destination)
                dependents[k] = dependents.get(k, 0) + 1
        rule.dependents = dependents
        return rule

    def _retain_status(self) -> None:
        """Drop the status records of pairs that are gone (a dataset or a child
        removed must not keep a failure on the books), except the records under
        a parent whose children are UNKNOWN (never discovered in this
        incarnation, or its last discovery failed): they are not gone. Not
        while an exit is pending (a list cut short is not the truth)."""
        if self._status is None or self.leave.is_set():
            return
        keep: Set[Tuple[str, Optional[str]]] = {(k.dataset, k.destination) for k in self._pairs}
        unknown = [p for p in self._parents if p not in self._children or p in self._failed]
        if unknown:
            prefixes = tuple(p + '/' for p in unknown)
            for rec in self._status.records:
                if rec.kind == health.KIND_SEND and rec.dataset.startswith(prefixes):
                    keep.add((rec.dataset, rec.destination))
        over = len(keep) > health.STATUS_MAX_RECORDS
        if over and not self._pairs_over_cap_noted:
            self._log.warning(
                f"{self._name} has {len(keep)} dataset x destination pairs (recursive datasets "
                f"included), more than the {health.STATUS_MAX_RECORDS} outcomes a status file "
                "holds: the least recently attempted are dropped, so `zfsbackup-config check` "
                "cannot report them"
            )
        self._pairs_over_cap_noted = over
        self._status.retain(keep)

    # ------------------------------------------------------------------ senders

    def _start_sender(self, key: PairKey, now: float) -> None:
        dsi, rc = self._pairs[key]
        # Read once, here: the sender skips its anchor-age check while children
        # wait for this pair (`SendSchedule.wants_confirmation`).
        force = self._sched.wants_confirmation(key)
        self._sched.started(key, now)
        thread = threading.Thread(
            target=self._sender_main, args=(key, dsi, rc, force, self._rule),
            name=f"send:{key.dataset}->{key.destination}", daemon=True,
        )
        self._senders[key] = thread
        try:
            thread.start()
        except Exception as e:
            # E.g. "can't start new thread": a failure of this pair, not of the loop.
            del self._senders[key]
            self._log.error(f"{self._name} cannot start a sender for {key.dataset} -> "
                            f"{key.destination}: {e!r}")
            # Resource exhaustion is transient: retried soon, not after an hour.
            self._on_sender_done(key, PairOutcome(
                key.dataset, key.destination, PAIR_FAILED, error=f"{type(e).__name__}: {e}",
                retryable=True,
            ))

    def _sender_main(self, key: PairKey, template, remote_cfg, force: bool, rule) -> None:
        """One sender thread. Always puts exactly one outcome, last."""
        kit: Optional[SenderKit] = None
        outcome: Optional[PairOutcome] = None
        try:
            kit = self._new_kit(template, remote_cfg)
            outcome = send_pair(
                kit.manager, kit.dsi, remote_cfg, LazySender(kit.resolve),
                self._should_stop, self._should_stop,
                rule=rule, logger=self._log, label=self._name, force_confirm=force,
            )
        except BaseException as e:  # noqa: BLE001 -- the outcome must reach the scheduler
            self._log.error(
                f"{self._name} error on {key.dataset} -> {key.destination}: {e!r}", exc_info=True,
            )
            outcome = PairOutcome(
                key.dataset, key.destination,
                PAIR_STOPPED if self._should_stop() else PAIR_FAILED,
                error=f"{type(e).__name__}: {e}",
            )
        finally:
            if kit is not None:
                try:
                    kit.close()
                except Exception as e:
                    self._log.debug(f"{self._name}: closing the managers of {key.dataset}: {e!r}")
            if outcome is None:  # not reachable; belt and braces
                outcome = PairOutcome(key.dataset, key.destination, PAIR_FAILED,
                                      error='the sender ended without an outcome')
            self._outbox.put(_SenderDone(key, outcome))

    # --------------------------------------------------------------- discovery

    def _start_discovery(self, parent: str, now: float) -> None:
        self._sched.discovery_started(parent, now)
        thread = threading.Thread(
            target=self._discovery_main, args=(parent,), name=f"discover:{parent}", daemon=True,
        )
        self._discoveries[parent] = thread
        try:
            thread.start()
        except Exception as e:
            del self._discoveries[parent]
            self._log.error(f"{self._name} cannot start the discovery of {parent}: {e!r}")
            self._on_discovery_done(_DiscoveryDone(parent, False, [], f"{type(e).__name__}: {e}"))

    def _discovery_main(self, parent: str) -> None:
        done: Optional[_DiscoveryDone] = None
        try:
            expansion = self._expand(parent, self._should_stop)
            error = expansion.failed.get(parent)
            done = _DiscoveryDone(
                parent, error is None and not self._should_stop(), list(expansion.children), error,
            )
        except BaseException as e:  # noqa: BLE001
            self._log.error(
                f"{self._name} could not expand {parent}: {e!r}", exc_info=True
            )
            done = _DiscoveryDone(parent, False, [], f"{type(e).__name__}: {e}")
        finally:
            if done is None:
                done = _DiscoveryDone(parent, False, [], 'discovery ended without a result')
            self._outbox.put(done)

    # ------------------------------------------------------------- R-2 warnings

    def _ancestor_progressing(self, anc: PairKey, now: float) -> bool:
        """Whether the ancestor pair is doing what a held child waits for: its
        sender is running, or it is itself due and only queued behind a free
        slot (or a cap). The schedule learns `confirmed` only from the sender's
        outcome, after the whole chain, so such a time is not the children's
        wait being stuck."""
        if anc in self._sched.running:
            return True
        # A held ancestor waits for ITS ancestor: look at the topmost holder,
        # the pair that is actually (or not) doing the work.
        while (up := self._sched.held_by(anc)) is not None:
            anc = up
        if anc in self._sched.running:
            return True
        due = self._sched.due_time(anc, now)
        return due is not None and due <= now

    def _announce_waiting(self, now: float, mono: float) -> None:
        """One WARNING per (ancestor, destination) episode naming how many
        children are held behind it (``SendSchedule.waiting_on``), once the hold
        has lasted ``WAITING_NOTICE`` while the ancestor was NOT sending or
        queued (a running or queued ancestor restarts the clock); one INFO when
        the episode ends because the ancestor was confirmed (nothing is logged
        when it merely left the pair set or lost its children). Nothing is
        announced for an ancestor whose copy the memory already knows to be
        confirmed (the outcome that tells the schedule is still to come). The
        latch is the memory's ``WAITING`` table, the sender's
        `AncestorRule.hold` shares it, so an episode is announced once by
        whoever notices first."""
        if self._held_dirty:
            self._held_dirty = False
            self._held = self._sched.waiting_on()
            for anc in self._held:
                self._held_since.setdefault(anc, mono)
            for anc in [a for a in self._held_since if a not in self._held]:
                del self._held_since[anc]
            for anc in [a for a in self._announced if a not in self._held]:
                self._announced.discard(anc)
                if not self._memory.clear(WAITING, tuple(anc)):
                    continue
                if self._sched.is_confirmed(anc):
                    self._log.info(released_message(anc.dataset, anc.destination))
                else:
                    self._log.debug(
                        f"{anc.dataset} -> {anc.destination}: no descendant waits for it any more"
                    )
        for anc, kids in self._held.items():
            if self._ancestor_progressing(anc, now):
                self._held_since[anc] = mono
                continue
            if anc in self._announced or mono - self._held_since[anc] < WAITING_NOTICE:
                continue
            if self._sched.held_by(anc) is not None:
                # Itself held behind its own ancestor: only the topmost holder's
                # episode is announced (neither a WARNING nor the WAITING latch
                # here). An episode announced before it became held ends below
                # through the usual INFO/DEBUG.
                continue
            if self._memory.noted(COPY_CONFIRMED, tuple(anc)):
                continue
            self._announced.add(anc)
            if self._memory.note(WAITING, tuple(anc)):
                self._log.warning(waiting_message(anc.dataset, anc.destination, len(kids)))

    # ------------------------------------------------------------------- flush

    def _call_flush(self, force: bool, heartbeat: bool) -> None:
        if self._flush is None:
            return
        try:
            self._flush(force=force, heartbeat=heartbeat)
        except Exception as e:
            self._log.error(f"{self._name} cannot flush its status: {e!r}", exc_info=True)

    # -------------------------------------------------------------------- exit

    def _drain(self) -> None:
        """Join every sender, in ``STOP_POLL`` slices, handling each outcome as
        it arrives. Unbounded by design: a sender stops at its next chain step
        (``leave`` is set), and a stream that cannot be cut is the supervisor's
        escalation to deal with (its stop budget, then SIGTERM). A heartbeat
        is written on the normal cadence meanwhile."""
        while self._senders or self._lingering:
            # A long drain must not let the status file go stale (`check` would
            # call the worker dead): the same cadence as in the loop, on every
            # exit path.
            self._heartbeat(self._monotonic())
            try:
                item = self._outbox.get(timeout=STOP_POLL)
            except queue.Empty:
                item = None
            if item is not None:
                self._handle(item)
                continue
            try:
                self._reap_dead_senders()
            except Exception as e:
                self._log.error(f"{self._name}: {e!r}", exc_info=True)
            self._lingering = [t for t in self._lingering if t.is_alive()]
        # Whatever else is on the queue (discovery results: ignored).
        self._handle_ready()

    def _abandon_discoveries(self) -> None:
        """Join the discovery threads within `discovery_join` in all; one still
        running is abandoned (a daemon thread, perhaps inside a `zfs` call that
        is not returning; the process is exiting)."""
        deadline = self._monotonic() + self._discovery_join
        for parent, thread in list(self._discoveries.items()):
            thread.join(timeout=max(0.0, deadline - self._monotonic()))
            if thread.is_alive():
                self._log.warning(
                    f"{self._name} is abandoning the discovery of {parent}: its zfs call has "
                    "not returned"
                )
        self._discoveries.clear()

"""`SendSchedule`: the pure scheduling core of the parallel remote worker.

Track B, item B2 of ``docs/sender_rework_plan.md`` ("Decisions" and "Track B ->
Scheduling").  This module decides **when** a (dataset, destination) pair is
sent and **which** due pairs may start now.  It does nothing else: no ``zfs``,
no thread, no logging, no I/O.  Every method takes the wall-clock time as an
argument (``now``, seconds since the epoch, UTC; ``None`` reads the injected
``clock``), so a test drives it with a fake clock.  The B3 scheduler thread owns
one instance, calls it from that one thread only (it is **not** thread-safe),
starts the senders it names, and feeds the outcomes back.

Plan notation
-------------
``F``
    the dataset's snapshot frequency, in seconds;
``P``
    the pair's send period, ``remote.frequency or F``;
``W = min(P, F)``
    the width the pair's start offset is spread over;
``lo = min(check_interval + 60 s, W / 2)``
    the earliest offset (the snapshot worker needs a ``check_interval`` to
    notice a new period boundary, plus a minute of margin; never more than
    half of ``W``);
``frac = (int(sha256(client_id ‖ dataset ‖ destination)[:8]) >> 11) / 2**53``
    the pair's stable fraction in ``[0, 1)`` (`pair_fraction`; the top 53 bits,
    so the division is exact and the result can never round up to 1.0);
``offset = lo + frac * (W - lo)`` (`slot_offset`);
slots ``s_k = k * P + offset``
    for integer ``k``, counted from the epoch, so they are aligned to UTC
    exactly as the snapshot boundaries are (``DatasetInfo.get_reference_time``
    floors to a multiple of the frequency since the epoch).  ``slot_index(t)``
    is the ``k`` of the latest slot at or before ``t``.

The assumed ``check_interval`` of the plan's worked table (A.1 below) is the
configured default, 5 minutes.

Rules, and where they live
--------------------------
Each is cited in the docstring of the method that implements it.

* **Regular slots.**  A pair is due at ``s_k`` unless slot ``k`` was already
  attempted (``_PairState.attempted``).  Several slots missed in a row (a long
  overrun, a suspended process, a clock step forward) are coalesced into one
  attempt (``SendSchedule._regular_due``).
* **Catch-up** (plan "Scheduling"; decision "An outage gets one catch-up within
  5 min").  A pair that enters the schedule with ``last_attempt < s_prev``
  (the status record's last attempt is older than the latest slot, or there is
  none) is due ``frac * min(W, 300 s)`` from now (`SendSchedule._init_state`).
  The same delay applies to a live pair that finds itself more than
  ``LATE_SLACK`` behind the slot it has just reached (`SendSchedule._arm`): that
  is an outage of this scheduler (stopped process, suspended host, forward clock
  step), and it gets exactly one catch-up.
* **Overrun** (plan "Overrun: one coalesced follow-up").  A send that is still
  running when later slots pass owes ONE follow-up, due as soon as it finishes
  (`SendSchedule.finished`, OK and NOT_DUE).  A failed attempt covers the slots
  that passed during it; its own retry rule decides what follows.
* **Clock jumps** (plan "recompute from wall-clock time and never re-run an
  attempted slot ``k``").  Nothing is accumulated: every decision is recomputed
  from the ``now`` it is given.  The attempted slots are one integer per pair,
  raised by an attempt.  **This deliberately deviates from the plan's "never
  re-run an attempted slot"**: a pair stalled for the whole size of a backward
  step (a host booted days behind before NTP syncs) is worse than an extra run,
  so whenever the attempted index is beyond ``slot_index(now)`` (which only a
  clock step back can cause) it is clamped to ``slot_index(now)``
  (`SendSchedule._unstall`).  The current slot then counts as attempted and the
  pair runs at the next slot, so "next regular due <= now + P" holds
  unconditionally.  A small step back across a slot boundary therefore re-runs
  the attempted slot once, when the clock reaches it again: at most one extra
  run per back-step, and the sender's own anchor-age check makes it a cheap
  ``NOT_DUE``.  A pending catch-up, retry, confirmation or discovery time lying
  further ahead than the longest delay it is ever given is pulled back the same
  way; those caps can only fire after a step back, because the delays are never
  longer than their caps.
* **Retries** (decision "Retries").  A *retryable* outcome (``RETRYABLE``, or
  ``FAILED`` with ``retryable=True``) is retried after 60, 120 and 300 s
  (``RETRY_DELAYS``) and is **recorded only once those retries are used up**:
  `SendSchedule.finished` returns ``record_now=False`` until then.  Any other
  failure is recorded at once and retried within ``min(next slot, 1 h)``
  (``FAILURE_RETRY_CAP``): the retry is due at ``now + 1 h`` and the next
  regular slot, being a due time of its own, preempts it when earlier.  Retries
  that are used up become that same "other failure" (recorded, retried within
  ``min(next slot, 1 h)``).
* **Exclusion and order** (plan "Caps and order").  See `SendSchedule.due` and
  `SendSchedule.dispatch`.
* **R-2 wait** (plan "R-2 Parent first", "applied to EVERY child pair").  See
  `SendSchedule.held_by`.
* **Discovery timing.**  See `SendSchedule.discovery_due`.

Worked table (``check_interval`` = 300 s, as above)
---------------------------------------------------
============================  ======  =====  ======  ==============================
case                          F       P      lo      offset
============================  ======  =====  ======  ==============================
F = P = 1 h, frac = 0.5       3600    3600   360     360 + 0.5 * 3240 = 1980 = :33
F = 1 h, P = 1 d, frac = 0.5  3600    86400  360     1980: slot 00:33 every day
F = 5 min, frac = 0.5         300     300    150     150 + 0.5 * 150  = 225 = 3m45
F = 1 d, P = 1 h, frac = 0.5  86400   3600   360     1980: :33 every hour (hourly)
frac = 0 (any case)           W       P      lo      lo
============================  ======  =====  ======  ==============================

The ``:33`` figures hold only for ``check_interval`` = 300 s: ``lo = ci + 60``
must be 360 s.  A different ``check_interval`` moves them (``ci`` = 60 s gives
``:31``).
"""

from __future__ import annotations

import enum
import hashlib
import math
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Callable, Dict, Iterable, Iterator, List, NamedTuple, Optional, Set, Tuple, Union,
)

__all__ = [
    'MAX_SENDERS', 'MAX_SENDERS_PER_DESTINATION', 'RETRY_DELAYS', 'FAILURE_RETRY_CAP',
    'CATCH_UP_CAP', 'LATE_SLACK', 'WAIT_RECHECK', 'LO_MARGIN', 'MAX_DISCOVERIES',
    'DISCOVERY_MAX_INTERVAL', 'DISCOVERY_RETRY',
    'Outcome', 'PairKey', 'PairSpec', 'SendSchedule',
    'pair_fraction', 'slot_offset', 'slot_time', 'slot_index',
]

#: Decision "Caps": constants, no configuration.
MAX_SENDERS = 4
MAX_SENDERS_PER_DESTINATION = 2

#: Decision "Retries": a retryable outcome is retried after these delays (s).
RETRY_DELAYS: Tuple[float, ...] = (60.0, 120.0, 300.0)
#: Decision "Retries": any other failure is retried within ``min(next slot, this)``.
FAILURE_RETRY_CAP = 3600.0
#: Plan "Scheduling": the catch-up delay is ``frac * min(W, CATCH_UP_CAP)``.
CATCH_UP_CAP = 300.0
#: ADDITION BEYOND THE PLAN (not in "Decisions" or "Scheduling"): how far behind
#: a slot a live idle pair may notice it before that counts as an outage (and
#: gets the catch-up delay instead of "due now"). The B3 loop ticks every
#: ``STOP_POLL`` (0.5 s), so a minute is a stall, not jitter.
LATE_SLACK = 60.0
#: ``lo = min(check_interval + LO_MARGIN, W / 2)``.
LO_MARGIN = 60.0
#: A sender that answers WAITING is asked again after this long (no budget used).
WAIT_RECHECK = 60.0
#: ADDITION BEYOND THE PLAN: at most this many discoveries in flight at once
#: (the plan says "one per parent", not how many parents).
MAX_DISCOVERIES = 4
#: A parent is re-discovered at least this often (s) ...
DISCOVERY_MAX_INTERVAL = 3600.0
#: ... and a failed discovery is retried after ``min(interval, this)`` (s).
DISCOVERY_RETRY = 300.0

Seconds = Union[float, int, timedelta]


def _secs(value: Seconds) -> float:
    return value.total_seconds() if isinstance(value, timedelta) else float(value)


class Outcome(str, enum.Enum):
    """What a finished send tells the schedule. Independent of B1's
    ``PairOutcome``: B3 maps between them. Plain strings are accepted
    wherever an `Outcome` is (``Outcome('ok')``).

    ``OK``         the pair is settled (sent, or up to date after a negotiate);
                   recorded.
    ``NOT_DUE``    the sender's own anchor-age check said "not due": nothing
                   happened, nothing is recorded; no retry.
    ``FAILED``     a failure; with ``retryable=True`` it is `RETRYABLE`.
    ``RETRYABLE``  a failure that is retried after 60, 120, 300 s and recorded
                   only once those are used up (B1.2: busy, reloading, an
                   unsettled ancestor, a connect failure before any byte).
    ``STOPPED``    cut short by an exit reason; nothing recorded, no retry (the
                   worker is leaving).
    ``WAITING``    the sender found nothing to do yet (for example a held
                   ancestor it knows about); nothing recorded, asked again
                   after ``WAIT_RECHECK`` without using the retry budget.
    """
    OK = 'ok'
    NOT_DUE = 'not_due'
    FAILED = 'failed'
    RETRYABLE = 'retryable'
    STOPPED = 'stopped'
    WAITING = 'waiting'


class PairKey(NamedTuple):
    """A pair: unique by (dataset, destination). A plain 2-tuple works too."""
    dataset: str
    destination: str


@dataclass(frozen=True)
class PairSpec:
    """One (dataset, destination) pair as the scheduler sees it.

    ``frequency`` (F) and ``period`` (P, ``None`` = F) are seconds or
    ``timedelta``s. ``config_order`` breaks ties (config order; children of a
    recursive dataset after their parent). ``depth`` defaults to the number of
    ``/`` in the dataset name. Ancestor links are not passed: they are the
    pairs of the same destination whose dataset name is a ``/``-prefix of this
    one's, and the nearest one is the R-2 parent. ``last_attempt`` (epoch
    seconds, from the status record's ``last_attempt``) is read only when the
    pair is NEW to the schedule.
    """
    dataset: str
    destination: str
    frequency: float
    period: Optional[float] = None
    config_order: int = 0
    depth: Optional[int] = None
    last_attempt: Optional[float] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, 'frequency', _secs(self.frequency))
        object.__setattr__(
            self, 'period', None if self.period is None else _secs(self.period)
        )
        if not self.frequency > 0 or not self.effective_period > 0:
            raise ValueError(
                f"pair {self.dataset!r} -> {self.destination!r}: frequency and period "
                "must be positive"
            )
        if self.depth is None:
            object.__setattr__(self, 'depth', self.dataset.count('/'))

    @property
    def key(self) -> PairKey:
        return PairKey(self.dataset, self.destination)

    @property
    def effective_period(self) -> float:
        """P."""
        return self.frequency if self.period is None else self.period

    @property
    def window(self) -> float:
        """W = min(P, F)."""
        return min(self.effective_period, self.frequency)


# ----------------------------------------------------------------------------
# The slot arithmetic (pure functions)
# ----------------------------------------------------------------------------

def pair_fraction(client_id: str, dataset: str, destination: str) -> float:
    """``frac``: the first 8 bytes of ``sha256(client_id ‖ dataset ‖
    destination)`` as a big-endian integer, its top 53 bits over ``2**53``
    (``(n >> 11) / 2**53``), in ``[0, 1)``. Dividing the full 64-bit integer
    by ``2**64`` in floating point can round a value within 2**-54 of the top
    up to exactly 1.0; 53 bits fit a double's mantissa, so this never does.

    The three parts are joined with NUL bytes, which no client id, dataset or
    destination name can contain, so ``("a", "bc/d")`` and ``("ab", "c/d")``
    never share an input (the plan writes only ``‖``).
    """
    digest = hashlib.sha256(
        b'\0'.join(part.encode('utf-8', 'surrogateescape')
                   for part in (client_id, dataset, destination))
    ).digest()
    return (int.from_bytes(digest[:8], 'big') >> 11) / 2 ** 53


def slot_offset(
    frac: float, frequency: Seconds, period: Optional[Seconds], check_interval: Seconds
) -> float:
    """``offset = lo + frac * (W - lo)`` with ``W = min(P, F)`` and
    ``lo = min(check_interval + 60 s, W / 2)``. ``frac = 0`` gives ``lo``."""
    f = _secs(frequency)
    p = f if period is None else _secs(period)
    w = min(p, f)
    lo = min(_secs(check_interval) + LO_MARGIN, w / 2)
    return lo + frac * (w - lo)


def slot_time(k: int, period: float, offset: float) -> float:
    """``s_k = k * P + offset``."""
    return k * period + offset


def slot_index(now: float, period: float, offset: float) -> int:
    """The ``k`` of the latest slot at or before ``now``."""
    return int(math.floor((now - offset) / period))


# ----------------------------------------------------------------------------
# State
# ----------------------------------------------------------------------------

class _PairState:
    __slots__ = (
        'spec', 'frac', 'offset', 'attempted', 'armed', 'catchup_at', 'retry_at',
        'retries_used', 'confirmed', 'confirm_due', 'confirm_at', 'confirm_used', 'last_start',
        'parent', 'children', 'was_held',
    )

    def __init__(self, spec: PairSpec, frac: float, offset: float) -> None:
        self.spec = spec
        self.frac = frac
        self.offset = offset
        #: Highest slot index attempted (or covered by an attempt). Only raised.
        self.attempted = 0
        #: The slot index `_arm` last looked at (None: none yet).
        self.armed: Optional[int] = None
        #: Due time that replaces ``s_k`` for the armed slot (catch-up delay).
        self.catchup_at: Optional[float] = None
        #: Retry timer (retryable backoff, "other failure" cap, WAITING).
        self.retry_at: Optional[float] = None
        #: Consecutive retryable failures not yet recorded.
        self.retries_used = 0
        self.confirmed = False
        #: R-2: due once, because children wait for this pair.
        self.confirm_due = False
        self.confirm_at = 0.0
        #: The one-shot confirmation attempt was started (`started`).
        self.confirm_used = False
        #: Wall time of the last `started` (or the fed-in ``last_attempt``).
        self.last_start: Optional[float] = None
        self.parent: Optional[PairKey] = None
        self.children: List[PairKey] = []
        #: R-2: the pair was held by an unconfirmed ancestor when last looked
        #: at (`SendSchedule._note_hold`), so its release is noticed.
        self.was_held = False


class _DiscoveryState:
    __slots__ = ('order', 'next_at', 'in_flight')

    def __init__(self, order: int, next_at: float) -> None:
        self.order = order
        self.next_at = next_at
        self.in_flight = False


def _ancestors(dataset: str) -> Iterator[str]:
    """Strict ancestors of a dataset name, nearest first."""
    name = dataset
    while True:
        cut = name.rfind('/')
        if cut <= 0:
            return
        name = name[:cut]
        yield name


class SendSchedule:
    """When each (dataset, destination) pair is sent, and which due pairs may
    start now. See the module docstring for the rules.

    Typical B3 loop (single thread, the scheduler's)::

        sched = SendSchedule(client_id, check_interval)
        sched.update_pairs(specs)                       # incl. last_attempt
        sched.set_discovery_parents(parent_names)
        while not leaving:
            for parent in sched.discovery_due():
                sched.discovery_started(parent); spawn_discovery(parent)
            for key in sched.dispatch():
                # The sender reads `force_confirm` once, at its start: its
                # `send_pair(..., force_confirm=sched.wants_confirmation(key))`
                # skips the anchor-age check while children wait for this pair.
                sched.started(key); spawn_sender(key, force=sched.wants_confirmation(key))
            wake = sched.next_wakeup()                  # sleep <= STOP_POLL, or until a
            ...                                         # queue item arrives
            # on a sender outcome (`PairOutcome`):
            #   if sched.finished(key, kind, retryable): record it
            #   if outcome.confirmed is True:  sched.confirmed(key)
            #   if outcome.confirmed is False: sched.unconfirmed(key)
            #   (`None`: the call learned nothing; leave the state as it is)
            # on a discovery result: sched.discovery_finished(parent, ok);
            #   if ok: sched.update_pairs(new_specs)

    Call `finished` BEFORE `confirmed`/`unconfirmed`: `finished` re-arms the
    one-shot confirmation of a NOT_DUE, STOPPED or WAITING pair that is still
    unconfirmed (`unconfirmed` does the same for a copy found gone), and
    `confirmed` then ends it. The senders' `SendMemory` is the truth about
    copies (`COPY_CONFIRMED`); this class only mirrors it through the outcomes.

    After ANY event (a start, a finish, ``confirmed``, ``update_pairs``) call
    ``dispatch`` again: a blocked pair becomes runnable only through an event,
    which is why `next_wakeup` ignores due-but-blocked pairs.
    """

    def __init__(
        self,
        client_id: str,
        check_interval: Seconds,
        *,
        clock: Callable[[], float] = time.time,
        fraction: Callable[[str, str, str], float] = pair_fraction,
        max_senders: int = MAX_SENDERS,
        max_per_destination: int = MAX_SENDERS_PER_DESTINATION,
    ) -> None:
        self.client_id = client_id
        self.check_interval = _secs(check_interval)
        self._clock = clock
        self._fraction = fraction
        self.max_senders = max_senders
        self.max_per_destination = max_per_destination
        self._states: Dict[PairKey, _PairState] = {}
        #: Pairs with a sender thread, kept apart from `_states` so that a
        #: pair removed while it runs still counts against the caps and the
        #: exclusion until `finished`.
        self._running: Set[PairKey] = set()
        self._parents: Dict[str, _DiscoveryState] = {}

    def _now(self, now: Optional[float]) -> float:
        return self._clock() if now is None else now

    # -- the pair set --------------------------------------------------------

    def update_pairs(self, pairs: Iterable[PairSpec], now: Optional[float] = None) -> None:
        """Replace the pair set (configured pairs plus each parent's last known
        children). Reload is out of scope: the worker restarts on reload, so
        this is for children appearing and vanishing between discoveries.

        A pair that is no longer listed loses ALL its state (including its R-2
        confirmation); a sender still running for it keeps its cap slot until
        ``finished``. A new pair starts fresh with the catch-up decision from
        its ``PairSpec.last_attempt`` (`_init_state`). A pair that stays keeps
        its state; if its P/F (hence its offset) changed, its attempted slot is
        re-derived from the wall time of its last start (a pending retry and its
        burst count are kept). Raises ``ValueError``
        on a duplicate (dataset, destination).
        """
        now = self._now(now)
        states: Dict[PairKey, _PairState] = {}
        for spec in pairs:
            key = spec.key
            if key in states:
                raise ValueError(f"duplicate pair {key!r}")
            frac = self._fraction(self.client_id, spec.dataset, spec.destination)
            offset = slot_offset(frac, spec.frequency, spec.effective_period, self.check_interval)
            old = self._states.get(key)
            if old is None:
                st = _PairState(spec, frac, offset)
                self._init_state(st, now, spec.last_attempt)
            else:
                st = old
                regrid = (
                    offset != st.offset or spec.effective_period != st.spec.effective_period
                )
                st.spec, st.frac, st.offset = spec, frac, offset
                if regrid:
                    # A pending retry and its burst count belong to the pair's
                    # history, not to its grid: keep them.
                    retry_at, retries_used = st.retry_at, st.retries_used
                    self._init_state(st, now, st.last_start)
                    st.retry_at, st.retries_used = retry_at, retries_used
            states[key] = st
        self._states = states
        self._link(now)

    def _init_state(self, st: _PairState, now: float, last_attempt: Optional[float]) -> None:
        """Catch-up (plan "Scheduling"): with ``last_attempt < s_prev`` the pair
        is due ``frac * min(W, 300 s)`` from ``now``; otherwise the current slot
        counts as attempted and the next regular slot is next."""
        p = st.spec.effective_period
        k = slot_index(now, p, st.offset)
        st.last_start = last_attempt
        st.retry_at = None
        st.retries_used = 0
        if last_attempt is not None and last_attempt >= slot_time(k, p, st.offset):
            st.attempted, st.armed, st.catchup_at = k, None, None
        else:
            st.attempted, st.armed = k - 1, k
            st.catchup_at = now + st.frac * min(st.spec.window, CATCH_UP_CAP)

    def _link(self, now: float) -> None:
        """Rebuild the nearest-ancestor links and the "waiting children" sets,
        and arm the once-per-incarnation confirmation (R-2)."""
        for st in self._states.values():
            st.parent = None
            st.children = []
        for key, st in self._states.items():
            for name in _ancestors(key.dataset):
                anc = PairKey(name, key.destination)
                if anc in self._states:
                    st.parent = anc
                    self._states[anc].children.append(key)
                    break
        for st in self._states.values():
            if st.confirmed or not st.children:
                st.confirm_due = False
            elif not st.confirm_used and not st.confirm_due:
                # Unconfirmed, children waiting, never asked: due once, now.
                st.confirm_due, st.confirm_at = True, now

    # -- queries -------------------------------------------------------------

    def keys(self) -> List[PairKey]:
        """The pairs, in the order `update_pairs` was given them."""
        return list(self._states)

    def spec(self, key: Tuple[str, str]) -> PairSpec:
        return self._states[PairKey(*key)].spec

    def offset(self, key: Tuple[str, str]) -> float:
        """The pair's start offset ``lo + frac * (W - lo)`` in seconds."""
        return self._states[PairKey(*key)].offset

    def fraction(self, key: Tuple[str, str]) -> float:
        return self._states[PairKey(*key)].frac

    @property
    def running(self) -> Set[PairKey]:
        return set(self._running)

    def is_confirmed(self, key: Tuple[str, str]) -> bool:
        st = self._states.get(PairKey(*key))
        return st is not None and st.confirmed

    def held_by(self, key: Tuple[str, str]) -> Optional[PairKey]:
        """R-2 (plan "R-2 Parent first", Scheduling "applied to EVERY child
        pair, configured or expanded"): the nearest ancestor pair on the same
        destination when it is not confirmed in this incarnation, else
        ``None``. A held pair is not listed by `due`, not started by
        `dispatch`, and ignored by `next_wakeup`; it is released by
        `confirmed` of its ancestor (or by the ancestor leaving the pair set).
        A pair with no ancestor pair is never held."""
        st = self._states.get(PairKey(*key))
        if st is None or st.parent is None:
            return None
        parent = self._states.get(st.parent)
        return None if parent is None or parent.confirmed else st.parent

    def waiting_on(self) -> Dict[PairKey, List[PairKey]]:
        """Ancestor pair -> the child pairs held behind it, for the caller's
        "one WARNING per (ancestor, destination) per episode, with a count"."""
        out: Dict[PairKey, List[PairKey]] = {}
        for key, st in self._states.items():
            held = self.held_by(key)
            if held is not None:
                out.setdefault(held, []).append(key)
        return out

    def wants_confirmation(self, key: Tuple[str, str]) -> bool:
        """True while this pair is unconfirmed and children wait for it: the
        sender must then negotiate whatever its own anchor-age check says (the
        serial worker's ``_confirmation_due``), until `confirmed` is called."""
        st = self._states.get(PairKey(*key))
        return st is not None and not st.confirmed and bool(st.children)

    # -- due times -----------------------------------------------------------

    def _note_hold(self, key: PairKey, st: _PairState, now: float) -> bool:
        """Whether R-2 holds the pair now, remembering it. When a hold ENDS (the
        ancestor was confirmed, or left the pair set) the pair is released
        unstaggered (decision "Children released by R-2 are not staggered"): the
        slot it sat out is armed on time, with no catch-up delay, so it is due
        at once (the caps still apply). Without this, `_arm` would see a slot
        that passed during a long hold as an outage and spread the children
        over ``frac * min(W, 300 s)``."""
        if self.held_by(key) is not None:
            st.was_held = True
            return True
        if st.was_held:
            st.was_held = False
            k = slot_index(now, st.spec.effective_period, st.offset)
            if st.attempted < k:
                st.armed, st.catchup_at = k, None
        return False

    def _arm(self, st: _PairState, now: float) -> None:
        """Look at the current slot once. Idempotent per slot.

        Outage catch-up (decision "An outage gets one catch-up within 5 min"):
        if the idle pair first sees its current, unattempted slot ``k`` more
        than ``LATE_SLACK`` after ``s_k`` (the scheduler was stopped, the host
        suspended, the clock stepped forward), its due time becomes ``now +
        frac * min(W, 300 s)`` instead of ``s_k``. Slots missed before ``k``
        are not run: they are coalesced into this one attempt.
        """
        p = st.spec.effective_period
        k = slot_index(now, p, st.offset)
        if st.attempted < k and st.armed != k:
            st.armed = k
            late = now - slot_time(k, p, st.offset)
            st.catchup_at = (
                now + st.frac * min(st.spec.window, CATCH_UP_CAP) if late > LATE_SLACK else None
            )

    def _unstall(self, st: _PairState, now: float) -> None:
        """Bound every pending due time after a clock step back (module
        docstring, "Clock jumps"). Idempotent, and a no-op while the clock only
        moves forward.

        * The attempted index is clamped to ``slot_index(now)`` whenever it is
          beyond it (only a step back can cause that): the pair then runs at
          the next slot, at most ``P`` after ``now``. This may re-run an
          attempted slot once, deliberately.
        * A catch-up, retry or confirmation time lying further ahead than the
          longest delay it is ever given (``CATCH_UP_CAP``, ``FAILURE_RETRY_CAP``;
          a confirmation is armed at ``now`` or, by `_rearm_confirmation`, no
          later than a retry) is pulled back to that bound. The test is the distance itself,
          so no legitimate delay is ever shortened.
        """
        p = st.spec.effective_period
        k = slot_index(now, p, st.offset)
        if st.attempted > k:
            st.attempted = k
            st.armed = None
            st.catchup_at = None
        if st.catchup_at is not None and st.catchup_at - now > CATCH_UP_CAP:
            st.catchup_at = now + CATCH_UP_CAP
        if st.retry_at is not None and st.retry_at - now > FAILURE_RETRY_CAP:
            st.retry_at = now + FAILURE_RETRY_CAP
        if st.confirm_due and st.confirm_at - now > FAILURE_RETRY_CAP:
            st.confirm_at = now

    def _regular_due(self, st: _PairState, now: float) -> float:
        """The slot-based due time. Never re-runs an attempted slot: when the
        current slot ``k`` is attempted (or the clock went back behind an
        attempted slot) the next candidate is ``s_(attempted+1)``."""
        self._unstall(st, now)
        self._arm(st, now)
        p = st.spec.effective_period
        k = slot_index(now, p, st.offset)
        if st.attempted < k:
            return st.catchup_at if st.catchup_at is not None else slot_time(k, p, st.offset)
        return slot_time(st.attempted + 1, p, st.offset)

    def _due_at(self, st: _PairState, now: float) -> float:
        times = [self._regular_due(st, now)]
        if st.retry_at is not None:
            times.append(st.retry_at)
        if st.confirm_due:
            times.append(st.confirm_at)
        return min(times)

    def due_time(self, key: Tuple[str, str], now: Optional[float] = None) -> Optional[float]:
        """The time the pair is (or was) due: the earliest of the next regular
        slot (or catch-up time), a pending retry and a pending R-2
        confirmation. ``None`` for an unknown or running pair. Ignores R-2
        holds and the caps."""
        now = self._now(now)
        key = PairKey(*key)
        st = self._states.get(key)
        if st is None or key in self._running:
            return None
        return self._due_at(st, now)

    def due(self, now: Optional[float] = None) -> List[PairKey]:
        """The pairs whose due time has come, in start order. Excludes running
        pairs and pairs held by R-2 (children are neither listed nor
        dispatched while their ancestor is unconfirmed).

        Order (plan "The ancestor goes first, then by due time, depth and config
        order"): a pair ranks by the earliest due time among itself and its due
        descendants on the same destination, so an ancestor never ranks behind
        a descendant; then by depth (shallower first), config order, and the
        names. This does not apply the exclusion or the caps: `dispatch` does.
        """
        now = self._now(now)
        due_at: Dict[PairKey, float] = {}
        for key, st in self._states.items():
            if key in self._running or self._note_hold(key, st, now):
                continue
            t = self._due_at(st, now)
            if t <= now:
                due_at[key] = t
        rank = dict(due_at)
        for key, t in due_at.items():
            for name in _ancestors(key.dataset):
                anc = PairKey(name, key.destination)
                if anc in rank and t < rank[anc]:
                    rank[anc] = t
        return sorted(
            due_at,
            key=lambda k: (
                rank[k], self._states[k].spec.depth, self._states[k].spec.config_order,
                k.dataset, k.destination,
            ),
        )

    def dispatch(
        self, now: Optional[float] = None, running: Optional[Iterable[Tuple[str, str]]] = None
    ) -> List[PairKey]:
        """The due pairs that may start now, in start order. The caller calls
        `started` for each before the next `dispatch` (this method reserves
        nothing between calls).

        ``running`` is the caller's own view of live senders; it is added to
        the schedule's own (`started` .. `finished`), so a pair removed by
        `update_pairs` while its sender lives still counts.

        * **Caps** (decision "Caps"): at most ``MAX_SENDERS`` in all and
          ``MAX_SENDERS_PER_DESTINATION`` per destination, counting running
          senders and the pairs chosen so far in this call. A pair held back
          by a cap does not stop pairs of other destinations.
        * **Exclusion**: a pair is skipped while a pair of the SAME destination
          that is its ancestor or descendant is running, or while an ancestor
          pair is due (the ancestor goes first, even if it is itself waiting
          for a free sender). A skipped pair never blocks unrelated pairs.
          The same dataset to several destinations is unrelated, so those may
          run at once.
        * R-2 holds are already excluded by `due`.
        """
        now = self._now(now)
        active: Set[PairKey] = set(self._running)
        if running is not None:
            active.update(PairKey(*k) for k in running)
        total = len(active)
        per_dest: Dict[str, int] = {}
        for key in active:
            per_dest[key.destination] = per_dest.get(key.destination, 0) + 1
        candidates = self.due(now)
        due_set = set(candidates)
        # Names of running pairs, and of every ancestor of a running pair.
        busy_names: Set[PairKey] = set(active)
        above_running: Set[PairKey] = set()
        for key in active:
            for name in _ancestors(key.dataset):
                above_running.add(PairKey(name, key.destination))
        chosen: List[PairKey] = []
        for key in candidates:
            if total >= self.max_senders:
                break
            if key in active:
                continue
            if per_dest.get(key.destination, 0) >= self.max_per_destination:
                continue
            ancestors = [PairKey(n, key.destination) for n in _ancestors(key.dataset)]
            if key in above_running:
                continue                    # a descendant is running
            if any(a in busy_names or a in due_set for a in ancestors):
                continue                    # an ancestor runs or is due ahead
            chosen.append(key)
            active.add(key)
            busy_names.add(key)
            for a in ancestors:
                above_running.add(a)
            total += 1
            per_dest[key.destination] = per_dest.get(key.destination, 0) + 1
        return chosen

    def next_wakeup(self, now: Optional[float] = None) -> Optional[float]:
        """The earliest time strictly after ``now`` at which a pair that is not
        due yet (and not held, not running) becomes due, or a parent's
        discovery becomes due. ``None`` when nothing is pending. Pairs that
        are due now but blocked (caps, exclusion, R-2) are ignored: they run
        only after an event, and the caller re-dispatches after every event.
        The caller sleeps ``min(wakeup - now, STOP_POLL)``."""
        now = self._now(now)
        best: Optional[float] = None
        for key, st in self._states.items():
            if key in self._running or self._note_hold(key, st, now):
                continue
            t = self._due_at(st, now)
            if t > now and (best is None or t < best):
                best = t
        for d in self._parents.values():
            at = self._discovery_at(d, now)
            if not d.in_flight and at > now and (best is None or at < best):
                best = at
        return best

    # -- lifecycle -----------------------------------------------------------

    def started(self, key: Tuple[str, str], now: Optional[float] = None) -> None:
        """A sender thread for the pair starts. Marks the current slot
        attempted (never lower than before, except that a clock step back first
        clamps the old attempted index to the current slot, `_unstall`: at most
        one extra, cheap run per back-step, a deliberate deviation from the
        plan's "never re-run an attempted slot"), clears the retry/catch-up timers and the one-shot R-2
        confirmation, and counts the pair as running. ``KeyError`` for an
        unknown pair, ``ValueError`` if it is already running (never two
        senders for one pair)."""
        now = self._now(now)
        key = PairKey(*key)
        st = self._states[key]
        if key in self._running:
            raise ValueError(f"{key!r} is already running")
        self._running.add(key)
        self._unstall(st, now)
        st.attempted = max(st.attempted, slot_index(now, st.spec.effective_period, st.offset))
        st.last_start = now
        st.catchup_at = None
        st.retry_at = None
        st.confirm_used = st.confirm_used or bool(st.children)
        st.confirm_due = False

    def finished(
        self, key: Tuple[str, str], kind: Union[Outcome, str],
        retryable: bool = False, now: Optional[float] = None,
    ) -> bool:
        """A sender ended. Returns ``record_now``: whether the caller should
        write the outcome to the status file now.

        =========================  ==========  =====================================
        outcome                    record_now  next
        =========================  ==========  =====================================
        ``OK``                     True        next slot; ONE follow-up, due now, if
                                               later slots passed while it ran
        ``NOT_DUE``                False       as ``OK``
        ``RETRYABLE`` (or          False       retry after 60, 120, 300 s, while any
        ``FAILED`` + retryable)                are left
                                   True        once they are used up (counted from
                                               the first retryable failure of the
                                               burst): then as ``FAILED``
        ``FAILED``                 True        retry due ``min(next slot, 1 h)``
        ``STOPPED``                False       nothing
        ``WAITING``                False       asked again after ``WAIT_RECHECK``
        =========================  ==========  =====================================

        ``NOT_DUE``, ``STOPPED`` and ``WAITING`` confirmed nothing: a pair that
        is still unconfirmed with children waiting has its one-shot R-2
        confirmation re-armed (`_rearm_confirmation`), or its children would
        wait for ever (`started` spent the one-shot).

        A pair no longer in the schedule only releases its cap slot and
        returns ``False`` (its record goes at the next ``retain``).

        Clock steps: a clock stepped back is handled first (`_unstall`: the
        attempted index is clamped to the current slot, which may re-run that
        slot once; a deliberate deviation from the plan's "never re-run an
        attempted slot"), so the due times set here are never more than one
        period (retry cap ``FAILURE_RETRY_CAP``, catch-up cap
        ``CATCH_UP_CAP``) away from ``now``.
        """
        now = self._now(now)
        key = PairKey(*key)
        kind = Outcome(kind)
        if kind is Outcome.FAILED and retryable:
            kind = Outcome.RETRYABLE
        self._running.discard(key)
        st = self._states.get(key)
        if st is None:
            return False
        p = st.spec.effective_period
        self._unstall(st, now)
        k = slot_index(now, p, st.offset)
        if kind in (Outcome.OK, Outcome.NOT_DUE):
            st.retries_used = 0
            st.retry_at = None
            if k > st.attempted:
                # Overrun: slots passed while it ran. One coalesced follow-up,
                # due now (armed on time: not an outage catch-up).
                st.armed, st.catchup_at = k, None
            if kind is Outcome.NOT_DUE:
                self._rearm_confirmation(st, now)
            return kind is Outcome.OK
        if kind is Outcome.STOPPED:
            self._rearm_confirmation(st, now)
            return False
        if kind is Outcome.WAITING:
            st.retry_at = now + WAIT_RECHECK
            self._rearm_confirmation(st, now)
            return False
        # A failure covers the slots that passed while it ran.
        st.attempted = max(st.attempted, k)
        if kind is Outcome.RETRYABLE and st.retries_used < len(RETRY_DELAYS):
            st.retry_at = now + RETRY_DELAYS[st.retries_used]
            st.retries_used += 1
            return False
        st.retries_used = 0
        st.retry_at = now + FAILURE_RETRY_CAP
        return True

    def confirmed(self, key: Tuple[str, str]) -> None:
        """R-2: the pair's copy is confirmed in this incarnation (its
        negotiate reported an existing copy, or a stream to it was
        acknowledged; a dry-run success counts: ``PairOutcome.confirmed is
        True``). Releases the children held behind it. Idempotent; unknown
        pairs are ignored. Confirmation lasts until the pair leaves the
        schedule (``update_pairs``) or `unconfirmed`."""
        st = self._states.get(PairKey(*key))
        if st is not None:
            st.confirmed = True
            st.confirm_due = False

    def unconfirmed(self, key: Tuple[str, str], now: Optional[float] = None) -> None:
        """R-2: the call learned the pair's copy is NOT there (``PairOutcome.
        confirmed is False``: the negotiate found it gone). Drops the
        confirmation, so the pair's children are held again (`held_by`), and,
        when it has children, re-arms the one-shot confirmation
        (`_rearm_confirmation`) so it negotiates again soon. Idempotent;
        unknown pairs are ignored."""
        st = self._states.get(PairKey(*key))
        if st is None:
            return
        st.confirmed = False
        self._rearm_confirmation(st, self._now(now))

    def _rearm_confirmation(self, st: _PairState, now: float) -> None:
        """Make the one-shot R-2 confirmation due again, for an unconfirmed
        pair with children (else nothing). Never hot: it is due no earlier than
        ``WAIT_RECHECK`` after the pair's last start and no earlier than a
        pending retry, so a sender that keeps answering "nothing" (or failing)
        is asked once a minute at most, and a failure's own backoff still
        governs."""
        if st.confirmed or not st.children:
            return
        at = now if st.last_start is None else max(now, st.last_start + WAIT_RECHECK)
        if st.retry_at is not None:
            at = max(at, st.retry_at)
        st.confirm_used = False
        st.confirm_due, st.confirm_at = True, at

    # -- discovery -----------------------------------------------------------

    def set_discovery_parents(
        self, parents: Iterable[str], now: Optional[float] = None
    ) -> None:
        """The enabled recursive datasets with ``remote:`` entries (config
        order). A new parent is due for discovery at once; a parent that is no
        longer listed is forgotten."""
        now = self._now(now)
        new: Dict[str, _DiscoveryState] = {}
        for order, name in enumerate(parents):
            old = self._parents.get(name)
            if old is None:
                new[name] = _DiscoveryState(order, now)
            else:
                old.order = order
                new[name] = old
        self._parents = new

    def discovery_interval(self, parent: str) -> float:
        """Seconds between discoveries of a parent: ``max(check_interval,
        min(smallest P of the parent's own pairs, 1 h))`` (``check_interval``
        when it has no pair). A new child is found before the next snapshot
        period of its parent, but a recursive listing is never run more often
        than the check interval."""
        periods = [
            st.spec.effective_period for k, st in self._states.items() if k.dataset == parent
        ]
        base = min(periods + [DISCOVERY_MAX_INTERVAL]) if periods else self.check_interval
        return max(self.check_interval, min(base, DISCOVERY_MAX_INTERVAL))

    def _discovery_at(self, d: _DiscoveryState, now: float) -> float:
        """``next_at``, pulled back to the longest delay it is ever given
        after a clock step back (module docstring, "Clock jumps"). The capped
        value is WRITTEN BACK, so it stops sliding with the clock and the
        discovery does become due. Idempotent, and never shortens a delay
        while the clock only moves forward."""
        cap = now + max(self.check_interval, DISCOVERY_MAX_INTERVAL)
        if d.next_at > cap:
            d.next_at = cap
        return d.next_at

    def discovery_due(self, now: Optional[float] = None) -> List[str]:
        """The parents whose discovery should start now: not in flight (**at
        most one per parent at a time**), ``next_at`` reached, never-discovered
        first then the longest overdue, and no more than ``MAX_DISCOVERIES``
        in flight in all. While a discovery runs, or after it fails, the
        caller keeps using the last known child set (it simply does not call
        `update_pairs` with a new one)."""
        now = self._now(now)
        free = MAX_DISCOVERIES - sum(1 for d in self._parents.values() if d.in_flight)
        if free <= 0:
            return []
        ready = [
            (self._discovery_at(d, now), d.order, name) for name, d in self._parents.items()
            if not d.in_flight and self._discovery_at(d, now) <= now
        ]
        return [name for _, _, name in sorted(ready)[:free]]

    def discovery_started(self, parent: str, now: Optional[float] = None) -> None:
        d = self._parents[parent]
        if d.in_flight:
            raise ValueError(f"discovery of {parent!r} is already running")
        d.in_flight = True

    def discovery_finished(
        self, parent: str, ok: bool = True, now: Optional[float] = None
    ) -> None:
        """The next discovery is due ``discovery_interval`` after a success and
        ``min(interval, 300 s)`` after a failure. An unknown parent (dropped
        meanwhile) is ignored."""
        now = self._now(now)
        d = self._parents.get(parent)
        if d is None:
            return
        d.in_flight = False
        interval = self.discovery_interval(parent)
        d.next_at = now + (interval if ok else min(interval, DISCOVERY_RETRY))

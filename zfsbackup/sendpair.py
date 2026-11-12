"""One (dataset, destination) pair's turn, as a function (plan "Track B", item B1.1).

`send_pair` is what the remote worker does for ONE pair: the parent-first wait
for the descendants of a recursive dataset (R-2), the due check (the dataset's
OWN anchor, the clock-tolerance ERROR), `backup_dataset`, the classification of
what it returned and the per-destination exception path. It RETURNS a
`PairOutcome` and records nothing: the caller owns the `health.StatusWriter`
(the serial worker today, the scheduler thread of Track B later), so a sender
thread can run `send_pair` without ever touching it. It shares state only
through the `SendMemory` its `AncestorRule` carries (thread-safe), the manager
objects it is given and the callables `should_stop` / `exit_pending`.

This module imports neither `zfsbackup.remote` nor `zfsbackup.local` (nor any
transport): the worker imports those lazily after the start-up version check
(S-8), and so must not anything that is imported before it.

Why `should_stop` and `exit_pending` are two callables: `should_stop` is polled
by the chain between steps (a safe point), `exit_pending` decides what a
`False` result means afterwards -- a stop or reload that cut the work short is
neither a success nor a failure. The serial worker passes the same function
for both; a sender thread passes "stop or leave" for `should_stop` and the
same for `exit_pending`.
"""

import logging
from dataclasses import dataclass, replace
from typing import Any, Callable, Mapping, Optional, Tuple

from zfsbackup.sendmemory import COPY_CONFIRMED, DRY_RUN_SENT, ORPHAN_HOLD, WAITING, SendMemory


_logger = logging.getLogger(__name__)

#: `backup_dataset` returned True. That is every way a pair can be finished
#: well: a stream sent and acknowledged, nothing to send (already up to date,
#: no snapshots) or a dry run. The return value cannot tell them apart, and
#: the status file records them all as one success. After at least one stream a
#: stop between chain steps is also a True (`PairOutcome.retryable` then says
#: work remains).
PAIR_DONE = 'done'
#: The destination's anchor is fresher than the frequency: nothing was asked of
#: the sender. Recorded nowhere.
PAIR_NOT_DUE = 'not_due'
#: Held by the parent-first rule (R-2): the ancestor's copy is not confirmed,
#: its pair cannot be found, or the child's snapshots could not be listed
#: (`PairOutcome.detail` says which). Recorded nowhere: the ancestor's own
#: record carries the root cause.
PAIR_WAITING = 'waiting'
#: `backup_dataset` returned False (or raised) with no exit reason pending: a
#: failure, with the sender's own account of why in `PairOutcome.error` when it
#: has one. Recorded as a failed send.
PAIR_FAILED = 'failed'
#: `backup_dataset` returned False (or raised) while an exit reason was pending:
#: a stop or reload cut the work short (a killed child, a closed stream). Neither
#: a success nor a failure, recorded nowhere.
PAIR_STOPPED = 'stopped'

#: `AncestorRule.hold` verdicts.
HOLD_WAITING = 'waiting'
HOLD_NO_SNAPSHOTS = 'no_snapshots'
HOLD_UNLISTED = 'unlisted'
HOLD_ORPHAN = 'orphan'

#: What a failed pair's status record says when the sender has no account.
NOT_COMPLETED = 'not completed (see the journal)'


@dataclass(frozen=True)
class PairOutcome:
    """What one `send_pair` call came to. `kind` is one of the `PAIR_*`
    constants; `error` is the text of a FAILED (or STOPPED-by-exception) pair;
    `warning` a standing warning about a success (`health.OutcomeRecord.warning`;
    no sender sets one today); `detail` a short reason for the kinds that are
    not recorded (the `HOLD_*` verdict of a WAITING pair, "no snapshots" of a
    DONE one); `retryable` is `ChainSender.last_retryable` for the pair: the
    outcome is transient (busy, reloading, an unsettled ancestor, a connect
    failure before any byte, a stop between steps) and worth retrying soon.

    `confirmed` is what this call learned about the destination's copy (R-2),
    read from the `SendMemory`'s `COPY_CONFIRMED` before and after the call:
    `True` when the copy is confirmed (the sender noted it, or a dry run came
    back DONE, as `AncestorRule.confirmed` counts it), `False` when the call
    learned there is NO copy (the negotiate cleared a confirmation), `None`
    when it learned nothing (not due, held, or no sender memory to read). It is
    the one thing a scheduler needs to keep its own R-2 state in step with the
    senders': `SendSchedule.confirmed(key)` on `True`, `unconfirmed(key)` on
    `False`."""
    dataset: str
    destination: str
    kind: str
    error: Optional[str] = None
    warning: Optional[str] = None
    retryable: bool = False
    detail: Optional[str] = None
    confirmed: Optional[bool] = None


def waiting_message(ancestor: str, destination: str, count: int) -> str:
    """The one WARNING per (ancestor, destination) episode (R-2): `count`
    descendants wait for `ancestor`'s copy. The sender (`AncestorRule.hold`) and
    the scheduler (`sendloop.SendLoop`) word it alike and share the `WAITING`
    latch, so one episode is announced once whoever notices it first."""
    return (
        f"{count} descendant{'' if count == 1 else 's'} of {ancestor} wait until "
        f"its copy at {destination} is confirmed in this run (a negotiate that finds "
        f"the copy, or an acknowledged send); they are not sent until then"
    )


def released_message(ancestor: str, destination: str) -> str:
    """The INFO that ends that episode."""
    return f"{ancestor} is confirmed at {destination}: its descendants follow"


class LazySender:
    """A sender that is resolved only if the pair gets past R-2. `resolve()`
    returns the manager (or raises: what a missing local backup manager does,
    ERROR and all); `send_pair` calls it after the parent-first rule, so a
    held pair never needs the sender, exactly as the serial worker always
    behaved."""

    def __init__(self, resolve: Callable[[], Any]):
        self.resolve = resolve


class AncestorRule:
    """R-2, the parent-first rule: an EXPANDED child (a descendant of a
    recursive dataset) is not sent to a destination until the nearest ancestor
    that has a pair to the same destination (configured or expanded) is
    CONFIRMED there in this incarnation (`ChainSender.copy_confirmed`). Built
    by the worker from one cycle's state:

    * `pairs`: this cycle's pairs by dataset name (never changed in place);
    * `dependents`: this cycle's expanded children per `(nearest ancestor pair,
      destination)` (never changed in place);
    * `memory`: the shared `SendMemory` (the once-per-episode WAITING latch and
      the dry-run set);
    * `sender_for(manager, destination)`: the sender serving a destination, or
      `None` when there is none (nothing is built or imported here). Kept for
      the callers that build the rule with it; `confirmed` no longer consults it.

    Methods only read the first two, so one rule serves any number of threads."""

    def __init__(
        self,
        pairs: Mapping[str, Any],
        dependents: Mapping[Tuple[str, str], int],
        memory: SendMemory,
        sender_for: Callable[[Any, str], Any],
    ):
        self.pairs = pairs
        self.dependents = dependents
        self.memory = memory
        self.sender_for = sender_for

    def nearest_pair(self, name: str, destination: str):
        """The nearest strict ancestor of `name` with a pair (a `remote:`
        entry) to `destination` in this cycle's list, configured or expanded,
        or `None`. A configured ancestor that is not a pair is passed over."""
        ancestor = name.rpartition('/')[0]
        while ancestor:
            info = self.pairs.get(ancestor)
            if info is not None and any(rc.destination == destination for rc in info.config.remote):
                return info
            ancestor = ancestor.rpartition('/')[0]
        return None

    def confirmed(self, manager, ancestor, destination: str) -> bool:
        """Whether the destination's copy of `ancestor` has been CONFIRMED in
        THIS incarnation (`ChainSender.copy_confirmed`): its `backup_dataset`
        returned True after a negotiate that reported an existing copy, or a
        stream to it committed. No anchor and no status record is evidence:
        an anchor can be stale (a changed destination URL or `zfs:` target,
        `identity set-client-id --force`, a copy the server lost), and a
        child sent first would then make `create_ancestors` build the
        ancestor's copy as an empty container that its own raw or
        encrypted-target full stream can never replace. Anything but an exact
        `True` (no sender, an exception) is "not confirmed".

        `--dry-run` sends and creates nothing, so there the ancestor's
        `backup_dataset` having returned True in this incarnation passes as
        well (the log then shows the children's turn)."""
        key = (ancestor.name, destination)
        if manager.config.dry_run and self.memory.noted(DRY_RUN_SENT, key):
            return True
        # Read straight from the memory the senders write (`ChainSender.
        # copy_confirmed` reads the same latch), so a scheduler thread needs no
        # sender object to decide.
        return self.memory.noted(COPY_CONFIRMED, key)

    def confirmation_due(self, manager, dsi, destination: str) -> bool:
        """`dsi` is the nearest ancestor pair of at least one waiting expanded
        child and its copy at `destination` is not confirmed yet: it is DUE
        whatever its anchor says, so it negotiates once per incarnation. An
        up-to-date copy then settles cheaply (no stream) and, being confirmed,
        releases the children."""
        if self.dependents.get((dsi.name, destination), 0) <= 0:
            return False
        return not self.confirmed(manager, dsi, destination)

    def hold(self, manager, dsi, destination: str, logger) -> Optional[str]:
        """R-2 for `dsi` -> `destination`: `None` (go on), or why the pair is
        held, one of the `HOLD_*` verdicts. Fails closed: with no ancestor pair
        found, the parent it was expanded from is the ancestor, and without a
        way to look at it the child waits (`HOLD_ORPHAN`, one WARNING). One
        WARNING per episode per (ancestor, destination) names the ancestor and
        how many children wait, the children at DEBUG.

        A child with no managed snapshots is not held but not sent either
        (`HOLD_NO_SNAPSHOTS`): `backup_dataset` returns before it negotiates,
        so it can create nothing, and the caller records it as done, as
        `backup_dataset` would. A failed listing holds it silently
        (`HOLD_UNLISTED`: the next cycle lists again), so the decision and the
        send never rest on two different listings.

        A configured dataset is never held by this rule (it keeps its
        pre-recursive behaviour: `expanded_from` is `None`)."""
        if dsi.expanded_from is None:
            return None
        ancestor = self.nearest_pair(dsi.name, destination)
        if ancestor is None:
            # Fail closed: the parent it was expanded from.
            ancestor = self.pairs.get(dsi.expanded_from)
        orphan_key = (dsi.name, destination)
        if ancestor is None:
            message = (
                f"{dsi.name} -> {destination}: cannot find {dsi.expanded_from}, which it was "
                "expanded from; not sent"
            )
            # One WARNING per episode (the scheduler asks again every minute);
            # the serial worker starts every cycle's episode afresh.
            if self.memory.note(ORPHAN_HOLD, orphan_key):
                logger.warning(message)
            else:
                logger.debug(message)
            return HOLD_ORPHAN
        self.memory.clear(ORPHAN_HOLD, orphan_key)
        key = (ancestor.name, destination)
        if self.confirmed(manager, ancestor, destination):
            if self.memory.clear(WAITING, key):
                logger.info(released_message(ancestor.name, destination))
            return None
        snapshots = manager.list_snapshots(dsi)
        if not snapshots:
            if manager.last_list_error is not None:
                logger.debug(
                    f"{dsi.name} -> {destination}: cannot list snapshots "
                    f"({manager.last_list_error}); waiting for {ancestor.name}"
                )
                return HOLD_UNLISTED
            logger.debug(f"No snapshots available to send for {dsi.name}")
            return HOLD_NO_SNAPSHOTS
        if self.memory.note(WAITING, key):
            logger.warning(waiting_message(ancestor.name, destination, self.dependents.get(key, 1)))
        logger.debug(f"{dsi.name} -> {destination}: waiting for {ancestor.name}")
        return HOLD_WAITING


def classify(
    dataset: str, destination: str, done: bool, sender, exit_pending: Callable[[], bool],
) -> PairOutcome:
    """What `backup_dataset`'s result `done` means (the classification the
    worker's `_record_send` has always made). A `False` while an exit reason is
    pending is a stop or reload cutting the work short: STOPPED, neither a
    success nor a failure. Otherwise `False` is FAILED, with the sender's own
    account of why (`last_problem`) when it has one. `retryable` is the
    sender's `last_retryable` for the pair (`False` for a sender without one)."""
    retryable = _retryable(sender, dataset, destination)
    if done:
        return PairOutcome(dataset, destination, PAIR_DONE, retryable=retryable)
    if exit_pending():
        return PairOutcome(dataset, destination, PAIR_STOPPED, retryable=retryable)
    problem = sender.last_problem(dataset, destination)
    return PairOutcome(
        dataset, destination, PAIR_FAILED,
        error=problem if isinstance(problem, str) else NOT_COMPLETED,
        retryable=retryable,
    )


def _retryable(sender, dataset: str, destination: str) -> bool:
    """`sender.last_retryable(dataset, destination)` as an exact bool; `False`
    for anything else (a sender double without one included)."""
    try:
        return sender.last_retryable(dataset, destination) is True
    except Exception:
        return False


def send_pair(
    manager,
    dsi,
    remote_cfg,
    sender,
    should_stop: Callable[[], bool],
    exit_pending: Callable[[], bool],
    *,
    rule: Optional[AncestorRule] = None,
    logger: Optional[logging.Logger] = None,
    label: str = 'remote-backup-worker',
    force_confirm: bool = False,
) -> PairOutcome:
    """Send `dsi` to `remote_cfg.destination` through `sender` (the manager
    that serves that destination, or a `LazySender` that resolves it after
    R-2) if it is due, and say what came of it. Never raises (an exception becomes a FAILED
    outcome, or STOPPED while an exit reason is pending, after one ERROR with
    the traceback naming `label`), and never touches a status writer.

    In order: R-2 (`rule.hold`; `rule=None` is no R-2), the due check, then
    `sender.backup_dataset(dsi, remote_cfg, should_stop=should_stop)`.

    The due check reads the dataset's OWN anchor for the destination
    (`manager.get_own_anchor`): an inherited one would make every child of a
    recursive dataset look sent. A destination whose anchor is fresher than the
    frequency is NOT_DUE; one whose anchor is dated beyond the clock tolerance
    in the future is due (the anchor's age says nothing: `clock_behind` logs
    the ERROR once); an unreadable anchor is None, so the pair is due and the
    negotiate decides.

    `force_confirm=True` skips the NOT_DUE decision: the negotiate decides, and
    nothing left to send settles as up to date (DONE, not retryable, so the
    record is not marked incomplete). The scheduler passes it for EVERY dispatch
    but one (`SendSchedule.wants_anchor_check`): the schedule, not this check,
    decides when a pair sends, and the anchor's age (measured from the snapshot's
    name time) would wrongly skip the slot after a send that ran late (a retry,
    a catch-up, a continuation). The check only guards the re-run of an
    already-attempted slot that a backwards clock step causes. It is also OR-ed
    with `AncestorRule.confirmation_due` (a nearest ancestor of waiting children
    whose copy is not confirmed yet: one negotiate per incarnation). The anchor
    is still READ when forced, so a future-dated one is announced (the ERROR
    above) whatever the caller forces.

    A pair that comes back DONE is noted in the memory's dry-run set (what a
    dry run's R-2 reads, since a dry run confirms no copy).

    `PairOutcome.confirmed` reports what the call learned about the copy
    (`PairOutcome`): read from the memory the rule carries (else the sender's)
    before and after the call. A NOT_DUE or WAITING pair learned nothing."""
    logger = logger if logger is not None else _logger
    destination = remote_cfg.destination
    key = (dsi.name, destination)
    # What the copy's confirmation was before the call, read from the memory the
    # rule carries, else from the RESOLVED sender: a `LazySender` has none, so
    # `_send_pair` fills it in right after resolving it (still after R-2).
    probe = _Probe()
    if rule is not None or not isinstance(sender, LazySender):
        probe.read(rule.memory if rule is not None else _memory_of(sender), key)
    try:
        outcome = _send_pair(
            manager, dsi, remote_cfg, sender, should_stop, exit_pending, rule, logger,
            force_confirm, probe, key,
        )
    except Exception as e:
        logger.error(
            f"{label} error on {dsi.name} -> {destination}: {e}", exc_info=True,
        )
        # An exception while a stop or reload is pending is most likely that stop
        # cutting the work short (a killed child, a closed stream): neither a
        # success nor a failure of the destination.
        outcome = PairOutcome(
            dsi.name, destination,
            PAIR_STOPPED if exit_pending() else PAIR_FAILED,
            error=f"{type(e).__name__}: {e}",
        )
    return replace(
        outcome, confirmed=_confirmation(outcome, manager, probe.memory, key, probe.before)
    )


class _Probe:
    """The sender memory a call reads its `PairOutcome.confirmed` from, and the
    confirmation it held before the call."""

    __slots__ = ('memory', 'before')

    def __init__(self) -> None:
        self.memory: Optional[SendMemory] = None
        self.before: Optional[bool] = None

    def read(self, memory: Optional[SendMemory], key) -> None:
        self.memory = memory
        self.before = memory.noted(COPY_CONFIRMED, key) if memory is not None else None


def _memory_of(sender) -> Optional[SendMemory]:
    """The `SendMemory` of a resolved sender (`ChainSender._memory`), or `None`
    (a `LazySender`, a double without one)."""
    memory = getattr(sender, '_memory', None)
    return memory if isinstance(memory, SendMemory) else None


def _confirmation(outcome: PairOutcome, manager, memory, key, before) -> Optional[bool]:
    """`PairOutcome.confirmed` for a finished call: `None` for a pair that was
    not due or was held (nothing was asked of the sender); `True` for a dry-run
    DONE (a dry run confirms no copy, so `AncestorRule.confirmed` counts it) or
    when the memory holds the confirmation after the call; `False` when it held
    one before and no longer does (the negotiate found no copy); else `None`."""
    if outcome.kind in (PAIR_NOT_DUE, PAIR_WAITING):
        return None
    if outcome.kind == PAIR_DONE and getattr(getattr(manager, 'config', None), 'dry_run', False) is True:
        return True
    if memory is None:
        return None
    if memory.noted(COPY_CONFIRMED, key):
        return True
    return False if before else None


def _send_pair(
    manager, dsi, remote_cfg, sender, should_stop, exit_pending, rule, logger,
    force_confirm=False, probe=None, key=None,
):
    destination = remote_cfg.destination
    if rule is not None:
        verdict = rule.hold(manager, dsi, destination, logger)
        if verdict == HOLD_NO_SNAPSHOTS:
            rule.memory.note(DRY_RUN_SENT, (dsi.name, destination))
            return PairOutcome(dsi.name, destination, PAIR_DONE, detail='no snapshots')
        if verdict is not None:
            return PairOutcome(dsi.name, destination, PAIR_WAITING, detail=verdict)
    if isinstance(sender, LazySender):
        sender = sender.resolve()
        if probe is not None and rule is None:
            probe.read(_memory_of(sender), key)
    freq = (remote_cfg.frequency or dsi.config.frequency).total_seconds()
    confirm = force_confirm or (rule is not None and rule.confirmation_due(manager, dsi, destination))
    # Read even when forced: `clock_behind` announces a future-dated anchor
    # (ERROR once), which a forced dispatch must not hide.
    anchor_name = manager.get_own_anchor(dsi, destination)
    if anchor_name:
        snapshots = manager.list_snapshots(dsi)
        anchor = next((s for s in snapshots if s.name == anchor_name), None)
        # An anchor dated beyond the clock tolerance in the future means the
        # clock is behind it: its age says nothing, so the destination is due
        # (`clock_behind` logs the ERROR once).
        behind = bool(anchor) and manager.clock_behind(dsi, anchor, f"anchor for {destination}")
        if anchor and not behind and not confirm and anchor.age.total_seconds() < freq:
            logger.debug(f"Backup of {dsi.name} to {destination} not due yet")
            return PairOutcome(dsi.name, destination, PAIR_NOT_DUE)
    # `should_stop` is polled before every stream of the chain (never during
    # one): no new stream during a stop or a reload (C3).
    done = sender.backup_dataset(dsi, remote_cfg, should_stop=should_stop)
    if done and rule is not None:
        rule.memory.note(DRY_RUN_SENT, (dsi.name, destination))
    return classify(dsi.name, destination, done, sender, exit_pending)

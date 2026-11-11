"""`zfsbackup.sendpair`: one (dataset, destination) pair's turn as a function (Track B, item B1.1).

`send_pair` is what the remote worker does for ONE pair (R-2, the due check, `backup_dataset`, the
classification, the per-destination exception path). It RETURNS a `PairOutcome` and records nothing:
the caller owns the status writer. Pinned here, with plain doubles (no `zfs`, no transport):

* the outcome table: every kind, its `detail`, the `error` text of a False result with and without
  `last_problem`, the `Type: msg` form of an exception, `stopped` vs `failed` decided by `exit_pending`;
* the due check on the dataset's OWN anchor (fresh, stale, unknown, future-dated, the remote frequency);
* `send_pair` never raises (apart from `BaseException`) and never touches a status writer;
* `LazySender` is resolved only after the R-2 hold;
* `AncestorRule`: `nearest_pair`, `confirmed`, `confirmation_due`, `hold`;
* `DRY_RUN_SENT` is noted on `done`;
* the worker's `_record_outcome` records only `done` and `failed`;
* import hygiene: the three new pure modules load no transport.

Nothing reaches `zfs`; the one subprocess test imports modules in a hermetic interpreter.
"""

import dataclasses
import inspect
import logging
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from _hermetic import hermetic_env
from zfsbackup import health, sendpair
from zfsbackup.workers import RemoteBackupWorker
from zfsbackup.sendmemory import COPY_CONFIRMED, DRY_RUN_SENT, ORPHAN_HOLD, WAITING, SendMemory
from zfsbackup.sendpair import (
    HOLD_NO_SNAPSHOTS, HOLD_ORPHAN, HOLD_UNLISTED, HOLD_WAITING, NOT_COMPLETED, PAIR_DONE,
    PAIR_FAILED, PAIR_NOT_DUE, PAIR_STOPPED, PAIR_WAITING, AncestorRule, LazySender,
    PairOutcome, classify, send_pair,
)

pytestmark = pytest.mark.unit

DEST = 'offsite'
OTHER = 'second'


# ---------------------------------------------------------------------------------------------
# doubles
# ---------------------------------------------------------------------------------------------


def snap(name, age_seconds=0.0):
    return SimpleNamespace(name=name, age=timedelta(seconds=age_seconds))


def make_dsi(name='pool/data', *, frequency=3600, destinations=(DEST,), expanded_from=None):
    return SimpleNamespace(
        name=name,
        expanded_from=expanded_from,
        config=SimpleNamespace(
            frequency=timedelta(seconds=frequency),
            remote=[SimpleNamespace(destination=d) for d in destinations],
        ),
    )


def make_cfg(destination=DEST, frequency=None):
    return SimpleNamespace(
        destination=destination,
        frequency=None if frequency is None else timedelta(seconds=frequency),
    )


class FakeManager:
    """The `DatasetManager` surface `send_pair` and the rule use."""

    def __init__(self, *, anchor=None, snapshots=(), clock_behind=False, dry_run=False,
                 list_error=None):
        self.config = SimpleNamespace(dry_run=dry_run)
        self.anchor = anchor
        self.snapshots = list(snapshots)
        self.behind = clock_behind
        self.last_list_error = list_error
        self.anchor_reads = []
        self.listings = []
        self.clock_checks = []

    def get_own_anchor(self, dsi, destination):
        self.anchor_reads.append((dsi.name, destination))
        return self.anchor

    def list_snapshots(self, dsi):
        self.listings.append(dsi.name)
        return list(self.snapshots)

    def clock_behind(self, dsi, snapshot, what):
        self.clock_checks.append((dsi.name, snapshot.name, what))
        return self.behind


class FakeSender:
    """A sender: `backup_dataset` returns `result` (or raises it), `last_problem` / `last_retryable`
    answer what a real one would about the last call."""

    def __init__(self, result=True, *, problem=None, retryable=False, confirmed=None):
        self.result = result
        self.problem = problem
        self.retryable = retryable
        self.confirmed = confirmed if confirmed is not None else set()
        self.calls = []
        self.problem_asked = []

    def backup_dataset(self, dsi, remote_cfg, should_stop=None):
        self.calls.append((dsi.name, remote_cfg.destination, should_stop))
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result

    def last_problem(self, dataset, destination):
        self.problem_asked.append((dataset, destination))
        return self.problem

    def last_retryable(self, dataset, destination):
        return self.retryable

    def copy_confirmed(self, dataset, destination):
        return (dataset, destination) in self.confirmed


def bare_remote_worker(**attrs):
    """A `RemoteBackupWorker` without `__init__` (no process machinery), with only the cycle state
    `send_pair`'s wiring reads. Nothing here assigns the memory-backed `_waiting_noted` /
    `_dry_run_sent`, which are read-only views of the worker's `SendMemory`."""
    worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
    worker._pairs = {}
    worker._dependents = {}
    for name, value in attrs.items():
        setattr(worker, name, value)
    return worker


NO_EXIT = lambda: False  # noqa: E731
EXIT = lambda: True  # noqa: E731


def run(manager=None, dsi=None, cfg=None, sender=None, *, should_stop=NO_EXIT, exit_pending=NO_EXIT,
        rule=None, **kw):
    return send_pair(
        manager or FakeManager(), dsi or make_dsi(), cfg or make_cfg(),
        sender if sender is not None else FakeSender(),
        should_stop, exit_pending, rule=rule, **kw,
    )


def make_rule(pairs=(), dependents=None, *, sender=None, memory=None):
    """A rule over `pairs` (DatasetInfo doubles by name) whose `sender_for` returns `sender`."""
    memory = memory if memory is not None else SendMemory()
    lookups = []

    def sender_for(manager, destination):
        lookups.append(destination)
        return sender

    for key in getattr(sender, 'confirmed', ()):
        memory.note(COPY_CONFIRMED, key)       # what a sender's acknowledged send leaves in the memory
    rule = AncestorRule({d.name: d for d in pairs}, dependents or {}, memory, sender_for)
    rule.lookups = lookups
    return rule


# ---------------------------------------------------------------------------------------------
# PairOutcome and the kinds
# ---------------------------------------------------------------------------------------------


class TestPairOutcome:
    def test_the_kinds_are_the_documented_strings(self):
        assert (PAIR_DONE, PAIR_NOT_DUE, PAIR_WAITING, PAIR_FAILED, PAIR_STOPPED) == (
            'done', 'not_due', 'waiting', 'failed', 'stopped')

    def test_it_is_a_frozen_dataclass_with_the_eight_fields(self):
        assert dataclasses.is_dataclass(PairOutcome)
        assert [f.name for f in dataclasses.fields(PairOutcome)] == [
            'dataset', 'destination', 'kind', 'error', 'warning', 'retryable', 'detail', 'confirmed']
        outcome = PairOutcome('a', 'b', PAIR_DONE)
        assert (outcome.error, outcome.warning, outcome.retryable, outcome.detail,
                outcome.confirmed) == (None, None, False, None, None)
        with pytest.raises(dataclasses.FrozenInstanceError):
            outcome.kind = PAIR_FAILED

    def test_equal_outcomes_compare_equal_and_hash(self):
        assert PairOutcome('a', 'b', PAIR_DONE) == PairOutcome('a', 'b', PAIR_DONE)
        assert len({PairOutcome('a', 'b', PAIR_DONE), PairOutcome('a', 'b', PAIR_DONE)}) == 1

    def test_the_hold_verdicts_are_the_documented_strings(self):
        assert (HOLD_WAITING, HOLD_ORPHAN, HOLD_UNLISTED) == ('waiting', 'orphan', 'unlisted')
        assert HOLD_NO_SNAPSHOTS == 'no_snapshots'


class TestOutcomeTable:
    """Every kind `send_pair` can come to, in one table: (what is set up) -> (the outcome)."""

    def test_done(self):
        sender = FakeSender(True)
        assert run(sender=sender) == PairOutcome('pool/data', DEST, PAIR_DONE)

    def test_done_carries_the_senders_retryable_flag(self):
        """A True that stopped between steps with work left."""
        outcome = run(sender=FakeSender(True, retryable=True))
        assert (outcome.kind, outcome.retryable) == (PAIR_DONE, True)

    def test_not_due(self):
        manager = FakeManager(anchor='s1', snapshots=[snap('s1', 60)])
        sender = FakeSender(True)
        assert run(manager, sender=sender) == PairOutcome('pool/data', DEST, PAIR_NOT_DUE)
        assert sender.calls == []

    @pytest.mark.parametrize('verdict', [HOLD_WAITING, HOLD_ORPHAN, HOLD_UNLISTED])
    def test_waiting_names_why_in_detail(self, verdict):
        rule = MagicMock()
        rule.hold.return_value = verdict
        sender = FakeSender(True)
        outcome = run(sender=sender, rule=rule)
        assert outcome == PairOutcome('pool/data', DEST, PAIR_WAITING, detail=verdict)
        assert sender.calls == []

    def test_no_snapshots_is_a_done_pair_with_a_detail(self):
        rule = MagicMock()
        rule.hold.return_value = HOLD_NO_SNAPSHOTS
        rule.memory = SendMemory()
        sender = FakeSender(True)
        outcome = run(sender=sender, rule=rule)
        assert outcome == PairOutcome('pool/data', DEST, PAIR_DONE, detail='no snapshots')
        assert sender.calls == []

    def test_failed_without_an_account(self):
        outcome = run(sender=FakeSender(False))
        assert outcome == PairOutcome('pool/data', DEST, PAIR_FAILED, error=NOT_COMPLETED)
        assert NOT_COMPLETED == 'not completed (see the journal)'

    def test_failed_with_the_senders_account(self):
        sender = FakeSender(False, problem='the server refused it')
        outcome = run(sender=sender)
        assert outcome.kind == PAIR_FAILED
        assert outcome.error == 'the server refused it'
        assert sender.problem_asked == [('pool/data', DEST)]

    @pytest.mark.parametrize('problem', [None, 0, b'bytes', ['x'], MagicMock()])
    def test_a_non_string_problem_reads_as_no_account(self, problem):
        assert run(sender=FakeSender(False, problem=problem)).error == NOT_COMPLETED

    def test_failed_carries_retryable(self):
        outcome = run(sender=FakeSender(False, retryable=True))
        assert (outcome.kind, outcome.retryable) == (PAIR_FAILED, True)

    def test_stopped_is_a_false_with_an_exit_pending(self):
        sender = FakeSender(False, problem='ignored')
        outcome = run(sender=sender, exit_pending=EXIT)
        assert outcome == PairOutcome('pool/data', DEST, PAIR_STOPPED)
        assert sender.problem_asked == [], "a stop is not diagnosed"

    def test_stopped_carries_retryable(self):
        outcome = run(sender=FakeSender(False, retryable=True), exit_pending=EXIT)
        assert (outcome.kind, outcome.retryable) == (PAIR_STOPPED, True)

    def test_a_done_pair_is_done_whatever_the_exit_reason(self):
        """A True is a finished chain (a stop between steps included): never `stopped`."""
        assert run(sender=FakeSender(True), exit_pending=EXIT).kind == PAIR_DONE

    def test_exit_pending_is_polled_once_and_after_the_send(self):
        order = []
        sender = FakeSender(False)
        real = sender.backup_dataset
        sender.backup_dataset = lambda *a, **k: (order.append('send'), real(*a, **k))[1]

        def pending():
            order.append('pending')
            return True

        assert run(sender=sender, exit_pending=pending).kind == PAIR_STOPPED
        assert order == ['send', 'pending']
        # (the stop flag the chain polls is the OTHER callable)

    def test_an_exception_is_a_failed_pair_in_type_message_form(self):
        outcome = run(sender=FakeSender(RuntimeError('boom')))
        assert outcome == PairOutcome(
            'pool/data', DEST, PAIR_FAILED, error='RuntimeError: boom')

    def test_an_exception_with_an_exit_pending_is_stopped_but_keeps_its_text(self):
        outcome = run(sender=FakeSender(OSError('pipe closed')), exit_pending=EXIT)
        assert outcome == PairOutcome(
            'pool/data', DEST, PAIR_STOPPED, error='OSError: pipe closed')

    def test_an_exception_is_logged_once_at_error_with_the_label_and_traceback(self, caplog):
        log = logging.getLogger('test.sendpair')
        with caplog.at_level(logging.DEBUG, logger='test.sendpair'):
            run(sender=FakeSender(RuntimeError('boom')), logger=log, label='remote-backup-worker')
        (record,) = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert record.getMessage() == (
            'remote-backup-worker error on pool/data -> offsite: boom')
        assert record.exc_info is not None

    def test_the_default_label_names_the_remote_worker(self, caplog):
        with caplog.at_level(logging.ERROR):
            run(sender=FakeSender(RuntimeError('x')))
        assert any('remote-backup-worker error on pool/data -> offsite: x' in r.getMessage()
                   for r in caplog.records)

    def test_a_base_exception_is_not_swallowed(self):
        with pytest.raises(KeyboardInterrupt):
            run(sender=FakeSender(KeyboardInterrupt()))

    def test_the_outcome_names_the_pair(self):
        outcome = run(dsi=make_dsi('tank/x/y'), cfg=make_cfg(OTHER), sender=FakeSender(True))
        assert (outcome.dataset, outcome.destination) == ('tank/x/y', OTHER)

    def test_a_sender_without_last_retryable_reads_as_not_retryable(self):
        class Old:
            def backup_dataset(self, *a, **k):
                return False

            def last_problem(self, *a):
                return None

        outcome = run(sender=Old())
        assert (outcome.kind, outcome.retryable) == (PAIR_FAILED, False)

    def test_a_raising_last_retryable_reads_as_not_retryable(self):
        sender = FakeSender(True)
        sender.last_retryable = MagicMock(side_effect=RuntimeError('x'))
        assert run(sender=sender).retryable is False

    @pytest.mark.parametrize('value', [1, 'yes', MagicMock()])
    def test_only_an_exact_true_is_retryable(self, value):
        sender = FakeSender(False)
        sender.last_retryable = lambda *a: value
        assert run(sender=sender).retryable is False

    def test_the_exception_path_is_not_retryable(self):
        assert run(sender=FakeSender(RuntimeError('x'), retryable=True)).retryable is False


class TestClassify:
    """`classify` alone: the classification the worker's `_record_send` has always made."""

    def test_done(self):
        assert classify('d', DEST, True, FakeSender(), NO_EXIT) == PairOutcome('d', DEST, PAIR_DONE)

    def test_a_stop_is_not_diagnosed(self):
        sender = FakeSender(problem='p')
        assert classify('d', DEST, False, sender, EXIT).kind == PAIR_STOPPED
        assert sender.problem_asked == []

    def test_a_failure_asks_the_sender_why(self):
        sender = FakeSender(problem='nope')
        outcome = classify('d', DEST, False, sender, NO_EXIT)
        assert (outcome.kind, outcome.error) == (PAIR_FAILED, 'nope')
        assert sender.problem_asked == [('d', DEST)]

    def test_only_a_false_result_polls_exit_pending(self):
        polls = []
        classify('d', DEST, True, FakeSender(), lambda: polls.append(1) or True)
        assert polls == []


# ---------------------------------------------------------------------------------------------
# the due check
# ---------------------------------------------------------------------------------------------


class TestDueCheck:
    def test_no_anchor_is_due(self):
        manager = FakeManager(anchor=None)
        sender = FakeSender(True)
        assert run(manager, sender=sender).kind == PAIR_DONE
        assert manager.anchor_reads == [('pool/data', DEST)]
        assert manager.listings == [], "no anchor, no listing"
        assert len(sender.calls) == 1

    def test_the_anchor_is_the_own_one_of_this_destination(self):
        manager = FakeManager(anchor=None)
        run(manager, dsi=make_dsi('pool/child'), cfg=make_cfg(OTHER))
        assert manager.anchor_reads == [('pool/child', OTHER)]

    def test_a_fresh_anchor_is_not_due(self):
        manager = FakeManager(anchor='s1', snapshots=[snap('s0', 10), snap('s1', 3599)])
        sender = FakeSender(True)
        assert run(manager, sender=sender).kind == PAIR_NOT_DUE
        assert sender.calls == []
        assert manager.clock_checks == [('pool/data', 's1', f'anchor for {DEST}')]

    def test_an_anchor_as_old_as_the_frequency_is_due(self):
        manager = FakeManager(anchor='s1', snapshots=[snap('s1', 3600)])
        assert run(manager, sender=FakeSender(True)).kind == PAIR_DONE

    def test_the_remote_frequency_overrides_the_datasets(self):
        manager = FakeManager(anchor='s1', snapshots=[snap('s1', 7200)])
        # 2 h old: due under the 1 h dataset frequency, not under a 1 d remote one.
        assert run(manager, cfg=make_cfg(frequency=86400)).kind == PAIR_NOT_DUE
        assert run(manager, cfg=make_cfg(frequency=None)).kind == PAIR_DONE

    def test_an_anchor_that_is_not_in_the_listing_is_due(self):
        manager = FakeManager(anchor='gone', snapshots=[snap('s1', 5)])
        assert run(manager, sender=FakeSender(True)).kind == PAIR_DONE

    def test_a_future_dated_anchor_is_due_whatever_its_age(self):
        manager = FakeManager(anchor='s1', snapshots=[snap('s1', 5)], clock_behind=True)
        sender = FakeSender(True)
        assert run(manager, sender=sender).kind == PAIR_DONE
        assert len(sender.calls) == 1

    def test_should_stop_and_exit_pending_are_two_callables(self):
        """`should_stop` goes to the chain (keyword); `exit_pending` only classifies afterwards."""
        sender = FakeSender(True)
        stop, pending = (lambda: False), (lambda: False)
        run(sender=sender, should_stop=stop, exit_pending=pending)
        ((_, _, passed),) = sender.calls
        assert passed is stop and passed is not pending

    def test_backup_dataset_gets_should_stop_as_a_keyword(self):
        sender = MagicMock()
        sender.backup_dataset.return_value = True
        sender.last_retryable.return_value = False
        stop = lambda: False  # noqa: E731
        run(sender=sender, should_stop=stop)
        dsi_arg, cfg_arg = sender.backup_dataset.call_args.args
        assert sender.backup_dataset.call_args.kwargs == {'should_stop': stop}
        assert (dsi_arg.name, cfg_arg.destination) == ('pool/data', DEST)

    def test_a_stale_anchor_is_logged_at_debug_when_not_due(self, caplog):
        manager = FakeManager(anchor='s1', snapshots=[snap('s1', 1)])
        log = logging.getLogger('test.sendpair.debug')
        with caplog.at_level(logging.DEBUG, logger='test.sendpair.debug'):
            run(manager, logger=log)
        assert [r.getMessage() for r in caplog.records] == [
            'Backup of pool/data to offsite not due yet']


class TestSendPairNeverRaises:
    @pytest.mark.parametrize('exc', [
        RuntimeError('x'), OSError(5, 'io'), ValueError('v'), KeyError('k'), AssertionError('a'),
    ])
    def test_a_sender_that_raises(self, exc):
        outcome = run(sender=FakeSender(exc))
        assert outcome.kind == PAIR_FAILED

    def test_a_manager_that_raises_on_the_anchor_read(self):
        manager = FakeManager()
        manager.get_own_anchor = MagicMock(side_effect=RuntimeError('zfs gone'))
        assert run(manager).error == 'RuntimeError: zfs gone'

    def test_a_rule_that_raises(self):
        rule = MagicMock()
        rule.hold.side_effect = RuntimeError('rule')
        outcome = run(rule=rule)
        assert (outcome.kind, outcome.error) == (PAIR_FAILED, 'RuntimeError: rule')

    def test_a_resolver_that_raises(self):
        def resolve():
            raise RuntimeError('no local manager')

        outcome = run(sender=LazySender(resolve))
        assert (outcome.kind, outcome.error) == (PAIR_FAILED, 'RuntimeError: no local manager')

    def test_a_dsi_without_a_frequency_is_a_failed_pair_not_a_raise(self):
        dsi = make_dsi()
        dsi.config.frequency = None
        assert run(dsi=dsi).kind == PAIR_FAILED


class _StatusSpy:
    """A status writer that fails on ANY attribute access."""

    def __getattr__(self, name):
        raise AssertionError(f'send_pair touched the status writer: {name}')


class TestSendPairNeverTouchesAStatusWriter:
    """The caller (the serial worker today, the scheduler thread later) is the only status-writer user."""

    def test_the_signature_has_no_status_parameter(self):
        assert not any('status' in p for p in inspect.signature(send_pair).parameters)
        assert not any('status' in p for p in inspect.signature(AncestorRule.__init__).parameters)

    def test_the_module_never_names_a_status_writer_in_code(self):
        import ast
        names = set()
        for node in ast.walk(ast.parse(inspect.getsource(sendpair))):
            if isinstance(node, ast.Name):
                names.add(node.id)
            elif isinstance(node, ast.Attribute):
                names.add(node.attr)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                names.update(a.name for a in node.names)
                names.add(getattr(node, 'module', None) or '')
        assert not {n for n in names if 'status' in n.lower() or n == 'health'}

    @pytest.mark.parametrize('case', ['done', 'not_due', 'waiting', 'failed', 'stopped', 'raises'])
    def test_no_kind_reaches_a_real_status_writer(self, case, mocker):
        """Every writer entry point raises: whatever `send_pair` returns, it never reached one."""
        for method in ('record', 'flush', 'retain', 'load'):
            mocker.patch.object(health.StatusWriter, method, side_effect=AssertionError(method))
        pair = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        rule = make_rule([pair, child])
        manager = FakeManager()
        sender = FakeSender(True)
        exit_pending = NO_EXIT
        dsi = pair
        if case == 'not_due':
            manager = FakeManager(anchor='s', snapshots=[snap('s', 1)])
        elif case == 'waiting':
            dsi = child
            manager = FakeManager(snapshots=[snap('s')])
        elif case == 'failed':
            sender = FakeSender(False)
        elif case == 'stopped':
            sender, exit_pending = FakeSender(False), EXIT
        elif case == 'raises':
            sender = FakeSender(RuntimeError('x'))
        manager.config.destinations = {}
        outcome = send_pair(manager, dsi, make_cfg(), sender, NO_EXIT, exit_pending, rule=rule)
        assert outcome.kind == {
            'done': PAIR_DONE, 'not_due': PAIR_NOT_DUE, 'waiting': PAIR_WAITING,
            'failed': PAIR_FAILED, 'stopped': PAIR_STOPPED, 'raises': PAIR_FAILED}[case]


# ---------------------------------------------------------------------------------------------
# LazySender: resolved only after R-2
# ---------------------------------------------------------------------------------------------


class TestLazySender:
    def test_it_holds_a_resolver(self):
        resolver = lambda: 7  # noqa: E731
        assert LazySender(resolver).resolve is resolver

    def test_it_is_not_resolved_when_the_pair_is_held(self):
        resolved = []
        pair = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        rule = make_rule([pair, child], {('pool/p', DEST): 1}, sender=FakeSender(confirmed=set()))
        manager = FakeManager(snapshots=[snap('s')])
        lazy = LazySender(lambda: resolved.append(1) or FakeSender(True))
        outcome = run(manager, child, sender=lazy, rule=rule)
        assert outcome == PairOutcome('pool/p/c', DEST, PAIR_WAITING, detail=HOLD_WAITING)
        assert resolved == []

    @pytest.mark.parametrize('listing,error,expected', [
        ([], None, ('done', 'no snapshots')),
        ([], 'zfs list failed', ('waiting', HOLD_UNLISTED)),
    ])
    def test_it_is_not_resolved_for_a_child_without_snapshots_either(self, listing, error, expected):
        resolved = []
        pair = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        rule = make_rule([pair, child], {('pool/p', DEST): 1}, sender=FakeSender())
        manager = FakeManager(snapshots=listing, list_error=error)
        outcome = run(manager, child, sender=LazySender(lambda: resolved.append(1)), rule=rule)
        assert (outcome.kind, outcome.detail) == expected
        assert resolved == []

    def test_a_held_child_with_an_unbuilt_sender_stays_held_silently(self, caplog):
        """The local manager of a destination that is not built is never asked for: no ERROR."""
        def resolve():
            raise RuntimeError('local destination here is not available')

        pair = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        rule = make_rule([pair, child], {('pool/p', DEST): 1}, sender=None)
        with caplog.at_level(logging.ERROR):
            outcome = run(
                FakeManager(snapshots=[snap('s')]), child, sender=LazySender(resolve), rule=rule)
        assert outcome.kind == PAIR_WAITING
        assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []

    def test_it_is_resolved_once_after_the_hold_passes_and_its_result_runs_the_send(self):
        built = FakeSender(True)
        calls = []

        def resolve():
            calls.append(1)
            return built

        outcome = run(sender=LazySender(resolve))
        assert outcome.kind == PAIR_DONE
        assert calls == [1]
        assert len(built.calls) == 1

    def test_the_resolved_sender_answers_last_problem_and_last_retryable(self):
        built = FakeSender(False, problem='from the resolved one', retryable=True)
        outcome = run(sender=LazySender(lambda: built))
        assert (outcome.error, outcome.retryable) == ('from the resolved one', True)

    def test_it_is_resolved_before_the_anchor_is_read(self):
        order = []
        manager = FakeManager()
        manager.get_own_anchor = lambda dsi, d: order.append('anchor') or None
        run(manager, sender=LazySender(lambda: order.append('resolve') or FakeSender(True)))
        assert order == ['resolve', 'anchor']

    def test_a_plain_sender_is_used_as_it_is(self):
        sender = FakeSender(True)
        run(sender=sender)
        assert len(sender.calls) == 1


# ---------------------------------------------------------------------------------------------
# AncestorRule
# ---------------------------------------------------------------------------------------------


class TestNearestPair:
    def test_the_nearest_strict_ancestor_with_a_pair_to_the_destination(self):
        top, mid = make_dsi('pool'), make_dsi('pool/a')
        rule = make_rule([top, mid])
        assert rule.nearest_pair('pool/a/b', DEST) is mid
        assert rule.nearest_pair('pool/a', DEST) is top

    def test_a_dataset_is_not_its_own_ancestor(self):
        rule = make_rule([make_dsi('pool/a')])
        assert rule.nearest_pair('pool/a', DEST) is None

    def test_an_ancestor_with_a_pair_to_another_destination_is_passed_over(self):
        top = make_dsi('pool', destinations=(DEST,))
        mid = make_dsi('pool/a', destinations=(OTHER,))
        rule = make_rule([top, mid])
        assert rule.nearest_pair('pool/a/b', DEST) is top
        assert rule.nearest_pair('pool/a/b', OTHER) is mid

    def test_an_ancestor_that_is_not_a_pair_is_passed_over(self):
        top = make_dsi('pool')
        rule = make_rule([top])                      # pool/a is not in the cycle's list
        assert rule.nearest_pair('pool/a/b', DEST) is top

    def test_an_ancestor_with_no_remote_is_not_a_pair(self):
        rule = make_rule([make_dsi('pool', destinations=())])
        assert rule.nearest_pair('pool/a', DEST) is None

    def test_none_when_there_is_no_ancestor(self):
        assert make_rule([]).nearest_pair('pool/a/b', DEST) is None
        assert make_rule([make_dsi('pool')]).nearest_pair('pool', DEST) is None

    def test_a_name_without_a_slash_has_no_ancestor(self):
        assert make_rule([make_dsi('pool')]).nearest_pair('other', DEST) is None

    def test_a_sibling_prefix_is_not_an_ancestor(self):
        """`pool/ab` is not below `pool/a`."""
        rule = make_rule([make_dsi('pool/a')])
        assert rule.nearest_pair('pool/ab', DEST) is None


class TestConfirmed:
    """`AncestorRule.confirmed` reads `COPY_CONFIRMED` from the memory the senders write; it needs
    no sender."""

    def test_the_memorys_confirmation_decides(self):
        pair = make_dsi('pool/p')
        memory = SendMemory()
        rule = make_rule([pair], memory=memory)
        assert rule.confirmed(FakeManager(), pair, DEST) is False
        memory.note(COPY_CONFIRMED, ('pool/p', DEST))
        assert rule.confirmed(FakeManager(), pair, DEST) is True
        assert rule.confirmed(FakeManager(), pair, OTHER) is False, 'per destination'

    def test_it_needs_no_sender(self):
        pair = make_dsi('pool/p')
        memory = SendMemory()
        memory.note(COPY_CONFIRMED, ('pool/p', DEST))
        rule = make_rule([pair], sender=None, memory=memory)
        assert rule.confirmed(FakeManager(), pair, DEST) is True
        assert rule.lookups == [], 'no sender is looked up'

    def test_it_is_not_confirmed_without_a_confirmation(self):
        pair = make_dsi('pool/p')
        assert make_rule([pair], sender=None).confirmed(FakeManager(), pair, DEST) is False

    def test_a_cleared_confirmation_is_not_confirmed_any_more(self):
        pair = make_dsi('pool/p')
        memory = SendMemory()
        memory.note(COPY_CONFIRMED, ('pool/p', DEST))
        rule = make_rule([pair], memory=memory)
        memory.clear(COPY_CONFIRMED, ('pool/p', DEST))
        assert rule.confirmed(FakeManager(), pair, DEST) is False

    def test_a_dry_run_passes_on_the_dry_run_set(self):
        pair = make_dsi('pool/p')
        memory = SendMemory()
        rule = make_rule([pair], sender=FakeSender(), memory=memory)
        dry = FakeManager(dry_run=True)
        assert rule.confirmed(dry, pair, DEST) is False
        memory.note(DRY_RUN_SENT, ('pool/p', DEST))
        assert rule.confirmed(dry, pair, DEST) is True
        assert rule.confirmed(dry, pair, OTHER) is False, "per destination"

    def test_outside_a_dry_run_the_dry_run_set_is_never_consulted(self):
        """No stream confirms nothing in a dry run, but a real run must not trust the set."""
        pair = make_dsi('pool/p')
        memory = SendMemory()
        memory.note(DRY_RUN_SENT, ('pool/p', DEST))
        rule = make_rule([pair], sender=FakeSender(), memory=memory)
        assert rule.confirmed(FakeManager(dry_run=False), pair, DEST) is False

    def test_a_dry_run_with_a_confirmation_in_the_memory_is_confirmed_too(self):
        pair = make_dsi('pool/p')
        rule = make_rule([pair], sender=FakeSender(confirmed={('pool/p', DEST)}))
        assert rule.confirmed(FakeManager(dry_run=True), pair, DEST) is True


class TestConfirmationDue:
    def test_due_when_a_dependent_waits_and_the_copy_is_not_confirmed(self):
        pair = make_dsi('pool/p')
        rule = make_rule([pair], {('pool/p', DEST): 2}, sender=FakeSender())
        assert rule.confirmation_due(FakeManager(), pair, DEST) is True

    def test_not_due_without_dependents(self):
        pair = make_dsi('pool/p')
        sender = FakeSender()
        rule = make_rule([pair], {}, sender=sender)
        assert rule.confirmation_due(FakeManager(), pair, DEST) is False

    def test_not_due_with_a_zero_count(self):
        pair = make_dsi('pool/p')
        rule = make_rule([pair], {('pool/p', DEST): 0}, sender=FakeSender())
        assert rule.confirmation_due(FakeManager(), pair, DEST) is False

    def test_not_due_once_confirmed(self):
        pair = make_dsi('pool/p')
        rule = make_rule(
            [pair], {('pool/p', DEST): 1}, sender=FakeSender(confirmed={('pool/p', DEST)}))
        assert rule.confirmation_due(FakeManager(), pair, DEST) is False

    def test_the_count_is_per_destination(self):
        pair = make_dsi('pool/p', destinations=(DEST, OTHER))
        rule = make_rule([pair], {('pool/p', DEST): 1}, sender=FakeSender())
        assert rule.confirmation_due(FakeManager(), pair, DEST) is True
        assert rule.confirmation_due(FakeManager(), pair, OTHER) is False

    def test_a_due_confirmation_skips_the_anchor_in_send_pair(self):
        """A fresh anchor would make the ancestor `not_due`; with waiting children it negotiates."""
        pair = make_dsi('pool/p')
        sender = FakeSender(True)
        rule = make_rule([pair], {('pool/p', DEST): 1}, sender=sender)
        manager = FakeManager(anchor='s', snapshots=[snap('s', 1)])
        outcome = run(manager, pair, sender=sender, rule=rule)
        assert outcome.kind == PAIR_DONE
        assert manager.anchor_reads == [], "the anchor is not even read"
        assert len(sender.calls) == 1

    def test_a_confirmed_ancestor_is_due_by_its_anchor_again(self):
        pair = make_dsi('pool/p')
        sender = FakeSender(True, confirmed={('pool/p', DEST)})
        rule = make_rule([pair], {('pool/p', DEST): 1}, sender=sender)
        manager = FakeManager(anchor='s', snapshots=[snap('s', 1)])
        assert run(manager, pair, sender=sender, rule=rule).kind == PAIR_NOT_DUE
        assert manager.anchor_reads == [('pool/p', DEST)]

    def test_without_a_rule_the_anchor_alone_decides(self):
        manager = FakeManager(anchor='s', snapshots=[snap('s', 1)])
        assert run(manager, make_dsi('pool/p'), rule=None).kind == PAIR_NOT_DUE


class TestHold:
    LOG = logging.getLogger('test.sendpair.hold')

    def rule_for(self, *, confirmed=False, dependents=1, sender=True, memory=None, pairs=None):
        pair = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        sender_obj = FakeSender(confirmed={('pool/p', DEST)} if confirmed else set()) if sender else None
        rule = make_rule(
            pairs if pairs is not None else [pair, child],
            {('pool/p', DEST): dependents}, sender=sender_obj, memory=memory)
        return rule, pair, child

    def test_a_configured_dataset_is_never_held(self):
        rule, pair, _ = self.rule_for()
        manager = FakeManager(snapshots=[snap('s')])
        assert rule.hold(manager, pair, DEST, self.LOG) is None
        assert manager.listings == []

    def test_an_expanded_child_waits_for_an_unconfirmed_ancestor(self):
        rule, _, child = self.rule_for(confirmed=False)
        assert rule.hold(FakeManager(snapshots=[snap('s')]), child, DEST, self.LOG) == HOLD_WAITING

    def test_a_confirmed_ancestor_releases_the_child(self):
        rule, _, child = self.rule_for(confirmed=True)
        manager = FakeManager(snapshots=[snap('s')])
        assert rule.hold(manager, child, DEST, self.LOG) is None
        assert manager.listings == [], "a released child is not listed by the rule"

    def test_the_nearest_ancestor_pair_is_the_one_asked(self):
        top = make_dsi('pool')
        mid = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool')
        # `pool` is confirmed, `pool/p` (the nearest pair) is not: the child waits.
        sender = FakeSender(confirmed={('pool', DEST)})
        rule = make_rule([top, mid, child], {('pool/p', DEST): 1}, sender=sender)
        assert rule.hold(FakeManager(snapshots=[snap('s')]), child, DEST, self.LOG) == HOLD_WAITING

    def test_without_any_ancestor_pair_the_parent_it_was_expanded_from_is_asked(self):
        parent = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        # `pool/p` has no pair to `OTHER`, so `nearest_pair` finds nothing: fall back to it by name.
        rule = make_rule([parent, child], {}, sender=FakeSender(confirmed={('pool/p', OTHER)}))
        assert rule.nearest_pair('pool/p/c', OTHER) is None
        assert rule.hold(FakeManager(snapshots=[snap('s')]), child, OTHER, self.LOG) is None

    def test_fails_closed_with_no_ancestor_at_all(self, caplog):
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        rule = make_rule([child], {})
        with caplog.at_level(logging.WARNING, logger='test.sendpair.hold'):
            verdict = rule.hold(FakeManager(snapshots=[snap('s')]), child, DEST, self.LOG)
        assert verdict == HOLD_ORPHAN
        (record,) = caplog.records
        assert record.levelno == logging.WARNING
        assert record.getMessage() == (
            "pool/p/c -> offsite: cannot find pool/p, which it was expanded from; not sent")

    def test_the_orphan_warning_is_logged_once_per_child_and_destination(self, caplog):
        child = make_dsi('pool/p/c', expanded_from='pool/p', destinations=(DEST, OTHER))
        rule = make_rule([child], {})
        with caplog.at_level(logging.DEBUG, logger='test.sendpair.hold'):
            for _ in range(3):
                assert rule.hold(FakeManager(), child, DEST, self.LOG) == HOLD_ORPHAN
            assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1
            assert len([r for r in caplog.records if r.levelno == logging.DEBUG]) == 2
            rule.hold(FakeManager(), child, OTHER, self.LOG)            # another destination: new episode
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 2
        assert rule.memory.noted(ORPHAN_HOLD, ('pool/p/c', DEST))

    def test_another_child_of_the_same_missing_parent_warns_on_its_own(self, caplog):
        a = make_dsi('pool/p/a', expanded_from='pool/p')
        b = make_dsi('pool/p/b', expanded_from='pool/p')
        rule = make_rule([a, b], {})
        with caplog.at_level(logging.WARNING, logger='test.sendpair.hold'):
            rule.hold(FakeManager(), a, DEST, self.LOG)
            rule.hold(FakeManager(), b, DEST, self.LOG)
            rule.hold(FakeManager(), a, DEST, self.LOG)
        assert len(caplog.records) == 2

    def test_an_orphan_that_is_found_again_warns_again_when_it_is_lost_again(self, caplog):
        parent = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        rule = make_rule([child], {})
        with caplog.at_level(logging.WARNING, logger='test.sendpair.hold'):
            rule.hold(FakeManager(), child, DEST, self.LOG)
            rule.pairs = {'pool/p': parent, 'pool/p/c': child}        # the parent is back in the list
            rule.hold(FakeManager(snapshots=[snap('s')]), child, DEST, self.LOG)
            assert rule.memory.noted(ORPHAN_HOLD, ('pool/p/c', DEST)) is False
            rule.pairs = {'pool/p/c': child}
            rule.hold(FakeManager(), child, DEST, self.LOG)
        assert len([r for r in caplog.records if 'cannot find' in r.getMessage()]) == 2

    def test_a_child_without_snapshots_is_done_not_held(self):
        rule, _, child = self.rule_for()
        manager = FakeManager(snapshots=[])
        assert rule.hold(manager, child, DEST, self.LOG) == HOLD_NO_SNAPSHOTS

    def test_a_failed_listing_holds_silently(self, caplog):
        rule, _, child = self.rule_for()
        manager = FakeManager(snapshots=[], list_error='cannot open')
        with caplog.at_level(logging.DEBUG, logger='test.sendpair.hold'):
            assert rule.hold(manager, child, DEST, self.LOG) == HOLD_UNLISTED
        assert [r.levelno for r in caplog.records] == [logging.DEBUG]
        assert 'cannot open' in caplog.records[0].getMessage()
        assert rule.memory.noted(WAITING, ('pool/p', DEST)) is False

    def test_one_warning_per_episode_then_debug(self, caplog):
        rule, _, child = self.rule_for(dependents=3)
        manager = FakeManager(snapshots=[snap('s')])
        with caplog.at_level(logging.DEBUG, logger='test.sendpair.hold'):
            for _ in range(3):
                assert rule.hold(manager, child, DEST, self.LOG) == HOLD_WAITING
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert warnings[0].getMessage() == (
            "3 descendants of pool/p wait until its copy at offsite is confirmed in this run (a "
            "negotiate that finds the copy, or an acknowledged send); they are not sent until then")
        assert len([r for r in caplog.records if r.levelno == logging.DEBUG]) == 3
        assert rule.memory.noted(WAITING, ('pool/p', DEST)) is True

    def test_a_single_dependent_is_singular(self, caplog):
        rule, _, child = self.rule_for(dependents=1)
        with caplog.at_level(logging.WARNING, logger='test.sendpair.hold'):
            rule.hold(FakeManager(snapshots=[snap('s')]), child, DEST, self.LOG)
        assert caplog.records[0].getMessage().startswith('1 descendant of pool/p wait')

    def test_the_episode_ends_with_one_info_when_the_ancestor_is_confirmed(self, caplog):
        memory = SendMemory()
        rule, pair, child = self.rule_for(memory=memory)
        manager = FakeManager(snapshots=[snap('s')])
        with caplog.at_level(logging.INFO, logger='test.sendpair.hold'):
            rule.hold(manager, child, DEST, self.LOG)
            memory.note(COPY_CONFIRMED, ('pool/p', DEST))          # the ancestor's send is acknowledged
            assert rule.hold(manager, child, DEST, self.LOG) is None
            assert rule.hold(manager, child, DEST, self.LOG) is None
        infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert infos == ['pool/p is confirmed at offsite: its descendants follow']
        assert memory.noted(WAITING, ('pool/p', DEST)) is False

    def test_a_new_episode_warns_again(self, caplog):
        memory = SendMemory()
        rule, _, child = self.rule_for(memory=memory)
        manager = FakeManager(snapshots=[snap('s')])
        with caplog.at_level(logging.WARNING, logger='test.sendpair.hold'):
            rule.hold(manager, child, DEST, self.LOG)
            memory.clear(WAITING, ('pool/p', DEST))
            rule.hold(manager, child, DEST, self.LOG)
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 2

    def test_no_sender_means_waiting_and_not_an_exception(self):
        rule, _, child = self.rule_for(sender=False)
        assert rule.hold(FakeManager(snapshots=[snap('s')]), child, DEST, self.LOG) == HOLD_WAITING

    def test_the_latch_is_per_ancestor_and_destination(self, caplog):
        memory = SendMemory()
        pair = make_dsi('pool/p', destinations=(DEST, OTHER))
        child = make_dsi('pool/p/c', expanded_from='pool/p', destinations=(DEST, OTHER))
        rule = make_rule([pair, child], {('pool/p', DEST): 1, ('pool/p', OTHER): 1},
                         sender=FakeSender(), memory=memory)
        manager = FakeManager(snapshots=[snap('s')])
        with caplog.at_level(logging.WARNING, logger='test.sendpair.hold'):
            rule.hold(manager, child, DEST, self.LOG)
            rule.hold(manager, child, OTHER, self.LOG)
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 2

    def test_rule_state_is_not_changed_by_use(self):
        """One rule serves any number of threads: it only READS `pairs` and `dependents`."""
        rule, _, child = self.rule_for()
        pairs, dependents = dict(rule.pairs), dict(rule.dependents)
        rule.hold(FakeManager(snapshots=[snap('s')]), child, DEST, self.LOG)
        rule.hold(FakeManager(snapshots=[]), child, DEST, self.LOG)
        assert rule.pairs == pairs and rule.dependents == dependents


# ---------------------------------------------------------------------------------------------
# DRY_RUN_SENT
# ---------------------------------------------------------------------------------------------


class TestDryRunSent:
    def test_noted_on_done(self):
        memory = SendMemory()
        rule = make_rule([make_dsi()], sender=FakeSender(), memory=memory)
        assert run(sender=FakeSender(True), rule=rule).kind == PAIR_DONE
        assert memory.noted(DRY_RUN_SENT, ('pool/data', DEST)) is True

    def test_noted_for_the_no_snapshots_done(self):
        memory = SendMemory()
        rule = MagicMock()
        rule.hold.return_value = HOLD_NO_SNAPSHOTS
        rule.memory = memory
        run(rule=rule)
        assert memory.noted(DRY_RUN_SENT, ('pool/data', DEST)) is True

    @pytest.mark.parametrize('case', ['failed', 'stopped', 'not_due', 'waiting', 'raises'])
    def test_not_noted_otherwise(self, case):
        memory = SendMemory()
        rule = make_rule([make_dsi()], sender=FakeSender(), memory=memory)
        manager, sender, pending = FakeManager(), FakeSender(False), NO_EXIT
        if case == 'stopped':
            pending = EXIT
        elif case == 'not_due':
            manager = FakeManager(anchor='s', snapshots=[snap('s', 1)])
        elif case == 'waiting':
            rule = MagicMock()
            rule.hold.return_value = HOLD_WAITING
            rule.memory = memory
        elif case == 'raises':
            sender = FakeSender(RuntimeError('x'))
        run(manager, sender=sender, exit_pending=pending, rule=rule)
        assert memory.noted(DRY_RUN_SENT, ('pool/data', DEST)) is False

    def test_it_is_keyed_by_the_destination(self):
        memory = SendMemory()
        rule = make_rule([make_dsi(destinations=(DEST, OTHER))], sender=FakeSender(), memory=memory)
        run(cfg=make_cfg(OTHER), sender=FakeSender(True), rule=rule)
        assert memory.noted(DRY_RUN_SENT, ('pool/data', OTHER)) is True
        assert memory.noted(DRY_RUN_SENT, ('pool/data', DEST)) is False

    def test_a_dry_run_child_follows_its_ancestors_done_pair(self):
        """What the set is for: a dry run confirms no copy, so the child reads the set."""
        memory = SendMemory()
        pair = make_dsi('pool/p')
        child = make_dsi('pool/p/c', expanded_from='pool/p')
        sender = FakeSender(True)
        rule = make_rule([pair, child], {('pool/p', DEST): 1}, sender=sender, memory=memory)
        manager = FakeManager(dry_run=True, snapshots=[snap('s')])
        assert run(manager, child, sender=sender, rule=rule).kind == PAIR_WAITING
        assert run(manager, pair, sender=sender, rule=rule).kind == PAIR_DONE
        assert run(manager, child, sender=sender, rule=rule).kind == PAIR_DONE

    def test_without_a_rule_nothing_is_noted(self):
        assert run(rule=None).kind == PAIR_DONE        # no memory to note it in; must not raise


# ---------------------------------------------------------------------------------------------
# PairOutcome.confirmed and force_confirm
# ---------------------------------------------------------------------------------------------


class MemorySender(FakeSender):
    """A sender with a memory, as a real `ChainSender` has: `backup_dataset` may confirm the copy
    (`confirms`) or find it missing (`forgets`) before it returns `result`."""

    def __init__(self, memory, result=True, *, confirms=False, forgets=False, **kw):
        super().__init__(result, **kw)
        self._memory = memory
        self.confirms, self.forgets = confirms, forgets

    def backup_dataset(self, dsi, remote_cfg, should_stop=None):
        key = (dsi.name, remote_cfg.destination)
        if self.confirms:
            self._memory.note(COPY_CONFIRMED, key)
        if self.forgets:
            self._memory.clear(COPY_CONFIRMED, key)
        return super().backup_dataset(dsi, remote_cfg, should_stop)


class TestConfirmedOutcome:
    KEY = ('pool/data', DEST)

    def test_true_when_this_call_confirmed_the_copy(self):
        memory = SendMemory()
        rule = make_rule([make_dsi()], memory=memory)
        outcome = run(sender=MemorySender(memory, True, confirms=True), rule=rule)
        assert (outcome.kind, outcome.confirmed) == (PAIR_DONE, True)

    def test_true_for_a_dry_run_done(self):
        outcome = run(FakeManager(dry_run=True), sender=FakeSender(True))
        assert (outcome.kind, outcome.confirmed) == (PAIR_DONE, True)

    def test_a_dry_run_that_failed_is_not_confirmed(self):
        outcome = run(FakeManager(dry_run=True), sender=FakeSender(False))
        assert (outcome.kind, outcome.confirmed) == (PAIR_FAILED, None)

    def test_false_when_this_call_learned_there_is_no_copy(self):
        memory = SendMemory()
        memory.note(COPY_CONFIRMED, self.KEY)
        rule = make_rule([make_dsi()], memory=memory)
        outcome = run(sender=MemorySender(memory, False, forgets=True), rule=rule)
        assert (outcome.kind, outcome.confirmed) == (PAIR_FAILED, False)

    def test_false_also_for_a_done_pair_whose_copy_vanished(self):
        memory = SendMemory()
        memory.note(COPY_CONFIRMED, self.KEY)
        rule = make_rule([make_dsi()], memory=memory)
        outcome = run(sender=MemorySender(memory, True, forgets=True), rule=rule)
        assert (outcome.kind, outcome.confirmed) == (PAIR_DONE, False)

    def test_none_when_nothing_was_learned(self):
        memory = SendMemory()
        rule = make_rule([make_dsi()], memory=memory)
        outcome = run(sender=MemorySender(memory, False), rule=rule)
        assert (outcome.kind, outcome.confirmed) == (PAIR_FAILED, None)

    def test_a_confirmation_that_stays_is_true(self):
        memory = SendMemory()
        memory.note(COPY_CONFIRMED, self.KEY)
        rule = make_rule([make_dsi()], memory=memory)
        outcome = run(sender=MemorySender(memory, True), rule=rule)
        assert outcome.confirmed is True

    @pytest.mark.parametrize('case', ['not_due', 'waiting'])
    def test_none_for_a_pair_nothing_was_asked_of(self, case):
        memory = SendMemory()
        memory.note(COPY_CONFIRMED, self.KEY)          # even a confirmed pair learns nothing here
        if case == 'not_due':
            rule = make_rule([make_dsi()], memory=memory)
            manager = FakeManager(anchor='s', snapshots=[snap('s', 1)])
            outcome = run(manager, sender=MemorySender(memory), rule=rule)
        else:
            rule = MagicMock()
            rule.hold.return_value = HOLD_WAITING
            rule.memory = memory
            outcome = run(sender=MemorySender(memory), rule=rule)
        assert outcome.kind in (PAIR_NOT_DUE, PAIR_WAITING) and outcome.confirmed is None

    def test_none_without_a_memory_to_read(self):
        assert run(sender=FakeSender(True)).confirmed is None

    def test_the_senders_memory_is_read_when_there_is_no_rule(self):
        memory = SendMemory()
        outcome = run(sender=MemorySender(memory, True, confirms=True), rule=None)
        assert outcome.confirmed is True

    def test_a_lazy_sender_without_a_rule_reports_the_confirmation_of_the_resolved_sender(self):
        """No rule and a `LazySender`: the memory is the resolved sender's, read right after
        `resolve()` (before it is asked to send). It was always `None` before."""
        memory = SendMemory()
        sender = LazySender(lambda: MemorySender(memory, True, confirms=True))
        outcome = run(sender=sender, rule=None)
        assert (outcome.kind, outcome.confirmed) == (PAIR_DONE, True)

    def test_a_lazy_sender_without_a_rule_reports_a_vanished_copy_as_false(self):
        """The resolved sender's memory held the note before the call and does not after it."""
        memory = SendMemory()
        memory.note(COPY_CONFIRMED, self.KEY)
        sender = LazySender(lambda: MemorySender(memory, True, forgets=True))
        outcome = run(sender=sender, rule=None)
        assert (outcome.kind, outcome.confirmed) == (PAIR_DONE, False)

    def test_a_lazy_sender_without_a_rule_that_learned_nothing_is_none(self):
        memory = SendMemory()
        sender = LazySender(lambda: MemorySender(memory, True))
        assert run(sender=sender, rule=None).confirmed is None

    def test_a_lazy_sender_without_a_rule_and_a_confirmed_copy_that_stays_is_true(self):
        memory = SendMemory()
        memory.note(COPY_CONFIRMED, self.KEY)
        sender = LazySender(lambda: MemorySender(memory, True))
        assert run(sender=sender, rule=None).confirmed is True

    def test_a_lazy_sender_whose_resolve_raises_has_no_confirmation(self):
        def resolve():
            raise RuntimeError('no local manager')

        outcome = run(sender=LazySender(resolve), rule=None)
        assert (outcome.kind, outcome.error, outcome.confirmed) == (
            PAIR_FAILED, 'RuntimeError: no local manager', None)

    def test_a_lazy_sender_with_a_rule_still_reads_the_rules_memory(self):
        """The rule's memory wins; the resolved sender's own is not consulted."""
        rule_memory, sender_memory = SendMemory(), SendMemory()
        rule = make_rule([make_dsi()], memory=rule_memory)
        sender = LazySender(lambda: MemorySender(sender_memory, True, confirms=True))
        outcome = run(sender=sender, rule=rule)
        assert (outcome.kind, outcome.confirmed) == (PAIR_DONE, None)

    def test_an_exception_after_the_copy_was_confirmed_still_reports_it(self):
        memory = SendMemory()
        rule = make_rule([make_dsi()], memory=memory)
        sender = MemorySender(memory, RuntimeError('boom'), confirms=True)
        outcome = run(sender=sender, rule=rule)
        assert (outcome.kind, outcome.confirmed) == (PAIR_FAILED, True)

    def test_the_no_snapshots_done_pair_learns_nothing_unless_confirmed(self):
        memory = SendMemory()
        rule = MagicMock()
        rule.hold.return_value = HOLD_NO_SNAPSHOTS
        rule.memory = memory
        outcome = run(rule=rule)
        assert (outcome.kind, outcome.detail, outcome.confirmed) == (PAIR_DONE, 'no snapshots', None)


class TestForceConfirm:
    def test_the_parameter_is_keyword_only_and_off_by_default(self):
        parameter = inspect.signature(send_pair).parameters['force_confirm']
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is False

    def test_a_fresh_anchor_does_not_make_the_pair_not_due(self):
        manager = FakeManager(anchor='s1', snapshots=[snap('s1', 5)])
        sender = FakeSender(True)
        outcome = run(manager, sender=sender, force_confirm=True)
        assert outcome.kind == PAIR_DONE and len(sender.calls) == 1
        assert manager.anchor_reads == [], 'the anchor is not even read'

    def test_without_it_the_same_pair_is_not_due(self):
        manager = FakeManager(anchor='s1', snapshots=[snap('s1', 5)])
        assert run(manager, sender=FakeSender(True)).kind == PAIR_NOT_DUE

    def test_it_does_not_skip_the_parent_first_rule(self):
        rule = MagicMock()
        rule.hold.return_value = HOLD_WAITING
        sender = FakeSender(True)
        outcome = run(sender=sender, rule=rule, force_confirm=True)
        assert outcome.kind == PAIR_WAITING and sender.calls == []

    def test_it_is_or_ed_with_the_rules_own_confirmation(self):
        pair = make_dsi('pool/p')
        sender = FakeSender(True)
        rule = make_rule([pair], {('pool/p', DEST): 1}, sender=sender)
        manager = FakeManager(anchor='s', snapshots=[snap('s', 1)])
        assert run(manager, pair, sender=sender, rule=rule, force_confirm=True).kind == PAIR_DONE
        assert run(manager, pair, sender=sender, rule=rule, force_confirm=False).kind == PAIR_DONE


# ---------------------------------------------------------------------------------------------
# the worker's _record_outcome (it records only done and failed)
# ---------------------------------------------------------------------------------------------


class TestRecordOutcome:
    @staticmethod
    def worker():
        status = MagicMock()
        return bare_remote_worker(_status=status), status

    def test_done_is_a_recorded_success_and_flushed(self):
        worker, status = self.worker()
        worker._record_outcome(PairOutcome('pool/data', DEST, PAIR_DONE))
        status.record.assert_called_once_with(
            health.KIND_SEND, 'pool/data', DEST, ok=True, error=None, detail=None)
        status.flush.assert_called_once_with(force=True, heartbeat=False)

    def test_a_done_with_a_detail_is_still_a_plain_success(self):
        worker, status = self.worker()
        worker._record_outcome(PairOutcome('pool/data', DEST, PAIR_DONE, detail='no snapshots'))
        status.record.assert_called_once_with(
            health.KIND_SEND, 'pool/data', DEST, ok=True, error=None, detail=None)

    def test_failed_is_a_recorded_failure_with_the_error_text(self):
        worker, status = self.worker()
        worker._record_outcome(PairOutcome('pool/data', DEST, PAIR_FAILED, error='why'))
        status.record.assert_called_once_with(
            health.KIND_SEND, 'pool/data', DEST, ok=False, error='why', detail=None)
        status.flush.assert_called_once_with(force=True, heartbeat=False)

    def test_a_warning_of_a_success_is_forwarded(self):
        worker, status = self.worker()
        worker._record_outcome(PairOutcome('pool/data', DEST, PAIR_DONE, warning='left out: x'))
        status.record.assert_called_once_with(
            health.KIND_SEND, 'pool/data', DEST, ok=True, error=None, detail=None,
            warning='left out: x')

    def test_a_warning_of_a_failure_is_forwarded_too(self):
        worker, status = self.worker()
        worker._record_outcome(PairOutcome('pool/data', DEST, PAIR_FAILED, error='why', warning='w'))
        status.record.assert_called_once_with(
            health.KIND_SEND, 'pool/data', DEST, ok=False, error='why', detail=None, warning='w')

    def test_the_retryable_flag_is_not_part_of_the_record(self):
        worker, status = self.worker()
        worker._record_outcome(
            PairOutcome('pool/data', DEST, PAIR_FAILED, error='why', retryable=True))
        assert 'retry' not in str(status.record.call_args)

    @pytest.mark.parametrize('kind', [PAIR_NOT_DUE, PAIR_WAITING, PAIR_STOPPED])
    def test_every_other_kind_records_and_flushes_nothing(self, kind):
        worker, status = self.worker()
        worker._record_outcome(PairOutcome('pool/data', DEST, kind, error='ignored', detail='x'))
        status.record.assert_not_called()
        status.flush.assert_not_called()

    def test_an_unknown_kind_records_nothing(self):
        worker, status = self.worker()
        worker._record_outcome(PairOutcome('pool/data', DEST, 'mystery'))
        status.record.assert_not_called()

    def test_without_a_status_writer_nothing_happens(self):
        worker = bare_remote_worker(_status=None)
        worker._record_outcome(PairOutcome('pool/data', DEST, PAIR_FAILED, error='x'))
        worker._record_outcome(PairOutcome('pool/data', DEST, PAIR_DONE))

    def test_the_whole_path_through_the_scheduler_records_only_done_and_failed(self, monkeypatch):
        """The scheduler loop -> `send_pair` -> `_record_outcome`, kind by kind: a success and a failure are
        recorded, a False cut short by a stop is not."""
        from _sendloop_support import LoopRig
        from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig
        config = BackupConfig(
            datasets=[DatasetConfig(name='pool/data', remote=[RemoteDatasetConfig(destination=DEST)])],
            destinations={DEST: Destination(url='https://h.example:8443')},
        )
        for sender_result, pending, expected in [
            (True, False, [True]), (False, False, [False]), (False, True, []),
        ]:
            rig = LoopRig(config, monkeypatch)
            status = MagicMock()
            rig.worker._open_status = lambda cfg, logger, status=status: status
            rig.remote.backup_dataset.side_effect = (
                lambda dsi, cfg, should_stop=None, r=sender_result, p=pending:
                (rig.stop.set() if p else None) or r
            )
            rig.remote.last_problem.return_value = 'p'
            rig.run()
            assert [c.kwargs['ok'] for c in status.record.call_args_list] == expected

    def test_a_held_child_through_the_scheduler_records_nothing_and_is_never_sent(self, monkeypatch):
        """The schedule (R-2) holds a child behind a parent that did not confirm: no sender, no record
        for the child; the parent's own failure is recorded."""
        from _sendloop_support import LoopRig
        from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig
        config = BackupConfig(
            datasets=[DatasetConfig(name=n, remote=[RemoteDatasetConfig(destination=DEST)])
                      for n in ('pool/p', 'pool/p/c')],
            destinations={DEST: Destination(url='https://h.example:8443')},
        )
        rig = LoopRig(config, monkeypatch)
        status = MagicMock()
        rig.worker._open_status = lambda cfg, logger: status
        rig.remote.outcomes[('pool/p', DEST)] = False
        rig.run()
        assert rig.remote.attempts == [('pool/p', DEST)]
        assert [(c.args[1], c.kwargs['ok']) for c in status.record.call_args_list] == [('pool/p', False)]


class TestWorkerWiring:
    """The rule a sender uses is built by the scheduler loop (`SendLoop._make_rule`): its wiring is in
    `test_zfsbackup_sendloop.py`."""

    def test_the_waiting_and_dry_run_names_are_read_only_views_of_the_memory(self):
        worker = RemoteBackupWorker(MagicMock(), MagicMock())
        for name in ('_waiting_noted', '_dry_run_sent'):
            with pytest.raises(AttributeError):
                setattr(worker, name, set())

    def test_the_worker_waiting_and_dry_run_sets_are_the_memorys_live_latches(self):
        from zfsbackup.workers import RemoteBackupWorker
        worker = RemoteBackupWorker(MagicMock(), MagicMock())
        assert worker._waiting_noted is worker._memory.latch_view(WAITING)
        assert worker._dry_run_sent is worker._memory.latch_view(DRY_RUN_SENT)

    def test_a_worker_built_without_init_makes_its_memory_on_first_use(self):
        from zfsbackup.workers import RemoteBackupWorker
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        first = worker._mem()
        assert isinstance(first, SendMemory) and worker._mem() is first

    def test_the_workers_dataset_manager_shares_its_memory(self):
        from zfsbackup.config import BackupConfig, DatasetConfig
        from zfsbackup.workers import RemoteBackupWorker
        worker = RemoteBackupWorker(MagicMock(), MagicMock())
        config = BackupConfig(datasets=[DatasetConfig(name='pool/x')])
        assert worker._new_manager(config)._memory is worker._memory

    def test_the_base_worker_builds_a_plain_manager(self):
        from zfsbackup.config import BackupConfig, DatasetConfig
        from zfsbackup.workers import SnapshotWorker
        worker = SnapshotWorker(MagicMock(), MagicMock())
        manager = worker._new_manager(BackupConfig(datasets=[DatasetConfig(name='pool/x')]))
        assert isinstance(manager._memory, SendMemory)


# ---------------------------------------------------------------------------------------------
# Import hygiene
# ---------------------------------------------------------------------------------------------

REPO = Path(__file__).resolve().parent.parent

_FORBIDDEN = [
    'zfsbackup.remote', 'zfsbackup.local', 'zfsbackup.api', 'zfsbackup.identity',
    'requests', 'websocket',
]

_IMPORT_PROBE = """
import importlib, json, sys
for name in {modules!r}:
    importlib.import_module(name)
loaded = sorted(m for m in sys.modules
                if m in {forbidden!r} or any(m.startswith(f + '.') for f in {forbidden!r}))
print(json.dumps({{'loaded': loaded, 'imported': {modules!r}}}))
"""


class TestImportHygiene:
    """`sendpair`, `sendmemory` and `sendschedule` are imported before the start-up version check
    (S-8) and by the scheduler thread: they must pull in no transport (`remote`, `local`, `api`,
    `identity`, `requests`, `websocket`)."""

    MODULES = ['zfsbackup.sendmemory', 'zfsbackup.sendpair', 'zfsbackup.sendschedule']

    @staticmethod
    def probe(tmp_path, modules):
        import json
        env = hermetic_env(PYTHONPATH=f"{REPO}{__import__('os').pathsep}{REPO / 'libs' / 'libzfseasy'}")
        proc = subprocess.run(
            [sys.executable, '-c', _IMPORT_PROBE.format(modules=modules, forbidden=_FORBIDDEN)],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
        )
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout.strip().splitlines()[-1])

    @pytest.mark.parametrize('module', MODULES)
    def test_each_loads_no_transport(self, tmp_path, module):
        assert self.probe(tmp_path, [module])['loaded'] == []

    def test_all_three_together_load_no_transport(self, tmp_path):
        assert self.probe(tmp_path, self.MODULES)['loaded'] == []

    def test_the_probe_would_notice_a_transport(self, tmp_path):
        """Not vacuous: importing the remote manager does load what the others must not."""
        loaded = self.probe(tmp_path, ['zfsbackup.remote'])['loaded']
        assert 'zfsbackup.remote' in loaded and 'requests' in loaded

    def test_sendmemory_is_stdlib_only(self, tmp_path):
        import json
        probe = (
            "import sys, json\nbefore = set(sys.modules)\nimport zfsbackup.sendmemory\n"
            "new = sorted({m.split('.')[0] for m in set(sys.modules) - before})\n"
            "print(json.dumps([m for m in new if m not in sys.stdlib_module_names "
            "and not m.startswith('_')]))\n"
        )
        env = hermetic_env(PYTHONPATH=str(REPO))
        proc = subprocess.run(
            [sys.executable, '-c', probe], cwd=tmp_path, env=env, capture_output=True,
            text=True, timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        assert json.loads(proc.stdout.strip().splitlines()[-1]) == ['zfsbackup']

"""Drivers for the parallel remote worker (Track B, B3); not a test module.

`LoopRig` runs the REAL `RemoteBackupWorker.run()` in the calling thread -- its scheduler
(`sendloop.SendLoop`), its sender threads, its factory (`_new_kit` / `_new_sender` /
`_before_loop`), its status writer -- over FAKE transports (`zfsbackup.remote.RemoteBackupManager`
and `zfsbackup.local.LocalBackupManager` are replaced by a factory that returns the rig's sender)
and either

* a real `DatasetManager` whose zfs-touching reads (`get_own_anchor`, `list_snapshots`,
  `clock_behind`) are mocked (the default), or
* a real `DatasetManager` over a fake zfs world patched one level below (`world=True`: the caller
  patched `subprocess.Popen`, e.g. `test_zfsbackup_recursive_sends.InheritingZfs`).

Nothing here reaches `zfs`, a database or the network.

The loop never returns by itself (it is a daemon's main loop), so the rig STOPS it: by default when
it is QUIET (no sender or discovery thread, nothing queued, nothing dispatchable now), or when
`until(rig)` is true. The predicate is evaluated on the scheduler thread at the top of each tick (the
only place `_exit_reason` is called from), so a sender that is classifying its result is never cut
short by it. A watchdog stops the loop after `bound` seconds anyway (`rig.timed_out`, asserted by
`run()`): no test using the rig can hang the suite.

Every pair is due at once (`fraction` 0 -> catch-up at `now`), so one `run()` is "one cycle": every
configured pair, then the children their discovery found, in the schedule's order.
"""

import logging
import threading
import time
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional
from unittest.mock import MagicMock

from _timer_support import timer_stretch
from zfsbackup import workers as workers_module
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.sendmemory import COPY_CONFIRMED, SendMemory
from zfsbackup.sendschedule import SendSchedule
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import ExitReason, RemoteBackupWorker

_STRETCH: List[float] = []


def stretch() -> float:
    """`timer_stretch()`, measured once per process and never below 1."""
    if not _STRETCH:
        _STRETCH.append(max(1.0, timer_stretch()))
    return _STRETCH[0]


def bound(seconds: float = 30.0) -> float:
    """A hard bound for a wait that normally takes milliseconds."""
    return seconds * stretch()


def wait_for(predicate: Callable[[], bool], timeout: Optional[float] = None, step: float = 0.005) -> bool:
    """Poll `predicate` until true or `timeout` (default `bound()`); -> its last value."""
    deadline = time.monotonic() + (bound() if timeout is None else timeout)
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return bool(predicate())


def fake_sender(done=True, problem=None, retryable=False, confirm=True) -> MagicMock:
    """A transport manager double (`RemoteBackupManager` / `LocalBackupManager`).

    `backup_dataset` records `(dataset, destination)` in `.attempts`, raises `RuntimeError('boom')`
    for a pair in `.raises`, and returns `.outcomes[pair]` (default `done`). A True answer CONFIRMS
    the copy in the worker's `SendMemory` (`COPY_CONFIRMED`, what an acknowledged send or a negotiate
    that found the copy does) unless `confirm` is false or the pair is in `.unconfirmed`. Replace
    `backup_dataset.side_effect` for anything else (the confirmation is then yours to make)."""
    sender = MagicMock(name='sender')
    sender.last_problem.return_value = problem
    sender.last_retryable.return_value = retryable
    sender.attempts = []
    sender.outcomes = {}
    sender.raises = set()
    sender.unconfirmed = set()
    sender.done = done
    sender.confirm = confirm

    def backup(dsi, remote_cfg, should_stop=None):
        key = (dsi.name, remote_cfg.destination)
        sender.attempts.append(key)
        if key in sender.raises:
            raise RuntimeError('boom')
        result = sender.outcomes.get(key, sender.done)
        if result and sender.confirm and key not in sender.unconfirmed:
            sender._memory.note(COPY_CONFIRMED, key)
        return result

    sender.backup_dataset.side_effect = backup
    return sender


class RecordingSchedule(SendSchedule):
    """A `SendSchedule` that records every `finished` call as `(key, kind, retryable, record_now)`."""

    def __init__(self, *args, log=None, gate_on_discovery=True, **kw):
        super().__init__(*args, **kw)
        self.log = log if log is not None else []
        #: No pair is dispatched while a parent's first discovery is due or running: the children are
        #: known before the parent's first send, so a test sees ONE ordering (in production a parent can
        #: be sent before its discovery returns, or still be running when it does; those races are covered
        #: by `test_zfsbackup_sendloop.py::TestDiscovery::test_a_parent_that_failed_before_its_discovery_returned_is_not_sent_again_at_once`
        #: and `...::test_a_parent_whose_discovery_returned_while_it_ran_is_not_sent_again_at_once`, over the
        #: plain `SendSchedule`, and by the `_link` cases and `TestConfirmationArmedWhileTheParentRuns` of
        #: `test_zfsbackup_sendschedule.py`).
        self.gate_on_discovery = gate_on_discovery

    def dispatch(self, now=None, running=None):
        if self.gate_on_discovery and (
            self.discovery_due(now) or any(d.in_flight for d in self._parents.values())
        ):
            return []
        return super().dispatch(now, running)

    def finished(self, key, kind, retryable=False, now=None):
        record_now = super().finished(key, kind, retryable, now)
        self.log.append((tuple(key), getattr(kind, 'value', kind), retryable, record_now))
        return record_now


def _quiet(loop) -> bool:
    """Nothing is running, queued or dispatchable now: the loop has nothing more to do this cycle."""
    sched, now = loop.schedule, time.time()
    return (
        not loop._senders and not loop._discoveries and loop._outbox.empty() and not loop._held_dirty
        and not sched.dispatch(now) and not sched.discovery_due(now)
    )


class LoopRig:
    """See the module docstring. Attributes: `worker`, `manager` (the shared `DatasetManager`
    unless `world=True`), `remote` / `local` (the transports), `built` (every transport the factory
    built: `('remote'|'local', kwargs)`), `finished` (the schedule's `finished` calls)."""

    def __init__(
        self, config, monkeypatch, *, remote=None, local=None, world=False, status_dir=None,
        stop=None, epoch=None, born_epoch=0, client_id='client1', tls_dir=None, fraction=0.0,
        poll=0.02, caps=None, notice=None, gate=True, real_methods=(), worker_cls=RemoteBackupWorker, **worker_kw,
    ):
        self.config = config
        self.world = world
        self.stop = stop if stop is not None else StopFlag()
        self.epoch = epoch
        self.worker = worker_cls(
            config, self.stop, reload_epoch=epoch, born_epoch=born_epoch, status_dir=status_dir,
            client_id=client_id, tls_dir=tls_dir, **worker_kw,
        )
        self.remote = remote if remote is not None else fake_sender()
        self.local = local if local is not None else fake_sender()
        # ONE memory for the worker and the senders, as in production: a sender double that owns a
        # `SendMemory` (`.memory`) is the one the worker adopts.
        for sender in (self.remote, self.local):
            memory = getattr(sender, 'memory', None)
            if isinstance(memory, SendMemory):
                self.worker._memory = memory
                break
        for sender in (self.remote, self.local):
            if isinstance(getattr(sender, 'memory', None), SendMemory):
                sender.memory = self.worker._memory
        self.memory = self.worker._mem()
        for sender in (self.remote, self.local):
            if not isinstance(getattr(sender, 'memory', None), SendMemory):
                sender._memory = self.memory
        self.built: List[tuple] = []
        self.finished: List[tuple] = []
        self.timed_out = False
        self.reasons: List[object] = []
        self._until: Optional[Callable[['LoopRig'], bool]] = None
        self._release: List[Callable[[], None]] = []

        def make(kind, sender):
            def factory(cfg, manager, **kw):
                self.built.append((kind, dict(kw)))
                if not isinstance(getattr(sender, 'memory', None), SendMemory):
                    sender._memory = kw.get('memory')
                return sender
            return factory

        # The scheduler's slice (it polls the exit reason once per slice): short, so a run ends within
        # a few ticks of its last outcome instead of waiting out a half-second slice.
        monkeypatch.setattr('zfsbackup.sendloop.STOP_POLL', poll)
        if notice is not None:
            monkeypatch.setattr('zfsbackup.sendloop.WAITING_NOTICE', notice)
        caps_kw = {} if caps is None else {'max_senders': caps[0], 'max_per_destination': caps[1]}
        monkeypatch.setattr('zfsbackup.remote.RemoteBackupManager', make('remote', self.remote))
        monkeypatch.setattr('zfsbackup.local.LocalBackupManager', make('local', self.local))
        monkeypatch.setattr(
            workers_module, 'SendSchedule',
            lambda cid, check: RecordingSchedule(
                cid, check, fraction=lambda *a: fraction, log=self.finished, gate_on_discovery=gate, **caps_kw),
        )
        if not world:
            self.manager = DatasetManager(config, memory=self.memory)
            for name, value in (('get_own_anchor', None), ('list_snapshots', []), ('clock_behind', False)):
                if name not in real_methods:
                    setattr(self.manager, name, MagicMock(name=name, return_value=value))
            self.worker._new_manager = lambda cfg: self.manager
        else:
            self.manager = None
        real = self.worker._exit_reason

        def exit_reason():
            reason = real()
            if reason is None and self._until is not None:
                loop = self.worker._loop
                if loop is not None and self._until(self):
                    reason = ExitReason.STOP
            if reason is not None and not self.reasons:
                self.reasons.append(reason)
            return reason

        self.real_exit_reason = real
        self.worker._exit_reason = exit_reason

    # ------------------------------------------------------------------------------------- run

    @property
    def loop(self):
        return self.worker._loop

    def on_exit_release(self, fn: Callable[[], None]) -> None:
        """Call `fn` if the watchdog fires (to unblock a gated sender)."""
        self._release.append(fn)

    def run(self, until=None, limit: Optional[float] = None) -> 'LoopRig':
        """`worker.run()` until `until(rig)` (default: quiet). A `SystemExit` (a reload's exit 3)
        propagates after the watchdog is cancelled; a watchdog firing is a test failure."""
        self._until = until if until is not None else (lambda rig: _quiet(rig.loop))
        self.worker._loop = None
        self.reasons.clear()

        def fire():
            self.timed_out = True
            self.stop.set()
            for fn in self._release:
                fn()

        timer = threading.Timer(bound(30.0) if limit is None else limit, fire)
        timer.daemon = True
        timer.start()
        try:
            self.worker.run()
        finally:
            timer.cancel()
        assert not self.timed_out, 'the loop did not finish within its bound (the watchdog stopped it)'
        return self

    def sent(self, sender=None) -> List[tuple]:
        """`(dataset, destination)` of every `backup_dataset` call of `sender`, in call order."""
        sender = sender if sender is not None else self.remote
        return [(c.args[0].name, c.args[1].destination) for c in sender.backup_dataset.call_args_list]

    def records(self, name='remote-backup-worker') -> Dict[tuple, object]:
        from zfsbackup.health import read_worker_status
        status = read_worker_status(self.worker._status_dir, name)
        return {} if status is None else {r.key: r for r in status.records}


def dsi_namespace(name, destinations=(), frequency_s=3600):
    """A scheduler-side dataset stand-in (`name`, `expanded_from`, `config`) for a direct `SendLoop`."""
    from datetime import timedelta
    return SimpleNamespace(
        name=name, expanded_from=None,
        config=SimpleNamespace(
            remote=[SimpleNamespace(destination=d, frequency=None) for d in destinations],
            frequency=timedelta(seconds=frequency_s), recursive=False,
        ),
    )


logging.getLogger(__name__).addHandler(logging.NullHandler())


def seed_status(directory, *rows, name='remote-backup-worker', age=7200.0):
    """Write a previous incarnation's status file: each row is `(dataset, destination, ok[, error])`
    (a row may be repeated: each repeat is another attempt), attempted `age` seconds ago, so a pair
    with such a record is NOT recent enough to count as "attempted in the current slot" and is due."""
    from zfsbackup.health import KIND_SEND, StatusWriter
    writer = StatusWriter(directory, name, clock=lambda: time.time() - age)
    for row in rows:
        dataset, destination, ok, *error = row
        writer.record(KIND_SEND, dataset, destination, ok=ok, error=error[0] if error else None)
    writer.flush(force=True)
    return writer


def age_status(worker, seconds=7200.0):
    """Make every record the worker's status writer holds `seconds` older (and write the file): the next
    incarnation sees pairs last attempted long ago, so they are due again. Use after `LoopRig.run()`."""
    writer = worker._status
    for record in writer.records:
        for field in ('last_attempt', 'last_success', 'last_error_at'):
            value = getattr(record, field)
            if value is not None:
                setattr(record, field, value - seconds)
    writer._dirty = True
    writer.flush(force=True)


def run_worker_until_sent(worker, monkeypatch, pairs, *, limit=600.0, poll=0.1, extra_patches=None):
    """Run the REAL `RemoteBackupWorker.run()` (real transports; for the real-ZFS tier, where `LoopRig`'s fake
    senders are no use) until every `(dataset, destination)` in `pairs` has a send record that is `ok`, not
    `incomplete` and attempted in THIS call, and nothing is running; then stop it (the scheduler's exit
    reason). The parallel remote worker never returns by itself, and patching `_wait` (the old serial loop's
    stop) does nothing to it. Every pair is due at once (`fraction` 0).

    Fails fast: as soon as a wanted pair has a send record with `result == 'error'` whose attempt is after
    this call's start (a retryable failure is recorded only when its retries are spent, so this is a final
    answer), the worker is stopped and the call fails naming the pair and its `last_error`; an error record
    of an earlier incarnation or of a pair not in `pairs` does not count. A watchdog stops it after `limit`
    seconds (scaled by `timer_stretch()`) for everything else (a pair that is never due, a send that hangs)
    and the call then fails.

    `extra_patches(mp)`, when given, is called with the `monkeypatch` context the run happens in (so what it
    patches is undone with the run): a driver that observes `backup_dataset` installs its wrapper there."""
    started = time.time()
    wanted = set(pairs)
    timed_out = []
    failures: List[str] = []

    def send_records():
        return {(r.dataset, r.destination): r for r in worker._status.records if r.kind == 'send'}

    def done():
        records = send_records()
        return all(
            k in records and records[k].result == 'ok' and not records[k].incomplete
            and (records[k].last_attempt or 0) >= started for k in wanted
        )

    def failed() -> List[str]:
        records = send_records()
        return [
            f"{k[0]} -> {k[1]}: {records[k].last_error or 'no error text'}"
            for k in sorted(wanted)
            if k in records and records[k].result == 'error' and (records[k].last_attempt or 0) >= started
        ]

    real_reason = worker._exit_reason

    def exit_reason():
        reason = real_reason()
        if reason is None and worker._status is not None:
            problems = failed()
            if problems:
                failures.extend(problems)
                reason = ExitReason.STOP
            elif done():
                loop = getattr(worker, '_loop', None)
                if loop is not None and not loop._senders and not loop._discoveries:
                    reason = ExitReason.STOP
        return reason

    worker._exit_reason = exit_reason

    def fire():
        timed_out.append(True)
        worker.stop_event.set()

    timer = threading.Timer(limit * stretch(), fire)
    timer.daemon = True
    with monkeypatch.context() as mp:
        mp.setattr('zfsbackup.sendloop.STOP_POLL', poll)
        mp.setattr(workers_module, 'SendSchedule', lambda cid, check: SendSchedule(cid, check, fraction=lambda *a: 0.0))
        if extra_patches is not None:
            extra_patches(mp)
        timer.start()
        try:
            worker.run()
        finally:
            timer.cancel()
    assert not failures, 'a pair failed (the worker was stopped at once): ' + '; '.join(failures)
    assert not timed_out, 'the worker did not send every pair within the bound (the watchdog stopped it)'

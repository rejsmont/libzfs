"""Tests for `zfsbackup.stopflag.StopFlag` -- the shared-memory, poll-based
replacement for `multiprocessing.Event` used to signal daemon workers to
stop.

See `zfsbackup/stopflag.py`'s own module docstring and the approved
item-8x plan for the defect this module exists to fix:
`multiprocessing.Event.set()` calls `Condition.notify_all()`, which blocks
on an internal semaphore once per registered sleeper. A process killed
while parked inside `Event.wait()` releases its "I am sleeping" token but
never reaches the `finally:` that would release the matching "I woke up"
token. The ghost is permanent: every LATER `set()` blocks forever holding
the Condition's lock, and because `Event.is_set()` takes the same lock, a
single wedged `set()` also freezes every future `is_set()` in every other
process sharing the Event -- the supervisor's poll loop, not merely its
shutdown path.

**The headline tests here (`TestStopFlagSurvivesAbruptSleeperDeath`) never
call the candidate `set()` on this process's own main thread.** A
regression that reintroduces the `Event`-shaped hang does not fail -- it
wedges the whole pytest run with no output. Every assertion about "does
`set()` return" instead runs `set()` inside a short-lived child-process
canary, joined with a hard timeout and killed if it doesn't finish, so a
regression here times out cleanly and reports an ordinary assertion
failure. This mirrors `tests/test_zfsbackup_signals.py`'s established
pattern of pinning `multiprocessing.get_context(...)` explicitly rather
than relying on the platform default (macOS: spawn, Linux on 3.14:
forkserver) -- an unpinned test would silently exercise only one start
method.

Mutation-check (recorded per the pytest-test-author brief, not itself a
test): swapping `StopFlag()` for `multiprocessing.Event()` inside
`TestStopFlagSurvivesAbruptSleeperDeath` makes all nine parametrised
cases fail as ordinary, prompt assertion failures (the canary's
`exitcode` is `None` after `CANARY_TIMEOUT`) -- never a hung test run.
`TestSensitivityControlMpEventWedges` is what proves that failure mode is
reachable at all, rather than the harness vacuously passing regardless of
what's under test.
"""

import ctypes
import multiprocessing
import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from zfsbackup.stopflag import STOP_POLL, StopFlag

#: Every start method actually available on this platform, pinned by name
#: rather than relying on `multiprocessing.get_context()`'s default --
#: see this module's docstring and `tests/test_zfsbackup_signals.py:18-27`.
START_METHODS = [
    m for m in ("fork", "spawn", "forkserver") if m in multiprocessing.get_all_start_methods()
]

#: All three reproduce the ghost identically (verified in the item-8x
#: plan) -- there is no "needs a SIGKILL" special case.
KILL_SIGNALS = [signal.SIGKILL, signal.SIGTERM, signal.SIGINT]

# How long we give sleeper children to report readiness, and how long we
# give a signaled child to actually finish dying.
READY_TIMEOUT = 10.0
DEATH_JOIN_TIMEOUT = 5.0
# A brief settle between "wrote the readiness marker" and "send the
# signal", so the child has actually entered the blocking part of
# wait() (a time.sleep() slice for StopFlag, `_wait_semaphore.acquire()`
# for Event) rather than being killed a microsecond too early, which
# would not reproduce either implementation's "died mid-wait" scenario.
KILL_SETTLE = 0.3
# Budget for the canary's set() to return under StopFlag. Generous
# relative to STOP_POLL; tiny relative to the sensitivity control's
# "never returns at all" case below.
CANARY_TIMEOUT = 10.0
# Deliberately short for the mp.Event sensitivity control: it only has
# to prove the canary is STILL ALIVE (i.e. blocked), not that it stays
# blocked forever. A short wait is exactly as conclusive as a long one
# and keeps this one slow-marked test cheap.
SENSITIVITY_TIMEOUT = 3.0


def _reset_to_sig_dfl():
    """Mirrors `zfsbackup.workers._init_child_process`'s first two
    statements (`workers.py:78-79`) exactly.

    Without this, a SIGINT delivered under this test process's own
    (default `KeyboardInterrupt`-raising) disposition unwinds cleanly
    through `Condition.wait`'s `finally:` and does NOT reproduce the
    ghost -- precisely escalation (b) recorded in the item-8x plan: item
    8's SIG_DFL reset is what converted a survivable Ctrl-C into a
    wedging one, and every real worker carries that reset.
    """
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def _wait_child(flag, ready_path):
    """Runs in a child process. Resets signal dispositions exactly as a
    real worker does, marks itself ready, then parks in `flag.wait()`
    with no timeout -- the blocking case of `BaseWorker.run`'s
    `stop_event.wait(timeout=...)`.

    If this process is killed abruptly while inside `wait()` (the
    scenario under test), it never reaches the line after `wait()` and
    never runs any cleanup at all -- that is the whole point.
    """
    _reset_to_sig_dfl()
    with open(ready_path, "w") as fh:
        fh.write("ready")
    flag.wait()
    # Only reached if wait() returned on its own -- not exercised by the
    # wedge tests below, but harmless if it ever happens.
    with open(ready_path + ".woke", "w") as fh:
        fh.write("woke")


def _canary_call_set(flag, result_path):
    """The canary: a short-lived, throwaway process whose only job is to
    call the candidate `set()` and report back. Isolates any hang to a
    process the test can kill instead of wedging pytest itself.

    Writes a `<result_path>.started` marker IMMEDIATELY BEFORE calling
    `set()` -- the sensitivity control
    (`TestSensitivityControlMpEventWedges`) asserts that marker exists
    before it asserts `canary.is_alive()`, so "still alive" can only mean
    "genuinely blocked inside `set()`", never "hasn't gotten there yet"
    (e.g. because `spawn` startup plus imports ate the whole timeout
    budget on a loaded runner). Harmless for every other caller of this
    function, which never checks for the marker.
    """
    with open(str(result_path) + ".started", "w") as fh:
        fh.write("started")
    flag.set()
    with open(result_path, "w") as fh:
        fh.write("set-returned")


def _child_wait_then_exit(flag):
    """For the wake-latency test: parks in `flag.wait()` and exits the
    instant it returns. The test asserts on `Process.join(timeout=...)`
    succeeding, never on a measured duration.
    """
    flag.wait()


def _child_set_flag(flag):
    """For the pickle/sharing test: sets the flag from a child process
    under `spawn`/`forkserver`, where the argument genuinely crosses a
    pickle boundary (`multiprocessing.reduction.ForkingPickler`), then
    exits. The parent asserts `flag.is_set()` afterward -- true only if
    the shared memory, not a private copy, made it across.
    """
    flag.set()


class _StopFlagWithSecondField(StopFlag):
    """Test-only subclass mirroring item 18's own trap: a SECOND shared
    field (`_gen`, a `Value('l')`, exactly the reload-generation counter
    item 18 adds) alongside the base `_flag`.

    `__getstate__` returning a MAPPING rather than the bare `self._flag`
    is what makes extending it safe: a subclass overrides `__getstate__`/
    `__setstate__` to add its own key to the same dict (see below) and the
    round trip carries both fields under `spawn`/`forkserver`, where the
    argument genuinely crosses a pickle boundary. A bare
    `return self._flag` (transferring only the base field, no matter what
    a subclass's own `__getstate__` tried to add) would still look green
    under `fork` -- the child inherits `_gen` via memory directly, no
    pickling involved at all -- and only die with
    `AttributeError: '_StopFlagWithSecondField' object has no attribute
    '_gen'` under `spawn`/`forkserver`, deep inside whatever child code
    first touches `_gen`. That platform split is exactly why this test is
    pinned to `spawn` rather than left to the process default.
    """

    def __init__(self) -> None:
        super().__init__()
        self._gen = multiprocessing.Value('l', 0, lock=False)

    def __getstate__(self) -> dict:
        state = super().__getstate__()
        state['gen'] = self._gen
        return state

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        self._gen = state['gen']

    def bump_generation(self) -> None:
        self._gen.value += 1

    @property
    def generation(self) -> int:
        return self._gen.value


def _child_bump_generation_and_set(flag):
    """Runs in a spawned child: touches BOTH shared fields the subclass
    adds, in an order that surfaces a broken `_gen` round trip (missing
    entirely, or a private copy) as an `AttributeError`/silent no-op
    before `set()` -- the base field's own round trip -- ever runs.
    """
    flag.bump_generation()
    flag.set()


def _wait_for_files(paths, timeout):
    deadline = time.monotonic() + timeout
    remaining = list(paths)
    while remaining and time.monotonic() < deadline:
        remaining = [p for p in remaining if not os.path.exists(p)]
        if remaining:
            time.sleep(0.02)
    return not remaining


def _kill_and_reap(proc, sig=signal.SIGKILL, timeout=DEATH_JOIN_TIMEOUT):
    """Best-effort cleanup for a process that must not outlive the test,
    regardless of what the test asserted or how it failed. Never raises.
    """
    if proc is None:
        return
    try:
        if proc.is_alive():
            try:
                os.kill(proc.pid, sig)
            except (ProcessLookupError, PermissionError):
                pass
            proc.join(timeout=timeout)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=timeout)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# API parity with multiprocessing.Event -- pure in-process behaviour, no
# subprocess involved. Fast and unconditional.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStopFlagApiParity:
    """`set`/`clear`/`is_set`/`wait` must duck-type `multiprocessing.Event`
    closely enough that the ~35 existing call sites (and
    `tests/test_zfsbackup_real.py:221`, which hands a real `Event` to a
    real `ApiWorker`) keep working unmodified regardless of which one
    they were constructed with.
    """

    def test_initial_state_is_clear(self):
        flag = StopFlag()
        assert flag.is_set() is False

    def test_set_then_is_set_true(self):
        flag = StopFlag()
        flag.set()
        assert flag.is_set() is True

    def test_clear_after_set_returns_to_false(self):
        flag = StopFlag()
        flag.set()
        flag.clear()
        assert flag.is_set() is False

    def test_is_set_returns_real_bool_not_raw_byte(self):
        flag = StopFlag()
        assert type(flag.is_set()) is bool
        flag.set()
        assert type(flag.is_set()) is bool

    def test_wait_zero_on_clear_flag_returns_false_without_sleeping(self, mocker):
        flag = StopFlag()
        sleep_spy = mocker.patch("zfsbackup.stopflag.time.sleep")
        assert flag.wait(0) is False
        sleep_spy.assert_not_called()

    def test_wait_zero_on_set_flag_returns_true_without_sleeping(self, mocker):
        flag = StopFlag()
        flag.set()
        sleep_spy = mocker.patch("zfsbackup.stopflag.time.sleep")
        assert flag.wait(0) is True
        sleep_spy.assert_not_called()

    def test_wait_none_on_preset_flag_returns_immediately_no_sleep(self, mocker):
        """Pins the exact behaviour `tests/test_zfsbackup_workers.py:309,376`
        depend on: `wait()` on an already-set flag must return without
        sleeping even once, or those tests stop being fast once 8x-3a
        wires `StopFlag` into `BaseWorker`.
        """
        flag = StopFlag()
        flag.set()
        sleep_spy = mocker.patch("zfsbackup.stopflag.time.sleep")
        assert flag.wait() is True
        assert flag.wait(None) is True
        sleep_spy.assert_not_called()

    def test_wait_with_timeout_returns_false_after_roughly_timeout(self):
        flag = StopFlag()
        start = time.monotonic()
        result = flag.wait(0.2)
        elapsed = time.monotonic() - start
        assert result is False
        # Generous window: at least the requested timeout, comfortably
        # less than one extra full STOP_POLL slice on top of it.
        assert 0.2 <= elapsed < 0.2 + STOP_POLL + 0.5

    def test_wait_returns_true_promptly_once_flag_is_set(self):
        flag = StopFlag()
        flag.set()
        assert flag.wait(5.0) is True

    @pytest.mark.parametrize(
        "timeout", [0, -1, float("nan")], ids=["zero", "negative", "nan"]
    )
    def test_wait_with_non_positive_or_nan_timeout_returns_without_sleeping(
        self, mocker, timeout
    ):
        """`not (timeout > 0)` rather than `timeout <= 0`, specifically so
        that NaN -- for which EVERY comparison is `False`, including
        `nan <= 0` -- still takes the no-sleep branch. Before this
        formulation, `wait(float('nan'))` never returned: the slice
        computed as `min(STOP_POLL, nan - now)` is `STOP_POLL` (`nan` loses
        every `min`/`max` comparison too), and the deadline test
        `monotonic() >= nan` is always `False`, so the call polled forever
        -- measured, a thread parked in `wait(float('nan'))` was still
        alive after 2s. `0` and `-1` share the same code path and are
        included so a regression that narrows the check back to
        `timeout <= 0` (which handles zero and negative timeouts but not
        NaN) is caught by the other two parametrised cases even if the NaN
        case alone were ever weakened.
        """
        flag = StopFlag()
        sleep_spy = mocker.patch("zfsbackup.stopflag.time.sleep")
        assert flag.wait(timeout) is False
        sleep_spy.assert_not_called()

    def test_wait_with_nan_timeout_on_already_set_flag_returns_true(self):
        """The already-set short-circuit at the top of `wait()` fires
        before the NaN-specific branch is ever reached, so a set flag
        must still report `True` for a NaN timeout -- not `False` from
        `self.is_set()` being (mis)read as the non-positive-timeout
        branch's return value on a flag that was actually set.
        """
        flag = StopFlag()
        flag.set()
        assert flag.wait(float("nan")) is True


# ---------------------------------------------------------------------------
# The headline regression: children parked in wait(), killed abruptly,
# set() must still return. Every set()/is_set() call under test happens
# inside a subprocess canary with a hard external timeout -- never on
# this test's own main thread. See module docstring.
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestStopFlagSurvivesAbruptSleeperDeath:
    """`StopFlag.set()` must return promptly even after every process
    that was parked in `flag.wait()` died abruptly while still inside the
    wait -- the scenario that permanently wedges
    `multiprocessing.Event.set()` (and, transitively, every later
    `is_set()`, per escalation (a) in the item-8x plan) instead.

    Parametrised over every available start method, pinned explicitly
    (never the process default -- see module docstring), and over
    SIGKILL/SIGTERM/SIGINT: all three reproduce the `Event` ghost
    identically, so there is no "needs a SIGKILL" special case to carve
    out.
    """

    NUM_SLEEPERS = 2

    @pytest.mark.parametrize("start_method", START_METHODS)
    @pytest.mark.parametrize("kill_signal", KILL_SIGNALS, ids=lambda s: s.name)
    def test_set_returns_after_sleepers_killed_abruptly(
        self, tmp_path, start_method, kill_signal
    ):
        ctx = multiprocessing.get_context(start_method)
        flag = StopFlag()

        sleepers = []
        ready_paths = []
        canary = None
        try:
            for i in range(self.NUM_SLEEPERS):
                ready_path = str(tmp_path / f"ready_{i}")
                ready_paths.append(ready_path)
                proc = ctx.Process(target=_wait_child, args=(flag, ready_path))
                proc.start()
                sleepers.append(proc)

            assert _wait_for_files(
                ready_paths, READY_TIMEOUT
            ), "sleeper(s) never reported ready"
            # Let each sleeper actually enter the blocking part of
            # wait(), not just reach the line before it.
            time.sleep(KILL_SETTLE)

            for proc in sleepers:
                os.kill(proc.pid, kill_signal)

            for proc in sleepers:
                proc.join(timeout=DEATH_JOIN_TIMEOUT)
                assert not proc.is_alive(), (
                    f"sleeper pid={proc.pid} did not die from "
                    f"{kill_signal.name}"
                )
                assert proc.exitcode != 0, (
                    "sleeper exited cleanly -- it never reached the "
                    "abrupt-death scenario under test"
                )

            # THE ASSERTION: set() must still return, called from a
            # fresh process, with a hard timeout enforced from OUTSIDE
            # that process so a regression here fails instead of hanging
            # the whole test run.
            result_path = tmp_path / "canary_result"
            canary = ctx.Process(
                target=_canary_call_set, args=(flag, str(result_path))
            )
            canary.start()
            canary.join(timeout=CANARY_TIMEOUT)
            assert canary.exitcode == 0, (
                "set() did not return within the budget -- StopFlag "
                "reproduced the Event ghost"
            )
            assert result_path.exists()

            # And the flag really is set as observed from THIS process
            # -- proving the write landed in shared memory, not just in
            # the canary's private copy.
            assert flag.is_set() is True
        finally:
            _kill_and_reap(canary)
            for proc in sleepers:
                _kill_and_reap(proc)


@pytest.mark.integration
@pytest.mark.slow
class TestSensitivityControlMpEventWedges:
    """Proves the harness above can actually detect the defect it exists
    to catch.

    `StopFlag` was verified to pass all nine start-method x signal
    combinations above, but that harness was never re-run against a real
    `multiprocessing.Event` -- so, on its own, nothing proves those nine
    passes aren't vacuous (e.g. because the settle time were too short,
    or the canary didn't really wait for the sleeper to die first). This
    test runs the identical shape with `multiprocessing.Event()` in
    place of `StopFlag()` and asserts the canary is STILL ALIVE after its
    timeout -- not that it eventually completes with a wrong result, but
    that it never completes within a budget that comfortably contains
    every prompt `StopFlag` case above.

    Deliberately a single start-method/signal combination, not the full
    3x3 matrix: the plan's own measurement already showed all nine
    combinations wedge identically for `Event` (`fork/spawn/forkserver x
    SIGKILL/SIGTERM -> set() WEDGED (>8s)`), so re-running all nine here
    would only extend a `slow`-marked test's runtime without adding
    coverage. `spawn` is available on every platform this suite targets
    (never assumes `fork`/`forkserver` exist); SIGTERM is the signal the
    daemon's own `_shutdown_workers` escalation actually sends.

    Teardown kills both the sleeper and the (necessarily wedged) canary
    unconditionally, so this test cannot leave an orphan behind even
    though the very thing it is proving is that the canary hangs.
    """

    def test_mp_event_set_wedges_forever_after_sleeper_killed(self, tmp_path):
        ctx = multiprocessing.get_context("spawn")
        # `ctx.Event()`, NOT the bare `multiprocessing.Event()` -- the
        # latter is built from the process-default context
        # (`fork` on Linux, `spawn` on macOS), silently undercutting the
        # very pinning this module's docstring (and
        # `tests/test_zfsbackup_signals.py`'s established pattern) exists
        # for: this test would then exercise whatever the default happens
        # to be on the host running it, not the `spawn` it explicitly
        # pins for the sleeper and the canary below.
        flag = ctx.Event()

        ready_path = str(tmp_path / "ready")
        sleeper = ctx.Process(target=_wait_child, args=(flag, ready_path))
        canary = None
        try:
            sleeper.start()
            assert _wait_for_files([ready_path], READY_TIMEOUT)
            time.sleep(KILL_SETTLE)

            os.kill(sleeper.pid, signal.SIGTERM)
            sleeper.join(timeout=DEATH_JOIN_TIMEOUT)
            assert not sleeper.is_alive()
            assert sleeper.exitcode != 0

            result_path = tmp_path / "canary_result"
            canary = ctx.Process(
                target=_canary_call_set, args=(flag, str(result_path))
            )
            canary.start()

            # Pin that the canary actually reached the line immediately
            # before `set()` BEFORE trusting `is_alive()` to mean "blocked
            # inside set()". Without this, `canary.is_alive()` being True
            # at `SENSITIVITY_TIMEOUT` could just as easily mean "spawn
            # startup and imports haven't finished yet" on a loaded
            # runner -- the exact vacuous-pass failure mode this control
            # exists to rule out. Measured today: the canary reaches this
            # marker at +0.05s, comfortably inside `READY_TIMEOUT`; this
            # assertion is what makes that a pinned property instead of an
            # incidental one.
            started_marker = str(result_path) + ".started"
            assert _wait_for_files([started_marker], READY_TIMEOUT), (
                "canary never reached the line immediately before set() -- "
                "cannot attribute is_alive() to being wedged inside set() "
                "rather than still starting up"
            )

            canary.join(timeout=SENSITIVITY_TIMEOUT)

            # THE CONTROL: the canary must still be running -- Event's
            # set() really is wedged, proving this harness can actually
            # tell "returns promptly" (StopFlag) apart from "never
            # returns" (Event) rather than passing regardless of what's
            # under test.
            assert canary.is_alive(), (
                "sensitivity control failed: mp.Event's set() returned -- "
                "this harness cannot detect the ghost at all, so the "
                "nine StopFlag passes above prove nothing"
            )
            assert not result_path.exists()
        finally:
            _kill_and_reap(canary)
            _kill_and_reap(sleeper)


# ---------------------------------------------------------------------------
# Sharing/pickling, wake latency, resource-tracker hygiene.
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestStopFlagPicklesAndIsSharedAcrossProcesses:
    """`spawn`/`forkserver` genuinely pickle `Process` arguments through
    `multiprocessing.reduction.ForkingPickler` -- unlike `fork`, where a
    child inherits the parent's memory directly and no pickling of
    `args` happens at all. `__getstate__`/`__setstate__` exist
    specifically for this case; without them `StopFlag` would either
    fail to pickle under spawn/forkserver at all, or -- worse -- pickle
    into a private copy that a child could set without the parent ever
    observing it.
    """

    @pytest.mark.parametrize(
        "start_method",
        [m for m in ("spawn", "forkserver") if m in START_METHODS],
    )
    def test_child_set_is_visible_to_parent_after_join(self, start_method):
        ctx = multiprocessing.get_context(start_method)
        flag = StopFlag()
        assert flag.is_set() is False

        proc = ctx.Process(target=_child_set_flag, args=(flag,))
        try:
            proc.start()
            proc.join(timeout=10)
            assert proc.exitcode == 0
            # If the child had received a private copy rather than the
            # shared byte, the parent's own flag would still read False
            # here.
            assert flag.is_set() is True
        finally:
            _kill_and_reap(proc)

    def test_subclass_with_second_shared_field_survives_spawn_round_trip(self):
        """Item 18's own trap, pinned directly: a subclass that adds a
        second shared field (`_gen = Value('l')`, the reload-generation
        counter item 18 introduces) alongside `_flag` must round-trip both
        fields under `spawn`, where crossing the pickle boundary is real.
        `__getstate__`/`__setstate__` returning/accepting a dict -- rather
        than the bare `self._flag` a simpler implementation might use --
        is what makes this possible without the subclass having to
        reimplement the base field's own pickling.
        """
        ctx = multiprocessing.get_context("spawn")
        flag = _StopFlagWithSecondField()
        assert flag.generation == 0
        assert flag.is_set() is False

        proc = ctx.Process(target=_child_bump_generation_and_set, args=(flag,))
        try:
            proc.start()
            proc.join(timeout=10)
            assert proc.exitcode == 0, (
                "child process failed -- almost certainly an AttributeError "
                "from a second shared field that did not survive the "
                "spawn pickle round trip"
            )
            # Both fields observed from the PARENT after the child's join:
            # true only if the child mutated the same shared memory the
            # parent holds, not a private copy of either field.
            assert flag.generation == 1
            assert flag.is_set() is True
        finally:
            _kill_and_reap(proc)


@pytest.mark.integration
class TestStopFlagWakeLatency:
    """Once the flag is set, a live child parked in `wait()` must notice
    within a generous multiple of `STOP_POLL` -- asserted via
    `Process.join(timeout=...)` succeeding (an event), never via a
    measured duration, per the item-8x plan's explicit instruction.
    """

    def test_live_child_exits_within_bounded_multiple_of_poll_interval(self):
        ctx = multiprocessing.get_context("spawn")
        flag = StopFlag()
        proc = ctx.Process(target=_child_wait_then_exit, args=(flag,))
        try:
            proc.start()
            # Give it a moment to actually be parked in wait() before
            # setting, so a set() that raced proc.start() couldn't
            # trivially pass regardless of poll latency.
            time.sleep(0.2)
            flag.set()
            proc.join(timeout=STOP_POLL * 10)
            assert proc.exitcode == 0, (
                "child did not wake within a generous multiple of "
                "STOP_POLL after set()"
            )
        finally:
            _kill_and_reap(proc)


@pytest.mark.unit
class TestStopFlagResourceTracker:
    """`StopFlag` backs onto one anonymous `multiprocessing.Value` byte
    (`multiprocessing.heap`), never a POSIX named semaphore or
    shared-memory block -- unlike `multiprocessing.Event`'s four
    semaphores, there is nothing for `multiprocessing.resource_tracker`
    to ever complain about leaking. Since nearly every daemon test
    constructs at least one `StopFlag`/`BackupDaemon`, this is worth
    pinning directly rather than trusting it by inspection.

    The original version of this class was vacuous: it asserted no
    "resource_tracker"/"leaked" text appeared in a fresh interpreter's
    output after 1000 construct/discard cycles, but `Event` substituted
    for `StopFlag` in the identical script produces the SAME result --
    `rc=0`, no `resource_tracker` text, no `leaked` text -- because
    `Event` unlinks its semaphores via `multiprocessing.util.Finalize` and
    never warns at a clean interpreter exit. That assertion could not
    fail for the reason its docstring claimed. The two tests below
    discriminate for real:

    - `test_backing_store_never_starts_the_resource_tracker` measures the
      thing that actually differs -- constructing an `Event` starts the
      resource tracker subprocess (`resource_tracker._resource_tracker._fd`
      goes from `None` to an fd number, verified interactively:
      `StopFlag -> None ; Event -> 5`), because registering ANY of its
      four semaphores calls `ensure_running()`. `StopFlag` never touches
      `resource_tracker` at all, so the fd must still be `None` after
      constructing fifty of them, in a fresh interpreter so no earlier
      test's `Event` usage has already started the tracker.
    - `test_backing_store_is_a_bare_ctypes_object_not_lock_wrapped` states
      the same invariant directly and cheaply, in-process: `StopFlag`'s
      `Value(..., lock=False)` returns a bare `ctypes.c_byte`, never the
      `SynchronizedBase` wrapper `lock=True` (or `Event`'s own internal
      `Value`) would produce -- the wrapper is what carries a `Lock`,
      and a `Lock` is exactly the kind of resource-tracked primitive this
      class exists to prove `StopFlag` has none of.
    """

    def test_backing_store_never_starts_the_resource_tracker(self):
        # Runs in a genuinely separate interpreter (not just this test
        # process) so that no earlier test's `multiprocessing.Event()` (or
        # any other tracked resource) has already started
        # `resource_tracker` for this session -- that would make `_fd`
        # non-`None` regardless of what `StopFlag` itself does, and pass
        # vacuously for the opposite reason the original assertion did.
        script = textwrap.dedent(
            """
            import multiprocessing.resource_tracker as rt
            from zfsbackup.stopflag import StopFlag

            for _ in range(1000):
                flag = StopFlag()
                flag.set()
                flag.is_set()
                flag.clear()
                del flag

            # THE ASSERTION: constructing StopFlags -- even fifty live at
            # once -- must never call resource_tracker.ensure_running().
            # multiprocessing.Event() flips this to a real fd (measured: 5)
            # the moment any one of its four semaphores is registered.
            xs = [StopFlag() for _ in range(50)]
            assert rt._resource_tracker._fd is None, (
                f"resource_tracker was started (fd={rt._resource_tracker._fd}) "
                "-- StopFlag must never register anything with it"
            )
            print("FD_CHECK_PASSED")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr
        assert "FD_CHECK_PASSED" in result.stdout
        combined = (result.stdout + result.stderr).lower()
        assert "resource_tracker" not in combined
        assert "leaked" not in combined

    def test_backing_store_is_a_bare_ctypes_object_not_lock_wrapped(self):
        """The same invariant as the subprocess test above, stated
        directly: `StopFlag._flag` is a bare `ctypes.c_byte` with no
        `Lock` (and so nothing `Lock` would register with
        `resource_tracker`), never the `SynchronizedBase`/`get_lock()`
        wrapper a locked `Value` (or `Event`'s own internals) carries.
        """
        flag = StopFlag()
        assert type(flag._flag) is ctypes.c_byte
        assert not hasattr(flag._flag, "get_lock")

"""A stop flag safe to `set()` from a signal handler and from any process
that dies while parked in `wait()`.

`multiprocessing.Event.set()` calls `Condition.notify_all()`, which blocks on
an internal semaphore once per registered sleeper. A process killed while
inside `Event.wait()` releases its "I am sleeping" token but never reaches
the `finally:` that would release the matching "I woke up" token -- the
ghost is permanent, and *every later* `Event.set()` blocks forever holding
the Condition's lock. Because `Event.is_set()` takes the same lock, a single
wedged `set()` also freezes every future `is_set()` in every other process
sharing the Event. Killing a worker abruptly (`SIGKILL`, or `SIGTERM`/
`SIGINT` reaching it under the default disposition) is routine for this
daemon's supervisor, so this is not a theoretical failure mode.

Independently of the ghost, `Condition.notify_all()` is not async-signal-safe
at all, so `Event.set()` was never legal to call from a signal handler in
the first place -- even with zero dead sleepers.

`StopFlag` replaces the Condition-based rendezvous with a single shared byte
(`multiprocessing.Value('b', 0, lock=False)`). `set()` is one shared-memory
store: no lock, no condition variable, no per-sleeper accounting, nothing a
dying process can leave behind, and safe to call from a signal handler.
`wait()` polls that byte in `STOP_POLL`-sized slices instead of blocking on a
semaphore.

The public surface (`set`, `clear`, `is_set`, `wait`) is deliberately a
duck-type match for `multiprocessing.Event`/`threading.Event`: callers that
already hold an `Event` keep working unchanged, and tests may pass either.

**A `StopFlag` must outlive every process it was handed to; never recreate
one while children still hold the old one.** The backing store comes from
`multiprocessing.heap`, which *recycles freed blocks*: two successive
`StopFlag()` objects were measured landing at the same shared address once
the first had been garbage-collected. Nothing in the daemon does that today
(one flag, created in `BackupDaemon.__init__` and alive for the process's
lifetime), but a reload state machine that builds a fresh flag per
generation would either resurrect a stale byte or -- worse -- hand the
children a block the parent has since reused for something else. Reuse the
same flag and `clear()` it, or keep the old one referenced until every
child holding it has been joined.
"""

import time
from typing import Optional

import multiprocessing

#: Longest a `wait()` call sleeps before re-checking the flag.
STOP_POLL = 0.5


class StopFlag:
    """A shared, multiprocess-safe boolean flag, polled rather than woken.

    Exposes exactly `set()`, `clear()`, `is_set()`, and `wait(timeout=None)`
    -- the subset of `multiprocessing.Event`'s API this daemon uses -- so it
    can be substituted for an `Event` without touching call sites.
    """

    def __init__(self) -> None:
        self._flag = multiprocessing.Value('b', 0, lock=False)

    def set(self) -> None:
        """Raise the flag. A single shared-memory store; signal-handler safe."""
        self._flag.value = 1

    def clear(self) -> None:
        """Lower the flag. Not signal-handler safe (matches `Event.clear`)."""
        self._flag.value = 0

    def is_set(self) -> bool:
        return bool(self._flag.value)

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until the flag is set or `timeout` seconds elapse.

        Returns immediately -- without sleeping -- if the flag is already
        set. With `timeout=None` blocks indefinitely, polling every
        `STOP_POLL` seconds. With `timeout=0`, a negative timeout, or NaN
        returns the current state without sleeping at all.
        """
        if self.is_set():
            return True
        # `not (timeout > 0)` rather than `timeout <= 0`, so that NaN --
        # for which *every* comparison is False -- takes this branch too.
        # With `timeout <= 0` a NaN never returned: the slice computed as
        # `min(STOP_POLL, nan - now)` is `STOP_POLL`, and the deadline test
        # `monotonic() >= nan` is False forever, so the call polled until
        # the flag was set or the process died (measured: a thread in
        # `wait(float('nan'))` still alive after 2s). Only reachable if a
        # worker's `_get_interval` ever yields NaN, which config validation
        # is supposed to prevent -- but the failure mode is a worker that
        # never runs another cycle, which is not one to leave to validation.
        if timeout is not None and not (timeout > 0):
            return self.is_set()

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = STOP_POLL if deadline is None else min(STOP_POLL, deadline - time.monotonic())
            if remaining <= 0:
                return False
            time.sleep(remaining)
            if self.is_set():
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False

    def __getstate__(self) -> dict:
        # A dict of the shared objects, NOT the bare `self._flag`.
        #
        # `self._flag` is a shared-memory ctypes object created via
        # `multiprocessing.heap`. It is only picklable through
        # `multiprocessing.reduction.ForkingPickler` (the pickler
        # `multiprocessing.Process` uses for its arguments under `spawn`
        # and `forkserver`), never through the plain `pickle` module.
        # Returning a container of it delegates that reduction to whichever
        # pickler is doing the work exactly as returning it bare did,
        # keeping the shared memory -- not a copy -- on the receiving end.
        #
        # The container is what makes adding a field safe. A bare return
        # silently transfers *only* that one object: under `fork` a second
        # attribute is inherited anyway and everything looks green, while
        # under `spawn`/`forkserver` the child's `StopFlag` never gets it
        # and dies with `AttributeError` on first use, deep inside a worker
        # loop. Item 18's reload generation ("a second `Value('l')` read in
        # the same poll") is precisely that change, and this daemon exists
        # to not have platform-dependent worker deaths. Add new shared
        # fields to this dict and to `__setstate__`.
        return {'flag': self._flag}

    def __setstate__(self, state: dict) -> None:
        self._flag = state['flag']

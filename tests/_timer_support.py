"""Shared probe for tests that bound a real-clock wait.

A host whose timer is coarse (a small, loaded VM where `sleep(0.01)` takes
several times as long) stretches every short sleep, so a fixed upper bound on
a wait made of short sleeps stops meaning anything. Such a test measures
`timer_stretch()`, scales its upper bound by it and skips with a reason when
the stretch is too large to tell a stretched timer from the regression it
guards against.
"""

import time


def timer_stretch(samples: int = 20, nominal: float = 0.01) -> float:
    """How many times longer than asked this host's short sleeps really take.

    SQLite's busy handler adds up the *nominal* delay of each step (1, 2, 5,
    10, 15, ... ms) and gives up when that sum reaches `busy_timeout`, however
    long each sleep really took. On a host whose timer is coarse (a small VM
    where `sleep(0.01)` takes ~65 ms) a wait therefore stretches by about this
    factor, and a real-clock bound on one statement stops meaning anything.
    """
    started = time.monotonic()
    for _ in range(samples):
        time.sleep(nominal)
    return (time.monotonic() - started) / (samples * nominal)


def coarse_timer_reason(stretch: float, extra: str = "") -> str:
    """The skip message for a timer too coarse for a real-clock bound."""
    return (
        f"timer too coarse for a real-clock bound: sleep(0.01) takes "
        f"{stretch:.1f}x as long as asked{extra}"
    )

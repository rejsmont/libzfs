"""A shared monotonic-clock stand-in for tests of code that does `import time` and calls
`time.monotonic()` (the API worker's validity-warning throttle, the remote worker's daily
certificate-expiry check).

Patch the MODULE's `time` attribute, never `time.monotonic` itself: `mocker.patch.object(workers,
"time", fake)`. Patching the real `time.monotonic` would change it for every thread in the process
(deadlines, `_eventually` loops), and a frozen clock would hang them.

`FakeTime` forwards everything but `monotonic` to the real `time` module. `monotonic()` is the real
clock plus `offset` (so it keeps flowing and `advance()` jumps it), or exactly `frozen` when that
is set (exact boundary arithmetic, for single-threaded unit tests).
"""

import time


class FakeTime:
    def __init__(self):
        self.offset = 0.0
        self.frozen = None

    def __getattr__(self, name):
        return getattr(time, name)

    def monotonic(self):
        return self.frozen if self.frozen is not None else time.monotonic() + self.offset

    def advance(self, seconds):
        if self.frozen is not None:
            self.frozen += seconds
        else:
            self.offset += seconds

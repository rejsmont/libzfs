"""Pids for FAKE worker processes that are fed through the real
`BackupDaemon._spawn` / `_check_workers` / `_shutdown_workers`.

Under the `forkserver` start method (the default from Python 3.14 on Linux)
`_spawn` records `procident.start_token(worker.pid)` and the orphan
classification asks the OS about that pid. A small made-up pid such as 100 or
5003 can belong to a REAL process on the machine running the tests (a CI
runner's processes live in the thousands), turning a fake dead worker into a
"live orphan" -- a flake that depends on the host. Fake pids therefore live
above every platform's `pid_max` (Linux: 4194304; macOS: 99999) yet below
2**31, so `os.kill(pid, 0)` answers ESRCH and `/proc/<pid>` never exists.
"""

import os

FAKE_PID_BASE = 2 ** 30


def fake_pid(n: int = 0) -> int:
    """`FAKE_PID_BASE + n`, checked to be unresolvable."""
    pid = FAKE_PID_BASE + n
    assert_unresolvable(pid)
    return pid


def assert_unresolvable(pid: int) -> None:
    """Fail loudly if a real process answers at `pid` (a future collision)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return
    except PermissionError:
        raise AssertionError(f"fake pid {pid} belongs to a real process")
    raise AssertionError(f"fake pid {pid} belongs to a live process")

"""Fork-pinned coverage for `workers._init_child_process`'s SIGINT/SIGTERM
reset.

Follow-up to item 8 Phase C, coordinator-reported defect 2: under the
`fork` multiprocessing start method, a worker child inherits whatever
SIGINT/SIGTERM disposition its parent process had at the moment of
`Process.start()` -- and `BackupDaemon.__init__` installs its own
`_signal_handler` on both signals before any worker is created. A tty
Ctrl-C then signals the whole foreground process group, so every worker
(sitting in `stop_event.wait()`) runs the SUPERVISOR's handler on its own
main thread and wedges holding the shared `Event`'s lock, which then
wedges the supervisor's own `is_set()` poll. `_init_child_process` resets
both dispositions to `SIG_DFL` as literally the first statement of every
worker's `run()`, specifically to make `fork` behave the same as `spawn`/
`forkserver` already do (both start children at `SIG_DFL`/the default
int handler, never inheriting anything).

**Pinned to `multiprocessing.get_context("fork")` explicitly, never the
process-wide default.** CI's macOS and Linux-3.14 defaults are spawn and
forkserver respectively, where every child already starts at `SIG_DFL`
regardless of what `_init_child_process` does -- an unpinned test would
pass here while testing nothing. Using `get_context("fork")` (matching
`tests/test_zfsbackup_store.py`'s `TestDbForkSafety` and
`tests/test_zfsbackup_workers.py`'s `TestWorkerForkedChildLoadsConfig`,
rather than a global `multiprocessing.set_start_method("fork")`) keeps
this test from changing the start method for anything else in the
process.

**Liveness/completion is asserted by `Process.join()` + `.exitcode`,
never `os.kill(pid, 0)`** -- the coordinator flagged that `kill(pid, 0)`
reports a zombie (a child that has exited but not yet been reaped) as
"alive", which fooled an earlier version of this investigation once.

**No test here sends a real SIGINT/SIGTERM to anything.** Both children
below only *read* their own signal disposition via `signal.getsignal` and
report it back through a JSON file -- exactly the coordinator's own
verification methodology. Actually delivering a signal to a live daemon
with workers is the item-8x wedge hazard (`workers._init_child_process`'s
own docstring); it is not reachable from here and must never be
triggered by a test.
"""

import json
import multiprocessing
import signal

import pytest

from zfsbackup.workers import _init_child_process

FORK_AVAILABLE = "fork" in multiprocessing.get_all_start_methods()


def _dummy_handler(signum, frame):  # pragma: no cover - never actually invoked
    pass


def _describe(handler) -> str:
    if handler is signal.SIG_DFL:
        return "SIG_DFL"
    if handler is signal.SIG_IGN:
        return "SIG_IGN"
    return getattr(handler, "__name__", repr(handler))


def _child_report_raw(result_path: str) -> None:
    """The negative control: does NOT call `_init_child_process` at all,
    so whatever this process inherited at fork time is still in effect.
    """
    result = {
        "sigterm": _describe(signal.getsignal(signal.SIGTERM)),
        "sigint": _describe(signal.getsignal(signal.SIGINT)),
    }
    with open(result_path, "w") as fh:
        json.dump(result, fh)


def _child_report_after_init(result_path: str) -> None:
    """Calls the real `_init_child_process` first -- exactly what every
    worker's `run()` does as its own first statement -- then reports.
    """
    _init_child_process(verbose=False)
    result = {
        "sigterm": _describe(signal.getsignal(signal.SIGTERM)),
        "sigint": _describe(signal.getsignal(signal.SIGINT)),
    }
    with open(result_path, "w") as fh:
        json.dump(result, fh)


@pytest.mark.skipif(
    not FORK_AVAILABLE,
    reason="'fork' multiprocessing start method unavailable on this platform",
)
@pytest.mark.integration
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestChildSignalDisposition:
    """Mutation-checked: deleting `_init_child_process`'s two
    `signal.signal(..., signal.SIG_DFL)` calls (leaving only the logging
    setup) fails 1 of the 2 tests in this class --
    `test_init_child_process_resets_to_sig_dfl_under_fork`, whose child
    then reports the inherited `_dummy_handler` instead of `SIG_DFL`. The
    control test is unaffected (it never calls `_init_child_process` at
    all) and correctly keeps passing either way.
    """

    def _install_supervisor_style_handlers(self):
        """Mirrors `BackupDaemon.__init__`'s own
        `signal.signal(SIGINT, self._signal_handler)` /
        `signal.signal(SIGTERM, self._signal_handler)` -- installed in
        THIS (parent) process, before any child forks, so a `fork` child
        inherits them exactly as it would from a real supervisor.
        `tests/conftest.py`'s `_restore_signal_handlers` (autouse) resets
        this process's own dispositions back to whatever they were before
        this test, afterward.
        """
        signal.signal(signal.SIGINT, _dummy_handler)
        signal.signal(signal.SIGTERM, _dummy_handler)

    def test_control_fork_child_inherits_handler_without_reset(self, tmp_path):
        """Proves the harness itself can detect inheritance: without
        `_init_child_process`, a `fork` child's SIGTERM/SIGINT
        dispositions are the SAME callable the parent installed --
        neither `SIG_DFL` nor `SIG_IGN`.
        """
        self._install_supervisor_style_handlers()
        result_path = tmp_path / "raw.json"
        ctx = multiprocessing.get_context("fork")
        proc = ctx.Process(target=_child_report_raw, args=(str(result_path),))
        proc.start()
        proc.join(timeout=10)

        assert proc.exitcode == 0
        result = json.loads(result_path.read_text())
        assert result["sigterm"] == "_dummy_handler"
        assert result["sigint"] == "_dummy_handler"

    def test_init_child_process_resets_to_sig_dfl_under_fork(self, tmp_path):
        self._install_supervisor_style_handlers()
        result_path = tmp_path / "reset.json"
        ctx = multiprocessing.get_context("fork")
        proc = ctx.Process(target=_child_report_after_init, args=(str(result_path),))
        proc.start()
        proc.join(timeout=10)

        assert proc.exitcode == 0
        result = json.loads(result_path.read_text())
        assert result["sigterm"] == "SIG_DFL"
        assert result["sigint"] == "SIG_DFL"

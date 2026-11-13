"""Helpers for tests that build a `RemoteBackupWorker` without running `__init__`.

The serial loop's pair state (`_pairs`, `_dependents`, the expansion flags) is gone: the scheduler
(`sendloop.SendLoop`) owns it. A worker built with `RemoteBackupWorker.__new__(RemoteBackupWorker)` has
class-level defaults for everything it needs and makes its `SendMemory` on first use (`_mem()`);
`_waiting_noted` and `_dry_run_sent` are read-only views of that memory. A `DatasetInfo` double handed to
`send_pair` also needs `expanded_from` (`None` for a configured pair): `configured_dsi()`.

For tests that RUN the worker use `tests/_sendloop_support.py::LoopRig`.
"""

from unittest.mock import MagicMock

from zfsbackup.workers import RemoteBackupWorker


def bare_remote_worker(**attrs):
    """A `RemoteBackupWorker` with no `__init__` run, plus `attrs`."""
    worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
    for name, value in attrs.items():
        setattr(worker, name, value)
    return worker


def configured_dsi(dsi=None):
    """`dsi` (default: a new `MagicMock`) as a CONFIGURED pair: `expanded_from is None`."""
    dsi = MagicMock() if dsi is None else dsi
    dsi.expanded_from = None
    return dsi

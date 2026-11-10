"""Shared state for tests that build a `RemoteBackupWorker` without running `__init__`.

`RemoteBackupWorker.__init__` sets the state of the recursive-dataset (A2) rules: the pairs of the
cycle, the waiting-children bookkeeping, the dry-run set, the expansion flags. A test that builds the
worker with `RemoteBackupWorker.__new__(RemoteBackupWorker)` (to skip the process machinery) must set
the same state: `bare_remote_worker()` does, and `test_zfsbackup_recursive_sends.py::TestBareWorkerHelper`
pins that it matches `__init__`. A `DatasetInfo` double handed to `_process_dataset` also needs
`expanded_from` (`None` for a configured pair): `configured_dsi()`.
"""

from unittest.mock import MagicMock

from zfsbackup.workers import RemoteBackupWorker

#: Instance attributes `RemoteBackupWorker.__init__` sets for recursive datasets, with a factory each.
REMOTE_RECURSIVE_STATE = {
    '_pairs': dict,
    '_dependents': dict,
    '_waiting_noted': set,
    '_dry_run_sent': set,
    '_expansion_failed': dict,
    '_expansion_crashed': lambda: False,
    '_pairs_over_cap_noted': lambda: False,
}


def set_remote_state(worker):
    for name, factory in REMOTE_RECURSIVE_STATE.items():
        setattr(worker, name, factory())
    return worker


def bare_remote_worker(**attrs):
    """A `RemoteBackupWorker` with no `__init__` run, its recursive-dataset state set, plus `attrs`."""
    worker = set_remote_state(RemoteBackupWorker.__new__(RemoteBackupWorker))
    for name, value in attrs.items():
        setattr(worker, name, value)
    return worker


def configured_dsi(dsi=None):
    """`dsi` (default: a new `MagicMock`) as a CONFIGURED pair: `expanded_from is None`."""
    dsi = MagicMock() if dsi is None else dsi
    dsi.expanded_from = None
    return dsi

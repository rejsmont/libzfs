"""Worker doubles for the version-skew tests (S-8).

They live in an importable module (not a test file) so that a worker started under
`spawn` or `forkserver` can unpickle them in the child. Nothing here touches ZFS.

`Probe` wraps `run()`: before the real `run()` it drops `zfsbackup.remote` and
`zfsbackup.api` from `sys.modules` (under `fork` the child inherits whatever the test
session imported, which would make "not imported" meaningless), and when `run()` ends --
by `SystemExit` or otherwise -- it writes `<record_dir>/<name>.json` with the exit-time
answer to "were the lazy modules imported?". A worker that leaves because of version skew
must say `false` for both: the check runs BEFORE those imports.
"""

import json
import os
import sys

from _reload_workers import GatedSnapshotWorker, _ForcedMethodWatch
from zfsbackup.workers import ApiWorker, PruningWorker, RemoteBackupWorker, SnapshotWorker

LAZY_MODULES = ("zfsbackup.remote", "zfsbackup.api")


class Probe:
    """Mixin: records what had been imported when `run()` ended."""

    record_dir = None

    def run(self):
        for module in LAZY_MODULES:
            sys.modules.pop(module, None)
        try:
            super().run()
        finally:
            seen = {module: module in sys.modules for module in LAZY_MODULES}
            path = os.path.join(self.record_dir, f"{self.name}.json")
            with open(path, "w") as fh:
                json.dump(seen, fh)


class ProbeSnapshotWorker(Probe, _ForcedMethodWatch, SnapshotWorker):
    pass


class ProbePruningWorker(Probe, _ForcedMethodWatch, PruningWorker):
    pass


class ProbeRemoteWorker(Probe, _ForcedMethodWatch, RemoteBackupWorker):
    pass


class ProbeApiWorker(Probe, _ForcedMethodWatch, ApiWorker):
    pass


class ProbeGatedSnapshotWorker(Probe, GatedSnapshotWorker):
    """The gated snapshot worker (blocks inside its first dataset until
    `release_path` exists) with the probe: for the "no skew, runs normally" cases."""


class GatedRemoteWorker(_ForcedMethodWatch, RemoteBackupWorker):
    """A remote worker (`drains_for_upgrade`) over the same gated datasets as
    `GatedSnapshotWorker`: `start:<name>`, block until `release_path` exists, `done:<name>`.
    `_before_loop` (which imports `zfsbackup.remote`) does nothing."""

    record_dir = None
    release_path = None
    names = GatedSnapshotWorker.names
    _get_datasets = GatedSnapshotWorker._get_datasets
    _note = GatedSnapshotWorker._note
    _process_dataset = GatedSnapshotWorker._process_dataset

    def _before_loop(self, config, manager):
        pass


PROBES = {
    "snapshot": ProbeSnapshotWorker,
    "pruning": ProbePruningWorker,
    "remote": ProbeRemoteWorker,
    "api": ProbeApiWorker,
}

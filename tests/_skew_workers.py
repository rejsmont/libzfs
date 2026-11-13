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
import time

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


class _GateDsi:
    """Picklable-by-construction stand-in for a configured `DatasetInfo` (only what `SendLoop` reads)."""

    def __init__(self, name, destination):
        from datetime import timedelta
        from types import SimpleNamespace
        self.name = name
        self.expanded_from = None
        self.config = SimpleNamespace(
            remote=[SimpleNamespace(destination=destination, frequency=None)],
            frequency=timedelta(hours=1), recursive=False,
        )


class _GateManager:
    """The scheduler's own `DatasetManager` of `GatedRemoteWorker`: it lists the configured datasets."""

    def __init__(self, names, destination, config):
        self.config = config
        self.datasets = [_GateDsi(n, destination) for n in names]


class GatedRemoteWorker(_ForcedMethodWatch, RemoteBackupWorker):
    """A remote worker (`drains_for_upgrade`) over gated pairs: the REAL scheduler loop and sender threads
    (`sendloop.SendLoop`), each sender's `backup_dataset` doing `start:<name>`, block until `release_path`
    exists, `done:<name>` (appended to `<record_dir>/events`). Three pairs to ONE destination, so the cap of
    two senders per destination leaves the third waiting: a drain must not start it. Every pair is due at
    once (`fraction` 0); `_before_loop` (which imports `zfsbackup.remote`) does nothing; `_new_kit` is the
    one factory the loop builds a sender from."""

    record_dir = None
    release_path = None
    names = ("pool/a", "pool/b", "pool/c")
    destination = "d"
    _note = GatedSnapshotWorker._note

    def run(self):
        from zfsbackup import workers
        from zfsbackup.sendschedule import SendSchedule
        # In THIS process only (a spawned or forked child; the module is never patched in the parent).
        workers.SendSchedule = lambda client_id, check: SendSchedule(
            client_id, check, fraction=lambda *a: 0.0
        )
        super().run()

    def _before_loop(self, config, manager):
        pass

    def _new_manager(self, config):
        return _GateManager(self.names, self.destination, config)

    def _new_kit(self, template, remote_cfg):
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from zfsbackup.sendloop import SenderKit
        worker = self
        manager = MagicMock(name="manager")
        manager.get_own_anchor.return_value = None
        manager.config.dry_run = False
        dsi = SimpleNamespace(name=template.name, config=template.config, expanded_from=None)

        class Sender:
            def backup_dataset(self, dsi, remote_cfg, should_stop=None):
                worker._note(f"start:{dsi.name}")
                deadline = time.monotonic() + 60
                while not os.path.exists(worker.release_path) and time.monotonic() < deadline:
                    time.sleep(0.05)
                worker._note(f"done:{dsi.name}")
                return True

            def last_problem(self, dataset, destination):
                return None

            def last_retryable(self, dataset, destination):
                return False

        return SenderKit(manager, dsi, Sender)


PROBES = {
    "snapshot": ProbeSnapshotWorker,
    "pruning": ProbePruningWorker,
    "remote": ProbeRemoteWorker,
    "api": ProbeApiWorker,
}

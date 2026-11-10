"""Worker doubles for the receive-writer gate's real-process test
(`test_zfsbackup_receive_writer_gate.py::TestRealProcessesRespectTheGate`); not a test module.

They live in an importable module so that a worker started under `spawn` or `forkserver` can unpickle
them in the child. Nothing here touches ZFS, a database or the network: `run()` is replaced.

Every double appends `<event> <time.time()>` lines to `<record_dir>/events` (one `O_APPEND` write each), so
the test can compare the moment the OLD API worker left with the moment the NEW remote worker started.

`LateExitApiWorker`
    Runs until the shared `ReloadEpoch` moves past its `born_epoch` (or the stop flag is raised), keeps
    "draining" for `exit_delay` seconds, then notes `api-exit` and exits `WORKER_EXIT_RELOAD`: an API worker
    that finishes a receive late.
`StartRecordingRemoteWorker`
    Notes `remote-start` at once, then idles until the stop flag is raised or the epoch moves.
"""

import os
import sys
import time

from _config_handoff import _SelectableStartMethod
from zfsbackup.workers import WORKER_EXIT_RELOAD, ApiWorker, RemoteBackupWorker

#: A double that nobody stops must still end: far above any test's own bound.
MAX_LIFE = 60.0


def note(record_dir, event):
    fd = os.open(os.path.join(record_dir, "events"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, f"{event} {time.time():.6f}\n".encode())
    finally:
        os.close(fd)


def read_events(record_dir):
    """-> `[(event, time)]` in file order."""
    path = os.path.join(record_dir, "events")
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as fh:
        for line in fh:
            event, stamp = line.rsplit(" ", 1)
            out.append((event, float(stamp)))
    return out


class LateExitApiWorker(_SelectableStartMethod, ApiWorker):
    record_dir = None
    exit_delay = 1.0

    def run(self):
        note(self.record_dir, f"api-start-{self.born_epoch}")
        deadline = time.monotonic() + MAX_LIFE
        while time.monotonic() < deadline:
            if self.stop_event.is_set():
                return
            if self.reload_epoch.get() != self.born_epoch:
                break
            time.sleep(0.02)
        else:
            return
        time.sleep(self.exit_delay)
        note(self.record_dir, f"api-exit-{self.born_epoch}")
        sys.exit(WORKER_EXIT_RELOAD)


class StartRecordingRemoteWorker(_SelectableStartMethod, RemoteBackupWorker):
    record_dir = None

    def run(self):
        note(self.record_dir, f"remote-start-{self.born_epoch}")
        deadline = time.monotonic() + MAX_LIFE
        while time.monotonic() < deadline:
            if self.stop_event.is_set() or self.reload_epoch.get() != self.born_epoch:
                return
            time.sleep(0.02)


class LateExitRemoteWorker(_SelectableStartMethod, RemoteBackupWorker):
    """A remote worker that is still sending when the reload comes: it notes `remote-start-<epoch>`, waits for
    the epoch to move AND for the file `release_path` to exist, notes `remote-exit-<epoch>` and exits
    `WORKER_EXIT_RELOAD`. The test, not a timer, decides when it leaves."""

    record_dir = None
    release_path = None

    def run(self):
        note(self.record_dir, f"remote-start-{self.born_epoch}")
        deadline = time.monotonic() + MAX_LIFE
        while time.monotonic() < deadline:
            if self.stop_event.is_set():
                return
            if self.reload_epoch.get() != self.born_epoch and os.path.exists(self.release_path):
                break
            time.sleep(0.02)
        else:
            return
        note(self.record_dir, f"remote-exit-{self.born_epoch}")
        sys.exit(WORKER_EXIT_RELOAD)


class StartRecordingApiWorker(_SelectableStartMethod, ApiWorker):
    """Notes `api-start-<epoch>` at once, then idles until the stop flag is raised or the epoch moves."""

    record_dir = None

    def run(self):
        note(self.record_dir, f"api-start-{self.born_epoch}")
        deadline = time.monotonic() + MAX_LIFE
        while time.monotonic() < deadline:
            if self.stop_event.is_set() or self.reload_epoch.get() != self.born_epoch:
                return
            time.sleep(0.02)

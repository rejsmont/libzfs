"""Worker doubles for the live-reload tests.

They live in an importable module (not a test file) so that a worker started
under `spawn` or `forkserver` can unpickle them in the child. Nothing here
touches ZFS: the datasets are plain stand-ins and `_process_dataset` is replaced.

`GatedSnapshotWorker`
    Takes two datasets. For each one it appends `start:<name>`, blocks until a
    `release` file exists, then appends `done:<name>` -- to `<record_dir>/events`.
    The first call also records what the worker's workers-lock descriptor looks
    like (`<record_dir>/lock.json`). It is the "dataset in flight" the reload and
    stop tests interrupt.

`FdProbeWorker`
    Replaces `run`: records how many descriptors on the workers-lock inode exist
    in the child BEFORE the worker takes its own lock (an inherited one would show
    up here), and how many after.
"""

import fcntl
import json
import os
import time

from _config_handoff import _SelectableStartMethod
from zfsbackup.workers import ApiWorker, PruningWorker, SnapshotWorker


class FakeDataset:
    """Picklable stand-in for `DatasetInfo` (only `.name` is used)."""

    def __init__(self, name):
        self.name = name


def descriptors_on(path) -> int:
    """How many of this process's open descriptors refer to `path`'s inode."""
    st = os.stat(path)
    count = 0
    for entry in os.listdir("/dev/fd"):
        try:
            other = os.fstat(int(entry))
        except (OSError, ValueError):
            continue
        if (other.st_dev, other.st_ino) == (st.st_dev, st.st_ino):
            count += 1
    return count


class _ForcedMethodWatch(_SelectableStartMethod):
    """`_arm_supervisor_watch` decides "is my parent the supervisor?" from the
    PROCESS-WIDE start method; these doubles force their own, so tell the watch
    the truth (a forkserver child's parent is the forkserver)."""

    def _arm_supervisor_watch(self):
        super()._arm_supervisor_watch()
        if self.start_method == "forkserver":
            self._direct_child = False


class GatedSnapshotWorker(_ForcedMethodWatch, SnapshotWorker):
    record_dir = None
    release_path = None
    names = ("pool/a", "pool/b")

    def _get_datasets(self, manager):
        return [FakeDataset(n) for n in self.names]

    def _note(self, line):
        fd = os.open(os.path.join(self.record_dir, "events"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (line + "\n").encode())
        finally:
            os.close(fd)

    def _process_dataset(self, manager, dsi):
        self._note(f"start:{dsi.name}")
        lock_json = os.path.join(self.record_dir, "lock.json")
        if not os.path.exists(lock_json):
            fd = self._workers_lock_fd
            with open(lock_json, "w") as fh:
                json.dump(
                    {
                        "fd": fd,
                        "cloexec": bool(fd is not None and fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC),
                    },
                    fh,
                )
        deadline = time.monotonic() + 60
        while not os.path.exists(self.release_path) and time.monotonic() < deadline:
            time.sleep(0.05)
        self._note(f"done:{dsi.name}")


class _SlowEnumeration:
    """Stands in for `DatasetManager` in `SlowEnumerationPruningWorker`: its
    `received_datasets(should_stop)` keeps the real contract (check `should_stop`
    before each dataset, return the partial list once it is true) but each
    "dataset" takes `delay` seconds, so the enumeration lasts `count * delay`.
    Every examined dataset is appended as `enum:<i>` to `<record_dir>/events`."""

    datasets = ()

    def __init__(self, note, count, delay):
        self._note, self._count, self._delay = note, count, delay

    def received_datasets(self, should_stop=None):
        found = []
        for i in range(self._count):
            if should_stop is not None and should_stop():
                self._note("cut")
                return found
            self._note(f"enum:{i}")
            time.sleep(self._delay)
            found.append(FakeDataset(f"recv/{i}"))
        self._note("complete")
        return found


class SlowEnumerationPruningWorker(_ForcedMethodWatch, PruningWorker):
    """The REAL `PruningWorker._get_datasets` over a slow, stubbed enumeration:
    "a stop or a reload is noticed during the listing, not after it". A dataset
    that reaches `_process_dataset` is appended as `pruned:<name>` -- after a
    stop or reload there must be none."""

    record_dir = None
    count = 400
    delay = 0.05

    def _note(self, line):
        fd = os.open(os.path.join(self.record_dir, "events"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (line + "\n").encode())
        finally:
            os.close(fd)

    def _get_datasets(self, manager):
        return PruningWorker._get_datasets(self, _SlowEnumeration(self._note, self.count, self.delay))

    def _process_dataset(self, manager, dsi):
        self._note(f"pruned:{dsi.name}")


class FdProbeWorker(_ForcedMethodWatch, SnapshotWorker):
    record_dir = None
    lock_file = None

    def run(self):
        before = descriptors_on(self.lock_file)
        self._arm_supervisor_watch()
        self._hold_workers_lock()
        after = descriptors_on(self.lock_file)
        with open(os.path.join(self.record_dir, f"probe.{os.getpid()}.json"), "w") as fh:
            json.dump({"before": before, "after": after, "pid": os.getpid()}, fh)


class SelectableApiWorker(_ForcedMethodWatch, ApiWorker):
    """A real, unmodified `ApiWorker.run` in a child started with `start_method`
    (`fork`, `spawn` or `forkserver`): the API worker's TLS tests run the real
    thing, not a double."""

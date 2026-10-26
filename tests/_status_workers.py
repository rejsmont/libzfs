"""Worker doubles for the status-file tests (S-7a); not a test module.

They live in an importable module so that a worker started under `spawn` or
`forkserver` can unpickle them in the child. Nothing here touches ZFS: the
datasets are plain stand-ins, `_before_loop` does nothing and `_process_dataset`
only calls the real `BaseWorker._record`, so the REAL `run()` loop, the REAL
`StatusWriter` (built in the child, loaded from the previous incarnation's file)
and the REAL flush calls are what the tests observe.

NOTE: do not reuse `_config_handoff._SelectableStartMethod` here -- it defines a
`_record` method (its own file recorder) that would shadow `BaseWorker._record`.
"""

import json
import multiprocessing
import os

from zfsbackup import health
from zfsbackup.workers import PruningWorker, SnapshotWorker


class FakeDataset:
    """Picklable stand-in for `DatasetInfo` (only `.name` is used)."""

    def __init__(self, name):
        self.name = name


class _StartMethod:
    """Start this one process with `self.start_method` instead of the process-wide default,
    and tell the supervisor watch the truth about a forkserver child's parent."""

    start_method = None

    @staticmethod
    def _Popen(process_obj):
        ctx = multiprocessing.get_context(process_obj.start_method)
        return ctx.Process._Popen(process_obj)

    def _arm_supervisor_watch(self):
        super()._arm_supervisor_watch()
        if self.start_method == 'forkserver':
            self._direct_child = False


class ScriptedSnapshotWorker(_StartMethod, SnapshotWorker):
    """Records, in each incarnation and for each dataset, ONE scripted outcome (`plan_path`
    is a JSON `{dataset: "ok" | "fail"}`; `fail` is a failure with the text `scripted
    failure`), and nothing more however many cycles follow. So `consecutive_failures`
    after N incarnations is exactly N if the counters were carried over, 1 if not."""

    plan_path = None
    names = ('pool/a', 'pool/b')

    def _before_loop(self, config, manager):
        pass

    def _get_datasets(self, manager):
        return [FakeDataset(n) for n in self.names]

    def _process_dataset(self, manager, dsi):
        done = getattr(self, '_done', None)
        if done is None:
            done = self._done = set()
        if dsi.name in done:
            return
        done.add(dsi.name)
        with open(self.plan_path) as fh:
            plan = json.load(fh)
        if plan.get(dsi.name) == 'fail':
            self._record(health.KIND_SNAPSHOT, dsi.name, ok=False, error='scripted failure')
        else:
            self._record(health.KIND_SNAPSHOT, dsi.name, ok=True, detail=f'pid {os.getpid()}')


class ScriptedPruningWorker(_StartMethod, PruningWorker):
    """As above, for the pruning worker (one `prune` record per dataset)."""

    plan_path = None
    names = ('pool/a',)

    def _before_loop(self, config, manager):
        pass

    def _get_datasets(self, manager):
        return [FakeDataset(n) for n in self.names]

    def _process_dataset(self, manager, dsi):
        done = getattr(self, '_done', None)
        if done is None:
            done = self._done = set()
        if dsi.name in done:
            return
        done.add(dsi.name)
        self._record(health.KIND_PRUNE, dsi.name, ok=True, detail='pruned 0')

"""Remote worker doubles for the REAL-process tests (`test_zfsbackup_remote_worker_processes.py`); not a test
module. They live in an importable module so that a worker started under `spawn` or `forkserver` can unpickle
them in the child. Nothing here touches ZFS, a database or the network.

`ScriptedRemoteWorker` is the REAL `RemoteBackupWorker.run()` (the scheduler loop, the sender threads, the real
status writer, the real exit protocol) over scripted senders: `<plan_path>` is JSON `{dataset: "ok" | "fail" |
"gate"}`. Every dataset is one pair to the destination `d`; a send notes `start:<dataset>` in
`<record_dir>/events`, then

* `ok`: notes `done:<dataset>`, returns True;
* `fail`: returns False (the account is `scripted failure`);
* `gate`: blocks until the file `release_path` exists, IGNORING `should_stop` (a stream in hand does not end
  at a stop: only its next chain step would), then notes `done:<dataset>` and returns True.

Every pair is due at once (`fraction` 0, set in this process only). `_before_loop` imports nothing.
"""

import json
import multiprocessing
import os
import time
from types import SimpleNamespace

from _skew_workers import _GateManager
from zfsbackup.workers import RemoteBackupWorker


class _StartMethod:
    """Start this one process with `self.start_method`, and tell the supervisor watch the truth about a
    forkserver child's parent (see `_status_workers._StartMethod`)."""

    start_method = None

    @staticmethod
    def _Popen(process_obj):
        ctx = multiprocessing.get_context(process_obj.start_method)
        return ctx.Process._Popen(process_obj)

    def _arm_supervisor_watch(self):
        super()._arm_supervisor_watch()
        if self.start_method == 'forkserver':
            self._direct_child = False


class ScriptedRemoteWorker(_StartMethod, RemoteBackupWorker):
    record_dir = None
    release_path = None
    plan_path = None
    destination = 'd'

    def _plan(self):
        with open(self.plan_path) as fh:
            return json.load(fh)

    def _note(self, line):
        fd = os.open(os.path.join(self.record_dir, 'events'), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (line + '\n').encode())
        finally:
            os.close(fd)

    def run(self):
        from zfsbackup import workers
        from zfsbackup.sendschedule import SendSchedule
        workers.SendSchedule = lambda client_id, check: SendSchedule(
            client_id, check, fraction=lambda *a: 0.0)
        super().run()

    def _before_loop(self, config, manager):
        pass

    def _new_manager(self, config):
        return _GateManager(list(self._plan()), self.destination, config)

    def _new_kit(self, template, remote_cfg):
        from unittest.mock import MagicMock
        from zfsbackup.sendloop import SenderKit
        worker = self
        manager = MagicMock(name='manager')
        manager.get_own_anchor.return_value = None
        manager.config.dry_run = False
        dsi = SimpleNamespace(name=template.name, config=template.config, expanded_from=None)

        class Sender:
            def backup_dataset(self, dsi, remote_cfg, should_stop=None):
                action = worker._plan()[dsi.name]
                worker._note(f'start:{dsi.name}')
                if action == 'fail':
                    return False
                if action == 'gate':
                    deadline = time.monotonic() + 120
                    while not os.path.exists(worker.release_path) and time.monotonic() < deadline:
                        time.sleep(0.05)
                worker._note(f'done:{dsi.name}')
                return True

            def last_problem(self, dataset, destination):
                return 'scripted failure'

            def last_retryable(self, dataset, destination):
                return False

        return SenderKit(manager, dsi, Sender)


def supervisor_main(method, record_dir, plan_path, release_path, config, pid_file):
    """The body of a throw-away SUPERVISOR process: starts a `ScriptedRemoteWorker` under `method`, writes the
    worker's pid to `pid_file` and sleeps (the test kills it with SIGKILL)."""
    from zfsbackup.stopflag import ReloadEpoch, StopFlag
    stop, epoch = StopFlag(), ReloadEpoch()
    worker = ScriptedRemoteWorker(config, stop, reload_epoch=epoch, born_epoch=0)
    worker.start_method = method
    worker.record_dir, worker.release_path, worker.plan_path = record_dir, release_path, plan_path
    worker.start()
    with open(pid_file, 'w') as fh:
        fh.write(str(worker.pid))
    time.sleep(600)

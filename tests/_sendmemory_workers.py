"""Worker double for the `SendMemory` process tests (`test_zfsbackup_sendmemory.py`); not a test module.

It lives in an importable module so that a worker started under `spawn` or `forkserver` can unpickle
it in the child. Nothing here touches ZFS, a database or the network: `run()` is replaced by a probe
that reports what the child SEES of the worker's `SendMemory` (what the parent noted before the
start, the aliases of the worker's old attribute names, the lock) and then USES it from several
threads, writing one JSON record.

NOTE: this does not reuse `_config_handoff._SelectableStartMethod`: it defines a `_record` method that
would shadow `BaseWorker._record`.
"""

import json
import multiprocessing
import os
import threading

from zfsbackup.sendmemory import (
    CLOCK, DRY_RUN_SENT, REFUSALS, TOKEN_SKIPS, WAITING, SendMemory,
)
from zfsbackup.workers import RemoteBackupWorker

THREADS = 8
OPS = 300


class _StartMethod:
    """Start this one process with `self.start_method` instead of the process-wide default."""

    start_method = None

    @staticmethod
    def _Popen(process_obj):
        ctx = multiprocessing.get_context(process_obj.start_method)
        return ctx.Process._Popen(process_obj)


def hammer(memory: SendMemory, threads: int = THREADS, ops: int = OPS) -> dict:
    """Use `memory` from `threads` threads at once: -> what the operations returned, summed up."""
    barrier = threading.Barrier(threads)
    wins, counts, errors = [], [], []

    def work():
        try:
            barrier.wait(30)
            mine, seen = [], []
            for i in range(ops):
                if memory.note(CLOCK, ('child', i)):
                    mine.append(i)
                seen.append(memory.incr(TOKEN_SKIPS, 'child'))
            wins.append(mine)
            counts.append(seen)
        except BaseException as e:  # noqa: BLE001 - reported, never lost
            errors.append(repr(e))

    pool = [threading.Thread(target=work) for _ in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join(60)
    flat_wins = sorted(i for mine in wins for i in mine)
    flat_counts = sorted(c for seen in counts for c in seen)
    return {
        'errors': errors,
        'one_winner_per_key': flat_wins == list(range(ops)),
        'counts_are_exact': flat_counts == list(range(1, threads * ops + 1)),
        'final_count': memory.count(TOKEN_SKIPS, 'child'),
    }


class MemoryProbeWorker(_StartMethod, RemoteBackupWorker):
    """Reports what its child sees of the worker's memory, then hammers it from threads."""

    record_path = None

    def run(self):
        mem = self._mem()
        lock_free = mem._lock.acquire(False)       # a recreated, unlocked lock
        if lock_free:
            mem._lock.release()
        manager = self._new_manager(self.config)
        record = {
            'pid': os.getpid(),
            'memory_type': type(mem).__name__,
            'preseeded_latch': mem.noted(CLOCK, ('pool/a', 'snapshot', 's1')),
            'preseeded_value': mem.get(REFUSALS, ('pool/a', 'offsite')),
            'preseeded_count': mem.count(TOKEN_SKIPS, ('pool/a', 'offsite')),
            'preseeded_expiry': mem.expiry_checked_at,
            'expiry_reclaimed_inside_window': mem.claim_expiry_check(100.0, 150.0),
            'waiting_is_the_memorys_latch': self._waiting_noted is mem.latch_view(WAITING),
            'dry_run_is_the_memorys_latch': self._dry_run_sent is mem.latch_view(DRY_RUN_SENT),
            'waiting': sorted(map(list, self._waiting_noted)),
            'lock_was_free': lock_free,
            'manager_shares_it': manager._memory is mem,
            'manager_clock_alias': manager._clock_noted is mem.latch_view(CLOCK),
            'hammer': hammer(mem),
        }
        with open(self.record_path, 'w') as fh:
            json.dump(record, fh)

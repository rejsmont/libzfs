"""Support for the config-handoff tests (item 15.2(b)); not a test module.

`config_signature` flattens a `BackupConfig` to JSON-able data that records
every `Duration`'s *literal* next to its value. `Duration.__eq__` is a plain
`timedelta` comparison and ignores the literal, so `config == other` would
pass for a config whose `60m` had silently become `1h`; the signature does not.

`Recording*Worker` are real `multiprocessing.Process` subclasses defined at
module level so that `spawn` and `forkserver` can import them in the child.
Their `run()` writes the signature of the config they were *handed* to a file
and returns -- no `zfs`, no loop, no database. `start_method` selects the
start method per instance, because `Process` otherwise uses the process-wide
default (which differs by platform and Python version).
"""

import dataclasses
import json
import multiprocessing
import os
from datetime import timedelta
from pathlib import PurePath

from zfsbackup.config.model import Duration
from zfsbackup.workers import ApiWorker, PruningWorker, SnapshotWorker


def config_signature(obj):
    """Recursively reduce `obj` to JSON-able data, literals included."""
    if isinstance(obj, Duration):
        return ["D", obj.total_seconds(), obj.literal]
    if isinstance(obj, timedelta):
        return ["T", obj.total_seconds()]
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: config_signature(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
    if isinstance(obj, dict):
        return {str(k): config_signature(v) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))}
    if isinstance(obj, (list, tuple)):
        return [config_signature(v) for v in obj]
    if isinstance(obj, PurePath):
        return str(obj)
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    return repr(obj)


class DbTouched(BaseException):
    """Raised by the `forbid_db` fixture (conftest) when anything tries to
    open the config database. A `BaseException` so no `except Exception` arm
    in the code under test can swallow the evidence."""


def literals_in(signature):
    """Every Duration literal found anywhere in a signature."""
    found = []
    if isinstance(signature, list):
        if len(signature) == 3 and signature[0] == "D":
            found.append(signature[2])
        else:
            for item in signature:
                found.extend(literals_in(item))
    elif isinstance(signature, dict):
        for item in signature.values():
            found.extend(literals_in(item))
    return found


class _SelectableStartMethod:
    """Mixin: start this one process with `self.start_method` instead of the
    process-wide default."""

    start_method = None

    @staticmethod
    def _Popen(process_obj):
        ctx = multiprocessing.get_context(process_obj.start_method)
        return ctx.Process._Popen(process_obj)

    def _record(self):
        path = os.path.join(self.record_dir, f"{self.name}.{os.getpid()}.json")
        with open(path, "w") as fh:
            json.dump(
                {
                    "pid": os.getpid(),
                    "config_id": id(self.config),
                    "signature": config_signature(self.config),
                    "type": type(self.config).__name__,
                },
                fh,
            )


class RecordingSnapshotWorker(_SelectableStartMethod, SnapshotWorker):
    record_dir = None

    def run(self):
        self._record()


class RecordingPruningWorker(_SelectableStartMethod, PruningWorker):
    record_dir = None

    def run(self):
        self._record()


class RecordingApiWorker(_SelectableStartMethod, ApiWorker):
    record_dir = None

    def run(self):
        self._record()


class ForkedSnapshotWorker(_SelectableStartMethod, SnapshotWorker):
    """A real, unmodified `SnapshotWorker.run` in a child started with
    `start_method`."""


def read_records(record_dir):
    out = []
    for name in sorted(os.listdir(record_dir)):
        with open(os.path.join(record_dir, name)) as fh:
            out.append(json.load(fh))
    return out

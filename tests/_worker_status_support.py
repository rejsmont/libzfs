"""Worker status files (`<db>.status/<worker>.json`, S-7) for the `zfsbackup-config`
tests (`check`, `status`).

Files are written through the real `zfsbackup.health` code -- `ensure_status_dir`
(mode 0750) and `write_status_file` (atomic, mode 0640) -- with the document built
by hand so a test can say any timestamp, pid and counter (the daemon-side
`StatusWriter` stamps the clock itself). Importable from any test module.
"""

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

from zfsbackup import health

#: A pid that is alive for the whole test (the one running it).
LIVE_PID = os.getpid()


def iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def rec(kind: str, dataset: str, destination: Optional[str] = None, *, failures: int = 0,
        result: Optional[str] = "default", attempt: Optional[float] = "default",
        success: Optional[float] = None, error: Optional[str] = None,
        error_at: Optional[float] = None, detail: Optional[str] = None) -> Dict:
    """One record in the file's JSON form. `result="default"` is `error` when
    `failures` > 0 else `ok`; `attempt="default"` is a minute ago. Keys with no
    value are left out, as the writer does."""
    now = time.time()
    if result == "default":
        result = "error" if failures else "ok"
    if attempt == "default":
        attempt = now - 60
    out: Dict = {"kind": kind, "dataset": dataset}
    if destination is not None:
        out["destination"] = destination
    if result is not None:
        out["result"] = result
    if attempt is not None:
        out["last_attempt"] = iso(attempt)
    if success is not None:
        out["last_success"] = iso(success)
    if failures and error is None:
        error = "boom"
    if error is not None:
        out["last_error"] = error
        out["last_error_at"] = iso(error_at if error_at is not None else (attempt or now - 60))
    if detail is not None:
        out["detail"] = detail
    out["consecutive_failures"] = failures
    return out


def status_dir(database: Path) -> Path:
    return health.status_dir(database)


def make_dir(database: Path) -> Path:
    """The (real) status directory, 0750, created the way the supervisor does."""
    directory = status_dir(database)
    health.ensure_status_dir(directory)
    return directory


def write_worker(database: Path, worker: str, records: List[Dict], *, pid="live",
                 updated_at="now", started_at="default", schema: int = 1) -> Path:
    """Write `<db>.status/<worker>.json` (creating the directory). `pid="live"` is
    this process; `None` leaves the key out. `updated_at="now"` is a minute ago;
    `None` leaves it out."""
    directory = make_dir(database)
    now = time.time()
    doc: Dict = {"schema": schema, "worker": worker}
    if pid == "live":
        pid = LIVE_PID
    if pid is not None:
        doc["pid"] = pid
    if started_at == "default":
        started_at = now - 3600
    if started_at is not None:
        doc["started_at"] = iso(started_at)
    if updated_at == "now":
        updated_at = now - 60
    if updated_at is not None:
        doc["updated_at"] = iso(updated_at)
    doc["records"] = records
    health.write_status_file(directory, worker, json.dumps(doc) + "\n")
    return directory / f"{worker}.json"

"""A scripted stand-in for `$EDITOR`, used by the `edit` tests. Never interactive.

Invoked as ``python _editor_stub.py [extra args...] BUFFER`` (the buffer path is
always the last argument, as the real editors get it). What it does is read from
a JSON *plan*: the file named by ``$ZFSBACKUP_STUB_PLAN``, a list with one
action list per invocation (the last entry repeats for any further invocation,
the invocation number is counted in ``<plan>.count``). Every invocation appends
one JSON line to ``<plan>.log`` describing what it saw:

    {"n": 1, "argv": [...], "buffer": "...", "dir_mode": 448, "file_mode": 384,
     "before": "<buffer text>", "sidecars": [...], "env": {...}}

``sidecars`` lists the ``-wal`` / ``-shm`` / ``-journal`` files that exist next
to ``$ZFSBACKUP_STUB_DB`` (if set) at that moment: a database connection left
open while the editor runs would show up there.

Actions (applied in order, each a dict with one key):

- ``{"replace": [old, new]}``     replace the first ``old`` (assert it is there)
- ``{"replace_all": [old, new]}`` replace every ``old`` (assert it is there)
- ``{"append": text}``            append text to the buffer
- ``{"write": text}``             replace the buffer with text
- ``{"write_bytes_hex": hex}``    replace the buffer with raw bytes
- ``{"remove": true}``            delete the buffer file
- ``{"cli": [args...]}``          run ``python -m zfsbackup.cli args...`` (a
                                  concurrent writer) and require exit 0
- ``{"exit": n}``                 exit with status n (after the actions so far
                                  are written)
- ``{"kill": sig}``               kill itself with signal ``sig``

With an empty action list the buffer is left exactly as it was.
"""

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def main() -> None:
    buffer = Path(sys.argv[-1])
    plan_path = Path(os.environ["ZFSBACKUP_STUB_PLAN"])
    plan = json.loads(plan_path.read_text())
    counter = Path(str(plan_path) + ".count")
    n = int(counter.read_text()) + 1 if counter.exists() else 1
    counter.write_text(str(n))
    actions = plan[min(n, len(plan)) - 1]

    db = os.environ.get("ZFSBACKUP_STUB_DB")
    sidecars = []
    if db:
        sidecars = sorted(
            Path(db + suffix).name for suffix in ("-wal", "-shm", "-journal")
            if Path(db + suffix).exists()
        )
    text = buffer.read_text(encoding="utf-8", errors="replace")
    record = {
        "n": n,
        "argv": sys.argv[1:],
        "buffer": str(buffer),
        "dir_mode": buffer.parent.stat().st_mode & 0o7777,
        "file_mode": buffer.stat().st_mode & 0o7777,
        "before": text,
        "sidecars": sidecars,
    }
    with open(str(plan_path) + ".log", "a", encoding="utf-8") as log:
        log.write(json.dumps(record) + "\n")

    for action in actions:
        (kind, arg), = action.items()
        if kind == "replace":
            old, new = arg
            data = buffer.read_text(encoding="utf-8")
            assert old in data, f"stub: {old!r} not in the buffer:\n{data}"
            buffer.write_text(data.replace(old, new, 1), encoding="utf-8")
        elif kind == "replace_all":
            old, new = arg
            data = buffer.read_text(encoding="utf-8")
            assert old in data, f"stub: {old!r} not in the buffer:\n{data}"
            buffer.write_text(data.replace(old, new), encoding="utf-8")
        elif kind == "append":
            with open(buffer, "a", encoding="utf-8") as handle:
                handle.write(arg)
        elif kind == "write":
            buffer.write_text(arg, encoding="utf-8")
        elif kind == "write_bytes_hex":
            buffer.write_bytes(bytes.fromhex(arg))
        elif kind == "remove":
            buffer.unlink()
        elif kind == "cli":
            env = dict(os.environ, PYTHONPATH=str(REPO))
            done = subprocess.run(
                [sys.executable, "-m", "zfsbackup.cli", *arg],
                capture_output=True, text=True, env=env, cwd=str(REPO),
            )
            assert done.returncode == 0, f"stub cli {arg} failed: {done.stdout}{done.stderr}"
        elif kind == "exit":
            sys.exit(arg)
        elif kind == "kill":
            os.kill(os.getpid(), arg)
            signal.pause()
        else:
            raise AssertionError(f"unknown stub action {kind!r}")


if __name__ == "__main__":
    main()

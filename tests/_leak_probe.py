"""Run by `test_orphan_safety_net.py` in a NESTED pytest session (it is not
collected by the normal run: the name does not match `test_*.py`). Leaks orphans on
purpose, to prove the session-end net notices them. Which one is chosen by
`$LEAK_PROBE`: `exec` (an exec'd child, found by the session marker in its
environment) or `fork` (forked WITHOUT exec: it has pytest's exec-time environment,
so only the at-fork record can find it)."""

import os
import subprocess
import sys
import time


def test_leaks_an_orphan():
    mode = os.environ.get("LEAK_PROBE", "exec")
    if mode == "exec":
        code = "import os, time\nif os.fork() == 0:\n    time.sleep(120)\nos._exit(0)\n"
        subprocess.run([sys.executable, "-c", code], check=True, timeout=30)
    else:
        first = os.fork()
        if first == 0:
            if os.fork() == 0:      # the grandchild: forked, never exec'd, outlives everyone
                null = os.open(os.devnull, os.O_RDWR)
                for fd in (0, 1, 2):            # do not hold the parent's pipes open
                    os.dup2(null, fd)
                time.sleep(120)
            os._exit(0)
        os.waitpid(first, 0)

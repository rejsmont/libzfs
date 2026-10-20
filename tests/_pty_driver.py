"""Run a command on a real pseudo-terminal, answer one prompt, report what it printed.

A helper PROCESS for tests (`python tests/_pty_driver.py PROMPT SEND TIMEOUT -- CMD...`):
it is single-threaded, so `pty.fork()` is safe here although the pytest process (which
has server threads) must not fork. It waits until PROMPT (text) appears in the output,
sends SEND (text, `\\n` appended) and waits for the command to exit. Prints one JSON
object on stdout: ``{"output": <base64 of everything the terminal showed>, "status":
<exit code>, "prompted": <bool>}``. The child inherits this process's environment (the
test passes a hermetic one). A command that outlives TIMEOUT is killed.
"""

import base64
import json
import os
import select
import signal
import sys
import time


def main() -> int:
    prompt, send, timeout = sys.argv[1].encode(), sys.argv[2].encode(), float(sys.argv[3])
    cmd = sys.argv[sys.argv.index("--") + 1:]
    import pty

    pid, fd = pty.fork()
    if pid == 0:
        os.execv(cmd[0], cmd)
    output = b""
    sent = False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ready, _, _ = select.select([fd], [], [], 0.2)
        if ready:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            output += chunk
            if prompt in output and not sent:
                time.sleep(0.2)
                os.write(fd, send + b"\n")
                sent = True
    else:
        os.kill(pid, signal.SIGKILL)
    _, status = os.waitpid(pid, 0)
    sys.stdout.write(json.dumps({
        "output": base64.b64encode(output).decode(),
        "status": os.waitstatus_to_exitcode(status),
        "prompted": sent,
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())

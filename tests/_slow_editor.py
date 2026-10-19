"""A stand-in `$EDITOR` for the signal tests: it announces itself in the log
named by ``$SLOW_LOG`` (pid and the dispositions of SIGINT / SIGQUIT / SIGTERM
it inherited), then behaves according to ``$SLOW_MODE``:

- ``default``       sleeps ``$SLOW_SECONDS`` (default 30), appends a comment to
                    the buffer (the last argument) and exits 0, logging
                    ``exit normally``;
- ``handle_exit``   logs ``got SIGTERM`` and exits 0 when it receives SIGTERM;
- ``ignore_term``   logs ``got SIGTERM`` and carries on (only SIGKILL stops it);
- ``sigint_self``   dies of SIGINT straight away (as an editor does when it
                    sees ^C and gives up);
- ``report``        logs the inherited dispositions and exits 0 at once.

Every log line is flushed as it is written: the tests poll the file.
"""

import os
import signal
import sys
import time

log = open(os.environ["SLOW_LOG"], "a", buffering=1)
mode = os.environ.get("SLOW_MODE", "default")
seconds = float(os.environ.get("SLOW_SECONDS", "30"))


def disposition(signum):
    handler = signal.getsignal(signum)
    if handler == signal.SIG_IGN:
        return "SIG_IGN"
    if handler == signal.SIG_DFL:
        return "SIG_DFL"
    return "handler"


log.write(
    f"start pid={os.getpid()} int={disposition(signal.SIGINT)} "
    f"quit={disposition(signal.SIGQUIT)} term={disposition(signal.SIGTERM)}\n"
)

if mode == "report":
    sys.exit(0)

if mode == "sigint_self":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    os.kill(os.getpid(), signal.SIGINT)
    time.sleep(10)

if mode == "handle_exit":
    def on_term(signum, frame):
        log.write("got SIGTERM\n")
        os._exit(0)

    signal.signal(signal.SIGTERM, on_term)
elif mode == "ignore_term":
    def on_term(signum, frame):
        log.write("got SIGTERM\n")

    signal.signal(signal.SIGTERM, on_term)

deadline = time.monotonic() + seconds
while time.monotonic() < deadline:
    time.sleep(0.05)

with open(sys.argv[-1], "a", encoding="utf-8") as buffer:
    buffer.write("# edited\n")
log.write("exit normally\n")

"""Console-script entry point of the daemon (`zfsbackupd`).

A console script imports `zfsbackup.daemon` as an ordinary module, so the
`if __name__ == '__main__'` SIGHUP guard at the top of `daemon.py` never runs
there. SIGHUP's default action is terminate, and the daemon's imports take a
few hundred milliseconds before its own startup handler is installed; a
`systemctl reload` in that window would kill it (exit 129). So this module,
which is tiny and imports nothing heavy, ignores SIGHUP FIRST and only then
imports the daemon. `daemon._process_main` puts the same disposition back on
the way out, exactly as `python -m zfsbackup.daemon` does.

Keep every import except `signal` inside `main()`.
"""

import signal


def main() -> int:
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    from zfsbackup import daemon
    return daemon._process_main()

"""Logging setup for the supervisor and every worker process (stdlib only).

`configure(level)` is the one place that installs the root handler. It is
called from `daemon.main()` and from every worker's `_init_child_process`, so a
`fork`, `spawn` or `forkserver` child logs exactly like the supervisor: a child
does not rely on inheriting anything. Importing `zfsbackup.daemon` or
`zfsbackup.workers` configures nothing.

**Where the lines go.** Standard output, as before. Under systemd with
`StandardOutput=journal` (the default for a service) the journal reads that
stream, and a line that begins with `<N>` (the sd-daemon(3) priority prefix, `N`
being a syslog priority) is stored with that priority, which is what makes
`journalctl -p err` find the errors. The prefix is only wanted there: anywhere
else it is noise.

**Journal mode is decided by identity, not by an environment variable.**
`$JOURNAL_STREAM` is `<device>:<inode>` of the journal stream systemd connected
to the service's stdout/stderr, and it is inherited by every child. The mode is
on only when it equals `fstat` of the handler's own stream, so a process whose
stdout was redirected elsewhere (a file, a pipe, a terminal) does not emit
prefixes just because the variable is set. In journal mode the timestamp is
dropped (the journal has its own) and EVERY line of a record carries the prefix,
including the continuation lines of a traceback or a multi-line message: a line
without one is stored at the default priority (info).

**The contract for the code that logs (S-3, S-12 and the rest).** Anything an
operator should see in `journalctl -p warning` is emitted through `logging` at
WARNING (or ERROR): never `print`, never a direct write to stdout/stderr, and
never through the werkzeug access logger. The level of a record is its priority;
there is no other channel. Messages that are about one peer and must be
throttled use the dedicated logger `PEER_LOGGER` (`zfsbackup.api.peer`), so an
operator can filter them by name without losing their priority. The API
handler's own per-request access line is demoted by status class (1xx-3xx DEBUG,
4xx INFO, 5xx WARNING) in `workers._api_server_class`; that demotion applies to
werkzeug's request line only, not to messages sent to `PEER_LOGGER`.
"""

import logging
import os
import sys
from typing import Mapping, Optional, TextIO, Union

#: The environment variable systemd sets: `<st_dev>:<st_ino>` of the journal
#: stream behind stdout/stderr.
JOURNAL_STREAM_ENV = 'JOURNAL_STREAM'

#: Reserved for per-peer, throttled messages (see the module docstring).
PEER_LOGGER = 'zfsbackup.api.peer'

#: The `--log-level` choices, finest first.
LEVEL_NAMES = ('debug', 'info', 'warning', 'error')

_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
}

_PLAIN_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_JOURNAL_FORMAT = '%(name)s - %(levelname)s - %(message)s'


def resolve_level(log_level: Optional[str] = None, verbose: bool = False) -> int:
    """The numeric level for `--log-level` (a name or `None`) and `-v`.

    `-v` means `--log-level debug`; given together, the finer of the two wins.
    Raises `ValueError` for a name that is not in `LEVEL_NAMES`."""
    level = logging.INFO
    if log_level is not None:
        try:
            level = _LEVELS[str(log_level).lower()]
        except KeyError:
            raise ValueError(
                f"unknown log level {log_level!r} (expected one of {', '.join(LEVEL_NAMES)})"
            ) from None
    if verbose:
        level = min(level, logging.DEBUG)
    return level


def syslog_priority(levelno: int) -> int:
    """The sd-daemon(3) priority (a syslog severity) for a logging level."""
    if levelno >= logging.CRITICAL:
        return 2
    if levelno >= logging.ERROR:
        return 3
    if levelno >= logging.WARNING:
        return 4
    if levelno >= logging.INFO:
        return 6
    return 7


def journal_stream_matches(
    stream: TextIO, environ: Optional[Mapping[str, str]] = None
) -> bool:
    """Is `stream` the journal stream `$JOURNAL_STREAM` names? False if the
    variable is unset or malformed, or the stream has no file descriptor or
    another `dev:ino`."""
    env = os.environ if environ is None else environ
    value = env.get(JOURNAL_STREAM_ENV)
    if not value:
        return False
    dev, sep, ino = value.partition(':')
    if not sep or not dev.isdigit() or not ino.isdigit():
        return False
    try:
        st = os.fstat(stream.fileno())
    except (AttributeError, OSError, ValueError):  # incl. io.UnsupportedOperation
        return False
    return (st.st_dev, st.st_ino) == (int(dev), int(ino))


#: journald cuts a stdout stream into records at `\n`, at NUL and at its
#: `LineMax` (48 KiB by default), and reads a `<N>` priority prefix at the start
#: of EACH piece. Text a peer controls could otherwise begin a piece with
#: `<2>` and be stored as a critical record. So NUL is never emitted, and a line
#: is cut well below `LineMax` (counted in characters: at most 4 bytes each in
#: UTF-8, so 8192 characters stay under 32 KiB) with the real prefix repeated
#: on every piece.
JOURNAL_WRAP = 8192


def _no_nul(text: str) -> str:
    return text.replace('\0', '\\0')


class PlainFormatter(logging.Formatter):
    """The usual timestamped format, with NUL escaped: when journald is
    reading the stream without `$JOURNAL_STREAM` having told us, it still
    splits at NUL and parses a leading `<N>` of the next piece."""

    def __init__(self) -> None:
        super().__init__(_PLAIN_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        return _no_nul(super().format(record))


class JournalFormatter(logging.Formatter):
    """No timestamp, and the `<N>` priority prefix on every line of the
    formatted record (message, exception text and stack included). NUL is
    escaped and a line longer than `JOURNAL_WRAP` characters is cut into
    pieces that each carry the prefix (see `JOURNAL_WRAP`)."""

    def __init__(self) -> None:
        super().__init__(_JOURNAL_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        prefix = f'<{syslog_priority(record.levelno)}>'
        pieces = []
        for line in _no_nul(super().format(record)).split('\n'):
            for start in range(0, max(len(line), 1), JOURNAL_WRAP):
                pieces.append(prefix + line[start:start + JOURNAL_WRAP])
        return '\n'.join(pieces)


class _ZfsbackupHandler(logging.StreamHandler):
    """Marks the handler `configure` installed, so a second call replaces it
    (and only it) instead of stacking another."""


def configure(level: int = logging.INFO, stream: Optional[TextIO] = None) -> None:
    """Make the root logger write to `stream` (default `sys.stdout`) at
    `level`, in journal format when `stream` is the journal stream (module
    docstring). Idempotent: a handler from an earlier call, including one a
    forked child inherited, is replaced; handlers someone else added are left
    alone."""
    target = sys.stdout if stream is None else stream
    handler = _ZfsbackupHandler(target)
    handler.setFormatter(
        JournalFormatter() if journal_stream_matches(target) else PlainFormatter()
    )
    root = logging.getLogger()
    for old in [h for h in root.handlers if isinstance(h, _ZfsbackupHandler)]:
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(level)
    # werkzeug's `_log` sets its logger to INFO when the level is NOTSET, which
    # would let its INFO lines through under `--log-level warning` and hide
    # its DEBUG ones under `-v`. Pin it to the configured level.
    logging.getLogger('werkzeug').setLevel(level)

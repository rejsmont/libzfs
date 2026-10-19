"""Minimal systemd readiness protocol (`sd_notify(3)`), standard library only.

The supervisor reports READY / RELOADING / STOPPING / STATUS / EXTEND_TIMEOUT_USEC
over the datagram socket named by `$NOTIFY_SOCKET`, so that a `Type=notify` (or
`Type=notify-reload`) unit knows when the daemon is up and when a reload has
been decided. With no `$NOTIFY_SOCKET` everything here is a no-op.

Rules this module keeps, and the daemon relies on:

- `Notifier.from_environ` reads `$NOTIFY_SOCKET` **once** and REMOVES it from
  the environment. Call it before the first fork, forkserver start or `zfs`
  exec: workers and every child process then never see the socket (and, with
  `NotifyAccess=main`, systemd would ignore them anyway).
- Every message is its own `AF_UNIX`/`SOCK_DGRAM` socket (close-on-exec),
  opened and closed inside `send`. No descriptor outlives a call, so none can
  survive a fork.
- `send` never raises. The first failure is logged at WARNING, later ones at
  DEBUG; the daemon works the same with an unreachable socket.
- Nothing here is async-signal-safe: never call it from a signal handler.
"""

import logging
import os
import socket
import time
from typing import Mapping, Optional

logger = logging.getLogger(__name__)

#: The environment variable systemd sets for `Type=notify` services.
NOTIFY_SOCKET_ENV = 'NOTIFY_SOCKET'

#: Per-send socket timeout (seconds): a full receive queue must not wedge the
#: supervisor, which is single-threaded.
_SEND_TIMEOUT = 1.0


def _decode_address(raw: str) -> Optional[str]:
    """`$NOTIFY_SOCKET` as a `socket` address: an absolute path stays as is,
    a leading '@' (Linux abstract namespace) becomes a leading NUL. Anything
    else is not an address systemd would have set, and is ignored."""
    if raw.startswith('/'):
        return raw
    if raw.startswith('@') and len(raw) > 1:
        return '\0' + raw[1:]
    return None


def _clean(text: str) -> str:
    """One protocol line: no newlines or NULs."""
    return text.replace('\r', ' ').replace('\n', ' ').replace('\0', ' ')


class Notifier:
    """Sends `sd_notify` messages to one address, or does nothing."""

    def __init__(self, address: Optional[str] = None) -> None:
        #: Decoded address (NUL-prefixed for the abstract namespace), or None.
        self._address = address
        self._failure_logged = False

    @classmethod
    def from_environ(cls, environ: Optional[Mapping[str, str]] = None) -> 'Notifier':
        """Read `$NOTIFY_SOCKET` and remove it from `os.environ`, even when
        the value turns out to be unusable. `environ` (a plain mapping, left
        untouched) is a test seam that replaces `os.environ` for the read.
        """
        if environ is None:
            raw = os.environ.pop(NOTIFY_SOCKET_ENV, None)
        else:
            raw = environ.get(NOTIFY_SOCKET_ENV)
        if not raw:
            return cls(None)
        address = _decode_address(raw)
        if address is None:
            logger.warning(
                f"ignoring {NOTIFY_SOCKET_ENV}={raw!r}: not an absolute path or "
                "an '@' abstract name"
            )
            return cls(None)
        return cls(address)

    @property
    def enabled(self) -> bool:
        return self._address is not None

    def send(self, *lines: str) -> bool:
        """Send `lines` as one datagram. True if sent; never raises."""
        if self._address is None:
            return False
        payload = '\n'.join(_clean(line) for line in lines if line).encode('utf-8', 'replace')
        if not payload:
            return False
        flags = getattr(socket, 'SOCK_CLOEXEC', 0)
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM | flags)
        except Exception as e:
            self._failed(e)
            return False
        try:
            sock.set_inheritable(False)
            sock.settimeout(_SEND_TIMEOUT)
            sock.sendto(payload, self._address)
            return True
        except Exception as e:
            self._failed(e)
            return False
        finally:
            try:
                sock.close()
            except Exception:  # pragma: no cover - close() does not fail on a socket
                pass

    def _failed(self, exc: BaseException) -> None:
        if not self._failure_logged:
            self._failure_logged = True
            logger.warning(
                f"cannot notify systemd via {NOTIFY_SOCKET_ENV} "
                f"({type(exc).__name__}: {exc}); further failures are logged at debug"
            )
        else:
            logger.debug(f"cannot notify systemd: {exc}")

    # -- the messages the daemon sends ------------------------------------

    def ready(self, status: Optional[str] = None) -> bool:
        return self.send('READY=1', f'STATUS={status}' if status else '')

    def reloading(self) -> bool:
        """`RELOADING=1` with the `MONOTONIC_USEC` that `Type=notify-reload`
        requires (CLOCK_MONOTONIC, microseconds). Must be followed by
        `ready()` once the reload is decided."""
        return self.send('RELOADING=1', f'MONOTONIC_USEC={time.monotonic_ns() // 1000}')

    def stopping(self) -> bool:
        return self.send('STOPPING=1')

    def status(self, text: str) -> bool:
        return self.send(f'STATUS={text}')

    def extend_timeout(self, usec: int, status: Optional[str] = None) -> bool:
        """Ask systemd to move the current start/stop/reload timeout to
        `usec` microseconds from now."""
        return self.send(f'EXTEND_TIMEOUT_USEC={int(usec)}', f'STATUS={status}' if status else '')

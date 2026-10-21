"""The remote-backup wire protocol version (plan S-5).

One place for the constants the server (`zfsbackup.api`), the client
(`zfsbackup.remote`) and the CLI (`zfsbackup.cli.enroll`) share, so none of
them has to import another for it. Standard library only and no imports from
the package: the CLI imports it, and it must never pull in the daemon.

**The contract.**

- Every request a client makes -- HTTP and the websocket upgrade of
  `/stream` -- carries `X-Zfsbackup-Protocol: <PROTOCOL_VERSION>`.
- The server answers a request without the header, or with a version it does
  not support, with **426** `{"error", "reason": "protocol", "protocol":
  <server version>, "supported": [...]}` (and the header itself on the
  response). Only `/health` is exempt, so a liveness probe needs no
  knowledge of the protocol.
- `GET /whoami` answers `"protocol": <server version>`, and a client refuses
  a server whose `/whoami` lacks it or names a version it does not support.

Bump `PROTOCOL_VERSION` for any change that an older peer would misread
(a new required field, a changed stream format, a new send flag). A server
that can still serve older clients lists their versions in
`SUPPORTED_PROTOCOLS`.
"""

import re
from typing import Optional

#: The protocol this code speaks.
PROTOCOL_VERSION = 1

#: The versions a server accepts and a client accepts from a server.
SUPPORTED_PROTOCOLS = frozenset({PROTOCOL_VERSION})

#: The request (and 426 response) header that carries the version.
PROTOCOL_HEADER = 'X-Zfsbackup-Protocol'

#: The `reason` of the server's 426 body.
PROTOCOL_REFUSAL_REASON = 'protocol'

_VERSION_RE = re.compile(r'[0-9]{1,6}')


def parse_protocol_header(value) -> Optional[int]:
    """The version a `X-Zfsbackup-Protocol` header value names, or `None` if
    it is missing or not a plain decimal number (surrounding blanks allowed;
    a sign, a fraction or anything else is refused)."""
    if not isinstance(value, str):
        return None
    text = value.strip(' \t')
    if not _VERSION_RE.fullmatch(text):
        return None
    return int(text)


def protocol_headers() -> dict:
    """`{PROTOCOL_HEADER: str(PROTOCOL_VERSION)}`, for a request."""
    return {PROTOCOL_HEADER: str(PROTOCOL_VERSION)}

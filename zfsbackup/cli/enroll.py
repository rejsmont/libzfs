"""`zfsbackup-config enroll` and `zfsbackup-config identity rotate` (TLS/mTLS
plan M13 and M14c): the client side of trust.

Commands (both take `-c/--config` and `-v/--verbose`, before or after the
subcommand like every other command)::

    enroll DESTINATION [--url URL] [--label L]
                       [--token-file PATH | --token - | <hidden prompt>]
    identity rotate [--cert NEW --key NEW]

Both talk to the server over a pinned TLS connection (`identity.pinned_session`:
the server's certificate fingerprint is checked before a single HTTP byte is
sent) and reach the database only through `write.py` / the store's public API.

enroll
------
1. The token is read (never echoed, never put on a command line) and parsed;
   a new destination needs `--url` (https only). Anything wrong with the
   inputs is found before anything is written or generated.
2. This host's client TLS identity is loaded -- or, for the default
   `<db>.tls/client.*` paths only, generated. Configured paths
   (`client_cert_file`) are never generated into.
3. `ensure_client_id` makes this host's client id if it has none. That is a
   committed config write (the generation moves by one), so the snapshot is
   re-read afterwards.
4. `GET /whoami` with the client certificate, against the stored pin and the
   token's server fingerprint: if the server already trusts this certificate
   the token is NOT spent -- the run goes straight to step 6 (idempotent, also
   after a failed step 6). A server that presents neither fingerprint, or a
   token for another server than the one that trusts us, is refused.
5. `POST /enroll` WITHOUT a client certificate, on a connection pinned to the
   token's fingerprint (a mismatch refuses before the token is sent).
6. `commit_config` stores the destination's `url` and `server_fingerprint`
   (compare-and-swap on the generation from step 3).

`begin_reload` runs exactly once, after the LAST committed write (step 3
and/or step 6), before any output -- on the failure path too, if step 3
committed and a later step failed.

identity rotate
---------------
A successor certificate (`client.next.*`, or the operator's `--cert/--key`) is
registered with EVERY enrolled destination through `POST /rotate`,
authenticated with the CURRENT certificate. Only if every destination answered
`rotated` or `already` are the files swapped (`promote_identity`; or, for
configured paths, `client_cert_file`/`client_key_file` are updated with one
`commit_config`). A partial success swaps nothing: re-running reuses the same
successor and the servers that already accepted answer `already`.

Wire protocol (plan S-5). A 200 from `/enroll` or `/rotate` whose `protocol` is
missing or unsupported means an old or incompatible server that has ALREADY
changed state. The local success path runs unchanged (destination recorded,
client key promoted, certificate swapped) and the command then exits 1 with a
message saying the server accepted but will refuse backups.

Exit codes: 0 ok; 1 refused, pin mismatch, bad token, `client_id_taken`,
`cert_bound_elsewhere`, other 4xx; 2 usage; 75 unreachable, timeout, 503 with
`retry`, conflict or busy; 78 unusable database.

Secrets. The token appears only in the request body. Server error texts are
sanitised (control characters removed, the token redacted) before they are
shown, and exceptions from the HTTP stack are replaced by our own wording.
"""

import base64
import copy
import os
import re
import ssl
import stat
import sys
import time
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlsplit

import click

from zfsbackup import identity
from zfsbackup.cli import tlsfiles
from zfsbackup.cli.errors import EXIT_BUSY, CliError
from zfsbackup.cli.write import (
    begin_reload,
    commit_config,
    notify_daemon,
    read_snapshot,
    release_database,
    where_flag,
)
from zfsbackup.config import BackupConfig, Destination, destination_url_problem
from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    ensure_client_id,
    validate_config,
)
from zfsbackup.protocol import (
    PROTOCOL_REFUSAL_REASON,
    PROTOCOL_VERSION,
    SUPPORTED_PROTOCOLS,
    protocol_headers,
)

__all__ = ["register", "enroll", "identity_rotate"]

#: Connect timeout and read timeout for every request.
_TIMEOUT = (10.0, 30.0)
#: A token is ~110 characters; anything this large is not one.
_MAX_TOKEN_BYTES = 4096
#: The server's own limit on a label.
_MAX_LABEL = 128
#: Longest server-supplied text shown to the operator.
_MAX_SHOWN = 300


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------


class _Retry(CliError):
    """Exit 75: unreachable, timeout, busy -- the operator can simply retry.
    `label` is the word shown in the `identity rotate` table."""

    exit_code = EXIT_BUSY

    def __init__(self, message: str, label: str = "unreachable") -> None:
        super().__init__(message)
        self.label = label


class _TransportError(Exception):
    """A request that never produced an HTTP answer. `kind` is one of `pin`
    (the server is not the pinned one; `actual` is what it presented),
    `rejected` (a TLS alert: the server does not accept OUR certificate),
    `tls` (any other TLS failure), `timeout`, `unreachable` and `invalid` (a
    URL requests cannot use). `reset` marks a reset right after the handshake,
    which is how a rejection sometimes surfaces."""

    def __init__(self, kind: str, detail: str = "", actual: Optional[str] = None,
                 reset: bool = False) -> None:
        super().__init__(detail or kind)
        self.kind = kind
        self.detail = detail
        self.actual = actual
        self.reset = reset


@dataclass
class _Reply:
    status: int
    body: Dict[str, Any]
    #: Whole seconds the server asked us to wait (`Retry-After`, else the body's
    #: `retry_after`), or `None`. Clamped to `_MAX_RETRY_AFTER`.
    retry_after: Optional[int] = None


#: A `Retry-After` beyond a day is not believed (and not shown as such).
_MAX_RETRY_AFTER = 86400


def _parse_retry_after(header: Any, body: Dict[str, Any]) -> Optional[int]:
    """`Retry-After` (delta-seconds or an HTTP date), else the body's
    `retry_after`, as whole seconds in `[0, _MAX_RETRY_AFTER]`; `None` if neither
    says. Server-supplied: nothing here raises."""
    seconds: Optional[float] = None
    if isinstance(header, str) and header.strip():
        text = header.strip()
        if text.isascii() and text.isdigit() and len(text) <= 9:
            seconds = float(text)
        else:
            try:
                from email.utils import parsedate_to_datetime

                when = parsedate_to_datetime(text)
                if when.tzinfo is None:
                    when = when.replace(tzinfo=timezone.utc)
                seconds = when.timestamp() - time.time()
            except (TypeError, ValueError, IndexError, OverflowError):
                seconds = None
    if seconds is None:
        value = body.get("retry_after")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value == value:
            seconds = float(value)
    if seconds is None:
        return None
    return int(min(max(seconds, 0.0), float(_MAX_RETRY_AFTER)))


def _wait_phrase(seconds: Optional[int]) -> str:
    """"wait about 15 minutes" / "wait about 2 seconds" / "wait a little"."""
    if seconds is None:
        return "wait a little"
    if seconds < 90:
        n = max(seconds, 1)
        return f"wait about {n} second{'s' if n != 1 else ''}"
    if seconds < 5400:
        minutes = (seconds + 30) // 60
        return f"wait about {minutes} minutes"
    hours = (seconds + 1800) // 3600
    return f"wait about {hours} hour{'s' if hours != 1 else ''}"


def _clean(text: Any, *secrets: str) -> str:
    """Server- or library-supplied text made safe to print: control characters
    become spaces, any `secrets` are redacted, and the length is capped."""
    out = "".join(ch if ch.isprintable() else " " for ch in str(text))
    for secret in secrets:
        if secret:
            out = out.replace(secret, "<redacted>")
    return out[:_MAX_SHOWN]


def _walk(exc: BaseException) -> Iterator[BaseException]:
    seen = set()
    stack = [exc]
    while stack:
        e = stack.pop()
        if e is None or id(e) in seen:
            continue
        seen.add(id(e))
        yield e
        stack.append(e.__cause__)
        stack.append(e.__context__)
        reason = getattr(e, "reason", None)
        if isinstance(reason, BaseException):
            stack.append(reason)
        stack.extend(a for a in getattr(e, "args", ()) if isinstance(a, BaseException))


_GOT_RE = re.compile(r'got "([0-9a-f]{64})"')


def _presented_fingerprint(exc: BaseException) -> Optional[str]:
    """The certificate fingerprint a pin mismatch saw, if the error says."""
    for e in _walk(exc):
        if isinstance(e, identity.PinMismatch):
            return e.actual
        match = _GOT_RE.search(str(e))
        if match:
            return match.group(1)
    return None


def _reason(exc: BaseException) -> str:
    """One short phrase for a connection failure ("Connection refused")."""
    for e in _walk(exc):
        if isinstance(e, ssl.SSLError):
            text = getattr(e, "reason", None) or e.strerror
            if text:
                return _clean(text)
        elif isinstance(e, OSError) and e.strerror:
            return _clean(e.strerror)
    return type(exc).__name__


def _is_reset(exc: BaseException) -> bool:
    import http.client

    kinds = (ConnectionResetError, BrokenPipeError, ConnectionAbortedError,
             http.client.RemoteDisconnected)
    return any(isinstance(e, kinds) for e in _walk(exc)) or _is_ssl_eof(exc)


def _is_ssl_eof(exc: BaseException) -> bool:
    """Did the peer just close the connection, as seen through TLS?

    Under TLS 1.3 a server that rejects our client certificate sends an alert
    and closes; depending on the Python/OpenSSL version and on who wins the
    race, the client sees the alert, a `ConnectionResetError`, or
    `ssl.SSLEOFError` ("EOF occurred in violation of protocol"). The last is a
    reset like the second. A local verification failure is never one."""
    chain = list(_walk(exc))
    if any(isinstance(e, ssl.SSLCertVerificationError) for e in chain):
        return False
    return any(isinstance(e, ssl.SSLEOFError) for e in chain)


def _probe_presented(host: str, port: int) -> Optional[str]:
    """Best effort: the fingerprint `host:port` presents (no pin), for an
    error message. Never raises."""
    import socket

    try:
        raw = socket.create_connection((host, port), timeout=_TIMEOUT[0])
    except OSError:
        return None
    tls = None
    try:
        tls = identity.client_context(None).wrap_socket(
            raw, server_hostname=None if _is_ip(host) else host
        )
        der = tls.getpeercert(binary_form=True)
        return identity.fingerprint_der(der) if der else None
    except (OSError, ValueError):
        return None
    finally:
        # `wrap_socket` detaches `raw`, so the SSLSocket is the one that owns
        # the descriptor; close both (closing a detached `raw` is a no-op).
        if tls is not None:
            tls.close()
        raw.close()


def _is_ip(host: str) -> bool:
    import ipaddress

    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return False


def _classify(exc: BaseException) -> _TransportError:
    import requests

    if identity.is_pin_mismatch(exc):
        return _TransportError("pin", actual=_presented_fingerprint(exc))
    if identity.is_tls_alert_rejection(exc):
        return _TransportError("rejected")
    if _is_ssl_eof(exc):
        return _TransportError("unreachable", _reason(exc), reset=True)
    if isinstance(exc, requests.exceptions.SSLError):
        return _TransportError("tls", _reason(exc))
    if isinstance(exc, (requests.exceptions.InvalidURL, requests.exceptions.MissingSchema,
                        requests.exceptions.InvalidSchema)):
        return _TransportError("invalid", _clean(exc))
    if isinstance(exc, requests.exceptions.Timeout):
        return _TransportError("timeout", "timed out")
    return _TransportError("unreachable", _reason(exc), reset=_is_reset(exc))


class _Server:
    """One pinned HTTPS connection pool to one destination."""

    def __init__(self, url: str, pin: str, ident: Optional[identity.Identity]) -> None:
        self.base = url.strip().rstrip("/")
        self.pin = pin
        parts = urlsplit(self.base)
        self.host = parts.hostname or ""
        self.port = parts.port or 443
        self.session = identity.pinned_session(ident, pin, _TIMEOUT[1])
        # The wire protocol version (plan S-5) rides on every request: the
        # server answers 426 to one without it, `/health` aside.
        self.session.headers.update(protocol_headers())

    def close(self) -> None:
        self.session.close()

    def request(self, method: str, path: str, body: Optional[dict] = None) -> _Reply:
        import requests

        try:
            response = self.session.request(
                method, self.base + path, json=body, timeout=_TIMEOUT,
                allow_redirects=False,
            )
        except requests.exceptions.RequestException as exc:
            # `from None`: nothing from the HTTP stack is carried into a message.
            failure = _classify(exc)
            if failure.kind == "pin" and failure.actual is None:
                failure.actual = _probe_presented(self.host, self.port)
            raise failure from None
        try:
            try:
                data = response.json() if response.content else {}
            except ValueError:
                data = {}
            body = data if isinstance(data, dict) else {}
            return _Reply(
                response.status_code, body,
                _parse_retry_after(response.headers.get("Retry-After"), body),
            )
        finally:
            response.close()


def _pin_message(url: str, expected: str, actual: Optional[str]) -> str:
    got = actual if actual else "a different certificate"
    return (
        f"the server at {url} presents certificate {got}, not the expected "
        f"{expected}. Refusing to talk to it (if the server was re-keyed, ask its operator "
        "for a new token and enroll again)"
    )


def _transport_cli_error(failure: _TransportError, url: str, expected: str,
                         sent_nothing: bool = False) -> CliError:
    """The `CliError` for a transport failure (exit 1, or 75 when retrying may
    help)."""
    if failure.kind == "pin":
        extra = ". The token was NOT sent." if sent_nothing else ""
        return CliError(_pin_message(url, expected, failure.actual) + extra)
    if failure.kind == "rejected":
        return CliError(
            f"{url} does not accept this host's client certificate (TLS alert)"
        )
    if failure.kind == "tls":
        return CliError(
            f"TLS failure talking to {url}: {failure.detail or 'handshake failed'} "
            "(is it a zfsbackup HTTPS API?)"
        )
    if failure.kind == "invalid":
        return CliError(f"cannot use {url}: {failure.detail}")
    if failure.kind == "timeout":
        return _Retry(f"{url} did not answer in time; retry later", "unreachable")
    return _Retry(f"cannot reach {url}: {failure.detail or 'connection failed'}", "unreachable")


def _error_text(reply: _Reply, *secrets: str) -> str:
    return _clean(reply.body.get("error", ""), *secrets)


def _protocol_mismatch(url: str, server_protocol: Any) -> CliError:
    """The refusal for a server speaking another wire protocol (exit 1)."""
    if isinstance(server_protocol, int) and not isinstance(server_protocol, bool):
        theirs = f"protocol {server_protocol}"
    else:
        theirs = "no wire protocol version (it predates the protocol check)"
    return CliError(
        f"{url} speaks {theirs}, but this client speaks protocol {PROTOCOL_VERSION}. "
        "The client and the server must be upgraded together"
    )


def _supported_protocol(value: Any) -> bool:
    """Is `value` (a body's `protocol`) a version this client speaks?"""
    return (
        isinstance(value, int) and not isinstance(value, bool) and value in SUPPORTED_PROTOCOLS
    )


def _protocol_after_success(url: str, server_protocol: Any, what: str) -> CliError:
    """The failure (exit 1) for a server that ACCEPTED `what` (an enrollment, a
    rotation) but answered with no wire protocol version or an unsupported
    one. The server-side change has happened, so the message says so and the
    caller completes its local success path before raising this."""
    return CliError(
        f"the server accepted {what}, but it speaks an unsupported wire protocol "
        "(or none): backups to it will be refused until client and server run matching "
        "versions. The server-side change was NOT undone.\n"
        + str(_protocol_mismatch(url, server_protocol))
    )


def _generic_failure(reply: _Reply, url: str, what: str, *secrets: str) -> CliError:
    """The `CliError` for a response that is not the expected success and was
    not handled specifically by the caller."""
    status = reply.status
    text = _error_text(reply, *secrets)
    reason = _clean(reply.body.get("reason", ""))
    suffix = f": {text}" if text else ""
    if status == 426 and reason == PROTOCOL_REFUSAL_REASON:
        return _protocol_mismatch(url, reply.body.get("protocol"))
    if status == 429:
        # The server locked this address out after repeated failed enrollments
        # (reason `rate_limited`): retrying before the lockout ends cannot
        # succeed, so say how long to wait.
        return _Retry(
            f"{url} is refusing requests from this host for now (HTTP 429"
            + (", too many failed enrollments" if reason == "rate_limited" else "")
            + f"); {_wait_phrase(reply.retry_after)} and try again"
            + ("" if reply.retry_after is None else f" (Retry-After: {reply.retry_after}s)"),
            "busy",
        )
    if status == 503:
        if reply.body.get("retry"):
            when = ("retry in a moment" if reply.retry_after is None
                    else f"{_wait_phrase(reply.retry_after)} and retry")
            return _Retry(f"{url} is busy (HTTP 503){suffix}; {when}", "busy")
        if reason == "dry_run":
            return CliError(f"{url} is a dry-run server and does not accept {what}{suffix}")
        return CliError(f"{url} cannot do {what} right now (HTTP 503){suffix}")
    if status in (502, 504):
        return _Retry(f"{url} is not reachable through its proxy (HTTP {status})", "unreachable")
    if status in (404, 405):
        return CliError(
            f"{url} answered HTTP {status} for {what}: it does not look like a zfsbackup "
            "server with TLS support (wrong URL, or a server that has not been upgraded)"
        )
    if status in (401, 403):
        return CliError(f"{url} refused {what} (HTTP {status}){suffix}")
    return CliError(f"{url} answered HTTP {status} to {what}{suffix}")


# --------------------------------------------------------------------------
# /whoami
# --------------------------------------------------------------------------


@dataclass
class _Whoami:
    client_id: str
    fingerprint: str


def _whoami(
    server: _Server, ours: identity.Identity, untrusted_ok: bool = False
) -> Optional[_Whoami]:
    """`GET /whoami` with the client certificate: the answer when the server
    trusts it, `None` when it does not. Transport failures propagate as
    `_TransportError` (a pin mismatch included); other answers as `CliError`.
    A reset straight after the handshake is retried once (a TLS 1.3 rejection
    sometimes surfaces that way, about one time in thirty). With `untrusted_ok`
    a reset that survives the retry is taken as the server's rejection of an
    untrusted certificate, i.e. `None`, and the caller goes on to `/enroll`
    (which reports a server that is really down, token unspent). `identity
    rotate` leaves it off: there a reset must not read as "revoked"."""
    for attempt in (1, 2):
        try:
            reply = server.request("GET", "/whoami")
            break
        except _TransportError as failure:
            if failure.kind == "rejected":
                return None
            if failure.kind == "unreachable" and failure.reset:
                if attempt == 1:
                    continue
                if untrusted_ok:
                    return None
            raise
    if reply.status == 200:
        client_id = reply.body.get("client_id")
        fingerprint = reply.body.get("fingerprint")
        if not isinstance(client_id, str) or not isinstance(fingerprint, str):
            raise CliError(f"{server.base} answered /whoami with an unexpected body")
        protocol = reply.body.get("protocol")
        if not _supported_protocol(protocol):
            raise _protocol_mismatch(server.base, protocol)
        if fingerprint != ours.fingerprint:
            raise CliError(
                f"{server.base} reports certificate {fingerprint} for this connection, "
                f"but this host presented {ours.fingerprint}"
            )
        return _Whoami(client_id, fingerprint)
    if reply.status in (401, 403):
        return None
    raise _generic_failure(reply, server.base, "/whoami")


# --------------------------------------------------------------------------
# Identity files
# --------------------------------------------------------------------------


def _identity_failure(exc: BaseException) -> CliError:
    text = str(exc) if isinstance(exc, identity.IdentityError) else (
        getattr(exc, "strerror", None) or str(exc)
    )
    lowered = text.lower()
    if "permission" in lowered and "run as" not in lowered:
        text += (
            "\nrun this command as root, or as the user that owns the TLS directory "
            "and the private key"
        )
    return CliError(text)


def _client_identity(files: tlsfiles.ClientFiles) -> identity.Identity:
    """This host's client identity: generated into the default directory if
    missing; loaded as is from configured paths (never generated there)."""
    try:
        if files.generated_in is not None:
            return identity.ensure_identity(files.generated_in, identity.ROLE_CLIENT)
        return identity.load_identity(files.cert, files.key, identity.ROLE_CLIENT)
    except (identity.IdentityError, OSError) as exc:
        if files.generated_in is None and isinstance(exc, identity.IdentityError):
            raise CliError(
                f"{exc}\nclient_cert_file / client_key_file are configured, so no identity "
                "is generated for them; provide the files or `unset client_cert_file` and "
                "`unset client_key_file` to use a generated one"
            ) from None
        raise _identity_failure(exc) from None


# --------------------------------------------------------------------------
# Reload bookkeeping
# --------------------------------------------------------------------------


class _Commits:
    """What this invocation has committed to the config database, so that
    `begin_reload` runs exactly once, for the last write."""

    def __init__(self) -> None:
        self.committed = False
        self.generation: Optional[int] = None
        self.store_id: Optional[int] = None
        self.generated_client_id: Optional[str] = None
        # Failures to raise only AFTER the local success path has run and been
        # reported: the server already changed state (a server speaking no
        # supported protocol that nevertheless accepted the enrollment).
        self.late_errors: List[CliError] = []

    def record(self, generation: Optional[int], store_id: Optional[int]) -> None:
        self.committed = True
        self.generation = generation
        self.store_id = store_id


def _echo(text: str = "", err: bool = False) -> None:
    try:
        click.echo(text, err=err)
    except OSError:  # a closed pipe must not cost the rest of the work
        pass


def _flush_failed(resolved: Any, commits: _Commits) -> None:
    """The failure path: something was committed, a later step failed. Ask the
    daemon to reload anyway (the id it needs is in the database now), once."""
    if not commits.committed:
        return
    request = begin_reload(resolved, commits.generation, commits.store_id)
    if commits.generated_client_id:
        _echo(f"generated this host's client id: {commits.generated_client_id}")
    notify_daemon(
        resolved, commits.generation, commits.store_id, reload_wait=0, request=request
    )


# --------------------------------------------------------------------------
# enroll
# --------------------------------------------------------------------------


def _read_token(
    state: Any, token_file: Optional[str], token_opt: Optional[str]
) -> identity.SecretStr:
    """The token text from `--token-file`, stdin (`--token -`) or a hidden
    prompt. Never echoed, and never part of an error message."""
    if token_file is not None and token_opt is not None:
        raise click.UsageError("give --token-file or --token -, not both")
    if token_opt is not None and token_opt != "-":
        # Deliberately does not repeat the value: it is a secret.
        raise click.UsageError(
            "--token takes only '-' (read the token from stdin); a token on the command "
            "line would be visible in `ps` and the shell history. Use --token-file PATH, "
            "`--token -` or the prompt."
        )
    if token_file is not None:
        return identity.SecretStr(_read_token_file(state, token_file))
    interactive = sys.stdin is not None and sys.stdin.isatty()
    if token_opt == "-" and not interactive:
        data = sys.stdin.read(_MAX_TOKEN_BYTES + 1)
        if len(data) > _MAX_TOKEN_BYTES:
            raise CliError("the token on stdin is far too long to be an enrollment token")
        return identity.SecretStr(data.strip())
    if interactive:
        return identity.SecretStr(
            click.prompt("Enrollment token", hide_input=True, err=True, type=str).strip()
        )
    raise click.UsageError(
        "no enrollment token: use --token-file PATH, or --token - to read it from stdin "
        "(there is no terminal to prompt on)"
    )


def _read_token_file(state: Any, path: str) -> str:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    except OSError as exc:
        raise CliError(f"cannot read the token file {path}: {exc.strerror}") from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise CliError(f"the token file {path} is not a regular file")
        if info.st_size > _MAX_TOKEN_BYTES:
            raise CliError(f"the token file {path} is far too large to hold a token")
        if info.st_mode & 0o004:
            state.warn(
                f"the token file {path} is readable by everyone; delete it once the "
                "enrollment has succeeded"
            )
        try:
            data = os.read(fd, _MAX_TOKEN_BYTES + 1)
        except OSError as exc:
            raise CliError(f"cannot read the token file {path}: {exc.strerror}") from None
    finally:
        os.close(fd)
    try:
        return data.decode("ascii").strip()
    except UnicodeDecodeError:
        raise CliError(f"the token file {path} does not hold an enrollment token") from None


def _check_label(label: Optional[str]) -> None:
    if label is None:
        return
    if len(label) > _MAX_LABEL or any(not ch.isprintable() for ch in label):
        raise CliError(
            f"--label must be printable text of at most {_MAX_LABEL} characters"
        )


def _with_destination(
    config: BackupConfig, name: str, url: str, fingerprint: Optional[str]
) -> BackupConfig:
    new = copy.copy(config)
    new.destinations = dict(config.destinations)
    new.destinations[name] = Destination(url=url, server_fingerprint=fingerprint)
    return new


def _new_key_remedy(ours: Optional[identity.Identity], configured: bool, name: str) -> str:
    """What to do when the server turns away the certificate `--new-key` is
    enrolling (a revoked certificate can never be enrolled, and a key left
    behind by an earlier run may be one)."""
    if ours is None:
        return ""
    if configured:
        fix = (
            f"use a different pair: re-run `zfsbackup-config enroll {name} --new-key "
            f"--cert NEW --key NEW` (the files {ours.cert_path} and {ours.key_path} are yours "
            "and are left alone)"
        )
    else:
        fix = (
            f"re-run `zfsbackup-config enroll {name} --new-key` (a new client key that no "
            "server trusts is discarded and replaced by a fresh one)"
        )
    return (
        f"\nIf the token is fine and the server REVOKED the new client certificate "
        f"{ours.fingerprint}, it can never be enrolled: {fix}. The token is not spent when "
        "the server refuses it."
    )


def _enroll_failure(reply: _Reply, url: str, secrets: Tuple[str, ...],
                    new_key: bool = False, name: str = "<destination>",
                    ours: Optional[identity.Identity] = None,
                    configured: bool = False) -> CliError:
    reason = reply.body.get("reason")
    text = _error_text(reply, *secrets)
    status = reply.status
    if status == 403 and reason == "refused":
        # The server gives one answer for every cause (that is deliberate), so
        # the revoked-certificate remedy is offered as a possibility.
        extra = _new_key_remedy(ours, configured, name) if new_key else (
            "\nIf the token is fine and this host's certificate was REVOKED on that server, "
            "that certificate can never be enrolled again: run `zfsbackup-config enroll "
            f"{name} --new-key` (it generates a new client key) with this token if it is "
            "still unspent, otherwise with a new one."
        )
        return CliError(
            f"{url} refused the token (it is unknown, expired, already used, issued for a "
            "different server, or scoped to a different client id). Ask the server "
            "operator for a new one: `zfsbackup-config trust token create`" + extra
        )
    if status == 403 and reason == "bad_signature":
        return CliError(
            f"{url} could not verify this host's proof of possession ({text or 'bad signature'}); "
            "this is a bug or a corrupted client key, not a problem with the token"
        )
    if status == 409 and reason == "client_id_taken":
        client_id = _clean(reply.body.get("client_id", "")) or "<this host's client id>"
        return CliError(
            f"{url} already knows a client with id {client_id!r}, and this token is not "
            f"scoped to it (the token was not used and is still valid).\n"
            f"Ask the server operator for a token for that id: "
            f"`zfsbackup-config trust token create --client-id {client_id}` "
            "(add --replace to drop the id's other certificates; without it they stay pinned "
            "there, which matters if this host's old key may be stolen)"
        )
    if status == 409 and reason == "cert_bound_elsewhere":
        return CliError(
            f"{url} already has this host's certificate enrolled under a different client "
            "id. Either change this host's client id to that one (`zfsbackup-config "
            "identity set-client-id`), or have the server operator revoke the certificate "
            "(`trust revoke`) and then enroll with a NEW key (`zfsbackup-config enroll "
            "<destination> --new-key`, once; other enrolled servers then need a plain `enroll` "
            "with a token created with `trust token create --client-id ID --replace`): a "
            "revoked certificate cannot be enrolled again"
        )
    if status in (400, 413, 422):
        suffix = f": {text}" if text else ""
        return CliError(f"{url} rejected the enrollment request (HTTP {status}){suffix}")
    return _generic_failure(reply, url, "/enroll", *secrets)


@dataclass
class _NewKey:
    """`enroll --new-key`: where the successor identity lives and what it
    replaces. `new_cert`/`new_key` are set only for configured paths."""

    old_fingerprint: Optional[str]
    new_cert: Optional[Path] = None
    new_key: Optional[Path] = None


def enroll(
    state: Any,
    destination: str,
    url_opt: Optional[str],
    label: Optional[str],
    token_file: Optional[str],
    token_opt: Optional[str],
    new_key: bool = False,
    cert: Optional[str] = None,
    key: Optional[str] = None,
) -> None:
    resolved = state.resolve()
    if (cert is None) != (key is None):
        raise click.UsageError("--cert and --key go together (both or neither)")
    if cert is not None and not new_key:
        raise click.UsageError(
            "--cert/--key only go with --new-key (they name the new client certificate; "
            "a plain `enroll` uses the configured one)"
        )
    token = _read_token(state, token_file, token_opt)
    try:
        token_fp, secret = identity.parse_token(str(token))
    except ValueError as exc:
        raise CliError(str(exc)) from None
    _check_label(label)

    commits = _Commits()
    try:
        lines = _enroll(
            state, resolved, commits, destination, url_opt, label, token, token_fp, secret,
            new_key, cert, key,
        )
    except BaseException:
        _flush_failed(resolved, commits)
        raise
    request = None
    if commits.committed:
        # Before any output: a closed stdout must not cost the signal.
        request = begin_reload(resolved, commits.generation, commits.store_id)
    for line in lines:
        _echo(line)
    if commits.committed:
        notify_daemon(resolved, commits.generation, commits.store_id, request=request)
    if commits.late_errors:
        raise commits.late_errors[0]


def _discard_successor(directory: Path, fingerprint: str) -> None:
    """Delete the stale `client.next` pair `fingerprint` that was probed, under
    the identity lock (`identity.discard_identity`). The caller has already run
    `complete_promotion`, so a half-promotion cannot be what is on disk.

    If nothing was removed -- the pair vanished, or a different one appeared
    since the probe because another `enroll`/`identity rotate` ran meanwhile --
    this stops rather than looping: the probe result no longer describes what
    is on disk, and the operator's re-run starts from a clean look."""
    if not identity.discard_identity(
        directory, identity.CLIENT_NEXT_NAME, expected_fingerprint=fingerprint
    ):
        raise CliError(
            f"client.next in {directory} changed while this command was running (another "
            "`enroll --new-key` or `identity rotate`?); nothing was discarded. Re-run "
            "the same command."
        )


def _successor_identity(
    state: Any,
    files: tlsfiles.ClientFiles,
    cert: Optional[str],
    key: Optional[str],
    trusted: Callable[[identity.Identity], bool],
) -> Tuple[identity.Identity, _NewKey]:
    """The identity `enroll --new-key` enrolls: `client.next` in the default
    directory, or the operator's own `--cert/--key` pair when the paths are
    configured (never generated: those files are the operator's).

    A `client.next` an earlier run left is reused ONLY when a server already
    trusts it (`trusted(identity)`: that is the idempotent re-run of an
    enrollment whose later steps failed). Otherwise it is discarded and a fresh
    key generated: `--new-key` is the remedy for a key that may be stolen, and
    a copied directory would bring a stale `client.next` along."""
    current = tlsfiles.describe_certificate(files.cert)
    old_fp = current.fingerprint
    try:
        if files.configured:
            if cert is None:
                raise click.UsageError(
                    "client_cert_file / client_key_file are configured, so the new key must "
                    "be yours: pass --cert NEW --key NEW (absolute paths of a new pair, "
                    "different from the configured files)"
                )
            new_cert = Path(os.path.abspath(os.path.expanduser(cert)))
            new_key = Path(os.path.abspath(os.path.expanduser(key)))
            if new_cert == files.cert or new_key == files.key:
                raise CliError(
                    "--cert/--key name the currently configured files; --new-key needs a "
                    "different pair"
                )
            successor = identity.load_identity(new_cert, new_key, identity.ROLE_CLIENT)
            marker = _NewKey(old_fp, new_cert, new_key)
        else:
            if cert is not None:
                raise click.UsageError(
                    "--cert/--key are for installations that set client_cert_file/"
                    "client_key_file; the generated identity needs no files from you: run "
                    "`zfsbackup-config enroll NAME --new-key`"
                )
            directory = files.generated_in
            try:
                before = identity.peek_identity(directory, identity.CLIENT_NEXT_NAME)
            except identity.IdentityError:
                before = None  # unparseable: debris, replaced below
            successor = identity.ensure_identity(
                directory, identity.ROLE_CLIENT, name=identity.CLIENT_NEXT_NAME
            )
            if before is not None and before.fingerprint == successor.fingerprint:
                # A pending client.next from an earlier run: only a server that
                # trusts it makes it the one to keep.
                if trusted(successor):
                    state.warn(
                        "reusing the new client key left by an earlier `enroll --new-key` "
                        f"(client.next, {successor.fingerprint}): the server already "
                        "trusts it"
                    )
                else:
                    state.warn(
                        f"discarding the unused new client key {successor.fingerprint} "
                        f"(client.next.* in {directory}) left by an earlier `enroll "
                        "--new-key` or `identity rotate`: the server at this destination "
                        "does not trust it, and the directory may have been copied. "
                        "Generating a fresh one. Any other server that accepted it as a "
                        "pending rotation successor will need this host enrolled there "
                        "again (see `identity rotate`)"
                    )
                    _discard_successor(directory, successor.fingerprint)
                    successor = identity.ensure_identity(
                        directory, identity.ROLE_CLIENT, name=identity.CLIENT_NEXT_NAME
                    )
            marker = _NewKey(old_fp)
    except (identity.IdentityError, OSError) as exc:
        raise _identity_failure(exc) from None
    if successor.fingerprint == old_fp:
        raise CliError(
            "the new client certificate is the current one; --new-key needs a different key"
        )
    return successor, marker


def _enroll(
    state: Any,
    resolved: Any,
    commits: _Commits,
    name: str,
    url_opt: Optional[str],
    label: Optional[str],
    token: identity.SecretStr,
    token_fp: str,
    secret: Any,
    new_key: bool = False,
    cert_opt: Optional[str] = None,
    key_opt: Optional[str] = None,
) -> List[str]:
    secrets = (str(token), str(token).rsplit(".", 1)[-1])

    # 1. Inputs, all checked before anything is written or generated.
    snapshot = read_snapshot(resolved)
    existing = snapshot.config.destinations.get(name)
    if url_opt is None and existing is None:
        raise click.UsageError(
            f"destination {name!r} does not exist yet: pass --url https://HOST:PORT"
        )
    url = (url_opt if url_opt is not None else existing.url).strip()
    problem = destination_url_problem(url)
    if problem:
        raise CliError(problem)
    stored_pin = existing.server_fingerprint if existing is not None else None
    try:
        validate_config(_with_destination(snapshot.config, name, url, token_fp))
    except ValueError as exc:
        raise CliError(str(exc)) from None

    # 2. The client TLS identity (a permission problem stops here, before any
    # database write). With --new-key it is the SUCCESSOR: a fresh key that
    # becomes this host's identity only once a server has enrolled it.
    files = tlsfiles.client_identity_files(snapshot.config, resolved)
    marker: Optional[_NewKey] = None
    finished: Optional[identity.Identity] = None
    if new_key and not files.configured:
        # An earlier `enroll --new-key` that died between the two renames of
        # its promotion left the new key as client.key with the OLD certificate
        # still named client.next.crt. Finish it first (as `identity rotate`
        # does): the promoted identity is the one the server enrolled, so this
        # run is a plain enroll of it and the token is not spent.
        try:
            finished = identity.complete_promotion(files.generated_in)
        except (identity.IdentityError, OSError) as exc:
            raise _identity_failure(exc) from None
        if finished is not None:
            new_key = False
    if new_key and not files.configured and not (
        os.path.lexists(files.cert) or os.path.lexists(files.key)
    ):
        state.warn(
            "this host has no client certificate yet, so --new-key changes nothing: "
            "enrolling with the one that is generated now"
        )
        new_key = False
    # The servers asked who trusts a certificate: the stored pin, and the
    # token's own fingerprint.
    pins = [stored_pin] if stored_pin else []
    if token_fp not in pins:
        pins.append(token_fp)

    def trusted_by_a_server(candidate: identity.Identity) -> bool:
        pin_failure: Optional[_TransportError] = None
        answered = False
        for pin in pins:
            server = _Server(url, pin, candidate)
            try:
                if _whoami(server, candidate, untrusted_ok=True) is not None:
                    return True
                answered = True  # a server presenting the expected certificate said no
            except _TransportError as failure:
                if failure.kind == "pin":
                    pin_failure = failure  # not the server it claims to be
                    continue
                # Without an answer the stale key cannot be told from a
                # trusted one, and guessing either way is wrong.
                raise _transport_cli_error(failure, url, pin) from None
            finally:
                server.close()
        if pin_failure is not None and not answered:
            # No server presenting the expected certificate answered, so the
            # key is not known to be untrusted; a discard is irreversible.
            raise _transport_cli_error(pin_failure, url, token_fp) from None
        return False

    if new_key:
        ours, marker = _successor_identity(
            state, files, cert_opt, key_opt, trusted_by_a_server
        )
    else:
        ours = _client_identity(files)

    # 3. This host's client id.
    try:
        client_id, created = ensure_client_id(resolved, lock=None)
    finally:
        release_database()
    if created:
        commits.generated_client_id = client_id
        commits.record(None, None)  # the store moved; the exact key is read next
        snapshot = read_snapshot(resolved)
        commits.record(snapshot.generation, snapshot.store_id)

    # 4. Already trusted by the server? With --new-key this asks about the
    # SUCCESSOR, never the old certificate: a token must be sent to enroll a
    # new key (and a `--replace` token only acts when it is sent). It only
    # finds a successor an earlier, interrupted run already got enrolled.
    trusted_pin: Optional[str] = None
    pin_failure: Dict[str, _TransportError] = {}
    for pin in pins:
        server = _Server(url, pin, ours)
        try:
            # A reset that survives the retry is the server turning an untrusted
            # certificate away (first-time enroll, a fresh key, or one that was
            # revoked); a server that is really down fails the /enroll below
            # with its own, correct message and the token unspent.
            who = _whoami(server, ours, untrusted_ok=True)
        except _TransportError as failure:
            if failure.kind == "pin":
                pin_failure[pin] = failure
                continue
            raise _transport_cli_error(failure, url, pin) from None
        finally:
            server.close()
        if who is None:
            continue
        if who.client_id != client_id:
            raise CliError(
                f"{url} has this host's certificate enrolled as client id {who.client_id!r}, "
                f"but this host's client id is {client_id!r}. Nothing was changed.\n"
                f"Either `zfsbackup-config{where_flag(resolved)} identity set-client-id "
                f"{who.client_id}` (every other server then sees a different id), or have "
                "the server operator `trust revoke --client "
                f"{who.client_id}` and issue a token for this host, then run `enroll "
                f"{name} --new-key` ONCE (for this server). Every other enrolled server "
                "already knows this host's client id "
                f"{client_id!r}: for each, run a plain `enroll` (no --new-key) with a token "
                f"from that server's operator created with `trust token create --client-id "
                f"{client_id} --replace`, which also drops the old certificate there"
            )
        if pin != token_fp:
            raise CliError(
                f"destination {name!r} is already enrolled with the server at {url} "
                f"(fingerprint {pin}), but this token was issued for a different server "
                f"({token_fp}). Nothing was changed; use the token that server's operator "
                "created, or remove the stale destination first"
            )
        trusted_pin = pin
        break

    lines: List[str] = []
    spent = False
    sent_token = False
    if trusted_pin is None:
        # 5. Enroll with the token. A pin failure against the token's own
        # fingerprint ends here: the token is never sent to the wrong server.
        failure = pin_failure.get(token_fp)
        if failure is not None:
            raise _transport_cli_error(failure, url, token_fp, sent_nothing=True)
        if stored_pin and stored_pin != token_fp:
            state.warn(
                f"destination {name!r} was pinned to server fingerprint {stored_pin}; the "
                f"server at {url} does not present it. Re-pinning to the token's {token_fp}"
            )
        status = _enroll_with_token(
            url, token_fp, ours, client_id, label, token, secret, secrets, new_key, name,
            configured=marker is not None and marker.new_cert is not None,
            late=commits.late_errors,
        )
        sent_token = True
        spent = status == "enrolled"

    # 6. Record the destination (and, for --new-key with configured paths, the
    # new files, in the same commit).
    unchanged = (
        existing is not None and existing.url == url and existing.server_fingerprint == token_fp
    )
    if not unchanged or (marker is not None and marker.new_cert is not None):
        new = _with_destination(snapshot.config, name, url, token_fp)
        if marker is not None and marker.new_cert is not None:
            new.client_cert_file = marker.new_cert
            new.client_key_file = marker.new_key
        try:
            result = commit_config(
                resolved, snapshot, new, warn=state.warn, announce=False
            )
        except (ConfigConflict, ConfigBusy) as exc:
            if hasattr(exc, "add_note"):
                if new_key:
                    exc.add_note(
                        "The server has ALREADY enrolled the new certificate; re-run the same "
                        "`zfsbackup-config enroll --new-key` command -- the server is asked "
                        "first and the token is not needed again."
                    )
                else:
                    exc.add_note(
                        "The server has ALREADY enrolled this host's certificate; re-run "
                        "`zfsbackup-config enroll` -- the token is not needed again (the same "
                        "token file is read for the server's fingerprint but is not sent: the "
                        "server knows this host)."
                    )
            raise
        commits.record(result.generation, result.store_id)

    if marker is not None and marker.new_cert is None:
        # The default files: the successor replaces client.* (the daemon picks
        # the new certificate up within one cycle).
        try:
            identity.promote_identity(files.generated_in)
        except (identity.IdentityError, OSError) as exc:
            error = _identity_failure(exc)
            raise CliError(
                f"the server enrolled the new certificate, but it could not be installed "
                f"here: {error}\nRe-run `zfsbackup-config{where_flag(resolved)} enroll "
                f"{name} --new-key` as root or as the key's owner: the server is asked first "
                "and the token is not needed again."
            ) from None

    if spent:
        lines.append(f"enrolled destination {name!r} ({url}) as client id {client_id}")
    elif sent_token:
        lines.append(
            f"the server at {url} already trusted this host's certificate (client id "
            f"{client_id}); the token was not used"
        )
        if unchanged:
            lines.append(f"destination {name!r} is already recorded; nothing to change")
    else:
        lines.append(
            f"this host is already enrolled at {url} as client id {client_id}; the token "
            "was not used"
        )
        if unchanged:
            lines.append(f"destination {name!r} is already recorded; nothing to change")
        if marker is None and finished is None:
            lines.append(
                "  If this was a `--replace` token, run `enroll --new-key` with it (or "
                "`trust revoke` the other certificates on the server): the token is only "
                "sent for a certificate the server does not trust yet."
            )
    if finished is not None:
        lines.insert(0, (
            "finished an interrupted key promotion: the new client key had been "
            "enrolled but not yet installed; the client certificate is now "
            f"{finished.fingerprint}" + (
                "" if spent else " (the server already trusts it, so this run did not use the token)"
            )
        ))
    lines.append(f"  client certificate: {ours.fingerprint}")
    lines.append(f"  server fingerprint: {token_fp}")
    if created:
        lines.append(f"  (generated this host's client id {client_id})")
    if marker is not None:
        lines += _new_key_lines(state, snapshot, name, marker, ours, files, client_id)
    elif finished is not None:
        _others_warning(state, snapshot, name, client_id, None)
    return lines


def _others_warning(
    state: Any, snapshot: Any, name: str, client_id: str, old: Optional[str]
) -> None:
    """After a new client key was enrolled at `name`: every other enrolled
    server still has the previous certificate (`old`, when known) pinned and
    does not know the new one."""
    others = sorted(
        n for n, d in snapshot.config.destinations.items() if n != name and d.server_fingerprint
    )
    if not others:
        return
    listed = ", ".join(repr(n) for n in others)
    previous = f"the OLD certificate ({old})" if old else "the previous certificate"
    state.warn(
        f"this host is also enrolled at {listed}; "
        + ("those servers trust " if old else "those servers may still trust ")
        + f"{previous} and will refuse the new one until it is enrolled there too. Each "
        f"already knows client id {client_id!r}, so its operator must create the token with "
        f"`zfsbackup-config trust token create --client-id {client_id} --replace` (an "
        "unscoped token is refused with 409 client_id_taken, and without --replace the old, "
        "possibly stolen, certificate stays pinned there). Then, for each, run "
        "`zfsbackup-config enroll NAME --token-file FILE` (plain: the new certificate is now "
        "this host's own; another `--new-key` would generate yet another key and break the "
        "destinations already done). To keep the old certificate working elsewhere, "
        "`identity rotate` first is the gentler path, but it needs the old certificate to be "
        "trusted everywhere."
    )


def _new_key_lines(
    state: Any,
    snapshot: Any,
    name: str,
    marker: _NewKey,
    ours: identity.Identity,
    files: tlsfiles.ClientFiles,
    client_id: str,
) -> List[str]:
    old = marker.old_fingerprint
    shown = old or "(none)"
    if marker.new_cert is None:
        where = f"{files.cert} and {files.key}"
        lines = [
            f"  new client key installed ({where}); this host no longer holds the key of "
            f"{shown}"
        ]
    else:
        lines = [
            f"  client_cert_file / client_key_file now point at {marker.new_cert} and "
            f"{marker.new_key}; the previous certificate was {shown} (its files are left in place)"
        ]
    if old:
        # `/enroll` does not say which pins it dropped, so this is conditional.
        lines.append(
            f"  The server at this destination still trusts {old} unless the token was "
            "created with `trust token create --client-id ... --replace`. If it was not (or "
            f"you cannot tell), have its operator run `zfsbackup-config trust revoke {old}` "
            "there, especially if the old key may have been stolen."
        )
    _others_warning(state, snapshot, name, client_id, old)
    return lines


def _enroll_with_token(
    url: str,
    token_fp: str,
    ours: identity.Identity,
    client_id: str,
    label: Optional[str],
    token: identity.SecretStr,
    secret: Any,
    secrets: Tuple[str, ...],
    new_key: bool = False,
    name: str = "<destination>",
    configured: bool = False,
    late: Optional[List[CliError]] = None,
) -> str:
    """`POST /enroll`, on a connection pinned to the token's fingerprint and
    carrying NO client certificate. Returns the server's status: `enrolled`
    (the token was spent) or `already_trusted` (it was not).

    A 200 whose `protocol` is missing or unsupported is still a success on the
    server (the token may be spent, the certificate is pinned there), so the
    status is returned and the failure is appended to `late` for the caller to
    raise once it has recorded the destination and reported."""
    payload = identity.enrollment_payload(
        token_fp, client_id, ours.fingerprint, identity.secret_hash(bytes(secret))
    )
    signature = base64.b64encode(identity.sign_payload(ours.key_path, payload)).decode("ascii")
    body: Dict[str, Any] = {
        "version": 1,
        "client_id": client_id,
        "token_secret": str(token),
        "cert_pem": ours.cert_pem,
        "signature": signature,
    }
    if label is not None:
        body["label"] = label
    server = _Server(url, token_fp, None)
    try:
        try:
            reply = server.request("POST", "/enroll", body)
        except _TransportError as failure:
            raise _transport_cli_error(failure, url, token_fp, sent_nothing=True) from None
    finally:
        server.close()
        body.clear()
    status = reply.body.get("status")
    if reply.status == 200 and status in ("enrolled", "already_trusted"):
        if (
            reply.body.get("client_id") != client_id
            or reply.body.get("fingerprint") != ours.fingerprint
        ):
            raise CliError(
                f"{url} answered /enroll with a client id or fingerprint that is not this "
                "host's; nothing was recorded"
            )
        if late is not None and not _supported_protocol(reply.body.get("protocol")):
            late.append(_protocol_after_success(
                url, reply.body.get("protocol"),
                "the enrollment" if status == "enrolled" else "this host's certificate",
            ))
        return str(status)
    raise _enroll_failure(reply, url, secrets, new_key, name, ours, configured)


# --------------------------------------------------------------------------
# identity rotate
# --------------------------------------------------------------------------


@dataclass
class _RotateResult:
    name: str
    url: str
    result: str  # rotated | already | refused | unreachable | busy
    detail: str = ""
    #: The server accepted the successor but speaks no supported protocol.
    protocol_problem: bool = False
    server_protocol: Any = None

    @property
    def ok(self) -> bool:
        return self.result in ("rotated", "already")


def _rotate_one(
    name: str,
    dest: Destination,
    client_id: str,
    current: identity.Identity,
    successor: identity.Identity,
) -> _RotateResult:
    url = dest.url
    pin = dest.server_fingerprint
    assert pin is not None
    server = _Server(url, pin, current)
    try:
        try:
            who = _whoami(server, current)
            if who is None:
                return _RotateResult(
                    name, url, "refused",
                    "the server does not trust this host's current certificate "
                    "(revoked, or enrolled elsewhere?); enroll again with a new token "
                    "(`enroll NAME --new-key` if it was revoked)",
                )
            if who.client_id != client_id:
                return _RotateResult(
                    name, url, "refused",
                    f"the server knows this certificate as client id {who.client_id!r}, "
                    f"not {client_id!r}",
                )
            payload = identity.rotation_payload(
                pin, client_id, current.fingerprint, successor.fingerprint
            )
            signature = base64.b64encode(
                identity.sign_payload(successor.key_path, payload)
            ).decode("ascii")
            reply = server.request("POST", "/rotate", {
                "version": 1,
                "successor_cert_pem": successor.cert_pem,
                "signature": signature,
            })
        except _TransportError as failure:
            error = _transport_cli_error(failure, url, pin)
            return _from_error(name, url, error)
        except CliError as error:
            return _from_error(name, url, error)
    finally:
        server.close()
    status = reply.body.get("status")
    if reply.status == 200 and status in ("rotated", "already"):
        if reply.body.get("fingerprint") != successor.fingerprint:
            return _RotateResult(
                name, url, "refused", "the server pinned a different certificate than sent"
            )
        if not _supported_protocol(reply.body.get("protocol")):
            return _RotateResult(
                name, url, str(status),
                "accepted, but the server speaks an unsupported wire protocol (or none): "
                "backups to it will be refused until client and server run matching versions",
                protocol_problem=True,
                server_protocol=reply.body.get("protocol"),
            )
        return _RotateResult(name, url, str(status))
    reason = reply.body.get("reason")
    if reply.status == 403 and reason == "bad_signature":
        return _RotateResult(name, url, "refused", "the server rejected the proof of possession")
    if reply.status == 403 and reason == "refused":
        return _RotateResult(
            name, url, "refused",
            "the server refused the rotation (the successor is pinned elsewhere, or a "
            "different successor is already pending for this certificate)",
        )
    if reply.status in (400, 413, 422):
        return _RotateResult(
            name, url, "refused",
            f"the server rejected the request (HTTP {reply.status}): "
            f"{_error_text(reply) or 'no reason given'}",
        )
    return _from_error(name, url, _generic_failure(reply, url, "/rotate"))


def _from_error(name: str, url: str, error: CliError) -> _RotateResult:
    if isinstance(error, _Retry):
        return _RotateResult(name, url, error.label, str(error))
    return _RotateResult(name, url, "refused", str(error))


def _table(header: List[str], rows: List[List[str]]) -> str:
    widths = [len(h) for h in header]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def line(cells: List[str]) -> str:
        padded = [c.ljust(widths[i]) for i, c in enumerate(cells[:-1])] + [cells[-1]]
        return "  ".join(padded).rstrip()

    return "".join(line(r) + "\n" for r in [header] + rows)


def identity_rotate(state: Any, cert: Optional[str], key: Optional[str]) -> None:
    resolved = state.resolve()
    if (cert is None) != (key is None):
        raise click.UsageError("--cert and --key go together (both or neither)")
    snapshot = read_snapshot(resolved)
    files = tlsfiles.client_identity_files(snapshot.config, resolved)
    where = where_flag(resolved)
    if files.configured and cert is None:
        raise click.UsageError(
            "client_cert_file / client_key_file are configured, so the successor must be "
            "yours: pass --cert NEW --key NEW (absolute paths of a new pair, different "
            "from the configured files, which stay in use until every server has accepted "
            "the successor)"
        )
    if not files.configured and cert is not None:
        raise click.UsageError(
            "--cert/--key are for installations that set client_cert_file/client_key_file; "
            "the generated identity rotates by itself: run `zfsbackup-config identity rotate`"
        )

    # A re-run first finishes a swap that was interrupted between its renames.
    if files.generated_in is not None:
        try:
            finished = identity.complete_promotion(files.generated_in)
        except (identity.IdentityError, OSError) as exc:
            raise _identity_failure(exc) from None
        if finished is not None:
            _echo(
                "finished an interrupted rotation: the client certificate is now "
                f"{finished.fingerprint}"
            )
            _echo(
                "Every server had already accepted it. Nothing else to do; run "
                "`identity rotate` again to rotate once more."
            )
            return

    client_id = snapshot.client_id
    enrolled = {
        n: d for n, d in sorted(snapshot.config.destinations.items()) if d.server_fingerprint
    }
    if not client_id or not enrolled:
        raise CliError(
            "no destination is enrolled, so there is nothing to rotate; "
            f"`zfsbackup-config{where} enroll NAME --url URL` creates the client identity"
        )

    try:
        current = identity.load_identity(files.cert, files.key, identity.ROLE_CLIENT)
    except (identity.IdentityError, OSError) as exc:
        raise _identity_failure(exc) from None

    new_cert: Optional[Path] = None
    new_key: Optional[Path] = None
    try:
        if files.configured:
            new_cert = Path(os.path.abspath(os.path.expanduser(cert)))
            new_key = Path(os.path.abspath(os.path.expanduser(key)))
            if new_cert == files.cert or new_key == files.key:
                raise CliError(
                    "--cert/--key name the currently configured files; the successor must "
                    "be a different pair (the current one stays in use until every server "
                    "has accepted the successor)"
                )
            successor = identity.load_identity(new_cert, new_key, identity.ROLE_CLIENT)
        else:
            pending = os.path.lexists(
                identity.identity_paths(files.generated_in, identity.CLIENT_NEXT_NAME)[0]
            )
            successor = identity.ensure_identity(
                files.generated_in, identity.ROLE_CLIENT, name=identity.CLIENT_NEXT_NAME
            )
            if pending:
                state.warn(
                    "reusing the successor certificate left by an earlier `identity rotate` "
                    f"(client.next, {successor.fingerprint})"
                )
    except (identity.IdentityError, OSError) as exc:
        raise _identity_failure(exc) from None
    if successor.fingerprint == current.fingerprint:
        raise CliError("the successor certificate is the current certificate; nothing to rotate")

    results = [_rotate_one(n, d, client_id, current, successor) for n, d in enrolled.items()]
    _echo(_table(
        ["DESTINATION", "RESULT", "URL", "DETAIL"],
        [[r.name, r.result, r.url, r.detail] for r in results],
    ).rstrip("\n"))

    failed = [r for r in results if not r.ok]
    if failed:
        refused = any(r.result == "refused" for r in failed)
        names = ", ".join(repr(r.name) for r in failed)
        again = (
            f"re-run `zfsbackup-config{where} identity rotate"
            + (f" --cert {new_cert} --key {new_key}" if files.configured else "")
            + "`"
        )
        message = (
            f"not rotated: {len(failed)} of {len(results)} destination(s) did not accept "
            f"the successor certificate ({names}). NOTHING was swapped; this host still "
            f"uses {current.fingerprint}.\n"
            f"Fix or remove the failing destination(s), then {again}: the same successor "
            "certificate is reused and servers that already accepted it answer `already`."
        )
        if refused:
            # A server that REVOKED the pending successor (for example because
            # an earlier rotation was undone on the server) refuses it for ever.
            accepted = [r for r in results if r.ok]
            caution = ""
            if accepted:
                # A server that accepted the successor refuses any other
                # successor for the same current certificate.
                caution = (
                    f"\nDestination(s) {', '.join(repr(r.name) for r in accepted)} already "
                    f"accepted the successor {successor.fingerprint} (answered rotated/"
                    "already) and will refuse a different successor for the current "
                    f"certificate: on each of those servers first run `zfsbackup-config "
                    f"trust revoke {successor.fingerprint}`, or enroll the new certificate "
                    "there with a token created by `trust token create --client-id ID "
                    "--replace`."
                )
            if files.configured:
                message += (
                    "\nIf a server revoked this successor it can never be accepted: re-run "
                    "with a different --cert/--key pair." + caution
                )
            else:
                nxt_cert, nxt_key = identity.identity_paths(
                    files.generated_in, identity.CLIENT_NEXT_NAME
                )
                message += (
                    "\nIf a server revoked this successor it can never be accepted: delete "
                    f"{nxt_cert} and {nxt_key} and re-run to generate a new one." + caution
                )
            raise CliError(message)
        raise _Retry(message)

    # A server that accepted the successor but speaks no supported protocol
    # still holds it as pending (the server-side change cannot be undone from
    # here) and cannot take backups either way, so the swap goes ahead exactly
    # as on success: holding back would leave a possibly stolen current key in
    # use at every destination that works. The exit status reports the problem.
    mismatched = [r for r in results if r.protocol_problem]
    if not files.configured:
        _swap_generated(files, current, successor, where)
    else:
        _swap_configured(
            state, resolved, snapshot, new_cert, new_key, current, successor, results
        )
    if mismatched:
        names = ", ".join(repr(r.name) for r in mismatched)
        raise CliError(
            f"the client certificate WAS rotated, but destination(s) {names} accepted the "
            "successor while speaking an unsupported wire protocol (or none): backups to "
            "them will be refused until client and server run matching versions. The "
            "server-side change was NOT undone.\n"
            + "\n".join(
                str(_protocol_mismatch(r.url, r.server_protocol)) for r in mismatched
            )
        )


def _swap_generated(
    files: tlsfiles.ClientFiles,
    current: identity.Identity,
    successor: identity.Identity,
    where: str,
) -> None:
    try:
        promoted = identity.promote_identity(files.generated_in)
    except (identity.IdentityError, OSError) as exc:
        error = _identity_failure(exc)
        raise CliError(
            f"every server accepted the successor, but it could not be installed here: "
            f"{error}\nRe-run `zfsbackup-config{where} identity rotate` as root or as the "
            "key's owner; the servers answer `already`."
        ) from None
    _echo(f"client certificate rotated: {current.fingerprint} -> {promoted.fingerprint}")
    _echo(
        "A running daemon switches to it within one cycle (no signal needed); each server "
        "retires the old certificate on the first request made with the new one."
    )


def _swap_configured(
    state: Any,
    resolved: Any,
    snapshot: Any,
    new_cert: Path,
    new_key: Path,
    current: identity.Identity,
    successor: identity.Identity,
    results: List[_RotateResult],
) -> None:
    new = copy.copy(snapshot.config)
    new.client_cert_file = new_cert
    new.client_key_file = new_key
    try:
        result = commit_config(resolved, snapshot, new, warn=state.warn, announce=False)
    except (ConfigConflict, ConfigBusy) as exc:
        if hasattr(exc, "add_note"):
            exc.add_note(
                "Every server accepted the successor but the configuration was not updated. "
                f"Re-run `zfsbackup-config{where_flag(resolved)} identity rotate --cert "
                f"{new_cert} --key {new_key}`: the servers answer `already`."
            )
        raise
    request = begin_reload(resolved, result.generation, result.store_id)
    _echo(f"client certificate rotated: {current.fingerprint} -> {successor.fingerprint}")
    _echo(
        f"client_cert_file / client_key_file now point at {new_cert} and {new_key}; each "
        "server retires the old certificate on the first request made with the new one."
    )
    notify_daemon(resolved, result.generation, result.store_id, request=request)


# --------------------------------------------------------------------------
# Click wiring
# --------------------------------------------------------------------------


def register(
    cli: click.Group,
    common: Callable[[Callable], Callable],
    get_state: Callable[[Optional[str], bool], Any],
) -> None:
    """Add `enroll` to `cli` and `rotate` to its `identity` group (so call
    this after `trust.register`)."""

    @cli.command(
        "enroll",
        help=(
            "Enroll this host with a backup server (destination DESTINATION) using the "
            "one-time token its operator created with `trust token create`. The token "
            "carries the server's certificate fingerprint, which is pinned; this host's "
            "client certificate (and client id, if it has none) are generated if needed. "
            "Give the token with --token-file PATH, --token - (stdin) or at the hidden "
            "prompt; never on the command line. --url is required for a new destination "
            "(https only). Running it again for an enrolled destination is safe: the "
            "server is asked first and the token is not used. A certificate the server "
            "revoked can never be enrolled again: use --new-key, which generates a new "
            "client key (client.next), enrolls it and, once the server accepted it, makes "
            "it this host's client certificate; the same applies to a token created with "
            "--replace while this host is still trusted. A pending client.next that no "
            "server trusts is discarded and replaced by a fresh key (it is reused only "
            "when the server at this destination already trusts it; another server that "
            "accepted it as a rotation successor must be enrolled again). Other enrolled destinations still trust "
            "the old certificate and must be enrolled again (plain `enroll`, with a token "
            "their operators create with `trust token create --client-id ID --replace`). "
            "With client_cert_file/client_key_file configured, give the new pair with "
            "--cert/--key."
        ),
    )
    @click.argument("destination", metavar="DESTINATION")
    @click.option("--url", "url", default=None, metavar="URL",
                  help="The server's https:// URL (required for a new destination).")
    @click.option("--label", default=None, metavar="TEXT",
                  help="A label the server shows for this host (at most 128 characters).")
    @click.option("--token-file", "token_file", default=None, metavar="PATH",
                  help="Read the enrollment token from this file.")
    @click.option("--token", "token_opt", default=None, metavar="-",
                  help="Read the enrollment token from stdin ('-' is the only value).")
    @click.option("--new-key", "new_key", is_flag=True, default=False,
                  help="Enroll a freshly generated client key instead of the current one "
                       "(after the current certificate was revoked or may be stolen, or with "
                       "a --replace token).")
    @click.option("--cert", "cert", default=None, metavar="FILE",
                  help="With --new-key and client_cert_file set: the new certificate.")
    @click.option("--key", "key", default=None, metavar="FILE",
                  help="With --new-key and client_key_file set: the new private key.")
    @common
    def enroll_cmd(
        destination: str, url: Optional[str], label: Optional[str],
        token_file: Optional[str], token_opt: Optional[str],
        new_key: bool, cert: Optional[str], key: Optional[str],
        config_path: Optional[str], verbose: bool,
    ) -> None:
        enroll(
            get_state(config_path, verbose), destination, url, label, token_file, token_opt,
            new_key, cert, key,
        )

    identity_group = cli.commands["identity"]
    assert isinstance(identity_group, click.Group)

    @identity_group.command(
        "rotate",
        help=(
            "Replace this host's client certificate without re-enrolling. A successor "
            "is created (client.next.*) and registered with EVERY enrolled destination, "
            "authenticated with the current certificate; only if all accept it are the "
            "files swapped. A failure on any destination swaps nothing; fix or remove it "
            "and re-run (servers that already accepted answer `already`). With "
            "client_cert_file/client_key_file configured, give the new pair with "
            "--cert/--key (different files) and the configuration is updated."
        ),
    )
    @click.option("--cert", "cert", default=None, metavar="FILE",
                  help="The new certificate (with --key; required if client_cert_file is set).")
    @click.option("--key", "key", default=None, metavar="FILE",
                  help="The new private key.")
    @common
    def identity_rotate_cmd(
        cert: Optional[str], key: Optional[str], config_path: Optional[str], verbose: bool,
    ) -> None:
        identity_rotate(get_state(config_path, verbose), cert, key)

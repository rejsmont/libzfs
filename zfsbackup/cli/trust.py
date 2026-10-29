"""`zfsbackup-config trust ...` and `zfsbackup-config identity ...` (TLS/mTLS
plan M12): the operator's side of enrollment, and this host's own identity.

Commands (all take `-c/--config` and `-v/--verbose`, before or after the
subcommand like every other command)::

    trust token create [--ttl 1h] [--client-id ID [--replace]] [--json]
    trust token show   [--json]
    trust token revoke
    trust list         [--json]
    trust revoke FINGERPRINT | --client ID
    identity show      [--json]
    identity set-client-id ID [--force]
    identity rotate [--cert NEW --key NEW]   # enroll.py (plan M14c)

The server side (`trust ...`)
-----------------------------
- `trust token create` makes the ONE enrollment token (a new one replaces the
  old, with a warning if the old one was still active). It needs the server's
  certificate fingerprint, which is part of the token: it reads the
  certificate with `identity.peek_identity` (never the key, so a group
  operator who cannot read the root-owned `server.key` can still issue
  tokens) and, if there is none yet, tries `identity.ensure_identity` (which
  needs write access to `<db>.tls/`: root, or the directory's owner). The
  token goes to STDOUT only -- it is a secret, so it is never written to
  stderr, a log or an exception message (`identity.SecretStr` keeps it out of
  reprs) and only its SHA-256 is stored.
- `trust revoke` unpins one certificate (a unique prefix of at least 8 hex
  characters of its fingerprint is accepted; the full one is echoed) or, with
  `--client ID`, every certificate of a client id and tombstones the id.
  Revoking a certificate also revokes every pending rotation successor it
  registered (transitively, reason `successor_revoked`), and says so.
- None of the trust commands signals the daemon: trust writes are not config
  generations, and the API worker polls its own trust generation.

The client side (`identity ...`)
--------------------------------
- `identity show` is read-only and generates nothing.
- `identity set-client-id` is a CONFIG write (the store bumps the config
  generation with the id, so a running daemon reloads and hands the new id to
  its workers): compare-and-swap on the snapshot's `(store_id, generation)`,
  then exactly one `begin_reload` before any output.

`--json` shapes (stable; keys may be added, none renamed or removed). Times
are UTC ISO-8601 strings (`2026-10-20T12:00:00Z`) or `null`::

    trust token create --json -> {
      "token": str,                    # the secret; shown only here
      "server_fingerprint": str,       # 64 lowercase hex
      "created_at": str, "expires_at": str,
      "client_id": str | null, "replace": bool,
      "replaced_previous": bool        # an ACTIVE token was replaced
    }
    trust token show --json -> {
      "state": "none" | "active" | "expired" | "consumed",
      "created_at": str | null, "expires_at": str | null,
      "server_fingerprint": str | null,
      "server_certificate_matches": bool | null,   # token vs the current server cert
      "client_id": str | null, "replace": bool,
      "consumed_at": str | null, "consumed_fingerprint": str | null,
      "consumed_client_id": str | null
    }
    trust list --json -> [
      {"client_id": str, "label": str | null, "status": "active" | "revoked",
       "created_at": str, "revoked_at": str | null,
       "pins": [{"fingerprint": str, "enrolled_via": "token" | "rotate",
                 "enrolled_at": str, "not_after": str, "expired": bool,
                 "predecessor": str | null}, ...],
       "revoked_pins": [{"fingerprint": str, "revoked_at": str,
                         "reason": "revoked" | "successor_revoked" |
                                   "client_revoked" | "replaced" | "retired"}, ...]}, ...
    ]
      (a revoked fingerprint is refused for ever: it can never be enrolled
      again, so the client must enroll a new key with `enroll --new-key`)
    identity show --json -> {
      "client_id": str | null,
      "client_certificate": CERT, "server_certificate": CERT
    }
      CERT = {"path": str, "key_path": str, "source": "configured" | "default",
              "exists": bool, "fingerprint": str | null, "not_after": str | null,
              "expired": bool, "key_present": bool, "error": str | null}
      ("source" is always "default" for the server certificate.)
"""

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import click

from zfsbackup import identity
from zfsbackup.cli import tlsfiles
from zfsbackup.cli.errors import CliError
from zfsbackup.cli.write import (
    begin_reload,
    notify_daemon,
    read_snapshot,
    release_database,
    where_flag,
)
from zfsbackup.config import Duration, client_id_problem
from zfsbackup.config.store import (
    TOKEN_MAX_TTL_SECONDS,
    ClientRecord,
    TokenState,
    TokenStatus,
    create_token,
    list_clients,
    revoke_client,
    revoke_pin,
    revoke_token,
    set_client_id,
    token_status,
)

__all__ = ["register"]

#: Shortest fingerprint prefix `trust revoke` accepts.
MIN_PREFIX = 8
#: Fingerprint characters shown in tables (`trust revoke` accepts any prefix of
#: at least `MIN_PREFIX`; `--json` and `identity show` carry the full value).
_SHOWN = 16

_REVOKE_NOTE = (
    "New connections are refused and streams already in flight are cut within ~2 s "
    "(a partial receive is kept for a resume); data under {target}/{client_id} is left in place.\n"
    "A revoked certificate can never be enrolled again: if the client should come back, "
    "it must enroll a NEW key with a new token (`zfsbackup-config enroll NAME --new-key`)."
)


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------


def _iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_dt(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp(ts: Optional[float]) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _span(seconds: float) -> str:
    """`59m`, `2d3h`: a duration as the repo writes them."""
    return Duration(timedelta(seconds=max(0, int(seconds)))).literal


def _relative(ts: float, now: float) -> str:
    return f"in {_span(ts - now)}" if ts > now else f"{_span(now - ts)} ago"


def _table(header: List[str], rows: List[List[str]]) -> str:
    widths = [len(h) for h in header]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def line(cells: List[str]) -> str:
        padded = [c.ljust(widths[i]) for i, c in enumerate(cells[:-1])] + [cells[-1]]
        return "  ".join(padded).rstrip()

    return "".join(line(r) + "\n" for r in [header] + rows)


def _dump(data: Any) -> str:
    return json.dumps(data, indent=2) + "\n"


@contextmanager
def _operator_errors() -> Iterator[None]:
    """Turn the exceptions the CLI's error table does not know into `CliError`
    (exit 1): a TLS identity problem and an unknown id/fingerprint
    (`LookupError`). Everything else (`ConfigBusy` -> 75, `SchemaError` -> 78,
    `ValueError` -> 1, ...) propagates to `errors.report`. The original is
    dropped (`from None`): nothing here may carry a secret into a message."""
    try:
        yield
    except identity.IdentityError as exc:
        raise CliError(str(exc)) from None
    except LookupError as exc:
        raise CliError(str(exc.args[0]) if exc.args else "not found") from None


def _parse_ttl(text: str) -> float:
    try:
        seconds = Duration(text).total_seconds()
    except ValueError as exc:
        raise CliError(f"invalid --ttl {text!r}: {exc} (examples: 30m, 1h, 2d)") from None
    if seconds <= 0:
        raise CliError(f"--ttl must be positive, got {text!r}")
    if seconds > TOKEN_MAX_TTL_SECONDS:
        raise CliError(
            f"--ttl {text} is longer than the maximum token lifetime of 7 days (7d)"
        )
    return seconds


def _target_dataset(state: Any) -> Optional[str]:
    """The server's `remote_backup.target_dataset`, best effort: used only to
    word a note, so any failure to read it is just `None` (a revoke must never
    be blocked by an unreadable configuration)."""
    try:
        snapshot = read_snapshot(state.resolve())
    except Exception:  # noqa: BLE001 - cosmetic only; the write already happened
        return None
    remote = snapshot.config.remote_backup
    return remote.target_dataset if remote is not None else None


def _peek_server(resolved: Any) -> Optional[identity.CertInfo]:
    return identity.peek_identity(tlsfiles.tls_directory(resolved), identity.SERVER_NAME)


# --------------------------------------------------------------------------
# trust token ...
# --------------------------------------------------------------------------


def _server_fingerprint(resolved: Any, state: Any) -> str:
    """The server certificate's fingerprint, generating the identity if there
    is none and this user may. A certificate that exists is only peeked (a
    group operator cannot read the key and does not need to)."""
    tdir = tlsfiles.tls_directory(resolved)
    with _operator_errors():
        info = _peek_server(resolved)
    if info is not None:
        return info.fingerprint
    try:
        ident = identity.ensure_identity(tdir, identity.ROLE_SERVER)
    except (identity.IdentityError, OSError) as exc:
        reason = str(exc) if isinstance(exc, identity.IdentityError) else (exc.strerror or str(exc))
        raise CliError(
            f"this host has no server TLS identity yet and it could not be created: {reason}. "
            f"Start the daemon once (not with --dry-run: it creates {tdir}/server.crt at its "
            "first start), or run this command as root (or as the owner of that directory)."
        ) from None
    state.warn(
        f"generated a new server TLS identity in {tdir} (fingerprint {ident.fingerprint})"
    )
    return ident.fingerprint


def _unknown_client_message(resolved: Any, what: str) -> str:
    return (
        f"{what}. See `zfsbackup-config{where_flag(resolved)} trust list`; omit "
        "--client-id to let a new client id enroll."
    )


def token_create(
    state: Any, ttl: str, client_id: Optional[str], replace: bool, as_json: bool
) -> None:
    state.quiet()
    resolved = state.resolve()
    if replace and client_id is None:
        raise click.UsageError("--replace needs --client-id (a token can only replace a known id)")
    if client_id is not None:
        problem = client_id_problem(client_id)
        if problem:
            raise CliError(f"invalid --client-id {client_id!r}: {problem}")
    seconds = _parse_ttl(ttl)

    # Also the preflight: a missing database, the wrong schema or a stored
    # configuration that cannot be loaded stop here, before anything is
    # generated next to the database.
    snapshot = read_snapshot(resolved)
    remote = snapshot.config.remote_backup
    if remote is None or not remote.enabled:
        state.warn(
            "remote_backup is not enabled in the stored configuration, so this host's API "
            "will not accept enrollments; enable it with `zfsbackup-config"
            f"{where_flag(resolved)} set remote_backup.enabled true` "
            "(remote_backup needs a target_dataset)"
        )

    if client_id is not None:
        # Before anything is generated: an unknown id must leave no trace (no
        # server identity created next to the database). `create_token` checks
        # again under the writer lock, which covers a revoke that races us.
        known = sorted(c.client_id for c in list_clients(resolved))
        release_database()
        if client_id not in known:
            raise CliError(_unknown_client_message(resolved, f"unknown client id {client_id!r}"))

    server_fp = _server_fingerprint(resolved, state)
    token, digest = identity.make_token(server_fp)
    now = time.time()
    try:
        previous = create_token(
            resolved,
            secret_sha256=digest,
            server_fingerprint=server_fp,
            created_at=now,
            expires_at=now + seconds,
            client_id=client_id,
            replace=replace,
            lock=None,
        )
    except LookupError as exc:
        raise CliError(
            _unknown_client_message(resolved, str(exc.args[0]) if exc.args else "unknown client id")
        ) from None
    finally:
        release_database()

    replaced_active = previous.state == TokenState.ACTIVE
    if replaced_active:
        state.warn(
            "replaced the previous enrollment token, which was still active "
            f"(it would have expired {_relative(previous.expires_at, now)}); it can no longer be used"
        )
    if replace:
        state.warn(
            f"enrolling with this token REPLACES every certificate pinned for client id "
            f"{client_id!r}; the old ones stop working. The replace happens when the client "
            "enrolls WITH the token: a client whose current certificate is still trusted "
            "here must run `zfsbackup-config enroll NAME --new-key` (a plain `enroll` "
            "sees it is already enrolled and does not send the token). The cost: the "
            "replace takes effect only through that `--new-key` enrollment, which gives the "
            "client a fresh key; a client that is enrolled at other servers too then needs "
            "its own `trust token create --client-id "
            f"{client_id} --replace` token from each of them and a plain `enroll` there"
        )

    expires_at = now + seconds
    if as_json:
        click.echo(_dump({
            "token": str(token),
            "server_fingerprint": server_fp,
            "created_at": _iso(now),
            "expires_at": _iso(expires_at),
            "client_id": client_id,
            "replace": replace,
            "replaced_previous": replaced_active,
        }), nl=False)
        return
    scope = (
        "any new client id" if client_id is None
        else f"client id {client_id}" + (" (replacing its certificates)" if replace else "")
    )
    # A replace token only acts when the client sends it, and a plain `enroll`
    # does not send it for a certificate the server already trusts.
    new_key_flag = " --new-key" if replace else ""
    note = ""
    if replace:
        note = (
            "\n(--new-key: the replace applies at that enrollment; a plain `enroll` would "
            "find the host already enrolled and not use the token.)"
        )
    elif client_id is not None:
        note = (
            "\n(If this client's certificate was revoked here, add --new-key: a revoked "
            "certificate can never be enrolled again.)"
        )
    click.echo(
        "Enrollment token (single use; shown only now, only its hash is stored):\n"
        f"\n  {token}\n\n"
        f"server fingerprint: {server_fp}\n"
        f"valid for:          {_span(seconds)} (until {_stamp(expires_at)})\n"
        f"usable by:          {scope}\n"
        "\nOn the client host, put the token in a file and run:\n"
        f"  zfsbackup-config enroll NAME --url https://<this-host>:{snapshot.config.api_port} "
        f"--token-file FILE{new_key_flag}" + note
    )


def _token_scope(status: TokenStatus) -> str:
    if status.client_id is None:
        return "any new client id"
    return f"client id {status.client_id}" + (
        " (replacing its certificates)" if status.replace else " (adding a certificate)"
    )


def token_show(state: Any, as_json: bool) -> None:
    state.quiet()
    resolved = state.resolve()
    now = time.time()
    status = token_status(resolved, now)
    release_database()

    matches: Optional[bool] = None
    if status.state == TokenState.ACTIVE and status.server_fingerprint is not None:
        try:
            info = _peek_server(resolved)
        except identity.IdentityError:
            info = None
        if info is not None:
            matches = info.fingerprint == status.server_fingerprint
            if not matches:
                state.warn(
                    "the token was issued for server fingerprint "
                    f"{status.server_fingerprint}, but this host's server certificate is now "
                    f"{info.fingerprint}; a client enrolling with it pins the old one and will "
                    "refuse this server. Create a new token."
                )

    if as_json:
        click.echo(_dump({
            "state": str(status.state),
            "created_at": _iso(status.created_at),
            "expires_at": _iso(status.expires_at),
            "server_fingerprint": status.server_fingerprint,
            "server_certificate_matches": matches,
            "client_id": status.client_id,
            "replace": status.replace,
            "consumed_at": _iso(status.consumed_at),
            "consumed_fingerprint": status.consumed_fingerprint,
            "consumed_client_id": status.consumed_client_id,
        }), nl=False)
        return
    if status.state == TokenState.NONE:
        click.echo("state: none (no enrollment token; create one with `zfsbackup-config trust token create`)")
        return
    lines = [f"state:              {status.state}"]
    lines.append(f"server fingerprint: {status.server_fingerprint}")
    lines.append(f"created:            {_stamp(status.created_at)}")
    if status.expires_at is not None:
        lines.append(f"expires:            {_stamp(status.expires_at)} ({_relative(status.expires_at, now)})")
    lines.append(f"usable by:          {_token_scope(status)}")
    if status.state == TokenState.CONSUMED:
        lines.append(f"consumed:           {_stamp(status.consumed_at)}")
        lines.append(f"  by fingerprint:   {status.consumed_fingerprint or '-'}")
        lines.append(f"  as client id:     {status.consumed_client_id or '-'}")
    click.echo("\n".join(lines))


def token_revoke(state: Any) -> None:
    resolved = state.resolve()
    with _operator_errors():
        removed = revoke_token(resolved, lock=None)
    release_database()
    if removed:
        click.echo("Revoked the enrollment token; it can no longer be used.")
    else:
        click.echo("There is no enrollment token; nothing changed.")


# --------------------------------------------------------------------------
# trust list / trust revoke
# --------------------------------------------------------------------------


def _pin_json(pin: Any, now: float) -> Dict[str, Any]:
    return {
        "fingerprint": pin.fingerprint,
        "enrolled_via": pin.enrolled_via,
        "enrolled_at": _iso(pin.enrolled_at),
        "not_after": _iso(pin.not_after),
        "expired": pin.not_after <= now,
        "predecessor": pin.predecessor,
    }


def _revoked_json(revoked: Any) -> Dict[str, Any]:
    entry = {
        "fingerprint": revoked.fingerprint,
        "revoked_at": _iso(revoked.revoked_at),
        "reason": revoked.reason,
    }
    successor = getattr(revoked, "superseded_by", None)
    if successor:
        entry["superseded_by"] = successor
    return entry


_REASON_TEXT = {
    "successor_revoked": "successor_revoked (registered by a revoked certificate via /rotate)",
}


def _reason_text(reason: str, superseded_by: Optional[str] = None) -> str:
    if reason == "retired" and superseded_by:
        return f"retired (replaced by {superseded_by[:_SHOWN]})"
    return _REASON_TEXT.get(reason, reason)


def trust_list(state: Any, as_json: bool) -> None:
    state.quiet()
    resolved = state.resolve()
    now = time.time()
    clients = list_clients(resolved)
    release_database()

    if as_json:
        click.echo(_dump([
            {
                "client_id": c.client_id,
                "label": c.label,
                "status": "revoked" if c.revoked else "active",
                "created_at": _iso(c.created_at),
                "revoked_at": _iso(c.revoked_at),
                "pins": [_pin_json(p, now) for p in c.pins],
                "revoked_pins": [_revoked_json(r) for r in c.revoked_pins],
            }
            for c in clients
        ]), nl=False)
        return
    revoked_rows = [
        [r.fingerprint[:_SHOWN], c.client_id, _stamp(r.revoked_at),
         _reason_text(r.reason, getattr(r, "superseded_by", None))]
        for c in clients for r in c.revoked_pins
    ]
    if not clients:
        click.echo("(no clients)")
        return
    rows: List[List[str]] = []
    for c in clients:
        status = "active" if not c.revoked else f"revoked {_stamp(c.revoked_at)}"
        head = [c.client_id, status, c.label or "-"]
        if not c.pins:
            rows.append(head + ["-", "-", "-", "-", "-"])
            continue
        for i, pin in enumerate(c.pins):
            expiry = _stamp(pin.not_after) + (" (expired)" if pin.not_after <= now else "")
            rows.append(
                (head if i == 0 else ["", "", ""])
                + [
                    pin.fingerprint[:_SHOWN],
                    pin.enrolled_via,
                    _stamp(pin.enrolled_at),
                    expiry,
                    pin.predecessor[:_SHOWN] if pin.predecessor else "-",
                ]
            )
    click.echo(
        _table(
            ["CLIENT ID", "STATUS", "LABEL", "FINGERPRINT", "VIA", "ENROLLED", "NOT AFTER", "PREDECESSOR"],
            rows,
        ),
        nl=False,
    )
    if revoked_rows:
        click.echo(
            "\nrevoked certificates (refused for ever; a client must enroll a new key):\n"
            + _table(["FINGERPRINT", "CLIENT ID", "REVOKED", "REASON"], revoked_rows),
            nl=False,
        )
    click.echo(
        f"(fingerprints shortened to {_SHOWN} characters; `trust revoke` accepts any unique "
        f"prefix of at least {MIN_PREFIX}, `--json` has the full values)",
        err=True,
    )


def _normalise_prefix(text: str) -> str:
    cleaned = text.strip().replace(":", "").lower()
    if (
        len(cleaned) < MIN_PREFIX
        or len(cleaned) > 64
        or any(ch not in "0123456789abcdef" for ch in cleaned)
    ):
        raise CliError(
            f"{text!r} is not a certificate fingerprint: give at least {MIN_PREFIX} and at most "
            "64 hexadecimal characters (a prefix of the SHA-256 fingerprint; see `trust list`)"
        )
    return cleaned


def _revocation_note(state: Any, client_id: str) -> str:
    target = _target_dataset(state) or "<target>"
    return _REVOKE_NOTE.format(target=target, client_id=client_id)


def trust_revoke(state: Any, fingerprint: Optional[str], client_id: Optional[str]) -> None:
    if (fingerprint is None) == (client_id is None):
        raise click.UsageError("give exactly one of FINGERPRINT or --client ID")
    state.quiet()
    resolved = state.resolve()
    clients = list_clients(resolved)
    release_database()

    if client_id is not None:
        _revoke_client(state, resolved, clients, client_id)
        return

    prefix = _normalise_prefix(fingerprint)
    matches: List[Tuple[str, Any]] = [
        (c.client_id, p) for c in clients for p in c.pins if p.fingerprint.startswith(prefix)
    ]
    if not matches:
        gone = [(c.client_id, r) for c in clients for r in c.revoked_pins
                if r.fingerprint.startswith(prefix)]
        if gone:
            if len({r.fingerprint for _, r in gone}) > 1:
                listed = "\n".join(
                    f"  {r.fingerprint}  (client id {cid}, revoked)" for cid, r in gone
                )
                raise CliError(
                    f"{prefix!r} matches {len(gone)} revoked certificates; give more "
                    f"characters:\n{listed}"
                )
            cid, r = gone[0]
            successor = getattr(r, "superseded_by", None)
            chained = r.reason == "retired" and bool(successor)
            if chained and _revoke_retired_chain(state, resolved, clients, r):
                return
            click.echo(
                f"certificate {r.fingerprint} (client id {cid}) is already revoked "
                f"({_reason_text(r.reason, successor)}, {_stamp(r.revoked_at)})"
                + ("; nothing that superseded it is still pinned" if chained else "")
                + "; nothing changed."
            )
            return
        raise CliError(
            f"no pinned certificate has a fingerprint starting with {prefix}; see "
            "`zfsbackup-config trust list`"
        )
    if len(matches) > 1:
        listed = "\n".join(f"  {p.fingerprint}  (client id {cid})" for cid, p in matches)
        raise CliError(
            f"{prefix!r} matches {len(matches)} pinned certificates; give more characters:\n{listed}"
        )
    owner, pin = matches[0]
    with _operator_errors():
        removed = revoke_pin(resolved, pin.fingerprint, lock=None)
    release_database()
    # The cascade removes pending rotation successors with it (any client id
    # in principle, in practice the same one): none of them counts as "other".
    cascaded = tuple(getattr(removed, "cascaded", ()))
    gone = {pin.fingerprint} | {p.fingerprint for p in cascaded}
    remaining = [p for c in clients if c.client_id == owner for p in c.pins if p.fingerprint not in gone]
    click.echo(f"Revoked certificate {removed.fingerprint} of client id {owner}.")
    for extra in cascaded:
        click.echo(
            f"also revoked {extra.fingerprint} (client id {extra.client_id}; registered by the "
            "revoked certificate via /rotate)"
        )
    if not remaining:
        click.echo(
            f"client id {owner} has no other pinned certificate, so it can no longer connect "
            "(the id itself stays known; `trust revoke --client` also retires the id)"
        )
    click.echo(_revocation_note(state, owner))


def _revoke_retired_chain(state: Any, resolved: Any, clients: Tuple[ClientRecord, ...],
                          retired: Any) -> bool:
    """`trust revoke` of a certificate that a rotation already retired: the
    store follows `superseded_by` and revokes the live successor (and that
    one's pending successors), the keys that still have access. True if
    something live was revoked; False if nothing remained (`LookupError`)."""
    try:
        result = revoke_pin(resolved, retired.fingerprint, lock=None)
    except LookupError:
        return False
    finally:
        release_database()
    cascaded = list(result.cascaded)
    direct = next((p for p in cascaded if p.fingerprint == retired.superseded_by), None)
    if direct is None:
        direct = cascaded[0]
    rest = [p for p in cascaded if p is not direct]
    line = (
        f"certificate {retired.fingerprint} (client id {result.client_id}) was already "
        f"retired by its successor {retired.superseded_by or direct.fingerprint}; "
        f"revoked {direct.fingerprint}"
    )
    if rest:
        line += " (and its successors " + ", ".join(p.fingerprint for p in rest) + ")"
    click.echo(line + ".")
    gone = {p.fingerprint for p in cascaded}
    if not any(
        p.fingerprint not in gone
        for c in clients if c.client_id == result.client_id for p in c.pins
    ):
        click.echo(
            f"client id {result.client_id} has no other pinned certificate, so it can no "
            "longer connect (the id itself stays known; `trust revoke --client` also "
            "retires the id)"
        )
    click.echo(_revocation_note(state, result.client_id))
    return True


def _revoke_client(state: Any, resolved: Any, clients: Tuple[ClientRecord, ...], client_id: str) -> None:
    known = {c.client_id: c for c in clients}
    record = known.get(client_id)
    if record is None:
        ids = ", ".join(sorted(known)) or "(none)"
        raise CliError(f"unknown client id {client_id!r}; known client ids: {ids}")
    if record.revoked and not record.pins:
        click.echo(f"client id {client_id} is already revoked; nothing changed.")
        return
    with _operator_errors():
        done = revoke_client(resolved, client_id, lock=None)
    release_database()
    n = len(done.pins)
    click.echo(
        f"Revoked client id {client_id}: {n} pinned certificate(s) removed"
        + ("".join(f"\n  {p.fingerprint}" for p in done.pins))
    )
    click.echo(_revocation_note(state, client_id))


# --------------------------------------------------------------------------
# identity ...
# --------------------------------------------------------------------------


def _cert_json(summary: tlsfiles.CertSummary, key: Any, source: str) -> Dict[str, Any]:
    return {
        "path": str(summary.path),
        "key_path": str(key),
        "source": source,
        "exists": summary.exists,
        "fingerprint": summary.fingerprint,
        "not_after": _iso_dt(summary.not_after),
        "expired": summary.expired,
        "key_present": os.path.lexists(key),
        "error": summary.error,
    }


def _cert_lines(title: str, entry: Dict[str, Any], note_missing: str) -> List[str]:
    lines = [f"{title}:"]
    if not entry["exists"]:
        lines.append(f"  none ({note_missing})")
    elif entry["error"]:
        lines.append(f"  unreadable: {entry['error']}")
    else:
        lines.append(f"  fingerprint: {entry['fingerprint']}")
        expiry = entry["not_after"] + (" (EXPIRED)" if entry["expired"] else "")
        lines.append(f"  expires:     {expiry}")
        if not entry["key_present"]:
            lines.append("  key:         MISSING (" + entry["key_path"] + ")")
    lines.append(f"  file:        {entry['path']}" + (
        " (configured)" if entry["source"] == "configured" else ""
    ))
    return lines


def identity_show(state: Any, as_json: bool) -> None:
    state.quiet()
    resolved = state.resolve()
    snapshot = read_snapshot(resolved)
    client_files = tlsfiles.client_identity_files(snapshot.config, resolved)
    server_cert, server_key = tlsfiles.server_identity_files(resolved)
    client = _cert_json(
        tlsfiles.describe_certificate(client_files.cert),
        client_files.key,
        "configured" if client_files.configured else "default",
    )
    server = _cert_json(tlsfiles.describe_certificate(server_cert), server_key, "default")

    if as_json:
        click.echo(_dump({
            "client_id": snapshot.client_id,
            "client_certificate": client,
            "server_certificate": server,
        }), nl=False)
        return
    lines = [
        "client id: " + (
            snapshot.client_id if snapshot.client_id
            else "not set -- generated by `zfsbackup-config enroll`"
        )
    ]
    lines += _cert_lines(
        "client certificate", client, "generated by `zfsbackup-config enroll`"
    )
    lines += _cert_lines(
        "server certificate", server, "created when the daemon first starts (not with "
        "--dry-run), or by `trust token create`"
    )
    click.echo("\n".join(lines))


def identity_set_client_id(state: Any, new_id: str, force: bool) -> None:
    problem = client_id_problem(new_id)
    if problem:
        raise CliError(f"invalid client id {new_id!r}: {problem}")
    state.quiet()
    resolved = state.resolve()
    snapshot = read_snapshot(resolved)
    previous = snapshot.client_id
    if previous == new_id:
        click.echo(f"client id is already {new_id}; nothing changed")
        return

    # With no id stored yet nothing can be bound to an old one (a fingerprint
    # imported from YAML pins a SERVER; no server has been told an id by this
    # host), so there is nothing to refuse.
    enrolled = sorted(
        name for name, dest in snapshot.config.destinations.items() if dest.server_fingerprint
    ) if previous else []
    where = where_flag(resolved)
    if enrolled and not force:
        names = ", ".join(repr(n) for n in enrolled)
        raise CliError(
            f"refusing to change the client id while destination(s) {names} are enrolled: each "
            f"server has this host's certificate bound to the OLD id {previous!r} and will answer "
            f"403 (mismatch) until this host is enrolled again under the new id.\n"
            f"To do it properly, for each server: first revoke this host's old certificate "
            f"THERE (`zfsbackup-config trust revoke --client {previous}`, run on the server); "
            f"the certificate stays bound to the old id, so re-enrolling it cannot work and a "
            f"revoked certificate can never be enrolled again. Then have that server's "
            f"operator create a token (`trust token create`) and enroll a NEW client "
            f"certificate with it. Do the key change ONCE: `zfsbackup-config enroll NAME "
            f"--new-key --token-file FILE` for the first server, then a plain "
            f"`zfsbackup-config enroll NAME --token-file FILE` (no --new-key: that would "
            f"generate yet another key) for each of the others, each with a token from that "
            f"server's operator. A server that already knows {new_id!r} needs its token "
            f"created with `--client-id {new_id} --replace` (an unscoped token is refused "
            f"with 409 client_id_taken, and without --replace the old certificate stays "
            f"pinned there).\n"
            f"Re-run with --force to change the id anyway."
        )
    if enrolled:
        state.warn(
            f"--force: destination(s) {', '.join(repr(n) for n in enrolled)} stay enrolled "
            f"under the old client id {previous!r} and will refuse this host (403 mismatch) "
            "until it is enrolled again"
        )
    old = f"from {previous!r} " if previous else ""
    state.warn(
        f"changing the client id {old}to {new_id!r}: every server will see this host as a NEW "
        "client, so the next remote backup of each dataset is a FULL send to "
        f"<target>/{new_id}; data under the old id on the servers is left in place"
    )

    with _operator_errors():
        update = set_client_id(
            resolved,
            new_id,
            expected_generation=snapshot.generation,
            expected_store_id=snapshot.store_id,
            lock=None,
        )
    release_database()
    if not update.changed:
        click.echo(f"client id is already {new_id}; nothing changed")
        return
    # Committed (a config generation bump): request the reload BEFORE printing
    # anything, so a closed stdout cannot cost the signal.
    request = begin_reload(resolved, update.generation, snapshot.store_id)
    click.echo(f"client id: {update.previous if update.previous else '(not set)'} -> {update.client_id}")
    notify_daemon(resolved, update.generation, snapshot.store_id, request=request)


# --------------------------------------------------------------------------
# Click wiring
# --------------------------------------------------------------------------


def register(
    cli: click.Group,
    common: Callable[[Callable], Callable],
    get_state: Callable[[Optional[str], bool], Any],
) -> None:
    """Add the `trust` and `identity` groups to `cli`. `common` is `main.py`'s
    `-c`/`-v` decorator and `get_state(config_path, verbose)` its `_state`
    (both live there; this module does not import `main`)."""
    json_option = click.option(
        "--json", "as_json", is_flag=True, default=False,
        help="Machine-readable JSON (stable shape; see zfsbackup/cli/trust.py).",
    )

    # ---- trust ----

    @cli.group(
        "trust",
        help=(
            "Server side of enrollment: the enrollment token and the pinned client "
            "certificates. None of these commands signals the daemon (trust changes "
            "are picked up by the API within ~2 s)."
        ),
    )
    @common
    def trust_group(config_path: Optional[str], verbose: bool) -> None:
        get_state(config_path, verbose)

    @trust_group.group("token", help="The one-time enrollment token.")
    @common
    def token_group(config_path: Optional[str], verbose: bool) -> None:
        get_state(config_path, verbose)

    @token_group.command(
        "create",
        help=(
            "Create the enrollment token (replacing any earlier one) and print it on "
            "STDOUT. The token is single use, valid for --ttl (default 1h, at most 7d), "
            "and contains this server's certificate fingerprint, which the client pins. "
            "With --client-id ID (an id this server already knows) it can only enroll "
            "that id: it adds a certificate, or with --replace replaces all of the id's "
            "certificates. Without it, it enrolls one NEW client id. Creates the server "
            "TLS identity if there is none and you may; otherwise start the daemon once."
        ),
    )
    @click.option("--ttl", default="1h", show_default=True, metavar="DURATION",
                  help="How long the token is valid (e.g. 30m, 1h, 2d); at most 7d.")
    @click.option("--client-id", "client_id", default=None, metavar="ID",
                  help="Scope the token to an already known client id.")
    @click.option("--replace", is_flag=True, default=False,
                  help="With --client-id: enrolling replaces all the id's current certificates.")
    @json_option
    @common
    def token_create_cmd(
        ttl: str, client_id: Optional[str], replace: bool, as_json: bool,
        config_path: Optional[str], verbose: bool,
    ) -> None:
        token_create(get_state(config_path, verbose), ttl, client_id, replace, as_json)

    @token_group.command(
        "show",
        help=(
            "Show the enrollment token's state (none, active, expired or consumed), "
            "expiry, server fingerprint, scope and, once consumed, who redeemed it. "
            "Never the secret."
        ),
    )
    @json_option
    @common
    def token_show_cmd(as_json: bool, config_path: Optional[str], verbose: bool) -> None:
        token_show(get_state(config_path, verbose), as_json)

    @token_group.command("revoke", help="Delete the enrollment token (in any state).")
    @common
    def token_revoke_cmd(config_path: Optional[str], verbose: bool) -> None:
        token_revoke(get_state(config_path, verbose))

    @trust_group.command(
        "list",
        help=(
            "The known client ids, grouped, with label, status (active or revoked) and "
            "each pinned certificate: how it was enrolled (token or rotate), when, its "
            "expiry and the certificate it succeeded."
        ),
    )
    @json_option
    @common
    def trust_list_cmd(as_json: bool, config_path: Optional[str], verbose: bool) -> None:
        trust_list(get_state(config_path, verbose), as_json)

    @trust_group.command(
        "revoke",
        help=(
            "Revoke trust. FINGERPRINT (a unique prefix of at least 8 hex characters is "
            "enough; the full one is echoed) unpins one certificate; --client ID unpins "
            "all of that client's certificates and retires the id. New connections are "
            "refused and streams already in flight are cut within ~2 s; the client's data on "
            "this server is left in place."
        ),
    )
    @click.argument("fingerprint", required=False, metavar="[FINGERPRINT]")
    @click.option("--client", "client_id", default=None, metavar="ID",
                  help="Revoke every certificate of this client id.")
    @common
    def trust_revoke_cmd(
        fingerprint: Optional[str], client_id: Optional[str],
        config_path: Optional[str], verbose: bool,
    ) -> None:
        trust_revoke(get_state(config_path, verbose), fingerprint, client_id)

    # ---- identity ----

    @cli.group(
        "identity",
        help=(
            "This host's identity as a backup client: its client id and TLS "
            "certificate."
        ),
    )
    @common
    def identity_group(config_path: Optional[str], verbose: bool) -> None:
        get_state(config_path, verbose)

    @identity_group.command(
        "show",
        help=(
            "Show the client id, and the client and server certificates (fingerprint, "
            "expiry, file). Read-only: nothing is generated."
        ),
    )
    @json_option
    @common
    def identity_show_cmd(as_json: bool, config_path: Optional[str], verbose: bool) -> None:
        identity_show(get_state(config_path, verbose), as_json)

    @identity_group.command(
        "set-client-id",
        help=(
            "Change this host's client id (letters, digits, '.', '_' and '-'; at most "
            "64; starting with a letter or digit). Every server then sees a NEW client and the "
            "next remote backup is a full send. Refused while any destination is "
            "enrolled (its server has this host's certificate bound to the old id) "
            "unless --force."
        ),
    )
    @click.argument("client_id", metavar="ID")
    @click.option("--force", is_flag=True, default=False,
                  help="Change the id although destinations are enrolled.")
    @common
    def identity_set_client_id_cmd(
        client_id: str, force: bool, config_path: Optional[str], verbose: bool,
    ) -> None:
        identity_set_client_id(get_state(config_path, verbose), client_id, force)

    # `identity rotate` (plan M14c) lives in `enroll.py`, which adds it to this
    # group (`enroll.register`, called after this function in `main.py`).

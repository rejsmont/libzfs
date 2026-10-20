"""Trust and host-identity access for the config store (TLS/mTLS plan M7).

The narrow API for the six tables that are NOT configuration
(`mapper.PRESERVED_TABLES`): `host_identity` (this host's client id, client
side) and `clients`, `trusted_clients`, `revoked_fingerprints`,
`enrollment_token`, `trust_state` (the pinned client certificates, the
permanently refused ones, and the one enrollment token, server side). Nothing
outside this module reads or writes those tables, except that
`mapper.load_config_snapshot` reads `host_identity.client_id` inside the
config snapshot (plan D2).

Who writes what (the invariant this module exists to keep checkable)
--------------------------------------------------------------------
- **Configuration and `host_identity`: the CLI only.** `ensure_client_id` /
  `set_client_id` require the CLI writer lock (`lock=`) and bump
  `GlobalSettings.generation` in the same transaction as the id change, so the
  daemon's existing poll/SIGHUP path reloads and hands the new id to the
  workers (`ConfigSnapshot.client_id`). Any change to the stored id bumps the
  config generation; nothing else here touches the config generation.
- **The trust tables have exactly two writers, both through this module:**
  the CLI (`create_token`, `revoke_token`, `revoke_pin`, `revoke_client`; under
  the writer lock) and the API worker (`enroll_client`, `rotate_client`,
  `retire_predecessor`). The API writers take NO flock: against the CLI they
  serialise only on SQLite's write lock (`BEGIN IMMEDIATE`), and against each
  other on `_API_WRITE_LOCK`, an in-process lock taken before the write
  connection is opened (so request threads queue on a lock that wakes them at
  once rather than in SQLite's sleeping busy handler). A trust write never
  bumps the config generation and never causes a reload. It bumps
  `trust_state.generation` -- only when the set of pins changes -- which the
  API worker polls (`read_trust_key`).
- **API writers filter read-only first.** `enroll_client` is reachable
  unauthenticated, so every one of the API writers first decides on a
  read-only snapshot whether it could possibly write, and returns its refusal
  (or no-op) from there without ever taking the write lock. Measured before
  this: 32 threads of wrong-secret `/enroll` held the write lock often enough
  to delay a CLI `create_token` by up to 1.8 s. The pre-check is only a
  filter; everything is re-checked under the write lock.
- **Revocation is permanent.** Every write that unpins a certificate --
  `revoke_pin` (`revoked`, plus `successor_revoked` for the rotation successors
  it takes down with it -- also when the key was already `retired`, through
  the record's `superseded_by`), `revoke_client` (`client_revoked`), a `--replace`
  enrollment (`replaced`) and `retire_predecessor` (`retired`; `rotate_client`
  retires the rotating pin's own still-pinned predecessor the same way, with
  `superseded_by`) -- records its
  fingerprint in `revoked_fingerprints` in the same transaction, and no code
  ever deletes such a row. `enroll_client` and `rotate_client` answer
  `REFUSED` for a recorded fingerprint without touching the token, decided in
  the read-only pre-check (the write transaction re-checks), and `read_trust`
  filters recorded fingerprints out of the pin set as the backstop. Before
  this, revoking deleted the pin and forgot it: whoever held a stolen key
  could redeem any fresh token with it and be trusted again. A retired
  rotation predecessor is refused too: it is the superseded key most likely
  to survive in an old copy of the client's TLS directory, and no legitimate
  client ever presents it again (a client that lost its successor enrolls
  with `--new-key`).
- **Never nest a trust writer inside another writer** (`db.session_for_
  engine`: at most one writer connection per file per process). A request
  thread must not call an API writer while it holds any writer session or
  connection of its own, and none of these functions calls another.

Engines, threads, forks
-----------------------
Every function opens ONE short-lived connection on a fresh, unpooled engine
(`paths.open_config_session(..., ephemeral=True)` -> `db.ephemeral_engine`),
does its work in ONE transaction, and closes and disposes everything before it
returns or raises. Nothing is cached and nothing is shared between calls or
threads, so the API worker may call any function here from any request thread
concurrently, and the supervisor's `dispose_all()` neither sees nor affects
these engines. No connection, WAL read mark or `-shm` mapping survives a call.

- Reads: read-only engine (`query_only`), explicit `BEGIN`, `check_schema`
  inside that snapshot, the `SELECT`s, `COMMIT`, close. One consistent
  snapshot per call (as `access.read_config`).
- Writes: writer engine, so the first statement is `BEGIN IMMEDIATE` (SQLite's
  write lock, held to commit); `check_schema` runs inside the write
  transaction, so a store migrated or replaced under the caller is refused
  (`SchemaError`) rather than written through the wrong models. Every decision
  is read and acted on under that one lock: concurrent enrollers serialise and
  exactly one consumes the token; two `ensure_client_id` calls converge on one
  id; racing `retire_predecessor` calls retire once.

Errors
------
`ConfigBusy`: the writer lock or SQLite's write lock was not obtained in time
("database is locked" after `busy_timeout`, 5 s); nothing was written, retry.
`ConfigConflict` / `StoreIdConflict`: `set_client_id`'s compare-and-swap lost.
`ConfigPathError` (preflight) and `SchemaError` (wrong schema, including a store
still at the pre-M5 revision) pass through unchanged. `ValueError`: a malformed
argument, or (`ensure_client_id` / `set_client_id`) an uninitialised store with
no `GlobalSettings` row. `LookupError`: an unknown fingerprint or client id for
a revoke, or an unknown client for a scoped token.

Only frozen dataclasses (and `str` enums / a `NamedTuple`) leave this module;
never an ORM object, a session or a connection. All statements are Core
statements on the mapped tables, so no ORM identity map is involved at all.

Time
----
Every timestamp is POSIX seconds (float), passed in by the caller where the
decision depends on it (`now`), so the API can use its own clock and tests a
fake one. The API writers (`enroll_client`, `rotate_client`) take `now` as
EITHER a number OR a zero-argument callable returning one (`time.time`): a
callable is read again after the write lock is obtained and that reading is
the one every decision and timestamp uses, so up to two busy timeouts of lock
waiting can never stretch a token's validity. A number is used as is (tests,
or a caller that has already taken its reading). The API should pass
`now=time.time`.

A token is redeemable only while `created_at <= now < expires_at`: a clock
earlier than the token's creation is never treated as valid, and a redemption
attempt that presents the token's correct server fingerprint and secret but
observes it expired retires it durably (`consumed_at` set,
`consumed_fingerprint` NULL), so a clock that later steps back into the
token's window does not revive it. Only a caller holding the secret can
trigger that latch: an unauthenticated request during a forward clock step
must not be able to burn a valid token.
"""

from __future__ import annotations

import enum
import hmac
import math
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, NamedTuple, Optional, Tuple, Union

from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from zfsbackup.config import (
    client_id_problem,
    generate_client_id,
    normalize_fingerprint,
)
from zfsbackup.config.store import models
from zfsbackup.config.store.db import _BUSY_TIMEOUT_MS, _is_busy_error
from zfsbackup.config.store.errors import ConfigBusy, ConfigConflict
from zfsbackup.config.store.mapper import StoreIdConflict
from zfsbackup.config.store.migrate import check_schema
from zfsbackup.config.store.paths import (
    DEFAULT_WRITER_LOCK_TIMEOUT,
    ResolvedConfigPath,
    check_config_db,
    open_config_session,
    require_writer_lock_held,
    writer_lock,
)

__all__ = [
    "ClientIdUpdate",
    "ClientRecord",
    "EnrollOutcome",
    "NewPin",
    "PinRevocation",
    "RevokedPin",
    "RotateOutcome",
    "TOKEN_MAX_TTL_SECONDS",
    "TokenState",
    "TokenStatus",
    "TrustKey",
    "TrustSnapshot",
    "TrustedPin",
    "create_token",
    "enroll_client",
    "ensure_client_id",
    "list_clients",
    "list_revoked",
    "read_client_id",
    "read_trust",
    "read_trust_key",
    "retire_predecessor",
    "revoke_client",
    "revoke_pin",
    "revoke_token",
    "rotate_client",
    "set_client_id",
    "token_precheck",
    "token_status",
]

#: Plan D7: an enrollment token lives at most 7 days. The CLI chooses the TTL
#: (default 1 h); this is the store's backstop.
TOKEN_MAX_TTL_SECONDS = 7 * 24 * 3600

_HEX64_RE = re.compile(r"[0-9a-f]{64}")
_LOCKED_MARKERS = ("database is locked", "database table is locked")

#: `now=` for the API writers: POSIX seconds, or a zero-argument callable
#: returning them (read after the write lock is obtained; see "Time" above).
Clock = Union[float, int, Callable[[], float]]

# Serialises the API worker's trust writers (`enroll_client`, `rotate_client`,
# `retire_predecessor`) within one process. Not needed for correctness --
# `BEGIN IMMEDIATE` already serialises them -- but SQLite's busy handler
# waits by SLEEPING in growing steps (up to 100 ms), so request threads
# contending on the write lock wake late and in no order; a `threading.Lock`
# hands over at once. Taken only after the read-only pre-check passed, never
# around a read, and never by a CLI writer (those hold the flock instead).
# Bounded by the same 5 s as `busy_timeout`; a timeout is `ConfigBusy`.
_API_WRITE_LOCK = threading.Lock()
_API_WRITE_LOCK_TIMEOUT_S = _BUSY_TIMEOUT_MS / 1000.0


def _reinit_api_lock_after_fork() -> None:
    """A fork while a request thread holds `_API_WRITE_LOCK` would leave the
    child with a lock nobody can release (the holder thread does not exist
    there), and every later API write in the child would time out as
    `ConfigBusy`. Replace it in the child."""
    global _API_WRITE_LOCK
    _API_WRITE_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):  # POSIX only
    os.register_at_fork(after_in_child=_reinit_api_lock_after_fork)

# Core table objects: every statement below is Core, so no ORM instance is ever
# created, cached in an identity map, or able to escape.
_GS = models.GlobalSettings.__table__
_HI = models.HostIdentity.__table__
_CL = models.Client.__table__
_TC = models.TrustedClient.__table__
_RF = models.RevokedFingerprint.__table__
_TK = models.EnrollmentToken.__table__
_TS = models.TrustState.__table__


# --------------------------------------------------------------------------
# Value types
# --------------------------------------------------------------------------


class TokenState(enum.StrEnum):
    NONE = "none"
    ACTIVE = "active"
    EXPIRED = "expired"
    CONSUMED = "consumed"


class EnrollOutcome(enum.StrEnum):
    ENROLLED = "enrolled"
    ALREADY_TRUSTED = "already_trusted"
    REFUSED = "refused"
    CLIENT_ID_TAKEN = "client_id_taken"
    CERT_BOUND_ELSEWHERE = "cert_bound_elsewhere"


class RotateOutcome(enum.StrEnum):
    ROTATED = "rotated"
    ALREADY = "already"
    REFUSED = "refused"


@dataclass(frozen=True)
class NewPin:
    """A certificate to pin (input to `enroll_client` / `rotate_client`).

    The caller (the API) has already verified the certificate
    (`identity.check_client_cert`) and computed `fingerprint` from `cert_pem`;
    the store does not parse certificates. `fingerprint` is normalised to 64
    lowercase hex on construction (`ValueError` if it is not a SHA-256
    fingerprint); `not_after` is the certificate's expiry, POSIX seconds.
    """

    fingerprint: str
    cert_pem: str
    not_after: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "fingerprint", normalize_fingerprint(self.fingerprint))
        if not isinstance(self.cert_pem, str) or not self.cert_pem.strip():
            raise ValueError("cert_pem must be a non-empty PEM string")
        object.__setattr__(self, "not_after", _finite(self.not_after, "not_after"))


@dataclass(frozen=True)
class TrustedPin:
    """One pinned client certificate. `predecessor` is the fingerprint this
    pin succeeds by rotation (`enrolled_via == 'rotate'`) while that
    predecessor is still pinned, else `None`."""

    fingerprint: str
    client_id: str
    cert_pem: str
    not_after: float
    enrolled_at: float
    enrolled_via: str
    predecessor: Optional[str]


@dataclass(frozen=True)
class PinRevocation(TrustedPin):
    """What `revoke_pin` returns: the pin that was revoked (these fields) plus
    `cascaded`, every pin removed with it because it was a rotation successor
    of a revoked pin, directly or transitively (ordered by enrollment time,
    fingerprint). A subclass of `TrustedPin`, so a caller that only wants the
    revoked pin is unaffected. Note `==` against a plain `TrustedPin` is False
    (dataclass equality compares the class); compare `.fingerprint`.

    `already_retired` is always False here: `revoke_pin` of a fingerprint that
    was already retired returns a `SupersededRevocation` instead (same
    `.fingerprint` / `.client_id` / `.cascaded`). Every `cascaded` pin is as
    it stood before this call unpinned anything, so `predecessor` still
    names the pin it succeeded."""

    cascaded: Tuple[TrustedPin, ...] = ()

    @property
    def already_retired(self) -> bool:
        return False


@dataclass(frozen=True)
class RevokedPin:
    """A fingerprint that was unpinned and may never be pinned again
    (`list_revoked`, `ClientRecord.revoked_pins`). `client_id` is the client it
    was pinned to; `reason` is one of `models.REVOCATION_REASONS`: `revoked`
    (`revoke_pin`), `successor_revoked` (`revoke_pin` of its rotation
    predecessor, which removes the successor too), `client_revoked`
    (`revoke_client`), `replaced` (a `--replace` enrollment dropped it) or
    `retired` (a rotation predecessor retired by its successor's first
    request). `superseded_by` is set only for `retired`: the successor whose
    first request retired it (`None` for every other reason, and for a
    record written before the column existed)."""

    fingerprint: str
    client_id: str
    revoked_at: float
    reason: str
    superseded_by: Optional[str] = None


@dataclass(frozen=True)
class SupersededRevocation(RevokedPin):
    """What `revoke_pin` returns for a fingerprint that was ALREADY retired
    (`reason == 'retired'`): its existing record (these fields, unchanged --
    the record keeps its first reason and time) plus `cascaded`, the pins this
    call unpinned by following `superseded_by` to the live successor and that
    successor's own pending successors (all recorded `successor_revoked`,
    ordered by enrollment time, fingerprint; never empty -- with nothing live
    to reach, `revoke_pin` raises `LookupError` instead).

    Shares `.fingerprint`, `.client_id`, `.cascaded` and `.already_retired`
    with `PinRevocation`, so a caller that only reports those handles both;
    it is NOT a `TrustedPin` (the retired pin's certificate is gone)."""

    cascaded: Tuple[TrustedPin, ...] = ()

    @property
    def already_retired(self) -> bool:
        return True


@dataclass(frozen=True)
class ClientRecord:
    """A client id known to this server, with its pins (`list_clients`).
    `revoked_at` is the tombstone: a revoked id holds no pins and is still
    "known" (an unscoped token cannot claim it). `revoked_pins` are the
    fingerprints this client once held that are refused for ever (ordered by
    `revoked_at`, fingerprint); a client that is not revoked may have some
    too (a single revoked pin, a `--replace`, a retired rotation
    predecessor)."""

    client_id: str
    label: Optional[str]
    created_at: float
    revoked_at: Optional[float]
    pins: Tuple[TrustedPin, ...] = ()
    revoked_pins: Tuple[RevokedPin, ...] = ()

    @property
    def revoked(self) -> bool:
        return self.revoked_at is not None


class TrustKey(NamedTuple):
    """`(store_id, generation)`: what the API worker polls. `store_id` is the
    file's `PRAGMA user_version`, so a database file replaced at the same trust
    generation still changes the key."""

    store_id: int
    generation: int


@dataclass(frozen=True)
class TrustSnapshot:
    """Every live pin (never one of a revoked client, never a revoked
    fingerprint) and the key it was read at, from one read snapshot."""

    store_id: int
    generation: int
    pins: Tuple[TrustedPin, ...]

    @property
    def key(self) -> TrustKey:
        return TrustKey(self.store_id, self.generation)


@dataclass(frozen=True)
class TokenStatus:
    """The enrollment token as the operator may see it. Never carries the
    secret hash.

    `state`: `none` (no token row), `active` (redeemable at the `now` it was
    evaluated at), `expired` (past `expires_at`, before `created_at` -- a
    clock that went backwards -- or retired unredeemed after an expired
    redemption attempt) or `consumed` (redeemed; `consumed_*` say by whom).
    """

    state: TokenState
    created_at: Optional[float] = None
    expires_at: Optional[float] = None
    server_fingerprint: Optional[str] = None
    client_id: Optional[str] = None
    replace: bool = False
    consumed_at: Optional[float] = None
    consumed_fingerprint: Optional[str] = None
    consumed_client_id: Optional[str] = None


@dataclass(frozen=True)
class ClientIdUpdate:
    """Result of `set_client_id`. `changed` is False (and `generation` the
    unchanged one) when the stored id already equalled the new one: nothing
    was committed, so no reload is needed."""

    client_id: str
    previous: Optional[str]
    generation: int
    changed: bool


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------


def _finite(value: object, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{what} must be a number of POSIX seconds, got {value!r}")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{what} must be finite, got {value!r}")
    return result


def _clock(now: object, what: str = "now") -> Callable[[], float]:
    """`now=` (a number or a zero-argument callable, see `Clock`) as a
    callable that returns validated POSIX seconds. A number is validated
    here, before anything is opened; a callable is validated on each read."""
    if callable(now):
        def read() -> float:
            return _finite(now(), what)
        return read
    value = _finite(now, what)
    return lambda: value


@contextmanager
def _api_writer(resolved: ResolvedConfigPath) -> Iterator[None]:
    """Hold `_API_WRITE_LOCK` for one API trust write (see there). A wait
    past the busy timeout is `ConfigBusy`, worded like SQLite's."""
    lock = _API_WRITE_LOCK
    if not lock.acquire(timeout=_API_WRITE_LOCK_TIMEOUT_S):
        raise ConfigBusy(
            f"the config database {resolved.path} is busy with other trust "
            "writes in this process; nothing was written. Retry in a moment."
        )
    try:
        yield
    finally:
        lock.release()


def _client_id(value: object, what: str = "client id") -> str:
    problem = client_id_problem(value)
    if problem is not None:
        raise ValueError(f"{what}: {problem}")
    return value  # type: ignore[return-value]


def _label(value: object) -> Optional[str]:
    if value is not None and not isinstance(value, str):
        raise ValueError(f"label must be a string or None, got {value!r}")
    return value


def _is_locked(exc: BaseException) -> bool:
    orig = getattr(exc, "orig", None) or exc
    if isinstance(orig, sqlite3.OperationalError) and _is_busy_error(orig):
        return True
    reason = str(orig).lower()
    return any(marker in reason for marker in _LOCKED_MARKERS)


@contextmanager
def _busy_is_config_busy(resolved: ResolvedConfigPath, *, writing: bool) -> Iterator[None]:
    """SQLite busy/locked (after `busy_timeout`) -> `ConfigBusy`; every other
    error passes through unchanged."""
    try:
        yield
    except (OperationalError, sqlite3.OperationalError) as exc:
        if _is_locked(exc):
            raise ConfigBusy(
                f"the config database {resolved.path} is locked by another "
                "process (it stayed locked past the busy timeout); "
                + ("nothing was written. " if writing else "")
                + "Retry in a moment."
            ) from exc
        raise


@contextmanager
def _read_tx(resolved: ResolvedConfigPath) -> Iterator[Session]:
    """One read snapshot on a fresh read-only engine: `BEGIN`, `check_schema`,
    the caller's `SELECT`s, `COMMIT`, close, dispose. The explicit `BEGIN`
    makes the reads one snapshot (a reader engine begins nothing itself), and
    the close releases the WAL read mark before this returns."""
    with _busy_is_config_busy(resolved, writing=False):
        with open_config_session(resolved, readonly=True, ephemeral=True) as session:
            session.execute(text("BEGIN"))
            try:
                check_schema(session.connection())
                yield session
            except BaseException:
                try:
                    session.execute(text("ROLLBACK"))
                except Exception:  # pragma: no cover - the close is the real
                    pass           # guarantee; never mask the original error
                raise
            session.execute(text("COMMIT"))


@contextmanager
def _write_tx(resolved: ResolvedConfigPath) -> Iterator[Session]:
    """One write transaction on a fresh writer engine. Its first statement is
    `BEGIN IMMEDIATE` (`db._install_begin_immediate`), so `check_schema` and
    every read the caller makes are under SQLite's write lock until the commit
    `session_for_engine` issues on a clean exit (rollback on any exception)."""
    with _busy_is_config_busy(resolved, writing=True):
        with open_config_session(resolved, readonly=False, ephemeral=True) as session:
            check_schema(session.connection())
            yield session


@contextmanager
def _cli_writer(
    resolved: ResolvedConfigPath, lock: Optional[Path], lock_timeout: float
) -> Iterator[None]:
    """The CLI writer-lock contract of `access.write_config(lock=)`: a given
    `lock` must be the one this thread holds for this database (`ValueError`
    otherwise, before anything is opened); `None` takes `writer_lock` for the
    duration of the call (`ConfigBusy` on timeout). The preflight runs first so
    no lock file is ever created beside a missing or foreign database."""
    if lock is not None:
        require_writer_lock_held(resolved, lock)
        yield
        return
    check_config_db(resolved)
    with writer_lock(resolved, timeout=lock_timeout):
        yield


def _pin_from_row(row) -> TrustedPin:  # noqa: ANN001
    return TrustedPin(
        fingerprint=row.fingerprint,
        client_id=row.client_id,
        cert_pem=row.cert_pem,
        not_after=float(row.not_after),
        enrolled_at=float(row.enrolled_at),
        enrolled_via=row.enrolled_via,
        predecessor=row.predecessor_fingerprint,
    )


def _token_redeemed(row) -> bool:  # noqa: ANN001
    return row.consumed_fingerprint is not None or row.consumed_client_id is not None


def _token_status_from_row(row, now: float) -> TokenStatus:  # noqa: ANN001
    if row is None:
        return TokenStatus(TokenState.NONE)
    if _token_redeemed(row):
        state = TokenState.CONSUMED
    elif row.consumed_at is not None:
        # Retired unredeemed (an expired redemption attempt latched it).
        state = TokenState.EXPIRED
    elif not (row.created_at <= now < row.expires_at):
        state = TokenState.EXPIRED
    else:
        state = TokenState.ACTIVE
    return TokenStatus(
        state=state,
        created_at=float(row.created_at),
        expires_at=float(row.expires_at),
        server_fingerprint=row.server_fingerprint,
        client_id=row.client_id,
        replace=bool(row.replace),
        consumed_at=None if row.consumed_at is None else float(row.consumed_at),
        consumed_fingerprint=row.consumed_fingerprint,
        consumed_client_id=row.consumed_client_id,
    )


def _select_token(session: Session):  # noqa: ANN202
    return session.execute(select(_TK).where(_TK.c.id == 1)).one_or_none()


def _select_pin(session: Session, fingerprint: str):  # noqa: ANN202
    return session.execute(
        select(_TC).where(_TC.c.fingerprint == fingerprint)
    ).one_or_none()


def _select_client(session: Session, client_id: str):  # noqa: ANN202
    return session.execute(
        select(_CL).where(_CL.c.client_id == client_id)
    ).one_or_none()


def _insert_pin(
    session: Session, pin: NewPin, client_id: str, *, now: float, via: str,
    predecessor: Optional[str] = None,
) -> None:
    session.execute(insert(_TC).values(
        fingerprint=pin.fingerprint,
        client_id=client_id,
        cert_pem=pin.cert_pem,
        not_after=pin.not_after,
        enrolled_at=now,
        enrolled_via=via,
        predecessor_fingerprint=predecessor,
    ))


def _revoked_from_row(row) -> RevokedPin:  # noqa: ANN001
    return RevokedPin(
        fingerprint=row.fingerprint,
        client_id=row.client_id,
        revoked_at=float(row.revoked_at),
        reason=row.reason,
        superseded_by=row.superseded_by,
    )


def _select_revoked(session: Session, fingerprint: str):  # noqa: ANN202
    return session.execute(
        select(_RF).where(_RF.c.fingerprint == fingerprint)
    ).one_or_none()


def _is_revoked(session: Session, fingerprint: str) -> bool:
    return _select_revoked(session, fingerprint) is not None


class _Unpin(NamedTuple):
    """One group of pins for `_unpin_all`: those matching `condition`, all
    recorded with `reason` (and `superseded_by`, set only for `retired`)."""

    condition: object
    reason: str
    superseded_by: Optional[str] = None


def _unpin(
    session: Session, condition, *, now: float, reason: str,  # noqa: ANN001
    superseded_by: Optional[str] = None,
) -> Tuple[TrustedPin, ...]:
    """`_unpin_all` with a single group: the removed pins."""
    (removed,) = _unpin_all(
        session, (_Unpin(condition, reason, superseded_by),), now=now,
    )
    return removed


def _unpin_all(
    session: Session, groups: Tuple[_Unpin, ...], *, now: float,
) -> Tuple[Tuple[TrustedPin, ...], ...]:
    """Delete every pin matching any group's condition and record each
    fingerprint in `revoked_fingerprints` (its group's `reason` /
    `superseded_by`, `revoked_at=now`), in the caller's write transaction;
    then clear predecessors left dangling. Returns the removed pins per group
    (each ordered by enrollment time, fingerprint; a pin matched by an
    earlier group is not repeated in a later one). The ONLY way any function
    here deletes a pin, so no unpinning path can forget the record that keeps
    the fingerprint from being re-enrolled. Does not bump the generation.

    Every group is READ before anything is written. Unpinning group by group
    (as `revoke_pin` once did) returned the cascaded successors after the
    first deletion had already cleared their links, so
    `PinRevocation.cascaded[0].predecessor` came back `None` while the
    grandchild's link was still there -- the same result reporting the chain
    two ways.

    An already-recorded fingerprint keeps its first record (`ON CONFLICT DO
    NOTHING`): unreachable while pins and records stay disjoint, and the
    original reason is the more useful one to show."""
    taken: set = set()
    removed = []
    for group in groups:
        pins = tuple(
            p for p in (
                _pin_from_row(r) for r in session.execute(
                    select(_TC).where(group.condition)
                    .order_by(_TC.c.enrolled_at, _TC.c.fingerprint)
                ).all()
            ) if p.fingerprint not in taken
        )
        taken.update(p.fingerprint for p in pins)
        removed.append(pins)
    if not taken:
        return tuple(removed)
    session.execute(
        sqlite_insert(_RF)
        .values([
            {"fingerprint": p.fingerprint, "client_id": p.client_id,
             "revoked_at": now, "reason": group.reason,
             "superseded_by": group.superseded_by}
            for group, pins in zip(groups, removed) for p in pins
        ])
        .on_conflict_do_nothing(index_elements=["fingerprint"])
    )
    # Delete exactly the rows recorded above, not the conditions re-evaluated.
    session.execute(delete(_TC).where(_TC.c.fingerprint.in_(sorted(taken))))
    _clear_dangling_predecessors(session)
    return tuple(removed)


def _clear_dangling_predecessors(session: Session) -> None:
    """After any pin deletion: a successor whose predecessor is no longer
    pinned must not keep pointing at it. Otherwise a fingerprint later
    re-enrolled (possibly for ANOTHER client) would be retired by that stale
    reference on the successor's next request."""
    session.execute(
        update(_TC)
        .where(
            _TC.c.predecessor_fingerprint.is_not(None),
            _TC.c.predecessor_fingerprint.not_in(select(_TC.c.fingerprint)),
        )
        .values(predecessor_fingerprint=None)
    )


def _bump_trust_generation(session: Session) -> None:
    """`trust_state.generation += 1`, creating the row (generation 1) when it
    is missing. Only called when the set of pins changed. Inside the caller's
    `BEGIN IMMEDIATE` transaction, so the read-modify-write cannot race."""
    result = session.execute(
        update(_TS).where(_TS.c.id == 1).values(generation=_TS.c.generation + 1)
    )
    if result.rowcount == 0:
        session.execute(insert(_TS).values(id=1, generation=1))


def _config_generation(session: Session) -> int:
    generation = session.execute(
        select(_GS.c.generation).where(_GS.c.id == 1)
    ).scalar_one_or_none()
    if generation is None:
        raise ValueError(
            "config store is uninitialized: no GlobalSettings row (id=1) "
            "-- was it created by import?"
        )
    return int(generation)


def _bump_config_generation(session: Session, current: int) -> int:
    """`GlobalSettings.generation += 1`, in the caller's write transaction:
    the reload trigger for a changed client id (plan D2)."""
    session.execute(
        update(_GS).where(_GS.c.id == 1).values(generation=current + 1)
    )
    return current + 1


def _store_id(session: Session) -> int:
    value = session.connection().exec_driver_sql("PRAGMA user_version").scalar()
    return int(value or 0)


# --------------------------------------------------------------------------
# Host identity (client side; the CLI is the only writer)
# --------------------------------------------------------------------------


def read_client_id(resolved: ResolvedConfigPath) -> Optional[str]:
    """This host's client id, or `None` when none has been generated yet.

    Read-only, one snapshot, schema-checked. The daemon does not call this: it
    gets the id from `read_config`'s snapshot (`ConfigSnapshot.client_id`).
    """
    with _read_tx(resolved) as session:
        return session.execute(
            select(_HI.c.client_id).where(_HI.c.id == 1)
        ).scalar_one_or_none()


def ensure_client_id(
    resolved: ResolvedConfigPath,
    *,
    lock: Optional[Path],
    lock_timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT,
) -> Tuple[str, bool]:
    """Return `(client_id, created)`, generating and storing an id first if
    the store has none.

    Under the CLI writer lock (`lock=` as for `access.write_config`: the held
    lock, or `None` to take it for this call) and `BEGIN IMMEDIATE`. With no
    `host_identity` row it inserts `generate_client_id()` AND bumps
    `GlobalSettings.generation` in the same transaction (the daemon reloads and
    picks the id up); otherwise it returns the stored id and writes nothing.
    Two concurrent calls serialise on the write lock, so both return the one id
    the first stored.

    `created=True` means the config generation moved by exactly one: a caller
    holding a `ConfigSnapshot` from before must re-read before a
    compare-and-swap `write_config`, and must send its reload signal.

    Raises `ValueError` for an uninitialised store (no `GlobalSettings` row) or
    an invalid `lock=`, `ConfigBusy`, `ConfigPathError`, `SchemaError`.
    """
    with _cli_writer(resolved, lock, lock_timeout):
        with _write_tx(resolved) as session:
            generation = _config_generation(session)
            existing = session.execute(
                select(_HI.c.client_id).where(_HI.c.id == 1)
            ).scalar_one_or_none()
            if existing is not None:
                return existing, False
            new_id = generate_client_id()
            session.execute(insert(_HI).values(
                id=1, client_id=new_id, created_at=time.time(), updated_at=None,
            ))
            _bump_config_generation(session, generation)
            return new_id, True


def set_client_id(
    resolved: ResolvedConfigPath,
    new_id: str,
    *,
    expected_generation: Optional[int],
    lock: Optional[Path],
    expected_store_id: Optional[int] = None,
    lock_timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT,
) -> ClientIdUpdate:
    """Store `new_id` as this host's client id, if the store is still at
    `expected_generation` (compare-and-swap; `None` = unconditional).

    Checks the A7 grammar first (`ValueError`, nothing opened). Then, under the
    writer lock and `BEGIN IMMEDIATE`: schema check, `expected_store_id` (as
    `write_config`: compared when non-zero; `StoreIdConflict` for a replaced
    file), the generation compare (`ConfigConflict`), then the upsert and a
    generation bump in the same transaction. Setting the id it already has is a
    no-op (`changed=False`, no bump, nothing committed). Policy (refusing while
    destinations are enrolled, warnings) is the CLI's.

    Raises `ValueError` (bad grammar, invalid `lock=`, uninitialised store),
    `ConfigConflict` / `StoreIdConflict`, `ConfigBusy`, `ConfigPathError`,
    `SchemaError`.
    """
    new_id = _client_id(new_id, "new client id")
    with _cli_writer(resolved, lock, lock_timeout):
        with _write_tx(resolved) as session:
            generation = _config_generation(session)
            stored_store_id = _store_id(session)
            if (
                expected_store_id is not None
                and expected_store_id != 0
                and expected_store_id != stored_store_id
            ):
                raise StoreIdConflict(
                    expected_store_id, stored_store_id, expected_generation, generation
                )
            if expected_generation is not None and generation != expected_generation:
                raise ConfigConflict(expected_generation, generation)
            row = session.execute(select(_HI).where(_HI.c.id == 1)).one_or_none()
            previous = None if row is None else row.client_id
            if previous == new_id:
                return ClientIdUpdate(new_id, previous, generation, changed=False)
            now = time.time()
            if row is None:
                session.execute(insert(_HI).values(
                    id=1, client_id=new_id, created_at=now, updated_at=None,
                ))
            else:
                session.execute(
                    update(_HI).where(_HI.c.id == 1).values(
                        client_id=new_id, updated_at=now,
                    )
                )
            new_generation = _bump_config_generation(session, generation)
            return ClientIdUpdate(new_id, previous, new_generation, changed=True)


# --------------------------------------------------------------------------
# Trust reads (server side; read-only, one snapshot each)
# --------------------------------------------------------------------------


def _trust_generation(session: Session) -> int:
    generation = session.execute(
        select(_TS.c.generation).where(_TS.c.id == 1)
    ).scalar_one_or_none()
    return 0 if generation is None else int(generation)


def read_trust_key(resolved: ResolvedConfigPath) -> TrustKey:
    """`(store_id, trust_generation)` from one read snapshot: the API worker's
    cheap poll. A missing `trust_state` row is generation 0; `store_id` is
    `PRAGMA user_version` (0 = never stamped). Holds nothing after it returns.
    """
    with _read_tx(resolved) as session:
        return TrustKey(_store_id(session), _trust_generation(session))


def read_trust(resolved: ResolvedConfigPath) -> TrustSnapshot:
    """Every live pin with its key, from one read snapshot. Pins of a revoked
    client and revoked fingerprints (`revoked_fingerprints`) are never
    returned -- revoking deletes the pin; these filters are the backstop, so
    the TLS pin set the API builds from this can never trust a revoked key.
    Ordered by client id, enrollment time, fingerprint."""
    with _read_tx(resolved) as session:
        rows = session.execute(
            select(_TC)
            .join(_CL, _CL.c.client_id == _TC.c.client_id)
            .where(
                _CL.c.revoked_at.is_(None),
                _TC.c.fingerprint.not_in(select(_RF.c.fingerprint)),
            )
            .order_by(_TC.c.client_id, _TC.c.enrolled_at, _TC.c.fingerprint)
        ).all()
        return TrustSnapshot(
            store_id=_store_id(session),
            generation=_trust_generation(session),
            pins=tuple(_pin_from_row(r) for r in rows),
        )


def list_clients(resolved: ResolvedConfigPath) -> Tuple[ClientRecord, ...]:
    """Every known client id (revoked ones included, for display) with its
    pins and its revoked fingerprints (`ClientRecord.revoked_pins`), ordered
    by client id; pins by enrollment time, fingerprint; revoked fingerprints
    by `revoked_at`, fingerprint."""
    with _read_tx(resolved) as session:
        pin_rows = session.execute(
            select(_TC).order_by(_TC.c.client_id, _TC.c.enrolled_at, _TC.c.fingerprint)
        ).all()
        pins_by_client: dict = {}
        for row in pin_rows:
            pins_by_client.setdefault(row.client_id, []).append(_pin_from_row(row))
        revoked_by_client: dict = {}
        for rev in _select_all_revoked(session):
            revoked_by_client.setdefault(rev.client_id, []).append(rev)
        client_rows = session.execute(select(_CL).order_by(_CL.c.client_id)).all()
        return tuple(
            ClientRecord(
                client_id=row.client_id,
                label=row.label,
                created_at=float(row.created_at),
                revoked_at=None if row.revoked_at is None else float(row.revoked_at),
                pins=tuple(pins_by_client.get(row.client_id, ())),
                revoked_pins=tuple(revoked_by_client.get(row.client_id, ())),
            )
            for row in client_rows
        )


def _select_all_revoked(session: Session, client_id: Optional[str] = None) -> Tuple[RevokedPin, ...]:
    query = select(_RF).order_by(_RF.c.revoked_at, _RF.c.fingerprint)
    if client_id is not None:
        query = query.where(_RF.c.client_id == client_id)
    return tuple(_revoked_from_row(r) for r in session.execute(query).all())


def list_revoked(resolved: ResolvedConfigPath) -> Tuple[RevokedPin, ...]:
    """Every fingerprint refused for ever (`revoked_fingerprints`), ordered
    by `revoked_at`, fingerprint. Read-only, one snapshot, schema-checked.
    (`list_clients` carries the same rows grouped per client.)"""
    with _read_tx(resolved) as session:
        return _select_all_revoked(session)


def token_status(resolved: ResolvedConfigPath, now: float) -> TokenStatus:
    """The enrollment token's state at `now` (see `TokenStatus`). Never
    returns the secret hash. Read-only: an expired token is reported, not
    retired (only a redemption attempt retires it)."""
    now = _finite(now, "now")
    with _read_tx(resolved) as session:
        return _token_status_from_row(_select_token(session), now)


# --------------------------------------------------------------------------
# Trust writes by the CLI (under the writer lock)
# --------------------------------------------------------------------------


def create_token(
    resolved: ResolvedConfigPath,
    *,
    secret_sha256: str,
    server_fingerprint: str,
    created_at: float,
    expires_at: float,
    client_id: Optional[str] = None,
    replace: bool = False,
    lock: Optional[Path],
    lock_timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT,
) -> TokenStatus:
    """Replace the one enrollment token; return the PREVIOUS token's status
    (evaluated at `created_at`; state `none` if there was none), so the CLI can
    warn that it replaced one.

    `secret_sha256` must be 64 lowercase hex (the hash, never the secret);
    `server_fingerprint` is normalised. `expires_at` must be after
    `created_at` and at most `TOKEN_MAX_TTL_SECONDS` later. `client_id` scopes
    the token to that id, which must already be known (a `clients` row; a
    revoked one is allowed -- re-enrolling clears the tombstone), else
    `LookupError`. `replace=True` requires `client_id`.

    Writes no pin, so the trust generation does not move.

    Raises `ValueError` (bad argument, invalid `lock=`), `LookupError`
    (unknown scoped client), `ConfigBusy`, `ConfigPathError`, `SchemaError`.
    """
    if not isinstance(secret_sha256, str) or not _HEX64_RE.fullmatch(secret_sha256):
        raise ValueError("secret_sha256 must be 64 lowercase hex digits")
    server_fp = normalize_fingerprint(server_fingerprint)
    created_at = _finite(created_at, "created_at")
    expires_at = _finite(expires_at, "expires_at")
    if expires_at <= created_at:
        raise ValueError("expires_at must be later than created_at")
    if expires_at - created_at > TOKEN_MAX_TTL_SECONDS:
        raise ValueError(
            f"token lifetime {expires_at - created_at:g}s exceeds the maximum "
            f"of {TOKEN_MAX_TTL_SECONDS}s (7 days)"
        )
    if client_id is not None:
        client_id = _client_id(client_id)
    if not isinstance(replace, bool):
        raise ValueError(f"replace must be a bool, got {replace!r}")
    if replace and client_id is None:
        raise ValueError("replace=True requires client_id")

    with _cli_writer(resolved, lock, lock_timeout):
        with _write_tx(resolved) as session:
            if client_id is not None and _select_client(session, client_id) is None:
                raise LookupError(
                    f"unknown client id {client_id!r}: a scoped token needs a "
                    "client this server already knows"
                )
            previous = _token_status_from_row(_select_token(session), created_at)
            session.execute(delete(_TK))
            session.execute(insert(_TK).values(
                id=1,
                secret_sha256=secret_sha256,
                server_fingerprint=server_fp,
                created_at=created_at,
                expires_at=expires_at,
                client_id=client_id,
                replace=replace,
                consumed_at=None,
                consumed_fingerprint=None,
                consumed_client_id=None,
            ))
            return previous


def revoke_token(
    resolved: ResolvedConfigPath,
    *,
    lock: Optional[Path],
    lock_timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT,
) -> bool:
    """Delete the enrollment token. True if a token row (in any state) was
    removed, False if there was none. No pin changes, no generation bump."""
    with _cli_writer(resolved, lock, lock_timeout):
        with _write_tx(resolved) as session:
            return session.execute(delete(_TK)).rowcount > 0


def _descendants(session: Session, fingerprint: str) -> Tuple[str, ...]:
    """Every LIVE pin that descends from `fingerprint`, transitively, nearest
    first and each once, through BOTH kinds of rotation link:

    - a pending successor: a pin whose `predecessor_fingerprint` names it;
    - a recorded supersession: a `retired` record's `superseded_by` (the
      successor that retired it). Retiring deletes the predecessor's pin and
      so clears the pending link; this column is what survives. The walk
      continues through a superseder that is no longer pinned (A -> C -> D
      with C retired by D: A leads to D), but only pinned fingerprints are
      returned.

    Read BEFORE any unpinning: deleting a pin clears the links that point at
    it (`_clear_dangling_predecessors`), which would hide them. Cycle-safe:
    each fingerprint is visited once (only a planted row can make a cycle,
    since a successor is always enrolled after what it supersedes)."""
    seen = {fingerprint}
    order = []
    frontier = [fingerprint]
    while frontier:
        following = []
        for row in session.execute(
            select(_TC.c.fingerprint)
            .where(_TC.c.predecessor_fingerprint.in_(frontier))
            .order_by(_TC.c.enrolled_at, _TC.c.fingerprint)
        ).all():
            if row.fingerprint not in seen:
                seen.add(row.fingerprint)
                order.append(row.fingerprint)
                following.append(row.fingerprint)
        for row in session.execute(
            select(_RF.c.superseded_by)
            .where(
                _RF.c.fingerprint.in_(frontier),
                _RF.c.reason == "retired",
                _RF.c.superseded_by.is_not(None),
            )
            .order_by(_RF.c.revoked_at, _RF.c.fingerprint)
        ).all():
            successor = row.superseded_by
            if successor in seen:
                continue
            seen.add(successor)
            following.append(successor)
            if _select_pin(session, successor) is not None:
                order.append(successor)
        frontier = following
    return tuple(order)


def revoke_pin(
    resolved: ResolvedConfigPath,
    fingerprint: str,
    *,
    lock: Optional[Path],
    now: Optional[float] = None,
    lock_timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT,
) -> Union[PinRevocation, SupersededRevocation]:
    """Unpin one certificate, record it as revoked for ever (reason
    `revoked`, `revoked_at=now`, default `time.time()`), and return it as a
    `PinRevocation` (a `TrustedPin`; its `cascaded` lists the other pins this
    call removed). The full fingerprint is required (the CLI resolves
    prefixes); `ValueError` if malformed, `LookupError` if not pinned and not
    a retired key with a live successor (below) -- including a fingerprint
    already revoked (the message says so). Bumps the trust generation (once,
    one transaction). The client itself stays known (and not revoked).

    **Cascade.** Every pin that names the revoked one as its `predecessor`,
    and every such pin's own successor in turn, is unpinned in the same
    transaction and recorded as revoked with reason `successor_revoked`. A
    successor is trusted only because its holder proved possession of the
    predecessor's key at `/rotate`; revoking that key because it was stolen
    must not leave in place the key its holder authorised. Without this a
    thief rotates A -> C (C their own key) before the operator reacts,
    `trust revoke A` removes A, and C keeps the client's access, with the
    link to A cleared so nothing even shows where it came from. The cascade
    runs in this direction only: a successor retiring its OWN predecessor
    (`retire_predecessor`) is the legitimate rotation and removes nothing
    else, and revoking a successor never touches its predecessor.

    **An already retired key.** The cascade must survive the successor's
    first request: one request with C retires A (reason `retired`) and
    clears C's `predecessor`, after which `trust revoke A` used to find A
    "already revoked" and leave C trusted -- the thief needed only one
    request to defeat the cascade. A `retired` record therefore remembers
    its successor (`superseded_by`), and revoking such a fingerprint follows
    that link (through retired intermediates, and through pending
    successors: `_descendants`) to every live pin it leads to, each unpinned
    with `successor_revoked`. A's own record is left as it was. The result is a
    `SupersededRevocation` (A's record, `already_retired` True, `cascaded`
    never empty). Nothing live at the end of that chain, or any other
    recorded reason -> `LookupError` ("... is already revoked"), nothing
    written.
    """
    fp = normalize_fingerprint(fingerprint)
    stamp = time.time() if now is None else _finite(now, "now")
    with _cli_writer(resolved, lock, lock_timeout):
        with _write_tx(resolved) as session:
            row = _select_pin(session, fp)
            # Read BEFORE the unpinning (it clears the links it follows).
            descendants = _descendants(session, fp)
            if row is not None:
                groups = (_Unpin(_TC.c.fingerprint == fp, "revoked"),)
                if descendants:
                    groups += (_Unpin(
                        _TC.c.fingerprint.in_(descendants), "successor_revoked",
                    ),)
                removed = _unpin_all(session, groups, now=stamp)
                _bump_trust_generation(session)
                return PinRevocation(
                    **vars(_pin_from_row(row)),
                    cascaded=removed[1] if descendants else (),
                )
            record = _select_revoked(session, fp)
            if record is None:
                raise LookupError(f"no pinned certificate with fingerprint {fp}")
            if record.reason != "retired" or not descendants:
                detail = (
                    " (retired by rotation; nothing that superseded it is "
                    "still pinned)" if record.reason == "retired" else ""
                )
                raise LookupError(
                    f"the certificate with fingerprint {fp} is already revoked{detail}"
                )
            cascaded = _unpin(
                session, _TC.c.fingerprint.in_(descendants),
                now=stamp, reason="successor_revoked",
            )
            _bump_trust_generation(session)
            return SupersededRevocation(
                **vars(_revoked_from_row(record)), cascaded=cascaded,
            )


def revoke_client(
    resolved: ResolvedConfigPath,
    client_id: str,
    *,
    lock: Optional[Path],
    now: Optional[float] = None,
    lock_timeout: float = DEFAULT_WRITER_LOCK_TIMEOUT,
) -> ClientRecord:
    """Revoke a client id: delete all its pins, record each fingerprint as
    revoked for ever (reason `client_revoked`), and set its tombstone
    (`revoked_at`, kept if already set). `LookupError` if the id is unknown.

    Returns the client's record as it now stands (`revoked_pins` included),
    EXCEPT that `pins` lists the pins this call removed (empty if it had
    none). Bumps the trust generation only when a pin was removed. `now`
    defaults to `time.time()`. A revoked id may be re-enrolled later with a
    token scoped to it, but only with a NEW certificate.
    """
    if not isinstance(client_id, str):
        raise ValueError(f"client id must be a string, got {client_id!r}")
    stamp = time.time() if now is None else _finite(now, "now")
    with _cli_writer(resolved, lock, lock_timeout):
        with _write_tx(resolved) as session:
            client = _select_client(session, client_id)
            if client is None:
                raise LookupError(f"unknown client id {client_id!r}")
            removed = _unpin(
                session, _TC.c.client_id == client_id,
                now=stamp, reason="client_revoked",
            )
            if removed:
                _bump_trust_generation(session)
            revoked_at = client.revoked_at
            if revoked_at is None:
                revoked_at = stamp
                session.execute(
                    update(_CL).where(_CL.c.client_id == client_id).values(
                        revoked_at=revoked_at,
                    )
                )
            return ClientRecord(
                client_id=client.client_id,
                label=client.label,
                created_at=float(client.created_at),
                revoked_at=float(revoked_at),
                pins=removed,
                revoked_pins=_select_all_revoked(session, client_id),
            )


# --------------------------------------------------------------------------
# Trust writes by the API worker (no flock). Each one: a read-only pre-check
# (refusals and no-ops end there, without the write lock), then
# `_API_WRITE_LOCK`, then one `BEGIN IMMEDIATE` transaction that re-checks
# everything -- the pre-check is a filter, never the decision.
# --------------------------------------------------------------------------


def _token_spent(row) -> bool:  # noqa: ANN001
    """A row with ANY `consumed_*` column set is spent, whichever columns
    they are (redeemed, or retired unredeemed by the expiry latch)."""
    return row.consumed_at is not None or _token_redeemed(row)


def _token_credentials_match(row, *, server_fp: str, secret_sha256: object) -> bool:  # noqa: ANN001
    """Does the caller present this token's server fingerprint AND secret
    hash? Both are compared, always, in constant time (`hmac.compare_digest`)
    and with no early exit, so a fingerprint mismatch cannot be told apart
    from a secret mismatch. A malformed hash is compared as a dummy
    (`compare_digest` needs ASCII str) and refused by `well_formed`, whatever
    the dummy compares to."""
    fp_ok = hmac.compare_digest(row.server_fingerprint, server_fp)
    well_formed = isinstance(secret_sha256, str) and bool(
        _HEX64_RE.fullmatch(secret_sha256)
    )
    given = secret_sha256 if well_formed else "0" * 64
    secret_ok = hmac.compare_digest(row.secret_sha256, given)
    return fp_ok and secret_ok and well_formed


def _token_worth_locking_for(row, *, server_fp: str, secret_sha256: object, now: float) -> bool:  # noqa: ANN001
    """The row-level core of `token_precheck` and of `enroll_client`'s
    pre-check: False means `REFUSED` with no write lock taken, ever.

    False for: no token, a spent token, wrong credentials, or a clock behind
    the token's creation (never valid, never latched). An EXPIRED token whose
    credentials match is True: it must reach the write transaction so the
    latch can retire it -- which only a caller holding the secret can cause.
    """
    if row is None or _token_spent(row):
        return False
    if not _token_credentials_match(row, server_fp=server_fp, secret_sha256=secret_sha256):
        return False
    return now >= row.created_at


def token_precheck(
    resolved: ResolvedConfigPath,
    *,
    secret_sha256: object,
    server_fingerprint: str,
    now: Clock,
    fingerprint: Optional[str] = None,
) -> bool:
    """Cheap, read-only "is this enrollment attempt worth any further work?".

    The API calls this at the top of `/enroll`, BEFORE anything costly (the
    `zfs.exists(target/<client_id>)` call), so an unauthenticated caller with
    a wrong token costs one SQLite read and no `zfs` exec, no write lock.

    True means: a token row exists, is unspent (neither redeemed nor retired
    by the expiry latch), its server fingerprint and secret hash both match
    in constant time (`hmac.compare_digest`, no early exit; a malformed
    `secret_sha256` is just False), `now` is not before its creation, and
    -- when `fingerprint` (the client certificate's) is given -- that
    fingerprint is not revoked (`revoked_fingerprints`). An
    EXPIRED token with matching credentials is True on purpose: it must reach
    `enroll_client` so the durable expiry latch can retire it (only a caller
    holding the secret can cause that). False carries no reason -- a wrong
    secret, a spent, absent or future-dated token are indistinguishable.

    A FILTER, never the decision: it takes no write lock and writes nothing,
    and `enroll_client` re-checks everything (expiry, pins, scope) inside its
    write transaction. True promises nothing about the redemption's outcome.

    `now`: POSIX seconds or a zero-argument callable (see "Time").
    `server_fingerprint` (and `fingerprint`, if given) must be a SHA-256
    fingerprint (`ValueError`, the API validates it first). Pass
    `fingerprint` so a revoked certificate costs no `zfs` call either; the
    revoked check is the same uniform False. Raises `ConfigBusy` (a read could not obtain
    its snapshot within the busy timeout: answer 503), `ConfigPathError`,
    `SchemaError`.
    """
    server_fp = normalize_fingerprint(server_fingerprint)
    client_fp = None if fingerprint is None else normalize_fingerprint(fingerprint)
    clock = _clock(now)
    with _read_tx(resolved) as session:
        # The credential comparison runs first and always (constant time),
        # whatever the revoked check then says.
        worth = _token_worth_locking_for(
            _select_token(session), server_fp=server_fp,
            secret_sha256=secret_sha256, now=clock(),
        )
        if client_fp is not None and _is_revoked(session, client_fp):
            return False
        return worth


def _token_redeemable(
    session: Session, row, *, server_fp: str, secret_sha256: object, now: float,  # noqa: ANN001
) -> bool:
    """Check 1 of `enroll_client`, under the write lock: may this token be
    redeemed at `now` (read AFTER the lock was obtained)? Every failure is
    the same `False`.

    Credentials are verified BEFORE the clock is consulted. Only then is an
    expired token retired (`consumed_at = now`, `consumed_fingerprint` NULL)
    as a durable latch, inside the caller's write transaction: a clock that
    later steps back into the token's window must not make it redeemable
    again. Checking the clock first let ANY unauthenticated `/enroll` during
    a forward clock step burn a valid token for good."""
    if row is None or _token_spent(row):
        return False
    if not _token_credentials_match(row, server_fp=server_fp, secret_sha256=secret_sha256):
        return False
    if now >= row.expires_at:
        session.execute(
            update(_TK)
            .where(_TK.c.id == 1, _TK.c.consumed_at.is_(None))
            .values(consumed_at=now)
        )
        return False
    if now < row.created_at:
        # The clock is behind the token's creation: never valid. Not latched
        # (the token may be valid once the clock is right again).
        return False
    return True


def enroll_client(
    resolved: ResolvedConfigPath,
    *,
    secret_sha256: str,
    server_fingerprint: str,
    client_id: str,
    pin: NewPin,
    label: Optional[str] = None,
    now: Clock,
    dataset_exists: bool,
) -> EnrollOutcome:
    """Redeem the enrollment token for `pin` under `client_id` (`/enroll`).

    No flock (the API worker calls this from a request thread). Two phases:

    - **Read-only pre-check** (one read snapshot, no write lock): no token, a
      spent one, a different server fingerprint or secret hash (constant
      time), `now` before the token's creation, or a revoked `pin`
      fingerprint -> `REFUSED` right there.
      This endpoint is unauthenticated; a flood of wrong guesses must not
      take SQLite's write lock (it starved the CLI's writers).
    - **Write transaction**: `_API_WRITE_LOCK`, then one `BEGIN IMMEDIATE`
      transaction holding SQLite's write lock across every check and write,
      so concurrent enrollers serialise and exactly one redeems a token.
      `now` is evaluated HERE when it is a callable (pass `time.time`), so
      lock waiting cannot extend a token's life. Everything is re-checked.

    Checks, in order (all under the write lock):

    0. **A revoked fingerprint** (`revoked_fingerprints`: revoked, revoked
       with its client, dropped by a `--replace`, or a retired rotation
       predecessor) -> `REFUSED`, before the token is looked at: the token
       is neither consumed nor latched. Revocation is permanent; without
       this a stolen, revoked key could be re-enrolled with any fresh token.
    1. **The token**: none, spent, a different server fingerprint or a
       different secret hash (`hmac.compare_digest`), expired, or `now`
       before its creation -> `REFUSED`, identical for every cause. An
       expired token is retired durably (the latch) only once the
       credentials have matched.
    2. **The fingerprint is already pinned**: to another id ->
       `CERT_BOUND_ELSEWHERE`, token not consumed. To `client_id`: with a
       token scoped to `client_id` with `replace`, the replace is applied
       (every OTHER pin of `client_id` is deleted and recorded as revoked,
       reason `replaced`; `label` set if given) and the redemption succeeds
       (step 5, `ENROLLED`) -- otherwise a client re-presenting its current
       certificate could never use a replace token to drop a lost or stolen
       sibling key. Any other token -> `ALREADY_TRUSTED`, token NOT consumed.
    3. **Scoped token for X**: `client_id != X` -> `REFUSED`. Otherwise pin to
       X, clear X's tombstone (and set `label` if given); with `replace`,
       delete X's other pins in the same transaction, recording each as
       revoked (reason `replaced`).
    4. **Unscoped token**: a known id -- a `clients` row (revoked or not) or
       `dataset_exists` (the server already holds `target/<client_id>`) ->
       `CLIENT_ID_TAKEN`, token NOT consumed. Otherwise create the client and
       its pin.
    5. **Success**: consume the token (`consumed_at`, `consumed_fingerprint`,
       `consumed_client_id`), bump the trust generation -> `ENROLLED`.

    Only `ENROLLED` changes pins, and every `ENROLLED` bumps the generation
    (the replace of an already-pinned certificate too, even when it had no
    other pin to drop). The expiry latch in
    check 1 is the one write on a refusing path, and only a caller holding
    the token's secret can reach it.

    `now`: POSIX seconds, or a zero-argument callable returning them (see
    the module's "Time"). `client_id` must satisfy the A7 grammar and
    `server_fingerprint` be a SHA-256 fingerprint (`ValueError`: the API
    validates both first, so this is a programming error). A malformed
    `secret_sha256` is just `REFUSED`. Raises `ConfigBusy` (a lock not
    obtained within the busy timeout: answer 503 with retry),
    `ConfigPathError`, `SchemaError`.
    """
    client_id = _client_id(client_id)
    server_fp = normalize_fingerprint(server_fingerprint)
    clock = _clock(now)
    label = _label(label)
    if not isinstance(pin, NewPin):
        raise ValueError(f"pin must be a NewPin, got {type(pin).__name__}")

    if not token_precheck(
        resolved, secret_sha256=secret_sha256, server_fingerprint=server_fp, now=clock,
        fingerprint=pin.fingerprint,
    ):
        return EnrollOutcome.REFUSED

    with _api_writer(resolved), _write_tx(resolved) as session:
        # `_write_tx` has already run `check_schema`, the transaction's first
        # statement, so `BEGIN IMMEDIATE` is done: the write lock is held and
        # this clock reading cannot be stale by any lock wait.
        now = clock()
        if _is_revoked(session, pin.fingerprint):
            # Check 0: revoked between the pre-check and the lock.
            return EnrollOutcome.REFUSED
        token = _select_token(session)
        if not _token_redeemable(
            session, token, server_fp=server_fp, secret_sha256=secret_sha256, now=now
        ):
            return EnrollOutcome.REFUSED

        scope = token.client_id
        existing = _select_pin(session, pin.fingerprint)
        if existing is not None:
            owner = _select_client(session, existing.client_id)
            if owner is None or owner.revoked_at is not None:
                # Unreachable while every write here keeps "a revoked client
                # holds no pins"; fail closed rather than report trust.
                return EnrollOutcome.REFUSED
            if existing.client_id != client_id:
                return EnrollOutcome.CERT_BOUND_ELSEWHERE
            if not (token.replace and scope == client_id):
                return EnrollOutcome.ALREADY_TRUSTED
            # Check 2, replace: the certificate stays as pinned; drop the
            # client's OTHER pins, then redeem the token (step 5).
            if label is not None:
                session.execute(
                    update(_CL).where(_CL.c.client_id == scope).values(label=label)
                )
            _unpin(
                session,
                (_TC.c.client_id == scope) & (_TC.c.fingerprint != pin.fingerprint),
                now=now, reason="replaced",
            )
        elif scope is not None:
            if client_id != scope:
                return EnrollOutcome.REFUSED
            client = _select_client(session, scope)
            if client is None:
                # `create_token` required the row and nothing deletes one;
                # recreate rather than fail on an FK error.
                session.execute(insert(_CL).values(
                    client_id=scope, label=label, created_at=now, revoked_at=None,
                ))
            else:
                values = {"revoked_at": None}
                if label is not None:
                    values["label"] = label
                session.execute(
                    update(_CL).where(_CL.c.client_id == scope).values(**values)
                )
            if token.replace:
                _unpin(session, _TC.c.client_id == scope, now=now, reason="replaced")
            _insert_pin(session, pin, scope, now=now, via="token")
        else:
            if dataset_exists or _select_client(session, client_id) is not None:
                return EnrollOutcome.CLIENT_ID_TAKEN
            session.execute(insert(_CL).values(
                client_id=client_id, label=label, created_at=now, revoked_at=None,
            ))
            _insert_pin(session, pin, client_id, now=now, via="token")

        _clear_dangling_predecessors(session)
        session.execute(
            update(_TK).where(_TK.c.id == 1).values(
                consumed_at=now,
                consumed_fingerprint=pin.fingerprint,
                consumed_client_id=client_id,
            )
        )
        _bump_trust_generation(session)
        return EnrollOutcome.ENROLLED


def _live_owner(session: Session, fingerprint: str):  # noqa: ANN202
    """`(pin_row, client_row)` when `fingerprint` is pinned to a client that
    is not revoked and is not itself a revoked fingerprint, else `None`."""
    row = _select_pin(session, fingerprint)
    if row is None or _is_revoked(session, fingerprint):
        return None
    owner = _select_client(session, row.client_id)
    if owner is None or owner.revoked_at is not None:
        return None
    return row, owner


def rotate_client(
    resolved: ResolvedConfigPath,
    *,
    old_fp: str,
    new_pin: NewPin,
    now: Clock,
) -> RotateOutcome:
    """Pin a successor certificate for the client that owns `old_fp`
    (`/rotate`, the store half of plan D4).

    A read-only pre-check refuses an `old_fp` that is not a live pin, or a
    revoked `new_pin` fingerprint, without taking the write lock; then `_API_WRITE_LOCK` and one `BEGIN IMMEDIATE`
    transaction re-check everything. `now` (a number or a callable, as for
    `enroll_client`) is evaluated under the write lock.

    - `old_fp` must be a live pin of a non-revoked client, else `REFUSED`.
    - `new_pin`'s fingerprint is revoked (`revoked_fingerprints`, for any
      reason, any client) -> `REFUSED`: revocation is permanent, and a
      rotation must not be a back door around it. A client re-running a
      rotation with a pending successor the operator has since unpinned must
      generate a new successor key.
    - `new_pin` already pinned to that client with `predecessor == old_fp` ->
      `ALREADY` (an idempotent re-run); pinned anywhere else (another client,
      or this client without that predecessor, including `new_pin == old_fp`)
      -> `REFUSED`.
    - **One successor per predecessor.** `old_fp` already has a live
      successor (a pin whose `predecessor == old_fp`) that is not `new_pin`
      -> `REFUSED`. Without this, A->B then a retry A->C (a regenerated key)
      both returned `ROTATED`; whichever successor retired A first left the
      other pinned forever, with no predecessor and nobody holding its key.
      Refusing (rather than replacing the pending B with C) is deliberate:
      the holder of A is also whoever stole A, and "replace" would let a
      thief evict the legitimate pending successor and lock the client out,
      whereas "refuse" lets the first successor win and its first request
      retire A for good. A client that lost B before using it recovers by
      re-running with the SAME key B (`ALREADY`), or the operator unpins B
      (`revoke_pin`) and the client rotates again.
    - Otherwise, if `old_fp` itself still has a pinned predecessor of the
      same client (it was a rotation successor whose first request has not
      retired that predecessor yet), that predecessor is retired first,
      exactly as `retire_predecessor(old_fp)` would (reason `retired`,
      `superseded_by=old_fp`, `old_fp`'s field cleared; a stale field is
      only cleared). A `/rotate` authenticated with `old_fp` proves it is in
      use, the condition first-use retirement waits for, and doing it here
      keeps the chain recorded, so `revoke_pin` of that predecessor still
      reaches `new_pin`. Then `new_pin` is pinned to that client with
      `enrolled_via='rotate'` and `predecessor=old_fp`, and the trust
      generation is bumped once -> `ROTATED`. The old pin stays until
      `retire_predecessor` (the successor's first authenticated request).

    The caller has authenticated `old_fp` (the TLS client certificate) and
    verified the successor's proof of possession. Raises `ValueError` (bad
    arguments), `ConfigBusy`, `ConfigPathError`, `SchemaError`.
    """
    old = normalize_fingerprint(old_fp)
    clock = _clock(now)
    if not isinstance(new_pin, NewPin):
        raise ValueError(f"new_pin must be a NewPin, got {type(new_pin).__name__}")

    with _read_tx(resolved) as session:
        if _live_owner(session, old) is None or _is_revoked(session, new_pin.fingerprint):
            return RotateOutcome.REFUSED

    with _api_writer(resolved), _write_tx(resolved) as session:
        now = clock()
        live = _live_owner(session, old)
        if live is None or _is_revoked(session, new_pin.fingerprint):
            return RotateOutcome.REFUSED
        old_row, _owner = live
        existing = _select_pin(session, new_pin.fingerprint)
        if existing is not None:
            if (
                existing.client_id == old_row.client_id
                and existing.predecessor_fingerprint == old
            ):
                return RotateOutcome.ALREADY
            return RotateOutcome.REFUSED
        # `new_pin` is not pinned (checked just above), so any pin naming
        # `old` as predecessor is a DIFFERENT successor.
        other_successor = session.execute(
            select(_TC.c.fingerprint)
            .where(_TC.c.predecessor_fingerprint == old)
            .limit(1)
        ).first()
        if other_successor is not None:
            return RotateOutcome.REFUSED
        # `old` rotating on proves it is in use -- exactly what first-use
        # retirement waits for -- so its own still-pinned predecessor is
        # retired here, as `retire_predecessor(old)` would. Otherwise, if
        # `old` rotated before any request of its own retired that
        # predecessor (a busy store, a dry run) and the new pin later retired
        # `old`, the predecessor stayed live with no link to the new pin, and
        # revoking it (a stolen key) left the new pin trusted.
        if old_row.predecessor_fingerprint is not None:
            predecessor = _select_pin(session, old_row.predecessor_fingerprint)
            session.execute(
                update(_TC).where(_TC.c.fingerprint == old).values(
                    predecessor_fingerprint=None,
                )
            )
            if predecessor is not None and predecessor.client_id == old_row.client_id:
                _unpin(
                    session, _TC.c.fingerprint == predecessor.fingerprint,
                    now=now, reason="retired", superseded_by=old,
                )
        _insert_pin(
            session, new_pin, old_row.client_id, now=now, via="rotate",
            predecessor=old,
        )
        _bump_trust_generation(session)
        return RotateOutcome.ROTATED


def retire_predecessor(
    resolved: ResolvedConfigPath,
    fingerprint: str,
    *,
    now: Optional[float] = None,
) -> bool:
    """Retire the predecessor of pin `fingerprint` (the successor's first
    authenticated request, plan D4). True if a pin was deleted.

    If pin `fingerprint` names a predecessor that is still pinned to the SAME
    client: delete the predecessor, record it as revoked for ever (reason
    `retired`, `revoked_at=now`, default `time.time()` read under the write
    lock; `superseded_by=fingerprint`, which `revoke_pin` of the retired
    key follows), clear the field, bump the trust generation. A retired key is
    refused for re-enrollment like a revoked one: it is the superseded key
    most likely to survive in an old copy of the client's TLS directory, and
    the client never legitimately presents it again. If the predecessor is gone or belongs to another client, the
    stale field is cleared and nothing else changes (False). Unknown pin or no
    predecessor: False, decided on a read-only snapshot with no write lock
    taken -- so the API may call this on every authenticated request. Otherwise
    `_API_WRITE_LOCK` and one `BEGIN IMMEDIATE` transaction that re-checks, so
    two racing calls retire once: the second sees the cleared field.

    Raises `ValueError` (malformed fingerprint), `ConfigBusy` (the caller
    retries on a later request), `ConfigPathError`, `SchemaError`.
    """
    fp = normalize_fingerprint(fingerprint)
    fixed_now = None if now is None else _finite(now, "now")
    with _read_tx(resolved) as session:
        row = _select_pin(session, fp)
        if row is None or row.predecessor_fingerprint is None:
            return False

    with _api_writer(resolved), _write_tx(resolved) as session:
        stamp = time.time() if fixed_now is None else fixed_now
        row = _select_pin(session, fp)
        if row is None or row.predecessor_fingerprint is None:
            return False
        predecessor = _select_pin(session, row.predecessor_fingerprint)
        retired = predecessor is not None and predecessor.client_id == row.client_id
        # Clear the field first, so `_unpin`'s dangling-predecessor sweep has
        # nothing of ours left to touch.
        session.execute(
            update(_TC).where(_TC.c.fingerprint == fp).values(
                predecessor_fingerprint=None,
            )
        )
        if retired:
            # `superseded_by` keeps the link this retirement clears, so a
            # later `revoke_pin` of the retired key still reaches `fp`.
            _unpin(
                session, _TC.c.fingerprint == predecessor.fingerprint,
                now=stamp, reason="retired", superseded_by=fp,
            )
            _bump_trust_generation(session)
        return retired

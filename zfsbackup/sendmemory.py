"""What the senders remember between sends (plan "Track B", item B1.3).

The once-per-episode log latches, the consecutive-skip counters, the
"copy confirmed" set, the local copies' pending map and the daily certificate
reminder used to live on one long-lived `RemoteBackupManager` /
`LocalBackupManager` / `DatasetManager` each. Track B builds fresh managers
for every send and runs several sends at once, so everything that outlives a
send, or is shared between sends, lives here: ONE `SendMemory` per worker,
handed to every manager it builds (`ChainSender(..., memory=)`,
`DatasetManager(config, memory=)`, or `adopt_memory()` for a manager that is
already built). A manager built without one gets a private instance, so a
manager used alone behaves as before.

**One lock, atomic operations.** Every operation takes the one
`threading.Lock`, does a dictionary or set operation and returns what the caller
needs to decide: `note()` says whether the latch was newly set (log), `clear()`
whether it was set (log the recovery), `incr()` the new count,
`note_value()` whether the stored message changed. The caller DECIDES from the
return value and LOGS OUTSIDE the lock; nothing here logs, calls back (apart
from the small pure predicates of `forget_where` / `retain` /
`replace_group`, which run under the lock and must not take another lock) or
does I/O.

**Per-call state stays on the instance.** `_server_info`, `_server_newer`,
`_call_note`, `_call_retryable`, the transports and the per-cycle checks belong
to one call or one manager and are NOT here.

**Tables are addressed by name** (the constants below). `latch_view`,
`counter_view` and `value_view` return the LIVE container for tests and
diagnostics; production code must use the operations (a view is read without
the lock). The object pickles (an empty memory is what a spawned worker
starts with; the lock is recreated).

Stdlib only: this module is imported by `backup_manager`, `chain`, `remote`,
`local` and `workers` and must never import any of them.
"""

import threading
from typing import Any, Callable, Dict, Hashable, Iterable, List, Optional, Set, Tuple


# ---- latches (sets of keys) -------------------------------------------------
#: (dataset name, what, snapshot name) of future-dated snapshots/anchors announced.
CLOCK = 'clock'
#: (dataset name, snapshot name) of managed snapshots dropped for having no `createtxg`.
NO_TXG = 'no_txg'
#: Parents whose recursive discovery fails (ERROR once), (parent, target) and
#: (parent, copy root) pairs skipped by `expand_recursive`.
EXPAND_ERROR = 'expand_error'
EXPAND_TARGET = 'expand_target'
EXPAND_COPY = 'expand_copy'
#: (ancestor, destination) pairs whose waiting children were announced (R-2).
WAITING = 'waiting'
#: (child, destination) pairs whose "cannot find the dataset it was expanded from"
#: WARNING (`AncestorRule.hold`) was logged: once per episode, cleared when the
#: child is no longer orphaned.
ORPHAN_HOLD = 'orphan_hold'
#: `--dry-run` only: (dataset, destination) pairs whose `backup_dataset` returned True.
DRY_RUN_SENT = 'dry_run_sent'
#: (dataset, destination) pairs whose copy was CONFIRMED in this incarnation.
COPY_CONFIRMED = 'copy_confirmed'
#: (dataset, destination) pairs whose retention-rule collisions were warned about.
RETENTION_WARNED = 'retention_warned'
#: (dataset, destination) pairs whose resume-token / negotiate-`zfs_timeout` ERROR was logged.
TOKEN_ERROR = 'token_error'
NEGOTIATE_ZFS_ERROR = 'negotiate_zfs_error'
#: `'remote'` once the "no client id" ERROR was logged.
NO_CLIENT_ID = 'no_client_id'

# ---- counters (key -> consecutive count) --------------------------------------
TOKEN_SKIPS = 'token_skips'
NEGOTIATE_ZFS_SKIPS = 'negotiate_zfs_skips'

# ---- value maps (key -> the last value) ---------------------------------------
#: `(label or None, destination) -> the last refusal logged for it`.
REFUSALS = 'refusals'
#: copy name -> `monotonic()` of the negotiate that found it missing (local copies).
PENDING = 'pending'
#: `'remote'` -> the last client-identity error logged.
IDENTITY_ERROR = 'identity_error'


class SendMemory:
    """See the module docstring. All operations are atomic with respect to each
    other; none logs."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latches: Dict[str, Set[Hashable]] = {}
        self._counters: Dict[str, Dict[Hashable, int]] = {}
        self._values: Dict[str, Dict[Hashable, Any]] = {}
        self._expiry_checked_at: Optional[float] = None

    # A spawned worker pickles its object (the memory is empty then, and idle);
    # a lock cannot be. The containers are handed over as they are, so an alias
    # of one (a test's `latch_view`) is still the same object after the round
    # trip (pickle memoizes by identity).
    def __getstate__(self) -> dict:
        return {
            'latches': self._latches,
            'counters': self._counters,
            'values': self._values,
            'expiry_checked_at': self._expiry_checked_at,
        }

    def __setstate__(self, state: dict) -> None:
        self._lock = threading.Lock()
        self._latches = state['latches']
        self._counters = state['counters']
        self._values = state['values']
        self._expiry_checked_at = state['expiry_checked_at']

    # ------------------------------------------------------------------ tables
    # (callers hold the lock)

    def _set(self, name: str) -> Set[Hashable]:
        table = self._latches.get(name)
        if table is None:
            table = self._latches[name] = set()
        return table

    def _count(self, name: str) -> Dict[Hashable, int]:
        table = self._counters.get(name)
        if table is None:
            table = self._counters[name] = {}
        return table

    def _map(self, name: str) -> Dict[Hashable, Any]:
        table = self._values.get(name)
        if table is None:
            table = self._values[name] = {}
        return table

    # ----------------------------------------------------------------- latches

    def note(self, name: str, key: Hashable) -> bool:
        """Set the latch `key` of table `name`. -> True when it was NOT set
        (the caller announces), False when it already was."""
        with self._lock:
            table = self._set(name)
            if key in table:
                return False
            table.add(key)
            return True

    def clear(self, name: str, key: Hashable) -> bool:
        """Clear the latch. -> True when it WAS set (the caller logs the
        recovery)."""
        with self._lock:
            table = self._set(name)
            if key in table:
                table.discard(key)
                return True
            return False

    def noted(self, name: str, key: Hashable) -> bool:
        with self._lock:
            return key in self._set(name)

    def reset_latches(self, name: str) -> None:
        """Clear every latch of table `name`."""
        with self._lock:
            self._set(name).clear()

    def forget_where(self, name: str, predicate: Callable[[Hashable], bool]) -> int:
        """Remove every key of table `name` for which `predicate(key)` holds
        (it runs under the lock: pure and quick). -> how many."""
        with self._lock:
            table = self._set(name)
            gone = [k for k in table if predicate(k)]
            table.difference_update(gone)
            return len(gone)

    def retain(self, name: str, predicate: Callable[[Hashable], bool]) -> None:
        """Keep only the keys of table `name` for which `predicate(key)` holds."""
        with self._lock:
            table = self._set(name)
            table.intersection_update([k for k in table if predicate(k)])

    def replace_group(
        self, name: str, in_group: Callable[[Hashable], bool], keys: Iterable[Hashable],
    ) -> List[Hashable]:
        """Atomically make the keys of table `name` that belong to a group
        (`in_group(key)`) exactly `keys`. -> the members of `keys` that were
        NOT set before, in the order given (what is new to announce)."""
        keys = list(keys)
        with self._lock:
            table = self._set(name)
            fresh = [k for k in keys if k not in table]
            table.difference_update([k for k in table if in_group(k)])
            table.update(keys)
            return fresh

    # ---------------------------------------------------------------- counters

    def incr(self, name: str, key: Hashable) -> int:
        """Add one to the counter. -> the new count."""
        with self._lock:
            table = self._count(name)
            count = table[key] = table.get(key, 0) + 1
            return count

    def reset(self, name: str, key: Hashable) -> Optional[int]:
        """Drop the counter. -> its last value, `None` if there was none."""
        with self._lock:
            return self._count(name).pop(key, None)

    def seed_max(self, name: str, key: Hashable, count: int) -> None:
        """Raise the counter to at least `count` (never lowers it)."""
        with self._lock:
            table = self._count(name)
            table[key] = max(table.get(key, 0), int(count))

    def count(self, name: str, key: Hashable) -> int:
        with self._lock:
            return self._count(name).get(key, 0)

    # ------------------------------------------------------------- value maps

    def note_value(self, name: str, key: Hashable, value: Any) -> bool:
        """Store `value` under `key`. -> True when it differs from what was
        stored (the caller announces it), False when it is the same (repeat)."""
        with self._lock:
            table = self._map(name)
            if key in table and table[key] == value:
                return False
            table[key] = value
            return True

    def put(self, name: str, key: Hashable, value: Any) -> None:
        with self._lock:
            self._map(name)[key] = value

    def get(self, name: str, key: Hashable, default: Any = None) -> Any:
        with self._lock:
            return self._map(name).get(key, default)

    def pop(self, name: str, key: Hashable, default: Any = None) -> Any:
        """Remove and return the value (`default` if there was none)."""
        with self._lock:
            return self._map(name).pop(key, default)

    def items(self, name: str) -> List[Tuple[Hashable, Any]]:
        """A COPY of the table's items: safe to iterate while other threads
        write."""
        with self._lock:
            return list(self._map(name).items())

    # ----------------------------------------------------------------- pending

    def pending_put(self, key: Hashable, now: float) -> None:
        """A copy was found missing at `now` (a monotonic time)."""
        self.put(PENDING, key, now)

    def pending_clear(self, key: Hashable) -> None:
        self.pop(PENDING, key)

    def pending_active(self, key: Hashable, ttl: float, now: float) -> bool:
        """Whether `key` was put less than `ttl` seconds before `now`. An
        expired entry is removed."""
        with self._lock:
            table = self._map(PENDING)
            since = table.get(key)
            if since is None:
                return False
            if now - since < ttl:
                return True
            del table[key]
            return False

    # ------------------------------------------------------------ certificate

    def claim_expiry_check(self, every: float, now: float) -> bool:
        """Whether the client certificate's expiry is to be checked now: the
        first caller of an incarnation, then at most once per `every` seconds
        (monotonic `now`). The claim is atomic, so of several callers exactly
        one gets True."""
        with self._lock:
            last = self._expiry_checked_at
            if last is not None and now - last < every:
                return False
            self._expiry_checked_at = now
            return True

    @property
    def expiry_checked_at(self) -> Optional[float]:
        return self._expiry_checked_at

    # ------------------------------------------------------------------- views

    def latch_view(self, name: str) -> Set[Hashable]:
        """The LIVE latch set (tests and diagnostics; read without the lock)."""
        with self._lock:
            return self._set(name)

    def counter_view(self, name: str) -> Dict[Hashable, int]:
        """The LIVE counter dict (tests and diagnostics)."""
        with self._lock:
            return self._count(name)

    def value_view(self, name: str) -> Dict[Hashable, Any]:
        """The LIVE value dict (tests and diagnostics)."""
        with self._lock:
            return self._map(name)

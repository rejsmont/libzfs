from libzfseasy.types import InvalidNameError, InvalidValueError, Validate, ZFS, Dataset, Filesystem, Volume, Snapshot as Snapshot, Bookmark, \
     SnapshotRange, Property, Properties
from typing import Optional, Union, Dict, Iterable, Callable, NamedTuple, Tuple

import collections
import contextlib
import contextvars
import dataclasses
import io
import logging
import math
import os
import re
import selectors
import shlex
import subprocess
import shutil
import signal
import threading
import time

# Allow configuring zfs and zpool commands via environment variables
ZFS_BIN = os.environ.get('ZFS_CMD') or shutil.which('zfs')
if ZFS_BIN is None:
    raise RuntimeError(
        'zfs command not found in PATH. Ensure ZFS utilities are installed and available. '
        'On macOS, install with: brew install openzfs '
        'Or set ZFS_CMD environment variable to the path to the zfs command.'
    )

ZPOOL_BIN = os.environ.get('ZPOOL_CMD') or shutil.which('zpool')
if ZPOOL_BIN is None:
    raise RuntimeError(
        'zpool command not found in PATH. Ensure ZFS utilities are installed and available. '
        'On macOS, install with: brew install openzfs '
        'Or set ZPOOL_CMD environment variable to the path to the zpool command.'
    )

def _zfs_cmd() -> str:
    """Return the zfs binary path, re-reading ZFS_CMD at call time to support runtime overrides."""
    return os.environ.get('ZFS_CMD') or ZFS_BIN


def _zpool_cmd() -> str:
    """Return the zpool binary path, re-reading ZPOOL_CMD at call time to support runtime overrides."""
    return os.environ.get('ZPOOL_CMD') or ZPOOL_BIN


Snapshots = Union[Snapshot, SnapshotRange]
SnapshotList = Union[Snapshots, Iterable[Snapshots]]
Datasets = Union[Filesystem, Volume, Dataset, Snapshot]
DatasetList = Union[Datasets, Iterable[Datasets]]
ZFSList = Union[ZFS, Iterable[ZFS]]
StringList = Union[str, Iterable[str]]
Sort = Union[str, Iterable[str], Dict[str, bool]]

# Retained for backward compatibility: accepted but ignored. Commands are logged at DEBUG on the
# 'libzfseasy' logger instead (configure logging to see them).
debug = True

logger = logging.getLogger('libzfseasy')

# Names already reported by _skip_invalid_name (bounded, once per distinct name per `_WARNED_NAMES_TTL` seconds).
_MAX_WARNED_NAMES = 1000
_warned_names = set()
_warned_lock = threading.Lock()
# The registry is emptied this often, so a name that keeps being skipped is reported again (about once an hour)
# instead of going quiet for the rest of a long-lived process.
_WARNED_NAMES_TTL = 3600.0
_warned_names_since = None
# Bumped (under the lock) every time `_skip_invalid_name` logs a WARNING, for callers that cannot see its return value.
_name_warnings = 0


def _skip_invalid_name(name: str, error: Exception) -> bool:
    """Log (once per distinct name per `_WARNED_NAMES_TTL`) that a row of zfs output was skipped because its name is
    one zfs allows but libzfseasy cannot represent. Returns True when this call logged a WARNING (a new name, one whose
    TTL expired, or the one-off "too many distinct names" notice), False when the name was already reported."""
    global _warned_names_since, _name_warnings
    with _warned_lock:
        now = time.monotonic()
        if _warned_names_since is None:
            _warned_names_since = now
        elif now - _warned_names_since >= _WARNED_NAMES_TTL:
            _warned_names.clear()
            _warned_names_since = now
        if name in _warned_names:
            return False
        if len(_warned_names) >= _MAX_WARNED_NAMES:
            if len(_warned_names) == _MAX_WARNED_NAMES:
                _warned_names.add(None)
                _name_warnings += 1
                logger.warning('skipping names from zfs output: more than %d distinct names libzfseasy cannot '
                               'represent; further ones are skipped silently', _MAX_WARNED_NAMES)
                return True
            return False
        _warned_names.add(name)
        _name_warnings += 1
    logger.warning('skipping %r from zfs output: libzfseasy cannot represent this name (%s)', _clip(name), error)
    return True


# How much of a skipped row (or name) a warning quotes, and how many skipped rows one listing reports one by one before
# it falls back to a count. zfs output can carry user-property values of several KiB, chosen by whoever set them.
_MAX_QUOTED = 160
_MAX_ROW_WARNINGS = 5


def _clip(text) -> str:
    text = str(text)
    return text if len(text) <= _MAX_QUOTED else text[:_MAX_QUOTED] + '...'


class _SkippedRows:
    """The malformed rows one `zfs list` / `zfs get` parse skipped. The first `_MAX_ROW_WARNINGS` are logged one by
    one, then `done()` adds a single summary line with the total. Kept per listing (never per process), so a row that
    keeps being malformed is reported on every listing that meets it."""

    def __init__(self, command: str):
        self.command = command
        self.count = 0
        # Every row the parse refused, logged one by one or not (`count` only counts what `skip` handled, for the
        # summary line), and why the first one was; `list(strict=True)` raises on a non-zero total.
        self.total = 0
        self.first_problem = None
        # The names zfs printed that libzfseasy cannot represent (reported once per name by `_skip_invalid_name`);
        # `done()` adds their count to the listing's summary.
        self.unrepresentable_names = 0
        # Whether this listing logged a per-name WARNING (see `_skip_invalid_name`), which decides the summary's level.
        self._name_warned = False

    def _remember(self, reason: str, row=None) -> None:
        self.total += 1
        if self.first_problem is None:
            self.first_problem = reason if row is None else '%s: %r' % (reason, _clip(_redact(row)))

    def untrusted(self, reason: str, row=None) -> None:
        """Count a row that was skipped (and reported) elsewhere, without logging it again."""
        self._remember(reason, row)

    def unrepresentable(self, name: str, error: Exception, row=None) -> None:
        """Report (once per name) and count a name zfs allows but libzfseasy cannot represent. It counts toward
        `total`, like `untrusted`, and toward the "cannot represent" count in the summary of `done()`."""
        if _skip_invalid_name(name, error):
            self._name_warned = True
        self.count_unrepresentable(name, error, row)

    def count_unrepresentable(self, name: str, error=None, row=None, warned: bool = False) -> None:
        """Count a name libzfseasy cannot represent that was already handled by `_skip_invalid_name`; `warned` says
        whether that call logged a WARNING."""
        if warned:
            self._name_warned = True
        self.unrepresentable_names += 1
        self._remember('libzfseasy cannot represent the name' + ('' if error is None else ' (%s)' % _clip(error)),
                       row if row is not None else name)

    def skip(self, reason: str, row=None) -> None:
        self._remember(reason, row)
        self.count += 1
        if self.count <= _MAX_ROW_WARNINGS:
            if row is None:
                logger.warning('skipping a row of zfs %s output: %s', self.command, reason)
            else:
                logger.warning('skipping a row of zfs %s output: %s: %r', self.command, reason, _clip(_redact(row)))

    def done(self) -> None:
        if self.count > _MAX_ROW_WARNINGS:
            logger.warning('skipped %d malformed rows of zfs %s output in total (the first %d are logged above)',
                           self.count, self.command, _MAX_ROW_WARNINGS)
        if self.unrepresentable_names:
            # A name that persists is listed every cycle: WARNING only when this listing also logged a per-name
            # WARNING (a new name, or one whose dedup TTL expired), otherwise DEBUG, so the hourly dedup holds.
            logger.log(logging.WARNING if self._name_warned else logging.DEBUG,
                       'skipped %d name%s libzfseasy cannot represent in zfs %s output',
                       self.unrepresentable_names, '' if self.unrepresentable_names == 1 else 's', self.command)


# The object types a row of `zfs list` / `zfs get` output can have.
_ROW_TYPES = ('filesystem', 'volume', 'snapshot', 'bookmark')


def _wanted_types(types) -> Optional[set]:
    """The set of row types a `-t` argument (str or list, already validated) admits, or None for any."""
    if not types:
        return None
    if isinstance(types, str):
        types = types.split(',')
    wanted = {str(t).lower() for t in types}
    return None if 'all' in wanted else wanted


def _type_problem(name: str, dstype: str, wanted: Optional[set]) -> Optional[str]:
    """Why a row's type cannot be right (unknown, not requested, or not what the name's shape says), or None."""
    if dstype not in _ROW_TYPES:
        return 'unknown type ' + repr(_clip(dstype))
    if wanted is not None and dstype not in wanted:
        return 'type ' + dstype + ' was not requested'
    shape = 'snapshot' if '@' in name else 'bookmark' if '#' in name else None
    if (shape or 'dataset') != ('dataset' if dstype in ('filesystem', 'volume') else dstype):
        return 'name does not match type ' + dstype
    return None


class ZFSOutputError(Exception):
    """zfs output could not be trusted (`zfs.get.rows(..., strict=True)`). Deliberately a plain `Exception` subclass,
    like every other zfs failure of this library: not a `ValueError` (that means a bad argument) and not an `OSError`."""


class GetRow(NamedTuple):
    """One row of `zfs get -H`, exactly as zfs printed it: `value` and `received` are raw (`-` is not mapped to
    None; invalid UTF-8 is kept through surrogateescape); `received` is None when the call asked for no `received`
    column."""
    name: str
    property: str
    value: str
    received: Optional[str]
    source: str


class GetRows(list):
    """The rows `zfs.get.rows()` could trust (a list of `GetRow`), with what it could not:

    complete -- True when nothing at all was refused or missing. Every safety decision (a protect set, a prune)
        must use this, or `strict=True`: an object's rows may be incomplete without anything naming the object.
    dropped -- frozenset of the names of the objects whose rows were refused as a whole (and, with `verify_names`,
        of the objects `zfs list` printed that `zfs get` did not). It only names what it can: a stray fragment of a
        forged value identifies no object, so `dropped` can be empty while `complete` is False. Use it to target
        a re-read, never to decide that nothing was lost.
    skipped -- how many rows / objects were refused in all, named or not."""

    def __init__(self, rows=(), skipped: int = 0, dropped=frozenset()):
        super().__init__(rows)
        self.skipped = int(skipped)
        self.dropped = frozenset(dropped)

    @property
    def complete(self) -> bool:
        return self.skipped == 0


class PropertySource(NamedTuple):
    """One row of `zfs.get.property_sources()`: an object, one of its properties and where that property's value comes
    from, exactly as zfs printed them. There is no value in it, by design. `source` is `local`, `default`, `received`,
    `temporary`, `inherited from <dataset>` or `-` (the property does not apply, or is a read-only one)."""
    name: str
    property: str
    source: str


# The sources `zfs get` prints besides `inherited from <dataset>`.
_PLAIN_SOURCES = frozenset(('local', 'default', 'received', 'temporary', '-'))
_INHERITED = 'inherited from '


def _plain_name_problem(name: str) -> Optional[str]:
    """Why `name` cannot be an object name zfs printed, or None. Deliberately only the *shape* (non-empty, no control
    character, no byte that was not valid UTF-8, within zfs's 255-byte limit), not `Validate.dataset`: a name zfs allows
    but libzfseasy cannot represent must still be seen by a caller that is deciding what is safe to keep."""
    if not name:
        return 'empty name'
    if any(ord(c) < 0x20 or ord(c) == 0x7f or 0xd800 <= ord(c) <= 0xdfff for c in name):
        return 'a control character or an invalid byte in a name'
    if len(name.encode('utf-8', 'surrogateescape')) > Validate.max_name_len:
        return 'a name longer than %d bytes' % Validate.max_name_len
    return None


class _Group:
    """The rows of one object in `zfs get -H` output (see `GetCommand._groups`)."""
    __slots__ = ('name', 'rows', 'bad', 'dstype')

    def __init__(self, name: str):
        self.name = name
        self.rows = []      # (property, value, received, source) in the order zfs printed them
        self.bad = False    # refused as a whole (already reported)
        self.dstype = None  # the value of the `type` row


class _Scope:
    """The names a `zfs list` / `zfs get` run over `roots` (with `-r` / `-d depth`) can print. A row naming anything
    else did not come from zfs's own iteration (a value that contains a newline forged it) and is skipped.

    Deliberately a little wider than zfs (never narrower): without `-r`/`-d`, the snapshots and bookmarks directly
    under a root dataset are in scope too, because `zfs list -t snapshot <dataset>` lists them."""

    def __init__(self, roots: Iterable[str], recursive: bool = False, depth: int = 0):
        self.roots = [str(r) for r in roots]
        self.recursive = bool(recursive)
        self.depth = depth if depth and depth > 0 else 0

    @staticmethod
    def _split(name: str):
        for sep in ('@', '#'):
            if sep in name:
                ds, _, short = name.partition(sep)
                return ds, sep, short
        return name, '', ''

    def _within(self, level: int) -> bool:
        return self.recursive or (self.depth > 0 and level <= self.depth)

    def __call__(self, name: str) -> bool:
        ds, sep, short = self._split(name)
        extra = 1 if sep else 0
        if not self.roots:
            # zfs iterates every pool: only `-d` limits the depth (counted from the pool).
            return self.depth == 0 or self.recursive or ds.count('/') + extra <= self.depth
        for root in self.roots:
            if name == root:
                return True
            rds, rsep, rshort = self._split(root)
            if ds == rds:
                level = 0
            elif ds.startswith(rds + '/'):
                level = ds[len(rds) + 1:].count('/') + 1
            else:
                continue
            if rsep:
                # A snapshot / bookmark root: the same short name on a descendant, with -r / -d only.
                if sep == rsep and short == rshort and level > 0 and self._within(level):
                    return True
                continue
            level += extra
            if self._within(level) or (level == 1 and sep and ds == rds):
                return True
        return False


# How long to give an abandoned child process to honor SIGTERM before
# escalating to SIGKILL when reaping it in Command._exec_lines's finally block.
_TERMINATE_TIMEOUT = 5


# ---------------------------------------------------------------------------------------------------------------------
# Slow-command watchdog (S2a) and opt-in timeouts (S2b)
# ---------------------------------------------------------------------------------------------------------------------

# Seconds after which a command that is still running is reported at WARNING on the 'libzfseasy' logger, and again at
# each further multiple. One rule for this module and for `libzfseasy.slow_command_warning` (the package forwards the
# attribute here; `del libzfseasy.slow_command_warning` restores the default): a positive finite number turns the
# warning on; None, zero, a negative number or infinity turn it off; anything else (a bool, a string, NaN) is invalid,
# which the package setter refuses with ValueError and a read here treats as off (a bad setting must never fail a
# command). Read when each command starts, so a change applies to the commands that start afterwards. Behaviour is
# otherwise unchanged: the watchdog only logs.
DEFAULT_SLOW_COMMAND_WARNING = 60
slow_command_warning = DEFAULT_SLOW_COMMAND_WARNING

# Bytes asked of a pipe per `os.read` when reading under a timeout.
_READ_SIZE = 65536
# After the deadline of a timed call: how long to wait for the child after SIGTERM, and again after SIGKILL.
_EXPIRY_WAIT = 2
# After the child of a timed-out call is gone: how long to wait for its stderr reader before leaving it to finish by
# itself (a grandchild may hold stderr open; the reader is a daemon thread and keeps its pipe alive until it ends).
_EXPIRY_JOIN = 0.05
# How long `_SlowCommandWatchdog.cancel` waits for the watchdog thread to return.
_WATCHDOG_JOIN = 0.1


class ZFSTimeoutError(Exception):
    """A command run under `timeout()` did not finish in time, or was refused because an earlier timed-out child is
    still alive. A plain `Exception` (not `OSError`, `ValueError` or the built-in `TimeoutError`). The outcome of a
    command that timed out is unknown: a write may or may not have happened."""


_timeout_var = contextvars.ContextVar('libzfseasy_timeout', default=None)

_Thread = threading.Thread
_Event = threading.Event


def _check_seconds(value, what: str):
    """`value` as a float if it is a positive, finite real number (not a bool); `ValueError` otherwise."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f'{what} must be a positive, finite number of seconds, got {value!r}')
    return float(value)


@contextlib.contextmanager
def timeout(seconds):
    """Bound every non-streaming command started inside the block to `seconds` of wall-clock time each.

    `with libzfseasy.timeout(30): libzfseasy.list(...)`. The limit is held in a `contextvars.ContextVar`, so it
    follows the calling thread (or asyncio task) only and applies to each command separately. It is read when a command
    starts running: for the generators behind `list`/`get` that is the first `next()`, in the context of whoever calls
    it, not where the call was made. Nested blocks: the innermost wins; `timeout(None)` lifts the limit inside an outer
    block. No command signature changes.

    Context caveats: a thread started inside the block normally begins with an empty context (no limit), but a
    context-copying start (free-threaded builds' default, `asyncio.to_thread`, `contextvars.copy_context().run`)
    inherits it. Do not put a `with timeout()` around a `yield` (in a generator or an async function): the limit
    would apply to whoever resumes it.

    A command that is not finished in time has its child terminated (SIGTERM, then SIGKILL after 2 s, each waited for
    at most 2 s) and `ZFSTimeoutError` is raised; the outcome of the command is unknown. A child that survives SIGKILL
    (uninterruptible sleep, typically a hung pool) cannot be freed: it stays registered, and every later call made
    under a timeout raises `ZFSTimeoutError` at once, without starting a child, until it has exited. Calls made
    without a timeout are never refused. A timeout frees the calling thread, not the stuck process;
    `libzfseasy.stuck_children()` reports such processes (and reaps the ones that have since exited).

    `send` and `receive` (and their `StreamHandle`s) ignore the timeout, as does `send.resume_info`. A lazy iterator
    (`lazy=True`) spends its budget from the first row request until its last row, including the time the consumer
    holds it between rows.

    Keyword arguments:
        seconds: float | None -- seconds per command, positive and finite; None for no limit

    Raises:
        ValueError -- `seconds` is not None and not a positive finite number
    """
    if seconds is not None:
        seconds = _check_seconds(seconds, 'timeout')
    token = _timeout_var.set(seconds)
    try:
        yield
    finally:
        _timeout_var.reset(token)


def _slow_setting_valid(value) -> bool:
    """Whether `value` is an acceptable `slow_command_warning`: None or a real number other than NaN (see the comment
    at the setting)."""
    return value is None or (isinstance(value, (int, float)) and not isinstance(value, bool) and not math.isnan(value))


def _slow_interval() -> Optional[float]:
    """The slow-command warning interval in effect now, or None when the warning is off or the setting is invalid."""
    value = slow_command_warning
    if not _slow_setting_valid(value) or value is None or value <= 0 or math.isinf(value):
        return None
    return float(value)


class _SlowCommandWatchdog:
    """Logs a WARNING for a command that is still running, every `interval` seconds, until cancelled.

    One daemon thread per watched call, parked on an `Event` (a Timer would need re-creating at every multiple). The
    thread holds no lock shared with other calls and no module state, so it has nothing to repair after `fork()`: the
    child simply has no such thread, and a `cancel()` there only sets the event. Starting it may fail (thread limit);
    the command then runs unwatched. `cancel()` joins for at most `_WATCHDOG_JOIN`, so it cannot block on a stuck logging handler."""

    def __init__(self, cmd, pid, interval: float):
        self._cmd = cmd
        self._pid = pid
        self._interval = interval
        self._started = time.monotonic()
        self._stop = _Event()
        self._thread = None

    @classmethod
    def for_command(cls, cmd, pid) -> Optional['_SlowCommandWatchdog']:
        """A watchdog for the child `pid` of argv `cmd` (not started), or None when the warning is off. The argv is
        redacted only when a warning is actually logged."""
        interval = _slow_interval()
        return None if interval is None else cls(cmd, pid, interval)

    def start(self) -> None:
        try:
            thread = _Thread(target=self._run, name='libzfseasy-watchdog', daemon=True)
            thread.start()
        except Exception:
            return
        self._thread = thread

    def cancel(self) -> None:
        """Stop the watchdog. The thread is joined for at most `_WATCHDOG_JOIN` (it only has to wake and return, so
        normally microseconds), so that no watchdog thread outlives the call it watched; a thread stuck in a logging
        handler is left behind rather than waited for."""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=_WATCHDOG_JOIN)

    def _run(self) -> None:
        try:
            n = 1
            while not self._stop.wait(max(0.0, self._started + n * self._interval - time.monotonic())):
                if self._stop.is_set():
                    break  # cancelled between the wake-up and now
                try:
                    label = _command_label(self._cmd)
                except Exception:
                    label = 'command'
                now = time.monotonic()
                logger.warning('%s (pid %s) is still running after %.1f s (slow-command warning every %g s)',
                               label, self._pid, now - self._started, self._interval)
                # After a stall (suspended process, clock jump, or the log call itself blocking on a slow handler) skip
                # the multiples already passed: no burst. Read the clock again, as the warning above may have taken long.
                n = max(n + 1, int((time.monotonic() - self._started) / self._interval) + 1)
        except Exception:
            pass


class _DeadlineReached(Exception):
    """Internal: a timed read ran out of time. Never leaves `_exec_lines`."""


class _StuckChild:
    """A child that a timed-out call could not get rid of (it survived SIGKILL). Kept in `_stuck` until it exits."""

    def __init__(self, process, stderr_thread, label: str):
        self.process = process
        self.stderr_thread = stderr_thread
        self.label = label
        self.since = time.monotonic()

    def release(self) -> None:
        """The child has exited: close what this thread may close. stdout was closed at registration (this thread was
        its only reader); stderr only once its reader has ended, as closing a pipe a thread is blocked on risks fd
        reuse."""
        StreamHandle._close_pipe(getattr(self.process, 'stdout', None))
        thread = self.stderr_thread
        if thread is None or not thread.is_alive():
            StreamHandle._close_pipe(getattr(self.process, 'stderr', None))


# Children that outlived a timeout plus SIGTERM plus SIGKILL. Guarded by `_stuck_lock`; both are replaced/emptied in a
# forked child (the children are not the child's own, and the lock may have been held by a thread that does not exist
# there).
_stuck = []
_stuck_lock = threading.Lock()


def _register_stuck(process, stderr_thread, label: str) -> None:
    with _stuck_lock:
        _stuck.append(_StuckChild(process, stderr_thread, label))


def _sweep_stuck() -> list:
    """Poll the registered children, release the ones that exited and drop them; return the ones still alive."""
    with _stuck_lock:
        alive = []
        for entry in _stuck:
            try:
                running = entry.process.poll() is None
            except Exception:
                running = False
            if running:
                alive.append(entry)
            else:
                entry.release()
        _stuck[:] = alive
        return list(alive)


class StuckChild(NamedTuple):
    """A snapshot of one child that survived a timeout's SIGTERM and SIGKILL (see `stuck_children`). Plain data: no
    process object, so holding it keeps nothing alive. `since` is a `time.monotonic()` value (when it was registered)."""
    pid: int
    label: str
    since: float


def stuck_children() -> Tuple[StuckChild, ...]:
    """The children of this process that a timed-out call could not get rid of (they survived SIGKILL, typically in
    uninterruptible sleep on a hung pool) and that are still running, oldest first.

    Cheap, thread-safe and never blocks: it takes the registry lock, polls each registered child without waiting
    (`poll()`, a non-blocking `waitpid`), releases and drops the ones that have exited (so a recovered pool clears the
    registry here, not only at the next timed call) and returns a snapshot. An empty tuple means no earlier timed-out
    command can still be running in this process, so a caller that must not overlap a new zfs mutation with an
    abandoned one polls this until it is empty. The registry is per process: it is empty in a forked child and lost
    when the process exits, which is why a process replacing a worker has to wait for this before it lets go.
    """
    return tuple(StuckChild(getattr(e.process, 'pid', -1), e.label, e.since) for e in _sweep_stuck())


def _admit_timed(cmd) -> None:
    """Admission for a call under a timeout: sweep `_stuck`; if a timed-out child is still alive, raise
    `ZFSTimeoutError` before any child is started."""
    alive = _sweep_stuck()
    if alive:
        pids = ', '.join(str(getattr(e.process, 'pid', '?')) for e in alive)
        raise ZFSTimeoutError(f'{_command_label(cmd)} not started: {len(alive)} earlier command(s) that timed out are '
                              f'still running (pid {pids}); zfs is not answering')


def _reset_stuck_after_fork() -> None:
    global _stuck_lock
    _stuck_lock = threading.Lock()
    _stuck.clear()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_reset_stuck_after_fork)

# Limits on the stderr a streaming child can make us keep: at most this many lines from the start and from the end
# (the middle is replaced by one marker line), each at most this many bytes.
_STDERR_KEEP_LINES = 200
_STDERR_MAX_LINE = 64 * 1024


_URL_USERINFO = re.compile(r'([A-Za-z][A-Za-z0-9+.\-]*://)[^/\s]*@')
# A `scheme://userinfo` cut off by the clip below, before its `@` could be seen.
_URL_USERINFO_CUT = re.compile(r'([A-Za-z][A-Za-z0-9+.\-]*://)[^/\s]*\Z')

# `_redact` looks at no more than this many characters (the userinfo pattern is quadratic in the length of a run of
# scheme characters). Far above any real argument or logged row, so those come out unchanged.
_MAX_REDACT = 2048


def _redact(arg) -> str:
    """Hide URL credentials (`scheme://user:pass@host` -> `scheme://***@host`) wherever they occur in an argument,
    e.g. inside `-o keylocation=https://user:pass@host/key`.

    An argument longer than `_MAX_REDACT` characters is clipped to that many (plus a `...` marker) before it is
    scanned, so a huge input cannot burn CPU; a credential cut by the clip is hidden as well."""
    text = str(arg)
    clipped = len(text) > _MAX_REDACT
    if clipped:
        text = text[:_MAX_REDACT]
    text = _URL_USERINFO.sub(r'\1***@', text)
    if clipped:
        text = _URL_USERINFO_CUT.sub(r'\1***', text) + '...'
    return text


def _log_command(cmd) -> None:
    """Log the command about to run at DEBUG, with URL credentials redacted. Nothing is formatted unless DEBUG is
    enabled on the 'libzfseasy' logger."""
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('Executing command: %s', shlex.join([_redact(a) for a in cmd]))


def _command_label(cmd) -> str:
    """The first words of an argv for a log message: the binary's base name and its subcommand, redacted and clipped."""
    try:
        words = [os.path.basename(str(cmd[0]))] + [str(a) for a in cmd[1:2]]
    except (TypeError, IndexError):
        return 'command'
    return shlex.join([_redact(w) for w in words])


def _check_values(properties) -> None:
    """Refuse (`Validate.value`) any value in a `{name: value}` mapping or `(name, value)` iterable that zfs output
    could not frame, before the command that would pass it to zfs is built. Values are checked as `str(value)`, the
    form every command puts into argv."""
    if not properties:
        return
    items = properties.items() if isinstance(properties, dict) else properties
    for k, v in items:
        Validate.value(str(v), k)


class _BoundedErrors(list):
    """A list of error lines that keeps the first and the last `_STDERR_KEEP_LINES` entries and collapses the
    middle into a single `... N lines omitted ...` marker, so a pathological stderr cannot grow memory without
    bound. Appended to by the stderr reader thread. It is read after the thread was joined; if the bounded join timed
    out (a stray holder of the pipe), the reader may still be appending, which is harmless for a read that only
    formats the lines collected so far."""

    def __init__(self):
        super().__init__()
        self._omitted = 0

    def append(self, line):
        keep = _STDERR_KEEP_LINES
        if self._omitted == 0 and len(self) < 2 * keep:
            super().append(line)
            return
        if self._omitted == 0:
            self.insert(keep, '')  # the marker slot, between the head and the tail
        super().append(line)
        del self[keep + 1]  # drop the oldest line of the tail
        self._omitted += 1
        self[keep] = '... %d lines omitted ...' % self._omitted


def _append_error(errors, line):
    """Append one stripped, non-empty stderr line to `errors`. Only the first line of a message is capitalised
    (`cannot destroy ...` -> `Cannot destroy ...`): continuation lines carry dataset names and other verbatim text
    (`pool/clone3`) that must stay byte-exact."""
    errors.append(line[0].upper() + line[1:] if not errors else line)


def _child_env() -> dict:
    """The environment every `zfs`/`zpool` child runs in: the caller's, with the C locale forced.

    zfs translates its messages and prints values by locale, and callers (`exists()`, the destroy/receive error
    handling in zfsbackup) match on the English text. `LC_ALL` beats every other locale variable, so a caller's own
    `LC_ALL`/`LANG` is deliberately overridden for the child; the rest of the environment is passed through."""
    return dict(os.environ, LC_ALL='C', LANG='C')


def _failure_text(errors, rc) -> str:
    """The text of a failed child's exception: the stderr lines joined by newlines. Only when stderr was empty,
    a description of the exit status instead: `Command failed with exit code N`, or `Command killed by signal
    <NAME>` when the child died of a signal (a negative return code, e.g. SIGPIPE)."""
    text = '\n'.join(errors)
    if text:
        return text
    if isinstance(rc, int) and not isinstance(rc, bool) and rc < 0:
        try:
            name = signal.Signals(-rc).name
        except ValueError:
            name = str(-rc)
        return f'Command killed by signal {name}'
    return f'Command failed with exit code {rc}'


class Command:

    @classmethod
    def _exec(cls, cmd):
        """Run `cmd` to completion for its side effect, discarding stdout. Built on `_exec_lines`, so it shares its
        bytes/surrogateescape reading, bounded stderr and cleanup: if the caller is interrupted (any `BaseException`,
        `KeyboardInterrupt` included) while the child runs, the child is terminated (SIGTERM, SIGKILL after
        `_TERMINATE_TIMEOUT`) and reaped before the exception propagates. Raises the bare subprocess-error Exception
        on a non-zero exit."""
        lines = cls._exec_lines(cmd)
        try:
            for _ in lines:
                pass
        finally:
            # Close explicitly: a traceback would otherwise keep the suspended generator (and its child) alive.
            lines.close()

    @classmethod
    def _exec_out(cls, cmd):
        """Run `cmd` and yield its stdout lines stripped, skipping empty ones (zfsbackup calls this directly). A
        layer over `_exec_lines`: only `\\n` ends a line (`\\r` stays inside it), and an abandoned generator reaps
        the child. Raises the bare subprocess-error Exception on a non-zero exit, after the last line."""
        lines = cls._exec_lines(cmd)
        try:
            for line in lines:
                line = line.strip()
                if line:
                    yield line
        finally:
            lines.close()

    @classmethod
    def _exec_lines(cls, cmd):
        """Run `cmd` and yield its stdout lines exactly as printed, for parsers that must frame `-H` output: only the
        line's `\\n` is removed (a trailing empty column and trailing spaces in a value survive, `\\r` is not a line
        break) and blank lines are yielded too. Stdout is read as bytes and decoded as UTF-8 with `surrogateescape`, so
        an invalid byte in a value can neither raise nor be lost. Raises the bare subprocess-error Exception (stderr
        lines) on a non-zero exit, after the last line. An abandoned generator, or any exception raised into it (an
        interrupt), terminates and reaps the child (see `_TERMINATE_TIMEOUT`). This is the one exec core: `_exec` and
        `_exec_out` are built on it.

        Unlike `_exec_out` (which strips and drops empty lines) nothing is stripped or dropped.

        Two opt-ins act here, both read when the generator first runs: a still-running child is reported by the
        slow-command watchdog (`slow_command_warning`), and under `timeout()` stdout is read against a deadline
        (`_read_until`) and an expired call ends in `_expire` and `ZFSTimeoutError`. Without a timeout the path is the
        plain `readline` loop."""
        _log_command(cmd)
        seconds = _timeout_var.get()
        if seconds is not None:
            _admit_timed(cmd)
        started = time.monotonic()
        errors = _BoundedErrors()
        process = None
        stderr_thread = None
        watchdog = None
        deadline = None if seconds is None else started + seconds
        timed = seconds is not None  # False again below if the pipe cannot be read against a deadline (a mock)
        completed = False
        settled = False
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=_child_env())
            stderr_thread = cls._start_stderr_reader_lines(process, errors)
            watchdog = _SlowCommandWatchdog.for_command(cmd, getattr(process, 'pid', None))
            if watchdog is not None:
                watchdog.start()
            fd = cls._timed_fd(process) if timed else None
            if fd is None:
                timed = False
                while True:
                    line = process.stdout.readline()
                    rc = process.poll()
                    if isinstance(line, (bytes, bytearray)):
                        line = bytes(line).decode('utf-8', errors='surrogateescape')
                    if not isinstance(line, str) or line == '':
                        break
                    yield line[:-1] if line.endswith('\n') else line
                # stdout hit EOF: block until the child exits (never spin), then collect the whole of stderr.
                if rc is None:
                    rc = process.wait()
            else:
                try:
                    rc = yield from cls._read_until(process, fd, deadline)
                except _DeadlineReached:
                    error = cls._expire(process, stderr_thread, cmd, seconds)
                    settled = True  # the child is reaped or registered; the cleanup below must not repeat it
                    raise error from None
            if timed:
                # The budget bounds this wait too (the child has exited; only a grandchild holding stderr can stall it).
                cls._join_stderr_reader(stderr_thread, min(_TERMINATE_TIMEOUT, max(0.0, deadline - time.monotonic())))
            else:
                cls._join_stderr_reader(stderr_thread)
            completed = True
        finally:
            # The child first, the watchdog after: cancelling joins the watchdog's thread, and a second interrupt
            # arriving during that join must not skip the SIGTERM/SIGKILL below.
            try:
                if not completed and not settled and process is not None and timed:
                    # Any exit other than the deadline (a consumer that raised, close() or GC of the generator, an
                    # interrupt): the same bounded sequence as an expiry, ending in the registry if the child survives
                    # SIGKILL. The original exception propagates afterwards.
                    cls._reap_timed(process, stderr_thread, cmd)
                elif not completed and not settled and process is not None:
                    abandoned = False
                    try:
                        if process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=_TERMINATE_TIMEOUT)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                try:
                                    process.wait(timeout=_TERMINATE_TIMEOUT)
                                except subprocess.TimeoutExpired:
                                    # SIGKILL did not take effect: the child is in uninterruptible sleep (typically
                                    # a hung pool). Never block the caller on it; Popen reaps it later when it exits.
                                    abandoned = True
                                    logger.warning('%s (pid %s) did not exit after SIGKILL (uninterruptible, likely a '
                                                   'hung pool); abandoning it', _command_label(cmd), process.pid)
                    except BaseException:
                        # A further interrupt arrived while waiting: do not leave a SIGTERM-ignoring child running.
                        try:
                            process.kill()
                        except OSError:
                            pass
                        raise
                    if not abandoned:
                        # (An abandoned child still holds stderr open, so the join could only time out.)
                        cls._join_stderr_reader(stderr_thread)
                        process.wait()
            finally:
                if watchdog is not None:
                    watchdog.cancel()
        if rc != 0:
            raise Exception(_failure_text(errors, rc))

    @staticmethod
    def _timed_fd(process) -> Optional[int]:
        """The stdout file descriptor to read under a timeout, or None when the process has no real one (a mocked
        pipe), in which case the call reads with `readline` and the timeout cannot apply."""
        try:
            fd = process.stdout.fileno()
        except (AttributeError, OSError, ValueError):
            return None
        return fd if isinstance(fd, int) and not isinstance(fd, bool) else None

    @staticmethod
    def _read_until(process, fd: int, deadline: float):
        """Generator behind `_exec_lines` under a timeout: yield the lines of the child's stdout (same framing and
        decoding as the `readline` path) reading with `selectors` and `os.read` so the wait can end at `deadline` (a
        monotonic time), then wait for the child to exit, also until `deadline`. Returns its return code. Raises
        `_DeadlineReached` only if the child is still running at the deadline; the selector is closed on every way
        out. Bypasses the `process.stdout` buffer entirely, so nothing that is read can be lost in it.

        A child that has already exited when the deadline passes is not a timeout: what is already in the pipe is
        read without blocking and the call completes normally (if a grandchild still holds the pipe open, stdout is
        closed here, this being its only reader)."""
        selector = selectors.DefaultSelector()
        held = False
        try:
            selector.register(fd, selectors.EVENT_READ)
            pending = b''
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if process.poll() is None:
                        raise _DeadlineReached()
                    remaining = 0.0  # finished: take what is already there, never block
                if not selector.select(remaining):
                    if remaining > 0:
                        continue  # the deadline is checked at the top
                    held = True  # nothing more to read, yet no EOF: someone else holds the write end
                    break
                chunk = os.read(fd, _READ_SIZE)
                if not chunk:
                    break
                pending += chunk
                start = 0
                while True:
                    end = pending.find(b'\n', start)
                    if end < 0:
                        break
                    yield pending[start:end].decode('utf-8', errors='surrogateescape')
                    start = end + 1
                pending = pending[start:]
            if pending:
                yield pending.decode('utf-8', errors='surrogateescape')
            rc = process.poll()
            if rc is None:
                try:
                    rc = process.wait(timeout=max(0.0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    rc = process.poll()
                    if rc is None:
                        raise _DeadlineReached() from None
            return rc
        finally:
            selector.close()
            if held:
                StreamHandle._close_pipe(process.stdout)

    @classmethod
    def _reap_timed(cls, process, stderr_thread, cmd) -> bool:
        """Get rid of the child of a timed call that ends early. Returns True if the child could not be killed.

        If the child is running: SIGTERM, wait up to `_EXPIRY_WAIT`; SIGKILL, wait up to `_EXPIRY_WAIT`. A child that
        is still alive then is registered in `_stuck` (it is not ours to wait for any longer) and a WARNING is logged.
        stdout is closed either way: this thread is its only reader. stderr is never closed while its reader may be
        blocked on it: for a reaped child the reader is given `_EXPIRY_JOIN` and the pipe is closed only if it ended;
        a stuck child's stderr stays open (closing a busy fd risks fd reuse) until a sweep finds the child gone. A
        further interrupt while waiting kills the child and propagates."""
        stuck = False
        try:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=_EXPIRY_WAIT)
                except subprocess.TimeoutExpired:
                    process.kill()
                    try:
                        process.wait(timeout=_EXPIRY_WAIT)
                    except subprocess.TimeoutExpired:
                        stuck = True
        except BaseException:
            try:
                process.kill()
            except OSError:
                pass
            raise
        if stuck:
            label = _command_label(cmd)
            _register_stuck(process, stderr_thread, label)
            logger.warning('%s (pid %s) did not exit after SIGTERM and SIGKILL (uninterruptible, likely a hung pool); '
                           'calls under a timeout are refused until it exits', label, process.pid)
        StreamHandle._close_pipe(process.stdout)
        if not stuck:
            if stderr_thread is not None:
                stderr_thread.join(timeout=_EXPIRY_JOIN)
            if stderr_thread is None or not stderr_thread.is_alive():
                StreamHandle._close_pipe(process.stderr)
        return stuck

    @classmethod
    def _expire(cls, process, stderr_thread, cmd, seconds) -> 'ZFSTimeoutError':
        """A timed call ran out of time with its child still running: reap it (`_reap_timed`) and build the
        `ZFSTimeoutError` to raise."""
        stuck = cls._reap_timed(process, stderr_thread, cmd)
        message = f'{_command_label(cmd)} timed out after {seconds:g}s; outcome unknown'
        if stuck:
            message += f' (pid {process.pid} did not exit after SIGKILL)'
        return ZFSTimeoutError(message)

    @staticmethod
    def _start_stderr_reader_lines(process, errors):
        """The stderr reader for a pipe that may be binary (decoded as UTF-8 with replacement, so an odd byte
        cannot kill the reader and leave the child blocked on a full pipe) or text (a mocked process). Lines are
        read at most `_STDERR_MAX_LINE` at a time; anything that is neither ends the thread quietly."""
        def reader():
            try:
                while True:
                    line = process.stderr.readline(_STDERR_MAX_LINE)
                    if isinstance(line, (bytes, bytearray)):
                        line = bytes(line).decode('utf-8', errors='replace')
                    if not isinstance(line, str) or line == '':
                        break
                    error = line.strip()
                    if error:
                        _append_error(errors, error)
            except (OSError, ValueError):
                # The pipe was closed under us (reaped); whatever was read so far stands.
                pass
        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        return thread

    @classmethod
    def _exec_collect(cls, cmd, raw_stderr: bool = False, env=None):
        """Run `cmd` to completion and return `(stdout_lines, error_lines, returncode)` without raising on a
        non-zero exit, for commands that print something useful and then fail (e.g. `zfs send -nv -t`). Stdout
        lines are right-stripped of the newline only. By default stderr lines are stripped, empty ones dropped and
        the first letter capitalised, like the other helpers; with `raw_stderr` they are returned exactly as
        printed (newline removed, indentation kept) for callers that parse structured stderr. The child is always
        reaped and the stderr thread joined (bounded, see `_join_stderr_reader`), even if the caller is interrupted.
        `env` replaces the child's environment when given (default: `_child_env()`, the C locale). Output is decoded as UTF-8 with
        replacement, so odd bytes can neither raise nor kill the stderr reader (which would leave the child blocked on a full pipe)."""
        _log_command(cmd)
        errors = _BoundedErrors()
        process = None
        stderr_thread = None
        lines = []
        watchdog = None
        completed = False
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                                       encoding='utf-8', errors='replace',
                                       env=_child_env() if env is None else env)
            if raw_stderr:
                stderr = process.stderr

                def reader():
                    try:
                        while True:
                            line = stderr.readline()
                            if line == '':
                                break
                            errors.append(line.rstrip('\r\n'))
                    except (OSError, ValueError):
                        # The pipe broke or was closed under us; whatever was read so far stands.
                        pass
                thread = threading.Thread(target=reader, daemon=True)
                thread.start()
                stderr_thread = thread  # only a started thread may be joined by the cleanup
            else:
                stderr_thread = cls._start_stderr_reader_lines(process, errors)
            watchdog = _SlowCommandWatchdog.for_command(cmd, getattr(process, 'pid', None))
            if watchdog is not None:
                watchdog.start()
            while True:
                line = process.stdout.readline()
                if line == '':
                    break
                lines.append(line.rstrip('\n'))
            rc = process.wait()
            cls._join_stderr_reader(stderr_thread)
            completed = True
        finally:
            try:  # the child before the watchdog, as in `_exec_lines`
                if not completed and process is not None:
                    if process.poll() is None:
                        process.kill()
                        process.wait()
                    cls._join_stderr_reader(stderr_thread)
            finally:
                if watchdog is not None:
                    watchdog.cancel()
        return lines, list(errors), rc

    @staticmethod
    def _start_stderr_reader_bin(process, errors):
        """Drain a binary stderr pipe on a daemon thread, appending each non-empty line (decoded leniently, first
        letter capitalised) to `errors`. Used by the streaming commands, whose stdout/stdin the caller owns, so a
        child that writes a lot of stderr can never block on a full pipe. Returns the thread, or None when the
        process has no stderr pipe. Anything that is not a bytes line (a mocked pipe) ends the thread quietly."""
        stderr = getattr(process, 'stderr', None)
        if not stderr:
            return None

        def reader():
            try:
                while True:
                    line = stderr.readline(_STDERR_MAX_LINE)
                    if not isinstance(line, (bytes, bytearray)) or not line:
                        break
                    truncated = False
                    if len(line) >= _STDERR_MAX_LINE and not line.endswith(b'\n'):
                        # Over-long line: keep its head, consume (and discard) the rest of it.
                        truncated = True
                        while True:
                            rest = stderr.readline(_STDERR_MAX_LINE)
                            if not isinstance(rest, (bytes, bytearray)) or not rest or rest.endswith(b'\n'):
                                break
                    error = bytes(line).decode('utf-8', errors='replace').strip()
                    if error:
                        _append_error(errors, error + (' [truncated]' if truncated else ''))
            except (OSError, ValueError):
                # The pipe was closed under us (abort/reap); whatever was read so far stands.
                pass
        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        return thread

    @staticmethod
    def _join_stderr_reader(thread, bound=None):
        # Bounded (`_TERMINATE_TIMEOUT`, or `bound` if given): a stray grandchild holding the pipe open must not hang
        # the caller (the thread is a daemon).
        # Joined at most once per thread: a second caller (e.g. the abort path after a failed start) must not
        # wait out the timeout again for a reader that already timed out.
        if thread is None or getattr(thread, '_libzfseasy_joined', False) is True:
            return
        thread.join(timeout=_TERMINATE_TIMEOUT if bound is None else bound)
        thread._libzfseasy_joined = True

    @classmethod
    def _abort_stream(cls, process, thread):
        """Reap a streaming child after a failed start (or an interrupt while starting): terminate it if it is
        still running, wait for it, join the stderr reader and close the pipes. Exceptions are swallowed, but the
        pipes are closed (and the child killed) even if a further interrupt arrives mid-cleanup; that interrupt
        then propagates."""
        reaped = False
        try:
            try:
                # A returncode means the child was already reaped (the failure came from its own exit status).
                if getattr(process, 'returncode', None) is None:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=_TERMINATE_TIMEOUT)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                    else:
                        process.wait()
                reaped = True
                cls._join_stderr_reader(thread)
            except Exception:
                pass
        finally:
            # Reached on a second interrupt too: do not leave the child running or the pipes open.
            try:
                if not reaped and getattr(process, 'returncode', None) is None and process.poll() is None:
                    process.kill()
            except Exception:
                pass
            for name in ('stdin', 'stdout', 'stderr'):
                pipe = getattr(process, name, None)
                if pipe is not None and not (thread is not None and thread.is_alive() and name == 'stderr'):
                    StreamHandle._close_pipe(pipe)


class StreamHandle:
    """Wraps the Popen and the binary pipe used for zfs send/receive streaming.

    Proxies attribute access (read/write/close/...) to the underlying
    BufferedReader (send) or BufferedWriter (receive), so existing
    stream-based usage keeps working unchanged. Can also be used as a
    context manager -- `__enter__` returns the wrapped stream and `__exit__`
    calls `close()`.

    Closing the handle (via `close()` or context-manager exit) closes the
    underlying stream, waits for the subprocess to exit, drains any
    remaining stderr, and raises the bare subprocess-error Exception if the
    process exited with a non-zero return code. This must only be done after
    the caller has finished reading (send) or writing (receive) the stream,
    otherwise a full pipe could deadlock the child.

    Keyword arguments:
        process: subprocess.Popen -- the running zfs send/receive process
        stream: io.BufferedReader | io.BufferedWriter -- process.stdout or process.stdin
        errors: list -- stderr lines captured so far (mutated in place, default [])
        stderr_thread: threading.Thread -- the thread appending the child's stderr lines to `errors`; joined by
            `close()` (default None: stderr is read directly in `close()` instead)

    Returns:
        None
    """

    def __init__(self, process, stream, errors: Optional[list] = None, stderr_thread=None):
        self._process = process
        self._stream = stream
        self._errors = errors if errors is not None else []
        self._stderr_thread = stderr_thread
        self._closed = False

    def __enter__(self):
        return self._stream

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def close(self):
        if self._closed:
            return
        # Closing a receive-side stream flushes buffered bytes; if zfs has already exited that raises
        # BrokenPipeError. Remember it, but still reap the process and collect its stderr: the real diagnostic is
        # what the caller needs, and skipping the wait would leave a zombie.
        stream_error = None
        if not getattr(self._stream, 'closed', False):
            try:
                self._stream.close()
            except (OSError, ValueError) as e:
                stream_error = e
        # The wait for the child to finish after the stream was closed is watched (slow-command warning); the transfer
        # itself, which the caller drove, never is.
        watchdog = _SlowCommandWatchdog.for_command(getattr(self._process, 'args', None),
                                                    getattr(self._process, 'pid', None))
        try:
            if watchdog is not None:
                watchdog.start()
            rc = self._process.wait()
        finally:
            if watchdog is not None:
                watchdog.cancel()
        # Only now is the handle done: an interrupt inside wait() leaves it open so a later close() can finish.
        self._closed = True
        reader = self._stderr_thread
        stderr = getattr(self._process, 'stderr', None)
        if reader is not None:
            # The reader ends at stderr EOF, i.e. once the child is gone; join it so `errors` is complete before
            # it is raised. Never close a pipe a (stuck) reader is still blocked on.
            Command._join_stderr_reader(reader)
            if stderr is not None and not reader.is_alive():
                self._close_pipe(stderr)
        elif stderr is not None:
            try:
                remainder = stderr.read()
            except Exception:
                remainder = None
            if remainder:
                for line in remainder.decode('utf-8', errors='replace').splitlines():
                    error = line.strip()
                    if error:
                        _append_error(self._errors, error)
            self._close_pipe(stderr)
        # process.stdout is only ever piped on the send side (the receive
        # side opens it as DEVNULL); if it's a distinct file object from the
        # wrapped stream, close it too so it isn't leaked until GC.
        stdout = getattr(self._process, 'stdout', None)
        if stdout is not None and stdout is not self._stream:
            self._close_pipe(stdout)
        if rc != 0:
            raise Exception(_failure_text(self._errors, rc)) from stream_error
        if stream_error is not None:
            raise stream_error

    @staticmethod
    def _close_pipe(pipe):
        if getattr(pipe, 'closed', False):
            return
        try:
            pipe.close()
        except Exception:
            pass


class PropertyCommand:

    @staticmethod
    def _get_props(properties: Properties, flag: bool = False):
        cmd = []
        if not properties:
            return cmd
        for k, v in properties.items():
            if flag:
                cmd += ['-o', k + '=' + str(v)]
            else:
                cmd += [k + '=' + str(v)]
        return cmd


class StringListArgument:

    @staticmethod
    def _slist_to_list(slist: StringList, separator: str = ',', validator: Optional[Callable[[str], None]] = None):
        if not slist:
            slist = []
        elif isinstance(slist, str):
            slist = str(slist).split(separator)
        else:
            slist = list(slist)  # materialise: a one-shot iterable must survive validating and then using it
        if validator:
            for s in slist:
                validator(s)
        return list(slist)

    @classmethod
    def _slist_to_str(cls, slist: StringList, separator: str = ',', validator: Optional[Callable[[str], None]] = None):
        return separator.join([str(s) for s in cls._slist_to_list(slist, separator, validator)])


class DatasetListArgument:

    @staticmethod
    def _dslist_to_list(dslist: DatasetList):
        if not dslist:
            dslist = []
        elif not isinstance(dslist, Iterable):
            dslist = [dslist]
        dslist = list(dslist)  # materialise: a one-shot iterable must survive validating and then using it
        for ds in dslist:
            if not isinstance(ds, (Filesystem, Volume, Dataset, Snapshot)):
                raise ValueError('Expected Filesystem, Volume, Dataset or Snapshot, got '
                                 + type(ds).__name__ + ' instead')
        return list(dslist)

    @classmethod
    def _dslist_to_str(cls, dslist: DatasetList, separator: str = ','):
        return separator.join([str(s) for s in cls._dslist_to_list(dslist)])


class ZFSListArgument:

    @staticmethod
    def _zlist_to_list(zlist: ZFSList):
        if not zlist:
            zlist = []
        elif not isinstance(zlist, Iterable):
            zlist = [zlist]
        zlist = list(zlist)  # materialise: a one-shot iterable must survive validating and then using it
        for ds in zlist:
            if not isinstance(ds, ZFS):
                raise ValueError('Expected Filesystem, Volume, Dataset, Snapshot or Bookmark, got '
                                 + type(ds).__name__ + ' instead')
        return list(zlist)

    @classmethod
    def _zlist_to_str(cls, dslist: DatasetList, separator: str = ','):
        return separator.join([str(s) for s in cls._zlist_to_list(dslist)])


class ListCommand(Command, StringListArgument, DatasetListArgument):

    def __call__(self, *args, **kwargs):
        """List ZFS datasets.

        Keyword arguments:
            roots: Dataset | str | [Dataset | str] -- the root dataset(s) to list from (default None)
            types: str -- types of datasets to list, e.g. 'snapshot' or 'filesystem,volume' (default None)
            recursive: bool -- whether to recursively list root dataset children (default False)
            depth: int -- child dataset depth to list (default 0)
            properties: str | [str] -- properties to include in the result (default None)
            sort: str | [str] | {str, bool} -- result sorting order (default None)
            asc: bool -- whether to sort in ascending order (default True)
            lazy: bool -- whether to return a generator instead of a list (default False)
            strict: bool -- raise instead of skipping rows that cannot be trusted (default False; see below)

        Returns:
            An Iterable containing ZFS datasets -- a Generator if lazy is True, List otherwise

        Rows of zfs output that cannot be trusted are skipped with a WARNING on the 'libzfseasy' logger (the first
        few per listing one by one, then a count), never raised: a wrong column count (a value containing a tab or
        newline), a type that is unknown, not requested or does not match the name, a name outside the requested
        roots / depth, a name libzfseasy cannot parse, and -- in the default (list) mode only -- a name that appears
        more than once, which drops every row with that name. A lazy listing applies the per-row checks only: it
        cannot take back an object it has already yielded when the same name appears again later.

        With `strict=True` the same rows are still logged, but if there was any of them the call raises a bare
        Exception whose message starts with 'zfs list output could not be trusted:' (it gives how many rows and why
        the first one was refused) instead of returning a partial result. It is raised only after zfs has been read
        to the end and has exited, and a zfs failure (non-zero exit) takes precedence with its own error.
        `strict=True` with `lazy=True` raises ValueError before anything runs: a lazy listing cannot take back
        objects it has already yielded.
        """

        return self._list(*args, **kwargs)

    @classmethod
    def _list(cls, roots: Optional[DatasetList] = None, types: Optional[str] = None, recursive: bool = False,
              depth: int = 0, properties: Optional[StringList] = None, sort: Optional[Sort] = None,
              asc: bool = True, lazy=False, strict: bool = False) -> Iterable[ZFS]:

        if strict and lazy:
            raise ValueError('strict=True cannot be combined with lazy=True: a lazy listing cannot take back the '
                             'objects it has already yielded')

        cmd = [_zfs_cmd(), 'list', '-H']

        if recursive:
            cmd += ['-r']
        elif depth > 0:
            cmd += ['-d', str(depth)]

        if types:
            types = cls._slist_to_list(types, validator=Validate.type)  # once: `types` is used again for `wanted`
            cmd += ['-t', ','.join(str(t) for t in types)]

        if isinstance(sort, str):
            cls._check_sort_key(sort)
            cmd += ['-s' if asc else '-S', sort]
        elif isinstance(sort, dict):
            for k, s in sort.items():
                cls._check_sort_key(k)
                cmd += ['-s' if s else '-S', k]
        elif isinstance(sort, Iterable):
            for s in sort:
                cls._check_sort_key(s)
                cmd += ['-s' if asc else '-S', s]

        if properties is None:
            properties = ['name', 'type']
        else:
            properties = cls._slist_to_list(properties, validator=Validate.attribute)
            properties = ['name', 'type'] + properties
        cmd += ['-o', ','.join(properties)]

        roots = cls._dslist_to_list(roots) if roots else []
        cmd += [str(r) for r in roots]

        scope = _Scope(roots, recursive=recursive, depth=depth)
        wanted = _wanted_types(cls._slist_to_list(types) if types else None)

        if lazy:
            return cls._lazy_objects(cmd, properties, scope, wanted)

        skips = _SkippedRows('list')
        names = collections.Counter()
        result = list(cls._rows_to_objects(cls._exec_lines(cmd), properties, skips, scope, wanted, names))
        # zfs prints every object once. A name seen twice means one of the rows was forged (by a value with a newline
        # in it) and nothing says which: drop them all.
        twice = {n for n, c in names.items() if c > 1}
        for n in sorted(twice):
            skips.skip('name appears %d times' % names[n], n)
        skips.done()
        if strict and skips.total:
            raise Exception('zfs list output could not be trusted: %d row%s skipped, the first: %s' % (
                skips.total, '' if skips.total == 1 else 's', skips.first_problem))
        return [o for o in result if o.name not in twice]

    @classmethod
    def _lazy_objects(cls, cmd, properties, scope, wanted):
        skips = _SkippedRows('list')
        try:
            yield from cls._rows_to_objects(cls._exec_lines(cmd), properties, skips, scope, wanted)
        finally:
            skips.done()

    @classmethod
    def _rows_to_objects(cls, lines, properties, skips, scope, types, names=None):
        """Yield the objects of the trusted rows. A row zfs's iteration cannot have printed (too short or long, a
        foreign type, out of scope, an unparsable name, or empty) is the rest of a value that contained a newline,
        so the object of the row right before it (whose last value was cut short there) is dropped too. Objects are
        therefore yielded one row late."""
        pending = None
        for line in lines:
            obj, foreign = cls._row_to_object(line, properties, skips, scope, types, names)
            if foreign and pending is not None:
                skips.skip('its last value continues on the next line', pending[1])
                pending = None
            if pending is not None:
                yield pending[0]
            pending = (obj, line) if obj is not None else None
        if pending is not None:
            yield pending[0]

    @staticmethod
    def _check_sort_key(key: str) -> None:
        # `name` is a valid zfs sort key but not a property this library knows; accept it here only.
        if key != 'name':
            Validate.attribute(key)

    @classmethod
    def _line_to_object(cls, line: str, properties: Iterable, skips: Optional[_SkippedRows] = None,
                        scope: Optional[Callable[[str], bool]] = None, types: Optional[set] = None) -> Optional[ZFS]:
        """Build the object one `zfs list -H` row describes, or return None (with a warning) for a row that cannot
        be trusted (see `_row_to_object`)."""
        return cls._row_to_object(line, properties, skips if skips is not None else _SkippedRows('list'), scope,
                                  types)[0]

    @staticmethod
    def _row_to_object(line: str, properties, skips: _SkippedRows, scope: Optional[Callable[[str], bool]] = None,
                       types: Optional[set] = None, names: Optional[collections.Counter] = None):
        """Return `(object or None, foreign)` for one `zfs list -H` row. `properties` are the `-o` columns (`name`,
        `type`, ...); `scope` and `types` (see `_Scope` and `_wanted_types`) say which names and types the command
        could print; `names` counts the name of every row with the right column count, for the caller's duplicate
        check. `foreign` is True for a row zfs's own iteration cannot have printed (the tail of a value that
        contained a newline), False for a trusted row and for one skipped only because libzfseasy cannot represent
        its name."""
        if line == '':
            skips.skip('empty line')
            return None, True
        zfs_info = line.split('\t')
        if len(zfs_info) != len(properties):
            skips.skip('expected %d columns, got %d' % (len(properties), len(zfs_info)), line)
            return None, True
        name, dstype = zfs_info[0:2]
        if names is not None:
            names[name] += 1
        problem = _type_problem(name, dstype, types)
        if problem is None and scope is not None and not scope(name):
            problem = 'name outside the requested roots'
        if problem is not None:
            skips.skip(problem, line)
            return None, True
        zfs_info = [i if not i == '-' else None for i in zfs_info]
        # The property names were validated strictly when the -o list was built, and a column such as `volsize` is
        # `-` on rows whose type lacks it, so apply the values leniently (as GetCommand._build does).
        try:
            obj = ZFS.from_name(name, dstype)
        except InvalidNameError as e:
            skips.unrepresentable(name, e, line)
            return None, False
        except ValueError as e:
            skips.skip('cannot parse the name (%s)' % _clip(e), line)
            return None, True
        if len(zfs_info) > 2:
            try:
                obj.update(dict(zip(properties[2:], zfs_info[2:])), lenient=True)
            except ValueError as e:
                skips.skip('cannot apply the properties (%s)' % _clip(e), line)
                return None, True
        return obj, False


class CreateCommand(Command):
    
    def __call__(self, *args, **kwargs):
        """Create ZFS filesystem or volume.

        Keyword arguments:
            ds: str -- dataset or volume name
            properties: Properties -- properties to set on the created dataset / volume (default None)

        Returns:
            A ZFS Filesystem or Volume object
        """

        return self._create(*args, **kwargs)

    @classmethod
    def _create(cls, ds: str, *args, **kwargs) -> Union[Filesystem, Volume]:
        if (args and isinstance(args[0], str)) or ('size' in kwargs):
            return cls.volume(ds, *args, **kwargs)
        else:
            return cls.filesystem(ds, *args, **kwargs)

    @classmethod
    def filesystem(cls, ds, properties: Optional[Properties] = None, mount=True, parents=False) -> Filesystem:
        """Create ZFS filesystem.

        Keyword arguments:
            ds: str | Filesystem -- dataset name or object
            properties: Properties -- properties to set on the created dataset (default None)
            mount: bool -- should the created dataset be mounted (default True)
            parents: bool -- whether to create parent datasets if needed (default False)

        Returns:
            A ZFS Filesystem object
        """

        filesystem = Filesystem(str(ds), properties)
        _check_values(filesystem.properties)
        cmd = [_zfs_cmd(), 'create']
        if parents:
            cmd += ['-p']
        if not mount:
            cmd += ['-u']
        if properties:
            for k, v in filesystem.properties:
                cmd += ['-o', k + '=' + str(v)]
        cmd += [str(filesystem)]
        cls._exec(cmd)

        return filesystem

    @classmethod
    def volume(cls, ds: str, size: str, properties: Optional[Properties] = None, sparse=False,
               parents=False) -> Volume:
        
        """Create ZFS volume.

        Keyword arguments:
            ds: str -- dataset or volume name
            size: str -- volume size
            properties: Properties -- properties to set on the created volume (default None)
            sparse: bool -- should the created volume be sparse (default False)
            parents: bool -- whether to create parent datasets if needed (default False)

        Returns:
            A ZFS Volume object
        """

        volume = Volume(ds, properties)
        Validate.value(str(size), 'volsize')
        _check_values(volume.properties)
        cmd = [_zfs_cmd(), 'create', '-V', size]
        if parents:
            cmd += ['-p']
        if sparse:
            cmd += ['-s']
        if properties:
            for k, v in volume.properties:
                cmd += ['-o', k + '=' + str(v)]
        cmd += [str(volume)]
        cls._exec(cmd)

        return volume


class SnapshotCommand(Command):

    def __call__(self, *args, **kwargs):
        return self._snapshot(*args, **kwargs)

    @classmethod
    def _snapshot(cls, ds: Dataset, name: str, recursive: bool = False,
                  properties: Optional[Properties] = None) -> Snapshot:

        snapshot = Snapshot(ds, name, properties)
        # Before the recursive listing below, which already runs zfs.
        _check_values(snapshot.properties)
        cmd = [_zfs_cmd(), 'snapshot']
        if recursive:
            cmd += ['-r']
            datasets = ListCommand()(ds, types='filesystem,volume', recursive=recursive)
            result = [Snapshot(cds, name, properties) for cds in datasets]
        else:
            result = snapshot
        if properties:
            for k, v in snapshot.properties:
                cmd += ['-o', k + '=' + str(v)]
        cmd += [str(snapshot)]
        cls._exec(cmd)

        return result


class BookmarkCommand(Command):

    def __call__(self, *args, **kwargs):
        return self._bookmark(*args, **kwargs)

    @classmethod
    def _bookmark(cls, ds: Union[Bookmark, Snapshot], name: str) -> Bookmark:
        if not isinstance(ds, (Bookmark, Snapshot)):
            raise ValueError('Expected Bookmark or Snapshot, got ' + type(ds).__name__ + ' instead')

        bookmark = Bookmark(ds.dataset, name)
        cmd = [_zfs_cmd(), 'bookmark', str(ds), str(bookmark)]
        cls._exec(cmd)

        return bookmark


class DestroyCommand(Command):
    """Wraps `zfs destroy -v -p` (a dry run `-n` unless `destroy=True`).

    Lazy mode (`lazy=True`, `dataset` and `snapshots`): the result is a generator that runs zfs on first iteration
    and yields the `destroy` lines zfs has printed so far; if zfs then fails, the lines already printed are still
    yielded and the bare subprocess-error Exception is raised only afterwards, at the end of the iteration.

    A dry run (`destroy=False`) does not predict every failure: it succeeds on a snapshot that carries a user hold
    (the hold is only checked when the snapshot is really destroyed) but fails on one that has a clone (without
    `clones=True`/`recursive=True`, which destroy the clones too). Do not read a clean dry run as a promise that the
    destroy will go through.
    """

    def __call__(self, *args, **kwargs):
        return self._destroy(*args, **kwargs)

    @classmethod
    def _destroy(cls, ds: Union[Datasets, Bookmark, SnapshotList], *args, **kwargs):
        if isinstance(ds, Dataset):
            return cls.dataset(ds, *args, **kwargs)
        elif isinstance(ds, Snapshot) or isinstance(ds, SnapshotRange) or isinstance(ds, Iterable):
            return cls.snapshots(ds, *args, **kwargs)
        elif isinstance(ds, Bookmark):
            cls.bookmark(ds, *args, **kwargs)
        else:
            raise ValueError('Expected Filesystem, Volume, Snapshot or Bookmark, got ' + type(ds).__name__ + ' instead')

    @staticmethod
    def _base(destroy: bool = False, recursive: bool = False, clones: bool = False) -> Iterable[str]:
        cmd = [_zfs_cmd(), 'destroy', '-v', '-p']
        if not destroy:
            cmd += ['-n']
        if recursive:
            cmd += ['-R'] if clones else ['-r']
        elif clones:
            raise ValueError('Clones can only be set to True if recursive is also set to True')
        return cmd

    @staticmethod
    def _destroy_lines(lines: Iterable[str]) -> Iterable[str]:
        # `zfs destroy -v -p` prints `destroy\t<name>` per object and a final `reclaim\t<bytes>` summary line; only
        # the former are returned.
        try:
            for line in lines:
                line = line.strip()
                if line.startswith('reclaim\t'):
                    continue
                yield line.replace('destroy\t', '')
        finally:
            close = getattr(lines, 'close', None)
            if close is not None:
                close()

    @classmethod
    def dataset(cls, dataset: Dataset, destroy: bool = False, recursive: bool = False, clones: bool = False,
                force: bool = False, lazy: bool = False) -> Iterable[str]:

        if not isinstance(dataset, Dataset):
            raise ValueError('Expected Filesystem or Volume, got ' + type(dataset).__name__ + ' instead')

        cmd = cls._base(destroy, recursive, clones)
        if force:
            cmd += ['-f']
        cmd += [str(dataset)]
        source = cls._exec_out(cmd)
        result = cls._destroy_lines(source)

        if lazy:
            # `result` itself, not `(o for o in result)`: on CPython 3.12.3 closing a generator expression does not
            # release the inner iterator (only deallocation does), so the child would outlive `close()`.
            return result
        try:
            return [o for o in result]
        finally:
            # An interrupt in the comprehension must not leave the child alive behind the traceback's frame.
            result.close()
            source.close()

    @classmethod
    def snapshots(cls, snapshots: Union[Snapshots, Iterable[Snapshots]], destroy: bool = False, recursive: bool = False,
                  clones: bool = False, force: bool = False, lazy: bool = False) -> Iterable[str]:

        ds, snaps = None, ''
        if not isinstance(snapshots, Iterable):
            snapshots = [snapshots]
        for snap in snapshots:
            if not isinstance(snap, (Snapshot, SnapshotRange)):
                raise ValueError('Expected Snapshot or SnapshotRange, got ' + type(snap).__name__ + ' instead')
            if ds is None:
                ds = snap.dataset
            elif str(snap.dataset) != str(ds):
                raise ValueError('Snapshots must come from the same dataset')
            snaps += ',' + snap.short if snaps else snap.short
        if ds is None:
            raise ValueError('Expected at least one Snapshot or SnapshotRange, got none')
        snaps = ds.name + '@' + snaps

        cmd = cls._base(destroy, recursive, clones)
        if force:
            cmd += ['-d']
        cmd += [snaps]
        source = cls._exec_out(cmd)
        result = cls._destroy_lines(source)

        if lazy:
            # `result` itself, not `(o for o in result)`: on CPython 3.12.3 closing a generator expression does not
            # release the inner iterator (only deallocation does), so the child would outlive `close()`.
            return result
        try:
            return [o for o in result]
        finally:
            # An interrupt in the comprehension must not leave the child alive behind the traceback's frame.
            result.close()
            source.close()

    @classmethod
    def bookmark(cls, bookmark: Bookmark, destroy: bool = True) -> None:
        if not destroy:
            return None

        if not isinstance(bookmark, Bookmark):
            raise ValueError('Expected Bookmark, got ' + type(bookmark).__name__ + ' instead')

        cmd = [_zfs_cmd(), 'destroy', str(bookmark.dataset.name) + '#' + str(bookmark.short)]
        cls._exec(cmd)


class RenameCommand(Command):

    def __call__(self, *args, **kwargs):
        return self._rename(*args, **kwargs)

    @classmethod
    def _rename(cls, ds: Datasets, name: str, **kwargs):
        if isinstance(ds, Filesystem):
            return cls.filesystem(ds, name, **kwargs)
        elif isinstance(ds, Volume):
            return cls.volume(ds, name, **kwargs)
        elif isinstance(ds, Snapshot):
            return cls.snapshot(ds, name, **kwargs)
        else:
            raise ValueError('Expected Filesystem, Volume or Snapshot, got ' + type(ds).__name__ + ' instead')

    @staticmethod
    def _base(force: bool = False, parents: bool = False):
        cmd = [_zfs_cmd(), 'rename']
        if force:
            cmd += ['-f']
        if parents:
            cmd += ['-p']
        return cmd

    @classmethod
    def filesystem(cls, dataset: Filesystem, name: str, mount: bool = True, force: bool = False,
                   parents: bool = False) -> Filesystem:

        new = Filesystem(name)
        new.update(dict(dataset.properties), lenient=True)
        cmd = cls._base(force, parents)
        if not mount:
            cmd += ['-u']
        cmd += [str(dataset), str(new)]
        cls._exec(cmd)

        return new

    @classmethod
    def volume(cls, dataset: Volume, name: str, force: bool = False, parents: bool = False) -> Volume:

        new = Volume(name)
        new.update(dict(dataset.properties), lenient=True)
        cmd = cls._base(force, parents)
        cmd += [str(dataset), str(new)]
        cls._exec(cmd)

        return new

    @classmethod
    def snapshot(cls, snapshot: Snapshot, name: str, force: bool = False, parents: bool = False) -> Snapshot:
        if '@' in name.strip('@'):
            new_snap = Snapshot.from_name(name)
            name = new_snap.short
            if str(new_snap.dataset) != str(snapshot.dataset):
                raise ValueError('Snapshots can only be renamed within the parent dataset')
        new = Snapshot(snapshot.dataset, name)
        new.update(dict(snapshot.properties), lenient=True)
        cmd = cls._base(force, parents)
        cmd += [str(snapshot), str(new)]
        cls._exec(cmd)

        return new


class AllowCommand(Command, StringListArgument):

    def __call__(self, *args, **kwargs):
        return self._allow(True, *args, **kwargs)

    def create(self, *args, **kwargs):
        return self._create(True, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self._set(True, *args, **kwargs)

    @classmethod
    def _allow(cls, allow: bool, ds: Dataset, permissions: StringList, users: Optional[StringList] = None,
               groups: Optional[StringList] = None, everyone: bool = False, dataset: bool = True, children: bool = True,
               recursive: bool = False) -> None:

        cmd = cls._base(allow, None, recursive)
        if dataset and not children:
            cmd += ['-l']
        elif children and not dataset:
            cmd += ['-d']
        elif not children and not dataset:
            raise ValueError('At least one of dataset or children needs to be True')
        if everyone:
            if users or groups:
                raise ValueError('Everyone and users or groups cannot be specified simultaneously')
            cmd += ['-e']
        else:
            if users:
                cmd += ['-u', cls._slist_to_str(users)]
            if groups:
                cmd += ['-g', cls._slist_to_str(groups)]
        cmd += [cls._slist_to_str(permissions), str(ds)]
        cls._exec(cmd)

    @classmethod
    def _create(cls, allow: bool, ds: Dataset, permissions: StringList, recursive: bool = False) -> None:

        cmd = cls._base(allow, '-c', recursive)
        cmd += [cls._slist_to_str(permissions), str(ds)]
        cls._exec(cmd)

    @classmethod
    def _set(cls, allow: bool, ds: Dataset, name: str, permissions: StringList, recursive: bool = False) -> None:
        try:
            Validate.zfsname(name.strip('@'))
            valid = True
        except ValueError:
            valid = False
        if not (name.startswith('@') and valid):
            raise ValueError(name + ' is not a valid ZFS permission set name')

        cmd = cls._base(allow, '-s', recursive)
        cmd += [name, cls._slist_to_str(permissions), str(ds)]
        cls._exec(cmd)

    @staticmethod
    def _base(allow: bool, param: Optional[str] = None, recursive: bool = False) -> Iterable[str]:
        extra = [param] if param else []
        if allow:
            cmd = [_zfs_cmd(), 'allow'] + extra
            if recursive:
                raise TypeError('_zfs_allow_base() got an unexpected keyword argument \'recursive\'')
        else:
            cmd = [_zfs_cmd(), 'unallow'] + extra
            if recursive:
                cmd += ['-r']

        return cmd


class UnAllowCommand(AllowCommand):

    def __call__(self, *args, **kwargs):
        return self._allow(False, *args, **kwargs)

    def create(self, *args, **kwargs):
        return self._create(False, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self._set(False, *args, **kwargs)


class CloneCommand(Command):

    def __call__(self, *args, **kwargs):
        return self._clone(*args, **kwargs)

    @classmethod
    def _clone(cls, snapshot: Snapshot, ds: str, properties: Optional[Properties] = None, parents=False) -> Dataset:

        dataset = Dataset(ds)
        cmd = [_zfs_cmd(), 'clone']
        if parents:
            cmd += ['-p']
        if properties:
            for k, v in properties.items():
                Validate.attribute(k)
                v = str(v.value if isinstance(v, Property) else v)
                Validate.value(v, k)
                cmd += ['-o', k + '=' + v]
        cmd += [str(snapshot), str(dataset)]
        cls._exec(cmd)

        props = properties.keys() if properties else None
        result = ListCommand()(dataset, recursive=True, properties=props)
        if len(result) == 1:
            return result[0]

        return result


# The rows `zfs get` prints first for every object: `name`, `type` for an explicit property list (`GetCommand` sends
# them first), `type` for `all` (the first entry of OpenZFS's property table; the leading `name` is a placeholder
# zfs skips). `GetCommand` never passes `-s` to zfs (it filters sources itself), which would drop these rows -- except
# `property_sources`, which has no value column and so no lead rule.
_LEAD_LIST = ('name', 'type')
_LEAD_ALL = ('type',)


class GetCommand(Command, StringListArgument, ZFSListArgument):

    def __call__(self, *args, **kwargs):
        """Get ZFS properties as objects (`zfs get -H -o name,property,value,received,source`).

        Keyword arguments:
            ds: ZFS | [ZFS] -- objects to read (default None: every dataset)
            properties: str | [str] -- properties to read (default 'all')
            types: str | [str] -- object types to read (default 'all')
            sources: str | [str] -- keep only properties from these sources (default 'all'); `type` is always kept
            recursive: bool -- also read the descendants (default False)
            depth: int -- descendant depth to read (default 0)
            lazy: bool -- whether to return a generator instead of a list (default False)

        Returns:
            An Iterable of ZFS objects -- a Generator if lazy is True, List otherwise

        A value containing a tab is recovered when the row is unambiguous (the `received` column is `-`, or the
        source is `received` and the two halves agree). Anything else that cannot be trusted is skipped with a
        WARNING (the first few per call one by one, then a count), never raised: a row that is too short or
        ambiguous, an invalid property name, a property twice for one object, a name outside the requested objects /
        depth, an object without a valid `type` row, an object whose rows do not start with the rows zfs prints first
        (`name`, `type` for a property list, `type` for `all`: rows forged for the next object would merge into its
        group, but not without repeating them; with a property list also an object whose rows are not exactly
        `name`, `type` and the requested properties, in that order) and -- in the default (list) mode only -- every
        object whose rows are not contiguous (zfs prints each object's rows together, so a name that comes back was forged). A lazy
        call drops a name that comes back too, but cannot take back the object it already yielded under that name.
        For one value read safely whatever it contains, use `value()`.
        """
        return self._get(*args, **kwargs)

    @classmethod
    def _get(cls, ds: Optional[DatasetList] = None, properties: Optional[StringList] = 'all',
             types: Optional[StringList] = 'all', sources: Optional[StringList] = 'all', recursive: bool = False,
             depth: int = 0, lazy=False) -> Iterable[ZFS]:

        sources = cls._slist_to_list(sources, validator=Validate.source)
        types = cls._slist_to_list(types, validator=Validate.type)  # once: a one-shot iterable is used twice below
        wanted = _wanted_types(types)
        types = ','.join(str(t) for t in types)
        properties = cls._slist_to_list(properties, validator=Validate.attribute)
        datasets = [str(d) for d in cls._zlist_to_list(ds)]
        # Enforce column order: name, property, value, received, source
        cmd = [_zfs_cmd(), 'get', '-H', '-o', 'name,property,value,received,source'] + \
            cls._get_options(recursive=recursive, depth=depth, types=types, properties=properties, datasets=datasets)
        scope = _Scope(datasets, recursive=recursive, depth=depth)
        lead = cls._lead_properties(properties)
        expected = cls._expected_properties(properties)

        if lazy:
            return cls._lazy_objects(cmd, sources, scope, wanted, lead, expected)

        skips = _SkippedRows('get')
        twice = set()
        result = list(cls._lines_to_objects(cls._exec_lines(cmd), sources, skips=skips, scope=scope, types=wanted,
                                            twice=twice, lead=lead, expected=expected))
        skips.done()
        return [o for o in result if o is not None and o.name not in twice]

    @classmethod
    def _lazy_objects(cls, cmd, sources, scope, wanted, lead=_LEAD_ALL, expected=None):
        skips = _SkippedRows('get')
        try:
            for obj in cls._lines_to_objects(cls._exec_lines(cmd), sources, skips=skips, scope=scope, types=wanted,
                                             lead=lead, expected=expected):
                if obj is not None:
                    yield obj
        finally:
            skips.done()

    @classmethod
    def rows(cls, ds: Optional[DatasetList] = None, properties: Optional[StringList] = 'all',
             types: Optional[StringList] = 'all', sources: Optional[StringList] = 'all', recursive: bool = False,
             depth: int = 0, strict: bool = False, verify_names: bool = True, received: bool = True) -> GetRows:
        """Read properties as the raw rows zfs printed (`zfs get -H`), for callers that must know whether anything
        was lost: a list of `GetRow(name, property, value, received, source)` that says whether it is `.complete`.

        Keyword arguments:
            ds: ZFS | [ZFS] -- objects to read (default None: every dataset)
            properties: str | [str] -- properties to read (default 'all')
            types: str | [str] -- object types to read (default 'all')
            sources: str | [str] -- keep only rows from these sources (default 'all'); the `name` and `type` rows are
                always kept. `inherited` also matches `inherited from <dataset>`. Filtered here, never by `zfs get -s`
                (which would drop the `type` rows the checks below need)
            recursive: bool -- also read the descendants (default False)
            depth: int -- descendant depth to read (default 0)
            strict: bool -- raise `ZFSOutputError` instead of returning a result that is not `.complete` (default False)
            verify_names: bool -- also run a name-only `zfs list -H -o name,type` with the same objects, types, `-r`
                and `-d`, and refuse any object whose name is not in it (a whole object forged through a value that
                contains newlines is not there: names cannot contain a tab or newline, so that listing cannot be
                forged) or that is in it but was not read (default True). Costs a second zfs run
            received: bool -- also read the `received` column (default True). False sends
                `-o name,property,value,source` and every `GetRow.received` is None: the column holds raw bytes of
                whoever sent a dataset, and without it a tab in a value is unambiguous. Safety callers should
                pass False. With the column, a row with a tab in the value or in the received column is refused
                (skipped and counted), never guessed at: the hidden received value could otherwise rewrite the
                reported value

        Returns:
            A `GetRows`: a list of `GetRow`, in the order zfs printed them, with the attributes `complete` (bool: not
            a single row or object was refused or missing -- use this, or `strict`, for every safety decision),
            `dropped` (frozenset of the names of the objects that were refused as a whole, as far as they can be named)
            and `skipped` (int: how many rows and objects were refused in all). Values are raw: `-` is not mapped to
            None, and invalid UTF-8 is kept (surrogateescape). Each object's rows include its `type` row (and, for a
            property list, its `name` row), as zfs printed them. A name libzfseasy cannot represent is kept like any
            other: nothing is built here, and dropping such a descendant could hide something that protects data.

        The checks are those of `get()`: a row that is too short or ambiguous, an invalid property name, a name
        outside the requested objects / depth, an object whose rows do not start with the rows zfs prints first, a
        property twice for one object, an object that does not have a valid `type`, and an object whose rows are not
        contiguous (every group of that name is dropped). With an explicit property list a group that contains a
        property that was not requested is dropped as well: a forged copy of a requested property is a duplicate and
        any other property is unrequested. So is a group whose rows are not exactly `name`, `type` and the
        requested properties, in that order (zfs prints one row per requested property, in the requested order). Everything refused is logged as `get()` does. With `'all'` a value that
        contains newlines can still forge extra rows inside its own object's group (never another object's). An
        explicit list closes in-group forging only with `received=False`: with the `received` column, the sender's
        raw bytes sit in a column the caller does not read, and a tab in it is refused rather than interpreted.

        Residual race of `verify_names`: `zfs get` and `zfs list` are two runs. An object forged by a value, whose
        name becomes a real object between the two runs, passes the cross-check; it can only add rows for an
        object that exists, never remove any, so a protect set built from the rows can only grow.

        Raises:
            ValueError -- an invalid argument, before zfs runs
            ZFSOutputError -- `strict` and the result is not complete (raised after zfs has exited)
            Exception -- zfs failed (a non-zero exit takes precedence), with zfs's stderr as the message
        """
        # Validate.source accepts any case, so compare lowercased (`get()` compares the raw strings: a pre-existing
        # quirk that is left alone there, so `sources='Local'` filters out everything).
        sources = [str(x).lower() for x in cls._slist_to_list(sources, validator=Validate.source)]
        # An empty `types` would leave `-t` off `zfs get` but also off `zfs list`, whose default is not the same set:
        # always send an explicit `-t`, to both.
        types = cls._slist_to_list(types, validator=Validate.type) or ['all']
        wanted = _wanted_types(types)
        types = ','.join(str(t) for t in types)
        properties = cls._slist_to_list(properties, validator=Validate.attribute)
        datasets = [str(d) for d in cls._zlist_to_list(ds)]
        columns = 'name,property,value,received,source' if received else 'name,property,value,source'
        cmd = [_zfs_cmd(), 'get', '-H', '-o', columns] + \
            cls._get_options(recursive=recursive, depth=depth, types=types, properties=properties, datasets=datasets)
        scope = _Scope(datasets, recursive=recursive, depth=depth)
        lead = cls._lead_properties(properties)
        # The exact rows zfs prints for each object for this call: `name`, `type`, then every property asked for.
        expected = cls._expected_properties(properties)

        skips = _SkippedRows('get')
        twice = set()
        kept = []
        dropped = set()
        for group in cls._groups(cls._exec_lines(cmd), skips, scope=scope, twice=twice, lead=lead,
                                 expected=expected, received=received, exact=received):
            problem = None
            if group.bad:
                dropped.add(group.name)
                continue
            if group.dstype is None:
                problem = 'no type row for %r' % _clip(group.name)
            else:
                problem = _type_problem(group.name, group.dstype, wanted)
                if problem is not None:
                    problem = '%s (%r)' % (problem, _clip(group.name))
            if problem is not None:
                skips.skip(problem)
                dropped.add(group.name)
                continue
            kept.append(group)
        # A name that came back was forged somewhere: neither of its groups can be trusted.
        dropped |= twice
        kept = [g for g in kept if g.name not in twice]

        extra_skipped, extra_first = 0, None
        if verify_names:
            listed, list_skips = cls._listed_names(recursive, depth, types, datasets, scope, wanted)
            extra_skipped += list_skips.total
            extra_first = list_skips.first_problem
            for group in kept:
                if group.name not in listed:
                    skips.skip('%r is not in the zfs list of the same objects' % _clip(group.name))
            kept = [g for g in kept if g.name in listed]
            read = {g.name for g in kept}
            # A name that is not an object (forged) is not worth naming: only real objects can be re-read.
            dropped &= listed
            for name in sorted(listed - read - dropped):
                skips.skip('%r was listed by zfs list but not read by zfs get' % _clip(name))
                dropped.add(name)
        skips.done()

        result = []
        for group in kept:
            for prop, value, rcv, source in group.rows:
                if prop in ('name', 'type') or 'all' in sources or source in sources or \
                        ('inherited' in sources and source.startswith('inherited from ')):
                    result.append(GetRow(group.name, prop, value, rcv, source))
        total = skips.total + extra_skipped
        if strict and total:
            raise ZFSOutputError('zfs get output could not be trusted: %d row%s skipped, the first: %s' % (
                total, '' if total == 1 else 's', skips.first_problem or extra_first))
        return GetRows(result, skipped=total, dropped=dropped)

    @classmethod
    def _listed_names(cls, recursive, depth, types, datasets, scope, wanted):
        """The names a name-only `zfs list -H -o name,type` prints for the same objects, types, `-r` and `-d` as a
        `zfs get`, and the `_SkippedRows` of the lines that were not `name<TAB>type`. Names are not built into
        objects, so a name libzfseasy cannot represent is a name like any other. Raises when zfs fails."""
        cmd = [_zfs_cmd(), 'list', '-H', '-o', 'name,type'] + \
            cls._scope_options(recursive=recursive, depth=depth, types=types) + list(datasets)
        skips = _SkippedRows('list')
        names = set()
        for line in cls._exec_lines(cmd):
            if not line:
                continue
            cols = line.split('\t')
            if len(cols) != 2:
                skips.skip('expected 2 columns, got %d' % len(cols), line)
                continue
            name, dstype = cols
            problem = _type_problem(name, dstype, wanted)
            if problem is None and not scope(name):
                problem = 'name outside the requested objects'
            if problem is not None:
                skips.skip(problem, line)
                continue
            names.add(name)
        skips.done()
        return names, skips

    @classmethod
    def value(cls, ds: Union[ZFS, str], prop: str, sources: Optional[StringList] = None,
              raw: bool = False) -> Optional[str]:
        """Read one property of one object, framing-safe whatever the value contains
        (`zfs get -H -o value [-s sources] <prop> <name>`: all of stdout is the value).

        Keyword arguments:
            ds: ZFS | str -- the filesystem, volume, snapshot or bookmark (a name is parsed with `ZFS.from_name`)
            prop: str -- one property name, validated strictly: a known native property (of the object's type,
                unless `ds` is a plain Dataset or a name) or a user property (with a `:`); `all`, lists and names
                starting with `-` are refused
            sources: str | [str] -- only report the value if its source is one of these (`local`, `default`,
                `inherited`, `temporary`, `received`; `all` or None for any) (default None)
            raw: bool -- return zfs's `-` (unset / not applicable) as the string '-' instead of None (default False)

        Returns:
            The value, its lines joined with `\\n` exactly as zfs printed them (tabs, carriage returns, leading and
            trailing spaces and empty lines kept; invalid UTF-8 decoded with `surrogateescape`). None when zfs prints
            `-` (unless `raw`), or prints nothing because the value's source is not in `sources`. A literal value of
            `-` is indistinguishable from unset and also reads as None unless `raw`.

        Raises:
            ValueError -- an invalid object, property name or source, before zfs runs
            Exception -- zfs failed (for example the object does not exist), with zfs's stderr as the message
        """
        if isinstance(ds, str):
            ds = ZFS.from_name(ds)
        elif not isinstance(ds, ZFS):
            raise ValueError('Expected Filesystem, Volume, Dataset, Snapshot or Bookmark, got '
                             + type(ds).__name__ + ' instead')
        if not isinstance(prop, str) or prop == 'all' or prop.startswith('-'):
            raise ValueError(_clip(prop) + ' is not a valid ZFS property name')
        Validate.attribute(prop)
        if type(ds) is not Dataset:
            # A plain Dataset (e.g. parsed from a name) does not know whether it is a filesystem or a volume.
            ds.check_property(prop)
        sources = cls._slist_to_list(sources, validator=Validate.source)
        cmd = [_zfs_cmd(), 'get', '-H', '-o', 'value']
        if sources and 'all' not in [s.lower() for s in sources]:
            cmd += ['-s', ','.join(sources)]
        cmd += [prop, str(ds)]
        lines = list(cls._exec_lines(cmd))
        if not lines:
            return None
        value = '\n'.join(lines)
        if value == '-' and not raw:
            return None
        return value

    @classmethod
    def property_sources(cls, ds: Optional[ZFSList] = None, types: Optional[StringList] = 'all',
                         sources: Optional[StringList] = 'all', recursive: bool = False, depth: int = 0,
                         prefix: Optional[StringList] = None) -> list:
        """Discover which properties exist on objects and where each comes from, with no values
        (`zfs get -H -o name,property,source [-s sources] [-r | -d depth] -t types all <ds>`): phase one of a
        fail-closed read. Phase two is an explicit-list `rows()` of just the names wanted.

        Why this cannot be forged: every column is a dataset name, a property name or a source, none of which can
        contain a tab or a newline (zfs refuses them in names; a property name is `[A-Za-z0-9:._-]`; a source is
        `local`, `default`, `received`, `temporary`, `-` or `inherited from <dataset name>`). The only column an owner
        controls freely, the value, is not requested, so one odd value anywhere (a newline in an unrelated user property,
        one inherited from the pool root) cannot disturb the read. The same fact is why `-s` is allowed here: `get()`
        and `rows()` never pass it because it drops the `type` rows their value-bearing lead rule needs, and there is no
        lead rule when there is no value to frame.

        Keyword arguments:
            ds: ZFS | [ZFS] -- objects to read (default None: every dataset). Do not pass overlapping roots with
                `recursive`: an object zfs prints twice is refused as a duplicate
            types: str | [str] -- object types to read (default 'all'); always sent as an explicit `-t`
            sources: str | [str] -- keep only properties from these sources (default 'all': no `-s`), filtered by zfs.
                `inherited` matches `inherited from <dataset>`
            recursive: bool -- also read the descendants (default False)
            depth: int -- descendant depth to read (default 0)
            prefix: str | [str] -- keep only properties whose name starts with one of these (default None: all),
                filtered here, after every row has been checked (so a malformed row is never hidden by it). A string is
                one prefix, never a comma list

        Returns:
            A list of `PropertySource(name, property, source)` in the order zfs printed them (each object's rows
            together). Names are not built into objects: a name libzfseasy cannot represent is kept like any other.

        Everything is checked and anything wrong raises, never skips: a line that is not exactly three tab-separated
        columns, an invalid object name, a name outside the requested objects / depth / types, an invalid property name
        (by shape: a lowercase native name, or a user property with a `:`; never `Validate.attribute`), a source outside the grammar above, a property twice for one object, an object
        whose rows are not contiguous. Raised only after zfs has exited, and a zfs failure takes precedence.

        Raises:
            ValueError -- an invalid argument, before zfs runs
            ZFSOutputError -- the output could not be trusted (the message names the first problem and how many)
            Exception -- zfs failed, with zfs's stderr as the message (`ZFSTimeoutError` under `timeout()`)
        """
        sources = [str(x).lower() for x in cls._slist_to_list(sources, validator=Validate.source)]
        types = cls._slist_to_list(types, validator=Validate.type) or ['all']
        wanted = _wanted_types(types)
        types = ','.join(str(t) for t in types)
        datasets = [str(d) for d in cls._zlist_to_list(ds)]
        if isinstance(depth, bool) or not isinstance(depth, int) or depth < 0:
            raise ValueError('depth must be a non-negative integer, got %r' % (depth,))
        if prefix is None:
            prefixes = []
        elif isinstance(prefix, str):
            prefixes = [prefix]
        elif isinstance(prefix, (list, tuple, set, frozenset)):
            prefixes = list(prefix)
        else:
            raise ValueError('prefix must be None, a string or a list of strings, got %s' % type(prefix).__name__)
        for x in prefixes:
            if not isinstance(x, str) or not re.fullmatch(Validate.name, x, re.ASCII):
                raise ValueError('%s is not a valid property name prefix' % _clip(repr(x)))

        cmd = [_zfs_cmd(), 'get', '-H', '-o', 'name,property,source']
        if sources and 'all' not in sources:
            cmd += ['-s', ','.join(sources)]
        cmd += cls._scope_options(recursive=recursive, depth=depth, types=types) + ['all'] + datasets
        scope = _Scope(datasets, recursive=recursive, depth=depth)

        found = []
        seen = set()        # (name, property) pairs
        finished = set()    # objects whose rows have ended
        current = None
        problems = 0
        first = None
        lines = cls._exec_lines(cmd)
        try:
            for line in lines:
                if not line:
                    continue
                problem = cls._source_row_problem(line, wanted, scope)
                row = None
                if problem is None:
                    row = PropertySource(*line.split('\t'))
                    if row.name != current:
                        if current is not None:
                            finished.add(current)
                        current = row.name
                        if row.name in finished:
                            problem = 'the rows of %r are not contiguous' % _clip(row.name)
                    if problem is None and (row.name, row.property) in seen:
                        problem = 'property %r appears twice for %r' % (_clip(row.property), _clip(row.name))
                    seen.add((row.name, row.property))
                if problem is not None:
                    problems += 1
                    if first is None:
                        first = '%s: %r' % (problem, _clip(_redact(line)))
                    continue
                if not prefixes or row.property.startswith(tuple(prefixes)):
                    found.append(row)
        finally:
            lines.close()
        if problems:
            raise ZFSOutputError('zfs get output could not be trusted: %d row%s refused, the first: %s' % (
                problems, '' if problems == 1 else 's', first))
        return found

    @staticmethod
    def _source_row_problem(line: str, wanted: Optional[set], scope: Callable[[str], bool]) -> Optional[str]:
        """Why one `name<TAB>property<TAB>source` row of `property_sources()` cannot be trusted, or None."""
        cols = line.split('\t')
        if len(cols) != 3:
            return 'expected 3 columns, got %d' % len(cols)
        name, prop, source = cols
        problem = _plain_name_problem(name)
        if problem is not None:
            return problem
        if '@' in name:
            if wanted is not None and 'snapshot' not in wanted:
                return 'a snapshot was not requested'
        elif '#' in name:
            if wanted is not None and 'bookmark' not in wanted:
                return 'a bookmark was not requested'
        elif wanted is not None and not wanted & {'filesystem', 'volume'}:
            return 'a dataset was not requested'
        if not scope(name):
            return 'name outside the requested objects'
        # Shape only (never `Validate.attribute`): zfs allows property names that this library refuses (a leading `-`, one
        # carried in a received stream), and an unrelated odd name must not stop the read. A name that is wanted
        # reaches argv in phase two, where `rows()` validates it strictly.
        if not (re.fullmatch(Validate.native, prop, re.ASCII) or
                (':' in prop and len(prop) <= Validate.max_name_len and re.fullmatch(Validate.name, prop, re.ASCII))):
            return 'invalid property name'
        if source not in _PLAIN_SOURCES:
            if not source.startswith(_INHERITED):
                return 'invalid source'
            origin = source[len(_INHERITED):]
            if _plain_name_problem(origin) is not None or '@' in origin or '#' in origin:
                return 'invalid source'
        return None

    @staticmethod
    def _scope_options(**kwargs):
        """The `-r`, `-d` and `-t` flags: the ones `zfs get` and the name-only `zfs list` of `rows(verify_names=True)`
        must both be given, so that they iterate the same objects."""
        cmd = []
        if kwargs.get('recursive', False):
            cmd += ['-r']

        depth = kwargs.get('depth', 0)
        if depth > 0:
            cmd += ['-d', str(depth)]
        types = kwargs.get('types')
        if types:
            cmd += ['-t', types]
        return cmd

    @staticmethod
    def _get_options(**kwargs):
        cmd = GetCommand._scope_options(**kwargs)

        properties = kwargs.get('properties')
        if 'all' in properties:
            properties = ['all']
        else:
            # `name,type` lead every object's rows (see `_lead_properties`); drop the caller's own copies (and any
            # other repeat, which zfs would print twice) so that no property appears twice for one object.
            properties = list(GetCommand._expected_properties(properties))
        cmd += [','.join(properties)]

        datasets = kwargs.get('datasets', False)
        if datasets:
            cmd += [datasets] if isinstance(datasets, str) else list(datasets)

        return cmd

    @staticmethod
    def _expected_properties(properties) -> Optional[tuple]:
        """The exact property sequence zfs prints for every object when the call carries an explicit property list
        (None for `all`, whose expansion is not fixed): `name`, `type`, then the caller's properties in order, each
        once. This is what `_get_options` puts on the command line, so it is what zfs was asked for; zfs prints one
        row per requested property, in that order, and nothing else (a user property, a property that does not apply
        to the type, `written@<snap>` and `userquota@` forms included)."""
        if 'all' in properties:
            return None
        return tuple(dict.fromkeys(['name', 'type'] + list(properties)))

    @staticmethod
    def _lead_properties(properties) -> tuple:
        """The rows zfs prints first for every object, for the property list `_get_options` sends: `name`, `type`
        for an explicit list (the two properties it puts first), `type` alone for `all` (zfs expands `all` in
        property-table order, whose first entry is `type`; the leading `name` placeholder is never printed)."""
        return _LEAD_ALL if 'all' in properties else _LEAD_LIST

    @staticmethod
    def _split_row(line: str, skips: _SkippedRows, received: bool = True, exact: bool = False):
        """Split one `name,property,value,received,source` row (`name,property,value,source` when `received` is
        False, and then `received` comes back None), recovering a value that contains tabs when that is unambiguous;
        None (with a warning) otherwise. With `exact` (which `rows()` uses) a row with more than five columns is
        never recovered: a tab in the value or the received column makes the split a guess, and a guess lets the
        hidden received column rewrite the value."""
        cols = line.split('\t')
        if not received:
            if len(cols) < 4:
                skips.skip('expected 4 columns, got %d' % len(cols), line)
                return None
            # Name, property and source cannot contain a tab: whatever lies between them is the value.
            return cols[0], cols[1], '\t'.join(cols[2:-1]), None, cols[-1]
        if len(cols) < 5:
            skips.skip('expected 5 columns, got %d' % len(cols), line)
            return None
        name, prop, source, middle = cols[0], cols[1], cols[-1], cols[2:-1]
        if len(middle) == 2:
            return name, prop, middle[0], middle[1], source
        if exact:
            skips.skip('a tab in the value or received column makes the row ambiguous', line)
            return None
        # `middle` is value + tab + received, and the value and/or the received value contain tabs. The split is
        # trusted only where exactly one split gives a `-` received column or a received column equal to the value
        # (and, for a `received` source, only the latter).
        candidates = []
        for i in range(1, len(middle)):
            value, received = '\t'.join(middle[:i]), '\t'.join(middle[i:])
            if received == value or (received == '-' and source != 'received'):
                candidates.append((value, received))
        if len(candidates) != 1:
            skips.skip('cannot tell the value from the received column', line)
            return None
        return (name, prop) + candidates[0] + (source,)

    @staticmethod
    def _groups(lines, skips: _SkippedRows, scope: Optional[Callable[[str], bool]] = None,
                twice: Optional[set] = None, lead: tuple = _LEAD_ALL, received: bool = True, exact: bool = False,
                expected: Optional[tuple] = None) -> Iterable[_Group]:
        """Group `zfs get -H` rows by object, applying every check that does not depend on what the caller wants to
        do with the rows: a row that is too short or ambiguous, a name outside `scope`, an invalid property name; and
        per group the lead rule, contiguity, a property twice, and (with `expected`, an explicit property list)
        a property nobody asked for and the exact sequence of rows. A group that fails a check is still
        yielded, with `bad` set (already reported), so that a caller can name it. A group is yielded when the next
        one starts, so the caller's own warnings keep their order.

        `lead` is the property sequence zfs prints first for every object (`GetCommand._lead_properties`). A group
        whose first rows differ is dropped whole: rows forged (through a value containing newlines) for the object
        zfs prints next would otherwise merge into its real group, but any forged prefix either does not start with
        the lead rows or repeats them, and the real group's own lead rows then repeat or follow it.

        `expected` (`GetCommand._expected_properties`) is the exact sequence of properties every object's rows must be
        when the call has an explicit property list: `name`, `type`, then the requested properties in order. A group
        with one missing, extra or out of order is dropped whole. None (`all`): no such check.

        A name that comes back after another object's rows is not contiguous: the group is bad and the name is added
        to `twice`, so a caller holding the whole result can drop the earlier group as well."""
        seen = set()
        group = None
        props_seen = set()
        nrows = 0
        allowed = set(expected) if expected is not None else None
        # The properties of the current group's rows, in order, for the exact-sequence check: the rows kept, plus the
        # property of a row of this object that was skipped (and counted) as unreadable -- a value with a tab or a
        # newline makes such a row, real zfs prints them, and the object stays as it was before the check existed.
        sequence = []

        def closed(group):
            # The exact-sequence check, at the end of a group. It runs last, so the specific refusals (lead, twice,
            # unrequested) keep their wording and only a group they did not catch is refused here.
            if expected is not None and not group.bad and tuple(sequence) != expected:
                skips.skip('the rows of %r are not the %d requested properties in order (got %d rows)'
                           % (_clip(group.name), len(expected), len(sequence)))
                group.bad = True
            return group

        for line in lines:
            if not line:
                continue
            row = GetCommand._split_row(line, skips, received, exact)
            if row is None:
                cols = line.split('\t')
                if group is not None and len(cols) >= 2 and cols[0] == group.name:
                    sequence.append(cols[1])
                continue
            name, prop, value, received_value, source = row
            if scope is not None and not scope(name):
                skips.skip('name outside the requested objects', line)
                continue
            try:
                Validate.native_attribute(prop)
            except ValueError:
                skips.skip('invalid property name', line)
                continue
            if group is None or name != group.name:
                if group is not None:
                    yield closed(group)
                    seen.add(group.name)
                group = _Group(name)
                props_seen = set()
                nrows = 0
                sequence = []
                if name in seen:
                    # zfs prints all of an object's rows together: a name that comes back was forged.
                    if twice is not None:
                        twice.add(name)
                    skips.skip('the rows of %r are not contiguous' % _clip(name))
                    group.bad = True
            if nrows < len(lead) and prop != lead[nrows]:
                if not group.bad:
                    skips.skip('the rows of %r do not start with the %r row zfs prints first (got %r)'
                               % (_clip(name), lead[nrows], _clip(prop)))
                group.bad = True
            nrows += 1
            if prop in props_seen:
                if not group.bad:
                    skips.skip('property %r appears twice for %r' % (_clip(prop), _clip(name)))
                group.bad = True
            elif allowed is not None and prop not in allowed:
                if not group.bad:
                    skips.skip('property %r of %r was not requested' % (_clip(prop), _clip(name)))
                group.bad = True
            props_seen.add(prop)
            sequence.append(prop)
            group.rows.append((prop, value, received_value, source))
            if prop == 'type':
                group.dstype = value
        if group is not None:
            yield closed(group)

    @staticmethod
    def _lines_to_objects(lines, sources, skips: Optional[_SkippedRows] = None,
                          scope: Optional[Callable[[str], bool]] = None, types: Optional[set] = None,
                          twice: Optional[set] = None, lead: tuple = _LEAD_ALL,
                          expected: Optional[tuple] = None) -> Iterable[Optional[ZFS]]:
        """Group `zfs get -H -o name,property,value,received,source` rows into objects. Rows that cannot be trusted
        are skipped (see `GetCommand.__call__`). An object whose rows are not contiguous is never yielded again,
        and its name is added to `twice` so a caller holding the whole result can drop the earlier one as well.
        The checks are those of `_groups`, which `rows()` shares."""
        if skips is None:
            skips = _SkippedRows('get')
        for group in GetCommand._groups(lines, skips, scope=scope, twice=twice, lead=lead, expected=expected):
            if group.bad:
                continue
            properties = {}
            for prop, value, received, source in group.rows:
                if prop == 'type':
                    properties[prop] = value
                elif 'all' in sources or source in sources or \
                        ('inherited' in sources and source.startswith('inherited from ')):
                    properties[prop] = Property(value, source, received)
            obj = GetCommand._finish(group.name, properties, False, skips, types)
            if obj is not None:
                yield obj

    @staticmethod
    def _finish(name: str, properties: dict, bad: bool, skips: _SkippedRows, types: Optional[set]) -> Optional[ZFS]:
        """Build the object for one complete group of rows, or None (already warned when `bad`)."""
        if bad:
            return None
        properties.pop('name', None)
        dstype = properties.pop('type', None)
        if dstype is None:
            skips.skip('no type row for %r' % _clip(name))
            return None
        problem = _type_problem(name, dstype, types)
        if problem is not None:
            skips.skip('%s (%r)' % (problem, _clip(name)))
            return None
        try:
            before = _name_warnings
            obj = GetCommand._build(name, dstype, properties)
            if obj is None:
                # `_build` returns None only for a name libzfseasy cannot represent, and has reported it (or not,
                # when the name was already reported: the serial says which).
                skips.count_unrepresentable(name, warned=_name_warnings != before)
            return obj
        except ValueError as e:
            skips.skip('cannot build %r (%s)' % (_clip(name), _clip(e)))
            return None

    @staticmethod
    def _build(name: str, dstype: str, properties: Properties) -> Optional[ZFS]:
        # The property names come from zfs itself (the caller's own names were validated strictly before the command
        # ran), so accept well-formed native names this library does not know yet (a newer OpenZFS adding a property
        # must not break `zfs get all`).
        # A name zfs allows but libzfseasy refuses is skipped (None) with a warning; the caller's properties for that
        # dataset are dropped with it, never merged into a neighbour. Any other ValueError (an unknown type) is raised:
        # `_finish` checks the type first and skips the rest.
        try:
            obj = ZFS.from_name(name, dstype)
        except InvalidNameError as e:
            _skip_invalid_name(name, e)
            return None
        obj.update(properties, lenient=True)
        return obj


class SetCommand(Command):

    def __call__(self, *args, **kwargs):
        return self._set(*args, **kwargs)

    @classmethod
    def _set(cls, ds: Datasets, properties: Properties) -> Datasets:

        # Reject invalid caller-supplied names before anything reaches zfs.
        for k in properties:
            ds.check_property(k)
        _check_values(properties)
        cmd = [_zfs_cmd(), 'set'] + cls._get_props(properties) + [str(ds)]
        cls._exec(cmd)
        ds.update(properties)

        return ds

    @staticmethod
    def _get_props(properties: Properties, flag: bool = False):
        cmd = []
        for k, v in properties.items():
            if flag:
                cmd += ['-o', k + '=' + str(v)]
            else:
                cmd += [k + '=' + str(v)]
        return cmd


class InheritCommand(Command):

    def __call__(self, *args, **kwargs):
        return self._inherit(*args, **kwargs)

    @classmethod
    def _inherit(cls, ds: Datasets, props: StringList, recursive: bool = False, received: bool = False) -> Datasets:

        if isinstance(props, str):
            props = [props]
        else:
            props = list(props)  # materialise: a one-shot iterable is walked twice below

        # Reject invalid caller-supplied names before anything reaches zfs.
        for prop in props:
            ds.check_property(prop)

        for prop in props:
            cmd = [_zfs_cmd(), 'inherit', prop] + cls._get_options(recursive=recursive, received=received) + [str(ds)]
            cls._exec(cmd)
            ds.update({prop: None})

        return ds

    @staticmethod
    def _get_options(**kwargs):
        cmd = []
        if kwargs.get('recursive', False):
            cmd += ['-r']
        if kwargs.get('received', False):
            cmd += ['-S']
        return cmd


class ResumeTokenError(ValueError):
    """A `receive_resume_token` that `zfs send -t` refuses outright (corrupt, or malformed before it was run)."""


@dataclasses.dataclass(frozen=True)
class ResumeToken:
    """What a `receive_resume_token` says about the interrupted receive, as printed by `zfs send -nv -t <token>`.

    Fields (None when the token does not carry them):
        toname: str -- the snapshot being received, as recorded when the receive started. This can be STALE: if the
            sender renamed the dataset since, zfs still prints the old name here; match the stream by `toguid`.
        toguid: int -- GUID of the target snapshot (the reliable identity).
        fromguid: int -- GUID of the incremental source snapshot; None for a full stream.
        object, offset, bytes: int -- where the receive stopped (object number, offset within it, bytes received).
        flags: frozenset -- names of the `*ok` stream features recorded in the token (embedok, compressok, rawok,
            largeblockok, savedok, ...). The recorded set depends on the dataset's feature state, not only on the
            flags the send was started with (for example `largeblockok` is recorded only when the `large_blocks`
            feature is active), and on the OpenZFS version. Accept a token iff `required <= flags <= allowed` for
            the send mode you use (both bounds: an unexpected flag such as `embedok` or `rawok` must be refused);
            never test equality.
        error: str -- zfs's reason when the token can no longer be resumed from this pool (target or source
            snapshot destroyed, ...); None when `zfs send -t` would work. The contents are still reported.
    """
    toname: Optional[str] = None
    toguid: Optional[int] = None
    fromguid: Optional[int] = None
    object: Optional[int] = None
    offset: Optional[int] = None
    bytes: Optional[int] = None
    flags: frozenset = frozenset()
    error: Optional[str] = None


# Resume tokens look like `1-<hex>-<hex>-<hex>` and run to a few hundred bytes (more with redaction lists).
_RESUME_TOKEN_MAX_LEN = 16384
_RESUME_TOKEN_RE = re.compile(r'[\x21-\x7e]+')
_RESUME_INT_FIELDS = ('toguid', 'fromguid', 'object', 'offset', 'bytes')


def _check_resume_token(token) -> None:
    """Raise ResumeTokenError unless `token` looks like a resume token: a non-empty printable-ASCII string of
    bounded length that cannot be taken for an option (no leading `-`). Used before any `zfs send -t` is run."""
    if not isinstance(token, str) or not token or len(token) > _RESUME_TOKEN_MAX_LEN \
            or not _RESUME_TOKEN_RE.fullmatch(token) or token.startswith('-'):
        raise ResumeTokenError('Not a valid ZFS resume token')


def _find_resume_block(lines):
    """Return `(first, end)` such that `lines[first:end]` is the `resume token contents:` header, the
    `nvlist version` line and the tab-indented `key = value` lines of `zfs send -nv -t`, or None if absent."""
    try:
        start = lines.index('resume token contents:')
    except ValueError:
        return None
    end = start + 1
    if end < len(lines) and lines[end].startswith('nvlist version'):
        end += 1
    while end < len(lines) and lines[end].startswith('\t'):
        end += 1
    return start, end


def _parse_resume_contents(lines) -> Optional[ResumeToken]:
    """Parse the `resume token contents:` / `nvlist version: 0` block of `zfs send -nv -t`, or None if absent."""
    span = _find_resume_block(lines)
    if span is None:
        return None
    fields, flags = {}, set()
    for line in lines[span[0] + 1:span[1]]:
        if not line.startswith('\t'):
            continue
        key, sep, value = line.strip().partition(' = ')
        if not sep:
            continue
        value = value.strip()
        if key == 'toname':
            fields[key] = value
        elif key in _RESUME_INT_FIELDS:
            try:
                fields[key] = int(value, 0)
            except ValueError:
                continue
        elif key.endswith('ok') and value == '1':
            flags.add(key)
    return ResumeToken(flags=frozenset(flags), **fields)


class SendCommand(Command):

    def __call__(self, *args, **kwargs):
        return self._send(*args, **kwargs)

    @classmethod
    def _send(cls, ds: Datasets, *args, **kwargs) -> 'StreamHandle':
        if isinstance(ds, Dataset):
            return cls.dataset(ds, *args, **kwargs)
        elif isinstance(ds, Snapshot):
            return cls.snapshot(ds, *args, **kwargs)
        raise ValueError('Expected Dataset or Snapshot, got ' + type(ds).__name__ + ' instead')

    @classmethod
    def snapshot(cls, ds: Snapshot, since: Optional[Snapshot] = None, intermediate: bool = False,
                 replicate: bool = False, holds: bool = False, properties: bool = False, backup: bool = False,
                 raw: bool = False, compressed: bool = False, embed: bool = False, large_blocks: bool = False,
                 skip_missing: bool = False) -> 'StreamHandle':

        """Generate a send stream for a given ZFS snapshot

        Keyword arguments:
            ds: Snapshot -- source ZFS snapshot
            since: Snapshot -- generate incremental stream from the specified snapshot (default None)
            intermediate: bool -- generate intermediate stream (default False)
            replicate: bool -- replicate the stream (default False)
            holds: bool -- include hold tags (default False)
            properties: bool -- include properties (default False)
            backup: bool -- include backup stream header (default False)
            raw: bool -- for encrypted datasets, send data exactly as it exists on disk (default False)
            compressed: bool -- generate a more compact stream by using dataset compression (default False)
            embed: bool -- generate a more compact stream by using the embedded_data pool feature (default False)
            large_blocks: bool -- generate a stream which may contain blocks larger than 128KB (default False)
            skip_missing: bool -- skip over missing snapshots (default False)

        Returns:
            StreamHandle -- context manager wrapping the stream; close()/__exit__ waits
                for the subprocess and raises on non-zero exit
        """

        cmd = [_zfs_cmd(), 'send'] + \
            cls._get_options(since=since, intermediate=intermediate, replicate=replicate, holds=holds,
                             properties=properties, backup=backup, raw=raw, compressed=compressed, embed=embed,
                             large_blocks=large_blocks, skip_missing=skip_missing) + [str(ds)]

        return cls._exec_stream(cmd)

    @classmethod
    def dataset(cls, ds: Dataset, since: Optional[Snapshot] = None, raw: bool = False, compressed: bool = False,
                embed: bool = False, large_blocks: bool = False) -> 'StreamHandle':

        """Generate a send stream for a given ZFS dataset

        Keyword arguments:
            ds: Dataset -- source ZFS dataset (filesystem or volume)
            since: Snapshot -- generate incremental stream from the specified snapshot (default None)
            raw: bool -- for encrypted datasets, send data exactly as it exists on disk (default False)
            compressed: bool -- generate a more compact stream by using dataset compression (default False)
            embed: bool -- generate a more compact stream by using the embedded_data pool feature (default False)
            large_blocks: bool -- generate a stream which may contain blocks larger than 128KB (default False)

        Returns:
            StreamHandle -- context manager wrapping the stream; close()/__exit__ waits
                for the subprocess and raises on non-zero exit
        """

        cmd = [_zfs_cmd(), 'send'] + \
            cls._get_options(since=since, raw=raw, compressed=compressed, embed=embed,
                             large_blocks=large_blocks) + [str(ds)]

        return cls._exec_stream(cmd)

    @classmethod
    def redact(cls, ds: Snapshot, redact: Bookmark, since: Union[Snapshot, Bookmark, None] = None,
               properties: bool = False, compressed: bool = False, embed: bool = False,
               large_blocks: bool = False) -> 'StreamHandle':

        """Generate a redacted send stream

        Keyword arguments:
            ds: Snapshot -- source ZFS snapshot
            redact: Bookmark -- bookmark containing the redaction list of blocks to exclude from the stream
            since: Snapshot -- generate incremental stream from the specified snapshot (default None)
            properties: bool -- include dataset properties in the send stream (default False)
            compressed: bool -- generate a more compact stream by using dataset compression (default False)
            embed: bool -- generate a more compact stream by using the embedded_data pool feature (default False)
            large_blocks: bool -- generate a stream which may contain blocks larger than 128KB (default False)

        Returns:
            StreamHandle -- context manager wrapping the stream; close()/__exit__ waits
                for the subprocess and raises on non-zero exit
        """

        cmd = [_zfs_cmd(), 'send', '--redact', str(redact)] + \
            cls._get_options(since=since, properties=properties, compressed=compressed, embed=embed,
                             large_blocks=large_blocks) + [str(ds)]

        return cls._exec_stream(cmd)

    @classmethod
    def resume(cls, token: str, embed: bool = False) -> 'StreamHandle':
        """Creates a send stream which resumes an interrupted receive

         Keyword arguments:
            token: str -- resume token generated by the receiving side
            embed: bool -- generate a more compact stream by using the embedded_data pool feature (default False)

        Returns:
            StreamHandle -- context manager wrapping the stream; close()/__exit__ waits
                for the subprocess and raises on non-zero exit

        Raises:
            ResumeTokenError: the token is malformed (checked, like `resume_info`, before zfs is run)
        """
        _check_resume_token(token)
        cmd = [_zfs_cmd(), 'send'] + cls._get_options(embed=embed) + ['-t', token]
        return cls._exec_stream(cmd)

    @classmethod
    def resume_info(cls, token: str) -> ResumeToken:
        """Decode a resume token (`zfs send -nv -t <token>`) without sending anything

        Keyword arguments:
            token: str -- resume token generated by the receiving side (`receive_resume_token`)

        Returns:
            ResumeToken -- the decoded contents. When zfs decodes the token but cannot resume it on this pool
                (the target or incremental source snapshot no longer exists, ...), the contents are still
                returned and `error` carries zfs's reason; nothing is raised. `toname` may be stale after a
                sender-side rename, so identify the stream by `toguid`.

        Raises:
            ResumeTokenError: the token is malformed, or zfs rejects it (for example a corrupt token)
        """
        _check_resume_token(token)

        # Which stream carries the contents block can vary by OpenZFS version (stdout on 2.4.1): look in stdout
        # first, then in stderr (read raw, so the tab indentation survives).
        # Run in the C locale so zfs's messages are the English ones callers match on (staleness, corruption).
        lines, stderr_lines, rc = cls._exec_collect([_zfs_cmd(), 'send', '-nv', '-t', token], raw_stderr=True,
                                                    env=_child_env())
        info = _parse_resume_contents(lines)
        span = None
        if info is None:
            info = _parse_resume_contents(stderr_lines)
            span = _find_resume_block(stderr_lines)
        if span is not None:
            # The block, and the size estimate printed around it, are not the error.
            stderr_lines = [l for i, l in enumerate(stderr_lines)
                            if not span[0] <= i < span[1]
                            and not l.startswith(('full send of', 'send from', 'total estimated size'))]
        errors = []
        for line in stderr_lines:
            line = line.strip()
            if line:
                _append_error(errors, line)
        text = '\n'.join(errors)
        if info is None:
            if rc == 0:
                raise ResumeTokenError('zfs printed no resume token contents')
            if 'resume' in text.lower():
                raise ResumeTokenError(text)
            raise Exception(_failure_text(errors, rc))
        if rc != 0:
            info = dataclasses.replace(info, error=_failure_text(errors, rc))
        return info

    @classmethod
    def partial(cls, ds: Dataset, since: Optional[Snapshot] = None) -> 'StreamHandle':
        """Generate a send stream from a dataset that has been partially received

        Keyword arguments:
            ds: Filesystem -- source ZFS filesystem
            since: Snapshot -- generate incremental stream from the specified snapshot (default None)

        Returns:
            StreamHandle -- context manager wrapping the stream; close()/__exit__ waits
                for the subprocess and raises on non-zero exit
        """

        cmd = [_zfs_cmd(), 'send'] + cls._get_options(since=since) + ['-S', str(ds)]

        return cls._exec_stream(cmd)

    @staticmethod
    def _get_options(**kwargs):
        cmd = []
        if kwargs.get('replicate', False):
            cmd += ['-R']
        if kwargs.get('holds', False):
            cmd += ['-h']
        if kwargs.get('properties', False):
            cmd += ['-p']
        if kwargs.get('backup', False):
            cmd += ['-b']
        if kwargs.get('raw', False):
            cmd += ['-w']
        if kwargs.get('compressed', False):
            cmd += ['-c']
        if kwargs.get('embed', False):
            cmd += ['-e']
        if kwargs.get('large_blocks', False):
            cmd += ['-L']
        if kwargs.get('skip_missing', False):
            if kwargs.get('replicate', False):
                cmd += ['-s']
            else:
                raise ValueError('skip_missing can only be specified together with replicate')
        since = kwargs.get('since', False)
        if since:
            if not isinstance(since, Snapshot):
                raise ValueError('Expected Snapshot, got ' + type(since).__name__ + ' instead')
            elif kwargs.get('intermediate', False):
                cmd += ['-I', str(since)]
            else:
                cmd += ['-i', str(since)]

        return cmd

    @classmethod
    def _exec_stream(cls, cmd) -> 'StreamHandle':
        _log_command(cmd)
        errors = _BoundedErrors()
        stderr_thread = None
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=_child_env())
        try:
            # stderr is drained on its own thread from the start, so a child writing more than a pipe buffer of
            # diagnostics can neither block itself nor stall the stdout peek below.
            stderr_thread = cls._start_stderr_reader_bin(process, errors)
            while True:
                output = cls._exec_capture_bin(process, errors, stderr_thread)
                if output is False:
                    # Process exited successfully without ever producing output
                    # (e.g. an empty stream); still hand back a StreamHandle so
                    # callers have a uniform close()/context-manager API.
                    return StreamHandle(process, process.stdout, errors, stderr_thread)
                elif output is not None:
                    return StreamHandle(process, output, errors, stderr_thread)
        except BaseException:
            # Startup failed (or was interrupted): leave no child, pipe or reader thread behind.
            cls._abort_stream(process, stderr_thread)
            raise

    @classmethod
    def _exec_capture_bin(cls, process, errors, stderr_thread=None) -> Union[bool, io.BufferedReader, None]:
        # Guard against mocked processes without real stdout. peek() blocks until the first byte or EOF; stderr is
        # drained concurrently by `stderr_thread`, so that cannot deadlock on a chatty child.
        stdout = process.stdout.peek(8) if getattr(process, 'stdout', None) else b''
        if len(stdout):
            return process.stdout
        # stdout hit EOF (or there is none): the child is finished or about to be. Block until it exits (never spin
        # on peek()/poll()), then let the reader finish so `errors` holds the complete stderr.
        if stderr_thread is None and getattr(process, 'stderr', None):
            # No reader thread (called directly rather than via _exec_stream): drain stderr to its end *before*
            # waiting, since a bare wait() could deadlock on a full stderr pipe.
            try:
                remainder = process.stderr.read().decode('utf-8', errors='replace')
                for line in remainder.splitlines():
                    error = line.strip()
                    if error:
                        _append_error(errors, error)
            except Exception:
                pass
        rc = process.poll()
        if rc is None:
            rc = process.wait()
        cls._join_stderr_reader(stderr_thread)
        if rc != 0:
            StreamHandle._close_pipe(getattr(process, 'stdout', None))
            if stderr_thread is None or not stderr_thread.is_alive():
                StreamHandle._close_pipe(getattr(process, 'stderr', None))
            raise Exception(_failure_text(errors, rc))
        return False


class ReceiveCommand(Command, StringListArgument):

    def __call__(self, *args, **kwargs):
        return self._receive(*args, **kwargs)

    @classmethod
    def _receive(cls, ds: Datasets, *args, **kwargs) -> 'StreamHandle':
        if isinstance(ds, Filesystem):
            return cls.filesystem(ds, *args, **kwargs)
        else:
            return cls.dataset(ds, *args, **kwargs)

    @classmethod
    def filesystem(cls, ds: Filesystem, props: Optional[Properties] = None, reset: Optional[StringList] = None,
                   origin: Optional[Snapshot] = None, force: bool = False, holds: bool = True, unmount: bool = False,
                   save: bool = False, mount: bool = True, ignore_first: bool = False,
                   ignore_all: bool = False) -> 'StreamHandle':

        """Receive ZFS filesystem stream

        Keyword arguments:
            ds: Filesystem -- target ZFS filesystem
            props: Properties -- properties to set on the target filesystem (default None)
            reset: StringList -- names of properties to exclude (`-x`: keep the received value out, use the local
                default); a name that is also in `props` (or `origin`) raises ValueError (default None)
            origin: Snapshot -- forces the stream to be received as a clone of the given snapshot (default None)
            force: bool -- force discarding changes or incompatible snapshots from the target filesystem (default False)
            holds: bool -- receive holds (default True)
            unmount: bool -- unmount the target filesystem during transfer (default False)
            save: bool -- save state token for resuming the transfer (default False)
            mount: bool -- whether to mount the received filesystem (default True)
            ignore_first: bool -- discard the first part of snapshot filesystem name (default False)
            ignore_all: bool -- discard all but the last part of snapshot filesystem name (default False)

        Returns:
            StreamHandle -- context manager wrapping the stream; close()/__exit__ waits
                for the subprocess and raises on non-zero exit
        """
        cmd = [_zfs_cmd(), 'receive'] + \
            cls._get_options(props=props, reset=reset, origin=origin, force=force, holds=holds, unmount=unmount,
                             save=save, mount=mount, ignore_first=ignore_first, ignore_all=ignore_all) + [str(ds)]

        return cls._exec_stream_in(cmd)

    @classmethod
    def dataset(cls, ds: Datasets, props: Optional[Properties] = None, reset: Optional[StringList] = None,
                origin: Optional[Snapshot] = None, force: bool = False, holds: bool = True, unmount: bool = False,
                save: bool = False, mount: bool = True) -> 'StreamHandle':

        """Receive ZFS filesystem stream

        Keyword arguments:
            ds: Filesystem -- target ZFS filesystem
            props: Properties -- properties to set on the target filesystem (default None)
            reset: StringList -- names of properties to exclude (`-x`: keep the received value out, use the local
                default); a name that is also in `props` (or `origin`) raises ValueError (default None)
            origin: Snapshot -- forces the stream to be received as a clone of the given snapshot (default None)
            force: bool -- force discarding changes or incompatible snapshots from the target filesystem (default False)
            holds: bool -- receive holds (default True)
            unmount: bool -- unmount the target filesystem during transfer (default False)
            save: bool -- save state token for resuming the transfer (default False)
            mount: bool -- whether to mount the received filesystem (default True)

        Returns:
            StreamHandle -- context manager wrapping the stream; close()/__exit__ waits
                for the subprocess and raises on non-zero exit
        """
        cmd = [_zfs_cmd(), 'receive'] + \
            cls._get_options(props=props, reset=reset, origin=origin, force=force, holds=holds, unmount=unmount,
                             save=save, mount=mount) + [str(ds)]

        return cls._exec_stream_in(cmd)

    @classmethod
    def abort(cls, ds: Datasets) -> None:
        """Abort an interrupted ZFS receive operation, deleting its saved partially received state

        Keyword arguments:
            ds: Filesystem -- target ZFS filesystem

        Returns:
            None
        """

        cmd = [_zfs_cmd(), 'receive', '-A'] + [str(ds)]

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        origin = kwargs.get('origin', None)
        if origin:
            cmd += ['-o', 'origin=' + str(origin)]
        props = kwargs.get('props', None)
        if props:
            # Reject invalid caller-supplied names (and unframeable values) before anything reaches zfs.
            for k in props:
                Validate.attribute(k)
            _check_values(props)
            cmd += cls._get_props(props, True)
        reset = kwargs.get('reset', None)
        if reset:
            reset_names = cls._slist_to_list(reset, validator=Validate.attribute)
            # zfs refuses a property that is both set (-o) and excluded (-x) with a usage error (rc 2).
            set_names = set(props or ()) | ({'origin'} if origin else set())
            both = sorted(set(reset_names) & set_names)
            if both:
                raise ValueError('Properties cannot be both set (props/origin) and reset: ' + ', '.join(both))
            for s in reset_names:
                cmd += ['-x', s]
        if kwargs.get('force', False):
            cmd += ['-F']
        if not kwargs.get('holds', True):
            cmd += ['-h']
        if kwargs.get('unmount', False):
            cmd += ['-M']
        if kwargs.get('save', False):
            cmd += ['-s']
        if not kwargs.get('mount', True):
            cmd += ['-u']
        if kwargs.get('ignore_first', False):
            cmd += ['-d']
        if kwargs.get('ignore_all', False):
            cmd += ['-e']

        return cmd

    @staticmethod
    def _get_props(properties: Properties, flag: bool = False):
        cmd = []
        for k, v in properties.items():
            if flag:
                cmd += ['-o', k + '=' + str(v)]
            else:
                cmd += [k + '=' + str(v)]
        return cmd

    @classmethod
    def _exec_stream_in(cls, cmd) -> 'StreamHandle':
        _log_command(cmd)
        # Only stdin (the stream we write to) and stderr (for error
        # reporting) are ever read from; stdout is intentionally discarded
        # (not piped) since nothing drains it and a chatty child could
        # otherwise fill the pipe and block on write(), deadlocking close()'s
        # process.wait().
        errors = _BoundedErrors()
        stderr_thread = None
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                   env=_child_env())
        try:
            # stderr is drained on a thread from the start (a receive writing a lot of diagnostics must not block
            # on a full pipe while the caller is still writing the stream); close() joins it before raising.
            stderr_thread = cls._start_stderr_reader_bin(process, errors)
            # Poll once to catch immediate startup failures; zfs receive emits no stderr while waiting for data
            rc = process.poll()
            if rc is not None and rc != 0:
                cls._join_stderr_reader(stderr_thread)
                raise Exception(_failure_text(errors, rc))
        except BaseException:
            cls._abort_stream(process, stderr_thread)
            raise
        return StreamHandle(process, process.stdin, errors, stderr_thread)


class LoadKeyCommand(Command):

    def __call__(self, *args, **kwargs):
        """Load the dataset key

        Keyword arguments:
            ds: Filesystem | None -- ZFS filesystem to load the key for (required). Pass None explicitly to load
                the keys of all filesystems (`zfs load-key -a`); that cannot be combined with `location` or
                `recursive`
            location: str -- key location (default None)
            recursive: bool -- whether to load keys for all child datasets (default False)

        Returns:
            None
        """
        return self._load_key(*args, **kwargs)

    @classmethod
    def _load_key(cls, ds: Optional[Filesystem], location: Optional[str] = None, recursive: bool = False) -> None:
        if ds is not None:
            cmd = [_zfs_cmd(), 'load-key'] + cls._get_options(location=location, recursive=recursive) + [str(ds)]
        else:
            if location is not None:
                raise ValueError('Key location cannot be explicitly specified when loading all keys')
            if recursive:
                raise ValueError('Recursive cannot be specified when loading all keys')

            cmd = [_zfs_cmd(), 'load-key', '-a']

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        location = kwargs.get('location', None)
        recursive = kwargs.get('recursive', False)
        if location and recursive:
            raise ValueError('Key location cannot be explicitly specified when loading keys recursively')
        elif location:
            cmd += ['-L', location]
        elif recursive:
            cmd += ['-r']

        return cmd


class UnLoadKeyCommand(Command):

    def __call__(self, *args, **kwargs):
        """Unload the dataset key

        Keyword arguments:
            ds: Filesystem | None -- ZFS filesystem to unload the key for (required). Pass None explicitly to
                unload the keys of all filesystems (`zfs unload-key -a`); that cannot be combined with `recursive`
            recursive: bool -- whether to unload keys for all child datasets (default False)

        Returns:
            None
        """
        return self._unload_key(*args, **kwargs)

    @classmethod
    def _unload_key(cls, ds: Optional[Filesystem], recursive: bool = False) -> None:
        if ds is not None:
            cmd = [_zfs_cmd(), 'unload-key'] + cls._get_options(recursive=recursive) + [str(ds)]
        else:
            if recursive:
                raise ValueError('Recursive cannot be specified when unloading all keys')

            cmd = [_zfs_cmd(), 'unload-key', '-a']

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        return ['-r'] if kwargs.get('recursive', False) else []


class ChangeKeyCommand(Command):

    def __call__(self, *args, **kwargs):
        """Change the dataset key

        Keyword arguments:
            ds: Filesystem -- ZFS filesystem to change the key for
            inherit: bool -- inhherit the key from parent dataset (default False)
            load: bool -- load the key before changing it (default False)
            location: str -- key location (default None)
            fmt: str -- key format, e.g. 'passphrase' or 'raw' (default None)
            iterations: int -- the number of PBKDF2 iterations, a positive int (default None; zfs enforces its own
                minimum)

        Returns:
            None
        """
        return self._change_key(*args, **kwargs)

    @classmethod
    def _change_key(cls, ds: Filesystem, inherit: bool = False, load: bool = False, location: Optional[str] = None,
                    fmt: Optional[str] = None, iterations: Optional[int] = None) -> None:
        cmd = [_zfs_cmd(), 'change-key'] + \
              cls._get_options(inherit=inherit, load=load, location=location,
                               fmt=fmt, iterations=iterations) + [str(ds)]

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        location = kwargs.get('location', None)
        fmt = kwargs.get('fmt', None)
        iterations = kwargs.get('iterations', None)
        # The one `-o` value that skips `Validate.value`: it is formatted from a number, so require one.
        if iterations is not None and (not isinstance(iterations, int) or isinstance(iterations, bool)
                                       or iterations < 1):
            raise ValueError('Number of pbkdf2 iterations must be a positive integer, got ' + repr(iterations))
        if kwargs.get('inherit', None):
            if location:
                raise ValueError('Key location cannot be specified when inheriting keys')
            if fmt:
                raise ValueError('Key format cannot be specified when inheriting keys')
            if iterations:
                raise ValueError('Number of pbkdf2 iterations cannot be specified when inheriting keys')
            cmd += ['-i']
        if kwargs.get('load', None):
            cmd += ['-l']
        if location:
            Validate.value(location, 'keylocation')
            cmd += ['-o', 'keylocation=' + location]
        if fmt:
            Validate.value(fmt, 'keyformat')
            cmd += ['-o', 'keyformat=' + fmt]
        if iterations:
            cmd += ['-o', 'pbkdf2iters=' + str(iterations)]

        return cmd


class MountCommand(Command, PropertyCommand):

    def __call__(self, *args, **kwargs):
        """Mount ZFS dataset

        Keyword arguments:
            ds: Filesystem -- ZFS filesystem to mount, if None, mount all ZFS filesystems (default None)
            flags: str | [str] -- mount flags (default None)
            properties: str | [str] -- mount properties (default None)
            overlay: bool -- proceed even if the mount point is not empty (default False)
            load_keys: bool -- whether to load keys for each mounted filesystem (default False)
            force: bool -- attempt to mount the filesystems that normally would not be mounted (default False)

        Returns:
            None
        """
        return self._mount(*args, **kwargs)

    @classmethod
    def _mount(cls, ds: Optional[Filesystem] = None, flags: Optional[StringList] = None,
               properties: Optional[Properties] = None,
               overlay: bool = False, load_keys: bool = False, force: bool = False) -> None:
        if ds is not None:
            cmd = [_zfs_cmd(), 'mount'] + cls._get_options(overlay=overlay, load_keys=load_keys, force=force) + \
                  cls._get_properties(flags, properties) + [str(ds)]
        else:
            cmd = [_zfs_cmd(), 'mount', '-a']

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        if kwargs.get('overlay', None):
            cmd += ['-O']
        if kwargs.get('load_keys', None):
            cmd += ['-l']
        if kwargs.get('force', None):
            cmd += ['-f']

        return cmd

    @classmethod
    def _get_properties(cls, flags: Optional[StringList], properties: Optional[Properties]):
        _check_values(properties)
        return cls._get_flags(flags) + cls._get_props(properties, True)

    @staticmethod
    def _get_flags(flags: Optional[StringList]):
        if not flags:
            return []
        if isinstance(flags, str):
            flags = flags.split(',')
        result = []
        for f in flags:
            f = str(f).strip()
            Validate.value(f, 'mount option')
            result += ['-o', f]
        return result


class UnMountCommand(Command):

    def __call__(self, *args, **kwargs):
        """Unmount ZFS filesystem

        Keyword arguments:
            ds: Filesystem -- ZFS filesystem to unmount, if None, unmount all ZFS filesystems (default None)
            force: bool -- forcefully unmount the file system, even if it is currently in use (default False)
            unload_keys: bool -- unload keys for any encryption roots being unmounted (default False)

        Returns:
            None
        """
        return self._unmount(*args, **kwargs)

    @classmethod
    def _unmount(cls, ds: Optional[Filesystem], force: bool = False, unload_keys: bool = False) -> None:
        if ds is not None:
            cmd = [_zfs_cmd(), 'unmount'] + cls._get_options(force=force, unload_keys=unload_keys) + [str(ds)]
        else:
            cmd = [_zfs_cmd(), 'unmount', '-a']

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        if kwargs.get('force', None):
            cmd += ['-f']
        if kwargs.get('unload_keys', None):
            cmd += ['-u']

        return cmd

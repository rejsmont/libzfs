from libzfseasy.types import InvalidNameError, InvalidValueError, Validate, ZFS, Dataset, Filesystem, Volume, Snapshot as Snapshot, Bookmark, \
     SnapshotRange, Property, Properties
from typing import Optional, Union, Dict, Iterable, Callable

import collections
import dataclasses
import io
import logging
import os
import re
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


def _skip_invalid_name(name: str, error: Exception) -> None:
    """Log (once per distinct name per `_WARNED_NAMES_TTL`) that a row of zfs output was skipped because its name is
    one zfs allows but libzfseasy cannot represent."""
    global _warned_names_since
    with _warned_lock:
        now = time.monotonic()
        if _warned_names_since is None:
            _warned_names_since = now
        elif now - _warned_names_since >= _WARNED_NAMES_TTL:
            _warned_names.clear()
            _warned_names_since = now
        if name in _warned_names:
            return
        if len(_warned_names) >= _MAX_WARNED_NAMES:
            if len(_warned_names) == _MAX_WARNED_NAMES:
                _warned_names.add(None)
                logger.warning('skipping names from zfs output: more than %d distinct names libzfseasy cannot '
                               'represent; further ones are skipped silently', _MAX_WARNED_NAMES)
            return
        _warned_names.add(name)
    logger.warning('skipping %r from zfs output: libzfseasy cannot represent this name (%s)', _clip(name), error)


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

    def skip(self, reason: str, row=None) -> None:
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
# escalating to SIGKILL when reaping it in Command._exec_out's finally block.
_TERMINATE_TIMEOUT = 5

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
    bound. Appended to by the stderr reader thread and read only after the thread was joined."""

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
        _log_command(cmd)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                                   env=_child_env())
        errors = []
        stderr_thread = cls._start_stderr_reader(process, errors)
        while True:
            output = cls._exec_capture(process, errors, stderr_thread)
            if output is False:
                break

    @classmethod
    def _exec_out(cls, cmd):
        _log_command(cmd)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                                   env=_child_env())
        errors = []
        stderr_thread = cls._start_stderr_reader(process, errors)
        completed = False
        try:
            while True:
                output = cls._exec_capture(process, errors, stderr_thread)
                if output is False:
                    completed = True
                    break
                elif output != '' and output is not None:
                    yield output
        finally:
            # If the generator is abandoned mid-iteration (caller breaks out of
            # a loop, or it's garbage collected), make sure the child is reaped
            # and the stderr reader thread isn't left blocked on readline()
            # forever. A normal, fully-consumed run has already reaped the
            # child and joined the thread inside _exec_capture, so skip
            # redoing that work (and an extra poll()) here.
            if not completed:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=_TERMINATE_TIMEOUT)
                    except subprocess.TimeoutExpired:
                        # Child ignored SIGTERM; escalate rather than risk
                        # the stderr thread staying blocked on readline()
                        # (and this wait()) forever.
                        process.kill()
                        process.wait()
                stderr_thread.join()
                process.wait()

    @classmethod
    def _exec_lines(cls, cmd):
        """Run `cmd` and yield its stdout lines exactly as printed, for parsers that must frame `-H` output: only the
        line's `\\n` is removed (a trailing empty column and trailing spaces in a value survive, `\\r` is not a line
        break) and blank lines are yielded too. Stdout is read as bytes and decoded as UTF-8 with `surrogateescape`, so
        an invalid byte in a value can neither raise nor be lost. Raises the bare subprocess-error Exception (stderr
        lines) on a non-zero exit, after the last line. An abandoned generator reaps the child like `_exec_out`.

        Unlike `_exec_out` (kept as it is: zfsbackup calls it directly) nothing is stripped or dropped."""
        _log_command(cmd)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=_child_env())
        errors = _BoundedErrors()
        stderr_thread = cls._start_stderr_reader_lines(process, errors)
        completed = False
        try:
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
            cls._join_stderr_reader(stderr_thread)
            completed = True
        finally:
            if not completed:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=_TERMINATE_TIMEOUT)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                cls._join_stderr_reader(stderr_thread)
                process.wait()
        if rc != 0:
            raise Exception(_failure_text(errors, rc))

    @staticmethod
    def _start_stderr_reader_lines(process, errors):
        """`_start_stderr_reader` for a pipe that may be binary (decoded as UTF-8 with replacement, so an odd byte
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
                        errors.append(error[0].upper() + error[1:])
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
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                                   encoding='utf-8', errors='replace',
                                   env=_child_env() if env is None else env)
        errors = _BoundedErrors()
        if raw_stderr:
            def reader():
                try:
                    while True:
                        line = process.stderr.readline()
                        if line == '':
                            break
                        errors.append(line.rstrip('\r\n'))
                except (OSError, ValueError):
                    # The pipe broke or was closed under us; whatever was read so far stands.
                    pass
            stderr_thread = threading.Thread(target=reader, daemon=True)
            stderr_thread.start()
        else:
            stderr_thread = cls._start_stderr_reader(process, errors)
        lines = []
        completed = False
        try:
            while True:
                line = process.stdout.readline()
                if line == '':
                    break
                lines.append(line.rstrip('\n'))
            rc = process.wait()
            cls._join_stderr_reader(stderr_thread)
            completed = True
        finally:
            if not completed:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                cls._join_stderr_reader(stderr_thread)
        return lines, list(errors), rc

    @staticmethod
    def _start_stderr_reader(process, errors):
        # Drain stderr in a dedicated thread so a full stderr pipe can never
        # block the child while we read stdout (and vice versa).
        def reader():
            while True:
                line = process.stderr.readline()
                if line == '':
                    break
                error = line.strip()
                if error:
                    errors.append(error[0].upper() + error[1:])
        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        return thread

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
                        errors.append(error[0].upper() + error[1:] + (' [truncated]' if truncated else ''))
            except (OSError, ValueError):
                # The pipe was closed under us (abort/reap); whatever was read so far stands.
                pass
        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        return thread

    @staticmethod
    def _join_stderr_reader(thread):
        # Bounded: a stray grandchild holding the pipe open must not hang the caller (the thread is a daemon).
        # Joined at most once per thread: a second caller (e.g. the abort path after a failed start) must not
        # wait out the timeout again for a reader that already timed out.
        if thread is None or getattr(thread, '_libzfseasy_joined', False) is True:
            return
        thread.join(timeout=_TERMINATE_TIMEOUT)
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

    @staticmethod
    def _exec_capture(process, errors, stderr_thread=None):
        stdout = process.stdout.readline()
        rc = process.poll()
        if stdout != '':
            return stdout.strip()
        # stdout hit EOF; block until the child actually exits instead of
        # returning None and letting the caller's `while True` loop spin on
        # readline()/poll() until it does.
        if rc is None:
            rc = process.wait()
        if stderr_thread is not None:
            stderr_thread.join()
        if rc != 0:
            raise Exception(_failure_text(errors, rc))
        return False


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
        rc = self._process.wait()
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
                        self._errors.append(error[0].upper() + error[1:])
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
            root: Dataset -- specify a root dataset to list from (default None)
            dstype: str -- types of datasets to list (default None)
            recursive: bool -- whether to recursively list root dataset children (default False)
            depth: int -- child dataset depth to list (default 0)
            properties: str | [str] -- properties to include in the result (default None)
            sort: str | [str] | {str, bool} -- result sorting order (default None)
            asc: bool -- whether to sort in ascending order (default True)
            lazy: bool -- whether to return a generator instead of a list (default False)

        Returns:
            An Iterable containing ZFS datasets -- a Generator if lazy is True, List otherwise

        Rows of zfs output that cannot be trusted are skipped with a WARNING on the 'libzfseasy' logger (the first
        few per listing one by one, then a count), never raised: a wrong column count (a value containing a tab or
        newline), a type that is unknown, not requested or does not match the name, a name outside the requested
        roots / depth, a name libzfseasy cannot parse, and -- in the default (list) mode only -- a name that appears
        more than once, which drops every row with that name. A lazy listing applies the per-row checks only: it
        cannot take back an object it has already yielded when the same name appears again later.
        """

        return self._list(*args, **kwargs)

    @classmethod
    def _list(cls, roots: Optional[DatasetList] = None, types: Optional[str] = None, recursive: bool = False,
              depth: int = 0, properties: Optional[StringList] = None, sort: Optional[Sort] = None,
              asc: bool = True, lazy=False) -> Iterable[ZFS]:

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
            _skip_invalid_name(name, e)
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
        for line in lines:
            line = line.strip()
            if line.startswith('reclaim\t'):
                continue
            yield line.replace('destroy\t', '')

    @classmethod
    def dataset(cls, dataset: Dataset, destroy: bool = False, recursive: bool = False, clones: bool = False,
                force: bool = False, lazy: bool = False) -> Iterable[str]:

        if not isinstance(dataset, Dataset):
            raise ValueError('Expected Filesystem or Volume, got ' + type(dataset).__name__ + ' instead')

        cmd = cls._base(destroy, recursive, clones)
        if force:
            cmd += ['-f']
        cmd += [str(dataset)]
        result = cls._destroy_lines(cls._exec_out(cmd))

        if lazy:
            return (o for o in result)
        else:
            return [o for o in result]

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
        result = cls._destroy_lines(cls._exec_out(cmd))

        if lazy:
            return (o for o in result)
        else:
            return [o for o in result]

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
# zfs skips). `GetCommand` never passes `-s` to zfs (it filters sources itself), which would drop these rows.
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
        group, but not without repeating them) and -- in the default (list) mode only -- every object whose rows
        are not contiguous (zfs prints each object's rows together, so a name that comes back was forged). A lazy
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

        if lazy:
            return cls._lazy_objects(cmd, sources, scope, wanted, lead)

        skips = _SkippedRows('get')
        twice = set()
        result = list(cls._lines_to_objects(cls._exec_lines(cmd), sources, skips=skips, scope=scope, types=wanted,
                                            twice=twice, lead=lead))
        skips.done()
        return [o for o in result if o is not None and o.name not in twice]

    @classmethod
    def _lazy_objects(cls, cmd, sources, scope, wanted, lead=_LEAD_ALL):
        skips = _SkippedRows('get')
        try:
            for obj in cls._lines_to_objects(cls._exec_lines(cmd), sources, skips=skips, scope=scope, types=wanted,
                                             lead=lead):
                if obj is not None:
                    yield obj
        finally:
            skips.done()

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

    @staticmethod
    def _get_options(**kwargs):
        cmd = []
        if kwargs.get('recursive', False):
            cmd += ['-r']

        depth = kwargs.get('depth', 0)
        if depth > 0:
            cmd += ['-d', str(depth)]
        types = kwargs.get('types')
        if types:
            cmd += ['-t', types]

        properties = kwargs.get('properties')
        if 'all' in properties:
            properties = ['all']
        else:
            # `name,type` lead every object's rows (see `_lead_properties`); drop the caller's own copies (and any
            # other repeat, which zfs would print twice) so that no property appears twice for one object.
            properties = list(dict.fromkeys(['name', 'type'] + properties))
        cmd += [','.join(properties)]

        datasets = kwargs.get('datasets', False)
        if datasets:
            cmd += [datasets] if isinstance(datasets, str) else list(datasets)

        return cmd

    @staticmethod
    def _lead_properties(properties) -> tuple:
        """The rows zfs prints first for every object, for the property list `_get_options` sends: `name`, `type`
        for an explicit list (the two properties it puts first), `type` alone for `all` (zfs expands `all` in
        property-table order, whose first entry is `type`; the leading `name` placeholder is never printed)."""
        return _LEAD_ALL if 'all' in properties else _LEAD_LIST

    @staticmethod
    def _split_row(line: str, skips: _SkippedRows):
        """Split one `name,property,value,received,source` row, recovering a value that contains tabs when that is
        unambiguous; None (with a warning) otherwise."""
        cols = line.split('\t')
        if len(cols) < 5:
            skips.skip('expected 5 columns, got %d' % len(cols), line)
            return None
        name, prop, source, middle = cols[0], cols[1], cols[-1], cols[2:-1]
        if len(middle) == 2:
            return name, prop, middle[0], middle[1], source
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
    def _lines_to_objects(lines, sources, skips: Optional[_SkippedRows] = None,
                          scope: Optional[Callable[[str], bool]] = None, types: Optional[set] = None,
                          twice: Optional[set] = None, lead: tuple = _LEAD_ALL) -> Iterable[Optional[ZFS]]:
        """Group `zfs get -H -o name,property,value,received,source` rows into objects. Rows that cannot be trusted
        are skipped (see `GetCommand.__call__`). An object whose rows are not contiguous is never yielded again,
        and its name is added to `twice` so a caller holding the whole result can drop the earlier one as well.

        `lead` is the property sequence zfs prints first for every object (`GetCommand._lead_properties`). A group
        whose first rows differ is dropped whole: rows forged (through a value containing newlines) for the object
        zfs prints next would otherwise merge into its real group, but any forged prefix either does not start with
        the lead rows or repeats them, and the real group's own lead rows then repeat or follow it."""
        if skips is None:
            skips = _SkippedRows('get')
        seen = set()
        dsname, properties, props_seen, bad = None, {}, set(), False
        nrows = 0
        for line in lines:
            if not line:
                continue
            row = GetCommand._split_row(line, skips)
            if row is None:
                continue
            name, prop, value, received, source = row
            if scope is not None and not scope(name):
                skips.skip('name outside the requested objects', line)
                continue
            try:
                Validate.native_attribute(prop)
            except ValueError:
                skips.skip('invalid property name', line)
                continue
            if name != dsname:
                if dsname is not None:
                    obj = GetCommand._finish(dsname, properties, bad, skips, types)
                    if obj is not None:
                        yield obj
                    seen.add(dsname)
                dsname, properties, props_seen, bad = name, {}, set(), False
                nrows = 0
                if name in seen:
                    # zfs prints all of an object's rows together: a name that comes back was forged.
                    if twice is not None:
                        twice.add(name)
                    skips.skip('the rows of %r are not contiguous' % _clip(name))
                    bad = True
            if nrows < len(lead) and prop != lead[nrows]:
                if not bad:
                    skips.skip('the rows of %r do not start with the %r row zfs prints first (got %r)'
                               % (_clip(name), lead[nrows], _clip(prop)))
                bad = True
            nrows += 1
            if prop in props_seen:
                if not bad:
                    skips.skip('property %r appears twice for %r' % (_clip(prop), _clip(name)))
                bad = True
            props_seen.add(prop)
            if prop == 'type':
                properties[prop] = value
            elif 'all' in sources or source in sources or \
                    ('inherited' in sources and source.startswith('inherited from ')):
                properties[prop] = Property(value, source, received)
        if dsname is not None:
            obj = GetCommand._finish(dsname, properties, bad, skips, types)
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
            return GetCommand._build(name, dstype, properties)
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
                errors.append(line[0].upper() + line[1:])
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
                        errors.append(error[0].upper() + error[1:])
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
            ds: Filesystem -- ZFS filesystem to unload the key for, if None,
                unload keys for all filesystems (default None)
            location: str -- key location (default None)
            recursive: bool -- whether to unload keys for all child datasets (default False)

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
            ds: Filesystem -- ZFS filesystem to unload the key for, if None,
                unload keys for all filesystems (default None)
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
            format: str -- key format (default None)
            iterations: int -- the number of PBKDF2 iterations (default None)

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

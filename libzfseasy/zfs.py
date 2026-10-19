from libzfseasy.types import InvalidNameError, Validate, ZFS, Dataset, Filesystem, Volume, Snapshot as Snapshot, Bookmark, \
     SnapshotRange, Property, Properties
from typing import Optional, Union, Dict, Iterable, Callable

import io
import logging
import os
import shlex
import subprocess
import shutil
import threading

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

# Names already reported by _skip_invalid_name (bounded, once per distinct name per process).
_MAX_WARNED_NAMES = 1000
_warned_names = set()
_warned_lock = threading.Lock()


def _skip_invalid_name(name: str, error: Exception) -> None:
    """Log (once per distinct name per process) that a row of zfs output was skipped because its name is one
    zfs allows but libzfseasy cannot represent."""
    with _warned_lock:
        if name in _warned_names:
            return
        if len(_warned_names) >= _MAX_WARNED_NAMES:
            if len(_warned_names) == _MAX_WARNED_NAMES:
                _warned_names.add(None)
                logger.warning('skipping names from zfs output: more than %d distinct names libzfseasy cannot '
                               'represent; further ones are skipped silently', _MAX_WARNED_NAMES)
            return
        _warned_names.add(name)
    logger.warning("skipping '%s' from zfs output: libzfseasy cannot represent this name (%s)", name, error)

# How long to give an abandoned child process to honor SIGTERM before
# escalating to SIGKILL when reaping it in Command._exec_out's finally block.
_TERMINATE_TIMEOUT = 5

# Limits on the stderr a streaming child can make us keep: at most this many lines from the start and from the end
# (the middle is replaced by one marker line), each at most this many bytes.
_STDERR_KEEP_LINES = 200
_STDERR_MAX_LINE = 64 * 1024


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


class Command:

    @classmethod
    def _exec(cls, cmd):
        logger.debug('Executing command: %s', shlex.join(cmd))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        errors = []
        stderr_thread = cls._start_stderr_reader(process, errors)
        while True:
            output = cls._exec_capture(process, errors, stderr_thread)
            if output is False:
                break

    @classmethod
    def _exec_out(cls, cmd):
        logger.debug('Executing command: %s', shlex.join(cmd))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
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
            raise Exception('\n'.join(errors))
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
            raise Exception('\n'.join(self._errors)) from stream_error
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
            cmd += ['-t', cls._slist_to_str(types, validator=Validate.type)]

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

        if roots:
            cmd += [str(r) for r in cls._dslist_to_list(roots)]

        result = (cls._line_to_object(line, properties) for line in cls._exec_out(cmd))

        if lazy:
            return (o for o in result if o is not None)
        else:
            return [o for o in result if o is not None]

    @staticmethod
    def _check_sort_key(key: str) -> None:
        # `name` is a valid zfs sort key but not a property this library knows; accept it here only.
        if key != 'name':
            Validate.attribute(key)

    @staticmethod
    def _line_to_object(line: str, properties: Iterable):
        zfs_info = line.split('\t')
        zfs_info = [i if not i == '-' else None for i in zfs_info]
        name, dstype = zfs_info[0:2]
        # The property names were validated strictly when the -o list was built, and a column such as `volsize` is
        # `-` on rows whose type lacks it, so apply the values leniently (as GetCommand._build does).
        try:
            obj = ZFS.from_name(name, dstype)
        except InvalidNameError as e:
            _skip_invalid_name(name, e)
            return None
        if len(zfs_info) > 2:
            obj.update(dict(zip(properties[2:], zfs_info[2:])), lenient=True)
        return obj


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
                cmd += ['-o', k + '=' + str(v.value if isinstance(v, Property) else v)]
        cmd += [str(snapshot), str(dataset)]
        cls._exec(cmd)

        props = properties.keys() if properties else None
        result = ListCommand()(dataset, recursive=True, properties=props)
        if len(result) == 1:
            return result[0]

        return result


class GetCommand(Command, StringListArgument, ZFSListArgument):

    def __call__(self, *args, **kwargs):
        return self._get(*args, **kwargs)

    @classmethod
    def _get(cls, ds: Optional[DatasetList] = None, properties: Optional[StringList] = 'all',
             types: Optional[StringList] = 'all', sources: Optional[StringList] = 'all', recursive: bool = False,
             depth: int = 0, lazy=False) -> Iterable[ZFS]:

        sources = cls._slist_to_list(sources, validator=Validate.source)
        types = cls._slist_to_str(types, validator=Validate.type)
        properties = cls._slist_to_list(properties, validator=Validate.attribute)
        datasets = [str(d) for d in cls._zlist_to_list(ds)]
        # Enforce column order: name, property, value, received, source
        cmd = [_zfs_cmd(), 'get', '-H', '-o', 'name,property,value,received,source'] + \
            cls._get_options(recursive=recursive, depth=depth, types=types, properties=properties, datasets=datasets)
        result = cls._lines_to_objects((line for line in cls._exec_out(cmd)), sources)

        if lazy:
            return (o for o in result if o is not None)
        else:
            return [o for o in result if o is not None]

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
            properties = ['name', 'type'] + properties
        cmd += [','.join(properties)]

        datasets = kwargs.get('datasets', False)
        if datasets:
            cmd += [datasets] if isinstance(datasets, str) else list(datasets)

        return cmd

    @staticmethod
    def _lines_to_objects(lines, sources) -> Iterable[Datasets]:
        name, dsname, properties = '', '', {}
        for line in lines:
            if not line:
                continue
            name, prop, value, received, source = line.split('\t')
            if not dsname:
                dsname = name
            elif name != dsname:
                properties.pop('name', None)
                dstype = properties.pop('type', None)
                if name and dstype:
                    obj = GetCommand._build(dsname, dstype, properties)
                    if obj is not None:
                        yield obj
                dsname, properties = name, {}
            if prop == 'type':
                properties[prop] = value
            elif 'all' in sources or source in sources or \
                    ('inherited' in sources and source.startswith('inherited from ')):
                properties[prop] = Property(value, source, received)
        properties.pop('name', None)
        dstype = properties.pop('type', None)
        if name and dstype:
            obj = GetCommand._build(dsname, dstype, properties)
            if obj is not None:
                yield obj

    @staticmethod
    def _build(name: str, dstype: str, properties: Properties) -> Optional[ZFS]:
        # The property names come from zfs itself (the caller's own names were validated strictly before the command
        # ran), so accept well-formed native names this library does not know yet (a newer OpenZFS adding a property
        # must not break `zfs get all`).
        # A name zfs allows but libzfseasy refuses is skipped (None) with a warning; the caller's properties for that
        # dataset are dropped with it, never merged into a neighbour.
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
        """
        cmd = [_zfs_cmd(), 'send'] + cls._get_options(embed=embed) + ['-t', token]
        return cls._exec_stream(cmd)

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
        logger.debug('Executing command: %s', shlex.join(cmd))
        errors = _BoundedErrors()
        stderr_thread = None
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
            raise Exception('\n'.join(errors))
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
            reset: Properties -- properties to reset on the target filesystem (default None)
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
            reset: Properties -- properties to reset on the target filesystem (default None)
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
            cmd += cls._get_props(props, True)
        reset = kwargs.get('reset', None)
        if reset:
            for s in cls._slist_to_list(reset):
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
        logger.debug('Executing command: %s', shlex.join(cmd))
        # Only stdin (the stream we write to) and stderr (for error
        # reporting) are ever read from; stdout is intentionally discarded
        # (not piped) since nothing drains it and a chatty child could
        # otherwise fill the pipe and block on write(), deadlocking close()'s
        # process.wait().
        errors = _BoundedErrors()
        stderr_thread = None
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            # stderr is drained on a thread from the start (a receive writing a lot of diagnostics must not block
            # on a full pipe while the caller is still writing the stream); close() joins it before raising.
            stderr_thread = cls._start_stderr_reader_bin(process, errors)
            # Poll once to catch immediate startup failures; zfs receive emits no stderr while waiting for data
            rc = process.poll()
            if rc is not None and rc != 0:
                cls._join_stderr_reader(stderr_thread)
                raise Exception('\n'.join(errors) or f'Command failed with exit code {rc}')
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
            cmd += ['-o', 'keylocation=' + location]
        if fmt:
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
        return cls._get_flags(flags) + cls._get_props(properties, True)

    @staticmethod
    def _get_flags(flags: Optional[StringList]):
        if not flags:
            return []
        if isinstance(flags, str):
            flags = flags.split(',')
        result = []
        for f in flags:
            result += ['-o', str(f).strip()]
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

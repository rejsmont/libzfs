"""Local destinations: back a dataset up to another dataset on this host.

A destination `url: zfs:<target>` (`Destination.is_local`) is sent to with a
direct `zfs send` -> `zfs receive` on this host, without HTTP, TLS or
enrollment. The copies live where a server would put them,
`<target>/<client_id>/<pool>/<path>` (`receiver.server_dataset`), carry the
same locally owned properties (`org.zfsbackup:client_id`, `source_dataset`,
`config`) and the same forced lock-down set (`receiver.receive_properties`, `-h`,
`-u`), so the received-dataset pruner and `zfs rename` work on them unchanged.

`LocalBackupManager` is a `chain.ChainSender`: the chain (send mode, rollback
permit, `-i` steps, resume, pins, anchors) is shared with the remote manager;
this module supplies the transport -- `_ready_transport`, `_negotiate`,
`_fetch_resume_token`, `_stream_step` -- doing on this host, in the same order,
what the server's `/negotiate` and `/stream` handlers do (ownership -> the owned
values -> the stream header -> the `-F` decision -> the abort of a partial
receive -> the spawn). There is no server behind it as a second line of
defence, so the receive-side checks (`decide_receive` above all) run here.

Every non-streaming `zfs` call of those checks runs under one time budget per
phase (`LOCAL_ZFS_BUDGET`, through `libzfseasy.timeout`): a suspended target
pool skips the destination for the cycle instead of wedging the worker. The
stream itself is never timed; `_Pump` copies it on the worker thread with
non-blocking descriptors and `select` in bounded slices and stops both children
after `LOCAL_STALL_TIMEOUT` without progress.

No HTTP, TLS or Flask import: only the chain, the receive helpers and libzfseasy.
"""

import contextlib
import fcntl
import logging
import os
import selectors
import shlex
import time
from typing import Callable, Dict, List, NamedTuple, Optional, Tuple

import libzfseasy as zfs
from libzfseasy import ZFSTimeoutError
from libzfseasy.types import Dataset, Filesystem

from zfsbackup import receiver
from zfsbackup.backup_manager import (
    PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET, DatasetInfo, DatasetManager,
)
from zfsbackup.chain import ChainSender, NegotiationFailed, _FAILED, _OK, _STOP, _SendStream
from zfsbackup.config import BackupConfig, Destination


logger = logging.getLogger(__name__)

#: The time budget (seconds) of the non-streaming `zfs` calls of one phase of
#: the receive-side checks: the target check, a negotiate, a resume-token read,
#: and a stream's checks before and after its header (each its own budget).
#: Every command runs under `libzfseasy.timeout(<what is left>)`; an expired
#: budget skips the destination for this cycle (`ZFSTimeoutError`).
LOCAL_ZFS_BUDGET = 60.0

#: No byte moved between `zfs send` and `zfs receive` for this long (seconds)
#: -> both are stopped (`StreamHandle.terminate()`); `-s` keeps the partial
#: receive for a resume. Also bounds the wait for the stream's first record
#: (`zfs send` is started without libzfseasy's blocking start-up peek,
#: `LocalBackupManager._send_peek = False`, so a send stuck before its first byte
#: on a suspended source pool is cut here too) and, after a failure or the end
#: of the stream, for a child to exit by itself.
LOCAL_STALL_TIMEOUT = 600.0

#: After the last byte, how long (seconds) `zfs receive` may take to commit
#: before it is stopped: the same bound the remote client gives a server's
#: final acknowledgement (`remote.WS_FINAL_ACK_TIMEOUT`), since a large
#: receive (or the `-F` rollback it was allowed) commits after its input ends.
#: The final commit cannot be interrupted: a receive stopped at the bound may
#: still complete it, so the copy may hold the snapshot afterwards.
LOCAL_COMMIT_TIMEOUT = 3600.0

#: How long (seconds) a copy that negotiate found missing counts as "first
#: backup not completed yet" for its children (the server's
#: `api.PENDING_CONFIG_TTL`): while it does, a child does not create it as a
#: plain container, which a raw (or encrypted-target) full stream of the
#: parent could never replace. Refreshed by every negotiate of the parent.
LOCAL_PENDING_TTL = 600.0

#: The pump moves at most this many bytes per slice (and keeps at most this
#: many in memory); the pipes are grown to it where the kernel allows.
PUMP_CHUNK = 1 << 20

#: One `select` slice of the pump (seconds): the stall bound is checked
#: between slices, never inside a blocked call.
PUMP_SLICE = 1.0

#: How often (seconds) the pump polls a child it waits for to exit.
PUMP_POLL_SLICE = 0.1

#: Most copy snapshots named in `server_newer` (as the server's
#: `api.SERVER_NEWER_MAX`); the count is always complete.
SERVER_NEWER_MAX = 64

#: Logged once per destination while the supervisor handed over no client id.
NO_CLIENT_ID_MESSAGE = (
    "no client id (the copies are laid out under it); set one with "
    "`zfsbackup-config identity set-client-id <id>`"
)

#: The text zfs gives when a receive would overwrite an existing dataset with
#: an encrypted stream, or an encrypted one (as `api._ENCRYPTED_OVERWRITE_MARKER`).
_ENCRYPTED_OVERWRITE_MARKER = 'overwrite an unencrypted one with an encrypted one'

#: What the operator should do about the stream refusals that need more than
#: the message (as `remote._STREAM_REFUSAL_HINTS`, worded for this host).
_LOCAL_REFUSAL_HINTS = {
    'stream_header': (
        " (zfs send produced a stream this daemon does not accept: a bug; report it with "
        "`zfsbackupd --version`)"
    ),
    'base_mismatch': (
        " (the stream is not incremental from the snapshot this daemon chose: a bug; "
        "report it with `zfsbackupd --version`)"
    ),
    'type_mismatch': (
        " (this host holds a dataset of the other kind under that name: `zfs rename` it "
        "to a name outside the local target; the next cycle sends a full stream)"
    ),
}


class _LocalTarget(NamedTuple):
    """A local destination's transport: its target dataset and this host's
    client id (the copies live under `<target>/<client_id>`)."""
    target: str
    client_id: str

    @property
    def base(self) -> str:
        return f"{self.target}/{self.client_id}"


class _Budget:
    """A `zfs_call` (`receiver`'s convention: `call(fn, *args, **kwargs)`)
    that runs every command under `libzfseasy.timeout(<what is left of
    LOCAL_ZFS_BUDGET>)`; once it is spent it raises `ZFSTimeoutError` without
    running anything. Run synchronously, never for `send`/`receive`.

    Same contract as `api.ZFSBudget` (not shared: that one is the server's, with
    its own constant and `remaining()`). The limit applies per `zfs` command: a
    helper that runs several (`read_zfsbackup_properties` runs up to three)
    can overrun by its extra commands, each limited to what was left when the
    helper started, and the kill of a timed-out child adds up to 4 s. A child
    that survives SIGKILL stays registered in this process (`_stuck_problem`),
    and every later timed call here fails fast until it exits."""

    def __init__(self, seconds: Optional[float] = None, clock: Callable[[], float] = time.monotonic):
        self.seconds = LOCAL_ZFS_BUDGET if seconds is None else seconds
        self._clock = clock
        self._deadline = clock() + self.seconds

    def __call__(self, fn, *args, **kwargs):
        left = self._deadline - self._clock()
        if left <= 0:
            raise ZFSTimeoutError(
                f"the {self.seconds:g} s budget for the zfs checks on this host is spent"
            )
        with zfs.timeout(left):
            return fn(*args, **kwargs)


def _stuck_problem() -> Optional[str]:
    """Why no timed `zfs` call can run in this process now, or `None`: a
    child that survived SIGKILL (`libzfseasy.stuck_children()`) makes every
    later timed call fail fast until it exits. Never raises."""
    try:
        stuck = zfs.stuck_children()
    except Exception:
        return None
    if not stuck:
        return None
    listing = ', '.join(f"pid {c.pid} ({c.label})" for c in stuck[:3])
    if len(stuck) > 3:
        listing += f", and {len(stuck) - 3} more"
    return (
        f"{len(stuck)} zfs process(es) of this worker did not exit after SIGKILL ({listing}; "
        "a hung pool?); timed zfs calls in this worker are refused until they exit, so local "
        "destinations are skipped until then"
    )


def _grow_pipe(fd: int) -> None:
    """Ask the kernel for a `PUMP_CHUNK` pipe buffer (Linux `F_SETPIPE_SZ`), so
    one slice can move that much; silently kept as it is where that is not
    available or allowed."""
    setsz = getattr(fcntl, 'F_SETPIPE_SZ', None)
    if setsz is None:
        return
    with contextlib.suppress(OSError):
        fcntl.fcntl(fd, setsz, PUMP_CHUNK)


class _Stalled(Exception):
    """Nothing moved for `LOCAL_STALL_TIMEOUT`."""


#: `_Pump.run` outcomes.
PUMP_OK = 'ok'
PUMP_RECEIVE_GONE = 'receive-gone'
PUMP_SEND_FAILED = 'send-failed'
PUMP_STALLED = 'stalled'


class _Pump:
    """Carries one `zfs send` stream into one `zfs receive` on the calling
    (worker) thread, never blocking in a `read()` or `write()`.

    Both pipe descriptors are made non-blocking and driven by a selector in
    slices of `PUMP_SLICE`, so the stall bound (`LOCAL_STALL_TIMEOUT`, no byte
    moved) is checked between slices whatever the children do: a child in
    uninterruptible sleep on a hung pool keeps its pipe end open, so EOF or
    EPIPE never comes, and nothing here waits for them.

    The send side is a libzfseasy `StreamHandle` over a `BufferedReader`,
    started with `peek=False` (nothing has been read yet, so the buffer is
    normally empty; with the default peek it already holds the first bytes).
    `prepare()` makes the descriptor non-blocking and then drains that buffer
    exactly once (`read1(-1)` returns every buffered byte without touching the
    descriptor when any are buffered, and does one non-blocking read when none
    are; the buffer is empty afterwards either way), so no byte is lost or
    reordered; from then on only the descriptor is read. A `zfs send` that fails
    at once is just EOF with no bytes: `read_head` returns `b''`, and the
    failure surfaces from `close()` (`finish_send()` / `send_error`). The receive side is written
    only at its descriptor: nothing ever enters its `BufferedWriter`, so
    closing it (the EOF `zfs receive` waits for) flushes nothing.

    Lifecycle: `prepare()` -> `read_head(n)` (the stream's first record, kept
    in the buffer to be fed first) -> `attach_receive(handle)` -> `run()` ->
    `close()`. `close()` is idempotent and must always run (`try`/`finally`):
    it unregisters the descriptors, stops a child still running
    (`terminate()`, bounded) and only then calls `close()` on each handle
    (the send first, the receive in a `finally`), keeping their errors in
    `send_error` / `receive_error` (the children's stderr is in them). It
    never closes a descriptor itself."""

    def __init__(self, send, clock: Callable[[], float] = time.monotonic):
        self.send = send
        self.receive = None
        self._clock = clock
        self._sel: Optional[selectors.BaseSelector] = None
        self._registered: Dict[int, int] = {}
        self._buf = bytearray()
        self._send_fd: Optional[int] = None
        self._recv_fd: Optional[int] = None
        self._recv_stream = None
        self._send_eof = False
        self._last_progress = clock()
        self._terminated: set = set()
        self._closed: set = set()
        #: `(side, pid)` of a child that survived SIGKILL (`terminate()`
        #: returned True): it is in `libzfseasy.stuck_children()`.
        self.abandoned: List[Tuple[str, Optional[int]]] = []
        self.send_error: Optional[str] = None
        self.receive_error: Optional[str] = None
        #: `run()` reached the end of the stream, but `zfs send` did not exit
        #: within `LOCAL_STALL_TIMEOUT` after it and was stopped.
        self.send_cut_after_eof = False
        #: `run()` returned `PUMP_STALLED` because `zfs receive` did not exit
        #: within `LOCAL_COMMIT_TIMEOUT` after its input ended (it may still
        #: complete its commit), not because no byte moved.
        self.commit_timed_out = False

    # ------------------------------------------------------------ set-up

    def prepare(self) -> None:
        """Make the send descriptor non-blocking and take what the stream's
        buffer already holds (see the class docstring). Starts the stall clock
        for the stream's first record (the pump is built before the receive-side
        checks, which may take a while)."""
        self._last_progress = self._clock()
        self._sel = selectors.DefaultSelector()
        fd = self.send.fileno()
        os.set_blocking(fd, False)
        _grow_pipe(fd)
        self._send_fd = fd
        read1 = getattr(self.send, 'read1', None)
        if read1 is not None:
            try:
                data = read1(-1)
            except BlockingIOError:
                data = b''
            if data:
                self._buf += data
                self._last_progress = self._clock()
            # `b''` is EOF or "nothing yet": the descriptor reads tell which.

    def attach_receive(self, receive) -> None:
        """Take over the started `zfs receive` (recorded first, so `close()`
        reaps it even if the set-up below fails). Restarts the stall clock: the
        checks between the header and the receive are not stream silence."""
        self._last_progress = self._clock()
        self.receive = receive
        # The context-manager value of a `StreamHandle` is the binary stream
        # it wraps (`__enter__` does nothing else); closing that stream is the
        # EOF `zfs receive` waits for.
        self._recv_stream = receive.__enter__()
        fd = receive.fileno()
        os.set_blocking(fd, False)
        _grow_pipe(fd)
        self._recv_fd = fd

    # ---------------------------------------------------------- selector

    def _interest(self, wanted: Dict[int, int]) -> None:
        for fd in list(self._registered):
            if fd not in wanted:
                self._sel.unregister(fd)
                del self._registered[fd]
        for fd, events in wanted.items():
            current = self._registered.get(fd)
            if current is None:
                self._sel.register(fd, events)
            elif current != events:
                self._sel.modify(fd, events)
            self._registered[fd] = events

    def _unregister_all(self) -> None:
        if self._sel is None:
            return
        for fd in list(self._registered):
            with contextlib.suppress(Exception):
                self._sel.unregister(fd)
            del self._registered[fd]

    # ------------------------------------------------------------- moves

    def _fill(self) -> int:
        """Read from `zfs send` until it would block, it ends, or the buffer
        holds `PUMP_CHUNK`. -> bytes read."""
        moved = 0
        while not self._send_eof and len(self._buf) < PUMP_CHUNK:
            try:
                data = os.read(self._send_fd, PUMP_CHUNK - len(self._buf))
            except BlockingIOError:
                break
            if not data:
                self._send_eof = True
                break
            self._buf += data
            moved += len(data)
        return moved

    def _drain(self) -> int:
        """Write the buffer to `zfs receive` until it would block or the
        buffer is empty. -> bytes written. `BrokenPipeError` (the receive
        is gone) propagates."""
        moved = 0
        while self._buf:
            try:
                with memoryview(self._buf) as view:
                    n = os.write(self._recv_fd, view)
            except BlockingIOError:
                break
            del self._buf[:n]
            moved += n
        return moved

    def _tick(self, moved: int) -> None:
        now = self._clock()
        if moved:
            self._last_progress = now
        elif now - self._last_progress > LOCAL_STALL_TIMEOUT:
            raise _Stalled()

    def read_head(self, size: int) -> bytes:
        """At least `size` bytes of the stream (fewer only if it ended; `b''`
        for a send that ended without a byte, whose error `finish_send()`
        reports), kept in the buffer to be fed to `zfs receive` first,
        unchanged. Raises `_Stalled` after `LOCAL_STALL_TIMEOUT` without a byte,
        also before the first one."""
        while len(self._buf) < size and not self._send_eof:
            self._interest({self._send_fd: selectors.EVENT_READ})
            self._sel.select(PUMP_SLICE)
            self._tick(self._fill())
        self._interest({})
        return bytes(self._buf)

    # -------------------------------------------------------------- run

    def run(self) -> str:
        """Copy the rest of the stream, close the receive's input at the send's
        EOF and wait for both children. -> `PUMP_OK` (both exited; their
        status is `close()`'s to report), `PUMP_RECEIVE_GONE` (the receive
        stopped reading: the send was stopped), `PUMP_SEND_FAILED` (the send
        could not be read: the receive was given EOF) or `PUMP_STALLED` (both
        stopped). Every child has exited or been stopped when it returns.
        `PUMP_STALLED` after the stream's end is `commit_timed_out`; a send
        stopped after its end is `send_cut_after_eof`."""
        self._last_progress = self._clock()
        try:
            while not (self._send_eof and not self._buf):
                wanted = {}
                if not self._send_eof and len(self._buf) < PUMP_CHUNK:
                    wanted[self._send_fd] = selectors.EVENT_READ
                if self._buf:
                    wanted[self._recv_fd] = selectors.EVENT_WRITE
                self._interest(wanted)
                self._sel.select(PUMP_SLICE)
                # Both sides are tried whatever the selector said: every call
                # is non-blocking, and a ready read is usually followed by
                # room to write.
                moved = self._fill()
                moved += self._drain()
                if not moved:
                    rc = self.receive.poll()
                    if rc is not None and (rc != 0 or self._buf):
                        # Exited before taking the whole stream (a write
                        # would get EPIPE; with nothing to write it might
                        # never come). A receive that exited 0 with nothing
                        # left to give it has read the end record: the
                        # send's EOF follows.
                        return self._receive_gone()
                self._tick(moved)
        except BrokenPipeError:
            return self._receive_gone()
        except _Stalled:
            return self._stalled()
        except OSError:
            # A read of `zfs send`'s pipe failed (a write failure other than
            # EPIPE lands here too: treated alike, the receive gets EOF).
            logger.debug("local stream pump: pipe error", exc_info=True)
            return self._send_failed()
        # The whole stream is in `zfs receive`'s pipe: its EOF.
        self._interest({})
        self._close_receive_input()
        self.send_cut_after_eof = self._await_exit('send', LOCAL_STALL_TIMEOUT) is None
        if self._await_exit('receive', LOCAL_COMMIT_TIMEOUT) is None:
            self.commit_timed_out = True
            return PUMP_STALLED
        return PUMP_OK

    def _close_receive_input(self) -> None:
        stream, self._recv_stream = self._recv_stream, None
        if stream is not None:
            with contextlib.suppress(OSError, ValueError):
                stream.close()

    def _receive_gone(self) -> str:
        self._interest({})
        self._terminate('send')
        self._close_receive_input()
        self._await_exit('receive', LOCAL_STALL_TIMEOUT)
        return PUMP_RECEIVE_GONE

    def _send_failed(self) -> str:
        self._interest({})
        self._terminate('send')
        # `-s` keeps what was received for a resume; the receive fails on the
        # truncated stream by itself.
        self._close_receive_input()
        self._await_exit('receive', LOCAL_STALL_TIMEOUT)
        return PUMP_SEND_FAILED

    def _stalled(self) -> str:
        self._interest({})
        self._terminate('send')
        self._terminate('receive')
        return PUMP_STALLED

    # ---------------------------------------------------------- children

    def _handle(self, side: str):
        return self.send if side == 'send' else self.receive

    def _terminate(self, side: str) -> None:
        """`terminate()` the child of `side` if it is still running (bounded,
        ~10 s; a child that survives SIGKILL is recorded in `abandoned`)."""
        handle = self._handle(side)
        if handle is None or side in self._terminated:
            return
        if handle.poll() is not None:
            return
        self._terminated.add(side)
        if handle.terminate():
            self.abandoned.append((side, getattr(handle, 'pid', None)))

    def _await_exit(self, side: str, bound: float) -> Optional[int]:
        """Poll the child of `side` in `PUMP_POLL_SLICE` slices until it exits
        (-> its return code) or `bound` seconds pass (-> `None`, after
        `terminate()`)."""
        handle = self._handle(side)
        if handle is None:
            return None
        started = self._clock()
        while True:
            rc = handle.poll()
            if rc is not None:
                return rc
            if self._clock() - started >= bound:
                self._terminate(side)
                return None
            time.sleep(PUMP_POLL_SLICE)

    def finish_send(self) -> Optional[str]:
        """Before a receive exists: end and reap `zfs send` (waited for when
        its output already ended, stopped otherwise) and return its error, else
        `None`. An error caused by our own stop (the signal it was killed by) is
        not the send's: it stays in `send_error` but is not returned."""
        self._interest({})
        if self._send_eof:
            self._await_exit('send', LOCAL_STALL_TIMEOUT)
        else:
            self._terminate('send')
        self._close_side('send')
        return None if 'send' in self._terminated else self.send_error

    def _close_side(self, side: str) -> None:
        handle = self._handle(side)
        if handle is None or side in self._closed:
            return
        self._closed.add(side)
        # `close()` is an unbounded wait unless the child has exited or was
        # terminated (`StreamHandle.poll`).
        self._terminate(side)
        try:
            handle.close()
        except Exception as e:
            if side == 'send':
                self.send_error = str(e)
            else:
                self.receive_error = str(e)

    def close(self) -> None:
        """Release everything (idempotent): see the class docstring."""
        try:
            self._unregister_all()
            if self._sel is not None:
                with contextlib.suppress(Exception):
                    self._sel.close()
            self._close_side('send')
        finally:
            self._close_side('receive')


class LocalBackupManager(ChainSender):
    """Sends datasets to local destinations (`url: zfs:<target>`) with a direct
    `zfs send` -> `zfs receive` on this host, into
    `<target>/<client_id>/<pool>/<path>`.

    `client_id` is this host's id from the supervisor's config snapshot
    (`daemon.Handoff.client_id`); without one nothing is sent (the layout
    needs it). A worker calls `begin_cycle()` once per pass over its
    datasets: the target dataset's existence is then checked once per cycle
    per destination."""

    _logger = logger

    _copy_noun = "local copy"
    _copy_placeholder = "<local copy>"
    _receiver = "the local copy"
    _on_receiver = "on this host"
    _taken_on_receiver = "taken in the local copy"
    _at_receiver = "in the local copy"
    _sender = "the source dataset"
    _busy_prefix = "local copy is busy"
    _rename_doc = "README 'Local destinations'"
    _rejoin_doc = None
    _refusal_label = "local refusal"
    # The pump bounds the first record: no blocking peek in the worker thread.
    _send_peek = False

    def __init__(
        self,
        config: BackupConfig,
        local_manager: DatasetManager,
        *,
        client_id: Optional[str],
    ):
        super().__init__(config, local_manager, client_id=client_id)
        # destination name -> target dataset found to exist this cycle.
        self._checked: Dict[str, str] = {}
        # destinations whose target was found gone mid-cycle: skipped (silently,
        # it was logged) for the rest of the cycle.
        self._unavailable: set = set()
        self._per_cycle = False
        # copy name -> `time.monotonic()` of the negotiate that found it
        # missing (`LOCAL_PENDING_TTL`).
        self._pending: Dict[str, float] = {}

    # ----------------------------------------------------------- the cycle

    def begin_cycle(self) -> None:
        """Start a new pass over the datasets: each local target is checked
        again (once) this cycle."""
        self._per_cycle = True
        self._checked.clear()
        self._unavailable.clear()

    def _before_destination_check(self) -> None:
        if not self._per_cycle:
            self._checked.clear()
            self._unavailable.clear()

    def _where(self, transport: _LocalTarget) -> str:
        return f"local {transport.target}"

    def _peer(self, transport: _LocalTarget) -> str:
        return f"local target {transport.target}"

    # ------------------------------------------------------------ messages

    def _zfs_timed_out(self, dsi: DatasetInfo, destination: str, where: str, exc) -> None:
        """A check's `zfs` did not answer within the budget (or was refused
        while a child is stuck): WARNING once per episode, the destination is
        skipped this cycle."""
        self._call_note[(dsi.name, destination)] = (
            f"skipped: zfs did not answer in time on this host ({where})"
        )
        self._log_warning_once(
            (f"{dsi.name} [local zfs timeout]", destination),
            f"{dsi.name} -> {destination}: zfs did not answer within {LOCAL_ZFS_BUDGET:g} s on "
            "this host; skipping this destination this cycle (a hung or suspended pool leaves "
            "zfs processes in uninterruptible sleep)",
        )
        self._logger.debug(f"{dsi.name} -> {destination}: zfs timed out {where}: {exc}")

    def _zfs_recovered(self, dsi: DatasetInfo, destination: str) -> None:
        if self._refusal_logged.pop((f"{dsi.name} [local zfs timeout]", destination), None):
            self._logger.info(f"{dsi.name} -> {destination}: zfs answers again on this host")

    @staticmethod
    def _owner_message(copy: str, dataset: str, client_id: str, verdict) -> str:
        """The refusal for a copy (or container) this host may not use, from
        `receiver`'s verdict, worded for this host."""
        if verdict.unowned:
            return (
                f"{copy} already exists on this host and is not recorded as a backup "
                f"({PROP_CLIENT_ID} is not set on it); refusing to adopt it. If it is meant to "
                "hold this host's backups, adopt it on purpose: "
                f"zfs set {PROP_CLIENT_ID}={client_id} {shlex.quote(copy)}"
            )
        if verdict.source_mismatch:
            return (
                f"{copy} already holds backups of another dataset ({PROP_SOURCE_DATASET} "
                f"differs); refusing to mix in {dataset!r}. Move it aside with `zfs rename`"
            )
        return (
            f"{copy} is recorded as another host's backup ({PROP_CLIENT_ID} differs from this "
            f"host's client id {client_id!r}); refusing to use it. Copies made under an "
            "earlier client id of this host are no longer used"
        )

    def _refuse_stream(self, dsi: DatasetInfo, destination: str, reason: str, message: str) -> None:
        """A stream this host does not receive: ERROR once per episode
        (cleared by the next completed step, `_record_step`)."""
        self._log_refusal_once(
            (f"{dsi.name} [{self._refusal_label}]", destination),
            f"{dsi.name} -> {destination}: nothing received ({reason}): {message}"
            f"{_LOCAL_REFUSAL_HINTS.get(reason, '')}",
        )

    @staticmethod
    def _receive_error(text: str, copy: str) -> str:
        if _ENCRYPTED_OVERWRITE_MARKER in text:
            return (
                f"{copy} already exists on this host and cannot receive a full encrypted stream "
                "(or a full stream under an encrypted target) over it. It is either a "
                "container made when a dataset below it was backed up first (back the parent "
                "up first, or rename the container aside; that moves the copies of its "
                "children too, so they are sent in full again), or a copy of the other kind "
                "(plain vs raw): `zfs rename` it aside; the next cycle sends a full stream. "
                + text
            )
        return text

    # ---------------------------------------------------------- transport

    def _ready_transport(self, destination: str, dest: Destination) -> Optional[_LocalTarget]:
        """The destination's `_LocalTarget` if it may be used now: a local
        destination, a client id, no stuck `zfs` child in this worker and an
        existing target dataset (checked once per cycle). `None` (logged once
        per episode) means skip the destination, before anything is sent."""
        key = (None, destination)
        if getattr(dest, 'is_local', False) is not True:
            self._log_refusal_once(key, (
                f"destination {destination} ({dest.url}) is not a local destination; the "
                "local backup manager does not send to it"
            ))
            return None
        if self.client_id is None:
            self._log_refusal_once(
                key, f"local backup to {destination} skipped: {NO_CLIENT_ID_MESSAGE}"
            )
            return None
        problem = _stuck_problem()
        if problem is not None:
            self._log_warning_once(key, f"local backup to {destination} skipped: {problem}")
            return None
        if destination in self._unavailable:
            # Found gone by an earlier dataset this cycle (logged then).
            return None
        target = dest.target_dataset
        if self._checked.get(destination) != target:
            try:
                exists = _Budget()(zfs.exists, Dataset(target))
            except ZFSTimeoutError as e:
                self._log_warning_once(key, (
                    f"local backup to {destination} skipped: zfs did not answer within "
                    f"{LOCAL_ZFS_BUDGET:g} s on this host while checking the target {target} "
                    "(a hung or suspended pool?); the next cycle retries"
                ))
                self._logger.debug(f"local target {target}: {e}")
                return None
            except Exception as e:
                self._log_refusal_once(key, (
                    f"local backup to {destination} skipped: cannot check the target {target}"
                    " (see the DEBUG line); the next cycle retries"
                ))
                self._logger.debug(f"local target {target}: {e}")
                return None
            if not exists:
                self._log_refusal_once(key, (
                    f"local destination {destination}: the target dataset {target} does not "
                    f"exist; create it (for example `zfs create -o canmount=off {target}`), or "
                    "import its pool if it is on an exported one (a removable disk). "
                    "Nothing is sent to it until then"
                ))
                return None
            if self._per_cycle:
                self._checked[destination] = target
        if self._refusal_logged.pop(key, None) is not None:
            self._logger.info(f"local destination {destination} ({target}) is usable again")
        return _LocalTarget(target, self.client_id)

    def _target_vanished(self, transport: _LocalTarget, dsi: DatasetInfo, destination: str) -> bool:
        """After an unexpected zfs failure: is the target dataset gone (its pool
        exported or destroyed mid-cycle)? If it is, the destination is marked
        unavailable for the rest of this cycle and ONE message is logged per
        episode (not an ERROR per remaining dataset); -> True, and the caller
        skips quietly. Any doubt (the check fails or times out too) -> False, and
        the caller reports its own failure."""
        target = transport.target
        try:
            exists = _Budget()(zfs.exists, Dataset(target))
        except Exception as e:
            self._logger.debug(f"local target {target}: re-check after a failure: {e}")
            return False
        if exists:
            return False
        self._unavailable.add(destination)
        self._checked.pop(destination, None)
        self._call_note[(dsi.name, destination)] = f"skipped: the target dataset {target} is gone"
        self._log_refusal_once((None, destination), (
            f"local destination {destination}: the target dataset {target} is gone (its pool "
            "exported or destroyed?); skipping this destination for the rest of this cycle. "
            f"Import the pool, or recreate the target (`zfs create -o canmount=off {target}`)"
        ))
        return True

    def _copy_name(self, transport: _LocalTarget, client_id: str, dsi: DatasetInfo, destination: str) -> Optional[str]:
        """The copy's name, or `None` (ERROR once) for a dataset name that
        cannot be laid out under the target."""
        try:
            return receiver.server_dataset(transport.target, client_id, dsi.name)
        except receiver.BadDatasetName as e:
            self._log_refusal_once((dsi.name, destination), f"{dsi.name} -> {destination}: {e}")
            return None

    def _ancestors_state(
        self, call, copy: str, base: str, client_id: str,
    ) -> Tuple[Optional[Tuple[str, str]], list, Optional[str]]:
        """`(unsettled, missing, conflict)` for the ancestors of `copy` up to
        `base` (`<target>/<id>`), as the server's `_ancestors_state` without
        claims: `unsettled` is `(ancestor, 'partial')` for an existing one
        holding a new first receive's partial state, or `(ancestor,
        'pending')` for a missing one whose own first backup has not completed
        (`LOCAL_PENDING_TTL`); `missing` lists the missing ancestors nearest
        first; `conflict` is the refusal for the nearest existing one that is
        not this host's container. Raises if zfs fails."""
        ancestor = copy.rsplit('/', 1)[0]
        missing: list = []
        while ancestor == base or ancestor.startswith(base + '/'):
            ds = Dataset(ancestor)
            if call(zfs.exists, ds):
                verdict = receiver.owner_verdict(
                    ancestor, receiver.local_client_id(call, ds, lib=zfs), client_id
                )
                if verdict.message is not None:
                    return None, missing, self._owner_message(ancestor, '', client_id, verdict)
                if receiver.ancestor_holds_partial_receive(call, ds, lib=zfs):
                    return (ancestor, 'partial'), missing, None
            elif self._is_pending(ancestor):
                return (ancestor, 'pending'), missing, None
            else:
                missing.append(ancestor)
            if ancestor == base:
                break
            ancestor = ancestor.rsplit('/', 1)[0]
        return None, missing, None

    def _is_pending(self, copy: str) -> bool:
        since = self._pending.get(copy)
        if since is None:
            return False
        if time.monotonic() - since < LOCAL_PENDING_TTL:
            return True
        del self._pending[copy]
        return False

    def _negotiate(
        self, transport: _LocalTarget, client_id: str, dsi: DatasetInfo, snapshots,
        destination: str,
    ) -> Optional[str]:
        """What the server's negotiate does, on this host: the ownership
        check, the missing ancestors created one level at a time (or the wait
        for an unsettled one), the owned values set on an existing copy, then a
        strict listing. Always fills `_server_info`, `_server_newer` and
        `_server_newer_status` on success. -> the newest snapshot both sides
        hold, or `None`. Raises `NegotiationFailed` (logged here; a zfs
        timeout is `zfs_timeout=True`)."""
        key = (dsi.name, destination)
        copy = self._copy_name(transport, client_id, dsi, destination)
        if copy is None:
            raise NegotiationFailed(f"invalid dataset name {dsi.name!r}")
        try:
            canonical = receiver.canonical_dataset_config(dsi.config.to_property(destination))
        except ValueError as e:
            self._log_refusal_once(key, (
                f"{dsi.name} -> {destination}: its configuration cannot be stored on the "
                f"local copy ({e}); nothing is sent"
            ))
            raise NegotiationFailed(str(e))
        copy_fs = Filesystem(copy)
        call = _Budget()
        dry_run = self.config.dry_run
        try:
            exists = call(zfs.exists, copy_fs)
            if exists:
                verdict = receiver.ownership_conflict(call, copy, dsi.name, client_id, lib=zfs)
                if verdict.message is not None:
                    message = self._owner_message(copy, dsi.name, client_id, verdict)
                    self._log_refusal_once(key, f"{dsi.name} -> {destination}: {message}")
                    raise NegotiationFailed(message)
                self._pending.pop(copy, None)

            missing: list = []
            if not exists and not dry_run:
                unsettled, missing, conflict = self._ancestors_state(
                    call, copy, transport.base, client_id
                )
                if conflict is not None:
                    self._log_refusal_once(key, f"{dsi.name} -> {destination}: {conflict}")
                    raise NegotiationFailed(conflict)
                if unsettled is not None:
                    ancestor, kind = unsettled
                    why = (
                        "it holds the partial state of a first receive (it completes with its "
                        f"next stream, or discard it with `zfs receive -A {ancestor}`)"
                        if kind == 'partial' else "its first backup has not completed"
                    )
                    self._log_warning_once(
                        (f"{dsi.name} [unsettled]", destination),
                        f"{dsi.name} -> {destination}: waiting for {ancestor!r}: {why}; "
                        f"{dsi.name} is not sent until it has settled",
                    )
                    raise NegotiationFailed(
                        f"{self._busy_prefix}: waiting for {ancestor}", reloading=True
                    )

            if not dry_run:
                if exists:
                    self._note_retention_shrink(call, dsi, destination, copy, copy_fs, canonical)
                    call(zfs.set, copy_fs, {
                        PROP_CONFIG: canonical,
                        PROP_CLIENT_ID: client_id,
                        PROP_SOURCE_DATASET: dsi.name,
                    })
                else:
                    # Only the containers, never the copy itself (the full
                    # stream creates it: zfs refuses a raw full, or a plain
                    # one under an encrypted target, onto an existing
                    # dataset), outermost first, no `-p`.
                    receiver.create_ancestors(call, missing, client_id, lib=zfs)
                    self._pending[copy] = time.monotonic()

            common = None
            copy_kind = 'none'
            newer = None
            if exists:
                try:
                    rows = call(receiver.server_snapshots, copy_fs, lib=zfs)
                    trusted = True
                except ZFSTimeoutError:
                    raise
                except Exception as e:
                    self._logger.warning(
                        f"{dsi.name} -> {destination}: the snapshot listing of {copy} cannot be "
                        f"trusted ({e}); the snapshots newer than the common one are unknown"
                    )
                    rows = call(receiver.server_snapshots, copy_fs, strict=False, lib=zfs)
                    trusted = False
                names = {r.name for r in rows}
                common = next((s.name for s in snapshots if s.name in names), None)
                if names:
                    copy_kind = receiver.server_copy_kind(call, copy_fs, lib=zfs)
                if common is not None and trusted:
                    newer = receiver.snapshots_newer_than(common, rows)
                    if newer is None:
                        self._logger.warning(
                            f"{dsi.name} -> {destination}: a snapshot of {copy} has no "
                            "createtxg; the snapshots newer than the common one are unknown"
                        )
        except NegotiationFailed:
            raise
        except ZFSTimeoutError as e:
            self._zfs_timed_out(dsi, destination, f"negotiating {copy}", e)
            raise NegotiationFailed(
                f"{self._busy_prefix}: zfs timed out", reloading=True, zfs_timeout=True
            )
        except Exception as e:
            if self._target_vanished(transport, dsi, destination):
                raise NegotiationFailed(f"the target dataset {transport.target} is gone") from e
            self._logger.error(
                f"Negotiation failed for {dsi.name} at {self._where(transport)}: {e}"
            )
            raise NegotiationFailed(str(e)) from e

        self._refusal_logged.pop(key, None)
        self._refusal_logged.pop((f"{dsi.name} [unsettled]", destination), None)
        self._zfs_recovered(dsi, destination)
        # Every field, always (a missing one would read as a server that does
        # not report it, and fail open): `known` with nothing newer when there
        # is no common snapshot.
        self._server_info[key] = (copy_kind, copy)
        if newer is not None:
            self._server_newer[key] = (len(newer), newer[:SERVER_NEWER_MAX])
            self._server_newer_status[key] = 'known'
        elif common is not None:
            self._server_newer[key] = (0, [])
            self._server_newer_status[key] = 'unknown'
        else:
            self._server_newer[key] = (0, [])
            self._server_newer_status[key] = 'known'
        return common

    def _note_retention_shrink(self, call, dsi, destination, copy, copy_fs, canonical) -> None:
        """One WARNING when the configuration about to be stored keeps
        snapshots for a shorter time than the one stored on the copy (as the
        server's negotiate). Never refuses; an unreadable stored value is
        skipped (DEBUG); a zfs timeout propagates."""
        try:
            stored = call(zfs.get.value, copy_fs, PROP_CONFIG, sources='local')
            shrink = receiver.retention_shrink(stored, canonical) if stored is not None else None
        except ZFSTimeoutError:
            raise
        except Exception as e:
            self._logger.debug(f"{dsi.name} -> {destination}: not comparing the retention of {copy}: {e}")
            return
        if shrink is not None:
            old, new = (receiver.format_horizon(h) for h in shrink)
            self._logger.warning(
                f"{dsi.name} -> {destination}: the retention horizon of {copy} shrinks from {old} "
                f"to {new}; snapshots of the copy older than {new} are pruned from the next "
                "pruning cycle"
            )

    def _fetch_resume_token(
        self, transport: _LocalTarget, client_id, dsi, destination
    ) -> Tuple[str, Optional[str]]:
        """The copy's `receive_resume_token` -> `('token', t)`, `('none',
        None)` or `('skip', None)` (counted by `_token_skipped`): an
        unreadable answer never becomes a fresh send, which would discard the
        partial receive."""
        copy = self._copy_name(transport, client_id, dsi, destination)
        if copy is None:
            return 'skip', None
        call = _Budget()
        try:
            verdict = receiver.ownership_conflict(call, copy, dsi.name, client_id, lib=zfs)
            if verdict.message is not None:
                message = self._owner_message(copy, dsi.name, client_id, verdict)
                self._log_refusal_once((dsi.name, destination), f"{dsi.name} -> {destination}: {message}")
                return 'skip', None
            token = receiver.resume_token(call, copy, lib=zfs)
        except ZFSTimeoutError as e:
            self._zfs_timed_out(dsi, destination, f"reading the resume token of {copy}", e)
            self._token_skipped(dsi, destination, "zfs did not answer in time on this host")
            return 'skip', None
        except Exception as e:
            if self._target_vanished(transport, dsi, destination):
                return 'skip', None
            self._logger.warning(
                f"cannot read the resume token of {copy}: {e}; skipping this cycle rather than "
                "risk discarding its partial progress with a fresh send"
            )
            self._token_skipped(dsi, destination, f"cannot read the resume token of {copy}: {e}")
            return 'skip', None
        return ('token', token) if token else ('none', None)

    def _stream_step(
        self,
        transport: _LocalTarget,
        client_id: str,
        dsi: DatasetInfo,
        destination: str,
        started: _SendStream,
        after_success: bool = False,
        should_stop: Optional[Callable[[], bool]] = None,
        discard: Tuple[str, ...] = (),
    ) -> str:
        """Receive ONE started `zfs send` (`started`) into the copy, in the
        server's order: ownership -> the owned values -> the stream's first
        record (compound streams refused, the declared base against
        `drr_fromguid`, an existing copy's type) -> `decide_receive` (the only
        guard against rolling back the copy's history) -> `receive -A` on a
        fresh step with a token -> `zfs receive -s -u -h -o <owned + forced>`
        (`-F` only when allowed), fed the first record, then the rest
        (`_Pump`). -> `_OK`, `_FAILED`, or `_STOP` (a zfs check timed out:
        skip the destination this cycle). Both children are reaped (or stopped
        and, if they survive SIGKILL, registered as stuck) on every path.
        `after_success` and `should_stop` are not used: a stop waits for the
        step to end."""
        pump = _Pump(started.stream)
        try:
            return self._receive_step(pump, transport, client_id, dsi, destination, started, discard)
        finally:
            pump.close()

    def _receive_step(
        self, pump: _Pump, transport: _LocalTarget, client_id: str, dsi: DatasetInfo,
        destination: str, started: _SendStream, discard: Tuple[str, ...],
    ) -> str:
        mode, base = started.mode, started.base
        where = self._where(transport)
        copy = self._copy_name(transport, client_id, dsi, destination)
        if copy is None:
            return _FAILED
        copy_fs = Filesystem(copy)
        try:
            owned = {
                PROP_CONFIG: receiver.canonical_dataset_config(dsi.config.to_property(destination)),
                PROP_CLIENT_ID: client_id,
                PROP_SOURCE_DATASET: dsi.name,
            }
        except ValueError as e:
            self._logger.error(f"{dsi.name} -> {destination}: cannot store its configuration ({e})")
            return _FAILED
        stream_base = receiver.StreamBase(
            base.name if base is not None else None,
            base.guid if base is not None else None,
            frozenset(int(g) for g in discard),
        )

        # 1. Ownership, and 2. whether the copy exists (asked once, reused).
        call = _Budget()
        try:
            verdict = receiver.ownership_conflict(call, copy, dsi.name, client_id, lib=zfs)
            existed = call(zfs.exists, copy_fs) if verdict.message is None else False
        except ZFSTimeoutError as e:
            self._zfs_timed_out(dsi, destination, f"checking {copy}", e)
            return _STOP
        except Exception as e:
            if self._target_vanished(transport, dsi, destination):
                return _STOP
            self._logger.error(f"Transfer failed for {dsi.name} -> {where}: cannot check {copy}: {e}")
            return _FAILED
        if verdict.message is not None:
            message = self._owner_message(copy, dsi.name, client_id, verdict)
            self._log_refusal_once((dsi.name, destination), f"{dsi.name} -> {destination}: {message}")
            return _FAILED

        # 3. The stream's first record (the send's time, not zfs's).
        try:
            pump.prepare()
            head = pump.read_head(receiver.DRR_RECORD_SIZE)
        except _Stalled:
            pump.close()  # stops the send now, so a child that survives is known below
            self._logger.error(
                f"Transfer failed for {dsi.name} -> {where}: zfs send produced no data for "
                f"{LOCAL_STALL_TIMEOUT:g} s; it was stopped{self._stuck_text(pump)}"
            )
            return _FAILED
        except OSError as e:
            self._logger.error(f"Transfer failed for {dsi.name} -> {where}: reading zfs send: {e}")
            return _FAILED
        try:
            header = receiver.parse_stream_header(head)
        except receiver.StreamHeaderError as e:
            send_error = pump.finish_send()
            if send_error:
                self._logger.error(
                    f"Transfer failed for {dsi.name} -> {where}: zfs send failed: {send_error}"
                )
            else:
                self._refuse_stream(dsi, destination, 'stream_header', str(e))
            return _FAILED

        call = _Budget()  # the checks between the header and the receive
        mismatch = receiver.stream_base_problem(header, stream_base)
        if mismatch is not None:
            self._refuse_stream(dsi, destination, 'base_mismatch', f"{mismatch}, so nothing is received")
            return _FAILED
        try:
            if existed:
                existing_type = call(zfs.get.value, Dataset(copy), 'type')
                if existing_type != header.kind:
                    self._refuse_stream(dsi, destination, 'type_mismatch', (
                        f"{copy} is a {existing_type} on this host and the stream is a "
                        f"{header.kind}, so nothing is received"
                    ))
                    return _FAILED

            # 4. `-F`, decided before anything is discarded.
            decision = receiver.ReceiveDecision(False)
            if existed:
                try:
                    rows = call(receiver.server_snapshots, copy_fs, lib=zfs)
                except ZFSTimeoutError:
                    raise
                except Exception as e:
                    self._refuse_stream(dsi, destination, 'listing', (
                        f"cannot list the snapshots of {copy} ({e}), so nothing is received"
                    ))
                    return _FAILED
                if stream_base.guid is None and stream_base.name is not None:
                    named = next((r for r in rows if r.name == stream_base.name), None)
                    if named is not None and named.guid is not None and named.guid != header.from_guid:
                        self._refuse_stream(dsi, destination, 'base_mismatch', (
                            f"{copy} holds {stream_base.name!r} with guid {named.guid}, but the "
                            f"stream's source guid is {header.from_guid}, so nothing is received"
                        ))
                        return _FAILED
                try:
                    decision = receiver.decide_receive(
                        copy, stream_base, rows, where='this host', sender='this host',
                        rename_doc=self._rename_doc,
                    )
                except ValueError as e:
                    self._refuse_stream(dsi, destination, 'listing', (
                        f"cannot order the snapshots of {copy}: {e}, so nothing is received"
                    ))
                    return _FAILED
            if decision.reason is not None:
                self._refuse_stream(dsi, destination, decision.reason, decision.message)
                return _FAILED
            if decision.rolled_back:
                self._logger.warning(
                    f"{dsi.name} -> {destination}: the receive destroys "
                    f"{len(decision.rolled_back)} snapshot(s) of {copy} newer than its incremental "
                    f"source, which this host no longer has: {', '.join(decision.rolled_back)}"
                )

            # 5. A fresh stream cannot continue a partial receive: discard it.
            if mode == 'fresh':
                token = receiver.resume_token(call, copy, lib=zfs)
                if token:
                    self._logger.warning(
                        f"discarding the partial receive state of {copy}: a fresh stream of "
                        f"{dsi.name} is sent"
                    )
                    receiver.abort_partial_receive(call, copy, lib=zfs)

            force = receiver.may_force(call, copy_fs, decision.force, lib=zfs)
        except ZFSTimeoutError as e:
            self._zfs_timed_out(dsi, destination, f"preparing the receive into {copy}", e)
            return _STOP
        except Exception as e:
            if self._target_vanished(transport, dsi, destination):
                return _STOP
            self._logger.error(
                f"Transfer failed for {dsi.name} -> {where}: cannot prepare the receive into "
                f"{copy}: {e}"
            )
            return _FAILED
        self._zfs_recovered(dsi, destination)

        # 6. The receive (never timed), fed the first record, then the rest.
        try:
            receive = receiver.start_receive(copy_fs, owned, header.kind, force, lib=zfs)
        except Exception as e:
            if self._target_vanished(transport, dsi, destination):
                return _STOP
            self._logger.error(
                f"Transfer failed for {dsi.name} -> {where}: cannot start zfs receive into "
                f"{copy}: {e}"
            )
            return _FAILED
        pump.attach_receive(receive)
        outcome = pump.run()
        pump.close()
        return self._step_outcome(pump, outcome, dsi, destination, where, copy)

    @staticmethod
    def _stuck_text(pump: _Pump) -> str:
        """The log suffix naming children that survived SIGKILL, else ''."""
        if not pump.abandoned:
            return ''
        return (
            "; " + ', '.join(f"zfs {side} (pid {pid})" for side, pid in pump.abandoned)
            + " did not exit after SIGKILL (a hung pool?): timed zfs calls in this worker "
            "are refused until it exits, so local destinations are skipped until then"
        )

    def _step_outcome(self, pump: _Pump, outcome: str, dsi, destination, where, copy) -> str:
        """Log what the pump and the two children's exit statuses say; -> `_OK`
        only for a completed stream that the receive reports success for."""
        send_error, receive_error = pump.send_error, pump.receive_error
        stuck = self._stuck_text(pump)
        if outcome == PUMP_STALLED and pump.commit_timed_out:
            # The whole stream was delivered; the receive was still committing.
            self._logger.error(
                f"Transfer of {dsi.name} -> {where}: zfs receive into {copy} did not finish "
                f"committing within {LOCAL_COMMIT_TIMEOUT:g} s after the end of the stream; it "
                "was stopped, but its final commit cannot be interrupted, so it may still "
                "complete. The next cycle settles it: it finds the snapshot in the copy, or "
                f"resumes the partial receive{stuck}"
            )
            return _FAILED
        if outcome == PUMP_STALLED:
            self._logger.error(
                f"Transfer failed for {dsi.name} -> {where}: no progress for "
                f"{LOCAL_STALL_TIMEOUT:g} s; zfs send and zfs receive were stopped, the partial "
                f"receive into {copy} is kept for a resume{stuck}"
            )
            return _FAILED
        if outcome == PUMP_OK and receive_error is None and (
            send_error is None or pump.send_cut_after_eof
        ):
            if send_error is not None:
                # The receive read the end record and committed (exit 0): the
                # copy holds the snapshot. Only the send's exit was late.
                self._logger.warning(
                    f"{dsi.name} -> {where}: zfs send did not exit within "
                    f"{LOCAL_STALL_TIMEOUT:g} s after the end of its stream and was stopped "
                    f"({send_error}); zfs receive committed {copy}, so the step counts as done"
                    f"{stuck}"
                )
            self._pending.pop(copy, None)
            return _OK
        if send_error is not None and outcome != PUMP_RECEIVE_GONE:
            self._logger.error(
                f"Transfer failed for {dsi.name} -> {where}: zfs send failed: {send_error}"
                + (f"; zfs receive into {copy}: {receive_error}" if receive_error else "")
                + stuck
            )
            return _FAILED
        detail = (
            self._receive_error(receive_error, copy) if receive_error
            else "it stopped reading before the end of the stream"
        )
        self._logger.error(
            f"Transfer failed for {dsi.name} -> {where}: zfs receive into {copy} failed: {detail}"
            + (f" (zfs send: {send_error})" if send_error else "")
            + stuck
        )
        return _FAILED

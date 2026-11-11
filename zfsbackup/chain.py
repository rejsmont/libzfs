"""The transport-independent send chain.

`ChainSender` holds everything about sending a dataset's snapshots to a
destination that does not depend on how bytes reach it: the send mode, the
rollback and server-copy checks, the `-i` chain, the resume-token decision,
the in-flight pin and the anchor. A subclass supplies the transport:

* `_ready_transport(destination, dest)` -- an opaque transport object, or
  `None` to skip the destination (the log lines name it through `_where`,
  which defaults to `transport.base_url`, and `_peer`);
* `_negotiate(transport, client_id, dsi, snapshots, destination)` -- the newest
  snapshot present on both sides, `None` for none, `NegotiationFailed` for no
  answer; it also fills `_server_info`, `_server_newer` and
  `_server_newer_status`;
* `_fetch_resume_token(transport, client_id, dsi, destination)` --
  `('token', t)`, `('none', None)` or `('skip', None)`;
* `_stream_step(...)` -- carry ONE started `zfs send` to the destination and
  return `_OK`, `_FAILED` or `_STOP`.

`zfsbackup.remote.RemoteBackupManager` is the HTTPS / websocket subclass and
`zfsbackup.local.LocalBackupManager` the same-host one (`zfs send` piped into
`zfs receive`); the wording hooks (`_copy_noun`, `_receiver`, `_on_receiver`,
...) let each name its receiving side, the defaults being the remote wording.
This module imports neither `requests`, `websocket` nor `zfsbackup.identity`.

`copy_confirmed(dataset, destination)` says whether a copy has been CONFIRMED
in this incarnation (a negotiate that found one, or an acknowledged stream);
the remote worker's parent-first rule for the descendants of a recursive
dataset reads it, and it is never persisted.

`last_problem(dataset, destination)` says why the last `backup_dataset` did not
complete and `last_retryable(dataset, destination)` whether that is worth
retrying soon (busy, reloading, an unsettled ancestor, a connect failure before
a stream byte, a stop between steps; never a refusal, a `zfs` timeout or a
resume-token skip): a per-call flag set where the chain, a subclass or the
receiver already classifies the case, never read back from a message.

What outlives a send (the once-per-episode latches, the skip counters, the
confirmed copies) is kept in a `sendmemory.SendMemory`: one per worker, passed
to every manager as `memory=` (or `adopt_memory()`), a private one otherwise.
The per-call state (`_server_info`, `_server_newer`, `_server_newer_status`,
`_call_note`, `_call_retryable`, `_call_refusals`) stays on the instance.

Every subclass `_negotiate` must fill `_server_info`, `_server_newer` and
`_server_newer_status` on success: a missing value fails OPEN (`absent` reads
as a server that predates B-2b in `_settle_up_to_date`, a missing
`server_copy` skips `_server_copy_matches`). `backup_dataset` passes
`client_id` on unchecked: the subclass's `_ready_transport` must refuse
without one.
"""

import logging
import re
from typing import Any, Callable, Dict, NamedTuple, Optional, Set, Tuple

import libzfseasy as zfs
from libzfseasy.zfs import ResumeTokenError
from zfsbackup.backup_manager import (
    DatasetInfo, DatasetManager, LocalAnchorUnreadable, parse_snapshot_timestamp,
    snapshot_newer,
)
from zfsbackup.config import (
    BackupConfig, Destination, RemoteDatasetConfig, _collapse_retention_rules,
)
from zfsbackup.sendmemory import (
    COPY_CONFIRMED, REFUSALS, RETENTION_WARNED, TOKEN_ERROR, TOKEN_SKIPS, SendMemory,
)


logger = logging.getLogger(__name__)


#: S-7a: the resume-token check (`_fetch_resume_token`, `_resume_target`) skips
#: the cycle when it cannot tell whether the server holds a partial receive --
#: for ever, if the cause stays. After this many consecutive skipped cycles of
#: one (dataset, destination) an ERROR is logged, once, until the check passes.
RESUME_TOKEN_SKIP_ERROR_AFTER = 3

#: What the status file's `last_error` of a token-skipped cycle starts with
#: (`ChainSender.last_problem`); the worker seeds the skip counts of a
#: new incarnation from the records that carry it.
TOKEN_SKIP_NOTE = "resume token check failed"


class NegotiationFailed(Exception):
    """`_negotiate` could not get an answer from the server. Distinct from a
    successful negotiation that found no common snapshot (which returns
    `None` and rightly leads to an initial full send): a failure must never
    fall through to a send. `reloading` is True for the server's retryable
    503 ("server is reloading; retry later")."""

    def __init__(self, message: str, reloading: bool = False, zfs_timeout: bool = False):
        super().__init__(message)
        self.reloading = reloading
        #: The server's zfs did not answer in time (already logged, counted).
        self.zfs_timeout = zfs_timeout




def _retry_word(e: 'NegotiationFailed', busy_prefix: str = "server is busy") -> str:
    """How to say a retryable negotiate refusal in a log line: `reloading`,
    `busy`, or `busy: <the server's reason>`. `busy_prefix` is how the
    refusal's text starts (`ChainSender._busy_prefix`)."""
    text = str(e)
    if 'reloading' in text:
        return "reloading"
    reason = text[len(busy_prefix):].lstrip(': ')
    return f"busy: {reason}" if reason else "busy"




class _SendMode(NamedTuple):
    """One send mode. `options` are the `zfs.send.snapshot` keyword flags of
    a fresh send. `required` and `allowed` bound the `*ok` flags a resume
    token may carry for this mode to be resumed: every required flag present
    and nothing outside `allowed` (a token made by a different mode, or by
    something else, is not resumed; `embedok` and `savedok` are never
    allowed). The recorded set depends on dataset state, not only on the flags
    sent (`largeblockok` appears only when the dataset holds blocks over
    128K), and a plain send of an unencrypted dataset records no `rawok`;
    OpenZFS 2.1.5, 2.2.2 and 2.4.1 agree (captured). The empty set (tokens of
    the flagless sends of earlier versions) stays acceptable in plain mode."""
    options: dict
    required: frozenset
    allowed: frozenset


#: The frozen send-flag sets (plan B1, irreversible per server dataset):
#: plain `-L -c`; raw `-w` (libzfs adds `-L -e -c` itself). A resumed send is
#: `zfs send -t <token>` with no flags: the token carries its own.
_SEND_MODES = {
    'plain': _SendMode(
        {'large_blocks': True, 'compressed': True},
        frozenset(),
        frozenset({'compressok', 'largeblockok'}),
    ),
    'raw': _SendMode(
        {'raw': True},
        frozenset({'rawok'}),
        frozenset({'rawok', 'compressok', 'largeblockok'}),
    ),
}

#: Substrings (lower case) of `resume_info(...).error` that mean the token is
#: DEFINITIVELY dead -- the snapshots it needs are gone or changed -- so a
#: fresh send is right. VM-verified: "used in the initial send no longer
#: exists", "...is no longer the same snapshot used in the initial send",
#: "incremental source (...) no longer exists", and "Invalid cross-device link"
#: (the incremental source is gone). Any other zfs error is NOT known to be
#: final: the cycle is skipped instead (D6), because a fresh send makes the
#: server discard resumable progress.
_STALE_TOKEN_ERRORS = (
    'used in the initial send no longer exists',
    'no longer the same snapshot',
    'invalid cross-device link',
)


def _token_error_is_definitive(error: str) -> bool:
    text = error.lower()
    return any(m in text for m in _STALE_TOKEN_ERRORS) or (
        'incremental source' in text and 'no longer exists' in text
    )


#: Outcomes of `ChainSender._push_step` / `_stream_step`.
_OK = 'ok'
_FAILED = 'failed'
_STOP = 'stop'
_ANCHOR_FAILED = 'anchor-failed'


class _SendStream(NamedTuple):
    """What `_try_resume` or `_start_fresh` started: the `zfs send` handle, the `mode`
    the server must be told (`resume` when it took the resume-token path,
    `fresh` otherwise -- see `api.STREAM_MODES`), whether the in-flight pin
    (`inflight.<dest>`) was written before the send began, and which local
    snapshots the stream carries (`sent`, a `SnapshotInfo`; `base`, the
    incremental source or `None` for a full stream). For a resume these come
    from the token's GUIDs, never from its (possibly stale) `toname`."""
    stream: object
    mode: str
    inflight_set: bool = False
    sent: object = None
    base: object = None


class ChainSender:
    """Sends a dataset's snapshots to one configured destination as a chain of
    incremental `zfs send` steps over a transport the subclass provides.

    `client_id` is this host's id from the supervisor's config snapshot
    (`daemon.Handoff.client_id`; `None` until `zfsbackup-config enroll`
    generates one). Per-call state is per manager; what outlives a send is in
    the `SendMemory` (shared by the managers of one worker), and none of it is
    persisted."""

    #: The logger the chain's messages go to; a subclass sets its own, so the
    #: messages appear under the transport's logger name.
    _logger = logger

    # Wording hooks: how the chain's log lines name the receiving side. The
    # defaults are the remote (server) wording, byte for byte; a subclass with
    # another kind of receiver (`zfsbackup.local`) overrides them.
    #: The receiver's copy of a dataset ("the server copy X ...").
    _copy_noun = "server copy"
    #: The placeholder for that copy's name when it is not known.
    _copy_placeholder = "<server dataset>"
    #: The receiving side as a subject ("the server discards ...").
    _receiver = "the server"
    #: Where an operator acts on the copy ("abort it on the server").
    _on_receiver = "on the server"
    #: Where snapshots of the copy that this sender never sent were made.
    _taken_on_receiver = "taken on the server"
    #: Where a snapshot already sent is ("... is already on the server").
    _at_receiver = "on the server"
    #: The sending side ("a snapshot this host does not have").
    _sender = "this host"
    #: How a retryable negotiate refusal's text starts (`_retry_word`).
    _busy_prefix = "server is busy"
    #: The runbook section on moving a copy aside, and the one on rejoining
    #: (`None`: no separate pointer).
    _rename_doc = "README 'Renaming a server copy aside'"
    _rejoin_doc: Optional[str] = "README 'Rejoining'"
    #: The label of the once-per-episode key of a receiver's stream refusal
    #: (`(f"{dataset} [{label}]", destination)`), cleared by `_record_step`.
    _refusal_label = "server refusal"
    #: Whether `zfs send` is started with libzfseasy's blocking start-up peek
    #: (the default: an immediate failure raises from the start call). A
    #: transport that bounds the stream's first record itself sets False: the
    #: handle then comes back at once, and an immediate failure shows as EOF
    #: with `close()` raising (`_send_kwargs`).
    _send_peek = True

    def __init__(
        self,
        config: BackupConfig,
        local_manager: DatasetManager,
        *,
        client_id: Optional[str],
        memory: Optional[SendMemory] = None,
    ):
        self.config = config
        self.local_manager = local_manager
        self.client_id = client_id
        #: What outlives a send or is shared between sends: the once-per-episode
        #: latches (the standing refusals, the retention-collision warnings, the
        #: resume-token ERROR), the consecutive-skip counters and the confirmed
        #: copies. ONE `SendMemory` per worker, handed to every manager it builds
        #: (`adopt_memory` for one that is already built); a manager built without
        #: one has a private instance, as it always had its own state.
        self._memory: SendMemory = memory if memory is not None else SendMemory()
        # `(dataset, destination) -> (server_copy, server_dataset)` from the
        # last successful negotiate (`server_copy` is `None` for a server that
        # does not report it).
        self._server_info: Dict[Tuple[str, str], Tuple[Optional[str], Optional[str]]] = {}
        # `(dataset name, destination) -> (server_newer_count, server_newer)`
        # from the last successful negotiate: the server snapshots newer than
        # the common one, `[{"name", "guid"}, ...]` oldest first. Empty for a
        # server that does not report them (an older release).
        self._server_newer: Dict[Tuple[str, str], Tuple[int, list]] = {}
        # `(dataset name, destination) -> 'known' | 'unknown' | 'absent'`:
        # whether the last negotiate reported `server_newer` (`known`, also
        # when the list is empty), reported `server_newer_unknown` (a common
        # snapshot whose newer snapshots the server could not tell) or
        # reported neither (`absent`: a server that predates B-2b).
        self._server_newer_status: Dict[Tuple[str, str], str] = {}
        # `(dataset, destination) -> why the last `backup_dataset` call did
        # not complete`, where this class knows (the token check); read by
        # `last_problem`, reset at the start of every call. Per call, so it
        # stays on the instance.
        self._call_note: Dict[Tuple[str, str], str] = {}
        # `(dataset, destination)` -> whether the last `backup_dataset` call
        # ended in a way that is worth retrying soon (`last_retryable`); reset
        # at the start of every call, like `_call_note`.
        self._call_retryable: Dict[Tuple[str, str], bool] = {}
        # The `REFUSALS` keys THIS call logged (`_log_refusal_once` /
        # `_log_warning_once`, whether newly or as a repeat) and, as
        # `(None, destination)`, a destination this call's `_ready_transport`
        # refused: the only refusals `last_problem` may report for the call.
        # The memory's table outlives the call and is shared with other pairs,
        # so reading all of it would blame this pair for another's refusal.
        # Reset at the start of every call, like `_call_note`.
        self._call_refusals: Set[Tuple[Optional[str], str]] = set()

    # What the memory holds, under the names the manager's state always had
    # (the LIVE containers, for tests and diagnostics; the chain itself only
    # uses `SendMemory`'s atomic operations).
    #
    # `(dataset name, destination) pairs whose effective per-destination
    # retention-rule collisions have already been warned about, for the
    # memory's lifetime. `to_property(destination)` ships the resolved
    # ruleset unresolved (correct -- resolution is a read-path concern), and
    # the ONLY collision warning used to appear in the *receiving* admin's
    # journal, naming a server-side path the client operator does not
    # administer. The client must warn about its own collisions too.
    @property
    def _retention_warned(self) -> Set[Tuple[str, str]]:
        return self._memory.latch_view(RETENTION_WARNED)

    # Key -> the last refusal logged for it, so a standing refusal is an
    # ERROR once and a DEBUG line afterwards. `(dataset, destination)`:
    # a dataset-level refusal (D7: the server holds this dataset for
    # another source). `(None, destination)`: a destination-level one
    # (not enrolled, client not trusted, client id mismatch, pin
    # mismatch, reset after the handshake), cleared by a good `/whoami`.
    @property
    def _refusal_logged(self) -> Dict[Tuple[Optional[str], str], str]:
        return self._memory.value_view(REFUSALS)

    # S-7a. `(dataset, destination) -> consecutive cycles skipped by the
    # resume-token check`, and the pairs whose ERROR was already logged.
    # In memory: a new worker (reload, crash) counts again from 0.
    @property
    def _token_skips(self) -> Dict[Tuple[str, str], int]:
        return self._memory.counter_view(TOKEN_SKIPS)

    @property
    def _token_error_logged(self) -> Set[Tuple[str, str]]:
        return self._memory.latch_view(TOKEN_ERROR)

    # `(dataset, destination)` pairs whose copy at the destination was
    # CONFIRMED in this incarnation: a negotiate reported an existing copy
    # (`server_copy` plain or raw, or a common snapshot) and the call then
    # returned True, or a stream to it committed (`_record_step`). Only
    # ever read through `copy_confirmed`; never persisted (a new worker
    # confirms again), and removed when a later negotiate finds no copy.
    @property
    def _copy_confirmed(self) -> Set[Tuple[str, str]]:
        return self._memory.latch_view(COPY_CONFIRMED)

    def adopt_memory(self, memory: SendMemory) -> None:
        """Use `memory` (the worker's shared one) instead of the private
        instance made at construction. Call it right after building the
        manager, before anything is sent: what the private memory held is
        dropped."""
        self._memory = memory

    def _token_skipped(self, dsi: DatasetInfo, destination: str, reason: str) -> None:
        """The resume-token check skipped this cycle for `dsi` -> `destination`
        (the caller has logged the WARNING). Counts it; the
        `RESUME_TOKEN_SKIP_ERROR_AFTER`th consecutive skip logs an ERROR, once
        until `_token_ok`."""
        key = (dsi.name, destination)
        self._call_note[key] = f"{TOKEN_SKIP_NOTE}: {reason}"
        # Decided under the memory's lock, logged outside it.
        count = self._memory.incr(TOKEN_SKIPS, key)
        if count >= RESUME_TOKEN_SKIP_ERROR_AFTER and self._memory.note(TOKEN_ERROR, key):
            server_ds = self._server_info.get(key, (None, None))[1] or self._copy_placeholder
            self._logger.error(
                f"{dsi.name} -> {destination}: the resume-token check has skipped the last "
                f"{count} cycles in a row ({reason}); nothing is sent for this dataset until "
                "it passes, so the destination is falling behind. See the WARNINGs above for "
                f"the cause. If {self._receiver}'s partial receive of this dataset is no longer "
                f"wanted, abort it {self._on_receiver} with `zfs receive -A {server_ds}`; the next "
                "cycle then sends fresh"
            )

    def seed_token_skips(self, counts: Dict[Tuple[str, str], int]) -> None:
        """Start the resume-token skip counts of `(dataset, destination)`
        pairs from `counts` (the worker takes them from its previous
        incarnation's status file), so that frequent reloads cannot keep the
        ERROR from ever firing. The ERROR itself is still logged once per
        incarnation, at the next skip once the count reaches the threshold."""
        for key, count in counts.items():
            if count > 0:
                self._memory.seed_max(TOKEN_SKIPS, key, count)

    def _token_ok(self, dsi: DatasetInfo, destination: str) -> None:
        """The resume-token check passed (or was not needed because the
        server is up to date): the skip count restarts, and the ERROR may be
        logged again for a later episode."""
        key = (dsi.name, destination)
        self._memory.reset(TOKEN_SKIPS, key)
        if self._memory.clear(TOKEN_ERROR, key):
            self._logger.info(f"{dsi.name} -> {destination}: the resume-token check passes again")

    def copy_confirmed(self, dataset: str, destination: str) -> bool:
        """Whether the destination's copy of `dataset` has been CONFIRMED in
        this incarnation: a `backup_dataset` call returned True after a
        negotiate that reported an existing copy (`server_copy` plain or raw,
        or a common snapshot), or a stream to it committed (acknowledged,
        `_record_step`). Not a statement about being up to date, and not
        evidence from any anchor or status record: an anchor can be stale (a
        changed URL or target, another client id, a lost copy). A dry run
        confirms nothing (no stream is sent, and a missing copy is not
        created). The worker's parent-first rule is the one reader."""
        return self._memory.noted(COPY_CONFIRMED, (dataset, destination))

    def last_retryable(self, dataset: str, destination: str) -> bool:
        """Whether the last `backup_dataset(dataset, destination)` ended in
        something worth retrying soon, as this manager knows (a per-call flag,
        reset at the start of every call; nothing is parsed from a message).
        True for: the receiver busy or reloading or draining (the retryable
        503, from a negotiate, a resume-token request, `/whoami` or a stream),
        an ancestor that is not settled yet, a connect failure before a stream
        byte, and a stop between steps (a stop before the first stream
        included). False for everything else: a `zfs` timeout (here or at the
        receiver), the resume-token skips, every refusal (authentication, the
        server holding the dataset for another source, a stream it will not
        accept, rollback and mode-mismatch ERRORs), a missing client id,
        enrollment or target. For a call that returned True it is True only
        when the chain stopped between steps with work left."""
        return self._call_retryable.get((dataset, destination), False)

    def _mark_retryable(self, dataset: str, destination: str) -> None:
        """This call's outcome is worth retrying soon (`last_retryable`)."""
        self._call_retryable[(dataset, destination)] = True

    def last_problem(self, dataset: str, destination: str) -> Optional[str]:
        """Why the last `backup_dataset(dataset, destination)` did not
        complete, as far as this manager knows (the resume-token check, else a
        refusal THIS call logged for the pair or for the whole destination;
        a refusal another call left in the memory is not this call's problem);
        `None` when it does not know. For the worker's status file."""
        note = self._call_note.get((dataset, destination))
        if note:
            return note
        mine = set(self._call_refusals)
        # Iterates a copy: other threads write the table while this reads it.
        for key, message in sorted(
            self._memory.items(REFUSALS), key=lambda item: (item[0][0] or '', item[0][1])
        ):
            if key not in mine:
                continue
            label, dest = key
            if dest == destination and (
                label is None or label == dataset or label.startswith(dataset + ' [')
            ):
                return message
        return None

    def backup_dataset(
        self,
        dsi: DatasetInfo,
        remote_cfg: RemoteDatasetConfig,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> bool:
        """Send a backup of dsi to the configured destination. Returns True on success.

        Before anything is negotiated or sent: the client id, the
        destination's enrollment (https URL and pinned server fingerprint),
        the client TLS identity and the server's `/whoami` binding must all
        be in order; otherwise the destination is skipped (logged once).

        `should_stop` (the worker passes "an exit reason is pending") is
        checked before every stream of the chain (between steps, never during
        one), so a stop or reload never begins another stream."""
        destination = remote_cfg.destination
        self._call_note.pop((dsi.name, destination), None)
        self._call_retryable.pop((dsi.name, destination), None)
        self._call_refusals = set()
        dest = self.config.destinations.get(destination)
        if dest is None:
            self._logger.error(f"Unknown destination '{destination}' for {dsi.name}")
            self._call_note[(dsi.name, destination)] = f"unknown destination '{destination}'"
            return False

        self._warn_retention_collisions(dsi, destination)

        self._before_destination_check()
        transport = self._ready_transport(destination, dest)
        if transport is None:
            # Whatever standing refusal the destination carries is this call's
            # reason (`last_problem`), logged or not by this very call.
            self._call_refusals.add((None, destination))
            if self._transport_retryable(destination):
                self._mark_retryable(dsi.name, destination)
            return False
        client_id = self.client_id

        snapshots = self.local_manager.list_snapshots(dsi)
        if not snapshots:
            # An empty listing and a FAILED one both come back as `[]`:
            # only `last_list_error` tells them apart. A failed listing is
            # not "nothing to send" (it was logged by `list_snapshots`): it
            # is a failure of this pair, recorded and graded as one, and what
            # the status file says.
            list_error = self.local_manager.last_list_error
            if list_error is not None:
                self._call_note[(dsi.name, destination)] = f"cannot list snapshots: {list_error}"
                return False
            self._logger.debug(f"No snapshots available to send for {dsi.name}")
            return True

        latest = snapshots[0]
        # The send mode is decided once, from the newest snapshot, and holds
        # for the whole chain.
        mode = self._send_mode(dsi, remote_cfg, latest)
        if mode is None:
            return False
        try:
            common_snap_name = self._negotiate(
                transport, client_id, dsi, snapshots, destination
            )
        except NegotiationFailed as e:
            # Never fall through to a send: with no answer we would treat
            # "unknown" as "no common snapshot" and start a full send.
            if e.reloading and not e.zfs_timeout:  # a zfs timeout was logged by `_negotiate`
                self._mark_retryable(dsi.name, destination)
                self._logger.info(
                    f"{self._peer(transport)} is {_retry_word(e, self._busy_prefix)}; "
                    f"skipping {dsi.name} this cycle"
                )
            return False
        key = (dsi.name, destination)
        existing = (
            self._server_info.get(key, (None, None))[0] in ('plain', 'raw')
            or bool(common_snap_name)
        )
        if not existing:
            # The destination holds no copy now: whatever was confirmed
            # earlier is gone (a stream of this call confirms it again).
            self._memory.clear(COPY_CONFIRMED, key)
        if not self._server_copy_matches(dsi, destination, mode):
            return False

        # Resolve the common snapshot object for incremental send. A named
        # common snapshot that is not among ours is never a reason for a full
        # send: skip the cycle.
        common_snap = None
        if common_snap_name:
            common_snap = next((s for s in snapshots if s.name == common_snap_name), None)
            if common_snap is None:
                self._logger.error(
                    f"{self._peer(transport)} reports {common_snap_name!r} as the common "
                    f"snapshot of {dsi.name}, which is not a managed local snapshot; "
                    "skipping this cycle rather than sending a full stream"
                )
                return False

        # Before any local anchor or pin is written, or anything is sent
        # (negotiate has already written on the server by now) -- in
        # particular before `_settle_up_to_date`, which would otherwise record
        # `latest` as the anchor of a rolled-back dataset and make it look
        # like a backstop.
        discard = self._rollback_permit(dsi, destination, common_snap, snapshots)
        if discard is None:
            return False

        done = self._transfer(
            transport, client_id, dsi, latest, common_snap, destination,
            snapshots=snapshots, should_stop=should_stop, mode=mode, discard=discard,
        )
        if done and existing and not self.config.dry_run:
            self._memory.note(COPY_CONFIRMED, key)
        return done

    def _send_mode(self, dsi: DatasetInfo, remote_cfg: RemoteDatasetConfig, newest) -> Optional[str]:
        """`'raw'` or `'plain'` for this dataset and destination, or `None`
        (ERROR once, nothing is sent). Decided from the newest snapshot's
        listed `encryption`/`keystatus` (no extra zfs call) and the
        destination's `raw` setting: `None` (auto) is raw if and only if the
        dataset is encrypted; `True` is raw and refuses an unencrypted dataset
        (`-w` on one records no `rawok`, a third kind of stream); `False` is
        plain, which needs the key loaded (`zfs send -nv` succeeds without it,
        so it is no preflight)."""
        key = (f"{dsi.name} [send mode]", remote_cfg.destination)
        encrypted = bool(newest.encrypted)
        setting = remote_cfg.raw
        problem = None
        if setting is True and not encrypted:
            problem = (
                f"{dsi.name} -> {remote_cfg.destination}: raw is true but the dataset is not "
                "encrypted; nothing is sent. Set raw to auto (or false) for this destination"
            )
        mode = 'raw' if (setting is True or (setting is None and encrypted)) else 'plain'
        if problem is None and mode == 'plain' and encrypted and newest.key_loaded is False:
            problem = (
                f"{dsi.name} -> {remote_cfg.destination}: raw is false but the dataset's key is "
                "not loaded, so it cannot be sent in plain form; nothing is sent. Load the key "
                "(zfs load-key), or set raw to auto to send it encrypted"
            )
        if problem is not None:
            self._log_refusal_once(key, problem)
            return None
        self._memory.pop(REFUSALS, key)
        return mode

    def _server_copy_matches(self, dsi: DatasetInfo, destination: str, mode: str) -> bool:
        """False (ERROR once, nothing is sent) when the server holds a copy of
        the other kind. A server copy cannot be switched between plain and
        raw by an incremental stream; the client never destroys or renames
        server data, so the operator moves the old copy aside and the next
        cycle sends a full stream. `none`, or no report (an older server),
        never blocks."""
        key = (f"{dsi.name} [server copy]", destination)
        copy, server_ds = self._server_info.get((dsi.name, destination), (None, None))
        if copy not in ('plain', 'raw') or copy == mode:
            self._memory.pop(REFUSALS, key)
            return True
        name = server_ds or dsi.name
        self._log_refusal_once(key, (
            f"{dsi.name} -> {destination}: the {self._copy_noun} {name} is a {copy} copy but this "
            f"dataset is now sent {mode}; nothing is sent. {self._on_receiver_capitalized()}, "
            f"move the old copy aside with `zfs rename {name} <a name outside the backup target>` "
            f"(see {self._rename_doc}); the next cycle sends a full {mode} stream"
        ))
        return False

    def _discard_names(self, dsi: DatasetInfo, destination: str, discard: Tuple[str, ...]) -> str:
        """The names (else the guids) of the server snapshots `discard` lets
        the server destroy, for log lines."""
        entries = self._server_newer.get((dsi.name, destination), (0, []))[1]
        by_guid = {e.get('guid'): e.get('name') for e in entries if isinstance(e, dict)}
        return ', '.join(str(by_guid.get(g) or f"guid {g}") for g in discard)

    def _rollback_permit(
        self, dsi: DatasetInfo, destination: str, common_snap, snapshots: list
    ) -> Optional[Tuple[str, ...]]:
        """Whether this cycle may send, and which server snapshots it lets the
        server destroy: `()` (send, destroy nothing), a tuple of snapshot guids
        (the `discard` of every stream of the cycle) or `None` (refused: ERROR
        once per episode, nothing is sent and nothing is written).

        The server receives into an existing dataset with `-F` only when that
        destroys nothing or only the snapshots named in `discard`
        (`api.decide_receive`), because `-F` destroys every server snapshot
        newer than the incremental base. Naming one is only right for a
        snapshot of ours that was sent and then destroyed here while it was
        being sent (the backstop of `_push_step` moved the anchor back to the
        base): the server has it, we do not, and that is the one snapshot we
        may let it discard. A host that rolled back or restored an older
        snapshot looks the same from the negotiate answer, so the evidence is
        local and all of it must hold:

        1. the dataset's OWN (local, never inherited) anchor for this
           destination names a snapshot that is in our managed listing and is
           not older (`createtxg`) than the common one. After the backstop the
           anchor is the base, and the server's pruner may since have removed
           the base from the server, making an older snapshot the common one;
           the anchor then still names a live local snapshot at or past the
           common one, so equality with the common snapshot would refuse that
           recovery for ever. A rollback or an in-place restore leaves the
           anchor naming a snapshot that no longer exists here, and a restore
           under a new name has no local anchor, so both are refused;
        2. the server names exactly one newer snapshot (and counts one);
        3. that snapshot's name is a managed one that is not a local snapshot
           name, and its guid equals no local snapshot's guid (a name can be
           reused for a different snapshot; a guid cannot);
        4. its guid is a readable decimal number.

        No newer snapshots, or a server that does not report them, needs no
        permit.

        With no common snapshot but a server copy that holds snapshots, a full
        stream cannot replace it (zfs refuses): refused here, once, rather
        than started every cycle."""
        key = (f"{dsi.name} [server newer]", destination)
        none_key = (f"{dsi.name} [no common]", destination)
        unknown_key = (f"{dsi.name} [server newer unknown]", destination)
        copy, server_ds = self._server_info.get((dsi.name, destination), (None, None))
        server_ds = server_ds or dsi.name
        if common_snap is None:
            self._memory.pop(REFUSALS, key)
            self._memory.pop(REFUSALS, unknown_key)
            if copy not in ('plain', 'raw'):
                self._memory.pop(REFUSALS, none_key)
                return ()
            self._log_refusal_once(none_key, (
                f"{dsi.name} -> {destination}: the {self._copy_noun} {server_ds} holds snapshots "
                f"but none of them is a snapshot {self._sender} still has, and a full stream "
                f"cannot replace it, so nothing is sent. To keep the {self._copy_noun}, "
                f"{self._on_receiver}: zfs rename {server_ds} <a name outside the backup target> "
                f"(the next cycle sends a full stream; see {self._rename_doc})."
                + self._rejoin_pointer()
            ))
            return None
        self._memory.pop(REFUSALS, none_key)
        if self._server_newer_status.get((dsi.name, destination)) == 'unknown':
            # The server has a common snapshot but could not tell what is newer
            # than it. That is not "nothing newer": writing an anchor now
            # could launder a rollback into the backstop later. Nothing is
            # written or sent (the stream would be refused with `listing`).
            self._memory.pop(REFUSALS, key)
            self._log_warning_once(unknown_key, (
                f"{dsi.name} -> {destination}: the {self._copy_noun} {server_ds} could not list "
                f"which of its snapshots are newer than {common_snap.name}, so nothing is sent "
                "and no anchor is written this cycle; the next cycle retries"
            ))
            return None
        self._memory.pop(REFUSALS, unknown_key)
        count, entries = self._server_newer.get((dsi.name, destination), (0, []))
        if not entries:
            self._memory.pop(REFUSALS, key)
            return ()
        guid = None
        unreadable_key = (f"{dsi.name} [local anchor]", destination)
        if len(entries) == 1 and count == 1 and isinstance(entries[0], dict):
            name, text = entries[0].get('name'), entries[0].get('guid')
            try:
                if (
                    isinstance(name, str) and isinstance(text, str)
                    and re.fullmatch(r'[0-9]{1,20}', text) is not None
                    and parse_snapshot_timestamp(self.config.snapshot_prefix, name) is not None
                    and name not in {s.name for s in snapshots}
                    and int(text) not in {s.guid for s in snapshots if s.guid is not None}
                    and self._anchor_at_or_past(dsi, destination, common_snap, snapshots)
                ):
                    guid = text
            except LocalAnchorUnreadable as e:
                # Not evidence of a rollback: the read failed. Say so instead
                # of the rollback ERROR, and send nothing this cycle.
                self._log_warning_once(unreadable_key, (
                    f"{dsi.name} -> {destination}: cannot read the local anchor property "
                    f"org.zfsbackup:anchor.{destination} ({e}), which decides whether "
                    f"{self._receiver} may discard its newer snapshot; nothing is sent this cycle"
                ))
                return None
        self._memory.pop(REFUSALS, unreadable_key)
        if guid is not None:
            self._memory.pop(REFUSALS, key)
            return (guid,)
        names = [str(e.get('name')) for e in entries if isinstance(e, dict)] or ['?']
        first, last = names[0], names[-1]
        truncated = count > len(entries)
        if len(names) == 1 and not truncated:
            shown, destroy = first, first
        elif truncated:
            # `first%` (a blank last) ranges to the newest (zfs-destroy(8)).
            shown = (
                f"{first}\u2026{last}; the list is truncated, the oldest {len(names)} of "
                f"{count} are named"
            )
            destroy = f"{first}%"
        else:
            shown, destroy = f"{first}\u2026{last}", f"{first}%{last}"
        self._log_refusal_once(key, (
            f"{dsi.name} -> {destination}: the {self._copy_noun} {server_ds} has {count} "
            f"snapshot(s) newer than {common_snap.name} that {self._sender} does not have "
            f"({shown}), for example after a rollback or an in-place restore of this dataset, "
            f"or snapshots {self._taken_on_receiver}. Sending would make {self._receiver} "
            f"destroy them, so nothing is sent. To keep them, {self._on_receiver}: "
            f"zfs rename {server_ds} <a name outside the backup target> (the next cycle "
            f"sends a full stream; see {self._rename_doc}). If they are "
            f"expendable: zfs destroy {server_ds}@{destroy} "
            "(the next cycle continues)."
            + self._rejoin_pointer()
        ))
        return None

    def _anchor_at_or_past(self, dsi: DatasetInfo, destination: str, common_snap, snapshots: list) -> bool:
        """Rule 1 of `_rollback_permit`: this dataset's local anchor for the
        destination names a snapshot in `snapshots` whose `createtxg` is at
        least the common snapshot's (both known)."""
        name = self.local_manager.get_local_anchor(dsi, destination)
        if not name:
            return False
        anchor = next((s for s in snapshots if s.name == name), None)
        return (
            anchor is not None
            and anchor.createtxg is not None and common_snap.createtxg is not None
            and anchor.createtxg >= common_snap.createtxg
        )

    def _log_warning_once(self, key, message: str) -> None:
        """`_log_refusal_once` at WARNING (a condition that heals by itself)."""
        self._call_refusals.add(key)
        if self._memory.note_value(REFUSALS, key, message):
            self._logger.warning(message)
        else:
            self._logger.debug(message)

    def _log_refusal_once(self, key, message: str) -> None:
        self._call_refusals.add(key)
        if self._memory.note_value(REFUSALS, key, message):
            self._logger.error(message)
        else:
            self._logger.debug(message)

    def _on_receiver_capitalized(self) -> str:
        """`_on_receiver` at the start of a sentence."""
        text = self._on_receiver
        return text[:1].upper() + text[1:]

    def _rejoin_pointer(self) -> str:
        """` See <the rejoin section>` to end a refusal with, or `''`."""
        return f" See {self._rejoin_doc}" if self._rejoin_doc else ""

    def _warn_retention_collisions(self, dsi: DatasetInfo, destination: str) -> None:
        """Warn locally about retention-rule collisions in the ruleset that
        will actually be sent for `destination` (dataset-level, or the
        destination's own override if it declared one -- see
        `DatasetConfig.effective_retention_rules`). Once per (dataset,
        destination) for the `SendMemory`'s lifetime; the client's own config is
        loaded once at process start, so unlike the received-side warning in
        `DatasetManager._retention_plan`, the ruleset here cannot change
        without a restart (which creates a new memory and resets this set).
        """
        key = (dsi.name, destination)
        if self._memory.noted(RETENTION_WARNED, key):
            return
        rules = dsi.config.effective_retention_rules(destination)
        _, collisions = _collapse_retention_rules(rules)
        if collisions and self._memory.note(RETENTION_WARNED, key):
            for msg in collisions:
                self._logger.warning(f"Dataset {dsi.name} -> {destination}: {msg}")

    def _transfer(
        self,
        transport: Any,
        client_id: str,
        dsi: DatasetInfo,
        latest,
        common_snap,
        destination: str,
        snapshots: Optional[list] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        mode: str = 'plain',
        discard: Tuple[str, ...] = (),
    ) -> bool:
        """Bring the server up to `latest`, one `zfs send -i` per snapshot.

        After one negotiate, the chain is the managed snapshots newer (by
        `createtxg`) than `common_snap`, oldest first, so every intermediate
        snapshot reaches the server; with no common snapshot only `latest` is
        sent in full (history before the first backup is not backfilled).
        A resumable token of the server runs first, as step 0; then the common
        snapshot is negotiated again and the chain continues from it. Each step:
        `should_stop` (a safe point), the in-flight pin, a `mode=fresh` stream
        and its ack, the anchor, the pin cleared. Any failure stops the chain
        (the next cycle continues from the anchor), except that a step whose
        snapshot has vanished is rebuilt around, once per missing member.
        `mode` is the send mode (`_SEND_MODES`), fixed for the whole chain.
        `discard` (`_rollback_permit`) is the guids of the server snapshots
        every stream of this cycle lets the server destroy.
        A stop mid-chain returns True: the work continues next cycle."""
        if snapshots is None:
            snapshots = self.local_manager.list_snapshots(dsi)
        # Nothing to send: the newest managed snapshot is already on the
        # server. Checked first, before the resume-token fetch, any pin, and
        # (so a dry run does not log a send of nothing) the dry-run notice --
        # `zfs send -i X X` is refused by zfs, and the pin would never be
        # cleared. Skipping the token is safe: a server that holds `latest`
        # as a completed snapshot cannot have partial state for anything
        # newer (nothing newer exists locally), and a stale older token is
        # discarded by the next fresh send (`mode=fresh` aborts it).
        if common_snap is not None and common_snap.name == latest.name:
            return self._settle_up_to_date(dsi, latest, destination, snapshots)

        # Check dry-run *before* starting a real `zfs send` subprocess: a
        # StreamHandle spawned here would otherwise be left unclosed (no
        # wait()/reap, no error surfaced) since we'd return before ever
        # reading or closing it.
        if self.config.dry_run:
            chain = self._plan_chain(snapshots, common_snap)
            self._logger.info(
                f"[DRY RUN] Would send {dsi.name}: "
                + (f"{common_snap.name} -> " if common_snap else "full ")
                + ' -> '.join(s.name for s in chain)
                + f" ({len(chain)} step(s)) -> {self._where(transport)}"
                + (
                    f"; {self._receiver} would be allowed to discard "
                    f"{self._discard_names(dsi, destination, discard)} "
                    f"(a snapshot it holds that {self._sender} does not)"
                    if discard else ""
                )
            )
            return True

        if should_stop is not None and should_stop():
            self._mark_retryable(dsi.name, destination)
            self._logger.info(f"stop requested; not starting a send of {dsi.name}")
            return False

        # Step 0: the server's partial receive, if it can be resumed.
        status, resumed = self._try_resume(
            transport, client_id, dsi, destination, snapshots, mode, should_stop
        )
        if status == 'skip':
            return False
        if resumed is not None:
            finished = resumed.sent.name == latest.name
            outcome = self._push_step(
                transport, client_id, dsi, destination, snapshots, resumed,
                clear_pin=finished, discard=discard,
            )
            if outcome != _OK:
                return outcome == _ANCHOR_FAILED and finished
            if finished:
                return True
            if should_stop is not None and should_stop():
                self._mark_retryable(dsi.name, destination)
                self._logger.info(
                    f"stop requested; {dsi.name} -> {destination} continues to "
                    f"{latest.name} next cycle"
                )
                # Cleanly settled on the anchor: the kept pin is not needed.
                self.local_manager.clear_inflight(dsi, destination)
                return True
            # The resumed snapshot is now on the server: negotiate again and
            # carry on from the new common snapshot.
            snapshots = self.local_manager.list_snapshots(dsi) or snapshots
            try:
                common_name = self._negotiate(transport, client_id, dsi, snapshots, destination)
            except NegotiationFailed as e:
                if e.reloading and not e.zfs_timeout:
                    self._mark_retryable(dsi.name, destination)
                    self._logger.info(
                        f"{self._peer(transport)} is {_retry_word(e, self._busy_prefix)}; "
                        f"{dsi.name} continues next cycle"
                    )
                return False
            latest = snapshots[0]
            common_snap = None
            if common_name:
                common_snap = next((s for s in snapshots if s.name == common_name), None)
                if common_snap is None:
                    self._logger.error(
                        f"{self._peer(transport)} reports {common_name!r} as the common snapshot of "
                        f"{dsi.name}, which is not a managed local snapshot; skipping the rest "
                        "of this cycle rather than sending a full stream"
                    )
                    return False
            discard = self._rollback_permit(dsi, destination, common_snap, snapshots)
            if discard is None:
                return False
            if common_snap is not None and common_snap.name == latest.name:
                return self._settle_up_to_date(dsi, latest, destination, snapshots)

        # The chain of fresh steps.
        sent_any = resumed is not None
        rebuilds = 0
        while True:
            chain = self._plan_chain(snapshots, common_snap)
            if not chain:
                if sent_any:
                    self.local_manager.clear_inflight(dsi, destination)
                return True
            if len(chain) > 1:
                self._logger.info(
                    f"Sending {len(chain)} snapshots of {dsi.name} to {destination}: "
                    f"{common_snap.name if common_snap else 'full'} -> "
                    + ' -> '.join(s.name for s in chain)
                )
            prev = common_snap
            rebuilt = False
            for step in chain:
                # One poll per step, the last look before `zfs send` is spawned
                # (never during a stream). After at least one stream a stop is
                # a clean pause; before the first send nothing was done.
                if should_stop is not None and should_stop():
                    self._mark_retryable(dsi.name, destination)
                    if sent_any:
                        self._logger.info(
                            f"stop requested; {dsi.name} -> {destination} continues to "
                            f"{chain[-1].name} next cycle"
                        )
                        # Settled on the anchor of the step just sent; the pin
                        # kept between steps is not needed any more.
                        self.local_manager.clear_inflight(dsi, destination)
                        return True
                    self._logger.info(f"stop requested; not starting a send of {dsi.name}")
                    return False
                started = self._start_fresh(dsi, destination, prev, step, mode)
                outcome = _FAILED if started is None else self._push_step(
                    transport, client_id, dsi, destination, snapshots, started,
                    clear_pin=step is chain[-1], after_success=sent_any,
                    should_stop=should_stop, discard=discard,
                )
                if outcome == _OK:
                    sent_any = True
                    prev = step
                    continue
                if outcome == _ANCHOR_FAILED:
                    return step.name == chain[-1].name
                if outcome == _FAILED and rebuilds < len(chain):
                    # B7: a member deleted between steps is not an error.
                    fresh = self.local_manager.list_snapshots(dsi)
                    names = {s.name for s in fresh}
                    if (
                        fresh and step.name not in names
                        and (prev is None or prev.name in names)
                    ):
                        rebuilds += 1
                        self._logger.warning(
                            f"{dsi.name}@{step.name} no longer exists; "
                            "rebuilding the rest of the chain without it"
                        )
                        snapshots = fresh
                        common_snap = (
                            next(s for s in fresh if s.name == prev.name) if prev else None
                        )
                        # The failed step may have flagged the call retryable;
                        # the rebuilt chain is a new attempt, and what ends it
                        # sets its own flag (a later non-retryable failure
                        # must not inherit this one).
                        self._call_retryable.pop((dsi.name, destination), None)
                        rebuilt = True
                        break
                return False
            if not rebuilt:
                return True

    @staticmethod
    def _plan_chain(snapshots: list, common_snap) -> list:
        """The snapshots to send, oldest first. With a common snapshot: every
        managed snapshot newer than it by `createtxg`. Without: just the
        newest, in full."""
        if common_snap is None:
            return [snapshots[0]] if snapshots else []
        return [s for s in reversed(snapshots) if snapshot_newer(s, common_snap)]

    def _start_fresh(self, dsi, destination, prev, step, mode: str) -> Optional[_SendStream]:
        """Pin `prev`..`step` (`inflight.<dest>`, written BEFORE `zfs send`
        starts; a failed write is logged and the send proceeds unpinned) and
        start `zfs send [-i prev] step` with the flags of `mode`. `None` (logged)
        when the send cannot be started."""
        inflight_set = self.local_manager.set_inflight(
            dsi, destination, prev.name if prev else None, step.name
        )
        options = _SEND_MODES[mode].options
        try:
            if prev:
                self._logger.info(f"Sending incremental {dsi.name}: {prev.name} -> {step.name}")
                return _SendStream(
                    zfs.send.snapshot(
                        step.snapshot, since=prev.snapshot, **options, **self._send_kwargs()
                    ),
                    'fresh', inflight_set, step, prev,
                )
            self._logger.info(f"Sending full snapshot {dsi.name}@{step.name}")
            return _SendStream(
                zfs.send.snapshot(step.snapshot, **options, **self._send_kwargs()),
                'fresh', inflight_set, step, None
            )
        except Exception as e:
            self._logger.error(f"Failed to start zfs send for {dsi.name}: {e}")
            return None

    def _send_kwargs(self) -> dict:
        """Extra keyword arguments for every `zfs.send.*` start: `{}` for the
        default blocking start-up peek (so the remote calls are unchanged),
        `{'peek': False}` when `_send_peek` is False."""
        return {} if self._send_peek else {'peek': False}

    # --------------------------------------------- what a subclass provides

    def _before_destination_check(self) -> None:
        """Called by `backup_dataset` right before `_ready_transport`: a
        subclass that caches a per-cycle readiness check drops it here when it
        is not running inside a worker cycle. Default: nothing."""

    def _transport_retryable(self, destination: str) -> bool:
        """Whether `_ready_transport` returned `None` for `destination` for a
        reason worth retrying soon (the receiver reloading, unreachable): the
        subclass's answer, read right after the `None`. Default: no."""
        return False

    def _where(self, transport) -> str:
        """Where the transport leads, for log lines (``-> <where>``). Default:
        `transport.base_url`."""
        return transport.base_url

    def _peer(self, transport) -> str:
        """The receiving side as a log-line subject: ``server <where>``."""
        return f"server {self._where(transport)}"

    def _ready_transport(self, destination: str, dest: Destination) -> Any:
        """The destination's transport if it may be used now, else `None`
        (logged by the subclass): the destination is skipped before any send."""
        raise NotImplementedError

    def _negotiate(
        self, transport: Any, client_id: str, dsi: DatasetInfo, snapshots, destination: str,
    ) -> Optional[str]:
        """The newest snapshot present on both sides, or `None` when there is
        none. Raises `NegotiationFailed` when there was no usable answer.
        Fills `_server_info`, `_server_newer` and `_server_newer_status` for
        `(dsi.name, destination)` on success."""
        raise NotImplementedError

    def _fetch_resume_token(
        self, transport, client_id, dsi, destination
    ) -> Tuple[str, Optional[str]]:
        """The receiver's resume token -> `('token', token)`, `('none', None)`
        or `('skip', None)` (no usable answer: the cycle is skipped)."""
        raise NotImplementedError

    def _stream_step(
        self,
        transport: Any,
        client_id: str,
        dsi: DatasetInfo,
        destination: str,
        started: _SendStream,
        after_success: bool = False,
        should_stop: Optional[Callable[[], bool]] = None,
        discard: Tuple[str, ...] = (),
    ) -> str:
        """Carry ONE started `zfs send` (`started`) to the destination and wait
        for its acknowledgement. -> `_OK` (the receiver has the snapshot),
        `_STOP` (nothing more this cycle, no diagnosis) or `_FAILED`. Always
        reaps the started `zfs send`. Records nothing: `_record_step` does."""
        raise NotImplementedError

    def close(self) -> None:
        """Release the transport's resources (idempotent). Default: nothing."""

    # ------------------------------------------------------- one chain step

    def _push_step(
        self,
        transport: Any,
        client_id: str,
        dsi: DatasetInfo,
        destination: str,
        snapshots: list,
        started: _SendStream,
        clear_pin: bool = True,
        after_success: bool = False,
        should_stop: Optional[Callable[[], bool]] = None,
        discard: Tuple[str, ...] = (),
    ) -> str:
        """Send ONE stream (`started`, from `_try_resume` or `_start_fresh`)
        with `_stream_step`, then record the anchor and clear the in-flight pin
        (`_record_step`; unless `clear_pin` is False: between chain
        steps the pin `(prev, sent)` stays, so `prev` is still protected if
        `sent` vanishes unnoticed; the next step's pin overwrites it and
        `_transfer` clears it after the last step or on a clean stop).
        -> `_OK`; `_ANCHOR_FAILED` (the receiver has
        the snapshot but the anchor could not be written: the pin stays and
        the caller stops); `_STOP` (the receiver is reloading or busy, or this
        destination was refused: nothing more this cycle, no diagnosis); or
        `_FAILED` (the transfer did not complete).

        `after_success`, `should_stop` and `discard` are for `_stream_step`
        (the connect retry after an earlier successful step of the cycle; the
        guids of the receiver's snapshots this stream lets it destroy)."""
        outcome = self._stream_step(
            transport, client_id, dsi, destination, started,
            after_success=after_success, should_stop=should_stop, discard=discard,
        )
        if outcome != _OK:
            return outcome
        return self._record_step(
            transport, dsi, destination, snapshots, started, clear_pin=clear_pin
        )

    def _record_step(
        self,
        transport: Any,
        dsi: DatasetInfo,
        destination: str,
        snapshots: list,
        started: _SendStream,
        clear_pin: bool = True,
    ) -> str:
        """The receiver has `started.sent`: write the anchor (never backwards),
        check by guid that the sent snapshot still exists here, and clear the
        in-flight pin when `clear_pin`. -> `_OK`, or `_ANCHOR_FAILED` (the
        anchor could not be written, or the sent snapshot was destroyed or
        cannot be confirmed while it was being sent: the pin stays)."""
        stream, mode, inflight_set, sent, base = started

        # The receiver acknowledged `sent`: the copy exists, whatever happens
        # to the bookkeeping below.
        self._memory.note(COPY_CONFIRMED, (dsi.name, destination))
        self._memory.pop(REFUSALS, (f"{dsi.name} [{self._refusal_label}]", destination))
        # The RAISING read: "unset" and "could not read" differ here. An
        # unreadable anchor must neither skip the never-backwards rule below
        # nor, with no `base`, let the sent-snapshot backstop `clear_anchor` a
        # real anchor. Treated as a failed anchor write: nothing is written
        # or cleared, the pin stays (nothing here has cleared it) and the
        # chain stops; the next cycle negotiates again.
        read_key = (f"{dsi.name} [local anchor after a send]", destination)
        try:
            old_anchor = self.local_manager.get_local_anchor(dsi, destination)
        except LocalAnchorUnreadable as e:
            self._log_warning_once(read_key, (
                f"{dsi.name} -> {destination}: cannot read the local anchor property "
                f"org.zfsbackup:anchor.{destination} ({e}) after sending {sent.name}; no anchor "
                f"is written or cleared and the in-flight pin is kept, so the next cycle "
                f"negotiates again"
            ))
            return _ANCHOR_FAILED
        self._memory.pop(REFUSALS, read_key)
        keep_anchor = False
        if old_anchor and old_anchor != sent.name:
            # Never move an anchor backwards (by createtxg): a snapshot older
            # than the current anchor -- a resumed one, or a chain step after
            # one -- leaves it where it is.
            current = next((s for s in snapshots if s.name == old_anchor), None)
            keep_anchor = snapshot_newer(current, sent)
        if keep_anchor:
            anchored = True
            self._logger.debug(
                f"Keeping anchor {old_anchor}: newer than the resumed {sent.name}"
            )
        else:
            anchored = self.local_manager.set_anchor(dsi, destination, sent.name)
            if old_anchor and old_anchor != sent.name:
                self._logger.debug(f"Previous anchor {old_anchor} superseded by {sent.name}")
        # The in-flight pin has done its job once the anchor protects the new
        # base: cleared whenever the anchor write succeeded, even if THIS
        # attempt's pin could not be written (an older stale pin goes too).
        # If the anchor write failed the pin stays (it still protects both
        # ends; the next send overwrites it). When a NEWER anchor was kept
        # the pin stays as well: nothing was written, and the next step writes
        # its own pin (or the next successful send replaces it).
        if anchored and not keep_anchor:
            # Backstop for the pruner: a destroy that read the anchors before
            # this step pinned its pair could still have taken `sent` while it
            # was being sent. Check, before the pin is released, that it is
            # still there. If not, put the anchor back on the server's
            # previous common snapshot (never backwards past it), keep the
            # pin and stop. (A `zfs hold` on the in-flight pair would be the
            # airtight alternative; deferred, since holds left by a crash
            # would need cleaning up.)
            present = self._snapshot_present(sent)
            if present is not True:
                restore = base.name if base is not None else old_anchor
                if present is False:
                    self._logger.error(
                        f"{dsi.name}@{sent.name} was destroyed while it was being sent to "
                        f"{destination}; {self._receiver} has it but {self._sender} no longer does. "
                        + (f"Putting the anchor back on {restore}. " if restore else "")
                        + "Stopping; the next cycle negotiates again"
                    )
                    if restore:
                        self.local_manager.set_anchor(dsi, destination, restore)
                    else:
                        self.local_manager.clear_anchor(dsi, destination)
                else:
                    self._logger.warning(
                        f"cannot confirm that {dsi.name}@{sent.name} still exists; keeping "
                        f"its in-flight pin and stopping"
                    )
                return _ANCHOR_FAILED
            if clear_pin:
                self.local_manager.clear_inflight(dsi, destination)

        if mode == 'resume':
            self._logger.info(
                f"Resumed {dsi.name}@{sent.name} "
                f"(from {base.name if base is not None else 'full'}) to {destination}"
            )
        else:
            self._logger.info(
                f"Backup complete: {dsi.name}@{sent.name}"
                + (f" (incremental from {base.name})" if base is not None else " (full)")
                + f" -> {self._where(transport)}"
            )
        return _OK if anchored else _ANCHOR_FAILED


    @staticmethod
    def _snapshot_present(sent) -> Optional[bool]:
        """Does the snapshot `sent` still exist, and is it still the same one
        (GUID)? One `zfs get` of that snapshot. `None` when zfs fails for any
        other reason."""
        try:
            guid = zfs.get.value(sent.snapshot, 'guid')
        except Exception as e:
            if 'does not exist' in str(e).lower():
                return False
            return None
        if guid is None or sent.guid is None:
            return True if guid is not None else None
        try:
            return int(guid, 0) == sent.guid
        except ValueError:
            return None

    def _settle_up_to_date(self, dsi, latest, destination, snapshots) -> bool:
        """`latest` is already on the server: make the bookkeeping say so.
        The anchor is written if it is missing or points elsewhere (never
        backwards: an anchor on a newer managed snapshot is left), and a stale
        `inflight.<dest>` pin is cleared once the anchor is right. Writes
        nothing in a dry run.

        -> `True` (settled, or deliberately left as it is), or `False` when
        the local anchor could not be read: it may name a snapshot this host
        lacks (rollback evidence), so nothing is written or cleared, one
        WARNING per episode, and the caller reports the cycle as not
        completed, as `_rollback_permit` does for the same read."""
        self._logger.debug(
            f"Nothing to send for {dsi.name} -> {destination}: "
            f"{latest.name} is already {self._at_receiver}"
        )
        self._token_ok(dsi, destination)
        if self.config.dry_run:
            return True
        read_key = (f"{dsi.name} [local anchor to settle]", destination)
        try:
            anchor = self.local_manager.get_local_anchor(dsi, destination)
        except LocalAnchorUnreadable as e:
            self._log_warning_once(read_key, (
                f"{dsi.name} -> {destination}: cannot read the local anchor property "
                f"org.zfsbackup:anchor.{destination} ({e}), so the anchor and the in-flight "
                f"pin are left as they are although {latest.name} is {self._at_receiver}; "
                "the next cycle retries"
            ))
            return False
        self._memory.pop(REFUSALS, read_key)
        anchored = anchor == latest.name
        wrote = False
        if not anchored:
            newer = False
            current = None
            if anchor and snapshots:
                current = next((s for s in snapshots if s.name == anchor), None)
                newer = snapshot_newer(current, latest)
            if (
                anchor and snapshots and current is None
                and self._server_newer_status.get((dsi.name, destination), 'absent') != 'absent'
            ):
                # The anchor names a snapshot this host no longer has (a
                # rollback or an in-place restore, or the pruner after a
                # backstop). A server that reports B-2b state may hold that
                # snapshot, so moving the anchor to `latest` would let a later
                # cycle mistake the rollback for the backstop and send
                # `discard`. Left as it is; the pin stays too.
                self._logger.debug(
                    f"{dsi.name} -> {destination}: the anchor {anchor!r} names a snapshot this "
                    f"host does not have; left unchanged although {latest.name} is {self._at_receiver}"
                )
                return True
            wrote = not newer
            anchored = newer or self.local_manager.set_anchor(dsi, destination, latest.name)
        if anchored and self.local_manager.get_own_inflight(dsi, destination) is not None:
            # As after a step: release the pin only if the snapshot is really
            # still here (the pruner may have taken it meanwhile). If it is
            # gone, put the anchor back where it was and keep the pin; if that
            # cannot be told, keep the pin.
            present = self._snapshot_present(latest)
            if present is not True:
                if present is False:
                    self._logger.error(
                        f"{dsi.name}@{latest.name} no longer exists although {destination} "
                        "holds it; keeping its in-flight pin"
                    )
                    if wrote:
                        if anchor:
                            self.local_manager.set_anchor(dsi, destination, anchor)
                        else:
                            self.local_manager.clear_anchor(dsi, destination)
                else:
                    self._logger.warning(
                        f"cannot confirm that {dsi.name}@{latest.name} still exists; "
                        "keeping its in-flight pin"
                    )
                return True
            self.local_manager.clear_inflight(dsi, destination)
        return True

    def _resume_target(self, token: str, dsi: DatasetInfo, snapshots, mode: str = 'plain') -> object:
        """Decide whether `token` can be resumed: -> `(to_snap, from_snap)`
        (`SnapshotInfo`s, `from_snap` `None` for a full stream), or a `str`
        giving the reason it must not be. Matching is by GUID, never by the
        token's `toname`, which goes stale when the sender renames. Raises (the
        caller skips the cycle) when the token cannot be decoded for any reason
        other than being corrupt, when zfs reports an error that is not a
        definitive "snapshots gone", or when the local snapshots' GUIDs cannot
        be read."""
        try:
            info = zfs.send.resume_info(token)
        except ResumeTokenError as e:
            return f"the resume token is corrupt or unusable: {e}"
        # Any other failure to decode PROPAGATES: not knowing is not a reason
        # to make the server discard its progress (the caller skips the cycle).
        if info.error:
            if _token_error_is_definitive(info.error):
                return f"zfs says the token cannot be resumed: {info.error}"
            raise RuntimeError(
                f"zfs reports a problem with the token but not a definitive one: {info.error}"
            )
        send_mode = _SEND_MODES[mode]
        if not (send_mode.required <= info.flags <= send_mode.allowed):
            return (
                "the token records stream features this daemon does not send in "
                f"{mode} mode (unexpected: "
                f"{', '.join(sorted(info.flags - send_mode.allowed)) or 'none'}; "
                f"missing: {', '.join(sorted(send_mode.required - info.flags)) or 'none'})"
            )
        if info.toguid is None:
            return "the token does not name its target snapshot"
        # GUIDs come from the one managed listing (`SnapshotInfo.guid`); only a
        # managed snapshot can match, as before. A managed snapshot whose
        # GUID was not listed makes "no match" uncertain, not definitive.
        by_guid = {s.guid: s for s in snapshots if s.guid is not None}
        unknown = [s.name for s in snapshots if s.guid is None]

        def _lookup(guid):
            found = by_guid.get(guid)
            if found is None and unknown:
                raise RuntimeError(
                    f"cannot read the GUID of local snapshot(s) {', '.join(unknown[:3])}"
                )
            return found

        to_snap = _lookup(info.toguid)
        if to_snap is None:
            return (
                f"its target snapshot (guid {info.toguid}, {info.toname}) is not a "
                "managed local snapshot of this dataset"
            )
        from_snap = None
        if info.fromguid is not None:
            from_snap = _lookup(info.fromguid)
            if from_snap is None:
                return (
                    f"its incremental source (guid {info.fromguid}) is not a "
                    "managed local snapshot of this dataset"
                )
            if not snapshot_newer(to_snap, from_snap):
                return "its incremental source is not older than its target"
        return to_snap, from_snap

    def _try_resume(
        self, transport, client_id, dsi, destination, snapshots, mode: str,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> Tuple[str, Optional[_SendStream]]:
        """Step 0: the server's resume token -> `('skip', None)` (no usable
        answer: skip the cycle), `('fresh', None)` (nothing to resume, or the
        token cannot be: carry on with a fresh send), or `('resume',
        stream)` with `zfs send -t <token>` started (`mode=resume`).

        The token is read first (`_fetch_resume_token`; no usable answer skips
        the cycle). It is resumed only if `_resume_target` accepts it -- no
        zfs error, only the stream features of `mode` (`_SEND_MODES`), its
        target (and incremental source) are managed local snapshots by GUID.
        Anything else (stale, corrupt, foreign, or `zfs send -t` failing to
        start) is logged at WARNING and falls through to a FRESH send (the
        server discards the stale state). A resume pins both of its snapshots
        (`inflight.<dest>`) BEFORE `zfs send` starts; a failed write is logged
        and the send proceeds unpinned. A failed send leaves the pin in place."""
        status, token = self._fetch_resume_token(transport, client_id, dsi, destination)
        if status == 'skip':
            return 'skip', None
        if not token:
            self._token_ok(dsi, destination)
            return 'fresh', None
        try:
            target = self._resume_target(token, dsi, snapshots, mode)
        except Exception as e:
            self._logger.warning(
                f"cannot check the resume token of {dsi.name}: {e}; skipping this "
                f"cycle rather than risk discarding {self._receiver}'s partial progress "
                "with a fresh send"
            )
            self._token_skipped(dsi, destination, f"cannot check the token: {e}")
            return 'skip', None
        self._token_ok(dsi, destination)
        if isinstance(target, str):
            self._logger.warning(
                f"Not resuming {dsi.name}: {target}; sending fresh ({self._receiver} "
                "discards its partial state)"
            )
            return 'fresh', None
        to_snap, from_snap = target
        if should_stop is not None and should_stop():
            self._mark_retryable(dsi.name, destination)
            self._logger.info(f"stop requested; not resuming {dsi.name}")
            return 'skip', None
        pinned = self.local_manager.set_inflight(
            dsi, destination, from_snap.name if from_snap else None, to_snap.name
        )
        try:
            self._logger.info(f"Resuming interrupted transfer for {dsi.name}@{to_snap.name}")
            return 'resume', _SendStream(
                zfs.send.resume(token, **self._send_kwargs()), 'resume', pinned, to_snap, from_snap
            )
        except Exception as e:
            self._logger.warning(
                f"zfs send -t failed to start for {dsi.name}: {e}; sending "
                f"fresh ({self._receiver} discards its partial state)"
            )
            return 'fresh', None

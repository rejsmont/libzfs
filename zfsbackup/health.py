"""Backup health: how far behind is each snapshot, send and received copy?

**Stdlib only, no `BackupConfig`, no ZFS.** `zfsbackup-config check` imports this
module (and, lazily, `zfsbackup.health_collect`, which does the ZFS reading), and
the daemon's workers import it for the status files (S-7, the last section).
Everything here works on plain values: seconds, epoch timestamps and names.

The flow is `health_collect.collect(config)` -> observations ->
`evaluate(observations, now)` -> entries -> `to_json` / `to_prometheus` /
`overall_state`.

**Semantics (decision C9).** Each observation measures an age against an
*expected interval*; the entry is WARN from `warn_factor` x the interval and CRIT
from `crit_factor` x it (defaults 2 and 4), both inclusive:

* `snapshot`: the newest managed snapshot of a configured dataset, against the
  dataset's `frequency`.
* `anchor`: the snapshot last sent to one destination, against
  `max(dataset frequency, destination frequency)`. A destination with no anchor
  yet is WARN ("not yet sent") until the oldest managed snapshot is itself
  older than the CRIT threshold, then CRIT.
* `received`: on a server, the newest snapshot of a received client dataset,
  against the largest of that client's frequencies. The same "nothing yet" rule
  as `anchor` applies.
* A newest snapshot dated further in the future than the observation's
  `tolerance_seconds` is CRIT, "clock behind newest snapshot".
* Anything the collector could not read (`error`) is CRIT.

**Output contract (C13, and S-7's additions).**

* JSON carries `"schema": 1`. Changes are additive only: a consumer must ignore
  keys it does not know. The overall `"state"` is the worst entry's state, so a
  future source can only make the verdict worse, never better.
* Prometheus metric names are prefixed by their source: `zfsbackup_snapshot_*`,
  `zfsbackup_anchor_*`, `zfsbackup_received_*`; S-7 adds `zfsbackup_worker_*`
  without renaming anything. Label values are escaped per the exposition format.
* A source that has no data is *no entry*, not a CRIT entry (a missing
  `<db>.status/` directory in S-7, for one).

States are `OK`, `WARN` and `CRIT` (`STATE_VALUES`: 0, 1, 2).
"""

from __future__ import annotations

import errno
import json
import logging
import os
import re
import stat as stat_module
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Callable, Collection, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union,
)

logger = logging.getLogger(__name__)

#: Version of the JSON document. Additive changes only; bump it for anything else.
JSON_SCHEMA = 1

STATE_OK = 'OK'
STATE_WARN = 'WARN'
STATE_CRIT = 'CRIT'

#: State -> number, worst highest (also the Prometheus value).
STATE_VALUES: Mapping[str, int] = {STATE_OK: 0, STATE_WARN: 1, STATE_CRIT: 2}

DEFAULT_WARN_FACTOR = 2.0
DEFAULT_CRIT_FACTOR = 4.0

SOURCE_SNAPSHOT = 'snapshot'
SOURCE_ANCHOR = 'anchor'
SOURCE_RECEIVED = 'received'

#: Sources that measure a lag, in output order.
LAG_SOURCES: Tuple[str, ...] = (SOURCE_SNAPSHOT, SOURCE_ANCHOR, SOURCE_RECEIVED)

#: The label that names an entry's `peer` in each source's metrics.
PEER_LABEL = {SOURCE_ANCHOR: 'destination', SOURCE_RECEIVED: 'client'}

_WHAT = {
    SOURCE_SNAPSHOT: 'newest snapshot',
    SOURCE_ANCHOR: 'last sent snapshot',
    SOURCE_RECEIVED: 'newest received snapshot',
}


# --------------------------------------------------------------------------
# Value types
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class CheckEntry:
    """One check's verdict. `source` groups it (`snapshot`, `anchor`,
    `received`, or anything a caller adds: `daemon`, `config`, ...); `subject`
    is what it is about (a dataset name, ...); `peer` is the destination or
    client it concerns, if any."""

    source: str
    subject: str
    state: str
    reason: str
    peer: Optional[str] = None


@dataclass(frozen=True)
class LagEntry(CheckEntry):
    """A `CheckEntry` that measured a lag. `age_seconds` is `None` when there
    was nothing to measure (no snapshot, no anchor, unreadable)."""

    age_seconds: Optional[float] = None
    expected_seconds: Optional[float] = None
    warn_seconds: Optional[float] = None
    crit_seconds: Optional[float] = None


@dataclass(frozen=True)
class Observation:
    """What the collector saw, as plain values; `evaluate` turns it into a
    `LagEntry`.

    `newest_ts` is the epoch time in the newest snapshot's name (for an anchor:
    in the anchor's name); `None` = there is none to measure. `oldest_ts` is the
    oldest managed snapshot's, used only for the "nothing sent yet" grace.
    `tolerance_seconds` is how far ahead of `now` a name may be before it
    means the clock is wrong. `error` marks a failed read (-> CRIT). `detail`
    is free text (a snapshot name, a pin) added to the reason.
    """

    source: str
    subject: str
    expected_seconds: float
    peer: Optional[str] = None
    newest_ts: Optional[float] = None
    oldest_ts: Optional[float] = None
    tolerance_seconds: float = 300.0
    error: Optional[str] = None
    detail: Optional[str] = None


# --------------------------------------------------------------------------
# Evaluation
# --------------------------------------------------------------------------

def format_duration(seconds: float) -> str:
    """`90061` -> `1d1h1m`; whole minutes, `0m` for less; negative gets a `-`."""
    if seconds < 0:
        return '-' + format_duration(-seconds)
    total = int(seconds)
    days, rest = divmod(total, 86400)
    hours, rest = divmod(rest, 3600)
    minutes = rest // 60
    parts = []
    if days:
        parts.append(f'{days}d')
    if hours:
        parts.append(f'{hours}h')
    if minutes or not parts:
        parts.append(f'{minutes}m')
    return ''.join(parts)


def _epoch(now: Union[float, int, datetime]) -> float:
    if isinstance(now, datetime):
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now.timestamp()
    return float(now)


def _grade(age: float, warn: float, crit: float) -> str:
    if age >= crit:
        return STATE_CRIT
    if age >= warn:
        return STATE_WARN
    return STATE_OK


def _evaluate_one(obs: Observation, now: float, warn_factor: float, crit_factor: float) -> LagEntry:
    expected = float(obs.expected_seconds)
    warn = expected * warn_factor
    crit = expected * crit_factor
    what = _WHAT.get(obs.source, 'newest snapshot')
    base = dict(
        source=obs.source, subject=obs.subject, peer=obs.peer,
        expected_seconds=expected, warn_seconds=warn, crit_seconds=crit,
    )

    def entry(state: str, reason: str, age: Optional[float] = None) -> LagEntry:
        if obs.detail and state != STATE_OK:
            reason = f'{reason} ({obs.detail})'
        return LagEntry(state=state, reason=reason, age_seconds=age, **base)

    if obs.error:
        return entry(STATE_CRIT, f'cannot read: {obs.error}')

    if obs.newest_ts is None:
        if obs.source == SOURCE_SNAPSHOT:
            return entry(STATE_CRIT, 'no managed snapshots')
        # Nothing sent / received yet: give it the CRIT threshold's worth of
        # snapshot history before it counts as broken.
        if obs.oldest_ts is None:
            return entry(STATE_WARN, 'not yet sent: there is no managed snapshot to send'
                         if obs.source == SOURCE_ANCHOR else 'no managed snapshots yet')
        oldest_age = now - obs.oldest_ts
        word = 'not yet sent' if obs.source == SOURCE_ANCHOR else 'no managed snapshots yet'
        if oldest_age >= crit:
            return entry(
                STATE_CRIT,
                f'{word}, and the oldest snapshot is {format_duration(oldest_age)} old '
                f'(crit at {format_duration(crit)})',
            )
        return entry(STATE_WARN, word)

    age = now - obs.newest_ts
    if age < -float(obs.tolerance_seconds):
        return entry(
            STATE_CRIT,
            f'clock behind newest snapshot: it is dated {format_duration(-age)} in the future',
            age,
        )
    age = max(age, 0.0)
    state = _grade(age, warn, crit)
    reason = f'{what} is {format_duration(age)} old; expected every {format_duration(expected)}'
    if state == STATE_WARN:
        reason += f' (warn at {format_duration(warn)})'
    elif state == STATE_CRIT:
        reason += f' (crit at {format_duration(crit)})'
    if obs.detail and state == STATE_OK:
        reason += f' ({obs.detail})'
    return entry(state, reason, age)


def evaluate(
    observations: Iterable[Union[Observation, CheckEntry]],
    now: Union[float, int, datetime],
    warn_factor: float = DEFAULT_WARN_FACTOR,
    crit_factor: float = DEFAULT_CRIT_FACTOR,
) -> List[CheckEntry]:
    """Grade `observations` at time `now` (epoch seconds, or an aware
    datetime); -> one entry each, in order. A `CheckEntry` in the input (a check
    that is not a lag: dry run, daemon presence) passes through unchanged.
    Pure: no I/O, no clock. Raises `ValueError` unless
    `0 < warn_factor <= crit_factor`."""
    if not (0 < warn_factor <= crit_factor):
        raise ValueError(
            f'need 0 < warn_factor <= crit_factor, got {warn_factor!r} and {crit_factor!r}'
        )
    at = _epoch(now)
    result: List[CheckEntry] = []
    for obs in observations:
        if isinstance(obs, Observation):
            result.append(_evaluate_one(obs, at, warn_factor, crit_factor))
        else:
            result.append(obs)
    return result


def overall_state(entries: Iterable[CheckEntry]) -> str:
    """The worst state among `entries` (`OK` for none)."""
    worst = STATE_OK
    for entry in entries:
        if STATE_VALUES.get(entry.state, 2) > STATE_VALUES[worst]:
            worst = entry.state if entry.state in STATE_VALUES else STATE_CRIT
    return worst


# --------------------------------------------------------------------------
# JSON
# --------------------------------------------------------------------------

def entry_dict(entry: CheckEntry) -> dict:
    """The JSON object of one entry. `peer` and the lag numbers are present
    only when the entry has them."""
    data: dict = {
        'source': entry.source,
        'subject': entry.subject,
        'state': entry.state,
        'reason': entry.reason,
    }
    if entry.peer is not None:
        data['peer'] = entry.peer
    if isinstance(entry, LagEntry):
        data['age_seconds'] = entry.age_seconds
        data['expected_seconds'] = entry.expected_seconds
        data['warn_seconds'] = entry.warn_seconds
        data['crit_seconds'] = entry.crit_seconds
    return data


def to_json(
    entries: Sequence[CheckEntry],
    now: Union[float, int, datetime],
    version: Optional[str] = None,
    *,
    indent: Optional[int] = 2,
) -> str:
    """The report as JSON: `{"schema": 1, "version", "generated_at",
    "state", "entries": [...]}`. Consumers ignore unknown keys."""
    at = _epoch(now)
    document = {
        'schema': JSON_SCHEMA,
        'version': version,
        'generated_at': datetime.fromtimestamp(at, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'state': overall_state(entries),
        'entries': [entry_dict(e) for e in entries],
    }
    return json.dumps(document, indent=indent, ensure_ascii=True)


# --------------------------------------------------------------------------
# Prometheus text exposition
# --------------------------------------------------------------------------

def escape_label_value(value: str) -> str:
    """Escape a label value per the exposition format: backslash, double quote
    and newline."""
    return value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')


def _sample(name: str, labels: Sequence[Tuple[str, str]], value: Union[int, float]) -> str:
    if labels:
        inner = ','.join(f'{key}="{escape_label_value(val)}"' for key, val in labels)
        head = f'{name}{{{inner}}}'
    else:
        head = name
    text = repr(float(value)) if isinstance(value, float) else str(value)
    return f'{head} {text}'


def _entry_labels(entry: CheckEntry) -> List[Tuple[str, str]]:
    labels = [('dataset', entry.subject)]
    if entry.peer is not None:
        labels.append((PEER_LABEL.get(entry.source, 'peer'), entry.peer))
    return labels


def to_prometheus(
    entries: Sequence[CheckEntry],
    now: Union[float, int, datetime],
    version: Optional[str] = None,
) -> str:
    """The report in the Prometheus text exposition format (for node_exporter's
    textfile collector, or stdout).

    Per lag source `S` in `snapshot`, `anchor`, `received`:
    `zfsbackup_S_age_seconds` (only where an age exists),
    `zfsbackup_S_threshold_seconds{level="warn"|"crit"}` and
    `zfsbackup_S_check_state` (0 OK, 1 WARN, 2 CRIT), labelled `dataset` and,
    for `anchor`, `destination`, for `received`, `client`. Any other entry is
    `zfsbackup_check_state{source,subject[,peer]}`. Then
    `zfsbackup_check_overall_state`, `zfsbackup_check_success` (1 when every
    entry is OK), `zfsbackup_check_timestamp_seconds` and
    `zfsbackup_build_info{version}`."""
    at = _epoch(now)
    blocks: List[Tuple[str, str, str, List[str]]] = []  # (name, type, help, samples)

    lag = [e for e in entries if isinstance(e, LagEntry)]
    for source in LAG_SOURCES:
        group = [e for e in lag if e.source == source]
        if not group:
            continue
        ages = [
            _sample(f'zfsbackup_{source}_age_seconds', _entry_labels(e), e.age_seconds)
            for e in group if e.age_seconds is not None
        ]
        thresholds = []
        for e in group:
            for level, value in (('warn', e.warn_seconds), ('crit', e.crit_seconds)):
                if value is not None:
                    thresholds.append(_sample(
                        f'zfsbackup_{source}_threshold_seconds',
                        _entry_labels(e) + [('level', level)], value,
                    ))
        states = [
            _sample(f'zfsbackup_{source}_check_state', _entry_labels(e), STATE_VALUES.get(e.state, 2))
            for e in group
        ]
        if ages:
            blocks.append((
                f'zfsbackup_{source}_age_seconds', 'gauge',
                f'Age of the {_WHAT[source]} in seconds.', ages,
            ))
        blocks.append((
            f'zfsbackup_{source}_threshold_seconds', 'gauge',
            'Age at which the check turns WARN or CRIT, in seconds.', thresholds,
        ))
        blocks.append((
            f'zfsbackup_{source}_check_state', 'gauge',
            'Check state: 0 OK, 1 WARN, 2 CRIT.', states,
        ))

    others = [e for e in entries if not isinstance(e, LagEntry)]
    if others:
        samples = []
        for e in others:
            labels = [('source', e.source), ('subject', e.subject)]
            if e.peer is not None:
                labels.append(('peer', e.peer))
            samples.append(_sample('zfsbackup_check_state', labels, STATE_VALUES.get(e.state, 2)))
        blocks.append(('zfsbackup_check_state', 'gauge', 'Check state: 0 OK, 1 WARN, 2 CRIT.', samples))

    overall = overall_state(entries)
    blocks.append((
        'zfsbackup_check_overall_state', 'gauge',
        'Worst check state: 0 OK, 1 WARN, 2 CRIT.',
        [_sample('zfsbackup_check_overall_state', [], STATE_VALUES[overall])],
    ))
    blocks.append((
        'zfsbackup_check_success', 'gauge',
        '1 if every check is OK.',
        [_sample('zfsbackup_check_success', [], 1 if overall == STATE_OK else 0)],
    ))
    blocks.append((
        'zfsbackup_check_timestamp_seconds', 'gauge',
        'Unix time of this check.',
        [_sample('zfsbackup_check_timestamp_seconds', [], int(at))],
    ))
    blocks.append((
        'zfsbackup_build_info', 'gauge',
        'Version of zfsbackup that produced this report.',
        [_sample('zfsbackup_build_info', [('version', version or 'unknown')], 1)],
    ))

    lines: List[str] = []
    for name, kind, helptext, samples in blocks:
        lines.append(f'# HELP {name} {helptext}')
        lines.append(f'# TYPE {name} {kind}')
        lines.extend(samples)
    return '\n'.join(lines) + '\n'


# --------------------------------------------------------------------------
# Atomic output
# --------------------------------------------------------------------------

def write_text_atomic(path: Union[str, 'os.PathLike[str]'], text: str, mode: int = 0o644) -> None:
    """Write `text` to `path` so that a reader (node_exporter's textfile
    collector) sees the old file or the new one, never a partial one: a
    temporary file in the same directory, fsynced, then `os.replace`d. `mode`
    is set exactly, whatever the umask."""
    target = os.fspath(path)
    directory = os.path.dirname(os.path.abspath(target))
    fd, tmp = tempfile.mkstemp(prefix=f'.{os.path.basename(target)}.', suffix='.tmp', dir=directory)
    try:
        try:
            os.fchmod(fd, mode)
            data = text.encode('utf-8')
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view):]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, target)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# --------------------------------------------------------------------------
# Worker status files (S-7)
# --------------------------------------------------------------------------
#
# `<db>.status/<worker>.json`, one file per worker, written only by that
# worker (the supervisor creates the directory, nothing else writes into it)
# and read by `zfsbackup-config check` / `status`. A record per
# `(kind, dataset, destination)` says when the last attempt was, when the last
# success was, what the last error said and how many attempts in a row have
# failed. Nothing is written under `--dry-run` (a dry run's "success" is not
# one). A missing directory or file is "no data", never a failure: an older
# daemon writes none. A worker rewrites its file at the end of every cycle even
# when no record changed (`updated_at` is a heartbeat, so a reader can tell a
# quiet worker from a dead one); between cycles it writes only on a change.
#
# Document (`"schema": 1`; additive changes only, a reader ignores unknown keys):
#
#     {"schema": 1, "worker": "remote-backup-worker", "pid": 4242,
#      "started_at": "2026-10-25T10:00:00Z", "updated_at": "2026-10-25T10:05:12Z",
#      "records": [
#        {"kind": "send", "dataset": "tank/data", "destination": "offsite",
#         "result": "error", "last_attempt": "2026-10-25T10:05:12Z",
#         "last_success": "2026-10-25T08:05:10Z",
#         "last_error": "resume token check failed: ...",
#         "last_error_at": "2026-10-25T10:05:12Z", "consecutive_failures": 3}]}
#
# Keys with no value are left out. Timestamps are UTC, whole seconds. Free
# text is clipped to `STATUS_TEXT_LIMIT` characters and carries no secrets
# (destination *names*, never URLs or tokens, identify a destination).

#: Version of the status document. Additive changes only; bump it for anything else.
STATUS_SCHEMA = 1
STATUS_DIR_SUFFIX = '.status'
#: The directory is `0750` in the DB file's group, the files `0640`, like `<db>.tls/`.
STATUS_DIR_MODE = 0o750
STATUS_FILE_MODE = 0o640
#: A reader refuses a file larger than this (a full file is about 0.5 MiB).
STATUS_MAX_BYTES = 2 * 1024 * 1024
#: Records kept per file; over it the oldest attempt is dropped.
STATUS_MAX_RECORDS = 2048
STATUS_TEXT_LIMIT = 300
#: A `StatusWriter` rewrites its file at most this often (seconds), except on
#: `flush(force=True)`: the cost stays bounded however many datasets a worker has.
STATUS_WRITE_INTERVAL = 2.0
#: A leftover temporary file of a killed writer is removed once it is this old.
STATUS_STALE_TMP_SECONDS = 3600.0

KIND_SNAPSHOT = 'snapshot'
KIND_PRUNE = 'prune'
KIND_SEND = 'send'
#: The pruning worker's enumeration of the datasets received from clients (a
#: server only): `dataset` is the backup target, never a client's dataset.
KIND_DISCOVER = 'discover'
#: What the daemon records today (a reader accepts other kinds and ignores them).
STATUS_KINDS: Tuple[str, ...] = (KIND_SNAPSHOT, KIND_PRUNE, KIND_SEND, KIND_DISCOVER)

RESULT_OK = 'ok'
RESULT_ERROR = 'error'

_WORKER_RE = re.compile(r'^[a-z][a-z0-9-]{0,39}$')
_KIND_RE = re.compile(r'^[a-z][a-z0-9_-]{0,31}$')
_NAME_LIMIT = 512
_ISO_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

_O_CLOEXEC = getattr(os, 'O_CLOEXEC', 0)
_O_NOFOLLOW = getattr(os, 'O_NOFOLLOW', 0)
_O_DIRECTORY = getattr(os, 'O_DIRECTORY', 0)
_O_NONBLOCK = getattr(os, 'O_NONBLOCK', 0)


class StatusError(Exception):
    """A status directory or file that cannot be used; the message says why."""


def status_dir(db_path: Union[str, 'os.PathLike[str]']) -> Path:
    """`<db>.status` for a config database path. Pure path arithmetic."""
    db = Path(db_path)
    return db.with_name(db.name + STATUS_DIR_SUFFIX)


def status_path(directory: Union[str, 'os.PathLike[str]'], worker: str) -> Path:
    """`<directory>/<worker>.json`; `worker` is a worker's process name
    (`snapshot-worker`, ...). Raises `ValueError` for any other string."""
    if not _WORKER_RE.match(worker):
        raise ValueError(f'not a worker name: {worker!r}')
    return Path(directory) / f'{worker}.json'


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(_ISO_FORMAT)


def _parse_iso(value: object) -> Optional[float]:
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, _ISO_FORMAT).replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


def clip_text(text: object, limit: int = STATUS_TEXT_LIMIT) -> str:
    """One line of at most `limit` characters: control characters and runs of
    whitespace become one space, a cut ends in `...`."""
    flat = ' '.join(''.join(c if c.isprintable() else ' ' for c in str(text)).split())
    return flat if len(flat) <= limit else flat[: max(limit - 3, 0)] + '...'


# -- value types ----------------------------------------------------------

@dataclass
class OutcomeRecord:
    """The outcomes of one thing a worker does repeatedly: snapshotting or
    pruning `dataset` (`destination` is `None`), or sending it to `destination`.
    Times are epoch seconds. `result` is the last attempt's (`ok`/`error`);
    `last_error` is kept after a later success (when it happened is
    `last_error_at`). `detail` is a short note about the last attempt (the
    snapshot created, how many were pruned)."""

    kind: str
    dataset: str
    destination: Optional[str] = None
    result: Optional[str] = None
    last_attempt: Optional[float] = None
    last_success: Optional[float] = None
    last_error: Optional[str] = None
    last_error_at: Optional[float] = None
    consecutive_failures: int = 0
    detail: Optional[str] = None

    @property
    def key(self) -> Tuple[str, str, Optional[str]]:
        return (self.kind, self.dataset, self.destination)


@dataclass(frozen=True)
class WorkerStatus:
    """One parsed status file. `started_at` is when that worker process began,
    `updated_at` when the file was last written; `pid` is the writer's."""

    worker: str
    path: str
    pid: Optional[int]
    started_at: Optional[float]
    updated_at: Optional[float]
    records: Tuple[OutcomeRecord, ...]


@dataclass(frozen=True)
class StatusReadResult:
    """`read_status_dir`'s answer. `present` is False when there is no
    directory at all ("no data": an older daemon, or nothing written yet).
    `problems` are human-readable lines about a directory or file that was
    refused or unreadable; the readable files are still in `statuses`."""

    present: bool
    statuses: Tuple[WorkerStatus, ...] = ()
    problems: Tuple[str, ...] = ()


def _record_to_json(rec: OutcomeRecord) -> dict:
    data: dict = {'kind': rec.kind, 'dataset': rec.dataset}
    if rec.destination is not None:
        data['destination'] = rec.destination
    if rec.result is not None:
        data['result'] = rec.result
    for name in ('last_attempt', 'last_success', 'last_error_at'):
        value = getattr(rec, name)
        if value is not None:
            data[name] = _iso(value)
    if rec.last_error is not None:
        data['last_error'] = rec.last_error
    data['consecutive_failures'] = rec.consecutive_failures
    if rec.detail is not None:
        data['detail'] = rec.detail
    return data


def _opt_text(value: object) -> Optional[str]:
    return clip_text(value) if isinstance(value, str) and value else None


def _record_from_json(obj: object) -> Optional[OutcomeRecord]:
    """A record, or `None` for anything malformed (the reader skips it)."""
    if not isinstance(obj, dict):
        return None
    kind, dataset, dest = obj.get('kind'), obj.get('dataset'), obj.get('destination')
    if not (isinstance(kind, str) and _KIND_RE.match(kind)):
        return None
    if not (isinstance(dataset, str) and 0 < len(dataset) <= _NAME_LIMIT):
        return None
    if dest is not None and not (isinstance(dest, str) and 0 < len(dest) <= _NAME_LIMIT):
        return None
    failures = obj.get('consecutive_failures', 0)
    if isinstance(failures, bool) or not isinstance(failures, int) or failures < 0:
        failures = 0
    result = obj.get('result')
    return OutcomeRecord(
        kind=kind, dataset=dataset, destination=dest,
        result=result if result in (RESULT_OK, RESULT_ERROR) else None,
        last_attempt=_parse_iso(obj.get('last_attempt')),
        last_success=_parse_iso(obj.get('last_success')),
        last_error=_opt_text(obj.get('last_error')),
        last_error_at=_parse_iso(obj.get('last_error_at')),
        consecutive_failures=min(failures, 10**9),
        detail=_opt_text(obj.get('detail')),
    )


# -- directory and file handling ------------------------------------------

def _db_of(directory: Path) -> Optional[Path]:
    name = directory.name
    if name.endswith(STATUS_DIR_SUFFIX) and len(name) > len(STATUS_DIR_SUFFIX):
        return directory.with_name(name[: -len(STATUS_DIR_SUFFIX)])
    return None


def _db_group(directory: Path) -> Optional[int]:
    """The gid to give a new status directory: the DB file's (as for
    `<db>.tls/`), if this process may chgrp to it (root, or a member)."""
    db = _db_of(directory)
    if db is None:
        return None
    try:
        gid = os.stat(db).st_gid
    except OSError:
        return None
    if os.geteuid() == 0 or gid == os.getegid() or gid in os.getgroups():
        return gid
    return None


def _default_trusted_uids(directory: Path) -> Set[int]:
    uids = {0, os.geteuid()}
    db = _db_of(directory)
    if db is not None:
        try:
            uids.add(os.stat(db).st_uid)
        except OSError:
            pass
    return uids


def _open_dir(directory: Path) -> int:
    """An `O_DIRECTORY` fd for `directory`, never through a symlink and
    without blocking, and only if it is on the same device as the DB's
    directory (a mount at `<db>.status` is refused: it could stall the
    supervisor or hide the real directory). `FileNotFoundError` propagates;
    anything else wrong is a `StatusError`."""
    try:
        fd = os.open(
            str(directory),
            os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_DIRECTORY | _O_NONBLOCK,
        )
    except FileNotFoundError:
        raise
    except OSError as exc:
        why = (
            'is a symlink or not a directory'
            if exc.errno in (errno.ELOOP, errno.ENOTDIR) else exc.strerror
        )
        raise StatusError(f'status directory {directory}: {why}') from None
    try:
        st = os.fstat(fd)
        if not stat_module.S_ISDIR(st.st_mode):
            raise StatusError(f'status directory {directory} is not a directory')
        try:
            parent_dev = os.stat(str(directory.parent)).st_dev
        except OSError:
            parent_dev = st.st_dev
        if st.st_dev != parent_dev:
            raise StatusError(
                f'status directory {directory} is a mount point (a different device '
                f'from {directory.parent}); refusing to use it'
            )
    except BaseException:
        os.close(fd)
        raise
    return fd


def _fsync_fd(fd: int) -> None:
    try:
        os.fsync(fd)
    except OSError as exc:  # some filesystems refuse fsync on a directory
        if exc.errno not in (errno.EINVAL, errno.ENOTSUP, errno.EBADF):
            raise


def ensure_status_dir(directory: Union[str, 'os.PathLike[str]']) -> None:
    """Create `directory` (`0750`, the DB file's group) if it is missing and
    check an existing one: it must be a real directory (no symlink, no mount)
    owned by this process's euid or root and not writable by group or others.
    The parent must exist (the DB's directory). The supervisor calls this
    before every worker spawn (cheap; startup is the first), never under
    `--dry-run`. Raises `StatusError`."""
    path = Path(directory)
    created = False
    try:
        os.mkdir(str(path), STATUS_DIR_MODE)
        created = True
    except FileExistsError:
        pass
    except FileNotFoundError:
        raise StatusError(
            f'cannot create status directory {path}: {path.parent} does not exist'
        ) from None
    except PermissionError:
        raise StatusError(
            f'cannot create status directory {path}: permission denied on {path.parent} '
            '(run as root or as the config DB\'s owner)'
        ) from None
    except OSError as exc:
        raise StatusError(f'cannot create status directory {path}: {exc.strerror}') from None
    try:
        fd = _open_dir(path)
    except FileNotFoundError:
        raise StatusError(f'status directory {path} vanished while it was being created') from None
    try:
        st = os.fstat(fd)
        if st.st_uid not in (0, os.geteuid()):
            raise StatusError(
                f'status directory {path} is owned by uid {st.st_uid}, not by this '
                f'process (uid {os.geteuid()}) or root; refusing to use it'
            )
        if created:
            gid = _db_group(path)
            if gid is not None and st.st_gid != gid:
                try:
                    os.fchown(fd, -1, gid)
                except OSError as exc:
                    logger.warning('could not give %s group id %d: %s', path, gid, exc.strerror)
            os.fchmod(fd, STATUS_DIR_MODE)  # mkdir's mode is filtered by the umask
            try:
                parent = os.open(str(path.parent), os.O_RDONLY | _O_CLOEXEC | _O_DIRECTORY)
            except OSError:
                parent = None  # best effort: the entry is as durable as the OS makes it
            if parent is not None:
                try:
                    _fsync_fd(parent)
                except OSError:
                    pass
                finally:
                    os.close(parent)
            logger.info('created status directory %s', path)
        elif stat_module.S_IMODE(st.st_mode) & 0o022:
            raise StatusError(
                f'status directory {path} is writable by group or others (mode '
                f'{stat_module.S_IMODE(st.st_mode):04o}); refusing to use it. '
                f'Run: chmod {STATUS_DIR_MODE:o} {path}'
            )
    finally:
        os.close(fd)


def _read_member(dfd: int, name: str, directory: Path, trusted: Collection[int]) -> Optional[bytes]:
    """The bytes of regular file `name` in the directory open as `dfd`:
    `None` if it does not exist; `StatusError` for a symlink, a special file, a
    file others can write, one owned by an untrusted user, or one that is too big."""
    path = directory / name
    try:
        fd = os.open(name, os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_NONBLOCK, dir_fd=dfd)
    except FileNotFoundError:
        return None
    except OSError as exc:
        why = 'is a symlink' if exc.errno == errno.ELOOP else exc.strerror
        raise StatusError(f'{path}: cannot open: {why}') from None
    try:
        st = os.fstat(fd)
        if not stat_module.S_ISREG(st.st_mode):
            raise StatusError(f'{path} is not a regular file')
        if st.st_uid not in trusted:
            raise StatusError(f'{path} is owned by uid {st.st_uid}, which is not trusted')
        if stat_module.S_IMODE(st.st_mode) & 0o022:
            raise StatusError(f'{path} is writable by group or others')
        if st.st_size > STATUS_MAX_BYTES:
            raise StatusError(f'{path} is {st.st_size} bytes; that is not a status file')
        chunks = []
        remaining = STATUS_MAX_BYTES + 1
        while remaining > 0:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b''.join(chunks)
        if len(data) > STATUS_MAX_BYTES:
            raise StatusError(f'{path} is larger than {STATUS_MAX_BYTES} bytes')
        return data
    except OSError as exc:
        raise StatusError(f'{path}: cannot read: {exc.strerror}') from None
    finally:
        os.close(fd)


def _check_reader_dir(dfd: int, directory: Path, trusted: Collection[int]) -> None:
    st = os.fstat(dfd)
    if st.st_uid not in trusted:
        raise StatusError(f'status directory {directory} is owned by uid {st.st_uid}, which is not trusted')
    if stat_module.S_IMODE(st.st_mode) & 0o022:
        raise StatusError(
            f'status directory {directory} is writable by group or others (mode '
            f'{stat_module.S_IMODE(st.st_mode):04o})'
        )


def _parse_status(data: bytes, worker: str, path: Path) -> WorkerStatus:
    """Parse one status document; raises only `StatusError`, whatever the
    bytes hold (deeply nested JSON is a `RecursionError`, for one)."""
    try:
        return _parse_status_doc(data, worker, path)
    except StatusError:
        raise
    except Exception as exc:
        raise StatusError(f'{path} cannot be parsed ({type(exc).__name__})') from None


def _parse_status_doc(data: bytes, worker: str, path: Path) -> WorkerStatus:
    try:
        doc = json.loads(data.decode('utf-8'))
    except Exception as exc:
        raise StatusError(f'{path} is not valid JSON ({type(exc).__name__}: {exc})') from None
    if not isinstance(doc, dict):
        raise StatusError(f'{path} is not a status document')
    schema = doc.get('schema')
    if isinstance(schema, bool) or not isinstance(schema, int) or schema < 1:
        raise StatusError(f'{path} has no usable "schema"')
    if schema > STATUS_SCHEMA:
        raise StatusError(
            f'{path} is schema {schema}; this version reads schema {STATUS_SCHEMA} '
            '(upgrade zfsbackup-config)'
        )
    if doc.get('worker') != worker:
        raise StatusError(f'{path} says it belongs to worker {doc.get("worker")!r}')
    raw_records = doc.get('records')
    if not isinstance(raw_records, list):
        raw_records = []
    records = []
    for raw in raw_records[:STATUS_MAX_RECORDS]:
        rec = _record_from_json(raw)
        if rec is not None:
            records.append(rec)
    pid = doc.get('pid')
    return WorkerStatus(
        worker=worker, path=str(path),
        pid=pid if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0 else None,
        started_at=_parse_iso(doc.get('started_at')),
        updated_at=_parse_iso(doc.get('updated_at')),
        records=tuple(records),
    )


def read_worker_status(
    directory: Union[str, 'os.PathLike[str]'],
    worker: str,
    *,
    trusted_uids: Optional[Collection[int]] = None,
) -> Optional[WorkerStatus]:
    """One worker's status, or `None` when the directory or the file does not
    exist ("no data"). Raises `StatusError` for a file or directory that is
    refused (see `read_status_dir`) or unreadable."""
    path = Path(directory)
    name = status_path(path, worker).name
    trusted = _default_trusted_uids(path) if trusted_uids is None else trusted_uids
    try:
        dfd = _open_dir(path)
    except FileNotFoundError:
        return None
    try:
        _check_reader_dir(dfd, path, trusted)
        data = _read_member(dfd, name, path, trusted)
    finally:
        os.close(dfd)
    if data is None:
        return None
    return _parse_status(data, worker, path / name)


def read_status_dir(
    directory: Union[str, 'os.PathLike[str]'],
    *,
    trusted_uids: Optional[Collection[int]] = None,
) -> StatusReadResult:
    """Every worker status under `directory` (`<db>.status`), tolerant:
    never raises for what it finds. A missing directory is `present=False`,
    "no data". A directory that is a symlink, writable by group or others, or
    owned by an untrusted user is refused whole (one problem line); so is any
    file that is a symlink or special file, writable by group or others,
    owned by an untrusted user, too big, not JSON, of a newer schema or
    naming another worker -- the others are still returned. Names that are not
    `<worker>.json` (temporary files, strangers) are ignored.

    `trusted_uids` are the owners accepted for the directory and its files;
    the default is root, this process's euid and the owner of the DB file the
    directory belongs to. A caller that knows the daemon's uid (for example
    from the presence lock's pid file) may add it."""
    path = Path(directory)
    trusted = _default_trusted_uids(path) if trusted_uids is None else trusted_uids
    try:
        dfd = _open_dir(path)
    except FileNotFoundError:
        return StatusReadResult(present=False)
    except StatusError as exc:
        return StatusReadResult(present=True, problems=(str(exc),))
    except OSError as exc:  # pragma: no cover - defensive
        return StatusReadResult(present=True, problems=(f'{path}: {exc.strerror}',))
    statuses: List[WorkerStatus] = []
    problems: List[str] = []
    try:
        try:
            _check_reader_dir(dfd, path, trusted)
            names = sorted(os.listdir(dfd))
        except StatusError as exc:
            return StatusReadResult(present=True, problems=(str(exc),))
        except OSError as exc:
            return StatusReadResult(present=True, problems=(f'{path}: cannot list: {exc.strerror}',))
        for name in names:
            if not name.endswith('.json') or not _WORKER_RE.match(name[:-5]):
                continue
            worker = name[:-5]
            try:
                data = _read_member(dfd, name, path, trusted)
                if data is not None:
                    statuses.append(_parse_status(data, worker, path / name))
            except StatusError as exc:
                problems.append(str(exc))
    finally:
        os.close(dfd)
    return StatusReadResult(present=True, statuses=tuple(statuses), problems=tuple(problems))


# -- the writer -----------------------------------------------------------

def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def write_status_file(
    directory: Union[str, 'os.PathLike[str]'], worker: str, text: str
) -> Tuple[int, int, int]:
    """Atomically replace `<directory>/<worker>.json` with `text`: a temporary
    file created exclusively in the same directory (never through a symlink),
    mode `0640`, in the directory's group, fsynced, renamed over the old file,
    and the directory fsynced. A reader sees the old file or the new one, never
    a partial one. -> `(inode, mtime_ns, size)` of the published file (what
    `StatusWriter` compares to notice that someone else replaced it). Raises
    `OSError` or `StatusError`; `StatusWriter` is the never-fatal wrapper."""
    path = Path(directory)
    final = status_path(path, worker).name
    data = text.encode('utf-8')
    dfd = _open_dir(path)
    tmp = None
    try:
        dir_gid = os.fstat(dfd).st_gid
        for attempt in range(4):
            candidate = f'.{final}.{os.getpid()}.{time.monotonic_ns()}.tmp'
            try:
                fd = os.open(
                    candidate,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC | _O_NOFOLLOW,
                    STATUS_FILE_MODE, dir_fd=dfd,
                )
            except FileExistsError:
                if attempt == 3:
                    raise
                continue
            tmp = candidate
            break
        try:
            os.fchmod(fd, STATUS_FILE_MODE)  # exact, whatever the umask
            if os.fstat(fd).st_gid != dir_gid:
                try:
                    os.fchown(fd, -1, dir_gid)
                except OSError:
                    pass  # not a member of the group: keep the default
            _write_all(fd, data)
            os.fsync(fd)
            done = os.fstat(fd)
        finally:
            os.close(fd)
        os.rename(tmp, final, src_dir_fd=dfd, dst_dir_fd=dfd)
        tmp = None
        _fsync_fd(dfd)
        return (done.st_ino, done.st_mtime_ns, done.st_size)
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp, dir_fd=dfd)
            except OSError:
                pass
        os.close(dfd)


def _remove_stale_temporaries(directory: Path, worker: str, now: float) -> None:
    """Best effort: delete this worker's `.<worker>.json.*.tmp` files left by a
    writer that was killed mid-write, once they are an hour old."""
    prefix = f'.{worker}.json.'
    try:
        dfd = _open_dir(directory)
    except (OSError, StatusError):
        return
    try:
        for name in os.listdir(dfd):
            if not (name.startswith(prefix) and name.endswith('.tmp')):
                continue
            try:
                st = os.stat(name, dir_fd=dfd, follow_symlinks=False)
                if now - st.st_mtime >= STATUS_STALE_TMP_SECONDS:
                    os.unlink(name, dir_fd=dfd)
            except OSError:
                pass
    except OSError:
        pass
    finally:
        os.close(dfd)


class StatusWriter:
    """One worker's outcome records and the file they live in. **Not
    thread-safe, and not shareable across processes**: build it inside the
    worker process (after any fork/spawn) and use it from one thread. Each
    worker writes only its own file.

    `directory=None` makes every method a no-op (`enabled` is False): that is
    how `--dry-run`, an unavailable directory and tests without a status
    directory behave. Nothing here raises: a failed write is one WARNING per
    failing streak, the records stay in memory, and the next `flush` tries
    again (after `min_interval`).

    `record` updates memory only; `flush` writes if anything changed, at most
    once per `min_interval` unless `force`. A worker calls `flush()` after each
    dataset and `flush(force=True)` at the end of a cycle and on the way out.
    """

    def __init__(
        self,
        directory: Optional[Union[str, 'os.PathLike[str]']],
        worker: str,
        *,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        min_interval: float = STATUS_WRITE_INTERVAL,
        max_records: int = STATUS_MAX_RECORDS,
        pid: Optional[int] = None,
    ):
        status_path('.', worker)  # validates the name
        self.directory = Path(directory) if directory is not None else None
        self.worker = worker
        self._clock = clock
        self._monotonic = monotonic
        self._min_interval = min_interval
        self._max_records = max_records
        self._pid = os.getpid() if pid is None else pid
        #: Whole seconds, as written. Strictly greater than the `started_at`
        #: of the file found by `load()` (even if the clock went back), so
        #: that "newer incarnation" is a plain comparison.
        self._started_at = int(clock())
        #: `(inode, mtime_ns, size)` of the file this writer last published.
        self._sig: Optional[Tuple[int, int, int]] = None
        #: Set once a file with a newer `started_at` was found: a newer
        #: incarnation of this worker owns the file, and this one writes no more.
        self._superseded = False
        self._records: Dict[Tuple[str, str, Optional[str]], OutcomeRecord] = {}
        self._dirty = False
        self._last_flush: Optional[float] = None
        self._failing = False

    @property
    def enabled(self) -> bool:
        return self.directory is not None

    @property
    def records(self) -> List[OutcomeRecord]:
        """A copy of the records held, sorted by key."""
        return [self._records[k] for k in sorted(self._records, key=lambda k: (k[0], k[1], k[2] or ''))]

    def load(self) -> None:
        """Carry over the counters of this worker's previous file (a reload or
        a crash restart respawns the worker; it must not forget that a send has
        been failing for a day). A missing, refused or damaged file starts
        empty -- a refused or damaged one is logged once at WARNING, and the
        first write replaces it. Also removes this worker's stale temporaries."""
        if not self.enabled:
            return
        try:
            previous = read_worker_status(self.directory, self.worker)
        except StatusError as exc:
            logger.warning('ignoring the previous status file: %s', exc)
            previous = None
        except Exception as exc:  # never fatal
            logger.warning('cannot read the previous status file: %s: %s', type(exc).__name__, exc)
            previous = None
        if previous is not None:
            for rec in previous.records:
                self._records[rec.key] = rec
            self._trim()
            if previous.started_at is not None and previous.started_at >= self._started_at:
                self._started_at = int(previous.started_at) + 1
        _remove_stale_temporaries(self.directory, self.worker, self._clock())

    def record(
        self,
        kind: str,
        dataset: str,
        destination: Optional[str] = None,
        *,
        ok: bool,
        error: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        """One attempt finished. Success: `last_success` moves, the failure
        count goes to 0. Failure: the count goes up and `error` (clipped)
        becomes `last_error`. Either way `last_attempt` moves."""
        if not self.enabled:
            return
        now = self._clock()
        rec = self._records.get((kind, dataset, destination))
        if rec is None:
            rec = self._records[(kind, dataset, destination)] = OutcomeRecord(
                kind=kind, dataset=dataset, destination=destination,
            )
        rec.last_attempt = now
        rec.detail = clip_text(detail) if detail else None
        if ok:
            rec.result = RESULT_OK
            rec.last_success = now
            rec.consecutive_failures = 0
        else:
            rec.result = RESULT_ERROR
            rec.last_error = clip_text(error) if error else 'failed (see the journal)'
            rec.last_error_at = now
            rec.consecutive_failures = min(rec.consecutive_failures + 1, 10**9)
        self._dirty = True
        self._trim()

    def retain(self, keep: Iterable[Tuple[str, Optional[str]]]) -> None:
        """Drop the records whose `(dataset, destination)` is not in `keep`
        (a dataset or destination removed from the configuration must not
        keep a failure on the books)."""
        if not self.enabled:
            return
        valid = set(keep)
        stale = [k for k in self._records if (k[1], k[2]) not in valid]
        for key in stale:
            del self._records[key]
        if stale:
            self._dirty = True

    def _trim(self) -> None:
        excess = len(self._records) - self._max_records
        if excess > 0:
            oldest = sorted(self._records.values(), key=lambda r: r.last_attempt or 0.0)[:excess]
            for rec in oldest:
                del self._records[rec.key]
            self._dirty = True

    def document(self) -> str:
        """The file's text (also used by tests)."""
        doc = {
            'schema': STATUS_SCHEMA,
            'worker': self.worker,
            'pid': self._pid,
            'started_at': _iso(self._started_at),
            'updated_at': _iso(self._clock()),
            'records': [_record_to_json(r) for r in self.records],
        }
        return json.dumps(doc, separators=(',', ':'), ensure_ascii=True) + '\n'

    def _newer_incarnation(self) -> Optional[float]:
        """The `started_at` of the file on disk if it is newer than this
        writer's own (a later incarnation of this worker has written it),
        else `None`. Costs one `stat` while the file is the one this writer
        published; a read otherwise. A file that cannot be read is not newer."""
        path = status_path(self.directory, self.worker)
        try:
            st = os.stat(path, follow_symlinks=False)
            if self._sig == (st.st_ino, st.st_mtime_ns, st.st_size):
                return None
            other = read_worker_status(self.directory, self.worker)
        except Exception:
            return None
        if other is not None and other.started_at is not None and other.started_at > self._started_at:
            return other.started_at
        return None

    def flush(self, force: bool = False, heartbeat: bool = False) -> bool:
        """Write the file if anything changed (and, unless `force`, if
        `min_interval` has passed since the last attempt). `heartbeat` writes
        it even when nothing changed, to refresh `updated_at` (the worker does
        this once per cycle, so a reader can tell a quiet worker from a dead
        one; pair it with `force`). -> True if a write succeeded. Never
        raises. Refuses to replace a file written by a
        later incarnation of this worker (one WARNING; this writer then
        writes nothing more)."""
        if not self.enabled or self._superseded or not (self._dirty or heartbeat):
            return False
        now = self._monotonic()
        if not force and self._last_flush is not None and now - self._last_flush < self._min_interval:
            return False
        self._last_flush = now
        try:
            text = self.document()
            while len(text) > STATUS_MAX_BYTES and self._records:
                self._max_records = max(len(self._records) // 2, 1)
                self._trim()
                text = self.document()
            newer = self._newer_incarnation()
            if newer is not None:
                self._superseded = True
                self._dirty = False
                self._records.clear()
                logger.warning(
                    'the status file %s was written by a newer %s (started %s); this one '
                    'will not overwrite it',
                    status_path(self.directory, self.worker), self.worker, _iso(newer),
                )
                return False
            self._sig = write_status_file(self.directory, self.worker, text)
        except Exception as exc:
            if not self._failing:
                self._failing = True
                logger.warning(
                    'cannot write the status file %s (%s: %s); outcomes are kept in '
                    'memory and written when this works again',
                    status_path(self.directory, self.worker), type(exc).__name__, exc,
                )
            return False
        if self._failing:
            self._failing = False
            logger.info('the status file %s can be written again', status_path(self.directory, self.worker))
        self._dirty = False
        return True

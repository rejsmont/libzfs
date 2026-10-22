"""Backup health: how far behind is each snapshot, send and received copy?

**Stdlib only, no `BackupConfig`, no ZFS.** `zfsbackup-config check` imports this
module (and, lazily, `zfsbackup.health_collect`, which does the ZFS reading), and
the worker status files (S-7) will extend it. Everything here works on plain
values: seconds, epoch timestamps and names.

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

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple, Union

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

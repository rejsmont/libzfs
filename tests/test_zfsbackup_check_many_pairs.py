"""`zfsbackup-config check` and `status` over the status file of a remote worker with MANY pairs (plan Track B,
item B4, `docs/sender_rework_plan.md`).

Before Track B the remote worker walked its datasets one at a time; with the parallel sender threads
(`zfsbackup/sendloop.py`) a status file is written by up to four senders and a scheduler at once, and a
recursive tree makes it long: 3 recursive parents x 40 expanded children + 2 configured datasets = 125
datasets, x 2 destinations = 250 `send` records. These tests pin that `check` and `status` read such a file
correctly and in bounded time.

The status files are NOT built by hand: `LoopRig` (`tests/_sendloop_support.py`) runs the REAL
`RemoteBackupWorker.run()` -- its scheduler, its discovery threads, its sender threads, its `StatusWriter` --
over fake transports and the `InheritingZfs` fake pool (`tests/test_zfsbackup_recursive_sends.py`), five
incarnations in a row (what a daemon does across reloads, so the failure counters really are consecutive
counters). The files are then copied into the `<db>.status/` directory of a real database (made by `import`)
that `check` and `status` read; the ZFS side of `check` is the read-only fake `zfs` of
`tests/test_zfsbackup_cli_check.py` (a recursive tree, every dataset freshly snapshotted and anchored, so
nothing but the worker records can be graded WARN or CRIT).

Pinned here:

* the build itself (concurrent senders really overlapped; a partial file was on disk, whole, while a send was
  still running; no read of the file ever failed or tore; one record per pair);
* `check`: the grading of every pair by its consecutive failures (text, `--json`, `--prometheus`, `--nagios`
  with its byte cap, `--nagios-max-bytes`), the counts of the summary line, one `worker_send` entry per pair,
  no extra `zfs` exec for the records (the ZFS work stays O(parents)), bounded wall time;
* `status`: unchanged verdict and exit code, the `workers:` row (250 records, 5 failing, the worst one);
* the status cap (`health.STATUS_MAX_RECORDS`): over it the worker warns ONCE and its file holds exactly the cap,
  which `check` and `status` read as a complete file;
* a heartbeat-only file: an incarnation with nothing due over a populated file leaves every record as it was;
  an incarnation with nothing due and no records writes a file with no records, which adds nothing.

Mutation-checked (scratch copy of the tree, one change at a time; failing tests in this file): `check` warn
grading `>=` -> `>` 4; the over-cap WARNING given on every pass 1; a success recorded without its destination
(the records collapse) 20; the writer's trim by the cap removed 3; `check` reading only the first 100 records 9;
`status` counting failing from 2 failures 1; no flush after a recorded outcome (a held send then never sees a
partial file; the fixture's own bound fires) 3.
"""

import itertools
import json
import logging
import re
import threading
import time
import zlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from unittest import mock

import pytest

from _cli_support import (  # noqa: F401  (fixtures are found by name)
    db,
    isolated,
    live_daemon,
    make_db,
    run,
    store_key,
)
from _sendloop_support import LoopRig, age_status, bound, fake_sender, wait_for
from _worker_status_support import make_dir, status_dir
from test_zfsbackup_cli_check import (  # noqa: F401  (`zfs` and `world` are fixtures)
    ANCHOR_PROP,
    SEND,
    World,
    install_recursive_fake,
    nbytes,
    split_nagios,
    tls_of,
    world,
    zfs,
)
from test_zfsbackup_health import Exposition
from test_zfsbackup_recursive_sends import InheritingZfs, stamp
from datetime import timedelta
from zfsbackup import health, identity
from zfsbackup.backup_manager import PROP_ANCHOR_PREFIX
from zfsbackup.cli import check as check_view
from zfsbackup.cli import status as status_view
from zfsbackup.config import BackupConfig
from zfsbackup.sendpair import PAIR_NOT_DUE, PairOutcome

pytestmark = pytest.mark.integration

PARENTS = ('tank/p0', 'tank/p1', 'tank/p2')
KIDS = 40
DESTS = ('offsite', 'second')
PLAIN = ('tank/plain0', 'tank/plain1')
CHILDREN = tuple(f'{p}/c{i:02d}' for p in PARENTS for i in range(KIDS))
ALL = (*PARENTS, *CHILDREN, *PLAIN)
PAIRS = tuple((d, dest) for d in ALL for dest in DESTS)
NPAIRS = len(PAIRS)
assert NPAIRS == 250

CYCLES = 5
#: `(dataset, destination)` -> the cycles (1-based) in which the pair FAILS. Five incarnations, so a pair that
#: fails in cycles k..5 ends with 6 - k consecutive failures; one that fails earlier and then succeeds, with 0.
CRIT_A = ('tank/p0/c07', 'offsite')       # fails every cycle: 5 in a row
CRIT_B = ('tank/plain0', 'second')        # RAISES in cycles 2..5: 4 in a row
WARN_A = ('tank/p1/c13', 'second')        # ok, ok, ok, fails, fails: 2 in a row
WARN_B = ('tank/p2/c39', 'offsite')       # ok, ok, fails x3: 3 in a row
ONE = ('tank/p0/c20', 'second')           # fails only in cycle 5: 1 in a row (below the warn factor: OK)
RECOVERED = ('tank/p2/c00', 'second')     # fails in cycles 1..4, ok in 5: 0
FAILS = {
    CRIT_A: {1, 2, 3, 4, 5}, WARN_A: {4, 5}, WARN_B: {3, 4, 5}, ONE: {5}, RECOVERED: {1, 2, 3, 4},
}
RAISES = {CRIT_B: {2, 3, 4, 5}}
EXPECTED_FAILURES = {CRIT_A: 5, CRIT_B: 4, WARN_A: 2, WARN_B: 3, ONE: 1, RECOVERED: 0}
EXPECTED_STATE = {CRIT_A: 'CRIT', CRIT_B: 'CRIT', WARN_A: 'WARN', WARN_B: 'WARN', ONE: 'OK', RECOVERED: 'OK'}
FAILING = {k for k, v in EXPECTED_FAILURES.items() if v >= 1}       # what `status` counts as failing
SLOW = ('tank/plain1', 'second')          # held, in cycle 1, until the file shows 30 other outcomes
HOLD_UNTIL = 30
CAP = 100


def many_yaml(plain=PLAIN, parents=PARENTS) -> str:
    def remote():
        return ''.join(f'      - destination: "{d}"\n' for d in DESTS)

    text = 'snapshot_prefix: "autosnap"\ncheck_interval: "5m"\ndatasets:\n'
    for name in parents:
        text += (f'  - name: "{name}"\n    frequency: "1h"\n    recursive: true\n    retention:\n'
                 f'      "1d": "30d"\n    remote:\n{remote()}')
    for name in plain:
        text += (f'  - name: "{name}"\n    frequency: "1h"\n    retention:\n      "1d": "30d"\n'
                 f'    remote:\n{remote()}')
    return text + 'destinations:\n' + ''.join(f'  {d}:\n    url: "https://{d}:8080"\n' for d in DESTS)


# ---------------------------------------------------------------------------------------------
# Building the status files with the REAL remote worker loop
# ---------------------------------------------------------------------------------------------

class _Patcher:
    """What `InheritingZfs(mocker)` needs of `mocker`, for a module-scoped fixture (no `mocker` there)."""

    def __init__(self):
        self.started = []

    def patch(self, target, **kw):
        patcher = mock.patch(target, **kw)
        patcher.start()
        self.started.append(patcher)

    def stop(self):
        for patcher in reversed(self.started):
            patcher.stop()


class _Poller(threading.Thread):
    """Reads the worker's status file as a `check` would, over and over, while the worker runs."""

    def __init__(self, directory):
        super().__init__(daemon=True, name='status-poller')
        self.directory = directory
        self.halt = threading.Event()
        self.counts: List[int] = []         # the record count of every successful read
        self.failures: List[str] = []       # every read that raised, or found no file after one existed
        self.latest = 0

    def run(self):
        seen = False
        while not self.halt.is_set():
            try:
                status = health.read_worker_status(self.directory, SEND)
            except Exception as exc:        # noqa: BLE001 - a reader must never fail
                self.failures.append(repr(exc))
                status = None
            else:
                if status is None and seen:
                    self.failures.append('the file vanished')
            if status is not None:
                seen = True
                self.latest = len(status.records)
                self.counts.append(self.latest)
            time.sleep(0.001)


@dataclass
class Built:
    """What the real loop left: the files of a status directory and what was observed while it ran."""

    files: Dict[str, str] = field(default_factory=dict)
    first_cycle_counts: List[int] = field(default_factory=list)
    poll_failures: List[str] = field(default_factory=list)
    max_inflight: int = 0
    held_timed_out: bool = False
    held_saw: int = 0
    warnings: List[str] = field(default_factory=list)
    attempts: List[Tuple[str, str]] = field(default_factory=list)
    heartbeat_files: Dict[str, str] = field(default_factory=dict)
    heartbeat_attempts: Optional[List[Tuple[str, str]]] = None

    def records(self, which=None) -> Dict[Tuple[str, str, Optional[str]], dict]:
        doc = json.loads((which or self.files)[SEND + '.json'])
        return {(r['kind'], r['dataset'], r.get('destination')): r for r in doc['records']}


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.messages: List[str] = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def _fresh_world(patcher, *, anchored=False, plain=PLAIN, parents=PARENTS, kids=KIDS) -> InheritingZfs:
    world = InheritingZfs(patcher)
    snap = stamp(timedelta(minutes=10))
    names = [*parents, *(f'{p}/c{i:02d}' for p in parents for i in range(kids)), *plain]
    for name in names:
        world.add(name, snaps=[snap])
        if anchored:
            for dest in DESTS:
                world.set_prop(name, f'{PROP_ANCHOR_PREFIX}{dest}', snap)
    return world


def _incarnation(config, directory, mp, *, cycle, poller=None, hold=False, tracker=None, anchored_world=None):
    """One incarnation of the real remote worker over the fake pool; -> (rig, sender)."""
    sender = fake_sender()
    sender.last_problem.side_effect = lambda ds, dest: f'{dest} refused {ds}' if (ds, dest) in FAILS else None
    inner = sender.backup_dataset.side_effect
    lock = threading.Lock()
    state = tracker if tracker is not None else {}
    state.setdefault('inflight', 0)
    state.setdefault('max', 0)
    state.setdefault('starts', itertools.count())
    barrier = threading.Barrier(2, timeout=bound(20.0)) if hold else None

    def backup(dsi, remote_cfg, should_stop=None):
        key = (dsi.name, remote_cfg.destination)
        with lock:
            state['inflight'] += 1
            state['max'] = max(state['max'], state['inflight'])
            number = next(state['starts'])
        try:
            if barrier is not None and number < 2:
                # two senders really are inside `backup_dataset` at the same time
                try:
                    barrier.wait()
                except threading.BrokenBarrierError:
                    state['overlap_failed'] = True
            if hold and key == SLOW and poller is not None:
                state['held_saw'] = 0
                if not wait_for(lambda: poller.latest >= HOLD_UNTIL, timeout=bound(10.0)):
                    state['held_timed_out'] = True
                state['held_saw'] = poller.latest
            time.sleep(0.0005 * (zlib.crc32(repr(key).encode()) % 5))       # a jitter: the outcomes interleave
            if cycle in RAISES.get(key, ()):
                raise RuntimeError('boom')
            if cycle in FAILS.get(key, ()):
                sender.outcomes[key] = False
            else:
                sender.outcomes.pop(key, None)
            return inner(dsi, remote_cfg, should_stop)
        finally:
            with lock:
                state['inflight'] -= 1

    sender.backup_dataset.side_effect = backup
    rig = LoopRig(config, mp, remote=sender, local=sender, world=True, status_dir=directory)
    return rig, sender


def build(tmp_path_factory, *, capped=False) -> Built:
    out = Built()
    root = tmp_path_factory.mktemp('many-pairs-build')
    (root / 'in.yaml').write_text(many_yaml())
    config = BackupConfig.from_file(root / 'in.yaml')
    directory = root / 'config.db.status'
    health.ensure_status_dir(directory)
    capture = _Capture()
    logging.getLogger().addHandler(capture)
    patcher = _Patcher()
    poller = _Poller(directory)
    try:
        _fresh_world(patcher)
        cycles = 1 if capped else CYCLES
        poller.start()
        tracker: dict = {}
        rig = None
        for cycle in range(1, cycles + 1):
            with pytest.MonkeyPatch.context() as mp:
                if capped:
                    mp.setattr(health, 'STATUS_MAX_RECORDS', CAP)

                    class CappedWriter(health.StatusWriter):
                        def __init__(self, *a, **kw):
                            kw.setdefault('max_records', CAP)
                            super().__init__(*a, **kw)

                    mp.setattr(health, 'StatusWriter', CappedWriter)
                before = len(poller.counts)
                rig, sender = _incarnation(
                    config, directory, mp, cycle=cycle, poller=poller, hold=(cycle == 1 and not capped),
                    tracker=tracker)
                rig.run()
                if cycle == 1:
                    out.first_cycle_counts = poller.counts[before:]
                out.attempts.extend((c.args[0].name, c.args[1].destination)
                                    for c in sender.backup_dataset.call_args_list)
                if cycle < cycles:
                    age_status(rig.worker)
        out.files = {p.name: p.read_text() for p in directory.iterdir() if p.suffix == '.json'}
        if not capped:
            # a heartbeat-only incarnation: everything was attempted a moment ago, nothing is due
            with pytest.MonkeyPatch.context() as mp:
                rig, sender = _incarnation(config, directory, mp, cycle=CYCLES + 1, tracker={})
                rig.run()
                out.heartbeat_attempts = [(c.args[0].name, c.args[1].destination)
                                          for c in sender.backup_dataset.call_args_list]
            out.heartbeat_files = {p.name: p.read_text() for p in directory.iterdir() if p.suffix == '.json'}
        out.max_inflight = tracker.get('max', 0)
        out.held_timed_out = bool(tracker.get('held_timed_out')) or bool(tracker.get('overlap_failed'))
        out.held_saw = tracker.get('held_saw', 0)
    finally:
        poller.halt.set()
        poller.join(timeout=bound(5.0))
        patcher.stop()
        logging.getLogger().removeHandler(capture)
    out.poll_failures = poller.failures
    out.warnings = capture.messages
    return out


@pytest.fixture(scope='module')
def mixed(tmp_path_factory) -> Built:
    """Five incarnations of the real worker, some pairs failing as `FAILS`/`RAISES` say, then a heartbeat-only one."""
    return build(tmp_path_factory)


@pytest.fixture(scope='module')
def capped(tmp_path_factory) -> Built:
    """ONE incarnation with `STATUS_MAX_RECORDS` (and the writer's cap) at `CAP`: 250 pairs, a file of 100."""
    return build(tmp_path_factory, capped=True)


# ---------------------------------------------------------------------------------------------
# The database, the fake zfs and the status directory `check` reads
# ---------------------------------------------------------------------------------------------

def install(db, files: Dict[str, str]) -> None:
    directory = make_dir(db)
    for name, text in files.items():
        health.write_status_file(directory, name[:-len('.json')], text)


def prepare(world: World, tmp_path, yaml_text=None, *, datasets=ALL) -> World:
    """A healthy host over `datasets` (every dataset snapshotted 10 minutes ago and anchored on that snapshot
    for both destinations, a live daemon on the stored generation, a client certificate)."""
    zfs = world.zfs
    install_recursive_fake(zfs, tmp_path)
    newest, older = zfs.name(600), zfs.name(4200)
    zfs.spec.update(
        datasets={n: 'filesystem' for n in datasets}, markers={}, fail_names=[], fail_markers=[], fail_tree=[],
        fail_props1=[], fail_rows=[], fail_get=[], slow=[],
    )
    zfs.spec['snapshots'] = {n: [[newest, 102], [older, 101]] for n in datasets}
    zfs.spec['props'] = {n: {f'{PROP_ANCHOR_PREFIX}{d}': newest for d in DESTS} for n in datasets}
    zfs.save()
    make_db(world.run, world.db, yaml_text or many_yaml())
    identity.ensure_identity(tls_of(world.db), 'client')
    world.daemon = world._daemon(world.db, applied=store_key(world.db))
    return world


@pytest.fixture
def host(world, tmp_path) -> World:
    return prepare(world, tmp_path)


def worker_entries(world, *flags) -> Dict[Tuple[str, str], dict]:
    """`(subject, destination)` -> the JSON entry of every `worker_send` entry."""
    data = json.loads(world.check('--json', *flags).stdout)
    return {(e['subject'], e['peer']): e for e in data['entries'] if e['source'] == 'worker_send'}


def timed(fn, *args, **kw):
    started = time.monotonic()
    result = fn(*args, **kw)
    return result, time.monotonic() - started


# ---------------------------------------------------------------------------------------------
# 1. The status file the real loop wrote
# ---------------------------------------------------------------------------------------------

class TestTheStatusFileTheRealLoopWrote:
    """The inputs are what the tests below claim they are: concurrent senders, interleaved flushes, one record
    per pair, the consecutive counters the script asked for."""

    def test_one_send_record_per_pair_and_nothing_else(self, mixed):
        records = mixed.records()
        assert len(records) == NPAIRS
        assert set(records) == {('send', d, dest) for d, dest in PAIRS}
        assert all(r['kind'] == 'send' for r in records.values())

    def test_the_failure_counters_are_the_scripts(self, mixed):
        records = mixed.records()
        for pair, failures in EXPECTED_FAILURES.items():
            rec = records[('send', *pair)]
            assert rec['consecutive_failures'] == failures, (pair, rec)
            assert (rec.get('result') == 'error') == bool(failures)
        for key, rec in records.items():
            if key[1:] not in EXPECTED_FAILURES:
                assert rec['consecutive_failures'] == 0 and rec['result'] == 'ok', key

    def test_a_raising_sender_is_charged_to_its_pair_only(self, mixed):
        rec = mixed.records()[('send', *CRIT_B)]
        assert rec['last_error'] == 'RuntimeError: boom'

    def test_a_failed_pair_carries_the_senders_account(self, mixed):
        assert mixed.records()[('send', *CRIT_A)]['last_error'] == f'{CRIT_A[1]} refused {CRIT_A[0]}'

    def test_senders_really_ran_concurrently(self, mixed):
        assert mixed.max_inflight >= 2
        assert not mixed.held_timed_out, 'the two overlapping senders never met'

    def test_a_partial_file_was_on_disk_while_a_send_was_still_running(self, mixed):
        """The held send (`SLOW`) waited, in cycle 1, until the file on disk showed 30 outcomes: the
        outcomes of the other senders are flushed at once, not at the end of the cycle."""
        assert not mixed.held_timed_out
        assert mixed.held_saw >= HOLD_UNTIL
        assert any(0 < n < NPAIRS for n in mixed.first_cycle_counts)

    def test_the_file_grew_whole_and_was_never_unreadable(self, mixed):
        assert mixed.poll_failures == []
        counts = mixed.first_cycle_counts
        assert counts == sorted(counts), 'within one incarnation the file only grows'
        assert counts[-1] == NPAIRS

    def test_every_pair_was_attempted_in_every_due_cycle(self, mixed):
        assert len(mixed.attempts) == CYCLES * NPAIRS
        assert len(set(mixed.attempts)) == NPAIRS

    def test_the_file_is_what_the_cli_reads_without_a_complaint(self, mixed, tmp_path):
        (tmp_path / 'c.db').write_bytes(b'')
        install(tmp_path / 'c.db', mixed.files)
        result = health.read_status_dir(status_dir(tmp_path / 'c.db'))
        assert result.problems == () and [s.worker for s in result.statuses] == [SEND]
        assert len(result.statuses[0].records) == NPAIRS


# ---------------------------------------------------------------------------------------------
# 2. check
# ---------------------------------------------------------------------------------------------

class TestCheckGradesManyPairs:
    @pytest.fixture
    def graded(self, host, mixed):
        install(host.db, mixed.files)
        return host

    def test_json_has_one_worker_entry_per_pair_graded_by_its_failures(self, graded):
        result = graded.check('--json')
        assert result.exit_code == 1, (result.stdout[-300:], result.stderr)
        entries = worker_entries(graded)
        assert set(entries) == set(PAIRS)
        for pair, entry in entries.items():
            failures = EXPECTED_FAILURES.get(pair, 0)
            assert entry['consecutive_failures'] == failures, pair
            assert entry['state'] == EXPECTED_STATE.get(pair, 'OK'), (pair, entry)
            assert entry['kind'] == 'send'
        data = json.loads(result.stdout)
        assert data['state'] == 'CRIT'

    def test_nothing_but_the_workers_records_is_graded_down(self, graded):
        """The ZFS side is healthy: the two CRIT and two WARN entries are the four pairs."""
        data = json.loads(graded.check('--json').stdout)
        bad = {(e['source'], e['subject'], e['peer']) for e in data['entries'] if e['state'] != 'OK'}
        assert bad == {('worker_send', *p) for p in (CRIT_A, CRIT_B, WARN_A, WARN_B)}

    def test_the_reasons_name_the_failures_in_a_row(self, graded):
        entries = worker_entries(graded)
        assert '5 attempts in a row failed' in entries[CRIT_A]['reason']
        assert '3 attempts in a row failed' in entries[WARN_B]['reason']
        assert 'RuntimeError: boom' in entries[CRIT_B]['reason']

    def test_children_of_every_parent_are_graded_per_destination(self, graded):
        entries = worker_entries(graded)
        for parent in PARENTS:
            for dest in DESTS:
                assert (parent, dest) in entries
            for i in range(KIDS):
                for dest in DESTS:
                    assert (f'{parent}/c{i:02d}', dest) in entries

    def test_the_text_form_lists_the_failing_pairs_and_counts_every_entry(self, graded):
        result = graded.check()
        assert result.exit_code == 1
        lines = result.stdout.splitlines()
        bad = [l for l in lines if l.startswith(('WARN', 'CRIT'))]
        parsed = [re.match(r'(\S+)\s+(worker_send: \S+ \(\S+\)): ', l).groups() for l in bad]
        assert sorted(parsed) == sorted(
            [('CRIT', f'worker_send: {p[0]} ({p[1]})') for p in (CRIT_A, CRIT_B)]
            + [('WARN', f'worker_send: {p[0]} ({p[1]})') for p in (WARN_A, WARN_B)])
        entries = json.loads(graded.check('--json').stdout)['entries']
        counts = {s: sum(e['state'] == s for e in entries) for s in ('OK', 'WARN', 'CRIT')}
        assert counts['WARN'] == 2 and counts['CRIT'] == 2
        assert lines[-1] == (
            f'overall: CRIT ({len(entries)} checks: {counts["OK"]} ok, 2 warn, 2 crit)')
        assert sum(l.startswith('OK') and 'worker_send:' in l for l in lines) == NPAIRS - 4

    def test_prometheus_has_one_failure_sample_per_pair(self, graded):
        result = graded.check('--prometheus')
        assert result.exit_code == 1
        parsed = Exposition(result.stdout)
        samples = [s for s in parsed.samples if s[0] == 'zfsbackup_worker_consecutive_failures']
        assert len(samples) == NPAIRS
        by_pair = {(s[1]['dataset'], s[1]['destination']): s[2] for s in samples}
        assert set(by_pair) == set(PAIRS)
        for pair, value in by_pair.items():
            assert value == EXPECTED_FAILURES.get(pair, 0), pair
        for pair, state in ((CRIT_A, 2), (WARN_B, 1), (RECOVERED, 0)):
            assert parsed.one('zfsbackup_check_state', source='worker_send', subject=pair[0], peer=pair[1]) == state
        assert parsed.one('zfsbackup_check_overall_state') == 2

    def test_nagios_is_capped_and_keeps_the_failing_pairs(self, graded):
        result = graded.check('--nagios')
        assert result.exit_code == 2
        out = result.stdout
        assert nbytes(out) <= check_view.NAGIOS_OUTPUT_LIMIT == 4000
        status, items, omitted = split_nagios(out)
        assert status.startswith('ZFSBACKUP CRIT - CRIT worker_send: ')
        assert omitted is not None and omitted > 0 and out.rstrip('\n').split(' ')[-1] == f'omitted={omitted}'
        labels = [i.split("'=")[0][1:] for i in items]
        for pair in (CRIT_A, CRIT_B, WARN_A, WARN_B):
            assert f'worker_send:{pair[0]}@{pair[1]}' in labels, pair
        # the failure items carry their counts and thresholds
        by_label = dict(zip(labels, items))
        assert by_label[f'worker_send:{CRIT_A[0]}@{CRIT_A[1]}'].endswith('=5;2;4;0')
        assert by_label[f'worker_send:{WARN_B[0]}@{WARN_B[1]}'].endswith('=3;2;4;0')
        # the entries' own order is kept among the kept items
        order = [e['subject'] + '@' + (e['peer'] or '') for e in worker_entries(graded).values()]
        position = {name: n for n, name in enumerate(order)}
        kept = [position[l.split(':', 1)[1]] for l in labels if l.startswith('worker_send:')]
        assert kept == sorted(kept)

    def test_nagios_with_room_for_everything_lists_every_pair(self, graded):
        result = graded.check('--nagios', '--nagios-max-bytes', '200000')
        assert result.exit_code == 2
        status, items, omitted = split_nagios(result.stdout)
        assert omitted is None
        workers = [i for i in items if i.startswith("'worker_send:")]
        assert len(workers) == NPAIRS
        assert sum(i.endswith('=0;2;4;0') for i in workers) == NPAIRS - len(FAILING)     # RECOVERED is back to 0
        assert sum(i.endswith('=1;2;4;0') for i in workers) == 1

    def test_the_records_cost_no_zfs_exec(self, host, mixed):
        start = len(host.zfs.calls)                    # the `import` that made the database read ZFS too
        assert host.check('--json').exit_code == 0
        bare = host.zfs.calls[start:]
        install(host.db, mixed.files)
        assert host.check('--json').exit_code == 1
        full = host.zfs.calls[start + len(bare):]
        assert full == bare, 'the status files are read without any zfs exec'
        # and the ZFS work is per parent, not per pair: one recursive snapshot listing each
        batches = [c for c in full if c[0] == 'list' and '-r' in c and 'snapshot' in c]
        assert sorted(c[-1] for c in batches) == sorted(PARENTS)
        assert len(full) < NPAIRS // 2

    @pytest.mark.parametrize('flags, code', [((), 1), (('--json',), 1), (('--prometheus',), 1), (('--nagios',), 2)])
    def test_the_check_finishes_in_bounded_time(self, graded, flags, code):
        result, elapsed = timed(graded.check, *flags)
        assert result.exit_code == code
        assert elapsed < bound(30.0), f'check {flags} took {elapsed:.1f}s'
        assert '(time limit)' not in result.stdout

    def test_reading_the_file_alone_is_fast(self, graded):
        from zfsbackup.config.store import dispose_all, resolve_config_path
        resolved = resolve_config_path(str(graded.db))
        try:
            workers, elapsed = timed(status_view.collect_workers, resolved, daemon_running=True)
        finally:
            dispose_all()
        assert elapsed < bound(2.0)
        assert workers.problems == () and workers.stale == {}
        assert len(workers.statuses[0].records) == NPAIRS

    def test_output_to_a_file_has_every_entry(self, graded, tmp_path):
        target = tmp_path / 'check.prom'
        result = graded.check('--prometheus', '--output', target)
        assert result.exit_code == 0 and result.stdout == '', 'with --output the exit code says only that the file was written'
        samples = [s for s in Exposition(target.read_text()).samples
                   if s[0] == 'zfsbackup_worker_consecutive_failures']
        assert len(samples) == NPAIRS


# ---------------------------------------------------------------------------------------------
# 3. status
# ---------------------------------------------------------------------------------------------

class TestStatusWithManyPairs:
    def test_the_verdict_and_exit_code_are_unaffected(self, host, mixed):
        bare = host.run('-c', host.db, 'status', '--json')
        install(host.db, mixed.files)
        full = host.run('-c', host.db, 'status', '--json')
        assert bare.exit_code == full.exit_code == 0
        a, b = json.loads(bare.stdout), json.loads(full.stdout)
        assert 'workers' not in a
        assert {k: v for k, v in b.items() if k != 'workers'} == a
        assert host.run('-c', host.db, 'status').exit_code == 0

    def test_the_workers_row_counts_the_records_and_the_failing_ones(self, host, mixed):
        install(host.db, mixed.files)
        data = json.loads(host.run('-c', host.db, 'status', '--json').stdout)['workers']
        assert data['problems'] == []
        (file,) = data['files']
        assert file['worker'] == SEND and file['stale'] is False
        assert file['records'] == NPAIRS and file['failing'] == len(FAILING)
        worst = file['worst']
        assert (worst['dataset'], worst['destination'], worst['consecutive_failures']) == (*CRIT_A, 5)
        assert worst['last_error'] == f'{CRIT_A[1]} refused {CRIT_A[0]}'

    def test_the_text_row(self, host, mixed):
        install(host.db, mixed.files)
        text = host.run('-c', host.db, 'status').stdout
        assert re.search(rf'^workers:  {SEND}: updated \S+ \(\d+s ago\), {NPAIRS} records, {len(FAILING)} failing$',
                         text, re.M), text
        assert f'  worst: send {CRIT_A[0]} -> {CRIT_A[1]}, 5 failure(s) in a row: ' in text

    def test_status_is_quick(self, host, mixed):
        install(host.db, mixed.files)
        result, elapsed = timed(host.run, '-c', host.db, 'status')
        assert result.exit_code == 0 and elapsed < bound(10.0)


# ---------------------------------------------------------------------------------------------
# 4. the status cap
# ---------------------------------------------------------------------------------------------

class TestTheStatusCap:
    """`STATUS_MAX_RECORDS` (2048 in production, `CAP` here): a worker with more pairs than the cap warns ONCE
    that `check` cannot report the least recently attempted ones, and its file holds exactly the cap."""

    def test_the_warning_is_given_once(self, capped):
        warnings = [w for w in capped.warnings if 'dataset x destination pairs' in w]
        assert len(warnings) == 1, capped.warnings
        # said as soon as the discoveries found more pairs than the cap (not at the end), so the count is
        # whatever the schedule held then
        assert int(re.search(r'has (\d+) dataset x destination pairs', warnings[0]).group(1)) > CAP
        assert f'more than the {CAP} outcomes' in warnings[0]
        assert 'recursive datasets included' in warnings[0]

    def test_the_file_holds_exactly_the_cap(self, capped):
        records = capped.records()
        assert len(records) == CAP
        assert all(k[0] == 'send' and (k[1], k[2]) in set(PAIRS) for k in records)

    def test_readers_never_failed_while_it_trimmed(self, capped):
        assert capped.poll_failures == []
        assert max(capped.first_cycle_counts) <= CAP

    def test_check_grades_the_records_the_file_holds(self, host, capped):
        install(host.db, capped.files)
        result = host.check('--json')
        assert result.exit_code == 0, (result.stdout[-300:], result.stderr)
        entries = worker_entries(host)
        assert len(entries) == CAP and set(entries) <= set(PAIRS)
        assert all(e['state'] == 'OK' for e in entries.values())
        # the anchors, which `check` reads from ZFS, still cover every pair
        data = json.loads(result.stdout)['entries']
        anchors = {(e['subject'], e['peer']) for e in data if e['source'] == 'anchor'}
        assert anchors == set(PAIRS)

    def test_check_text_and_status_read_it_as_a_complete_file(self, host, capped):
        install(host.db, capped.files)
        text = host.check()
        assert text.exit_code == 0 and text.stdout.count('worker_send:') == CAP
        data = json.loads(host.run('-c', host.db, 'status', '--json').stdout)['workers']
        assert data['problems'] == [] and data['files'][0]['records'] == CAP


# ---------------------------------------------------------------------------------------------
# 5. heartbeat-only files
# ---------------------------------------------------------------------------------------------

class TestHeartbeatOnlyFiles:
    PARENT_PAIRS = sorted((p, d) for p in PARENTS for d in DESTS)

    def test_an_incarnation_with_everything_attempted_sends_only_the_parents_confirmations(self, mixed):
        """Every pair was attempted a moment ago, so nothing is due -- except that a recursive parent with
        children is sent once per incarnation, whatever its slot, to CONFIRM its copy before its children go
        (R-2). That is the whole of the work of this incarnation."""
        assert sorted(mixed.heartbeat_attempts) == self.PARENT_PAIRS

    def test_its_file_keeps_every_other_record_as_it_was_and_refreshes_the_heartbeat(self, mixed):
        before, after = mixed.records(), mixed.records(mixed.heartbeat_files)
        assert set(after) == set(before)
        parents = {('send', *p) for p in self.PARENT_PAIRS}
        assert {k: v for k, v in after.items() if k not in parents} == {
            k: v for k, v in before.items() if k not in parents}, 'a pair that was not attempted is untouched'
        for key in parents:
            assert after[key]['result'] == 'ok' and after[key]['consecutive_failures'] == 0
            assert after[key]['last_attempt'] >= before[key]['last_attempt']
        doc_before = json.loads(mixed.files[SEND + '.json'])
        doc_after = json.loads(mixed.heartbeat_files[SEND + '.json'])
        assert doc_after['updated_at'] >= doc_before['updated_at']
        assert doc_after['started_at'] >= doc_before['started_at']

    def test_check_grades_it_as_before(self, host, mixed):
        install(host.db, mixed.files)
        first = host.check('--json')
        install(host.db, mixed.heartbeat_files)
        second = host.check('--json')
        assert first.exit_code == second.exit_code == 1

        def graded(result):
            return {(e['source'], e['subject'], e.get('peer')): (e['state'], e.get('consecutive_failures'))
                    for e in json.loads(result.stdout)['entries']}
        assert graded(first) == graded(second)

    def test_a_file_with_no_records_adds_nothing(self, world, tmp_path):
        """A worker whose every pair is not due and has no record yet (so there is nothing to write but the
        heartbeat) leaves a file with no records: no entry, the host as healthy as without it."""
        plain = tuple(f'tank/q{i:02d}' for i in range(40))
        names = (*plain,)
        host = prepare(world, tmp_path, many_yaml(plain=plain, parents=()), datasets=names)
        root = tmp_path / 'quiet'
        root.mkdir()
        (root / 'in.yaml').write_text(many_yaml(plain=plain, parents=()))
        config = BackupConfig.from_file(root / 'in.yaml')
        directory = root / 'config.db.status'
        health.ensure_status_dir(directory)
        patcher = _Patcher()
        try:
            pool = _fresh_world(patcher, anchored=True, plain=plain, parents=())
            with pytest.MonkeyPatch.context() as mp:
                rig, sender = _incarnation(config, directory, mp, cycle=1)
                # every pair comes back NOT_DUE (what `send_pair` answers for the re-run of an attempted slot after
                # a clock step back: the only dispatch that keeps the anchor-age check)
                mp.setattr(
                    'zfsbackup.sendloop.send_pair',
                    lambda manager, dsi, remote_cfg, sender_, should_stop, exit_pending, **kw: PairOutcome(
                        dsi.name, remote_cfg.destination, PAIR_NOT_DUE),
                )
                rig.run()
            assert sender.backup_dataset.call_count == 0, 'every pair is not due'
        finally:
            patcher.stop()
        text = (directory / f'{SEND}.json').read_text()
        assert json.loads(text)['records'] == []
        bare = host.check('--json')
        install(host.db, {f'{SEND}.json': text})
        quiet = host.check('--json')
        assert bare.exit_code == quiet.exit_code == 0

        def stable(result):                        # the ages move with the clock
            return [{k: v for k, v in e.items() if k != 'age_seconds'} for e in json.loads(result.stdout)['entries']]
        assert stable(bare) == stable(quiet)
        status = json.loads(host.run('-c', host.db, 'status', '--json').stdout)['workers']['files'][0]
        assert status['records'] == 0 and status['failing'] == 0 and status['worst'] is None

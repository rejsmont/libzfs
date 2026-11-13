"""`zfsbackup.health_collect` (plan C5): the read-only collector over `DatasetManager`.

The ZFS world is faked one level below libzfseasy, at `subprocess.Popen`, so the tests see the real
argv the collector causes (`zfs list ...`, `zfs get ...`) and can prove that nothing else runs.
"""

import io
import json
import os
import shlex
import sys
from datetime import datetime, timedelta, timezone

import pytest

from zfsbackup import health
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX, PROP_CLIENT_ID, PROP_CONFIG, PROP_INFLIGHT_PREFIX,
    SNAPSHOT_LIST_PROPERTIES, DatasetManager,
)
from pathlib import Path

from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, Duration, RemoteDatasetConfig, RemoteServerConfig,
)
from zfsbackup.health import (
    CheckEntry, Observation, STATE_CRIT, STATE_OK, STATE_WARN, evaluate, overall_state,
)
from zfsbackup import health_collect
from zfsbackup.health_collect import collect
from libzfseasy.zfs import ZFSTimeoutError
from _clock_support import FakeTime
from _snapshot_support import NO_TXG, list_row

#: The fixed instant every age is measured against.
NOW_DT = datetime(2026, 10, 22, 12, 0, 0, tzinfo=timezone.utc)
NOW = NOW_DT.timestamp()
HOUR = 3600
DAY = 86400
PREFIX = 'autosnap'


def at(age_seconds: float, prefix: str = PREFIX) -> str:
    """A managed snapshot name dated `age_seconds` before `NOW`."""
    return f"{prefix}_{(NOW_DT - timedelta(seconds=age_seconds)).strftime('%Y%m%d%H%M%S')}"


def epoch_of(name: str) -> float:
    stamp = name.rsplit('_', 1)[1]
    return datetime.strptime(stamp, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc).timestamp()


# ---------------------------------------------------------------------------------------------
# A fake `zfs` at the Popen level
# ---------------------------------------------------------------------------------------------

class _Proc:
    def __init__(self, stdout: bytes, stderr: bytes, rc: int):
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO(stderr)
        self.returncode = rc
        self.pid = 4242

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode

    def terminate(self):
        pass

    kill = terminate


class FakeZfsWorld:
    """What `zfs list` and `zfs get` print for a small pool.

    * `snaps[dataset]` -- snapshot names, or `(name, txg)` pairs to pin the `createtxg`
    * `props[(dataset, prop)]` -- a user property (everything is `local`)
    * `filesystems` / `volumes` -- what `zfs list -t filesystem,volume -r <target>` returns
    * `list_fails` -- datasets whose snapshot listing exits 1
    * `missing_targets` -- targets whose strict (non-recursive) filesystem listing prints nothing
      (`empty_target`) or exits 1 with "does not exist"; `target_list_fails` -- targets whose
      listing fails with a pool error

    Every call's argv (after the binary) is in `calls`; any other subcommand is recorded and
    raises AssertionError at once (so a write cannot succeed silently, and cannot hang a text-mode
    reader either)."""

    def __init__(self, mocker):
        self.snaps = {}
        self.props = {}
        self.filesystems = []
        self.volumes = []
        self.list_fails = set()
        self.get_fails = set()
        self.get_prop_fails = set()       # (dataset, property) pairs whose value read fails
        self.empty_targets = set()
        self.missing_targets = set()
        self.target_list_fails = set()
        self.enum_fails = set()           # roots whose recursive filesystem,volume enumeration exits 1
        self.hook = None                  # called with each call's argv (after the binary) before it is answered
        self.calls = []
        mocker.patch('subprocess.Popen', side_effect=self._popen)

    # -- helpers for the tests ----------------------------------------------------------------
    @property
    def subcommands(self):
        return [argv[0] for argv in self.calls]

    def set_prop(self, dataset, prop, value):
        self.props[(dataset, prop)] = value

    # -- the fake ---------------------------------------------------------------------------
    def _popen(self, argv, *args, **kwargs):
        argv = [os.fspath(a) for a in argv]
        rest = argv[1:]
        self.calls.append(rest)
        if self.hook is not None:
            self.hook(rest)
        sub = rest[0]
        if sub == 'list':
            return self._list(rest)
        if sub == 'get':
            return self._get(rest)
        raise AssertionError(f'fake zfs: {sub} is not allowed here: {rest}')

    def _list(self, rest):
        types = rest[rest.index('-t') + 1] if '-t' in rest else 'filesystem,volume,snapshot'
        columns = rest[rest.index('-o') + 1].split(',')
        root = rest[-1]
        if types == 'snapshot':
            if root in self.list_fails:
                return _Proc(b'', f"cannot open '{root}': pool I/O is currently suspended\n".encode(), 1)
            known = ['name', 'type'] + list(SNAPSHOT_LIST_PROPERTIES)
            lines = []
            for entry in self.snaps.get(root, []):
                name, txg = entry if isinstance(entry, tuple) else (entry, None)
                kw = {} if txg is None else {'txg': txg}
                cells = dict(zip(known, list_row(name, root, **kw).rstrip('\n').split('\t')))
                lines.append('\t'.join(cells.get(c, '-') for c in columns))
            return _Proc(''.join(line + '\n' for line in lines).encode(), b'', 0)
        # the strict look at the target is filesystem-only; the recursive enumeration of received
        # datasets (`received_datasets`) asks for volumes too
        assert types in ('filesystem', 'filesystem,volume'), types
        assert ('-r' in rest) == (types == 'filesystem,volume'), rest
        if '-r' not in rest:
            # the strict look at the target itself (health_collect._target_problem)
            if root in self.target_list_fails:
                return _Proc(b'', f"cannot open '{root}': pool I/O is currently suspended\n".encode(), 1)
            if root in self.missing_targets:
                return _Proc(b'', f"cannot open '{root}': dataset does not exist\n".encode(), 1)
            if root in self.empty_targets:
                return _Proc(b'', b'', 0)
            return _Proc(('\t'.join({'name': root, 'type': 'filesystem'}.get(c, '-') for c in columns)
                          + '\n').encode(), b'', 0)
        if root in self.enum_fails:
            return _Proc(b'', f"cannot open '{root}': pool I/O is currently suspended\n".encode(), 1)
        lines = ['\t'.join({'name': n, 'type': 'filesystem'}.get(c, '-') for c in columns)
                 for n in self.filesystems]
        lines += ['\t'.join({'name': n, 'type': 'volume'}.get(c, '-') for c in columns)
                  for n in self.volumes]
        return _Proc(''.join(line + '\n' for line in lines).encode(), b'', 0)

    def _get(self, rest):
        if rest[rest.index('-o') + 1] != 'value':
            return self._get_rows(rest)
        prop, name = rest[-2], rest[-1]
        if name in self.get_fails or (name, prop) in self.get_prop_fails:
            return _Proc(b'', f"cannot open '{name}': dataset does not exist\n".encode(), 1)
        value = self.props.get((name, prop))
        return _Proc(f"{'-' if value is None else value}\n".encode(), b'', 0)


    def _get_rows(self, rest):
        """The batch `zfs get -H -o name,property,value,source -r -t filesystem,volume name,type,<p>... <target>`
        of `received_datasets()` (plan B-1d): one object after another, `name` and `type` rows first, then
        each requested property in order (`-` when unset), every set property local."""
        assert rest[rest.index('-o') + 1] == 'name,property,value,source', rest
        assert '-r' in rest and '-s' not in rest, rest
        target, requested = rest[-1], rest[-2].split(',')[2:]
        if target in self.get_fails:
            return _Proc(b'', f"cannot open '{target}': dataset does not exist\n".encode(), 1)
        out = []
        for name, dstype in [(n, 'filesystem') for n in self.filesystems] + [(n, 'volume') for n in self.volumes]:
            if not (name == target or name.startswith(target + '/')):
                continue
            out.append(f'{name}\tname\t{name}\t-')
            out.append(f'{name}\ttype\t{dstype}\t-')
            for prop in requested:
                value = self.props.get((name, prop))
                out.append(f'{name}\t{prop}\t-\t-' if value is None else f'{name}\t{prop}\t{value}\tlocal')
        return _Proc(''.join(line + '\n' for line in out).encode(), b'', 0)


@pytest.fixture
def world(mocker):
    return FakeZfsWorld(mocker)


def config(*datasets, **kw) -> BackupConfig:
    kw.setdefault('snapshot_prefix', PREFIX)
    return BackupConfig(datasets=list(datasets), **kw)


def ds(name='pool/data', frequency='1h', *remotes, **kw) -> DatasetConfig:
    return DatasetConfig(name=name, frequency=Duration(frequency), remote=list(remotes), **kw)


def remote(destination='offsite', frequency=None) -> RemoteDatasetConfig:
    return RemoteDatasetConfig(destination=destination, frequency=Duration(frequency) if frequency else None)


def by_source(items, source):
    return [o for o in items if getattr(o, 'source', None) == source]


def graded(cfg, world_, **kw):
    return evaluate(collect(cfg), NOW, **kw)


# ---------------------------------------------------------------------------------------------
# Local datasets
# ---------------------------------------------------------------------------------------------

class TestLocalObservations:
    def test_one_snapshot_observation_per_dataset(self, world):
        world.snaps['pool/data'] = [at(7200), at(3600), at(60)]
        (o,) = collect(config(ds()))
        assert isinstance(o, Observation)
        assert (o.source, o.subject, o.peer) == ('snapshot', 'pool/data', None)
        assert o.expected_seconds == HOUR
        assert o.newest_ts == epoch_of(at(60))
        assert o.oldest_ts == epoch_of(at(7200))
        assert o.error is None
        assert o.detail == at(60)

    def test_the_expectation_is_the_datasets_frequency(self, world):
        world.snaps['pool/a'] = [at(60)]
        world.snaps['pool/b'] = [at(60)]
        a, b = collect(config(ds('pool/a', '15m'), ds('pool/b', '1d')))
        assert (a.expected_seconds, b.expected_seconds) == (900, DAY)

    def test_datasets_come_in_config_order(self, world):
        names = ['pool/z', 'pool/a', 'pool/m']
        result = collect(config(*[ds(n) for n in names]))
        assert [o.subject for o in result] == names

    def test_no_snapshots_gives_an_observation_that_grades_crit(self, world):
        (o,) = collect(config(ds()))
        assert o.newest_ts is None and o.oldest_ts is None and o.error is None
        (entry,) = evaluate([o], NOW)
        assert entry.state == STATE_CRIT and entry.reason == 'no managed snapshots'

    def test_unmanaged_snapshots_do_not_count(self, world):
        world.snaps['pool/data'] = ['manual-backup', 'autosnap_notadate', f"other_{at(10).split('_')[1]}",
                                    at(7200)]
        (o,) = collect(config(ds()))
        assert o.newest_ts == o.oldest_ts == epoch_of(at(7200))

    def test_only_unmanaged_snapshots_is_no_snapshots(self, world):
        world.snaps['pool/data'] = ['manual-backup']
        (o,) = collect(config(ds()))
        assert o.newest_ts is None

    def test_the_configured_prefix_decides_what_is_managed(self, world):
        world.snaps['pool/data'] = [at(60, 'mine'), at(10)]
        (o,) = collect(config(ds(), snapshot_prefix='mine'))
        assert o.newest_ts == epoch_of(at(60, 'mine'))

    def test_createtxg_decides_newest_and_oldest(self, world):
        # the name of A says it is older, but it was created later: a clock that went back
        a, b, c = at(7200), at(600), at(3600)
        world.snaps['pool/data'] = [(a, 30), (b, 10), (c, 20)]
        (o,) = collect(config(ds()))
        assert o.newest_ts == epoch_of(a)              # txg 30
        assert o.oldest_ts == epoch_of(b)              # txg 10
        assert o.detail == a

    def test_a_snapshot_without_a_createtxg_is_left_out(self, world):
        good, ungated = at(7200), at(10)
        world.snaps['pool/data'] = [good, (ungated, NO_TXG)]
        (o,) = collect(config(ds()))
        assert o.newest_ts == epoch_of(good)

    def test_the_clock_tolerance_is_the_managers(self, world):
        (a,) = collect(config(ds()))
        (b,) = collect(config(ds(), check_interval=timedelta(minutes=20)))
        assert a.tolerance_seconds == 300
        assert b.tolerance_seconds == 1200

    def test_end_to_end_grading(self, world):
        world.snaps['pool/ok'] = [at(600)]
        world.snaps['pool/warn'] = [at(2 * HOUR + 5)]
        world.snaps['pool/crit'] = [at(4 * HOUR + 5)]
        cfg = config(ds('pool/ok'), ds('pool/warn'), ds('pool/crit'), ds('pool/empty'))
        states = {e.subject: e.state for e in graded(cfg, world)}
        assert states == {'pool/ok': STATE_OK, 'pool/warn': STATE_WARN, 'pool/crit': STATE_CRIT,
                          'pool/empty': STATE_CRIT}

    def test_a_future_dated_newest_snapshot_grades_clock_behind(self, world):
        world.snaps['pool/data'] = [at(-2 * HOUR)]
        (entry,) = graded(config(ds()), world)
        assert entry.state == STATE_CRIT and 'clock behind' in entry.reason

    def test_a_slightly_future_snapshot_within_tolerance_is_fine(self, world):
        world.snaps['pool/data'] = [at(-60)]
        (entry,) = graded(config(ds()), world)
        assert entry.state == STATE_OK


class TestListFailures:
    def test_a_failed_listing_is_an_error_observation(self, world):
        world.list_fails.add('pool/data')
        (o,) = collect(config(ds()))
        assert o.source == 'snapshot' and o.error
        assert 'pool I/O is currently suspended' in o.error
        assert o.newest_ts is None

    def test_a_failed_listing_grades_crit_cannot_read(self, world):
        world.list_fails.add('pool/data')
        (entry,) = graded(config(ds()), world)
        assert entry.state == STATE_CRIT
        assert entry.reason.startswith('cannot read: ')
        assert 'suspended' in entry.reason

    def test_a_failed_listing_is_not_confused_with_no_snapshots(self, world):
        world.list_fails.add('pool/data')
        (failed,) = graded(config(ds()), world)
        world.list_fails.clear()
        (empty,) = graded(config(ds()), world)
        assert failed.reason != empty.reason
        assert empty.reason == 'no managed snapshots'

    def test_the_datasets_anchors_are_errors_too(self, world):
        world.list_fails.add('pool/data')
        result = collect(config(ds('pool/data', '1h', remote('a'), remote('b', '6h'))))
        assert [(o.source, o.peer) for o in result] == [('snapshot', None), ('anchor', 'a'), ('anchor', 'b')]
        assert all(o.error and 'suspended' in o.error for o in result)
        assert result[2].expected_seconds == 6 * HOUR

    def test_the_failed_anchors_read_no_property(self, world):
        world.list_fails.add('pool/data')
        collect(config(ds('pool/data', '1h', remote('a'))))
        assert 'get' not in world.subcommands

    def test_every_error_entry_is_crit(self, world):
        world.list_fails.add('pool/data')
        entries = graded(config(ds('pool/data', '1h', remote('a'), remote('b'))), world)
        assert [e.state for e in entries] == [STATE_CRIT] * 3

    def test_other_datasets_are_still_observed(self, world):
        world.list_fails.add('pool/bad')
        world.snaps['pool/good'] = [at(60)]
        bad, good = collect(config(ds('pool/bad'), ds('pool/good')))
        assert bad.error and good.error is None and good.newest_ts == epoch_of(at(60))

    def test_collect_never_raises_on_a_zfs_failure(self, world):
        world.list_fails.update({'pool/a', 'pool/b'})
        assert len(collect(config(ds('pool/a'), ds('pool/b')))) == 2

    def test_a_failure_that_raises_something_odd(self, mocker, world):
        manager = DatasetManager(config(ds()))
        mocker.patch.object(manager, '_list_snapshots_strict', side_effect=RuntimeError(''))
        (o,) = collect(config(ds()), manager=manager)
        assert o.error == 'RuntimeError'                 # an empty message still names the failure

    def test_the_public_list_snapshots_is_not_used(self, mocker, world):
        """`list_snapshots` swallows a `zfs list` failure and returns `[]`; using it would report an
        unreadable dataset as 'no managed snapshots' instead of 'cannot read'."""
        world.list_fails.add('pool/data')
        manager = DatasetManager(config(ds()))
        mocker.patch.object(manager, 'list_snapshots', return_value=[])
        (o,) = collect(config(ds()), manager=manager)
        assert o.error


class TestAnchors:
    def anchor_prop(self, destination='offsite'):
        return PROP_ANCHOR_PREFIX + destination

    def test_the_anchor_observation(self, world):
        world.snaps['pool/data'] = [at(3 * HOUR), at(2 * HOUR), at(HOUR)]
        world.set_prop('pool/data', self.anchor_prop(), at(2 * HOUR))
        local, anchor = collect(config(ds('pool/data', '1h', remote('offsite'))))
        assert (anchor.source, anchor.subject, anchor.peer) == ('anchor', 'pool/data', 'offsite')
        assert anchor.newest_ts == epoch_of(at(2 * HOUR))
        assert anchor.oldest_ts == epoch_of(at(3 * HOUR))
        assert anchor.expected_seconds == HOUR
        assert anchor.error is None
        assert anchor.detail == at(2 * HOUR)

    def test_the_local_observation_precedes_its_anchors(self, world):
        result = collect(config(ds('pool/a', '1h', remote('x'), remote('y')), ds('pool/b', '1h', remote('x'))))
        assert [(o.source, o.subject, o.peer) for o in result] == [
            ('snapshot', 'pool/a', None), ('anchor', 'pool/a', 'x'), ('anchor', 'pool/a', 'y'),
            ('snapshot', 'pool/b', None), ('anchor', 'pool/b', 'x'),
        ]

    @pytest.mark.parametrize('dataset, remote_freq, expected', [
        ('1h', None, HOUR),                 # inherits the dataset's frequency
        ('1h', '6h', 6 * HOUR),             # a slower remote widens it
        ('1d', '1h', DAY),                  # a faster remote cannot beat the snapshots
        ('1h', '1h', HOUR),
        ('30m', '45m', 45 * 60),
    ])
    def test_the_expectation_is_the_larger_of_dataset_and_remote(self, world, dataset, remote_freq, expected):
        _, anchor = collect(config(ds('pool/data', dataset, remote('offsite', remote_freq))))
        assert anchor.expected_seconds == expected

    def test_a_day_old_dataset_snapshot_with_an_hourly_remote(self, world):
        """A snapshot 1d old with a 1h remote: the expectation is 1d, so an anchor 1d1h old is OK."""
        world.snaps['pool/data'] = [at(DAY + HOUR), at(DAY)]
        world.set_prop('pool/data', self.anchor_prop(), at(DAY + HOUR))
        entries = graded(config(ds('pool/data', '1d', remote('offsite', '1h'))), world)
        assert [(e.source, e.state) for e in entries] == [('snapshot', STATE_OK), ('anchor', STATE_OK)]

    def test_two_destinations_read_their_own_properties(self, world):
        world.snaps['pool/data'] = [at(3 * HOUR), at(2 * HOUR), at(HOUR)]
        world.set_prop('pool/data', self.anchor_prop('a'), at(3 * HOUR))
        world.set_prop('pool/data', self.anchor_prop('b'), at(HOUR))
        _, a, b = collect(config(ds('pool/data', '1h', remote('a'), remote('b'))))
        assert (a.peer, a.newest_ts) == ('a', epoch_of(at(3 * HOUR)))
        assert (b.peer, b.newest_ts) == ('b', epoch_of(at(HOUR)))

    def test_no_anchor_property_is_no_newest_with_the_oldest_snapshot(self, world):
        world.snaps['pool/data'] = [at(30 * 60), at(10)]
        _, anchor = collect(config(ds('pool/data', '1h', remote())))
        assert anchor.newest_ts is None
        assert anchor.oldest_ts == epoch_of(at(30 * 60))
        assert anchor.detail is None

    def test_no_anchor_grades_warn_then_crit_by_the_oldest_snapshot(self, world):
        world.snaps['pool/data'] = [at(30 * 60)]
        _, young = graded(config(ds('pool/data', '1h', remote())), world)
        assert young.state == STATE_WARN and young.reason == 'not yet sent'
        world.snaps['pool/data'] = [at(4 * HOUR), at(60)]
        _, old = graded(config(ds('pool/data', '1h', remote())), world)
        assert old.state == STATE_CRIT and 'not yet sent' in old.reason

    def test_no_anchor_and_no_snapshots_is_warn_for_the_anchor_crit_for_the_dataset(self, world):
        snapshot, anchor = graded(config(ds('pool/data', '1h', remote())), world)
        assert (snapshot.state, anchor.state) == (STATE_CRIT, STATE_WARN)

    def test_an_anchor_missing_from_the_local_snapshots_counts_as_no_anchor(self, world):
        world.snaps['pool/data'] = [at(HOUR)]
        world.set_prop('pool/data', self.anchor_prop(), at(5 * HOUR))        # destroyed meanwhile
        _, anchor = collect(config(ds('pool/data', '1h', remote())))
        assert anchor.newest_ts is None
        assert at(5 * HOUR) in anchor.detail and 'not among the local managed snapshots' in anchor.detail
        (_, entry) = evaluate(collect(config(ds('pool/data', '1h', remote()))), NOW)
        assert entry.state == STATE_WARN and 'not among' in entry.reason

    def test_an_anchor_equal_to_the_newest_snapshot_is_ok(self, world):
        world.snaps['pool/data'] = [at(2 * HOUR), at(600)]
        world.set_prop('pool/data', self.anchor_prop(), at(600))
        snapshot, anchor = graded(config(ds('pool/data', '1h', remote())), world)
        assert snapshot.state == anchor.state == STATE_OK
        assert snapshot.age_seconds == anchor.age_seconds == 600

    def test_a_stale_anchor_grades_by_the_anchor_not_the_newest_snapshot(self, world):
        world.snaps['pool/data'] = [at(5 * HOUR), at(60)]
        world.set_prop('pool/data', self.anchor_prop(), at(5 * HOUR))
        snapshot, anchor = graded(config(ds('pool/data', '1h', remote())), world)
        assert snapshot.state == STATE_OK and anchor.state == STATE_CRIT

    def test_a_pinned_send_is_a_note_and_leaves_the_grade_alone(self, world):
        world.snaps['pool/data'] = [at(2 * HOUR), at(600)]
        world.set_prop('pool/data', self.anchor_prop(), at(2 * HOUR))
        world.set_prop('pool/data', PROP_INFLIGHT_PREFIX + 'offsite', f'{at(2 * HOUR)},{at(600)}')
        _, anchor = collect(config(ds('pool/data', '1h', remote())))
        assert 'send in flight' in anchor.detail
        assert f'{at(2 * HOUR)} -> {at(600)}' in anchor.detail
        _, entry = graded(config(ds('pool/data', '1h', remote())), world)
        assert entry.state == STATE_WARN                                  # 2h old anchor: WARN regardless
        world.set_prop('pool/data', self.anchor_prop(), at(600))
        _, fresh = graded(config(ds('pool/data', '1h', remote())), world)
        assert fresh.state == STATE_OK and 'send in flight' in fresh.reason

    def test_a_pinned_full_send(self, world):
        world.snaps['pool/data'] = [at(600)]
        world.set_prop('pool/data', PROP_INFLIGHT_PREFIX + 'offsite', f',{at(600)}')
        _, anchor = collect(config(ds('pool/data', '1h', remote())))
        assert f'(full) -> {at(600)}' in anchor.detail

    def test_a_malformed_pin_is_ignored(self, world):
        world.snaps['pool/data'] = [at(600)]
        world.set_prop('pool/data', PROP_INFLIGHT_PREFIX + 'offsite', 'garbage-without-comma')
        _, anchor = collect(config(ds('pool/data', '1h', remote())))
        assert anchor.detail is None

    def test_a_future_dated_anchor_is_clock_behind(self, world):
        future = at(-3 * HOUR)
        world.snaps['pool/data'] = [at(HOUR), future]
        world.set_prop('pool/data', self.anchor_prop(), future)
        anchor = graded(config(ds('pool/data', '1h', remote())), world)[1]
        assert anchor.state == STATE_CRIT and 'clock behind' in anchor.reason

    def test_a_property_read_failure_is_an_error_not_no_anchor(self, world):
        """A3 (A1 review L2): an unreadable anchor is "cannot read", never "not yet sent"."""
        world.snaps['pool/data'] = [at(600)]
        world.get_fails.add('pool/data')
        _, anchor = collect(config(ds('pool/data', '1h', remote())))
        assert anchor.newest_ts is None
        assert anchor.error and anchor.error.startswith('the anchor could not be read: ')
        assert 'dataset does not exist' in anchor.error
        _, entry = graded(config(ds('pool/data', '1h', remote())), world)
        assert entry.state == STATE_CRIT and entry.reason.startswith('cannot read: ')

    def test_a_dataset_without_remotes_has_no_anchor_observations(self, world):
        world.snaps['pool/data'] = [at(600)]
        collect(config(ds()))
        assert 'get' not in world.subcommands


# ---------------------------------------------------------------------------------------------
# dry_run, disabled datasets
# ---------------------------------------------------------------------------------------------

class TestDryRunAndDisabled:
    def test_dry_run_is_a_warn_check_entry(self, world):
        result = collect(config(ds(), dry_run=True))
        first = result[0]
        assert isinstance(first, CheckEntry) and not isinstance(first, Observation)
        assert (first.source, first.subject, first.state) == ('config', 'dry_run', STATE_WARN)
        assert 'dry_run' in first.reason

    def test_dry_run_passes_through_evaluate_and_makes_the_overall_warn(self, world):
        world.snaps['pool/data'] = [at(60)]
        entries = graded(config(ds(), dry_run=True), world)
        assert [e.state for e in entries] == [STATE_WARN, STATE_OK]
        assert overall_state(entries) == STATE_WARN

    def test_dry_run_does_not_hide_a_crit(self, world):
        entries = graded(config(ds(), dry_run=True), world)
        assert overall_state(entries) == STATE_CRIT

    def test_no_dry_run_no_entry(self, world):
        world.snaps['pool/data'] = [at(60)]
        assert not [e for e in collect(config(ds())) if isinstance(e, CheckEntry) and not isinstance(e, Observation)]

    def test_disabled_datasets_are_skipped(self, world):
        world.snaps['pool/on'] = [at(60)]
        world.snaps['pool/off'] = [at(60)]
        result = collect(config(ds('pool/on'), ds('pool/off', enabled=False)))
        assert [o.subject for o in result] == ['pool/on']

    def test_a_disabled_dataset_costs_no_zfs_call(self, world):
        collect(config(ds('pool/off', '1h', remote(), enabled=False)))
        assert world.calls == []

    def test_everything_disabled_is_empty(self, world):
        assert collect(config(ds('pool/off', enabled=False))) == []

    def test_disabled_remote_entries_of_a_disabled_dataset_do_not_appear(self, world):
        result = collect(config(ds('pool/off', '1h', remote('x'), enabled=False)))
        assert by_source(result, 'anchor') == []

    def test_no_datasets_at_all(self, world):
        assert collect(config()) == []


# ---------------------------------------------------------------------------------------------
# Received datasets (a server)
# ---------------------------------------------------------------------------------------------

TARGET = 'pool/backups'


def client_ds(name='pool/src', frequency='1h', *remotes) -> DatasetConfig:
    return ds(name, frequency, *remotes)


def server_config(*datasets, enabled=True, target=TARGET, **kw) -> BackupConfig:
    return BackupConfig(datasets=list(datasets), snapshot_prefix=PREFIX,
                        remote_backup=RemoteServerConfig(target_dataset=target, enabled=enabled), **kw)


def add_received(world, name, client_id, cfg, snapshots=()):
    world.filesystems.append(name)
    world.set_prop(name, PROP_CLIENT_ID, client_id)
    world.set_prop(name, PROP_CONFIG, cfg.to_property())
    world.snaps[name] = list(snapshots)


def add_received_volume(world, name, client_id, cfg, snapshots=()):
    world.volumes.append(name)
    world.set_prop(name, PROP_CLIENT_ID, client_id)
    world.set_prop(name, PROP_CONFIG, cfg.to_property())
    world.snaps[name] = list(snapshots)


class TestReceivedObservations:
    def test_a_received_volume_is_observed_like_a_filesystem(self, world):
        add_received_volume(world, f'{TARGET}/laptop/vol', 'laptop', client_ds('pool/vol'), [at(7200), at(600)])
        (o,) = collect(server_config())
        assert (o.source, o.subject, o.peer) == ('received', f'{TARGET}/laptop/vol', 'laptop')
        assert o.newest_ts == epoch_of(at(600))
        assert o.expected_seconds == HOUR and o.error is None

    def test_a_received_volume_is_graded_by_its_lag(self, world):
        add_received_volume(world, f'{TARGET}/c/vol', 'c', client_ds('pool/vol', '1h'), [at(10 * HOUR)])
        add_received(world, f'{TARGET}/c/fs', 'c', client_ds('pool/fs', '1h'), [at(60)])
        entries = {e.subject: e for e in graded(server_config(), world)}
        assert entries[f'{TARGET}/c/vol'].state == STATE_CRIT
        assert entries[f'{TARGET}/c/fs'].state == STATE_OK

    def test_the_enumeration_asks_for_volumes_too_and_snapshots_are_listed_by_name(self, world):
        add_received_volume(world, f'{TARGET}/c/vol', 'c', client_ds('pool/vol'), [at(60)])
        collect(server_config())
        enumeration = [a for a in world.calls if a[0] == 'list' and '-r' in a and 'snapshot' not in a]
        assert enumeration and all(a[a.index('-t') + 1] == 'filesystem,volume' for a in enumeration)
        assert not [a for a in world.calls if a[0] not in ('list', 'get')]       # read-only, no mount handling

    def test_a_volume_beside_filesystems_in_one_tree(self, world):
        add_received(world, f'{TARGET}/c/fs', 'c', client_ds('pool/fs'), [at(60)])
        add_received_volume(world, f'{TARGET}/c/vol', 'c', client_ds('pool/vol'), [at(60)])
        assert sorted(o.subject for o in collect(server_config())) == [f'{TARGET}/c/fs', f'{TARGET}/c/vol']

    def test_a_received_dataset_is_observed(self, world):
        add_received(world, f'{TARGET}/laptop/src', 'laptop', client_ds(), [at(7200), at(600)])
        (o,) = collect(server_config())
        assert isinstance(o, Observation)
        assert (o.source, o.subject, o.peer) == ('received', f'{TARGET}/laptop/src', 'laptop')
        assert o.newest_ts == epoch_of(at(600))
        assert o.expected_seconds == HOUR
        assert o.detail == at(600)
        assert o.error is None

    def test_the_client_id_comes_from_the_dataset_path(self, world):
        # the property is checked for the same id by the manager; the observation reads the path
        add_received(world, f'{TARGET}/box-1/a/b', 'box-1', client_ds('pool/a/b'), [at(60)])
        add_received(world, f'{TARGET}/box-2', 'box-2', client_ds('pool/x'), [at(60)])
        a, b = collect(server_config())
        assert (a.peer, b.peer) == ('box-1', 'box-2')

    def test_the_target_with_a_trailing_slash(self, world):
        add_received(world, f'{TARGET}/laptop/src', 'laptop', client_ds(), [at(60)])
        (o,) = collect(server_config(target=TARGET + '/'))
        assert o.peer == 'laptop'

    def test_containers_without_properties_are_not_observed(self, world):
        world.filesystems += [TARGET, f'{TARGET}/laptop']
        add_received(world, f'{TARGET}/laptop/src', 'laptop', client_ds(), [at(60)])
        assert [o.subject for o in collect(server_config())] == [f'{TARGET}/laptop/src']

    def test_the_expectation_is_the_clients_dataset_frequency(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(frequency='6h'), [at(60)])
        (o,) = collect(server_config())
        assert o.expected_seconds == 6 * HOUR

    def test_the_largest_client_frequency_wins(self, world):
        cfg = client_ds('pool/src', '1h', remote('a', '12h'), remote('b', '2h'), remote('c'))
        add_received(world, f'{TARGET}/c/x', 'c', cfg, [at(60)])
        (o,) = collect(server_config())
        assert o.expected_seconds == 12 * HOUR

    def test_a_slow_dataset_beats_faster_remotes(self, world):
        cfg = client_ds('pool/src', '1d', remote('a', '1h'))
        add_received(world, f'{TARGET}/c/x', 'c', cfg, [at(60)])
        (o,) = collect(server_config())
        assert o.expected_seconds == DAY

    def test_the_expectation_governs_the_grade(self, world):
        cfg = client_ds('pool/src', '1h', remote('a', '12h'))
        add_received(world, f'{TARGET}/c/x', 'c', cfg, [at(10 * HOUR)])
        (entry,) = graded(server_config(), world)
        assert entry.state == STATE_OK                       # 10h against a 12h expectation
        add_received(world, f'{TARGET}/c/y', 'c', client_ds('pool/y', '1h'), [at(10 * HOUR)])
        entry_y = graded(server_config(), world)[1]
        assert entry_y.state == STATE_CRIT                   # 10h against 1h

    def test_createtxg_decides_the_newest_received_snapshot(self, world):
        newer_name, older_name = at(60), at(3600)
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [(newer_name, 5), (older_name, 9)])
        (o,) = collect(server_config())
        assert o.newest_ts == epoch_of(older_name)

    def test_received_snapshots_use_the_servers_prefix(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60, 'clientpfx'), at(7200)])
        (o,) = collect(server_config())
        assert o.newest_ts == epoch_of(at(7200))

    def test_no_matching_snapshots_is_warn(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60, 'other')])
        (entry,) = graded(server_config(), world)
        assert entry.state == STATE_WARN
        assert entry.age_seconds is None

    def test_no_snapshots_at_all_is_warn(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds())
        (entry,) = graded(server_config(), world)
        assert entry.state == STATE_WARN

    def test_a_listing_failure_is_an_error_observation(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        add_received(world, f'{TARGET}/c/y', 'c', client_ds('pool/y'), [at(60)])
        world.list_fails.add(f'{TARGET}/c/x')
        bad, good = collect(server_config())
        assert bad.error and 'suspended' in bad.error and bad.peer == 'c'
        assert good.error is None and good.newest_ts is not None
        assert graded(server_config(), world)[0].state == STATE_CRIT

    def test_a_future_dated_received_snapshot_is_clock_behind(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(-5 * HOUR)])
        (entry,) = graded(server_config(), world)
        assert entry.state == STATE_CRIT and 'clock behind' in entry.reason

    def test_a_received_dataset_is_listed_non_recursively(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        collect(server_config())
        snapshot_lists = [a for a in world.calls if a[0] == 'list' and 'snapshot' in a]
        assert snapshot_lists and all('-r' not in a for a in snapshot_lists)

    def test_the_tolerance_applies_to_received_too(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        (o,) = collect(server_config(check_interval=timedelta(minutes=20)))
        assert o.tolerance_seconds == 1200

    def test_remote_backup_disabled_gives_no_received_entries(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        assert collect(server_config(enabled=False)) == []

    def test_remote_backup_disabled_does_not_even_enumerate(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        collect(server_config(enabled=False))
        assert world.calls == []

    def test_no_remote_backup_section_gives_no_received_entries(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        assert collect(config()) == []
        assert world.calls == []

    def test_local_and_received_sit_together(self, world):
        world.snaps['pool/data'] = [at(60)]
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        result = collect(server_config(ds()))
        assert [o.source for o in result] == ['snapshot', 'received']

    def test_dry_run_comes_first_then_local_then_received(self, world):
        world.snaps['pool/data'] = [at(60)]
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        result = collect(server_config(ds(), dry_run=True))
        assert [getattr(o, 'source') for o in result] == ['config', 'snapshot', 'received']

    def test_an_unreadable_enumeration_is_not_a_crash(self, mocker, world):
        manager = DatasetManager(server_config())
        mocker.patch.object(manager, 'received_datasets', side_effect=RuntimeError('listing broke'))
        (o,) = collect(server_config(), manager=manager)
        assert o.source == 'received' and o.error == 'listing broke'
        assert evaluate([o], NOW)[0].state == STATE_CRIT


class TestReceivedTarget:
    """`received_datasets()` logs a failed listing and returns [], which would make a server whose pool
    did not import look healthy; the collector looks at the target itself strictly first."""

    def test_a_missing_target_is_a_crit_on_the_target(self, world):
        world.empty_targets.add(TARGET)
        (o,) = collect(server_config())
        assert (o.source, o.subject, o.peer) == ('received', TARGET, None)
        assert o.error and 'does not exist' in o.error and TARGET in o.error
        (entry,) = evaluate([o], NOW)
        assert entry.state == STATE_CRIT and entry.reason.startswith('cannot read: ')

    def test_a_target_zfs_reports_as_nonexistent_is_a_crit_too(self, world):
        world.missing_targets.add(TARGET)
        (o,) = collect(server_config())
        assert o.subject == TARGET and 'does not exist' in o.error
        assert evaluate([o], NOW)[0].state == STATE_CRIT

    def test_a_failed_target_listing_is_a_crit(self, world):
        world.target_list_fails.add(TARGET)
        (o,) = collect(server_config())
        assert o.source == 'received' and o.subject == TARGET
        assert 'suspended' in o.error
        assert evaluate([o], NOW)[0].state == STATE_CRIT

    def test_a_trailing_slash_target_is_named_without_it(self, world):
        world.empty_targets.add(TARGET)
        (o,) = collect(server_config(target=TARGET + '/'))
        assert o.subject == TARGET

    def test_no_received_dataset_is_enumerated_when_the_target_is_bad(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        world.empty_targets.add(TARGET)
        collect(server_config())
        # only the strict, non-recursive look at the target: nothing is read for any child
        assert not [a for a in world.calls if a[0] == 'get']
        assert not [a for a in world.calls if a[0] == 'list' and '-r' in a]

    def test_a_bad_target_does_not_hide_the_local_datasets(self, world):
        world.snaps['pool/data'] = [at(60)]
        world.empty_targets.add(TARGET)
        local, received = collect(server_config(ds()))
        assert (local.source, local.error) == ('snapshot', None)
        assert (received.source, received.subject) == ('received', TARGET)
        assert overall_state(evaluate([local, received], NOW)) == STATE_CRIT

    def test_a_healthy_target_gives_the_per_dataset_entries(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        add_received(world, f'{TARGET}/c/y', 'c', client_ds('pool/y'), [at(7200)])
        result = collect(server_config())
        assert [(o.source, o.subject, o.peer, o.error) for o in result] == [
            ('received', f'{TARGET}/c/x', 'c', None), ('received', f'{TARGET}/c/y', 'c', None)]
        assert [e.state for e in evaluate(result, NOW)] == [STATE_OK, STATE_WARN]

    def test_a_healthy_target_with_no_clients_yet_has_no_entries(self, world):
        world.filesystems += [TARGET]
        assert collect(server_config()) == []

    def test_a_received_dataset_with_no_managed_snapshot_stays_warn(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60, 'other')])
        (o,) = collect(server_config())
        assert o.newest_ts is None and o.oldest_ts is None       # received entries carry no oldest_ts
        (entry,) = evaluate([o], NOW)
        assert entry.state == STATE_WARN

    def test_a_received_dataset_with_no_snapshot_stays_warn_however_long_ago(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds())
        (o,) = collect(server_config())
        assert evaluate([o], NOW + 365 * DAY)[0].state == STATE_WARN

    def test_the_target_check_is_not_made_when_remote_backup_is_off(self, world):
        collect(server_config(enabled=False))
        assert world.calls == []


class TestCheckIntervalFloor:
    """The snapshot worker looks only every `check_interval`, so a dataset configured finer than that
    cannot be expected more often than that."""

    def test_a_fast_dataset_is_expected_every_check_interval(self, world):
        (o,) = collect(config(ds('pool/data', '1m')))
        assert o.expected_seconds == 300                       # the default check_interval is 5m

    def test_the_floor_follows_the_configured_check_interval(self, world):
        (o,) = collect(config(ds('pool/data', '1m'), check_interval=timedelta(minutes=20)))
        assert o.expected_seconds == 1200

    def test_a_slower_dataset_keeps_its_own_frequency(self, world):
        (o,) = collect(config(ds('pool/data', '1h'), check_interval=timedelta(minutes=20)))
        assert o.expected_seconds == HOUR

    def test_equal_frequency_and_interval(self, world):
        (o,) = collect(config(ds('pool/data', '5m')))
        assert o.expected_seconds == 300

    def test_the_floor_governs_the_grade(self, world):
        # 8m old against a 1m frequency would be CRIT (>= 4m); against the 5m floor it is OK (< 10m)
        world.snaps['pool/data'] = [at(8 * 60)]
        (entry,) = graded(config(ds('pool/data', '1m')), world)
        assert entry.state == STATE_OK
        world.snaps['pool/data'] = [at(10 * 60)]
        assert graded(config(ds('pool/data', '1m')), world)[0].state == STATE_WARN

    def test_a_fast_dataset_with_no_snapshot_still_reports_the_floor_thresholds(self, world):
        (entry,) = graded(config(ds('pool/data', '1m')), world)
        assert (entry.expected_seconds, entry.warn_seconds, entry.crit_seconds) == (300, 600, 1200)

    def test_an_error_observation_carries_the_floor_too(self, world):
        world.list_fails.add('pool/data')
        snapshot, anchor = collect(config(ds('pool/data', '1m', remote('offsite', '2m'))))
        assert snapshot.expected_seconds == anchor.expected_seconds == 300

    @pytest.mark.parametrize('dataset, remote_freq, interval, expected', [
        ('1m', None, 5, 300),             # inherits: the floored dataset interval
        ('1m', '2m', 5, 300),             # a faster remote cannot beat the floor
        ('1m', '30m', 5, 1800),           # a slower remote widens it
        ('1h', '2m', 5, HOUR),
        ('1h', '30m', 90, 5400),          # the floor can exceed both
        ('1m', '1m', 20, 1200),
    ])
    def test_the_anchor_expectation_is_the_floored_interval_or_the_remote(
            self, world, dataset, remote_freq, interval, expected):
        cfg = config(ds('pool/data', dataset, remote('offsite', remote_freq)),
                     check_interval=timedelta(minutes=interval))
        snapshot, anchor = collect(cfg)
        assert anchor.expected_seconds == expected

    def test_the_floor_does_not_apply_to_received_datasets(self, world):
        # the client's own wire config decides (L3: documented, not floored)
        add_received(world, f'{TARGET}/c/x', 'c', client_ds('pool/src', '1m'), [at(60)])
        (o,) = collect(server_config(check_interval=timedelta(minutes=20)))
        assert o.expected_seconds == 60


# ---------------------------------------------------------------------------------------------
# Read-only
# ---------------------------------------------------------------------------------------------

class TestReadOnly:
    WRITES = {'set', 'inherit', 'destroy', 'snapshot', 'rename', 'send', 'receive', 'hold', 'release',
              'create', 'clone', 'promote', 'rollback', 'bookmark', 'load-key', 'unload-key', 'mount',
              'unmount', 'allow', 'unallow', 'diff'}

    def busy_server(self, world):
        world.snaps['pool/data'] = [at(7200), at(600)]
        world.set_prop('pool/data', PROP_ANCHOR_PREFIX + 'offsite', at(7200))
        world.set_prop('pool/data', PROP_INFLIGHT_PREFIX + 'offsite', f'{at(7200)},{at(600)}')
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        world.filesystems += [TARGET, f'{TARGET}/c']
        return server_config(ds('pool/data', '1h', remote('offsite'), remote('second')), dry_run=False)

    def test_only_list_and_get_run(self, world):
        cfg = self.busy_server(world)
        collect(cfg)
        assert world.calls
        assert set(world.subcommands) <= {'list', 'get'}
        assert not set(world.subcommands) & self.WRITES

    def test_only_list_and_get_run_when_everything_fails(self, world):
        cfg = self.busy_server(world)
        world.list_fails.update({'pool/data', f'{TARGET}/c/x'})
        collect(cfg)
        assert set(world.subcommands) <= {'list', 'get'}

    def test_get_is_read_only_shaped(self, world):
        collect(self.busy_server(world))
        value_reads = batch_reads = 0
        for argv in world.calls:
            if argv[0] == 'get':
                assert '-H' in argv
                columns = argv[argv.index('-o') + 1]
                assert columns in ('value', 'name,property,value,source'), argv   # never `received`
                if '-s' in argv:
                    # only the dataset's OWN anchor / pin read (`get_own_*`): `-s local`, one value
                    assert columns == 'value' and argv[argv.index('-s') + 1] == 'local', argv
                value_reads += columns == 'value' and argv[-1].startswith(TARGET + '/')
                batch_reads += columns != 'value'
        assert batch_reads == 1                      # the ONE batch read of the received datasets
        assert value_reads == 0                      # every received dataset came out of it: no per-dataset read

    def test_the_fake_really_refuses_a_write(self, world):
        # guards the guard: a collector that wrote would raise here, and the call is recorded
        import subprocess
        with pytest.raises(AssertionError, match='not allowed'):
            subprocess.Popen(['zfs', 'set', 'a:b=c', 'pool/data'])
        assert world.subcommands == ['set']

    def test_no_property_is_written_through_the_library(self, mocker, world):
        import libzfseasy as zfs
        for name in ('set', 'inherit', 'destroy', 'snapshot'):
            mocker.patch.object(zfs, name, side_effect=AssertionError(f'zfs.{name} called'))
        collect(self.busy_server(world))

    def test_no_files_are_created(self, tmp_path, monkeypatch, world):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv('HOME', str(tmp_path))
        monkeypatch.setenv('TMPDIR', str(tmp_path))
        before = sorted(p for p in tmp_path.rglob('*'))
        collect(self.busy_server(world))
        assert sorted(p for p in tmp_path.rglob('*')) == before == []

    def test_the_world_is_unchanged(self, world):
        cfg = self.busy_server(world)
        before = (dict(world.snaps), dict(world.props), list(world.filesystems))
        collect(cfg)
        assert (dict(world.snaps), dict(world.props), list(world.filesystems)) == before

    def test_it_does_not_touch_the_config_database(self):
        source = open(health_collect.__file__, encoding='utf-8').read()
        assert 'config.store' not in source and 'sqlalchemy' not in source

    def test_a_supplied_manager_is_used(self, mocker, world):
        manager = DatasetManager(config(ds()))
        spy = mocker.spy(manager, '_list_snapshots_strict')
        collect(config(ds()), manager=manager)
        assert spy.call_count == 1

    def test_the_default_manager_is_built_over_the_config(self, mocker, world):
        built = []
        real = DatasetManager

        def make(cfg):
            built.append(cfg)
            return real(cfg)

        mocker.patch.object(health_collect, 'DatasetManager', side_effect=make)
        cfg = config(ds())
        collect(cfg)
        assert built == [cfg]

    def test_the_result_is_observations_and_check_entries_only(self, world):
        cfg = self.busy_server(world)
        cfg.dry_run = True
        assert all(isinstance(o, (Observation, CheckEntry)) for o in collect(cfg))

    def test_the_collector_is_deterministic(self, world):
        cfg = self.busy_server(world)
        assert collect(cfg) == collect(cfg)

    def test_the_whole_report_renders(self, world):
        entries = graded(self.busy_server(world), world)
        assert '"schema": 1' in health.to_json(entries, NOW)
        assert 'zfsbackup_check_success' in health.to_prometheus(entries, NOW)


# ---------------------------------------------------------------------------------------------
# A3: the descendants of recursive datasets, over the read-only check fake (`_fake_zfs_check.py`)
# ---------------------------------------------------------------------------------------------

FAKE_CHECK_ZFS = Path(__file__).resolve().parent / '_fake_zfs_check.py'
ANCHOR_A = PROP_ANCHOR_PREFIX + 'a'
ANCHOR_B = PROP_ANCHOR_PREFIX + 'b'
INFLIGHT_B = PROP_INFLIGHT_PREFIX + 'b'
RECURSIVE = 'pool/data'


class CheckFake:
    """The JSON-spec fake `zfs` of `_fake_zfs_check.py` as a real executable behind `ZFS_CMD` (read at
    call time by libzfseasy). It answers the listings and reads `collect` makes, exits 2 on anything
    else, and logs every argv, so a test can prove what was (not) asked."""

    def __init__(self, directory: Path, monkeypatch):
        self.spec_path = directory / 'check-spec.json'
        self.log_path = directory / 'check-log.jsonl'
        self.spec = {'snapshots': {}, 'props': {}, 'fail': [], 'datasets': {}, 'markers': {},
                     'fail_names': [], 'fail_markers': [], 'fail_get': [],
                     'fail_tree': [], 'garbage_tree': [], 'garbage_first': [], 'fail_props1': [], 'fail_rows': [],
                     'fail_names_again': []}
        self.log_path.write_text('')
        wrapper = directory / 'zfs-check'
        wrapper.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(FAKE_CHECK_ZFS))} "$@"\n')
        wrapper.chmod(0o755)
        self.save()
        for key, value in {'ZFS_CMD': wrapper, 'ZPOOL_CMD': wrapper, 'FAKECHECK_SPEC': self.spec_path,
                           'FAKECHECK_LOG': self.log_path}.items():
            monkeypatch.setenv(key, str(value))

    def save(self):
        self.spec_path.write_text(json.dumps(self.spec))

    def tree(self, *names, snapshots=True):
        """`names` exist as filesystems (listed in this order, a parent before its children); each
        with one managed snapshot 30 minutes old unless `snapshots=False`."""
        for name in names:
            self.spec['datasets'][name] = 'filesystem'
            if snapshots:
                self.snaps(name, 1800)
        self.save()

    def snaps(self, dataset, *ages):
        """Managed snapshots `ages` seconds old; the youngest gets the highest `createtxg`."""
        ordered = sorted(ages, reverse=True)
        self.spec['snapshots'][dataset] = [[at(a), 100 + i] for i, a in enumerate(ordered)]
        self.save()
        return [at(a) for a in ordered]

    def prop(self, dataset, key, value):
        self.spec['props'].setdefault(dataset, {})[key] = value
        self.save()

    def marker(self, dataset, key=PROP_CLIENT_ID, source='local'):
        self.spec['markers'].setdefault(dataset, {})[key] = source
        self.save()

    def fail(self, kind, *entries):
        self.spec[kind].extend(entries)
        self.save()

    @property
    def calls(self):
        return [json.loads(line) for line in self.log_path.read_text().splitlines()]

    def calls_of(self, sub):
        return [c for c in self.calls if c[0] == sub]


@pytest.fixture
def check_zfs(tmp_path, monkeypatch):
    return CheckFake(tmp_path, monkeypatch)


def parent_ds(name=RECURSIVE, frequency='1h', *remotes, **kw):
    return ds(name, frequency, *(remotes or (remote('a'), remote('b', '6h'))), recursive=True, **kw)


def keys(items):
    return [(o.source, o.subject, getattr(o, 'peer', None)) for o in items]


@pytest.mark.subprocess
class TestDescendantObservations:
    def test_one_anchor_observation_per_child_and_destination_and_no_snapshot_observation(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x', f'{RECURSIVE}/x/y', f'{RECURSIVE}/z')
        result = collect(config(parent_ds()))
        assert keys(result) == [
            ('snapshot', RECURSIVE, None), ('anchor', RECURSIVE, 'a'), ('anchor', RECURSIVE, 'b'),
            ('anchor', f'{RECURSIVE}/x', 'a'), ('anchor', f'{RECURSIVE}/x', 'b'),
            ('anchor', f'{RECURSIVE}/x/y', 'a'), ('anchor', f'{RECURSIVE}/x/y', 'b'),
            ('anchor', f'{RECURSIVE}/z', 'a'), ('anchor', f'{RECURSIVE}/z', 'b'),
        ]
        assert all(isinstance(o, Observation) for o in result)

    def test_the_observation_carries_the_parents_interval_and_the_childs_own_snapshot_times(self, check_zfs):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(RECURSIVE)
        check_zfs.tree(child, snapshots=False)
        old, mid, new = check_zfs.snaps(child, 3 * HOUR, 2 * HOUR, HOUR)
        check_zfs.prop(child, ANCHOR_A, mid)
        # the parent: 1d, destination a: 2d, destination b: inherits 1d
        result = collect(config(parent_ds(RECURSIVE, '1d', remote('a', '2d'), remote('b'))))
        a, b = [o for o in result if o.subject == child]
        assert (a.source, a.subject, a.peer) == ('anchor', child, 'a')
        assert a.expected_seconds == 2 * DAY and b.expected_seconds == DAY
        assert a.newest_ts == epoch_of(mid) and a.oldest_ts == epoch_of(old)
        assert a.detail == mid and a.error is None
        assert a.tolerance_seconds == 300
        assert b.newest_ts is None and b.oldest_ts == epoch_of(old) and b.detail is None

    def test_the_interval_is_floored_at_the_check_interval_like_the_parents(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        result = collect(config(parent_ds(RECURSIVE, '1m', remote('a')), check_interval=timedelta(minutes=20)))
        parent, child = [o for o in result if o.source == 'anchor']
        assert child.expected_seconds == parent.expected_seconds == 1200
        assert child.tolerance_seconds == 1200

    def test_a_child_has_no_snapshot_observation_of_its_own(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        result = collect(config(parent_ds()))
        assert not [o for o in result if o.source == 'snapshot' and o.subject != RECURSIVE]
        assert not by_source(graded(config(parent_ds()), check_zfs), 'received')

    def test_children_follow_the_configured_datasets_in_config_order(self, check_zfs):
        check_zfs.tree('pool/one', 'pool/one/c1', 'pool/plain', 'pool/two', 'pool/two/c2')
        cfg = config(parent_ds('pool/one', '1h', remote('a')), ds('pool/plain'),
                     parent_ds('pool/two', '1h', remote('a')))
        assert keys(collect(cfg)) == [
            ('snapshot', 'pool/one', None), ('anchor', 'pool/one', 'a'),
            ('snapshot', 'pool/plain', None),
            ('snapshot', 'pool/two', None), ('anchor', 'pool/two', 'a'),
            ('anchor', 'pool/one/c1', 'a'), ('anchor', 'pool/two/c2', 'a'),
        ]

    def test_a_child_configured_explicitly_is_observed_once_by_its_own_entry(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x', f'{RECURSIVE}/y')
        cfg = config(parent_ds(RECURSIVE, '1h', remote('a')), ds(f'{RECURSIVE}/x', '2h', remote('a')))
        result = collect(cfg)
        assert keys(result).count(('anchor', f'{RECURSIVE}/x', 'a')) == 1
        assert keys(result).count(('snapshot', f'{RECURSIVE}/x', None)) == 1
        (own,) = [o for o in result if o.subject == f'{RECURSIVE}/x' and o.source == 'anchor']
        assert own.expected_seconds == 2 * HOUR                 # its own configuration, not the parent's
        (expanded,) = [o for o in result if o.subject == f'{RECURSIVE}/y']
        assert expanded.expected_seconds == HOUR

    def test_a_recursive_dataset_without_children_adds_nothing(self, check_zfs):
        check_zfs.tree(RECURSIVE)
        assert keys(collect(config(parent_ds()))) == [
            ('snapshot', RECURSIVE, None), ('anchor', RECURSIVE, 'a'), ('anchor', RECURSIVE, 'b')]

    @pytest.mark.parametrize('kw', [{'enabled': False}, {'remote': []}], ids=['disabled', 'no-remote'])
    def test_a_recursive_dataset_that_sends_nothing_costs_no_expansion(self, check_zfs, kw):
        import dataclasses
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        collect(config(dataclasses.replace(parent_ds(), **kw)))
        assert not [c for c in check_zfs.calls_of('list') if '-r' in c and 'snapshot' not in c]
        assert not check_zfs.calls_of('get') or kw == {'remote': []}

    def test_a_config_without_recursive_datasets_asks_nothing_extra(self, check_zfs):
        check_zfs.tree('pool/a')
        collect(config(ds('pool/a', '1h', remote('a'))))
        assert not [c for c in check_zfs.calls_of('list') if '-r' in c]
        assert not [c for c in check_zfs.calls_of('get') if 'name,property,source' in c]

    def test_the_expansion_is_the_read_only_name_listing_and_the_value_free_marker_read(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        collect(config(parent_ds()))
        # the same read-only name listing twice: the expansion, then the re-listing that drops a vanished child
        names_calls = [c for c in check_zfs.calls_of('list') if '-r' in c and 'snapshot' not in c]
        assert len(names_calls) == 2 and names_calls[0] == names_calls[1]
        names = names_calls[0]
        assert names[-1] == RECURSIVE and '-r' in names
        assert names[names.index('-o') + 1] == 'name,type' and names[names.index('-t') + 1] == 'filesystem,volume'
        (markers,) = [c for c in check_zfs.calls_of('get') if 'name,property,source' in c and c[-2] != 'all']
        assert markers[markers.index('-s') + 1] == 'local,received,inherited' and '-r' in markers
        assert markers[-2] == f'{PROP_CLIENT_ID},org.zfsbackup:source_dataset' and markers[-1] == RECURSIVE
        assert set(c[0] for c in check_zfs.calls) <= {'list', 'get'}

    def test_every_anchor_and_pin_read_is_local_only(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        collect(config(parent_ds()))
        # the configured parent: one `-s local` value read per property ...
        reads = [c for c in check_zfs.calls_of('get') if c[3] == 'value']
        assert {c[-2] for c in reads} == {ANCHOR_A, ANCHOR_B, PROP_INFLIGHT_PREFIX + 'a', INFLIGHT_B}
        assert {c[-1] for c in reads} == {RECURSIVE}
        assert all(c[4:6] == ['-s', 'local'] for c in reads)
        # ... and its child comes out of the batch, whose phase 1 asks for local/received values only
        # and whose phase 2 names the properties the phase 1 found (here none: the child has no anchor)
        (phase1,) = [c for c in check_zfs.calls_of('get') if 'name,property,source' in c and c[-2] == 'all']
        assert phase1[phase1.index('-s') + 1] == 'local,received' and phase1[-1] == RECURSIVE

    def test_a_pin_is_noted_on_the_childs_observation(self, check_zfs):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(RECURSIVE)
        check_zfs.tree(child, snapshots=False)
        old, new = check_zfs.snaps(child, 2 * HOUR, 600)
        check_zfs.prop(child, ANCHOR_B, old)
        check_zfs.prop(child, INFLIGHT_B, f'{old},{new}')
        (b,) = [o for o in collect(config(parent_ds())) if o.subject == child and o.peer == 'b']
        assert f'send in flight: {old} -> {new}' in b.detail and b.detail.startswith(old)

    def test_the_whole_report_grades_and_serialises(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        entries = graded(config(parent_ds()), check_zfs)
        assert '"schema": 1' in health.to_json(entries, NOW)
        assert 'zfsbackup_anchor_check_state' in health.to_prometheus(entries, NOW)

    def test_the_source_name_is_exported(self):
        assert health_collect.SOURCE_DESCENDANTS == 'descendants'
        assert 'SOURCE_DESCENDANTS' in health_collect.__all__


@pytest.mark.subprocess
class TestNeverSentChild:
    def test_warn_while_young_then_crit_once_the_oldest_snapshot_passes_the_threshold(self, check_zfs):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(RECURSIVE)
        check_zfs.tree(child, snapshots=False)
        cfg = config(parent_ds(RECURSIVE, '1h', remote('a')))
        check_zfs.snaps(child, 30 * 60, 60)
        (young,) = [e for e in graded(cfg, check_zfs) if e.subject == child]
        assert young.state == STATE_WARN and young.reason == 'not yet sent'
        check_zfs.snaps(child, 4 * HOUR - 1, 60)
        (almost,) = [e for e in graded(cfg, check_zfs) if e.subject == child]
        assert almost.state == STATE_WARN
        check_zfs.snaps(child, 4 * HOUR, 60)
        (old,) = [e for e in graded(cfg, check_zfs) if e.subject == child]
        assert old.state == STATE_CRIT and 'not yet sent' in old.reason

    def test_the_grace_is_the_childs_own_oldest_snapshot_not_the_parents(self, check_zfs):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(child, snapshots=False)
        check_zfs.snaps(RECURSIVE, 10 * HOUR, 60)           # an old parent
        check_zfs.snaps(child, 600)                          # a child that only just appeared
        entries = graded(config(parent_ds(RECURSIVE, '1h', remote('a'))), check_zfs)
        (c,) = [e for e in entries if e.subject == child]
        (p,) = [e for e in entries if e.source == 'anchor' and e.subject == RECURSIVE]
        assert c.state == STATE_WARN and p.state == STATE_CRIT

    def test_a_child_without_any_managed_snapshot_is_warn(self, check_zfs):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(RECURSIVE)
        check_zfs.tree(child, snapshots=False)
        (c,) = [e for e in graded(config(parent_ds(RECURSIVE, '1h', remote('a'))), check_zfs) if e.subject == child]
        assert c.state == STATE_WARN and 'no managed snapshot to send' in c.reason

    def test_a_sent_child_is_ok_and_goes_stale_by_its_anchor(self, check_zfs):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(RECURSIVE)
        check_zfs.tree(child, snapshots=False)
        old, new = check_zfs.snaps(child, 6 * HOUR, 600)
        check_zfs.prop(child, ANCHOR_A, new)
        cfg = config(parent_ds(RECURSIVE, '1h', remote('a')))
        (ok,) = [e for e in graded(cfg, check_zfs) if e.subject == child]
        assert ok.state == STATE_OK
        check_zfs.prop(child, ANCHOR_A, old)
        (stale,) = [e for e in graded(cfg, check_zfs) if e.subject == child]
        assert stale.state == STATE_CRIT

    def test_the_parents_own_anchor_is_never_borrowed_by_a_child(self, check_zfs):
        """The child's anchor is read `-s local`: a parent anchor naming a snapshot the child also
        holds (the recursive snapshot) says nothing about the child."""
        child = f'{RECURSIVE}/x'
        snap = at(600)
        check_zfs.tree(RECURSIVE, child, snapshots=False)
        check_zfs.snaps(RECURSIVE, 600)
        check_zfs.snaps(child, 600)
        check_zfs.prop(RECURSIVE, ANCHOR_A, snap)
        entries = graded(config(parent_ds(RECURSIVE, '1h', remote('a'))), check_zfs)
        (p,) = [e for e in entries if e.source == 'anchor' and e.subject == RECURSIVE]
        (c,) = [e for e in entries if e.subject == child]
        assert p.state == STATE_OK and c.state == STATE_WARN
        assert c.reason == 'not yet sent'


@pytest.mark.subprocess
class TestDescendantDiscoveryFailure:
    def test_a_failed_listing_is_one_warn_entry_that_survives_evaluate(self, check_zfs):
        check_zfs.tree('pool/bad', 'pool/bad/c', 'pool/good', 'pool/good/c1', 'pool/plain')
        check_zfs.fail('fail_names', 'pool/bad')
        cfg = config(parent_ds('pool/bad', '1h', remote('a')), parent_ds('pool/good', '1h', remote('a')),
                     ds('pool/plain', '1h', remote('a')))
        collected = collect(cfg)
        (entry,) = [o for o in collected if isinstance(o, CheckEntry)]
        assert (entry.source, entry.subject, entry.state) == ('descendants', 'pool/bad', STATE_WARN)
        assert entry.reason.startswith('the descendants of pool/bad could not be listed: ')
        assert 'pool I/O is currently suspended' in entry.reason
        assert entry.reason.endswith('; their copies are not checked')
        graded_ = evaluate(collected, NOW)
        assert [e for e in graded_ if e.source == 'descendants'] == [entry]
        assert [e for e in graded_ if e is entry][0].state == STATE_WARN
        assert overall_state(graded_) == STATE_WARN

    def test_the_other_parents_children_and_the_configured_datasets_are_still_observed(self, check_zfs):
        check_zfs.tree('pool/bad', 'pool/bad/c', 'pool/good', 'pool/good/c1', 'pool/plain')
        check_zfs.fail('fail_names', 'pool/bad')
        cfg = config(parent_ds('pool/bad', '1h', remote('a')), parent_ds('pool/good', '1h', remote('a')),
                     ds('pool/plain', '1h', remote('a')))
        assert keys(collect(cfg)) == [
            ('snapshot', 'pool/bad', None), ('anchor', 'pool/bad', 'a'),      # the parent itself is unaffected
            ('snapshot', 'pool/good', None), ('anchor', 'pool/good', 'a'),
            ('snapshot', 'pool/plain', None), ('anchor', 'pool/plain', 'a'),
            ('anchor', 'pool/good/c1', 'a'),
            ('descendants', 'pool/bad', None),
        ]

    def test_a_failed_marker_read_is_a_failed_expansion_too(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        check_zfs.fail('fail_markers', RECURSIVE)
        collected = collect(config(parent_ds()))
        (entry,) = [o for o in collected if isinstance(o, CheckEntry)]
        assert entry.state == STATE_WARN and 'backup-copy markers' in entry.reason
        assert not [o for o in collected if getattr(o, 'subject', '') == f'{RECURSIVE}/x']   # never expanded without it

    def test_every_failed_parent_gets_its_own_entry(self, check_zfs):
        check_zfs.tree('pool/a', 'pool/b')
        check_zfs.fail('fail_names', 'pool/a', 'pool/b')
        collected = collect(config(parent_ds('pool/a', '1h', remote('x')), parent_ds('pool/b', '1h', remote('x'))))
        assert [(e.source, e.subject) for e in collected if isinstance(e, CheckEntry)] == [
            ('descendants', 'pool/a'), ('descendants', 'pool/b')]

    def test_it_is_a_warn_never_a_crit_and_never_silence(self, check_zfs):
        check_zfs.tree(RECURSIVE)
        check_zfs.fail('fail_names', RECURSIVE)
        entries = graded(config(parent_ds()), check_zfs)
        assert [e.state for e in entries if e.source == 'descendants'] == [STATE_WARN]

    def test_expand_recursive_raising_is_one_recursive_datasets_entry(self, mocker, check_zfs):
        check_zfs.tree('pool/plain')
        cfg = config(ds('pool/plain', '1h', remote('a')), parent_ds())
        manager = DatasetManager(cfg)
        mocker.patch.object(manager, 'expand_recursive', side_effect=RuntimeError('boom'))
        collected = collect(cfg, manager=manager)
        (entry,) = [o for o in collected if isinstance(o, CheckEntry)]
        assert (entry.source, entry.subject, entry.state) == ('descendants', '(recursive datasets)', STATE_WARN)
        assert entry.reason == 'the descendants of the recursive datasets could not be listed: boom'
        assert ('snapshot', 'pool/plain', None) in keys(collected)          # the rest is still observed
        assert evaluate(collected, NOW)[-1] is entry

    def test_an_exception_without_a_message_still_names_itself(self, mocker, check_zfs):
        cfg = config(parent_ds())
        manager = DatasetManager(cfg)
        mocker.patch.object(manager, 'expand_recursive', side_effect=KeyError())
        (entry,) = [o for o in collect(cfg, manager=manager) if isinstance(o, CheckEntry)]
        assert entry.reason.endswith(': KeyError')

    def test_the_received_observations_still_follow_a_failed_expansion(self, check_zfs):
        check_zfs.tree('pool/backups', 'pool/backups/c', RECURSIVE)
        check_zfs.tree('pool/backups/c/x', snapshots=False)
        check_zfs.snaps('pool/backups/c/x', 60)
        check_zfs.prop('pool/backups/c/x', PROP_CLIENT_ID, 'c')
        check_zfs.prop('pool/backups/c/x', PROP_CONFIG, client_ds().to_property())
        check_zfs.fail('fail_names', RECURSIVE)
        cfg = server_config(parent_ds(RECURSIVE, '1h', remote('a')))
        collected = collect(cfg)
        assert [(getattr(o, 'source'), o.subject) for o in collected] == [
            ('snapshot', RECURSIVE), ('anchor', RECURSIVE), ('descendants', RECURSIVE),
            ('received', 'pool/backups/c/x')]


@pytest.mark.subprocess
class TestBackupCopiesAreNotChildren:
    def setup_tree(self, fake):
        fake.tree(RECURSIVE, f'{RECURSIVE}/ok', f'{RECURSIVE}/copy', f'{RECURSIVE}/copy/c1',
                  f'{RECURSIVE}/copyx')

    def subjects(self, cfg):
        return [o.subject for o in collect(cfg) if o.source == 'anchor' and o.subject != RECURSIVE]

    @pytest.mark.parametrize('source', ['local', 'received'])
    @pytest.mark.parametrize('key', [PROP_CLIENT_ID, 'org.zfsbackup:source_dataset'])
    def test_a_marker_on_a_descendant_removes_it_and_its_subtree(self, check_zfs, key, source):
        self.setup_tree(check_zfs)
        check_zfs.marker(f'{RECURSIVE}/copy', key, source)
        assert self.subjects(config(parent_ds(RECURSIVE, '1h', remote('a')))) == [
            f'{RECURSIVE}/ok', f'{RECURSIVE}/copyx']

    def test_an_inherited_marker_makes_each_descendant_a_copy(self, check_zfs):
        """A marker on an ancestor ABOVE the recursive parent is inherited by the parent and everything
        below it: the parent is inside a copy and no child is observed."""
        check_zfs.tree('pool', RECURSIVE, f'{RECURSIVE}/a', f'{RECURSIVE}/b')
        check_zfs.marker('pool')
        assert self.subjects(config(parent_ds(RECURSIVE, '1h', remote('a')))) == []

    def test_an_inherited_marker_below_the_parent_hides_the_subtree_but_not_its_siblings(self, check_zfs):
        self.setup_tree(check_zfs)
        check_zfs.marker(f'{RECURSIVE}/copy')                 # `copy/c1` inherits it
        assert f'{RECURSIVE}/copy/c1' not in self.subjects(config(parent_ds(RECURSIVE, '1h', remote('a'))))

    def test_a_copy_costs_no_snapshot_listing(self, check_zfs):
        self.setup_tree(check_zfs)
        check_zfs.marker(f'{RECURSIVE}/copy', PROP_CLIENT_ID, 'received')
        collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        # one listing for the whole tree; the copy never gets a call of its own, and no child does either
        assert {c[-1] for c in check_zfs.calls_of('list') if 'snapshot' in c} == {RECURSIVE}
        assert not [c for c in check_zfs.calls if c[-1].startswith(f'{RECURSIVE}/copy/') or c[-1] == f'{RECURSIVE}/copy']
        # ... and nothing that is under the copy is observed
        assert {o.subject for o in collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
                if o.source == 'anchor'} == {RECURSIVE, f'{RECURSIVE}/ok', f'{RECURSIVE}/copyx'}

    def test_a_local_destination_target_below_the_parent_is_no_source(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/ok', f'{RECURSIVE}/copies', f'{RECURSIVE}/copies/client/x')
        cfg = config(parent_ds(RECURSIVE, '1h', remote('loc')),
                     destinations={'loc': Destination(url=f'zfs:{RECURSIVE}/copies')})
        assert self.subjects(cfg) == [f'{RECURSIVE}/ok']

    @pytest.mark.parametrize('enabled', [True, False])
    def test_the_server_target_below_the_parent_is_no_source_enabled_or_not(self, check_zfs, enabled):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/ok', f'{RECURSIVE}/srv', f'{RECURSIVE}/srv/c/x')
        cfg = server_config(parent_ds(RECURSIVE, '1h', remote('a')), enabled=enabled, target=f'{RECURSIVE}/srv')
        subjects = [o.subject for o in collect(cfg) if o.source == 'anchor' and o.subject != RECURSIVE]
        assert subjects == [f'{RECURSIVE}/ok']

    def test_the_target_itself_is_excluded_with_a_trailing_slash(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/ok', f'{RECURSIVE}/copies')
        cfg = config(parent_ds(RECURSIVE, '1h', remote('loc')),
                     destinations={'loc': Destination(url=f'zfs:{RECURSIVE}/copies/')})
        assert self.subjects(cfg) == [f'{RECURSIVE}/ok']

    def test_a_target_lookalike_is_a_source(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/copiesx')
        cfg = config(parent_ds(RECURSIVE, '1h', remote('loc')),
                     destinations={'loc': Destination(url=f'zfs:{RECURSIVE}/copies')})
        assert self.subjects(cfg) == [f'{RECURSIVE}/copiesx']


@pytest.mark.subprocess
class TestUnreadableAnchorOrPin:
    """A configured dataset reads its anchor and pin one value at a time (`get_local_anchor` /
    `get_local_inflight`, the RAISING readers). An unreadable ANCHOR is a CRIT error; an unreadable
    PIN no longer discards a readable anchor: the anchor is graded normally and the detail says so."""

    def test_an_unreadable_anchor_is_an_error_for_that_destination_only(self, check_zfs):
        check_zfs.tree('pool/plain')
        old, new = check_zfs.snaps('pool/plain', 2 * HOUR, 600)
        check_zfs.prop('pool/plain', ANCHOR_B, new)
        check_zfs.fail('fail_get', f'pool/plain::{ANCHOR_A}')
        collected = collect(config(ds('pool/plain', '1h', remote('a'), remote('b', '6h'))))
        snapshot, a, b = collected
        assert snapshot.source == 'snapshot' and snapshot.error is None        # its snapshot observation is intact
        assert (a.peer, b.peer) == ('a', 'b')
        assert a.error.startswith('the anchor could not be read: ')
        assert 'pool I/O is currently suspended' in a.error
        assert a.newest_ts is None and a.oldest_ts is None and a.detail is None
        assert a.expected_seconds == HOUR and b.expected_seconds == 6 * HOUR
        assert b.error is None and b.newest_ts == epoch_of(new)
        ea, eb = evaluate(collected, NOW)[1:]
        assert ea.state == STATE_CRIT and ea.reason.startswith('cannot read: the anchor could not be read')
        assert eb.state == STATE_OK

    def test_an_unreadable_pin_does_not_discard_a_readable_anchor(self, check_zfs):
        check_zfs.tree('pool/plain')
        old, new = check_zfs.snaps('pool/plain', 2 * HOUR, 600)
        check_zfs.prop('pool/plain', ANCHOR_B, new)
        check_zfs.fail('fail_get', f'pool/plain::{INFLIGHT_B}')
        collected = collect(config(ds('pool/plain', '1h', remote('a'), remote('b'))))
        _, a, b = collected
        assert b.error is None                                  # not an error observation
        assert b.newest_ts == epoch_of(new)                     # the readable anchor is graded
        assert b.detail.startswith(new) and 'send pin could not be read' in b.detail
        assert 'pool I/O is currently suspended' in b.detail
        assert a.error is None and a.detail is None             # the other destination is untouched
        states = [e.state for e in evaluate(collected, NOW)]
        assert states == [STATE_OK, STATE_WARN, STATE_OK]       # `a` has no anchor yet: WARN, `b` is fresh: OK

    def test_an_unreadable_pin_with_no_anchor_is_still_not_yet_sent_with_the_note(self, check_zfs):
        check_zfs.tree('pool/plain')
        check_zfs.fail('fail_get', f'pool/plain::{INFLIGHT_B}')
        _, _a, b = collect(config(ds('pool/plain', '1h', remote('a'), remote('b'))))
        assert b.error is None and b.newest_ts is None
        assert b.detail.startswith('send pin could not be read: ')

    def test_the_pin_note_follows_a_pin_that_is_readable_and_set(self, check_zfs):
        """A readable pin and an unreadable one for different destinations: each note on its own."""
        check_zfs.tree('pool/plain')
        old, new = check_zfs.snaps('pool/plain', 2 * HOUR, 600)
        check_zfs.prop('pool/plain', ANCHOR_A, old)
        check_zfs.prop('pool/plain', PROP_INFLIGHT_PREFIX + 'a', f'{old},{new}')
        check_zfs.prop('pool/plain', ANCHOR_B, old)
        check_zfs.fail('fail_get', f'pool/plain::{INFLIGHT_B}')
        _, a, b = collect(config(ds('pool/plain', '1h', remote('a'), remote('b'))))
        assert 'send in flight' in a.detail and 'could not be read' not in a.detail
        assert 'send pin could not be read' in b.detail and 'send in flight' not in b.detail

    def test_an_unreadable_dataset_makes_every_destination_an_error_but_not_the_other_datasets(self, check_zfs):
        check_zfs.tree('pool/one', 'pool/two')
        check_zfs.fail('fail_get', 'pool/one')
        collected = collect(config(ds('pool/one', '1h', remote('a'), remote('b')),
                                   ds('pool/two', '1h', remote('a'), remote('b'))))
        assert [(o.subject, o.peer, bool(o.error)) for o in collected if o.source == 'anchor'] == [
            ('pool/one', 'a', True), ('pool/one', 'b', True),
            ('pool/two', 'a', False), ('pool/two', 'b', False)]
        assert all(o.error.startswith('the anchor could not be read') for o in collected
                   if o.source == 'anchor' and o.subject == 'pool/one')

    def test_the_unreadable_anchor_is_not_never_sent(self, check_zfs):
        check_zfs.tree('pool/plain')
        cfg = config(ds('pool/plain', '1h', remote('a')))
        (_, never) = graded(cfg, check_zfs)
        check_zfs.fail('fail_get', 'pool/plain')
        (_, unread) = graded(cfg, check_zfs)
        assert never.state == STATE_WARN and never.reason == 'not yet sent'
        assert unread.state == STATE_CRIT and unread.reason != never.reason

    def test_the_other_datasets_error_does_not_stop_the_dataset_loop(self, check_zfs):
        check_zfs.tree('pool/one', 'pool/two')
        check_zfs.fail('fail_get', 'pool/one')
        collected = collect(config(ds('pool/one', '1h', remote('a')), ds('pool/two', '1h', remote('a'))))
        assert [(o.source, o.subject, bool(o.error)) for o in collected] == [
            ('snapshot', 'pool/one', False), ('anchor', 'pool/one', True),
            ('snapshot', 'pool/two', False), ('anchor', 'pool/two', False)]

    def test_the_pin_note_survives_the_grade_and_the_report(self, check_zfs):
        check_zfs.tree('pool/plain')
        old, new = check_zfs.snaps('pool/plain', 2 * HOUR, 600)
        check_zfs.prop('pool/plain', ANCHOR_A, new)
        check_zfs.fail('fail_get', f'pool/plain::{PROP_INFLIGHT_PREFIX}a')
        _, entry = graded(config(ds('pool/plain', '1h', remote('a'))), check_zfs)
        assert entry.state == STATE_OK and 'send pin could not be read' in entry.reason
        assert 'send pin could not be read' in health.to_json([entry], NOW)


# ---------------------------------------------------------------------------------------------
# The descendants are read in BATCHES: O(parents), not O(children)
# ---------------------------------------------------------------------------------------------

def children_of(parent, n):
    return [f'{parent}/c{i:03d}' for i in range(n)]


def give_every_child_its_anchor(fake, names, prop=ANCHOR_A):
    for name in names:
        fake.prop(name, prop, fake.spec['snapshots'][name][0][0])


@pytest.mark.subprocess
class TestBatchedReads:
    def run(self, check_zfs, n, anchors, parents=1):
        cfgs = []
        for p in range(parents):
            parent = f'pool/p{p}' if parents > 1 else RECURSIVE
            names = children_of(parent, n)
            check_zfs.tree(parent, *names)
            if anchors:
                give_every_child_its_anchor(check_zfs, names)
            cfgs.append(parent_ds(parent, '1h', remote('a'), remote('b', '6h')))
        return collect(config(*cfgs))

    @pytest.mark.parametrize('anchors, total', [(False, 10), (True, 11)])
    def test_the_number_of_zfs_calls_does_not_depend_on_the_number_of_children(self, check_zfs, anchors, total):
        counts = {}
        for n in (1, 5, 60):
            check_zfs.log_path.write_text('')
            check_zfs.spec['datasets'].clear()
            check_zfs.spec['snapshots'].clear()
            check_zfs.spec['props'].clear()
            check_zfs.save()
            self.run(check_zfs, n, anchors)
            counts[n] = len(check_zfs.calls)
        assert counts == {1: total, 5: total, 60: total}

    def test_fifty_children_and_two_destinations_are_four_execs_for_the_children(self, check_zfs):
        result = self.run(check_zfs, 55, anchors=True)
        child_obs = [o for o in result if o.source == 'anchor' and o.subject != RECURSIVE]
        assert len(child_obs) == 110 and all(o.error is None for o in child_obs)
        # no call names a child; the tree, the property reads and the re-listing name the PARENT
        assert {c[-1] for c in check_zfs.calls} == {RECURSIVE}
        assert len(check_zfs.calls) == 11
        # the parent itself is the only dataset read one value at a time (2 destinations x anchor + pin)
        assert len([c for c in check_zfs.calls_of('get') if c[3] == 'value']) == 4

    def test_the_cost_is_per_parent(self, check_zfs):
        self.run(check_zfs, 30, anchors=True, parents=3)
        # per parent: snapshot list + 4 value reads + names + markers + tree + 2 property phases + re-listing
        assert len(check_zfs.calls) == 3 * 11
        assert {c[-1] for c in check_zfs.calls} == {'pool/p0', 'pool/p1', 'pool/p2'}

    def test_the_tree_listing_is_one_strict_recursive_snapshot_listing(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        collect(config(parent_ds()))
        (tree,) = [c for c in check_zfs.calls_of('list') if '-r' in c and 'snapshot' in c]
        assert tree[tree.index('-t') + 1] == 'snapshot' and tree[-1] == RECURSIVE
        assert tree[tree.index('-o') + 1] == ','.join(['name', 'type'] + list(SNAPSHOT_LIST_PROPERTIES))

    def test_phase_two_names_exactly_what_phase_one_found(self, check_zfs):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(RECURSIVE, child)
        check_zfs.prop(child, ANCHOR_B, at(1800))
        check_zfs.prop(child, 'org.zfsbackup:config', 'not-a-value-we-read')
        collect(config(parent_ds()))
        (phase2,) = [c for c in check_zfs.calls_of('get') if 'name,property,value,source' in c]
        assert '-s' not in phase2 and phase2[-1] == RECURSIVE
        # narrowed to anchor.* / inflight.*: the unrelated `config` is never asked for
        assert phase2[-2] == f'name,type,{ANCHOR_B}'

    def test_an_unrelated_odd_value_below_cannot_break_the_batch(self, check_zfs):
        """The batch asks for `anchor.*`/`inflight.*` only: a newline in a child's `config` forges no row."""
        child, other = f'{RECURSIVE}/x', f'{RECURSIVE}/y'
        check_zfs.tree(RECURSIVE, child, other)
        check_zfs.prop(child, ANCHOR_A, at(1800))
        check_zfs.prop(other, 'org.zfsbackup:config', 'first line\n' + f'{other}\tname\t{other}\t-')
        result = collect(config(parent_ds()))
        assert [o.error for o in result if o.source == 'anchor'] == [None] * 6
        x_a = [o for o in result if o.subject == child and o.peer == 'a'][0]
        assert x_a.newest_ts == epoch_of(at(1800))

    def test_the_tree_snapshots_are_each_childs_own(self, check_zfs):
        x, y = f'{RECURSIVE}/x', f'{RECURSIVE}/y'
        check_zfs.tree(RECURSIVE, x, y, snapshots=False)
        ax = check_zfs.snaps(x, 5 * HOUR, 2 * HOUR)
        ay = check_zfs.snaps(y, 4 * HOUR, HOUR)
        check_zfs.snaps(RECURSIVE, 600)
        check_zfs.prop(x, ANCHOR_A, ax[1])
        check_zfs.prop(y, ANCHOR_A, ay[0])
        result = collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        ox = [o for o in result if o.subject == x][0]
        oy = [o for o in result if o.subject == y][0]
        assert (ox.newest_ts, ox.oldest_ts) == (epoch_of(ax[1]), epoch_of(ax[0]))
        assert (oy.newest_ts, oy.oldest_ts) == (epoch_of(ay[0]), epoch_of(ay[0]))
        assert 'not among' not in (ox.detail or '') and ox.error is None and oy.error is None

    def test_createtxg_decides_the_childs_snapshots_in_the_batch_too(self, check_zfs):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(RECURSIVE)
        check_zfs.tree(child, snapshots=False)
        newer_name, older_name = at(5 * HOUR), at(HOUR)
        check_zfs.spec['snapshots'][child] = [[newer_name, 30], [older_name, 10]]
        check_zfs.save()
        (a,) = [o for o in collect(config(parent_ds(RECURSIVE, '1h', remote('a')))) if o.subject == child]
        assert a.oldest_ts == epoch_of(older_name)               # txg 10 is the oldest, whatever the name says


@pytest.mark.subprocess
class TestOnlyLocalChildValuesCount:
    P = RECURSIVE

    def test_a_received_child_anchor_is_ignored(self, check_zfs):
        child = f'{self.P}/x'
        check_zfs.tree(self.P, child)
        check_zfs.prop(child, ANCHOR_A, at(1800))
        check_zfs.spec.setdefault('prop_sources', {}).setdefault(child, {})[ANCHOR_A] = 'received'
        check_zfs.save()
        (a,) = [o for o in collect(config(parent_ds(self.P, '1h', remote('a')))) if o.subject == child]
        assert a.error is None and a.newest_ts is None and a.detail is None
        (entry,) = [e for e in graded(config(parent_ds(self.P, '1h', remote('a'))), check_zfs) if e.subject == child]
        assert entry.reason == 'not yet sent'

    def test_a_received_pin_is_ignored_too(self, check_zfs):
        child = f'{self.P}/x'
        check_zfs.tree(self.P, child)
        check_zfs.prop(child, INFLIGHT_B, f'{at(1800)},{at(1800)}')
        check_zfs.spec.setdefault('prop_sources', {}).setdefault(child, {})[INFLIGHT_B] = 'received'
        check_zfs.save()
        b = [o for o in collect(config(parent_ds())) if o.subject == child and o.peer == 'b'][0]
        assert b.detail is None

    def test_an_inherited_child_anchor_is_ignored_while_a_sibling_has_a_local_one(self, check_zfs):
        """The sibling's local anchor makes phase 2 ask for the property on EVERY descendant, so the child
        without one prints `inherited from <parent>`: it must read as no anchor, not as the parent's."""
        sent, fresh = f'{self.P}/sent', f'{self.P}/fresh'
        check_zfs.tree(self.P, sent, fresh)
        check_zfs.prop(self.P, ANCHOR_A, at(1800))              # the parent's own anchor, inherited by both
        check_zfs.prop(sent, ANCHOR_A, at(1800))
        check_zfs.spec['inherit'] = [ANCHOR_A]
        check_zfs.save()
        result = collect(config(parent_ds(self.P, '1h', remote('a'))))
        assert [o.newest_ts for o in result if o.subject == sent] == [epoch_of(at(1800))]
        assert [o.newest_ts for o in result if o.subject == fresh] == [None]
        (phase2,) = [c for c in check_zfs.calls_of('get') if 'name,property,value,source' in c]
        assert phase2[-2] == f'name,type,{ANCHOR_A}'            # the read did happen

    def test_a_local_value_equal_to_the_parents_still_counts(self, check_zfs):
        child = f'{self.P}/x'
        check_zfs.tree(self.P, child)
        check_zfs.prop(self.P, ANCHOR_A, at(1800))
        check_zfs.prop(child, ANCHOR_A, at(1800))
        check_zfs.spec['inherit'] = [ANCHOR_A]
        check_zfs.save()
        (a,) = [o for o in collect(config(parent_ds(self.P, '1h', remote('a')))) if o.subject == child]
        assert a.newest_ts == epoch_of(at(1800))

    def test_a_received_value_beside_a_local_one_for_another_destination(self, check_zfs):
        child = f'{self.P}/x'
        check_zfs.tree(self.P, child)
        check_zfs.prop(child, ANCHOR_A, at(1800))
        check_zfs.prop(child, ANCHOR_B, at(1800))
        check_zfs.spec.setdefault('prop_sources', {}).setdefault(child, {})[ANCHOR_B] = 'received'
        check_zfs.save()
        a, b = [o for o in collect(config(parent_ds())) if o.subject == child]
        assert a.newest_ts == epoch_of(at(1800)) and b.newest_ts is None


@pytest.mark.subprocess
class TestBatchFailureMatrix:
    """The batch is the unit: a read that fails (or cannot be trusted) is the error of EVERY child of that
    parent, for every destination, never a smaller or wrong answer; another parent is untouched."""

    BAD, GOOD = 'pool/one', 'pool/two'

    def setup_world(self, fake):
        fake.tree(self.BAD, f'{self.BAD}/c1', f'{self.BAD}/c2', self.GOOD, f'{self.GOOD}/c3')
        for name in (f'{self.BAD}/c1', f'{self.BAD}/c2', f'{self.GOOD}/c3'):
            fake.prop(name, ANCHOR_A, fake.spec['snapshots'][name][0][0])     # phase 2 will run for both
        return config(parent_ds(self.BAD, '1h', remote('a'), remote('b', '6h')),
                      parent_ds(self.GOOD, '1h', remote('a'), remote('b', '6h')))

    FAILURES = {
        'listing': lambda fake, bad: fake.fail('fail_tree', bad),
        'untrusted-listing': lambda fake, bad: fake.fail('garbage_tree', bad),
        'phase-1': lambda fake, bad: fake.fail('fail_props1', bad),
        'phase-2': lambda fake, bad: fake.fail('fail_rows', bad),
        'phases-disagree': lambda fake, bad: fake.spec.setdefault('vanish', {}).update(
            {f'{bad}/c1': [ANCHOR_A]}) or fake.save(),
    }

    @pytest.fixture(params=sorted(FAILURES))
    def failed(self, request, check_zfs):
        cfg = self.setup_world(check_zfs)
        self.FAILURES[request.param](check_zfs, self.BAD)
        return request.param, cfg, collect(cfg)

    def test_every_child_of_that_parent_is_an_error_for_every_destination(self, failed):
        kind, _, result = failed
        bad = [o for o in result if o.subject.startswith(self.BAD + '/')]
        assert [(o.subject, o.peer) for o in bad] == [
            (f'{self.BAD}/c1', 'a'), (f'{self.BAD}/c1', 'b'), (f'{self.BAD}/c2', 'a'), (f'{self.BAD}/c2', 'b')]
        assert all(o.source == 'anchor' and o.error for o in bad)
        assert all(o.newest_ts is None and o.oldest_ts is None and o.detail is None for o in bad)
        assert [o.expected_seconds for o in bad] == [HOUR, 6 * HOUR] * 2

    def test_the_error_says_what_failed(self, failed):
        kind, _, result = failed
        errors = {o.error for o in result if o.subject.startswith(self.BAD + '/')}
        assert len(errors) == 1                                   # the same reason for every child
        (error,) = errors
        if kind in ('phase-1', 'phase-2', 'phases-disagree'):
            assert error.startswith('the anchor could not be read: ')
        if kind in ('listing', 'phase-1', 'phase-2'):
            assert 'pool I/O is currently suspended' in error
        if kind == 'untrusted-listing':
            assert 'could not be trusted' in error
        if kind == 'phases-disagree':
            assert 'changed while they were being read' in error

    def test_each_is_crit_cannot_read_never_not_yet_sent(self, failed):
        _, cfg, result = failed
        entries = [e for e in evaluate(result, NOW) if e.subject.startswith(self.BAD + '/')]
        assert [e.state for e in entries] == [STATE_CRIT] * 4
        assert all(e.reason.startswith('cannot read: ') for e in entries)

    def test_the_other_parent_and_the_parents_themselves_are_untouched(self, failed):
        _, cfg, result = failed
        good = [o for o in result if o.subject == f'{self.GOOD}/c3']
        assert [(o.peer, o.error) for o in good] == [('a', None), ('b', None)]
        assert good[0].newest_ts is not None                     # c3's anchor was read
        for parent in (self.BAD, self.GOOD):
            assert [(o.source, o.error) for o in result if o.subject == parent] == [
                ('snapshot', None), ('anchor', None), ('anchor', None)]

    def test_a_sibling_without_any_anchor_of_its_own_is_an_error_too(self, check_zfs):
        """c2 has no anchor property: it is still unreadable, because the batch that would have said so failed."""
        check_zfs.tree(self.BAD, f'{self.BAD}/c1', f'{self.BAD}/c2')
        check_zfs.prop(f'{self.BAD}/c1', ANCHOR_A, at(1800))
        check_zfs.fail('fail_rows', self.BAD)
        result = collect(config(parent_ds(self.BAD, '1h', remote('a'))))
        c2 = [o for o in result if o.subject == f'{self.BAD}/c2']
        assert [bool(o.error) for o in c2] == [True]

    def test_a_failing_phase_one_makes_no_phase_two_read(self, check_zfs):
        self.setup_world(check_zfs)
        check_zfs.fail('fail_props1', self.BAD)
        collect(config(parent_ds(self.BAD, '1h', remote('a')), parent_ds(self.GOOD, '1h', remote('a'))))
        phase2 = [c for c in check_zfs.calls_of('get') if 'name,property,value,source' in c]
        assert [c[-1] for c in phase2] == [self.GOOD]

    def test_a_disagreement_that_does_not_persist_is_retried_once_and_trusted(self, check_zfs):
        """An anchor cleared between the two phases: the pair is read again and the second read agrees."""
        self.setup_world(check_zfs)
        check_zfs.spec['vanish_once'] = {f'{self.BAD}/c1': [ANCHOR_A]}
        check_zfs.save()
        result = collect(config(parent_ds(self.BAD, '1h', remote('a'))))
        assert [o.error for o in result if o.subject.startswith(self.BAD + '/')] == [None, None]
        phase1 = [c for c in check_zfs.calls_of('get') if c[-2] == 'all']
        assert len(phase1) == 2                                   # read twice, then agreed

    def test_when_both_batches_fail_the_listings_error_is_the_one_reported(self, check_zfs):
        """The two batches are independent: each failure is reported once, as the one error of the child."""
        self.setup_world(check_zfs)
        check_zfs.fail('fail_tree', self.BAD)
        check_zfs.fail('fail_rows', self.BAD)
        result = collect(config(parent_ds(self.BAD, '1h', remote('a'))))
        errors = {o.error for o in result if o.subject.startswith(self.BAD + '/')}
        assert len(errors) == 1 and 'could not be read' not in next(iter(errors))     # the listing's error wins

    def test_a_child_gone_from_the_re_listing_is_dropped_even_when_the_batch_failed(self, check_zfs):
        self.setup_world(check_zfs)
        check_zfs.fail('fail_tree', self.BAD)
        check_zfs.spec['gone_after_first'] = {self.BAD: [f'{self.BAD}/c2']}
        check_zfs.save()
        result = collect(config(parent_ds(self.BAD, '1h', remote('a'))))
        assert [o.subject for o in result if o.subject.startswith(self.BAD + '/')] == [f'{self.BAD}/c1']


@pytest.mark.subprocess
class TestReListingAfterTheExpansion:
    P = RECURSIVE

    def test_a_child_gone_since_the_expansion_is_dropped(self, check_zfs):
        x, y, z = f'{self.P}/x', f'{self.P}/y', f'{self.P}/z'
        check_zfs.tree(self.P, x, y, z)
        check_zfs.prop(y, ANCHOR_A, at(1800))                    # it has an anchor and snapshots in the batch
        check_zfs.spec['gone_after_first'] = {self.P: [y]}
        check_zfs.save()
        result = collect(config(parent_ds(self.P, '1h', remote('a'))))
        assert [o.subject for o in result if o.source == 'anchor' and o.subject != self.P] == [x, z]
        assert [c for c in check_zfs.calls_of('list') if '-r' in c and 'snapshot' not in c] == [
            ['list', '-H', '-r', '-t', 'filesystem,volume', '-o', 'name,type', self.P]] * 2

    def test_every_child_gone_leaves_only_the_parent(self, check_zfs):
        x = f'{self.P}/x'
        check_zfs.tree(self.P, x)
        check_zfs.spec['gone_after_first'] = {self.P: [x]}
        check_zfs.save()
        assert keys(collect(config(parent_ds(self.P, '1h', remote('a'))))) == [
            ('snapshot', self.P, None), ('anchor', self.P, 'a')]

    def test_a_failed_re_listing_keeps_the_children_as_expanded(self, check_zfs):
        x, y = f'{self.P}/x', f'{self.P}/y'
        check_zfs.tree(self.P, x, y)
        check_zfs.prop(x, ANCHOR_A, at(1800))
        check_zfs.fail('fail_names_again', self.P)
        result = collect(config(parent_ds(self.P, '1h', remote('a'))))
        kids = [o for o in result if o.source == 'anchor' and o.subject != self.P]
        assert [(o.subject, o.error) for o in kids] == [(x, None), (y, None)]
        assert kids[0].newest_ts == epoch_of(at(1800))
        assert not [o for o in result if o.source == 'descendants']      # no entry: the reads stand on their own

    def test_a_vanished_child_is_not_a_cannot_read(self, check_zfs):
        x = f'{self.P}/x'
        check_zfs.tree(self.P, x)
        check_zfs.spec['gone_after_first'] = {self.P: [x]}
        check_zfs.save()
        assert not any(getattr(e, 'state', None) == STATE_CRIT and e.subject == x
                       for e in graded(config(parent_ds(self.P, '1h', remote('a'))), check_zfs))


# ---------------------------------------------------------------------------------------------
# The time bound: `deadline` and `per_call`
# ---------------------------------------------------------------------------------------------

@pytest.fixture
def clock(mocker):
    """The collector's monotonic clock, frozen at 0; tests advance it from inside a faked `zfs` call."""
    fake = FakeTime()
    fake.frozen = 0.0
    mocker.patch.object(health_collect, 'time', fake)
    return fake


def limit_entries(items):
    return [e for e in items if isinstance(e, CheckEntry) and e.source == 'check']


def names_cfg(n, remotes=False):
    return config(*[ds(f'pool/d{i}', '1h', *([remote('x')] if remotes else [])) for i in range(n)])


class TestTimeLimitArguments:
    @pytest.mark.parametrize('bad', [0, -1, -0.5, float('nan'), float('inf'), -float('inf'), True, False,
                                     '10', [], object()])
    @pytest.mark.parametrize('arg', ['deadline', 'per_call'])
    def test_a_bad_value_is_a_value_error_before_a_command_runs(self, world, arg, bad):
        with pytest.raises(ValueError, match=arg):
            collect(config(ds()), **{arg: bad})
        assert world.calls == []

    def test_validation_comes_before_even_the_dry_run_entry(self, world):
        with pytest.raises(ValueError):
            collect(config(ds(), dry_run=True), deadline=0)

    @pytest.mark.parametrize('arg', ['deadline', 'per_call'])
    @pytest.mark.parametrize('ok', [None, 1, 0.001, 60, 600.5])
    def test_valid_values_run(self, world, arg, ok):
        world.snaps['pool/data'] = [at(60)]
        (o,) = collect(config(ds()), **{arg: ok})
        assert o.error is None

    def test_both_are_keyword_only(self, world):
        with pytest.raises(TypeError):
            collect(config(ds()), None, 60.0)

    def test_the_defaults(self):
        assert health_collect.DEFAULT_PER_CALL_SECONDS == 10.0
        import inspect
        sig = inspect.signature(collect)
        assert sig.parameters['deadline'].default is None
        assert sig.parameters['per_call'].default == health_collect.DEFAULT_PER_CALL_SECONDS
        assert sig.parameters['deadline'].kind is sig.parameters['per_call'].kind is inspect.Parameter.KEYWORD_ONLY

    def test_the_names_are_exported(self):
        for name in ('SOURCE_CHECK', 'DEFAULT_PER_CALL_SECONDS', 'SOURCE_COPIES', 'COPIES_SHOWN_PER_PARENT'):
            assert name in health_collect.__all__
        assert health_collect.SOURCE_CHECK == 'check' and health_collect.SOURCE_COPIES == 'copies'
        assert health_collect.COPIES_SHOWN_PER_PARENT == 20


class TestEveryReadIsTimed:
    @pytest.fixture
    def timeouts(self, mocker):
        import libzfseasy
        return mocker.patch.object(health_collect.zfs, 'timeout', wraps=libzfseasy.timeout)

    def seen(self, timeouts):
        return [c.args[0] for c in timeouts.call_args_list]

    def test_the_default_per_call_bounds_every_command(self, world, timeouts):
        world.snaps['pool/data'] = [at(60)]
        collect(config(ds('pool/data', '1h', remote('a'))))
        seen = self.seen(timeouts)
        assert len(seen) == len(world.calls) == 3 and set(seen) == {10.0}

    def test_the_per_call_value_is_the_limit(self, world, timeouts):
        collect(config(ds('pool/data', '1h', remote('a'))), per_call=2.5)
        assert set(self.seen(timeouts)) == {2.5}

    def test_the_time_left_caps_it(self, world, clock, timeouts):
        collect(config(ds('pool/data', '1h', remote('a'))), deadline=3, per_call=7)
        assert set(self.seen(timeouts)) == {3}

    def test_the_time_left_shrinks_as_the_clock_runs(self, world, clock, timeouts):
        world.hook = lambda rest: clock.advance(1.0)
        collect(config(ds('pool/data', '1h', remote('a'))), deadline=100, per_call=50)
        seen = self.seen(timeouts)
        assert seen[0] == 50 and seen == sorted(seen, reverse=True) and seen[-1] <= 50
        assert min(seen) == 50                                    # never anywhere near the deadline here

    def test_no_limits_at_all_means_no_timeout_value(self, world, timeouts):
        collect(config(ds('pool/data', '1h', remote('a'))), per_call=None)
        assert set(self.seen(timeouts)) == {None}

    def test_the_target_and_the_received_discovery_are_timed_too(self, world, timeouts):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        collect(server_config(), per_call=4)
        assert len(self.seen(timeouts)) == len(world.calls) and set(self.seen(timeouts)) == {4}
        assert len(world.calls) >= 4

    @pytest.mark.subprocess
    def test_the_recursive_discovery_and_the_batches_are_timed(self, check_zfs, timeouts):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        check_zfs.prop(f'{RECURSIVE}/x', ANCHOR_A, at(1800))
        collect(config(parent_ds()), per_call=6)
        # every COMMAND enters a scope of its own (the two property phases each have one), bulk or not
        assert len(self.seen(timeouts)) == len(check_zfs.calls) == 11
        assert set(self.seen(timeouts)) == {6}


class TestTimedOutReadsAreCrit:
    def test_a_timed_out_listing_is_cannot_read_not_a_warning(self, mocker, world):
        manager = DatasetManager(config(ds('pool/data', '1h', remote('a'))))
        mocker.patch.object(manager, '_list_snapshots_strict',
                            side_effect=ZFSTimeoutError('zfs list timed out after 10 s'))
        collected = collect(config(ds('pool/data', '1h', remote('a'))), manager=manager)
        assert [o.error for o in collected] == ['zfs list timed out after 10 s'] * 2
        assert [e.state for e in evaluate(collected, NOW)] == [STATE_CRIT, STATE_CRIT]
        assert not limit_entries(collected)                       # a command's timeout is not the deadline

    def test_a_timed_out_anchor_read_is_the_anchor_error(self, mocker, world):
        world.snaps['pool/data'] = [at(60)]
        cfg = config(ds('pool/data', '1h', remote('a')))
        manager = DatasetManager(cfg)
        mocker.patch.object(health_collect.zfs.get, 'value',
                            side_effect=ZFSTimeoutError('zfs get timed out after 10 s'))
        _, anchor = collect(cfg, manager=manager)
        assert anchor.error == 'the anchor could not be read: zfs get timed out after 10 s'

    def test_a_received_discovery_timeout_is_a_crit_on_the_target(self, mocker, world):
        manager = DatasetManager(server_config())
        mocker.patch.object(manager, 'received_datasets', side_effect=ZFSTimeoutError('timed out'))
        (o,) = collect(server_config(), manager=manager)
        assert (o.source, o.subject, o.error) == ('received', TARGET, 'timed out')
        assert evaluate([o], NOW)[0].state == STATE_CRIT

    @pytest.mark.subprocess
    def test_a_hung_zfs_is_killed_at_per_call_and_the_rest_is_still_read(self, check_zfs):
        check_zfs.tree('pool/hung', 'pool/fine')
        check_zfs.spec['slow'] = {'list:pool/hung': 60}
        check_zfs.save()
        collected = collect(config(ds('pool/hung'), ds('pool/fine')), per_call=2.5)
        hung, fine = collected
        assert (hung.subject, fine.subject) == ('pool/hung', 'pool/fine')
        assert hung.error and 'timed out' in hung.error
        assert fine.error is None and fine.newest_ts is not None
        assert evaluate(collected, NOW)[0].state == STATE_CRIT
        assert not limit_entries(collected)


class TestReceivedDiscoveryFailure:
    """`received_datasets()` logs its own failure and returns []; the collector reads `last_received_error`."""

    def test_a_failed_enumeration_is_a_crit_on_the_target(self, world):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        world.enum_fails.add(TARGET)
        (o,) = collect(server_config())
        assert (o.source, o.subject, o.peer) == ('received', TARGET, None)
        assert o.error and 'suspended' in o.error
        (entry,) = evaluate([o], NOW)
        assert entry.state == STATE_CRIT and entry.reason.startswith('cannot read: ')

    def test_it_is_not_confused_with_an_empty_target(self, world):
        world.filesystems += [TARGET]
        assert collect(server_config()) == []
        world.enum_fails.add(TARGET)
        assert len(collect(server_config())) == 1

    def test_the_failure_text_is_the_managers(self, mocker, world):
        manager = DatasetManager(server_config())

        def failing(*args, **kwargs):
            manager.last_received_error = 'RuntimeError: the batch read broke'
            return []

        mocker.patch.object(manager, 'received_datasets', side_effect=failing)
        (o,) = collect(server_config(), manager=manager)
        assert o.error == 'RuntimeError: the batch read broke' and o.subject == TARGET

    def test_a_per_call_timeout_in_a_datasets_fallback_read_is_a_crit_on_the_target_not_a_skip(
            self, mocker, world):
        # the batch read fails, so every dataset is read one property at a time; one of those
        # reads times out (a per-call timeout, the deadline is not involved)
        for n in 'xy':
            add_received(world, f'{TARGET}/c/{n}', 'c', client_ds(f'pool/{n}'), [at(60)])
        world.get_fails.add(TARGET)
        mocker.patch.object(health_collect.zfs.get, 'value',
                            side_effect=ZFSTimeoutError('zfs get timed out after 10 s'))
        result = collect(server_config())
        assert not limit_entries(result)
        (o,) = result
        assert (o.source, o.subject) == ('received', TARGET)
        assert o.error and 'timed out after 10 s' in o.error
        (entry,) = evaluate([o], NOW)
        assert entry.state == STATE_CRIT and entry.reason.startswith('cannot read: ')

    def test_the_local_datasets_are_still_observed(self, world):
        world.snaps['pool/data'] = [at(60)]
        world.enum_fails.add(TARGET)
        local, received = collect(server_config(ds()))
        assert local.error is None and received.error

    def test_the_discovery_is_called_with_the_budgets_scope(self, mocker, world):
        manager = DatasetManager(server_config())
        spy = mocker.spy(manager, 'received_datasets')
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        collect(server_config(), manager=manager)
        assert callable(spy.call_args.kwargs['scope'])


class TestDeadline:
    def test_a_deadline_that_is_not_reached_adds_nothing(self, world, clock):
        world.snaps['pool/d0'] = [at(60)]
        result = collect(names_cfg(3), deadline=60)
        assert len(result) == 3 and not limit_entries(result)

    def test_no_deadline_means_no_entry_however_long_it_takes(self, world, clock):
        world.hook = lambda rest: clock.advance(10_000)
        result = collect(names_cfg(4))
        assert len(result) == 4 and not limit_entries(result)

    def test_the_deadline_before_a_unit_names_it_and_the_rest(self, world, clock):
        world.hook = lambda rest: clock.advance(20) if rest[-1] == 'pool/d1' else None
        result = collect(names_cfg(5), deadline=10)
        assert [o.subject for o in result if isinstance(o, Observation)] == ['pool/d0', 'pool/d1']
        (entry,) = limit_entries(result)
        assert entry is result[-1]                                  # last in the result
        assert (entry.source, entry.subject, entry.state) == ('check', '(time limit)', STATE_WARN)
        assert entry.reason == 'not checked within the time limit (10 s): pool/d2, pool/d3, pool/d4'
        assert 'pool/d0' not in entry.reason and 'pool/d1' not in entry.reason

    @pytest.mark.parametrize('count, tail', [(6, ''), (7, ' and 1 more'), (12, ' and 6 more')])
    def test_the_entry_names_five_and_counts_the_rest(self, world, clock, count, tail):
        world.hook = lambda rest: clock.advance(20) if rest[-1] == 'pool/d0' else None
        result = collect(names_cfg(count), deadline=10)
        (entry,) = limit_entries(result)
        shown = ', '.join(f'pool/d{i}' for i in range(1, 6))
        assert entry.reason == f'not checked within the time limit (10 s): {shown}{tail}'

    def test_a_deadline_with_a_fraction_prints_as_given(self, world, clock):
        world.hook = lambda rest: clock.advance(20)
        (entry,) = limit_entries(collect(names_cfg(3), deadline=0.5))
        assert '(0.5 s)' in entry.reason

    def test_a_deadline_already_spent_reads_nothing(self, mocker, world, clock):
        cfg = names_cfg(3, remotes=True)
        manager = DatasetManager(cfg)

        def spend():
            clock.advance(100)
            return timedelta(minutes=5)

        mocker.patch.object(manager, 'clock_tolerance', side_effect=lambda: spend())
        result = collect(cfg, manager=manager, deadline=10)
        assert world.calls == []
        assert len(result) == 1 and result[0].source == 'check'
        assert result[0].reason == 'not checked within the time limit (10 s): pool/d0, pool/d1, pool/d2'

    def test_the_dry_run_entry_stays_first_and_the_limit_entry_last(self, world, clock):
        world.hook = lambda rest: clock.advance(20)
        result = collect(config(ds('pool/a'), ds('pool/b'), dry_run=True), deadline=10)
        assert [(getattr(r, 'source')) for r in result] == ['config', 'snapshot', 'check']

    def test_a_unit_cut_off_midway_contributes_nothing(self, world, clock):
        world.snaps['pool/a'] = [at(60)]
        world.snaps['pool/b'] = [at(60)]
        world.set_prop('pool/b', PROP_ANCHOR_PREFIX + 'x', at(60))
        # the clock runs out at pool/b's anchor read: its snapshot observation is already built, and dropped
        world.hook = lambda rest: clock.advance(20) if rest[-1] == 'pool/b' and rest[0] == 'get' else None
        result = collect(config(ds('pool/a', '1h', remote('x')), ds('pool/b', '1h', remote('x'))), deadline=10)
        assert [(o.source, o.subject) for o in result if isinstance(o, Observation)] == [
            ('snapshot', 'pool/a'), ('anchor', 'pool/a')]
        (entry,) = limit_entries(result)
        assert entry.reason == 'not checked within the time limit (10 s): pool/b'

    def test_a_failure_that_ends_at_the_deadline_is_the_limits_not_a_finding(self, world, clock):
        world.list_fails.add('pool/b')
        world.hook = lambda rest: clock.advance(20) if rest[-1] == 'pool/b' else None
        result = collect(config(ds('pool/a'), ds('pool/b')), deadline=10)
        assert [o.subject for o in result if isinstance(o, Observation)] == ['pool/a']
        assert not [o for o in result if isinstance(o, Observation) and o.error]
        assert limit_entries(result)[0].reason.endswith(': pool/b')

    def test_a_failure_before_the_deadline_is_still_a_crit(self, world, clock):
        world.list_fails.add('pool/b')
        result = collect(config(ds('pool/a'), ds('pool/b')), deadline=10)
        assert [bool(o.error) for o in result] == [False, True] and not limit_entries(result)

    def test_the_limit_entry_survives_evaluate_as_a_warn(self, world, clock):
        world.snaps['pool/d0'] = [at(60)]
        world.hook = lambda rest: clock.advance(20)
        entries = evaluate(collect(names_cfg(3), deadline=10), NOW)
        assert [e.state for e in entries] == [STATE_OK, STATE_WARN]
        assert entries[-1].source == 'check' and overall_state(entries) == STATE_WARN
        assert 'not checked within the time limit' in health.to_json(entries, NOW)

    def test_a_failure_at_the_deadline_is_the_limits_but_the_same_failure_earlier_is_a_crit(self, world, clock):
        world.list_fails.add('pool/d0')
        world.hook = lambda rest: clock.advance(20) if rest[-1] == 'pool/d0' else None
        # d0 fails AT the deadline: the limit's, so no CRIT; the same failure earlier is a CRIT
        entries = evaluate(collect(names_cfg(2), deadline=10), NOW)
        assert overall_state(entries) == STATE_WARN
        world.hook = None
        clock.frozen = 0.0
        entries = evaluate(collect(names_cfg(2), deadline=10), NOW)
        assert overall_state(entries) == STATE_CRIT

    def test_the_received_datasets_are_units_of_their_own(self, world, clock):
        for n in 'xyz':
            add_received(world, f'{TARGET}/c/{n}', 'c', client_ds(f'pool/{n}'), [at(60)])
        world.hook = lambda rest: clock.advance(20) if rest[0] == 'list' and rest[-1] == f'{TARGET}/c/x' else None
        result = collect(server_config(), deadline=10)
        assert [o.subject for o in result if isinstance(o, Observation)] == [f'{TARGET}/c/x']
        assert limit_entries(result)[0].reason == (
            f'not checked within the time limit (10 s): {TARGET}/c/y, {TARGET}/c/z')

    def test_an_enumeration_that_fails_at_the_deadline_is_named_whole_never_as_a_cannot_read(self, world, clock):
        for n in 'xy':
            add_received(world, f'{TARGET}/c/{n}', 'c', client_ds(f'pool/{n}'), [at(60)])
        world.enum_fails.add(TARGET)
        world.hook = lambda rest: (clock.advance(20)
                                   if rest[0] == 'list' and '-r' in rest and rest[-1] == TARGET else None)
        result = collect(server_config(), deadline=10)
        assert not [o for o in result if isinstance(o, Observation)]
        (entry,) = limit_entries(result)
        assert entry.reason == f'not checked within the time limit (10 s): {TARGET} (received datasets)'

    def test_a_discovery_that_runs_out_of_time_midway_is_never_silent(self, world, clock):
        for n in 'xy':
            add_received(world, f'{TARGET}/c/{n}', 'c', client_ds(f'pool/{n}'), [at(60)])
        world.hook = lambda rest: (clock.advance(20)
                                   if rest[0] == 'list' and '-r' in rest and rest[-1] == TARGET else None)
        result = collect(server_config(), deadline=10)
        assert [o for o in result if isinstance(o, Observation)] or limit_entries(result)

    def test_the_received_discovery_is_named_when_there_is_no_time_for_it(self, world, clock):
        world.snaps['pool/data'] = [at(60)]
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        world.hook = lambda rest: clock.advance(20) if rest[-1] == 'pool/data' else None
        result = collect(server_config(ds()), deadline=10)
        assert [o.source for o in result if isinstance(o, Observation)] == ['snapshot']
        assert limit_entries(result)[0].reason == (
            f'not checked within the time limit (10 s): {TARGET} (received datasets)')
        assert not [c for c in world.calls if TARGET in c[-1]]


@pytest.mark.subprocess
class TestDeadlineAndTheDescendants:
    def test_the_expansion_cut_off_names_the_parents_it_did_not_reach(self, mocker, check_zfs, clock):
        check_zfs.tree('pool/one', 'pool/one/c', 'pool/two', 'pool/two/c', 'pool/three', 'pool/three/c')
        real = DatasetManager._copy_roots
        seen = []

        def spend_after_the_first(parent, scope=None):
            seen.append(parent)
            result = real(parent, scope=scope)
            clock.advance(100)
            return result

        mocker.patch.object(DatasetManager, '_copy_roots', staticmethod(spend_after_the_first))
        cfg = config(*[parent_ds(p, '1h', remote('a')) for p in ('pool/one', 'pool/two', 'pool/three')])
        result = collect(cfg, deadline=60)
        # the configured datasets came first and were read; no child of anyone was
        assert [o.subject for o in result if isinstance(o, Observation)] == [
            'pool/one', 'pool/one', 'pool/two', 'pool/two', 'pool/three', 'pool/three']
        assert seen == ['pool/one']                              # the expansion stopped after the first parent
        (entry,) = limit_entries(result)
        prefix, _, names = entry.reason.partition(': ')
        assert prefix == 'not checked within the time limit (60 s)'
        assert set(names.split(', ')) == {'pool/one (descendants)', 'pool/two (descendants)',
                                          'pool/three (descendants)'}

    def test_a_parent_cut_off_midway_contributes_no_child(self, mocker, check_zfs, clock):
        check_zfs.tree('pool/one', 'pool/one/c1', 'pool/one/c2', 'pool/two', 'pool/two/c3')
        real = DatasetManager._list_snapshot_tree_strict

        def spend_on_pool_one(self_, ds_, **kw):
            result = real(self_, ds_, **kw)
            if ds_.name == 'pool/one':
                clock.advance(100)
            return result

        mocker.patch.object(DatasetManager, '_list_snapshot_tree_strict', spend_on_pool_one)
        cfg = config(parent_ds('pool/one', '1h', remote('a')), parent_ds('pool/two', '1h', remote('a')))
        result = collect(cfg, deadline=60)
        subjects = [o.subject for o in result if isinstance(o, Observation)]
        assert not [s for s in subjects if '/c' in s]            # not even the parent that finished its first reads
        (entry,) = limit_entries(result)
        assert 'pool/one (descendants)' in entry.reason and 'pool/two (descendants)' in entry.reason

    def test_a_parent_finished_before_the_deadline_keeps_every_child(self, mocker, check_zfs, clock):
        check_zfs.tree('pool/one', 'pool/one/c1', 'pool/one/c2', 'pool/two', 'pool/two/c3')
        real = DatasetManager._list_snapshot_tree_strict

        def spend_after_pool_two(self_, ds_, **kw):
            result = real(self_, ds_, **kw)
            if ds_.name == 'pool/two':
                clock.advance(100)
            return result

        mocker.patch.object(DatasetManager, '_list_snapshot_tree_strict', spend_after_pool_two)
        cfg = config(parent_ds('pool/one', '1h', remote('a')), parent_ds('pool/two', '1h', remote('a')))
        result = collect(cfg, deadline=60)
        subjects = [o.subject for o in result if isinstance(o, Observation) and '/c' in o.subject]
        assert subjects == ['pool/one/c1', 'pool/one/c2']
        (entry,) = limit_entries(result)
        assert entry.reason.endswith(': pool/two (descendants)')

    def test_a_timeout_in_the_expansion_is_that_parents_descendants_warning(self, check_zfs):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        check_zfs.spec['slow'] = {f'list:{RECURSIVE}': 60}
        check_zfs.save()
        # the snapshot listing of the parent itself is `list -t snapshot`, equally slow: it is the parent's CRIT,
        # the expansion's `list -r` is the descendants' WARN -- both under the per-call limit
        collected = collect(config(parent_ds()), per_call=1.5)
        assert [e for e in collected if isinstance(e, CheckEntry) and not isinstance(e, Observation)
                and e.source == 'descendants']
        assert any(isinstance(o, Observation) and o.error for o in collected)


# ---------------------------------------------------------------------------------------------
# Backup copies left out of a recursive parent: one WARN `copies` entry each
# ---------------------------------------------------------------------------------------------

def copies_entries(items):
    return [e for e in items if isinstance(e, CheckEntry) and not isinstance(e, Observation)
            and e.source == 'copies']


@pytest.mark.subprocess
class TestCopiesEntries:
    P = RECURSIVE
    PROPS = f'{PROP_CLIENT_ID} or org.zfsbackup:source_dataset'

    def cfg(self, **kw):
        return config(parent_ds(self.P, '1h', remote('a')), **kw)

    def test_a_local_copy_is_one_warn_entry_that_says_what_to_do(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/ok', f'{self.P}/copy', f'{self.P}/copy/c1')
        check_zfs.marker(f'{self.P}/copy', PROP_CLIENT_ID, 'local')
        (entry,) = copies_entries(collect(self.cfg()))
        assert (entry.source, entry.subject, entry.state) == ('copies', f'{self.P}/copy', STATE_WARN)
        assert entry.reason == (
            f'backup copy below the recursive dataset {self.P} (carries a local {self.PROPS}): it is not '
            f'snapshotted, sent or pruned with the tree; move it out '
            f'(`zfs rename {self.P}/copy <a name outside {self.P}>`)')

    def test_a_received_marker_says_received(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/copy')
        check_zfs.marker(f'{self.P}/copy', 'org.zfsbackup:source_dataset', 'received')
        (entry,) = copies_entries(collect(self.cfg()))
        assert f'(carries a received {self.PROPS})' in entry.reason

    def test_an_inherited_marker_names_where_it_comes_from(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/copy', f'{self.P}/copy/c1')
        check_zfs.marker(f'{self.P}/copy')
        (entry,) = copies_entries(collect(self.cfg()))
        assert entry.subject == f'{self.P}/copy'                  # topmost only: c1 gets no entry of its own
        assert 'carries a local' in entry.reason

    def test_a_receive_target_below_the_parent(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/ok', f'{self.P}/srv', f'{self.P}/srv/c/x')
        cfg = server_config(parent_ds(self.P, '1h', remote('a')), target=f'{self.P}/srv')
        (entry,) = copies_entries(collect(cfg))
        assert entry.subject == f'{self.P}/srv' and entry.state == STATE_WARN
        assert f'(a receive target, which holds copies)' in entry.reason
        assert entry.reason.startswith(f'backup copy below the recursive dataset {self.P} (')

    def test_a_local_destination_target_below_the_parent(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/copies', f'{self.P}/copies/client/x')
        cfg = config(parent_ds(self.P, '1h', remote('loc')),
                     destinations={'loc': Destination(url=f'zfs:{self.P}/copies')})
        (entry,) = copies_entries(collect(cfg))
        assert entry.subject == f'{self.P}/copies' and 'a receive target' in entry.reason

    def test_the_parent_inside_a_copy_is_one_entry_about_the_parent(self, check_zfs):
        check_zfs.tree('pool', self.P, f'{self.P}/a', f'{self.P}/b')
        check_zfs.marker('pool')
        (entry,) = copies_entries(collect(self.cfg()))
        assert entry.subject == self.P and entry.state == STATE_WARN
        assert entry.reason.startswith(f'the recursive dataset {self.P} is itself inside a backup copy or '
                                       f'receive target ({self.P}: inherits {self.PROPS} from pool)')
        assert f'move {self.P} out' in entry.reason and 'or stop it being recursive' in entry.reason
        assert 'none of its descendants is snapshotted, sent or pruned with the tree' in entry.reason

    def test_the_parent_inside_a_receive_target_is_one_entry_about_the_parent(self, check_zfs):
        check_zfs.tree('pool/srv', f'pool/srv/{self.P.split("/")[1]}', f'pool/srv/{self.P.split("/")[1]}/a')
        parent = f'pool/srv/{self.P.split("/")[1]}'
        cfg = server_config(parent_ds(parent, '1h', remote('a')), target='pool/srv')
        (entry,) = copies_entries(collect(cfg))
        assert entry.subject == parent and 'is itself inside a backup copy or receive target (pool/srv: ' in entry.reason

    def test_a_parent_whose_discovery_failed_has_no_copies_entry(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/copy')
        check_zfs.marker(f'{self.P}/copy')
        check_zfs.fail('fail_names', self.P)
        assert copies_entries(collect(self.cfg())) == []

    def test_a_parent_without_copies_has_none(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/a')
        assert copies_entries(collect(self.cfg())) == []

    def test_the_siblings_are_still_observed(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/ok', f'{self.P}/copy')
        check_zfs.marker(f'{self.P}/copy')
        assert [o.subject for o in collect(self.cfg()) if isinstance(o, Observation) and o.subject != self.P] == [
            f'{self.P}/ok']

    @pytest.mark.parametrize('count, shown, more', [(1, 1, 0), (20, 20, 0), (21, 20, 1), (25, 20, 5)])
    def test_at_most_twenty_entries_per_parent_then_one_for_the_rest(self, check_zfs, count, shown, more):
        names = [f'{self.P}/copy{i:02d}' for i in range(count)]
        check_zfs.tree(self.P, *names)
        for name in names:
            check_zfs.marker(name)
        entries = copies_entries(collect(self.cfg()))
        assert len(entries) == shown + (1 if more else 0)
        assert [e.subject for e in entries[:shown]] == names[:shown]
        if more:
            tail = entries[-1]
            assert (tail.subject, tail.state) == (f'{self.P} (+{more} more)', STATE_WARN)
            assert tail.reason == (
                f'and {more} more backup copies below the recursive dataset {self.P} '
                f'(the first 20 are listed): none is snapshotted, sent or pruned with the tree; move them out')

    def test_a_copy_below_a_nested_recursive_dataset_is_reported_once_under_the_inner_parent(self, check_zfs):
        # A and A/B are both recursive with remote entries; the copy sits below A/B. A/B owns its
        # subtree (`_expansion_owner`), so A does not walk into it: one entry, not one per parent.
        inner = f'{self.P}/B'
        check_zfs.tree(self.P, inner, f'{inner}/copy', f'{self.P}/other')
        check_zfs.marker(f'{inner}/copy', PROP_CLIENT_ID, 'local')
        cfg = config(parent_ds(self.P, '1h', remote('a')), parent_ds(inner, '1h', remote('a')))
        entries = copies_entries(collect(cfg))
        assert [(e.subject, e.reason.split(' below the recursive dataset ')[1].split(' ')[0]) for e in entries] == [
            (f'{inner}/copy', inner)]
        keys = [(e.source, e.subject) for e in entries]
        assert len(keys) == len(set(keys))

    def test_a_copy_below_a_nested_recursive_dataset_without_remote_entries_is_not_reported_by_the_outer_parent(self, check_zfs):
        # A/B is recursive but sends nothing (`remote: []`): it still owns its subtree, so A never
        # lists the copy and A/B is not a root: nobody reports it, and nobody reports it twice.
        inner = f'{self.P}/B'
        check_zfs.tree(self.P, inner, f'{inner}/copy')
        check_zfs.marker(f'{inner}/copy', PROP_CLIENT_ID, 'local')
        cfg = config(parent_ds(self.P, '1h', remote('a')), ds(inner, '1h', recursive=True))
        assert [e.subject for e in copies_entries(collect(cfg))] == []

    def test_the_cap_is_per_parent(self, check_zfs):
        for parent in ('pool/one', 'pool/two'):
            names = [f'{parent}/copy{i:02d}' for i in range(22)]
            check_zfs.tree(parent, *names)
            for name in names:
                check_zfs.marker(name)
        entries = copies_entries(collect(config(parent_ds('pool/one', '1h', remote('a')),
                                                parent_ds('pool/two', '1h', remote('a')))))
        assert len(entries) == 2 * 21
        assert [e.reason.split(' ')[0] for e in entries if e.reason.startswith('and ')] == ['and', 'and']

    def test_the_check_logs_no_error_and_leaves_the_episodes_alone(self, check_zfs, caplog):
        check_zfs.tree(self.P, f'{self.P}/copy', f'{self.P}/srv')
        check_zfs.marker(f'{self.P}/copy')
        cfg = server_config(parent_ds(self.P, '1h', remote('a')), target=f'{self.P}/srv')
        manager = DatasetManager(cfg)
        with caplog.at_level('DEBUG'):
            result = collect(cfg, manager=manager)
        assert len(copies_entries(result)) == 2
        assert not [r for r in caplog.records if r.levelno >= 40]
        assert manager._expand_copy_noted == set() and manager._expand_target_noted == set()

    def test_the_overall_verdict_is_warn_and_the_entries_survive_evaluate(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/copy')
        check_zfs.snaps(self.P, 600)
        check_zfs.marker(f'{self.P}/copy')
        entries = evaluate(collect(self.cfg()), NOW)
        assert overall_state(entries) == STATE_WARN
        (entry,) = [e for e in entries if e.source == 'copies']
        assert entry.state == STATE_WARN and entry.subject == f'{self.P}/copy'
        assert '"copies"' in health.to_json(entries, NOW)

    def test_the_copy_does_not_make_a_healthy_tree_worse_than_warn(self, check_zfs):
        check_zfs.tree(self.P, f'{self.P}/ok', f'{self.P}/copy')
        check_zfs.prop(self.P, ANCHOR_A, at(1800))
        check_zfs.prop(f'{self.P}/ok', ANCHOR_A, at(1800))
        check_zfs.marker(f'{self.P}/copy')
        entries = evaluate(collect(self.cfg()), NOW)
        assert {e.state for e in entries} == {STATE_OK, STATE_WARN}
        assert [e.source for e in entries if e.state == STATE_WARN] == ['copies']


class TestBudget:
    """`health_collect._Budget`, the two limits as one object (the manager methods that take `scope=` call
    `scope()` themselves, so its refusal after the deadline is part of the contract)."""

    def test_no_limits_never_expire(self, clock):
        budget = health_collect._Budget(None, None)
        clock.advance(10 ** 9)
        assert budget.left() is None and budget.expired() is False

    def test_left_and_expired_follow_the_clock(self, clock):
        budget = health_collect._Budget(10, 4)
        assert budget.left() == 10 and not budget.expired()
        clock.advance(9.5)
        assert budget.left() == 0.5 and not budget.expired()
        clock.advance(0.5)
        assert budget.left() == 0 and budget.expired()

    def test_scope_is_the_smaller_of_per_call_and_time_left(self, clock, mocker):
        timeout = mocker.patch.object(health_collect.zfs, 'timeout')
        budget = health_collect._Budget(10, 4)
        budget.scope()
        clock.advance(7)
        budget.scope()
        assert [c.args for c in timeout.call_args_list] == [(4,), (3,)]

    def test_scope_refuses_once_the_deadline_has_passed(self, clock):
        budget = health_collect._Budget(10, 4)
        clock.advance(10)
        with pytest.raises(ZFSTimeoutError, match=r'time limit \(10 s\) is spent'):
            budget.scope()

    def test_call_raises_out_of_time_for_a_failure_that_ends_at_the_deadline(self, clock):
        budget = health_collect._Budget(10, None)

        def fail_late():
            clock.advance(20)
            raise RuntimeError('boom')

        with pytest.raises(health_collect._OutOfTime):
            budget.call(fail_late)

    def test_call_lets_an_ordinary_failure_through(self, clock):
        budget = health_collect._Budget(10, None)
        with pytest.raises(RuntimeError, match='boom'):
            budget.call(lambda: (_ for _ in ()).throw(RuntimeError('boom')))

    def test_call_refuses_to_start_after_the_deadline(self, clock):
        budget = health_collect._Budget(10, None)
        clock.advance(11)
        ran = []
        with pytest.raises(health_collect._OutOfTime):
            budget.call(lambda: ran.append(1))
        assert ran == []

    def test_out_of_time_is_not_an_exception(self):
        """So no `except Exception` that turns a failed read into a finding can swallow it."""
        assert not issubclass(health_collect._OutOfTime, Exception)


# ---------------------------------------------------------------------------------------------
# Review fixes: the batched reads get the time left, each exec is timed on its own
# ---------------------------------------------------------------------------------------------

class Trace:
    """Every `libzfseasy.timeout` scope the collector enters and every `zfs` exec it makes, in order.
    `pairs()` joins an exec with the limit of the scope entered right before it (`None` when no scope
    was entered since the previous exec). `before_exec(argv)` / `after_exec(argv)` are test hooks; an
    exec is recorded before `before_exec` runs."""

    def __init__(self):
        self.events = []
        self.before_exec = None
        self.after_exec = None

    def record(self, argv):
        """An exec is about to start (the `world.hook` signature)."""
        self.events.append(('exec', list(argv)))
        if self.before_exec is not None:
            self.before_exec(list(argv))

    def pairs(self):
        out, pending = [], None
        for kind, value in self.events:
            if kind == 'scope':
                pending = value
            else:
                out.append((value, pending))
                pending = None
        return out

    @staticmethod
    def kind(argv):
        """What an exec is, by its argv (after the binary)."""
        sub = argv[0]
        if sub == 'list' and 'snapshot' in argv:
            return 'tree' if '-r' in argv else 'snapshots'
        if sub == 'list':
            return 'names' if '-r' in argv else 'target'
        if sub == 'get':
            columns = argv[argv.index('-o') + 1]
            if columns == 'name,property,source':
                return 'sources' if argv[-2] == 'all' else 'markers'
            return {'name,property,value,source': 'rows', 'value': 'value'}[columns]
        return sub

    def kinds(self):
        return [self.kind(argv) for argv, _ in self.pairs()]

    def limits(self, *kinds):
        """`[(kind, limit)]` of the execs of these kinds, in order."""
        return [(self.kind(argv), limit) for argv, limit in self.pairs() if self.kind(argv) in kinds]


@pytest.fixture
def trace(mocker):
    """A `Trace` of the budget's `zfs.timeout` scopes. With the Popen-level `world` set
    `world.hook = trace.record`; with `real_trace` the executables are traced too."""
    import libzfseasy
    t = Trace()
    real_timeout = libzfseasy.timeout

    def timeout(limit):
        t.events.append(('scope', limit))
        return real_timeout(limit)

    mocker.patch.object(health_collect.zfs, 'timeout', side_effect=timeout)
    return t


@pytest.fixture
def real_trace(trace, mocker):
    """`trace` over the real `subprocess.Popen` too (for the `check_zfs` executable). `before_exec` runs before
    the child starts, so it may rewrite the fake's spec without a race; `after_exec` runs once it has started
    (use it to move the clock, not the spec)."""
    import subprocess
    real_popen = subprocess.Popen

    def popen(argv, *args, **kwargs):
        rest = [os.fspath(a) for a in argv][1:]
        trace.record(rest)
        proc = real_popen(argv, *args, **kwargs)
        if trace.after_exec is not None:
            trace.after_exec(rest)
        return proc

    mocker.patch('subprocess.Popen', side_effect=popen)
    return trace


class TestBudgetBulkScope:
    """`_Budget.bulk_scope` and the wrappers: the batched reads get the time LEFT; `scope` stays `min(per_call, left)`."""

    @pytest.fixture
    def timeout(self, mocker):
        return mocker.patch.object(health_collect.zfs, 'timeout')

    def test_bulk_scope_is_the_time_left_not_per_call(self, clock, timeout):
        budget = health_collect._Budget(60, 10)
        budget.bulk_scope()
        clock.advance(25)
        budget.bulk_scope()
        assert [c.args for c in timeout.call_args_list] == [(60,), (35,)]

    def test_scope_stays_the_smaller_of_the_two_beside_it(self, clock, timeout):
        budget = health_collect._Budget(60, 10)
        budget.scope()
        clock.advance(55)
        budget.scope()
        assert [c.args for c in timeout.call_args_list] == [(10,), (5,)]

    @pytest.mark.parametrize('deadline, per_call', [(None, 10), (None, None)])
    def test_without_a_deadline_bulk_scope_is_per_call(self, clock, timeout, deadline, per_call):
        health_collect._Budget(deadline, per_call).bulk_scope()
        assert [c.args for c in timeout.call_args_list] == [(per_call,)]

    def test_bulk_scope_without_per_call_is_still_the_time_left(self, clock, timeout):
        health_collect._Budget(60, None).bulk_scope()
        assert [c.args for c in timeout.call_args_list] == [(60,)]

    def test_bulk_scope_refuses_once_the_deadline_has_passed(self, clock, timeout):
        budget = health_collect._Budget(10, 4)
        clock.advance(10)
        with pytest.raises(ZFSTimeoutError, match=r'time limit \(10 s\) is spent'):
            budget.bulk_scope()
        assert timeout.call_count == 0

    @pytest.mark.parametrize('method', ['scope', 'bulk_scope'])
    def test_the_time_left_is_read_once_per_scope(self, mocker, timeout, method):
        budget = health_collect._Budget(60, 10)
        left = mocker.patch.object(health_collect._Budget, 'left', return_value=7.0)
        getattr(budget, method)()
        assert left.call_count == 1
        assert timeout.call_args.args == (7.0,)                  # the time left, which is below per_call

    def test_call_bulk_runs_under_the_time_left(self, clock, timeout):
        budget = health_collect._Budget(100, 5)
        budget.call_bulk(lambda: None)
        budget.call(lambda: None)
        assert [c.args for c in timeout.call_args_list] == [(100,), (5,)]

    def test_call_bulk_passes_the_arguments_and_returns_the_value(self, clock, timeout):
        assert health_collect._Budget(100, 5).call_bulk(lambda a, b=0: a + b, 1, b=2) == 3

    def test_call_with_bulk_scope_hands_over_the_scope_and_enters_none_itself(self, clock, timeout):
        budget = health_collect._Budget(100, 5)
        got = []

        def fn(x, *, scope):
            got.append((x, scope))
            return 'done'

        assert budget.call_with_bulk_scope(fn, 'x') == 'done'
        assert got == [('x', budget.bulk_scope)] and timeout.call_count == 0

    def test_call_with_bulk_scope_refuses_to_start_after_the_deadline(self, clock):
        budget = health_collect._Budget(10, 5)
        clock.advance(11)
        ran = []
        with pytest.raises(health_collect._OutOfTime):
            budget.call_with_bulk_scope(lambda scope: ran.append(1))
        assert ran == []

    def test_call_with_bulk_scope_failure_at_the_deadline_is_out_of_time(self, clock):
        budget = health_collect._Budget(10, 5)

        def fail_late(scope):
            clock.advance(20)
            raise RuntimeError('boom')

        with pytest.raises(health_collect._OutOfTime):
            budget.call_with_bulk_scope(fail_late)

    def test_call_with_bulk_scope_lets_an_ordinary_failure_through(self, clock):
        def fail(scope):
            raise RuntimeError('boom')

        with pytest.raises(RuntimeError, match='boom'):
            health_collect._Budget(10, 5).call_with_bulk_scope(fail)

    # -- L3: the scope is entered inside the try -----------------------------------------------

    @pytest.mark.parametrize('how', ['call', 'call_bulk'])
    def test_a_deadline_that_passes_between_the_check_and_the_scope_is_out_of_time(self, mocker, how):
        """`left()` is positive for `expired()`, then non-positive when the scope is entered: the scope's
        refusal is the limit's (`_OutOfTime`), not a `ZFSTimeoutError` for the caller to report as a finding."""
        mocker.patch.object(health_collect._Budget, 'left', side_effect=[5.0, 0.0, 0.0])
        ran = []
        with pytest.raises(health_collect._OutOfTime):
            getattr(health_collect._Budget(10, 4), how)(lambda: ran.append(1))
        assert ran == []

    def test_the_same_for_a_function_that_enters_the_scope_itself(self, mocker):
        mocker.patch.object(health_collect._Budget, 'left', side_effect=[5.0, 0.0, 0.0])
        budget = health_collect._Budget(10, 4)

        def fn(scope):
            with scope():
                raise AssertionError('the scope must refuse')

        with pytest.raises(health_collect._OutOfTime):
            budget.call_with_bulk_scope(fn)

    def test_a_refusal_that_is_not_after_the_deadline_stays_an_ordinary_failure(self, mocker):
        """The deadline still has time, yet the scope refuses (any `ZFSTimeoutError`): not the limit's."""
        mocker.patch.object(health_collect._Budget, 'left', return_value=5.0)
        mocker.patch.object(health_collect.zfs, 'timeout', side_effect=ZFSTimeoutError('boom'))
        with pytest.raises(ZFSTimeoutError, match='boom'):
            health_collect._Budget(10, 4).call(lambda: None)


@pytest.mark.subprocess
class TestBulkReadsGetTheTimeLeft:
    """With `deadline=60, per_call=10`: the batched reads of a recursive parent run under `zfs.timeout(left)`, a
    single dataset's reads under `min(per_call, left)`."""

    def setup_tree(self, fake):
        fake.tree(RECURSIVE, f'{RECURSIVE}/x')
        fake.prop(f'{RECURSIVE}/x', ANCHOR_A, at(1800))

    def test_the_recursive_discovery_and_batches_use_the_time_left_the_rest_per_call(self, check_zfs, clock, real_trace):
        self.setup_tree(check_zfs)
        real_trace.after_exec = lambda argv: clock.advance(5) if Trace.kind(argv) == 'snapshots' else None
        collect(config(parent_ds()), deadline=60, per_call=10)
        assert real_trace.limits('snapshots', 'value') == [('snapshots', 10)] + [('value', 10)] * 4
        # the expansion (names + markers), both property phases, the tree and the re-listing: 60 - 5 left
        assert real_trace.kinds()[5:] == ['names', 'markers', 'sources', 'rows', 'tree', 'names']
        assert real_trace.limits('names', 'markers', 'sources', 'rows', 'tree') == [
            ('names', 55), ('markers', 55), ('sources', 55), ('rows', 55), ('tree', 55), ('names', 55)]

    def test_without_a_deadline_every_read_has_per_call(self, check_zfs, real_trace):
        self.setup_tree(check_zfs)
        collect(config(parent_ds()), per_call=6)
        assert len(real_trace.pairs()) == 11 and {limit for _, limit in real_trace.pairs()} == {6}

    def test_without_either_limit_every_read_has_none(self, check_zfs, real_trace):
        self.setup_tree(check_zfs)
        collect(config(parent_ds()), per_call=None)
        assert len(real_trace.pairs()) == 11 and {limit for _, limit in real_trace.pairs()} == {None}

    def test_a_small_deadline_caps_the_single_reads_and_the_bulk_ones_alike(self, check_zfs, clock, real_trace):
        self.setup_tree(check_zfs)
        collect(config(parent_ds()), deadline=8, per_call=10)
        assert {limit for _, limit in real_trace.pairs()} == {8}

    def test_the_configured_datasets_reads_are_the_smaller_of_per_call_and_left(self, check_zfs, clock, real_trace):
        check_zfs.tree('pool/one')
        check_zfs.prop('pool/one', ANCHOR_A, at(1800))
        real_trace.after_exec = lambda argv: clock.advance(4) if Trace.kind(argv) == 'snapshots' else None
        collect(config(ds('pool/one', '1h', remote('a'))), deadline=12, per_call=10)
        # the listing started with 12 left (min -> 10); the two property reads with 8 left (min -> 8)
        assert real_trace.limits('snapshots', 'value') == [('snapshots', 10), ('value', 8), ('value', 8)]

    def test_each_bulk_exec_re_reads_the_time_left(self, check_zfs, clock, real_trace):
        """Both property phases, the tree and the re-listing are commands of their own: a second goes by
        after each, and each scope sees the smaller `left`."""
        self.setup_tree(check_zfs)
        real_trace.after_exec = lambda argv: (clock.advance(1) if Trace.kind(argv) in
                                              ('names', 'markers', 'sources', 'rows', 'tree') else None)
        collect(config(parent_ds()), deadline=60, per_call=10)
        assert real_trace.limits('names', 'markers', 'sources', 'rows', 'tree') == [
            ('names', 60), ('markers', 59), ('sources', 58), ('rows', 57), ('tree', 56), ('names', 55)]

    def test_every_exec_has_a_scope_entered_right_before_it(self, check_zfs, real_trace):
        self.setup_tree(check_zfs)
        collect(config(parent_ds()), deadline=60, per_call=10)
        assert len(real_trace.pairs()) == len(check_zfs.calls) == 11
        assert all(limit is not None for _, limit in real_trace.pairs())

    def test_the_property_retry_is_four_scopes_each_with_its_own_left(self, check_zfs, clock, real_trace):
        child = f'{RECURSIVE}/x'
        self.setup_tree(check_zfs)
        check_zfs.spec['vanish_once'] = {child: [ANCHOR_A]}                   # phase 2 disagrees once
        check_zfs.save()
        real_trace.after_exec = lambda argv: (clock.advance(1) if Trace.kind(argv) in ('sources', 'rows') else None)
        result = collect(config(parent_ds()), deadline=60, per_call=10)
        assert [o.error for o in result if o.subject == child] == [None, None]
        assert real_trace.limits('sources', 'rows') == [
            ('sources', 60), ('rows', 59), ('sources', 58), ('rows', 57)]

    def test_a_deadline_passing_between_the_property_phases_runs_no_second_phase(self, check_zfs, clock, real_trace):
        """Phase 2 needs its own scope: after the clock ran out following phase 1 it is refused, so neither it
        nor the tree listing runs, and the parent's descendants are named, not reported as unreadable."""
        self.setup_tree(check_zfs)
        real_trace.after_exec = lambda argv: clock.advance(100) if Trace.kind(argv) == 'sources' else None
        result = collect(config(parent_ds()), deadline=60, per_call=10)
        assert 'rows' not in real_trace.kinds() and 'tree' not in real_trace.kinds()
        assert not [o for o in result if isinstance(o, Observation) and o.error]
        (entry,) = limit_entries(result)
        assert f'{RECURSIVE} (descendants)' in entry.reason
        assert [e.state for e in evaluate(result, NOW) if e.state == STATE_CRIT] == []


class TestReceivedReadsGetTheTimeLeft:
    """The received datasets: the target check and every per-dataset read are `min(per_call, left)`; the recursive
    listing and the batched property read get what is left of the deadline."""

    def setup_world(self, world):
        for n in 'xy':
            add_received(world, f'{TARGET}/c/{n}', 'c', client_ds(f'pool/{n}'), [at(60)])

    def test_the_listing_and_the_batch_use_the_time_left(self, world, clock, trace):
        self.setup_world(world)
        world.hook = trace.record
        trace.before_exec = lambda argv: clock.advance(3) if Trace.kind(argv) == 'target' else None
        result = collect(server_config(), deadline=60, per_call=10)
        assert [o.error for o in result] == [None, None]
        assert trace.limits('target', 'names', 'rows', 'snapshots') == [
            ('target', 10), ('names', 57), ('rows', 57), ('snapshots', 10), ('snapshots', 10)]

    def test_without_a_deadline_per_call_bounds_them_too(self, world, clock, trace):
        self.setup_world(world)
        world.hook = trace.record
        collect(server_config(), per_call=4)
        assert {limit for _, limit in trace.pairs()} == {4}
        assert len(trace.pairs()) == len(world.calls)

    def test_the_fallback_reads_use_per_call_not_the_time_left(self, world, clock, trace):
        """The batch read fails: each dataset's properties are read one `get.value` at a time, each in a scope of
        its own, bounded by `per_call` (they do not grow with the tree)."""
        self.setup_world(world)
        world.get_fails.add(TARGET)                                          # the batch exits 1
        world.hook = trace.record
        collect(server_config(), deadline=60, per_call=10)
        limits = trace.limits('target', 'names', 'rows', 'value', 'snapshots')
        assert limits[:3] == [('target', 10), ('names', 60), ('rows', 60)]
        values = [limit for kind, limit in limits if kind == 'value']
        assert len(values) == 4 and set(values) == {10}              # 2 properties per client dataset, 2 datasets

    def test_the_fallback_of_a_dataset_without_properties_is_three_scopes(self, world, clock, trace):
        world.filesystems += [TARGET, f'{TARGET}/c']
        add_received(world, f'{TARGET}/c/x', 'c', client_ds('pool/x'), [at(60)])
        world.get_fails.add(TARGET)
        world.hook = trace.record
        collect(server_config(), deadline=60, per_call=10)
        values = [(argv[-2:], limit) for argv, limit in trace.pairs() if Trace.kind(argv) == 'value']
        by_dataset = {}
        for (prop, name), limit in values:
            by_dataset.setdefault(name, []).append((prop, limit))
        # the container has no local property: client_id, config, then config's received source, one scope each
        assert by_dataset[f'{TARGET}/c'] == [(PROP_CLIENT_ID, 10), (PROP_CONFIG, 10), (PROP_CONFIG, 10)]
        assert by_dataset[f'{TARGET}/c/x'] == [(PROP_CLIENT_ID, 10), (PROP_CONFIG, 10)]
        assert all(limit is not None for _, limit in trace.pairs())      # nothing runs scope-less

    def test_a_deadline_passing_between_the_listing_and_the_batch_runs_no_batch(self, world, clock, trace):
        self.setup_world(world)
        world.hook = trace.record
        trace.before_exec = lambda argv: clock.advance(100) if Trace.kind(argv) == 'names' else None
        result = collect(server_config(), deadline=60, per_call=10)
        assert 'rows' not in trace.kinds()
        assert not [o for o in result if isinstance(o, Observation) and o.error]
        (entry,) = limit_entries(result)
        assert entry.reason == f'not checked within the time limit (60 s): {TARGET} (received datasets)'


@pytest.fixture
def fresh_name_warnings():
    """libzfseasy reports an unrepresentable name once per name per hour; start each test with none reported."""
    import libzfseasy.zfs as lz

    lz._warned_names.clear()
    yield
    lz._warned_names.clear()


@pytest.mark.subprocess
class TestNonAsciiNamesBelowARecursiveParent:
    """A snapshot or dataset name libzfseasy cannot represent (a user's `zfs snapshot 'pool/data/x@café'`) made the
    strict tree listing refuse EVERY child of the parent as "could not be trusted" (CRIT, each destination)."""

    X, Y = f'{RECURSIVE}/x', f'{RECURSIVE}/y'

    def setup_tree(self, fake):
        fake.tree(RECURSIVE, self.X, self.Y)
        fake.prop(self.X, ANCHOR_A, at(1800))
        fake.prop(self.Y, ANCHOR_A, at(1800))

    def children(self, result):
        return [o for o in result if isinstance(o, Observation) and o.subject in (self.X, self.Y)]

    def test_a_non_ascii_snapshot_beside_a_good_one_leaves_every_child_graded(
            self, check_zfs, caplog, fresh_name_warnings):
        self.setup_tree(check_zfs)
        check_zfs.spec['snapshots'][self.X].append([at(900) + 'é', 200])
        check_zfs.save()
        with caplog.at_level('DEBUG'):
            result = collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        kids = self.children(result)
        assert [(o.subject, o.error) for o in kids] == [(self.X, None), (self.Y, None)]
        assert all(o.newest_ts == epoch_of(at(1800)) for o in kids)           # the anchor is found
        assert 'CRIT' not in {e.state for e in evaluate(result, NOW)}
        assert not [o for o in result if isinstance(o, Observation) and o.error]
        assert not [r for r in caplog.records if r.levelno >= 30 and 'could not be trusted' in r.getMessage()]
        # libzfseasy says so itself, once
        assert len([r for r in caplog.records if r.name == 'libzfseasy' and r.levelno == 30
                    and 'cannot represent' in r.getMessage() and 'skipping' in r.getMessage()]) == 1

    def test_the_name_path_is_exactly_two_tree_listings(self, check_zfs, fresh_name_warnings):
        self.setup_tree(check_zfs)
        check_zfs.spec['snapshots'][self.X].append([at(900) + 'é', 200])
        check_zfs.save()
        collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        trees = [c for c in check_zfs.calls_of('list') if '-r' in c and 'snapshot' in c]
        assert len(trees) == 2 and trees[0] == trees[1]

    def test_a_clean_tree_is_one_listing(self, check_zfs):
        self.setup_tree(check_zfs)
        collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        assert len([c for c in check_zfs.calls_of('list') if '-r' in c and 'snapshot' in c]) == 1

    def test_a_non_ascii_dataset_below_the_parent_does_not_fail_its_siblings(self, check_zfs, fresh_name_warnings):
        self.setup_tree(check_zfs)
        odd = f'{RECURSIVE}/über'
        check_zfs.tree(odd)
        result = collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        assert [(o.subject, o.error) for o in self.children(result)] == [(self.X, None), (self.Y, None)]
        assert 'CRIT' not in {e.state for e in evaluate(result, NOW)}

    def test_a_wrong_column_count_first_still_fails_every_child(self, check_zfs, fresh_name_warnings):
        self.setup_tree(check_zfs)
        check_zfs.spec['snapshots'][self.X].append([at(900) + 'é', 200])
        check_zfs.fail('garbage_first', RECURSIVE)
        result = collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        kids = self.children(result)
        assert len(kids) == 2 and all('could not be trusted' in o.error for o in kids)
        assert {e.state for e in evaluate(kids, NOW)} == {STATE_CRIT}
        assert len([c for c in check_zfs.calls_of('list') if '-r' in c and 'snapshot' in c]) == 1

    def test_a_failed_tree_listing_is_not_retried(self, check_zfs, fresh_name_warnings):
        self.setup_tree(check_zfs)
        check_zfs.fail('fail_tree', RECURSIVE)
        result = collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        assert all('suspended' in o.error for o in self.children(result))
        assert len([c for c in check_zfs.calls_of('list') if '-r' in c and 'snapshot' in c]) == 1


class TestDeadlinePassingAtTheScope:
    """L3: `_Budget._run` enters the scope inside its `try`. A deadline that passes between the `expired()` check and
    the scope entry makes the scope refuse (`ZFSTimeoutError`); that is the limit's, so the dataset goes on the
    "(time limit)" list, never into a CRIT "cannot read"."""

    @pytest.fixture
    def spend_at_the_scope(self, monkeypatch, clock):
        """`_Budget.left` is positive for every `expired()`, then non-positive (and for ever after) the first time
        `_timeout` asks, once armed: exactly the window between the check and the scope entry."""
        state = {'armed': False, 'spent': False}
        real_left = health_collect._Budget.left

        def left(self):
            if state['armed'] and sys._getframe(1).f_code.co_name == '_timeout':
                state['spent'] = True
            return -1.0 if state['spent'] else real_left(self)

        monkeypatch.setattr(health_collect._Budget, 'left', left)
        return state

    def test_a_configured_dataset_is_on_the_time_limit_list_not_a_crit(self, world, spend_at_the_scope):
        world.snaps['pool/a'] = [at(60)]
        world.snaps['pool/b'] = [at(60)]
        world.hook = lambda rest: spend_at_the_scope.update(armed=True) if rest[-1] == 'pool/a' else None
        result = collect(config(ds('pool/a'), ds('pool/b')), deadline=60)
        assert [(o.subject, o.error) for o in result if isinstance(o, Observation)] == [('pool/a', None)]
        (entry,) = limit_entries(result)
        assert entry.reason == 'not checked within the time limit (60 s): pool/b'
        assert [e.state for e in evaluate(result, NOW)] == [STATE_OK, STATE_WARN]
        assert not [c for c in world.calls if c[-1] == 'pool/b']

    def test_the_received_target_check_is_named_not_a_crit_on_the_target(self, world, spend_at_the_scope):
        add_received(world, f'{TARGET}/c/x', 'c', client_ds(), [at(60)])
        spend_at_the_scope['armed'] = True
        result = collect(server_config(), deadline=60)
        assert not [o for o in result if isinstance(o, Observation)]
        (entry,) = limit_entries(result)
        assert entry.reason == f'not checked within the time limit (60 s): {TARGET} (received datasets)'
        assert world.calls == []

    @pytest.mark.subprocess
    def test_a_recursive_parents_descendants_are_named_not_unreadable(self, check_zfs, spend_at_the_scope, real_trace):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        # arm once the expansion is done: the batch's first bulk scope is then the one that finds the time gone
        real_trace.before_exec = lambda argv: (spend_at_the_scope.update(armed=True)
                                               if Trace.kind(argv) == 'markers' else None)
        result = collect(config(parent_ds()), deadline=60)
        assert not [o for o in result if isinstance(o, Observation) and o.error]
        assert [o.subject for o in result if isinstance(o, Observation) and '/x' in o.subject] == []
        (entry,) = limit_entries(result)
        assert f'{RECURSIVE} (descendants)' in entry.reason
        assert 'CRIT' not in {e.state for e in evaluate(result, NOW)}


@pytest.mark.subprocess
class TestAnchorOnASnapshotCreatedMeanwhile:
    """Nit: `_observe_parent_children` reads the properties BEFORE the snapshot tree. A snapshot the daemon creates
    and anchors between the two reads is then in the listing; read the other way round the anchor would name a
    snapshot the listing (taken a moment earlier) lacks, "not among the local managed snapshots"."""

    def test_an_anchor_on_a_snapshot_created_after_the_property_read_began_is_found(self, check_zfs, real_trace):
        child = f'{RECURSIVE}/x'
        check_zfs.tree(RECURSIVE)
        check_zfs.tree(child, snapshots=False)
        old, new = at(1800), at(600)
        check_zfs.spec['snapshots'][child] = [[old, 100]]
        check_zfs.prop(child, ANCHOR_A, old)
        check_zfs.save()
        state = {'done': False}

        def snapshot_and_anchor(argv):
            # the daemon snapshots and anchors right before the batch's value read: the properties read from
            # here on name `new`, and `new` exists for every listing that starts from here on
            if Trace.kind(argv) == 'rows' and not state['done']:
                state['done'] = True
                check_zfs.spec['snapshots'][child] = [[old, 100], [new, 101]]
                check_zfs.prop(child, ANCHOR_A, new)

        real_trace.before_exec = snapshot_and_anchor
        result = collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        (obs,) = [o for o in result if o.subject == child]
        assert state['done'] and obs.error is None
        assert 'not among' not in (obs.detail or '')
        assert obs.newest_ts == epoch_of(new) and obs.detail == new

    def test_the_batch_order_is_properties_then_tree_then_the_re_listing(self, check_zfs, real_trace):
        check_zfs.tree(RECURSIVE, f'{RECURSIVE}/x')
        check_zfs.prop(f'{RECURSIVE}/x', ANCHOR_A, at(1800))
        collect(config(parent_ds(RECURSIVE, '1h', remote('a'))))
        kinds = real_trace.kinds()
        assert kinds[-4:] == ['sources', 'rows', 'tree', 'names']

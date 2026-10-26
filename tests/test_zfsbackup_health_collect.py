"""`zfsbackup.health_collect` (plan C5): the read-only collector over `DatasetManager`.

The ZFS world is faked one level below libzfseasy, at `subprocess.Popen`, so the tests see the real
argv the collector causes (`zfs list ...`, `zfs get ...`) and can prove that nothing else runs.
"""

import io
import os
from datetime import datetime, timedelta, timezone

import pytest

from zfsbackup import health
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX, PROP_CLIENT_ID, PROP_CONFIG, PROP_INFLIGHT_PREFIX,
    SNAPSHOT_LIST_PROPERTIES, DatasetManager,
)
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Duration, RemoteDatasetConfig, RemoteServerConfig,
)
from zfsbackup.health import (
    CheckEntry, Observation, STATE_CRIT, STATE_OK, STATE_WARN, evaluate, overall_state,
)
from zfsbackup import health_collect
from zfsbackup.health_collect import collect
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
        self.empty_targets = set()
        self.missing_targets = set()
        self.target_list_fails = set()
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
        lines = ['\t'.join({'name': n, 'type': 'filesystem'}.get(c, '-') for c in columns)
                 for n in self.filesystems]
        lines += ['\t'.join({'name': n, 'type': 'volume'}.get(c, '-') for c in columns)
                  for n in self.volumes]
        return _Proc(''.join(line + '\n' for line in lines).encode(), b'', 0)

    def _get(self, rest):
        if rest[rest.index('-o') + 1] != 'value':
            return self._get_rows(rest)
        prop, name = rest[-2], rest[-1]
        if name in self.get_fails:
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

    def test_a_property_read_failure_reads_as_no_anchor(self, world):
        world.snaps['pool/data'] = [at(600)]
        world.get_fails.add('pool/data')
        _, anchor = collect(config(ds('pool/data', '1h', remote())))
        assert anchor.newest_ts is None and anchor.error is None

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
                assert '-H' in argv and '-s' not in argv
                columns = argv[argv.index('-o') + 1]
                assert columns in ('value', 'name,property,value,source'), argv   # never `received`
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

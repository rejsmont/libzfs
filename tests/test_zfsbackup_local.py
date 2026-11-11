"""`zfsbackup.local.LocalBackupManager` and the pieces around it (plan item 3.1), mocked.

A local destination (`url: zfs:<dataset>`) is sent to with a direct `zfs send` -> `zfs receive` on
this host. The manager is a `chain.ChainSender` whose four transport hooks do on this host, in the
same order, what the server's `/negotiate` and `/stream` do; there is no server behind it as a
second line of defence, so every receive-side check runs here. Pinned in this file:

* `_ready_transport` (refusal order, one message per episode, INFO on recovery, the target's
  existence cached per cycle) and the `_Budget` / `_stuck_problem` it uses;
* `_negotiate`: owned values on an existing copy, ancestors outermost first and never `-p` and never
  the copy, the partial/pending-ancestor wait, ownership conflicts (the `OwnerVerdict`
  classification), `_server_info` / `_server_newer` / `_server_newer_status` ALWAYS filled (a missing
  value fails OPEN), dry run, timeouts, the pending map;
* `_fetch_resume_token`: token / none / skip, counted by `_token_skipped`;
* `_stream_step`: the order of the checks (ownership -> exists -> header -> declared base -> type ->
  strict listing -> `decide_receive` -> abort -> `-F` -> receive), every refusal reason logged once
  under `[local refusal]` and cleared by `_record_step`, `-F` only when allowed and the copy exists,
  the header fed first, `_STOP` on a timeout, the started send reaped on every early return;
* `receiver.decide_receive`'s wording hooks, `snapshots_newer_than`, the `zfsbackup.api` wrappers,
  and that `import zfsbackup.local` loads no HTTP stack;
* `RemoteBackupWorker`'s dispatch to the two managers, and the wording hooks' defaults (the remote
  messages stay byte for byte what they were).

The `zfs` commands are `tests/_local_support.FakeHost`; the two streams are real pipes with
scripted exit statuses (the real `_Pump` runs over them); the real-children pump tests are
`test_zfsbackup_local_pump.py`. Nothing reaches `zfs`.
"""

import logging
import os
import subprocess
import sys
import types
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import libzfseasy
from libzfseasy import ZFSTimeoutError
from libzfseasy.types import Filesystem

from _clock_support import FakeTime
from _hermetic import hermetic_env
from _local_support import (
    BASE, CLIENT, COPY, DEST, DS, PARENT, TARGET, FakeHost, FakeLib, FakeReceive, FakeSend,
    ServerRow, owned_props,
)
from _remote_support import snapshot_presence  # noqa: F401  (fixture)
from _snapshot_support import default_guid
from _stream_support import (
    FILESYSTEM_FORCED, VOLUME_FORCED, expected_receive_argv, incremental_header, make_header,
)
from test_zfsbackup_chain import S1, S2, S3, S4, Ledger, listing, sinfo
from test_zfsbackup_chain import Rig as ChainRig
from test_zfsbackup_local_dest_config import HTTPS, LOCAL
from test_zfsbackup_local_dest_config import Rig as WorkerRig
from test_zfsbackup_local_dest_config import sdir  # noqa: F401  (fixture)
from test_zfsbackup_chain import DEST as CHAIN_DEST, DS as CHAIN_DS
from zfsbackup import chain as chain_module
from zfsbackup import local, receiver
from zfsbackup.backup_manager import (
    PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET, DatasetInfo, DatasetManager,
)
from zfsbackup.chain import (
    ChainSender, NegotiationFailed, _FAILED, _OK, _STOP, _SendStream, _retry_word,
)
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig, RetentionRule,
)
from zfsbackup.local import LocalBackupManager, _Budget, _LocalTarget
from zfsbackup.health import StatusWriter
from zfsbackup.sendmemory import TOKEN_SKIPS, SendMemory
from zfsbackup.receiver import OwnerVerdict

pytestmark = pytest.mark.unit

REAL_OWNERSHIP_CONFLICT = receiver.ownership_conflict      # before any rig patches it
LISTING = listing(S1, S2, S3)          # the local snapshots, newest first
OTHER_CLIENT = 'someone-else'


def records(caplog, level=None, logger='zfsbackup.local'):
    return [
        (r.levelname, r.getMessage()) for r in caplog.records
        if r.name == logger and (level is None or r.levelno == level)
    ]


def messages(caplog, level):
    return [m for _, m in records(caplog, level)]


def copy_rows(*names, first_txg=100):
    """`[(name, txg, guid)]` of a copy's snapshots, txg in the given order, guid as the local one."""
    return [(n, first_txg + i, default_guid(n)) for i, n in enumerate(names)]


def config_of(*keep_days) -> DatasetConfig:
    rules = [RetentionRule(timedelta(hours=1) * (i + 1), timedelta(days=d)) for i, d in enumerate(keep_days)]
    return DatasetConfig(name=DS, retention_rules=rules, remote=[RemoteDatasetConfig(destination=DEST)])


class LocalRig:
    """A `LocalBackupManager` for `pool/data` -> `zfs:bk/backups` (client `host1`), the `zfs` commands
    a `FakeHost`, the manager's anchors and pins a `Ledger`."""

    def __init__(self, mocker, *, client_id=CLIENT, dry_run=False, listings=(LISTING,)):
        self.host = FakeHost()
        self.lib = FakeLib(self.host)
        mocker.patch('zfsbackup.local.zfs', self.lib)
        real = REAL_OWNERSHIP_CONFLICT
        mocker.patch(
            'zfsbackup.receiver.ownership_conflict',
            side_effect=lambda *a, **k: real(*a, read_properties=self.host.read_properties, **k),
        )
        self.config = BackupConfig(
            datasets=[DatasetConfig(name=DS, remote=[RemoteDatasetConfig(destination=DEST)])],
            destinations={DEST: Destination(url=f'zfs:{TARGET}')},
            dry_run=dry_run,
        )
        self.manager = DatasetManager(self.config)
        self.dsi = self.manager.datasets[0]
        self.ledger = Ledger(mocker, self.manager, listings)
        self.mgr = LocalBackupManager(self.config, self.manager, client_id=client_id)
        self.transport = _LocalTarget(TARGET, CLIENT)
        self.dest = self.config.destinations[DEST]
        self.host.add(TARGET)

    @property
    def canonical(self) -> str:
        return receiver.canonical_dataset_config(self.dsi.config.to_property(DEST))

    def owned_copy(self, snaps=(), **kw):
        return self.host.add_copy(snaps=snaps, props=owned_props(config=self.canonical), **kw)

    def dsi_for(self, name, **kw) -> DatasetInfo:
        return DatasetInfo(Filesystem(name), DatasetConfig(
            name=name, remote=[RemoteDatasetConfig(destination=DEST)], **kw))

    def negotiate(self, snaps=None, dsi=None):
        return self.mgr._negotiate(
            self.transport, CLIENT, dsi or self.dsi, LISTING if snaps is None else snaps, DEST)

    def key(self, dsi=None):
        return ((dsi or self.dsi).name, DEST)


@pytest.fixture(autouse=True)
def _bounded_pump(mocker):
    """A step whose refusal was removed would wait on a fake send that never ends: bound every
    pump wait, so a regression fails instead of hanging."""
    mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 3.0)
    mocker.patch.object(local, 'LOCAL_COMMIT_TIMEOUT', 3.0)
    mocker.patch.object(local, 'PUMP_SLICE', 0.05)
    mocker.patch.object(local, 'PUMP_POLL_SLICE', 0.02)


@pytest.fixture
def rig(mocker):
    return LocalRig(mocker)


@pytest.fixture
def streams():
    """The fake send/receive handles a test makes; the pipes are closed at the end."""
    made = []

    class Streams:
        def send(self, *a, **kw):
            made.append(s := FakeSend(*a, **kw))
            return s

        def receive(self, *a, **kw):
            made.append(r := FakeReceive(*a, **kw))
            return r

    yield Streams()
    for item in made:
        if isinstance(item, FakeReceive):
            item.release()
        else:
            item.close()


# ---------------------------------------------------------------------------------------------
# _Budget and _stuck_problem
# ---------------------------------------------------------------------------------------------


class TestBudget:
    @pytest.fixture
    def seen(self, rig):
        """The `libzfseasy.timeout(...)` arguments `_Budget` asked for."""
        calls = []

        import contextlib

        @contextlib.contextmanager
        def timeout(seconds):
            calls.append(seconds)
            yield

        rig.lib.timeout = timeout
        return calls

    def test_the_default_is_the_module_constant(self, rig, mocker):
        mocker.patch.object(local, 'LOCAL_ZFS_BUDGET', 12.5)
        assert _Budget().seconds == 12.5

    def test_a_command_runs_under_what_is_left_of_the_budget(self, rig, seen):
        now = [100.0]
        budget = _Budget(10, clock=lambda: now[0])
        now[0] = 104.0
        assert budget(lambda a, k=0: (a, k), 1, k=2) == (1, 2)
        assert seen == [pytest.approx(6.0)]

    def test_each_command_gets_less_than_the_one_before(self, rig, seen):
        now = [0.0]
        budget = _Budget(10, clock=lambda: now[0])
        for t in (1.0, 3.0, 9.5):
            now[0] = t
            budget(lambda: None)
        assert seen == [pytest.approx(9.0), pytest.approx(7.0), pytest.approx(0.5)]

    def test_a_spent_budget_raises_without_running_anything(self, rig, seen):
        now = [0.0]
        budget = _Budget(5, clock=lambda: now[0])
        now[0] = 5.0
        ran = []
        with pytest.raises(ZFSTimeoutError, match='5 s budget'):
            budget(ran.append, 1)
        assert ran == [] and seen == []

    def test_what_the_command_raises_propagates(self, rig, seen):
        def boom():
            raise RuntimeError('nope')

        with pytest.raises(RuntimeError, match='nope'):
            _Budget(5)(boom)

    def test_the_real_libzfseasy_timeout_is_used_by_default(self, rig):
        """No stand-in: the budget reaches `libzfseasy.timeout` (the fake lib hands it through)."""
        assert rig.lib.timeout is libzfseasy.timeout
        assert _Budget(5)(lambda: 'ok') == 'ok'


class TestStuckProblem:
    def test_none_when_nothing_is_stuck(self, rig):
        assert local._stuck_problem() is None

    def test_names_the_children_and_says_the_calls_are_refused(self, rig):
        rig.host.stuck = [SimpleNamespace(pid=11, label='zfs receive'), SimpleNamespace(pid=12, label='zfs send')]
        text = local._stuck_problem()
        assert text.startswith('2 zfs process(es) of this worker did not exit after SIGKILL')
        assert 'pid 11 (zfs receive), pid 12 (zfs send)' in text
        assert 'timed zfs calls in this worker are refused until they exit' in text
        assert 'local destinations are skipped until then' in text

    def test_lists_three_and_counts_the_rest(self, rig):
        rig.host.stuck = [SimpleNamespace(pid=n, label='zfs list') for n in range(1, 6)]
        text = local._stuck_problem()
        assert text.startswith('5 zfs process(es)')
        assert 'pid 3 (zfs list), and 2 more' in text and 'pid 4' not in text

    def test_a_failing_sweep_never_raises(self, rig, mocker):
        mocker.patch.object(rig.lib, 'stuck_children', side_effect=RuntimeError('sweep'))
        assert local._stuck_problem() is None


# ---------------------------------------------------------------------------------------------
# _ready_transport
# ---------------------------------------------------------------------------------------------


class TestReadyTransport:
    def ready(self, rig, dest=None):
        return rig.mgr._ready_transport(DEST, dest or rig.dest)

    def test_a_usable_destination_gives_its_target_and_client_id(self, rig):
        transport = self.ready(rig)
        assert transport == _LocalTarget(TARGET, rig.mgr.client_id)
        assert transport.base == f'{TARGET}/{CLIENT}'

    def test_not_a_local_destination_is_refused_first_and_asks_zfs_nothing(self, rig, caplog):
        rig.mgr.client_id = None                    # would also be refused, later
        rig.host.stuck = [SimpleNamespace(pid=1, label='zfs')]
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            assert self.ready(rig, Destination(url='https://h/p')) is None
        (error,) = messages(caplog, logging.ERROR)
        assert 'is not a local destination' in error and 'https://h/p' in error
        assert rig.host.events == []

    def test_is_local_must_be_exactly_true(self, rig):
        looks_local = SimpleNamespace(is_local='yes', url='zfs:x/y', target_dataset='x/y')
        assert self.ready(rig, looks_local) is None
        assert rig.host.events == []

    def test_no_client_id_is_second_and_asks_zfs_nothing(self, rig, caplog):
        rig.mgr.client_id = None
        rig.host.stuck = [SimpleNamespace(pid=1, label='zfs')]
        assert self.ready(rig) is None
        (error,) = messages(caplog, logging.ERROR)
        assert error == f'local backup to {DEST} skipped: {local.NO_CLIENT_ID_MESSAGE}'
        assert 'zfsbackup-config identity set-client-id' in error
        assert rig.host.events == []

    def test_a_stuck_child_is_third_a_warning_and_asks_zfs_nothing(self, rig, caplog):
        rig.host.stuck = [SimpleNamespace(pid=7, label='zfs receive')]
        rig.host.datasets.pop(TARGET)                # would also be refused, later
        assert self.ready(rig) is None
        (warning,) = messages(caplog, logging.WARNING)
        assert warning.startswith(f'local backup to {DEST} skipped: 1 zfs process(es)')
        assert messages(caplog, logging.ERROR) == []
        assert rig.host.events == []

    def test_the_check_comes_last_a_timeout_is_a_warning(self, rig, caplog):
        rig.host.fail_on('exists', ZFSTimeoutError('slow pool'))
        assert self.ready(rig) is None
        (warning,) = messages(caplog, logging.WARNING)
        assert warning.startswith(f'local backup to {DEST} skipped: zfs did not answer within 60 s')
        assert f'while checking the target {TARGET}' in warning and 'the next cycle retries' in warning

    def test_an_expired_budget_is_the_same_timeout(self, rig, caplog, mocker):
        mocker.patch.object(local, 'LOCAL_ZFS_BUDGET', 0.0)
        assert self.ready(rig) is None
        assert rig.host.events == []
        assert 'zfs did not answer within 0 s' in messages(caplog, logging.WARNING)[0]

    def test_another_failure_is_an_error_that_defers_to_debug(self, rig, caplog):
        rig.host.fail_on('exists', RuntimeError('I/O error'))
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            assert self.ready(rig) is None
        (error,) = messages(caplog, logging.ERROR)
        assert 'cannot check the target bk/backups (see the DEBUG line)' in error
        assert any('I/O error' in m for m in messages(caplog, logging.DEBUG))

    def test_a_missing_target_says_how_to_create_it(self, rig, caplog):
        rig.host.datasets.pop(TARGET)
        assert self.ready(rig) is None
        (error,) = messages(caplog, logging.ERROR)
        assert error == (
            f'local destination {DEST}: the target dataset {TARGET} does not exist; create it '
            f'(for example `zfs create -o canmount=off {TARGET}`), or import its pool if it is on an '
            'exported one (a removable disk). Nothing is sent to it until then'
        )

    def test_one_message_per_episode(self, rig, caplog):
        rig.host.datasets.pop(TARGET)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            for _ in range(3):
                assert self.ready(rig) is None
        assert len(messages(caplog, logging.ERROR)) == 1
        assert len(messages(caplog, logging.DEBUG)) == 2

    def test_a_recovery_is_an_info_once_and_the_next_episode_logs_again(self, rig, caplog):
        rig.host.datasets.pop(TARGET)
        self.ready(rig)
        rig.host.add(TARGET)
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert self.ready(rig) is not None
            assert self.ready(rig) is not None
        assert messages(caplog, logging.INFO) == [f'local destination {DEST} ({TARGET}) is usable again']
        rig.host.datasets.pop(TARGET)
        self.ready(rig)
        assert len(messages(caplog, logging.ERROR)) == 2

    def test_a_recovery_from_a_timeout_is_also_an_info(self, rig, caplog):
        rig.host.fail_on('exists', ZFSTimeoutError('slow'))
        self.ready(rig)
        rig.host._fail.clear()
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert self.ready(rig) is not None
        assert any('is usable again' in m for m in messages(caplog, logging.INFO))

    def test_without_begin_cycle_the_target_is_checked_every_time(self, rig):
        for _ in range(3):
            assert self.ready(rig) is not None
        assert len(rig.host.of('exists')) == 3

    def test_with_begin_cycle_it_is_checked_once_per_cycle(self, rig):
        rig.mgr.begin_cycle()
        for _ in range(3):
            assert self.ready(rig) is not None
        assert len(rig.host.of('exists')) == 1
        rig.mgr.begin_cycle()
        assert self.ready(rig) is not None
        assert len(rig.host.of('exists')) == 2

    def test_a_missing_target_is_not_cached(self, rig):
        rig.mgr.begin_cycle()
        rig.host.datasets.pop(TARGET)
        assert self.ready(rig) is None
        rig.host.add(TARGET)
        assert self.ready(rig) is not None
        assert len(rig.host.of('exists')) == 2

    def test_a_changed_target_is_checked_again_within_the_cycle(self, rig):
        rig.mgr.begin_cycle()
        self.ready(rig)
        rig.host.add('other/target')
        assert rig.mgr._ready_transport(DEST, Destination(url='zfs:other/target')) == _LocalTarget(
            'other/target', CLIENT)
        assert [e[1] for e in rig.host.of('exists')] == [TARGET, 'other/target']

    def test_each_destination_has_its_own_cache(self, rig):
        rig.mgr.begin_cycle()
        rig.host.add('bk/second')
        self.ready(rig)
        rig.mgr._ready_transport('second', Destination(url='zfs:bk/second'))
        self.ready(rig)
        rig.mgr._ready_transport('second', Destination(url='zfs:bk/second'))
        assert len(rig.host.of('exists')) == 2


# ---------------------------------------------------------------------------------------------
# The ownership classification
# ---------------------------------------------------------------------------------------------


class TestOwnerMessage:
    """`_owner_message` classifies `receiver`'s `OwnerVerdict` by its flags (`unowned`,
    `source_mismatch`; anything else is another host's); the real verdicts of
    `receiver.ownership_conflict` are run through it below."""

    def message(self, verdict, copy=COPY):
        return LocalBackupManager._owner_message(copy, DS, CLIENT, verdict)

    def test_unowned_names_the_adopting_command(self):
        text = self.message(OwnerVerdict('whatever', unowned=True))
        assert text == (
            f'{COPY} already exists on this host and is not recorded as a backup '
            f'({PROP_CLIENT_ID} is not set on it); refusing to adopt it. If it is meant to hold '
            f'this host\'s backups, adopt it on purpose: zfs set {PROP_CLIENT_ID}={CLIENT} {COPY}'
        )

    def test_the_copy_name_is_shell_quoted_in_the_command(self):
        text = self.message(OwnerVerdict('x', unowned=True), copy='bk/a b')
        assert "'bk/a b'" in text

    def test_a_source_mismatch_says_refusing_to_mix_in(self):
        text = self.message(OwnerVerdict('whatever', source_mismatch=True))
        assert text == (
            f"{COPY} already holds backups of another dataset ({PROP_SOURCE_DATASET} differs); "
            f"refusing to mix in {DS!r}. Move it aside with `zfs rename`"
        )

    def test_anything_else_is_another_hosts_backup(self):
        text = self.message(OwnerVerdict('different client'))
        assert text == (
            f"{COPY} is recorded as another host's backup ({PROP_CLIENT_ID} differs from this "
            f"host's client id {CLIENT!r}); refusing to use it. Copies made under an earlier "
            "client id of this host are no longer used"
        )

    def test_the_classification_reads_the_flags_not_the_wording(self):
        text = self.message(OwnerVerdict("server dataset x already holds backups of 'a'; refusing to mix in 'b'"))
        assert "another host's backup" in text

    def test_unowned_beats_the_source_mismatch_flag(self):
        assert 'not recorded as a backup' in self.message(OwnerVerdict('m', unowned=True, source_mismatch=True))

    def test_a_none_message_is_another_hosts(self):
        assert "another host's backup" in self.message(OwnerVerdict(None))

    @pytest.mark.parametrize('props, expected', [
        ({}, 'not recorded as a backup'),
        ({PROP_CLIENT_ID: OTHER_CLIENT, PROP_SOURCE_DATASET: DS}, "another host's backup"),
        ({PROP_CLIENT_ID: CLIENT, PROP_SOURCE_DATASET: 'pool/other'}, 'refusing to mix in'),
    ])
    def test_receivers_real_verdicts_classify_as_intended(self, rig, props, expected):
        rig.host.add(COPY, props=props)
        verdict = REAL_OWNERSHIP_CONFLICT(
            lambda f, *a, **k: f(*a, **k), COPY, DS, CLIENT, read_properties=rig.host.read_properties,
            lib=rig.lib,
        )
        assert verdict.message is not None
        assert expected in self.message(verdict)


# ---------------------------------------------------------------------------------------------
# _negotiate
# ---------------------------------------------------------------------------------------------


class TestNegotiateNewCopy:
    def test_ancestors_are_created_outermost_first_and_never_the_copy(self, rig):
        assert rig.negotiate() is None
        creates = rig.host.of('create')
        assert [c[1] for c in creates] == [BASE, PARENT]
        assert COPY not in rig.host.datasets
        for _, _, props, mount, kw in creates:
            assert props == {'canmount': 'noauto', PROP_CLIENT_ID: CLIENT}
            assert mount is False
            assert kw == {}, "no -p, no size, nothing but properties and mount"

    def test_only_the_missing_ancestors_are_created(self, rig):
        rig.host.add(BASE, props={PROP_CLIENT_ID: CLIENT})
        rig.negotiate()
        assert [c[1] for c in rig.host.of('create')] == [PARENT]

    def test_the_target_itself_is_never_created(self, rig):
        rig.negotiate()
        assert TARGET not in [c[1] for c in rig.host.of('create')]

    def test_nothing_is_set_on_a_copy_that_does_not_exist(self, rig):
        rig.negotiate()
        assert rig.host.of('set') == []

    def test_the_copy_is_remembered_as_pending(self, rig):
        rig.negotiate()
        assert COPY in rig.mgr._pending

    def test_it_reports_a_missing_copy_as_known_with_nothing_newer(self, rig):
        rig.negotiate()
        key = rig.key()
        assert rig.mgr._server_info[key] == ('none', COPY)
        assert rig.mgr._server_newer[key] == (0, [])
        assert rig.mgr._server_newer_status[key] == 'known'

    def test_no_snapshot_listing_is_asked_of_a_missing_copy(self, rig):
        rig.negotiate()
        assert rig.host.of('list-snaps') == []

    def test_a_bad_dataset_name_is_one_error_and_a_refusal(self, rig, caplog):
        bad = SimpleNamespace(name='pool/a@b', config=rig.dsi.config)
        for _ in range(2):
            with pytest.raises(NegotiationFailed, match="invalid dataset name 'pool/a@b'"):
                rig.negotiate(dsi=bad)
        assert len(messages(caplog, logging.ERROR)) == 1
        assert rig.host.events == []

    def test_a_config_that_cannot_be_stored_is_one_error_and_a_refusal(self, rig, caplog, mocker):
        mocker.patch.object(receiver, 'canonical_dataset_config', side_effect=ValueError('too large'))
        for _ in range(2):
            with pytest.raises(NegotiationFailed, match='too large'):
                rig.negotiate()
        (error,) = messages(caplog, logging.ERROR)
        assert 'its configuration cannot be stored on the local copy (too large); nothing is sent' in error
        assert rig.host.events == []


class TestNegotiateExistingCopy:
    def test_the_owned_values_are_set_on_the_copy(self, rig):
        rig.owned_copy(copy_rows(S1))
        rig.negotiate()
        assert rig.host.of('set') == [('set', COPY, {
            PROP_CONFIG: rig.canonical, PROP_CLIENT_ID: CLIENT, PROP_SOURCE_DATASET: DS,
        })]

    def test_the_stored_config_is_replaced_by_the_canonical_one(self, rig):
        rig.owned_copy(copy_rows(S1)).props[PROP_CONFIG] = 'stale'
        rig.negotiate()
        assert rig.host.datasets[COPY].props[PROP_CONFIG] == rig.canonical

    def test_nothing_is_created_for_a_copy_that_exists(self, rig):
        rig.owned_copy(copy_rows(S1))
        rig.negotiate()
        assert rig.host.of('create') == []

    def test_the_ancestors_are_not_examined(self, rig):
        rig.owned_copy(copy_rows(S1))
        rig.negotiate()
        assert {e[1] for e in rig.host.of('exists')} == {COPY}, "only the copy itself, never an ancestor"

    def test_a_pending_entry_is_dropped(self, rig):
        rig.mgr._pending[COPY] = 1.0
        rig.owned_copy(copy_rows(S1))
        rig.negotiate()
        assert COPY not in rig.mgr._pending

    def test_the_common_snapshot_is_the_newest_local_one_the_copy_holds(self, rig):
        rig.owned_copy(copy_rows(S1, S2))
        assert rig.negotiate() == S2

    def test_no_common_snapshot_is_none(self, rig):
        rig.owned_copy(copy_rows('other_20200101000000'))
        assert rig.negotiate() is None

    def test_an_unreadable_stored_config_does_not_stop_the_negotiate(self, rig, caplog):
        rig.owned_copy(copy_rows(S1)).props[PROP_CONFIG] = '!!not base64!!'
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            assert rig.negotiate() == S1
        assert any('not comparing the retention' in m for m in messages(caplog, logging.DEBUG))
        assert messages(caplog, logging.WARNING) == []


class TestNegotiateRetentionShrink:
    def test_a_shorter_horizon_is_one_warning_and_never_a_refusal(self, rig, caplog):
        rig.dsi.config = config_of(7)
        rig.owned_copy(copy_rows(S1)).props[PROP_CONFIG] = config_of(30).to_property(DEST)
        assert rig.negotiate() == S1
        (warning,) = messages(caplog, logging.WARNING)
        assert f'the retention horizon of {COPY} shrinks from 30d to 7d' in warning
        assert 'pruned from the next pruning cycle' in warning

    def test_an_unchanged_or_longer_horizon_is_quiet(self, rig, caplog):
        rig.dsi.config = config_of(30)
        rig.owned_copy(copy_rows(S1)).props[PROP_CONFIG] = config_of(7).to_property(DEST)
        rig.negotiate()
        assert messages(caplog, logging.WARNING) == []

    def test_a_dry_run_does_not_compare(self, mocker, caplog):
        rig = LocalRig(mocker, dry_run=True)
        rig.dsi.config = config_of(7)
        rig.owned_copy(copy_rows(S1)).props[PROP_CONFIG] = config_of(30).to_property(DEST)
        rig.negotiate()
        assert messages(caplog, logging.WARNING) == []
        assert ('value', COPY, PROP_CONFIG, 'local') not in rig.host.events

    def test_a_timeout_reading_it_is_the_negotiates_timeout(self, rig):
        rig.owned_copy(copy_rows(S1))
        rig.host.fail_on('value', ZFSTimeoutError('slow'), name=COPY)
        with pytest.raises(NegotiationFailed) as raised:
            rig.negotiate()
        assert raised.value.zfs_timeout is True


class TestNegotiateOwnershipConflicts:
    CASES = {
        'unowned': ({}, 'not recorded as a backup'),
        'another-client': ({PROP_CLIENT_ID: OTHER_CLIENT, PROP_SOURCE_DATASET: DS}, "another host's backup"),
        'another-source': ({PROP_CLIENT_ID: CLIENT, PROP_SOURCE_DATASET: 'pool/other'}, 'refusing to mix in'),
    }

    @pytest.mark.parametrize('case', list(CASES))
    def test_one_error_and_nothing_written_or_listed(self, rig, caplog, case):
        props, expected = self.CASES[case]
        rig.host.add(COPY, props=props, snaps=[ServerRow(*r) for r in copy_rows(S1)])
        for _ in range(2):
            with pytest.raises(NegotiationFailed) as raised:
                rig.negotiate()
        assert expected in str(raised.value) and raised.value.reloading is False
        (error,) = messages(caplog, logging.ERROR)
        assert error.startswith(f'{DS} -> {DEST}: {COPY}') and expected in error
        assert rig.host.of('set') == [] and rig.host.of('create') == []
        assert rig.host.of('list-snaps') == []
        assert rig.key() not in rig.mgr._server_info

    def test_the_refusal_is_the_last_problem(self, rig):
        rig.host.add(COPY, props={})
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        assert 'not recorded as a backup' in rig.mgr.last_problem(DS, DEST)

    def test_resolving_it_logs_a_later_refusal_again(self, rig, caplog):
        rig.host.add(COPY, props={})
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        rig.host.datasets[COPY].props = owned_props(config=rig.canonical)
        rig.negotiate()
        rig.host.datasets[COPY].props = {}
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        assert len(messages(caplog, logging.ERROR)) == 2

    def test_a_conflicting_ancestor_is_refused_with_its_own_name(self, rig, caplog):
        rig.host.add(BASE, props={})
        with pytest.raises(NegotiationFailed) as raised:
            rig.negotiate()
        assert f'{BASE} already exists on this host and is not recorded as a backup' in str(raised.value)
        assert rig.host.of('create') == [] and rig.host.of('set') == []
        assert len(messages(caplog, logging.ERROR)) == 1

    def test_an_ancestor_of_another_client_is_refused(self, rig):
        rig.host.add(BASE, props={PROP_CLIENT_ID: OTHER_CLIENT})
        with pytest.raises(NegotiationFailed, match="another host's backup"):
            rig.negotiate()
        assert rig.host.of('create') == []

    def test_the_target_itself_is_not_examined_for_ownership(self, rig):
        rig.host.add(TARGET, props={PROP_CLIENT_ID: OTHER_CLIENT})
        assert rig.negotiate() is None
        assert [c[1] for c in rig.host.of('create')] == [BASE, PARENT]


class TestNegotiateUnsettledAncestors:
    def test_an_ancestor_holding_a_first_receives_partial_state_makes_it_wait(self, rig, caplog):
        rig.host.add(BASE, props={PROP_CLIENT_ID: CLIENT})
        rig.host.add(PARENT, props={PROP_CLIENT_ID: CLIENT}, token='1-abc')
        for _ in range(2):
            with pytest.raises(NegotiationFailed) as raised:
                rig.negotiate()
        assert raised.value.reloading is True and raised.value.zfs_timeout is False
        assert str(raised.value) == f'local copy is busy: waiting for {PARENT}'
        (warning,) = messages(caplog, logging.WARNING)
        assert f"waiting for '{PARENT}': it holds the partial state of a first receive" in warning
        assert f'zfs receive -A {PARENT}' in warning and f'{DS} is not sent until it has settled' in warning
        assert rig.host.of('create') == [] and rig.host.of('set') == []

    def test_a_cut_incremental_ancestor_with_snapshots_does_not_block(self, rig):
        rig.host.add(BASE, props={PROP_CLIENT_ID: CLIENT})
        rig.host.add(PARENT, props={PROP_CLIENT_ID: CLIENT}, token='1-abc', snaps=[ServerRow(*copy_rows(S1)[0])])
        assert rig.negotiate() is None
        assert [c[1] for c in rig.host.of('create')] == []

    def test_a_parent_negotiated_but_not_yet_received_makes_its_child_wait(self, rig, caplog):
        parent = rig.dsi_for('pool')
        rig.negotiate(dsi=parent)                       # creates only the container above it
        assert [c[1] for c in rig.host.of('create')] == [BASE]
        assert PARENT in rig.mgr._pending
        with pytest.raises(NegotiationFailed) as raised:
            rig.negotiate()
        assert raised.value.reloading is True
        assert str(raised.value) == f'local copy is busy: waiting for {PARENT}'
        (warning,) = messages(caplog, logging.WARNING)
        assert f"waiting for '{PARENT}': its first backup has not completed" in warning
        assert [c[1] for c in rig.host.of('create')] == [BASE], "the child created nothing"

    def test_the_wait_ends_after_the_ttl_and_the_child_makes_a_plain_container(self, rig, mocker):
        clock = FakeTime()
        clock.frozen = 1000.0
        mocker.patch.object(local, 'time', clock)
        rig.negotiate(dsi=rig.dsi_for('pool'))
        clock.advance(local.LOCAL_PENDING_TTL - 1)
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        clock.advance(1)
        rig.negotiate()
        assert [c[1] for c in rig.host.of('create')] == [BASE, PARENT]
        assert PARENT not in rig.mgr._pending

    def test_every_negotiate_of_the_parent_refreshes_the_ttl(self, rig, mocker):
        clock = FakeTime()
        clock.frozen = 1000.0
        mocker.patch.object(local, 'time', clock)
        parent = rig.dsi_for('pool')
        rig.negotiate(dsi=parent)
        clock.advance(local.LOCAL_PENDING_TTL - 1)
        rig.negotiate(dsi=parent)
        clock.advance(local.LOCAL_PENDING_TTL - 1)
        with pytest.raises(NegotiationFailed):
            rig.negotiate()

    def test_a_settled_wait_logs_a_later_one_again(self, rig, caplog):
        rig.negotiate(dsi=rig.dsi_for('pool'))
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        rig.mgr._pending.clear()
        rig.host.add(PARENT, props={PROP_CLIENT_ID: CLIENT})
        rig.negotiate()
        rig.host.datasets[PARENT].token = '1-x'
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        assert len(messages(caplog, logging.WARNING)) == 2

    def test_a_dry_run_neither_waits_nor_records_a_pending_copy(self, mocker, caplog):
        rig = LocalRig(mocker, dry_run=True)
        rig.host.add(BASE, props={PROP_CLIENT_ID: CLIENT})
        rig.host.add(PARENT, props={PROP_CLIENT_ID: CLIENT}, token='1-abc')
        assert rig.negotiate() is None
        assert rig.mgr._pending == {}
        assert messages(caplog, logging.WARNING) == []


class TestNegotiateClaim:
    """B3: two senders of unrelated datasets under one `zfs:` target can both find the same container
    missing. The one that creates it claims the missing ancestors in the shared `SendMemory`
    (`claim`, all or nothing, never blocking); another sender that needs one of them gets a RETRYABLE
    `NegotiationFailed` (like the server's 503 for an unsettled ancestor) instead of "dataset already
    exists" (an ERROR and a hard failure). The claim is released on every path.

    Mutation-checked (scratch copy): no claim at all fails 3 tests here; no re-look under the claim 1;
    no release in the `finally` 2."""

    def sibling(self, rig):
        return rig.dsi_for('pool2/other')

    def test_the_missing_ancestors_are_claimed_all_together_and_released_after(self, rig):
        mem = rig.mgr._memory
        seen = []
        real_claim, real_release = mem.claim, mem.release
        mem.claim = lambda names: (seen.append(('claim', list(names))), real_claim(names))[1]
        mem.release = lambda names: (seen.append(('release', list(names))), real_release(names))[1]
        rig.negotiate()
        assert [(k, sorted(n)) for k, n in seen] == [('claim', [BASE, PARENT]), ('release', [BASE, PARENT])]
        assert real_claim([BASE, PARENT]) is True, 'nothing is left claimed'

    def test_a_held_ancestor_is_a_retryable_failure_that_creates_nothing(self, rig, caplog):
        rig.mgr._memory.claim([PARENT])                    # another sender is creating it right now
        with pytest.raises(NegotiationFailed) as raised:
            rig.negotiate()
        assert raised.value.reloading is True and raised.value.zfs_timeout is False
        assert str(raised.value) == f'local copy is busy: another send is creating {BASE}', 'names the outermost'
        assert rig.host.of('create') == []
        assert messages(caplog, logging.ERROR) == []
        assert rig.mgr._memory.claim([PARENT]) is False, 'the other sender still holds it: we released nothing'

    def test_a_refused_claim_takes_none_of_the_names(self, rig):
        rig.mgr._memory.claim([PARENT])
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        assert rig.mgr._memory.claim([BASE]) is True, 'all or nothing: BASE was not taken'

    def test_two_siblings_creating_the_same_container(self, rig, caplog):
        """The second negotiate starts while the first is in the middle of its `zfs create`: it waits (retryable),
        it neither fails hard nor creates the container twice; once the first is done it settles quietly."""
        outcomes = []
        real_create = rig.lib.create

        def create(ds, properties=None, mount=True, **kw):
            if not outcomes:
                try:
                    rig.negotiate(dsi=self.sibling(rig))
                except NegotiationFailed as e:
                    outcomes.append(e)
            return real_create(ds, properties=properties, mount=mount, **kw)

        rig.lib.create = create
        rig.negotiate()
        (error,) = outcomes
        assert error.reloading is True and 'another send is creating' in str(error)
        rig.lib.create = real_create
        creates = [c[1] for c in rig.host.of('create')]
        assert creates == [BASE, PARENT], 'each container created once'
        assert messages(caplog, logging.ERROR) == []

    def test_what_the_sibling_created_between_the_look_and_the_claim_is_not_created_again(self, rig):
        """Look, claim, look again UNDER the claim: the other sender may have created and released a container
        since the first look."""
        mem = rig.mgr._memory
        real_claim = mem.claim

        def claim(names):
            rig.host.add(BASE, props={PROP_CLIENT_ID: CLIENT})
            rig.host.add(PARENT, props={PROP_CLIENT_ID: CLIENT})
            return real_claim(names)

        mem.claim = claim
        rig.negotiate()                                      # no "already exists"
        assert rig.host.of('create') == []
        assert real_claim([BASE, PARENT]) is True

    def test_the_claim_is_released_when_a_create_fails(self, rig):
        real_create = rig.lib.create

        def create(ds, properties=None, mount=True, **kw):
            if str(ds) == PARENT:
                raise OSError('pool is full')
            return real_create(ds, properties=properties, mount=mount, **kw)

        rig.lib.create = create
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        assert rig.mgr._memory.claim([BASE, PARENT]) is True

    def test_the_claim_is_released_when_the_negotiate_is_cut_by_a_timeout(self, rig):
        real_create = rig.lib.create

        def create(ds, properties=None, mount=True, **kw):
            raise ZFSTimeoutError('create timed out')

        rig.lib.create = create
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        assert rig.mgr._memory.claim([BASE, PARENT]) is True

    def test_an_existing_copy_and_a_dry_run_claim_nothing(self, mocker):
        rig = LocalRig(mocker)
        rig.owned_copy()
        claims = []
        real = rig.mgr._memory.claim
        rig.mgr._memory.claim = lambda names: (claims.append(list(names)), real(names))[1]
        rig.negotiate()
        dry = LocalRig(mocker, dry_run=True)
        dry.mgr._memory.claim = lambda names: claims.append(list(names)) or True
        dry.negotiate()
        assert claims == []

    def test_ancestors_that_exist_need_no_claim(self, rig):
        rig.host.add(BASE, props={PROP_CLIENT_ID: CLIENT})
        rig.host.add(PARENT, props={PROP_CLIENT_ID: CLIENT})
        claims = []
        real = rig.mgr._memory.claim
        rig.mgr._memory.claim = lambda names: (claims.append(list(names)), real(names))[1]
        rig.negotiate()
        assert claims == []


class TestNegotiateAlwaysFillsTheServerInfo:
    """A missing value fails OPEN in the chain (`absent` reads as a server that predates B-2b), so
    every successful negotiate sets all three, in every shape of copy."""

    def build(self, rig, shape):
        if shape == 'missing':
            return None
        if shape == 'empty':
            rig.owned_copy()
            return None
        if shape == 'no-common':
            rig.owned_copy(copy_rows('other_20200101000000'))
            return None
        if shape == 'common-newest':
            rig.owned_copy(copy_rows(S1, S2, S3))
            return S3
        if shape == 'common-with-newer':
            rig.owned_copy(copy_rows(S1, 'extra_a', 'extra_b'))
            return S1
        if shape == 'untrusted':
            rig.owned_copy(copy_rows(S1, 'extra_a'))
            rig.host.untrusted.add(COPY)
            return S1
        if shape == 'no-txg':
            rig.owned_copy([(S1, 100, default_guid(S1)), ('extra_a', None, 5)])
            return S1
        if shape == 'untrusted-no-common':
            rig.owned_copy(copy_rows('other_20200101000000'))
            rig.host.untrusted.add(COPY)
            return None
        raise AssertionError(shape)

    SHAPES = ['missing', 'empty', 'no-common', 'common-newest', 'common-with-newer', 'untrusted',
              'no-txg', 'untrusted-no-common']

    @pytest.mark.parametrize('shape', SHAPES)
    @pytest.mark.parametrize('dry_run', [False, True])
    def test_all_three_are_set(self, mocker, shape, dry_run):
        rig = LocalRig(mocker, dry_run=dry_run)
        expected_common = self.build(rig, shape)
        assert rig.negotiate() == expected_common
        key = rig.key()
        assert key in rig.mgr._server_info
        assert key in rig.mgr._server_newer
        assert key in rig.mgr._server_newer_status
        assert rig.mgr._server_newer_status[key] in ('known', 'unknown')
        assert rig.mgr._server_info[key][1] == COPY

    def test_a_copy_without_snapshots_is_none(self, rig):
        self.build(rig, 'empty')
        rig.negotiate()
        assert rig.mgr._server_info[rig.key()] == ('none', COPY)
        assert rig.mgr._server_newer_status[rig.key()] == 'known'

    def test_nothing_newer_than_the_common_snapshot_is_known_and_empty(self, rig):
        self.build(rig, 'common-newest')
        rig.negotiate()
        assert rig.mgr._server_newer[rig.key()] == (0, [])
        assert rig.mgr._server_newer_status[rig.key()] == 'known'
        assert rig.mgr._server_info[rig.key()] == ('plain', COPY)

    def test_newer_snapshots_are_listed_oldest_first_with_string_guids(self, rig):
        self.build(rig, 'common-with-newer')
        rig.negotiate()
        count, newer = rig.mgr._server_newer[rig.key()]
        assert count == 2
        assert newer == [
            {'name': 'extra_a', 'guid': str(default_guid('extra_a'))},
            {'name': 'extra_b', 'guid': str(default_guid('extra_b'))},
        ]
        assert rig.mgr._server_newer_status[rig.key()] == 'known'

    def test_the_list_is_cut_at_64_but_the_count_is_complete(self, rig):
        names = [f'extra_{n:03d}' for n in range(70)]
        rig.owned_copy(copy_rows(S1, *names))
        rig.negotiate()
        count, newer = rig.mgr._server_newer[rig.key()]
        assert local.SERVER_NEWER_MAX == 64
        assert count == 70 and len(newer) == 64
        assert [n['name'] for n in newer] == names[:64]

    def test_exactly_64_is_not_cut(self, rig):
        names = [f'extra_{n:03d}' for n in range(64)]
        rig.owned_copy(copy_rows(S1, *names))
        rig.negotiate()
        assert rig.mgr._server_newer[rig.key()][0] == 64
        assert len(rig.mgr._server_newer[rig.key()][1]) == 64

    def test_order_is_by_createtxg_not_by_name(self, rig):
        rig.owned_copy([(S1, 100, default_guid(S1)), ('b_late_name', 101, 1), ('a_early_name', 102, 2)])
        rig.negotiate()
        assert [n['name'] for n in rig.mgr._server_newer[rig.key()][1]] == ['b_late_name', 'a_early_name']

    def test_an_untrusted_listing_with_a_common_snapshot_is_unknown_and_warns(self, rig, caplog):
        self.build(rig, 'untrusted')
        assert rig.negotiate() == S1
        key = rig.key()
        assert rig.mgr._server_newer_status[key] == 'unknown'
        assert rig.mgr._server_newer[key] == (0, [])
        (warning,) = messages(caplog, logging.WARNING)
        assert 'the snapshot listing of' in warning and 'cannot be trusted' in warning
        assert rig.host.of('list-snaps')[-2:] == [
            ('list-snaps', COPY, True), ('list-snaps', COPY, False)], "the lenient re-list finds the common one"

    def test_a_missing_createtxg_is_unknown_and_warns(self, rig, caplog):
        self.build(rig, 'no-txg')
        assert rig.negotiate() == S1
        assert rig.mgr._server_newer_status[rig.key()] == 'unknown'
        (warning,) = messages(caplog, logging.WARNING)
        assert f'a snapshot of {COPY} has no createtxg' in warning

    def test_an_untrusted_listing_without_a_common_snapshot_has_nothing_to_report(self, rig):
        self.build(rig, 'untrusted-no-common')
        assert rig.negotiate() is None
        assert rig.mgr._server_newer_status[rig.key()] == 'known'
        assert rig.mgr._server_newer[rig.key()] == (0, [])

    def test_the_kind_of_a_copy_with_snapshots(self, rig):
        ds = rig.owned_copy(copy_rows(S1))
        rig.negotiate()
        assert rig.mgr._server_info[rig.key()][0] == 'plain'
        ds.encryption, ds.encryptionroot = 'aes-256-gcm', COPY
        rig.negotiate()
        assert rig.mgr._server_info[rig.key()][0] == 'raw'
        ds.encryptionroot = TARGET
        rig.negotiate()
        assert rig.mgr._server_info[rig.key()][0] == 'plain'

    def test_a_failed_negotiate_leaves_the_previous_answer_alone(self, rig):
        rig.owned_copy(copy_rows(S1))
        rig.negotiate()
        before = (rig.mgr._server_info[rig.key()], rig.mgr._server_newer_status[rig.key()])
        rig.host.fail_on('list-snaps', RuntimeError('boom'))
        with pytest.raises(NegotiationFailed):             # strict and lenient listings both fail
            rig.negotiate()
        assert (rig.mgr._server_info[rig.key()], rig.mgr._server_newer_status[rig.key()]) == before


class TestNegotiateDryRun:
    def test_a_missing_copy_writes_and_creates_nothing(self, mocker):
        rig = LocalRig(mocker, dry_run=True)
        assert rig.negotiate() is None
        assert rig.host.of('create') == [] and rig.host.of('set') == []
        assert rig.host.kinds() == ['exists']

    def test_an_existing_copy_is_checked_and_listed_but_not_written(self, mocker):
        rig = LocalRig(mocker, dry_run=True)
        rig.owned_copy(copy_rows(S1))
        assert rig.negotiate() == S1
        assert rig.host.of('set') == [] and rig.host.of('create') == []
        assert rig.host.of('list-snaps') != []

    def test_the_ownership_check_still_runs(self, mocker):
        rig = LocalRig(mocker, dry_run=True)
        rig.host.add(COPY, props={})
        with pytest.raises(NegotiationFailed, match='not recorded as a backup'):
            rig.negotiate()


class TestNegotiateFailures:
    def test_a_zfs_timeout_is_a_retryable_refusal_marked_as_such(self, rig):
        rig.host.fail_on('exists', ZFSTimeoutError('slow'), name=COPY)
        with pytest.raises(NegotiationFailed) as raised:
            rig.negotiate()
        assert raised.value.zfs_timeout is True and raised.value.reloading is True
        assert str(raised.value) == 'local copy is busy: zfs timed out'

    def test_a_timeout_is_one_warning_a_note_and_the_last_problem(self, rig, caplog):
        rig.host.fail_on('exists', ZFSTimeoutError('slow'), name=COPY)
        for _ in range(3):
            with pytest.raises(NegotiationFailed):
                rig.negotiate()
        (warning,) = messages(caplog, logging.WARNING)
        assert warning == (
            f'{DS} -> {DEST}: zfs did not answer within 60 s on this host; skipping this '
            'destination this cycle (a hung or suspended pool leaves zfs processes in '
            'uninterruptible sleep)'
        )
        assert rig.mgr.last_problem(DS, DEST) == (
            f'skipped: zfs did not answer in time on this host (negotiating {COPY})')

    def test_an_expired_budget_is_the_same(self, rig, mocker):
        mocker.patch.object(local, 'LOCAL_ZFS_BUDGET', 0.0)
        with pytest.raises(NegotiationFailed) as raised:
            rig.negotiate()
        assert raised.value.zfs_timeout is True
        assert rig.host.events == []

    def test_a_timeout_while_listing_is_not_mistaken_for_an_untrusted_listing(self, rig):
        rig.owned_copy(copy_rows(S1))
        rig.host.fail_on('list-snaps', ZFSTimeoutError('slow'))
        with pytest.raises(NegotiationFailed) as raised:
            rig.negotiate()
        assert raised.value.zfs_timeout is True
        assert [e[2] for e in rig.host.of('list-snaps')] == [True], "no lenient re-list after a timeout"

    def test_an_answer_after_a_timeout_logs_the_recovery(self, rig, caplog):
        rig.host.fail_on('exists', ZFSTimeoutError('slow'), name=COPY)
        with pytest.raises(NegotiationFailed):
            rig.negotiate()
        rig.host._fail.clear()
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            rig.negotiate()
        assert messages(caplog, logging.INFO) == [f'{DS} -> {DEST}: zfs answers again on this host']

    def test_another_failure_is_an_error_and_a_plain_refusal(self, rig, caplog):
        rig.host.fail_on('exists', RuntimeError('boom'), name=COPY)
        with pytest.raises(NegotiationFailed) as raised:
            rig.negotiate()
        assert str(raised.value) == 'boom'
        assert raised.value.reloading is False and raised.value.zfs_timeout is False
        assert messages(caplog, logging.ERROR) == [f'Negotiation failed for {DS} at local {TARGET}: boom']

    def test_a_failure_creating_an_ancestor_stops_there(self, rig):
        rig.host.fail_on('create', RuntimeError('no space'), name=PARENT)
        with pytest.raises(NegotiationFailed, match='no space'):
            rig.negotiate()
        assert [c[1] for c in rig.host.of('create')] == [BASE, PARENT]
        assert PARENT not in rig.host.datasets and COPY not in rig.mgr._pending

    def test_a_failed_set_is_a_refusal(self, rig):
        rig.owned_copy(copy_rows(S1))
        rig.host.fail_on('set', RuntimeError('read-only pool'))
        with pytest.raises(NegotiationFailed, match='read-only pool'):
            rig.negotiate()
        assert rig.key() not in rig.mgr._server_info


# ---------------------------------------------------------------------------------------------
# _fetch_resume_token
# ---------------------------------------------------------------------------------------------


class TestFetchResumeToken:
    def fetch(self, rig):
        return rig.mgr._fetch_resume_token(rig.transport, CLIENT, rig.dsi, DEST)

    def test_a_token(self, rig):
        rig.owned_copy(copy_rows(S1), token='1-abc-def')
        assert self.fetch(rig) == ('token', '1-abc-def')

    def test_none_for_a_copy_without_one(self, rig):
        rig.owned_copy(copy_rows(S1))
        assert self.fetch(rig) == ('none', None)

    def test_none_for_a_copy_that_does_not_exist(self, rig):
        assert self.fetch(rig) == ('none', None)

    def test_the_ownership_is_checked_first_and_a_foreign_copy_is_skipped_with_one_error(self, rig, caplog):
        rig.host.add(COPY, props={PROP_CLIENT_ID: OTHER_CLIENT}, token='1-abc')
        for _ in range(2):
            assert self.fetch(rig) == ('skip', None)
        (error,) = messages(caplog, logging.ERROR)
        assert "another host's backup" in error
        assert rig.host.of('token') == [], "the token of a copy that is not ours is never read"

    def test_a_bad_dataset_name_is_skipped(self, rig):
        bad = SimpleNamespace(name='pool/a@b', config=rig.dsi.config)
        assert rig.mgr._fetch_resume_token(rig.transport, CLIENT, bad, DEST) == ('skip', None)
        assert rig.host.events == []

    def test_a_timeout_is_a_skip_counted_and_noted(self, rig, caplog):
        rig.owned_copy(copy_rows(S1), token='1-abc')
        rig.host.fail_on('token', ZFSTimeoutError('slow'))
        assert self.fetch(rig) == ('skip', None)
        assert rig.mgr._token_skips[rig.key()] == 1
        assert rig.mgr.last_problem(DS, DEST) == (
            'resume token check failed: zfs did not answer in time on this host')
        (warning,) = messages(caplog, logging.WARNING)
        assert 'zfs did not answer within 60 s on this host' in warning

    def test_an_error_is_a_skip_counted_and_noted_with_the_cause(self, rig, caplog):
        rig.owned_copy(copy_rows(S1), token='1-abc')
        rig.host.fail_on('token', RuntimeError('I/O error'))
        assert self.fetch(rig) == ('skip', None)
        assert rig.mgr._token_skips[rig.key()] == 1
        assert rig.mgr.last_problem(DS, DEST) == f'resume token check failed: cannot read the resume token of {COPY}: I/O error'
        (warning,) = messages(caplog, logging.WARNING)
        assert warning.startswith(f'cannot read the resume token of {COPY}: I/O error; skipping this cycle')
        assert 'rather than risk discarding its partial progress with a fresh send' in warning

    def test_the_third_skip_in_a_row_is_an_error_naming_the_local_abort(self, rig, caplog):
        rig.owned_copy(copy_rows(S1), token='1-abc')
        rig.host.fail_on('token', RuntimeError('I/O error'))
        rig.mgr._server_info[rig.key()] = ('plain', COPY)
        for _ in range(chain_module.RESUME_TOKEN_SKIP_ERROR_AFTER):
            self.fetch(rig)
        (error,) = messages(caplog, logging.ERROR)
        assert 'the resume-token check has skipped the last 3 cycles in a row' in error
        assert "If the local copy's partial receive of this dataset is no longer wanted, abort it on this host" in error
        assert f'`zfs receive -A {COPY}`' in error
        assert 'server' not in error

    def test_a_successful_fetch_does_not_reset_the_count_by_itself(self, rig):
        rig.owned_copy(copy_rows(S1))
        rig.mgr._token_skips[rig.key()] = 2
        assert self.fetch(rig) == ('none', None)
        assert rig.mgr._token_skips[rig.key()] == 2, "`_resume_target` calls `_token_ok`, not the fetch"


# ---------------------------------------------------------------------------------------------
# _stream_step
# ---------------------------------------------------------------------------------------------

PAYLOAD = b'ZFS-STREAM-BODY' * 20
HEADER_FULL = make_header('filesystem')
HEADER_VOLUME = make_header('volume')
BASE_SNAP = sinfo(S1)


def header_from(base=BASE_SNAP, kind='filesystem'):
    return incremental_header(base.guid, kind)


class StepRig:
    """`rig` plus a way to run `_stream_step` over fake streams."""

    def __init__(self, rig, streams):
        self.rig, self.streams = rig, streams
        self.send = None
        self.receive = None

    def run(self, data=None, *, mode='fresh', base=None, discard=(), send=None, receive=True,
            running=False, dsi=None):
        rig = self.rig
        if send is None:
            send = self.streams.send(
                HEADER_FULL + PAYLOAD if data is None else data, running=running, keep_open=running,
                events=rig.host.events,
            )
        self.send = send
        if receive is True:
            receive = self.streams.receive(events=rig.host.events)
        if receive:
            rig.host.receives.append(receive)
        self.receive = receive or None
        started = _SendStream(send, mode, True, sinfo(S2), base)
        return rig.mgr._stream_step(
            rig.transport, CLIENT, dsi or rig.dsi, DEST, started, discard=discard)


@pytest.fixture
def step(rig, streams):
    return StepRig(rig, streams)


def phases(host):
    names = ('props', 'send-read', 'type', 'list-snaps', 'token', 'abort', 'start-receive')
    return [e[0] for e in host.events if e[0] in names]


class TestStreamStepHappyPath:
    def test_a_full_stream_into_a_missing_copy(self, rig, step):
        assert step.run() == _OK
        assert step.receive.received() == HEADER_FULL + PAYLOAD

    def test_the_header_bytes_are_fed_first_and_unchanged_then_the_rest(self, rig, step):
        body = bytes(range(256)) * 8
        assert step.run(HEADER_FULL + body) == _OK
        received = step.receive.received()
        assert received[:312] == HEADER_FULL and received[312:] == body

    def test_the_receive_gets_owned_then_forced_properties_and_the_frozen_flags(self, rig, step):
        step.run()
        (call,) = rig.host.start_calls
        assert call['name'] == COPY
        assert list(call['props'].items()) == [
            (PROP_CONFIG, rig.canonical), (PROP_CLIENT_ID, CLIENT), (PROP_SOURCE_DATASET, DS),
            *FILESYSTEM_FORCED.items(),
        ]
        assert call['force'] is False
        assert call['holds'] is False and call['save'] is True and call['mount'] is False

    def test_a_volume_stream_gets_the_volume_set_only(self, rig, step):
        step.run(HEADER_VOLUME + PAYLOAD)
        (call,) = rig.host.start_calls
        assert list(call['props']) == [PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET, *VOLUME_FORCED]

    @pytest.mark.parametrize('force', [False, True])
    def test_the_argv_on_the_wire(self, rig, step, mocker, force):
        """The recorded `receive` keywords become `-o ... [-F] -h -s -u <copy>`."""
        step.run()
        call = dict(rig.host.start_calls[0], force=force)

        argvs = []

        class Popen:
            def __init__(self, cmd, *a, **kw):
                argvs.append(list(cmd))
                self.args, self.pid, self.returncode = cmd, 4242, 0
                self.stdin, self.stdout, self.stderr = MagicMock(), None, MagicMock()
                self.stderr.readline.side_effect = [b'']

            def poll(self):
                return 0

            def wait(self, timeout=None):
                return 0

        mocker.patch('subprocess.Popen', Popen)
        name = call.pop('name')
        props = call.pop('props')
        owned = {k: props[k] for k in (PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET)}
        handle = libzfseasy.receive.filesystem(Filesystem(name), props=props, **call)
        assert argvs[0][1:] == expected_receive_argv(owned, 'filesystem', COPY, force=force)
        del handle

    def test_the_copy_is_not_pending_afterwards(self, rig, step):
        rig.mgr._pending[COPY] = 1.0
        step.run()
        assert COPY not in rig.mgr._pending

    def test_both_handles_are_closed(self, rig, step):
        step.run()
        assert step.send.closes == 1 and step.receive.closes == 1

    def test_an_incremental_stream_into_a_copy_whose_newest_is_the_base(self, rig, step):
        rig.owned_copy(copy_rows(S1))
        assert step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP) == _OK
        (call,) = rig.host.start_calls
        assert call['force'] is True, "the base is the newest: -F discards changes to the head only"

    def test_an_existing_copy_without_snapshots_is_received_with_force(self, rig, step):
        rig.owned_copy()
        assert step.run(mode='resume') == _OK
        assert rig.host.start_calls[0]['force'] is True


class TestStreamStepOrder:
    """ownership -> exists -> header -> declared base -> type -> strict listing -> decide ->
    token -> abort -> `-F` -> receive."""

    def test_the_full_order_for_a_fresh_step_over_a_partial_receive(self, rig, step):
        rig.owned_copy(copy_rows(S1), token='1-partial')
        step.run(header_from() + PAYLOAD, mode='fresh', base=BASE_SNAP)
        assert phases(rig.host) == [
            'props', 'send-read', 'type', 'list-snaps', 'token', 'abort', 'start-receive']

    def test_ownership_and_existence_come_before_the_first_byte_is_read(self, rig, step):
        rig.owned_copy(copy_rows(S1))
        step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP)
        kinds = rig.host.kinds()
        assert kinds[:5] == ['exists', 'props', 'value', 'exists', 'send-read']
        assert rig.host.events[0][1] == COPY

    def test_a_resume_step_never_reads_the_token_or_aborts(self, rig, step):
        rig.owned_copy(copy_rows(S1), token='1-partial')
        step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP)
        assert phases(rig.host) == ['props', 'send-read', 'type', 'list-snaps', 'start-receive']

    def test_the_listing_is_strict(self, rig, step):
        rig.owned_copy(copy_rows(S1))
        step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP)
        assert rig.host.of('list-snaps') == [('list-snaps', COPY, True)]

    def test_a_copy_that_does_not_exist_is_not_inspected_after_the_header(self, rig, step):
        step.run(mode='resume')
        assert phases(rig.host) == ['send-read', 'start-receive']

    def test_the_force_decision_asks_exists_again_only_when_allowed(self, rig, step):
        rig.owned_copy(copy_rows(S1))
        step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP)
        after = rig.host.events[[e[0] for e in rig.host.events].index('send-read'):]
        assert [e[0] for e in after if e[0] == 'exists'] == ['exists']

    def test_when_force_is_not_allowed_exists_is_not_asked_again(self, rig, step):
        step.run(mode='resume')                            # copy missing: decision.force is False
        after = rig.host.events[[e[0] for e in rig.host.events].index('send-read'):]
        assert [e for e in after if e[0] == 'exists'] == []


class TestStreamStepAbort:
    def test_a_fresh_step_with_a_token_aborts_it_once_with_a_warning(self, rig, step, caplog):
        rig.owned_copy(copy_rows(S1), token='1-partial')
        step.run(header_from() + PAYLOAD, mode='fresh', base=BASE_SNAP)
        assert rig.host.of('abort') == [('abort', COPY)]
        assert any(
            m == f'discarding the partial receive state of {COPY}: a fresh stream of {DS} is sent'
            for m in messages(caplog, logging.WARNING))

    def test_a_fresh_step_without_a_token_does_not_abort(self, rig, step):
        rig.owned_copy(copy_rows(S1))
        step.run(header_from() + PAYLOAD, mode='fresh', base=BASE_SNAP)
        assert rig.host.of('abort') == [] and rig.host.of('token') != []

    def test_a_resume_step_does_not_abort(self, rig, step):
        rig.owned_copy(copy_rows(S1), token='1-partial')
        step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP)
        assert rig.host.of('abort') == []

    def test_a_refused_stream_aborts_nothing(self, rig, step):
        rig.owned_copy(copy_rows(S1, 'extra_a'), token='1-partial')
        assert step.run(header_from() + PAYLOAD, mode='fresh', base=BASE_SNAP) == _FAILED
        assert rig.host.of('abort') == [] and rig.host.of('token') == []

    def test_an_abort_that_destroys_a_new_copy_makes_the_receive_unforced(self, rig, step):
        rig.owned_copy(token='1-partial', abort_destroys=True)     # a first receive that was cut
        assert step.run(mode='fresh') == _OK
        assert rig.host.of('abort') != []
        assert rig.host.start_calls[0]['force'] is False, "decided True before the abort; the copy is gone now"

    def test_a_failing_abort_stops_before_any_receive(self, rig, step):
        rig.owned_copy(copy_rows(S1), token='1-partial')
        rig.host.fail_on('abort', RuntimeError('busy'))
        assert step.run(header_from() + PAYLOAD, mode='fresh', base=BASE_SNAP) == _FAILED
        assert rig.host.start_calls == []


class TestStreamStepForce:
    def test_force_when_allowed_and_the_copy_exists(self, rig, step):
        rig.owned_copy(copy_rows(S1))
        step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP)
        assert rig.host.start_calls[0]['force'] is True

    def test_no_force_for_a_copy_that_does_not_exist(self, rig, step):
        step.run(mode='resume')
        assert rig.host.start_calls[0]['force'] is False

    def test_snapshots_newer_than_the_base_that_the_client_allowed_are_rolled_back_with_a_warning(
            self, rig, step, caplog):
        rig.owned_copy(copy_rows(S1, 'extra_a'))
        newer_guid = default_guid('extra_a')
        assert step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP,
                        discard=(str(newer_guid),)) == _OK
        assert rig.host.start_calls[0]['force'] is True
        (warning,) = messages(caplog, logging.WARNING)
        assert warning == (
            f'{DS} -> {DEST}: the receive destroys 1 snapshot(s) of {COPY} newer than its '
            'incremental source, which this host no longer has: extra_a'
        )

    def test_an_unallowed_newer_snapshot_refuses_and_starts_nothing(self, rig, step):
        rig.owned_copy(copy_rows(S1, 'extra_a'))
        assert step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP,
                        discard=('12345',)) == _FAILED
        assert rig.host.start_calls == []


class TestStreamStepRefusals:
    """Each reason: `_FAILED`, one ERROR under `[local refusal]`, nothing received, the send
    reaped."""

    REFUSALS = {}

    def refuse(self, rig, step, reason):
        """Arrange `reason`; -> the kwargs for `step.run`."""
        if reason == 'stream_header':
            return dict(data=b'\0' * 400)
        if reason == 'compound':
            return dict(data=make_header(hdrtype=2) + PAYLOAD)
        if reason == 'base_mismatch':
            rig.owned_copy(copy_rows(S1))
            return dict(data=incremental_header(default_guid(S1) + 1) + PAYLOAD, base=BASE_SNAP, mode='resume')
        if reason == 'base_mismatch_full_declared_incremental':
            return dict(data=HEADER_FULL + PAYLOAD, base=BASE_SNAP, mode='resume')
        if reason == 'type_mismatch':
            rig.owned_copy(copy_rows(S1), type='volume')
            return dict(data=header_from() + PAYLOAD, base=BASE_SNAP, mode='resume')
        if reason == 'listing':
            rig.owned_copy(copy_rows(S1))
            rig.host.untrusted.add(COPY)
            return dict(data=header_from() + PAYLOAD, base=BASE_SNAP, mode='resume')
        if reason == 'listing_no_txg':
            rig.owned_copy([(S1, 100, default_guid(S1)), ('extra_a', None, 7)])
            return dict(data=header_from() + PAYLOAD, base=BASE_SNAP, mode='resume')
        if reason == 'server_newer':
            rig.owned_copy(copy_rows(S1, 'extra_a'))
            return dict(data=header_from() + PAYLOAD, base=BASE_SNAP, mode='resume')
        if reason == 'base_missing':
            rig.owned_copy(copy_rows('other_20200101000000'))
            return dict(data=header_from() + PAYLOAD, base=BASE_SNAP, mode='resume')
        if reason == 'no_common':
            rig.owned_copy(copy_rows(S1))
            return dict(data=HEADER_FULL + PAYLOAD, mode='fresh')
        if reason == 'name_only_base_guid':
            rig.owned_copy(copy_rows(S1))
            named = types.SimpleNamespace(name=S1, guid=None)
            return dict(data=incremental_header(default_guid(S1) + 5) + PAYLOAD, base=named, mode='resume')
        raise AssertionError(reason)

    REASON_OF = {
        'stream_header': 'stream_header', 'compound': 'stream_header',
        'base_mismatch': 'base_mismatch', 'base_mismatch_full_declared_incremental': 'base_mismatch',
        'type_mismatch': 'type_mismatch', 'listing': 'listing', 'listing_no_txg': 'listing',
        'server_newer': 'server_newer', 'base_missing': 'base_missing', 'no_common': 'no_common',
        'name_only_base_guid': 'base_mismatch',
    }

    #: A bad header is judged on the first 312 bytes while the send is still running in real life; the
    #: fake send for these cases has ended (see `test_a_bad_header_from_a_send_that_is_still_running...`).
    HEADER_CASES = ('stream_header', 'compound')

    @pytest.mark.parametrize('case', list(REASON_OF))
    def test_failed_logged_once_nothing_received_and_the_send_reaped(self, rig, step, caplog, case):
        kwargs = self.refuse(rig, step, case)
        running = case not in self.HEADER_CASES
        kwargs['running'] = running
        assert step.run(**kwargs) == _FAILED
        reason = self.REASON_OF[case]
        (error,) = messages(caplog, logging.ERROR)
        assert error.startswith(f'{DS} -> {DEST}: nothing received ({reason}): ')
        assert rig.host.start_calls == []
        assert step.send.closes == 1, "the started send is reaped"
        assert step.send.terminated is running, "a send still running is stopped first"
        assert (f'{DS} [local refusal]', DEST) in rig.mgr._refusal_logged

    @pytest.mark.parametrize('case', list(REASON_OF))
    def test_a_standing_refusal_is_logged_once_per_episode(self, rig, streams, caplog, case):
        for _ in range(2):
            rig.host.datasets.clear()
            rig.host.add(TARGET)
            step = StepRig(rig, streams)
            kwargs = self.refuse(rig, step, case)
            kwargs['running'] = case not in self.HEADER_CASES
            assert step.run(**kwargs) == _FAILED
        assert len(messages(caplog, logging.ERROR)) == 1

    @pytest.mark.parametrize('data', [b'\0' * 400, make_header(hdrtype=2) + PAYLOAD], ids=['bad-magic', 'compound'])
    def test_a_bad_header_from_a_send_that_is_still_running_is_a_stream_header_refusal(
            self, rig, step, caplog, data):
        """A stream refused on its header while `zfs send` is still writing: the send is stopped by
        us, and that stop must not be reported as the send's own failure."""
        assert step.run(data, running=True) == _FAILED
        (error,) = messages(caplog, logging.ERROR)
        assert 'nothing received (stream_header)' in error and 'SIGTERM' not in error
        assert step.send.terminated and step.send.closes == 1

    def test_the_hints(self, rig, step, caplog):
        step.run(data=b'\0' * 400)
        assert messages(caplog, logging.ERROR)[0].endswith(
            ' (zfs send produced a stream this daemon does not accept: a bug; report it with '
            '`zfsbackupd --version`)')

    def test_the_base_mismatch_hint(self, rig, step, caplog):
        self.refuse(rig, step, 'base_mismatch')
        step.run(**self.refuse(rig, step, 'base_mismatch'))
        assert messages(caplog, logging.ERROR)[0].endswith(
            ' (the stream is not incremental from the snapshot this daemon chose: a bug; report '
            'it with `zfsbackupd --version`)')

    def test_the_type_mismatch_message_and_hint(self, rig, step, caplog):
        step.run(**self.refuse(rig, step, 'type_mismatch'))
        assert messages(caplog, logging.ERROR)[0] == (
            f'{DS} -> {DEST}: nothing received (type_mismatch): {COPY} is a volume on this host and '
            'the stream is a filesystem, so nothing is received (this host holds a dataset of the '
            'other kind under that name: `zfs rename` it to a name outside the local target; the '
            'next cycle sends a full stream)'
        )

    def test_the_compound_stream_message(self, rig, step, caplog):
        step.run(**self.refuse(rig, step, 'compound'))
        assert 'compound streams (zfs send -p, -R or -I) are not accepted' in messages(caplog, logging.ERROR)[0]

    def test_the_listing_messages(self, rig, step, caplog):
        step.run(**self.refuse(rig, step, 'listing'))
        assert f'cannot list the snapshots of {COPY} (cannot trust the snapshot listing), so nothing is received' in \
            messages(caplog, logging.ERROR)[0]

    def test_a_snapshot_without_createtxg_cannot_be_ordered(self, rig, step, caplog):
        step.run(**self.refuse(rig, step, 'listing_no_txg'))
        assert f'cannot order the snapshots of {COPY}: a server snapshot has no createtxg, so nothing is received' in \
            messages(caplog, logging.ERROR)[0]

    def test_the_name_only_base_must_match_the_copys_guid(self, rig, step, caplog):
        step.run(**self.refuse(rig, step, 'name_only_base_guid'))
        assert f'{COPY} holds {S1!r} with guid {default_guid(S1)}, but the stream\'s source guid is ' \
               f'{default_guid(S1) + 5}, so nothing is received' in messages(caplog, logging.ERROR)[0]

    def test_a_name_only_base_that_matches_is_received(self, rig, step):
        rig.owned_copy(copy_rows(S1))
        named = types.SimpleNamespace(name=S1, guid=None)
        assert step.run(header_from() + PAYLOAD, base=named, mode='resume') == _OK

    def test_the_decide_receive_messages_are_worded_for_this_host(self, rig, step, caplog):
        step.run(**self.refuse(rig, step, 'server_newer'))
        message = messages(caplog, logging.ERROR)[0]
        assert 'that this host did not allow to be destroyed' in message
        assert 'To keep them, on this host:' in message
        assert "see README 'Local destinations'" in message
        assert 'the server' not in message and 'the client' not in message

    def test_the_base_missing_message_is_worded_for_this_host(self, rig, step, caplog):
        step.run(**self.refuse(rig, step, 'base_missing'))
        message = messages(caplog, logging.ERROR)[0]
        assert "this host's pruner may have removed it" in message
        assert 'on this host: `zfs rename' in message

    def test_a_completed_step_clears_the_refusals_so_the_next_one_logs_again(
            self, rig, streams, caplog, snapshot_presence):
        step = StepRig(rig, streams)
        step.run(**self.refuse(rig, step, 'server_newer'), running=True)
        key = (f'{DS} [local refusal]', DEST)
        assert key in rig.mgr._refusal_logged
        started = _SendStream(FakeSend(), 'fresh', True, sinfo(S2), BASE_SNAP)
        assert rig.mgr._record_step(rig.transport, rig.dsi, DEST, LISTING, started) == _OK
        assert key not in rig.mgr._refusal_logged
        rig.host.datasets.clear()
        rig.host.add(TARGET)
        step2 = StepRig(rig, streams)
        step2.run(**self.refuse(rig, step2, 'server_newer'), running=True)
        assert len(messages(caplog, logging.ERROR)) == 2

    def test_a_refusal_is_the_last_problem(self, rig, step):
        step.run(**self.refuse(rig, step, 'type_mismatch'))
        assert 'type_mismatch' in rig.mgr.last_problem(DS, DEST)


class TestStreamStepOwnershipAndErrors:
    def test_an_unowned_copy_is_refused_before_a_byte_is_read(self, rig, step, caplog):
        rig.host.add(COPY, props={})
        assert step.run(running=True) == _FAILED
        assert 'send-read' not in rig.host.kinds()
        (error,) = messages(caplog, logging.ERROR)
        assert 'not recorded as a backup' in error
        assert rig.host.start_calls == []
        assert step.send.closes == 1 and step.send.terminated

    def test_a_copy_of_another_client_is_refused(self, rig, step):
        rig.host.add(COPY, props={PROP_CLIENT_ID: OTHER_CLIENT})
        assert step.run() == _FAILED

    def test_the_existence_is_asked_once_and_reused(self, rig, step):
        rig.owned_copy(copy_rows(S1))
        step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP)
        before_header = rig.host.events[:[e[0] for e in rig.host.events].index('send-read')]
        assert len([e for e in before_header if e[0] == 'exists']) == 2, "ownership's own + the one reused"

    def test_a_timeout_checking_the_copy_stops_the_destination(self, rig, step, caplog):
        rig.host.fail_on('exists', ZFSTimeoutError('slow'), name=COPY)
        assert step.run(running=True) == _STOP
        assert 'send-read' not in rig.host.kinds()
        (warning,) = messages(caplog, logging.WARNING)
        assert 'zfs did not answer within 60 s on this host' in warning
        assert rig.mgr.last_problem(DS, DEST) == f'skipped: zfs did not answer in time on this host (checking {COPY})'
        assert step.send.closes == 1 and step.send.terminated

    def test_a_timeout_preparing_the_receive_stops_the_destination(self, rig, step, caplog):
        rig.owned_copy(copy_rows(S1))
        rig.host.fail_on('list-snaps', ZFSTimeoutError('slow'))
        assert step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP, running=True) == _STOP
        assert rig.host.start_calls == []
        assert f'preparing the receive into {COPY}' in rig.mgr.last_problem(DS, DEST)
        assert step.send.closes == 1 and step.send.terminated

    def test_a_timeout_in_the_abort_is_a_stop_too(self, rig, step):
        rig.owned_copy(copy_rows(S1), token='1-x')
        rig.host.fail_on('abort', ZFSTimeoutError('slow'))
        assert step.run(header_from() + PAYLOAD, mode='fresh', base=BASE_SNAP, running=True) == _STOP
        assert rig.host.start_calls == []

    def test_an_answer_after_a_timeout_logs_the_recovery(self, rig, streams, caplog):
        rig.host.fail_on('exists', ZFSTimeoutError('slow'), name=COPY)
        step = StepRig(rig, streams)
        assert step.run(running=True) == _STOP
        rig.host._fail.clear()
        step2 = StepRig(rig, streams)
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert step2.run() == _OK
        assert messages(caplog, logging.INFO)[0] == f'{DS} -> {DEST}: zfs answers again on this host'

    def test_a_failure_checking_the_copy_is_an_error(self, rig, step, caplog):
        rig.host.fail_on('exists', RuntimeError('I/O'), name=COPY)
        assert step.run(running=True) == _FAILED
        assert messages(caplog, logging.ERROR) == [
            f'Transfer failed for {DS} -> local {TARGET}: cannot check {COPY}: I/O']
        assert step.send.closes == 1 and step.send.terminated

    def test_a_failure_preparing_the_receive_is_an_error(self, rig, step, caplog):
        rig.owned_copy(copy_rows(S1))
        rig.host.fail_on('type', RuntimeError('I/O'))
        assert step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP, running=True) == _FAILED
        assert messages(caplog, logging.ERROR) == [
            f'Transfer failed for {DS} -> local {TARGET}: cannot prepare the receive into {COPY}: I/O']
        assert step.send.closes == 1 and step.send.terminated

    def test_a_receive_that_cannot_start_is_an_error_and_the_send_is_reaped(self, rig, step, caplog):
        rig.host.start_error = RuntimeError('no such binary')
        assert step.run(receive=False, running=True) == _FAILED
        assert messages(caplog, logging.ERROR) == [
            f'Transfer failed for {DS} -> local {TARGET}: cannot start zfs receive into {COPY}: no such binary']
        assert step.send.closes == 1 and step.send.terminated

    def test_a_bad_dataset_name_fails_and_reaps_the_send(self, rig, step):
        bad = SimpleNamespace(name='pool/a@b', config=rig.dsi.config)
        assert step.run(dsi=bad, running=True) == _FAILED
        assert step.send.closes == 1 and step.send.terminated

    def test_a_config_that_cannot_be_stored_fails_and_reaps_the_send(self, rig, step, mocker, caplog):
        mocker.patch.object(receiver, 'canonical_dataset_config', side_effect=ValueError('too large'))
        assert step.run(running=True) == _FAILED
        assert f'{DS} -> {DEST}: cannot store its configuration (too large)' in messages(caplog, logging.ERROR)
        assert step.send.closes == 1 and step.send.terminated


class TestStreamStepShortOrFailedSends:
    def test_a_send_that_failed_before_the_header_reports_its_own_error(self, rig, step, caplog):
        send = step.streams.send(b'abc', rc=1, stderr='cannot send: dataset is busy', events=rig.host.events)
        assert step.run(send=send) == _FAILED
        (error,) = messages(caplog, logging.ERROR)
        assert error == (
            f'Transfer failed for {DS} -> local {TARGET}: zfs send failed: cannot send: dataset is busy')
        assert not any('nothing received' in m for m in messages(caplog, logging.ERROR))
        assert (f'{DS} [local refusal]', DEST) not in rig.mgr._refusal_logged
        assert rig.host.start_calls == []

    def test_a_clean_but_short_stream_is_a_stream_header_refusal(self, rig, step, caplog):
        send = step.streams.send(b'abc', rc=0, events=rig.host.events)
        assert step.run(send=send) == _FAILED
        (error,) = messages(caplog, logging.ERROR)
        assert 'nothing received (stream_header): the stream ended after 3 bytes, before its 312-byte header' in error

    def test_a_send_that_produces_nothing_for_the_stall_bound_is_an_error_and_is_stopped(
            self, rig, step, caplog, mocker):
        mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 0.15)
        mocker.patch.object(local, 'PUMP_SLICE', 0.02)
        send = step.streams.send(b'x' * 100, running=True, keep_open=True, events=rig.host.events)
        assert step.run(send=send) == _FAILED
        (error,) = messages(caplog, logging.ERROR)
        assert error.startswith(f'Transfer failed for {DS} -> local {TARGET}: zfs send produced no data for 0.15 s; ')
        assert send.terminated and send.closes == 1


class TestStreamStepOutcomes:
    """What the two exit statuses and the pump's outcome become (the pump is real; the exits are
    scripted)."""

    def test_a_receive_that_fails_reports_its_stderr_and_stops_the_send(self, rig, step, caplog):
        receive = step.streams.receive(dead_rc=1, stderr='cannot receive: invalid stream', events=rig.host.events)
        send = step.streams.send(HEADER_FULL + PAYLOAD, running=True, keep_open=True, events=rig.host.events)
        assert step.run(send=send, receive=receive) == _FAILED
        assert messages(caplog, logging.ERROR) == [
            f'Transfer failed for {DS} -> local {TARGET}: zfs receive into {COPY} failed: '
            'cannot receive: invalid stream (zfs send: Command killed by signal SIGTERM)']
        assert send.terminated
        assert COPY not in rig.mgr._pending

    def test_a_send_that_fails_after_the_header_reports_its_stderr(self, rig, step, caplog):
        send = step.streams.send(HEADER_FULL + PAYLOAD, rc=2, stderr='send exploded', events=rig.host.events)
        assert step.run(send=send) == _FAILED
        assert messages(caplog, logging.ERROR) == [
            f'Transfer failed for {DS} -> local {TARGET}: zfs send failed: send exploded']
        assert step.receive.received() == HEADER_FULL + PAYLOAD, "the receive got what was sent, then EOF"

    def test_both_failing_reports_both(self, rig, step, caplog):
        receive = step.streams.receive(rc=1, stderr='receive says no', events=rig.host.events)
        send = step.streams.send(HEADER_FULL + PAYLOAD, rc=2, stderr='send exploded', events=rig.host.events)
        assert step.run(send=send, receive=receive) == _FAILED
        (error,) = messages(caplog, logging.ERROR)
        assert 'zfs send failed: send exploded; zfs receive into' in error and 'receive says no' in error

    def test_a_receive_that_fails_at_the_end_is_a_failure(self, rig, step, caplog):
        receive = step.streams.receive(rc=1, stderr='checksum mismatch', events=rig.host.events)
        assert step.run(receive=receive) == _FAILED
        assert 'zfs receive into' in messages(caplog, logging.ERROR)[0]
        assert 'checksum mismatch' in messages(caplog, logging.ERROR)[0]

    def test_a_success_keeps_a_pending_entry_out(self, rig, step):
        rig.mgr._pending[COPY] = 5.0
        assert step.run() == _OK
        assert COPY not in rig.mgr._pending

    def test_a_failure_keeps_the_pending_entry(self, rig, step):
        rig.mgr._pending[COPY] = 5.0
        receive = step.streams.receive(rc=1, stderr='no', events=rig.host.events)
        assert step.run(receive=receive) == _FAILED
        assert rig.mgr._pending.get(COPY) == 5.0


HEADER_RAW = make_header('filesystem', versioninfo=0x5080011)          # a raw filesystem (2.1.5)
HEADER_RAW_VOLUME = make_header('volume', versioninfo=0x5080001)       # a raw volume


class TestStepEnforcesForcedProperties:
    """OpenZFS 2.1 skips `-o sharesmb=off` on a RAW receive and exits 0: after a step whose receive
    committed, `_enforce_forced_properties` reads which forced properties are local and `zfs set`s the
    others, under a fresh `_Budget`. It never fails the step."""

    COPY_FS = Filesystem(COPY)
    DID_NOT_APPLY = f'{DS} -> {DEST}: zfs receive did not apply %s to {COPY}; set it after the receive'
    CANNOT = (f'{DS} -> {DEST}: cannot make the forced properties of {COPY} local after the '
              'receive; the next receive tries again')
    AGAIN = f'{DS} -> {DEST}: the forced properties of {COPY} are local again'

    def committed(self, rig, step, props, header=HEADER_RAW, **kw):
        """A step whose receive creates the copy with `props` as its local properties."""
        def add():
            rig.host.add_copy(props={**owned_props(config=rig.canonical), **props})

        receive = step.streams.receive(events=rig.host.events, on_commit=add)
        return step.run(header + PAYLOAD, receive=receive, **kw)

    # -- the three outcomes ------------------------------------------------------------------------

    def test_every_forced_property_local_reads_once_and_writes_nothing(self, rig, step, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            assert self.committed(rig, step, FILESYSTEM_FORCED) == _OK
        assert rig.host.of('sources') == [('sources', COPY, 'local')]
        assert rig.host.of('set') == []
        assert [r for r in records(caplog) if r[0] in ('INFO', 'WARNING', 'ERROR')] == []

    def test_sharesmb_missing_is_set_to_off_and_nothing_else(self, rig, step, caplog):
        props = {k: v for k, v in FILESYSTEM_FORCED.items() if k != 'sharesmb'}
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert self.committed(rig, step, props) == _OK
        assert rig.host.of('set') == [('set', COPY, {'sharesmb': 'off'})]
        assert rig.host.datasets[COPY].props['sharesmb'] == 'off'
        assert messages(caplog, logging.INFO) == [self.DID_NOT_APPLY % 'sharesmb=off']
        assert messages(caplog, logging.WARNING) == [] and messages(caplog, logging.ERROR) == []

    def test_several_missing_are_set_in_one_command(self, rig, step, caplog):
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert self.committed(rig, step, {'canmount': 'noauto'}) == _OK
        (event,) = rig.host.of('set')
        assert event[2] == {k: v for k, v in FILESYSTEM_FORCED.items() if k != 'canmount'}
        (info,) = messages(caplog, logging.INFO)
        assert 'readonly=on' in info and 'sharesmb=off' in info and 'canmount' not in info

    def test_the_read_follows_the_receive_and_the_write_the_read(self, rig, step):
        self.committed(rig, step, {})
        kinds = rig.host.kinds('start-receive', 'sources', 'set')
        assert kinds == ['start-receive', 'sources', 'set']

    @pytest.mark.parametrize('exc', [RuntimeError('zfs said no'), ZFSTimeoutError('slow'), OSError('gone')])
    def test_a_failing_read_is_a_warning_and_the_step_succeeds(self, rig, step, caplog, exc):
        rig.host.fail_on('sources', exc)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            assert self.committed(rig, step, {}) == _OK
        assert messages(caplog, logging.WARNING) == [self.CANNOT]
        assert messages(caplog, logging.ERROR) == []
        assert rig.host.of('set') == []
        assert any(str(exc) in m for m in messages(caplog, logging.DEBUG))
        assert COPY not in rig.mgr._pending

    @pytest.mark.parametrize('exc', [RuntimeError('permission denied'), ZFSTimeoutError('slow')])
    def test_a_failing_set_is_a_warning_and_the_step_succeeds(self, rig, step, caplog, exc):
        rig.host.fail_on('set', exc)
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert self.committed(rig, step, {}) == _OK
        assert messages(caplog, logging.WARNING) == [self.CANNOT]
        assert not [m for m in messages(caplog, logging.INFO) if 'did not apply' in m]

    def test_the_warning_is_given_once_per_episode_and_recovery_is_announced(self, rig, step, caplog):
        rig.host.fail_on('sources', RuntimeError('x'))
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            for _ in range(3):
                assert self.committed(rig, step, FILESYSTEM_FORCED) == _OK
            mine = [(lvl, m) for lvl, m in records(caplog) if m == self.CANNOT]
            assert [lvl for lvl, _ in mine] == ['WARNING', 'DEBUG', 'DEBUG']
            caplog.clear()
            rig.host._fail.clear()
            assert self.committed(rig, step, FILESYSTEM_FORCED) == _OK
            assert messages(caplog, logging.INFO) == [self.AGAIN]
            caplog.clear()
            assert self.committed(rig, step, FILESYSTEM_FORCED) == _OK
            assert messages(caplog, logging.INFO) == []
            rig.host.fail_on('sources', RuntimeError('x'))
            assert self.committed(rig, step, FILESYSTEM_FORCED) == _OK
            assert messages(caplog, logging.WARNING) == [self.CANNOT]       # a new episode

    def test_a_standing_enforcement_warning_is_never_reported_as_why_a_send_failed(self, rig, step):
        rig.host.fail_on('sources', RuntimeError('x'))
        assert self.committed(rig, step, {}) == _OK
        assert rig.mgr.last_problem(DS, DEST) is None

    def test_an_episode_is_per_dataset_and_destination(self, rig, step, caplog):
        rig.host.fail_on('sources', RuntimeError('x'))
        other = rig.dsi_for('pool/other')
        with caplog.at_level(logging.WARNING, logger='zfsbackup.local'):
            self.committed(rig, step, {})
            rig.mgr._enforce_forced_properties(Filesystem(f'{BASE}/pool/other'),
                                               parse_header(HEADER_RAW), other, DEST)
        assert len(messages(caplog, logging.WARNING)) == 2

    # -- a step that did not commit ----------------------------------------------------------------

    def test_a_failed_receive_enforces_nothing(self, rig, step):
        receive = step.streams.receive(rc=1, stderr='cannot receive: no space', events=rig.host.events)
        assert step.run(HEADER_RAW + PAYLOAD, receive=receive) == _FAILED
        assert rig.host.of('sources') == [] and rig.host.of('set') == []

    def test_a_dead_receive_enforces_nothing(self, rig, step):
        receive = step.streams.receive(dead_rc=1, stderr='cannot receive', events=rig.host.events)
        assert step.run(HEADER_RAW + PAYLOAD, receive=receive) == _FAILED
        assert rig.host.of('sources') == []

    def test_a_failed_send_enforces_nothing(self, rig, step):
        send = step.streams.send(HEADER_RAW + PAYLOAD, rc=2, stderr='boom', events=rig.host.events)
        assert step.run(send=send) == _FAILED
        assert rig.host.of('sources') == []

    def test_a_receive_that_does_not_commit_enforces_nothing(self, rig, step, mocker):
        mocker.patch.object(local, 'LOCAL_COMMIT_TIMEOUT', 0.2)
        receive = step.streams.receive(hang=True, events=rig.host.events)
        assert step.run(HEADER_RAW + PAYLOAD, receive=receive) != _OK
        assert rig.host.of('sources') == [] and rig.host.of('set') == []

    def test_a_refused_stream_enforces_nothing(self, rig, step):
        """A header the checks refuse (a compound stream) never reaches a receive."""
        rig.owned_copy(copy_rows(S1))
        bad = make_header('filesystem', versioninfo=0x5080010 | 2)
        assert step.run(bad + PAYLOAD, mode='resume') == _FAILED
        assert rig.host.start_calls == [] and rig.host.of('sources') == []

    def test_a_receive_that_cannot_start_enforces_nothing(self, rig, step):
        rig.host.start_error = RuntimeError('cannot receive: busy')
        assert step.run(HEADER_RAW + PAYLOAD, receive=False) == _FAILED
        assert rig.host.of('sources') == []

    # -- plain streams -----------------------------------------------------------------------------

    @pytest.mark.parametrize('header', [HEADER_FULL, HEADER_VOLUME], ids=['filesystem', 'volume'])
    def test_a_plain_stream_runs_no_zfs_command_afterwards(self, rig, step, header):
        rig.host.fail_on('sources', RuntimeError('never reached'))
        rig.host.fail_on('set', RuntimeError('never reached'))
        assert step.run(header + PAYLOAD) == _OK
        assert rig.host.of('sources') == [] and rig.host.of('set') == []

    # -- raw volume --------------------------------------------------------------------------------

    def test_a_raw_volume_with_its_set_local_writes_nothing(self, rig, step):
        assert self.committed(rig, step, VOLUME_FORCED, HEADER_RAW_VOLUME) == _OK
        assert len(rig.host.of('sources')) == 1 and rig.host.of('set') == []

    def test_a_raw_volume_gets_the_volume_set_only(self, rig, step):
        assert self.committed(rig, step, {'volmode': 'none'}, HEADER_RAW_VOLUME) == _OK
        assert rig.host.of('set') == [('set', COPY, {'readonly': 'on'})]

    def test_a_raw_volume_never_gets_a_filesystem_property(self, rig, step):
        self.committed(rig, step, {}, HEADER_RAW_VOLUME)
        (event,) = rig.host.of('set')
        assert event[2] == VOLUME_FORCED

    # -- the budget --------------------------------------------------------------------------------

    def test_the_commands_run_under_a_libzfseasy_timeout(self, rig, step):
        import contextlib

        @contextlib.contextmanager
        def timeout(seconds):
            rig.host.events.append(('timeout', seconds))
            yield

        rig.lib.timeout = timeout
        self.committed(rig, step, {})
        kinds = [e[0] for e in rig.host.events]
        tail = [k for k in kinds[kinds.index('start-receive'):] if k in ('timeout', 'sources', 'set')]
        assert tail == ['timeout', 'sources', 'timeout', 'set']

    def test_the_budget_is_made_after_the_receive(self, rig, step, mocker):
        real = local._Budget
        events = rig.host.events
        mocker.patch.object(local, '_Budget', lambda *a, **k: (events.append(('budget',)), real(*a, **k))[1])
        self.committed(rig, step, {})
        kinds = [e[0] for e in events]
        assert kinds.index('budget', kinds.index('start-receive')) < kinds.index('sources')

    def test_the_budget_is_fresh_whatever_the_receive_took(self, rig, step, mocker):
        """A budget made before the receive would be spent by the time the receive committed."""
        now = [1000.0]
        real = local._Budget
        mocker.patch.object(local, '_Budget', lambda *a, **k: real(*a, clock=lambda: now[0], **k))

        def add():
            now[0] += 1_000_000
            rig.host.add_copy(props=owned_props(config=rig.canonical))

        receive = step.streams.receive(events=rig.host.events, on_commit=add)
        assert step.run(HEADER_RAW + PAYLOAD, receive=receive) == _OK
        assert len(rig.host.of('sources')) == 1 and len(rig.host.of('set')) == 1


def parse_header(raw):
    return receiver.parse_stream_header(raw)


class TestEnforcementThroughTheChain:
    """`backup_dataset` over a raw stream: the enforcement runs after each committed step and the
    chain does not notice it."""

    def raw_sends(self, rig, streams, mocker):
        sends = Sends(rig, streams, mocker)

        def call(snapshot, since=None, **kwargs):
            sends.calls.append((snapshot.short, since.short if since else None, kwargs))
            header = make_header(
                'filesystem', from_guid=default_guid(since.short) if since else 0,
                to_guid=default_guid(snapshot.short), versioninfo=0x5080011,
            )
            return streams.send(header + PAYLOAD, events=rig.host.events)

        mocker.patch('zfsbackup.chain.zfs.send.snapshot', side_effect=call)
        return sends

    def backup(self, rig):
        return rig.mgr.backup_dataset(rig.dsi, RemoteDatasetConfig(destination=DEST))

    def test_a_first_raw_full_send_sets_the_forced_properties_then_the_anchor_is_written(
            self, rig, streams, mocker, snapshot_presence):
        sends = self.raw_sends(rig, streams, mocker)
        commits(rig, streams, S3)
        assert self.backup(rig) is True
        assert sends.calls and rig.ledger.anchor == S3
        (event,) = rig.host.of('set')
        assert event[1] == COPY and event[2] == FILESYSTEM_FORCED
        kinds = rig.host.kinds('start-receive', 'sources', 'set')
        assert kinds == ['start-receive', 'sources', 'set']

    def test_a_failing_enforcement_does_not_stop_the_chain_or_the_anchor(
            self, rig, streams, mocker, snapshot_presence, caplog):
        self.raw_sends(rig, streams, mocker)
        rig.owned_copy(copy_rows(S1))
        rig.host.datasets[COPY].props.update(FILESYSTEM_FORCED)
        rig.host.fail_on('sources', RuntimeError('x'))
        commits(rig, streams, S2, S3)
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert self.backup(rig) is True
        assert rig.ledger.anchors() == [S2, S3]
        assert len([m for m in messages(caplog, logging.WARNING) if 'forced properties' in m]) == 1

    def test_every_step_of_a_chain_is_checked(self, rig, streams, mocker, snapshot_presence):
        self.raw_sends(rig, streams, mocker)
        rig.owned_copy(copy_rows(S1))
        rig.host.datasets[COPY].props.update(FILESYSTEM_FORCED)
        commits(rig, streams, S2, S3)
        assert self.backup(rig) is True
        assert len(rig.host.of('sources')) == 2
        assert [e for e in rig.host.of('set') if 'sharesmb' in e[2]] == []   # (negotiate's own set aside)


class TestStepOutcomeMessages:
    """`_step_outcome` on its own: every branch."""

    def outcome(self, rig, outcome, send_error=None, receive_error=None, abandoned=(),
                cut_after_eof=False, commit_timed_out=False):
        pump = SimpleNamespace(
            send_error=send_error, receive_error=receive_error, abandoned=list(abandoned),
            send_cut_after_eof=cut_after_eof, commit_timed_out=commit_timed_out,
        )
        return rig.mgr._step_outcome(pump, outcome, rig.dsi, DEST, f'local {TARGET}', COPY)

    def test_ok_only_when_both_children_succeeded(self, rig, caplog):
        assert self.outcome(rig, local.PUMP_OK) == _OK
        assert self.outcome(rig, local.PUMP_OK, send_error='s') == _FAILED
        assert self.outcome(rig, local.PUMP_OK, receive_error='r') == _FAILED

    def test_stalled_names_the_bound_and_the_kept_partial_receive(self, rig, caplog):
        assert self.outcome(rig, local.PUMP_STALLED) == _FAILED
        (error,) = messages(caplog, logging.ERROR)
        assert error == (
            f'Transfer failed for {DS} -> local {TARGET}: no progress for {local.LOCAL_STALL_TIMEOUT:g} s; zfs send and zfs '
            f'receive were stopped, the partial receive into {COPY} is kept for a resume'
        )

    def test_a_commit_that_timed_out_is_its_own_message_and_a_failure(self, rig, caplog):
        assert self.outcome(rig, local.PUMP_STALLED, commit_timed_out=True) == _FAILED
        (error,) = messages(caplog, logging.ERROR)
        assert error.startswith(
            f'Transfer of {DS} -> local {TARGET}: zfs receive into {COPY} did not finish committing '
            f'within {local.LOCAL_COMMIT_TIMEOUT:g} s after the end of the stream; it was stopped, but its final commit cannot '
            'be interrupted, so it may still complete.')
        assert 'no progress' not in error

    def test_a_send_cut_after_its_eof_does_not_fail_a_committed_receive(self, rig, caplog):
        assert self.outcome(
            rig, local.PUMP_OK, send_error='Command killed by signal SIGTERM', cut_after_eof=True) == _OK
        (warning,) = messages(caplog, logging.WARNING)
        assert f'zfs send did not exit within {local.LOCAL_STALL_TIMEOUT:g} s after the end of its stream and was stopped' in warning
        assert f'zfs receive committed {COPY}, so the step counts as done' in warning

    def test_a_send_cut_after_its_eof_with_a_receive_error_is_still_a_failure(self, rig, caplog):
        assert self.outcome(rig, local.PUMP_OK, send_error='killed', receive_error='bad', cut_after_eof=True) == _FAILED

    def test_a_send_error_without_the_cut_flag_is_a_failure(self, rig):
        assert self.outcome(rig, local.PUMP_OK, send_error='killed', cut_after_eof=False) == _FAILED

    def test_abandoned_children_are_listed_and_the_consequence_stated(self, rig, caplog):
        self.outcome(rig, local.PUMP_STALLED, abandoned=[('send', 11), ('receive', 22)])
        (error,) = messages(caplog, logging.ERROR)
        assert error.endswith(
            '; zfs send (pid 11), zfs receive (pid 22) did not exit after SIGKILL (a hung pool?): '
            'timed zfs calls in this worker are refused until it exits, so local destinations are '
            'skipped until then')

    def test_a_send_failure_without_a_receive_error(self, rig, caplog):
        self.outcome(rig, local.PUMP_OK, send_error='boom')
        assert messages(caplog, logging.ERROR) == [f'Transfer failed for {DS} -> local {TARGET}: zfs send failed: boom']

    def test_a_send_failure_with_a_receive_error(self, rig, caplog):
        self.outcome(rig, local.PUMP_SEND_FAILED, send_error='boom', receive_error='truncated')
        assert messages(caplog, logging.ERROR) == [
            f'Transfer failed for {DS} -> local {TARGET}: zfs send failed: boom; zfs receive into {COPY}: truncated']

    def test_receive_gone_with_its_stderr_and_the_stopped_sends(self, rig, caplog):
        self.outcome(rig, local.PUMP_RECEIVE_GONE, send_error='killed', receive_error='bad stream')
        assert messages(caplog, logging.ERROR) == [
            f'Transfer failed for {DS} -> local {TARGET}: zfs receive into {COPY} failed: bad stream (zfs send: killed)']

    def test_receive_gone_without_a_message(self, rig, caplog):
        self.outcome(rig, local.PUMP_RECEIVE_GONE)
        assert messages(caplog, logging.ERROR) == [
            f'Transfer failed for {DS} -> local {TARGET}: zfs receive into {COPY} failed: it stopped '
            'reading before the end of the stream']

    def test_an_ok_pump_with_only_a_receive_error_is_a_receive_failure(self, rig, caplog):
        self.outcome(rig, local.PUMP_OK, receive_error='cannot mount')
        assert 'zfs receive into' in messages(caplog, logging.ERROR)[0]

    def test_the_encrypted_overwrite_text_gets_the_local_advice(self, rig, caplog):
        text = "cannot receive new filesystem stream: zfs receive cannot overwrite an unencrypted one with an encrypted one"
        self.outcome(rig, local.PUMP_RECEIVE_GONE, receive_error=text)
        message = messages(caplog, logging.ERROR)[0]
        assert f'{COPY} already exists on this host and cannot receive a full encrypted stream' in message
        assert '`zfs rename` it aside' in message and text in message

    def test_other_receive_text_is_unchanged(self):
        assert LocalBackupManager._receive_error('plain failure', COPY) == 'plain failure'


# ---------------------------------------------------------------------------------------------
# Through backup_dataset: the pieces together
# ---------------------------------------------------------------------------------------------


class TestBackupDatasetThroughTheLocalTransport:
    def test_no_client_id_sends_nothing(self, mocker, caplog):
        rig = LocalRig(mocker, client_id=None)
        assert rig.mgr.backup_dataset(rig.dsi, SimpleNamespace(destination=DEST, raw=None)) is False
        assert rig.host.events == []
        assert local.NO_CLIENT_ID_MESSAGE in messages(caplog, logging.ERROR)[0]

    def test_a_missing_target_sends_nothing(self, rig):
        rig.host.datasets.pop(TARGET)
        assert rig.mgr.backup_dataset(rig.dsi, SimpleNamespace(destination=DEST, raw=None)) is False
        assert rig.host.of('create') == [] and rig.host.start_calls == []
        assert 'does not exist' in rig.mgr.last_problem(DS, DEST)

    def test_the_chain_uses_the_local_wording_and_logger(self, rig):
        assert rig.mgr._logger.name == 'zfsbackup.local'
        assert rig.mgr._where(rig.transport) == f'local {TARGET}'
        assert rig.mgr._peer(rig.transport) == f'local target {TARGET}'

    def test_a_retryable_negotiate_refusal_reads_in_the_local_words(self, rig, caplog):
        rig.negotiate(dsi=rig.dsi_for('pool'))              # the parent's first backup is pending
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert rig.mgr.backup_dataset(rig.dsi, RemoteDatasetConfig(destination=DEST)) is False
        assert f'local target {TARGET} is busy: waiting for {PARENT}; skipping {DS} this cycle' in \
            messages(caplog, logging.INFO)
        assert rig.host.start_calls == []


# ---------------------------------------------------------------------------------------------
# The target found gone mid-cycle; the send start-up
# ---------------------------------------------------------------------------------------------


class TestTargetVanished:
    """After an unexpected zfs failure the manager asks once whether the target dataset is still
    there (its pool exported or destroyed mid-cycle): if not, one message per episode, and the
    destination is skipped quietly for the rest of the cycle."""

    GONE = ('local destination here: the target dataset bk/backups is gone (its pool exported or '
            'destroyed?); skipping this destination for the rest of this cycle. Import the pool, or '
            'recreate the target (`zfs create -o canmount=off bk/backups`)')

    def test_a_failed_negotiate_with_the_target_gone_is_one_quiet_message(self, rig, caplog):
        rig.host.fail_on('exists', RuntimeError('I/O error'), name=COPY)
        rig.host.datasets.pop(TARGET)
        for _ in range(2):
            with pytest.raises(NegotiationFailed, match='the target dataset bk/backups is gone'):
                rig.negotiate()
        assert messages(caplog, logging.ERROR) == [self.GONE]
        assert rig.mgr.last_problem(DS, DEST) == 'skipped: the target dataset bk/backups is gone'
        assert DEST in rig.mgr._unavailable

    def test_with_the_target_still_there_the_failure_is_reported_as_itself(self, rig, caplog):
        rig.host.fail_on('exists', RuntimeError('I/O error'), name=COPY)
        with pytest.raises(NegotiationFailed, match='I/O error'):
            rig.negotiate()
        assert DEST not in rig.mgr._unavailable
        assert messages(caplog, logging.ERROR) == [f'Negotiation failed for {DS} at local {TARGET}: I/O error']

    def test_a_check_that_itself_fails_is_no_proof_that_the_target_is_gone(self, rig):
        rig.host.fail_on('exists', RuntimeError('I/O error'))
        with pytest.raises(NegotiationFailed, match='I/O error'):
            rig.negotiate()
        assert DEST not in rig.mgr._unavailable

    def test_the_destination_is_skipped_silently_for_the_rest_of_the_cycle(self, rig, caplog):
        rig.mgr.begin_cycle()
        rig.mgr._unavailable.add(DEST)
        assert rig.mgr._ready_transport(DEST, rig.dest) is None
        assert rig.host.events == [] and messages(caplog, logging.ERROR) == []

    def test_a_new_cycle_checks_again(self, rig):
        rig.mgr._unavailable.add(DEST)
        rig.mgr.begin_cycle()
        assert rig.mgr._unavailable == set()
        assert rig.mgr._ready_transport(DEST, rig.dest) is not None

    def test_outside_a_worker_cycle_every_call_checks_again(self, rig):
        rig.mgr._unavailable.add(DEST)
        rig.mgr._before_destination_check()
        assert rig.mgr._unavailable == set()

    def test_a_failed_stream_step_with_the_target_gone_is_a_stop_and_one_message(self, rig, step, caplog):
        rig.owned_copy(copy_rows(S1))
        rig.host.fail_on('type', RuntimeError('I/O error'))
        rig.host.fail_on('list-snaps', RuntimeError('I/O error'))

        def export_the_pool(*_):
            rig.host.datasets.pop(TARGET, None)

        original = rig.lib.get.value
        rig.lib.get.value = lambda *a, **k: (export_the_pool(), original(*a, **k))[1]
        assert step.run(header_from() + PAYLOAD, mode='resume', base=BASE_SNAP, running=True) == _STOP
        assert messages(caplog, logging.ERROR) == [self.GONE]
        assert step.send.closes == 1 and step.send.terminated

    def test_a_receive_that_cannot_start_with_the_target_gone_is_a_stop(self, rig, step, caplog):
        rig.host.start_error = RuntimeError('pool is gone')
        rig.host.datasets.pop(TARGET)
        assert step.run(receive=False) == _STOP
        assert messages(caplog, logging.ERROR) == [self.GONE]

    def test_a_failed_resume_token_read_with_the_target_gone_is_a_quiet_skip(self, rig, caplog):
        rig.owned_copy(copy_rows(S1), token='1-x')
        rig.host.fail_on('token', RuntimeError('I/O'))
        rig.host.datasets.pop(TARGET)
        assert rig.mgr._fetch_resume_token(rig.transport, CLIENT, rig.dsi, DEST) == ('skip', None)
        assert messages(caplog, logging.ERROR) == [self.GONE]
        assert messages(caplog, logging.WARNING) == []


class TestSendStartUp:
    def test_the_local_manager_starts_zfs_send_without_the_blocking_peek(self):
        assert LocalBackupManager._send_peek is False
        assert ChainSender._send_peek is True

    def test_the_peek_flag_reaches_every_send_start(self, rig, mocker, streams):
        starts = mocker.patch('zfsbackup.chain.zfs.send.snapshot', side_effect=lambda *a, **k: streams.send())
        assert rig.mgr._send_kwargs() == {'peek': False}
        rig.mgr._start_fresh(rig.dsi, DEST, None, sinfo(S2), 'plain')
        assert starts.call_args.kwargs == {'large_blocks': True, 'compressed': True, 'peek': False}

    def test_the_remote_start_is_unchanged(self, mocker, streams):
        rig = ChainRig(mocker, listing(S1, S2), common=None)
        assert rig.sender._send_kwargs() == {}
        rig.run()
        assert rig.send_snapshot.call_args.kwargs == {'large_blocks': True, 'compressed': True}


# ---------------------------------------------------------------------------------------------
# End to end through backup_dataset: the chain over the local transport
# ---------------------------------------------------------------------------------------------


class Sends:
    """`zfs.send.snapshot` as the chain sees it: one `FakeSend` per call carrying a header whose
    source guid is the base's, then the payload."""

    def __init__(self, rig, streams, mocker):
        self.rig, self.streams = rig, streams
        self.calls = []
        mocker.patch('zfsbackup.chain.zfs.send.snapshot', side_effect=self)

    def __call__(self, snapshot, since=None, **kwargs):
        self.calls.append((snapshot.short, since.short if since else None, kwargs))
        header = make_header(
            'filesystem', from_guid=default_guid(since.short) if since else 0,
            to_guid=default_guid(snapshot.short),
        )
        return self.streams.send(header + PAYLOAD, events=self.rig.host.events)

    def pairs(self):
        return [(since, name) for name, since, _ in self.calls]


@pytest.fixture
def sends(rig, streams, mocker, snapshot_presence):
    return Sends(rig, streams, mocker)


def commits(rig, streams, *names):
    """Queue one successful receive per name; closing it adds that snapshot to the copy."""
    for name in names:
        def add(name=name):
            ds = rig.host.datasets[COPY] if COPY in rig.host.datasets else rig.host.add(COPY)
            ds.snaps.append(ServerRow(name, 200 + len(ds.snaps), default_guid(name)))
            ds.props.setdefault(PROP_CLIENT_ID, CLIENT)
        rig.host.receives.append(streams.receive(events=rig.host.events, on_commit=add))


class TestEndToEnd:
    def backup(self, rig, **kw):
        return rig.mgr.backup_dataset(rig.dsi, RemoteDatasetConfig(destination=DEST, **kw))

    def test_a_first_full_send_into_a_missing_copy(self, rig, sends, streams, caplog):
        commits(rig, streams, S3)
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert self.backup(rig) is True
        assert sends.calls == [(S3, None, {'large_blocks': True, 'compressed': True, 'peek': False})]
        assert [c[1] for c in rig.host.of('create')] == [BASE, PARENT]
        (call,) = rig.host.start_calls
        assert call['name'] == COPY and call['force'] is False
        assert rig.ledger.anchor == S3 and rig.ledger.pin is None
        assert f'Backup complete: {DS}@{S3} (full) -> local {TARGET}' in messages(caplog, logging.INFO)
        assert rig.host.datasets[COPY].props[PROP_CLIENT_ID] == CLIENT

    def test_the_order_of_everything(self, rig, sends, streams):
        commits(rig, streams, S3)
        self.backup(rig)
        names = [e if isinstance(e, str) else e[0] for e in rig.ledger.events]
        assert names == ['pin', 'anchor', 'clear']
        host = rig.host.kinds('create', 'send-read', 'start-receive')
        assert host == ['create', 'create', 'send-read', 'start-receive']

    def test_an_incremental_chain_one_step_at_a_time_with_force_each_time(self, rig, sends, streams, caplog):
        rig.owned_copy(copy_rows(S1))
        commits(rig, streams, S2, S3)
        with caplog.at_level(logging.INFO, logger='zfsbackup.local'):
            assert self.backup(rig) is True
        assert sends.pairs() == [(S1, S2), (S2, S3)]
        assert [c['force'] for c in rig.host.start_calls] == [True, True]
        assert rig.ledger.anchors() == [S2, S3]
        info = messages(caplog, logging.INFO)
        assert f'Backup complete: {DS}@{S2} (incremental from {S1}) -> local {TARGET}' in info
        assert f'Backup complete: {DS}@{S3} (incremental from {S2}) -> local {TARGET}' in info
        assert rig.host.of('abort') == []

    def test_a_copy_that_is_already_up_to_date_settles_the_anchor_and_sends_nothing(self, rig, sends):
        rig.owned_copy(copy_rows(S1, S2, S3))
        assert self.backup(rig) is True
        assert sends.calls == [] and rig.host.start_calls == []
        assert rig.ledger.anchor == S3

    def test_a_failing_receive_stops_the_chain_and_writes_no_anchor(self, rig, sends, streams):
        rig.owned_copy(copy_rows(S1))
        rig.host.receives.append(streams.receive(rc=1, stderr='cannot receive: no space', events=rig.host.events))
        assert self.backup(rig) is False
        assert sends.pairs() == [(S1, S2)]
        assert rig.ledger.anchors() == [] and rig.ledger.pin == (S1, S2), "the pin stays"

    def test_a_stop_between_steps_pauses_the_chain(self, rig, sends, streams):
        rig.owned_copy(copy_rows(S1))
        commits(rig, streams, S2, S3)
        assert rig.mgr.backup_dataset(
            rig.dsi, RemoteDatasetConfig(destination=DEST), lambda: bool(sends.calls)) is True
        assert sends.pairs() == [(S1, S2)]
        assert rig.ledger.anchors() == [S2]

    def test_a_dry_run_sends_and_writes_nothing(self, mocker, streams, snapshot_presence):
        rig = LocalRig(mocker, dry_run=True)
        sends = Sends(rig, streams, mocker)
        rig.owned_copy(copy_rows(S1))
        assert self.backup(rig) is True
        assert sends.calls == [] and rig.host.start_calls == []
        assert rig.host.of('set') == [] and rig.host.of('create') == []

    def test_no_client_id_sends_nothing(self, mocker, caplog):
        rig = LocalRig(mocker, client_id=None)
        assert self.backup(rig) is False
        assert rig.host.events == []
        assert local.NO_CLIENT_ID_MESSAGE in messages(caplog, logging.ERROR)[0]

    def test_a_missing_target_sends_nothing(self, rig, sends):
        rig.host.datasets.pop(TARGET)
        assert self.backup(rig) is False
        assert sends.calls == [] and rig.host.of('create') == []
        assert 'does not exist' in rig.mgr.last_problem(DS, DEST)


class TestChainWordingOverTheLocalTransport:
    """The chain's messages, worded by the local hooks (the remote ones are `TestHookDefaults`)."""

    def backup(self, rig):
        return rig.mgr.backup_dataset(rig.dsi, RemoteDatasetConfig(destination=DEST))

    def test_snapshots_newer_on_the_copy_are_refused_in_the_local_words(self, rig, sends, caplog):
        rig.owned_copy(copy_rows(S1, 'extra_a'))
        assert self.backup(rig) is False
        assert sends.calls == [] and rig.host.start_calls == []
        (error,) = messages(caplog, logging.ERROR)
        assert error == (
            f"{DS} -> {DEST}: the local copy {COPY} has 1 snapshot(s) newer than {S1} that the "
            "source dataset does not have (extra_a), for example after a rollback or an in-place "
            "restore of this dataset, or snapshots taken in the local copy. Sending would make the "
            "local copy destroy them, so nothing is sent. To keep them, on this host: "
            f"zfs rename {COPY} <a name outside the backup target> (the next cycle sends a full "
            "stream; see README 'Local destinations'). If they are expendable: "
            f"zfs destroy {COPY}@extra_a (the next cycle continues)."
        )
        assert 'Rejoining' not in error

    def test_a_copy_sharing_no_snapshot_is_refused_in_the_local_words(self, rig, sends, caplog):
        rig.owned_copy(copy_rows('other_20200101000000'))
        assert self.backup(rig) is False
        (error,) = messages(caplog, logging.ERROR)
        assert error == (
            f"{DS} -> {DEST}: the local copy {COPY} holds snapshots but none of them is a snapshot "
            "the source dataset still has, and a full stream cannot replace it, so nothing is sent. "
            "To keep the local copy, on this host: "
            f"zfs rename {COPY} <a name outside the backup target> (the next cycle sends a full "
            "stream; see README 'Local destinations')."
        )

    def test_a_raw_copy_of_a_plain_dataset_is_refused_in_the_local_words(self, rig, sends, caplog):
        ds = rig.owned_copy(copy_rows(S1))
        ds.encryption, ds.encryptionroot = 'aes-256-gcm', COPY
        assert self.backup(rig) is False
        (error,) = messages(caplog, logging.ERROR)
        assert error == (
            f"{DS} -> {DEST}: the local copy {COPY} is a raw copy but this dataset is now sent "
            "plain; nothing is sent. On this host, move the old copy aside with "
            f"`zfs rename {COPY} <a name outside the backup target>` (see README 'Local "
            "destinations'); the next cycle sends a full plain stream"
        )

    def test_an_untrusted_listing_names_the_local_copy(self, rig, sends, caplog):
        rig.owned_copy(copy_rows(S1, 'extra_a'))
        rig.host.untrusted.add(COPY)
        assert self.backup(rig) is False
        warnings = messages(caplog, logging.WARNING)
        assert f'{DS} -> {DEST}: the local copy {COPY} could not list which of its snapshots are newer than {S1}, so nothing is sent and no anchor is written this cycle; the next cycle retries' in warnings


# ---------------------------------------------------------------------------------------------
# The wording hooks' defaults: the remote messages are byte for byte what they were
# ---------------------------------------------------------------------------------------------


class TestHookDefaults:
    DEFAULTS = {
        '_copy_noun': 'server copy',
        '_copy_placeholder': '<server dataset>',
        '_receiver': 'the server',
        '_on_receiver': 'on the server',
        '_taken_on_receiver': 'taken on the server',
        '_at_receiver': 'on the server',
        '_sender': 'this host',
        '_busy_prefix': 'server is busy',
        '_rename_doc': "README 'Renaming a server copy aside'",
        '_rejoin_doc': "README 'Rejoining'",
        '_refusal_label': 'server refusal',
    }

    LOCAL = {
        '_copy_noun': 'local copy',
        '_copy_placeholder': '<local copy>',
        '_receiver': 'the local copy',
        '_on_receiver': 'on this host',
        '_taken_on_receiver': 'taken in the local copy',
        '_at_receiver': 'in the local copy',
        '_sender': 'the source dataset',
        '_busy_prefix': 'local copy is busy',
        '_rename_doc': "README 'Local destinations'",
        '_rejoin_doc': None,
        '_refusal_label': 'local refusal',
    }

    @pytest.mark.parametrize('name', list(DEFAULTS))
    def test_chain_sender_defaults(self, name):
        assert getattr(ChainSender, name) == self.DEFAULTS[name]

    @pytest.mark.parametrize('name', list(DEFAULTS))
    def test_the_remote_manager_overrides_none_of_them(self, name):
        from zfsbackup.remote import RemoteBackupManager
        assert name not in vars(RemoteBackupManager)
        assert getattr(RemoteBackupManager, name) == self.DEFAULTS[name]

    @pytest.mark.parametrize('name', list(LOCAL))
    def test_the_local_manager_overrides_every_one_of_them(self, name):
        assert name in vars(LocalBackupManager)
        assert getattr(LocalBackupManager, name) == self.LOCAL[name]

    def test_the_helpers(self):
        sender = ChainSender.__new__(ChainSender)
        assert sender._on_receiver_capitalized() == 'On the server'
        assert sender._rejoin_pointer() == " See README 'Rejoining'"
        mgr = LocalBackupManager.__new__(LocalBackupManager)
        assert mgr._on_receiver_capitalized() == 'On this host'
        assert mgr._rejoin_pointer() == ''

    def test_retry_word(self):
        assert _retry_word(NegotiationFailed('server is reloading; retry later')) == 'reloading'
        assert _retry_word(NegotiationFailed('server is busy')) == 'busy'
        assert _retry_word(NegotiationFailed('server is busy: another stream')) == 'busy: another stream'
        assert _retry_word(NegotiationFailed('local copy is busy: waiting for x'), 'local copy is busy') == \
            'busy: waiting for x'
        assert _retry_word(NegotiationFailed('local copy is busy'), 'local copy is busy') == 'busy'


class TestRemoteMessagesAreUnchanged:
    """Each message the hooks reword, produced with the default hooks, against the literal text the
    chain had before the hooks existed."""

    @pytest.fixture
    def crig(self, mocker, snapshot_presence):
        rig = ChainRig(mocker, listing(S1, S2, S3), common=S1)
        rig.key = (CHAIN_DS, CHAIN_DEST)
        return rig

    def info(self, rig, kind='plain', ds='srv/c/pool/data'):
        rig.sender._server_info[rig.key] = (kind, ds)

    def test_a_copy_of_the_other_kind(self, crig, caplog):
        self.info(crig, 'raw')
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            assert crig.sender._server_copy_matches(crig.dsi, CHAIN_DEST, 'plain') is False
        assert messages_of(caplog) == [
            f"{CHAIN_DS} -> {CHAIN_DEST}: the server copy srv/c/pool/data is a raw copy but this "
            "dataset is now sent plain; nothing is sent. On the server, move the old copy aside "
            "with `zfs rename srv/c/pool/data <a name outside the backup target>` (see README "
            "'Renaming a server copy aside'); the next cycle sends a full plain stream"
        ]

    def test_no_common_snapshot_on_a_non_empty_copy(self, crig, caplog):
        self.info(crig, 'plain')
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            assert crig.sender._rollback_permit(crig.dsi, CHAIN_DEST, None, []) is None
        assert messages_of(caplog) == [
            f"{CHAIN_DS} -> {CHAIN_DEST}: the server copy srv/c/pool/data holds snapshots but none "
            "of them is a snapshot this host still has, and a full stream cannot replace it, so "
            "nothing is sent. To keep the server copy, on the server: zfs rename srv/c/pool/data "
            "<a name outside the backup target> (the next cycle sends a full stream; see README "
            "'Renaming a server copy aside'). See README 'Rejoining'"
        ]

    def test_newer_server_snapshots(self, crig, caplog):
        self.info(crig, 'plain')
        crig.sender._server_newer[crig.key] = (1, [{'name': 'extra_a', 'guid': '5'}])
        crig.sender._server_newer_status[crig.key] = 'known'
        snaps = listing(S1, S2, S3)
        common = next(s for s in snaps if s.name == S1)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            assert crig.sender._rollback_permit(crig.dsi, CHAIN_DEST, common, snaps) is None
        assert messages_of(caplog) == [
            f"{CHAIN_DS} -> {CHAIN_DEST}: the server copy srv/c/pool/data has 1 snapshot(s) newer "
            f"than {S1} that this host does not have (extra_a), for example after a rollback or an "
            "in-place restore of this dataset, or snapshots taken on the server. Sending would "
            "make the server destroy them, so nothing is sent. To keep them, on the server: "
            "zfs rename srv/c/pool/data <a name outside the backup target> (the next cycle sends a "
            "full stream; see README 'Renaming a server copy aside'). If they are expendable: "
            "zfs destroy srv/c/pool/data@extra_a (the next cycle continues). See README 'Rejoining'"
        ]

    def test_an_unknown_newer_list(self, crig, caplog):
        self.info(crig, 'plain')
        crig.sender._server_newer_status[crig.key] = 'unknown'
        snaps = listing(S1, S2, S3)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            crig.sender._rollback_permit(crig.dsi, CHAIN_DEST, snaps[-1], snaps)
        assert messages_of(caplog, logging.WARNING) == [
            f"{CHAIN_DS} -> {CHAIN_DEST}: the server copy srv/c/pool/data could not list which of "
            f"its snapshots are newer than {S1}, so nothing is sent and no anchor is written this "
            "cycle; the next cycle retries"
        ]

    def test_the_third_skipped_token_check(self, crig, caplog):
        self.info(crig, 'plain')
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            for _ in range(3):
                crig.sender._token_skipped(crig.dsi, CHAIN_DEST, 'HTTP 500')
        assert messages_of(caplog) == [
            f"{CHAIN_DS} -> {CHAIN_DEST}: the resume-token check has skipped the last 3 cycles in "
            "a row (HTTP 500); nothing is sent for this dataset until it passes, so the "
            "destination is falling behind. See the WARNINGs above for the cause. If the server's "
            "partial receive of this dataset is no longer wanted, abort it on the server with "
            "`zfs receive -A srv/c/pool/data`; the next cycle then sends fresh"
        ]

    def test_the_placeholder_when_the_copy_is_not_known(self, crig, caplog):
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            for _ in range(3):
                crig.sender._token_skipped(crig.dsi, CHAIN_DEST, 'x')
        assert 'abort it on the server with `zfs receive -A <server dataset>`' in messages_of(caplog)[0]

    def test_a_retryable_negotiate_refusal_in_the_chain(self, crig, caplog):
        crig.sender.common = [NegotiationFailed('server is busy: another stream', reloading=True)]
        with caplog.at_level(logging.INFO, logger='zfsbackup.chain'):
            assert crig.run() is False
        assert 'server fake://destination is busy: another stream; skipping pool/data this cycle' in \
            messages_of(caplog, logging.INFO)

    def test_a_snapshot_destroyed_while_it_was_sent(self, crig, caplog, snapshot_presence):
        snapshot_presence.gone.add(S2)
        started = _SendStream(MagicMock(), 'fresh', True, sinfo(S2), sinfo(S1))
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            result = crig.sender._record_step(
                crig.sender.transport, crig.dsi, CHAIN_DEST, listing(S1, S2, S3), started)
        assert result == chain_module._ANCHOR_FAILED
        assert messages_of(caplog) == [
            f"{CHAIN_DS}@{S2} was destroyed while it was being sent to {CHAIN_DEST}; the server has "
            f"it but this host no longer does. Putting the anchor back on {S1}. Stopping; the next "
            "cycle negotiates again"
        ]

    def test_up_to_date_is_already_on_the_server(self, crig, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.chain'):
            crig.sender._settle_up_to_date(crig.dsi, sinfo(S3), CHAIN_DEST, listing(S1, S2, S3))
        assert f'Nothing to send for {CHAIN_DS} -> {CHAIN_DEST}: {S3} is already on the server' in \
            messages_of(caplog, logging.DEBUG)

    def test_the_stream_refusal_key_is_cleared_under_the_server_label(self, crig, snapshot_presence):
        key = (f'{CHAIN_DS} [server refusal]', CHAIN_DEST)
        crig.sender._refusal_logged[key] = 'x'
        started = _SendStream(MagicMock(), 'fresh', True, sinfo(S2), sinfo(S1))
        crig.sender._record_step(crig.sender.transport, crig.dsi, CHAIN_DEST, listing(S1, S2, S3), started)
        assert key not in crig.sender._refusal_logged

    def test_the_local_label_is_cleared_by_the_local_manager_only(self, rig, snapshot_presence):
        server_key = (f'{DS} [server refusal]', DEST)
        local_key = (f'{DS} [local refusal]', DEST)
        rig.mgr._refusal_logged[server_key] = 'x'
        rig.mgr._refusal_logged[local_key] = 'y'
        started = _SendStream(MagicMock(), 'fresh', True, sinfo(S2), sinfo(S1))
        rig.mgr._record_step(rig.transport, rig.dsi, DEST, LISTING, started)
        assert local_key not in rig.mgr._refusal_logged and server_key in rig.mgr._refusal_logged


def messages_of(caplog, level=logging.ERROR, logger='zfsbackup.chain'):
    return [r.getMessage() for r in caplog.records if r.name == logger and r.levelno == level]


# ---------------------------------------------------------------------------------------------
# receiver: decide_receive wording, snapshots_newer_than; the api wrappers
# ---------------------------------------------------------------------------------------------


def srow(name, txg, guid):
    return receiver.ServerSnapshot(name, txg, guid)


BASE_G = receiver.StreamBase('b', 100, frozenset())


class TestDecideReceiveWording:
    RENAME_SERVER = "README 'Renaming a server copy aside'"

    def test_the_defaults_are_the_servers_words_byte_for_byte(self):
        ds = 'bk/srv/c/p'
        no_common = receiver.decide_receive(ds, receiver.StreamBase(), [srow('a', 1, 1)])
        assert no_common.message == (
            f"{ds} already holds snapshots and a full stream cannot replace them, so nothing is "
            f"received. On the server: `zfs rename {ds} <a name outside the backup target>` "
            f"(the next cycle sends a full stream; see {self.RENAME_SERVER})"
        )
        missing = receiver.decide_receive(ds, BASE_G, [srow('a', 1, 1)])
        assert missing.message == (
            f"{ds} does not hold the incremental source snapshot (guid 100), so nothing is received. "
            "This can be transient (the server's pruner may have removed it since negotiation; the "
            "next cycle negotiates again). If it persists, on the server: "
            f"`zfs rename {ds} <a name outside the backup target>` (the next cycle sends a full "
            f"stream; see {self.RENAME_SERVER})"
        )
        newer = receiver.decide_receive(ds, BASE_G, [srow('a', 1, 100), srow('b', 2, 5), srow('c', 3, 6)])
        assert newer.message == (
            f"{ds} holds 2 snapshot(s) newer than a (b…c) that the client did not allow to be "
            f"destroyed, so nothing is received (receiving would destroy them). To keep them, on "
            f"the server: `zfs rename {ds} <a name outside the backup target>` (the next cycle "
            f"sends a full stream; see {self.RENAME_SERVER}). If they are expendable: "
            f"`zfs destroy {ds}@b%c` (the next cycle continues)"
        )

    def test_where_sender_and_rename_doc_reword_every_refusal(self):
        ds = 'bk/c/p'
        kwargs = dict(where='this host', sender='this host', rename_doc="README 'Local destinations'")
        for decision in (
            receiver.decide_receive(ds, receiver.StreamBase(), [srow('a', 1, 1)], **kwargs),
            receiver.decide_receive(ds, BASE_G, [srow('a', 1, 1)], **kwargs),
            receiver.decide_receive(ds, BASE_G, [srow('a', 1, 100), srow('b', 2, 5)], **kwargs),
        ):
            assert decision.reason is not None
            assert 'on this host:' in decision.message.lower() or 'on this host' in decision.message
            assert "see README 'Local destinations'" in decision.message
            assert 'server' not in decision.message and 'client' not in decision.message
            assert 'Renaming a server copy' not in decision.message

    def test_the_sender_names_who_did_not_allow_the_destroy(self):
        decision = receiver.decide_receive(
            'd', BASE_G, [srow('a', 1, 100), srow('b', 2, 5)], sender='this host')
        assert 'that this host did not allow to be destroyed' in decision.message

    def test_the_decision_itself_does_not_depend_on_the_words(self):
        rows = [srow('a', 1, 100), srow('b', 2, 5)]
        plain = receiver.decide_receive('d', BASE_G, rows)
        worded = receiver.decide_receive('d', BASE_G, rows, where='x', sender='y', rename_doc='z')
        assert (plain.force, plain.reason, plain.rolled_back) == (worded.force, worded.reason, worded.rolled_back)

    def test_no_snapshots_forces_and_a_newest_base_forces(self):
        assert receiver.decide_receive('d', BASE_G, []).force is True
        assert receiver.decide_receive('d', BASE_G, [srow('a', 1, 100)]).force is True


class TestSnapshotsNewerThan:
    def test_newer_by_txg_oldest_first_with_string_guids(self):
        rows = [srow('c', 30, 3), srow('a', 10, 1), srow('b', 20, None), srow('d', 40, 4)]
        assert receiver.snapshots_newer_than('b', rows) == [
            {'name': 'c', 'guid': '3'}, {'name': 'd', 'guid': '4'}]

    def test_none_newer_is_an_empty_list(self):
        assert receiver.snapshots_newer_than('b', [srow('a', 1, 1), srow('b', 2, 2)]) == []

    def test_an_unknown_guid_is_none(self):
        assert receiver.snapshots_newer_than('a', [srow('a', 1, 1), srow('b', 2, None)]) == [
            {'name': 'b', 'guid': None}]

    def test_a_snapshot_without_createtxg_makes_it_unknown(self):
        assert receiver.snapshots_newer_than('a', [srow('a', 1, 1), srow('b', None, 2)]) is None

    def test_the_name_order_is_irrelevant(self):
        rows = [srow('z_first', 1, 1), srow('a_second', 2, 2)]
        assert receiver.snapshots_newer_than('z_first', rows) == [{'name': 'a_second', 'guid': '2'}]

    def test_equal_createtxg_is_not_newer(self):
        assert receiver.snapshots_newer_than('a', [srow('a', 5, 1), srow('b', 5, 2)]) == []


class TestApiWrappersKeepTheirContracts:
    def test_the_names_the_api_always_exported_are_the_receivers_objects(self):
        from zfsbackup import api
        for name in (
            'parse_stream_header', 'stream_base_problem', 'decide_receive', 'StreamBase',
            'StreamHeader', 'StreamHeaderError', 'ReceiveDecision', 'ServerSnapshot',
            'retention_horizon', 'retention_shrink', 'format_horizon', 'DRR_RECORD_SIZE',
            'DMU_BACKUP_MAGIC', 'ZFS_USER_PROPERTY_MAXLEN', 'CONFIG_TOO_LARGE_ERROR',
            'DMU_OST_ZFS', 'DMU_OST_ZVOL', 'DMU_SUBSTREAM', 'DMU_COMPOUNDSTREAM', 'DRR_BEGIN',
        ):
            assert getattr(api, name) is getattr(receiver, name), name

    def test_canonical_dataset_config_follows_the_apis_own_limit(self, mocker):
        from zfsbackup import api
        encoded = DatasetConfig(name=DS).to_property()
        assert api.canonical_dataset_config(encoded) == receiver.canonical_dataset_config(encoded)
        mocker.patch.object(api, 'ZFS_USER_PROPERTY_MAXLEN', 10)
        with pytest.raises(ValueError, match='too large'):
            api.canonical_dataset_config(encoded)
        assert receiver.canonical_dataset_config(encoded)       # the receiver's own default is unaffected

    def test_canonical_dataset_config_refuses_what_the_receiver_refuses(self):
        from zfsbackup import api
        for bad in (None, '', 'not base64!', 5):
            with pytest.raises(ValueError):
                api.canonical_dataset_config(bad)

    def test_server_snapshots_goes_through_the_apis_zfs(self, mocker):
        from zfsbackup import api
        host = FakeHost()
        lib = FakeLib(host)
        host.add(COPY, snaps=[ServerRow(*r) for r in copy_rows(S1, S2)])
        mocker.patch.object(api, 'zfs', lib)
        rows = api.server_snapshots(Filesystem(COPY))
        assert [(r.name, r.txg, r.guid) for r in rows] == copy_rows(S1, S2)
        assert host.of('list-snaps') == [('list-snaps', COPY, True)]
        api.server_snapshots(Filesystem(COPY), strict=False)
        assert host.of('list-snaps')[-1] == ('list-snaps', COPY, False)

    def test_newer_than_keeps_its_signature_and_logs_when_it_cannot_tell(self, caplog):
        from zfsbackup import api
        rows = [srow('a', 1, 1), srow('b', 2, 2)]
        assert api.newer_than('bk/x', 'a', rows) == [{'name': 'b', 'guid': '2'}]
        with caplog.at_level(logging.WARNING, logger='zfsbackup.api'):
            assert api.newer_than('bk/x', 'a', [srow('a', 1, 1), srow('b', None, 2)]) is None
        assert [r.getMessage() for r in caplog.records if r.name == 'zfsbackup.api'] == [
            'negotiate for bk/x: a server snapshot has no createtxg; not reporting server_newer']

    def test_the_wrappers_are_the_apis_own_functions_so_a_test_can_patch_them(self):
        from zfsbackup import api
        for name in ('canonical_dataset_config', 'server_snapshots', 'newer_than'):
            func = getattr(api, name)
            assert func.__module__ == 'zfsbackup.api', name
            assert func is not getattr(receiver, name, None), name

    def test_a_patched_wrapper_is_what_the_api_module_sees(self, mocker):
        from zfsbackup import api
        for name in ('canonical_dataset_config', 'server_snapshots', 'newer_than'):
            sentinel = object()
            mocker.patch.object(api, name, return_value=sentinel)
            assert getattr(api, name)() is sentinel


@pytest.mark.subprocess
class TestImportWeight:
    BAD = ('flask', 'werkzeug', 'requests', 'urllib3', 'websocket', 'simple_websocket', 'flask_sock',
           'zfsbackup._vendor', 'zfsbackup.remote', 'zfsbackup.api', 'zfsbackup.identity')

    def modules(self, code):
        proc = subprocess.run(
            [sys.executable, '-c', code], capture_output=True, text=True, timeout=120,
            env=hermetic_env(PYTHONPATH=os.pathsep.join(sys.path)),
        )
        assert proc.returncode == 0, proc.stderr
        return set(proc.stdout.split())

    PROBE = (
        "import sys\n"
        "{imp}\n"
        "print(' '.join(sorted(m for m in sys.modules if m.split('.')[0] in {bad!r} or m in {bad!r})))\n"
    )

    def test_importing_local_loads_no_http_stack(self):
        loaded = self.modules(self.PROBE.format(imp='import zfsbackup.local', bad=self.BAD))
        assert not {m for m in loaded if m.split('.')[0] in ('flask', 'werkzeug', 'requests', 'urllib3',
                                                             'websocket', 'simple_websocket', 'flask_sock')}
        assert not (loaded & {'zfsbackup.remote', 'zfsbackup.api', 'zfsbackup.identity'})

    def test_importing_receiver_loads_no_http_stack_either(self):
        loaded = self.modules(self.PROBE.format(imp='import zfsbackup.receiver', bad=self.BAD))
        assert not loaded & {'zfsbackup.api', 'zfsbackup.remote', 'flask', 'werkzeug', 'requests'}

    def test_the_control_the_probe_does_see_them_after_importing_the_api(self):
        loaded = self.modules(self.PROBE.format(imp='import zfsbackup.api', bad=self.BAD))
        assert 'flask' in loaded and 'zfsbackup.api' in loaded


# ---------------------------------------------------------------------------------------------
# RemoteBackupWorker: the two managers
# ---------------------------------------------------------------------------------------------

NOTE = 'resume token check failed'


@pytest.fixture
def wrig(monkeypatch, sdir):
    """Worker rig: one dataset sent to a local and an https destination (local first)."""
    return WorkerRig(monkeypatch, sdir, {'here': LOCAL, 'off': HTTPS})


class TestWorkerBuildsTheManagers:
    """The remote worker imports the transport CLASSES once, in `_before_loop` (after the version check,
    never mid-run), and every send builds its OWN manager from them (`_new_sender`, lazily, after R-2),
    over the worker's one `SendMemory`, and closes it."""

    def test_remote_only_configs_never_import_the_local_class(self, monkeypatch, sdir):
        rig = WorkerRig(monkeypatch, sdir, {'off': HTTPS})
        rig.run()
        assert rig.rig.worker._local_cls is None
        assert {kind for kind, _ in rig.rig.built} == {'remote'}

    def test_a_declared_local_destination_imports_it_even_when_no_dataset_uses_it_yet(self, monkeypatch, sdir):
        """Imported whenever the configuration has one: `zfsbackup.local` is imported at start-up,
        right after the version check, never in the middle of a send."""
        rig = WorkerRig(monkeypatch, sdir, {'here': LOCAL, 'off': HTTPS}, uses=['off'])
        worker = rig.rig.worker
        worker._before_loop(rig.config, rig.manager)
        assert worker._local_cls is not None
        rig.run()
        assert {kind for kind, _ in rig.rig.built} == {'remote'}, 'imported, but nothing used it'

    def test_a_disabled_dataset_using_it_still_imports_it(self, wrig):
        wrig.config.datasets[0].enabled = False
        worker = wrig.rig.worker
        worker._before_loop(wrig.config, wrig.manager)
        assert worker._local_cls is not None

    def test_an_enabled_dataset_using_it_builds_a_sender_with_the_client_id_and_the_memory(self, wrig):
        wrig.run()
        (kw,) = [kw for kind, kw in wrig.rig.built if kind == 'local']
        assert kw['client_id'] == 'client1' and kw['memory'] is wrig.rig.memory
        assert 'tls_dir' not in kw, 'a local destination has no TLS identity'
        # two remote managers: the send's, and the throwaway one of the certificate reminder
        remote = [kw for kind, kw in wrig.rig.built if kind == 'remote']
        assert len(remote) == 2
        assert all(kw['client_id'] == 'client1' and kw['memory'] is wrig.rig.memory and 'tls_dir' in kw for kw in remote)

    def test_the_remote_class_is_always_imported_in_before_loop(self, monkeypatch, sdir):
        rig = WorkerRig(monkeypatch, sdir, {'here': LOCAL})
        rig.rig.worker._before_loop(rig.config, rig.manager)
        assert rig.rig.worker._remote_cls is not None

    def test_there_is_no_lazy_import_a_missing_class_is_an_error_not_an_import(self, monkeypatch, sdir, caplog):
        rig = WorkerRig(monkeypatch, sdir, {'off': HTTPS})
        worker = rig.rig.worker
        worker._before_loop(rig.config, rig.manager)
        config = BackupConfig(datasets=[], destinations={'here': LOCAL})
        manager = DatasetManager(config, memory=worker._mem())
        with caplog.at_level(logging.ERROR, logger='zfsbackup.workers'):
            with pytest.raises(RuntimeError, match='local backup manager was not imported at start-up'):
                worker._new_sender(manager, 'here')
        assert any('not importing zfsbackup.local mid-run' in r.getMessage() for r in caplog.records
                   if r.levelno == logging.ERROR)
        assert [kind for kind, _ in rig.rig.built] == []

    def test_a_pair_whose_class_was_not_imported_fails_alone_and_is_recorded(self, wrig, monkeypatch):
        """`_before_loop` did not import the local class (a double): the local pair fails with the
        reason, the remote pair is unaffected."""
        worker = wrig.rig.worker
        real = worker._before_loop

        def before(config, manager):
            real(config, manager)
            worker._local_cls = None

        monkeypatch.setattr(worker, '_before_loop', before)
        wrig.run()
        records = wrig.records()
        assert records[('send', 'pool/a', 'here')].result == 'error'
        assert 'local backup manager was not imported at start-up' in records[('send', 'pool/a', 'here')].last_error
        assert records[('send', 'pool/a', 'off')].result == 'ok'

    def test_every_send_builds_its_own_manager_over_the_shared_memory_and_closes_it(self, monkeypatch, sdir):
        rig = WorkerRig(monkeypatch, sdir, {'here': LOCAL}, names=('pool/a', 'pool/b'))
        rig.run()
        built = [kw for kind, kw in rig.rig.built if kind == 'local']
        assert len(built) == 2 and all(kw['memory'] is rig.rig.memory for kw in built)
        assert len(rig.lbm.close.call_args_list) == 2    # a list append is atomic, `call_count += 1` is not

    def test_a_manager_that_does_not_share_the_memory_is_refused_not_trusted(self, wrig, monkeypatch):
        """A manager built without the worker's memory would silently lose `COPY_CONFIRMED`, and R-2
        would hold the children for ever: `_shares_memory` raises (it is not an `assert`)."""
        wrig.lbm._memory = SendMemory()
        monkeypatch.setattr(
            'zfsbackup.local.LocalBackupManager',
            lambda config, manager, **kw: wrig.lbm,
        )
        wrig.run()
        rec = wrig.records()[('send', 'pool/a', 'here')]
        assert rec.result == 'error' and "does not share the worker's SendMemory" in rec.last_error
        assert wrig.records()[('send', 'pool/a', 'off')].result == 'ok'

    def test_begin_cycle_is_never_called_on_either_manager(self, wrig):
        wrig.run()
        wrig.rbm.begin_cycle.assert_not_called()
        wrig.lbm.begin_cycle.assert_not_called()

    def test_the_local_module_is_imported_only_when_a_local_destination_is_configured(self):
        code = (
            "import sys\n"
            "from unittest.mock import MagicMock\n"
            "from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig\n"
            "from zfsbackup.stopflag import StopFlag\n"
            "from zfsbackup.workers import RemoteBackupWorker\n"
            "def probe(dest, url):\n"
            "    cfg = BackupConfig(datasets=[DatasetConfig(name='pool/a', remote=[RemoteDatasetConfig(destination=dest)])],\n"
            "                       destinations={dest: Destination(url=url)})\n"
            "    manager = MagicMock(); manager.config = cfg\n"
            "    w = RemoteBackupWorker(cfg, StopFlag(), client_id='cid')\n"
            "    w._before_loop(cfg, manager)\n"
            "    return 'zfsbackup.local' in sys.modules\n"
            "print(probe('off', 'https://h/p'))\n"
            "print(probe('here', 'zfs:bk/b'))\n"
        )
        proc = subprocess.run(
            [sys.executable, '-c', code], capture_output=True, text=True, timeout=120,
            env=hermetic_env(PYTHONPATH=os.pathsep.join(sys.path)),
        )
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.split() == ['False', 'True']



class TestWorkerDispatch:
    def test_only_a_destination_that_is_exactly_local_goes_to_the_local_manager(self, monkeypatch, sdir):
        rig = WorkerRig(
            monkeypatch, sdir,
            {'off': HTTPS, 'odd': SimpleNamespace(is_local='yes', url='zfs:x/y')}, uses=['odd'],
        )
        rig.run()
        rig.lbm.backup_dataset.assert_not_called()
        assert rig.rbm.backup_dataset.call_count == 1

    def test_the_period_counts_local_destinations_like_any_other(self, wrig):
        wrig.config.datasets[0].remote[0].frequency = timedelta(minutes=20)       # the local one
        wrig.run()
        assert wrig.rig.loop.schedule.spec(('pool/a', 'here')).effective_period == 20 * 60

    def test_each_pair_records_the_account_of_the_manager_that_ran_it(self, wrig):
        wrig.lbm.backup_dataset.side_effect = None
        wrig.lbm.backup_dataset.return_value = False
        wrig.lbm.last_problem.return_value = 'local says no'
        wrig.rbm.backup_dataset.side_effect = None
        wrig.rbm.backup_dataset.return_value = False
        wrig.rbm.last_problem.return_value = 'remote says no'
        wrig.run()
        records = wrig.records()
        assert records[('send', 'pool/a', 'here')].last_error == 'local says no'
        assert records[('send', 'pool/a', 'off')].last_error == 'remote says no'

    def test_an_ancestor_is_sent_before_its_descendants_to_both_kinds(self, monkeypatch, sdir):
        rig = WorkerRig(
            monkeypatch, sdir, {'here': LOCAL, 'off': HTTPS},
            names=('pool/a/b/c', 'pool/a', 'pool/z', 'pool/a/b'), serial=True,
        )
        rig.run()
        assert len(rig.order) == 8
        for destination in ('here', 'off'):
            seen = [name for _, name, dest in rig.order if dest == destination]
            assert sorted(seen) == sorted(['pool/a/b/c', 'pool/a', 'pool/z', 'pool/a/b'])
            assert seen.index('pool/a') < seen.index('pool/a/b') < seen.index('pool/a/b/c')
        assert {(kind, dest) for kind, _, dest in rig.order} == {('local', 'here'), ('remote', 'off')}

    def test_a_sender_is_asked_once_per_destination_per_dataset_per_run(self, wrig):
        wrig.run()
        assert len(wrig.lbm.backup_dataset.call_args_list) == 1
        assert len(wrig.rbm.backup_dataset.call_args_list) == 1


class TestWorkerSeedsTheSkipCountsPerPair:
    """`_before_loop` seeds the shared memory's `TOKEN_SKIPS` per (dataset, destination) from the previous
    incarnation's status file: the managers share the memory, so there is nothing to hand each one."""

    def seed(self, sdir, *rows):
        w = StatusWriter(sdir, 'remote-backup-worker')
        for dataset, dest, failures in rows:
            for _ in range(failures):
                w.record('send', dataset, dest, ok=False, error=f'{NOTE}: x')
        w.flush(force=True)

    def counts(self, rig):
        return {d: rig.rig.memory.count(TOKEN_SKIPS, ('pool/a', d)) for d in ('here', 'off')}

    def test_the_note_is_the_one_the_chain_writes(self):
        assert chain_module.TOKEN_SKIP_NOTE == NOTE

    def test_each_pair_gets_its_own_count(self, wrig, sdir):
        self.seed(sdir, ('pool/a', 'here', 2), ('pool/a', 'off', 3))
        wrig.run()
        assert self.counts(wrig) == {'here': 2, 'off': 3}

    def test_local_counts_alone_seed_only_the_local_pair(self, wrig, sdir):
        self.seed(sdir, ('pool/a', 'here', 2))
        wrig.run()
        assert self.counts(wrig) == {'here': 2, 'off': 0}

    def test_remote_counts_alone_seed_only_the_remote_pair(self, wrig, sdir):
        self.seed(sdir, ('pool/a', 'off', 2))
        wrig.run()
        assert self.counts(wrig) == {'here': 0, 'off': 2}

    def test_other_failures_do_not_seed(self, wrig, sdir):
        w = StatusWriter(sdir, 'remote-backup-worker')
        for dest in ('here', 'off'):
            for _ in range(4):
                w.record('send', 'pool/a', dest, ok=False, error='connection refused')
        w.flush(force=True)
        wrig.run()
        assert self.counts(wrig) == {'here': 0, 'off': 0}


# ---------------------------------------------------------------------------------------------
# A2: a copy whose snapshots could never exist (full name over 255 characters)
# ---------------------------------------------------------------------------------------------


class TestNegotiateNameTooLong:
    """`<target>/<client>/<dataset>@<snapshot>` over ZFS's 255 characters is refused in `_negotiate`
    before anything is asked of or written to zfs: ERROR once per episode, `NegotiationFailed`."""

    SNAP = LISTING[0].name                                   # 23 characters

    @staticmethod
    def dsi_with_copy_length(rig, copy_length):
        name = 'pool/' + 'a' * (copy_length - len(BASE) - 1 - len('pool/'))
        dsi = rig.dsi_for(name)
        assert len(f'{BASE}/{name}') == copy_length
        return dsi

    def fits(self, rig):
        return self.dsi_with_copy_length(rig, 255 - 1 - len(self.SNAP))

    def too_long(self, rig):
        return self.dsi_with_copy_length(rig, 255 - len(self.SNAP))

    def test_exactly_255_is_negotiated(self, rig):
        assert rig.negotiate(dsi=self.fits(rig)) is None
        assert rig.host.of('create'), 'it went on to create the ancestors'

    def test_256_is_refused_before_any_zfs_command(self, rig, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            with pytest.raises(NegotiationFailed, match='256-character name'):
                rig.negotiate(dsi=self.too_long(rig))
        assert rig.host.events == [], 'not even an existence check, let alone a write'
        (error,) = messages(caplog, logging.ERROR)
        assert 'nothing is sent' in error and 'shorten the dataset name' in error

    def over_by_one(self):
        """The listing plus one snapshot whose name is a character longer than the others."""
        return [SimpleNamespace(name=self.SNAP + 'x')] + LISTING

    def test_the_longest_snapshot_name_decides(self, rig):
        with pytest.raises(NegotiationFailed):
            rig.negotiate(dsi=self.fits(rig), snaps=self.over_by_one())
        assert rig.host.events == []

    def test_one_error_per_episode_and_a_new_one_after_a_success(self, rig, caplog):
        dsi = self.fits(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            for _ in range(3):
                with pytest.raises(NegotiationFailed):
                    rig.negotiate(dsi=dsi, snaps=self.over_by_one())
            assert len(messages(caplog, logging.ERROR)) == 1
            assert rig.negotiate(dsi=dsi) is None                 # the same pair negotiates: episode over
            with pytest.raises(NegotiationFailed):
                rig.negotiate(dsi=dsi, snaps=self.over_by_one())
        assert len(messages(caplog, logging.ERROR)) == 2

    def test_through_backup_dataset_nothing_is_written_and_the_problem_is_reported(self, rig):
        dsi = self.too_long(rig)
        assert rig.mgr.backup_dataset(dsi, RemoteDatasetConfig(destination=DEST)) is False
        assert not rig.host.of('create') and not rig.host.of('set') and not rig.host.of('start-receive')
        assert '256-character' in rig.mgr.last_problem(dsi.name, DEST)



# ---------------------------------------------------------------------------------------------
# A2: `copy_confirmed` on the local manager (what the parent-first rule of recursive datasets reads)
# ---------------------------------------------------------------------------------------------


class TestCopyConfirmedLocal:
    """The copy under the local target is CONFIRMED in this incarnation when a negotiate found it and the
    call returned True, or a stream committed. No anchor is evidence, and a dry run confirms nothing."""

    def backup(self, rig):
        return rig.mgr.backup_dataset(rig.dsi, RemoteDatasetConfig(destination=DEST))

    def confirmed(self, rig):
        return rig.mgr.copy_confirmed(DS, DEST)

    def test_nothing_is_confirmed_at_first(self, rig):
        rig.owned_copy(copy_rows(S1, S2, S3))
        assert self.confirmed(rig) is False

    def test_an_existing_up_to_date_copy_confirms_without_a_stream(self, rig, sends):
        rig.owned_copy(copy_rows(S1, S2, S3))
        assert self.backup(rig) is True and sends.calls == []
        assert self.confirmed(rig) is True

    def test_a_first_full_send_that_commits_confirms(self, rig, sends, streams):
        commits(rig, streams, S3)
        assert self.backup(rig) is True
        assert self.confirmed(rig) is True

    def test_a_committed_stream_confirms_even_if_the_chain_then_fails(self, rig, sends, streams):
        rig.owned_copy(copy_rows(S1))
        commits(rig, streams, S2)
        rig.host.receives.append(streams.receive(rc=1, stderr='cannot receive: no space', events=rig.host.events))
        assert self.backup(rig) is False
        assert self.confirmed(rig) is True

    def test_a_failing_first_receive_confirms_nothing(self, rig, sends, streams):
        rig.host.receives.append(streams.receive(rc=1, stderr='cannot receive: no space', events=rig.host.events))
        assert self.backup(rig) is False
        assert self.confirmed(rig) is False

    def test_a_dry_run_confirms_nothing(self, mocker, streams, snapshot_presence):
        rig = LocalRig(mocker, dry_run=True)
        rig.owned_copy(copy_rows(S1, S2, S3))
        assert self.backup(rig) is True
        assert self.confirmed(rig) is False

    def test_a_copy_that_has_gone_unsets_the_confirmation(self, rig, sends, streams):
        rig.owned_copy(copy_rows(S1, S2, S3))
        assert self.backup(rig) is True and self.confirmed(rig) is True
        del rig.host.datasets[COPY]                                   # the copy was lost
        rig.host.receives.append(streams.receive(rc=1, stderr='cannot receive: boom', events=rig.host.events))
        assert self.backup(rig) is False
        assert self.confirmed(rig) is False

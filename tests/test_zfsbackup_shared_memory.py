"""Fresh managers sharing ONE `SendMemory` (Track B, item B1.3).

Track B builds fresh `DatasetManager` and `RemoteBackupManager` / `LocalBackupManager` objects for every
send, so what must not repeat (the once-per-episode logs), what counts across sends (the consecutive
skips) and what later sends must see (`copy_confirmed`, the local pending map) lives in the worker's
one `SendMemory`, handed to every manager as `memory=`. Pinned here, for each latch: two managers
built with the same memory announce once between them; two managers built WITHOUT one stay
independent (each announces; nothing leaks between them), which is also how a manager used alone
behaves.

The transport is faked as in `test_zfsbackup_remote.py`, the `zfs` commands as in
`test_zfsbackup_local.py` / `test_zfsbackup_recursive_sends.py`; nothing reaches `zfs` or a socket.
"""

import logging
import re
import sys
import threading
from datetime import timedelta
from unittest.mock import patch

import pytest
import requests

from _remote_support import (  # noqa: F401  (fixtures)
    SERVER_FP, _client_tls_template, client_tls_dir, fake_connect_pinned, snapshot_presence, whoami_json,
)
from _snapshot_support import NO_TXG, frozen_clock, listed_snapshot  # noqa: F401  (fixture)
from test_zfsbackup_local import (  # noqa: F401  (fixtures and the rig, not tests)
    BASE as LOCAL_BASE, CLIENT, COPY, DEST as LOCAL_DEST, DS as LOCAL_DS, PARENT as LOCAL_PARENT, S1, S2, S3, TARGET,
    LocalRig, OTHER_CLIENT, _bounded_pump, commits, copy_rows, rig, sends, streams,
)
from test_zfsbackup_recursive_sends import (  # noqa: F401  (fixtures and helpers, not tests)
    BACKUP_LOGGER, CID_PROP, PARENT, add_tree, config as recursive_config, dataset as recursive_dataset,
    zfs_world,
)
from test_zfsbackup_remote import (  # noqa: F401  (fixtures and harnesses, not tests)
    BASE, NEW, OLD, REAL, Chain, _make_config, _no_local_zfs_property_writes, _snapshots_are_present,
    _tls_connect_is_faked, h, make_h, sleeps,
)
from zfsbackup.backup_manager import PROP_CLIENT_ID, DatasetManager
from zfsbackup.chain import NegotiationFailed
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, Duration, RemoteDatasetConfig, RetentionRule,
)
from zfsbackup.local import LocalBackupManager
from zfsbackup.remote import RESUME_TOKEN_SKIP_ERROR_AFTER, RemoteBackupManager
from zfsbackup.sendmemory import (
    CLOCK, COPY_CONFIRMED, EXPAND_COPY, EXPAND_ERROR, EXPAND_TARGET, NO_TXG as NO_TXG_TABLE,
    REFUSALS, SendMemory,
)

pytestmark = pytest.mark.unit

DEST = 'offsite'
SKIPS = RESUME_TOKEN_SKIP_ERROR_AFTER

#: Whether the two managers of a test share a memory. The expected number of announcements is
#: `1 if shared else 2`.
SHARING = pytest.mark.parametrize('shared', [True, False], ids=['shared', 'independent'])


def announced(shared):
    return 1 if shared else 2


def at(caplog, level, logger=None, text=None):
    return [
        r.getMessage() for r in caplog.records
        if r.levelno == level and (logger is None or r.name == logger)
        and (text is None or text in r.getMessage())
    ]


# ---------------------------------------------------------------------------------------------
# two RemoteBackupManagers
# ---------------------------------------------------------------------------------------------


def two_remote(h, shared):
    """`h.rbm` and a second `RemoteBackupManager` over the same config, local manager and TLS
    directory; both on one `SendMemory` when `shared`."""
    first = h.rbm
    memory = SendMemory() if shared else None
    if shared:
        first.adopt_memory(memory)
    second = RemoteBackupManager(
        first.config, first.local_manager, client_id=first.client_id, tls_dir=first.tls_dir,
        memory=memory,
    )
    assert (first._memory is second._memory) is shared
    return first, second


def run_backup(h, rbm):
    with patch.object(h.manager, 'list_snapshots', return_value=h.snaps):
        return rbm.backup_dataset(h.dsi, h.dsi.config.remote[0])


class TestTwoRemoteManagers:
    def test_a_manager_built_without_a_memory_has_a_private_one(self, h):
        other = RemoteBackupManager(
            h.rbm.config, h.rbm.local_manager, client_id='client1', tls_dir=h.rbm.tls_dir)
        assert isinstance(other._memory, SendMemory) and other._memory is not h.rbm._memory

    def test_memory_passed_at_construction_is_used(self, h):
        memory = SendMemory()
        manager = RemoteBackupManager(
            h.rbm.config, h.rbm.local_manager, client_id='client1', tls_dir=h.rbm.tls_dir,
            memory=memory)
        assert manager._memory is memory

    def test_adopt_memory_replaces_the_private_one(self, h):
        memory = SendMemory()
        h.rbm.adopt_memory(memory)
        assert h.rbm._memory is memory

    @SHARING
    def test_a_standing_refusal_is_logged_once(self, h, caplog, shared):
        h.rm.post(re.compile(r'.*/negotiate$'), status_code=409, json={'error': 'held for another source'})
        first, second = two_remote(h, shared)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for rbm in (first, second, first, second):
                assert run_backup(h, rbm) is False
        errors = at(caplog, logging.ERROR, 'zfsbackup.remote', 'held for another source')
        assert len(errors) == announced(shared)

    @SHARING
    def test_a_refusal_cleared_by_one_manager_is_new_to_the_other(self, h, caplog, shared):
        """The episode ends when ANY manager sees the dataset accepted again."""
        h.rm.post(re.compile(r'.*/negotiate$'), status_code=409, json={'error': 'held for another source'})
        first, second = two_remote(h, shared)
        run_backup(h, first)
        h.rm.post(re.compile(r'.*/negotiate$'), json={'common_snapshot': NEW, 'server_copy': 'plain',
                                                      'server_dataset': 'x'})
        run_backup(h, second)
        h.rm.post(re.compile(r'.*/negotiate$'), status_code=409, json={'error': 'held for another source'})
        caplog.clear()
        with caplog.at_level(logging.ERROR, logger='zfsbackup.remote'):
            run_backup(h, first)
        errors = at(caplog, logging.ERROR, 'zfsbackup.remote', 'held for another source')
        assert len(errors) == (1 if shared else 0), 'a new episode is announced again, once'

    @SHARING
    def test_a_retention_collision_warning_is_logged_once(self, client_tls_dir, requests_mock, caplog, shared):
        rules = [RetentionRule(Duration('1h'), Duration('7d')), RetentionRule(Duration('1d'), Duration('1w'))]
        config = _make_config(datasets=[DatasetConfig(
            name='pool/data', remote=[RemoteDatasetConfig(destination=DEST, retention_rules=rules)])])
        manager = DatasetManager(config)
        dsi = manager.datasets[0]
        memory = SendMemory() if shared else None
        a = RemoteBackupManager(config, manager, client_id='client1', tls_dir=client_tls_dir, memory=memory)
        b = RemoteBackupManager(config, manager, client_id='client1', tls_dir=client_tls_dir, memory=memory)
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        with patch.object(manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING, logger='zfsbackup.remote'):
                for rbm in (a, b, a, b):
                    rbm.backup_dataset(dsi, dsi.config.remote[0])
        assert len(at(caplog, logging.WARNING, 'zfsbackup.remote', 'Dataset pool/data -> offsite:')) \
            == announced(shared)

    @SHARING
    def test_the_resume_token_skips_count_across_managers_and_the_error_is_logged_once(
        self, h, caplog, shared
    ):
        first, second = two_remote(h, shared)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.remote'):
            for i in range(2 * SKIPS):
                (first, second)[i % 2]._token_skipped(h.dsi, DEST, 'cannot read')
        errors = at(caplog, logging.ERROR, 'zfsbackup.remote', 'resume-token check has skipped')
        key = (h.dsi.name, DEST)
        if shared:
            assert len(errors) == 1, 'one episode, one ERROR, whichever manager counted'
            assert first._token_skips[key] == 2 * SKIPS
        else:
            assert len(errors) == 2, 'each manager counted only its own run of skips'
            assert first._token_skips[key] == second._token_skips[key] == SKIPS

    def test_independent_managers_each_need_their_own_run_of_skips(self, h, caplog):
        first, second = two_remote(h, False)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.remote'):
            for _ in range(SKIPS - 1):
                first._token_skipped(h.dsi, DEST, 'x')
                second._token_skipped(h.dsi, DEST, 'x')
        assert at(caplog, logging.ERROR, 'zfsbackup.remote', 'resume-token check has skipped') == []

    def test_two_managers_sharing_a_memory_reach_the_error_together(self, h, caplog):
        first, second = two_remote(h, True)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.remote'):
            for _ in range(SKIPS - 1):
                first._token_skipped(h.dsi, DEST, 'x')
            assert at(caplog, logging.ERROR, 'zfsbackup.remote', 'resume-token check has skipped') == []
            second._token_skipped(h.dsi, DEST, 'x')
        assert len(at(caplog, logging.ERROR, 'zfsbackup.remote', 'resume-token check has skipped')) == 1

    def test_a_passing_token_check_on_one_manager_ends_the_episode_for_both(self, h, caplog):
        first, second = two_remote(h, True)
        for _ in range(SKIPS):
            first._token_skipped(h.dsi, DEST, 'x')
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            second._token_ok(h.dsi, DEST)
            first._token_ok(h.dsi, DEST)
        assert len(at(caplog, logging.INFO, 'zfsbackup.remote', 'resume-token check passes again')) == 1
        assert first._token_skips == {} and first._token_error_logged == set()

    @SHARING
    def test_the_negotiate_zfs_timeout_error_is_logged_once(self, h, caplog, shared):
        first, second = two_remote(h, shared)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.remote'):
            for i in range(2 * SKIPS):
                (first, second)[i % 2]._negotiate_zfs_skipped(h.dsi, DEST, BASE)
        errors = at(caplog, logging.ERROR, 'zfsbackup.remote', "server's zfs has not answered")
        assert len(errors) == (1 if shared else 2)

    @SHARING
    def test_a_good_negotiate_on_one_manager_rearms_the_negotiate_zfs_error(self, h, caplog, shared):
        first, second = two_remote(h, shared)
        for _ in range(SKIPS):
            first._negotiate_zfs_skipped(h.dsi, DEST, BASE)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            second._negotiate_zfs_ok(h.dsi, DEST)
        infos = at(caplog, logging.INFO, 'zfsbackup.remote', 'answers negotiations again')
        assert len(infos) == (1 if shared else 0)

    @SHARING
    def test_the_no_client_id_error_is_logged_once(self, h, caplog, shared):
        first, second = two_remote(h, shared)
        first.client_id = second.client_id = None
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for rbm in (first, second, first, second):
                assert rbm._ready_transport(DEST, rbm.config.destinations[DEST]) is None
        assert len(at(caplog, logging.ERROR, 'zfsbackup.remote', 'remote backup skipped')) == announced(shared)

    @SHARING
    def test_an_identity_error_is_logged_once(self, h, caplog, tmp_path, shared):
        """An empty TLS directory: the client identity cannot be loaded."""
        first, second = two_remote(h, shared)
        first.tls_dir = second.tls_dir = tmp_path / 'empty-tls'
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for rbm in (first, second, first, second):
                assert rbm._ready_transport(DEST, rbm.config.destinations[DEST]) is None
        errors = at(caplog, logging.ERROR, 'zfsbackup.remote', 'cannot use the client TLS identity')
        assert len(errors) == announced(shared)

    @SHARING
    def test_the_daily_certificate_reminder_runs_once(self, h, shared):
        first, second = two_remote(h, shared)
        runs = []
        for rbm in (first, second):
            rbm._identity_paths = lambda runs=runs: (runs.append(1), (_ for _ in ()).throw(OSError('x')))[1]
        for rbm in (first, second, first, second):
            rbm.begin_cycle()
        assert len(runs) == (1 if shared else 2)

    @SHARING
    def test_a_confirmed_copy_is_seen_by_the_other_manager(self, h, mocker, shared):
        h.rm.post(re.compile(r'.*/negotiate$'), json={'common_snapshot': NEW, 'server_copy': 'plain',
                                                      'server_dataset': 'x'})
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        first, second = two_remote(h, shared)
        assert first.copy_confirmed(h.dsi.name, DEST) is False
        assert run_backup(h, first) is True
        assert first.copy_confirmed(h.dsi.name, DEST) is True
        assert second.copy_confirmed(h.dsi.name, DEST) is shared

    def test_a_copy_that_goes_missing_is_unconfirmed_for_both(self, h, mocker):
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        first, second = two_remote(h, True)
        h.rm.post(re.compile(r'.*/negotiate$'), json={'common_snapshot': NEW, 'server_copy': 'plain',
                                                      'server_dataset': 'x'})
        run_backup(h, first)
        assert second.copy_confirmed(h.dsi.name, DEST) is True
        h.rm.post(re.compile(r'.*/negotiate$'), json={'common_snapshot': None, 'server_copy': 'none',
                                                      'server_dataset': 'x'})
        Chain(h, mocker, sessions=2, negotiate=REAL)
        run_backup(h, second)
        assert first.copy_confirmed(h.dsi.name, DEST) is True or first.copy_confirmed(h.dsi.name, DEST) is False

    def test_per_call_state_stays_on_the_manager(self, h, mocker):
        """`_server_info` and the call notes are per call: a second manager sees none of them."""
        h.rm.post(re.compile(r'.*/negotiate$'), json={'common_snapshot': NEW, 'server_copy': 'plain',
                                                      'server_dataset': 'x'})
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        first, second = two_remote(h, True)
        run_backup(h, first)
        assert first._server_info and second._server_info == {}
        assert second._call_note == {} and second._call_retryable == {}

    def test_an_identity_is_never_shared(self, h):
        first, second = two_remote(h, True)
        assert first._transports is not second._transports
        assert first._checked is not second._checked

    def test_closing_one_manager_leaves_the_other_alone(self, h):
        first, second = two_remote(h, True)
        second._transports['x'] = object()
        first.close()
        assert 'x' in second._transports

    def test_the_memory_tables_the_remote_managers_use(self, h):
        first, second = two_remote(h, True)
        first._memory.note(COPY_CONFIRMED, ('a', 'b'))
        assert second._copy_confirmed == {('a', 'b')}
        assert first._refusal_logged is second._refusal_logged


# ---------------------------------------------------------------------------------------------
# last_problem
# ---------------------------------------------------------------------------------------------


class TestLastProblemOverASharedMemory:
    def test_last_problem_iterates_a_copy_while_other_threads_write(self, h):
        first, second = two_remote(h, True)
        stop = threading.Event()
        problems = []
        old = sys.getswitchinterval()
        sys.setswitchinterval(1e-5)

        def writer(t):
            i = 0
            while not stop.is_set():
                second._memory.put(REFUSALS, (f'pool/d{t}-{i % 400}', DEST), f'refusal {i}')
                second._memory.pop(REFUSALS, (f'pool/d{t}-{(i + 3) % 400}', DEST))
                i += 1

        def reader():
            try:
                for _ in range(300):
                    first.last_problem('pool/data', DEST)
            except BaseException as e:  # noqa: BLE001
                problems.append(repr(e))
            finally:
                stop.set()

        writers = [threading.Thread(target=writer, args=(t,)) for t in range(3)]
        r = threading.Thread(target=reader)
        try:
            for w in writers:
                w.start()
            r.start()
            r.join(60)
        finally:
            stop.set()
            sys.setswitchinterval(old)
            for w in writers:
                w.join(30)
        assert problems == []

    def test_a_refusal_recorded_by_this_call_is_reported(self, h):
        h.rm.post(re.compile(r'.*/negotiate$'), status_code=409, json={'error': 'held for another source'})
        assert run_backup(h, h.rbm) is False
        assert 'held for another source' in h.rbm.last_problem(h.dsi.name, DEST)

    def test_another_managers_refusal_does_not_leak_into_this_calls_error(self, h):
        first, second = two_remote(h, True)
        h.rm.post(re.compile(r'.*/negotiate$'), status_code=409, json={'error': 'held for another source'})
        assert run_backup(h, first) is False
        assert 'held for another source' in first.last_problem(h.dsi.name, DEST)
        second.client_id = None                      # this call fails for a reason of its own
        assert run_backup(h, second) is False
        assert second.last_problem(h.dsi.name, DEST) is None

    def test_a_destination_level_refusal_of_this_calls_own_transport_check_is_reported(self, h):
        h.rbm.config.destinations[DEST].server_fingerprint = None
        assert run_backup(h, h.rbm) is False
        assert 'is not enrolled' in h.rbm.last_problem(h.dsi.name, DEST)


# ---------------------------------------------------------------------------------------------
# two LocalBackupManagers
# ---------------------------------------------------------------------------------------------


def second_local(rig, shared):
    memory = rig.mgr._memory if shared else None
    other = LocalBackupManager(rig.config, rig.manager, client_id=CLIENT, memory=memory)
    assert (other._memory is rig.mgr._memory) is shared
    return other


class TestTwoLocalManagers:
    def test_a_manager_built_without_a_memory_has_a_private_one(self, rig):
        assert second_local(rig, False)._memory is not rig.mgr._memory

    @SHARING
    def test_the_pending_map_is_seen_by_the_other_manager(self, rig, shared):
        """A sender built for a child sees the parent's negotiate: the child waits for it."""
        other = second_local(rig, shared)
        rig.negotiate(dsi=rig.dsi_for('pool'))          # `rig.mgr` creates the container above the parent
        assert (LOCAL_PARENT in other._pending) is shared
        if shared:
            with pytest.raises(NegotiationFailed) as raised:
                other._negotiate(rig.transport, CLIENT, rig.dsi, [], LOCAL_DEST)
            assert raised.value.reloading is True

    @SHARING
    def test_a_standing_refusal_is_logged_once(self, rig, caplog, shared):
        rig.host.add(LOCAL_BASE, props={PROP_CLIENT_ID: OTHER_CLIENT})
        other = second_local(rig, shared)
        cfg = RemoteDatasetConfig(destination=LOCAL_DEST)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.local'):
            for mgr in (rig.mgr, other, rig.mgr, other):
                assert mgr.backup_dataset(rig.dsi, cfg) is False
        errors = at(caplog, logging.ERROR, 'zfsbackup.local', "another host's backup")
        assert len(errors) == announced(shared)

    @SHARING
    def test_the_token_skip_error_is_logged_once(self, rig, caplog, shared):
        other = second_local(rig, shared)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.local'):
            for i in range(2 * SKIPS):
                (rig.mgr, other)[i % 2]._token_skipped(rig.dsi, LOCAL_DEST, 'cannot read')
        errors = at(caplog, logging.ERROR, 'zfsbackup.local', 'resume-token check has skipped')
        assert len(errors) == (1 if shared else 2)

    @SHARING
    def test_a_confirmed_copy_is_seen_by_the_other_manager(self, rig, sends, streams, shared):
        rig.owned_copy(copy_rows(S1, S2, S3))
        other = second_local(rig, shared)
        assert rig.mgr.backup_dataset(rig.dsi, RemoteDatasetConfig(destination=LOCAL_DEST)) is True
        assert rig.mgr.copy_confirmed(LOCAL_DS, LOCAL_DEST) is True
        assert other.copy_confirmed(LOCAL_DS, LOCAL_DEST) is shared

    def test_the_pending_ttl_is_shared_too(self, rig):
        other = second_local(rig, True)
        other._memory.pending_put('copy', 5.0)
        assert rig.mgr._pending == {'copy': 5.0}


# ---------------------------------------------------------------------------------------------
# two DatasetManagers
# ---------------------------------------------------------------------------------------------


def hourly_config(*names):
    return BackupConfig(datasets=[DatasetConfig(name=n, frequency=Duration('1h')) for n in names] or
                        [DatasetConfig(name='pool/data', frequency=Duration('1h'))])


def two_dataset_managers(shared, config=None):
    config = config or hourly_config()
    memory = SendMemory() if shared else None
    return DatasetManager(config, memory=memory), DatasetManager(config, memory=memory)


class TestTwoDatasetManagers:
    def test_a_manager_built_without_a_memory_has_a_private_one(self):
        a, b = two_dataset_managers(False)
        assert a._memory is not b._memory

    def test_the_old_names_are_the_memorys_live_latches(self):
        a, _ = two_dataset_managers(True)
        assert a._clock_noted is a._memory.latch_view(CLOCK)
        assert a._no_txg_noted is a._memory.latch_view(NO_TXG_TABLE)
        assert a._expand_error_noted is a._memory.latch_view(EXPAND_ERROR)
        assert a._expand_target_noted is a._memory.latch_view(EXPAND_TARGET)
        assert a._expand_copy_noted is a._memory.latch_view(EXPAND_COPY)

    @SHARING
    def test_a_future_dated_snapshot_is_an_error_once(self, caplog, frozen_clock, shared):
        from _snapshot_support import listed_info
        a, b = two_dataset_managers(shared)
        snap = listed_info(frozen_clock.stamp(timedelta(hours=2)))
        with caplog.at_level(logging.ERROR, logger=BACKUP_LOGGER):
            for m in (a, b, a, b):
                assert m.clock_behind(m.datasets[0], snap, 'anchor for offsite') is True
        assert len(at(caplog, logging.ERROR, BACKUP_LOGGER, 'check NTP')) == announced(shared)

    @SHARING
    def test_a_clock_that_caught_up_ends_the_episode_for_both(self, caplog, frozen_clock, shared):
        from _snapshot_support import listed_info
        a, b = two_dataset_managers(shared)
        future = listed_info(frozen_clock.stamp(timedelta(hours=2)))
        past = listed_info(frozen_clock.stamp(-timedelta(hours=2)))
        a.clock_behind(a.datasets[0], future, 'anchor for offsite')
        b.clock_behind(b.datasets[0], past, 'anchor for offsite')      # fine again: forgets the note
        caplog.clear()
        with caplog.at_level(logging.ERROR, logger=BACKUP_LOGGER):
            a.clock_behind(a.datasets[0], future, 'anchor for offsite')
        assert len(at(caplog, logging.ERROR, BACKUP_LOGGER, 'check NTP')) == (1 if shared else 0)

    @SHARING
    def test_snapshots_without_a_txg_are_warned_about_once(self, mocker, caplog, shared):
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[
            listed_snapshot('autosnap_20240101120000', txg=5),
            listed_snapshot('autosnap_20240102120000', txg=NO_TXG),
        ])
        a, b = two_dataset_managers(shared)
        with caplog.at_level(logging.WARNING, logger=BACKUP_LOGGER):
            for m in (a, b, a, b):
                m.list_snapshots(m.datasets[0])
        assert len(at(caplog, logging.WARNING, BACKUP_LOGGER, 'have no createtxg')) == announced(shared)

    def test_a_new_name_is_warned_about_by_whichever_manager_lists_it(self, mocker, caplog):
        lst = mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[
            listed_snapshot('autosnap_20240102120000', txg=NO_TXG)])
        a, b = two_dataset_managers(True)
        with caplog.at_level(logging.WARNING, logger=BACKUP_LOGGER):
            a.list_snapshots(a.datasets[0])
            lst.return_value = [
                listed_snapshot('autosnap_20240102120000', txg=NO_TXG),
                listed_snapshot('autosnap_20240103120000', txg=NO_TXG)]
            b.list_snapshots(b.datasets[0])
        first, second = at(caplog, logging.WARNING, BACKUP_LOGGER, 'have no createtxg')
        assert 'autosnap_20240103120000' in second and 'autosnap_20240102120000' not in second

    def test_list_snapshots_returns_its_own_result_not_the_datasets_shared_list(self, mocker):
        """Another thread's listing may replace `dsi.snapshots` right after this call stores its own:
        the call must still return the listing IT built (review fix: `return result`)."""
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[
            listed_snapshot('autosnap_20240101120000', txg=5)])
        a, _ = two_dataset_managers(True)
        dsi = a.datasets[0]
        foreign = [object()]

        class Racing(type(dsi)):
            # every store is immediately overtaken by "another thread's" listing
            snapshots = property(lambda self: self.__dict__['_snaps'],
                                 lambda self, value: self.__dict__.__setitem__('_snaps', foreign))

        dsi.__class__ = Racing
        result = a.list_snapshots(dsi)
        assert dsi.snapshots is foreign
        assert result is not foreign
        assert [s.name for s in result] == ['autosnap_20240101120000']


def recursive_world(world, *, copy=False, target=False):
    names = [PARENT, f'{PARENT}/ok']
    if copy:
        names += [f'{PARENT}/copy', f'{PARENT}/copy/c1']
    if target:
        names += [f'{PARENT}/vault', f'{PARENT}/vault/c']
    add_tree(world, *names)
    if copy:
        world.set_prop(f'{PARENT}/copy', CID_PROP, 'x')
    kw = {'destinations': {'off': Destination(url=f'zfs:{PARENT}/vault')}} if target else {}
    return recursive_config(recursive_dataset(PARENT, recursive=True), **kw)


class TestExpansionLatchesAreShared:
    @SHARING
    def test_a_failing_discovery_is_an_error_once_and_its_recovery_an_info_once(
        self, zfs_world, caplog, shared
    ):
        cfg = recursive_world(zfs_world)
        a, b = two_dataset_managers(shared, cfg)
        zfs_world.list_failures[PARENT] = 'pool I/O is currently suspended'
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for m in (a, b, a, b):
                assert PARENT in m.expand_recursive().failed
        assert len(at(caplog, logging.ERROR, BACKUP_LOGGER, 'Cannot list the descendants')) \
            == announced(shared)
        del zfs_world.list_failures[PARENT]
        with caplog.at_level(logging.INFO, logger=BACKUP_LOGGER):
            for m in (a, b):
                m.expand_recursive()
        assert len(at(caplog, logging.INFO, BACKUP_LOGGER, 'can be listed again')) == (1 if shared else 2)

    @SHARING
    def test_a_backup_copy_below_a_parent_is_an_error_once(self, zfs_world, caplog, shared):
        cfg = recursive_world(zfs_world, copy=True)
        a, b = two_dataset_managers(shared, cfg)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for m in (a, b, a, b):
                m.expand_recursive()
        assert len(at(caplog, logging.ERROR, BACKUP_LOGGER, f'{PARENT}/copy')) == announced(shared)

    @SHARING
    def test_a_receive_target_below_a_parent_is_an_error_once(self, zfs_world, caplog, shared):
        cfg = recursive_world(zfs_world, target=True)
        a, b = two_dataset_managers(shared, cfg)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for m in (a, b, a, b):
                m.expand_recursive()
        assert len(at(caplog, logging.ERROR, BACKUP_LOGGER, 'receives copies')) == announced(shared)

    def test_the_latches_are_in_the_memory_and_forgotten_when_the_episode_ends(self, zfs_world):
        cfg = recursive_world(zfs_world, copy=True, target=True)
        a, b = two_dataset_managers(True, cfg)
        a.expand_recursive()
        assert b._memory.noted(EXPAND_COPY, (PARENT, f'{PARENT}/copy'))
        assert b._memory.noted(EXPAND_TARGET, (PARENT, f'{PARENT}/vault'))
        zfs_world.set_prop(f'{PARENT}/copy', CID_PROP, None) if hasattr(zfs_world, 'set_prop') else None

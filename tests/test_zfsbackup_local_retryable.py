"""`ChainSender.last_retryable` through the real `LocalBackupManager.backup_dataset` (Track B, item B1.2).

The local destination has no server, so the retryable cases are the ones the manager itself
classifies: an ancestor copy that is not settled yet (the pending map or a first receive's partial
state: the retryable `NegotiationFailed`), and a stop (before the first stream, between steps).
Everything else is False: a missing or vanished target, a stuck child, a `zfs` timeout, no client id,
an ownership refusal, a receive that failed. The flag is per call: reset at the start of every call.

The `zfs` commands are `tests/_local_support.FakeHost`, the streams real pipes with scripted exit
statuses (as in `test_zfsbackup_local.py`, whose rig and fixtures are reused); nothing reaches `zfs`.
"""

import time

import pytest

from libzfseasy import ZFSTimeoutError
from test_zfsbackup_local import (  # noqa: F401  (fixtures and the rig, not tests)
    BASE, COPY, DEST, DS, OTHER_CLIENT, PARENT, S1, S2, S3, TARGET, LocalRig, RemoteDatasetConfig,
    ServerRow, _bounded_pump, commits, copy_rows, local, owned_props, rig, sends, snapshot_presence,
    streams, PROP_CLIENT_ID, CLIENT,
)

pytestmark = pytest.mark.unit


def backup(rig, **kw):
    return rig.mgr.backup_dataset(rig.dsi, RemoteDatasetConfig(destination=DEST), **kw)


def flag(rig):
    return rig.mgr.last_retryable(DS, DEST)


class TestNotRetryable:
    def test_a_missing_target(self, rig):
        rig.host.datasets.pop(TARGET)
        assert backup(rig) is False and flag(rig) is False

    def test_no_client_id(self, mocker):
        r = LocalRig(mocker, client_id=None)
        assert backup(r) is False and flag(r) is False

    def test_a_stuck_child(self, rig, mocker):
        mocker.patch.object(local, '_stuck_problem', return_value='a child is stuck')
        assert backup(rig) is False and flag(rig) is False

    def test_a_zfs_timeout_in_the_negotiate(self, rig):
        rig.host.fail_on('exists', ZFSTimeoutError('slow'), name=COPY)
        assert backup(rig) is False and flag(rig) is False

    def test_a_zfs_timeout_in_the_snapshot_listing_of_the_copy(self, rig, mocker):
        rig.owned_copy(copy_rows(S1))
        mocker.patch('zfsbackup.receiver.server_snapshots', side_effect=ZFSTimeoutError('slow'))
        assert backup(rig) is False and flag(rig) is False

    def test_a_target_that_vanished_mid_negotiate(self, rig):
        rig.host.fail_on('exists', RuntimeError('I/O error'), name=COPY)
        rig.host.datasets.pop(TARGET)
        assert backup(rig) is False and flag(rig) is False

    def test_an_unexpected_zfs_failure(self, rig):
        rig.host.fail_on('exists', RuntimeError('I/O error'), name=COPY)
        assert backup(rig) is False and flag(rig) is False

    def test_an_ancestor_of_another_client(self, rig):
        rig.host.add(BASE, props={PROP_CLIENT_ID: OTHER_CLIENT})
        assert backup(rig) is False and flag(rig) is False

    def test_a_conflicting_ancestor_that_is_not_a_backup(self, rig):
        rig.host.add(BASE, props={})
        assert backup(rig) is False and flag(rig) is False

    def test_a_copy_that_belongs_to_another_client_dataset(self, rig):
        rig.host.add_copy(snaps=copy_rows(S1), props=owned_props(source='pool/someone-else'))
        assert backup(rig) is False and flag(rig) is False

    def test_a_receive_failure(self, rig, sends, streams):
        rig.owned_copy(copy_rows(S1))
        rig.host.receives.append(
            streams.receive(rc=1, stderr='cannot receive: no space', events=rig.host.events))
        assert backup(rig) is False and flag(rig) is False

    def test_an_unknown_destination(self, rig):
        rig.mgr.config.destinations.pop(DEST)
        assert backup(rig) is False and flag(rig) is False

    def test_a_dry_run(self, mocker):
        r = LocalRig(mocker, dry_run=True)
        r.owned_copy(copy_rows(S1))
        assert backup(r) is True and flag(r) is False


class TestRetryable:
    def test_a_first_receive_of_the_parent_that_has_not_completed(self, rig):
        """The parent was negotiated but not received yet: its child waits (`pending`)."""
        rig.negotiate(dsi=rig.dsi_for('pool'))
        assert PARENT in rig.mgr._pending
        assert backup(rig) is False and flag(rig) is True

    def test_a_pending_entry_put_straight_into_the_memory(self, rig):
        rig.mgr._memory.pending_put(PARENT, time.monotonic())
        assert backup(rig) is False and flag(rig) is True

    def test_an_ancestor_holding_a_first_receives_partial_state(self, rig):
        rig.host.add(BASE, props={PROP_CLIENT_ID: CLIENT})
        rig.host.add(PARENT, props={PROP_CLIENT_ID: CLIENT}, token='1-abc')
        assert backup(rig) is False and flag(rig) is True

    def test_a_stop_before_the_first_stream(self, rig, sends, streams):
        rig.owned_copy(copy_rows(S1))
        assert backup(rig, should_stop=lambda: True) is False and flag(rig) is True

    def test_a_stop_between_steps_returns_true_and_is_retryable(self, rig, sends, streams):
        rig.owned_copy(copy_rows(S1))
        commits(rig, streams, S2, S3)
        answers = iter([False, False, True])
        assert backup(rig, should_stop=lambda: next(answers, True)) is True
        assert flag(rig) is True


class TestLifecycle:
    def test_a_complete_chain_is_not_retryable(self, rig, sends, streams):
        rig.owned_copy(copy_rows(S1))
        commits(rig, streams, S2, S3)
        assert backup(rig, should_stop=lambda: False) is True and flag(rig) is False

    def test_the_flag_resets_at_the_start_of_every_call(self, rig):
        rig.mgr._memory.pending_put(PARENT, time.monotonic())
        assert backup(rig) is False and flag(rig) is True
        rig.mgr._memory.pending_clear(PARENT)
        rig.host.datasets.pop(TARGET)
        assert backup(rig) is False and flag(rig) is False

    def test_a_pending_entry_that_expired_is_not_a_wait(self, rig, mocker):
        rig.mgr._memory.pending_put(PARENT, time.monotonic() - local.LOCAL_PENDING_TTL - 1)
        rig.host.add(BASE, props={PROP_CLIENT_ID: CLIENT})
        assert rig.mgr._is_pending(PARENT) is False

    def test_it_is_false_for_a_pair_never_run(self, rig):
        assert rig.mgr.last_retryable('pool/other', DEST) is False

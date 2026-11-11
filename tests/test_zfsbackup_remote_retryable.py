"""`ChainSender.last_retryable` through the real `RemoteBackupManager.backup_dataset` (Track B, item B1.2).

`last_retryable(dataset, destination)` says whether the last `backup_dataset` call ended in something
worth retrying soon. It is a per-call flag set where the chain, the transport or the receiver already
classifies the case, never read back from a message, and reset at the start of every call.

True: the receiver busy / reloading / draining (the retryable 503 of a negotiate, `/whoami`, a
resume-token request or a stream's ack), an unsettled ancestor, a failure to connect before any byte
(`/whoami` unreachable, a stream connect that failed, retries exhausted), a stop before the first
stream, between steps, after a resumed step or before a resume.
False: `zfs_timeout`, resume-token skips, every refusal (409, 400, `server_newer`, `base_missing`,
`no_common`, authentication and pin refusals, a mode mismatch, rollback and local-anchor ERRORs), an
unknown destination, no client id, not enrolled, identity failures, a failure in the middle of a
stream. For a call that returned True: True only when the chain stopped between steps with work left.

The transport is faked as in `test_zfsbackup_remote.py` (`requests_mock`, a recorded websocket connect);
nothing reaches `zfs` or a socket.
"""

import json
import re

import pytest
import requests
import websocket

from _remote_support import (  # noqa: F401  (fixtures)
    _client_tls_template, client_tls_dir, fake_connect_pinned, snapshot_presence, whoami_json,
)
from test_zfsbackup_remote import (  # noqa: F401  (fixtures and harnesses, not tests)
    BASE, MID, NEW, OLD, REAL, TOKEN_INCR, Chain, S1, S2, S3, S5, _no_local_zfs_property_writes,
    _snapshots_are_present, _tls_connect_is_faked, h, h5, make_h, sinfo, sleeps, upgrade_refused,
)

pytestmark = pytest.mark.unit

DEST = 'offsite'
COMMON = {'common_snapshot': OLD, 'server_copy': 'plain', 'server_dataset': 'x'}
RELOADING = {'error': 'server is reloading; retry later', 'retry': True}
BUSY = {'error': 'busy', 'retry': True}
ZFS_TIMEOUT = {'error': 'zfs timed out; retry later', 'retry': True, 'reason': 'zfs_timeout'}
UNSETTLED = {'error': 'waiting', 'retry': True, 'reason': 'unsettled_ancestor', 'ancestor': 'pool'}


def negotiate_route(h, **kw):
    h.rm.post(re.compile(r'.*/negotiate$'), **kw)


def backup(h, mocker, *, should_stop=None, sessions=3, negotiate=REAL, snaps=None):
    """`backup_dataset` for real; -> (result, last_retryable, chain)."""
    mocker.patch.object(h.manager, 'list_snapshots', return_value=snaps or h.snaps)
    chain = Chain(h, mocker, sessions=sessions, negotiate=negotiate)
    result = h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0], should_stop=should_stop)
    return result, h.rbm.last_retryable(h.dsi.name, DEST), chain


def ack(chain, index, payload):
    ws = chain.sessions[index]
    ws.recv.side_effect = None
    ws.recv.return_value = json.dumps(payload)


@pytest.fixture
def common(h):
    negotiate_route(h, json=COMMON)
    return h


# ---------------------------------------------------------------------------------------------
# negotiate
# ---------------------------------------------------------------------------------------------


class TestNegotiate:
    @pytest.mark.parametrize('body,expected', [
        (BUSY, True),
        (RELOADING, True),
        (UNSETTLED, True),
        (ZFS_TIMEOUT, False),
    ], ids=['busy', 'reloading', 'unsettled_ancestor', 'zfs_timeout'])
    def test_a_503(self, h, mocker, body, expected):
        negotiate_route(h, status_code=503, json=body)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is expected

    @pytest.mark.parametrize('status,body', [
        (409, {'error': 'held for another source'}),
        (409, {'error': 'unowned', 'reason': 'unowned'}),
        (400, {'error': 'bad config'}),
        (400, {'error': 'name too long', 'reason': 'name_too_long'}),
        (500, {}),
    ])
    def test_a_refusal_is_not_retryable(self, h, mocker, status, body):
        negotiate_route(h, status_code=status, json=body)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_a_server_copy_of_the_other_mode_is_not_retryable(self, h, mocker):
        negotiate_route(h, json={'common_snapshot': None, 'server_copy': 'raw', 'server_dataset': 'x'})
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    @pytest.mark.parametrize('exc', [
        requests.ConnectionError('down'), requests.exceptions.ConnectTimeout('slow'),
    ], ids=['connection_error', 'connect_timeout'])
    def test_a_connection_failure_before_any_answer_is_retryable(self, h, mocker, exc):
        negotiate_route(h, exc=exc)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is True

    def test_a_negotiate_that_cannot_be_parsed_is_not_retryable(self, h, mocker):
        negotiate_route(h, status_code=200, text='not json')
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False


# ---------------------------------------------------------------------------------------------
# /whoami and the destination checks
# ---------------------------------------------------------------------------------------------


class TestReadyTransport:
    def test_whoami_503_reloading_is_retryable(self, h, mocker):
        h.rm.get(f'{BASE}/whoami', status_code=503, json=RELOADING)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is True

    def test_whoami_unreachable_is_retryable(self, h, mocker):
        h.rm.get(f'{BASE}/whoami', exc=requests.ConnectionError('down'))
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is True

    def test_a_cached_whoami_failure_stays_retryable_for_the_next_dataset_of_the_cycle(self, h, mocker):
        h.rm.get(f'{BASE}/whoami', status_code=503, json=RELOADING)
        h.rbm.begin_cycle()
        assert backup(h, mocker)[1] is True
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is True
        whoamis = [r for r in h.rm.request_history if r.path.endswith('/whoami')]
        assert len(whoamis) == 1, 'the second call reused the cached check'

    @pytest.mark.parametrize('status,body', [
        (403, {'error': 'client not trusted'}),
        (403, {'error': 'client_id mismatch', 'bound': 'other'}),
        (401, {'error': 'no certificate'}),
        (426, {'error': 'protocol', 'protocol': 99}),
    ])
    def test_whoami_refusals_are_not_retryable(self, h, mocker, status, body):
        h.rm.get(f'{BASE}/whoami', status_code=status, json=body)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_a_server_that_binds_another_id_is_not_retryable(self, h, mocker):
        h.rm.get(f'{BASE}/whoami', json=whoami_json('someone-else'))
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_no_client_id_is_not_retryable(self, h, mocker):
        h.rbm.client_id = None
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_a_destination_that_is_not_enrolled_is_not_retryable(self, h, mocker):
        h.rbm.config.destinations[DEST].server_fingerprint = None
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_an_unknown_destination_is_not_retryable(self, h, mocker):
        h.rbm.config.destinations.pop(DEST)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_a_local_destination_given_to_the_remote_manager_is_not_retryable(self, h, mocker):
        h.rbm.config.destinations[DEST].url = 'zfs:bk/backups'
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    @pytest.mark.parametrize('how', ['no_client_id', 'not_enrolled'])
    def test_a_transport_refused_before_whoami_is_not_retryable_even_after_a_retryable_whoami(
        self, h, mocker, how
    ):
        """An earlier `/whoami` of this cycle flagged the destination; a later call that never reaches
        `/whoami` must not read the stale flag."""
        h.rm.get(f'{BASE}/whoami', status_code=503, json=RELOADING)
        h.rbm.begin_cycle()
        assert backup(h, mocker)[1] is True
        if how == 'no_client_id':
            h.rbm.client_id = None
        else:
            h.rbm.config.destinations[DEST].server_fingerprint = None
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_a_failed_transport_build_is_not_retryable_after_a_retryable_whoami(self, h, mocker):
        h.rm.get(f'{BASE}/whoami', status_code=503, json=RELOADING)
        h.rbm.begin_cycle()
        assert backup(h, mocker)[1] is True
        mocker.patch.object(h.rbm, '_transport', return_value=None)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_an_identity_failure_is_not_retryable_after_a_retryable_whoami(self, h, mocker):
        h.rm.get(f'{BASE}/whoami', status_code=503, json=RELOADING)
        h.rbm.begin_cycle()
        assert backup(h, mocker)[1] is True
        mocker.patch.object(h.rbm, '_client_identity', return_value=None)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False


# ---------------------------------------------------------------------------------------------
# the resume token
# ---------------------------------------------------------------------------------------------


class TestResumeToken:
    @pytest.mark.parametrize('status,body,expected', [
        (503, RELOADING, True),
        (503, BUSY, True),
        (503, ZFS_TIMEOUT, False),
        (500, {}, False),
        (404, {'error': 'nope'}, False),
    ], ids=['reloading', 'busy', 'zfs_timeout', 'http_500', 'http_404'])
    def test_responses(self, h, common, mocker, status, body, expected):
        h.rm.get(h.url, status_code=status, json=body)
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is expected

    def test_an_undecodable_token_skips_the_cycle_without_being_retryable(self, h, common, mocker):
        h.token()
        h.resume_info.side_effect = RuntimeError('cannot decode')
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_a_stale_token_falls_through_to_a_fresh_send_and_is_not_retryable(self, h, common, mocker):
        h.token()
        h.decodes_to(toguid=999, fromguid=111)
        done, retry, _ = backup(h, mocker)
        assert done is True and retry is False

    def test_a_stop_before_a_resume_is_retryable(self, h):
        h.token()
        assert h.start(should_stop=lambda: True) is None
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    def test_a_resume_token_503_counted_as_a_zfs_timeout_is_not_retryable_through_start(self, h):
        h.rm.get(h.url, status_code=503, json=ZFS_TIMEOUT)
        assert h.start() is None
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False


# ---------------------------------------------------------------------------------------------
# the stream: connect, acknowledgement, a failure mid-stream
# ---------------------------------------------------------------------------------------------


class TestStream:
    def test_a_connect_failure_on_the_first_stream_is_retryable(self, common, mocker):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        chain.sessions[0].connect.side_effect = OSError('refused')
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    @pytest.mark.parametrize('exc', [
        TimeoutError('timed out'),
        websocket.WebSocketTimeoutException('timed out'),
        websocket.WebSocketConnectionClosedException('closed'),
        ConnectionResetError('reset'),
    ], ids=['timeout', 'ws_timeout', 'ws_closed', 'reset'])
    def test_connection_level_failures_on_the_upgrade_are_retryable(self, common, mocker, exc):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        chain.sessions[0].connect.side_effect = exc
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    def test_an_upgrade_refused_with_503_is_retryable(self, common, mocker):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        chain.sessions[0].connect.side_effect = upgrade_refused(503, 'busy')
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    @pytest.mark.parametrize('status', [409, 400, 500])
    def test_an_upgrade_refused_with_another_status_is_not_retryable(self, common, mocker, status):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        chain.sessions[0].connect.side_effect = upgrade_refused(status, 'held for another source')
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False

    @pytest.mark.parametrize('status,error', [
        (403, 'client not trusted'), (403, 'client_id mismatch'), (401, 'no certificate'),
        (426, 'protocol'),
    ])
    def test_an_authentication_refusal_on_the_upgrade_is_not_retryable(self, common, mocker, status, error):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        chain.sessions[0].connect.side_effect = upgrade_refused(status, error)
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False

    def test_a_connect_that_keeps_failing_after_a_good_step_is_retryable_once_the_retries_are_spent(
        self, common, mocker
    ):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=8, negotiate=REAL)
        for ws in chain.sessions[1:]:
            ws.connect.side_effect = OSError('refused')
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    def test_a_stop_during_the_connect_retry_waits_is_retryable(self, common, mocker):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=8, negotiate=REAL)
        for ws in chain.sessions[1:]:
            ws.connect.side_effect = OSError('refused')
        # entry, before step 1, before step 2, then the first wait slice
        stop = h.stopper(False, False, False, True)
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0], should_stop=stop) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    def test_an_ack_that_says_reloading_is_retryable(self, common, mocker):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        ack(chain, 0, {'status': 'error', **RELOADING})
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    def test_an_ack_that_says_busy_is_retryable(self, common, mocker):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        ack(chain, 0, {'status': 'error', **BUSY})
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    @pytest.mark.parametrize('reason', ['server_newer', 'base_missing', 'no_common', 'listing'])
    def test_an_ack_refusal_is_not_retryable(self, common, mocker, reason):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        ack(chain, 0, {'status': 'error', 'error': 'refused', 'reason': reason})
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False

    def test_a_plain_ack_error_is_not_retryable(self, common, mocker):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        ack(chain, 0, {'status': 'error', 'error': 'receive failed'})
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False

    def test_a_send_that_failed_with_a_reloading_refusal_left_in_the_socket_is_retryable(
        self, common, mocker
    ):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        ws = chain.sessions[0]
        ws.send_binary.side_effect = BrokenPipeError('closed by the server')
        ws.recv.side_effect = None
        ws.recv.return_value = json.dumps({'status': 'error', **RELOADING})
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True

    def test_a_send_that_failed_with_a_real_refusal_left_in_the_socket_is_not_retryable(
        self, common, mocker
    ):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        ws = chain.sessions[0]
        ws.send_binary.side_effect = BrokenPipeError('closed by the server')
        ws.recv.side_effect = None
        ws.recv.return_value = json.dumps(
            {'status': 'error', 'error': 'refused', 'reason': 'server_newer'})
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False

    def test_a_failure_in_the_middle_of_a_stream_is_not_retryable(self, common, mocker):
        h = common
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        ws = chain.sessions[0]
        ws.send_binary.side_effect = BrokenPipeError('closed')
        ws.recv.side_effect = websocket.WebSocketConnectionClosedException('nothing left')
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False


# ---------------------------------------------------------------------------------------------
# stops
# ---------------------------------------------------------------------------------------------


class TestStops:
    def test_a_stop_before_the_first_stream_is_retryable(self, common, mocker):
        done, retry, _ = backup(common, mocker, should_stop=lambda: True)
        assert done is False and retry is True

    def test_a_stop_between_steps_returns_true_and_is_retryable(self, common, mocker):
        done, retry, _ = backup(common, mocker, should_stop=common.stopper(False, False, True))
        assert done is True and retry is True

    def test_a_stop_after_a_resumed_step_returns_true_and_is_retryable(self, h5, mocker):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        negotiate_route(h5, json={'common_snapshot': S1, 'server_copy': 'plain', 'server_dataset': 'x'})
        # entry, after the token is accepted, after the resume
        done, retry, _ = backup(
            h5, mocker, should_stop=h5.stopper(False, False, True), sessions=6, snaps=h5.snaps)
        assert done is True and retry is True

    def test_a_stop_polled_but_never_true_is_not_retryable(self, common, mocker):
        done, retry, _ = backup(common, mocker, should_stop=lambda: False)
        assert done is True and retry is False

    def test_a_complete_chain_is_not_retryable(self, common, mocker):
        done, retry, _ = backup(common, mocker)
        assert done is True and retry is False

    def test_a_dry_run_is_not_retryable(self, common, mocker):
        common.rbm.config.dry_run = True
        done, retry, _ = backup(common, mocker)
        assert done is True and retry is False

    def test_up_to_date_is_not_retryable(self, h, mocker):
        negotiate_route(h, json={'common_snapshot': NEW, 'server_copy': 'plain', 'server_dataset': 'x'})
        done, retry, _ = backup(h, mocker)
        assert done is True and retry is False


# ---------------------------------------------------------------------------------------------
# the flag's life
# ---------------------------------------------------------------------------------------------


class TestFlagLifecycle:
    def test_it_is_false_before_any_call(self, h):
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False

    def test_it_is_false_for_a_pair_never_run(self, h, mocker):
        negotiate_route(h, status_code=503, json=BUSY)
        assert backup(h, mocker)[1] is True
        assert h.rbm.last_retryable('pool/other', DEST) is False
        assert h.rbm.last_retryable(h.dsi.name, 'elsewhere') is False

    def test_it_resets_at_the_start_of_every_call(self, h, mocker):
        negotiate_route(h, status_code=503, json=BUSY)
        assert backup(h, mocker)[1] is True
        negotiate_route(h, json=COMMON)
        done, retry, _ = backup(h, mocker)
        assert done is True and retry is False

    def test_an_unknown_destination_resets_it(self, h, mocker):
        negotiate_route(h, status_code=503, json=BUSY)
        assert backup(h, mocker)[1] is True
        h.rbm.config.destinations.pop(DEST)
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False

    def test_a_non_retryable_failure_after_a_retryable_one_reads_false(self, h, mocker):
        negotiate_route(h, status_code=503, json=BUSY)
        assert backup(h, mocker)[1] is True
        negotiate_route(h, status_code=409, json={'error': 'held'})
        done, retry, _ = backup(h, mocker)
        assert done is False and retry is False

    def test_the_flag_is_per_pair(self, h, mocker):
        negotiate_route(h, status_code=503, json=BUSY)
        backup(h, mocker)
        h.rbm._call_retryable[('pool/other', DEST)] = True
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True
        assert h.rbm.last_retryable('pool/other', DEST) is True
        negotiate_route(h, json=COMMON)
        backup(h, mocker)
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False
        assert h.rbm.last_retryable('pool/other', DEST) is True, 'another pair is untouched'

    def test_it_is_an_exact_bool(self, h, mocker):
        negotiate_route(h, status_code=503, json=BUSY)
        backup(h, mocker)
        assert h.rbm.last_retryable(h.dsi.name, DEST) is True
        assert h.rbm.last_retryable('x', 'y') is False

    def test_a_chain_rebuild_does_not_leave_a_sticky_true(self, common, mocker):
        """Step 1 fails to connect (retryable), its snapshot is found deleted and the chain is rebuilt
        around it; the next step then fails for good: the call's verdict is the LAST failure's."""
        h = common
        gone = {'MID': False}
        full = list(h.snaps)

        def listing(dsi):
            return [s for s in full if not (gone['MID'] and s.name == MID)]

        mocker.patch.object(h.manager, 'list_snapshots', side_effect=listing)
        chain = Chain(h, mocker, sessions=4, negotiate=REAL)
        mocker.patch.object(h.manager, 'list_snapshots', side_effect=listing)

        def refuse_and_delete(*a, **k):
            gone['MID'] = True
            raise OSError('refused')

        chain.sessions[0].connect.side_effect = refuse_and_delete
        ack(chain, 1, {'status': 'error', 'error': 'receive failed'})
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert h.rbm.last_retryable(h.dsi.name, DEST) is False

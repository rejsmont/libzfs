"""S-14, client half: the server's `zfs_timeout` refusals (`api.ZFS_TIMEOUT_ERROR`) skip the cycle.

The API answers a handler whose `zfs` call timed out with HTTP 503 `{'error': 'zfs timed out; retry later',
'retry': True, 'reason': 'zfs_timeout'}` (a websocket text message of the same JSON on `/stream`). The client
treats it as the transient condition it is and never as a reason to send:

* negotiate and stream: the existing "retryable 503" handling already skips the cycle quietly (INFO, no ERROR,
  no fresh-send fallback, no anchor, the in-flight pin kept);
* `/resume_token`: a skip like any unreadable token check -- one WARNING per episode (`_log_warning_once`,
  key `"<dataset> [zfs_timeout]"`, re-armed by a good answer), counted by `_token_skipped` so that a server whose
  zfs stays stuck is escalated by the 3rd consecutive skip's ERROR (S-7a); a "reloading" or "busy" 503 stays
  an uncounted INFO.

The server side is `test_zfsbackup_api_timeouts.py`. The pinned transport is faked (`requests_mock`, the
fixtures of `test_zfsbackup_remote`); no socket, no real `zfs`.
"""

import json
import logging

import pytest

from _api_support import CLIENT_ID, Authed
from _remote_support import (  # noqa: F401  (fixtures)
    _client_tls_template, client_tls_dir, fake_connect_pinned, snapshot_presence,
)
from test_zfsbackup_api import GOOD_CONFIG
from test_zfsbackup_remote import (  # noqa: F401  (fixtures and harnesses, not tests)
    S1, S2, S3, S5, Chain, _no_local_zfs_property_writes, _snapshots_are_present, _tls_connect_is_faked, h, h5, make_h,
    sleeps,
)
from test_zfsbackup_remote_token_error import (  # noqa: F401  (fixtures and the rig, not tests)
    DEST, DS, Rig, _info, _no_sleep, _presence, token_errors,
)
import libzfseasy
from zfsbackup import api
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig
from zfsbackup.remote import RESUME_TOKEN_SKIP_ERROR_AFTER, TOKEN_SKIP_NOTE

pytestmark = pytest.mark.unit

TIMEOUT = api.ZFS_TIMEOUT_ERROR
BUSY = {'error': 'busy', 'retry': True}


def timeout_warnings(caplog):
    return [r for r in caplog.records
            if r.name == 'zfsbackup.remote' and 'could not read the resume state' in r.getMessage()]


def at_least_warning(caplog):
    return [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.fixture
def rig(requests_mock, mocker):
    return Rig(requests_mock, mocker)


def zfs_timeout(rig, dataset=DS):
    rig.rm.get(rig.url(dataset), status_code=503, json=TIMEOUT)


def busy(rig, dataset=DS):
    rig.rm.get(rig.url(dataset), status_code=503, json=BUSY)


# ---------------------------------------------------------------------------
# The wire contract the client keys on
# ---------------------------------------------------------------------------


class TestTheContract:
    def test_the_refusal_is_retryable_and_names_the_reason_the_client_reads(self):
        assert TIMEOUT == {'error': 'zfs timed out; retry later', 'retry': True, 'reason': 'zfs_timeout'}

    def test_the_real_server_reply_is_what_the_client_skips_on(self, rig, mocker, caplog):
        """The server's own answer (a real app, `zfs.exists` timing out) fed to the client's token fetch."""
        mocker.patch('zfsbackup.api.zfs.exists', side_effect=libzfseasy.ZFSTimeoutError('zfs list timed out'))
        authed = Authed(BackupConfig(
            datasets=[DatasetConfig(name=DS)],
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        ))
        reply = authed.client().get(f'/backup/{CLIENT_ID}/{DS}/resume_token')
        assert reply.status_code == 503
        rig.rm.get(rig.url(), status_code=reply.status_code, json=reply.get_json())
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert rig.cycle() == 'skip'
        assert rig.skips() == 1
        assert [r.levelname for r in timeout_warnings(caplog)] == ['WARNING']

    def test_the_real_server_reply_to_negotiate_is_a_quiet_skip(self, h, mocker, caplog):
        mocker.patch('zfsbackup.api.zfs.exists', side_effect=libzfseasy.ZFSTimeoutError('zfs list timed out'))
        authed = Authed(BackupConfig(
            datasets=[DatasetConfig(name='pool/data')],
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        ))
        reply = authed.client().post(
            f'/backup/{CLIENT_ID}/pool/data/negotiate', json={'snapshots': [], 'config': GOOD_CONFIG},
        )
        assert reply.status_code == 503
        h.server_says(status_code=reply.status_code, json=reply.get_json())
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        with caplog.at_level(logging.INFO):
            assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert 'Negotiation failed' not in caplog.text
        h.nothing_started()


# ---------------------------------------------------------------------------
# /resume_token
# ---------------------------------------------------------------------------


class TestResumeTokenZfsTimeout:
    def test_it_is_a_skip_with_no_send_and_no_pin(self, rig):
        zfs_timeout(rig)
        assert rig.cycle() == 'skip'
        rig.set_inflight.assert_not_called()
        rig.send_resume.assert_not_called()
        rig.resume_info.assert_not_called()

    def test_the_first_skip_is_one_warning_and_counted(self, rig, caplog):
        zfs_timeout(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert rig.cycle() == 'skip'
        (warning,) = timeout_warnings(caplog)
        assert warning.levelname == 'WARNING'
        text = warning.getMessage()
        assert DS in text and 'its zfs did not answer' in text and 'skipping this cycle' in text
        assert rig.skips() == 1

    def test_the_warning_is_not_the_reloading_info(self, rig, caplog):
        zfs_timeout(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            rig.cycle()
        assert 'is reloading' not in caplog.text

    def test_a_repeated_timeout_is_debug_after_the_first_warning(self, rig, caplog):
        zfs_timeout(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            rig.cycles(2)
        assert [r.levelname for r in timeout_warnings(caplog)] == ['WARNING', 'DEBUG']
        assert rig.skips() == 2

    def test_the_third_consecutive_skip_is_the_one_error(self, rig, caplog):
        zfs_timeout(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            rig.cycles(RESUME_TOKEN_SKIP_ERROR_AFTER - 1)
            assert token_errors(caplog) == []
            rig.cycle()
            (error,) = token_errors(caplog)
            rig.cycles(4)
        assert token_errors(caplog) == [error]                 # once, not every cycle
        assert "zfs did not answer in time" in error.getMessage()
        assert f'{DS} -> {DEST}' in error.getMessage() and 'falling behind' in error.getMessage()

    def test_last_problem_reports_the_skip(self, rig):
        zfs_timeout(rig)
        rig.cycle()
        note = rig.rbm.last_problem(DS, DEST)
        assert note.startswith(TOKEN_SKIP_NOTE) and "zfs did not answer in time" in note

    def test_a_good_answer_re_arms_the_warning_and_the_error(self, rig, caplog):
        zfs_timeout(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            rig.cycles(3)
            assert len(token_errors(caplog)) == 1
            rig.no_token()
            rig.cycle()                                        # the check passes: the episode is over
            assert rig.skips() == 0
            zfs_timeout(rig)
            rig.cycles(3)
        assert [r.levelname for r in timeout_warnings(caplog)] == [
            'WARNING', 'DEBUG', 'DEBUG', 'WARNING', 'DEBUG', 'DEBUG']
        assert len(token_errors(caplog)) == 2

    def test_a_good_answer_with_a_token_also_re_arms_the_warning(self, rig, caplog):
        zfs_timeout(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            rig.cycle()
            rig.token()
            rig.cycle()
            zfs_timeout(rig)
            rig.cycle()
        assert [r.levelname for r in timeout_warnings(caplog)] == ['WARNING', 'WARNING']

    def test_the_episode_is_per_dataset_and_destination(self, requests_mock, mocker, caplog):
        r = Rig(requests_mock, mocker, datasets=(DS, 'pool/other'), destinations=(DEST, 'second'))
        for ds in (DS, 'pool/other'):
            zfs_timeout(r, ds)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            r.cycle(dataset=DS, destination=DEST)
            r.cycle(dataset='pool/other', destination=DEST)
            r.cycle(dataset=DS, destination='second')
            r.cycle(dataset=DS, destination=DEST)
        assert [r_.levelname for r_ in timeout_warnings(caplog)] == ['WARNING', 'WARNING', 'WARNING', 'DEBUG']
        assert (r.skips(DS, DEST), r.skips('pool/other', DEST), r.skips(DS, 'second')) == (2, 1, 1)

    def test_the_skip_does_not_log_an_error_before_the_threshold(self, rig, caplog):
        zfs_timeout(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            rig.cycles(2)
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_the_skips_add_up_with_other_causes(self, rig, caplog):
        zfs_timeout(rig)
        rig.cycle()
        rig.http_500()
        rig.cycle()
        assert token_errors(caplog) == []
        zfs_timeout(rig)
        rig.cycle()
        assert len(token_errors(caplog)) == 1 and rig.skips() == 3

    def test_a_seeded_count_makes_the_next_timeout_the_error(self, rig, caplog):
        rig.rbm.seed_token_skips({(DS, DEST): RESUME_TOKEN_SKIP_ERROR_AFTER - 1})
        zfs_timeout(rig)
        rig.cycle()
        assert len(token_errors(caplog)) == 1


@pytest.mark.unit
class TestWhatIsStillReloading:
    def test_a_reloading_503_is_info_and_not_counted(self, rig, caplog):
        rig.reloading()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert rig.cycles(5) == ['skip'] * 5
        assert rig.skips() == 0
        assert [r.levelname for r in caplog.records if 'is reloading; skipping' in r.getMessage()] == ['INFO'] * 5
        assert timeout_warnings(caplog) == []
        assert not at_least_warning(caplog)

    def test_a_busy_503_is_not_counted_either(self, rig, caplog):
        busy(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert rig.cycles(4) == ['skip'] * 4
        assert rig.skips() == 0 and token_errors(caplog) == [] and timeout_warnings(caplog) == []

    def test_reloading_between_timeouts_keeps_the_count_and_the_episode(self, rig, caplog):
        zfs_timeout(rig)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            rig.cycles(2)
            rig.reloading()
            rig.cycles(5)
            assert rig.skips() == 2 and token_errors(caplog) == []
            zfs_timeout(rig)
            rig.cycle()
        assert len(token_errors(caplog)) == 1
        assert [r.levelname for r in timeout_warnings(caplog)] == ['WARNING', 'DEBUG', 'DEBUG']

    def test_a_reason_other_than_zfs_timeout_on_a_retry_503_is_still_reloading(self, rig, caplog):
        rig.rm.get(rig.url(), status_code=503, json={**TIMEOUT, 'reason': 'unsettled_ancestor'})
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert rig.cycle() == 'skip'
        assert rig.skips() == 0 and timeout_warnings(caplog) == []

    def test_the_reason_without_retry_is_the_generic_http_error(self, rig, caplog):
        rig.rm.get(rig.url(), status_code=503, json={'error': 'x', 'reason': 'zfs_timeout'})
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert rig.cycle() == 'skip'
        assert rig.skips() == 1 and timeout_warnings(caplog) == []
        assert [r for r in caplog.records if 'HTTP 503' in r.getMessage()]


# ---------------------------------------------------------------------------
# negotiate
# ---------------------------------------------------------------------------


@pytest.fixture
def negotiating(h, mocker):
    mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
    return h


def negotiate_warnings(caplog):
    return [r for r in caplog.records
            if r.name == 'zfsbackup.remote' and 'could not answer the negotiation' in r.getMessage()]


def negotiate_errors(caplog):
    return [r for r in caplog.records
            if r.levelno == logging.ERROR and "zfs has not answered the negotiation" in r.getMessage()]


class TestNegotiateZfsTimeout:
    def negotiate(self, h, dsi=None, destination='offsite'):
        return h.rbm._negotiate(h.transport, 'client1', dsi or h.dsi, h.snaps, destination)

    def test_the_cycle_is_skipped_and_nothing_is_sent(self, negotiating, caplog):
        h = negotiating
        h.server_says(status_code=503, json=TIMEOUT)
        with caplog.at_level(logging.DEBUG):
            assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is False
        assert 'Negotiation failed' not in caplog.text       # not the generic HTTP failure
        h.nothing_started()
        assert h.token_gets() == []                            # no resume-token question either

    def test_it_is_one_warning_and_not_the_busy_info_line(self, negotiating, caplog):
        h = negotiating
        h.server_says(status_code=503, json=TIMEOUT)
        with caplog.at_level(logging.INFO):
            h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0])
        (warning,) = negotiate_warnings(caplog)
        assert warning.levelname == 'WARNING' and 'pool/data' in warning.getMessage()
        assert 'its zfs did not answer' in warning.getMessage() and 'skipping this cycle' in warning.getMessage()
        assert not [r for r in caplog.records if 'is busy' in r.getMessage() or 'is reloading' in r.getMessage()]
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_it_is_a_retryable_failure_flagged_as_a_zfs_timeout(self, h):
        from zfsbackup.remote import NegotiationFailed
        h.server_says(status_code=503, json=TIMEOUT)
        with pytest.raises(NegotiationFailed) as failed:
            self.negotiate(h)
        assert failed.value.reloading is True and failed.value.zfs_timeout is True

    def test_a_busy_or_reloading_negotiate_is_not_flagged(self, h):
        from zfsbackup.remote import NegotiationFailed
        for body in (BUSY, {'error': 'server is reloading; retry later', 'retry': True}):
            h.server_says(status_code=503, json=body)
            with pytest.raises(NegotiationFailed) as failed:
                self.negotiate(h)
            assert failed.value.reloading is True and failed.value.zfs_timeout is False
        assert h.rbm._negotiate_zfs_skips == {}

    def test_no_state_is_written_on_the_client(self, negotiating):
        h = negotiating
        h.server_says(status_code=503, json=TIMEOUT)
        h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0])
        h.set_anchor.assert_not_called()
        h.clear_anchor.assert_not_called()
        h.set_inflight.assert_not_called()

    def test_last_problem_says_what_happened(self, negotiating):
        h = negotiating
        h.server_says(status_code=503, json=TIMEOUT)
        h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0])
        note = h.rbm.last_problem(h.dsi.name, 'offsite')
        assert note == "negotiate skipped: https://backup.example.com's zfs did not answer in time"

    def test_a_repeating_timeout_is_debug_after_the_first_warning(self, h, caplog):
        from zfsbackup.remote import NegotiationFailed
        h.server_says(status_code=503, json=TIMEOUT)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for _ in range(2):
                with pytest.raises(NegotiationFailed):
                    self.negotiate(h)
        assert [r.levelname for r in negotiate_warnings(caplog)] == ['WARNING', 'DEBUG']

    def test_the_third_consecutive_skip_is_one_error(self, h, caplog):
        from zfsbackup.remote import NegotiationFailed
        h.server_says(status_code=503, json=TIMEOUT)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for _ in range(RESUME_TOKEN_SKIP_ERROR_AFTER - 1):
                with pytest.raises(NegotiationFailed):
                    self.negotiate(h)
            assert negotiate_errors(caplog) == []
            with pytest.raises(NegotiationFailed):
                self.negotiate(h)
            (error,) = negotiate_errors(caplog)
            for _ in range(4):
                with pytest.raises(NegotiationFailed):
                    self.negotiate(h)
        assert negotiate_errors(caplog) == [error]
        assert 'pool/data -> offsite' in error.getMessage() and 'falling behind' in error.getMessage()
        assert f'last {RESUME_TOKEN_SKIP_ERROR_AFTER} cycles' in error.getMessage()

    def test_a_good_negotiate_re_arms_the_warning_and_the_error(self, h, caplog):
        from zfsbackup.remote import NegotiationFailed
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            h.server_says(status_code=503, json=TIMEOUT)
            for _ in range(RESUME_TOKEN_SKIP_ERROR_AFTER):
                with pytest.raises(NegotiationFailed):
                    self.negotiate(h)
            assert len(negotiate_errors(caplog)) == 1
            h.server_says(common=None)
            assert self.negotiate(h) is None
            assert [r for r in caplog.records if 'answers negotiations again' in r.getMessage()]
            assert h.rbm._negotiate_zfs_skips == {}
            h.server_says(status_code=503, json=TIMEOUT)
            for _ in range(RESUME_TOKEN_SKIP_ERROR_AFTER):
                with pytest.raises(NegotiationFailed):
                    self.negotiate(h)
        assert [r.levelname for r in negotiate_warnings(caplog)] == [
            'WARNING', 'DEBUG', 'DEBUG', 'WARNING', 'DEBUG', 'DEBUG']
        assert len(negotiate_errors(caplog)) == 2

    def test_busy_or_reloading_in_between_neither_counts_nor_resets(self, h, caplog):
        from zfsbackup.remote import NegotiationFailed
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            h.server_says(status_code=503, json=TIMEOUT)
            for _ in range(2):
                with pytest.raises(NegotiationFailed):
                    self.negotiate(h)
            h.server_says(status_code=503, json=BUSY)
            for _ in range(4):
                with pytest.raises(NegotiationFailed):
                    self.negotiate(h)
            assert h.rbm._negotiate_zfs_skips == {(h.dsi.name, 'offsite'): 2}
            assert negotiate_errors(caplog) == []
            h.server_says(status_code=503, json=TIMEOUT)
            with pytest.raises(NegotiationFailed):
                self.negotiate(h)
        assert len(negotiate_errors(caplog)) == 1

    def test_the_count_is_per_dataset_and_destination(self, h, caplog):
        from zfsbackup.remote import NegotiationFailed
        h.server_says(status_code=503, json=TIMEOUT)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for destination in ('offsite', 'offsite', 'second'):
                with pytest.raises(NegotiationFailed):
                    self.negotiate(h, destination=destination)
        assert h.rbm._negotiate_zfs_skips == {(h.dsi.name, 'offsite'): 2, (h.dsi.name, 'second'): 1}
        assert [r.levelname for r in negotiate_warnings(caplog)] == ['WARNING', 'DEBUG', 'WARNING']

    def test_the_resume_token_count_is_separate(self, h):
        from zfsbackup.remote import NegotiationFailed
        h.server_says(status_code=503, json=TIMEOUT)
        with pytest.raises(NegotiationFailed):
            self.negotiate(h)
        assert h.rbm._token_skips == {}


class TestConstants:
    def test_the_server_budget_ends_before_either_client_timeout(self):
        from zfsbackup.remote import HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT
        worst = api.ZFS_REQUEST_BUDGET + api._ZFS_KILL_OVERHEAD + api._ZFS_HELPER_MARGIN
        assert worst < HTTP_TIMEOUT and worst < RESUME_TOKEN_TIMEOUT


# ---------------------------------------------------------------------------
# /stream
# ---------------------------------------------------------------------------


class TestStreamZfsTimeout:
    @pytest.fixture
    def refused(self, h5, mocker):
        """A chain whose first stream is answered with the server's timeout refusal, in the final ack."""
        chain = Chain(h5, mocker, sessions=5)
        chain.sessions[0].recv.return_value = json.dumps(TIMEOUT)
        return chain

    def test_the_chain_stops_quietly(self, h5, refused, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert refused.run(common=S1) is False
        assert not at_least_warning(caplog)
        infos = [r for r in caplog.records if r.levelname == 'INFO' and 'asks to retry' in r.getMessage()]
        assert len(infos) == 1
        assert 'zfs timed out; retry later' in infos[0].getMessage() and 'skipping this cycle' in infos[0].getMessage()

    def test_no_further_step_and_no_fresh_send(self, h5, refused):
        refused.run(common=S1)
        assert refused.sent() == [(S1, S2)]                    # step 1 only, and incremental
        assert refused.ws_cls.call_count == 1
        assert refused.relist.call_count == 0                  # not an ordinary failure: no search for a vanished member

    def test_no_anchor_is_written_and_the_pin_stays(self, h5, refused):
        refused.run(common=S1)
        h5.set_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()
        assert [c.args[1:] for c in h5.set_inflight.call_args_list] == [('offsite', S1, S2)]

    def test_a_full_first_send_is_not_retried_as_anything_else(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=3)
        chain.sessions[0].recv.return_value = json.dumps(TIMEOUT)
        assert chain.run(common=None) is False
        assert chain.sent() == [(None, S5)]
        assert chain.ws_cls.call_count == 1
        h5.set_anchor.assert_not_called()

    def test_a_refusal_seen_as_a_failed_send_is_the_same_quiet_stop(self, h5, refused, caplog):
        """The server answers before the header and closes: the client's first send hits the closed socket
        and finds the refusal left in it."""
        refused.sessions[0].send_binary.side_effect = BrokenPipeError('broken pipe')
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert refused.run(common=S1) is False
        assert not at_least_warning(caplog)
        assert refused.sent() == [(S1, S2)] and refused.ws_cls.call_count == 1
        h5.set_anchor.assert_not_called()
        assert [r for r in caplog.records if 'asks to retry' in r.getMessage()]

    def test_a_refusal_on_a_later_step_keeps_the_earlier_steps_anchor(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=5)
        chain.sessions[1].recv.return_value = json.dumps(TIMEOUT)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert chain.run(common=S1) is False
        assert chain.sent() == [(S1, S2), (S2, S3)]
        assert [c.args[1:] for c in h5.set_anchor.call_args_list] == [('offsite', S2)]      # step 1's, never moved back
        assert not at_least_warning(caplog)
        assert chain.ws_cls.call_count == 2
        h5.clear_inflight.assert_not_called()


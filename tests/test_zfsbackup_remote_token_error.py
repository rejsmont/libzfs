"""S-7a: the resume-token check's ERROR, `last_problem`, and the seeded skip counts.

`RemoteBackupManager._fetch_resume_token` / `_resume_target` skip the cycle when they cannot
tell whether the server holds a partial receive (sending fresh would make it discard its
progress). For ever, if the cause stays: after `RESUME_TOKEN_SKIP_ERROR_AFTER` (3)
consecutive skipped cycles of one (dataset, destination) there is one ERROR, none more until
the check passes again (no token, a usable token, a stale one, or "already on the server"),
and an INFO when it does. Reloading and authentication refusals are other conditions with
their own logging and neither count nor trigger the ERROR.

The pinned transport is faked (`make_transport` + `requests_mock`), `zfs` is mocked at
`zfsbackup.remote.zfs`; no socket, no real `zfs`.
"""

import logging
import re

import pytest
import requests

from _remote_support import HTTPS_BASE, SERVER_FP, make_transport, snapshot_presence  # noqa: F401
from _snapshot_support import listed_info
from libzfseasy.zfs import ResumeToken
from zfsbackup import remote
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig
from zfsbackup.remote import (
    RESUME_TOKEN_SKIP_ERROR_AFTER,
    TOKEN_SKIP_NOTE,
    RemoteBackupManager,
)

OLD = 'autosnap_20240101120000'
NEW = 'autosnap_20240103120000'
DS = 'pool/data'
DEST = 'offsite'
URL = f'{HTTPS_BASE}/backup/client1/{DS}/resume_token'
RELOADING = {'error': 'reloading', 'retry': True}


def snaps():
    return [listed_info(NEW, 'autosnap', guid=333), listed_info(OLD, 'autosnap', guid=111)]


class Rig:
    """A manager, a dataset and a fake transport; `cycle()` is one `_try_resume`."""

    def __init__(self, requests_mock, mocker, dry_run=False, datasets=(DS,), destinations=(DEST,)):
        self.rm = requests_mock
        self.mocker = mocker
        config = BackupConfig(
            datasets=[
                DatasetConfig(name=name, remote=[RemoteDatasetConfig(destination=d) for d in destinations])
                for name in datasets
            ],
            destinations={d: Destination(url=HTTPS_BASE, server_fingerprint=SERVER_FP) for d in destinations},
            dry_run=dry_run,
        )
        self.manager = DatasetManager(config)
        self.rbm = RemoteBackupManager(config, self.manager, client_id='client1')
        self.dsi = {d.name: d for d in self.manager.datasets}
        self.transport = make_transport()
        self.resume_info = mocker.patch(
            'zfsbackup.remote.zfs.send.resume_info',
            return_value=ResumeToken(toname=f'{DS}@{OLD}', toguid=111, fromguid=None, flags=frozenset()),
        )
        self.send_resume = mocker.patch('zfsbackup.remote.zfs.send.resume')
        self.set_inflight = mocker.patch.object(self.manager, 'set_inflight', return_value=True)
        mocker.patch.object(self.manager, 'get_anchor', return_value=None)
        mocker.patch.object(self.manager, 'get_inflight', return_value=None)
        mocker.patch.object(self.manager, 'set_anchor', return_value=True)
        self.rbm._server_info[(DS, DEST)] = ('plain', 'pool/b/client1/data')

    # -- what the server says to GET .../resume_token ------------------------
    def url(self, dataset=DS):
        return f'{HTTPS_BASE}/backup/client1/{dataset}/resume_token'

    def http_500(self, dataset=DS):
        self.rm.get(self.url(dataset), status_code=500)

    def unreachable(self, dataset=DS):
        self.rm.get(self.url(dataset), exc=requests.ConnectionError('connection refused'))

    def reloading(self, dataset=DS):
        self.rm.get(self.url(dataset), status_code=503, json=RELOADING)

    def unauthorized(self, dataset=DS):
        self.rm.get(self.url(dataset), status_code=401, json={'error': 'no certificate'})

    def forbidden(self, dataset=DS):
        self.rm.get(self.url(dataset), status_code=403, json={'error': 'not trusted'})

    def no_token(self, dataset=DS):
        self.rm.get(self.url(dataset), json={'resume_token': None})

    def token(self, dataset=DS, token='1-sometoken'):
        self.rm.get(self.url(dataset), json={'resume_token': token})

    def undecodable(self):
        self.token()
        self.resume_info.side_effect = RuntimeError('zfs binary missing')

    def decodable(self):
        self.resume_info.side_effect = None

    # -- act ----------------------------------------------------------------
    def cycle(self, dataset=DS, destination=DEST):
        return self.rbm._try_resume(
            self.transport, 'client1', self.dsi[dataset], destination, snaps(), 'plain', None
        )[0]

    def cycles(self, count, **kw):
        return [self.cycle(**kw) for _ in range(count)]

    def settle(self, dataset=DS, destination=DEST):
        """`_settle_up_to_date`: the server already has the newest snapshot."""
        self.rbm._settle_up_to_date(self.dsi[dataset], snaps()[0], destination, snaps())

    def skips(self, dataset=DS, destination=DEST):
        return self.rbm._token_skips.get((dataset, destination), 0)


@pytest.fixture(autouse=True)
def _presence(snapshot_presence):  # noqa: F811
    return snapshot_presence


@pytest.fixture(autouse=True)
def _no_sleep(mocker):
    mocker.patch('zfsbackup.remote.time.sleep')


@pytest.fixture
def rig(requests_mock, mocker):
    return Rig(requests_mock, mocker)


def token_errors(caplog):
    return [r for r in caplog.records
            if r.levelno == logging.ERROR and 'resume-token check has skipped' in r.getMessage()]


def passes_again(caplog):
    return [r for r in caplog.records
            if r.levelno == logging.INFO and 'resume-token check passes again' in r.getMessage()]


@pytest.fixture(autouse=True)
def _info(caplog):
    caplog.set_level(logging.INFO, logger='zfsbackup.remote')


def test_the_threshold_is_three():
    assert RESUME_TOKEN_SKIP_ERROR_AFTER == 3


@pytest.mark.unit
class TestErrorOnTheThirdSkip:
    @pytest.mark.parametrize('cause', ['http_500', 'unreachable', 'undecodable'])
    def test_the_third_consecutive_skip_is_one_error_and_no_more_follow(self, rig, caplog, cause):
        getattr(rig, cause)()
        assert rig.cycle() == 'skip'
        assert rig.cycle() == 'skip'
        assert token_errors(caplog) == []                        # two skips: WARNINGs only
        assert [r.levelname for r in caplog.records if 'skipping this cycle' in r.getMessage()] == ['WARNING'] * 2
        assert rig.cycle() == 'skip'
        (err,) = token_errors(caplog)
        assert rig.cycles(5) == ['skip'] * 5
        assert token_errors(caplog) == [err]                     # the 4th ... 8th are silent

    def test_the_error_names_the_pair_the_count_the_cause_and_the_way_out(self, rig, caplog):
        rig.http_500()
        rig.cycles(3)
        (err,) = token_errors(caplog)
        text = err.getMessage()
        assert err.name == 'zfsbackup.remote'
        assert f'{DS} -> {DEST}' in text
        assert 'last 3 cycles in a row' in text
        assert 'HTTP 500' in text
        assert 'falling behind' in text
        assert 'zfs receive -A pool/b/client1/data' in text

    def test_the_way_out_names_a_placeholder_when_the_server_dataset_is_unknown(self, rig, caplog):
        rig.rbm._server_info.clear()
        rig.http_500()
        rig.cycles(3)
        assert '<server dataset>' in token_errors(caplog)[0].getMessage()

    def test_an_exception_in_resume_target_counts(self, rig, caplog):
        rig.undecodable()
        rig.cycles(3)
        (err,) = token_errors(caplog)
        assert 'cannot check the token' in err.getMessage() and 'zfs binary missing' in err.getMessage()

    def test_the_causes_add_up_across_kinds(self, rig, caplog):
        rig.http_500()
        rig.cycle()
        rig.unreachable()
        rig.cycle()
        assert token_errors(caplog) == []
        rig.undecodable()
        rig.cycle()
        assert len(token_errors(caplog)) == 1
        assert rig.skips() == 3

    def test_a_skip_is_counted_per_dataset_and_destination(self, requests_mock, mocker, caplog):
        r = Rig(requests_mock, mocker, datasets=(DS, 'pool/other'), destinations=(DEST, 'second'))
        for ds in (DS, 'pool/other'):
            r.http_500(ds)
        r.cycles(2, dataset=DS, destination=DEST)
        r.cycles(2, dataset='pool/other', destination=DEST)
        r.cycles(2, dataset=DS, destination='second')
        assert token_errors(caplog) == []
        r.cycle(dataset=DS, destination=DEST)
        assert [m.getMessage().split(':')[0] for m in token_errors(caplog)] == [f'{DS} -> {DEST}']
        r.cycle(dataset='pool/other', destination=DEST)
        r.cycle(dataset=DS, destination='second')
        assert [m.getMessage().split(':')[0] for m in token_errors(caplog)] == [
            f'{DS} -> {DEST}', f'pool/other -> {DEST}', f'{DS} -> second']

    def test_the_skip_note_is_what_last_problem_reports(self, rig):
        rig.http_500()
        rig.cycle()
        note = rig.rbm.last_problem(DS, DEST)
        assert note.startswith(TOKEN_SKIP_NOTE) and 'HTTP 500' in note
        assert TOKEN_SKIP_NOTE == 'resume token check failed'


@pytest.mark.unit
class TestWhatDoesNotCount:
    def test_a_reloading_server_neither_counts_nor_logs_the_error(self, rig, caplog):
        rig.reloading()
        assert rig.cycles(10) == ['skip'] * 10
        assert rig.skips() == 0 and token_errors(caplog) == []
        assert rig.rbm.last_problem(DS, DEST) is None

    def test_reloading_in_between_does_not_reset_the_count(self, rig, caplog):
        rig.http_500()
        rig.cycles(2)
        rig.reloading()
        rig.cycles(5)
        assert token_errors(caplog) == [] and rig.skips() == 2
        rig.http_500()
        rig.cycle()
        assert len(token_errors(caplog)) == 1

    @pytest.mark.parametrize('refusal', ['unauthorized', 'forbidden'])
    def test_an_authentication_refusal_is_its_own_error_not_this_one(self, rig, caplog, refusal):
        getattr(rig, refusal)()
        assert rig.cycles(6) == ['skip'] * 6
        assert rig.skips() == 0 and token_errors(caplog) == []
        # the destination-level refusal is logged once, as before
        assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1

    def test_an_auth_refusal_is_what_last_problem_says_when_nothing_more_specific_is_known(self, rig):
        rig.unauthorized()
        rig.cycle()
        assert 'did not receive this client' in rig.rbm.last_problem(DS, DEST)

    def test_the_skip_note_beats_a_standing_destination_refusal(self, rig):
        rig.rbm._refusal_logged[(None, DEST)] = 'the destination refused us'
        rig.http_500()
        rig.cycle()
        assert rig.rbm.last_problem(DS, DEST).startswith(TOKEN_SKIP_NOTE)

    def test_a_server_that_answers_ok_with_an_error_body_counts_as_unreadable(self, rig, caplog):
        rig.rm.get(rig.url(), json={'error': 'zfs get failed: pool busy'})
        rig.cycles(3)
        assert len(token_errors(caplog)) == 1


@pytest.mark.unit
class TestRecovery:
    def test_no_token_resets_the_count(self, rig, caplog):
        rig.http_500()
        rig.cycles(2)
        rig.no_token()
        assert rig.cycle() == 'fresh'
        assert rig.skips() == 0
        rig.http_500()
        rig.cycles(2)
        assert token_errors(caplog) == []
        rig.cycle()
        assert len(token_errors(caplog)) == 1

    def test_a_stale_token_resets_the_count(self, rig, caplog):
        rig.http_500()
        rig.cycles(2)
        rig.token()
        rig.resume_info.return_value = ResumeToken(toname='x@y', toguid=999, fromguid=None, flags=frozenset())
        assert rig.cycle() == 'fresh'                            # stale: its target is not ours
        assert rig.skips() == 0

    def test_a_usable_token_resets_the_count(self, rig, caplog):
        rig.http_500()
        rig.cycles(2)
        rig.token()
        assert rig.cycle() == 'resume'
        assert rig.skips() == 0

    def test_up_to_date_resets_the_count(self, requests_mock, mocker, caplog):
        r = Rig(requests_mock, mocker, dry_run=True)             # settle returns right after the reset
        r.http_500()
        r.cycles(2)
        assert r.skips() == 2
        r.settle()
        assert r.skips() == 0
        r.cycles(2)
        assert token_errors(caplog) == []

    def test_up_to_date_after_the_error_re_arms_it(self, requests_mock, mocker, caplog):
        r = Rig(requests_mock, mocker, dry_run=True)
        r.http_500()
        r.cycles(3)
        assert len(token_errors(caplog)) == 1
        r.settle()
        assert len(passes_again(caplog)) == 1
        r.cycles(3)
        assert len(token_errors(caplog)) == 2

    def test_success_then_three_more_skips_is_a_second_error(self, rig, caplog):
        rig.http_500()
        rig.cycles(3)
        assert len(token_errors(caplog)) == 1
        rig.no_token()
        assert rig.cycle() == 'fresh'
        (info,) = passes_again(caplog)
        assert f'{DS} -> {DEST}' in info.getMessage()
        rig.http_500()
        rig.cycles(2)
        assert len(token_errors(caplog)) == 1
        rig.cycle()
        assert len(token_errors(caplog)) == 2

    def test_no_info_when_there_was_no_error_to_recover_from(self, rig, caplog):
        rig.http_500()
        rig.cycles(2)
        rig.no_token()
        rig.cycle()
        assert passes_again(caplog) == []

    def test_a_fully_successful_cycle_leaves_no_note(self, rig):
        rig.http_500()
        rig.cycle()
        assert rig.rbm.last_problem(DS, DEST) is not None
        rig.rbm._call_note.clear()                                # what backup_dataset does at its start
        rig.no_token()
        rig.cycle()
        assert rig.rbm.last_problem(DS, DEST) is None


@pytest.mark.unit
class TestLastProblem:
    def test_unknown_when_nothing_is_known(self, rig):
        assert rig.rbm.last_problem(DS, DEST) is None

    def test_an_unknown_destination_is_reported(self, rig):
        remote_cfg = RemoteDatasetConfig(destination='nowhere')
        assert rig.rbm.backup_dataset(rig.dsi[DS], remote_cfg) is False
        assert rig.rbm.last_problem(DS, 'nowhere') == "unknown destination 'nowhere'"

    def test_backup_dataset_forgets_the_previous_calls_note(self, rig):
        """A note belongs to the call that made it: the next `backup_dataset` for the pair starts
        clean (here it fails before the token check, with no account of its own)."""
        rig.rbm._call_note[(DS, DEST)] = 'stale note from the previous cycle'
        assert rig.rbm.backup_dataset(rig.dsi[DS], RemoteDatasetConfig(destination=DEST)) is False
        assert rig.rbm.last_problem(DS, DEST) != 'stale note from the previous cycle'

    def test_an_unknown_destination_replaces_an_old_note(self, rig):
        rig.rbm._call_note[(DS, 'nowhere')] = 'stale'
        rig.rbm.backup_dataset(rig.dsi[DS], RemoteDatasetConfig(destination='nowhere'))
        assert rig.rbm.last_problem(DS, 'nowhere') == "unknown destination 'nowhere'"

    def test_the_priority_is_the_note_then_the_pair_refusal_then_the_destination_refusal(self, rig):
        rbm = rig.rbm
        rbm._refusal_logged[(None, DEST)] = 'destination refusal'
        assert rbm.last_problem(DS, DEST) == 'destination refusal'
        rbm._refusal_logged[(f'{DS} [server refusal]', DEST)] = 'server refused this dataset'
        # sorted by label: '' (None) sorts before the dataset's, so the destination-wide one wins
        # over a pair refusal, exactly as the code orders them
        assert rbm.last_problem(DS, DEST) == 'destination refusal'
        del rbm._refusal_logged[(None, DEST)]
        assert rbm.last_problem(DS, DEST) == 'server refused this dataset'
        rbm._call_note[(DS, DEST)] = 'a note'
        assert rbm.last_problem(DS, DEST) == 'a note'

    def test_refusals_of_other_datasets_and_destinations_are_not_reported(self, rig):
        rbm = rig.rbm
        rbm._refusal_logged[('pool/data2', DEST)] = 'other dataset'
        rbm._refusal_logged[(DS, 'elsewhere')] = 'other destination'
        rbm._refusal_logged[(f'{DS}x [unsettled]', DEST)] = 'a dataset whose name merely starts the same'
        assert rbm.last_problem(DS, DEST) is None

    @pytest.mark.parametrize('label', [DS, f'{DS} [unsettled]', f'{DS} [server refusal]'])
    def test_a_refusal_for_the_dataset_with_or_without_a_tag_is_reported(self, rig, label):
        rig.rbm._refusal_logged[(label, DEST)] = 'the refusal'
        assert rig.rbm.last_problem(DS, DEST) == 'the refusal'


@pytest.mark.unit
class TestSeededSkipCounts:
    """A reload respawns the worker; the skip count must not restart at 0 or a host that
    reloads more often than every third cycle would never see the ERROR."""

    def test_a_seeded_count_brings_the_error_forward(self, rig, caplog):
        rig.rbm.seed_token_skips({(DS, DEST): 2})
        rig.http_500()
        rig.cycle()
        (err,) = token_errors(caplog)
        assert 'last 3 cycles' in err.getMessage()

    def test_a_seed_below_the_threshold_logs_nothing_by_itself(self, rig, caplog):
        rig.rbm.seed_token_skips({(DS, DEST): 2})
        assert token_errors(caplog) == []
        assert rig.skips() == 2

    def test_a_seed_at_the_threshold_logs_at_the_next_skip_once(self, rig, caplog):
        rig.rbm.seed_token_skips({(DS, DEST): 7})
        assert token_errors(caplog) == []
        rig.http_500()
        rig.cycles(3)
        (err,) = token_errors(caplog)
        assert 'last 8 cycles' in err.getMessage()

    def test_zero_and_negative_counts_are_ignored(self, rig):
        rig.rbm.seed_token_skips({(DS, DEST): 0, ('pool/x', DEST): -4})
        assert rig.rbm._token_skips == {}

    def test_a_seed_never_lowers_a_count(self, rig):
        rig.http_500()
        rig.cycles(2)
        rig.rbm.seed_token_skips({(DS, DEST): 1})
        assert rig.skips() == 2
        rig.rbm.seed_token_skips({(DS, DEST): 5})
        assert rig.skips() == 5

    def test_a_success_clears_a_seeded_count(self, rig, caplog):
        rig.rbm.seed_token_skips({(DS, DEST): 2})
        rig.no_token()
        rig.cycle()
        assert rig.skips() == 0
        rig.http_500()
        rig.cycles(2)
        assert token_errors(caplog) == []

"""`zfsbackup.chain.ChainSender`: the transport-independent send chain, pinned without HTTP.

`RemoteBackupManager(ChainSender)` supplies the transport through four hooks (`_ready_transport`,
`_negotiate`, `_fetch_resume_token`, `_stream_step`). Here a minimal in-test subclass
(`FakeSender`, scripted answers, no `requests`/`websocket`/`identity`) drives `backup_dataset` end to
end, `_record_step` is called on its own, the abstract hooks are checked, and a fresh interpreter
shows that `chain.py` imports none of the transport libraries. The HTTPS behaviour of the real
subclass is `test_zfsbackup_remote.py`.

Only the edges are faked, as in `test_zfsbackup_remote.py`: `zfs send` / `resume_info` (patched on the
shared libzfseasy module), the dataset's anchor / in-flight pin properties (a `Ledger` over the
`DatasetManager` instance), the post-step `zfs get guid` (`snapshot_presence`). Nothing reaches `zfs`.
"""

import logging
import os
import subprocess
import sys
import types
from unittest.mock import MagicMock

import pytest

from _hermetic import hermetic_env
from _remote_support import HTTPS_BASE, SERVER_FP, snapshot_presence  # noqa: F401  (fixture)
from _snapshot_support import listed_info
from libzfseasy.zfs import ResumeToken
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.chain import (
    ChainSender, NegotiationFailed, _ANCHOR_FAILED, _FAILED, _OK, _STOP, _SendStream,
)
from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig

S1, S2, S3, S4, S5 = (f'autosnap_2024010{n}120000' for n in range(1, 6))
DEST = 'offsite'
DS = 'pool/data'


def sinfo(name, **props):
    """A managed snapshot as the manager's one listing delivers it (createtxg follows the name)."""
    return listed_info(name, 'autosnap', **props)


def listing(*names):
    """Managed snapshots, newest first by createtxg, as `DatasetManager.list_snapshots` returns them."""
    return sorted((sinfo(n) for n in names), key=lambda s: s.createtxg, reverse=True)


def make_config(**kwargs) -> BackupConfig:
    defaults = dict(
        datasets=[DatasetConfig(name=DS, remote=[RemoteDatasetConfig(destination=DEST)])],
        destinations={DEST: Destination(url=HTTPS_BASE, server_fingerprint=SERVER_FP)},
    )
    defaults.update(kwargs)
    return BackupConfig(**defaults)


class Ledger:
    """The dataset's anchor and in-flight pin properties, kept in memory over a real
    `DatasetManager` instance, plus `events`, the order of everything the chain does:
    ('pin', from, to), ('anchor', name), ('clear-anchor',), 'clear' (pin cleared)."""

    def __init__(self, mocker, manager, listings):
        self.anchor = None
        self.pin = None
        self.events = []
        self.anchor_writes_succeed = True
        self.listings = [list(x) for x in listings]
        self.list_calls = 0
        # The sender's reads: the raising local anchor (`_record_step`, `_settle_up_to_date`, the
        # rollback permit) and the own pin. `get_anchor` (the pruner's effective read) and
        # `get_own_anchor` (the due check's swallowing read) are tripwires: a sender reading them
        # would be crediting a parent's anchor, or swallowing an unreadable one.
        wrong = AssertionError('the sender read an anchor through the wrong reader')
        mocker.patch.object(manager, 'get_anchor', side_effect=wrong)
        mocker.patch.object(manager, 'get_own_anchor', side_effect=wrong)
        mocker.patch.object(manager, 'get_local_anchor', side_effect=lambda dsi, dest: self.anchor)
        mocker.patch.object(manager, 'get_own_inflight', side_effect=lambda dsi, dest: self.pin)
        self.set_anchor = mocker.patch.object(manager, 'set_anchor', side_effect=self._set_anchor)
        self.clear_anchor = mocker.patch.object(manager, 'clear_anchor', side_effect=self._clear_anchor)
        self.set_inflight = mocker.patch.object(manager, 'set_inflight', side_effect=self._set_pin)
        self.clear_inflight = mocker.patch.object(manager, 'clear_inflight', side_effect=self._clear_pin)
        mocker.patch.object(manager, 'list_snapshots', side_effect=self._list)

    def _list(self, dsi):
        index = min(self.list_calls, len(self.listings) - 1)
        self.list_calls += 1
        return list(self.listings[index])

    def _set_anchor(self, dsi, dest, name):
        self.events.append(('anchor', name))
        if self.anchor_writes_succeed:
            self.anchor = name
        return self.anchor_writes_succeed

    def _clear_anchor(self, dsi, dest):
        self.events.append(('clear-anchor',))
        self.anchor = None

    def _set_pin(self, dsi, dest, from_name, to_name):
        self.events.append(('pin', from_name, to_name))
        self.pin = (from_name, to_name)
        return True

    def _clear_pin(self, dsi, dest):
        self.events.append('clear')
        self.pin = None
        return True

    def anchors(self):
        return [e[1] for e in self.events if isinstance(e, tuple) and e[0] == 'anchor']

    def streams(self):
        """`[(mode, since, snapshot)]` of every `_stream_step` call, in order."""
        return [(e[1], e[2], e[3]) for e in self.events if isinstance(e, tuple) and e[0] == 'stream']


def _scripted(seq, default=None):
    """The next entry of `seq` (the last one repeats; an exception entry is raised)."""
    item = seq.pop(0) if len(seq) > 1 else (seq[0] if seq else default)
    if isinstance(item, BaseException):
        raise item
    return item


class FakeTransport:
    base_url = 'fake://destination'


class FakeSender(ChainSender):
    """A `ChainSender` whose four hooks are scripted. Everything they do is appended to
    `ledger.events` beside the chain's own property writes, so a test reads one ordered list.

    `common`: what `_negotiate` answers (a name or None; a list is one answer per call, the last one
    repeats; an exception entry is raised). `tokens`: `_fetch_resume_token` answers, same rules.
    `outcomes`: what `_stream_step` returns per call (default `_OK`; the last repeats)."""

    def __init__(self, config, manager, ledger, *, common=None, tokens=(('none', None),),
                 outcomes=(_OK,), ready=True):
        super().__init__(config, manager, client_id='client1')
        self.ledger = ledger
        self.common = list(common) if isinstance(common, list) else [common]
        self.tokens = list(tokens)
        self.outcomes = list(outcomes)
        self.ready = ready
        self.transport = FakeTransport()
        self.negotiate_calls = []
        self.stream_calls = []
        self.closed_streams = []

    def _before_destination_check(self):
        self.ledger.events.append('before-check')

    def _ready_transport(self, destination, dest):
        self.ledger.events.append(('ready', destination))
        return self.transport if self.ready else None

    def _negotiate(self, transport, client_id, dsi, snapshots, destination):
        self.negotiate_calls.append((transport, client_id, dsi, list(snapshots), destination))
        self.ledger.events.append('negotiate')
        return _scripted(self.common)

    def _fetch_resume_token(self, transport, client_id, dsi, destination):
        self.ledger.events.append('token')
        return _scripted(self.tokens)

    def _stream_step(self, transport, client_id, dsi, destination, started, after_success=False,
                     should_stop=None, discard=()):
        self.stream_calls.append(dict(
            transport=transport, client_id=client_id, destination=destination, started=started,
            after_success=after_success, should_stop=should_stop, discard=discard,
        ))
        self.ledger.events.append((
            'stream', started.mode, started.base.name if started.base else None, started.sent.name,
        ))
        started.stream.close()
        self.closed_streams.append(started.stream)
        return _scripted(self.outcomes, _OK)


class Rig:
    """A `FakeSender` over a real `DatasetManager` for `pool/data`, `zfs send` faked."""

    def __init__(self, mocker, *listings, **sender_kw):
        self.config = make_config()
        self.manager = DatasetManager(self.config)
        self.dsi = self.manager.datasets[0]
        self.ledger = Ledger(mocker, self.manager, listings)
        self.sender = FakeSender(self.config, self.manager, self.ledger, **sender_kw)
        self.events = self.ledger.events
        self.remote = types.SimpleNamespace(destination=DEST, raw=None)
        self.send_snapshot = mocker.patch(
            'zfsbackup.chain.zfs.send.snapshot', side_effect=lambda *a, **k: MagicMock(name='send'))
        self.send_resume = mocker.patch(
            'zfsbackup.chain.zfs.send.resume', side_effect=lambda *a, **k: MagicMock(name='send-t'))
        self.resume_info = mocker.patch('zfsbackup.chain.zfs.send.resume_info')

    def run(self, should_stop=None):
        return self.sender.backup_dataset(self.dsi, self.remote, should_stop)

    def sent(self):
        """`[(since, snapshot)]` short names of every `zfs send` spawned, in order."""
        return [
            (c.kwargs['since'].short if c.kwargs.get('since') else None, c.args[0].short)
            for c in self.send_snapshot.call_args_list
        ]

    def without_marks(self):
        """`events` without the entry/readiness bookkeeping."""
        skip = ('before-check', 'negotiate', 'token')
        return [e for e in self.events if e not in skip and not (isinstance(e, tuple) and e[0] == 'ready')]


@pytest.fixture(autouse=True)
def _snapshots_are_present(snapshot_presence):
    """`zfs get guid` after every acknowledged step: every listed snapshot is still there."""
    return snapshot_presence


@pytest.fixture
def rig_factory(mocker):
    return lambda *listings, **kw: Rig(mocker, *listings, **kw)


def stopper(*script, default=False):
    """A `should_stop` answering `script` in order, then `default`; `polls()` counts the asks."""
    answers = iter(script)
    count = [0]

    def should_stop():
        count[0] += 1
        return next(answers, default)

    should_stop.polls = lambda: count[0]
    return should_stop


# --------------------------------------------------------------------------------------------------
# backup_dataset end to end, over fake hooks
# --------------------------------------------------------------------------------------------------


@pytest.mark.unit
class TestFirstFullSend:
    def test_only_the_newest_is_sent_in_full(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3), common=None)
        assert rig.run() is True
        assert rig.sent() == [(None, S3)]
        assert rig.ledger.streams() == [('fresh', None, S3)]
        assert rig.ledger.anchor == S3 and rig.ledger.pin is None

    def test_the_order_is_negotiate_token_pin_stream_anchor_clear(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3), common=None)
        rig.run()
        assert rig.events == [
            'before-check', ('ready', DEST), 'negotiate', 'token',
            ('pin', None, S3), ('stream', 'fresh', None, S3), ('anchor', S3), 'clear',
        ]

    def test_the_frozen_plain_flags_and_the_hook_arguments(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=None)
        rig.run()
        assert rig.send_snapshot.call_args.kwargs == {'large_blocks': True, 'compressed': True}
        (call,) = rig.sender.stream_calls
        assert call['transport'] is rig.sender.transport
        assert call['client_id'] == 'client1' and call['destination'] == DEST
        assert call['after_success'] is False and call['discard'] == ()
        assert call['started'].mode == 'fresh' and call['started'].inflight_set is True
        (negotiate_call,) = rig.sender.negotiate_calls
        assert negotiate_call[0] is rig.sender.transport
        assert [s.name for s in negotiate_call[3]] == [S2, S1]

    def test_the_started_zfs_send_is_handed_to_the_hook_and_reaped_there(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=None)
        rig.run()
        (stream,) = rig.sender.closed_streams
        stream.close.assert_called_once()

    def test_no_snapshots_is_success_and_nothing_is_negotiated(self, rig_factory):
        rig = rig_factory([], common=None)
        assert rig.run() is True
        assert rig.sender.negotiate_calls == [] and rig.sender.stream_calls == []

    def test_the_completion_line_names_the_destination_through_where(self, rig_factory, caplog):
        rig = rig_factory(listing(S1, S2), common=None)
        with caplog.at_level(logging.INFO, logger='zfsbackup.chain'):
            rig.run()
        assert f'Backup complete: {DS}@{S2} (full) -> fake://destination' in caplog.text


@pytest.mark.unit
class TestIncrementalChain:
    def test_every_newer_snapshot_is_one_incremental_step_oldest_first(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3, S4, S5), common=S1)
        assert rig.run() is True
        assert rig.sent() == [(S1, S2), (S2, S3), (S3, S4), (S4, S5)]
        assert [s[2] for s in rig.ledger.streams()] == [S2, S3, S4, S5]
        assert rig.ledger.anchors() == [S2, S3, S4, S5]

    def test_each_step_is_pinned_before_its_stream_and_anchored_after_it(self, rig_factory):
        """The pin is kept between steps (the next step's pin overwrites it), cleared once after
        the last step's anchor."""
        rig = rig_factory(listing(S1, S2, S3, S4), common=S2)
        rig.run()
        assert rig.without_marks() == [
            ('pin', S2, S3), ('stream', 'fresh', S2, S3), ('anchor', S3),
            ('pin', S3, S4), ('stream', 'fresh', S3, S4), ('anchor', S4), 'clear',
        ]

    def test_only_the_first_step_is_not_after_a_success(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3, S4), common=S1)
        rig.run()
        assert [c['after_success'] for c in rig.sender.stream_calls] == [False, True, True]

    def test_one_negotiate_and_one_token_fetch_for_the_chain(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3, S4), common=S1)
        rig.run()
        assert rig.events.count('negotiate') == 1 and rig.events.count('token') == 1

    def test_a_common_snapshot_that_is_the_newest_just_settles_the_anchor(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=S2)
        assert rig.run() is True
        assert rig.send_snapshot.call_count == 0 and rig.sender.stream_calls == []
        assert rig.ledger.anchor == S2
        assert 'token' not in rig.events

    def test_a_common_snapshot_we_do_not_have_sends_nothing(self, rig_factory, caplog):
        rig = rig_factory(listing(S2, S3), common=S1)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            assert rig.run() is False
        assert rig.send_snapshot.call_count == 0 and rig.ledger.set_inflight.call_count == 0
        assert 'not a managed local snapshot' in caplog.text

    def test_the_dry_run_plans_the_chain_and_sends_nothing(self, rig_factory, caplog):
        rig = rig_factory(listing(S1, S2, S3), common=S1)
        rig.config.dry_run = True
        with caplog.at_level(logging.INFO, logger='zfsbackup.chain'):
            assert rig.run() is True
        assert rig.send_snapshot.call_count == 0 and rig.sender.stream_calls == []
        assert f'[DRY RUN] Would send {DS}: {S1} -> {S2} -> {S3} (2 step(s)) -> fake://destination' in caplog.text


@pytest.mark.unit
class TestResumeIsStepZero:
    def resume_token(self, rig, *, to, frm):
        rig.resume_info.return_value = ResumeToken(
            toname=f'{DS}@{to.name}', toguid=to.guid, fromguid=frm.guid if frm else None,
            flags=frozenset(),
        )

    def test_resume_then_one_renegotiate_then_the_rest_of_the_chain(self, rig_factory):
        snaps = listing(S1, S2, S3, S4)
        rig = rig_factory(snaps, common=[S1, S2], tokens=[('token', 'TOKEN')])
        self.resume_token(rig, to=sinfo(S2), frm=sinfo(S1))
        assert rig.run() is True
        rig.send_resume.assert_called_once_with('TOKEN')
        assert rig.sent() == [(S2, S3), (S3, S4)]
        assert rig.without_marks() == [
            ('pin', S1, S2), ('stream', 'resume', S1, S2), ('anchor', S2),
            ('pin', S2, S3), ('stream', 'fresh', S2, S3), ('anchor', S3),
            ('pin', S3, S4), ('stream', 'fresh', S3, S4), ('anchor', S4), 'clear',
        ]
        assert rig.events.count('negotiate') == 2 and rig.events.count('token') == 1
        assert [c['after_success'] for c in rig.sender.stream_calls] == [False, True, True]

    def test_the_renegotiate_gets_the_current_listing(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3), common=[S1, S2], tokens=[('token', 'TOKEN')])
        self.resume_token(rig, to=sinfo(S2), frm=sinfo(S1))
        rig.run()
        first, second = rig.sender.negotiate_calls
        assert [s.name for s in first[3]] == [S3, S2, S1] == [s.name for s in second[3]]
        assert rig.ledger.list_calls == 2                 # once up front, once after the resume

    def test_a_resume_of_the_newest_ends_the_cycle_and_clears_the_pin(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3), common=S2, tokens=[('token', 'TOKEN')])
        self.resume_token(rig, to=sinfo(S3), frm=sinfo(S2))
        assert rig.run() is True
        assert rig.sent() == []
        assert rig.events.count('negotiate') == 1
        assert rig.without_marks()[-2:] == [('anchor', S3), 'clear']

    def test_a_stale_token_falls_through_to_a_fresh_send(self, rig_factory, caplog):
        rig = rig_factory(listing(S1, S2), common=S1, tokens=[('token', 'TOKEN')])
        rig.resume_info.return_value = ResumeToken(
            toname=f'{DS}@gone', toguid=999, fromguid=sinfo(S1).guid, flags=frozenset())
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            assert rig.run() is True
        rig.send_resume.assert_not_called()
        assert rig.ledger.streams() == [('fresh', S1, S2)]
        assert 'Not resuming' in caplog.text

    def test_a_skip_from_the_token_hook_skips_the_cycle(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=S1, tokens=[('skip', None)])
        assert rig.run() is False
        assert rig.sender.stream_calls == [] and rig.ledger.set_inflight.call_count == 0

    def test_a_failed_resume_stream_never_reaches_the_chain(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3), common=[S1, S2], tokens=[('token', 'TOKEN')],
                          outcomes=[_FAILED])
        self.resume_token(rig, to=sinfo(S2), frm=sinfo(S1))
        assert rig.run() is False
        assert len(rig.sender.stream_calls) == 1
        assert rig.ledger.anchors() == [] and 'clear' not in rig.events
        assert rig.events.count('negotiate') == 1
        rig.send_snapshot.assert_not_called()

    def test_a_stop_after_the_resume_pauses_the_chain(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3), common=[S1, S2], tokens=[('token', 'TOKEN')])
        self.resume_token(rig, to=sinfo(S2), frm=sinfo(S1))
        stop = stopper(False, False, True)       # entry, after the token is accepted, after the resume
        assert rig.run(stop) is True
        assert len(rig.sender.stream_calls) == 1 and rig.events.count('negotiate') == 1
        assert rig.without_marks()[-2:] == [('anchor', S2), 'clear']


@pytest.mark.unit
class TestStopAndFailure:
    def test_a_stop_between_steps_pauses_and_streams_nothing_further(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3, S4), common=S1)
        stop = stopper(False, False, True)       # entry, before step 1, before step 2
        assert rig.run(stop) is True
        assert rig.ledger.streams() == [('fresh', S1, S2)]
        assert rig.send_snapshot.call_count == 1
        assert rig.ledger.anchor == S2
        assert rig.without_marks() == [
            ('pin', S1, S2), ('stream', 'fresh', S1, S2), ('anchor', S2), 'clear',
        ]                                        # settled: the kept pin is released
        assert stop.polls() == 3

    def test_a_stop_before_the_first_stream_does_nothing(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3), common=S1)
        assert rig.run(stopper(False, True)) is False
        assert rig.sender.stream_calls == [] and rig.send_snapshot.call_count == 0
        assert rig.ledger.set_inflight.call_count == 0 and rig.ledger.anchors() == []

    def test_a_stop_at_entry_does_nothing_either(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=S1)
        assert rig.run(stopper(True)) is False
        assert rig.sender.stream_calls == [] and rig.ledger.set_inflight.call_count == 0

    def test_should_stop_is_handed_to_the_stream_hook(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=S1)
        stop = stopper()
        rig.run(stop)
        assert rig.sender.stream_calls[0]['should_stop'] is stop

    def test_a_stop_outcome_from_the_hook_ends_the_cycle_without_an_anchor(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3, S4), common=S1, outcomes=[_STOP])
        assert rig.run() is False
        assert len(rig.sender.stream_calls) == 1
        assert rig.ledger.anchors() == [] and 'clear' not in rig.events
        assert rig.ledger.pin == (S1, S2)                          # kept: nothing was acknowledged

    def test_a_failed_first_stream_stops_the_chain_without_anchoring(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3, S4), common=S1, outcomes=[_FAILED])
        assert rig.run() is False
        assert len(rig.sender.stream_calls) == 1
        assert rig.ledger.anchors() == [] and 'clear' not in rig.events
        assert rig.ledger.pin == (S1, S2)

    def test_a_failure_after_a_success_keeps_the_earlier_anchor_only(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3, S4), common=S1, outcomes=[_OK, _FAILED, _OK])
        assert rig.run() is False
        assert [s[2] for s in rig.ledger.streams()] == [S2, S3]    # no third stream
        assert rig.ledger.anchors() == [S2] and rig.ledger.anchor == S2
        assert 'clear' not in rig.events and rig.ledger.pin == (S2, S3)

    def test_a_member_destroyed_meanwhile_is_rebuilt_around(self, rig_factory, caplog):
        before = listing(S1, S2, S3, S4)
        after = listing(S1, S3, S4)                                # S2 vanished
        rig = rig_factory(before, after, common=S1, outcomes=[_FAILED, _OK, _OK])
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            assert rig.run() is True
        assert [(m, b, s) for m, b, s in rig.ledger.streams()] == [
            ('fresh', S1, S2), ('fresh', S1, S3), ('fresh', S3, S4)]
        assert rig.ledger.anchors() == [S3, S4]
        assert f'{DS}@{S2} no longer exists; rebuilding' in caplog.text

    def test_an_anchor_failure_stops_the_chain(self, rig_factory):
        rig = rig_factory(listing(S1, S2, S3), common=S1)
        rig.ledger.anchor_writes_succeed = False
        assert rig.run() is False                                  # not the last step
        assert len(rig.sender.stream_calls) == 1
        assert rig.ledger.pin == (S1, S2)


@pytest.mark.unit
class TestTransportOutcomes:
    def test_a_destination_the_transport_refuses_is_skipped_before_everything(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=S1, ready=False)
        assert rig.run() is False
        assert rig.events == ['before-check', ('ready', DEST)]
        assert rig.sender.negotiate_calls == []

    def test_before_check_runs_before_the_transport_is_asked(self, rig_factory):
        rig = rig_factory(listing(S1), common=None)
        rig.run()
        assert rig.events[:2] == ['before-check', ('ready', DEST)]

    def test_an_unknown_destination_never_reaches_the_hooks(self, rig_factory, caplog):
        rig = rig_factory(listing(S1), common=None)
        rig.remote = types.SimpleNamespace(destination='nowhere', raw=None)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            assert rig.run() is False
        assert rig.events == []
        assert "Unknown destination 'nowhere'" in caplog.text
        assert rig.sender.last_problem(DS, 'nowhere') == "unknown destination 'nowhere'"

    def test_a_failed_negotiate_never_becomes_a_full_send(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=[NegotiationFailed('no answer')])
        assert rig.run() is False
        assert rig.send_snapshot.call_count == 0 and rig.ledger.set_inflight.call_count == 0
        assert 'token' not in rig.events

    def test_a_reloading_negotiate_is_logged_through_the_peer_hook(self, rig_factory, caplog):
        rig = rig_factory(listing(S1, S2),
                          common=[NegotiationFailed('server is reloading; retry later', reloading=True)])
        with caplog.at_level(logging.INFO, logger='zfsbackup.chain'):
            assert rig.run() is False
        assert f'server fake://destination is reloading; skipping {DS} this cycle' in caplog.text

    def test_a_zfs_timeout_negotiate_is_not_logged_again(self, rig_factory, caplog):
        rig = rig_factory(listing(S1, S2), common=[
            NegotiationFailed('zfs timed out', reloading=True, zfs_timeout=True)])
        with caplog.at_level(logging.INFO, logger='zfsbackup.chain'):
            assert rig.run() is False
        assert 'skipping' not in caplog.text

    def test_the_mode_is_decided_by_the_chain_and_a_raw_setting_on_plain_data_refuses(
            self, rig_factory, caplog):
        rig = rig_factory(listing(S1, S2), common=S1)
        rig.remote = types.SimpleNamespace(destination=DEST, raw=True)
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            assert rig.run() is False
        assert 'raw is true but the dataset is not encrypted' in caplog.text
        assert rig.sender.negotiate_calls == [] and rig.sender.stream_calls == []


# --------------------------------------------------------------------------------------------------
# _push_step / _record_step in isolation
# --------------------------------------------------------------------------------------------------


@pytest.fixture
def seq(rig_factory):
    """A rig over S1..S4 for the step-level tests."""
    return rig_factory(listing(S1, S2, S3, S4), common=S1)


def started(rig, sent, base=None, mode='fresh', pinned=True):
    return _SendStream(MagicMock(name='stream'), mode, pinned,
                       next(s for s in rig.ledger.listings[0] if s.name == sent),
                       next((s for s in rig.ledger.listings[0] if s.name == base), None))


def record(rig, st, **kw):
    snapshots = rig.ledger.listings[0]
    return rig.sender._record_step(rig.sender.transport, rig.dsi, DEST, snapshots, st, **kw)


@pytest.mark.unit
class TestRecordStep:
    def test_the_anchor_moves_to_the_sent_snapshot_and_the_pin_is_cleared(self, seq):
        seq.ledger.anchor = S1
        assert record(seq, started(seq, S2, S1)) == _OK
        assert seq.events == [('anchor', S2), 'clear']
        assert seq.ledger.anchor == S2

    def test_a_first_full_send_anchors_with_no_prior_anchor(self, seq):
        assert record(seq, started(seq, S4)) == _OK
        assert seq.ledger.anchors() == [S4]

    def test_the_anchor_never_moves_backwards(self, seq):
        seq.ledger.anchor = S3
        assert record(seq, started(seq, S2, S1)) == _OK
        assert seq.ledger.anchors() == [] and seq.ledger.anchor == S3
        assert 'clear' not in seq.events                  # nothing written: the pin is not released
        assert seq.send_snapshot.call_count == 0

    def test_a_resumed_older_snapshot_leaves_a_newer_anchor_alone(self, seq, caplog):
        seq.ledger.anchor = S4
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.chain'):
            record(seq, started(seq, S2, S1, mode='resume'))
        assert seq.ledger.anchor == S4 and seq.ledger.anchors() == []
        assert f'Keeping anchor {S4}: newer than the resumed {S2}' in caplog.text

    def test_the_order_is_by_createtxg_not_by_name(self, rig_factory):
        names = [sinfo(S1, txg=40), sinfo(S2, txg=30), sinfo(S3, txg=20), sinfo(S4, txg=10)]
        rig = rig_factory(sorted(names, key=lambda s: s.createtxg, reverse=True), common=None)
        rig.ledger.anchor = S2                            # txg 30: newer than S3's 20
        assert record(rig, started(rig, S3, S4)) == _OK
        assert rig.ledger.anchors() == [] and rig.ledger.anchor == S2

    def test_a_destroyed_sent_snapshot_puts_the_anchor_back_on_the_base_and_keeps_the_pin(
            self, seq, snapshot_presence, caplog):
        snapshot_presence.gone.add(S2)
        seq.ledger.anchor = S1
        with caplog.at_level(logging.ERROR, logger='zfsbackup.chain'):
            assert record(seq, started(seq, S2, S1)) == _ANCHOR_FAILED
        assert seq.ledger.anchors() == [S2, S1] and seq.ledger.anchor == S1
        assert 'clear' not in seq.events
        assert f'{DS}@{S2} was destroyed while it was being sent' in caplog.text
        assert f'Putting the anchor back on {S1}' in caplog.text

    def test_a_replaced_snapshot_with_another_guid_counts_as_destroyed(self, seq, snapshot_presence):
        snapshot_presence.replaced[S2] = 12345
        assert record(seq, started(seq, S2, S1)) == _ANCHOR_FAILED
        assert seq.ledger.anchor == S1 and 'clear' not in seq.events

    def test_a_destroyed_full_send_with_no_prior_anchor_clears_the_anchor(self, seq, snapshot_presence):
        snapshot_presence.gone.add(S4)
        assert record(seq, started(seq, S4)) == _ANCHOR_FAILED
        assert seq.events == [('anchor', S4), ('clear-anchor',)]
        assert seq.ledger.anchor is None

    def test_a_destroyed_full_send_restores_the_earlier_anchor(self, seq, snapshot_presence):
        snapshot_presence.gone.add(S4)
        seq.ledger.anchor = S2
        assert record(seq, started(seq, S4)) == _ANCHOR_FAILED
        assert seq.ledger.anchor == S2 and 'clear' not in seq.events

    def test_a_snapshot_whose_presence_cannot_be_told_keeps_the_pin_and_the_anchor(
            self, seq, snapshot_presence, caplog):
        snapshot_presence.unknown.add(S2)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            assert record(seq, started(seq, S2, S1)) == _ANCHOR_FAILED
        assert seq.ledger.anchors() == [S2] and 'clear' not in seq.events
        assert f'cannot confirm that {DS}@{S2} still exists' in caplog.text

    def test_a_failed_anchor_write_keeps_the_pin_and_does_not_check_presence(self, seq, snapshot_presence):
        seq.ledger.anchor_writes_succeed = False
        assert record(seq, started(seq, S2, S1)) == _ANCHOR_FAILED
        assert 'clear' not in seq.events and snapshot_presence.calls == []

    def test_clear_pin_true_clears_it(self, seq):
        assert record(seq, started(seq, S2, S1), clear_pin=True) == _OK
        assert 'clear' in seq.events

    def test_clear_pin_false_keeps_it(self, seq):
        seq.ledger.pin = (S1, S2)
        assert record(seq, started(seq, S2, S1), clear_pin=False) == _OK
        assert 'clear' not in seq.events and seq.ledger.pin == (S1, S2)
        assert seq.ledger.anchors() == [S2]

    def test_the_sent_snapshot_is_looked_up_by_guid_once(self, seq, snapshot_presence):
        record(seq, started(seq, S2, S1))
        assert snapshot_presence.calls == [(f'{DS}@{S2}', 'guid')]

    def test_the_completion_lines(self, seq, caplog):
        with caplog.at_level(logging.INFO, logger='zfsbackup.chain'):
            record(seq, started(seq, S2, S1))
            record(seq, started(seq, S3, S2, mode='resume'))
            record(seq, started(seq, S1))
        text = caplog.text
        assert f'Backup complete: {DS}@{S2} (incremental from {S1}) -> fake://destination' in text
        assert f'Resumed {DS}@{S3} (from {S2}) to {DEST}' in text
        assert f'Backup complete: {DS}@{S1} (full) -> fake://destination' in text

    def test_a_standing_server_refusal_is_forgotten(self, seq):
        key = (f'{DS} [server refusal]', DEST)
        seq.sender._refusal_logged[key] = 'refused'
        record(seq, started(seq, S2, S1))
        assert key not in seq.sender._refusal_logged


@pytest.mark.unit
class TestUnreadableLocalAnchorFailsClosed:
    """L1: `_record_step` and `_settle_up_to_date` read the anchor with the RAISING `get_local_anchor`
    (not the swallowing `get_own_anchor`): "could not read" is not "unset". An unreadable anchor
    must neither skip the never-backwards rule nor let the backstop clear a real anchor."""

    ERROR = 'cannot open pool/data: pool I/O is currently suspended'

    @staticmethod
    def unreadable(rig, mocker):
        from zfsbackup.backup_manager import LocalAnchorUnreadable
        return mocker.patch.object(rig.manager, 'get_local_anchor',
                                   side_effect=LocalAnchorUnreadable(TestUnreadableLocalAnchorFailsClosed.ERROR))

    def test_record_step_writes_nothing_keeps_the_pin_and_stops(self, seq, mocker, snapshot_presence):
        self.unreadable(seq, mocker)
        seq.ledger.pin = (S1, S2)
        assert record(seq, started(seq, S2, S1)) == _ANCHOR_FAILED
        assert seq.events == []                              # no anchor, no clear-anchor, no clear
        assert seq.ledger.pin == (S1, S2)
        assert snapshot_presence.calls == []                 # nothing was written, so nothing to confirm

    def test_record_step_never_clears_a_real_anchor_when_there_is_no_base(self, seq, mocker):
        """The sent-snapshot backstop `clear_anchor`s when it has no base and no remembered anchor:
        an unreadable anchor reads as no anchor there, so it must not get that far."""
        self.unreadable(seq, mocker)
        seq.ledger.anchor = S3
        assert record(seq, started(seq, S2)) == _ANCHOR_FAILED
        assert seq.ledger.anchor == S3 and ('clear-anchor',) not in seq.events

    def test_record_step_warns_once_per_episode_and_rearms_on_a_good_read(self, seq, mocker, caplog):
        patched = self.unreadable(seq, mocker)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            record(seq, started(seq, S2, S1))
            record(seq, started(seq, S2, S1))
        texts = [r.getMessage() for r in caplog.records if 'cannot read the local anchor property' in r.getMessage()]
        assert len(texts) == 1 and self.ERROR in texts[0] and f'after sending {S2}' in texts[0]
        patched.side_effect = None
        patched.return_value = None
        assert record(seq, started(seq, S2, S1)) == _OK       # a good read ends the episode
        caplog.clear()
        from zfsbackup.backup_manager import LocalAnchorUnreadable
        patched.side_effect = LocalAnchorUnreadable(self.ERROR)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            record(seq, started(seq, S2, S1))
        assert len([r for r in caplog.records if 'cannot read the local anchor property' in r.getMessage()]) == 1

    def test_settle_writes_and_clears_nothing_and_reports_the_cycle_as_not_completed(
            self, rig_factory, mocker, caplog):
        rig = rig_factory(listing(S1, S2), common=S2)
        self.unreadable(rig, mocker)
        rig.ledger.pin = (S1, S2)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            assert rig.run() is False
        assert rig.ledger.anchors() == [] and rig.ledger.pin == (S1, S2)
        assert 'clear' not in rig.events and ('clear-anchor',) not in rig.events
        assert rig.send_snapshot.call_count == 0
        (rec,) = [r for r in caplog.records if 'cannot read the local anchor property' in r.getMessage()]
        assert rec.levelno == logging.WARNING and 'the next cycle retries' in rec.getMessage()

    def test_record_step_with_no_base_and_a_gone_snapshot_changes_nothing_and_says_why(
            self, seq, mocker, snapshot_presence, caplog):
        """The backstop (`base=None`, the sent snapshot destroyed meanwhile) would `clear_anchor`; with
        an unreadable anchor it never gets there: no set, no clear, the pin stays, and
        `last_problem` carries the WARNING for the worker's status record."""
        snapshot_presence.gone.add(S4)
        self.unreadable(seq, mocker)
        seq.ledger.anchor = S2
        seq.ledger.pin = (S3, S4)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.chain'):
            assert record(seq, started(seq, S4)) == _ANCHOR_FAILED
            assert record(seq, started(seq, S4)) == _ANCHOR_FAILED
        assert seq.events == [] and seq.ledger.anchor == S2 and seq.ledger.pin == (S3, S4)
        assert snapshot_presence.calls == []
        seq.ledger.set_anchor.assert_not_called()
        seq.ledger.clear_anchor.assert_not_called()
        seq.ledger.clear_inflight.assert_not_called()
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1                              # the second cycle's line is DEBUG
        problem = seq.sender.last_problem(DS, DEST)
        assert problem == warnings[0].getMessage() and self.ERROR in problem

    def test_settle_warns_once_has_its_own_latch_and_rearms_on_a_good_read(
            self, rig_factory, mocker, caplog):
        rig = rig_factory(listing(S1, S2), common=S2)
        patched = self.unreadable(rig, mocker)
        rig.ledger.anchor = 'autosnap_20990101000000'          # names a snapshot this host lacks
        rig.ledger.pin = (S1, S2)
        settle_key = (f'{DS} [local anchor to settle]', DEST)
        permit_key = (f'{DS} [local anchor]', DEST)

        def settle_warnings():
            return [r for r in caplog.records if r.levelno == logging.WARNING
                    and 'the next cycle retries' in r.getMessage()]

        with caplog.at_level(logging.DEBUG, logger='zfsbackup.chain'):
            assert rig.run() is False
            assert rig.run() is False
        assert len(settle_warnings()) == 1                     # not repeated next cycle (DEBUG at most)
        assert settle_key in rig.sender._refusal_logged and permit_key not in rig.sender._refusal_logged
        assert rig.ledger.set_anchor.call_count == 0 and rig.ledger.clear_anchor.call_count == 0
        assert rig.ledger.clear_inflight.call_count == 0 and rig.ledger.pin == (S1, S2)
        assert 'cannot read the local anchor property' in rig.sender.last_problem(DS, DEST)

        patched.side_effect = None                             # a good read ends the episode
        patched.return_value = S2
        assert rig.run() is True
        assert settle_key not in rig.sender._refusal_logged

        from zfsbackup.backup_manager import LocalAnchorUnreadable
        patched.side_effect = LocalAnchorUnreadable(self.ERROR)
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            assert rig.run() is False
        assert len(settle_warnings()) == 1                     # a new episode warns again

    def test_the_rollback_permits_latch_does_not_silence_the_settle_warning(self, rig_factory, mocker, caplog):
        rig = rig_factory(listing(S1, S2), common=S2)
        self.unreadable(rig, mocker)
        permit_key = (f'{DS} [local anchor]', DEST)
        rig.sender._refusal_logged[permit_key] = 'the permit already said something'
        with caplog.at_level(logging.WARNING, logger='zfsbackup.chain'):
            assert rig.run() is False
        assert [r for r in caplog.records if 'the next cycle retries' in r.getMessage()]
        assert rig.sender._refusal_logged[permit_key] == 'the permit already said something'

    def test_settle_in_a_dry_run_does_not_read_the_anchor(self, rig_factory, mocker):
        rig = rig_factory(listing(S1, S2), common=S2)
        patched = self.unreadable(rig, mocker)
        rig.config.dry_run = True
        assert rig.run() is True
        patched.assert_not_called()

    def test_the_sender_reads_neither_the_effective_nor_the_swallowing_anchor(self, seq):
        """Guards the doubles: `Ledger` makes both raise AssertionError, so a full chain passing means
        the sender went through the local reader only."""
        assert seq.run() is True
        assert seq.manager.get_local_anchor.call_count >= 1
        seq.manager.get_anchor.assert_not_called()
        seq.manager.get_own_anchor.assert_not_called()


@pytest.mark.unit
class TestPushStep:
    def push(self, rig, st, **kw):
        return rig.sender._push_step(
            rig.sender.transport, 'client1', rig.dsi, DEST, rig.ledger.listings[0], st, **kw)

    @pytest.mark.parametrize('outcome', [_FAILED, _STOP])
    def test_a_stream_that_did_not_complete_is_returned_as_it_is_and_records_nothing(
            self, rig_factory, outcome):
        rig = rig_factory(listing(S1, S2), common=S1, outcomes=[outcome])
        assert self.push(rig, started(rig, S2, S1)) == outcome
        assert rig.ledger.anchors() == [] and 'clear' not in rig.events

    def test_an_acknowledged_stream_is_recorded(self, rig_factory):
        rig = rig_factory(listing(S1, S2), common=S1)
        assert self.push(rig, started(rig, S2, S1)) == _OK
        assert rig.ledger.anchor == S2 and rig.events[-1] == 'clear'

    def test_clear_pin_and_the_stream_options_are_passed_on(self, rig_factory, mocker):
        rig = rig_factory(listing(S1, S2), common=S1)
        record_step = mocker.patch.object(rig.sender, '_record_step', return_value=_ANCHOR_FAILED)
        st = started(rig, S2, S1)
        stop = stopper()
        result = self.push(rig, st, clear_pin=False, after_success=True, should_stop=stop,
                           discard=('7',))
        assert result == _ANCHOR_FAILED
        (call,) = rig.sender.stream_calls
        assert call['after_success'] is True and call['should_stop'] is stop
        assert call['discard'] == ('7',) and call['started'] is st
        record_step.assert_called_once_with(
            rig.sender.transport, rig.dsi, DEST, rig.ledger.listings[0], st, clear_pin=False)


# --------------------------------------------------------------------------------------------------
# The seam itself
# --------------------------------------------------------------------------------------------------


@pytest.fixture
def bare():
    config = make_config()
    return ChainSender(config, DatasetManager(config), client_id='client1')


@pytest.mark.unit
class TestAbstractHooks:
    def test_ready_transport(self, bare):
        with pytest.raises(NotImplementedError):
            bare._ready_transport(DEST, bare.config.destinations[DEST])

    def test_negotiate(self, bare):
        with pytest.raises(NotImplementedError):
            bare._negotiate(object(), 'client1', bare.local_manager.datasets[0], [], DEST)

    def test_fetch_resume_token(self, bare):
        with pytest.raises(NotImplementedError):
            bare._fetch_resume_token(object(), 'client1', bare.local_manager.datasets[0], DEST)

    def test_stream_step(self, bare):
        with pytest.raises(NotImplementedError):
            bare._stream_step(object(), 'client1', bare.local_manager.datasets[0], DEST,
                              _SendStream(MagicMock(), 'fresh'))

    def test_backup_dataset_on_the_bare_class_reaches_the_transport_hook_first(self, bare):
        dsi = bare.local_manager.datasets[0]
        with pytest.raises(NotImplementedError):
            bare.backup_dataset(dsi, RemoteDatasetConfig(destination=DEST))

    def test_the_hooks_with_a_default_do_nothing_or_derive_from_the_transport(self, bare):
        assert bare.close() is None
        assert bare._before_destination_check() is None
        transport = types.SimpleNamespace(base_url='https://x.example:8443')
        assert bare._where(transport) == 'https://x.example:8443'
        assert bare._peer(transport) == 'server https://x.example:8443'

    def test_the_default_where_needs_a_base_url(self, bare):
        with pytest.raises(AttributeError):
            bare._where(object())

    def test_a_subclass_overriding_where_changes_peer_with_it(self):
        class Local(FakeSender):
            def _where(self, transport):
                return 'local pool'
        config = make_config()
        sender = Local(config, DatasetManager(config), None)
        assert sender._peer(sender.transport) == 'server local pool'


@pytest.mark.unit
class TestLoggerName:
    def test_the_chain_logs_as_zfsbackup_chain_by_default(self):
        assert ChainSender._logger.name == 'zfsbackup.chain'

    def test_a_subclass_that_sets_nothing_logs_under_the_chain_logger(self, rig_factory, caplog):
        rig = rig_factory(listing(S1, S2), common=S1)
        with caplog.at_level(logging.INFO):
            rig.run()
        names = {r.name for r in caplog.records}
        assert names == {'zfsbackup.chain'}

    def test_the_remote_manager_still_logs_as_zfsbackup_remote(self, caplog):
        from zfsbackup.remote import RemoteBackupManager
        config = make_config()
        manager = RemoteBackupManager(config, DatasetManager(config), client_id='client1')
        assert manager._logger.name == 'zfsbackup.remote'
        dsi = manager.local_manager.datasets[0]
        with caplog.at_level(logging.DEBUG):
            assert manager.backup_dataset(dsi, RemoteDatasetConfig(destination='nowhere')) is False
        (record,) = [r for r in caplog.records if 'Unknown destination' in r.getMessage()]
        assert record.name == 'zfsbackup.remote'

    def test_chain_methods_of_the_remote_manager_log_through_its_logger(self, caplog):
        from zfsbackup.remote import RemoteBackupManager
        config = make_config()
        manager = RemoteBackupManager(config, DatasetManager(config), client_id='client1')
        dsi = manager.local_manager.datasets[0]
        with caplog.at_level(logging.INFO):
            for _ in range(3):
                manager._token_skipped(dsi, DEST, 'boom')
        (record,) = [r for r in caplog.records if 'resume-token check has skipped' in r.getMessage()]
        assert record.name == 'zfsbackup.remote' and record.levelname == 'ERROR'

    def test_the_remote_manager_is_a_chain_sender_overriding_every_hook(self):
        from zfsbackup.remote import RemoteBackupManager
        assert issubclass(RemoteBackupManager, ChainSender)
        for hook in ('_ready_transport', '_negotiate', '_fetch_resume_token', '_stream_step',
                     'close', '_before_destination_check'):
            assert getattr(RemoteBackupManager, hook) is not getattr(ChainSender, hook), hook
        # `_where` / `_peer` are the chain's defaults: the pinned transport has a `base_url`
        for default in ('_where', '_peer'):
            assert default not in vars(RemoteBackupManager), default
        # ... and none of the chain's own methods is overridden by accident
        for shared in ('backup_dataset', '_transfer', '_push_step', '_record_step', '_try_resume',
                       '_start_fresh', '_plan_chain', '_resume_target', '_settle_up_to_date'):
            assert shared not in vars(RemoteBackupManager), shared

    def test_the_names_remote_re_exports_are_the_chains(self):
        import zfsbackup.chain as chain
        import zfsbackup.remote as remote
        for name in ('NegotiationFailed', '_OK', '_FAILED', '_STOP', '_ANCHOR_FAILED', '_SendStream',
                     '_SEND_MODES', 'RESUME_TOKEN_SKIP_ERROR_AFTER', 'TOKEN_SKIP_NOTE',
                     '_retry_word', '_token_error_is_definitive', '_STALE_TOKEN_ERRORS'):
            assert getattr(remote, name) is getattr(chain, name), name


def _modules_after(code):
    proc = subprocess.run(
        [sys.executable, '-c', code], capture_output=True, text=True, timeout=120,
        env=hermetic_env(PYTHONPATH=os.pathsep.join(sys.path)),
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout.split()


_PROBE = (
    "import sys\n"
    "{imp}\n"
    "for name in ('requests', 'websocket', 'zfsbackup.identity', 'zfsbackup.remote', 'zfsbackup.chain'):\n"
    "    print(name, name in sys.modules)\n"
)


@pytest.mark.unit
@pytest.mark.subprocess
class TestNoTransportImports:
    def test_chain_imports_neither_requests_websocket_nor_identity(self):
        out = _modules_after(_PROBE.format(imp='import zfsbackup.chain'))
        assert dict(zip(out[::2], out[1::2])) == {
            'requests': 'False', 'websocket': 'False', 'zfsbackup.identity': 'False',
            'zfsbackup.remote': 'False', 'zfsbackup.chain': 'True',
        }

    def test_the_probe_does_see_them_when_remote_is_imported(self):
        """The control: the same probe after importing the HTTPS subclass."""
        out = _modules_after(_PROBE.format(imp='import zfsbackup.remote'))
        assert dict(zip(out[::2], out[1::2])) == {
            'requests': 'True', 'websocket': 'True', 'zfsbackup.identity': 'True',
            'zfsbackup.remote': 'True', 'zfsbackup.chain': 'True',
        }


@pytest.mark.unit
class TestLastProblemOwnRefusal:
    """B3 review (CC 3): senders of one destination share the `REFUSALS` table, so a sibling's pop or
    replacement between this call's refusal and its classification must not erase the status text.
    `last_problem` reads the call's OWN message first (`_call_refusal_messages`); the shared table is only
    for a key this call recorded no message for; `_forget_refusal` ends a refusal in both places."""

    KEY = (DS, DEST)

    def sender(self, rig_factory):
        rig = rig_factory()
        rig.sender._log_refusal_once(self.KEY, 'refused: not enrolled')
        return rig.sender

    def test_the_own_message_is_reported(self, rig_factory):
        assert self.sender(rig_factory).last_problem(DS, DEST) == 'refused: not enrolled'

    def test_a_siblings_pop_cannot_erase_it(self, rig_factory):
        from zfsbackup.sendmemory import REFUSALS
        sender = self.sender(rig_factory)
        sender._memory.pop(REFUSALS, self.KEY)                       # another sender's good answer
        assert sender.last_problem(DS, DEST) == 'refused: not enrolled'

    def test_a_siblings_replacement_cannot_change_it(self, rig_factory):
        from zfsbackup.sendmemory import REFUSALS
        sender = self.sender(rig_factory)
        sender._memory.put(REFUSALS, self.KEY, 'a different refusal')
        assert sender.last_problem(DS, DEST) == 'refused: not enrolled'

    def test_the_warning_variant_is_kept_too(self, rig_factory):
        from zfsbackup.sendmemory import REFUSALS
        rig = rig_factory()
        rig.sender._log_warning_once(self.KEY, 'will heal')
        rig.sender._memory.pop(REFUSALS, self.KEY)
        assert rig.sender.last_problem(DS, DEST) == 'will heal'

    def test_forget_refusal_ends_it_in_the_memory_and_in_the_call(self, rig_factory):
        from zfsbackup.sendmemory import REFUSALS
        sender = self.sender(rig_factory)
        assert sender._forget_refusal(self.KEY) == 'refused: not enrolled'
        assert sender.last_problem(DS, DEST) is None
        assert self.KEY not in sender._call_refusals and self.KEY not in sender._call_refusal_messages
        assert sender._memory.get(REFUSALS, self.KEY) is None

    def test_forgetting_what_is_not_there_is_harmless(self, rig_factory):
        assert rig_factory().sender._forget_refusal(self.KEY) is None

    def test_a_key_without_an_own_message_falls_back_to_the_shared_table(self, rig_factory):
        """`(None, destination)`: a refusal an EARLIER call on this manager logged."""
        from zfsbackup.sendmemory import REFUSALS
        rig = rig_factory()
        rig.sender._memory.put(REFUSALS, (None, DEST), 'earlier refusal')
        rig.sender._call_refusals.add((None, DEST))
        assert rig.sender.last_problem(DS, DEST) == 'earlier refusal'

    def test_the_next_call_starts_with_no_own_messages(self, rig_factory):
        rig = rig_factory(listing(S1))
        rig.sender._log_refusal_once(self.KEY, 'refused')
        rig.run()
        assert self.KEY not in rig.sender._call_refusal_messages

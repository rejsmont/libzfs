"""`_sendloop_support.run_worker_until_sent`, the driver the real-ZFS tier runs the REAL remote worker with.

The real tier cannot run in the mocked suite, so the driver's own behaviour is pinned here over `LoopRig`'s
fake transport (a REAL scheduler loop, a REAL `StatusWriter`): it returns once every wanted pair has been sent
in this call, it FAILS FAST on a wanted pair's error record (instead of waiting out the watchdog), it ignores
an earlier incarnation's error record and the error of a pair nobody asked for, and `extra_patches` runs inside
the run's `monkeypatch` context.

Mutation-checked (scratch copy): without the fail-fast branch the failing-pair tests (3) run into the watchdog
and fail on its message; ignoring `started` fails the stale-record test; ignoring `wanted` fails the
other-pair test; calling `extra_patches` outside the context fails the undo test.
"""

import time

import pytest

from _sendloop_support import run_worker_until_sent, seed_status, stretch
from test_zfsbackup_status_workers import RemoteRig, sdir  # noqa: F401  (fixture)

pytestmark = pytest.mark.unit

DEST = 'off'


@pytest.fixture
def two_pairs(monkeypatch, sdir):
    """`pool/a` and `pool/b` to one destination, a plain fake transport that sends everything."""
    return RemoteRig(monkeypatch, sdir, names=('pool/a', 'pool/b'))


def fail(rrig, dataset, problem='the copy is not ours'):
    """Make every send of `dataset` an incomplete one with `problem` (a recorded, final error)."""
    answers = rrig.rbm.backup_dataset
    answers.side_effect = lambda dsi, remote_cfg, should_stop=None: dsi.name != dataset
    rrig.rbm.last_problem.side_effect = lambda ds, dest: problem if ds == dataset else None


class TestRunsToCompletion:
    def test_it_returns_once_every_wanted_pair_was_sent(self, two_pairs, monkeypatch):
        run_worker_until_sent(two_pairs.rig.worker, monkeypatch, [('pool/a', DEST), ('pool/b', DEST)], limit=30.0)
        recs = two_pairs.records()
        assert recs[('send', 'pool/a', DEST)].result == 'ok'
        assert recs[('send', 'pool/b', DEST)].result == 'ok'

    def test_extra_patches_run_inside_the_context_and_are_undone_with_it(self, two_pairs, monkeypatch):
        import zfsbackup.sendloop as sendloop
        seen = []

        def extra(mp):
            mp.setattr(sendloop, 'DISCOVERY_JOIN', 123.0)
            seen.append(('applied', sendloop.DISCOVERY_JOIN))

        original = sendloop.DISCOVERY_JOIN
        run_worker_until_sent(
            two_pairs.rig.worker, monkeypatch, [('pool/a', DEST), ('pool/b', DEST)], limit=30.0, extra_patches=extra,
        )
        assert seen == [('applied', 123.0)]
        assert sendloop.DISCOVERY_JOIN == original, 'undone with the run, not left behind'

    def test_the_patches_are_in_force_while_the_worker_runs(self, two_pairs, monkeypatch):
        import zfsbackup.sendloop as sendloop
        during = []

        def backup(dsi, remote_cfg, should_stop=None):
            during.append(sendloop.DISCOVERY_JOIN)
            return True

        two_pairs.rbm.backup_dataset.side_effect = backup
        run_worker_until_sent(
            two_pairs.rig.worker, monkeypatch, [('pool/a', DEST)], limit=30.0,
            extra_patches=lambda mp: mp.setattr('zfsbackup.sendloop.DISCOVERY_JOIN', 77.0),
        )
        assert during and set(during) == {77.0}


class TestFailsFast:
    def test_a_wanted_pair_with_an_error_record_stops_the_run_and_names_it(self, two_pairs, monkeypatch):
        fail(two_pairs, 'pool/b', 'resume token check failed: HTTP 500')
        began = time.monotonic()
        with pytest.raises(AssertionError) as raised:
            run_worker_until_sent(
                two_pairs.rig.worker, monkeypatch, [('pool/a', DEST), ('pool/b', DEST)], limit=20.0,
            )
        message = str(raised.value)
        assert 'a pair failed' in message and 'watchdog' not in message
        assert f'pool/b -> {DEST}: resume token check failed: HTTP 500' in message
        assert 'pool/a' not in message
        assert time.monotonic() - began < 10.0 * stretch(), 'far below the 20 s watchdog'

    def test_the_other_pairs_in_flight_are_drained_not_abandoned(self, two_pairs, monkeypatch):
        fail(two_pairs, 'pool/b')
        with pytest.raises(AssertionError, match='a pair failed'):
            run_worker_until_sent(
                two_pairs.rig.worker, monkeypatch, [('pool/a', DEST), ('pool/b', DEST)], limit=20.0,
            )
        loop = two_pairs.rig.worker._loop
        assert not loop._senders and not loop._discoveries

    def test_an_error_without_text_still_names_the_pair(self, two_pairs, monkeypatch):
        fail(two_pairs, 'pool/a', problem=None)
        with pytest.raises(AssertionError, match=r'pool/a -> off: not completed'):
            run_worker_until_sent(two_pairs.rig.worker, monkeypatch, [('pool/a', DEST)], limit=20.0)

    def test_an_error_record_of_an_earlier_incarnation_does_not_count(self, monkeypatch, sdir):
        seed_status(sdir, ('pool/a', DEST, False, 'last night: connection refused'), age=7200.0)
        rrig = RemoteRig(monkeypatch, sdir, names=('pool/a',))
        run_worker_until_sent(rrig.rig.worker, monkeypatch, [('pool/a', DEST)], limit=30.0)
        assert rrig.records()[('send', 'pool/a', DEST)].result == 'ok'

    def test_an_error_of_a_pair_nobody_asked_for_does_not_count(self, two_pairs, monkeypatch):
        fail(two_pairs, 'pool/b')
        run_worker_until_sent(two_pairs.rig.worker, monkeypatch, [('pool/a', DEST)], limit=30.0)
        recs = two_pairs.records()
        assert recs[('send', 'pool/a', DEST)].result == 'ok'


class TestTheWatchdogStaysForEverythingElse:
    def test_a_pair_that_never_gets_sent_is_stopped_by_the_watchdog(self, two_pairs, monkeypatch):
        with pytest.raises(AssertionError, match='watchdog stopped it'):
            run_worker_until_sent(
                two_pairs.rig.worker, monkeypatch, [('pool/a', DEST), ('pool/nobody', DEST)], limit=1.0,
            )

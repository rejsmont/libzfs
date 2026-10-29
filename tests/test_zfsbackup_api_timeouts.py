"""S-14: the API handlers' `zfs` calls run under a per-request time budget (`api.ZFSBudget`).

A handler whose `zfs` call does not answer (a hung pool) used to hold a request thread, and the claim on
its server dataset, for ever. Now every non-streaming call of a handler runs under `libzfseasy.timeout(<what is
left of the request's budget>)`, and a call that times out (or is refused because an earlier timed-out child
of this worker is still stuck) is answered as a transient failure the client skips quietly:

* HTTP 503 `api.ZFS_TIMEOUT_ERROR` (`{'error': 'zfs timed out; retry later', 'retry': True, 'reason':
  'zfs_timeout'}`) on the plain endpoints, the same JSON as a websocket text message on `/stream`; never a
  500, never `reason: 'listing'`;
* the claim on the server dataset is released (before the reply on a stream); nothing is spawned (no
  `zfs receive`) and nothing about the outcome of a write or an abort that timed out is assumed;
* one WARNING per (handler, server dataset) episode, DEBUG after, re-armed by the next success;
* `/enroll` keeps its existing 503 `{'reason': 'zfs', 'retry': True}`.

Layout: `TestZFSBudget` (the class), `TestNegotiateTimeouts`, `TestResumeTokenTimeouts`, `TestStreamTimeouts`,
`TestSharedBudget` (a fake clock: the calls of one request share the budget, a stream restarts it after the
header), `TestTimeoutLogging`, `TestStuckChildFailsFast` (the libzfseasy registry, with fake entries and with a
real child on an "unkillable" pool), `TestEnrollTimeout`. The client half is
`test_zfsbackup_remote_zfs_timeout.py`.

Every `zfs` entry point is mocked at `zfsbackup.api.zfs.*` (the tests that need the real libzfseasy admission
patch `subprocess.Popen` or point `ZFS_CMD` at a fake script); no real `zfs` is reached.
"""

import contextlib
import itertools
import json
import logging
import stat
import subprocess
import sys
import threading
import time
import types
import unittest.mock as mock_module
from unittest.mock import MagicMock

import pytest

import libzfseasy
from libzfseasy import zfs as zfs_module
from libzfseasy.zfs import _timeout_var

from _api_support import (
    CLIENT_ID, Authed, _find_in_closures, enroll_body, make_client_cert, pending_configs,
    streams_in_progress,
)
from _snapshot_support import listed_snapshot
from _stream_support import HEADER, get_value_answers, stream_script
from test_zfsbackup_api import GOOD_CONFIG, STREAM_PATH, TOKEN, Stream  # noqa: F401  (Stream: a harness)
from zfsbackup import api, identity
from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig

pytestmark = pytest.mark.unit

SERVER_DS = "pool/backups/client1/data"
OTHER_DS = "tank/data"                  # maps to the same server dataset as pool/data
TOKEN_URL = f"/backup/{CLIENT_ID}/pool/data/resume_token"
TIMEOUT_TEXT = "zfs list timed out after 30s; outcome unknown"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class Fault:
    """A timeout that can be switched off: `error()` is the `ZFSTimeoutError` libzfseasy raises, `maybe()` raises
    it while `on`. `raised` counts them, so a scenario proves its fault was really hit exactly where intended."""

    def __init__(self):
        self.on = True
        self.raised = 0

    def error(self):
        self.raised += 1
        return libzfseasy.ZFSTimeoutError(TIMEOUT_TEXT)

    def maybe(self):
        if self.on:
            raise self.error()

    def raiser(self, *args, **kwargs):
        """A mock `side_effect` that only raises."""
        self.maybe()


def nth(fault, n, value=True):
    """A mock `side_effect`: the `n`th call (1-based) raises while `fault.on`, every call answers `value`."""
    calls = itertools.count(1)

    def side_effect(*args, **kwargs):
        if next(calls) == n:
            fault.maybe()
        return value

    return side_effect


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def slow(clock, seconds, mock, value=None):
    """`mock` takes `seconds` of the fake clock per call and answers `value`."""
    mock.side_effect = lambda *args, **kwargs: (clock.advance(seconds), value)[1]


def timeout_records(caplog):
    return [r for r in caplog.records
            if r.name == "zfsbackup.api" and "did not answer in time" in r.getMessage()]


def _config():
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data")],
        remote_backup=RemoteServerConfig(target_dataset="pool/backups", enabled=True),
    )


def negotiate(client, dataset="pool/data", snapshots=()):
    return client.post(
        f"/backup/{CLIENT_ID}/{dataset}/negotiate", json={"snapshots": list(snapshots), "config": GOOD_CONFIG},
    )


def assert_zfs_timeout(resp):
    assert resp.status_code == 503, resp.get_data(as_text=True)
    assert resp.get_json() == api.ZFS_TIMEOUT_ERROR


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_stuck_registry():
    """libzfseasy's registry of children that outlived a timeout is module state: every test starts with it empty
    (a test that fills it must not make the rest of the suite fail fast) and the originals are put back."""
    saved_lock = zfs_module._stuck_lock
    saved = list(zfs_module._stuck)
    zfs_module._stuck.clear()
    assert _timeout_var.get() is None
    yield
    zfs_module._stuck_lock = saved_lock
    zfs_module._stuck[:] = saved


@pytest.fixture
def fault():
    return Fault()


@pytest.fixture
def untimed(monkeypatch):
    """Which `zfs` calls ran outside a libzfseasy timeout. `untimed.watch(name=mock, ...)` registers mocks; at
    teardown the test fails if any watched mock was called while no `libzfseasy.timeout` was in effect, i.e. if a
    handler called `zfs` directly instead of through the request budget (`_zfs`). The `zfs.receive` of a stream
    is deliberately not watched: it must run untimed."""
    watched = {}
    calls = []
    real = mock_module.CallableMixin._mock_call

    def spy(self, /, *args, **kwargs):
        name = watched.get(id(self))
        if name is not None and _timeout_var.get() is None:
            calls.append(name)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(mock_module.CallableMixin, "_mock_call", spy)

    class Watch:
        pass

    watch = Watch()
    watch.calls = calls
    watch.watch = lambda **mocks: watched.update({id(m): n for n, m in mocks.items()})
    yield watch
    assert calls == [], f"zfs calls made outside the request budget: {calls}"


@pytest.fixture
def zfs(mocker, untimed):
    """The `zfs` calls negotiate and `/resume_token` make, patched at `zfsbackup.api.zfs.*`. `answers` is the
    `get.value` stand-in (`.values` the live table): the dataset's LOCAL client id is the client's, nothing else is
    set, it is unencrypted."""
    z = types.SimpleNamespace()
    z.exists = mocker.patch("zfsbackup.api.zfs.exists", return_value=False)
    z.list = mocker.patch("zfsbackup.api.zfs.list", return_value=[])
    z.create = mocker.patch("zfsbackup.api.zfs.create")
    z.set = mocker.patch("zfsbackup.api.zfs.set")
    z.props = mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
    z.answers = get_value_answers({
        PROP_CLIENT_ID: CLIENT_ID, PROP_CONFIG: None, "encryption": "off", "encryptionroot": None,
        "receive_resume_token": None, "type": "filesystem",
    })
    z.get_value = mocker.patch("zfsbackup.api.zfs.get.value", side_effect=z.answers)
    untimed.watch(exists=z.exists, list=z.list, create=z.create, set=z.set, props=z.props, get_value=z.get_value)
    return z


def fail_property(z, prop, fault):
    """`zfs.get.value` of `prop` times out; every other property answers as before."""
    inner = z.answers

    def get(ds, p, *args, **kwargs):
        if p == prop:
            fault.maybe()
        return inner(ds, p, *args, **kwargs)

    z.get_value.side_effect = get


@pytest.fixture
def authed():
    return Authed(_config())


@pytest.fixture
def client(authed):
    return authed.client()


@pytest.fixture
def limits(mocker):
    """The `libzfseasy.timeout(seconds)` limits `ZFSBudget.call` set, in order (the real context manager still
    runs)."""
    seen = []
    real = libzfseasy.timeout

    @contextlib.contextmanager
    def spy(seconds):
        seen.append(seconds)
        with real(seconds):
            yield

    mocker.patch("zfsbackup.api.zfs_timeout", spy)
    return seen


@pytest.fixture
def clock(mocker):
    """Every `ZFSBudget` the app makes runs on a fake clock, with a 30 s budget."""
    fake = FakeClock()
    real = api.ZFSBudget
    mocker.patch("zfsbackup.api.ZFS_REQUEST_BUDGET", 30.0)       # the tests below do their sums with 30 s
    mocker.patch("zfsbackup.api.ZFSBudget", lambda *a, **k: real(*a, clock=fake, **k))
    return fake


@pytest.fixture
def stream(mocker, untimed):
    """The `Stream` harness of `test_zfsbackup_api` (its mocks answer an existing, owned, configured dataset with
    no snapshots and a resume token to abort), with `read_zfsbackup_properties` mocked as `.props`."""
    props = mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
    s = Stream(mocker)
    s.props = props
    s.token(TOKEN)
    untimed.watch(
        exists=s.exists, list=s.list, create=s.create, set=s.set, abort=s.abort, get_value=s.get_value, props=props,
    )
    return s


def rearm(stream):
    """A fresh websocket script for the next `run` (the first may have consumed the header)."""
    stream.ws.receive.side_effect = stream_script(b"chunk")
    stream._default_script = stream.ws.receive.side_effect


# ---------------------------------------------------------------------------
# ZFSBudget
# ---------------------------------------------------------------------------


class TestTheWatchItself:
    def test_a_call_outside_a_timeout_is_recorded_and_one_inside_is_not(self, untimed):
        probe = MagicMock()
        untimed.watch(probe=probe)
        with libzfseasy.timeout(5):
            probe()
        assert untimed.calls == []
        probe()
        assert untimed.calls == ["probe"]
        untimed.calls.clear()                       # (the fixture would fail the test at teardown)

    def test_an_unwatched_mock_is_ignored(self, untimed):
        MagicMock()()
        assert untimed.calls == []


class TestZFSBudget:
    def test_the_default_is_the_module_constant(self):
        assert api.ZFS_REQUEST_BUDGET > 0
        assert api.ZFSBudget().seconds == api.ZFS_REQUEST_BUDGET
        assert api.ZFSBudget(5).seconds == 5

    def test_the_constant_is_read_when_a_budget_is_made(self, mocker):
        mocker.patch("zfsbackup.api.ZFS_REQUEST_BUDGET", 7.5)
        assert api.ZFSBudget().seconds == 7.5

    def test_call_passes_arguments_and_returns_the_result(self):
        budget = api.ZFSBudget(30, clock=FakeClock())
        seen = []

        def fn(a, b, *, c):
            seen.append((a, b, c))
            return "result"

        assert budget.call(fn, 1, 2, c=3) == "result"
        assert seen == [(1, 2, 3)]

    def test_the_call_runs_under_what_is_left_of_the_budget(self):
        clock = FakeClock()
        budget = api.ZFSBudget(30, clock=clock)
        limits = []

        def fn():
            limits.append(_timeout_var.get())

        budget.call(fn)
        clock.advance(20)
        budget.call(fn)
        clock.advance(9.5)
        budget.call(fn)
        assert limits == [30.0, 10.0, 0.5]

    def test_the_limit_is_lifted_after_the_call_and_after_a_failure(self):
        budget = api.ZFSBudget(30, clock=FakeClock())
        budget.call(lambda: None)
        assert _timeout_var.get() is None
        with pytest.raises(RuntimeError, match="boom"):
            budget.call(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        assert _timeout_var.get() is None

    @pytest.mark.parametrize("advance", [30, 31, 1000])
    def test_a_spent_budget_raises_without_running_anything(self, advance):
        clock = FakeClock()
        budget = api.ZFSBudget(30, clock=clock)
        clock.advance(advance)
        fn = MagicMock()
        with pytest.raises(libzfseasy.ZFSTimeoutError, match=r"the 30 s budget for this request is spent"):
            budget.call(fn)
        fn.assert_not_called()

    def test_a_bare_zero_budget_is_spent_at_once(self):
        fn = MagicMock()
        with pytest.raises(libzfseasy.ZFSTimeoutError):
            api.ZFSBudget(0, clock=FakeClock()).call(fn)
        fn.assert_not_called()

    def test_the_functions_own_timeout_error_propagates_untouched(self):
        budget = api.ZFSBudget(30, clock=FakeClock())
        error = libzfseasy.ZFSTimeoutError("zfs list timed out after 30s; outcome unknown")

        def fn():
            raise error

        with pytest.raises(libzfseasy.ZFSTimeoutError) as caught:
            budget.call(fn)
        assert caught.value is error

    def test_a_budget_is_per_instance(self):
        clock = FakeClock()
        spent = api.ZFSBudget(30, clock=clock)
        clock.advance(100)
        fresh = api.ZFSBudget(30, clock=clock)
        fresh.call(lambda: None)
        with pytest.raises(libzfseasy.ZFSTimeoutError):
            spent.call(lambda: None)

    def test_the_real_libzfseasy_limit_expires_a_real_slow_call(self, tmp_path, monkeypatch):
        """No mock between the budget and libzfseasy: a fake `zfs` that sleeps is cut at the deadline and the
        outcome is a `ZFSTimeoutError`, not a hang."""
        script = tmp_path / "zfs"
        script.write_text("#!/bin/sh\nexec sleep 30\n")
        script.chmod(script.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setenv("ZFS_CMD", str(script))
        monkeypatch.setattr(zfs_module, "_EXPIRY_WAIT", 1.0)
        started = time.monotonic()
        with pytest.raises(libzfseasy.ZFSTimeoutError, match="timed out"):
            api.ZFSBudget(0.3).call(libzfseasy.exists, libzfseasy.types.Filesystem("pool/x"))
        assert time.monotonic() - started < 10
        assert zfs_module._stuck == []


class TestBudgetInequality:
    """`api` asserts at import that the server answers `zfs_timeout` while the client is still listening:
    budget + kill overhead + helper margin < min(HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT)."""

    @staticmethod
    def load(replacements):
        """A copy of `zfsbackup/api.py` with `replacements` applied, executed as a throwaway module."""
        import importlib.util
        from pathlib import Path

        source = Path(api.__file__).read_text()
        for old, new in replacements:
            assert source.count(old) == 1, old
            source = source.replace(old, new)
        spec = importlib.util.spec_from_loader("zfsbackup._api_inequality_copy", loader=None)
        module = importlib.util.module_from_spec(spec)
        module.__file__ = api.__file__
        exec(compile(source, api.__file__, "exec"), module.__dict__)
        return module

    def test_the_shipped_constants_satisfy_it(self):
        from zfsbackup.remote import HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT
        assert (api.ZFS_REQUEST_BUDGET + api._ZFS_KILL_OVERHEAD + api._ZFS_HELPER_MARGIN
                < min(HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT))

    def test_the_unmodified_source_imports(self):
        assert self.load([]).ZFS_REQUEST_BUDGET == api.ZFS_REQUEST_BUDGET

    @pytest.mark.parametrize("budget", ["20.0", "30.0", "100.0"])
    def test_a_budget_past_the_inequality_fails_the_import(self, budget):
        with pytest.raises(AssertionError, match="ZFS_REQUEST_BUDGET must stay below"):
            self.load([("ZFS_REQUEST_BUDGET = %s\n" % api.ZFS_REQUEST_BUDGET, "ZFS_REQUEST_BUDGET = %s\n" % budget)])

    def test_a_budget_just_inside_the_inequality_imports(self):
        from zfsbackup.remote import HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT
        room = min(HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT) - api._ZFS_KILL_OVERHEAD - api._ZFS_HELPER_MARGIN
        self.load([("ZFS_REQUEST_BUDGET = %s\n" % api.ZFS_REQUEST_BUDGET, "ZFS_REQUEST_BUDGET = %s\n" % (room - 0.5))])

    def test_a_larger_kill_overhead_or_margin_fails_the_import_too(self):
        with pytest.raises(AssertionError):
            self.load([("_ZFS_HELPER_MARGIN = %s\n" % api._ZFS_HELPER_MARGIN, "_ZFS_HELPER_MARGIN = 60.0\n")])
        with pytest.raises(AssertionError):
            self.load([("_ZFS_KILL_OVERHEAD = %s\n" % api._ZFS_KILL_OVERHEAD, "_ZFS_KILL_OVERHEAD = 60.0\n")])

    def test_a_client_timeout_that_shrinks_fails_the_import(self):
        from zfsbackup import remote
        original = remote.RESUME_TOKEN_TIMEOUT
        remote.RESUME_TOKEN_TIMEOUT = 10
        try:
            with pytest.raises(AssertionError):
                self.load([])
        finally:
            remote.RESUME_TOKEN_TIMEOUT = original


# ---------------------------------------------------------------------------
# negotiate
# ---------------------------------------------------------------------------


def exists_first(z, client, f):
    z.exists.side_effect = f.raiser


def ownership_exists(z, client, f):
    z.exists.side_effect = nth(f, 2, True)


def ownership_properties(z, client, f):
    z.exists.return_value = True
    z.props.side_effect = f.raiser


def ownership_client_id(z, client, f):
    z.exists.return_value = True
    fail_property(z, PROP_CLIENT_ID, f)


def pending_owner(z, client, f):
    # tank/data negotiates first and leaves a pending entry for the (missing) server dataset; pool/data
    # then meets an existing placeholder without an owner and reads its properties a second time
    assert negotiate(client, OTHER_DS).status_code == 200
    z.create.reset_mock()
    z.exists.return_value = True
    z.props.side_effect = [{}, f.error()]


def ancestors_exists(z, client, f):
    z.exists.side_effect = nth(f, 2, False)


def ancestors_owner(z, client, f):
    z.exists.side_effect = [False, True]
    fail_property(z, PROP_CLIENT_ID, f)


def ancestors_token(z, client, f):
    z.exists.side_effect = [False, True]
    fail_property(z, "receive_resume_token", f)


def ancestors_listing(z, client, f):
    z.exists.side_effect = [False, True]
    z.answers.values["receive_resume_token"] = "1-partial"
    z.list.side_effect = f.raiser


def retention_config_read(z, client, f):
    z.exists.return_value = True
    fail_property(z, PROP_CONFIG, f)


def set_property(z, client, f):
    z.exists.return_value = True
    z.set.side_effect = f.raiser


def create_parent(z, client, f):
    z.create.side_effect = f.raiser


def listing_strict(z, client, f):
    z.exists.return_value = True
    z.list.side_effect = f.raiser


def listing_not_strict(z, client, f):
    z.exists.return_value = True

    def listing(*args, **kwargs):
        if kwargs.get("strict"):
            raise RuntimeError("a row libzfseasy cannot trust")
        f.maybe()

    z.list.side_effect = listing


def server_copy_encryption(z, client, f):
    z.exists.return_value = True
    z.list.return_value = [listed_snapshot("a", SERVER_DS)]
    fail_property(z, "encryption", f)


def server_copy_encryptionroot(z, client, f):
    z.exists.return_value = True
    z.list.return_value = [listed_snapshot("a", SERVER_DS)]
    z.answers.values["encryption"] = "aes-256-gcm"
    fail_property(z, "encryptionroot", f)


NEGOTIATE_SITES = [
    pytest.param(exists_first, id="exists-of-the-server-dataset"),
    pytest.param(ownership_exists, id="ownership-exists"),
    pytest.param(ownership_properties, id="ownership-read_zfsbackup_properties"),
    pytest.param(ownership_client_id, id="ownership-local-client-id"),
    pytest.param(pending_owner, id="pending-owner-read"),
    pytest.param(ancestors_exists, id="ancestors-exists"),
    pytest.param(ancestors_owner, id="ancestors-owner"),
    pytest.param(ancestors_token, id="ancestors-resume-token"),
    pytest.param(ancestors_listing, id="ancestors-snapshot-listing"),
    pytest.param(retention_config_read, id="stored-config-read"),
    pytest.param(set_property, id="set"),
    pytest.param(create_parent, id="create"),
    pytest.param(listing_strict, id="snapshot-listing-strict"),
    pytest.param(listing_not_strict, id="snapshot-listing-not-strict"),
    pytest.param(server_copy_encryption, id="server-copy-encryption"),
    pytest.param(server_copy_encryptionroot, id="server-copy-encryptionroot"),
]


@pytest.mark.usefixtures("zfs")
class TestNegotiateTimeouts:
    @pytest.mark.parametrize("site", NEGOTIATE_SITES)
    def test_a_timeout_at_every_zfs_call_is_the_one_503(self, site, client, authed, zfs, fault, caplog):
        site(zfs, client, fault)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = negotiate(client, snapshots=["a"])
        assert_zfs_timeout(resp)
        assert fault.raised == 1, "the scenario did not time out at the call it names"
        assert streams_in_progress(authed.app) == set()
        assert authed.gate.active == 0
        assert [r.levelno for r in timeout_records(caplog)] == [logging.WARNING]
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_the_reply_is_not_the_busy_or_the_reloading_one(self, client, zfs, fault):
        zfs.exists.side_effect = fault.raiser
        body = negotiate(client).get_json()
        assert body != api.BUSY_ERROR and body != api.RELOADING_ERROR
        assert body["reason"] == "zfs_timeout" and body["retry"] is True
        assert "reloading" not in body["error"]

    def test_the_reply_does_not_carry_the_exception_text(self, client, zfs, fault):
        zfs.exists.side_effect = fault.raiser
        assert TIMEOUT_TEXT not in negotiate(client).get_data(as_text=True)

    def test_a_timed_out_create_forgets_the_pending_entry_and_is_done_again_next_time(
        self, client, authed, zfs, fault
    ):
        zfs.create.side_effect = fault.raiser
        assert_zfs_timeout(negotiate(client))
        assert dict(pending_configs(authed.app)) == {}          # nothing is assumed: the entry is not kept
        fault.on = False
        zfs.create.side_effect = None
        assert negotiate(client).status_code == 200
        assert zfs.create.call_count == 2                        # the parent chain is created again
        assert SERVER_DS in pending_configs(authed.app)

    def test_a_timed_out_set_is_done_again_by_the_next_negotiate(self, client, authed, zfs, fault):
        zfs.exists.return_value = True
        zfs.set.side_effect = fault.raiser
        assert_zfs_timeout(negotiate(client))
        fault.on = False
        zfs.set.side_effect = None
        assert negotiate(client).status_code == 200
        assert zfs.set.call_count == 2

    def test_a_read_that_timed_out_before_a_write_writes_nothing(self, client, zfs, fault):
        zfs.exists.return_value = True
        fail_property(zfs, PROP_CONFIG, fault)
        assert_zfs_timeout(negotiate(client))
        zfs.set.assert_not_called()
        zfs.create.assert_not_called()

    def test_a_timeout_is_checked_before_the_snapshot_listing_and_nothing_after_it_runs(
        self, client, zfs, fault
    ):
        zfs.exists.return_value = True
        zfs.set.side_effect = fault.raiser
        assert_zfs_timeout(negotiate(client))
        zfs.list.assert_not_called()

    def test_the_following_negotiate_is_not_busy(self, client, authed, zfs, fault):
        zfs.exists.side_effect = fault.raiser
        assert_zfs_timeout(negotiate(client))
        fault.on = False
        zfs.exists.side_effect = None
        resp = negotiate(client)
        assert resp.status_code == 200 and resp.get_json() != api.BUSY_ERROR

    def test_every_call_runs_under_a_limit_no_longer_than_the_budget(self, client, zfs, limits):
        seen = []
        zfs.exists.side_effect = lambda ds: (seen.append(_timeout_var.get()), False)[1]
        zfs.create.side_effect = lambda *a, **k: seen.append(_timeout_var.get())
        assert negotiate(client).status_code == 200
        assert len(seen) == 3                                    # exists, exists of the parent, create
        assert all(0 < s <= api.ZFS_REQUEST_BUDGET for s in seen)
        assert _timeout_var.get() is None                        # and nothing is left set
        assert limits and all(0 < s <= api.ZFS_REQUEST_BUDGET for s in limits)

    # a plain zfs failure is still what it was

    def test_a_plain_failure_to_set_is_still_the_500(self, client, zfs):
        zfs.exists.return_value = True
        zfs.set.side_effect = RuntimeError("cannot set property: permission denied")
        resp = negotiate(client)
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to record the configuration of {SERVER_DS}"}

    def test_a_plain_failure_of_the_ownership_check_is_still_the_500(self, client, zfs):
        zfs.exists.return_value = True
        zfs.props.side_effect = RuntimeError("pool I/O is currently suspended")
        resp = negotiate(client)
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to check {SERVER_DS}: pool I/O is currently suspended"}

    def test_a_plain_failure_of_the_server_copy_read_is_still_the_500(self, client, zfs):
        zfs.exists.return_value = True
        zfs.list.return_value = [listed_snapshot("a", SERVER_DS)]
        zfs.answers.values["encryption"] = RuntimeError("cannot open")
        resp = negotiate(client, snapshots=["a"])
        assert resp.status_code == 500
        assert "Failed to list server snapshots" in resp.get_json()["error"]

    def test_an_untrusted_strict_listing_is_still_reported_by_the_plain_one(self, client, zfs):
        """Only the SECOND listing's timeout is a zfs_timeout; the first failing for another reason is the
        old fallback (`server_newer_unknown`), not a 503."""
        zfs.exists.return_value = True
        zfs.list.side_effect = lambda *a, **k: (
            (_ for _ in ()).throw(RuntimeError("a row libzfseasy cannot trust")) if k.get("strict")
            else [listed_snapshot("a", SERVER_DS)]
        )
        resp = negotiate(client, snapshots=["a"])
        assert resp.status_code == 200
        assert resp.get_json()["server_newer_unknown"] is True


# ---------------------------------------------------------------------------
# /resume_token
# ---------------------------------------------------------------------------


def token_exists_first(z, client, f):
    z.exists.side_effect = f.raiser


def token_ownership_properties(z, client, f):
    z.exists.return_value = True
    z.props.side_effect = f.raiser


def token_ownership_client_id(z, client, f):
    z.exists.return_value = True
    fail_property(z, PROP_CLIENT_ID, f)


def token_exists(z, client, f):
    z.exists.side_effect = nth(f, 2, True)


def token_listing(z, client, f):
    z.exists.return_value = True
    z.list.side_effect = f.raiser


@pytest.mark.usefixtures("zfs")
class TestResumeTokenTimeouts:
    @pytest.mark.parametrize("site", [
        pytest.param(token_exists_first, id="ownership-exists"),
        pytest.param(token_ownership_properties, id="ownership-read_zfsbackup_properties"),
        pytest.param(token_ownership_client_id, id="ownership-local-client-id"),
        pytest.param(token_exists, id="token-exists"),
        pytest.param(token_listing, id="token-listing"),
    ])
    def test_a_timeout_at_every_zfs_call_is_the_one_503(self, site, client, authed, zfs, fault, caplog):
        site(zfs, client, fault)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = client.get(TOKEN_URL)
        assert_zfs_timeout(resp)
        assert fault.raised == 1
        assert authed.gate.active == 0
        assert "resume_token" not in resp.get_json()
        records = timeout_records(caplog)
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "outcome of that command is unknown" not in records[0].getMessage()      # a read

    def test_a_plain_failure_is_still_the_500(self, client, zfs):
        zfs.exists.return_value = True
        zfs.list.side_effect = RuntimeError("cannot list")
        resp = client.get(TOKEN_URL)
        assert resp.status_code == 500 and resp.get_json() == {"error": "cannot list"}

    def test_a_good_answer_is_unchanged(self, client, zfs):
        zfs.exists.return_value = False
        resp = client.get(TOKEN_URL)
        assert resp.status_code == 200 and resp.get_json() == {"resume_token": None}


# ---------------------------------------------------------------------------
# /stream
# ---------------------------------------------------------------------------


def stream_ownership_exists(s, f):
    s.exists.side_effect = nth(f, 1)


def stream_ownership_properties(s, f):
    s.props.side_effect = lambda ds: (f.maybe(), {})[1]


def stream_ownership_client_id(s, f):
    fail_stream_property(s, PROP_CLIENT_ID, f)


def stream_config_exists(s, f):
    s.exists.side_effect = nth(f, 2)


def stream_config_read(s, f):
    fail_stream_property(s, PROP_CONFIG, f)


def stream_type_read(s, f):
    fail_stream_property(s, "type", f)


def stream_snapshot_listing(s, f):
    s.list.side_effect = lambda *a, **k: (
        f.maybe() if k.get("types") == "snapshot" else None, s._list(*a, **k))[1]


def stream_token_exists(s, f):
    s.exists.side_effect = nth(f, 3)


def stream_token_listing(s, f):
    s.list.side_effect = lambda *a, **k: (
        f.maybe() if k.get("types") != "snapshot" else None, s._list(*a, **k))[1]


def stream_abort(s, f):
    s.abort.side_effect = f.raiser


def stream_final_exists(s, f):
    s.exists.side_effect = nth(f, 4)


def fail_stream_property(s, prop, f):
    inner = s._get_value

    def get(ds, p, *args, **kwargs):
        if p == prop:
            f.maybe()
        return inner(ds, p, *args, **kwargs)

    s.get_value.side_effect = get


STREAM_SITES = [
    pytest.param(stream_ownership_exists, id="ownership-exists"),
    pytest.param(stream_ownership_properties, id="ownership-read_zfsbackup_properties"),
    pytest.param(stream_ownership_client_id, id="ownership-local-client-id"),
    pytest.param(stream_config_exists, id="config-exists"),
    pytest.param(stream_config_read, id="config-read"),
    pytest.param(stream_type_read, id="type-read"),
    pytest.param(stream_snapshot_listing, id="snapshot-listing"),
    pytest.param(stream_token_exists, id="token-exists"),
    pytest.param(stream_token_listing, id="token-listing"),
    pytest.param(stream_abort, id="abort"),
    pytest.param(stream_final_exists, id="exists-before-the-receive"),
]


class TestStreamTimeouts:
    @pytest.mark.parametrize("site", STREAM_SITES)
    def test_a_timeout_at_every_zfs_call_is_the_text_reply_and_nothing_is_received(
        self, site, stream, fault, caplog
    ):
        at_send = []
        stream.ws.send.side_effect = lambda message: at_send.append(stream.claimed)
        site(stream, fault)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            stream.run("fresh")
        assert fault.raised == 1, "the scenario did not time out at the call it names"
        assert stream.sent == [api.ZFS_TIMEOUT_ERROR]
        stream.receive.assert_not_called()
        stream.writer.write.assert_not_called()
        assert at_send == [set()], "the claim must be released BEFORE the reply"
        assert stream.claimed == set()
        assert stream.gate.active == 0
        assert [r.levelno for r in timeout_records(caplog)] == [logging.WARNING]
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    @pytest.mark.parametrize("site", STREAM_SITES)
    def test_the_next_stream_is_not_busy_and_succeeds(self, site, stream, fault):
        site(stream, fault)
        stream.run("fresh")
        assert stream.sent == [api.ZFS_TIMEOUT_ERROR]
        fault.on = False
        rearm(stream)
        stream.run("fresh")
        assert stream.sent[1:] == [{"status": "ok"}]
        stream.receive.assert_called_once()
        assert stream.claimed == set() and stream.gate.active == 0

    def test_the_reply_is_not_a_listing_refusal(self, stream, fault):
        stream_snapshot_listing(stream, fault)
        stream.run("fresh")
        (message,) = stream.sent
        assert message.get("reason") == "zfs_timeout" and message["retry"] is True
        assert "listing" not in json.dumps(message)

    def test_a_plain_listing_failure_is_still_the_listing_refusal(self, stream):
        stream.fail_snapshot_listing(RuntimeError("cannot list"))
        stream.run("fresh")
        (message,) = stream.sent
        assert message["reason"] == "listing" and "retry" not in message
        stream.receive.assert_not_called()

    def test_an_abort_that_timed_out_is_not_assumed_to_have_worked(self, stream, fault):
        stream_abort(stream, fault)
        stream.run("fresh")
        assert stream.sent == [api.ZFS_TIMEOUT_ERROR]
        stream.receive.assert_not_called()
        stream.abort.assert_called_once()
        token_lookups = lambda: [c for c in stream.list.call_args_list if c.kwargs.get("types") != "snapshot"]
        assert len(token_lookups()) == 1

        fault.on = False
        rearm(stream)
        stream.run("fresh")
        assert stream.sent[1:] == [{"status": "ok"}]
        assert len(token_lookups()) == 2             # looked at again ...
        assert stream.abort.call_count == 2          # ... and aborted again before the receive
        stream.receive.assert_called_once()

    def test_an_abort_that_timed_out_says_so_in_the_log(self, stream, fault, caplog):
        stream_abort(stream, fault)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            stream.run("fresh")
        (record,) = timeout_records(caplog)
        assert "the outcome of that command is unknown and is re-read on the next attempt" in record.getMessage()

    def test_a_read_that_timed_out_does_not_say_the_outcome_is_unknown(self, stream, fault, caplog):
        stream_snapshot_listing(stream, fault)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            stream.run("fresh")
        (record,) = timeout_records(caplog)
        assert "re-read on the next attempt" not in record.getMessage()

    def test_the_force_flag_exists_is_asked_under_the_budget_and_a_timeout_spawns_nothing(self, stream, fault):
        """The `-F` decision's `zfs.exists` was part of the receive call's argument list; it is its own call now,
        so that its timeout is caught before anything is built."""
        stream_final_exists(stream, fault)
        stream.run("fresh")
        stream.receive.assert_not_called()
        assert stream.sent == [api.ZFS_TIMEOUT_ERROR]
        assert stream.abort.call_count == 1           # the abort before it did run

    @pytest.mark.parametrize("cut_at, aborted", [(3, False), (4, True)])
    def test_a_session_cut_during_the_exists_calls_never_reaches_the_receive(self, stream, cut_at, aborted):
        """The last cut check is below the final `exists` and right before the spawn: a revoke that lands
        during the (slow) `exists` of the `-F` decision must not start `zfs receive`. (A cut during the token
        lookup is caught earlier, before the abort.)"""
        registry = stream.app.extensions["zfsbackup.sessions"]
        calls = itertools.count(1)

        def exists(ds):
            if next(calls) == cut_at:
                with registry._lock:
                    (session,) = list(registry._sessions)
                session.cut_reason = "its client certificate was revoked (revoked)"
            return True

        stream.exists.side_effect = exists
        stream.run("fresh")
        stream.receive.assert_not_called()
        assert (stream.abort.call_count == 1) is aborted
        assert stream.claimed == set() and stream.gate.active == 0
        assert stream.sent == []                                  # nobody to answer

    def test_a_stream_success_forgets_its_refusals_while_it_iterates_a_copy(self, stream):
        noted = _find_in_closures(stream.app, "_refused_noted")
        noted.update({(SERVER_DS, "base_mismatch"), (SERVER_DS, "type_mismatch"), ("pool/backups/client1/x", "r")})
        stream.run("fresh")
        assert stream.sent == [{"status": "ok"}]
        assert noted == {("pool/backups/client1/x", "r")}

    def test_clearing_the_refusals_is_safe_while_another_thread_adds_to_them(self, stream):
        """`_refused_noted` is shared by every stream of the worker; the success path iterates over a copy."""
        noted = _find_in_closures(stream.app, "_refused_noted")
        noted.update((f"pool/backups/client1/filler{i}", "r") for i in range(20000))
        stop = threading.Event()

        def churn():
            i = 0
            while not stop.is_set():
                key = (f"pool/backups/client1/churn{i % 50}", "r")
                noted.add(key)
                noted.discard(key)
                i += 1

        thread = threading.Thread(target=churn, daemon=True)
        interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-5)
        thread.start()
        try:
            for _ in range(60):
                rearm(stream)
                stream.run("fresh")
        finally:
            stop.set()
            thread.join(10)
            sys.setswitchinterval(interval)
        assert not thread.is_alive()
        assert [m for m in stream.sent if m != {"status": "ok"}] == []

    def test_the_receive_is_not_under_a_timeout(self, stream):
        seen = []

        def receive(*args, **kwargs):
            seen.append(_timeout_var.get())
            return stream.writer

        stream.receive.side_effect = receive
        stream.run("fresh")
        assert stream.sent == [{"status": "ok"}]
        assert seen == [None]

    def test_the_force_flag_is_still_what_decide_receive_says(self, stream):
        stream.run("fresh")
        assert stream.receive.call_args.kwargs["force"] is True       # an existing copy with no snapshots
        stream.exists.side_effect = [True, True, True, False]         # ... and the abort destroyed it
        rearm(stream)
        stream.run("fresh")
        assert stream.receive.call_args.kwargs["force"] is False

    def test_a_force_check_that_fails_for_another_reason_is_the_old_start_error(self, stream):
        calls = itertools.count(1)

        def exists(ds):
            if next(calls) == 4:
                raise RuntimeError("cannot open pool")
            return True

        stream.exists.side_effect = exists
        stream.run("fresh")
        (message,) = stream.sent
        assert message == {"error": "Failed to start zfs receive: cannot open pool"}
        stream.receive.assert_not_called()
        assert stream.claimed == set()

    def test_a_timed_out_stream_releases_its_own_claim_only(self, stream, fault):
        """It releases exactly once: a second release would drop another stream's claim."""
        stream_snapshot_listing(stream, fault)
        stream.run("fresh")
        api_claims = streams_in_progress(stream.app)
        api_claims.add("pool/backups/client1/other")          # somebody else's claim
        try:
            fault.on = False
            rearm(stream)
            stream.run("fresh")
            assert "pool/backups/client1/other" in api_claims
        finally:
            api_claims.discard("pool/backups/client1/other")


# ---------------------------------------------------------------------------
# The budget, with a fake clock
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("zfs")
class TestSharedBudget:
    def test_the_calls_of_one_request_share_the_budget(self, client, authed, zfs, clock, limits, caplog):
        slow(clock, 20, zfs.exists, True)
        slow(clock, 20, zfs.props, {})
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            resp = negotiate(client)
        assert_zfs_timeout(resp)
        assert zfs.exists.call_count == 2                      # t=0 (30 s left) and t=20 (10 s left)
        zfs.props.assert_not_called()                          # t=40: spent, never run
        zfs.get_value.assert_not_called()
        zfs.set.assert_not_called()
        assert limits == [30.0, 10.0]
        (record,) = timeout_records(caplog)
        assert "budget for this request is spent" in record.getMessage()
        assert streams_in_progress(authed.app) == set()

    def test_the_second_call_is_what_is_cut_when_the_first_takes_the_whole_budget(self, client, zfs, clock, limits):
        slow(clock, 31, zfs.exists, True)
        assert_zfs_timeout(negotiate(client))
        assert zfs.exists.call_count == 1
        zfs.props.assert_not_called()
        assert limits == [30.0]

    def test_a_third_call_that_still_fits_runs(self, client, zfs, clock, limits):
        slow(clock, 9, zfs.exists, False)
        slow(clock, 9, zfs.create)
        assert negotiate(client).status_code == 200            # exists, exists (ancestor), create: 27 s
        assert limits == [30.0, 21.0, 12.0]

    def test_each_request_has_its_own_budget(self, client, zfs, clock, limits):
        slow(clock, 20, zfs.exists, True)
        slow(clock, 20, zfs.props, {})
        assert_zfs_timeout(client.get(TOKEN_URL))
        assert_zfs_timeout(client.get(TOKEN_URL))
        assert limits == [30.0, 10.0, 30.0, 10.0]

    def test_the_budget_starts_at_the_requests_first_zfs_call(self, client, zfs, clock, limits, mocker):
        """Time spent before the first call (parsing, a busy worker) is not zfs's."""
        real = api.canonical_dataset_config
        mocker.patch(
            "zfsbackup.api.canonical_dataset_config",
            side_effect=lambda value: (clock.advance(500), real(value))[1],
        )
        assert negotiate(client).status_code == 200
        assert limits[0] == 30.0

    def test_resume_token_is_budgeted_too(self, client, zfs, clock, limits):
        slow(clock, 20, zfs.exists, True)
        slow(clock, 20, zfs.props, {})
        assert_zfs_timeout(client.get(TOKEN_URL))
        zfs.get_value.assert_not_called()
        zfs.list.assert_not_called()

    def test_the_checks_before_the_header_share_one_budget(self, stream, clock, limits):
        slow(clock, 20, stream.exists, True)
        slow(clock, 20, stream.props, {})
        stream.run("fresh")
        assert stream.sent == [api.ZFS_TIMEOUT_ERROR]
        assert limits == [30.0, 10.0]
        assert stream.reads == []                              # the third call (client id) was never made
        stream.receive.assert_not_called()
        assert stream.claimed == set()

    def test_the_stream_restarts_the_budget_after_the_header_wait(self, stream, clock, limits):
        """The wait for the first bytes is the peer's time. Without the restart the type read would find the
        budget long spent."""
        script = iter(stream_script(b"chunk"))

        def receive(*args, **kwargs):
            message = next(script)
            if message is HEADER:
                clock.advance(100)
            return message

        stream.ws.receive.side_effect = receive
        stream.run("fresh")
        assert stream.sent == [{"status": "ok"}]
        stream.receive.assert_called_once()
        assert len(limits) >= 9
        assert set(limits) == {30.0}
        assert ("type", None) in stream.reads

    def test_a_timeout_after_the_restart_is_the_text_reply_and_the_new_budget_is_shared_too(
        self, stream, clock, limits
    ):
        script = iter(stream_script(b"chunk"))

        def receive(*args, **kwargs):
            message = next(script)
            if message is HEADER:
                clock.advance(100)
            return message

        stream.ws.receive.side_effect = receive
        inner = stream._get_value
        stream.get_value.side_effect = lambda ds, p, *a, **k: (
            clock.advance(40) if p == "type" else None, inner(ds, p, *a, **k))[1]
        stream.run("fresh")
        assert stream.sent == [api.ZFS_TIMEOUT_ERROR]    # the type read used up the NEW budget
        assert limits[-1] == 30.0
        assert not [c for c in stream.list.call_args_list if c.kwargs.get("types") == "snapshot"]
        stream.receive.assert_not_called()
        assert stream.claimed == set()


# ---------------------------------------------------------------------------
# The log
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("zfs")
class TestTimeoutLogging:
    def test_one_warning_then_debug_for_a_repeating_timeout(self, client, zfs, fault, caplog):
        zfs.exists.side_effect = fault.raiser
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(4):
                assert_zfs_timeout(negotiate(client))
        assert [r.levelno for r in timeout_records(caplog)] == [
            logging.WARNING, logging.DEBUG, logging.DEBUG, logging.DEBUG]

    def test_the_warning_names_the_handler_client_dataset_server_dataset_and_cause(self, client, zfs, fault, caplog):
        zfs.exists.side_effect = fault.raiser
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            negotiate(client)
        (record,) = timeout_records(caplog)
        text = record.getMessage()
        assert text.startswith(f"negotiate from client {CLIENT_ID} for 'pool/data'")
        assert SERVER_DS in text and TIMEOUT_TEXT in text and "answering 503 retry" in text
        assert "the outcome of that command is unknown" in text           # negotiate may have written

    def test_a_success_re_arms_the_warning(self, client, zfs, fault, caplog):
        zfs.exists.side_effect = fault.raiser
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            negotiate(client)
            negotiate(client)
            fault.on = False
            zfs.exists.side_effect = None
            assert negotiate(client).status_code == 200
            fault.on = True
            zfs.exists.side_effect = fault.raiser
            negotiate(client)
            negotiate(client)
        assert [r.levelno for r in timeout_records(caplog)] == [
            logging.WARNING, logging.DEBUG, logging.WARNING, logging.DEBUG]

    def test_a_non_timeout_reply_of_the_handler_also_re_arms(self, client, zfs, fault, caplog):
        """The handler finished without a timeout (here a 409): the episode is over."""
        zfs.exists.side_effect = fault.raiser
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            negotiate(client)
            fault.on = False
            zfs.exists.side_effect = None
            zfs.exists.return_value = True
            zfs.answers.values[PROP_CLIENT_ID] = "someone-else"
            assert negotiate(client).status_code == 409
            zfs.answers.values[PROP_CLIENT_ID] = CLIENT_ID
            fault.on = True
            zfs.exists.side_effect = fault.raiser
            negotiate(client)
        assert [r.levelno for r in timeout_records(caplog)] == [logging.WARNING, logging.WARNING]

    def test_another_dataset_is_another_episode(self, client, zfs, fault, caplog):
        zfs.exists.side_effect = fault.raiser
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            negotiate(client, "pool/data")
            negotiate(client, "pool/other")
            negotiate(client, "pool/data")
            negotiate(client, "pool/other")
        records = timeout_records(caplog)
        assert [r.levelno for r in records] == [logging.WARNING, logging.WARNING, logging.DEBUG, logging.DEBUG]
        assert "pool/backups/client1/data" in records[0].getMessage()
        assert "pool/backups/client1/other" in records[1].getMessage()

    def test_another_client_of_the_same_dataset_name_is_another_server_dataset(self, authed, zfs, fault, caplog):
        zfs.exists.side_effect = fault.raiser
        other = authed.client(authed.cert_for("client2"))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            negotiate(authed.client(), "pool/data")
            other.post("/backup/client2/pool/data/negotiate", json={"snapshots": [], "config": GOOD_CONFIG})
        assert [r.levelno for r in timeout_records(caplog)] == [logging.WARNING, logging.WARNING]

    def test_each_handler_has_its_own_episode(self, client, zfs, fault, caplog):
        zfs.exists.side_effect = fault.raiser
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            negotiate(client)
            client.get(TOKEN_URL)
            negotiate(client)
            client.get(TOKEN_URL)
        records = timeout_records(caplog)
        assert [r.levelno for r in records] == [logging.WARNING, logging.WARNING, logging.DEBUG, logging.DEBUG]
        assert records[0].getMessage().startswith("negotiate from client")
        assert records[1].getMessage().startswith("resume_token from client")

    def test_the_stream_is_a_third_handler_and_its_success_re_arms_it(self, stream, fault, caplog):
        stream_snapshot_listing(stream, fault)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            stream.run("fresh")
            rearm(stream)
            stream.run("fresh")
            fault.on = False
            rearm(stream)
            stream.run("fresh")
            assert stream.sent[-1] == {"status": "ok"}
            fault.on = True
            rearm(stream)
            stream.run("fresh")
        records = timeout_records(caplog)
        assert [r.levelno for r in records] == [logging.WARNING, logging.DEBUG, logging.WARNING]
        assert records[0].getMessage().startswith(f"stream from client {CLIENT_ID} for 'pool/data'")

    def test_all_three_handlers_of_one_app_announce_once_each(self, stream, caplog):
        """negotiate and resume_token on the stream harness's app, with everything timing out."""
        def boom(*args, **kwargs):
            raise libzfseasy.ZFSTimeoutError(TIMEOUT_TEXT)

        stream.exists.side_effect = boom
        client = stream.authed.client()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(2):
                assert_zfs_timeout(negotiate(client))
                assert_zfs_timeout(client.get(TOKEN_URL))
                stream.run("fresh")
        levels = [(r.getMessage().split(" ")[0], r.levelno) for r in timeout_records(caplog)]
        assert sorted(levels[:3]) == [
            ("negotiate", logging.WARNING), ("resume_token", logging.WARNING), ("stream", logging.WARNING)]
        assert {lvl for _, lvl in levels[3:]} == {logging.DEBUG} and len(levels) == 6

    @pytest.mark.parametrize("size, left", [(1024, 1025), (1025, 1)])
    def test_the_remembered_episodes_are_bounded(self, client, authed, zfs, fault, size, left):
        noted = _find_in_closures(authed.app, "_zfs_timeout_noted")
        noted.update(("negotiate", f"pool/filler/{i}") for i in range(size))
        zfs.exists.side_effect = fault.raiser
        assert_zfs_timeout(negotiate(client))
        assert len(noted) == left
        assert ("negotiate", SERVER_DS) in noted

    def test_the_noted_set_belongs_to_the_app(self):
        first, second = Authed(_config()), Authed(_config())
        assert _find_in_closures(first.app, "_zfs_timeout_noted") is not _find_in_closures(
            second.app, "_zfs_timeout_noted")


# ---------------------------------------------------------------------------
# libzfseasy's registry of stuck children: fail fast
# ---------------------------------------------------------------------------


class FakeChild:
    """A child that cannot be reaped: `poll()` says it is running until `code` is set."""
    pid = 424242
    stdout = None
    stderr = None

    def __init__(self, code=None):
        self.code = code

    def poll(self):
        return self.code


def register_stuck(code=None):
    child = FakeChild(code)
    zfs_module._register_stuck(child, None, "zfs list")
    return child


def drive_stream(authed, ws=None):
    """The `/stream` handler of `authed`'s app, called directly with a mock websocket."""
    ws = ws if ws is not None else MagicMock()
    handler = authed.app.view_functions["backup_stream"].__wrapped__
    with authed.context(STREAM_PATH, query_string={"mode": "fresh"}):
        handler(ws, CLIENT_ID, "pool/data")
    return ws


class TestStuckChildFailsFast:
    """Nothing here patches a `zfsbackup.api.zfs.*` call: the real libzfseasy admission runs, `subprocess.Popen`
    is a mock that must never be reached."""

    @pytest.fixture
    def popen(self, mocker):
        return mocker.patch("subprocess.Popen")

    def test_negotiate_is_refused_at_once(self, client, authed, popen):
        register_stuck()
        assert_zfs_timeout(negotiate(client))
        popen.assert_not_called()
        assert streams_in_progress(authed.app) == set()

    def test_resume_token_is_refused_at_once(self, client, popen):
        register_stuck()
        assert_zfs_timeout(client.get(TOKEN_URL))
        popen.assert_not_called()

    def test_a_stream_is_refused_at_once_with_the_text_reply(self, authed, popen):
        register_stuck()
        ws = drive_stream(authed)
        assert [json.loads(c.args[0]) for c in ws.send.call_args_list] == [api.ZFS_TIMEOUT_ERROR]
        popen.assert_not_called()
        assert streams_in_progress(authed.app) == set()

    def test_enroll_keeps_its_own_503(self, popen):
        register_stuck()
        authed = Authed(_config(), client_id=None)
        resp = enroll(authed)
        assert resp.status_code == 503
        assert resp.get_json()["reason"] == "zfs" and resp.get_json()["retry"] is True
        popen.assert_not_called()
        assert authed.gateway.enroll_calls == []

    def test_the_refusal_names_the_stuck_child_in_the_log(self, client, popen, caplog):
        child = register_stuck()
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            negotiate(client)
        (record,) = timeout_records(caplog)
        assert f"pid {child.pid}" in record.getMessage() and "zfs is not answering" in record.getMessage()

    def test_an_entry_that_has_exited_is_swept_and_the_call_goes_through(self, client, mock_subprocess):
        register_stuck(code=0)                                   # the "hung" child has exited since
        mock_subprocess.setup_multi(
            ([], "cannot open 'pool/backups/client1/data': dataset does not exist\n", 1),
            ([], "cannot open 'pool/backups/client1/data': dataset does not exist\n", 1),
        )
        resp = client.get(TOKEN_URL)
        assert resp.status_code == 200 and resp.get_json() == {"resume_token": None}
        assert zfs_module._stuck == []

    def test_a_live_entry_does_not_refuse_calls_made_without_a_timeout(self, mock_subprocess):
        """The registry only fails TIMED calls fast; that is why the handlers' calls are all timed."""
        register_stuck()
        mock_subprocess.setup(stdout=[], stderr="cannot open 'pool/x': dataset does not exist\n", returncode=1)
        assert libzfseasy.exists(libzfseasy.types.Filesystem("pool/x")) is False
        with libzfseasy.timeout(5), pytest.raises(libzfseasy.ZFSTimeoutError, match="not started"):
            libzfseasy.exists(libzfseasy.types.Filesystem("pool/x"))

    def test_the_entry_is_not_dropped_by_a_refusal(self, client, popen):
        child = register_stuck()
        for _ in range(3):
            assert_zfs_timeout(negotiate(client))
        assert zfs_module._stuck and zfs_module._stuck[0].process is child
        child.code = 0                                         # the pool wakes up: the sweep drops it
        assert zfs_module._sweep_stuck() == [] and zfs_module._stuck == []
        popen.assert_not_called()


class Unkillable:
    """Real children through `subprocess.Popen`, with a switch that makes `poll()` say "running" and a timed
    `wait()` time out, whatever is really true (what an uninterruptible child on a hung pool looks like). The
    kill really lands; `close()` reaps everything."""

    def __init__(self, monkeypatch):
        self.procs = []
        self.on = False
        outer = self
        real = subprocess.Popen                  # the (guarded) class: subclassing keeps the guard

        class Recording(real):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                outer.procs.append(self)

            def poll(self):
                return None if outer.on else super().poll()

            def wait(self, timeout=None):
                if outer.on and timeout is not None:
                    raise subprocess.TimeoutExpired(self.args, timeout)
                return super().wait(timeout=timeout)

        monkeypatch.setattr(subprocess, "Popen", Recording)

    def close(self):
        self.on = False
        for proc in self.procs:
            with contextlib.suppress(OSError):
                proc.kill()
            with contextlib.suppress(Exception):
                proc.wait(timeout=10)
            for pipe in (proc.stdout, proc.stderr):
                with contextlib.suppress(Exception):
                    pipe.close()


@pytest.mark.subprocess
class TestHungPoolEndToEnd:
    """A real child that ignores SIGTERM stands for a `zfs` on a hung pool."""

    #: The first exec of a freshly written script costs 0.15-0.5 s on macOS, with spikes, so the
    #: hang phase gets a budget well above that (the fail-fast assertions below do no exec and
    #: do not depend on it) and the `zfs-ok` phase a generous one.
    HANG_BUDGET = 4.0
    OK_BUDGET = 10.0
    #: The child writes `ready` only after it ignores TERM; the test checks it exists once the
    #: budget expired, so "ignores TERM" is a verified premise, not an assumption.
    HANG = (
        "#!{python}\n"
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "open({ready!r}, 'w').close()\n"
        "time.sleep(300)\n"
    )
    MISSING = (
        "#!/bin/sh\n"
        "echo \"cannot open 'pool/backups/client1/data': dataset does not exist\" >&2\n"
        "exit 1\n"
    )

    @staticmethod
    def script(path, text):
        path.write_text(text)
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
        return str(path)

    def hang_script(self, tmp_path):
        ready = tmp_path / "hang-ready"
        return self.script(
            tmp_path / "zfs-hang", self.HANG.format(python=sys.executable, ready=str(ready))
        ), ready

    @staticmethod
    def assert_child_ignored_term(ready):
        assert ready.exists(), (
            "the stub never reached signal(SIGTERM, SIG_IGN) within the budget: the test's "
            "premise (a child that ignores TERM) did not hold, raise HANG_BUDGET"
        )

    def test_after_one_timeout_every_request_fails_fast_until_the_child_exits(
        self, client, authed, tmp_path, monkeypatch, mocker
    ):
        mocker.patch("zfsbackup.api.ZFS_REQUEST_BUDGET", self.HANG_BUDGET)
        monkeypatch.setattr(zfs_module, "_EXPIRY_WAIT", 0.2)
        kids = Unkillable(monkeypatch)
        hang, ready = self.hang_script(tmp_path)
        monkeypatch.setenv("ZFS_CMD", hang)
        try:
            kids.on = True
            started = time.monotonic()
            assert_zfs_timeout(client.get(TOKEN_URL))                 # the first request pays the budget
            assert time.monotonic() - started < 20
            self.assert_child_ignored_term(ready)
            assert len(kids.procs) == 1 and len(zfs_module._stuck) == 1
            assert streams_in_progress(authed.app) == set()

            for _ in range(3):                                        # then nothing is started any more
                before = time.monotonic()
                assert_zfs_timeout(negotiate(client))
                assert_zfs_timeout(client.get(TOKEN_URL))
                assert drive_stream(authed).send.call_count == 1
                assert time.monotonic() - before < 0.5
            assert len(kids.procs) == 1

            kids.on = False                                           # the pool wakes up: the child is gone
            deadline = time.monotonic() + 10
            while kids.procs[0].poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            assert kids.procs[0].poll() is not None
            monkeypatch.setenv("ZFS_CMD", self.script(tmp_path / "zfs-ok", self.MISSING))
            mocker.patch("zfsbackup.api.ZFS_REQUEST_BUDGET", self.OK_BUDGET)  # a slow first exec is no hang
            resp = client.get(TOKEN_URL)
            assert resp.status_code == 200 and resp.get_json() == {"resume_token": None}
            assert zfs_module._stuck == []
        finally:
            kids.close()

    def test_a_child_that_ignores_term_but_dies_of_kill_leaves_no_registry_entry(
        self, client, tmp_path, monkeypatch, mocker
    ):
        """The normal timeout: TERM ignored, KILL lands. The request is a 503 and the NEXT one is not refused."""
        mocker.patch("zfsbackup.api.ZFS_REQUEST_BUDGET", self.HANG_BUDGET)
        monkeypatch.setattr(zfs_module, "_EXPIRY_WAIT", 0.3)
        hang, ready = self.hang_script(tmp_path)
        monkeypatch.setenv("ZFS_CMD", hang)
        assert_zfs_timeout(client.get(TOKEN_URL))
        self.assert_child_ignored_term(ready)
        assert zfs_module._stuck == []
        monkeypatch.setenv("ZFS_CMD", self.script(tmp_path / "zfs-ok", self.MISSING))
        mocker.patch("zfsbackup.api.ZFS_REQUEST_BUDGET", self.OK_BUDGET)  # a slow first exec is no hang
        assert client.get(TOKEN_URL).status_code == 200


# ---------------------------------------------------------------------------
# /enroll
# ---------------------------------------------------------------------------


def enroll(authed, client_id="host-z"):
    token = str(identity.make_token(authed.server_fp)[0])
    body = enroll_body(authed.server_fp, client_id, make_client_cert(), token, label="lbl")
    return authed.client(None).post("/enroll", json=body)


class TestEnrollTimeout:
    def test_a_timed_out_exists_is_the_existing_retryable_503_not_the_new_one(self, mocker, fault, caplog):
        exists = mocker.patch("zfsbackup.api.zfs.exists", side_effect=fault.raiser)
        authed = Authed(_config(), client_id=None)
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp = enroll(authed)
        assert fault.raised == 1
        assert resp.status_code == 503
        assert resp.get_json() == {
            "error": "the server cannot check its datasets right now; retry later",
            "reason": "zfs", "retry": True,
        }
        assert resp.get_json() != api.ZFS_TIMEOUT_ERROR
        assert authed.gateway.enroll_calls == []                  # nothing was enrolled on a guess
        assert exists.call_count == 1
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and TIMEOUT_TEXT in errors[0].getMessage()
        assert "enrollment from" in errors[0].getMessage()
        assert TIMEOUT_TEXT not in resp.get_data(as_text=True)

    def test_the_exists_runs_under_the_request_budget(self, mocker, limits):
        seen = []
        mocker.patch(
            "zfsbackup.api.zfs.exists", side_effect=lambda ds: (seen.append(_timeout_var.get()), False)[1],
        )
        authed = Authed(_config(), client_id=None)
        assert enroll(authed).status_code == 200
        assert seen == [pytest.approx(api.ZFS_REQUEST_BUDGET, abs=1.0)]
        assert limits == [pytest.approx(api.ZFS_REQUEST_BUDGET, abs=1.0)]

    def test_a_good_exists_still_enrolls(self, mocker):
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        authed = Authed(_config(), client_id=None)
        assert enroll(authed).status_code == 200
        assert authed.gateway.enroll_calls[0]["dataset_exists"] is True

    def test_a_budget_spent_before_the_exists_is_the_same_503(self, mocker):
        """A budget that is spent when the call is made is the same retryable 503 (no zfs call)."""
        exists = mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.ZFS_REQUEST_BUDGET", 0.0)
        authed = Authed(_config(), client_id=None)
        resp = enroll(authed)
        assert resp.status_code == 503 and resp.get_json()["reason"] == "zfs"
        exists.assert_not_called()
        assert authed.gateway.enroll_calls == []

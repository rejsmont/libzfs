"""Unit tests for `RemoteBackupManager`: negotiation, the resume protocol and the transfer.

The pinned-mTLS transport is faked here. `_negotiate`, `_fetch_resume_token`, `_try_resume`,
`_start_fresh`, `_push_step` and `_transfer` take a `remote._Transport` (`make_transport`: a plain
`requests.Session` that the `requests_mock` fixture answers, and a stand-in SSL context), and the
websocket's TLS connect is `fake_connect_pinned`. The TLS behaviour itself -- the checks that run before any send, the error
mapping, the identity reload and real handshakes -- is `test_zfsbackup_remote_tls.py`.
"""

import json
import logging
import re
import types
import pytest
from unittest.mock import DEFAULT, MagicMock, patch

from _remote_support import (  # noqa: F401  (fixtures)
    HTTPS_BASE, SERVER_FP, client_tls_dir, fake_connect_pinned, make_transport, snapshot_presence,
    whoami_json, _client_tls_template,
)
from zfsbackup.config import (
    BackupConfig, DatasetConfig, RemoteDatasetConfig, Destination, RetentionRule, Duration,
)
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from _snapshot_support import ABSENT, listed_info
from zfsbackup.remote import NegotiationFailed, RemoteBackupManager, _SendStream

BASE = HTTPS_BASE


def tr(base=BASE):
    """The fake transport for `base` (see `_remote_support.make_transport`)."""
    return make_transport(base)


def _make_config(**kwargs) -> BackupConfig:
    defaults = dict(
        datasets=[DatasetConfig(
            name='pool/data',
            remote=[RemoteDatasetConfig(destination='offsite')],
        )],
        destinations={'offsite': Destination(url=BASE, server_fingerprint=SERVER_FP)},
    )
    defaults.update(kwargs)
    return BackupConfig(**defaults)


def _rbm(config, manager, tls_dir=None, client_id='client1'):
    return RemoteBackupManager(config, manager, client_id=client_id, tls_dir=tls_dir)


def _finite_stream():
    """A `zfs send` stand-in that ends at once (never an endless MagicMock read)."""
    stream = MagicMock()
    stream.read.return_value = b''
    return stream


@pytest.fixture(autouse=True)
def _tls_connect_is_faked(fake_connect_pinned):
    """No test here opens a socket: the websocket's pinned TLS connect is a recorder."""
    return fake_connect_pinned


@pytest.fixture(autouse=True)
def sleeps(mocker):
    """`time.sleep` as `zfsbackup.remote` sees it: nothing really waits for a connect retry
    (~7 s in all). The mock records the slices."""
    return mocker.patch('zfsbackup.remote.time.sleep')


@pytest.fixture(autouse=True)
def _snapshots_are_present(snapshot_presence):
    """The client checks (`zfs get guid`) that a snapshot it has just sent still exists before it
    releases the pin: every listed snapshot does, with no `zfs` run. Tests of the destroyed or
    unknowable cases use the `snapshot_presence` fixture itself."""
    return snapshot_presence


@pytest.fixture(autouse=True)
def _no_local_zfs_property_writes(mocker):
    """The anchor / in-flight pin properties are `zfs get|set|inherit` calls on the LOCAL dataset:
    harmless defaults here, so a test that does not care never reaches even the stub `zfs`.
    Tests that assert on them patch the instance (which wins over these class patches)."""
    mocker.patch.object(DatasetManager, 'get_anchor', return_value=None)
    mocker.patch.object(DatasetManager, 'set_anchor', return_value=True)
    mocker.patch.object(DatasetManager, 'get_inflight', return_value=None)
    mocker.patch.object(DatasetManager, 'set_inflight', return_value=True)
    mocker.patch.object(DatasetManager, 'clear_inflight', return_value=True)


class TestRemoteBackupManagerWhoami:
    """`GET /whoami` replaced `POST /backup/register`: who the server takes this client for
    (the checks around it are in test_zfsbackup_remote_tls.py)."""

    def _rbm(self):
        config = _make_config()
        return _rbm(config, DatasetManager(config))

    def test_whoami_success(self, requests_mock):
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        assert self._rbm()._whoami('offsite', tr()) is True

    def test_whoami_is_one_get_with_the_http_timeout(self, requests_mock):
        from zfsbackup.remote import HTTP_TIMEOUT
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        self._rbm()._whoami('offsite', tr())
        assert requests_mock.call_count == 1
        assert requests_mock.last_request.method == 'GET'
        assert requests_mock.last_request.timeout == HTTP_TIMEOUT

    def test_whoami_failure(self, requests_mock):
        requests_mock.get(f'{BASE}/whoami', status_code=500)
        assert self._rbm()._whoami('offsite', tr()) is False

    def test_whoami_connection_error(self, requests_mock):
        import requests
        requests_mock.get(f'{BASE}/whoami', exc=requests.ConnectionError)
        assert self._rbm()._whoami('offsite', tr()) is False

    def test_whoami_for_a_different_client_id_is_refused(self, requests_mock):
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('somebody-else'))
        assert self._rbm()._whoami('offsite', tr()) is False

    def test_there_is_no_register_any_more(self):
        assert not hasattr(RemoteBackupManager, '_register')


class TestRemoteBackupManagerNegotiate:
    def _make_rbm(self, tmp_path):
        config = _make_config()
        manager = DatasetManager(config)
        return _rbm(config, manager), manager.datasets[0]

    def test_negotiate_returns_common_snapshot(self, tmp_path, requests_mock):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.post(
            f'https://backup.example.com/backup/client1/{dsi.name}/negotiate',
            json={'common_snapshot': 'autosnap_20240101120000', 'server_dataset': 'pool/b/c1'},
        )
        snaps = [MagicMock(name='autosnap_20240101120000')]
        snaps[0].name = 'autosnap_20240101120000'
        result = rbm._negotiate(tr(), 'client1', dsi, snaps, 'offsite')
        assert result == 'autosnap_20240101120000'

    def test_negotiate_returns_none_when_no_common(self, tmp_path, requests_mock):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.post(
            f'https://backup.example.com/backup/client1/{dsi.name}/negotiate',
            json={'common_snapshot': None, 'server_dataset': 'pool/b/c1'},
        )
        result = rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')
        assert result is None

    def test_negotiate_returns_none_on_error(self, tmp_path, requests_mock):
        import requests
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.post(
            f'https://backup.example.com/backup/client1/{dsi.name}/negotiate',
            exc=requests.ConnectionError,
        )
        # A failed negotiation is NOT "no common snapshot": it raises, so the
        # caller can never fall through to a needless full send.
        with pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is False

    def test_negotiate_passes_destination_through_to_property(self, tmp_path, requests_mock):
        """The whole point of the 5th `destination` argument: `_negotiate` must
        call `to_property(destination)` with the destination it is negotiating
        for, not the dataset-level rules unconditionally (item 3b)."""
        config = _make_config(
            datasets=[DatasetConfig(
                name='pool/data',
                retention_rules=[RetentionRule(Duration('1h'), Duration('1d'))],
                remote=[RemoteDatasetConfig(
                    destination='offsite',
                    retention_rules=[RetentionRule(Duration('1d'), Duration('5y'))],
                )],
            )],
        )
        manager = DatasetManager(config)
        rbm = _rbm(config, manager)
        dsi = manager.datasets[0]

        requests_mock.post(
            f'https://backup.example.com/backup/client1/{dsi.name}/negotiate',
            json={'common_snapshot': None, 'server_dataset': 'pool/b/c1'},
        )
        rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')

        sent_config = requests_mock.last_request.json()['config']
        # The destination declared its own retention, so the negotiated config
        # must carry THAT ruleset -- not the dataset-level one.
        assert sent_config == dsi.config.to_property('offsite')
        assert sent_config != dsi.config.to_property()


# ---------------------------------------------------------------------------
# Resume protocol (client side)
#
# `_fetch_resume_token` -> skip / none / token; `_resume_target` accepts a
# token only when it is provably ours (GUID-matched, flags within the send
# mode's bounds); `_try_resume` returns step 0 (`skip`, `fresh` or a started
# `resume` stream) and `_start_fresh` starts a chain step; `_transfer` then
# drives the chain over the websocket (`/stream?mode=fresh|resume`): the
# in-flight pin, the anchor, one clear after the last step.
# ---------------------------------------------------------------------------

from datetime import datetime  # noqa: E402

import requests as _requests  # noqa: E402

from libzfseasy.types import Filesystem, Snapshot as ZFSSnapshot  # noqa: E402
from libzfseasy.zfs import ResumeToken, ResumeTokenError  # noqa: E402
from zfsbackup.backup_manager import SnapshotInfo  # noqa: E402

OLD = 'autosnap_20240101120000'
MID = 'autosnap_20240102120000'
NEW = 'autosnap_20240103120000'
GUIDS = {111: OLD, 222: MID, 333: NEW}

# Real tokens from a ZFS 2.4.1 VM (`zfs receive -s` interrupted at 8 MB); see
# test_commands.py::TestResumeInfo for their decoded contents.
TOKEN_FULL = (
    '1-a292306e5-b8-789c636064000310a500c4ec50360710e72765a52697303030419460caa7a515a7968064cae0f26c48f2499525a9c52'
    '0f1d00aacfa4bf2d34b33531818f238834cbe7b33276a20c97382e5f312735319188a4a0af48b53f352f4131d8a0da16e0100ed3915db'
)
TOKEN_INCR = (
    '1-e06a8f740-e0-789c636064000310a501c49c50360710a715e5e7a69766a63040c121db7a11f66796fe0a40363b92bafca4acd4e4120'
    '60666a83a0cf9b4b4e2d412904c195c9e0d493ea9b224b518c40eaec0aabf241fe20acdab06df0f7931276a20c97382e5f312735319188'
    'a4a0af48b53f352f4931c8a8da06e0100e3ab1d54'
)
# What `zfs send -nv -t <token>` printed for them (stdout lines).
FULL_CONTENTS = [
    'resume token contents:', 'nvlist version: 0', '\tobject = 0x2', '\toffset = 0x760000', '\tbytes = 0x785520',
    '\ttoguid = 0x61034bf73452096e', '\ttoname = rtp/send/a@s1',
    'full send of rtp/send/a@s1 estimated size is 21.2M', 'total estimated size is 21.2M',
]
INCR_CONTENTS = [
    'resume token contents:', 'nvlist version: 0', '\tfromguid = 0x4f39e607147f3dc2', '\tobject = 0x3',
    '\toffset = 0x760000', '\tbytes = 0x785308', '\ttoguid = 0x61034ac2f730d529', '\ttoname = rtp/send/b@s2',
    'send from rtp/send/b@s1 to rtp/send/b@s2 estimated size is 21.2M', 'total estimated size is 21.2M',
]


_GUID_OF = {name: guid for guid, name in GUIDS.items()}


def sinfo(name, guid=None, **props):
    """A managed snapshot as the manager's one listing delivers it: `createtxg` (ordered like the
    name unless `txg=` says otherwise) and `guid` (`GUIDS` for OLD/MID/NEW; `guid=ABSENT` omits it)."""
    if guid is None:
        guid = _GUID_OF.get(name, 'auto')
    if guid == 'auto':
        return listed_info(name, 'autosnap', **props)
    return listed_info(name, 'autosnap', guid=guid, **props)


def messages(caplog, text, level=None):
    return [r for r in caplog.records
            if text in r.getMessage() and (level is None or r.levelname == level)]


class Harness:
    """A `RemoteBackupManager` whose edges are mocked: the resume-token GET
    (via requests_mock), the token decoder, `zfs send` and the property writes. `order` records
    the interleaving. Local snapshots carry their `guid` and `createtxg` as the manager's one
    listing delivers them (S-1): there is no separate GUID listing to mock, and `zfs_list`
    stands for any other `zfs list` the resume path might (wrongly) run."""

    def __init__(self, tmp_path, requests_mock, mocker, decode=True, tls_dir=None):
        self.rm = requests_mock
        self.mocker = mocker
        config = _make_config()
        self.manager = DatasetManager(config)
        self.rbm = _rbm(config, self.manager, tls_dir)
        self.dsi = self.manager.datasets[0]
        # the fake transport the unit tests hand to the transport-level methods; `backup_dataset`
        # builds its own (real, pinned) one from `tls_dir`'s identity and answers /whoami below
        self.transport = tr()
        self.rm.get(f'{BASE}/whoami', json=whoami_json('client1'))
        self.snaps = [sinfo(NEW), sinfo(MID), sinfo(OLD)]
        self.latest = self.snaps[0]
        self.order = []
        self.url = f'{BASE}/backup/client1/{self.dsi.name}/resume_token'
        self.no_token()

        self.streams = []
        #: Everything the chain does, in order: ('pin', from, to), 'send' / 'send-t' (a `zfs send`
        #: spawned), ('anchor', name), 'clear' (pin cleared), ('stop?', n) (a `should_stop` poll,
        #: see `stopper`). `order` is the subset `('pin', ...)` / 'send' / 'send-t' that the
        #: older tests assert on.
        self.events = []

        def make_stream(label):
            def start(*args, **kwargs):
                self.order.append(label)
                self.events.append(label)
                stream = _finite_stream()
                stream.read.side_effect = [b'chunk', b'']          # one chunk, then the end
                self.streams.append(stream)
                return stream
            return start

        self.send_snapshot = mocker.patch('zfsbackup.remote.zfs.send.snapshot', side_effect=make_stream('send'))
        self.send_resume = mocker.patch('zfsbackup.remote.zfs.send.resume', side_effect=make_stream('send-t'))
        # `decode=False` leaves the real `resume_info` in place.
        if decode:
            self.resume_info = mocker.patch(
                'zfsbackup.remote.zfs.send.resume_info',
                return_value=ResumeToken(toname=f'pool/data@{MID}', toguid=222, fromguid=111, flags=frozenset()),
            )
        self.zfs_list = mocker.patch('zfsbackup.remote.zfs.list', return_value=[])
        lm = self.manager

        def pin(dsi, dest, from_name, to_name):
            self.order.append(('pin', from_name, to_name))
            self.events.append(('pin', from_name, to_name))
            return True

        def anchored(dsi, dest, name):
            self.events.append(('anchor', name))
            return DEFAULT                                          # the mock's return_value

        def cleared(dsi, dest):
            self.events.append('clear')
            return DEFAULT

        self.set_inflight = mocker.patch.object(lm, 'set_inflight', side_effect=pin)
        self.clear_inflight = mocker.patch.object(lm, 'clear_inflight', side_effect=cleared, return_value=True)
        self.set_anchor = mocker.patch.object(lm, 'set_anchor', side_effect=anchored, return_value=True)
        self.clear_anchor = mocker.patch.object(lm, 'clear_anchor', return_value=None)
        self.get_anchor = mocker.patch.object(lm, 'get_anchor', return_value=None)
        self.get_inflight = mocker.patch.object(lm, 'get_inflight', return_value=None)

    def stopper(self, *script, default=False):
        """A `should_stop` that answers `script` in order, then `default`, and records every
        poll as ('stop?', n) in `events`."""
        answers = iter(script)
        count = [0]

        def should_stop():
            count[0] += 1
            self.events.append(('stop?', count[0]))
            return next(answers, default)
        should_stop.polls = lambda: count[0]
        return should_stop

    # -- the server's answer to GET .../resume_token --------------------------
    def no_token(self):
        self.rm.get(self.url, json={'resume_token': None})

    def token(self, token=TOKEN_FULL):
        self.rm.get(self.url, json={'resume_token': token})

    NOFIELD = object()

    def server_says(self, common=None, copy=NOFIELD, server_dataset='pool/b/client1/data', **response):
        """The server's answer to POST .../negotiate (for `Chain(negotiate=REAL)`): the common
        snapshot, and `server_copy` unless `copy` is left out (an older server)."""
        body = {'common_snapshot': common, 'server_dataset': server_dataset}
        if copy is not self.NOFIELD:
            body['server_copy'] = copy
        self.rm.post(re.compile(r'.*/negotiate$'), **(response or {'json': body}))

    def token_gets(self):
        """The resume-token GETs so far (`/whoami` is also a GET)."""
        return [r for r in self.rm.request_history
                if r.method == 'GET' and r.path.endswith('/resume_token')]

    def decodes_to(self, **fields):
        fields.setdefault('flags', frozenset())
        self.resume_info.return_value = ResumeToken(**fields)

    # -- act --------------------------------------------------------------------
    def resume(self, snaps=None, mode='plain', should_stop=None):
        """Step 0 on its own: `_try_resume` -> ('skip' | 'fresh' | 'resume', stream)."""
        return self.rbm._try_resume(
            self.transport, 'client1', self.dsi, 'offsite',
            self.snaps if snaps is None else snaps, mode, should_stop,
        )

    def start(self, common=None, snaps=None, should_stop=None, mode='plain'):
        """What `_transfer` does to start its first stream: step 0 (`_try_resume`), and when
        that falls through the first fresh step, `latest` after `common` (`_start_fresh`; the
        chain planner is covered by TestChainSteps). -> the `_SendStream`, or None when the
        cycle is skipped (step 0 said so, or `_start_fresh` could not start `zfs send`)."""
        status, stream = self.resume(snaps, mode, should_stop)
        if status == 'skip':
            return None
        if stream is not None:
            return stream
        return self.rbm._start_fresh(self.dsi, 'offsite', common, self.latest, mode)

    def nothing_started(self):
        self.send_snapshot.assert_not_called()
        self.send_resume.assert_not_called()
        self.set_inflight.assert_not_called()


@pytest.fixture
def make_h(tmp_path, requests_mock, mocker, client_tls_dir):
    return lambda **kw: Harness(tmp_path, requests_mock, mocker, tls_dir=client_tls_dir, **kw)


@pytest.fixture
def h(make_h):
    return make_h()


class TestFetchResumeToken:
    """`_fetch_resume_token` -> ('skip'|'none'|'token', token)."""

    def fetch(self, h):
        return h.rbm._fetch_resume_token(h.transport, 'client1', h.dsi, 'offsite')

    def test_a_token_is_returned(self, h):
        h.token('1-abc')
        assert self.fetch(h) == ('token', '1-abc')

    @pytest.mark.parametrize('body', [{'resume_token': None}, {'resume_token': ''}, {}, {'resume_token': 0}])
    def test_no_token_is_none(self, h, body):
        h.rm.get(h.url, json=body)
        assert self.fetch(h) == ('none', None)

    def test_the_retryable_503_is_a_quiet_skip_at_info(self, h, caplog):
        h.rm.get(h.url, status_code=503, json={'error': 'server is reloading; retry later', 'retry': True})
        with caplog.at_level(logging.DEBUG):
            assert self.fetch(h) == ('skip', None)
        assert [r.levelname for r in messages(caplog, 'is reloading; skipping pool/data this cycle')] == ['INFO']
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    @pytest.mark.parametrize('status, body', [
        (503, {'error': 'remote_backup disabled'}),     # a 503 without `retry` is NOT the reload refusal
        (500, {'error': 'boom'}),
        (404, {}),
        (400, {'error': 'bad'}),
        (502, {}),
    ])
    def test_any_other_http_error_is_a_warning_skip(self, h, caplog, status, body):
        h.rm.get(h.url, status_code=status, json=body)
        with caplog.at_level(logging.DEBUG):
            assert self.fetch(h) == ('skip', None)
        (rec,) = messages(caplog, f'HTTP {status}')
        assert rec.levelname == 'WARNING'
        assert 'skipping this cycle' in rec.getMessage() and 'pool/data' in rec.getMessage()
        assert 'is reloading' not in caplog.text

    @pytest.mark.parametrize('status, body, text', [
        (401, {}, 'did not receive this client\'s certificate'),
        (403, {'error': 'client not trusted'}, 'does not trust this client'),
        (403, {'error': 'client_id mismatch', 'bound': 'other'}, 'binds this client\'s certificate'),
        (403, {}, 'refused this client (HTTP 403)'),
    ], ids=['401', '403-not-trusted', '403-mismatch', '403-other'])
    def test_an_auth_refusal_is_a_skip_logged_once_at_error_not_a_resume_warning(
        self, h, caplog, status, body, text
    ):
        """401/403 are destination-level refusals (the server will not talk to this client): an
        ERROR once, DEBUG after, and the destination is skipped for the rest of the cycle."""
        h.rm.get(h.url, status_code=status, json=body)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert self.fetch(h) == ('skip', None)
            assert self.fetch(h) == ('skip', None)
        own = [r for r in caplog.records if r.name == 'zfsbackup.remote']
        assert [r.levelname for r in own] == ['ERROR', 'DEBUG']
        assert text in own[0].getMessage() and 'destination offsite' in own[0].getMessage()
        assert not [r for r in own if r.levelname == 'WARNING']
        assert h.rbm._checked['offsite'][1] is False                 # skipped for the cycle

    @pytest.mark.parametrize('exc', [
        _requests.ConnectionError('refused'), _requests.Timeout('slow'), _requests.exceptions.ChunkedEncodingError('x'),
    ])
    def test_a_network_error_is_a_warning_skip(self, h, caplog, exc):
        h.rm.get(h.url, exc=exc)
        with caplog.at_level(logging.DEBUG):
            assert self.fetch(h) == ('skip', None)
        (rec,) = messages(caplog, 'cannot ask')
        assert rec.levelname == 'WARNING' and 'skipping this cycle' in rec.getMessage()

    def test_a_body_that_is_not_json_is_a_warning_skip(self, h, caplog):
        h.rm.get(h.url, text='<html>gateway</html>')
        with caplog.at_level(logging.DEBUG):
            assert self.fetch(h) == ('skip', None)
        assert [r.levelname for r in messages(caplog, 'cannot ask')] == ['WARNING']

    @pytest.mark.parametrize('body', [['resume_token'], 'token', 7])
    def test_a_json_body_that_is_not_an_object_is_a_warning_skip(self, h, caplog, body):
        h.rm.get(h.url, json=body)
        with caplog.at_level(logging.DEBUG):
            assert self.fetch(h) == ('skip', None)
        (rec,) = messages(caplog, 'unexpected reply')
        assert rec.levelname == 'WARNING'

    def test_an_error_body_on_a_200_is_a_warning_skip_naming_the_error(self, h, caplog):
        h.rm.get(h.url, json={'error': 'zfs get failed: pool busy', 'resume_token': 'ignored'})
        with caplog.at_level(logging.DEBUG):
            assert self.fetch(h) == ('skip', None)
        (rec,) = messages(caplog, 'pool busy')
        assert rec.levelname == 'WARNING'

    def test_exactly_one_get_with_a_10_second_timeout(self, h):
        h.token('1-abc')
        self.fetch(h)
        assert h.rm.call_count == 1
        assert h.rm.last_request.method == 'GET'
        assert h.rm.last_request.timeout == 10


class TestBusyServer:
    """`{"error": "busy", "retry": true}`: another stream of this dataset is running on the
    server. Retryable like the reload refusal: INFO, never an ERROR, never a fresh fallback."""

    BUSY = {'error': 'busy', 'retry': True}

    def test_a_busy_resume_token_reply_is_a_quiet_skip(self, h, caplog):
        h.rm.get(h.url, status_code=503, json=self.BUSY)
        with caplog.at_level(logging.DEBUG):
            assert h.start() is None
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert [r.levelname for r in messages(caplog, 'skipping pool/data this cycle')] == ['INFO']
        h.nothing_started()

    def test_a_busy_negotiate_is_a_reloading_style_failure(self, h):
        h.rm.post(re.compile(r'.*/negotiate$'), status_code=503, json=self.BUSY)
        with pytest.raises(NegotiationFailed) as exc:
            h.rbm._negotiate(h.transport, 'client1', h.dsi, [], 'offsite')
        assert exc.value.reloading is True

    def test_a_busy_final_ack_is_info_not_error(self, h, mocker, caplog):
        chain = Chain(h, mocker, sessions=1)
        chain.sessions[0].recv.return_value = json.dumps(self.BUSY)
        with caplog.at_level(logging.DEBUG):
            assert chain.run() is False
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert [r.levelname for r in messages(caplog, f'server {BASE} is busy with another stream of pool/data')] == ['INFO']
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_not_called()
        assert chain.ws_cls.call_count == 1              # and no fresh retry in the same cycle

    def test_a_busy_refusal_left_behind_a_failed_send_is_info_not_error(self, h, mocker, caplog):
        chain = Chain(h, mocker, sessions=1)
        chain.sessions[0].send_binary.side_effect = BrokenPipeError('broken pipe')
        chain.sessions[0].recv.return_value = json.dumps(self.BUSY)
        with caplog.at_level(logging.DEBUG):
            assert chain.run() is False
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_an_error_without_retry_is_still_an_error(self, h, mocker, caplog):
        chain = Chain(h, mocker, sessions=1)
        chain.sessions[0].recv.return_value = json.dumps({'error': 'busy'})
        with caplog.at_level(logging.DEBUG):
            assert chain.run() is False
        assert [r.levelname for r in messages(caplog, 'Transfer failed for pool/data: busy')] == ['ERROR']


class TestNoUsableAnswerSkipsTheCycle:
    """A skip must never fall through to a fresh send: that would make the
    server discard the partial progress it is holding."""

    @pytest.mark.parametrize('answer', [
        dict(status_code=503, json={'error': 'server is reloading; retry later', 'retry': True}),
        dict(status_code=500, json={'error': 'boom'}),
        dict(status_code=404),
        dict(exc=_requests.ConnectionError('refused')),
        dict(text='not json'),
        dict(json={'error': 'x'}),
        dict(json=['a']),
    ], ids=['reloading-503', '500', '404', 'network', 'bad-json', 'error-body', 'list-body'])
    def test_get_resume_stream_is_none_and_nothing_is_started(self, h, answer):
        h.rm.get(h.url, **answer)
        assert h.start() is None
        h.nothing_started()
        h.resume_info.assert_not_called()
        h.zfs_list.assert_not_called()

    @pytest.mark.parametrize('answer', [
        dict(status_code=503, json={'retry': True}), dict(status_code=500), dict(exc=_requests.ConnectionError('x')),
    ], ids=['reloading', '500', 'network'])
    def test_transfer_is_false_with_no_websocket_and_no_anchor_change(self, h, mocker, answer):
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        h.rm.get(h.url, **answer)
        ok = h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, None, 'offsite', snapshots=h.snaps)
        assert ok is False
        ws_cls.assert_not_called()
        h.nothing_started()
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_not_called()


class TestFreshSend:
    def test_a_full_send_when_nothing_is_common(self, h, caplog):
        with caplog.at_level(logging.INFO):
            started = h.start()
        assert isinstance(started, _SendStream)
        assert started.mode == 'fresh' and started.inflight_set is True
        assert started.sent is h.latest and started.base is None
        h.send_snapshot.assert_called_once_with(h.latest.snapshot, large_blocks=True, compressed=True)
        h.send_resume.assert_not_called()
        h.resume_info.assert_not_called()                  # no token, nothing to decode
        assert f'Sending full snapshot pool/data@{NEW}' in caplog.text

    def test_an_incremental_send_from_the_common_snapshot(self, h, caplog):
        common = h.snaps[2]
        with caplog.at_level(logging.INFO):
            started = h.start(common=common)
        assert started.mode == 'fresh' and started.base is common and started.sent is h.latest
        h.send_snapshot.assert_called_once_with(
            h.latest.snapshot, since=common.snapshot, large_blocks=True, compressed=True)
        assert f'Sending incremental pool/data: {OLD} -> {NEW}' in caplog.text

    def test_a_raw_send_is_w_alone(self, h):
        """Raw: only `raw=True` (libzfs adds `-L -e -c` itself): no `large_blocks`/`compressed`."""
        started = h.start(common=h.snaps[2], mode='raw')
        assert started.mode == 'fresh'
        h.send_snapshot.assert_called_once_with(h.latest.snapshot, since=h.snaps[2].snapshot, raw=True)

    def test_a_raw_full_send_is_w_alone(self, h):
        h.start(mode='raw')
        h.send_snapshot.assert_called_once_with(h.latest.snapshot, raw=True)

    def test_the_pin_is_written_before_zfs_send_starts(self, h):
        h.start(common=h.snaps[2])
        assert h.order == [('pin', OLD, NEW), 'send']

    def test_a_full_pin_has_an_empty_from(self, h):
        h.start()
        assert h.order == [('pin', None, NEW), 'send']
        assert h.set_inflight.call_args.args[1:] == ('offsite', None, NEW)

    def test_a_failed_pin_write_still_sends_but_reports_no_pin(self, h):
        h.set_inflight.side_effect = None
        h.set_inflight.return_value = False
        started = h.start()
        assert started.inflight_set is False and started.mode == 'fresh'
        h.send_snapshot.assert_called_once()

    def test_zfs_send_failing_to_start_is_an_error_and_none(self, h, caplog):
        h.send_snapshot.side_effect = Exception('cannot open pool/data@x')
        with caplog.at_level(logging.INFO):
            assert h.start() is None
        (rec,) = messages(caplog, 'Failed to start zfs send for pool/data')
        assert rec.levelname == 'ERROR' and 'cannot open pool/data@x' in rec.getMessage()
        # the pin was already written; it is left for the next send to overwrite
        h.clear_inflight.assert_not_called()


class TestTokenRejected:
    """A token that is not provably ours is never resumed: WARNING naming the
    reason, then a fresh send (the server discards its stale partial state)."""

    def rejected(self, h, caplog, reason, common=None):
        with caplog.at_level(logging.INFO):
            started = h.start(common=common)
        assert isinstance(started, _SendStream) and started.mode == 'fresh'
        assert started.sent is h.latest
        h.send_resume.assert_not_called()
        h.send_snapshot.assert_called_once()
        (rec,) = messages(caplog, 'Not resuming pool/data')
        assert rec.levelname == 'WARNING'
        assert reason in rec.getMessage()
        assert 'sending fresh' in rec.getMessage()
        assert 'Resuming interrupted transfer' not in caplog.text
        # the (only) pin is the fresh one, written before the fresh send
        assert h.order[-2:] == [('pin', common.name if common else None, NEW), 'send']
        assert [o for o in h.order if isinstance(o, tuple)] == [h.order[-2]]
        return rec.getMessage()

    @pytest.mark.parametrize('error', [
        "cannot resume send: 'rtp/send/c@s1' used in the initial send no longer exists",
        "cannot resume send: 'rtp/send/c@s2' is no longer the same snapshot used in the initial send",
        "cannot resume send: incremental source 0x4f75902409cdf08 no longer exists",
        "cannot resume send: incremental source (rtp/send/f@s1) no longer exists",
        "Invalid cross-device link",
        "CANNOT RESUME SEND: 'X' USED IN THE INITIAL SEND NO LONGER EXISTS",      # case-insensitive
    ])
    def test_a_definitive_zfs_error_means_send_fresh(self, h, caplog, error):
        h.token()
        h.decodes_to(toguid=222, fromguid=111, error=error)
        msg = self.rejected(h, caplog, 'zfs says the token cannot be resumed')
        assert error in msg
        h.zfs_list.assert_not_called()                      # decided before matching any GUID

    @pytest.mark.parametrize('flag', ['rawok', 'embedok', 'savedok'])
    def test_a_token_with_a_flag_this_mode_never_sends_is_not_resumed(self, h, caplog, flag):
        h.token()
        h.decodes_to(toguid=222, fromguid=111, flags=frozenset({flag}))
        msg = self.rejected(h, caplog, 'stream features this daemon does not send in plain mode')
        assert f'unexpected: {flag}' in msg

    def test_several_flags_are_all_named_sorted(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=111, flags=frozenset({'rawok', 'embedok', 'compressok'}))
        msg = self.rejected(h, caplog, 'stream features')
        assert 'unexpected: embedok, rawok' in msg           # compressok is allowed in plain mode

    def test_a_corrupt_token_resume_token_error(self, h, caplog):
        h.token('garbage')
        h.resume_info.side_effect = ResumeTokenError('Not a valid ZFS resume token')
        self.rejected(h, caplog, 'the resume token is corrupt or unusable: Not a valid ZFS resume token')
        h.zfs_list.assert_not_called()

    # (A decode failure that is NOT a ResumeTokenError skips the cycle: TestUndecidableTokenSkipsTheCycle.)

    def test_a_token_that_does_not_name_its_target(self, h, caplog):
        h.token()
        h.decodes_to(toguid=None, fromguid=None)
        self.rejected(h, caplog, 'does not name its target snapshot')

    def test_an_unknown_target_guid(self, h, caplog):
        h.token()
        h.decodes_to(toname='pool/data@gone', toguid=999, fromguid=None)
        msg = self.rejected(h, caplog, 'is not a managed local snapshot of this dataset')
        assert 'guid 999' in msg and 'pool/data@gone' in msg

    def test_a_target_guid_of_an_unmanaged_snapshot(self, h, caplog):
        """The snapshot exists locally (another prefix, say) but is not one of the daemon's: the
        managed listing, the only place GUIDs come from, does not have it."""
        h.token()
        h.decodes_to(toguid=444, fromguid=None)
        self.rejected(h, caplog, 'is not a managed local snapshot of this dataset')

    def test_an_unmanaged_incremental_source(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=444)
        msg = self.rejected(h, caplog, 'its incremental source (guid 444) is not a managed local snapshot')

    def test_an_unknown_incremental_source(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=999)
        self.rejected(h, caplog, 'its incremental source (guid 999) is not a managed local snapshot')

    def test_a_source_newer_than_the_target(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=333)               # from NEW to MID
        self.rejected(h, caplog, 'its incremental source is not older than its target')

    def test_a_source_equal_to_the_target(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=222)
        self.rejected(h, caplog, 'its incremental source is not older than its target')

    def test_a_snapshot_that_is_not_in_the_managed_listing_is_unmanaged_even_if_its_guid_is_local(self, h, caplog):
        """Matching is against the managed snapshot LIST, not just the local GUIDs."""
        h.token()
        h.decodes_to(toguid=222, fromguid=111)
        with caplog.at_level(logging.INFO):
            started = h.start(snaps=[h.snaps[0], h.snaps[2]])   # MID is not managed
        assert started.mode == 'fresh'
        assert messages(caplog, 'is not a managed local snapshot', 'WARNING')

    def test_the_rejection_carries_the_common_snapshot_into_the_fresh_incremental(self, h, caplog):
        h.token()
        h.decodes_to(toguid=999, fromguid=None)
        common = h.snaps[1]
        self.rejected(h, caplog, 'is not a managed local snapshot', common=common)
        h.send_snapshot.assert_called_once_with(
            h.latest.snapshot, since=common.snapshot, large_blocks=True, compressed=True)

    def test_the_token_is_decoded_once_and_never_sent_to_zfs_send(self, h):
        h.token(TOKEN_INCR)
        h.decodes_to(toguid=999)
        h.start()
        h.resume_info.assert_called_once_with(TOKEN_INCR)
        h.send_resume.assert_not_called()


class TestUndecidableTokenSkipsTheCycle:
    """Not knowing is not a reason to make the server discard its progress: only
    ResumeTokenError and the definitive `info.error` texts mean "send fresh"."""

    def skipped(self, h, caplog, reason):
        with caplog.at_level(logging.INFO):
            assert h.start() is None
        (rec,) = messages(caplog, 'cannot check the resume token of pool/data')
        assert rec.levelname == 'WARNING'
        assert 'skipping this cycle' in rec.getMessage() and reason in rec.getMessage()
        assert not messages(caplog, 'Not resuming')
        h.nothing_started()

    @pytest.mark.parametrize('exc', [
        Exception('zfs binary missing'), OSError('Too many open files'), RuntimeError('boom'),
        FileNotFoundError('zfs'), PermissionError('denied'),
    ])
    def test_any_decode_failure_other_than_a_corrupt_token_skips(self, h, caplog, exc):
        h.token()
        h.resume_info.side_effect = exc
        self.skipped(h, caplog, str(exc))
        h.zfs_list.assert_not_called()

    @pytest.mark.parametrize('error', [
        'cannot open pool: pool I/O is currently suspended',
        'out of memory', 'Operation not permitted', 'some new zfs message', 'the token is no longer valid',
    ])
    def test_an_unrecognised_zfs_error_skips(self, h, caplog, error):
        h.token()
        h.decodes_to(toguid=222, fromguid=111, error=error)
        self.skipped(h, caplog, error)
        assert 'not a definitive one' in caplog.text

    def test_a_partial_match_is_not_definitive(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=111, error='incremental source is busy')       # no "no longer exists"
        self.skipped(h, caplog, 'incremental source is busy')

    def test_a_corrupt_token_still_means_send_fresh(self, h, caplog):
        h.token('garbage')
        h.resume_info.side_effect = ResumeTokenError('Not a valid ZFS resume token')
        with caplog.at_level(logging.INFO):
            assert h.start().mode == 'fresh'

    def test_transfer_is_false_and_opens_no_websocket(self, h, mocker):
        h.token()
        h.resume_info.side_effect = Exception('zfs binary missing')
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        assert h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, None, 'offsite', snapshots=h.snaps) is False
        ws_cls.assert_not_called()
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_not_called()


class TestSnapshotWithoutAGuid:
    """S1-d: GUIDs come from the one managed listing (`SnapshotInfo.guid`); there is no second
    `zfs list`. A managed snapshot whose guid was not listed makes a lookup MISS uncertain
    (the target could be that snapshot), so the cycle is skipped instead of discarding the
    server's partial state; a lookup HIT is certain whatever else is missing."""

    def snaps_missing(self, h, *names):
        """The harness's listing with `names` lacking a guid."""
        return [sinfo(s.name, guid=ABSENT) if s.name in names else s for s in h.snaps]

    def skipped(self, h, caplog, snaps):
        with caplog.at_level(logging.INFO):
            assert h.start(snaps=snaps) is None
        (rec,) = messages(caplog, 'cannot check the resume token')
        assert rec.levelname == 'WARNING' and 'skipping this cycle' in rec.getMessage()
        assert not messages(caplog, 'Not resuming')
        h.nothing_started()
        return rec.getMessage()

    def test_no_guid_and_a_lookup_miss_skips_the_cycle(self, h, caplog):
        h.token()
        h.decodes_to(toguid=999, fromguid=None)
        msg = self.skipped(h, caplog, self.snaps_missing(h, NEW))
        assert f'cannot read the GUID of local snapshot(s) {NEW}' in msg

    def test_the_miss_may_be_on_the_incremental_source_too(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=999)                # target found, source not
        msg = self.skipped(h, caplog, self.snaps_missing(h, NEW))
        assert NEW in msg

    def test_every_guidless_name_is_named_up_to_three(self, h, caplog):
        h.token()
        h.decodes_to(toguid=999, fromguid=None)
        msg = self.skipped(h, caplog, self.snaps_missing(h, NEW, MID, OLD))
        assert all(n in msg for n in (NEW, MID, OLD))

    def test_a_hit_is_certain_even_with_a_guidless_snapshot(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=111)
        with caplog.at_level(logging.INFO):
            started = h.start(snaps=self.snaps_missing(h, NEW))
        assert started.mode == 'resume' and started.sent.name == MID and started.base.name == OLD
        assert not messages(caplog, 'cannot check the resume token')

    def test_all_guids_present_and_a_miss_is_definitive(self, h, caplog):
        h.token()
        h.decodes_to(toguid=999, fromguid=None)
        with caplog.at_level(logging.INFO):
            started = h.start()
        assert started.mode == 'fresh'
        assert messages(caplog, 'is not a managed local snapshot', 'WARNING')

    def test_no_token_means_no_guid_is_ever_needed(self, h):
        started = h.start(snaps=self.snaps_missing(h, NEW, MID, OLD))
        assert started.mode == 'fresh'

    def test_transfer_is_false_and_opens_no_websocket(self, h, mocker):
        h.token()
        h.decodes_to(toguid=999, fromguid=None)
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        snaps = self.snaps_missing(h, NEW)
        assert h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, None, 'offsite', snapshots=snaps) is False
        ws_cls.assert_not_called()
        h.nothing_started()

    def test_there_is_no_second_listing_for_guids(self, h):
        assert not hasattr(type(h.rbm), '_snapshot_guids')
        h.token()
        h.start()
        h.zfs_list.assert_not_called()


class TestTokenAccepted:
    def test_an_incremental_token_resumes_with_the_guid_matched_pair(self, h, caplog):
        h.token(TOKEN_INCR)
        h.decodes_to(toname='pool/data@whatever', toguid=222, fromguid=111)
        with caplog.at_level(logging.INFO):
            started = h.start(common=h.snaps[1])              # the negotiated `common` is irrelevant here
        assert started.mode == 'resume' and started.inflight_set is True
        assert started.sent is h.snaps[1] and started.base is h.snaps[2]
        h.send_resume.assert_called_once_with(TOKEN_INCR)
        h.send_snapshot.assert_not_called()
        assert f'Resuming interrupted transfer for pool/data@{MID}' in caplog.text
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_a_full_token_has_no_base_and_an_empty_pin_from(self, h):
        h.token(TOKEN_FULL)
        h.decodes_to(toguid=111, fromguid=None)
        started = h.start()
        assert started.mode == 'resume' and started.base is None and started.sent is h.snaps[2]
        assert h.order == [('pin', None, OLD), 'send-t']

    def test_the_pin_is_written_before_zfs_send_t(self, h):
        h.token()
        h.start()
        assert h.order == [('pin', OLD, MID), 'send-t']
        assert h.set_inflight.call_args.args[1:] == ('offsite', OLD, MID)

    def test_the_pin_names_come_from_the_guid_match_not_the_tokens_toname(self, h):
        """The sender renamed since: `toname` is stale; `toguid` is the identity."""
        h.token()
        h.decodes_to(toname='pool/olddata@autosnap_19990101000000', toguid=222, fromguid=111)
        started = h.start()
        assert started.mode == 'resume' and started.sent.name == MID
        assert h.order[0] == ('pin', OLD, MID)

    def test_a_failed_pin_write_still_resumes_but_reports_no_pin(self, h):
        h.token()
        h.set_inflight.side_effect = None
        h.set_inflight.return_value = False
        started = h.start()
        assert started.mode == 'resume' and started.inflight_set is False
        h.send_resume.assert_called_once()

    def test_the_resume_token_is_the_only_thing_passed_to_zfs_send_t(self, h):
        h.token(TOKEN_INCR)
        h.start()
        assert h.send_resume.call_args.args == (TOKEN_INCR,) and h.send_resume.call_args.kwargs == {}

    def test_no_listing_is_read_for_guids(self, h):
        """The guids were listed with the snapshots (S1-a); matching reads nothing more."""
        h.token()
        h.start()
        h.zfs_list.assert_not_called()

    def test_zfs_send_t_failing_to_start_warns_and_sends_fresh(self, h, caplog):
        h.token()
        h.send_resume.side_effect = Exception('cannot resume send: bad')
        with caplog.at_level(logging.INFO):
            started = h.start(common=h.snaps[2])
        assert started.mode == 'fresh' and started.sent is h.latest and started.base is h.snaps[2]
        (rec,) = messages(caplog, 'zfs send -t failed to start for pool/data')
        assert rec.levelname == 'WARNING' and 'cannot resume send: bad' in rec.getMessage()
        assert 'sending fresh' in rec.getMessage()
        # the resume pin, then the fresh pin (it overwrites), then the fresh send
        assert h.order == [('pin', OLD, MID), ('pin', OLD, NEW), 'send']
        h.send_snapshot.assert_called_once_with(
            h.latest.snapshot, since=h.snaps[2].snapshot, large_blocks=True, compressed=True)

    def test_the_fresh_send_after_a_failed_resume_start_reports_its_own_pin(self, h):
        h.token()
        h.send_resume.side_effect = Exception('bad')
        calls = iter([True, False])
        h.set_inflight.side_effect = lambda *a: next(calls)
        assert h.start().inflight_set is False


class TestRealTokensThroughTheRealDecoder:
    """The decision on top of the real `resume_info` parsing: `zfs send -nv -t`
    output captured on a ZFS 2.4.1 VM (only `Popen` is mocked)."""

    @staticmethod
    def lines(block):
        return [line + '\n' for line in block]

    def test_a_real_full_token_resumes(self, make_h, mock_subprocess):
        h = make_h(decode=False)
        mock_subprocess.setup(stdout=self.lines(FULL_CONTENTS))
        snaps = [sinfo(NEW), sinfo(OLD, guid=0x61034bf73452096e)]
        h.rm.get(h.url, json={'resume_token': TOKEN_FULL})
        started = h.start(snaps=snaps)
        assert started.mode == 'resume' and started.sent.name == OLD and started.base is None
        argv = list(mock_subprocess.call_args_list[0].args[0])
        assert argv[1:] == ['send', '-nv', '-t', TOKEN_FULL]
        assert h.order == [('pin', None, OLD), 'send-t']

    def test_a_real_incremental_token_resumes(self, make_h, mock_subprocess):
        h = make_h(decode=False)
        mock_subprocess.setup(stdout=self.lines(INCR_CONTENTS))
        snaps = [sinfo(NEW), sinfo(MID, guid=0x61034ac2f730d529), sinfo(OLD, guid=0x4f39e607147f3dc2)]
        h.rm.get(h.url, json={'resume_token': TOKEN_INCR})
        started = h.start(snaps=snaps)
        assert started.mode == 'resume' and started.sent.name == MID and started.base.name == OLD

    def test_a_real_token_for_a_destroyed_target_is_not_resumed(self, make_h, mock_subprocess, caplog):
        """`zfs send -nv -t` prints the contents, then fails with rc 255."""
        h = make_h(decode=False)
        mock_subprocess.setup(
            stdout=self.lines(FULL_CONTENTS[:-2]),
            stderr="cannot resume send: 'rtp/send/c@s1' used in the initial send no longer exists",
            returncode=255,
        )
        snaps = [sinfo(OLD, guid=0x61034bf73452096e)]
        h.rm.get(h.url, json={'resume_token': TOKEN_FULL})
        with caplog.at_level(logging.INFO):
            started = h.start(snaps=snaps)
        assert started.mode == 'fresh'
        (rec,) = messages(caplog, 'Not resuming pool/data')
        assert 'no longer exists' in rec.getMessage()
        assert 'zfs says the token cannot be resumed' in rec.getMessage()

    def test_a_garbage_token_is_not_resumed(self, make_h, mock_subprocess, caplog):
        h = make_h(decode=False)
        h.rm.get(h.url, json={'resume_token': 'not a token'})       # fails validation before any zfs call
        with caplog.at_level(logging.INFO):
            started = h.start()
        assert started.mode == 'fresh'
        assert mock_subprocess.call_count == 0
        assert messages(caplog, 'the resume token is corrupt or unusable', 'WARNING')


S1, S2, S3 = OLD, MID, NEW
S4 = 'autosnap_20240104120000'
S5 = 'autosnap_20240105120000'
FIVE = [S1, S2, S3, S4, S5]
PLAIN_FLAGS = dict(large_blocks=True, compressed=True)


def five(h, txgs=None):
    """`h.snaps` / `h.latest` := S5..S1, newest first by `createtxg` (the order the manager
    delivers). `txgs` ({name: txg}) overrides the default txg, which follows the name."""
    snaps = [sinfo(n, **({'txg': txgs[n]} if txgs else {})) for n in FIVE]
    snaps.sort(key=lambda s: s.createtxg, reverse=True)
    h.snaps = snaps
    h.latest = snaps[0]
    return snaps


#: `Chain(negotiate=REAL)`: leave `_negotiate` alone (the server's answer is a requests_mock route).
REAL = object()


class Chain:
    """`_transfer` end to end on top of a `Harness`: websocket sessions are handed out in order
    (`sessions`), and `_negotiate` and the re-listing are stubbed.

    `negotiate`: a snapshot name or None (every call answers it), an exception (raised), or a
    list (one answer per call, exceptions raised). `relist`: None (every re-listing is `h.snaps`)
    or a list of listings, one per call. Every websocket records 'connect' and 'ack' (the final
    `recv`) in `h.events`."""

    def __init__(self, h, mocker, sessions=2, negotiate=MID, relist=None):
        import json as _json
        self.h = h
        self.sessions = []
        for _ in range(sessions):
            ws = MagicMock()
            ws.recv.return_value = _json.dumps({'status': 'ok'})
            ws.recv.side_effect = lambda: (h.events.append('ack'), DEFAULT)[1]
            ws.connect.side_effect = lambda *a, **k: (h.events.append('connect'), DEFAULT)[1]
            self.sessions.append(ws)
        self.ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket', side_effect=list(self.sessions))
        if negotiate is REAL:
            self.negotiate = None
        elif isinstance(negotiate, (Exception, list)):
            self.negotiate = mocker.patch.object(h.rbm, '_negotiate', side_effect=negotiate)
        else:
            self.negotiate = mocker.patch.object(h.rbm, '_negotiate', return_value=negotiate)
        if relist is None:
            self.relist = mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        else:
            self.relist = mocker.patch.object(h.manager, 'list_snapshots', side_effect=relist)

    def run(self, common=None, latest=None, mode='plain', **kw):
        h = self.h
        if isinstance(common, str):
            common = by_name(h.snaps, common)
        return h.rbm._transfer(
            h.transport, 'client1', h.dsi, latest or h.latest, common, 'offsite',
            snapshots=h.snaps, mode=mode, **kw,
        )

    def urls(self):
        return [s.connect.call_args.args[0].split('/stream', 1)[1] for s in self.sessions
                if s.connect.called]

    def sent(self):
        """`[(since, snapshot)]` short names of every `zfs send` spawned, in order."""
        calls = self.h.send_snapshot.call_args_list
        return [(c.kwargs['since'].short if c.kwargs.get('since') else None, c.args[0].short)
                for c in calls]


@pytest.fixture
def h5(h):
    """The harness with five snapshots S1..S5 (S1 the oldest)."""
    five(h)
    return h


def websocket_timeout():
    import websocket
    return websocket.WebSocketTimeoutException('timed out')


def upgrade_refused(status, error):
    import websocket
    return websocket.WebSocketBadStatusException(
        f'Handshake status {status}', status, 'refused', {}, json.dumps({'error': error}).encode()
    )


class TestChainSteps:
    """B5-b: after one negotiate, every managed snapshot newer than the common one reaches the
    server, oldest first, one `zfs send -i prev next` each; a full send is only the newest."""

    def test_n_newer_snapshots_are_n_incremental_sends_oldest_first(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=5)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1) is True
        assert chain.sent() == [(S1, S2), (S2, S3), (S3, S4), (S4, S5)]
        assert chain.urls() == [f'?mode=fresh&from={a}' for a in (S1, S2, S3, S4)]
        assert chain.ws_cls.call_count == 4
        assert f'Sending 4 snapshots of pool/data to offsite: {S1} -> {S2} -> {S3} -> {S4} -> {S5}' in caplog.text

    def test_every_send_carries_the_frozen_plain_flags(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1)
        for call in h5.send_snapshot.call_args_list:
            assert {k: v for k, v in call.kwargs.items() if k != 'since'} == PLAIN_FLAGS

    def test_one_negotiate_and_one_token_fetch_for_the_whole_chain(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1)
        chain.negotiate.assert_not_called()            # `_transfer` is handed the negotiated common snapshot
        assert len(h5.token_gets()) == 1
        chain.relist.assert_not_called()

    def test_the_order_follows_createtxg_not_the_name(self, h5, mocker):
        """Names ascend S1 < S2 < S3 < S4, txgs descend: S4 is the oldest (the common one)."""
        five(h5, txgs={S1: 400, S2: 300, S3: 200, S4: 100, S5: 500})
        chain = Chain(h5, mocker, sessions=5)
        assert h5.latest.name == S5
        assert chain.run(common=S4) is True
        assert chain.sent() == [(S4, S3), (S3, S2), (S2, S1), (S1, S5)]

    def test_snapshots_older_than_the_common_one_are_not_sent(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S3)
        assert chain.sent() == [(S3, S4), (S4, S5)]

    def test_a_full_send_sends_only_the_newest_with_no_history(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=5)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=None) is True
        assert chain.sent() == [(None, S5)]
        assert chain.urls() == ['?mode=fresh']
        assert h5.set_inflight.call_args.args[1:] == ('offsite', None, S5)
        assert 'Sending 1 snapshots' not in caplog.text and 'Sending full snapshot pool/data@' + S5 in caplog.text
        assert [c.args[1:] for c in h5.set_anchor.call_args_list] == [('offsite', S5)]

    def test_each_step_is_pinned_before_its_send_and_anchored_after_its_ack(self, h5, mocker):
        """The pin is kept between steps (the next step's pin overwrites it) and released once,
        after the last step's anchor."""
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S3)
        assert [e for e in h5.events if not (isinstance(e, tuple) and e[0] == 'stop?')] == [
            ('pin', S3, S4), 'send', 'connect', 'ack', ('anchor', S4),
            ('pin', S4, S5), 'send', 'connect', 'ack', ('anchor', S5), 'clear',
        ]

    def test_back_to_back_steps_use_one_websocket_each(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S3)
        assert chain.ws_cls.call_count == 2
        for ws in chain.sessions[:2]:
            ws.close.assert_called()

    def test_every_step_streams_its_own_bytes_then_done(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S3)
        for ws in chain.sessions[:2]:
            assert [c.args[0] for c in ws.send_binary.call_args_list] == [b'chunk']
            assert json.loads(ws.send.call_args.args[0]) == {'done': True}

    def test_the_planner_alone(self, h5):
        plan = h5.rbm._plan_chain
        assert [s.name for s in plan(h5.snaps, by_name(h5.snaps, S2))] == [S3, S4, S5]
        assert [s.name for s in plan(h5.snaps, None)] == [S5]
        assert plan([], None) == []
        assert plan(h5.snaps, h5.latest) == []


class TestChainFailuresStopIt:
    """A failure of any step ends the chain: no later step, never a full send."""

    def run_failing_at_step_2(self, h5, mocker, kind):
        chain = Chain(h5, mocker, sessions=5)
        ws = chain.sessions[1]
        if kind == 'server-error':
            ws.recv.return_value = json.dumps({'status': 'error', 'error': 'disk full'})
        elif kind == 'send-timeout':
            ws.send_binary.side_effect = websocket_timeout()
        elif kind == 'connect':
            # the server is not there right after step 1: every attempt (the first and the
            # retries, each on a fresh websocket) is refused
            for retry_ws in chain.sessions[1:]:
                retry_ws.connect.side_effect = ConnectionRefusedError('refused')
        elif kind == 'done-fails':
            ws.send.side_effect = websocket_timeout()
        elif kind == 'busy-ack':
            ws.recv.return_value = json.dumps({'error': 'busy', 'retry': True})
        elif kind == 'reloading-ack':
            ws.recv.return_value = json.dumps({'error': 'server is reloading; retry later', 'retry': True})
        elif kind == 'busy-behind-a-failed-send':
            ws.send_binary.side_effect = BrokenPipeError('broken pipe')
            ws.recv.return_value = json.dumps({'error': 'busy', 'retry': True})
        elif kind == 'unauthorized':
            ws.connect.side_effect = upgrade_refused(401, 'no client certificate')
        elif kind == 'not-trusted':
            ws.connect.side_effect = upgrade_refused(403, 'client not trusted')
        elif kind == 'id-mismatch':
            ws.connect.side_effect = upgrade_refused(403, 'client_id mismatch')
        elif kind == 'zfs-send-fails-to-start':
            real = h5.send_snapshot.side_effect
            calls = []

            def flaky(*a, **k):
                calls.append(1)
                if len(calls) == 2:
                    raise Exception('cannot open pool/data@x')
                return real(*a, **k)
            h5.send_snapshot.side_effect = flaky
        return chain, chain.run(common=S1)

    #: kind -> does the chain re-list (an ordinary failure may be a vanished member)
    KINDS = {
        'server-error': True, 'send-timeout': True, 'done-fails': True,
        'zfs-send-fails-to-start': True,
        'connect': False,                       # exhausted connect retries after a success: a stop
        'busy-ack': False, 'reloading-ack': False, 'busy-behind-a-failed-send': False,
        'unauthorized': False, 'not-trusted': False, 'id-mismatch': False,
    }

    @pytest.mark.parametrize('kind', list(KINDS))
    def test_no_further_step_and_never_a_full_send(self, h5, mocker, kind):
        chain, ok = self.run_failing_at_step_2(h5, mocker, kind)
        assert ok is False
        # step 1 went through, step 2 was attempted, nothing after it
        assert h5.send_snapshot.call_count == 2
        assert chain.sent() == [(S1, S2), (S2, S3)]
        assert all(since is not None for since, _ in chain.sent())        # never a full send
        assert [c.args[1:] for c in h5.set_anchor.call_args_list] == [('offsite', S2)]   # step 1 only
        h5.clear_inflight.assert_not_called()                            # step 1's pin is kept for step 2's to overwrite; step 2's stays
        assert [c.args[1:] for c in h5.set_inflight.call_args_list] == [
            ('offsite', S1, S2), ('offsite', S2, S3)]

    @pytest.mark.parametrize('kind, relists', list(KINDS.items()))
    def test_only_an_ordinary_failure_looks_for_a_vanished_member(self, h5, mocker, kind, relists):
        chain, _ = self.run_failing_at_step_2(h5, mocker, kind)
        assert chain.relist.call_count == (1 if relists else 0)

    @pytest.mark.parametrize('kind', ['unauthorized', 'not-trusted', 'id-mismatch'])
    def test_an_auth_refusal_skips_the_destination_for_the_rest_of_the_cycle(self, h5, mocker, kind, caplog):
        with caplog.at_level(logging.INFO):
            chain, ok = self.run_failing_at_step_2(h5, mocker, kind)
        assert h5.rbm._checked['offsite'][1] is False
        assert len([r for r in caplog.records if r.levelname == 'ERROR']) == 1

    @pytest.mark.parametrize('kind', ['busy-ack', 'reloading-ack', 'busy-behind-a-failed-send'])
    def test_a_busy_or_reloading_server_is_info_never_an_error(self, h5, mocker, kind, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            self.run_failing_at_step_2(h5, mocker, kind)
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert [r.levelname for r in caplog.records
                if r.name == 'zfsbackup.remote' and 'skipping' in r.getMessage()] == ['INFO']

    def test_a_failing_first_step_sends_nothing_after_it(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.sessions[0].recv.return_value = json.dumps({'status': 'error', 'error': 'no'})
        assert chain.run(common=S1) is False
        assert chain.sent() == [(S1, S2)]
        h5.set_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()

    def test_the_next_cycle_resumes_from_the_last_good_step(self, h5, mocker):
        """After the failure the server holds S2: the next negotiate says so and the chain
        carries on with S3."""
        chain, ok = self.run_failing_at_step_2(h5, mocker, 'send-timeout')
        assert ok is False
        h5.send_snapshot.reset_mock()
        again = Chain(h5, mocker, sessions=5)
        assert again.run(common=S2) is True
        assert again.sent() == [(S2, S3), (S3, S4), (S4, S5)]


class TestAnchorWriteFailureStopsTheChain:
    def test_the_chain_stops_and_the_pin_stays(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        h5.set_anchor.return_value = False
        assert chain.run(common=S1) is False             # a step that is not the last one
        assert chain.sent() == [(S1, S2)]
        assert h5.set_anchor.call_count == 1
        h5.clear_inflight.assert_not_called()
        chain.relist.assert_not_called()

    def test_a_failed_anchor_write_on_the_last_step_is_still_a_success(self, h5, mocker):
        """The server has everything; only the local bookkeeping failed (the next cycle's
        negotiate finds the common snapshot and settles it)."""
        chain = Chain(h5, mocker, sessions=5)
        h5.set_anchor.return_value = False
        assert chain.run(common=S4) is True
        assert chain.sent() == [(S4, S5)]
        h5.clear_inflight.assert_not_called()

    def test_the_second_step_failing_to_anchor_keeps_its_own_pin(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        results = iter([True, False])
        h5.set_anchor.side_effect = lambda *a: next(results)
        assert chain.run(common=S1) is False
        assert chain.sent() == [(S1, S2), (S2, S3)]
        h5.clear_inflight.assert_not_called()                 # nothing is released before the last step


class TestAnchorNeverMovesBackwardsInAChain:
    def test_a_newer_anchor_is_kept_through_the_earlier_steps(self, h5, mocker):
        """The anchor is already on S5 (say a resume or a concurrent cycle got there): S2..S4
        leave it and their pins stay; S5 itself is written (anchor == sent) and clears the pin."""
        h5.get_anchor.return_value = S5
        chain = Chain(h5, mocker, sessions=5)
        assert chain.run(common=S1) is True
        assert [c.args[1:] for c in h5.set_anchor.call_args_list] == [('offsite', S5)]
        assert h5.clear_inflight.call_count == 1

    def test_an_older_anchor_moves_forward_at_every_step(self, h5, mocker):
        h5.get_anchor.return_value = S1
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1)
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S3, S4, S5]

    def test_the_comparison_is_by_createtxg_not_by_name(self, h5, mocker):
        """S1 carries the NEWEST txg although its name is the oldest: an anchor on it is newer
        than every other step."""
        five(h5, txgs={S1: 900, S2: 20, S3: 30, S4: 40, S5: 50})
        h5.get_anchor.return_value = S1
        chain = Chain(h5, mocker, sessions=5)
        assert h5.latest.name == S1
        chain.run(common=S2)                         # steps by txg: S3, S4, S5, S1
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S1]


class TestStopBetweenSteps:
    def test_a_stop_after_a_step_returns_true_and_says_it_continues_next_cycle(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=5)
        # polls: entry, before step 1, then the first poll after step 1 says stop
        stop = h5.stopper(False, False, True)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1, should_stop=stop) is True
        assert chain.sent() == [(S1, S2)]
        assert f'stop requested; pool/data -> offsite continues to {S5} next cycle' in caplog.text
        assert [c.args[1:] for c in h5.set_anchor.call_args_list] == [('offsite', S2)]   # step 1 is kept
        h5.clear_inflight.assert_called_once()

    def test_a_stop_before_the_first_send_returns_false(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=5)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1, should_stop=lambda: True) is False
        chain.ws_cls.assert_not_called()
        h5.nothing_started()
        assert 'continues' not in caplog.text

    def test_a_stop_that_arrives_just_before_the_first_spawn_returns_false(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=5)
        stop = h5.stopper(False, True)                  # entry: no; right before `zfs send`: yes
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1, should_stop=stop) is False
        h5.nothing_started()
        assert 'stop requested; not starting a send of pool/data' in caplog.text

    def test_should_stop_is_never_asked_while_a_stream_is_open(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1, should_stop=h5.stopper())
        events = h5.events
        open_stream = False
        for e in events:
            if e == 'send':
                open_stream = True
            elif e == 'ack':
                open_stream = False
            elif isinstance(e, tuple) and e[0] == 'stop?':
                assert not open_stream, events

    def test_should_stop_is_polled_once_per_step(self, h5, mocker):
        """Between two streams exactly one poll (the safe point); before the first stream the
        entry check and the last look before `zfs send`."""
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1, should_stop=h5.stopper())
        segments, current = [], 0
        for e in h5.events:
            if isinstance(e, tuple) and e[0] == 'stop?':
                current += 1
            elif isinstance(e, tuple) and e[0] == 'pin':
                segments.append(current)
                current = 0
        assert len(segments) == 4
        assert segments[0] <= 2
        assert segments[1:] == [1, 1, 1]

    def test_no_stop_callback_means_no_polling(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        assert chain.run(common=S1) is True
        assert h5.send_snapshot.call_count == 4

    def test_a_stop_pending_after_the_last_step_does_not_hide_the_success(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        assert chain.run(common=S4, should_stop=h5.stopper(False, False)) is True
        assert chain.sent() == [(S4, S5)]


class TestVanishedChainMember:
    """B7: a snapshot the pruner (or an operator) deletes between two steps is not an error."""

    def without(self, h5, *names):
        return [s for s in h5.snaps if s.name not in names]

    def test_a_vanished_member_is_rebuilt_around_once(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=6, relist=[self.without(h5, S3)])
        chain.sessions[1].send_binary.side_effect = websocket_timeout()           # S3's stream dies
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1) is True
        # S2 ok; S3 fails and is gone; the rest is rebuilt from S2: S4 then S5
        assert chain.sent() == [(S1, S2), (S2, S3), (S2, S4), (S4, S5)]
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S4, S5]
        assert f'pool/data@{S3} no longer exists; rebuilding the rest of the chain without it' in caplog.text
        chain.relist.assert_called_once()

    def test_each_vanished_member_is_rebuilt_once(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=8, relist=[self.without(h5, S3), self.without(h5, S3, S4)])
        chain.sessions[1].send_binary.side_effect = websocket_timeout()       # S3 vanishes
        chain.sessions[2].send_binary.side_effect = websocket_timeout()       # then S4
        assert chain.run(common=S1) is True
        assert chain.sent() == [(S1, S2), (S2, S3), (S2, S4), (S2, S5)]
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S5]
        assert chain.relist.call_count == 2

    def test_a_vanished_first_step_is_rebuilt_from_the_common_snapshot(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=6, relist=[self.without(h5, S2)])
        chain.sessions[0].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=S1) is True
        assert chain.sent() == [(S1, S2), (S1, S3), (S3, S4), (S4, S5)]

    def test_a_failure_while_the_snapshot_still_exists_stops(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=6, relist=[list(h5.snaps)])
        chain.sessions[1].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=S1) is False
        assert chain.sent() == [(S1, S2), (S2, S3)]
        chain.relist.assert_called_once()

    def test_a_vanished_base_stops(self, h5, mocker):
        """S3 failed and is gone, but so is its base S2: there is no incremental source left."""
        chain = Chain(h5, mocker, sessions=6, relist=[self.without(h5, S2, S3)])
        chain.sessions[1].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=S1) is False
        assert chain.sent() == [(S1, S2), (S2, S3)]

    def test_an_empty_re_listing_stops(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=6, relist=[[]])
        chain.sessions[1].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=S1) is False

    def test_a_full_send_whose_snapshot_vanished_is_rebuilt_to_the_new_newest(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=6, relist=[self.without(h5, S5)])
        chain.sessions[0].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=None) is True
        assert chain.sent() == [(None, S5), (None, S4)]

    def test_a_stop_or_refusal_is_not_taken_for_a_vanished_member(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=6, relist=[self.without(h5, S3)])
        chain.sessions[1].recv.return_value = json.dumps({'error': 'busy', 'retry': True})
        assert chain.run(common=S1) is False
        chain.relist.assert_not_called()
        assert chain.sent() == [(S1, S2), (S2, S3)]


class TestResumeThenTheRestOfTheChain:
    """The server's resume token runs as step 0 (`mode=resume`, `zfs send -t`, no flags); then
    ONE re-negotiate and the chain continues from the new common snapshot."""

    @pytest.fixture
    def resumed_s2(self, h5):
        """The server holds a partial S1 -> S2."""
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        return h5

    def test_the_resume_is_step_zero_then_one_renegotiate_then_the_chain(self, resumed_s2, mocker, caplog):
        h5 = resumed_s2
        chain = Chain(h5, mocker, sessions=6, negotiate=S2)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1) is True
        assert chain.urls() == [f'?mode=resume&from={S1}'] + [f'?mode=fresh&from={a}' for a in (S2, S3, S4)]
        h5.send_resume.assert_called_once_with(TOKEN_INCR)
        assert chain.sent() == [(S2, S3), (S3, S4), (S4, S5)]
        chain.negotiate.assert_called_once()
        chain.relist.assert_called_once()                     # re-listed, not reused
        assert len(h5.token_gets()) == 1                      # the token is read once per call
        assert f'Resumed pool/data@{S2} (from {S1}) to offsite' in caplog.text
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S3, S4, S5]

    def test_the_renegotiate_gets_the_current_listing(self, resumed_s2, mocker):
        chain = Chain(resumed_s2, mocker, sessions=6, negotiate=S2)
        chain.run(common=S1)
        assert chain.negotiate.call_args.args == (
            resumed_s2.transport, 'client1', resumed_s2.dsi, resumed_s2.snaps, 'offsite')

    def test_the_resume_has_its_own_pin_and_the_order_is_pin_send_t_then_pin_send(self, resumed_s2, mocker):
        h5 = resumed_s2
        chain = Chain(h5, mocker, sessions=6, negotiate=S2)
        chain.run(common=S1)
        assert h5.order[:4] == [('pin', S1, S2), 'send-t', ('pin', S2, S3), 'send']

    def test_a_snapshot_taken_meanwhile_ends_the_chain(self, resumed_s2, mocker):
        h5 = resumed_s2
        s6 = sinfo('autosnap_20240106120000')
        chain = Chain(h5, mocker, sessions=7, negotiate=S2, relist=[[s6] + h5.snaps])
        assert chain.run(common=S1) is True
        assert chain.sent()[-1] == (S5, s6.name)
        assert len(chain.sent()) == 4

    def test_no_chain_when_the_resumed_snapshot_is_the_latest(self, h5, mocker, caplog):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=h5.latest.guid, fromguid=sinfo(S4).guid)
        chain = Chain(h5, mocker, sessions=3, negotiate=S4)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1) is True
        assert chain.ws_cls.call_count == 1
        chain.negotiate.assert_not_called()
        chain.relist.assert_not_called()
        h5.send_snapshot.assert_not_called()
        assert f'Resumed pool/data@{S5} (from {S4}) to offsite' in caplog.text

    def test_a_renegotiate_that_finds_latest_already_there_just_settles(self, resumed_s2, mocker):
        h5 = resumed_s2
        chain = Chain(h5, mocker, sessions=3, negotiate=S5)
        assert chain.run(common=S1) is True
        assert chain.ws_cls.call_count == 1
        h5.send_snapshot.assert_not_called()
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S5]

    def test_a_stop_after_the_resume_skips_the_rest(self, resumed_s2, mocker, caplog):
        h5 = resumed_s2
        chain = Chain(h5, mocker, sessions=6, negotiate=S2)
        stop = h5.stopper(False, False, True)         # entry, after the token is accepted, after the resume
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1, should_stop=stop) is True
        assert chain.ws_cls.call_count == 1
        chain.negotiate.assert_not_called()
        assert f'stop requested; pool/data -> offsite continues to {S5} next cycle' in caplog.text
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2]

    def test_a_failed_renegotiate_ends_it_after_the_resume_completed(self, resumed_s2, mocker):
        h5 = resumed_s2
        chain = Chain(h5, mocker, sessions=6, negotiate=NegotiationFailed('server said no'))
        assert chain.run(common=S1) is False
        assert chain.ws_cls.call_count == 1
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2]    # the resume is safe
        h5.send_snapshot.assert_not_called()

    def test_a_reloading_renegotiate_is_info_and_false(self, resumed_s2, mocker, caplog):
        chain = Chain(resumed_s2, mocker, sessions=6,
                      negotiate=NegotiationFailed('server is reloading', reloading=True))
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1) is False
        assert [r.levelname for r in messages(caplog, 'pool/data continues next cycle')] == ['INFO']

    def test_a_failed_resume_never_reaches_the_chain(self, resumed_s2, mocker):
        chain = Chain(resumed_s2, mocker, sessions=6, negotiate=S2)
        chain.sessions[0].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=S1) is False
        assert chain.ws_cls.call_count == 1
        chain.negotiate.assert_not_called()
        resumed_s2.set_anchor.assert_not_called()
        resumed_s2.clear_inflight.assert_not_called()
        resumed_s2.send_snapshot.assert_not_called()

    def test_a_refusal_of_the_resume_stops_without_a_fresh_send(self, resumed_s2, mocker):
        chain = Chain(resumed_s2, mocker, sessions=6, negotiate=S2)
        chain.sessions[0].recv.return_value = json.dumps({'error': 'busy', 'retry': True})
        assert chain.run(common=S1) is False
        resumed_s2.send_snapshot.assert_not_called()
        chain.negotiate.assert_not_called()

    def test_a_resume_anchor_write_failure_stops(self, resumed_s2, mocker):
        chain = Chain(resumed_s2, mocker, sessions=6, negotiate=S2)
        resumed_s2.set_anchor.return_value = False
        assert chain.run(common=S1) is False
        chain.negotiate.assert_not_called()
        resumed_s2.clear_inflight.assert_not_called()

    def test_a_stale_token_leads_to_a_normal_fresh_chain(self, h5, mocker, caplog):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=999, fromguid=None)               # a snapshot we do not have
        chain = Chain(h5, mocker, sessions=6)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1) is True
        assert chain.sent() == [(S1, S2), (S2, S3), (S3, S4), (S4, S5)]
        h5.send_resume.assert_not_called()
        chain.negotiate.assert_not_called()                    # no re-negotiate without a resume
        assert messages(caplog, 'Not resuming pool/data', 'WARNING')

    def test_an_undecidable_token_skips_the_cycle(self, h5, mocker):
        h5.token(TOKEN_INCR)
        h5.resume_info.side_effect = RuntimeError('zfs binary missing')
        chain = Chain(h5, mocker, sessions=6)
        assert chain.run(common=S1) is False
        chain.ws_cls.assert_not_called()
        h5.nothing_started()

    def test_a_flag_from_the_wrong_mode_is_not_resumed(self, h5, mocker):
        """A raw token while the dataset is sent plain: a fresh chain, not a resume."""
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111, flags=frozenset({'rawok', 'compressok'}))
        chain = Chain(h5, mocker, sessions=6)
        assert chain.run(common=S1, mode='plain') is True
        h5.send_resume.assert_not_called()
        assert chain.urls()[0] == f'?mode=fresh&from={S1}'

    def test_the_resume_is_zfs_send_t_with_no_flags(self, resumed_s2, mocker):
        chain = Chain(resumed_s2, mocker, sessions=6, negotiate=S2)
        chain.run(common=S1)
        call = resumed_s2.send_resume.call_args
        assert call.args == (TOKEN_INCR,) and call.kwargs == {}


class TestResumeBehindANewerAnchor:
    def test_the_pin_stays_after_the_resume_and_the_next_step_replaces_and_clears_it(self, h, mocker):
        h.rm.get(h.url, [{'json': {'resume_token': TOKEN_FULL}}, {'json': {'resume_token': None}}])
        h.decodes_to(toguid=222, fromguid=111)
        h.get_anchor.return_value = NEW                       # already ahead of the resumed MID
        chain = Chain(h, mocker)
        assert chain.run() is True
        assert h.order == [('pin', OLD, MID), 'send-t', ('pin', MID, NEW), 'send']
        # resume: anchor kept, pin kept.  next step: anchor written (NEW), pin cleared once
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', NEW)]
        assert h.clear_inflight.call_count == 1


class TestStopCheckedBeforeEachStream:
    """The last look before `zfs send` is spawned (never during the stream)."""

    def test_a_stop_before_a_fresh_send_starts_nothing(self, h, mocker, caplog):
        chain = Chain(h, mocker)
        with caplog.at_level(logging.INFO):
            assert chain.run(should_stop=lambda: True) is False
        h.nothing_started()
        assert 'stop requested; not starting a send of pool/data' in caplog.text

    def test_a_stop_before_a_resume_starts_nothing_and_writes_no_pin(self, h, caplog):
        h.token()
        with caplog.at_level(logging.INFO):
            assert h.start(should_stop=lambda: True) is None
        h.nothing_started()
        assert 'stop requested; not resuming pool/data' in caplog.text

    def test_a_stop_after_a_rejected_token_does_not_fall_through_to_a_fresh_send(self, h, mocker, caplog):
        h.token()
        h.decodes_to(toguid=999)
        chain = Chain(h, mocker)
        with caplog.at_level(logging.INFO):
            assert chain.run(should_stop=h.stopper(False, True)) is False
        h.nothing_started()
        assert messages(caplog, 'Not resuming pool/data', 'WARNING')

    def test_a_stop_after_zfs_send_t_failed_to_start_does_not_start_a_fresh_one(self, h, mocker):
        h.token()
        h.send_resume.side_effect = Exception('bad')
        chain = Chain(h, mocker)
        assert chain.run(should_stop=h.stopper(False, False, True)) is False
        h.send_snapshot.assert_not_called()

    def test_no_stop_starts_normally(self, h):
        assert h.start(should_stop=lambda: False).mode == 'fresh'
        h.clear_inflight.assert_not_called()

    def test_the_resume_decision_asks_once(self, h):
        stop = h.stopper()
        h.start(should_stop=stop)
        assert stop.polls() == 0                       # no token: nothing to resume, nothing asked
        h.token()
        h.start(should_stop=stop)
        assert stop.polls() == 1


class TestAlreadyOnTheServer:
    """`common == latest`: nothing to send. Settled before the resume-token fetch, any
    pin, any `zfs send` and the dry-run notice (`zfs send -i X X` is refused by zfs and
    the pin would never be cleared)."""

    def run(self, h, latest=None, **kw):
        latest = latest or h.latest
        return h.rbm._transfer(h.transport, 'client1', h.dsi, latest, latest, 'offsite', snapshots=h.snaps, **kw)

    def assert_nothing_started(self, h, mocker_ws=None):
        h.nothing_started()
        h.resume_info.assert_not_called()
        h.zfs_list.assert_not_called()
        h.send_snapshot.assert_not_called()
        assert not h.token_gets()      # no resume-token fetch

    def test_nothing_is_sent_fetched_pinned_or_decoded(self, h, mocker, caplog):
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        with caplog.at_level(logging.DEBUG):
            assert self.run(h) is True
        self.assert_nothing_started(h)
        ws_cls.assert_not_called()
        assert [r.levelname for r in messages(
            caplog, f'Nothing to send for pool/data -> offsite: {NEW} is already on the server')] == ['DEBUG']
        assert not [r for r in caplog.records if r.levelno >= logging.INFO]
        assert 'Backup complete' not in caplog.text and 'Sending' not in caplog.text

    def test_it_is_the_name_that_matters_not_the_object(self, h, mocker):
        """`backup_dataset` resolves `common` from the listing it negotiated with, but a
        re-listing yields fresh objects: equal names are enough."""
        mocker.patch('zfsbackup.remote.websocket.WebSocket')
        again = sinfo(NEW)
        assert again is not h.latest
        assert h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, again, 'offsite', snapshots=h.snaps) is True
        self.assert_nothing_started(h)

    def test_a_stale_server_token_is_not_even_looked_at(self, h, mocker):
        h.token()                                                 # a server holding a partial of something older
        mocker.patch('zfsbackup.remote.websocket.WebSocket')
        assert self.run(h) is True
        self.assert_nothing_started(h)

    def test_it_is_decided_before_the_stop_check(self, h):
        stop = MagicMock(return_value=True)
        assert self.run(h, should_stop=stop) is True
        stop.assert_not_called()

    def test_a_different_common_snapshot_still_sends(self, h, mocker):
        chain = Chain(h, mocker, sessions=1)
        assert h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, h.snaps[1], 'offsite', snapshots=h.snaps) is True
        assert chain.ws_cls.call_count == 1
        h.send_snapshot.assert_called_once()

    def test_no_common_snapshot_still_sends_in_full(self, h, mocker):
        chain = Chain(h, mocker, sessions=1)
        assert h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, None, 'offsite', snapshots=h.snaps) is True
        assert chain.urls() == ['?mode=fresh']

    # -- the anchor ---------------------------------------------------------------

    def test_a_missing_anchor_is_written(self, h):
        h.get_anchor.return_value = None
        self.run(h)
        h.set_anchor.assert_called_once()
        assert h.set_anchor.call_args.args[1:] == ('offsite', NEW)

    def test_an_anchor_that_is_already_right_is_not_rewritten(self, h):
        h.get_anchor.return_value = NEW
        self.run(h)
        h.set_anchor.assert_not_called()

    def test_an_anchor_on_an_older_snapshot_is_moved_forward(self, h):
        h.get_anchor.return_value = OLD
        self.run(h)
        assert h.set_anchor.call_args.args[1:] == ('offsite', NEW)

    def test_an_anchor_naming_an_unknown_snapshot_is_replaced(self, h):
        h.get_anchor.return_value = 'autosnap_20990101000000'
        self.run(h)
        assert h.set_anchor.call_args.args[1:] == ('offsite', NEW)

    def test_an_anchor_on_a_newer_managed_snapshot_is_never_moved_backwards(self, h):
        h.get_anchor.return_value = NEW
        self.run(h, latest=h.snaps[1])                            # latest == common == MID, anchor already NEW
        h.set_anchor.assert_not_called()

    def test_the_anchor_is_read_for_the_destination(self, h):
        self.run(h)
        assert h.get_anchor.call_args.args[1] == 'offsite'

    # -- the in-flight pin ----------------------------------------------------------

    def test_a_stale_pin_is_cleared_once_the_anchor_is_written(self, h):
        h.get_inflight.return_value = (OLD, MID)
        self.run(h)
        h.set_anchor.assert_called_once()
        h.clear_inflight.assert_called_once()
        assert h.clear_inflight.call_args.args[1] == 'offsite'

    def test_a_stale_pin_is_cleared_when_the_anchor_was_already_right(self, h):
        h.get_anchor.return_value = NEW
        h.get_inflight.return_value = (OLD, NEW)
        self.run(h)
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_called_once()

    def test_a_stale_pin_is_cleared_when_a_newer_anchor_was_left_alone(self, h):
        h.get_anchor.return_value = NEW
        h.get_inflight.return_value = (OLD, MID)
        self.run(h, latest=h.snaps[1])
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_called_once()

    def test_the_pin_is_kept_when_the_anchor_write_fails(self, h):
        h.set_anchor.return_value = False
        h.get_inflight.return_value = (OLD, MID)
        self.run(h)
        h.set_anchor.assert_called_once()
        h.clear_inflight.assert_not_called()

    def test_no_pin_means_nothing_to_clear(self, h):
        h.get_inflight.return_value = None
        self.run(h)
        h.clear_inflight.assert_not_called()

    # -- dry run -------------------------------------------------------------------

    def test_a_dry_run_writes_nothing_and_does_not_announce_a_send(self, h, mocker, caplog):
        h.rbm.config.dry_run = True
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        h.get_inflight.return_value = (OLD, MID)
        with caplog.at_level(logging.DEBUG):
            assert self.run(h) is True
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_not_called()
        h.get_anchor.assert_not_called()
        h.get_inflight.assert_not_called()
        ws_cls.assert_not_called()
        self.assert_nothing_started(h)
        assert 'DRY RUN' not in caplog.text and 'Would send' not in caplog.text

    def test_a_dry_run_with_something_to_send_still_announces_it(self, h):
        h.rbm.config.dry_run = True
        ok = h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, h.snaps[1], 'offsite', snapshots=h.snaps)
        assert ok is True
        h.nothing_started()

    # -- through the callers ------------------------------------------------------------

    def test_after_a_resume_a_follow_up_that_finds_latest_already_there_just_settles(self, h, mocker):
        h.token()
        h.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h, mocker, negotiate=NEW)                   # the server now holds even NEW
        assert chain.run() is True
        assert chain.ws_cls.call_count == 1 and chain.urls() == [f'?mode=resume&from={OLD}']
        assert len(h.token_gets()) == 1       # no second token fetch
        assert h.order == [('pin', OLD, MID), 'send-t']                                # no second pin or send
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID), ('offsite', NEW)]
        h.send_snapshot.assert_not_called()

    def test_backup_dataset_with_the_newest_snapshot_already_on_the_server(self, h, mocker):
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        mocker.patch.object(h.rbm, '_negotiate', return_value=NEW)
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is True
        ws_cls.assert_not_called()
        self.assert_nothing_started(h)
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', NEW)]


class TestBackupDatasetPlumbing:
    """`backup_dataset(dsi, remote_cfg, should_stop=None)` through register,
    negotiate and the resume decision."""

    @pytest.fixture
    def wired(self, h, mocker):
        h.rm.post(re.compile(r'.*/negotiate$'), json={'common_snapshot': OLD})
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=OLD)
        return h, chain, h.dsi.config.remote[0]

    def test_a_normal_cycle_sends_every_newer_snapshot_from_the_negotiated_one(self, wired):
        h, chain, cfg = wired
        assert h.rbm.backup_dataset(h.dsi, cfg) is True
        assert chain.urls() == [f'?mode=fresh&from={OLD}', f'?mode=fresh&from={MID}']
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID), ('offsite', NEW)]

    def test_the_send_mode_and_the_common_snapshot_reach_transfer(self, wired, mocker):
        h, chain, cfg = wired
        spy = mocker.spy(h.rbm, '_transfer')
        h.rbm.backup_dataset(h.dsi, cfg)
        assert spy.call_args.kwargs['mode'] == 'plain'
        assert spy.call_args.args[4].name == OLD

    def test_should_stop_is_checked_before_any_send_or_token_request(self, wired):
        h, chain, cfg = wired
        assert h.rbm.backup_dataset(h.dsi, cfg, should_stop=lambda: True) is False
        assert not h.token_gets()
        chain.ws_cls.assert_not_called()
        h.nothing_started()

    def test_a_resumable_token_is_resumed_through_backup_dataset(self, h, mocker):
        h.rm.post(re.compile(r'.*/negotiate$'), json={'common_snapshot': OLD})
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        chain = Chain(h, mocker, sessions=3, negotiate=[OLD, MID])
        cfg = h.dsi.config.remote[0]
        h.rm.get(h.url, [{'json': {'resume_token': TOKEN_FULL}}, {'json': {'resume_token': None}}])
        h.decodes_to(toguid=222, fromguid=111)
        assert h.rbm.backup_dataset(h.dsi, cfg, should_stop=lambda: False) is True
        # the interrupted OLD -> MID first, then (one re-negotiate) the chain on to NEW
        assert chain.urls() == [f'?mode=resume&from={OLD}', f'?mode=fresh&from={MID}']
        assert chain.negotiate.call_count == 2

    def test_a_skip_from_the_resume_token_is_false_and_leaves_everything(self, wired):
        h, chain, cfg = wired
        h.rm.get(h.url, status_code=500)
        assert h.rbm.backup_dataset(h.dsi, cfg) is False
        chain.ws_cls.assert_not_called()
        h.nothing_started()
        h.set_anchor.assert_not_called()


class TestTransfer:
    """`_transfer` with the start of the stream stubbed: `_try_resume` falls through and
    `_start_fresh` hands out a given stream for the step it is asked for."""

    @pytest.fixture(autouse=True)
    def _no_local_zfs(self, mocker):
        mocker.patch.object(DatasetManager, 'get_anchor', return_value=None)
        mocker.patch.object(DatasetManager, 'set_inflight', return_value=True)
        mocker.patch.object(DatasetManager, 'clear_inflight', return_value=True)

    def _make_rbm(self, tmp_path):
        config = _make_config()
        manager = DatasetManager(config)
        return _rbm(config, manager), manager.datasets[0]

    def _make_snap(self, name='autosnap_20240101120000'):
        return sinfo(name)

    def _stub_start(self, mocker, rbm, stream):
        """`_try_resume` says "nothing to resume"; `_start_fresh` returns `stream` for the step."""
        mocker.patch.object(rbm, '_try_resume', return_value=('fresh', None))
        return mocker.patch.object(
            rbm, '_start_fresh',
            side_effect=lambda dsi, dest, prev, step, mode: _SendStream(stream, 'fresh', False, step, prev),
        )

    def test_returns_false_when_stream_is_none(self, tmp_path, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        mocker.patch.object(rbm, '_try_resume', return_value=('fresh', None))
        mocker.patch.object(rbm, '_start_fresh', return_value=None)
        mocker.patch.object(rbm.local_manager, 'list_snapshots', return_value=[self._make_snap()])
        latest = self._make_snap()
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is False

    def test_a_skipped_resume_starts_nothing(self, tmp_path, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        mocker.patch.object(rbm, '_try_resume', return_value=('skip', None))
        start = mocker.patch.object(rbm, '_start_fresh')
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        latest = self._make_snap()
        assert rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest]) is False
        start.assert_not_called()
        ws_cls.assert_not_called()

    def test_dry_run_returns_true_without_websocket(self, tmp_path, mocker):
        config = _make_config(dry_run=True)
        manager = DatasetManager(config)
        rbm = _rbm(config, manager)
        dsi = manager.datasets[0]

        try_resume = mocker.patch.object(rbm, '_try_resume')
        start = mocker.patch.object(rbm, '_start_fresh')
        mock_ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')

        latest = self._make_snap()
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is True
        mock_ws_cls.assert_not_called()
        try_resume.assert_not_called()
        start.assert_not_called()

    def test_connection_error_returns_false(self, tmp_path, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        mock_stream = MagicMock()
        self._stub_start(mocker, rbm, mock_stream)
        mocker.patch.object(rbm.local_manager, 'list_snapshots', return_value=[self._make_snap()])

        mock_ws = MagicMock()
        mock_ws.connect.side_effect = Exception('connection refused')
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        latest = self._make_snap()
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is False
        mock_ws.close.assert_called()
        mock_stream.close.assert_called_once()                 # the started `zfs send` is reaped

    def test_server_error_response_returns_false(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'data', b'']
        self._stub_start(mocker, rbm, mock_stream)
        mocker.patch.object(rbm.local_manager, 'list_snapshots', return_value=[self._make_snap()])

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'error', 'error': 'disk full'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        latest = self._make_snap()
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is False

    def test_successful_transfer_sets_anchor(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'chunk1', b'chunk2', b'']
        self._stub_start(mocker, rbm, mock_stream)

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        mock_set_anchor = mocker.patch.object(rbm.local_manager, 'set_anchor')

        latest = self._make_snap('autosnap_20240202120000')
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is True
        mock_set_anchor.assert_called_once_with(dsi, 'offsite', 'autosnap_20240202120000')

    def test_successful_transfer_logs_superseded_anchor(self, tmp_path, mocker, caplog):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'']
        self._stub_start(mocker, rbm, mock_stream)

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        mocker.patch.object(rbm.local_manager, 'get_anchor', return_value='autosnap_OLD')
        mocker.patch.object(rbm.local_manager, 'set_anchor')

        latest = self._make_snap('autosnap_20240202120000')
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is True
        assert 'Previous anchor autosnap_OLD superseded by autosnap_20240202120000' in caplog.text

    def test_incremental_transfer_uses_ws_url(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'', ]
        self._stub_start(mocker, rbm, mock_stream)

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)
        mocker.patch.object(rbm.local_manager, 'set_anchor')

        common = self._make_snap('autosnap_20240101120000')
        latest = self._make_snap('autosnap_20240202120000')

        result = rbm._transfer(tr(), 'client1', dsi, latest, common, 'offsite', snapshots=[latest, common])
        assert result is True
        connect_url = mock_ws.connect.call_args[0][0]
        assert connect_url.endswith('/stream?mode=fresh&from=autosnap_20240101120000')


class TestBackupDataset:
    def test_returns_false_when_destination_unknown(self, tmp_path):
        config = _make_config(
            destinations={},
        )
        manager = DatasetManager(config)
        rbm = _rbm(config, manager)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]
        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is False

    def test_returns_true_when_no_snapshots(self, client_tls_dir, requests_mock, mocker):
        config = _make_config()
        manager = DatasetManager(config)
        rbm = _rbm(config, manager, client_tls_dir)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        mocker.patch.object(manager, 'list_snapshots', return_value=[])

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True

    def test_returns_false_when_whoami_fails(self, client_tls_dir, requests_mock, mocker):
        config = _make_config()
        manager = DatasetManager(config)
        rbm = _rbm(config, manager, client_tls_dir)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.get(f'{BASE}/whoami', status_code=500)
        negotiate = requests_mock.post(re.compile(r'.*/negotiate$'), json={})

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is False
        assert negotiate.call_count == 0            # nothing is negotiated for a destination that failed /whoami

    def test_resolves_common_snap_from_negotiate_result(self, client_tls_dir, requests_mock, mocker):
        config = _make_config()
        manager = DatasetManager(config)
        rbm = _rbm(config, manager, client_tls_dir)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))

        client_id = 'client1'
        old_snap_name = 'autosnap_20240101120000'
        new_snap_name = 'autosnap_20240202120000'

        from libzfseasy.types import Filesystem, Snapshot as ZFSSnapshot
        fs = Filesystem('pool/data')
        old_snap = ZFSSnapshot(fs, old_snap_name)
        new_snap = ZFSSnapshot(fs, new_snap_name)

        from zfsbackup.backup_manager import SnapshotInfo
        old_info = SnapshotInfo(old_snap, 'autosnap')
        new_info = SnapshotInfo(new_snap, 'autosnap')

        mocker.patch.object(manager, 'list_snapshots', return_value=[new_info, old_info])

        requests_mock.post(
            f'https://backup.example.com/backup/{client_id}/{dsi.name}/negotiate',
            json={'common_snapshot': old_snap_name, 'server_dataset': 'pool/backups/c/pool/data'},
        )

        # Mock _transfer to avoid WebSocket and stream setup; we're testing the
        # common_snap resolution logic in backup_dataset (line 85).
        captured = {}

        def capture_transfer(transport, cid, d, latest, common_snap, destination, **kwargs):
            captured['transport'] = transport
            captured['cid'] = cid
            captured['common_snap'] = common_snap
            captured['kwargs'] = kwargs
            return True

        mocker.patch.object(rbm, '_transfer', side_effect=capture_transfer)

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True
        assert captured.get('common_snap') is old_info
        # the listing and the stop callback are handed on, not re-read
        assert captured['kwargs']['snapshots'] == [new_info, old_info]
        assert captured['kwargs']['should_stop'] is None
        # the transport is the destination's: its URL, its pinned fingerprint, this host's id in the URLs
        assert captured['transport'].base_url == BASE
        assert captured['transport'].server_fp == SERVER_FP
        assert captured['cid'] == 'client1'

    def test_dry_run_returns_true_without_transfer(self, client_tls_dir, requests_mock, mocker):
        config = _make_config(
            dry_run=True,
        )
        manager = DatasetManager(config)
        rbm = _rbm(config, manager, client_tls_dir)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        requests_mock.post(
            f'https://backup.example.com/backup/client1/{dsi.name}/negotiate',
            json={'common_snapshot': None, 'server_dataset': 'pool/b'},
        )
        requests_mock.get(
            f'https://backup.example.com/backup/client1/{dsi.name}/resume_token',
            json={'resume_token': None},
        )

        send = mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=MagicMock())
        mocker.patch.object(manager, 'list_snapshots', return_value=[sinfo('autosnap_20240101120000')])

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True
        send.assert_not_called()


class TestBackupDatasetNegotiateWiring:
    """`backup_dataset` passes `remote_cfg.destination` into `_negotiate` --
    previously untested: every negotiate test above calls `_negotiate`
    directly with a literal `'offsite'`, and `TestBackupDataset` mocks around
    the real negotiate call, so the one line of production wiring
    (`backup_dataset` -> `_negotiate(..., remote_cfg.destination)`) had no
    coverage. A wrong value here (e.g. `dsi.name` instead of
    `remote_cfg.destination`) would send the wrong destination's rules."""

    def test_backup_dataset_calls_negotiate_with_remote_cfg_destination(
        self, client_tls_dir, requests_mock, mocker
    ):
        # Deliberately use a destination name distinct from dsi.name (and
        # from other declared destinations) so a swap -- e.g. passing
        # dsi.name, or the first destination in the mapping -- is caught.
        config = _make_config(
            datasets=[DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='dc2')],
            )],
            destinations={
                'offsite': Destination(url='https://backup.example.com', server_fingerprint=SERVER_FP),
                'dc2': Destination(url='https://dc2.example.com', server_fingerprint=SERVER_FP),
            },
        )
        manager = DatasetManager(config)
        rbm = _rbm(config, manager, client_tls_dir)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.get('https://dc2.example.com/whoami', json=whoami_json('client1'))
        mocker.patch.object(manager, 'list_snapshots', return_value=[sinfo('autosnap_20240101120000')])
        mock_negotiate = mocker.patch.object(rbm, '_negotiate', return_value=None)
        mocker.patch.object(rbm, '_transfer', return_value=True)

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True
        mock_negotiate.assert_called_once()
        # Positional signature: (transport, client_id, dsi, snapshots, destination)
        assert mock_negotiate.call_args.args[-1] == 'dc2'
        assert mock_negotiate.call_args.args[0].base_url == 'https://dc2.example.com'     # dc2's transport, not offsite's


class TestClientSideRetentionCollisionWarning:
    """Client-side collision warning: previously a colliding *per-destination*
    ruleset warned nowhere on the client -- the only warning appeared in the
    receiving admin's journal, naming a server-side path the client operator
    does not administer, with canonicalized literals. `RemoteBackupManager.
    backup_dataset` now warns locally too, once per `(dataset, destination)`,
    naming the operator's own literals."""

    def _make_rbm(self, tls_dir, retention_rules=None, destination='offsite'):
        config = _make_config(
            datasets=[DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(
                    destination=destination,
                    retention_rules=retention_rules or [],
                )],
            )],
        )
        manager = DatasetManager(config)
        return _rbm(config, manager, tls_dir), manager.datasets[0]

    def _colliding_rules(self):
        # 1h:7d and 1d:1w collide on keep_for -- both are 604800s.
        return [
            RetentionRule(Duration('1h'), Duration('7d')),
            RetentionRule(Duration('1d'), Duration('1w')),
        ]

    def test_warns_once_for_colliding_destination_ruleset(self, client_tls_dir, requests_mock, caplog):
        rbm, dsi = self._make_rbm(client_tls_dir, retention_rules=self._colliding_rules())
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        with patch.object(rbm.local_manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1
        assert warnings[0].message.startswith('Dataset pool/data -> offsite:')

    def test_warning_names_operator_literal_not_canonicalized(self, client_tls_dir, requests_mock, caplog):
        # The message must contain "7d" -- the text the operator wrote --
        # not the numerically-equal-but-different canonicalized "1w". That
        # distinction is the entire point of warning client-side: the
        # server-side message names its own (canonicalized) literals, which
        # this client operator never sees and does not administer.
        rbm, dsi = self._make_rbm(client_tls_dir, retention_rules=self._colliding_rules())
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        with patch.object(rbm.local_manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1
        assert '7d' in warnings[0].message

    def test_does_not_rewarn_same_ruleset(self, client_tls_dir, requests_mock, caplog):
        rbm, dsi = self._make_rbm(client_tls_dir, retention_rules=self._colliding_rules())
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        with patch.object(rbm.local_manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
                rbm.backup_dataset(dsi, dsi.config.remote[0])
                rbm.backup_dataset(dsi, dsi.config.remote[0])
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1

    def test_warns_separately_for_two_different_destinations(self, client_tls_dir, requests_mock, caplog):
        rules = self._colliding_rules()
        config = _make_config(
            datasets=[DatasetConfig(
                name='pool/data',
                remote=[
                    RemoteDatasetConfig(destination='offsite', retention_rules=rules),
                    RemoteDatasetConfig(destination='dc2', retention_rules=rules),
                ],
            )],
            destinations={
                'offsite': Destination(url='https://backup.example.com', server_fingerprint=SERVER_FP),
                'dc2': Destination(url='https://dc2.example.com', server_fingerprint=SERVER_FP),
            },
        )
        manager = DatasetManager(config)
        rbm = _rbm(config, manager, client_tls_dir)
        dsi = manager.datasets[0]

        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        requests_mock.get('https://dc2.example.com/whoami', json=whoami_json('client1'))
        with patch.object(manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
                rbm.backup_dataset(dsi, dsi.config.remote[1])
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 2
        assert any(w.message.startswith('Dataset pool/data -> offsite:') for w in warnings)
        assert any(w.message.startswith('Dataset pool/data -> dc2:') for w in warnings)

    def test_no_collision_produces_no_warning(self, client_tls_dir, requests_mock, caplog):
        rules = [RetentionRule(Duration('1h'), Duration('1d'))]
        rbm, dsi = self._make_rbm(client_tls_dir, retention_rules=rules)
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        with patch.object(rbm.local_manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
        assert [r for r in caplog.records if r.levelname == 'WARNING'] == []


class TestNegotiationFailedAndReload:
    """The server's retryable 503 ("reloading") and every other failure are
    told apart; neither is ever read as "no common snapshot"."""

    URL = 'https://backup.example.com'
    RELOADING = {'error': 'server is reloading; retry later', 'retry': True}

    def _make(self, tmp_path, tls_dir=None):
        config = _make_config()
        manager = DatasetManager(config)
        rbm = _rbm(config, manager, tls_dir)
        dsi = manager.datasets[0]
        return rbm, manager, dsi, dsi.config.remote[0]

    def _post(self, requests_mock, dsi, **kw):
        # any client id: `backup_dataset` uses the manager's own identity
        requests_mock.post(re.compile(rf'{re.escape(self.URL)}/backup/[^/]+/{re.escape(dsi.name)}/negotiate$'), **kw)

    # -- _negotiate ---------------------------------------------------------

    def test_a_retryable_503_is_reloading(self, tmp_path, requests_mock):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, status_code=503, json=self.RELOADING)
        with pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is True

    def test_a_reloading_503_is_not_logged_as_an_error(self, tmp_path, requests_mock, caplog):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, status_code=503, json=self.RELOADING)
        with caplog.at_level(logging.DEBUG), pytest.raises(NegotiationFailed):
            rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    @pytest.mark.parametrize(
        'kwargs',
        [
            {'status_code': 500, 'json': {'error': 'boom'}},
            {'status_code': 503, 'json': {'error': 'remote_backup disabled'}},          # no retry flag
            {'status_code': 503, 'json': {'error': 'x', 'retry': False}},
            {'status_code': 503, 'text': 'Service Unavailable'},                         # not JSON
            {'status_code': 404, 'json': {}},
            {'status_code': 200, 'text': 'not json'},
        ],
    )
    def test_any_other_failure_is_not_reloading(self, tmp_path, requests_mock, caplog, kwargs):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, **kwargs)
        with caplog.at_level(logging.ERROR), pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is False
        assert 'Negotiation failed' in caplog.text

    @pytest.mark.parametrize('exc_type', ['ConnectionError', 'Timeout'])
    def test_a_connection_error_is_not_reloading(self, tmp_path, requests_mock, exc_type):
        import requests
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, exc=getattr(requests, exc_type))
        with pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is False

    def test_a_null_common_snapshot_is_none_not_a_failure(self, tmp_path, requests_mock):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, json={'common_snapshot': None, 'server_dataset': 'x'})
        assert rbm._negotiate(tr(), 'client1', dsi, [], 'offsite') is None

    def test_a_common_snapshot_is_returned(self, tmp_path, requests_mock):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, json={'common_snapshot': 'autosnap_1'})
        assert rbm._negotiate(tr(), 'client1', dsi, [], 'offsite') == 'autosnap_1'

    # -- backup_dataset -----------------------------------------------------

    @pytest.fixture
    def wired(self, tmp_path, requests_mock, mocker, client_tls_dir):
        rbm, manager, dsi, remote_cfg = self._make(tmp_path, client_tls_dir)
        requests_mock.get(f'{self.URL}/whoami', json=whoami_json('client1'))
        mocker.patch.object(manager, 'list_snapshots', return_value=[sinfo('autosnap_20240101120000')])
        sends = {
            'transfer': mocker.spy(rbm, '_transfer'),
            'resume': mocker.spy(rbm, '_try_resume'),
            # A bare MagicMock stream's `.read()` is always truthy: a URL
            # mismatch that fell through to a real send would loop forever.
            'send_snapshot': mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=_finite_stream()),
            'send_resume': mocker.patch('zfsbackup.remote.zfs.send.resume', return_value=_finite_stream()),
        }
        return rbm, dsi, remote_cfg, sends

    def assert_no_send(self, sends):
        for name, mock in sends.items():
            assert not mock.called, f'{name} was called after a failed negotiation'

    def test_reloading_logs_info_returns_false_and_never_sends(self, wired, requests_mock, caplog):
        rbm, dsi, remote_cfg, sends = wired
        self._post(requests_mock, dsi, status_code=503, json=self.RELOADING)
        with caplog.at_level(logging.INFO):
            assert rbm.backup_dataset(dsi, remote_cfg) is False
        infos = [r for r in caplog.records if r.levelno == logging.INFO
                 and r.getMessage() == f'server {self.URL} is reloading; skipping {dsi.name} this cycle']
        assert len(infos) == 1
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        self.assert_no_send(sends)

    def test_another_failure_logs_an_error_returns_false_and_never_sends(self, wired, requests_mock, caplog):
        rbm, dsi, remote_cfg, sends = wired
        self._post(requests_mock, dsi, status_code=500, json={'error': 'boom'})
        with caplog.at_level(logging.INFO):
            assert rbm.backup_dataset(dsi, remote_cfg) is False
        assert [r for r in caplog.records if r.levelno == logging.ERROR and 'Negotiation failed' in r.getMessage()]
        assert 'is reloading' not in caplog.text
        self.assert_no_send(sends)

    def test_a_connection_error_never_sends(self, wired, requests_mock):
        import requests
        rbm, dsi, remote_cfg, sends = wired
        self._post(requests_mock, dsi, exc=requests.ConnectionError)
        assert rbm.backup_dataset(dsi, remote_cfg) is False
        self.assert_no_send(sends)

    def test_a_null_common_snapshot_still_does_the_full_initial_send(self, wired, requests_mock, mocker):
        rbm, dsi, remote_cfg, sends = wired
        self._post(requests_mock, dsi, json={'common_snapshot': None})
        requests_mock.get(re.compile(r'.*/resume_token$'), json={'resume_token': None})
        ws = MagicMock()
        ws.recv.return_value = '{"status": "ok"}'
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=ws)
        stream = MagicMock()
        stream.read.side_effect = [b'x', b'']
        sends['send_snapshot'].return_value = stream
        mocker.patch.object(rbm.local_manager, 'set_anchor')
        assert rbm.backup_dataset(dsi, remote_cfg) is True
        sends['transfer'].assert_called_once()
        assert sends['transfer'].call_args.args[4] is None            # no common snapshot: full send
        sends['send_snapshot'].assert_called_once()
        assert 'since' not in sends['send_snapshot'].call_args.kwargs

    def test_a_reloading_whoami_is_false_with_an_info_log_and_goes_no_further(self, wired, requests_mock, caplog):
        rbm, dsi, remote_cfg, sends = wired
        requests_mock.get(f'{self.URL}/whoami', status_code=503, json=self.RELOADING)
        negotiate = requests_mock.post(re.compile(r'.*/negotiate$'), json={})
        with caplog.at_level(logging.INFO):
            assert rbm.backup_dataset(dsi, remote_cfg) is False
        assert [r.levelname for r in caplog.records if r.getMessage() == (
            f'server {self.URL} is reloading; destination offsite skipped this cycle')] == ['INFO']
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert negotiate.call_count == 0
        self.assert_no_send(sends)

    def test_a_non_retry_whoami_503_is_an_error_not_a_reload(self, wired, requests_mock, caplog):
        rbm, dsi, remote_cfg, sends = wired
        requests_mock.get(f'{self.URL}/whoami', status_code=503, json={'error': 'remote_backup disabled'})
        negotiate = requests_mock.post(re.compile(r'.*/negotiate$'), json={})
        with caplog.at_level(logging.INFO):
            assert rbm.backup_dataset(dsi, remote_cfg) is False
        assert 'is reloading' not in caplog.text
        assert [r.levelname for r in caplog.records if 'unexpected /whoami answer' in r.getMessage()
                and 'HTTP 503' in r.getMessage()] == ['ERROR']
        assert negotiate.call_count == 0
        self.assert_no_send(sends)

    # -- resume_token -------------------------------------------------------

    def test_a_reloading_resume_token_skips_the_dataset_without_a_fresh_send(self, tmp_path, requests_mock, mocker, caplog):
        rbm, manager, dsi, _ = self._make(tmp_path)
        send_snapshot = mocker.patch('zfsbackup.remote.zfs.send.snapshot')
        send_resume = mocker.patch('zfsbackup.remote.zfs.send.resume')
        requests_mock.get(
            f'{self.URL}/backup/client1/{dsi.name}/resume_token', status_code=503, json=self.RELOADING
        )
        latest = sinfo('autosnap_20240101120000')
        with caplog.at_level(logging.INFO):
            assert rbm._try_resume(tr(), 'client1', dsi, 'offsite', [latest], 'plain') == ('skip', None)
        assert f'server {self.URL} is reloading; skipping {dsi.name} this cycle' in caplog.text
        send_snapshot.assert_not_called()
        send_resume.assert_not_called()

    def test_a_reloading_resume_token_makes_transfer_false(self, tmp_path, requests_mock, mocker):
        rbm, manager, dsi, _ = self._make(tmp_path)
        send_snapshot = mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=_finite_stream())
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        requests_mock.get(
            f'{self.URL}/backup/client1/{dsi.name}/resume_token', status_code=503, json=self.RELOADING
        )
        latest = sinfo('autosnap_20240101120000')
        assert rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest]) is False
        send_snapshot.assert_not_called()
        ws_cls.assert_not_called()

    def test_an_ordinary_resume_token_failure_skips_the_cycle_instead_of_sending_fresh(
        self, tmp_path, requests_mock, mocker, caplog
    ):
        """Old best-effort behaviour (fall through to a fresh send) would make the
        server discard its partial progress: see TestFetchResumeToken."""
        rbm, manager, dsi, _ = self._make(tmp_path)
        send_snapshot = mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=_finite_stream())
        requests_mock.get(f'{self.URL}/backup/client1/{dsi.name}/resume_token', status_code=500)
        latest = sinfo('autosnap_20240101120000')
        with caplog.at_level(logging.INFO):
            assert rbm._try_resume(tr(), 'client1', dsi, 'offsite', [latest], 'plain') == ('skip', None)
        send_snapshot.assert_not_called()
        assert [r.levelname for r in caplog.records if 'HTTP 500' in r.getMessage()] == ['WARNING']


class TestUrlPathEncoding:
    """Item 3a: client ids and dataset names are percent-encoded in URL paths
    (`/` and `:` stay literal in a path; `?from=` encodes everything).

    `requests` itself would turn a raw space into `%20`, so these tests capture
    the URL handed to `requests`/`websocket` instead of going through a real
    request, where an unencoded `%`/`#`/`?` would otherwise pass unnoticed."""

    BASE = 'https://backup.example.com'

    def _rbm(self, tmp_path, name='pool/my data'):
        config = _make_config(
            datasets=[DatasetConfig(name=name, remote=[RemoteDatasetConfig(destination='offsite')])],
            )
        manager = DatasetManager(config)
        return _rbm(config, manager), manager.datasets[0]

    @pytest.mark.parametrize('name, client, quoted', [
        ('pool/my data', 'client 1', 'client%201/pool/my%20data'),
        ('pool/data', 'client1', 'client1/pool/data'),
        ('pool/a:b', 'host-abc12345', 'host-abc12345/pool/a:b'),
        ('pool/data', 'c#d?e%f', 'c%23d%3Fe%25f/pool/data'),
        ('pool/dot.ted-and_under', 'my.host-1', 'my.host-1/pool/dot.ted-and_under'),
        ('pool/data', 'hôst-1', 'h%C3%B4st-1/pool/data'),
    ])
    def test_negotiate_url(self, tmp_path, mocker, name, client, quoted):
        rbm, dsi = self._rbm(tmp_path, name)
        session = MagicMock()
        session.post.return_value.json.return_value = {'common_snapshot': None}
        session.post.return_value.status_code = 200
        rbm._negotiate(make_transport(self.BASE, session=session), client, dsi, [], 'offsite')
        assert session.post.call_args.args[0] == f'{self.BASE}/backup/{quoted}/negotiate'

    @pytest.mark.parametrize('name, client, quoted', [
        ('pool/my data', 'client 1', 'client%201/pool/my%20data'),
        ('pool/data', 'client1', 'client1/pool/data'),
        ('pool/a:b', 'c', 'c/pool/a:b'),
    ])
    def test_resume_token_url(self, tmp_path, mocker, name, client, quoted):
        rbm, dsi = self._rbm(tmp_path, name)
        session = MagicMock()
        session.get.return_value.ok = True
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {'resume_token': None}
        status, stream = rbm._try_resume(
            make_transport(self.BASE, session=session), client, dsi, 'offsite', [], 'plain'
        )
        assert (status, stream) == ('fresh', None)               # no token: nothing to resume
        assert session.get.call_args.args[0] == f'{self.BASE}/backup/{quoted}/resume_token'

    def _stream_url(self, tmp_path, mocker, name, client, common, base=None, mode='fresh'):
        rbm, dsi = self._rbm(tmp_path, name)
        latest = sinfo(NEW)
        # (a name no real snapshot may have: only `.name` and an older `createtxg` matter here)
        common_snap = None if common is None else types.SimpleNamespace(
            name=common, createtxg=1, timestamp=None, guid=None)
        if mode == 'resume':
            mocker.patch.object(rbm, '_try_resume', return_value=(
                'resume', _SendStream(_finite_stream(), 'resume', False, latest, common_snap)))
        else:
            mocker.patch.object(rbm, '_try_resume', return_value=('fresh', None))
            mocker.patch.object(rbm, '_start_fresh', return_value=_SendStream(
                _finite_stream(), 'fresh', False, latest, common_snap))
        mocker.patch.object(rbm.local_manager, 'get_anchor', return_value=None)
        mocker.patch.object(rbm.local_manager, 'set_anchor', return_value=True)
        ws = MagicMock()
        ws.recv.return_value = '{"status": "ok"}'
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=ws)
        assert rbm._transfer(make_transport(base or self.BASE), client, dsi, latest, common_snap, 'offsite',
                             snapshots=[latest] + ([common_snap] if common_snap else [])) is True
        return ws.connect.call_args.args[0]

    def test_stream_url_full_send(self, tmp_path, mocker):
        url = self._stream_url(tmp_path, mocker, 'pool/my data', 'client 1', None)
        assert url == 'wss://backup.example.com/backup/client%201/pool/my%20data/stream?mode=fresh'

    def test_stream_url_incremental_encodes_the_snapshot_name_fully(self, tmp_path, mocker):
        url = self._stream_url(tmp_path, mocker, 'pool/my data', 'client 1', 'snap:one two/x')
        assert url == (
            'wss://backup.example.com/backup/client%201/pool/my%20data/stream'
            '?mode=fresh&from=snap%3Aone%20two%2Fx'
        )

    def test_a_plain_snapshot_name_is_unchanged(self, tmp_path, mocker):
        url = self._stream_url(tmp_path, mocker, 'pool/data', 'client1', 'autosnap_20240101120000')
        assert url == (
            'wss://backup.example.com/backup/client1/pool/data/stream'
            '?mode=fresh&from=autosnap_20240101120000'
        )

    def test_the_websocket_is_always_wss_the_scheme_is_never_rewritten_from_the_url_text(self, tmp_path, mocker):
        """There is no plaintext path: a destination under a sub-path and a non-default port stays
        `wss://` with its path and port, and the websocket connects over the pinned socket."""
        url = self._stream_url(tmp_path, mocker, 'pool/my data', 'c', None,
                               base='https://backup.example.com:8443/zb')
        assert url == 'wss://backup.example.com:8443/zb/backup/c/pool/my%20data/stream?mode=fresh'

    def test_the_query_value_round_trips(self, tmp_path, mocker):
        from urllib.parse import parse_qs, urlsplit
        url = self._stream_url(tmp_path, mocker, 'pool/data', 'c', 'a b:c&d=e#f')
        assert parse_qs(urlsplit(url).query) == {'mode': ['fresh'], 'from': ['a b:c&d=e#f']}

    def test_a_resume_url_says_mode_resume(self, tmp_path, mocker):
        url = self._stream_url(tmp_path, mocker, 'pool/my data', 'client 1', 'autosnap_20240101120000',
                               mode='resume')
        assert url == (
            'wss://backup.example.com/backup/client%201/pool/my%20data/stream'
            '?mode=resume&from=autosnap_20240101120000'
        )

    def test_path_helper_keeps_slash_and_colon_only(self):
        from zfsbackup.remote import _path
        assert _path('pool/a:b c') == 'pool/a:b%20c'
        assert _path('a#b?c%d&e') == 'a%23b%3Fc%25d%26e'
        assert _path('plain-name_1.x') == 'plain-name_1.x'


class TestUrlRoundTripThroughTheServer:
    """The quoted negotiate URL the client builds is decoded by the API's
    routes back to the original client id and dataset name."""

    def test_the_client_quoted_negotiate_url_reaches_the_server_decoded(self, tmp_path, mocker):
        from urllib.parse import urlsplit
        from _api_support import Authed
        from zfsbackup.config import RemoteServerConfig

        server_config = _make_config(
            datasets=[DatasetConfig(name='pool/srv')],
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
            )
        # the server binds this client's certificate to the id in the URL (plan D5)
        web = Authed(server_config, client_id='client-1').client()
        exists = mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        zset = mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        mocker.patch('zfsbackup.api.read_zfsbackup_properties', return_value={})

        config = _make_config(
            datasets=[DatasetConfig(name='pool/my data', remote=[RemoteDatasetConfig(destination='offsite')])],
            )
        manager = DatasetManager(config)
        rbm = _rbm(config, manager)
        dsi = manager.datasets[0]

        def deliver(url, json=None, timeout=None):
            path = urlsplit(url).path
            assert path == '/backup/client-1/pool/my%20data/negotiate'
            resp = web.post(path, json=json)       # the server decodes the percent-escapes
            fake = MagicMock()
            fake.status_code = resp.status_code
            fake.ok = resp.status_code < 400
            fake.json.return_value = resp.get_json()
            return fake

        session = MagicMock()
        session.post.side_effect = deliver
        snaps = [MagicMock()]
        snaps[0].name = 'autosnap_20240101120000'
        assert rbm._negotiate(
            make_transport(BASE, session=session), 'client-1', dsi, snaps, 'offsite'
        ) is None

        server_fs = exists.call_args.args[0]
        assert server_fs.name == 'pool/backups/client-1/my data'
        props = zset.call_args.args[1]
        assert props['org.zfsbackup:client_id'] == 'client-1'
        assert props['org.zfsbackup:source_dataset'] == 'pool/my data'


class TestNegotiateConflict:
    """D7: the server refuses to mix this dataset into one held for another source."""

    URL_RE = re.compile(r'.*/negotiate$')

    def rbm_dsi(self, tmp_path, tls_dir=None):
        config = _make_config()
        manager = DatasetManager(config)
        return _rbm(config, manager, tls_dir), manager.datasets[0]

    @staticmethod
    def own(caplog):
        """The records of `zfsbackup.remote` only (urllib3 logs its own DEBUG lines)."""
        return [r for r in caplog.records if r.name == 'zfsbackup.remote']

    def refused(self, rbm, dsi):
        with pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is False
        return exc.value

    def test_a_409_carries_the_servers_reason(self, tmp_path, requests_mock):
        rbm, dsi = self.rbm_dsi(tmp_path)
        requests_mock.post(self.URL_RE, status_code=409,
                           json={'error': 'pool/backups/c1/data already holds another source'})
        err = self.refused(rbm, dsi)
        assert 'already holds another source' in str(err)

    def test_a_409_without_json_uses_the_body_text(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self.rbm_dsi(tmp_path)
        requests_mock.post(self.URL_RE, status_code=409, text='conflict!')
        with caplog.at_level(logging.DEBUG):
            assert str(self.refused(rbm, dsi)) == 'conflict!'
        assert [(r.levelname, r.getMessage()) for r in self.own(caplog)] == [
            ('ERROR', 'server refused pool/data: conflict!')]

    def test_a_409_never_leads_to_a_send(self, tmp_path, client_tls_dir, requests_mock, mocker):
        rbm, dsi = self.rbm_dsi(tmp_path, client_tls_dir)
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        requests_mock.post(self.URL_RE, status_code=409, json={'error': 'no'})
        mocker.patch.object(rbm.local_manager, 'list_snapshots', return_value=[sinfo(NEW)])
        started = mocker.patch.object(rbm, '_try_resume')
        assert rbm.backup_dataset(dsi, dsi.config.remote[0]) is False
        started.assert_not_called()

    def test_the_first_refusal_is_an_error_with_the_exact_text(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self.rbm_dsi(tmp_path)
        requests_mock.post(self.URL_RE, status_code=409, json={'error': 'pool/backups/c1/data is held for pool/other'})
        with caplog.at_level(logging.DEBUG):
            self.refused(rbm, dsi)
        assert [(r.levelname, r.getMessage()) for r in self.own(caplog)] == [
            ('ERROR', 'server refused pool/data: pool/backups/c1/data is held for pool/other')]

    def test_the_same_refusal_repeats_at_debug(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self.rbm_dsi(tmp_path)
        requests_mock.post(self.URL_RE, status_code=409, json={'error': 'held elsewhere'})
        with caplog.at_level(logging.DEBUG):
            for _ in range(4):
                self.refused(rbm, dsi)
        assert [r.levelname for r in self.own(caplog)] == ['ERROR', 'DEBUG', 'DEBUG', 'DEBUG']
        assert len({r.getMessage() for r in self.own(caplog)}) == 1

    def test_a_different_message_is_an_error_again(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self.rbm_dsi(tmp_path)
        with caplog.at_level(logging.DEBUG):
            requests_mock.post(self.URL_RE, status_code=409, json={'error': 'one'})
            self.refused(rbm, dsi)
            requests_mock.post(self.URL_RE, status_code=409, json={'error': 'two'})
            self.refused(rbm, dsi)
            self.refused(rbm, dsi)
        assert [r.levelname for r in self.own(caplog)] == ['ERROR', 'ERROR', 'DEBUG']

    def test_a_successful_negotiate_resets_it(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self.rbm_dsi(tmp_path)
        with caplog.at_level(logging.DEBUG):
            requests_mock.post(self.URL_RE, status_code=409, json={'error': 'held elsewhere'})
            self.refused(rbm, dsi)
            requests_mock.post(self.URL_RE, json={'common_snapshot': None})
            assert rbm._negotiate(tr(), 'client1', dsi, [], 'offsite') is None
            requests_mock.post(self.URL_RE, status_code=409, json={'error': 'held elsewhere'})
            self.refused(rbm, dsi)
        assert [r.levelname for r in self.own(caplog)] == ['ERROR', 'ERROR']

    def test_other_failures_in_between_do_not_reset_it(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self.rbm_dsi(tmp_path)
        with caplog.at_level(logging.DEBUG):
            requests_mock.post(self.URL_RE, status_code=409, json={'error': 'held elsewhere'})
            self.refused(rbm, dsi)
            requests_mock.post(self.URL_RE, status_code=503, json={'error': 'x', 'retry': True})
            with pytest.raises(NegotiationFailed):
                rbm._negotiate(tr(), 'client1', dsi, [], 'offsite')
            requests_mock.post(self.URL_RE, status_code=409, json={'error': 'held elsewhere'})
            self.refused(rbm, dsi)
        assert [r.levelname for r in messages(caplog, 'server refused')] == ['ERROR', 'DEBUG']

    def test_it_is_tracked_per_dataset_and_destination(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self.rbm_dsi(tmp_path)
        requests_mock.post(self.URL_RE, status_code=409, json={'error': 'held elsewhere'})
        with caplog.at_level(logging.DEBUG):
            self.refused(rbm, dsi)
            with pytest.raises(NegotiationFailed):
                rbm._negotiate(tr(), 'client1', dsi, [], 'second-destination')
        assert [r.levelname for r in self.own(caplog)] == ['ERROR', 'ERROR']

    def test_backup_dataset_cycles_log_the_refusal_once(self, tmp_path, client_tls_dir, requests_mock, mocker, caplog):
        rbm, dsi = self.rbm_dsi(tmp_path, client_tls_dir)
        requests_mock.get(f'{BASE}/whoami', json=whoami_json('client1'))
        requests_mock.post(self.URL_RE, status_code=409, json={'error': 'held elsewhere'})
        mocker.patch.object(rbm.local_manager, 'list_snapshots', return_value=[sinfo(NEW)])
        started = mocker.patch.object(rbm, '_try_resume')
        with caplog.at_level(logging.DEBUG):
            for _ in range(3):
                assert rbm.backup_dataset(dsi, dsi.config.remote[0]) is False
        assert [r.levelname for r in messages(caplog, 'server refused pool/data: held elsewhere')] == [
            'ERROR', 'DEBUG', 'DEBUG']
        assert not messages(caplog, 'Negotiation failed')
        started.assert_not_called()


# ===========================================================================
# S1-d (table T-S1): every newest/older/newer comparison on the client follows
# `createtxg`, not the name timestamp.
#
# The fixtures below make the two orders DISAGREE: the snapshot named OLD
# (20240101) was created last (txg 900), NEW (20240103) first (txg 10). A
# comparison by name would give the opposite answer in every test.
# ===========================================================================

from _snapshot_support import listed_snapshot  # noqa: E402
from zfsbackup.backup_manager import snapshot_newer  # noqa: E402

#: name -> (guid, txg): names ascend OLD < MID < NEW, creation order is the reverse.
INVERTED = {OLD: (111, 900), MID: (222, 20), NEW: (333, 10)}


def inverted_snaps(h, spec=None):
    """`h.snaps` / `h.latest` for `spec` ({name: (guid, txg)}), newest first by txg as the manager
    delivers them."""
    spec = spec or INVERTED
    snaps = [sinfo(n, guid=g, txg=t) for n, (g, t) in spec.items()]
    snaps.sort(key=lambda s: s.createtxg, reverse=True)
    h.snaps = snaps
    h.latest = snaps[0]
    return snaps


def by_name(snaps, name):
    return next(s for s in snaps if s.name == name)


class TestAnchorNotMovedBackwardsFollowsTxg:
    """`_push_step`: a step older than the current anchor leaves it in place, by `createtxg`.
    Every send is held to it (a resumed snapshot, or a chain step after one)."""

    def resume_to_mid(self, h, mocker, anchor):
        """A resumed MID (txg 20), stopped right after it, with the anchor on `anchor`."""
        inverted_snaps(h)
        h.get_anchor.return_value = anchor
        h.rm.get(h.url, json={'resume_token': TOKEN_FULL})
        h.decodes_to(toguid=222, fromguid=None)                 # a full send of MID resumes
        chain = Chain(h, mocker, sessions=1)
        assert chain.run(should_stop=h.stopper(False, False, True)) is True
        return chain

    def test_an_anchor_with_a_newer_txg_is_kept_though_its_name_is_older(self, h, mocker):
        """OLD (txg 900) is newer than the resumed MID (txg 20) although its NAME is older."""
        self.resume_to_mid(h, mocker, anchor=OLD)
        h.set_anchor.assert_not_called()
        # the step itself released nothing (the anchor was kept, not written); the clean stop
        # right after the resume clears the pin once
        h.clear_inflight.assert_called_once()
        assert h.events[-1] == 'clear' and ('anchor', MID) not in h.events

    def test_an_anchor_with_an_older_txg_moves_though_its_name_is_newer(self, h, mocker):
        """NEW (txg 10) is older than the resumed MID (txg 20) although its NAME is newer: a
        comparison by name would have left the anchor on it."""
        self.resume_to_mid(h, mocker, anchor=NEW)
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID)]
        h.clear_inflight.assert_called_once()

    def test_an_anchor_naming_a_vanished_snapshot_is_replaced(self, h, mocker):
        self.resume_to_mid(h, mocker, anchor='autosnap_20230101000000')
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID)]

    def test_an_anchor_already_on_the_resumed_snapshot_is_rewritten_not_compared(self, h, mocker):
        self.resume_to_mid(h, mocker, anchor=MID)
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID)]

    def test_a_fresh_send_of_the_newest_moves_an_older_anchor(self, h, mocker):
        inverted_snaps(h)
        h.get_anchor.return_value = by_name(h.snaps, MID).name
        chain = Chain(h, mocker, sessions=1, negotiate=None)
        assert chain.run() is True
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', OLD)]    # latest by txg

    def test_a_fresh_chain_step_older_than_the_anchor_leaves_it(self, h, mocker):
        """Common NEW (txg 10); the steps by txg are MID (20) then OLD (900); the anchor is on
        OLD: MID must not pull it back, and OLD itself rewrites it."""
        inverted_snaps(h)
        h.get_anchor.return_value = OLD
        chain = Chain(h, mocker, sessions=2)
        assert chain.run(common=NEW) is True
        assert chain.sent() == [(NEW, MID), (MID, OLD)]
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', OLD)]
        assert h.clear_inflight.call_count == 1


class TestSettleUpToDateFollowsTxg:
    """`common == latest`: only the bookkeeping runs."""

    def settle(self, h, anchor, latest_name=MID):
        inverted_snaps(h)
        latest = by_name(h.snaps, latest_name)
        h.get_anchor.return_value = anchor
        return h.rbm._transfer(h.transport, 'client1', h.dsi, latest, latest, 'offsite', snapshots=h.snaps)

    def test_an_anchor_newer_by_txg_is_left_alone(self, h):
        assert self.settle(h, anchor=OLD) is True               # OLD: older name, txg 900
        h.set_anchor.assert_not_called()

    def test_an_anchor_older_by_txg_is_moved_forward_though_its_name_is_newer(self, h):
        assert self.settle(h, anchor=NEW) is True               # NEW: newer name, txg 10
        assert h.set_anchor.call_args.args[1:] == ('offsite', MID)

    def test_a_missing_anchor_is_written(self, h):
        self.settle(h, anchor=None)
        assert h.set_anchor.call_args.args[1:] == ('offsite', MID)

    def test_the_stale_pin_is_cleared_when_a_newer_anchor_was_left(self, h):
        h.get_inflight.return_value = (OLD, MID)
        self.settle(h, anchor=OLD)
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_called_once()


class TestResumeOrderingFollowsTxg:
    """`_resume_target`: the incremental source must be older than the target."""

    def resume(self, h, spec, *, to_name, from_name):
        snaps = [sinfo(n, guid=g, txg=t) for n, (g, t) in spec.items()]
        snaps.sort(key=lambda s: s.createtxg, reverse=True)
        h.token(TOKEN_INCR)
        h.decodes_to(toguid=spec[to_name][0], fromguid=spec[from_name][0])
        return h.start(snaps=snaps)

    def test_the_source_must_be_older_by_txg(self, h, caplog):
        """Names say OLD < MID; txgs say OLD (50) is NEWER than MID (40): not an increment."""
        with caplog.at_level(logging.INFO):
            started = self.resume(h, {OLD: (111, 50), MID: (222, 40)}, to_name=MID, from_name=OLD)
        assert started.mode == 'fresh'
        assert messages(caplog, 'its incremental source is not older than its target')

    def test_inverted_names_do_not_matter_when_the_txg_order_is_right(self, h):
        """The source's NAME is the later one, but it was created first: a valid increment."""
        started = self.resume(h, {NEW: (111, 5), OLD: (222, 6)}, to_name=OLD, from_name=NEW)
        assert started.mode == 'resume'
        assert started.sent.name == OLD and started.base.name == NEW

    def test_equal_txg_falls_back_to_the_name_stamp(self, h):
        """`zfs snapshot a@x a@y` shares one txg: the older NAME is the source."""
        started = self.resume(h, {OLD: (111, 7), MID: (222, 7)}, to_name=MID, from_name=OLD)
        assert started.mode == 'resume'

    def test_equal_txg_and_the_wrong_name_order_is_refused(self, h, caplog):
        with caplog.at_level(logging.INFO):
            started = self.resume(h, {OLD: (111, 7), MID: (222, 7)}, to_name=OLD, from_name=MID)
        assert started.mode == 'fresh'
        assert messages(caplog, 'is not older than its target')

    def test_a_source_equal_to_the_target_is_refused(self, h, caplog):
        h.token(TOKEN_INCR)
        h.decodes_to(toguid=222, fromguid=222)
        with caplog.at_level(logging.INFO):
            assert h.start(snaps=inverted_snaps(h)).mode == 'fresh'
        assert messages(caplog, 'is not older than its target')


class TestLatestAndNegotiateOrderFollowTxg:
    """`latest` and the order of negotiate's list come from the manager's sort (newest by txg
    first); the server takes the first common name in the client's order."""

    def test_backup_dataset_negotiates_and_sends_in_txg_order(self, h, mocker):
        # three snapshots whose names ascend while their txgs descend
        h.zfs_list.return_value = [
            listed_snapshot(OLD, txg=900, guid=111),
            listed_snapshot(MID, txg=20, guid=222),
            listed_snapshot(NEW, txg=10, guid=333),
        ]
        negotiate = h.rm.post(re.compile(r'.*/negotiate$'), json={'common_snapshot': None})
        ws = MagicMock()
        ws.recv.return_value = json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=ws)
        assert h.rbm.backup_dataset(h.dsi, h.dsi.config.remote[0]) is True
        assert negotiate.last_request.json()['snapshots'] == [OLD, MID, NEW]
        h.send_snapshot.assert_called_once()
        sent = h.send_snapshot.call_args.args[0]
        assert sent.short == OLD                                     # newest by txg, oldest by name
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', OLD)]

    def test_name_order_would_have_said_the_opposite(self, h):
        h.zfs_list.return_value = [
            listed_snapshot(OLD, txg=900, guid=111),
            listed_snapshot(NEW, txg=10, guid=333),
        ]
        snaps = h.manager.list_snapshots(h.dsi)
        assert [s.name for s in snaps] == [OLD, NEW]
        assert sorted(s.name for s in snaps)[-1] == NEW             # by name, NEW would be "latest"


class TestSnapshotNewerHelperInRemoteContext:
    def test_an_unlisted_object_is_never_newer(self):
        """A snapshot built without the listing's columns cannot be ordered: the anchor is then
        simply rewritten, never kept on a guess."""
        bare = listed_info(OLD, 'autosnap', txg=ABSENT)
        known = sinfo(MID)
        assert snapshot_newer(bare, known) is False and snapshot_newer(known, bare) is False

    def test_an_anchor_on_an_unorderable_snapshot_is_rewritten(self, h):
        bare = listed_info(OLD, 'autosnap', txg=ABSENT, guid=111)
        snaps = [sinfo(NEW), sinfo(MID), bare]
        h.get_anchor.return_value = OLD
        latest = by_name(snaps, MID)
        assert h.rbm._transfer(h.transport, 'client1', h.dsi, latest, latest, 'offsite', snapshots=snaps) is True
        assert h.set_anchor.call_args.args[1:] == ('offsite', MID)


# ===========================================================================
# B5-a: the frozen send flags and the per-mode resume-token check
# ===========================================================================

from zfsbackup.remote import _FAILED, _OK, _SEND_MODES, _STOP  # noqa: E402

ALL_FLAGS = ['compressok', 'largeblockok', 'rawok', 'embedok', 'savedok']

#: (mode, token flags, accepted)
TOKEN_FLAG_MATRIX = [
    ('plain', set(), True),                                  # an earlier, flagless send
    ('plain', {'compressok'}, True),
    ('plain', {'largeblockok'}, True),
    ('plain', {'compressok', 'largeblockok'}, True),
    ('plain', {'embedok'}, False),
    ('plain', {'savedok'}, False),
    ('plain', {'rawok'}, False),
    ('plain', {'rawok', 'compressok'}, False),
    ('plain', {'compressok', 'embedok'}, False),            # `-w` on an unencrypted dataset
    ('raw', {'rawok', 'compressok'}, True),
    ('raw', {'rawok', 'compressok', 'largeblockok'}, True),
    ('raw', {'rawok'}, True),
    ('raw', {'compressok'}, False),                          # a plain `-c` token while sending raw
    ('raw', {'rawok', 'embedok'}, False),
    ('raw', {'rawok', 'compressok', 'savedok'}, False),
    ('raw', set(), False),
    ('raw', {'compressok', 'largeblockok'}, False),
]


class TestTokenFlagMatrix:
    """`_resume_target` accepts a token only when `required <= flags <= allowed` for the send
    mode of the chain: the flags a token records depend on the dataset's state as well as on the
    flags it was sent with, so both bounds are checked."""

    @pytest.mark.parametrize('mode, flags, accepted', TOKEN_FLAG_MATRIX, ids=[
        f"{m}-{'+'.join(sorted(f)) or 'none'}-{'yes' if a else 'no'}" for m, f, a in TOKEN_FLAG_MATRIX])
    def test_the_matrix(self, h, caplog, mode, flags, accepted):
        h.token()
        h.decodes_to(toguid=222, fromguid=111, flags=frozenset(flags))
        with caplog.at_level(logging.INFO):
            status, stream = h.resume(mode=mode)
        if accepted:
            assert status == 'resume' and stream.mode == 'resume'
            assert stream.sent.name == MID and stream.base.name == OLD
            h.send_resume.assert_called_once_with(TOKEN_FULL)
            assert not messages(caplog, 'Not resuming')
        else:
            assert (status, stream) == ('fresh', None)
            h.send_resume.assert_not_called()
            h.set_inflight.assert_not_called()                   # nothing pinned for a stream not sent
            (rec,) = messages(caplog, 'Not resuming pool/data', 'WARNING')
            send_mode = _SEND_MODES[mode]
            unexpected = ', '.join(sorted(set(flags) - send_mode.allowed)) or 'none'
            missing = ', '.join(sorted(send_mode.required - set(flags))) or 'none'
            assert (f'stream features this daemon does not send in {mode} mode '
                    f'(unexpected: {unexpected}; missing: {missing})') in rec.getMessage()
            assert 'sending fresh' in rec.getMessage()

    def test_the_two_modes_are_exactly_these(self):
        assert set(_SEND_MODES) == {'plain', 'raw'}
        plain, raw = _SEND_MODES['plain'], _SEND_MODES['raw']
        assert plain.options == {'large_blocks': True, 'compressed': True}
        assert raw.options == {'raw': True}
        assert plain.required == frozenset() and plain.allowed == {'compressok', 'largeblockok'}
        assert raw.required == {'rawok'} and raw.allowed == {'rawok', 'compressok', 'largeblockok'}

    @pytest.mark.parametrize('flag', ['embedok', 'savedok'])
    def test_embedok_and_savedok_are_never_allowed(self, flag):
        assert all(flag not in m.allowed for m in _SEND_MODES.values())

    def test_an_equality_check_would_get_both_modes_wrong(self, h):
        """Plain tokens come with `compressok` or `largeblockok` or both, or none; an `==`
        against the flags sent would refuse the others. Pinned by listing every subset."""
        import itertools
        for r in range(3):
            for combo in itertools.combinations(['compressok', 'largeblockok'], r):
                h.token()
                h.decodes_to(toguid=222, fromguid=111, flags=frozenset(combo))
                assert h.resume(mode='plain')[0] == 'resume', combo
                h.send_resume.reset_mock()
                h.set_inflight.reset_mock()

    def test_the_mode_reaches_the_check_through_transfer(self, h5, mocker):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111, flags=frozenset({'rawok', 'compressok'}))
        chain = Chain(h5, mocker, sessions=6, negotiate=S2)
        assert chain.run(common=S1, mode='raw') is True
        assert chain.urls()[0] == f'?mode=resume&from={S1}'
        h5.send_resume.assert_called_once_with(TOKEN_INCR)


from _openzfs_fixtures import VERSIONS, load_m1_summary, load_section  # noqa: E402


class TestTokenFlagsOnRealCaptures:
    """The per-mode check on the REAL `zfs send -nv -t` output of OpenZFS 2.1.5, 2.2.2 and 2.4.1
    (tests/fixtures/openzfs, M1): each token's flags come out of the real decoder, and the
    verdict per mode is what the chain's send flags produce:

    * plain (`-L -c`): every plain-dataset token of a `none`/`L`/`c`/`Lc` send and a plain `-L -c`
      from an encrypted source (key loaded); never an `-e` or `-w` token;
    * raw (`-w`): the `-w` token of an ENCRYPTED dataset only (`-w` on an unencrypted dataset
      records no `rawok`)."""

    @pytest.fixture(params=VERSIONS)
    def version(self, request):
        return request.param

    @staticmethod
    def verdicts(case):
        """(plain accepted, raw accepted) for an M1 case id like `128K_lz4_enc_w`."""
        _size, _comp, rest = case.split('_', 2)
        kind, label = rest.split('_', 1)
        plain = label in ('none', 'L', 'c', 'Lc')
        raw = kind == 'enc' and label == 'w'
        return plain, raw

    @staticmethod
    def replay(mock_subprocess, rec):
        proc = mock_subprocess.setup(stdout=rec.stdout_lines, stderr=rec.stderr_lines, returncode=rec.rc)
        proc.poll.side_effect = None
        proc.poll.return_value = rec.rc

    def test_every_captured_token_gets_the_verdict_of_its_send_mode(self, make_h, mock_subprocess, version):
        import libzfseasy as real_zfs
        h = make_h(decode=False)                       # the real `resume_info`, fed by the captured stdout
        rows = load_m1_summary(version)
        assert rows
        recs = load_section(version, 'M1')
        for row in rows:
            rec = recs['M1/%s.%s.3_send_nv_t' % (row['case'], row['kind'])]
            self.replay(mock_subprocess, rec)
            info = real_zfs.send.resume_info(rec.token)
            assert info.flags == row['flags'], row['case']
            snaps = [sinfo(NEW, guid=info.toguid)]
            if info.fromguid is not None:
                snaps.append(sinfo(OLD, guid=info.fromguid))
            plain, raw = self.verdicts(row['case'])
            for mode, expected in (('plain', plain), ('raw', raw)):
                self.replay(mock_subprocess, rec)
                result = h.rbm._resume_target(rec.token, h.dsi, snaps, mode)
                assert (not isinstance(result, str)) is expected, (version, row['case'], row['kind'], mode, result)

    def test_the_summary_covers_both_kinds_and_every_label(self, version):
        rows = load_m1_summary(version)
        assert {r['kind'] for r in rows} == {'full', 'incr'}
        labels = {r['case'].split('_', 2)[2] for r in rows}
        assert labels == {'plain_none', 'plain_L', 'plain_Lc', 'plain_c', 'plain_e', 'plain_w',
                          'enc_none', 'enc_Lc', 'enc_w'}


class TestSendArgvPerMode:
    """The real `zfs send` command line each mode produces (only `Popen` is mocked)."""

    @pytest.fixture
    def popen(self, mock_subprocess):
        proc = MagicMock()
        proc.stdout.peek.return_value = b'ZFS_DATA'
        proc.poll.return_value = None
        mock_subprocess.return_value = proc
        return mock_subprocess

    def rbm_dsi(self):
        config = _make_config()
        manager = DatasetManager(config)
        return _rbm(config, manager), manager.datasets[0]

    @staticmethod
    def argv(popen):
        return list(popen.call_args_list[-1][0][0])[1:]

    def test_plain_is_L_and_c(self, popen):
        rbm, dsi = self.rbm_dsi()
        assert rbm._start_fresh(dsi, 'offsite', None, sinfo(NEW), 'plain') is not None
        argv = self.argv(popen)
        assert argv[0] == 'send' and '-L' in argv and '-c' in argv
        assert not {'-w', '-e', '-i', '-I', '-p', '-R'} & set(argv)
        assert argv[-1] == f'pool/data@{NEW}'

    def test_plain_incremental_adds_i_prev(self, popen):
        rbm, dsi = self.rbm_dsi()
        rbm._start_fresh(dsi, 'offsite', sinfo(MID), sinfo(NEW), 'plain')
        argv = self.argv(popen)
        assert '-L' in argv and '-c' in argv
        assert argv[argv.index('-i') + 1] == f'pool/data@{MID}'
        assert argv[-1] == f'pool/data@{NEW}'

    def test_raw_is_w_alone(self, popen):
        rbm, dsi = self.rbm_dsi()
        rbm._start_fresh(dsi, 'offsite', None, sinfo(NEW), 'raw')
        argv = self.argv(popen)
        assert argv == ['send', '-w', f'pool/data@{NEW}']

    def test_raw_incremental_is_w_and_i(self, popen):
        rbm, dsi = self.rbm_dsi()
        rbm._start_fresh(dsi, 'offsite', sinfo(MID), sinfo(NEW), 'raw')
        argv = self.argv(popen)
        assert argv[:2] == ['send', '-w']
        assert not {'-L', '-c', '-e'} & set(argv)
        assert argv[argv.index('-i') + 1] == f'pool/data@{MID}'

    def test_a_resume_is_send_t_with_no_flags_in_either_mode(self, popen, requests_mock, mocker):
        for mode in ('plain', 'raw'):
            rbm, dsi = self.rbm_dsi()
            requests_mock.get(f'{BASE}/backup/client1/{dsi.name}/resume_token', json={'resume_token': TOKEN_FULL})
            flags = frozenset({'rawok', 'compressok'}) if mode == 'raw' else frozenset({'compressok'})
            mocker.patch('zfsbackup.remote.zfs.send.resume_info',
                         return_value=ResumeToken(toname=f'pool/data@{OLD}', toguid=111, fromguid=None, flags=flags))
            status, stream = rbm._try_resume(tr(), 'client1', dsi, 'offsite', [sinfo(NEW), sinfo(OLD)], mode)
            assert status == 'resume'
            assert self.argv(popen) == ['send', '-t', TOKEN_FULL]


# ===========================================================================
# S6-e: the send mode, decided once per backup_dataset call
# ===========================================================================

ENC = 'aes-256-gcm'

#: (raw setting, encryption, keystatus) -> the mode, or None when nothing is sent (ERROR once)
MODE_MATRIX = [
    (None, 'off', '-', 'plain'),
    (None, ENC, 'available', 'raw'),
    (None, ENC, 'unavailable', 'raw'),
    (None, None, None, 'plain'),                       # not listed: not known to be encrypted
    (True, 'off', '-', None),
    (True, ENC, 'available', 'raw'),
    (True, ENC, 'unavailable', 'raw'),
    (True, None, None, None),
    (False, 'off', '-', 'plain'),
    (False, ENC, 'available', 'plain'),
    (False, ENC, 'unavailable', None),
    (False, ENC, None, 'plain'),                       # key state unknown: not refused
    (False, None, None, 'plain'),
]
MODE_FLAGS = {'plain': PLAIN_FLAGS, 'raw': {'raw': True}}


def cfg_for(raw):
    return RemoteDatasetConfig(destination='offsite', raw=raw)


def encrypted_snaps(h, encryption, keystatus, names=(NEW, MID, OLD)):
    snaps = [sinfo(n, encryption=encryption, keystatus=keystatus) for n in names]
    snaps.sort(key=lambda s: s.createtxg, reverse=True)
    h.snaps = snaps
    h.latest = snaps[0]
    return snaps


class TestSendModeDecision:
    """The destination's `raw` setting x the newest snapshot's encryption and key state."""

    def run(self, h, mocker, raw, encryption, keystatus, **chain_kw):
        encrypted_snaps(h, encryption, keystatus)
        chain = Chain(h, mocker, sessions=chain_kw.pop('sessions', 1), negotiate=chain_kw.pop('negotiate', None))
        return chain, h.rbm.backup_dataset(h.dsi, cfg_for(raw))

    @pytest.mark.parametrize('raw, encryption, keystatus, mode', MODE_MATRIX, ids=[
        f"raw={r}-{e}-{k}" for r, e, k, _ in MODE_MATRIX])
    def test_the_matrix(self, h, mocker, caplog, raw, encryption, keystatus, mode):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            chain, ok = self.run(h, mocker, raw, encryption, keystatus)
        errors = [r for r in caplog.records if r.name == 'zfsbackup.remote' and r.levelname == 'ERROR']
        if mode is None:
            assert ok is False
            assert len(errors) == 1 and 'nothing is sent' in errors[0].getMessage()
            chain.negotiate.assert_not_called()
            assert not h.token_gets()
            chain.ws_cls.assert_not_called()
            h.nothing_started()                                  # no `zfs send`, no `set_inflight`
            h.set_anchor.assert_not_called()
        else:
            assert ok is True and not errors
            chain.negotiate.assert_called_once()
            (call,) = h.send_snapshot.call_args_list
            assert call.kwargs == MODE_FLAGS[mode]

    @pytest.mark.parametrize('raw, encryption, keystatus', [(True, 'off', '-'), (True, None, None)])
    def test_raw_true_on_an_unencrypted_dataset_says_how_to_fix_it(self, h, mocker, caplog, raw, encryption, keystatus):
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            self.run(h, mocker, raw, encryption, keystatus)
        (rec,) = messages(caplog, 'raw is true but the dataset is not encrypted', 'ERROR')
        assert 'pool/data -> offsite' in rec.getMessage() and 'auto (or false)' in rec.getMessage()

    def test_raw_false_on_an_encrypted_dataset_with_the_key_unloaded_says_how_to_fix_it(self, h, mocker, caplog):
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            self.run(h, mocker, False, ENC, 'unavailable')
        (rec,) = messages(caplog, "raw is false but the dataset's key is not loaded", 'ERROR')
        assert 'zfs load-key' in rec.getMessage() and 'auto' in rec.getMessage()

    @pytest.mark.parametrize('raw, encryption, keystatus', [(True, 'off', '-'), (False, ENC, 'unavailable')])
    def test_the_error_is_logged_once_then_at_debug(self, h, mocker, caplog, raw, encryption, keystatus):
        encrypted_snaps(h, encryption, keystatus)
        chain = Chain(h, mocker, sessions=1, negotiate=None)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for _ in range(3):
                assert h.rbm.backup_dataset(h.dsi, cfg_for(raw)) is False
        own = [(r.levelname, r.getMessage()) for r in caplog.records
               if r.name == 'zfsbackup.remote' and 'nothing is sent' in r.getMessage()]
        assert [lv for lv, _ in own] == ['ERROR', 'DEBUG', 'DEBUG']
        assert len({m for _, m in own}) == 1

    def test_it_is_logged_again_after_the_problem_went_away_and_came_back(self, h, mocker, caplog):
        encrypted_snaps(h, 'off', '-')
        chain = Chain(h, mocker, sessions=3, negotiate=None)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert h.rbm.backup_dataset(h.dsi, cfg_for(True)) is False         # ERROR
            assert h.rbm.backup_dataset(h.dsi, cfg_for(True)) is False         # DEBUG
            assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is True          # fixed
            assert h.rbm.backup_dataset(h.dsi, cfg_for(True)) is False         # back: ERROR again
        levels_ = [r.levelname for r in caplog.records
                   if r.name == 'zfsbackup.remote' and 'raw is true' in r.getMessage()]
        assert levels_ == ['ERROR', 'DEBUG', 'ERROR']

    def test_the_two_problems_have_their_own_state(self, h, mocker, caplog):
        encrypted_snaps(h, ENC, 'unavailable')
        Chain(h, mocker, sessions=1, negotiate=None)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            h.rbm.backup_dataset(h.dsi, cfg_for(False))
            h.rbm.backup_dataset(h.dsi, cfg_for(False))
        assert [r.levelname for r in messages(caplog, "raw is false")] == ['ERROR', 'DEBUG']

    def test_each_destination_has_its_own_state(self, h, mocker, caplog):
        encrypted_snaps(h, 'off', '-')
        Chain(h, mocker, sessions=1, negotiate=None)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            h.rbm.backup_dataset(h.dsi, cfg_for(True))
            other = RemoteDatasetConfig(destination='dc2', raw=True)
            h.rbm.config.destinations['dc2'] = Destination(url=BASE, server_fingerprint=SERVER_FP)
            h.rbm.backup_dataset(h.dsi, other)
        assert [r.levelname for r in messages(caplog, 'raw is true')] == ['ERROR', 'ERROR']

    def test_the_mode_is_decided_before_anything_is_negotiated_even_when_up_to_date(self, h, mocker, caplog):
        """Server already holds `latest`: still the mode is judged first, so `raw: true` on an
        unencrypted dataset is reported rather than hidden behind "nothing to send"."""
        encrypted_snaps(h, 'off', '-')
        chain = Chain(h, mocker, sessions=1, negotiate=NEW)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert h.rbm.backup_dataset(h.dsi, cfg_for(True)) is False
        assert messages(caplog, 'raw is true but the dataset is not encrypted', 'ERROR')
        chain.negotiate.assert_not_called()
        h.set_anchor.assert_not_called()


class TestModeFixedAcrossTheChain:
    def test_every_step_of_a_raw_chain_is_w(self, h5, mocker):
        encrypted = [sinfo(n, encryption=ENC, keystatus='unavailable') for n in reversed(FIVE)]
        h5.snaps = encrypted
        h5.latest = encrypted[0]
        chain = Chain(h5, mocker, sessions=6, negotiate=S1)
        assert h5.rbm.backup_dataset(h5.dsi, cfg_for(None)) is True
        assert chain.sent() == [(S1, S2), (S2, S3), (S3, S4), (S4, S5)]
        for call in h5.send_snapshot.call_args_list:
            assert {k: v for k, v in call.kwargs.items() if k != 'since'} == {'raw': True}
        assert chain.urls() == [f'?mode=fresh&from={a}' for a in (S1, S2, S3, S4)]

    def test_every_step_of_a_plain_chain_is_L_and_c(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=6, negotiate=S1)
        assert h5.rbm.backup_dataset(h5.dsi, cfg_for(None)) is True
        for call in h5.send_snapshot.call_args_list:
            assert {k: v for k, v in call.kwargs.items() if k != 'since'} == PLAIN_FLAGS

    def test_the_mode_survives_the_resume_renegotiate_and_a_changed_listing(self, h5, mocker):
        """The listing re-read after the resume says "not encrypted" (say the key state or the
        dataset changed meanwhile): the mode decided at the start still holds for the rest of
        the chain, and the token is checked against it."""
        raw_listing = [sinfo(n, encryption=ENC, keystatus='unavailable') for n in reversed(FIVE)]
        plain_listing = [sinfo(n) for n in reversed(FIVE)]
        h5.snaps = raw_listing
        h5.latest = raw_listing[0]
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111, flags=frozenset({'rawok', 'compressok'}))
        chain = Chain(h5, mocker, sessions=6, negotiate=[S1, S2], relist=[raw_listing, plain_listing])
        assert h5.rbm.backup_dataset(h5.dsi, cfg_for(None)) is True
        assert chain.urls()[0] == f'?mode=resume&from={S1}'
        assert [s for s, _ in chain.sent()] == [S2, S3, S4]
        for call in h5.send_snapshot.call_args_list:
            assert {k: v for k, v in call.kwargs.items() if k != 'since'} == {'raw': True}

    def test_a_plain_token_is_not_resumed_by_a_raw_chain(self, h5, mocker):
        encrypted = [sinfo(n, encryption=ENC, keystatus='unavailable') for n in reversed(FIVE)]
        h5.snaps = encrypted
        h5.latest = encrypted[0]
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111, flags=frozenset({'compressok'}))
        chain = Chain(h5, mocker, sessions=6, negotiate=S1)
        assert h5.rbm.backup_dataset(h5.dsi, cfg_for(None)) is True
        h5.send_resume.assert_not_called()
        assert chain.urls()[0] == f'?mode=fresh&from={S1}'
        assert h5.send_snapshot.call_args_list[0].kwargs['raw'] is True


class TestServerCopyMismatch:
    """S6-d/e: the server reports what kind of copy it holds (`plain`, `raw`, `none`); a copy of
    the other kind than the chain would send is never extended, the client only says so."""

    def run(self, h, mocker, copy, encryption='off', keystatus='-', raw=None, common=None):
        encrypted_snaps(h, encryption, keystatus)
        h.server_says(common=common, copy=copy)
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        return ws_cls, h.rbm.backup_dataset(h.dsi, cfg_for(raw))

    def mutating_requests(self, h):
        return [r for r in h.rm.request_history if r.method not in ('GET', 'POST')
                or (r.method == 'POST' and not r.path.endswith('/negotiate'))]

    def assert_refused(self, h, ws_cls, ok, caplog, kind):
        assert ok is False
        ws_cls.assert_not_called()
        h.nothing_started()                                       # no `zfs send`, no `set_inflight`
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_not_called()
        assert not h.token_gets()
        assert self.mutating_requests(h) == []                   # the server's data is not touched
        (rec,) = messages(caplog, 'nothing is sent', 'ERROR')
        text = rec.getMessage()
        assert f'is a {kind} copy' in text
        assert f'zfs rename pool/b/client1/data pool/b/client1/data.{kind}-<date>' in text
        return text

    def test_a_plain_copy_while_sending_raw(self, h, mocker, caplog):
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            ws_cls, ok = self.run(h, mocker, 'plain', ENC, 'unavailable', common=OLD)
        text = self.assert_refused(h, ws_cls, ok, caplog, 'plain')
        assert 'is now sent raw' in text and 'full raw stream' in text

    def test_a_raw_copy_while_sending_plain(self, h, mocker, caplog):
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            ws_cls, ok = self.run(h, mocker, 'raw', 'off', '-', common=OLD)
        text = self.assert_refused(h, ws_cls, ok, caplog, 'raw')
        assert 'is now sent plain' in text and 'full plain stream' in text

    def test_a_raw_copy_while_raw_false_forces_plain(self, h, mocker, caplog):
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            ws_cls, ok = self.run(h, mocker, 'raw', ENC, 'available', raw=False, common=OLD)
        self.assert_refused(h, ws_cls, ok, caplog, 'raw')

    def test_the_error_is_logged_once_then_at_debug(self, h, mocker, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for _ in range(3):
                self.run(h, mocker, 'plain', ENC, 'unavailable', common=OLD)
        assert [r.levelname for r in messages(caplog, 'nothing is sent')] == ['ERROR', 'DEBUG', 'DEBUG']

    def test_it_is_logged_again_after_the_server_copy_was_moved_aside_and_the_mismatch_returns(
        self, h, mocker, caplog
    ):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            ws_cls, ok = self.run(h, mocker, 'plain', ENC, 'unavailable', common=OLD)
            assert ok is False
            # the operator renamed it away: the server holds nothing (`none`), the full stream goes
            h.server_says(common=None, copy='none')
            chain = Chain(h, mocker, sessions=1, negotiate=REAL)
            assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is True
            h.send_snapshot.assert_called_once()
            h.server_says(common=OLD, copy='plain')
            assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is False
        assert [r.levelname for r in messages(caplog, 'nothing is sent', 'ERROR')] == ['ERROR', 'ERROR']

    @pytest.mark.parametrize('copy', ['none', Harness.NOFIELD], ids=['none', 'no-field'])
    @pytest.mark.parametrize('encryption, keystatus, kind', [('off', '-', 'plain'), (ENC, 'unavailable', 'raw')])
    def test_none_and_a_missing_field_never_block(self, h, mocker, caplog, copy, encryption, keystatus, kind):
        """`none`: the server holds no snapshot. No field: an older server."""
        encrypted_snaps(h, encryption, keystatus)
        h.server_says(common=None, copy=copy)
        Chain(h, mocker, sessions=1, negotiate=REAL)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is True
        assert not messages(caplog, 'nothing is sent')
        (call,) = h.send_snapshot.call_args_list
        assert call.kwargs == MODE_FLAGS[kind]

    @pytest.mark.parametrize('copy, encryption, keystatus, kind', [
        ('plain', 'off', '-', 'plain'), ('raw', ENC, 'unavailable', 'raw'),
    ])
    def test_a_matching_copy_proceeds_incrementally(self, h, mocker, caplog, copy, encryption, keystatus, kind):
        encrypted_snaps(h, encryption, keystatus)
        h.server_says(common=OLD, copy=copy)
        chain = Chain(h, mocker, sessions=3, negotiate=REAL)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is True
        assert not messages(caplog, 'nothing is sent')
        assert chain.sent() == [(OLD, MID), (MID, NEW)]
        for call in h.send_snapshot.call_args_list:
            assert {k: v for k, v in call.kwargs.items() if k != 'since'} == MODE_FLAGS[kind]

    def test_a_mismatch_is_reported_even_when_the_server_is_up_to_date(self, h, mocker, caplog):
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            ws_cls, ok = self.run(h, mocker, 'plain', ENC, 'unavailable', common=NEW)
        assert ok is False
        assert messages(caplog, 'nothing is sent', 'ERROR')
        h.set_anchor.assert_not_called()

    def test_negotiate_records_what_the_server_said(self, h):
        h.server_says(common=None, copy='raw')
        h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
        assert h.rbm._server_info[('pool/data', 'offsite')] == ('raw', 'pool/b/client1/data')

    def test_an_older_server_without_the_field_is_recorded_as_unknown(self, h):
        h.server_says(common=None)
        h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
        assert h.rbm._server_info[('pool/data', 'offsite')] == (None, 'pool/b/client1/data')

    def test_the_answer_is_kept_per_dataset_and_destination(self, h):
        h.server_says(common=None, copy='plain')
        h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
        h.server_says(common=None, copy='none')
        h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'other')
        assert h.rbm._server_info[('pool/data', 'offsite')][0] == 'plain'
        assert h.rbm._server_info[('pool/data', 'other')][0] == 'none'

    def test_the_hint_names_the_dataset_the_server_reported(self, h, mocker, caplog):
        encrypted_snaps(h, ENC, 'unavailable')
        h.server_says(common=OLD, copy='plain', server_dataset='tank/backups/client-a/data')
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        mocker.patch('zfsbackup.remote.websocket.WebSocket')
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            h.rbm.backup_dataset(h.dsi, cfg_for(None))
        (rec,) = messages(caplog, 'nothing is sent', 'ERROR')
        assert 'zfs rename tank/backups/client-a/data tank/backups/client-a/data.plain-<date>' in rec.getMessage()


# ===========================================================================
# Dry run, a missing common snapshot
# ===========================================================================


class TestDryRunPlansTheChain:
    @pytest.fixture
    def dry(self, h5, mocker):
        h5.rbm.config.dry_run = True
        return h5, Chain(h5, mocker, sessions=1)

    def assert_inert(self, h, chain):
        chain.ws_cls.assert_not_called()
        h.nothing_started()
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_not_called()
        h.get_anchor.assert_not_called()
        h.get_inflight.assert_not_called()
        assert not h.token_gets()
        h.resume_info.assert_not_called()

    def test_it_logs_the_whole_chain_and_does_nothing_else(self, dry, caplog):
        h, chain = dry
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S2) is True
        assert f'[DRY RUN] Would send pool/data: {S2} -> {S3} -> {S4} -> {S5} (3 step(s)) -> {BASE}' in caplog.text
        self.assert_inert(h, chain)
        chain.relist.assert_not_called()

    def test_a_full_send_plans_only_the_newest(self, dry, caplog):
        h, chain = dry
        with caplog.at_level(logging.INFO):
            assert chain.run(common=None) is True
        assert f'[DRY RUN] Would send pool/data: full {S5} (1 step(s)) -> {BASE}' in caplog.text
        self.assert_inert(h, chain)

    def test_it_is_decided_before_the_stop_check(self, dry):
        h, chain = dry
        stop = h.stopper(True)
        assert chain.run(common=S2, should_stop=stop) is True
        assert stop.polls() == 0

    def test_up_to_date_is_not_announced_and_writes_nothing(self, dry, caplog):
        h, chain = dry
        with caplog.at_level(logging.DEBUG):
            assert chain.run(common=S5) is True
        assert 'DRY RUN' not in caplog.text
        self.assert_inert(h, chain)

    def test_backup_dataset_in_a_dry_run_negotiates_but_sends_and_writes_nothing(self, dry, mocker):
        h, chain = dry
        chain = Chain(h, mocker, sessions=1, negotiate=S3)
        assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is True
        chain.negotiate.assert_called_once()
        self.assert_inert(h, chain)
        assert chain.relist.call_count == 1                 # the one listing `backup_dataset` makes


class TestMissingCommonSnapshot:
    """The server names a common snapshot that is not a managed local one: never a full send."""

    def test_at_the_start_of_the_cycle(self, h5, mocker, caplog):
        h5.server_says(common='autosnap_20200101000000', copy='plain')
        chain = Chain(h5, mocker, sessions=6, negotiate=REAL)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert h5.rbm.backup_dataset(h5.dsi, cfg_for(None)) is False
        (rec,) = messages(caplog, 'autosnap_20200101000000', 'ERROR')
        assert 'is not a managed local snapshot' in rec.getMessage()
        assert 'skipping this cycle rather than sending a full stream' in rec.getMessage()
        chain.ws_cls.assert_not_called()
        h5.nothing_started()                      # no pin, no `zfs send` (so no full send either)
        assert not h5.token_gets()
        h5.set_anchor.assert_not_called()

    def test_after_the_resume_renegotiate(self, h5, mocker, caplog):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h5, mocker, sessions=6, negotiate=[S1, 'autosnap_20200101000000'])
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert h5.rbm.backup_dataset(h5.dsi, cfg_for(None)) is False
        (rec,) = messages(caplog, 'autosnap_20200101000000', 'ERROR')
        assert 'skipping the rest of this cycle rather than sending a full stream' in rec.getMessage()
        assert chain.ws_cls.call_count == 1                              # only the resume
        h5.send_snapshot.assert_not_called()                             # no full send, no chain
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2]     # the resume is kept

    def test_a_none_common_snapshot_is_a_full_send_not_an_error(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=2, negotiate=None)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert h5.rbm.backup_dataset(h5.dsi, cfg_for(None)) is True
        assert chain.sent() == [(None, S5)]
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


# ===========================================================================
# A step whose snapshot was destroyed while it was being sent
# ===========================================================================


class TestSnapshotDestroyedWhileSending:
    """After the ack and the anchor write, before the pin is released, the client asks `zfs get
    guid` for the snapshot it has just sent. A destroy that read the anchors before the step
    pinned its pair could have taken it: the server has it, this host no longer does."""

    def ordered(self, h, mocker):
        """Record the check in `events`, between the anchor write and the pin's release."""
        from zfsbackup.remote import zfs as remote_zfs
        inner = remote_zfs.get.value.side_effect
        remote_zfs.get.value.side_effect = lambda *a, **k: (h.events.append('guid?'), inner(*a, **k))[1]

    def test_the_check_sits_between_the_anchor_and_the_release_of_the_pin(self, h5, mocker, snapshot_presence):
        self.ordered(h5, mocker)
        chain = Chain(h5, mocker, sessions=2)
        chain.run(common=S4)
        assert [e for e in h5.events if e in ('ack', 'clear', 'guid?') or e[0] == 'anchor'] == [
            'ack', ('anchor', S5), 'guid?', 'clear']
        assert snapshot_presence.calls == [(f'pool/data@{S5}', 'guid')]

    def test_one_extra_zfs_get_per_step_and_none_for_a_kept_anchor(self, h5, mocker, snapshot_presence):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1)
        assert [c[0].split('@')[1] for c in snapshot_presence.calls] == [S2, S3, S4, S5]
        snapshot_presence.calls.clear()
        h5.get_anchor.return_value = S5          # a newer anchor is kept: nothing is written, nothing to check
        again = Chain(h5, mocker, sessions=5)
        again.run(common=S1)
        assert [c[0].split('@')[1] for c in snapshot_presence.calls] == [S5]

    @pytest.mark.parametrize('how', ['gone', 'recreated'])
    def test_a_destroyed_step_restores_the_anchor_to_its_base_keeps_the_pin_and_stops(
        self, h5, mocker, caplog, snapshot_presence, how
    ):
        h5.get_anchor.return_value = S1                  # not the base of the failing step: told apart
        if how == 'gone':
            snapshot_presence.gone.add(S3)
        else:
            snapshot_presence.replaced[S3] = 424242       # same name, another guid
        chain = Chain(h5, mocker, sessions=6)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1) is False
        assert chain.sent() == [(S1, S2), (S2, S3)]                     # nothing after it
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S3, S2]   # written, then put back on the BASE
        h5.clear_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()                           # step 1's pin was kept; step 2's stays
        (rec,) = messages(caplog, 'was destroyed while it was being sent', 'ERROR')
        assert f'pool/data@{S3}' in rec.getMessage() and f'Putting the anchor back on {S2}' in rec.getMessage()
        assert f'Backup complete: pool/data@{S3}' not in caplog.text    # not reported as a clean step

    def test_a_destroyed_full_send_restores_the_previous_anchor(self, h5, mocker, snapshot_presence):
        h5.get_anchor.return_value = 'autosnap_20230101000000'
        snapshot_presence.gone.add(S5)
        chain = Chain(h5, mocker, sessions=2)
        assert chain.run(common=None) is True                           # it was the last (only) step
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5, 'autosnap_20230101000000']
        h5.clear_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()

    def test_a_destroyed_full_send_with_no_previous_anchor_clears_it(self, h5, mocker, snapshot_presence):
        h5.get_anchor.return_value = None
        snapshot_presence.gone.add(S5)
        chain = Chain(h5, mocker, sessions=2)
        assert chain.run(common=None) is True
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5]
        h5.clear_anchor.assert_called_once_with(h5.dsi, 'offsite')
        h5.clear_inflight.assert_not_called()

    def test_a_destroyed_resumed_snapshot_is_put_back_on_the_resume_base(self, h5, mocker, snapshot_presence):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        snapshot_presence.gone.add(S2)
        chain = Chain(h5, mocker, sessions=6, negotiate=S2)
        assert chain.run(common=S1) is False
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S1]
        h5.send_snapshot.assert_not_called()
        chain.negotiate.assert_not_called()

    def test_an_inconclusive_check_warns_keeps_the_pin_and_stops_without_touching_the_anchor(
        self, h5, mocker, caplog, snapshot_presence
    ):
        snapshot_presence.unknown.add(S3)
        chain = Chain(h5, mocker, sessions=6)
        with caplog.at_level(logging.INFO):
            assert chain.run(common=S1) is False
        assert chain.sent() == [(S1, S2), (S2, S3)]
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S3]   # no restore
        h5.clear_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()
        (rec,) = messages(caplog, 'cannot confirm that pool/data@' + S3 + ' still exists')
        assert rec.levelname == 'WARNING' and 'keeping its in-flight pin' in rec.getMessage()
        assert not messages(caplog, 'was destroyed', 'ERROR')

    def test_the_last_step_destroyed_still_reports_success_of_the_transfer(self, h5, mocker, snapshot_presence):
        snapshot_presence.gone.add(S5)
        chain = Chain(h5, mocker, sessions=2)
        assert chain.run(common=S4) is True
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5, S4]

    def test_a_present_snapshot_releases_the_pin_as_before(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=2)
        assert chain.run(common=S4) is True
        h5.clear_inflight.assert_called_once()
        h5.clear_anchor.assert_not_called()

    def test_snapshot_present_answers(self, h5, snapshot_presence):
        present = h5.rbm._snapshot_present
        step = by_name(h5.snaps, S3)
        assert present(step) is True
        snapshot_presence.gone.add(S3)
        assert present(step) is False
        snapshot_presence.gone.clear()
        snapshot_presence.unknown.add(S3)
        assert present(step) is None
        snapshot_presence.unknown.clear()
        snapshot_presence.replaced[S3] = 7
        assert present(step) is False

    def test_a_guid_that_zfs_cannot_parse_is_inconclusive(self, h5, snapshot_presence):
        snapshot_presence.replaced[S3] = 'not-a-number'
        assert h5.rbm._snapshot_present(by_name(h5.snaps, S3)) is None


# ===========================================================================
# A refusal behind a failed send, and a stream that cannot be set up
# ===========================================================================


class CountingStream:
    """A `zfs send` stand-in with one chunk, counting its closes."""

    def __init__(self):
        self.closes = 0
        self._chunks = [b'x']

    def read(self, n):
        return self._chunks.pop(0) if self._chunks else b''

    def close(self):
        self.closes += 1


class TestRefusalBehindAFailedSend:
    def failed_send_then(self, h5, mocker, reply):
        chain = Chain(h5, mocker, sessions=4)
        chain.sessions[0].send_binary.side_effect = BrokenPipeError('broken pipe')
        chain.sessions[0].recv.return_value = json.dumps(reply)
        return chain

    @pytest.mark.parametrize('reply', [
        {'error': 'busy', 'retry': True},
        {'error': 'server is reloading; retry later', 'retry': True},
        {'retry': True},
    ])
    def test_a_retry_refusal_is_a_stop_the_chain_ends_without_a_diagnosis(self, h5, mocker, reply):
        chain = self.failed_send_then(h5, mocker, reply)
        assert chain.run(common=S1) is False
        chain.relist.assert_not_called()
        assert chain.ws_cls.call_count == 1
        assert chain.sent() == [(S1, S2)]

    def test_a_retry_refusal_is_reported_as_an_outcome_of_stop(self, h5, mocker):
        ws = MagicMock()
        ws.send_binary.side_effect = BrokenPipeError('broken pipe')
        ws.recv.return_value = json.dumps({'error': 'busy', 'retry': True})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=ws)
        step = by_name(h5.snaps, S2)
        started = _SendStream(CountingStream(), 'fresh', True, step, by_name(h5.snaps, S1))
        assert h5.rbm._push_step(h5.transport, 'client1', h5.dsi, 'offsite', h5.snaps, started) == _STOP

    @pytest.mark.parametrize('reply', [
        {'error': 'pool/backups/c1/data is held for pool/other'},
        {'status': 'error', 'error': 'unknown mode'},
        {'error': 'x', 'retry': False},
    ])
    def test_a_refusal_without_retry_is_an_ordinary_failure_that_looks_for_a_vanished_member(
        self, h5, mocker, reply
    ):
        chain = self.failed_send_then(h5, mocker, reply)
        assert chain.run(common=S1) is False
        chain.relist.assert_called_once()
        assert chain.ws_cls.call_count == 1

    def test_a_non_retry_refusal_outcome_is_failed(self, h5, mocker):
        ws = MagicMock()
        ws.send_binary.side_effect = BrokenPipeError('broken pipe')
        ws.recv.return_value = json.dumps({'error': 'held elsewhere'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=ws)
        step = by_name(h5.snaps, S2)
        started = _SendStream(CountingStream(), 'fresh', True, step, by_name(h5.snaps, S1))
        assert h5.rbm._push_step(h5.transport, 'client1', h5.dsi, 'offsite', h5.snaps, started) == _FAILED


class TestStreamSetupFailureReapsTheStream:
    """The URL and the websocket object are built inside the `try` that owns the started `zfs
    send`: whatever fails there, the stream is closed exactly once and the step fails."""

    def push(self, h5, step=S2, base=S1):
        stream = CountingStream()
        started = _SendStream(stream, 'fresh', True, by_name(h5.snaps, step),
                              by_name(h5.snaps, base) if base else None)
        outcome = h5.rbm._push_step(h5.transport, 'client1', h5.dsi, 'offsite', h5.snaps, started)
        return stream, outcome

    def test_the_path_helper_raising(self, h5, mocker):
        mocker.patch('zfsbackup.remote._path', side_effect=RuntimeError('boom'))
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        stream, outcome = self.push(h5)
        assert outcome == _FAILED and stream.closes == 1
        ws_cls.assert_not_called()
        h5.set_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()

    def test_quote_raising_for_the_base_name(self, h5, mocker):
        mocker.patch('zfsbackup.remote._path', side_effect=lambda text: text)      # (`_path` uses `quote` too)
        mocker.patch('zfsbackup.remote.quote', side_effect=RuntimeError('boom'))
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        stream, outcome = self.push(h5)
        assert outcome == _FAILED and stream.closes == 1
        ws_cls.assert_not_called()

    def test_quote_is_not_asked_for_a_full_send(self, h5, mocker):
        mocker.patch('zfsbackup.remote._path', side_effect=lambda text: text)
        quote = mocker.patch('zfsbackup.remote.quote', side_effect=RuntimeError('boom'))
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=MagicMock(
            recv=MagicMock(return_value='{"status": "ok"}')))
        stream, outcome = self.push(h5, step=S5, base=None)
        assert outcome == _OK
        quote.assert_not_called()

    def test_the_websocket_constructor_raising(self, h5, mocker):
        mocker.patch('zfsbackup.remote.websocket.WebSocket', side_effect=RuntimeError('boom'))
        stream, outcome = self.push(h5)
        assert outcome == _FAILED and stream.closes == 1

    def test_the_failure_is_logged_with_its_text(self, h5, mocker, caplog):
        mocker.patch('zfsbackup.remote._path', side_effect=RuntimeError('boom'))
        with caplog.at_level(logging.ERROR, logger='zfsbackup.remote'):
            self.push(h5)
        assert 'Transfer failed for pool/data' in caplog.text and 'boom' in caplog.text

    def test_a_failing_stream_close_is_swallowed(self, h5, mocker):
        mocker.patch('zfsbackup.remote._path', side_effect=RuntimeError('boom'))
        stream = CountingStream()
        stream.close = MagicMock(side_effect=Exception('zfs send exited 1'))
        started = _SendStream(stream, 'fresh', True, by_name(h5.snaps, S2), by_name(h5.snaps, S1))
        assert h5.rbm._push_step(h5.transport, 'client1', h5.dsi, 'offsite', h5.snaps, started) == _FAILED
        stream.close.assert_called_once()

    def test_through_transfer_the_chain_stops_and_the_stream_is_reaped_once(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=0)
        mocker.patch('zfsbackup.remote.websocket.WebSocket', side_effect=RuntimeError('boom'))
        streams = []

        def start(*a, **k):
            streams.append(CountingStream())
            return streams[-1]
        h5.send_snapshot.side_effect = start
        assert chain.run(common=S1) is False
        assert len(streams) == 1 and streams[0].closes == 1


# ===========================================================================
# The server's retryable 503 on negotiate: a reload, or a busy / unsettled dataset
# ===========================================================================

from zfsbackup.remote import _retry_word  # noqa: E402

from zfsbackup import api as _api  # noqa: E402

#: The server's own bodies: the reload refusal, the busy refusal, and the "a parent has not settled"
#: 503 `backup_negotiate` answers for a dataset whose ancestor has no completed first backup.
RELOAD_503 = dict(_api.RELOADING_ERROR)
BUSY_503 = dict(_api.BUSY_ERROR)
UNSETTLED_ANCESTOR = 'pool/b/client1/parent'
UNSETTLED_503 = {
    'error': f'{UNSETTLED_ANCESTOR} has not settled yet; retry later', 'retry': True,
    'reason': 'unsettled_ancestor', 'ancestor': UNSETTLED_ANCESTOR,
}


class TestRetryableNegotiate503:
    """One 503 covers a reload and a busy / unsettled dataset; the server's own text tells which,
    the control flow is the same (`reloading=True`: skip the cycle, send nothing) and only the log
    wording differs."""

    @staticmethod
    def failure(h, body):
        h.server_says(status_code=503, json=body)
        with pytest.raises(NegotiationFailed) as exc:
            h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
        return exc.value

    def test_a_reload_is_reloading(self, h):
        err = self.failure(h, RELOAD_503)
        assert str(err) == 'server is reloading' and err.reloading is True

    def test_the_real_busy_refusal_is_plain_busy_and_still_retryable(self, h):
        err = self.failure(h, BUSY_503)
        assert str(err) == 'server is busy' and err.reloading is True

    def test_an_unsettled_ancestor_is_busy_with_the_servers_reason_and_still_retryable(self, h):
        err = self.failure(h, UNSETTLED_503)
        assert str(err) == f"server is busy: {UNSETTLED_503['error']}" and err.reloading is True

    def test_a_503_without_text_is_busy(self, h):
        err = self.failure(h, {'retry': True})
        assert str(err).startswith('server is busy') and err.reloading is True

    def test_a_503_without_retry_is_not_retryable(self, h):
        h.server_says(status_code=503, json={'error': 'remote_backup disabled'})
        with pytest.raises(NegotiationFailed) as exc:
            h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
        assert exc.value.reloading is False

    @pytest.mark.parametrize('text, word', [
        ('server is reloading', 'reloading'),
        ('server is busy', 'busy'),
        ('server is busy: an ancestor is still being created', 'busy: an ancestor is still being created'),
        ('server is busy: reloading ancestors', 'reloading'),        # the server's text decides, as in `_negotiate`
        ('server is ', 'busy'),
        ('', 'busy'),
    ])
    def test_retry_word(self, text, word):
        assert _retry_word(NegotiationFailed(text, reloading=True)) == word

    @pytest.mark.parametrize('body, word', [
        (RELOAD_503, 'reloading'), (BUSY_503, 'busy'),
        (UNSETTLED_503, f"busy: {UNSETTLED_503['error']}")], ids=['reload', 'busy', 'unsettled'])
    def test_backup_dataset_skips_the_cycle_without_sending(self, h, mocker, caplog, body, word):
        h.server_says(status_code=503, json=body)
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is False
        (rec,) = [r for r in caplog.records if 'skipping pool/data this cycle' in r.getMessage()]
        assert rec.levelname == 'INFO' and rec.getMessage() == f'server {BASE} is {word}; skipping pool/data this cycle'
        # (an unsettled ancestor is the one retryable case that also warns -- once: see
        # TestUnsettledAncestorWarnsOnce)
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING and 'waiting for' not in r.getMessage()]
        ws_cls.assert_not_called()
        h.nothing_started()
        assert not h.token_gets()
        h.set_anchor.assert_not_called()

    @pytest.mark.parametrize('error, word', [
        (NegotiationFailed('server is reloading', reloading=True), 'reloading'),
        (NegotiationFailed('server is busy', reloading=True), 'busy'),
        (NegotiationFailed('server is busy: not settled', reloading=True), 'busy: not settled'),
    ], ids=['reload', 'busy', 'unsettled'])
    def test_the_mid_chain_renegotiate_says_which(self, h5, mocker, caplog, error, word):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h5, mocker, sessions=6, negotiate=error)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert chain.run(common=S1) is False
        assert [r.levelname for r in messages(caplog, f'server {BASE} is {word}; pool/data continues next cycle')] == ['INFO']
        assert chain.ws_cls.call_count == 1
        h5.send_snapshot.assert_not_called()


# ===========================================================================
# The in-flight pin is kept between chain steps
# ===========================================================================


class TestPinKeptBetweenSteps:
    """A step's pin (`inflight.<dest>` = `<from>,<to>`) is not cleared when the step is
    acknowledged unless it is the last of the chain: the next step's `set_inflight` overwrites it,
    so there is never a moment with the sent snapshot unpinned and unanchored."""

    def clears(self, h):
        return [e for e in h.events if e == 'clear']

    def test_non_final_steps_push_without_clearing_and_the_final_one_with(self, h5, mocker):
        spy = mocker.spy(h5.rbm, '_push_step')
        chain = Chain(h5, mocker, sessions=5)
        assert chain.run(common=S2) is True
        assert [c.kwargs['clear_pin'] for c in spy.call_args_list] == [False, False, True]

    def test_the_next_pin_overwrites_the_kept_one(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1)
        pins = [e for e in h5.events if isinstance(e, tuple) and e[0] == 'pin']
        assert pins == [('pin', S1, S2), ('pin', S2, S3), ('pin', S3, S4), ('pin', S4, S5)]
        # between two pins: the anchor of the first, never a clear
        between = h5.events[h5.events.index(pins[0]):h5.events.index(pins[1])]
        assert ('anchor', S2) in between and 'clear' not in between

    def test_exactly_one_clear_after_the_last_step(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1)
        assert self.clears(h5) == ['clear']
        assert h5.events[-1] == 'clear' and h5.events[-2] == ('anchor', S5)
        h5.clear_inflight.assert_called_once()

    def test_a_full_send_clears_once(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=2)
        chain.run(common=None)
        h5.clear_inflight.assert_called_once()

    def test_a_stop_between_steps_clears_exactly_once_and_returns_true(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        stop = h5.stopper(False, False, True)      # entry, before step 1: no; before step 2: stop
        assert chain.run(common=S1, should_stop=stop) is True
        assert chain.sent() == [(S1, S2)]
        h5.clear_inflight.assert_called_once()
        non_stop = [e for e in h5.events if not (isinstance(e, tuple) and e[0] == 'stop?')]
        assert non_stop[-2:] == [('anchor', S2), 'clear']

    def test_a_stop_before_the_first_send_clears_nothing(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        assert chain.run(common=S1, should_stop=lambda: True) is False
        h5.clear_inflight.assert_not_called()

    @pytest.mark.parametrize('kind', ['server-error', 'busy-ack', 'unauthorized', 'send-timeout'])
    def test_a_failed_or_refused_step_keeps_its_pin_and_clears_nothing(self, h5, mocker, kind):
        chain, ok = TestChainFailuresStopIt().run_failing_at_step_2(h5, mocker, kind)
        assert ok is False
        h5.clear_inflight.assert_not_called()
        assert h5.events.count(('pin', S2, S3)) == 1          # the failed step's pin is the last one written

    def test_an_anchor_failure_keeps_its_pin_and_clears_nothing(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        h5.set_anchor.return_value = False
        assert chain.run(common=S1) is False
        h5.clear_inflight.assert_not_called()

    def test_an_anchor_failure_on_the_last_step_keeps_the_pin_too(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=5)
        h5.set_anchor.return_value = False
        assert chain.run(common=S4) is True
        h5.clear_inflight.assert_not_called()

    def test_the_presence_backstop_still_runs_on_every_step(self, h5, mocker, snapshot_presence):
        chain = Chain(h5, mocker, sessions=5)
        chain.run(common=S1)
        assert [c[0].split('@')[1] for c in snapshot_presence.calls] == [S2, S3, S4, S5]

    def test_a_resumed_step_followed_by_more_steps_does_not_clear(self, h5, mocker):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h5, mocker, sessions=6, negotiate=S2)
        assert chain.run(common=S1) is True
        non_stop = [e for e in h5.events if not (isinstance(e, tuple) and e[0] == 'stop?')]
        # resume S1->S2: anchor, no clear; then the fresh steps; one clear at the very end
        assert non_stop[:5] == [('pin', S1, S2), 'send-t', 'connect', 'ack', ('anchor', S2)]
        assert non_stop[5] == ('pin', S2, S3)
        assert self.clears(h5) == ['clear'] and non_stop[-1] == 'clear'

    def test_a_resume_that_is_the_latest_clears_once(self, h5, mocker):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=h5.latest.guid, fromguid=sinfo(S4).guid)
        chain = Chain(h5, mocker, sessions=2, negotiate=S4)
        assert chain.run(common=S1) is True
        h5.clear_inflight.assert_called_once()

    def test_a_stop_right_after_a_resume_clears_exactly_once(self, h5, mocker):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h5, mocker, sessions=6, negotiate=S2)
        assert chain.run(common=S1, should_stop=h5.stopper(False, False, True)) is True
        h5.clear_inflight.assert_called_once()
        chain.negotiate.assert_not_called()
        non_stop = [e for e in h5.events if not (isinstance(e, tuple) and e[0] == 'stop?')]
        assert non_stop[-2:] == [('anchor', S2), 'clear']

    def test_a_failed_resume_keeps_its_pin(self, h5, mocker):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h5, mocker, sessions=6, negotiate=S2)
        chain.sessions[0].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=S1) is False
        h5.clear_inflight.assert_not_called()

    def test_an_empty_rebuilt_chain_after_a_step_clears_once(self, h5, mocker):
        """S4 arrives, S5 fails and is gone: nothing is left to send; the pin kept after S4 is
        released."""
        relisted = [s for s in h5.snaps if s.name != S5]
        chain = Chain(h5, mocker, sessions=4, relist=[relisted])
        chain.sessions[1].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=S3) is True
        assert chain.sent() == [(S3, S4), (S4, S5)]
        h5.clear_inflight.assert_called_once()
        non_stop = [e for e in h5.events if not (isinstance(e, tuple) and e[0] == 'stop?')]
        assert non_stop[-1] == 'clear'

    def test_an_empty_rebuilt_chain_with_nothing_sent_clears_nothing(self, h5, mocker):
        relisted = [s for s in h5.snaps if s.name != S5]
        chain = Chain(h5, mocker, sessions=4, relist=[relisted])
        chain.sessions[0].send_binary.side_effect = websocket_timeout()
        assert chain.run(common=S4) is True
        h5.clear_inflight.assert_not_called()


class TestSettleBackstop:
    """`_settle_up_to_date`: before an existing pin is released, the snapshot the server already
    holds is checked to still be here."""

    def run(self, h5, mocker, anchor, pin=(S4, S5), dry=False):
        h5.get_anchor.return_value = anchor
        h5.get_inflight.return_value = pin
        h5.rbm.config.dry_run = dry
        chain = Chain(h5, mocker, sessions=1)
        return chain.run(common=S5)

    def test_present_writes_the_anchor_and_clears_the_pin(self, h5, mocker, snapshot_presence):
        assert self.run(h5, mocker, anchor=S4) is True
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5]
        h5.clear_inflight.assert_called_once()
        assert snapshot_presence.calls == [(f'pool/data@{S5}', 'guid')]

    def test_gone_restores_the_anchor_it_wrote_keeps_the_pin_and_says_so(self, h5, mocker, caplog, snapshot_presence):
        snapshot_presence.gone.add(S5)
        with caplog.at_level(logging.INFO):
            assert self.run(h5, mocker, anchor=S4) is True
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5, S4]
        h5.clear_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()
        (rec,) = messages(caplog, f'pool/data@{S5} no longer exists although offsite holds it', 'ERROR')
        assert 'keeping its in-flight pin' in rec.getMessage()

    def test_gone_with_no_previous_anchor_clears_the_one_it_wrote(self, h5, mocker, snapshot_presence):
        snapshot_presence.gone.add(S5)
        assert self.run(h5, mocker, anchor=None) is True
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5]
        h5.clear_anchor.assert_called_once_with(h5.dsi, 'offsite')
        h5.clear_inflight.assert_not_called()

    def test_gone_with_the_anchor_already_right_changes_no_anchor(self, h5, mocker, snapshot_presence):
        snapshot_presence.gone.add(S5)
        assert self.run(h5, mocker, anchor=S5) is True
        h5.set_anchor.assert_not_called()
        h5.clear_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()

    def test_a_recreated_snapshot_counts_as_gone(self, h5, mocker, snapshot_presence):
        snapshot_presence.replaced[S5] = 99
        assert self.run(h5, mocker, anchor=S4) is True
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5, S4]
        h5.clear_inflight.assert_not_called()

    def test_inconclusive_keeps_the_pin_with_a_warning_and_leaves_the_anchor(self, h5, mocker, caplog, snapshot_presence):
        snapshot_presence.unknown.add(S5)
        with caplog.at_level(logging.INFO):
            assert self.run(h5, mocker, anchor=S4) is True
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5]          # written, not restored
        h5.clear_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()
        (rec,) = messages(caplog, f'cannot confirm that pool/data@{S5} still exists')
        assert rec.levelname == 'WARNING' and 'keeping its in-flight pin' in rec.getMessage()

    def test_no_pin_means_no_extra_zfs_call(self, h5, mocker, snapshot_presence):
        assert self.run(h5, mocker, anchor=S4, pin=None) is True
        assert snapshot_presence.calls == []
        h5.clear_inflight.assert_not_called()
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S5]

    def test_an_anchor_that_was_right_with_a_pin_still_checks(self, h5, mocker, snapshot_presence):
        assert self.run(h5, mocker, anchor=S5) is True
        h5.set_anchor.assert_not_called()
        assert len(snapshot_presence.calls) == 1
        h5.clear_inflight.assert_called_once()

    def test_a_newer_anchor_left_alone_still_checks_before_releasing(self, h5, mocker, snapshot_presence):
        """Settling for S4 (latest) with the anchor on a newer S5: nothing written, so nothing to restore."""
        h5.get_anchor.return_value = S5
        h5.get_inflight.return_value = (S3, S4)
        chain = Chain(h5, mocker, sessions=1)
        snapshot_presence.gone.add(S4)
        assert chain.run(common=S4, latest=by_name(h5.snaps, S4)) is True
        h5.set_anchor.assert_not_called()
        h5.clear_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()

    def test_a_dry_run_writes_and_checks_nothing(self, h5, mocker, snapshot_presence):
        assert self.run(h5, mocker, anchor=S4, dry=True) is True
        assert snapshot_presence.calls == []
        h5.set_anchor.assert_not_called()
        h5.clear_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()

    def test_after_a_resume_the_renegotiate_settle_has_the_same_backstop(self, h5, mocker, snapshot_presence):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        h5.get_inflight.return_value = (S2, S5)
        snapshot_presence.gone.add(S5)
        chain = Chain(h5, mocker, sessions=2, negotiate=S5)
        assert chain.run(common=S1) is True
        h5.clear_inflight.assert_not_called()
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2, S5]
        h5.clear_anchor.assert_called_once_with(h5.dsi, 'offsite')     # there was no anchor before the settle wrote one


class TestBusyWording:
    def test_the_client_and_the_server_agree_on_the_busy_text(self):
        from zfsbackup import remote
        assert remote._BUSY_TEXT == _api.BUSY_ERROR['error']

    def test_the_real_busy_negotiate_never_says_busy_busy(self, h, mocker, caplog):
        h.server_says(status_code=503, json=BUSY_503)
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is False
        assert 'busy: busy' not in caplog.text
        assert f'server {BASE} is busy; skipping pool/data this cycle' in caplog.text

    def test_the_real_busy_mid_chain_renegotiate_never_says_busy_busy(self, h5, mocker, caplog):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        h5.server_says(status_code=503, json=BUSY_503)
        chain = Chain(h5, mocker, sessions=3, negotiate=REAL)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert chain.run(common=S1) is False
        assert 'busy: busy' not in caplog.text
        assert f'server {BASE} is busy; pool/data continues next cycle' in caplog.text

    def test_a_busy_final_ack_names_the_stream_and_never_says_busy_busy(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=3)
        chain.sessions[0].recv.return_value = json.dumps(_api.BUSY_ERROR)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert chain.run(common=S1) is False
        assert 'busy: busy' not in caplog.text
        assert f'server {BASE} is busy with another stream of pool/data; skipping this cycle' in caplog.text

    def test_retry_word_for_the_real_texts(self):
        assert _retry_word(NegotiationFailed('server is busy', reloading=True)) == 'busy'
        assert _retry_word(NegotiationFailed('server is reloading', reloading=True)) == 'reloading'
        reason = UNSETTLED_503['error']
        assert _retry_word(NegotiationFailed(f'server is busy: {reason}', reloading=True)) == f'busy: {reason}'


class TestUnsettledAncestorWarnsOnce:
    """A 503 `reason: unsettled_ancestor` is a WARNING once per (dataset, destination), then DEBUG,
    re-armed by a successful negotiate."""

    def negotiate(self, h, destination='offsite'):
        with pytest.raises(NegotiationFailed):
            h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, destination)

    @staticmethod
    def warnings(caplog):
        return [r for r in caplog.records if r.name == 'zfsbackup.remote' and r.levelname == 'WARNING'
                and 'waiting for' in r.getMessage()]

    def test_three_503s_give_one_warning(self, h, caplog):
        h.server_says(status_code=503, json=UNSETTLED_503)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for _ in range(3):
                self.negotiate(h)
        (rec,) = self.warnings(caplog)
        assert rec.getMessage() == (
            f"pool/data -> offsite: waiting for {UNSETTLED_ANCESTOR!r}, whose first backup has not "
            "completed on the server; pool/data is not sent until it has")
        assert len([r for r in caplog.records if 'waiting for' in r.getMessage()]) == 3      # 1 WARNING + 2 DEBUG
        assert not [r for r in caplog.records if r.name == 'zfsbackup.remote' and r.levelno >= logging.ERROR]

    def test_a_successful_negotiate_rearms_it(self, h, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            h.server_says(status_code=503, json=UNSETTLED_503)
            self.negotiate(h)
            self.negotiate(h)
            h.server_says(common=None, copy='none')
            h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
            h.server_says(status_code=503, json=UNSETTLED_503)
            self.negotiate(h)
        assert len(self.warnings(caplog)) == 2

    def test_another_destination_warns_separately(self, h, caplog):
        h.server_says(status_code=503, json=UNSETTLED_503)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            self.negotiate(h)
            self.negotiate(h, 'dc2')
            self.negotiate(h)
            self.negotiate(h, 'dc2')
        assert len(self.warnings(caplog)) == 2

    def test_another_ancestor_text_warns_again(self, h, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            h.server_says(status_code=503, json=UNSETTLED_503)
            self.negotiate(h)
            h.server_says(status_code=503, json={**UNSETTLED_503, 'ancestor': 'pool/b/client1/other'})
            self.negotiate(h)
        assert len(self.warnings(caplog)) == 2

    def test_busy_and_reload_503s_do_not_warn(self, h, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            for body in (BUSY_503, RELOAD_503):
                h.server_says(status_code=503, json=body)
                self.negotiate(h)
        assert self.warnings(caplog) == []
        assert not [r for r in caplog.records if r.name == 'zfsbackup.remote' and r.levelno >= logging.WARNING]

    def test_the_cycle_is_skipped_without_sending(self, h, mocker, caplog):
        h.server_says(status_code=503, json=UNSETTLED_503)
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is False
        ws_cls.assert_not_called()
        h.nothing_started()
        assert len(self.warnings(caplog)) == 1
        assert f"server {BASE} is busy: {UNSETTLED_503['error']}; skipping pool/data this cycle" in caplog.text

    def test_the_wording_matches_what_the_server_sends(self):
        """The body the tests use is the shape `backup_negotiate` returns."""
        import inspect
        source = inspect.getsource(_api.create_app)
        assert "'reason': 'unsettled_ancestor', 'ancestor': ancestor" in source
        assert "has not settled yet; retry later" in source


class TestNegotiateHttpErrors:
    """L3: a negotiate answered with HTTP >= 400 and a JSON `error` says what the server said."""

    @pytest.mark.parametrize('status', [400, 404, 422, 500])
    def test_the_servers_text_is_logged_at_error_and_raised(self, h, caplog, status):
        h.server_says(status_code=status, json={'error': 'config: unknown key "x"'})
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            with pytest.raises(NegotiationFailed) as exc:
                h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
        assert str(exc.value) == f'HTTP {status}: config: unknown key "x"' and exc.value.reloading is False
        (rec,) = [r for r in caplog.records if r.name == 'zfsbackup.remote' and r.levelname == 'ERROR']
        assert rec.getMessage() == (
            f'Negotiation failed for pool/data at {BASE}: HTTP {status}: config: unknown key "x"')

    def test_a_400_leads_to_no_send(self, h, mocker):
        h.server_says(status_code=400, json={'error': 'bad config'})
        mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        assert h.rbm.backup_dataset(h.dsi, cfg_for(None)) is False
        ws_cls.assert_not_called()
        h.nothing_started()

    def test_an_error_status_without_a_json_error_keeps_the_generic_text(self, h, caplog):
        h.server_says(status_code=400, text='<html>bad</html>')
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            with pytest.raises(NegotiationFailed):
                h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
        (rec,) = [r for r in caplog.records if r.name == 'zfsbackup.remote' and r.levelname == 'ERROR']
        assert 'Negotiation failed for pool/data' in rec.getMessage() and 'HTTP 400: ' not in rec.getMessage()

    def test_409_503_and_auth_keep_their_own_paths(self, h, caplog):
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            h.server_says(status_code=409, json={'error': 'held elsewhere'})
            with pytest.raises(NegotiationFailed) as exc:
                h.rbm._negotiate(h.transport, 'client1', h.dsi, h.snaps, 'offsite')
        assert str(exc.value) == 'held elsewhere'
        assert 'HTTP 409' not in caplog.text


class TestStreamRefusalToNegotiateAgain:
    """L4: the server's "negotiate again" stream refusal carries `retry: True`: an INFO skip, and the
    chain stops (`_STOP`) rather than failing the transfer."""

    REFUSAL = {
        'error': 'no configuration is recorded for pool/b/client1/data; negotiate again before streaming '
                 '(a restart of the server forgets the configuration of a dataset that has not been '
                 'received yet)',
        'retry': True,
    }

    def test_it_is_an_info_skip_naming_the_server_text(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=4)
        chain.sessions[0].recv.return_value = json.dumps(self.REFUSAL)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert chain.run(common=S1) is False
        own = [r for r in caplog.records if r.name == 'zfsbackup.remote' and r.levelno >= logging.INFO]
        skips = [r for r in own if 'asks to retry pool/data' in r.getMessage()]
        assert [r.levelname for r in skips] == ['INFO'] and 'negotiate again' in skips[0].getMessage()
        assert not [r for r in own if r.levelno >= logging.WARNING]
        assert 'Transfer failed' not in caplog.text

    def test_the_chain_stops_with_no_diagnosis_and_no_clear(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=4)
        chain.sessions[0].recv.return_value = json.dumps(self.REFUSAL)
        assert chain.run(common=S1) is False
        assert chain.ws_cls.call_count == 1 and chain.sent() == [(S1, S2)]
        chain.relist.assert_not_called()
        h5.set_anchor.assert_not_called()
        h5.clear_inflight.assert_not_called()

    def test_behind_a_failed_send_it_is_a_stop_too(self, h5, mocker):
        chain = Chain(h5, mocker, sessions=4)
        chain.sessions[0].send_binary.side_effect = BrokenPipeError('broken pipe')
        chain.sessions[0].recv.return_value = json.dumps(self.REFUSAL)
        assert chain.run(common=S1) is False
        chain.relist.assert_not_called()

    def test_the_same_refusal_without_retry_is_still_an_error(self, h5, mocker, caplog):
        chain = Chain(h5, mocker, sessions=4)
        chain.sessions[0].recv.return_value = json.dumps({'error': self.REFUSAL['error']})
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            chain.run(common=S1)
        assert 'Transfer failed for pool/data' in caplog.text

    def test_the_server_sends_it_with_retry(self):
        import inspect
        source = inspect.getsource(_api.create_app)
        i = source.index('negotiate again before streaming')
        assert "'retry': True" in source[i - 300:i + 600]


# ===========================================================================
# A connect refused right after a successful step is retried (the server may be exiting for a reload)
# ===========================================================================

import errno  # noqa: E402
import ssl  # noqa: E402
import websocket as _websocket  # noqa: E402

from zfsbackup.identity import PinMismatch  # noqa: E402
from zfsbackup.remote import CONNECT_RETRY_DELAYS, CONNECT_RETRY_SLICE  # noqa: E402


def reset():
    return ConnectionResetError(errno.ECONNRESET, 'Connection reset by peer')


def fail_connects(fake, when, error=reset):
    """`connect_pinned` raises `error()` for the calls `when(call number)` says (1 = the first
    connect of the cycle, i.e. step 1); the others hand out a socket."""
    def effect(*args):
        if when(len(fake.calls)):
            raise error()
        return MagicMock(name='tls_sock')
    fake.side_effect = effect


def tls_alert_error():
    exc = ssl.SSLError(1, '[SSL: TLSV1_ALERT_UNKNOWN_CA] tlsv1 alert unknown ca (_ssl.c:2648)')
    exc.reason = 'TLSV1_ALERT_UNKNOWN_CA'
    return exc


def slept(sleeps):
    return sum(c.args[0] for c in sleeps.call_args_list)


class TestConnectRetryAfterASuccessfulStep:
    NOT_ACCEPTING = (f'server {BASE} is not accepting connections right after a successful step '
                     '(probably reloading); pool/data -> offsite continues next cycle')

    def test_the_constants(self):
        assert CONNECT_RETRY_DELAYS == (1.0, 2.0, 4.0) and CONNECT_RETRY_SLICE == 0.25
        assert all(d / CONNECT_RETRY_SLICE == int(d / CONNECT_RETRY_SLICE) for d in CONNECT_RETRY_DELAYS)

    # -- it works on attempt 2, 3 or 4 -------------------------------------------------------------
    @pytest.mark.parametrize('failures', [1, 2, 3])
    def test_success_on_a_later_attempt_continues_the_chain(
        self, h5, mocker, fake_connect_pinned, sleeps, failures
    ):
        fail_connects(fake_connect_pinned, lambda n: 2 <= n <= 1 + failures)
        chain = Chain(h5, mocker, sessions=8)
        assert chain.run(common=S3) is True
        assert chain.sent() == [(S3, S4), (S4, S5)]                     # each zfs send started once
        assert len(fake_connect_pinned.calls) == 2 + failures           # step 1, the failed tries, step 2
        assert sum(c.args[0] for c in sleeps.call_args_list) == sum(CONNECT_RETRY_DELAYS[:failures])
        assert {c.args[0] for c in sleeps.call_args_list} == {CONNECT_RETRY_SLICE}
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S4, S5]
        assert h5.events.count(('pin', S4, S5)) == 1
        h5.clear_inflight.assert_called_once()                          # only after the last step

    def test_the_previous_pin_is_never_cleared_while_waiting(self, h5, mocker, fake_connect_pinned, sleeps):
        fail_connects(fake_connect_pinned, lambda n: n in (2, 3))
        events_at_sleep = []
        sleeps.side_effect = lambda *_: events_at_sleep.append(list(h5.events))
        chain = Chain(h5, mocker, sessions=8)
        assert chain.run(common=S3) is True
        assert events_at_sleep and all('clear' not in e for e in events_at_sleep)
        assert all(('anchor', S4) in e for e in events_at_sleep)        # step 1 is anchored and its pin kept

    def test_each_failed_attempt_closes_its_websocket_and_the_next_uses_a_fresh_one(
        self, h5, mocker, fake_connect_pinned
    ):
        fail_connects(fake_connect_pinned, lambda n: n in (2, 3))
        chain = Chain(h5, mocker, sessions=8)
        chain.run(common=S3)
        assert chain.ws_cls.call_count == 1 + 3                         # step 1, then three tries for step 2
        chain.sessions[1].close.assert_called()
        chain.sessions[2].close.assert_called()
        assert chain.sessions[3].send_binary.called                     # the third try carried the stream

    def test_a_failed_websocket_handshake_is_retried_too(self, h5, mocker, sleeps):
        chain = Chain(h5, mocker, sessions=8)
        chain.sessions[1].connect.side_effect = _websocket.WebSocketBadStatusException(
            'Handshake status 500', 500, 'Internal Server Error', {}, b'')
        assert chain.run(common=S3) is True
        assert slept(sleeps) == CONNECT_RETRY_DELAYS[0]
        assert chain.sent() == [(S3, S4), (S4, S5)]

    def test_a_ssl_error_that_is_not_an_alert_is_retried(self, h5, mocker, fake_connect_pinned, sleeps):
        fail_connects(fake_connect_pinned, lambda n: n == 2, error=lambda: ssl.SSLError('EOF occurred in violation of protocol'))
        chain = Chain(h5, mocker, sessions=8)
        assert chain.run(common=S3) is True
        assert slept(sleeps) == CONNECT_RETRY_DELAYS[0]

    # -- it gives up ---------------------------------------------------------------------------------
    def test_exhausted_retries_stop_quietly_and_change_nothing(
        self, h5, mocker, fake_connect_pinned, sleeps, caplog
    ):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.remote'):
            assert chain.run(common=S1) is False
        assert len(fake_connect_pinned.calls) == 1 + 4                  # step 1, then 1 + 3 attempts
        assert slept(sleeps) == sum(CONNECT_RETRY_DELAYS) == 7.0
        assert len(sleeps.call_args_list) == 28
        assert chain.sent() == [(S1, S2), (S2, S3)]                     # step 2's zfs send was spawned once
        assert h5.send_snapshot.call_count == 2
        own = [(r.levelname, r.getMessage()) for r in caplog.records
               if r.name == 'zfsbackup.remote' and r.levelno >= logging.INFO]
        assert ('INFO', self.NOT_ACCEPTING) in own
        assert not [lv for lv, _ in own if lv in ('WARNING', 'ERROR')]
        assert 'Transfer failed' not in caplog.text
        # no re-list, nothing changed: step 1's anchor and pin stay as they were
        chain.relist.assert_not_called()
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2]
        h5.clear_inflight.assert_not_called()
        assert [c.args[1:] for c in h5.set_inflight.call_args_list] == [('offsite', S1, S2), ('offsite', S2, S3)]

    def test_the_started_zfs_send_is_reaped_exactly_once(self, h5, mocker, fake_connect_pinned):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8)
        chain.run(common=S1)
        h5.streams[1].close.assert_called_once()                        # step 2's stream
        h5.streams[1].read.assert_not_called()                          # not a byte of it was read

    def test_exhausted_retries_do_not_look_for_a_vanished_member(self, h5, mocker, fake_connect_pinned):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8, relist=[[s for s in h5.snaps if s.name != S3]])
        assert chain.run(common=S1) is False
        chain.relist.assert_not_called()
        assert chain.sent() == [(S1, S2), (S2, S3)]                     # not rebuilt around S3

    # -- the first stream of a cycle is unchanged ------------------------------------------------------
    def test_a_first_step_connect_failure_is_the_old_error(
        self, h5, mocker, fake_connect_pinned, sleeps, caplog
    ):
        fail_connects(fake_connect_pinned, lambda n: True)
        chain = Chain(h5, mocker, sessions=8)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert chain.run(common=S1) is False
        assert len(fake_connect_pinned.calls) == 1
        sleeps.assert_not_called()
        assert [r.levelname for r in messages(caplog, 'Transfer failed for pool/data')] == ['ERROR']
        assert 'not accepting connections' not in caplog.text
        chain.relist.assert_called_once()                               # an ordinary failure looks for a vanished member
        h5.streams[0].close.assert_called_once()

    def test_resume_step_zero_is_not_retried(self, h5, mocker, fake_connect_pinned, sleeps, caplog):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        fail_connects(fake_connect_pinned, lambda n: True)
        chain = Chain(h5, mocker, sessions=8, negotiate=S2)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert chain.run(common=S1) is False
        assert len(fake_connect_pinned.calls) == 1
        sleeps.assert_not_called()
        assert messages(caplog, 'Transfer failed for pool/data', 'ERROR')

    def test_the_step_after_a_resume_is_retried(self, h5, mocker, fake_connect_pinned, sleeps):
        h5.token(TOKEN_INCR)
        h5.decodes_to(toguid=222, fromguid=111)
        fail_connects(fake_connect_pinned, lambda n: n == 2)
        chain = Chain(h5, mocker, sessions=8, negotiate=S2)
        assert chain.run(common=S1) is True
        assert slept(sleeps) == CONNECT_RETRY_DELAYS[0]
        assert [s for s, _ in chain.sent()] == [S2, S3, S4]

    # -- a stop ----------------------------------------------------------------------------------------
    def test_a_stop_before_the_first_retry_waits_for_nothing(self, h5, mocker, fake_connect_pinned, sleeps, caplog):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8)
        stop = h5.stopper(False, False, False, True)        # entry, step 1, step 2, then the first slice
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert chain.run(common=S1, should_stop=stop) is False
        sleeps.assert_not_called()
        assert len(fake_connect_pinned.calls) == 2
        assert 'stop requested; pool/data -> offsite continues next cycle' in caplog.text
        assert 'not accepting connections' not in caplog.text
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2]
        h5.clear_inflight.assert_not_called()
        h5.streams[1].close.assert_called_once()

    def test_a_stop_in_a_slice_ends_the_wait_there(self, h5, mocker, fake_connect_pinned, sleeps, caplog):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8)
        stop = h5.stopper(False, False, False, False, False, True)    # the third slice of the first wait
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert chain.run(common=S1, should_stop=stop) is False
        assert len(sleeps.call_args_list) == 2
        assert len(fake_connect_pinned.calls) == 2                       # no further attempt
        assert 'stop requested; pool/data -> offsite continues next cycle' in caplog.text
        h5.streams[1].close.assert_called_once()
        chain.relist.assert_not_called()

    def test_a_stop_after_the_last_slice_is_seen_before_the_next_attempt(
        self, h5, mocker, fake_connect_pinned, sleeps, caplog
    ):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8)
        slices = round(CONNECT_RETRY_DELAYS[0] / CONNECT_RETRY_SLICE)
        stop = h5.stopper(*([False] * (3 + slices)), True)               # the poll after the first wait
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            assert chain.run(common=S1, should_stop=stop) is False
        assert len(sleeps.call_args_list) == slices
        assert len(fake_connect_pinned.calls) == 2                       # the second attempt never started
        assert 'stop requested' in caplog.text

    def test_the_stop_is_asked_in_every_slice(self, h5, mocker, fake_connect_pinned, sleeps):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8)
        stop = h5.stopper()
        chain.run(common=S1, should_stop=stop)
        # entry + one per step + (slices + 1) per wait
        waits = sum(round(d / CONNECT_RETRY_SLICE) + 1 for d in CONNECT_RETRY_DELAYS)
        assert stop.polls() == 1 + 2 + waits

    # -- what is not retried ---------------------------------------------------------------------------
    @pytest.mark.parametrize('how', [
        'timeout-error', 'socket-timeout', 'ws-timeout', 'pin-mismatch', 'tls-alert',
        'upgrade-401', 'upgrade-403-not-trusted', 'upgrade-403-mismatch',
    ])
    def test_not_retried(self, h5, mocker, fake_connect_pinned, sleeps, how):
        import socket
        errors = {
            'timeout-error': lambda: TimeoutError('timed out'),
            'socket-timeout': lambda: socket.timeout('timed out'),
            'ws-timeout': lambda: _websocket.WebSocketTimeoutException('timed out'),
            'pin-mismatch': lambda: PinMismatch(SERVER_FP, 'cd' * 32),
            'tls-alert': tls_alert_error,
        }
        chain = Chain(h5, mocker, sessions=8)
        if how in errors:
            fail_connects(fake_connect_pinned, lambda n: n >= 2, error=errors[how])
        else:
            status, error = {'upgrade-401': (401, 'no client certificate'),
                             'upgrade-403-not-trusted': (403, 'client not trusted'),
                             'upgrade-403-mismatch': (403, 'client_id mismatch')}[how]
            chain.sessions[1].connect.side_effect = upgrade_refused(status, error)
        assert chain.run(common=S1) is False
        sleeps.assert_not_called()
        assert chain.ws_cls.call_count == 2                              # step 1 and ONE try of step 2
        assert chain.sent() == [(S1, S2), (S2, S3)]
        assert [c.args[1:][1] for c in h5.set_anchor.call_args_list] == [S2]
        h5.streams[1].close.assert_called_once()

    @pytest.mark.parametrize('how', ['pin-mismatch', 'tls-alert', 'upgrade-401'])
    def test_a_refusal_skips_the_destination_and_is_not_the_not_accepting_message(
        self, h5, mocker, fake_connect_pinned, how, caplog
    ):
        chain = Chain(h5, mocker, sessions=8)
        if how == 'upgrade-401':
            chain.sessions[1].connect.side_effect = upgrade_refused(401, 'no client certificate')
        else:
            fail_connects(fake_connect_pinned, lambda n: n >= 2,
                          error=(lambda: PinMismatch(SERVER_FP, 'cd' * 32)) if how == 'pin-mismatch' else tls_alert_error)
        with caplog.at_level(logging.INFO, logger='zfsbackup.remote'):
            chain.run(common=S1)
        assert h5.rbm._checked['offsite'][1] is False
        assert 'not accepting connections' not in caplog.text
        assert len([r for r in caplog.records if r.name == 'zfsbackup.remote' and r.levelname == 'ERROR']) == 1

    # -- failures after the connect are unchanged --------------------------------------------------------
    @pytest.mark.parametrize('how', ['send-timeout', 'broken-pipe', 'server-error', 'ack-timeout', 'done-fails'])
    def test_failures_after_entering_are_not_retried(self, h5, mocker, fake_connect_pinned, sleeps, how):
        chain = Chain(h5, mocker, sessions=8)
        ws = chain.sessions[1]
        if how == 'send-timeout':
            ws.send_binary.side_effect = websocket_timeout()
        elif how == 'broken-pipe':
            ws.send_binary.side_effect = BrokenPipeError('broken pipe')
        elif how == 'server-error':
            ws.recv.return_value = json.dumps({'status': 'error', 'error': 'disk full'})
        elif how == 'ack-timeout':
            ws.recv.side_effect = websocket_timeout()
        else:
            ws.send.side_effect = ConnectionResetError('reset')
        assert chain.run(common=S1) is False
        sleeps.assert_not_called()
        assert len(fake_connect_pinned.calls) == 2
        assert chain.sent() == [(S1, S2), (S2, S3)]
        chain.relist.assert_called_once()                                # the ordinary failure path

    # -- the time bound ------------------------------------------------------------------------------------
    def test_the_total_wait_is_bounded_by_the_delays(self, h5, mocker, fake_connect_pinned, sleeps):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8)
        chain.run(common=S1)
        assert slept(sleeps) <= sum(CONNECT_RETRY_DELAYS)
        assert len(fake_connect_pinned.calls) - 1 <= 1 + len(CONNECT_RETRY_DELAYS)

    def test_each_wait_is_its_own_delay_in_slices(self, h5, mocker, fake_connect_pinned, sleeps):
        fail_connects(fake_connect_pinned, lambda n: n >= 2)
        chain = Chain(h5, mocker, sessions=8)
        chain.run(common=S1)
        sizes = [round(d / CONNECT_RETRY_SLICE) for d in CONNECT_RETRY_DELAYS]
        assert sizes == [4, 8, 16] and len(sleeps.call_args_list) == sum(sizes)

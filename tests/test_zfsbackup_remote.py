"""Unit tests for `RemoteBackupManager`: negotiation, the resume protocol and the transfer.

The pinned-mTLS transport is faked here. `_negotiate`, `_fetch_resume_token`, `_get_resume_stream`
and `_transfer` take a `remote._Transport` (`make_transport`: a plain `requests.Session` that the
`requests_mock` fixture answers, and a stand-in SSL context), and the websocket's TLS connect is
`fake_connect_pinned`. The TLS behaviour itself -- the checks that run before any send, the error
mapping, the identity reload and real handshakes -- is `test_zfsbackup_remote_tls.py`.
"""

import json
import logging
import re
import pytest
from unittest.mock import MagicMock, patch

from _remote_support import (  # noqa: F401  (fixtures)
    HTTPS_BASE, SERVER_FP, client_tls_dir, fake_connect_pinned, make_transport, whoami_json,
    _client_tls_template,
)
from zfsbackup.config import (
    BackupConfig, DatasetConfig, RemoteDatasetConfig, Destination, RetentionRule, Duration,
)
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
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
# token only when it is provably ours (GUID-matched, no unexpected stream
# features); `_get_resume_stream` returns a `_SendStream` or None (skip the
# dataset this cycle); `_transfer` then drives the websocket
# (`/stream?mode=fresh|resume`), the in-flight pin, the anchor, and the one
# follow-up (D4).
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


def sinfo(name):
    return SnapshotInfo(ZFSSnapshot(Filesystem('pool/data'), name), 'autosnap')


def messages(caplog, text, level=None):
    return [r for r in caplog.records
            if text in r.getMessage() and (level is None or r.levelname == level)]


class Harness:
    """A `RemoteBackupManager` whose edges are mocked: the resume-token GET
    (via requests_mock), the token decoder, the local snapshot GUID listing,
    `zfs send` and the property writes. `order` records the interleaving."""

    def __init__(self, tmp_path, requests_mock, mocker, decode=True, guids=True, tls_dir=None):
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

        def make_stream(label):
            def start(*args, **kwargs):
                self.order.append(label)
                stream = _finite_stream()
                stream.read.side_effect = [b'chunk', b'']          # one chunk, then the end
                self.streams.append(stream)
                return stream
            return start

        self.send_snapshot = mocker.patch('zfsbackup.remote.zfs.send.snapshot', side_effect=make_stream('send'))
        self.send_resume = mocker.patch('zfsbackup.remote.zfs.send.resume', side_effect=make_stream('send-t'))
        # `decode=False` / `guids=False` leave the real `resume_info` / `_snapshot_guids` in place.
        if decode:
            self.resume_info = mocker.patch(
                'zfsbackup.remote.zfs.send.resume_info',
                return_value=ResumeToken(toname=f'pool/data@{MID}', toguid=222, fromguid=111, flags=frozenset()),
            )
        if guids:
            self.guids = mocker.patch.object(self.rbm, '_snapshot_guids', return_value=dict(GUIDS))
        lm = self.manager

        def pin(dsi, dest, from_name, to_name):
            self.order.append(('pin', from_name, to_name))
            return True

        self.set_inflight = mocker.patch.object(lm, 'set_inflight', side_effect=pin)
        self.clear_inflight = mocker.patch.object(lm, 'clear_inflight', return_value=True)
        self.set_anchor = mocker.patch.object(lm, 'set_anchor', return_value=True)
        self.get_anchor = mocker.patch.object(lm, 'get_anchor', return_value=None)
        self.get_inflight = mocker.patch.object(lm, 'get_inflight', return_value=None)

    # -- the server's answer to GET .../resume_token --------------------------
    def no_token(self):
        self.rm.get(self.url, json={'resume_token': None})

    def token(self, token=TOKEN_FULL):
        self.rm.get(self.url, json={'resume_token': token})

    def token_gets(self):
        """The resume-token GETs so far (`/whoami` is also a GET)."""
        return [r for r in self.rm.request_history
                if r.method == 'GET' and r.path.endswith('/resume_token')]

    def decodes_to(self, **fields):
        fields.setdefault('flags', frozenset())
        self.resume_info.return_value = ResumeToken(**fields)

    # -- act --------------------------------------------------------------------
    def start(self, common=None, snaps=None, should_stop=None):
        return self.rbm._get_resume_stream(
            self.transport, 'client1', self.dsi, self.latest, common, 'offsite',
            self.snaps if snaps is None else snaps, should_stop=should_stop,
        )

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
        h.guids.assert_not_called()

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
        h.send_snapshot.assert_called_once_with(h.latest.snapshot)
        h.send_resume.assert_not_called()
        h.resume_info.assert_not_called()                  # no token, nothing to decode
        assert f'Sending full snapshot pool/data@{NEW}' in caplog.text

    def test_an_incremental_send_from_the_common_snapshot(self, h, caplog):
        common = h.snaps[2]
        with caplog.at_level(logging.INFO):
            started = h.start(common=common)
        assert started.mode == 'fresh' and started.base is common and started.sent is h.latest
        h.send_snapshot.assert_called_once_with(h.latest.snapshot, since=common.snapshot)
        assert f'Sending incremental pool/data: {OLD} -> {NEW}' in caplog.text

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
        h.guids.assert_not_called()                         # decided before reading local GUIDs

    @pytest.mark.parametrize('flag', ['rawok', 'compressok', 'embedok', 'largeblockok', 'savedok'])
    def test_a_token_with_any_stream_feature_flag_is_not_resumed(self, h, caplog, flag):
        h.token()
        h.decodes_to(toguid=222, fromguid=111, flags=frozenset({flag}))
        msg = self.rejected(h, caplog, 'stream features this daemon does not send')
        assert flag in msg

    def test_several_flags_are_all_named_sorted(self, h, caplog):
        h.token()
        h.decodes_to(toguid=222, fromguid=111, flags=frozenset({'rawok', 'embedok', 'compressok'}))
        msg = self.rejected(h, caplog, 'stream features')
        assert 'compressok, embedok, rawok' in msg

    def test_a_corrupt_token_resume_token_error(self, h, caplog):
        h.token('garbage')
        h.resume_info.side_effect = ResumeTokenError('Not a valid ZFS resume token')
        self.rejected(h, caplog, 'the resume token is corrupt or unusable: Not a valid ZFS resume token')
        h.guids.assert_not_called()

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
        """The snapshot exists locally (its GUID is listed) but is not one of the
        daemon's: another prefix, say."""
        h.token()
        h.guids.return_value = {**GUIDS, 444: 'manual_20240102130000'}
        h.decodes_to(toguid=444, fromguid=None)
        self.rejected(h, caplog, 'is not a managed local snapshot of this dataset')

    def test_an_unmanaged_incremental_source(self, h, caplog):
        h.token()
        h.guids.return_value = {**GUIDS, 444: 'manual_20240101130000'}
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
        h.send_snapshot.assert_called_once_with(h.latest.snapshot, since=common.snapshot)

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
        h.guids.assert_not_called()

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


class TestSnapshotGuidFailure:
    def test_a_failing_guid_listing_skips_the_cycle(self, h, caplog):
        h.token()
        h.guids.side_effect = Exception('cannot open pool/data')
        with caplog.at_level(logging.INFO):
            assert h.start() is None
        (rec,) = messages(caplog, 'cannot check the resume token')
        assert rec.levelname == 'WARNING' and 'skipping this cycle' in rec.getMessage()
        assert 'cannot open pool/data' in rec.getMessage()
        assert not messages(caplog, 'Not resuming')
        h.nothing_started()

    def test_the_guid_listing_is_not_read_without_a_token(self, h):
        h.start()
        h.guids.assert_not_called()

    def test_transfer_is_false_when_the_listing_fails(self, h, mocker):
        h.token()
        h.guids.side_effect = Exception('boom')
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        assert h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, None, 'offsite', snapshots=h.snaps) is False
        ws_cls.assert_not_called()
        h.nothing_started()

    def test_snapshot_guids_is_one_zfs_list_for_snapshots_with_the_guid_property(self, make_h, mocker):
        h = make_h(guids=False)
        listing = []
        for short, guid in ((OLD, '111'), (MID, '0xde'), (NEW, '333'), ('nog', None)):
            snap = MagicMock()
            snap.short = short
            snap.__getitem__.side_effect = lambda k, g=guid: g
            listing.append(snap)
        lst = mocker.patch('zfsbackup.remote.zfs.list', return_value=listing)
        assert h.rbm._snapshot_guids(h.dsi) == {111: OLD, 0xde: MID, 333: NEW}
        lst.assert_called_once()
        assert lst.call_args.kwargs == {'roots': h.dsi.dataset, 'types': 'snapshot', 'properties': ['guid']}

    def test_snapshot_guids_raises_what_zfs_list_raises(self, make_h, mocker):
        h = make_h(guids=False)
        mocker.patch('zfsbackup.remote.zfs.list', side_effect=Exception('dataset does not exist'))
        with pytest.raises(Exception, match='does not exist'):
            h.rbm._snapshot_guids(h.dsi)


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

    def test_the_guid_listing_is_read_once(self, h):
        h.token()
        h.start()
        h.guids.assert_called_once()

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
        h.send_snapshot.assert_called_once_with(h.latest.snapshot, since=h.snaps[2].snapshot)

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
        h.guids.return_value = {0x61034bf73452096e: OLD, 333: NEW}
        h.rm.get(h.url, json={'resume_token': TOKEN_FULL})
        started = h.start()
        assert started.mode == 'resume' and started.sent.name == OLD and started.base is None
        argv = list(mock_subprocess.call_args_list[0].args[0])
        assert argv[1:] == ['send', '-nv', '-t', TOKEN_FULL]
        assert h.order == [('pin', None, OLD), 'send-t']

    def test_a_real_incremental_token_resumes(self, make_h, mock_subprocess):
        h = make_h(decode=False)
        mock_subprocess.setup(stdout=self.lines(INCR_CONTENTS))
        h.guids.return_value = {0x4f39e607147f3dc2: OLD, 0x61034ac2f730d529: MID, 333: NEW}
        h.rm.get(h.url, json={'resume_token': TOKEN_INCR})
        started = h.start()
        assert started.mode == 'resume' and started.sent.name == MID and started.base.name == OLD

    def test_a_real_token_for_a_destroyed_target_is_not_resumed(self, make_h, mock_subprocess, caplog):
        """`zfs send -nv -t` prints the contents, then fails with rc 255."""
        h = make_h(decode=False)
        mock_subprocess.setup(
            stdout=self.lines(FULL_CONTENTS[:-2]),
            stderr="cannot resume send: 'rtp/send/c@s1' used in the initial send no longer exists",
            returncode=255,
        )
        h.guids.return_value = {0x61034bf73452096e: OLD}
        h.rm.get(h.url, json={'resume_token': TOKEN_FULL})
        with caplog.at_level(logging.INFO):
            started = h.start()
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


class Chain:
    """`_transfer` end to end on top of a `Harness`: websocket sessions are
    handed out in order, `_negotiate` and the re-listing are stubbed."""

    def __init__(self, h, mocker, sessions=2, negotiate=MID):
        import json as _json
        self.h = h
        self.sessions = []
        for _ in range(sessions):
            ws = MagicMock()
            ws.recv.return_value = _json.dumps({'status': 'ok'})
            self.sessions.append(ws)
        self.ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket', side_effect=list(self.sessions))
        if isinstance(negotiate, Exception):
            self.negotiate = mocker.patch.object(h.rbm, '_negotiate', side_effect=negotiate)
        else:
            self.negotiate = mocker.patch.object(h.rbm, '_negotiate', return_value=negotiate)
        self.relist = mocker.patch.object(h.manager, 'list_snapshots', return_value=h.snaps)

    def run(self, **kw):
        h = self.h
        return h.rbm._transfer(h.transport, 'client1', h.dsi, h.latest, None, 'offsite', snapshots=h.snaps, **kw)

    def urls(self):
        return [s.connect.call_args.args[0].split('/stream', 1)[1] for s in self.sessions
                if s.connect.called]


class TestResumeThenFollowUp:
    """D4: a resumed send finishes the OLD snapshot. Continue ONCE in the same
    cycle (re-negotiate, a normal send to `latest`)."""

    @pytest.fixture
    def chain(self, h, mocker):
        h.rm.get(h.url, [{'json': {'resume_token': TOKEN_FULL}}, {'json': {'resume_token': None}}])
        h.decodes_to(toguid=222, fromguid=111)               # resumes OLD -> MID; latest is NEW
        return Chain(h, mocker)

    def test_a_follow_up_runs_once_to_the_latest_snapshot(self, h, chain, caplog):
        with caplog.at_level(logging.INFO):
            assert chain.run() is True
        assert chain.urls() == [f'?mode=resume&from={OLD}', f'?mode=fresh&from={MID}']
        assert chain.ws_cls.call_count == 2
        chain.negotiate.assert_called_once()
        chain.relist.assert_called_once()                    # re-listed, not reused
        assert f'Resumed pool/data@{MID} (from {OLD}) to offsite' in caplog.text
        assert f'Backup complete: pool/data@{NEW} (incremental from {MID})' in caplog.text

    def test_each_leg_has_its_own_pin_and_anchor(self, h, chain):
        chain.run()
        assert [o for o in h.order if isinstance(o, tuple)] == [('pin', OLD, MID), ('pin', MID, NEW)]
        assert h.order == [('pin', OLD, MID), 'send-t', ('pin', MID, NEW), 'send']
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID), ('offsite', NEW)]
        assert h.clear_inflight.call_count == 2
        h.send_snapshot.assert_called_once_with(h.latest.snapshot, since=h.snaps[1].snapshot)

    def test_the_follow_up_renegotiates_with_the_current_listing(self, h, chain):
        chain.run()
        args = chain.negotiate.call_args.args
        assert args == (h.transport, 'client1', h.dsi, h.snaps, 'offsite')

    def test_there_is_never_a_second_follow_up(self, h, mocker):
        """The follow-up may itself resume a token (mode=resume, not at latest):
        it must not chain a third send."""
        h.rm.get(h.url, [{'json': {'resume_token': TOKEN_FULL}}, {'json': {'resume_token': TOKEN_INCR}}])
        infos = iter([ResumeToken(toguid=111, fromguid=None, flags=frozenset()),     # resume OLD (full)
                      ResumeToken(toguid=222, fromguid=111, flags=frozenset())])     # then MID, still < NEW
        h.resume_info.side_effect = lambda token: next(infos)
        chain = Chain(h, mocker, sessions=3, negotiate=OLD)
        assert chain.run() is True
        assert chain.ws_cls.call_count == 2
        chain.negotiate.assert_called_once()
        assert chain.urls() == ['?mode=resume', f'?mode=resume&from={OLD}']

    def test_no_follow_up_when_the_resumed_snapshot_is_the_latest(self, h, mocker, caplog):
        h.rm.get(h.url, json={'resume_token': TOKEN_INCR})
        h.decodes_to(toguid=333, fromguid=222)
        chain = Chain(h, mocker)
        with caplog.at_level(logging.INFO):
            assert chain.run() is True
        assert chain.ws_cls.call_count == 1
        chain.negotiate.assert_not_called()
        chain.relist.assert_not_called()
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', NEW)]
        assert f'Resumed pool/data@{NEW} (from {MID}) to offsite' in caplog.text

    def test_no_follow_up_after_a_fresh_send(self, h, mocker):
        chain = Chain(h, mocker)
        assert chain.run() is True
        assert chain.ws_cls.call_count == 1 and chain.urls() == ['?mode=fresh']
        chain.negotiate.assert_not_called()

    def test_a_stop_pending_after_the_resume_skips_the_follow_up(self, h, chain, caplog):
        calls = iter([False, False, True])                   # not stopping before/at the send; stopping after
        with caplog.at_level(logging.INFO):
            assert chain.run(should_stop=lambda: next(calls)) is True
        assert chain.ws_cls.call_count == 1
        chain.negotiate.assert_not_called()
        assert f'stop requested; pool/data -> offsite continues to {NEW} next cycle' in caplog.text
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID)]   # resume was kept

    def test_a_stop_pending_from_the_start_sends_nothing(self, h, chain):
        assert chain.run(should_stop=lambda: True) is False
        chain.ws_cls.assert_not_called()
        h.nothing_started()

    def test_a_stop_that_never_comes_is_polled_before_each_send(self, h, chain):
        stop = MagicMock(return_value=False)
        assert chain.run(should_stop=stop) is True
        # leg 1: _transfer entry, just before `zfs send -t`; between the legs; leg 2: entry, just before `zfs send`
        assert stop.call_count == 5

    def test_a_failed_follow_up_negotiate_is_false_after_the_resume_completed(self, h, mocker):
        h.rm.get(h.url, json={'resume_token': TOKEN_FULL})
        h.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h, mocker, negotiate=NegotiationFailed('server said no'))
        assert chain.run() is False
        assert chain.ws_cls.call_count == 1
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID)]   # leg 1 is safe
        h.clear_inflight.assert_called_once()

    def test_a_reloading_follow_up_negotiate_is_false_at_info(self, h, mocker, caplog):
        h.rm.get(h.url, json={'resume_token': TOKEN_FULL})
        h.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h, mocker, negotiate=NegotiationFailed('server is reloading', reloading=True))
        with caplog.at_level(logging.INFO):
            assert chain.run() is False
        assert [r.levelname for r in messages(caplog, 'pool/data continues next cycle')] == ['INFO']
        assert chain.ws_cls.call_count == 1

    def test_a_failed_follow_up_send_is_false_and_leaves_its_pin(self, h, chain):
        chain.sessions[1].send_binary.side_effect = websocket_timeout()
        assert chain.run() is False
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', MID)]   # NEW never anchored
        assert h.clear_inflight.call_count == 1                                          # only leg 1's pin
        assert h.order[-2:] == [('pin', MID, NEW), 'send']

    def test_an_empty_re_listing_falls_back_to_the_listing_in_hand(self, h, mocker):
        h.rm.get(h.url, [{'json': {'resume_token': TOKEN_FULL}}, {'json': {'resume_token': None}}])
        h.decodes_to(toguid=222, fromguid=111)
        chain = Chain(h, mocker)
        chain.relist.return_value = []
        assert chain.run() is True
        assert chain.negotiate.call_args.args[3] is h.snaps

    def test_a_failed_leg_one_never_reaches_the_follow_up(self, h, chain):
        chain.sessions[0].send_binary.side_effect = websocket_timeout()
        assert chain.run() is False
        assert chain.ws_cls.call_count == 1
        chain.negotiate.assert_not_called()
        h.set_anchor.assert_not_called()
        h.clear_inflight.assert_not_called()


def websocket_timeout():
    import websocket
    return websocket.WebSocketTimeoutException('timed out')


class TestStopRecheckedRightBeforeZfsSend:
    """The last look before `zfs send` is spawned (never during the stream)."""

    def test_a_stop_before_a_fresh_send_starts_nothing(self, h, caplog):
        with caplog.at_level(logging.INFO):
            assert h.start(should_stop=lambda: True) is None
        h.nothing_started()
        assert f'stop requested; not starting a send of pool/data' in caplog.text

    def test_a_stop_before_a_resume_starts_nothing_and_writes_no_pin(self, h, caplog):
        h.token()
        with caplog.at_level(logging.INFO):
            assert h.start(should_stop=lambda: True) is None
        h.nothing_started()
        assert 'stop requested; not resuming pool/data' in caplog.text

    def test_a_stop_after_a_rejected_token_does_not_fall_through_to_a_fresh_send(self, h, caplog):
        h.token()
        h.decodes_to(toguid=999)
        with caplog.at_level(logging.INFO):
            assert h.start(should_stop=lambda: True) is None
        h.nothing_started()
        assert messages(caplog, 'Not resuming pool/data', 'WARNING')

    def test_a_stop_after_zfs_send_t_failed_to_start_does_not_start_a_fresh_one(self, h):
        h.token()
        h.send_resume.side_effect = Exception('bad')
        stops = iter([False, True])                       # fine before the resume, stopping before the fallback
        assert h.start(should_stop=lambda: next(stops)) is None
        h.send_snapshot.assert_not_called()

    def test_no_stop_starts_normally(self, h):
        assert h.start(should_stop=lambda: False).mode == 'fresh'
        h.clear_inflight.assert_not_called()

    def test_the_stop_is_asked_once_per_start(self, h):
        stop = MagicMock(return_value=False)
        h.start(should_stop=stop)
        assert stop.call_count == 1
        stop.reset_mock()
        h.token()
        h.start(should_stop=stop)
        assert stop.call_count == 1


class TestResumeBehindANewerAnchor:
    def test_the_pin_stays_after_the_resume_and_the_follow_up_replaces_and_clears_it(self, h, mocker):
        h.rm.get(h.url, [{'json': {'resume_token': TOKEN_FULL}}, {'json': {'resume_token': None}}])
        h.decodes_to(toguid=222, fromguid=111)
        h.get_anchor.return_value = NEW                       # already ahead of the resumed MID
        chain = Chain(h, mocker)
        assert chain.run() is True
        assert h.order == [('pin', OLD, MID), 'send-t', ('pin', MID, NEW), 'send']
        # leg 1: anchor kept, pin kept.  leg 2: anchor written (NEW), pin cleared once
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', NEW)]
        assert h.clear_inflight.call_count == 1


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
        h.guids.assert_not_called()
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
        chain = Chain(h, mocker, sessions=2, negotiate=OLD)
        return h, chain, h.dsi.config.remote[0]

    def test_a_normal_cycle_sends_incrementally_from_the_negotiated_snapshot(self, wired):
        h, chain, cfg = wired
        assert h.rbm.backup_dataset(h.dsi, cfg) is True
        assert chain.urls() == [f'?mode=fresh&from={OLD}']
        assert [c.args[1:] for c in h.set_anchor.call_args_list] == [('offsite', NEW)]

    def test_should_stop_is_checked_before_any_send_or_token_request(self, wired):
        h, chain, cfg = wired
        assert h.rbm.backup_dataset(h.dsi, cfg, should_stop=lambda: True) is False
        assert not h.token_gets()
        chain.ws_cls.assert_not_called()
        h.nothing_started()

    def test_a_resumable_token_is_resumed_through_backup_dataset(self, wired):
        h, chain, cfg = wired
        h.rm.get(h.url, [{'json': {'resume_token': TOKEN_FULL}}, {'json': {'resume_token': None}}])
        h.decodes_to(toguid=222, fromguid=111)
        assert h.rbm.backup_dataset(h.dsi, cfg, should_stop=lambda: False) is True
        # the interrupted OLD -> MID first, then the follow-up to NEW (`_negotiate` is stubbed to say OLD)
        assert chain.urls() == [f'?mode=resume&from={OLD}', f'?mode=fresh&from={OLD}']

    def test_a_skip_from_the_resume_token_is_false_and_leaves_everything(self, wired):
        h, chain, cfg = wired
        h.rm.get(h.url, status_code=500)
        assert h.rbm.backup_dataset(h.dsi, cfg) is False
        chain.ws_cls.assert_not_called()
        h.nothing_started()
        h.set_anchor.assert_not_called()


class TestTransfer:
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
        snap = MagicMock()
        snap.name = name
        snap.snapshot = MagicMock()
        return snap

    def test_returns_false_when_stream_is_none(self, tmp_path, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        mocker.patch.object(rbm, '_get_resume_stream', return_value=None)
        latest = self._make_snap()
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is False

    def test_dry_run_returns_true_without_websocket(self, tmp_path, mocker):
        config = _make_config(dry_run=True)
        manager = DatasetManager(config)
        rbm = _rbm(config, manager)
        dsi = manager.datasets[0]

        mock_stream = MagicMock()
        mocker.patch.object(rbm, '_get_resume_stream', return_value=_SendStream(mock_stream, 'fresh'))
        mock_ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')

        latest = self._make_snap()
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is True
        mock_ws_cls.assert_not_called()

    def test_connection_error_returns_false(self, tmp_path, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        mock_stream = MagicMock()
        mocker.patch.object(rbm, '_get_resume_stream', return_value=_SendStream(mock_stream, 'fresh'))

        mock_ws = MagicMock()
        mock_ws.connect.side_effect = Exception('connection refused')
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        latest = self._make_snap()
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is False
        mock_ws.close.assert_called()

    def test_server_error_response_returns_false(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'data', b'']
        mocker.patch.object(rbm, '_get_resume_stream', return_value=_SendStream(mock_stream, 'fresh'))

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
        mocker.patch.object(rbm, '_get_resume_stream', return_value=_SendStream(mock_stream, 'fresh'))

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        mock_set_anchor = mocker.patch.object(rbm.local_manager, 'set_anchor')

        latest = self._make_snap('autosnap_20240202120000')
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is True
        mock_set_anchor.assert_called_once_with(dsi, 'offsite', 'autosnap_20240202120000')

    def test_successful_transfer_logs_superseded_anchor(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'']
        mocker.patch.object(rbm, '_get_resume_stream', return_value=_SendStream(mock_stream, 'fresh'))

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        mocker.patch.object(rbm.local_manager, 'get_anchor', return_value='autosnap_OLD')
        mocker.patch.object(rbm.local_manager, 'set_anchor')

        latest = self._make_snap('autosnap_20240202120000')
        result = rbm._transfer(tr(), 'client1', dsi, latest, None, 'offsite', snapshots=[latest])
        assert result is True

    def test_incremental_transfer_uses_ws_url(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'', ]
        common = self._make_snap('autosnap_20240101120000')
        mocker.patch.object(rbm, '_get_resume_stream',
                            return_value=_SendStream(mock_stream, 'fresh', False, None, common))

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)
        mocker.patch.object(rbm.local_manager, 'set_anchor')

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

        mock_stream = MagicMock()
        mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=mock_stream)

        from libzfseasy.types import Filesystem, Snapshot as ZFSSnapshot
        snap_mock = MagicMock(spec=ZFSSnapshot)
        snap_mock.short = 'autosnap_20240101120000'
        snap_info = MagicMock()
        snap_info.name = 'autosnap_20240101120000'
        snap_info.snapshot = snap_mock

        mocker.patch.object(manager, 'list_snapshots', return_value=[snap_info])

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True


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
        snap = MagicMock()
        snap.name = 'autosnap_20240101120000'
        mocker.patch.object(manager, 'list_snapshots', return_value=[snap])
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
        snap = MagicMock()
        snap.name = 'autosnap_20240101120000'
        mocker.patch.object(manager, 'list_snapshots', return_value=[snap])
        sends = {
            'transfer': mocker.spy(rbm, '_transfer'),
            'resume': mocker.spy(rbm, '_get_resume_stream'),
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
        latest = MagicMock()
        latest.name = 'autosnap_1'
        with caplog.at_level(logging.INFO):
            assert rbm._get_resume_stream(tr(), 'client1', dsi, latest, None, 'offsite', [latest]) is None
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
        latest = MagicMock()
        latest.name = 'autosnap_1'
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
        latest = MagicMock()
        latest.name = 'autosnap_1'
        with caplog.at_level(logging.INFO):
            assert rbm._get_resume_stream(tr(), 'client1', dsi, latest, None, 'offsite', [latest]) is None
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
        mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=_finite_stream())
        mocker.patch.object(rbm.local_manager, 'set_inflight', return_value=True)
        started = rbm._get_resume_stream(
            make_transport(self.BASE, session=session), client, dsi, MagicMock(), None, 'offsite', []
        )
        assert started is not None and started.mode == 'fresh'
        assert session.get.call_args.args[0] == f'{self.BASE}/backup/{quoted}/resume_token'

    def _stream_url(self, tmp_path, mocker, name, client, common, base=None, mode='fresh'):
        rbm, dsi = self._rbm(tmp_path, name)
        common_snap = None
        if common is not None:
            common_snap = MagicMock()
            common_snap.name = common
        mocker.patch.object(rbm, '_get_resume_stream', return_value=_SendStream(
            _finite_stream(), mode, False, None, common_snap))
        mocker.patch.object(rbm.local_manager, 'get_anchor', return_value=None)
        mocker.patch.object(rbm.local_manager, 'set_anchor', return_value=True)
        ws = MagicMock()
        ws.recv.return_value = '{"status": "ok"}'
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=ws)
        latest = MagicMock()
        assert rbm._transfer(make_transport(base or self.BASE), client, dsi, latest, common_snap, 'offsite',
                             snapshots=[latest], followup=False) is True
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
        started = mocker.patch.object(rbm, '_get_resume_stream')
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
        started = mocker.patch.object(rbm, '_get_resume_stream')
        with caplog.at_level(logging.DEBUG):
            for _ in range(3):
                assert rbm.backup_dataset(dsi, dsi.config.remote[0]) is False
        assert [r.levelname for r in messages(caplog, 'server refused pool/data: held elsewhere')] == [
            'ERROR', 'DEBUG', 'DEBUG']
        assert not messages(caplog, 'Negotiation failed')
        started.assert_not_called()

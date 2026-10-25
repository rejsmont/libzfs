"""M9 of the TLS/mTLS plan: the API worker serves HTTPS only, with a per-connection
handshake, a `TrustGateway` over the pinned client certificates, and a server identity
of its own.

Layout
------

* `TestPerConnectionServer` -- the server class from `workers._bind_api_server` on a
  real loopback socket: HTTPS only, TLS 1.3 only, a silent peer blocks nobody,
  `shutdown()` with stalled handshakes, slots released on handshake failure,
  revocation and addition reach the next connection (and not the one in flight).
* `TestTrustGateway*` -- the gateway on its scripted seams (no database): fail closed,
  polling, a failed read keeps the view and logs once per distinct error, a bad PEM is
  dropped while the others work; any different key is installed (a rolled-back database
  with a WARNING); a failed rebuild keeps the old context and the old pins minus the
  revoked ones, and an `IdentityError` asks for a restart; `poll()` never waits for a
  refresh; concurrent refresh/poll never go backwards. `TestStoreCallLock`: every
  `store.trust` call is serialised (and the lock is fresh after a fork).
* `TestConnectionLimits` -- the total cap, the anonymous cap, the absolute request
  deadline (a dribbling peer is cut off), pinned versus anonymous after the app starts.
* `TestRejectLinger` -- after a failed handshake the connection is half-closed and the peer's
  bytes are discarded until it closes or `reject_linger` passes, so a TLS 1.3 client that
  finished its handshake first reads the rejection alert instead of an EOF or a reset; 0 closes
  at once; slots come back; a successful handshake never lingers.
* `TestTrustPollThread` / `TestApiWorkerRestartOnIdentityChange` /
  `TestApiStartSweepAndSetupFailures` -- the API worker's own threads and exits.
* `TestRealApiWorker` -- a real `ApiWorker` process under fork, spawn and forkserver
  against a real config database: revocation and addition through
  `zfsbackup.config.store.trust` take effect with no restart.
* `TestDryRunApiWorker` / `TestServerIdentityAtStartup` / `TestTestConfigTls` /
  `TestRealDaemonTls` -- where the server identity comes from: created by the
  supervisor at startup, never created in a dry run (a throwaway identity in a private
  temporary directory instead), reported read-only by `--test-config`.
* `TestHandshakeValidityWarning` / `TestValidityWarnThrottle` -- S-12: a pinned client
  certificate that is expired / not yet valid is one WARNING per peer IP per hour on
  the server (every other handshake failure stays DEBUG); the per-IP throttle.
* `TestStoreWarningDedupe` -- `api.dedupe_store_warnings` and its uninstall handle (K-5),
  installed by the API worker for the length of its run only.

Hygiene: no `zfs` (the conftest stub), servers bind `127.0.0.1` port 0 or a free port,
every thread and child process is stopped in a `finally`, daemons run in their own
session and are SIGKILLed as a group if they do not leave.
"""

import contextlib
import dataclasses
import datetime as dt
import errno
import fcntl
import json
import logging
import multiprocessing
import os
import signal
import socket
import ssl
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

from _api_support import make_client_cert
from _clock_support import FakeTime
from _hermetic import hermetic_env
from _remote_support import write_identity_files
from _tls_support import (
    IO_TIMEOUT,
    LOOPBACK,
    StubTrust,
    client_identity,
    ensure_server_identity,
    https_get,
    insecure_client_context,
    make_gateway,
    pin_client,
    raw_connect,
    server_identity,
    trusted_pin,
    try_get,
    unpin_client,
)
from zfsbackup import api, daemon as daemon_module, identity, workers
from zfsbackup.logsetup import PEER_LOGGER
from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig
from zfsbackup.config.store import dispose_all
from zfsbackup.stopflag import StopFlag

ROOT = Path(__file__).resolve().parent.parent

START_METHODS = [
    m for m in ("fork", "spawn", "forkserver") if m in multiprocessing.get_all_start_methods()
]

#: A stand-in WSGI app: reports what the TLS layer put in the environ.
def echo_app(environ, start_response):
    body = json.dumps({
        "scheme": environ.get("wsgi.url_scheme"),
        "cert": environ.get("SSL_CLIENT_CERT"),
        "path": environ.get("PATH_INFO"),
    }).encode()
    start_response("200 OK", [("Content-Type", "application/json"),
                              ("Content-Length", str(len(body)))])
    return [body]


#: Armoured like a certificate but not one: `load_verify_locations` rejects it.
#: (Text with no BEGIN line at all is skipped by OpenSSL when other pins load, so it
#: would not make the whole set fail.)
_CORRUPT_PEM = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((LOOPBACK, 0))
        return s.getsockname()[1]


def _repeat_filters() -> dict:
    """-> `{store logger name: [its _RepeatFilter objects]}`, only loggers that carry any."""
    found = {}
    for name in api._STORE_LOGGERS:
        mine = [f for f in logging.getLogger(name).filters if isinstance(f, api._RepeatFilter)]
        if mine:
            found[name] = mine
    return found


def _eventually(predicate, timeout=20.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


class Serving:
    """`workers._bind_api_server` served from a thread; shut down on exit."""

    def __init__(self, app, gateway, **kwargs):
        cls = workers._api_server_class()
        self.server = cls(LOOPBACK, 0, app, gateway, **kwargs)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.port = self.server.server_port

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop()

    def stop(self):
        self.server.shutdown()
        self.thread.join(10)
        self.server.server_close()
        assert not self.thread.is_alive()


@pytest.fixture
def pki(tmp_path):
    """-> a namespace: a gateway over a stub trust, and client identities `alice`
    (pinned by `trust.set_pins`) and `mallory` (never pinned)."""

    class Pki:
        pass

    p = Pki()
    gateway, server, trust = make_gateway(tmp_path / "srv.tls")
    p.gateway, p.server, p.trust = gateway, server, trust
    p.alice = client_identity(tmp_path / "alice.tls")
    p.mallory = client_identity(tmp_path / "mallory.tls")
    p.pin_alice = lambda: (trust.set_pins([trusted_pin(p.alice, "alice")],
                                          generation=trust.generation + 1), gateway.refresh())
    p.unpin_alice = lambda: (trust.set_pins([], generation=trust.generation + 1),
                             gateway.refresh())
    return p


# ===========================================================================
# The server class
# ===========================================================================


@pytest.mark.integration
class TestPerConnectionServer:
    # -- HTTPS only ----------------------------------------------------------

    @pytest.mark.parametrize("gateway", [
        pytest.param(object(), id="no-current_context"),
        pytest.param(type("G", (), {"current_context": lambda self: None})(), id="none"),
        pytest.param(type("G", (), {"current_context": lambda self: "ctx"})(), id="not-a-context"),
        pytest.param(None, id="no-gateway"),
    ])
    def test_a_gateway_without_a_tls_context_is_refused(self, gateway):
        with pytest.raises(TypeError, match="HTTPS only"):
            workers._bind_api_server(echo_app, LOOPBACK, 0, gateway)

    def test_a_refused_gateway_leaves_no_listening_socket(self):
        port = _free_port()
        with pytest.raises(TypeError):
            workers._bind_api_server(echo_app, LOOPBACK, port, object())
        with socket.socket() as s:
            s.bind((LOOPBACK, port))                       # still free

    def test_there_is_no_plaintext_mode(self, pki):
        """The class cannot be built without a gateway, and the wrapper takes none
        optionally."""
        import inspect

        params = inspect.signature(workers._bind_api_server).parameters
        assert list(params) == ["app", "host", "port", "gateway"]
        assert params["gateway"].default is inspect.Parameter.empty

    def test_the_listening_socket_is_not_wrapped_and_the_context_is_only_for_the_environ(
        self, pki
    ):
        server = workers._bind_api_server(echo_app, LOOPBACK, 0, pki.gateway)
        try:
            assert not isinstance(server.socket, ssl.SSLSocket)
            assert isinstance(server.ssl_context, ssl.SSLContext)
            assert server.ssl_context is pki.gateway.current_context()
        finally:
            server.server_close()

    # -- TLS 1.3 only --------------------------------------------------------

    def test_a_tls_12_client_is_refused(self, pki):
        ctx = insecure_client_context(maximum_version=ssl.TLSVersion.TLSv1_2)
        with Serving(echo_app, pki.gateway) as s:
            with pytest.raises(ssl.SSLError):
                https_get(s.port, "/", ctx)
            assert https_get(s.port, "/")[0] == 200            # and the server is still fine

    def test_tls_13_is_what_is_negotiated(self, pki):
        with Serving(echo_app, pki.gateway) as s:
            with socket.create_connection((LOOPBACK, s.port), timeout=IO_TIMEOUT) as raw:
                with insecure_client_context().wrap_socket(raw) as tls:
                    assert tls.version() == "TLSv1.3"

    def test_a_plaintext_request_gets_no_http_answer_and_the_server_lives(self, pki):
        with Serving(echo_app, pki.gateway) as s:
            with raw_connect(s.port) as sock:
                sock.sendall(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
                try:
                    data = sock.recv(4096)
                except OSError:
                    data = b""
                assert not data.startswith(b"HTTP/")
            assert https_get(s.port, "/")[0] == 200

    # -- client certificates -------------------------------------------------

    def test_a_client_without_a_certificate_completes_the_handshake_and_has_no_cert(self, pki):
        with Serving(echo_app, pki.gateway) as s:
            status, body = https_get(s.port, "/x")
        seen = json.loads(body)
        assert status == 200
        assert seen["scheme"] == "https" and seen["cert"] is None and seen["path"] == "/x"

    def test_a_pinned_client_certificate_reaches_the_app(self, pki):
        pki.pin_alice()
        with Serving(echo_app, pki.gateway) as s:
            status, body = https_get(s.port, "/", insecure_client_context(pki.alice))
        seen = json.loads(body)
        assert status == 200
        assert identity.fingerprint_pem(seen["cert"]) == pki.alice.fingerprint

    def test_an_unpinned_client_certificate_is_refused_at_the_handshake(self, pki):
        pki.pin_alice()
        with Serving(echo_app, pki.gateway) as s:
            result = try_get(s.port, "/", insecure_client_context(pki.mallory))
            assert isinstance(result, Exception)
            assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200

    def test_nobody_is_trusted_before_the_first_read(self, pki):
        assert pki.gateway.view.key is None
        with Serving(echo_app, pki.gateway) as s:
            assert isinstance(try_get(s.port, "/", insecure_client_context(pki.alice)), Exception)
            assert https_get(s.port, "/")[0] == 200            # no certificate is still served

    # -- revocation and addition: next connection, no restart ---------------

    def test_addition_and_revocation_reach_the_next_connection_without_a_restart(self, pki):
        with Serving(echo_app, pki.gateway) as s:
            ctx = lambda: insecure_client_context(pki.alice)    # noqa: E731
            assert isinstance(try_get(s.port, "/", ctx()), Exception)          # not pinned yet
            pki.pin_alice()
            assert https_get(s.port, "/", ctx())[0] == 200                      # added
            pki.unpin_alice()
            assert isinstance(try_get(s.port, "/", ctx()), Exception)          # revoked
            pki.pin_alice()
            assert https_get(s.port, "/", ctx())[0] == 200                      # and back

    def test_a_response_in_flight_completes_after_a_revocation(self, pki):
        """Revocation applies to NEW connections: a stream already started (here a
        response whose second half is held back) is finished. (Werkzeug closes the
        connection after every response, so "established" means "mid-response".)"""
        import http.client

        pki.pin_alice()
        release = threading.Event()

        def streaming_app(environ, start_response):
            start_response("200 OK", [("Content-Type", "text/plain")])

            def body():
                yield b"first-half|"
                release.wait(IO_TIMEOUT)
                yield b"second-half"
            return body()

        with Serving(streaming_app, pki.gateway) as s:
            conn = http.client.HTTPSConnection(
                LOOPBACK, s.port, timeout=IO_TIMEOUT, context=insecure_client_context(pki.alice)
            )
            try:
                conn.request("GET", "/stream")
                response = conn.getresponse()
                assert response.read(11) == b"first-half|"
                pki.unpin_alice()                                   # revoked mid-response
                assert isinstance(
                    try_get(s.port, "/", insecure_client_context(pki.alice)), Exception
                )                                                   # new connections: refused
                release.set()
                assert response.read() == b"second-half"            # the stream in flight: finished
            finally:
                release.set()
                conn.close()

    def test_each_connection_is_wrapped_with_the_context_current_at_that_moment(
        self, pki, mocker
    ):
        seen = []
        real = pki.gateway.current_context

        def spy():
            context = real()
            seen.append(context)
            return context

        mocker.patch.object(pki.gateway, "current_context", spy)
        with Serving(echo_app, pki.gateway) as s:
            seen.clear()                                      # (the constructor asks once)
            https_get(s.port, "/")
            pki.pin_alice()
            https_get(s.port, "/")
        assert len(seen) == 2 and seen[0] is not seen[1]

    # -- silent peers, shutdown, slots ---------------------------------------

    def test_silent_peers_do_not_block_other_clients(self, pki):
        silent = []
        try:
            with Serving(echo_app, pki.gateway) as s:
                for _ in range(3):
                    silent.append(raw_connect(s.port))        # connect and say nothing
                started = time.monotonic()
                assert https_get(s.port, "/")[0] == 200
                assert time.monotonic() - started < 5
        finally:
            for sock in silent:
                sock.close()

    def test_a_silent_peer_between_two_clients_blocks_neither(self, pki):
        with Serving(echo_app, pki.gateway) as s:
            assert https_get(s.port, "/")[0] == 200
            with raw_connect(s.port):
                assert https_get(s.port, "/")[0] == 200
            assert https_get(s.port, "/")[0] == 200

    def test_shutdown_returns_promptly_with_many_stalled_handshakes(self, pki):
        silent = []
        s = Serving(echo_app, pki.gateway)
        s.thread.start()
        try:
            for _ in range(20):
                silent.append(raw_connect(s.port))
            time.sleep(0.3)                                    # let the threads start
            started = time.monotonic()
            s.server.shutdown()
            elapsed = time.monotonic() - started
            assert elapsed < 3.0, f"shutdown took {elapsed:.1f}s with stalled handshakes"
            s.thread.join(5)
            assert not s.thread.is_alive()
        finally:
            for sock in silent:
                sock.close()
            s.server.server_close()

    def test_failed_handshakes_release_their_slots(self, pki):
        """More failed handshakes than there are slots, one after the other, and
        a real client is still served: every failure gave its slot back."""
        attempts = workers.API_MAX_CONNECTIONS + 40
        with Serving(echo_app, pki.gateway) as s:
            for _ in range(attempts):
                with raw_connect(s.port) as sock:
                    sock.sendall(b"not a tls hello at all\r\n\r\n")
                    sock.settimeout(IO_TIMEOUT)
                    try:
                        while sock.recv(4096):
                            pass
                    except OSError:
                        pass
            assert _eventually(lambda: isinstance(https_get(s.port, "/")[0], int), 10)
            assert https_get(s.port, "/")[0] == 200

    def test_rejected_certificates_release_their_slots_too(self, pki):
        pki.pin_alice()
        with Serving(echo_app, pki.gateway) as s:
            for _ in range(workers.API_MAX_CONNECTIONS + 10):
                assert isinstance(
                    try_get(s.port, "/", insecure_client_context(pki.mallory)), Exception
                )
            assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200

    def test_a_handshake_that_never_completes_is_dropped_after_its_timeout(self, pki):
        """The timeout is per connection thread: a short one frees the peer."""
        cls = workers._api_server_class()
        server = cls(LOOPBACK, 0, echo_app, pki.gateway, handshake_timeout=0.5)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with raw_connect(server.server_port) as sock:
                sock.settimeout(IO_TIMEOUT)
                started = time.monotonic()
                assert sock.recv(10) == b""                  # the server closed it
                assert 0.3 < time.monotonic() - started < 5
        finally:
            server.shutdown()
            thread.join(5)
            server.server_close()


# ===========================================================================
# TrustGateway
# ===========================================================================


@pytest.mark.unit
class TestTrustGateway:
    @pytest.fixture
    def kit(self, tmp_path):
        gateway, server, stub = make_gateway(tmp_path / "srv.tls")
        return gateway, server, stub, client_identity(tmp_path / "c.tls")

    def test_it_starts_empty_and_fails_closed(self, kit):
        gateway, server, stub, _ = kit
        assert gateway.view.key is None and dict(gateway.view.pins) == {}
        assert isinstance(gateway.current_context(), ssl.SSLContext)
        assert gateway.server_fingerprint == server.fingerprint
        assert stub.read_calls == 0                         # nothing is read at construction

    def test_a_pinned_certificate_appears_after_the_first_successful_read(self, kit):
        gateway, _, stub, client = kit
        stub.set_pins([trusted_pin(client, "host-a")])
        before = gateway.current_context()
        assert gateway.poll() is True
        assert gateway.current_context() is not before
        info = gateway.lookup(client.fingerprint)
        assert info.client_id == "host-a" and info.predecessor is None
        assert info.not_after == pytest.approx(client.not_after.timestamp())
        assert gateway.view.key == (stub.store_id, stub.generation)
        assert gateway.lookup("00" * 32) is None

    def test_poll_reads_the_pins_only_when_the_key_moved(self, kit):
        gateway, _, stub, client = kit
        stub.set_pins([trusted_pin(client)])
        gateway.poll()
        reads = stub.read_calls
        assert gateway.poll() is False and gateway.poll() is False
        assert stub.read_calls == reads                     # key polled, pins not re-read
        stub.set_pins([], generation=stub.generation + 1)
        assert gateway.poll() is True
        assert stub.read_calls == reads + 1 and gateway.lookup(client.fingerprint) is None

    def test_refresh_reads_unconditionally(self, kit):
        gateway, _, stub, client = kit
        stub.set_pins([trusted_pin(client)])
        gateway.refresh()
        reads = stub.read_calls
        gateway.refresh()
        assert stub.read_calls == reads + 1

    def test_the_view_is_immutable(self, kit):
        gateway, _, stub, client = kit
        stub.set_pins([trusted_pin(client)])
        gateway.refresh()
        view = gateway.view
        with pytest.raises(Exception):
            view.key = None
        with pytest.raises(TypeError):
            view.pins["x"] = None

    def test_a_failed_first_read_trusts_nobody_and_logs_once(self, kit, caplog):
        gateway, _, stub, client = kit
        stub.fail = OSError("database is locked")
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            assert gateway.poll() is False and gateway.refresh() is False
            assert gateway.poll() is False
        assert gateway.view.key is None and gateway.lookup(client.fingerprint) is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "nobody is trusted yet" in warnings[0].getMessage()
        assert "database is locked" in warnings[0].getMessage()

    def test_a_failed_read_keeps_the_last_view_and_logs_once_per_distinct_error(
        self, kit, caplog
    ):
        gateway, _, stub, client = kit
        stub.set_pins([trusted_pin(client, "host-a")])
        gateway.refresh()
        kept = gateway.view
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            stub.fail = OSError("database is locked")
            for _ in range(4):
                assert gateway.poll() is False
            assert gateway.view is kept                        # untouched
            stub.fail = RuntimeError("something else")
            gateway.poll()
            gateway.poll()
            stub.fail = None
            stub.generation += 1                               # and it works again
            assert gateway.poll() is True
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 2
        assert "database is locked" in warnings[0] and "keeping trust generation" in warnings[0]
        assert "something else" in warnings[1]
        assert any(r.levelno == logging.INFO and "can be read again" in r.getMessage()
                   for r in caplog.records)

    def test_poll_and_refresh_never_raise(self, kit):
        gateway, _, stub, _ = kit
        for error in (OSError("x"), RuntimeError("y"), ValueError("z")):
            stub.fail = error
            assert gateway.poll() is False
            assert gateway.refresh() is False

    def test_a_bad_pem_is_dropped_while_the_other_pins_still_work(self, kit, caplog):
        gateway, server, stub, good = kit
        bad = trusted_pin(good, "broken")
        bad = bad.__class__(
            fingerprint="ab" * 32, client_id="broken", cert_pem=_CORRUPT_PEM,
            not_after=bad.not_after, enrolled_at=1.0, enrolled_via="token", predecessor=None,
        )
        stub.set_pins([bad, trusted_pin(good, "host-a")])
        with caplog.at_level(logging.ERROR, logger="zfsbackup.api"):
            assert gateway.refresh() is True
            stub.generation += 1
            assert gateway.refresh() is True                   # re-read: still one report
        assert gateway.lookup("ab" * 32) is None
        assert gateway.lookup(good.fingerprint).client_id == "host-a"
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and "ab" * 32 in errors[0] and "broken" in errors[0]

    def test_a_bad_pem_does_not_lock_everyone_out_on_a_real_connection(self, kit):
        gateway, _, stub, good = kit
        bad = trusted_pin(good, "broken").__class__(
            fingerprint="cd" * 32, client_id="broken", cert_pem=_CORRUPT_PEM,
            not_after=good.not_after.timestamp(), enrolled_at=1.0, enrolled_via="token",
            predecessor=None,
        )
        stub.set_pins([bad, trusted_pin(good, "host-a")])
        gateway.refresh()
        with Serving(echo_app, gateway) as s:
            assert https_get(s.port, "/", insecure_client_context(good))[0] == 200

    def test_the_poll_decides_whether_to_read_but_the_view_takes_the_reads_own_key(self, kit):
        """A write between the key poll and the pins read is part of what is
        installed, under the key of the read."""
        gateway, _, stub, client = kit
        stub.set_pins([trusted_pin(client)], generation=5)
        real_read = stub.read_trust

        def racing_read(resolved):
            stub.generation = 6                                # a write lands in between
            return real_read(resolved)

        gateway._read_trust = racing_read
        assert gateway.poll() is True
        assert gateway.view.key == (stub.store_id, 6)


# ===========================================================================
# TrustGateway: key installation, failed rebuilds, concurrency
# ===========================================================================


class _Healing:
    """A `context_factory` that delegates to `identity.server_context` until told to
    fail with `error` (the construction-time empty context always works)."""

    def __init__(self):
        self.error = None
        self.calls = 0

    def __call__(self, ident, pems):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return identity.server_context(ident, pems)


@pytest.mark.unit
class TestTrustGatewayKeys:
    """Any key different from the installed one is installed (a replaced database
    file, a rolled-back database); the same key is not rebuilt."""

    @pytest.fixture
    def kit(self, tmp_path):
        gateway, server, stub = make_gateway(tmp_path / "srv.tls")
        return gateway, stub, client_identity(tmp_path / "a.tls"), client_identity(tmp_path / "b.tls")

    def test_newer_is_gone(self):
        assert not hasattr(api, "_newer")

    def test_a_higher_generation_in_the_same_store_is_installed(self, kit):
        gateway, stub, alice, _ = kit
        stub.set_pins([trusted_pin(alice)], generation=3)
        gateway.refresh()
        stub.set_pins([], generation=4)
        assert gateway.poll() is True
        assert gateway.view.key == (stub.store_id, 4) and gateway.lookup(alice.fingerprint) is None

    def test_the_same_key_is_not_rebuilt(self, kit):
        gateway, stub, alice, _ = kit
        stub.set_pins([trusted_pin(alice)], generation=3)
        gateway.refresh()
        context = gateway.current_context()
        assert gateway.refresh() is True
        assert gateway.current_context() is context

    def test_a_lower_generation_in_the_same_store_is_a_rollback_and_is_installed_with_a_warning(
        self, kit, caplog
    ):
        gateway, stub, alice, bob = kit
        stub.set_pins([trusted_pin(alice)], generation=5)
        gateway.refresh()
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            stub.set_pins([trusted_pin(bob, "bob")], generation=3)       # restored from a backup
            assert gateway.poll() is True
        assert gateway.view.key == (stub.store_id, 3)
        assert gateway.lookup(alice.fingerprint) is None                  # the older truth: no alice
        assert gateway.lookup(bob.fingerprint).client_id == "bob"
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and "went back from 5 to 3" in warnings[0]

    def test_a_rolled_back_revocation_is_not_hidden_until_the_generation_catches_up(self, kit):
        gateway, stub, alice, _ = kit
        stub.set_pins([trusted_pin(alice)], generation=9)
        gateway.refresh()
        stub.set_pins([], generation=2)                                    # rolled back, alice revoked
        gateway.poll()
        assert gateway.lookup(alice.fingerprint) is None

    def test_a_different_store_id_with_a_lower_generation_is_installed_without_the_warning(
        self, kit, caplog
    ):
        """A replaced database file: its own trust is the truth, whatever its counter says."""
        gateway, stub, alice, bob = kit
        stub.set_pins([trusted_pin(alice)], generation=50)
        gateway.refresh()
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            stub.set_pins([trusted_pin(bob, "bob")], generation=1, store_id=stub.store_id + 1)
            assert gateway.poll() is True
        assert gateway.view.key == (stub.store_id, 1)
        assert gateway.lookup(alice.fingerprint) is None and gateway.lookup(bob.fingerprint)
        assert not [r for r in caplog.records if "went back" in r.getMessage()]

    def test_a_different_store_id_with_a_higher_generation_is_installed(self, kit):
        gateway, stub, alice, _ = kit
        stub.set_pins([trusted_pin(alice)], generation=2)
        gateway.refresh()
        stub.set_pins([], generation=7, store_id=stub.store_id + 1)
        assert gateway.poll() is True and gateway.view.key == (stub.store_id, 7)

    def test_the_first_read_is_installed_whatever_its_key(self, kit):
        gateway, stub, alice, _ = kit
        stub.set_pins([trusted_pin(alice)], generation=0, store_id=0)
        assert gateway.poll() is True and gateway.view.key == (0, 0)


def _swap_server_identity(directory, scratch):
    """Replace the server certificate AND key in `directory` with a freshly generated
    identity's (what an operator re-keying the server does); -> the new `Identity`."""
    other = server_identity(scratch)
    os.replace(other.cert_path, Path(directory) / "server.crt")
    os.replace(other.key_path, Path(directory) / "server.key")
    return other


def _swap_server_key(directory, scratch):
    """Replace only the server KEY in `directory` (the certificate stays)."""
    other = server_identity(scratch)
    os.replace(other.key_path, Path(directory) / "server.key")


@contextlib.contextmanager
def _identity_lock_held(directory):
    """Hold `<directory>/identity.lock` exclusively, as the CLI's `enroll` / `identity
    rotate` do. flock conflicts between open file descriptions, so this blocks the
    gateway's shared read in the same process."""
    fd = os.open(str(Path(directory) / identity.IDENTITY_LOCK), os.O_RDWR | os.O_CREAT, 0o640)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


@pytest.mark.unit
class TestTrustGatewayFailedRebuild:
    """A rebuild that fails keeps the old context but installs the new key with
    the old pins MINUS every pin the read no longer has (revocations apply to
    route authentication); the view is degraded, so `poll()` keeps retrying;
    an `IdentityError` asks the worker to restart."""

    @pytest.fixture
    def kit(self, tmp_path):
        healing = _Healing()
        gateway, server, stub = make_gateway(tmp_path / "srv.tls", context_factory=healing)
        alice = client_identity(tmp_path / "a.tls")
        bob = client_identity(tmp_path / "b.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        assert gateway.refresh() is True
        return gateway, stub, healing, alice, bob

    def test_a_failed_rebuild_keeps_the_old_context_and_installs_the_new_key(self, kit, caplog):
        gateway, stub, healing, alice, bob = kit
        old_context = gateway.current_context()
        healing.error = ValueError("ssl rejects the set")
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=2)
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            assert gateway.refresh() is False
        assert gateway.current_context() is old_context
        assert gateway.view.key == (stub.store_id, 2)                      # the new key is in
        assert gateway.lookup(alice.fingerprint) is not None
        assert gateway.lookup(bob.fingerprint) is None                     # new pins wait
        assert gateway._degraded is True and gateway.restart_reason is None

    def test_a_revocation_still_applies_to_route_authentication(self, kit, caplog):
        gateway, stub, healing, alice, _ = kit
        old_context = gateway.current_context()
        healing.error = ValueError("ssl rejects the set")
        stub.set_pins([], generation=2)                                    # alice revoked
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            assert gateway.refresh() is False
        assert gateway.lookup(alice.fingerprint) is None                   # refused by the app
        assert gateway.current_context() is old_context                    # TLS layer still lenient
        assert any("revoked certificate(s) refused by the API" in r.getMessage()
                   for r in caplog.records)

    def test_poll_retries_a_degraded_view_even_with_an_unchanged_key(self, kit):
        gateway, stub, healing, alice, bob = kit
        healing.error = ValueError("ssl rejects the set")
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=2)
        gateway.refresh()
        reads, calls = stub.read_calls, healing.calls
        assert gateway.poll() is False                                      # still failing
        assert stub.read_calls == reads + 1 and healing.calls > calls       # but it did retry
        healing.error = None                                                # healed
        assert gateway.poll() is True
        assert gateway._degraded is False
        assert gateway.lookup(bob.fingerprint).client_id == "bob"           # the waiting pin arrives
        assert gateway.current_context() is not None
        reads = stub.read_calls
        assert gateway.poll() is False and stub.read_calls == reads         # healthy again: key-only

    def test_a_degraded_failure_is_logged_once(self, kit, caplog):
        gateway, stub, healing, alice, bob = kit
        healing.error = ValueError("ssl rejects the set")
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=2)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            for _ in range(5):
                gateway.poll()
        failures = [r for r in caplog.records if "cannot apply the trusted client" in r.getMessage()]
        assert len(failures) == 1

    def test_a_replaced_server_identity_asks_for_a_restart_and_keeps_serving(
        self, kit, caplog, tmp_path
    ):
        gateway, stub, healing, alice, bob = kit
        old_context = gateway.current_context()
        replacement = _swap_server_identity(gateway._identity.cert_path.parent, tmp_path / "new.tls")
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=2)
        assert gateway.restart_reason is None
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            assert gateway.refresh() is False
        reason = gateway.restart_reason
        assert reason is not None and "no longer matches" in reason
        assert gateway._identity.fingerprint in reason and replacement.fingerprint in reason
        assert gateway.current_context() is old_context
        assert gateway.lookup(alice.fingerprint) is not None
        assert gateway.lookup(bob.fingerprint) is None                       # new pins wait
        assert "revocations still apply" in caplog.text
        assert "retrying every" not in caplog.text                           # a restart will cure it

    def test_the_restart_reason_is_set_once(self, kit, tmp_path):
        gateway, stub, healing, alice, bob = kit
        directory = gateway._identity.cert_path.parent
        first_id = _swap_server_identity(directory, tmp_path / "new1.tls")
        stub.set_pins([], generation=2)
        gateway.refresh()
        first = gateway.restart_reason
        second_id = _swap_server_identity(directory, tmp_path / "new2.tls")
        stub.set_pins([], generation=3)
        gateway.refresh()
        assert gateway.restart_reason == first
        assert first_id.fingerprint in first and second_id.fingerprint not in first

    def test_a_replaced_server_identity_still_applies_revocations(self, kit, tmp_path):
        gateway, stub, healing, alice, _ = kit
        _swap_server_identity(gateway._identity.cert_path.parent, tmp_path / "new.tls")
        stub.set_pins([], generation=2)
        gateway.refresh()
        assert gateway.lookup(alice.fingerprint) is None and gateway.restart_reason

    def test_a_degraded_view_takes_the_kept_pins_details_from_the_new_snapshot(self, kit):
        """A pin in both reads keeps its place but reports the NEW read's binding, expiry and
        predecessor (a client re-bound or rotated while the context could not be rebuilt)."""
        gateway, stub, healing, alice, bob = kit
        before = gateway.lookup(alice.fingerprint)
        assert before == api.PinInfo("alice", alice.not_after.timestamp(), None)
        healing.error = ValueError("ssl rejects the set")
        moved = trusted_pin(alice, "alice-renamed", predecessor="cd" * 32)
        moved = dataclasses.replace(moved, not_after=before.not_after + 1000.0)
        stub.set_pins([moved], generation=2)
        assert gateway.refresh() is False
        assert gateway.lookup(alice.fingerprint) == api.PinInfo(
            "alice-renamed", before.not_after + 1000.0, "cd" * 32
        )

    def test_a_plain_failure_does_not_ask_for_a_restart(self, kit):
        gateway, stub, healing, alice, bob = kit
        healing.error = RuntimeError("transient")
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=2)
        gateway.refresh()
        assert gateway.restart_reason is None


@pytest.mark.unit
class TestTrustGatewayIdentityChange:
    """Only `identity.IdentityChanged`, confirmed against the certificate on disk, asks
    for a restart. A lock timeout, EMFILE, an unreadable file or a key-only change
    degrade the view instead (new pins wait, revocations apply), are logged once with
    "retrying every 2 s", and heal on a later poll. Real identity files, the gateway's
    default context factory (`server_context(lock_timeout=GATEWAY_LOCK_TIMEOUT)`)."""

    @pytest.fixture
    def kit(self, tmp_path):
        gateway, server, stub = make_gateway(tmp_path / "srv.tls")
        alice = client_identity(tmp_path / "a.tls")
        bob = client_identity(tmp_path / "b.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        assert gateway.refresh() is True
        return gateway, stub, alice, bob

    @staticmethod
    def _add_bob(stub, alice, bob, generation=2):
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=generation)

    @staticmethod
    def _retry_lines(caplog):
        return [r.getMessage() for r in caplog.records
                if r.levelno == logging.WARNING and "retrying every 2 s" in r.getMessage()]

    # ---- the rebuilds use the bounded lock wait

    def test_rebuilds_wait_at_most_the_gateway_lock_timeout_but_the_first_context_does_not(
        self, tmp_path, mocker
    ):
        assert api.GATEWAY_LOCK_TIMEOUT == 5.0 and api.REQUEST_REFRESH_WAIT == 2.0
        calls = []
        real = identity.server_context

        def spy(ident, pems, **kwargs):
            calls.append(kwargs)
            return real(ident, pems, **kwargs)

        mocker.patch("zfsbackup.identity.server_context", spy)
        gateway, _, stub = make_gateway(tmp_path / "srv.tls")
        alice = client_identity(tmp_path / "a.tls")
        stub.set_pins([trusted_pin(alice)], generation=1)
        assert gateway.refresh() is True
        assert calls[0] == {}                                            # the initial context
        assert calls[1] == {"lock_timeout": api.GATEWAY_LOCK_TIMEOUT}    # a rebuild

    # ---- IdentityChanged: restart, but only once the disk confirms it

    def test_a_real_replacement_of_the_server_files_restarts_and_names_both_fingerprints(
        self, kit, tmp_path, mocker
    ):
        gateway, stub, alice, bob = kit
        peek = mocker.spy(identity, "peek_identity")
        replacement = _swap_server_identity(gateway._identity.cert_path.parent, tmp_path / "n.tls")
        self._add_bob(stub, alice, bob)
        assert gateway.refresh() is False
        assert gateway.restart_reason is not None
        assert replacement.fingerprint in gateway.restart_reason
        peek.assert_called_once()                       # confirmed against the certificate on disk
        assert gateway._degraded is True and gateway.lookup(bob.fingerprint) is None

    def test_identity_changed_raised_while_the_disk_still_holds_the_loaded_certificate_is_no_restart(
        self, tmp_path, caplog
    ):
        """The files may be mid-write by a process that does not take the lock: the
        restart waits for `peek_identity` to see another certificate."""
        healing = _Healing()
        gateway, server, stub = make_gateway(tmp_path / "srv.tls", context_factory=healing)
        alice, bob = client_identity(tmp_path / "a.tls"), client_identity(tmp_path / "b.tls")
        stub.set_pins([trusted_pin(alice)], generation=1)
        gateway.refresh()
        healing.error = identity.IdentityChanged(
            "certificate changed", server.cert_path, expected=server.fingerprint, actual="ab" * 32
        )
        self._add_bob(stub, alice, bob)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            assert gateway.refresh() is False
            gateway.poll()
        assert gateway.restart_reason is None and gateway._degraded is True
        assert len(self._retry_lines(caplog)) == 1

    def test_a_missing_server_certificate_is_no_restart(self, kit, caplog):
        gateway, stub, alice, bob = kit
        gateway._identity.cert_path.unlink()
        self._add_bob(stub, alice, bob)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            assert gateway.refresh() is False
        assert gateway.restart_reason is None and gateway._degraded is True
        assert len(self._retry_lines(caplog)) == 1

    def test_a_key_only_change_degrades_without_a_restart_and_logs_once(
        self, kit, tmp_path, caplog, mocker
    ):
        gateway, stub, alice, bob = kit
        old_context = gateway.current_context()
        _swap_server_key(gateway._identity.cert_path.parent, tmp_path / "n.tls")
        self._add_bob(stub, alice, bob)
        caught = []
        real = identity.server_context

        def record(*a, **k):
            try:
                return real(*a, **k)
            except Exception as exc:
                caught.append(exc)
                raise

        mocker.patch.object(gateway, "_context_factory", record)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            for _ in range(4):
                assert gateway.poll() is False
        assert isinstance(caught[0], identity.IdentityChanged) and caught[0].actual is None
        assert gateway.restart_reason is None and gateway._degraded is True
        assert gateway.current_context() is old_context and gateway.lookup(bob.fingerprint) is None
        assert len(self._retry_lines(caplog)) == 1
        assert "no longer matches" in self._retry_lines(caplog)[0]        # the key message

    def test_a_key_only_change_heals_when_the_key_is_put_back(self, kit, tmp_path):
        gateway, stub, alice, bob = kit
        key = gateway._identity.key_path
        original = key.read_bytes()
        _swap_server_key(key.parent, tmp_path / "n.tls")
        self._add_bob(stub, alice, bob)
        assert gateway.poll() is False
        key.write_bytes(original)
        assert gateway.poll() is True and gateway._degraded is False
        assert gateway.lookup(bob.fingerprint).client_id == "bob"

    # ---- plain failures: the lock, EMFILE, an unreadable file

    def test_a_held_identity_lock_degrades_without_a_restart_and_heals_on_the_next_poll(
        self, tmp_path, monkeypatch, caplog
    ):
        monkeypatch.setattr(api, "GATEWAY_LOCK_TIMEOUT", 0.1)
        gateway, _, stub = make_gateway(tmp_path / "srv.tls")
        alice, bob = client_identity(tmp_path / "a.tls"), client_identity(tmp_path / "b.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        assert gateway.refresh() is True
        self._add_bob(stub, alice, bob)
        old_context = gateway.current_context()
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            with _identity_lock_held(gateway._identity.cert_path.parent):
                started = time.monotonic()
                for _ in range(4):
                    assert gateway.poll() is False
                assert time.monotonic() - started < 5
            assert gateway.restart_reason is None and gateway._degraded is True
            assert gateway.current_context() is old_context
            assert gateway.lookup(bob.fingerprint) is None and gateway.lookup(alice.fingerprint)
            assert len(self._retry_lines(caplog)) == 1                    # logged once
            assert "timed out" in self._retry_lines(caplog)[0]
            assert gateway.poll() is True                                 # the lock is free
        assert gateway._degraded is False and gateway.restart_reason is None
        assert gateway.lookup(bob.fingerprint).client_id == "bob"
        assert gateway.current_context() is not old_context
        assert any("can be read again" in r.getMessage() for r in caplog.records)

    def test_a_revocation_applies_while_the_identity_lock_is_held(self, tmp_path, monkeypatch):
        monkeypatch.setattr(api, "GATEWAY_LOCK_TIMEOUT", 0.1)
        gateway, _, stub = make_gateway(tmp_path / "srv.tls")
        alice = client_identity(tmp_path / "a.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        gateway.refresh()
        stub.set_pins([], generation=2)
        with _identity_lock_held(gateway._identity.cert_path.parent):
            assert gateway.poll() is False
        assert gateway.lookup(alice.fingerprint) is None and gateway.restart_reason is None

    def test_emfile_degrades_without_a_restart_and_logs_once(self, kit, mocker, caplog):
        gateway, stub, alice, bob = kit
        self._add_bob(stub, alice, bob)
        real_open = os.open
        cert = str(gateway._identity.cert_path)

        def failing_open(path, *a, **k):
            if str(path) == cert:
                raise OSError(errno.EMFILE, os.strerror(errno.EMFILE))
            return real_open(path, *a, **k)

        mocker.patch("zfsbackup.identity.os.open", failing_open)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            for _ in range(3):
                assert gateway.poll() is False
        assert gateway.restart_reason is None and gateway._degraded is True
        lines = self._retry_lines(caplog)
        assert len(lines) == 1 and "Too many open files" in lines[0]
        mocker.stopall()
        assert gateway.poll() is True and gateway.lookup(bob.fingerprint) is not None

    def test_an_unreadable_server_certificate_degrades_without_a_restart(self, kit, caplog):
        gateway, stub, alice, bob = kit
        self._add_bob(stub, alice, bob)
        cert = gateway._identity.cert_path
        cert.write_text("not a certificate\n")
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            assert gateway.refresh() is False
        assert gateway.restart_reason is None and gateway._degraded is True
        assert len(self._retry_lines(caplog)) == 1

    def test_each_distinct_message_is_logged_once(self, kit, tmp_path, caplog):
        gateway, stub, alice, bob = kit
        directory = gateway._identity.cert_path.parent
        key = gateway._identity.key_path
        original = key.read_bytes()
        self._add_bob(stub, alice, bob)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            _swap_server_key(directory, tmp_path / "n.tls")
            gateway.poll(); gateway.poll()
            key.write_bytes(original)
            gateway._identity.cert_path.write_text("junk\n")
            gateway.poll(); gateway.poll()
        assert len(self._retry_lines(caplog)) == 2

    # ---- an identity that is not a generated layout: exc.actual is the evidence

    @pytest.fixture
    def custom(self, tmp_path):
        """A gateway over an operator-supplied pair (`my.crt` / `my.key`: not the
        generated layout, so `peek_identity` cannot be used)."""
        src = server_identity(tmp_path / "src.tls")
        d = tmp_path / "custom"
        d.mkdir()
        cert, key = d / "my.crt", d / "my.key"
        shutil.copy2(src.cert_path, cert)
        shutil.copy2(src.key_path, key)
        ident = identity.load_identity(cert, key, identity.ROLE_SERVER)
        stub = StubTrust([])
        gateway = api.TrustGateway(
            ident, object(), read_trust_key=stub.read_trust_key, read_trust=stub.read_trust
        )
        alice = client_identity(tmp_path / "a.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        assert gateway.refresh() is True
        return gateway, stub, alice, ident, cert, key

    def test_a_non_layout_identity_restarts_on_the_fingerprint_the_load_found(
        self, custom, tmp_path, mocker
    ):
        gateway, stub, alice, ident, cert, key = custom
        peek = mocker.patch("zfsbackup.identity.peek_identity",
                            side_effect=AssertionError("must not be used"))
        replacement = server_identity(tmp_path / "n.tls")
        shutil.copy2(replacement.cert_path, cert)
        shutil.copy2(replacement.key_path, key)
        stub.set_pins([], generation=2)
        assert gateway.refresh() is False
        assert gateway.restart_reason is not None
        assert replacement.fingerprint in gateway.restart_reason
        peek.assert_not_called()
        assert gateway.lookup(alice.fingerprint) is None                  # revocation applied

    def test_a_non_layout_key_only_change_is_no_restart(self, custom, tmp_path):
        gateway, stub, alice, ident, cert, key = custom
        shutil.copy2(server_identity(tmp_path / "n.tls").key_path, key)
        stub.set_pins([], generation=2)
        assert gateway.refresh() is False
        assert gateway.restart_reason is None and gateway._degraded is True


@pytest.mark.unit
class TestTrustGatewayConcurrency:
    @pytest.fixture
    def kit(self, tmp_path):
        gateway, server, stub = make_gateway(tmp_path / "srv.tls")
        return gateway, stub, client_identity(tmp_path / "a.tls")

    def test_poll_never_waits_for_a_running_refresh(self, kit):
        gateway, stub, alice = kit
        entered, release = threading.Event(), threading.Event()
        real_read = stub.read_trust

        def slow_read(resolved):
            entered.set()
            assert release.wait(IO_TIMEOUT)
            return real_read(resolved)

        gateway._read_trust = slow_read
        refresher = threading.Thread(target=gateway.refresh, daemon=True)
        refresher.start()
        try:
            assert entered.wait(IO_TIMEOUT)
            keys = stub.key_calls
            started = time.monotonic()
            assert gateway.poll() is False                              # skipped, at once
            assert time.monotonic() - started < 1.0
            assert stub.key_calls == keys                               # it did not even read the key
        finally:
            release.set()
            refresher.join(IO_TIMEOUT)
        assert not refresher.is_alive() and gateway.view.key is not None

    def test_concurrent_refresh_and_poll_never_install_an_older_view(self, kit):
        gateway, stub, alice = kit
        stop = threading.Event()
        errors = []
        pin = trusted_pin(alice)

        def writer():
            for generation in range(2, 80):
                stub.set_pins([pin] if generation % 2 else [], generation=generation)
                time.sleep(0.002)
            stop.set()

        def reader(use_refresh):
            try:
                while not stop.is_set():
                    (gateway.refresh if use_refresh else gateway.poll)()
            except BaseException as exc:            # noqa: BLE001
                errors.append(exc)

        sampled = []

        def sampler():
            while not stop.is_set():
                key = gateway.view.key
                if key is not None:
                    sampled.append(key[1])

        stub.set_pins([], generation=1)
        threads = [threading.Thread(target=writer)] + [
            threading.Thread(target=reader, args=(i % 2 == 0,)) for i in range(4)
        ] + [threading.Thread(target=sampler)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(IO_TIMEOUT * 3)
        assert not errors and not any(t.is_alive() for t in threads)
        assert sampled == sorted(sampled)                                # never went backwards
        gateway.refresh()
        assert gateway.view.key == (stub.store_id, stub.generation)
        assert (gateway.lookup(alice.fingerprint) is not None) == bool(stub.generation % 2)


@pytest.mark.unit
class TestTrustGatewayBoundedRefresh:
    """`refresh(timeout=)`: request threads never wait longer than their budget for
    another refresh (`_lock`) or another store call (`_STORE_LOCK`) -- one deadline for
    both -- and a skipped refresh is a DEBUG line, never a WARNING."""

    @pytest.fixture
    def kit(self, tmp_path):
        gateway, server, stub = make_gateway(tmp_path / "srv.tls")
        alice = client_identity(tmp_path / "a.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        return gateway, stub, alice

    @staticmethod
    def _release_later(lock, delay):
        t = threading.Timer(delay, lock.release)
        t.start()
        return t

    def test_a_free_gateway_refreshes_within_the_budget(self, kit):
        gateway, stub, alice = kit
        assert gateway.refresh(timeout=api.REQUEST_REFRESH_WAIT) is True
        assert gateway.lookup(alice.fingerprint).client_id == "alice" and stub.read_calls == 1

    def test_a_running_refresh_is_waited_for_up_to_the_budget_then_skipped(self, kit, caplog):
        gateway, stub, alice = kit
        gateway._lock.acquire()
        try:
            with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
                started = time.monotonic()
                assert gateway.refresh(timeout=0.4) is False
                elapsed = time.monotonic() - started
        finally:
            gateway._lock.release()
        assert 0.35 <= elapsed < 2.0
        assert stub.read_calls == 0 and gateway.view.key is None
        assert not [r for r in caplog.records if r.levelno > logging.DEBUG]
        assert any("skipped" in r.getMessage() for r in caplog.records)

    def test_a_zero_budget_does_not_wait_at_all(self, kit):
        gateway, stub, _ = kit
        gateway._lock.acquire()
        try:
            started = time.monotonic()
            assert gateway.refresh(timeout=0) is False
            assert time.monotonic() - started < 0.3
        finally:
            gateway._lock.release()
        assert stub.read_calls == 0
        assert gateway.refresh(timeout=0) is True                          # free: it runs

    def test_a_refresh_that_ends_within_the_budget_is_followed_by_ours(self, kit):
        gateway, stub, alice = kit
        gateway._lock.acquire()
        timer = self._release_later(gateway._lock, 0.2)
        try:
            assert gateway.refresh(timeout=3.0) is True
        finally:
            timer.join()
        assert gateway.lookup(alice.fingerprint) is not None

    def test_a_busy_store_lock_skips_the_refresh_quietly(self, kit, caplog):
        gateway, stub, alice = kit
        api._STORE_LOCK.acquire()
        try:
            with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
                started = time.monotonic()
                assert gateway.refresh(timeout=0.4) is False
                elapsed = time.monotonic() - started
        finally:
            api._STORE_LOCK.release()
        assert 0.35 <= elapsed < 2.0 and stub.read_calls == 0
        assert not [r for r in caplog.records if r.levelno > logging.DEBUG]
        assert not gateway._lock.locked()                                  # nothing left held
        assert gateway.refresh(timeout=0.4) is True                        # and it recovers

    def test_both_waits_share_one_deadline(self, kit):
        """`_lock` is granted after ~0.4 s of a 0.6 s budget: the store-lock wait gets the
        remaining ~0.2 s, not a fresh 0.6 s."""
        gateway, stub, _ = kit
        gateway._lock.acquire()
        api._STORE_LOCK.acquire()
        timer = self._release_later(gateway._lock, 0.4)
        try:
            started = time.monotonic()
            assert gateway.refresh(timeout=0.6) is False
            elapsed = time.monotonic() - started
        finally:
            timer.join()
            api._STORE_LOCK.release()
        assert 0.55 <= elapsed < 0.95, elapsed
        assert stub.read_calls == 0 and not gateway._lock.locked()

    def test_the_unbounded_form_still_waits_as_long_as_it_takes(self, kit):
        gateway, stub, alice = kit
        gateway._lock.acquire()
        timer = self._release_later(gateway._lock, 0.5)
        try:
            assert gateway.refresh() is True
        finally:
            timer.join()
        assert gateway.lookup(alice.fingerprint) is not None

    def test_a_failed_read_under_a_budget_is_still_reported(self, kit, caplog):
        gateway, stub, _ = kit
        stub.fail = OSError("database is locked")
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            assert gateway.refresh(timeout=1.0) is False
        assert "database is locked" in caplog.text                         # a real failure, not a skip


@pytest.mark.unit
class TestTrustGatewayRefreshDeadline:
    """`refresh(timeout=)` hands its deadline to the context rebuild: with the default factory
    the identity-lock wait is `min(GATEWAY_LOCK_TIMEOUT, time left)`, one budget for the whole
    build (the `ValueError` fallback's second call gets only what is left). Worker start and
    `poll()` keep `GATEWAY_LOCK_TIMEOUT`; a custom factory is called as `(identity, pems)`."""

    @pytest.fixture
    def kit(self, tmp_path):
        gateway, server, stub = make_gateway(tmp_path / "srv.tls")
        alice, bob = client_identity(tmp_path / "a.tls"), client_identity(tmp_path / "b.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        return gateway, stub, alice, bob

    @staticmethod
    def _record(gateway):
        """Wrap the gateway's `server_context`; -> the list of `lock_timeout`s it was given."""
        seen = []
        real = gateway._server_context

        def wrapper(ident, pems, **kwargs):
            seen.append(kwargs.get("lock_timeout"))
            return real(ident, pems, **kwargs)

        gateway._server_context = wrapper
        return seen

    def test_a_bounded_refresh_returns_within_its_budget_while_the_identity_lock_is_held(self, kit):
        gateway, stub, alice, bob = kit
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=2)
        with _identity_lock_held(gateway._identity.cert_path.parent):
            started = time.monotonic()
            assert gateway.refresh(timeout=0.5) is False
            elapsed = time.monotonic() - started
        assert 0.4 <= elapsed < api.GATEWAY_LOCK_TIMEOUT - 1.5, elapsed     # not the 5 s of a poll
        assert gateway.restart_reason is None and gateway._degraded is True
        assert gateway.lookup(bob.fingerprint) is None

    def test_a_zero_budget_tries_the_identity_lock_once(self, kit):
        gateway, stub, alice, bob = kit
        seen = self._record(gateway)
        with _identity_lock_held(gateway._identity.cert_path.parent):
            started = time.monotonic()
            assert gateway.refresh(timeout=0) is False
            assert time.monotonic() - started < 1.0
        assert seen == [0.0]
        assert gateway.refresh(timeout=0) is True                            # lock free: one try is enough

    def test_the_rebuild_gets_the_smaller_of_the_budget_and_the_gateway_timeout(self, kit):
        gateway, stub, alice, bob = kit
        seen = self._record(gateway)
        assert gateway.refresh(timeout=1.5) is True
        assert 0 < seen[0] <= 1.5
        stub.set_pins([], generation=2)
        assert gateway.refresh(timeout=60.0) is True                         # a big budget: capped
        assert seen[1] == api.GATEWAY_LOCK_TIMEOUT

    def test_poll_and_the_unbounded_refresh_keep_the_gateway_lock_timeout(self, kit):
        gateway, stub, alice, bob = kit
        seen = self._record(gateway)
        assert gateway.refresh() is True
        stub.set_pins([], generation=2)
        assert gateway.poll() is True
        assert seen == [api.GATEWAY_LOCK_TIMEOUT, api.GATEWAY_LOCK_TIMEOUT]

    def test_the_value_error_fallback_only_gets_the_remaining_budget(self, kit):
        gateway, stub, alice, bob = kit
        timeouts = []
        real = gateway._server_context

        def slow_first_then_ok(ident, pems, **kwargs):
            timeouts.append(kwargs["lock_timeout"])
            if len(timeouts) == 1:
                time.sleep(0.4)                       # the first attempt spent part of the budget
                raise ValueError("one pin is unusable")
            return real(ident, pems, **kwargs)

        gateway._server_context = slow_first_then_ok
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=2)
        assert gateway.refresh(timeout=1.5) is True
        first, second = timeouts
        assert first <= 1.5 and second <= first - 0.35, timeouts
        assert second >= 0.0

    def test_the_fallback_after_an_exhausted_budget_makes_one_try(self, kit):
        gateway, stub, alice, bob = kit
        timeouts = []
        real = gateway._server_context

        def spend_it_all(ident, pems, **kwargs):
            timeouts.append(kwargs["lock_timeout"])
            if len(timeouts) == 1:
                time.sleep(0.3)
                raise ValueError("one pin is unusable")
            return real(ident, pems, **kwargs)

        gateway._server_context = spend_it_all
        stub.set_pins([trusted_pin(alice, "alice")], generation=2)
        assert gateway.refresh(timeout=0.1) is True
        assert timeouts[1] == 0.0

    def test_a_custom_factory_is_still_called_with_identity_and_pems_only(self, tmp_path):
        calls = []

        def factory(ident, pems):
            calls.append((ident, tuple(pems)))
            return identity.server_context(ident, pems)

        gateway, server, stub = make_gateway(tmp_path / "srv.tls", context_factory=factory)
        alice = client_identity(tmp_path / "a.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        assert gateway.refresh(timeout=1.0) is True
        assert calls[-1] == (server, (alice.cert_pem,))


@pytest.mark.unit
class TestTrustGatewayDirtyFlag:
    """`_dirty`: a bounded refresh that failed or did not run, or a failed read, makes `poll()`
    re-read even when the polled key equals the installed one (the installed view may predate
    a commit); a read clears it, and a mark set while that read runs is kept."""

    @pytest.fixture
    def kit(self, tmp_path):
        gateway, server, stub = make_gateway(tmp_path / "srv.tls")
        alice = client_identity(tmp_path / "a.tls")
        stub.set_pins([trusted_pin(alice, "alice")], generation=1)
        assert gateway.refresh() is True
        return gateway, stub, alice

    def test_a_healthy_gateway_is_clean_and_poll_reads_nothing(self, kit):
        gateway, stub, _ = kit
        assert gateway._dirty is False
        reads = stub.read_calls
        assert gateway.poll() is False and stub.read_calls == reads

    def test_a_bounded_refresh_that_did_not_run_marks_it_and_poll_rereads_the_same_key(self, kit):
        gateway, stub, alice = kit
        gateway._lock.acquire()
        try:
            assert gateway.refresh(timeout=0.05) is False
        finally:
            gateway._lock.release()
        assert gateway._dirty is True
        reads = stub.read_calls
        assert gateway.poll() is True                                   # same key, read anyway
        assert stub.read_calls == reads + 1 and gateway._dirty is False
        assert gateway.poll() is False and stub.read_calls == reads + 1  # clean again

    def test_a_busy_store_lock_marks_it(self, kit):
        gateway, stub, _ = kit
        api._STORE_LOCK.acquire()
        try:
            assert gateway.refresh(timeout=0.05) is False
        finally:
            api._STORE_LOCK.release()
        assert gateway._dirty is True
        reads = stub.read_calls
        assert gateway.poll() is True and stub.read_calls == reads + 1

    def test_a_failed_read_marks_it_and_the_next_poll_rereads(self, kit):
        gateway, stub, _ = kit
        stub.fail = OSError("database is locked")
        assert gateway.refresh() is False and gateway._dirty is True
        stub.fail = None                                                # healed, same key
        reads = stub.read_calls
        assert gateway.poll() is True
        assert stub.read_calls == reads + 1 and gateway._dirty is False

    def test_a_successful_bounded_refresh_stays_clean(self, kit):
        gateway, stub, _ = kit
        assert gateway.refresh(timeout=1.0) is True and gateway._dirty is False
        reads = stub.read_calls
        assert gateway.poll() is False and stub.read_calls == reads

    def test_a_failed_bounded_refresh_marks_it_though_the_read_worked(self, kit, tmp_path):
        """The view could not be rebuilt (degraded): the refresh failed, so the flag is set."""
        gateway, stub, alice = kit
        bob = client_identity(tmp_path / "b.tls")
        stub.set_pins([trusted_pin(alice, "alice"), trusted_pin(bob, "bob")], generation=2)
        with _identity_lock_held(gateway._identity.cert_path.parent):
            assert gateway.refresh(timeout=0.2) is False
        assert gateway._dirty is True

    def test_the_flag_is_cleared_before_the_read_so_a_mark_set_meanwhile_survives(self, kit):
        gateway, stub, _ = kit
        gateway._dirty = True
        real = gateway._read_trust

        seen = []

        def racing_read(resolved):
            seen.append(gateway._dirty)                 # cleared when the read started
            gateway._dirty = True                       # another thread's failed refresh
            return real(resolved)

        gateway._read_trust = racing_read
        gateway.refresh()
        assert seen == [False]
        assert gateway._dirty is True


@pytest.mark.unit
class TestStoreCallLock:
    """`api._store_call`: every `store.trust` call of the API worker is one at a
    time (closing any descriptor of a file drops every POSIX lock the process
    holds on it, including another thread's live SQLite lock)."""

    def test_calls_do_not_overlap(self):
        active, peak = [0], [0]
        guard = threading.Lock()

        def fn():
            with guard:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.02)
            with guard:
                active[0] -= 1
            return 1

        threads = [threading.Thread(target=api._store_call, args=(fn,)) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(IO_TIMEOUT)
        assert peak[0] == 1

    def test_it_returns_the_value_and_releases_on_an_exception(self):
        assert api._store_call(lambda a, b=0: a + b, 1, b=2) == 3
        with pytest.raises(KeyError):
            api._store_call(lambda: {}["x"])
        assert api._STORE_LOCK.acquire(blocking=False)               # released
        api._STORE_LOCK.release()

    def test_a_bounded_wait_gives_up_with_config_busy(self):
        from zfsbackup.config.store.errors import ConfigBusy

        api._STORE_LOCK.acquire()
        try:
            started = time.monotonic()
            with pytest.raises(ConfigBusy, match="nothing was written"):
                api._store_call(lambda: 1, wait=0.2)
            assert 0.15 < time.monotonic() - started < 3
        finally:
            api._STORE_LOCK.release()

    def test_an_unbounded_wait_blocks_until_the_lock_is_free(self):
        box = []
        api._STORE_LOCK.acquire()
        thread = threading.Thread(target=lambda: box.append(api._store_call(lambda: "done")))
        thread.start()
        try:
            time.sleep(0.3)
            assert box == [] and thread.is_alive()
        finally:
            api._STORE_LOCK.release()
        thread.join(IO_TIMEOUT)
        assert box == ["done"]

    def test_the_polls_reads_go_through_it(self, tmp_path):
        gateway, _, stub = make_gateway(tmp_path / "srv.tls")
        api._STORE_LOCK.acquire()
        box = []
        thread = threading.Thread(target=lambda: box.append(gateway.poll()), daemon=True)
        thread.start()
        try:
            time.sleep(0.3)
            assert box == [] and stub.key_calls == 0                     # waiting, nothing read
        finally:
            api._STORE_LOCK.release()
        thread.join(IO_TIMEOUT)
        assert box == [True] and stub.key_calls == 1

    @pytest.mark.parametrize("writer", ["enroll_client", "rotate_client"])
    def test_the_writers_give_up_after_store_call_wait(self, tmp_path, mocker, writer):
        from zfsbackup.config.store.errors import ConfigBusy

        gateway, _, _ = make_gateway(
            tmp_path / "srv.tls", enroll_client=lambda *a, **k: "enrolled",
            rotate_client=lambda *a, **k: "rotated",
        )
        mocker.patch.object(api, "STORE_CALL_WAIT", 0.2)
        assert getattr(gateway, writer)() in ("enrolled", "rotated")
        api._STORE_LOCK.acquire()
        try:
            with pytest.raises(ConfigBusy):
                getattr(gateway, writer)()
            with pytest.raises(ConfigBusy):
                gateway.retire_predecessor("ab" * 32)
        finally:
            api._STORE_LOCK.release()

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork")
    @pytest.mark.filterwarnings(
        "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
        "may lead to deadlocks in the child:DeprecationWarning"
    )
    def test_a_forked_child_gets_a_fresh_lock(self):
        """A fork while a thread holds the lock must not leave the child a lock
        nobody can release."""
        api._STORE_LOCK.acquire()
        pid = os.fork()
        if pid == 0:                                   # child
            code = 1
            try:
                ok = api._STORE_LOCK.acquire(timeout=2)
                code = 0 if ok else 2
            finally:
                os._exit(code)
        try:
            _, status = os.waitpid(pid, 0)
        finally:
            api._STORE_LOCK.release()
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0


# ===========================================================================
# Connection limits: total, anonymous, and the absolute request deadline
# ===========================================================================


def _anonymous_tls(port, ctx=None):
    """A TLS 1.3 connection with no certificate (the handshake completed)."""
    raw = socket.create_connection((LOOPBACK, port), timeout=IO_TIMEOUT)
    return (ctx or insecure_client_context()).wrap_socket(raw)


def _closed_by_peer(sock, within) -> bool:
    """Whether the peer closes `sock` (EOF or reset) within `within` seconds."""
    sock.settimeout(within)
    try:
        return sock.recv(1) == b""
    except socket.timeout:
        return False
    except OSError:
        return True


@pytest.mark.integration
class TestConnectionLimits:
    def test_the_defaults(self):
        assert workers.API_MAX_CONNECTIONS == 64
        assert workers.API_MAX_ANONYMOUS_CONNECTIONS == 16
        assert workers.API_HANDSHAKE_TIMEOUT == 30.0 and workers.API_REQUEST_IDLE_TIMEOUT == 30.0
        server = workers._bind_api_server(echo_app, LOOPBACK, 0, make_gateway(
            Path(tempfile.mkdtemp()) / "s.tls")[0])
        try:
            assert server.max_connections == 64 and server.max_anonymous == 16
            assert server.handshake_timeout == 30.0 and server.request_idle_timeout == 30.0
        finally:
            server.server_close()

    def test_over_the_cap_a_new_connection_is_closed_at_once_and_slots_come_back(
        self, pki, caplog
    ):
        silent = []
        with Serving(echo_app, pki.gateway, max_connections=4) as s:
            try:
                with caplog.at_level(logging.WARNING, logger="zfsbackup.workers"):
                    for _ in range(4):
                        silent.append(raw_connect(s.port))        # connects, says nothing
                    time.sleep(0.3)                               # let the threads take their slots
                    over = [raw_connect(s.port) for _ in range(3)]
                    try:
                        assert all(_closed_by_peer(c, 3.0) for c in over)
                    finally:
                        for c in over:
                            c.close()
                capacity = [r for r in caplog.records if "connections already open" in r.getMessage()]
                assert len(capacity) == 1                         # rate limited, not one per refusal
            finally:
                for sock in silent:
                    sock.close()
            assert _eventually(lambda: isinstance(try_get(s.port, "/"), tuple), 10)
            assert https_get(s.port, "/")[0] == 200

    def test_anonymous_connections_cannot_hold_every_slot(self, pki):
        pki.pin_alice()
        holders = []
        with Serving(echo_app, pki.gateway, max_anonymous=2, request_idle_timeout=30.0) as s:
            try:
                for _ in range(2):
                    holders.append(_anonymous_tls(s.port))        # handshake done, no request
                time.sleep(0.3)
                refused = _eventually(
                    lambda: isinstance(try_get(s.port, "/"), Exception), 10
                )
                assert refused, "a third anonymous connection was served"
                # a pinned client still gets through
                assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200
            finally:
                for sock in holders:
                    sock.close()
            assert _eventually(lambda: isinstance(try_get(s.port, "/"), tuple), 10)
            assert https_get(s.port, "/")[0] == 200               # slots released

    def test_a_pinned_connection_does_not_take_an_anonymous_slot(self, pki):
        pki.pin_alice()
        with Serving(echo_app, pki.gateway, max_anonymous=0) as s:
            assert isinstance(try_get(s.port, "/"), Exception)    # no anonymous slot at all
            assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200

    def test_the_anonymous_warning_is_rate_limited(self, pki, caplog):
        with Serving(echo_app, pki.gateway, max_anonymous=0) as s:
            with caplog.at_level(logging.WARNING, logger="zfsbackup.workers"):
                for _ in range(5):
                    try_get(s.port, "/")
        records = [r for r in caplog.records if "without a pinned client" in r.getMessage()]
        assert len(records) == 1

    def test_a_silent_connection_after_the_handshake_is_dropped_at_the_deadline(self, pki):
        with Serving(echo_app, pki.gateway, request_idle_timeout=0.6) as s:
            sock = _anonymous_tls(s.port)
            try:
                started = time.monotonic()
                assert _closed_by_peer(sock, 5.0)
                assert 0.4 < time.monotonic() - started < 4.0
            finally:
                sock.close()

    def test_the_deadline_is_absolute_a_dribbling_peer_is_cut_off(self, pki):
        """One byte every 0.3 s would defeat a per-read timeout of 1 s for ever."""
        with Serving(echo_app, pki.gateway, request_idle_timeout=1.0) as s:
            sock = _anonymous_tls(s.port)
            try:
                started = time.monotonic()
                cut = False
                for byte in b"GET /health HTTP/1.1\r\nHost: x\r\nX-Slow: " + b"a" * 100:
                    try:
                        sock.sendall(bytes([byte]))
                    except OSError:
                        cut = True
                        break
                    time.sleep(0.3)
                    if time.monotonic() - started > 6:
                        break
                elapsed = time.monotonic() - started
                if not cut:
                    cut = _closed_by_peer(sock, 1.0)
                assert cut and elapsed < 5.0, f"dribbler still attached after {elapsed:.1f}s"
            finally:
                sock.close()

    @staticmethod
    def _slow_post(port, ctx, body=b"0123456789", pause=0.25):
        """POST whose headers arrive at once and whose body dribbles (~2.5 s)."""
        sock = _anonymous_tls(port, ctx)
        sock.sendall(
            b"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n" % len(body)
        )
        try:
            for byte in body:
                sock.sendall(bytes([byte]))
                time.sleep(pause)
            sock.settimeout(IO_TIMEOUT)
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
            return data
        except OSError:
            return b""
        finally:
            sock.close()

    @staticmethod
    def _body_length_app(environ, start_response):
        length = len(environ["wsgi.input"].read(int(environ.get("CONTENT_LENGTH") or 0)))
        body = str(length).encode()
        start_response("200 OK", [("Content-Length", str(len(body)))])
        return [body]

    def test_a_pinned_connection_is_not_limited_once_the_app_starts(self, pki):
        pki.pin_alice()
        with Serving(self._body_length_app, pki.gateway, request_idle_timeout=1.0) as s:
            data = self._slow_post(s.port, insecure_client_context(pki.alice))
        assert data.startswith(b"HTTP/1.") and data.endswith(b"10")

    def test_an_anonymous_request_gets_a_fresh_deadline_for_the_whole_request(self, pki):
        with Serving(self._body_length_app, pki.gateway, request_idle_timeout=1.0) as s:
            data = self._slow_post(s.port, None)
        assert not data.endswith(b"10")                      # cut off before the body completed


# ===========================================================================
# Failed handshakes: the reject linger
# ===========================================================================


def _unpinned_request(port, ctx, delay=0.0):
    """What an unpinned client sees: it finishes its (TLS 1.3) handshake, waits `delay`, sends a
    GET and reads. -> the exception it got, or the bytes it read. The delay is what makes a lost
    alert deterministic: the server has rejected the certificate by the time the client writes,
    and a server that closed outright answers that write with a reset."""
    raw = socket.create_connection((LOOPBACK, port), timeout=IO_TIMEOUT)
    tls = ctx.wrap_socket(raw)
    try:
        if delay:
            time.sleep(delay)
        tls.sendall(b"GET /whoami HTTP/1.1\r\nHost: x\r\n\r\n")
        return tls.recv(4096)
    except Exception as exc:                  # SSLError, OSError
        return exc
    finally:
        tls.close()


def _slots(server):
    return server._slots._value, server._anonymous_slots._value


def _all_slots_free(server):
    return _slots(server) == (server.max_connections, server.max_anonymous)


def _time_until_free(server, started, limit):
    """Seconds from `started` until every slot of `server` is back, or None within `limit`."""
    deadline = started + limit
    while time.monotonic() < deadline:
        if _all_slots_free(server):
            return time.monotonic() - started
        time.sleep(0.01)
    return time.monotonic() - started if _all_slots_free(server) else None


def _handshaken_unpinned(port, ctx):
    """An unpinned client with its handshake done and nothing sent. -> the TLS socket."""
    raw = socket.create_connection((LOOPBACK, port), timeout=IO_TIMEOUT)
    return ctx.wrap_socket(raw)


@pytest.mark.integration
class TestRejectLinger:
    DELAYS = [0.0, 0.01, 0.05, 0.1, 0.2]

    # -- the constant and the keyword ----------------------------------------

    def test_the_default_is_one_second(self):
        assert workers.API_REJECT_LINGER == 1.0

    def test_the_keyword_is_keyword_only_and_defaults_to_the_constant(self):
        import inspect

        param = inspect.signature(workers._api_server_class().__init__).parameters["reject_linger"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default == workers.API_REJECT_LINGER

    @pytest.mark.parametrize("kwargs, expected", [({}, 1.0), ({"reject_linger": 0.25}, 0.25),
                                                  ({"reject_linger": 0}, 0)])
    def test_a_server_stores_the_value(self, pki, kwargs, expected):
        server = workers._api_server_class()(LOOPBACK, 0, echo_app, pki.gateway, **kwargs)
        try:
            assert server.reject_linger == expected
        finally:
            server.server_close()

    def test_the_bound_api_server_uses_the_default(self, pki):
        server = workers._bind_api_server(echo_app, LOOPBACK, 0, pki.gateway)
        try:
            assert server.reject_linger == workers.API_REJECT_LINGER
        finally:
            server.server_close()

    # -- the alert reaches a client that writes late -------------------------

    @pytest.mark.parametrize("delay", DELAYS)
    def test_an_unpinned_client_that_writes_after_a_delay_reads_the_alert(self, pki, delay):
        """The point of the linger. Without it the server's `close()` answers the client's
        write with a reset, which can discard the alert unread: the client then sees a
        `BrokenPipeError`, `ConnectionResetError` or `SSLEOFError`."""
        pki.pin_alice()
        ctx = insecure_client_context(pki.mallory)
        with Serving(echo_app, pki.gateway) as s:
            results = [_unpinned_request(s.port, ctx, delay) for _ in range(6)]
        for result in results:
            assert isinstance(result, Exception), result
            assert identity.is_tls_alert_rejection(result), repr(result)

    def test_the_alert_is_read_by_clients_that_write_at_different_times_at_once(self, pki):
        pki.pin_alice()
        ctx = insecure_client_context(pki.mallory)
        results = []
        lock = threading.Lock()

        def one(delay):
            result = _unpinned_request(s.port, ctx, delay)
            with lock:
                results.append(result)

        with Serving(echo_app, pki.gateway) as s:
            threads = [threading.Thread(target=one, args=(d,)) for d in self.DELAYS * 3]
            for t in threads:
                t.start()
            for t in threads:
                t.join(30)
        assert len(results) == len(threads)
        assert all(isinstance(r, Exception) and identity.is_tls_alert_rejection(r)
                   for r in results), [repr(r) for r in results]

    def test_the_server_is_still_fine_afterwards(self, pki):
        pki.pin_alice()
        with Serving(echo_app, pki.gateway) as s:
            for _ in range(3):
                assert isinstance(_unpinned_request(s.port, insecure_client_context(pki.mallory),
                                                    0.05), Exception)
            assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200
            assert https_get(s.port, "/")[0] == 200

    # -- how long the linger lasts -------------------------------------------

    def test_the_linger_ends_when_the_client_closes(self, pki):
        """A long bound, a client that closes at once: the server does not wait for the bound."""
        pki.pin_alice()
        ctx = insecure_client_context(pki.mallory)
        with Serving(echo_app, pki.gateway, reject_linger=30.0) as s:
            started = time.monotonic()
            result = _unpinned_request(s.port, ctx, 0.05)          # closes when it has the alert
            assert identity.is_tls_alert_rejection(result), repr(result)
            freed = _time_until_free(s.server, started, 5.0)
        assert freed is not None and freed < 3.0, freed

    def test_a_silent_client_is_held_for_the_linger_and_no_longer(self, pki):
        """It is a linger, not an immediate close, and not an unbounded one: a client that
        does not write and does not close is released at `reject_linger`."""
        pki.pin_alice()
        linger = 1.0
        with Serving(echo_app, pki.gateway, reject_linger=linger) as s:
            sock = _handshaken_unpinned(s.port, insecure_client_context(pki.mallory))
            try:
                started = time.monotonic()
                time.sleep(0.2)                                   # the server has rejected it
                assert not _all_slots_free(s.server), "closed at once: no linger"
                freed = _time_until_free(s.server, started, linger + 3.0)
            finally:
                sock.close()
        assert freed is not None, "a silent client was held beyond the linger"
        assert linger * 0.8 <= freed <= linger + 1.5, freed

    def test_a_client_that_keeps_writing_is_still_cut_off_at_the_linger(self, pki):
        """The bound is absolute, not 'since the last byte'."""
        pki.pin_alice()
        linger = 0.8
        with Serving(echo_app, pki.gateway, reject_linger=linger) as s:
            tls = _handshaken_unpinned(s.port, insecure_client_context(pki.mallory))
            raw = socket.socket(fileno=tls.detach())
            try:
                started = time.monotonic()
                freed = None
                while time.monotonic() - started < linger + 3.0:
                    try:
                        raw.sendall(b"x" * 64)
                    except OSError:
                        pass                                      # the server has closed
                    if _all_slots_free(s.server):
                        freed = time.monotonic() - started
                        break
                    time.sleep(0.05)
            finally:
                raw.close()
        assert freed is not None, "a dribbling client held the connection beyond the linger"
        assert linger * 0.8 <= freed <= linger + 1.5, freed

    def test_a_client_that_closes_mid_linger_frees_the_slot_at_once(self, pki):
        pki.pin_alice()
        with Serving(echo_app, pki.gateway, reject_linger=30.0) as s:
            sock = _handshaken_unpinned(s.port, insecure_client_context(pki.mallory))
            time.sleep(0.2)
            assert not _all_slots_free(s.server)
            started = time.monotonic()
            sock.close()
            freed = _time_until_free(s.server, started, 5.0)
        assert freed is not None and freed < 3.0, freed

    def test_a_reset_by_the_client_mid_linger_frees_the_slot(self, pki):
        import struct

        pki.pin_alice()
        with Serving(echo_app, pki.gateway, reject_linger=30.0) as s:
            sock = _handshaken_unpinned(s.port, insecure_client_context(pki.mallory))
            time.sleep(0.2)
            started = time.monotonic()
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            sock.close()
            freed = _time_until_free(s.server, started, 5.0)
        assert freed is not None and freed < 3.0, freed

    # -- reject_linger=0 -----------------------------------------------------

    def test_zero_closes_immediately(self, pki):
        pki.pin_alice()
        with Serving(echo_app, pki.gateway, reject_linger=0) as s:
            sock = _handshaken_unpinned(s.port, insecure_client_context(pki.mallory))
            try:
                started = time.monotonic()
                freed = _time_until_free(s.server, started, 5.0)
                assert freed is not None and freed < 1.0, "a silent client was held with no linger"
                # the connection is gone for the client too: EOF, a reset or the alert
                assert _closed_by_peer_or_alert(sock)
            finally:
                sock.close()

    # -- slots ---------------------------------------------------------------

    def test_slots_are_released_after_many_rejections(self, pki):
        pki.pin_alice()
        ctx = insecure_client_context(pki.mallory)
        with Serving(echo_app, pki.gateway, max_connections=4, reject_linger=0.3) as s:
            for i in range(24):                                   # six times the cap
                assert isinstance(_unpinned_request(s.port, ctx, 0.02 if i % 2 else 0.0), Exception)
            assert _eventually(lambda: _all_slots_free(s.server), 10.0), _slots(s.server)
            assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200

    def test_lingering_connections_hold_a_total_slot_but_never_an_anonymous_one(self, pki):
        pki.pin_alice()
        ctx = insecure_client_context(pki.mallory)
        with Serving(echo_app, pki.gateway, max_connections=8, max_anonymous=2,
                     reject_linger=1.0) as s:
            socks = [_handshaken_unpinned(s.port, ctx) for _ in range(5)]
            try:
                assert _eventually(lambda: _slots(s.server)[0] == 3, 5.0), _slots(s.server)
                assert _slots(s.server)[1] == 2               # no anonymous slot is taken
                # and a pinned client is served while they linger
                assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200
            finally:
                for sock in socks:
                    sock.close()
            assert _eventually(lambda: _all_slots_free(s.server), 10.0), _slots(s.server)

    def test_a_linger_does_not_block_other_clients(self, pki):
        """Each connection has its own thread: a slow-to-close rejected peer holds nobody up."""
        pki.pin_alice()
        with Serving(echo_app, pki.gateway, reject_linger=5.0) as s:
            socks = [_handshaken_unpinned(s.port, insecure_client_context(pki.mallory))
                     for _ in range(5)]
            try:
                time.sleep(0.2)
                started = time.monotonic()
                assert https_get(s.port, "/", insecure_client_context(pki.alice))[0] == 200
                assert time.monotonic() - started < 3.0
            finally:
                for sock in socks:
                    sock.close()

    def test_shutdown_returns_promptly_with_lingering_connections(self, pki):
        pki.pin_alice()
        s = Serving(echo_app, pki.gateway, reject_linger=10.0)
        s.thread.start()
        socks = []
        try:
            for _ in range(10):
                socks.append(_handshaken_unpinned(s.port, insecure_client_context(pki.mallory)))
            time.sleep(0.3)
            started = time.monotonic()
            s.server.shutdown()
            assert time.monotonic() - started < 3.0
            s.thread.join(5)
            assert not s.thread.is_alive()
        finally:
            for sock in socks:
                sock.close()
            s.server.server_close()

    # -- other failed handshakes ---------------------------------------------

    def test_a_handshake_that_never_completes_lingers_after_its_timeout(self, pki):
        """The linger covers every failed handshake, a port scan included: dropped at the
        handshake timeout (half-closed: the peer reads EOF), released at timeout + linger."""
        with Serving(echo_app, pki.gateway, handshake_timeout=0.4, reject_linger=0.6) as s:
            with raw_connect(s.port) as sock:
                started = time.monotonic()
                sock.settimeout(IO_TIMEOUT)
                assert sock.recv(10) == b""                       # the half-close, at ~0.4 s
                assert 0.25 < time.monotonic() - started < 3.0
                freed = _time_until_free(s.server, started, 5.0)
        assert freed is not None and 0.8 <= freed <= 2.5, freed

    def test_a_tls_12_client_is_refused_and_released(self, pki):
        ctx = insecure_client_context(maximum_version=ssl.TLSVersion.TLSv1_2)
        with Serving(echo_app, pki.gateway, reject_linger=0.3) as s:
            with pytest.raises(ssl.SSLError):
                https_get(s.port, "/", ctx)
            assert _eventually(lambda: _all_slots_free(s.server), 10.0), _slots(s.server)
            assert https_get(s.port, "/")[0] == 200

    # -- never on a success --------------------------------------------------

    @pytest.mark.parametrize("who", ["pinned", "anonymous"])
    def test_a_successful_handshake_never_lingers(self, pki, mocker, who):
        pki.pin_alice()
        ctx = insecure_client_context(pki.alice if who == "pinned" else None)
        with Serving(echo_app, pki.gateway, reject_linger=30.0) as s:
            spy = mocker.spy(s.server, "_close_after_failed_handshake")
            started = time.monotonic()
            assert https_get(s.port, "/", ctx)[0] == 200
            assert _eventually(lambda: _all_slots_free(s.server), 5.0), _slots(s.server)
            assert time.monotonic() - started < 5.0                   # not held for the linger
        assert spy.call_count == 0

    def test_a_refused_anonymous_connection_closes_at_once_with_no_linger(self, pki, mocker):
        """The anonymous cap closes AFTER a successful handshake: not a failed handshake."""
        with Serving(echo_app, pki.gateway, max_anonymous=0, reject_linger=30.0) as s:
            spy = mocker.spy(s.server, "_close_after_failed_handshake")
            sock = _anonymous_tls(s.port)
            try:
                started = time.monotonic()
                assert _closed_by_peer(sock, 5.0)
                assert time.monotonic() - started < 3.0
                assert _eventually(lambda: _all_slots_free(s.server), 5.0), _slots(s.server)
            finally:
                sock.close()
        assert spy.call_count == 0

    def test_a_rejected_certificate_does_linger(self, pki, mocker):
        """The other side of the two tests above: a rejection is routed through the linger."""
        pki.pin_alice()
        with Serving(echo_app, pki.gateway, reject_linger=0.3) as s:
            spy = mocker.spy(s.server, "_close_after_failed_handshake")
            assert isinstance(_unpinned_request(s.port, insecure_client_context(pki.mallory)),
                              Exception)
            assert _eventually(lambda: spy.call_count == 1, 5.0)

    # -- robustness of the close ---------------------------------------------

    def test_a_peer_that_vanishes_during_the_linger_does_not_leak_the_slot_or_log_errors(
        self, pki, caplog
    ):
        import struct

        pki.pin_alice()
        with caplog.at_level(logging.DEBUG):
            with Serving(echo_app, pki.gateway, reject_linger=2.0) as s:
                for _ in range(5):
                    sock = _handshaken_unpinned(s.port, insecure_client_context(pki.mallory))
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    sock.close()
                assert _eventually(lambda: _all_slots_free(s.server), 10.0), _slots(s.server)
        assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


def _closed_by_peer_or_alert(sock) -> bool:
    """Whether reading `sock` ends (EOF, an alert or a reset) rather than timing out."""
    sock.settimeout(3.0)
    try:
        sock.recv(1)
        return True
    except socket.timeout:
        return False
    except OSError:
        return True


# ===========================================================================
# The trust-poll thread and the restart
# ===========================================================================


@pytest.mark.unit
class TestTrustPollThread:
    class FakeGateway:
        def __init__(self):
            self.polls = 0
            self.error = None

        def poll(self):
            self.polls += 1
            if self.error:
                raise self.error
            return False

    @pytest.fixture
    def worker(self, worker_config, mocker):
        mocker.patch.object(api, "TRUST_POLL_SECONDS", 0.03)
        w = workers.ApiWorker(worker_config, StopFlag())
        w._trust = self.FakeGateway()
        return w

    def test_the_old_main_loop_polling_is_gone(self):
        for name in ("_tick_trust", "_trust_due", "_stop_drain_tick"):
            assert not hasattr(workers.ApiWorker, name), name

    def test_it_polls_on_its_own_daemon_thread_until_stopped(self, worker):
        stopped = worker._start_trust_poll(logging.getLogger("t"))
        try:
            assert _eventually(lambda: worker._trust.polls >= 3, 5)
            (thread,) = [t for t in threading.enumerate() if t.name == "trust-poll"]
            assert thread.daemon is True
        finally:
            stopped.set()
        thread.join(5)
        assert not thread.is_alive()
        polls = worker._trust.polls
        time.sleep(0.2)
        assert worker._trust.polls == polls                    # and stays stopped

    def test_a_failing_poll_does_not_end_the_loop(self, worker):
        worker._trust.error = RuntimeError("boom")
        stopped = worker._start_trust_poll(logging.getLogger("t"))
        try:
            assert _eventually(lambda: worker._trust.polls >= 3, 5)
        finally:
            stopped.set()

    def test_the_reload_drain_callback_polls_nothing_and_watches_stop_and_supervisor(
        self, worker, mocker
    ):
        reason = mocker.patch.object(worker, "_exit_reason", return_value=None)
        assert worker._reload_drain_abort() is False
        reason.return_value = workers.ExitReason.STOP
        assert worker._reload_drain_abort() is True
        reason.return_value = workers.ExitReason.SUPERVISOR_GONE
        assert worker._reload_drain_abort() is True
        reason.return_value = workers.ExitReason.RELOAD
        assert worker._reload_drain_abort() is False          # the drain is for a reload already
        assert worker._trust.polls == 0


@pytest.mark.unit
class TestApiWorkerRestartOnIdentityChange:
    """`TrustGateway.restart_reason`: the worker logs ERROR once, drains like a
    reload (a stop arriving during it becomes a stop) and exits 1."""

    @pytest.fixture
    def run_worker(self, tls_config_db, mocker):
        def build(restart_reason, *, stop_during_drain=False, active=2):
            reasons = {"value": restart_reason}

            class Gateway(api.TrustGateway):
                @property
                def restart_reason(self):
                    return reasons["value"]

            mocker.patch("zfsbackup.api.TrustGateway", Gateway)
            stop = StopFlag()
            app = mocker.MagicMock()
            gate = app.extensions["zfsbackup.stream_gate"]
            gate.active = active
            if stop_during_drain:
                gate.wait_idle.side_effect = lambda *a, **k: stop.set()
            mocker.patch("zfsbackup.api.create_app", return_value=app)
            mocker.patch("zfsbackup.workers._bind_api_server", return_value=mocker.MagicMock())
            thread = mocker.MagicMock()
            thread.is_alive.return_value = True
            mocker.patch("zfsbackup.workers.threading.Thread", return_value=thread)
            worker = workers.ApiWorker(
                BackupConfig(datasets=[DatasetConfig(name="pool/data")]),
                stop, config_db=tls_config_db,
            )
            return worker, gate, stop
        return build

    def test_it_exits_one_after_draining_and_logs_once(self, run_worker, caplog):
        worker, gate, _ = run_worker("the server certificate on disk changed", active=2)
        with caplog.at_level(logging.INFO), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1
        gate.start_draining.assert_called_once()
        gate.wait_idle.assert_called_once()
        assert gate.wait_idle.call_args.args == (None,)
        assert gate.wait_idle.call_args.kwargs["abort"] == worker._reload_drain_abort
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and "API worker restarting: the server certificate on disk changed" in errors[0]
        assert "waiting for 2 in-flight receive(s) to finish before restarting" in caplog.text
        assert "API worker stopped" not in caplog.text

    def test_with_nothing_in_flight_it_exits_one_without_waiting(self, run_worker):
        worker, gate, _ = run_worker("changed", active=0)
        with pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1
        gate.wait_idle.assert_not_called()

    def test_a_stop_during_the_drain_becomes_a_stop(self, run_worker, caplog):
        worker, gate, stop = run_worker("changed", stop_during_drain=True)
        with caplog.at_level(logging.INFO):
            worker.run()                                       # returns: no SystemExit(1)
        assert stop.is_set() and "API worker stopped" in caplog.text

    def test_without_a_restart_reason_it_runs_until_stopped(self, run_worker, caplog):
        worker, gate, stop = run_worker(None)
        stop.set()
        with caplog.at_level(logging.INFO):
            worker.run()
        assert "API worker stopped" in caplog.text and "restarting" not in caplog.text


# ===========================================================================
# Dry-run throwaways: the sweep runs at every API start; setup failures exit 1
# ===========================================================================


@pytest.mark.unit
class TestApiStartSweepAndSetupFailures:
    @pytest.fixture
    def scratch(self, tmp_path, monkeypatch):
        d = tmp_path / "scratch-tmp"
        d.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(d))
        return d

    @pytest.fixture
    def quiet_api(self, mocker):
        mocker.patch("zfsbackup.api.create_app")
        mocker.patch("zfsbackup.workers._bind_api_server")
        mocker.patch("zfsbackup.workers.threading.Thread")

    def _run(self, config_db, **config):
        stop = StopFlag()
        stop.set()
        workers.ApiWorker(
            BackupConfig(datasets=[DatasetConfig(name="pool/data")], **config),
            stop, config_db=config_db,
        ).run()

    def test_a_dead_workers_throwaway_is_swept_at_a_normal_start_too(
        self, tls_config_db, scratch, quiet_api
    ):
        ns = workers._pid_namespace()
        dead = scratch / f"{workers._DRY_RUN_PREFIX}{ns}-2147480001-xyz"
        live = scratch / f"{workers._DRY_RUN_PREFIX}{ns}-{os.getpid()}-xyz"
        foreign = scratch / f"{workers._DRY_RUN_PREFIX}other-ns-2147480002-xyz"
        for d in (dead, live, foreign):
            d.mkdir()
        self._run(tls_config_db)                               # NOT a dry run
        assert not dead.exists() and live.exists() and foreign.exists()

    def test_a_failing_sweep_does_not_stop_the_start(self, tls_config_db, mocker, quiet_api, caplog):
        mocker.patch("zfsbackup.workers._sweep_dry_run_dirs", side_effect=OSError("denied"))
        with caplog.at_level(logging.INFO):
            self._run(tls_config_db)
        assert "API worker stopped" in caplog.text

    def test_the_throwaway_name_carries_namespace_and_pid(self, sqlite_config_db, scratch, mocker):
        seen = []
        real = workers._remove_tree
        mocker.patch.object(workers, "_remove_tree", lambda path: (seen.append(path), real(path)))
        mocker.patch("zfsbackup.api.create_app")
        mocker.patch("zfsbackup.workers._bind_api_server")
        mocker.patch("zfsbackup.workers.threading.Thread")
        self._run(sqlite_config_db, dry_run=True)
        (path,) = seen
        assert os.path.basename(path).startswith(
            f"{workers._DRY_RUN_PREFIX}{workers._pid_namespace()}-{os.getpid()}-"
        )
        assert not os.path.exists(path)

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_any_gateway_setup_failure_is_a_clean_exit_one(
        self, tls_config_db, sqlite_config_db, scratch, mocker, caplog, dry_run
    ):
        db = sqlite_config_db if dry_run else tls_config_db
        # (dry run + no identity: a throwaway directory is made first and must go)
        mocker.patch("zfsbackup.api.TrustGateway", side_effect=RuntimeError("boom"))
        with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as info:
            self._run(db, dry_run=dry_run)
        assert info.value.code == 1
        assert "cannot set up its TLS identity and client trust: RuntimeError: boom" in caplog.text
        assert [p for p in scratch.iterdir() if p.name.startswith(workers._DRY_RUN_PREFIX)] == []


@pytest.mark.unit
class TestAdvisoryIdentityLockTimeout:
    def test_the_supervisors_advisory_read_does_not_wait_long_for_the_identity_lock(
        self, tmp_path, mocker
    ):
        assert daemon_module.ADVISORY_IDENTITY_LOCK_TIMEOUT == 0.5
        tls = tmp_path / "config.db.tls"
        client_identity(tls)
        calls = []
        real = identity.load_identity

        def spy(*args, **kwargs):
            calls.append(kwargs)
            return real(*args, **kwargs)

        mocker.patch.object(daemon_module.identity, "load_identity", spy)
        config = BackupConfig(
            datasets=[DatasetConfig(
                name="pool/data",
                remote=[RemoteDatasetConfig(destination="o")],
            )],
            destinations={"o": Destination(url="https://h/p", server_fingerprint="ab" * 32)},
        )
        daemon_module._client_tls_problem(config, "host-1", tls)
        assert calls and calls[0]["lock_timeout"] == daemon_module.ADVISORY_IDENTITY_LOCK_TIMEOUT


# ===========================================================================
# A real ApiWorker process
# ===========================================================================


class ApiProcess:
    """A real `ApiWorker` child on a free port, started with `method`."""

    def __init__(self, resolved, method, *, dry_run=False, config_overrides=None):
        from _reload_workers import SelectableApiWorker

        self.port = _free_port()
        config = BackupConfig(
            datasets=[DatasetConfig(name="pool/data")], api_host=LOOPBACK, api_port=self.port,
            dry_run=dry_run, **(config_overrides or {}),
        )
        self.stop = StopFlag()
        self.worker = SelectableApiWorker(config, self.stop, config_db=resolved)
        self.worker.start_method = method

    def __enter__(self):
        self.worker.start()
        return self

    def ready(self, timeout=90.0):
        def up():
            if not self.worker.is_alive():
                pytest.fail(f"the API worker exited early (exit code {self.worker.exitcode})")
            result = try_get(self.port, "/health")
            return isinstance(result, tuple) and result[0] == 200
        assert _eventually(up, timeout, 0.2), "the API worker never answered /health"

    def __exit__(self, *exc):
        self.halt()

    def halt(self):
        self.stop.set()
        self.worker.join(30)
        if self.worker.is_alive():
            self.worker.terminate()
            self.worker.join(10)
        if self.worker.is_alive():
            self.worker.kill()
            self.worker.join(10)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestRealApiWorker:
    """The real thing: identity from `<db>.tls`, trust from the config database
    through `store.trust`, under each start method."""

    @pytest.mark.parametrize("method", START_METHODS)
    def test_revocation_and_addition_take_effect_with_no_restart(
        self, tls_config_db, tmp_path, method
    ):
        alice = client_identity(tmp_path / "alice.tls")
        server_fp = identity.peek_identity(
            identity.tls_dir(tls_config_db.resolved_path), "server"
        ).fingerprint
        with ApiProcess(tls_config_db, method) as api_proc:
            api_proc.ready()
            pid = api_proc.worker.pid

            def accepted():
                r = try_get(api_proc.port, "/health", insecure_client_context(alice))
                return isinstance(r, tuple) and r[0] == 200

            # served: no certificate; and the identity presented is the one on disk
            with socket.create_connection((LOOPBACK, api_proc.port), timeout=IO_TIMEOUT) as raw:
                with insecure_client_context().wrap_socket(raw) as tls:
                    assert tls.version() == "TLSv1.3"
                    assert identity.fingerprint_der(tls.getpeercert(binary_form=True)) == server_fp
            assert not accepted()                               # alice is not pinned yet

            pin_client(tls_config_db, alice, "alice")           # the CLI / API worker's writer
            dispose_all()
            assert _eventually(accepted, 30), "a newly pinned certificate was never accepted"

            unpin_client(tls_config_db, alice)
            dispose_all()
            assert _eventually(lambda: not accepted(), 30), "a revoked certificate stayed trusted"
            assert api_proc.worker.is_alive() and api_proc.worker.pid == pid   # never restarted

    @pytest.mark.parametrize("method", START_METHODS)
    def test_https_only_tls_13_only(self, tls_config_db, method):
        with ApiProcess(tls_config_db, method) as api_proc:
            api_proc.ready()
            assert https_get(api_proc.port, "/health")[0] == 200
            with pytest.raises(ssl.SSLError):
                https_get(api_proc.port, "/health",
                          insecure_client_context(maximum_version=ssl.TLSVersion.TLSv1_2))
            with raw_connect(api_proc.port) as sock:
                sock.sendall(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
                try:
                    data = sock.recv(4096)
                except OSError:
                    data = b""
                assert not data.startswith(b"HTTP/")

    def test_a_stop_ends_the_worker_with_a_silent_peer_attached(self, tls_config_db):
        method = START_METHODS[0]
        with ApiProcess(tls_config_db, method) as api_proc:
            api_proc.ready()
            with raw_connect(api_proc.port):
                started = time.monotonic()
                api_proc.stop.set()
                api_proc.worker.join(30)
                assert not api_proc.worker.is_alive()
                assert api_proc.worker.exitcode == 0
                assert time.monotonic() - started < 25


# ===========================================================================
# The server identity: startup, dry run, --test-config
# ===========================================================================


def _worker_in_process(config, resolved):
    """An `ApiWorker` to call `run()` on inline (the supervisor watch is not armed
    in the constructing process)."""
    return workers.ApiWorker(config, StopFlag(), config_db=resolved)


@pytest.mark.unit
class TestApiWorkerStartRefusals:
    def _config(self, **kw):
        return BackupConfig(datasets=[DatasetConfig(name="pool/data")], **kw)

    def test_no_config_database_means_exit_one(self, caplog):
        worker = workers.ApiWorker(self._config(), StopFlag())
        with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1
        assert "no config database path" in caplog.text and "no plaintext mode" in caplog.text

    def test_a_missing_server_identity_is_exit_one_outside_a_dry_run(
        self, sqlite_config_db, caplog
    ):
        worker = _worker_in_process(self._config(), sqlite_config_db)
        with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1
        assert "cannot load the server TLS identity" in caplog.text
        assert not identity.tls_dir(sqlite_config_db.resolved_path).exists()   # and created none

    def test_an_unusable_server_identity_is_exit_one(self, tls_config_db, caplog):
        tls = identity.tls_dir(tls_config_db.resolved_path)
        (tls / "server.key").write_text("garbage\n")
        (tls / "server.key").chmod(0o600)
        worker = _worker_in_process(self._config(), tls_config_db)
        with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1 and "cannot load the server TLS identity" in caplog.text

    def test_a_server_certificate_that_is_a_client_certificate_is_exit_one(
        self, sqlite_config_db, tmp_path, caplog
    ):
        tls = identity.tls_dir(sqlite_config_db.resolved_path)
        wrong = client_identity(tmp_path / "wrong.tls")
        tls.mkdir(mode=0o750)
        for suffix in ("crt", "key"):
            (tls / f"server.{suffix}").write_bytes((tmp_path / "wrong.tls" / f"client.{suffix}").read_bytes())
        (tls / "server.key").chmod(0o600)
        worker = _worker_in_process(self._config(), sqlite_config_db)
        with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1 and "serverAuth" in caplog.text
        assert wrong.fingerprint                                 # (silence: the file was used)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skipif("fork" not in multiprocessing.get_all_start_methods(), reason="no fork")
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestDryRunApiWorker:
    """`--dry-run` creates no key: with no server identity the API worker serves
    with a throwaway one in a private temporary directory, removed on exit."""

    PREFIX = workers._DRY_RUN_PREFIX

    @pytest.fixture
    def private_tmp(self, tmp_path, monkeypatch):
        """Where the throwaway directories go (inherited by a forked child)."""
        scratch = tmp_path / "scratch-tmp"
        scratch.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(scratch))
        return scratch

    def _presented(self, port) -> str:
        with socket.create_connection((LOOPBACK, port), timeout=IO_TIMEOUT) as raw:
            with insecure_client_context().wrap_socket(raw) as tls:
                return identity.fingerprint_der(tls.getpeercert(binary_form=True))

    def _throwaways(self, scratch):
        return sorted(p for p in scratch.iterdir() if p.name.startswith(self.PREFIX))

    def test_without_an_identity_it_serves_a_throwaway_and_creates_nothing_durable(
        self, sqlite_config_db, private_tmp
    ):
        tls = identity.tls_dir(sqlite_config_db.resolved_path)
        with ApiProcess(sqlite_config_db, "fork", dry_run=True) as proc:
            proc.ready()
            fingerprint = self._presented(proc.port)
            (scratch_dir,) = self._throwaways(private_tmp)
            assert f"{self.PREFIX}{workers._pid_namespace()}-{proc.worker.pid}-" in scratch_dir.name
            assert (scratch_dir.stat().st_mode & 0o777) == 0o700            # private
            thrown = identity.peek_identity(scratch_dir / "server.tls", "server")
            assert thrown is not None and thrown.fingerprint == fingerprint
            assert not tls.exists()                                          # nothing durable
        assert self._throwaways(private_tmp) == []                           # cleaned on exit
        assert not tls.exists()

    def test_a_half_written_identity_counts_as_missing_and_is_left_alone(
        self, sqlite_config_db, private_tmp
    ):
        tls = identity.tls_dir(sqlite_config_db.resolved_path)
        real = server_identity(tls)
        (tls / "server.key").unlink()                     # a cert with no key
        before = (tls / "server.crt").read_bytes()
        with ApiProcess(sqlite_config_db, "fork", dry_run=True) as proc:
            proc.ready()
            assert self._presented(proc.port) != real.fingerprint
            assert len(self._throwaways(private_tmp)) == 1
        assert (tls / "server.crt").read_bytes() == before and not (tls / "server.key").exists()
        assert self._throwaways(private_tmp) == []

    def test_with_an_identity_it_serves_that_one_and_makes_no_throwaway(
        self, tls_config_db, private_tmp
    ):
        tls = identity.tls_dir(tls_config_db.resolved_path)
        on_disk = identity.peek_identity(tls, "server").fingerprint
        listing = sorted(p.name for p in tls.iterdir())
        with ApiProcess(tls_config_db, "fork", dry_run=True) as proc:
            proc.ready()
            assert self._presented(proc.port) == on_disk
            assert self._throwaways(private_tmp) == []
        assert sorted(p.name for p in tls.iterdir()) == listing            # nothing created


@pytest.mark.unit
class TestDryRunSweep:
    """`workers._sweep_dry_run_dirs`: a worker killed by a signal cannot clean up;
    a later sweep removes the directories of dead workers of this PID namespace,
    never a live one's, never another namespace's, never anything else."""

    def _name(self, pid, namespace=None, suffix="abc"):
        ns = workers._pid_namespace() if namespace is None else namespace
        return f"{workers._DRY_RUN_PREFIX}{ns}-{pid}-{suffix}"

    def test_dead_workers_directories_are_removed_and_live_ones_kept(self, tmp_path):
        dead = tmp_path / self._name(2147480001)               # a pid that cannot exist
        live = tmp_path / self._name(os.getpid())
        other_ns = tmp_path / self._name(2147480003, namespace="not-this-namespace-0")
        unrelated = tmp_path / "unrelated-dir"
        notpid = tmp_path / self._name("notapid")
        for d in (dead, live, other_ns, unrelated, notpid):
            d.mkdir()
            (d / "file").write_text("x")
        workers._sweep_dry_run_dirs(str(tmp_path))
        assert not dead.exists()
        assert live.exists() and unrelated.exists() and notpid.exists()
        assert other_ns.exists()                                # pids mean nothing across namespaces

    def test_a_missing_directory_is_not_an_error(self, tmp_path):
        workers._sweep_dry_run_dirs(str(tmp_path / "missing"))

    def test_a_symlink_with_the_prefix_is_not_followed(self, tmp_path):
        victim = tmp_path / "victim"
        victim.mkdir()
        (victim / "keep").write_text("x")
        (tmp_path / self._name(2147480002, suffix="link")).symlink_to(victim)
        workers._sweep_dry_run_dirs(str(tmp_path))
        assert (victim / "keep").exists()                       # rmtree of a symlink is refused


@pytest.mark.unit
class TestServerIdentityAtStartup:
    """`daemon._ensure_server_identity`: the supervisor creates the server
    identity after the presence lock and before the first fork; a dry-run FLAG
    creates nothing."""

    def tls(self, resolved):
        return identity.tls_dir(resolved.resolved_path)

    def test_it_creates_the_identity_with_the_right_modes(self, sqlite_config_db, caplog):
        with caplog.at_level(logging.INFO):
            assert daemon_module._ensure_server_identity(sqlite_config_db, False) is True
        tls = self.tls(sqlite_config_db)
        assert (tls.stat().st_mode & 0o777) == 0o750
        assert ((tls / "server.key").stat().st_mode & 0o777) == 0o600
        assert ((tls / "server.crt").stat().st_mode & 0o777) == 0o640
        fingerprint = identity.peek_identity(tls, "server").fingerprint
        assert f"fingerprint {fingerprint}" in caplog.text

    def test_it_is_idempotent(self, sqlite_config_db):
        assert daemon_module._ensure_server_identity(sqlite_config_db, False)
        first = identity.peek_identity(self.tls(sqlite_config_db), "server").fingerprint
        assert daemon_module._ensure_server_identity(sqlite_config_db, False)
        assert identity.peek_identity(self.tls(sqlite_config_db), "server").fingerprint == first

    def test_a_dry_run_flag_creates_nothing_and_says_so(self, sqlite_config_db, caplog):
        before = sorted(p.name for p in Path(sqlite_config_db.resolved_path).parent.iterdir())
        with caplog.at_level(logging.WARNING):
            assert daemon_module._ensure_server_identity(sqlite_config_db, True) is True
        after = sorted(p.name for p in Path(sqlite_config_db.resolved_path).parent.iterdir())
        assert after == before and not self.tls(sqlite_config_db).exists()
        assert "throwaway identity" in caplog.text and "does not create one" in caplog.text

    def test_a_dry_run_validates_an_existing_identity_without_changing_it(
        self, tls_config_db, caplog
    ):
        tls = self.tls(tls_config_db)
        files = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tls.iterdir()}
        with caplog.at_level(logging.INFO):
            assert daemon_module._ensure_server_identity(tls_config_db, True) is True
        assert files == {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tls.iterdir()}
        assert "server TLS identity" in caplog.text

    def test_a_dry_run_refuses_what_a_real_start_would_refuse(self, tls_config_db, caplog):
        (self.tls(tls_config_db) / "server.crt").write_text("not a certificate\n")
        with caplog.at_level(logging.ERROR):
            assert daemon_module._ensure_server_identity(tls_config_db, True) is False
        assert "cannot set up the API server's TLS identity" in caplog.text

    def test_a_real_start_does_not_replace_a_broken_pair(self, tls_config_db, caplog):
        """A complete but unacceptable pair (here: the key does not match) is
        refused, not silently regenerated -- that would orphan every pin."""
        tls = self.tls(tls_config_db)
        other = server_identity(tls.parent / "other.tls")
        (tls / "server.key").write_bytes(other.key_path.read_bytes())
        (tls / "server.key").chmod(0o600)
        cert_before = (tls / "server.crt").read_bytes()
        with caplog.at_level(logging.ERROR):
            assert daemon_module._ensure_server_identity(tls_config_db, False) is False
        assert (tls / "server.crt").read_bytes() == cert_before
        assert "HTTPS only" in caplog.text

    def test_a_fifo_in_place_of_the_key_is_refused_without_hanging(self, sqlite_config_db):
        from test_zfsbackup_reload import _within

        tls = self.tls(sqlite_config_db)
        tls.mkdir(mode=0o750)
        (tls / "server.crt").write_text("x")
        os.mkfifo(tls / "server.key")
        assert _within(5, daemon_module._ensure_server_identity, sqlite_config_db, False) is False


@pytest.mark.unit
class TestTestConfigTls:
    """`--test-config` reports the TLS state and creates nothing."""

    def _run(self, db, caplog, *flags):
        from unittest.mock import patch

        with patch("sys.argv", ["daemon", "-c", str(db), "--test-config", *flags]):
            with caplog.at_level(logging.INFO):
                return daemon_module.main()

    def _listing(self, resolved):
        return sorted(p.name for p in Path(resolved.resolved_path).parent.iterdir())

    def test_no_identity_yet_is_reported_and_nothing_is_created(self, sqlite_config_db, caplog):
        before = set(self._listing(sqlite_config_db))
        assert self._run(sqlite_config_db.path, caplog) == 0
        assert "Server TLS identity: none yet" in caplog.text
        assert "the daemon creates it when it starts" in caplog.text
        assert "client id: not set yet" in caplog.text
        # (reading the database may add its own WAL sidecars; nothing TLS or daemon)
        created = set(self._listing(sqlite_config_db)) - before
        assert not [n for n in created if "tls" in n or "pid" in n or n.startswith("server")]
        assert not identity.tls_dir(sqlite_config_db.resolved_path).exists()

    def test_an_existing_identity_is_reported_by_fingerprint_without_touching_it(
        self, tls_config_db, caplog
    ):
        tls = identity.tls_dir(tls_config_db.resolved_path)
        fingerprint = identity.peek_identity(tls, "server").fingerprint
        files = {p.name: p.stat().st_mtime_ns for p in tls.iterdir()}
        assert self._run(tls_config_db.path, caplog) == 0
        assert f"Server TLS identity: fingerprint {fingerprint}, expires " in caplog.text
        assert files == {p.name: p.stat().st_mtime_ns for p in tls.iterdir()}

    def test_an_unreadable_certificate_is_a_warning_not_a_failure(self, tls_config_db, caplog):
        tls = identity.tls_dir(tls_config_db.resolved_path)
        (tls / "server.crt").write_text("garbage\n")
        assert self._run(tls_config_db.path, caplog) == 0
        assert "Server TLS identity: unusable" in caplog.text

    def test_it_takes_no_lock_and_writes_no_pid_file(self, tls_config_db, lock_spy, caplog):
        assert self._run(tls_config_db.path, caplog) == 0
        assert lock_spy == []
        assert not Path(str(tls_config_db.resolved_path) + ".daemon.pid").exists()


# ===========================================================================
# S-12: a client whose certificate has expired (or is not valid yet) is named
# ===========================================================================


@pytest.fixture
def fake_time(mocker):
    clock = FakeTime()
    mocker.patch.object(workers, "time", clock)
    return clock


def _expired_message(peer):
    return (f"API: TLS client {peer} refused: its client certificate has expired; the client "
            "must enroll again with a new token (check its clock too)")


def _not_yet_valid_message(peer):
    return (f"API: TLS client {peer} refused: its client certificate is not valid yet; check "
            "the clocks of the client and of this host")


def _validity_warnings(caplog):
    return [r for r in caplog.records
            if r.name == PEER_LOGGER and r.levelno == logging.WARNING
            and "refused" in r.getMessage()]


def _verification_error(code):
    exc = ssl.SSLCertVerificationError(1, f"certificate verify failed (code {code})")
    exc.verify_code = code
    return exc


@pytest.mark.integration
class TestHandshakeValidityWarning:
    """S-12 (1): a TLS 1.3 client's rejected certificate is otherwise one DEBUG line on the
    server. An expired or not-yet-valid PINNED certificate (OpenSSL verify codes 10 / 9) is
    now one WARNING per peer IP per `API_VALIDITY_WARN_EVERY`; every other handshake failure
    stays DEBUG."""

    @pytest.fixture
    def kit(self, tmp_path):
        class Kit:
            pass

        k = Kit()
        k.gateway, k.server_identity, k.trust = make_gateway(tmp_path / "srv.tls")
        k.tmp = tmp_path
        k.n = 0

        def pinned(label, *, not_before, not_after):
            """-> an `ssl.SSLContext` presenting a certificate with this validity window,
            pinned in the gateway."""
            k.n += 1
            cert = make_client_cert(cn=f"{label}-{k.n}", not_before=not_before, not_after=not_after)
            k.trust.set_pins(
                [*k.trust.pins, trusted_pin(cert, f"host-{k.n}")],
                generation=k.trust.generation + 1,
            )
            k.gateway.refresh()
            directory = tmp_path / f"{label}-{k.n}.tls"
            write_identity_files(directory, cert)
            ctx = insecure_client_context()
            ctx.load_cert_chain(str(directory / "client.crt"), str(directory / "client.key"))
            return ctx

        now = dt.datetime.now(dt.timezone.utc)
        k.expired = lambda: pinned(
            "expired", not_before=now - dt.timedelta(days=9), not_after=now - dt.timedelta(days=1))
        k.not_yet_valid = lambda: pinned(
            "future", not_before=now + dt.timedelta(days=1), not_after=now + dt.timedelta(days=9))
        k.valid = lambda: pinned(
            "valid", not_before=now - dt.timedelta(hours=1), not_after=now + dt.timedelta(days=9))
        k.unpinned = lambda: insecure_client_context(client_identity(tmp_path / "stranger.tls"))
        return k

    @contextlib.contextmanager
    def _server(self, kit, **kwargs):
        """An `ApiServer` that is NOT serving: connections are fed to `process_request`
        over a socketpair (so the peer address can be anything), and the call blocks until
        the server has finished with the connection."""
        cls = workers._api_server_class()
        server = cls(LOOPBACK, 0, echo_app, kit.gateway, **kwargs)
        try:
            yield server
        finally:
            server.server_close()

    @staticmethod
    def _feed(server, address, client_ctx=None, *, send=b"", timeout=IO_TIMEOUT):
        """One connection from `address` through `server.process_request`; -> once the
        server has closed it (it does that after logging)."""
        server_side, client_side = socket.socketpair()
        client_side.settimeout(timeout)
        server.process_request(server_side, address)
        try:
            if client_ctx is not None:
                try:
                    client = client_ctx.wrap_socket(client_side)
                except OSError:
                    client = None
                    client_side.settimeout(timeout)
                else:
                    client_side = client
            if send:
                client_side.sendall(send)
            try:
                while client_side.recv(4096):
                    pass
            except OSError:
                # The TLS alert arrives before the server has logged anything; the
                # server's half-close after logging is the EOF to wait for, on the
                # plain socket.
                if isinstance(client_side, ssl.SSLSocket):
                    client_side = socket.socket(fileno=client_side.detach())
                    client_side.settimeout(timeout)
                    try:
                        while client_side.recv(4096):
                            pass
                    except OSError:
                        pass
        finally:
            client_side.close()

    # -- the real thing: a pinned expired certificate over TCP -----------------------

    def test_an_expired_pinned_certificate_is_one_warning_naming_the_peer(self, kit, caplog):
        ctx = kit.expired()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"):
            with Serving(echo_app, kit.gateway) as s:
                spy = _spy_allow(s.server)
                assert isinstance(try_get(s.port, "/", ctx), Exception)
                assert _eventually(lambda: len(spy.calls) >= 1, 10)
                assert _eventually(lambda: _validity_warnings(caplog), 10)
        (record,) = _validity_warnings(caplog)
        assert record.getMessage() == _expired_message(LOOPBACK)
        assert record.name == PEER_LOGGER and record.levelno == logging.WARNING
        # the DEBUG handshake line stays where it was, on the worker logger
        (debug,) = [r for r in caplog.records
                    if r.name == "zfsbackup.workers" and r.levelno == logging.DEBUG
                    and "TLS handshake with" in r.getMessage()]
        assert "certificate has expired" in debug.getMessage()
        assert [r for r in caplog.records
                if r.name == "zfsbackup.workers" and r.levelno >= logging.WARNING] == []

    def test_a_not_yet_valid_pinned_certificate_has_its_own_wording(self, kit, caplog):
        ctx = kit.not_yet_valid()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"):
            with Serving(echo_app, kit.gateway) as s:
                assert isinstance(try_get(s.port, "/", ctx), Exception)
                assert _eventually(lambda: _validity_warnings(caplog), 10)
        (record,) = _validity_warnings(caplog)
        assert record.getMessage() == _not_yet_valid_message(LOOPBACK)

    def test_a_valid_pinned_certificate_logs_no_warning(self, kit, caplog):
        ctx = kit.valid()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"):
            with Serving(echo_app, kit.gateway) as s:
                assert https_get(s.port, "/", ctx)[0] == 200
        assert _validity_warnings(caplog) == []

    def test_the_server_keeps_serving_after_the_refusals(self, kit, caplog):
        expired, valid = kit.expired(), kit.valid()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"):
            with Serving(echo_app, kit.gateway) as s:
                for _ in range(3):
                    assert isinstance(try_get(s.port, "/", expired), Exception)
                assert https_get(s.port, "/", valid)[0] == 200

    # -- the throttle, through the full handshake path ---------------------------

    def test_a_repeat_from_the_same_ip_within_an_hour_is_silent(self, kit, caplog, fake_time):
        ctx = kit.expired()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            for _ in range(4):
                self._feed(server, ("203.0.113.7", 40000), ctx)
        assert [r.getMessage() for r in _validity_warnings(caplog)] == [_expired_message("203.0.113.7")]

    def test_the_same_ip_is_named_again_after_an_hour(self, kit, caplog, fake_time):
        ctx = kit.expired()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            self._feed(server, ("203.0.113.7", 40000), ctx)
            fake_time.advance(workers.API_VALIDITY_WARN_EVERY - 30)
            self._feed(server, ("203.0.113.7", 40001), ctx)
            assert len(_validity_warnings(caplog)) == 1           # 30 s short of the hour
            fake_time.advance(60)
            self._feed(server, ("203.0.113.7", 40002), ctx)
            assert len(_validity_warnings(caplog)) == 2
            self._feed(server, ("203.0.113.7", 40003), ctx)
            assert len(_validity_warnings(caplog)) == 2           # and the new hour has begun

    def test_distinct_ips_are_independent(self, kit, caplog, fake_time):
        ctx = kit.expired()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            for ip in ("198.51.100.1", "198.51.100.2", "198.51.100.1", "198.51.100.3",
                       "198.51.100.2"):
                self._feed(server, (ip, 40000), ctx)
        assert [r.getMessage() for r in _validity_warnings(caplog)] == [
            _expired_message("198.51.100.1"), _expired_message("198.51.100.2"),
            _expired_message("198.51.100.3"),
        ]

    def test_the_peer_port_does_not_make_a_new_peer(self, kit, caplog, fake_time):
        ctx = kit.expired()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            for port in (1, 2, 3):
                self._feed(server, ("198.51.100.9", port), ctx)
        assert len(_validity_warnings(caplog)) == 1

    def test_an_ipv6_peer_is_named_by_its_address(self, kit, caplog, fake_time):
        ctx = kit.not_yet_valid()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            self._feed(server, ("2001:db8::7", 40000, 0, 0), ctx)
        assert [r.getMessage() for r in _validity_warnings(caplog)] == [
            _not_yet_valid_message("2001:db8::7")]

    # -- everything else stays quiet ---------------------------------------------------

    def test_an_unpinned_certificate_is_not_a_validity_warning(self, kit, caplog):
        kit.valid()                                               # (some pins exist)
        ctx = kit.unpinned()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            spy = _spy_allow(server)
            self._feed(server, ("203.0.113.7", 40000), ctx)
        assert "handshake with" in caplog.text and "failed" in caplog.text   # it was refused
        assert _validity_warnings(caplog) == [] and spy.calls == []

    def test_a_handshake_that_times_out_is_not_a_validity_warning(self, kit, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), \
                self._server(kit, handshake_timeout=0.3) as server:
            spy = _spy_allow(server)
            self._feed(server, ("203.0.113.7", 40000))              # says nothing at all
        assert "handshake with" in caplog.text
        assert _validity_warnings(caplog) == [] and spy.calls == []

    def test_a_peer_that_does_not_speak_tls_is_not_a_validity_warning(self, kit, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            spy = _spy_allow(server)
            self._feed(server, ("203.0.113.7", 40000), send=b"GET / HTTP/1.1\r\n\r\n")
        assert "handshake with" in caplog.text
        assert _validity_warnings(caplog) == [] and spy.calls == []

    @pytest.mark.parametrize("exc", [
        pytest.param(_verification_error(code), id=f"verify-code-{code}")
        for code in (0, 2, 7, 8, 18, 19, 20, 21, 23, 26, 27, 34, 62, 11, 12)
    ] + [
        pytest.param(ssl.SSLError("[SSL: WRONG_VERSION_NUMBER] wrong version number"),
                     id="plain-SSLError"),
        pytest.param(ssl.SSLEOFError("EOF occurred in violation of protocol"), id="eof"),
        pytest.param(ssl.SSLZeroReturnError("closed"), id="zero-return"),
        pytest.param(TimeoutError("timed out"), id="timeout"),
        pytest.param(ConnectionResetError("reset"), id="reset"),
        pytest.param(OSError("boom"), id="oserror"),
        pytest.param(ssl.SSLError("certificate has expired"), id="expired-text-but-not-a-verify-error"),
    ])
    def test_other_failures_never_warn(self, kit, caplog, exc):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            server._warn_certificate_validity(exc, ("203.0.113.7", 1))
        assert _validity_warnings(caplog) == []

    def test_an_error_without_a_verify_code_never_warns(self, kit, caplog):
        exc = ssl.SSLCertVerificationError(1, "no code")
        exc.verify_code = None
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            server._warn_certificate_validity(exc, ("203.0.113.7", 1))
        assert _validity_warnings(caplog) == []

    @pytest.mark.parametrize("code, message", [
        (10, _expired_message), (9, _not_yet_valid_message),
    ])
    def test_the_verify_code_alone_decides_the_wording(self, kit, caplog, code, message):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            server._warn_certificate_validity(_verification_error(code), ("203.0.113.7", 1))
        assert [r.getMessage() for r in _validity_warnings(caplog)] == [message("203.0.113.7")]

    # -- robustness ---------------------------------------------------------------------

    @pytest.mark.parametrize("address", [None, (), ("",), 5, [], object()],
                             ids=["none", "empty-tuple", "empty-host", "int", "empty-list", "object"])
    def test_a_malformed_client_address_never_raises(self, kit, caplog, address):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            server._warn_certificate_validity(_verification_error(10), address)

    @pytest.mark.parametrize("address", [None, ()], ids=["none", "empty-tuple"])
    def test_a_malformed_client_address_does_not_break_the_handshake_path(
        self, kit, caplog, address
    ):
        """The failed handshake is still logged at DEBUG, the connection is closed (the
        peer reads the rejection, not a hang), no warning is made up, and the server goes
        on serving."""
        expired, valid = kit.expired(), kit.valid()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), \
                Serving(echo_app, kit.gateway) as s:
            started = time.monotonic()
            self._feed(s.server, address, expired)
            assert time.monotonic() - started < 5
            assert https_get(s.port, "/", valid)[0] == 200
        assert "handshake with" in caplog.text
        assert _validity_warnings(caplog) == []

    def test_a_failing_throttle_does_not_break_the_handshake_path(self, kit, caplog, mocker):
        ctx = kit.expired()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.workers"), self._server(kit) as server:
            mocker.patch.object(server._validity_throttle, "allow", side_effect=RuntimeError("boom"))
            self._feed(server, ("203.0.113.7", 40000), ctx)
        assert _validity_warnings(caplog) == []
        assert "validity warning failed" in caplog.text            # DEBUG, with the traceback

    def test_the_slot_is_released_after_a_refusal(self, kit, fake_time):
        ctx = kit.expired()
        with self._server(kit, max_connections=2) as server:
            for i in range(6):                                     # more refusals than slots
                self._feed(server, ("203.0.113.7", 40000 + i), ctx)
            assert server._slots.acquire(blocking=False)           # still room
            server._slots.release()


class _SpyAllow:
    def __init__(self, throttle):
        self.calls = []
        self._real = throttle.allow
        throttle.allow = self

    def __call__(self, peer):
        result = self._real(peer)
        self.calls.append((peer, result))
        return result


def _spy_allow(server) -> _SpyAllow:
    return _SpyAllow(server._validity_throttle)


@pytest.mark.unit
class TestValidityWarnThrottle:
    """`workers._ValidityWarnThrottle`: one WARNING per peer per interval, bounded memory,
    least-recently-WARNED eviction, thread-safe."""

    @pytest.fixture
    def clock(self, fake_time):
        fake_time.frozen = 1000.0
        return fake_time

    def test_the_defaults_are_the_documented_constants(self):
        assert workers.API_VALIDITY_WARN_EVERY == 3600.0
        assert workers.API_VALIDITY_WARN_PEERS == 256
        t = workers._ValidityWarnThrottle()
        assert t._every == 3600.0 and t._max == 256

    def test_the_server_gets_the_default_throttle(self, pki):
        server = workers._bind_api_server(echo_app, LOOPBACK, 0, pki.gateway)
        try:
            assert isinstance(server._validity_throttle, workers._ValidityWarnThrottle)
            assert server._validity_throttle._every == workers.API_VALIDITY_WARN_EVERY
            assert server._validity_throttle._max == workers.API_VALIDITY_WARN_PEERS
        finally:
            server.server_close()

    def test_the_first_attempt_is_allowed_and_the_repeat_is_not(self, clock):
        t = workers._ValidityWarnThrottle(every=100, max_peers=8)
        assert t.allow("a") is True
        assert t.allow("a") is False
        assert t.allow("a") is False

    def test_the_boundary_is_exactly_the_interval(self, clock):
        t = workers._ValidityWarnThrottle(every=100, max_peers=8)
        assert t.allow("a") is True
        clock.frozen += 99.9
        assert t.allow("a") is False
        clock.frozen += 0.1
        assert t.allow("a") is True                                # 100 s after the warning

    def test_a_suppressed_attempt_does_not_extend_the_window(self, clock):
        t = workers._ValidityWarnThrottle(every=100, max_peers=8)
        assert t.allow("a") is True
        clock.frozen += 60
        assert t.allow("a") is False                               # seen again, not warned
        clock.frozen += 40
        assert t.allow("a") is True                                # measured from the warning

    def test_an_allowed_attempt_restarts_the_window(self, clock):
        t = workers._ValidityWarnThrottle(every=100, max_peers=8)
        assert t.allow("a") is True
        clock.frozen += 100
        assert t.allow("a") is True
        clock.frozen += 99
        assert t.allow("a") is False

    def test_peers_are_independent(self, clock):
        t = workers._ValidityWarnThrottle(every=100, max_peers=8)
        assert [t.allow(p) for p in "abab"] == [True, True, False, False]

    def test_the_map_never_outgrows_the_cap(self, clock):
        t = workers._ValidityWarnThrottle(every=100, max_peers=5)
        for i in range(1000):
            t.allow(f"10.0.{i // 256}.{i % 256}")
            assert len(t._last) <= 5
        assert len(t._last) == 5

    def test_a_flood_of_distinct_ips_evicts_the_oldest_warned_first(self, clock):
        t = workers._ValidityWarnThrottle(every=1000, max_peers=4)
        for i, peer in enumerate("abcd"):
            clock.frozen = 1000.0 + i
            assert t.allow(peer) is True
        clock.frozen = 1010.0
        assert t.allow("e") is True                                # evicts "a"
        assert [t.allow(p) for p in "bcde"] == [False] * 4         # the rest are remembered
        assert t.allow("a") is True                                # "a" was forgotten

    def test_eviction_is_by_last_warned_not_by_last_seen(self, clock):
        t = workers._ValidityWarnThrottle(every=1000, max_peers=2)
        assert t.allow("a") is True
        clock.frozen += 1
        assert t.allow("b") is True
        clock.frozen += 1
        assert t.allow("a") is False                               # "a" seen again: still the oldest warned
        assert t.allow("c") is True                                # so "a" goes, not "b"
        assert t.allow("b") is False and t.allow("c") is False
        assert t.allow("a") is True

    def test_warning_again_makes_a_peer_the_newest(self, clock):
        t = workers._ValidityWarnThrottle(every=10, max_peers=2)
        assert t.allow("a") is True
        clock.frozen += 1
        assert t.allow("b") is True
        clock.frozen += 10
        assert t.allow("a") is True                                # warned again: now the newest
        assert t.allow("c") is True                                # evicts "b"
        assert t.allow("a") is False and t.allow("c") is False
        assert t.allow("b") is True

    def test_a_cap_of_one_remembers_only_the_last_peer(self, clock):
        t = workers._ValidityWarnThrottle(every=100, max_peers=1)
        assert t.allow("a") is True and t.allow("a") is False
        assert t.allow("b") is True
        assert t.allow("a") is True                                # forgotten

    def test_a_forgotten_peer_is_warned_again_even_within_the_interval(self, clock):
        """The price of the bound: past the cap the oldest entries lose their throttle."""
        t = workers._ValidityWarnThrottle(every=1000, max_peers=2)
        for p in "abc":
            assert t.allow(p) is True
        assert t.allow("a") is True

    def test_exactly_one_of_many_simultaneous_attempts_is_allowed(self):
        t = workers._ValidityWarnThrottle(every=3600, max_peers=8)
        barrier = threading.Barrier(32)
        results = []

        def attempt():
            barrier.wait(10)
            results.append(t.allow("203.0.113.7"))

        threads = [threading.Thread(target=attempt) for _ in range(32)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(10)
        assert not any(th.is_alive() for th in threads)
        assert sorted(results) == [False] * 31 + [True]

    def test_threads_hammering_distinct_peers_keep_the_bound_and_never_raise(self):
        """The bound is checked under the throttle's own lock: inside `allow()` the map
        briefly holds one entry more than the cap (insert, then evict), so an unlocked
        read from another thread can legitimately see cap + 1."""
        t = workers._ValidityWarnThrottle(every=3600, max_peers=16)
        errors = []
        allowed = []

        def hammer(seed):
            try:
                for i in range(400):
                    if t.allow(f"p{(seed * 7 + i) % 90}"):
                        allowed.append(1)
                    with t._guard:
                        assert len(t._last) <= 16
            except BaseException as exc:                      # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=hammer, args=(n,)) for n in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(30)
        assert not any(th.is_alive() for th in threads) and errors == []
        assert len(t._last) == 16 and allowed

    def test_it_has_a_lock(self):
        t = workers._ValidityWarnThrottle()
        assert hasattr(t._guard, "acquire") and hasattr(t._guard, "release")


@pytest.mark.unit
class TestStoreWarningDedupe:
    """`api.dedupe_store_warnings` / `_RepeatFilter`: the trust poll opens the
    database every two seconds, so the store's preflight WARNING must not repeat
    every time. Installed by the API worker only."""

    @pytest.fixture(autouse=True)
    def _no_filter_leaks(self):
        """K-5: the product's own uninstall handle is what keeps the process-wide
        loggers clean; nothing here (or in conftest) strips filters behind its
        back. A test that leaves a `_RepeatFilter` behind fails right here."""
        assert _repeat_filters() == {}, "a _RepeatFilter was already installed"
        yield
        assert _repeat_filters() == {}, "the test left a _RepeatFilter installed"

    def _record(self, text, level=logging.WARNING):
        return logging.LogRecord("zfsbackup.config.store.paths", level, __file__, 1, text, None, None)

    def test_the_same_warning_passes_once_per_interval(self, mocker):
        clock = [1000.0]
        mocker.patch.object(api.time, "monotonic", lambda: clock[0])
        flt = api._RepeatFilter(interval=3600.0)
        assert flt.filter(self._record("group mismatch on x")) is True
        assert flt.filter(self._record("group mismatch on x")) is False
        clock[0] += 3599
        assert flt.filter(self._record("group mismatch on x")) is False
        clock[0] += 2
        assert flt.filter(self._record("group mismatch on x")) is True        # an hour later

    def test_different_messages_are_independent(self):
        flt = api._RepeatFilter()
        assert flt.filter(self._record("a")) is True
        assert flt.filter(self._record("b")) is True
        assert flt.filter(self._record("a")) is False and flt.filter(self._record("b")) is False

    def test_errors_always_pass(self):
        flt = api._RepeatFilter()
        for _ in range(3):
            assert flt.filter(self._record("boom", logging.ERROR)) is True

    def test_the_default_interval_is_one_hour(self):
        assert api._RepeatFilter()._interval == 3600.0

    def test_dedupe_installs_one_filter_on_each_store_logger_and_is_idempotent(self):
        first = api.dedupe_store_warnings()
        second = api.dedupe_store_warnings()
        try:
            for name in api._STORE_LOGGERS:
                assert len(_repeat_filters()[name]) == 1
            assert set(api._STORE_LOGGERS) == {
                "zfsbackup.config.store.paths", "zfsbackup.config.store.db"
            }
        finally:
            second()
            first()

    def test_the_installed_filter_really_drops_the_repeat(self, caplog):
        uninstall = api.dedupe_store_warnings()
        lg = logging.getLogger("zfsbackup.config.store.paths")
        seen = []

        class Catch(logging.Handler):
            def emit(self, record):
                seen.append(record.getMessage())

        handler = Catch()
        lg.addHandler(handler)
        try:
            for _ in range(5):
                lg.warning("sidecar group differs")
        finally:
            lg.removeHandler(handler)
            uninstall()
        assert seen == ["sidecar group differs"]

    # -- K-5: the uninstall handle ------------------------------------------------

    def test_the_handle_removes_exactly_what_it_installed(self):
        uninstall = api.dedupe_store_warnings()
        assert set(_repeat_filters()) == set(api._STORE_LOGGERS)
        uninstall()
        assert _repeat_filters() == {}

    def test_the_handle_leaves_other_filters_alone(self):
        class Other(logging.Filter):
            pass

        others = {name: Other() for name in api._STORE_LOGGERS}
        for name, flt in others.items():
            logging.getLogger(name).addFilter(flt)
        try:
            uninstall = api.dedupe_store_warnings()
            uninstall()
            for name, flt in others.items():
                assert flt in logging.getLogger(name).filters
        finally:
            for name, flt in others.items():
                logging.getLogger(name).removeFilter(flt)

    def test_the_handle_is_idempotent(self):
        uninstall = api.dedupe_store_warnings()
        uninstall()
        uninstall()
        uninstall()
        assert _repeat_filters() == {}

    def test_a_stale_handle_does_not_remove_a_later_installs_filters(self):
        stale = api.dedupe_store_warnings()
        stale()
        fresh = api.dedupe_store_warnings()
        try:
            stale()                                              # second call: a no-op
            assert set(_repeat_filters()) == set(api._STORE_LOGGERS)
        finally:
            fresh()

    def test_a_second_install_while_installed_returns_a_no_op_handle(self):
        first = api.dedupe_store_warnings()
        installed = _repeat_filters()
        second = api.dedupe_store_warnings()
        try:
            assert _repeat_filters() == installed                # same objects, no new ones
            second()                                             # no-op: the first install stays
            assert _repeat_filters() == installed
        finally:
            first()
        assert _repeat_filters() == {}

    def test_the_outer_handle_may_go_first_and_the_inner_one_stays_harmless(self):
        first = api.dedupe_store_warnings()
        second = api.dedupe_store_warnings()
        first()
        assert _repeat_filters() == {}
        second()                                                 # nothing left to remove
        assert _repeat_filters() == {}

    def test_a_logger_already_carrying_a_filter_is_not_taken_over(self):
        """Only the call that installed a filter can remove it: a filter already on
        one logger stays when the handle goes, the other logger's is removed."""
        paths, db = (logging.getLogger(n) for n in api._STORE_LOGGERS)
        mine = api._RepeatFilter()
        paths.addFilter(mine)
        try:
            uninstall = api.dedupe_store_warnings()
            uninstall()
            assert mine in paths.filters
            assert not any(isinstance(f, api._RepeatFilter) for f in db.filters)
        finally:
            paths.removeFilter(mine)

    # -- K-5: ApiWorker.run installs it for the run and removes it on every exit ----

    def _worker(self, config_db, **config):
        stop = StopFlag()
        stop.set()
        return workers.ApiWorker(
            BackupConfig(datasets=[DatasetConfig(name="pool/data")], **config),
            stop, config_db=config_db,
        )

    def test_the_api_worker_installs_it_for_the_run_and_removes_it_after(
        self, tls_config_db, mocker
    ):
        during = []

        def bind(*args, **kwargs):
            during.append(_repeat_filters())
            return mocker.MagicMock()

        mocker.patch("zfsbackup.api.create_app")
        mocker.patch("zfsbackup.workers._bind_api_server", side_effect=bind)
        mocker.patch("zfsbackup.workers.threading.Thread")
        self._worker(tls_config_db, dry_run=True).run()
        (seen,) = during
        assert set(seen) == set(api._STORE_LOGGERS)
        assert all(len(v) == 1 for v in seen.values())
        assert _repeat_filters() == {}

    def test_a_bind_failure_exits_one_and_leaves_no_filter(self, tls_config_db, mocker, caplog):
        during = []

        def bind(*args, **kwargs):
            during.append(_repeat_filters())
            raise OSError(errno.EADDRINUSE, "Address already in use")

        mocker.patch("zfsbackup.api.create_app")
        mocker.patch("zfsbackup.workers._bind_api_server", side_effect=bind)
        with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as info:
            self._worker(tls_config_db).run()
        assert info.value.code == 1 and "cannot listen on" in caplog.text
        assert set(during[0]) == set(api._STORE_LOGGERS)         # it WAS installed
        assert _repeat_filters() == {}

    def test_a_server_identity_failure_exits_one_and_leaves_no_filter(
        self, sqlite_config_db, mocker, caplog
    ):
        """No server identity outside a dry run: `IdentityError`, exit 1."""
        during = []
        real = workers._server_identity

        def spy(*args, **kwargs):
            during.append(_repeat_filters())
            return real(*args, **kwargs)

        mocker.patch("zfsbackup.workers._server_identity", side_effect=spy)
        with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as info:
            self._worker(sqlite_config_db).run()
        assert info.value.code == 1 and "cannot load the server TLS identity" in caplog.text
        assert set(during[0]) == set(api._STORE_LOGGERS)
        assert _repeat_filters() == {}

    def test_a_gateway_setup_failure_exits_one_and_leaves_no_filter(
        self, tls_config_db, mocker, caplog
    ):
        mocker.patch("zfsbackup.api.TrustGateway", side_effect=RuntimeError("boom"))
        with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as info:
            self._worker(tls_config_db).run()
        assert info.value.code == 1 and "client trust: RuntimeError: boom" in caplog.text
        assert _repeat_filters() == {}

    def test_an_unexpected_error_in_the_run_still_removes_the_filter(self, tls_config_db, mocker):
        mocker.patch("zfsbackup.api.create_app", side_effect=RuntimeError("app"))
        with pytest.raises(RuntimeError, match="app"):
            self._worker(tls_config_db).run()
        assert _repeat_filters() == {}

    def test_a_worker_without_a_config_database_installs_nothing(self, mocker):
        install = mocker.patch("zfsbackup.api.dedupe_store_warnings")
        with pytest.raises(SystemExit):
            workers.ApiWorker(BackupConfig(datasets=[DatasetConfig(name="pool/data")]),
                              StopFlag()).run()
        install.assert_not_called()

    def test_a_filter_installed_before_the_run_survives_it(self, tls_config_db, mocker):
        mine = api.dedupe_store_warnings()
        installed = _repeat_filters()
        try:
            mocker.patch("zfsbackup.api.create_app")
            mocker.patch("zfsbackup.workers._bind_api_server")
            mocker.patch("zfsbackup.workers.threading.Thread")
            self._worker(tls_config_db, dry_run=True).run()
            assert _repeat_filters() == installed
        finally:
            mine()

    def test_a_later_warning_is_not_swallowed_after_a_run(self, tls_config_db, mocker, caplog):
        """The reason for K-5: a worker that ran inline must not make a later test (or
        anything else in the process) lose a repeated store WARNING."""
        mocker.patch("zfsbackup.api.create_app")
        mocker.patch("zfsbackup.workers._bind_api_server")
        mocker.patch("zfsbackup.workers.threading.Thread")
        self._worker(tls_config_db, dry_run=True).run()
        lg = logging.getLogger("zfsbackup.config.store.paths")
        with caplog.at_level(logging.WARNING, logger=lg.name):
            for _ in range(3):
                lg.warning("same text every time")
        assert [r.getMessage() for r in caplog.records].count("same text every time") == 3

    @pytest.mark.parametrize("cls", [workers.SnapshotWorker, workers.PruningWorker,
                                     workers.RemoteBackupWorker])
    def test_no_other_worker_installs_it(self, cls, worker_config, mocker, monkeypatch, tmp_path):
        monkeypatch.setenv("HOME", str(tmp_path))            # (the legacy remote identity is lazy)
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        mocker.patch("zfsbackup.backup_manager.zfs.set")
        stop = StopFlag()
        stop.set()
        cls(worker_config, stop).run()
        assert _repeat_filters() == {}


# ===========================================================================
# A real daemon
# ===========================================================================


@pytest.mark.integration
@pytest.mark.slow
class TestRealDaemonTls:
    """A real `python -m zfsbackup.daemon`: the supervisor creates `<db>.tls/`
    at startup, the API answers HTTPS (TLS 1.3; `/health` needs no client
    certificate) and a `--dry-run` daemon creates nothing durable."""

    @pytest.fixture
    def daemon(self, tmp_path, monkeypatch):
        from test_zfsbackup_reload import RealDaemon

        d = RealDaemon(tmp_path, monkeypatch)
        (tmp_path / "tmp").mkdir()
        d.env["TMPDIR"] = str(tmp_path / "tmp")               # where a dry run's throwaway goes
        d.env["HOME"] = str(tmp_path / "home")
        (tmp_path / "home").mkdir()
        api_port = None
        for line in d.yaml_path.read_text().splitlines():
            if line.startswith("api_port:"):
                api_port = int(line.split(":")[1])
        d.api_port = api_port
        yield d
        d.stop()

    def _stop(self, d):
        os.kill(d.supervisor, signal.SIGTERM)
        assert d.proc.wait(timeout=60) == 0, d.log()

    def test_startup_creates_the_server_identity_and_serves_https(self, daemon):
        d = daemon
        tls = identity.tls_dir(d.db)
        d.import_config()
        assert not tls.exists()                                # the CLI's import creates none
        d.start()
        d.wait_for_regex(r"API listening on 127\.0\.0\.1:\d+ \(HTTPS", 60)
        assert (tls / "server.crt").is_file() and (tls / "server.key").is_file()
        assert ((tls / "server.key").stat().st_mode & 0o777) == 0o600
        fingerprint = identity.peek_identity(tls, "server").fingerprint
        assert f"fingerprint {fingerprint}" in d.log()         # logged at INFO
        status, body = https_get(d.api_port, "/health")
        assert (status, json.loads(body)) == (200, {"status": "ok"})
        with socket.create_connection((LOOPBACK, d.api_port), timeout=IO_TIMEOUT) as raw:
            with insecure_client_context().wrap_socket(raw) as tls_sock:
                assert identity.fingerprint_der(tls_sock.getpeercert(binary_form=True)) == fingerprint
        self._stop(d)

    def test_a_restart_keeps_the_identity(self, daemon):
        d = daemon
        tls = identity.tls_dir(d.db)
        d.import_config()
        d.start()
        d.wait_for_regex(r"API listening", 60)
        first = identity.peek_identity(tls, "server").fingerprint
        self._stop(d)
        d.start()
        d.wait_for_regex(r"API listening", 60)
        assert identity.peek_identity(tls, "server").fingerprint == first
        assert https_get(d.api_port, "/health")[0] == 200
        self._stop(d)

    def test_a_dry_run_creates_no_tls_directory_and_serves_a_throwaway(self, daemon, tmp_path):
        d = daemon
        tls = identity.tls_dir(d.db)
        d.import_config()
        d.start("--dry-run")
        d.wait_for_regex(r"API listening on 127\.0\.0\.1:\d+ \(HTTPS", 60)
        assert not tls.exists()
        assert https_get(d.api_port, "/health")[0] == 200
        assert "throwaway" in d.log()
        assert len([p for p in (tmp_path / "tmp").iterdir()
                    if p.name.startswith(workers._DRY_RUN_PREFIX)]) == 1
        self._stop(d)
        assert not tls.exists()
        assert [p for p in (tmp_path / "tmp").iterdir()
                if p.name.startswith(workers._DRY_RUN_PREFIX)] == []     # cleaned up

    def test_a_dry_run_with_an_existing_identity_uses_it_and_changes_nothing(self, daemon):
        d = daemon
        tls = identity.tls_dir(d.db)
        d.import_config()
        d.start()
        d.wait_for_regex(r"API listening", 60)
        self._stop(d)
        files = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tls.iterdir()}
        fingerprint = identity.peek_identity(tls, "server").fingerprint
        d.start("--dry-run")
        d.wait_for_regex(r"API listening", 60)
        with socket.create_connection((LOOPBACK, d.api_port), timeout=IO_TIMEOUT) as raw:
            with insecure_client_context().wrap_socket(raw) as tls_sock:
                assert identity.fingerprint_der(tls_sock.getpeercert(binary_form=True)) == fingerprint
        self._stop(d)
        assert files == {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tls.iterdir()}


# ===========================================================================
# R1: the per-dataset claim is released before the final `ok`, over real TLS
# ===========================================================================


@pytest.mark.integration
class TestStreamClaimReleasedBeforeOkOverTls:
    """The real `/stream` route (flask-sock over the per-connection TLS server, the client
    authenticated by its pinned certificate), with only `zfs` faked: a client that opens its
    next stream the moment it reads `ok` must not be told `busy`.

    The server's tail after `ok` (back in `_receive_stream`: its `finally` and the gate) is
    stretched to `ACK_TAIL` seconds by delaying the return of the `ok` send, so the second
    stream deterministically arrives while the first handler is still running. Before R1 that
    window was the whole difference; after it the claim is already free."""

    ACK_TAIL = 1.0
    SERVER_DS = "pool/backups/alice/pool/data"
    BUSY = {"error": "busy", "retry": True}

    @pytest.fixture
    def rig(self, pki, mocker):
        import types
        from unittest.mock import MagicMock, PropertyMock

        import simple_websocket

        from zfsbackup.backup_manager import DatasetManager
        from zfsbackup.config import RemoteServerConfig

        pki.pin_alice()
        mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
        mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch(
            "zfsbackup.api.zfs.get.value",
            return_value=api.canonical_dataset_config(DatasetConfig(name="pool/data").to_property()),
        )
        writers = []

        def new_writer(*args, **kwargs):
            writers.append(MagicMock())
            return writers[-1]

        mocker.patch("zfsbackup.api.zfs.receive.filesystem", side_effect=new_writer)
        ok_sent_at = []
        tail = self.ACK_TAIL
        real_send = simple_websocket.Server.send

        def slow_ok_send(self, data, *args, **kwargs):
            real_send(self, data, *args, **kwargs)
            if data == json.dumps({"status": "ok"}):
                ok_sent_at.append(time.monotonic())
                time.sleep(tail)           # the handler's tail, after the client has `ok`

        mocker.patch.object(simple_websocket.Server, "send", slow_ok_send)
        config = BackupConfig(
            datasets=[DatasetConfig(name="pool/data")],
            remote_backup=RemoteServerConfig(target_dataset="pool/backups", enabled=True),
        )
        app = api.create_app(config, pki.gateway, pki.server.fingerprint)
        return types.SimpleNamespace(
            app=app, gate=app.extensions["zfsbackup.stream_gate"], writers=writers,
            ok_sent_at=ok_sent_at, ctx=insecure_client_context(pki.alice),
        )

    @staticmethod
    def _open(port, ctx):
        import websocket

        from zfsbackup.protocol import protocol_headers

        raw = socket.create_connection((LOOPBACK, port), timeout=IO_TIMEOUT)
        tls = ctx.wrap_socket(raw)
        ws = websocket.WebSocket()
        ws.connect(
            f"wss://{LOOPBACK}:{port}/backup/alice/pool/data/stream?mode=fresh",
            socket=tls, timeout=IO_TIMEOUT, header=protocol_headers(),
        )
        return ws

    @classmethod
    def _stream(cls, port, ctx, chunks=3):
        """Open a stream, send `chunks` chunks and `done`; -> (ws, the first reply)."""
        ws = cls._open(port, ctx)
        for _ in range(chunks):
            ws.send_binary(b"x" * 4096)
        ws.send(json.dumps({"done": True}))
        return ws, json.loads(ws.recv())

    def test_the_next_stream_right_after_ok_is_served_not_busy(self, pki, rig):
        opened = []
        with Serving(rig.app, pki.gateway) as s:
            try:
                first, reply = self._stream(s.port, rig.ctx)
                opened.append(first)
                assert reply == {"status": "ok"}
                got_ok = time.monotonic()
                # ...at once, without waiting for the first handler to return:
                second, reply = self._stream(s.port, rig.ctx)
                opened.append(second)
                assert reply == {"status": "ok"}, reply            # not 'busy'
                assert time.monotonic() - got_ok < self.ACK_TAIL, (
                    "the second stream was not concurrent with the first handler's tail"
                )
                assert len(rig.writers) == 2
                for writer in rig.writers:
                    writer.close.assert_called_once()
            finally:
                for ws in opened:
                    with contextlib.suppress(Exception):
                        ws.close()
                assert _eventually(lambda: rig.gate.active == 0, timeout=30)
        assert len(rig.ok_sent_at) == 2

    def test_the_claim_is_held_while_a_stream_is_still_receiving(self, pki, rig):
        """The control: with the first stream still open (no `done` yet) the second one IS
        refused, so the test above passes because of the early release, not because the
        claims never collide."""
        opened = []
        with Serving(rig.app, pki.gateway) as s:
            try:
                first = self._open(s.port, rig.ctx)
                opened.append(first)
                first.send_binary(b"x" * 4096)
                assert _eventually(lambda: rig.gate.active == 1, timeout=20)
                second = self._open(s.port, rig.ctx)
                opened.append(second)
                second.send(json.dumps({"done": True}))
                assert json.loads(second.recv()) == self.BUSY
                first.send(json.dumps({"done": True}))
                assert json.loads(first.recv()) == {"status": "ok"}
                # once the first has been answered the name is free again
                third, reply = self._stream(s.port, rig.ctx)
                opened.append(third)
                assert reply == {"status": "ok"}
            finally:
                for ws in opened:
                    with contextlib.suppress(Exception):
                        ws.close()
                assert _eventually(lambda: rig.gate.active == 0, timeout=30)

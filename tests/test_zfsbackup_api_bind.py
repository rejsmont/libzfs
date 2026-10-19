"""`zfsbackup.workers._bind_api_server` / `_api_server_class`: the API server's bind.

Werkzeug's servers inherit `http.server.HTTPServer.server_bind`, which calls
`socket.getfqdn(host)` (a reverse DNS lookup). On a host with a slow resolver
that stalled every API worker start for ~30 s. `ApiServer` replaces it with
`socketserver.TCPServer.server_bind` plus `server_name = host` / `server_port`.

* in-process: no name lookup is ever made, the bookkeeping attributes are right,
  a real request through the bound server sees the right WSGI environ, the
  failure wording is unchanged, the class is cached and werkzeug stays lazy;
* one real daemon whose `getfqdn` / `gethostbyaddr` / `getnameinfo` sleep for 20 s
  (patched at module level of a driver script, so forked, forkserver and spawned
  children get the patch too) still reaches "API listening" in seconds.
"""

import http.client
import multiprocessing
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from unittest.mock import PropertyMock

import pytest

from _hermetic import hermetic_env
from test_zfsbackup_reload import ROOT, RealDaemon
from zfsbackup import workers
from zfsbackup.api import create_app
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import BackupConfig, DatasetConfig

LOOKUPS = ("getfqdn", "gethostbyaddr", "getnameinfo")


@pytest.fixture
def no_lookups(monkeypatch):
    """Any name lookup fails the test (recorded too, for calls on other threads)."""
    calls = []

    def make(name):
        def forbidden(*a, **k):
            calls.append(name)
            raise AssertionError(f"socket.{name}{a!r} was called: the bind must not do reverse DNS")

        return forbidden

    for name in LOOKUPS:
        monkeypatch.setattr(socket, name, make(name))
    yield calls
    assert calls == []


@pytest.fixture
def real_app(mocker):
    mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
    return create_app(BackupConfig(datasets=[DatasetConfig(name="pool/data")]))


class Serving:
    """A bound server served from a thread and shut down on exit."""

    def __init__(self, app, host="127.0.0.1", port=0):
        self.server = workers._bind_api_server(app, host, port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.thread.join(10)
        self.server.server_close()
        assert not self.thread.is_alive()

    def get(self, path, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=10)
        try:
            conn.request("GET", path, headers=headers or {})
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()


@pytest.mark.unit
class TestBindDoesNoNameLookup:
    def test_it_binds_and_sets_the_bookkeeping_without_any_lookup(self, no_lookups):
        server = workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", 0)
        try:
            port = server.socket.getsockname()[1]
            assert port > 0
            assert server.server_name == "127.0.0.1"
            assert server.server_port == port
            assert server.server_address == ("127.0.0.1", port)
        finally:
            server.server_close()

    def test_a_fixed_port_is_kept(self, no_lookups):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        server = workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", port)
        try:
            assert server.server_port == port and server.server_name == "127.0.0.1"
        finally:
            server.server_close()

    def test_a_hostname_is_kept_as_the_server_name_without_being_resolved_back(self, no_lookups):
        server = workers._bind_api_server(lambda e, s: iter(()), "localhost", 0)
        try:
            assert server.server_name == "localhost"
            assert server.server_port == server.socket.getsockname()[1]
        finally:
            server.server_close()

    @pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="no AF_UNIX")
    def test_a_unix_socket_address_sets_a_name_and_no_port(self, no_lookups):
        path_dir = tempfile.mkdtemp(prefix="zsn", dir="/tmp")
        path = os.path.join(path_dir, "api.sock")
        try:
            server = workers._bind_api_server(lambda e, s: iter(()), f"unix://{path}", 0)
            try:
                assert server.address_family == socket.AF_UNIX
                assert server.server_name == f"unix://{path}"
                assert server.server_address == path
            finally:
                server.server_close()
        finally:
            shutil.rmtree(path_dir, ignore_errors=True)

    def test_the_server_is_a_werkzeug_threaded_server(self):
        from werkzeug.serving import ThreadedWSGIServer

        server = workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", 0)
        try:
            assert isinstance(server, ThreadedWSGIServer)
            assert isinstance(server, workers._api_server_class())
            assert server.multithread is True
        finally:
            server.server_close()


@pytest.mark.unit
@pytest.mark.subprocess
class TestARealRequest:
    def test_health_is_200_and_the_environ_and_host_header_are_right(self, no_lookups, real_app):
        seen = []

        def wrapper(environ, start_response):
            seen.append(dict(environ))
            return real_app(environ, start_response)

        with Serving(wrapper) as s:
            port = s.server.server_port
            status, body = s.get("/health", {"Host": "custom.example:1234"})
            assert (status, body.strip()) == (200, b'{"status":"ok"}')
            status, _ = s.get("/health")
            assert status == 200
        first, second = seen
        assert first["SERVER_NAME"] == "127.0.0.1"
        assert first["SERVER_PORT"] == str(port)
        assert first["HTTP_HOST"] == "custom.example:1234"        # the client's Host is preserved
        assert second["HTTP_HOST"] == f"127.0.0.1:{port}"
        assert first["REMOTE_ADDR"] == "127.0.0.1"

    def test_the_real_app_answers_other_routes_through_it(self, no_lookups, real_app):
        with Serving(real_app) as s:
            status, body = s.get("/config")
            assert status == 200 and b"snapshot_prefix" in body
            assert s.get("/nope")[0] == 404

    def test_concurrent_requests_are_served(self, no_lookups, real_app):
        with Serving(real_app) as s:
            results = []
            threads = [
                threading.Thread(target=lambda: results.append(s.get("/health")[0]))
                for _ in range(8)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(15)
            assert results == [200] * 8


@pytest.mark.unit
class TestBindFailures:
    def test_an_occupied_port_raises_oserror_with_the_existing_wording(self, capsys):
        first = workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", 0)
        try:
            port = first.server_port
            with pytest.raises(OSError) as info:
                workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", port)
            assert str(info.value) == (
                f"cannot bind 127.0.0.1:{port} (address in use or not available)"
            )
            assert isinstance(info.value.__cause__, SystemExit)
        finally:
            first.server_close()

    def test_an_address_that_is_not_ours_raises_oserror(self):
        with pytest.raises(OSError, match=r"cannot bind 203\.0\.113\.77:0 "):
            workers._bind_api_server(lambda e, s: iter(()), "203.0.113.77", 0)

    def test_a_failed_bind_leaves_the_port_usable_by_the_next_attempt(self):
        first = workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", 0)
        port = first.server_port
        with pytest.raises(OSError):
            workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", port)
        first.server_close()
        again = workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", port)
        try:
            assert again.server_port == port
        finally:
            again.server_close()


@pytest.mark.unit
class TestClassCaching:
    def test_one_class_object_for_every_call(self):
        assert workers._api_server_class() is workers._api_server_class()
        a = workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", 0)
        b = workers._bind_api_server(lambda e, s: iter(()), "127.0.0.1", 0)
        try:
            assert type(a) is type(b) is workers._api_server_class()
        finally:
            a.server_close()
            b.server_close()

    def test_server_bind_is_overridden_not_inherited(self):
        from http.server import HTTPServer

        cls = workers._api_server_class()
        assert "server_bind" in vars(cls)
        assert cls.server_bind is not HTTPServer.server_bind

    def test_importing_the_workers_module_does_not_import_werkzeug(self):
        result = subprocess.run(
            [sys.executable, "-c",
             "import sys, zfsbackup.workers\n"
             "bad = sorted(m for m in sys.modules if m.split('.')[0] in ('werkzeug', 'flask'))\n"
             "assert not bad, bad\n"
             "zfsbackup.workers._api_server_class()\n"
             "assert 'werkzeug.serving' in sys.modules\n"],
            cwd=str(ROOT), env=hermetic_env(), capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr


# ---------------------------------------------------------------------------
# A real daemon on a host whose reverse DNS never answers
# ---------------------------------------------------------------------------

_DRIVER = '''\
import multiprocessing, os, socket, sys, time


def sleepy(*a, **k):
    with open(os.environ["DRV_MARK"], "a") as fh:      # proof the lookup was reached
        fh.write(f"{os.getpid()}\\n")
    time.sleep(20)
    return "x"


# Module level on purpose: forkserver/spawn children re-import this file as
# `__mp_main__`, so they get the same patch as the supervisor.
socket.getfqdn = sleepy
socket.gethostbyaddr = sleepy
socket.getnameinfo = sleepy

if __name__ == "__main__":
    multiprocessing.set_start_method(os.environ["DRV_METHOD"], force=True)
    sys.argv = ["zfsbackup.daemon"] + sys.argv[1:]
    from zfsbackup import daemon
    sys.exit(daemon.main())
'''


class SlowDnsDaemon(RealDaemon):
    def __init__(self, tmp_path, monkeypatch, method):
        super().__init__(tmp_path, monkeypatch)
        self.mark = tmp_path / "lookups.txt"
        self.driver = tmp_path / "driver_slow_dns.py"
        self.driver.write_text(_DRIVER)
        self.env["DRV_METHOD"] = method
        self.env["DRV_MARK"] = str(self.mark)
        # A script's sys.path[0] is its own directory, not the cwd: name the repo.
        self.env["PYTHONPATH"] = os.pathsep.join(
            [str(ROOT)] + ([self.env["PYTHONPATH"]] if self.env.get("PYTHONPATH") else [])
        )

    def start(self, *flags) -> None:
        self._log_fh = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            [sys.executable, str(self.driver), "-c", str(self.db), *flags],
            stdout=self._log_fh, stderr=subprocess.STDOUT, cwd=ROOT, env=self.env,
            start_new_session=True,
        )


@pytest.mark.integration
@pytest.mark.slow
class TestRealDaemonWithSlowReverseDns:
    API_WITHIN = 15.0      # the old behaviour stalls for 20 s on the first lookup

    @pytest.mark.parametrize("method", ["fork", "spawn", "forkserver"])
    def test_the_api_listens_within_seconds_and_no_lookup_is_made(
        self, tmp_path, monkeypatch, method
    ):
        if method not in multiprocessing.get_all_start_methods():
            pytest.skip(f"no {method} start method here")
        d = SlowDnsDaemon(tmp_path, monkeypatch, method)
        try:
            d.import_config()
            started = time.monotonic()
            d.start("--dry-run")
            d.wait_for_regex(r"API listening on 127\.0\.0\.1:\d+", self.API_WITHIN)
            elapsed = time.monotonic() - started
            assert elapsed < self.API_WITHIN, d.log()
            assert not d.mark.exists(), d.mark.read_text()
            os.kill(d.supervisor, signal.SIGTERM)
            assert d.proc.wait(timeout=60) == 0, d.log()
        finally:
            d.stop()

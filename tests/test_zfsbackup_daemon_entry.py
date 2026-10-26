"""Milestone C, item C2: `zfsbackupd --version` and the console-script entry.

The unit-level facts about `daemon_entry.main` (SIGHUP ignored before the
daemon runs, the status passes through, `signal` is the only module-level
import) live in `test_zfsbackup_version.py::TestDaemonEntry`. This module adds
what only a real process shows:

- ``TestVersionFlag``         `--version` prints `version_string('zfsbackupd')`
  and exits 0 before logging, config or a database; the same output from
  `python -m zfsbackup.daemon`, from `daemon_entry.main` and from a generated
  wrapper.
- ``TestSighupOrdering``      in a real interpreter SIGHUP is set to SIG_IGN
  before `zfsbackup.daemon` (or SQLAlchemy) is imported, and stays ignored on
  the way out.
- ``TestSpawnWithThreads``    `_spawn` under fork with extra threads logs one ERROR
  per spawn and still spawns; nothing under spawn/forkserver or single-threaded.
- ``TestNothingExits76``      the version-change exit status (S-8) is defined in
  `version.py`, returned by `BackupDaemon.run` alone, and named nowhere else.
- ``TestGeneratedWrapper``    an extension-less script of the exact shape pip
  generates for ``zfsbackupd = zfsbackup.daemon_entry:main``, under fork, spawn
  and forkserver: it reaches READY, turns over a reload, survives a storm of
  SIGHUPs from the moment `daemon_entry` ignored it until READY, and stops
  with status 0 and no orphan.

Everything runs under `hermetic_env()` (stub `zfs`, `--dry-run`, a private
config database); every daemon leads its own session and the whole group is
killed in a `finally`.

Mutation-checked (scratch copy of ``zfsbackup/``): deleting the
`signal.signal(SIGHUP, SIG_IGN)` line of `daemon_entry.main` fails 5 tests
(`TestSighupOrdering` x1 and `TestGeneratedWrapper::test_sighup_from_exec_to_ready`
x3 start methods, plus `test_the_wrapper_ignores_sighup_before_the_daemon_is_imported`).
Also: removing `_spawn`'s multi-thread check fails 3; building `version_string`
before parsing (not lazily) fails 3.
"""

import ast
import logging
import os
import re
import signal
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

import zfsbackup.daemon as daemon_module
from _hermetic import hermetic_env
from test_zfsbackup_reload import ROOT
from test_zfsbackup_sdnotify import (  # noqa: F401  (fixtures and helpers)
    NotifyDaemon,
    READY_WAIT,
    START_METHODS,
    Server,
    is_ready,
    lines,
    method_or_skip,
    process_group_members,
    sockdir,
    take,
)
from zfsbackup.config.store.migrate import EXPECTED_HEAD
from zfsbackup.protocol import PROTOCOL_VERSION
from zfsbackup.version import installed_version, version_string

PACKAGE = ROOT / "zfsbackup"
EXPECTED_VERSION_LINE = f"zfsbackupd {installed_version()} (wire protocol {PROTOCOL_VERSION}, config schema {EXPECTED_HEAD})"


def run_python(*args, env=None, timeout=120, **kw):
    return subprocess.run(
        [sys.executable, *args], cwd=ROOT, env=env or hermetic_env(PYTHONPATH=str(ROOT)),
        capture_output=True, text=True, timeout=timeout, **kw,
    )


# ---------------------------------------------------------------------------
# --version
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVersionFlag:
    def test_version_string_shape(self):
        assert version_string("zfsbackupd") == EXPECTED_VERSION_LINE
        assert re.fullmatch(
            r"zfsbackupd \S+ \(wire protocol \d+, config schema [0-9a-f]{12}\)", EXPECTED_VERSION_LINE
        )

    def test_main_prints_it_and_exits_zero(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["zfsbackupd", "--version"])
        with pytest.raises(SystemExit) as exc:
            daemon_module.main()
        assert exc.value.code == 0
        assert capsys.readouterr().out.strip() == EXPECTED_VERSION_LINE

    def test_it_runs_before_logging_and_config(self, monkeypatch, mocker, capsys):
        """No `configure`, no `_run`, no config path resolution, no handler on
        the root logger: `--version` must work on a host with no database."""
        configure = mocker.patch.object(daemon_module.logsetup, "configure")
        run = mocker.patch.object(daemon_module, "_run")
        resolve = mocker.patch.object(daemon_module, "resolve_config_path")
        import logging

        handlers = list(logging.getLogger().handlers)
        monkeypatch.setattr(sys, "argv", ["zfsbackupd", "--version", "-c", "/nonexistent/db"])
        with pytest.raises(SystemExit) as exc:
            daemon_module.main()
        assert exc.value.code == 0
        configure.assert_not_called()
        run.assert_not_called()
        resolve.assert_not_called()
        assert logging.getLogger().handlers == handlers
        assert capsys.readouterr().out.strip() == EXPECTED_VERSION_LINE

    def test_it_wins_over_the_other_flags(self, monkeypatch, mocker, capsys):
        run = mocker.patch.object(daemon_module, "_run")
        monkeypatch.setattr(sys, "argv", ["zfsbackupd", "-v", "--test-config", "--dry-run", "--version"])
        with pytest.raises(SystemExit) as exc:
            daemon_module.main()
        assert exc.value.code == 0
        run.assert_not_called()

    def test_the_line_is_built_only_when_the_flag_is_given(self, monkeypatch, mocker):
        """`version_string` imports the store package for the schema head; a
        normal start must not pay for it."""
        built = mocker.patch.object(daemon_module, "version_string", return_value="zfsbackupd test")
        mocker.patch.object(daemon_module.logsetup, "configure")
        mocker.patch.object(daemon_module, "_run", return_value=0)
        mocker.patch.object(daemon_module, "dispose_all")
        monkeypatch.setattr(sys, "argv", ["zfsbackupd", "-c", "x.db"])
        daemon_module.main()
        built.assert_not_called()

    def test_help_does_not_build_it_either(self, monkeypatch, mocker, capsys):
        built = mocker.patch.object(daemon_module, "version_string")
        monkeypatch.setattr(sys, "argv", ["zfsbackupd", "--help"])
        with pytest.raises(SystemExit) as exc:
            daemon_module.main()
        assert exc.value.code == 0
        built.assert_not_called()
        assert "--version" in capsys.readouterr().out

    def test_the_flag_builds_it_once_and_prints_it(self, monkeypatch, mocker, capsys):
        built = mocker.patch.object(daemon_module, "version_string", return_value="zfsbackupd test-line")
        monkeypatch.setattr(sys, "argv", ["zfsbackupd", "--version"])
        with pytest.raises(SystemExit) as exc:
            daemon_module.main()
        assert exc.value.code == 0
        built.assert_called_once_with("zfsbackupd")
        assert capsys.readouterr().out.strip() == "zfsbackupd test-line"

    def test_python_dash_m_prints_the_same_line(self, tmp_path):
        result = run_python("-m", "zfsbackup.daemon", "--version")
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == EXPECTED_VERSION_LINE
        assert result.stderr == ""

    def test_the_console_entry_prints_the_same_line(self):
        result = run_python(
            "-c",
            "import sys; from zfsbackup.daemon_entry import main; "
            "sys.argv = ['zfsbackupd', '--version']; sys.exit(main())",
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == EXPECTED_VERSION_LINE

    def test_it_touches_no_database(self, tmp_path):
        db = tmp_path / "never.db"
        result = run_python("-m", "zfsbackup.daemon", "-c", str(db), "--version")
        assert result.returncode == 0
        assert list(tmp_path.iterdir()) == []

    def test_help_lists_the_flags(self):
        out = run_python("-m", "zfsbackup.daemon", "--help").stdout
        for flag in ("--version", "--log-level", "--verbose", "--dry-run", "--test-config"):
            assert flag in out


# ---------------------------------------------------------------------------
# SIGHUP ordering in a real interpreter
# ---------------------------------------------------------------------------


_ORDERING_PROBE = textwrap.dedent(
    """
    import signal, sys
    real = signal.signal
    seen = []

    def spy(num, handler):
        if num == signal.SIGHUP and handler == signal.SIG_IGN:
            seen.append((
                'zfsbackup.daemon' in sys.modules,
                'sqlalchemy' in sys.modules,
                'libzfseasy' in sys.modules,
            ))
        return real(num, handler)

    signal.signal = spy
    assert signal.getsignal(signal.SIGHUP) == signal.SIG_DFL
    from zfsbackup import daemon_entry
    sys.argv = ['zfsbackupd', '--version']
    try:
        daemon_entry.main()
    except SystemExit as exc:
        status = exc.code
    print(seen, status, signal.getsignal(signal.SIGHUP) == signal.SIG_IGN)
    """
)


@pytest.mark.unit
class TestSighupOrdering:
    def test_sighup_is_ignored_before_anything_heavy_is_imported(self):
        result = run_python("-c", _ORDERING_PROBE)
        assert result.returncode == 0, result.stderr
        last = result.stdout.strip().splitlines()[-1]
        # The first SIG_IGN (daemon_entry's) came with nothing of the daemon
        # imported; the second is `_process_main`'s "on the way out"; the exit
        # status is 0 and the disposition is SIG_IGN at the end.
        assert last == "[(False, False, False), (True, True, True)] 0 True"

    def test_the_daemon_module_itself_does_not_ignore_sighup_when_imported(self):
        """Only `__main__` does (a console script never runs that guard): an
        importer, such as the test suite or the CLI, keeps its own disposition."""
        result = run_python(
            "-c",
            "import signal; import zfsbackup.daemon; "
            "print(signal.getsignal(signal.SIGHUP) == signal.SIG_DFL)",
        )
        assert result.stdout.strip() == "True", result.stderr


# ---------------------------------------------------------------------------
# Who may exit 76
# ---------------------------------------------------------------------------


def _python_files():
    return sorted(p for p in PACKAGE.rglob("*.py") if "__pycache__" not in p.parts)


#: The only modules that may name `EXIT_CODE_VERSION_CHANGED`: `version.py` defines it,
#: `daemon.py` returns it from `BackupDaemon.run` (S-8), and `workers.py` refers to it in prose.
SANCTIONED_76 = {"version.py", "daemon.py", "workers.py"}


def _names_76(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "EXIT_CODE_VERSION_CHANGED":
            yield node
        elif isinstance(node, ast.Attribute) and node.attr == "EXIT_CODE_VERSION_CHANGED":
            yield node
        elif isinstance(node, ast.alias) and node.name == "EXIT_CODE_VERSION_CHANGED":
            yield node


@pytest.mark.unit
class TestNothingExits76:
    """`EXIT_CODE_VERSION_CHANGED = 76` is the self-restart on upgrade (S-8) and is defined in
    `zfsbackup/version.py`. A unit's `RestartForceExitStatus=76` restarts the daemon on it, so
    an unlisted 76 would restart a daemon for no reason: only `BackupDaemon.run` may return it,
    and only after a drain for an upgrade."""

    def test_the_constant_is_referenced_only_where_s8_uses_it(self):
        offenders = []
        for path in _python_files():
            if path.parent == PACKAGE and path.name in SANCTIONED_76:
                continue
            for node in _names_76(ast.parse(path.read_text(), str(path))):
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
        assert offenders == []

    def test_only_run_and_the_startup_refusal_return_it(self):
        tree = ast.parse((PACKAGE / "daemon.py").read_text())
        returns = []
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(fn):
                if (
                    isinstance(node, ast.Return)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "EXIT_CODE_VERSION_CHANGED"
                ):
                    returns.append(fn.name)
        # `run` after a drain; `_run_startup` when the install is in flight before the first fork
        assert sorted(returns) == ["_run_startup", "run"]

    def test_workers_never_exit_with_it(self):
        """A worker leaves with `WORKER_EXIT_VERSION_SKEW`; 76 is the supervisor's alone."""
        tree = ast.parse((PACKAGE / "workers.py").read_text())
        code = [
            n for n in _names_76(tree)
            if not isinstance(n, ast.alias)
        ]
        assert code == [], "workers.py must not use the constant in code, only in prose"

    def test_the_worker_exit_status_is_defined_once_and_is_not_76(self):
        from zfsbackup import version, workers

        assert workers.WORKER_EXIT_VERSION_SKEW == 4 != version.EXIT_CODE_VERSION_CHANGED
        offenders = []
        for path in _python_files():
            for node in ast.walk(ast.parse(path.read_text(), str(path))):
                if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "WORKER_EXIT_VERSION_SKEW"
                    for t in node.targets
                ):
                    offenders.append(path.name)
        assert offenders == ["workers.py"]

    def test_no_literal_76_exit_status_in_the_package(self):
        offenders = []
        for path in _python_files():
            tree = ast.parse(path.read_text(), str(path))
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Constant) and node.value == 76 and type(node.value) is int):
                    continue
                if path.name == "version.py" and path.parent == PACKAGE:
                    continue
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
        assert offenders == []

    def test_the_constant_itself_is_defined_where_the_unit_will_look_for_it(self):
        from zfsbackup import version

        assert version.EXIT_CODE_VERSION_CHANGED == 76

    def test_no_exit_status_in_the_package_is_computed_from_the_constant(self):
        """`sys.exit(...)` / `os._exit(...)` / `SystemExit(...)` calls with a
        literal are all small and not 76 (guards the literal test above against
        an alias such as `code = 7 * 11 - 1`)."""
        for path in _python_files():
            for node in ast.walk(ast.parse(path.read_text(), str(path))):
                if not isinstance(node, ast.Call):
                    continue
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
                if name not in ("exit", "_exit", "SystemExit"):
                    continue
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, int):
                        assert arg.value != 76, f"{path}:{node.lineno}"


# ---------------------------------------------------------------------------
# Forking with other threads running
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSpawnWithThreads:
    """Under `fork`, a thread that holds a lock (a logging handler's, the stdout
    buffer's) at the instant of the fork leaves it held for ever in the child.
    The supervisor is single-threaded today; `_spawn` says so loudly if that
    stops being true (and still spawns: refusing would turn a latent hazard
    into an outage)."""

    @pytest.fixture
    def daemon(self, literal_config_db, mocker):
        import multiprocessing.process as mp_process

        from zfsbackup.daemon import BackupDaemon
        from zfsbackup.runtime_config import load_runtime_config

        snapshot = load_runtime_config(literal_config_db)
        daemon_module.dispose_all()
        snapshot.config.dry_run = True
        start = mocker.patch.object(mp_process.BaseProcess, "start")
        mocker.patch("zfsbackup.daemon.dispose_all")
        d = BackupDaemon(snapshot.config, literal_config_db)
        d.start_mock = start
        return d

    @pytest.fixture
    def extra_thread(self):
        release = threading.Event()
        t = threading.Thread(target=release.wait, name="stray-helper-thread", daemon=True)
        t.start()
        yield t
        release.set()
        t.join(5)

    def errors(self, caplog):
        return [r for r in caplog.records if r.levelno >= logging.ERROR and "forking worker" in r.getMessage()]

    def start_method(self, mocker, method):
        mocker.patch.object(daemon_module.multiprocessing, "get_start_method", return_value=method)

    def only_this_thread(self, mocker):
        mocker.patch.object(daemon_module.threading, "enumerate", return_value=[threading.current_thread()])

    def test_fork_with_another_thread_logs_one_error_and_still_spawns(
        self, daemon, extra_thread, caplog, mocker
    ):
        self.start_method(mocker, "fork")
        with caplog.at_level(logging.INFO, logger="zfsbackup.daemon"):
            daemon._spawn("snapshot")
        (rec,) = self.errors(caplog)
        assert rec.name == "zfsbackup.daemon" and rec.levelno == logging.ERROR
        assert "stray-helper-thread" in rec.getMessage() and "snapshot" in rec.getMessage()
        daemon.start_mock.assert_called_once()
        assert "snapshot" in daemon._workers

    def test_each_spawn_is_reported_separately(self, daemon, extra_thread, caplog, mocker):
        self.start_method(mocker, "fork")
        with caplog.at_level(logging.INFO, logger="zfsbackup.daemon"):
            daemon._spawn("snapshot")
            daemon._spawn("pruning")
        assert len(self.errors(caplog)) == 2
        assert daemon.start_mock.call_count == 2

    def test_fork_without_other_threads_logs_nothing(self, daemon, caplog, mocker):
        self.start_method(mocker, "fork")
        self.only_this_thread(mocker)
        with caplog.at_level(logging.INFO, logger="zfsbackup.daemon"):
            daemon._spawn("snapshot")
        assert self.errors(caplog) == []
        daemon.start_mock.assert_called_once()

    @pytest.mark.parametrize("method", ["spawn", "forkserver"])
    def test_other_start_methods_are_never_reported(self, daemon, extra_thread, caplog, mocker, method):
        self.start_method(mocker, method)
        with caplog.at_level(logging.INFO, logger="zfsbackup.daemon"):
            daemon._spawn("snapshot")
        assert self.errors(caplog) == []
        daemon.start_mock.assert_called_once()

    def test_the_error_comes_before_the_fork(self, daemon, extra_thread, caplog, mocker):
        order = []
        self.start_method(mocker, "fork")
        daemon.start_mock.side_effect = lambda *a, **k: order.append("start")
        real_error = daemon_module.logger.error
        mocker.patch.object(
            daemon_module.logger, "error",
            side_effect=lambda *a, **k: (order.append("error"), real_error(*a, **k))[1],
        )
        daemon._spawn("snapshot")
        assert order == ["error", "start"]


# ---------------------------------------------------------------------------
# The generated console script
# ---------------------------------------------------------------------------

#: The body pip/poetry generate for ``zfsbackupd = zfsbackup.daemon_entry:main``,
#: with two test-only additions that change nothing the daemon sees:
#: the start method is chosen under ``__main__`` (a real console script leaves it
#: to the interpreter default, which differs per platform and version), and
#: markers are written so the test knows when SIGHUP became ignored.
WRAPPER = '''\
#!{python}
# -*- coding: utf-8 -*-
import os
import re
import signal
import sys

_entered = os.environ.get("ZFSB_WRAPPER_ENTERED")
_ignored = os.environ.get("ZFSB_WRAPPER_IGNORED")
if _ignored and __name__ == "__main__":
    _real = signal.signal

    def _spy(num, handler):
        result = _real(num, handler)
        if num == signal.SIGHUP and handler == signal.SIG_IGN:
            open(_ignored, "a").close()
        return result

    signal.signal = _spy

from zfsbackup.daemon_entry import main

if __name__ == "__main__":
    import multiprocessing
    method = os.environ.get("ZFSB_WRAPPER_METHOD")
    if method:
        multiprocessing.set_start_method(method)
    if _entered:
        open(_entered, "a").close()
    sys.argv[0] = re.sub(r"(-script\\.pyw|\\.exe)?$", "", sys.argv[0])
    sys.exit(main())
'''


def write_wrapper(path: Path) -> Path:
    assert path.suffix == ""                      # extension-less, like a console script
    path.write_text(WRAPPER.format(python=sys.executable))
    path.chmod(0o755)
    return path


class WrapperDaemon(NotifyDaemon):
    """A `NotifyDaemon` started through the generated script instead of
    `python -m zfsbackup.daemon`."""

    def __init__(self, tmp_path, monkeypatch, *, notify_socket, method):
        super().__init__(tmp_path, monkeypatch, notify_socket=notify_socket, method=method)
        self.script = write_wrapper(tmp_path / "zfsbackupd")
        self.entered = tmp_path / "wrapper.entered"
        self.ignored = tmp_path / "wrapper.ignored"
        self.env.update(
            PYTHONPATH=str(ROOT),
            ZFSB_WRAPPER_METHOD=method,
            ZFSB_WRAPPER_ENTERED=str(self.entered),
            ZFSB_WRAPPER_IGNORED=str(self.ignored),
        )

    def start(self, *flags) -> None:
        self._log_fh = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            [sys.executable, str(self.script), "-c", str(self.db), *flags],
            stdout=self._log_fh, stderr=subprocess.STDOUT, cwd=ROOT, env=self.env,
            start_new_session=True,
        )

    def wait_marker(self, marker: Path, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if marker.exists():
                return True
            if self.proc.poll() is not None:
                return False
            time.sleep(0.002)
        return marker.exists()


class HupStorm:
    """SIGHUPs `pid` every few milliseconds until stopped."""

    def __init__(self, pid, interval=0.01):
        self.pid, self.interval = pid, interval
        self.sent = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            try:
                os.kill(self.pid, signal.SIGHUP)
            except ProcessLookupError:
                return
            self.sent += 1
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(5)


@pytest.fixture
def wrapper_daemon(tmp_path, monkeypatch, sockdir):
    made, servers = [], []

    def make(method):
        method_or_skip(method)
        server = Server(os.path.join(sockdir, "w.sock"))
        servers.append(server)
        d = WrapperDaemon(tmp_path, monkeypatch, notify_socket=server.address, method=method)
        d.import_config()
        made.append(d)
        return d, server

    yield make
    for d in made:
        d.stop()
    for s in servers:
        s.close()


@pytest.mark.integration
@pytest.mark.slow
class TestGeneratedWrapper:
    def test_the_script_is_extension_less(self, tmp_path):
        script = write_wrapper(tmp_path / "zfsbackupd")
        assert script.suffix == "" and script.read_text().startswith("#!")
        with pytest.raises(AssertionError):
            write_wrapper(tmp_path / "zfsbackupd.py")

    def test_version_through_the_script(self, tmp_path):
        script = write_wrapper(tmp_path / "zfsbackupd")
        result = run_python(str(script), "--version")
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == EXPECTED_VERSION_LINE

    @pytest.mark.parametrize("method", START_METHODS)
    def test_ready_reload_turnover_and_a_clean_stop(self, wrapper_daemon, method):
        d, srv = wrapper_daemon(method)
        d.start("--dry-run")

        got = take(srv, d, is_ready, READY_WAIT)
        assert got == ["READY=1\nSTATUS=generation 0"]
        d.wait_for_regex(r"Started \w+ worker \(pid=\d+\)", 10, count=3)

        # A committed change plus SIGHUP: reload, turnover of every worker.
        before = d.worker_pids()
        d.raw_update("snapshot_prefix = 'reloaded'")
        os.kill(d.supervisor, signal.SIGHUP)
        got = take(srv, d, is_ready)
        assert [lines(m)[0] for m in got] == ["RELOADING=1", "READY=1"], got
        assert lines(got[1])[1] == "STATUS=generation 1"
        d.wait_applied(1)
        d.wait_for_regex(r"Reloaded \w+ worker \(pid=\d+\)", 20, count=3)
        after = d.worker_pids()
        assert set(after) == set(before)
        assert all(after[name] != before[name] for name in before), (before, after)

        os.kill(d.supervisor, signal.SIGTERM)
        assert d.proc.wait(timeout=90) == 0, d.log()
        deadline = time.monotonic() + 15
        while process_group_members(d.supervisor) and time.monotonic() < deadline:
            time.sleep(0.2)
        assert process_group_members(d.supervisor) == [], "orphans left behind"

    @pytest.mark.parametrize("method", START_METHODS)
    def test_sighup_from_exec_to_ready(self, wrapper_daemon, method):
        """The window C2 closes: SIGHUP is SIG_IGN from `daemon_entry.main`'s
        first statement, while the daemon's few hundred milliseconds of imports
        run, and until the supervisor's own handler is installed. (The ~25 ms
        before `daemon_entry` runs cannot be closed from Python; the unit's
        `RestartForceExitStatus=SIGHUP` covers it, and the storm here starts
        only once `main` reports SIG_IGN.)"""
        d, srv = wrapper_daemon(method)
        d.start("--dry-run")

        assert d.wait_marker(d.entered, 60), d.log()
        # `ignored` is written by the spy right after the SIG_IGN call; if the
        # line is missing the storm is sent anyway and the process dies with it.
        d.wait_marker(d.ignored, 10)
        with HupStorm(d.supervisor) as storm:
            got = take(srv, d, is_ready, READY_WAIT)
        assert d.alive(), f"the daemon died of SIGHUP (exit {d.proc.poll()}):\n{d.log()}"
        assert d.ignored.exists(), "daemon_entry.main never set SIGHUP to SIG_IGN"
        assert storm.sent >= 5
        # The startup tick says nothing, however many SIGHUPs came before it.
        assert got == ["READY=1\nSTATUS=generation 0"]

        # And the daemon is still a normal daemon afterwards.
        os.kill(d.supervisor, signal.SIGHUP)
        got = take(srv, d, is_ready)
        assert [lines(m)[0] for m in got] == ["RELOADING=1", "READY=1"], got
        os.kill(d.supervisor, signal.SIGTERM)
        assert d.proc.wait(timeout=90) == 0, d.log()

    def test_the_wrapper_ignores_sighup_before_the_daemon_is_imported(self, wrapper_daemon):
        """A SIGHUP in the middle of the import is the whole point: prove the
        marker (written the instant after SIG_IGN) precedes the daemon's own
        startup by checking that the supervisor had not started anything yet."""
        d, srv = wrapper_daemon("fork")
        d.start("--dry-run")
        assert d.wait_marker(d.ignored, 60), d.log()
        assert "Started" not in d.log()
        os.kill(d.supervisor, signal.SIGHUP)
        take(srv, d, is_ready, READY_WAIT)
        assert d.alive()
        assert d.terminate() == 0

    @pytest.mark.parametrize("method", START_METHODS)
    def test_workers_log_through_logsetup_under_every_start_method(self, wrapper_daemon, method):
        """The children are fresh interpreters under spawn and forkserver: each
        must configure logging itself. A worker line proves it did."""
        d, srv = wrapper_daemon(method)
        d.start("--dry-run", "--log-level", "info")
        take(srv, d, is_ready, READY_WAIT)
        d.wait_for_regex(r"zfsbackup\.workers - INFO - (snapshot|pruning)-worker started", 20, count=2)
        assert d.terminate() == 0

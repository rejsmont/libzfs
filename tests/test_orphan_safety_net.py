"""The test-hygiene layer (`tests/_hermetic.py` and its hooks in `conftest.py`):

- ``TestOrphanSafetyNet``      the session-end net that kills and reports processes a test
                               left running, including children forked WITHOUT exec and
                               orphans adopted by a subreaper;
- ``TestSessionMarker``        a session always mints a fresh marker (only an xdist worker of
                               the controller that minted it shares it), and a nested session
                               never reuses the outer one;
- ``TestRealTierIsOptIn``      without `ZFSB_REAL_ZFS=1` every `real_zfs` test is skipped and
                               nothing is stubbed away from the real tier when it IS opted into
                               (logic tested with mocks; the real tier is never run here);
- ``TestHermeticEnv``          stubs, the private `PATH`, no mutation of `os.environ`;
- ``TestGuardUsesTheOriginalPath``  a fake `zfs` script put first on `PATH` is not "the real one".

The leaks are staged in nested pytest sessions (`_leak_probe.py`) started with
`nested_env()`, so the outer session is unaffected.
"""

import os
import re
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import _hermetic
from _hermetic import (
    REAL_TIER_REASON,
    apply_stub_policy,
    find_orphans,
    hermetic_env,
    is_real_zfs,
    nested_env,
    real_tier_enabled,
    register_fork_recorder,
    session_marker,
)

ROOT = Path(__file__).resolve().parent.parent
PROBE = Path(__file__).resolve().parent / "_leak_probe.py"

#: a process that leaves an orphan (its parent exits at once) carrying the environment
ORPHANER = "import os, time\nif os.fork() == 0:\n    time.sleep(120)\nos._exit(0)\n"

#: the same without exec: registers the recorder, then double-forks; NO marker in its environment
FORKER = (
    "import os, sys, time\n"
    "sys.path.insert(0, sys.argv[2])\n"
    "from _hermetic import register_fork_recorder\n"
    "register_fork_recorder(sys.argv[1])\n"
    "first = os.fork()\n"
    "if first == 0:\n"
    "    second = os.fork()\n"
    "    if second == 0:\n"
    "        null = os.open(os.devnull, os.O_RDWR)\n"
    "        [os.dup2(null, fd) for fd in (0, 1, 2)]   # do not hold the test's pipes open\n"
    "        time.sleep(120)\n"
    "    os._exit(0)\n"
    "os.waitpid(first, 0)\n"
    "print('ready', flush=True)\n"
)


def nested(**extra):
    """A nested pytest session over the leak probe, with its own marker."""
    return subprocess.run(
        [sys.executable, "-m", "pytest", str(PROBE), "-p", "no:cacheprovider", "--no-cov",
         "-o", "addopts=", "-q"],
        cwd=str(ROOT), env=nested_env(**extra), capture_output=True, text=True, timeout=180,
    )


def pytest_with_conftest(test_file, *args, env=None, timeout=180):
    """A nested pytest session over a file OUTSIDE tests/ that still loads this
    repo's conftest (as a plugin), so its hooks and fixtures are really exercised."""
    env = dict(env if env is not None else nested_env())
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT / "tests"), env.get("PYTHONPATH", "")]))
    return subprocess.run(
        [sys.executable, "-m", "pytest", str(test_file), "-p", "conftest", "-p", "no:cacheprovider",
         "--no-cov", "-o", "addopts=", "-q", "--rootdir", str(test_file.parent), *args],
        cwd=str(test_file.parent), env=env, capture_output=True, text=True, timeout=timeout,
    )


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout
    return bool(state.strip()) and not state.strip().startswith("Z")


def kill_all(pids):
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def leaked_pids(output):
    return [int(m) for m in re.findall(r"^  pid (\d+) ", output, re.M)]


@pytest.mark.integration
class TestOrphanSafetyNet:
    @pytest.mark.slow
    def test_find_orphans_sees_a_reparented_process_by_its_marker(self):
        marker = uuid.uuid4().hex
        subprocess.run(
            [sys.executable, "-c", ORPHANER], check=True, timeout=30,
            env=dict(os.environ, ZFSB_TEST_SESSION=marker),
        )
        found = []
        try:
            found = find_orphans(marker, grace=5)
            assert len(found) == 1
            pid, ppid, state, text = found[0]
            assert ppid != os.getpid()                    # not our descendant (init OR a subreaper)
            assert f"ZFSB_TEST_SESSION={marker}" in text
            assert find_orphans(uuid.uuid4().hex, grace=0) == []   # another session's marker: not ours
        finally:
            kill_all([row[0] for row in found])

    def test_a_process_with_a_live_parent_is_not_an_orphan(self):
        marker = uuid.uuid4().hex
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            env=dict(os.environ, ZFSB_TEST_SESSION=marker),
        )
        try:
            time.sleep(0.5)
            assert find_orphans(marker, grace=0) == []
        finally:
            child.kill()
            child.wait()

    def test_orphans_are_judged_by_descent_not_by_ppid_one(self):
        """Not `ppid == 1`: anything that is not a live descendant of the pytest process
        counts -- systemd --user, tini and docker --init adopt orphans too, and a worker
        left under a dead supervisor's live parent is just as leaked. Staged by asking the
        question from a root that is NOT this process: the holder and its child are then
        outside that root's tree although neither has ppid 1."""
        marker = uuid.uuid4().hex
        script = (
            "import subprocess, sys, time\n"
            "kid = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])\n"
            "print(kid.pid, flush=True)\n"
            "time.sleep(120)\n"
        )
        holder = subprocess.Popen(
            [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True,
            env=dict(os.environ, ZFSB_TEST_SESSION=marker), start_new_session=True,
        )
        try:
            kid = int(holder.stdout.readline())
            elsewhere = find_orphans(marker, grace=0, root_pid=os.getpid() + 100000)
            by_pid = {row[0]: row for row in elsewhere}
            assert set(by_pid) == {holder.pid, kid}
            assert by_pid[kid][1] == holder.pid != 1              # adopted by a live process, not init
            # from OUR point of view both are live descendants: not leaks
            assert find_orphans(marker, grace=0) == []
        finally:
            os.killpg(holder.pid, signal.SIGKILL)
            holder.wait(timeout=10)

    @pytest.mark.slow
    def test_a_child_forked_without_exec_is_found_through_the_fork_record(self, tmp_path):
        """The marker lives in the environment a process was EXEC'd with; a child that
        was only forked has pytest's exec-time one, so the environment scan cannot see
        it. The at-fork record can."""
        records = tmp_path / "forks"
        records.mkdir()
        marker = uuid.uuid4().hex
        env = {k: v for k, v in os.environ.items() if k != "ZFSB_TEST_SESSION"}   # NO marker
        done = subprocess.run(
            [sys.executable, "-c", FORKER, str(records), str(Path(__file__).resolve().parent)],
            capture_output=True, text=True, timeout=60, env=env,
        )
        assert "ready" in done.stdout, done.stderr
        found = []
        try:
            assert find_orphans(marker, grace=0) == []                         # the env scan alone: blind
            found = find_orphans(marker, grace=5, fork_dir=str(records))
            assert len(found) == 1, found                                      # the grandchild
            assert alive(found[0][0])
        finally:
            kill_all([row[0] for row in found])

    def test_a_recorded_pid_that_has_died_is_not_reported(self, tmp_path):
        records = tmp_path / "forks"
        records.mkdir()
        gone = subprocess.Popen([sys.executable, "-c", "pass"])
        gone.wait()
        (records / str(gone.pid)).write_text("")
        assert find_orphans(uuid.uuid4().hex, grace=0, fork_dir=str(records)) == []

    def test_a_recycled_pid_is_not_taken_for_ours(self, tmp_path):
        """A record older than the process that now has that pid: the pid was recycled."""
        records = tmp_path / "forks"
        records.mkdir()
        stranger = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
        elsewhere = os.getpid() + 100000          # a root the stranger does not descend from
        try:
            time.sleep(1.2)
            record = records / str(stranger.pid)
            record.write_text("")
            old = time.time() - 600
            os.utime(record, (old, old))                       # "recorded" 10 minutes ago
            assert find_orphans(uuid.uuid4().hex, grace=0, fork_dir=str(records), root_pid=elsewhere) == []
            os.utime(record, None)                              # recorded now, after it started
            found = find_orphans(uuid.uuid4().hex, grace=0, fork_dir=str(records), root_pid=elsewhere)
            assert stranger.pid in [row[0] for row in found]
        finally:
            stranger.kill()
            stranger.wait()

    def test_the_recorder_writes_one_file_per_forked_child(self, tmp_path):
        records = tmp_path / "forks"
        records.mkdir()
        done = subprocess.run(
            [sys.executable, "-c",
             "import os, sys\n"
             f"sys.path.insert(0, {str(Path(__file__).resolve().parent)!r})\n"
             "from _hermetic import register_fork_recorder\n"
             f"register_fork_recorder({str(records)!r})\n"
             "pid = os.fork()\n"
             "if pid == 0:\n"
             "    os._exit(0)\n"
             "os.waitpid(pid, 0)\n"
             "print(pid)\n"],
            capture_output=True, text=True, timeout=60,
        )
        assert done.returncode == 0, done.stderr
        assert sorted(p.name for p in records.iterdir()) == [done.stdout.strip()]

    @pytest.mark.slow
    @pytest.mark.parametrize("probe", ["exec", "fork"])
    def test_a_leaking_session_is_killed_reported_and_failed(self, probe):
        result = nested(LEAK_PROBE=probe)
        out = result.stdout + result.stderr
        pids = leaked_pids(out)
        try:
            assert result.returncode == 1, out
            assert "LEAKED PROCESSES: 1 process(es)" in out
            assert "killed" in out and "start_new_session=True" in out
            assert len(pids) == 1
            time.sleep(0.5)
            assert not alive(pids[0])                           # the net killed it
        finally:
            kill_all(pids)

    @pytest.mark.slow
    @pytest.mark.parametrize("probe", ["exec", "fork"])
    def test_allow_orphans_downgrades_the_failure_to_a_warning(self, probe):
        result = nested(LEAK_PROBE=probe, ZFSB_ALLOW_ORPHANS="1")
        out = result.stdout + result.stderr
        pids = leaked_pids(out)
        try:
            assert result.returncode == 0, out
            assert "LEAKED PROCESSES" in out
        finally:
            kill_all(pids)

    def test_a_clean_nested_session_reports_nothing(self, tmp_path):
        clean = tmp_path / "test_clean.py"
        clean.write_text("def test_nothing():\n    assert True\n")
        done = pytest_with_conftest(clean)
        assert done.returncode == 0, done.stdout + done.stderr
        assert "LEAKED PROCESSES" not in done.stdout + done.stderr

class TestSessionMarker:
    def test_a_fresh_marker_is_minted_and_exported(self):
        environ = {}
        marker = session_marker(environ, ppid=1234)
        assert re.fullmatch(r"[0-9a-f]{32}", marker)
        assert environ["ZFSB_TEST_SESSION"] == marker
        assert environ["ZFSB_TEST_SESSION_OWNER"] == str(os.getpid())

    def test_an_exported_marker_is_never_reused(self):
        environ = {"ZFSB_TEST_SESSION": "mine", "ZFSB_TEST_SESSION_OWNER": "1"}
        assert session_marker(environ, ppid=1) != "mine"

    def test_an_inherited_marker_without_xdist_is_never_reused(self):
        environ = {"ZFSB_TEST_SESSION": "outer", "ZFSB_TEST_SESSION_OWNER": "4321"}
        assert session_marker(environ, ppid=4321) != "outer"

    def test_a_worker_of_the_minting_controller_shares_it(self):
        environ = {
            "ZFSB_TEST_SESSION": "run", "ZFSB_TEST_SESSION_OWNER": "4321",
            "PYTEST_XDIST_WORKER": "gw0",
        }
        assert session_marker(environ, ppid=4321) == "run"
        assert environ["ZFSB_TEST_SESSION"] == "run"
        assert environ["ZFSB_TEST_SESSION_OWNER"] == "4321"      # unchanged: still the controller's

    def test_a_session_started_inside_a_worker_does_not_share_it(self):
        """The nested session inherits the xdist variables too, but its parent is the
        worker's test process, not the controller."""
        environ = {
            "ZFSB_TEST_SESSION": "run", "ZFSB_TEST_SESSION_OWNER": "4321",
            "PYTEST_XDIST_WORKER": "gw0",
        }
        marker = session_marker(environ, ppid=9999)
        assert marker != "run" and environ["ZFSB_TEST_SESSION"] == marker

    def test_two_fresh_markers_differ(self):
        assert session_marker({}, 1) != session_marker({}, 1)

    def test_the_running_session_has_its_own_marker(self):
        import conftest

        assert os.environ["ZFSB_TEST_SESSION"] == conftest.SESSION_MARKER

    def test_nested_env_carries_no_session_identity(self, monkeypatch):
        for name in ("ZFSB_TEST_SESSION", "ZFSB_TEST_SESSION_OWNER", "PYTEST_XDIST_WORKER",
                     "PYTEST_XDIST_TESTRUNUID", "ZFSB_ALLOW_ORPHANS"):
            monkeypatch.setenv(name, "x")
        env = nested_env(EXTRA="1")
        for name in ("ZFSB_TEST_SESSION", "ZFSB_TEST_SESSION_OWNER", "PYTEST_XDIST_WORKER",
                     "PYTEST_XDIST_TESTRUNUID", "ZFSB_ALLOW_ORPHANS"):
            assert name not in env
        assert env["EXTRA"] == "1"

    def test_a_nested_collect_only_session_does_not_hunt_the_outer_sessions_processes(self):
        """The cross-test flake: a nested session reusing the outer marker would
        SIGKILL processes a concurrent test of the OUTER session is watching."""
        marker = os.environ["ZFSB_TEST_SESSION"]
        root = int(os.environ["ZFSB_TEST_SESSION_OWNER"])        # the controller (or the plain process)
        before = {row[0] for row in find_orphans(marker, grace=0, root_pid=root)}
        victim = subprocess.Popen(
            [sys.executable, "-c", ORPHANER], env=dict(os.environ),   # carries the OUTER marker
        )
        victim.wait()
        found = []
        try:
            deadline = time.monotonic() + 10
            while not found and time.monotonic() < deadline:
                found = [
                    row for row in find_orphans(marker, grace=0, root_pid=root)
                    if row[0] not in before and "time.sleep(120)" in row[3]
                ]
                time.sleep(0.1)
            assert found, "the staged orphan should be visible to the outer session"
            done = subprocess.run(
                [sys.executable, "-m", "pytest", "tests/test_types.py", "--collect-only", "-q",
                 "--no-cov", "-p", "no:cacheprovider", "-o", "addopts="],
                cwd=str(ROOT), env=dict(os.environ), capture_output=True, text=True, timeout=120,
            )
            # NOT scrubbed on purpose: the nested session inherits the outer session's
            # marker and xdist variables, as test_marker_tiers' used to
            assert done.returncode == 0, done.stdout + done.stderr
            assert "LEAKED" not in done.stdout + done.stderr
            assert all(alive(row[0]) for row in found)               # and it killed nothing of ours
        finally:
            kill_all([row[0] for row in found])

class TestRealTierIsOptIn:
    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "On", " 1 "])
    def test_opt_in_values(self, value):
        assert real_tier_enabled({"ZFSB_REAL_ZFS": value}) is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "2", "enabled"])
    def test_everything_else_is_off(self, value):
        assert real_tier_enabled({"ZFSB_REAL_ZFS": value}) is False

    def test_unset_is_off(self):
        assert real_tier_enabled({}) is False

    def test_the_stub_policy_stubs_unset_binaries_when_not_opted_in(self):
        environ = {}
        apply_stub_policy(environ)
        for var in ("ZFS_CMD", "ZPOOL_CMD"):
            assert os.access(environ[var], os.X_OK)
            assert Path(environ[var]).read_text().startswith("#!/bin/sh")

    def test_the_stub_policy_keeps_values_that_are_set(self):
        environ = {"ZFS_CMD": "/x/zfs", "ZPOOL_CMD": "/x/zpool"}
        apply_stub_policy(environ)
        assert environ == {"ZFS_CMD": "/x/zfs", "ZPOOL_CMD": "/x/zpool"}

    def test_the_stub_policy_replaces_an_empty_value(self):
        environ = {"ZFS_CMD": "", "ZPOOL_CMD": "/x/zpool"}
        apply_stub_policy(environ)
        assert os.access(environ["ZFS_CMD"], os.X_OK) and environ["ZPOOL_CMD"] == "/x/zpool"

    def test_nothing_is_stubbed_when_opted_in(self):
        environ = {"ZFSB_REAL_ZFS": "1"}
        apply_stub_policy(environ)
        assert environ == {"ZFSB_REAL_ZFS": "1"}              # the real binaries stay reachable

    def test_the_collection_hook_skips_real_zfs_tests_without_the_opt_in(self, monkeypatch):
        import conftest

        monkeypatch.delenv("ZFSB_REAL_ZFS", raising=False)
        real, mocked = MagicMock(), MagicMock()
        real.get_closest_marker.side_effect = lambda name: object() if name in ("real_zfs",) else None
        mocked.get_closest_marker.side_effect = lambda name: object() if name == "unit" else None
        conftest.pytest_collection_modifyitems(MagicMock(), [real, mocked])
        added = [c.args[0] for c in real.add_marker.call_args_list]
        skips = [m for m in added if getattr(m, "name", "") == "skip"]
        assert len(skips) == 1 and skips[0].kwargs["reason"] == REAL_TIER_REASON
        assert "ZFSB_REAL_ZFS=1" in REAL_TIER_REASON
        assert [m for m in (c.args[0] for c in mocked.add_marker.call_args_list) if getattr(m, "name", "") == "skip"] == []

    def test_the_collection_hook_leaves_the_real_tier_alone_when_opted_in(self, monkeypatch):
        import conftest

        monkeypatch.setenv("ZFSB_REAL_ZFS", "1")
        real = MagicMock()
        real.get_closest_marker.side_effect = lambda name: object() if name == "real_zfs" else None
        conftest.pytest_collection_modifyitems(MagicMock(), [real])
        assert [m for m in (c.args[0] for c in real.add_marker.call_args_list) if getattr(m, "name", "") == "skip"] == []

    def test_a_real_zfs_item_is_not_also_marked_unit(self, monkeypatch):
        import conftest

        monkeypatch.delenv("ZFSB_REAL_ZFS", raising=False)
        real = MagicMock()
        real.get_closest_marker.side_effect = lambda name: object() if name == "real_zfs" else None
        conftest.pytest_collection_modifyitems(MagicMock(), [real])
        assert all(getattr(c.args[0], "name", "") != "unit" for c in real.add_marker.call_args_list)

    @pytest.mark.slow
    def test_a_real_zfs_run_without_the_opt_in_skips_everything(self, tmp_path):
        """Dry check of the switch: `pytest -m real_zfs` over the two real files with
        NO opt-in must skip every test and set up no pool. PATH holds only the
        interpreter's directory, so even a broken skip could not reach `sudo`,
        `truncate` or a `zfs` -- the nested session is harmless by construction."""
        env = nested_env(PATH=os.path.dirname(sys.executable))
        env.pop("ZFSB_REAL_ZFS", None)
        env.pop("ZFS_CMD", None)
        env.pop("ZPOOL_CMD", None)
        done = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/test_real_zfs.py", "tests/test_zfsbackup_real.py",
             "-m", "real_zfs", "-rs", "-p", "no:cacheprovider", "--no-cov", "-o", "addopts=", "-q"],
            cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=300,
        )
        out = done.stdout + done.stderr
        assert done.returncode == 0, out
        assert " passed" not in out.splitlines()[-1] and " failed" not in out.splitlines()[-1]
        assert re.search(r"\d+ skipped", out.splitlines()[-1]), out
        assert "ZFSB_REAL_ZFS=1" in out                     # the reason is printed (-rs)
        assert re.fullmatch(r"\d+ skipped in [\d.]+s", out.splitlines()[-1].strip()), out

    @pytest.mark.parametrize("opted_in", [False, True])
    def test_the_stub_policy_in_a_real_session(self, tmp_path, opted_in):
        """End to end, with the repo's conftest loaded: with no ZFS_CMD set, the
        binary libzfseasy resolves is the STUB unless the real tier is opted into,
        in which case it is whatever is on PATH (here a harmless fake `zfs` script)."""
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        for name in ("zfs", "zpool"):
            (bin_dir / name).write_text("#!/bin/sh\nexit 0\n")
            (bin_dir / name).chmod(0o755)
        probe = tmp_path / "test_probe.py"
        probe.write_text(
            "import libzfseasy.zfs as z\n"
            "def test_bin():\n"
            "    print('ZFS_BIN=' + z.ZFS_BIN)\n"
        )
        env = nested_env(PATH=os.pathsep.join([str(bin_dir), os.path.dirname(sys.executable)]))
        env.pop("ZFS_CMD", None)
        env.pop("ZPOOL_CMD", None)
        if opted_in:
            env["ZFSB_REAL_ZFS"] = "1"
        done = pytest_with_conftest(probe, "-s", env=env)
        assert done.returncode == 0, done.stdout + done.stderr
        printed = re.search(r"ZFS_BIN=(\S+)", done.stdout).group(1)
        if opted_in:
            assert printed == str(bin_dir / "zfs")                 # untouched: the real tier's binary
        else:
            assert "zfsbackup-test-stub-" in printed and printed != str(bin_dir / "zfs")

    @pytest.mark.slow
    def test_a_fake_zfs_first_on_path_is_fine_in_a_real_session(self, tmp_path):
        """The guard measures "the real zfs" against the PATH of conftest import: a
        module-scoped fixture (set up BEFORE the per-test guard) that puts a fake
        `zfs` first on PATH must not turn a bare `zfs` call into a violation."""
        probe = tmp_path / "test_probe.py"
        probe.write_text(
            "import os, subprocess, pytest\n"
            "@pytest.fixture(scope='module', autouse=True)\n"
            "def fake_zfs_first(tmp_path_factory):\n"
            "    d = tmp_path_factory.mktemp('fakebin')\n"
            "    (d / 'zfs').write_text('#!/bin/sh\\necho fake\\n')\n"
            "    (d / 'zfs').chmod(0o755)\n"
            "    old = os.environ['PATH']\n"
            "    os.environ['PATH'] = str(d) + os.pathsep + old\n"
            "    yield\n"
            "    os.environ['PATH'] = old\n"
            "def test_bare_zfs():\n"
            "    assert subprocess.run(['zfs'], capture_output=True, text=True).stdout == 'fake\\n'\n"
        )
        done = pytest_with_conftest(probe)
        assert done.returncode == 0, done.stdout + done.stderr

class TestHermeticEnv:
    def test_a_real_binary_named_by_the_environment_is_replaced(self, tmp_path, monkeypatch):
        import shutil

        real = tmp_path / "bin" / "zfs"
        real.parent.mkdir()
        shutil.copy(shutil.which("echo") or "/bin/echo", real)      # a compiled file named zfs
        monkeypatch.setenv("ZFS_CMD", str(real))
        monkeypatch.setenv("ZPOOL_CMD", str(real))
        env = hermetic_env()
        assert env["ZFS_CMD"] != str(real) and env["ZPOOL_CMD"] != str(real)
        assert os.access(env["ZFS_CMD"], os.X_OK)

    def test_a_stub_is_kept_and_extras_are_added(self, tmp_path, monkeypatch):
        stub = tmp_path / "stub"
        stub.write_text("#!/bin/sh\nexit 0\n")
        stub.chmod(0o755)
        monkeypatch.setenv("ZFS_CMD", str(stub))
        monkeypatch.delenv("ZPOOL_CMD", raising=False)
        env = hermetic_env(EXTRA="1")
        assert env["ZFS_CMD"] == str(stub) and env["EXTRA"] == "1"
        assert env["ZPOOL_CMD"] and os.access(env["ZPOOL_CMD"], os.X_OK)

    def test_it_does_not_mutate_the_process_environment(self, monkeypatch):
        monkeypatch.delenv("ZFS_CMD", raising=False)
        monkeypatch.delenv("ZPOOL_CMD", raising=False)
        before = dict(os.environ)
        env = hermetic_env()
        assert dict(os.environ) == before
        assert env["ZFS_CMD"] and env["ZPOOL_CMD"]

    def test_it_never_looks_for_zfs_on_path(self, monkeypatch):
        monkeypatch.delenv("ZFS_CMD", raising=False)
        monkeypatch.delenv("ZPOOL_CMD", raising=False)
        monkeypatch.setattr("shutil.which", lambda name, *a, **k: "/usr/local/zfs/bin/" + name)
        env = hermetic_env()
        assert "/usr/local/zfs" not in env["ZFS_CMD"] and "/usr/local/zfs" not in env["ZPOOL_CMD"]

    def test_path_is_a_private_directory_plus_the_interpreter_only(self):
        env = hermetic_env()
        directories = env["PATH"].split(os.pathsep)
        assert len(directories) == 2 and directories[1] == os.path.dirname(sys.executable)
        assert "/usr/bin" not in directories and "/bin" not in directories

    def test_a_bare_zfs_or_zpool_resolves_to_the_stub(self):
        import shutil

        env = hermetic_env()
        for name in ("zfs", "zpool"):
            found = shutil.which(name, path=env["PATH"])
            assert found is not None
            assert Path(found).resolve().read_text().startswith("#!/bin/sh")   # the stub, whatever PATH had

    def test_a_subprocess_that_runs_zfs_by_name_gets_the_stub(self):
        env = hermetic_env()
        private = env["PATH"].split(os.pathsep)[0]
        done = subprocess.run(
            [str(Path(private) / "sh"), "-c", "zfs list; echo rc=$?; command -v zfs; command -v zpool"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        assert "rc=0" in done.stdout
        assert done.stdout.count(private) == 2                      # both resolved inside the private dir

    def test_the_coreutils_a_test_script_needs_are_available(self):
        import shutil

        env = hermetic_env()
        for tool in ("sh", "true", "false", "sleep", "cat", "echo"):
            assert shutil.which(tool, path=env["PATH"]), tool

    def test_the_private_directory_is_not_a_system_directory(self):
        first = hermetic_env()["PATH"].split(os.pathsep)[0]
        assert first not in ("/usr/bin", "/bin", "/usr/local/bin", "/usr/sbin", "/sbin")
        assert sorted(p.name for p in Path(first).iterdir() if p.name in ("zfs", "zpool")) == ["zfs", "zpool"]


class TestGuardUsesTheOriginalPath:
    """The in-process guard measures "the real zfs" against the PATH of conftest
    import, not the PATH of the moment: a test may put a fake script first."""

    def test_a_fake_zfs_script_first_on_path_runs_fine(self, tmp_path, monkeypatch):
        fake = tmp_path / "zfs"
        fake.write_text("#!/bin/sh\necho fake\n")
        fake.chmod(0o755)
        monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
        done = subprocess.run(["zfs"], capture_output=True, text=True)       # bare name, resolved via PATH
        assert done.stdout == "fake\n"

    def test_the_installed_set_comes_from_the_original_path(self, tmp_path, monkeypatch):
        fake = tmp_path / "zfs"
        fake.write_text("#!/bin/sh\nexit 0\n")
        fake.chmod(0o755)
        monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
        assert os.path.realpath(fake) not in _hermetic.installed_zfs_binaries()
        import conftest

        assert conftest.INSTALLED_ZFS is _hermetic.installed_zfs_binaries()

    def test_is_real_zfs_judges_a_script_by_what_it_is(self, tmp_path):
        script = tmp_path / "zfs"
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(0o755)
        assert is_real_zfs(str(script), installed=frozenset()) is False
        assert is_real_zfs(str(script), installed=frozenset({os.path.realpath(script)})) is True   # at a real path

    def test_a_compiled_file_named_zfs_is_real(self, tmp_path):
        import shutil

        binary = tmp_path / "zpool"
        shutil.copy(shutil.which("echo") or "/bin/echo", binary)
        assert is_real_zfs(str(binary), installed=frozenset()) is True

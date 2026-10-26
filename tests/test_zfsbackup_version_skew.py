"""Milestone D, item S-8: the version-skew guard.

An upgrade replaces the code under a running daemon. `spawn` and `forkserver` import from
disk and the remote/API workers import `zfsbackup.remote` / `zfsbackup.api` lazily, so a
worker (re)started after the upgrade would run NEW code under an OLD supervisor. The guard:

* `version.code_signature()` -- installed version + identity of the dist-info `RECORD`
  (`st_ino`, `st_mtime_ns`, `st_size`) + the Python interpreter (`sys.version`, the stat of
  `sys.executable`); one distribution lookup, two stats, no hashing, no cache, no `zfs`.
* the supervisor takes it when the PROCESS starts (before the unbounded wait for a previous
  daemon's workers), compares it every `RELOAD_POLL_SECONDS` (at once on a pending SIGHUP)
  BEFORE the reload tick and before every spawn, and drains only on the same difference read
  twice, a poll apart;
* a drain is not a stop: only the remote and API workers (they import code lazily) leave at
  their next safe point and are not respawned; snapshot and pruning keep running; reloads are
  off; when the last has left the stop flag goes up for the rest and `run()` returns
  `EXIT_CODE_VERSION_CHANGED` (76); a SIGTERM meanwhile is the ordinary stop and exit code;
* every worker compares the signature first thing in `run()` (before the lazy imports) and
  exits `WORKER_EXIT_VERSION_SKEW` (4); the supervisor counts that as no failure and decides
  for itself (a confirmed change drains, anything else restarts the worker after a delay).

Layers:

* ``TestCodeSignature``            `version.code_signature` / `CodeSignature` over throwaway
                                   dist-info directories on `sys.path`.
* ``TestDaemonLockDraining`` is in `test_zfsbackup_daemonlock.py`.
* ``TestSupervisor*``              a real `BackupDaemon` over the scripted `World` of
                                   `test_zfsbackup_reload.py`; the disk is the `Disk` double
                                   behind the `_signature_now` seam. Nothing forks.
* ``TestStartup``                  `_run_startup`'s early read and the checks before the first fork.
* ``TestExitCodePlumbing``         76 survives `_run`, `main()` and `_hard_exit`.
* ``TestWorkerCheck``              `_SupervisorWatch._exit_on_version_skew` in-process, its place
                                   in `run()`, and `ExitReason.UPGRADE`.
* ``TestRealWorkerProcesses``      real worker processes under fork, spawn and forkserver.
* ``TestRealDaemon``               a real `python -m zfsbackup.daemon --dry-run` whose version
                                   metadata is a throwaway dist-info FIRST on `PYTHONPATH`;
                                   replacing its `RECORD` mid-run must end in exit 76.

Everything runs under the hermetic environment (stub `zfs`); every real daemon leads its own
session and the whole group is killed in a `finally`.

Mutation counts (scratch copy of the tree, one revert at a time) are in the hand-off report.
"""

import dataclasses
import importlib
import importlib.metadata
import json
import logging
import os
import pickle
import signal
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from unittest.mock import patch

import pytest

import zfsbackup
import zfsbackup.daemon as daemon_module
import zfsbackup.version as version_mod
import zfsbackup.workers as workers_module
from _skew_workers import LAZY_MODULES, PROBES, GatedRemoteWorker, ProbeGatedSnapshotWorker
from test_zfsbackup_daemon import _args, _stub_daemon, no_hard_exit  # noqa: F401  (fixture)
from test_zfsbackup_daemon_closeout import make_config
from test_zfsbackup_reload import (  # noqa: F401  (autouse fixtures and helpers)
    STORE,
    World,
    _debug_logging,
    _private_home,
    _restore_signal_handlers,
    cfg,
    exited_for_reload,
    messages,
)
from test_zfsbackup_reload_workers import ALL_FIVE, _Concrete, _build, wait_until
from test_zfsbackup_sdnotify import (  # noqa: F401  (fixtures and helpers)
    READY_WAIT,
    START_METHODS,
    NotifyDaemon,
    Recorder,
    Server,
    is_ready,
    method_or_skip,
    process_group_members,
    sockdir,
    take,
)
from zfsbackup.daemon import (
    RELOAD_POLL_SECONDS,
    VERSION_DRAIN_REPEAT,
    WORKER_MAX_CONSECUTIVE_FAILURES,
    WORKER_RESTART_DELAY,
    WORKER_STABLE_UPTIME,
    BackupDaemon,
    Handoff,
    main,
)
from zfsbackup.stopflag import ReloadEpoch, StopFlag
from zfsbackup.version import (
    DIST_NAME,
    EXIT_CODE_VERSION_CHANGED,
    UNKNOWN_VERSION,
    CodeSignature,
    code_signature,
)
from zfsbackup.workers import (
    EPOCH_UPGRADE_DRAIN,
    EPOCH_UPGRADE_DRAIN_API,
    WORKER_EXIT_RELOAD,
    WORKER_EXIT_VERSION_SKEW,
    ApiWorker,
    ExitReason,
    RemoteBackupWorker,
)

#: Part of the one warning every drain logs.
CHANGED = "changed on disk"

PY = sys.version
EXE = None  # (set below, lazily, by `exe_ident`)


def exe_ident():
    st = os.stat(sys.executable)
    return (st.st_ino, st.st_mtime_ns, st.st_size)


# ---------------------------------------------------------------------------
# Throwaway dist-info directories
# ---------------------------------------------------------------------------


def make_dist(root: Path, ver: str = "9.9.9", *, record: bool = True, extra=None) -> Path:
    """`<root>/zfsbackup-<ver>.dist-info/{METADATA[,RECORD]}`."""
    info = root / f"{DIST_NAME}-{ver}.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {DIST_NAME}\nVersion: {ver}\n"
    )
    if record:
        (info / "RECORD").write_text(f"{info.name}/RECORD,,\n")
    for name, text in (extra or {}).items():
        (info / name).write_text(text)
    importlib.invalidate_caches()
    return info


def replace_record(info: Path, data: bytes = None) -> None:
    """What `pip install` does to the `RECORD`: write a new file and rename it over the old.
    By default the SAME bytes, so the size is unchanged and only the inode / mtime move."""
    target = info / "RECORD"
    payload = target.read_bytes() if data is None else data
    new = info / "RECORD.new"
    new.write_bytes(payload)
    os.replace(new, target)


@pytest.fixture
def site(tmp_path, monkeypatch):
    """A directory FIRST on `sys.path`: a dist-info made here shadows the real one."""
    root = tmp_path / "site"
    root.mkdir()
    monkeypatch.syspath_prepend(str(root))
    return root


# ---------------------------------------------------------------------------
# code_signature()
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCodeSignature:
    def test_reports_the_version_the_record_the_interpreter_and_their_identities(self, site):
        info = make_dist(site, "9.9.9")
        sig = code_signature()
        st = os.stat(info / "RECORD")
        assert sig == CodeSignature(
            "9.9.9", str(info / "RECORD"), (st.st_ino, st.st_mtime_ns, st.st_size),
            sys.version, exe_ident(),
        )
        assert sig.installed is True

    def test_the_real_install_is_readable(self):
        """Whatever this environment is, the answer is a signature, never an exception."""
        sig = code_signature()
        assert isinstance(sig, CodeSignature)
        assert sig.version == version_mod.installed_version()
        assert sig.python == sys.version

    def test_a_same_size_replacement_of_the_record_changes_the_identity(self, site):
        """A same-version reinstall: pip rewrites the `RECORD`, byte for byte the same size."""
        info = make_dist(site)
        before = code_signature()
        replace_record(info)
        after = code_signature()
        assert after != before
        assert (after.version, after.file) == (before.version, before.file)
        assert after.ident[2] == before.ident[2]          # the size did not move...
        assert after.ident != before.ident                # ...but the inode / mtime did
        assert "reinstalled" in after.change_from(before)

    def test_a_touch_alone_changes_the_identity(self, site):
        info = make_dist(site)
        before = code_signature()
        st = os.stat(info / "RECORD")
        os.utime(info / "RECORD", ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))
        after = code_signature()
        assert after != before and after.ident[1] != before.ident[1]

    def test_a_version_bump_is_reported_in_words(self, site):
        make_dist(site, "9.9.9")
        before = code_signature()
        (site / "zfsbackup-9.9.9.dist-info").rename(site / "old-gone.tmp")
        make_dist(site, "9.9.10")
        after = code_signature()
        assert (before.version, after.version) == ("9.9.9", "9.9.10")
        assert after.change_from(before) == "9.9.9 -> 9.9.10"

    def test_it_is_not_cached(self, site):
        info = make_dist(site)
        record = info / "RECORD"
        base = os.stat(record).st_mtime_ns
        seen = []
        for n in range(3):
            seen.append(code_signature())
            replace_record(info)
            # explicit, clearly different mtimes: two replacements inside one filesystem timestamp tick
            # (~1 ms on ext4, which also recycles inode numbers) would otherwise repeat the identity
            stamp = base + (n + 1) * 10_000_000_000
            os.utime(record, ns=(stamp, stamp))
        seen.append(code_signature())
        assert len(set(seen)) == 4

    def test_a_missing_record_falls_back_to_the_package_init(self, site):
        info = make_dist(site)
        before = code_signature()
        (info / "RECORD").unlink()
        after = code_signature()
        init = Path(zfsbackup.__file__).resolve()
        st = os.stat(init)
        assert after.version == "9.9.9" and after.installed
        assert Path(after.file) == init
        assert after.ident == (st.st_ino, st.st_mtime_ns, st.st_size)
        assert after != before, "a vanishing RECORD must read as a change"

    def test_a_distribution_that_never_had_a_record_uses_the_package_init(self, site):
        make_dist(site, record=False)
        sig = code_signature()
        assert sig.version == "9.9.9"
        assert Path(sig.file) == Path(zfsbackup.__file__).resolve()

    def test_a_record_under_a_vanished_directory_is_a_fallback_too(self, site):
        make_dist(site)
        real_stat = os.stat

        def stat(path, *a, **k):
            if os.path.basename(os.fspath(path)) == "RECORD":
                raise NotADirectoryError(path)
            return real_stat(path, *a, **k)

        with mock.patch.object(version_mod.os, "stat", side_effect=stat):
            sig = code_signature()
        assert sig is not None and Path(sig.file).name == "__init__.py"

    def test_not_installed_is_an_empty_signature_that_still_names_the_interpreter(self):
        with mock.patch.object(
            version_mod, "distribution",
            side_effect=importlib.metadata.PackageNotFoundError(DIST_NAME),
        ):
            sig = code_signature()
        assert sig == CodeSignature(UNKNOWN_VERSION, None, None, sys.version, exe_ident())
        assert sig.installed is False
        assert str(sig).startswith(UNKNOWN_VERSION)

    @pytest.mark.filterwarnings("ignore:Implicit None on return values")
    def test_metadata_without_a_version_is_not_installed_either(self, site):
        info = site / "zfsbackup-0.dist-info"
        info.mkdir()
        (info / "METADATA").write_text("Metadata-Version: 2.1\nName: zfsbackup\n")
        importlib.invalidate_caches()
        sig = code_signature()
        assert sig.installed is False and sig.file is None and sig.ident is None

    def test_removing_the_whole_distribution_reads_as_something_else(self, site):
        info = make_dist(site)
        assert code_signature().version == "9.9.9"
        for child in info.iterdir():
            child.unlink()
        info.rmdir()
        importlib.invalidate_caches()
        # The real install further down `sys.path` may still answer; a bare tree does not.
        sig = code_signature()
        assert sig is None or sig.version != "9.9.9"

    @pytest.mark.parametrize(
        "exc", [PermissionError(13, "denied"), OSError(5, "I/O error"), OSError(24, "EMFILE")]
    )
    def test_an_unexpected_io_error_on_the_record_is_cannot_tell(self, site, exc):
        make_dist(site)
        real_stat = os.stat

        def stat(path, *a, **k):
            if os.path.basename(os.fspath(path)) == "RECORD":
                raise exc
            return real_stat(path, *a, **k)

        with mock.patch.object(version_mod.os, "stat", side_effect=stat):
            assert code_signature() is None

    def test_a_failing_fallback_stat_is_cannot_tell(self, site):
        info = make_dist(site)
        (info / "RECORD").unlink()
        real_stat = os.stat

        def stat(path, *a, **k):
            if os.path.basename(os.fspath(path)) == "__init__.py":
                raise OSError(5, "I/O error")
            return real_stat(path, *a, **k)

        with mock.patch.object(version_mod.os, "stat", side_effect=stat):
            assert code_signature() is None

    def test_an_unreadable_interpreter_is_cannot_tell_but_a_missing_one_is_not(self, site):
        make_dist(site)
        real_stat = os.stat

        def stat_for(exc):
            def stat(path, *a, **k):
                if os.fspath(path) == sys.executable:
                    raise exc
                return real_stat(path, *a, **k)
            return stat

        with mock.patch.object(version_mod.os, "stat", side_effect=stat_for(PermissionError(13, "x"))):
            assert code_signature() is None
        with mock.patch.object(version_mod.os, "stat", side_effect=stat_for(FileNotFoundError(2, "x"))):
            sig = code_signature()
        assert sig.exe is None and sig.version == "9.9.9"

    def test_an_empty_sys_executable_is_no_exe_identity(self, site, monkeypatch):
        make_dist(site)
        monkeypatch.setattr(version_mod.sys, "executable", "")
        assert code_signature().exe is None

    def test_any_other_exception_is_cannot_tell(self, site):
        make_dist(site)
        with mock.patch.object(version_mod, "distribution", side_effect=ValueError("corrupt metadata")):
            assert code_signature() is None

    def test_exactly_one_distribution_lookup_so_version_and_path_cannot_disagree(self, site):
        make_dist(site)
        with mock.patch.object(
            version_mod, "distribution", wraps=importlib.metadata.distribution
        ) as lookups:
            assert code_signature().version == "9.9.9"
        assert lookups.call_count == 1

    def test_it_stats_the_record_and_the_interpreter_reads_no_record_and_runs_nothing(
        self, site, monkeypatch
    ):
        """No hashing, no `zfs`: one `stat` of the RECORD, one of the interpreter, never an
        `open` of the RECORD, no subprocess."""
        info = make_dist(site)
        statted, opened = [], []
        real_stat = os.stat
        real_open = open

        def stat(path, *a, **k):
            statted.append(os.fspath(path))
            return real_stat(path, *a, **k)

        def opener(file, *a, **k):
            opened.append(os.fspath(file) if isinstance(file, (str, bytes, os.PathLike)) else file)
            return real_open(file, *a, **k)

        def forbidden(*a, **k):
            raise AssertionError("code_signature must not run a subprocess")

        monkeypatch.setattr(version_mod.os, "stat", stat)
        monkeypatch.setattr("builtins.open", opener)
        monkeypatch.setattr("subprocess.Popen", forbidden)
        assert code_signature() is not None
        assert [p for p in statted if p.endswith("RECORD")] == [str(info / "RECORD")]
        assert statted.count(sys.executable) == 1
        assert not [p for p in opened if isinstance(p, str) and p.endswith("RECORD")]

    def test_a_different_interpreter_version_changes_the_signature(self, site, monkeypatch):
        make_dist(site)
        before = code_signature()
        monkeypatch.setattr(version_mod.sys, "version", "3.99.1 (main, Jan 1 2099)")
        after = code_signature()
        assert after != before
        assert after.change_from(before).startswith("the Python interpreter changed (")
        assert "3.99.1" in after.change_from(before)

    def test_a_replaced_interpreter_binary_changes_the_signature(self, site):
        make_dist(site)
        before = code_signature()
        real = version_mod._stat_ident

        def replaced(path):
            ident = real(path)
            return (ident[0] + 1, ident[1], ident[2]) if path == sys.executable else ident

        with mock.patch.object(version_mod, "_stat_ident", side_effect=replaced):
            after = code_signature()
        assert after != before
        assert after.change_from(before) == "the Python interpreter binary was replaced"

    def test_it_pickles_and_is_a_frozen_value(self):
        sig = CodeSignature("1.2.3", "/x/RECORD", (1, 2, 3), "3.14.8 (main)", (4, 5, 6))
        assert pickle.loads(pickle.dumps(sig)) == sig
        assert hash(sig) == hash(CodeSignature("1.2.3", "/x/RECORD", (1, 2, 3), "3.14.8 (main)", (4, 5, 6)))
        with pytest.raises(dataclasses.FrozenInstanceError):
            sig.version = "9"
        none = CodeSignature(UNKNOWN_VERSION, None, None)
        assert pickle.loads(pickle.dumps(none)) == none

    def test_the_real_signature_pickles(self):
        sig = code_signature()
        assert pickle.loads(pickle.dumps(sig)) == sig

    def test_as_tuple_is_a_plain_tuple_that_rebuilds_the_signature(self):
        sig = CodeSignature("1.2.3", "/x/RECORD", (1, 2, 3), "3.14.8 (main)", (4, 5, 6))
        plain = sig.as_tuple()
        assert type(plain) is tuple
        assert plain == ("1.2.3", "/x/RECORD", (1, 2, 3), "3.14.8 (main)", (4, 5, 6))
        assert CodeSignature(*plain) == sig
        assert pickle.loads(pickle.dumps(plain)) == plain

    def test_str_names_the_file_the_identity_the_version_and_the_python(self):
        sig = CodeSignature(
            "1.2.3", "/s/zfsbackup-1.2.3.dist-info/RECORD", (11, 22, 33), "3.14.8 (main, x)", None
        )
        assert str(sig) == "1.2.3 (RECORD inode 11, mtime_ns 22, 33 bytes), Python 3.14.8"
        assert str(CodeSignature(UNKNOWN_VERSION, None, None, "3.14.8 (main)")) == f"{UNKNOWN_VERSION}, Python 3.14.8"
        assert str(CodeSignature("1", None, None)).endswith("Python ?")

    def test_change_from_in_words(self):
        old = CodeSignature("1.0.0", "/s/RECORD", (1, 2, 3), "3.14.8 (a)", (7, 8, 9))
        newer = dataclasses.replace(old, version="1.1.0")
        same = dataclasses.replace(old, ident=(9, 2, 3))
        gone = CodeSignature(UNKNOWN_VERSION, None, None, old.python, old.exe)
        assert newer.change_from(old) == "1.0.0 -> 1.1.0"
        assert same.change_from(old) == f"1.0.0 was reinstalled ({old} -> {same})"
        assert gone.change_from(old) == "zfsbackup is being reinstalled or was removed"
        assert dataclasses.replace(old, python="3.14.9 (b)").change_from(old) == \
            "the Python interpreter changed (3.14.8 -> 3.14.9)"
        assert dataclasses.replace(old, exe=(7, 9, 9)).change_from(old) == \
            "the Python interpreter binary was replaced"
        # Nothing was installed before: the plain version wording applies.
        assert old.change_from(gone) == f"{UNKNOWN_VERSION} -> 1.0.0"

    def test_the_public_files_list_is_the_fallback_for_the_record_path(self, tmp_path):
        record = tmp_path / "zfsbackup-1.dist-info" / "RECORD"
        pp = importlib.metadata.PackagePath("zfsbackup-1.dist-info/RECORD")
        pp.dist = SimpleNamespace(locate_file=lambda p: tmp_path / str(p))
        other = importlib.metadata.PackagePath("zfsbackup/RECORD")      # not in a dist-info
        other.dist = pp.dist
        no_path = SimpleNamespace(files=[other, pp])
        assert version_mod._record_path(no_path) == str(record)
        assert version_mod._record_path(SimpleNamespace(files=[other])) is None
        assert version_mod._record_path(SimpleNamespace(files=None)) is None

    def test_the_private_path_wins_over_the_files_list(self, tmp_path):
        dist = SimpleNamespace(_path=tmp_path / "x.dist-info", files=[])
        assert version_mod._record_path(dist) == str(tmp_path / "x.dist-info" / "RECORD")

    @pytest.mark.parametrize(
        "where, expected",
        [
            ("/usr/lib/python3/site-packages/zfsbackup/version.py", True),
            ("/usr/lib/python3/dist-packages/zfsbackup/version.py", True),
            ("/home/me/src/libzfs/zfsbackup/version.py", False),
            ("/home/me/site-packages-src/zfsbackup/version.py", False),
        ],
    )
    def test_installed_in_site_packages_looks_at_the_path_components(self, monkeypatch, where, expected):
        monkeypatch.setattr(version_mod, "__file__", where)
        assert version_mod.installed_in_site_packages() is expected

    def test_editable_install_reads_direct_url_json(self, site):
        make_dist(site, extra={"direct_url.json": json.dumps(
            {"url": "file:///src", "dir_info": {"editable": True}})})
        assert version_mod.editable_install() is True

    @pytest.mark.parametrize(
        "text",
        [
            json.dumps({"url": "file:///src", "dir_info": {}}),
            json.dumps({"url": "file:///src", "dir_info": {"editable": False}}),
            json.dumps({"url": "https://x/y.whl", "archive_info": {}}),
            "not json",
            "",
        ],
    )
    def test_a_regular_or_unreadable_install_is_not_editable(self, site, text):
        make_dist(site, extra={"direct_url.json": text})
        assert version_mod.editable_install() is False

    def test_no_direct_url_json_is_not_editable(self, site):
        make_dist(site)
        assert version_mod.editable_install() is False


# ---------------------------------------------------------------------------
# The supervisor, over the scripted World
# ---------------------------------------------------------------------------

SIG = CodeSignature(
    "1.0.0", "/site/zfsbackup-1.0.0.dist-info/RECORD", (11, 1000, 50), "3.14.8 (main, Oct 2026)", (1, 2, 3)
)
SIG_REINSTALLED = dataclasses.replace(SIG, ident=(12, 2000, 50))
SIG_UPGRADED = dataclasses.replace(
    SIG, version="1.1.0", file="/site/zfsbackup-1.1.0.dist-info/RECORD", ident=(13, 3000, 60)
)
SIG_NEW_PYTHON = dataclasses.replace(SIG, python="3.14.9 (main, Nov 2026)")
NOT_INSTALLED = CodeSignature(UNKNOWN_VERSION, None, None, SIG.python, SIG.exe)


class Disk:
    """What the supervisor's `_signature_now` seam reads: the installed code. `queue` is
    consumed first (one value per read), then `value` answers every read."""

    def __init__(self, value=SIG):
        self.value = value
        self.queue = []
        self.reads = 0

    def __call__(self):
        self.reads += 1
        current = self.queue.pop(0) if self.queue else self.value
        if isinstance(current, BaseException):
            raise current
        return current


@pytest.fixture
def disk(mocker):
    d = Disk()
    mocker.patch.object(BackupDaemon, "_signature_now", staticmethod(d))
    return d


@pytest.fixture
def make_world(disk, mocker, tmp_path):
    """`make_world(**World kwargs)`; the supervisor's start-up read is `disk.value` at that moment."""

    def make(**kwargs):
        return World(mocker, tmp_path, **kwargs)

    return make


@pytest.fixture
def sworld(make_world):
    """Four workers: snapshot, pruning, api, remote."""
    return make_world(config=cfg(remote=True))


class FlagRecorder(Recorder):
    """A `Recorder` that also notes, at each send, whether the stop flag was up and which
    of the world's workers had already left."""

    def __init__(self, world):
        super().__init__(log=world.events)
        self.world = world
        self.at_send = []

    def send(self, *msg):
        d = self.world.daemon
        gone = sorted(n for n, w in d._workers.items() if not w.is_alive())
        self.at_send.append((msg[0] if msg else "", d._stop_event.is_set(), gone))
        return super().send(*msg)

    def seen(self, head_prefix):
        return [(flag, gone) for head, flag, gone in self.at_send if head.startswith(head_prefix)]


@pytest.fixture
def srec(sworld):
    r = FlagRecorder(sworld)
    sworld.daemon._notify = r
    return r


class FlagHandler(logging.Handler):
    """Records `(level, message, stop flag up?)` for every record `zfsbackup.daemon` logs."""

    def __init__(self, daemon):
        super().__init__(logging.DEBUG)
        self.daemon = daemon
        self.seen = []

    def emit(self, record):
        self.seen.append((record.levelno, record.getMessage(), self.daemon._stop_event.is_set()))


@pytest.fixture
def flaglog(sworld):
    handler = FlagHandler(sworld.daemon)
    log = logging.getLogger("zfsbackup.daemon")
    log.addHandler(handler)
    yield handler
    log.removeHandler(handler)


def drive(world, mocker, *steps):
    """`daemon.run()`, where the Nth `_supervisor_sleep` runs `steps[N]` and, once the steps are
    spent, raises the stop flag as an ordinary stop would. Returns `run()`'s code."""
    pending = iter(steps)

    def fake_sleep(seconds):
        step = next(pending, None)
        if step is None:
            world.daemon._stop_event.set()
        else:
            step()

    mocker.patch.object(world.daemon, "_supervisor_sleep", side_effect=fake_sleep)
    return world.daemon.run()


def advance(world, seconds=RELOAD_POLL_SECONDS):
    return lambda: world.advance(seconds)


def reads(disk, world, new, *, then=RELOAD_POLL_SECONDS):
    """A step: the disk now reads `new`, and `then` seconds pass (one supervisor poll)."""
    def step():
        disk.value = new
        world.advance(then)
    return step


def confirm(disk, world, new):
    """The two steps that make a difference confirmed: it is read twice, a poll apart."""
    return [reads(disk, world, new), advance(world)]


def leave(world, *names):
    """A step: these workers reach their safe point and exit 0."""
    def step():
        for name in names:
            world.daemon._workers[name].die(0)
    return step


def begin_drain(world, disk, new=SIG_UPGRADED):
    """Drive the supervisor to a confirmed difference without `run()`."""
    d = world.daemon
    disk.value = new
    world.advance(RELOAD_POLL_SECONDS)
    assert d._check_code_version() is False         # first read: only a candidate
    world.advance(RELOAD_POLL_SECONDS)
    assert d._check_code_version() is True           # second, a poll later: confirmed
    assert d._version_skew is not None


def skew_warnings(caplog):
    return messages(caplog, logging.WARNING, containing=CHANGED)


@pytest.mark.unit
class TestSupervisorRecordsTheSignature:
    def test_it_is_read_once_at_construction_when_none_is_passed(self, sworld, disk):
        assert disk.reads == 1
        assert sworld.daemon._code_signature == SIG
        assert sworld.daemon._version_skew is None and sworld.daemon._signature_problem is None

    def test_a_signature_passed_in_is_the_baseline_and_nothing_is_read(self, sworld, disk):
        before = disk.reads
        d = BackupDaemon(
            sworld.running_config, sworld.resolved, key=(STORE, 0), started_signature=SIG_UPGRADED,
        )
        assert d._code_signature == SIG_UPGRADED and disk.reads == before

    @pytest.mark.parametrize(
        "passed, problem", [(None, "unreadable"), (NOT_INSTALLED, "not installed")]
    )
    def test_a_passed_none_or_not_installed_turns_the_guard_off_and_says_why(
        self, sworld, passed, problem
    ):
        d = BackupDaemon(
            sworld.running_config, sworld.resolved, key=(STORE, 0), started_signature=passed,
        )
        assert d._code_signature is None and d._signature_problem == problem

    def test_every_worker_class_is_handed_the_signature_as_a_plain_tuple(self, sworld):
        sworld.start_workers()
        for attr in ("SnapshotWorker", "PruningWorker", "ApiWorker", "RemoteBackupWorker"):
            passed = getattr(daemon_module, attr).call_args.kwargs["code_signature"]
            assert type(passed) is tuple and passed == SIG.as_tuple(), attr
        assert all(w.code_signature == SIG.as_tuple() for w in sworld.made)
        assert len(sworld.made) == 4

    @pytest.mark.parametrize("value", [None, NOT_INSTALLED], ids=["unreadable", "not-installed"])
    def test_guard_off_hands_workers_nothing(self, disk, make_world, value):
        disk.value = value
        world = make_world(config=cfg(remote=True))
        assert world.daemon._code_signature is None
        world.start_workers()
        for attr in ("SnapshotWorker", "PruningWorker", "ApiWorker", "RemoteBackupWorker"):
            assert "code_signature" not in getattr(daemon_module, attr).call_args.kwargs, attr

    def test_guard_off_unreadable_is_a_warning_and_the_disk_is_never_read_again(
        self, disk, make_world, mocker, caplog
    ):
        disk.value = None
        world = make_world()
        before = disk.reads
        code = drive(
            world, mocker,
            lambda: (world.advance(60), setattr(disk, "value", SIG_UPGRADED)),
            advance(world, 60), advance(world, 60),
        )
        assert code == 0 and disk.reads == before
        (warning,) = messages(caplog, logging.WARNING, containing="version-skew guard is off")
        assert "cannot read the installed zfsbackup's signature" in warning
        assert not skew_warnings(caplog)

    def test_guard_off_not_installed_is_one_info(self, disk, make_world, mocker, caplog):
        disk.value = NOT_INSTALLED
        world = make_world()
        drive(world, mocker)
        (line,) = messages(caplog, logging.INFO, containing="version-skew guard is off")
        assert "not an installed distribution" in line
        assert not messages(caplog, logging.WARNING, containing="version-skew guard")

    def test_guard_on_says_what_it_is_watching_and_what_it_cannot_see(
        self, sworld, mocker, caplog
    ):
        drive(sworld, mocker)
        assert messages(caplog, logging.INFO, containing=f"zfsbackup {SIG}")
        (on,) = messages(caplog, logging.INFO, containing="version-skew guard on")
        assert "NOT detected" in on and "werkzeug" in on
        assert not messages(caplog, containing="version-skew guard is off")

    @pytest.mark.parametrize("editable", [True, False])
    def test_an_editable_install_is_said_once(self, sworld, mocker, caplog, editable):
        mocker.patch.object(daemon_module, "editable_install", return_value=editable)
        drive(sworld, mocker)
        assert bool(messages(caplog, logging.INFO, containing="editable install")) is editable

    def test_the_default_seam_is_version_code_signature(self):
        assert BackupDaemon._signature_now() == code_signature()

    def test_the_signature_is_not_part_of_the_reload_handoff(self, sworld):
        """It never changes in a supervisor's life, so a reload cannot drop or replace it."""
        assert "code_signature" not in {f.name for f in dataclasses.fields(Handoff)}
        sworld.start_workers()
        sworld.write((STORE, 1), config=cfg(remote=True, prefix="landed"))
        sworld.hup()
        assert sworld.daemon._handoff.epoch == 1
        sworld.daemon._spawn("snapshot", verb="Reloaded")
        assert sworld.daemon._code_signature == SIG
        assert daemon_module.SnapshotWorker.call_args.kwargs["code_signature"] == SIG.as_tuple()


@pytest.mark.unit
class TestSupervisorPoll:
    def test_nothing_is_read_before_ten_seconds_and_one_read_at_ten(self, sworld, disk):
        d = sworld.daemon
        sworld.advance(RELOAD_POLL_SECONDS - 1)
        assert d._check_code_version() is False and disk.reads == 1       # the start-up read only
        sworld.advance(1)
        assert d._check_code_version() is False and disk.reads == 2

    def test_the_cadence_restarts_from_every_read(self, sworld, disk):
        d = sworld.daemon
        sworld.advance(RELOAD_POLL_SECONDS)
        d._check_code_version()                                  # read #2
        sworld.advance(RELOAD_POLL_SECONDS - 1)
        d._check_code_version()                                  # not due
        assert disk.reads == 2
        sworld.advance(1)
        d._check_code_version()
        assert disk.reads == 3

    def test_force_reads_now_even_when_the_cadence_is_not_due(self, sworld, disk):
        assert sworld.daemon._check_code_version(force=False) is False
        assert disk.reads == 1
        assert sworld.daemon._check_code_version(force=True) is False
        assert disk.reads == 2

    def test_run_passes_the_pending_sighup_as_force(self, sworld, disk, mocker):
        """One iteration, no clock movement: only a SIGHUP makes the check due."""
        def hup():
            sworld.daemon._sighup_handler(signal.SIGHUP, None)

        spy = mocker.spy(sworld.daemon, "_check_code_version")
        drive(sworld, mocker, advance(sworld, 1), hup, advance(sworld, 1))
        assert [c.kwargs["force"] for c in spy.call_args_list][:3] == [False, True, False]

    def test_an_unchanged_disk_never_drains_anything(self, sworld, disk, mocker, caplog):
        code = drive(sworld, mocker, *[advance(sworld)] * 6)
        assert code == 0 and sworld.daemon._version_skew is None
        assert not skew_warnings(caplog)
        assert sworld.daemon._reload_epoch.get() == 0


@pytest.mark.unit
class TestSupervisorConfirmsTheDifference:
    """A drain is irreversible and long: the supervisor drains on the same difference read
    twice, a poll apart. (importlib.metadata swallows a failed directory listing, which reads
    as 'not installed'; an install in flight changes under the reader.)"""

    def test_one_differing_read_does_nothing(self, sworld, disk, caplog):
        d = sworld.daemon
        disk.value = SIG_UPGRADED
        sworld.advance(RELOAD_POLL_SECONDS)
        assert d._check_code_version() is False
        assert d._version_skew is None and d._reload_epoch.get() == 0
        assert not skew_warnings(caplog) and not d._stop_event.is_set()

    def test_the_same_difference_a_poll_later_drains(self, sworld, disk, caplog):
        begin_drain(sworld, disk, SIG_UPGRADED)
        (warning,) = skew_warnings(caplog)
        assert "1.0.0 -> 1.1.0" in warning

    def test_a_transient_difference_is_forgotten_when_the_disk_reads_the_baseline_again(
        self, sworld, disk
    ):
        d = sworld.daemon
        for value in (SIG_UPGRADED, SIG, SIG_UPGRADED):         # differ, equal, differ: no pair
            disk.value = value
            sworld.advance(RELOAD_POLL_SECONDS)
            assert d._check_code_version() is False
        assert d._version_skew is None
        sworld.advance(RELOAD_POLL_SECONDS)
        assert d._check_code_version() is True                   # now the same one twice

    def test_two_different_differences_are_not_a_pair(self, sworld, disk):
        d = sworld.daemon
        for value in (SIG_UPGRADED, SIG_REINSTALLED):
            disk.value = value
            sworld.advance(RELOAD_POLL_SECONDS)
            assert d._check_code_version() is False
        assert d._version_skew is None

    def test_two_reads_less_than_a_poll_apart_are_not_a_confirmation(self, sworld, disk):
        d = sworld.daemon
        disk.value = SIG_UPGRADED
        sworld.advance(RELOAD_POLL_SECONDS)
        d._check_code_version(force=True)
        sworld.advance(RELOAD_POLL_SECONDS - 1)
        assert d._check_code_version(force=True) is False
        sworld.advance(1)
        assert d._check_code_version(force=True) is True

    def test_a_forced_read_counts_like_a_polled_one(self, sworld, disk, mocker):
        steps = [
            reads(disk, sworld, SIG_UPGRADED),
            lambda: (sworld.advance(RELOAD_POLL_SECONDS), sworld.daemon._sighup_handler(signal.SIGHUP, None)),
            leave(sworld, "api", "remote"),
        ]
        assert drive(sworld, mocker, *steps) == 76

    @pytest.mark.parametrize(
        "new, words",
        [
            (SIG_UPGRADED, "1.0.0 -> 1.1.0"),
            (SIG_REINSTALLED, "1.0.0 was reinstalled"),
            (NOT_INSTALLED, "zfsbackup is being reinstalled or was removed"),
            (SIG_NEW_PYTHON, "the Python interpreter changed (3.14.8 -> 3.14.9)"),
            (dataclasses.replace(SIG, exe=(9, 9, 9)), "the Python interpreter binary was replaced"),
        ],
        ids=["upgrade", "reinstall", "not-installed", "new-python", "new-exe"],
    )
    def test_the_warning_names_old_and_new(self, sworld, disk, caplog, new, words):
        begin_drain(sworld, disk, new)
        (warning,) = skew_warnings(caplog)
        assert words in warning and "exit 76" in warning

    def test_unreadable_is_no_change_and_one_warning_while_it_lasts(
        self, sworld, disk, caplog
    ):
        d = sworld.daemon
        disk.value = None
        for _ in range(4):
            sworld.advance(RELOAD_POLL_SECONDS)
            assert d._check_code_version() is False
        assert d._version_skew is None
        (warning,) = messages(caplog, logging.WARNING, containing="cannot read the installed")
        assert "will retry" in warning

    def test_it_warns_again_for_a_new_outage_after_a_good_read(self, sworld, disk, caplog):
        d = sworld.daemon
        for value in (None, None, SIG, None):
            disk.value = value
            sworld.advance(RELOAD_POLL_SECONDS)
            d._check_code_version()
        assert len(messages(caplog, logging.WARNING, containing="cannot read the installed")) == 2
        assert not skew_warnings(caplog)

    def test_nothing_is_checked_when_the_stop_flag_is_already_up(self, sworld, disk):
        sworld.daemon._stop_event.set()
        disk.value = SIG_UPGRADED
        assert sworld.daemon._check_code_version(force=True) is False
        assert sworld.daemon._version_skew is None and disk.reads == 1

    def test_a_sigterm_pending_when_the_disk_changes_is_never_76(self, sworld, disk, mocker, caplog):
        def sigterm():
            sworld.daemon._signal_handler(signal.SIGTERM, None)

        steps = [reads(disk, sworld, SIG_UPGRADED), advance(sworld), sigterm]
        assert drive(sworld, mocker, *steps) == 0
        assert sworld.daemon._version_exit is False

    def test_a_sigint_is_never_76_either(self, sworld, disk, mocker):
        def sigint():
            sworld.daemon._signal_handler(signal.SIGINT, None)

        assert drive(sworld, mocker, reads(disk, sworld, SIG_UPGRADED), advance(sworld), sigint) == 0


# -- the drain ----------------------------------------------------------------


@pytest.mark.unit
class TestDrain:
    def test_beginning_a_drain_is_not_a_stop(self, sworld, disk, srec, flaglog):
        workers = sworld.start_workers()
        begin_drain(sworld, disk)
        d = sworld.daemon
        assert d._version_skew == (SIG, SIG_UPGRADED)
        assert d._reload_epoch.get() == EPOCH_UPGRADE_DRAIN == -1
        assert not d._stop_event.is_set(), "the stop flag would give the transfers 30 s"
        assert all(w.is_alive() and w.exitcode is None for w in workers.values())
        warned = [flag for level, text, flag in flaglog.seen if CHANGED in text]
        assert warned == [False]
        assert srec.seen("STATUS=zfsbackup upgraded") == [(False, [])]

    def test_it_is_published_in_the_pid_file_and_to_systemd(self, sworld, disk, srec):
        sworld.start_workers()
        before = int(time.time())
        begin_drain(sworld, disk)
        state = sworld.lock.state
        assert before <= state["draining"] <= int(time.time())
        assert "zfsbackup upgraded (1.0.0 -> 1.1.0)" in state["draining_reason"]
        (status,) = [h for h in srec.heads() if h.startswith("STATUS=")]
        assert status == "STATUS=" + state["draining_reason"]
        assert "draining the remote worker, then the API worker" in status and "76" in status

    def test_the_run_ends_with_76_only_when_the_remote_and_api_workers_have_left(
        self, sworld, disk, srec, mocker
    ):
        d = sworld.daemon
        seen = {}

        def api_only():
            d._workers["api"].die(0)

        def check_not_over():
            seen["running"] = not d._stop_event.is_set() and d._version_exit is False

        steps = [
            *confirm(disk, sworld, SIG_UPGRADED), api_only, check_not_over, leave(sworld, "remote"),
        ]
        assert drive(sworld, mocker, *steps) == 76
        assert seen["running"] is True
        assert d._version_exit is True

    def test_snapshot_and_pruning_do_not_hold_the_drain_up_and_are_stopped_after_it(
        self, sworld, disk, srec, mocker
    ):
        steps = [*confirm(disk, sworld, SIG_UPGRADED), leave(sworld, "api", "remote")]
        assert drive(sworld, mocker, *steps) == 76
        by_name = {w.name: w.exitcode for w in sworld.made}
        # api/remote left on their own (0); snapshot/pruning were joined after the stop flag
        # went up (the fake `join` ends a live worker with 0): nobody was terminated or killed.
        assert by_name == {"snapshot": 0, "pruning": 0, "api": 0, "remote": 0}
        assert sworld.forks() == ["fork:snapshot", "fork:pruning", "fork:api", "fork:remote"]

    def test_stopping_is_sent_only_after_the_drained_workers_are_gone_and_with_the_flag_up(
        self, sworld, disk, srec, mocker
    ):
        steps = [*confirm(disk, sworld, SIG_UPGRADED), leave(sworld, "api", "remote")]
        drive(sworld, mocker, *steps)
        heads = srec.heads()
        assert heads[0] == "READY=1" and heads[-1] == "STOPPING=1"
        assert heads[1].startswith("STATUS=zfsbackup upgraded (1.0.0 -> 1.1.0)")
        ((flag, gone),) = srec.seen("STOPPING=1")
        assert flag is True and gone == ["api", "remote"]
        # snapshot and pruning were still running when the drain began:
        ((flag0, gone0),) = srec.seen("STATUS=zfsbackup upgraded")
        assert flag0 is False and gone0 == []

    def test_a_sigterm_during_the_drain_is_the_ordinary_stop_and_exit_code(
        self, sworld, disk, srec, mocker, caplog
    ):
        def sigterm():
            sworld.daemon._signal_handler(signal.SIGTERM, None)

        steps = [*confirm(disk, sworld, SIG_UPGRADED), sigterm]
        assert drive(sworld, mocker, *steps) == 0
        d = sworld.daemon
        assert d._version_exit is False and d._stop_event.is_set()
        assert "Received SIGTERM" in caplog.text
        assert srec.heads()[-1] == "STOPPING=1"
        # the ordinary shutdown joined every worker, remote and API included
        assert all(w.exitcode == 0 for w in sworld.made)

    def test_a_sigterm_during_the_final_shutdown_is_the_ordinary_exit_code(self, sworld, disk, mocker):
        """The drain ran to its end (`_version_exit`), then an operator's SIGTERM lands while the
        rest is being stopped: not 76."""
        d = sworld.daemon
        real = d._shutdown_workers

        def shutdown_with_a_sigterm(*a, **k):
            d._signal_handler(signal.SIGTERM, None)
            real(*a, **k)

        mocker.patch.object(d, "_shutdown_workers", side_effect=shutdown_with_a_sigterm)
        steps = [*confirm(disk, sworld, SIG_UPGRADED), leave(sworld, "api", "remote")]
        assert drive(sworld, mocker, *steps) == 0
        assert d._version_exit is True

    def test_a_sigterm_racing_the_last_worker_is_still_the_ordinary_stop(self, sworld, disk, mocker):
        def last_worker_and_sigterm():
            leave(sworld, "api", "remote")()
            sworld.daemon._signal_handler(signal.SIGTERM, None)

        steps = [*confirm(disk, sworld, SIG_UPGRADED), last_worker_and_sigterm]
        assert drive(sworld, mocker, *steps) == 0

    def test_a_sighup_during_the_drain_reloads_nothing_but_is_answered(
        self, sworld, disk, srec, mocker
    ):
        d = sworld.daemon
        mark = {}

        def hup_with_a_waiting_change():
            mark["events"] = len(sworld.events)
            sworld.write((STORE, 1), config=cfg(remote=True, prefix="landed"))
            d._sighup_handler(signal.SIGHUP, None)

        steps = [*confirm(disk, sworld, SIG_UPGRADED), hup_with_a_waiting_change,
                 leave(sworld, "api", "remote")]
        assert drive(sworld, mocker, *steps) == 76
        assert "load" not in sworld.events[mark["events"]:], "no configuration was read after the drain began"
        assert d._handoff.key == (STORE, 0)
        assert d._hup_requested is False, "consumed, or _supervisor_sleep would spin on it"
        heads = srec.heads()
        i = heads.index("RELOADING=1")
        assert heads[i + 1] == "READY=1"
        assert srec.statuses()[i + 1].startswith("zfsbackup upgraded"), "READY keeps the drain status"
        assert sworld.lock.state["deferred"] == (STORE, 1)
        assert "upgrade draining" in sworld.lock.state["deferred_reason"]

    def test_a_sighup_with_nothing_stored_is_answered_without_a_deferral(self, sworld, disk, srec, mocker):
        def hup():
            sworld.daemon._sighup_handler(signal.SIGHUP, None)

        drive(sworld, mocker, *confirm(disk, sworld, SIG_UPGRADED), hup, leave(sworld, "api", "remote"))
        assert "RELOADING=1" in srec.heads()
        assert sworld.lock.state["deferred"] is None

    def test_a_skew_is_found_before_the_reload_tick_never_after(self, sworld, disk, mocker):
        """The confirming read happens in an iteration that also carries a SIGHUP and a waiting
        configuration: the reload is not processed."""
        mark = {}

        def hup_with_a_waiting_change_and_the_confirming_read():
            mark["events"] = len(sworld.events)
            sworld.write((STORE, 1), config=cfg(remote=True, prefix="landed"))
            sworld.advance(RELOAD_POLL_SECONDS)
            sworld.daemon._sighup_handler(signal.SIGHUP, None)

        steps = [
            reads(disk, sworld, SIG_UPGRADED),              # the first sighting (idle reload tick)
            hup_with_a_waiting_change_and_the_confirming_read,
            leave(sworld, "api", "remote"),
        ]
        assert drive(sworld, mocker, *steps) == 76
        assert "load" not in sworld.events[mark["events"]:]
        assert sworld.daemon._handoff.key == (STORE, 0)

    def test_a_reload_accepted_earlier_cannot_mask_the_skew_that_follows(self, sworld, disk, mocker):
        def reload():
            sworld.write((STORE, 1), config=cfg(remote=True, prefix="landed"))
            sworld.daemon._sighup_handler(signal.SIGHUP, None)

        steps = [reload, *confirm(disk, sworld, SIG_UPGRADED), leave(sworld, "api", "remote")]
        assert drive(sworld, mocker, *steps) == 76
        assert sworld.daemon._handoff.key == (STORE, 1)
        assert {w.code_signature for w in sworld.made} == {SIG.as_tuple()}

    def test_nothing_is_spawned_during_a_drain(self, sworld, disk):
        sworld.start_workers()
        begin_drain(sworld, disk)
        disk.value = SIG                    # even if the install now reads like the baseline again
        made = len(sworld.made)
        assert sworld.daemon._spawn("snapshot") is None
        assert len(sworld.made) == made

    def test_dead_workers_during_a_drain_are_uncounted_and_not_respawned(self, sworld, disk, caplog):
        d = sworld.daemon
        workers = sworld.start_workers()
        begin_drain(sworld, disk)
        caplog.clear()
        workers["snapshot"].die(1)
        workers["pruning"].die(-signal.SIGKILL)
        workers["api"].die(0)
        workers["remote"].die(WORKER_EXIT_RELOAD)
        d._check_workers()
        d._check_workers()
        assert d._failures == {} and d._restart_at == {} and d._crash_looped == set()
        assert len(sworld.made) == 4
        # said once each, as drain deaths -- never as crashes with a backoff
        errors = messages(caplog, logging.ERROR)
        assert errors and all("during the upgrade drain; it is not restarted" in e for e in errors)
        assert not [m for m in messages(caplog) if "restarting in" in m]

    def test_a_reload_turnover_is_not_respawned_during_a_drain(self, sworld, disk):
        workers = sworld.start_workers()
        sworld.write((STORE, 1), config=cfg(remote=True, prefix="landed"))
        sworld.hup()
        begin_drain(sworld, disk)
        forks = list(sworld.forks())
        exited_for_reload(workers)
        sworld.daemon._check_workers()
        assert sworld.forks() == forks

    def test_the_same_turnover_without_a_drain_does_respawn_control(self, sworld):
        workers = sworld.start_workers()
        sworld.write((STORE, 1), config=cfg(remote=True, prefix="landed"))
        sworld.hup()
        forks = list(sworld.forks())
        exited_for_reload(workers)
        sworld.daemon._check_workers()
        assert len(sworld.forks()) == len(forks) + 4

    def test_no_deferred_switch_warnings_during_a_drain(self, sworld, disk, mocker):
        d = sworld.daemon
        sworld.start_workers()
        spy = mocker.spy(d, "_log_deferred_switches")
        d._check_workers()
        assert spy.call_count == 1                    # control: a normal pass calls it
        begin_drain(sworld, disk)
        d._check_workers()
        assert spy.call_count == 1

    def test_a_lost_orphan_that_exited_is_not_replaced_during_a_drain(self, sworld, disk, mocker):
        d = sworld.daemon
        sworld.start_workers()
        begin_drain(sworld, disk)
        d._lost.add("snapshot")
        mocker.patch.object(d, "_lost_running", return_value=False)
        d._check_workers()
        assert len(sworld.made) == 4 and d._failures == {}
        assert "snapshot" in d._workers and "snapshot" in d._lost, "left for the drain to notice"
        assert d._drain_lost_a_worker() is True

    def test_the_same_lost_orphan_is_replaced_without_a_drain_control(self, sworld, mocker):
        d = sworld.daemon
        sworld.start_workers()
        d._lost.add("snapshot")
        mocker.patch.object(d, "_lost_running", return_value=False)
        d._check_workers()
        assert len(sworld.made) == 5

    def test_a_running_drained_worker_that_is_a_forkserver_orphan_holds_the_drain_open(
        self, sworld, disk, mocker
    ):
        d = sworld.daemon
        workers = sworld.start_workers()
        begin_drain(sworld, disk)
        for name in ("api", "remote"):
            workers[name].die(0)
        d._lost.add("api")
        running = mocker.patch.object(d, "_lost_running", return_value=True)
        assert d._version_drained() is False
        running.return_value = False
        assert d._version_drained() is True and d._version_exit is True

    def test_a_forkserver_255_worker_is_not_respawned_during_a_drain(self, sworld, disk, mocker):
        d = sworld.daemon
        workers = sworld.start_workers()
        begin_drain(sworld, disk)
        d._worker_token["snapshot"] = "tok"
        mocker.patch.object(d, "_orphaned", return_value=False)
        mocker.patch.object(d, "_forkserver_state", return_value="gone")
        workers["snapshot"].die(255)
        d._check_workers()
        assert len(sworld.made) == 4 and d._failures == {}

    def test_a_long_drain_says_which_workers_it_is_waiting_for(self, sworld, disk, caplog):
        d = sworld.daemon
        workers = sworld.start_workers()
        begin_drain(sworld, disk)
        caplog.clear()
        d._log_version_drain()
        assert not messages(caplog, logging.WARNING)                     # not yet
        sworld.advance(VERSION_DRAIN_REPEAT)
        workers["remote"].die(0)
        d._log_version_drain()
        (line,) = messages(caplog, logging.WARNING, containing="still draining")
        assert "api" in line and "remote" not in line and "snapshot" not in line
        d._log_version_drain()
        assert len(messages(caplog, logging.WARNING, containing="still draining")) == 1

    def test_a_drain_with_no_remote_worker_in_the_config_ends_with_the_api_worker(
        self, make_world, disk, mocker
    ):
        world = make_world()                        # snapshot, pruning, api
        steps = [*confirm(disk, world, SIG_UPGRADED), leave(world, "api")]
        assert drive(world, mocker, *steps) == 76


@pytest.mark.unit
class TestDrainStages:
    """Stage 1 (epoch -1): the remote worker leaves at a safe point while the API worker keeps
    serving (a remote worker stuck in a hung `zfs` call must not leave the server refusing every
    client). Stage 2 (epoch -2): the API worker finishes its in-flight receives and leaves."""

    def test_the_api_worker_is_told_to_drain_only_after_the_remote_worker_has_left(
        self, sworld, disk, mocker
    ):
        d = sworld.daemon
        seen = {}

        def remote_still_running():
            seen["epoch_while_remote_runs"] = d._reload_epoch.get()

        def remote_leaves():
            d._workers["remote"].die(0)

        def after_remote():
            seen["epoch_after_remote"] = d._reload_epoch.get()
            seen["over_before_api"] = d._version_exit

        steps = [
            *confirm(disk, sworld, SIG_UPGRADED), remote_still_running, remote_still_running,
            remote_leaves, after_remote, leave(sworld, "api"),
        ]
        assert drive(sworld, mocker, *steps) == 76
        assert seen == {
            "epoch_while_remote_runs": EPOCH_UPGRADE_DRAIN,
            "epoch_after_remote": EPOCH_UPGRADE_DRAIN_API,
            "over_before_api": False,
        }

    def test_without_a_remote_worker_the_api_stage_starts_at_once(self, make_world, disk, mocker):
        world = make_world()                              # snapshot, pruning, api
        d = world.daemon
        seen = {}

        def epoch_now():
            seen["epoch"] = d._reload_epoch.get()

        assert drive(world, mocker, *confirm(disk, world, SIG_UPGRADED), epoch_now,
                     leave(world, "api")) == 76
        assert seen["epoch"] == EPOCH_UPGRADE_DRAIN_API

    def test_a_stuck_remote_worker_never_makes_the_api_worker_leave(self, sworld, disk, mocker):
        d = sworld.daemon
        epochs = []

        def watch():
            epochs.append(d._reload_epoch.get())

        steps = [*confirm(disk, sworld, SIG_UPGRADED), *[watch] * 5]
        assert drive(sworld, mocker, *steps) == 0          # the script's stop, not a drain's end
        assert epochs == [EPOCH_UPGRADE_DRAIN] * 5
        assert d._workers["api"].exitcode in (None, 0) and d._version_exit is False

    def test_the_hand_over_is_logged_once(self, sworld, disk, mocker, caplog):
        steps = [*confirm(disk, sworld, SIG_UPGRADED), leave(sworld, "remote"), advance(sworld, 1),
                 leave(sworld, "api")]
        drive(sworld, mocker, *steps)
        assert len(messages(caplog, logging.INFO, containing="the remote worker has left")) == 1

    def test_a_snapshot_or_pruning_worker_that_is_not_running_ends_the_drain(
        self, sworld, disk, srec, mocker, caplog
    ):
        d = sworld.daemon
        steps = [*confirm(disk, sworld, SIG_UPGRADED), lambda: d._workers["snapshot"].die(1)]
        assert drive(sworld, mocker, *steps) == 76
        (error,) = messages(caplog, logging.ERROR, containing="not running during the zfsbackup upgrade drain")
        assert "snapshot" in error
        # the remote and API workers had NOT left: the ordinary stop joined them
        assert d._stop_event.is_set() and [w.exitcode for w in sworld.made if w.name in ("api", "remote")] == [0, 0]
        assert srec.heads()[-1] == "STOPPING=1"

    def test_it_does_not_fire_while_both_are_running(self, sworld, disk):
        sworld.start_workers()
        begin_drain(sworld, disk)
        assert sworld.daemon._drain_lost_a_worker() is False
        assert sworld.daemon._version_exit is False

    def test_it_does_not_fire_once_a_stop_is_under_way(self, sworld, disk):
        d = sworld.daemon
        workers = sworld.start_workers()
        begin_drain(sworld, disk)
        workers["pruning"].die(1)
        d._stop_event.set()
        assert d._drain_lost_a_worker() is False and d._version_exit is False

    def test_an_exited_forkserver_orphan_counts_as_not_running(self, sworld, disk, mocker):
        d = sworld.daemon
        sworld.start_workers()
        begin_drain(sworld, disk)
        d._lost.add("snapshot")
        running = mocker.patch.object(d, "_lost_running", return_value=True)
        assert d._drain_lost_a_worker() is False            # an orphan that still runs is running
        running.return_value = False
        assert d._drain_lost_a_worker() is True and d._version_exit is True

    def test_a_deferred_respawn_that_never_happened_counts_as_not_running(self, sworld, disk):
        d = sworld.daemon
        workers = sworld.start_workers()
        begin_drain(sworld, disk)
        d._forget_worker("pruning")                # as after a turnover whose respawn was deferred
        assert d._drain_lost_a_worker() is True

    def test_a_drained_worker_leaving_with_0_is_info_and_anything_else_is_an_error_once(
        self, sworld, disk, caplog
    ):
        d = sworld.daemon
        workers = sworld.start_workers()
        begin_drain(sworld, disk)
        caplog.clear()
        workers["remote"].die(0)
        workers["pruning"].die(1)
        d._check_workers()
        d._check_workers()
        assert len(messages(caplog, logging.INFO, containing="remote worker")) == 1
        assert "has left for the upgrade" in messages(caplog, logging.INFO, containing="remote worker")[0]
        (error,) = messages(caplog, logging.ERROR, containing="pruning worker")
        assert "exited with code 1 during the upgrade drain; it is not restarted" in error


@pytest.mark.unit
class TestDeferredSpawnsAndReady:
    """A spawn the guard deferred leaves no worker; READY=1 means 'started', so it waits."""

    def test_ready_waits_for_the_deferred_workers_and_follows_the_last_fork(
        self, sworld, disk, srec, mocker
    ):
        d = sworld.daemon
        disk.queue = [SIG]                           # the start-block read; every later read differs
        disk.value = SIG_UPGRADED

        def settled_and_due():
            disk.value = SIG                         # the install finished and matches again
            sworld.advance(WORKER_RESTART_DELAY)

        before = {}

        def look():
            before["heads"] = list(srec.heads())

        code = drive(sworld, mocker, look, settled_and_due)
        assert code == 0
        assert "READY=1" not in before["heads"], "no READY while nothing has been started"
        heads = srec.heads()
        assert heads.count("READY=1") == 1 and heads[-1] == "STOPPING=1"
        assert len(sworld.made) == 4

    def test_a_drain_that_begins_with_workers_owed_sends_ready_with_the_drain_status(
        self, sworld, disk, srec, mocker
    ):
        disk.queue = [SIG]
        disk.value = SIG_UPGRADED
        code = drive(sworld, mocker, advance(sworld))     # the confirming read; no worker exists
        assert code == 76
        heads = srec.heads()
        assert heads[0].startswith("STATUS=zfsbackup upgraded")
        assert heads[1] == "READY=1" and srec.statuses()[1].startswith("zfsbackup upgraded")
        assert heads[-1] == "STOPPING=1"
        assert sworld.made == []

    def test_a_generation_is_not_applied_while_a_respawn_is_deferred(self, sworld, disk):
        d = sworld.daemon
        sworld.start_workers()
        sworld.write((STORE, 1), config=cfg(remote=True, prefix="landed"))
        sworld.hup()
        epoch = d._handoff.epoch
        assert epoch == 1
        for name in d._workers:                      # every live worker is on the new epoch...
            d._worker_epoch[name] = epoch
        d._applied_epoch = 0
        d._deferred_spawns = frozenset({"pruning"})  # ...but one respawn is still deferred
        d._maybe_complete()
        assert d._applied_epoch == 0
        assert sworld.lock.state["applied"] != d._handoff.key
        d._deferred_spawns = frozenset()
        d._maybe_complete()
        assert d._applied_epoch == epoch and sworld.lock.state["applied"] == d._handoff.key

    def test_a_deferred_spawn_is_remembered_and_cleared_when_it_happens(self, sworld, disk):
        d = sworld.daemon
        disk.value = SIG_UPGRADED
        d._spawn("snapshot")
        assert d._deferred_spawns == frozenset({"snapshot"})
        disk.value = SIG
        sworld.advance(WORKER_RESTART_DELAY)
        assert d._spawn("snapshot") is not None
        assert d._deferred_spawns == frozenset()


# -- worker exit 4 ----------------------------------------------------------


@pytest.mark.unit
class TestWorkerReportsSkew:
    def test_exit_4_while_the_supervisor_sees_no_difference_restarts_the_worker_uncounted(
        self, sworld, caplog
    ):
        d = sworld.daemon
        workers = sworld.start_workers()
        forks = list(sworld.forks())
        workers["snapshot"].die(WORKER_EXIT_VERSION_SKEW)
        d._check_workers()
        assert d._version_skew is None and not d._stop_event.is_set()
        assert d._failures == {} and d._crash_looped == set()
        assert d._restart_at == {"snapshot": sworld.clock[0] + WORKER_RESTART_DELAY}
        assert sworld.forks() == forks
        (warning,) = messages(caplog, logging.WARNING, containing="does not (yet) see")
        assert "snapshot worker" in warning
        sworld.advance(WORKER_RESTART_DELAY)
        d._check_workers()
        assert len(sworld.forks()) == len(forks) + 1 and d._failures == {}

    def test_three_reports_in_a_row_are_believed_and_drain_the_daemon(self, sworld, caplog):
        d = sworld.daemon
        workers = sworld.start_workers()
        for round_ in range(1, 4):
            workers["snapshot"].die(WORKER_EXIT_VERSION_SKEW)
            d._check_workers()
            if round_ < 3:
                assert d._version_skew is None
                sworld.advance(WORKER_RESTART_DELAY)
                d._check_workers()
                workers["snapshot"] = d._workers["snapshot"]
        assert d._version_skew is not None and d._reload_epoch.get() == EPOCH_UPGRADE_DRAIN
        assert d._failures == {}
        assert "reported it 3 times in a row" in skew_warnings(caplog)[0]

    def test_a_worker_that_lived_long_enough_resets_the_count(self, sworld):
        d = sworld.daemon
        workers = sworld.start_workers()
        d._skew_reports["snapshot"] = 2
        sworld.advance(WORKER_STABLE_UPTIME)
        workers["snapshot"].die(WORKER_EXIT_VERSION_SKEW)
        d._check_workers()
        assert d._version_skew is None and d._skew_reports["snapshot"] == 1

    def test_a_report_the_supervisor_confirms_drains_at_once(self, sworld, disk, caplog):
        d = sworld.daemon
        workers = sworld.start_workers()
        disk.value = SIG_UPGRADED
        sworld.advance(RELOAD_POLL_SECONDS)
        assert d._check_code_version() is False              # the poll's own first sighting
        sworld.advance(RELOAD_POLL_SECONDS)
        workers["pruning"].die(WORKER_EXIT_VERSION_SKEW)
        d._check_workers()                                   # the worker's report is the second
        assert d._version_skew == (SIG, SIG_UPGRADED)
        (warning,) = skew_warnings(caplog)
        assert "pruning worker" in warning and "1.0.0 -> 1.1.0" in warning
        assert d._failures == {} and d._restart_at == {}

    def test_a_report_with_a_first_sighting_only_restarts_the_worker(self, sworld, disk):
        d = sworld.daemon
        workers = sworld.start_workers()
        disk.value = SIG_UPGRADED
        workers["pruning"].die(WORKER_EXIT_VERSION_SKEW)
        d._check_workers()
        assert d._version_skew is None
        assert "pruning" in d._restart_at and d._skew_candidate is not None

    def test_a_report_during_a_drain_or_a_stop_is_ignored(self, sworld, disk, caplog):
        d = sworld.daemon
        workers = sworld.start_workers()
        d._stop_event.set()
        workers["snapshot"].die(WORKER_EXIT_VERSION_SKEW)
        d._check_workers()
        assert d._version_skew is None and d._restart_at == {} and d._skew_reports == {}

    def test_exit_4_among_reload_turnovers_neither_blocks_them_nor_drains(self, sworld):
        d = sworld.daemon
        workers = sworld.start_workers()
        sworld.write((STORE, 1), config=cfg(remote=True, prefix="landed"))
        sworld.hup()
        forks = list(sworld.forks())
        exited_for_reload({n: workers[n] for n in ("snapshot", "pruning")})
        workers["api"].die(WORKER_EXIT_VERSION_SKEW)
        d._check_workers()
        assert d._version_skew is None
        assert len(sworld.forks()) == len(forks) + 2          # the two turnovers
        assert d._failures == {} and "api" in d._restart_at

    def test_a_crash_loop_detected_first_keeps_exit_1(self, sworld, mocker):
        d = sworld.daemon
        d._failures["snapshot"] = WORKER_MAX_CONSECUTIVE_FAILURES - 1

        def crash():
            d._workers["snapshot"].die(1)

        assert drive(sworld, mocker, crash) == 1
        assert d._crash_looped == {"snapshot"} and d._version_skew is None

    def test_a_crash_loop_and_a_skew_exit_in_the_same_pass_keep_exit_1(self, sworld, mocker):
        d = sworld.daemon
        d._failures["snapshot"] = WORKER_MAX_CONSECUTIVE_FAILURES - 1

        def both():
            d._workers["snapshot"].die(1)                                # earlier in dict order
            d._workers["api"].die(WORKER_EXIT_VERSION_SKEW)

        assert drive(sworld, mocker, both) == 1

    def test_a_crash_loop_stop_during_a_drain_is_exit_1_not_76(self, sworld, disk, mocker):
        d = sworld.daemon

        def crash_loop():
            d._failures["snapshot"] = WORKER_MAX_CONSECUTIVE_FAILURES - 1

        # a drain makes `_check_workers` ignore deaths, so only a crash BEFORE it can loop
        steps = [crash_loop, lambda: d._workers["snapshot"].die(1)]
        assert drive(sworld, mocker, *steps) == 1


# -- the spawn guard and the first fork ----------------------------------------


@pytest.mark.unit
class TestSpawnGuards:
    def test_a_differing_disk_defers_the_spawn_without_a_failure(self, sworld, disk, caplog):
        d = sworld.daemon
        disk.value = SIG_UPGRADED
        assert d._spawn("snapshot") is None
        assert sworld.made == [] and d._failures == {}
        assert d._restart_at == {"snapshot": sworld.clock[0] + WORKER_RESTART_DELAY}
        (line,) = messages(caplog, logging.WARNING, containing="not starting the snapshot worker")
        assert "1.0.0 -> 1.1.0" in line
        assert d._version_skew is None

    def test_the_same_difference_a_poll_later_at_the_next_spawn_drains_instead(self, sworld, disk, caplog):
        d = sworld.daemon
        disk.value = SIG_UPGRADED
        d._spawn("snapshot")
        sworld.advance(RELOAD_POLL_SECONDS)
        assert d._spawn("snapshot") is None
        assert d._version_skew == (SIG, SIG_UPGRADED) and sworld.made == []
        assert "before starting the snapshot worker" in skew_warnings(caplog)[0]

    def test_a_spawn_with_an_equal_or_unreadable_disk_goes_ahead(self, sworld, disk):
        assert sworld.daemon._spawn("snapshot") is not None
        disk.value = None
        assert sworld.daemon._spawn("pruning") is not None
        assert len(sworld.made) == 2

    def test_no_check_without_a_baseline(self, disk, make_world):
        disk.value = None
        world = make_world()
        reads_before = disk.reads
        assert world.daemon._spawn("snapshot") is not None
        assert disk.reads == reads_before

    def test_a_deferred_spawn_is_forked_later_by_the_due_loop(self, sworld, disk):
        d = sworld.daemon
        sworld.start_workers()
        disk.value = SIG_UPGRADED
        d._workers["pruning"].die(1)
        d._check_workers()                                    # crash restart -> deferred by the guard
        assert d._version_skew is None
        disk.value = SIG                                      # the install settled back
        sworld.advance(WORKER_RESTART_DELAY * 4)
        d._check_workers()
        assert d._workers["pruning"].is_alive()


@pytest.mark.unit
class TestStartBlock:
    """Right before the first fork the OLD code decides (the install may have landed while the
    process waited for a previous daemon's workers or read its configuration)."""

    @staticmethod
    def never_hang(d, mocker):
        """If the block is missing the loop would run for ever: end it with an ordinary stop."""
        sleep = mocker.patch.object(d, "_supervisor_sleep", side_effect=lambda s: d._stop_event.set())
        return sleep

    def test_a_difference_read_twice_blocks_every_worker_and_exits_76(self, sworld, disk, srec, mocker):
        d = sworld.daemon
        disk.queue = [SIG_UPGRADED, SIG_UPGRADED]
        wait = mocker.patch.object(d._stop_event, "wait", return_value=False)
        self.never_hang(d, mocker)
        assert d.run() == EXIT_CODE_VERSION_CHANGED
        wait.assert_called_once_with(1.0)
        assert sworld.made == [] and sworld.forks() == []
        assert srec.heads() == ["STOPPING=1"]                  # no READY: nothing started

    def test_a_pending_sighup_does_not_collapse_the_two_reads_into_one_instant(
        self, sworld, disk, mocker
    ):
        """The wait is the stop flag's, not `_supervisor_sleep` (which a pending SIGHUP ends at once)."""
        d = sworld.daemon
        d._hup_requested = True
        disk.queue = [SIG_UPGRADED, SIG_UPGRADED]
        sleep = self.never_hang(d, mocker)
        wait = mocker.patch.object(d._stop_event, "wait", return_value=False)
        assert d.run() == EXIT_CODE_VERSION_CHANGED
        sleep.assert_not_called()
        wait.assert_called_once_with(1.0)

    def test_the_second_read_decides(self, sworld, disk, mocker):
        d = sworld.daemon
        mocker.patch.object(d._stop_event, "wait", return_value=False)
        disk.queue = [SIG_UPGRADED, SIG]                       # a transient error, not an upgrade
        assert drive(sworld, mocker, lambda: None) == 0
        assert len(sworld.made) == 4

    def test_an_unreadable_first_read_starts_normally(self, sworld, disk, mocker):
        disk.queue = [None]
        assert drive(sworld, mocker) == 0
        assert len(sworld.made) == 4

    def test_an_unchanged_disk_costs_one_read_and_no_wait(self, sworld, disk, mocker):
        d = sworld.daemon
        sleep = mocker.patch.object(d, "_supervisor_sleep", side_effect=lambda s: d._stop_event.set())
        d.run()
        assert [c.args for c in sleep.call_args_list][0] == (daemon_module.SUPERVISOR_POLL,)

    def test_a_signature_passed_from_before_the_wait_is_compared_with_the_disk_at_start(
        self, sworld, disk, srec, mocker
    ):
        """The M1 shape without the process: the baseline is older than the disk."""
        d = BackupDaemon(
            sworld.running_config, sworld.resolved, key=(STORE, 0),
            presence_lock=sworld.lock, started_signature=SIG,
        )
        d._notify = srec
        disk.value = SIG_UPGRADED                                # landed while the process waited
        mocker.patch.object(d._stop_event, "wait", return_value=False)
        self.never_hang(d, mocker)
        assert d.run() == EXIT_CODE_VERSION_CHANGED
        assert sworld.made == []


@pytest.mark.unit
class TestStartup:
    def test_the_baseline_is_taken_before_the_wait_for_previous_workers(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit
    ):
        """M1: an upgrade landing during the unbounded wait must be a CHANGE, not the baseline
        of a supervisor that runs the old code."""
        state = {"sig": SIG}
        mocker.patch.object(daemon_module, "code_signature", side_effect=lambda: state["sig"])
        mocker.patch.object(daemon_module, "installed_in_site_packages", return_value=False)
        waited = []

        def wait(resolved):
            waited.append(True)
            state["sig"] = SIG_UPGRADED                         # the install lands during the wait

        mocker.patch.object(daemon_module, "_wait_for_previous_workers", side_effect=wait)
        factory, _ = _stub_daemon(mocker)
        daemon_module._run(_args(sqlite_config_db.path))
        assert waited == [True]
        assert factory.call_args.kwargs["started_signature"] == SIG != state["sig"]

    @pytest.mark.parametrize("value", [None, NOT_INSTALLED], ids=["unreadable", "not-installed"])
    def test_missing_metadata_under_site_packages_exits_76_at_once(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, caplog, value
    ):
        mocker.patch.object(daemon_module, "code_signature", return_value=value)
        mocker.patch.object(daemon_module, "installed_in_site_packages", return_value=True)
        factory, stub = _stub_daemon(mocker)
        with caplog.at_level(logging.ERROR):
            assert daemon_module._run(_args(sqlite_config_db.path)) == EXIT_CODE_VERSION_CHANGED
        factory.assert_not_called()
        assert lock_spy == [], "no lock was taken: nothing started"
        assert "install is probably in progress" in caplog.text
        assert ("signature" if value is None else "metadata (not installed)") in caplog.text

    @pytest.mark.parametrize("value", [None, NOT_INSTALLED], ids=["unreadable", "not-installed"])
    def test_the_same_outside_site_packages_starts_with_the_guard_off(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, value
    ):
        mocker.patch.object(daemon_module, "code_signature", return_value=value)
        mocker.patch.object(daemon_module, "installed_in_site_packages", return_value=False)
        factory, _ = _stub_daemon(mocker)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert factory.call_args.kwargs["started_signature"] is value

    def test_test_config_never_exits_76(self, sqlite_config_db, mocker, no_hard_exit):
        mocker.patch.object(daemon_module, "code_signature", return_value=None)
        mocker.patch.object(daemon_module, "installed_in_site_packages", return_value=True)
        assert daemon_module._run(_args(sqlite_config_db.path, test_config=True)) != EXIT_CODE_VERSION_CHANGED

    def test_a_readable_installed_signature_starts_normally(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit
    ):
        mocker.patch.object(daemon_module, "code_signature", return_value=SIG)
        mocker.patch.object(daemon_module, "installed_in_site_packages", return_value=True)
        factory, _ = _stub_daemon(mocker)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert factory.call_args.kwargs["started_signature"] == SIG


# ---------------------------------------------------------------------------
# 76 on the way out
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExitCodePlumbing:
    def test_run_passes_76_through_and_releases_the_lock(self, sqlite_config_db, mocker, lock_spy):
        _stub_daemon(mocker, run=76)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 76
        assert len(lock_spy) == 1 and not lock_spy[0].held

    def test_main_returns_76(self, sqlite_config_db, mocker, no_hard_exit):
        _stub_daemon(mocker, run=76)
        with patch("sys.argv", ["daemon", "-c", str(sqlite_config_db.path)]):
            assert main() == 76
        no_hard_exit.assert_not_called()

    def test_main_hard_exits_with_76_when_workers_were_abandoned(
        self, sqlite_config_db, mocker, no_hard_exit
    ):
        _stub_daemon(mocker, run=76, abandoned=["snapshot"])
        with patch("sys.argv", ["daemon", "-c", str(sqlite_config_db.path)]):
            main()
        no_hard_exit.assert_called_once_with(76, ["snapshot"])

    def test_hard_exit_keeps_76(self, monkeypatch):
        exits = []
        monkeypatch.setattr(daemon_module.os, "_exit", lambda c: exits.append(c))
        monkeypatch.setattr(daemon_module.logging, "shutdown", lambda: None)
        daemon_module._hard_exit(76, ["snapshot"])
        assert exits == [76]

    def test_the_constants_do_not_collide(self):
        assert WORKER_EXIT_VERSION_SKEW == 4
        assert EXIT_CODE_VERSION_CHANGED == 76
        assert WORKER_EXIT_VERSION_SKEW not in (0, 1, 2, WORKER_EXIT_RELOAD, 255)
        assert WORKER_EXIT_VERSION_SKEW != daemon_module._FORKSERVER_LOST_EXITCODE
        assert EPOCH_UPGRADE_DRAIN < 0


# ---------------------------------------------------------------------------
# The worker's own check
# ---------------------------------------------------------------------------

LOG = logging.getLogger("test_version_skew")


class _Halt(Exception):
    """Ends a worker's `run()` right after the step under test."""


def worker_of(cls, *, signature=SIG, stop=None, **kwargs):
    return _build(cls, make_config(), stop or StopFlag(), code_signature=signature, **kwargs)


@pytest.mark.unit
class TestWorkerCheck:
    @pytest.fixture
    def current(self, mocker):
        """`workers.code_signature()` -- what the worker reads from the disk at its start."""
        return mocker.patch.object(workers_module, "code_signature", return_value=SIG)

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_a_matching_signature_returns(self, cls, current):
        worker_of(cls)._exit_on_version_skew(LOG)
        current.assert_called_once_with()

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_the_baseline_is_stored_as_a_plain_tuple_whatever_was_passed(self, cls):
        assert worker_of(cls, signature=SIG).code_signature == SIG.as_tuple()
        assert worker_of(cls, signature=SIG.as_tuple()).code_signature == SIG.as_tuple()
        assert worker_of(cls, signature=None).code_signature is None

    @pytest.mark.parametrize("cls", ALL_FIVE)
    @pytest.mark.parametrize(
        "now", [SIG_UPGRADED, SIG_REINSTALLED, NOT_INSTALLED, SIG_NEW_PYTHON],
        ids=["upgrade", "reinstall", "gone", "new-python"],
    )
    def test_a_mismatch_exits_4_with_one_warning(self, cls, now, current, caplog):
        current.return_value = now
        w = worker_of(cls)
        with caplog.at_level(logging.WARNING, logger="test_version_skew"):
            with pytest.raises(SystemExit) as exc:
                w._exit_on_version_skew(LOG)
        assert exc.value.code == WORKER_EXIT_VERSION_SKEW == 4
        (record,) = [r for r in caplog.records if r.name == "test_version_skew"]
        assert "changed since the daemon started" in record.getMessage()
        assert "mixed code" in record.getMessage()
        assert now.change_from(SIG) in record.getMessage()

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_unreadable_is_no_change(self, cls, current):
        current.return_value = None
        worker_of(cls)._exit_on_version_skew(LOG)

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_it_checks_even_when_a_stop_is_pending(self, cls, current):
        """The supervisor ignores exit 4 during a stop; the worker still refuses to run mixed
        code (a stop is not a promise to start nothing)."""
        stop = StopFlag()
        stop.set()
        current.return_value = SIG_UPGRADED
        with pytest.raises(SystemExit) as exc:
            worker_of(cls, stop=stop)._exit_on_version_skew(LOG)
        assert exc.value.code == 4

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_without_a_baseline_there_is_no_check(self, cls, current):
        current.return_value = SIG_UPGRADED
        w = worker_of(cls, signature=None)
        w._exit_on_version_skew(LOG)
        current.assert_not_called()

    @pytest.mark.parametrize("garbage", [5, object(), 3.5], ids=["int", "object", "float"])
    def test_a_baseline_that_cannot_be_compared_never_raises(self, current, garbage):
        current.return_value = SIG_UPGRADED
        w = worker_of(workers_module.SnapshotWorker, signature=None)
        w.code_signature = garbage
        w._exit_on_version_skew(LOG)          # returns: "cannot tell" is "no change"

    def test_a_baseline_of_the_wrong_shape_may_only_exit_4(self, current):
        current.return_value = SIG_UPGRADED
        w = worker_of(workers_module.SnapshotWorker, signature=None)
        w.code_signature = ("1.0.0", "/x/RECORD", (1, 2, 3))            # an older, shorter tuple
        with pytest.raises(SystemExit) as exc:
            w._exit_on_version_skew(LOG)
        assert exc.value.code == 4

    def test_a_current_value_that_is_not_a_signature_is_no_change(self, current):
        current.return_value = object()
        worker_of(workers_module.SnapshotWorker)._exit_on_version_skew(LOG)

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_the_baseline_keyword_is_not_positional(self, cls):
        with pytest.raises(TypeError):
            _build(cls, make_config(), StopFlag(), False, None, 0, None, None, SIG, SIG)

    def test_a_worker_built_without_init_has_an_inert_default(self):
        w = workers_module.SnapshotWorker.__new__(workers_module.SnapshotWorker)
        assert w.code_signature is None and w.drains_for_upgrade is False

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_run_checks_after_the_supervisor_watch_and_before_the_workers_lock(self, cls, mocker):
        calls = []
        w = worker_of(cls)
        mocker.patch.object(
            workers_module, "_init_child_process", side_effect=lambda level: calls.append("init")
        )
        mocker.patch.object(w, "_arm_supervisor_watch", side_effect=lambda: calls.append("arm"))
        mocker.patch.object(w, "_exit_on_version_skew", side_effect=lambda lg: calls.append("skew"))

        def hold():
            calls.append("hold")
            raise _Halt

        mocker.patch.object(w, "_hold_workers_lock", side_effect=hold)
        with pytest.raises(_Halt):
            w.run()
        assert calls == ["init", "arm", "skew", "hold"]

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_run_leaves_with_4_before_it_takes_the_lock_or_does_anything(self, cls, current, mocker):
        current.return_value = SIG_UPGRADED
        w = worker_of(cls)
        mocker.patch.object(workers_module, "_init_child_process")
        mocker.patch.object(w, "_arm_supervisor_watch")
        hold = mocker.patch.object(w, "_hold_workers_lock")
        with pytest.raises(SystemExit) as exc:
            w.run()
        assert exc.value.code == 4
        hold.assert_not_called()


@pytest.mark.unit
class TestExitReasonUpgrade:
    @pytest.fixture
    def make(self, mocker):
        def build(cls, born=0):
            epoch = ReloadEpoch()
            w = _build(cls, make_config(), StopFlag(), reload_epoch=epoch, born_epoch=born)
            mocker.patch.object(w, "_supervisor_gone", return_value=False)
            return w, epoch

        return build

    @pytest.mark.parametrize(
        "cls, stage1, stage2",
        [
            (RemoteBackupWorker, ExitReason.UPGRADE, ExitReason.UPGRADE),
            (ApiWorker, None, ExitReason.UPGRADE),                 # the API serves through stage 1
            (workers_module.SnapshotWorker, None, None),
            (workers_module.PruningWorker, None, None),
            ("base", None, None),
        ],
        ids=["remote", "api", "snapshot", "pruning", "base"],
    )
    def test_the_two_stages_of_the_drain_by_worker(self, make, cls, stage1, stage2):
        w, epoch = make(cls)
        epoch.set(EPOCH_UPGRADE_DRAIN)
        assert w._exit_reason() is stage1
        epoch.set(EPOCH_UPGRADE_DRAIN_API)
        assert w._exit_reason() is stage2
        assert (w.upgrade_drain_epoch is not None) is (stage2 is not None)

    def test_the_drain_epochs_and_who_obeys_which(self):
        assert (EPOCH_UPGRADE_DRAIN, EPOCH_UPGRADE_DRAIN_API) == (-1, -2)
        assert RemoteBackupWorker.upgrade_drain_epoch == EPOCH_UPGRADE_DRAIN
        assert ApiWorker.upgrade_drain_epoch == EPOCH_UPGRADE_DRAIN_API
        assert workers_module.SnapshotWorker.upgrade_drain_epoch is None
        assert workers_module.PruningWorker.upgrade_drain_epoch is None
        assert RemoteBackupWorker.drains_for_upgrade and ApiWorker.drains_for_upgrade

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_a_stop_beats_a_drain_and_a_gone_supervisor_beats_it_too(self, make, cls, mocker):
        w, epoch = make(cls)
        epoch.set(EPOCH_UPGRADE_DRAIN_API)
        w._supervisor_gone.return_value = True
        assert w._exit_reason() is ExitReason.SUPERVISOR_GONE
        w.stop_event.set()
        assert w._exit_reason() is ExitReason.STOP

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_an_ordinary_epoch_change_is_still_a_reload(self, make, cls):
        w, epoch = make(cls)
        epoch.set(5)
        assert w._exit_reason() is ExitReason.RELOAD

    def test_the_drain_value_is_distinct_from_every_real_epoch(self):
        assert EPOCH_UPGRADE_DRAIN == -1 and ReloadEpoch().get() == 0

    @pytest.mark.parametrize(
        "setup, name",
        [
            (lambda w, e: w.stop_event.set(), "stop"),
            (lambda w, e: e.set(3), "reload"),
            (lambda w, e: e.set(EPOCH_UPGRADE_DRAIN), "drain"),
        ],
    )
    def test_before_loop_is_skipped_once_a_reason_to_leave_is_pending(self, mocker, setup, name):
        epoch = ReloadEpoch()
        w = RemoteBackupWorker(make_config(), StopFlag(), reload_epoch=epoch, born_epoch=0)
        mocker.patch.object(workers_module, "_init_child_process")
        mocker.patch.object(workers_module, "DatasetManager")
        mocker.patch.object(w, "_arm_supervisor_watch")
        mocker.patch.object(w, "_supervisor_gone", return_value=False)
        mocker.patch.object(w, "_hold_workers_lock")
        mocker.patch.object(w, "_open_status", return_value=None)
        before = mocker.patch.object(w, "_before_loop")
        mocker.patch.object(w, "_get_datasets", side_effect=_Halt)      # never loop for ever
        setup(w, epoch)
        try:
            w.run()
        except (SystemExit, _Halt):
            pass                                    # a reload exits 3
        before.assert_not_called()

    def test_before_loop_runs_when_nothing_is_pending_control(self, mocker):
        w = RemoteBackupWorker(make_config(), StopFlag(), reload_epoch=ReloadEpoch())
        mocker.patch.object(workers_module, "_init_child_process")
        mocker.patch.object(workers_module, "DatasetManager")
        mocker.patch.object(w, "_arm_supervisor_watch")
        mocker.patch.object(w, "_supervisor_gone", return_value=False)
        mocker.patch.object(w, "_hold_workers_lock")
        mocker.patch.object(w, "_open_status", return_value=None)
        before = mocker.patch.object(w, "_before_loop")
        mocker.patch.object(w, "_get_datasets", side_effect=_Halt)
        with pytest.raises(_Halt):
            w.run()
        before.assert_called_once()

    @pytest.mark.parametrize("cls", [RemoteBackupWorker, workers_module.SnapshotWorker])
    def test_finishing_for_an_upgrade_returns_normally_exit_0_not_3(self, cls, caplog):
        w = _build(cls, make_config(), StopFlag())
        with caplog.at_level(logging.INFO, logger="test_version_skew"):
            w._finish(ExitReason.UPGRADE, LOG)            # returns: no SystemExit
        assert "restart on the upgraded zfsbackup" in caplog.text
        with pytest.raises(SystemExit) as exc:
            w._finish(ExitReason.RELOAD, LOG)
        assert exc.value.code == WORKER_EXIT_RELOAD


# ---------------------------------------------------------------------------
# Real worker processes
# ---------------------------------------------------------------------------


def _record(rec: Path, name: str):
    path = rec / f"{name}.json"
    return json.loads(path.read_text()) if path.exists() else None


def _events(rec: Path):
    try:
        return (rec / "events").read_text().split()
    except FileNotFoundError:
        return []


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
@pytest.mark.parametrize("method", START_METHODS)
class TestRealWorkerProcesses:
    @pytest.fixture
    def started(self, tmp_path):
        made = []

        def start(cls, method, signature=None, *, stop=None, epoch=None, **attrs):
            rec = tmp_path / f"rec-{cls.__name__}-{method}"
            rec.mkdir()
            stop = stop if stop is not None else StopFlag()
            kwargs = {"code_signature": signature}
            if epoch is not None:
                kwargs.update(reload_epoch=epoch, born_epoch=0)
            worker = cls(make_config(), stop, **kwargs)
            worker.start_method = method
            worker.record_dir = str(rec)
            worker.release_path = str(rec / "release")
            for key, value in attrs.items():
                setattr(worker, key, value)
            made.append((worker, rec))
            worker.start()
            return worker, rec, stop

        yield start
        for worker, rec in made:
            (rec / "release").touch()
            worker.join(15)
            if worker.is_alive():
                worker.kill()
                worker.join(10)

    @pytest.mark.parametrize("kind", sorted(PROBES))
    def test_a_stale_baseline_exits_4_before_the_lazy_imports(self, started, method, kind):
        worker, rec, _ = started(PROBES[kind], method, SIG.as_tuple())
        worker.join(60)
        assert worker.exitcode == WORKER_EXIT_VERSION_SKEW, f"{kind}/{method}"
        seen = _record(rec, worker.name)
        assert seen == {m: False for m in LAZY_MODULES}, (
            f"{kind}/{method}: the version check must run before zfsbackup.remote / "
            f"zfsbackup.api are imported; imported at exit: {seen}"
        )

    def test_a_stale_baseline_exits_4_even_with_a_stop_already_pending(self, started, method):
        stop = StopFlag()
        stop.set()
        worker, rec, _ = started(ProbeGatedSnapshotWorker, method, SIG.as_tuple(), stop=stop)
        worker.join(60)
        assert worker.exitcode == WORKER_EXIT_VERSION_SKEW

    def test_a_matching_baseline_runs_and_a_stop_is_a_clean_exit(self, started, method):
        worker, rec, stop = started(ProbeGatedSnapshotWorker, method, code_signature().as_tuple())
        assert wait_until(lambda: "start:pool/a" in _events(rec), 60), \
            f"{method}: a worker whose signature matches must start its work"
        assert worker.is_alive() and worker.exitcode is None
        stop.set()
        (rec / "release").touch()
        worker.join(30)
        assert worker.exitcode == 0

    def test_a_remote_worker_finishes_its_dataset_for_a_drain_and_exits_0_not_3(self, started, method):
        epoch = ReloadEpoch()
        worker, rec, _ = started(GatedRemoteWorker, method, None, epoch=epoch)
        assert wait_until(lambda: "start:pool/a" in _events(rec), 60)
        epoch.set(EPOCH_UPGRADE_DRAIN)
        time.sleep(workers_module.STOP_POLL * 3)
        assert worker.is_alive(), "a drain never interrupts the dataset in hand"
        (rec / "release").touch()
        worker.join(30)
        assert worker.exitcode == 0
        assert _events(rec) == ["start:pool/a", "done:pool/a"], "no further dataset is started"

    def test_a_snapshot_worker_ignores_a_drain_and_keeps_working_until_stopped(self, started, method):
        epoch = ReloadEpoch()
        worker, rec, stop = started(ProbeGatedSnapshotWorker, method, None, epoch=epoch)
        assert wait_until(lambda: "start:pool/a" in _events(rec), 60)
        epoch.set(EPOCH_UPGRADE_DRAIN)
        (rec / "release").touch()
        assert wait_until(lambda: "done:pool/b" in _events(rec), 60), \
            "the snapshot worker carries on through the drain"
        assert worker.is_alive()
        stop.set()
        worker.join(30)
        assert worker.exitcode == 0


# ---------------------------------------------------------------------------
# A real daemon
# ---------------------------------------------------------------------------


class SkewDaemon(NotifyDaemon):
    """A `NotifyDaemon` whose `zfsbackup` distribution metadata is a throwaway dist-info that
    comes FIRST on `PYTHONPATH`, so a test can reinstall it under the running daemon."""

    def __init__(self, tmp_path, monkeypatch, *, notify_socket, method):
        super().__init__(tmp_path, monkeypatch, notify_socket=notify_socket, method=method)
        self.site = tmp_path / "fake-site"
        self.site.mkdir()
        self.dist_info = make_dist(self.site, "9.9.9")
        old = self.env.get("PYTHONPATH")
        self.env["PYTHONPATH"] = os.pathsep.join([str(self.site)] + ([old] if old else []))


@pytest.fixture
def skew_daemon(tmp_path, monkeypatch, sockdir):
    made, servers = [], []

    def make(method="default"):
        if method != "default":
            method_or_skip(method)
        server = Server(os.path.join(sockdir, "s.sock"))
        servers.append(server)
        d = SkewDaemon(tmp_path, monkeypatch, notify_socket=server.address, method=method)
        d.import_config()
        made.append(d)
        return d, server

    yield make
    for d in made:
        d.stop()
    for s in servers:
        s.close()


def no_survivors(d, timeout=20):
    deadline = time.monotonic() + timeout
    while process_group_members(d.supervisor) and time.monotonic() < deadline:
        time.sleep(0.2)
    return process_group_members(d.supervisor) == []


@pytest.mark.integration
@pytest.mark.slow
class TestRealDaemon:
    @pytest.mark.parametrize("method", START_METHODS)
    def test_replacing_the_record_drains_and_exits_76(self, skew_daemon, method):
        d, srv = skew_daemon(method)
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        d.wait_ready()
        assert d.info().draining is None
        replace_record(d.dist_info)
        t0 = time.monotonic()
        got = take(srv, d, lambda m: m.startswith("STOPPING=1"), 90)
        assert d.proc.wait(timeout=90) == EXIT_CODE_VERSION_CHANGED, d.log()
        # one poll to see it, one to confirm it, then the drain: well under a minute
        assert time.monotonic() - t0 < 60
        assert got[-1] == "STOPPING=1"
        statuses = [m for m in got if m.startswith("STATUS=zfsbackup upgraded")]
        assert len(statuses) == 1 and "draining" in statuses[0]
        log = d.log()
        assert log.count(CHANGED) == 1 and "Daemon stopped" in log
        assert "crashed repeatedly" not in log
        assert no_survivors(d)

    def test_a_sighup_with_the_difference_pending_does_not_start_a_reload_after_the_drain(
        self, skew_daemon
    ):
        d, srv = skew_daemon()
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        d.wait_ready()
        replace_record(d.dist_info)
        os.kill(d.supervisor, signal.SIGHUP)           # the first sighting, at once
        time.sleep(1.0)
        os.kill(d.supervisor, signal.SIGHUP)           # (< a poll later: not yet a confirmation)
        assert d.proc.wait(timeout=90) == EXIT_CODE_VERSION_CHANGED, d.log()
        assert d.log().count(CHANGED) == 1
        assert no_survivors(d)

    def test_a_sigterm_after_the_replacement_is_the_ordinary_exit(self, skew_daemon):
        d, srv = skew_daemon()
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        d.wait_ready()
        replace_record(d.dist_info)
        assert d.terminate() == 0, d.log()
        assert CHANGED not in d.log()
        assert no_survivors(d)

    def test_removing_the_distribution_is_a_change_too(self, skew_daemon):
        d, srv = skew_daemon()
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        d.wait_ready()
        for child in d.dist_info.iterdir():
            child.unlink()
        d.dist_info.rmdir()
        assert d.proc.wait(timeout=90) == EXIT_CODE_VERSION_CHANGED, d.log()
        log = d.log()
        assert CHANGED in log
        # with no other install the daemon says "removed"; with the real editable install
        # further down sys.path it says which version took over
        assert "being reinstalled or was removed" in log or "9.9.9 ->" in log
        assert no_survivors(d)

    def test_a_worker_killed_during_an_upgrade_does_not_wedge_or_crash_loop_the_daemon(
        self, skew_daemon
    ):
        d, srv = skew_daemon("spawn" if "spawn" in START_METHODS else "default")
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        pids = d.wait_ready()
        replace_record(d.dist_info)
        os.kill(pids["pruning"], signal.SIGKILL)        # its replacement starts into the new code
        assert d.proc.wait(timeout=120) == EXIT_CODE_VERSION_CHANGED, d.log()
        assert "crashed repeatedly" not in d.log()
        assert no_survivors(d)

    def test_the_pid_file_shows_the_drain_and_a_sighup_is_answered_but_not_applied(
        self, skew_daemon
    ):
        """The API worker is stopped (SIGSTOP), so it cannot reach its safe point: the drain stays
        open and everything about it can be observed deterministically."""
        d, srv = skew_daemon()
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        pids = d.wait_ready()
        os.kill(pids["api"], signal.SIGSTOP)
        try:
            replace_record(d.dist_info)
            d.wait_for_regex(CHANGED, 90)
            time.sleep(0.5)
            assert d.proc.poll() is None, "a drain waits for the API worker, however long it takes"
            info = d.info()
            assert info.pid == d.supervisor
            assert isinstance(info.draining, int) and info.draining > 0
            assert "zfsbackup upgraded" in info.draining_reason
            assert d.worker_pids() == pids, "nothing was respawned, snapshot and pruning still run"

            d.raw_update("snapshot_prefix = 'later'")
            os.kill(d.supervisor, signal.SIGHUP)
            d.wait_for_regex(r"SIGHUP while draining for the zfsbackup upgrade", 20)
            got = take(srv, d, lambda m: m.startswith("READY=1"), 20)
            assert any(m.startswith("RELOADING=1") for m in got), got
            assert got[-1].startswith("READY=1") and "zfsbackup upgraded" in got[-1]
            info = d.info()
            assert info.deferred is not None and "upgrade draining" in (info.deferred_reason or "")
            assert "Reloading:" not in d.log()
            assert d.proc.poll() is None
        finally:
            os.kill(pids["api"], signal.SIGCONT)
        assert d.proc.wait(timeout=90) == EXIT_CODE_VERSION_CHANGED, d.log()
        assert no_survivors(d)

    def test_a_sigterm_during_the_drain_is_the_ordinary_stop(self, skew_daemon):
        d, srv = skew_daemon()
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        pids = d.wait_ready()
        os.kill(pids["api"], signal.SIGSTOP)
        try:
            replace_record(d.dist_info)
            d.wait_for_regex(CHANGED, 90)
            os.kill(d.supervisor, signal.SIGTERM)
            d.wait_for_regex(r"Received SIGTERM", 20)
        finally:
            os.kill(pids["api"], signal.SIGCONT)
        assert d.proc.wait(timeout=90) == 0, d.log()
        assert no_survivors(d)

"""`zfsbackup.procident` (plan item 18.0): process start tokens, identity, and
`flock` write-holders.

These back the verified SIGHUP in `daemonlock.request_reload` and the workers'
orphan watch. The contract that matters: **never raises**, and "cannot tell" is
`None` / `'unknown'`, never a guess -- callers treat it as "do not act".

The platform dispatch is checked at call time (`sys.platform`, `_PROC_ROOT`), so
the Linux parsers are exercised on every host against a fixture tree, and the
macOS sysctl path against a fake libc. Only the "real process" tests are
platform-specific:

* [L] a real child holding `LOCK_EX` is listed by `flock_write_holders`;
* [M] `start_token` agrees with `ps -o lstart=`.

Every process started here is a helper this module spawned; each is killed and
reaped in a `finally`.
"""

import ast
import builtins
import ctypes
import errno
import fcntl
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from zfsbackup import procident

from _hermetic import hermetic_env

IS_LINUX = sys.platform.startswith("linux")
IS_DARWIN = sys.platform == "darwin"
HAS_TOKENS = IS_LINUX or IS_DARWIN
IS_ROOT = hasattr(os, "geteuid") and os.geteuid() == 0

REPO_ROOT = Path(__file__).resolve().parent.parent

needs_tokens = pytest.mark.skipif(not HAS_TOKENS, reason="start tokens exist only on Linux and macOS")
linux_only = pytest.mark.skipif(
    not (IS_LINUX and os.path.exists("/proc/locks")), reason="needs Linux /proc/locks"
)
darwin_only = pytest.mark.skipif(not IS_DARWIN, reason="needs macOS")


def _spawn_sleeper(code="import time; time.sleep(120)"):
    return subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=hermetic_env(),
        cwd=str(REPO_ROOT),
    )


def _reap(proc):
    if proc is None:
        return
    try:
        proc.kill()
    except (ProcessLookupError, OSError):
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:  # pragma: no cover
        pass
    for stream in (proc.stdout, proc.stdin):
        if stream is not None:
            try:
                stream.close()
            except OSError:
                pass


@pytest.fixture
def sleeper():
    procs = []

    def start(code="import time; time.sleep(120)"):
        proc = _spawn_sleeper(code)
        procs.append(proc)
        return proc

    yield start
    for p in procs:
        _reap(p)


# ---------------------------------------------------------------------------
# Real processes (macOS and Linux)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@needs_tokens
class TestRealProcessTokens:
    def test_own_token_is_non_none_and_stable(self):
        first = procident.start_token(os.getpid())
        assert first
        assert procident.start_token(os.getpid()) == first
        assert procident.start_token(os.getpid()) == first

    def test_token_is_a_printable_single_word(self):
        token = procident.start_token(os.getpid())
        assert token.isprintable() and not any(c.isspace() for c in token)
        assert token.startswith(("linux:", "darwin:"))

    def test_a_childs_token_differs_from_ours(self, sleeper):
        child = sleeper()
        child_token = procident.start_token(child.pid)
        assert child_token
        assert child_token != procident.start_token(os.getpid())

    def test_two_children_have_different_tokens_or_pids(self, sleeper):
        a, b = sleeper(), sleeper()
        assert a.pid != b.pid
        assert procident.start_token(a.pid) and procident.start_token(b.pid)

    def test_a_reaped_child_is_gone(self, sleeper):
        child = sleeper()
        token = procident.start_token(child.pid)
        assert token
        assert procident.identity(child.pid, token) == "match"
        child.kill()
        child.wait(timeout=10)
        assert procident.identity(child.pid, token) == "gone"
        assert procident.start_token(child.pid) is None

    def test_identity_matches_the_live_process(self, sleeper):
        child = sleeper()
        token = procident.start_token(child.pid)
        assert procident.identity(child.pid, token) == "match"
        assert procident.identity(os.getpid(), procident.start_token(os.getpid())) == "match"

    def test_identity_mismatch_for_another_processs_token(self, sleeper):
        child = sleeper()
        ours = procident.start_token(os.getpid())
        assert procident.identity(child.pid, ours) == "mismatch"

    def test_identity_mismatch_for_a_bogus_token(self):
        assert procident.identity(os.getpid(), "bogus:1") == "mismatch"
        assert procident.identity(os.getpid(), "linux:0") == "mismatch"

    def test_a_pid_that_does_not_exist_is_gone(self):
        # a pid just past the kernel limit can never be a process
        assert procident.identity(2**22 + 12345, "linux:1") in ("gone", "unknown")
        assert procident.start_token(2**22 + 12345) is None


# ---------------------------------------------------------------------------
# Inputs that must never be acted on, and the never-raises contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestIdentityRefusesJunk:
    @pytest.mark.parametrize("pid", [None, 0, 1, -1, -(2**40), True, False, "1234", 12.5, [], b"1"])
    def test_pid_that_is_not_a_real_pid_is_unknown(self, pid):
        assert procident.identity(pid, "linux:123") == "unknown"
        assert procident.identity(pid, "darwin:123.000000") == "unknown"

    @pytest.mark.parametrize("token", [None, "", 0, b"", []])
    def test_empty_token_is_unknown(self, token):
        assert procident.identity(os.getpid(), token) == "unknown"

    def test_pid_one_is_unknown_even_with_its_real_token(self):
        # pid 1 is never a target, whatever its token says
        token = procident.start_token(1)
        assert procident.identity(1, token or "x") == "unknown"

    @pytest.mark.parametrize("pid", [None, 0, -1, True, "1", 1.5, object()])
    def test_start_token_of_junk_is_none_and_never_raises(self, pid):
        assert procident.start_token(pid) is None

    @pytest.mark.parametrize("pid", [2**70, 2**32, 2**31, 2**31 + 5, 2**32 + 12345])
    def test_a_pid_beyond_the_c_int_range_names_no_process(self, pid):
        """`0 < pid <= 2**31 - 1`: ctypes would otherwise wrap it to another process."""
        assert procident.start_token(pid) is None
        assert procident.identity(pid, "darwin:1.000000") == "unknown"

    @pytest.mark.parametrize("args", [(None, None), (object(), object()), ("x", "y"), (2**200, "t")])
    def test_identity_never_raises(self, args):
        assert procident.identity(*args) in ("match", "mismatch", "gone", "unknown")

    @pytest.mark.parametrize("fd", [None, "5", 1.5, object(), [], True, False, -1, -5, 2**40, 987654])
    def test_flock_write_holders_of_a_non_fd_is_none_and_never_raises(self, fd):
        assert procident.flock_write_holders(fd) is None

    def test_identity_survives_an_internal_failure(self, monkeypatch):
        def boom(pid):
            raise RuntimeError("boom")

        monkeypatch.setattr(procident, "_linux_token", boom)
        monkeypatch.setattr(procident, "_darwin_token", boom)
        assert procident.identity(os.getpid(), "linux:1") == "unknown"
        assert procident.start_token(os.getpid()) is None

    def test_flock_write_holders_survives_an_internal_failure(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")

        def boom(inode):
            raise RuntimeError("boom")

        monkeypatch.setattr(procident, "_linux_flock_write_holders", boom)
        assert procident.flock_write_holders(5) is None


@pytest.mark.unit
class TestUnsupportedPlatform:
    @pytest.fixture(autouse=True)
    def _freebsd(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "freebsd14")

    def test_start_token_is_none(self):
        assert procident.start_token(os.getpid()) is None

    def test_identity_is_unknown(self):
        assert procident.identity(os.getpid(), "linux:123") == "unknown"
        assert procident.identity(os.getpid(), "darwin:1.000000") == "unknown"

    def test_holders_are_none_not_empty(self):
        assert procident.flock_write_holders(1234) is None

    def test_win32_is_equally_unsupported(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "win32")
        assert procident.start_token(os.getpid()) is None
        assert procident.identity(os.getpid(), "x") == "unknown"
        assert procident.flock_write_holders(1) is None

    def test_the_platform_is_read_at_call_time(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")
        assert procident._is_linux() and not procident._is_darwin()
        monkeypatch.setattr(sys, "platform", "darwin")
        assert procident._is_darwin() and not procident._is_linux()


# ---------------------------------------------------------------------------
# Linux /proc/<pid>/stat parsing, against a fixture tree (any host)
# ---------------------------------------------------------------------------


def _stat_line(pid, comm, starttime, *, state="S"):
    """`pid (comm) state` then fields 4..21 (18 of them), then field 22."""
    zeros = " ".join(["0"] * 18)
    return f"{pid} ({comm}) {state} {zeros} {starttime} 0 0 0\n"


@pytest.fixture
def proc_root(tmp_path, monkeypatch):
    root = tmp_path / "proc"
    root.mkdir()
    monkeypatch.setattr(procident, "_PROC_ROOT", str(root))
    monkeypatch.setattr(sys, "platform", "linux")
    return root


def _write_stat(root, pid, text):
    d = root / str(pid)
    d.mkdir(exist_ok=True)
    (d / "stat").write_bytes(text if isinstance(text, bytes) else text.encode())


@pytest.mark.unit
class TestLinuxStatParser:
    def test_simple_comm(self, proc_root):
        _write_stat(proc_root, 4242, _stat_line(4242, "python", 987654))
        assert procident.start_token(4242) == "linux:987654"

    def test_field_22_is_the_one_used(self, proc_root):
        # Fields 4..21 hold distinct values that must not be mistaken for it.
        vals = " ".join(str(100 + i) for i in range(18))
        _write_stat(proc_root, 77, f"77 (x) R {vals} 555 9 9\n")
        assert procident.start_token(77) == "linux:555"

    def test_comm_with_spaces(self, proc_root):
        _write_stat(proc_root, 300, _stat_line(300, "my daemon thing", 31337))
        assert procident.start_token(300) == "linux:31337"

    def test_comm_with_a_closing_paren_and_an_opening_one(self, proc_root):
        _write_stat(proc_root, 301, _stat_line(301, "a) (b c", 24680))
        assert procident.start_token(301) == "linux:24680"

    def test_comm_that_looks_like_stat_fields(self, proc_root):
        _write_stat(proc_root, 302, _stat_line(302, "x) S 1 2 3 99999 (y", 111))
        assert procident.start_token(302) == "linux:111"

    def test_comm_ending_in_a_paren(self, proc_root):
        _write_stat(proc_root, 303, _stat_line(303, "weird)", 222))
        assert procident.start_token(303) == "linux:222"

    def test_no_trailing_newline(self, proc_root):
        _write_stat(proc_root, 304, _stat_line(304, "p", 333).rstrip("\n"))
        assert procident.start_token(304) == "linux:333"

    def test_identity_match_and_mismatch(self, proc_root):
        _write_stat(proc_root, 500, _stat_line(500, "d", 1000))
        assert procident.identity(500, "linux:1000") == "match"
        assert procident.identity(500, "linux:1001") == "mismatch"
        assert procident.identity(500, "darwin:1000.000000") == "mismatch"

    def test_pid_reuse_is_a_mismatch(self, proc_root):
        _write_stat(proc_root, 600, _stat_line(600, "old", 100))
        token = procident.start_token(600)
        _write_stat(proc_root, 600, _stat_line(600, "new", 200))
        assert procident.identity(600, token) == "mismatch"

    @pytest.mark.parametrize(
        "text",
        [
            "700 (comm) S 0 0 0",  # truncated before field 22
            "700 (comm)",  # nothing after comm
            "700 (comm) ",
            "700 comm S 0 0",  # no parentheses at all
            "",
            "\n",
            "700 (comm) S " + " ".join(["0"] * 18) + " notanumber 0\n",  # non-numeric starttime
            "700 (comm) S " + " ".join(["0"] * 18) + " -5 0\n",  # negative is not digits
            "700 (comm) S " + " ".join(["0"] * 18) + " 12.5 0\n",
        ],
    )
    def test_malformed_stat_is_unknown(self, proc_root, text):
        _write_stat(proc_root, 700, text)
        assert procident.start_token(700) is None
        assert procident.identity(700, "linux:1") == "unknown"

    def test_non_ascii_junk_is_unknown_not_a_crash(self, proc_root):
        _write_stat(proc_root, 701, b"\xff\xfe\x00 (\xff) \xff\n")
        assert procident.start_token(701) is None
        assert procident.identity(701, "linux:1") == "unknown"

    def test_a_missing_pid_is_gone_when_signal_zero_agrees(self, proc_root, monkeypatch):
        monkeypatch.setattr(procident, "_kill0", lambda pid: "gone")
        assert procident.identity(31999, "linux:1") == "gone"
        assert procident.start_token(31999) is None

    def test_a_missing_proc_root_is_gone_for_the_pid(self, proc_root, monkeypatch):
        monkeypatch.setattr(procident, "_PROC_ROOT", str(proc_root / "does-not-exist"))
        monkeypatch.setattr(procident, "_kill0", lambda pid: "gone")
        assert procident.identity(31999, "linux:1") == "gone"

    def test_process_lookup_error_is_gone(self, proc_root, monkeypatch):
        # a task that exits between open() and read() reports ESRCH
        def fake_open(path, *a, **kw):
            raise ProcessLookupError(errno.ESRCH, "No such process")

        monkeypatch.setattr(procident, "open", fake_open, raising=False)
        monkeypatch.setattr(procident, "_kill0", lambda pid: "gone")
        assert procident.identity(800, "linux:1") == "gone"

    @pytest.mark.parametrize(
        "exc",
        [
            PermissionError(errno.EACCES, "denied"),
            OSError(errno.EIO, "io error"),
            OSError(errno.EMFILE, "too many open files"),
            IsADirectoryError(errno.EISDIR, "is a directory"),
        ],
    )
    def test_permission_and_other_oserrors_are_unknown(self, proc_root, monkeypatch, exc):
        def fake_open(path, *a, **kw):
            raise exc

        monkeypatch.setattr(procident, "open", fake_open, raising=False)
        assert procident.identity(900, "linux:1") == "unknown"
        assert procident.start_token(900) is None

    def test_a_real_unreadable_file_is_unknown(self, proc_root):
        if IS_ROOT:
            pytest.skip("root reads any file")
        _write_stat(proc_root, 901, _stat_line(901, "d", 5))
        (proc_root / "901" / "stat").chmod(0)
        assert procident.identity(901, "linux:5") == "unknown"

    def test_stat_that_is_a_directory_is_unknown(self, proc_root):
        (proc_root / "902" / "stat").mkdir(parents=True)
        assert procident.identity(902, "linux:5") == "unknown"

    def test_a_read_failure_after_open_is_unknown(self, proc_root, monkeypatch):
        class Bad:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                raise OSError(errno.EIO, "io")

        monkeypatch.setattr(procident, "open", lambda *a, **kw: Bad(), raising=False)
        assert procident.identity(903, "linux:5") == "unknown"

    def test_the_token_has_no_pid_or_boot_component(self, proc_root):
        _write_stat(proc_root, 1000, _stat_line(1000, "d", 42))
        assert procident.start_token(1000) == "linux:42"


# ---------------------------------------------------------------------------
# A zombie is gone
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLinuxZombies:
    @pytest.mark.parametrize("state", ["Z", "X"])
    def test_exited_states_are_gone_for_every_question(self, proc_root, state):
        _write_stat(proc_root, 4000, _stat_line(4000, "dead", 555, state=state))
        assert procident.start_token(4000) is None
        assert procident.identity(4000, "linux:555") == "gone"      # the token would have matched
        assert procident.is_zombie(4000) is True

    @pytest.mark.parametrize("state", ["S", "R", "D", "T", "I", "t"])
    def test_live_states_are_not_zombies(self, proc_root, state):
        _write_stat(proc_root, 4001, _stat_line(4001, "live", 556, state=state))
        assert procident.is_zombie(4001) is False
        assert procident.start_token(4001) == "linux:556"
        assert procident.identity(4001, "linux:556") == "match"

    def test_a_zombie_with_a_comm_full_of_parens_is_still_a_zombie(self, proc_root):
        _write_stat(proc_root, 4002, _stat_line(4002, "a) (Z) S", 557, state="Z"))
        assert procident.is_zombie(4002) is True

    def test_a_comm_that_looks_like_a_zombie_state_is_not_one(self, proc_root):
        _write_stat(proc_root, 4003, _stat_line(4003, "x) Z (y", 558, state="S"))
        assert procident.is_zombie(4003) is False

    def test_unreadable_is_none(self, proc_root, monkeypatch):
        def deny(path, *a, **kw):
            raise PermissionError(errno.EACCES, "denied")

        monkeypatch.setattr(procident, "open", deny, raising=False)
        assert procident.is_zombie(4004) is None

    @pytest.mark.parametrize("answer", ["unknown", "gone"])
    def test_hidden_or_missing_is_none_not_false(self, proc_root, monkeypatch, answer):
        monkeypatch.setattr(procident, "_kill0", lambda pid: answer)
        assert procident.is_zombie(4005) is None

    def test_a_malformed_stat_is_none(self, proc_root):
        _write_stat(proc_root, 4006, "4006 (x) Z 0 0")
        assert procident.is_zombie(4006) is None

    @pytest.mark.parametrize("pid", [None, 0, -1, True, "1", 2**31, 1.5])
    def test_an_invalid_pid_is_none(self, pid):
        assert procident.is_zombie(pid) is None

    def test_other_platforms_are_none(self, proc_root, monkeypatch):
        _write_stat(proc_root, 4007, _stat_line(4007, "dead", 5, state="Z"))
        monkeypatch.setattr(sys, "platform", "freebsd14")
        assert procident.is_zombie(4007) is None
        assert procident.identity(4007, "linux:5") == "unknown"


@pytest.mark.integration
@needs_tokens
class TestRealZombie:
    """Both platforms: an exited child nobody has reaped is gone."""

    def _zombie(self):
        proc = subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.stdin.read()"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=hermetic_env(), cwd=str(REPO_ROOT),
        )
        token = None
        deadline = time.monotonic() + 30
        while token is None and time.monotonic() < deadline:
            token = procident.start_token(proc.pid)
            time.sleep(0.02)
        assert token
        proc.stdin.close()                 # exits now; NOT reaped (no wait/poll)
        deadline = time.monotonic() + 30
        while procident.is_zombie(proc.pid) is not True and time.monotonic() < deadline:
            time.sleep(0.02)
        return proc, token

    def test_an_unreaped_child_is_a_zombie_and_gone(self):
        proc, token = self._zombie()
        try:
            assert procident.is_zombie(proc.pid) is True
            assert procident.identity(proc.pid, token) == "gone"
            assert procident.start_token(proc.pid) is None
            assert os.kill(proc.pid, 0) is None, "signal 0 still succeeds on a zombie: why 'gone' needs more"
        finally:
            proc.wait(timeout=10)

    def test_once_reaped_it_is_gone_but_not_a_zombie(self):
        proc, token = self._zombie()
        proc.wait(timeout=10)
        assert procident.identity(proc.pid, token) == "gone"
        assert procident.is_zombie(proc.pid) is None

    def test_a_live_child_is_not_a_zombie(self, sleeper):
        child = sleeper()
        assert procident.is_zombie(child.pid) is False
        assert procident.is_zombie(os.getpid()) is False


# ---------------------------------------------------------------------------
# Why the lock holders could not be determined
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFlockHoldersUnavailableReason:
    def test_the_codes(self):
        assert procident.UNAVAILABLE_NOT_LINUX == "not-linux"
        assert procident.UNAVAILABLE_MOUNT_DEVICE == "mount-device"
        assert procident.UNAVAILABLE_LOCKS_UNREADABLE == "locks-unreadable"
        assert procident.UNAVAILABLE_UNKNOWN == "unknown"

    @pytest.mark.parametrize("platform", ["darwin", "freebsd14", "win32"])
    def test_not_linux(self, table, monkeypatch, platform):
        monkeypatch.setattr(sys, "platform", platform)
        assert procident.flock_write_holders(table.fd) is None
        assert procident.flock_holders_unavailable_reason(table.fd) == procident.UNAVAILABLE_NOT_LINUX

    def test_not_linux_wins_over_a_bad_fd(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "darwin")
        assert procident.flock_holders_unavailable_reason(-1) == procident.UNAVAILABLE_NOT_LINUX

    def test_the_mount_device_cannot_be_resolved(self, table):
        table.set_fdinfo("pos:\t0\nino:\t5\n")                  # no mnt_id
        assert table.holders() is None
        assert procident.flock_holders_unavailable_reason(table.fd) == procident.UNAVAILABLE_MOUNT_DEVICE

    def test_the_mount_is_not_listed(self, table):
        table.set_mount("8:1", mnt_id=MNT_ID + 1)
        assert procident.flock_holders_unavailable_reason(table.fd) == procident.UNAVAILABLE_MOUNT_DEVICE

    def test_the_lock_table_is_unreadable(self, table):
        (table.root / "locks").unlink()
        assert table.holders() is None
        assert procident.flock_holders_unavailable_reason(table.fd) == procident.UNAVAILABLE_LOCKS_UNREADABLE

    def test_a_denied_lock_table(self, table, monkeypatch):
        real = builtins.open

        def fake_open(path, *a, **kw):
            if str(path).endswith("/locks"):
                raise PermissionError(errno.EACCES, "denied")
            return real(path, *a, **kw)

        monkeypatch.setattr(procident, "open", fake_open, raising=False)
        assert procident.flock_holders_unavailable_reason(table.fd) == procident.UNAVAILABLE_LOCKS_UNREADABLE

    def test_the_mount_device_is_checked_before_the_lock_table(self, table):
        (table.root / "locks").unlink()
        (table.root / "self" / "mountinfo").unlink()
        assert procident.flock_holders_unavailable_reason(table.fd) == procident.UNAVAILABLE_MOUNT_DEVICE

    @pytest.mark.parametrize("fd", [-1, None, True, "3", 1.5])
    def test_a_bad_fd_is_unknown(self, table, fd):
        assert procident.flock_holders_unavailable_reason(fd) == procident.UNAVAILABLE_UNKNOWN

    def test_anything_else_is_unknown(self, table):
        """Called when there is nothing wrong at all (it re-derives)."""
        assert table.holders() == {1001}
        assert procident.flock_holders_unavailable_reason(table.fd) == procident.UNAVAILABLE_UNKNOWN

    def test_it_never_raises(self, table, monkeypatch):
        monkeypatch.setattr(procident, "_linux_fd_device", lambda fd: (_ for _ in ()).throw(RuntimeError("x")))
        assert procident.flock_holders_unavailable_reason(table.fd) == procident.UNAVAILABLE_UNKNOWN


# ---------------------------------------------------------------------------
# hidepid: ENOENT is confirmed with signal 0
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHidepidConfirmation:
    """Under `hidepid=2` another user's LIVE process also reads as ENOENT, so
    "no such file" alone is not "gone"."""

    @pytest.fixture
    def seen(self, proc_root, monkeypatch):
        calls = []

        def make(answer):
            def kill0(pid):
                calls.append(pid)
                return answer

            monkeypatch.setattr(procident, "_kill0", kill0)

        make.calls = calls
        return make

    def test_enoent_with_esrch_is_gone(self, seen):
        seen("gone")
        assert procident.identity(31999, "linux:1") == "gone"
        assert seen.calls == [31999]

    def test_enoent_with_a_live_process_is_unknown_not_gone(self, seen):
        seen("unknown")
        assert procident.identity(31999, "linux:1") == "unknown"
        assert procident.start_token(31999) is None

    def test_signal_zero_is_not_consulted_when_stat_reads(self, proc_root, seen):
        seen("gone")
        _write_stat(proc_root, 50, _stat_line(50, "d", 9))
        assert procident.identity(50, "linux:9") == "match"
        assert seen.calls == []

    def test_kill0_sends_only_signal_zero(self, monkeypatch):
        sent = []
        monkeypatch.setattr(procident.os, "kill", lambda pid, sig: sent.append((pid, sig)))
        assert procident._kill0(4242) == "unknown"          # it exists
        assert sent == [(4242, 0)]

    @pytest.mark.parametrize(
        "exc, expected",
        [
            (ProcessLookupError(errno.ESRCH, "x"), "gone"),
            (PermissionError(errno.EPERM, "x"), "unknown"),     # exists, not ours
            (OSError(errno.EINVAL, "x"), "unknown"),
        ],
    )
    def test_kill0_maps_errors(self, monkeypatch, exc, expected):
        sent = []

        def kill(pid, sig):
            sent.append((pid, sig))
            raise exc

        monkeypatch.setattr(procident.os, "kill", kill)
        assert procident._kill0(4242) == expected
        assert sent == [(4242, 0)]

    def test_kill0_on_a_process_that_does_not_exist_is_gone(self):
        proc = _spawn_sleeper("pass")
        proc.wait(timeout=30)
        proc.stdout.close()
        assert procident._kill0(proc.pid) == "gone"

    def test_kill0_on_ourselves_is_unknown(self):
        assert procident._kill0(os.getpid()) == "unknown"


# ---------------------------------------------------------------------------
# Linux /proc/locks + fdinfo + mountinfo, against a fixture tree (any host)
# ---------------------------------------------------------------------------

#: `1: FLOCK ADVISORY WRITE <pid> <MAJ:MIN hex>:<inode decimal> 0 EOF`
LOCKS = """\
1: FLOCK  ADVISORY  WRITE 1001 08:01:{ino} 0 EOF
2: FLOCK  ADVISORY  READ  1002 08:01:{ino} 0 EOF
3: POSIX  ADVISORY  WRITE 1003 08:01:{ino} 0 EOF
4: OFDLCK ADVISORY  WRITE -1 08:01:{ino} 0 EOF
5: -> FLOCK  ADVISORY  WRITE 1005 08:01:{ino} 0 EOF
6: FLOCK  ADVISORY  WRITE 0 08:01:{ino} 0 EOF
7: FLOCK  ADVISORY  WRITE 1007 fe:09:{ino} 0 EOF
8: FLOCK  ADVISORY  WRITE 1008 08:01:{other} 0 EOF
9: LEASE  BREAKING  READ  1009 08:01:{ino} 0 EOF
"""

MNT_ID = 77


class LockTable:
    """A fixture `/proc` for ONE real open file: `self/fdinfo/<fd>`,
    `self/mountinfo` and `locks`. `device` is what mountinfo says (decimal)."""

    def __init__(self, root, tmp_path):
        self.root = root
        self.path = tmp_path / "locked-file"
        self.path.write_text("x")
        self.fd = os.open(str(self.path), os.O_RDONLY)
        self.ino = os.fstat(self.fd).st_ino
        self.other = self.ino + 1_000_003
        self.set_mount("8:1")
        self.set_fdinfo(f"pos:\t0\nflags:\t02100000\nmnt_id:\t{MNT_ID}\nino:\t{self.ino}\n")
        self.set_locks(LOCKS)

    def set_fdinfo(self, text, fd=None):
        d = self.root / "self" / "fdinfo"
        d.mkdir(parents=True, exist_ok=True)
        (d / str(self.fd if fd is None else fd)).write_text(text)

    def set_mount(self, device, mnt_id=MNT_ID, extra=""):
        (self.root / "self").mkdir(parents=True, exist_ok=True)
        (self.root / "self" / "mountinfo").write_text(
            f"22 1 0:21 / /sys rw - sysfs sysfs rw\n{mnt_id} 22 {device} / /data rw - ext4 /dev/sda1 rw\n{extra}"
        )

    def set_locks(self, text):
        (self.root / "locks").write_text(text.replace("{ino}", str(self.ino)).replace("{other}", str(self.other)))

    def holders(self):
        return procident.flock_write_holders(self.fd)


@pytest.fixture
def table(proc_root, tmp_path):
    t = LockTable(proc_root, tmp_path)
    yield t
    try:
        os.close(t.fd)
    except OSError:
        pass


@pytest.mark.unit
class TestLinuxLocksParser:
    def test_only_flock_write_holders_of_that_device_and_inode(self, table):
        # 1001 only. Not READ, POSIX, OFDLCK, waiters, pid 0, leases, another
        # inode -- and NOT 1007, whose inode matches on another device.
        assert table.holders() == {1001}

    def test_the_device_is_matched_not_ignored(self, table):
        assert 1007 not in table.holders()
        table.set_locks("1: FLOCK  ADVISORY  WRITE 1007 fe:09:{ino} 0 EOF\n")
        assert table.holders() == set()

    def test_the_same_inode_on_another_device_is_never_a_holder(self, table):
        table.set_locks("".join(
            f"{n}: FLOCK  ADVISORY  WRITE {2000 + n} {dev}:{{ino}} 0 EOF\n"
            for n, dev in enumerate(["08:02", "09:01", "00:01", "01:01", "8:1x", "ff:ff"])
        ))
        assert table.holders() == set()

    def test_the_same_device_and_inode_matches(self, table):
        table.set_locks("1: FLOCK  ADVISORY  WRITE 4321 08:01:{ino} 0 EOF\n")
        assert table.holders() == {4321}

    def test_the_lock_table_device_is_hex_and_mountinfo_is_decimal(self, table):
        # fd:03 == 253:3
        table.set_mount("253:3")
        table.set_locks(
            "1: FLOCK  ADVISORY  WRITE 11 fd:03:{ino} 0 EOF\n"
            "2: FLOCK  ADVISORY  WRITE 12 253:03:{ino} 0 EOF\n"      # 253 read as hex is 0x253: no
            "3: FLOCK  ADVISORY  WRITE 13 FD:03:{ino} 0 EOF\n"       # case-insensitive hex
        )
        assert table.holders() == {11, 13}

    def test_a_hex_minor_is_not_read_as_decimal(self, table):
        table.set_mount("8:10")                                       # minor ten
        table.set_locks(
            "1: FLOCK  ADVISORY  WRITE 21 08:0a:{ino} 0 EOF\n"       # 0x0a == 10: match
            "2: FLOCK  ADVISORY  WRITE 22 08:10:{ino} 0 EOF\n"       # 0x10 == 16: no
        )
        assert table.holders() == {21}

    def test_a_large_device_number(self, table):
        table.set_mount("259:262144")
        table.set_locks("1: FLOCK  ADVISORY  WRITE 31 103:40000:{ino} 0 EOF\n")
        assert table.holders() == {31}

    def test_a_different_inode(self, table):
        table.set_locks("1: FLOCK  ADVISORY  WRITE 1008 08:01:{other} 0 EOF\n")
        assert table.holders() == set()

    def test_flock_read_is_not_a_holder(self, table):
        assert 1002 not in table.holders()

    def test_posix_and_ofd_locks_are_not_holders(self, table):
        holders = table.holders()
        assert 1003 not in holders and -1 not in holders

    def test_blocked_waiters_are_skipped(self, table):
        assert 1005 not in table.holders()

    def test_pid_zero_is_dropped(self, table):
        assert 0 not in table.holders()

    def test_an_inode_nobody_holds_is_an_empty_set_not_none(self, table):
        table.set_locks("1: FLOCK  ADVISORY  WRITE 1008 08:01:{other} 0 EOF\n")
        result = table.holders()
        assert result == set() and result is not None

    def test_an_empty_locks_file_is_an_empty_set(self, table):
        table.set_locks("")
        assert table.holders() == set()

    @pytest.mark.parametrize(
        "line",
        [
            "garbage",
            "",
            "1:",
            "1: FLOCK",
            "1: FLOCK ADVISORY WRITE",
            "1: FLOCK ADVISORY WRITE notapid 08:01:{ino} 0 EOF",
            "1: FLOCK ADVISORY WRITE 1234 noinodefield 0 EOF",
            "1: FLOCK ADVISORY WRITE 1234 08:01:notaninode 0 EOF",
            "1: FLOCK ADVISORY WRITE 1234 08:01: 0 EOF",
            "1: FLOCK ADVISORY WRITE 1234 zz:01:{ino} 0 EOF",
            "1: FLOCK ADVISORY WRITE 1234 08:{ino} 0 EOF",
            "1: FLOCK ADVISORY WRITE 1234 08:01:{ino}:9 0 EOF",
            "\x00\x01\x02 FLOCK",
            "1: FLOCK ADVISORY WRITE -7 08:01:{ino} 0 EOF",
        ],
    )
    def test_junk_lines_are_ignored_without_hiding_real_ones(self, table, line):
        table.set_locks(line + "\n" + "1: FLOCK  ADVISORY  WRITE 4321 08:01:{ino} 0 EOF\n")
        assert table.holders() == {4321}

    def test_non_ascii_bytes_do_not_break_parsing(self, table):
        (table.root / "locks").write_bytes(
            b"\xff\xfe junk\n1: FLOCK  ADVISORY  WRITE 4321 08:01:" + str(table.ino).encode() + b" 0 EOF\n"
        )
        assert table.holders() == {4321}

    def test_several_holders_of_one_inode(self, table):
        table.set_locks(
            "1: FLOCK  ADVISORY  WRITE 10 08:01:{ino} 0 EOF\n"
            "2: FLOCK  ADVISORY  WRITE 11 08:01:{ino} 0 EOF\n"
        )
        assert table.holders() == {10, 11}

    def test_the_inode_is_matched_exactly_not_as_a_suffix(self, table):
        table.set_locks(
            f"1: FLOCK  ADVISORY  WRITE 10 08:01:9{table.ino} 0 EOF\n"
            f"2: FLOCK  ADVISORY  WRITE 11 08:01:{table.ino}9 0 EOF\n"
        )
        assert table.holders() == set()

    def test_lock_type_match_is_exact(self, table):
        table.set_locks(
            "1: FLOCKX  ADVISORY  WRITE 10 08:01:{ino} 0 EOF\n2: FLOCK ADVISORY WRITEX 11 08:01:{ino} 0 EOF\n"
        )
        assert table.holders() == set()

    # -- None: "cannot tell" is never "nobody" --------------------------------

    def test_a_missing_locks_file_is_none(self, table):
        (table.root / "locks").unlink()
        assert table.holders() is None

    def test_an_unreadable_locks_file_is_none(self, table, monkeypatch):
        real = builtins.open

        def fake_open(path, *a, **kw):
            if str(path).endswith("/locks"):
                raise PermissionError(errno.EACCES, "denied")
            return real(path, *a, **kw)

        monkeypatch.setattr(procident, "open", fake_open, raising=False)
        assert table.holders() is None

    def test_locks_that_is_a_directory_is_none(self, table):
        (table.root / "locks").unlink()
        (table.root / "locks").mkdir()
        assert table.holders() is None

    def test_a_missing_mnt_id_is_none(self, table):
        table.set_fdinfo("pos:\t0\nflags:\t02100000\nino:\t5\n")
        assert table.holders() is None

    def test_a_non_numeric_mnt_id_is_none(self, table):
        table.set_fdinfo("pos:\t0\nmnt_id:\tabc\n")
        assert table.holders() is None

    def test_a_mount_that_mountinfo_does_not_list_is_none(self, table):
        table.set_mount("8:1", mnt_id=MNT_ID + 1)
        assert table.holders() is None

    def test_a_malformed_mountinfo_device_is_none(self, table):
        for device in ("8-1", "x:y", "8", ":", "8:1:3"):
            table.set_mount(device)
            assert table.holders() is None, device

    def test_a_short_mountinfo_line_is_skipped(self, table):
        table.set_mount("8:1", extra="")
        (table.root / "self" / "mountinfo").write_text(f"{MNT_ID}\n{MNT_ID} 22\n")
        assert table.holders() is None

    def test_a_missing_fdinfo_is_none(self, table):
        (table.root / "self" / "fdinfo" / str(table.fd)).unlink()
        assert table.holders() is None

    def test_a_missing_mountinfo_is_none(self, table):
        (table.root / "self" / "mountinfo").unlink()
        assert table.holders() is None

    def test_unreadable_fdinfo_and_mountinfo_are_none(self, table, monkeypatch):
        real = builtins.open
        for needle in ("/fdinfo/", "/mountinfo"):
            def fake_open(path, *a, _n=needle, **kw):
                if _n in str(path):
                    raise PermissionError(errno.EACCES, "denied")
                return real(path, *a, **kw)

            monkeypatch.setattr(procident, "open", fake_open, raising=False)
            assert table.holders() is None, needle

    def test_a_closed_fd_is_none(self, table):
        os.close(table.fd)
        assert table.holders() is None

    def test_a_mnt_id_is_matched_as_a_whole_field(self, table):
        table.set_mount("8:1", mnt_id=MNT_ID * 10)               # '770', not '77'
        assert table.holders() is None

    def test_only_the_first_mnt_id_line_counts(self, table):
        table.set_fdinfo(f"mnt_id:\t{MNT_ID}\nmnt_id:\t999\n")
        assert table.holders() == {1001}

    def test_not_linux_is_none_even_with_a_perfect_table(self, table, monkeypatch):
        monkeypatch.setattr(sys, "platform", "darwin")
        assert table.holders() is None
        monkeypatch.setattr(sys, "platform", "freebsd14")
        assert table.holders() is None

    def test_the_fd_is_what_is_asked_about_not_a_path(self, table):
        other = table.path.parent / "other-file"
        other.write_text("y")
        fd2 = os.open(str(other), os.O_RDONLY)
        try:
            table.set_fdinfo(f"mnt_id:\t{MNT_ID}\n", fd=fd2)
            assert procident.flock_write_holders(fd2) == set()    # a different inode: nobody
            assert table.holders() == {1001}
        finally:
            os.close(fd2)


@pytest.mark.unit
class TestProcessUid:
    def test_a_fixture_dir_returns_its_owners_uid(self, proc_root):
        (proc_root / "777").mkdir()
        assert procident.process_uid(777) == os.stat(proc_root / "777").st_uid

    def test_the_target_pid_is_what_is_statted(self, proc_root, monkeypatch):
        seen = []
        real = os.stat

        def spy(path, *a, **kw):
            seen.append(str(path))
            return real(path, *a, **kw)

        (proc_root / "888").mkdir()
        monkeypatch.setattr(procident.os, "stat", spy)
        procident.process_uid(888)
        assert seen == [f"{proc_root}/888"]

    def test_a_missing_dir_is_none(self, proc_root):
        assert procident.process_uid(31999) is None

    @pytest.mark.parametrize("pid", [None, 0, -1, 2**31, 2**40, True, False, "1", 1.5, object()])
    def test_an_invalid_pid_is_none_without_touching_the_filesystem(self, proc_root, monkeypatch, pid):
        monkeypatch.setattr(procident.os, "stat", lambda *a, **kw: pytest.fail("statted"))
        assert procident.process_uid(pid) is None

    def test_permission_error_is_none(self, proc_root, monkeypatch):
        def deny(path, *a, **kw):
            raise PermissionError(errno.EACCES, "denied")

        monkeypatch.setattr(procident.os, "stat", deny)
        assert procident.process_uid(5) is None

    def test_other_platforms_are_none(self, proc_root, monkeypatch):
        (proc_root / "777").mkdir()
        for platform in ("darwin", "freebsd14", "win32"):
            monkeypatch.setattr(sys, "platform", platform)
            assert procident.process_uid(777) is None

    @pytest.mark.skipif(not sys.platform.startswith("linux"), reason="needs a real /proc")
    def test_our_own_uid_on_linux(self):
        assert procident.process_uid(os.getpid()) == os.getuid()

    def test_never_raises_whatever_happens(self, proc_root, monkeypatch):
        monkeypatch.setattr(procident.os, "stat", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        assert procident.process_uid(5) is None


# ---------------------------------------------------------------------------
# macOS sysctl path, against a fake libc (any host)
# ---------------------------------------------------------------------------


class _FakeLibc:
    """Stands in for `ctypes.CDLL(None)`: `sysctl` fills `buf` and `size`."""

    AUTO = object()

    def __init__(self, *, rc=0, size=None, data=b"", errno_value=0, pid=AUTO):
        """`pid`: what `p_pid` (bytes 40-44) says; `AUTO` = the pid asked about
        (the MIB's last element), anything else is written as given."""
        self.rc = rc
        self.size = size
        self.data = data
        self.pid = pid
        self.calls = []

    def sysctl(self, mib, namelen, buf, sizeref, newp, newlen):
        self.calls.append((list(mib), namelen, sizeref._obj.value))
        if self.rc != 0:
            return self.rc
        size = len(self.data) if self.size is None else self.size
        data = bytearray(self.data)
        if len(data) >= 44:
            pid = list(mib)[3] if self.pid is self.AUTO else self.pid
            data[40:44] = int(pid).to_bytes(4, sys.byteorder, signed=True)
        ctypes.memmove(buf, bytes(data), min(len(data), ctypes.sizeof(buf)))
        sizeref._obj.value = size
        return 0


def _timeval(sec, usec, stat=2):
    """A fake `kinfo_proc`: `p_starttime` at 0, `p_stat` at 36 (2 = SRUN,
    5 = SZOMB); `p_pid` at 40 is filled in by `_FakeLibc`."""
    head = int(sec).to_bytes(8, sys.byteorder, signed=True) + int(usec).to_bytes(4, sys.byteorder, signed=True)
    body = bytearray(head + b"\x00" * (648 - len(head)))
    body[36] = stat
    return bytes(body)


@pytest.fixture
def fake_darwin(monkeypatch):
    """Fake libc via the `_libc()` seam (the real one is cached process-wide; a
    patched `ctypes.CDLL` would be cached into `_LIBC` and poison every later test)."""
    monkeypatch.setattr(sys, "platform", "darwin")

    def install(libc):
        monkeypatch.setattr(procident, "_libc", lambda: libc)
        return libc

    return install


@pytest.mark.unit
class TestDarwinSysctl:
    def test_token_format_pads_microseconds_to_six_digits(self, fake_darwin):
        libc = fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 42), size=648))
        assert procident.start_token(1234) == "darwin:1700000000.000042"
        assert procident.identity(1234, "darwin:1700000000.000042") == "match"
        assert procident.identity(1234, "darwin:1700000000.000043") == "mismatch"
        assert libc.calls[0][0] == [1, 14, 1, 1234]  # CTL_KERN, KERN_PROC, KERN_PROC_PID, pid
        assert libc.calls[0][1] == 4

    def test_a_full_microsecond_value(self, fake_darwin):
        fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 999_999), size=648))
        assert procident.start_token(5) == "darwin:1700000000.999999"

    def test_sysctl_failure_is_unknown(self, fake_darwin):
        fake_darwin(_FakeLibc(rc=-1))
        assert procident.start_token(1234) is None
        assert procident.identity(1234, "darwin:1.000000") == "unknown"

    def test_size_zero_means_the_process_is_gone(self, fake_darwin):
        fake_darwin(_FakeLibc(data=b"", size=0))
        assert procident.start_token(1234) is None
        assert procident.identity(1234, "darwin:1700000000.000042") == "gone"

    def test_a_short_answer_is_unknown(self, fake_darwin):
        fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 1), size=8))
        assert procident.identity(1234, "darwin:1700000000.000001") == "unknown"

    @pytest.mark.parametrize(
        "sec, usec",
        [(0, 0), (-5, 10), (1_700_000_000, -1), (1_700_000_000, 1_000_000), (1_700_000_000, 2**30)],
    )
    def test_implausible_timevals_are_unknown(self, fake_darwin, sec, usec):
        fake_darwin(_FakeLibc(data=_timeval(sec, usec), size=648))
        assert procident.identity(1234, "darwin:1.000000") == "unknown"

    # -- zombie / p_pid / size -------------------------------------------------

    def test_p_stat_szomb_is_a_zombie_and_so_gone(self, fake_darwin):
        fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 5, stat=5), size=648))
        assert procident.is_zombie(1234) is True
        assert procident.start_token(1234) is None
        assert procident.identity(1234, "darwin:1700000000.000005") == "gone"

    @pytest.mark.parametrize("stat", [1, 2, 3, 4, 6, 0])
    def test_any_other_p_stat_is_not_a_zombie(self, fake_darwin, stat):
        fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 5, stat=stat), size=648))
        assert procident.is_zombie(1234) is False
        assert procident.start_token(1234) == "darwin:1700000000.000005"
        assert procident.identity(1234, "darwin:1700000000.000005") == "match"

    def test_a_p_pid_that_is_not_the_pid_asked_about_is_unknown(self, fake_darwin):
        fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 5), size=648, pid=999))
        assert procident.start_token(1234) is None
        assert procident.identity(1234, "darwin:1700000000.000005") == "unknown"
        assert procident.is_zombie(1234) is None

    def test_a_zombie_with_the_wrong_p_pid_is_unknown_not_a_zombie(self, fake_darwin):
        fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 5, stat=5), size=648, pid=0))
        assert procident.is_zombie(1234) is None
        assert procident.identity(1234, "darwin:1700000000.000005") == "unknown"

    @pytest.mark.parametrize("size", [1, 12, 40, 43])
    def test_an_answer_shorter_than_44_bytes_is_unknown(self, fake_darwin, size):
        fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 5), size=size))
        assert procident.identity(1234, "darwin:1700000000.000005") == "unknown"
        assert procident.is_zombie(1234) is None

    def test_exactly_44_bytes_is_enough(self, fake_darwin):
        fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 5), size=44))
        assert procident.start_token(1234) == "darwin:1700000000.000005"

    def test_the_mib_names_the_pid(self, fake_darwin):
        libc = fake_darwin(_FakeLibc(data=_timeval(1_700_000_000, 5), size=648))
        procident.start_token(31337)
        assert libc.calls[0][0] == [1, 14, 1, 31337]

    def test_ctypes_failures_are_unknown(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "darwin")

        def boom():
            raise OSError("no libc")

        monkeypatch.setattr(procident, "_libc", boom)
        assert procident.start_token(1234) is None
        assert procident.identity(1234, "darwin:1.000000") == "unknown"

    def test_libc_is_loaded_once_across_many_calls(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "darwin")
        monkeypatch.setattr(procident, "_LIBC", None)         # restored (to the real handle) at teardown
        loads = []
        libc = _FakeLibc(data=_timeval(1_700_000_000, 7), size=648)

        def cdll(name, **kw):
            loads.append((name, kw))
            return libc

        monkeypatch.setattr(ctypes, "CDLL", cdll)
        for pid in (11, 12, 13, 14):
            assert procident.start_token(pid) == "darwin:1700000000.000007"
        assert len(loads) == 1
        assert loads[0] == (None, {"use_errno": True})
        assert procident._libc() is libc
        assert len(libc.calls) == 4

    def test_libc_is_cached_in_the_module(self, monkeypatch):
        monkeypatch.setattr(procident, "_LIBC", None)
        monkeypatch.setattr(ctypes, "CDLL", lambda *a, **kw: object())
        first = procident._libc()
        assert procident._libc() is first and procident._LIBC is first

    def test_a_missing_sysctl_symbol_is_unknown(self, fake_darwin):
        class NoSysctl:
            pass

        fake_darwin(NoSysctl())
        assert procident.identity(1234, "darwin:1.000000") == "unknown"

    def test_holders_are_none_on_macos(self, fake_darwin):
        assert procident.flock_write_holders(1) is None


# ---------------------------------------------------------------------------
# Platform-specific agreement with the real OS
# ---------------------------------------------------------------------------


@pytest.mark.integration
@linux_only
class TestLinuxRealFlockHolders:
    """[L] The real /proc: lock table, fdinfo and mountinfo."""

    def _holder(self, sleeper, path, mode):
        child = sleeper(
            "import fcntl, os, sys, time\n"
            f"fd = os.open({str(path)!r}, os.O_RDWR)\n"
            f"fcntl.flock(fd, {mode})\n"
            "print('ready', flush=True)\n"
            "time.sleep(120)\n"
        )
        assert child.stdout.readline().strip() == b"ready"
        return child

    def test_a_real_child_holding_lock_ex_is_found_via_the_real_proc(self, sleeper, tmp_path):
        path = tmp_path / "locked"
        path.write_text("x")
        child = self._holder(sleeper, path, "fcntl.LOCK_EX")
        fd = os.open(str(path), os.O_RDONLY)
        try:
            assert procident.flock_write_holders(fd) == {child.pid}
        finally:
            os.close(fd)

    def test_a_real_child_holding_lock_sh_is_not_listed(self, sleeper, tmp_path):
        path = tmp_path / "shared"
        path.write_text("x")
        child = self._holder(sleeper, path, "fcntl.LOCK_SH")
        fd = os.open(str(path), os.O_RDONLY)
        try:
            assert child.pid not in procident.flock_write_holders(fd)
        finally:
            os.close(fd)

    def test_the_holder_disappears_when_it_exits(self, sleeper, tmp_path):
        path = tmp_path / "gone"
        path.write_text("x")
        child = self._holder(sleeper, path, "fcntl.LOCK_EX")
        fd = os.open(str(path), os.O_RDONLY)
        try:
            assert child.pid in procident.flock_write_holders(fd)
            child.kill()
            child.wait(timeout=10)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and procident.flock_write_holders(fd):
                time.sleep(0.05)
            assert procident.flock_write_holders(fd) == set()
        finally:
            os.close(fd)

    def test_our_own_lock_is_listed_under_our_pid(self, tmp_path):
        path = tmp_path / "ours"
        path.write_text("x")
        fd = os.open(str(path), os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            assert os.getpid() in procident.flock_write_holders(fd)
        finally:
            os.close(fd)

    def test_the_fds_mount_device_equals_the_device_proc_locks_prints(self, tmp_path):
        path = tmp_path / "dev"
        path.write_text("x")
        fd = os.open(str(path), os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            ino = os.fstat(fd).st_ino
            printed = None
            for line in Path("/proc/locks").read_text().splitlines():
                fields = line.split()
                if len(fields) > 5 and fields[1] == "FLOCK" and fields[4] == str(os.getpid()):
                    maj, mnt, node = fields[5].split(":")
                    if int(node) == ino:
                        printed = (int(maj, 16), int(mnt, 16))
            assert printed is not None, "our lock is not in /proc/locks"
            assert procident._linux_fd_device(fd) == printed
        finally:
            os.close(fd)

    def test_real_proc_stat_matches_a_manual_parse(self):
        with open(f"/proc/{os.getpid()}/stat", "rb") as fh:
            text = fh.read().decode("ascii", "replace")
        starttime = text[text.rindex(")") + 1:].split()[19]
        assert procident.start_token(os.getpid()) == f"linux:{starttime}"

    def test_process_uid_is_our_uid(self):
        assert procident.process_uid(os.getpid()) == os.getuid()


@pytest.mark.integration
@darwin_only
class TestDarwinRealStartTime:
    @staticmethod
    def _ps_epoch(pid):
        out = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            env={**os.environ, "LC_ALL": "C"},
            capture_output=True,
            text=True,
            timeout=20,
        ).stdout.strip()
        assert out, "ps printed no lstart"
        return time.mktime(time.strptime(out, "%a %b %d %H:%M:%S %Y"))

    def test_own_start_token_agrees_with_ps_within_a_second(self):
        token = procident.start_token(os.getpid())
        assert token.startswith("darwin:")
        sec = float(token.split(":", 1)[1])
        assert abs(sec - self._ps_epoch(os.getpid())) <= 1.0

    def test_a_childs_start_token_agrees_with_ps_within_a_second(self, sleeper):
        child = sleeper()
        sec = float(procident.start_token(child.pid).split(":", 1)[1])
        assert abs(sec - self._ps_epoch(child.pid)) <= 1.0

    def test_a_child_started_later_has_a_later_token(self, sleeper):
        first = sleeper()
        time.sleep(0.05)
        second = sleeper()
        a = float(procident.start_token(first.pid).split(":", 1)[1])
        b = float(procident.start_token(second.pid).split(":", 1)[1])
        assert b > a

    def test_holders_are_unavailable_on_macos(self, tmp_path):
        fd = os.open(str(tmp_path), os.O_RDONLY)
        try:
            assert procident.flock_write_holders(fd) is None
        finally:
            os.close(fd)


# ---------------------------------------------------------------------------
# Layering: stdlib only, no config store
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLayering:
    def test_source_does_not_import_the_config_store(self):
        tree = ast.parse(Path(procident.__file__).read_text())
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        assert not [m for m in imported if m.startswith("zfsbackup")], imported
        assert not [m for m in imported if "sqlalchemy" in m or "alembic" in m]

    def test_importing_it_in_a_fresh_interpreter_loads_no_store(self):
        script = (
            "import sys\n"
            "import zfsbackup.procident\n"
            "bad = [m for m in sys.modules if m.startswith('zfsbackup.config.store')"
            " or m.startswith('sqlalchemy')]\n"
            "assert not bad, bad\n"
            "print('OK')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=60,
            env=hermetic_env(),
            cwd=str(REPO_ROOT),
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout

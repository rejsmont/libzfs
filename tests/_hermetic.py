"""Keeps the test suite away from a real `zfs`/`zpool`, and from leaking processes.

Imported by `conftest.py` BEFORE `libzfseasy` (which resolves `$ZFS_CMD` /
`$ZPOOL_CMD` at import time), and by the test modules that start real
subprocesses.

**The real tier is opt-in.** The mocked tiers (everything but ``-m real_zfs``)
never touch the host's ZFS: unless ``ZFSB_REAL_ZFS=1`` is exported,

- ``apply_stub_policy()`` points an unset ``ZFS_CMD`` / ``ZPOOL_CMD`` at a stub, so a
  bare ``pytest`` can never resolve the host's real binaries from ``PATH``;
- every test marked ``real_zfs`` is SKIPPED (``conftest.pytest_collection_modifyitems``),
  so the pool fixture -- which would run a real ``sudo zpool create`` -- never starts, and
  no real test ever runs against a stub.

With ``ZFSB_REAL_ZFS=1`` nothing is stubbed (the real tier needs the real binaries) and the
``real_zfs`` tests run; the mocked tiers' subprocess helpers still refuse a real ``zfs``
(``hermetic_env``) and the in-process guard still protects every non-``real_zfs`` test.

Helpers:

- ``real_tier_enabled(environ)``  whether ``ZFSB_REAL_ZFS`` opts in.
- ``hermetic_env(**extra)``       the environment for a subprocess that may reach ``zfs``: a
                                  copy of ``os.environ`` whose ``ZFS_CMD`` / ``ZPOOL_CMD`` are
                                  stubs (never a real binary, even if the caller's environment
                                  names one) and whose ``PATH`` is a PRIVATE directory holding
                                  only ``zfs``/``zpool`` stubs and a handful of coreutils, plus
                                  the interpreter's directory. ``/usr/bin`` is deliberately NOT
                                  on it: some distributions install ``zfs`` there, so a bare
                                  ``zfs`` or ``sudo zfs`` must never be able to resolve a real
                                  binary. It never mutates ``os.environ``.
- ``nested_env(**extra)``         the environment for a NESTED pytest session: no inherited
                                  session identity (so it mints its own marker).
- ``is_real_zfs(path, installed)``  the guard's definition of "a real zfs".
- ``session_marker`` / ``fork_record_dir`` / ``register_fork_recorder`` / ``find_orphans``
                                  the leak safety net (see ``conftest.pytest_sessionfinish``).
"""

import atexit
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

_STUB_SCRIPT = "#!/bin/sh\n# test stub: always succeeds with empty output\nexit 0\n"
_stub_dir = None

#: `PATH` as it was when this module was first imported (conftest import time, before any
#: test touched it): what "the host's real zfs" is measured against.
ORIGINAL_PATH = os.environ.get("PATH", os.defpath)

#: Tools a hermetic subprocess may still find by name.
_PRIVATE_TOOLS = ("sh", "true", "false", "sleep", "cat", "echo")


def real_tier_enabled(environ=None) -> bool:
    """Is the real-ZFS tier opted into (`ZFSB_REAL_ZFS` = 1/true/yes/on)?"""
    value = (environ if environ is not None else os.environ).get("ZFSB_REAL_ZFS", "")
    return value.strip().lower() in ("1", "true", "yes", "on")


REAL_TIER_REASON = (
    "real ZFS tier is opt-in: it creates a pool with `sudo zpool create` and runs real "
    "zfs commands. Export ZFSB_REAL_ZFS=1 to run it (see tests/README.md)."
)


def _make_stub(name: str = "zfs-stub") -> str:
    global _stub_dir
    if _stub_dir is None:
        _stub_dir = tempfile.mkdtemp(prefix="zfsbackup-test-stub-")
        atexit.register(shutil.rmtree, _stub_dir, True)
    path = Path(_stub_dir) / name
    if not path.exists():
        path.write_text(_STUB_SCRIPT)
        path.chmod(0o755)
    return str(path)


def _private_bin() -> str:
    """A directory holding only `zfs` / `zpool` stubs and a few coreutils."""
    stub = _make_stub()
    directory = Path(_stub_dir) / "bin"
    if not directory.exists():
        directory.mkdir()
        for name in ("zfs", "zpool"):
            (directory / name).symlink_to(stub)
        for tool in _PRIVATE_TOOLS:
            found = shutil.which(tool, path=ORIGINAL_PATH)
            if found and not (directory / tool).exists():
                (directory / tool).symlink_to(found)
    return str(directory)


_installed = None


def installed_zfs_binaries():
    """Real paths of the `zfs` / `zpool` found on the ORIGINAL `PATH` (cached at
    first use, which is conftest import: a test that later puts its own `zfs` script
    first on `PATH` must not turn that script into "the real one")."""
    global _installed
    if _installed is None:
        _installed = frozenset(
            os.path.realpath(found)
            for found in (
                shutil.which("zfs", path=ORIGINAL_PATH),
                shutil.which("zpool", path=ORIGINAL_PATH),
            )
            if found
        )
    return _installed


def is_compiled(path: str) -> bool:
    """A file that is not a `#!` script (a real binary)."""
    try:
        with open(path, "rb") as handle:
            return handle.read(2) != b"#!"
    except OSError:
        return False


def is_real_zfs(program: str, installed=None) -> bool:
    """Would running `program` start a real `zfs` / `zpool`? Real = the binary
    found on the original `PATH`, or any compiled (non-`#!`) file named zfs/zpool.
    A script, e.g. a fake placed first on `PATH`, is not."""
    base = os.path.basename(program)
    resolved = shutil.which(program) or program
    if installed is not None and os.path.realpath(resolved) in installed:
        return True
    return base in ("zfs", "zpool") and is_compiled(resolved)


def apply_stub_policy(environ=None) -> None:
    """Unless the real tier is opted into, set an unset (or empty) `ZFS_CMD` /
    `ZPOOL_CMD` to a stub. Called once, at conftest import."""
    environ = os.environ if environ is None else environ
    if real_tier_enabled(environ):
        return
    for var in ("ZFS_CMD", "ZPOOL_CMD"):
        if not environ.get(var):
            environ[var] = _make_stub()


def hermetic_env(**extra) -> dict:
    """The environment for a subprocess that could reach `zfs` (see module doc)."""
    env = dict(os.environ)
    installed = installed_zfs_binaries()
    for var in ("ZFS_CMD", "ZPOOL_CMD"):
        if not env.get(var) or is_real_zfs(env[var], installed):
            env[var] = _make_stub()
    env["PATH"] = os.pathsep.join([_private_bin(), os.path.dirname(sys.executable)])
    env.update(extra)
    return env


_SESSION_VARS = (
    "ZFSB_TEST_SESSION", "ZFSB_TEST_SESSION_OWNER", "ZFSB_ALLOW_ORPHANS",
    "PYTEST_XDIST_WORKER", "PYTEST_XDIST_WORKER_COUNT", "PYTEST_XDIST_TESTRUNUID",
)


def nested_env(**extra) -> dict:
    """`os.environ` for a pytest session started FROM a test: without the outer
    session's identity (marker, owner, xdist worker variables), so the nested one
    mints its own marker and hunts only its own leftovers."""
    env = {k: v for k, v in os.environ.items() if k not in _SESSION_VARS}
    env.update(extra)
    return env


# --------------------------------------------------------------------------
# Session identity
# --------------------------------------------------------------------------


def session_marker(environ, ppid: int) -> str:
    """This session's marker, and record it in `environ` for children.

    ALWAYS a fresh one, except in a pytest-xdist worker whose parent is the
    controller that minted it (`ZFSB_TEST_SESSION_OWNER` is the controller's pid):
    the workers of one run share its marker, and nothing else does. In particular a
    marker exported by a developer, or inherited by a session started from a test
    (including from inside a worker, where the xdist variables are inherited too),
    is never reused -- it would make this session hunt, and kill, somebody else's
    processes.
    """
    import uuid

    inherited = environ.get("ZFSB_TEST_SESSION")
    if (
        inherited
        and environ.get("PYTEST_XDIST_WORKER")
        and environ.get("ZFSB_TEST_SESSION_OWNER") == str(ppid)
    ):
        return inherited
    marker = uuid.uuid4().hex
    environ["ZFSB_TEST_SESSION"] = marker
    environ["ZFSB_TEST_SESSION_OWNER"] = str(os.getpid())
    return marker


def fork_record_dir(marker: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"zfsb-forks-{marker}")


def register_fork_recorder(directory: str) -> None:
    """Record the pid of every child this process (and its forked descendants) forks
    WITHOUT exec-ing, into `directory`, one empty file per pid. Such a child has the
    pytest process's exec-time environment, so the marker is invisible to an
    environment scan; the record is how the safety net finds it."""

    def record():
        try:
            fd = os.open(os.path.join(directory, str(os.getpid())), os.O_WRONLY | os.O_CREAT, 0o600)
            os.close(fd)
        except OSError:
            pass

    if hasattr(os, "register_at_fork"):
        os.register_at_fork(after_in_child=record)


# --------------------------------------------------------------------------
# Orphan hunt (session safety net; see conftest.pytest_sessionfinish)
# --------------------------------------------------------------------------


def _process_table():
    """{pid: (ppid, state, started_epoch_or_None, text)} for every process we can see.
    `text` holds the process environment when the platform lets us read it
    (/proc/<pid>/environ on Linux, `ps -E` on macOS/BSD)."""
    import subprocess

    rows = {}
    if os.path.isdir("/proc/self"):
        try:
            boot = next(
                int(line.split()[1]) for line in open("/proc/stat") if line.startswith("btime")
            )
            ticks = os.sysconf("SC_CLK_TCK")
        except (OSError, StopIteration, ValueError):
            boot, ticks = None, 100
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat", "rb") as handle:
                    stat = handle.read().decode("utf-8", "replace")
                fields = stat[stat.rindex(")") + 2:].split()
            except (OSError, ValueError):
                continue
            try:
                with open(f"/proc/{entry}/environ", "rb") as handle:
                    environ = handle.read().replace(b"\0", b" ").decode("utf-8", "replace")
            except OSError:
                environ = ""
            started = boot + int(fields[19]) / ticks if boot is not None else None
            rows[int(entry)] = (int(fields[1]), fields[0], started, environ)
        return rows
    # `ps` by the path found on the ORIGINAL PATH (the session may run with a
    # restricted PATH); an unreadable process table means "cannot hunt", not a crash
    ps = shutil.which("ps", path=ORIGINAL_PATH + os.pathsep + "/bin" + os.pathsep + "/usr/bin") or "ps"
    try:
        out = subprocess.run(
            [ps, "-axww", "-E", "-o", "pid=,ppid=,stat=,lstart=,command="],
            capture_output=True, text=True, timeout=30,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return rows
    for line in out.splitlines():
        parts = line.split(None, 8)
        if len(parts) < 9 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        try:
            started = time.mktime(time.strptime(" ".join(parts[3:8]), "%a %b %d %H:%M:%S %Y"))
        except ValueError:
            started = None
        rows[int(parts[0])] = (int(parts[1]), parts[2], started, parts[8])
    return rows


def _descends_from(pid: int, root: int, table) -> bool:
    for _ in range(128):
        row = table.get(pid)
        if row is None:
            return False
        pid = row[0]
        if pid == root:
            return True
        if pid <= 1:
            return False
    return False


def _recorded_pids(directory):
    """{pid: mtime} of the fork records in `directory`."""
    found = {}
    if directory and os.path.isdir(directory):
        for name in os.listdir(directory):
            if name.isdigit():
                try:
                    found[int(name)] = os.stat(os.path.join(directory, name)).st_mtime
                except OSError:
                    pass
    return found


def find_orphans(marker: str, grace: float = 5.0, *, root_pid=None, fork_dir=None):
    """Processes of this test session that were left running: carrying
    `ZFSB_TEST_SESSION=<marker>` in their environment, OR recorded by the at-fork
    recorder (`fork_dir`), and that are NOT live descendants of `root_pid` (the pytest
    process; default: this one). That is "orphaned" in every sense -- reparented to init,
    to a subreaper (systemd --user, tini, docker --init) or left under a dead parent --
    and it leaves a worker that is merely still running under a live supervisor alone.

    A recorded pid is trusted only if the process started no later than its record
    (so a pid recycled by an unrelated process is never taken for ours). Waits up to
    `grace` seconds for ones that are merely mid-exit. Returns
    [(pid, ppid, state, text)]."""
    if root_pid is None:
        root_pid = os.getpid()
    if os.getpid() == 1:  # we ARE init (a container entrypoint): every child looks orphaned
        return []
    needle = f"ZFSB_TEST_SESSION={marker}"
    deadline = time.monotonic() + grace
    while True:
        table = _process_table()
        recorded = _recorded_pids(fork_dir)
        found = []
        for pid, (ppid, state, started, text) in table.items():
            if pid == root_pid or pid == os.getpid() or state.startswith("Z"):
                continue
            by_env = needle in text
            by_fork = (
                pid in recorded and started is not None and started <= recorded[pid] + 1.5
            )
            if not (by_env or by_fork):
                continue
            if _descends_from(pid, root_pid, table):
                continue
            found.append((pid, ppid, state, text))
        if not found or time.monotonic() >= deadline:
            return sorted(found)
        time.sleep(0.25)

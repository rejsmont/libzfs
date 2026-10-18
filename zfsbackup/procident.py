"""Process identity and `flock` holders, for verifying a signal target.

A pid alone is not an identity: pids are reused. `start_token(pid)` returns an
opaque string that is stable for the life of a process and different for any
later process that happens to get the same pid, and `identity(pid, token)`
compares a live process against a token recorded earlier. `flock_write_holders`
answers "which pids hold an exclusive `flock` on this inode?", which is the proof
that a pid named in a *file* really is the process holding the file's lock (a
`0660` pid file can be written by any group operator, so its content is
untrusted).

Everything here is best-effort and **never raises**: an unreadable or
unsupported answer is `None` / `'unknown'`, and the caller (the CLI's reload
request, a worker's orphan watch) treats "unknown" as "do not act".

Platforms:

* Linux: `/proc/<pid>/stat` field 22 (`starttime`, clock ticks since boot);
  `/proc/locks` for flock holders.
* macOS: `sysctl(CTL_KERN, KERN_PROC, KERN_PROC_PID, pid)` through `ctypes`
  (`kinfo_proc.kp_proc.p_un.__p_starttime`, a `struct timeval` at offset 0).
  There is no flock-holder query, so `flock_write_holders` is `None`.
* Anything else: no token (`None`), identity `'unknown'`.

**A zombie is gone.** An exited process that nobody has reaped keeps its pid,
its start time and its `/proc` entry, and `kill(pid, 0)` and `pidfd_open`
still succeed on it -- but it runs no code and handles no signal. So
`identity()` reports a zombie as `'gone'` (Linux: `/proc/<pid>/stat` state `Z`
or `X`; macOS: `kp_proc.p_stat == SZOMB`), `start_token()` returns `None` for
it, and `is_zombie()` says so explicitly for callers that must tell a zombie
from a hidden `/proc` entry.

No boot id is part of a token: tokens are only compared while the daemon's
presence lock is held, and no holder survives a reboot.

Stdlib only. Must not import `zfsbackup.config.store` (this module is shared by
the lock module and the workers).
"""

import os
import sys
from typing import Optional, Set, Tuple

#: Patchable root of the Linux proc filesystem.
_PROC_ROOT = "/proc"

# macOS sysctl MIB: CTL_KERN, KERN_PROC, KERN_PROC_PID, <pid>.
_CTL_KERN = 1
_KERN_PROC = 14
_KERN_PROC_PID = 1
#: `sizeof(struct kinfo_proc)` is 648 on Darwin; leave headroom.
_KINFO_BUFFER = 1024

# Internal read results.
_OK = "ok"
_GONE = "gone"
_UNKNOWN = "unknown"
_ZOMBIE = "zombie"

#: `/proc/<pid>/stat` states of a process that has exited: `Z` zombie, `X`
#: dead (being reaped). Neither runs code or handles a signal.
_LINUX_DEAD_STATES = ("Z", "X")

# macOS `struct extern_proc` (sys/proc.h), the first member of `kinfo_proc`
# (verified on Darwin 25 / arm64 against getpid() and an unreaped child):
#   0  p_un (union; __p_starttime is a struct timeval: int64 sec, int32 usec)
#  16  p_vmspace (pointer)   24  p_sigacts (pointer)   32  p_flag (int)
#  36  p_stat (char)         40  p_pid (pid_t)
_DARWIN_P_STAT = 36
_DARWIN_P_PID = 40
#: `p_stat` of a zombie (`SZOMB` in sys/proc.h; `SRUN` is 2).
_DARWIN_SZOMB = 5

#: Why `flock_write_holders` could not answer; see
#: `flock_holders_unavailable_reason`.
UNAVAILABLE_NOT_LINUX = "not-linux"
UNAVAILABLE_LOCKS_UNREADABLE = "locks-unreadable"
UNAVAILABLE_MOUNT_DEVICE = "mount-device"
UNAVAILABLE_UNKNOWN = "unknown"


def _is_linux() -> bool:
    return sys.platform.startswith("linux")


def _is_darwin() -> bool:
    return sys.platform == "darwin"


#: Largest pid representable as a C `int` (ctypes `c_int` wraps silently
#: beyond it, which would make `sysctl` answer for a different process).
_MAX_PID = 2**31 - 1


def _valid_pid(pid) -> bool:
    return isinstance(pid, int) and not isinstance(pid, bool) and 0 < pid <= _MAX_PID


def _kill0(pid: int) -> str:
    """`kill(pid, 0)` -- sends no signal -- as `'gone'` (ESRCH) or
    `'unknown'` (exists, or cannot tell). A seam for tests."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return _GONE
    except OSError:
        return _UNKNOWN
    return _UNKNOWN


# ---------------------------------------------------------------------------
# Linux
# ---------------------------------------------------------------------------


def _linux_token(pid: int) -> Tuple[str, Optional[str]]:
    try:
        with open(f"{_PROC_ROOT}/{pid}/stat", "rb") as fh:
            raw = fh.read()
    except (FileNotFoundError, ProcessLookupError):
        # Under `hidepid=2` another user's live process also reads as ENOENT,
        # so "no such file" alone is not "gone": confirm with signal 0
        # (ESRCH = gone; EPERM or success = it exists -> we cannot tell).
        return _kill0(pid), None
    except OSError:  # PermissionError (hidepid=1), EIO, ...
        return _UNKNOWN, None
    try:
        text = raw.decode("ascii", errors="replace")
        # `comm` is parenthesised and may itself contain spaces and ')'; the
        # fields we want come after the LAST ')'. Index 0 there is field 3
        # (state), so field 22 (starttime) is index 19.
        rest = text[text.rindex(")") + 1:].split()
        state = rest[0]
        starttime = rest[19]
        if not starttime.isdigit():
            return _UNKNOWN, None
    except (ValueError, IndexError):
        return _UNKNOWN, None
    if state in _LINUX_DEAD_STATES:
        return _ZOMBIE, None
    return _OK, f"linux:{starttime}"


def _linux_fd_device(fd: int) -> Optional[Tuple[int, int]]:
    """`(major, minor)` of the superblock behind open `fd`, as `/proc/locks`
    prints it (`MAJOR(inode->i_sb->s_dev)`), or `None` if it cannot be told.

    Taken from the *mount*, not from `st_dev`: `fdinfo` gives the fd's
    `mnt_id`, and `mountinfo` field 3 for that mount ID is `sb->s_dev`. On
    btrfs subvolumes and overlayfs `st_dev` differs from it, so `st_dev`
    would never match the lock table."""
    try:
        with open(f"{_PROC_ROOT}/self/fdinfo/{fd}", "r", encoding="ascii", errors="replace") as fh:
            mnt_id = None
            for line in fh:
                if line.startswith("mnt_id:"):
                    mnt_id = int(line.split(":", 1)[1].strip())
                    break
        if mnt_id is None:
            return None
        with open(f"{_PROC_ROOT}/self/mountinfo", "r", encoding="ascii", errors="replace") as fh:
            for line in fh:
                fields = line.split()
                if len(fields) > 2 and fields[0] == str(mnt_id):
                    major, minor = fields[2].split(":")
                    return int(major), int(minor)
    except (OSError, ValueError):
        return None
    return None


def _linux_flock_write_holders(device: Tuple[int, int], inode: int) -> Optional[Set[int]]:
    try:
        with open(f"{_PROC_ROOT}/locks", "r", encoding="ascii", errors="replace") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return None
    holders: Set[int] = set()
    for line in lines:
        fields = line.split()
        # `1: FLOCK  ADVISORY  WRITE 1234 08:01:131077 0 EOF`
        # A blocked waiter is `1: -> FLOCK ...`; skip it.
        if len(fields) < 6 or fields[1] != "FLOCK" or fields[3] != "WRITE":
            continue
        try:
            pid = int(fields[4])
            # MAJ:MIN:INODE -- the kernel prints major and minor in hex
            # (`%02x:%02x`) and the inode in decimal. Both the device and the
            # inode must match: an inode number alone collides across
            # filesystems.
            maj_s, min_s, ino_s = fields[5].split(":")
            dev = (int(maj_s, 16), int(min_s, 16))
            ino = int(ino_s)
        except (ValueError, IndexError):
            continue
        if dev == device and ino == inode and pid > 0:  # pid 0: another pid namespace
            holders.add(pid)
    return holders


# ---------------------------------------------------------------------------
# macOS
# ---------------------------------------------------------------------------


_LIBC = None


def _libc():
    """The process-wide libc handle, loaded once."""
    global _LIBC
    if _LIBC is None:
        import ctypes

        _LIBC = ctypes.CDLL(None, use_errno=True)
    return _LIBC


def _darwin_token(pid: int) -> Tuple[str, Optional[str]]:
    try:
        import ctypes

        libc = _libc()
        mib = (ctypes.c_int * 4)(_CTL_KERN, _KERN_PROC, _KERN_PROC_PID, pid)
        buf = ctypes.create_string_buffer(_KINFO_BUFFER)
        size = ctypes.c_size_t(_KINFO_BUFFER)
        rc = libc.sysctl(mib, 4, buf, ctypes.byref(size), None, ctypes.c_size_t(0))
        if rc != 0:
            return _UNKNOWN, None
        if size.value == 0:
            return _GONE, None
        if size.value < _DARWIN_P_PID + 4:
            return _UNKNOWN, None
        raw = buf.raw
        # The answer must be about the pid we asked for; anything else means
        # the struct layout is not the one assumed above.
        if int.from_bytes(raw[_DARWIN_P_PID:_DARWIN_P_PID + 4], sys.byteorder, signed=True) != pid:
            return _UNKNOWN, None
        # kp_proc.p_un.__p_starttime: struct timeval at offset 0
        # (int64 tv_sec, int32 tv_usec at offset 8).
        sec = int.from_bytes(raw[0:8], sys.byteorder, signed=True)
        usec = int.from_bytes(raw[8:12], sys.byteorder, signed=True)
        if sec <= 0 or not 0 <= usec < 1_000_000:
            return _UNKNOWN, None
        if raw[_DARWIN_P_STAT] == _DARWIN_SZOMB:
            return _ZOMBIE, None
        return _OK, f"darwin:{sec}.{usec:06d}"
    except Exception:  # ctypes missing, symbol missing, anything
        return _UNKNOWN, None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _read_token(pid) -> Tuple[str, Optional[str]]:
    if not _valid_pid(pid):
        return _UNKNOWN, None
    try:
        if _is_linux():
            return _linux_token(pid)
        if _is_darwin():
            return _darwin_token(pid)
    except Exception:
        pass
    return _UNKNOWN, None


def start_token(pid) -> Optional[str]:
    """An opaque, stable-for-the-life-of-the-process token for `pid`, or `None`
    when the process is gone (a zombie included), unreadable, or the platform
    is unsupported."""
    status, token = _read_token(pid)
    return token if status == _OK else None


def is_zombie(pid) -> Optional[bool]:
    """`True` if `pid` is an exited, unreaped process (a zombie); `False` if
    it is a live process we could read; `None` when that cannot be told (gone,
    hidden by `hidepid`, unreadable, unsupported platform). Never raises."""
    try:
        status, _ = _read_token(pid)
    except Exception:
        return None
    if status == _ZOMBIE:
        return True
    if status == _OK:
        return False
    return None


def identity(pid, token) -> str:
    """Is `pid` still the process that had `token`?

    `'match'`, `'mismatch'` (pid reused), `'gone'` (no such process, or a
    zombie -- see the module docstring), or `'unknown'` (pid `None`/<=1,
    empty token, unreadable, unsupported platform). Never raises.
    """
    try:
        if pid is None or not _valid_pid(pid) or pid <= 1 or not token:
            return _UNKNOWN
        status, current = _read_token(pid)
        if status in (_GONE, _ZOMBIE):
            return _GONE
        if status != _OK or current is None:
            return _UNKNOWN
        return "match" if current == token else "mismatch"
    except Exception:
        return _UNKNOWN


def flock_write_holders(fd: int) -> Optional[Set[int]]:
    """Pids holding an exclusive `flock` on the file open as `fd`, or `None`
    when that cannot be determined (every platform but Linux, `/proc/locks`
    unreadable, or the file's device cannot be established from the mount).

    The match is on device AND inode (see `_linux_fd_device`). `None` is NOT
    "nobody": callers must treat it as "unverified". An empty set means the
    lock table was read and nobody holds it.
    """
    try:
        if not _is_linux() or not isinstance(fd, int) or isinstance(fd, bool) or fd < 0:
            return None
        device = _linux_fd_device(fd)
        if device is None:
            return None
        return _linux_flock_write_holders(device, os.fstat(fd).st_ino)
    except Exception:
        return None


def flock_holders_unavailable_reason(fd: int) -> str:
    """Why `flock_write_holders(fd)` returned `None`, as one of the stable
    `UNAVAILABLE_*` codes: `not-linux` (no lock-table query on this platform),
    `mount-device` (the pid file's mount device could not be established),
    `locks-unreadable` (`/proc/locks` could not be read), or `unknown`.
    Re-derives the answer, so call it only after a `None`. Never raises."""
    try:
        if not _is_linux():
            return UNAVAILABLE_NOT_LINUX
        if not isinstance(fd, int) or isinstance(fd, bool) or fd < 0:
            return UNAVAILABLE_UNKNOWN
        if _linux_fd_device(fd) is None:
            return UNAVAILABLE_MOUNT_DEVICE
        try:
            with open(f"{_PROC_ROOT}/locks", "rb") as fh:
                fh.read(1)
        except OSError:
            return UNAVAILABLE_LOCKS_UNREADABLE
    except Exception:
        pass
    return UNAVAILABLE_UNKNOWN


def process_uid(pid) -> Optional[int]:
    """The owning uid of process `pid` (Linux: the owner of `/proc/<pid>`), or
    `None` when it cannot be told (gone, hidden by `hidepid`, other platforms).
    Never raises."""
    try:
        if not _is_linux() or not _valid_pid(pid):
            return None
        return os.stat(f"{_PROC_ROOT}/{pid}").st_uid
    except Exception:
        return None

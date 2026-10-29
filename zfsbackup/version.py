"""The one place that knows the installed version of zfsbackup.

Stdlib only at import time, and it never imports the daemon: the CLI, the
daemon and `zfsbackup/__init__.py` (for the lazy `__version__`) all reach it.

The version has one source, the installed distribution's metadata
(`importlib.metadata.version(DIST_NAME)`, i.e. `pyproject.toml`'s
`[project] version`). Nothing else spells it. The `libzfseasy` package is a
separate distribution (`LIB_DIST_NAME`) released in lockstep; the version-skew
guard watches both installs, but `installed_version()` is zfsbackup's alone.

`installed_version()` deliberately does NOT cache. The self-restart on upgrade
(S-8) compares the version this process started with against what is on disk
now; a module-level cache would make that comparison always equal.
`importlib.metadata`'s own path cache is keyed on directory mtime, so it sees
a changed install.
"""

import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, distribution, version
from typing import Optional, Tuple

#: The distribution name (`[project] name`). It ships the `zfsbackup` package
#: only; `libzfseasy` is its own distribution (`LIB_DIST_NAME`).
DIST_NAME = "zfsbackup"

#: The ZFS bindings' distribution (`libs/libzfseasy`), a dependency pinned to
#: this version. The version-skew guard (S-8) records its identity too: it can
#: be reinstalled or upgraded without `zfsbackup` being touched.
LIB_DIST_NAME = "libzfseasy"

#: What `installed_version()` returns when the distribution is not installed
#: (a `git archive` tree or a bare checkout run without `pip install`).
UNKNOWN_VERSION = "0+unknown"

#: The daemon's exit status for "the installed version changed under me, restart
#: me" (S-8). The unit lists it in `RestartForceExitStatus=`. The daemon exits
#: with it for no other reason. (75 and 78 are the CLI's retry/config codes.)
EXIT_CODE_VERSION_CHANGED = 76


def installed_version() -> str:
    """The installed distribution's version, re-read from metadata on every
    call (no cache), or `UNKNOWN_VERSION` if it is not installed."""
    try:
        return version(DIST_NAME) or UNKNOWN_VERSION
    except (PackageNotFoundError, KeyError):    # KeyError: metadata without a Version (Python >= 3.15)
        return UNKNOWN_VERSION


@dataclass(frozen=True)
class CodeSignature:
    """What identifies the installed code, for the version-skew guard (S-8).

    `version` is the installed distribution's version (what
    `installed_version()` reports). `file` is the file whose identity is
    recorded: the distribution's dist-info `RECORD` (pip rewrites it on every
    install, upgrade and reinstall, and the dist-info directory's name carries
    the version, so even a same-version reinstall gives it a new inode) or, for
    an install without a `RECORD`, the package's own `__init__.py`. `ident` is
    that file's `(st_ino, st_mtime_ns, st_size)`. `file is None` means the
    distribution is not installed (`UNKNOWN_VERSION`).

    `lib_version`, `lib_file` and `lib_ident` are the same three facts for the
    `libzfseasy` distribution (its `RECORD`, else the `__init__.py` the import
    system resolves for it, loaded or not -- on a Debian install, where
    dh_python3 writes no `RECORD`, dpkg replaces files by rename, so the inode
    changes on every upgrade).
    `lib_version == ""` means the field was not recorded (a signature made by
    older code); `UNKNOWN_VERSION` with no file means the distribution is not
    installed. They never decide `installed`, which is zfsbackup's alone.

    `python` is `sys.version` of the process that read the signature and
    `exe` the `(st_ino, st_mtime_ns, st_size)` of `sys.executable` (stat
    follows symlinks, so a venv's `python` reports the binary it points at).
    `sys.version` never changes inside one process, but a `spawn` or
    `forkserver` worker is a *new* interpreter started from `sys.executable`
    as it is on disk now, so after an interpreter upgrade it reads a different
    `python` than its supervisor; the supervisor sees the replaced binary in
    `exe`. A standalone upgrade of another package (werkzeug, cryptography,
    ...) is NOT part of the signature.

    Plain data, so it pickles across `spawn` and `forkserver`."""

    version: str
    file: Optional[str]
    ident: Optional[Tuple[int, int, int]]
    python: str = ""
    exe: Optional[Tuple[int, int, int]] = None
    # Appended last: the workers rebuild a signature with `CodeSignature(*tuple)`,
    # so the field order is the `as_tuple()` order and new fields go at the end.
    lib_version: str = ""
    lib_file: Optional[str] = None
    lib_ident: Optional[Tuple[int, int, int]] = None

    @property
    def installed(self) -> bool:
        return self.version != UNKNOWN_VERSION

    def as_tuple(self) -> tuple:
        """The signature as a plain tuple, which is how it crosses to the
        workers (S-8): they compare tuples, so a field added later cannot make
        an old worker's `==` raise."""
        return (
            self.version, self.file, self.ident, self.python, self.exe,
            self.lib_version, self.lib_file, self.lib_ident,
        )

    @staticmethod
    def _describe(ver: str, file: Optional[str], ident: Optional[Tuple[int, int, int]]) -> str:
        if file is None or ident is None:
            return ver
        ino, mtime_ns, size = ident
        return f"{ver} ({os.path.basename(file)} inode {ino}, mtime_ns {mtime_ns}, {size} bytes)"

    def __str__(self) -> str:
        py = self.python.split()[0] if self.python else "?"
        text = self._describe(self.version, self.file, self.ident)
        if self.lib_version:
            text += f", {LIB_DIST_NAME} " + self._describe(
                self.lib_version, self.lib_file, self.lib_ident
            )
        return f"{text}, Python {py}"

    def change_from(self, old: "CodeSignature") -> str:
        """`old` -> `self` in words."""
        if not self.installed and old.installed:
            return "zfsbackup is being reinstalled or was removed"
        if old.version != self.version:
            return f"{old.version} -> {self.version}"
        if (old.file, old.ident) != (self.file, self.ident):
            return f"{self.version} was reinstalled ({old} -> {self})"
        if old.lib_version and self.lib_version:
            if self.lib_version == UNKNOWN_VERSION and old.lib_version != UNKNOWN_VERSION:
                return f"{LIB_DIST_NAME} is being reinstalled or was removed"
            if old.lib_version != self.lib_version:
                return f"{LIB_DIST_NAME} {old.lib_version} -> {self.lib_version}"
            if (old.lib_file, old.lib_ident) != (self.lib_file, self.lib_ident):
                return f"{LIB_DIST_NAME} {self.lib_version} was reinstalled"
        if not old.lib_version and self.lib_version:
            # A baseline from an older zfsbackup: its signature had no
            # libzfseasy fields, so it cannot be compared on them.
            if (old.python, old.exe) == (self.python, self.exe):
                return "the baseline signature came from an older zfsbackup (signature format changed)"
        if old.python != self.python:
            return (
                f"the Python interpreter changed "
                f"({old.python.split()[0] if old.python else '?'} -> "
                f"{self.python.split()[0] if self.python else '?'})"
            )
        return "the Python interpreter binary was replaced"


def _record_path(dist) -> Optional[str]:
    """Where `dist`'s `RECORD` would be: its dist-info directory (the
    `PathDistribution` path) plus `RECORD`, else the public `files` list."""
    base = getattr(dist, "_path", None)
    if base is not None:
        return os.path.join(os.fspath(base), "RECORD")
    for f in dist.files or ():
        if f.name == "RECORD" and f.parent.name.endswith(".dist-info"):
            return os.fspath(f.locate())
    return None


def _stat_ident(path: str) -> Optional[Tuple[int, int, int]]:
    """`(st_ino, st_mtime_ns, st_size)`, or `None` if `path` is missing."""
    try:
        st = os.stat(path)
    except (FileNotFoundError, NotADirectoryError):
        return None
    return (st.st_ino, st.st_mtime_ns, st.st_size)


def _lib_init_path(dist) -> str:
    """The `libzfseasy` package's `__init__.py`, the same answer whether or not
    this process has imported it: the import system's own (`find_spec` returns
    the loaded module's spec if it is imported, else searches `sys.path` the
    way an import would; for a top-level name it imports nothing). Preferring
    the loaded module's `__file__` or the distribution's recorded location
    instead would make the answer depend on import state, and a supervisor that
    has the package imported could then disagree with a worker that has not
    (a PYTHONPATH-shadowed copy on an install without a `RECORD`). Only if the
    import system finds nothing usable does it fall back to where the
    distribution says the file lives. May raise (the caller answers `None`)."""
    try:
        spec = importlib.util.find_spec(LIB_DIST_NAME)
    except (ImportError, ValueError):
        spec = None     # ValueError: a module whose __spec__ is None
    origin = getattr(spec, "origin", None)
    if origin and os.path.isabs(origin) and os.path.basename(origin) == "__init__.py":
        return os.path.abspath(origin)
    return os.fspath(dist.locate_file(f"{LIB_DIST_NAME}/__init__.py"))


def _lib_identity() -> Tuple[str, Optional[str], Optional[Tuple[int, int, int]]]:
    """`(lib_version, lib_file, lib_ident)` for the `libzfseasy` distribution,
    by one distribution lookup: its `RECORD`'s identity, else its
    `__init__.py`'s (a missing `RECORD`, as on a dh_python3 install). Not
    installed -> `(UNKNOWN_VERSION, None, None)`. Any other failure raises
    (the caller answers `None`, "cannot tell")."""
    try:
        dist = distribution(LIB_DIST_NAME)
        ver = dist.version or UNKNOWN_VERSION
    except (PackageNotFoundError, KeyError):
        return (UNKNOWN_VERSION, None, None)
    if ver == UNKNOWN_VERSION:
        return (UNKNOWN_VERSION, None, None)
    path = _record_path(dist)
    ident = _stat_ident(path) if path is not None else None
    if ident is None:
        path = _lib_init_path(dist)
        st = os.stat(path)
        ident = (st.st_ino, st.st_mtime_ns, st.st_size)
    return (ver, path, ident)


def code_signature() -> Optional[CodeSignature]:
    """The installed code's `CodeSignature`, re-read on every call: ONE
    distribution lookup per distribution (`zfsbackup` and `libzfseasy`; each
    one's `.version` and dist-info path, so the two cannot come from different
    installs) and one `stat` each (`RECORD`) plus one for the interpreter,
    never a hash of file contents, never a `zfs` call, nothing cached (S-8
    compares this with the value taken at start).

    * `zfsbackup` is not installed (a bare checkout, or removed mid-upgrade)
      -> a signature with `UNKNOWN_VERSION` and no file (`libzfseasy` is not
      looked at: that signature already differs from any installed one);
    * `libzfseasy` is not installed -> its fields read `UNKNOWN_VERSION` and
      no file; that is a difference from a signature that had it;
    * a `RECORD` is missing or vanished (mid-upgrade, or an install that has
      none, like dh_python3's) -> the package's `__init__.py` stands in, which
      differs from any `RECORD` signature, so a vanishing `RECORD` reads as a
      change (and dpkg's replace-by-rename gives it a new inode);
    * anything unexpected (an I/O error that is not "missing") -> `None`,
      "cannot tell", which a caller must treat as no change."""
    try:
        python = sys.version
        exe = _stat_ident(sys.executable) if sys.executable else None
        try:
            dist = distribution(DIST_NAME)
            ver = dist.version or UNKNOWN_VERSION
            path = _record_path(dist) if ver != UNKNOWN_VERSION else None
        except (PackageNotFoundError, KeyError):
            # KeyError: metadata without a Version. Python 3.15 raises it where <= 3.14 returned None (and warned).
            # Either way the distribution is not (or no longer) installed, not "cannot tell".
            return CodeSignature(UNKNOWN_VERSION, None, None, python, exe)
        if ver == UNKNOWN_VERSION:
            return CodeSignature(UNKNOWN_VERSION, None, None, python, exe)
        ident = _stat_ident(path) if path is not None else None
        if ident is None:
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "__init__.py")
            st = os.stat(path)
            ident = (st.st_ino, st.st_mtime_ns, st.st_size)
        lib_version, lib_file, lib_ident = _lib_identity()
        return CodeSignature(ver, path, ident, python, exe, lib_version, lib_file, lib_ident)
    except Exception:
        return None


def installed_in_site_packages() -> bool:
    """Whether the zfsbackup package files live under a `site-packages` or
    `dist-packages` directory (a real install, not a checkout): then missing
    dist-info metadata means an install in flight, not a development tree."""
    parts = os.path.abspath(__file__).split(os.sep)
    return "site-packages" in parts or "dist-packages" in parts


def editable_install() -> bool:
    """Whether `zfsbackup` or `libzfseasy` is an editable install
    (`direct_url.json` says `dir_info.editable`). Read once at start for a log
    line, not part of the signature; `False` when it cannot be told."""
    for name in (DIST_NAME, LIB_DIST_NAME):
        try:
            text = distribution(name).read_text("direct_url.json")
            if text and json.loads(text).get("dir_info", {}).get("editable"):
                return True
        except Exception:
            continue
    return False


def version_string(component: str) -> str:
    """The `--version` line: `<component> <ver> (wire protocol N, config
    schema <head>)`, e.g. `zfsbackupd 0.1.0 (wire protocol 1, config schema
    5d2f8a60c1e4)`.

    The protocol and schema imports are local so that importing this module
    stays cheap: the schema head lives in the SQLAlchemy/Alembic-backed store
    package, which only `--version` itself needs.
    """
    from zfsbackup.protocol import PROTOCOL_VERSION
    from zfsbackup.config.store.migrate import EXPECTED_HEAD

    return (
        f"{component} {installed_version()} "
        f"(wire protocol {PROTOCOL_VERSION}, config schema {EXPECTED_HEAD})"
    )

"""The one place that knows the installed version of zfsbackup.

Stdlib only at import time, and it never imports the daemon: the CLI, the
daemon and `zfsbackup/__init__.py` (for the lazy `__version__`) all reach it.

The version has one source, the installed distribution's metadata
(`importlib.metadata.version(DIST_NAME)`, i.e. `pyproject.toml`'s
`[tool.poetry] version`). Nothing else spells it.

`installed_version()` deliberately does NOT cache. The self-restart on upgrade
(S-8) compares the version this process started with against what is on disk
now; a module-level cache would make that comparison always equal.
`importlib.metadata`'s own path cache is keyed on directory mtime, so it sees
a changed install.
"""

from importlib.metadata import PackageNotFoundError, version

#: The distribution name (`[tool.poetry] name`). It ships both the `zfsbackup`
#: and the `libzfseasy` packages.
DIST_NAME = "zfsbackup"

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
        return version(DIST_NAME)
    except PackageNotFoundError:
        return UNKNOWN_VERSION


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

"""ZFS Backup Daemon - Automated ZFS snapshot management with retention policies."""

# `__version__` is resolved on first access from the installed distribution's
# metadata (`zfsbackup.version`), so importing the package stays trivial and the
# version has a single source (`pyproject.toml`).


def __getattr__(name):
    if name == "__version__":
        from zfsbackup.version import installed_version
        return installed_version()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted([*globals(), "__version__"])

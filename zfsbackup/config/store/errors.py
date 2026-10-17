"""Exceptions for the two ways a config write can lose a race.

Deliberately NOT subclasses of `ConfigPathError` or `SchemaError`: those
mean "this database is unusable" (the CLI maps them to exit 78), whereas
both of these mean "the database is fine, someone else got there first --
look again and retry" (the CLI maps them to exit 75). Kept in a module of
their own, with no store imports, so `mapper.py` (which raises
`ConfigConflict`), `paths.py` (which raises `ConfigBusy` from the writer
lock) and `access.py` can all import them without a cycle.
"""

from __future__ import annotations

from typing import Optional

__all__ = ["ConfigBusy", "ConfigConflict"]


class ConfigConflict(Exception):
    """A compare-and-swap write found the stored `GlobalSettings.generation`
    different from the one the caller's snapshot was read at: somebody else
    (another CLI invocation, an `import`) committed in between. Nothing was
    written. The caller must re-read, redo its edit and try again; it must
    never retry the same write blindly -- that would overwrite the other
    writer's change, which is exactly what the check exists to prevent.

    `actual` is `None` when the store holds no configuration at all (no
    `GlobalSettings` row).
    """

    def __init__(self, expected: int, actual: Optional[int]) -> None:
        self.expected = expected
        self.actual = actual
        now = "no configuration" if actual is None else f"generation {actual}"
        super().__init__(
            f"the config changed underneath this write (read at generation "
            f"{expected}, now {now}); nothing was written. Re-read and retry."
        )


class ConfigBusy(Exception):
    """Another writer holds the config store and did not let go in time: the
    `<db>.writer.lock` flock could not be taken within its timeout, or SQLite
    reported `database is locked` after `busy_timeout`. Nothing was written.
    Safe to retry as is -- unlike `ConfigConflict`, no data changed hands.
    """

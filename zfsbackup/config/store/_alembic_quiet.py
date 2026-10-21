"""Per-thread silencing of Alembic's routine INFO lines for `check_schema`.

`MigrationContext.configure` logs two INFO lines ("Context impl SQLiteImpl",
"Will assume non-transactional DDL") on EVERY call. `check_schema` runs on
every trust read, and the API worker reads trust every 2 s from several
threads, so those lines flooded the daemon log. They are silenced for the
read-only check only: `migrate`/`upgrade_schema`/`ensure_schema` build their
contexts outside `quiet_alembic` and keep their output.

Done with a per-thread flag consulted by ONE permanent filter, not by
temporarily raising the logger's level: a level save/restore is racy under
concurrent callers (thread B saves the WARNING thread A just set, then
restores it after A restored INFO, leaving the logger muted for good, or the
reverse) and would also swallow another thread's real `migrate` output. The
record is emitted synchronously in the calling thread, so a thread-local flag
is exact.

**Why this is its own module, importing nothing from the store.** The filter
is installed on a third-party logger that lives in `logging`'s manager for
the life of the interpreter, and a filter instance pins its class's methods,
whose `__globals__` is the DEFINING module's dict. When the filter was defined
in `migrate.py`, that dict pinned `open_config_connection` -> `paths.py`'s
dict -> `get_engine` -> `db.py`'s dict -> `_ENGINES` -> every cached engine
and its pooled DBAPI connections. Those connections then outlived interpreter
finalization: `sqlite3_close` never ran, so a short-lived process that exits
without `dispose_all()` (a `zfsbackup-config import`, any script) left the
WAL un-checkpointed and `-wal`/`-shm` behind on disk. Before the filter,
module teardown freed the engines and the last close removed both sidecars.
Leftover sidecars are not corruption, but they read as "another process has
this database open" to the failed-first-import cleanup (`importer.py`), which
then refuses to remove the file, and they keep committed data out of the main
file for anyone copying `config.db` alone. Keep this module's globals free of
anything that can reach an engine, a session or a connection.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Iterator

__all__ = ["ALEMBIC_LOGGER", "QUIET_FILTER_MARKER", "QuietWhileChecking", "quiet", "quiet_alembic"]

ALEMBIC_LOGGER = logging.getLogger("alembic.runtime.migration")
quiet = threading.local()

QUIET_FILTER_MARKER = "_zfsbackup_quiet_while_checking"


class QuietWhileChecking(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(quiet, "depth", 0)


# Kept for the name callers and tests match on (`type(f).__name__`).
QuietWhileChecking.__name__ = QuietWhileChecking.__qualname__ = "_QuietWhileChecking"
setattr(QuietWhileChecking, QUIET_FILTER_MARKER, True)

# Idempotent across reloads (`importlib.reload` of this module or of
# `migrate`, which only re-imports these names). Matched on a marker
# attribute, NOT `isinstance(_f, QuietWhileChecking)`: a reload has already
# rebound that name to a NEW class, which the previous filter instance is not
# an instance of, so an isinstance guard never removed it and one stale filter
# accumulated per reload.
def _install() -> None:
    for stale in list(ALEMBIC_LOGGER.filters):
        if getattr(stale, QUIET_FILTER_MARKER, False) is True:
            ALEMBIC_LOGGER.removeFilter(stale)
    ALEMBIC_LOGGER.addFilter(QuietWhileChecking())


_install()


@contextmanager
def quiet_alembic() -> Iterator[None]:
    """Drop `alembic.runtime.migration` records emitted by THIS thread inside
    the block. Re-entrant; other threads are unaffected."""
    quiet.depth = getattr(quiet, "depth", 0) + 1
    try:
        yield
    finally:
        quiet.depth -= 1

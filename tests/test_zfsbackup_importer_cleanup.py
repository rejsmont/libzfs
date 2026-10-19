"""Fix-up round A-11 / A-11.2 -- a failed first import never deletes a database
somebody else is using, and never starts next to orphaned sidecars.

The hazard (R2, reproduced in the review): `import_yaml` creates the target
(`created=True`), a later phase fails, and the cleanup used to `unlink` the file
because "the inode is still the one I made". That proves only that it was not
*replaced*, not that it was not *adopted*:

- a concurrent `import` of the same path takes the re-import branch, commits,
  is told it succeeded -- and then has its committed configuration unlinked;
- a connected reader has the DB, `-wal` and `-shm` unlinked under it.

The cleanup now removes the file only after proving nobody else uses it (see
`importer._remove_if_unused`): the inode is still ours, no `-wal`/`-shm`/
`-journal` exists, and a raw `BEGIN IMMEDIATE` probe finds `global_settings`
absent or empty. It never unlinks a sidecar, and it logs one WARNING per
skipped cleanup. A-11.2 refuses a first import next to orphaned sidecars.

The in-process tests make the second party deterministic by *being* it inside
the failing phase (a wrapper around `open_config_session`): no timing. The
`integration` class re-runs the two measured reproductions across a real
`fork` in a timed, killable driver subprocess.

Mutation counts (scratch copy of the tree, one revert at a time; tests in this
file, plus the pre-existing ones where noted, that then fail): the old
unconditional `unlink` 8; no inode check 2; no live-sidecar check 3; no probe 7;
probe run on zero-byte files 1; no swap re-check before the unlink 1; no
sidecar re-check before the unlink 1; a failed `dispose_all()` not stopping the
cleanup 1; a busy/garbage probe treated as "empty" 2; the probe ignoring a
committed row 4; sidecars unlinked too 1; A-11.2 refusal removed 7, run after
the directory work 1, run even when the database exists 5 (4 here plus
`test_failed_import_over_existing_db_preserves_it`).

A-21 (the fingerprint, the concurrent-create message), same method. The
fingerprint is taken ONCE before the probe and compared at step 5; the stall
tests (`stall`) land a whole concurrent import after the probe's close()
("after_probe" -- the gap the removed retake turned into the baseline), just
before step 5's `_file_id` and just before its fingerprint lstat. Mutations:
the comparison removed 25; a fingerprint of `(ino, size)` only 23; without
`st_ctime_ns` 12; without `st_mtime_ns` 5; the post-probe retake put back 14
(including the three full-import stall tests and the cross-process one at
"after_probe"); zero-byte files exempt 10; the "changed" skip under another
kind 14; its next-step text reverted 5; no re-look at the database in
`_refuse_orphaned_sidecars` 6; the concurrent-create message naming a
hardcoded -wal/-shm instead of the sidecars found 3; that re-look always
raising the concurrent-create error 10 (7 pre-existing orphan tests); the new
error dropped from `zfsbackup.config.store` or `paths.__all__` 1 each. The
`integration` class repeats the measured reproduction with B as a real
separate process.

15.5 (the pre-lock call returns silently once the database has appeared; the
under-lock call still raises), scratch copy, tests failing in this file: the
`before_lock` early return removed 3; the pre-lock call not passing
`before_lock=True` 2; the "created concurrently" branch removed altogether 9.
"""

import json
import logging
import multiprocessing
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

from zfsbackup.config.store import (
    ConfigBusy,
    ConfigDbCreatedConcurrently,
    ConfigDbOrphanedSidecars,
    ConfigPathError,
    dispose_all,
    import_yaml,
    load_config,
    open_config_session,
    resolve_config_path,
)
from zfsbackup.config.store import importer as importer_module
from zfsbackup.config.store import paths as store_paths

# No module-level tier marker: the in-process classes are tier-less (conftest
# files them under `unit`), and the cross-process class is `integration` --
# a module-level `unit` would double-count it (tests/test_marker_tiers.py).

ROOT = Path(__file__).resolve().parent.parent
FORK_AVAILABLE = "fork" in multiprocessing.get_all_start_methods() and hasattr(os, "fork")

YAML_A = "datasets:\n  - name: pool/A\n    frequency: 1h\n"
YAML_B = "datasets:\n  - name: pool/B\n    frequency: 1h\n"


def warnings_of(caplog):
    """The cleanup's WARNINGs only: the unrelated "directory already exists
    (mode ...)" notice every import into a pre-made directory logs is dropped."""
    return [
        r.getMessage() for r in caplog.records
        if r.levelno == logging.WARNING and "already exists (mode" not in r.getMessage()
    ]


def listing(directory: Path):
    return sorted(p.name for p in directory.iterdir())


#: Since item 15.4 every `import_yaml` that gets as far as the target takes
#: `<db>.writer.lock`; it is an empty file, created 0660 and never deleted,
#: so it outlives even a failed first import's unlink of the database itself.
LOCK = "config.db" + store_paths.WRITER_LOCK_SUFFIX


@contextmanager
def lock_ignoring_opener():
    """Run the body as an opener that does NOT honour the writer lock (the
    `sqlite3` shell, an old zfsbackup, a script). The A-11 cleanup heuristics
    (inode, sidecars, probe, fingerprint) exist for exactly that party now:
    a lock-honouring second writer cannot run inside A's critical section, so
    the in-thread "B commits during A's failing phase" simulations model B
    this way. B's calls to `import_yaml` resolve `importer.writer_lock` at call
    time, so patching it here leaves A's already-entered lock alone."""

    @contextmanager
    def no_lock(*_a, **_k):
        yield None

    real = importer_module.writer_lock
    importer_module.writer_lock = no_lock
    try:
        yield
    finally:
        importer_module.writer_lock = real


def import_ignoring_the_lock(path, resolved):
    with lock_ignoring_opener():
        return import_yaml(path, resolved)


def with_lock_timeout(monkeypatch, seconds):
    """Make every FUTURE `import_yaml` wait at most `seconds` for the writer
    lock (the default is 10 s, far too long for a test)."""
    real = importer_module.writer_lock

    def short(resolved, *, timeout=seconds):
        return real(resolved, timeout=timeout)

    monkeypatch.setattr(importer_module, "writer_lock", short)


def inodes(*paths):
    return [os.lstat(p).st_ino for p in paths]


def locked(*_a, **_k):
    raise OperationalError("BEGIN IMMEDIATE", {}, Exception("database is locked"))


@pytest.fixture
def yaml_a(tmp_path):
    path = tmp_path / "a.yaml"
    path.write_text(YAML_A)
    return path


@pytest.fixture
def yaml_b(tmp_path):
    path = tmp_path / "b.yaml"
    path.write_text(YAML_B)
    return path


@pytest.fixture
def store(tmp_path):
    """(target path, resolved) inside a dedicated directory, so a directory
    listing is exactly the database and its sidecars."""
    directory = tmp_path / "store"
    directory.mkdir()
    target = directory / "config.db"
    return target, resolve_config_path(str(target))


def fail_phase3(monkeypatch, before=None):
    """Make phase 3 fail like a locked database. `before(real_open)` runs
    first, inside the failing phase -- the moment a concurrent party could
    act, after phase 2 committed and closed."""
    real = importer_module.open_config_session

    def failing(*args, **kwargs):
        # Back to the real function, so anything `before` runs (a full
        # `import_yaml` for the concurrent writer) uses the real one.
        monkeypatch.setattr(importer_module, "open_config_session", real)
        if before is not None:
            before()
        locked()

    monkeypatch.setattr(importer_module, "open_config_session", failing)


def make_plain_db(path: Path, *, config_row=False, schema=True) -> None:
    """A rollback-journal (non-WAL) SQLite file, so a held lock leaves no
    sidecar behind -- the only way to reach the probe with a lock held."""
    conn = sqlite3.connect(str(path))
    if schema:
        conn.execute("CREATE TABLE global_settings (id INTEGER PRIMARY KEY)")
        if config_row:
            conn.execute("INSERT INTO global_settings (id) VALUES (1)")
    else:
        conn.execute("CREATE TABLE unrelated (x)")
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# R2 (a): another writer's committed import survives
# ---------------------------------------------------------------------------


class TestAnotherWritersImportSurvives:
    def test_concurrent_reimport_that_committed_is_not_deleted(
        self, store, yaml_a, yaml_b, monkeypatch, caplog
    ):
        target, resolved = store

        def writer_b():
            result = import_ignoring_the_lock(yaml_b, resolved)  # re-import: commits
            assert result.created is False
            dispose_all()

        fail_phase3(monkeypatch, before=writer_b)

        with caplog.at_level(logging.INFO):
            with pytest.raises(OperationalError, match="database is locked"):
                import_yaml(yaml_a, resolved)  # A: the failing first import
        monkeypatch.undo()

        # B's committed configuration is intact...
        assert target.exists()
        with open_config_session(resolved, readonly=True) as session:
            assert [d.name for d in load_config(session).datasets] == ["pool/B"]
        # ...and A's caller is told, in so many words, what did not happen.
        (message,) = [m for m in warnings_of(caplog) if "NOT removed" in m]
        assert str(target) in message
        assert "Another process has since imported a configuration into" in message
        assert "it was left intact" in message
        assert "this import was NOT applied" in message
        assert message.endswith("Never delete its -wal/-shm files on their own.")
        assert not any("removed" in r.getMessage() and "created by" in r.getMessage()
                       for r in caplog.records)

    def test_the_original_error_is_not_masked_by_the_cleanup(
        self, store, yaml_a, yaml_b, monkeypatch
    ):
        _target, resolved = store
        fail_phase3(
            monkeypatch,
            before=lambda: (import_ignoring_the_lock(yaml_b, resolved), dispose_all()),
        )
        with pytest.raises(OperationalError):
            import_yaml(yaml_a, resolved)


# ---------------------------------------------------------------------------
# Item 15.4: a second import that HONOURS the writer lock
# ---------------------------------------------------------------------------


class TestLockHonouringSecondImport:
    """The A-11 race, re-stated under the writer lock. B cannot run inside A's
    critical section any more -- in the same thread it is a `RuntimeError`
    (not re-entrant), in another thread or process it WAITS. Either B runs
    before A looked (A is then a re-import, creating and deleting nothing) or
    after A's cleanup is over (B then finds what A left). No committed import
    is lost in either order."""

    @staticmethod
    def _start_b(yaml_b, resolved, box):
        def run():
            try:
                box["result"] = import_yaml(yaml_b, resolved)
                dispose_all()
            except BaseException as exc:  # reported to the test thread
                box["error"] = exc

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread

    def test_the_same_thread_cannot_nest_a_second_import(
        self, store, yaml_a, yaml_b, monkeypatch
    ):
        _target, resolved = store
        seen = {}

        def nested():
            with pytest.raises(RuntimeError, match="not re-entrant") as excinfo:
                import_yaml(yaml_b, resolved)
            seen["message"] = str(excinfo.value)

        fail_phase3(monkeypatch, before=nested)
        with pytest.raises(OperationalError, match="database is locked"):
            import_yaml(yaml_a, resolved)  # the nesting error did not mask it
        assert "this thread already holds it" in seen["message"]

    def test_b_waits_for_a_failed_first_imports_unlink_then_creates(
        self, store, yaml_a, yaml_b, monkeypatch, caplog
    ):
        target, resolved = store
        box = {}

        def b_starts_and_waits():
            box["thread"] = self._start_b(yaml_b, resolved, box)
            box["thread"].join(0.5)
            box["alive_in_phase3"] = box["thread"].is_alive()
            box["db_in_phase3"] = target.exists()
            box["listing_in_phase3"] = listing(target.parent)

        fail_phase3(monkeypatch, before=b_starts_and_waits)
        with caplog.at_level(logging.INFO):
            with pytest.raises(OperationalError, match="database is locked"):
                import_yaml(yaml_a, resolved)
        monkeypatch.undo()
        box["thread"].join(30)

        assert not box["thread"].is_alive() and "error" not in box, box
        # B was genuinely parked inside A's critical section...
        assert box["alive_in_phase3"] is True
        assert box["db_in_phase3"] is True
        # ...A's cleanup ran to the end (it unlinked its own empty database)...
        assert any("created by this failed import" in r.getMessage() for r in caplog.records)
        # ...and B then created its own, which is what is left.
        assert box["result"].created is True
        with open_config_session(resolved, readonly=True) as session:
            assert [d.name for d in load_config(session).datasets] == ["pool/B"]
        dispose_all()
        assert listing(target.parent) == sorted(["config.db", LOCK])

    def test_a_reimport_that_fails_leaves_the_second_import_s_commit_alone(
        self, store, yaml_a, yaml_b, monkeypatch
    ):
        """A is a RE-import over an existing database and fails in phase 3
        (rolled back); B waits and then commits. Final state: B's."""
        target, resolved = store
        import_yaml(yaml_a, resolved)
        dispose_all()
        box = {}

        def b_starts():
            box["thread"] = self._start_b(yaml_b, resolved, box)
            box["thread"].join(0.3)
            box["alive"] = box["thread"].is_alive()

        fail_phase3(monkeypatch, before=b_starts)
        with pytest.raises(OperationalError):
            import_yaml(yaml_a, resolved)
        monkeypatch.undo()
        box["thread"].join(30)

        assert box["alive"] is True and "error" not in box, box
        assert box["result"].created is False
        with open_config_session(resolved, readonly=True) as session:
            assert [d.name for d in load_config(session).datasets] == ["pool/B"]

    def test_b_gives_up_with_config_busy_and_a_s_cleanup_still_runs(
        self, store, yaml_a, yaml_b, monkeypatch, caplog
    ):
        target, resolved = store
        box = {}

        def b_times_out():
            with_lock_timeout(monkeypatch, 0.2)

            def run():
                try:
                    import_yaml(yaml_b, resolved)
                except ConfigBusy as exc:
                    box["busy"] = exc
                except BaseException as exc:  # pragma: no cover - reported below
                    box["other"] = exc

            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            thread.join(10)
            box["finished"] = not thread.is_alive()

        fail_phase3(monkeypatch, before=b_times_out)
        with caplog.at_level(logging.INFO):
            with pytest.raises(OperationalError, match="database is locked"):
                import_yaml(yaml_a, resolved)
        monkeypatch.undo()

        assert box.get("finished") is True and "other" not in box, box
        busy = box["busy"]
        assert isinstance(busy, ConfigBusy)
        assert str(target) in str(busy)
        assert not isinstance(busy, ConfigPathError)  # exit 75, not 78
        # Nothing of B's was written: A's cleanup found an empty schema-only
        # database it had itself created, and removed it.
        assert any("created by this failed import" in r.getMessage() for r in caplog.records)
        assert listing(target.parent) == [LOCK]

    def test_the_lock_is_released_when_the_import_fails(self, store, yaml_a, yaml_b, monkeypatch):
        """A failed import must not leave the lock held: the next one runs."""
        target, resolved = store
        fail_phase3(monkeypatch)
        with pytest.raises(OperationalError):
            import_yaml(yaml_a, resolved)
        monkeypatch.undo()
        with_lock_timeout(monkeypatch, 0.0)  # one try: a held lock would be ConfigBusy
        assert import_yaml(yaml_b, resolved).created is True


# ---------------------------------------------------------------------------
# R2 (b): a connected reader keeps its files
# ---------------------------------------------------------------------------


class TestConnectedReaderKeepsItsFiles:
    def test_db_wal_and_shm_keep_their_inodes_and_the_reader_still_reads(
        self, store, yaml_a, monkeypatch, caplog
    ):
        target, resolved = store
        held = {}

        def reader_connects():
            conn = sqlite3.connect(str(target))
            conn.execute("select count(*) from alembic_version").fetchall()
            held["conn"] = conn
            held["before"] = inodes(
                target, Path(f"{target}-wal"), Path(f"{target}-shm")
            )

        fail_phase3(monkeypatch, before=reader_connects)
        try:
            with caplog.at_level(logging.WARNING):
                with pytest.raises(OperationalError):
                    import_yaml(yaml_a, resolved)
            monkeypatch.undo()

            assert listing(target.parent) == sorted(
                ["config.db", "config.db-shm", "config.db-wal", LOCK]
            )
            assert inodes(target, Path(f"{target}-wal"), Path(f"{target}-shm")) == held["before"]
            # The reader is not just "connected": its database is still readable.
            assert held["conn"].execute("select count(*) from alembic_version").fetchone()[0] >= 0

            (message,) = [m for m in warnings_of(caplog) if "NOT removed" in m]
            assert "exist (another process may have it open)" in message
            assert "another process has it open" not in message
            assert "config.db-wal" in message and "config.db-shm" in message
            assert "re-running the import reuses it" in message
            assert message.endswith("Never delete its -wal/-shm files on their own.")
        finally:
            held["conn"].close()

    def test_nothing_is_unlinked_while_a_reader_is_attached(
        self, store, yaml_a, monkeypatch
    ):
        target, resolved = store
        held = {}

        def reader_connects():
            held["conn"] = sqlite3.connect(str(target))
            held["conn"].execute("select count(*) from alembic_version").fetchall()

        unlinked = []
        real_unlink = os.unlink
        monkeypatch.setattr(
            importer_module.os, "unlink",
            lambda p, *a, **k: (unlinked.append(Path(p)), real_unlink(p, *a, **k))[1],
        )
        fail_phase3(monkeypatch, before=reader_connects)
        try:
            with pytest.raises(OperationalError):
                import_yaml(yaml_a, resolved)
        finally:
            held["conn"].close()
        assert unlinked == []

    def test_a_retry_after_the_reader_left_reuses_the_schema_only_database(
        self, store, yaml_a, monkeypatch
    ):
        # "re-running the import reuses it": the leftover is a schema-only DB
        # that the re-import path adopts and completes.
        target, resolved = store
        held = {}

        def reader_connects():
            held["conn"] = sqlite3.connect(str(target))
            held["conn"].execute("select count(*) from alembic_version").fetchall()

        with monkeypatch.context() as m:
            fail_phase3(m, before=reader_connects)
            with pytest.raises(OperationalError):
                import_yaml(yaml_a, resolved)
        held["conn"].close()
        dispose_all()

        result = import_yaml(yaml_a, resolved)
        assert result.created is False
        with open_config_session(resolved, readonly=True) as session:
            assert [d.name for d in load_config(session).datasets] == ["pool/A"]


# ---------------------------------------------------------------------------
# R2 (c): a zero-byte file from a phase-2 failure is removed
# ---------------------------------------------------------------------------


class TestZeroByteFileFromPhase2:
    def _fail_phase2(self, monkeypatch, *, before=None):
        def failing(*_a, **_k):
            if before is not None:
                before()
            raise RuntimeError("injected phase-2 failure")

        monkeypatch.setattr(importer_module, "open_config_connection", failing)

    def test_zero_byte_file_is_removed_and_the_retry_is_a_first_import(
        self, store, yaml_a, monkeypatch, caplog
    ):
        target, resolved = store
        observed = {}
        with monkeypatch.context() as m:
            self._fail_phase2(m, before=lambda: observed.update(size=target.stat().st_size))
            with caplog.at_level(logging.INFO):
                with pytest.raises(RuntimeError, match="phase-2"):
                    import_yaml(yaml_a, resolved)

        assert observed == {"size": 0}  # non-vacuous: it was the zero-byte file
        assert not os.path.lexists(target)
        assert f"removed {target}, created by this failed import" in [
            r.getMessage() for r in caplog.records if r.levelno == logging.INFO
        ]
        assert warnings_of(caplog) == []
        assert import_yaml(yaml_a, resolved).created is True

    def test_the_probe_is_skipped_for_a_zero_byte_file(self, store, yaml_a, monkeypatch):
        # Nobody else can have written to an empty file: no probe connection.
        target, resolved = store
        monkeypatch.setattr(
            importer_module, "_probe_config_empty",
            lambda *_a: pytest.fail("probed a zero-byte file"),
        )
        self._fail_phase2(monkeypatch)
        with pytest.raises(RuntimeError):
            import_yaml(yaml_a, resolved)
        assert not os.path.lexists(target)

    def test_a_sidecar_next_to_a_zero_byte_file_blocks_the_removal(
        self, store, yaml_a, monkeypatch, caplog
    ):
        target, resolved = store
        wal = Path(f"{target}-wal")
        self._fail_phase2(monkeypatch, before=lambda: wal.write_bytes(b""))

        with caplog.at_level(logging.WARNING):
            with pytest.raises(RuntimeError):
                import_yaml(yaml_a, resolved)

        assert target.exists() and wal.exists()  # nothing removed, sidecar untouched
        (message,) = warnings_of(caplog)
        assert f"Remove the empty {target} once nothing has it open, then retry." in message
        assert "config.db-wal" in message


# ---------------------------------------------------------------------------
# R2 (d)/(e): the probe, and the window between probe and unlink
# ---------------------------------------------------------------------------


class TestProbe:
    def test_empty_when_global_settings_is_absent(self, tmp_path):
        db = tmp_path / "x.db"
        make_plain_db(db, schema=False)
        assert importer_module._probe_config_empty(db) is True

    def test_empty_when_global_settings_has_no_rows(self, tmp_path):
        db = tmp_path / "x.db"
        make_plain_db(db)
        assert importer_module._probe_config_empty(db) is True

    def test_not_empty_when_global_settings_has_a_row(self, tmp_path):
        db = tmp_path / "x.db"
        make_plain_db(db, config_row=True)
        assert importer_module._probe_config_empty(db) is False

    def test_it_never_creates_a_missing_file(self, tmp_path):
        missing = tmp_path / "nope.db"
        with pytest.raises(sqlite3.Error):
            importer_module._probe_config_empty(missing)
        assert listing(tmp_path) == []

    def test_it_rolls_back_and_releases_the_database(self, tmp_path):
        db = tmp_path / "x.db"
        make_plain_db(db)
        importer_module._probe_config_empty(db)
        # The write lock is released: another writer can take it at once.
        other = sqlite3.connect(str(db), timeout=0.05, isolation_level=None)
        other.execute("BEGIN IMMEDIATE")
        other.execute("ROLLBACK")
        other.close()
        assert listing(tmp_path) == ["x.db"]

    def test_a_busy_database_raises_instead_of_answering(self, tmp_path, monkeypatch):
        db = tmp_path / "x.db"
        make_plain_db(db)
        holder = sqlite3.connect(str(db), isolation_level=None)
        holder.execute("BEGIN IMMEDIATE")
        real_connect = sqlite3.connect
        monkeypatch.setattr(
            importer_module.sqlite3, "connect",
            lambda *a, **k: real_connect(*a, **{**k, "timeout": 0.05}),
        )
        try:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                importer_module._probe_config_empty(db)
        finally:
            holder.execute("ROLLBACK")
            holder.close()


class TestRemoveIfUnused:
    """`_remove_if_unused(target, created_id)` driven directly, over plain
    files, so each branch of the algorithm is reached on purpose."""

    def _target(self, tmp_path, **kw):
        directory = tmp_path / "d"
        directory.mkdir()
        db = directory / "config.db"
        make_plain_db(db, **kw)
        return db, importer_module._file_id(db)

    def test_busy_probe_removes_nothing(self, tmp_path, monkeypatch, caplog):
        db, created_id = self._target(tmp_path)
        holder = sqlite3.connect(str(db), isolation_level=None)
        holder.execute("BEGIN IMMEDIATE")  # rollback mode: no sidecar appears
        real_connect = sqlite3.connect
        monkeypatch.setattr(
            importer_module.sqlite3, "connect",
            lambda *a, **k: real_connect(*a, **{**k, "timeout": 0.05}),
        )
        try:
            assert importer_module._sidecars_present(db) == []  # reaches the probe
            with caplog.at_level(logging.WARNING):
                importer_module._remove_if_unused(db, created_id)
        finally:
            holder.execute("ROLLBACK")
            holder.close()

        assert db.exists()
        (message,) = warnings_of(caplog)
        assert "could not confirm it is empty" in message
        assert "re-running the import reuses it" in message

    def test_a_non_database_file_removes_nothing(self, tmp_path, caplog):
        directory = tmp_path / "d"
        directory.mkdir()
        db = directory / "config.db"
        db.write_bytes(b"this is not a sqlite database" * 10)
        created_id = importer_module._file_id(db)
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)
        assert db.exists()
        (message,) = warnings_of(caplog)
        assert "could not confirm it is empty" in message

    def test_a_row_in_global_settings_means_another_import_committed(
        self, tmp_path, caplog
    ):
        db, created_id = self._target(tmp_path, config_row=True)
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)
        assert db.exists()
        (message,) = warnings_of(caplog)
        assert "Another process has since imported a configuration into" in message

    def test_an_empty_schema_only_database_is_removed(self, tmp_path, caplog):
        db, created_id = self._target(tmp_path)
        with caplog.at_level(logging.INFO):
            importer_module._remove_if_unused(db, created_id)
        assert not db.exists()
        assert warnings_of(caplog) == []
        assert any(
            r.levelno == logging.INFO
            and r.getMessage() == f"removed {db}, created by this failed import"
            for r in caplog.records
        )

    def test_a_sidecar_appearing_between_probe_and_unlink_removes_nothing(
        self, tmp_path, monkeypatch, caplog
    ):
        db, created_id = self._target(tmp_path)
        wal = Path(f"{db}-wal")
        real_probe = importer_module._probe_config_empty

        def probe_then_a_process_attaches(target):
            verdict = real_probe(target)
            wal.write_bytes(b"")  # another connection opens right after the verdict
            return verdict

        monkeypatch.setattr(importer_module, "_probe_config_empty", probe_then_a_process_attaches)
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)

        assert db.exists() and wal.exists()  # neither removed
        (message,) = warnings_of(caplog)
        assert "another process opened it during cleanup" in message
        assert "config.db-wal" in message

    def test_a_file_swapped_in_between_probe_and_unlink_is_not_removed(
        self, tmp_path, monkeypatch, caplog
    ):
        db, created_id = self._target(tmp_path)
        impostor = db.parent / "impostor"
        make_plain_db(impostor, config_row=True)
        real_probe = importer_module._probe_config_empty

        def probe_then_replace(target):
            verdict = real_probe(target)
            os.rename(impostor, target)
            return verdict

        monkeypatch.setattr(importer_module, "_probe_config_empty", probe_then_replace)
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)

        assert db.exists()
        assert importer_module._file_id(db) != created_id
        (message,) = warnings_of(caplog)
        assert "replaced during cleanup" in message

    def test_a_different_inode_from_the_start_is_left_alone(self, tmp_path, caplog):
        db, created_id = self._target(tmp_path)
        other = db.parent / "other"
        make_plain_db(other)
        os.rename(other, db)  # now a different file at the path
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)
        assert db.exists()
        (message,) = warnings_of(caplog)
        assert "not the file this import created" in message
        assert "left untouched" in message

    def test_a_file_that_is_already_gone_is_not_a_warning(self, tmp_path, caplog):
        db, created_id = self._target(tmp_path)
        db.unlink()
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)
        assert warnings_of(caplog) == []

    def test_live_sidecars_remove_nothing_and_are_never_unlinked(
        self, tmp_path, monkeypatch, caplog
    ):
        db, created_id = self._target(tmp_path)
        sidecars = [Path(f"{db}{sfx}") for sfx in ("-wal", "-shm", "-journal")]
        for sidecar in sidecars:
            sidecar.write_bytes(b"x")
        monkeypatch.setattr(
            importer_module.os, "unlink", lambda *_a, **_k: pytest.fail("unlinked something")
        )
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)
        assert db.exists() and all(s.exists() for s in sidecars)
        (message,) = warnings_of(caplog)
        assert "config.db-wal, config.db-shm, config.db-journal" in message

    def test_a_failing_unlink_is_a_warning_not_an_exception(
        self, tmp_path, monkeypatch, caplog
    ):
        db, created_id = self._target(tmp_path)

        def refuse(*_a, **_k):
            raise PermissionError(13, "Permission denied")

        monkeypatch.setattr(importer_module.os, "unlink", refuse)
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)
        assert db.exists()
        (message,) = warnings_of(caplog)
        assert "could not unlink it" in message


class TestRemoveCreatedTarget:
    def test_dispose_all_failure_removes_nothing_and_does_not_mask_the_error(
        self, store, yaml_a, monkeypatch, caplog
    ):
        target, resolved = store

        def broken():
            raise RuntimeError("pool is wedged")

        real_dispose = importer_module.dispose_all
        fail_phase3(monkeypatch)
        # Only the importer's own cleanup call is broken; the fixture's
        # teardown uses the real function.
        monkeypatch.setattr(importer_module, "dispose_all", broken)
        try:
            with caplog.at_level(logging.WARNING):
                with pytest.raises(OperationalError):  # the REAL error, not "wedged"
                    import_yaml(yaml_a, resolved)
        finally:
            monkeypatch.setattr(importer_module, "dispose_all", real_dispose)

        assert target.exists()
        (message,) = [m for m in warnings_of(caplog) if "NOT removed" in m]
        assert "could not close this process's database connections" in message
        assert message.endswith("Never delete its -wal/-shm files on their own.")

    def test_a_failed_first_import_unlinks_only_the_database(
        self, store, yaml_a, monkeypatch
    ):
        # SQLite removes the sidecars itself when the last connection closes;
        # the importer must never do it by path.
        target, resolved = store
        unlinked = []
        real_unlink = os.unlink
        monkeypatch.setattr(
            importer_module.os, "unlink",
            lambda p, *a, **k: (unlinked.append(Path(p)), real_unlink(p, *a, **k))[1],
        )
        fail_phase3(monkeypatch)
        with pytest.raises(OperationalError):
            import_yaml(yaml_a, resolved)

        # The database -- and only the database. The writer lock file is not
        # state, is never deleted (an unlink would let two writers lock two
        # different inodes), and stays.
        assert unlinked == [target]
        assert listing(target.parent) == [LOCK]
        assert (target.parent / LOCK).stat().st_size == 0


# ---------------------------------------------------------------------------
# A-21.1: the probe's verdict is stale once its lock is released -- the
# fingerprint `(st_ino, st_size, st_mtime_ns, st_ctime_ns)`, taken ONCE before
# the probe and compared at step 5, catches a writer that landed anywhere
# between the two (never retaken after the probe: the retake's own gap let a
# whole concurrent import become the baseline)
# ---------------------------------------------------------------------------

#: B, an opener that does NOT honour the writer lock (the cleanup heuristics'
#: remaining customer; a lock-honouring B cannot run inside A's cleanup at all).
B_IMPORT = (
    "import sys, logging, contextlib; logging.disable(logging.WARNING);"
    "import zfsbackup.config.store.importer as imp;"
    "imp.writer_lock = contextlib.contextmanager(lambda *a, **k: (yield None));"
    "from zfsbackup.config.store.importer import import_yaml;"
    "from zfsbackup.config.store.paths import resolve_config_path;"
    "r = import_yaml(sys.argv[1], resolve_config_path(sys.argv[2]));"
    "print('B created=%s' % r.created)"
)


#: B, an opener that DOES honour the writer lock (every zfsbackup-config writer).
B_IMPORT_HONOURING = (
    "import sys, logging; logging.disable(logging.WARNING);"
    "from zfsbackup.config.store.importer import import_yaml;"
    "from zfsbackup.config.store.paths import resolve_config_path;"
    "r = import_yaml(sys.argv[1], resolve_config_path(sys.argv[2]));"
    "print('B created=%s' % r.created)"
)


#: Where in `_remove_if_unused` a stalled cleanup lets another process run:
#: "after_top"      -- right after the top-of-function fingerprint (before step 2);
#: "after_probe"    -- right after `_probe_config_empty` returns, i.e. its
#:                     connection has closed (the window the post-probe retake
#:                     used to make fatal);
#: "before_step5"   -- just before step 5's `_file_id` lstat;
#: "before_compare" -- after step 5's `_file_id`, just before the fingerprint
#:                     comparison's own lstat.
STALL_POINTS = ("after_top", "after_probe", "before_step5", "before_compare")


def stall(monkeypatch, point, action):
    """Run `action()` once, at `point` of `_remove_if_unused`. Returns a dict
    with `events` (`"fingerprint"` / `"probe"` / `"action"`, in order) and
    `top` (the fingerprint the cleanup took before anything else)."""
    assert point in STALL_POINTS
    seen = {"events": [], "top": None}
    state = {"started": False, "fired": False, "file_ids": 0, "fingerprints": 0}
    real_fp = importer_module._fingerprint
    real_probe = importer_module._probe_config_empty
    real_file_id = importer_module._file_id

    def fire():
        if not state["fired"]:
            state["fired"] = True
            seen["events"].append("action")
            action()

    def fingerprint(path):
        state["fingerprints"] += 1
        if state["started"] and point == "before_compare":
            fire()
        value = real_fp(path)
        seen["events"].append("fingerprint")
        if not state["started"]:
            state["started"] = True
            seen["top"] = value
            if point == "after_top":
                fire()
        return value

    def probe(target):
        verdict = real_probe(target)
        seen["events"].append("probe")
        if point == "after_probe":
            fire()
        return verdict

    def file_id(path):
        # `_file_id` calls after the top fingerprint: 1 is step 2, 2 is step 5.
        if state["started"]:
            state["file_ids"] += 1
            if point == "before_step5" and state["file_ids"] == 2:
                fire()
        return real_file_id(path)

    monkeypatch.setattr(importer_module, "_fingerprint", fingerprint)
    monkeypatch.setattr(importer_module, "_probe_config_empty", probe)
    monkeypatch.setattr(importer_module, "_file_id", file_id)
    return seen


class TestFingerprint:
    def test_it_is_inode_size_mtime_and_ctime_from_lstat(self, tmp_path):
        path = tmp_path / "f"
        path.write_bytes(b"abc")
        st = os.lstat(path)
        assert importer_module._fingerprint(path) == (
            st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns,
        )

    def test_a_missing_path_is_none(self, tmp_path):
        assert importer_module._fingerprint(tmp_path / "nope") is None

    def test_a_symlink_is_fingerprinted_as_itself(self, tmp_path):
        real = tmp_path / "real"
        real.write_bytes(b"abc")
        link = tmp_path / "link"
        link.symlink_to(real)
        assert importer_module._fingerprint(link)[0] == os.lstat(link).st_ino
        assert importer_module._fingerprint(link) != importer_module._fingerprint(real)

    def test_utime_moves_mtime_and_ctime_but_not_inode_or_size(self, tmp_path):
        path = tmp_path / "f"
        path.write_bytes(b"abc")
        before = importer_module._fingerprint(path)
        time.sleep(0.05)
        os.utime(path, ns=(before[2] - 10**10, before[2] - 10**10))
        after = importer_module._fingerprint(path)
        assert after[:2] == before[:2]  # size alone would not notice
        assert after[2] != before[2] and after[3] != before[3]

    def test_chmod_moves_only_ctime(self, tmp_path):
        path = tmp_path / "f"
        path.write_bytes(b"abc")
        os.chmod(path, 0o600)
        before = importer_module._fingerprint(path)
        time.sleep(0.05)
        os.chmod(path, 0o640)
        after = importer_module._fingerprint(path)
        assert after[:3] == before[:3]
        assert after[3] != before[3]


class TestFingerprintStall:
    """A whole concurrent import runs while the cleanup is stalled somewhere
    between its first fingerprint and the unlink. The fingerprint is taken
    ONCE, before the probe, and step 5 compares against that: a retake after
    the probe would let a writer landing in the gap become the baseline."""

    CHANGED = "changed since it was checked"

    def _run_concurrent_import(self, yaml_b, resolved):
        time.sleep(0.05)  # a coarse-clock filesystem must see a later mtime/ctime
        # B ignores the writer lock: a lock-honouring B cannot run here at all
        # (A holds the lock through its cleanup; see TestLockHonouringSecondImport).
        result = import_ignoring_the_lock(yaml_b, resolved)  # re-import: commits
        assert result.created is False
        dispose_all()  # B "exits": last close checkpoints and removes the sidecars

    @pytest.mark.parametrize("point", ["after_probe", "before_step5", "before_compare"])
    def test_a_full_import_in_the_stall_window_leaves_the_database_alone(
        self, store, yaml_a, yaml_b, monkeypatch, caplog, point
    ):
        target, resolved = store
        seen = {}
        real_fp = importer_module._fingerprint  # not the recording wrapper

        def b_runs():
            self._run_concurrent_import(yaml_b, resolved)
            seen["after"] = real_fp(target)

        fail_phase3(monkeypatch)
        stalled = stall(monkeypatch, point, b_runs)
        with caplog.at_level(logging.WARNING):
            with pytest.raises(OperationalError, match="database is locked"):
                import_yaml(yaml_a, resolved)
        monkeypatch.undo()

        # Non-vacuous: the probe ran, B ran after the top fingerprint and
        # before step 5's comparison.
        assert stalled["events"] == ["fingerprint", "probe", "action", "fingerprint"]
        top, after = stalled["top"], seen["after"]
        # The measured case: same file, same size -- only the times moved.
        # (A size-only comparison would have unlinked B's configuration.)
        assert after[0] == top[0]
        assert after[1] == top[1]
        assert after[2] != top[2]
        assert after[3] != top[3]

        assert listing(target.parent) == sorted(["config.db", LOCK])
        with open_config_session(resolved, readonly=True) as session:
            assert [d.name for d in load_config(session).datasets] == ["pool/B"]
        (message,) = [m for m in warnings_of(caplog) if "NOT removed" in m]
        assert self.CHANGED in message
        assert "another process may have written to it" in message
        assert f"{target} was left intact." in message
        assert "If it holds no configuration, re-running the import reuses it." in message
        assert message.endswith("Never delete its -wal/-shm files on their own.")
        assert "it now holds a configuration" not in message  # not the "imported" kind
        assert not any(r.getMessage().startswith("removed ") for r in caplog.records)

    def test_the_original_error_is_not_masked_by_the_skip(
        self, store, yaml_a, yaml_b, monkeypatch
    ):
        _target, resolved = store
        fail_phase3(monkeypatch)
        stall(monkeypatch, "after_probe", lambda: self._run_concurrent_import(yaml_b, resolved))
        with pytest.raises(OperationalError):
            import_yaml(yaml_a, resolved)

    @pytest.mark.parametrize("point", ["after_probe", "before_step5", "before_compare"])
    @pytest.mark.parametrize("touch", ["rewrite", "utime", "chmod"])
    def test_a_touch_of_a_probed_database_is_noticed(
        self, tmp_path, monkeypatch, caplog, touch, point
    ):
        directory = tmp_path / "d"
        directory.mkdir()
        db = directory / "config.db"
        make_plain_db(db)
        created_id = importer_module._file_id(db)
        size = db.stat().st_size

        def touched():
            time.sleep(0.05)
            if touch == "rewrite":  # same bytes, same size: only mtime/ctime move
                with open(db, "r+b") as handle:
                    handle.write(db.read_bytes()[:1])
            elif touch == "utime":
                mtime = os.lstat(db).st_mtime_ns
                os.utime(db, ns=(mtime - 10**10, mtime - 10**10))
            else:
                os.chmod(db, 0o600 if (db.stat().st_mode & 0o777) != 0o600 else 0o640)

        stalled = stall(monkeypatch, point, touched)
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)

        assert stalled["events"] == ["fingerprint", "probe", "action", "fingerprint"]
        assert db.exists() and db.stat().st_size == size
        (message,) = warnings_of(caplog)
        assert self.CHANGED in message

    @pytest.mark.parametrize("point", ["after_top", "before_step5", "before_compare"])
    @pytest.mark.parametrize("touch", ["write", "utime", "chmod"])
    def test_a_zero_byte_file_touched_after_the_top_fingerprint_is_not_removed(
        self, store, yaml_a, monkeypatch, caplog, touch, point
    ):
        target, resolved = store

        def touched():
            time.sleep(0.05)
            if touch == "write":
                target.write_bytes(b"x")
            elif touch == "utime":
                mtime = os.lstat(target).st_mtime_ns
                os.utime(target, ns=(mtime - 10**10, mtime - 10**10))
            else:
                mode = os.stat(target).st_mode & 0o777
                os.chmod(target, 0o600 if mode != 0o600 else 0o640)

        def failing(*_a, **_k):
            raise RuntimeError("injected phase-2 failure")

        monkeypatch.setattr(importer_module, "open_config_connection", failing)
        stalled = stall(monkeypatch, point, touched)
        monkeypatch.setattr(
            importer_module, "_probe_config_empty",
            lambda *_a: pytest.fail("probed a zero-byte file"),
        )
        with caplog.at_level(logging.WARNING):
            with pytest.raises(RuntimeError, match="phase-2"):
                import_yaml(yaml_a, resolved)

        assert target.exists()
        assert stalled["top"][1] == 0
        # The top, then step 5's comparison -- no probe, nothing in between.
        assert stalled["events"].count("fingerprint") == 2
        (message,) = warnings_of(caplog)
        assert self.CHANGED in message
        assert f"{target} was left intact." in message

    def test_an_unchanged_fingerprint_still_removes_the_database(
        self, tmp_path, monkeypatch, caplog
    ):
        directory = tmp_path / "d"
        directory.mkdir()
        db = directory / "config.db"
        make_plain_db(db)
        created_id = importer_module._file_id(db)
        events = []
        real_fp, real_probe = importer_module._fingerprint, importer_module._probe_config_empty
        monkeypatch.setattr(
            importer_module, "_fingerprint",
            lambda p: (events.append("fingerprint"), real_fp(p))[1],
        )
        monkeypatch.setattr(
            importer_module, "_probe_config_empty",
            lambda t: (events.append("probe"), real_probe(t))[1],
        )
        with caplog.at_level(logging.INFO):
            importer_module._remove_if_unused(db, created_id)

        assert not db.exists()
        assert warnings_of(caplog) == []
        # The top one (kind/size and the baseline), the probe, step 5's
        # comparison -- and NO retake between the probe and step 5.
        assert events == ["fingerprint", "probe", "fingerprint"]

    def test_an_unchanged_zero_byte_file_is_still_removed_with_two_fingerprints(
        self, tmp_path, monkeypatch, caplog
    ):
        directory = tmp_path / "d"
        directory.mkdir()
        db = directory / "config.db"
        db.write_bytes(b"")
        created_id = importer_module._file_id(db)
        calls = []
        real_fp = importer_module._fingerprint
        monkeypatch.setattr(
            importer_module, "_fingerprint", lambda p: (calls.append(p), real_fp(p))[1]
        )
        with caplog.at_level(logging.INFO):
            importer_module._remove_if_unused(db, created_id)

        assert not db.exists()
        assert warnings_of(caplog) == []
        assert len(calls) == 2  # the top, and step 5's

    def test_the_replaced_check_still_wins_over_the_changed_check(
        self, tmp_path, monkeypatch, caplog
    ):
        # Both differ after a swap; the message must say "replaced", not "changed".
        directory = tmp_path / "d"
        directory.mkdir()
        db = directory / "config.db"
        make_plain_db(db)
        created_id = importer_module._file_id(db)
        impostor = directory / "impostor"
        make_plain_db(impostor, config_row=True)
        stall(monkeypatch, "after_probe", lambda: os.rename(impostor, db))
        with caplog.at_level(logging.WARNING):
            importer_module._remove_if_unused(db, created_id)
        (message,) = warnings_of(caplog)
        assert "replaced during cleanup" in message
        assert self.CHANGED not in message
        assert importer_module._file_id(db) != created_id


class TestTheProbeNeverChangesTheFingerprint:
    """The invariant the single top-of-function fingerprint depends on: the
    probe's own `BEGIN IMMEDIATE` / `ROLLBACK` / `close()` must leave
    `(ino, size, mtime_ns, ctime_ns)` exactly as it found it, or every
    probed cleanup would report itself as "changed"."""

    def test_a_rollback_journal_database(self, tmp_path):
        db = tmp_path / "config.db"
        make_plain_db(db)
        before = importer_module._fingerprint(db)
        time.sleep(0.05)  # a probe that touched the file must show a later time
        assert importer_module._probe_config_empty(db) is True
        assert importer_module._fingerprint(db) == before
        assert listing(tmp_path) == ["config.db"]

    def test_a_wal_schema_only_database_from_a_real_failed_import(
        self, store, yaml_a, monkeypatch
    ):
        target, resolved = store
        real_fp, real_probe = importer_module._fingerprint, importer_module._probe_config_empty
        seen = {}

        def probe(path):
            seen["before"] = real_fp(path)
            time.sleep(0.05)
            seen["verdict"] = real_probe(path)
            seen["after"] = real_fp(path)
            return seen["verdict"]

        monkeypatch.setattr(importer_module, "_probe_config_empty", probe)
        fail_phase3(monkeypatch)
        with pytest.raises(OperationalError):
            import_yaml(yaml_a, resolved)

        assert seen["verdict"] is True  # the probe ran, and found it empty
        assert seen["after"] == seen["before"]
        assert not os.path.lexists(target)  # ...and, unchanged, it was removed


# ---------------------------------------------------------------------------
# A-21.2: sidecars next to a database that appeared in the meantime are live
# ---------------------------------------------------------------------------


class TestCreatedConcurrentlyIsNotOrphaned:
    @pytest.fixture
    def live_database(self, store, yaml_b):
        """B's finished first import, plus a reader that keeps -wal/-shm live."""
        target, resolved = store
        import_yaml(yaml_b, resolved)
        dispose_all()
        reader = sqlite3.connect(str(target))
        reader.execute("select count(*) from alembic_version").fetchall()
        assert Path(f"{target}-wal").exists() and Path(f"{target}-shm").exists()
        yield target, resolved, reader
        reader.close()

    @staticmethod
    def stale_first_check(monkeypatch):
        """`_target_exists` answers False once (what A saw before B created
        the database), then tells the truth. Returns the call list."""
        real = importer_module._target_exists
        calls = []

        def stale(path):
            calls.append(Path(path))
            return False if len(calls) == 1 else real(path)

        monkeypatch.setattr(importer_module, "_target_exists", stale)
        return calls

    def test_the_database_appearing_after_the_first_check_is_waited_for_not_refused(
        self, live_database, yaml_a, monkeypatch
    ):
        """15.5: the pre-lock check no longer raises `ConfigDbCreatedConcurrently`.
        A database that appeared after the caller's existence check is a
        concurrent first import holding the writer lock; the right behaviour
        is to queue behind it and re-import, which is what happens here (the
        writer lock is free, so it queues for no time at all)."""
        target, resolved, reader = live_database
        calls = self.stale_first_check(monkeypatch)

        result = import_yaml(yaml_a, resolved)

        assert len(calls) >= 2  # the import's own (stale) check, then the re-look
        assert result.created is False
        assert result.generation == 1  # B's first import was generation 0
        # The reader that kept -wal/-shm live was never disturbed.
        assert reader.execute("select count(*) from alembic_version").fetchone()[0] >= 0
        monkeypatch.undo()
        with open_config_session(resolved, readonly=True) as session:
            assert [d.name for d in load_config(session).datasets] == ["pool/A"]

    def test_the_pre_lock_call_returns_silently_when_the_database_has_appeared(self, store):
        target, _resolved = store
        wal = Path(f"{target}-wal")
        wal.write_bytes(b"x")
        target.write_bytes(b"")  # the database appears
        importer_module._refuse_orphaned_sidecars(target, before_lock=True)  # no raise
        assert wal.read_bytes() == b"x"

    def test_the_pre_lock_call_still_refuses_genuine_orphans(self, store):
        target, _resolved = store
        Path(f"{target}-wal").write_bytes(b"x")
        with pytest.raises(ConfigDbOrphanedSidecars):
            importer_module._refuse_orphaned_sidecars(target, before_lock=True)

    def test_the_under_lock_check_still_reports_a_database_created_beneath_it(
        self, store, yaml_a, monkeypatch
    ):
        """Reachable only if a process that ignores the writer lock creates the
        database (plus its -wal/-shm) between `_import_locked`'s existence
        check and its sidecar scan: nothing that honours the lock can. Then
        the under-lock re-look must still say "created concurrently", never
        the orphan advice (deleting a live -wal corrupts its database)."""
        target, resolved = store
        real_lock = importer_module.writer_lock
        real_exists = importer_module._target_exists
        state = {"armed": False, "consumed": False}
        keep = []

        @contextmanager
        def lock_then_foreign_create(*a, **k):
            with real_lock(*a, **k):
                # A lock-ignoring process creates the database in WAL mode and
                # keeps it open, so -wal/-shm are live.
                conn = sqlite3.connect(str(target))
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("create table t (x)")
                conn.commit()
                keep.append(conn)
                state["armed"] = True
                yield

        def stale_once_under_the_lock(path):
            if state["armed"] and not state["consumed"] and Path(path) == target:
                state["consumed"] = True
                return False  # what `_import_locked` saw just before the create landed
            return real_exists(path)

        monkeypatch.setattr(importer_module, "writer_lock", lock_then_foreign_create)
        monkeypatch.setattr(importer_module, "_target_exists", stale_once_under_the_lock)
        try:
            with pytest.raises(ConfigDbCreatedConcurrently) as excinfo:
                import_yaml(yaml_a, resolved)
        finally:
            for conn in keep:
                conn.close()
        message = str(excinfo.value)
        assert state["consumed"]
        assert "created concurrently" in message and "retry" in message
        for advice in ("remove", "Remove", "fuser", "lsof"):
            assert advice not in message, advice
        assert not isinstance(excinfo.value, ConfigDbOrphanedSidecars)

    def test_a_database_that_is_still_absent_keeps_the_orphan_error(
        self, store, yaml_a, monkeypatch
    ):
        target, resolved = store
        orphan = Path(f"{target}-wal")
        orphan.write_bytes(b"orphaned pages")
        real = importer_module._target_exists
        calls = []
        monkeypatch.setattr(
            importer_module, "_target_exists",
            lambda p: (calls.append(p), real(p))[1],
        )

        with pytest.raises(ConfigDbOrphanedSidecars) as excinfo:
            import_yaml(yaml_a, resolved)

        assert not isinstance(excinfo.value, ConfigDbCreatedConcurrently)
        assert len(calls) == 2  # the re-look happened, and still found nothing
        message = str(excinfo.value)
        assert "Refusing to create" in message
        assert "fuser" in message and "lsof" in message
        assert "once no process has them open" in message
        assert listing(target.parent) == [orphan.name]
        assert orphan.read_bytes() == b"orphaned pages"

    def test_refuse_helper_directly_both_ways(self, store):
        target, _resolved = store
        wal = Path(f"{target}-wal")
        wal.write_bytes(b"x")
        with pytest.raises(ConfigDbOrphanedSidecars):
            importer_module._refuse_orphaned_sidecars(target)
        target.write_bytes(b"")  # the database appears
        with pytest.raises(ConfigDbCreatedConcurrently):
            importer_module._refuse_orphaned_sidecars(target)
        wal.unlink()  # no sidecars: nothing to refuse, and no re-look needed
        importer_module._refuse_orphaned_sidecars(target)

    @pytest.mark.parametrize(
        "found", [["-journal"], ["-wal"], ["-shm", "-journal"]], ids="+".join
    )
    def test_the_message_lists_exactly_the_sidecars_found(self, store, found):
        target, _resolved = store
        target.write_bytes(b"")  # the database exists
        for suffix in found:
            Path(f"{target}{suffix}").write_bytes(b"x")

        with pytest.raises(ConfigDbCreatedConcurrently) as excinfo:
            importer_module._refuse_orphaned_sidecars(target)

        message = str(excinfo.value)
        for suffix in ("-wal", "-shm", "-journal"):
            assert (f"{target}{suffix}" in message) == (suffix in found), suffix
        for advice in ("remove", "Remove", "fuser", "lsof"):
            assert advice not in message, advice
        assert "retry" in message

    def test_no_sidecars_never_consult_the_database_check(self, store, monkeypatch):
        target, _resolved = store
        monkeypatch.setattr(
            importer_module, "_target_exists",
            lambda *_a: pytest.fail("looked for the database with no sidecar present"),
        )
        importer_module._refuse_orphaned_sidecars(target)

    def test_the_new_error_is_exported_and_is_a_config_path_error(self):
        import zfsbackup.config.store as store_pkg

        assert store_pkg.ConfigDbCreatedConcurrently is store_paths.ConfigDbCreatedConcurrently
        assert importer_module.ConfigDbCreatedConcurrently is store_paths.ConfigDbCreatedConcurrently
        assert "ConfigDbCreatedConcurrently" in store_pkg.__all__
        assert "ConfigDbCreatedConcurrently" in store_paths.__all__
        assert issubclass(ConfigDbCreatedConcurrently, ConfigPathError)
        assert not issubclass(ConfigDbCreatedConcurrently, ConfigDbOrphanedSidecars)
        assert not issubclass(ConfigDbOrphanedSidecars, ConfigDbCreatedConcurrently)


# ---------------------------------------------------------------------------
# A-11.2: refuse a first import next to orphaned sidecars
# ---------------------------------------------------------------------------


class TestOrphanedSidecarsRefused:
    @pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
    def test_refused_nothing_created_orphan_untouched(
        self, store, yaml_a, suffix
    ):
        target, resolved = store
        orphan = Path(f"{target}{suffix}")
        orphan.write_bytes(b"orphaned pages")
        before = os.lstat(orphan)

        with pytest.raises(ConfigDbOrphanedSidecars) as excinfo:
            import_yaml(yaml_a, resolved)

        assert isinstance(excinfo.value, ConfigPathError)
        assert not isinstance(excinfo.value, OSError)
        assert not os.path.lexists(target)
        # Refused BEFORE the writer lock: not even the lock file exists.
        assert listing(target.parent) == [orphan.name]
        assert not os.path.lexists(target.parent / LOCK)
        after = os.lstat(orphan)
        assert (after.st_ino, after.st_size, after.st_mtime_ns) == (
            before.st_ino, before.st_size, before.st_mtime_ns,
        )
        assert orphan.read_bytes() == b"orphaned pages"

    def test_the_message_names_every_orphan_and_the_way_out(self, store, yaml_a):
        target, resolved = store
        for suffix in ("-wal", "-shm"):
            Path(f"{target}{suffix}").write_bytes(b"x")

        with pytest.raises(ConfigDbOrphanedSidecars) as excinfo:
            import_yaml(yaml_a, resolved)

        message = str(excinfo.value)
        assert f"{target}-wal" in message and f"{target}-shm" in message
        assert "once no process has them open" in message
        assert "fuser" in message and "lsof" in message
        assert "Nothing was created or changed" in message
        assert not os.path.lexists(target.parent / LOCK)

    def test_the_default_directory_is_not_chmodded_either(
        self, tmp_path, yaml_a, monkeypatch
    ):
        # `_prepare_target_dir` would bring the OWNED default directory to
        # 2770; a refused import must not even do that.
        directory = tmp_path / "var-lib-zfsbackup"
        directory.mkdir()
        os.chmod(directory, 0o755)
        target = directory / "config.db"
        monkeypatch.setattr(store_paths, "DEFAULT_CONFIG_DB", target)
        Path(f"{target}-wal").write_bytes(b"x")
        chmods = []
        monkeypatch.setattr(
            importer_module.os, "chmod", lambda *a, **k: chmods.append(a)
        )

        with pytest.raises(ConfigDbOrphanedSidecars):
            import_yaml(yaml_a, resolve_config_path(str(target)))

        assert chmods == []
        assert (directory.stat().st_mode & 0o7777) == 0o755

    def test_a_missing_directory_is_not_created(self, tmp_path, yaml_a):
        # With no directory there can be no orphans: the refusal is simply not
        # reached, and a plain first import proceeds (and creates it).
        target = tmp_path / "newdir" / "config.db"
        assert import_yaml(yaml_a, resolve_config_path(str(target))).created is True

    def test_a_genuine_orphaned_wal_is_refused_not_replayed(self, store, yaml_a):
        # The concrete harm: a stale -wal holding pages written for a
        # DIFFERENT database would be replayed into the brand-new file.
        import shutil

        target, resolved = store
        donor = target.parent / "donor.db"
        conn = sqlite3.connect(str(donor))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE t (x)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        try:
            orphan = Path(f"{target}-wal")
            shutil.copy(f"{donor}-wal", orphan)  # a real WAL, no database beside it
            assert orphan.stat().st_size > 0

            with pytest.raises(ConfigDbOrphanedSidecars):
                import_yaml(yaml_a, resolved)

            assert orphan.exists() and not target.exists()
        finally:
            conn.close()

    def test_create_false_still_reports_a_missing_database_first(self, store, yaml_a):
        target, resolved = store
        Path(f"{target}-wal").write_bytes(b"x")
        from zfsbackup.config.store.paths import ConfigDbNotFound

        with pytest.raises(ConfigDbNotFound):
            import_yaml(yaml_a, resolved, create=False)
        assert listing(target.parent) == [f"{target.name}-wal"]  # no lock file either

    def test_an_invalid_yaml_is_still_reported_as_such(self, store, tmp_path):
        target, resolved = store
        Path(f"{target}-wal").write_bytes(b"x")
        bad = tmp_path / "bad.yaml"
        bad.write_text("datasets:\n  - name: pool/data\n    enabled: 5\n")
        with pytest.raises(ValueError, match="enabled"):
            import_yaml(bad, resolved)
        assert listing(target.parent) == [f"{target.name}-wal"]  # no lock file either

    def test_a_reimport_over_an_existing_database_is_not_refused(
        self, store, yaml_a, yaml_b
    ):
        # Sidecars next to an EXISTING database are normal (a live reader).
        target, resolved = store
        import_yaml(yaml_a, resolved)
        dispose_all()
        reader = sqlite3.connect(str(target))
        reader.execute("select count(*) from alembic_version").fetchall()
        try:
            assert Path(f"{target}-wal").exists()
            result = import_yaml(yaml_b, resolved)
        finally:
            reader.close()
        assert result.created is False


# ---------------------------------------------------------------------------
# Cross-process variant of the two measured reproductions (integration)
# ---------------------------------------------------------------------------

DRIVER = textwrap.dedent(
    '''
    """mode=import: A = first import (phase 3 fails after B ran); B = re-import by an
    opener that IGNORES the writer lock (the cleanup heuristics' remaining customer).
    mode=locked: as `import`, but B honours the writer lock: it must block until A's
    cleanup is over, then run as a FIRST import (A unlinked its own empty database).
    mode=reader: A = first import (phase 3 fails while R holds a read connection).
    Prints one `RESULT <json>` line. Self-destructs via SIGALRM."""
    import json, logging, os, signal, sqlite3, sys, time
    from pathlib import Path

    signal.alarm(60)
    mode, workdir = sys.argv[1], Path(sys.argv[2])
    db = workdir / "config.db"
    ya, yb = workdir / "a.yaml", workdir / "b.yaml"
    ya.write_text("datasets:\\n  - name: pool/A\\n    frequency: 1h\\n")
    yb.write_text("datasets:\\n  - name: pool/B\\n    frequency: 1h\\n")
    go, inph3, alog = workdir / "go", workdir / "inph3", workdir / "a.log"

    def files():
        return sorted(p.name for p in workdir.iterdir() if p.name.startswith("config.db"))

    pid = os.fork()
    if pid == 0:  # process A
        code = 0
        try:
            signal.alarm(45)
            logging.basicConfig(filename=str(alog), level=logging.INFO, format="%(levelname)s %(message)s")
            import zfsbackup.config.store.importer as imp
            from zfsbackup.config.store.paths import resolve_config_path
            from sqlalchemy.exc import OperationalError

            def failing_open(*a, **kw):
                inph3.write_text("x")
                while not go.exists():
                    time.sleep(0.05)
                raise OperationalError("BEGIN IMMEDIATE", {}, Exception("database is locked"))

            imp.open_config_session = failing_open
            try:
                imp.import_yaml(ya, resolve_config_path(str(db)))
            except OperationalError:
                pass
        except BaseException as exc:
            code = 1
            alog.write_text("DRIVER-ERROR " + repr(exc))
        os._exit(code)

    out = {}
    while not inph3.exists():
        time.sleep(0.05)
    out["before_cleanup"] = files()
    if mode in ("import", "locked"):
        import contextlib, threading
        import zfsbackup.config.store.importer as imp
        from zfsbackup.config.store.paths import resolve_config_path
        from zfsbackup.config.store.db import dispose_all

        @contextlib.contextmanager
        def no_lock(*a, **k):
            yield None

        if mode == "import":
            imp.writer_lock = no_lock
        box = {}

        def b_main():
            try:
                box["created"] = imp.import_yaml(yb, resolve_config_path(str(db))).created
            except BaseException as exc:
                box["error"] = repr(exc)

        b = threading.Thread(target=b_main)
        b.start()
        if mode == "locked":
            b.join(0.7)
            out["b_blocked_while_a_holds_the_lock"] = b.is_alive()
        else:
            b.join(40)  # B commits BEFORE A's cleanup runs
        go.write_text("x")
        b.join(40)
        out["b_finished"] = not b.is_alive()
        out["b_created"] = box.get("created")
        out["b_error"] = box.get("error")
        dispose_all()
        os.waitpid(pid, 0)
        out["files"] = files()
        if db.exists():
            c = sqlite3.connect(str(db))
            out["datasets"] = [r[0] for r in c.execute("select name from datasets")]
            c.close()
    else:
        c = sqlite3.connect(str(db))
        c.execute("select count(*) from alembic_version").fetchall()
        out["reader_files"] = files()
        st = {n: os.lstat(workdir / n).st_ino for n in files()}
        go.write_text("x")
        os.waitpid(pid, 0)
        out["files"] = files()
        out["same_inodes"] = all(os.lstat(workdir / n).st_ino == i for n, i in st.items())
        out["reader_still_reads"] = c.execute("select count(*) from alembic_version").fetchone()[0] >= 0
        c.close()
    out["a_log"] = alog.read_text() if alog.exists() else ""
    print("RESULT " + json.dumps(out), flush=True)
    '''
)


def run_driver(tmp_path, mode):
    script = tmp_path / "driver.py"
    script.write_text(DRIVER)
    workdir = tmp_path / "work"
    workdir.mkdir()
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    proc = subprocess.Popen(
        [sys.executable, str(script), mode, str(workdir)],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=90)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        pytest.fail(f"cleanup-race driver hung (mode {mode})")
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)  # never leave process A behind
        except ProcessLookupError:
            pass
    assert proc.returncode == 0, stderr
    lines = [line for line in stdout.splitlines() if line.startswith("RESULT ")]
    assert lines, f"driver printed no result:\n{stdout}\n{stderr}"
    return json.loads(lines[-1][len("RESULT "):])


@pytest.mark.integration
@pytest.mark.skipif(not FORK_AVAILABLE, reason="needs os.fork")
class TestCleanupRaceAcrossProcesses:
    """The measured reproductions from the review (R2), over a real `fork`:
    two OS processes, A's cleanup running while B / a reader are live."""

    def test_a_concurrent_import_that_committed_survives_a_failed_first_import(
        self, tmp_path
    ):
        result = run_driver(tmp_path, "import")

        assert "DRIVER-ERROR" not in result["a_log"], result
        assert result["b_created"] is False  # B took the re-import path
        assert "config.db" in result["files"], result
        assert result["datasets"] == ["pool/B"], result
        assert "Another process has since imported a configuration" in result["a_log"]
        assert "this import was NOT applied" in result["a_log"]

    def test_a_lock_honouring_second_import_waits_for_the_cleanup_then_creates(
        self, tmp_path
    ):
        """A-11 under the writer lock (item 15.4), over a real `fork`: B starts
        while A (a failing FIRST import) holds the lock, is observed blocked,
        and only runs once A's cleanup -- the unlink of A's own empty
        database -- is over. B then creates the database afresh and its
        configuration is the one that survives; nothing committed is lost."""
        result = run_driver(tmp_path, "locked")

        assert "DRIVER-ERROR" not in result["a_log"], result
        assert result["b_blocked_while_a_holds_the_lock"] is True, result
        assert result["b_finished"] is True and result["b_error"] is None, result
        assert result["b_created"] is True, result  # A's file was gone by the time B looked
        assert result["datasets"] == ["pool/B"], result
        assert result["files"] == sorted(["config.db", LOCK]), result
        assert "created by this failed import" in result["a_log"], result  # A did unlink
        assert "NOT removed" not in result["a_log"], result

    def test_a_connected_reader_keeps_db_wal_and_shm(self, tmp_path):
        result = run_driver(tmp_path, "reader")

        assert "DRIVER-ERROR" not in result["a_log"], result
        assert result["reader_files"] == sorted(
            ["config.db", "config.db-shm", "config.db-wal", LOCK]
        ), result
        assert result["files"] == result["reader_files"], result
        assert result["same_inodes"] is True, result
        assert result["reader_still_reads"] is True, result
        assert "exist (another process may have it open)" in result["a_log"]
        assert "another process has it open" not in result["a_log"]


@pytest.mark.integration
class TestFingerprintStallAcrossProcesses:
    """The measured reproduction of A-21.1: the concurrent importer is a real,
    separate OS process that runs to completion (commit, close, exit) inside
    the window between the probe's close() and step 5 (the gap the old
    post-probe retake turned into the baseline)."""

    @pytest.mark.parametrize("point", ["after_probe", "before_step5"])
    def test_a_whole_importing_process_in_the_stall_window_is_noticed(
        self, store, yaml_a, yaml_b, monkeypatch, caplog, point
    ):
        target, resolved = store
        out = {}

        def b_process_runs():
            time.sleep(0.05)
            proc = subprocess.run(
                [sys.executable, "-c", B_IMPORT, str(yaml_b), str(target)],
                cwd=ROOT, env=dict(os.environ, PYTHONPATH=str(ROOT)),
                capture_output=True, text=True, timeout=60,
            )
            out["proc"] = proc
            out["files_after_b"] = listing(target.parent)

        fail_phase3(monkeypatch)
        stall(monkeypatch, point, b_process_runs)
        with caplog.at_level(logging.WARNING):
            with pytest.raises(OperationalError):
                import_yaml(yaml_a, resolved)
        monkeypatch.undo()

        proc = out["proc"]
        assert proc.returncode == 0, proc.stderr
        assert "B created=False" in proc.stdout  # B adopted A's schema-only file
        # B exited: no sidecar left to see (and A's lock file is simply there)
        assert out["files_after_b"] == sorted(["config.db", LOCK])
        assert target.exists()
        conn = sqlite3.connect(str(target))
        try:
            assert [r[0] for r in conn.execute("select name from datasets")] == ["pool/B"]
        finally:
            conn.close()
        (message,) = [m for m in warnings_of(caplog) if "NOT removed" in m]
        assert "changed since it was checked" in message
        assert "re-running the import reuses it" in message

    @pytest.mark.parametrize("point", ["after_probe", "before_step5"])
    def test_a_lock_honouring_process_waits_for_the_unlink_then_creates(
        self, store, yaml_a, yaml_b, monkeypatch, caplog, point
    ):
        """The same window, with B a real process that honours the writer lock:
        it cannot run inside A's cleanup, so there is nothing for the
        fingerprint to catch. B is parked until A's unlink is over, then
        creates the database afresh; B's configuration is the one that
        remains and A logs a plain removal, never a "NOT removed" skip."""
        target, resolved = store
        out = {}

        def b_process_starts():
            out["proc"] = subprocess.Popen(
                [sys.executable, "-c", B_IMPORT_HONOURING, str(yaml_b), str(target)],
                cwd=ROOT, env=dict(os.environ, PYTHONPATH=str(ROOT)),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            time.sleep(1.0)  # long enough for B to import and start waiting
            out["blocked"] = out["proc"].poll() is None
            out["files_during"] = listing(target.parent)

        fail_phase3(monkeypatch)
        stall(monkeypatch, point, b_process_starts)
        try:
            with caplog.at_level(logging.INFO):
                with pytest.raises(OperationalError):
                    import_yaml(yaml_a, resolved)
            monkeypatch.undo()
            stdout, stderr = out["proc"].communicate(timeout=60)
        finally:
            if "proc" in out and out["proc"].poll() is None:
                out["proc"].kill()
                out["proc"].communicate()

        assert out["blocked"] is True
        assert out["proc"].returncode == 0, stderr
        assert "B created=True" in stdout
        assert warnings_of(caplog) == []
        assert any("created by this failed import" in r.getMessage() for r in caplog.records)
        conn = sqlite3.connect(str(target))
        try:
            assert [r[0] for r in conn.execute("select name from datasets")] == ["pool/B"]
        finally:
            conn.close()
        assert LOCK in listing(target.parent)


#: Process A of the 15.5 pre-lock race: a real first import that HOLDS the
#: writer lock after the database and its WAL exist (a reader keeps -wal/-shm
#: live), until told to go. It re-enters `import_yaml` with the lock patched
#: out only because the test needs to hold the real lock across the window.
A_HOLDS_THE_LOCK = textwrap.dedent(
    '''
    import contextlib, logging, os, sqlite3, sys, time
    logging.disable(logging.WARNING)
    import zfsbackup.config.store.importer as imp
    from zfsbackup.config.store import resolve_config_path, writer_lock

    yaml_a, db, go = sys.argv[1:4]
    resolved = resolve_config_path(db)
    with writer_lock(resolved):
        imp.writer_lock = contextlib.contextmanager(lambda *a, **k: (yield None))
        r = imp.import_yaml(yaml_a, resolved)
        reader = sqlite3.connect(db)
        reader.execute("select count(*) from alembic_version").fetchall()
        assert os.path.exists(db + "-wal") and os.path.exists(db + "-shm"), os.listdir(os.path.dirname(db))
        print("ready", r.created, flush=True)
        deadline = time.monotonic() + 60
        while not os.path.exists(go) and time.monotonic() < deadline:
            time.sleep(0.02)
        reader.close()
    print("released", flush=True)
    '''
)


@pytest.mark.integration
class TestPreLockRaceAcrossProcesses:
    """15.5, measured with a real second process: B's existence check saw no
    database; A (a separate process) then created it plus its WAL under the
    writer lock; B's pre-lock sidecar check ran while A still held the lock.
    It used to raise `ConfigDbCreatedConcurrently`; it must wait and re-import.
    """

    def test_b_waits_for_a_then_reimports_over_it(self, store, yaml_a, yaml_b, tmp_path, monkeypatch):
        target, resolved = store
        go = tmp_path / "go"
        proc = subprocess.Popen(
            [sys.executable, "-c", A_HOLDS_THE_LOCK, str(yaml_a), str(target), str(go)],
            cwd=ROOT, env=dict(os.environ, PYTHONPATH=str(ROOT)),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        box = {}
        try:
            assert proc.stdout.readline().split() == ["ready", "True"], proc.stderr.read()
            assert Path(f"{target}-wal").exists() and Path(f"{target}-shm").exists()
            assert target.exists()

            # B's first existence check ran before A created the database.
            real = importer_module._target_exists
            calls = []

            def stale(path):
                calls.append(Path(path))
                return False if len(calls) == 1 else real(path)

            monkeypatch.setattr(importer_module, "_target_exists", stale)

            def b_main():
                try:
                    box["result"] = import_yaml(yaml_b, resolve_config_path(str(target)))
                except BaseException as exc:
                    box["error"] = exc

            b = threading.Thread(target=b_main)
            b.start()
            b.join(1.0)
            assert b.is_alive(), f"B should be queued behind A's lock, got {box!r}"
            assert "error" not in box and "result" not in box
            assert len(calls) >= 2  # the stale check, then the pre-lock re-look

            go.write_text("x")
            b.join(30)
            assert not b.is_alive()
        finally:
            go.write_text("x")
            try:
                proc.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate()
        assert "error" not in box, repr(box.get("error"))
        assert box["result"].created is False
        assert box["result"].generation == 1
        monkeypatch.undo()
        dispose_all()
        with open_config_session(resolved, readonly=True) as session:
            assert [d.name for d in load_config(session).datasets] == ["pool/B"]

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
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

from zfsbackup.config.store import (
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
            result = import_yaml(yaml_b, resolved)  # re-import branch: commits
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
        fail_phase3(monkeypatch, before=lambda: (import_yaml(yaml_b, resolved), dispose_all()))
        with pytest.raises(OperationalError):
            import_yaml(yaml_a, resolved)


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

            assert listing(target.parent) == ["config.db", "config.db-shm", "config.db-wal"]
            assert inodes(target, Path(f"{target}-wal"), Path(f"{target}-shm")) == held["before"]
            # The reader is not just "connected": its database is still readable.
            assert held["conn"].execute("select count(*) from alembic_version").fetchone()[0] >= 0

            (message,) = [m for m in warnings_of(caplog) if "NOT removed" in m]
            assert "another process has it open" in message
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

        assert unlinked == [target]
        assert listing(target.parent) == []


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
        assert listing(target.parent) == [orphan.name]
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

    def test_an_invalid_yaml_is_still_reported_as_such(self, store, tmp_path):
        target, resolved = store
        Path(f"{target}-wal").write_bytes(b"x")
        bad = tmp_path / "bad.yaml"
        bad.write_text("datasets:\n  - name: pool/data\n    enabled: 5\n")
        with pytest.raises(ValueError, match="enabled"):
            import_yaml(bad, resolved)

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

    def test_the_interim_cli_reports_the_refusal_cleanly(
        self, store, yaml_a, capsys
    ):
        target, _resolved = store
        Path(f"{target}-shm").write_bytes(b"x")
        code = importer_module._main([str(yaml_a), "-c", str(target)])
        assert code == 1
        assert "Refusing to create" in capsys.readouterr().err
        assert listing(target.parent) == ["config.db-shm"]


# ---------------------------------------------------------------------------
# Cross-process variant of the two measured reproductions (integration)
# ---------------------------------------------------------------------------

DRIVER = textwrap.dedent(
    '''
    """mode=import: A = first import (phase 3 fails after B ran); B = re-import.
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
    if mode == "import":
        import zfsbackup.config.store.importer as imp
        from zfsbackup.config.store.paths import resolve_config_path
        from zfsbackup.config.store.db import dispose_all
        out["b_created"] = imp.import_yaml(yb, resolve_config_path(str(db))).created
        dispose_all()
        go.write_text("x")
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

    def test_a_connected_reader_keeps_db_wal_and_shm(self, tmp_path):
        result = run_driver(tmp_path, "reader")

        assert "DRIVER-ERROR" not in result["a_log"], result
        assert result["reader_files"] == ["config.db", "config.db-shm", "config.db-wal"], result
        assert result["files"] == result["reader_files"], result
        assert result["same_inodes"] is True, result
        assert result["reader_still_reads"] is True, result
        assert "another process has it open" in result["a_log"]

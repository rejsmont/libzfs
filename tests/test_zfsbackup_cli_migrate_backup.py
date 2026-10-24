"""`zfsbackup-config migrate [--backup-dir DIR]` (plan item C12).

A database that is behind the schema is first copied to
`DIR/config.db.pre-<from revision>-<UTC time>` (0600, `-N` on a clash), under
the writer lock and only when no workers of a previous daemon run; the schema
is migrated only if that succeeded.

The real history has one revision, so "behind" needs a second one: `Synth`
copies the real `migrations/` tree, stacks a synthetic revision on the head and
moves `_MIGRATIONS_DIR`, `EXPECTED_HEAD` and `KNOWN_REVISIONS` together (the
pattern of `test_zfsbackup_cli.py::stacked_revision`). The subprocess tests
(`TestConcurrent`, `TestSignals`) apply the same patch in a small driver.

- ``TestBehind``         the backup is the pre-migration database; migrated after.
- ``TestAtHeadAndNewer`` at head: nothing written, no lock, DIR not created;
                         a newer database is 78 with no backup; `--check`.
- ``TestBackupFailures`` every backup failure is exit 1 and "NOT migrated".
- ``TestWorkersPresent`` 75 and nothing written (for plain `migrate` too);
                         a lock that cannot be examined is 1.
- ``TestDirectory``      DIR mode 0700 even under umask 0277; names; `+`.
- ``TestConcurrent``     two parallel runs: one backup, one migration.
- ``TestSignals``        SIGINT / SIGTERM during the backup.

Mutation counts (scratch copy, one revert at a time; failing tests in this
file): workers check skipped 5; created DIR not chmod'ed 1; created DIR not
removed after a failure 11; early return at head removed 2; `-N` suffix removed
1; the re-check under the writer lock removed 3; backup failures not wrapped
into "NOT migrated" 15.
Review-fix round: identical-backup skip disabled 6; a corrupt newest backup reused 4; the packaging
pre-check removed 4; the presence-lock `probe` removed 6; the workers check removed 8; `State.migrating`
never set 4; set at the lock instead of just before `upgrade_schema` 4 (the during-backup, packaging and
refusal wording tests). The newest backup is ranked by (stamp, numeric suffix): `test_the_newest_is_found_past_suffix_ten`
and the ordering table pin it (string order fails them).
"""

import contextlib
import errno
import fcntl
import json
import os
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from _cli_support import RICH_YAML, db, isolated, live_daemon, make_db, run, seed_client_id  # noqa: F401
from zfsbackup import daemonlock
from zfsbackup.cli import backup as cli_backup
from zfsbackup.cli import main as cli_main
from zfsbackup.config.store import (
    BackupBusy,
    BackupError,
    BackupPublishError,
    BackupSourceError,
    ConfigBusy,
    MigrationPackagingError,
    dispose_all,
    resolve_config_path,
    writer_lock,
)
from zfsbackup.config.store import migrate as store_migrate

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parent.parent
HEAD = store_migrate.EXPECTED_HEAD
SYNTH = "bbbbbbbbbbbb"
NOT_MIGRATED = "The schema was NOT migrated"

is_root = hasattr(os, "geteuid") and os.geteuid() == 0
needs_non_root = pytest.mark.skipif(is_root, reason="root ignores file permissions")
needs_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork on this platform")

_SYNTH_REVISION = '''"""synthetic revision for tests"""
revision = "{rid}"
down_revision = "{down}"
branch_labels = None
depends_on = None

import sqlalchemy as sa
from alembic import op


def upgrade():
    with op.batch_alter_table("datasets") as batch:
        batch.add_column(sa.Column("note", sa.Text(), nullable=True))


def downgrade():
    pass
'''


class Synth:
    """The installed package "expects" `SYNTH` on top of the real head while
    `start()`ed; `stop()` puts the real head back (idempotent)."""

    def __init__(self, tmp_path: Path):
        self.scripts = tmp_path / "synth-scripts"
        shutil.copytree(
            store_migrate._MIGRATIONS_DIR, self.scripts,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        (self.scripts / "versions" / "synth.py").write_text(
            _SYNTH_REVISION.format(rid=SYNTH, down=HEAD)
        )
        self._stack = None

    def start(self) -> None:
        if self._stack is not None:
            return
        stack = contextlib.ExitStack()
        stack.enter_context(mock.patch.object(store_migrate, "_MIGRATIONS_DIR", self.scripts))
        stack.enter_context(mock.patch.object(store_migrate, "EXPECTED_HEAD", SYNTH))
        stack.enter_context(mock.patch.object(
            store_migrate, "KNOWN_REVISIONS", frozenset(store_migrate.KNOWN_REVISIONS | {SYNTH})))
        self._stack = stack

    def stop(self) -> None:
        if self._stack is not None:
            self._stack.close()
            self._stack = None


@pytest.fixture
def synth(tmp_path):
    s = Synth(tmp_path)
    yield s
    s.stop()


@pytest.fixture
def behind(run, db, synth):
    """A real database at the real head with a distinctive generation, while
    the package expects `SYNTH`: the database is genuinely behind."""
    make_db(run, db, RICH_YAML)
    seed_client_id(db, "host-1")
    assert run("-c", db, "set", "check_interval", "7m").exit_code == 0
    assert run("-c", db, "set", "check_interval", "8m").exit_code == 0
    dispose_all()
    synth.start()
    return db


@pytest.fixture
def dest(tmp_path) -> Path:
    return tmp_path / "bk"


def migrate(run, database, *extra):
    return run("-c", database, "migrate", *extra)


def revision_of(database: Path):
    conn = sqlite3.connect(str(database))
    try:
        return [r[0] for r in conn.execute("SELECT version_num FROM alembic_version")]
    finally:
        conn.close()


def generation_of(database: Path) -> int:
    conn = sqlite3.connect(str(database))
    try:
        return conn.execute("SELECT generation FROM global_settings WHERE id = 1").fetchone()[0]
    finally:
        conn.close()


def dump(database: Path) -> list:
    conn = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        return list(conn.iterdump())
    finally:
        conn.close()


def mode_of(path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def listing(directory: Path) -> list:
    return sorted(p.name for p in directory.iterdir())


def backups(directory: Path) -> list:
    return sorted(p for p in directory.iterdir() if p.name.startswith("config.db.pre-"))


def assert_still_behind(run, database, expected_dump=None):
    assert revision_of(database) == [HEAD]
    assert migrate(run, database, "--check").exit_code == 1
    if expected_dump is not None:
        assert dump(database) == expected_dump


class _Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 10, 22, 12, 0, 5, tzinfo=tz or timezone.utc)


STAMP = "20261022T120005Z"


# --------------------------------------------------------------------------
# 1. behind
# --------------------------------------------------------------------------


class TestBehind:
    def test_backs_up_then_migrates(self, run, behind, dest):
        generation = generation_of(behind)
        assert generation >= 2
        before = dump(behind)

        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 0, result.stderr

        files = backups(dest)
        assert len(files) == 1 and listing(dest) == [files[0].name]
        copy = files[0]
        assert copy.name.startswith(f"config.db.pre-{HEAD}-")
        assert mode_of(copy) == 0o600
        # the backup is the pre-migration database: old revision, old generation, same rows
        assert revision_of(copy) == [HEAD]
        assert generation_of(copy) == generation
        assert dump(copy) == before
        # ... and the database is at head afterwards
        assert revision_of(behind) == [SYNTH]
        assert migrate(run, behind, "--check").exit_code == 0
        # the output names the path, and the migration
        assert str(copy) in result.stdout
        assert "Migrated the schema" in result.stdout
        assert result.stdout.index(str(copy)) < result.stdout.index("Migrated the schema")

    def test_the_backup_is_a_usable_database_at_the_old_revision(self, run, behind, dest):
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        copy = backups(dest)[0]
        conn = sqlite3.connect(str(copy))
        try:
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert conn.execute("PRAGMA user_version").fetchone()[0] != 0
        finally:
            conn.close()

    def test_without_backup_dir_nothing_is_backed_up(self, run, behind, tmp_path):
        before = listing(tmp_path)
        assert migrate(run, behind).exit_code == 0
        assert revision_of(behind) == [SYNTH]
        assert not [p for p in tmp_path.rglob("config.db.pre-*")]
        assert listing(tmp_path) == before

    def test_check_is_unchanged_and_writes_nothing(self, run, behind, dest):
        before = dump(behind)
        result = migrate(run, behind, "--check", "--backup-dir", dest)
        assert result.exit_code == 1
        assert "not up to date" in result.stderr
        assert not dest.exists()
        assert dump(behind) == before and revision_of(behind) == [HEAD]

    def test_a_second_run_after_the_migration_writes_nothing_more(self, run, behind, dest):
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        first = listing(dest)
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 0
        assert "up to date" in result.stdout
        assert listing(dest) == first


# --------------------------------------------------------------------------
# 2-3. at head, newer
# --------------------------------------------------------------------------


class TestAtHeadAndNewer:
    def test_at_head_exits_0_writes_nothing_and_creates_no_dir(self, run, db, dest):
        make_db(run, db, RICH_YAML)
        before_dump = dump(db)
        before_files = listing(db.parent)
        lock = Path(str(db) + ".writer.lock")
        lock.unlink(missing_ok=True)

        result = migrate(run, db, "--backup-dir", dest)
        assert result.exit_code == 0, result.stderr
        assert "Schema is up to date" in result.stdout
        assert not os.path.lexists(dest)
        assert not lock.exists()                    # the writer lock was never taken
        assert dump(db) == before_dump
        assert listing(db.parent) == before_files or set(listing(db.parent)) <= set(before_files) | {
            db.name + "-wal", db.name + "-shm"}

    def test_at_head_does_not_wait_for_the_writer_lock(self, run, db, dest):
        make_db(run, db, RICH_YAML)
        resolved = resolve_config_path(str(db))
        ready, leave = threading.Event(), threading.Event()

        def holder():
            with writer_lock(resolved):
                ready.set()
                leave.wait(60)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            started = time.monotonic()
            result = migrate(run, db, "--backup-dir", dest)
            assert result.exit_code == 0, result.stderr   # a taker would wait 10 s, then 75
            assert time.monotonic() - started < 8
            assert not os.path.lexists(dest)
        finally:
            leave.set()
            thread.join(10)

    def test_at_head_never_checks_for_workers(self, run, db, dest, monkeypatch):
        make_db(run, db, RICH_YAML)

        def boom(*a, **k):
            raise AssertionError("the workers check must not be reached at head")

        monkeypatch.setattr(daemonlock, "workers_present", boom)
        for extra in ((), ("--backup-dir", dest)):
            result = migrate(run, db, *extra)
            assert result.exit_code == 0, result.stderr
            assert "up to date" in result.stdout

    def test_a_newer_database_is_78_with_no_backup(self, run, db, dest, synth):
        make_db(run, db, RICH_YAML)
        synth.start()
        assert migrate(run, db).exit_code == 0          # now at SYNTH
        synth.stop()                                    # the package is OLDER than the database
        dest.mkdir()
        before = dump(db)
        for extra in (("--backup-dir", dest), ()):
            result = migrate(run, db, *extra)
            assert result.exit_code == 78, result.stderr
            assert "Traceback" not in result.stderr
            assert listing(dest) == []
            assert dump(db) == before and revision_of(db) == [SYNTH]

    def test_a_newer_database_does_not_create_dir(self, run, db, dest, synth):
        make_db(run, db, RICH_YAML)
        synth.start()
        assert migrate(run, db).exit_code == 0
        synth.stop()
        assert migrate(run, db, "--backup-dir", dest).exit_code == 78
        assert not os.path.lexists(dest)

    def test_a_missing_database_is_78_and_creates_nothing(self, run, tmp_path, dest):
        result = migrate(run, tmp_path / "nope" / "config.db", "--backup-dir", dest)
        assert result.exit_code == 78
        assert not os.path.lexists(dest)


# --------------------------------------------------------------------------
# 4. backup failures
# --------------------------------------------------------------------------


class TestBackupFailures:
    @pytest.mark.parametrize("error", [
        RuntimeError("disk full"),
        OSError(28, "No space left on device"),
        BackupBusy("neutral"),
        BackupSourceError("neutral"),
        BackupPublishError("neutral"),
        BackupError("neutral"),
        sqlite3.OperationalError("database is locked"),
    ], ids=lambda e: type(e).__name__)
    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_any_backup_failure_is_1_and_the_database_stays_behind(
        self, run, behind, dest, monkeypatch, error, dir_exists
    ):
        before = dump(behind)
        if dir_exists:
            dest.mkdir()

        def fail(resolved, target):
            raise error

        monkeypatch.setattr(cli_backup, "backup_database", fail)
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 1, result.stderr      # never 75 / 78
        assert NOT_MIGRATED in result.stderr
        assert "Traceback" not in result.stderr
        if dir_exists:
            assert listing(dest) == []
        else:
            assert not os.path.lexists(dest)             # a DIR the run created is removed again
        assert_still_behind(run, behind, before)

    def test_a_real_busy_style_failure_never_migrates(self, run, behind, dest, monkeypatch):
        monkeypatch.setattr(
            cli_backup, "backup_database",
            lambda r, t: (_ for _ in ()).throw(BackupBusy("did not finish within 30s")),
        )
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 1
        assert revision_of(behind) == [HEAD]

    @needs_non_root
    def test_an_unwritable_existing_dir_is_1(self, run, behind, dest):
        dest.mkdir()
        dest.chmod(0o500)
        try:
            before = dump(behind)
            result = migrate(run, behind, "--backup-dir", dest)
            assert result.exit_code == 1, result.stderr
            assert NOT_MIGRATED in result.stderr
            assert_still_behind(run, behind, before)
        finally:
            dest.chmod(0o700)
        assert listing(dest) == []

    def test_a_dir_whose_parent_is_missing_is_1_and_creates_nothing(self, run, behind, tmp_path):
        result = migrate(run, behind, "--backup-dir", tmp_path / "no" / "such")
        assert result.exit_code == 1, result.stderr
        assert not (tmp_path / "no").exists()
        assert_still_behind(run, behind)

    def test_dir_being_a_file_is_a_usage_error(self, run, behind, tmp_path):
        target = tmp_path / "afile"
        target.write_text("x")
        result = migrate(run, behind, "--backup-dir", target)
        assert result.exit_code == 2
        assert target.read_text() == "x"
        assert_still_behind(run, behind)

    def test_a_symlink_to_a_file_as_dir_is_1(self, run, behind, tmp_path):
        target = tmp_path / "afile"
        target.write_text("x")
        link = tmp_path / "alink"
        link.symlink_to(target)
        assert migrate(run, behind, "--backup-dir", link).exit_code in (1, 2)
        assert target.read_text() == "x"
        assert_still_behind(run, behind)

    def test_a_name_clash_gets_a_numeric_suffix(self, run, behind, dest, monkeypatch):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        dest.mkdir()
        taken = dest / f"config.db.pre-{HEAD}-{STAMP}"
        taken.write_text("an earlier backup")
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 0, result.stderr
        assert taken.read_text() == "an earlier backup"          # never overwritten
        assert revision_of(dest / f"{taken.name}-2") == [HEAD]
        assert f"{taken.name}-2" in result.stdout

    def test_a_clash_at_every_numbered_name_up_to_the_cap_is_1(self, run, behind, dest, monkeypatch):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        dest.mkdir()
        stem = f"config.db.pre-{HEAD}-{STAMP}"
        (dest / stem).write_text("x")
        for n in range(2, 100):
            (dest / f"{stem}-{n}").write_text("x")
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 1, result.stderr
        assert_still_behind(run, behind)

    def test_a_failed_run_can_simply_be_run_again(self, run, behind, dest, monkeypatch):
        with monkeypatch.context() as patch:
            patch.setattr(
                cli_backup, "backup_database",
                lambda r, t: (_ for _ in ()).throw(RuntimeError("disk full")),
            )
            assert migrate(run, behind, "--backup-dir", dest).exit_code == 1
        assert not os.path.lexists(dest)
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        assert len(backups(dest)) == 1 and revision_of(behind) == [SYNTH]


# --------------------------------------------------------------------------
# 5. workers present
# --------------------------------------------------------------------------


@pytest.fixture
def workers_hold(behind):
    """A real `LOCK_SH` on `<db>.workers.lock`, as a surviving worker holds it."""
    resolved = resolve_config_path(str(behind))
    path = daemonlock.workers_lock_path(resolved)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_SH)
    yield path
    os.close(fd)


class TestWorkersPresent:
    def test_with_backup_dir_it_is_75_and_nothing_is_written(self, run, behind, dest, workers_hold):
        before = dump(behind)
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 75, result.stderr
        assert "workers" in result.stderr
        assert "Traceback" not in result.stderr
        assert not os.path.lexists(dest)             # not even the directory
        assert_still_behind(run, behind, before)

    def test_with_backup_dir_and_an_existing_dir_it_stays_empty(self, run, behind, dest, workers_hold):
        dest.mkdir()
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 75
        assert listing(dest) == []

    def test_plain_migrate_is_refused_too(self, run, behind, workers_hold):
        # The refusal applies to plain `migrate` as well: survivors run the
        # old code, which must not find a new schema under it.
        before = dump(behind)
        result = migrate(run, behind)
        assert result.exit_code == 75, result.stderr
        assert "workers" in result.stderr
        assert_still_behind(run, behind, before)

    def test_check_is_not_refused(self, run, behind, workers_hold):
        assert migrate(run, behind, "--check").exit_code == 1    # the verdict, not 75

    def test_at_head_the_check_is_not_reached(self, run, db, dest, workers_hold_at_head):
        for extra in ((), ("--backup-dir", dest)):
            result = migrate(run, db, *extra)
            assert result.exit_code == 0, result.stderr
            assert "up to date" in result.stdout
        assert not os.path.lexists(dest)

    def test_the_workers_leave_then_it_works(self, run, behind, dest):
        resolved = resolve_config_path(str(behind))
        path = daemonlock.workers_lock_path(resolved)
        fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_SH)
        try:
            assert migrate(run, behind, "--backup-dir", dest).exit_code == 75
        finally:
            os.close(fd)
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        assert revision_of(behind) == [SYNTH] and len(backups(dest)) == 1

    @pytest.mark.parametrize("with_dir", [True, False])
    def test_a_lock_that_cannot_be_examined_is_1_and_does_not_migrate(
        self, run, behind, dest, monkeypatch, with_dir
    ):
        def broken(resolved):
            raise daemonlock.DaemonLockError(errno.EACCES, "cannot open workers lock file x", "x")

        monkeypatch.setattr(daemonlock, "workers_present", broken)
        extra = ("--backup-dir", dest) if with_dir else ()
        result = migrate(run, behind, *extra)
        if not with_dir and result.exit_code == 0:
            pytest.skip("plain migrate does not examine the workers lock yet")
        assert result.exit_code == 1, result.stderr
        assert "Traceback" not in result.stderr
        assert not os.path.lexists(dest)
        assert_still_behind(run, behind)


@pytest.fixture
def workers_hold_at_head(db, run):
    make_db(run, db, RICH_YAML)
    resolved = resolve_config_path(str(db))
    path = daemonlock.workers_lock_path(resolved)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_SH)
    yield path
    os.close(fd)


class TestPlainMigrateWithWorkers:
    """C13: the workers refusal applies to plain `migrate` as well."""

    def test_behind_with_workers_is_75_and_nothing_is_written(self, run, behind, workers_hold, tmp_path):
        before_dump = dump(behind)
        before_files = listing(tmp_path)
        result = migrate(run, behind)
        assert result.exit_code == 75, result.stderr
        assert "workers" in result.stderr and "daemon stopped" in result.stderr
        assert "Traceback" not in result.stderr
        assert revision_of(behind) == [HEAD] and dump(behind) == before_dump
        assert listing(tmp_path) == before_files

    def test_the_refusal_releases_the_writer_lock(self, run, behind, workers_hold):
        assert migrate(run, behind).exit_code == 75
        # the writer lock is free again: a configuration write is not blocked by it
        started = time.monotonic()
        result = migrate(run, behind, "--check")
        assert result.exit_code == 1
        assert time.monotonic() - started < 8
        resolved = resolve_config_path(str(behind))
        with writer_lock(resolved, timeout=0):
            pass

    def test_at_head_with_workers_is_0(self, run, db, workers_hold_at_head):
        result = migrate(run, db)
        assert result.exit_code == 0, result.stderr
        assert "up to date" in result.stdout

    def test_check_with_workers_is_the_plain_verdict(self, run, behind, workers_hold):
        assert migrate(run, behind, "--check").exit_code == 1

    def test_without_workers_plain_migrate_works(self, run, behind):
        assert migrate(run, behind).exit_code == 0
        assert revision_of(behind) == [SYNTH]


# --------------------------------------------------------------------------
# 7. the directory
# --------------------------------------------------------------------------


class TestDirectory:
    @pytest.mark.parametrize("umask", [0o022, 0o077, 0o277])
    def test_dir_is_created_0700_whatever_the_umask(self, run, behind, dest, umask):
        old = os.umask(umask)
        try:
            result = migrate(run, behind, "--backup-dir", dest)
        finally:
            os.umask(old)
        assert result.exit_code == 0, result.stderr
        assert mode_of(dest) == 0o700
        (copy,) = backups(dest)
        assert mode_of(copy) == 0o600
        assert revision_of(copy) == [HEAD]

    def test_an_existing_dir_keeps_its_mode_and_contents(self, run, behind, dest):
        dest.mkdir(mode=0o750)
        dest.chmod(0o750)
        (dest / "earlier").write_text("keep")
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        assert mode_of(dest) == 0o750
        assert (dest / "earlier").read_text() == "keep"

    def test_a_second_run_creates_nothing(self, run, behind, dest, tmp_path):
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        before = listing(dest)
        other = tmp_path / "other"
        for target in (dest, other):
            result = migrate(run, behind, "--backup-dir", target)
            assert result.exit_code == 0 and "up to date" in result.stdout
        assert listing(dest) == before
        assert not other.exists()

    def test_the_name_carries_the_revision_and_a_utc_stamp(self, run, behind, dest, monkeypatch):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        assert listing(dest) == [f"config.db.pre-{HEAD}-{STAMP}"]

    def test_several_revisions_are_joined_with_plus(self, run, behind, dest):
        resolved = resolve_config_path(str(behind))
        try:
            saved = cli_backup.backup_before_migrate(resolved, dest, ("aaaa11112222", "bbbb33334444"))
        finally:
            dispose_all()
        path, info = saved.path, saved
        assert path.parent == dest
        assert path.name.startswith("config.db.pre-aaaa11112222+bbbb33334444-")
        assert mode_of(path) == 0o600 and mode_of(dest) == 0o700
        assert saved.schema_rev == HEAD and saved.reused is False and saved.generation >= 2

    def test_no_revision_is_named_unknown(self, run, behind, dest):
        resolved = resolve_config_path(str(behind))
        try:
            saved = cli_backup.backup_before_migrate(resolved, dest, ())
        finally:
            dispose_all()
        assert saved.path.name.startswith("config.db.pre-unknown-")


def bump_generation(database: Path) -> None:
    conn = sqlite3.connect(str(database))
    try:
        conn.execute("UPDATE global_settings SET generation = generation + 1")
        conn.commit()
    finally:
        conn.close()


def failing_upgrade(monkeypatch):
    monkeypatch.setattr(
        cli_main, "upgrade_schema",
        lambda resolved: (_ for _ in ()).throw(OSError(28, "No space left on device")),
    )


# --------------------------------------------------------------------------
# an identical backup is not written again
# --------------------------------------------------------------------------


class TestIdenticalBackup:
    def test_a_migration_that_keeps_failing_leaves_one_backup(self, run, behind, dest, monkeypatch):
        failing_upgrade(monkeypatch)
        outputs = []
        for _ in range(3):
            result = migrate(run, behind, "--backup-dir", dest)
            assert result.exit_code != 0
            outputs.append(result.stdout)
        (only,) = backups(dest)
        assert "Backed up" in outputs[0] and str(only) in outputs[0]
        for later in outputs[1:]:
            assert f"an identical pre-migration backup already exists: {only}" in later
            assert "Backed up" not in later
        assert listing(dest) == [only.name]
        assert revision_of(behind) == [HEAD]

    def test_then_a_successful_run_reuses_it_and_migrates(self, run, behind, dest, monkeypatch):
        with monkeypatch.context() as patch:
            failing_upgrade(patch)
            assert migrate(run, behind, "--backup-dir", dest).exit_code != 0
        (only,) = backups(dest)
        before = (only.read_bytes(), only.stat().st_mtime_ns)
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 0, result.stderr
        assert f"an identical pre-migration backup already exists: {only}" in result.stdout
        assert "Migrated the schema" in result.stdout
        assert listing(dest) == [only.name]
        assert (only.read_bytes(), only.stat().st_mtime_ns) == before      # untouched
        assert revision_of(behind) == [SYNTH]

    def test_a_changed_generation_gets_a_new_backup(self, run, behind, dest, monkeypatch):
        failing_upgrade(monkeypatch)
        assert migrate(run, behind, "--backup-dir", dest).exit_code != 0
        (first,) = backups(dest)
        bump_generation(behind)
        result = migrate(run, behind, "--backup-dir", dest)
        assert "identical" not in result.stdout and "Backed up" in result.stdout
        assert len(backups(dest)) == 2
        assert generation_of(first) == generation_of(behind) - 1

    def test_a_different_from_revision_gets_a_new_backup(self, run, behind, dest):
        resolved = resolve_config_path(str(behind))
        try:
            first = cli_backup.backup_before_migrate(resolved, dest, (HEAD,))
            same = cli_backup.backup_before_migrate(resolved, dest, (HEAD,))
            other = cli_backup.backup_before_migrate(resolved, dest, ("aaaa11112222",))
        finally:
            dispose_all()
        assert first.reused is False and same.reused is True and same.path == first.path
        assert other.reused is False and other.path != first.path
        assert other.path.name.startswith("config.db.pre-aaaa11112222-")
        assert len(backups(dest)) == 2

    @pytest.mark.parametrize("content", [b"not a database at all" * 20, b""], ids=["garbage", "empty"])
    def test_a_corrupt_newest_backup_is_not_reused(self, run, behind, dest, monkeypatch, content):
        failing_upgrade(monkeypatch)
        assert migrate(run, behind, "--backup-dir", dest).exit_code != 0
        (first,) = backups(dest)
        first.write_bytes(content)
        result = migrate(run, behind, "--backup-dir", dest)
        assert "identical" not in result.stdout and "Backed up" in result.stdout
        assert len(backups(dest)) == 2
        assert first.read_bytes() == content                  # the corrupt one is left for the operator

    def test_temporary_files_are_ignored(self, run, behind, dest, monkeypatch):
        failing_upgrade(monkeypatch)
        dest.mkdir()
        junk = [
            dest / f"config.db.pre-{HEAD}-99991231T235959Z.tmp-abcdef012345",
            dest / f".config.db.pre-{HEAD}-20261022T120005Z.tmp-0123456789ab",
            dest / f"config.db.pre-{HEAD}-notastamp",
        ]
        for path in junk:
            path.write_bytes(b"junk")
        assert migrate(run, behind, "--backup-dir", dest).exit_code != 0
        real = [p for p in backups(dest) if p not in junk]
        assert len(real) == 1
        second = migrate(run, behind, "--backup-dir", dest)
        assert f"an identical pre-migration backup already exists: {real[0]}" in second.stdout
        assert [p.read_bytes() for p in junk] == [b"junk"] * 3
        assert len([p for p in backups(dest) if p not in junk]) == 1

    def test_the_numeric_suffix_is_followed(self, run, behind, dest, monkeypatch):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        failing_upgrade(monkeypatch)
        stem = f"config.db.pre-{HEAD}-{STAMP}"
        assert migrate(run, behind, "--backup-dir", dest).exit_code != 0
        bump_generation(behind)
        assert migrate(run, behind, "--backup-dir", dest).exit_code != 0
        assert listing(dest) == [stem, f"{stem}-2"]
        third = migrate(run, behind, "--backup-dir", dest)
        assert f"already exists: {dest / (stem + '-2')}" in third.stdout     # the newest, not the first
        assert listing(dest) == [stem, f"{stem}-2"]

    def test_the_newest_is_found_past_suffix_ten(self, run, behind, dest, monkeypatch):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        resolved = resolve_config_path(str(behind))
        try:
            for _ in range(10):
                assert cli_backup.backup_before_migrate(resolved, dest, (HEAD,)).reused is False
                bump_generation(behind)
            last = cli_backup.backup_before_migrate(resolved, dest, (HEAD,))
            again = cli_backup.backup_before_migrate(resolved, dest, (HEAD,))
        finally:
            dispose_all()
        assert again.reused is True and again.path == last.path

    @pytest.mark.parametrize("names, newest", [
        (["20261022T120005Z", "20261022T120005Z-2", "20261022T120005Z-9", "20261022T120005Z-10"],
         "20261022T120005Z-10"),                                           # -10 is after -9
        (["20261022T120005Z-9", "20261022T120006Z"], "20261022T120006Z"),  # a later stamp beats a higher suffix
        (["20261022T120006Z-12", "20261022T120007Z", "20261022T120005Z-30"], "20261022T120007Z"),
        (["20261022T120005Z", "20261022T120005Z-2"], "20261022T120005Z-2"),  # a plain name is suffix 1
        (["20261022T120005Z", "20261022T120004Z-5"], "20261022T120005Z"),
        (["20261021T235959Z-3", "20261022T000000Z"], "20261022T000000Z"),
    ])
    def test_the_newest_backup_is_ranked_by_stamp_then_numeric_suffix(
        self, run, behind, dest, names, newest
    ):
        resolved = resolve_config_path(str(behind))
        prefix = f"config.db.pre-{HEAD}"
        try:
            seed = cli_backup.backup_before_migrate(resolved, tmp_seed := dest.parent / "seed", (HEAD,))
        finally:
            dispose_all()
        good = seed.path.read_bytes()
        dest.mkdir()
        for name in names:                         # only the expected-newest name holds the matching copy
            (dest / f"{prefix}-{name}").write_bytes(good if name == newest else b"garbage" * 10)
        dir_fd = os.open(dest, os.O_RDONLY)
        try:
            found = cli_backup._identical_backup(
                dir_fd, dest, prefix, (seed.store_id, seed.generation), (HEAD,))
        finally:
            os.close(dir_fd)
        assert found == dest / f"{prefix}-{newest}"

    def test_a_backup_in_another_directory_does_not_count(self, run, behind, dest, tmp_path, monkeypatch):
        failing_upgrade(monkeypatch)
        assert migrate(run, behind, "--backup-dir", dest).exit_code != 0
        other = tmp_path / "other"
        result = migrate(run, behind, "--backup-dir", other)
        assert "Backed up" in result.stdout and len(backups(other)) == 1

    def test_the_reuse_reports_the_flag_and_the_facts(self, run, behind, dest):
        resolved = resolve_config_path(str(behind))
        try:
            first = cli_backup.backup_before_migrate(resolved, dest, (HEAD,))
            again = cli_backup.backup_before_migrate(resolved, dest, (HEAD,))
        finally:
            dispose_all()
        assert isinstance(again, cli_backup.PreMigrationBackup)
        assert (again.store_id, again.generation) == (first.store_id, first.generation)
        assert again.reused is True and again.schema_rev


# --------------------------------------------------------------------------
# the packaging pre-check, and what propagates without a backup
# --------------------------------------------------------------------------


class TestPackagingPrecheck:
    def test_a_packaging_defect_is_78_with_nothing_on_disk(self, run, behind, dest, monkeypatch):
        def broken():
            raise MigrationPackagingError("the migration scripts disagree with EXPECTED_HEAD")

        monkeypatch.setattr(cli_main, "check_migration_packaging", broken)
        before = dump(behind)
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 78, result.stderr
        assert "Traceback" not in result.stderr
        assert not os.path.lexists(dest)
        assert_still_behind(run, behind, before)

    def test_with_an_existing_directory_it_stays_empty(self, run, behind, dest, monkeypatch):
        dest.mkdir()
        monkeypatch.setattr(
            cli_main, "check_migration_packaging",
            lambda: (_ for _ in ()).throw(MigrationPackagingError("bad")))
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 78
        assert listing(dest) == []

    def test_it_runs_before_the_workers_refusal(self, run, behind, dest, workers_hold, monkeypatch):
        monkeypatch.setattr(
            cli_main, "check_migration_packaging",
            lambda: (_ for _ in ()).throw(MigrationPackagingError("bad")))
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 78       # not 75

    def test_it_runs_under_the_writer_lock(self, run, behind, dest, monkeypatch):
        resolved = resolve_config_path(str(behind))
        outcome = []

        def spy():
            def attempt():
                try:
                    with writer_lock(resolved, timeout=0):
                        outcome.append("free")
                except ConfigBusy:
                    outcome.append("held")

            thread = threading.Thread(target=attempt)
            thread.start()
            thread.join(10)

        monkeypatch.setattr(cli_main, "check_migration_packaging", spy)
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        assert outcome == ["held"]

    def test_it_is_not_called_at_head(self, run, db, dest, monkeypatch):
        make_db(run, db, RICH_YAML)

        def boom():
            raise AssertionError("the packaging check must not run at head")

        monkeypatch.setattr(cli_main, "check_migration_packaging", boom)
        result = migrate(run, db, "--backup-dir", dest)
        assert result.exit_code == 0 and "up to date" in result.stdout
        assert not os.path.lexists(dest)

    def test_the_real_check_passes_on_this_tree(self, run, behind, dest):
        # (the synthetic head is a consistent package)
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0

    def test_a_foreign_database_propagates_with_no_backup(self, run, tmp_path, dest):
        foreign = tmp_path / "cfg" / "config.db"
        foreign.parent.mkdir()
        conn = sqlite3.connect(str(foreign))
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, x TEXT)")
        conn.commit()
        conn.close()
        before = foreign.read_bytes()
        result = migrate(run, foreign, "--backup-dir", dest)
        assert result.exit_code == 78, result.stderr
        assert not os.path.lexists(dest)
        assert foreign.read_bytes() == before

    def test_an_unknown_revision_propagates_with_no_backup(self, run, behind, dest):
        conn = sqlite3.connect(str(behind))
        conn.execute("UPDATE alembic_version SET version_num = 'deadbeef0001'")
        conn.commit()
        conn.close()
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 78, result.stderr
        assert not os.path.lexists(dest)
        assert revision_of(behind) == ["deadbeef0001"]

    def test_a_tableless_database_propagates_with_no_backup(self, run, tmp_path, dest):
        empty = tmp_path / "cfg" / "config.db"
        empty.parent.mkdir()
        conn = sqlite3.connect(str(empty))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE t (x)")
        conn.execute("DROP TABLE t")
        conn.commit()
        conn.close()
        result = migrate(run, empty, "--backup-dir", dest)
        assert result.exit_code in (1, 78), result.stderr
        assert not os.path.lexists(dest)


# --------------------------------------------------------------------------
# the presence lock
# --------------------------------------------------------------------------


class TestPresenceLock:
    @pytest.mark.parametrize("backup_flag", [True, False])
    def test_a_running_daemon_is_75_and_nothing_is_written(
        self, run, behind, dest, live_daemon, backup_flag
    ):
        live_daemon(behind)
        before = dump(behind)
        extra = ("--backup-dir", dest) if backup_flag else ()
        result = migrate(run, behind, *extra)
        assert result.exit_code == 75, result.stderr
        assert "daemon" in result.stderr and "Traceback" not in result.stderr
        assert not os.path.lexists(dest)
        assert_still_behind(run, behind, before)

    def test_workers_alone_are_75(self, run, behind, dest, workers_hold):
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 75
        assert not os.path.lexists(dest)

    def test_both_are_75(self, run, behind, dest, workers_hold, live_daemon):
        live_daemon(behind)
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 75
        assert not os.path.lexists(dest)
        assert revision_of(behind) == [HEAD]

    def test_the_message_says_the_daemon_must_be_stopped(self, run, behind, live_daemon):
        live_daemon(behind)
        text = " ".join(migrate(run, behind).stderr.split())
        assert "a migration needs the daemon stopped, including its own workers" in text

    @pytest.mark.parametrize("backup_flag", [True, False])
    def test_a_lock_that_cannot_be_examined_is_1(self, run, behind, dest, monkeypatch, backup_flag):
        def broken(resolved):
            raise daemonlock.DaemonLockError(errno.EACCES, "cannot open the daemon lock", "x")

        monkeypatch.setattr(daemonlock, "probe", broken)
        extra = ("--backup-dir", dest) if backup_flag else ()
        result = migrate(run, behind, *extra)
        assert result.exit_code == 1, result.stderr
        assert "Traceback" not in result.stderr
        assert not os.path.lexists(dest)
        assert_still_behind(run, behind)

    def test_at_head_a_running_daemon_is_not_a_problem(self, run, db, dest, live_daemon):
        make_db(run, db, RICH_YAML)
        live_daemon(db)
        for extra in ((), ("--backup-dir", dest)):
            result = migrate(run, db, *extra)
            assert result.exit_code == 0 and "up to date" in result.stdout
        assert not os.path.lexists(dest)

    def test_check_is_not_refused_by_a_daemon(self, run, behind, live_daemon):
        live_daemon(behind)
        assert migrate(run, behind, "--check").exit_code == 1

    def test_once_the_daemon_has_stopped_it_works(self, run, behind, dest, live_daemon):
        daemon = live_daemon(behind)
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 75
        daemon.close()
        assert migrate(run, behind, "--backup-dir", dest).exit_code == 0
        assert len(backups(dest)) == 1 and revision_of(behind) == [SYNTH]

    def test_the_success_text_says_to_start_the_daemon(self, run, behind):
        result = migrate(run, behind)
        assert result.exit_code == 0
        assert "Start the daemon to run this schema" in " ".join(result.stdout.split())


# --------------------------------------------------------------------------
# what an interrupt says
# --------------------------------------------------------------------------


class TestInterruptedWording:
    """`State.migrating` is set immediately before `upgrade_schema`: only an
    interrupt from there on cannot say whether the schema changed."""

    def interrupt_in(self, monkeypatch, where, exc=KeyboardInterrupt):
        def raiser(*a, **k):
            raise exc()

        monkeypatch.setattr(*where, raiser)

    def test_during_the_upgrade_the_outcome_is_unknown(self, run, behind, monkeypatch):
        self.interrupt_in(monkeypatch, (cli_main, "upgrade_schema"))
        result = migrate(run, behind)
        assert result.exit_code == 130
        text = " ".join(result.stderr.split())
        assert "interrupted -- the migration may or may not have been applied" in text
        assert "migrate --check" in text

    def test_a_terminate_during_the_upgrade_says_terminated(self, run, behind, monkeypatch):
        self.interrupt_in(monkeypatch, (cli_main, "upgrade_schema"), cli_main._Terminated)
        result = migrate(run, behind)
        assert result.exit_code == 143
        assert "terminated -- the migration may or may not have been applied" in " ".join(result.stderr.split())

    def test_the_hint_names_the_database(self, run, behind, monkeypatch):
        self.interrupt_in(monkeypatch, (cli_main, "upgrade_schema"))
        assert str(behind) in migrate(run, behind).stderr

    def test_during_the_upgrade_with_a_backup_dir_too(self, run, behind, dest, monkeypatch):
        self.interrupt_in(monkeypatch, (cli_main, "upgrade_schema"))
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 130
        assert "may or may not have been applied" in result.stderr
        assert len(backups(dest)) == 1                       # the backup was written before the upgrade began

    @pytest.mark.parametrize("exc, word, code", [
        (KeyboardInterrupt, "interrupted", 130), (None, "terminated", 143)])
    def test_during_the_backup_it_is_the_plain_word(self, run, behind, dest, monkeypatch, exc, word, code):
        self.interrupt_in(monkeypatch, (cli_backup, "backup_database"), exc or cli_main._Terminated)
        before = dump(behind)
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == code
        assert result.stderr.strip() == word
        assert "may or may not" not in result.stderr
        assert not os.path.lexists(dest)
        assert_still_behind(run, behind, before)

    def test_during_the_packaging_check_it_is_the_plain_word(self, run, behind, dest, monkeypatch):
        self.interrupt_in(monkeypatch, (cli_main, "check_migration_packaging"))
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 130 and result.stderr.strip() == "interrupted"
        assert not os.path.lexists(dest)

    def test_during_the_refusals_it_is_the_plain_word(self, run, behind, dest, monkeypatch):
        self.interrupt_in(monkeypatch, (daemonlock, "probe"))
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 130 and result.stderr.strip() == "interrupted"
        assert not os.path.lexists(dest)
        assert_still_behind(run, behind)

    def test_check_keeps_the_plain_word(self, run, behind, monkeypatch):
        self.interrupt_in(monkeypatch, (cli_main, "check_config_schema"))
        result = migrate(run, behind, "--check")
        assert result.exit_code == 130
        assert result.stderr.strip() == "interrupted"

    def test_before_the_lock_it_is_the_plain_word_too(self, run, behind, monkeypatch):
        self.interrupt_in(monkeypatch, (cli_main, "check_config_schema"))
        result = migrate(run, behind)
        assert result.exit_code == 130
        assert result.stderr.strip() == "interrupted"
        assert "may or may not" not in result.stderr

    def test_another_command_is_not_affected(self, run, behind, monkeypatch):
        self.interrupt_in(monkeypatch, (cli_main, "check_config_schema"))
        result = run("-c", behind, "status")
        assert "may or may not have been applied" not in result.stderr


# --------------------------------------------------------------------------
# subprocess drivers
# --------------------------------------------------------------------------

DRIVER = (
    "import json, os, signal, sys, time\n"
    "from pathlib import Path\n"
    "from zfsbackup.cli import main\n"
    "from zfsbackup.config.store import migrate as sm\n"
    "spec = json.loads(sys.argv[1])\n"
    "sm._MIGRATIONS_DIR = Path(spec['scripts'])\n"
    "sm.EXPECTED_HEAD = spec['synth']\n"
    "sm.KNOWN_REVISIONS = frozenset(sm.KNOWN_REVISIONS | {spec['synth']})\n"
    "if spec.get('marker'):\n"
    "    real_lock = main.writer_lock\n"
    "    def marking(*a, **k):\n"
    "        Path(spec['marker']).write_text('at the lock')\n"
    "        return real_lock(*a, **k)\n"
    "    main.writer_lock = marking\n"
    "if spec.get('signal'):\n"
    "    import zfsbackup.config.store.backup as sb\n"
    "    sig = getattr(signal, spec['signal'])\n"
    "    def link(src, dst, *a, **k):\n"
    "        print('TEMP-WRITTEN', os.path.exists(src), flush=True)\n"
    "        os.kill(os.getpid(), sig)\n"
    "        time.sleep(20)\n"
    "    sb.os.link = link\n"
    "main.main(spec['args'])\n"
)


def spawn(spec: dict, cwd: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", DRIVER, json.dumps(spec)],
        env=dict(os.environ, PYTHONPATH=str(REPO)), cwd=str(cwd), start_new_session=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


def finish(process, timeout=90):
    try:
        out, err = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise
    finally:
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGKILL)
    return process.returncode, out, err


# --------------------------------------------------------------------------
# 6. concurrent migrates
# --------------------------------------------------------------------------


@needs_fork
class TestConcurrent:
    def test_two_parallel_runs_give_one_backup_and_one_migration(
        self, run, behind, synth, dest, tmp_path
    ):
        resolved = resolve_config_path(str(behind))
        markers = [tmp_path / "m1", tmp_path / "m2"]
        children = []
        ready, leave = threading.Event(), threading.Event()

        def holder():
            with writer_lock(resolved):
                ready.set()
                leave.wait(120)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            for marker in markers:
                children.append(spawn({
                    "scripts": str(synth.scripts), "synth": SYNTH, "marker": str(marker),
                    "args": ["-c", str(behind), "migrate", "--backup-dir", str(dest)],
                }, tmp_path))
            # Both have passed the "behind" pre-check and wait for the lock.
            deadline = time.monotonic() + 90
            while not all(m.exists() for m in markers):
                assert time.monotonic() < deadline, "the children never reached the lock"
                assert all(c.poll() is None for c in children), "a child exited early"
                time.sleep(0.05)
        finally:
            leave.set()
            thread.join(10)
        results = [finish(c) for c in children]
        assert [r[0] for r in results] == [0, 0], results
        outputs = [r[1] for r in results]
        assert sum("Migrated the schema" in o for o in outputs) == 1, outputs
        assert sum("up to date" in o for o in outputs) == 1, outputs
        assert len(backups(dest)) == 1 and listing(dest) == [backups(dest)[0].name]
        assert sum("Backed up" in o for o in outputs) == 1
        assert revision_of(backups(dest)[0]) == [HEAD]
        assert revision_of(behind) == [SYNTH]
        assert migrate(run, behind, "--check").exit_code == 0

    def test_a_parallel_run_that_loses_does_not_create_its_own_dir(
        self, run, behind, synth, dest, tmp_path
    ):
        other = tmp_path / "other-bk"
        resolved = resolve_config_path(str(behind))
        marker = tmp_path / "m"
        ready, leave = threading.Event(), threading.Event()

        def holder():
            with writer_lock(resolved):
                ready.set()
                leave.wait(120)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            child = spawn({
                "scripts": str(synth.scripts), "synth": SYNTH, "marker": str(marker),
                "args": ["-c", str(behind), "migrate", "--backup-dir", str(other)],
            }, tmp_path)
            deadline = time.monotonic() + 90
            while not marker.exists():
                assert time.monotonic() < deadline and child.poll() is None
                time.sleep(0.05)
            # a competitor migrates while the child waits, then the lock is released
            leave.set()
            thread.join(10)
        finally:
            leave.set()
        code, out, err = finish(child)
        assert code == 0, err
        assert len(backups(other)) == 1 and revision_of(behind) == [SYNTH]


# --------------------------------------------------------------------------
# 8. signals
# --------------------------------------------------------------------------


@needs_fork
class TestSignals:
    @pytest.mark.parametrize("signame, code", [("SIGINT", 130), ("SIGTERM", 143)])
    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_a_signal_during_the_backup_does_not_migrate_and_leaves_no_file(
        self, run, behind, synth, dest, tmp_path, signame, code, dir_exists
    ):
        if dir_exists:
            dest.mkdir()
        before = dump(behind)
        process = spawn({
            "scripts": str(synth.scripts), "synth": SYNTH, "signal": signame,
            "args": ["-c", str(behind), "migrate", "--backup-dir", str(dest)],
        }, tmp_path)
        rc, out, err = finish(process)
        assert "TEMP-WRITTEN True" in out, (out, err)   # the signal landed with the copy half-published
        assert rc == code, (out, err)
        assert "Traceback" not in err
        if dir_exists:
            assert listing(dest) == []                  # no half-written file of any name
        else:
            assert not os.path.lexists(dest)
        assert_still_behind(run, behind, before)

    def test_a_keyboard_interrupt_in_process_is_130_and_not_migrated(
        self, run, behind, dest, monkeypatch
    ):
        def interrupt(resolved, target):
            raise KeyboardInterrupt

        monkeypatch.setattr(cli_backup, "backup_database", interrupt)
        result = migrate(run, behind, "--backup-dir", dest)
        assert result.exit_code == 130
        assert not os.path.lexists(dest)
        assert_still_behind(run, behind)

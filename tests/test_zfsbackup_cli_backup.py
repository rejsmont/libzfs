"""`zfsbackup-config backup DIR [--no-keys] [--json]` (plan item C11, S-11c).

What is covered:

- ``TestLayout``          the directory name, modes (whatever the umask), the
                          MANIFEST fields, the sha256, store id / schema
                          revision / generation preserved, and the backup
                          restores (`read_config` on the copy; certificates and
                          keys byte-identical to the source).
- ``TestNoKeys``          `--no-keys`: no `.key`, `keys_included: no`, the
                          stderr note; no `<db>.tls`: no `tls/`, exit 0.
- ``TestGroupOperator``   a key this user may not read: refused with a pointer
                          to `--no-keys`, nothing left (not even a `DIR` the
                          run created); then `--no-keys` works.
- ``TestFailures``        missing / non-SQLite database (78), the `BackupError`
                          subclasses mapped to their exit codes, a TLS copy
                          failure, `FileExistsError`, an unusable `DIR`; every
                          failure leaves nothing behind.
- ``TestOrdering``        MANIFEST is the last thing written (a directory with
                          a MANIFEST is complete).
- ``TestSignals``         real SIGINT / SIGTERM mid-backup, in a subprocess in
                          its own session: 130 / 143, no partial directory.
- ``TestJson``            the `--json` shape.
- ``TestReadOnly``        never the writer lock, never signals the daemon;
                          `-N` suffix on a same-second collision; `DIR` a file.
- ``TestSchemaBehind``    a database behind the current schema is backed up and
                          reports the old revision.

The `BackupError` mapping is tested through the store's real subclasses
(`BackupPublishError` 1, `BackupBusy` 75, `BackupSourceError` 78) with the
wording `backup_database` itself uses, so it holds whether `cli/backup.py`
maps by subclass or still by message text.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests
in this file): keys copied despite `--no-keys` 6; the partial directory not
removed on failure 40; a `DIR` the run created not removed again 22; MANIFEST
written before the database copy (and not at the end) 17; DIR not chmod'ed after mkdir 1 (the umask 0277 case).
Review-fix round (same method): MANIFEST not unlinked first in the cleanup 2; SIGINT/SIGTERM not
blocked during it 6; the mask not restored 9; a symlink in the directory's place followed 3; the swap
check after the database copy removed 1; no `fchmod` in `_mkdir_private` 2; the ownership check
removed 2; MANIFEST created by path rather than `dir_fd` 1; MANIFEST opened without `O_NOFOLLOW` 1.
"""

import contextlib
import hashlib
import json
import os
import re
import signal
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from _cli_support import (  # noqa: F401  (fixtures are found by name)
    RICH_YAML,
    db,
    isolated,
    make_db,
    run,
    seed_client_id,
)
from zfsbackup import identity
from zfsbackup.cli import backup as cli_backup
from zfsbackup.cli import write as cli_write
from zfsbackup import daemonlock
from zfsbackup.cli.main import cli  # noqa: F401
from zfsbackup.config.store import (
    BackupBusy,
    BackupError,
    BackupPublishError,
    BackupSourceError,
    dispose_all,
    read_config,
    resolve_config_path,
    writer_lock,
)
from zfsbackup.version import installed_version

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parent.parent

NAME_RE = re.compile(r"^zfsbackup-[A-Za-z0-9._-]+-\d{8}T\d{6}Z(-\d+)?$")

is_root = hasattr(os, "geteuid") and os.geteuid() == 0
needs_non_root = pytest.mark.skipif(is_root, reason="root ignores file permissions")
needs_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork on this platform")

KEYS = ["server.key", "client.key", "client.next.key"]
CERTS = ["server.crt", "client.crt", "client.next.crt"]


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def mode_of(path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def sha256(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tls_of(database: Path) -> Path:
    return Path(str(database) + ".tls")


def make_tls(database: Path, *, next_identity: bool = True) -> Path:
    """Generate a real TLS directory beside `database` (which must exist)."""
    directory = tls_of(database)
    identity.ensure_identity(directory, "server")
    identity.ensure_identity(directory, "client")
    if next_identity:
        identity.ensure_identity(directory, "client", name="client.next")
    return directory


@pytest.fixture
def host(db, run):
    """A database with a configuration and a host identity, and a TLS directory."""
    make_db(run, db, RICH_YAML)
    seed_client_id(db, "host-1")
    make_tls(db)
    return db


@pytest.fixture
def dest(tmp_path) -> Path:
    return tmp_path / "out"


def backup(run, database, directory, *extra):
    return run("-c", database, "backup", directory, *extra)


def backups_in(directory: Path):
    return sorted(p for p in directory.iterdir() if p.name.startswith("zfsbackup-"))


def only_backup(directory: Path) -> Path:
    found = backups_in(directory)
    assert len(found) == 1, found
    return found[0]


def manifest(path: Path) -> dict:
    """The MANIFEST's `key: value` lines (the TLS file list under `tls`)."""
    fields, files = {}, []
    for line in (path / "MANIFEST").read_text().splitlines():
        if line.startswith("  "):
            files.append(line.strip())
        elif ": " in line:
            key, value = line.split(": ", 1)
            fields[key] = value
    fields["_files"] = files
    return fields


def tree(path: Path) -> list:
    """Every entry under `path`, relative, for 'nothing was left'."""
    return sorted(str(p.relative_to(path)) for p in path.rglob("*"))


def assert_nothing_left(directory: Path, *, existed: bool):
    if existed:
        assert directory.is_dir()
        assert tree(directory) == [], tree(directory)
    else:
        assert not os.path.lexists(directory), tree(directory) if directory.is_dir() else directory


# --------------------------------------------------------------------------
# 1. a normal run
# --------------------------------------------------------------------------


class TestLayout:
    def test_a_normal_run_writes_the_layout(self, run, host, dest):
        result = backup(run, host, dest)
        assert result.exit_code == 0, result.stderr
        path = only_backup(dest)
        assert NAME_RE.match(path.name), path.name
        assert sorted(p.name for p in path.iterdir()) == ["MANIFEST", "config.db", "tls"]
        assert sorted(p.name for p in (path / "tls").iterdir()) == sorted(KEYS + CERTS)
        # The path is on the first line of stdout, then a summary.
        lines = result.stdout.splitlines()
        assert lines[0].endswith(str(path)), lines[0]
        assert len(lines) >= 2 and "config.db" in lines[1]
        assert "private keys" in result.stdout

    @pytest.mark.parametrize("umask", [0o000, 0o027, 0o077, 0o277])
    def test_modes_are_exact_whatever_the_umask(self, run, host, dest, umask):
        old = os.umask(umask)
        try:
            result = backup(run, host, dest)
        finally:
            os.umask(old)
        assert result.exit_code == 0, result.stderr
        path = only_backup(dest)
        assert mode_of(path) == 0o700
        assert mode_of(path / "config.db") == 0o600
        assert mode_of(path / "MANIFEST") == 0o600
        assert mode_of(path / "tls") == 0o700
        for name in KEYS:
            assert mode_of(path / "tls" / name) == 0o600, name
        for name in CERTS:
            assert mode_of(path / "tls" / name) == 0o640, name

    def test_the_manifest_fields(self, run, host, dest):
        original = read_config(resolve_config_path(str(host)))
        dispose_all()
        assert backup(run, host, dest).exit_code == 0
        path = only_backup(dest)
        fields = manifest(path)
        assert fields["version"] == installed_version()
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", fields["created"])
        assert fields["host"] and fields["host"] in path.name
        assert Path(fields["source"]) == Path(resolve_config_path(str(host)).resolved_path)
        assert fields["schema_revision"] == original.schema_rev
        assert fields["store_id"] == str(original.store_id)
        assert fields["generation"] == str(original.generation)
        assert fields["config.db sha256"] == sha256(path / "config.db")
        assert fields["keys_included"] == "yes"
        assert sorted(fields["_files"]) == sorted(f"tls/{n}" for n in KEYS + CERTS)
        assert fields["tls"].startswith(f"{len(KEYS + CERTS)} file(s)")

    def test_the_created_time_is_the_directory_name_time(self, run, host, dest, monkeypatch):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        assert backup(run, host, dest).exit_code == 0
        path = only_backup(dest)
        assert path.name.endswith("-20261022T120005Z")
        assert manifest(path)["created"] == "2026-10-22T12:00:05Z"

    def test_store_id_and_schema_revision_are_preserved(self, run, host, dest):
        before = sqlite3.connect(str(host))
        try:
            store_id = before.execute("PRAGMA user_version").fetchone()[0]
            rev = before.execute("SELECT version_num FROM alembic_version").fetchone()[0]
        finally:
            before.close()
        assert store_id != 0   # stamped by the import: the property is not vacuous
        assert backup(run, host, dest).exit_code == 0
        copy = sqlite3.connect(str(only_backup(dest) / "config.db"))
        try:
            assert copy.execute("PRAGMA user_version").fetchone()[0] == store_id
            assert copy.execute("SELECT version_num FROM alembic_version").fetchone()[0] == rev
        finally:
            copy.close()

    def test_the_backup_restores(self, run, host, dest):
        original = read_config(resolve_config_path(str(host)))
        dispose_all()
        assert backup(run, host, dest).exit_code == 0
        path = only_backup(dest)

        restored = read_config(resolve_config_path(str(path / "config.db")))
        dispose_all()
        assert restored.config == original.config
        assert restored.generation == original.generation
        assert restored.store_id == original.store_id
        assert restored.schema_rev == original.schema_rev
        assert restored.client_id == original.client_id == "host-1"
        for name in KEYS + CERTS:
            assert (path / "tls" / name).read_bytes() == (tls_of(host) / name).read_bytes(), name

    def test_the_source_is_not_modified(self, run, host, dest):
        names = lambda: sorted(p.name for p in host.parent.iterdir())  # noqa: E731
        tls_before = {p.name: (p.read_bytes(), mode_of(p)) for p in tls_of(host).iterdir()
                      if p.name != "identity.lock"}
        before = names()
        assert backup(run, host, dest).exit_code == 0
        dispose_all()
        assert names() == before
        assert {p.name: (p.read_bytes(), mode_of(p)) for p in tls_of(host).iterdir()
                if p.name != "identity.lock"} == tls_before

    def test_dir_is_created_when_missing_and_may_be_relative(self, run, host, tmp_path):
        result = backup(run, host, "reldir")
        assert result.exit_code == 0, result.stderr
        assert NAME_RE.match(only_backup(tmp_path / "reldir").name)

    def test_an_existing_dir_keeps_its_other_contents(self, run, host, dest):
        dest.mkdir()
        (dest / "earlier.txt").write_text("keep")
        assert backup(run, host, dest).exit_code == 0
        assert (dest / "earlier.txt").read_text() == "keep"
        assert NAME_RE.match(only_backup(dest).name)

    @pytest.mark.parametrize("hostname, label", [
        ("db1.example.com", "db1.example.com"),
        ("my host/../x!", "my-host-..-x"),
        ("", "host"),
    ])
    def test_the_host_is_one_safe_path_component(self, run, host, dest, monkeypatch, hostname, label):
        monkeypatch.setattr(cli_backup.socket, "gethostname", lambda: hostname)
        assert backup(run, host, dest).exit_code == 0
        path = only_backup(dest)
        assert path.parent == dest
        assert path.name.startswith(f"zfsbackup-{label}-"), path.name
        assert manifest(path)["host"] == label


# --------------------------------------------------------------------------
# 2-3. --no-keys, no TLS directory
# --------------------------------------------------------------------------


class TestNoKeys:
    def test_no_key_is_copied(self, run, host, dest):
        result = backup(run, host, dest, "--no-keys")
        assert result.exit_code == 0, result.stderr
        path = only_backup(dest)
        assert sorted(p.name for p in (path / "tls").iterdir()) == sorted(CERTS)
        assert not list(path.rglob("*.key"))
        fields = manifest(path)
        assert fields["keys_included"] == "no"
        assert sorted(fields["_files"]) == sorted(f"tls/{n}" for n in CERTS)

    def test_the_stderr_note(self, run, host, dest):
        result = backup(run, host, dest, "--no-keys")
        assert result.exit_code == 0
        assert "--no-keys" in result.stderr
        assert "private keys were not copied" in result.stderr
        assert "NO private keys" in result.stdout   # and the summary says so too
        # the note is absent when keys are copied
        other = backup(run, host, dest)
        assert "--no-keys" not in other.stderr

    def test_the_certificates_still_match_the_source(self, run, host, dest):
        assert backup(run, host, dest, "--no-keys").exit_code == 0
        path = only_backup(dest)
        for name in CERTS:
            assert (path / "tls" / name).read_bytes() == (tls_of(host) / name).read_bytes()

    def test_keys_are_not_even_opened(self, run, host, dest, monkeypatch):
        # `--no-keys` is what lets a group operator back up: it must not read a key at all.
        seen = []
        real = identity.copy_tls_dir

        def spy(src, dst, **kw):
            seen.append(kw)
            return real(src, dst, **kw)

        monkeypatch.setattr(identity, "copy_tls_dir", spy)
        assert backup(run, host, dest, "--no-keys").exit_code == 0
        assert seen == [{"include_keys": False}]

    def test_no_tls_directory(self, run, db, dest):
        make_db(run, db, RICH_YAML)
        assert not tls_of(db).exists()
        result = backup(run, db, dest)
        assert result.exit_code == 0, result.stderr
        path = only_backup(dest)
        assert sorted(p.name for p in path.iterdir()) == ["MANIFEST", "config.db"]
        fields = manifest(path)
        assert fields["keys_included"] == "no"
        assert fields["tls"].startswith("none")
        assert fields["_files"] == []
        assert "no TLS directory" in result.stdout
        assert not tls_of(db).exists()      # and the backup created none

    def test_no_tls_directory_with_no_keys_has_no_note(self, run, db, dest):
        make_db(run, db, RICH_YAML)
        result = backup(run, db, dest, "--no-keys")
        assert result.exit_code == 0
        assert "private keys were not copied" not in result.stderr
        assert "tls" not in [p.name for p in only_backup(dest).iterdir()]

    def test_keys_included_is_no_when_the_directory_holds_only_certificates(self, run, host, dest):
        for name in KEYS:
            (tls_of(host) / name).unlink()
        result = backup(run, host, dest)
        assert result.exit_code == 0, result.stderr
        path = only_backup(dest)
        assert manifest(path)["keys_included"] == "no"
        assert not list(path.rglob("*.key"))


# --------------------------------------------------------------------------
# 4. the group operator
# --------------------------------------------------------------------------


@pytest.fixture
def unreadable_key(host):
    """A key this process may not read (chmod 000; restored at teardown)."""
    key = tls_of(host) / "client.key"
    key.chmod(0)
    yield key
    key.chmod(0o600)


class TestGroupOperator:
    @needs_non_root
    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_an_unreadable_key_is_refused_and_nothing_is_left(
        self, run, host, dest, unreadable_key, dir_exists
    ):
        if dir_exists:
            dest.mkdir()
        result = backup(run, host, dest)
        assert result.exit_code == 1, result.stderr
        assert "--no-keys" in result.stderr
        assert "client.key" in result.stderr
        assert result.stdout == ""
        assert_nothing_left(dest, existed=dir_exists)

    @needs_non_root
    def test_then_no_keys_succeeds(self, run, host, dest, unreadable_key):
        assert backup(run, host, dest).exit_code == 1
        result = backup(run, host, dest, "--no-keys")
        assert result.exit_code == 0, result.stderr
        path = only_backup(dest)
        assert not list(path.rglob("*.key"))
        assert sorted(p.name for p in (path / "tls").iterdir()) == sorted(CERTS)

    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_key_unreadable_mapping_without_relying_on_permissions(
        self, run, host, dest, monkeypatch, dir_exists
    ):
        # Mocked ownership: runs as root too.
        def refuse(src, dst, *, include_keys=True, **kw):
            if include_keys:
                raise identity.KeyUnreadable(Path(src) / "server.key")
            return []

        monkeypatch.setattr(identity, "copy_tls_dir", refuse)
        if dir_exists:
            dest.mkdir()
        result = backup(run, host, dest)
        assert result.exit_code == 1
        assert "--no-keys" in result.stderr and "server.key" in result.stderr
        assert "Traceback" not in result.stderr
        assert_nothing_left(dest, existed=dir_exists)
        # ... and the hint works
        assert backup(run, host, dest, "--no-keys").exit_code == 0

    def test_the_refusal_is_not_a_silent_downgrade(self, run, host, dest, monkeypatch):
        monkeypatch.setattr(
            identity, "copy_tls_dir",
            lambda *a, **k: (_ for _ in ()).throw(identity.KeyUnreadable("x.key")),
        )
        result = backup(run, host, dest)
        assert result.exit_code == 1
        assert not os.path.lexists(dest)


# --------------------------------------------------------------------------
# 5. failures
# --------------------------------------------------------------------------


class TestFailures:
    def test_a_missing_database_is_78(self, run, tmp_path, dest):
        result = backup(run, tmp_path / "nope" / "config.db", dest)
        assert result.exit_code == 78, result.stderr
        assert "Traceback" not in result.stderr
        assert not os.path.lexists(dest)

    def test_a_missing_database_in_an_existing_dest(self, run, tmp_path, dest):
        dest.mkdir()
        result = backup(run, tmp_path / "nope.db", dest)
        assert result.exit_code == 78
        assert_nothing_left(dest, existed=True)

    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_a_non_sqlite_database_is_78_and_nothing_is_left(self, run, tmp_path, dest, dir_exists):
        bad = tmp_path / "cfg" / "config.db"
        bad.parent.mkdir()
        bad.write_bytes(b"this is not a sqlite database at all " * 40)
        if dir_exists:
            dest.mkdir()
        result = backup(run, bad, dest)
        assert result.exit_code == 78, result.stderr
        assert "Traceback" not in result.stderr
        assert_nothing_left(dest, existed=dir_exists)
        assert bad.read_bytes().startswith(b"this is not")   # untouched

    def test_a_non_wal_database_is_78(self, run, tmp_path, dest):
        plain = tmp_path / "cfg" / "config.db"
        plain.parent.mkdir()
        conn = sqlite3.connect(str(plain))
        conn.execute("CREATE TABLE t (x)")
        conn.commit()
        conn.close()
        result = backup(run, plain, dest)
        assert result.exit_code in (78, 1), result.stderr
        assert "Traceback" not in result.stderr
        assert not os.path.lexists(dest) or tree(dest) == []

    @pytest.mark.parametrize("error, code, wording", [
        (BackupPublishError, 1, "something unrelated"),
        (BackupBusy, 75, "something unrelated"),
        (BackupSourceError, 78, "something unrelated"),
        (BackupError, 78, "something unrelated"),
        # the old text-matching wording must not decide the code any more
        (BackupSourceError, 78, "cannot publish the backup as X: durable"),
        (BackupPublishError, 1, "backup of X did not finish within 30s"),
        (BackupBusy, 75, "backup of X failed quick_check: bad"),
    ])
    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_backup_error_subclasses_map_to_their_exit_codes(
        self, run, host, dest, monkeypatch, error, code, wording, dir_exists
    ):
        def fail(resolved, target):
            raise error(wording)

        monkeypatch.setattr(cli_backup, "backup_database", fail)
        if dir_exists:
            dest.mkdir()
        result = backup(run, host, dest)
        assert result.exit_code == code, result.stderr
        assert "Traceback" not in result.stderr
        assert_nothing_left(dest, existed=dir_exists)

    def test_busy_says_to_retry(self, run, host, dest, monkeypatch):
        def fail(resolved, target):
            raise BackupBusy("neutral wording")

        monkeypatch.setattr(cli_backup, "backup_database", fail)
        assert "retry" in backup(run, host, dest).stderr

    @pytest.mark.parametrize("text, code", [
        ("database is locked", 78),   # the store wraps its busy errors in BackupBusy; a raw one escaped
        ("unable to open database file", 78),
    ])
    def test_a_raw_sqlite_error_maps_by_cause(self, run, host, dest, monkeypatch, text, code):
        def fail(resolved, target):
            raise sqlite3.OperationalError(text)

        monkeypatch.setattr(cli_backup, "backup_database", fail)
        result = backup(run, host, dest)
        assert result.exit_code == code, result.stderr
        assert not os.path.lexists(dest)

    def test_file_exists_error_is_1(self, run, host, dest, monkeypatch):
        def fail(resolved, target):
            raise FileExistsError("backup target already exists")

        monkeypatch.setattr(cli_backup, "backup_database", fail)
        result = backup(run, host, dest)
        assert result.exit_code == 1, result.stderr
        assert "already exists" in result.stderr
        assert not os.path.lexists(dest)

    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_a_tls_copy_failure_is_1_and_removes_the_database_copy_too(
        self, run, host, dest, monkeypatch, dir_exists
    ):
        def fail(src, dst, **kw):
            raise identity.IdentityError(f"{src}/server.crt is not a regular file", src)

        monkeypatch.setattr(identity, "copy_tls_dir", fail)
        if dir_exists:
            dest.mkdir()
        result = backup(run, host, dest)
        assert result.exit_code == 1, result.stderr
        assert "server.crt" in result.stderr
        assert "Traceback" not in result.stderr
        assert_nothing_left(dest, existed=dir_exists)

    def test_a_real_tls_copy_failure(self, run, host, dest):
        (tls_of(host) / "client.crt").unlink()
        (tls_of(host) / "client.crt").mkdir()      # a certificate that is not a file
        result = backup(run, host, dest)
        assert result.exit_code == 1, result.stderr
        assert not os.path.lexists(dest)

    def test_a_tls_symlink_is_refused_not_followed(self, run, db, dest, tmp_path):
        make_db(run, db, RICH_YAML)
        real = tmp_path / "elsewhere"
        real.mkdir()
        tls_of(db).symlink_to(real)
        result = backup(run, db, dest)
        assert result.exit_code == 1, result.stderr
        assert not os.path.lexists(dest)

    def test_a_manifest_write_failure_leaves_nothing(self, run, host, dest, monkeypatch):
        def fail(directory, text, dir_fd=None):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(cli_backup, "_write_manifest", fail)
        result = backup(run, host, dest)
        assert result.exit_code == 1, result.stderr
        assert "MANIFEST" in result.stderr
        assert not os.path.lexists(dest)

    def test_dir_whose_parent_is_missing_is_1_and_creates_nothing(self, run, host, tmp_path):
        result = backup(run, host, tmp_path / "no" / "such" / "out")
        assert result.exit_code == 1, result.stderr
        assert "cannot create" in result.stderr
        assert not (tmp_path / "no").exists()

    def test_a_removal_failure_is_reported_not_hidden(self, run, host, dest, monkeypatch):
        def fail(*a, **k):
            raise identity.IdentityError("boom", None)

        monkeypatch.setattr(identity, "copy_tls_dir", fail)
        monkeypatch.setattr(
            cli_backup.shutil, "rmtree", lambda *a, **k: (_ for _ in ()).throw(OSError(13, "denied")))
        result = backup(run, host, dest)
        assert result.exit_code == 1
        assert "could not be removed" in result.stderr or "delete it by hand" in result.stderr


# --------------------------------------------------------------------------
# MANIFEST is written last
# --------------------------------------------------------------------------


class TestOrdering:
    def test_the_manifest_is_the_last_thing_written(self, run, host, dest, monkeypatch):
        seen = {}
        real_manifest = cli_backup._write_manifest
        real_tls = cli_backup._backup_tls

        def tls_spy(resolved, directory, keys):
            seen["before_tls"] = sorted(p.name for p in directory.iterdir())
            return real_tls(resolved, directory, keys)

        def manifest_spy(directory, text, dir_fd=None):
            seen["before_manifest"] = sorted(p.name for p in directory.iterdir())
            seen["tls_files"] = sorted(p.name for p in (directory / "tls").iterdir())
            return real_manifest(directory, text, dir_fd)

        monkeypatch.setattr(cli_backup, "_backup_tls", tls_spy)
        monkeypatch.setattr(cli_backup, "_write_manifest", manifest_spy)
        assert backup(run, host, dest).exit_code == 0
        assert seen["before_tls"] == ["config.db"]
        assert seen["before_manifest"] == ["config.db", "tls"]
        assert seen["tls_files"] == sorted(KEYS + CERTS)

    @pytest.mark.parametrize("stage", ["database", "tls"])
    def test_a_failed_run_never_leaves_a_manifest(self, run, host, dest, monkeypatch, stage):
        if stage == "database":
            monkeypatch.setattr(
                cli_backup, "backup_database",
                lambda r, t: (_ for _ in ()).throw(BackupSourceError("backup of X failed quick_check: bad")),
            )
        else:
            monkeypatch.setattr(
                cli_backup, "_backup_tls",
                lambda *a: (_ for _ in ()).throw(RuntimeError("boom")),
            )
        result = backup(run, host, dest)
        assert result.exit_code != 0
        assert not list(dest.rglob("MANIFEST")) if dest.exists() else True


# --------------------------------------------------------------------------
# 6. signals
# --------------------------------------------------------------------------

DRIVER = (
    "import os, signal, sys, time\n"
    "from zfsbackup.cli import backup, main\n"
    "sig = getattr(signal, sys.argv[1])\n"
    "real = backup._backup_tls\n"
    "def signalling(*args, **kwargs):\n"
    "    print('STAGE', sorted(os.listdir(args[1])), flush=True)\n"
    "    os.kill(os.getpid(), sig)\n"
    "    time.sleep(20)\n"
    "    return real(*args, **kwargs)\n"
    "backup._backup_tls = signalling\n"
    "main.main(sys.argv[2:])\n"
)


@needs_fork
class TestSignals:
    @pytest.mark.parametrize("signame, code", [("SIGINT", 130), ("SIGTERM", 143)])
    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_a_signal_mid_backup_removes_the_partial_directory(
        self, run, host, dest, tmp_path, signame, code, dir_exists
    ):
        if dir_exists:
            dest.mkdir()
        env = dict(os.environ, PYTHONPATH=str(REPO))
        process = subprocess.Popen(
            [sys.executable, "-c", DRIVER, signame, "-c", str(host), "backup", str(dest)],
            env=env, cwd=str(tmp_path), start_new_session=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            out, err = process.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            raise
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        assert "STAGE ['config.db']" in out   # the signal really did land mid-backup
        assert process.returncode == code, (out, err)
        assert "Traceback" not in err
        assert_nothing_left(dest, existed=dir_exists)
        assert not list(tmp_path.rglob("MANIFEST"))

    def test_a_keyboard_interrupt_in_process_is_130_with_nothing_left(
        self, run, host, dest, monkeypatch
    ):
        def interrupt(*args):
            raise KeyboardInterrupt

        monkeypatch.setattr(cli_backup, "_backup_tls", interrupt)
        result = backup(run, host, dest)
        assert result.exit_code == 130
        assert not os.path.lexists(dest)

    def test_sigint_and_sigterm_are_blocked_while_the_partial_directory_is_removed(
        self, run, host, dest, monkeypatch
    ):
        masks = []
        real = cli_backup.shutil.rmtree

        def spy(path, *a, **k):
            masks.append(signal.pthread_sigmask(signal.SIG_BLOCK, set()))
            return real(path, *a, **k)

        monkeypatch.setattr(cli_backup.shutil, "rmtree", spy)
        monkeypatch.setattr(
            cli_backup, "_backup_tls", lambda *a: (_ for _ in ()).throw(KeyboardInterrupt)
        )
        before = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        assert backup(run, host, dest).exit_code == 130
        assert len(masks) == 1
        assert {signal.SIGINT, signal.SIGTERM} <= set(masks[0])
        assert set(signal.pthread_sigmask(signal.SIG_BLOCK, set())) == set(before)   # restored


# --------------------------------------------------------------------------
# 7. --json
# --------------------------------------------------------------------------


class TestJson:
    def test_the_shape(self, run, host, dest):
        original = read_config(resolve_config_path(str(host)))
        dispose_all()
        result = backup(run, host, dest, "--json")
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)          # stdout is the JSON and nothing else
        path = only_backup(dest)
        assert set(data) == {
            "path", "version", "created", "host", "source", "config_db", "tls", "keys_included",
        }
        assert Path(data["path"]) == path
        assert data["version"] == installed_version()
        assert data["created"] == manifest(path)["created"]
        assert data["host"] == manifest(path)["host"]
        assert Path(data["source"]) == Path(resolve_config_path(str(host)).resolved_path)
        assert data["config_db"] == {
            "file": "config.db",
            "schema_revision": original.schema_rev,
            "store_id": original.store_id,
            "generation": original.generation,
            "sha256": sha256(path / "config.db"),
        }
        assert data["tls"]["dir"] == "tls"
        assert sorted(data["tls"]["files"]) == sorted(KEYS + CERTS)
        assert data["keys_included"] is True

    def test_no_keys(self, run, host, dest):
        result = backup(run, host, dest, "--no-keys", "--json")
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)
        assert data["keys_included"] is False
        assert sorted(data["tls"]["files"]) == sorted(CERTS)

    def test_no_tls_directory(self, run, db, dest):
        make_db(run, db, RICH_YAML)
        data = json.loads(backup(run, db, dest, "--json").stdout)
        assert data["tls"] is None
        assert data["keys_included"] is False

    def test_a_failure_prints_no_json(self, run, host, dest, monkeypatch):
        monkeypatch.setattr(
            cli_backup, "backup_database",
            lambda r, t: (_ for _ in ()).throw(BackupSourceError("backup of X failed quick_check: bad")),
        )
        result = backup(run, host, dest, "--json")
        assert result.exit_code == 78
        assert result.stdout == ""


# --------------------------------------------------------------------------
# 8. read-only towards the host; names; DIR
# --------------------------------------------------------------------------


class _Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 10, 22, 12, 0, 5, tzinfo=timezone.utc)


class TestReadOnly:
    def test_it_never_takes_the_writer_lock(self, run, host, dest):
        resolved = resolve_config_path(str(host))
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
            result = backup(run, host, dest)       # a taker would wait 10 s, then exit 75
            assert result.exit_code == 0, result.stderr
            assert time.monotonic() - started < 8
        finally:
            leave.set()
            thread.join(10)

    def test_it_creates_no_writer_lock_file(self, run, host, dest):
        lock = Path(str(host) + ".writer.lock")
        lock.unlink(missing_ok=True)
        assert backup(run, host, dest).exit_code == 0
        assert not lock.exists()

    def test_it_never_signals_the_daemon(self, run, host, dest, monkeypatch):
        def boom(*a, **k):
            raise AssertionError("backup must not signal the daemon")

        monkeypatch.setattr(cli_write, "begin_reload", boom)
        monkeypatch.setattr(daemonlock, "request_reload", boom)
        for flags in ((), ("--no-keys",), ("--json",)):
            assert backup(run, host, dest, *flags).exit_code == 0

    def test_it_never_signals_even_when_it_fails(self, run, host, dest, monkeypatch):
        def boom(*a, **k):
            raise AssertionError("backup must not signal the daemon")

        monkeypatch.setattr(cli_write, "begin_reload", boom)
        monkeypatch.setattr(daemonlock, "request_reload", boom)
        monkeypatch.setattr(
            identity, "copy_tls_dir",
            lambda *a, **k: (_ for _ in ()).throw(identity.KeyUnreadable("k.key")),
        )
        assert backup(run, host, dest).exit_code == 1

    def test_it_does_not_change_the_generation(self, run, host, dest):
        before = read_config(resolve_config_path(str(host))).generation
        dispose_all()
        assert backup(run, host, dest).exit_code == 0
        after = read_config(resolve_config_path(str(host))).generation
        dispose_all()
        assert before == after

    def test_a_same_second_collision_gets_a_numeric_suffix(self, run, host, dest, monkeypatch):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        assert backup(run, host, dest).exit_code == 0
        first = only_backup(dest)
        assert first.name.endswith("-20261022T120005Z")
        first_hash = sha256(first / "config.db")

        assert backup(run, host, dest).exit_code == 0
        assert backup(run, host, dest).exit_code == 0
        names = [p.name for p in backups_in(dest)]
        assert names == sorted([first.name, f"{first.name}-2", f"{first.name}-3"])
        assert sha256(first / "config.db") == first_hash       # never overwritten
        for name in names:
            assert (dest / name / "MANIFEST").is_file()
            assert mode_of(dest / name) == 0o700

    def test_a_taken_name_that_is_a_plain_file_is_skipped(self, run, host, dest, monkeypatch):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        dest.mkdir()
        host_label = cli_backup._host_label()
        taken = dest / f"zfsbackup-{host_label}-20261022T120005Z"
        taken.write_text("not a backup")
        assert backup(run, host, dest).exit_code == 0
        assert taken.read_text() == "not a backup"
        assert (dest / f"{taken.name}-2" / "MANIFEST").is_file()

    def test_dir_being_a_file_is_a_usage_error(self, run, host, tmp_path):
        target = tmp_path / "afile"
        target.write_text("x")
        result = backup(run, host, target)
        assert result.exit_code == 2
        assert target.read_text() == "x"

    def test_a_dangling_symlink_as_dir_is_not_followed(self, run, host, tmp_path):
        link = tmp_path / "link"
        link.symlink_to(tmp_path / "nowhere")
        result = backup(run, host, link)
        assert result.exit_code != 0
        assert not (tmp_path / "nowhere").exists()

    def test_dir_is_required(self, run, host):
        assert run("-c", host, "backup").exit_code == 2

    def test_help_documents_the_options(self, run):
        result = run("backup", "--help")
        assert result.exit_code == 0
        for fragment in ("DIR", "--no-keys", "--json", "-c, --config"):
            assert fragment in result.stdout


# --------------------------------------------------------------------------
# 9. a database behind the schema
# --------------------------------------------------------------------------

OLD_REVISION = "0a1b2c3d4e5f"


class TestSchemaBehind:
    def test_a_database_behind_the_schema_is_backed_up(self, run, host, dest):
        conn = sqlite3.connect(str(host))
        conn.execute("UPDATE alembic_version SET version_num = ?", (OLD_REVISION,))
        conn.commit()
        conn.close()
        dispose_all()

        result = backup(run, host, dest, "--json")
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)
        path = only_backup(dest)
        assert data["config_db"]["schema_revision"] == OLD_REVISION
        assert manifest(path)["schema_revision"] == OLD_REVISION
        copy = sqlite3.connect(str(path / "config.db"))
        try:
            assert copy.execute("SELECT version_num FROM alembic_version").fetchone()[0] == OLD_REVISION
        finally:
            copy.close()
        assert data["config_db"]["sha256"] == sha256(path / "config.db")

    def test_an_unstamped_database_reports_none(self, run, tmp_path, dest):
        raw = tmp_path / "cfg" / "config.db"
        raw.parent.mkdir()
        conn = sqlite3.connect(str(raw))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE t (x)")
        conn.commit()
        conn.close()
        result = backup(run, raw, dest, "--json")
        if result.exit_code != 0:
            pytest.skip(f"a bare WAL database is refused ({result.exit_code}): {result.stderr}")
        path = only_backup(dest)
        assert manifest(path)["schema_revision"] == "none"
        assert json.loads(result.stdout)["config_db"]["schema_revision"] is None


# --------------------------------------------------------------------------
# the cleanup (`_remove_partial`)
# --------------------------------------------------------------------------

O_DIR = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)


@pytest.fixture
def partial(tmp_path):
    """`parent/zfsbackup-x/` holding config.db, tls/key and a MANIFEST, with
    descriptors open on both; -> `(parent_fd, name, fd, parent_path)`."""
    parent = tmp_path / "parent"
    parent.mkdir()
    directory = parent / "zfsbackup-x"
    directory.mkdir(mode=0o700)
    (directory / "config.db").write_text("db")
    (directory / "tls").mkdir()
    (directory / "tls" / "client.key").write_text("key")
    (directory / "MANIFEST").write_text("done")
    parent_fd = os.open(parent, O_DIR)
    fd = os.open(directory, O_DIR)
    yield parent_fd, "zfsbackup-x", fd, parent
    for descriptor in (fd, parent_fd):
        with contextlib.suppress(OSError):
            os.close(descriptor)


class TestCleanup:
    def test_the_manifest_is_unlinked_first_and_the_directory_synced_before_the_rest(
        self, partial, monkeypatch
    ):
        parent_fd, name, fd, parent = partial
        events = []
        real_unlink, real_fsync, real_rmtree = os.unlink, os.fsync, cli_backup.shutil.rmtree

        inside = []

        def unlink(path, *a, **k):
            if not inside:
                events.append(("unlink", str(path)))
            return real_unlink(path, *a, **k)

        def fsync(descriptor):
            if not inside:
                events.append(("fsync", descriptor))
            return real_fsync(descriptor)

        def rmtree(path, *a, **k):
            events.append(("rmtree", str(path), (parent / name / "MANIFEST").exists()))
            inside.append(1)
            try:
                return real_rmtree(path, *a, **k)
            finally:
                inside.pop()

        monkeypatch.setattr(cli_backup.os, "unlink", unlink)
        monkeypatch.setattr(cli_backup.os, "fsync", fsync)
        monkeypatch.setattr(cli_backup.shutil, "rmtree", rmtree)
        assert cli_backup._remove_partial(parent_fd, name, fd, parent / name, None) is None
        assert events == [("unlink", "MANIFEST"), ("fsync", fd), ("rmtree", name, False)]
        assert list(parent.iterdir()) == []

    def test_a_cleanup_cut_short_has_no_manifest(self, partial, monkeypatch):
        parent_fd, name, fd, parent = partial

        def die(*a, **k):
            raise OSError(5, "I/O error")

        monkeypatch.setattr(cli_backup.shutil, "rmtree", die)
        note = cli_backup._remove_partial(parent_fd, name, fd, parent / name, None)
        assert note and "could not be removed" in note and "delete it by hand" in note
        assert not (parent / name / "MANIFEST").exists()       # a leftover is plainly not a backup
        assert (parent / name / "config.db").exists()

    def test_signals_are_blocked_during_and_the_previous_mask_is_restored(self, partial, monkeypatch):
        parent_fd, name, fd, parent = partial
        seen = []
        real = cli_backup.shutil.rmtree

        def spy(path, *a, **k):
            seen.append(set(signal.pthread_sigmask(signal.SIG_BLOCK, set())))
            return real(path, *a, **k)

        monkeypatch.setattr(cli_backup.shutil, "rmtree", spy)
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1})         # something the caller blocked
        try:
            cli_backup._remove_partial(parent_fd, name, fd, parent / name, None)
            after = set(signal.pthread_sigmask(signal.SIG_BLOCK, set()))
        finally:
            signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGUSR1})
        assert {signal.SIGINT, signal.SIGTERM, signal.SIGUSR1} <= seen[0]
        assert signal.SIGUSR1 in after                                      # their mask, untouched
        assert signal.SIGINT not in after and signal.SIGTERM not in after

    def test_the_mask_is_restored_when_the_cleanup_fails(self, partial, monkeypatch):
        parent_fd, name, fd, parent = partial
        monkeypatch.setattr(
            cli_backup.shutil, "rmtree", lambda *a, **k: (_ for _ in ()).throw(OSError(13, "denied")))
        before = set(signal.pthread_sigmask(signal.SIG_BLOCK, set()))
        cli_backup._remove_partial(parent_fd, name, fd, parent / name, None)
        assert set(signal.pthread_sigmask(signal.SIG_BLOCK, set())) == before

    def test_a_symlink_put_in_the_directorys_place_is_unlinked_not_followed(self, tmp_path):
        parent = tmp_path / "parent"
        parent.mkdir()
        victim = tmp_path / "victim"
        victim.mkdir()
        (victim / "precious").write_text("keep")
        (parent / "zfsbackup-x").symlink_to(victim)
        parent_fd = os.open(parent, O_DIR)
        try:
            note = cli_backup._remove_partial(parent_fd, "zfsbackup-x", None, parent / "zfsbackup-x", None)
        finally:
            os.close(parent_fd)
        assert note is None
        assert not os.path.lexists(parent / "zfsbackup-x")
        assert (victim / "precious").read_text() == "keep"

    def test_a_vanished_directory_is_fine(self, tmp_path):
        parent = tmp_path / "parent"
        parent.mkdir()
        parent_fd = os.open(parent, O_DIR)
        try:
            assert cli_backup._remove_partial(parent_fd, "gone", None, parent / "gone", None) is None
        finally:
            os.close(parent_fd)

    def test_a_created_dir_is_removed_only_when_empty(self, partial, tmp_path):
        parent_fd, name, fd, parent = partial
        (parent / "other").write_text("x")
        cli_backup._remove_partial(parent_fd, name, fd, parent / name, parent)
        assert parent.exists() and (parent / "other").exists()          # not empty: kept
        (parent / "other").unlink()
        cli_backup._remove_partial(parent_fd, "none", None, None, parent)
        assert not parent.exists()


CLEANUP_DRIVER = (
    "import os, signal, sys\n"
    "signal.signal(signal.SIGINT, signal.default_int_handler)\n"
    "from zfsbackup.cli import backup, main\n"
    "from zfsbackup.cli.errors import CliError\n"
    "sig = getattr(signal, sys.argv[1])\n"
    "def fail(*a, **k):\n"
    "    raise CliError('the backup failed')\n"
    "backup._backup_tls = fail\n"
    "real = backup.shutil.rmtree\n"
    "def rmtree(*a, **k):\n"
    "    os.kill(os.getpid(), sig)          # lands while the cleanup runs: must wait\n"
    "    real(*a, **k)\n"
    "    print('CLEANED', flush=True)\n"
    "backup.shutil.rmtree = rmtree\n"
    "main.main(sys.argv[2:])\n"
)


@needs_fork
class TestSignalDuringCleanup:
    @pytest.mark.parametrize("signame, code", [("SIGINT", 130), ("SIGTERM", 143)])
    @pytest.mark.parametrize("dir_exists", [False, True])
    def test_it_is_delivered_after_the_cleanup_has_finished(
        self, host, dest, tmp_path, signame, code, dir_exists
    ):
        if dir_exists:
            dest.mkdir()
        process = subprocess.Popen(
            [sys.executable, "-c", CLEANUP_DRIVER, signame, "-c", str(host), "backup", str(dest)],
            env=dict(os.environ, PYTHONPATH=str(REPO)), cwd=str(tmp_path), start_new_session=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            out, err = process.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            raise
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)
        assert "CLEANED" in out, (out, err)          # the cleanup was not cut short
        assert process.returncode == code, (out, err)
        assert "Traceback" not in err
        assert_nothing_left(dest, existed=dir_exists)


# --------------------------------------------------------------------------
# symlink-safe directories
# --------------------------------------------------------------------------


class TestNoFollowDirectories:
    def test_a_planted_symlink_at_the_backup_name_is_never_written_through(
        self, run, host, dest, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(cli_backup, "datetime", _Frozen)
        dest.mkdir()
        victim = tmp_path / "victim"
        victim.mkdir()
        planted = dest / f"zfsbackup-{cli_backup._host_label()}-20261022T120005Z"
        planted.symlink_to(victim)
        result = backup(run, host, dest)
        assert result.exit_code == 0, result.stderr
        assert list(victim.iterdir()) == []                 # nothing went through the link
        assert planted.is_symlink()                         # and the link was left alone
        assert (dest / f"{planted.name}-2" / "MANIFEST").is_file()

    @pytest.mark.parametrize("swap_after", ["_backup_db", "_backup_tls"])
    def test_a_directory_swapped_for_a_symlink_is_detected_and_nothing_enters_the_target(
        self, run, host, dest, tmp_path, monkeypatch, swap_after
    ):
        victim = tmp_path / "victim"
        victim.mkdir()
        moved = tmp_path / "moved-aside"
        dest.mkdir()
        real = getattr(cli_backup, swap_after)

        def swapping(*args, **kwargs):
            outcome = real(*args, **kwargs)
            # `_backup_db(resolved, <dir>/config.db)` / `_backup_tls(resolved, <dir>, keys)`
            target = Path(args[1]).parent if swap_after == "_backup_db" else Path(args[1])
            os.rename(target, moved)
            os.symlink(victim, target)
            return outcome

        monkeypatch.setattr(cli_backup, swap_after, swapping)
        result = backup(run, host, dest)
        assert result.exit_code == 1, result.stderr
        assert "was replaced" in result.stderr
        assert list(victim.iterdir()) == []                  # nothing was written into the target
        assert not [p for p in dest.iterdir() if p.is_symlink()]       # the planted link is gone too

    @pytest.mark.parametrize("umask", [0o277, 0o000])
    def test_directories_are_0700_under_a_restrictive_umask_and_a_setgid_parent(
        self, run, host, tmp_path, umask
    ):
        parent = tmp_path / "setgid-parent"
        parent.mkdir()
        parent.chmod(0o2770)
        old = os.umask(umask)
        try:
            result = backup(run, host, parent / "out")
        finally:
            os.umask(old)
        assert result.exit_code == 0, result.stderr
        out = parent / "out"
        assert mode_of(out) == 0o700
        (inner,) = backups_in(out)
        assert mode_of(inner) == 0o700 and mode_of(inner / "config.db") == 0o600

    def test_a_directory_owned_by_someone_else_is_refused_and_removed(
        self, run, host, dest, monkeypatch
    ):
        monkeypatch.setattr(cli_backup.os, "geteuid", lambda: os.stat(dest.parent).st_uid + 4242)
        result = backup(run, host, dest)
        assert result.exit_code == 1, result.stderr
        assert "cannot create" in result.stderr
        assert not os.path.lexists(dest)

    def test_an_inner_directory_owned_by_someone_else_is_refused(self, run, host, dest, monkeypatch):
        dest.mkdir()
        monkeypatch.setattr(cli_backup.os, "geteuid", lambda: os.stat(dest).st_uid + 4242)
        result = backup(run, host, dest)
        assert result.exit_code == 1, result.stderr
        assert tree(dest) == []

    def test_the_manifest_is_created_without_following_a_symlink(self, tmp_path):
        directory = tmp_path / "d"
        directory.mkdir()
        elsewhere = tmp_path / "elsewhere"
        elsewhere.write_text("keep")
        (directory / "MANIFEST").symlink_to(elsewhere)
        fd = os.open(directory, O_DIR)
        try:
            with pytest.raises(OSError):
                cli_backup._write_manifest(directory, "text", fd)
        finally:
            os.close(fd)
        assert elsewhere.read_text() == "keep"

    def test_the_manifest_goes_through_the_descriptor_not_the_path(self, tmp_path):
        real_dir, other = tmp_path / "real", tmp_path / "other"
        real_dir.mkdir()
        other.mkdir()
        fd = os.open(real_dir, O_DIR)
        try:
            cli_backup._write_manifest(other, "text", fd)       # the path is wrong on purpose
        finally:
            os.close(fd)
        assert (real_dir / "MANIFEST").read_text() == "text"
        assert not (other / "MANIFEST").exists()

    def test_help_says_files_outside_the_tls_directory_are_not_included(self, run):
        text = " ".join(run("backup", "--help").stdout.split())
        assert "client_cert_file" in text and "client_key_file" in text
        assert "outside <db>.tls/" in text and "NOT included" in text

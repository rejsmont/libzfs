"""C8: `zfsbackup.config.store.backup.backup_database` -- the online backup of the config DB.

`backup_database(resolved, dst) -> BackupInfo(store_id, generation, schema_rev,
sha256)`. `dst` is a file path, or an existing directory (then `dst/config.db`).
It never overwrites. The source is snapshotted through SQLite's backup API (one
step, an unpooled read-only store connection) into an IN-MEMORY database, checked
(`quick_check`), serialised and written with plain `os.write` to an `O_EXCL` 0600
temporary file, fsynced, published with an atomic no-clobber hard link and a
directory fsync. SQLite never opens anything at the destination. No writer lock;
works on a database that is behind the current schema; needs write access to the
source (a writer-or-root operation). Failures are `BackupError`,
`ConfigDbPermissionError` or `FileExistsError`, and leave nothing behind.

Sections:

- ``TestWhatIsCopied`` -- `BackupInfo` equals what the copy says, the copy is the
  whole database (config, trust, host identity, `user_version`,
  `alembic_version`), it passes the store's own readers, mode 0600, no leftovers,
  and a pending `-wal` is included (a file copy would lose it);
- ``TestDestination`` -- file, directory, existing destination, missing parent;
- ``TestNoWriterLock`` -- works while the writer lock is held, in-process and by
  another process; it never blocks a writer;
- ``TestConcurrentWriter`` -- one consistent commit's state under a writer thread
  and under a writer in another process (WAL), `integrity_check` ok;
- ``TestSchemaStates`` -- behind schema, unstamped, no `global_settings`;
- ``TestLiveConnectionGuard`` -- a pooled connection's POSIX lock survives a
  backup and a peer's close does not delete the sidecars (`_liveconn`);
- ``TestPlantedSidecars`` -- SQLite never opens anything at the destination: a
  hostile `<tmp>-wal` (a real WAL carrying a planted table) is not replayed;
- ``TestRefusals`` -- non-WAL source, the size cap, the progress deadline, a
  failed `quick_check`;
- ``TestPermissions`` -- an unwritable source or destination directory is a
  `ConfigDbPermissionError` with nothing left;
- ``TestPublish`` -- failure after the link leaves neither name, a link that is
  not permitted is a `BackupError` (no rename fallback), no clobbering, lstat
  errors other than "absent" propagate;
- ``TestFork`` -- the same call in a forked child.

Everything is file-backed under `tmp_path` and goes through `store.db` (the
project's engine factory), so `foreign_keys=ON`, WAL and the pid-keyed engine
cache are the real ones. No test here uses `mock_subprocess`.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests in
this file): SQLite opens the temporary file (the planted `-wal` is replayed) 4;
`replace` instead of `link` 3; mode 0644 1; non-WAL accepted 1; no size cap 1;
deadline ignored 3; published name not removed after a failed fsync 2; temporary
name not removed on failure 3; rename fallback restored 2; lstat swallows every
error 1; no `O_EXCL` 2; `quick_check` ignored 1; generation not read 4; the
explicit `fchmod` dropped 1 (the umask 0o277 case). Not killed: using a pooled instead of an ephemeral source connection (a
pooled connection is reset on return, so no stale snapshot is constructible from
a test), and skipping any `fsync` (not observable).
"""

import errno
import hashlib
import importlib
import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import textwrap
import threading
import time
import traceback
from pathlib import Path

import pytest
from _cli_support import seed_client_id
from _hermetic import hermetic_env

from zfsbackup.config import BackupConfig, DatasetConfig
from zfsbackup.config import store as store_package
from zfsbackup.config.store import (
    EXPECTED_HEAD,
    KNOWN_REVISIONS,
    BackupBusy,
    BackupError,
    BackupInfo,
    BackupPublishError,
    BackupSourceError,
    ConfigDbPermissionError,
    ConfigSnapshot,
    SchemaOutOfDate,
    backup_database,
    check_config_db,
    dispose_all,
    import_yaml,
    read_config,
    resolve_config_path,
    writer_lock,
    write_config,
)
from zfsbackup.config.store import _liveconn
from zfsbackup.config.store import paths as store_paths
from zfsbackup.config.store.trust import create_token

backup_module = importlib.import_module("zfsbackup.config.store.backup")

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_YAML = ROOT / "zfsbackup" / "config.example.yaml"
SEED_YAML = "snapshot_prefix: g0\ndatasets:\n  - name: tank/d0\n    frequency: 1h\n"

needs_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
HAS_PROC_LOCKS = os.path.exists("/proc/locks")
FD_DIR = "/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_engines():
    dispose_all()
    yield
    dispose_all()


@pytest.fixture
def store_dir(tmp_path):
    directory = tmp_path / "store"
    directory.mkdir()
    return directory


@pytest.fixture
def db_path(store_dir):
    return store_dir / "config.db"


@pytest.fixture
def resolved(db_path):
    return resolve_config_path(str(db_path))


@pytest.fixture
def out_dir(tmp_path):
    directory = tmp_path / "out"
    directory.mkdir()
    return directory


@pytest.fixture
def seeded(db_path, resolved, tmp_path):
    """A database holding `config.example.yaml`, a host identity and an
    enrollment token, with no engine left open."""
    import_yaml(EXAMPLE_YAML, resolved)
    dispose_all()
    seed_client_id(db_path, "host-1")
    create_token(
        resolved, secret_sha256="ab" * 32, server_fingerprint="cd" * 32,
        created_at=time.time(), expires_at=time.time() + 3600, lock=None,
    )
    dispose_all()
    return resolved


@pytest.fixture
def seeded_g0(db_path, resolved, tmp_path):
    """A database at generation 0 holding the `g0` config of `cfg_for`."""
    source = tmp_path / "seed.yaml"
    source.write_text(SEED_YAML)
    import_yaml(source, resolved)
    dispose_all()
    return resolved


def cfg_for(g: int) -> BackupConfig:
    """The configuration written as generation `g`: its prefix and its dataset
    count both encode `g`, so a copy that mixes two commits is detectable."""
    return BackupConfig(
        datasets=[DatasetConfig(name=f"tank/d{i}") for i in range(1 + g % 5)],
        snapshot_prefix=f"g{g}",
    )


def dump(db: Path) -> list:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return list(conn.iterdump())
    finally:
        conn.close()


def sql(db: Path, statement: str):
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(statement).fetchall()
    finally:
        conn.close()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def names(directory: Path) -> list:
    return sorted(p.name for p in directory.iterdir())


def copy_facts(path: Path) -> dict:
    """What the copy itself says (a throwaway connection; read BEFORE hashing
    would be wrong -- a WAL-flagged file is touched by being opened)."""
    conn = sqlite3.connect(str(path))
    try:
        gen = conn.execute("SELECT generation FROM global_settings WHERE id = 1").fetchone()
        return {
            "store_id": conn.execute("PRAGMA user_version").fetchone()[0],
            "generation": gen[0] if gen else 0,
            "schema_rev": conn.execute("SELECT version_num FROM alembic_version").fetchone()[0],
            "integrity": conn.execute("PRAGMA integrity_check").fetchall(),
            "prefix": conn.execute("SELECT snapshot_prefix FROM global_settings").fetchone()[0],
            "datasets": conn.execute("SELECT count(*) FROM datasets").fetchone()[0],
        }
    finally:
        conn.close()


def child_env():
    return hermetic_env(PYTHONPATH=str(ROOT))


# --------------------------------------------------------------------------
# What is copied
# --------------------------------------------------------------------------


@pytest.mark.integration
class TestWhatIsCopied:
    def test_the_api_is_exported(self):
        assert "backup_database" in store_package.__all__ and "BackupInfo" in store_package.__all__
        assert BackupInfo._fields == ("store_id", "generation", "schema_rev", "sha256")

    def test_backup_info_equals_the_values_read_from_the_copy_and_the_hash_matches(
        self, seeded, db_path, out_dir
    ):
        dst = out_dir / "backup.db"
        info = backup_database(seeded, dst)
        assert isinstance(info, BackupInfo)
        digest = sha256(dst)               # before any connection touches the copy
        facts = copy_facts(dst)
        assert info.sha256 == digest
        assert info.store_id == facts["store_id"] != 0
        assert info.generation == facts["generation"] == 0
        assert info.schema_rev == facts["schema_rev"] == EXPECTED_HEAD
        assert len(info.sha256) == 64

    def test_user_version_and_alembic_version_equal_the_source(self, seeded, db_path, out_dir):
        info = backup_database(seeded, out_dir / "b.db")
        assert info.store_id == sql(db_path, "PRAGMA user_version")[0][0]
        assert sql(out_dir / "b.db", "PRAGMA user_version") == sql(db_path, "PRAGMA user_version")
        assert sql(out_dir / "b.db", "SELECT version_num FROM alembic_version") == sql(
            db_path, "SELECT version_num FROM alembic_version"
        )

    def test_the_copy_is_the_whole_database(self, seeded, db_path, out_dir):
        backup_database(seeded, out_dir / "b.db")
        assert dump(out_dir / "b.db") == dump(db_path)
        # ...including what `export` omits: trust and the host identity.
        assert sql(out_dir / "b.db", "SELECT client_id FROM host_identity") == [("host-1",)]
        assert sql(out_dir / "b.db", "SELECT count(*) FROM enrollment_token") == [(1,)]

    def test_the_copy_passes_the_stores_own_checks_and_reads_back_equal(
        self, seeded, out_dir
    ):
        dst = out_dir / "b.db"
        info = backup_database(seeded, dst)
        copy = resolve_config_path(str(dst))
        check_config_db(copy)                                  # does not raise
        snap = read_config(copy)
        assert isinstance(snap, ConfigSnapshot)
        assert snap.config == BackupConfig.from_file(EXAMPLE_YAML)
        assert snap.generation == info.generation
        assert snap.store_id == info.store_id
        assert snap.schema_rev == info.schema_rev == EXPECTED_HEAD
        assert snap.client_id == "host-1"

    @pytest.mark.parametrize("umask", [0o000, 0o022, 0o277])
    def test_the_copy_is_exactly_mode_0600_whatever_the_umask(self, seeded, out_dir, umask):
        old = os.umask(umask)           # 0o277 would leave 0400 without an explicit fchmod
        try:
            backup_database(seeded, out_dir / "b.db")
        finally:
            os.umask(old)
        st = (out_dir / "b.db").stat()
        assert stat.S_IMODE(st.st_mode) == 0o600
        assert st.st_nlink == 1                    # the temporary name is gone
        assert st.st_uid == os.getuid()

    def test_nothing_but_the_copy_is_left_behind(self, seeded, db_path, out_dir):
        # The source has a live pooled connection (so its own -wal/-shm exist),
        # which must not leak into the destination.
        read_config(seeded)
        assert os.path.exists(str(db_path) + "-wal")
        backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == ["b.db"]
        assert not any(
            n.startswith(".") or n.endswith(("-wal", "-shm", "-journal")) for n in names(out_dir)
        )

    def test_a_pending_wal_is_included(self, seeded_g0, db_path, out_dir):
        """A plain copy of `config.db` while commits sit in the `-wal` loses
        them. A raw connection with auto-checkpointing off keeps the commit in
        the `-wal` (and, staying open, keeps the last close from checkpointing)."""
        holder = sqlite3.connect(str(db_path))
        try:
            holder.execute("PRAGMA wal_autocheckpoint=0")
            holder.execute("UPDATE global_settings SET snapshot_prefix = 'pending', generation = 7")
            holder.commit()
            wal = Path(str(db_path) + "-wal")
            assert wal.exists() and wal.stat().st_size > 0            # precondition
            assert sql(db_path, "SELECT generation FROM global_settings")[0][0] == 7
            # ...and that the main file alone does NOT have it:
            main_only = out_dir / "main-only.db"
            main_only.write_bytes(db_path.read_bytes())
            assert sql(main_only, "SELECT generation FROM global_settings") == [(0,)]
            main_only.unlink()
            for sidecar in out_dir.iterdir():
                sidecar.unlink()

            info = backup_database(seeded_g0, out_dir / "b.db")
        finally:
            holder.close()
        assert info.generation == 7
        facts = copy_facts(out_dir / "b.db")
        assert facts["generation"] == 7 and facts["prefix"] == "pending"
        assert names(out_dir) == ["b.db"]

    def test_a_copy_is_a_fixed_point_a_backup_of_a_backup_is_the_same_data(
        self, seeded, out_dir
    ):
        backup_database(seeded, out_dir / "one.db")
        info_two = backup_database(resolve_config_path(str(out_dir / "one.db")), out_dir / "two.db")
        assert dump(out_dir / "two.db") == dump(out_dir / "one.db")
        assert info_two.store_id == sql(out_dir / "one.db", "PRAGMA user_version")[0][0]

    def test_the_source_is_left_alone(self, seeded, db_path, out_dir):
        before = dump(db_path)
        read_config(seeded)
        backup_database(seeded, out_dir / "b.db")
        backup_database(seeded, out_dir / "c.db")
        dispose_all()
        assert dump(db_path) == before
        assert read_config(seeded).generation == 0


# --------------------------------------------------------------------------
# Destination
# --------------------------------------------------------------------------


@pytest.mark.integration
class TestDestination:
    def test_an_existing_directory_gets_config_db(self, seeded, out_dir):
        info = backup_database(seeded, out_dir)
        assert names(out_dir) == ["config.db"]
        assert info.sha256 == sha256(out_dir / "config.db")

    def test_a_str_or_a_path_both_work(self, seeded, out_dir):
        backup_database(seeded, str(out_dir / "a.db"))
        backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == ["a.db", "b.db"]

    def test_an_existing_file_is_never_overwritten(self, seeded, out_dir):
        dst = out_dir / "b.db"
        dst.write_bytes(b"precious")
        with pytest.raises(FileExistsError):
            backup_database(seeded, dst)
        assert dst.read_bytes() == b"precious"
        assert names(out_dir) == ["b.db"]

    def test_an_existing_config_db_in_a_directory_is_never_overwritten(self, seeded, out_dir):
        (out_dir / "config.db").write_bytes(b"precious")
        with pytest.raises(FileExistsError):
            backup_database(seeded, out_dir)
        assert (out_dir / "config.db").read_bytes() == b"precious"
        assert names(out_dir) == ["config.db"]

    def test_a_second_backup_to_the_same_name_is_refused_and_the_first_survives(
        self, seeded, out_dir
    ):
        first = backup_database(seeded, out_dir / "b.db")
        with pytest.raises(FileExistsError):
            backup_database(seeded, out_dir / "b.db")
        assert sha256(out_dir / "b.db") == first.sha256
        assert names(out_dir) == ["b.db"]

    def test_a_dangling_symlink_at_the_destination_is_not_followed_or_replaced(
        self, seeded, out_dir, tmp_path
    ):
        victim = tmp_path / "victim"
        link = out_dir / "b.db"
        link.symlink_to(victim)
        with pytest.raises(FileExistsError):
            backup_database(seeded, link)
        assert link.is_symlink() and not victim.exists()

    def test_a_missing_parent_directory_fails_and_creates_nothing(self, seeded, tmp_path):
        with pytest.raises(OSError):
            backup_database(seeded, tmp_path / "nodir" / "b.db")
        assert not (tmp_path / "nodir").exists()

    def test_a_missing_source_fails_and_leaves_no_temporary_file(self, tmp_path, out_dir):
        missing = resolve_config_path(str(tmp_path / "nowhere" / "config.db"))
        with pytest.raises(Exception) as excinfo:
            backup_database(missing, out_dir / "b.db")
        assert not isinstance(excinfo.value, FileExistsError)
        assert names(out_dir) == []
        assert not (tmp_path / "nowhere").exists()


# --------------------------------------------------------------------------
# No writer lock
# --------------------------------------------------------------------------

HOLDER = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    from zfsbackup.config.store import resolve_config_path, writer_lock
    with writer_lock(resolve_config_path(sys.argv[1])):
        Path(sys.argv[2]).write_text("held")
        sys.stdin.read()          # held until the parent closes the pipe / kills us
    """
)


class LockHolder:
    """A child process holding `<db>.writer.lock` until released."""

    def __init__(self, db: Path, ready: Path):
        self.proc = subprocess.Popen(
            [sys.executable, "-c", HOLDER, str(db), str(ready)],
            cwd=ROOT, env=child_env(), stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
        deadline = time.time() + 30
        while not ready.exists():
            if self.proc.poll() is not None or time.time() > deadline:
                err = self.proc.stderr.read() if self.proc.stderr else ""
                self.close()
                pytest.fail(f"lock holder never became ready:\n{err}")
            time.sleep(0.02)

    def close(self):
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=30)
        for stream in (self.proc.stdin, self.proc.stderr):
            if stream and not stream.closed:
                try:
                    stream.close()
                except OSError:
                    pass


@pytest.mark.integration
class TestNoWriterLock:
    def test_it_succeeds_while_this_process_holds_the_writer_lock(self, seeded, out_dir):
        with writer_lock(seeded, timeout=0):
            info = backup_database(seeded, out_dir / "b.db")
        assert info.sha256 == sha256(out_dir / "b.db")

    def test_it_succeeds_while_another_process_holds_the_writer_lock(
        self, seeded, db_path, tmp_path, out_dir
    ):
        holder = LockHolder(db_path, tmp_path / "ready")
        try:
            started = time.monotonic()
            info = backup_database(seeded, out_dir / "b.db")
            assert time.monotonic() - started < 5   # never waited out the lock timeout
            assert info.generation == 0
            assert holder.proc.poll() is None       # and it is still held
        finally:
            holder.close()

    def test_it_creates_no_writer_lock_file(self, seeded, db_path, out_dir):
        lock_file = Path(str(db_path) + ".writer.lock")
        lock_file.unlink(missing_ok=True)
        backup_database(seeded, out_dir / "b.db")
        assert not lock_file.exists()

    def test_it_does_not_block_a_writer_in_this_process(self, seeded_g0, out_dir):
        backup_database(seeded_g0, out_dir / "b.db")
        write_config(seeded_g0, cfg_for(1), expected_generation=0)
        assert read_config(seeded_g0).generation == 1


# --------------------------------------------------------------------------
# A concurrent writer
# --------------------------------------------------------------------------

WRITER = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    from zfsbackup.config import BackupConfig, DatasetConfig
    from zfsbackup.config.store import resolve_config_path, write_config

    db, ready, stop = sys.argv[1:4]
    resolved = resolve_config_path(db)
    g = 0
    Path(ready).write_text("go")
    while not Path(stop).exists():
        nxt = g + 1
        cfg = BackupConfig(
            datasets=[DatasetConfig(name=f"tank/d{i}") for i in range(1 + nxt % 5)],
            snapshot_prefix=f"g{nxt}",
        )
        write_config(resolved, cfg, expected_generation=g)
        g = nxt
    print(g)
    """
)


def assert_one_commit(copy_path: Path, info: BackupInfo, source_store_id: int):
    """The copy is a single commit's state: the generation, the prefix and the
    dataset count all agree, the file is sound, and the store id is the source's."""
    digest = sha256(copy_path)
    facts = copy_facts(copy_path)
    assert facts["integrity"] == [("ok",)]
    assert info.sha256 == digest
    assert info.generation == facts["generation"]
    assert facts["prefix"] == f"g{facts['generation']}"
    assert facts["datasets"] == 1 + facts["generation"] % 5
    assert facts["store_id"] == info.store_id == source_store_id
    assert facts["schema_rev"] == EXPECTED_HEAD


@pytest.mark.integration
class TestConcurrentWriter:
    def _observe(self, resolved, out_dir, store_id, advanced):
        """Back up repeatedly until the writer has been seen to advance by
        `advanced` generations between backups (proves the two overlapped)."""
        first = None
        deadline = time.monotonic() + 60
        n = 0
        while time.monotonic() < deadline:
            dst = out_dir / f"b{n}.db"
            info = backup_database(resolved, dst)
            assert_one_commit(dst, info, store_id)
            dst.unlink()
            first = info.generation if first is None else first
            if info.generation - first >= advanced:
                return info.generation
            n += 1
        pytest.fail("the writer never advanced while backups were running")

    def test_a_writer_thread(self, seeded_g0, db_path, out_dir):
        store_id = sql(db_path, "PRAGMA user_version")[0][0]
        stop = threading.Event()
        errors = []

        def write():
            g = 0
            try:
                while not stop.is_set():
                    write_config(seeded_g0, cfg_for(g + 1), expected_generation=g)
                    g += 1
            except BaseException as exc:                 # pragma: no cover - failure path
                errors.append(exc)

        thread = threading.Thread(target=write, daemon=True)
        thread.start()
        try:
            self._observe(seeded_g0, out_dir, store_id, advanced=5)
        finally:
            stop.set()
            thread.join(timeout=60)
        assert not thread.is_alive()
        assert errors == []

    def test_a_writer_in_another_process(self, seeded_g0, db_path, tmp_path, out_dir):
        store_id = sql(db_path, "PRAGMA user_version")[0][0]
        ready, stop = tmp_path / "w-ready", tmp_path / "w-stop"
        proc = subprocess.Popen(
            [sys.executable, "-c", WRITER, str(db_path), str(ready), str(stop)],
            cwd=ROOT, env=child_env(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            deadline = time.time() + 30
            while not ready.exists():
                if proc.poll() is not None or time.time() > deadline:
                    pytest.fail(f"writer never started: {proc.stderr.read()}")
                time.sleep(0.02)
            self._observe(seeded_g0, out_dir, store_id, advanced=5)
        finally:
            stop.write_text("stop")
            try:
                out, err = proc.communicate(timeout=60)
            except subprocess.TimeoutExpired:            # pragma: no cover - failure path
                proc.kill()
                out, err = proc.communicate()
        assert proc.returncode == 0, err
        written = int(out.strip())
        assert written >= 5
        dispose_all()
        assert read_config(seeded_g0).generation == written   # the writer was never harmed

    def test_a_copy_taken_before_a_write_does_not_see_it(self, seeded_g0, out_dir):
        info = backup_database(seeded_g0, out_dir / "before.db")
        write_config(seeded_g0, cfg_for(1), expected_generation=0)
        assert info.generation == 0
        assert copy_facts(out_dir / "before.db")["prefix"] == "g0"
        assert backup_database(seeded_g0, out_dir / "after.db").generation == 1


# --------------------------------------------------------------------------
# Schema states
# --------------------------------------------------------------------------


@pytest.mark.integration
class TestSchemaStates:
    def test_a_database_that_is_behind_the_current_schema_is_backed_up(
        self, seeded, db_path, out_dir
    ):
        behind = sorted(KNOWN_REVISIONS - {EXPECTED_HEAD})
        if not behind:
            pytest.skip("only one schema revision exists")
        older = behind[0]
        conn = sqlite3.connect(str(db_path))
        conn.execute("UPDATE alembic_version SET version_num = ?", (older,))
        conn.commit()
        conn.close()
        with pytest.raises(SchemaOutOfDate):
            read_config(seeded)                      # precondition: it IS behind
        dispose_all()

        info = backup_database(seeded, out_dir / "b.db")
        assert info.schema_rev == older
        assert copy_facts(out_dir / "b.db")["schema_rev"] == older
        assert dump(out_dir / "b.db") == dump(db_path)

    def test_the_hash_of_a_behind_database_matches_the_file(self, seeded, db_path, out_dir):
        behind = sorted(KNOWN_REVISIONS - {EXPECTED_HEAD})
        if not behind:
            pytest.skip("only one schema revision exists")
        conn = sqlite3.connect(str(db_path))
        conn.execute("UPDATE alembic_version SET version_num = ?", (behind[0],))
        conn.commit()
        conn.close()
        info = backup_database(seeded, out_dir / "b.db")
        assert info.sha256 == sha256(out_dir / "b.db")

    def test_an_unstamped_database_without_global_settings_has_generation_zero(
        self, tmp_path, out_dir
    ):
        raw = tmp_path / "raw" / "config.db"
        raw.parent.mkdir()
        conn = sqlite3.connect(str(raw))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        conn.close()
        assert sql(raw, "PRAGMA user_version") == [(0,)]
        dispose_all()

        info = backup_database(resolve_config_path(str(raw)), out_dir / "b.db")
        assert info.store_id == 0
        assert info.generation == 0
        assert info.schema_rev is None
        assert info.sha256 == sha256(out_dir / "b.db")
        assert sql(out_dir / "b.db", "SELECT x FROM t") == [(1,)]

    def test_a_schema_only_database_has_generation_zero_and_the_head_revision(
        self, tmp_path, out_dir
    ):
        from test_zfsbackup_store_upgrade import make_schema_only

        raw = tmp_path / "schema" / "config.db"
        raw.parent.mkdir()
        conn = sqlite3.connect(str(raw))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE _t (x)")
        conn.execute("DROP TABLE _t")
        conn.commit()
        conn.close()
        resolved = resolve_config_path(str(raw))
        make_schema_only(resolved)
        assert sql(raw, "SELECT count(*) FROM global_settings") == [(0,)]

        info = backup_database(resolved, out_dir / "b.db")
        assert info.generation == 0
        assert info.schema_rev == EXPECTED_HEAD
        assert info.store_id == sql(raw, "PRAGMA user_version")[0][0]

    def test_a_non_sqlite_file_is_refused_and_nothing_is_left(self, tmp_path, out_dir):
        bad = tmp_path / "bad.db"
        bad.write_bytes(b"this is not a database" * 100)
        with pytest.raises(Exception) as excinfo:
            backup_database(resolve_config_path(str(bad)), out_dir / "b.db")
        assert not isinstance(excinfo.value, FileExistsError)
        assert names(out_dir) == []


# --------------------------------------------------------------------------
# The _liveconn guard
# --------------------------------------------------------------------------


def posix_locks(path) -> list:
    ino = os.stat(path).st_ino
    pid = str(os.getpid())
    kinds = []
    for line in Path("/proc/locks").read_text().splitlines():
        f = line.split()
        if len(f) >= 6 and f[1] == "POSIX" and f[4] == pid and f[5].endswith(f":{ino}"):
            kinds.append(f[3])
    return kinds


def fds_on(path) -> int:
    key = (os.stat(path).st_dev, os.stat(path).st_ino)
    n = 0
    for name in os.listdir(FD_DIR):
        try:
            st = os.fstat(int(name))
        except (OSError, ValueError):
            continue
        if (st.st_dev, st.st_ino) == key:
            n += 1
    return n


def run_other_process(db: Path, code: str) -> None:
    subprocess.run(
        [sys.executable, "-c",
         "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]);" + code + "; c.close()", str(db)],
        check=True, capture_output=True, timeout=60, env=hermetic_env(),
    )


@pytest.mark.integration
class TestLiveConnectionGuard:
    """This module never opens or closes the source file itself. A raw
    `close()` on `config.db` drops every POSIX lock this process holds on it
    (the `_liveconn` bug), so a backup must neither park nor leak a descriptor
    nor release a pooled connection's lock."""

    @pytest.fixture(autouse=True)
    def clean_parked(self):
        yield
        dispose_all()
        with _liveconn._LOCK:
            for fds in list(_liveconn._PARKED.values()):
                for fd in fds:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
            _liveconn._PARKED.clear()

    @pytest.mark.skipif(
        not sys.platform.startswith("linux") or not HAS_PROC_LOCKS,
        reason="Linux only: observes POSIX locks through /proc/locks",
    )
    def test_a_pooled_connections_lock_survives_a_backup(self, seeded, db_path, out_dir):
        read_config(seeded)
        if "READ" not in posix_locks(db_path):
            pytest.skip("this kernel does not list the process's own POSIX locks")
        before = posix_locks(db_path)
        backup_database(seeded, out_dir / "b.db")
        assert "READ" in posix_locks(db_path)
        assert posix_locks(db_path) == before

    def test_a_peer_closing_after_a_backup_does_not_delete_the_sidecars(
        self, seeded, db_path, out_dir
    ):
        """While this process stays connected another process is never "the last
        connection": the `-wal`/`-shm` must survive its close."""
        read_config(seeded)
        backup_database(seeded, out_dir / "b.db")
        assert os.path.exists(str(db_path) + "-wal") and os.path.exists(str(db_path) + "-shm")
        run_other_process(db_path, "c.execute('SELECT count(*) FROM global_settings').fetchone()")
        assert os.path.exists(str(db_path) + "-wal") and os.path.exists(str(db_path) + "-shm")
        assert sql(db_path, "PRAGMA integrity_check") == [("ok",)]
        assert read_config(seeded).generation == 0


# --------------------------------------------------------------------------
# The staged copy: nothing at the destination for SQLite to find
# --------------------------------------------------------------------------

FIXED_HEX = "aabbccddeeff"


@pytest.fixture
def fixed_tmp_name(monkeypatch):
    """The temporary name becomes predictable (`.<name>.tmp-aabbccddeeff`), as
    it would be to an attacker who can read the code but not guess 48 bits."""
    monkeypatch.setattr(backup_module.secrets, "token_hex", lambda n=None: FIXED_HEX)


def hostile_wal(donor_dir: Path, source_db: Path) -> bytes:
    """A real `-wal` taken from a copy of `source_db` that has an extra table
    `planted` committed in it (same page size, so any WAL-flagged database of
    that page size would replay it)."""
    donor = donor_dir / "donor.db"
    donor.write_bytes(source_db.read_bytes())
    conn = sqlite3.connect(str(donor))
    try:
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("CREATE TABLE planted (x TEXT)")
        conn.execute("INSERT INTO planted VALUES ('attacker data')")
        conn.commit()
        return Path(str(donor) + "-wal").read_bytes()
    finally:
        conn.close()


@pytest.mark.integration
class TestPlantedSidecars:
    """SQLite resolves `-wal`/`-shm`/`-journal` by NAME and the copy's header
    carries the WAL flag. Had SQLite opened the temporary file, a `<tmp>-wal`
    planted in a shared-writable destination would have been replayed into the
    published backup. SQLite must never open anything at the destination."""

    def test_the_attack_is_real_against_a_sqlite_that_opens_the_file(
        self, seeded, db_path, tmp_path
    ):
        """Precondition (so the regression below is not vacuous): this hostile
        WAL, planted beside a WAL-flagged copy, IS replayed when SQLite opens it."""
        probe_dir = tmp_path / "probe"
        probe_dir.mkdir()
        wal = hostile_wal(tmp_path, db_path)
        probe = probe_dir / "probe.db"
        probe.write_bytes(db_path.read_bytes())
        Path(str(probe) + "-wal").write_bytes(wal)
        assert sql(probe, "SELECT name FROM sqlite_master WHERE name = 'planted'") == [("planted",)]

    @pytest.mark.parametrize("suffixes", [("-wal",), ("-wal", "-shm"), ("-journal",), ("-wal", "-shm", "-journal")])
    def test_a_planted_sidecar_of_the_temporary_name_is_not_replayed(
        self, seeded, db_path, tmp_path, out_dir, fixed_tmp_name, suffixes
    ):
        wal = hostile_wal(tmp_path, db_path)
        planted = []
        for suffix in suffixes:
            path = out_dir / f".b.db.tmp-{FIXED_HEX}{suffix}"
            path.write_bytes(wal if suffix == "-wal" else b"\0" * 32768 if suffix == "-shm" else wal)
            planted.append(path)

        info = backup_database(seeded, out_dir / "b.db")

        assert (out_dir / "b.db").exists()
        assert info.sha256 == sha256(out_dir / "b.db")
        for path in planted:                      # the attacker's files are untouched
            assert path.exists() and path.stat().st_size > 0
            path.unlink()
        assert names(out_dir) == ["b.db"]
        assert sql(out_dir / "b.db", "SELECT name FROM sqlite_master WHERE name = 'planted'") == []
        assert sql(out_dir / "b.db", "PRAGMA integrity_check") == [("ok",)]
        assert dump(out_dir / "b.db") == dump(db_path)

    def test_the_temporary_name_itself_planted_is_refused_not_followed(
        self, seeded, out_dir, tmp_path, fixed_tmp_name
    ):
        """`O_EXCL`: a file (or symlink) already at the temporary name is never
        opened through."""
        victim = tmp_path / "victim"
        (out_dir / f".b.db.tmp-{FIXED_HEX}").symlink_to(victim)
        with pytest.raises(FileExistsError):
            backup_database(seeded, out_dir / "b.db")
        assert not victim.exists() and not (out_dir / "b.db").exists()

    def test_no_sqlite_file_is_ever_created_at_the_destination(
        self, seeded, out_dir, monkeypatch
    ):
        """Whatever the backup does while it runs, the destination directory
        only ever holds the temporary file and the published one."""
        seen = set()
        real_write = backup_module._write_all

        def look(fd, data):
            seen.update(p.name for p in out_dir.iterdir())
            return real_write(fd, data)

        monkeypatch.setattr(backup_module, "_write_all", look)
        backup_database(seeded, out_dir / "b.db")
        assert len(seen) == 1 and next(iter(seen)).startswith(".b.db.tmp-")
        assert names(out_dir) == ["b.db"]


# --------------------------------------------------------------------------
# Refusals and failures: nothing is left under the final name
# --------------------------------------------------------------------------


@pytest.mark.integration
class TestRefusals:
    def test_a_source_that_is_not_in_wal_mode_is_refused(self, seeded, db_path, out_dir):
        conn = sqlite3.connect(str(db_path))
        assert conn.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
        conn.close()
        with pytest.raises(BackupSourceError, match="WAL"):
            backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == []

    def test_a_source_over_the_size_cap_is_refused(self, seeded, out_dir, monkeypatch):
        monkeypatch.setattr(backup_module, "MAX_BACKUP_BYTES", 1000)
        with pytest.raises(BackupSourceError, match="limit"):
            backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == []

    def test_a_source_exactly_at_the_size_cap_is_accepted(self, seeded, db_path, out_dir, monkeypatch):
        size = os.stat(db_path).st_size
        pages = sql(db_path, "PRAGMA page_count")[0][0] * sql(db_path, "PRAGMA page_size")[0][0]
        monkeypatch.setattr(backup_module, "MAX_BACKUP_BYTES", pages)
        backup_database(seeded, out_dir / "b.db")
        assert size > 0 and names(out_dir) == ["b.db"]

    def test_the_cap_and_deadline_are_what_the_contract_says(self):
        assert backup_module.MAX_BACKUP_BYTES == 256 * 1024 * 1024
        assert backup_module.BACKUP_DEADLINE_SECONDS == 30.0

    def test_the_progress_deadline_raises_backup_error(self, seeded, out_dir, monkeypatch):
        monkeypatch.setattr(backup_module, "BACKUP_DEADLINE_SECONDS", -1.0)
        with pytest.raises(BackupBusy, match="did not finish"):
            backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == []

    def test_backup_error_is_a_runtime_error(self):
        assert issubclass(BackupError, RuntimeError)

    def test_the_subclasses_are_distinct_backup_errors_exported_by_the_store(self):
        subclasses = (BackupBusy, BackupSourceError, BackupPublishError)
        for cls in subclasses:
            assert issubclass(cls, BackupError) and cls is not BackupError
            assert cls.__name__ in store_package.__all__
            assert getattr(store_package, cls.__name__) is cls
            assert getattr(backup_module, cls.__name__) is cls
        assert "BackupError" in store_package.__all__
        for a in subclasses:                      # siblings: no one catches another's
            for b in subclasses:
                assert a is b or not issubclass(a, b)

    def test_every_refusal_is_still_caught_as_a_backup_error(
        self, seeded, db_path, out_dir, monkeypatch
    ):
        monkeypatch.setattr(backup_module, "BACKUP_DEADLINE_SECONDS", -1.0)
        with pytest.raises(BackupError):
            backup_database(seeded, out_dir / "b.db")
        monkeypatch.undo()
        monkeypatch.setattr(backup_module, "MAX_BACKUP_BYTES", 1)
        with pytest.raises(BackupError):
            backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == []

    @pytest.mark.parametrize("code", [5, 6, 261, 517, 262])
    def test_a_busy_or_locked_error_mid_copy_is_backup_busy(
        self, seeded, out_dir, monkeypatch, code
    ):
        """SQLITE_BUSY (5) / SQLITE_LOCKED (6) and their extended codes (the low
        byte is the primary code). Mocked: a real BUSY cannot be reached now that
        a non-WAL source is refused."""
        def locked(source, where):
            exc = sqlite3.OperationalError("database is locked")
            exc.sqlite_errorcode = code
            raise exc

        monkeypatch.setattr(backup_module, "_snapshot_to_memory", locked)
        with pytest.raises(BackupBusy) as excinfo:
            backup_database(seeded, out_dir / "b.db")
        assert isinstance(excinfo.value.__cause__, sqlite3.OperationalError)
        assert not isinstance(excinfo.value, BackupSourceError)
        assert names(out_dir) == []

    @pytest.mark.parametrize("code", [1, 10, 11, 14, 266, None])
    def test_any_other_operational_error_mid_copy_is_a_source_error(
        self, seeded, out_dir, monkeypatch, code
    ):
        def broken(source, where):
            exc = sqlite3.OperationalError("disk I/O error")
            if code is not None:
                exc.sqlite_errorcode = code
            raise exc

        monkeypatch.setattr(backup_module, "_snapshot_to_memory", broken)
        with pytest.raises(BackupSourceError) as excinfo:
            backup_database(seeded, out_dir / "b.db")
        assert not isinstance(excinfo.value, BackupBusy)
        assert names(out_dir) == []

    def test_a_failed_quick_check_publishes_nothing(self, seeded, out_dir, monkeypatch):
        real = backup_module._scalar

        def scalar(conn, statement):
            return "*** in database main ***" if statement == "PRAGMA quick_check" else real(conn, statement)

        monkeypatch.setattr(backup_module, "_scalar", scalar)
        with pytest.raises(BackupSourceError, match="quick_check"):
            backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == []

    def test_a_failure_leaves_a_previous_backup_alone(self, seeded, out_dir, monkeypatch):
        first = backup_database(seeded, out_dir / "good.db")
        monkeypatch.setattr(backup_module, "BACKUP_DEADLINE_SECONDS", -1.0)
        with pytest.raises(BackupBusy):
            backup_database(seeded, out_dir / "next.db")
        assert names(out_dir) == ["good.db"]
        assert sha256(out_dir / "good.db") == first.sha256

    def test_a_failure_leaves_the_source_alone(self, seeded, db_path, out_dir, monkeypatch):
        before = dump(db_path)
        monkeypatch.setattr(backup_module, "BACKUP_DEADLINE_SECONDS", -1.0)
        with pytest.raises(BackupBusy):
            backup_database(seeded, out_dir / "b.db")
        dispose_all()
        assert dump(db_path) == before


@pytest.mark.integration
class TestPermissions:
    """Needs write access to the source (a writer-or-root operation) and to the
    destination directory; both are refusals with the store's own error."""

    def test_a_source_directory_that_is_not_writable_is_refused(
        self, seeded, db_path, out_dir, monkeypatch
    ):
        directory = db_path.parent
        real = store_paths._can_access

        def can_access(path, mode):                     # root ignores modes: mock it
            if path == directory and mode == os.W_OK:
                return False
            return real(path, mode)

        monkeypatch.setattr(store_paths, "_can_access", can_access)
        with pytest.raises(ConfigDbPermissionError):
            backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == []

    @pytest.mark.skipif(os.geteuid() == 0, reason="permission checks are meaningless as root")
    def test_a_really_read_only_source_directory_is_refused(self, seeded, db_path, out_dir):
        os.chmod(db_path.parent, 0o555)
        try:
            with pytest.raises(ConfigDbPermissionError):
                backup_database(seeded, out_dir / "b.db")
        finally:
            os.chmod(db_path.parent, 0o755)
        assert names(out_dir) == []

    @pytest.mark.skipif(os.geteuid() == 0, reason="permission checks are meaningless as root")
    def test_a_destination_directory_that_is_not_writable_is_refused(self, seeded, out_dir):
        os.chmod(out_dir, 0o555)
        try:
            with pytest.raises(ConfigDbPermissionError) as excinfo:
                backup_database(seeded, out_dir / "b.db")
        finally:
            os.chmod(out_dir, 0o755)
        assert str(out_dir) in str(excinfo.value)
        assert names(out_dir) == []

    def test_an_unwritable_destination_is_refused_even_where_modes_are_ignored(
        self, seeded, out_dir, monkeypatch
    ):
        real_open = backup_module.os.open

        def deny(path, flags, *args, **kwargs):
            if os.fspath(path).startswith(str(out_dir)) and flags & os.O_EXCL:
                raise PermissionError(errno.EACCES, "Permission denied")
            return real_open(path, flags, *args, **kwargs)

        monkeypatch.setattr(backup_module.os, "open", deny)
        with pytest.raises(ConfigDbPermissionError):
            backup_database(seeded, out_dir / "b.db")
        monkeypatch.undo()
        assert names(out_dir) == []

    def test_a_missing_source_fails_with_a_path_error_and_creates_nothing(self, tmp_path, out_dir):
        missing = resolve_config_path(str(tmp_path / "nowhere" / "config.db"))
        with pytest.raises(store_package.ConfigPathError):
            backup_database(missing, out_dir / "b.db")
        assert names(out_dir) == [] and not (tmp_path / "nowhere").exists()


@pytest.mark.integration
class TestPublish:
    def test_failure_after_link_leaves_neither_name(self, seeded, out_dir, monkeypatch):
        """The link succeeded, the directory fsync did not: a backup that may not
        survive a crash is not published."""
        def failing(directory):
            raise OSError(errno.EIO, "Input/output error")

        monkeypatch.setattr(backup_module, "_fsync_dir", failing)
        with pytest.raises(BackupPublishError, match="durable"):
            backup_database(seeded, out_dir / "b.db")
        assert names(out_dir) == []                 # not the final name, not the temporary one

    def test_failure_after_link_into_a_directory_leaves_nothing(self, seeded, out_dir, monkeypatch):
        monkeypatch.setattr(
            backup_module, "_fsync_dir", lambda d: (_ for _ in ()).throw(OSError(errno.EIO, "x"))
        )
        with pytest.raises(BackupPublishError):
            backup_database(seeded, out_dir)
        assert names(out_dir) == []

    def test_the_directory_is_fsynced_once_on_success(self, seeded, out_dir, monkeypatch):
        calls = []
        real = backup_module._fsync_dir
        monkeypatch.setattr(backup_module, "_fsync_dir", lambda d: (calls.append(Path(d)), real(d))[1])
        backup_database(seeded, out_dir / "b.db")
        assert calls == [out_dir]

    def test_fsync_dir_reports_its_failure(self, tmp_path):
        with pytest.raises(OSError):
            backup_module._fsync_dir(tmp_path / "does-not-exist")

    def test_a_link_that_is_not_permitted_is_a_backup_error_and_leaves_no_file(
        self, seeded, out_dir, monkeypatch
    ):
        def eperm(src, dst, *args, **kwargs):
            raise PermissionError(errno.EPERM, "Operation not permitted")

        monkeypatch.setattr(backup_module.os, "link", eperm)
        with pytest.raises(BackupPublishError, match="hard links"):
            backup_database(seeded, out_dir / "b.db")
        monkeypatch.undo()
        assert names(out_dir) == []

    @pytest.mark.parametrize("code", [errno.EPERM, errno.EXDEV, errno.EMLINK, errno.ENOTSUP, errno.EIO])
    def test_any_link_oserror_is_a_publish_error_not_a_source_error(
        self, seeded, out_dir, monkeypatch, code
    ):
        def failing(src, dst, *args, **kwargs):
            raise OSError(code, os.strerror(code))

        monkeypatch.setattr(backup_module.os, "link", failing)
        with pytest.raises(BackupPublishError) as excinfo:
            backup_database(seeded, out_dir / "b.db")
        monkeypatch.undo()
        assert not isinstance(excinfo.value, (BackupBusy, BackupSourceError))
        assert isinstance(excinfo.value.__cause__, OSError)
        assert names(out_dir) == []

    def test_there_is_no_overwriting_rename_fallback(self, seeded, out_dir, monkeypatch):
        replaced = []
        monkeypatch.setattr(backup_module.os, "link", lambda *a, **k: (_ for _ in ()).throw(OSError(errno.EXDEV, "x")))
        monkeypatch.setattr(backup_module.os, "rename", lambda *a, **k: replaced.append(a))
        monkeypatch.setattr(backup_module.os, "replace", lambda *a, **k: replaced.append(a))
        with pytest.raises(BackupPublishError):
            backup_database(seeded, out_dir / "b.db")
        monkeypatch.undo()
        assert replaced == [] and names(out_dir) == []

    def test_a_destination_that_appears_before_the_link_is_not_clobbered(
        self, seeded, out_dir, monkeypatch
    ):
        dst = out_dir / "b.db"
        real_link = os.link

        def late(src, target, *args, **kwargs):
            Path(target).write_bytes(b"made by someone else")
            return real_link(src, target, *args, **kwargs)

        monkeypatch.setattr(backup_module.os, "link", late)
        with pytest.raises(FileExistsError):
            backup_database(seeded, dst)
        monkeypatch.undo()
        assert dst.read_bytes() == b"made by someone else"
        assert names(out_dir) == ["b.db"]

    def test_the_first_backup_is_byte_identical_after_a_refused_second(self, seeded, out_dir):
        first = backup_database(seeded, out_dir / "b.db")
        before = (out_dir / "b.db").stat()
        with pytest.raises(FileExistsError):
            backup_database(seeded, out_dir / "b.db")
        after = (out_dir / "b.db").stat()
        assert sha256(out_dir / "b.db") == first.sha256
        assert (before.st_ino, before.st_mtime_ns) == (after.st_ino, after.st_mtime_ns)

    @pytest.mark.parametrize("kind", ["loop", "not-a-directory"])
    def test_an_lstat_error_other_than_absent_propagates(self, seeded, out_dir, kind):
        if kind == "loop":
            (out_dir / "loop").symlink_to(out_dir / "loop")
        else:
            (out_dir / "plain").write_bytes(b"x")
        dst = out_dir / ("loop" if kind == "loop" else "plain") / "b.db"
        with pytest.raises(OSError) as excinfo:
            backup_database(seeded, dst)
        assert not isinstance(excinfo.value, (FileExistsError, FileNotFoundError, BackupError))
        assert excinfo.value.errno in (errno.ELOOP, errno.ENOTDIR)
        assert sorted(n for n in names(out_dir)) == sorted(["loop"] if kind == "loop" else ["plain"])

    @pytest.mark.skipif(os.geteuid() == 0, reason="permission checks are meaningless as root")
    def test_an_unsearchable_destination_directory_propagates_eacces(self, seeded, out_dir):
        sub = out_dir / "sub"
        sub.mkdir()
        os.chmod(sub, 0o000)
        try:
            with pytest.raises(PermissionError):
                backup_database(seeded, sub / "b.db")
        finally:
            os.chmod(sub, 0o755)
        assert names(sub) == []

    def test_the_lstat_helper_treats_only_enoent_as_absent(self, out_dir):
        assert backup_module._lstat_or_none(out_dir / "nothing") is None
        (out_dir / "loop").symlink_to(out_dir / "loop")
        assert backup_module._lstat_or_none(out_dir / "loop") is not None  # lstat does not follow
        with pytest.raises(OSError) as excinfo:
            backup_module._lstat_or_none(out_dir / "loop" / "child")
        assert excinfo.value.errno == errno.ELOOP


# --------------------------------------------------------------------------
# Fork
# --------------------------------------------------------------------------


def run_in_child(fn, timeout: int = 60):
    """Fork, run `fn()` in the child (its JSON-able result comes back through a
    pipe), wait for it and return `(exit_code, result)`. A hung child is killed
    by SIGALRM; nothing is left behind."""
    rfd, wfd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child
        code = 1
        try:
            os.close(rfd)
            signal.alarm(timeout)
            os.write(wfd, json.dumps(fn()).encode())
            code = 0
        except BaseException:
            traceback.print_exc()
        finally:
            os._exit(code)
    os.close(wfd)
    try:
        chunks = []
        while True:
            chunk = os.read(rfd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(rfd)
        _, status = os.waitpid(pid, 0)
    data = b"".join(chunks)
    return os.waitstatus_to_exitcode(status), (json.loads(data) if data else None)


@needs_fork
@pytest.mark.integration
class TestFork:
    def test_a_forked_child_can_back_up_while_the_parent_holds_a_pooled_engine(
        self, seeded, db_path, out_dir
    ):
        read_config(seeded)                      # the parent's pooled engine is live
        code, result = run_in_child(
            lambda: list(backup_database(seeded, out_dir / "child.db"))
        )
        assert code == 0 and result is not None
        info = BackupInfo(*result)
        assert info.sha256 == sha256(out_dir / "child.db")
        assert info.generation == 0 and info.schema_rev == EXPECTED_HEAD
        assert copy_facts(out_dir / "child.db")["integrity"] == [("ok",)]

        # ...and the parent's pool, and the source, are undamaged.
        parent = backup_database(seeded, out_dir / "parent.db")
        assert parent.store_id == info.store_id
        assert sql(db_path, "PRAGMA integrity_check") == [("ok",)]
        assert read_config(seeded).generation == 0
        assert names(out_dir) == ["child.db", "parent.db"]

    def test_a_forked_child_refuses_to_overwrite_a_parents_backup(self, seeded, out_dir):
        backup_database(seeded, out_dir / "b.db")

        def attempt():
            try:
                backup_database(seeded, out_dir / "b.db")
            except FileExistsError:
                return "exists"
            return "overwrote"

        code, result = run_in_child(attempt)
        assert code == 0 and result == "exists"
        assert names(out_dir) == ["b.db"]

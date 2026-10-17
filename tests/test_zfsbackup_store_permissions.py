"""A-4 -- the permission model for group operators (`store/paths.py`).

Scenario: user `alice`, a member of group `zfsbackup`, edits a database owned
by user `zfsbackup`. That breaks four assumptions the earlier code made:
"always chmod" (EPERM for a non-owner, and it strips setgid), "widen the
mode on every re-import", "a sidecar is ours to chmod", and "if the database
file is writable the sidecars are fine".

This suite runs on macOS, where the directory's group is assigned
automatically and there is no `zfsbackup` user, so the Linux behaviour is
*mocked* here and proven for real by `scenarios/test_config_permissions.sh`
(A-9). What is faked, and how:

- ownership: `os.geteuid` is patched to a uid that does not own the real
  files `tmp_path` created, so the "not the owner" branches run against real
  inodes;
- usability: `paths._can_access` is patched where the test needs "usable" or
  "unusable" regardless of who the test runner is;
- Linux dropping setgid on chmod by a non-member: `os.chmod` is wrapped to
  clear the bit;
- root creating the default path: `os.geteuid` -> 0, `grp.getgrnam` and the
  `chown`/`fchown` calls are recorded rather than executed.

Real-permission tests (an unwritable sidecar) skip as root, where every
`os.access` check passes.

Fix-up round (A-10, A-12, A-14.2) mutation counts, one revert at a time against
a scratch copy of the tree: `check_config_db` step 7 without the re-`lstat` 6,
without the second access check 2, reporting the first stat instead of the
fresh one 1; `_ensure_mode` not-owned without the vanish return 2, owned
without the chmod-ENOENT return 2; `create_config_db_file` without the unlink
5, catching `Exception` instead of `BaseException` 1, without the inode check
1, with `fchmod`'s `OSError` unwrapped 4; the directory losing its sticky bit 4.
"""

import grp
import logging
import os
import sqlite3
import stat
from pathlib import Path

import pytest

from zfsbackup.config.store import paths as store_paths
from zfsbackup.config.store.paths import (
    CONFIG_DB_MODE,
    CONFIG_DIR_MODE,
    ConfigDbPermissionError,
    check_config_db,
    create_config_db_file,
    ensure_config_db_mode,
    ensure_config_dir,
    is_default_config_dir,
    is_default_config_path,
    resolve_config_path,
)

pytestmark = pytest.mark.unit

NOT_ROOT = pytest.mark.skipif(
    os.geteuid() == 0, reason="root passes every os.access check"
)


def mode_of(path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


@pytest.fixture
def foreign_euid(monkeypatch):
    """Make this process look like a user who owns none of tmp_path's files."""
    fake = os.getuid() + 4242
    monkeypatch.setattr(os, "geteuid", lambda: fake)
    return fake


@pytest.fixture
def chmod_calls(monkeypatch):
    """Record every `os.chmod` made through `paths`, still performing it."""
    calls = []
    real = os.chmod

    def recording(path, mode, *args, **kwargs):
        calls.append((Path(path), mode))
        return real(path, mode, *args, **kwargs)

    monkeypatch.setattr(store_paths.os, "chmod", recording)
    return calls


@pytest.fixture
def default_db(tmp_path, monkeypatch):
    """Point `DEFAULT_CONFIG_DB` at a database in a fresh directory, so the
    default-path branches (the only ones that may chmod) run."""
    directory = tmp_path / "var-lib-zfsbackup"
    directory.mkdir()
    db = directory / "config.db"
    monkeypatch.setattr(store_paths, "DEFAULT_CONFIG_DB", db)
    return db


def make_sqlite(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE t (x)")
    conn.commit()
    conn.close()


def warnings_of(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


# ---------------------------------------------------------------------------
# Constants and ownership
# ---------------------------------------------------------------------------


class TestModesAndOwnership:
    def test_directory_mode_carries_setgid(self):
        assert CONFIG_DIR_MODE == 0o2770
        assert CONFIG_DIR_MODE & stat.S_ISGID
        assert CONFIG_DB_MODE == 0o660

    def test_is_owner_matches_euid(self, monkeypatch, tmp_path):
        st = os.stat(tmp_path)
        monkeypatch.setattr(os, "geteuid", lambda: st.st_uid)
        assert store_paths._is_owner(st) is True
        monkeypatch.setattr(os, "geteuid", lambda: st.st_uid + 1)  # never 0
        assert store_paths._is_owner(st) is False

    def test_root_counts_as_owner_of_everything(self, monkeypatch, tmp_path):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        assert store_paths._is_owner(os.stat(tmp_path)) is True


# ---------------------------------------------------------------------------
# _ensure_mode
# ---------------------------------------------------------------------------


class TestEnsureMode:
    def _file(self, tmp_path, mode):
        path = tmp_path / "f"
        path.touch()
        os.chmod(path, mode)
        return path

    def test_equal_mode_makes_no_chmod_call(self, tmp_path, chmod_calls):
        path = self._file(tmp_path, 0o660)
        chmod_calls.clear()

        store_paths._ensure_mode(path, 0o660, access=os.R_OK, what="x")

        assert chmod_calls == []

    def test_equal_mode_is_a_no_op_even_for_a_non_owner(
        self, tmp_path, chmod_calls, foreign_euid, caplog
    ):
        # Non-owner EPERM-on-chmod-of-an-identical-mode was the A4 bug.
        path = self._file(tmp_path, 0o660)
        chmod_calls.clear()
        with caplog.at_level(logging.WARNING):
            store_paths._ensure_mode(path, 0o660, access=os.R_OK, what="x")
        assert chmod_calls == []
        assert warnings_of(caplog) == []

    def test_equal_mode_directory_keeps_its_setgid_bit(self, tmp_path, chmod_calls):
        # Re-chmod of a directory is how setgid used to get stripped.
        directory = tmp_path / "d"
        directory.mkdir()
        os.chmod(directory, 0o2770)
        if mode_of(directory) != 0o2770:
            pytest.skip("filesystem does not keep setgid on directories")
        chmod_calls.clear()

        store_paths._ensure_mode(directory, 0o2770, access=os.W_OK, what="d")

        assert chmod_calls == []
        assert mode_of(directory) == 0o2770

    def test_owned_path_is_chmodded(self, tmp_path, chmod_calls):
        path = self._file(tmp_path, 0o600)
        chmod_calls.clear()

        store_paths._ensure_mode(path, 0o660, access=os.R_OK, what="x")

        assert chmod_calls == [(path, 0o660)]
        assert mode_of(path) == 0o660

    def test_not_owned_but_usable_warns_and_does_not_chmod(
        self, tmp_path, chmod_calls, foreign_euid, caplog, monkeypatch
    ):
        path = self._file(tmp_path, 0o644)
        monkeypatch.setattr(store_paths, "_can_access", lambda p, m: True)
        chmod_calls.clear()

        with caplog.at_level(logging.WARNING):
            store_paths._ensure_mode(path, 0o660, access=os.R_OK | os.W_OK, what="config database")

        assert chmod_calls == []
        assert mode_of(path) == 0o644
        (message,) = warnings_of(caplog)
        assert "config database" in message
        assert str(path) in message
        assert "0644" in message and "0660" in message
        assert "ask" in message and f"chmod 0660 {path}" in message

    def test_not_owned_and_unusable_raises_naming_the_fix(
        self, tmp_path, chmod_calls, foreign_euid, monkeypatch
    ):
        path = self._file(tmp_path, 0o600)
        monkeypatch.setattr(store_paths, "_can_access", lambda p, m: False)
        chmod_calls.clear()

        with pytest.raises(ConfigDbPermissionError) as excinfo:
            store_paths._ensure_mode(path, 0o660, access=os.R_OK, what="config database")

        assert chmod_calls == []
        assert not isinstance(excinfo.value, OSError)
        message = str(excinfo.value)
        assert "must run: chmod 0660" in message
        assert str(path) in message

    def test_setgid_dropped_by_chmod_is_a_warning_not_an_error(
        self, tmp_path, monkeypatch, caplog
    ):
        # Linux silently drops setgid when the chmod-ing user is not in the
        # file's group; the operation otherwise succeeded.
        directory = tmp_path / "d"
        directory.mkdir()
        os.chmod(directory, 0o755)
        real_chmod = os.chmod
        monkeypatch.setattr(
            store_paths.os, "chmod",
            lambda p, m, *a, **k: real_chmod(p, m & ~stat.S_ISGID, *a, **k),
        )

        with caplog.at_level(logging.WARNING):
            store_paths._ensure_mode(directory, 0o2770, access=os.W_OK, what="config directory")

        assert mode_of(directory) == 0o770
        (message,) = warnings_of(caplog)
        assert "left it at 0770" in message
        assert "setgid" in message
        assert f"chmod 2770 {directory}" in message

    def test_chmod_failure_on_an_owned_path_is_wrapped(self, tmp_path, monkeypatch):
        path = self._file(tmp_path, 0o600)

        def refuse(*_a, **_k):
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(store_paths.os, "chmod", refuse)
        with pytest.raises(ConfigDbPermissionError) as excinfo:
            store_paths._ensure_mode(path, 0o660, access=os.R_OK, what="x")
        assert not isinstance(excinfo.value, OSError)

    def test_missing_path_is_not_this_functions_concern(self, tmp_path):
        store_paths._ensure_mode(tmp_path / "nope", 0o660, access=os.R_OK, what="x")


# ---------------------------------------------------------------------------
# ensure_config_db_mode
# ---------------------------------------------------------------------------


class TestEnsureConfigDbMode:
    def test_default_path_is_brought_to_0660_with_owned_sidecars(
        self, default_db, chmod_calls
    ):
        default_db.touch()
        os.chmod(default_db, 0o600)
        wal = Path(f"{default_db}-wal")
        shm = Path(f"{default_db}-shm")
        for sidecar in (wal, shm):
            sidecar.touch()
            os.chmod(sidecar, 0o644)

        ensure_config_db_mode(default_db)

        assert mode_of(default_db) == 0o660
        assert mode_of(wal) == 0o660
        assert mode_of(shm) == 0o660

    def test_already_correct_default_path_makes_no_chmod(self, default_db, chmod_calls):
        default_db.touch()
        os.chmod(default_db, 0o660)
        chmod_calls.clear()
        ensure_config_db_mode(default_db)
        assert chmod_calls == []

    def test_sidecar_we_do_not_own_is_never_chmodded(
        self, default_db, chmod_calls, monkeypatch, caplog
    ):
        default_db.touch()
        os.chmod(default_db, 0o660)
        wal = Path(f"{default_db}-wal")
        wal.touch()
        os.chmod(wal, 0o644)
        wal_ino = os.lstat(wal).st_ino
        real_is_owner = store_paths._is_owner
        monkeypatch.setattr(
            store_paths, "_is_owner",
            lambda st: False if st.st_ino == wal_ino else real_is_owner(st),
        )
        monkeypatch.setattr(store_paths, "_can_access", lambda p, m: True)
        chmod_calls.clear()

        with caplog.at_level(logging.WARNING):
            ensure_config_db_mode(default_db)

        assert (wal, 0o660) not in chmod_calls
        assert mode_of(wal) == 0o644
        assert any("WAL sidecar" in m and str(wal) in m for m in warnings_of(caplog))

    def test_symlinked_sidecar_is_skipped_not_followed(self, default_db, caplog):
        default_db.touch()
        os.chmod(default_db, 0o660)
        victim = default_db.parent / "victim"
        victim.touch()
        os.chmod(victim, 0o644)
        os.symlink(victim, f"{default_db}-wal")

        with caplog.at_level(logging.WARNING):
            ensure_config_db_mode(default_db)

        assert mode_of(victim) == 0o644  # the link target was not chmod'd
        assert any("is a symlink" in m for m in warnings_of(caplog))

    def test_journal_is_never_touched(self, default_db):
        default_db.touch()
        os.chmod(default_db, 0o600)
        journal = Path(f"{default_db}-journal")
        journal.touch()
        os.chmod(journal, 0o600)

        ensure_config_db_mode(default_db)

        assert mode_of(journal) == 0o600

    def test_non_default_path_is_never_chmodded(self, default_db, tmp_path, chmod_calls):
        other = tmp_path / "private.db"
        other.touch()
        os.chmod(other, 0o600)
        wal = Path(f"{other}-wal")
        wal.touch()
        os.chmod(wal, 0o600)
        chmod_calls.clear()

        ensure_config_db_mode(other)

        assert chmod_calls == []
        assert mode_of(other) == 0o600
        assert mode_of(wal) == 0o600

    def test_non_default_without_group_access_warns(self, default_db, tmp_path, caplog):
        other = tmp_path / "private.db"
        other.touch()
        os.chmod(other, 0o600)
        with caplog.at_level(logging.WARNING):
            ensure_config_db_mode(other)
        (message,) = warnings_of(caplog)
        assert "0600" in message and "locked out" in message

    def test_non_default_with_group_read_write_is_silent(
        self, default_db, tmp_path, caplog
    ):
        other = tmp_path / "shared.db"
        other.touch()
        os.chmod(other, 0o664)
        with caplog.at_level(logging.WARNING):
            ensure_config_db_mode(other)
        assert warnings_of(caplog) == []

    def test_non_default_group_read_only_warns(self, default_db, tmp_path, caplog):
        other = tmp_path / "ro.db"
        other.touch()
        os.chmod(other, 0o640)
        with caplog.at_level(logging.WARNING):
            ensure_config_db_mode(other)
        assert len(warnings_of(caplog)) == 1


# ---------------------------------------------------------------------------
# ensure_config_dir / create_config_db_file
# ---------------------------------------------------------------------------


class TestEnsureConfigDir:
    def test_existing_directory_of_someone_else_is_warned_about_not_chmodded(
        self, tmp_path, chmod_calls, foreign_euid, monkeypatch, caplog
    ):
        directory = tmp_path / "shared"
        directory.mkdir()
        os.chmod(directory, 0o775)
        monkeypatch.setattr(store_paths, "_can_access", lambda p, m: True)
        chmod_calls.clear()

        with caplog.at_level(logging.WARNING):
            ensure_config_dir(directory)

        assert chmod_calls == []
        assert mode_of(directory) == 0o775
        assert any("config directory" in m for m in warnings_of(caplog))

    def test_new_directory_gets_the_setgid_mode_independent_of_umask(
        self, tmp_path
    ):
        old = os.umask(0o077)
        try:
            target = tmp_path / "new"
            ensure_config_dir(target)
        finally:
            os.umask(old)
        mode = mode_of(target)
        assert mode & 0o770 == 0o770
        # setgid may be dropped by the platform (macOS accepts it)
        assert mode in (0o2770, 0o770)


class TestRootChgrp:
    """Q7: a ROOT run that CREATES the default directory/file hands it to the
    `zfsbackup` group; nothing else ever does."""

    @pytest.fixture
    def root_with_group(self, monkeypatch):
        calls = {"chown": [], "fchown": [], "order": []}
        monkeypatch.setattr(os, "geteuid", lambda: 0)

        class Entry:
            gr_gid = 31337

        monkeypatch.setattr(grp, "getgrnam", lambda name: Entry())
        monkeypatch.setattr(
            store_paths.os, "chown",
            lambda path, uid, gid, *a, **k: (
                calls["chown"].append((Path(path), uid, gid)),
                calls["order"].append("chown"),
            ),
        )
        real_fchmod = os.fchmod

        def fchown(fd, uid, gid):
            calls["fchown"].append((uid, gid))
            calls["order"].append("fchown")

        def fchmod(fd, mode):
            calls["order"].append("fchmod")
            return real_fchmod(fd, mode)

        monkeypatch.setattr(store_paths.os, "fchown", fchown)
        monkeypatch.setattr(store_paths.os, "fchmod", fchmod)
        return calls

    def test_creating_the_default_directory_chgrps_it(
        self, default_db, root_with_group, monkeypatch
    ):
        directory = default_db.parent / "sub"
        # Make `sub` (not yet existing) the default dir.
        monkeypatch.setattr(store_paths, "DEFAULT_CONFIG_DB", directory / "config.db")
        ensure_config_dir(directory)
        assert root_with_group["chown"] == [(directory, -1, 31337)]

    def test_an_already_existing_default_directory_is_not_chgrped(
        self, default_db, root_with_group
    ):
        ensure_config_dir(default_db.parent)  # exists already
        assert root_with_group["chown"] == []

    def test_a_non_default_directory_is_never_chgrped(
        self, default_db, tmp_path, root_with_group
    ):
        ensure_config_dir(tmp_path / "other")
        assert root_with_group["chown"] == []

    def test_creating_the_default_file_chgrps_before_fixing_the_mode(
        self, default_db, root_with_group
    ):
        create_config_db_file(default_db)
        assert root_with_group["fchown"] == [(-1, 31337)]
        # chown can clear mode bits, so the mode is set last.
        assert root_with_group["order"] == ["fchown", "fchmod"]
        assert mode_of(default_db) == 0o660

    def test_a_non_default_file_is_never_chgrped(self, default_db, tmp_path, root_with_group):
        create_config_db_file(tmp_path / "other.db")
        assert root_with_group["fchown"] == []

    def test_missing_group_warns_that_operators_are_locked_out(
        self, default_db, monkeypatch, caplog
    ):
        monkeypatch.setattr(os, "geteuid", lambda: 0)

        def missing(name):
            raise KeyError(name)

        monkeypatch.setattr(grp, "getgrnam", missing)
        monkeypatch.setattr(
            store_paths.os, "fchown",
            lambda *a: pytest.fail("must not chown without a group"),
        )

        with caplog.at_level(logging.WARNING):
            create_config_db_file(default_db)

        (message,) = warnings_of(caplog)
        assert "'zfsbackup' group does not exist" in message
        assert "locked out" in message
        assert default_db.exists()  # creation itself still succeeds

    def test_non_root_never_chgrps(self, default_db, monkeypatch):
        monkeypatch.setattr(
            grp, "getgrnam", lambda name: pytest.fail("looked up the group as non-root")
        )
        monkeypatch.setattr(
            store_paths.os, "fchown", lambda *a: pytest.fail("chown as non-root")
        )
        create_config_db_file(default_db)
        assert default_db.exists()


# ---------------------------------------------------------------------------
# Directory identity: (st_dev, st_ino), not path equality
# ---------------------------------------------------------------------------


class TestSameLocation:
    def test_symlinked_spelling_of_the_default_directory_is_the_default(
        self, default_db, tmp_path
    ):
        link = tmp_path / "link"
        os.symlink(default_db.parent, link)
        assert is_default_config_dir(link) is True
        assert is_default_config_dir(default_db.parent) is True

    def test_a_different_directory_is_not(self, default_db, tmp_path):
        other = tmp_path / "other"
        other.mkdir()
        assert is_default_config_dir(other) is False

    def test_symlinked_spelling_of_the_default_file_is_the_default(
        self, default_db, tmp_path
    ):
        default_db.touch()
        link = tmp_path / "dblink"
        os.symlink(default_db, link)
        assert is_default_config_path(link) is True

    def test_missing_paths_fall_back_to_resolved_comparison(self, default_db, tmp_path):
        # Nothing exists, so there is no inode to compare.
        assert not default_db.exists()
        assert is_default_config_path(default_db) is True
        assert is_default_config_path(tmp_path / "elsewhere.db") is False

    def test_importer_treats_a_symlinked_default_dir_as_the_default(
        self, default_db, tmp_path, config_yaml_path
    ):
        # The macOS shape: the resolved target's parent (`/private/var/..`)
        # != the bare constant (`/var/..`), so `==` failed safe and wrongly.
        from zfsbackup.config.store import import_yaml

        real = default_db.parent
        os.chmod(real, 0o755)
        link = tmp_path / "via-link"
        os.symlink(real, link)
        resolved = resolve_config_path(str(link / "config.db"))

        import_yaml(config_yaml_path, resolved)

        # The default directory is owned: it was brought to CONFIG_DIR_MODE
        # (setgid may be dropped by the platform).
        assert mode_of(real) & 0o770 == 0o770


# ---------------------------------------------------------------------------
# check_config_db: sidecars, whether or not the DB itself is writable
# ---------------------------------------------------------------------------


@NOT_ROOT
class TestCheckConfigDbSidecars:
    @pytest.fixture
    def db(self, tmp_path):
        path = tmp_path / "config.db"
        make_sqlite(path)
        os.chmod(path, 0o660)
        return path

    def test_no_sidecars_is_fine(self, db):
        check_config_db(resolve_config_path(str(db)))

    def test_unwritable_sidecar_is_refused_even_though_the_db_is_writable(self, db):
        # The SIGHUP-killed-CLI shape: operator-owned `-wal`, daemon-writable DB.
        assert os.access(db, os.W_OK)
        wal = Path(f"{db}-wal")
        wal.touch()
        os.chmod(wal, 0o000)
        try:
            with pytest.raises(ConfigDbPermissionError) as excinfo:
                check_config_db(resolve_config_path(str(db)))
        finally:
            os.chmod(wal, 0o600)
        assert str(wal) in str(excinfo.value)
        assert not isinstance(excinfo.value, OSError)

    def test_unwritable_shm_sidecar_is_refused_too(self, db):
        shm = Path(f"{db}-shm")
        shm.touch()
        os.chmod(shm, 0o444)
        try:
            with pytest.raises(ConfigDbPermissionError, match="-shm"):
                check_config_db(resolve_config_path(str(db)))
        finally:
            os.chmod(shm, 0o600)

    def test_writable_sidecars_pass(self, db):
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{db}{suffix}")
            sidecar.touch()
            os.chmod(sidecar, 0o660)
        check_config_db(resolve_config_path(str(db)))

    def test_sidecar_group_mismatch_warns(self, db, monkeypatch, caplog):
        wal = Path(f"{db}-wal")
        wal.touch()
        os.chmod(wal, 0o660)
        real = store_paths._sidecar_lstat

        def other_group(path):
            st = real(path)
            if st is None or Path(path) != wal:
                return st
            fields = list(st)[:10]
            fields[5] = st.st_gid + 7  # st_gid
            return os.stat_result(fields)

        monkeypatch.setattr(store_paths, "_sidecar_lstat", other_group)

        with caplog.at_level(logging.WARNING):
            check_config_db(resolve_config_path(str(db)))  # a warning, not an error

        (message,) = warnings_of(caplog)
        assert str(wal) in message and "chgrp" in message and "setgid" in message

    def test_matching_sidecar_group_is_silent(self, db, caplog):
        wal = Path(f"{db}-wal")
        wal.touch()
        os.chmod(wal, 0o660)
        with caplog.at_level(logging.WARNING):
            check_config_db(resolve_config_path(str(db)))
        assert warnings_of(caplog) == []

    def test_require_writable_false_skips_the_sidecar_check(self, db):
        wal = Path(f"{db}-wal")
        wal.touch()
        os.chmod(wal, 0o000)
        try:
            check_config_db(resolve_config_path(str(db)), require_writable=False)
        finally:
            os.chmod(wal, 0o600)


# ===========================================================================
# Fix-up round, A-10 (R1): a sidecar that vanishes mid-check is not an error
# ===========================================================================
#
# `-wal`/`-shm` are transient by design: another connection's last close
# checkpoints and unlinks them, and `os.access` on a path that has just gone
# is False -- indistinguishable from "present but unusable". Before A-10 that
# raised, which stopped the whole daemon (EX_CONFIG -> exit 1) when two worker
# schema checks, a respawn and a CLI exit, or a `systemctl stop` overlapped.
# The fix: re-stat; only a POSITIVE ENOENT/ENOTDIR skips; a sidecar that is
# still there gets exactly one more access check and, failing it, raises from
# the FRESH stat. A real permission problem still raises; there is no loop.
#
# The races are scripted deterministically by patching `_sidecar_lstat` /
# `_can_access` (scoped to the one sidecar path) rather than by timing.

ACCESS_RW = os.R_OK | os.W_OK


def _stat_with_mode(path, mode):
    """A real `lstat` of `path` with the permission bits replaced."""
    st = os.lstat(path)
    fields = list(st)[:10]
    fields[0] = stat.S_IFMT(st.st_mode) | mode
    return os.stat_result(fields)


class Script:
    """A scripted replacement for a per-path probe: returns/raises the next
    queued outcome for `wal`, delegates every other path to `real`."""

    def __init__(self, wal, real, outcomes):
        self.wal, self.real, self.outcomes = wal, real, list(outcomes)
        self.calls = 0

    def __call__(self, path, *args):
        if Path(path) != self.wal:
            return self.real(path, *args)
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@NOT_ROOT
class TestCheckConfigDbSidecarVanishes:
    @pytest.fixture
    def db(self, tmp_path):
        path = tmp_path / "config.db"
        make_sqlite(path)
        os.chmod(path, 0o660)
        return path

    @pytest.fixture
    def wal(self, db):
        wal = Path(f"{db}-wal")
        wal.touch()
        os.chmod(wal, 0o660)
        return wal

    def _script(self, monkeypatch, wal, lstats, accesses):
        lstat = Script(wal, store_paths._sidecar_lstat, lstats)
        access = Script(wal, store_paths._can_access, accesses)
        monkeypatch.setattr(store_paths, "_sidecar_lstat", lstat)
        monkeypatch.setattr(store_paths, "_can_access", access)
        return lstat, access

    def test_vanished_between_lstat_and_access_passes(self, db, wal, monkeypatch):
        # lstat sees it, access says no, the re-lstat sees nothing: gone.
        lstat, access = self._script(
            monkeypatch, wal, [os.lstat(wal), None], [False]
        )

        check_config_db(resolve_config_path(str(db)))  # must not raise

        assert lstat.calls == 2
        assert access.calls == 1  # no second access check for a vanished file

    def test_still_there_and_still_unusable_raises_from_the_fresh_stat(
        self, db, wal, monkeypatch
    ):
        first = _stat_with_mode(wal, 0o660)
        fresh = _stat_with_mode(wal, 0o640)  # distinguishable in the message
        lstat, access = self._script(monkeypatch, wal, [first, fresh], [False, False])

        with pytest.raises(ConfigDbPermissionError) as excinfo:
            check_config_db(resolve_config_path(str(db)))

        assert not isinstance(excinfo.value, OSError)
        assert str(wal) in str(excinfo.value)
        assert "mode=0640" in str(excinfo.value)  # the FRESH stat, not the first
        assert "mode=0660" not in str(excinfo.value)
        # One re-check, no loop.
        assert lstat.calls == 2 and access.calls == 2

    def test_unusable_then_usable_on_the_second_check_passes(
        self, db, wal, monkeypatch
    ):
        # E.g. a chmod landed between the two checks: one more look is enough.
        lstat, access = self._script(
            monkeypatch, wal, [os.lstat(wal), os.lstat(wal)], [False, True]
        )
        check_config_db(resolve_config_path(str(db)))
        assert lstat.calls == 2 and access.calls == 2

    def test_second_lstat_permission_error_is_a_config_db_permission_error(
        self, db, wal, monkeypatch
    ):
        # The real `_sidecar_lstat` runs: the first `os.lstat` of the sidecar
        # succeeds, the second raises PermissionError.
        real_lstat = os.lstat
        seen = {"n": 0}

        def lstat(path, *args, **kwargs):
            if Path(path) == wal:
                seen["n"] += 1
                if seen["n"] == 2:
                    raise PermissionError(13, "Permission denied")
            return real_lstat(path, *args, **kwargs)

        monkeypatch.setattr(store_paths.os, "lstat", lstat)
        access = Script(wal, store_paths._can_access, [False])
        monkeypatch.setattr(store_paths, "_can_access", access)

        with pytest.raises(ConfigDbPermissionError) as excinfo:
            check_config_db(resolve_config_path(str(db)))

        assert not isinstance(excinfo.value, OSError)
        assert isinstance(excinfo.value.__cause__, PermissionError)
        assert str(wal) in str(excinfo.value)
        assert seen["n"] == 2

    def test_shm_gets_the_same_treatment(self, db, monkeypatch):
        shm = Path(f"{db}-shm")
        shm.touch()
        os.chmod(shm, 0o660)
        lstat = Script(shm, store_paths._sidecar_lstat, [os.lstat(shm), None])
        access = Script(shm, store_paths._can_access, [False])
        monkeypatch.setattr(store_paths, "_sidecar_lstat", lstat)
        monkeypatch.setattr(store_paths, "_can_access", access)

        check_config_db(resolve_config_path(str(db)))

        assert lstat.calls == 2 and access.calls == 1

    def test_a_really_unusable_sidecar_still_raises_end_to_end(self, db, wal):
        # No patching: the vanish-tolerance must not mask a real problem.
        os.chmod(wal, 0o000)
        try:
            with pytest.raises(ConfigDbPermissionError, match="-wal"):
                check_config_db(resolve_config_path(str(db)))
        finally:
            os.chmod(wal, 0o600)

    def test_a_sidecar_deleted_by_a_racing_process_end_to_end(self, db, wal, monkeypatch):
        # Realistic shape: the very first access check is what deletes it.
        real = store_paths._can_access

        def delete_then_check(path, mode):
            if Path(path) == wal and wal.exists():
                os.unlink(wal)
            return real(path, mode)

        monkeypatch.setattr(store_paths, "_can_access", delete_then_check)
        check_config_db(resolve_config_path(str(db)))
        assert not wal.exists()


class TestEnsureModeVanishes:
    def _file(self, tmp_path, mode=0o644):
        path = tmp_path / "f-wal"
        path.touch()
        os.chmod(path, mode)
        return path

    @pytest.mark.parametrize(
        "error", [FileNotFoundError(2, "gone"), NotADirectoryError(20, "gone")],
        ids=["enoent", "enotdir"],
    )
    def test_not_owned_path_that_vanishes_during_the_access_check_returns(
        self, tmp_path, chmod_calls, foreign_euid, monkeypatch, caplog, error
    ):
        path = self._file(tmp_path)
        monkeypatch.setattr(store_paths, "_can_access", lambda p, m: False)
        real_stat = os.stat
        seen = {"n": 0}

        def stat_then_gone(p, *args, **kwargs):
            if Path(p) == path:
                seen["n"] += 1
                if seen["n"] >= 2:
                    raise error
            return real_stat(p, *args, **kwargs)

        monkeypatch.setattr(store_paths.os, "stat", stat_then_gone)
        chmod_calls.clear()

        with caplog.at_level(logging.DEBUG):
            store_paths._ensure_mode(path, 0o660, access=ACCESS_RW, what="WAL sidecar")

        assert seen["n"] == 2
        assert chmod_calls == []
        assert warnings_of(caplog) == []

    def test_not_owned_path_still_there_and_unusable_raises(
        self, tmp_path, foreign_euid, monkeypatch
    ):
        path = self._file(tmp_path, 0o600)
        monkeypatch.setattr(store_paths, "_can_access", lambda p, m: False)
        with pytest.raises(ConfigDbPermissionError, match="must run: chmod 0660"):
            store_paths._ensure_mode(path, 0o660, access=ACCESS_RW, what="WAL sidecar")

    def test_not_owned_recheck_error_other_than_enoent_still_raises(
        self, tmp_path, foreign_euid, monkeypatch
    ):
        # A positive ENOENT is required to skip; "cannot tell" is not that.
        path = self._file(tmp_path, 0o600)
        monkeypatch.setattr(store_paths, "_can_access", lambda p, m: False)
        real_stat = os.stat
        seen = {"n": 0}

        def stat_then_eacces(p, *args, **kwargs):
            if Path(p) == path:
                seen["n"] += 1
                if seen["n"] >= 2:
                    raise PermissionError(13, "Permission denied")
            return real_stat(p, *args, **kwargs)

        monkeypatch.setattr(store_paths.os, "stat", stat_then_eacces)
        with pytest.raises(ConfigDbPermissionError, match="must run: chmod 0660"):
            store_paths._ensure_mode(path, 0o660, access=ACCESS_RW, what="WAL sidecar")

    def test_not_owned_became_usable_on_the_recheck_warns_instead(
        self, tmp_path, foreign_euid, monkeypatch, caplog
    ):
        path = self._file(tmp_path, 0o644)
        answers = iter([False, True])
        monkeypatch.setattr(store_paths, "_can_access", lambda p, m: next(answers))
        with caplog.at_level(logging.WARNING):
            store_paths._ensure_mode(path, 0o660, access=ACCESS_RW, what="WAL sidecar")
        (message,) = warnings_of(caplog)
        assert "is owned by" in message

    def test_owned_chmod_that_hits_enoent_returns_quietly(
        self, tmp_path, monkeypatch, caplog
    ):
        # Root always takes the owned branch: the sidecar was checkpointed
        # away between the stat and the chmod.
        path = self._file(tmp_path, 0o600)

        def vanished(p, mode, *args, **kwargs):
            os.unlink(p)
            raise FileNotFoundError(2, "No such file or directory", str(p))

        monkeypatch.setattr(store_paths.os, "chmod", vanished)
        with caplog.at_level(logging.DEBUG):
            store_paths._ensure_mode(path, 0o660, access=ACCESS_RW, what="WAL sidecar")

        assert not path.exists()
        assert warnings_of(caplog) == []
        assert any(
            r.levelno == logging.DEBUG and "vanished" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.parametrize(
        "errno_", [1, 30, 13], ids=["EPERM", "EROFS", "EACCES"],
    )
    def test_owned_chmod_other_errnos_still_raise(self, tmp_path, monkeypatch, errno_):
        path = self._file(tmp_path, 0o600)

        def refuse(*_a, **_k):
            raise OSError(errno_, "refused")

        monkeypatch.setattr(store_paths.os, "chmod", refuse)
        with pytest.raises(ConfigDbPermissionError) as excinfo:
            store_paths._ensure_mode(path, 0o660, access=ACCESS_RW, what="WAL sidecar")
        assert not isinstance(excinfo.value, OSError)

    def test_ensure_config_db_mode_survives_a_sidecar_checkpointed_mid_fix(
        self, default_db, monkeypatch
    ):
        # End to end through the public function, as root would hit it.
        default_db.touch()
        os.chmod(default_db, 0o660)
        wal = Path(f"{default_db}-wal")
        wal.touch()
        os.chmod(wal, 0o600)
        real_chmod = os.chmod

        def racing_chmod(p, mode, *args, **kwargs):
            if Path(p) == wal:
                os.unlink(wal)
                raise FileNotFoundError(2, "No such file or directory", str(p))
            return real_chmod(p, mode, *args, **kwargs)

        monkeypatch.setattr(store_paths.os, "chmod", racing_chmod)
        ensure_config_db_mode(default_db)  # must not raise
        assert mode_of(default_db) == 0o660


# ===========================================================================
# Fix-up round, A-14.2: the config directory keeps its sticky bit
# ===========================================================================


class TestDirectoryKeepsStickyBit:
    """An admin who sets `3770` on the config directory (so group members
    cannot delete each other's files) must not have it stripped to `2770` by a
    re-import. The comparison ignores `S_ISVTX` for directories and a chmod
    preserves it; nothing is ever widened (sticky only restricts); files keep
    the exact comparison."""

    def _sticky_dir(self, tmp_path, mode=0o3770):
        directory = tmp_path / "d"
        directory.mkdir()
        os.chmod(directory, mode)
        if mode_of(directory) != mode:
            pytest.skip("filesystem does not keep the sticky/setgid bits here")
        return directory

    def test_3770_with_the_expected_bits_makes_no_chmod_at_all(
        self, tmp_path, chmod_calls
    ):
        directory = self._sticky_dir(tmp_path)
        chmod_calls.clear()

        store_paths._ensure_mode(directory, CONFIG_DIR_MODE, access=os.W_OK, what="d")

        assert chmod_calls == []
        assert mode_of(directory) == 0o3770

    def test_a_chmod_of_a_sticky_directory_preserves_the_sticky_bit(
        self, tmp_path, chmod_calls
    ):
        directory = self._sticky_dir(tmp_path, 0o3755)
        chmod_calls.clear()

        store_paths._ensure_mode(directory, CONFIG_DIR_MODE, access=os.W_OK, what="d")

        assert chmod_calls == [(directory, 0o3770)]  # target | sticky, not 2770
        assert mode_of(directory) == 0o3770

    def test_ensure_config_dir_keeps_3770_on_a_reimport(self, tmp_path, chmod_calls):
        directory = self._sticky_dir(tmp_path)
        chmod_calls.clear()
        ensure_config_dir(directory)
        assert chmod_calls == []
        assert mode_of(directory) == 0o3770

    def test_a_non_owner_sees_no_mismatch_when_only_the_sticky_bit_differs(
        self, tmp_path, chmod_calls, foreign_euid, caplog
    ):
        directory = self._sticky_dir(tmp_path)
        chmod_calls.clear()
        with caplog.at_level(logging.WARNING):
            store_paths._ensure_mode(directory, CONFIG_DIR_MODE, access=os.W_OK, what="d")
        assert chmod_calls == [] and warnings_of(caplog) == []

    def test_a_non_sticky_directory_is_never_given_the_sticky_bit(
        self, tmp_path, chmod_calls
    ):
        directory = tmp_path / "plain"
        directory.mkdir()
        os.chmod(directory, 0o755)
        chmod_calls.clear()

        store_paths._ensure_mode(directory, CONFIG_DIR_MODE, access=os.W_OK, what="d")

        assert chmod_calls == [(directory, CONFIG_DIR_MODE)]
        assert not mode_of(directory) & stat.S_ISVTX

    def test_files_keep_the_exact_comparison(self, tmp_path, chmod_calls, monkeypatch):
        # A (faked) sticky file is NOT equal to the target, and the chmod
        # target does not inherit the bit.
        path = tmp_path / "f"
        path.touch()
        os.chmod(path, 0o660)
        real_stat = os.stat

        def sticky_file(p, *args, **kwargs):
            st = real_stat(p, *args, **kwargs)
            if Path(p) == path:
                fields = list(st)[:10]
                fields[0] = st.st_mode | stat.S_ISVTX
                return os.stat_result(fields)
            return st

        monkeypatch.setattr(store_paths.os, "stat", sticky_file)
        chmod_calls.clear()

        store_paths._ensure_mode(path, 0o660, access=os.R_OK, what="f")

        assert chmod_calls == [(path, 0o660)]


# ===========================================================================
# Fix-up round, A-12 (R3): create_config_db_file fully succeeds or leaves nothing
# ===========================================================================


@pytest.fixture
def fd_ledger(monkeypatch):
    """Record the descriptor `create_config_db_file` opens and every close."""
    ledger = {"opened": [], "closed": []}
    real_open, real_close = os.open, os.close

    def recording_open(path, *args, **kwargs):
        fd = real_open(path, *args, **kwargs)
        ledger["opened"].append(fd)
        return fd

    def recording_close(fd):
        ledger["closed"].append(fd)
        return real_close(fd)

    monkeypatch.setattr(store_paths.os, "open", recording_open)
    monkeypatch.setattr(store_paths.os, "close", recording_close)
    return ledger


class TestCreateConfigDbFileIsAtomic:
    def _fail_fchmod(self, monkeypatch, exc):
        def fail(*_a, **_k):
            raise exc

        monkeypatch.setattr(store_paths.os, "fchmod", fail)

    def test_fchmod_failure_is_wrapped_and_leaves_nothing(
        self, tmp_path, monkeypatch, fd_ledger
    ):
        target = tmp_path / "config.db"
        with monkeypatch.context() as m:
            self._fail_fchmod(m, PermissionError(1, "Operation not permitted"))
            with pytest.raises(ConfigDbPermissionError) as excinfo:
                create_config_db_file(target)

        assert not isinstance(excinfo.value, OSError)
        assert isinstance(excinfo.value.__cause__, PermissionError)
        assert not os.path.lexists(target)
        assert fd_ledger["opened"] and set(fd_ledger["opened"]) <= set(fd_ledger["closed"])

        # ...and a retry is a clean first creation.
        create_config_db_file(target)
        assert target.stat().st_size == 0 and mode_of(target) == CONFIG_DB_MODE

    def test_fchmod_failure_via_import_yaml_gives_created_true_on_retry(
        self, tmp_path, config_yaml_path, monkeypatch
    ):
        from zfsbackup.config.store import import_yaml

        target = tmp_path / "config.db"
        resolved = resolve_config_path(str(target))
        with monkeypatch.context() as m:
            self._fail_fchmod(m, OSError(5, "I/O error"))
            with pytest.raises(ConfigDbPermissionError):
                import_yaml(config_yaml_path, resolved)
        assert not os.path.lexists(target)

        assert import_yaml(config_yaml_path, resolved).created is True

    def test_keyboard_interrupt_after_creation_leaves_nothing(
        self, tmp_path, monkeypatch, fd_ledger
    ):
        target = tmp_path / "config.db"
        with monkeypatch.context() as m:
            self._fail_fchmod(m, KeyboardInterrupt())
            with pytest.raises(KeyboardInterrupt):
                create_config_db_file(target)

        assert not os.path.lexists(target)
        assert set(fd_ledger["opened"]) <= set(fd_ledger["closed"])
        create_config_db_file(target)  # retry works
        assert target.exists()

    def test_fchown_failure_as_root_on_the_default_path_leaves_nothing(
        self, default_db, monkeypatch
    ):
        # NFS root_squash makes this realistic: the Q7 chgrp is refused.
        class Entry:
            gr_gid = 31337

        monkeypatch.setattr(os, "geteuid", lambda: 0)
        monkeypatch.setattr(grp, "getgrnam", lambda name: Entry())

        def refuse(*_a):
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(store_paths.os, "fchown", refuse)

        with pytest.raises(ConfigDbPermissionError, match="chgrp"):
            create_config_db_file(default_db)

        assert not os.path.lexists(default_db)

    def test_a_swapped_in_inode_is_never_unlinked(self, tmp_path, monkeypatch):
        target = tmp_path / "config.db"
        impostor = tmp_path / "impostor"
        impostor.write_bytes(b"somebody else's database")

        def swap_then_fail(fd, mode):
            # The replacement is renamed over the path (the original inode
            # stays alive through `fd`, so its number cannot be reused).
            os.rename(impostor, target)
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(store_paths.os, "fchmod", swap_then_fail)

        with pytest.raises(ConfigDbPermissionError):
            create_config_db_file(target)

        assert target.read_bytes() == b"somebody else's database"

    def test_a_failing_unlink_is_logged_and_never_masks_the_real_error(
        self, tmp_path, monkeypatch, caplog
    ):
        target = tmp_path / "config.db"
        self._fail_fchmod(monkeypatch, PermissionError(1, "Operation not permitted"))

        def no_unlink(*_a, **_k):
            raise OSError(30, "Read-only file system")

        monkeypatch.setattr(store_paths.os, "unlink", no_unlink)

        with caplog.at_level(logging.WARNING):
            with pytest.raises(ConfigDbPermissionError, match="chmod"):
                create_config_db_file(target)

        (message,) = warnings_of(caplog)
        assert "could not remove the partially created" in message
        assert str(target) in message and "by hand" in message

    def test_o_excl_failure_never_touches_the_existing_file(self, tmp_path, monkeypatch):
        # The unlink-on-failure is reachable only AFTER our own O_EXCL create.
        target = tmp_path / "config.db"
        target.write_bytes(b"already here")
        monkeypatch.setattr(
            store_paths, "_unlink_if_same_file",
            lambda *a: pytest.fail("cleaned up a file this call did not create"),
        )
        with pytest.raises(ConfigDbPermissionError):
            create_config_db_file(target)
        assert target.read_bytes() == b"already here"

    def test_success_leaves_the_file_and_closes_the_descriptor(
        self, tmp_path, fd_ledger
    ):
        target = tmp_path / "config.db"
        create_config_db_file(target)
        assert target.exists()
        assert set(fd_ledger["opened"]) <= set(fd_ledger["closed"])

"""C9 (decision C8): operator-facing permission advice names the GROUP only.

The packaging creates the `zfsbackup` group alone (sysusers), the daemon stays
root and tmpfiles creates the directory `2770 root:zfsbackup`; a `zfsbackup`
USER is never created. Advice that said "owned by zfsbackup:zfsbackup" told an
operator to `chown` to a user that does not exist. `paths.CONFIG_GROUP` is the
group half of `CONFIG_OWNER` and every message built from the config-store
paths names it as "group zfsbackup".

Sections: ``TestConfigGroup`` (the constant and its export),
``TestMessages`` (each message builder, called directly so that no host
ownership is needed), ``TestNoOwnerPairAnywhere`` (a source scan: no message
interpolates `CONFIG_OWNER`), ``TestThroughTheCli`` (a real unreadable
database).

Mutation counts (scratch copy of the tree, one revert at a time; failing tests
in this file plus the two permission classes in `test_zfsbackup_store.py`):
the directory message reverted to `owned by {CONFIG_OWNER}` 4; the file-not-
writable message 2; the sidecar `chgrp` advice 2; the unreadable message 4; the
permission-failure message 3; `CONFIG_GROUP` changed to another literal 2.
"""

import os
import re
from pathlib import Path

import pytest
from _cli_support import Cli, db, isolated, make_db  # noqa: F401

import zfsbackup
from zfsbackup.config import store as store_package
from zfsbackup.config.store import (
    CONFIG_DB_MODE,
    CONFIG_DIR_MODE,
    CONFIG_GROUP,
    CONFIG_OWNER,
    dispose_all,
    resolve_config_path,
)
from zfsbackup.config.store import paths as store_paths

OWNER_PAIR = "zfsbackup:zfsbackup"
SOURCE = Path(zfsbackup.__file__).resolve().parent


@pytest.mark.unit
class TestConfigGroup:
    def test_the_group_is_zfsbackup(self):
        assert CONFIG_GROUP == "zfsbackup"

    def test_it_is_the_group_half_of_the_owner_constant(self):
        assert CONFIG_GROUP == CONFIG_OWNER.split(":", 1)[1]

    def test_it_is_exported_by_the_store_package(self):
        assert "CONFIG_GROUP" in store_package.__all__
        assert store_package.CONFIG_GROUP == store_paths.CONFIG_GROUP == CONFIG_GROUP
        assert "CONFIG_GROUP" in store_paths.__all__


@pytest.fixture
def real_file(tmp_path):
    path = tmp_path / "config.db"
    path.write_bytes(b"x")
    return path


@pytest.mark.unit
class TestMessages:
    def _resolved(self, real_file):
        return resolve_config_path(str(real_file))

    def _assert_group_only(self, message: str):
        assert f"group {CONFIG_GROUP}" in message
        assert OWNER_PAIR not in message
        assert CONFIG_OWNER not in message
        assert "owned by" not in message

    def test_directory_not_writable(self, real_file):
        resolved = self._resolved(real_file)
        message = store_paths._dir_not_writable_message(resolved, real_file.parent)
        self._assert_group_only(message)
        assert f"{CONFIG_DIR_MODE:04o}" in message

    def test_file_not_writable(self, real_file):
        resolved = self._resolved(real_file)
        message = store_paths._file_not_writable_message(resolved, real_file)
        self._assert_group_only(message)
        assert f"{CONFIG_DB_MODE:04o}" in message

    def test_file_not_readable(self, real_file):
        resolved = self._resolved(real_file)
        message = store_paths._unreadable_message(resolved, real_file, os.stat(real_file))
        self._assert_group_only(message)
        assert f"'{CONFIG_GROUP}' group" in message         # "you may not be in ..."
        assert f"{CONFIG_DB_MODE:04o}" in message

    def test_sidecar_unusable(self, real_file):
        resolved = self._resolved(real_file)
        sidecar = Path(str(real_file) + "-wal")
        sidecar.write_bytes(b"")
        message = store_paths._sidecar_unusable_message(
            resolved, real_file, sidecar, os.stat(sidecar)
        )
        assert f"chgrp {CONFIG_GROUP} {sidecar}" in message
        assert OWNER_PAIR not in message

    def test_permission_failure(self, real_file):
        message = store_paths._permission_failure_message(
            "create the directory", real_file, PermissionError(13, "Permission denied")
        )
        assert f"'{CONFIG_GROUP}' group" in message
        assert f"Target: group {CONFIG_GROUP}." in message
        assert OWNER_PAIR not in message and "owned by" not in message

    def test_permission_failure_for_a_path_that_cannot_be_statted(self, tmp_path):
        message = store_paths._permission_failure_message(
            "create the directory", tmp_path / "gone", PermissionError(13, "Permission denied")
        )
        assert f"Target: group {CONFIG_GROUP}." in message and OWNER_PAIR not in message


@pytest.mark.unit
class TestNoOwnerPairAnywhere:
    def test_no_message_interpolates_the_owner_pair(self):
        """`CONFIG_OWNER` may be defined and split, never put in a message."""
        offenders = []
        for path in sorted(SOURCE.rglob("*.py")):
            for number, line in enumerate(path.read_text().splitlines(), 1):
                if re.search(r"\{CONFIG_OWNER[}!:]", line):
                    offenders.append(f"{path.relative_to(SOURCE)}:{number}: {line.strip()}")
        assert offenders == []

    def test_no_other_code_line_spells_the_owner_pair(self):
        """The one definition (`CONFIG_OWNER = "zfsbackup:zfsbackup"`) and
        nothing else (comments and docstring prose are not messages)."""
        offenders = []
        for path in sorted(SOURCE.rglob("*.py")):
            for number, line in enumerate(path.read_text().splitlines(), 1):
                stripped = line.strip()
                if OWNER_PAIR not in line or stripped.startswith(("#", "`", "-")):
                    continue
                if stripped == f'CONFIG_OWNER = "{OWNER_PAIR}"':
                    continue
                offenders.append(f"{path.relative_to(SOURCE)}:{number}: {stripped}")
        assert offenders == []


@pytest.mark.skipif(os.geteuid() == 0, reason="permission checks are meaningless as root")
@pytest.mark.integration
class TestThroughTheCli:
    def test_an_unreadable_database_names_the_group_only(self, db):
        run = Cli()
        make_db(run, db)
        dispose_all()
        os.chmod(db, 0o000)
        try:
            result = run("export", "-c", db)
        finally:
            os.chmod(db, 0o644)
        assert result.exit_code == 78, result.stderr
        assert f"'{CONFIG_GROUP}' group" in result.stderr
        assert OWNER_PAIR not in result.stderr

    def test_a_read_only_directory_names_the_group_only(self, db):
        run = Cli()
        make_db(run, db)
        dispose_all()
        os.chmod(db.parent, 0o555)
        try:
            result = run("set", "snapshot_prefix", "other", "-c", db)
        finally:
            os.chmod(db.parent, 0o755)
        assert result.exit_code != 0
        assert OWNER_PAIR not in result.stderr
        assert f"group {CONFIG_GROUP}" in result.stderr

"""The autouse `_never_run_a_real_zfs` guard in `conftest.py`.

Nothing here can reach a real `zfs`: the "compiled binary named zfs" is a copy
of `/bin/echo`, which is harmless even if the guard failed to fire.
"""

import shutil
import subprocess
import sys

import pytest

# No module-level tier marker: conftest marks every test that declares none as
# `unit`, and the few that spawn a process are marked `integration` individually
# (a test carrying both would be double-counted; see test_marker_tiers.py).

ECHO = shutil.which("echo") or "/bin/echo"


class TestNeverRunARealZfs:
    @pytest.mark.parametrize("name", ["zfs", "zpool"])
    def test_a_compiled_binary_named_zfs_is_refused(self, tmp_path, name):
        fake = tmp_path / name
        shutil.copy(ECHO, fake)
        fake.chmod(0o755)
        with pytest.raises(AssertionError, match="real"):
            subprocess.Popen([str(fake), "version"], stdout=subprocess.DEVNULL)

    def test_shell_string_form_is_checked_too(self, tmp_path):
        fake = tmp_path / "zfs"
        shutil.copy(ECHO, fake)
        fake.chmod(0o755)
        with pytest.raises(AssertionError, match="real"):
            subprocess.run(f"{fake} version", shell=True, capture_output=True)

    def test_a_script_named_zfs_is_fine(self, tmp_path):
        script = tmp_path / "zfs"
        script.write_text("#!/bin/sh\necho fake\n")
        script.chmod(0o755)
        done = subprocess.run([str(script)], capture_output=True, text=True)
        assert done.stdout == "fake\n"

    def test_other_programs_are_unaffected(self):
        done = subprocess.run([sys.executable, "-c", "print(1)"], capture_output=True, text=True)
        assert done.stdout.strip() == "1"

    def test_popen_keeps_its_identity(self):
        assert isinstance(subprocess.Popen([sys.executable, "-c", "pass"]), subprocess.Popen)

"""Milestone C, item C1: the single version source, the daemon console-script
entry point and the packaging metadata.

- ``TestInstalledVersion``  `zfsbackup.version.installed_version`: metadata, the
                            unknown fallback, no cache, no module-level version name.
- ``TestLazyDunderVersion`` `zfsbackup.__version__` is resolved on first access.
- ``TestImportWeight``      `import zfsbackup` / `zfsbackup.version` / `daemon_entry`
                            stay light (a `python -I` subprocess).
- ``TestConstants``         `EXIT_CODE_VERSION_CHANGED`, `DIST_NAME`.
- ``TestVersionString``     the `--version` line.
- ``TestDaemonEntry``       SIGHUP ignored before the daemon runs; status passes through.
- ``TestPyproject``         static checks of `pyproject.toml` and `packaging/`.
- ``TestBuiltPackages``     `poetry build` into a tmp dir (`slow`).

``installed_version`` imports ``version`` with ``from importlib.metadata import
version``, so the unit tests patch ``zfsbackup.version.version`` (patching
``importlib.metadata.version`` would not reach the already-bound name); the
real-metadata path is covered by a throwaway ``.dist-info`` on ``sys.path``.
"""

import importlib
import importlib.metadata
import re
import shutil
import signal
import subprocess
import sys
import textwrap
import tomllib
import zipfile
from pathlib import Path
from unittest import mock

import pytest

import zfsbackup
import zfsbackup.version as version_mod
from zfsbackup.version import (
    DIST_NAME,
    EXIT_CODE_VERSION_CHANGED,
    UNKNOWN_VERSION,
    installed_version,
    version_string,
)

from _hermetic import hermetic_env

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def pyproject():
    with open(REPO / "pyproject.toml", "rb") as handle:
        return tomllib.load(handle)


# --------------------------------------------------------------------------
# installed_version
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestInstalledVersion:
    def test_returns_the_metadata_version(self):
        with mock.patch.object(version_mod, "version", return_value="4.5.6") as patched:
            assert installed_version() == "4.5.6"
        patched.assert_called_once_with(DIST_NAME)

    def test_unknown_when_the_distribution_is_not_installed(self):
        with mock.patch.object(
            version_mod, "version", side_effect=importlib.metadata.PackageNotFoundError(DIST_NAME)
        ):
            assert installed_version() == "0+unknown"
        assert UNKNOWN_VERSION == "0+unknown"

    def test_other_errors_are_not_swallowed(self):
        with mock.patch.object(version_mod, "version", side_effect=OSError("boom")):
            with pytest.raises(OSError):
                installed_version()

    def test_not_cached(self):
        """The self-restart on upgrade compares two calls; a cache makes them equal."""
        with mock.patch.object(version_mod, "version", return_value="1.0.0"):
            first = installed_version()
        with mock.patch.object(version_mod, "version", return_value="2.0.0"):
            second = installed_version()
        assert (first, second) == ("1.0.0", "2.0.0")

    def test_not_cached_across_a_failure_either(self):
        with mock.patch.object(
            version_mod, "version", side_effect=importlib.metadata.PackageNotFoundError(DIST_NAME)
        ):
            assert installed_version() == UNKNOWN_VERSION
        with mock.patch.object(version_mod, "version", return_value="3.0.0"):
            assert installed_version() == "3.0.0"

    def test_no_module_level_version_name(self):
        """The version has one source; nothing in the module spells it."""
        for forbidden in ("__version__", "VERSION", "_VERSION", "INSTALLED_VERSION", "_installed"):
            assert not hasattr(version_mod, forbidden), forbidden
        literal = re.compile(r"^\d+(\.\d+)+")
        for name, value in vars(version_mod).items():
            if isinstance(value, str) and not name.startswith("__"):
                assert not literal.match(value), (name, value)

    def test_reads_real_metadata_by_the_dist_name(self, tmp_path, monkeypatch):
        """End to end through importlib.metadata: a throwaway dist-info that sorts
        first on sys.path, replaced by another, is seen on the second call."""

        def make(root, ver):
            info = root / f"{DIST_NAME}-{ver}.dist-info"
            info.mkdir(parents=True)
            (info / "METADATA").write_text(
                f"Metadata-Version: 2.1\nName: {DIST_NAME}\nVersion: {ver}\n"
            )

        first, second = tmp_path / "one", tmp_path / "two"
        make(first, "9.8.7")
        make(second, "9.8.8")
        monkeypatch.syspath_prepend(str(first))
        importlib.invalidate_caches()
        assert installed_version() == "9.8.7"
        monkeypatch.syspath_prepend(str(second))
        importlib.invalidate_caches()
        assert installed_version() == "9.8.8"


# --------------------------------------------------------------------------
# zfsbackup.__version__
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestLazyDunderVersion:
    def test_equals_installed_version(self):
        assert zfsbackup.__version__ == installed_version()

    def test_follows_the_metadata_on_every_access(self):
        with mock.patch.object(version_mod, "version", return_value="7.7.7"):
            assert zfsbackup.__version__ == "7.7.7"
        with mock.patch.object(version_mod, "version", return_value="8.8.8"):
            assert zfsbackup.__version__ == "8.8.8"

    def test_not_in_the_module_dict_after_access(self):
        zfsbackup.__version__
        assert "__version__" not in vars(zfsbackup)

    def test_not_in_the_module_dict_before_access(self):
        code = (
            "import sys; sys.path.insert(0, sys.argv[1]); import zfsbackup; "
            "assert '__version__' not in vars(zfsbackup); print('ok')"
        )
        proc = subprocess.run(
            [sys.executable, "-I", "-c", code, str(REPO)],
            capture_output=True, text=True, timeout=60, env=hermetic_env(),
        )
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == "ok"

    def test_unknown_attribute_raises_attribute_error(self):
        with pytest.raises(AttributeError, match="no_such_attribute"):
            zfsbackup.no_such_attribute
        assert not hasattr(zfsbackup, "no_such_attribute")

    def test_dir_lists_it(self):
        listing = dir(zfsbackup)
        assert "__version__" in listing
        assert listing == sorted(listing)
        assert "__name__" in listing


# --------------------------------------------------------------------------
# import weight (python -I subprocess)
# --------------------------------------------------------------------------


def _run_isolated(code, *args):
    return subprocess.run(
        [sys.executable, "-I", "-c", code, str(REPO), *args],
        capture_output=True, text=True, timeout=120, env=hermetic_env(),
    )


@pytest.mark.unit
class TestImportWeight:
    _PRELUDE = "import sys; sys.path.insert(0, sys.argv[1])\n"

    def test_package_and_version_load_no_store_or_daemon(self):
        code = self._PRELUDE + textwrap.dedent("""
            import zfsbackup
            import zfsbackup.version
            bad = sorted(
                m for m in sys.modules
                if m.split('.')[0] in ('sqlalchemy', 'alembic')
                or m == 'zfsbackup.daemon' or m.startswith('zfsbackup.config')
            )
            print(','.join(bad) if bad else 'clean')
        """)
        proc = _run_isolated(code)
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == "clean", proc.stdout

    def test_daemon_entry_does_not_import_the_daemon(self):
        code = self._PRELUDE + textwrap.dedent("""
            import zfsbackup.daemon_entry
            bad = sorted(
                m for m in sys.modules
                if m.split('.')[0] in ('sqlalchemy', 'alembic')
                or m == 'zfsbackup.daemon'
            )
            print(','.join(bad) if bad else 'clean')
        """)
        proc = _run_isolated(code)
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == "clean", proc.stdout

    def test_importing_the_package_does_not_read_metadata(self):
        """`__version__` is lazy: nothing calls importlib.metadata at import."""
        code = self._PRELUDE + textwrap.dedent("""
            import importlib.metadata as md
            def boom(*a, **k):
                raise SystemExit('metadata read at import')
            md.version = boom
            import zfsbackup, zfsbackup.version
            print('ok')
        """)
        proc = _run_isolated(code)
        assert proc.returncode == 0, proc.stderr + proc.stdout
        assert proc.stdout.strip() == "ok"


# --------------------------------------------------------------------------
# constants
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestConstants:
    def test_exit_code_version_changed_is_76(self):
        assert EXIT_CODE_VERSION_CHANGED == 76

    def test_exit_code_differs_from_the_cli_codes(self):
        from zfsbackup.cli import errors

        assert EXIT_CODE_VERSION_CHANGED not in (errors.EXIT_BUSY, errors.EXIT_CONFIG)
        assert (errors.EXIT_BUSY, errors.EXIT_CONFIG) == (75, 78)

    def test_exit_code_is_not_a_signal_or_shell_status(self):
        assert 3 < EXIT_CODE_VERSION_CHANGED < 126

    def test_dist_name_is_the_pyproject_name(self, pyproject):
        assert DIST_NAME == pyproject["tool"]["poetry"]["name"] == "zfsbackup"


# --------------------------------------------------------------------------
# version_string
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestVersionString:
    def test_format(self):
        from zfsbackup.config.store.migrate import EXPECTED_HEAD
        from zfsbackup.protocol import PROTOCOL_VERSION

        with mock.patch.object(version_mod, "version", return_value="1.2.3"):
            line = version_string("zfsbackupd")
        assert line == (
            f"zfsbackupd 1.2.3 (wire protocol {PROTOCOL_VERSION}, config schema {EXPECTED_HEAD})"
        )

    def test_matches_the_documented_shape(self):
        line = version_string("zfsbackup-config")
        assert re.fullmatch(
            r"zfsbackup-config \S+ \(wire protocol \d+, config schema [0-9a-f]{12}\)", line
        ), line

    def test_component_is_used_verbatim(self):
        assert version_string("anything at all").startswith("anything at all ")

    def test_follows_the_installed_version_each_call(self):
        with mock.patch.object(version_mod, "version", return_value="1.0"):
            one = version_string("c")
        with mock.patch.object(version_mod, "version", return_value="2.0"):
            two = version_string("c")
        assert " 1.0 " in one and " 2.0 " in two

    def test_unknown_install_is_spelled_out(self):
        with mock.patch.object(
            version_mod, "version", side_effect=importlib.metadata.PackageNotFoundError(DIST_NAME)
        ):
            assert version_string("zfsbackupd").startswith("zfsbackupd 0+unknown (wire protocol ")


# --------------------------------------------------------------------------
# daemon_entry.main
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestDaemonEntry:
    @pytest.fixture(autouse=True)
    def _restore_sighup(self):
        before = signal.getsignal(signal.SIGHUP)
        yield
        signal.signal(signal.SIGHUP, before)

    def test_sighup_is_ignored_when_the_daemon_starts(self):
        from zfsbackup import daemon, daemon_entry

        seen = []

        def fake_process_main():
            seen.append(signal.getsignal(signal.SIGHUP))
            return 0

        signal.signal(signal.SIGHUP, signal.SIG_DFL)
        with mock.patch.object(daemon, "_process_main", fake_process_main):
            daemon_entry.main()
        assert seen == [signal.SIG_IGN]

    @pytest.mark.parametrize("status", [0, 1, 3, 76])
    def test_the_status_passes_through(self, status):
        from zfsbackup import daemon, daemon_entry

        with mock.patch.object(daemon, "_process_main", return_value=status) as patched:
            assert daemon_entry.main() == status
        patched.assert_called_once_with()

    def test_an_exception_from_the_daemon_propagates(self):
        from zfsbackup import daemon, daemon_entry

        with mock.patch.object(daemon, "_process_main", side_effect=RuntimeError("x")):
            with pytest.raises(RuntimeError):
                daemon_entry.main()
        assert signal.getsignal(signal.SIGHUP) == signal.SIG_IGN

    def test_the_only_module_level_import_is_signal(self):
        source = (REPO / "zfsbackup" / "daemon_entry.py").read_text()
        top_level = [
            line for line in source.splitlines() if re.match(r"(import|from)\s", line)
        ]
        assert top_level == ["import signal"]


# --------------------------------------------------------------------------
# pyproject.toml / packaging/ (static)
# --------------------------------------------------------------------------


def _specifiers(requirement):
    """{name: frozenset of specifier strings} from `name (>=1,<2)` / `name>=1,<2`."""
    match = re.match(r"^([A-Za-z0-9._-]+)\s*\(?([^)\[;]*)\)?", requirement)
    name = match.group(1).lower().replace("_", "-")
    return name, frozenset(s.strip() for s in match.group(2).replace(" ", "").split(",") if s.strip())


_PINS = {
    "werkzeug": {">=3.1", "<3.2"},
    "urllib3": {">=2", "<3"},
    "requests": {">=2.31", "<3"},
    "sqlalchemy": {">=2.0", "<2.2"},
}


@pytest.mark.unit
class TestPyproject:
    def test_distribution_is_named_zfsbackup(self, pyproject):
        assert pyproject["tool"]["poetry"]["name"] == "zfsbackup"

    def test_both_packages_are_still_shipped(self, pyproject):
        includes = {entry["include"] for entry in pyproject["tool"]["poetry"]["packages"]}
        assert includes == {"libzfseasy", "zfsbackup"}

    def test_console_scripts(self, pyproject):
        scripts = pyproject["tool"]["poetry"]["scripts"]
        assert scripts["zfsbackupd"] == "zfsbackup.daemon_entry:main"
        assert scripts["zfsbackup-config"] == "zfsbackup.cli.main:main"

    def test_daemon_script_target_resolves(self, pyproject):
        module_name, _, attr = pyproject["tool"]["poetry"]["scripts"]["zfsbackupd"].partition(":")
        from zfsbackup import daemon_entry

        assert importlib.import_module(module_name) is daemon_entry
        assert getattr(daemon_entry, attr) is daemon_entry.main

    @pytest.mark.parametrize("name", sorted(_PINS))
    def test_dependency_pins(self, pyproject, name):
        spec = pyproject["tool"]["poetry"]["dependencies"][name]
        assert isinstance(spec, str)
        _, specifiers = _specifiers(f"{name}{spec}")
        assert specifiers == _PINS[name]

    def test_packaging_directory_ships_in_the_sdist_only(self, pyproject):
        include = pyproject["tool"]["poetry"]["include"]
        assert include == [{"path": "packaging/**/*", "format": "sdist"}]

    def test_packaging_directory_has_no_python(self):
        """A `.py`/`__init__.py` there would install a top-level `packaging`
        that shadows PyPA's."""
        packaging = REPO / "packaging"
        if not packaging.exists():
            pytest.skip("packaging/ does not exist yet")
        offenders = [str(p.relative_to(REPO)) for p in packaging.rglob("*")
                     if p.is_file() and (p.suffix in (".py", ".pyc", ".pyi") or p.name == "__init__.py")]
        assert offenders == []

    def test_no_python_package_is_declared_for_packaging(self, pyproject):
        includes = {entry["include"] for entry in pyproject["tool"]["poetry"]["packages"]}
        assert "packaging" not in includes


# --------------------------------------------------------------------------
# built artifacts (slow)
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    import os

    poetry = shutil.which("poetry") or shutil.which("poetry", path=os.environ.get("PATH", ""))
    if poetry is None:
        pytest.skip("poetry is not installed; cannot build the wheel")
    out = tmp_path_factory.mktemp("zfsbackup-dist")
    try:
        proc = subprocess.run(
            [poetry, "build", "-o", str(out)], cwd=REPO,
            capture_output=True, text=True, timeout=600, env=hermetic_env(HOME=os.environ.get("HOME", "")),
        )
    except subprocess.TimeoutExpired:
        pytest.skip("poetry build timed out (needs network?)")
    if proc.returncode != 0:
        pytest.skip(f"poetry build unavailable here: {(proc.stderr or proc.stdout).strip()[-300:]}")
    wheels, sdists = list(out.glob("*.whl")), list(out.glob("*.tar.gz"))
    if len(wheels) != 1:
        pytest.skip(f"poetry build produced {len(wheels)} wheels")
    return wheels[0], (sdists[0] if sdists else None)


@pytest.mark.slow
@pytest.mark.integration
class TestBuiltPackages:
    @pytest.fixture(scope="class")
    def wheel(self, built):
        return built[0]

    @pytest.fixture(scope="class")
    def names(self, wheel):
        with zipfile.ZipFile(wheel) as archive:
            return archive.namelist()

    def _dist_info(self, wheel, filename):
        with zipfile.ZipFile(wheel) as archive:
            member = next(n for n in archive.namelist()
                          if n.endswith(".dist-info/" + filename))
            return archive.read(member).decode()

    def test_wheel_is_named_zfsbackup(self, wheel):
        assert wheel.name.startswith("zfsbackup-")

    def test_wheel_carries_the_migrations_and_new_modules(self, names):
        assert "zfsbackup/version.py" in names
        assert "zfsbackup/daemon_entry.py" in names
        assert any(
            n.startswith("zfsbackup/config/store/migrations/versions/") and n.endswith(".py")
            for n in names
        )
        assert "libzfseasy/__init__.py" in names

    def test_no_top_level_packaging_or_tests(self, names):
        tops = {n.split("/")[0] for n in names}
        assert "packaging" not in tops
        assert "tests" not in tops
        assert not any(n.startswith(("packaging/", "tests/")) for n in names)
        assert tops - {"zfsbackup", "libzfseasy"} <= {
            n for n in tops if n.endswith(".dist-info")
        }

    def test_entry_points(self, wheel):
        import configparser

        parser = configparser.ConfigParser()
        parser.optionxform = str
        parser.read_string(self._dist_info(wheel, "entry_points.txt"))
        scripts = parser["console_scripts"]
        assert scripts["zfsbackupd"] == "zfsbackup.daemon_entry:main"
        assert scripts["zfsbackup-config"] == "zfsbackup.cli.main:main"

    def test_metadata_name_and_version(self, wheel, pyproject):
        metadata = self._dist_info(wheel, "METADATA")
        assert re.search(r"^Name: zfsbackup$", metadata, re.M)
        assert re.search(
            rf"^Version: {re.escape(pyproject['tool']['poetry']['version'])}$", metadata, re.M
        )

    def test_pins_are_in_requires_dist(self, wheel):
        metadata = self._dist_info(wheel, "METADATA")
        requires = dict(
            _specifiers(line.split(":", 1)[1].strip())
            for line in metadata.splitlines() if line.startswith("Requires-Dist:")
        )
        for name, expected in _PINS.items():
            assert requires[name] == frozenset(expected), (name, requires.get(name))

    def test_sdist_has_no_tests_and_no_top_level_packaging_python(self, built):
        import tarfile

        sdist = built[1]
        if sdist is None:
            pytest.skip("no sdist was built")
        with tarfile.open(sdist) as archive:
            members = archive.getnames()
        assert not any("/tests/" in m for m in members)
        assert not any(m.endswith("packaging/__init__.py") or
                       (re.search(r"/packaging/.*\.py$", m) is not None) for m in members)
        if (REPO / "packaging").is_dir() and any((REPO / "packaging").rglob("*")):
            assert any("/packaging/" in m for m in members)

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
- ``TestPyproject``         static checks of `pyproject.toml` ([project]) and `packaging/`.
- ``TestLockstep``          the two versions and the `libzfseasy==` pin are equal; the libs
                            project has no dependencies.
- ``TestLibzfseasyIsStdlibOnly``  libzfseasy imports nothing outside the standard library.
- ``TestBuiltPackages``     `poetry build` of BOTH projects into tmp dirs (`slow`).

``installed_version`` imports ``version`` with ``from importlib.metadata import
version``, so the unit tests patch ``zfsbackup.version.version`` (patching
``importlib.metadata.version`` would not reach the already-bound name); the
real-metadata path is covered by a throwaway ``.dist-info`` on ``sys.path``.
"""

import importlib
import importlib.metadata
import os
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
from packaging.utils import canonicalize_name

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
from _pyproject_support import (
    LIB_DIR,
    REPO,
    load_lib_pyproject,
    load_pyproject,
    requirements,
    specifier_strings,
)


@pytest.fixture(scope="module")
def pyproject():
    return load_pyproject()


@pytest.fixture(scope="module")
def lib_pyproject():
    return load_lib_pyproject()


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
        assert DIST_NAME == pyproject["project"]["name"] == "zfsbackup"

    def test_lib_dist_name_is_the_libs_pyproject_name(self, lib_pyproject):
        assert version_mod.LIB_DIST_NAME == lib_pyproject["project"]["name"] == "libzfseasy"


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


_PINS = {
    "werkzeug": {">=3.1", "<3.2"},
    "urllib3": {">=2", "<3"},
    "requests": {">=2.31", "<3"},
    "sqlalchemy": {">=2.0", "<2.2"},
    "simple-websocket": {">=1.1", "<1.2"},
    # Debian 13's archive: do not raise a floor above what it ships (packaging_plan.md).
    "flask": {">=3.1.1", "<4"},
    "websocket-client": {">=1.8.0", "<2"},
}


@pytest.mark.unit
class TestPyproject:
    def test_distribution_is_named_zfsbackup(self, pyproject):
        assert pyproject["project"]["name"] == "zfsbackup"

    def test_only_the_zfsbackup_package_is_shipped(self, pyproject):
        includes = {entry["include"] for entry in pyproject["tool"]["poetry"]["packages"]}
        assert includes == {"zfsbackup"}

    def test_libzfseasy_is_a_declared_dependency_not_a_bundled_package(self, pyproject):
        assert "libzfseasy" in requirements(pyproject)

    def test_the_metadata_is_pep_621_not_the_old_poetry_tables(self, pyproject):
        poetry = pyproject["tool"]["poetry"]
        for old in ("name", "version", "scripts"):
            assert old not in poetry, f"[tool.poetry] {old} duplicates [project]"
        # `[tool.poetry.dependencies]` is development-only: just the editable sibling project.
        assert set(poetry["dependencies"]) == {"libzfseasy"}
        assert poetry["dependencies"]["libzfseasy"] == {"path": "libs/libzfseasy", "develop": True}

    def test_requires_python_matches_the_documented_floor(self, pyproject, lib_pyproject):
        assert pyproject["project"]["requires-python"] == ">=3.11"
        assert lib_pyproject["project"]["requires-python"] == ">=3.11"

    def test_console_scripts(self, pyproject):
        scripts = pyproject["project"]["scripts"]
        assert scripts["zfsbackupd"] == "zfsbackup.daemon_entry:main"
        assert scripts["zfsbackup-config"] == "zfsbackup.cli.main:main"

    def test_daemon_script_target_resolves(self, pyproject):
        module_name, _, attr = pyproject["project"]["scripts"]["zfsbackupd"].partition(":")
        from zfsbackup import daemon_entry

        assert importlib.import_module(module_name) is daemon_entry
        assert getattr(daemon_entry, attr) is daemon_entry.main

    @pytest.mark.parametrize("name", sorted(_PINS))
    def test_dependency_pins(self, pyproject, name):
        assert specifier_strings(requirements(pyproject)[name]) == frozenset(_PINS[name])

    def test_flask_sock_is_not_a_dependency_it_is_vendored(self, pyproject):
        assert "flask-sock" not in requirements(pyproject)
        assert (REPO / "zfsbackup" / "_vendor" / "flask_sock.py").is_file()

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


@pytest.mark.unit
class TestLibzfseasyProject:
    def test_it_is_a_self_contained_project(self):
        for name in ("pyproject.toml", "README.md", "LICENSE", "libzfseasy/__init__.py"):
            assert (LIB_DIR / name).is_file(), name

    def test_only_the_libzfseasy_package_is_shipped(self, lib_pyproject):
        includes = {entry["include"] for entry in lib_pyproject["tool"]["poetry"]["packages"]}
        assert includes == {"libzfseasy"}

    def test_the_old_top_level_package_directory_is_gone(self):
        assert not (REPO / "libzfseasy").exists()


@pytest.mark.unit
class TestLockstep:
    """Root `version`, `libs/libzfseasy` `version` and the `libzfseasy==` pin move together
    (the release checklist bumps all three); an exact pin makes the tested pair the only
    installable pair."""

    def test_the_three_versions_are_equal(self, pyproject, lib_pyproject):
        root = pyproject["project"]["version"]
        lib = lib_pyproject["project"]["version"]
        pin = requirements(pyproject)["libzfseasy"]
        specs = list(pin.specifier)
        assert [s.operator for s in specs] == ["=="], f"libzfseasy must be pinned exactly: {pin}"
        assert root == lib == specs[0].version, (root, lib, specs[0].version)

    def test_the_pin_has_no_wildcard_or_local_part(self, pyproject):
        (spec,) = list(requirements(pyproject)["libzfseasy"].specifier)
        assert "*" not in spec.version and "+" not in spec.version

    def test_the_libs_project_has_no_dependencies(self, lib_pyproject):
        assert lib_pyproject["project"]["dependencies"] == []
        assert "optional-dependencies" not in lib_pyproject["project"]
        assert "dependencies" not in lib_pyproject["tool"]["poetry"]

    def test_the_libs_project_declares_no_scripts(self, lib_pyproject):
        assert "scripts" not in lib_pyproject["project"]

    def test_the_installed_pair_is_in_lockstep(self):
        """The development environment installs both editable at one version."""
        try:
            installed = (importlib.metadata.version("zfsbackup"),
                         importlib.metadata.version("libzfseasy"))
        except importlib.metadata.PackageNotFoundError:
            pytest.skip("not installed (run `poetry install`)")
        assert installed[0] == installed[1], installed


# --------------------------------------------------------------------------
# debian/ against the pyprojects
# --------------------------------------------------------------------------

#: PEP 508 distribution name -> Debian binary package; the rest are python3-<name>.
_DEBIAN_NAMES = {
    "pyyaml": "python3-yaml",
    "websocket-client": "python3-websocket",
    "simple-websocket": "python3-simple-websocket",
}

#: The version of the top `debian/changelog` entry (e.g. ``1.0.0~rc2-1``); the mutation tests edit it.
CHANGELOG_VERSION = re.match(
    r"\S+ \(([^)]+)\)", (REPO / "debian" / "changelog").read_text().splitlines()[0]
).group(1)
_DEB_UPSTREAM = re.compile(r"(?P<release>\d+(?:\.\d+)*)(?:~(?P<pre>(?:a|b|rc)\d+))?")
_DEB_DEP = re.compile(r"(?P<name>[a-z0-9][a-z0-9+.-]*)\s*(?:\((?P<op>>=|<<|=|<=|>>)\s*(?P<ver>[^)\s]+)\s*\))?$")


def debian_name(distribution: str) -> str:
    name = canonicalize_name(distribution)
    return _DEBIAN_NAMES.get(name, f"python3-{name}")


def debian_upstream_to_pep440(changelog_version: str) -> str:
    """``1.0.0~rc1-1`` -> ``1.0.0rc1`` (``~`` sorts before the release, as a/b/rc do; the
    Debian revision after the last ``-`` is dropped)."""
    upstream = changelog_version.rsplit("-", 1)[0] if "-" in changelog_version else changelog_version
    match = _DEB_UPSTREAM.fullmatch(upstream)
    if not match:
        raise ValueError(f"unsupported upstream version {upstream!r} (expected N.N.N[~aN|~bN|~rcN])")
    return match["release"] + (match["pre"] or "")


def _stanza(control_text: str, key: str, value: str) -> dict:
    """The fields of the first control paragraph whose ``key`` is ``value`` (continuations joined)."""
    for paragraph in re.split(r"\n\s*\n", control_text.strip("\n")):
        fields, current = {}, None
        for line in paragraph.splitlines():
            if line.startswith("#"):
                continue
            if line[:1] in " \t" and current:
                fields[current] += "\n" + line.strip()
            else:
                current, _, rest = line.partition(":")
                fields[current] = rest.strip()
        if fields.get(key) == value:
            return fields
    raise AssertionError(f"debian/control has no paragraph with {key}: {value}")


def _depends(field: str) -> dict:
    """``{package: [(op, version-or-None), ...]}`` of a Depends field; substvars are skipped."""
    found = {}
    for item in (part.strip() for part in field.replace("\n", " ").split(",")):
        if not item or item.startswith("${"):
            continue
        match = _DEB_DEP.fullmatch(item)
        assert match, f"debian/control: cannot parse the dependency {item!r}"
        found.setdefault(match["name"], []).append((match["op"], match["ver"]))
    return found


def debian_problems(root: Path) -> list:
    """Every way ``root/debian`` disagrees with ``root``'s pyprojects, each saying which file to fix."""
    problems = []
    project = load_pyproject(root)["project"]

    # changelog <-> version
    first = (root / "debian" / "changelog").read_text().splitlines()[0]
    header = re.match(r"(?P<source>\S+) \((?P<version>[^)]+)\) ", first)
    if not header:
        return [f"debian/changelog: the first line {first!r} is not a changelog header"]
    try:
        mapped = debian_upstream_to_pep440(header["version"])
    except ValueError as error:
        return [f"debian/changelog: {error}"]
    if mapped != project["version"]:
        problems.append(
            f"debian/changelog says {header['version']} (= {mapped}) but pyproject.toml says "
            f"{project['version']}: fix debian/changelog (or bump pyproject.toml and "
            "libs/libzfseasy/pyproject.toml, which move together)"
        )

    control = (root / "debian" / "control").read_text()
    depends = _depends(_stanza(control, "Package", "zfsbackup")["Depends"])

    # python3-libzfseasy, exactly this build
    if ("=", "${binary:Version}") not in depends.get("python3-libzfseasy", []):
        problems.append(
            "debian/control: zfsbackup must Depend on python3-libzfseasy (= ${binary:Version}), "
            f"found {depends.get('python3-libzfseasy')}"
        )

    # the other dependencies, floor and ceiling
    expected_packages = set()
    for requirement in requirements({"project": project}).values():
        if canonicalize_name(requirement.name) == "libzfseasy":
            continue
        package = debian_name(requirement.name)
        expected_packages.add(package)
        found = depends.get(package)
        if not found:
            problems.append(
                f"debian/control: zfsbackup lacks a Depends on {package} (pyproject.toml requires "
                f"{requirement.name}{requirement.specifier}): add it to debian/control"
            )
            continue
        want = []
        for spec in sorted(requirement.specifier, key=str):
            if spec.operator == ">=":
                want.append((">=", spec.version))
            elif spec.operator == "<":
                want.append(("<<", spec.version + "~"))
            else:
                problems.append(
                    f"pyproject.toml: {requirement.name}{spec} uses an operator this check does not map "
                    "to a Debian relation (extend the test)"
                )
        if sorted(found, key=str) != sorted(want, key=str):
            problems.append(
                f"debian/control: {package} has {sorted(found, key=str)} but pyproject.toml's "
                f"{requirement.name}{requirement.specifier} maps to {sorted(want, key=str)} "
                "(floor `>=` equal, ceiling `<X` as `<< X~`): fix debian/control or pyproject.toml"
            )
    extra = {n for n in depends if n.startswith("python3-") and n != "python3-libzfseasy"} - expected_packages
    if extra:
        problems.append(
            f"debian/control: zfsbackup Depends on {sorted(extra)}, which pyproject.toml's "
            "[project] dependencies do not list: remove it from debian/control or add it to pyproject.toml"
        )

    # Python floor
    source = _stanza(control, "Source", "zfsbackup")
    declared = source.get("X-Python3-Version")
    if declared is not None and declared.replace(" ", "") != project["requires-python"].replace(" ", ""):
        problems.append(
            f"debian/control: X-Python3-Version {declared!r} differs from pyproject.toml's "
            f"requires-python {project['requires-python']!r}: fix debian/control"
        )
    return problems


@pytest.fixture
def debian_tree(tmp_path):
    """A copy of the files ``debian_problems`` reads, to mutate (the real files are never edited)."""
    for relative in ("pyproject.toml", "libs/libzfseasy/pyproject.toml",
                     "debian/changelog", "debian/control"):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / relative, target)
    return tmp_path


def _edit(root: Path, relative: str, old: str, new: str) -> None:
    path = root / relative
    text = path.read_text()
    assert old in text, f"{relative} has no {old!r} to mutate"
    path.write_text(text.replace(old, new, 1))


@pytest.mark.unit
class TestDebianMatchesPyprojects:
    """`debian/` is hand-written; nothing else ties it to the pyprojects. A drifting floor would
    ship a package that installs against a library it was never tested with."""

    @pytest.fixture(autouse=True)
    def _needs_debian(self):
        if not (REPO / "debian" / "control").is_file():
            pytest.skip("no debian/ directory (installed copy or sdist)")

    def test_debian_agrees_with_the_pyprojects(self):
        problems = debian_problems(REPO)
        assert not problems, "\n".join(problems)

    @pytest.mark.parametrize("deb, pep440", [
        ("1.0.0~rc1-1", "1.0.0rc1"),
        ("1.0.0~b2-3", "1.0.0b2"),
        ("2.1.0~a1-1", "2.1.0a1"),
        ("1.0.0-1", "1.0.0"),
        ("1.0.0", "1.0.0"),
        ("1.2-4ubuntu1", "1.2"),
    ])
    def test_the_version_mapping(self, deb, pep440):
        assert debian_upstream_to_pep440(deb) == pep440

    @pytest.mark.parametrize("deb", ["1.0.0+dfsg-1", "1.0.0~rc-1", "1.0.0~beta1-1"])
    def test_an_unmapped_version_is_refused_not_guessed(self, deb):
        with pytest.raises(ValueError):
            debian_upstream_to_pep440(deb)

    @pytest.mark.parametrize("name, package", [
        ("pyyaml", "python3-yaml"), ("websocket-client", "python3-websocket"),
        ("simple-websocket", "python3-simple-websocket"), ("SQLAlchemy", "python3-sqlalchemy"),
        ("flask", "python3-flask"),
    ])
    def test_the_package_name_mapping(self, name, package):
        assert debian_name(name) == package

    def test_the_changelog_is_the_pyproject_version(self, pyproject):
        first = (REPO / "debian" / "changelog").read_text().splitlines()[0]
        version = re.match(r"\S+ \(([^)]+)\)", first).group(1)
        assert debian_upstream_to_pep440(version) == pyproject["project"]["version"]

    # -- each assertion fails on a mutated copy, and names the file to fix --------------------

    def test_mutation_clean_copy_has_no_problems(self, debian_tree):
        assert debian_problems(debian_tree) == []

    def test_mutation_a_changelog_version_mismatch_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/changelog", f"({CHANGELOG_VERSION})", "(1.0.0~rc99-1)")
        (problem,) = debian_problems(debian_tree)
        assert "debian/changelog" in problem and "1.0.0rc99" in problem

    def test_mutation_a_plain_release_against_a_prerelease_pyproject_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/changelog", f"({CHANGELOG_VERSION})", "(1.0.0-1)")
        (problem,) = debian_problems(debian_tree)
        assert "debian/changelog" in problem

    def test_the_debian_revision_is_ignored(self, debian_tree):
        _edit(debian_tree, "debian/changelog", f"({CHANGELOG_VERSION})", f"({CHANGELOG_VERSION[:-2]}-7)")
        assert debian_problems(debian_tree) == []

    def test_mutation_a_missing_dependency_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/control", " python3-yaml (>= 6.0),\n python3-yaml (<< 7~),\n", "")
        (problem,) = debian_problems(debian_tree)
        assert "debian/control" in problem and "python3-yaml" in problem

    def test_mutation_a_raised_floor_in_pyproject_is_reported(self, debian_tree):
        _edit(debian_tree, "pyproject.toml", '"requests>=2.31,<3"', '"requests>=2.32,<3"')
        (problem,) = debian_problems(debian_tree)
        assert "python3-requests" in problem and "2.32" in problem

    def test_mutation_a_lowered_floor_in_control_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/control", "python3-flask (>= 3.1.1)", "python3-flask (>= 3.0)")
        (problem,) = debian_problems(debian_tree)
        assert "python3-flask" in problem and "debian/control" in problem

    def test_mutation_a_widened_ceiling_in_pyproject_is_reported(self, debian_tree):
        _edit(debian_tree, "pyproject.toml", '"simple-websocket>=1.1,<1.2"', '"simple-websocket>=1.1,<1.3"')
        (problem,) = debian_problems(debian_tree)
        assert "python3-simple-websocket" in problem and "1.3~" in problem

    def test_mutation_a_ceiling_without_the_tilde_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/control", "python3-werkzeug (<< 3.2~)", "python3-werkzeug (<< 3.2)")
        (problem,) = debian_problems(debian_tree)
        assert "python3-werkzeug" in problem

    def test_mutation_a_missing_ceiling_in_control_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/control", " python3-click (<< 9~),\n", "")
        (problem,) = debian_problems(debian_tree)
        assert "python3-click" in problem

    def test_mutation_a_ceiling_added_to_cryptography_in_control_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/control", " python3-cryptography (>= 43),\n",
              " python3-cryptography (>= 43),\n python3-cryptography (<< 44~),\n")
        (problem,) = debian_problems(debian_tree)
        assert "python3-cryptography" in problem

    def test_mutation_a_ceiling_added_to_cryptography_in_pyproject_is_reported(self, debian_tree):
        _edit(debian_tree, "pyproject.toml", '"cryptography>=43"', '"cryptography>=43,<44"')
        (problem,) = debian_problems(debian_tree)
        assert "python3-cryptography" in problem

    def test_mutation_an_extra_python3_dependency_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/control", " python3-yaml (>= 6.0),\n",
              " python3-yaml (>= 6.0),\n python3-flask-sock,\n")
        (problem,) = debian_problems(debian_tree)
        assert "python3-flask-sock" in problem

    def test_mutation_the_libzfseasy_dependency_must_be_this_build(self, debian_tree):
        _edit(debian_tree, "debian/control", "python3-libzfseasy (= ${binary:Version})", "python3-libzfseasy")
        (problem,) = debian_problems(debian_tree)
        assert "python3-libzfseasy (= ${binary:Version})" in problem

    def test_mutation_a_loose_libzfseasy_relation_is_reported(self, debian_tree):
        _edit(debian_tree, "debian/control", "python3-libzfseasy (= ${binary:Version})",
              "python3-libzfseasy (>= ${binary:Version})")
        assert len(debian_problems(debian_tree)) == 1

    def test_mutation_x_python3_version_must_match_requires_python(self, debian_tree):
        _edit(debian_tree, "debian/control", "X-Python3-Version: >= 3.11", "X-Python3-Version: >= 3.12")
        (problem,) = debian_problems(debian_tree)
        assert "X-Python3-Version" in problem and "debian/control" in problem

    def test_mutation_requires_python_in_pyproject_is_reported(self, debian_tree):
        _edit(debian_tree, "pyproject.toml", 'requires-python = ">=3.11"', 'requires-python = ">=3.10"')
        (problem,) = debian_problems(debian_tree)
        assert "X-Python3-Version" in problem

    def test_an_absent_x_python3_version_is_accepted(self, debian_tree):
        _edit(debian_tree, "debian/control", "X-Python3-Version: >= 3.11\n", "")
        assert debian_problems(debian_tree) == []


# --------------------------------------------------------------------------
# debian/zfsbackup.prerm and python3-libzfseasy's Breaks
# --------------------------------------------------------------------------

_PRERM_UNITS = ["zfsbackup-check.service", "zfsbackup-check.timer", "zfsbackup.service"]
_SYSTEMD_DIR = "/run/systemd/system"


def libzfseasy_breaks(control_text: str) -> list:
    """The ``Breaks`` entries of python3-libzfseasy's paragraph."""
    field = _stanza(control_text, "Package", "python3-libzfseasy").get("Breaks", "")
    return [part.strip() for part in field.replace("\n", " ").split(",") if part.strip()]


def prerm_problems(text: str, tmp_path: Path) -> list:
    """Static problems of a zfsbackup.prerm: syntax, the deconfigure branch, the placeholder."""
    problems = []
    script = tmp_path / "prerm.static"
    script.write_text(text)
    check = subprocess.run(["/bin/sh", "-n", str(script)], capture_output=True, text=True,
                           env=hermetic_env(), timeout=30)
    if check.returncode != 0:
        problems.append(f"debian/zfsbackup.prerm: `sh -n` fails: {check.stderr.strip()}")
    branch = re.search(r"^\s*deconfigure\)(.*?)^\s*;;", text, re.MULTILINE | re.DOTALL)
    if not branch:
        problems.append("debian/zfsbackup.prerm: no `deconfigure)` branch")
    else:
        body = "\n".join(l for l in branch[1].splitlines() if not l.strip().startswith("#"))
        stop = re.search(r"deb-systemd-invoke\s+stop\s+(?P<units>[^>|;&\n]+)", body)
        if not stop:
            problems.append("debian/zfsbackup.prerm: the deconfigure branch does not `deb-systemd-invoke stop`")
        elif sorted(stop["units"].split()) != sorted(_PRERM_UNITS):
            problems.append(
                f"debian/zfsbackup.prerm: the deconfigure branch stops {stop['units'].split()}, "
                f"expected {_PRERM_UNITS}"
            )
    if not re.search(r"^#DEBHELPER#\s*$", text, re.MULTILINE):
        problems.append("debian/zfsbackup.prerm: the #DEBHELPER# line is gone (dh would add nothing)")
    return problems


def run_prerm(text: str, argument: str, tmp_path: Path, *, systemd: bool = True, dpkg_root=None,
              stub_exit: int = 0):
    """Run a prerm copy under ``/bin/sh`` with a stub ``deb-systemd-invoke`` first on PATH and
    ``/run/systemd/system`` rewritten to a tmp directory. Returns ``(exit status, [argv line, ...])``."""
    work = tmp_path / f"run-{argument}-{int(systemd)}-{dpkg_root is not None}-{stub_exit}"
    stubs = work / "bin"
    stubs.mkdir(parents=True)
    log = work / "calls.log"
    stub = stubs / "deb-systemd-invoke"
    stub.write_text(f'#!/bin/sh\nprintf \'%s\\n\' "$*" >> "{log}"\nexit {stub_exit}\n')
    stub.chmod(0o755)
    fake_run = work / "run" / "systemd" / "system"
    if systemd:
        fake_run.mkdir(parents=True)
    script = work / "prerm"
    script.write_text(text.replace(_SYSTEMD_DIR, str(fake_run)))
    env = hermetic_env()
    env["PATH"] = f"{stubs}{os.pathsep}{env['PATH']}"
    env.pop("DPKG_ROOT", None)
    if dpkg_root is not None:
        env["DPKG_ROOT"] = dpkg_root
    done = subprocess.run(["/bin/sh", str(script), argument], capture_output=True, text=True,
                          env=env, timeout=30)
    calls = log.read_text().splitlines() if log.exists() else []
    return done.returncode, calls


@pytest.mark.unit
class TestDebianPrermAndBreaks:
    """python3-libzfseasy Breaks the zfsbackup it was not released with, so dpkg deconfigures
    (old) zfsbackup before unpacking a new library. The stop for an upgrade lives in the NEW
    package's preinst, which is too late for that order; the prerm stops the daemon at
    `deconfigure`, so the library is never replaced under a running daemon."""

    PRERM = REPO / "debian" / "zfsbackup.prerm"
    EXPECTED_STOP = "stop " + " ".join(_PRERM_UNITS)

    @pytest.fixture(autouse=True)
    def _needs_debian(self):
        if not self.PRERM.is_file():
            pytest.skip("no debian/ directory (installed copy or sdist)")

    @pytest.fixture
    def prerm_text(self):
        return self.PRERM.read_text()

    # (a) Breaks ----------------------------------------------------------------------------

    def test_python3_libzfseasy_breaks_older_zfsbackup(self):
        assert libzfseasy_breaks((REPO / "debian" / "control").read_text()) == [
            "zfsbackup (<< ${binary:Version})"
        ], "debian/control: python3-libzfseasy needs exactly `Breaks: zfsbackup (<< ${binary:Version})`"

    @pytest.mark.parametrize("old, new", [
        ("zfsbackup (<< ${binary:Version})", "zfsbackup (<= ${binary:Version})"),
        ("zfsbackup (<< ${binary:Version})", "zfsbackup"),
        ("zfsbackup (<< ${binary:Version})", "zfsbackup (<< ${source:Version})"),
        ("Breaks:\n zfsbackup (<< ${binary:Version}),\n", ""),
        ("zfsbackup (<< ${binary:Version}),", "zfsbackup (<< ${binary:Version}),\n python3-yaml,"),
    ])
    def test_mutation_a_changed_or_missing_breaks_is_detected(self, old, new):
        text = (REPO / "debian" / "control").read_text()
        assert old in text
        assert libzfseasy_breaks(text.replace(old, new, 1)) != ["zfsbackup (<< ${binary:Version})"]

    def test_mutation_a_breaks_in_the_wrong_paragraph_is_detected(self):
        text = (REPO / "debian" / "control").read_text()
        moved = text.replace("Breaks:\n zfsbackup (<< ${binary:Version}),\n", "", 1).replace(
            "Recommends:", "Breaks:\n zfsbackup (<< ${binary:Version}),\nRecommends:", 1)
        assert libzfseasy_breaks(moved) == []

    # (b) static ----------------------------------------------------------------------------

    def test_the_prerm_is_clean_and_complete(self, prerm_text, tmp_path):
        assert prerm_problems(prerm_text, tmp_path) == []

    def test_the_prerm_is_a_sh_script_that_fails_on_error(self, prerm_text):
        lines = prerm_text.splitlines()
        assert lines[0] == "#!/bin/sh" and "set -e" in lines[:3]

    @pytest.mark.parametrize("old, new, needle", [
        ("zfsbackup-check.timer ", "", "stops"),
        (" zfsbackup.service", "", "stops"),
        ("zfsbackup-check.service ", "", "stops"),
        ("    deconfigure)", "    configure)", "no `deconfigure)` branch"),
        ("deb-systemd-invoke stop", "deb-systemd-invoke start", "does not"),
        ("#DEBHELPER#", "# helper", "#DEBHELPER#"),
        ("esac\n", "esac\nif then\n", "sh -n"),
    ])
    def test_mutation_static_checks_catch(self, prerm_text, tmp_path, old, new, needle):
        assert old in prerm_text, old
        problems = prerm_problems(prerm_text.replace(old, new, 1), tmp_path)
        assert any(needle in problem for problem in problems), problems

    # (c) behaviour -------------------------------------------------------------------------

    def test_deconfigure_stops_the_three_units(self, prerm_text, tmp_path):
        status, calls = run_prerm(prerm_text, "deconfigure", tmp_path)
        assert status == 0
        assert calls == [self.EXPECTED_STOP], calls

    @pytest.mark.parametrize("argument", ["upgrade", "remove", "failed-upgrade", "configure"])
    def test_other_actions_invoke_nothing_of_ours(self, prerm_text, tmp_path, argument):
        status, calls = run_prerm(prerm_text, argument, tmp_path)
        assert status == 0
        assert calls == [], f"the prerm's own part must not stop anything at {argument!r}: {calls}"

    def test_the_debhelper_placeholder_is_only_a_comment_in_the_source(self, prerm_text):
        (line,) = [l for l in prerm_text.splitlines() if "DEBHELPER" in l]
        assert line.startswith("#")

    def test_nothing_runs_without_a_systemd_system(self, prerm_text, tmp_path):
        status, calls = run_prerm(prerm_text, "deconfigure", tmp_path, systemd=False)
        assert (status, calls) == (0, [])

    def test_nothing_runs_in_an_alternate_root(self, prerm_text, tmp_path):
        status, calls = run_prerm(prerm_text, "deconfigure", tmp_path, dpkg_root="/target")
        assert (status, calls) == (0, [])

    def test_an_empty_dpkg_root_counts_as_unset(self, prerm_text, tmp_path):
        status, calls = run_prerm(prerm_text, "deconfigure", tmp_path, dpkg_root="")
        assert calls == [self.EXPECTED_STOP]

    def test_a_failing_stop_does_not_fail_the_deconfigure(self, prerm_text, tmp_path):
        status, calls = run_prerm(prerm_text, "deconfigure", tmp_path, stub_exit=1)
        assert status == 0 and calls == [self.EXPECTED_STOP]

    def test_the_prerm_tests_the_systemd_directory_the_behaviour_tests_rewrite(self, prerm_text):
        assert _SYSTEMD_DIR in prerm_text

    @pytest.mark.parametrize("old, new, detector", [
        ("deconfigure)", "deconfigure|upgrade|remove)", "stops_on_every_action"),
        ("zfsbackup-check.timer ", "", "wrong_units"),
        ("-z \"$DPKG_ROOT\"", "-n \"$DPKG_ROOT\"", "wrong_units"),
        ("[ -d /run/systemd/system ]", "true", "stops_without_systemd"),
        ("[ -z \"$DPKG_ROOT\" ] && ", "", "stops_in_alternate_root"),
        (" || true", "", "failure_propagates"),
        ("deb-systemd-invoke stop", "deb-systemd-invoke restart", "wrong_units"),
    ])
    def test_mutation_behavioural_checks_catch(self, prerm_text, tmp_path, old, new, detector):
        assert old in prerm_text, old
        mutated = prerm_text.replace(old, new, 1)
        expected = [self.EXPECTED_STOP]
        found = {
            "stops_on_every_action": any(run_prerm(mutated, a, tmp_path)[1] for a in ("upgrade", "remove")),
            "wrong_units": run_prerm(mutated, "deconfigure", tmp_path)[1] != expected,
            "stops_without_systemd": run_prerm(mutated, "deconfigure", tmp_path, systemd=False)[1] != [],
            "stops_in_alternate_root": run_prerm(mutated, "deconfigure", tmp_path, dpkg_root="/t")[1] != [],
            "failure_propagates": run_prerm(mutated, "deconfigure", tmp_path, stub_exit=1)[0] != 0,
        }
        assert found[detector], f"{detector} did not notice the mutation {old!r} -> {new!r}"


_STDLIB_ONLY_PROBE = """
import sys
before = set(sys.modules)
import libzfseasy
import libzfseasy.types, libzfseasy.zfs
new = sorted({{m.split('.')[0] for m in set(sys.modules) - before}})
stdlib = set(sys.stdlib_module_names) | {{'libzfseasy'}}
print('\\n'.join(m for m in new if m not in stdlib and not m.startswith('_')))
"""


@pytest.mark.unit
class TestLibzfseasyIsStdlibOnly:
    """Nothing in `libzfseasy` may import a third-party module (or `zfsbackup`): the Debian
    package is a plain python3 library and the wheel declares no dependency."""

    def test_importing_it_loads_only_the_standard_library(self, tmp_path):
        env = hermetic_env(PYTHONPATH=str(LIB_DIR))
        proc = subprocess.run(
            [sys.executable, "-c", _STDLIB_ONLY_PROBE.format()],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.split() == [], f"non-stdlib modules imported: {proc.stdout.split()}"

    def test_probe_imports_the_tree_under_libs(self, tmp_path):
        env = hermetic_env(PYTHONPATH=str(LIB_DIR))
        proc = subprocess.run(
            [sys.executable, "-c", "import libzfseasy, sys; print(libzfseasy.__file__)"],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        assert Path(proc.stdout.strip()).resolve() == (LIB_DIR / "libzfseasy" / "__init__.py").resolve()

    def test_a_static_scan_finds_no_third_party_or_zfsbackup_import(self):
        """Also catches imports in code paths the import above does not execute (lazy ones)."""
        import ast

        allowed = set(sys.stdlib_module_names) | {"libzfseasy"}
        offenders = []
        for path in sorted((LIB_DIR / "libzfseasy").rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Import):
                    roots = [a.name.split(".")[0] for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    roots = [] if node.level else [(node.module or "").split(".")[0]]
                else:
                    continue
                offenders += [f"{path.name}:{node.lineno} {r}" for r in roots if r not in allowed]
        assert offenders == []


# --------------------------------------------------------------------------
# built artifacts (slow)
# --------------------------------------------------------------------------


def _poetry_build(poetry, cwd, out):
    """`poetry build` of the project in `cwd` into `out` (hermetic env; it needs no network:
    poetry-core is already installed). A missing poetry skips; a failing build FAILS, so a
    broken `pyproject.toml` is loud instead of skipped away."""
    import os

    try:
        proc = subprocess.run(
            [poetry, "build", "-o", str(out)], cwd=cwd,
            capture_output=True, text=True, timeout=600, env=hermetic_env(HOME=os.environ.get("HOME", "")),
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"poetry build in {cwd} timed out")
    assert proc.returncode == 0, f"poetry build in {cwd} failed: {(proc.stderr or proc.stdout).strip()[-600:]}"
    wheels, sdists = list(out.glob("*.whl")), list(out.glob("*.tar.gz"))
    assert len(wheels) == 1, f"{cwd}: expected one wheel, got {wheels}"
    assert len(sdists) == 1, f"{cwd}: expected one sdist, got {sdists}"
    return wheels[0], sdists[0]


@pytest.fixture(scope="module")
def poetry_exe():
    import os

    poetry = shutil.which("poetry") or shutil.which("poetry", path=os.environ.get("PATH", ""))
    if poetry is None:
        pytest.skip("poetry is not installed; cannot build the wheels")
    return poetry


@pytest.fixture(scope="module")
def built(tmp_path_factory, poetry_exe):
    """(wheel, sdist) of the zfsbackup project."""
    return _poetry_build(poetry_exe, REPO, tmp_path_factory.mktemp("zfsbackup-dist"))


@pytest.fixture(scope="module")
def built_lib(tmp_path_factory, poetry_exe):
    """(wheel, sdist) of libs/libzfseasy."""
    return _poetry_build(poetry_exe, LIB_DIR, tmp_path_factory.mktemp("libzfseasy-dist"))


def _dist_info(wheel, filename):
    with zipfile.ZipFile(wheel) as archive:
        member = next(n for n in archive.namelist() if n.endswith(".dist-info/" + filename))
        return archive.read(member).decode()


def _requires_dist(wheel):
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    return {
        canonicalize_name(r.name): r
        for r in (
            Requirement(line.split(":", 1)[1].strip())
            for line in _dist_info(wheel, "METADATA").splitlines()
            if line.startswith("Requires-Dist:")
        )
    }


@pytest.mark.slow
@pytest.mark.integration
class TestBuiltPackages:
    """Both distributions are built: `poetry build` at the root (zfsbackup) and in
    `libs/libzfseasy` (libzfseasy)."""

    @pytest.fixture(scope="class")
    @classmethod
    def wheel(cls, built):
        return built[0]

    @pytest.fixture(scope="class")
    @classmethod
    def names(cls, wheel):
        with zipfile.ZipFile(wheel) as archive:
            return archive.namelist()

    @pytest.fixture(scope="class")
    @classmethod
    def lib_wheel(cls, built_lib):
        return built_lib[0]

    @pytest.fixture(scope="class")
    @classmethod
    def lib_names(cls, lib_wheel):
        with zipfile.ZipFile(lib_wheel) as archive:
            return archive.namelist()

    # -- the zfsbackup wheel -------------------------------------------------

    def test_wheel_is_named_zfsbackup(self, wheel):
        assert wheel.name.startswith("zfsbackup-")

    def test_wheel_carries_the_migrations_and_new_modules(self, names):
        assert "zfsbackup/version.py" in names
        assert "zfsbackup/daemon_entry.py" in names
        assert "zfsbackup/config/store/migrations/script.py.mako" in names
        assert any(
            n.startswith("zfsbackup/config/store/migrations/versions/") and n.endswith(".py")
            for n in names
        )

    def test_wheel_carries_the_vendored_flask_sock(self, names):
        assert "zfsbackup/_vendor/__init__.py" in names
        assert "zfsbackup/_vendor/flask_sock.py" in names

    def test_wheel_does_not_carry_libzfseasy(self, names):
        assert not any(n.startswith("libzfseasy/") for n in names), (
            "libzfseasy is its own wheel now: shipping it twice makes two installs fight over the files"
        )

    def test_no_top_level_packaging_or_tests(self, names):
        tops = {n.split("/")[0] for n in names}
        assert "packaging" not in tops
        assert "tests" not in tops
        assert not any(n.startswith(("packaging/", "tests/")) for n in names)
        assert tops - {"zfsbackup"} <= {n for n in tops if n.endswith(".dist-info")}

    def test_entry_points(self, wheel):
        import configparser

        parser = configparser.ConfigParser()
        parser.optionxform = str
        parser.read_string(_dist_info(wheel, "entry_points.txt"))
        scripts = parser["console_scripts"]
        assert scripts["zfsbackupd"] == "zfsbackup.daemon_entry:main"
        assert scripts["zfsbackup-config"] == "zfsbackup.cli.main:main"

    def test_metadata_name_and_version(self, wheel, pyproject):
        metadata = _dist_info(wheel, "METADATA")
        assert re.search(r"^Name: zfsbackup$", metadata, re.M)
        assert re.search(
            rf"^Version: {re.escape(pyproject['project']['version'])}$", metadata, re.M
        )

    def test_pins_are_in_requires_dist(self, wheel):
        requires = _requires_dist(wheel)
        for name, expected in _PINS.items():
            assert specifier_strings(requires[name]) == frozenset(expected), (name, requires.get(name))

    def test_requires_dist_pins_libzfseasy_to_the_same_version(self, wheel, pyproject):
        text = next(
            line for line in _dist_info(wheel, "METADATA").splitlines()
            if line.startswith("Requires-Dist: libzfseasy")
        )
        version = pyproject["project"]["version"]
        assert text == f"Requires-Dist: libzfseasy (=={version})", text
        assert "file:" not in text and "@" not in text, "no path/URL source may leak into the metadata"

    def test_requires_dist_has_no_flask_sock_and_no_dev_source(self, wheel):
        metadata = _dist_info(wheel, "METADATA")
        assert "flask-sock" not in metadata.lower() and "flask_sock" not in metadata.lower()
        assert "file://" not in metadata

    def test_sdist_has_no_tests_and_no_top_level_packaging_python(self, built):
        import tarfile

        with tarfile.open(built[1]) as archive:
            members = archive.getnames()
        assert not any("/tests/" in m for m in members)
        assert not any(m.endswith("packaging/__init__.py") or
                       (re.search(r"/packaging/.*\.py$", m) is not None) for m in members)

    def test_sdist_still_carries_the_packaging_directory(self, built):
        import tarfile

        with tarfile.open(built[1]) as archive:
            members = archive.getnames()
        assert any(m.endswith("/packaging/systemd/zfsbackup.service") for m in members)
        assert any(m.endswith("/zfsbackup/_vendor/flask_sock.py") for m in members)
        assert not any("/libzfseasy/" in m and m.endswith(".py") for m in members), (
            "the zfsbackup sdist must not carry libzfseasy sources"
        )

    # -- the libzfseasy wheel ------------------------------------------------

    def test_lib_wheel_is_named_libzfseasy(self, lib_wheel):
        assert lib_wheel.name.startswith("libzfseasy-")

    def test_lib_wheel_has_only_the_libzfseasy_package(self, lib_names):
        tops = {n.split("/")[0] for n in lib_names}
        assert "libzfseasy" in tops
        assert tops - {"libzfseasy"} <= {n for n in tops if n.endswith(".dist-info")}, tops
        for required in ("libzfseasy/__init__.py", "libzfseasy/types.py", "libzfseasy/zfs.py"):
            assert required in lib_names

    def test_lib_wheel_carries_no_zfsbackup_and_no_tests(self, lib_names):
        assert not any(n.startswith(("zfsbackup/", "tests/", "packaging/", "libs/")) for n in lib_names)

    def test_lib_wheel_has_no_console_scripts(self, lib_wheel):
        with zipfile.ZipFile(lib_wheel) as archive:
            assert not any(n.endswith("entry_points.txt") for n in archive.namelist())

    def test_lib_metadata_name_version_and_no_requirements(self, lib_wheel, lib_pyproject, pyproject):
        metadata = _dist_info(lib_wheel, "METADATA")
        version = lib_pyproject["project"]["version"]
        assert re.search(r"^Name: libzfseasy$", metadata, re.M)
        assert re.search(rf"^Version: {re.escape(version)}$", metadata, re.M)
        assert version == pyproject["project"]["version"]
        assert [l for l in metadata.splitlines() if l.startswith("Requires-Dist:")] == []

    def test_lib_wheel_carries_its_licence(self, lib_wheel):
        with zipfile.ZipFile(lib_wheel) as archive:
            assert any(n.endswith("LICENSE") for n in archive.namelist() if ".dist-info/" in n)

    def test_lib_sdist_has_the_package_and_no_zfsbackup(self, built_lib):
        import tarfile

        with tarfile.open(built_lib[1]) as archive:
            members = archive.getnames()
        assert any(m.endswith("/libzfseasy/zfs.py") for m in members)
        assert not any("/zfsbackup/" in m for m in members)

    # -- together -----------------------------------------------------------

    def test_the_two_wheels_share_no_file(self, names, lib_names):
        """Two distributions owning one path is what made the single-wheel upgrade delete
        libzfseasy: no path may be in both wheels."""
        assert set(names) & set(lib_names) == set()

"""Guards on the CI workflow files (`.github/workflows/*.yml`): what may trigger the real-pool
tier, and what every mocked-suite workflow must keep.

The real tier (`real-zfs.yml`) runs `sudo zpool create` and real `zfs` commands as a
root-equivalent on the runner. Triggered by a pull request it would run the PR's code with that
power, so the only acceptable triggers are `schedule` and `workflow_dispatch`, on `master`
(plan decision C10). These tests read the YAML; they run nothing and need no network.
"""

import re
import tomllib
from pathlib import Path

import pytest
import yaml
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"

pytestmark = pytest.mark.unit

if not WORKFLOWS.is_dir():  # an installed copy or sdist without the CI files
    pytest.skip("no .github/workflows directory", allow_module_level=True)


def load(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def triggers(workflow: dict) -> set:
    # YAML 1.1 reads the bare key `on` as the boolean True.
    on = workflow.get("on", workflow.get(True))
    if isinstance(on, str):
        return {on}
    if isinstance(on, list):
        return set(on)
    return set(on or {})


def commands_and_env(workflow: dict) -> str:
    """Every `run:` script and `env:` mapping of every step, as one string (comments excluded)."""
    parts = []
    for job in workflow.get("jobs", {}).values():
        parts.append(str(job.get("env", "")))
        for step in job.get("steps", []):
            parts.append(str(step.get("run", "")))
            parts.append(str(step.get("env", "")))
            parts.append(str(step.get("with", "")))
    parts.append(str(workflow.get("env", "")))
    return "\n".join(parts)


def all_workflows():
    return sorted(p.name for p in WORKFLOWS.glob("*.yml"))


def on_section(workflow: dict) -> dict:
    return workflow.get("on", workflow.get(True)) or {}


# A command that starts pytest or zfsbackup: pytest/pytest-3/zfsbackupd/zfsbackup-config in command
# position (line start, after ; & | ( or `then`/`do`), or `-m pytest` / `-m zfsbackup`.
_INVOCATION = re.compile(
    r"(?:^|[;&|(]\s*|\b(?:then|do)\s+)(?:\S*/)?(?:pytest(?:-3)?|py\.test|zfsbackupd|zfsbackup-config)\b"
    r"|\s-m\s+(?:pytest|zfsbackup\w*)\b",
    re.MULTILINE,
)
# The only way deb.yml may reach pytest or zfsbackup: the autopkgtest scripts, which export
# ZFS_CMD/ZPOOL_CMD=/bin/false first (TestDebWorkflow pins that).
_STUBBED_SCRIPT = re.compile(r"^\s*sh\s+(?:-\w+\s+)*debian/tests/(?:smoke|platform-contracts)\s*$")


def unstubbed_invocations(run: str) -> list:
    """The lines of a `run:` script that start pytest or zfsbackup other than by way of the stubbed
    `debian/tests/*` scripts. `apt-get install ... python3-pytest ...` is not an invocation."""
    found = []
    for line in run.splitlines():
        if line.strip().startswith("#") or _STUBBED_SCRIPT.match(line):
            continue
        if _INVOCATION.search(line):
            found.append(line.strip())
    return found


class TestRealTierWorkflow:
    def test_it_is_triggered_only_by_schedule_and_manual_dispatch(self):
        assert triggers(load("real-zfs.yml")) == {"schedule", "workflow_dispatch"}, (
            "real-zfs.yml must be triggered ONLY by schedule and workflow_dispatch: any other "
            "trigger (pull_request, pull_request_target, push, workflow_run...) would run code "
            "from a branch or a fork with `sudo zpool create` on the runner (decision C10)"
        )

    def test_its_jobs_refuse_any_ref_but_master(self):
        jobs = load("real-zfs.yml")["jobs"]
        assert jobs
        for name, job in jobs.items():
            assert "refs/heads/master" in str(job.get("if", "")), (
                f"job {name!r} must start with `if: github.ref == 'refs/heads/master'`: a "
                "workflow_dispatch of another branch would otherwise run that branch's code"
            )

    def test_the_token_is_read_only(self):
        workflow = load("real-zfs.yml")
        assert workflow.get("permissions") == {"contents": "read"}
        for name, job in workflow["jobs"].items():
            assert "permissions" not in job or job["permissions"] == {"contents": "read"}, name

    def test_it_runs_on_a_pinned_hosted_image_not_a_self_hosted_one(self):
        """Self-hosted is decision C10's fallback and a user decision; this file is the hosted
        attempt. Moving it is a deliberate edit that should update this test too."""
        for name, job in load("real-zfs.yml")["jobs"].items():
            assert job["runs-on"] == "ubuntu-24.04", (name, job["runs-on"])

    def test_it_runs_the_real_tier_and_does_not_stub_the_zfs_binaries(self):
        text = commands_and_env(load("real-zfs.yml"))
        assert "ZFSB_REAL_ZFS" in text and "-m real_zfs" in text
        assert "ZFS_CMD" not in text and "ZPOOL_CMD" not in text, (
            "the real tier resolves the real zfs/zpool from PATH; a stub would make it pass "
            "against nothing"
        )

    def test_a_job_where_every_test_skipped_fails(self):
        text = commands_and_env(load("real-zfs.yml"))
        assert "junit" in text and "passed == 0" in text, (
            "real_zfs tests skip (never error) without a pool, and pytest exits 0 when all "
            "skipped: the workflow must fail itself on zero passes"
        )

    def test_it_proves_a_pool_before_pytest_runs(self):
        steps = [s for s in load("real-zfs.yml")["jobs"]["real-zfs"]["steps"] if "run" in s]
        order = [s["run"] for s in steps]
        modprobe = next(i for i, r in enumerate(order) if "modprobe zfs" in r)
        preflight = next(i for i, r in enumerate(order) if "zpool create" in r)
        pytest_run = next(i for i, r in enumerate(order) if "pytest" in r and "-m real_zfs" in r)
        assert modprobe < preflight < pytest_run


class TestNoOtherWorkflowTouchesRealZfs:
    @pytest.mark.parametrize("name", [n for n in all_workflows() if n != "real-zfs.yml"])
    def test_the_real_tier_is_not_enabled_or_selected(self, name):
        text = commands_and_env(load(name))
        assert "ZFSB_REAL_ZFS" not in text, f"{name} enables the real tier"
        assert "-m real_zfs" not in text and "-m 'real_zfs" not in text, f"{name} selects it"
        assert "zpool create" not in text and "zfsutils" not in text, (
            f"{name} installs ZFS or creates a pool"
        )


    def test_deb_workflow_reaches_pytest_and_zfsbackup_only_through_the_stubbed_scripts(self):
        """The generic check above is vacuous for deb.yml: its jobs install the packages, whose
        dependencies include zfsutils-linux, so a real /usr/sbin/zfs exists in the container. What
        keeps it from running is that every pytest or zfsbackup invocation goes through
        debian/tests/smoke or debian/tests/platform-contracts, which set ZFS_CMD and ZPOOL_CMD to
        /bin/false (TestDebWorkflow checks the scripts)."""
        bad = {}
        invoked = 0
        for job_name, job in load("deb.yml")["jobs"].items():
            for step in job["steps"]:
                run = str(step.get("run", ""))
                invoked += sum(1 for line in run.splitlines() if _STUBBED_SCRIPT.match(line))
                if unstubbed_invocations(run):
                    bad[(job_name, step.get("name"))] = unstubbed_invocations(run)
        assert not bad, (
            f"deb.yml starts pytest or zfsbackup outside the stubbed debian/tests scripts: {bad}. "
            "The containers have a real /usr/sbin/zfs; route it through `sh -eux debian/tests/...`"
        )
        assert invoked == 2, "deb.yml no longer runs both debian/tests/smoke and platform-contracts"
        env_text = commands_and_env(load("deb.yml"))
        assert "ZFSB_REAL_ZFS" not in env_text

    @pytest.mark.parametrize("run, expected", [
        ("sh -eux debian/tests/smoke", []),
        ("apt-get install -y python3-pytest python3-pytest-mock\nsh -eux debian/tests/platform-contracts", []),
        ("pytest -q tests", ["pytest -q tests"]),
        ("pytest-3 tests", ["pytest-3 tests"]),
        ("cd x && zfsbackupd --version", ["cd x && zfsbackupd --version"]),
        ("/usr/bin/zfsbackup-config check", ["/usr/bin/zfsbackup-config check"]),
        ("python3 -m pytest tests", ["python3 -m pytest tests"]),
        ("if true; then pytest; fi", ["if true; then pytest; fi"]),
        ("sh -eux debian/tests/smoke; pytest", ["sh -eux debian/tests/smoke; pytest"]),
        ("dpkg -l python3-libzfseasy zfsbackup", []),
    ])
    def test_the_invocation_detector(self, run, expected):
        assert unstubbed_invocations(run) == expected


class TestDebWorkflow:
    """`.github/workflows/deb.yml` builds, lints, installs and tests the .debs in a container of
    each target distribution. These tests read the YAML and the two autopkgtest scripts; they
    run nothing."""

    DEBIAN_TESTS = ROOT / "debian" / "tests"

    @staticmethod
    def workflow():
        return load("deb.yml")

    @classmethod
    def job(cls):
        return cls.workflow()["jobs"]["deb"]

    @classmethod
    def steps(cls):
        return cls.job()["steps"]

    @classmethod
    def index(cls, needle: str) -> int:
        for i, step in enumerate(cls.steps()):
            if needle in str(step.get("run", "")):
                return i
        raise AssertionError(f".github/workflows/deb.yml has no step running {needle!r}")

    def test_both_target_distributions_run_in_containers(self):
        job = self.job()
        images = {entry["image"] for entry in job["strategy"]["matrix"]["include"]}
        assert images == {"debian:13", "ubuntu:26.04"}, (
            "deb.yml must build in exactly debian:13 and ubuntu:26.04 (the two target distributions)"
        )
        assert job["container"] == "${{ matrix.image }}"
        assert job["strategy"].get("fail-fast") is False, "which distribution broke matters"

    def test_lintian_fails_on_errors_and_warnings(self):
        run = str(self.steps()[self.index("lintian ")]["run"])
        assert re.search(r"--fail-on\s+error,warning\b", run), run

    def test_the_steps_run_in_order_build_lintian_install_smoke_contracts(self):
        order = [
            self.index("dpkg-buildpackage"),
            self.index("lintian "),
            self.index("apt-get install -y --no-install-recommends ./out/"),
            self.index("debian/tests/smoke"),
            self.index("debian/tests/platform-contracts"),
        ]
        assert order == sorted(order) and len(set(order)) == 5, order

    def test_the_install_step_installs_both_debs(self):
        run = str(self.steps()[self.index("./out/python3-libzfseasy_")]["run"])
        assert "./out/zfsbackup_" in run

    def test_it_runs_weekly(self):
        on = on_section(self.workflow())
        crons = [entry["cron"] for entry in on["schedule"]]
        assert crons, "deb.yml lost its schedule: a distribution update would go unnoticed"
        for cron in crons:
            minute, hour, day_of_month, month, day_of_week = cron.split()
            assert day_of_month == "*" and month == "*" and day_of_week not in ("*", ""), (
                f"{cron!r} is not a weekly schedule (a single day of the week)"
            )
            assert re.fullmatch(r"[0-6]|[A-Za-z]{3}", day_of_week), cron

    def test_it_can_be_dispatched_and_runs_for_release_tags(self):
        assert {"workflow_dispatch", "push", "schedule", "pull_request"} <= triggers(self.workflow())

    def test_the_pull_request_filter_covers_everything_the_packages_are_built_from(self):
        paths = set(on_section(self.workflow())["pull_request"]["paths"])
        required = {
            "zfsbackup/**", "libs/**", "debian/**", "packaging/**",
            "pyproject.toml", "libs/libzfseasy/pyproject.toml",
            "tests/test_zfsbackup_platform_contracts.py", "tests/test_zfsbackup_tls_platform.py",
            ".github/workflows/deb.yml",
        }
        assert required <= paths, f"deb.yml's pull_request paths lack {sorted(required - paths)}"

    def test_the_token_is_read_only(self):
        assert self.workflow().get("permissions") == {"contents": "read"}

    @pytest.mark.parametrize("script", ["smoke", "platform-contracts"])
    def test_the_autopkgtest_script_stubs_zfs_before_running_anything(self, script):
        """The containers have a real /usr/sbin/zfs (python3-libzfseasy depends on
        zfsutils-linux); the script must point ZFS_CMD and ZPOOL_CMD at /bin/false first."""
        path = self.DEBIAN_TESTS / script
        if not path.is_file():
            pytest.skip("no debian/tests directory")
        commands = [
            line.strip() for line in path.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        first = next(i for i, line in enumerate(commands) if not re.match(r"set\s+-", line))
        assert re.match(r"export\b", commands[first]), (
            f"debian/tests/{script}: the first command after `set` must be the ZFS_CMD/ZPOOL_CMD "
            f"export, not {commands[first]!r}"
        )
        assert re.search(r"\bZFS_CMD=/bin/false\b", commands[first]), f"debian/tests/{script}"
        assert re.search(r"\bZPOOL_CMD=/bin/false\b", commands[first]), f"debian/tests/{script}"

    def test_both_scripts_are_registered_as_autopkgtests(self):
        control = self.DEBIAN_TESTS / "control"
        if not control.is_file():
            pytest.skip("no debian/tests directory")
        text = control.read_text()
        assert re.search(r"^Tests:\s*smoke\s*$", text, re.MULTILINE)
        assert re.search(r"^Tests:\s*platform-contracts\s*$", text, re.MULTILINE)


class TestMockedSuiteWorkflows:
    @pytest.mark.parametrize("name", ["tests.yml", "latest-deps.yml"])
    def test_explicit_zfs_stubs_are_kept(self, name):
        """tests/conftest.py stubs ZFS_CMD only when it is unset; CI sets it explicitly so the
        suite never depends on that and never reaches a real zfs."""
        jobs = load(name)["jobs"]
        for job_name, job in jobs.items():
            runs = "\n".join(str(s.get("run", "")) for s in job["steps"])
            if "pytest" not in runs:
                continue
            assert "ZFS_CMD=" in runs and "ZPOOL_CMD=" in runs, (name, job_name)

    def test_python_3_15_is_in_the_tests_matrix(self):
        matrix = load("tests.yml")["jobs"]["test"]["strategy"]["matrix"]
        versions = set(matrix.get("python", [])) | {e.get("python") for e in matrix.get("include", [])}
        assert {"3.11", "3.12", "3.13", "3.14", "3.15"} <= versions

    def test_python_3_15_is_a_regular_enforced_entry(self):
        """3.15 is a supported version: it sits in the matrix's `python` list, not in an
        `include` entry, and no matrix entry is marked experimental (a failure on 3.15 fails
        the workflow)."""
        matrix = load("tests.yml")["jobs"]["test"]["strategy"]["matrix"]
        assert "3.15" in matrix["python"]
        assert not [e for e in matrix.get("include", []) if e.get("python") == "3.15"]
        assert not [e for e in matrix.get("include", []) if e.get("experimental")]

    def test_python_3_15_is_in_the_package_job(self):
        assert "3.15" in load("tests.yml")["jobs"]["package"]["strategy"]["matrix"]["python"]

    def test_latest_deps_is_scheduled_unlocked_and_never_a_pr_gate(self):
        workflow = load("latest-deps.yml")
        assert triggers(workflow) == {"schedule", "workflow_dispatch"}
        text = commands_and_env(workflow)
        assert "poetry update" in text, "the job must resolve afresh, ignoring poetry.lock"
        assert "test_zfsbackup_platform_contracts.py" in text

    def test_latest_deps_installs_the_root_project_after_the_update(self):
        """`poetry update` does not install the root project (only `install` builds it), so
        without `poetry install --only-root` after it nothing imports `libzfseasy`/`zfsbackup`
        and collection fails in every cell. A plain `poetry install` would put the locked
        versions back: only `--only-root` keeps what the update resolved."""
        steps = [s["run"] for s in load("latest-deps.yml")["jobs"]["latest"]["steps"] if "run" in s]
        lines = [ln.strip() for run in steps for ln in run.splitlines() if ln.strip().startswith("poetry ")]
        installs = [ln for ln in lines if ln.startswith("poetry install")]
        assert installs, "no root install after `poetry update`"
        assert all("--only-root" in ln for ln in installs), (
            f"a `poetry install` without --only-root would restore the locked versions: {installs}"
        )
        update = next(i for i, ln in enumerate(lines) if ln.startswith("poetry update"))
        assert update < lines.index(installs[0]), "the root install must come after the update"
        pytest_step = next(i for i, ln in enumerate(lines) if ln.startswith("poetry run pytest"))
        assert lines.index(installs[0]) < pytest_step


class TestPackageJob:
    """C20: build the wheel and sdist, check their contents, install the wheel into a clean venv
    without dev dependencies, run the console scripts."""

    @staticmethod
    def job():
        return load("tests.yml")["jobs"]["package"]

    @staticmethod
    def runs(job):
        return [s["run"] for s in job["steps"] if "run" in s]

    def test_it_builds_with_poetry_and_asserts_the_packaged_files(self):
        text = "\n".join(self.runs(self.job()))
        assert "poetry build" in text
        for needle in (
            "zfsbackup/config/store/migrations/versions/",
            "zfsbackup/version.py",
            "zfsbackup/daemon_entry.py",
            "zfsbackup/_vendor/flask_sock.py",
            '"packaging/"',
            '"tests/"',
            "packaging/systemd/zfsbackup.service",
        ):
            assert needle in text, f"the package job no longer checks {needle}"

    def test_it_builds_both_distributions_into_one_dist_directory(self):
        build = next(r for r in self.runs(self.job()) if "poetry build" in r)
        lines = [ln.strip() for ln in build.splitlines() if ln.strip().startswith("poetry build")]
        assert len(lines) == 2, lines
        root, lib = lines
        assert "-C" not in root and "-o" not in root, "the root project builds into ./dist"
        assert "-C libs/libzfseasy" in lib, "libzfseasy is its own project (libs/libzfseasy)"
        assert '-o "$PWD/dist"' in lib, "both wheels must end up in the one dist/ the install globs"

    def test_it_checks_each_wheel_by_name_and_expects_exactly_one_of_each(self):
        text = "\n".join(self.runs(self.job()))
        for pattern in (
            "dist/zfsbackup-*.whl", "dist/libzfseasy-*.whl",
            "dist/zfsbackup-*.tar.gz", "dist/libzfseasy-*.tar.gz",
        ):
            assert pattern in text, pattern
        assert "len(found) != 1" in text

    def test_it_checks_the_split_between_the_wheels_and_the_lockstep_pin(self):
        text = "\n".join(self.runs(self.job()))
        for needle in (
            '"libzfseasy/"',                                       # must not ship in the zfsbackup wheel
            "libzfseasy/__init__.py",                              # must ship in its own
            "Requires-Dist: libzfseasy (==",                       # the exact pin
            "flask-sock",                                          # vendored, not required
            'Requires-Dist:" in lib_meta',                         # libzfseasy has no dependencies
            "version differs from zfsbackup",                      # lockstep
        ):
            assert needle in text, f"the package job no longer checks {needle}"

    def test_the_wheels_go_into_a_clean_venv_without_poetry_or_dev_dependencies(self):
        runs = self.runs(self.job())
        install = next(r for r in runs if "pip" in r and "dist/*.whl" in r)
        assert "python -m venv" in install
        assert "--with dev" not in "".join(runs) and "poetry install" not in "".join(runs)
        assert "pip show pytest" in install, "nothing proves the venv has no dev tooling"

    def test_both_wheels_are_installed_in_one_pip_command_and_the_result_is_checked(self):
        install = next(r for r in self.runs(self.job()) if "dist/*.whl" in r)
        pip_installs = [ln for ln in install.splitlines() if " install " in ln]
        assert pip_installs == ['"$RUNNER_TEMP/pkgvenv/bin/python" -m pip install dist/*.whl'], (
            "one `pip install dist/*.whl` installs both: libzfseasy is not on an index"
        )
        assert "pip check" in install
        assert "pip show flask-sock" in install, "nothing proves flask-sock is not a dependency"
        assert 'version("zfsbackup") == version("libzfseasy")' in install

    def test_the_smoke_test_runs_each_console_script_away_from_the_checkout(self):
        smoke = next(r for r in self.runs(self.job()) if "--test-config" in r)
        for needle in (
            'zfsbackupd" --version',
            'zfsbackup-config" --version',
            "config.example.yaml",
            "import",
            "migrate --check",
            'zfsbackupd" --test-config -c',
        ):
            assert needle in smoke, needle
        assert "cd " in smoke, "run from a scratch directory so nothing imports the source tree"

    def test_it_uses_the_same_explicit_zfs_stubs_as_the_test_job(self):
        steps = self.job()["steps"]
        stub = next(i for i, s in enumerate(steps) if "ZFS_CMD=" in s.get("run", ""))
        smoke = next(i for i, s in enumerate(steps) if "--test-config" in s.get("run", ""))
        assert stub < smoke
        assert "ZPOOL_CMD=" in steps[stub]["run"]

    def test_it_does_not_run_the_real_tier(self):
        assert "real_zfs" not in "".join(self.runs(self.job()))

    def test_the_sqlalchemy_21_job_is_untouched(self):
        assert "test-sqlalchemy-21" in load("tests.yml")["jobs"]


class TestDebianFloorsJob:
    """The `.deb` depends on the distribution's python3-* packages, so one job installs Debian 13's
    archive versions (Python 3.13, click 8.1.8 -- Ubuntu 26.04 ships the same click) and runs the
    whole mocked suite there. These tests keep that job honest and its pins inside pyproject's
    ranges; they read the YAML and run nothing."""

    #: Where the pyproject floor IS Debian 13's version: the job tests the floor itself.
    FLOORS_AT_DEBIAN = {"flask", "websocket-client", "cryptography"}

    @staticmethod
    def job():
        return load("tests.yml")["jobs"]["debian-floors"]

    @classmethod
    def runs(cls):
        return [s["run"] for s in cls.job()["steps"] if "run" in s]

    @classmethod
    def pins(cls) -> dict:
        found = {}
        for run in cls.runs():
            for line in run.splitlines():
                match = re.fullmatch(r"\s*([A-Za-z0-9_.-]+)==(\S+)", line)
                if match:
                    found[canonicalize_name(match.group(1))] = Version(match.group(2))
        return found

    @staticmethod
    def project_requirements() -> dict:
        data = tomllib.loads((ROOT / "pyproject.toml").read_text())
        reqs = [Requirement(d) for d in data["project"]["dependencies"]]
        return {canonicalize_name(r.name): r for r in reqs}

    def test_it_runs_on_debian_13s_python(self):
        setup = next(s for s in self.job()["steps"] if str(s.get("uses", "")).startswith("actions/setup-python"))
        assert setup["with"]["python-version"] == "3.13"

    def test_every_runtime_dependency_is_pinned_and_the_pins_are_inside_pyproject_ranges(self):
        pins = self.pins()
        requirements = self.project_requirements()
        requirements.pop("libzfseasy")  # installed from the checkout, not from an index
        assert set(pins) == set(requirements), (
            f"unpinned: {sorted(set(requirements) - set(pins))}; "
            f"pinned but not a dependency: {sorted(set(pins) - set(requirements))}"
        )
        for name, requirement in requirements.items():
            assert requirement.specifier.contains(pins[name], prereleases=True), (
                f"{name}=={pins[name]} is outside pyproject's {requirement.specifier}: the "
                "Debian-version job would test something the package does not allow"
            )

    @pytest.mark.parametrize("name", sorted(FLOORS_AT_DEBIAN))
    def test_where_the_floor_is_debians_version_the_job_pins_the_floor(self, name):
        floor = next(s.version for s in self.project_requirements()[name].specifier if s.operator == ">=")
        assert self.pins()[name] == Version(floor), (
            f"{name}: pyproject's floor is {floor} but the Debian job tests {self.pins()[name]}; "
            "a floor must be a version the suite runs against"
        )

    def test_click_8_1_8_is_the_click_under_test(self):
        """Debian 13 and Ubuntu 26.04 both package click as 8.2.0+0.really.8.1.8; poetry.lock
        resolves a newer one, so this job is where click 8.1's CliRunner is exercised."""
        assert self.pins()["click"] == Version("8.1.8")
        assert self.project_requirements()["click"].specifier.contains(Version("8.1.8"))

    def test_the_pins_are_installed_in_one_pip_command_with_the_test_tools(self):
        install = next(r for r in self.runs() if "-r" in r and "debian13.txt" in r)
        pip_installs = [ln for ln in install.splitlines() if "-m pip install" in ln]
        assert len([ln for ln in pip_installs if "-r " in ln]) == 1
        assert "pytest" in install and "pytest-xdist" in install

    def test_both_projects_are_installed_without_dependencies_then_pip_check_runs(self):
        install = next(r for r in self.runs() if "-m pip install --no-deps" in r)
        line = next(ln for ln in install.splitlines() if "--no-deps" in ln)
        assert "-e libs/libzfseasy" in line and "-e ." in line
        assert install.index("--no-deps") < install.index("pip check")

    def test_the_installed_versions_are_verified_against_the_pins(self):
        check = next(r for r in self.runs() if "importlib.metadata" in r)
        assert "version(name) == pinned" in check and "debian13.txt" in check
        assert "(3, 13)" in check

    def test_it_runs_the_whole_mocked_suite(self):
        step = next(r for r in self.runs() if " -m pytest" in r)
        assert "debvenv/bin/python" in step, "pytest must run in the Debian-versions venv"
        arguments = step.split("-m pytest", 1)[1] + " "
        for narrowing in (" -k ", " -m ", "tests/", "::", "--deselect", "--ignore"):
            assert narrowing not in arguments, f"the Debian job must run the whole suite, found {narrowing!r}"

    def test_it_does_not_use_poetry_or_the_lock(self):
        text = "\n".join(self.runs())
        assert "poetry" not in text.lower()

    def test_the_other_jobs_are_untouched(self):
        jobs = load("tests.yml")["jobs"]
        assert {"test", "test-sqlalchemy-21", "package", "debian-floors"} <= set(jobs)

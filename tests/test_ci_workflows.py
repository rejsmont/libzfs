"""Guards on the CI workflow files (`.github/workflows/*.yml`): what may trigger the real-pool
tier, and what every mocked-suite workflow must keep.

The real tier (`real-zfs.yml`) runs `sudo zpool create` and real `zfs` commands as a
root-equivalent on the runner. Triggered by a pull request it would run the PR's code with that
power, so the only acceptable triggers are `schedule` and `workflow_dispatch`, on `master`
(plan decision C10). These tests read the YAML; they run nothing and need no network.
"""

from pathlib import Path

import pytest
import yaml

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
            '"packaging/"',
            '"tests/"',
            "packaging/systemd/zfsbackup.service",
        ):
            assert needle in text, f"the package job no longer checks {needle}"

    def test_the_wheel_goes_into_a_clean_venv_without_poetry_or_dev_dependencies(self):
        runs = self.runs(self.job())
        install = next(r for r in runs if "pip" in r and "dist/*.whl" in r)
        assert "python -m venv" in install
        assert "--with dev" not in "".join(runs) and "poetry install" not in "".join(runs)
        assert "pip show pytest" in install, "nothing proves the venv has no dev tooling"

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

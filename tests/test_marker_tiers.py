"""The test-tier markers partition the mocked suite (Checkpoint A, Q8).

`tests/conftest.py` marks every test that declares none of `unit` /
`integration` / `real_zfs` as `unit`, so that `pytest -m unit` plus
`pytest -m integration` cover the whole mocked suite. This pins the three
properties that make "sum the two counts" a meaningful check: nothing is
left out, nothing is in both, and real-ZFS tests are in neither.

Collection only (`--collect-only`), in a subprocess: nothing is executed, and
the parent run's own `-m` expression cannot interfere with it.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _collected(expression: str) -> int:
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest", "tests/", "--collect-only", "-q",
            "--no-cov", "-p", "no:cacheprovider", "-o", "addopts=",
            "-m", expression,
        ],
        cwd=ROOT, env=dict(os.environ), capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode in (0, 5), proc.stdout + proc.stderr
    # "982/1108 tests collected (126 deselected)" or "1108 tests collected"
    match = re.search(r"(\d+)(?:/\d+)? tests? collected", proc.stdout)
    if match is None:  # "no tests collected" (exit 5)
        return 0
    return int(match.group(1))


@pytest.mark.slow
class TestMarkerTiers:
    def test_unit_and_integration_partition_the_mocked_suite(self):
        mocked = _collected("not real_zfs")
        unit = _collected("unit")
        integration = _collected("integration")
        both = _collected("unit and integration")

        assert both == 0, "a test carries both tier markers and is double-counted"
        assert unit + integration == mocked
        assert unit > 0 and integration > 0

    def test_real_zfs_tests_are_in_neither_mocked_tier(self):
        assert _collected("real_zfs and (unit or integration)") == 0

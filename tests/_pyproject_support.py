"""Reading the two ``pyproject.toml`` files (PEP 621 ``[project]``) for the packaging tests.

The root project is ``zfsbackup``; ``libs/libzfseasy`` is the self-contained ``libzfseasy``
project. Requirements are PEP 508 strings, parsed with ``packaging.requirements`` (a hard
dependency of pytest, so it is in every dev environment).
"""

import tomllib
from pathlib import Path
from typing import Dict

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

REPO = Path(__file__).resolve().parent.parent
LIB_DIR = REPO / "libs" / "libzfseasy"


def load_pyproject(directory: Path = REPO) -> dict:
    with open(directory / "pyproject.toml", "rb") as handle:
        return tomllib.load(handle)


def load_lib_pyproject() -> dict:
    return load_pyproject(LIB_DIR)


def requirements(pyproject: dict) -> Dict[str, Requirement]:
    """``{canonical name: Requirement}`` of ``[project] dependencies``."""
    return {
        canonicalize_name(r.name): r
        for r in (Requirement(text) for text in pyproject["project"].get("dependencies", []))
    }


def specifier_strings(requirement: Requirement) -> frozenset:
    return frozenset(str(s) for s in requirement.specifier)

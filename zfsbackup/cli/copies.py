"""Keep backup copies out from below recursive datasets (plan item A2d).

The daemon snapshots a `recursive: true` dataset with one atomic `zfs snapshot
-r`, and that also snapshots into every backup copy below it. `validate_config`
already refuses the copies the configuration DECLARES (a receive target at,
below or recursively above a configured dataset). This module is the CLI's
second line, for the copies the configuration does not know about: before a
configuration change is committed it reads ZFS and refuses (exit 1, nothing
written) when the change would leave a backup copy below a recursive dataset.
`--force` writes it anyway, with the same text as `warning:` lines. The daemon
keeps its own backstop (`DatasetManager._snapshot_tree`, A2c: it snapshots the
sources only, logs one ERROR per (dataset, copy) episode and leaves a standing
WARNING note in the parent's snapshot status record, which `check` shows) for
what no check here can see.

Two checks, both comparing the OLD stored configuration with the NEW one:

1. **A dataset that becomes recursive** (enabled AND recursive now, and not
   both before; a newly added one counts): `DatasetManager.copy_tops` reads the
   copies below it (`org.zfsbackup:client_id`/`source_dataset` markers, local,
   received or inherited, plus the receive targets of the NEW configuration).
   Any copy, or the dataset itself being inside one, is a finding.
2. **A receive target that is given up** (a `zfs:` destination removed, its
   target changed or switched to https; the server's `remote_backup.
   target_dataset` removed or changed): when the old target lies at or below an
   enabled recursive dataset of the NEW configuration and exists in ZFS, the
   copies under it would be left below that dataset. The target is only "given
   up" when it is no receive target of the new configuration any more.

Failure to look is never a refusal. No `zfs` on this host (`libzfseasy` raises
at import), a pool that is not imported, a permission error, a timeout, any
read error: one `warning:` says the check could not be done and why, and the
write goes ahead (`zfsbackup-config` must work on hosts without `zfs`). A dataset
that `zfs` reports as not existing is skipped silently ONLY when its pool is
there: zfs prints the same "dataset does not exist" for a pool that is not
imported, so on that answer the pool (`zfs list` of its first path component) is
looked at too, and an absent pool is a could-not-check reason. Every `zfs` call
runs under `libzfseasy.timeout` for at most `ZFS_CALL_TIMEOUT` and at most the
budget that is left, a timeout ends the checking, and the whole check has a time
budget (`CHECK_BUDGET`) that is tested before EVERY `zfs` call (one
`DatasetManager.copy_tops` is a single step here: it makes 1 + the number of
receive targets calls of its own, each under that timeout), so a hung pool
cannot hang the CLI. Nothing is imported or run when there is nothing to
check (no newly recursive dataset and no given-up target).

The check runs OUTSIDE the writer lock, before the write. The commit is a
compare-and-swap on the generation the old configuration was read at, so if
the configuration changes between this check and the write the write fails
with a conflict (exit 75) and no stale answer is committed; holding the flock
for up to the time budget of ZFS reads would only make other writers wait. (`rename` takes
the lock for its `zfs set` WRITES, which must be justified by a committed
rename; this module only reads.) `import` has no compare-and-swap (a full
replace), so there the check is advisory against a concurrent writer.
"""

import shlex
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from zfsbackup.cli.errors import CliError
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import validate_config

__all__ = [
    "ZFS_CALL_TIMEOUT",
    "CHECK_BUDGET",
    "newly_recursive",
    "given_up_targets",
    "find_copy_problems",
    "check_copies",
    "enforce_copies",
]

#: Seconds each single `zfs` command of the check may run (`libzfseasy.timeout`).
ZFS_CALL_TIMEOUT = 30.0
#: Seconds the whole check may take; datasets not reached are named in the warning.
CHECK_BUDGET = 120.0
#: Copies listed per dataset before "and N more".
_SHOWN = 5

_FORCE_HINT = (
    "Move the copies out, keep those datasets non-recursive, or re-run with --force to "
    "write the configuration anyway (the daemon then keeps the copies out of its "
    "snapshots itself, which are no longer one `zfs snapshot -r`; it logs one ERROR "
    "per copy and `check` shows a standing warning on the dataset until they are moved)."
)


def _target(text: Optional[str]) -> str:
    return (text or "").strip().rstrip("/")


def _receive_targets(config: BackupConfig) -> List[Tuple[str, str]]:
    """`[(label, target)]`: the server target and every local destination's
    target, like `DatasetManager._receive_targets` (the server's counts whether
    or not `remote_backup` is enabled), with the label a message names it by."""
    found: List[Tuple[str, str]] = []
    rb = config.remote_backup
    if rb and _target(rb.target_dataset):
        found.append(("remote_backup.target_dataset", _target(rb.target_dataset)))
    for name, dest in sorted((config.destinations or {}).items()):
        if getattr(dest, "is_local", False) is True and _target(dest.target_dataset):
            found.append((f"destinations[{name}]", _target(dest.target_dataset)))
    return found


def _recursive_enabled(config: Optional[BackupConfig]) -> Dict[str, Any]:
    if config is None:
        return {}
    return {d.name: d for d in config.enabled_datasets if d.recursive}


def newly_recursive(old: Optional[BackupConfig], new: BackupConfig) -> List[Any]:
    """The `DatasetConfig`s that are enabled AND recursive in `new` and were not
    in `old` (`None` = nothing stored: every one counts), in configuration order."""
    before = _recursive_enabled(old)
    return [d for d in new.enabled_datasets if d.recursive and d.name not in before]


def given_up_targets(old: Optional[BackupConfig], new: BackupConfig) -> List[Tuple[str, str]]:
    """`[(label, old target)]` of the receive targets `old` had that `new` no
    longer has (removed, changed, or switched kind). A target `new` still
    receives at, by any destination, is not given up."""
    if old is None:
        return []
    kept: Set[str] = {target for _, target in _receive_targets(new)}
    return [(label, target) for label, target in _receive_targets(old) if target not in kept]


def _quote(name: str) -> str:
    return shlex.quote(name)


def _copy_problem(root: str, copy: str, why: str) -> str:
    return (
        f"datasets[{root}] is recursive and the backup copy {copy} is below it ({why}), so "
        f"`zfs snapshot -r` would snapshot into the copy. Move the copy out "
        f"(`zfs rename {_quote(copy)} <a name outside {root}>`) or keep datasets[{root}] "
        "non-recursive"
    )


def _inside_problem(root: str, origin: str, source: str) -> str:
    if origin == root:
        what = f"carries the backup-copy markers itself (source: {source}), so it is a backup copy"
        fix = (
            f"keep datasets[{root}] non-recursive or move it; if it is really a source with "
            f"stale markers, clear them (`zfs inherit org.zfsbackup:client_id {_quote(root)}`, "
            f"`zfs inherit org.zfsbackup:source_dataset {_quote(root)}`; that works for local "
            "values only)"
        )
    else:
        what = f"is itself inside the backup copy {origin} (source: {source})"
        fix = (
            f"move it out of {origin} (`zfs rename {_quote(root)} <a name outside {origin}>`) "
            f"or keep datasets[{root}] non-recursive"
        )
    return f"datasets[{root}] is recursive and {what}: {fix}"


def _given_up_problem(label: str, target: str, root: str) -> str:
    return (
        f"{label} ({target}) is no longer a receive target, and it lies below the recursive "
        f"datasets[{root}]: its copies would be left below it, where `zfs snapshot -r` "
        f"would snapshot into them. Move them out first "
        f"(`zfs rename {_quote(target)} <a name outside {root}>`) or keep datasets[{root}] "
        "non-recursive"
    )


def _load_zfs() -> Tuple[Any, Optional[str]]:
    """(`zfsbackup.backup_manager`, None), or (None, why not): importing it
    imports `libzfseasy`, which raises `RuntimeError` when no `zfs` binary can
    be found, which must not break a host that only manages configuration."""
    try:
        from zfsbackup import backup_manager  # noqa: PLC0415 - lazy by design
    except (ImportError, RuntimeError) as exc:
        return None, str(exc).strip() or type(exc).__name__
    return backup_manager, None


def _reason(exc: BaseException) -> str:
    text = str(exc).strip()
    return text or type(exc).__name__


def find_copy_problems(
    old: Optional[BackupConfig], new: BackupConfig, *, zfs_module: Any = None
) -> Tuple[List[str], List[str]]:
    """`(problems, could_not_check)` for the change `old` -> `new`.

    `problems` are the findings (each one sentence about one dataset or target);
    `could_not_check` the reasons part of the check could not be done, which
    callers print as ONE warning each. Never raises for a ZFS failure. Needs no
    ZFS at all (and imports none) when there is nothing to look at.
    `zfs_module` is for tests (a `backup_manager`-shaped object)."""
    roots = newly_recursive(old, new)
    abandoned = given_up_targets(old, new)
    recursive_new = _recursive_enabled(new)
    # (b) only matters for a target below (or at) a recursive dataset of `new`.
    below: List[Tuple[str, str, str]] = []  # (label, target, root)
    for label, target in abandoned:
        for root in sorted(recursive_new):
            if target == root or target.startswith(root + "/"):
                below.append((label, target, root))
    if not roots and not below:
        return [], []
    try:
        # Whatever it reports, an invalid configuration is refused by
        # `commit_config` right after; do not spend ZFS reads on it.
        validate_config(new)
    except ValueError:
        return [], []

    bm, unavailable = (zfs_module, None) if zfs_module is not None else _load_zfs()
    if bm is None:
        return [], [f"ZFS cannot be used here ({unavailable})"]

    problems: List[str] = []
    failures: List[str] = []
    out_of_budget: List[str] = []  # not looked at: the time budget ran out
    after_timeout: List[str] = []  # not looked at: an earlier zfs call timed out
    found_tops: Dict[str, Set[str]] = {}  # root -> copy tops found by check (a)
    deadline = time.monotonic() + CHECK_BUDGET
    timed_out = False
    pools: Dict[str, bool] = {}  # pool -> imported

    class _OutOfBudget(Exception):
        pass

    def look(call: Callable[[], Any]) -> Any:
        """Run one zfs step, never past the budget (tested BEFORE the call)."""
        left = deadline - time.monotonic()
        if left <= 0:
            raise _OutOfBudget()
        with bm.zfs.timeout(min(ZFS_CALL_TIMEOUT, left)):
            return call()

    def pool_absent(dataset: str) -> Optional[str]:
        """Called after `exists(dataset)` was False: zfs says "dataset does not
        exist" for a pool that is not imported as well. The reason text when the
        pool is not there, else None."""
        pool = dataset.split("/", 1)[0]
        if pool not in pools:  # one `zfs list` per pool
            pools[pool] = pool != dataset and bool(look(lambda: bm.zfs.exists(bm.Dataset(pool))))
        return None if pools[pool] else f"pool {pool} is not imported (or does not exist)"

    def not_reached(what: str) -> None:
        (after_timeout if timed_out else out_of_budget).append(what)

    try:
        manager = bm.DatasetManager(new)
        for config in roots:
            name = config.name
            if timed_out:
                not_reached(f"datasets[{name}]")
                continue
            try:
                if not look(lambda: bm.zfs.exists(bm.Dataset(name))):
                    gone = pool_absent(name)
                    if gone is not None:
                        failures.append(f"datasets[{name}]: {gone}")
                    continue  # else nothing below a dataset that is not there yet
                tops = look(lambda: manager.copy_tops(config))
            except _OutOfBudget:
                not_reached(f"datasets[{name}]")
                continue
            except bm.ZFSTimeoutError as exc:
                failures.append(f"datasets[{name}]: {_reason(exc)}")
                timed_out = True  # a hung pool: do not wait for the next one
                continue
            except Exception as exc:  # noqa: BLE001 - any read failure: warn, never refuse
                failures.append(f"datasets[{name}]: {_reason(exc)}")
                continue
            inside = getattr(tops, "inside", None)
            covered = getattr(tops, "covered", frozenset())
            if inside is not None:
                problems.append(_inside_problem(name, inside[0], inside[1]))
            listed = [(top, why) for top, why in tops.items() if top not in covered]
            for top, why in listed[:_SHOWN]:
                problems.append(_copy_problem(name, top, why))
            if len(listed) > _SHOWN:
                problems.append(
                    f"datasets[{name}] has {len(listed) - _SHOWN} more backup copies below it"
                )
            found_tops[name] = set(tops) | ({inside[0]} if inside else set())

        existing: Dict[str, bool] = {}
        for label, target, root in below:
            if any(
                target == top or target.startswith(top + "/") for top in found_tops.get(root, ())
            ):
                continue  # already named by check (a)
            if timed_out:
                not_reached(f"{label} ({target})")
                continue
            try:
                if target not in existing:
                    existing[target] = bool(look(lambda: bm.zfs.exists(bm.Dataset(target))))
                    if not existing[target]:
                        gone = pool_absent(target)
                        if gone is not None:
                            failures.append(f"{label} ({target}): {gone}")
            except _OutOfBudget:
                not_reached(f"{label} ({target})")
                continue
            except bm.ZFSTimeoutError as exc:
                failures.append(f"{target}: {_reason(exc)}")
                timed_out = True
                continue
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{target}: {_reason(exc)}")
                continue
            if existing[target]:
                problems.append(_given_up_problem(label, target, root))
    except Exception as exc:  # noqa: BLE001 - e.g. an odd module
        failures.append(_reason(exc))

    could_not: List[str] = []
    if failures:
        could_not.append("; ".join(failures))
    if after_timeout:
        could_not.append(
            f"not checked: an earlier zfs call timed out: {', '.join(after_timeout)}"
        )
    if out_of_budget:
        could_not.append(
            f"not reached within the {CHECK_BUDGET:g} s budget: {', '.join(out_of_budget)}"
        )
    return problems, could_not


def check_copies(
    old: Optional[BackupConfig], new: BackupConfig, *, force: bool,
    warn: Callable[[str], None], zfs_module: Any = None, hint: Optional[str] = None,
) -> Optional[str]:
    """Run the check for `old` -> `new`. Warns for a check that could not be
    done. A finding is returned as ONE refusal text, or, with `force`, printed
    as `--force:` warnings and `None` returned. `None` also when nothing is wrong.
    `hint` replaces the closing advice (`_FORCE_HINT`) for a caller whose
    "re-run" is not a plain re-run (the `edit` loop)."""
    problems, could_not = find_copy_problems(old, new, zfs_module=zfs_module)
    for reason in could_not:
        warn(
            "could not check whether this change leaves a backup copy below a recursive "
            f"dataset ({reason}); writing the configuration anyway. The daemon keeps copies "
            "out of its snapshots and logs an ERROR for any it finds"
        )
    if not problems:
        return None
    if force:
        for problem in problems:
            warn(f"--force: {problem}")
        return None
    return (
        "refusing to write: this change would leave backup copies below a recursive "
        "dataset:\n  - " + "\n  - ".join(problems) + f"\nNothing was written. {hint or _FORCE_HINT}"
    )


def enforce_copies(
    old: Optional[BackupConfig], new: BackupConfig, *, force: bool,
    warn: Callable[[str], None], zfs_module: Any = None, hint: Optional[str] = None,
) -> None:
    """`check_copies`, raising `CliError` (exit 1) for a refusal."""
    refusal = check_copies(old, new, force=force, warn=warn, zfs_module=zfs_module, hint=hint)
    if refusal is not None:
        raise CliError(refusal)

"""`zfsbackup-config rename dataset|destination OLD NEW` (plan item 14.2).

A name is identity. The database cannot change one in place for a destination
(the `dataset_remotes.destination_name` foreign key has no ON UPDATE action),
and a dataset's name is also the key of everything the daemon keeps in ZFS user
properties, so both renames are a single full-replace write of a configuration
rebuilt with the new name (`write.commit_config`: validated in memory, then a
compare-and-swap on the snapshot's generation), plus the ZFS-side bookkeeping
below. ZFS is reached only through `zfsbackup.backup_manager`'s module-level
helpers (`read_zfsbackup_properties`, `copy_anchor_property`,
`clear_zfsbackup_properties`), imported LAZILY inside the functions that need
them: `libzfseasy` resolves `$ZFS_CMD` at import time, and every other command
of this tool must keep working on a host without ZFS. If it cannot be loaded,
the ZFS steps that are optional are skipped with a warning and the ones that
are required (copying anchors, clearing properties) fail before anything is
written.

**rename dataset OLD NEW**
- refused if NEW already exists in the configuration;
- the `org.zfsbackup:*` properties on the ZFS dataset OLD do not follow the
  rename; they are reported as stale. If OLD no longer exists in ZFS but NEW
  carries `org.zfsbackup:*` properties, the dataset was `zfs rename`d: nothing
  is stale and nothing is reported;
- a dataset with remote destinations needs `--force`: the server derives its
  copy's path from the client id and the dataset name, so a changed path means
  the next backup is likely a full send and the old server copy is orphaned.
  This tool never touches a server;
- `--clear-stale-props` removes `org.zfsbackup:config` and every
  `org.zfsbackup:anchor.*` from the old dataset. It is refused up front when
  the new ZFS dataset does not exist yet (the operator may be about to
  `zfs rename`, and the properties follow the dataset). The daemon check is a
  single `daemonlock.probe()` made AFTER the configuration write
  (`_no_daemon`), not a held lock: a daemon that starts later reads the NEW
  configuration and never touches the old dataset, while one running now may
  have loaded the old one. If one is running (or the probe cannot tell), the
  properties are left and the exact commands to run once it stops are printed
  (exit 1; the rename itself was done). Without `--clear-stale-props` the same
  commands are printed after the rename.

**rename destination OLD NEW**
- refused if NEW already exists;
- before the write, `anchor.OLD` is COPIED to `anchor.NEW` on every
  referencing dataset, OVERWRITING a differing `anchor.NEW` (both values are
  printed; the snapshot the overwritten value named is no longer pinned by
  it) -- never moved: the daemon protects every local or received `anchor.*`,
  so the old property keeps protecting its snapshot until the operator clears
  it. Because the overwrite is a ZFS change only a successful rename
  justifies, the order is: take the writer lock (just the flock -- no database
  connection is open during the zfs calls), re-check that the stored
  generation is still the snapshot's (else exit 75 before ZFS is touched),
  copy, then write with the lock handed to `write_config(lock=...)`. If
  anything fails after a copy, the exact commands that put each previous
  value back (`zfs set` / `zfs inherit`) are printed. A referencing dataset
  that does not exist in ZFS aborts before anything is copied, unless
  `--skip-missing`; the new name must be usable in a ZFS property name.
- the stale `anchor.OLD` properties are listed afterwards and left in place;
- no `--force`: the server path does not depend on the destination's name.

`--dry-run` for both prints the ZFS property actions and the configuration
change and does neither (it only reads).
"""

import contextlib
import shlex
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import click

from zfsbackup import daemonlock
from zfsbackup.cli import dotpath
from zfsbackup.cli.errors import CliError
from zfsbackup.cli.write import commit_config, notify_daemon, release_database, where_flag
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    ConfigConflict,
    ConfigSnapshot,
    ResolvedConfigPath,
    destination_name_problem,
    read_generation,
    validate_config,
    writer_lock,
)

__all__ = ["rename_dataset", "rename_destination"]

_PROP_PREFIX = "org.zfsbackup:"
_PROP_CONFIG = "org.zfsbackup:config"
_PROP_ANCHOR = "org.zfsbackup:anchor."


# --------------------------------------------------------------------------
# ZFS access (lazy)
# --------------------------------------------------------------------------


def _load_backup_manager() -> Tuple[Any, Optional[str]]:
    """(`zfsbackup.backup_manager`, None), or (None, why not). Importing it
    imports `libzfseasy`, which raises `RuntimeError` when no `zfs` binary can
    be found; that must not break a host that only manages configuration."""
    try:
        from zfsbackup import backup_manager  # noqa: PLC0415 - lazy by design
    except (ImportError, RuntimeError) as exc:
        return None, str(exc) or type(exc).__name__
    # libzfseasy ships with `zfs.debug = True`, which prints every command it
    # runs on STDOUT; this tool's stdout is for its own results.
    try:
        import libzfseasy.zfs as _zfs_module  # noqa: PLC0415

        _zfs_module.debug = False
    except (ImportError, AttributeError):
        pass
    return backup_manager, None


def _inspect(bm: Any, name: str) -> Tuple[bool, Dict[str, str]]:
    """(does the ZFS dataset exist, its `org.zfsbackup:*` properties)."""
    ds = bm.Dataset(name)
    if not bm.zfs.exists(ds):
        return False, {}
    return True, bm.read_zfsbackup_properties(ds)


# --------------------------------------------------------------------------
# Shared checks
# --------------------------------------------------------------------------


def _check_new_name(kind: str, old: str, new: str, existing) -> None:
    if old == new:
        raise CliError(f"the {kind} is already named {old!r}")
    if not new or new != new.strip():
        raise CliError(f"{new!r} is not a usable {kind} name")
    if kind == "dataset" and (
        any(c in new for c in "@#") or new.startswith("/") or new.endswith("/") or "//" in new
    ):
        raise CliError(f"{new!r} is not a valid ZFS dataset name")
    if new in existing:
        raise CliError(
            f"a {kind} named {new!r} already exists in the configuration; "
            "nothing was changed"
        )


def _server_relative(name: str) -> str:
    """What the server appends after `<target>/<client id>`: the dataset name
    without its first (pool) component (`api._server_dataset`)."""
    return name.split("/", 1)[1] if "/" in name else ""


def _client_id(config: BackupConfig) -> str:
    """The client id if the configuration names its file explicitly and it is
    readable; otherwise a placeholder (the default file is resolved in the
    daemon's process, not here)."""
    if config.client_id_file is not None:
        try:
            text = Path(config.client_id_file).read_text(encoding="utf-8").strip()
        except OSError:
            text = ""
        if text:
            return text
    return "<client-id>"


def _server_path(client_id: str, relative: str) -> str:
    return f"<server target>/{client_id}" + (f"/{relative}" if relative else "")


def _server_note(config: BackupConfig, old: str, new: str, carried: bool) -> str:
    cid = _client_id(config)
    old_rel, new_rel = _server_relative(old), _server_relative(new)
    if old_rel != new_rel:
        return (
            f"the server keeps this dataset at {_server_path(cid, old_rel)}; under the new "
            f"name it would be {_server_path(cid, new_rel)}. The incremental base is "
            "negotiated by snapshot name, so the next backup to each destination is likely "
            "a full send unless the server already has the dataset under the new path; the "
            f"existing copy at {_server_path(cid, old_rel)} stays where it is, orphaned "
            "(this tool never touches a server)"
        )
    tail = (
        "the anchor properties came along with the dataset"
        if carried
        else "a new dataset does not carry the old one's anchor properties, which only "
        "keep its base snapshot from being pruned locally"
    )
    return (
        f"the server-side path {_server_path(cid, new_rel)} does not change (only the pool "
        f"part of the name differs), so incremental sends can continue if the new dataset "
        f"has the snapshots the server already holds; {tail}"
    )


def _renamed_dataset_config(config: BackupConfig, old: str, new: str) -> BackupConfig:
    data = dotpath.to_dict(config)
    for entry in data["datasets"]:
        if entry["name"] == old:
            entry["name"] = new
    return dotpath.parse_dict(data)


def _renamed_destination_config(config: BackupConfig, old: str, new: str) -> BackupConfig:
    data = dotpath.to_dict(config)
    data["destinations"] = {
        (new if name == old else name): body for name, body in data["destinations"].items()
    }
    for entry in data["datasets"]:
        for remote in entry.get("remote", []):
            if remote["destination"] == old:
                remote["destination"] = new
    return dotpath.parse_dict(data)


def _daemon_running(resolved: ResolvedConfigPath) -> Optional[str]:
    """None if no daemon holds the presence lock; otherwise a description. A
    lock that cannot be examined is reported too: "cannot tell" is not "no"."""
    try:
        info = daemonlock.probe(resolved)
    except OSError as exc:
        return f"cannot tell whether a daemon is running ({exc.strerror or exc})"
    if info is None:
        return None
    return f"a daemon is running (pid {info.pid if info.pid is not None else 'unknown'})"


class _DaemonPresent(Exception):
    """A daemon is (or may be) running: the stale properties stay."""


@contextlib.contextmanager
def _no_daemon(resolved: ResolvedConfigPath):
    """Check ONCE, now, that no daemon is running, failing with
    `_DaemonPresent` if `daemonlock.probe` finds one or cannot tell.

    The caller has already committed the rename, and a daemon takes the
    presence file's exclusive lock BEFORE it reads the configuration. So a
    daemon that holds the lock now may have loaded the old configuration (and
    relies on the old dataset's anchors), whereas one that starts after this
    point reads the new configuration and never touches the old dataset: no
    lock needs to be held while the properties are cleared. (A missing or
    stale pid file means no daemon is running now -- a clean stop removes the
    file, a crash leaves it unlocked.) Holding a lock here would only make a
    daemon that starts meanwhile refuse with a misleading message.
    """
    busy = _daemon_running(resolved)
    if busy is not None:
        raise _DaemonPresent(busy)
    yield


def _inherit_commands(dataset: str, props: Sequence[str]) -> str:
    return "\n".join(f"  zfs inherit {p} {shlex.quote(dataset)}" for p in props)


def _clear_stale(
    resolved: ResolvedConfigPath, bm: Any, old: str, props: Sequence[str]
) -> List[str]:
    """Clear `props` (`org.zfsbackup:config` / `anchor.*`) on the old dataset,
    one `zfs inherit` at a time, under `_no_daemon`. Raises `CliError`, always
    saying which were cleared and which `zfs inherit` commands are left."""
    ordered = sorted(props)
    cleared: List[str] = []
    try:
        with _no_daemon(resolved):
            ds = bm.Dataset(old)
            for prop in ordered:
                if prop == _PROP_CONFIG:
                    bm.clear_zfsbackup_properties(ds, config=True, anchors=False)
                else:
                    bm.clear_zfsbackup_properties(
                        ds, config=False, anchors=[prop[len(_PROP_ANCHOR):]]
                    )
                cleared.append(prop)
    except _DaemonPresent as exc:
        raise CliError(
            "the configuration was renamed, but the stale properties on "
            f"{old!r} were NOT cleared: {exc}. Once it is stopped, run:\n"
            + _inherit_commands(old, ordered)
        ) from None
    except Exception as exc:  # noqa: BLE001
        left = [p for p in ordered if p not in cleared]
        raise CliError(
            "the configuration was renamed, but clearing the stale properties on "
            f"{old!r} failed: {str(exc).strip() or type(exc).__name__}. "
            f"Cleared so far: {', '.join(cleared) or 'none'}. Still set, to clear by hand:\n"
            + _inherit_commands(old, left)
        ) from exc
    except BaseException as exc:
        exc.add_note(
            f"the configuration was renamed; cleared on {old!r} before the interrupt: "
            f"{', '.join(cleared) or 'none'}. Properties that may remain: "
            f"{', '.join(p for p in ordered if p not in cleared)}"
        )
        raise
    return cleared


def _anchor_message(
    old: str, new: str, outcome: str, old_value: Optional[str], new_value: Optional[str],
    dry_run: bool,
) -> str:
    """One line on what happened (or would happen) to `anchor.<new>`."""
    src = f"copied from anchor.{old}"
    if outcome == "no-source":
        return f"has no anchor.{old} to copy"
    if outcome == "same" or (
        outcome == "dry-run" and new_value is not None and new_value == old_value
    ):
        return f"anchor.{new} is already current ({new_value})"
    if outcome == "exists":  # not reachable with overwrite=True; kept for safety
        return f"anchor.{new} is {new_value} and was left alone"
    if outcome == "overwritten" or (outcome == "dry-run" and new_value is not None):
        verb = "would be overwritten: was" if dry_run else "was"
        tail = "would become" if dry_run else "now"
        unpinned = "" if dry_run else f"; snapshot {new_value} is no longer pinned by anchor.{new}"
        return f"anchor.{new} {verb} {new_value}, {tail} {old_value} ({src}){unpinned}"
    # "copied", or a dry run where anchor.<new> is absent
    return f"anchor.{new} {'would be set to' if dry_run else 'set to'} {old_value} ({src})"


def _inherit_commands_for(datasets: Sequence[str], prop: str) -> str:
    return "\n".join(f"  zfs inherit {prop} {shlex.quote(d)}" for d in datasets)


def _require_generation(resolved: ResolvedConfigPath, snapshot: ConfigSnapshot) -> None:
    """Raise `ConfigConflict` (exit 75) unless the stored generation is still
    the one `snapshot` was read at. Opens and closes a read connection; nothing
    stays open for the zfs calls that follow."""
    try:
        current = read_generation(resolved)
    finally:
        release_database()
    if current != snapshot.generation:
        raise ConfigConflict(snapshot.generation, current)


def _interrupted_after_write(
    exc: BaseException, resolved: ResolvedConfigPath, applied: str,
    dataset: Optional[str], to_clear: Sequence[str],
) -> None:
    """An interrupt after the configuration write committed: say so (unless
    `_clear_stale` already did), with the `zfs inherit` commands still due."""
    if isinstance(exc, Exception):
        return
    if any("was renamed" in note for note in getattr(exc, "__notes__", ())):
        return
    note = f"{applied}; run `zfsbackup-config{where_flag(resolved)} export` to see the configuration"
    if dataset and to_clear:
        note += (
            f". The stale properties on {dataset!r} were not cleared (or not all); to clear "
            "them once no daemon is running:\n" + _inherit_commands(dataset, sorted(to_clear))
        )
    exc.add_note(note)


def _interrupted_write(exc: BaseException, resolved: ResolvedConfigPath, what: str) -> None:
    """Annotate an interrupt that landed while the write was in flight: it may
    have committed."""
    if not isinstance(exc, Exception):
        exc.add_note(
            f"the {what} may or may not have been applied; run "
            f"`zfsbackup-config{where_flag(resolved)} export` to check"
        )


# --------------------------------------------------------------------------
# rename dataset
# --------------------------------------------------------------------------


def rename_dataset(
    resolved: ResolvedConfigPath,
    snapshot: ConfigSnapshot,
    old: str,
    new: str,
    *,
    force: bool = False,
    clear_stale_props: bool = False,
    dry_run: bool = False,
    warn: Callable[[str], None],
) -> None:
    config = snapshot.config
    ds = next((d for d in config.datasets if d.name == old), None)
    if ds is None:
        raise CliError(
            f"no dataset named {old!r}; datasets: {', '.join(d.name for d in config.datasets)}"
        )
    _check_new_name("dataset", old, new, {d.name for d in config.datasets})
    remotes = [r.destination for r in ds.remote]

    # Read-only look at ZFS. Failing to look is a warning, not an error:
    # `rename` is first of all a configuration change.
    bm, unavailable = _load_backup_manager()
    old_exists: Optional[bool] = None
    new_exists: Optional[bool] = None
    old_props: Dict[str, str] = {}
    new_props: Dict[str, str] = {}
    if bm is not None:
        try:
            old_exists, old_props = _inspect(bm, old)
            new_exists, new_props = _inspect(bm, new)
        except Exception as exc:  # noqa: BLE001 - any zfs failure: look no further
            old_exists = new_exists = None
            old_props, new_props = {}, {}
            unavailable = f"reading ZFS properties failed: {str(exc).strip() or type(exc).__name__}"
    zfs_renamed = old_exists is False and bool(new_props)
    about_to_zfs_rename = bool(old_exists) and new_exists is False

    if clear_stale_props:
        if unavailable is not None:
            raise CliError(
                f"--clear-stale-props needs ZFS, which cannot be used here ({unavailable}); "
                "nothing was changed"
            )
        if about_to_zfs_rename:
            raise CliError(
                f"--clear-stale-props refused: the ZFS dataset {new!r} does not exist yet "
                f"but {old!r} does. If you are about to `zfs rename {old} {new}`, the "
                "org.zfsbackup:* properties follow the dataset and must not be cleared; "
                "rename it first, then run this again. Nothing was changed."
            )

    if remotes and not force:
        raise CliError(
            f"{old!r} sends to {', '.join(remotes)}. Renaming it affects its remote "
            f"backups: {_server_note(config, old, new, zfs_renamed)}. "
            "Re-run with --force to rename anyway. Nothing was changed."
        )

    new_config = _renamed_dataset_config(config, old, new)
    prefix = "[dry run] " if dry_run else ""

    if unavailable is not None:
        warn(
            f"ZFS was not consulted ({unavailable}); stale org.zfsbackup:* properties on "
            f"{old!r} (if any) were not checked"
        )
    elif zfs_renamed:
        click.echo(
            f"{new!r} already carries org.zfsbackup:* properties and {old!r} no longer "
            "exists in ZFS: treating this as a `zfs rename` that happened already."
        )
    elif old_exists and old_props:
        names = ", ".join(sorted(old_props))
        if about_to_zfs_rename:
            suffix = (
                f". If you are about to `zfs rename {old} {new}`, these properties will "
                "follow it -- don't clear them"
            )
        else:
            suffix = ""  # the commands to remove them are printed after the rename
        warn(
            f"the ZFS dataset {old!r} keeps its org.zfsbackup:* properties ({names}); they "
            f"do not follow the configuration rename and become stale{suffix}"
        )
    if remotes:
        warn(f"--force: {_server_note(config, old, new, zfs_renamed)}")

    stale_props = sorted(
        p for p in old_props if p == _PROP_CONFIG or p.startswith(_PROP_ANCHOR)
    ) if old_exists else []
    to_clear = stale_props if clear_stale_props else []
    if dry_run:
        click.echo(f"{prefix}would rename dataset {old!r} -> {new!r} in the configuration")
        if to_clear:
            click.echo(f"{prefix}would clear on {old!r}: " + ", ".join(to_clear))
            busy = _daemon_running(resolved)
            if busy is not None:
                click.echo(
                    f"{prefix}note: {busy}; after the write the clear would be skipped "
                    "and the zfs inherit commands printed instead"
                )
        return

    try:
        result = commit_config(resolved, snapshot, new_config, warn=warn, announce=False)
    except BaseException as exc:
        _interrupted_write(exc, resolved, "rename")
        raise
    try:
        click.echo(f"Renamed dataset {old!r} -> {new!r} in the configuration.")
        if stale_props and not clear_stale_props and not about_to_zfs_rename:
            # Re-running with --clear-stale-props is not possible (OLD is no
            # longer a configured dataset), so give the commands themselves.
            click.echo(
                f"The stale properties on {old!r} were left in place. To remove them, once "
                f"no daemon is running:\n{_inherit_commands(old, stale_props)}"
            )

        if to_clear:
            # The daemon check is made HERE, after the write (`_no_daemon`).
            try:
                cleared = _clear_stale(resolved, bm, old, to_clear)
            except CliError:
                notify_daemon(resolved, result.generation)
                raise
            click.echo(f"Cleared on {old!r}: {', '.join(cleared) or '(nothing set)'}")
        notify_daemon(resolved, result.generation)
    except BaseException as exc:
        _interrupted_after_write(
            exc, resolved, f"the rename of dataset {old!r} was applied", old, to_clear
        )
        raise


# --------------------------------------------------------------------------
# rename destination
# --------------------------------------------------------------------------


def rename_destination(
    resolved: ResolvedConfigPath,
    snapshot: ConfigSnapshot,
    old: str,
    new: str,
    *,
    dry_run: bool = False,
    skip_missing: bool = False,
    warn: Callable[[str], None],
) -> None:
    problem = destination_name_problem(new)
    if problem:
        raise CliError(
            f"{new!r} cannot be used as a destination name: {problem}. Nothing was changed."
        )
    config = snapshot.config
    if old not in config.destinations:
        raise CliError(
            f"no destination named {old!r}; destinations: "
            f"{', '.join(sorted(config.destinations)) or '(none)'}"
        )
    _check_new_name("destination", old, new, set(config.destinations))
    referencing = [
        d.name for d in config.datasets if any(r.destination == old for r in d.remote)
    ]
    new_config = _renamed_destination_config(config, old, new)
    # Pure and cheap: a configuration the store would refuse must be found
    # BEFORE any anchor is copied, so that the only failures left after a copy
    # are I/O ones (`commit_config` repeats the call; warnings print once).
    for message in validate_config(new_config):
        warn(message)
    prefix = "[dry run] " if dry_run else ""

    outcomes: Dict[str, str] = {}
    to_copy: List[str] = []
    bm: Any = None
    if referencing:
        bm, unavailable = _load_backup_manager()
        if bm is None:
            raise CliError(
                f"the anchor properties of {', '.join(referencing)} must be copied to "
                f"anchor.{new}, but ZFS cannot be used here ({unavailable}). Nothing "
                "was changed."
            )
        # Look first, copy later: a missing dataset must not be found after
        # some anchors were already copied.
        missing: List[str] = []
        for name in referencing:
            try:
                if not bm.zfs.exists(bm.Dataset(name)):
                    missing.append(name)
            except Exception as exc:  # noqa: BLE001
                raise CliError(
                    f"cannot look up the ZFS dataset {name!r}: "
                    f"{str(exc).strip() or type(exc).__name__}. Nothing was changed."
                ) from exc
        if missing and not skip_missing:
            raise CliError(
                f"{', '.join(map(repr, missing))} send(s) to {old!r} but "
                f"{'does' if len(missing) == 1 else 'do'} not exist in ZFS, so "
                f"anchor.{new} cannot be copied for {'it' if len(missing) == 1 else 'them'}. "
                "Nothing was changed. Fix that, or re-run with --skip-missing to rename the "
                "destination without copying an anchor for the missing dataset(s)."
            )
        for name in missing:
            warn(
                f"dataset {name!r} does not exist in ZFS: no anchor.{new} was copied for it "
                "(--skip-missing)"
            )
        to_copy = [n for n in referencing if n not in missing]

    old_prop, new_prop = f"{_PROP_ANCHOR}{old}", f"{_PROP_ANCHOR}{new}"
    values: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    # dataset -> the value anchor.<new> had BEFORE this command changed it
    # (None = it was absent): what puts it back.
    restore: Dict[str, Optional[str]] = {}

    def restore_text(extra: Optional[Dict[str, Optional[str]]] = None) -> str:
        pending = dict(restore)
        pending.update(extra or {})
        if not pending:
            return ""
        lines = []
        for name, previous in pending.items():
            if previous is None:
                lines.append(f"  zfs inherit {new_prop} {shlex.quote(name)}")
            else:
                lines.append(f"  zfs set {shlex.quote(f'{new_prop}={previous}')} {shlex.quote(name)}")
        replaced = {n: v for n, v in pending.items() if v is not None}
        added = [n for n, v in pending.items() if v is None]
        facts = []
        if replaced:
            facts.append(
                f"{new_prop} was overwritten on {', '.join(map(repr, replaced))}, which unpins "
                "the snapshot its previous value protected"
            )
        if added:
            facts.append(
                f"{new_prop} was newly set on {', '.join(map(repr, added))} (it did not exist "
                "before)"
            )
        return (
            "; ".join(facts)
            + ". To put the previous values back run:\n"
            + "\n".join(lines)
            + f"\n(or re-run the rename, which copies anchor.{old} again)"
        )

    def copy_anchors() -> None:
        for name in to_copy:
            ds = bm.Dataset(name)
            try:
                before = bm.read_zfsbackup_properties(ds)
                values[name] = (before.get(old_prop), before.get(new_prop))
                outcomes[name] = bm.copy_anchor_property(
                    ds, old, new, dry_run=dry_run, overwrite=True
                )
            except Exception as exc:  # noqa: BLE001
                tail = restore_text()
                raise CliError(
                    f"copying {old_prop} to anchor.{new} on {name!r} failed: "
                    f"{str(exc).strip() or type(exc).__name__}. Nothing was written to the "
                    "configuration." + (f"\n{tail}" if tail else "")
                ) from exc
            except BaseException as exc:
                # The copy in flight may or may not have landed.
                inflight = {name: values[name][1]} if name in values and not dry_run else {}
                tail = restore_text(inflight)
                exc.add_note(
                    f"interrupted while copying {old_prop} to anchor.{new} on {name!r}. "
                    "Nothing was written to the configuration."
                    + (f"\n{tail}" if tail else "")
                )
                raise
            if outcomes[name] == "copied":
                restore[name] = None
            elif outcomes[name] == "overwritten":
                restore[name] = values[name][1]
        for name, outcome in outcomes.items():
            click.echo(
                f"{prefix}{name}: "
                + _anchor_message(old, new, outcome, *values.get(name, (None, None)), dry_run)
            )

    if dry_run:
        if to_copy:
            copy_anchors()
        click.echo(
            f"{prefix}would rename destination {old!r} -> {new!r} in the configuration"
            + (f" and in the remote: lists of {', '.join(referencing)}" if referencing else "")
        )
        return

    def commit(lock: Any = None):
        try:
            return commit_config(
                resolved, snapshot, new_config, warn=warn, announce=False, lock=lock
            )
        except BaseException as exc:
            _interrupted_write(exc, resolved, "rename")
            tail = restore_text()
            if tail:
                exc.add_note(tail)
            raise

    if not to_copy:
        result = commit()
    else:
        # The copies change ZFS state that only a successful rename justifies,
        # so nobody else may write between "the configuration is still as I
        # read it" and the write: hold the writer lock (just the flock -- no
        # database connection is open during the zfs calls) across all three.
        with writer_lock(resolved) as held:
            _require_generation(resolved, snapshot)
            copy_anchors()
            result = commit(held)
    try:
        click.echo(f"Renamed destination {old!r} -> {new!r} in the configuration.")
        stale = [n for n, o in outcomes.items() if o != "no-source"]
        if stale:
            commands = _inherit_commands_for(stale, old_prop)
            click.echo(
                f"Restart the daemon now so it uses the new name {new!r}. Until then, and after: "
                f"the daemon protects every local or received org.zfsbackup:anchor.* property, so "
                f"anchor.{old} keeps protecting its snapshot from pruning on "
                f"{', '.join(repr(n) for n in stale)}. Once a send to {new!r} has succeeded, "
                f"clear the old property (until then it pins that snapshot):\n{commands}"
            )
        notify_daemon(resolved, result.generation)
    except BaseException as exc:
        _interrupted_after_write(
            exc, resolved, f"the rename of destination {old!r} was applied", None, []
        )
        raise

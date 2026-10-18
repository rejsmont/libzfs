"""`zfsbackup-config edit` -- the whole configuration in `$EDITOR` (plan item 12).

The flow, in the order it runs:

0. The caller has read `snapshot = (config, generation)` (`write.read_snapshot`
   leaves no connection open). **No database handle exists while the editor
   runs**, however long that takes.
1. The configuration is rendered (`render.render_yaml`, the same text `export`
   prints, under a short `#` header) into `config.yaml` in a private
   (`mkdtemp`, 0700) directory, file mode 0600.
2. `--editor` > `$VISUAL` > `$EDITOR` > `vi`, split with `shlex.split` (so
   `EDITOR="code -w"` works) and run with **inherited stdio, never captured**.
   An editor that exits non-zero or is killed by a signal aborts: nothing is
   written and the buffer's path is printed.
3. "No changes" is a *canonical* comparison: both configurations are rendered
   with `render_yaml` and compared as text. That covers every value and every
   `Duration` literal (so `1h` -> `60m` is a real edit that persists), and is
   neither a byte comparison (comments, ordering and quoting are noise) nor a
   dataclass `==` (`Duration.__eq__` ignores literals). Exact-duplicate
   retention rules are deduplicated by the render (D1).
4. The edited text is parsed through `BackupConfig.from_file`'s guards
   (`dotpath.parse_dict`) and `validate_config`. Any problem is printed (a YAML
   syntax error with its line) and the operator may Retry -- the editor is
   re-opened on the same buffer -- or Abort. Only the warnings the edit
   introduces are shown (the caller installs the baseline suppression).
5. Names are *identity as set membership*: a name that appears is a create, a
   name that disappears is a delete. A rename-shaped diff -- exactly one dataset
   (or one destination) deleted and exactly one created, with otherwise
   identical fields, literals included -- is refused: applying it would
   orphan the dataset's ZFS user properties (and, for a destination, its
   anchors). `allow_recreate=True` (`--allow-recreate`) applies it as the
   delete + create it is. Dropping a destination from a dataset's `remote:`
   (or deleting a destination) warns that `org.zfsbackup:anchor.<dest>` stops
   protecting snapshots.
6. `write.commit_config(resolved, snapshot, new)` writes with a compare-and-swap
   on the snapshot's generation. A concurrent write (`ConfigConflict`, exit 75)
   or a held writer lock (`ConfigBusy`, 75) leaves the database untouched.

The temporary directory is removed after a successful write and after "no
changes". In every other case -- abort, editor failure, validation given up,
conflict, ^C -- it is **kept** and its path is printed on stderr, so the
operator's edit is never lost.

Prompting: a terminal is required (`stdin` must be a tty) unless `prompt=False`
(`--no-prompt`), the mode for scripts and tests: no terminal needed, never
prompts, and a rejected buffer aborts (exit 1, buffer kept). The tty test is
`_stdin_is_tty`, a module-level function so a test can patch it.
"""

import copy
import difflib
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import click
import yaml

from zfsbackup.cli import dotpath
from zfsbackup.cli.errors import CliError
from zfsbackup.cli.render import (
    render_dataset_fragment,
    render_destination_fragment,
    render_yaml,
)
from zfsbackup.cli.write import commit_config, notify_daemon, release_database
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    ConfigSnapshot,
    ResolvedConfigPath,
    validate_config,
)

__all__ = ["edit_config", "edit_dataset", "edit_destination", "editor_argv"]

_HEADER = """\
# zfsbackup configuration. Save and quit to apply it; quit without saving (or
# make no real change) to leave the stored configuration as it is.
# Dataset and destination NAMES are identity: a new name creates one, a name
# you delete deletes one. To rename, quit and use `zfsbackup-config rename`.
# (Comments are not stored.)
"""


class EditAborted(CliError):
    """The operator, or the editor, ended the edit. Nothing was written."""


def _stdin_is_tty() -> bool:
    try:
        return sys.stdin.isatty()
    except (AttributeError, ValueError):
        return False


# --------------------------------------------------------------------------
# The editor
# --------------------------------------------------------------------------


def editor_argv(override: Optional[str] = None) -> List[str]:
    """The editor command: `override`, else `$VISUAL`, else `$EDITOR`, else
    `vi`, split like a shell would (`code -w` is two words)."""
    for source, value in (
        ("--editor", override),
        ("$VISUAL", os.environ.get("VISUAL")),
        ("$EDITOR", os.environ.get("EDITOR")),
    ):
        if value is None or not value.strip():
            continue
        try:
            argv = shlex.split(value)
        except ValueError as exc:
            raise CliError(f"cannot parse {source} ({value!r}): {exc}") from exc
        if argv:
            return argv
    return ["vi"]


#: How long a forwarded SIGTERM may take before the editor is killed.
_TERMINATE_GRACE = 10.0


def _run_editor(argv: Sequence[str], buffer: Path) -> None:
    """Run the editor on `buffer` with the terminal's stdio, and require a
    clean exit.

    Signals follow what git does while it waits for an editor: SIGINT and
    SIGQUIT are ignored here for the duration (a ^C at the terminal goes to the
    whole foreground process group, so the editor sees it and decides; this
    process must not die and take the editor's buffer with it), and a SIGTERM
    is forwarded to the editor, which is then given `_TERMINATE_GRACE` seconds
    before it is killed. The previous handlers are restored afterwards; a
    forwarded SIGTERM is then re-delivered to this process (so it unwinds
    exactly as `kill` always does: buffer kept, exit 143), and an editor that
    itself died of SIGINT is reported as an interrupt (exit 130).
    """
    release_database()  # nothing may stay open while a human types
    try:
        # No stdin/stdout/stderr arguments: the editor inherits ours. The
        # editor is started BEFORE the signals are ignored: an ignored
        # disposition would be inherited across exec.
        child = subprocess.Popen([*argv, str(buffer)])
    except FileNotFoundError:
        raise CliError(
            f"editor not found: {argv[0]!r} (set $VISUAL or $EDITOR, or use --editor)"
        ) from None
    except OSError as exc:
        raise CliError(f"cannot run the editor {argv[0]!r}: {exc.strerror or exc}") from exc

    terminated_at: List[float] = []

    def forward_term(signum, frame):  # noqa: ARG001
        if not terminated_at:
            terminated_at.append(time.monotonic())
        try:
            child.send_signal(signal.SIGTERM)
        except OSError:
            pass

    previous: Dict[int, Any] = {}
    try:
        for signum, handler in (
            (signal.SIGINT, signal.SIG_IGN),
            (signal.SIGQUIT, signal.SIG_IGN),
            (signal.SIGTERM, forward_term),
        ):
            previous[signum] = signal.signal(signum, handler)
    except ValueError:
        # Not the main thread: signals cannot be rearranged; wait plainly.
        previous = {}
    try:
        while True:
            try:
                code = child.wait(timeout=0.25)
                break
            except subprocess.TimeoutExpired:
                if terminated_at and time.monotonic() - terminated_at[0] > _TERMINATE_GRACE:
                    child.kill()
    except BaseException:
        # Anything that still got through (a handler that could not be
        # installed): never leave the editor running unattended.
        child.kill()
        child.wait()
        raise
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)

    if terminated_at:
        # Re-deliver to ourselves with the original handler back in place.
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.5)  # the handler raises here; a default handler ends us
    if code == -signal.SIGINT:
        raise KeyboardInterrupt
    if code < 0:
        try:
            how = signal.Signals(-code).name
        except ValueError:
            how = f"signal {-code}"
        raise EditAborted(f"the editor ({argv[0]}) was killed by {how}; nothing was changed")
    if code != 0:
        raise EditAborted(f"the editor ({argv[0]}) exited with status {code}; nothing was changed")


# --------------------------------------------------------------------------
# Parsing and checking the edited text
# --------------------------------------------------------------------------


def _describe_problem(exc: BaseException, text: str) -> str:
    """The message to show for a rejected buffer, with the offending line for
    a YAML syntax error."""
    if isinstance(exc, yaml.MarkedYAMLError) and exc.problem_mark is not None:
        mark = exc.problem_mark
        lines = text.splitlines()
        shown = lines[mark.line] if 0 <= mark.line < len(lines) else ""
        what = exc.problem or "invalid YAML"
        return (
            f"invalid YAML at line {mark.line + 1}, column {mark.column + 1}: {what}\n"
            f"    {shown}\n"
            f"    {' ' * mark.column}^"
        )
    if isinstance(exc, yaml.YAMLError):
        return f"invalid YAML: {exc}"
    return str(exc) or type(exc).__name__


def _entries(config: BackupConfig) -> Tuple[Dict[str, dict], Dict[str, dict], dict]:
    """(datasets by name, destinations by name, everything else) as the plain
    mappings `export` prints -- literals included."""
    data = dotpath.to_dict(config)
    datasets = {e["name"]: e for e in data.get("datasets", [])}
    destinations = dict(data.get("destinations") or {})
    rest = {k: v for k, v in data.items() if k not in ("datasets", "destinations")}
    return datasets, destinations, rest


def _without_name(entry: dict) -> dict:
    return {k: v for k, v in entry.items() if k != "name"}


def _rename_shaped(
    old: Dict[str, dict], new: Dict[str, dict], strip_name: bool
) -> Optional[Tuple[str, str]]:
    gone = [n for n in old if n not in new]
    made = [n for n in new if n not in old]
    if len(gone) != 1 or len(made) != 1:
        return None
    before, after = old[gone[0]], new[made[0]]
    if strip_name:
        before, after = _without_name(before), _without_name(after)
    return (gone[0], made[0]) if before == after else None


def _check_rename(old: BackupConfig, new: BackupConfig) -> Optional[str]:
    old_ds, old_dest, _ = _entries(old)
    new_ds, new_dest, _ = _entries(new)
    pair = _rename_shaped(old_ds, new_ds, strip_name=True)
    if pair is not None:
        kind, a, b = "dataset", pair[0], pair[1]
    else:
        pair = _rename_shaped(old_dest, new_dest, strip_name=False)
        if pair is None:
            return None
        kind, a, b = "destination", pair[0], pair[1]
    return (
        f"this looks like a rename ({kind} {a!r} -> {b!r}); use "
        f"`zfsbackup-config rename {kind} {a} {b}` instead, or pass "
        "`--allow-recreate` if you really mean delete-and-create."
    )


def _anchor_warnings(old: BackupConfig, new: BackupConfig) -> List[str]:
    """Warnings for every destination a dataset stops sending to, or that is
    deleted. Its `anchor.<dest>` property stays behind: the daemon protects
    the snapshot of every local or received anchor (user decision, 2026-10-18), so it
    keeps that snapshot from being pruned but never advances again -- the
    operator should clear it once it is no longer needed."""
    old_ds, old_dest, _ = _entries(old)
    new_ds, new_dest, _ = _entries(new)
    messages: List[str] = []
    covered: Set[str] = set()
    for name, entry in old_ds.items():
        if name not in new_ds:
            continue
        before = {r["destination"] for r in entry.get("remote", [])}
        after = {r["destination"] for r in new_ds[name].get("remote", [])}
        for dest in sorted(before - after):
            covered.add(dest)
            messages.append(
                f"datasets[{name}] no longer sends to {dest!r}: its "
                f"org.zfsbackup:anchor.{dest} property stays behind and keeps "
                "pinning that snapshot against pruning; clear it with "
                f"`zfs inherit org.zfsbackup:anchor.{dest} {shlex.quote(name)}` once it is "
                "no longer needed"
            )
    for dest in sorted(set(old_dest) - set(new_dest) - covered):
        messages.append(
            f"destination {dest!r} is deleted: datasets that sent to it keep "
            f"their org.zfsbackup:anchor.{dest} property, which keeps pinning "
            "its snapshot against pruning until you clear it with "
            f"`zfs inherit org.zfsbackup:anchor.{dest} <dataset>`"
        )
    return messages


def _where(resolved: ResolvedConfigPath) -> str:
    """` -c <db>` unless the database is the default one."""
    if getattr(resolved, "source", "default") == "default":
        return ""
    return f" -c {shlex.quote(str(resolved.resolved_path))}"


def _recreate_warnings(old: BackupConfig, new: BackupConfig) -> List[str]:
    """A deleted dataset plus a created one that send to a common destination
    is probably a rename the exact-match detector missed (something else
    changed too): delete + create orphans the ZFS properties and the server
    copy, which `rename dataset` handles."""
    old_ds, _, _ = _entries(old)
    new_ds, _, _ = _entries(new)
    messages: List[str] = []
    for gone, before in old_ds.items():
        if gone in new_ds:
            continue
        for made, after in new_ds.items():
            if made in old_ds or _without_name(before) == _without_name(after):
                continue
            shared = sorted(
                {r["destination"] for r in before.get("remote", [])}
                & {r["destination"] for r in after.get("remote", [])}
            )
            if shared:
                messages.append(
                    f"dataset {gone!r} is deleted and {made!r} created, and both send to "
                    f"{', '.join(shared)}: if this is a rename, quit and use "
                    f"`zfsbackup-config rename dataset {gone} {made}` -- deleting and creating "
                    "leaves the old dataset's org.zfsbackup:* properties stale and makes "
                    "the next backup a full send"
                )
    return messages


def _destination_name_warnings(old: BackupConfig, new: BackupConfig) -> List[str]:
    """A destination created by this edit whose name cannot be part of a ZFS
    property name. Only a warning: configurations that already have such a
    name keep working (their anchors just cannot be stored)."""
    messages: List[str] = []
    for name in sorted(set(new.destinations) - set(old.destinations)):
        problem = dotpath.destination_name_problem(name)
        if problem:
            messages.append(
                f"destination {name!r}: {problem}; its anchor property cannot be set"
            )
    return messages


def _summary(old: BackupConfig, new: BackupConfig) -> str:
    old_ds, old_dest, old_rest = _entries(old)
    new_ds, new_dest, new_rest = _entries(new)
    parts: List[str] = []

    def membership(noun: str, a: Dict[str, Any], b: Dict[str, Any]) -> None:
        created = [n for n in b if n not in a]
        deleted = [n for n in a if n not in b]
        changed = [n for n in b if n in a and a[n] != b[n]]
        for label, names in (("created", created), ("deleted", deleted), ("changed", changed)):
            if names:
                parts.append(f"{noun} {label}: {', '.join(names)}")

    membership("datasets", old_ds, new_ds)
    membership("destinations", old_dest, new_dest)
    settings = sorted(k for k in set(old_rest) | set(new_rest) if old_rest.get(k) != new_rest.get(k))
    if settings:
        parts.append(f"settings changed: {', '.join(settings)}")
    return "; ".join(parts) if parts else "literal-only changes"


# --------------------------------------------------------------------------
# The command
# --------------------------------------------------------------------------


def _write_buffer(directory: Path, text: str) -> Path:
    path = directory / "config.yaml"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    return path


def _ask_retry() -> bool:
    try:
        answer = click.prompt(
            "[r]etry editing or [a]bort?",
            type=click.Choice(["r", "a"], case_sensitive=False),
            default="r",
            err=True,
        )
    except click.Abort:  # EOF or ^C at the prompt: abort
        click.echo(err=True)
        return False
    return answer.lower() == "r"


_TOP_KEYS = (
    "snapshot_prefix", "check_interval", "prune_interval", "api_host", "api_port",
    "dry_run", "client_id_file", "destinations", "remote_backup", "datasets",
)
_DATASET_KEYS = ("name", "enabled", "frequency", "recursive", "retention", "remote")
_REMOTE_KEYS = ("destination", "frequency", "retention")
_DESTINATION_KEYS = ("url",)
_SERVER_KEYS = ("target_dataset", "enabled")


def _unknown_keys(where: str, mapping: Any, valid: Sequence[str]) -> Optional[str]:
    """A message for the first key of `mapping` outside `valid`, or None.
    The model ignores keys it does not know, so a misspelt `retenton:` would
    silently fall back to a default; in an edit buffer that is refused."""
    if not isinstance(mapping, dict):
        return None
    for key in mapping:
        if key in valid:
            continue
        close = difflib.get_close_matches(str(key), valid, n=1)
        hint = f" (did you mean {close[0]!r}?)" if close else ""
        return (
            f"{where}: unknown key {str(key)!r}{hint}; valid keys: {', '.join(valid)}"
        )
    return None


def _check_keys(data: Any) -> None:
    """Raise `ValueError` for any key outside the configuration schema."""
    if not isinstance(data, dict):
        return
    problem = _unknown_keys("top level", data, _TOP_KEYS)
    if problem is None:
        problem = _unknown_keys("remote_backup", data.get("remote_backup"), _SERVER_KEYS)
    destinations = data.get("destinations")
    if problem is None and isinstance(destinations, dict):
        for name, body in destinations.items():
            problem = _unknown_keys(f"destinations[{name}]", body, _DESTINATION_KEYS)
            if problem:
                break
    datasets = data.get("datasets")
    if problem is None and isinstance(datasets, list):
        for index, entry in enumerate(datasets):
            label = f"datasets[{entry.get('name', index) if isinstance(entry, dict) else index}]"
            problem = _unknown_keys(label, entry, _DATASET_KEYS)
            if problem is None and isinstance(entry, dict) and isinstance(entry.get("remote"), list):
                for position, remote in enumerate(entry["remote"]):
                    dest = remote.get("destination", position) if isinstance(remote, dict) else position
                    problem = _unknown_keys(f"{label}.remote[{dest}]", remote, _REMOTE_KEYS)
                    if problem:
                        break
            if problem:
                break
    if problem:
        raise ValueError(problem)


def _unique_rules(rules):
    seen = set()
    kept = []
    for rule in rules:
        key = (rule.age.total_seconds(), rule.keep_for.total_seconds())
        if key not in seen:
            seen.add(key)
            kept.append(rule)
    return kept


def _canonical(config: BackupConfig) -> BackupConfig:
    """A copy of `config` as the store would keep it: exact-duplicate retention
    rules (same age AND keep_for, whatever their literals) collapsed to the
    first (D1). "No changes" and the rename check compare this, not the raw
    parse: a buffer whose only difference is a duplicate rule changes nothing."""
    canon = copy.deepcopy(config)
    for ds in canon.datasets:
        ds.retention_rules = _unique_rules(ds.retention_rules)
        for remote in ds.remote:
            remote.retention_rules = _unique_rules(remote.retention_rules)
    return canon


def _undeclared_destinations(data: Any) -> Optional[str]:
    """A message listing every destination that datasets send to but the
    buffer does not declare (and the datasets that do), or None. Checked on
    the raw mapping so a deleted-but-still-referenced destination names ALL
    its referencing datasets, not just the first one `from_file` meets."""
    if not isinstance(data, dict):
        return None
    declared = data.get("destinations")
    declared = declared if isinstance(declared, dict) else {}
    users: Dict[str, List[str]] = {}
    datasets = data.get("datasets")
    for entry in datasets if isinstance(datasets, list) else []:
        if not isinstance(entry, dict) or not isinstance(entry.get("remote"), list):
            continue
        for remote in entry["remote"]:
            dest = remote.get("destination") if isinstance(remote, dict) else None
            if isinstance(dest, str) and dest not in declared:
                users.setdefault(dest, []).append(str(entry.get("name")))
    if not users:
        return None
    parts = [
        f"destination {dest!r} is not declared under destinations: but is still used by "
        f"{', '.join(names)}"
        for dest, names in users.items()
    ]
    return (
        "; ".join(parts)
        + ". Remove it from those datasets' remote: lists first, or keep the destination."
    )


def _parse_whole(text: str) -> BackupConfig:
    data = yaml.safe_load(text)
    _check_keys(data)
    problem = _undeclared_destinations(data)
    if problem:
        raise ValueError(problem)
    return dotpath.parse_dict(data)


def _fragment(text: str, what: str) -> dict:
    """The mapping in an entry buffer. A `name:` key is refused (identity is
    changed by `rename` only); an empty buffer is refused rather than read as
    "everything default"."""
    data = yaml.safe_load(text)
    if data is None:
        raise ValueError(
            f"the buffer is empty; quit the editor without saving to abandon the edit "
            f"(an empty {what} would reset every setting to its default)"
        )
    if not isinstance(data, dict):
        raise ValueError(f"the buffer must be a mapping of settings, got {type(data).__name__}")
    if "name" in data:
        raise ValueError(
            f"a name: key is not accepted here; the {what}'s name is its identity and "
            f"is changed only with `zfsbackup-config rename {what} <old> <new>`"
        )
    return data


def _dataset_parser(config: BackupConfig, name: str) -> Callable[[str], BackupConfig]:
    def parse(text: str) -> BackupConfig:
        fragment = _fragment(text, "dataset")
        data = dotpath.to_dict(config)
        entries = data["datasets"]
        index = next(i for i, e in enumerate(entries) if e["name"] == name)
        entries[index] = {"name": name, **fragment}
        _check_keys(data)
        problem = _undeclared_destinations(data)
        if problem:
            raise ValueError(problem)
        return dotpath.parse_dict(data)

    return parse


def _destination_parser(config: BackupConfig, name: str) -> Callable[[str], BackupConfig]:
    def parse(text: str) -> BackupConfig:
        fragment = _fragment(text, "destination")
        unknown = sorted(set(fragment) - {"url"})
        if unknown:
            raise ValueError(
                f"unknown key(s) {', '.join(map(repr, unknown))}; a destination has only a url:"
            )
        data = dotpath.to_dict(config)
        data["destinations"][name] = {"url": fragment.get("url")}
        return dotpath.parse_dict(data)

    return parse


def _session(
    resolved: ResolvedConfigPath,
    snapshot: ConfigSnapshot,
    *,
    header: str,
    initial_body: str,
    parse: Callable[[str], BackupConfig],
    check_rename: bool,
    warn: Callable[[str], None],
    editor: Optional[str],
    allow_recreate: bool,
    prompt: bool,
) -> None:
    """The edit loop shared by the whole-config and single-entry commands."""
    if prompt and not _stdin_is_tty():
        raise CliError(
            "edit needs an interactive terminal (stdin is not a tty). To edit "
            "non-interactively, `zfsbackup-config export -o FILE`, change FILE, "
            "and `zfsbackup-config import FILE` (a full replace, with no check "
            "for concurrent changes); or pass --no-prompt to run the editor "
            "without prompting."
        )
    argv = editor_argv(editor)
    original = snapshot.config
    original_text = render_yaml(original)

    directory = Path(tempfile.mkdtemp(prefix="zfsbackup-config-edit-"))
    keep = True
    committing = False
    try:
        os.chmod(directory, 0o700)
        initial = header + initial_body
        buffer = _write_buffer(directory, initial)

        while True:
            _run_editor(argv, buffer)
            try:
                text = buffer.read_text(encoding="utf-8")
            except FileNotFoundError:
                raise EditAborted(
                    "the editor removed the buffer; nothing was changed"
                ) from None
            except UnicodeDecodeError as exc:
                text, problem = "", f"the buffer is not valid UTF-8: {exc}"
            else:
                problem = None

            if problem is None and text == initial:
                keep = False
                click.echo("No changes; the stored configuration was not touched.")
                return

            new: Optional[BackupConfig] = None
            if problem is None:
                try:
                    new = parse(text)
                    canon = _canonical(new)
                    if render_yaml(canon) == original_text:
                        keep = False
                        click.echo("No changes; the stored configuration was not touched.")
                        return
                    if check_rename and not allow_recreate:
                        problem = _check_rename(original, canon)
                    if problem is None:
                        for message in validate_config(new):
                            warn(message)
                except (ValueError, yaml.YAMLError) as exc:
                    problem = _describe_problem(exc, text)

            if problem is None:
                break
            click.echo(f"error: {problem}", err=True)
            if not (prompt and _ask_retry()):
                raise EditAborted(
                    "edit abandoned; nothing was written"
                    + ("" if prompt else " (--no-prompt never retries)")
                )

        assert new is not None
        canon = _canonical(new)
        for message in (
            _anchor_warnings(original, canon)
            + _recreate_warnings(original, canon)
            + _destination_name_warnings(original, canon)
        ):
            warn(message)
        committing = True
        # No database handle exists yet; `commit_config` opens one, compares
        # the generation inside its write transaction, and closes it again.
        result = commit_config(resolved, snapshot, new, warn=warn, announce=False)
        keep = False
        click.echo(f"Configuration saved ({_summary(original, new)}).")
        notify_daemon(resolved, result.generation)
    except BaseException as exc:
        # The note is printed after the error itself (errors.report, and the
        # ^C handler, print `__notes__`).
        if keep:
            if committing and not isinstance(exc, Exception):
                # Interrupted while the write was in flight (it may even have
                # committed): do not claim either way.
                exc.add_note(
                    "the edit may or may not have been applied; run "
                    f"`zfsbackup-config{_where(resolved)} export` to check. Your edit is "
                    f"kept in {directory / 'config.yaml'}"
                )
            else:
                exc.add_note(
                    f"your edit is kept in {directory / 'config.yaml'}; nothing was "
                    "written to the database"
                )
        raise
    finally:
        if not keep:
            shutil.rmtree(directory, ignore_errors=True)


def edit_config(
    resolved: ResolvedConfigPath,
    snapshot: ConfigSnapshot,
    *,
    warn: Callable[[str], None],
    editor: Optional[str] = None,
    allow_recreate: bool = False,
    prompt: bool = True,
) -> None:
    """Run the whole-config edit described in the module docstring. Returns
    normally for a successful write and for "no changes"; raises otherwise
    (`EditAborted`, `ConfigConflict`, ... -- all leaving the stored
    configuration untouched, and the buffer on disk)."""
    _session(
        resolved, snapshot,
        header=_HEADER, initial_body=render_yaml(snapshot.config), parse=_parse_whole,
        check_rename=True, warn=warn, editor=editor,
        allow_recreate=allow_recreate, prompt=prompt,
    )


def _entry_header(kind: str, name: str, also: str) -> str:
    return (
        f"# Editing {kind} {name!r}. Save and quit to apply it; quit without saving\n"
        f"# (or make no real change) to leave it as it is.\n"
        f"# The name is not part of this buffer: it is the {kind}'s identity and is\n"
        f"# changed only with `zfsbackup-config rename {kind} <old> <new>`.\n"
        f"# {also}\n"
        "# (Comments are not stored.)\n"
    )


def edit_dataset(
    resolved: ResolvedConfigPath,
    snapshot: ConfigSnapshot,
    name: str,
    *,
    warn: Callable[[str], None],
    editor: Optional[str] = None,
    prompt: bool = True,
) -> None:
    """Edit one dataset's settings (never its name) in `$EDITOR`."""
    config = snapshot.config
    ds = next((d for d in config.datasets if d.name == name), None)
    if ds is None:
        names = ", ".join(d.name for d in config.datasets)
        raise CliError(f"no dataset named {name!r}; datasets: {names}")
    _session(
        resolved, snapshot,
        header=_entry_header(
            "dataset", name,
            "Omitted keys take their defaults (as in a config file): enabled: true, "
            "frequency: 1h, retention: 1d -> 30d, no remote:.",
        ),
        initial_body=render_dataset_fragment(ds), parse=_dataset_parser(config, name),
        check_rename=False, warn=warn, editor=editor, allow_recreate=True, prompt=prompt,
    )


def edit_destination(
    resolved: ResolvedConfigPath,
    snapshot: ConfigSnapshot,
    name: str,
    *,
    warn: Callable[[str], None],
    editor: Optional[str] = None,
    prompt: bool = True,
) -> None:
    """Edit one destination's `url` (never its name) in `$EDITOR`."""
    config = snapshot.config
    dest = config.destinations.get(name)
    if dest is None:
        names = ", ".join(sorted(config.destinations)) or "(none)"
        raise CliError(f"no destination named {name!r}; destinations: {names}")
    _session(
        resolved, snapshot,
        header=_entry_header("destination", name, "A destination has only a url:."),
        initial_body=render_destination_fragment(dest), parse=_destination_parser(config, name),
        check_rename=False, warn=warn, editor=editor, allow_recreate=True, prompt=prompt,
    )

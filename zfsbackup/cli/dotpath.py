"""Dot-path addressing of single settings: `zfsbackup-config get / set / unset`
(plan item 11).

Two halves, both pure functions of a `BackupConfig` (the caller reads it with
`write.read_snapshot` and writes the result with `write.commit_config`; nothing
here touches the database):

**Resolution** -- `resolve(text, config)` turns what the operator typed into a
tuple of canonical segments, e.g. ``("datasets", "tank/vm.01", "frequency")``;
`format_path` turns segments back into the one form this tool ever prints, the
bracket form ``datasets[tank/vm.01].frequency``.

ZFS dataset names contain dots and slashes (``tank/vm.01``), so splitting the
path on ``.`` would silently address the wrong thing. Resolution is therefore
directed by the schema and by the names that exist: at ``datasets.`` (and
``destinations.``, and ``remote.`` under a dataset) every existing name is tried,
and every reading of the remainder that is a structurally valid path is kept.
Exactly one valid reading resolves; more than one is an *ambiguity* and an error
that asks for the bracket form (``datasets[a.retention]`` vs
``datasets[a].retention``) -- this module never guesses. The bracket form is
always accepted, with the same grammar::

    snapshot_prefix | check_interval | prune_interval | api_host | api_port
    | dry_run | client_id_file
    datasets . NAME . (enabled | recursive | frequency)
    datasets . NAME . retention [ . AGE ]
    datasets . NAME . remote . DEST . frequency
    datasets . NAME . remote . DEST . retention [ . AGE ]
    destinations . NAME . url
    remote_backup . (target_dataset | enabled)

(``NAME`` may be written ``[NAME]`` instead of ``.NAME``; so may ``DEST`` and
``AGE``.) The name and membership paths (``datasets.N.name``, ``datasets.N``,
``datasets.N.remote``, ``destinations.N``, ...) resolve too -- `get` shows them
read-only -- but `set` / `unset` refuse them and point at `rename` / `edit`.

**Editing** -- `set_value` / `unset_value` return a new `BackupConfig`. The
value is parsed with ``yaml.safe_load`` (so ``true``, ``yes``, ``1``, ``5m``,
``forever`` mean exactly what they mean in a config file) and written into the
YAML *dict* of the rendered configuration, which is then parsed back through
`BackupConfig.from_file` -- the very guards `import` applies: the audit's
rejections, the integer 0/1 acceptance with a warning, ``0m`` as "inherit",
``client_id_file ""`` as unset, durations stored as typed. `BackupConfig` has no
public from-dict constructor, so the dict is written to a private (0700
directory, 0600 file) temporary YAML and read with `from_file`; the file is
removed before returning.

Retention rules are matched by their length in seconds (``retention.7d`` finds
a rule written ``1w``) and stored with the literal as typed. Removing a
dataset's last rule leaves it with ``retention: forever`` (never prune); a
destination override's last rule removed leaves it inheriting.
"""

import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import yaml

from zfsbackup.cli.errors import CliError
from zfsbackup.cli.render import duration_literal, render_yaml
from zfsbackup.config.store import destination_name_problem  # noqa: F401 - re-exported
from zfsbackup.config import BackupConfig, DatasetConfig, Duration, RemoteDatasetConfig, RetentionRule

__all__ = [
    "Change",
    "NOT_SET",
    "PathError",
    "compact",
    "destination_name_problem",
    "format_path",
    "prefix_change_warnings",
    "get_lines",
    "kind_of",
    "parse_dict",
    "resolve",
    "set_value",
    "to_dict",
    "unset_value",
]

Segs = Tuple[str, ...]

GLOBAL_SETTINGS: Tuple[str, ...] = (
    "snapshot_prefix", "check_interval", "prune_interval",
    "api_host", "api_port", "dry_run", "client_id_file",
)
_DATASET_FIELDS = ("enabled", "recursive", "frequency")
_SERVER_FIELDS = ("target_dataset", "enabled")
_ROOTS: Tuple[str, ...] = GLOBAL_SETTINGS + ("datasets", "destinations", "remote_backup")
_MAX_LISTED = 12

#: The tri-states (plan item 3c): `unset` means "derive it at run time".
_TRI_STATE = ("prune_interval", "client_id_file")
_TEXT_SETTINGS = ("snapshot_prefix", "api_host", "client_id_file")

_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class PathError(CliError):
    """A path or value that cannot be used. Exit 1; nothing was written."""


# --------------------------------------------------------------------------
# Printing paths
# --------------------------------------------------------------------------


def _bracket(name: str) -> str:
    # A name containing ']' cannot be bracketed; the dotted form still resolves
    # it (longest existing name wins).
    return f"[{name}]" if "]" not in name else f".{name}"


def format_path(segs: Segs) -> str:
    """The bracket form of `segs`: ``datasets[tank/vm.01].remote[offsite].frequency``."""
    out = segs[0]
    i = 1
    if segs[0] in ("datasets", "destinations") and len(segs) > 1:
        out += _bracket(segs[1])
        i = 2
    while i < len(segs):
        part = segs[i]
        if segs[0] == "datasets" and i == 2 and part == "remote":
            out += ".remote"
            if i + 1 < len(segs):
                out += _bracket(segs[i + 1])
            i += 2
            continue
        out += "." + part
        i += 1
    return out


def kind_of(segs: Segs) -> str:
    """What `segs` addresses (the editing and reading code dispatches on it)."""
    head, n = segs[0], len(segs)
    if head in GLOBAL_SETTINGS:
        return "global"
    if head == "datasets":
        if n == 1:
            return "datasets"
        if n == 2:
            return "dataset"
        part = segs[2]
        if part == "name":
            return "dataset_name"
        if part in _DATASET_FIELDS:
            return "dataset_field"
        if part == "retention":
            return "retention" if n == 3 else "rule"
        if part == "remote":
            if n == 3:
                return "remotes"
            if n == 4:
                return "remote"
            sub = segs[4]
            if sub == "destination":
                return "remote_name"
            if sub == "frequency":
                return "remote_frequency"
            if sub == "retention":
                return "remote_retention" if n == 5 else "remote_rule"
    if head == "destinations":
        if n == 1:
            return "destinations"
        if n == 2:
            return "destination"
        return "destination_name" if segs[2] == "name" else "destination_url"
    if head == "remote_backup":
        return "server" if n == 1 else "server_field"
    raise AssertionError(f"unclassified path {segs!r}")  # pragma: no cover


# --------------------------------------------------------------------------
# Resolution
# --------------------------------------------------------------------------


def _listed(names: Sequence[str]) -> str:
    shown = ", ".join(names[:_MAX_LISTED])
    if len(names) > _MAX_LISTED:
        shown += f", ... ({len(names) - _MAX_LISTED} more)"
    return shown or "(none)"


def _split_word(text: str) -> Tuple[str, str]:
    match = _WORD.match(text)
    if not match:
        return "", text
    return match.group(), text[match.end():]


class _Resolver:
    """Collects every structurally valid reading of a path (a generator per
    grammar production) and the failures of the readings that were not."""

    def __init__(self, config: BackupConfig) -> None:
        self.config = config
        self.failures: List[Tuple[int, str]] = []

    def fail(self, depth: int, message: str) -> None:
        self.failures.append((depth, message))

    def best_failure(self) -> str:
        if not self.failures:
            return "no such setting"
        deepest = max(depth for depth, _ in self.failures)
        return next(msg for depth, msg in self.failures if depth == deepest)

    # -- productions ---------------------------------------------------

    def root(self, text: str) -> Iterator[Segs]:
        word, tail = _split_word(text)
        if word in GLOBAL_SETTINGS:
            if tail:
                self.fail(1, f"{word} has no sub-settings")
                return
            yield (word,)
        elif word == "datasets":
            names = [ds.name for ds in self.config.datasets]
            for name, rest in self._named(("datasets",), tail, "dataset", names):
                if name is None:
                    yield ("datasets",)
                else:
                    yield from self._dataset(("datasets", name), rest)
        elif word == "destinations":
            names = sorted(self.config.destinations)
            for name, rest in self._named(("destinations",), tail, "destination", names):
                if name is None:
                    yield ("destinations",)
                elif rest is None:
                    yield ("destinations", name)
                else:
                    sub, subtail = _split_word(rest)
                    if sub in ("url", "name") and not subtail:
                        yield ("destinations", name, sub)
                    elif sub in ("url", "name"):
                        self.fail(2, f"{format_path(('destinations', name))}.{sub} has no sub-settings")
                    else:
                        self.fail(
                            2,
                            f"{format_path(('destinations', name))} has no setting {rest!r}; valid: url",
                        )
        elif word == "remote_backup":
            if not tail:
                yield ("remote_backup",)
                return
            if tail[0] != ".":
                self.fail(1, f"unexpected {tail!r} after remote_backup")
                return
            sub, subtail = _split_word(tail[1:])
            if sub in _SERVER_FIELDS and not subtail:
                yield ("remote_backup", sub)
            else:
                self.fail(
                    1,
                    f"remote_backup has no setting {tail[1:]!r}; valid: {', '.join(_SERVER_FIELDS)}",
                )
        else:
            shown = word or text
            self.fail(
                0,
                f"no setting {shown!r}; valid: {', '.join(_ROOTS)} "
                "(per-dataset settings are under datasets[NAME], destinations under destinations[NAME])",
            )

    def _named(
        self, segs: Segs, tail: str, noun: str, names: Sequence[str]
    ) -> Iterator[Tuple[Optional[str], Optional[str]]]:
        """Readings of `tail` (what follows a collection keyword) as a name and
        the remainder after it. `(None, None)` is the collection itself."""
        depth = len(segs)
        where = format_path(segs)
        if not tail:
            yield None, None
            return
        if tail[0] == "[":
            end = tail.find("]")
            if end < 0:
                self.fail(depth, f"unterminated '[' after {where}")
                return
            name, after = tail[1:end], tail[end + 1:]
            if after and after[0] != ".":
                self.fail(depth, f"unexpected {after!r} after {where}[{name}]")
                return
            if name not in names:
                self.fail(depth, f"no {noun} named {name!r}; {noun}s: {_listed(names)}")
                return
            yield name, (after[1:] if after else None)
            return
        if tail[0] != ".":
            self.fail(depth, f"unexpected {tail!r} after {where}")
            return
        body = tail[1:]
        matched = False
        # Every existing name that is a prefix of the remainder: the caller
        # keeps whichever readings turn out to be valid paths.
        for name in sorted(names, key=len, reverse=True):
            if body == name:
                matched = True
                yield name, None
            elif body.startswith(name + "."):
                matched = True
                yield name, body[len(name) + 1:]
        if not matched:
            self.fail(
                depth,
                f"no {noun} matches {body!r}; {noun}s: {_listed(names)}. A name that "
                f"contains dots can be written {where}[NAME]",
            )

    def _age(self, segs: Segs, tail: str) -> Iterator[Segs]:
        if not tail:
            yield segs
            return
        if tail[0] == ".":
            age = tail[1:]
        elif tail[0] == "[" and tail.endswith("]") and tail.count("]") == 1:
            age = tail[1:-1]
        else:
            self.fail(len(segs), f"unexpected {tail!r} after {format_path(segs)}")
            return
        if not age:
            self.fail(len(segs), f"{format_path(segs)}. needs a rule age (e.g. 1d)")
            return
        try:
            Duration(age)
        except ValueError:
            # A reading whose age cannot be a duration can never succeed;
            # it must not count as a (possibly ambiguous) reading.
            self.fail(
                len(segs) + 1,
                f"{format_path(segs)}: {age!r} is not a duration (examples: 30m, 1d, 1w, 1M, 1y)",
            )
            return
        yield segs + (age,)

    def _dataset(self, segs: Segs, rest: Optional[str]) -> Iterator[Segs]:
        if rest is None:
            yield segs
            return
        ds = next(d for d in self.config.datasets if d.name == segs[1])
        word, tail = _split_word(rest)
        valid = _DATASET_FIELDS + ("retention", "remote")
        if word == "name":
            if tail:
                self.fail(2, f"{format_path(segs)}.name has no sub-settings")
            else:
                yield segs + ("name",)
        elif word in _DATASET_FIELDS:
            if tail:
                self.fail(2, f"{format_path(segs)}.{word} has no sub-settings")
            else:
                yield segs + (word,)
        elif word == "retention":
            yield from self._age(segs + ("retention",), tail)
        elif word == "remote":
            attached = [r.destination for r in ds.remote]
            for dest, after in self._named(segs + ("remote",), tail, "attached destination", attached):
                if dest is None:
                    yield segs + ("remote",)
                else:
                    yield from self._remote(segs + ("remote", dest), after)
        else:
            self.fail(
                2,
                f"{format_path(segs)} has no setting {rest!r}; valid: {', '.join(valid)}",
            )

    def _remote(self, segs: Segs, rest: Optional[str]) -> Iterator[Segs]:
        if rest is None:
            yield segs
            return
        word, tail = _split_word(rest)
        if word in ("frequency", "destination"):
            if tail:
                self.fail(4, f"{format_path(segs)}.{word} has no sub-settings")
            else:
                yield segs + (word,)
        elif word == "retention":
            yield from self._age(segs + ("retention",), tail)
        else:
            self.fail(
                4,
                f"{format_path(segs)} has no setting {rest!r}; valid: frequency, retention",
            )


def resolve(text: str, config: BackupConfig) -> Segs:
    """The canonical segments for the path `text`; `PathError` for an unknown
    path (listing the valid siblings) or an ambiguous one (never guessed)."""
    resolver = _Resolver(config)
    found: List[Segs] = []
    for segs in resolver.root(text):
        if segs not in found:
            found.append(segs)
    if len(found) == 1:
        return found[0]
    if len(found) > 1:
        readings = " and ".join(format_path(s) for s in found)
        raise PathError(
            f"ambiguous path {text!r}: it can mean {readings}. "
            "Use the bracket form to say which."
        )
    raise PathError(f"unknown path {text!r}: {resolver.best_failure()}")


# --------------------------------------------------------------------------
# Reading
# --------------------------------------------------------------------------


def to_dict(config: BackupConfig) -> Dict[str, Any]:
    """The YAML mapping of `config` exactly as `export` would print it."""
    return yaml.safe_load(render_yaml(config))


def _dump(obj: Any) -> List[str]:
    text = yaml.safe_dump(
        obj, sort_keys=False, default_flow_style=False, allow_unicode=True, width=1 << 30
    )
    return text.rstrip("\n").splitlines()


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _lit(value, key: str) -> str:
    return duration_literal(value, key)


def _dataset(config: BackupConfig, name: str) -> DatasetConfig:
    return next(ds for ds in config.datasets if ds.name == name)


def _remote(ds: DatasetConfig, dest: str) -> RemoteDatasetConfig:
    return next(r for r in ds.remote if r.destination == dest)


def _rule_lines(rules: Sequence[RetentionRule], key: str) -> List[str]:
    return [
        f"{_lit(r.age, key)}: {_lit(r.keep_for, key)}"
        for r in sorted(rules, key=lambda rule: rule.age)
    ]


def _seconds(age: str, where: str) -> float:
    try:
        return Duration(age).total_seconds()
    except ValueError:
        raise PathError(
            f"{where}: {age!r} is not a duration (examples: 30m, 1d, 1w, 1M, 1y)"
        ) from None


def _find_rule(rules: Sequence[RetentionRule], age: str, where: str) -> Optional[RetentionRule]:
    seconds = _seconds(age, where)
    return next((r for r in rules if r.age.total_seconds() == seconds), None)


def _refuse_name(kind: str) -> PathError:
    if kind == "dataset_name":
        return PathError(
            "names are not addressable here; rename a dataset with "
            "`zfsbackup-config rename dataset <old> <new>`"
        )
    return PathError(
        "names are not addressable here; rename a destination with "
        "`zfsbackup-config rename destination <old> <new>`"
    )


def get_lines(config: BackupConfig, segs: Segs) -> List[str]:
    """The value at `segs`, one printable line per item, as stored (a duration
    keeps the literal it was written with). Raises `PathError` for a setting
    that does not exist in this configuration."""
    kind = kind_of(segs)
    path = format_path(segs)
    if kind == "global":
        key = segs[0]
        value = getattr(config, key)
        if key == "prune_interval":
            if value is None:
                return [f"derived ({_lit(config.check_interval, 'check_interval')})"]
            return [_lit(value, key)]
        if key == "check_interval":
            return [_lit(value, key)]
        if key == "client_id_file":
            if value is None:
                return ["derived ($HOME/.config/zfsbackup/client_id)"]
            return [str(value)]
        if key == "dry_run":
            return [_bool(value)]
        return [str(value)]
    if kind in ("datasets", "destinations", "remotes"):
        noun = "destinations" if kind == "destinations" else "datasets"
        raise PathError(f"{path} is a collection; use `zfsbackup-config list {noun}`")
    if kind in ("dataset_name", "remote_name", "destination_name"):
        raise _refuse_name(kind)
    if kind == "dataset":
        return _dump(_dataset_entry(config, segs[1]))
    if kind == "remote":
        entry = next(
            r for r in _dataset_entry(config, segs[1])["remote"] if r["destination"] == segs[3]
        )
        return _dump(entry)
    if kind == "destination":
        return _dump({"url": config.destinations[segs[1]].url})
    if kind == "dataset_field":
        ds = _dataset(config, segs[1])
        field_name = segs[2]
        if field_name == "frequency":
            return [_lit(ds.frequency, path)]
        return [_bool(getattr(ds, field_name))]
    if kind == "retention":
        ds = _dataset(config, segs[1])
        if not ds.retention_rules:
            return ["forever"]
        return _rule_lines(ds.retention_rules, path)
    if kind == "rule":
        ds = _dataset(config, segs[1])
        rule = _find_rule(ds.retention_rules, segs[3], path)
        if rule is None:
            have = ", ".join(_rule_lines(ds.retention_rules, path)) or "none (never prune)"
            raise PathError(f"{path}: no retention rule with age {segs[3]}; rules: {have}")
        return [_lit(rule.keep_for, path)]
    if kind == "remote_frequency":
        ds = _dataset(config, segs[1])
        remote = _remote(ds, segs[3])
        if remote.frequency is None:
            return [f"inherit ({_lit(ds.frequency, f'datasets[{ds.name}].frequency')})"]
        return [_lit(remote.frequency, path)]
    if kind == "remote_retention":
        ds = _dataset(config, segs[1])
        remote = _remote(ds, segs[3])
        if remote.retention_rules:
            return _rule_lines(remote.retention_rules, path)
        inherited = (
            ", ".join(_rule_lines(ds.retention_rules, path)) if ds.retention_rules else "forever"
        )
        return [f"inherit ({inherited})"]
    if kind == "remote_rule":
        ds = _dataset(config, segs[1])
        remote = _remote(ds, segs[3])
        if not remote.retention_rules:
            raise PathError(
                f"{path}: this destination has no retention override; it inherits the "
                f"dataset's rules (see {format_path(segs[:-1])})"
            )
        rule = _find_rule(remote.retention_rules, segs[5], path)
        if rule is None:
            have = ", ".join(_rule_lines(remote.retention_rules, path))
            raise PathError(f"{path}: no retention rule with age {segs[5]}; rules: {have}")
        return [_lit(rule.keep_for, path)]
    if kind == "destination_url":
        return [config.destinations[segs[1]].url]
    if kind in ("server", "server_field"):
        server = config.remote_backup
        if server is None:
            raise PathError(
                f"{path}: no remote_backup section (this host is not configured to "
                "receive backups)"
            )
        if kind == "server":
            return _dump({"enabled": server.enabled, "target_dataset": server.target_dataset})
        if segs[1] == "enabled":
            return [_bool(server.enabled)]
        return [server.target_dataset]
    raise AssertionError(kind)  # pragma: no cover


def _dataset_entry(config: BackupConfig, name: str) -> Dict[str, Any]:
    return next(e for e in to_dict(config)["datasets"] if e["name"] == name)


NOT_SET = "(not set)"


def compact(config: BackupConfig, segs: Segs) -> str:
    """`get_lines` on one line, for "old -> new" messages; `(not set)` when the
    setting does not exist in `config`."""
    try:
        return ", ".join(get_lines(config, segs))
    except PathError:
        return NOT_SET


# --------------------------------------------------------------------------
# Editing
# --------------------------------------------------------------------------


@dataclass
class Change:
    """The outcome of a `set` / `unset`: the new configuration, warnings this
    module adds on top of the model's own, and -- for a retention-rule edit --
    the retention scope whose effective set the caller should show."""

    config: BackupConfig
    warnings: List[str] = field(default_factory=list)
    scope: Optional[Segs] = None


def parse_dict(data: Dict[str, Any]) -> BackupConfig:
    """`data` (a config-file mapping) through `BackupConfig.from_file`, so it
    meets every YAML-boundary guard and logs the model's parse warnings."""
    with tempfile.TemporaryDirectory(prefix="zfsbackup-config-") as tmp:
        os.chmod(tmp, 0o700)
        path = Path(tmp) / "config.yaml"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="ascii") as handle:
            # ASCII-only output: `from_file` opens the file in the locale's
            # encoding, and escapes read back the same in every one.
            yaml.safe_dump(
                data, handle, sort_keys=False, default_flow_style=False,
                allow_unicode=False, width=1 << 30,
            )
        return BackupConfig.from_file(path)


def parse_value(text: str) -> Any:
    """The value a config file would hold for `text` (``yaml.safe_load``)."""
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        reason = str(exc).splitlines()[0] if str(exc) else "invalid YAML"
        raise PathError(
            f"cannot read the value {text!r} as YAML ({reason}); "
            "quote it, e.g. '\"your text\"'"
        ) from exc


def _require_text(value: Any, path: str, text: str) -> None:
    if value is not None and not isinstance(value, str):
        raise PathError(
            f"{path} must be a string, got {type(value).__name__} {value!r} (values are "
            f"read as YAML; to set the literal text {text!r}, quote it: '\"{text}\"')"
        )


def _unsettable(path: str) -> PathError:
    return PathError(f"{path} always has a value and cannot be unset; use `set`")


def _membership(kind: str, path: str) -> PathError:
    if kind in ("dataset_name", "remote_name", "destination_name"):
        return _refuse_name(kind)
    if kind in ("datasets", "dataset"):
        return PathError(
            f"{path}: datasets are added and removed with `zfsbackup-config edit` "
            "(the whole configuration), not here"
        )
    if kind in ("destinations", "destination"):
        return PathError(
            f"{path}: destinations are added and removed with `zfsbackup-config edit`, not here"
        )
    return PathError(
        f"{path}: which destinations a dataset sends to is changed with "
        "`zfsbackup-config edit dataset <name>`, not here"
    )


_MEMBERSHIP_KINDS = (
    "datasets", "dataset", "dataset_name", "destinations", "destination",
    "destination_name", "remotes", "remote", "remote_name",
)


def _entry(data: Dict[str, Any], name: str) -> Dict[str, Any]:
    return next(e for e in data["datasets"] if e["name"] == name)


def _remote_entry(entry: Dict[str, Any], dest: str) -> Dict[str, Any]:
    return next(r for r in entry["remote"] if r["destination"] == dest)


def _mapping_of(holder: Dict[str, Any]) -> Dict[Any, Any]:
    current = holder.get("retention")
    return dict(current) if isinstance(current, dict) else {}


def _put_rule(mapping: Dict[Any, Any], age: str, keep_for: Any, where: str) -> None:
    seconds = _seconds(age, where)
    for key in list(mapping):
        try:
            if Duration(key).total_seconds() == seconds:
                del mapping[key]
        except (ValueError, TypeError):
            continue
    # One rule per keep_for: say which existing rule the new one duplicates,
    # with both literals, rather than leaving it to the store's check.
    try:
        keep_seconds = Duration(keep_for).total_seconds() if isinstance(keep_for, str) else None
    except ValueError:
        keep_seconds = None
    if keep_seconds is not None:
        for key, value in mapping.items():
            try:
                if Duration(value).total_seconds() == keep_seconds:
                    raise PathError(
                        f"{where}: duplicate rule: keep_for {keep_for} ({age}: {keep_for}) is "
                        f"the same duration as the existing rule {key}: {value}; "
                        "a retention set has one rule per keep_for"
                    )
            except (ValueError, TypeError):
                continue
    mapping[age] = keep_for


def _drop_rule(mapping: Dict[Any, Any], age: str, where: str) -> None:
    seconds = _seconds(age, where)
    for key in list(mapping):
        try:
            if Duration(key).total_seconds() == seconds:
                del mapping[key]
                return
        except (ValueError, TypeError):
            continue
    have = ", ".join(f"{k}: {v}" for k, v in mapping.items()) or "none"
    raise PathError(f"{where}: no retention rule with age {age}; rules: {have}")


def _reparse(data: Dict[str, Any], path: str) -> BackupConfig:
    """`parse_dict`, with a refusal naming the setting that was being changed."""
    try:
        return parse_dict(data)
    except ValueError as exc:
        message = str(exc)
        # The model's messages usually name their key already
        # (`datasets[x].frequency must be ...`): do not say it twice.
        raise PathError(message if message.startswith(path) else f"{path}: {message}") from exc


def prefix_change_warnings(old: BackupConfig, new: BackupConfig) -> List[str]:
    if old.snapshot_prefix == new.snapshot_prefix:
        return []
    return [
        f"snapshot_prefix changes from {old.snapshot_prefix!r} to {new.snapshot_prefix!r}: "
        f"existing {old.snapshot_prefix}_* snapshots will no longer be managed or pruned "
        "(they stay until destroyed by hand)"
    ]


def _pruning_starts(old: BackupConfig, new: BackupConfig, segs: Segs) -> List[str]:
    """A warning when this edit gives a never-pruned dataset retention rules:
    the pruning worker starts destroying its old snapshots."""
    if kind_of(segs) not in ("retention", "rule"):
        return []
    before = _dataset(old, segs[1])
    after = _dataset(new, segs[1])
    if before.retention_rules or not after.retention_rules:
        return []
    return [
        f"datasets[{segs[1]}] was never pruned; it now has retention rules, so the pruning "
        "worker will start destroying its snapshots that are older than they allow"
    ]


_RECEIVE_WARNING = (
    "remote_backup is enabled: this host accepts incoming backup streams on "
    "{host}:{port}, and that API has no authentication"
)


def set_value(config: BackupConfig, segs: Segs, text: str) -> Change:
    """Set the setting at `segs` to `text` (read as YAML). `PathError` /
    `ValueError` leave nothing changed -- this returns a new object."""
    kind = kind_of(segs)
    path = format_path(segs)
    if kind in _MEMBERSHIP_KINDS:
        raise _membership(kind, path)
    if kind == "server":
        raise PathError(
            f"{path}: set remote_backup.target_dataset (creates the section, enabled) "
            "or remote_backup.enabled; `unset remote_backup` removes it"
        )
    value = parse_value(text)
    data = to_dict(config)
    warnings: List[str] = []
    scope: Optional[Segs] = None

    if kind == "global":
        key = segs[0]
        if key in _TEXT_SETTINGS:
            _require_text(value, path, text)
        if key in _TRI_STATE and (value is None or value == ""):
            data.pop(key, None)  # "" and null both mean unset (A-14.1)
        else:
            data[key] = value
    elif kind == "dataset_field":
        _entry(data, segs[1])[segs[2]] = value
    elif kind == "retention":
        entry = _entry(data, segs[1])
        if value is None:
            entry.pop("retention", None)  # absent = the 1d -> 30d default
        else:
            entry["retention"] = value  # 'forever' or a mapping; the model validates
    elif kind == "rule":
        entry = _entry(data, segs[1])
        mapping = _mapping_of(entry)
        _put_rule(mapping, segs[3], value, path)
        entry["retention"] = mapping
        scope = segs[:3]
    elif kind == "remote_frequency":
        remote = _remote_entry(_entry(data, segs[1]), segs[3])
        if value is None:
            remote.pop("frequency", None)
        else:
            remote["frequency"] = value
    elif kind == "remote_retention":
        remote = _remote_entry(_entry(data, segs[1]), segs[3])
        if value is None:
            remote.pop("retention", None)
        else:
            remote["retention"] = value
    elif kind == "remote_rule":
        remote = _remote_entry(_entry(data, segs[1]), segs[3])
        # The first override rule REPLACES the inherited set: start empty,
        # never from the dataset's rules.
        mapping = _mapping_of(remote)
        _put_rule(mapping, segs[5], value, path)
        remote["retention"] = mapping
        scope = segs[:5]
    elif kind == "destination_url":
        _require_text(value, path, text)
        data["destinations"][segs[1]]["url"] = value
    elif kind == "server_field":
        server = data.get("remote_backup")
        if segs[1] == "target_dataset":
            _require_text(value, path, text)
            if not isinstance(server, dict):
                data["remote_backup"] = {"enabled": True, "target_dataset": value}
                warnings.append(
                    _RECEIVE_WARNING.format(host=config.api_host, port=config.api_port)
                )
            else:
                server["target_dataset"] = value
        else:
            if not isinstance(server, dict):
                raise PathError(
                    f"{path}: there is no remote_backup section; "
                    "set remote_backup.target_dataset to create it"
                )
            server["enabled"] = value
    else:  # pragma: no cover
        raise AssertionError(kind)

    new = _reparse(data, path)
    warnings.extend(_pruning_starts(config, new, segs))
    warnings.extend(prefix_change_warnings(config, new))
    if (
        kind == "server_field"
        and segs[1] == "enabled"
        and new.remote_backup is not None
        and new.remote_backup.enabled
        and not (config.remote_backup and config.remote_backup.enabled)
    ):
        warnings.append(
            _RECEIVE_WARNING.format(host=new.api_host, port=new.api_port)
        )
    return Change(new, warnings, scope)


def unset_value(config: BackupConfig, segs: Segs) -> Change:
    """Return `config` with the setting at `segs` unset (see the module
    docstring for what each kind of setting reverts to)."""
    kind = kind_of(segs)
    path = format_path(segs)
    if kind in _MEMBERSHIP_KINDS:
        raise _membership(kind, path)
    data = to_dict(config)
    scope: Optional[Segs] = None

    if kind == "global":
        if segs[0] not in _TRI_STATE:
            raise _unsettable(path)
        data.pop(segs[0], None)
    elif kind in ("dataset_field", "destination_url"):
        raise _unsettable(path)
    elif kind == "retention":
        _entry(data, segs[1]).pop("retention", None)  # the 1d -> 30d default
    elif kind == "rule":
        entry = _entry(data, segs[1])
        mapping = _mapping_of(entry)
        _drop_rule(mapping, segs[3], path)
        entry["retention"] = mapping or "forever"
        scope = segs[:3]
    elif kind == "remote_frequency":
        _remote_entry(_entry(data, segs[1]), segs[3]).pop("frequency", None)
    elif kind == "remote_retention":
        _remote_entry(_entry(data, segs[1]), segs[3]).pop("retention", None)
    elif kind == "remote_rule":
        remote = _remote_entry(_entry(data, segs[1]), segs[3])
        if not isinstance(remote.get("retention"), dict):
            raise PathError(
                f"{path}: this destination has no retention override; it already "
                "inherits the dataset's rules"
            )
        mapping = _mapping_of(remote)
        _drop_rule(mapping, segs[5], path)
        if mapping:
            remote["retention"] = mapping
        else:
            remote.pop("retention", None)  # last override rule gone = inherit
        scope = segs[:5]
    elif kind == "server":
        data.pop("remote_backup", None)
    elif kind == "server_field":
        raise PathError(
            f"{path} cannot be unset on its own; `unset remote_backup` removes the "
            "receive role, or `set` it"
        )
    else:  # pragma: no cover
        raise AssertionError(kind)

    new = _reparse(data, path)
    return Change(new, _pruning_starts(config, new, segs), scope)

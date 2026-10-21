"""Render a `BackupConfig` as YAML that `BackupConfig.from_file` reads back.

This is the one serialiser the CLI has. `export` writes it to stdout or a
file, `import` writes it as the pre-import backup of the outgoing
configuration, and the editor commands (plan items 12 and 14) render the
buffer the operator edits with it.

The contract (docs/config_db_cli_plan.md, item 10.2) is a round trip:
`BackupConfig.from_file(render_yaml(c))` reproduces `c` in value AND in
duration literal (`Duration.literal`; `Duration.__eq__` ignores literals, so a
`30d` that came back as `1M` would compare equal and still be a silent edit),
with one exception -- D1's lossy dedupe of exact-duplicate retention rules,
which `save_config` applies on the way into the store and which therefore never
reaches a rendered config.

Choices that make the contract hold:

- Fields whose "unset" state means "derive it at run time" (`prune_interval`,
  `client_cert_file` / `client_key_file`; plan item 3c) and a destination's
  `server_fingerprint` ("not enrolled") are omitted when `None`, never filled
  in. The fingerprint, like every string, is double-quoted: an all-digit hex
  string would otherwise be read back by YAML as an integer.
- The client id is not configuration (it lives in the config database) and is
  never rendered; neither is the retired `client_id_file`.
- `enabled`, `recursive` and `dry_run` are always written as explicit booleans:
  under YAML an empty value means falsy, so leaving a flag out of a rendering
  would make its meaning depend on the reader's defaults.
- A dataset with no retention rules is "never prune", spelled
  `retention: forever`. An absent `retention:` key would read back as the
  1d->30d default. A destination's `retention` is written only when it
  overrides the dataset's rules, because an empty override is rejected on read.
- Destinations are sorted by name (the store has no stable order for them);
  datasets and a dataset's `remote:` entries keep the config's own order, which
  is the order the operator wrote them in and the order the daemon walks them.
- Durations are double-quoted literals, so a literal such as `30d` or `12h6d`
  is never reinterpreted by YAML and is stored exactly as written.

The output deliberately carries no comments: any YAML round trip through a
database discards them, and pretending otherwise would be misleading.
"""

import textwrap
from datetime import timedelta
from typing import Dict, List

import yaml

from zfsbackup.config import BackupConfig, DatasetConfig, Destination, Duration, RetentionRule

__all__ = [
    "render_yaml",
    "render_dataset_fragment",
    "render_destination_fragment",
    "duration_literal",
]


class _Quoted(str):
    """A string always emitted double-quoted."""


class _Dumper(yaml.SafeDumper):
    """`SafeDumper` that indents sequences under their key (`datasets:` then
    an indented `- name:`), which PyYAML's default does not."""

    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def _represent_quoted(dumper: yaml.SafeDumper, value: _Quoted):
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(value), style='"')


_Dumper.add_representer(_Quoted, _represent_quoted)


def _dump(obj) -> str:
    return yaml.dump(
        obj,
        Dumper=_Dumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=1 << 30,
    )


def duration_literal(value: timedelta, key: str = "duration") -> str:
    """The literal to write for `value`: the `Duration`'s own, exactly as it was
    typed. A plain `timedelta` (an in-code config) gets the canonical one.

    Raises `ValueError` naming `key` for a sub-second value, which has no
    literal in the grammar and must not be silently truncated.
    """
    literal = value.literal if isinstance(value, Duration) else Duration(value).literal
    if literal is None:
        raise ValueError(
            f"{key}: {value!r} has sub-second precision and cannot be written "
            "as a duration literal"
        )
    return literal


def _retention_mapping(rules: List[RetentionRule], key: str) -> Dict[_Quoted, _Quoted]:
    """`{age: keep_for}` as quoted literals, ordered by age (as `from_file`
    sorts them). Two rules with the same age literal and the same keep_for are
    an exact duplicate and collapse; the same age with a different keep_for
    cannot be written as one mapping key and is refused."""
    mapping: Dict[_Quoted, _Quoted] = {}
    for rule in sorted(rules, key=lambda r: r.age):
        age = _Quoted(duration_literal(rule.age, f"{key} age"))
        keep_for = _Quoted(duration_literal(rule.keep_for, f"{key} keep_for"))
        if age in mapping and mapping[age] != keep_for:
            raise ValueError(
                f"{key}: age {str(age)!r} appears twice with different keep_for "
                f"({str(mapping[age])!r} and {str(keep_for)!r}); cannot be written"
            )
        mapping[age] = keep_for
    return mapping


def _dataset_entry(ds: DatasetConfig) -> dict:
    key = f"datasets[{ds.name}]"
    entry: dict = {
        "name": _Quoted(ds.name),
        "enabled": bool(ds.enabled),
        "frequency": _Quoted(duration_literal(ds.frequency, f"{key}.frequency")),
        "recursive": bool(ds.recursive),
    }
    if ds.retention_rules:
        entry["retention"] = _retention_mapping(ds.retention_rules, f"{key}.retention")
    else:
        entry["retention"] = "forever"
    if ds.remote:
        remotes = []
        for remote in ds.remote:
            rkey = f"{key}.remote[{remote.destination}]"
            item: dict = {"destination": _Quoted(remote.destination)}
            if remote.frequency is not None:
                item["frequency"] = _Quoted(
                    duration_literal(remote.frequency, f"{rkey}.frequency")
                )
            if remote.retention_rules:
                item["retention"] = _retention_mapping(
                    remote.retention_rules, f"{rkey}.retention"
                )
            remotes.append(item)
        entry["remote"] = remotes
    return entry


def _destination_entry(dest: Destination) -> dict:
    entry: dict = {"url": _Quoted(dest.url)}
    if dest.server_fingerprint is not None:
        entry["server_fingerprint"] = _Quoted(dest.server_fingerprint)
    return entry


def render_yaml(config: BackupConfig) -> str:
    """Render `config` as YAML in the layout of `zfsbackup/config.example.yaml`
    (no comments). See the module docstring for the round-trip contract."""
    top: dict = {
        "snapshot_prefix": _Quoted(config.snapshot_prefix),
        "check_interval": _Quoted(duration_literal(config.check_interval, "check_interval")),
    }
    if config.prune_interval is not None:
        top["prune_interval"] = _Quoted(
            duration_literal(config.prune_interval, "prune_interval")
        )
    top["api_host"] = _Quoted(config.api_host)
    top["api_port"] = int(config.api_port)
    top["dry_run"] = bool(config.dry_run)
    if config.client_cert_file is not None:
        top["client_cert_file"] = _Quoted(str(config.client_cert_file))
    if config.client_key_file is not None:
        top["client_key_file"] = _Quoted(str(config.client_key_file))

    chunks = [_dump(top)]

    if config.destinations:
        chunks.append(_dump({
            "destinations": {
                _Quoted(name): _destination_entry(dest)
                for name, dest in sorted(config.destinations.items())
            }
        }))

    if config.remote_backup is not None:
        chunks.append(_dump({
            "remote_backup": {
                "enabled": bool(config.remote_backup.enabled),
                "target_dataset": _Quoted(config.remote_backup.target_dataset),
            }
        }))

    if config.datasets:
        entries = [
            textwrap.indent(_dump([_dataset_entry(ds)]), "  ") for ds in config.datasets
        ]
        chunks.append("datasets:\n" + "\n".join(entries))
    else:
        chunks.append("datasets: []\n")

    return "\n".join(chunks)


def render_dataset_fragment(ds: DatasetConfig) -> str:
    """One dataset's settings as a YAML mapping WITHOUT its `name` (the buffer
    of `edit dataset`: a name is identity, changed only by `rename`). Parsed
    back, with the name restored, by the same per-dataset path as an entry of
    a config file."""
    entry = _dataset_entry(ds)
    entry.pop("name")
    return _dump(entry)


def render_destination_fragment(dest: Destination) -> str:
    """One destination's settings (`url`, and `server_fingerprint` once enrolled)
    as a YAML mapping, without its name."""
    return _dump(_destination_entry(dest))

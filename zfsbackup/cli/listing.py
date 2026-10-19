"""`zfsbackup-config list datasets` / `list destinations` -- read-only views of
the stored configuration (plan item 13).

Both views are pure functions of a `BackupConfig` (the caller reads it with
`write.read_snapshot`, which leaves no connection open), so nothing here can
touch the database.

Order: datasets are listed in stored order (the order of their database ids,
which is the order the operator wrote them and the order the daemon walks
them); destinations by name.

Text columns follow the daemon's startup report (`DatasetManager.dataset_report`
in `backup_manager.py`: name, frequency, recursive, retention-rule count, remote
destinations) plus `enabled`, which the report implies by listing only enabled
datasets. `RETENTION` is `never` for a dataset with no retention rules (never
pruned) and the rule count otherwise. A remote destination that inherits the
dataset's frequency shows `inherit (X)`, X being the dataset frequency.
Durations are shown as typed (`30d` stays `30d`, never `1M`).

`--json` shape (stable; new keys may be added, none renamed or removed)::

    list datasets --json  ->  [
      {
        "name":            str,
        "enabled":         bool,
        "frequency":       str,           # duration literal, e.g. "1h"
        "recursive":       bool,
        "never_prune":     bool,          # true iff retention_rules is empty
        "retention_rules": [{"age": str, "keep_for": str}, ...],   # by age
        "remotes": [
          {
            "destination":         str,
            "frequency":           str | null,   # null = inherits the dataset's
            "effective_frequency": str,
            "retention_override":  [{"age": str, "keep_for": str}, ...] | null
                                                  # null = inherits the dataset's rules
          }, ...
        ]
      }, ...
    ]

    list destinations --json  ->  [
      {"name": str, "url": str, "datasets": [str, ...]}, ...
    ]            # "datasets": the datasets that send to it, in stored order

`--enabled-only` keeps enabled datasets; for destinations it counts only enabled
datasets as referrers and so drops a destination that no enabled dataset sends
to.
"""

import json
from typing import Any, Dict, List

from zfsbackup.cli.render import duration_literal
from zfsbackup.config import BackupConfig, DatasetConfig, RetentionRule

__all__ = ["render_datasets", "render_destinations"]


def _rules(rules: List[RetentionRule], key: str) -> List[Dict[str, str]]:
    return [
        {
            "age": duration_literal(r.age, f"{key} age"),
            "keep_for": duration_literal(r.keep_for, f"{key} keep_for"),
        }
        for r in sorted(rules, key=lambda r: r.age)
    ]


def _dataset_json(ds: DatasetConfig) -> Dict[str, Any]:
    key = f"datasets[{ds.name}]"
    remotes = []
    for r in ds.remote:
        remotes.append({
            "destination": r.destination,
            "frequency": (
                None if r.frequency is None
                else duration_literal(r.frequency, f"{key}.remote[{r.destination}].frequency")
            ),
            "effective_frequency": duration_literal(
                r.frequency if r.frequency is not None else ds.frequency,
                f"{key}.remote[{r.destination}].frequency",
            ),
            "retention_override": (
                _rules(r.retention_rules, f"{key}.remote[{r.destination}].retention")
                if r.retention_rules else None
            ),
        })
    return {
        "name": ds.name,
        "enabled": bool(ds.enabled),
        "frequency": duration_literal(ds.frequency, f"{key}.frequency"),
        "recursive": bool(ds.recursive),
        "never_prune": not ds.retention_rules,
        "retention_rules": _rules(ds.retention_rules, f"{key}.retention"),
        "remotes": remotes,
    }


def _table(header: List[str], rows: List[List[str]]) -> str:
    widths = [len(h) for h in header]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def line(cells: List[str]) -> str:
        # The last column is not padded: no trailing whitespace.
        padded = [c.ljust(widths[i]) for i, c in enumerate(cells[:-1])] + [cells[-1]]
        return "  ".join(padded).rstrip()

    return "".join(line(r) + "\n" for r in [header] + rows)


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def render_datasets(config: BackupConfig, *, enabled_only: bool = False, as_json: bool = False) -> str:
    """The `list datasets` output."""
    datasets = [ds for ds in config.datasets if ds.enabled or not enabled_only]
    entries = [_dataset_json(ds) for ds in datasets]
    if as_json:
        return json.dumps(entries, indent=2) + "\n"

    rows = []
    for e in entries:
        if e["remotes"]:
            remote = ", ".join(
                f"{r['destination']}: "
                + (f"inherit ({r['effective_frequency']})" if r["frequency"] is None else r["frequency"])
                for r in e["remotes"]
            )
        else:
            remote = "-"
        rows.append([
            e["name"],
            _yes_no(e["enabled"]),
            e["frequency"],
            _yes_no(e["recursive"]),
            "never" if e["never_prune"] else str(len(e["retention_rules"])),
            remote,
        ])
    if not rows:
        return "(no datasets)\n" if not enabled_only else "(no enabled datasets)\n"
    return _table(
        ["NAME", "ENABLED", "FREQUENCY", "RECURSIVE", "RETENTION", "REMOTE DESTINATIONS"], rows
    )


def render_destinations(config: BackupConfig, *, enabled_only: bool = False, as_json: bool = False) -> str:
    """The `list destinations` output."""
    referrers: Dict[str, List[str]] = {name: [] for name in config.destinations}
    for ds in config.datasets:
        if enabled_only and not ds.enabled:
            continue
        for r in ds.remote:
            referrers.setdefault(r.destination, []).append(ds.name)

    entries = [
        {"name": name, "url": dest.url, "datasets": referrers.get(name, [])}
        for name, dest in sorted(config.destinations.items())
        if referrers.get(name) or not enabled_only
    ]
    if as_json:
        return json.dumps(entries, indent=2) + "\n"
    if not entries:
        return "(no destinations)\n"
    rows = [
        [e["name"], e["url"], ", ".join(e["datasets"]) if e["datasets"] else "-"]
        for e in entries
    ]
    return _table(["NAME", "URL", "DATASETS"], rows)

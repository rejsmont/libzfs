"""Configuration file parser for ZFS backup daemon."""

import base64
import json
import re
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yaml


class Duration(timedelta):
    """A `timedelta` subclass that remembers the literal it was parsed from (or
    synthesizes one), so config round-trips keep a user's original duration text
    (e.g. ``"1M"`` rather than the numerically-equal-but-different ``"30d"``).

    `Duration` is a drop-in replacement for `timedelta`: comparisons (including
    reflected comparisons against a bare `timedelta`), arithmetic, hashing, and
    truthiness are all inherited unchanged. `__str__` is deliberately NOT
    overridden, so log output stays byte-identical to plain `timedelta`; use
    `.literal` or `.render()` to get the duration text.

    Accepts three construction forms, unambiguous because `timedelta`'s first
    positional argument is `days`, a number:

    - a literal string, single or compound (``Duration("1M")``, ``Duration("6d12h")``)
    - an existing `timedelta` (``Duration(timedelta(hours=1))``)
    - native `timedelta` positional/keyword arguments (``Duration(days=6, hours=12)``)
    """

    # Largest-unit-first, used for literal synthesis. `M` (months) and `y` (years)
    # are exact multiples of days in this scheme, so decomposition never approximates.
    _UNIT_SECONDS = (
        ('y', 365 * 86400),
        ('M', 30 * 86400),
        ('w', 7 * 86400),
        ('d', 86400),
        ('h', 3600),
        ('m', 60),
        ('s', 1),
    )

    _TERM_RE = re.compile(r'(\d+)([A-Za-z])')
    _COMPOUND_RE = re.compile(r'(?:\d+[A-Za-z])+')

    def __new__(cls, *args, **kwargs):
        literal = None
        if len(args) == 1 and not kwargs and isinstance(args[0], str):
            literal = args[0]
            parsed = cls._parse(literal)
            self = timedelta.__new__(
                cls, days=parsed.days, seconds=parsed.seconds, microseconds=parsed.microseconds
            )
        elif len(args) == 1 and not kwargs and isinstance(args[0], timedelta):
            td = args[0]
            self = timedelta.__new__(
                cls, days=td.days, seconds=td.seconds, microseconds=td.microseconds
            )
            if isinstance(td, Duration):
                # Copy-constructing from an existing Duration inherits its literal
                # rather than synthesizing a new one -- ``Duration(Duration("30d"))``
                # must stay "30d", not be rewritten to "1M". `.timedelta` remains the
                # documented way to deliberately shed the literal: it returns a
                # value of exactly type `timedelta`, which does not match this branch.
                literal = td.literal
        else:
            self = timedelta.__new__(cls, *args, **kwargs)

        if literal is None:
            literal = cls._synthesize(self)
        self._literal = literal
        return self

    @staticmethod
    def _parse(duration_str: str) -> timedelta:
        """Parse a (possibly compound) duration literal into a plain `timedelta`.

        Grammar: an optional leading ``-`` negating the whole value, then one or
        more ``(integer)(unit)`` terms with no separators or whitespace, summed.
        Units: ``s``/``h``/``d``/``w``/``y`` are case-insensitive; ``m`` (minutes)
        and ``M`` (months, 30d) are case-SENSITIVE and must never be confused with
        each other or lowercased before dispatch.
        """
        if not duration_str:
            raise ValueError("Duration string cannot be empty")

        negative = False
        body = duration_str
        if body[0] == '-':
            negative = True
            body = body[1:]

        if not body or not Duration._COMPOUND_RE.fullmatch(body):
            raise ValueError(f"Invalid duration format: {duration_str}")

        seen_units = set()
        total = timedelta()
        for value_str, unit in Duration._TERM_RE.findall(body):
            value = int(value_str)
            canonical = unit if unit in ('m', 'M') else unit.lower()
            if canonical in seen_units:
                raise ValueError(f"Duplicate duration unit '{unit}' in: {duration_str}")
            seen_units.add(canonical)

            if unit == 'm':
                total += timedelta(minutes=value)
            elif unit == 'M':
                total += timedelta(days=value * 30)
            elif unit.lower() == 's':
                total += timedelta(seconds=value)
            elif unit.lower() == 'h':
                total += timedelta(hours=value)
            elif unit.lower() == 'd':
                total += timedelta(days=value)
            elif unit.lower() == 'w':
                total += timedelta(weeks=value)
            elif unit.lower() == 'y':
                total += timedelta(days=value * 365)
            else:
                raise ValueError(f"Unknown duration unit: {unit}. Use s/m/h/d/w/M/y")

        return -total if negative else total

    @staticmethod
    def _synthesize(td: timedelta) -> Optional[str]:
        """Best-effort literal for a value with no literal of its own: greedy,
        largest-unit-first, exact decomposition. Returns `None` for sub-second
        values, which have no representable literal in this grammar.
        """
        if td.microseconds:
            return None

        total = td.days * 86400 + td.seconds
        if total == 0:
            return "0s"

        negative = total < 0
        remaining = abs(total)
        parts = []
        for unit, unit_seconds in Duration._UNIT_SECONDS:
            if remaining >= unit_seconds:
                count, remaining = divmod(remaining, unit_seconds)
                parts.append(f"{count}{unit}")

        literal = ''.join(parts)
        return f"-{literal}" if negative else literal

    def __reduce__(self):
        # timedelta.__reduce__ reconstructs via (cls, (days, seconds, microseconds))
        # and would drop the literal -- silently replacing it with the synthesized
        # canonical form rather than raising. Restore it explicitly via state.
        return (
            self.__class__,
            (self.days, self.seconds, self.microseconds),
            {'literal': self._literal},
        )

    def __setstate__(self, state):
        self._literal = state['literal']

    @property
    def literal(self) -> Optional[str]:
        """The source literal as written (``"1M"``, ``"6d12h"``), or a
        synthesized best-effort literal for values built from a `timedelta` or
        native kwargs. `None` only for sub-second values, which have no
        representable form.
        """
        return self._literal

    @property
    def timedelta(self) -> timedelta:
        """A pure `timedelta` with the same value and no literal attached. Use
        this wherever a value must be serialized, compared for exact type, or
        handed to code that must not see a `Duration`.
        """
        return timedelta(days=self.days, seconds=self.seconds, microseconds=self.microseconds)

    def render(self) -> str:
        """Return `literal` if set; otherwise raise -- sub-second values have no
        representable literal and must never be silently truncated.
        """
        if self._literal is not None:
            return self._literal
        raise ValueError(
            f"Duration({timedelta.__str__(self)}) has sub-second precision and no "
            "representable literal"
        )

    def __repr__(self) -> str:
        return f"Duration({self._literal!r}, {timedelta.__str__(self)})"


def parse_time_duration(duration_str: str) -> timedelta:
    """Backwards-compatible shim; parsing logic now lives in Duration."""
    return Duration(duration_str).timedelta


def _duration_from_config(value, key: str) -> Duration:
    """Build a `Duration` from a value taken directly off parsed YAML.

    `Duration`'s constructor accepts native `timedelta` kwargs (e.g.
    ``Duration(30)`` == ``Duration(days=30)``), which is deliberate for
    programmatic callers but dangerous at the YAML boundary: an untyped
    scalar like ``check_interval: 300`` would silently become "300 days"
    instead of failing loudly. Reject anything that isn't already a
    duration string here, before it reaches `Duration`.
    """
    if not isinstance(value, str):
        raise ValueError(
            f"{key} must be a duration string like '5m' or '6d12h', "
            f"got {type(value).__name__} {value!r}"
        )
    return Duration(value)


def _positive_duration_from_config(value, key: str) -> Duration:
    """Parse a retention-rule duration and reject a non-positive value.

    A zero (or negative) `age` is a zero-width slot: it is a division-by-zero
    in the slot-based retention algorithm (`DatasetManager.needs_prunning`,
    `interval_secs` is a divisor). A zero or negative `keep_for` means
    "expire immediately", which is at best a footgun and never a meaningful
    retention tier either. Both endpoints are rejected here at load, before
    either can reach the pruning worker.
    """
    d = _duration_from_config(value, key)
    if d.timedelta <= timedelta(0):
        raise ValueError(
            f"{key}: duration must be positive, got {_retention_literal(d)!r} "
            f"({d.total_seconds():.0f}s)"
        )
    return d


def _mapping_from_config(value, key: str, expected: str) -> dict:
    """Ensure a value taken directly off parsed YAML is a mapping before it is
    indexed or iterated with `.items()`/`.get()`.

    YAML happily parses a list or a bare scalar wherever a mapping is
    expected (e.g. ``retention: ["1d", "30d"]`` instead of
    ``retention: {1d: 30d}``); left unchecked that surfaces as a raw
    `AttributeError` deep inside dict-only code. Reject anything that isn't
    already a `dict` here, before it reaches such code.
    """
    if not isinstance(value, dict):
        raise ValueError(
            f"{key} must be {expected}, got {type(value).__name__} {value!r}"
        )
    return value


def _list_from_config(value, key: str, expected: str) -> list:
    """Ensure a value taken directly off parsed YAML is a list before it is
    indexed or iterated as a sequence of entries.

    YAML happily parses a mapping or a bare scalar wherever a list is
    expected (e.g. ``remote: {destination: offsite}`` instead of
    ``remote: [{destination: offsite}]``); left unchecked, iterating a
    mapping silently yields its keys instead of raising, and iterating a
    string silently yields characters. Reject anything that isn't already a
    `list` here, before it reaches such code.
    """
    if not isinstance(value, list):
        raise ValueError(
            f"{key} must be {expected}, got {type(value).__name__} {value!r}"
        )
    return value


@dataclass
class RetentionRule:
    """Retention rule defining one tier of a tiered retention policy."""
    age: timedelta
    keep_for: timedelta

    def __repr__(self):
        return f"RetentionRule(age={self.age}, keep_for={self.keep_for})"


def _retention_literal(d: timedelta) -> str:
    """Best-effort literal text for a duration, for validation error messages.

    Falls back to `str(d)` for a plain `timedelta` (e.g. one constructed
    directly in a test), which has no `.literal`.
    """
    literal = getattr(d, 'literal', None)
    return literal if literal is not None else str(d)


def validate_retention_uniqueness(rules: List[RetentionRule], key: str) -> None:
    """Raise ValueError if any two rules in `rules` share an `age` or a `keep_for`.

    Retention is meant to be a bijection between intervals (`age`) and expiries
    (`keep_for`): one rule per age, one rule per keep_for. This is the strict
    counterpart to the read-path resolution in `_collapse_retention_rules`
    (below) -- write paths (the future config CLI's `set`/`edit`, `import`)
    call this to reject ambiguous input outright, rather than silently
    resolving it the way a running daemon must. Two rules that are exact
    duplicates (same age AND same keep_for) are not a conflict and are
    allowed through.

    `key` is the caller's key path (e.g. ``datasets[pool/data].retention``),
    used in the error message the same way `_duration_from_config` names its
    key.
    """
    seen_keep_for: Dict[float, RetentionRule] = {}
    for rule in rules:
        keep_for = rule.keep_for.total_seconds()
        existing = seen_keep_for.get(keep_for)
        if existing is not None and existing.age.total_seconds() != rule.age.total_seconds():
            raise ValueError(
                f"{key}: {_retention_literal(existing.keep_for)!r} and "
                f"{_retention_literal(rule.keep_for)!r} are the same duration "
                f"({keep_for:.0f}s); one rule per keep_for"
            )
        seen_keep_for[keep_for] = rule

    seen_age: Dict[float, RetentionRule] = {}
    for rule in rules:
        age = rule.age.total_seconds()
        existing = seen_age.get(age)
        if existing is not None and existing.keep_for.total_seconds() != rule.keep_for.total_seconds():
            raise ValueError(
                f"{key}: {_retention_literal(existing.age)!r} and "
                f"{_retention_literal(rule.age)!r} are the same duration "
                f"({age:.0f}s); one rule per age"
            )
        seen_age[age] = rule


def _collapse_retention_rules(
    rules: List[RetentionRule],
) -> Tuple[Dict[float, float], List[str]]:
    """Collapse retention rules into a `{keep_for_seconds: age_seconds}` plan,
    resolving collisions on either axis toward retaining more data (see
    docs/config_db_cli_plan.md item 2b -- this is the pre-existing data-loss
    bug where the old `{rule.keep_for: rule.age for rule in rules}` dict
    comprehension silently discarded the finer-grained rule on any collision):

      - same `keep_for`, different `age`   -> take the min `age` (finer interval keeps more)
      - same `age`, different `keep_for`   -> take the max `keep_for` (longer expiry keeps more)

    Order is significant and the two passes do NOT commute: `keep_for` is
    collapsed first, then `age`. Collapsing `age` first can discard a rule
    that `keep_for`-first would have kept (see the plan for a worked example).

    A rule pair that is an exact duplicate (same age AND same keep_for) is not
    a conflict and is dropped silently -- no warning.

    This is the lenient read-path counterpart to `validate_retention_uniqueness`
    above -- callers that must resolve a running config rather than reject a
    write use this. Never raises. Returns the resolved plan plus a list of
    human-readable collision descriptions (empty if there was nothing to
    resolve); the caller decides if/when to log them.
    """
    warnings: List[str] = []

    # Pass 1: collapse rules sharing a keep_for -> keep the smaller age.
    by_keep_for: Dict[float, RetentionRule] = {}
    for rule in rules:
        keep_for = rule.keep_for.total_seconds()
        age = rule.age.total_seconds()
        existing = by_keep_for.get(keep_for)
        if existing is None:
            by_keep_for[keep_for] = rule
            continue
        if existing.age.total_seconds() == age:
            continue  # exact duplicate, silent
        chosen = existing if existing.age.total_seconds() <= age else rule
        warnings.append(
            f"retention rules {_retention_literal(existing.age)!r}:{_retention_literal(existing.keep_for)!r} "
            f"and {_retention_literal(rule.age)!r}:{_retention_literal(rule.keep_for)!r} have the same "
            f"keep_for ({keep_for:.0f}s); keeping the finer age {_retention_literal(chosen.age)!r}"
        )
        by_keep_for[keep_for] = chosen

    # Pass 2: collapse rules sharing an age -> keep the larger keep_for.
    by_age: Dict[float, RetentionRule] = {}
    for rule in by_keep_for.values():
        age = rule.age.total_seconds()
        existing = by_age.get(age)
        if existing is None:
            by_age[age] = rule
            continue
        if existing.keep_for.total_seconds() == rule.keep_for.total_seconds():
            continue  # exact duplicate, silent
        chosen = (
            existing if existing.keep_for.total_seconds() >= rule.keep_for.total_seconds()
            else rule
        )
        warnings.append(
            f"retention rules {_retention_literal(existing.age)!r}:{_retention_literal(existing.keep_for)!r} "
            f"and {_retention_literal(rule.age)!r}:{_retention_literal(rule.keep_for)!r} have the same "
            f"age ({age:.0f}s); keeping the longer keep_for {_retention_literal(chosen.keep_for)!r}"
        )
        by_age[age] = chosen

    plan = {rule.keep_for.total_seconds(): rule.age.total_seconds() for rule in by_age.values()}
    return plan, warnings


@dataclass
class Destination:
    """A remote server that accepts backup streams."""
    url: str


@dataclass
class RemoteDatasetConfig:
    """Per-destination remote backup config for a single dataset."""
    destination: str
    frequency: Optional[timedelta] = None  # None = inherit from DatasetConfig
    # Empty = inherit DatasetConfig.retention_rules; non-empty REPLACES the
    # dataset-level set entirely for this destination (override, not merge).
    retention_rules: List[RetentionRule] = field(default_factory=list)


@dataclass
class RemoteServerConfig:
    """Server-side config for accepting incoming backup streams."""
    target_dataset: str
    enabled: bool = True


@dataclass
class DatasetConfig:
    """Configuration for a single dataset to backup."""
    name: str
    recursive: bool = False
    frequency: timedelta = field(default_factory=lambda: Duration("1h"))
    retention_rules: List[RetentionRule] = field(default_factory=list)
    enabled: bool = True
    remote: List[RemoteDatasetConfig] = field(default_factory=list)

    def effective_retention_rules(self, destination: Optional[str] = None) -> List[RetentionRule]:
        """Return the retention rules that apply when backing up to `destination`.

        With no `destination` (the default), returns the dataset-level rules
        used for local pruning.

        With a `destination`, returns that destination's own `retention_rules`
        override if it declared any, otherwise the dataset-level set (inherit,
        not merge -- a destination that declares any rule replaces the whole
        set). An unknown `destination` (not present in `self.remote`) falls
        back to the dataset-level set, same as "no override declared".
        """
        if destination is not None:
            remote_cfg = next((r for r in self.remote if r.destination == destination), None)
            if remote_cfg is not None and remote_cfg.retention_rules:
                return remote_cfg.retention_rules
        return self.retention_rules

    def to_property(self, destination: Optional[str] = None) -> str:
        """Serialize to a base64-encoded JSON string for storage as a ZFS user property.

        With no `destination` (the default), emits the dataset-level config used
        for local pruning -- `sync_config_property` always calls it this way, and
        the output is byte-identical to before this parameter existed (wire
        compatibility with already-deployed servers).

        With a `destination`, emits the *effective* config for backing up to that
        destination: its own `retention_rules` override if it declared any,
        otherwise the dataset-level set (inherit, not merge -- a destination that
        declares any rule replaces the whole set). The output shape is identical
        either way; only the contents of `retention_rules` change, so a receiving
        server needs no changes to understand either form.
        """
        retention_rules = self.effective_retention_rules(destination)

        data = {
            'name': self.name,
            'recursive': self.recursive,
            'frequency': self.frequency.total_seconds(),
            'enabled': self.enabled,
            'retention_rules': [
                {'age': r.age.total_seconds(), 'keep_for': r.keep_for.total_seconds()}
                for r in retention_rules
            ],
            'remote': [
                {
                    'destination': r.destination,
                    'frequency': r.frequency.total_seconds() if r.frequency else None,
                }
                for r in self.remote
            ],
        }
        return base64.b64encode(json.dumps(data, separators=(',', ':')).encode()).decode()

    @classmethod
    def from_property(cls, encoded: str) -> 'DatasetConfig':
        """Deserialize from a base64-encoded JSON ZFS user property value.

        NOTE: this is a *wire decoder*, not the inverse of `to_property`, and
        must not be used as one. `to_property(destination)` intentionally
        emits only the single *effective* `retention_rules` list for that
        destination (dataset-level or the destination's override, never
        both -- see `effective_retention_rules`); the other per-destination
        overrides in `self.remote` are never put on the wire at all. Round-
        tripping a decoded value back through `to_property` therefore always
        yields a `DatasetConfig` with `remote[*].retention_rules == []` for
        every entry, silently losing any per-destination rules the original
        had. Do not reuse this pair as an ORM <-> dataclass round-trip (e.g.
        for the config DB mapper); read the per-destination overrides from
        their own source instead.
        """
        data = json.loads(base64.b64decode(encoded).decode())
        return cls(
            name=data['name'],
            recursive=data.get('recursive', False),
            frequency=Duration(seconds=data['frequency']),
            enabled=data.get('enabled', True),
            retention_rules=[
                RetentionRule(
                    age=Duration(seconds=r['age']),
                    keep_for=Duration(seconds=r['keep_for']),
                )
                for r in data.get('retention_rules', [])
            ],
            remote=[
                RemoteDatasetConfig(
                    destination=r['destination'],
                    frequency=Duration(seconds=r['frequency']) if r.get('frequency') else None,
                )
                for r in data.get('remote', [])
            ],
        )

    @classmethod
    def from_dict(cls, data: dict, key: str = 'datasets[?]') -> 'DatasetConfig':
        """Create DatasetConfig from a YAML-parsed dictionary.

        `key` is the caller's key path for this entry (e.g. ``datasets[2]``),
        used only to name the dataset in errors raised before `name` itself
        has been read off `data`.
        """
        data = _mapping_from_config(data, key, "a mapping")

        name = data.get('name')
        if not name:
            raise ValueError("Dataset 'name' is required")

        recursive = data.get('recursive', False)
        enabled = data.get('enabled', True)

        freq_str = data.get('frequency', '1h')
        frequency = _duration_from_config(freq_str, f"datasets[{name}].frequency")

        retention_rules = []
        retention_dict = data.get('retention')
        if retention_dict is None:
            # Absent 'retention' has a documented meaning at dataset level:
            # default to a single 1d->30d tier. `config.example.yaml` and
            # long-standing behaviour rely on this.
            retention_dict = {'1d': '30d'}
        else:
            # Shape check runs BEFORE the emptiness check below, so a
            # malformed shape (list, string, int, ...) always raises a named
            # ValueError instead of silently falling through to the default
            # the way `if not retention_dict:` used to. Only a `dict` -- empty
            # or not -- reaches the emptiness check.
            retention_dict = _mapping_from_config(
                retention_dict,
                f"datasets[{name}].retention",
                "a mapping of age -> keep_for",
            )
            if not retention_dict:
                # An explicit but empty mapping (`retention: {}`) keeps the
                # same documented default as an absent key. This deliberately
                # differs from the destination-level override, where `{}` has
                # no meaning and is rejected (see remote[*].retention below).
                retention_dict = {'1d': '30d'}
        # NOTE: this is the dataset-level rule set. Strict duplicate age/keep_for
        # validation (`validate_retention_uniqueness`, defined above) is
        # deliberately NOT called here -- load must never hard-fail on a
        # colliding config (see docs/config_db_cli_plan.md item 2b); the write
        # paths that should call it (CLI `set`/`edit`, `import`) don't exist yet.
        for age_str, keep_str in retention_dict.items():
            age = _positive_duration_from_config(age_str, f"datasets[{name}].retention")
            keep_for = _positive_duration_from_config(keep_str, f"datasets[{name}].retention")
            retention_rules.append(RetentionRule(age=age, keep_for=keep_for))
        retention_rules.sort(key=lambda r: r.age)

        remote_data = _list_from_config(
            data.get('remote', []), f"datasets[{name}].remote", "a list of mappings"
        )
        remote = []
        for idx, r in enumerate(remote_data):
            r = _mapping_from_config(r, f"datasets[{name}].remote[{idx}]", "a mapping")
            dest = r.get('destination')
            if not dest:
                raise ValueError(
                    f"datasets[{name}].remote[{idx}] requires a 'destination'"
                )
            freq = (
                _duration_from_config(r['frequency'], f"datasets[{name}].remote[{dest}].frequency")
                if r.get('frequency') is not None else None
            )

            remote_retention_rules: List[RetentionRule] = []
            retention_override = r.get('retention')
            if retention_override is not None:
                retention_override = _mapping_from_config(
                    retention_override,
                    f"datasets[{name}].remote[{dest}].retention",
                    "a mapping of age -> keep_for",
                )
                if not retention_override:
                    raise ValueError(
                        f"datasets[{name}].remote[{dest}].retention is empty; "
                        "omit 'retention' to inherit the dataset-level rules, "
                        "or declare at least one rule"
                    )
                for age_str, keep_str in retention_override.items():
                    age = _positive_duration_from_config(
                        age_str, f"datasets[{name}].remote[{dest}].retention"
                    )
                    keep_for = _positive_duration_from_config(
                        keep_str, f"datasets[{name}].remote[{dest}].retention"
                    )
                    remote_retention_rules.append(RetentionRule(age=age, keep_for=keep_for))
                remote_retention_rules.sort(key=lambda rr: rr.age)

            remote.append(RemoteDatasetConfig(
                destination=dest, frequency=freq, retention_rules=remote_retention_rules
            ))

        return cls(
            name=name,
            recursive=recursive,
            frequency=frequency,
            retention_rules=retention_rules,
            enabled=enabled,
            remote=remote,
        )


@dataclass
class BackupConfig:
    """Main backup daemon configuration."""
    datasets: List[DatasetConfig]
    snapshot_prefix: str = "autosnap"
    check_interval: timedelta = field(default_factory=lambda: Duration("5m"))
    prune_interval: timedelta = field(default_factory=lambda: Duration("1h"))
    api_host: str = "127.0.0.1"
    api_port: int = 8080
    dry_run: bool = False
    destinations: Dict[str, Destination] = field(default_factory=dict)
    remote_backup: Optional[RemoteServerConfig] = None
    client_id_file: Path = field(
        default_factory=lambda: Path.home() / '.config' / 'zfsbackup' / 'client_id'
    )

    @classmethod
    def from_file(cls, config_path: Path) -> 'BackupConfig':
        """Load configuration from YAML file."""
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)

        if not data:
            raise ValueError("Config file is empty")
        data = _mapping_from_config(data, "config", "a top-level mapping")

        datasets_data = _list_from_config(
            data.get('datasets', []), "datasets", "a list of dataset mappings"
        )
        if not datasets_data:
            raise ValueError("No datasets configured")
        datasets = [
            DatasetConfig.from_dict(ds, key=f"datasets[{idx}]")
            for idx, ds in enumerate(datasets_data)
        ]

        snapshot_prefix = data.get('snapshot_prefix', 'autosnap')
        dry_run = data.get('dry_run', False)

        check_str = data.get('check_interval', '5m')
        check_interval = _duration_from_config(check_str, "check_interval")

        prune_str = data.get('prune_interval', check_str)
        prune_interval = _duration_from_config(prune_str, "prune_interval")

        api_host = data.get('api_host', '127.0.0.1')
        api_port = int(data.get('api_port', 8080))

        destinations_data = _mapping_from_config(
            data.get('destinations', {}), "destinations", "a mapping of name -> destination"
        )
        destinations: Dict[str, Destination] = {}
        for dest_name, dest_data in destinations_data.items():
            dest_data = _mapping_from_config(
                dest_data, f"destinations[{dest_name}]", "a mapping"
            )
            url = dest_data.get('url')
            if not url:
                raise ValueError(f"Destination '{dest_name}' requires 'url'")
            destinations[dest_name] = Destination(url=url)

        for ds in datasets:
            for r in ds.remote:
                # Any reference to an undeclared destination fails at load,
                # not just one carrying a 'retention' override: a destination
                # absent from 'destinations:' can never be backed up to
                # (RemoteBackupManager.backup_dataset looks it up the same
                # way and errors every cycle forever), so there is no
                # legitimate reason to let it load clean.
                if r.destination not in destinations:
                    raise ValueError(
                        f"datasets[{ds.name}].remote[{r.destination}] references "
                        f"destination '{r.destination}', which is not declared "
                        "in 'destinations'"
                    )

        remote_backup: Optional[RemoteServerConfig] = None
        rb_data = data.get('remote_backup')
        if rb_data:
            rb_data = _mapping_from_config(rb_data, "remote_backup", "a mapping")
            target = rb_data.get('target_dataset')
            if not target:
                raise ValueError("remote_backup requires 'target_dataset'")
            remote_backup = RemoteServerConfig(
                target_dataset=target,
                enabled=rb_data.get('enabled', True),
            )

        cid = data.get('client_id_file')
        client_id_file = Path(cid) if cid else Path.home() / '.config' / 'zfsbackup' / 'client_id'

        return cls(
            datasets=datasets,
            snapshot_prefix=snapshot_prefix,
            check_interval=check_interval,
            prune_interval=prune_interval,
            api_host=api_host,
            api_port=api_port,
            dry_run=dry_run,
            destinations=destinations,
            remote_backup=remote_backup,
            client_id_file=client_id_file,
        )

    @property
    def enabled_datasets(self) -> List[DatasetConfig]:
        return [ds for ds in self.datasets if ds.enabled]

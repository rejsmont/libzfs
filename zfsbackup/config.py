"""Configuration file parser for ZFS backup daemon."""

import base64
import json
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional
import yaml


def parse_time_duration(duration_str: str) -> timedelta:
    """Parse time duration string like '1h', '2d', '1w', '1m' (month), '1y'."""
    if not duration_str:
        raise ValueError("Duration string cannot be empty")

    if duration_str[0].lower() == 'm' and len(duration_str) > 1:
        if duration_str[1].lower() == 'i':
            unit = 'm'
        else:
            unit = 'M'
    else:
        unit = duration_str[-1]

    try:
        value = int(duration_str[:-1])
    except ValueError:
        raise ValueError(f"Invalid duration format: {duration_str}")

    if unit.lower() == 's':
        return timedelta(seconds=value)
    if unit == 'm':
        return timedelta(minutes=value)
    if unit.lower() == 'h':
        return timedelta(hours=value)
    elif unit.lower() == 'd':
        return timedelta(days=value)
    elif unit.lower() == 'w':
        return timedelta(weeks=value)
    elif unit == 'M':
        return timedelta(days=value * 30)
    elif unit.lower() == 'y':
        return timedelta(days=value * 365)
    else:
        raise ValueError(f"Unknown duration unit: {unit}. Use s/m/h/d/w/M/y")


@dataclass
class RetentionRule:
    """Retention rule defining one tier of a tiered retention policy."""
    age: timedelta
    keep_for: timedelta

    def __repr__(self):
        return f"RetentionRule(age={self.age}, keep_for={self.keep_for})"


@dataclass
class Destination:
    """A remote server that accepts backup streams."""
    url: str


@dataclass
class RemoteDatasetConfig:
    """Per-destination remote backup config for a single dataset."""
    destination: str
    frequency: Optional[timedelta] = None  # None = inherit from DatasetConfig


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
    frequency: timedelta = field(default_factory=lambda: timedelta(hours=1))
    retention_rules: List[RetentionRule] = field(default_factory=list)
    enabled: bool = True
    remote: List[RemoteDatasetConfig] = field(default_factory=list)

    def to_property(self) -> str:
        """Serialize to a base64-encoded JSON string for storage as a ZFS user property."""
        data = {
            'name': self.name,
            'recursive': self.recursive,
            'frequency': self.frequency.total_seconds(),
            'enabled': self.enabled,
            'retention_rules': [
                {'age': r.age.total_seconds(), 'keep_for': r.keep_for.total_seconds()}
                for r in self.retention_rules
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
        """Deserialize from a base64-encoded JSON ZFS user property value."""
        data = json.loads(base64.b64decode(encoded).decode())
        return cls(
            name=data['name'],
            recursive=data.get('recursive', False),
            frequency=timedelta(seconds=data['frequency']),
            enabled=data.get('enabled', True),
            retention_rules=[
                RetentionRule(
                    age=timedelta(seconds=r['age']),
                    keep_for=timedelta(seconds=r['keep_for']),
                )
                for r in data.get('retention_rules', [])
            ],
            remote=[
                RemoteDatasetConfig(
                    destination=r['destination'],
                    frequency=timedelta(seconds=r['frequency']) if r.get('frequency') else None,
                )
                for r in data.get('remote', [])
            ],
        )

    @classmethod
    def from_dict(cls, data: dict) -> 'DatasetConfig':
        """Create DatasetConfig from a YAML-parsed dictionary."""
        name = data.get('name')
        if not name:
            raise ValueError("Dataset 'name' is required")

        recursive = data.get('recursive', False)
        enabled = data.get('enabled', True)

        freq_str = data.get('frequency', '1h')
        frequency = parse_time_duration(freq_str)

        retention_rules = []
        retention_dict = data.get('retention', {})
        if not retention_dict:
            retention_dict = {'1d': '30d'}
        for age_str, keep_str in retention_dict.items():
            age = parse_time_duration(age_str)
            keep_for = parse_time_duration(keep_str)
            retention_rules.append(RetentionRule(age=age, keep_for=keep_for))
        retention_rules.sort(key=lambda r: r.age)

        remote = []
        for r in data.get('remote', []):
            dest = r.get('destination')
            if not dest:
                raise ValueError("Each remote entry requires a 'destination'")
            freq = parse_time_duration(r['frequency']) if r.get('frequency') else None
            remote.append(RemoteDatasetConfig(destination=dest, frequency=freq))

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
    check_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    prune_interval: timedelta = field(default_factory=lambda: timedelta(hours=1))
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

        datasets_data = data.get('datasets', [])
        if not datasets_data:
            raise ValueError("No datasets configured")
        datasets = [DatasetConfig.from_dict(ds) for ds in datasets_data]

        snapshot_prefix = data.get('snapshot_prefix', 'autosnap')
        dry_run = data.get('dry_run', False)

        check_str = data.get('check_interval', '5m')
        check_interval = parse_time_duration(check_str)

        prune_str = data.get('prune_interval', check_str)
        prune_interval = parse_time_duration(prune_str)

        api_host = data.get('api_host', '127.0.0.1')
        api_port = int(data.get('api_port', 8080))

        destinations: Dict[str, Destination] = {}
        for dest_name, dest_data in data.get('destinations', {}).items():
            url = dest_data.get('url')
            if not url:
                raise ValueError(f"Destination '{dest_name}' requires 'url'")
            destinations[dest_name] = Destination(url=url)

        remote_backup: Optional[RemoteServerConfig] = None
        rb_data = data.get('remote_backup', {})
        if rb_data:
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

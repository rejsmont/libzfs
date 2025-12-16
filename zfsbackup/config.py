"""Configuration file parser for ZFS backup daemon."""

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional
import yaml


def parse_time_duration(duration_str: str) -> timedelta:
    """Parse time duration string like '1h', '2d', '1w', '1m' (month), '1y'."""
    if not duration_str:
        raise ValueError("Duration string cannot be empty")
    
    unit = duration_str[-1].lower()
    try:
        value = int(duration_str[:-1])
    except ValueError:
        raise ValueError(f"Invalid duration format: {duration_str}")
    
    if unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    elif unit == 'w':
        return timedelta(weeks=value)
    elif unit == 'm':
        return timedelta(days=value * 30)  # Approximate month as 30 days
    elif unit == 'y':
        return timedelta(days=value * 365)  # Approximate year
    else:
        raise ValueError(f"Unknown duration unit: {unit}. Use h/d/w/m/y")


@dataclass
class RetentionRule:
    """Retention rule for snapshots of a specific age."""
    age: timedelta  # How old snapshots must be to apply this rule
    keep_for: timedelta  # How long to keep snapshots of this age
    
    def __repr__(self):
        return f"RetentionRule(age={self.age}, keep_for={self.keep_for})"


@dataclass
class DatasetConfig:
    """Configuration for a single dataset to backup."""
    name: str
    recursive: bool = False
    frequency: timedelta = field(default_factory=lambda: timedelta(hours=1))
    retention_rules: List[RetentionRule] = field(default_factory=list)
    enabled: bool = True
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DatasetConfig':
        """Create DatasetConfig from dictionary."""
        name = data.get('name')
        if not name:
            raise ValueError("Dataset 'name' is required")
        
        recursive = data.get('recursive', False)
        enabled = data.get('enabled', True)
        
        # Parse frequency
        freq_str = data.get('frequency', '1h')
        frequency = parse_time_duration(freq_str)
        
        # Parse retention rules
        retention_rules = []
        retention_dict = data.get('retention', {})
        
        # Default retention: keep everything for 30 days
        if not retention_dict:
            retention_dict = {'1d': '30d'}
        
        for age_str, keep_str in retention_dict.items():
            age = parse_time_duration(age_str)
            keep_for = parse_time_duration(keep_str)
            retention_rules.append(RetentionRule(age=age, keep_for=keep_for))
        
        # Sort retention rules by age
        retention_rules.sort(key=lambda r: r.age)
        
        return cls(
            name=name,
            recursive=recursive,
            frequency=frequency,
            retention_rules=retention_rules,
            enabled=enabled
        )


@dataclass
class BackupConfig:
    """Main backup daemon configuration."""
    datasets: List[DatasetConfig]
    snapshot_prefix: str = "autosnap"
    check_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    dry_run: bool = False
    
    @classmethod
    def from_file(cls, config_path: Path) -> 'BackupConfig':
        """Load configuration from YAML file."""
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        if not data:
            raise ValueError("Config file is empty")
        
        # Parse datasets
        datasets_data = data.get('datasets', [])
        if not datasets_data:
            raise ValueError("No datasets configured")
        
        datasets = [DatasetConfig.from_dict(ds) for ds in datasets_data]
        
        # Parse global settings
        snapshot_prefix = data.get('snapshot_prefix', 'autosnap')
        dry_run = data.get('dry_run', False)
        
        check_str = data.get('check_interval', '5m')
        check_interval = parse_time_duration(check_str)
        
        return cls(
            datasets=datasets,
            snapshot_prefix=snapshot_prefix,
            check_interval=check_interval,
            dry_run=dry_run
        )
    
    def get_enabled_datasets(self) -> List[DatasetConfig]:
        """Return only enabled datasets."""
        return [ds for ds in self.datasets if ds.enabled]

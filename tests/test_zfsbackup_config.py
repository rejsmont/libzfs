"""Unit tests for zfsbackup.config module."""

import pytest
from datetime import timedelta
from pathlib import Path

from zfsbackup.config import (
    parse_time_duration,
    RetentionRule,
    DatasetConfig,
    RemoteDatasetConfig,
    RemoteServerConfig,
    Destination,
    BackupConfig,
)


class TestParseTimeDuration:
    def test_minutes(self):
        assert parse_time_duration("5m") == timedelta(minutes=5)

    def test_hours(self):
        assert parse_time_duration("1h") == timedelta(hours=1)

    def test_hours_uppercase(self):
        assert parse_time_duration("2H") == timedelta(hours=2)

    def test_days(self):
        assert parse_time_duration("7d") == timedelta(days=7)

    def test_weeks(self):
        assert parse_time_duration("2w") == timedelta(weeks=2)

    def test_months(self):
        assert parse_time_duration("1M") == timedelta(days=30)

    def test_months_3(self):
        assert parse_time_duration("3M") == timedelta(days=90)

    def test_years(self):
        assert parse_time_duration("1y") == timedelta(days=365)

    def test_years_10(self):
        assert parse_time_duration("10y") == timedelta(days=3650)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            parse_time_duration("")

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_time_duration("xh")

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError, match="Unknown duration unit"):
            parse_time_duration("5z")


class TestParseDurationMinutePrefix:
    """Cover lines 17-21: the 'm'/'M'-prefix branch in parse_time_duration.

    Inputs starting with 'm'/'M' enter the special-casing block that distinguishes
    'mi' (minute) from plain 'M' (month), but value extraction via int(duration_str[:-1])
    cannot succeed for prefix-unit formats — the branch is dead as currently written.
    These tests document the behaviour and keep the lines covered.
    """

    def test_m_prefix_raises_invalid_format(self):
        # "M5": hits line 17 (True), line 20-21 (else: unit='M'), then int("M") fails
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_time_duration("M5")

    def test_mi_prefix_raises_invalid_format(self):
        # "mi5": hits line 17 (True), lines 18-19 (unit='m'), then int("mi") fails
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_time_duration("mi5")


class TestRetentionRule:
    def test_repr(self):
        rule = RetentionRule(age=timedelta(days=1), keep_for=timedelta(days=7))
        r = repr(rule)
        assert "RetentionRule" in r
        assert "age=" in r
        assert "keep_for=" in r


class TestDatasetConfig:
    def test_from_dict_basic(self):
        cfg = DatasetConfig.from_dict({'name': 'pool/data'})
        assert cfg.name == 'pool/data'
        assert cfg.frequency == timedelta(hours=1)
        assert cfg.recursive is False
        assert cfg.enabled is True

    def test_from_dict_all_fields(self):
        data = {
            'name': 'pool/data',
            'frequency': '15m',
            'recursive': True,
            'enabled': False,
            'retention': {'1h': '1d', '1d': '1w'},
        }
        cfg = DatasetConfig.from_dict(data)
        assert cfg.name == 'pool/data'
        assert cfg.frequency == timedelta(minutes=15)
        assert cfg.recursive is True
        assert cfg.enabled is False
        assert len(cfg.retention_rules) == 2

    def test_from_dict_missing_name_raises(self):
        with pytest.raises(ValueError, match="'name' is required"):
            DatasetConfig.from_dict({'frequency': '1h'})

    def test_from_dict_default_retention(self):
        cfg = DatasetConfig.from_dict({'name': 'pool/data'})
        assert len(cfg.retention_rules) == 1
        assert cfg.retention_rules[0].age == timedelta(days=1)
        assert cfg.retention_rules[0].keep_for == timedelta(days=30)

    def test_from_dict_retention_sorted(self):
        data = {
            'name': 'pool/data',
            'retention': {'1w': '1M', '1d': '1w', '1h': '1d'},
        }
        cfg = DatasetConfig.from_dict(data)
        ages = [r.age for r in cfg.retention_rules]
        assert ages == sorted(ages)

    def test_from_dict_remote(self):
        data = {
            'name': 'pool/data',
            'remote': [
                {'destination': 'offsite'},
                {'destination': 'dc2', 'frequency': '4h'},
            ],
        }
        cfg = DatasetConfig.from_dict(data)
        assert len(cfg.remote) == 2
        assert cfg.remote[0].destination == 'offsite'
        assert cfg.remote[0].frequency is None
        assert cfg.remote[1].destination == 'dc2'
        assert cfg.remote[1].frequency == timedelta(hours=4)

    def test_from_dict_remote_missing_destination_raises(self):
        data = {'name': 'pool/data', 'remote': [{'frequency': '1h'}]}
        with pytest.raises(ValueError, match="'destination'"):
            DatasetConfig.from_dict(data)

    def test_to_property_roundtrip_basic(self):
        cfg = DatasetConfig(
            name='pool/data',
            frequency=timedelta(hours=1),
            recursive=True,
            enabled=True,
            retention_rules=[RetentionRule(timedelta(days=1), timedelta(days=30))],
        )
        encoded = cfg.to_property()
        assert isinstance(encoded, str)
        restored = DatasetConfig.from_property(encoded)
        assert restored.name == cfg.name
        assert restored.frequency == cfg.frequency
        assert restored.recursive == cfg.recursive
        assert restored.enabled == cfg.enabled
        assert len(restored.retention_rules) == 1
        assert restored.retention_rules[0].age == timedelta(days=1)
        assert restored.retention_rules[0].keep_for == timedelta(days=30)

    def test_to_property_roundtrip_with_remote(self):
        cfg = DatasetConfig(
            name='pool/data',
            frequency=timedelta(hours=1),
            remote=[RemoteDatasetConfig(destination='offsite', frequency=timedelta(hours=4))],
        )
        restored = DatasetConfig.from_property(cfg.to_property())
        assert len(restored.remote) == 1
        assert restored.remote[0].destination == 'offsite'
        assert restored.remote[0].frequency == timedelta(hours=4)

    def test_to_property_roundtrip_remote_no_frequency(self):
        cfg = DatasetConfig(
            name='pool/data',
            frequency=timedelta(hours=1),
            remote=[RemoteDatasetConfig(destination='offsite', frequency=None)],
        )
        restored = DatasetConfig.from_property(cfg.to_property())
        assert restored.remote[0].frequency is None

    def test_to_property_is_string(self):
        cfg = DatasetConfig(name='pool/data', frequency=timedelta(hours=1))
        encoded = cfg.to_property()
        assert isinstance(encoded, str)
        assert len(encoded) > 0


class TestBackupConfig:
    def test_from_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            BackupConfig.from_file(tmp_path / 'nonexistent.yaml')

    def test_from_file_empty_raises(self, tmp_path):
        f = tmp_path / 'empty.yaml'
        f.write_text('')
        with pytest.raises(ValueError, match="empty"):
            BackupConfig.from_file(f)

    def test_from_file_no_datasets_raises(self, tmp_path):
        f = tmp_path / 'nodatasets.yaml'
        f.write_text('snapshot_prefix: test\n')
        with pytest.raises(ValueError, match="No datasets"):
            BackupConfig.from_file(f)

    def test_from_file_minimal(self, tmp_path):
        f = tmp_path / 'minimal.yaml'
        f.write_text('datasets:\n  - name: pool/data\n')
        cfg = BackupConfig.from_file(f)
        assert len(cfg.datasets) == 1
        assert cfg.datasets[0].name == 'pool/data'
        assert cfg.snapshot_prefix == 'autosnap'
        assert cfg.check_interval == timedelta(minutes=5)
        assert cfg.api_host == '127.0.0.1'
        assert cfg.api_port == 8080
        assert cfg.dry_run is False

    def test_from_file_all_options(self, tmp_path):
        f = tmp_path / 'full.yaml'
        f.write_text(
            "snapshot_prefix: mysnap\n"
            "check_interval: 10m\n"
            "prune_interval: 2h\n"
            "api_host: 0.0.0.0\n"
            "api_port: 9090\n"
            "dry_run: true\n"
            "datasets:\n"
            "  - name: pool/data\n"
            "destinations:\n"
            "  offsite:\n"
            "    url: http://backup.example.com\n"
            "remote_backup:\n"
            "  target_dataset: pool/backups\n"
            "  enabled: true\n"
        )
        cfg = BackupConfig.from_file(f)
        assert cfg.snapshot_prefix == 'mysnap'
        assert cfg.check_interval == timedelta(minutes=10)
        assert cfg.prune_interval == timedelta(hours=2)
        assert cfg.api_host == '0.0.0.0'
        assert cfg.api_port == 9090
        assert cfg.dry_run is True
        assert 'offsite' in cfg.destinations
        assert cfg.destinations['offsite'].url == 'http://backup.example.com'
        assert cfg.remote_backup is not None
        assert cfg.remote_backup.target_dataset == 'pool/backups'
        assert cfg.remote_backup.enabled is True

    def test_from_file_destination_missing_url_raises(self, tmp_path):
        f = tmp_path / 'baddest.yaml'
        f.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "destinations:\n"
            "  offsite:\n"
            "    host: backup.example.com\n"
        )
        with pytest.raises(ValueError, match="requires 'url'"):
            BackupConfig.from_file(f)

    def test_from_file_remote_backup_missing_target_raises(self, tmp_path):
        f = tmp_path / 'badrb.yaml'
        f.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "remote_backup:\n"
            "  enabled: true\n"
        )
        with pytest.raises(ValueError, match="target_dataset"):
            BackupConfig.from_file(f)

    def test_from_file_custom_client_id_file(self, tmp_path):
        id_file = tmp_path / 'my_id'
        f = tmp_path / 'cfg.yaml'
        f.write_text(f"datasets:\n  - name: pool/data\nclient_id_file: {id_file}\n")
        cfg = BackupConfig.from_file(f)
        assert cfg.client_id_file == id_file

    def test_enabled_datasets(self):
        cfg = BackupConfig(datasets=[
            DatasetConfig(name='pool/a', enabled=True),
            DatasetConfig(name='pool/b', enabled=False),
            DatasetConfig(name='pool/c', enabled=True),
        ])
        enabled = cfg.enabled_datasets
        assert len(enabled) == 2
        assert all(d.enabled for d in enabled)

    def test_prune_interval_inherits_check_interval(self, tmp_path):
        f = tmp_path / 'cfg.yaml'
        f.write_text("datasets:\n  - name: pool/data\ncheck_interval: 30m\n")
        cfg = BackupConfig.from_file(f)
        assert cfg.prune_interval == cfg.check_interval

    def test_from_file_remote_backup_disabled(self, tmp_path):
        f = tmp_path / 'cfg.yaml'
        f.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "remote_backup:\n"
            "  target_dataset: pool/backups\n"
            "  enabled: false\n"
        )
        cfg = BackupConfig.from_file(f)
        assert cfg.remote_backup is not None
        assert cfg.remote_backup.enabled is False

"""Unit tests for zfsbackup.config module."""

import base64
import json
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
    Duration,
    validate_retention_uniqueness,
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


class TestValidateRetentionUniqueness:
    """Item 2b: the strict validator for future write paths (CLI `set`/`edit`,
    `import`). Deliberately NOT wired into `from_dict`/`from_file` -- a
    colliding config must still *load*, see `TestValidateRetentionUniquenessNotCalledFromLoad`.
    """

    def test_same_keep_for_raises_naming_key_and_both_literals(self):
        rules = [
            RetentionRule(age=Duration('1h'), keep_for=Duration('7d')),
            RetentionRule(age=Duration('1d'), keep_for=Duration('1w')),
        ]
        with pytest.raises(ValueError) as exc_info:
            validate_retention_uniqueness(rules, 'datasets[pool/data].retention')
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert '7d' in msg
        assert '1w' in msg
        assert 'keep_for' in msg

    def test_same_age_raises_naming_key_and_both_literals(self):
        rules = [
            RetentionRule(age=Duration('1d'), keep_for=Duration('1w')),
            RetentionRule(age=Duration('24h'), keep_for=Duration('1M')),
        ]
        with pytest.raises(ValueError) as exc_info:
            validate_retention_uniqueness(rules, 'datasets[pool/data].retention')
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert '1d' in msg
        assert '24h' in msg
        assert 'age' in msg

    def test_exact_duplicates_are_silent(self):
        rules = [
            RetentionRule(age=Duration('1d'), keep_for=Duration('30d')),
            RetentionRule(age=Duration('1d'), keep_for=Duration('30d')),
        ]
        validate_retention_uniqueness(rules, 'datasets[pool/data].retention')  # must not raise

    def test_non_colliding_rules_pass(self):
        rules = [
            RetentionRule(age=Duration('1h'), keep_for=Duration('1d')),
            RetentionRule(age=Duration('1d'), keep_for=Duration('1w')),
        ]
        validate_retention_uniqueness(rules, 'datasets[pool/data].retention')  # must not raise


class TestValidateRetentionUniquenessNotCalledFromLoad:
    """The read path (`from_dict`/`from_file`) must stay permissive -- a
    colliding config loads successfully, per item 2b's "load never
    hard-fails" decision. Wiring `validate_retention_uniqueness` into load
    would break upgrades for already-deployed configs."""

    def test_from_dict_loads_colliding_retention(self):
        data = {
            'name': 'pool/data',
            'retention': {'1h': '7d', '1d': '1w'},  # same keep_for (604800s)
        }
        cfg = DatasetConfig.from_dict(data)  # must not raise
        assert len(cfg.retention_rules) == 2

    def test_from_file_loads_colliding_retention(self, tmp_path):
        f = tmp_path / 'colliding.yaml'
        f.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "    retention:\n"
            "      1h: 7d\n"
            "      1d: 1w\n"
        )
        config = BackupConfig.from_file(f)  # must not raise
        assert len(config.datasets[0].retention_rules) == 2


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


class TestDatasetConfigPerDestinationRetention:
    """Item 3b: `to_property(destination)` emits the *effective* rules for
    that destination -- override, not merge."""

    def _cfg(self, remote_retention=None):
        return DatasetConfig(
            name='pool/data',
            frequency=timedelta(hours=1),
            retention_rules=[RetentionRule(timedelta(hours=1), timedelta(days=1))],
            remote=[RemoteDatasetConfig(
                destination='offsite',
                retention_rules=remote_retention or [],
            )],
        )

    def _decode(self, encoded: str) -> dict:
        return json.loads(base64.b64decode(encoded).decode())

    def test_override_replaces_not_merges(self):
        # Dataset-level: 1h->1d. Destination override: 1d->5y. A merge would
        # emit BOTH rules; override emits ONLY the destination's own rule.
        cfg = self._cfg(remote_retention=[RetentionRule(timedelta(days=1), timedelta(days=1825))])
        data = self._decode(cfg.to_property('offsite'))
        ages = [r['age'] for r in data['retention_rules']]
        assert ages == [86400.0]  # only the override -- not [3600.0, 86400.0]

    def test_no_destination_retention_inherits_dataset_level(self):
        cfg = self._cfg(remote_retention=[])  # destination declares no rules
        data = self._decode(cfg.to_property('offsite'))
        assert data['retention_rules'] == [{'age': 3600.0, 'keep_for': 86400.0}]
        # Identical to the dataset-level (no-argument) output.
        assert cfg.to_property('offsite') == cfg.to_property()

    def test_no_argument_returns_dataset_level_rules(self):
        cfg = self._cfg(remote_retention=[RetentionRule(timedelta(days=1), timedelta(days=1825))])
        data = self._decode(cfg.to_property())
        assert data['retention_rules'] == [{'age': 3600.0, 'keep_for': 86400.0}]

    def test_wire_compatibility_no_per_destination_rules_byte_identical(self):
        """Required: for a config with no per-destination rules, `to_property()`
        must be byte-identical to a pre-item-3b build's output, because
        already-deployed servers parse it verbatim. Value independently
        verified before pinning (see docs/config_db_cli_plan.md item 3b)."""
        cfg = DatasetConfig.from_dict({
            'name': 'tank/d',
            'frequency': '1h',
            'retention': {'1d': '30d'},
            'remote': [{'destination': 'offsite'}],
        })
        encoded = cfg.to_property()
        expected = (
            'eyJuYW1lIjoidGFuay9kIiwicmVjdXJzaXZlIjpmYWxzZSwiZnJlcXVlbmN5IjozNjAwLjAsImVuYWJsZWQiOnRydWUs'
            'InJldGVudGlvbl9ydWxlcyI6W3siYWdlIjo4NjQwMC4wLCJrZWVwX2ZvciI6MjU5MjAwMC4wfV0sInJlbW90ZSI6W3si'
            'ZGVzdGluYXRpb24iOiJvZmZzaXRlIiwiZnJlcXVlbmN5IjpudWxsfV19'
        )
        assert encoded == expected

    def test_from_property_round_trip_with_destination_argument(self):
        cfg = self._cfg(remote_retention=[RetentionRule(timedelta(days=1), timedelta(days=1825))])
        encoded = cfg.to_property('offsite')
        restored = DatasetConfig.from_property(encoded)
        assert restored.name == 'pool/data'
        assert len(restored.retention_rules) == 1
        assert restored.retention_rules[0].age == timedelta(days=1)
        assert restored.retention_rules[0].keep_for == timedelta(days=1825)

    def test_unknown_destination_falls_back_to_dataset_level(self):
        cfg = self._cfg(remote_retention=[RetentionRule(timedelta(days=1), timedelta(days=1825))])
        data = self._decode(cfg.to_property('nonexistent'))
        assert data['retention_rules'] == [{'age': 3600.0, 'keep_for': 86400.0}]

    def test_from_property_is_not_an_orm_round_trip_inverse(self):
        """`from_property` is a *wire decoder*, not the inverse of
        `to_property`, and must not be reused as one (see item 4's config-DB
        mapper). `to_property(destination)` intentionally emits only the
        single *effective* `retention_rules` list for that destination --
        the per-destination overrides living in `self.remote` are never put
        on the wire at all. Round-tripping a decoded value back through
        `from_property` therefore always yields `remote[*].retention_rules
        == []` for every entry, silently dropping the original's
        per-destination rules. Pin the lossiness explicitly so a future
        "just reuse to_property/from_property for the DB mapper" shortcut
        fails loudly instead of silently losing data."""
        original = self._cfg(
            remote_retention=[RetentionRule(timedelta(days=1), timedelta(days=1825))]
        )
        assert original.remote[0].retention_rules != []  # sanity: original HAS rules

        # Even the dataset-level (no-destination) encoding carries `remote`
        # entries (destination + frequency), but never their retention_rules.
        restored = DatasetConfig.from_property(original.to_property())
        assert len(restored.remote) == 1
        assert restored.remote[0].destination == 'offsite'
        assert restored.remote[0].retention_rules == []

        # Also true for the destination-scoped encoding: it substitutes the
        # override into the top-level (dataset-level) retention_rules slot,
        # but `remote[*].retention_rules` is still lost.
        restored_for_dest = DatasetConfig.from_property(original.to_property('offsite'))
        assert len(restored_for_dest.remote) == 1
        assert restored_for_dest.remote[0].retention_rules == []


class TestDatasetConfigRetentionShapeAndBounds:
    """Dataset-level `retention:` shape/emptiness/bounds checks.

    The shape check (`_mapping_from_config`) runs BEFORE the emptiness
    check, so a malformed shape (list, string, int, ...) always raises a
    named ValueError naming the `datasets[name].retention` key path,
    rather than silently falling through to the documented default the
    way `if not retention_dict:` used to.
    """

    def test_retention_list_raises_named_error(self):
        data = {'name': 'pool/data', 'retention': []}
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert 'list' in msg

    def test_retention_empty_string_raises_named_error(self):
        data = {'name': 'pool/data', 'retention': ''}
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert 'str' in msg

    def test_retention_zero_raises_named_error(self):
        data = {'name': 'pool/data', 'retention': 0}
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert 'int' in msg

    def test_retention_empty_mapping_defaults_to_1d_30d(self):
        """Deliberate asymmetry with the destination-level override, where
        `{}` has no meaning and is rejected (see
        TestDatasetConfigRemoteRetentionRejections.test_empty_retention_dict_on_destination_raises).
        `config.example.yaml` and long-standing behaviour rely on the
        dataset-level default; a future "consistency" cleanup that makes
        this raise instead would break real configs."""
        cfg = DatasetConfig.from_dict({'name': 'pool/data', 'retention': {}})
        assert len(cfg.retention_rules) == 1
        assert cfg.retention_rules[0].age == timedelta(days=1)
        assert cfg.retention_rules[0].keep_for == timedelta(days=30)

    def test_retention_absent_defaults_to_1d_30d(self):
        cfg = DatasetConfig.from_dict({'name': 'pool/data'})
        assert len(cfg.retention_rules) == 1
        assert cfg.retention_rules[0].age == timedelta(days=1)
        assert cfg.retention_rules[0].keep_for == timedelta(days=30)

    def test_retention_none_defaults_to_1d_30d(self):
        cfg = DatasetConfig.from_dict({'name': 'pool/data', 'retention': None})
        assert len(cfg.retention_rules) == 1
        assert cfg.retention_rules[0].age == timedelta(days=1)
        assert cfg.retention_rules[0].keep_for == timedelta(days=30)

    def test_retention_zero_age_raises(self):
        data = {'name': 'pool/data', 'retention': {'0s': '1d'}}
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert 'positive' in msg

    def test_retention_zero_keep_for_raises(self):
        data = {'name': 'pool/data', 'retention': {'1h': '0s'}}
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert 'positive' in msg

    def test_retention_negative_age_raises(self):
        data = {'name': 'pool/data', 'retention': {'-1h': '1d'}}
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert 'positive' in msg

    def test_retention_negative_keep_for_raises(self):
        data = {'name': 'pool/data', 'retention': {'1h': '-1d'}}
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].retention' in msg
        assert 'positive' in msg


class TestDatasetConfigDestinationRetentionBounds:
    """Non-positive durations rejected on the destination-level override too."""

    def test_destination_retention_zero_age_raises(self):
        data = {
            'name': 'pool/data',
            'remote': [{'destination': 'offsite', 'retention': {'0s': '1d'}}],
        }
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].remote[offsite].retention' in msg
        assert 'positive' in msg

    def test_destination_retention_zero_keep_for_raises(self):
        data = {
            'name': 'pool/data',
            'remote': [{'destination': 'offsite', 'retention': {'1h': '0s'}}],
        }
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].remote[offsite].retention' in msg
        assert 'positive' in msg

    def test_destination_retention_negative_age_raises(self):
        data = {
            'name': 'pool/data',
            'remote': [{'destination': 'offsite', 'retention': {'-1h': '1d'}}],
        }
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'positive' in str(exc_info.value)

    def test_destination_retention_negative_keep_for_raises(self):
        data = {
            'name': 'pool/data',
            'remote': [{'destination': 'offsite', 'retention': {'1h': '-1d'}}],
        }
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'positive' in str(exc_info.value)


class TestDatasetConfigRemoteRetentionRejections:
    """Item 3b edge cases in `DatasetConfig.from_dict`."""

    def test_empty_retention_dict_on_destination_raises(self):
        data = {
            'name': 'pool/data',
            'remote': [{'destination': 'offsite', 'retention': {}}],
        }
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].remote[offsite].retention' in msg
        # Message must state both alternatives.
        assert 'omit' in msg
        assert 'declare' in msg

    def test_retention_as_list_under_remote_entry_raises(self):
        data = {
            'name': 'pool/data',
            'remote': [{'destination': 'offsite', 'retention': ['1d', '30d']}],
        }
        with pytest.raises(ValueError) as exc_info:
            DatasetConfig.from_dict(data)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].remote[offsite].retention' in msg
        assert 'list' in msg

    def test_destination_retention_sorted_by_age(self):
        data = {
            'name': 'pool/data',
            'remote': [{
                'destination': 'offsite',
                'retention': {'1w': '1M', '1d': '1w', '1h': '1d'},
            }],
        }
        cfg = DatasetConfig.from_dict(data)
        ages = [r.age for r in cfg.remote[0].retention_rules]
        assert ages == sorted(ages)


class TestEffectiveRetentionRules:
    """`DatasetConfig.effective_retention_rules(destination)` -- the method
    `to_property` delegates to. Tested directly (not just through the
    base64/JSON wire format) so a break here is diagnosed at the right
    layer."""

    def _cfg(self, remote_retention=None):
        return DatasetConfig(
            name='pool/data',
            frequency=timedelta(hours=1),
            retention_rules=[RetentionRule(timedelta(hours=1), timedelta(days=1))],
            remote=[RemoteDatasetConfig(
                destination='offsite',
                retention_rules=remote_retention or [],
            )],
        )

    def test_no_destination_returns_dataset_level_rules(self):
        cfg = self._cfg(remote_retention=[RetentionRule(timedelta(days=1), timedelta(days=1825))])
        rules = cfg.effective_retention_rules()
        assert rules == cfg.retention_rules

    def test_destination_with_override_returns_override(self):
        override = [RetentionRule(timedelta(days=1), timedelta(days=1825))]
        cfg = self._cfg(remote_retention=override)
        rules = cfg.effective_retention_rules('offsite')
        assert rules == override
        assert rules != cfg.retention_rules

    def test_destination_without_override_inherits_dataset_level(self):
        cfg = self._cfg(remote_retention=[])
        rules = cfg.effective_retention_rules('offsite')
        assert rules == cfg.retention_rules

    def test_unknown_destination_falls_back_to_dataset_level(self):
        cfg = self._cfg(remote_retention=[RetentionRule(timedelta(days=1), timedelta(days=1825))])
        rules = cfg.effective_retention_rules('nonexistent')
        assert rules == cfg.retention_rules


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
            "    url: https://backup.example.com\n"
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
        assert cfg.destinations['offsite'].url == 'https://backup.example.com'
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

    @pytest.mark.parametrize('line', [
        'client_id_file: /var/lib/zfsbackup/my_id',
        'client_id_file:',
        'client_id_file: ""',
        'client_id_file: .',
    ], ids=['path', 'null', 'empty-string', 'dot'])
    def test_from_file_rejects_client_id_file(self, tmp_path, line):
        """M15/D2: the key is refused whatever its value."""
        f = tmp_path / 'cfg.yaml'
        f.write_text(f"datasets:\n  - name: pool/data\n{line}\n")
        with pytest.raises(ValueError, match='client_id_file was removed') as excinfo:
            BackupConfig.from_file(f)
        assert 'zfsbackup-config identity' in str(excinfo.value)

    def test_client_id_file_rejection_fires_before_dataset_errors(self, tmp_path):
        f = tmp_path / 'cfg.yaml'
        f.write_text(
            "client_id_file: /x/id\n"
            "datasets:\n  - name: pool/data\n    frequency: not-a-duration\n"
            "  - frequency: 5m\n"
        )
        with pytest.raises(ValueError, match='client_id_file was removed'):
            BackupConfig.from_file(f)
        # The same file without the key reports the dataset problem instead.
        f.write_text(
            "datasets:\n  - name: pool/data\n    frequency: not-a-duration\n"
        )
        with pytest.raises(ValueError) as excinfo:
            BackupConfig.from_file(f)
        assert 'client_id_file' not in str(excinfo.value)

    def test_backup_config_has_no_client_id_file(self):
        import dataclasses

        cfg = BackupConfig(datasets=[])
        assert not hasattr(cfg, 'client_id_file')
        assert not hasattr(cfg, 'effective_client_id_file')
        assert not hasattr(BackupConfig, 'effective_client_id_file')
        assert 'client_id_file' not in {f.name for f in dataclasses.fields(BackupConfig)}
        with pytest.raises(TypeError):
            BackupConfig(datasets=[], client_id_file=Path('/x/id'))

    def test_loading_a_config_never_resolves_home(self, tmp_path, monkeypatch):
        """The only live idea of the removed `effective_client_id_file` tests:
        a config's load and use must not freeze or read `$HOME` (the client id
        now lives in the database, so there is nothing to derive from it)."""
        def boom(*_a, **_k):
            raise AssertionError("config touched Path.home()")

        monkeypatch.setattr(Path, 'home', boom)
        f = tmp_path / 'cfg.yaml'
        f.write_text("datasets:\n  - name: pool/data\n")
        cfg = BackupConfig.from_file(f)
        assert cfg.same_as(BackupConfig.from_file(f))

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
        assert cfg.prune_interval is None
        assert cfg.effective_prune_interval == cfg.check_interval

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

    def test_from_file_destination_retention_undeclared_destination_raises(self, tmp_path):
        """Item 3b: a remote entry declaring `retention` for a destination not
        present in the top-level `destinations:` mapping must fail
        validation. `DatasetConfig.from_dict` alone cannot catch this -- it
        has no visibility into `destinations:` -- so this is enforced in
        `BackupConfig.from_file` after both are parsed. The check is no
        longer retention-specific (see
        test_from_file_remote_undeclared_destination_raises_even_without_retention),
        so the message names the reference generically rather than the
        `.retention` key path."""
        f = tmp_path / 'cfg.yaml'
        f.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "    remote:\n"
            "      - destination: offsite\n"
            "        retention:\n"
            "          1d: 5y\n"
        )
        with pytest.raises(ValueError) as exc_info:
            BackupConfig.from_file(f)
        msg = str(exc_info.value)
        assert 'datasets[pool/data].remote[offsite]' in msg
        assert 'offsite' in msg
        assert 'destinations' in msg

    def test_from_file_destination_retention_declared_destination_ok(self, tmp_path):
        f = tmp_path / 'cfg.yaml'
        f.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "    remote:\n"
            "      - destination: offsite\n"
            "        retention:\n"
            "          1d: 5y\n"
            "destinations:\n"
            "  offsite:\n"
            "    url: https://backup.example.com\n"
        )
        cfg = BackupConfig.from_file(f)  # must not raise
        assert len(cfg.datasets[0].remote[0].retention_rules) == 1

    def test_from_file_remote_undeclared_destination_raises_even_without_retention(self, tmp_path):
        # A remote entry with NO retention override still references a
        # destination that must exist in `destinations:` -- an undeclared
        # destination can never be backed up to (RemoteBackupManager.
        # backup_dataset looks it up the same way and would error every
        # cycle forever), so this now fails at load regardless of whether
        # the entry also carries a retention override. This supersedes the
        # old behaviour (previously named
        # test_from_file_remote_without_retention_needs_no_destination_declaration)
        # where only a retention override triggered the check, letting a
        # typo'd destination load clean and then log "Unknown destination"
        # forever.
        f = tmp_path / 'cfg.yaml'
        f.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "    remote:\n"
            "      - destination: offsite\n"
        )
        with pytest.raises(ValueError) as exc_info:
            BackupConfig.from_file(f)
        msg = str(exc_info.value)
        assert 'offsite' in msg
        assert 'destinations' in msg

    def test_from_file_remote_without_retention_declared_destination_ok(self, tmp_path):
        # The positive counterpart: once the destination IS declared, a
        # remote entry with no retention override loads cleanly and inherits
        # the dataset-level retention rules.
        f = tmp_path / 'cfg.yaml'
        f.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "    remote:\n"
            "      - destination: offsite\n"
            "destinations:\n"
            "  offsite:\n"
            "    url: https://backup.example.com\n"
        )
        cfg = BackupConfig.from_file(f)  # must not raise
        assert cfg.datasets[0].remote[0].destination == 'offsite'
        assert cfg.datasets[0].remote[0].retention_rules == []


# ---------------------------------------------------------------------------
# B2: BackupConfig.same_as -- exact structural equality
# ---------------------------------------------------------------------------


def _rich_config(**over) -> BackupConfig:
    """A config touching every nested type `same_as` walks: a dataset with
    retention rules, a remote entry with its own frequency and retention
    override, a destination, and the remote-server block."""
    data = dict(
        snapshot_prefix='zb',
        check_interval=Duration('5m'),
        prune_interval=Duration('1h'),
        datasets=[
            DatasetConfig(
                name='pool/a',
                frequency=Duration('1h'),
                retention_rules=[
                    RetentionRule(age=Duration('1d'), keep_for=Duration('4w')),
                    RetentionRule(age=Duration('1w'), keep_for=Duration('1M')),
                ],
                remote=[
                    RemoteDatasetConfig(
                        destination='offsite',
                        frequency=Duration('30m'),
                        retention_rules=[
                            RetentionRule(age=Duration('2d'), keep_for=Duration('8w')),
                        ],
                    )
                ],
            ),
            DatasetConfig(name='pool/b', recursive=True),
        ],
        destinations={'offsite': Destination(
            url='https://h/pool', server_fingerprint='ab' * 32)},
        remote_backup=RemoteServerConfig(target_dataset='pool/recv'),
        client_cert_file=Path('/x/cert.pem'),
        client_key_file=Path('/x/key.pem'),
    )
    data.update(over)
    return BackupConfig(**data)


@pytest.mark.unit
class TestSameAs:
    def test_two_independently_built_identical_configs_are_the_same(self):
        assert _rich_config().same_as(_rich_config())

    def test_a_config_is_the_same_as_itself_and_a_deepcopy(self):
        import copy

        c = _rich_config()
        assert c.same_as(c)
        assert c.same_as(copy.deepcopy(c))
        assert copy.deepcopy(c).same_as(c)

    def test_a_pickle_round_trip_is_the_same(self):
        import pickle

        c = _rich_config()
        assert c.same_as(pickle.loads(pickle.dumps(c)))

    def test_a_literal_only_difference_is_not_the_same_though_equal(self):
        """`1h` and `60m` are `==` (Duration inherits timedelta's `__eq__`),
        but are different operator-visible text: the whole point of `same_as`."""
        a = _rich_config(check_interval=Duration('1h'))
        b = _rich_config(check_interval=Duration('60m'))
        assert a.check_interval == b.check_interval
        assert a == b                       # dataclass == cannot see the literal
        assert not a.same_as(b) and not b.same_as(a)

    @pytest.mark.parametrize(
        "path",
        ['dataset_freq', 'retention_age', 'retention_keep', 'remote_freq',
         'remote_retention_age', 'remote_retention_keep'],
    )
    def test_a_literal_only_difference_deep_in_the_tree_is_seen(self, path):
        b = _rich_config()
        ds = b.datasets[0]
        if path == 'dataset_freq':
            ds.frequency = Duration('60m')
        elif path == 'retention_age':
            ds.retention_rules[0] = RetentionRule(Duration('24h'), ds.retention_rules[0].keep_for)
        elif path == 'retention_keep':
            ds.retention_rules[0] = RetentionRule(ds.retention_rules[0].age, Duration('28d'))
        elif path == 'remote_freq':
            ds.remote[0].frequency = Duration('1800s')
        elif path == 'remote_retention_age':
            rr = ds.remote[0].retention_rules[0]
            ds.remote[0].retention_rules[0] = RetentionRule(Duration('48h'), rr.keep_for)
        elif path == 'remote_retention_keep':
            rr = ds.remote[0].retention_rules[0]
            ds.remote[0].retention_rules[0] = RetentionRule(rr.age, Duration('56d'))
        a = _rich_config()
        assert a == b                        # equal by value...
        assert not a.same_as(b)              # ...different by literal

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda c: setattr(c, 'snapshot_prefix', 'other'),
            lambda c: setattr(c, 'check_interval', Duration('6m')),
            lambda c: setattr(c, 'api_port', 9999),
            lambda c: setattr(c, 'api_host', '0.0.0.0'),
            lambda c: setattr(c, 'dry_run', True),
            lambda c: setattr(c, 'client_cert_file', Path('/x/other.pem')),
            lambda c: setattr(c, 'client_key_file', Path('/x/other-key.pem')),
            lambda c: setattr(c.datasets[0], 'recursive', True),
            lambda c: setattr(c.datasets[0], 'enabled', False),
            lambda c: setattr(c.datasets[1], 'name', 'pool/c'),
            lambda c: c.datasets[0].remote[0].__setattr__('destination', 'elsewhere'),
            lambda c: c.destinations['offsite'].__setattr__('url', 'https://other/pool'),
            lambda c: c.destinations.update(extra=Destination(url='https://x/y')),
            lambda c: setattr(c.remote_backup, 'enabled', False),
            lambda c: setattr(c, 'remote_backup', None),
            lambda c: c.datasets.append(DatasetConfig(name='pool/c')),
            lambda c: c.datasets[0].retention_rules.pop(),
            lambda c: setattr(c, 'prune_interval', None),
            lambda c: setattr(c, 'prune_interval', Duration('2h')),
            lambda c: setattr(c.remote_backup, 'target_dataset', 'pool/other'),
            lambda c: setattr(c.datasets[0].remote[0], 'frequency', None),
            lambda c: setattr(c.datasets[0].remote[0], 'frequency', Duration('31m')),
            lambda c: c.datasets[0].remote[0].retention_rules.clear(),
            lambda c: setattr(c.datasets[0], 'frequency', Duration('2h')),
        ],
        ids=[
            'prefix', 'check_interval_value', 'api_port', 'api_host', 'dry_run',
            'client_cert_file', 'client_key_file', 'recursive', 'enabled', 'dataset_name',
            'remote_destination', 'destination_url', 'extra_destination',
            'remote_backup_flag', 'remote_backup_none', 'dataset_added',
            'retention_rule_dropped', 'prune_interval_to_none', 'prune_interval_value',
            'target_dataset', 'remote_freq_to_none', 'remote_freq_value',
            'remote_override_emptied', 'dataset_freq_value',
        ],
    )
    def test_any_value_difference_is_not_the_same(self, mutate):
        b = _rich_config()
        mutate(b)
        assert not _rich_config().same_as(b)
        assert not b.same_as(_rich_config())

    def test_dataset_order_is_a_difference(self):
        a = _rich_config()
        b = _rich_config()
        b.datasets.reverse()
        assert not a.same_as(b)

    def test_retention_rule_order_is_a_difference(self):
        a = _rich_config()
        b = _rich_config()
        b.datasets[0].retention_rules.reverse()
        assert not a.same_as(b)

    def test_destination_dict_order_is_not_a_difference(self):
        a = _rich_config(destinations={
            'one': Destination(url='https://1/p'), 'two': Destination(url='https://2/p'),
        })
        b = _rich_config(destinations={
            'two': Destination(url='https://2/p'), 'one': Destination(url='https://1/p'),
        })
        assert a.same_as(b)

    def test_a_bare_timedelta_against_a_duration_compares_values_only(self):
        """A bare `timedelta` has no literal to compare."""
        a = _rich_config(check_interval=timedelta(minutes=5))
        b = _rich_config(check_interval=Duration('5m'))
        assert a.same_as(b) and b.same_as(a)
        c = _rich_config(check_interval=Duration('6m'))
        assert not a.same_as(c)

    def test_a_timedelta_never_equals_a_non_timedelta(self):
        a = _rich_config(check_interval=Duration('5m'))
        b = _rich_config(check_interval=300)
        assert not a.same_as(b) and not b.same_as(a)

    def test_none_against_a_value_is_a_difference_both_ways(self):
        a = _rich_config(prune_interval=None)
        b = _rich_config(prune_interval=Duration('1h'))
        assert not a.same_as(b) and not b.same_as(a)
        assert a.same_as(_rich_config(prune_interval=None))
        c = _rich_config()
        c.datasets[0].remote[0].frequency = None
        assert not c.same_as(_rich_config()) and not _rich_config().same_as(c)

    @pytest.mark.parametrize("other", [None, 'x', 42, {}, object()])
    def test_a_wrong_type_is_not_the_same(self, other):
        assert not _rich_config().same_as(other)

    def test_a_subclass_is_not_the_same(self):
        class Sub(BackupConfig):
            pass

        base = _rich_config()
        sub = Sub(**{f: getattr(base, f) for f in base.__dataclass_fields__})
        assert not base.same_as(sub)

    def test_a_bool_is_not_an_int(self):
        a = _rich_config(api_port=1)
        b = _rich_config(api_port=True)
        assert not a.same_as(b)

    def test_a_config_loaded_twice_from_the_same_yaml_is_the_same(self, tmp_path):
        f = tmp_path / 'c.yaml'
        f.write_text(
            "check_interval: 600s\nprune_interval: 90m\n"
            "datasets:\n  - name: pool/data\n    frequency: 60m\n"
            "    retention:\n      1d: 4w\n      1w: 1M\n"
        )
        assert BackupConfig.from_file(f).same_as(BackupConfig.from_file(f))

    def test_the_same_yaml_with_one_literal_changed_is_not_the_same(self, tmp_path):
        body = (
            "check_interval: {ci}\n"
            "datasets:\n  - name: pool/data\n    frequency: 1h\n"
        )
        a, b = tmp_path / 'a.yaml', tmp_path / 'b.yaml'
        a.write_text(body.format(ci='1h'))
        b.write_text(body.format(ci='60m'))
        ca, cb = BackupConfig.from_file(a), BackupConfig.from_file(b)
        assert ca == cb and not ca.same_as(cb)


def _walk_dataclasses(obj, found=None):
    """Every dataclass instance reachable from `obj` (lists, dicts, fields)."""
    import dataclasses

    found = [] if found is None else found
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        found.append(obj)
        for f in dataclasses.fields(obj):
            _walk_dataclasses(getattr(obj, f.name), found)
    elif isinstance(obj, (list, tuple)):
        for x in obj:
            _walk_dataclasses(x, found)
    elif isinstance(obj, dict):
        for x in obj.values():
            _walk_dataclasses(x, found)
    return found


def _changed(value):
    """A value of the same kind that differs from `value` in VALUE."""
    from datetime import timedelta as _td

    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, float):
        return value + 1.0
    if isinstance(value, str):
        return value + 'x'
    if isinstance(value, Path):
        return value / 'changed'
    if isinstance(value, _td):
        return Duration(value + _td(seconds=7))
    if isinstance(value, list):
        return value[:-1] if value else [object()]
    if isinstance(value, dict):
        new = dict(value)
        new['__added__'] = next(iter(value.values())) if value else None
        return new
    raise AssertionError(f"extend _changed() for {type(value).__name__}")


@pytest.mark.unit
class TestSameAsCoversEveryField:
    """A guard, not a sample: every field of every config dataclass must move
    `same_as`. A new field, or a `field(compare=False)`, cannot slip through."""

    def _classes_in_model(self):
        import dataclasses

        import zfsbackup.config.model as model

        return {
            obj for obj in vars(model).values()
            if isinstance(obj, type) and dataclasses.is_dataclass(obj)
            and obj.__module__ == model.__name__
        }

    def test_the_sample_config_reaches_every_config_dataclass(self):
        reached = {type(o) for o in _walk_dataclasses(_rich_config())}
        missing = self._classes_in_model() - reached
        assert not missing, (
            f"add {sorted(c.__name__ for c in missing)} to _rich_config so the "
            "field guard below covers them"
        )

    def test_every_field_of_every_instance_is_detected(self):
        import copy
        import dataclasses

        base = _rich_config()
        instances = _walk_dataclasses(base)
        checked = 0
        for index, inst in enumerate(instances):
            for f in dataclasses.fields(inst):
                variant = copy.deepcopy(base)
                target = _walk_dataclasses(variant)[index]
                current = getattr(target, f.name)
                if dataclasses.is_dataclass(current):
                    continue                  # walked as an instance in its own right
                assert current is not None, (
                    f"{type(inst).__name__}.{f.name} is None in the sample config; "
                    "give _rich_config a value"
                )
                setattr(target, f.name, _changed(current))
                assert not base.same_as(variant), f"{type(inst).__name__}.{f.name}"
                assert not variant.same_as(base), f"{type(inst).__name__}.{f.name}"
                checked += 1
        assert checked >= 25

    def test_no_dataclass_carries_state_outside_its_fields(self):
        """A plain attribute (not a field) is invisible to `fields()`, hence to
        `same_as`."""
        import dataclasses

        for inst in _walk_dataclasses(_rich_config()):
            names = {f.name for f in dataclasses.fields(inst)}
            assert set(vars(inst)) <= names, type(inst).__name__

    def test_no_field_opts_out_of_comparison(self):
        import dataclasses

        for cls in self._classes_in_model():
            for f in dataclasses.fields(cls):
                assert f.compare, f"{cls.__name__}.{f.name} is compare=False"

    def test_duration_state_is_value_plus_literal_only(self):
        d = Duration('1h')
        assert set(vars(d)) <= {'_literal'}


# ---------------------------------------------------------------------------
# TLS/mTLS M4: fingerprints, https-only destinations, client id grammar,
# client certificate/key paths
# ---------------------------------------------------------------------------

import re  # noqa: E402

from zfsbackup.config import (  # noqa: E402
    client_id_problem,
    client_tls_files_problem,
    destination_url_problem,
    generate_client_id,
    normalize_fingerprint,
)

HEX64 = '0123456789abcdef' * 4
HEX64_COLON = ':'.join(HEX64[i:i + 2] for i in range(0, 64, 2))


@pytest.mark.unit
class TestNormalizeFingerprint:
    def test_canonical_form_is_returned_unchanged(self):
        assert normalize_fingerprint(HEX64) == HEX64

    @pytest.mark.parametrize('value', [HEX64.upper(), HEX64[:32].upper() + HEX64[32:]])
    def test_case_is_folded_to_lower(self, value):
        assert normalize_fingerprint(value) == HEX64

    @pytest.mark.parametrize(
        'value', [HEX64_COLON, HEX64_COLON.upper()], ids=['lower-colon', 'upper-colon'],
    )
    def test_colon_form_openssl_prints_is_accepted(self, value):
        assert normalize_fingerprint(value) == HEX64

    @pytest.mark.parametrize('pad', ['  {}', '{}\n', '\t{} \n'])
    def test_surrounding_whitespace_is_ignored(self, pad):
        assert normalize_fingerprint(pad.format(HEX64)) == HEX64
        assert normalize_fingerprint(pad.format(HEX64_COLON)) == HEX64

    @pytest.mark.parametrize(
        'value',
        [
            '', ' ', HEX64[:63], HEX64 + '0', HEX64[:32], '0x' + HEX64[:62],
            HEX64[:-1] + 'g',                                   # non-hex digit
            HEX64[:10] + ' ' + HEX64[10:],                      # inner space
            '\uff10' * 64,                                      # full-width digits
            '\u0660' * 64,                                      # Arabic-indic digits
            HEX64_COLON[:-3],                                   # 31 pairs
            HEX64_COLON + ':00',                                # 33 pairs
            HEX64_COLON.replace(':', '-'),                      # wrong separator
            HEX64_COLON.replace(':', '', 1),                    # one colon missing
            'a' + HEX64_COLON[1:].replace(':', '', 1),
            ':' + HEX64_COLON, HEX64_COLON + ':',               # leading/trailing colon
            HEX64[:60] + ':' + HEX64[60:],                      # colon in the 64-hex form
            HEX64_COLON.replace(':', '::', 1),
        ],
    )
    def test_malformed_values_are_rejected(self, value):
        with pytest.raises(ValueError):
            normalize_fingerprint(value)

    @pytest.mark.parametrize('value', [None, 5, b'ab' * 32, 1.5, ['ab'], True])
    def test_a_non_string_is_rejected_naming_its_type(self, value):
        with pytest.raises(ValueError, match=type(value).__name__):
            normalize_fingerprint(value)

    def test_the_result_is_always_64_lowercase_hex(self):
        for value in (HEX64.upper(), HEX64_COLON.upper(), f' {HEX64} '):
            out = normalize_fingerprint(value)
            assert re.fullmatch(r'[0-9a-f]{64}', out)

    def test_it_is_idempotent(self):
        once = normalize_fingerprint(HEX64_COLON.upper())
        assert normalize_fingerprint(once) == once


@pytest.mark.unit
class TestDestinationUrlProblem:
    @pytest.mark.parametrize(
        'url',
        ['https://h', 'https://h.example.com:8443', 'https://h:8443/pool/x',
         'https://[::1]:8443', 'https://10.0.0.2', '  https://h  ',
         'HTTPS://H', 'Https://h:443', 'https://user@h'],
    )
    def test_https_urls_with_a_host_are_fine(self, url):
        assert destination_url_problem(url) is None

    @pytest.mark.parametrize('url', ['http://h', 'HTTP://h:80', 'Http://h/x', ' http://h '])
    def test_plaintext_http_is_refused_with_the_https_hint(self, url):
        problem = destination_url_problem(url)
        assert problem is not None
        assert 'plaintext http is not supported' in problem and 'use https://' in problem

    @pytest.mark.parametrize(
        'url, scheme',
        [('ssh://h/pool', 'ssh'), ('ftp://h', 'ftp'), ('ws://h', 'ws'),
         ('wss://h', 'wss'), ('h:8080/x', 'h')],
    )
    def test_any_other_scheme_is_refused_naming_it(self, url, scheme):
        problem = destination_url_problem(url)
        assert problem is not None and repr(scheme) in problem

    @pytest.mark.parametrize('url', ['backup.example.com', '//h', '/path/only', 'localhost'])
    def test_no_scheme_is_refused(self, url):
        problem = destination_url_problem(url)
        assert problem is not None and 'no scheme' in problem

    @pytest.mark.parametrize('url', ['https://', 'https:///path', 'https://:443', 'https://?q=1'])
    def test_no_host_is_refused(self, url):
        problem = destination_url_problem(url)
        assert problem is not None and 'no host' in problem

    @pytest.mark.parametrize(
        'url',
        ['https://h:99999', 'https://h:abc', 'https://h:-1', 'https://h:65536',
         'https://[::1', 'https://h:8o80'],
    )
    def test_a_bad_port_or_malformed_authority_is_refused(self, url):
        problem = destination_url_problem(url)
        assert problem is not None and 'not a valid URL' in problem

    @pytest.mark.parametrize(
        'url',
        ['https://h:0', 'https://h:00', 'https://h:0/path', 'https://[::1]:0',
         'https://user@h:0', ' https://h:0 '],
    )
    def test_port_zero_is_refused_naming_the_url(self, url):
        problem = destination_url_problem(url)
        assert problem is not None and 'port 0' in problem and 'leave the port out' in problem

    @pytest.mark.parametrize(
        'url',
        ['https://h:', 'https://h:/path', 'https://[::1]:', 'https://u@h:', 'https://u:p@h:',
         'https://h.example.com:?q=1', ' https://h: '],
    )
    def test_an_empty_port_is_refused_rather_than_read_as_443(self, url):
        problem = destination_url_problem(url)
        assert problem is not None and 'empty port' in problem

    @pytest.mark.parametrize(
        'url', ['https://h:1', 'https://h:443', 'https://h:65535', 'https://[::1]:8443',
                'https://u@h:8443', 'https://u:p@h:8443/x', 'https://h/a:b', 'https://h/x:',
                'https://h?x=:', 'https://h#:'],
    )
    def test_a_real_port_and_a_colon_outside_the_authority_are_fine(self, url):
        assert destination_url_problem(url) is None

    @pytest.mark.parametrize('url', ['', '   ', '\n', None, 5, b'https://h', ['https://h']])
    def test_empty_or_non_string_is_refused(self, url):
        assert destination_url_problem(url) == 'url must be a non-empty string'

    def test_it_never_raises(self):
        for url in ['https://h:\u0663', 'https://\x00', 'https://h\n', '\x00', 'https://[x]']:
            result = destination_url_problem(url)
            assert result is None or isinstance(result, str)


@pytest.mark.unit
class TestClientIdProblem:
    @pytest.mark.parametrize(
        'value',
        ['a', 'A', '0', '9z', 'host-1234abcd', 'a.b', 'a_b', 'a-b',
         'a.', 'a-', 'a_', 'a..b', 'a' * 64, 'Z' * 63 + '9', 'My-Host.example.com-0a1b2c3d'],
    )
    def test_valid_ids(self, value):
        assert client_id_problem(value) is None

    def test_empty_is_refused(self):
        assert 'empty' in client_id_problem('')

    @pytest.mark.parametrize('n', [65, 66, 100, 1000])
    def test_over_64_characters_is_refused_naming_the_length(self, n):
        problem = client_id_problem('a' * n)
        assert problem is not None and str(n) in problem and '64' in problem

    def test_exactly_64_characters_is_fine_and_65_is_not(self):
        assert client_id_problem('a' * 64) is None
        assert client_id_problem('a' * 65) is not None

    @pytest.mark.parametrize('value', ['.', '..', '.a', '.hidden', '...', '.-', '._'])
    def test_a_leading_dot_is_refused(self, value):
        problem = client_id_problem(value)
        assert problem == "client id must start with an ASCII letter or digit, not '.'"
        assert "must not start with" not in problem        # the old wording is gone

    @pytest.mark.parametrize('value, first', [
        ('-', '-'), ('-a', '-'), ('--', '-'), ('-x-y', '-'), ('-.', '-'),
        ('_', '_'), ('_a', '_'), ('__', '_'), ('_x_y', '_'),
    ])
    def test_a_leading_hyphen_or_underscore_is_refused_naming_the_character(self, value, first):
        problem = client_id_problem(value)
        assert problem == (
            f"client id must start with an ASCII letter or digit, not {first!r}")

    @pytest.mark.parametrize('first', list('abzAZ059'))
    def test_every_letter_or_digit_may_start_an_id(self, first):
        assert client_id_problem(first) is None
        assert client_id_problem(first + '-._') is None

    @pytest.mark.parametrize('value', ['-\x00', '-a b', '-\u00e9', '-\n'])
    def test_a_bad_character_is_reported_before_a_bad_first_character(self, value):
        # Both rules are broken; the character-set message wins (it is the more basic one).
        assert 'ASCII letters, digits' in client_id_problem(value)

    @pytest.mark.parametrize(
        'value',
        ['a b', ' a', 'a ', 'a/b', '/a', 'a\\b', 'a@b', 'a#b', 'a:b', 'a=b', 'a%41',
         'a\nb', 'abc\n', '\nabc', 'a\tb', 'a\x00b', '\x00', 'a*b', 'a?b', 'a[b', 'a,b',
         '\u00e9', 'caf\u00e9', '\u65e5\u672c', 'a\u200bb', '\uff21', 'a\u0301'],
    )
    def test_characters_outside_the_grammar_are_refused(self, value):
        problem = client_id_problem(value)
        assert problem is not None and 'ASCII letters, digits' in problem

    @pytest.mark.parametrize('value', [None, 5, b'abc', ['a'], 1.5])
    def test_non_strings_are_refused(self, value):
        assert 'must be a string' in client_id_problem(value)

    def test_the_id_is_safe_as_a_path_component_and_a_zfs_name_part(self):
        # The point of the grammar: nothing that can climb or split a path or a
        # ZFS name (`/`, `@`, `#`, `%`, whitespace, `..`) is ever valid.
        for bad in ['..', '../x', 'a/../b', 'a@snap', 'a#bm', 'a%b', 'a b', '.']:
            assert client_id_problem(bad) is not None


@pytest.mark.unit
class TestGenerateClientId:
    @staticmethod
    def _host(monkeypatch, name):
        import zfsbackup.config.model as model

        monkeypatch.setattr(model.socket, 'gethostname', lambda: name)

    def test_shape_is_hostname_dash_eight_hex(self, monkeypatch):
        self._host(monkeypatch, 'backup01')
        assert re.fullmatch(r'backup01-[0-9a-f]{8}', generate_client_id())

    def test_two_calls_differ(self, monkeypatch):
        self._host(monkeypatch, 'h')
        assert generate_client_id() != generate_client_id()

    def test_a_long_hostname_is_truncated_to_55_so_the_id_is_64(self, monkeypatch):
        self._host(monkeypatch, 'a' * 200)
        cid = generate_client_id()
        assert len(cid) == 64 and cid.startswith('a' * 55 + '-')
        assert client_id_problem(cid) is None

    def test_a_55_character_hostname_fits_exactly(self, monkeypatch):
        self._host(monkeypatch, 'b' * 55)
        cid = generate_client_id()
        assert len(cid) == 64 and cid.startswith('b' * 55 + '-')

    def test_a_54_character_hostname_is_kept_whole(self, monkeypatch):
        self._host(monkeypatch, 'c' * 54)
        assert generate_client_id().startswith('c' * 54 + '-')

    def test_non_ascii_characters_become_dashes(self, monkeypatch):
        self._host(monkeypatch, 'caf\u00e9')
        cid = generate_client_id()
        assert cid.startswith('caf--') and client_id_problem(cid) is None

    def test_an_all_non_ascii_hostname_is_still_valid(self, monkeypatch):
        self._host(monkeypatch, '\u65e5\u672c\u8a9e')
        cid = generate_client_id()
        assert re.fullmatch(r'host-[0-9a-f]{8}', cid)
        assert client_id_problem(cid) is None

    @pytest.mark.parametrize('name', ['', None])
    def test_an_empty_hostname_becomes_host(self, monkeypatch, name):
        self._host(monkeypatch, name)
        assert re.fullmatch(r'host-[0-9a-f]{8}', generate_client_id())

    @pytest.mark.parametrize(
        'name, prefix',
        [('.local', 'local-'), ('...x', 'x-'), ('.a.b', 'a.b-'), ('.-x', 'x-'),
         ('-x', 'x-'), ('_x', 'x-'), ('-_.-x', 'x-'), ('_.x_', 'x_-')],
    )
    def test_leading_dots_are_dropped(self, monkeypatch, name, prefix):
        self._host(monkeypatch, name)
        cid = generate_client_id()
        assert cid.startswith(prefix) and client_id_problem(cid) is None

    @pytest.mark.parametrize('name', ['....', '-', '_', '-_.', '---', '__', '.-_.-_'])
    def test_a_hostname_of_only_dots_hyphens_and_underscores_becomes_host(self, monkeypatch, name):
        self._host(monkeypatch, name)
        assert re.fullmatch(r'host-[0-9a-f]{8}', generate_client_id())

    @pytest.mark.parametrize('name', ['\u65e5\u672c\u8a9e', '\u00e9', '\u00e9\u00e8', ' ', '/', '@@'])
    def test_a_hostname_with_nothing_ascii_left_becomes_host(self, monkeypatch, name):
        # Every character turns into '-', and leading '-' is dropped: nothing remains.
        self._host(monkeypatch, name)
        assert re.fullmatch(r'host-[0-9a-f]{8}', generate_client_id())

    def test_a_non_ascii_prefix_does_not_leave_a_leading_hyphen(self, monkeypatch):
        self._host(monkeypatch, '\u00e9host')
        cid = generate_client_id()
        assert cid.startswith('host-') and client_id_problem(cid) is None

    def test_the_generator_never_produces_an_invalid_id_over_random_hostnames(self, monkeypatch):
        """Property test (seeded, no hypothesis in this environment): hostnames drawn from an
        alphabet heavy in the troublesome characters, any length, never give an invalid id --
        the generator's own assert is not what makes this pass."""
        import random

        alphabet = list('.-_ab09AZ /@:%\n\x00') + ['\u00e9', '\u65e5', '\u200b', '\U0001f600']
        rng = random.Random(20261020)
        for _ in range(600):
            name = ''.join(rng.choice(alphabet) for _ in range(rng.randint(0, 90)))
            self._host(monkeypatch, name)
            cid = generate_client_id()
            assert client_id_problem(cid) is None, (name, cid)
            assert cid[0].isascii() and cid[0].isalnum(), (name, cid)

    def test_a_long_leading_dot_run_is_dropped_before_truncating(self, monkeypatch):
        self._host(monkeypatch, '.' * 100 + 'z' * 100)
        cid = generate_client_id()
        assert cid.startswith('z' * 55 + '-') and len(cid) == 64

    def test_gethostname_raising_oserror_falls_back_to_host(self, monkeypatch):
        import zfsbackup.config.model as model

        def boom():
            raise OSError('no hostname')

        monkeypatch.setattr(model.socket, 'gethostname', boom)
        assert re.fullmatch(r'host-[0-9a-f]{8}', generate_client_id())

    @pytest.mark.parametrize(
        'name',
        ['a b', 'a/b', 'a@b', 'x\ny', 'a\x00b', 'h%41', 'UPPER', 'under_score', 'a..b',
         '\u00e9' * 80, ' ' * 70, 'ok.example.com', '-', '.-.', 'a' * 55, 'a' * 56],
    )
    def test_the_result_always_satisfies_the_grammar(self, monkeypatch, name):
        self._host(monkeypatch, name)
        for _ in range(3):
            assert client_id_problem(generate_client_id()) is None


@pytest.mark.unit
class TestClientTlsFilesProblem:
    @pytest.mark.parametrize(
        'cert, key',
        [(None, None), ('/etc/c.pem', '/etc/k.pem'), (Path('/etc/c.pem'), Path('/etc/k.pem')),
         (Path('/c'), '/k')],
    )
    def test_neither_or_both_absolute_is_fine(self, cert, key):
        assert client_tls_files_problem(cert, key) is None

    @pytest.mark.parametrize('cert, key', [('/etc/c.pem', None), (None, Path('/etc/k.pem'))])
    def test_exactly_one_is_refused(self, cert, key):
        assert 'must be set together' in client_tls_files_problem(cert, key)

    @pytest.mark.parametrize(
        'cert, key, which',
        [('c.pem', '/k', 'client_cert_file'), ('/c', 'k.pem', 'client_key_file'),
         (Path('rel/c.pem'), Path('/k'), 'client_cert_file'),
         ('./c.pem', '/k', 'client_cert_file'), ('~/c.pem', '/k', 'client_cert_file')],
    )
    def test_a_relative_path_is_refused_naming_the_field(self, cert, key, which):
        problem = client_tls_files_problem(cert, key)
        assert which in problem and 'absolute' in problem

    def test_the_pairing_rule_wins_over_the_absolute_rule(self):
        assert 'together' in client_tls_files_problem('relative.pem', None)

    def test_a_relative_cert_is_reported_before_a_relative_key(self):
        assert 'client_cert_file' in client_tls_files_problem('c', 'k')


@pytest.mark.unit
class TestDestinationFingerprint:
    def test_default_is_not_enrolled(self):
        assert Destination(url='https://h').server_fingerprint is None

    @pytest.mark.parametrize('value', [HEX64, HEX64.upper(), HEX64_COLON, f' {HEX64_COLON.upper()} '])
    def test_it_is_normalised_on_construction(self, value):
        assert Destination(url='https://h', server_fingerprint=value).server_fingerprint == HEX64

    @pytest.mark.parametrize('value', ['', 'abc', HEX64[:-1], 5, b'x' * 64])
    def test_an_invalid_value_raises_at_construction(self, value):
        with pytest.raises(ValueError):
            Destination(url='https://h', server_fingerprint=value)

    def test_the_url_scheme_is_not_checked_by_the_dataclass(self):
        # A config-boundary policy (from_file / validate_config), so a legacy
        # row can still be loaded and repaired.
        assert Destination(url='http://legacy').url == 'http://legacy'

    def test_positional_construction_still_works(self):
        d = Destination('https://h')
        assert d.url == 'https://h' and d.server_fingerprint is None

    def test_equality_covers_the_fingerprint(self):
        assert Destination('https://h', HEX64) == Destination('https://h', HEX64.upper())
        assert Destination('https://h', HEX64) != Destination('https://h')


@pytest.mark.unit
class TestFromFileTlsKeys:
    BASE = "datasets:\n  - name: pool/data\n"

    def _load(self, tmp_path, text, name='c.yaml'):
        f = tmp_path / name
        f.write_text(self.BASE + text)
        return BackupConfig.from_file(f)

    def _dest(self, fp_line, url='https://backup.example.com'):
        return f"destinations:\n  offsite:\n    url: {url}\n{fp_line}"

    def test_a_quoted_fingerprint_is_loaded_and_normalised(self, tmp_path):
        cfg = self._load(tmp_path, self._dest(f'    server_fingerprint: "{HEX64.upper()}"\n'))
        assert cfg.destinations['offsite'].server_fingerprint == HEX64

    def test_a_quoted_colon_fingerprint_is_loaded_and_normalised(self, tmp_path):
        cfg = self._load(tmp_path, self._dest(f'    server_fingerprint: "{HEX64_COLON.upper()}"\n'))
        assert cfg.destinations['offsite'].server_fingerprint == HEX64

    def test_an_unquoted_letter_containing_fingerprint_is_a_string(self, tmp_path):
        cfg = self._load(tmp_path, self._dest(f'    server_fingerprint: {HEX64}\n'))
        assert cfg.destinations['offsite'].server_fingerprint == HEX64

    def test_an_unquoted_all_digit_fingerprint_is_an_int_and_refused(self, tmp_path):
        # YAML types `111...1` (64 digits) as an int; that must fail loudly
        # rather than be stringified (leading zeros would already be lost).
        with pytest.raises(ValueError, match=r"destinations\[offsite\]\.server_fingerprint must be a string, got int"):
            self._load(tmp_path, self._dest('    server_fingerprint: ' + '1' * 64 + '\n'))

    def test_an_unquoted_colon_fingerprint_of_small_decimal_pairs_is_a_sexagesimal_int(self, tmp_path):
        # PyYAML reads `12:34:12:34...` as a base-60 integer.
        value = ':'.join(['12', '34'] * 16)
        with pytest.raises(ValueError, match='must be a string, got int'):
            self._load(tmp_path, self._dest(f'    server_fingerprint: {value}\n'))

    @pytest.mark.parametrize('line', ['    server_fingerprint:\n', '    server_fingerprint: ""\n',
                                      '    server_fingerprint: "   "\n', '    server_fingerprint: null\n'])
    def test_null_and_blank_mean_not_enrolled(self, tmp_path, line):
        cfg = self._load(tmp_path, self._dest(line))
        assert cfg.destinations['offsite'].server_fingerprint is None

    def test_an_absent_key_means_not_enrolled(self, tmp_path):
        cfg = self._load(tmp_path, self._dest(''))
        assert cfg.destinations['offsite'].server_fingerprint is None

    @pytest.mark.parametrize('bad', [HEX64[:-1], 'z' * 64, HEX64 + '00', 'not-a-fingerprint'])
    def test_a_malformed_fingerprint_names_its_destination(self, tmp_path, bad):
        with pytest.raises(ValueError, match=r'destinations\[offsite\]\.server_fingerprint'):
            self._load(tmp_path, self._dest(f'    server_fingerprint: "{bad}"\n'))

    @pytest.mark.parametrize('value', ['[1, 2]', '{a: b}', 'true', '1.5'])
    def test_a_non_string_fingerprint_is_refused(self, tmp_path, value):
        with pytest.raises(ValueError, match='must be a string'):
            self._load(tmp_path, self._dest(f'    server_fingerprint: {value}\n'))

    @pytest.mark.parametrize('url', ['http://h:8080', 'HTTP://h', 'ssh://h/pool', 'h:8080',
                                     'https://', 'https://h:99999'])
    def test_a_non_https_destination_url_is_refused(self, tmp_path, url):
        with pytest.raises(ValueError, match=r'destinations\[offsite\]: '):
            self._load(tmp_path, self._dest('', url=f'"{url}"'))

    @pytest.mark.parametrize('url, text', [
        ('https://h:0', 'port 0'), ('https://h:', 'empty port'),
        ('https://[::1]:', 'empty port'), ('https://u@h:', 'empty port'),
        ('https://[::1]:0', 'port 0'),
    ])
    def test_port_zero_and_an_empty_port_are_refused_when_loading_yaml(self, tmp_path, url, text):
        with pytest.raises(ValueError, match=r'destinations\[offsite\]: ') as excinfo:
            self._load(tmp_path, self._dest('', url=f'"{url}"'))
        assert text in str(excinfo.value)

    def test_the_http_message_says_to_use_https(self, tmp_path):
        with pytest.raises(ValueError, match='plaintext http is not supported; use https://'):
            self._load(tmp_path, self._dest('', url='http://h'))

    def test_the_url_is_stripped(self, tmp_path):
        cfg = self._load(tmp_path, self._dest('', url='"  https://h:8443  "'))
        assert cfg.destinations['offsite'].url == 'https://h:8443'

    def test_an_uppercase_scheme_is_accepted(self, tmp_path):
        cfg = self._load(tmp_path, self._dest('', url='"HTTPS://H"'))
        assert cfg.destinations['offsite'].url == 'HTTPS://H'

    def test_cert_and_key_are_loaded_as_paths(self, tmp_path):
        cfg = self._load(tmp_path, "client_cert_file: /etc/zb/c.pem\nclient_key_file: /etc/zb/k.pem\n")
        assert cfg.client_cert_file == Path('/etc/zb/c.pem')
        assert cfg.client_key_file == Path('/etc/zb/k.pem')

    def test_cert_and_key_default_to_none(self, tmp_path):
        cfg = self._load(tmp_path, "")
        assert cfg.client_cert_file is None and cfg.client_key_file is None

    @pytest.mark.parametrize('text', ['client_cert_file: ""\nclient_key_file: ""\n',
                                      'client_cert_file:\nclient_key_file:\n'])
    def test_empty_or_null_cert_and_key_are_unset(self, tmp_path, text):
        cfg = self._load(tmp_path, text)
        assert cfg.client_cert_file is None and cfg.client_key_file is None

    @pytest.mark.parametrize('text', ['client_cert_file: /etc/c.pem\n', 'client_key_file: /etc/k.pem\n',
                                      'client_cert_file: /etc/c.pem\nclient_key_file: ""\n'])
    def test_only_one_of_cert_and_key_is_refused(self, tmp_path, text):
        with pytest.raises(ValueError, match='must be set together'):
            self._load(tmp_path, text)

    @pytest.mark.parametrize('text, field', [
        ('client_cert_file: c.pem\nclient_key_file: /k\n', 'client_cert_file'),
        ('client_cert_file: /c\nclient_key_file: rel/k.pem\n', 'client_key_file'),
    ])
    def test_a_relative_path_is_refused(self, tmp_path, text, field):
        with pytest.raises(ValueError, match=f'{field}.*absolute'):
            self._load(tmp_path, text)

    def test_a_non_string_path_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match='client_cert_file must be a string'):
            self._load(tmp_path, "client_cert_file: 5\nclient_key_file: /k\n")

    def test_the_shipped_example_yaml_loads(self):
        example = Path(__file__).resolve().parent.parent / 'zfsbackup' / 'config.example.yaml'
        cfg = BackupConfig.from_file(example)
        assert not hasattr(cfg, 'client_id_file')
        assert all(d.url.lower().startswith('https://') for d in cfg.destinations.values())


class TestFromPropertyIsStrict:
    """`DatasetConfig.from_property` decodes base64 with `validate=True` (plan B-1c): a character
    outside the standard alphabet -- a newline, a tab, a non-ASCII character -- raises instead of
    being skipped, so a value carrying an embedded row or control character never decodes as if it
    were clean. `to_property()` output always decodes."""

    @staticmethod
    def good():
        from zfsbackup.config import DatasetConfig
        return DatasetConfig(name="pool/data").to_property()

    @pytest.mark.parametrize("where", [0, 1, 8, 20, -1])
    @pytest.mark.parametrize("char", ["\n", "\t", "\r", " ", "\x00"])
    def test_a_stray_whitespace_or_control_character_is_refused(self, where, char):
        from zfsbackup.config import DatasetConfig
        value = self.good()
        at = where if where >= 0 else len(value)
        with pytest.raises(Exception):
            DatasetConfig.from_property(value[:at] + char + value[at:])

    def test_lax_decoding_would_have_accepted_these(self):
        """The control: the stdlib's default decoder skips them, so this is the change."""
        value = self.good()
        assert base64.b64decode(value[:10] + "\n" + value[10:]) == base64.b64decode(value)

    @pytest.mark.parametrize("junk", ["é", "é", "日本", " ", "\ud800", "\udfff", "\U0001f600"])
    def test_non_ascii_and_lone_surrogates_are_refused(self, junk):
        from zfsbackup.config import DatasetConfig
        for value in (junk + self.good(), self.good()[:10] + junk + self.good()[10:], self.good() + junk):
            with pytest.raises(Exception):
                DatasetConfig.from_property(value)

    def test_unpadded_base64_is_refused(self):
        from zfsbackup.config import DatasetConfig
        # names of 1..3 extra characters give every padding length; each must be refused unpadded
        for extra in ("", "x", "xy"):
            value = DatasetConfig(name="pool/data" + extra).to_property()
            stripped = value.rstrip("=")
            if stripped == value:
                continue
            with pytest.raises(Exception):
                DatasetConfig.from_property(stripped)
        value = DatasetConfig(name="pool/datax").to_property()
        stripped = value.rstrip("=")
        assert stripped != value, "the fixture must need padding"
        with pytest.raises(Exception):
            DatasetConfig.from_property(stripped)

    @pytest.mark.parametrize("value", ["", "=", "====", "!!!!", "e30"])
    def test_garbage_is_refused(self, value):
        from zfsbackup.config import DatasetConfig
        with pytest.raises(Exception):
            DatasetConfig.from_property(value)

    def test_the_urlsafe_alphabet_is_refused(self):
        from zfsbackup.config import DatasetConfig
        payload = json.dumps({"name": "a?b>c", "frequency": 5}).encode()
        urlsafe = base64.urlsafe_b64encode(payload).decode()
        standard = base64.b64encode(payload).decode()
        assert urlsafe != standard
        DatasetConfig.from_property(standard)
        with pytest.raises(Exception):
            DatasetConfig.from_property(urlsafe)

    @pytest.mark.parametrize("make", [
        lambda D, R, Rr, T: D(name="pool/data"),
        lambda D, R, Rr, T: D(name="pool/with space/x", recursive=True),
        lambda D, R, Rr, T: D(name="tank/é", frequency=T(minutes=7)),
        lambda D, R, Rr, T: D(name="p/a", enabled=False, retention_rules=[R(T(hours=1), T(days=2))]),
        lambda D, R, Rr, T: D(name="p/b", remote=[Rr(destination="offsite"), Rr(destination="dc2")]),
        lambda D, R, Rr, T: D(name="p/c" * 40),
        lambda D, R, Rr, T: D(name="p/\n\t\x00weird"),
    ], ids=["plain", "space", "non-ascii-name", "rules", "remote", "long", "control-characters-in-name"])
    def test_to_property_output_round_trips(self, make):
        from zfsbackup.config import DatasetConfig, RemoteDatasetConfig, RetentionRule
        cfg = make(DatasetConfig, RetentionRule, RemoteDatasetConfig, lambda **k: timedelta(**k))
        encoded = cfg.to_property()
        assert re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", encoded)
        restored = DatasetConfig.from_property(encoded)
        assert restored.name == cfg.name
        assert restored.recursive == cfg.recursive and restored.enabled == cfg.enabled
        assert restored.frequency == cfg.frequency
        assert restored.to_property() == encoded

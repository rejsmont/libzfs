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
            "    url: http://backup.example.com\n"
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
            "    url: http://backup.example.com\n"
        )
        cfg = BackupConfig.from_file(f)  # must not raise
        assert cfg.datasets[0].remote[0].destination == 'offsite'
        assert cfg.datasets[0].remote[0].retention_rules == []

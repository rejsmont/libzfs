"""Unit tests for the `Duration` value type in zfsbackup.config.

`Duration` subclasses `datetime.timedelta` and remembers (or synthesizes) the
literal it was built from, so config round-trips preserve a user's original
duration text (e.g. "1M" rather than the numerically-equal "30d"). See
docs/config_db_cli_plan.md item 2 for the full design rationale and item 9 for
the authoritative coverage checklist this file implements.

`tests/test_zfsbackup_config.py` and `tests/test_zfsbackup_basic.py` already
cover the `parse_time_duration` backwards-compatibility shim with 19
pre-existing assertions; those files are intentionally left untouched.
"""

import copy
import pickle
import random
from datetime import timedelta

import pytest

from zfsbackup.config import (
    Duration,
    DatasetConfig,
    RetentionRule,
    BackupConfig,
    parse_time_duration,
)


class TestDurationConstructionForms:
    """All three constructor forms from item 9: literal string, existing
    timedelta, and native timedelta kwargs/positional args.
    """

    def test_literal_string_form_parses_and_sets_literal(self):
        d = Duration("1M")
        assert d == timedelta(days=30)
        assert d.literal == "1M"

    def test_timedelta_form_synthesizes_literal(self):
        d = Duration(timedelta(days=30))
        assert d == timedelta(days=30)
        assert d.literal == "1M"

    def test_native_kwargs_form_synthesizes_literal(self):
        d = Duration(days=30)
        assert d == timedelta(days=30)
        assert d.literal == "1M"

    def test_all_three_forms_compare_equal(self):
        by_literal = Duration("1M")
        by_timedelta = Duration(timedelta(days=30))
        by_kwargs = Duration(days=30)
        assert by_literal == by_timedelta == by_kwargs

    def test_positional_days_dispatch_pinned(self):
        # A bare positional int must dispatch as `days`, exactly like
        # timedelta(30), never as a count of seconds. A future refactor
        # that tries to special-case a lone int must fail this test.
        assert Duration(30) == Duration(days=30)
        assert Duration(30) == timedelta(days=30)


class TestDurationCopyConstruction:
    """Copy-constructing from an existing Duration inherits its literal;
    copy-constructing from its plain .timedelta value synthesizes a new one.
    This asymmetry is intentional (item 2 / __new__ docstring).
    """

    def test_copy_from_duration_inherits_literal(self):
        original = Duration("30d")
        assert Duration(original).literal == "30d"

    def test_copy_from_shed_timedelta_synthesizes_literal(self):
        original = Duration("30d")
        # .timedelta explicitly sheds the literal; re-wrapping it must
        # synthesize the canonical form, not recover "30d".
        assert Duration(original.timedelta).literal == "1M"


class TestDurationAccessors:
    def test_literal_returns_source_string(self):
        assert Duration("6d12h").literal == "6d12h"

    def test_literal_is_none_for_subsecond_only_when_no_literal_supplied(self):
        # A literal-string form never yields a None literal; only
        # constructions with sub-second precision and no literal do (see
        # TestDurationSubSecondPrecision).
        assert Duration("1M").literal is not None

    def test_timedelta_accessor_is_exactly_type_timedelta(self):
        d = Duration("1M")
        # isinstance() would pass trivially since Duration IS-A timedelta;
        # the accessor must shed the subclass entirely.
        assert type(d.timedelta) is timedelta
        assert d.timedelta == timedelta(days=30)

    def test_timedelta_accessor_carries_no_literal(self):
        plain = Duration("1M").timedelta
        assert not hasattr(plain, "literal")


class TestDurationStrUnchanged:
    """__str__ is deliberately NOT overridden. Daemon logs at
    backup_manager.py:97,101,208 depend on this exact rendering.
    """

    def test_str_is_plain_timedelta_rendering(self):
        assert str(Duration("1M")) == "30 days, 0:00:00"

    def test_str_matches_bare_timedelta_str(self):
        assert str(Duration("1M")) == str(timedelta(days=30))

    def test_str_does_not_leak_the_literal(self):
        rendered = str(Duration("6d12h"))
        assert "6d12h" not in rendered


class TestDurationPickleAndDeepcopy:
    """Pickle/deepcopy must preserve .literal via __reduce__.

    Uses a NON-canonical literal ("30d") deliberately: timedelta.__reduce__
    reconstructs via (cls, (days, seconds, microseconds)), which the
    native-kwargs constructor now accepts without error, then literal
    synthesis silently fills in the canonical form. A broken __reduce__
    would still produce "1M" for input "1M" -- only a non-canonical literal
    like "30d" actually catches the bug.
    """

    def test_pickle_preserves_noncanonical_literal(self):
        d = Duration("30d")
        restored = pickle.loads(pickle.dumps(d))
        assert restored.literal == "30d"
        assert restored == d

    def test_deepcopy_preserves_noncanonical_literal(self):
        d = Duration("30d")
        restored = copy.deepcopy(d)
        assert restored.literal == "30d"
        assert restored == d

    def test_pickle_preserves_compound_noncanonical_literal(self):
        d = Duration("12h6d")
        restored = pickle.loads(pickle.dumps(d))
        assert restored.literal == "12h6d"

    def test_pickle_roundtrip_type_is_duration(self):
        d = Duration("30d")
        restored = pickle.loads(pickle.dumps(d))
        assert isinstance(restored, Duration)


class TestDurationRoundTripTable:
    """The compound round-trip table from item 9 (values independently
    re-verified, including 1y2M3d = 36979200s).
    """

    TABLE = [
        ("5m", 300),
        ("1h", 3600),
        ("1d", 86400),
        ("1w", 604800),
        ("1M", 2592000),
        ("30d", 2592000),
        ("1y", 31536000),
        ("10y", 315360000),
        ("365d", 31536000),
        ("6d12h", 561600),
        ("1h30m", 5400),
        ("1y2M3d", 36979200),
        ("12h6d", 561600),
        ("-5d", -432000),
    ]

    @pytest.mark.parametrize("literal,seconds", TABLE)
    def test_seconds(self, literal, seconds):
        assert Duration(literal).total_seconds() == seconds

    @pytest.mark.parametrize("literal,seconds", TABLE)
    def test_literal_preserved_not_renormalized(self, literal, seconds):
        # The literal as written must survive unchanged -- "1M" must not
        # become "30d" nor vice versa, and term order ("12h6d") is kept as
        # written rather than being reordered to "6d12h".
        d = Duration(literal)
        assert d.literal == literal
        assert d.render() == literal


class TestDurationUnitCaseSensitivity:
    """`m` (minutes) vs `M` (months) is the collision that defeated every
    third-party duration library evaluated in the plan (pytimeparse and
    humanfriendly both read "1M" as one minute). This must stay a hard,
    case-SENSITIVE distinction. A future refactor that lowercases the unit
    before dispatch must fail loudly here, not just in a diff review.
    """

    def test_lowercase_m_is_minutes(self):
        assert Duration("5m").total_seconds() == 5 * 60

    def test_uppercase_M_is_months(self):
        assert Duration("5M").total_seconds() == 5 * 30 * 86400

    def test_lowercase_m_and_uppercase_M_are_not_interchangeable(self):
        assert Duration("5m") != Duration("5M")

    def test_uppercase_H_is_hours_case_insensitive(self):
        assert Duration("2H").total_seconds() == 2 * 3600

    def test_compound_mixed_case_month_and_minute(self):
        # 1M30m must be 30 days + 30 minutes, not 1 minute + 30 minutes and
        # not 30 days + 30 days.
        d = Duration("1M30m")
        assert d.total_seconds() == 30 * 86400 + 30 * 60


class TestDurationCompoundParsingErrors:
    @pytest.mark.parametrize("bad", ["", "xh", "5z", "M5", "mi5", "mi"])
    def test_invalid_literals_raise_value_error(self, bad):
        with pytest.raises(ValueError):
            Duration(bad)

    @pytest.mark.parametrize("bad", ["1d1d", "1h30m15m"])
    def test_duplicate_units_rejected(self, bad):
        with pytest.raises(ValueError, match="Duplicate"):
            Duration(bad)

    def test_per_term_sign_rejected(self):
        # Sign is whole-duration only; a sign inside a compound term is
        # treated as a typo and rejected, not as "6 days minus 12 hours".
        with pytest.raises(ValueError):
            Duration("6d-12h")


class TestDurationNegativeDurations:
    def test_negative_literal_parses(self):
        assert Duration("-5d").total_seconds() == -432000

    def test_negative_kwargs_synthesizes_signed_literal(self):
        assert Duration(days=-5).literal == "-5d"

    def test_negative_roundtrips(self):
        d = Duration(days=-5)
        assert Duration(d.literal).total_seconds() == d.total_seconds()


class TestDurationSubSecondPrecision:
    def test_construction_is_permitted(self):
        d = Duration(microseconds=1)
        assert d.microseconds == 1

    def test_literal_is_none(self):
        assert Duration(microseconds=1).literal is None

    def test_render_raises_rather_than_truncating(self):
        with pytest.raises(ValueError):
            Duration(microseconds=1).render()


class TestDurationLiteralSynthesis:
    def test_compound_days_hours(self):
        assert Duration(days=6, hours=12).literal == "6d12h"

    def test_three_plus_terms(self):
        assert Duration(days=400).literal == "1y1M5d"

    def test_hours_over_a_day(self):
        assert Duration(hours=36).literal == "1d12h"

    def test_zero(self):
        assert Duration(0).literal == "0s"

    def test_negative(self):
        assert Duration(days=-5).literal == "-5d"


class TestDurationSynthesisExactnessProperty:
    """Synthesis must be exact (never approximate) and deterministic --
    the same value always yields the same literal, since item 12's
    "content byte-identical -> no changes" check depends on it.
    """

    NOTABLE_SECONDS = [
        0,
        1,
        45,                 # sub-minute
        90,
        3600,               # exact hour
        604800,             # exact week boundary
        2592000,            # exact month boundary
        31536000,           # exact year boundary
        34560000,           # 400 days -- needs 3+ terms (1y1M5d)
        -432000,            # negative
    ]

    @pytest.mark.parametrize("seconds", NOTABLE_SECONDS)
    def test_exact_roundtrip(self, seconds):
        original = Duration(seconds=seconds)
        literal = original.literal
        assert literal is not None
        reparsed = Duration(literal)
        assert reparsed.total_seconds() == seconds

    def test_exact_roundtrip_property_over_a_spread_of_values(self):
        rng = random.Random(42)
        for _ in range(200):
            seconds = rng.randint(-50_000_000, 50_000_000)
            literal = Duration(seconds=seconds).literal
            assert Duration(literal).total_seconds() == seconds

    def test_determinism(self):
        rng = random.Random(1234)
        for _ in range(50):
            seconds = rng.randint(0, 100_000_000)
            first = Duration(seconds=seconds).literal
            second = Duration(seconds=seconds).literal
            assert first == second


class TestDurationConsumerSurface:
    """Item 9's consumer-surface rows -- these pin the exact patterns used in
    backup_manager.py and workers.py, so a subclassing regression (e.g.
    switching to a wrapper type) is caught immediately rather than in
    production.
    """

    def test_reflected_comparison_against_bare_timedelta(self):
        # backup_manager.py:205-206 -- `age >= dsi.frequency` where `age`
        # is a bare timedelta and `dsi.frequency` is a Duration.
        assert timedelta(days=40) >= Duration("1M")
        assert not (timedelta(days=20) >= Duration("1M"))

    def test_or_truthiness_with_zero_duration(self):
        # workers.py:146-150 -- `(r.frequency or ds.frequency)`. A zero
        # Duration must remain falsy, exactly like timedelta(0).
        assert bool(Duration(0)) is False
        assert bool(timedelta(0)) is False
        fallback = Duration(0) or Duration("1h")
        assert fallback == Duration("1h")

    def test_or_truthiness_with_nonzero_duration(self):
        primary = Duration("15m")
        fallback = Duration("1h")
        assert (primary or fallback) == Duration("15m")

    def test_equality_against_bare_timedelta(self):
        # Literal is metadata, not identity -- Duration('1M') must equal
        # timedelta(days=30) even though their literals/types differ.
        assert Duration("1M") == timedelta(days=30)

    def test_positional_days_dispatch_against_bare_timedelta(self):
        assert Duration(30) == Duration(days=30)

    def test_hashable_and_hash_matches_equal_timedelta(self):
        assert hash(Duration("1h")) == hash(timedelta(hours=1))

    def test_sorting_uses_inherited_lt(self):
        durations = [Duration("1w"), Duration("1h"), Duration("1d")]
        assert sorted(durations) == [Duration("1h"), Duration("1d"), Duration("1w")]

    def test_arithmetic_result_is_plain_timedelta(self):
        # Duration + timedelta returns a plain timedelta (timedelta.__add__
        # constructs the base class) -- correct and expected, not a bug.
        result = Duration("1h") + timedelta(minutes=30)
        assert type(result) is timedelta
        assert result == timedelta(hours=1, minutes=30)


class TestDurationDataclassDefaults:
    """Item 2: the field(default_factory=...) defaults now build Durations
    directly, so every loaded config carries a literal with no extra step.
    """

    def test_dataset_config_frequency_default_is_duration_with_literal(self):
        cfg = DatasetConfig(name="pool/data")
        assert isinstance(cfg.frequency, Duration)
        assert cfg.frequency.literal == "1h"


class TestDurationWireFormat:
    """Item 12 / item 9: to_property() output must be byte-identical
    whether durations are plain timedeltas or Durations (only
    total_seconds() is serialized). from_property() now synthesizes a
    literal instead of leaving it unrepresentable.
    """

    def test_to_property_byte_identical_for_duration_vs_plain_timedelta(self):
        cfg_plain = DatasetConfig(
            name="pool/data",
            frequency=timedelta(hours=1),
            retention_rules=[RetentionRule(timedelta(days=1), timedelta(days=30))],
        )
        cfg_duration = DatasetConfig(
            name="pool/data",
            frequency=Duration("1h"),
            retention_rules=[RetentionRule(Duration("1d"), Duration("30d"))],
        )
        assert cfg_plain.to_property() == cfg_duration.to_property()

    def test_from_property_yields_duration_with_synthesized_literal(self):
        cfg = DatasetConfig(name="pool/data", frequency=Duration("1h"))
        restored = DatasetConfig.from_property(cfg.to_property())
        assert isinstance(restored.frequency, Duration)
        assert restored.frequency.literal == "1h"

    def test_from_property_synthesizes_canonical_form_not_original_literal(self):
        # The wire format carries no literal, only total_seconds(). A
        # dataset configured with "30d" (2592000s) reconstructs from the
        # remote side as "1M" -- deliberate and documented, not a bug.
        cfg = DatasetConfig(name="pool/data", frequency=Duration("30d"))
        restored = DatasetConfig.from_property(cfg.to_property())
        assert restored.frequency.literal == "1M"
        assert restored.frequency == cfg.frequency

    def test_from_property_retention_rules_render_without_error(self):
        cfg = DatasetConfig(
            name="pool/data",
            retention_rules=[RetentionRule(Duration("1d"), Duration("1M"))],
        )
        restored = DatasetConfig.from_property(cfg.to_property())
        rule = restored.retention_rules[0]
        assert rule.age.render() == "1d"
        assert rule.keep_for.render() == "1M"


class TestParseTimeDurationShimTypeExactness:
    """The 22 pre-existing parse_time_duration assertions (in
    test_zfsbackup_config.py and test_zfsbackup_basic.py) are equality
    checks against timedelta, which Duration would also satisfy via
    isinstance/eq. These tests pin the exact return type so the shim
    contract ("still returns a plain timedelta of exactly that type") is
    genuinely verified, not merely coincidental.
    """

    def test_shim_returns_exactly_timedelta_not_duration(self):
        result = parse_time_duration("1M")
        assert type(result) is timedelta
        assert not isinstance(result, Duration)

    def test_shim_return_value_has_no_literal_attribute(self):
        result = parse_time_duration("6d12h")
        assert not hasattr(result, "literal")


class TestDurationYamlBoundaryGuardRegression:
    """Regression coverage for the fixed `_duration_from_config` boundary guard.

    Every internal parse site used to call `Duration(x)` directly with `x`
    straight off `yaml.safe_load`. Because `Duration`'s constructor accepts
    native-kwargs numeric forms (`Duration(30) == Duration(days=30)`), an
    untyped YAML scalar (`check_interval: 300`) fell through to that branch
    and was silently interpreted as *days* -- a user meaning 300 seconds got
    a daemon that snapshots roughly once a year. The old `parse_time_duration`
    raised `TypeError` on the same input, so this was a regression from
    "fails loudly" to "runs, but wrong". These tests pin the fix: every
    parse site must reject a non-`str` scalar and name the offending key.
    """

    # --- DatasetConfig.from_dict sites -------------------------------------

    def test_dataset_frequency_int_rejected_and_key_named(self):
        with pytest.raises(ValueError, match=r"datasets\[tank/d\]\.frequency"):
            DatasetConfig.from_dict({"name": "tank/d", "frequency": 2})

    def test_dataset_frequency_bool_rejected(self):
        # bool is a subclass of int in Python -- `frequency: true` must not
        # slip through an `isinstance(value, int)`-style guard.
        with pytest.raises(ValueError, match=r"datasets\[tank/d\]\.frequency"):
            DatasetConfig.from_dict({"name": "tank/d", "frequency": True})

    def test_dataset_frequency_bool_message_names_bool_type(self):
        with pytest.raises(ValueError, match="bool"):
            DatasetConfig.from_dict({"name": "tank/d", "frequency": True})

    def test_dataset_frequency_yaml_sexagesimal_int_rejected(self):
        # YAML 1.1 resolves an unquoted "1:30" scalar to the int 90 (sexagesimal
        # minutes:seconds). A user who intended "1 hour 30 minutes" and wrote
        # it unquoted must get a clear type error, not "90 days".
        with pytest.raises(ValueError, match=r"datasets\[tank/d\]\.frequency"):
            DatasetConfig.from_dict({"name": "tank/d", "frequency": 90})

    def test_dataset_frequency_quoted_numeric_string_reaches_grammar(self):
        # The near-miss a user hits right after seeing the new guard error:
        # quoting the number. It must still fail, but at the Duration
        # grammar (Invalid duration format), not at the type guard.
        with pytest.raises(ValueError, match="Invalid duration format"):
            DatasetConfig.from_dict({"name": "tank/d", "frequency": "300"})

    def test_dataset_frequency_valid_string_loads_with_literal(self):
        cfg = DatasetConfig.from_dict({"name": "tank/d", "frequency": "1h"})
        assert isinstance(cfg.frequency, Duration)
        assert cfg.frequency.literal == "1h"

    def test_retention_bad_key_rejected_and_key_named(self):
        # {1: "30d"} -- the retention *age* is an untyped int. This is the
        # important case: "1 day -> 30 days" looks entirely plausible in a
        # log, which is exactly what hides this class of bug.
        with pytest.raises(ValueError, match=r"datasets\[tank/d\]\.retention"):
            DatasetConfig.from_dict({"name": "tank/d", "retention": {1: "30d"}})

    def test_retention_bad_value_rejected_and_key_named(self):
        # {"1d": 30} -- the retention *keep_for* is an untyped int.
        with pytest.raises(ValueError, match=r"datasets\[tank/d\]\.retention"):
            DatasetConfig.from_dict({"name": "tank/d", "retention": {"1d": 30}})

    def test_remote_frequency_int_rejected_and_key_named(self):
        with pytest.raises(ValueError, match=r"datasets\[tank/d\]\.remote\[foo\]\.frequency"):
            DatasetConfig.from_dict(
                {
                    "name": "tank/d",
                    "remote": [{"destination": "foo", "frequency": 5}],
                }
            )

    # --- BackupConfig.from_file sites (real YAML, not a hand-built dict) ---

    def test_check_interval_int_rejected_and_key_named(self, tmp_path):
        f = tmp_path / "cfg.yaml"
        f.write_text("check_interval: 300\ndatasets:\n  - name: pool/data\n")
        with pytest.raises(ValueError, match="check_interval"):
            BackupConfig.from_file(f)

    def test_check_interval_int_message_names_int_type(self, tmp_path):
        f = tmp_path / "cfg.yaml"
        f.write_text("check_interval: 300\ndatasets:\n  - name: pool/data\n")
        with pytest.raises(ValueError, match="int"):
            BackupConfig.from_file(f)

    def test_prune_interval_int_rejected_and_key_named(self, tmp_path):
        f = tmp_path / "cfg.yaml"
        f.write_text(
            "check_interval: 5m\nprune_interval: 300\ndatasets:\n  - name: pool/data\n"
        )
        with pytest.raises(ValueError, match="prune_interval"):
            BackupConfig.from_file(f)

    def test_dataset_frequency_unquoted_sexagesimal_yaml_rejected(self, tmp_path):
        # The real end-to-end YAML case: an unquoted "1:30" under a real
        # config file, not a hand-built dict -- confirms yaml.safe_load's
        # sexagesimal resolution actually reaches the guard in from_file.
        f = tmp_path / "cfg.yaml"
        f.write_text("datasets:\n  - name: pool/data\n    frequency: 1:30\n")
        with pytest.raises(ValueError, match=r"datasets\[pool/data\]\.frequency"):
            BackupConfig.from_file(f)

    def test_dataset_frequency_true_via_yaml_rejected(self, tmp_path):
        f = tmp_path / "cfg.yaml"
        f.write_text("datasets:\n  - name: pool/data\n    frequency: true\n")
        with pytest.raises(ValueError, match=r"datasets\[pool/data\]\.frequency"):
            BackupConfig.from_file(f)

    def test_dataset_frequency_valid_unquoted_string_via_yaml_loads(self, tmp_path):
        f = tmp_path / "cfg.yaml"
        f.write_text("datasets:\n  - name: pool/data\n    frequency: 1h\n")
        cfg = BackupConfig.from_file(f)
        assert isinstance(cfg.datasets[0].frequency, Duration)
        assert cfg.datasets[0].frequency.literal == "1h"


class TestConfigProducesDurationsWithLiterals:
    """End-to-end coverage for the motivating assertion of item 2: a user
    who writes "30d" gets "30d" back, not the numerically-equal "1M" that
    literal synthesis would produce if the parsed literal were lost. Uses
    non-canonical literals throughout (30d / 365d rather than 1M / 1y) for
    the same reason the pickle tests do: a canonical literal would survive
    synthesis and pass even if the real literal were being dropped.
    """

    @pytest.fixture
    def yaml_config(self, tmp_path):
        f = tmp_path / "cfg.yaml"
        f.write_text(
            "check_interval: 30d\n"
            "prune_interval: 365d\n"
            "datasets:\n"
            "  - name: pool/data\n"
            "    frequency: 30d\n"
            "    retention:\n"
            "      30d: 365d\n"
            "    remote:\n"
            "      - destination: offsite\n"
            "        frequency: 30d\n"
        )
        return BackupConfig.from_file(f)

    def test_check_interval_is_duration_with_literal(self, yaml_config):
        assert isinstance(yaml_config.check_interval, Duration)
        assert yaml_config.check_interval.literal == "30d"

    def test_prune_interval_is_duration_with_literal(self, yaml_config):
        assert isinstance(yaml_config.prune_interval, Duration)
        assert yaml_config.prune_interval.literal == "365d"

    def test_dataset_frequency_is_duration_with_literal(self, yaml_config):
        ds = yaml_config.datasets[0]
        assert isinstance(ds.frequency, Duration)
        assert ds.frequency.literal == "30d"

    def test_retention_age_is_duration_with_literal(self, yaml_config):
        rule = yaml_config.datasets[0].retention_rules[0]
        assert isinstance(rule.age, Duration)
        assert rule.age.literal == "30d"

    def test_retention_keep_for_is_duration_with_literal(self, yaml_config):
        rule = yaml_config.datasets[0].retention_rules[0]
        assert isinstance(rule.keep_for, Duration)
        assert rule.keep_for.literal == "365d"

    def test_remote_frequency_is_duration_with_literal(self, yaml_config):
        remote = yaml_config.datasets[0].remote[0]
        assert isinstance(remote.frequency, Duration)
        assert remote.frequency.literal == "30d"


class TestBackupConfigDurationDefaults:
    """The BackupConfig-level defaults the reviewer flagged as untested:
    check_interval/prune_interval defaults, and prune_interval falling back
    to check_interval when only the latter is set (config.py:214/386).
    """

    def test_check_interval_default_is_duration_5m(self):
        cfg = BackupConfig(datasets=[DatasetConfig(name="pool/data")])
        assert isinstance(cfg.check_interval, Duration)
        assert cfg.check_interval.literal == "5m"

    def test_prune_interval_default_is_duration_1h(self):
        cfg = BackupConfig(datasets=[DatasetConfig(name="pool/data")])
        assert isinstance(cfg.prune_interval, Duration)
        assert cfg.prune_interval.literal == "1h"

    def test_from_file_prune_interval_falls_back_to_check_interval(self, tmp_path):
        f = tmp_path / "cfg.yaml"
        f.write_text("check_interval: 30m\ndatasets:\n  - name: pool/data\n")
        cfg = BackupConfig.from_file(f)
        assert isinstance(cfg.prune_interval, Duration)
        assert cfg.prune_interval.literal == "30m"
        assert cfg.prune_interval == cfg.check_interval

    def test_from_file_prune_interval_independent_when_both_set(self, tmp_path):
        f = tmp_path / "cfg.yaml"
        f.write_text(
            "check_interval: 30m\nprune_interval: 2h\ndatasets:\n  - name: pool/data\n"
        )
        cfg = BackupConfig.from_file(f)
        assert cfg.check_interval.literal == "30m"
        assert cfg.prune_interval.literal == "2h"

"""Milestone D, plan B-3-lite: negotiate says so when it shortens a dataset's retention.

The server applies the retention the client sends, verbatim (that is the design: the client
decides how long its own history lives). A typo on the client -- `30d` becoming `7d` -- would make
the next pruning cycle on the server destroy history, so negotiate compares the longest `keep_for`
(the horizon) of the config now stored with the one it is about to store and logs ONE WARNING when
it shrinks. It never refuses and never raises: an undecodable or unreadable stored config is a
DEBUG line and the negotiate goes on.

Layout: `TestRetentionHorizon`, `TestFormatHorizon`, `TestRetentionShrink` (the pure functions),
`TestNegotiateWarns` (the table through the route), `TestNeverRefuses`.
"""

import logging
from datetime import timedelta

import pytest

from _api_support import Authed, pending_configs
from test_zfsbackup_api_negotiate import (  # noqa: F401  (fixtures and helpers)
    CLIENT_ID, SERVER_DS, _config, assert_nothing_written, authed, client, existing, negotiate, zfs,
)
from zfsbackup.api import canonical_dataset_config, format_horizon, retention_horizon, retention_shrink
from zfsbackup.backup_manager import PROP_CONFIG
from zfsbackup.config import DatasetConfig, RetentionRule


def config_of(*keep_days, keep_hours=()):
    """A dataset config whose rules keep for these many days (and hours): one rule per `keep_for`."""
    rules = [RetentionRule(timedelta(hours=1) * (i + 1), timedelta(days=d)) for i, d in enumerate(keep_days)]
    rules += [RetentionRule(timedelta(minutes=10) * (i + 1), timedelta(hours=h)) for i, h in enumerate(keep_hours)]
    return DatasetConfig(name="pool/data", retention_rules=rules)


def wire(config) -> str:
    """The canonical value negotiate stores for `config`."""
    return canonical_dataset_config(config.to_property())


# ---------------------------------------------------------------------------
# The pure functions
# ---------------------------------------------------------------------------


class TestRetentionHorizon:
    def test_no_rules_is_forever(self):
        assert retention_horizon(config_of()) is None

    def test_one_rule_is_its_keep_for(self):
        assert retention_horizon(config_of(30)) == timedelta(days=30)

    def test_the_longest_keep_for_wins_in_any_order(self):
        assert retention_horizon(config_of(7, 90, 30)) == timedelta(days=90)
        assert retention_horizon(config_of(90, 7)) == timedelta(days=90)

    def test_hours_and_days_compare_by_duration(self):
        assert retention_horizon(config_of(1, keep_hours=(47,))) == timedelta(hours=47)
        assert retention_horizon(config_of(2, keep_hours=(47,))) == timedelta(days=2)


class TestFormatHorizon:
    @pytest.mark.parametrize("horizon, text", [
        (None, "forever"),
        (timedelta(days=30), "30d"),
        (timedelta(days=7), "7d"),
        (timedelta(days=1), "1d"),
        (timedelta(hours=36), "36h"),
        (timedelta(hours=24), "1d"),
        (timedelta(hours=1), "1h"),
        (timedelta(minutes=90), "90m"),
        (timedelta(minutes=2), "2m"),
        (timedelta(minutes=1), "1m"),
        (timedelta(seconds=90), "90s"),
        (timedelta(seconds=61), "61s"),
        (timedelta(seconds=59), "59s"),
        (timedelta(seconds=1), "1s"),
        (timedelta(milliseconds=500), "0.5s"),
        (timedelta(days=1, seconds=1), "86401s"),
        (timedelta(days=365), "365d"),
        (timedelta(0), "0s"),
        # a fractional horizon keeps its full decimal value in seconds, never an exponent
        (timedelta(seconds=100000.25), "100000.25s"),
        (timedelta(seconds=1000010.5), "1000010.5s"),
        (timedelta(seconds=1, microseconds=1), "1.000001s"),
        (timedelta(microseconds=1), "0.000001s"),
        (timedelta(days=400, microseconds=500000), "34560000.5s"),
        # a negative horizon (not a valid retention, but it must not misprint)
        (timedelta(seconds=-5), "-5s"),
        (timedelta(days=-2), "-2d"),
        (timedelta(seconds=-0.5), "-0.5s"),
    ])
    def test_the_largest_unit_that_divides_exactly(self, horizon, text):
        assert format_horizon(horizon) == text


class TestRetentionShrink:
    @pytest.mark.parametrize("old, new, expected", [
        ((30,), (7,), (timedelta(days=30), timedelta(days=7))),            # shrinks
        ((7,), (30,), None),                                                # grows
        ((30,), (30,), None),                                               # equal
        ((), (7,), (None, timedelta(days=7))),                              # forever -> finite
        ((7,), (), None),                                                   # finite -> forever
        ((), (), None),                                                     # forever -> forever
        ((7, 90), (30,), (timedelta(days=90), timedelta(days=30))),         # the horizon is the longest
        ((30,), (7, 30), None),                                             # a short tier added: same horizon
        ((7, 30), (30,), None),                                             # a short tier dropped: same horizon
        ((30,), (29,), (timedelta(days=30), timedelta(days=29))),           # by a day
    ], ids=["shrink", "grow", "equal", "forever-to-finite", "finite-to-forever", "forever-forever",
            "longest-tier-shortened", "tier-added", "tier-dropped", "one-day"])
    def test_the_table(self, old, new, expected):
        assert retention_shrink(wire(config_of(*old)), wire(config_of(*new))) == expected

    def test_only_the_retention_matters(self):
        """A faster schedule or a renamed dataset is no shrink."""
        old = config_of(30)
        new = DatasetConfig(name="pool/other", frequency=timedelta(minutes=5), retention_rules=config_of(30).retention_rules)
        assert retention_shrink(wire(old), wire(new)) is None

    @pytest.mark.parametrize("stored", ["", "not base64 at all!", "bm90IGpzb24=", "e30=", "W10="],
                             ids=["empty", "garbage", "not-json", "empty-object", "list"])
    def test_an_undecodable_stored_value_is_a_value_error(self, stored):
        with pytest.raises(ValueError, match="stored config does not decode"):
            retention_shrink(stored, wire(config_of(7)))

    def test_the_error_does_not_carry_the_stored_text(self):
        with pytest.raises(ValueError) as caught:
            retention_shrink("SECRET-NOT-BASE64!", wire(config_of(7)))
        assert "SECRET" not in str(caught.value)
        assert caught.value.__cause__ is None                    # `from None`: no chained stored text


# ---------------------------------------------------------------------------
# Through the route
# ---------------------------------------------------------------------------


def stored(zfs, config=None, *, raw=None):
    """The dataset exists and holds this config as its local `org.zfsbackup:config`."""
    existing(zfs, SERVER_DS)
    zfs.answers.values[PROP_CONFIG] = raw if raw is not None else (wire(config) if config is not None else None)


def sent(client, config, **kwargs):
    return negotiate(client, config=config.to_property(), **kwargs)


def shrink_warnings(caplog):
    return [r.getMessage() for r in caplog.records
            if r.name == "zfsbackup.api" and r.levelno == logging.WARNING and "retention horizon" in r.getMessage()]


def debug_notes(caplog):
    return [r.getMessage() for r in caplog.records
            if r.name == "zfsbackup.api" and r.levelno == logging.DEBUG and "retention" in r.getMessage()]


class TestNegotiateWarns:
    @pytest.mark.parametrize("old, new, warns, text", [
        ((30,), (7,), True, "shrinks from 30d to 7d"),
        ((7,), (30,), False, None),
        ((30,), (30,), False, None),
        ((), (7,), True, "shrinks from forever to 7d"),
        ((7,), (), False, None),
        ((), (), False, None),
        ((90, 30), (7,), True, "shrinks from 90d to 7d"),
        ((30,), (7, 30), False, None),
    ], ids=["shrink", "grow", "equal", "forever-to-finite", "finite-to-forever", "forever", "longest-tier",
            "tier-added"])
    def test_the_table_through_negotiate(self, client, zfs, caplog, old, new, warns, text):
        stored(zfs, config_of(*old))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = sent(client, config_of(*new))
        assert resp.status_code == 200
        found = shrink_warnings(caplog)
        assert len(found) == (1 if warns else 0), found
        if warns:
            assert text in found[0]

    def test_the_warning_names_the_client_the_client_dataset_and_the_server_dataset(self, client, zfs, caplog):
        stored(zfs, config_of(30))
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            sent(client, config_of(7))
        (warning,) = shrink_warnings(caplog)
        assert CLIENT_ID in warning and "'pool/data'" in warning
        assert f"the retention horizon of {SERVER_DS} shrinks from 30d to 7d" in warning
        assert "older than 7d are pruned from the next pruning cycle" in warning

    def test_it_is_a_warning_not_an_error(self, client, zfs, caplog):
        stored(zfs, config_of(30))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            sent(client, config_of(7))
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_the_comparison_is_against_the_stored_config_so_a_repeat_is_silent(self, client, zfs, caplog):
        stored(zfs, config_of(30))
        zfs["set"].side_effect = lambda ds, props, *a, **k: zfs.answers.values.update(
            {PROP_CONFIG: props[PROP_CONFIG]})
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            sent(client, config_of(7))
            sent(client, config_of(7))
            sent(client, config_of(7))
        assert len(shrink_warnings(caplog)) == 1                # the shrink happened once; then 7d is the state

    def test_a_second_shrink_warns_again(self, client, zfs, caplog):
        stored(zfs, config_of(30))
        zfs["set"].side_effect = lambda ds, props, *a, **k: zfs.answers.values.update(
            {PROP_CONFIG: props[PROP_CONFIG]})
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            sent(client, config_of(7))
            sent(client, config_of(30))                          # back up: silent
            sent(client, config_of(7))                           # down again: a new warning
        assert len(shrink_warnings(caplog)) == 2

    def test_the_stored_config_is_read_locally_from_the_server_dataset(self, client, zfs):
        stored(zfs, config_of(30))
        sent(client, config_of(7))
        (call,) = [c for c in zfs["get_value"].call_args_list if c.args[1] == PROP_CONFIG]
        assert str(call.args[0]) == SERVER_DS and call.kwargs == {"sources": "local"}

    def test_an_unset_stored_config_has_nothing_to_compare(self, client, zfs, caplog):
        stored(zfs, None)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            assert sent(client, config_of(7)).status_code == 200
        assert shrink_warnings(caplog) == [] and debug_notes(caplog) == []

    def test_a_missing_dataset_is_not_read_at_all(self, client, zfs, caplog):
        existing(zfs)                                           # nothing exists
        zfs.answers.values[PROP_CONFIG] = wire(config_of(30))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            assert sent(client, config_of(7)).status_code == 200
        assert shrink_warnings(caplog) == []
        assert not [c for c in zfs["get_value"].call_args_list if c.args[1] == PROP_CONFIG]

    def test_a_dry_run_compares_nothing(self, zfs, caplog):
        dry = Authed(_config(dry_run=True))
        stored(zfs, config_of(30))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            assert sent(dry.client(), config_of(7)).status_code == 200
        assert shrink_warnings(caplog) == []
        assert not [c for c in zfs["get_value"].call_args_list if c.args[1] == PROP_CONFIG]

    def test_the_comparison_precedes_the_write(self, client, zfs):
        """The old value is read before `zfs set` overwrites it."""
        stored(zfs, config_of(30))
        order = []
        real = zfs["get_value"].side_effect
        zfs["get_value"].side_effect = lambda ds, prop, **kw: (order.append(("get", prop)), real(ds, prop, **kw))[1]
        zfs["set"].side_effect = lambda *a, **k: order.append(("set",))
        sent(client, config_of(7))
        assert order.index(("get", PROP_CONFIG)) < order.index(("set",))


class TestNeverRefuses:
    @pytest.mark.parametrize("raw", ["not base64 at all!", "bm90IGpzb24=", "e30=", "SECRET-VALUE-XYZ"],
                             ids=["garbage", "not-json", "empty-object", "secret"])
    def test_an_undecodable_stored_config_is_a_debug_line_and_the_negotiate_goes_on(
        self, client, zfs, caplog, raw
    ):
        stored(zfs, raw=raw)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = sent(client, config_of(7))
        assert resp.status_code == 200
        assert shrink_warnings(caplog) == []
        assert not [r for r in caplog.records if r.name == "zfsbackup.api" and r.levelno >= logging.WARNING]
        (note,) = debug_notes(caplog)
        assert "not comparing the retention" in note and SERVER_DS in note
        assert "SECRET-VALUE-XYZ" not in caplog.text
        (set_call,) = zfs["set"].call_args_list                  # the new config was stored all the same
        assert set_call.args[1][PROP_CONFIG] == wire(config_of(7))

    def test_an_unreadable_stored_config_is_a_debug_line_and_the_negotiate_goes_on(self, client, zfs, caplog):
        stored(zfs, config_of(30))
        zfs.answers.values[PROP_CONFIG] = RuntimeError("cannot open: permission denied for tank/secret")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = sent(client, config_of(7))
        assert resp.status_code == 200
        assert shrink_warnings(caplog) == []
        assert not [r for r in caplog.records if r.name == "zfsbackup.api" and r.levelno >= logging.WARNING]
        (note,) = debug_notes(caplog)
        assert "not comparing the retention" in note
        zfs["set"].assert_called_once()

    def test_a_shrink_is_applied_not_refused(self, client, authed, zfs):
        stored(zfs, config_of(30))
        resp = sent(client, config_of(7))
        assert resp.status_code == 200
        (call,) = zfs["set"].call_args_list
        assert call.args[1][PROP_CONFIG] == wire(config_of(7))
        assert resp.get_json()["server_dataset"] == SERVER_DS

    def test_a_forever_to_finite_change_is_applied_not_refused(self, client, zfs):
        stored(zfs, config_of())
        assert sent(client, config_of(1)).status_code == 200
        zfs["set"].assert_called_once()

    def test_the_comparison_never_changes_what_a_refused_request_does(self, client, zfs, caplog):
        """A 400 is decided before anything is read or written; no comparison is logged for it."""
        stored(zfs, config_of(30))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = negotiate(client, config="")
        assert resp.status_code == 400
        assert shrink_warnings(caplog) == [] and debug_notes(caplog) == []
        assert_nothing_written(zfs)

    def test_a_pending_dataset_has_nothing_stored_to_shrink(self, client, authed, zfs, caplog):
        """The first negotiate of a missing dataset, then a second with a shorter horizon: the
        second still compares against nothing on disk (the entry is in memory), and just replaces it."""
        existing(zfs)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            assert sent(client, config_of(30)).status_code == 200
            assert sent(client, config_of(7)).status_code == 200
        assert shrink_warnings(caplog) == []
        assert pending_configs(authed.app)[SERVER_DS][0] == wire(config_of(7))

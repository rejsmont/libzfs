"""`POST /backup/<id>/<dataset>/negotiate` and the client dataset name: a client cannot put its own
bytes into the server's properties (production readiness plan B-1a).

A client's negotiate body names the server dataset's `org.zfsbackup:config`, which the SERVER later
reads back as retention instructions. The route therefore

* requires a JSON object, `snapshots` as a list of strings and a non-empty `config` that
  `DatasetConfig.from_property` decodes into the right types (`canonical_dataset_config`);
* stores only `to_property()` of the DECODED config -- the client's own string never reaches `zfs`
  -- and always (an empty config is refused, so a copy that arrives in a stream is never the one in
  effect);
* decides every 400 before any write;
* answers 400 `BAD_PROPERTY_ERROR` when libzfseasy refuses a value (a `ValueError`), 500 for any
  other `zfs` failure, with the details only in the log;
* refuses client dataset names with `\\n \\r \\t \\0` or that are not a valid filesystem name (the
  pool component included) with 400 on negotiate and `resume_token`, and an error message on the
  stream websocket (`client_dataset_problem`).

Layout: `TestBodyShape`, `TestConfigShape`, `TestCanonicalDatasetConfig` (the function),
`TestStoredValue`, `TestZfsFailures`, `TestDryRun`, `TestClientDatasetName`.
"""

import base64
import json
import logging
import re
from datetime import timedelta

import pytest

from _api_support import CLIENT_ID, Authed
from test_zfsbackup_api import STREAM_PATH, Stream  # noqa: F401  (Stream: a harness, not a test)
from zfsbackup.api import (
    BAD_CONFIG_ERROR, BAD_PROPERTY_ERROR, RECEIVE_EXCLUDED_PROPERTIES, canonical_dataset_config,
    client_dataset_problem,
)
from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig, RetentionRule

URL = f"/backup/{CLIENT_ID}/pool/data/negotiate"
SERVER_DS = "pool/backups/client1/data"
SHAPE_ERROR = "request body must be a JSON object"
SNAPSHOTS_ERROR = "snapshots: must be a list of snapshot names"

#: What `to_property()` decodes from, as a dict (the wire shape of a good config).
GOOD = {
    "name": "pool/data", "recursive": False, "frequency": 3600.0, "enabled": True,
    "retention_rules": [{"age": 86400.0, "keep_for": 2592000.0}],
    "remote": [{"destination": "offsite", "frequency": None}],
}


def enc(obj) -> str:
    """The client's `config` value for the JSON object (or raw text) `obj`."""
    text = obj if isinstance(obj, str) else json.dumps(obj)
    return base64.b64encode(text.encode()).decode()


def good(**changes) -> dict:
    return {**GOOD, **changes}


def _config(**kwargs) -> BackupConfig:
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data")],
        remote_backup=RemoteServerConfig(target_dataset="pool/backups", enabled=True),
        **kwargs,
    )


@pytest.fixture(autouse=True)
def zfs(mocker):
    mocks = {n: mocker.patch(f"zfsbackup.api.zfs.{n}") for n in ("exists", "list", "create", "set")}
    mocks["exists"].return_value = False
    mocks["list"].return_value = []
    mocks["props"] = mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
    return mocks


@pytest.fixture
def authed():
    return Authed(_config())


@pytest.fixture
def client(authed):
    return authed.client()


def post_json(client, body, url=URL):
    return client.post(url, json=body)


def post_raw(client, raw, url=URL, content_type="application/json"):
    kwargs = {} if content_type is None else {"content_type": content_type}
    return client.post(url, data=raw, **kwargs)


def body_with(**changes) -> dict:
    return {"snapshots": [], "config": enc(GOOD), **changes}


def assert_nothing_written(zfs):
    for name in ("create", "set"):
        assert not zfs[name].called, f"zfs.{name} was called by a refused request"


# ---------------------------------------------------------------------------
# The body
# ---------------------------------------------------------------------------


class TestBodyShape:
    @pytest.mark.parametrize("raw", [
        b"[]", b'"text"', b"42", b"null", b"true", b"1.5",
        b"{", b"", b"not json", b"\xff\xfe\xfd", b'{"snapshots": [], "snapshots"',
        b"[" * 100_000 + b"]" * 100_000,
    ], ids=["list", "string", "int", "null", "bool", "float", "open-brace", "empty", "text",
            "invalid-utf8", "truncated", "deeply-nested"])
    def test_a_body_that_is_not_a_json_object_is_400(self, client, authed, zfs, raw):
        resp = post_raw(client, raw)
        assert resp.status_code == 400
        assert resp.get_json() == {"error": SHAPE_ERROR}
        assert_nothing_written(zfs)
        assert authed.gate.active == 0

    def test_the_content_type_is_not_required(self, client, zfs):
        for content_type in (None, "text/plain", "application/x-www-form-urlencoded"):
            resp = post_raw(client, json.dumps(body_with()).encode(), content_type=content_type)
            assert resp.status_code == 200, content_type

    @pytest.mark.parametrize("snapshots", [
        pytest.param(None, id="null"), pytest.param("a", id="string"), pytest.param({}, id="dict"),
        pytest.param({"a": 1}, id="dict-items"), pytest.param(5, id="int"),
        pytest.param(True, id="bool"), pytest.param([1], id="int-item"),
        pytest.param(["a", None], id="null-item"), pytest.param([["a"]], id="nested"),
        pytest.param([True], id="bool-item"), pytest.param([{"a": 1}], id="dict-item"),
        pytest.param(["a", 2.5], id="float-item"),
    ])
    def test_snapshots_must_be_a_list_of_strings(self, client, authed, zfs, snapshots):
        resp = post_json(client, body_with(snapshots=snapshots))
        assert resp.status_code == 400
        assert resp.get_json() == {"error": SNAPSHOTS_ERROR}
        assert_nothing_written(zfs)
        assert authed.gate.active == 0

    def test_snapshots_missing_is_400(self, client, zfs):
        resp = post_json(client, {"config": enc(GOOD)})
        assert resp.status_code == 400 and resp.get_json() == {"error": SNAPSHOTS_ERROR}
        assert_nothing_written(zfs)

    @pytest.mark.parametrize("snapshots", [[], ["a"], ["autosnap_20240101120000", "autosnap_20240102120000"]])
    def test_a_list_of_strings_is_accepted(self, client, zfs, snapshots):
        assert post_json(client, body_with(snapshots=snapshots)).status_code == 200

    def test_extra_keys_are_ignored(self, client, zfs):
        assert post_json(client, body_with(extra="x", anchor={"a": 1})).status_code == 200

    def test_a_refusal_makes_no_zfs_call_at_all(self, client, zfs):
        """Validation comes before the server dataset is even looked at."""
        post_json(client, {"snapshots": "x", "config": enc(GOOD)})
        post_json(client, {"snapshots": [], "config": "!!"})
        for name, mock in zfs.items():
            assert not mock.called, f"zfs access {name} before validation"


class TestConfigShape:
    @pytest.mark.parametrize("config", [
        pytest.param(None, id="null"), pytest.param("", id="empty"), pytest.param(0, id="zero"),
        pytest.param(12, id="int"), pytest.param(1.5, id="float"), pytest.param(True, id="bool"),
        pytest.param([], id="empty-list"), pytest.param(["x"], id="list"),
        pytest.param({}, id="empty-dict"), pytest.param({"name": "pool/data"}, id="dict"),
    ])
    def test_config_must_be_a_non_empty_string(self, client, authed, zfs, config):
        resp = post_json(client, body_with(config=config))
        assert resp.status_code == 400
        assert resp.get_json() == {"error": BAD_CONFIG_ERROR}
        assert_nothing_written(zfs)
        assert authed.gate.active == 0

    def test_config_missing_is_400(self, client, zfs):
        resp = post_json(client, {"snapshots": []})
        assert resp.status_code == 400 and resp.get_json() == {"error": BAD_CONFIG_ERROR}
        assert_nothing_written(zfs)

    #: (id, the value of `config`). Every one decodes to something the pruner must never see.
    BAD_CONFIGS = [
        ("not-base64", "this is not base64!!"),
        ("newline-in-base64", enc(GOOD)[:20] + "\n" + enc(GOOD)[20:]),
        ("space-in-base64", enc(GOOD)[:20] + " " + enc(GOOD)[20:]),
        ("unpadded", enc(GOOD).rstrip("=") + "x"),
        ("non-ascii", "é" + enc(GOOD)),
        ("urlsafe-alphabet", base64.urlsafe_b64encode(b"\xfb\xff\xfe" * 20).decode()),
        ("not-utf8", base64.b64encode(b"\xff\xfe\xfd").decode()),
        ("not-json", enc("{not json")),
        ("json-list", enc("[1, 2, 3]")),
        ("json-string", enc('"text"')),
        ("json-null", enc("null")),
        ("json-int", enc("5")),
        ("deeply-nested", enc("[" * 100_000 + "]" * 100_000)),
        ("nan-frequency", enc('{"name": "a", "frequency": NaN}')),
        ("infinity-frequency", enc('{"name": "a", "frequency": Infinity}')),
        ("minus-infinity", enc('{"name": "a", "frequency": -Infinity}')),
        ("1e300-frequency", enc(good(frequency=1e300))),
        ("1e300-age", enc(good(retention_rules=[{"age": 1e300, "keep_for": 1}]))),
        ("1e300-keep-for", enc(good(retention_rules=[{"age": 1, "keep_for": 1e300}]))),
        ("1e300-remote-frequency", enc(good(remote=[{"destination": "x", "frequency": 1e300}]))),
        ("huge-integer", enc('{"name": "a", "frequency": ' + "9" * 400 + "}")),
        ("1e999", enc('{"name": "a", "frequency": 1e999}')),
        # plan C: a duration must be strictly positive. Zero is the regression case: an `age` of 0
        # is a zero-width slot, a division by zero in `needs_prunning` on every server pruning cycle.
        ("zero-frequency", enc(good(frequency=0))),
        ("zero-frequency-float", enc(good(frequency=0.0))),
        ("negative-zero-frequency", enc(good(frequency=-0.0))),
        ("sub-microsecond-frequency", enc(good(frequency=1e-9))),
        ("zero-age", enc(good(retention_rules=[{"age": 0, "keep_for": 5}]))),
        ("zero-keep-for", enc(good(retention_rules=[{"age": 5, "keep_for": 0}]))),
        ("zero-age-in-a-later-rule", enc(good(retention_rules=[
            {"age": 60, "keep_for": 60}, {"age": 0, "keep_for": 60}]))),
        ("negative-frequency", enc(good(frequency=-1))),
        ("negative-age", enc(good(retention_rules=[{"age": -1, "keep_for": 5}]))),
        ("negative-keep-for", enc(good(retention_rules=[{"age": 5, "keep_for": -1}]))),
        ("negative-remote-frequency", enc(good(remote=[{"destination": "x", "frequency": -5}]))),
        ("recursive-yes", enc(good(recursive="yes"))),
        ("recursive-one", enc(good(recursive=1))),
        ("recursive-null", enc(good(recursive=None))),
        ("enabled-no", enc(good(enabled="no"))),
        ("enabled-null", enc(good(enabled=None))),
        ("name-int", enc(good(name=5))),
        ("name-null", enc(good(name=None))),
        ("name-list", enc(good(name=["pool/data"]))),
        ("name-missing", enc({k: v for k, v in GOOD.items() if k != "name"})),
        ("frequency-missing", enc({k: v for k, v in GOOD.items() if k != "frequency"})),
        ("frequency-string", enc(good(frequency="1h"))),
        ("frequency-null", enc(good(frequency=None))),
        ("rules-not-a-list", enc(good(retention_rules=5))),
        ("rules-dict", enc(good(retention_rules={"age": 1}))),
        ("rule-string", enc(good(retention_rules=["x"]))),
        ("rule-missing-keep-for", enc(good(retention_rules=[{"age": 1}]))),
        ("rule-age-string", enc(good(retention_rules=[{"age": "1d", "keep_for": 1}]))),
        ("remote-not-a-list", enc(good(remote="x"))),
        ("remote-null", enc(good(remote=None))),
        ("remote-destination-int", enc(good(remote=[{"destination": 5}]))),
        ("remote-destination-missing", enc(good(remote=[{"frequency": 5}]))),
        ("remote-frequency-string", enc(good(remote=[{"destination": "x", "frequency": "q"}]))),
    ]

    @pytest.mark.parametrize("config", [c for _, c in BAD_CONFIGS], ids=[i for i, _ in BAD_CONFIGS])
    def test_an_undecodable_or_mistyped_config_is_400_and_writes_nothing(
        self, client, authed, zfs, config
    ):
        resp = post_json(client, body_with(config=config))
        assert resp.status_code == 400, resp.get_data(as_text=True)
        assert resp.get_json() == {"error": BAD_CONFIG_ERROR}
        assert_nothing_written(zfs)
        assert authed.gate.active == 0

    def test_the_400_says_nothing_about_why(self, client):
        """The decoder's complaint (a `KeyError`, a `JSONDecodeError`...) stays in the log."""
        body = post_json(client, body_with(config=enc(good(name=5)))).get_data(as_text=True)
        for word in ("KeyError", "TypeError", "JSONDecodeError", "Traceback", "name is not", "decode"):
            assert word not in body

    def test_the_log_names_the_problem_but_never_the_input(self, client, caplog):
        marker = "SECRETMARKER-" + "z" * 20
        config = enc(good(name=marker, frequency=-1))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            assert post_json(client, body_with(config=config)).status_code == 400
        text = caplog.text
        assert "refusing its config" in text and "frequency is not positive" in text
        assert marker not in text and config not in text

    def test_a_bad_config_is_400_even_when_the_dataset_exists_and_is_owned(self, client, zfs):
        zfs["exists"].return_value = True
        zfs["props"].return_value = {PROP_CLIENT_ID: CLIENT_ID, PROP_SOURCE_DATASET: "pool/data"}
        zfs["list"].return_value = []
        assert post_json(client, body_with(config="")).status_code == 400
        assert_nothing_written(zfs)

    @pytest.mark.parametrize("config", [
        # a remote `frequency` of 0 is already "inherit" to the decoder (`None`), so it is valid; only
        # a negative one reaches the positivity check (see the `negative-remote-frequency` bad config)
        pytest.param(good(remote=[{"destination": "x", "frequency": 0}]), id="remote-zero-frequency-inherits"),
        pytest.param(good(retention_rules=[]), id="no-rules"),
        pytest.param(good(remote=[]), id="no-remote"),
        pytest.param({k: v for k, v in GOOD.items() if k not in ("recursive", "enabled", "remote", "retention_rules")},
                     id="defaults"),
        pytest.param(good(recursive=True, enabled=False), id="booleans"),
        pytest.param(good(remote=[{"destination": "x"}]), id="remote-without-frequency"),
        pytest.param(good(remote=[{"destination": "x", "frequency": 60.5}]), id="remote-frequency"),
        pytest.param(good(frequency=86400 * 365 * 100), id="a-century"),
    ])
    def test_valid_variants_are_accepted(self, client, zfs, config):
        resp = post_json(client, body_with(config=enc(config)))
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert zfs["set"].call_count == 1


class TestCanonicalDatasetConfig:
    """The function behind the route."""

    def test_the_result_is_to_property_of_the_decoded_config(self):
        sent = enc(good())
        assert canonical_dataset_config(sent) == DatasetConfig.from_property(sent).to_property()

    def test_a_canonical_value_is_unchanged(self):
        canonical = DatasetConfig(name="pool/data", frequency=timedelta(hours=2), retention_rules=[
            RetentionRule(timedelta(days=1), timedelta(days=7))]).to_property()
        assert canonical_dataset_config(canonical) == canonical

    @pytest.mark.parametrize("value", [None, "", 0, [], {}, b"abc", True])
    def test_a_missing_or_empty_value(self, value):
        with pytest.raises(ValueError, match="missing or empty"):
            canonical_dataset_config(value)

    @pytest.mark.parametrize("value", [c for _, c in TestConfigShape.BAD_CONFIGS],
                             ids=[i for i, _ in TestConfigShape.BAD_CONFIGS])
    def test_every_bad_config_is_a_value_error_and_only_that(self, value):
        """Whatever the input, the only exception is `ValueError` (the route maps nothing else)."""
        with pytest.raises(ValueError):
            canonical_dataset_config(value)

    @pytest.mark.parametrize("value", [0, 0.0, -0.0, -1, -86400, 1e-9])
    def test_a_frequency_that_is_not_positive_names_the_frequency(self, value):
        with pytest.raises(ValueError, match="^frequency is not positive$"):
            canonical_dataset_config(enc(good(frequency=value)))

    @pytest.mark.parametrize("rule", [
        {"age": 0, "keep_for": 5}, {"age": 5, "keep_for": 0}, {"age": 0, "keep_for": 0},
        {"age": -1, "keep_for": 5}, {"age": 5, "keep_for": -1},
    ])
    def test_a_retention_duration_that_is_not_positive_names_the_rule(self, rule):
        with pytest.raises(ValueError, match="^a retention rule has a duration that is not positive$"):
            canonical_dataset_config(enc(good(retention_rules=[rule])))

    @pytest.mark.parametrize("value", [-1, -0.5, -86400])
    def test_a_negative_remote_frequency_names_the_remote_frequency(self, value):
        with pytest.raises(ValueError, match="^a remote frequency is not positive$"):
            canonical_dataset_config(enc(good(remote=[{"destination": "x", "frequency": value}])))

    @pytest.mark.parametrize("value", [None, 0, 0.0])
    def test_a_remote_frequency_that_is_absent_or_zero_means_inherit(self, value):
        """The decoder turns a falsy remote `frequency` into `None`: nothing to be positive."""
        out = DatasetConfig.from_property(
            canonical_dataset_config(enc(good(remote=[{"destination": "x", "frequency": value}]))))
        assert out.remote[0].frequency is None

    def test_the_smallest_positive_values_are_accepted(self):
        canonical_dataset_config(enc(good(
            frequency=0.000001, retention_rules=[{"age": 0.000001, "keep_for": 0.000001}],
            remote=[{"destination": "x", "frequency": 0.000001}])))

    def test_the_message_never_contains_the_input(self):
        marker = "SECRETMARKER" * 3
        for value in (enc(good(name=marker, frequency=-1)), enc(good(name=marker, recursive="yes")),
                      enc(good(name=[marker])), marker + "!", enc("[" + marker)):
            with pytest.raises(ValueError) as excinfo:
                canonical_dataset_config(value)
            assert marker not in str(excinfo.value) and marker not in repr(excinfo.value.__cause__)

    def test_a_bool_where_a_number_is_expected_does_not_break_the_result(self):
        """JSON `true` is a number to Python; whatever is accepted must re-decode (the function
        checks that itself)."""
        out = canonical_dataset_config(enc(good(frequency=True)))
        assert DatasetConfig.from_property(out).frequency.total_seconds() == 1.0

    def test_retention_rules_and_destinations_survive(self):
        out = DatasetConfig.from_property(canonical_dataset_config(enc(good(
            retention_rules=[{"age": 60, "keep_for": 120}, {"age": 3600, "keep_for": 7200}],
            remote=[{"destination": "a", "frequency": 30}, {"destination": "b", "frequency": None}],
        ))))
        assert [(r.age.total_seconds(), r.keep_for.total_seconds()) for r in out.retention_rules] == [
            (60, 120), (3600, 7200)]
        assert [(r.destination, r.frequency and r.frequency.total_seconds()) for r in out.remote] == [
            ("a", 30), ("b", None)]


# ---------------------------------------------------------------------------
# What is stored
# ---------------------------------------------------------------------------

_BASE64 = re.compile(r"[A-Za-z0-9+/]+={0,2}")


class TestStoredValue:
    def stored(self, zfs):
        (call,) = zfs["set"].call_args_list
        return call[0][1]

    def test_the_stored_value_is_to_property_of_the_decoded_config(self, client, zfs):
        sent = enc(good())
        assert post_json(client, body_with(config=sent)).status_code == 200
        assert self.stored(zfs)[PROP_CONFIG] == DatasetConfig.from_property(sent).to_property()

    @pytest.mark.parametrize("kind", ["indent", "reordered", "extra-keys", "unicode-escapes",
                                      "control-characters", "duplicate-keys", "whitespace"])
    def test_the_clients_own_string_is_never_the_stored_one(self, client, zfs, kind):
        """The same config spelled differently is stored in the one canonical spelling, and
        control characters inside the JSON do not survive as such."""
        text = {
            "indent": json.dumps(GOOD, indent=2),
            "reordered": json.dumps(dict(reversed(list(GOOD.items())))),
            "extra-keys": json.dumps({**GOOD, "evil": "x\ny\tz\x00", "org.zfsbackup:anchor.x": "1"}),
            "unicode-escapes": json.dumps({**GOOD, "name": "pool/data"}, ensure_ascii=True)
            .replace('"pool/data"', '"pool\\u002fdata"'),
            "control-characters": '{"name": "pool/data", "note": "a\\nb\\u0000c\\td", "frequency": 3600,'
                                  ' "retention_rules": [], "remote": []}',
            "duplicate-keys": '{"name": "other", "name": "pool/data", "frequency": 3600}',
            "whitespace": "  \n" + json.dumps(GOOD) + "\n\t ",
        }[kind]
        sent = enc(text)
        expected = DatasetConfig.from_property(sent).to_property()
        assert post_json(client, body_with(config=sent)).status_code == 200
        value = self.stored(zfs)[PROP_CONFIG]
        assert value == expected
        assert value != sent
        assert _BASE64.fullmatch(value)
        assert not any(c in value for c in "\n\r\t\0 ")
        assert DatasetConfig.from_property(value) == DatasetConfig.from_property(sent)

    def test_a_name_with_control_characters_in_the_json_is_stored_framed_safely(self, client, zfs):
        sent = enc(good(name="pool/da\nta\tx\x00y\rz"))
        assert post_json(client, body_with(config=sent)).status_code == 200
        value = self.stored(zfs)[PROP_CONFIG]
        assert _BASE64.fullmatch(value)
        assert DatasetConfig.from_property(value).name == "pool/da\nta\tx\x00y\rz"

    def test_the_property_set_is_exactly_config_client_id_and_source_dataset(self, client, zfs):
        post_json(client, body_with())
        props = self.stored(zfs)
        assert set(props) == {PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET}
        assert props[PROP_CLIENT_ID] == CLIENT_ID          # the bound id
        assert props[PROP_SOURCE_DATASET] == "pool/data"   # from the URL, not the config's name

    def test_the_source_dataset_comes_from_the_url_not_the_config_name(self, client, zfs):
        post_json(client, body_with(config=enc(good(name="somewhere/else"))))
        assert self.stored(zfs)[PROP_SOURCE_DATASET] == "pool/data"

    def test_it_is_written_for_an_existing_dataset_too(self, client, zfs):
        zfs["exists"].return_value = True
        post_json(client, body_with())
        assert zfs["create"].call_count == 0
        assert self.stored(zfs)[PROP_CONFIG] == DatasetConfig.from_property(enc(good())).to_property()

    def test_it_is_written_every_time(self, client, zfs):
        """A copy of the property that arrived inside a received stream is overwritten by the next
        negotiate (the property is excluded from the receive, but this is the second line)."""
        zfs["exists"].return_value = True
        for _ in range(3):
            post_json(client, body_with())
        assert zfs["set"].call_count == 3

    def test_a_new_dataset_is_created_with_parents_before_the_set(self, client, zfs):
        order = []
        zfs["create"].side_effect = lambda *a, **k: order.append(("create", k))
        zfs["set"].side_effect = lambda *a, **k: order.append(("set",))
        assert post_json(client, body_with()).status_code == 200
        assert order == [("create", {"parents": True}), ("set",)]
        assert str(zfs["create"].call_args[0][0]) == SERVER_DS

    def test_a_previously_stored_foreign_config_is_replaced(self, client, zfs):
        """An existing dataset whose recorded config is something else: the negotiate rewrites it."""
        zfs["exists"].return_value = True
        zfs["props"].return_value = {PROP_CLIENT_ID: CLIENT_ID, PROP_SOURCE_DATASET: "pool/data",
                                     PROP_CONFIG: enc(good(retention_rules=[]))}
        post_json(client, body_with(config=enc(good(retention_rules=[{"age": 5, "keep_for": 9}]))))
        stored = DatasetConfig.from_property(self.stored(zfs)[PROP_CONFIG])
        assert [(r.age.total_seconds(), r.keep_for.total_seconds()) for r in stored.retention_rules] == [(5, 9)]

    def test_the_reply_is_unchanged(self, client, zfs):
        zfs["list"].return_value = []
        resp = post_json(client, body_with(snapshots=["a"]))
        assert resp.get_json() == {"common_snapshot": None, "server_dataset": SERVER_DS}

    def test_the_excluded_property_list_is_what_negotiate_writes(self):
        assert RECEIVE_EXCLUDED_PROPERTIES == (PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET)


# ---------------------------------------------------------------------------
# zfs failing
# ---------------------------------------------------------------------------

SECRET = "INTERNAL-DETAIL /dev/zfs permission denied for tank/secret"


class TestZfsFailures:
    @pytest.mark.parametrize("failing", ["set", "create"])
    def test_a_value_error_is_400_bad_property_with_the_details_only_in_the_log(
        self, client, authed, zfs, caplog, failing
    ):
        zfs[failing].side_effect = ValueError(SECRET)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = post_json(client, body_with())
        assert resp.status_code == 400
        assert resp.get_json() == {"error": BAD_PROPERTY_ERROR}
        assert "INTERNAL" not in resp.get_data(as_text=True) and "/dev/zfs" not in resp.get_data(as_text=True)
        records = [r for r in caplog.records if r.name == "zfsbackup.api"]
        assert any(SECRET in r.getMessage() and r.levelno == logging.WARNING for r in records)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("failing", ["set", "create"])
    @pytest.mark.parametrize("error", [
        RuntimeError(SECRET), OSError(13, SECRET), Exception(SECRET), KeyError(SECRET),
    ], ids=["runtime", "oserror", "exception", "keyerror"])
    def test_any_other_failure_is_500_without_the_exception_text(
        self, client, authed, zfs, caplog, failing, error
    ):
        zfs[failing].side_effect = error
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = post_json(client, body_with())
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to record the configuration of {SERVER_DS}"}
        text = resp.get_data(as_text=True)
        assert "INTERNAL" not in text and "/dev/zfs" not in text and "tank/secret" not in text
        assert any(SECRET in r.getMessage() and r.levelno == logging.ERROR for r in caplog.records)
        assert authed.gate.active == 0

    def test_a_failed_set_after_a_create_lists_nothing(self, client, zfs):
        zfs["set"].side_effect = RuntimeError(SECRET)
        post_json(client, body_with(snapshots=["a"]))
        zfs["list"].assert_not_called()

    def test_the_next_request_is_served_after_a_failure(self, client, zfs):
        zfs["set"].side_effect = [RuntimeError(SECRET), None]
        assert post_json(client, body_with()).status_code == 500
        assert post_json(client, body_with()).status_code == 200


class TestDryRun:
    @pytest.fixture
    def dry(self):
        return Authed(_config(dry_run=True))

    def test_it_still_validates(self, dry, zfs):
        client = dry.client()
        assert post_json(client, body_with(config="")).get_json() == {"error": BAD_CONFIG_ERROR}
        assert post_json(client, body_with(snapshots="x")).get_json() == {"error": SNAPSHOTS_ERROR}
        assert post_raw(client, b"[]").get_json() == {"error": SHAPE_ERROR}
        assert post_json(client, body_with(config=enc(good(frequency=-1)))).status_code == 400
        assert_nothing_written(zfs)
        assert dry.gate.active == 0

    def test_a_good_body_is_served_and_writes_nothing(self, dry, zfs):
        resp = post_json(dry.client(), body_with())
        assert resp.status_code == 200
        assert resp.get_json()["server_dataset"] == SERVER_DS
        assert_nothing_written(zfs)

    def test_a_dry_run_finds_the_common_snapshot_of_an_existing_dataset(self, dry, zfs):
        zfs["exists"].return_value = True
        snap = type("S", (), {"short": "autosnap_1"})()
        zfs["list"].return_value = [snap]
        resp = post_json(dry.client(), body_with(snapshots=["autosnap_2", "autosnap_1"]))
        assert resp.get_json()["common_snapshot"] == "autosnap_1"
        assert_nothing_written(zfs)


# ---------------------------------------------------------------------------
# The client dataset name
# ---------------------------------------------------------------------------


class TestClientDatasetProblem:
    @pytest.mark.parametrize("name", [
        "pool/data", "tank", "pool/a/b/c", "pool/a b", "pool/with-dash_and.dot:colon",
        "mirror/data", "c0/data", "log/data",
    ])
    def test_valid_names(self, name):
        assert client_dataset_problem(name) is None

    @pytest.mark.parametrize("name, fragment", [
        ("", "empty"), (None, "empty"), (5, "empty"),
        ("pool/da\nta", "control character"), ("pool/da\tta", "control character"),
        ("pool/da\rta", "control character"), ("pool/da\0ta", "control character"),
        ("pool\n/data", "control character"), ("\npool/data", "control character"),
        ("pool/data\n", "control character"),
        ("po ol/data", "invalid"), ("po$ol/data", "invalid"), ("1pool/data", "invalid"),
        ("pool%/data", "invalid"), ("/pool/data", "invalid"), ("pool//data", "invalid"),
        ("pool/data/", "invalid"), ("pool/a@b", "invalid"), ("pool/a#b", "invalid"),
        ("pool/.hidden", "invalid"), ("pool/é", "invalid"), ("pool/" + "a" * 300, "invalid"),
    ])
    def test_refused_names(self, name, fragment):
        problem = client_dataset_problem(name)
        assert problem is not None and fragment in problem
        assert "\n" not in problem and "\0" not in problem        # the message is safe to log

    def test_the_pool_component_is_checked_although_the_server_name_drops_it(self):
        """`1pool/data` maps to the valid server dataset `<target>/<id>/data`."""
        assert client_dataset_problem("1pool/data") is not None


#: Bad names as they arrive in a URL path. A NEWLINE cannot: werkzeug's `<path:>` converter does not
#: match one, so those requests are a routing 404 (still a refusal, and no `zfs` call); the view's own
#: 400 for them is exercised by calling the view function (`TestViewRefusesControlCharacters`).
BAD_NAMES = [
    pytest.param("pool/da%09ta", id="tab"),
    pytest.param("pool/da%0Dta", id="carriage-return"), pytest.param("pool/da%00ta", id="nul"),
    pytest.param("po%24ol/data", id="dollar-in-pool"), pytest.param("po%20ol/data", id="space-in-pool"),
    pytest.param("1pool/data", id="digit-pool"), pytest.param("po%25ol/data", id="percent-in-pool"),
    pytest.param("pool/a%25b", id="percent"), pytest.param("pool/.hidden", id="dot-component"),
    pytest.param("pool%09/data", id="tab-in-pool"), pytest.param("pool%00/data", id="nul-in-pool"),
]
NEWLINE_NAMES = [
    pytest.param("pool/da%0Ata", id="newline"), pytest.param("pool%0A/data", id="newline-in-pool"),
    pytest.param("pool/data%0A", id="trailing-newline"),
]
CONTROL_NAMES = [
    pytest.param("pool/da\nta", id="newline"), pytest.param("pool/da\tta", id="tab"),
    pytest.param("pool/da\rta", id="carriage-return"), pytest.param("pool/da\0ta", id="nul"),
    pytest.param("pool\n/data", id="newline-in-pool"), pytest.param("pool\t/data", id="tab-in-pool"),
    pytest.param("pool/data\n", id="trailing-newline"),
]


def _no_zfs(zfs):
    for mock_name, mock in zfs.items():
        assert not mock.called, mock_name


class TestClientDatasetName:
    @pytest.mark.parametrize("name", BAD_NAMES)
    def test_negotiate_is_400_and_no_zfs_call(self, client, authed, zfs, name):
        resp = post_json(client, body_with(), url=f"/backup/{CLIENT_ID}/{name}/negotiate")
        assert resp.status_code == 400, resp.get_data(as_text=True)
        assert resp.get_json()["error"].startswith("invalid dataset name ")
        _no_zfs(zfs)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name", BAD_NAMES)
    def test_resume_token_is_400_and_no_zfs_call(self, client, authed, zfs, name):
        resp = client.get(f"/backup/{CLIENT_ID}/{name}/resume_token")
        assert resp.status_code == 400, resp.get_data(as_text=True)
        assert resp.get_json()["error"].startswith("invalid dataset name ")
        _no_zfs(zfs)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name", NEWLINE_NAMES)
    def test_a_newline_in_the_url_is_refused_and_reaches_no_zfs(self, client, authed, zfs, name):
        for resp in (post_json(client, body_with(), url=f"/backup/{CLIENT_ID}/{name}/negotiate"),
                     client.get(f"/backup/{CLIENT_ID}/{name}/resume_token")):
            assert resp.status_code in (400, 404)
        _no_zfs(zfs)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name", ["pool/da\nta", "pool/da\tta", "pool/da\rta", "pool/da\0ta",
                                      "po ol/data", "po$ol/data", "1pool/data", "pool/.hidden"])
    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_stream_sends_an_error_and_runs_nothing(self, mocker, name, mode):
        stream = Stream(mocker)
        stream.run(mode, dataset=name)
        assert len(stream.sent) == 1
        assert stream.sent[0]["error"].startswith("invalid dataset name ")
        stream.untouched()
        assert stream.claimed == set()
        assert stream.gate.active == 0

    def test_a_single_component_name_is_fine(self, client, zfs):
        resp = post_json(client, body_with(), url=f"/backup/{CLIENT_ID}/pool/negotiate")
        assert resp.status_code == 200
        assert resp.get_json()["server_dataset"] == "pool/backups/client1"

    def test_a_name_with_a_space_is_fine_and_stored_verbatim(self, client, zfs):
        resp = post_json(client, body_with(), url=f"/backup/{CLIENT_ID}/pool/a%20b/negotiate")
        assert resp.status_code == 200
        (call,) = zfs["set"].call_args_list
        assert call[0][1][PROP_SOURCE_DATASET] == "pool/a b"


class TestViewRefusesControlCharacters:
    """`\\n \\r \\t \\0` in the client dataset name, with the route's converter out of the way: the
    view itself answers 400 (negotiate / resume_token) and nothing reaches `zfs`."""

    @pytest.mark.parametrize("name", CONTROL_NAMES)
    def test_negotiate(self, authed, zfs, name):
        view = authed.app.view_functions["backup_negotiate"]
        with authed.context(URL, method="POST", json=body_with()):
            resp, status = view(CLIENT_ID, name)
        assert status == 400
        assert resp.get_json()["error"].startswith("invalid dataset name ")
        _no_zfs(zfs)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name", CONTROL_NAMES)
    def test_resume_token(self, authed, zfs, name):
        view = authed.app.view_functions["backup_resume_token"]
        with authed.context(f"/backup/{CLIENT_ID}/pool/data/resume_token"):
            resp, status = view(CLIENT_ID, name)
        assert status == 400
        assert resp.get_json()["error"].startswith("invalid dataset name ")
        _no_zfs(zfs)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name", ["pool\n/data", "pool\t/data", "pool\r/data", "pool\0/data"])
    def test_the_control_character_check_names_itself_when_the_server_name_is_valid(
        self, authed, zfs, name
    ):
        """A control character in the POOL component is dropped by the server-side mapping
        (`<target>/<id>/data` is valid): only `client_dataset_problem` catches it."""
        view = authed.app.view_functions["backup_negotiate"]
        with authed.context(URL, method="POST", json=body_with()):
            resp, status = view(CLIENT_ID, name)
        assert status == 400 and "control character" in resp.get_json()["error"]

    @pytest.mark.xfail(strict=True, reason=(
        "product: `_server_dataset` checks the derived server name first and appends the "
        "InvalidNameError text, which repeats the name with its RAW control character; the `!r` "
        "escaping in api.py is defeated for control characters in the relative part"))
    @pytest.mark.parametrize("name", ["pool/da\nta", "pool/da\tta", "pool/da\rta", "pool/da\0ta"])
    def test_the_error_text_carries_no_raw_control_character(self, authed, zfs, name):
        view = authed.app.view_functions["backup_negotiate"]
        with authed.context(URL, method="POST", json=body_with()):
            resp, status = view(CLIENT_ID, name)
        assert not any(c in resp.get_json()["error"] for c in "\n\r\t\0")

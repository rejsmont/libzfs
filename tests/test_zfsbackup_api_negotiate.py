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
`TestStoredValue`, `TestZfsFailures`, `TestDryRun`, `TestClientDatasetName`, then milestone B:
`TestPendingConfigs` (R0: a dataset that does not exist is not created by negotiate, its config waits
in memory), `TestPendingMapBounds` (the cap and the lock), `TestServerCopy` (S6-d), then the R0 fixes:
`TestPlaceholderOwnership` (D7 for an existing dataset without an owner, the atomic 409),
`TestNegotiateClaim` (negotiate holds the name), `TestUnsettledAncestor`, `TestPendingTtl` and
`TestConfigSizeLimit` (8 KiB), and B-2b: `TestServerNewer` (the server snapshots beyond the common one).
"""

import base64
import json
import logging
import re
import threading
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from _api_support import (
    CLIENT_ID, Authed, patch_api_clock, pending_api, pending_configs, pending_deadlines, pending_guard,
    streams_in_progress,
)
from test_zfsbackup_api import STREAM_PATH, Stream  # noqa: F401  (Stream: a harness, not a test)
from zfsbackup.api import (
    BAD_CONFIG_ERROR, CONFIG_TOO_LARGE_ERROR, BAD_PROPERTY_ERROR, SERVER_NEWER_MAX, SERVER_OWNED_PROPERTIES,
    canonical_dataset_config,
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
    def test_valid_variants_are_accepted(self, client, authed, zfs, config):
        """On a missing dataset the canonical config waits in memory (R0); on an existing one it is
        set at once."""
        resp = post_json(client, body_with(config=enc(config)))
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert zfs["set"].call_count == 0
        assert pending_configs(authed.app)[SERVER_DS][0] == canonical_dataset_config(enc(config))
        zfs["exists"].return_value = True
        resp = post_json(client, body_with(config=enc(config)))
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert zfs["set"].call_count == 1
        assert zfs["set"].call_args[0][1][PROP_CONFIG] == canonical_dataset_config(enc(config))


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
    """What negotiate stores. For an existing dataset it is `zfs set` at once; for one that does not
    exist yet (R0) the same canonical config waits in the in-memory pending map, and the stream
    puts it in effect with `-o`. The content tests run against both places (`where`)."""

    @pytest.fixture(params=["existing", "pending"])
    def where(self, request, zfs):
        zfs["exists"].return_value = request.param == "existing"
        return request.param

    @pytest.fixture(autouse=True)
    def _app(self, authed):
        self.authed = authed

    def stored(self, zfs):
        (call,) = zfs["set"].call_args_list
        return call[0][1]

    def stored_config(self, where, zfs):
        if where == "existing":
            return self.stored(zfs)[PROP_CONFIG]
        assert not zfs["set"].called            # nothing is set on a dataset that is not there
        return pending_configs(self.authed.app)[SERVER_DS][0]

    def test_the_stored_value_is_to_property_of_the_decoded_config(self, client, zfs, where):
        sent = enc(good())
        assert post_json(client, body_with(config=sent)).status_code == 200
        assert self.stored_config(where, zfs) == DatasetConfig.from_property(sent).to_property()

    @pytest.mark.parametrize("kind", ["indent", "reordered", "extra-keys", "unicode-escapes",
                                      "control-characters", "duplicate-keys", "whitespace"])
    def test_the_clients_own_string_is_never_the_stored_one(self, client, zfs, where, kind):
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
        value = self.stored_config(where, zfs)
        assert value == expected
        assert value != sent
        assert _BASE64.fullmatch(value)
        assert not any(c in value for c in "\n\r\t\0 ")
        assert DatasetConfig.from_property(value) == DatasetConfig.from_property(sent)

    def test_a_name_with_control_characters_in_the_json_is_stored_framed_safely(self, client, zfs, where):
        sent = enc(good(name="pool/da\nta\tx\x00y\rz"))
        assert post_json(client, body_with(config=sent)).status_code == 200
        value = self.stored_config(where, zfs)
        assert _BASE64.fullmatch(value)
        assert DatasetConfig.from_property(value).name == "pool/da\nta\tx\x00y\rz"

    def test_the_property_set_is_exactly_config_client_id_and_source_dataset(self, client, zfs):
        zfs["exists"].return_value = True
        post_json(client, body_with())
        props = self.stored(zfs)
        assert set(props) == {PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET}
        assert props[PROP_CLIENT_ID] == CLIENT_ID          # the bound id
        assert props[PROP_SOURCE_DATASET] == "pool/data"   # from the URL, not the config's name

    def test_the_source_dataset_comes_from_the_url_not_the_config_name(self, client, zfs, where):
        post_json(client, body_with(config=enc(good(name="somewhere/else"))))
        if where == "existing":
            assert self.stored(zfs)[PROP_SOURCE_DATASET] == "pool/data"
        else:
            assert pending_configs(self.authed.app)[SERVER_DS][1] == "pool/data"

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

    def test_a_new_dataset_gets_its_parents_and_nothing_else(self, client, zfs):
        """R0: only the parent chain (`-p`), never the leaf and never a `zfs set`."""
        order = []
        zfs["create"].side_effect = lambda *a, **k: order.append(("create", str(a[0]), k))
        zfs["set"].side_effect = lambda *a, **k: order.append(("set",))
        assert post_json(client, body_with()).status_code == 200
        assert order == [("create", "pool/backups/client1", {"parents": True})]

    def test_a_top_level_dataset_creates_the_target_and_the_id_directory(self, client, zfs):
        """`pool` -> `<target>/<id>`: its parent is the target, and `-p` makes the id directory."""
        assert post_json(client, body_with(), url=f"/backup/{CLIENT_ID}/pool/negotiate").status_code == 200
        assert str(zfs["create"].call_args[0][0]) == "pool/backups"
        assert zfs["create"].call_args.kwargs == {"parents": True}

    def test_a_previously_stored_foreign_config_is_replaced(self, client, zfs):
        """An existing dataset whose recorded config is something else: the negotiate rewrites it."""
        zfs["exists"].return_value = True
        zfs["props"].return_value = {PROP_CLIENT_ID: CLIENT_ID, PROP_SOURCE_DATASET: "pool/data",
                                     PROP_CONFIG: enc(good(retention_rules=[]))}
        post_json(client, body_with(config=enc(good(retention_rules=[{"age": 5, "keep_for": 9}]))))
        stored = DatasetConfig.from_property(self.stored(zfs)[PROP_CONFIG])
        assert [(r.age.total_seconds(), r.keep_for.total_seconds()) for r in stored.retention_rules] == [(5, 9)]

    def test_the_reply_has_exactly_these_fields(self, client, zfs):
        zfs["list"].return_value = []
        resp = post_json(client, body_with(snapshots=["a"]))
        assert resp.get_json() == {
            "common_snapshot": None, "server_dataset": SERVER_DS, "server_copy": "none",
        }

    def test_the_excluded_property_list_is_what_negotiate_writes(self):
        assert SERVER_OWNED_PROPERTIES == (PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET)


# ---------------------------------------------------------------------------
# zfs failing
# ---------------------------------------------------------------------------

SECRET = "INTERNAL-DETAIL /dev/zfs permission denied for tank/secret"


class TestZfsFailures:
    @pytest.mark.parametrize("failing", ["set", "create"])
    def test_a_value_error_is_400_bad_property_with_the_details_only_in_the_log(
        self, client, authed, zfs, caplog, failing
    ):
        zfs["exists"].return_value = failing == "set"      # a missing dataset only creates the parent
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
        zfs["exists"].return_value = failing == "set"
        zfs[failing].side_effect = error
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = post_json(client, body_with())
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to record the configuration of {SERVER_DS}"}
        text = resp.get_data(as_text=True)
        assert "INTERNAL" not in text and "/dev/zfs" not in text and "tank/secret" not in text
        assert any(SECRET in r.getMessage() and r.levelno == logging.ERROR for r in caplog.records)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("failing", ["set", "create"])
    def test_a_failed_write_lists_nothing(self, client, zfs, failing):
        zfs["exists"].return_value = failing == "set"
        zfs[failing].side_effect = RuntimeError(SECRET)
        post_json(client, body_with(snapshots=["a"]))
        zfs["list"].assert_not_called()

    def test_a_failed_create_remembers_nothing(self, client, authed, zfs):
        """No entry is kept for a dataset whose parent could not be made: a stream then says
        'negotiate again' rather than receiving into a missing parent."""
        zfs["create"].side_effect = RuntimeError(SECRET)
        assert post_json(client, body_with()).status_code == 500
        assert dict(pending_configs(authed.app)) == {}

    def test_a_failed_set_on_an_existing_dataset_drops_nothing_and_adds_nothing(self, client, authed, zfs):
        zfs["exists"].return_value = True
        zfs["set"].side_effect = RuntimeError(SECRET)
        assert post_json(client, body_with()).status_code == 500
        assert dict(pending_configs(authed.app)) == {}

    def test_the_next_request_is_served_after_a_failure(self, client, zfs):
        zfs["exists"].return_value = True
        zfs["set"].side_effect = [RuntimeError(SECRET), None]
        assert post_json(client, body_with()).status_code == 500
        assert post_json(client, body_with()).status_code == 200

    def test_the_next_request_is_served_after_a_failed_create(self, client, authed, zfs):
        zfs["create"].side_effect = [RuntimeError(SECRET), None]
        assert post_json(client, body_with()).status_code == 500
        assert post_json(client, body_with()).status_code == 200
        assert SERVER_DS in pending_configs(authed.app)


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
        assert dict(pending_configs(dry.app)) == {}      # not even in memory

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

    def test_a_name_with_a_space_is_fine_and_stored_verbatim(self, client, authed, zfs):
        resp = post_json(client, body_with(), url=f"/backup/{CLIENT_ID}/pool/a%20b/negotiate")
        assert resp.status_code == 200
        assert pending_configs(authed.app)["pool/backups/client1/a b"][1] == "pool/a b"
        zfs["exists"].return_value = True
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


# ---------------------------------------------------------------------------
# R0: the leaf is created by the receive; negotiate only makes the parents
# ---------------------------------------------------------------------------


def negotiate(client, *, dataset="pool/data", config=None, snapshots=(), client_id=CLIENT_ID):
    return post_json(
        client, {"snapshots": list(snapshots), "config": enc(GOOD) if config is None else config},
        url=f"/backup/{client_id}/{dataset}/negotiate",
    )


class TestPendingConfigs:
    """zfs refuses a raw full (and a plain full under an encrypted target) onto a pre-created
    dataset, so a missing dataset is NOT created: negotiate makes only its parent chain and keeps
    the canonical config, with the client dataset that sent it, in `pending_configs`."""

    def test_a_missing_dataset_gets_its_parent_only_and_a_pending_entry(self, client, authed, zfs):
        resp = negotiate(client)
        assert resp.status_code == 200
        assert resp.get_json() == {
            "common_snapshot": None, "server_dataset": SERVER_DS, "server_copy": "none"}
        (call,) = zfs["create"].call_args_list
        assert str(call.args[0]) == "pool/backups/client1"       # the parent, never the leaf
        assert call.kwargs == {"parents": True}
        zfs["set"].assert_not_called()
        zfs["list"].assert_not_called()                          # nothing to compare on a missing dataset
        assert dict(pending_configs(authed.app)) == {
            SERVER_DS: (canonical_dataset_config(enc(GOOD)), "pool/data")}

    def test_the_leaf_is_never_created_for_any_depth(self, zfs):
        # a fresh app per name: a child's negotiate may be told to retry while an ancestor is pending
        for dataset, parent in (("pool/a", "pool/backups/client1"),
                                ("pool/a/b", "pool/backups/client1/a"),
                                ("pool/a/b/c", "pool/backups/client1/a/b"),
                                ("pool", "pool/backups")):
            zfs["create"].reset_mock()
            assert negotiate(Authed(_config()).client(), dataset=dataset).status_code == 200
            (call,) = zfs["create"].call_args_list
            assert str(call.args[0]) == parent, dataset
            assert call.kwargs == {"parents": True}

    def test_an_existing_dataset_gets_its_three_properties_and_no_create(self, client, authed, zfs):
        zfs["exists"].return_value = True
        assert negotiate(client).status_code == 200
        zfs["create"].assert_not_called()
        (call,) = zfs["set"].call_args_list
        assert str(call.args[0]) == SERVER_DS
        assert call.args[1] == {
            PROP_CONFIG: canonical_dataset_config(enc(GOOD)),
            PROP_CLIENT_ID: CLIENT_ID, PROP_SOURCE_DATASET: "pool/data"}
        assert dict(pending_configs(authed.app)) == {}

    def test_a_dataset_that_now_exists_drops_its_pending_entry(self, client, authed, zfs):
        """The first receive created it (or an operator did): the local property is the config."""
        negotiate(client)
        assert SERVER_DS in pending_configs(authed.app)
        zfs["exists"].return_value = True
        negotiate(client)
        assert dict(pending_configs(authed.app)) == {}

    def test_a_second_negotiate_replaces_the_entry(self, client, authed, zfs):
        negotiate(client, config=enc(good(frequency=60)))
        negotiate(client, config=enc(good(frequency=120)))
        entry = pending_configs(authed.app)[SERVER_DS]
        assert DatasetConfig.from_property(entry[0]).frequency.total_seconds() == 120
        assert len(pending_configs(authed.app)) == 1

    def test_a_different_client_dataset_for_the_same_server_name_is_409(self, client, authed, zfs):
        """`tank/data` and `pool/data` both map to `<target>/<id>/data`."""
        assert negotiate(client, dataset="pool/data").status_code == 200
        zfs["create"].reset_mock()
        resp = negotiate(client, dataset="tank/data")
        assert resp.status_code == 409
        assert resp.get_json() == {"error": (
            f"server dataset {SERVER_DS} already holds backups of 'pool/data'; "
            "refusing to mix in 'tank/data'")}
        zfs["create"].assert_not_called()
        zfs["set"].assert_not_called()
        assert pending_configs(authed.app)[SERVER_DS][1] == "pool/data"      # the first one keeps it
        assert authed.gate.active == 0

    def test_the_refusal_is_the_same_text_as_for_an_existing_dataset(self, client, zfs):
        negotiate(client, dataset="pool/data")
        pending_text = negotiate(client, dataset="tank/data").get_json()["error"]
        zfs["exists"].return_value = True
        zfs["props"].return_value = {PROP_SOURCE_DATASET: "pool/data"}
        assert negotiate(client, dataset="tank/data").get_json()["error"] == pending_text

    def test_the_other_client_dataset_is_refused_until_the_worker_restarts(self, client, authed, zfs):
        negotiate(client, dataset="pool/data")
        assert negotiate(client, dataset="tank/data").status_code == 409
        again = Authed(_config())                   # a restart: the map is gone
        assert negotiate(again.client(), dataset="tank/data").status_code == 200

    def test_two_clients_do_not_share_an_entry(self, authed, zfs):
        other = authed.cert_for("client2")
        assert negotiate(authed.client(), dataset="pool/data").status_code == 200
        assert negotiate(authed.client(other), dataset="tank/data", client_id="client2").status_code == 200
        assert sorted(pending_configs(authed.app)) == [
            "pool/backups/client1/data", "pool/backups/client2/data"]

    def test_a_dry_run_creates_nothing_and_remembers_nothing(self, zfs):
        dry = Authed(_config(dry_run=True))
        assert negotiate(dry.client()).status_code == 200
        assert_nothing_written(zfs)
        assert dict(pending_configs(dry.app)) == {}

    def test_a_dry_run_never_makes_an_entry_so_nothing_pending_refuses(self, zfs):
        """No entry is ever made in a dry run, so nothing pending can refuse."""
        dry = Authed(_config(dry_run=True))
        assert negotiate(dry.client(), dataset="pool/data").status_code == 200
        assert negotiate(dry.client(), dataset="tank/data").status_code == 200

    def test_a_bad_request_remembers_nothing(self, client, authed, zfs):
        assert negotiate(client, config="").status_code == 400
        assert dict(pending_configs(authed.app)) == {}

    def test_the_ownership_conflict_of_an_existing_dataset_comes_first(self, client, authed, zfs):
        zfs["exists"].return_value = True
        zfs["props"].return_value = {PROP_CLIENT_ID: "someone-else"}
        assert negotiate(client).status_code == 409
        assert dict(pending_configs(authed.app)) == {}

    def test_negotiating_twice_creates_the_parent_twice_and_keeps_one_entry(self, client, authed, zfs):
        """`zfs create -p` is idempotent: a second negotiate of an unreceived dataset creates again."""
        negotiate(client)
        negotiate(client)
        assert zfs["create"].call_count == 2
        assert len(pending_configs(authed.app)) == 1


class TestPendingMapBounds:
    def test_the_oldest_entries_are_evicted_past_the_cap(self, client, authed, zfs, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_MAX", 3)
        for n in range(5):
            assert negotiate(client, dataset=f"pool/d{n}").status_code == 200
        assert list(pending_configs(authed.app)) == [
            "pool/backups/client1/d2", "pool/backups/client1/d3", "pool/backups/client1/d4"]

    def test_renegotiating_refreshes_an_entry_so_it_is_evicted_last(self, client, authed, zfs, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_MAX", 3)
        for n in range(3):
            negotiate(client, dataset=f"pool/d{n}")
        negotiate(client, dataset="pool/d0")                   # now the newest
        negotiate(client, dataset="pool/d3")                   # evicts the oldest: d1
        assert list(pending_configs(authed.app)) == [
            "pool/backups/client1/d2", "pool/backups/client1/d0", "pool/backups/client1/d3"]

    def test_an_evicted_dataset_is_told_to_negotiate_again(self, authed, zfs, mocker):
        from test_zfsbackup_api import Stream
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_MAX", 1)
        s = Stream(mocker)
        s.exists.return_value = False
        for n in range(2):
            assert negotiate(s.authed.client(), dataset=f"pool/d{n}").status_code == 200
        s.run("fresh", dataset="pool/d0")
        assert "negotiate again" in s.sent[0]["error"]
        s.receive.assert_not_called()
        s.ws.receive.side_effect = [b"chunk", json.dumps({"done": True})]
        s.run("fresh", dataset="pool/d1")
        assert s.sent[-1] == {"status": "ok"}

    def test_the_cap_is_large_and_finite(self):
        from zfsbackup.api import PENDING_CONFIGS_MAX
        assert 1000 <= PENDING_CONFIGS_MAX <= 100_000

    def test_every_access_takes_the_lock(self, authed):
        """Hold the guard: each of the three helpers, called on another thread, waits for it."""
        guard = pending_guard(authed.app)
        claim, lookup, forget = pending_api(authed.app)
        calls = {
            "claim": lambda: claim(SERVER_DS, "cfg", "pool/data"),
            "lookup": lambda: lookup(SERVER_DS),
            "forget": lambda: forget(SERVER_DS),
        }
        for name, call in calls.items():
            done = threading.Event()
            thread = threading.Thread(target=lambda c=call: (c(), done.set()), daemon=True)
            with guard:
                thread.start()
                assert not done.wait(0.3), f"{name} did not wait for the lock"
            assert done.wait(10), f"{name} never finished"
            thread.join(10)

    def test_two_threads_leave_the_map_consistent_and_bounded(self, authed, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_MAX", 50)
        claim, lookup, forget = pending_api(authed.app)
        errors = []

        def hammer(tag):
            try:
                for n in range(2000):
                    name = f"pool/backups/client1/{tag}{n % 120}"
                    claim(name, f"cfg-{tag}", f"pool/{tag}")
                    lookup(name)
                    if n % 3 == 0:
                        forget(f"pool/backups/client1/{tag}{(n * 7) % 120}")
            except BaseException as e:           # noqa: BLE001 (report any failure to the test)
                errors.append(e)

        threads = [threading.Thread(target=hammer, args=(t,), daemon=True) for t in "ab"]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert errors == []
        assert not any(t.is_alive() for t in threads)
        live = pending_configs(authed.app)
        assert 0 < len(live) <= 50
        assert all(value[1] == f"pool/{key.rsplit('/', 1)[1][0]}" for key, value in live.items())

    def test_concurrent_negotiates_of_one_dataset_leave_one_entry(self, authed, zfs):
        results = []

        def go():
            results.append(negotiate(authed.client()).status_code)

        threads = [threading.Thread(target=go, daemon=True) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        assert results == [200] * 8
        assert list(pending_configs(authed.app)) == [SERVER_DS]
        assert authed.gate.active == 0


# ---------------------------------------------------------------------------
# S6-d: `server_copy`
# ---------------------------------------------------------------------------

ID_TREE = "pool/backups/client1"


@pytest.fixture
def server_props(mocker):
    """`zfs get -o value` for `encryption` and `encryptionroot` of the server dataset; set
    `.encryption` / `.root` (None is zfs's `-`)."""
    holder = MagicMock()
    holder.encryption = "off"
    holder.root = None
    holder.calls = []

    def value(ds, prop, **kwargs):
        holder.calls.append((str(ds), prop, kwargs))
        if prop == "encryption":
            return holder.encryption
        if prop == "encryptionroot":
            return holder.root
        raise AssertionError(f"unexpected property {prop}")

    holder.mock = mocker.patch("zfsbackup.api.zfs.get.value", side_effect=value)
    return holder


def with_snapshots(zfs, *names):
    """The server dataset exists and holds these snapshots, oldest first: a name (`createtxg` and
    `guid` derived from it by `_snapshot_support`) or a `(name, txg, guid)` tuple."""
    from _snapshot_support import listed_snapshot

    zfs["exists"].return_value = True
    zfs["list"].return_value = [
        listed_snapshot(n, SERVER_DS) if isinstance(n, str)
        else listed_snapshot(n[0], SERVER_DS, txg=n[1], guid=n[2])
        for n in names
    ]


class TestServerCopy:
    """What negotiate says the server holds, so a client whose send mode would not match can stop
    before sending anything: `none` (no dataset, or no snapshots), `raw` (an encrypted dataset
    with its own encryption root: itself or one under `<target>/<id>/`) or `plain`."""

    #: (encryption, encryptionroot, reply). Roots are relative to nothing: full names.
    TABLE = [
        pytest.param("off", None, "plain", id="unencrypted"),
        pytest.param(None, None, "plain", id="encryption-unset"),
        pytest.param("off", "-", "plain", id="unencrypted-with-a-dash-root"),
        pytest.param("aes-256-gcm", SERVER_DS, "raw", id="own-root"),
        pytest.param("aes-256-ccm", SERVER_DS, "raw", id="own-root-ccm"),
        pytest.param("aes-256-gcm", f"{SERVER_DS}/child", "plain", id="root-below-the-dataset"),
        pytest.param("aes-256-gcm", f"{ID_TREE}/other", "plain", id="root-elsewhere-in-the-id-tree"),
        pytest.param("aes-256-gcm", f"{ID_TREE}/a/b/c", "plain", id="root-deep-in-the-id-tree"),
        pytest.param("aes-256-gcm", f"{ID_TREE}/data", "raw", id="root-is-the-dataset-spelled-out"),
        pytest.param("aes-256-gcm", "pool/backups", "plain", id="root-is-the-target"),
        pytest.param("aes-256-gcm", ID_TREE, "plain", id="root-is-the-id-directory"),
        pytest.param("aes-256-gcm", "pool", "plain", id="root-is-the-pool"),
        pytest.param("aes-256-gcm", "pool/backups/client10/x", "plain", id="a-longer-id-is-not-a-prefix"),
        pytest.param("aes-256-gcm", "pool/backups/client2/x", "plain", id="another-clients-tree"),
        pytest.param("aes-256-gcm", None, "plain", id="encrypted-without-a-root"),
    ]

    @pytest.mark.parametrize("encryption, root, expected", TABLE)
    def test_the_predicate(self, client, zfs, server_props, encryption, root, expected):
        with_snapshots(zfs, "a")
        server_props.encryption, server_props.root = encryption, root
        resp = negotiate(client, snapshots=["a"])
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert resp.get_json()["server_copy"] == expected
        assert resp.get_json()["common_snapshot"] == "a"

    def test_a_missing_dataset_is_none_and_nothing_is_read(self, client, zfs, server_props):
        resp = negotiate(client, snapshots=["a"])
        assert resp.get_json()["server_copy"] == "none"
        server_props.mock.assert_not_called()

    def test_an_existing_dataset_without_snapshots_is_none_and_nothing_is_read(
        self, client, zfs, server_props
    ):
        """A placeholder parent, or a dataset whose first receive never finished."""
        with_snapshots(zfs)
        server_props.encryption, server_props.root = "aes-256-gcm", SERVER_DS
        resp = negotiate(client, snapshots=["a"])
        assert resp.get_json() == {
            "common_snapshot": None, "server_dataset": SERVER_DS, "server_copy": "none"}
        server_props.mock.assert_not_called()

    def test_the_two_properties_are_read_for_the_server_dataset(self, client, zfs, server_props):
        with_snapshots(zfs, "a")
        server_props.encryption, server_props.root = "aes-256-gcm", SERVER_DS
        negotiate(client, snapshots=["a"])
        assert [c[:2] for c in server_props.calls] == [
            (SERVER_DS, "encryption"), (SERVER_DS, "encryptionroot")]

    def test_an_unencrypted_dataset_reads_one_property_only(self, client, zfs, server_props):
        with_snapshots(zfs, "a")
        negotiate(client, snapshots=["a"])
        assert [c[1] for c in server_props.calls] == ["encryption"]

    def test_it_does_not_depend_on_a_common_snapshot(self, client, zfs, server_props):
        with_snapshots(zfs, "x")
        server_props.encryption, server_props.root = "aes-256-gcm", SERVER_DS
        resp = negotiate(client, snapshots=["a", "b"])
        assert resp.get_json()["common_snapshot"] is None
        assert resp.get_json()["server_copy"] == "raw"

    @pytest.mark.parametrize("which", ["encryption", "encryptionroot"])
    def test_a_zfs_failure_is_500_with_the_snapshot_list_wording_and_the_gate_is_released(
        self, client, authed, zfs, server_props, which
    ):
        with_snapshots(zfs, "a")
        server_props.encryption, server_props.root = "aes-256-gcm", SERVER_DS
        real = server_props.mock.side_effect

        def failing(ds, prop, **kwargs):
            if prop == which:
                raise RuntimeError(SECRET)
            return real(ds, prop, **kwargs)

        server_props.mock.side_effect = failing
        resp = negotiate(client, snapshots=["a"])
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to list server snapshots: {SECRET}"}
        assert authed.gate.active == 0

    def test_the_next_request_is_served_after_a_zfs_failure(self, client, zfs, server_props):
        with_snapshots(zfs, "a")
        server_props.mock.side_effect = [RuntimeError("x"), "off"]
        assert negotiate(client, snapshots=["a"]).status_code == 500
        assert negotiate(client, snapshots=["a"]).get_json()["server_copy"] == "plain"

    def test_a_dry_run_still_computes_it(self, zfs, server_props):
        dry = Authed(_config(dry_run=True))
        with_snapshots(zfs, "a")
        server_props.encryption, server_props.root = "aes-256-gcm", SERVER_DS
        resp = negotiate(dry.client(), snapshots=["a"])
        assert resp.get_json()["server_copy"] == "raw"
        assert_nothing_written(zfs)

    def test_a_dry_run_on_a_missing_dataset_is_none(self, zfs, server_props):
        dry = Authed(_config(dry_run=True))
        assert negotiate(dry.client()).get_json()["server_copy"] == "none"

    def test_the_protocol_version_is_unchanged(self):
        """An additive field of protocol 1: an older client ignores it, an older server omits it."""
        from zfsbackup.protocol import PROTOCOL_VERSION
        assert PROTOCOL_VERSION == 1

    def test_through_the_real_get_command(self, client, zfs, mock_subprocess):
        """Only `Popen` is mocked: the argv and the framing of the two reads."""
        with_snapshots(zfs, "a")
        mock_subprocess.setup_multi((["aes-256-gcm\n"],), ([SERVER_DS + "\n"],))
        resp = negotiate(client, snapshots=["a"])
        assert resp.get_json()["server_copy"] == "raw"
        argvs = [c[0][0][1:] for c in mock_subprocess.call_args_list]
        assert argvs == [
            ["get", "-H", "-o", "value", "encryption", SERVER_DS],
            ["get", "-H", "-o", "value", "encryptionroot", SERVER_DS],
        ]

    def test_through_the_real_get_command_for_a_plain_copy(self, client, zfs, mock_subprocess):
        with_snapshots(zfs, "a")
        mock_subprocess.setup_multi((["off\n"],))
        assert negotiate(client, snapshots=["a"]).get_json()["server_copy"] == "plain"
        assert len(mock_subprocess.call_args_list) == 1


# ---------------------------------------------------------------------------
# R0 fixes: D7 on a placeholder, the claim, ancestors, the TTL, the size limit
# ---------------------------------------------------------------------------

BUSY = {"error": "busy", "retry": True}
OTHER_DS = "tank/data"          # maps to the same server dataset as pool/data
PARENT_DS = "pool/backups/client1"


@pytest.fixture
def ancestors(mocker):
    """`zfs.get.value` (the `receive_resume_token` of an ancestor): `.token` maps a dataset name to
    its token (default: none)."""
    holder = MagicMock()
    holder.token = {}
    holder.mock = mocker.patch(
        "zfsbackup.api.zfs.get.value",
        side_effect=lambda ds, prop, **kw: holder.token.get(str(ds)),
    )
    return holder


def existing(zfs, *names):
    """Only `names` exist (everything else is missing)."""
    zfs["exists"].side_effect = lambda ds: str(ds) in names


class TestPlaceholderOwnership:
    """A server dataset that exists WITHOUT a recorded owner (a placeholder parent made for a child,
    or a receive that has not yet applied its `-o` values) is still spoken for by the client dataset
    that negotiated it first."""

    def test_b_is_refused_on_the_placeholder_and_not_stamped_and_the_owner_still_negotiates(
        self, client, authed, zfs
    ):
        assert negotiate(client, dataset="pool/data").status_code == 200
        assert negotiate(client, dataset=OTHER_DS).status_code == 409           # while it is missing
        zfs["exists"].return_value = True                                        # the placeholder appears
        zfs["props"].return_value = {}                                           # ... with no owner
        zfs["set"].reset_mock()

        refused = negotiate(client, dataset=OTHER_DS)

        assert refused.status_code == 409
        assert refused.get_json() == {"error": (
            f"server dataset {SERVER_DS} already holds backups of 'pool/data'; "
            f"refusing to mix in '{OTHER_DS}'")}
        zfs["set"].assert_not_called()                          # B is not stamped
        assert pending_configs(authed.app)[SERVER_DS][1] == "pool/data"          # A's entry is intact
        owner = negotiate(client, dataset="pool/data")          # the rightful owner
        assert owner.status_code == 200
        assert zfs["set"].call_args.args[1][PROP_SOURCE_DATASET] == "pool/data"
        assert dict(pending_configs(authed.app)) == {}          # dropped: the dataset has its own config now

    def test_a_recorded_owner_beats_a_stale_pending_entry(self, client, authed, zfs):
        negotiate(client, dataset="pool/data")
        zfs["exists"].return_value = True
        zfs["props"].return_value = {PROP_SOURCE_DATASET: OTHER_DS}
        assert negotiate(client, dataset=OTHER_DS).status_code == 200
        assert zfs["set"].call_args.args[1][PROP_SOURCE_DATASET] == OTHER_DS

    def test_another_owner_on_an_existing_dataset_is_the_ownership_checks_409(self, client, zfs):
        negotiate(client, dataset="pool/data")
        zfs["exists"].return_value = True
        zfs["props"].return_value = {PROP_SOURCE_DATASET: "pool/data"}
        resp = negotiate(client, dataset=OTHER_DS)
        assert resp.status_code == 409
        zfs["set"].assert_not_called()

    def test_the_owner_read_failure_is_a_500_and_writes_nothing(self, client, authed, zfs):
        negotiate(client, dataset="pool/data")
        zfs["exists"].return_value = True
        zfs["props"].side_effect = [{}, RuntimeError(SECRET)]       # the ownership check, then the owner read
        resp = negotiate(client, dataset=OTHER_DS)
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to check {SERVER_DS}: {SECRET}"}
        zfs["set"].assert_not_called()
        assert streams_in_progress(authed.app) == set()

    def test_a_placeholder_nobody_negotiated_for_is_stamped(self, client, zfs):
        """No pending entry: nothing speaks for it, the first negotiator owns it."""
        zfs["exists"].return_value = True
        assert negotiate(client, dataset=OTHER_DS).status_code == 200
        assert zfs["set"].call_args.args[1][PROP_SOURCE_DATASET] == OTHER_DS

    def test_two_racing_negotiates_give_one_winner_and_the_loser_changes_nothing(
        self, authed, zfs
    ):
        inside_create, release = threading.Event(), threading.Event()

        def slow_create(*args, **kwargs):
            inside_create.set()
            assert release.wait(20)

        zfs["create"].side_effect = slow_create
        results = {}

        def winner():
            results["winner"] = negotiate(authed.client(), dataset="pool/data").status_code

        thread = threading.Thread(target=winner, daemon=True)
        thread.start()
        try:
            assert inside_create.wait(20)
            loser = negotiate(authed.client(), dataset=OTHER_DS)       # the same server dataset, mid-create
            assert loser.status_code in (409, 503)
            assert loser.get_json() in (BUSY, {"error": (
                f"server dataset {SERVER_DS} already holds backups of 'pool/data'; "
                f"refusing to mix in '{OTHER_DS}'")})
            assert zfs["create"].call_count == 1                      # the loser created nothing
            zfs["set"].assert_not_called()
        finally:
            release.set()
            thread.join(20)
        assert results == {"winner": 200}
        assert pending_configs(authed.app)[SERVER_DS][1] == "pool/data"
        assert streams_in_progress(authed.app) == set()
        assert authed.gate.active == 0

    def test_a_failed_create_removes_only_its_own_entry(self, client, authed, zfs):
        """Another client dataset took the name over while this create was running (its entry had
        expired): the failure must not drop the new owner's entry."""
        claim, _lookup, forget = pending_api(authed.app)
        claim("pool/backups/client1/other", "cfg-o", "pool/other")

        def create_fails_after_a_takeover(*args, **kwargs):
            forget(SERVER_DS)
            claim(SERVER_DS, "cfg-new-owner", OTHER_DS)
            raise RuntimeError(SECRET)

        zfs["create"].side_effect = create_fails_after_a_takeover
        assert negotiate(client, dataset="pool/data").status_code == 500
        assert pending_configs(authed.app)[SERVER_DS] == ("cfg-new-owner", OTHER_DS)
        assert "pool/backups/client1/other" in pending_configs(authed.app)

    def test_a_failed_create_removes_its_entry(self, client, authed, zfs):
        zfs["create"].side_effect = RuntimeError(SECRET)
        assert negotiate(client, dataset="pool/data").status_code == 500
        assert dict(pending_configs(authed.app)) == {}
        assert dict(pending_deadlines(authed.app)) == {}


class TestNegotiateClaim:
    """Negotiate holds the server dataset's stream claim for its whole duration: a stream or another
    negotiate of the name is told `busy`, and nothing is written meanwhile."""

    def test_a_name_with_a_stream_in_progress_is_busy_and_nothing_is_written(self, client, authed, zfs):
        zfs["exists"].return_value = True
        streams_in_progress(authed.app).add(SERVER_DS)
        resp = negotiate(client)
        assert resp.status_code == 503 and resp.get_json() == BUSY
        assert_nothing_written(zfs)
        zfs["list"].assert_not_called()
        assert streams_in_progress(authed.app) == {SERVER_DS}       # the stream's claim is not ours to drop
        assert authed.gate.active == 0
        streams_in_progress(authed.app).discard(SERVER_DS)
        assert negotiate(client).status_code == 200
        assert streams_in_progress(authed.app) == set()

    def test_a_missing_dataset_with_a_stream_in_progress_is_busy_too(self, client, authed, zfs):
        streams_in_progress(authed.app).add(SERVER_DS)
        assert negotiate(client).get_json() == BUSY
        assert dict(pending_configs(authed.app)) == {}

    def test_the_busy_reply_is_decided_before_any_zfs_call(self, client, authed, zfs):
        streams_in_progress(authed.app).add(SERVER_DS)
        negotiate(client)
        for name in ("exists", "list", "create", "set", "props"):
            assert not zfs[name].called, name

    def test_another_name_is_not_blocked(self, client, authed, zfs):
        streams_in_progress(authed.app).add(SERVER_DS)
        assert negotiate(client, dataset="pool/other").status_code == 200

    def test_the_claim_is_held_while_negotiate_writes(self, authed, zfs):
        seen = []
        zfs["create"].side_effect = lambda *a, **k: seen.append(set(streams_in_progress(authed.app)))
        assert negotiate(authed.client()).status_code == 200
        # the dataset itself and the missing ancestor its `create -p` will make
        assert seen == [{SERVER_DS, PARENT_DS}]

    @pytest.mark.parametrize("how", ["ok", "bad-body", "conflict", "create-fails", "set-fails",
                                     "value-error", "list-fails", "dry-run", "ancestor-unsettled"])
    def test_the_claim_is_released_after_every_outcome(self, zfs, how, ancestors):
        app = Authed(_config(dry_run=(how == "dry-run")))
        body = body_with()
        if how == "bad-body":
            body = body_with(config="")
        elif how == "conflict":
            zfs["exists"].return_value = True
            zfs["props"].return_value = {PROP_CLIENT_ID: "someone-else"}
        elif how == "create-fails":
            zfs["create"].side_effect = RuntimeError(SECRET)
        elif how == "set-fails":
            zfs["exists"].return_value = True
            zfs["set"].side_effect = RuntimeError(SECRET)
        elif how == "value-error":
            zfs["create"].side_effect = ValueError(SECRET)
        elif how == "list-fails":
            zfs["exists"].return_value = True
            zfs["list"].side_effect = RuntimeError(SECRET)
        elif how == "ancestor-unsettled":
            existing(zfs, PARENT_DS + "/a")
            ancestors.token[PARENT_DS + "/a"] = "1-token"
        url = f"/backup/{CLIENT_ID}/pool/a/b/negotiate" if how == "ancestor-unsettled" else URL
        app.client().post(url, json=body)
        assert streams_in_progress(app.app) == set(), how
        assert app.gate.active == 0

    def test_an_unexpected_exception_still_releases_the_claim(self, authed, zfs):
        zfs["create"].side_effect = KeyboardInterrupt()
        with pytest.raises(KeyboardInterrupt):
            negotiate(authed.client())
        assert streams_in_progress(authed.app) == set()
        assert dict(pending_configs(authed.app)) == {}               # and its entry was withdrawn too

    def test_a_stream_arriving_during_a_negotiate_is_busy(self, mocker):
        """The reverse: negotiate is inside `zfs.create`; a stream of the same name is told to retry."""
        stream = Stream(mocker)
        stream.exists.return_value = False
        inside, release = threading.Event(), threading.Event()

        def slow_create(*a, **k):
            inside.set()
            assert release.wait(20)

        stream.create.side_effect = slow_create
        result = {}
        thread = threading.Thread(
            target=lambda: result.update(code=negotiate(stream.authed.client()).status_code), daemon=True)
        thread.start()
        try:
            assert inside.wait(20)
            stream.run("fresh")
            assert stream.sent == [BUSY]
            stream.receive.assert_not_called()
        finally:
            release.set()
            thread.join(20)
        assert result == {"code": 200}
        assert streams_in_progress(stream.app) == set()

    def test_a_negotiate_during_a_stream_is_busy(self, mocker):
        from test_zfsbackup_api import HeldStream

        stream = Stream(mocker)
        with HeldStream(stream) as held:
            resp = negotiate(stream.authed.client())
            assert resp.status_code == 503 and resp.get_json() == BUSY
            stream.set.assert_not_called()
            held.release()
        assert held.errors == []
        assert negotiate(stream.authed.client()).status_code == 200


def ancestor_negotiate(client, dataset):
    return post_json(client, body_with(), url=f"/backup/{CLIENT_ID}/{dataset}/negotiate")


UNSETTLED = {"retry": True}


class TestUnsettledAncestor:
    """A child's negotiate must not create (with `create -p`) an ancestor that a stream is working
    on, that was negotiated and not received, or that holds a cut first receive."""

    def assert_unsettled(self, resp, ancestor):
        assert resp.status_code == 503
        assert resp.get_json() == {
            "error": f"{ancestor} has not settled yet; retry later", "retry": True,
            "reason": "unsettled_ancestor", "ancestor": ancestor}

    def test_a_claimed_ancestor(self, client, authed, zfs, ancestors):
        streams_in_progress(authed.app).add(PARENT_DS + "/a")
        self.assert_unsettled(ancestor_negotiate(client, "pool/a/b"), PARENT_DS + "/a")
        assert_nothing_written(zfs)
        assert dict(pending_configs(authed.app)) == {}

    def test_an_ancestor_with_a_pending_entry(self, client, authed, zfs, ancestors):
        assert ancestor_negotiate(client, "pool/a").status_code == 200
        zfs["create"].reset_mock()
        self.assert_unsettled(ancestor_negotiate(client, "pool/a/b"), PARENT_DS + "/a")
        zfs["create"].assert_not_called()
        assert list(pending_configs(authed.app)) == [PARENT_DS + "/a"]

    def test_the_grandparent_counts_too(self, client, authed, zfs, ancestors):
        assert ancestor_negotiate(client, "pool/a").status_code == 200
        self.assert_unsettled(ancestor_negotiate(client, "pool/a/b/c"), PARENT_DS + "/a")

    def test_the_nearest_unsettled_ancestor_is_named(self, client, authed, zfs, ancestors):
        ancestor_negotiate(client, "pool/a")
        ancestor_negotiate(client, "pool/a/b")                       # refused: a is pending
        streams_in_progress(authed.app).add(PARENT_DS + "/a/b")
        self.assert_unsettled(ancestor_negotiate(client, "pool/a/b/c"), PARENT_DS + "/a/b")

    def test_an_existing_ancestor_with_a_resume_token(self, client, zfs, ancestors):
        existing(zfs, PARENT_DS + "/a")
        ancestors.token[PARENT_DS + "/a"] = "1-abc"
        self.assert_unsettled(ancestor_negotiate(client, "pool/a/b"), PARENT_DS + "/a")
        assert ancestors.mock.call_args.args[1] == "receive_resume_token"
        assert_nothing_written(zfs)

    def test_an_existing_ancestor_without_a_token_proceeds(self, client, zfs, ancestors):
        existing(zfs, PARENT_DS + "/a")
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200
        assert str(zfs["create"].call_args.args[0]) == PARENT_DS + "/a"

    def test_ancestors_that_do_not_exist_proceed(self, client, zfs, ancestors):
        existing(zfs)
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        assert str(zfs["create"].call_args.args[0]) == PARENT_DS + "/a/b"

    def test_the_calls_are_exactly_the_ancestors_between_the_dataset_and_the_id_directory(
        self, client, zfs, ancestors
    ):
        existing(zfs, PARENT_DS, PARENT_DS + "/a", PARENT_DS + "/a/b")
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        assert [str(c.args[0]) for c in zfs["exists"].call_args_list] == [
            PARENT_DS + "/a/b/c", PARENT_DS + "/a/b", PARENT_DS + "/a", PARENT_DS]
        assert [str(c.args[0]) for c in ancestors.mock.call_args_list] == [
            PARENT_DS + "/a/b", PARENT_DS + "/a", PARENT_DS]        # the target itself is never read
        assert {c.args[1] for c in ancestors.mock.call_args_list} == {"receive_resume_token"}

    @pytest.mark.parametrize("dataset, extra_exists", [
        ("pool", 0),            # `<target>/<id>`: its parent is the target, which is not checked
        ("pool/a", 1),          # one ancestor: `<target>/<id>`
        ("pool/a/b", 2),
        ("pool/a/b/c", 3),
    ])
    def test_the_cost_is_one_exists_per_ancestor(self, client, zfs, ancestors, dataset, extra_exists):
        assert ancestor_negotiate(client, dataset).status_code == 200
        assert zfs["exists"].call_count == 1 + extra_exists
        assert ancestors.mock.call_count == 0                       # nothing exists: no `zfs get`

    def test_an_existing_dataset_skips_the_check(self, client, zfs, ancestors):
        zfs["exists"].return_value = True
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        assert {str(c.args[0]) for c in zfs["exists"].call_args_list} == {PARENT_DS + "/a/b/c"}
        ancestors.mock.assert_not_called()

    def test_a_dry_run_skips_the_check(self, zfs, ancestors):
        dry = Authed(_config(dry_run=True))
        streams_in_progress(dry.app).add(PARENT_DS + "/a")
        assert ancestor_negotiate(dry.client(), "pool/a/b").status_code == 200
        assert {str(c.args[0]) for c in zfs["exists"].call_args_list} == {PARENT_DS + "/a/b"}
        ancestors.mock.assert_not_called()

    @pytest.mark.parametrize("failing", ["get", "exists"])
    def test_a_zfs_failure_is_a_500_and_the_details_stay_in_the_log(
        self, client, authed, zfs, ancestors, caplog, failing
    ):
        if failing == "get":
            existing(zfs, PARENT_DS + "/a")
            ancestors.mock.side_effect = RuntimeError(SECRET)
        else:
            zfs["exists"].side_effect = [False, RuntimeError(SECRET)]
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            resp = ancestor_negotiate(client, "pool/a/b")
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to check the parents of {PARENT_DS}/a/b"}
        assert SECRET not in resp.get_data(as_text=True)
        assert any(SECRET in r.getMessage() for r in caplog.records)
        assert_nothing_written(zfs)
        assert streams_in_progress(authed.app) == set() and authed.gate.active == 0

    def test_the_child_is_served_once_the_parent_has_settled(self, client, authed, zfs, ancestors):
        ancestor_negotiate(client, "pool/a")
        assert ancestor_negotiate(client, "pool/a/b").status_code == 503
        pending_api(authed.app)[2](PARENT_DS + "/a")                 # the first receive finished
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200

    def test_a_sibling_is_not_an_ancestor(self, client, zfs, ancestors):
        ancestor_negotiate(client, "pool/a")
        assert ancestor_negotiate(client, "pool/ab").status_code == 200
        assert ancestor_negotiate(client, "pool/b/c").status_code == 200

    def test_a_removed_parent_blocks_its_child_only_until_the_ttl(self, client, zfs, ancestors, mocker):
        """The parent was negotiated, then dropped from the client's configuration: nothing will ever
        receive it. Its entry expires and the child proceeds."""
        from zfsbackup.api import PENDING_CONFIG_TTL

        clock = patch_api_clock(mocker)
        assert ancestor_negotiate(client, "pool/a").status_code == 200
        clock.advance(PENDING_CONFIG_TTL - 1)
        assert ancestor_negotiate(client, "pool/a/b").status_code == 503
        clock.advance(2)
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200


class TestPendingTtl:
    """A pending configuration is honoured for `PENDING_CONFIG_TTL` seconds of the monotonic clock after
    the negotiate that made or refreshed it. Every reader ignores and drops an expired entry."""

    @pytest.fixture
    def clock(self, mocker):
        return patch_api_clock(mocker)

    def keys_agree(self, app):
        assert set(pending_configs(app)) == set(pending_deadlines(app))

    def test_the_ttl_is_ten_minutes(self):
        from zfsbackup.api import PENDING_CONFIG_TTL
        assert PENDING_CONFIG_TTL == 600

    def test_an_entry_gets_a_deadline_one_ttl_ahead(self, client, authed, zfs, clock):
        from zfsbackup.api import PENDING_CONFIG_TTL
        negotiate(client)
        assert pending_deadlines(authed.app)[SERVER_DS] == clock.now + PENDING_CONFIG_TTL

    def test_the_409_reader(self, client, authed, zfs, clock):
        from zfsbackup.api import PENDING_CONFIG_TTL
        negotiate(client, dataset="pool/data")
        clock.advance(PENDING_CONFIG_TTL - 1)
        assert negotiate(client, dataset=OTHER_DS).status_code == 409
        clock.advance(2)
        assert negotiate(client, dataset=OTHER_DS).status_code == 200      # the expired entry is replaced
        assert pending_configs(authed.app)[SERVER_DS][1] == OTHER_DS
        self.keys_agree(authed.app)

    def test_the_owner_check_on_an_existing_dataset_reads_the_expired_entry_as_none(
        self, client, authed, zfs, clock
    ):
        from zfsbackup.api import PENDING_CONFIG_TTL
        negotiate(client, dataset="pool/data")
        zfs["exists"].return_value = True
        zfs["props"].return_value = {}
        clock.advance(PENDING_CONFIG_TTL - 1)
        assert negotiate(client, dataset=OTHER_DS).status_code == 409
        clock.advance(2)
        assert negotiate(client, dataset=OTHER_DS).status_code == 200
        assert dict(pending_configs(authed.app)) == {}
        self.keys_agree(authed.app)

    def test_the_unsettled_ancestor_reader(self, client, authed, zfs, clock):
        from zfsbackup.api import PENDING_CONFIG_TTL
        ancestor_negotiate(client, "pool/a")
        clock.advance(PENDING_CONFIG_TTL - 1)
        assert ancestor_negotiate(client, "pool/a/b").status_code == 503
        clock.advance(2)
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200
        assert PARENT_DS + "/a" not in pending_configs(authed.app)         # dropped by the reader
        self.keys_agree(authed.app)

    def test_the_lookup_reader_drops_what_it_ignores(self, client, authed, zfs, clock):
        from zfsbackup.api import PENDING_CONFIG_TTL
        _claim, lookup, _forget = pending_api(authed.app)
        negotiate(client)
        clock.advance(PENDING_CONFIG_TTL - 1)
        assert lookup(SERVER_DS) is not None
        clock.advance(2)
        assert lookup(SERVER_DS) is None
        assert dict(pending_configs(authed.app)) == {} and dict(pending_deadlines(authed.app)) == {}

    def test_the_claim_reader_replaces_an_expired_entry_of_another_dataset(self, client, authed, zfs, clock):
        from zfsbackup.api import PENDING_CONFIG_TTL
        claim, _lookup, _forget = pending_api(authed.app)
        assert claim(SERVER_DS, "cfg-a", "pool/data") is None
        clock.advance(PENDING_CONFIG_TTL - 1)
        assert claim(SERVER_DS, "cfg-b", OTHER_DS) == "pool/data"
        clock.advance(2)
        assert claim(SERVER_DS, "cfg-b", OTHER_DS) is None
        assert pending_configs(authed.app)[SERVER_DS] == ("cfg-b", OTHER_DS)

    def test_a_renegotiate_refreshes_the_deadline(self, client, authed, zfs, clock):
        """500 s in, negotiate again: still live at 1000 s after the first (500 s after the refresh),
        expired 700 s after the refresh."""
        negotiate(client, dataset="pool/data")
        clock.advance(500)
        negotiate(client, dataset="pool/data")
        clock.advance(500)
        assert negotiate(client, dataset=OTHER_DS).status_code == 409           # live
        clock.advance(200)
        assert negotiate(client, dataset=OTHER_DS).status_code == 200            # expired

    def test_the_keys_of_both_maps_always_agree(self, client, authed, zfs, clock, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_MAX", 3)
        claim, lookup, forget = pending_api(authed.app)
        for n in range(6):                                  # past the cap: the oldest are evicted
            negotiate(client, dataset=f"pool/d{n}")
            self.keys_agree(authed.app)
        assert len(pending_configs(authed.app)) == 3
        clock.advance(100)
        forget("pool/backups/client1/d4")
        self.keys_agree(authed.app)
        negotiate(client, dataset="pool/d9")
        self.keys_agree(authed.app)
        clock.advance(10_000)
        lookup("pool/backups/client1/d5")                    # an expired read drops its entry
        self.keys_agree(authed.app)
        assert "pool/backups/client1/d5" not in pending_deadlines(authed.app)
        zfs["create"].side_effect = RuntimeError(SECRET)
        negotiate(client, dataset="pool/d10")                # a failed create withdraws its entry
        self.keys_agree(authed.app)

    def test_eviction_drops_the_deadline_with_the_entry(self, client, authed, zfs, mocker):
        mocker.patch("zfsbackup.api.PENDING_CONFIGS_MAX", 2)
        for n in range(4):
            negotiate(client, dataset=f"pool/d{n}")
        assert sorted(pending_deadlines(authed.app)) == [
            "pool/backups/client1/d2", "pool/backups/client1/d3"]

    def test_the_stream_reader(self, mocker):
        from zfsbackup.api import PENDING_CONFIG_TTL

        clock = patch_api_clock(mocker)
        for age, served in ((PENDING_CONFIG_TTL - 1, True), (PENDING_CONFIG_TTL + 1, False)):
            stream = Stream(mocker)
            stream.exists.return_value = False
            negotiate(stream.authed.client())
            clock.advance(age)
            stream.run("fresh")
            if served:
                assert stream.sent == [{"status": "ok"}]
                stream.receive.assert_called_once()
            else:
                assert "negotiate again" in stream.sent[0]["error"]
                stream.receive.assert_not_called()
                assert dict(pending_configs(stream.app)) == {}
            clock.advance(-age)

    def test_a_long_transfer_is_unaffected(self, mocker):
        """The values were copied at the start of the stream; the entry may expire mid-transfer."""
        from zfsbackup.api import PENDING_CONFIG_TTL

        clock = patch_api_clock(mocker)
        stream = Stream(mocker)
        stream.exists.return_value = False
        negotiate(stream.authed.client())
        stream.writer.write.side_effect = lambda data: clock.advance(PENDING_CONFIG_TTL * 3)
        stream.run("fresh")
        assert stream.sent == [{"status": "ok"}]
        assert dict(pending_configs(stream.app)) == {}


def config_of_canonical_length(n):
    """A `DatasetConfig` whose canonical property value is exactly `n` characters (base64: a multiple
    of 4), made by padding the dataset name."""
    for pad in range(max(n * 3 // 4 - 200, 1), n * 3 // 4 + 200):
        cfg = DatasetConfig(name="pool/" + "x" * pad)
        value = cfg.to_property()
        if len(value) == n:
            return cfg, value
        if len(value) > n:
            break
    raise AssertionError(f"no config of canonical length {n}")


class TestConfigSizeLimit:
    """The canonical config is a ZFS user-property value (negotiate, then `receive -o`): one that cannot
    be stored is refused at negotiate with the usual 400, before anything is written (the real limit is
    pinned in test_zfsbackup_real.py)."""

    @pytest.mark.parametrize("length, accepted", [(8184, True), (8188, True), (8192, False), (8196, False)])
    def test_around_the_limit(self, client, authed, zfs, length, accepted):
        _cfg, value = config_of_canonical_length(length)
        resp = negotiate(client, config=value)
        if accepted:
            assert resp.status_code == 200, resp.get_data(as_text=True)
            assert pending_configs(authed.app)[SERVER_DS][0] == value
        else:
            assert resp.status_code == 400
            assert resp.get_json() == {"error": CONFIG_TOO_LARGE_ERROR}
            assert_nothing_written(zfs)
            assert dict(pending_configs(authed.app)) == {}
        assert streams_in_progress(authed.app) == set()

    def test_the_limit_is_strict_at_the_comparison(self, mocker):
        """8188 characters with a limit of 8188 is refused, with one of 8189 accepted: `>=`."""
        _cfg, value = config_of_canonical_length(8188)
        import zfsbackup.api as api
        mocker.patch.object(api, "ZFS_USER_PROPERTY_MAXLEN", 8189)
        assert canonical_dataset_config(value) == value
        mocker.patch.object(api, "ZFS_USER_PROPERTY_MAXLEN", 8188)
        with pytest.raises(ValueError, match="too large"):
            canonical_dataset_config(value)

    def test_the_limit_is_8_kib(self):
        from zfsbackup.api import ZFS_USER_PROPERTY_MAXLEN
        assert ZFS_USER_PROPERTY_MAXLEN == 8192

    def test_it_is_the_canonical_form_that_counts(self):
        """The client's own spelling may be longer (indented) than the canonical one that is stored."""
        cfg, value = config_of_canonical_length(8188)
        padded = enc(json.dumps(json.loads(base64.b64decode(value)), indent=4))
        assert len(padded) > 8192
        assert canonical_dataset_config(padded) == value

    def test_the_message_names_the_problem_and_never_the_input(self, client, caplog):
        _cfg, value = config_of_canonical_length(8192)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            assert negotiate(client, config=value).status_code == 400
        assert "too large" in caplog.text
        assert value not in caplog.text

    def test_a_dry_run_refuses_it_too(self, zfs):
        dry = Authed(_config(dry_run=True))
        _cfg, value = config_of_canonical_length(8192)
        assert negotiate(dry.client(), config=value).status_code == 400

    def test_an_oversized_config_for_an_existing_dataset_sets_nothing(self, client, zfs):
        zfs["exists"].return_value = True
        _cfg, value = config_of_canonical_length(8192)
        assert negotiate(client, config=value).status_code == 400
        assert_nothing_written(zfs)


class TestAncestorClaims:
    """The ancestors a `create -p` will create are claimed (like the dataset) for the duration of the
    create: a stream or negotiate of one of them, or a sibling that would create the same parent,
    is told busy."""

    def held_during_create(self, authed, zfs):
        seen = []
        zfs["create"].side_effect = lambda *a, **k: seen.append(set(streams_in_progress(authed.app)))
        return seen

    def test_all_missing_ancestors_are_claimed_during_the_create(self, client, authed, zfs, ancestors):
        existing(zfs)
        seen = self.held_during_create(authed, zfs)
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        assert seen == [{PARENT_DS + "/a/b/c", PARENT_DS + "/a/b", PARENT_DS + "/a", PARENT_DS}]
        assert streams_in_progress(authed.app) == set()

    def test_an_existing_ancestor_is_not_claimed(self, client, authed, zfs, ancestors):
        existing(zfs, PARENT_DS, PARENT_DS + "/a")
        seen = self.held_during_create(authed, zfs)
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        assert seen == [{PARENT_DS + "/a/b/c", PARENT_DS + "/a/b"}]       # only the missing one, and the leaf

    def test_a_dry_run_claims_nothing_but_the_name_and_creates_nothing(self, zfs, ancestors):
        dry = Authed(_config(dry_run=True))
        seen = []

        def exists(ds):
            seen.append((str(ds), set(streams_in_progress(dry.app))))
            return False

        zfs["exists"].side_effect = exists
        assert ancestor_negotiate(dry.client(), "pool/a/b").status_code == 200
        zfs["create"].assert_not_called()
        assert {name for name, _ in seen} == {PARENT_DS + "/a/b"}          # no ancestor is even read
        assert all(held == {PARENT_DS + "/a/b"} for _, held in seen)        # only the dataset's own claim
        assert streams_in_progress(dry.app) == set()

    @pytest.mark.parametrize("held", ["a", "a/b"])
    def test_a_missing_ancestor_held_by_a_stream_or_negotiate_is_busy_and_nothing_is_created(
        self, client, authed, zfs, ancestors, held
    ):
        # a held name that does not exist and is not pending: _ancestors_state reports it unsettled
        # (claimed) or, for a held one reached later, the claim of the missing list fails
        existing(zfs)
        streams_in_progress(authed.app).add(f"{PARENT_DS}/{held}")
        resp = ancestor_negotiate(client, "pool/a/b/c")
        assert resp.status_code == 503 and resp.get_json()["retry"] is True
        assert_nothing_written(zfs)
        assert dict(pending_configs(authed.app)) == {}

    def test_a_claim_lost_to_a_race_is_busy_with_no_create(self, client, authed, zfs, ancestors):
        """The ancestor is free when the state is read and taken before it is claimed."""
        existing(zfs)
        real_exists = zfs["exists"].side_effect
        taken = []

        def exists(ds):
            answer = real_exists(ds)
            if str(ds) == PARENT_DS + "/a" and not taken:       # read, then a stream grabs it
                taken.append(True)
                streams_in_progress(authed.app).add(PARENT_DS + "/a")
            return answer

        zfs["exists"].side_effect = exists
        resp = ancestor_negotiate(client, "pool/a/b")
        assert resp.status_code == 503 and resp.get_json() == BUSY
        assert_nothing_written(zfs)
        assert dict(pending_configs(authed.app)) == {}
        assert streams_in_progress(authed.app) == {PARENT_DS + "/a"}      # theirs; ours were released

    def test_a_failure_claiming_the_second_ancestor_releases_the_first(self, client, authed, zfs, ancestors):
        existing(zfs)
        real_exists = zfs["exists"].side_effect
        # missing, nearest first: .../a/b then .../a then PARENT_DS. Take the LAST read (PARENT_DS) after the
        # nearer ones were read, so the first claims succeed and a later one fails.
        def exists(ds):
            answer = real_exists(ds)
            if str(ds) == PARENT_DS:
                streams_in_progress(authed.app).add(PARENT_DS)
            return answer

        zfs["exists"].side_effect = exists
        resp = ancestor_negotiate(client, "pool/a/b/c")
        assert resp.status_code in (503,)
        assert_nothing_written(zfs)
        # only the foreign claim remains: everything this negotiate took was released
        assert streams_in_progress(authed.app) == {PARENT_DS}

    def test_the_ancestors_are_released_after_a_create_that_raises(self, client, authed, zfs, ancestors):
        existing(zfs)
        zfs["create"].side_effect = RuntimeError(SECRET)
        assert ancestor_negotiate(client, "pool/a/b").status_code == 500
        assert streams_in_progress(authed.app) == set()
        assert dict(pending_configs(authed.app)) == {}

    def test_the_ancestors_are_released_after_a_409_from_the_pending_claim(self, client, authed, zfs, ancestors):
        claim, _lookup, _forget = pending_api(authed.app)
        existing(zfs)
        real_exists = zfs["exists"].side_effect
        done = []

        def exists(ds):
            answer = real_exists(ds)
            if str(ds) == PARENT_DS and not done:        # after the early pending check: a rival appears
                done.append(True)
                claim(PARENT_DS + "/a/b", "cfg-rival", OTHER_DS)
            return answer

        zfs["exists"].side_effect = exists
        resp = ancestor_negotiate(client, "pool/a/b")
        assert resp.status_code == 409
        assert "refusing to mix in" in resp.get_json()["error"]
        zfs["create"].assert_not_called()
        assert streams_in_progress(authed.app) == set()

    def test_the_ancestors_are_released_after_success(self, client, authed, zfs, ancestors):
        existing(zfs)
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        assert streams_in_progress(authed.app) == set()

    def test_a_concurrent_sibling_that_would_create_the_same_parent_is_busy(self, authed, zfs, ancestors):
        existing(zfs)
        inside, release = threading.Event(), threading.Event()

        def slow_create(*args, **kwargs):
            inside.set()
            assert release.wait(20)

        zfs["create"].side_effect = slow_create
        result = {}
        thread = threading.Thread(
            target=lambda: result.update(code=ancestor_negotiate(authed.client(), "pool/a/b").status_code),
            daemon=True)
        thread.start()
        try:
            assert inside.wait(20)
            sibling = ancestor_negotiate(authed.client(), "pool/a/c")      # would create `.../a` too
            assert sibling.status_code == 503
            assert sibling.get_json()["retry"] is True
            assert zfs["create"].call_count == 1
        finally:
            release.set()
            thread.join(20)
        assert result == {"code": 200}
        assert streams_in_progress(authed.app) == set()
        existing(zfs, PARENT_DS + "/a")                                    # the parent exists now
        # `.../a` exists now and is not pending (only the leaf `.../a/b` is): the sibling proceeds
        assert ancestor_negotiate(authed.client(), "pool/a/c").status_code == 200


class TestPlainChildUnderARawParent:
    """L2: `raw` means the dataset is its OWN encryption root. A plain child received under a raw
    parent inherits the parent's root and is plain, whatever tree the root is in."""

    def test_a_child_whose_root_is_its_raw_parent_is_plain(self, client, zfs, server_props):
        with_snapshots(zfs, "a")
        server_props.encryption = "aes-256-gcm"
        server_props.root = f"{ID_TREE}/a"                          # the parent, inside <target>/<id>/
        resp = post_json(client, body_with(snapshots=["a"]), url=f"/backup/{CLIENT_ID}/pool/a/b/negotiate")
        assert resp.get_json()["server_copy"] == "plain"
        assert resp.get_json()["server_dataset"] == f"{ID_TREE}/a/b"

    def test_the_same_dataset_is_raw_once_it_is_its_own_root(self, client, zfs, server_props):
        with_snapshots(zfs, "a")
        server_props.encryption = "aes-256-gcm"
        server_props.root = f"{ID_TREE}/a/b"
        resp = post_json(client, body_with(snapshots=["a"]), url=f"/backup/{CLIENT_ID}/pool/a/b/negotiate")
        assert resp.get_json()["server_copy"] == "raw"

    @pytest.mark.parametrize("root", ["pool/backups", "pool", ID_TREE, f"{ID_TREE}/other"])
    def test_a_root_anywhere_else_is_plain(self, client, zfs, server_props, root):
        with_snapshots(zfs, "a")
        server_props.encryption, server_props.root = "aes-256-gcm", root
        assert negotiate(client, snapshots=["a"]).get_json()["server_copy"] == "plain"


class TestPartialAncestorRule:
    """An existing ancestor holds its children up only with a `receive_resume_token` AND no snapshot (a
    cut FIRST receive). With snapshots (a cut incremental) it does not; without a token it does not."""

    @pytest.fixture
    def parent(self, zfs, ancestors):
        existing(zfs, PARENT_DS + "/a")
        ancestors.token[PARENT_DS + "/a"] = "1-token"
        return PARENT_DS + "/a"

    def snapshots(self, zfs, *names):
        zfs["list"].return_value = [type("S", (), {"short": n})() for n in names]

    def test_a_token_and_no_snapshot_blocks_with_the_reason_and_the_ancestor(self, client, zfs, parent):
        self.snapshots(zfs)
        resp = ancestor_negotiate(client, "pool/a/b")
        assert resp.status_code == 503
        assert resp.get_json() == {
            "error": f"{parent} has not settled yet; retry later", "retry": True,
            "reason": "unsettled_ancestor", "ancestor": parent}
        assert_nothing_written(zfs)

    def test_a_token_and_a_snapshot_does_not_block(self, client, zfs, parent):
        self.snapshots(zfs, "autosnap_1")
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200
        assert str(zfs["create"].call_args.args[0]) == parent

    def test_no_token_does_not_block_and_nothing_is_listed(self, client, zfs, ancestors):
        existing(zfs, PARENT_DS + "/a")
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200
        zfs["list"].assert_not_called()

    def test_the_snapshot_listing_runs_only_for_an_ancestor_with_a_token(self, client, zfs, parent):
        self.snapshots(zfs, "x")
        ancestor_negotiate(client, "pool/a/b")
        (call,) = zfs["list"].call_args_list
        assert str(call.kwargs["roots"]) == parent and call.kwargs["types"] == "snapshot"

    def test_a_listing_failure_is_a_500(self, client, zfs, parent):
        zfs["list"].side_effect = RuntimeError(SECRET)
        resp = ancestor_negotiate(client, "pool/a/b")
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to check the parents of {PARENT_DS}/a/b"}
        assert SECRET not in resp.get_data(as_text=True)

    def test_claimed_and_pending_still_block_whatever_the_snapshots(self, client, authed, zfs, parent):
        self.snapshots(zfs, "x")
        streams_in_progress(authed.app).add(parent)
        assert ancestor_negotiate(client, "pool/a/b").get_json()["reason"] == "unsettled_ancestor"
        streams_in_progress(authed.app).discard(parent)
        pending_api(authed.app)[0](parent, "cfg", "pool/a")
        resp = ancestor_negotiate(client, "pool/a/b")
        assert resp.status_code == 503 and resp.get_json()["ancestor"] == parent


class TestUnsettledWarnings:
    """The server says why a child waits: a WARNING once per ancestor and episode (the remedy for a
    partial one), DEBUG after, and nothing above DEBUG for a claimed (passing) one."""

    def warnings(self, caplog):
        return [r.getMessage() for r in caplog.records
                if r.levelno == logging.WARNING and r.name == "zfsbackup.api"]

    def test_a_pending_ancestor_warns_once_for_three_negotiates(self, client, zfs, ancestors, caplog):
        ancestor_negotiate(client, "pool/a")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(3):
                assert ancestor_negotiate(client, "pool/a/b").status_code == 503
        (message,) = self.warnings(caplog)
        assert PARENT_DS + "/a" in message and "first backup has not completed" in message
        assert sum("still waiting for" in r.getMessage() and r.levelno == logging.DEBUG
                   for r in caplog.records) == 2

    def test_a_partial_ancestor_warns_once_naming_the_remedy(self, client, zfs, ancestors, caplog):
        existing(zfs, PARENT_DS + "/a")
        ancestors.token[PARENT_DS + "/a"] = "1-token"
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(3):
                assert ancestor_negotiate(client, "pool/a/b").status_code == 503
        (message,) = self.warnings(caplog)
        assert f"zfs receive -A {PARENT_DS}/a" in message and "partial state of a first receive" in message

    def test_a_claimed_ancestor_never_warns(self, client, authed, zfs, ancestors, caplog):
        streams_in_progress(authed.app).add(PARENT_DS + "/a")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(3):
                assert ancestor_negotiate(client, "pool/a/b").status_code == 503
        assert self.warnings(caplog) == []
        assert any("is busy" in r.getMessage() and r.levelno == logging.DEBUG for r in caplog.records)

    def test_it_is_announced_again_after_a_child_got_past_it(self, client, authed, zfs, ancestors, caplog):
        claim, _lookup, forget = pending_api(authed.app)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            claim(PARENT_DS + "/a", "cfg", "pool/a")
            assert ancestor_negotiate(client, "pool/a/b").status_code == 503
            assert ancestor_negotiate(client, "pool/a/b").status_code == 503
            assert len(self.warnings(caplog)) == 1
            forget(PARENT_DS + "/a")                                   # settled
            assert ancestor_negotiate(client, "pool/a/b").status_code == 200
            claim(PARENT_DS + "/a", "cfg", "pool/a")                   # a new episode
            existing(zfs)
            assert ancestor_negotiate(client, "pool/a/c").status_code == 503
        assert len(self.warnings(caplog)) == 2

    def test_two_ancestors_are_announced_separately(self, client, authed, zfs, ancestors, caplog):
        claim = pending_api(authed.app)[0]
        claim(PARENT_DS + "/a", "cfg", "pool/a")
        claim(PARENT_DS + "/x", "cfg", "pool/x")
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            ancestor_negotiate(client, "pool/a/b")
            ancestor_negotiate(client, "pool/x/b")
            ancestor_negotiate(client, "pool/a/c")
        messages = self.warnings(caplog)
        assert len(messages) == 2
        assert PARENT_DS + "/a" in messages[0] and PARENT_DS + "/x" in messages[1]

    def test_the_noted_set_is_bounded(self, client, authed, zfs, ancestors, caplog):
        claim = pending_api(authed.app)[0]
        noted = _find_noted(authed.app)
        for n in range(1100):
            claim(f"{PARENT_DS}/p{n}", "cfg", f"pool/p{n}")
            ancestor_negotiate(client, f"pool/p{n}/c")
        assert len(noted) <= 1025


def _find_noted(app):
    from _api_support import _find_in_closures
    return _find_in_closures(app, "_unsettled_noted")


class TestSizeRefusalWording:
    def test_too_large_and_malformed_have_different_messages(self, client, zfs):
        _cfg, big = config_of_canonical_length(8192)
        too_large = negotiate(client, config=big)
        malformed = negotiate(client, config=enc("[1, 2]"))
        assert too_large.status_code == malformed.status_code == 400
        assert too_large.get_json() == {"error": CONFIG_TOO_LARGE_ERROR}
        assert malformed.get_json() == {"error": BAD_CONFIG_ERROR}
        assert "8 KiB" in CONFIG_TOO_LARGE_ERROR
        assert CONFIG_TOO_LARGE_ERROR != BAD_CONFIG_ERROR


# ---------------------------------------------------------------------------
# B-2b: `server_newer` -- what the server holds beyond the common snapshot
# ---------------------------------------------------------------------------


class TestServerNewer:
    """With a common snapshot, negotiate also names the server snapshots newer than it (by
    `createtxg`, never by name), oldest first, guids as decimal strings, at most 64, with their
    number in `server_newer_count`. The client uses them to refuse (or, for exactly one snapshot it
    sent and lost, to allow) the `-F` rollback of server history. Additive: protocol 1."""

    A, B, C, D = (f"autosnap_2026010{i}000000" for i in (1, 2, 3, 4))

    @pytest.fixture(autouse=True)
    def _props(self, server_props):
        return server_props

    def reply(self, client, zfs, server, offered, **kw):
        with_snapshots(zfs, *server)
        resp = negotiate(client, snapshots=offered, **kw)
        assert resp.status_code == 200, resp.get_data(as_text=True)
        return resp.get_json()

    def test_nothing_newer_is_an_empty_list_and_a_zero_count(self, client, zfs):
        body = self.reply(client, zfs, [self.A, self.B], [self.B, self.A])
        assert body["common_snapshot"] == self.B
        assert body["server_newer"] == [] and body["server_newer_count"] == 0

    def test_newer_snapshots_are_listed_oldest_first_with_string_guids(self, client, zfs):
        body = self.reply(client, zfs, [(self.A, 100, 11), (self.B, 200, 22), (self.C, 300, 33)],
                          [self.A])
        assert body["server_newer"] == [
            {"name": self.B, "guid": "22"}, {"name": self.C, "guid": "33"},
        ]
        assert body["server_newer_count"] == 2
        assert all(isinstance(e["guid"], str) for e in body["server_newer"])

    def test_a_guid_above_two_to_the_fifty_third_survives_exactly(self, client, zfs):
        big = 2 ** 64 - 1
        body = self.reply(client, zfs, [(self.A, 1, 5), (self.B, 2, big)], [self.A])
        assert body["server_newer"] == [{"name": self.B, "guid": "18446744073709551615"}]

    def test_the_order_is_createtxg_not_the_name(self, client, zfs):
        """B's name is older than C's but C was created first; the common snapshot is B (by name)."""
        body = self.reply(client, zfs, [(self.A, 100, 1), (self.B, 300, 2), (self.C, 200, 3)],
                          [self.A])
        assert [e["name"] for e in body["server_newer"]] == [self.C, self.B]
        # a name that sorts after the common one but was created BEFORE it is not newer
        body = self.reply(client, zfs, [(self.A, 100, 1), (self.B, 300, 2), (self.C, 200, 3)],
                          [self.B])
        assert body["server_newer"] == [] and body["server_newer_count"] == 0

    def test_newer_is_relative_to_the_common_snapshot_the_client_listing_picks(self, client, zfs):
        """The client lists newest first; the first of its names the server holds is the common one."""
        body = self.reply(client, zfs, [self.A, self.B, self.C, self.D],
                          ["autosnap_29990101000000", self.B, self.A])
        assert body["common_snapshot"] == self.B
        assert [e["name"] for e in body["server_newer"]] == [self.C, self.D]

    def test_the_list_is_capped_at_64_and_the_count_is_the_total(self, client, zfs):
        names = [(f"autosnap_20260101{i:06d}", 100 + i, 1000 + i) for i in range(71)]
        body = self.reply(client, zfs, names, [names[0][0]])
        assert body["server_newer_count"] == 70
        assert len(body["server_newer"]) == SERVER_NEWER_MAX == 64
        assert [e["name"] for e in body["server_newer"]] == [n[0] for n in names[1:65]]   # the OLDEST 64

    def test_exactly_at_the_cap(self, client, zfs):
        names = [(f"autosnap_20260101{i:06d}", 100 + i, 1000 + i) for i in range(65)]
        body = self.reply(client, zfs, names, [names[0][0]])
        assert body["server_newer_count"] == len(body["server_newer"]) == 64

    def test_no_common_snapshot_means_neither_key(self, client, zfs):
        body = self.reply(client, zfs, [self.A, self.B], ["autosnap_29990101000000"])
        assert body["common_snapshot"] is None
        assert "server_newer" not in body and "server_newer_count" not in body
        assert body["server_copy"] == "plain"

    def test_a_missing_dataset_means_neither_key(self, client, zfs):
        resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 200
        assert "server_newer" not in resp.get_json() and "server_newer_count" not in resp.get_json()

    def test_a_dataset_without_snapshots_means_neither_key(self, client, zfs):
        body = self.reply(client, zfs, [], [self.A])
        assert body == {"common_snapshot": None, "server_dataset": SERVER_DS, "server_copy": "none"}

    def test_a_snapshot_without_a_guid_is_null(self, client, zfs):
        from _snapshot_support import ABSENT, listed_snapshot
        zfs["exists"].return_value = True
        zfs["list"].return_value = [
            listed_snapshot(self.A, SERVER_DS, txg=1, guid=5),
            listed_snapshot(self.B, SERVER_DS, txg=2, guid=ABSENT),
        ]
        body = negotiate(client, snapshots=[self.A]).get_json()
        assert body["server_newer"] == [{"name": self.B, "guid": None}]

    def test_a_snapshot_without_createtxg_omits_the_list_and_warns(self, client, zfs, caplog):
        """The server cannot say what is newer; the stream, which decides for itself, then fails
        closed. The negotiate answer is otherwise intact."""
        from _snapshot_support import ABSENT, listed_snapshot
        zfs["exists"].return_value = True
        zfs["list"].return_value = [
            listed_snapshot(self.A, SERVER_DS, txg=1, guid=5),
            listed_snapshot(self.B, SERVER_DS, txg=ABSENT, guid=6),
        ]
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["common_snapshot"] == self.A and body["server_copy"] == "plain"
        assert "server_newer" not in body and "server_newer_count" not in body
        assert body["server_newer_unknown"] is True
        (rec,) = [r for r in caplog.records if r.levelno == logging.WARNING and "createtxg" in r.getMessage()]
        assert SERVER_DS in rec.getMessage()

    def test_one_listing_with_createtxg_and_guid_serves_common_and_newer(self, client, zfs):
        self.reply(client, zfs, [self.A, self.B], [self.A])
        (call,) = zfs["list"].call_args_list
        assert call.kwargs["types"] == "snapshot"
        assert call.kwargs["properties"] == ["createtxg", "guid"]
        assert str(call.kwargs["roots"]) == SERVER_DS

    def test_the_other_keys_are_unchanged(self, client, zfs):
        body = self.reply(client, zfs, [self.A, self.B], [self.A])
        assert set(body) == {"common_snapshot", "server_dataset", "server_copy",
                             "server_newer", "server_newer_count"}
        assert body["server_dataset"] == SERVER_DS

    def test_a_listing_failure_is_still_the_500(self, client, zfs):
        zfs["exists"].return_value = True
        zfs["list"].side_effect = RuntimeError(SECRET)
        resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to list server snapshots: {SECRET}"}

    def test_a_dry_run_negotiates_the_same(self, mocker, zfs):
        authed = Authed(_config(dry_run=True))
        with_snapshots(zfs, (self.A, 1, 5), (self.B, 2, 6))
        body = negotiate(authed.client(), snapshots=[self.A]).get_json()
        assert body["server_newer"] == [{"name": self.B, "guid": "6"}]


class TestServerNewerNeedsATrustedListing:
    """`server_newer` is read from a strict listing. When libzfseasy had to skip a row, negotiate still
    answers from the plain listing (as it always has) but omits `server_newer`; the stream refuses on its own."""

    A, B = "autosnap_20260101000000", "autosnap_20260102000000"
    UNTRUSTED = Exception("zfs list output could not be trusted: 1 row skipped, the first: expected 4 columns, got 2")

    @pytest.fixture(autouse=True)
    def _props(self, server_props):
        return server_props

    def rows(self):
        from _snapshot_support import listed_snapshot
        return [listed_snapshot(self.A, SERVER_DS, txg=1, guid=5), listed_snapshot(self.B, SERVER_DS, txg=2, guid=6)]

    def arrange(self, zfs, strict_raises, plain_raises=None):
        zfs["exists"].return_value = True
        calls = []

        def listing(*args, **kwargs):
            calls.append(kwargs.get("strict", False))
            if kwargs.get("strict"):
                if strict_raises is not None:
                    raise strict_raises
            elif plain_raises is not None:
                raise plain_raises
            return self.rows()

        zfs["list"].side_effect = listing
        return calls

    def test_a_trusted_listing_is_one_strict_call(self, client, zfs):
        calls = self.arrange(zfs, None)
        body = negotiate(client, snapshots=[self.A]).get_json()
        assert calls == [True] and body["server_newer_count"] == 1

    def test_an_untrusted_listing_falls_back_and_omits_server_newer(self, client, zfs, caplog):
        calls = self.arrange(zfs, self.UNTRUSTED)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 200
        body = resp.get_json()
        assert calls == [True, False]
        assert body["common_snapshot"] == self.A and body["server_copy"] == "plain"
        assert "server_newer" not in body and "server_newer_count" not in body
        assert body["server_newer_unknown"] is True
        (rec,) = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert SERVER_DS in rec.getMessage() and "could not be trusted" in rec.getMessage()

    def test_a_failure_of_zfs_itself_is_the_500_when_the_fallback_fails_too(self, client, zfs):
        self.arrange(zfs, RuntimeError(SECRET), plain_raises=RuntimeError(SECRET))
        resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to list server snapshots: {SECRET}"}

    def test_the_gate_is_released_on_both_paths(self, client, authed, zfs):
        self.arrange(zfs, self.UNTRUSTED)
        negotiate(client, snapshots=[self.A])
        assert authed.gate.active == 0
        self.arrange(zfs, RuntimeError("x"), plain_raises=RuntimeError("x"))
        negotiate(client, snapshots=[self.A])
        assert authed.gate.active == 0


class TestServerNewerUnknown:
    """B-2b review fix: a common snapshot whose newer snapshots cannot be told is answered with
    `"server_newer_unknown": true` and without `server_newer` / `server_newer_count`. Silence alone
    would read as "a server that predates the fields", and the client must not settle its anchor on a
    list it does not have. The marker is never `false`: it is absent whenever the list is trusted
    (or there is no common snapshot to be newer than)."""

    A, B = "autosnap_20260101000000", "autosnap_20260102000000"
    UNTRUSTED = Exception("zfs list output could not be trusted: 1 row skipped, the first: expected 4 columns, got 2")
    LIST_KEYS = ("server_newer", "server_newer_count")

    @pytest.fixture(autouse=True)
    def _props(self, server_props):
        return server_props

    def rows(self, *, txg_b=2):
        from _snapshot_support import listed_snapshot
        return [listed_snapshot(self.A, SERVER_DS, txg=1, guid=5),
                listed_snapshot(self.B, SERVER_DS, txg=txg_b, guid=6)]

    def untrusted(self, zfs, **rows_kw):
        """The strict listing raises; the plain one returns the rows."""
        zfs["exists"].return_value = True

        def listing(*args, **kwargs):
            if kwargs.get("strict"):
                raise self.UNTRUSTED
            return self.rows(**rows_kw)

        zfs["list"].side_effect = listing

    def no_createtxg(self, zfs):
        from _snapshot_support import ABSENT
        zfs["exists"].return_value = True
        zfs["list"].return_value = self.rows(txg_b=ABSENT)

    def assert_marked(self, body, common):
        assert body["server_newer_unknown"] is True
        assert not any(k in body for k in self.LIST_KEYS)
        assert body["common_snapshot"] == common and body["server_copy"] == "plain"
        assert body["server_dataset"] == SERVER_DS

    def test_an_untrusted_listing_with_a_common_snapshot_is_marked(self, client, zfs):
        self.untrusted(zfs)
        resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 200
        self.assert_marked(resp.get_json(), self.A)

    def test_a_snapshot_without_createtxg_with_a_common_snapshot_is_marked(self, client, zfs):
        self.no_createtxg(zfs)
        resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 200
        self.assert_marked(resp.get_json(), self.A)

    def test_the_marker_is_the_json_literal_true(self, client, zfs):
        self.untrusted(zfs)
        raw = negotiate(client, snapshots=[self.A]).get_data(as_text=True)
        assert '"server_newer_unknown":true' in raw.replace(" ", "")

    def test_the_whole_reply_is_exactly_the_old_keys_plus_the_marker(self, client, zfs):
        self.untrusted(zfs)
        body = negotiate(client, snapshots=[self.A]).get_json()
        assert set(body) == {"common_snapshot", "server_dataset", "server_copy", "server_newer_unknown"}

    # -- never present when the list is trusted or there is nothing to be newer than --------------------------

    def test_a_trusted_listing_has_no_marker(self, client, zfs):
        zfs["exists"].return_value = True
        zfs["list"].return_value = self.rows()
        body = negotiate(client, snapshots=[self.A]).get_json()
        assert "server_newer_unknown" not in body
        assert body["server_newer"] == [{"name": self.B, "guid": "6"}] and body["server_newer_count"] == 1

    def test_a_trusted_listing_with_nothing_newer_has_no_marker_either(self, client, zfs):
        zfs["exists"].return_value = True
        zfs["list"].return_value = self.rows()
        body = negotiate(client, snapshots=[self.B]).get_json()
        assert "server_newer_unknown" not in body
        assert body["server_newer"] == [] and body["server_newer_count"] == 0

    @pytest.mark.parametrize("cause", ["untrusted", "no_createtxg"])
    def test_no_common_snapshot_has_no_marker_whatever_the_listing(self, client, zfs, cause):
        getattr(self, cause)(zfs)
        body = negotiate(client, snapshots=["autosnap_29990101000000"]).get_json()
        assert body["common_snapshot"] is None
        assert "server_newer_unknown" not in body
        assert not any(k in body for k in self.LIST_KEYS)

    def test_a_missing_dataset_has_no_marker(self, client, zfs):
        resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 200
        assert "server_newer_unknown" not in resp.get_json()

    def test_a_dataset_without_snapshots_has_no_marker(self, client, zfs):
        with_snapshots(zfs)
        body = negotiate(client, snapshots=[self.A]).get_json()
        assert body == {"common_snapshot": None, "server_dataset": SERVER_DS, "server_copy": "none"}

    def test_a_failure_of_both_listings_is_the_500_without_a_marker(self, client, zfs):
        zfs["exists"].return_value = True
        zfs["list"].side_effect = RuntimeError(SECRET)
        resp = negotiate(client, snapshots=[self.A])
        assert resp.status_code == 500
        assert "server_newer_unknown" not in resp.get_json()

    def test_a_trusted_negotiate_after_an_untrusted_one_is_unmarked(self, client, zfs):
        """No state is kept between requests."""
        self.untrusted(zfs)
        assert negotiate(client, snapshots=[self.A]).get_json()["server_newer_unknown"] is True
        zfs["list"].side_effect = None
        zfs["list"].return_value = self.rows()
        body = negotiate(client, snapshots=[self.A]).get_json()
        assert "server_newer_unknown" not in body and body["server_newer_count"] == 1

    def test_a_dry_run_marks_the_same(self, mocker, zfs):
        authed = Authed(_config(dry_run=True))
        self.untrusted(zfs)
        self.assert_marked(negotiate(authed.client(), snapshots=[self.A]).get_json(), self.A)

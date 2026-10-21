"""Checkpoint A, decision (a) -- the empty-value / type audit, A-1 and A-2.

Every row of the "Decision (a) -- field audit" table in
`docs/config_db_cli_plan.md` is pinned here through BOTH doors into a
`BackupConfig`:

1. `BackupConfig.from_file` (the YAML boundary, A-1), and
2. `import_yaml` -> `load_config` (the DB path, A-2): the same YAML written
   into a SQLite store and read back.

"Normalize" rows assert the concrete value the YAML runtime already behaved
as, plus the WARNING (or its absence) via `caplog`. "Reject" rows assert a
`ValueError` naming the key -- and, on the import path, that **nothing was
created**: not the database, not even its directory (the importer's phase 0).

The second half covers `validate_config` directly on in-code configs (which
never pass through the YAML guards) and the importer's failure handling.

Mutation checks (one revert at a time, against a scratch copy of the tree):

- `_bool_from_config(None)` returning `True` (the old "falsy -> enabled"
  bug): 10 tests fail.
- `_interval_from_config` accepting zero (`< 0` instead of `<= 0`): 3 fail
  (the `from_file` rows; the import rows still fail because `validate_config`
  independently refuses them -- the two guards back each other up).
- `validate_config` no longer checking `Dataset.enabled`: 3 fail.
- The importer not removing a failed first import's file: 2 fail.
- The importer not logging `validate_config`'s warnings: 1 fails.
- `_dataset_level_rules` not warning for a never-prune dataset: 1 fails.
- `validate_config` not rejecting `snapshot_prefix is None`: 1 fails.
- Dropping `order_by` on destinations: 1 fails.

Fix-up round (A-14.1, A-15, A-16), same method: `_bool_from_config` without its
integer WARNING 17, accepting any int 17, accepting `1.0` 8, accepting `"1"` 8,
without the unauthenticated-receive-endpoint note 1; the duplicate-remote check
in `DatasetConfig.from_dict` removed 6; `validate_config` not rejecting a
`client_id_file` of `.` 5, `save_config` not storing `""` as NULL 1, no
`""` warning 1. (Both rules, and A-21 below, were superseded by TLS/mTLS
M15/D2: `from_file` refuses the `client_id_file` key outright.)

A-21 (the normalised current-directory rule, in `validate_config` and
`from_file`), same method: `validate_config` comparing to the literal `"."` 7;
`validate_config` also refusing `""` 4 (2 pre-existing); `from_file` comparing
to the literal `"."` 5; `from_file` also refusing `""` 2 (1 pre-existing).
"""

import logging
import os
from datetime import timedelta
from pathlib import Path

import pytest
import yaml

from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
    RemoteServerConfig,
    RetentionRule,
)
from zfsbackup.config.store import (
    dispose_all,
    import_yaml,
    load_config,
    open_config_session,
    resolve_config_path,
    save_config,
    validate_config,
)
from zfsbackup.config.store import importer as importer_module
from zfsbackup.config.store.models import GlobalSettings

pytestmark = pytest.mark.unit

ABSENT = object()  # a key to delete from the base mapping


def make(ds=None, remote_destinations=False, **top):
    """A one-dataset config mapping, overridden per test.

    `ds` overrides/extends the dataset mapping; `top` the top level. A value
    of `ABSENT` removes the key. `remote_destinations=True` declares the
    `srv` destination the remote rows refer to.
    """
    dataset = {"name": "pool/data", "frequency": "1h"}
    dataset.update(ds or {})
    data = {"datasets": [dataset]}
    if remote_destinations:
        data["destinations"] = {"srv": {"url": "https://127.0.0.1:18082"}}
    data.update(top)
    for container in (data, dataset):
        for key in [k for k, v in container.items() if v is ABSENT]:
            del container[key]
    return data


def write(tmp_path, data, name="cfg.yaml") -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


def via_import(tmp_path, yaml_path):
    """Import `yaml_path` into a fresh store, return the loaded config."""
    resolved = resolve_config_path(str(tmp_path / "store" / "config.db"))
    import_yaml(yaml_path, resolved)
    with open_config_session(resolved, readonly=True) as session:
        return load_config(session)


def warnings_of(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


REMOTE_ROW = {"remote": [{"destination": "srv"}]}


def remote_row(**kw):
    return {"remote": [{"destination": "srv", **kw}]}


#: The four YAML boolean fields: (id, key named in messages, builder(value) -> mapping).
BOOL_FIELDS = [
    ("dataset-enabled", "datasets[pool/data].enabled",
     lambda v: make(ds={"enabled": v})),
    ("dataset-recursive", "datasets[pool/data].recursive",
     lambda v: make(ds={"recursive": v})),
    ("dry-run", "dry_run",
     lambda v: make(dry_run=v)),
    ("remote-backup-enabled", "remote_backup.enabled",
     lambda v: make(remote_backup={"target_dataset": "pool/recv", "enabled": v})),
]

#: Values `_bool_from_config` must still refuse: only the integers 0 and 1 are
#: tolerated (A-15). `1.0` and `"1"` look like 1 but are not integers.
BAD_BOOL_VALUES = [("int-2", 2), ("int-minus-1", -1), ("float-1", 1.0), ("string-1", "1")]

# ---------------------------------------------------------------------------
# Normalize rows
# ---------------------------------------------------------------------------

#: (id, data, extractor, expected, warning fragment or None for "silent")
NORMALIZE = [
    pytest.param(
        make(ds={"enabled": None}),
        lambda c: c.datasets[0].enabled, False, "enabled",
        id="dataset-enabled-null-is-disabled-with-warning",
    ),
    pytest.param(
        make(ds={"recursive": None}),
        lambda c: c.datasets[0].recursive, False, None,
        id="dataset-recursive-null-is-false-silently",
    ),
    pytest.param(
        make(ds=remote_row(frequency="0m"), remote_destinations=True),
        lambda c: c.datasets[0].remote[0].frequency, None, "frequency",
        id="remote-frequency-0m-inherits-with-warning",
    ),
    pytest.param(
        make(ds=remote_row(frequency=None), remote_destinations=True),
        lambda c: c.datasets[0].remote[0].frequency, None, None,
        id="remote-frequency-null-inherits-silently",
    ),
    pytest.param(
        make(ds={"retention": ABSENT}),
        lambda c: [(r.age, r.keep_for) for r in c.datasets[0].retention_rules],
        [(timedelta(days=1), timedelta(days=30))], None,
        id="retention-absent-is-default-1d-30d",
    ),
    pytest.param(
        make(ds={"retention": None}),
        lambda c: [(r.age, r.keep_for) for r in c.datasets[0].retention_rules],
        [(timedelta(days=1), timedelta(days=30))], None,
        id="retention-null-is-default-1d-30d",
    ),
    pytest.param(
        make(ds={"retention": {}}),
        lambda c: [(r.age, r.keep_for) for r in c.datasets[0].retention_rules],
        [(timedelta(days=1), timedelta(days=30))], None,
        id="retention-empty-mapping-is-default-1d-30d",
    ),
    pytest.param(
        make(ds={"retention": "forever"}),
        lambda c: c.datasets[0].retention_rules, [], "forever",
        id="retention-forever-is-never-prune-with-warning",
    ),
    pytest.param(
        make(snapshot_prefix="my_snap"),
        lambda c: c.snapshot_prefix, "my_snap", None,
        id="snapshot-prefix-with-underscore-is-silent",
    ),
    pytest.param(
        make(prune_interval=None),
        lambda c: c.prune_interval, None, None,
        id="prune-interval-null-derives-silently",
    ),
    pytest.param(
        make(api_host=None),
        lambda c: c.api_host, "127.0.0.1", None,
        id="api-host-null-is-loopback-silently",
    ),
    pytest.param(
        make(api_port="9090"),
        lambda c: c.api_port, 9090, None,
        id="api-port-digit-string-is-accepted",
    ),
    pytest.param(
        make(api_port=0),
        lambda c: c.api_port, 0, "api_port",
        id="api-port-0-warns",
    ),
    pytest.param(
        make(dry_run=None),
        lambda c: c.dry_run, False, None,
        id="dry-run-null-is-live-silently",
    ),
    pytest.param(
        make(remote_backup={"target_dataset": "pool/recv", "enabled": None}),
        lambda c: c.remote_backup.enabled, False, "remote_backup.enabled",
        id="remote-backup-enabled-null-is-disabled-with-warning",
    ),
]


#: The NORMALIZE warning fragments that are SEMANTIC (the value parses; the
#: operator should still know what it does at run time). `validate_config` is
#: the single source of those: `from_file` is silent for them and the import
#: path (which validates) warns exactly once. The others are parse-boundary
#: warnings and still come from `from_file`.
SEMANTIC_WARNINGS = {"forever", "api_port"}


class TestNormalizeRows:
    @pytest.mark.parametrize("data, extract, expected, warning", NORMALIZE)
    def test_from_file(self, tmp_path, caplog, data, extract, expected, warning):
        path = write(tmp_path, data)
        with caplog.at_level(logging.WARNING):
            config = BackupConfig.from_file(path)

        assert extract(config) == expected
        found = warnings_of(caplog)
        if warning is None or warning in SEMANTIC_WARNINGS:
            # Semantic warnings belong to `validate_config` alone.
            assert found == []
        else:
            assert any(warning in message for message in found), found

    @pytest.mark.parametrize(
        "data, extract, expected, warning",
        [row for row in NORMALIZE if row.values[3] in SEMANTIC_WARNINGS],
    )
    def test_semantic_warning_comes_from_validate_config(
        self, tmp_path, data, extract, expected, warning
    ):
        config = BackupConfig.from_file(write(tmp_path, data))
        found = validate_config(config)
        assert len(found) == 1 and warning in found[0], found

    @pytest.mark.parametrize("data, extract, expected, warning", NORMALIZE)
    def test_import_then_load_config(
        self, tmp_path, caplog, data, extract, expected, warning
    ):
        path = write(tmp_path, data)
        expected_config = BackupConfig.from_file(path)  # the YAML-boundary truth

        caplog.clear()
        with caplog.at_level(logging.WARNING):
            loaded = via_import(tmp_path, path)

        assert extract(loaded) == expected
        # The DB path must not shift behaviour relative to the YAML runtime.
        assert loaded == expected_config
        found = warnings_of(caplog)
        if warning is None:
            # `load_config` itself warns only for a never-prune dataset.
            assert [m for m in found if "never be pruned" not in m] == []
        else:
            assert any(warning in message for message in found), found

    def test_enabled_null_is_disabled_in_the_database_not_enabled(self, tmp_path):
        # A1: the ORM's Python `default=True` used to turn a falsy-under-YAML
        # `enabled: null` into an ENABLED dataset on import.
        loaded = via_import(tmp_path, write(tmp_path, make(ds={"enabled": None})))
        assert loaded.datasets[0].enabled is False
        assert loaded.enabled_datasets == []

    def test_remote_backup_enabled_null_does_not_enable_the_receive_path(self, tmp_path):
        # A1 (the security-relevant instance): `remote_backup.enabled: null`
        # was OFF under YAML but imported as ON.
        data = make(remote_backup={"target_dataset": "pool/recv", "enabled": None})
        loaded = via_import(tmp_path, write(tmp_path, data))
        assert loaded.remote_backup.enabled is False

    def test_forever_round_trips_as_empty_through_the_store(self, tmp_path, caplog):
        # A3: dataset-level `[]` = never prune must survive save -> load
        # (it used to reload as 1d -> 30d, i.e. start destroying snapshots).
        path = write(tmp_path, make(ds={"retention": "forever"}))
        with caplog.at_level(logging.WARNING):
            loaded = via_import(tmp_path, path)
        assert loaded.datasets[0].retention_rules == []
        assert any(
            "pool/data" in m and "never be pruned" in m for m in warnings_of(caplog)
        )

    def test_prune_interval_null_is_stored_as_null_not_check_interval(self, tmp_path):
        resolved_path = tmp_path / "store" / "config.db"
        via_import(tmp_path, write(tmp_path, make(prune_interval=None)))
        resolved = resolve_config_path(str(resolved_path))
        with open_config_session(resolved, readonly=True) as session:
            row = session.get(GlobalSettings, 1)
            assert row.prune_interval_seconds is None
            assert row.prune_interval_literal is None


# ---------------------------------------------------------------------------
# A-15: the integers 0 and 1 are accepted as booleans, with a WARNING
# ---------------------------------------------------------------------------

BOOL_EXTRACT = {
    "dataset-enabled": lambda c: c.datasets[0].enabled,
    "dataset-recursive": lambda c: c.datasets[0].recursive,
    "dry-run": lambda c: c.dry_run,
    "remote-backup-enabled": lambda c: c.remote_backup.enabled,
}


class TestIntegerBooleansAcceptedWithWarning:
    """`enabled: 1` / `dry_run: 0` behaved as written under YAML truthiness, so
    the YAML boundary keeps accepting them -- loudly. `2`, `-1`, `1.0` and `"1"`
    stay rejected (the REJECT table above)."""

    @pytest.mark.parametrize("number", [0, 1])
    @pytest.mark.parametrize("field, key, build", BOOL_FIELDS, ids=[f[0] for f in BOOL_FIELDS])
    def test_from_file_accepts_and_warns(self, tmp_path, caplog, field, key, build, number):
        path = write(tmp_path, build(number))
        with caplog.at_level(logging.WARNING):
            config = BackupConfig.from_file(path)

        value = BOOL_EXTRACT[field](config)
        assert value is bool(number)
        matching = [m for m in warnings_of(caplog) if key in m]
        assert len(matching) == 1, warnings_of(caplog)
        assert f"is the integer {number}" in matching[0]
        assert ("treating it as true" if number else "treating it as false") in matching[0]
        assert "write true/false" in matching[0]

    @pytest.mark.parametrize("number", [0, 1])
    @pytest.mark.parametrize("field, key, build", BOOL_FIELDS, ids=[f[0] for f in BOOL_FIELDS])
    def test_import_then_load_config_accepts_and_warns(
        self, tmp_path, caplog, field, key, build, number
    ):
        path = write(tmp_path, build(number))
        expected = BackupConfig.from_file(path)

        caplog.clear()
        with caplog.at_level(logging.WARNING):
            loaded = via_import(tmp_path, path)

        value = BOOL_EXTRACT[field](loaded)
        assert value is bool(number)
        assert loaded == expected  # the DB path does not shift behaviour
        matching = [m for m in warnings_of(caplog) if key in m and "the integer" in m]
        assert len(matching) == 1, warnings_of(caplog)

    def test_remote_backup_enabled_one_names_the_unauthenticated_receive_endpoint(
        self, tmp_path, caplog
    ):
        data = make(remote_backup={"target_dataset": "pool/recv", "enabled": 1})
        with caplog.at_level(logging.WARNING):
            config = BackupConfig.from_file(write(tmp_path, data))
        assert config.remote_backup.enabled is True
        messages = [m for m in warnings_of(caplog) if "remote_backup.enabled" in m]
        assert len(messages) == 1
        assert "unauthenticated receive endpoint" in messages[0]

    @pytest.mark.parametrize(
        "field, key, build",
        [f for f in BOOL_FIELDS if f[0] != "remote-backup-enabled"],
        ids=[f[0] for f in BOOL_FIELDS if f[0] != "remote-backup-enabled"],
    )
    def test_only_remote_backup_enabled_mentions_the_receive_endpoint(
        self, tmp_path, caplog, field, key, build
    ):
        with caplog.at_level(logging.WARNING):
            BackupConfig.from_file(write(tmp_path, build(1)))
        assert not any("receive endpoint" in m for m in warnings_of(caplog))

    def test_remote_backup_enabled_zero_does_not_mention_the_receive_endpoint(
        self, tmp_path, caplog
    ):
        data = make(remote_backup={"target_dataset": "pool/recv", "enabled": 0})
        with caplog.at_level(logging.WARNING):
            config = BackupConfig.from_file(write(tmp_path, data))
        assert config.remote_backup.enabled is False
        assert not any("receive endpoint" in m for m in warnings_of(caplog))

    def test_boolean_true_and_false_stay_silent(self, tmp_path, caplog):
        data = make(
            ds={"enabled": True, "recursive": False},
            dry_run=True,
            remote_backup={"target_dataset": "pool/recv", "enabled": False},
        )
        with caplog.at_level(logging.WARNING):
            BackupConfig.from_file(write(tmp_path, data))
        assert warnings_of(caplog) == []


# ---------------------------------------------------------------------------
# Reject rows
# ---------------------------------------------------------------------------

REJECT = [
    # --- booleans given as non-booleans (truthy "false" etc.) ---
    pytest.param(make(ds={"enabled": "false"}), "enabled", id="dataset-enabled-string"),
    pytest.param(make(ds={"recursive": "true"}), "recursive", id="dataset-recursive-string"),
    pytest.param(make(dry_run="false"), "dry_run", id="dry-run-string"),
    pytest.param(
        make(remote_backup={"target_dataset": "pool/recv", "enabled": "no"}),
        "remote_backup.enabled", id="remote-backup-enabled-string",
    ),
    # --- ints other than 0/1, floats and digit strings (A-15: only 0/1 are accepted) ---
    *[
        pytest.param(build(bad), key, id=f"{field}-{label}")
        for field, key, build in BOOL_FIELDS
        for label, bad in BAD_BOOL_VALUES
    ],
    # --- intervals <= 0 (hot loop / per-cycle exception) ---
    pytest.param(make(ds={"frequency": "0m"}), "frequency", id="dataset-frequency-zero"),
    pytest.param(make(ds={"frequency": "-1h"}), "frequency", id="dataset-frequency-negative"),
    pytest.param(
        make(ds=remote_row(frequency="-5m"), remote_destinations=True),
        "frequency", id="remote-frequency-negative",
    ),
    pytest.param(make(check_interval="0m"), "check_interval", id="check-interval-zero"),
    pytest.param(make(check_interval="-5m"), "check_interval", id="check-interval-negative"),
    pytest.param(make(prune_interval="0m"), "prune_interval", id="prune-interval-zero"),
    pytest.param(make(prune_interval="-1h"), "prune_interval", id="prune-interval-negative"),
    # --- snapshot_prefix (Q2) ---
    pytest.param(make(snapshot_prefix=None), "snapshot_prefix", id="snapshot-prefix-null"),
    pytest.param(make(snapshot_prefix=123), "snapshot_prefix", id="snapshot-prefix-int"),
    pytest.param(make(snapshot_prefix=True), "snapshot_prefix", id="snapshot-prefix-bool"),
    pytest.param(make(snapshot_prefix=["a"]), "snapshot_prefix", id="snapshot-prefix-list"),
    # --- api_port ---
    pytest.param(make(api_port=None), "api_port", id="api-port-null"),
    pytest.param(make(api_port=True), "api_port", id="api-port-bool"),
    pytest.param(make(api_port=8080.5), "api_port", id="api-port-float"),
    pytest.param(make(api_port=70000), "api_port", id="api-port-too-large"),
    pytest.param(make(api_port=-1), "api_port", id="api-port-negative"),
    pytest.param(make(api_port="abc"), "api_port", id="api-port-non-digit-string"),
    pytest.param(make(api_port="-1"), "api_port", id="api-port-negative-string"),
    pytest.param(make(api_port=""), "api_port", id="api-port-empty-string"),
    # --- other typed scalars ---
    pytest.param(make(api_host=123), "api_host", id="api-host-int"),
    pytest.param(make(ds={"name": 123}), "name", id="dataset-name-int"),
    pytest.param(
        make(destinations={1: {"url": "https://x"}}), "destinations key",
        id="destination-key-int",
    ),
    pytest.param(
        make(destinations={"srv": {"url": 5}}), "url", id="destination-url-int",
    ),
    pytest.param(
        make(ds={"remote": [{"destination": 5}]}, destinations={"5": {"url": "u"}}),
        "destination", id="remote-destination-int",
    ),
    pytest.param(
        make(remote_backup={"target_dataset": 7}), "target_dataset",
        id="remote-backup-target-int",
    ),
    # --- A-16: the same destination twice on one dataset (R5) ---
    pytest.param(
        make(
            ds={"remote": [
                {"destination": "srv"},
                {"destination": "srv", "retention": {"2d": "10d"}},
            ]},
            remote_destinations=True,
        ),
        "duplicate destination 'srv'", id="duplicate-remote-destination",
    ),
    pytest.param(
        make(
            ds={"remote": [{"destination": "srv"}, {"destination": "srv"}]},
            remote_destinations=True,
        ),
        "duplicate destination 'srv'", id="exact-duplicate-remote-destination",
    ),
    pytest.param(
        make(
            ds={"remote": [
                {"destination": "srv"}, {"destination": "other"}, {"destination": "srv"},
            ]},
            destinations={
                "srv": {"url": "https://127.0.0.1:18082"},
                "other": {"url": "https://127.0.0.1:18083"},
            },
        ),
        "duplicate destination 'srv'", id="non-adjacent-duplicate-remote-destination",
    ),
    # --- destination-level "never prune" is NOT a thing (item-3b deferral) ---
    pytest.param(
        make(ds=remote_row(retention="forever"), remote_destinations=True),
        "retention", id="destination-retention-forever",
    ),
    pytest.param(
        make(ds=remote_row(retention={}), remote_destinations=True),
        "retention", id="destination-retention-empty-mapping",
    ),
]


class TestRejectRows:
    @pytest.mark.parametrize("data, key", REJECT)
    def test_from_file_raises_naming_the_key(self, tmp_path, data, key):
        with pytest.raises(ValueError) as excinfo:
            BackupConfig.from_file(write(tmp_path, data))
        assert key in str(excinfo.value)

    @pytest.mark.parametrize("data, key", REJECT)
    def test_import_raises_and_creates_nothing(self, tmp_path, data, key):
        target = tmp_path / "brandnew" / "config.db"
        resolved = resolve_config_path(str(target))

        with pytest.raises(ValueError) as excinfo:
            import_yaml(write(tmp_path, data), resolved)

        assert key in str(excinfo.value)
        assert not target.exists()
        assert not target.parent.exists()

    def test_snapshot_prefix_null_message_tells_the_operator_what_to_do(self, tmp_path):
        # Q2: an operator with `None_*` snapshots needs the way out.
        with pytest.raises(ValueError) as excinfo:
            BackupConfig.from_file(write(tmp_path, make(snapshot_prefix=None)))
        message = str(excinfo.value)
        assert "snapshot_prefix" in message
        assert '"None"' in message
        assert "None_" in message

    def test_dry_run_string_is_a_clean_error_not_a_statement_error(self, tmp_path):
        # A1: `dry_run: "false"` crashed import with an ORM StatementError
        # traceback, after the target had been created.
        target = tmp_path / "d" / "config.db"
        with pytest.raises(ValueError, match="dry_run"):
            import_yaml(
                write(tmp_path, make(dry_run="false")),
                resolve_config_path(str(target)),
            )
        assert not target.parent.exists()

    def test_destination_level_empty_mapping_message_still_says_omit_or_declare(
        self, tmp_path
    ):
        with pytest.raises(ValueError, match="omit 'retention'"):
            BackupConfig.from_file(
                write(tmp_path, make(ds=remote_row(retention={}), remote_destinations=True))
            )


# ---------------------------------------------------------------------------
# A-16 (R5): duplicate remote destinations are rejected at the YAML boundary
# ---------------------------------------------------------------------------


class TestDuplicateRemoteDestinations:
    """Before A-16 `from_file` accepted a dataset naming one destination
    twice -- the second entry's retention was silently ignored -- and only
    `import` refused it (with a bare `IntegrityError` at insert, then
    `validate_config`). Now both doors refuse with `validate_config`'s
    wording, and nothing is created."""

    DUPLICATE = make(
        ds={"remote": [
            {"destination": "srv", "retention": {"1d": "7d"}},
            {"destination": "srv", "retention": {"2d": "30d"}},
        ]},
        remote_destinations=True,
    )

    def test_from_file_names_both_positions(self, tmp_path):
        with pytest.raises(ValueError) as excinfo:
            BackupConfig.from_file(write(tmp_path, self.DUPLICATE))
        message = str(excinfo.value)
        assert "datasets[pool/data].remote[1]: duplicate destination 'srv'" in message
        assert "already used by datasets[pool/data].remote[0]" in message

    def test_from_file_wording_matches_validate_configs(self, tmp_path):
        config = good_config(destinations={"srv": Destination("https://x")})
        config.datasets[0].remote = [RemoteDatasetConfig("srv"), RemoteDatasetConfig("srv")]
        with pytest.raises(ValueError) as in_code:
            validate_config(config)
        with pytest.raises(ValueError) as from_yaml:
            BackupConfig.from_file(write(tmp_path, self.DUPLICATE))
        for fragment in ("remote[1]: duplicate destination 'srv'", "already used by"):
            assert fragment in str(in_code.value)
            assert fragment in str(from_yaml.value)

    def test_from_dict_rejects_it_directly(self):
        with pytest.raises(ValueError, match="duplicate destination 'srv'"):
            DatasetConfig.from_dict(
                {"name": "pool/data", "remote": [{"destination": "srv"}, {"destination": "srv"}]}
            )

    def test_importing_it_creates_nothing_not_even_the_directory(self, tmp_path):
        target = tmp_path / "brandnew" / "config.db"
        with pytest.raises(ValueError, match="duplicate destination"):
            import_yaml(write(tmp_path, self.DUPLICATE), resolve_config_path(str(target)))
        assert not target.exists() and not target.parent.exists()

    def test_importing_it_leaves_an_existing_database_untouched(self, tmp_path):
        good = write(tmp_path, make(), name="good.yaml")
        resolved = resolve_config_path(str(tmp_path / "config.db"))
        import_yaml(good, resolved)
        with open_config_session(resolved, readonly=True) as session:
            before = load_config(session)
        dispose_all()

        with pytest.raises(ValueError, match="duplicate destination"):
            import_yaml(write(tmp_path, self.DUPLICATE), resolved)

        with open_config_session(resolved, readonly=True) as session:
            assert load_config(session) == before

    def test_the_same_destination_on_two_different_datasets_is_fine(self, tmp_path):
        data = {
            "datasets": [
                {"name": "pool/a", "remote": [{"destination": "srv"}]},
                {"name": "pool/b", "remote": [{"destination": "srv"}]},
            ],
            "destinations": {"srv": {"url": "https://127.0.0.1:18082"}},
        }
        path = write(tmp_path, data)
        assert [d.remote[0].destination for d in BackupConfig.from_file(path).datasets] == [
            "srv", "srv",
        ]
        loaded = via_import(tmp_path, path)
        assert loaded == BackupConfig.from_file(path)

    def test_distinct_destinations_on_one_dataset_are_fine(self, tmp_path):
        data = make(
            ds={"remote": [{"destination": "srv"}, {"destination": "other"}]},
            destinations={
                "srv": {"url": "https://127.0.0.1:18082"},
                "other": {"url": "https://127.0.0.1:18083"},
            },
        )
        config = BackupConfig.from_file(write(tmp_path, data))
        assert [r.destination for r in config.datasets[0].remote] == ["srv", "other"]


# ---------------------------------------------------------------------------
# A-14.1 / A-21 (reworked by TLS/mTLS M15/D2): `client_id_file` is gone.
# `BackupConfig.from_file` refuses the key outright, whatever its value (a
# path, null, "", or any spelling of the current directory); the field and its
# column no longer exist, so there is nothing to validate, store or derive.
# The old per-spelling messages ("names a directory, not a file") are gone with
# the rule: every value gets the one removal message.
# ---------------------------------------------------------------------------

REMOVED_MESSAGE = "client_id_file was removed"

#: Every value the key can carry in YAML: ordinary paths, the unset
#: spellings (null, ""), and every spelling of the current directory.
ANY_YAML_VALUE = [
    pytest.param(v, id=repr(v))
    for v in [
        "/x/client_id", "x/id", "client_id", "..", "./a", "a/.", "/", ".hidden", "../x",
        ".", "./", "././", "a/..", "a/b/../..",
        "", None, 123, True, ["a"],
    ]
]


class TestClientIdFileRemoved:
    @pytest.mark.parametrize("value", ANY_YAML_VALUE)
    def test_from_file_refuses_the_key_whatever_its_value(self, tmp_path, value):
        path = write(tmp_path, make(client_id_file=value))
        with pytest.raises(ValueError, match=REMOVED_MESSAGE) as excinfo:
            BackupConfig.from_file(path)
        message = str(excinfo.value)
        assert "identity show" in message and "identity set-client-id" in message
        # The old directory rule's wording is gone with the rule.
        assert "names a directory" not in message

    def test_from_file_without_the_key_loads(self, tmp_path):
        config = BackupConfig.from_file(write(tmp_path, make()))
        assert not hasattr(config, "client_id_file")

    def test_the_key_is_refused_before_dataset_errors(self, tmp_path):
        # A config with both problems reports the client_id_file one: the check
        # runs right after the top-level mapping is validated.
        data = make(client_id_file="/x/id", ds={"frequency": "not-a-duration"})
        with pytest.raises(ValueError, match=REMOVED_MESSAGE):
            BackupConfig.from_file(write(tmp_path, data))
        # Without the key the same file reports the dataset problem.
        del data["client_id_file"]
        with pytest.raises(ValueError) as excinfo:
            BackupConfig.from_file(write(tmp_path, data, name="b.yaml"))
        assert "client_id_file" not in str(excinfo.value)

    def test_backup_config_has_no_client_id_file(self):
        config = good_config()
        assert not hasattr(config, "client_id_file")
        assert not hasattr(config, "effective_client_id_file")
        with pytest.raises(TypeError):
            BackupConfig(datasets=[], client_id_file=Path("/x/id"))

    def test_validate_config_has_no_client_id_file_rule(self):
        # The store check that read the field is gone: an attribute stuck onto
        # a config object by hand is simply not looked at.
        config = good_config()
        config.client_id_file = "/x/client_id"
        assert validate_config(config) == []

    def test_the_column_is_gone_from_the_table(self):
        assert not hasattr(GlobalSettings, "client_id_file")
        assert "client_id_file" not in GlobalSettings.__table__.columns

    @pytest.mark.parametrize(
        "value", ["/x/client_id", ".", "", None, "./", "a/.."],
        ids=["path", "dot", "empty-string", "null", "dot-slash", "a-dotdot"],
    )
    def test_yaml_with_the_key_is_refused_at_import_with_nothing_created(
        self, tmp_path, value
    ):
        target = tmp_path / "brandnew" / "config.db"
        with pytest.raises(ValueError, match=REMOVED_MESSAGE):
            import_yaml(
                write(tmp_path, make(client_id_file=value)),
                resolve_config_path(str(target)),
            )
        assert not target.exists() and not target.parent.exists()

    def test_yaml_without_the_key_imports_and_round_trips(self, tmp_path):
        path = write(tmp_path, make())
        loaded = via_import(tmp_path, path)
        assert not hasattr(loaded, "client_id_file")
        assert loaded == BackupConfig.from_file(path)


# ---------------------------------------------------------------------------
# Listed gaps: non-canonical per-destination literals through the mapper
# ---------------------------------------------------------------------------


class TestNonCanonicalPerDestinationLiterals:
    """`90m` (a remote frequency a synthesizer would render as `1h30m`) and
    `30d` (which it would render as `1M`) are literals a re-synthesizing
    mapper would silently rewrite. The dataset-level path was covered by
    item 9; these are the per-destination columns."""

    def _yaml(self, tmp_path):
        data = make(
            ds={
                "frequency": "1h",
                "retention": {"1d": "30d"},
                "remote": [{
                    "destination": "srv",
                    "frequency": "90m",
                    "retention": {"2d": "30d", "7d": "6M"},
                }],
            },
            remote_destinations=True,
        )
        return write(tmp_path, data)

    def test_literals_survive_import_then_load(self, tmp_path):
        path = self._yaml(tmp_path)
        loaded = via_import(tmp_path, path)
        remote = loaded.datasets[0].remote[0]

        assert remote.frequency.literal == "90m"
        assert [(r.age.literal, r.keep_for.literal) for r in remote.retention_rules] == [
            ("2d", "30d"),
            ("7d", "6M"),
        ]
        assert loaded == BackupConfig.from_file(path)

    def test_literals_are_preserved_not_resynthesized(self, tmp_path):
        loaded = via_import(tmp_path, self._yaml(tmp_path))
        remote = loaded.datasets[0].remote[0]
        # Value-equal to the canonical spelling, but the stored text wins.
        assert remote.retention_rules[0].keep_for == Duration("1M")
        assert remote.retention_rules[0].keep_for.literal == "30d"
        assert remote.frequency.total_seconds() == 5400
        assert remote.frequency == Duration("90m")


# ---------------------------------------------------------------------------
# validate_config, directly, on in-code configs
# ---------------------------------------------------------------------------


def good_config(**over) -> BackupConfig:
    cfg = BackupConfig(
        datasets=[
            DatasetConfig(
                name="pool/data",
                frequency=Duration("1h"),
                retention_rules=[RetentionRule(Duration("1d"), Duration("30d"))],
            )
        ],
        snapshot_prefix="autosnap",
    )
    for key, value in over.items():
        setattr(cfg, key, value)
    return cfg


class TestValidateConfigDirect:
    def test_clean_config_has_no_warnings(self):
        assert validate_config(good_config()) == []

    def test_stays_pure_and_never_reads_home(self, monkeypatch):
        # Validation must not touch $HOME (the client id no longer derives
        # from a file at all).
        def boom(*_a, **_k):
            raise AssertionError("validate_config touched $HOME")

        monkeypatch.setattr(Path, "home", boom)
        assert validate_config(good_config()) == []

    @pytest.mark.parametrize(
        "mutate, key",
        [
            (lambda c: setattr(c.datasets[0], "enabled", None), "enabled"),
            (lambda c: setattr(c.datasets[0], "recursive", None), "recursive"),
            (lambda c: setattr(c, "dry_run", None), "dry_run"),
            (
                lambda c: setattr(
                    c, "remote_backup", RemoteServerConfig("pool/recv", enabled=None)
                ),
                "remote_backup.enabled",
            ),
        ],
        ids=["dataset-enabled", "dataset-recursive", "dry-run", "remote-backup-enabled"],
    )
    def test_none_booleans_are_rejected_not_defaulted(self, mutate, key):
        config = good_config()
        mutate(config)
        with pytest.raises(ValueError, match=key):
            validate_config(config)

    @pytest.mark.parametrize(
        "mutate, key",
        [
            (lambda c: setattr(c.datasets[0], "enabled", 1), "enabled"),
            (lambda c: setattr(c.datasets[0], "enabled", "false"), "enabled"),
            (lambda c: setattr(c, "dry_run", 0), "dry_run"),
            (lambda c: setattr(c, "snapshot_prefix", 5), "snapshot_prefix"),
            (lambda c: setattr(c, "api_host", None), "api_host"),
            (lambda c: setattr(c, "api_port", True), "api_port"),
            (lambda c: setattr(c, "api_port", 80.0), "api_port"),
            (lambda c: setattr(c, "api_port", "8080"), "api_port"),
            (lambda c: setattr(c, "api_port", 65536), "api_port"),
            (lambda c: setattr(c, "api_port", -1), "api_port"),
            (lambda c: setattr(c, "check_interval", 300), "check_interval"),
            (lambda c: setattr(c, "prune_interval", "1h"), "prune_interval"),
            (lambda c: setattr(c.datasets[0], "name", 12), "name"),
            (lambda c: setattr(c.datasets[0], "frequency", 3600), "frequency"),
        ],
        ids=[
            "enabled-int", "enabled-str", "dry-run-int", "prefix-int", "api-host-none",
            "api-port-bool", "api-port-float", "api-port-str", "api-port-high",
            "api-port-negative", "check-interval-int", "prune-interval-str",
            "name-int", "frequency-int",
        ],
    )
    def test_type_checks(self, mutate, key):
        config = good_config()
        mutate(config)
        with pytest.raises(ValueError, match=key):
            validate_config(config)

    @pytest.mark.parametrize("seconds", [0, -60])
    @pytest.mark.parametrize(
        "field, key",
        [
            ("check_interval", "check_interval"),
            ("prune_interval", "prune_interval"),
        ],
    )
    def test_non_positive_global_intervals_are_rejected(self, field, key, seconds):
        config = good_config(**{field: timedelta(seconds=seconds)})
        with pytest.raises(ValueError, match=key):
            validate_config(config)

    @pytest.mark.parametrize("seconds", [0, -60])
    def test_non_positive_dataset_and_remote_frequency_are_rejected(self, seconds):
        config = good_config()
        config.datasets[0].frequency = timedelta(seconds=seconds)
        with pytest.raises(ValueError, match="frequency"):
            validate_config(config)

        config = good_config(destinations={"srv": Destination("https://x")})
        config.datasets[0].remote = [
            RemoteDatasetConfig("srv", frequency=timedelta(seconds=seconds))
        ]
        with pytest.raises(ValueError, match=r"remote\[srv\]\.frequency"):
            validate_config(config)

    def test_remote_frequency_none_means_inherit_and_is_valid(self):
        config = good_config(destinations={"srv": Destination("https://x")})
        config.datasets[0].remote = [RemoteDatasetConfig("srv", frequency=None)]
        assert validate_config(config) == []

    def test_snapshot_prefix_none_hint_text(self):
        with pytest.raises(ValueError) as excinfo:
            validate_config(good_config(snapshot_prefix=None))
        assert '"None"' in str(excinfo.value)
        assert "None_" in str(excinfo.value)

    def test_empty_dataset_list_and_duplicate_names_are_rejected(self):
        with pytest.raises(ValueError, match="No datasets"):
            validate_config(BackupConfig(datasets=[]))
        config = good_config()
        config.datasets.append(DatasetConfig(name="pool/data"))
        with pytest.raises(ValueError, match="duplicate dataset name"):
            validate_config(config)

    def test_undeclared_and_duplicate_remote_destinations_are_rejected(self):
        config = good_config()
        config.datasets[0].remote = [RemoteDatasetConfig("ghost")]
        with pytest.raises(ValueError, match="ghost"):
            validate_config(config)

        config = good_config(destinations={"srv": Destination("https://x")})
        config.datasets[0].remote = [RemoteDatasetConfig("srv"), RemoteDatasetConfig("srv")]
        with pytest.raises(ValueError, match="duplicate destination"):
            validate_config(config)

    def test_keep_for_collision_is_rejected(self):
        config = good_config()
        config.datasets[0].retention_rules = [
            RetentionRule(Duration("24h"), Duration("1M")),
            RetentionRule(Duration("7d"), Duration("30d")),
        ]
        with pytest.raises(ValueError, match="same duration"):
            validate_config(config)

    def test_warning_never_prune_for_empty_dataset_level_rules(self):
        config = good_config()
        config.datasets[0].retention_rules = []
        warnings = validate_config(config)
        assert len(warnings) == 1
        assert "pool/data" in warnings[0] and "never be pruned" in warnings[0]

    def test_prefix_with_underscore_gives_no_warning(self):
        # F1.2: the parser bug the old "contains '_'" warning described is
        # fixed in backup_manager (the prefix may now contain '_'), so the
        # warning is gone. Nothing else about the prefix warns either.
        assert validate_config(good_config(snapshot_prefix="a_b")) == []
        assert validate_config(good_config(snapshot_prefix="_a__b_")) == []

    def test_warning_api_port_zero(self):
        warnings = validate_config(good_config(api_port=0))
        assert warnings == [
            "api_port is 0: the API will bind an ephemeral port that changes on every start"
        ]

    def test_exact_duplicate_rules_warn_naming_both_literals(self):
        config = good_config()
        config.datasets[0].retention_rules = [
            RetentionRule(Duration("1d"), Duration("30d")),
            RetentionRule(Duration("24h"), Duration("1M")),
        ]
        warnings = validate_config(config)
        assert len(warnings) == 1
        message = warnings[0]
        assert "1d:30d" in message and "24h:1M" in message
        assert "keeping 1d:30d" in message

    def test_duplicates_in_a_destination_scope_warn_with_that_scope_key(self):
        config = good_config(destinations={"srv": Destination("https://x")})
        config.datasets[0].remote = [RemoteDatasetConfig(
            "srv",
            retention_rules=[
                RetentionRule(Duration("2d"), Duration("10d")),
                RetentionRule(Duration("48h"), Duration("10d")),
            ],
        )]
        warnings = validate_config(config)
        assert len(warnings) == 1
        assert "remote[srv]" in warnings[0]
        assert "2d:10d" in warnings[0] and "48h:10d" in warnings[0]

    def test_warnings_accumulate(self):
        # The underscore prefix contributes nothing (F1.2); the other two
        # conditions still each warn once.
        config = good_config(snapshot_prefix="a_b", api_port=0)
        config.datasets[0].retention_rules = []
        found = validate_config(config)
        assert len(found) == 2
        assert any("api_port is 0" in m for m in found)
        assert any("never be pruned" in m for m in found)
        assert not any("a_b" in m for m in found)

    def test_save_config_validates_before_it_wipes(self, engine=None):
        # `save_config` takes validate_config's place as step 1 (A-2): a bad
        # config must not delete a good store.
        from sqlalchemy.orm import Session

        from zfsbackup.config.store import Base, make_engine

        eng = make_engine("sqlite:///:memory:", foreign_keys=True)
        Base.metadata.create_all(eng)
        with Session(eng) as session:
            save_config(session, good_config())
            session.commit()
            with pytest.raises(ValueError, match="dry_run"):
                save_config(session, good_config(dry_run=None))
            session.rollback()
            assert load_config(session).datasets[0].name == "pool/data"
        eng.dispose()


# ---------------------------------------------------------------------------
# D2: destinations load in a deterministic order
# ---------------------------------------------------------------------------


class TestDestinationOrder:
    def test_destinations_load_sorted_by_name_not_insertion_order(self, tmp_path):
        # "No changes" detection in the editor compares renders, so the
        # order must not depend on the planner or on rowid.
        path = write(tmp_path, make(destinations={
            "zeta": {"url": "https://z"},
            "mid": {"url": "https://m"},
            "alpha": {"url": "https://a"},
        }))
        loaded = via_import(tmp_path, path)
        assert list(loaded.destinations) == ["alpha", "mid", "zeta"]


# ---------------------------------------------------------------------------
# [] round trip (A3) on in-code configs
# ---------------------------------------------------------------------------


class TestNeverPruneRoundTrip:
    def test_empty_dataset_level_rules_survive_save_then_load(self, caplog):
        from sqlalchemy.orm import Session

        from zfsbackup.config.store import Base, make_engine

        eng = make_engine("sqlite:///:memory:", foreign_keys=True)
        Base.metadata.create_all(eng)
        config = good_config()
        config.datasets[0].retention_rules = []
        with Session(eng) as session:
            save_config(session, config)
            session.commit()
            session.expire_all()
            with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.mapper"):
                loaded = load_config(session)
        eng.dispose()

        assert loaded.datasets[0].retention_rules == []
        assert loaded == config
        assert any("never be pruned" in m for m in warnings_of(caplog))

    def test_destination_scope_empty_still_means_inherit(self):
        from sqlalchemy.orm import Session

        from zfsbackup.config.store import Base, make_engine

        eng = make_engine("sqlite:///:memory:", foreign_keys=True)
        Base.metadata.create_all(eng)
        config = good_config(destinations={"srv": Destination("https://x")})
        config.datasets[0].remote = [RemoteDatasetConfig("srv")]
        with Session(eng) as session:
            save_config(session, config)
            session.commit()
            loaded = load_config(session)
        eng.dispose()
        assert loaded.datasets[0].remote[0].retention_rules == []
        assert loaded.datasets[0].retention_rules == config.datasets[0].retention_rules


# ---------------------------------------------------------------------------
# Importer: phase 0 ordering, cleanup on failure, mode preservation
# ---------------------------------------------------------------------------


COLLIDING_YAML = (
    "datasets:\n"
    "  - name: pool/data\n"
    "    retention:\n"
    "      24h: 1M\n"
    "      7d: 30d\n"
)


class TestImporterFailureHandling:
    def test_from_file_accepts_what_validate_config_rejects(self, tmp_path):
        # The premise of the next test: only the session-free validation
        # catches a `{24h: 1M, 7d: 30d}` keep_for collision.
        path = tmp_path / "collide.yaml"
        path.write_text(COLLIDING_YAML)
        config = BackupConfig.from_file(path)  # lenient on load (the strict check is validate_config's)
        with pytest.raises(ValueError, match="same duration"):
            validate_config(config)

    def test_colliding_yaml_leaves_no_target_behind(self, tmp_path):
        path = tmp_path / "collide.yaml"
        path.write_text(COLLIDING_YAML)
        target = tmp_path / "newdir" / "config.db"

        with pytest.raises(ValueError, match="same duration"):
            import_yaml(path, resolve_config_path(str(target)))

        # Previously found only in phase 3: after the file had been
        # created, migrated and stamped.
        assert not target.exists()
        assert not target.parent.exists()
        assert list(tmp_path.glob("newdir*")) == []

    def test_colliding_yaml_leaves_an_existing_database_untouched(
        self, tmp_path, config_yaml_path
    ):
        target = tmp_path / "config.db"
        resolved = resolve_config_path(str(target))
        import_yaml(config_yaml_path, resolved)
        with open_config_session(resolved, readonly=True) as session:
            before = load_config(session)
        dispose_all()

        path = tmp_path / "collide.yaml"
        path.write_text(COLLIDING_YAML)
        with pytest.raises(ValueError, match="same duration"):
            import_yaml(path, resolved)

        with open_config_session(resolved, readonly=True) as session:
            assert load_config(session) == before

    def test_validate_warnings_are_logged_before_anything_is_written(
        self, tmp_path, caplog
    ):
        path = write(tmp_path, make(api_port=0))
        with caplog.at_level(logging.WARNING):
            via_import(tmp_path, path)
        assert any("api_port is 0" in m for m in warnings_of(caplog))

    def test_an_underscore_prefix_imports_without_any_warning(self, tmp_path, caplog):
        path = write(tmp_path, make(snapshot_prefix="a_b"))
        with caplog.at_level(logging.WARNING):
            loaded = via_import(tmp_path, path)
        assert loaded.snapshot_prefix == "a_b"
        assert not any("a_b" in m or "'_'" in m for m in warnings_of(caplog))

    def _failing_save(self, monkeypatch, target, observed):
        real = importer_module.save_config

        def failing(session, config):
            real(session, config)  # a real write, so -wal/-shm exist
            observed["wal"] = os.path.exists(f"{target}-wal")
            observed["shm"] = os.path.exists(f"{target}-shm")
            raise RuntimeError("injected phase-3 failure")

        monkeypatch.setattr(importer_module, "save_config", failing)

    def test_failed_first_import_removes_the_db_and_its_sidecars(
        self, tmp_path, config_yaml_path, monkeypatch
    ):
        target = tmp_path / "config.db"
        resolved = resolve_config_path(str(target))
        observed = {}
        self._failing_save(monkeypatch, target, observed)

        with pytest.raises(RuntimeError, match="injected"):
            import_yaml(config_yaml_path, resolved)

        # Non-vacuous: the sidecars really existed at the moment of failure.
        assert observed == {"wal": True, "shm": True}
        for suffix in ("", "-wal", "-shm", "-journal"):
            assert not os.path.lexists(f"{target}{suffix}"), suffix

    def test_failed_first_import_can_be_retried_as_a_first_import(
        self, tmp_path, config_yaml_path, monkeypatch
    ):
        target = tmp_path / "config.db"
        resolved = resolve_config_path(str(target))
        with monkeypatch.context() as m:
            self._failing_save(m, target, {})
            with pytest.raises(RuntimeError):
                import_yaml(config_yaml_path, resolved)

        result = import_yaml(config_yaml_path, resolved)
        assert result.created is True

    def test_failed_import_over_existing_db_preserves_it(
        self, tmp_path, config_yaml_path, monkeypatch
    ):
        target = tmp_path / "config.db"
        resolved = resolve_config_path(str(target))
        import_yaml(config_yaml_path, resolved)
        with open_config_session(resolved, readonly=True) as session:
            before = load_config(session)
            generation = session.get(GlobalSettings, 1).generation

        other = write(tmp_path, make(ds={"name": "pool/other"}), name="other.yaml")
        self._failing_save(monkeypatch, target, {})
        with pytest.raises(RuntimeError, match="injected"):
            import_yaml(other, resolved)
        monkeypatch.undo()

        assert target.exists()
        with open_config_session(resolved, readonly=True) as session:
            assert load_config(session) == before
            assert session.get(GlobalSettings, 1).generation == generation

    def test_preexisting_0600_non_default_target_stays_0600_across_reimport(
        self, tmp_path, config_yaml_path, caplog
    ):
        target = tmp_path / "private.db"
        resolved = resolve_config_path(str(target))
        import_yaml(config_yaml_path, resolved)
        dispose_all()
        os.chmod(target, 0o600)

        other = write(tmp_path, make(ds={"name": "pool/other"}), name="other.yaml")
        with caplog.at_level(logging.WARNING):
            result = import_yaml(other, resolved)

        assert result.created is False
        assert (target.stat().st_mode & 0o7777) == 0o600
        # ...and the operator is told that the group is locked out.
        assert any("permissions were left alone" in m for m in warnings_of(caplog))
        with open_config_session(resolved, readonly=True) as session:
            assert [d.name for d in load_config(session).datasets] == ["pool/other"]

    def test_existing_non_default_directory_mode_is_not_widened(
        self, tmp_path, config_yaml_path
    ):
        directory = tmp_path / "home"
        directory.mkdir()
        os.chmod(directory, 0o755)
        resolved = resolve_config_path(str(directory / "x.db"))

        import_yaml(config_yaml_path, resolved)

        assert (directory.stat().st_mode & 0o7777) == 0o755


# ---------------------------------------------------------------------------
# Dataset names the daemon cannot manage (G3 R2): `validate_config` reuses
# libzfseasy's `Validate.dataset`. An ENABLED dataset (or remote_backup target)
# with such a name would make every worker raise and crash-loop the daemon, so
# it is refused at the write boundary; a DISABLED one only warns.
# ---------------------------------------------------------------------------

#: What every refusal/warning says about the name (stable substrings).
CANNOT_MANAGE = "cannot be managed by zfsbackup"
LIBRARY_ACCEPTS = "its ZFS library accepts pool[/component...] in ASCII only, at most 255 characters"
REASON = (
    "(its ZFS library accepts pool[/component...] in ASCII only, at most 255 characters: "
    "the pool name starts with a letter and uses letters, digits and _ . - only, with no "
    "':' or spaces; each component starts with a letter, digit or _ (so '.hidden', '-x' "
    "and ':x' are refused) and may contain ':' and inner spaces but not end with a space; "
    "no leading, trailing or doubled '/'; no '@' or '#'; no newlines or other control "
    "characters)"
)
DISABLED_DATASET = "; the dataset is disabled so it is ignored, but enabling it will be refused"
DISABLED_REMOTE = "; remote_backup is disabled so it is ignored, but enabling it will be refused"

BAD_NAMES = [
    ("leading-space", "tank/ a"),
    ("trailing-space", "tank/a "),
    ("space-in-pool", "my pool/x"),
    ("space-only-component", "tank/ "),
    ("non-ascii", "tank/d\u015b"),
    ("non-ascii-pool", "t\u00e4nk/x"),
    ("trailing-newline", "tank/x\n"),
    ("embedded-newline", "tank/a\nb"),
    ("tab", "tank/a\tb"),
    ("leading-dot", "tank/.hidden"),
    ("leading-dash", "tank/-x"),
    ("leading-colon", "tank/:x"),
    ("colon-in-pool", "tank:x/y"),
    ("colon-in-bare-pool", "tank:x"),
    ("256-characters", "t" + "a" * 255),
    ("trailing-slash", "tank/rb/"),
    ("double-slash", "tank//x"),
    ("at-sign", "tank/a@s"),
    ("hash", "tank/a#h"),
    ("leading-slash", "/tank/x"),
    ("digit-pool", "123"),
]
BAD = [pytest.param(name, id=ident) for ident, name in BAD_NAMES]
GOOD = [
    pytest.param("tank/ok", id="plain"),
    pytest.param("tank/a.b:c_d-e", id="punctuation"),
    pytest.param("null", id="yaml-keyword"),
    # A space is fine INSIDE a component (just not at its start or end).
    pytest.param("tank/a b", id="inner-space"),
    pytest.param("tank/two  spaces/x y", id="inner-spaces-in-several-components"),
    pytest.param("tank/a b/c d e", id="inner-space-nested"),
    pytest.param("tank/x:y", id="colon-inside-a-component"),
    pytest.param("tank/a b/c", id="inner-space-then-plain"),
    pytest.param("tank/_x", id="leading-underscore"),
    pytest.param("tank/9", id="leading-digit"),
    pytest.param("t" + "a" * 254, id="255-characters"),
]


def named(name, *, enabled=True, **over) -> BackupConfig:
    config = good_config(**over)
    config.datasets[0].name = name
    config.datasets[0].enabled = enabled
    return config


class TestDatasetNameValidation:
    def test_the_full_refusal_text(self):
        with pytest.raises(ValueError) as excinfo:
            validate_config(named("tank/ a"))
        assert str(excinfo.value) == (
            "datasets[tank/ a]: dataset name 'tank/ a' cannot be managed by zfsbackup "
            + REASON
        )

    @pytest.mark.parametrize("name", BAD)
    def test_an_enabled_dataset_with_a_bad_name_is_refused_naming_it(self, name):
        with pytest.raises(ValueError) as excinfo:
            validate_config(named(name))
        message = str(excinfo.value)
        assert message.startswith(f"datasets[{name}]: dataset name {name!r} ")
        assert CANNOT_MANAGE in message and LIBRARY_ACCEPTS in message
        assert "is not a valid ZFS dataset name" not in message  # OpenZFS allows e.g. non-ASCII

    @pytest.mark.parametrize("name", GOOD)
    def test_valid_names_pass_without_a_warning(self, name):
        assert validate_config(named(name)) == []

    @pytest.mark.parametrize("name", BAD)
    def test_a_disabled_dataset_with_a_bad_name_only_warns(self, name):
        warnings = validate_config(named(name, enabled=False))
        assert warnings == [
            f"datasets[{name}]: dataset name {name!r} {CANNOT_MANAGE} {REASON}" + DISABLED_DATASET
        ]

    @pytest.mark.parametrize("name", GOOD)
    def test_a_disabled_dataset_with_a_good_name_does_not_warn(self, name):
        assert validate_config(named(name, enabled=False)) == []

    def test_only_the_bad_dataset_among_several_is_named(self):
        config = good_config()
        config.datasets.append(
            DatasetConfig(
                name="tank/ two", frequency=Duration("1h"),
                retention_rules=[RetentionRule(Duration("1d"), Duration("30d"))],
            )
        )
        with pytest.raises(ValueError, match=r"datasets\[tank/ two\]"):
            validate_config(config)

    def test_a_disabled_bad_name_does_not_hide_an_enabled_one(self):
        config = named("tank/ off", enabled=False)
        config.datasets.append(
            DatasetConfig(
                name="tank/ on", frequency=Duration("1h"),
                retention_rules=[RetentionRule(Duration("1d"), Duration("30d"))],
            )
        )
        with pytest.raises(ValueError, match=r"datasets\[tank/ on\]"):
            validate_config(config)

    def test_the_bad_name_is_reported_before_later_problems_in_the_same_dataset(self):
        config = named("tank/ a")
        config.datasets[0].frequency = Duration(seconds=0)  # also invalid, and checked later
        config.datasets[0].retention_rules = []
        with pytest.raises(ValueError) as excinfo:
            validate_config(config)
        assert CANNOT_MANAGE in str(excinfo.value)
        assert "frequency" not in str(excinfo.value)

    def test_the_bad_name_wins_over_the_dataset_s_own_type_errors_that_follow(self):
        config = named("tank/ a")
        config.datasets[0].frequency = 3600  # a type error, checked after the name
        with pytest.raises(ValueError, match=CANNOT_MANAGE):
            validate_config(config)

    # -- remote_backup.target_dataset --------------------------------------

    def test_an_enabled_remote_backup_with_a_bad_target_is_refused(self):
        config = good_config(remote_backup=RemoteServerConfig("tank/rb/", enabled=True))
        with pytest.raises(ValueError) as excinfo:
            validate_config(config)
        assert str(excinfo.value) == (
            "remote_backup.target_dataset 'tank/rb/' " + CANNOT_MANAGE + " " + REASON
        )

    def test_a_disabled_remote_backup_with_a_bad_target_only_warns(self):
        config = good_config(remote_backup=RemoteServerConfig("tank/rb/", enabled=False))
        assert validate_config(config) == [
            f"remote_backup.target_dataset 'tank/rb/' {CANNOT_MANAGE} {REASON}" + DISABLED_REMOTE
        ]

    @pytest.mark.parametrize("enabled", [True, False])
    def test_a_good_target_is_fine(self, enabled):
        config = good_config(remote_backup=RemoteServerConfig("tank/rb", enabled=enabled))
        assert validate_config(config) == []

    @pytest.mark.parametrize("name", BAD)
    def test_every_bad_name_is_refused_as_a_target_too(self, name):
        config = good_config(remote_backup=RemoteServerConfig(name, enabled=True))
        with pytest.raises(ValueError, match=r"remote_backup\.target_dataset"):
            validate_config(config)

    def test_a_remote_backup_flag_is_checked_before_its_name(self):
        config = good_config(remote_backup=RemoteServerConfig("tank/rb/", enabled=None))
        with pytest.raises(ValueError, match="remote_backup.enabled"):
            validate_config(config)

    # -- the validator is libzfseasy's, taken without a zfs binary -----------

    def test_the_rule_is_validate_dataset_of_libzfseasy(self):
        from libzfseasy.types import Validate

        for ident, name in BAD_NAMES + [("ok", "tank/ok"), ("inner", "tank/a b")]:
            try:
                Validate.dataset(name)
                accepted = True
            except ValueError:
                accepted = False
            try:
                validate_config(named(name))
                managed = True
            except ValueError:
                managed = False
            assert accepted == managed, name

    def test_validate_config_works_with_libzfseasy_unimportable(self, tmp_path):
        """A host that only manages configuration has no `zfs`: importing the
        `libzfseasy` package raises there. The validator is then loaded from
        its `types.py` by path, without running the package `__init__`, and
        `libzfseasy` is never put in `sys.modules`."""
        import shutil
        import subprocess
        import sys

        repo = Path(__file__).resolve().parent.parent
        interpreter_dir = str(Path(sys.executable).parent)
        env = {
            "PATH": interpreter_dir,  # NOT the hermetic stub dir: no `zfs` anywhere
            "PYTHONPATH": str(repo),
            "HOME": str(tmp_path),
            "LANG": "C.UTF-8",
        }
        assert shutil.which("zfs", path=interpreter_dir) is None
        assert shutil.which("zpool", path=interpreter_dir) is None
        premise = subprocess.run(
            [sys.executable, "-c", "import libzfseasy"],
            env=env, cwd=str(tmp_path), capture_output=True, text=True, timeout=60,
        )
        assert premise.returncode != 0, "libzfseasy imported without a zfs binary: the premise is gone"

        script = (
            "import json, sys\n"
            "from zfsbackup.config import BackupConfig, DatasetConfig, Duration, RetentionRule\n"
            "from zfsbackup.config.store import validate_config\n"
            "def cfg(name, enabled=True):\n"
            "    return BackupConfig(snapshot_prefix='a', datasets=[DatasetConfig(name=name,\n"
            "        enabled=enabled, frequency=Duration('1h'),\n"
            "        retention_rules=[RetentionRule(Duration('1d'), Duration('30d'))])])\n"
            "out = {'ok': validate_config(cfg('tank/ok'))}\n"
            "try:\n"
            "    validate_config(cfg('tank/ a'))\n"
            "    out['bad'] = None\n"
            "except ValueError as exc:\n"
            "    out['bad'] = str(exc)\n"
            "out['disabled'] = validate_config(cfg('tank/ a', False))\n"
            "out['libzfseasy_loaded'] = any(m == 'libzfseasy' or m.startswith('libzfseasy.') for m in sys.modules)\n"
            "print(json.dumps(out))\n"
        )
        import json

        result = subprocess.run(
            [sys.executable, "-c", script],
            env=env, cwd=str(tmp_path), capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, result.stderr
        out = json.loads(result.stdout.strip().splitlines()[-1])
        assert out["ok"] == []
        assert out["bad"].startswith("datasets[tank/ a]: dataset name 'tank/ a' " + CANNOT_MANAGE)
        assert len(out["disabled"]) == 1 and out["disabled"][0].endswith(DISABLED_DATASET)
        assert out["libzfseasy_loaded"] is False


# -- the write paths -----------------------------------------------------------


def _stored(tmp_path, name="store/config.db"):
    """A database holding the `make()` configuration; -> (resolved, snapshot)."""
    from zfsbackup.config.store import read_config

    resolved = resolve_config_path(str(tmp_path / name))
    import_yaml(write(tmp_path, make()), resolved)
    dispose_all()
    snapshot = read_config(resolved)
    dispose_all()
    return resolved, snapshot


def _fingerprint(resolved):
    import hashlib

    dispose_all()
    path = Path(str(resolved.resolved_path))
    sidecars = sorted(
        p.name for p in path.parent.iterdir()
        if p.name.startswith(path.name + "-") or ".pre-import-" in p.name
    )
    return hashlib.sha256(path.read_bytes()).hexdigest(), sidecars


class TestBadNamesNeverReachTheDatabase:
    @pytest.mark.parametrize("name", [pytest.param("tank/ a", id="leading-space"), pytest.param("tank/x/", id="slash")])
    def test_write_config_refuses_and_leaves_the_database_unchanged(self, tmp_path, name):
        from zfsbackup.config.store import read_config, write_config

        resolved, snapshot = _stored(tmp_path)
        before = _fingerprint(resolved)
        with pytest.raises(ValueError, match=CANNOT_MANAGE):
            write_config(
                resolved, named(name), expected_generation=snapshot.generation,
                expected_store_id=snapshot.store_id,
            )
        assert _fingerprint(resolved) == before
        after = read_config(resolved)
        dispose_all()
        assert after.generation == snapshot.generation and after.config == snapshot.config

    def test_write_config_accepts_the_same_name_disabled_and_warns_via_validate(self, tmp_path):
        from zfsbackup.config.store import read_config, write_config

        resolved, snapshot = _stored(tmp_path)
        result = write_config(
            resolved, named("tank/ a", enabled=False), expected_generation=snapshot.generation
        )
        assert result.generation == snapshot.generation + 1
        stored = read_config(resolved).config
        dispose_all()
        assert stored.datasets[0].name == "tank/ a" and stored.datasets[0].enabled is False

    def test_write_config_refuses_a_bad_remote_backup_target(self, tmp_path):
        from zfsbackup.config.store import write_config

        resolved, snapshot = _stored(tmp_path)
        before = _fingerprint(resolved)
        bad = good_config(remote_backup=RemoteServerConfig("tank/rb/", enabled=True))
        bad.datasets[0].name = "pool/data"
        with pytest.raises(ValueError, match=r"remote_backup\.target_dataset"):
            write_config(resolved, bad, expected_generation=snapshot.generation)
        assert _fingerprint(resolved) == before

    def test_save_config_refuses_and_writes_nothing(self, tmp_path):
        from zfsbackup.config.store import read_config

        resolved, snapshot = _stored(tmp_path)
        before = _fingerprint(resolved)
        with pytest.raises(ValueError, match=CANNOT_MANAGE):
            with open_config_session(resolved, readonly=False) as session:
                save_config(session, named("tank/ a"))
        assert _fingerprint(resolved) == before
        after = read_config(resolved)
        dispose_all()
        assert after.config == snapshot.config and after.generation == snapshot.generation

    def test_import_refuses_a_bad_enabled_name_and_leaves_the_database_unchanged(self, tmp_path):
        resolved, snapshot = _stored(tmp_path)
        before = _fingerprint(resolved)
        called = []
        bad = write(tmp_path, make(ds={"name": "tank/ a"}), "bad.yaml")
        with pytest.raises(ValueError, match=CANNOT_MANAGE):
            import_yaml(bad, resolved, backup=lambda outgoing: called.append(outgoing) or tmp_path / "x")
        assert called == [], "a refused import must not even write the outgoing backup"
        assert _fingerprint(resolved) == before

    def test_import_accepts_the_same_name_when_it_is_disabled(self, tmp_path, caplog):
        resolved, _ = _stored(tmp_path)
        ok = write(tmp_path, make(ds={"name": "tank/ a", "enabled": False}), "ok.yaml")
        with caplog.at_level(logging.WARNING):
            import_yaml(ok, resolved)
        assert any(CANNOT_MANAGE in m and "enabling it will be refused" in m for m in warnings_of(caplog))

    def test_a_refused_first_import_leaves_nothing_behind(self, tmp_path):
        target = tmp_path / "newdir" / "config.db"
        bad = write(tmp_path, make(ds={"name": "tank/ a"}), "bad.yaml")
        with pytest.raises(ValueError, match=CANNOT_MANAGE):
            import_yaml(bad, resolve_config_path(str(target)))
        assert not target.exists() and not target.parent.exists()
        assert list(tmp_path.glob("newdir*")) == []

    def test_a_refused_import_over_a_bad_remote_backup_target_leaves_nothing_behind(self, tmp_path):
        target = tmp_path / "newdir" / "config.db"
        data = make(remote_backup={"target_dataset": "tank/rb/", "enabled": True})
        with pytest.raises(ValueError, match=r"remote_backup\.target_dataset"):
            import_yaml(write(tmp_path, data, "bad.yaml"), resolve_config_path(str(target)))
        assert not target.parent.exists()


class TestSpaceInsideAComponentEndToEnd:
    """3c: libzfseasy now accepts a space INSIDE a dataset component, so the
    store does too -- import -> load -> validate -> write, nothing refused and
    nothing warned about. (Edge spaces are refused: see the BAD list above.)"""

    NAME = "tank/my data/two  words"

    def test_import_load_validate(self, tmp_path, caplog):
        path = write(tmp_path, make(ds={"name": self.NAME}))
        assert BackupConfig.from_file(path).datasets[0].name == self.NAME
        with caplog.at_level(logging.WARNING):
            loaded = via_import(tmp_path, path)
        assert loaded.datasets[0].name == self.NAME
        assert loaded.datasets[0].enabled is True
        assert validate_config(loaded) == []
        assert [m for m in warnings_of(caplog) if CANNOT_MANAGE in m] == []

    def test_write_config_round_trips_it(self, tmp_path):
        from zfsbackup.config.store import read_config, write_config

        resolved, snapshot = _stored(tmp_path)
        write_config(resolved, named(self.NAME), expected_generation=snapshot.generation)
        stored = read_config(resolved).config
        dispose_all()
        assert stored.datasets[0].name == self.NAME and stored.datasets[0].enabled is True

    def test_a_remote_backup_target_with_an_inner_space_is_accepted(self):
        config = good_config(remote_backup=RemoteServerConfig("tank/rb dir", enabled=True))
        assert validate_config(config) == []

    @pytest.mark.parametrize("name", ["tank/a ", "tank/ a", "tank/a /b", "tank/a/ b"])
    def test_a_space_at_either_edge_of_any_component_is_refused(self, name):
        with pytest.raises(ValueError, match=CANNOT_MANAGE):
            validate_config(named(name))

    def test_the_refusal_names_the_new_rule(self):
        with pytest.raises(ValueError) as excinfo:
            validate_config(named("my pool/x"))
        message = str(excinfo.value)
        assert "in ASCII only" in message
        assert "may contain ':' and inner spaces but not end with a space" in message
        assert "with no ':' or spaces" in message  # the pool name
        assert "at most 255 characters" in message
        assert "no newlines or other control characters" in message
        # The old wording said "no spaces" for every name; only the pool name
        # still has none.
        assert "only, no spaces" not in message

    def test_a_trailing_newline_is_no_longer_an_accepted_quirk(self, tmp_path):
        # YAML `"tank/x\n"`: Validate.dataset used `$`, which matched before
        # a trailing newline; `re.fullmatch` does not.
        path = write(tmp_path, make(ds={"name": "tank/x\n"}))
        with pytest.raises(ValueError, match=CANNOT_MANAGE):
            via_import(tmp_path, path)

    def test_non_ascii_other_values_are_still_fine(self, tmp_path):
        # Only dataset NAMES are ASCII-bound (they reach `zfs`); a destination
        # name and URL are free text and still round-trip.
        data = make(destinations={"d\u00e9p\u00f4t": {"url": "https://127.0.0.1:9/caf\u00e9"}})
        loaded = via_import(tmp_path, write(tmp_path, data))
        assert loaded.destinations["d\u00e9p\u00f4t"].url == "https://127.0.0.1:9/caf\u00e9"


# -- through the CLI --------------------------------------------------------------


class TestBadNamesThroughTheCli:
    """`error: datasets[...]: ...` and exit 1, nothing written, nothing announced."""

    OFF = make(ds={"name": "tank/ a", "enabled": False})

    @pytest.fixture
    def off(self, tmp_path):
        """A database whose only dataset has a bad name but is DISABLED (the
        import accepts it with a warning)."""
        from click.testing import CliRunner

        from zfsbackup.cli.main import cli

        db = tmp_path / "cfg" / "config.db"
        source = write(tmp_path, self.OFF)
        result = CliRunner().invoke(cli, ["import", str(source), "-c", str(db)], prog_name="zfsbackup-config")
        assert result.exit_code == 0, result.stderr
        assert CANNOT_MANAGE in result.stderr and "enabling it will be refused" in result.stderr
        dispose_all()
        return db

    @staticmethod
    def invoke(*args, env=None, input=None):
        from click.testing import CliRunner

        from zfsbackup.cli.main import cli

        return CliRunner().invoke(
            cli, [str(a) for a in args], env=env, input=input, prog_name="zfsbackup-config"
        )

    @staticmethod
    def digest(db):
        import hashlib

        dispose_all()
        return hashlib.sha256(Path(db).read_bytes()).hexdigest()

    def test_set_that_enables_a_bad_name_is_refused(self, off):
        before = self.digest(off)
        result = self.invoke("set", "datasets[tank/ a].enabled", "true", "-c", off)
        assert result.exit_code == 1, result.stdout
        assert result.stderr.startswith("error: datasets[tank/ a]: dataset name 'tank/ a' " + CANNOT_MANAGE)
        assert "config updated" not in result.stdout
        assert self.digest(off) == before

    def test_set_of_something_else_is_not_blocked_by_a_standing_disabled_bad_name(self, off):
        """The disabled dataset was accepted (with a warning) at import; an
        unrelated edit neither refuses nor repeats that standing warning."""
        result = self.invoke("set", "check_interval", "10m", "-c", off)
        assert result.exit_code == 0, result.stderr
        assert CANNOT_MANAGE not in result.stderr

    def test_edit_that_enables_a_bad_name_is_refused(self, off, tmp_path):
        from _cli_support import Stub

        stub = Stub(tmp_path)
        stub.plan([{"replace": ["enabled: false", "enabled: true"]}])
        before = self.digest(off)
        result = self.invoke("edit", "--no-prompt", "-c", off, env=stub.env(off))
        assert result.exit_code == 1, result.stdout
        assert "error: datasets[tank/ a]: dataset name 'tank/ a' " + CANNOT_MANAGE in result.stderr
        assert self.digest(off) == before

    def test_edit_that_adds_a_bad_dataset_is_refused(self, off, tmp_path):
        from _cli_support import Stub

        stub = Stub(tmp_path)
        stub.plan([{"append": '  - name: "tank/new#x"\n    frequency: "1h"\n'}])
        result = self.invoke("edit", "--no-prompt", "-c", off, env=stub.env(off))
        assert result.exit_code == 1
        assert "error: datasets[tank/new#x]: dataset name 'tank/new#x' " + CANNOT_MANAGE in result.stderr

    def test_import_of_a_bad_enabled_name_is_refused(self, off, tmp_path):
        before = self.digest(off)
        bad = write(tmp_path, make(ds={"name": "tank/ a"}), "bad.yaml")
        result = self.invoke("import", bad, "-c", off)
        assert result.exit_code == 1, result.stdout
        assert "error: datasets[tank/ a]: dataset name 'tank/ a' " + CANNOT_MANAGE in result.stderr
        assert "config updated" not in result.stdout
        assert self.digest(off) == before
        assert list(Path(off).parent.glob("*.pre-import-*.yaml")) == []

    def test_a_first_import_of_a_bad_enabled_name_creates_no_database(self, tmp_path):
        target = tmp_path / "fresh" / "config.db"
        bad = write(tmp_path, make(ds={"name": "tank/ a"}), "bad.yaml")
        result = self.invoke("import", bad, "-c", target)
        assert result.exit_code == 1
        assert result.stderr.startswith("error: datasets[tank/ a]: ")
        assert not target.parent.exists()

    def test_the_target_dataset_of_an_enabled_remote_backup_is_refused_on_import(self, tmp_path):
        data = make(remote_backup={"target_dataset": "tank/rb/", "enabled": True})
        result = self.invoke("import", write(tmp_path, data, "bad.yaml"), "-c", tmp_path / "x" / "config.db")
        assert result.exit_code == 1
        assert result.stderr.startswith("error: remote_backup.target_dataset 'tank/rb/' " + CANNOT_MANAGE)


# ---------------------------------------------------------------------------
# L3: a database written BEFORE the stricter validator
# ---------------------------------------------------------------------------


class TestPreexistingBadNamesInTheStore:
    """Dataset names the old validator accepted (non-ASCII, a trailing
    newline) can already sit in a store as ENABLED rows. Nothing rewrites
    them: reading works, but every consumer that validates refuses with the
    clean message, until the operator disables (or renames) the dataset.
    The row is written straight into SQLite, bypassing every write-path check,
    exactly as an older version of the tool would have left it."""

    NAMES = [
        pytest.param("tank/café", id="non-ascii"),
        pytest.param("tank/x\n", id="trailing-newline"),
    ]

    @pytest.fixture
    def old_db(self, tmp_path, request):
        import sqlite3

        name = request.getfixturevalue("name")
        db = tmp_path / "cfg" / "config.db"
        source = write(tmp_path, make(ds={"name": "tank/seed"}))
        resolved = resolve_config_path(str(db))
        import_yaml(source, resolved)
        dispose_all()
        conn = sqlite3.connect(str(db))
        try:
            changed = conn.execute(
                "UPDATE datasets SET name = ? WHERE name = ?", (name, "tank/seed")
            ).rowcount
            conn.commit()
        finally:
            conn.close()
        assert changed == 1
        return db

    @staticmethod
    def invoke(*args):
        from click.testing import CliRunner

        from zfsbackup.cli.main import cli

        return CliRunner().invoke(
            cli, [str(a) for a in args], prog_name="zfsbackup-config"
        )

    @staticmethod
    def digest(db):
        import hashlib

        dispose_all()
        return hashlib.sha256(Path(db).read_bytes()).hexdigest()

    @pytest.mark.parametrize("name", NAMES)
    def test_load_config_succeeds_and_keeps_the_name(self, old_db, name):
        resolved = resolve_config_path(str(old_db))
        with open_config_session(resolved, readonly=True) as session:
            loaded = load_config(session)
        assert [d.name for d in loaded.datasets] == [name]
        assert loaded.datasets[0].enabled is True

    @pytest.mark.parametrize("name", NAMES)
    def test_validate_config_refuses_with_the_clean_message(self, old_db, name):
        resolved = resolve_config_path(str(old_db))
        with open_config_session(resolved, readonly=True) as session:
            loaded = load_config(session)
        with pytest.raises(ValueError) as excinfo:
            validate_config(loaded)
        message = str(excinfo.value)
        # The name is repr()ed in the sentence: a newline shows as `\n`.
        assert f"dataset name {name!r} {CANNOT_MANAGE} {REASON}" in message
        if "\n" in name:
            assert "'tank/x\\n'" in message

    @pytest.mark.parametrize("name", NAMES)
    def test_every_cli_write_is_refused_and_changes_nothing(self, old_db, name):
        before = self.digest(old_db)
        result = self.invoke("set", "check_interval", "10m", "-c", old_db)
        assert result.exit_code == 1, result.stdout
        assert result.stderr.startswith("error: datasets[")
        assert CANNOT_MANAGE in result.stderr
        assert "config updated" not in result.stdout
        assert self.digest(old_db) == before

    @pytest.mark.parametrize("name", NAMES)
    def test_disabling_the_dataset_unblocks_writes_and_the_name_only_warns(
        self, old_db, name
    ):
        result = self.invoke("set", f"datasets[{name}].enabled", "false", "-c", old_db)
        assert result.exit_code == 0, result.stderr
        assert result.stderr.count("enabling it will be refused") == 1, result.stderr

        # ...now an unrelated write goes through...
        result = self.invoke("set", "check_interval", "10m", "-c", old_db)
        assert result.exit_code == 0, result.stderr
        assert CANNOT_MANAGE not in result.stderr  # the standing warning is not repeated

        # ...and the stored row is the disabled bad name, which only warns.
        resolved = resolve_config_path(str(old_db))
        with open_config_session(resolved, readonly=True) as session:
            loaded = load_config(session)
        assert loaded.datasets[0].name == name and loaded.datasets[0].enabled is False
        warnings = validate_config(loaded)
        assert len(warnings) == 1
        assert CANNOT_MANAGE in warnings[0] and warnings[0].endswith(DISABLED_DATASET)

    @pytest.mark.parametrize("name", NAMES)
    def test_the_daemon_refuses_the_config_with_the_message(self, old_db, name, tmp_path):
        import subprocess
        import sys

        from _hermetic import hermetic_env

        repo = Path(__file__).resolve().parent.parent
        env = hermetic_env(PYTHONPATH=str(repo))
        result = subprocess.run(
            [sys.executable, "-m", "zfsbackup.daemon", "--test-config", "-c", str(old_db)],
            env=env, cwd=str(tmp_path), capture_output=True, text=True, timeout=120,
        )
        assert result.returncode == 1, (result.stdout, result.stderr)
        output = result.stdout + result.stderr
        assert CANNOT_MANAGE in output and "in ASCII only" in output

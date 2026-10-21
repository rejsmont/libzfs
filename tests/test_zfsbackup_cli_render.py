"""`zfsbackup.cli.render` -- the one YAML serialiser the config CLI has.

The contract (docs/config_db_cli_plan.md, item 10.2): for any config `c`,

    BackupConfig.from_file(render_yaml(c)) == c          (value)
    ...and every `Duration.literal` is the same          (literal)
    render_yaml(from_file(render_yaml(c))) == render_yaml(c)   (a fixed point)

`Duration.__eq__` ignores literals, so a `30d` that came back as `1M` would
compare equal and still be a silent edit of the operator's configuration --
every round trip here therefore compares the literals explicitly.

Sections: ``TestRoundTrip`` (the repo YAMLs, a hand-built hard case and a
seeded random property), ``TestLayout`` (what the text looks like),
``TestRefusals`` (what it will not write), and ``TestThroughTheDatabase``
(import -> export through the CLI and the store: the same contract with the
SQLite mapper in the middle).

Mutation counts (scratch copy, one revert at a time; failing tests across this
file and `test_zfsbackup_cli.py`): no `retention: forever` 46; `enabled`
omitted 48; `dry_run` omitted when false 1; destinations unsorted 10; remote
order sorted 28; `prune_interval` filled in from `check_interval` 2;
`client_id_file` always written 33 (pre-TLS; now `client_cert_file`/`client_key_file`, see
the TLS mutation counts below); `remote_backup` omitted 33; canonical
literal instead of the original one 62; durations unquoted 1; names unquoted 1
/ destination keys unquoted 1; retention not sorted by age 1; destination
retention always written 59 / destination frequency always written 70;
same-age/different-keep_for not refused 1; sub-second silently truncated 5;
`api_port` quoted 1.
"""

import random
import re
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from zfsbackup.cli.main import cli
from zfsbackup.cli.render import duration_literal, render_yaml
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
    RemoteServerConfig,
    RetentionRule,
)
from zfsbackup.config.store import CONFIG_PATH_ENV, dispose_all
from zfsbackup.config.store import paths as store_paths

REPO = Path(__file__).resolve().parent.parent
REPO_YAMLS = [REPO / "zfsbackup" / "config.example.yaml", REPO / "zfsbackup" / "config.test.yaml"]

D = Duration

#: 64 hex digits that are all decimal: YAML reads an unquoted one as an int.
DIGIT_FINGERPRINT = "1234567890" * 6 + "1234"


@pytest.fixture(autouse=True)
def _hermetic(tmp_path, monkeypatch):
    monkeypatch.delenv(CONFIG_PATH_ENV, raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(store_paths, "DEFAULT_CONFIG_DB", tmp_path / "default" / "config.db")
    yield
    dispose_all()


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _lit(value):
    """The literal of a `Duration`; fails loudly if a plain `timedelta` came
    back from `from_file` (it must always be a `Duration`)."""
    assert isinstance(value, Duration), f"{value!r} is not a Duration"
    return value.literal


def literals(config: BackupConfig):
    """Every duration literal in `config`, in a stable order and with a label."""
    out = [("check_interval", _lit(config.check_interval))]
    out.append((
        "prune_interval",
        None if config.prune_interval is None else _lit(config.prune_interval),
    ))
    for ds in config.datasets:
        out.append((f"{ds.name}.frequency", _lit(ds.frequency)))
        out.append((
            f"{ds.name}.retention",
            [(_lit(r.age), _lit(r.keep_for)) for r in ds.retention_rules],
        ))
        for remote in ds.remote:
            out.append((
                f"{ds.name}.{remote.destination}.frequency",
                None if remote.frequency is None else _lit(remote.frequency),
            ))
            out.append((
                f"{ds.name}.{remote.destination}.retention",
                [(_lit(r.age), _lit(r.keep_for)) for r in remote.retention_rules],
            ))
    return out


def reload(text: str, tmp_path: Path) -> BackupConfig:
    path = tmp_path / "rendered.yaml"
    path.write_text(text)
    return BackupConfig.from_file(path)


def assert_round_trip(config: BackupConfig, tmp_path: Path, *, check_literals=True) -> str:
    text = render_yaml(config)
    back = reload(text, tmp_path)
    assert back == config
    if check_literals:
        assert literals(back) == literals(config)
    assert render_yaml(back) == text, "render is not a fixed point"
    return text


def synthetic() -> BackupConfig:
    """The hard case: non-canonical literals, destinations declared out of
    order, a per-destination frequency AND retention override next to a plain
    inherit, never-prune, names YAML would misread, and every optional field."""
    return BackupConfig(
        snapshot_prefix="snap:#1",
        check_interval=D("90m"),
        prune_interval=D("30d"),
        api_host="::1",
        api_port=9090,
        dry_run=True,
        client_cert_file=Path("/var/lib/zfsbackup/client cert.pem"),
        client_key_file=Path("/var/lib/zfsbackup/client#key.pem"),
        destinations={
            # a pin (given in the colon / upper-case form: the model normalises it)
            "zeta": Destination("https://zeta.example:8080/a?b=c#frag", ":".join(["AB"] * 32)),
            "alpha": Destination("https://alpha.example"),
            # an all-digit pin: unquoted YAML would read it back as an integer
            "123": Destination("https://numeric", DIGIT_FINGERPRINT),
        },
        remote_backup=RemoteServerConfig(target_dataset="tank/received", enabled=False),
        datasets=[
            DatasetConfig(
                name='tank/a"b:c#d.e',
                recursive=True,
                enabled=False,
                frequency=D("30d"),
                retention_rules=[
                    RetentionRule(D("90m"), D("12h6d")),
                    RetentionRule(D("1d"), D("1M")),
                    RetentionRule(D("1w"), D("45d")),
                ],
                remote=[
                    # inherits both frequency and retention
                    RemoteDatasetConfig("zeta"),
                    # overrides both
                    RemoteDatasetConfig(
                        "alpha",
                        frequency=D("12h6d"),
                        retention_rules=[
                            RetentionRule(D("2h"), D("30d")),
                            RetentionRule(D("6d12h"), D("90m")),
                        ],
                    ),
                    # overrides only the retention
                    RemoteDatasetConfig(
                        "123", retention_rules=[RetentionRule(D("1M"), D("1y"))]
                    ),
                ],
            ),
            DatasetConfig(name="null", frequency=D("45s"), retention_rules=[]),
            DatasetConfig(
                # Not a valid ZFS name, so DISABLED: `validate_config` (every DB
                # path runs it) refuses an enabled dataset libzfseasy would
                # reject, and only warns about a disabled one. The point here is
                # the YAML quoting of the string.
                name="it's #1: x.y",
                enabled=False,
                frequency=D("1h"),
                retention_rules=[RetentionRule(D("1d"), D("30d"))],
                remote=[RemoteDatasetConfig("alpha", frequency=D("1d"))],
            ),
            DatasetConfig(
                name="tank/ünï/ö",  # not ASCII: disabled for the same reason
                enabled=False,
                recursive=True,
                frequency=D("5m"),
                retention_rules=[RetentionRule(D("30d"), D("30d"))],
            ),
        ],
    )


# --------------------------------------------------------------------------
# the round trip
# --------------------------------------------------------------------------


class TestRoundTrip:
    @pytest.mark.parametrize("source", REPO_YAMLS, ids=lambda p: p.name)
    def test_repo_yamls_round_trip_in_value_and_literal(self, source, tmp_path):
        config = BackupConfig.from_file(source)
        assert_round_trip(config, tmp_path)

    @pytest.mark.parametrize("source", REPO_YAMLS, ids=lambda p: p.name)
    def test_repo_yaml_literals_are_kept_verbatim(self, source, tmp_path):
        original = yaml.safe_load(source.read_text())
        rendered = yaml.safe_load(render_yaml(BackupConfig.from_file(source)))
        assert rendered["check_interval"] == original["check_interval"]
        for have, want in zip(rendered["datasets"], original["datasets"]):
            assert have["name"] == want["name"]
            assert have["frequency"] == want["frequency"]
            assert have["retention"] == want["retention"]
            assert list(have["retention"]) == list(want["retention"])

    def test_synthetic_config_round_trips_in_value_and_literal(self, tmp_path):
        assert_round_trip(synthetic(), tmp_path)

    def test_non_canonical_literals_survive_exactly(self, tmp_path):
        config = synthetic()
        back = reload(render_yaml(config), tmp_path)
        by_name = {d.name: d for d in back.datasets}
        weird = by_name['tank/a"b:c#d.e']
        # 30d must not become 1M, 90m must not become 1h30m, 12h6d must not be normalised.
        assert _lit(weird.frequency) == "30d"
        assert _lit(back.check_interval) == "90m"
        assert _lit(back.prune_interval) == "30d"
        assert [(_lit(r.age), _lit(r.keep_for)) for r in weird.retention_rules] == [
            ("90m", "12h6d"), ("1d", "1M"), ("1w", "45d"),
        ]
        alpha = next(r for r in weird.remote if r.destination == "alpha")
        assert _lit(alpha.frequency) == "12h6d"
        assert [(_lit(r.age), _lit(r.keep_for)) for r in alpha.retention_rules] == [
            ("2h", "30d"), ("6d12h", "90m"),
        ]

    def test_destination_override_versus_inherit_is_preserved(self, tmp_path):
        back = reload(render_yaml(synthetic()), tmp_path)
        remotes = {r.destination: r for r in back.datasets[0].remote}
        assert remotes["zeta"].frequency is None and remotes["zeta"].retention_rules == []
        assert remotes["alpha"].frequency is not None and remotes["alpha"].retention_rules
        assert remotes["123"].frequency is None and remotes["123"].retention_rules
        # An override that happens to equal the dataset's rules is still an override.
        same = BackupConfig(
            datasets=[DatasetConfig(
                name="tank/a",
                retention_rules=[RetentionRule(D("1d"), D("30d"))],
                remote=[RemoteDatasetConfig(
                    "nas", frequency=D("1h"),
                    retention_rules=[RetentionRule(D("1d"), D("30d"))],
                )],
            )],
            destinations={"nas": Destination("https://nas")},
        )
        again = reload(render_yaml(same), tmp_path)
        assert again.datasets[0].remote[0].retention_rules
        assert _lit(again.datasets[0].remote[0].frequency) == "1h"

    def test_never_prune_round_trips_as_an_empty_rule_list(self, tmp_path):
        config = BackupConfig(datasets=[DatasetConfig(name="tank/x", retention_rules=[])])
        text = render_yaml(config)
        assert "retention: forever" in text
        back = reload(text, tmp_path)
        assert back.datasets[0].retention_rules == []
        # ...which is NOT the 1d -> 30d default an absent key would give.
        assert render_yaml(back) == text

    def test_names_that_yaml_would_misread_stay_strings(self, tmp_path):
        names = ["null", "123", "yes", "No", "~", "1e3", "2026-10-17", "on", "tank/#c", "a: b", "- x", "&a", "*a", "! x", "'", '"', "é"]
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name=n, retention_rules=[RetentionRule(D("1d"), D("30d"))],
                    remote=[RemoteDatasetConfig(n)],
                )
                for n in names
            ],
            destinations={n: Destination(f"https://{i}") for i, n in enumerate(names)},
            snapshot_prefix="true",
            api_host="0",
        )
        back = reload(render_yaml(config), tmp_path)
        assert [d.name for d in back.datasets] == names
        assert set(back.destinations) == set(names)
        assert back.snapshot_prefix == "true" and back.api_host == "0"
        assert back == config

    @pytest.mark.parametrize("seed", range(60))
    def test_random_configs_round_trip(self, seed, tmp_path):
        config = random_config(random.Random(seed))
        assert_round_trip(config, tmp_path)


# Pairwise distinct in seconds (no "1w"/"7d" pair, no "1M"/"30d" pair), so that
# any subset is a valid rule set for the store's one-rule-per-age/keep_for check.
_LITERALS = ["45s", "1m", "5m", "90m", "2h", "12h6d", "1d", "1w", "30d", "3M", "1y", "10y", "2w3d"]
_NAMES = [
    "tank/a", "tank/b.c", 'tank/q"uote', "tank/c:d", "tank/#hash", "pool/it's",
    "null", "123", "yes", "tank/ünï", "a b", "tank/x-y_z", "T/UPPER",
]


def _zfs_valid(name: str) -> bool:
    """Would libzfseasy accept `name` as a dataset name? (Independent of the
    store's own check: this is the validator the daemon builds datasets with.)"""
    from libzfseasy.types import Validate

    try:
        Validate.dataset(name)
    except ValueError:
        return False
    return True


def _random_remote_backup(rng: random.Random):
    if rng.random() < 0.5:
        target = rng.choice(_NAMES)
        enabled = rng.random() < 0.5 and _zfs_valid(target)
        return RemoteServerConfig(target, enabled=enabled)
    return None


#: A pin, or `None` (not enrolled); includes an all-digit and an all-letter one.
_FINGERPRINTS = [None, None, "ab" * 32, DIGIT_FINGERPRINT, "0123456789abcdef" * 4, "e" * 64, "9" * 64]


def random_config(rng: random.Random) -> BackupConfig:
    """A random but VALID configuration: `validate_config` refuses an ENABLED
    dataset (or `remote_backup`) whose name libzfseasy rejects, so the
    YAML-hostile names in `_NAMES` are only ever used disabled. (The random
    draws are unchanged: the flag is decided after them.)"""
    def rules(max_rules=4):
        count = rng.randint(1, max_rules)
        ages = rng.sample(_LITERALS, count)
        keeps = rng.sample(_LITERALS, count)
        out = [RetentionRule(D(a), D(k)) for a, k in zip(ages, keeps)]
        return sorted(out, key=lambda r: r.age)

    dest_names = rng.sample(_NAMES, rng.randint(0, 4))
    destinations = {
        n: Destination(f"https://h{i}:{8000 + i}", rng.choice(_FINGERPRINTS))
        for i, n in enumerate(dest_names)
    }
    tls = rng.random() < 0.5
    datasets = []
    for name in rng.sample(_NAMES, rng.randint(1, 6)):
        remote = []
        for dest in rng.sample(dest_names, rng.randint(0, len(dest_names))):
            remote.append(RemoteDatasetConfig(
                dest,
                frequency=D(rng.choice(_LITERALS)) if rng.random() < 0.5 else None,
                retention_rules=rules() if rng.random() < 0.5 else [],
            ))
        recursive = rng.random() < 0.5
        enabled = rng.random() < 0.7 and _zfs_valid(name)
        datasets.append(DatasetConfig(
            name=name,
            recursive=recursive,
            enabled=enabled,
            frequency=D(rng.choice(_LITERALS)),
            retention_rules=rules() if rng.random() < 0.8 else [],
            remote=remote,
        ))
    return BackupConfig(
        datasets=datasets,
        snapshot_prefix=rng.choice(["autosnap", "x:y", "#p", "zb", "true"]),
        check_interval=D(rng.choice(_LITERALS)),
        prune_interval=D(rng.choice(_LITERALS)) if rng.random() < 0.5 else None,
        api_host=rng.choice(["127.0.0.1", "0.0.0.0", "::1", "host.example"]),
        api_port=rng.choice([1, 80, 8080, 65535]),
        dry_run=rng.random() < 0.5,
        destinations=destinations,
        remote_backup=_random_remote_backup(rng),
        client_cert_file=Path(rng.choice(["/tmp/c.pem", "/a b/c#d.pem"])) if tls else None,
        client_key_file=Path(rng.choice(["/tmp/k.key", "/a b/k:1.key"])) if tls else None,
    )


# --------------------------------------------------------------------------
# layout
# --------------------------------------------------------------------------


class TestLayout:
    def test_top_level_key_order(self):
        doc = yaml.safe_load(render_yaml(synthetic()))
        assert list(doc) == [
            "snapshot_prefix", "check_interval", "prune_interval", "api_host", "api_port",
            "dry_run", "client_cert_file", "client_key_file", "destinations", "remote_backup",
            "datasets",
        ]

    def test_dataset_key_order(self):
        doc = yaml.safe_load(render_yaml(synthetic()))
        assert list(doc["datasets"][0]) == [
            "name", "enabled", "frequency", "recursive", "retention", "remote",
        ]
        assert list(doc["datasets"][0]["remote"][1]) == ["destination", "frequency", "retention"]

    def test_prune_interval_and_tls_paths_omitted_when_none(self):
        config = BackupConfig(datasets=[DatasetConfig(name="tank/a")])
        text = render_yaml(config)
        for key in ("prune_interval", "client_cert_file", "client_key_file", "server_fingerprint"):
            assert key not in text
        doc = yaml.safe_load(text)
        assert "prune_interval" not in doc and "client_cert_file" not in doc and "client_key_file" not in doc

    def test_prune_interval_and_tls_paths_written_when_set(self):
        doc = yaml.safe_load(render_yaml(synthetic()))
        assert doc["prune_interval"] == "30d"
        assert doc["client_cert_file"] == "/var/lib/zfsbackup/client cert.pem"
        assert doc["client_key_file"] == "/var/lib/zfsbackup/client#key.pem"

    def test_client_id_file_is_gone(self):
        # Retired (plan M15): the client id lives in the config database, not the YAML, and
        # the model no longer has the field at all.
        assert "client_id_file" not in render_yaml(synthetic())
        config = BackupConfig(datasets=[DatasetConfig(name="tank/a")])
        assert not hasattr(config, "client_id_file")
        with pytest.raises(TypeError, match="client_id_file"):
            BackupConfig(datasets=[DatasetConfig(name="tank/a")], client_id_file=Path("/x/id"))
        assert "client_id" not in render_yaml(config)

    def test_tls_paths_are_double_quoted_strings(self):
        text = render_yaml(synthetic())
        assert 'client_cert_file: "/var/lib/zfsbackup/client cert.pem"' in text
        assert 'client_key_file: "/var/lib/zfsbackup/client#key.pem"' in text

    def test_server_fingerprint_is_double_quoted(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a")],
            destinations={"nas": Destination("https://nas", "ab" * 32)},
        )
        text = render_yaml(config)
        assert f'    server_fingerprint: "{"ab" * 32}"\n' in text
        assert list(yaml.safe_load(text)["destinations"]["nas"]) == ["url", "server_fingerprint"]

    def test_an_all_digit_fingerprint_is_quoted_so_it_stays_a_string(self, tmp_path):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a")],
            destinations={"nas": Destination("https://nas", DIGIT_FINGERPRINT)},
        )
        text = render_yaml(config)
        assert f'server_fingerprint: "{DIGIT_FINGERPRINT}"' in text
        assert yaml.safe_load(text)["destinations"]["nas"]["server_fingerprint"] == DIGIT_FINGERPRINT
        # An unquoted all-digit scalar would be an int: prove the quoting matters.
        assert isinstance(yaml.safe_load(f"fp: {DIGIT_FINGERPRINT}\n")["fp"], int)
        assert assert_round_trip(config, tmp_path)

    def test_server_fingerprint_is_omitted_when_not_enrolled(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a")], destinations={"nas": Destination("https://nas")},
        )
        text = render_yaml(config)
        assert "server_fingerprint" not in text
        assert yaml.safe_load(text)["destinations"]["nas"] == {"url": "https://nas"}

    def test_a_colon_upper_case_fingerprint_renders_normalised(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a")],
            destinations={"nas": Destination("https://nas", ":".join(["AB"] * 32))},
        )
        assert yaml.safe_load(render_yaml(config))["destinations"]["nas"]["server_fingerprint"] == "ab" * 32

    def test_only_enrolled_destinations_carry_a_fingerprint(self):
        doc = yaml.safe_load(render_yaml(synthetic()))["destinations"]
        assert set(doc["123"]) == {"url", "server_fingerprint"}
        assert doc["123"]["server_fingerprint"] == DIGIT_FINGERPRINT
        assert doc["zeta"]["server_fingerprint"] == "ab" * 32
        assert set(doc["alpha"]) == {"url"}

    def test_destination_fragment_carries_the_fingerprint_only_when_set(self):
        from zfsbackup.cli.render import render_destination_fragment

        assert render_destination_fragment(Destination("https://nas")) == 'url: "https://nas"\n'
        assert render_destination_fragment(Destination("https://nas", "ab" * 32)) == (
            f'url: "https://nas"\nserver_fingerprint: "{"ab" * 32}"\n'
        )

    def test_tls_paths_are_written_as_a_pair_and_only_when_set(self):
        none = yaml.safe_load(render_yaml(BackupConfig(datasets=[DatasetConfig(name="tank/a")])))
        assert "client_cert_file" not in none and "client_key_file" not in none
        both = yaml.safe_load(render_yaml(BackupConfig(
            datasets=[DatasetConfig(name="tank/a")],
            client_cert_file=Path("/c.pem"), client_key_file=Path("/k.pem"),
        )))
        assert (both["client_cert_file"], both["client_key_file"]) == ("/c.pem", "/k.pem")
        # They sit right after dry_run (prune_interval is unset, hence omitted).
        keys = list(both)
        assert keys[keys.index("dry_run"):][:3] == ["dry_run", "client_cert_file", "client_key_file"]

    def test_optional_sections_omitted_when_empty(self):
        doc = yaml.safe_load(render_yaml(BackupConfig(datasets=[DatasetConfig(name="tank/a")])))
        assert "destinations" not in doc
        assert "remote_backup" not in doc
        assert "remote" not in doc["datasets"][0]

    def test_booleans_are_always_explicit(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a"), DatasetConfig(name="tank/b", recursive=True, enabled=False)]
        )
        doc = yaml.safe_load(render_yaml(config))
        assert doc["dry_run"] is False
        assert (doc["datasets"][0]["enabled"], doc["datasets"][0]["recursive"]) == (True, False)
        assert (doc["datasets"][1]["enabled"], doc["datasets"][1]["recursive"]) == (False, True)

    def test_truthy_non_bool_flags_are_written_as_booleans(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a", enabled=0, recursive=1)], dry_run=1
        )
        doc = yaml.safe_load(render_yaml(config))
        assert doc["dry_run"] is True
        assert doc["datasets"][0]["enabled"] is False and doc["datasets"][0]["recursive"] is True

    def test_remote_backup_enabled_is_always_explicit(self):
        for enabled in (True, False):
            config = BackupConfig(
                datasets=[DatasetConfig(name="tank/a")],
                remote_backup=RemoteServerConfig("tank/r", enabled=enabled),
            )
            assert yaml.safe_load(render_yaml(config))["remote_backup"] == {
                "enabled": enabled, "target_dataset": "tank/r",
            }

    def test_destinations_are_sorted_by_name_regardless_of_insertion_order(self):
        names = ["zeta", "alpha", "Mid", "123", "beta"]
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a")],
            destinations={n: Destination(f"https://{n}") for n in names},
        )
        assert list(yaml.safe_load(render_yaml(config))["destinations"]) == sorted(names)

    def test_dataset_order_and_remote_order_are_preserved(self):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name=n,
                    remote=[RemoteDatasetConfig(d) for d in ("zeta", "alpha", "beta")],
                )
                for n in ("tank/z", "tank/a", "tank/m")
            ],
            destinations={d: Destination("https://x") for d in ("alpha", "beta", "zeta")},
        )
        doc = yaml.safe_load(render_yaml(config))
        assert [d["name"] for d in doc["datasets"]] == ["tank/z", "tank/a", "tank/m"]
        for dataset in doc["datasets"]:
            assert [r["destination"] for r in dataset["remote"]] == ["zeta", "alpha", "beta"]

    def test_retention_is_written_ordered_by_age(self):
        config = BackupConfig(datasets=[DatasetConfig(
            name="tank/a",
            retention_rules=[RetentionRule(D("1w"), D("3M")), RetentionRule(D("1d"), D("1M")), RetentionRule(D("1M"), D("1y"))],
        )])
        retention = yaml.safe_load(render_yaml(config))["datasets"][0]["retention"]
        assert list(retention.items()) == [("1d", "1M"), ("1w", "3M"), ("1M", "1y")]

    def test_destination_retention_only_when_it_overrides(self):
        config = BackupConfig(
            datasets=[DatasetConfig(
                name="tank/a",
                retention_rules=[RetentionRule(D("1d"), D("30d"))],
                remote=[
                    RemoteDatasetConfig("inherit"),
                    RemoteDatasetConfig("override", retention_rules=[RetentionRule(D("2d"), D("60d"))]),
                ],
            )],
            destinations={"inherit": Destination("https://i"), "override": Destination("https://o")},
        )
        remotes = yaml.safe_load(render_yaml(config))["datasets"][0]["remote"]
        assert remotes[0] == {"destination": "inherit"}
        assert remotes[1] == {"destination": "override", "retention": {"2d": "60d"}}

    def test_never_prune_is_spelled_forever_and_nothing_else(self):
        text = render_yaml(BackupConfig(datasets=[DatasetConfig(name="tank/a", retention_rules=[])]))
        assert re.search(r"^    retention: forever$", text, re.M)
        assert yaml.safe_load(text)["datasets"][0]["retention"] == "forever"

    def test_durations_are_double_quoted(self):
        text = render_yaml(synthetic())
        for key in ("check_interval", "prune_interval", "frequency"):
            lines = [l for l in text.splitlines() if re.match(rf"\s*(- )?{key}:", l)]
            assert lines
            for line in lines:
                assert re.fullmatch(rf'\s*(- )?{key}: "[^"]+"', line), line
        # retention keys and values: "age": "keep_for"
        retention_lines = [l for l in text.splitlines() if re.match(r'^\s+"\d+[A-Za-z][A-Za-z0-9]*":', l)]
        assert retention_lines
        for line in retention_lines:
            assert re.fullmatch(r'\s+"[^"]+": "[^"]+"', line), line

    def test_names_and_destination_keys_are_double_quoted(self):
        text = render_yaml(synthetic())
        names = [l for l in text.splitlines() if re.match(r"\s*(- )?name:", l)]
        assert len(names) == len(synthetic().datasets)
        assert all(re.fullmatch(r'\s*(- )?name: ".*"', l) for l in names), names
        dests = [l for l in text.splitlines() if re.match(r"\s*(- )?destination:", l)]
        assert dests and all(re.fullmatch(r'\s*(- )?destination: ".*"', l) for l in dests)
        keys = [l for l in text.splitlines() if re.fullmatch(r'  "[^"]*":', l)]
        assert keys == ['  "123":', '  "alpha":', '  "zeta":']

    def test_api_port_is_a_bare_integer_and_text_fields_are_quoted(self):
        text = render_yaml(synthetic())
        assert re.search(r"^api_port: 9090$", text, re.M)
        assert re.search(r'^api_host: "::1"$', text, re.M)
        assert re.search(r'^snapshot_prefix: "snap:#1"$', text, re.M)
        assert re.search(r"^dry_run: true$", text, re.M)

    def test_output_is_comment_free_and_ends_with_one_newline(self):
        text = render_yaml(synthetic())
        assert text.endswith("\n") and not text.endswith("\n\n")
        # '#' only ever appears inside a quoted scalar.
        for line in text.splitlines():
            assert not line.lstrip().startswith("#")
        assert render_yaml(synthetic()) == text

    def test_layout_follows_the_example_file_sections(self):
        text = render_yaml(synthetic())
        positions = [text.index(h) for h in ("snapshot_prefix:", "\ndestinations:", "\nremote_backup:", "\ndatasets:")]
        assert positions == sorted(positions)
        assert "\n\n  - name:" in text  # blank line between dataset entries

    def test_empty_dataset_list_is_written_explicitly(self):
        assert render_yaml(BackupConfig(datasets=[])).rstrip().endswith("datasets: []")

    def test_plain_timedelta_gets_a_canonical_literal(self, tmp_path):
        config = BackupConfig(
            datasets=[DatasetConfig(
                name="tank/a",
                frequency=timedelta(hours=1),
                retention_rules=[RetentionRule(timedelta(days=1), timedelta(days=30))],
            )],
            check_interval=timedelta(minutes=5),
            prune_interval=timedelta(hours=2),
        )
        doc = yaml.safe_load(render_yaml(config))
        assert doc["check_interval"] == "5m"
        assert doc["prune_interval"] == "2h"
        assert doc["datasets"][0]["frequency"] == "1h"
        (age, keep), = doc["datasets"][0]["retention"].items()
        assert age == "1d" and keep == Duration(timedelta(days=30)).literal
        assert_round_trip(config, tmp_path, check_literals=False)

    def test_duration_literal_helper(self):
        assert duration_literal(D("12h6d")) == "12h6d"
        assert duration_literal(timedelta(minutes=90)) == Duration(timedelta(minutes=90)).literal


# --------------------------------------------------------------------------
# refusals
# --------------------------------------------------------------------------


class TestRefusals:
    def test_sub_second_check_interval_is_refused_naming_the_key(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a")], check_interval=timedelta(milliseconds=1500)
        )
        with pytest.raises(ValueError, match="check_interval"):
            render_yaml(config)

    def test_sub_second_dataset_frequency_names_the_dataset(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a", frequency=timedelta(milliseconds=1))]
        )
        with pytest.raises(ValueError, match=r"datasets\[tank/a\]\.frequency"):
            render_yaml(config)

    def test_sub_second_retention_and_remote_values_are_refused(self):
        with pytest.raises(ValueError, match="retention"):
            render_yaml(BackupConfig(datasets=[DatasetConfig(
                name="tank/a",
                retention_rules=[RetentionRule(timedelta(days=1), timedelta(microseconds=5))],
            )]))
        with pytest.raises(ValueError, match="remote"):
            render_yaml(BackupConfig(datasets=[DatasetConfig(
                name="tank/a",
                remote=[RemoteDatasetConfig("nas", frequency=timedelta(milliseconds=5))],
            )], destinations={"nas": Destination("https://n")}))

    def test_sub_second_prune_interval_is_refused(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/a")], prune_interval=timedelta(seconds=0.5)
        )
        with pytest.raises(ValueError, match="prune_interval"):
            render_yaml(config)

    def test_same_age_with_different_keep_for_cannot_be_written(self):
        config = BackupConfig(datasets=[DatasetConfig(
            name="tank/a",
            retention_rules=[RetentionRule(D("1d"), D("30d")), RetentionRule(D("1d"), D("60d"))],
        )])
        with pytest.raises(ValueError, match="appears twice"):
            render_yaml(config)

    def test_exact_duplicate_rules_collapse_to_one_key(self, tmp_path):
        config = BackupConfig(datasets=[DatasetConfig(
            name="tank/a",
            retention_rules=[RetentionRule(D("1d"), D("30d")), RetentionRule(D("1d"), D("30d"))],
        )])
        retention = yaml.safe_load(render_yaml(config))["datasets"][0]["retention"]
        assert retention == {"1d": "30d"}
        assert len(reload(render_yaml(config), tmp_path).datasets[0].retention_rules) == 1

    def test_refusal_writes_nothing(self):
        # render_yaml is a pure function: a ValueError leaves no partial output.
        with pytest.raises(ValueError):
            render_yaml(BackupConfig(
                datasets=[DatasetConfig(name="tank/a")], check_interval=timedelta(milliseconds=1)
            ))


# --------------------------------------------------------------------------
# through the database
# --------------------------------------------------------------------------


class TestThroughTheDatabase:
    @pytest.fixture
    def run(self):
        runner = CliRunner()

        def invoke(*args):
            result = runner.invoke(cli, [str(a) for a in args], prog_name="zfsbackup-config")
            assert result.exit_code == 0, result.stderr
            return result

        return invoke

    @pytest.mark.parametrize("source", REPO_YAMLS, ids=lambda p: p.name)
    def test_import_then_export_reproduces_the_yaml_in_value_and_literal(
        self, source, tmp_path, run
    ):
        db = tmp_path / "cfg" / "c.db"
        run("import", source, "-c", db)
        exported = run("export", "-c", db).stdout
        original = BackupConfig.from_file(source)
        back = reload(exported, tmp_path)
        assert back == original
        assert literals(back) == literals(original)

    @pytest.mark.parametrize("source", REPO_YAMLS, ids=lambda p: p.name)
    def test_export_equals_render_of_the_yaml(self, source, tmp_path, run):
        """The DB path adds nothing and loses nothing: the stored config renders
        to the same bytes as the file it was imported from."""
        db = tmp_path / "cfg" / "c.db"
        run("import", source, "-c", db)
        assert run("export", "-c", db).stdout == render_yaml(BackupConfig.from_file(source))

    def test_synthetic_config_import_export_is_byte_identical(self, tmp_path, run):
        rendered = render_yaml(synthetic())
        source = tmp_path / "synthetic.yaml"
        source.write_text(rendered)
        db = tmp_path / "cfg" / "c.db"
        run("import", source, "-c", db)
        assert run("export", "-c", db).stdout == rendered

    def test_export_import_export_is_a_fixed_point(self, tmp_path, run):
        first_source = tmp_path / "s.yaml"
        first_source.write_text(render_yaml(synthetic()))
        a, b = tmp_path / "a" / "c.db", tmp_path / "b" / "c.db"
        run("import", first_source, "-c", a)
        first = run("export", "-c", a).stdout
        second_source = tmp_path / "s2.yaml"
        second_source.write_text(first)
        run("import", second_source, "-c", b)
        assert run("export", "-c", b).stdout == first

    def test_the_database_preserves_literals_and_overrides(self, tmp_path, run):
        source = tmp_path / "s.yaml"
        source.write_text(render_yaml(synthetic()))
        db = tmp_path / "cfg" / "c.db"
        run("import", source, "-c", db)
        back = reload(run("export", "-c", db).stdout, tmp_path)
        assert back == synthetic()
        assert literals(back) == literals(synthetic())

    @pytest.mark.parametrize("seed", range(15))
    def test_random_configs_survive_the_database(self, seed, tmp_path, run):
        config = random_config(random.Random(1000 + seed))
        rendered = render_yaml(config)
        source = tmp_path / "r.yaml"
        source.write_text(rendered)
        db = tmp_path / "cfg" / "c.db"
        run("import", source, "-c", db)
        assert run("export", "-c", db).stdout == rendered

    def test_the_pins_and_tls_paths_survive_the_database(self, tmp_path, run):
        source = tmp_path / "s.yaml"
        source.write_text(render_yaml(synthetic()))
        db = tmp_path / "cfg" / "c.db"
        run("import", source, "-c", db)
        back = reload(run("export", "-c", db).stdout, tmp_path)
        assert back.destinations["123"].server_fingerprint == DIGIT_FINGERPRINT
        assert back.destinations["zeta"].server_fingerprint == "ab" * 32
        assert back.destinations["alpha"].server_fingerprint is None
        assert back.client_cert_file == Path("/var/lib/zfsbackup/client cert.pem")
        assert back.client_key_file == Path("/var/lib/zfsbackup/client#key.pem")

    def test_a_colon_upper_case_fingerprint_is_normalised_on_import(self, tmp_path, run):
        source = tmp_path / "s.yaml"
        source.write_text(
            'datasets:\n  - name: "tank/a"\n'
            'destinations:\n  nas:\n    url: "https://nas"\n'
            f'    server_fingerprint: "{":".join(["AB"] * 32)}"\n'
            '  bare:\n    url: "https://bare"\n'
            f'    server_fingerprint: "{"AB" * 32}"\n'
        )
        db = tmp_path / "cfg" / "c.db"
        run("import", source, "-c", db)
        doc = yaml.safe_load(run("export", "-c", db).stdout)["destinations"]
        assert doc["nas"]["server_fingerprint"] == "ab" * 32
        assert doc["bare"]["server_fingerprint"] == "ab" * 32

    def test_an_all_digit_fingerprint_imports_exports_and_reimports(self, tmp_path, run):
        source = tmp_path / "s.yaml"
        source.write_text(
            'datasets:\n  - name: "tank/a"\n'
            f'destinations:\n  nas:\n    url: "https://nas"\n    server_fingerprint: "{DIGIT_FINGERPRINT}"\n'
        )
        db = tmp_path / "cfg" / "c.db"
        run("import", source, "-c", db)
        exported = run("export", "-c", db).stdout
        assert f'server_fingerprint: "{DIGIT_FINGERPRINT}"' in exported
        second = tmp_path / "s2.yaml"
        second.write_text(exported)
        run("import", second, "-c", tmp_path / "cfg2" / "c.db")
        assert run("export", "-c", tmp_path / "cfg2" / "c.db").stdout == exported

    def test_the_pre_import_backup_uses_the_same_renderer(self, tmp_path, run):
        db = tmp_path / "cfg" / "c.db"
        source = tmp_path / "s.yaml"
        source.write_text(render_yaml(synthetic()))
        run("import", source, "-c", db)
        run("import", REPO_YAMLS[0], "-c", db)
        (backup,) = db.parent.glob("c.db.pre-import-*.yaml")
        assert backup.read_text() == render_yaml(synthetic())

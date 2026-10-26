"""`zfsbackup-config check` (plan item C13, B-6b): is this host's backup healthy?

The ZFS side is a read-only fake `zfs` (`tests/_fake_zfs_check.py`) that answers
`zfs list -t snapshot` and `zfs get -H -o value` from a JSON spec and exits 2 on
anything else; the database, the pid file and the TLS directory are real.

- ``TestFormats``       text, `--json`, `--prometheus`, `--nagios` on a healthy host
                        and on a degraded one; the factors.
- ``TestExitMatrix``    0 / 1 (WARN) / 1 (CRIT) / 78 / 2 / 75 / 130 / 143.
- ``TestNagiosMatrix``  0 / 1 / 2 / 3, and every way of failing to find out is 3.
- ``TestEscaping``      quotes, backslashes and newlines in labels.
- ``TestOutputFile``    `--output FILE`: atomic, 0644, nothing on stdout.
- ``TestReadOnly``      no writer lock, no signal, only `zfs list` / `zfs get`.
- ``TestDaemonEntries`` the daemon verdict table (fake `DaemonInfo`s) and end to end.
- ``TestCertificate``   the client-certificate table (`now` passed in).
- ``TestLazyImports``   `import zfsbackup.cli.main` loads neither `health_collect`
                        nor `libzfseasy` (a subprocess).
- ``TestQuiet``         config warnings are not printed.
- ``TestSignals``       real SIGINT / SIGTERM during the collection.
- ``TestWorkerOutcomes`` (S-7b) the daemon's worker status files: grading, refused
                        files, formats, `--output`, staleness (``TestWorkerStaleness``).

Signal tests run the CLI in a subprocess in its own session and reset SIGINT to
its Python handler first, so a pytest started from a shell that ignores SIGINT
(a backgrounded job) cannot turn 130 into something else.

Mutation counts (scratch copy, one revert at a time; failing tests in this
file): no daemon graded OK 4; Nagios failures not mapped to UNKNOWN 11; the
certificate warning window 30 -> 3 days 5; `check` not quiet 1; `--output` mode
0600 10; the Nagios parse-error hook removed 4; `--nagios --output` allowed 2;
factors validated after the ZFS work 3; Prometheus labels not escaped 1; a
pid-file read error aborting the check 1; an expired certificate graded WARN 3;
a daemon that is behind graded OK 8; the `--output` exit code back to the verdict (M4) 4.
"""

import contextlib
import json
import os
import re
import shlex
import signal
import stat
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from _worker_status_support import LIVE_PID, iso, make_dir, rec, status_dir, write_worker
from test_zfsbackup_health import Exposition
from _cli_support import (  # noqa: F401  (fixtures are found by name)
    RICH_YAML,
    WARNY_YAML,
    db,
    db_state,
    isolated,
    live_daemon,
    make_db,
    run,
    store_key,
)
from zfsbackup import daemonlock, health, identity
from zfsbackup.cli import check as check_view
from zfsbackup.cli import status as status_view
from zfsbackup.cli import tlsfiles
from zfsbackup.cli import write as cli_write
from zfsbackup.cli.errors import CliError
from zfsbackup.config.store import (
    ConfigBusy,
    SchemaError,
    dispose_all,
    read_config,
    resolve_config_path,
    writer_lock,
)

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parent.parent
FAKE_ZFS = Path(__file__).resolve().parent / "_fake_zfs_check.py"

needs_non_root = pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0, reason="root ignores file permissions"
)
needs_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork on this platform")

CHECK_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/a"
    frequency: "1h"
    retention:
      "1d": "30d"
    remote:
      - destination: "offsite"
destinations:
  offsite:
    url: "https://offsite:8080"
"""

LOCAL_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/a"
    frequency: "1h"
    retention:
      "1d": "30d"
"""

ANCHOR_PROP = "org.zfsbackup:anchor.offsite"


# --------------------------------------------------------------------------
# the world: a database, a fake zfs, a daemon, a certificate
# --------------------------------------------------------------------------


class FakeZfsCheck:
    """The fake `zfs` of one test. `ZFS_CMD` points at a wrapper script."""

    def __init__(self, directory: Path, monkeypatch) -> None:
        self.spec_path = directory / "check-spec.json"
        self.log_path = directory / "check-log.jsonl"
        self.spec = {"snapshots": {}, "props": {}, "fail": []}
        self.log_path.write_text("")
        wrapper = directory / "zfs-check"
        wrapper.write_text(
            f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(FAKE_ZFS))} \"$@\"\n"
        )
        wrapper.chmod(0o755)
        self.save()
        for key, value in {
            "ZFS_CMD": str(wrapper), "ZPOOL_CMD": str(wrapper),
            "FAKECHECK_SPEC": str(self.spec_path), "FAKECHECK_LOG": str(self.log_path),
        }.items():
            monkeypatch.setenv(key, value)

    def save(self) -> None:
        self.spec_path.write_text(json.dumps(self.spec))

    @staticmethod
    def name(age_seconds: float, prefix: str = "autosnap") -> str:
        moment = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
        return f"{prefix}_{moment:%Y%m%d%H%M%S}"

    def snapshots(self, dataset: str, *ages: float) -> list:
        """Managed snapshots `ages` seconds old (newest first); -> their names."""
        names = [self.name(a) for a in ages]
        self.spec["snapshots"][dataset] = [[n, 100 + len(ages) - i] for i, n in enumerate(names)]
        self.save()
        return names

    def prop(self, dataset: str, key: str, value: str) -> None:
        self.spec["props"].setdefault(dataset, {})[key] = value
        self.save()

    def dataset(self, name: str, kind: str = "filesystem") -> None:
        self.spec.setdefault("datasets", {})[name] = kind
        self.save()

    def fail(self, dataset: str) -> None:
        self.spec["fail"].append(dataset)
        self.save()

    @property
    def calls(self) -> list:
        return [json.loads(line) for line in self.log_path.read_text().splitlines()]


@pytest.fixture
def zfs(tmp_path, monkeypatch) -> FakeZfsCheck:
    return FakeZfsCheck(tmp_path, monkeypatch)


def tls_of(database: Path) -> Path:
    return Path(str(database) + ".tls")


class World:
    def __init__(self, run, db, zfs, live_daemon):
        self.run, self.db, self.zfs, self._daemon = run, db, zfs, live_daemon
        self.daemon = None

    def build(self, yaml_text=CHECK_YAML, *, snapshot_age=600, anchored=True, cert=True,
              daemon=True):
        make_db(self.run, self.db, yaml_text)
        names = self.zfs.snapshots("tank/a", snapshot_age, snapshot_age + 3600)
        if anchored and "offsite" in yaml_text:
            self.zfs.prop("tank/a", ANCHOR_PROP, names[0])
        if cert and "offsite" in yaml_text:
            identity.ensure_identity(tls_of(self.db), "client")
        if daemon:
            self.daemon = self._daemon(self.db, applied=store_key(self.db))
        return self

    def check(self, *args, **kw):
        return self.run("-c", self.db, "check", *args, **kw)

    def entries(self, *args):
        result = self.check("--json", *args)
        return {(e["source"], e["subject"], e.get("peer")): e for e in json.loads(result.stdout)["entries"]}


@pytest.fixture
def world(run, db, zfs, live_daemon) -> World:
    return World(run, db, zfs, live_daemon)


@pytest.fixture
def healthy(world) -> World:
    return world.build()


def state_of(world, source, subject, peer=None):
    return world.entries()[(source, subject, peer)]["state"]


# --------------------------------------------------------------------------
# 1. formats
# --------------------------------------------------------------------------

PROM_SAMPLE = re.compile(
    r'^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-z_]+="([^"\\\n]|\\.)*",?)*\})? -?[0-9.e+-]+$'
)


class TestFormats:
    def test_text_on_a_healthy_host(self, healthy):
        result = healthy.check()
        assert result.exit_code == 0, result.stderr
        lines = result.stdout.splitlines()
        assert lines[0].startswith("OK   snapshot: tank/a: newest snapshot is ")
        assert any(line.startswith("OK   anchor: tank/a (offsite): ") for line in lines)
        assert "OK   daemon: process: running (pid " in result.stdout
        assert "OK   daemon: config_applied: applied the stored generation" in result.stdout
        assert any(line.startswith("OK   tls: client_certificate: ") for line in lines)
        assert lines[-1] == "overall: OK (5 checks: 5 ok, 0 warn, 0 crit)"
        assert result.stderr == ""

    def test_json_on_a_healthy_host(self, healthy):
        result = healthy.check("--json")
        assert result.exit_code == 0, result.stderr
        data = json.loads(result.stdout)           # stdout is the JSON and nothing else
        assert data["schema"] == 1
        assert data["state"] == "OK"
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", data["generated_at"])
        assert data["version"]
        assert [(e["source"], e["subject"]) for e in data["entries"]] == [
            ("snapshot", "tank/a"), ("anchor", "tank/a"), ("daemon", "process"),
            ("daemon", "config_applied"), ("tls", "client_certificate"),
        ]
        snapshot = data["entries"][0]
        assert snapshot["age_seconds"] == pytest.approx(600, abs=30)
        assert (snapshot["expected_seconds"], snapshot["warn_seconds"], snapshot["crit_seconds"]) == (
            3600, 7200, 14400)
        assert data["entries"][1]["peer"] == "offsite"
        assert "age_seconds" not in data["entries"][2]

    def test_prometheus_on_a_healthy_host(self, healthy):
        result = healthy.check("--prometheus")
        assert result.exit_code == 0, result.stderr
        lines = result.stdout.splitlines()
        samples = [line for line in lines if not line.startswith("#")]
        assert samples and all(PROM_SAMPLE.match(s) for s in samples), [
            s for s in samples if not PROM_SAMPLE.match(s)]
        assert 'zfsbackup_check_overall_state 0' in samples
        assert 'zfsbackup_check_success 1' in samples
        assert 'zfsbackup_snapshot_check_state{dataset="tank/a"} 0' in samples
        assert 'zfsbackup_anchor_check_state{dataset="tank/a",destination="offsite"} 0' in samples
        assert 'zfsbackup_check_state{source="daemon",subject="process"} 0' in samples
        assert any(s.startswith('zfsbackup_snapshot_age_seconds{dataset="tank/a"} ') for s in samples)
        assert any(s.startswith("zfsbackup_check_timestamp_seconds ") for s in samples)
        for sample in samples:
            name = re.match(r"[a-zA-Z_:][a-zA-Z0-9_:]*", sample).group(0)
            assert f"# TYPE {name} gauge" in lines, name

    def test_nagios_on_a_healthy_host(self, healthy):
        result = healthy.check("--nagios")
        assert result.exit_code == 0, result.stderr
        assert result.stdout.count("\n") == 1                      # one line
        match = re.fullmatch(
            r"ZFSBACKUP OK - 5 checks OK \| "
            r"'snapshot:tank/a'=(\d+)s;7200;14400;0 'anchor:tank/a@offsite'=(\d+)s;7200;14400;0\n",
            result.stdout,
        )
        assert match, result.stdout
        assert abs(int(match.group(1)) - 600) < 30

    def test_text_names_what_is_wrong(self, healthy):
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        result = healthy.check()
        assert result.exit_code == 1
        assert re.search(r"^CRIT snapshot: tank/a: newest snapshot is 5h", result.stdout, re.M)
        assert "(crit at 4h)" in result.stdout
        assert result.stdout.splitlines()[-1].startswith("overall: CRIT (")

    def test_nagios_names_the_worst_first_and_counts_the_rest(self, healthy):
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        result = healthy.check("--nagios")
        assert result.exit_code == 2
        assert result.stdout.startswith("ZFSBACKUP CRIT - CRIT snapshot: tank/a: newest snapshot is 5h")
        assert " | " in result.stdout

    def test_nagios_summary_is_capped_at_three(self, world):
        world.build(daemon=False, cert=False, anchored=False)    # snapshot ok; anchor, daemon x1, tls warn
        world.zfs.snapshots("tank/a", 5 * 3600)
        result = world.check("--nagios")
        assert result.exit_code == 2
        assert "and 1 more" in result.stdout

    def test_the_factors_move_the_thresholds(self, healthy):
        # 600 s old against a 3600 s frequency: WARN from 0.1 x (360 s), CRIT from 0.15 x (540 s)
        assert healthy.check("--warn-factor", "0.1", "--crit-factor", "5").exit_code == 1
        entries = healthy.entries("--warn-factor", "0.1", "--crit-factor", "5")
        assert entries[("snapshot", "tank/a", None)]["state"] == "WARN"
        assert entries[("snapshot", "tank/a", None)]["warn_seconds"] == pytest.approx(360)
        entries = healthy.entries("--warn-factor", "0.1", "--crit-factor", "0.15")
        assert entries[("snapshot", "tank/a", None)]["state"] == "CRIT"

    def test_without_destinations_there_is_no_certificate_entry(self, world):
        world.build(LOCAL_YAML)
        entries = world.entries()
        assert not [k for k in entries if k[0] == "tls"]
        assert not [k for k in entries if k[0] == "anchor"]
        assert world.check().exit_code == 0

    def test_a_disabled_dataset_is_not_checked(self, world):
        world.build(RICH_YAML)               # tank/vm.01 is `enabled: false`
        entries = world.entries()
        assert ("snapshot", "tank/vm.01", None) not in entries


SERVER_YAML = LOCAL_YAML + """\
remote_backup:
  enabled: true
  target_dataset: "bk"
"""


class TestServerReceivedDatasets:
    """B-1d on the server: the received datasets' `client_id` and `config` come from ONE batched read."""

    def test_two_received_datasets_cost_one_list_and_one_batch_get(self, world):
        from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
        from zfsbackup.config import DatasetConfig, Duration
        world.build(SERVER_YAML, cert=False)
        config = DatasetConfig(name="tank/src", frequency=Duration("1h")).to_property()
        for name in ("bk", "bk/c1", "bk/c1/a", "bk/c1/b"):
            world.zfs.dataset(name)
        for name in ("bk/c1/a", "bk/c1/b"):
            world.zfs.prop(name, PROP_CLIENT_ID, "c1")
            world.zfs.prop(name, PROP_CONFIG, config)
            world.zfs.snapshots(name, 600)
        before = len(world.zfs.calls)
        entries = world.entries()
        assert entries[("received", "bk/c1/a", "c1")]["state"] == "OK"
        assert entries[("received", "bk/c1/b", "c1")]["state"] == "OK"
        calls = world.zfs.calls[before:]
        batch = [c for c in calls if c[0] == "get" and "name,property,value,source" in c]
        assert len(batch) == 1
        value_reads = [c for c in calls if c[0] == "get" and c[c.index("-o") + 1] == "value"
                       and c[-1].startswith("bk/")]
        assert value_reads == []                    # no per-dataset read: the batch served them
        assert {c[0] for c in calls} <= {"list", "get"}


# --------------------------------------------------------------------------
# 2. exit codes
# --------------------------------------------------------------------------


class TestExitMatrix:
    def test_healthy_is_0(self, healthy):
        assert healthy.check().exit_code == 0

    def test_a_warning_alone_is_1(self, healthy):
        # the daemon runs an older generation: WARN, everything else OK
        healthy.daemon.set(applied=(store_key(healthy.db)[0], store_key(healthy.db)[1] - 1))
        result = healthy.check()
        assert result.exit_code == 1
        assert not re.search(r"^CRIT ", result.stdout, re.M)
        assert re.search(r"^WARN daemon: config_applied", result.stdout, re.M)

    def test_a_critical_is_1(self, healthy):
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        assert healthy.check().exit_code == 1

    def test_no_daemon_is_1(self, world):
        world.build(daemon=False)
        result = world.check()
        assert result.exit_code == 1
        assert re.search(r"^CRIT daemon: process: no daemon is running", result.stdout, re.M)

    def test_a_missing_database_is_78(self, run, tmp_path, zfs):
        result = run("-c", tmp_path / "nope" / "config.db", "check")
        assert result.exit_code == 78, result.stderr
        assert "Traceback" not in result.stderr
        assert result.stdout == ""

    def test_a_non_sqlite_database_is_78(self, run, tmp_path, zfs):
        bad = tmp_path / "cfg" / "config.db"
        bad.parent.mkdir()
        bad.write_bytes(b"this is not a sqlite database at all " * 40)
        result = run("-c", bad, "check")
        assert result.exit_code == 78, result.stderr
        assert result.stdout == ""

    def test_a_schema_error_found_while_reading_the_daemon_state_is_78(self, healthy, monkeypatch):
        real = status_view.collect

        def collect(resolved, **kw):
            status = real(resolved, **kw)
            return status_view.Status(
                status.database, status.stored, status.daemon, status.verdict, status.message,
                SchemaError("the schema is wrong"),
            )

        monkeypatch.setattr(status_view, "collect", collect)
        result = healthy.check()
        assert result.exit_code == 78, result.stderr
        assert "schema is wrong" in result.stderr

    @pytest.mark.parametrize("args", [
        ["--json", "--prometheus"],
        ["--warn-factor", "5", "--crit-factor", "1"],
        ["--warn-factor", "0"],
        ["--warn-factor", "abc"],
        ["--bogus"],
        ["extra-argument"],
    ])
    def test_usage_errors_are_2(self, healthy, args):
        result = healthy.check(*args)
        assert result.exit_code == 2, result.stderr
        assert result.stdout == ""
        assert "Traceback" not in result.stderr

    def test_bad_factors_fail_before_any_zfs_call(self, healthy):
        before = len(healthy.zfs.calls)
        assert healthy.check("--warn-factor", "5", "--crit-factor", "1").exit_code == 2
        assert len(healthy.zfs.calls) == before

    def test_a_busy_database_is_75(self, healthy, monkeypatch):
        def busy(resolved):
            raise ConfigBusy("busy")

        monkeypatch.setattr(check_view, "read_snapshot", busy)
        result = healthy.check()
        assert result.exit_code == 75, result.stderr

    def test_a_keyboard_interrupt_is_130(self, healthy, monkeypatch):
        def interrupt(config):
            raise KeyboardInterrupt

        monkeypatch.setattr("zfsbackup.health_collect.collect", interrupt)
        assert healthy.check().exit_code == 130

    def test_an_unreadable_dataset_is_a_crit_entry_not_an_abort(self, healthy):
        healthy.zfs.fail("tank/a")
        result = healthy.check()
        assert result.exit_code == 1, result.stderr
        assert re.search(r"^CRIT snapshot: tank/a: cannot read: ", result.stdout, re.M)
        assert "daemon: process" in result.stdout        # the other entries are still there

    def test_a_collector_exception_is_not_swallowed_in_normal_mode(self, healthy, monkeypatch):
        def boom(config):
            raise RuntimeError("collector bug")

        monkeypatch.setattr("zfsbackup.health_collect.collect", boom)
        result = healthy.check()
        assert result.exit_code == 1
        assert result.stdout == ""


# --------------------------------------------------------------------------
# 3. Nagios
# --------------------------------------------------------------------------


def one_unknown_line(result):
    assert result.exit_code == 3, (result.stdout, result.stderr)
    lines = result.stdout.splitlines()
    assert len(lines) == 1 and lines[0].startswith("ZFSBACKUP UNKNOWN - "), result.stdout
    assert "Traceback" not in result.stdout + result.stderr
    return lines[0]


class TestNagiosMatrix:
    def test_ok_is_0(self, healthy):
        assert healthy.check("--nagios").exit_code == 0

    def test_warn_is_1(self, healthy):
        healthy.daemon.set(applied=(store_key(healthy.db)[0], store_key(healthy.db)[1] - 1))
        result = healthy.check("--nagios")
        assert result.exit_code == 1
        assert result.stdout.startswith("ZFSBACKUP WARN - WARN daemon: config_applied")

    def test_crit_is_2(self, healthy):
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        assert healthy.check("--nagios").exit_code == 2

    def test_a_missing_database_is_3(self, run, tmp_path, zfs):
        line = one_unknown_line(run("-c", tmp_path / "nope" / "config.db", "check", "--nagios"))
        assert "nope" in line

    def test_a_non_sqlite_database_is_3(self, run, tmp_path, zfs):
        bad = tmp_path / "cfg" / "config.db"
        bad.parent.mkdir()
        bad.write_bytes(b"this is not a sqlite database at all " * 40)
        one_unknown_line(run("-c", bad, "check", "--nagios"))

    def test_a_collector_exception_is_3(self, healthy, monkeypatch):
        def boom(config):
            raise RuntimeError("collector bug")

        monkeypatch.setattr("zfsbackup.health_collect.collect", boom)
        line = one_unknown_line(healthy.check("--nagios"))
        assert "collector bug" in line

    def test_a_busy_database_is_3(self, healthy, monkeypatch):
        monkeypatch.setattr(
            check_view, "read_snapshot", lambda resolved: (_ for _ in ()).throw(ConfigBusy("busy")))
        one_unknown_line(healthy.check("--nagios"))

    @pytest.mark.parametrize("args", [
        ["--json"], ["--prometheus"], ["--output", "x.out"], ["-o", "x.out"],
    ])
    def test_nagios_cannot_be_combined(self, healthy, args, tmp_path):
        line = one_unknown_line(healthy.check("--nagios", *args))
        assert "--nagios" in line
        assert not (tmp_path / "x.out").exists()

    def test_bad_factors_are_3(self, healthy):
        one_unknown_line(healthy.check("--nagios", "--warn-factor", "5", "--crit-factor", "1"))

    @pytest.mark.parametrize("args", [
        ["--bogus"], ["--warn-factor", "abc"], ["--crit-factor"], ["stray"],
    ])
    def test_click_parse_errors_are_3_in_plugin_form(self, healthy, args):
        line = one_unknown_line(healthy.check("--nagios", *args))
        assert "usage" in line.lower()

    def test_a_parse_error_without_nagios_stays_a_usage_error(self, healthy):
        result = healthy.check("--bogus")
        assert result.exit_code == 2
        assert "ZFSBACKUP" not in result.stdout + result.stderr

    def test_a_bad_c_before_the_subcommand(self, run, tmp_path, zfs):
        # The database named with the group-level -c is resolved by the
        # subcommand, so a missing one is an UNKNOWN plugin line.
        line = one_unknown_line(run("-c", tmp_path / "gone.db", "check", "--nagios"))
        assert "gone.db" in line

    def test_a_bad_group_option_before_the_subcommand_is_clicks_usage_error(self, healthy):
        # A parse error of the GROUP happens before `check` is chosen: plain
        # Click usage error, exit 2 (the subcommand's plugin form cannot apply).
        result = healthy.run("-c", healthy.db, "--reload-wait", "abc", "check", "--nagios")
        assert result.exit_code == 2
        assert "ZFSBACKUP" not in result.stdout

    def test_a_group_level_c_equals_a_subcommand_level_c(self, healthy):
        a = healthy.run("-c", healthy.db, "check", "--nagios")
        b = healthy.run("check", "--nagios", "-c", healthy.db)
        assert a.exit_code == b.exit_code == 0

    def test_unknown_is_never_crit(self, healthy, monkeypatch):
        monkeypatch.setattr(
            "zfsbackup.health_collect.collect",
            lambda config: (_ for _ in ()).throw(OSError("pool unreadable")),
        )
        assert healthy.check("--nagios").exit_code == 3


# --------------------------------------------------------------------------
# 4. escaping
# --------------------------------------------------------------------------

NASTY = 'a"b\\c\nd'


class TestEscaping:
    def entries(self):
        return [
            health.CheckEntry("tls", NASTY, health.STATE_WARN, f"reason {NASTY}", peer=NASTY),
            health.LagEntry(
                "anchor", NASTY, health.STATE_CRIT, "late", peer=NASTY,
                age_seconds=900.0, expected_seconds=60.0, warn_seconds=120.0, crit_seconds=240.0),
        ]

    def test_prometheus_escapes_quote_backslash_and_newline(self):
        text = health.to_prometheus(self.entries(), 1_700_000_000, "1.0")
        escaped = 'a\\"b\\\\c\\nd'
        assert f'zfsbackup_check_state{{source="tls",subject="{escaped}",peer="{escaped}"}} 1' in text
        assert f'dataset="{escaped}",destination="{escaped}"' in text
        for line in text.splitlines():
            if not line.startswith("#"):
                assert PROM_SAMPLE.match(line), line      # no raw newline broke a sample

    def test_json_round_trips(self):
        data = json.loads(health.to_json(self.entries(), 1_700_000_000, "1.0"))
        assert data["entries"][0]["subject"] == NASTY
        assert data["entries"][0]["peer"] == NASTY
        assert data["entries"][1]["subject"] == NASTY
        assert "\n" not in health.to_json(self.entries(), 1_700_000_000, "1.0", indent=None)

    def test_text_collapses_whitespace(self):
        text = check_view.render_text(health, self.entries())
        lines = text.splitlines()
        assert len(lines) == 3                                 # two entries and the overall line
        assert lines[0] == 'WARN tls: a"b\\c d (a"b\\c d): reason a"b\\c d'

    def test_nagios_collapses_whitespace_and_stays_one_line(self):
        text = check_view.render_nagios(health, self.entries())
        assert text.count("\n") == 1
        assert 'a"b\\c d' in text

    def test_nagios_perfdata_label_quotes_and_equals(self):
        entry = health.LagEntry(
            "snapshot", "it's a=b", health.STATE_OK, "fine",
            age_seconds=60.0, expected_seconds=60.0, warn_seconds=120.0, crit_seconds=240.0)
        text = check_view.render_nagios(health, [entry])
        assert "'snapshot:it''s a:b'=60s;120;240;0" in text
        assert text.endswith("\n") and text.count("\n") == 1

    def test_perfdata_label_with_a_peer_and_whitespace(self):
        entry = health.LagEntry(
            "anchor", "tank/a\tb", health.STATE_OK, "fine", peer="of f",
            age_seconds=1.0, expected_seconds=1.0, warn_seconds=2.0, crit_seconds=4.0)
        assert "'anchor:tank/a b@of f'=1s;2;4;0" in check_view.render_nagios(health, [entry])

    def test_an_entry_without_a_lag_has_no_perfdata(self):
        text = check_view.render_nagios(
            health, [health.CheckEntry("daemon", "process", health.STATE_OK, "fine")])
        assert "|" not in text and text == "ZFSBACKUP OK - 1 checks OK\n"


# --------------------------------------------------------------------------
# 5. --output
# --------------------------------------------------------------------------


def mode_of(path) -> int:
    return os.stat(path).st_mode & 0o7777


class TestOutputFile:
    @pytest.mark.parametrize("flag, check", [
        (None, lambda text: text.startswith("OK ") and "overall: OK" in text),
        ("--json", lambda text: json.loads(text)["state"] == "OK"),
        ("--prometheus", lambda text: "zfsbackup_check_success 1" in text),
    ])
    @pytest.mark.parametrize("option", ["--output", "-o"])
    def test_written_atomically_with_nothing_on_stdout(self, healthy, tmp_path, flag, check, option):
        target = tmp_path / "out" / "check.prom"
        target.parent.mkdir()
        result = healthy.check(*([flag] if flag else []), option, target)
        assert result.exit_code == 0, result.stderr
        assert result.stdout == ""
        assert check(target.read_text())
        assert mode_of(target) == 0o644
        assert [p.name for p in target.parent.iterdir()] == ["check.prom"]   # no temp file left

    @pytest.mark.parametrize("umask", [0o000, 0o077, 0o277])
    def test_the_mode_is_exact_whatever_the_umask(self, healthy, tmp_path, umask):
        target = tmp_path / "check.prom"
        old = os.umask(umask)
        try:
            assert healthy.check("--prometheus", "-o", target).exit_code == 0
        finally:
            os.umask(old)
        assert mode_of(target) == 0o644

    def test_it_replaces_the_previous_file(self, healthy, tmp_path):
        target = tmp_path / "check.prom"
        target.write_text("old contents\n")
        target.chmod(0o600)
        assert healthy.check("--prometheus", "-o", target).exit_code == 0
        assert "old contents" not in target.read_text()
        assert mode_of(target) == 0o644
        assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []

    @pytest.mark.parametrize("flag", [None, "--json", "--prometheus"])
    def test_an_unhealthy_check_writes_the_file_and_exits_0(self, healthy, tmp_path, flag):
        # M4: with --output the exit code says "the file was written", not
        # "the backups are healthy" (the verdict is IN the file).
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        target = tmp_path / "check.out"
        result = healthy.check(*([flag] if flag else []), "-o", target)
        assert result.exit_code == 0, result.stderr
        assert result.stdout == ""
        text = target.read_text()
        assert ("CRIT" in text) or ("zfsbackup_check_success 0" in text)

    def test_the_prometheus_file_carries_the_failed_verdict(self, healthy, tmp_path):
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        target = tmp_path / "check.prom"
        assert healthy.check("--prometheus", "-o", target).exit_code == 0
        assert "zfsbackup_check_success 0" in target.read_text()
        assert "zfsbackup_check_overall_state 2" in target.read_text()

    def test_without_output_the_verdict_is_still_the_exit_code(self, healthy):
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        assert healthy.check("--prometheus").exit_code == 1
        assert healthy.check("--json", "--output", "-").exit_code == 1       # `-` is stdout

    def test_a_missing_directory_is_1_and_no_file(self, healthy, tmp_path):
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        target = tmp_path / "nodir" / "check.prom"
        result = healthy.check("--prometheus", "-o", target)
        assert result.exit_code == 1, result.stderr
        assert not target.exists() and not target.parent.exists()

    def test_a_database_failure_is_78_even_with_output(self, run, tmp_path, zfs):
        target = tmp_path / "check.prom"
        result = run("-c", tmp_path / "gone" / "config.db", "check", "--prometheus", "-o", target)
        assert result.exit_code == 78
        assert not target.exists()

    def test_a_busy_database_is_75_even_with_output(self, healthy, tmp_path, monkeypatch):
        monkeypatch.setattr(
            check_view, "read_snapshot", lambda resolved: (_ for _ in ()).throw(ConfigBusy("busy")))
        target = tmp_path / "check.prom"
        assert healthy.check("--prometheus", "-o", target).exit_code == 75
        assert not target.exists()

    def test_nagios_is_unaffected_by_the_output_rule(self, healthy):
        healthy.zfs.snapshots("tank/a", 5 * 3600)
        assert healthy.check("--nagios").exit_code == 2

    @pytest.mark.parametrize("how", ["missing database", "collector exception", "busy"])
    def test_a_failed_check_leaves_the_old_file(self, run, healthy, tmp_path, monkeypatch, how):
        target = tmp_path / "check.prom"
        target.write_text("previous run\n")
        os.utime(target, (1, 1))
        database = healthy.db
        if how == "missing database":
            database = tmp_path / "gone" / "config.db"
        elif how == "collector exception":
            monkeypatch.setattr(
                "zfsbackup.health_collect.collect",
                lambda config: (_ for _ in ()).throw(RuntimeError("boom")))
        else:
            monkeypatch.setattr(
                check_view, "read_snapshot", lambda resolved: (_ for _ in ()).throw(ConfigBusy("busy")))
        result = run("-c", database, "check", "--prometheus", "-o", target)
        assert result.exit_code == {"missing database": 78, "collector exception": 1, "busy": 75}[how]
        assert target.read_text() == "previous run\n"
        assert int(target.stat().st_mtime) == 1
        assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".check")] == []

    def test_an_unwritable_directory_is_1(self, healthy, tmp_path):
        result = healthy.check("--json", "-o", tmp_path / "no" / "such" / "check.json")
        assert result.exit_code == 1, result.stderr
        assert "cannot write" in result.stderr
        assert result.stdout == ""

    @needs_non_root
    def test_a_read_only_directory_is_1_and_leaves_nothing(self, healthy, tmp_path):
        directory = tmp_path / "ro"
        directory.mkdir()
        directory.chmod(0o500)
        try:
            result = healthy.check("--json", "-o", directory / "check.json")
        finally:
            directory.chmod(0o700)
        assert result.exit_code == 1
        assert list(directory.iterdir()) == []

    def test_output_dash_is_stdout(self, healthy, tmp_path):
        result = healthy.check("--json", "--output", "-")
        assert result.exit_code == 0
        assert json.loads(result.stdout)["state"] == "OK"
        assert not (tmp_path / "-").exists()

    def test_a_relative_output_path_goes_to_the_cwd(self, healthy, tmp_path):
        assert healthy.check("--json", "-o", "rel.json").exit_code == 0
        assert json.loads((tmp_path / "rel.json").read_text())["state"] == "OK"


# --------------------------------------------------------------------------
# 6. read-only
# --------------------------------------------------------------------------


class TestReadOnly:
    def test_no_signal_no_lock_no_write(self, healthy, monkeypatch):
        def boom(*a, **k):
            raise AssertionError("check must not signal the daemon")

        monkeypatch.setattr(cli_write, "begin_reload", boom)
        monkeypatch.setattr(daemonlock, "request_reload", boom)
        lock = Path(str(healthy.db) + ".writer.lock")
        lock.unlink(missing_ok=True)
        workers = Path(str(healthy.db) + ".workers.lock")
        workers.unlink(missing_ok=True)
        before_files = sorted(p.name for p in healthy.db.parent.iterdir())
        key = store_key(healthy.db)
        pid_file = daemonlock.lock_path(healthy.db).read_bytes()

        for flags in ((), ("--json",), ("--prometheus",), ("--nagios",)):
            healthy.check(*flags)

        assert not lock.exists() and not workers.exists()
        assert store_key(healthy.db) == key                 # the generation is unchanged
        assert daemonlock.lock_path(healthy.db).read_bytes() == pid_file
        assert sorted(p.name for p in healthy.db.parent.iterdir()) == before_files

    def test_it_does_not_wait_for_the_writer_lock(self, healthy):
        import threading

        resolved = resolve_config_path(str(healthy.db))
        ready, leave = threading.Event(), threading.Event()

        def holder():
            with writer_lock(resolved):
                ready.set()
                leave.wait(60)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            started = time.monotonic()
            assert healthy.check().exit_code == 0
            assert time.monotonic() - started < 8
        finally:
            leave.set()
            thread.join(10)

    def test_only_zfs_list_and_get_run(self, healthy):
        for flags in ((), ("--json",)):
            assert healthy.check(*flags).exit_code == 0
        calls = healthy.zfs.calls
        assert calls
        assert {c[0] for c in calls} == {"list", "get"}
        assert all(c[0] != "get" or c[:4] == ["get", "-H", "-o", "value"] for c in calls)

    def test_it_never_writes_a_property(self, healthy):
        healthy.check()
        assert not [c for c in healthy.zfs.calls if c[0] in ("set", "inherit", "snapshot", "destroy", "send")]

    def test_the_stored_configuration_is_not_changed(self, healthy):
        before = db_state(healthy.db)
        healthy.check()
        healthy.check("--nagios")
        assert db_state(healthy.db) == before


# --------------------------------------------------------------------------
# 7. the daemon verdict table
# --------------------------------------------------------------------------


def fake_status(verdict, daemon="alive", message="message from status", generation=7):
    info = None
    if daemon == "alive":
        info = daemonlock.DaemonInfo(pid=4321, generation=generation, start="tok")
    elif daemon == "nopid":
        info = daemonlock.DaemonInfo(pid=None, generation=generation, start="tok")
    return status_view.Status(
        "/x/config.db", SimpleNamespace(store_id=1, generation=generation), info, verdict, message)


def by_subject(entries):
    return {e.subject: e for e in entries}


class TestDaemonEntries:
    @pytest.mark.parametrize("verdict, config_state", [
        (status_view.IN_SYNC, "OK"),
        (status_view.APPLYING, "OK"),
        (status_view.BEHIND, "WARN"),
        (status_view.DEFERRED, "WARN"),
        (status_view.REJECTED, "WARN"),
        (status_view.UNREADABLE, "WARN"),
    ])
    def test_a_running_daemon(self, verdict, config_state):
        entries = by_subject(check_view.daemon_entries(health, fake_status(verdict)))
        assert set(entries) == {"process", "config_applied"}
        assert entries["process"].state == "OK"
        assert "pid 4321" in entries["process"].reason
        assert entries["config_applied"].state == config_state
        assert all(e.source == "daemon" for e in entries.values())

    def test_in_sync_names_the_generation(self):
        entries = by_subject(check_view.daemon_entries(health, fake_status(status_view.IN_SYNC, generation=9)))
        assert "9" in entries["config_applied"].reason

    @pytest.mark.parametrize("verdict", [status_view.BEHIND, status_view.DEFERRED, status_view.REJECTED])
    def test_a_warning_carries_the_status_message(self, verdict):
        entries = by_subject(check_view.daemon_entries(health, fake_status(verdict, message="because X")))
        assert entries["config_applied"].reason == "because X"

    def test_no_daemon_is_crit(self):
        entries = check_view.daemon_entries(
            health, fake_status(status_view.NO_DAEMON, daemon=None))
        assert [(e.subject, e.state) for e in entries] == [("process", "CRIT")]
        assert entries[0].reason == "no daemon is running"

    def test_a_gone_supervisor_is_crit_and_has_no_config_entry(self):
        entries = check_view.daemon_entries(
            health, fake_status(status_view.SUPERVISOR_GONE, message="supervisor pid 4321 is gone"))
        assert [(e.subject, e.state) for e in entries] == [("process", "CRIT")]
        assert "gone" in entries[0].reason

    def test_an_unknown_pid_is_still_running(self):
        entries = by_subject(check_view.daemon_entries(
            health, fake_status(status_view.IN_SYNC, daemon="nopid")))
        assert entries["process"].state == "OK" and "pid unknown" in entries["process"].reason

    def test_end_to_end_in_sync(self, healthy):
        entries = healthy.entries()
        assert entries[("daemon", "process", None)]["state"] == "OK"
        assert entries[("daemon", "config_applied", None)]["state"] == "OK"

    def test_end_to_end_a_daemon_that_is_behind(self, healthy):
        sid, gen = store_key(healthy.db)
        healthy.daemon.set(applied=(sid, gen - 1) if gen else (sid + 1, 0))
        entries = healthy.entries()
        assert entries[("daemon", "process", None)]["state"] == "OK"
        assert entries[("daemon", "config_applied", None)]["state"] == "WARN"

    def test_end_to_end_a_deferred_key_is_a_warning(self, healthy):
        sid, gen = store_key(healthy.db)
        healthy.daemon.set(applied=(sid, gen - 1) if gen else (sid + 1, 0),
                           deferred=(sid, gen), deferred_reason="port in use")
        entries = healthy.entries()
        assert entries[("daemon", "config_applied", None)]["state"] == "WARN"
        assert "port in use" in entries[("daemon", "config_applied", None)]["reason"]

    def test_end_to_end_no_daemon(self, world):
        world.build(daemon=False)
        entries = world.entries()
        assert entries[("daemon", "process", None)]["state"] == "CRIT"
        assert ("daemon", "config_applied", None) not in entries

    def test_a_pid_file_read_error_is_one_warning_not_an_abort(self, healthy, monkeypatch):
        def broken(resolved, **kw):
            raise CliError("could not check whether a daemon is running for x: Permission denied")

        monkeypatch.setattr(status_view, "collect", broken)
        result = healthy.check("--json")
        assert result.exit_code == 1, result.stderr
        entries = {(e["source"], e["subject"]): e for e in json.loads(result.stdout)["entries"]}
        assert entries[("daemon", "process")]["state"] == "WARN"
        assert "cannot tell whether a daemon runs" in entries[("daemon", "process")]["reason"]
        assert ("daemon", "config_applied") not in entries
        assert entries[("snapshot", "tank/a")]["state"] == "OK"      # the rest was still checked
        assert json.loads(result.stdout)["state"] == "WARN"


class TestDaemonUpgradeEntry:
    """S-8: `daemon/upgrade` is a WARN while the daemon drains for an in-place upgrade (the pid
    file's `draining=`), never CRIT, and absent otherwise."""

    SINCE = 1_790_000_000
    REASON = "zfsbackup upgraded (1.0.0 -> 1.1.0); draining the remote worker, then the API worker"

    def draining(self, **kw):
        info = daemonlock.DaemonInfo(
            pid=4321, generation=7, start="tok", draining=self.SINCE, draining_reason=self.REASON, **kw)
        return status_view.Status(
            "/x/config.db", SimpleNamespace(store_id=1, generation=7), info,
            status_view.IN_SYNC, "in sync")

    def test_a_draining_daemon_adds_one_warning_with_since_age_and_reason(self):
        now = self.SINCE + 125
        entries = by_subject(check_view.daemon_entries(health, self.draining(), now))
        assert set(entries) == {"process", "config_applied", "upgrade"}
        entry = entries["upgrade"]
        assert (entry.source, entry.state) == ("daemon", "WARN")
        since = datetime.fromtimestamp(self.SINCE, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        assert f"since {since}" in entry.reason
        assert health.format_duration(125.0) in entry.reason and "ago" in entry.reason
        assert self.REASON in entry.reason
        assert entries["process"].state == "OK" and entries["config_applied"].state == "OK"

    def test_a_draining_daemon_without_a_reason_has_no_trailing_colon(self):
        status = self.draining()
        status = dataclasses_replace(status, daemon=dataclasses_replace(status.daemon, draining_reason=None))
        entry = by_subject(check_view.daemon_entries(health, status, self.SINCE + 1))["upgrade"]
        assert not entry.reason.rstrip().endswith(":")

    def test_an_age_in_the_past_is_clamped_to_zero(self):
        entry = by_subject(check_view.daemon_entries(health, self.draining(), self.SINCE - 500))["upgrade"]
        assert health.format_duration(0.0) in entry.reason

    @pytest.mark.parametrize("verdict", [
        status_view.IN_SYNC, status_view.BEHIND, status_view.DEFERRED, status_view.REJECTED,
        status_view.APPLYING,
    ])
    def test_it_is_never_crit_whatever_the_verdict(self, verdict):
        status = dataclasses_replace(self.draining(), verdict=verdict)
        entries = check_view.daemon_entries(health, status, self.SINCE + 5)
        assert {e.state for e in entries if e.subject == "upgrade"} == {"WARN"}
        assert "CRIT" not in {e.state for e in entries}

    def test_not_draining_adds_nothing(self):
        entries = by_subject(check_view.daemon_entries(health, fake_status(status_view.IN_SYNC)))
        assert "upgrade" not in entries

    def test_no_daemon_adds_nothing(self):
        entries = check_view.daemon_entries(health, fake_status(status_view.NO_DAEMON, daemon=None))
        assert [e.subject for e in entries] == ["process"]

    def test_end_to_end_json_and_overall_warn(self, healthy):
        healthy.daemon.set(draining=self.SINCE, draining_reason=self.REASON)
        result = healthy.check("--json")
        assert result.exit_code == 1, result.stderr
        document = json.loads(result.stdout)
        assert document["state"] == "WARN"
        entry = {(e["source"], e["subject"]): e for e in document["entries"]}[("daemon", "upgrade")]
        assert entry["state"] == "WARN" and self.REASON in entry["reason"]
        assert healthy.entries()[("daemon", "process", None)]["state"] == "OK"

    def test_end_to_end_it_disappears_with_the_drain(self, healthy):
        healthy.daemon.set(draining=self.SINCE, draining_reason=self.REASON)
        assert ("daemon", "upgrade", None) in healthy.entries()
        healthy.daemon.set(draining=None, draining_reason=None)
        assert ("daemon", "upgrade", None) not in healthy.entries()
        assert healthy.check().exit_code == 0

    def test_end_to_end_without_a_daemon_there_is_no_upgrade_entry(self, world):
        world.build(daemon=False)
        assert ("daemon", "upgrade", None) not in world.entries()

    def test_end_to_end_prometheus(self, healthy):
        healthy.daemon.set(draining=self.SINCE, draining_reason=self.REASON)
        text = healthy.check("--prometheus").stdout
        assert 'zfsbackup_check_state{source="daemon",subject="upgrade"} 1' in text.splitlines()
        assert "daemon" in text and 'subject="process"} 0' in text
        for line in text.splitlines():
            assert line.startswith("#") or PROM_SAMPLE.match(line), line

    def test_end_to_end_prometheus_has_no_upgrade_series_otherwise(self, healthy):
        assert 'subject="upgrade"' not in healthy.check("--prometheus").stdout

    def test_end_to_end_nagios_is_warning_not_critical(self, healthy):
        healthy.daemon.set(draining=self.SINCE, draining_reason=self.REASON)
        result = healthy.check("--nagios")
        assert result.exit_code == 1
        assert result.stdout.startswith("ZFSBACKUP WARN - WARN daemon: upgrade")

    def test_end_to_end_text(self, healthy):
        healthy.daemon.set(draining=self.SINCE, draining_reason=self.REASON)
        out = healthy.check().stdout
        assert "daemon" in out and "upgrade" in out and "draining for an upgrade since" in out


def dataclasses_replace(obj, **kw):
    import dataclasses

    return dataclasses.replace(obj, **kw)


# --------------------------------------------------------------------------
# 8. the client certificate
# --------------------------------------------------------------------------


@pytest.fixture
def cert_world(run, db):
    make_db(run, db, CHECK_YAML)
    identity.ensure_identity(tls_of(db), "client")
    resolved = resolve_config_path(str(db))
    config = read_config(resolved).config
    dispose_all()
    files = tlsfiles.client_identity_files(config, resolved)
    not_after = tlsfiles.describe_certificate(files.cert).not_after
    return SimpleNamespace(db=db, resolved=resolved, config=config, cert=files.cert, not_after=not_after)


def cert_entry(world, now):
    return check_view.client_certificate_entry(health, world.config, world.resolved, now)


class TestCertificate:
    def test_the_warning_threshold(self):
        assert check_view.CERT_WARN_DAYS == 30

    def test_valid(self, cert_world):
        entry = cert_entry(cert_world, cert_world.not_after - timedelta(days=200))
        assert (entry.source, entry.subject, entry.state) == ("tls", "client_certificate", "OK")
        assert f"valid until {cert_world.not_after:%Y-%m-%d}" in entry.reason

    def test_just_outside_the_window_is_ok(self, cert_world):
        entry = cert_entry(cert_world, cert_world.not_after - timedelta(days=30, seconds=1))
        assert entry.state == "OK"

    @pytest.mark.parametrize("days", [30, 29.5, 10, 1])
    def test_inside_the_window_is_a_warning(self, cert_world, days):
        entry = cert_entry(cert_world, cert_world.not_after - timedelta(days=days))
        assert entry.state == "WARN"
        assert "expires in" in entry.reason and "identity rotate" in entry.reason

    def test_the_days_left_are_named(self, cert_world):
        entry = cert_entry(cert_world, cert_world.not_after - timedelta(days=10, hours=3))
        assert "expires in 10 day(s)" in entry.reason

    def test_expired_is_crit(self, cert_world):
        entry = cert_entry(cert_world, cert_world.not_after + timedelta(seconds=1))
        assert entry.state == "CRIT"
        assert f"expired on {cert_world.not_after:%Y-%m-%d}" in entry.reason

    def test_expiring_this_instant_is_crit(self, cert_world):
        assert cert_entry(cert_world, cert_world.not_after).state == "CRIT"

    def test_missing_is_a_warning(self, cert_world):
        cert_world.cert.unlink()
        entry = cert_entry(cert_world, cert_world.not_after - timedelta(days=200))
        assert entry.state == "WARN"
        assert "does not exist" in entry.reason

    @pytest.mark.parametrize("content", [b"not a certificate", b"", b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"])
    def test_unreadable_is_a_warning(self, cert_world, content):
        cert_world.cert.write_bytes(content)
        entry = cert_entry(cert_world, cert_world.not_after - timedelta(days=200))
        assert entry.state == "WARN" and "cannot be read" in entry.reason

    @needs_non_root
    def test_unreadable_by_permission_is_a_warning(self, cert_world):
        cert_world.cert.chmod(0)
        try:
            entry = cert_entry(cert_world, cert_world.not_after - timedelta(days=200))
        finally:
            cert_world.cert.chmod(0o640)
        assert entry.state == "WARN"

    def test_a_certificate_the_server_would_refuse_is_a_warning(self, cert_world, monkeypatch):
        def refuse(pem, now):
            raise ValueError("wrong extended key usage")

        monkeypatch.setattr(identity, "check_client_cert", refuse)
        entry = cert_entry(cert_world, cert_world.not_after - timedelta(days=200))
        assert entry.state == "WARN"
        assert "would not be accepted" in entry.reason and "wrong extended key usage" in entry.reason

    def test_no_destinations_means_no_entry(self, run, db):
        make_db(run, db, LOCAL_YAML)
        resolved = resolve_config_path(str(db))
        config = read_config(resolved).config
        dispose_all()
        assert check_view.client_certificate_entry(
            health, config, resolved, datetime.now(timezone.utc)) is None

    def test_it_never_reads_a_key(self, cert_world):
        key = tls_of(cert_world.db) / "client.key"
        key.unlink()
        assert cert_entry(cert_world, cert_world.not_after - timedelta(days=200)).state == "OK"

    def test_the_configured_certificate_file_is_honoured(self, run, db, tmp_path):
        other = tmp_path / "elsewhere"
        identity.ensure_identity(other, "client")
        yaml_text = (
            CHECK_YAML + f'client_cert_file: "{other / "client.crt"}"\n'
            f'client_key_file: "{other / "client.key"}"\n'
        )
        make_db(run, db, yaml_text)
        resolved = resolve_config_path(str(db))
        config = read_config(resolved).config
        dispose_all()
        assert not tls_of(db).exists()                      # the default location holds nothing
        files = tlsfiles.client_identity_files(config, resolved)
        assert files.cert == other / "client.crt"
        not_after = tlsfiles.describe_certificate(files.cert).not_after
        entry = check_view.client_certificate_entry(
            health, config, resolved, not_after - timedelta(days=200))
        assert entry.state == "OK"
        (other / "client.crt").unlink()
        assert check_view.client_certificate_entry(
            health, config, resolved, not_after - timedelta(days=200)).state == "WARN"

    def test_end_to_end_not_enrolled_is_a_warning(self, world):
        world.build(cert=False)
        entries = world.entries()
        assert entries[("tls", "client_certificate", None)]["state"] == "WARN"
        assert world.check().exit_code == 1

    def test_end_to_end_an_expired_certificate_is_crit(self, healthy, monkeypatch):
        real = check_view.client_certificate_entry
        monkeypatch.setattr(
            check_view, "client_certificate_entry",
            lambda h, c, r, now: real(h, c, r, now + timedelta(days=4000)))
        entries = healthy.entries()
        assert entries[("tls", "client_certificate", None)]["state"] == "CRIT"
        assert healthy.check("--nagios").exit_code == 2


# --------------------------------------------------------------------------
# 9. lazy imports
# --------------------------------------------------------------------------


class TestLazyImports:
    def run_python(self, code):
        return subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=120,
            cwd=str(REPO), env=dict(os.environ, PYTHONPATH=str(REPO)),
        )

    def test_importing_the_cli_loads_neither_health_collect_nor_libzfseasy(self):
        result = self.run_python(
            "import sys\n"
            "import zfsbackup.cli.main\n"
            "bad = [m for m in ('zfsbackup.health_collect', 'libzfseasy', 'zfsbackup.backup_manager')"
            " if m in sys.modules]\n"
            "assert not bad, bad\n"
        )
        assert result.returncode == 0, result.stderr

    def test_the_help_and_a_usage_error_do_not_load_them_either(self):
        result = self.run_python(
            "import sys\n"
            "from zfsbackup.cli import main\n"
            "try:\n"
            "    main.main(['check', '--help'])\n"
            "except SystemExit as e:\n"
            "    assert e.code == 0, e.code\n"
            "bad = [m for m in ('zfsbackup.health_collect', 'libzfseasy') if m in sys.modules]\n"
            "assert not bad, bad\n"
        )
        assert result.returncode == 0, result.stderr

    def test_the_health_module_is_stdlib_only(self):
        result = self.run_python(
            "import sys\n"
            "import zfsbackup.health\n"
            "bad = [m for m in ('libzfseasy', 'zfsbackup.config', 'zfsbackup.backup_manager', 'sqlalchemy')"
            " if m in sys.modules]\n"
            "assert not bad, bad\n"
        )
        assert result.returncode == 0, result.stderr


# --------------------------------------------------------------------------
# 10. quiet
# --------------------------------------------------------------------------


class TestQuiet:
    def test_config_warnings_are_not_printed(self, run, db, zfs):
        make_db(run, db, WARNY_YAML)
        zfs.snapshots("tank/forever", 600)
        zfs.snapshots("tank/ok", 600)
        # the same read prints the warnings in a command that is not quiet ...
        noisy = run("-c", db, "export")
        assert "warning:" in noisy.stderr
        # ... and check, which is read-only, prints none of them
        result = run("-c", db, "check")
        assert "warning:" not in result.stderr
        assert result.stderr == ""
        for flags in (("--json",), ("--prometheus",), ("--nagios",)):
            assert "warning:" not in run("-c", db, "check", *flags).stderr


# --------------------------------------------------------------------------
# 2b. signals
# --------------------------------------------------------------------------

DRIVER = (
    "import os, signal, sys, time\n"
    "signal.signal(signal.SIGINT, signal.default_int_handler)\n"   # a backgrounded shell may ignore it
    "from zfsbackup import health_collect\n"
    "from zfsbackup.cli import main\n"
    "sig = getattr(signal, sys.argv[1])\n"
    "def collect(config, *a, **k):\n"
    "    print('COLLECTING', flush=True)\n"
    "    os.kill(os.getpid(), sig)\n"
    "    time.sleep(20)\n"
    "health_collect.collect = collect\n"
    "main.main(sys.argv[2:])\n"
)


@needs_fork
class TestSignals:
    @pytest.mark.parametrize("signame, code", [("SIGINT", 130), ("SIGTERM", 143)])
    @pytest.mark.parametrize("extra", [[], ["--nagios"], ["--prometheus", "-o", "OUT"]])
    def test_a_signal_while_collecting(self, healthy, tmp_path, signame, code, extra):
        out = tmp_path / "check.prom"
        out.write_text("previous run\n")
        args = [str(out) if a == "OUT" else a for a in extra]
        process = subprocess.Popen(
            [sys.executable, "-c", DRIVER, signame, "-c", str(healthy.db), "check", *args],
            env=dict(os.environ, PYTHONPATH=str(REPO)), cwd=str(tmp_path), start_new_session=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            stdout, stderr = process.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            raise
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)
        assert "COLLECTING" in stdout
        assert process.returncode == code, (stdout, stderr)
        assert "Traceback" not in stderr
        assert out.read_text() == "previous run\n"                  # nothing was written
        assert not [p for p in tmp_path.iterdir() if p.name.startswith(".check")]


# --------------------------------------------------------------------------
# 12. worker outcomes (S-7b): `<db>.status/<worker>.json`
# --------------------------------------------------------------------------

SEND = "remote-backup-worker"
SNAP = "snapshot-worker"
PRUNE = "pruning-worker"


def dead_pid() -> int:
    """The pid of a process that has exited and been reaped."""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def worker_entries_of(world, *args):
    return {k: v for k, v in world.entries(*args).items() if k[0].startswith("worker")}


def send_state(world, failures, *args):
    write_worker(world.db, SEND, [rec("send", "tank/a", "offsite", failures=failures)])
    return world.entries(*args)[("worker_send", "tank/a", "offsite")]


@pytest.fixture
def at_rest(healthy):
    """A healthy host (live daemon, current generation) with no status directory."""
    return healthy


class TestWorkerNoData:
    """No data is never a failure: the output is what it was before S-7b."""

    def test_no_status_directory_adds_nothing_in_any_format(self, at_rest):
        assert not status_dir(at_rest.db).exists()
        text = at_rest.check()
        assert text.exit_code == 0 and "worker" not in text.stdout
        assert text.stdout.splitlines()[-1] == "overall: OK (5 checks: 5 ok, 0 warn, 0 crit)"
        data = json.loads(at_rest.check("--json").stdout)
        assert data["state"] == "OK" and len(data["entries"]) == 5
        assert not [e for e in data["entries"] if e["source"].startswith("worker")]
        assert all("kind" not in e and "consecutive_failures" not in e for e in data["entries"])
        prom = at_rest.check("--prometheus").stdout
        assert "worker" not in prom and "consecutive_failures" not in prom
        nagios = at_rest.check("--nagios").stdout
        assert nagios.startswith("ZFSBACKUP OK - 5 checks OK | ") and "worker" not in nagios

    def test_an_empty_status_directory_adds_nothing_either(self, at_rest):
        make_dir(at_rest.db)
        result = at_rest.check()
        assert result.exit_code == 0 and "worker" not in result.stdout
        assert result.stdout.splitlines()[-1].startswith("overall: OK (5 checks")

    def test_a_worker_without_a_file_has_no_entries(self, at_rest):
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a")])
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_snapshot", "tank/a", None)}
        assert at_rest.check().exit_code == 0

    def test_a_file_with_no_records_adds_nothing(self, at_rest):
        write_worker(at_rest.db, SEND, [])
        assert worker_entries_of(at_rest) == {}

    def test_unrelated_names_in_the_directory_are_ignored(self, at_rest):
        directory = make_dir(at_rest.db)
        (directory / ".snapshot-worker.json.1.2.tmp").write_text("{")
        (directory / "README").write_text("hello")
        (directory / "Not_A_Worker.json").write_text("{")
        assert worker_entries_of(at_rest) == {}
        assert at_rest.check().exit_code == 0


class TestWorkerGrading:
    @pytest.mark.parametrize("failures, state", [
        (0, "OK"), (1, "OK"), (2, "WARN"), (3, "WARN"), (4, "CRIT"), (5, "CRIT"),
    ])
    def test_default_factors(self, at_rest, failures, state):
        entry = send_state(at_rest, failures)
        assert entry["state"] == state, entry
        assert entry["consecutive_failures"] == failures
        assert at_rest.check().exit_code == (0 if state == "OK" else 1)

    @pytest.mark.parametrize("failures, state", [
        (0, "OK"), (1, "WARN"), (2, "WARN"), (3, "CRIT"), (4, "CRIT"), (5, "CRIT"),
    ])
    def test_custom_factors(self, at_rest, failures, state):
        entry = send_state(at_rest, failures, "--warn-factor", "1", "--crit-factor", "3")
        assert entry["state"] == state, entry

    def test_fractional_factors(self, at_rest):
        # 1 failure against WARN from 0.5: WARN; CRIT from 1.5: not yet
        assert send_state(at_rest, 1, "--warn-factor", "0.5", "--crit-factor", "1.5")["state"] == "WARN"
        assert send_state(at_rest, 2, "--warn-factor", "0.5", "--crit-factor", "1.5")["state"] == "CRIT"

    def test_one_failure_is_noted_but_not_a_verdict(self, at_rest):
        entry = send_state(at_rest, 1)
        assert entry["state"] == "OK"
        assert "last attempt failed" in entry["reason"] and "1 in a row; WARN from 2" in entry["reason"]
        assert "boom" in entry["reason"]

    def test_the_reason_of_a_warning_names_both_thresholds_and_the_last_success(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=3,
                                            success=time.time() - 7200, error="resume token check failed")])
        entry = at_rest.entries()[("worker_send", "tank/a", "offsite")]
        assert entry["state"] == "WARN"
        assert "3 attempts in a row failed (WARN from 2, CRIT from 4)" in entry["reason"]
        assert "resume token check failed" in entry["reason"]
        assert "last success 2h" in entry["reason"]

    def test_never_succeeded_says_never(self, at_rest):
        entry = send_state(at_rest, 4)
        assert "last success never" in entry["reason"]

    def test_success_after_failures_is_ok(self, at_rest):
        write_worker(at_rest.db, SEND, [rec(
            "send", "tank/a", "offsite", failures=0, result="ok", success=time.time() - 30,
            error="an old error", error_at=time.time() - 3600)])
        entry = at_rest.entries()[("worker_send", "tank/a", "offsite")]
        assert entry["state"] == "OK" and entry["consecutive_failures"] == 0
        assert "last attempt succeeded" in entry["reason"]
        assert "an old error" not in entry["reason"]

    def test_the_detail_of_a_success_is_shown(self, at_rest):
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a", detail="created autosnap_20260101000000")])
        entry = at_rest.entries()[("worker_snapshot", "tank/a", None)]
        assert "(created autosnap_20260101000000)" in entry["reason"]

    def test_no_attempt_yet_is_ok(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", result=None, attempt=None)])
        entry = at_rest.entries()[("worker_send", "tank/a", "offsite")]
        assert entry["state"] == "OK" and entry["reason"] == "no attempt recorded yet"
        assert entry["last_attempt"] is None and entry["last_success"] is None

    def test_result_error_with_zero_failures_counts_as_one(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", result="error", failures=0,
                                            error="x")])
        entries = at_rest.entries()
        assert entries[("worker_send", "tank/a", "offsite")]["state"] == "OK"
        assert entries[("worker_send", "tank/a", "offsite")]["consecutive_failures"] == 1
        warned = at_rest.entries("--warn-factor", "1", "--crit-factor", "3")
        assert warned[("worker_send", "tank/a", "offsite")]["state"] == "WARN"

    def test_an_error_result_without_failures_never_reads_as_success(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", result="error", failures=0,
                                            error="x")])
        entry = at_rest.entries()[("worker_send", "tank/a", "offsite")]
        assert "last attempt succeeded" not in entry["reason"]

    def test_unknown_kinds_are_skipped(self, at_rest):
        write_worker(at_rest.db, SNAP, [rec("scrub", "tank/a", failures=9), rec("snapshot", "tank/a")])
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_snapshot", "tank/a", None)}
        assert at_rest.check().exit_code == 0

    def test_every_kind_gets_its_own_source(self, at_rest):
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a")])
        write_worker(at_rest.db, PRUNE, [rec("prune", "tank/a")])
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite")])
        assert set(worker_entries_of(at_rest)) == {
            ("worker_snapshot", "tank/a", None), ("worker_prune", "tank/a", None),
            ("worker_send", "tank/a", "offsite")}

    def test_a_discover_record_is_graded_like_the_others(self, at_rest):
        error = "cannot list the received datasets: pool busy"
        write_worker(at_rest.db, SEND, [rec("discover", "bk", failures=4, error=error)])
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_discover", "bk", None)}
        entry = entries[("worker_discover", "bk", None)]
        assert entry["state"] == "CRIT" and entry["kind"] == "discover"
        assert error in entry["reason"]
        result = at_rest.check()
        assert result.exit_code == 1
        assert re.search(r"^CRIT worker_discover: bk: 4 attempts in a row failed", result.stdout, re.M)
        parsed = Exposition(at_rest.check("--prometheus").stdout)
        assert parsed.one("zfsbackup_worker_consecutive_failures", kind="discover", dataset="bk") == 4
        assert "'worker_discover:bk'=4;2;4;0" in at_rest.check("--nagios").stdout

    @pytest.mark.parametrize("failures, state", [(0, "OK"), (1, "OK"), (2, "WARN"), (4, "CRIT")])
    def test_discover_boundaries(self, at_rest, failures, state):
        write_worker(at_rest.db, SEND, [rec("discover", "bk", failures=failures)])
        assert at_rest.entries()[("worker_discover", "bk", None)]["state"] == state

    def test_a_heartbeat_file_without_records_has_no_outcome_entries(self, at_rest):
        for worker in (SNAP, PRUNE, SEND):
            write_worker(at_rest.db, worker, [])
        assert worker_entries_of(at_rest) == {}
        result = at_rest.check()
        assert result.exit_code == 0 and result.stdout.splitlines()[-1].startswith("overall: OK (5 checks")

    def test_one_entry_per_destination(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4),
                                        rec("send", "tank/a", "dc.two")])
        entries = worker_entries_of(at_rest)
        assert entries[("worker_send", "tank/a", "offsite")]["state"] == "CRIT"
        assert entries[("worker_send", "tank/a", "dc.two")]["state"] == "OK"

    def test_a_crit_worker_makes_the_overall_crit(self, at_rest):
        write_worker(at_rest.db, PRUNE, [rec("prune", "tank/a", failures=4)])
        result = at_rest.check()
        assert result.exit_code == 1
        assert re.search(r"^CRIT worker_prune: tank/a: 4 attempts in a row failed", result.stdout, re.M)
        assert result.stdout.splitlines()[-1].startswith("overall: CRIT")
        assert at_rest.check("--nagios").exit_code == 2

    def test_the_text_line_of_a_send_names_the_destination(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=2)])
        assert re.search(r"^WARN worker_send: tank/a \(offsite\): ", at_rest.check().stdout, re.M)

    def test_the_entries_come_after_the_certificate_entry(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite")])
        sources = [e["source"] for e in json.loads(at_rest.check("--json").stdout)["entries"]]
        assert sources[-2:] == ["tls", "worker_send"]

    def test_status_files_do_not_hide_the_other_checks(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)])
        at_rest.zfs.snapshots("tank/a", 5 * 3600)
        entries = at_rest.entries()
        assert entries[("snapshot", "tank/a", None)]["state"] == "CRIT"
        assert entries[("worker_send", "tank/a", "offsite")]["state"] == "CRIT"


def _symlink(world, directory):
    target = directory / "elsewhere"
    target.write_text("{}")
    os.chmod(target, 0o640)
    (directory / "pruning-worker.json").symlink_to(target)


def _group_writable(world, directory):
    write_worker(world.db, PRUNE, [rec("prune", "tank/a")])
    os.chmod(directory / "pruning-worker.json", 0o660)


def _not_json(world, directory):
    path = directory / "pruning-worker.json"
    path.write_text("{ not json")
    os.chmod(path, 0o640)


def _schema_two(world, directory):
    write_worker(world.db, PRUNE, [rec("prune", "tank/a")], schema=2)


def _wrong_worker(world, directory):
    path = directory / "pruning-worker.json"
    path.write_text(json.dumps({"schema": 1, "worker": "snapshot-worker", "records": []}))
    os.chmod(path, 0o640)


@needs_non_root
class TestWorkerRefusedFiles:
    """A file the reader refuses is one WARN `worker_status` entry naming it; the other files are
    still graded."""

    CASES = [
        ("symlink", _symlink, "symlink"),
        ("group-writable", _group_writable, "writable by group or others"),
        ("not-json", _not_json, "not valid JSON"),
        ("schema-2", _schema_two, "schema 2"),
        ("wrong-worker", _wrong_worker, "belongs to worker"),
    ]

    @pytest.mark.parametrize("prepare, fragment", [c[1:] for c in CASES], ids=[c[0] for c in CASES])
    def test_one_warning_and_the_others_are_graded(self, at_rest, prepare, fragment):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)])
        prepare(at_rest, status_dir(at_rest.db))
        entries = worker_entries_of(at_rest)
        refused = [v for k, v in entries.items() if k[0] == "worker_status"]
        assert len(refused) == 1, entries
        assert refused[0]["state"] == "WARN" and refused[0]["subject"] == "pruning-worker.json"
        assert fragment in refused[0]["reason"] and refused[0]["reason"].startswith("worker status not read: ")
        assert entries[("worker_send", "tank/a", "offsite")]["state"] == "CRIT"     # still graded
        assert "kind" not in refused[0]

    def test_a_refused_file_alone_is_a_warning_not_a_crit(self, at_rest):
        make_dir(at_rest.db)
        _not_json(at_rest, status_dir(at_rest.db))
        result = at_rest.check()
        assert result.exit_code == 1
        assert "WARN worker_status: pruning-worker.json: worker status not read" in result.stdout
        assert result.stdout.splitlines()[-1].startswith("overall: WARN")
        assert at_rest.check("--nagios").exit_code == 1

    def test_a_group_writable_directory_is_one_warning_and_nothing_is_graded(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)])
        os.chmod(status_dir(at_rest.db), 0o770)
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_status", "status directory", None)}
        assert entries[("worker_status", "status directory", None)]["state"] == "WARN"
        assert "writable by group or others" in entries[("worker_status", "status directory", None)]["reason"]
        os.chmod(status_dir(at_rest.db), 0o750)

    def test_a_mount_point_directory_is_one_warning(self, at_rest, monkeypatch):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)])
        directory = status_dir(at_rest.db)
        real = os.stat

        def other_device(path, *a, **k):
            result = real(path, *a, **k)
            if not a and not k and str(path) == str(directory.parent):
                fields = list(result)[:10]
                fields[2] = result.st_dev + 1
                return os.stat_result(fields)
            return result

        monkeypatch.setattr(os, "stat", other_device)
        entries = worker_entries_of(at_rest)
        assert [k[:2] for k in entries] == [("worker_status", "status directory")]
        assert "is a mount point" in entries[("worker_status", "status directory", None)]["reason"]

    def test_a_symlinked_directory_is_one_warning(self, at_rest, tmp_path):
        real = tmp_path / "real-status"
        real.mkdir(mode=0o750)
        status_dir(at_rest.db).symlink_to(real)
        entries = worker_entries_of(at_rest)
        assert [k[:2] for k in entries] == [("worker_status", "status directory")]
        assert "symlink" in entries[("worker_status", "status directory", None)]["reason"]

    def test_an_untrusted_owner_is_one_warning_per_file(self, at_rest, monkeypatch):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite")])
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a")])
        # the directory check is skipped so that the per-file owner check is what refuses
        monkeypatch.setattr(health, "_check_reader_dir", lambda *a, **k: None)
        monkeypatch.setattr(status_view, "trusted_status_uids", lambda resolved: {os.geteuid() + 4242})
        entries = worker_entries_of(at_rest)
        assert sorted(k[1] for k in entries) == ["remote-backup-worker.json", "snapshot-worker.json"]
        for entry in entries.values():
            assert entry["source"] == "worker_status" and entry["state"] == "WARN"
            assert "is not trusted" in entry["reason"]

    def test_a_directory_owned_by_an_untrusted_user_is_refused_whole(self, at_rest, monkeypatch):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite")])
        monkeypatch.setattr(status_view, "trusted_status_uids", lambda resolved: {os.geteuid() + 4242})
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_status", "status directory", None)}
        assert "not trusted" in entries[("worker_status", "status directory", None)]["reason"]

    def test_duplicate_subjects_are_made_unique(self):
        workers = status_view.Workers(
            "/x.status", True, (),
            ("/a/x.status/snapshot-worker.json is a symlink",
             "/b/x.status/snapshot-worker.json is not valid JSON",
             "/c/snapshot-worker.json is schema 2",
             "the directory is odd", "the directory is odder"),
        )
        entries = check_view.worker_entries(health, workers, 0.0, 2.0, 4.0)
        assert [e.subject for e in entries] == [
            "snapshot-worker.json", "snapshot-worker.json#2", "snapshot-worker.json#3",
            "status directory", "status directory#2"]
        assert all(e.source == "worker_status" and e.state == health.STATE_WARN for e in entries)

    def test_unique_subjects_make_unique_prometheus_series(self):
        workers = status_view.Workers(
            "/x.status", True, (),
            ("/a/snapshot-worker.json is bad", "/b/snapshot-worker.json is bad"))
        entries = check_view.worker_entries(health, workers, 0.0, 2.0, 4.0)
        parsed = Exposition(health.to_prometheus(entries, 1_700_000_000, "1"))
        assert len(parsed.values("zfsbackup_check_state", source="worker_status")) == 2

    def test_the_problem_text_is_one_line(self):
        workers = status_view.Workers("/x", True, (), ("/a/x.json broke\non two\nlines",))
        (entry,) = check_view.worker_entries(health, workers, 0.0, 2.0, 4.0)
        assert "\n" not in entry.reason


class TestWorkerTrustedUids:
    """The daemon's uid is the owner of `<db>.daemon.pid` (the service user may differ from the DB's
    owner); a symlinked pid file is never trusted."""

    @pytest.fixture
    def resolved(self, run, db):
        make_db(run, db, CHECK_YAML)
        return resolve_config_path(str(db))

    def _lstat(self, monkeypatch, pid_path, mode, uid):
        real = os.lstat

        def fake(path, *a, **k):
            if str(path) == str(pid_path):
                fields = list(real(path, *a, **k))[:10]
                fields[0] = mode | 0o640
                fields[4] = uid
                return os.stat_result(fields)
            return real(path, *a, **k)

        monkeypatch.setattr(os, "lstat", fake)

    def test_the_default_set(self, resolved):
        uids = status_view.trusted_status_uids(resolved)
        assert {0, os.geteuid(), os.stat(resolved.resolved_path).st_uid} <= uids

    def test_the_pid_file_owner_is_added(self, resolved, monkeypatch):
        pid_path = daemonlock.lock_path(resolved)
        pid_path.write_text("1\n0\n")
        self._lstat(monkeypatch, pid_path, stat.S_IFREG, 7777)
        assert 7777 in status_view.trusted_status_uids(resolved)

    def test_a_symlinked_pid_file_is_not_trusted(self, resolved, monkeypatch):
        pid_path = daemonlock.lock_path(resolved)
        pid_path.write_text("1\n0\n")
        self._lstat(monkeypatch, pid_path, stat.S_IFLNK, 7777)
        assert 7777 not in status_view.trusted_status_uids(resolved)

    def test_a_directory_is_not_trusted_either(self, resolved, monkeypatch):
        pid_path = daemonlock.lock_path(resolved)
        pid_path.write_text("1\n0\n")
        self._lstat(monkeypatch, pid_path, stat.S_IFDIR, 7777)
        assert 7777 not in status_view.trusted_status_uids(resolved)

    def test_a_missing_pid_file_adds_nothing(self, resolved):
        assert not daemonlock.lock_path(resolved).exists()
        assert status_view.trusted_status_uids(resolved) == {
            0, os.geteuid(), os.stat(resolved.resolved_path).st_uid}

    def test_collect_workers_passes_the_set_to_the_reader(self, resolved, monkeypatch):
        seen = {}

        def fake(directory, *, trusted_uids=None):
            seen["uids"] = set(trusted_uids)
            return health.StatusReadResult(present=False)

        monkeypatch.setattr(health, "read_status_dir", fake)
        pid_path = daemonlock.lock_path(resolved)
        pid_path.write_text("1\n0\n")
        self._lstat(monkeypatch, pid_path, stat.S_IFREG, 7777)
        status_view.collect_workers(resolved)
        assert 7777 in seen["uids"]


class TestWorkerFormats:
    def failing(self, world, dataset="tank/vm.01", destination="offsite", failures=3):
        write_worker(world.db, SEND, [rec("send", dataset, destination, failures=failures,
                                          success=time.time() - 7200)])

    def test_json_extra_keys_and_schema(self, at_rest):
        self.failing(at_rest, "tank/a")
        data = json.loads(at_rest.check("--json").stdout)
        assert data["schema"] == 1
        (entry,) = [e for e in data["entries"] if e["source"] == "worker_send"]
        assert entry["subject"] == "tank/a" and entry["peer"] == "offsite" and entry["state"] == "WARN"
        assert entry["kind"] == "send" and entry["consecutive_failures"] == 3
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", entry["last_attempt"])
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", entry["last_success"])
        others = [e for e in data["entries"] if not e["source"].startswith("worker")]
        assert all("kind" not in e and "last_attempt" not in e for e in others)

    def test_a_snapshot_entry_has_no_peer(self, at_rest):
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a")])
        (entry,) = [e for e in json.loads(at_rest.check("--json").stdout)["entries"]
                    if e["source"] == "worker_snapshot"]
        assert "peer" not in entry or entry["peer"] is None

    def test_prometheus_series_and_placement(self, at_rest):
        self.failing(at_rest, "tank/a")
        text = at_rest.check("--prometheus").stdout
        parsed = Exposition(text)
        assert parsed.one("zfsbackup_check_state", source="worker_send", subject="tank/a",
                          peer="offsite") == 1
        assert parsed.one("zfsbackup_worker_consecutive_failures", kind="send", dataset="tank/a",
                          destination="offsite") == 3
        assert parsed.kind["zfsbackup_worker_consecutive_failures"] == "gauge"
        # placed before the overall-state block
        assert text.index("# HELP zfsbackup_worker_consecutive_failures") < text.index(
            "# HELP zfsbackup_check_overall_state")
        assert text.index("# HELP zfsbackup_check_state") < text.index(
            "# HELP zfsbackup_worker_consecutive_failures")
        assert parsed.one("zfsbackup_check_overall_state") == 1

    def test_the_failures_gauge_has_no_destination_label_for_a_snapshot(self, at_rest):
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a", failures=1)])
        parsed = Exposition(at_rest.check("--prometheus").stdout)
        (sample,) = [s for s in parsed.samples if s[0] == "zfsbackup_worker_consecutive_failures"]
        assert sample[1] == {"kind": "snapshot", "dataset": "tank/a"} and sample[2] == 1

    def test_prometheus_without_workers_has_no_failures_gauge(self, at_rest):
        text = at_rest.check("--prometheus").stdout
        assert "zfsbackup_worker_consecutive_failures" not in text
        Exposition(text)

    def test_prometheus_label_escaping(self):
        nasty = 'tank/a"b\\c\nd'
        entry = check_view.OutcomeEntry(
            "worker_send", nasty, health.STATE_WARN, "r", nasty,
            kind="send", consecutive_failures=2, warn_failures=2.0, crit_failures=4.0)
        text = check_view.render_prometheus(health, [entry], 1_700_000_000, "1")
        parsed = Exposition(text)                                   # strict: raw newline would fail
        assert parsed.one("zfsbackup_worker_consecutive_failures", kind="send", dataset=nasty,
                          destination=nasty) == 2
        assert parsed.one("zfsbackup_check_state", source="worker_send", subject=nasty, peer=nasty) == 1
        assert 'dataset="tank/a\\"b\\\\c\\nd"' in text

    def test_prometheus_dotted_dataset(self, at_rest):
        self.failing(at_rest)
        parsed = Exposition(at_rest.check("--prometheus").stdout)
        assert parsed.one("zfsbackup_worker_consecutive_failures", dataset="tank/vm.01") == 3

    def test_nagios_perfdata_has_the_failures_and_both_thresholds(self, at_rest):
        self.failing(at_rest, "tank/a")
        result = at_rest.check("--nagios")
        assert result.exit_code == 1
        assert "'worker_send:tank/a@offsite'=3;2;4;0" in result.stdout
        assert "'snapshot:tank/a'=" in result.stdout                # the lag items are still there

    def test_nagios_thresholds_follow_the_factors(self, at_rest):
        self.failing(at_rest, "tank/a")
        out = at_rest.check("--nagios", "--warn-factor", "1.5", "--crit-factor", "6").stdout
        assert "'worker_send:tank/a@offsite'=3;1.5;6;0" in out

    def test_nagios_perfdata_escapes_quotes_and_equals(self):
        entry = check_view.OutcomeEntry(
            "worker_send", "it's a=b", health.STATE_OK, "fine", "of f=g",
            kind="send", consecutive_failures=0, warn_failures=2.0, crit_failures=4.0)
        text = check_view.render_nagios(health, [entry])
        assert "'worker_send:it''s a:b@of f:g'=0;2;4;0" in text
        assert text.count("\n") == 1

    def test_nagios_names_a_failing_worker_in_the_summary(self, at_rest):
        self.failing(at_rest, "tank/a", failures=4)
        out = at_rest.check("--nagios").stdout
        assert out.startswith("ZFSBACKUP CRIT - CRIT worker_send: tank/a (offsite): 4 attempts")

    @pytest.mark.parametrize("flag, option", [("--json", "--output"), ("--prometheus", "--output"),
                                              (None, "--output")])
    def test_output_carries_the_entries(self, at_rest, tmp_path, flag, option):
        self.failing(at_rest, "tank/a", failures=4)
        target = tmp_path / "out.txt"
        args = ([flag] if flag else []) + [option, target]
        result = at_rest.check(*args)
        assert result.exit_code == 0 and result.stdout == ""
        text = target.read_text()
        assert "worker_send" in text
        if flag == "--json":
            (entry,) = [e for e in json.loads(text)["entries"] if e["source"] == "worker_send"]
            assert entry["state"] == "CRIT" and entry["consecutive_failures"] == 4
        elif flag == "--prometheus":
            assert Exposition(text).one("zfsbackup_worker_consecutive_failures", dataset="tank/a") == 4

    def test_a_dotted_dataset_survives_all_four_formats(self, at_rest, tmp_path):
        self.failing(at_rest)
        text = at_rest.check().stdout
        assert "worker_send: tank/vm.01 (offsite)" in text
        data = json.loads(at_rest.check("--json").stdout)
        assert ("tank/vm.01", "offsite") in [(e["subject"], e.get("peer")) for e in data["entries"]
                                             if e["source"] == "worker_send"]
        assert 'dataset="tank/vm.01"' in at_rest.check("--prometheus").stdout
        assert "'worker_send:tank/vm.01@offsite'=3;2;4;0" in at_rest.check("--nagios").stdout

    def test_text_collapses_a_hostile_error(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4,
                                            error="line1\nline2 \x1b[31mred")])
        result = at_rest.check()
        assert result.stdout.count("worker_send") == 1
        assert "\x1b" not in result.stdout
        assert "\n" not in at_rest.check("--nagios").stdout.rstrip("\n")


class TestWorkerStaleness:
    """A file that cannot be taken as current -- no daemon runs, its writer is gone, or it has not
    been updated for `max(4 x check_interval, 24 h)` -- is ONE `worker_status` WARN per worker, its
    records are not graded, and it is never OK or CRIT because of its age."""

    def test_a_stopped_daemons_files_are_stale(self, world):
        world.build(daemon=False)
        write_worker(world.db, SEND, [rec("send", "tank/a", "offsite", failures=9)])
        write_worker(world.db, SNAP, [rec("snapshot", "tank/a")])
        entries = worker_entries_of(world)
        assert set(entries) == {("worker_status", SEND, None), ("worker_status", SNAP, None)}
        for entry in entries.values():
            assert entry["state"] == "WARN"
            assert entry["reason"].startswith(f"no recent data from {entry['subject']}: ")
            assert "no daemon is running" in entry["reason"]
        assert "1 record(s), 1 failing" in entries[("worker_status", SEND, None)]["reason"]
        assert "1 record(s), 0 failing" in entries[("worker_status", SNAP, None)]["reason"]
        assert not [k for k in entries if k[0] == "worker_send"]

    def test_a_failing_record_in_a_stale_file_is_never_crit(self, world):
        world.build(daemon=False)
        write_worker(world.db, SEND, [rec("send", "tank/a", "offsite", failures=50)])
        data = json.loads(world.check("--json").stdout)
        states = {(e["source"], e["subject"]): e["state"] for e in data["entries"]}
        assert states[("worker_status", SEND)] == "WARN"
        assert states[("daemon", "process")] == "CRIT"              # the daemon entry carries the CRIT
        # with the daemon back, staleness alone leaves the host at WARN
        write_worker(world.db, SEND, [rec("send", "tank/a", "offsite", failures=50)], pid=dead_pid())
        world.daemon = world._daemon(world.db, applied=store_key(world.db))
        again = json.loads(world.check("--json").stdout)
        assert again["state"] == "WARN"
        assert world.check().exit_code == 1 and world.check("--nagios").exit_code == 1

    def test_a_dead_writer_pid_is_stale(self, at_rest):
        pid = dead_pid()
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=9)], pid=pid)
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_status", SEND, None)}
        assert f"its writer (pid {pid}) is gone" in entries[("worker_status", SEND, None)]["reason"]
        assert entries[("worker_status", SEND, None)]["state"] == "WARN"

    def test_a_live_writer_pid_is_current(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)], pid=LIVE_PID)
        assert ("worker_send", "tank/a", "offsite") in worker_entries_of(at_rest)

    def test_a_file_without_a_pid_is_judged_by_age_only(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)], pid=None)
        assert ("worker_send", "tank/a", "offsite") in worker_entries_of(at_rest)

    def test_an_unknowable_pid_is_not_stale_by_that(self, at_rest, monkeypatch):
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: None)
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)], pid=dead_pid())
        assert ("worker_send", "tank/a", "offsite") in worker_entries_of(at_rest)

    @pytest.mark.parametrize("hours, stale", [(23, False), (24.5, True), (30, True)])
    def test_the_age_bound_is_24_hours(self, at_rest, hours, stale):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)],
                     updated_at=time.time() - hours * 3600)
        entries = worker_entries_of(at_rest)
        if stale:
            assert set(entries) == {("worker_status", SEND, None)}
            assert "not updated for" in entries[("worker_status", SEND, None)]["reason"]
            assert "(more than 1d)" in entries[("worker_status", SEND, None)]["reason"]
        else:
            assert set(entries) == {("worker_send", "tank/a", "offsite")}

    @pytest.mark.parametrize("hours, stale", [(25, False), (27, False), (29, True)])
    def test_a_long_check_interval_widens_the_bound_to_four_intervals(self, world, hours, stale):
        world.build(CHECK_YAML.replace('"5m"', '"7h"'))               # 4 x 7h = 28h
        write_worker(world.db, SEND, [rec("send", "tank/a", "offsite", failures=4)],
                     updated_at=time.time() - hours * 3600)
        entries = worker_entries_of(world)
        assert set(entries) == (
            {("worker_status", SEND, None)} if stale else {("worker_send", "tank/a", "offsite")})
        if stale:
            assert "(more than 1d4h)" in entries[("worker_status", SEND, None)]["reason"]

    def test_a_missing_updated_at_is_stale(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)], updated_at=None)
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_status", SEND, None)}
        assert "records no update time" in entries[("worker_status", SEND, None)]["reason"]

    def test_mixed_fresh_and_stale(self, at_rest):
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a", failures=9)], pid=dead_pid())
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)])
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_status", SNAP, None), ("worker_send", "tank/a", "offsite")}
        assert entries[("worker_status", SNAP, None)]["state"] == "WARN"
        assert entries[("worker_send", "tank/a", "offsite")]["state"] == "CRIT"

    def test_one_entry_per_worker_however_many_records(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4),
                                        rec("send", "tank/b", "offsite", failures=4),
                                        rec("send", "tank/c", "offsite")], pid=dead_pid())
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_status", SEND, None)}
        assert "3 record(s), 2 failing" in entries[("worker_status", SEND, None)]["reason"]

    def test_a_stale_entry_has_no_outcome_keys(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=4)], pid=dead_pid())
        (entry,) = [e for e in json.loads(at_rest.check("--json").stdout)["entries"]
                    if e["source"] == "worker_status"]
        assert "kind" not in entry and "consecutive_failures" not in entry

    def test_stale_in_all_four_formats(self, at_rest, tmp_path):
        write_worker(at_rest.db, SEND, [rec("send", "tank/vm.01", "offsite", failures=9)], pid=dead_pid())
        text = at_rest.check()
        assert re.search(rf"^WARN worker_status: {SEND}: no recent data from {SEND}: ", text.stdout, re.M)
        assert text.exit_code == 1 and text.stdout.splitlines()[-1].startswith("overall: WARN")
        data = json.loads(at_rest.check("--json").stdout)
        assert data["state"] == "WARN"
        parsed = Exposition(at_rest.check("--prometheus").stdout)
        assert parsed.one("zfsbackup_check_state", source="worker_status", subject=SEND) == 1
        assert "zfsbackup_worker_consecutive_failures" not in at_rest.check("--prometheus").stdout
        nagios = at_rest.check("--nagios")
        assert nagios.exit_code == 1 and "worker_send" not in nagios.stdout
        target = tmp_path / "o.prom"
        assert at_rest.check("--prometheus", "--output", target).exit_code == 0
        assert 'source="worker_status"' in target.read_text()

    def test_a_fresh_file_of_a_running_daemon_is_unaffected(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite")], updated_at=time.time() - 5)
        assert at_rest.entries()[("worker_send", "tank/a", "offsite")]["state"] == "OK"
        assert at_rest.check().exit_code == 0

    def test_the_stale_bound_function(self):
        assert status_view.stale_after(None) == 24 * 3600
        assert status_view.stale_after(300) == 24 * 3600
        assert status_view.stale_after(24 * 3600) == 4 * 24 * 3600
        assert status_view.stale_after(7 * 3600) == 28 * 3600

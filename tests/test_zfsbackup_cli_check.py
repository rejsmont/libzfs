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
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

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

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
- ``TestNagiosCap``     `--nagios` output capped at 4000 bytes: whole perfdata items, the kept
                        set chosen CRIT > WARN > OK but emitted in the entries' ORIGINAL order,
                        `omitted=N` last, the exact-fit boundary, bytes not characters, a
                        clipped status line, exit code, help texts.
- ``TestRecursiveDescendants`` (A3) the expanded descendants of a recursive dataset end to
                        end through a section-local fake `zfs` that answers the BATCHED reads
                        (one recursive snapshot listing and one two-phase property read per
                        parent): child anchor entries, no child snapshot entry, a failed
                        discovery as one WARN `descendants` entry, the ERROR on stderr only, a
                        failed batch as CRIT for every child of that parent, a copy marker as a
                        `copies` WARN entry.
- ``TestNagiosMaxBytes`` `--nagios-max-bytes` (min 64, default 4000; usage error / UNKNOWN below,
                        refused without `--nagios`, the budget used exactly, help).
- ``TestTimeLimitOptions`` / ``TestTimeLimitPassedToCollect`` / ``TestTimeLimitExceeded``
                        `--time-limit` and `--per-call-timeout`: the parse matrix, the keyword
                        hand-over `gather` -> `collect`, a real overrun (a slow fake `zfs`) as one
                        `(time limit)` WARN in every format, the shipped unit's TimeoutStartSec.
- ``TestWorkerWarning`` an outcome record's `warning`: WARN (never CRIT) for a succeeded attempt,
                        a failing record keeps its grading, JSON `warning`, stale files.
- ``TestCopiesEntries`` / ``TestBatching`` `copies` WARN entries in every format (cap 20 + "and N
                        more", nothing on stderr) and O(parents) execs for 40 children.

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
Nagios cap and A3 (this file's `TestNagiosCap` + `TestRecursiveDescendants`, 36 tests): limit
4000 -> 8000 15; `omitted=N` never appended 16; no severity sort 5; OK first 7; alphabetical
within a state 2; characters instead of bytes 5; `continue` for `break` 1; status line never
clipped 4; no room reserved for `omitted` 8; item cost without its space 9; all-fit `<=` -> `<`
1; the newline not budgeted 2; `omitted` off by one 14; no-perfdata line unclipped 1; outcome
perfdata dropped 1; option help removed 1; A3: descendants not observed 9; failed discovery
silent 3; child gets a snapshot entry 3; an unreadable anchor read as "none" 1; a failed
discovery graded OK 3.
Check round 2 (perfdata order, `--nagios-max-bytes`, `--time-limit`/`--per-call-timeout`, record
`warning`, `copies`, batched reads; failing tests in this file, one revert at a time): kept items
emitted in severity order 6 (all-fit path 6); `omitted` first 23; the minimum 64 -> 32 8, `<` -> `<=`
5, callback removed 6; `max_bytes` ignored 16; the "only applies with --nagios" refusal removed 9, an
explicit default allowed 4; `0` kept as a limit 5, the words `off`/`unlimited` dropped 10, negative
accepted 7, nan accepted 8, inf accepted 5; the two limits swapped 8, `per_call` not handed over 6,
`_run` dropping both 40, the defaults 60 -> 90 9, 10 -> 20 5, 60 -> 61 18; a warning not making WARN
13, not in the reason 6, kept on a failing record 7, applied to a failing OK record 2, not flattened
5, no JSON key 22, a record without an attempt warned 1; the time-limit help paragraph 4; copies
logged as ERROR during check 10, cap 20 -> 21 4, not reported 20; an exec per child 3; the
`(time limit)` entry CRIT 4, silent 4, deadline not applied 4, per-call limit not applied 1; a failed
tree listing / property batch not CRIT for the children 2 / 3. One survivor, equivalent: the
`max(..., 0)` around the status-line clip in `render_nagios` (`_fit_bytes` clamps itself).
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
        def interrupt(config, **kw):
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
        def boom(config, **kw):
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
        def boom(config, **kw):
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


# --------------------------------------------------------------------------
# 9. the `--nagios` output cap (4000 bytes for the whole output)
# --------------------------------------------------------------------------

#: One perfdata item as `render_nagios` writes it: a single-quoted label (a
#: quote doubled) and `age s;warn;crit;0` (lag) or `count;warn;crit;0` (outcome).
PERF_ITEM = r"'(?:[^']|'')*'=\d+s?;\d*;\d*;0"
OMITTED = r"omitted=(\d+)"
LIMIT = check_view.NAGIOS_OUTPUT_LIMIT


def lag(index, state=health.STATE_OK, *, subject=None, reason=None, peer="backup"):
    """An anchor `LagEntry` for `tank/vm.<index>` with thresholds 7200/14400 s."""
    return health.LagEntry(
        "anchor", subject if subject is not None else f"tank/vm.{index:04d}", state,
        reason if reason is not None else ("late" if state != health.STATE_OK else "fine"),
        peer=peer, age_seconds=3600.0 + index, expected_seconds=3600.0,
        warn_seconds=7200.0, crit_seconds=14400.0,
    )


def item_of(entry) -> str:
    """The perfdata item of a `lag()` entry, written independently of the code under test."""
    return f"'anchor:{entry.subject}@{entry.peer}'={entry.age_seconds:.0f}s;7200;14400;0"


def nbytes(text: str) -> int:
    return len(text.encode("utf-8"))


def render(entries) -> str:
    return check_view.render_nagios(health, entries)


def split_nagios(out: str):
    """-> (status line, perfdata items, N of `omitted=N` or None). Asserts the
    perfdata section is whole items (the grammar) and nothing else."""
    assert out.endswith("\n") and out.count("\n") == 1, repr(out[-80:])
    status, sep, perf = out[:-1].partition(" | ")
    if not sep:
        return status, [], None
    tail = re.search(rf"(?:^| ){OMITTED}$", perf)
    body = perf[:tail.start()] if tail else perf
    items = re.findall(PERF_ITEM, body)
    assert " ".join(items) == body, f"not whole items: {body[-120:]!r}"
    return status, items, int(tail.group(1)) if tail else None


class TestNagiosCap:
    @pytest.mark.parametrize("count", [300, 3000])
    def test_the_whole_output_stays_within_4000_bytes(self, count):
        entries = [lag(i) for i in range(count)]
        out = render(entries)
        assert LIMIT == 4000
        assert nbytes(out) <= 4000
        assert out.count("\n") == 1 and out.endswith("\n")
        status, items, omitted = split_nagios(out)
        assert status == f"ZFSBACKUP OK - {count} checks OK"
        assert omitted == count - len(items) and 0 < len(items) < count
        # the cap is used, not undershot: the next item would not have fitted
        assert nbytes(out) + nbytes(item_of(entries[len(items)])) + 1 > 4000 + 10

    @pytest.mark.parametrize("count", [300, 3000])
    def test_every_state_mix_stays_within_the_cap(self, count):
        entries = [lag(i, (health.STATE_CRIT, health.STATE_WARN, health.STATE_OK)[i % 3])
                   for i in range(count)]
        out = render(entries)
        assert nbytes(out) <= 4000
        status, items, omitted = split_nagios(out)
        assert status.startswith("ZFSBACKUP CRIT - ") and "and " in status
        assert omitted == count - len(items)

    def test_a_small_set_is_untouched(self):
        entries = [lag(i) for i in range(5)]
        out = render(entries)
        assert out == (
            "ZFSBACKUP OK - 5 checks OK | " + " ".join(item_of(e) for e in entries) + "\n")
        assert split_nagios(out)[2] is None

    def test_items_are_whole_and_omitted_is_the_last_token(self):
        out = render([lag(i) for i in range(500)])
        status, items, omitted = split_nagios(out)
        assert out.rstrip("\n").split(" ")[-1] == f"omitted={omitted}"
        assert items == [item_of(lag(i)) for i in range(len(items))]       # no item cut or reordered
        assert out.count("omitted=") == 1

    def test_kept_items_are_chosen_by_severity_but_emitted_in_the_entries_order(self):
        entries = [lag(i) for i in range(3000)]
        crit, warn = [2999, 100, 2000], [2500, 7, 1500]
        for i in crit:
            entries[i] = lag(i, health.STATE_CRIT)
        for i in warn:
            entries[i] = lag(i, health.STATE_WARN)
        out = render(entries)
        status, items, omitted = split_nagios(out)
        failing = sorted(crit + warn)
        # the set: every failing item, then OK ones from the front of the entries
        oks = len(items) - len(failing)
        assert 0 < oks < 3000 - len(failing)
        kept = sorted(failing + [i for i in range(3000) if i not in failing][:oks])
        # the order: the entries' own, NOT severity-first (the CRIT 2999 is last, WARN 7 is inside the run)
        assert items == [item_of(lag(i)) for i in kept]
        assert items[-1] == item_of(lag(2999)) and kept.index(7) == 7
        # the status line is the verdict's, in the entries' own order (not re-sorted)
        assert status.startswith("ZFSBACKUP CRIT - WARN anchor: tank/vm.0007 (backup): late; "
                                 "CRIT anchor: tank/vm.0100 (backup): late; ")
        assert status.endswith("; and 3 more")
        assert omitted == 3000 - len(items)
        assert out.rstrip("\n").split(" ")[-1] == f"omitted={omitted}"       # omitted is still the last item

    def test_a_truncated_output_of_ok_entries_is_the_all_fit_prefix(self):
        entries = [lag(i) for i in range(600)]
        _, items, omitted = split_nagios(render(entries))
        assert items == [item_of(lag(i)) for i in range(len(items))] and omitted == 600 - len(items)

    def test_when_everything_fits_the_order_is_the_entries_with_a_warn_and_crit_in_the_middle(self):
        entries = [lag(0), lag(1, health.STATE_WARN), lag(2), lag(3, health.STATE_CRIT), lag(4)]
        out = render(entries)
        assert out.split(" | ")[1] == " ".join(item_of(e) for e in entries) + "\n"     # byte for byte
        assert split_nagios(out)[2] is None

    def test_a_toggling_warn_does_not_move_the_other_items_when_everything_fits(self):
        base = [lag(i) for i in range(6)]
        outs = []
        for state in (health.STATE_OK, health.STATE_WARN, health.STATE_CRIT):
            entries = list(base)
            entries[2] = lag(2, state)
            outs.append(render(entries).split(" | ")[1])
        assert outs[0] == outs[1] == outs[2] == " ".join(item_of(e) for e in base) + "\n"

    def test_a_toggling_warn_keeps_the_kept_items_in_place_when_truncated(self):
        # the failing item joins the kept set but is emitted at ITS position, never first
        def kept(state):
            entries = [lag(i) for i in range(700)]
            entries[650] = lag(650, state)
            return split_nagios(render(entries))[1]
        quiet, loud = kept(health.STATE_OK), kept(health.STATE_WARN)
        assert item_of(lag(650)) not in quiet and loud[-1] == item_of(lag(650))
        assert loud[:-1] == quiet[:len(loud) - 1]                       # the others did not move

    def test_the_order_is_the_entries_not_alphabetical(self):
        names = ["tank/zeta", "tank/alpha", "tank/mid"]
        entries = [lag(i) for i in range(400)]
        for name in names:
            entries.append(lag(0, health.STATE_CRIT, subject=name))
        _, items, _ = split_nagios(render(entries))
        assert items[-3:] == [item_of(lag(0, subject=n)) for n in names]
        assert items[0] == item_of(lag(0))

    def test_the_loop_stops_at_the_first_item_that_does_not_fit(self):
        # three short WARN items (the status line names these), then two long
        # ones, the second too long for what is left; the small OK ones after
        # them would fit, but items are never taken out of order
        short = [lag(i, health.STATE_WARN) for i in range(3)]
        big = [lag(3, health.STATE_WARN, subject="b" * 1800),
               lag(4, health.STATE_WARN, subject="c" * 2500)]
        out = render(short + big + [lag(i) for i in range(5, 40)])
        status, items, omitted = split_nagios(out)
        assert items == [item_of(e) for e in short + big[:1]]
        assert omitted == 36 and nbytes(out) <= 4000

    def test_a_late_warn_beats_an_early_ok_when_only_a_few_fit(self):
        entries = [lag(i) for i in range(3000)] + [lag(3000, health.STATE_WARN)]
        _, items, omitted = split_nagios(render(entries))
        assert items[-1] == item_of(lag(3000)) and omitted > 0           # kept, at its own position (last)
        assert items[0] == item_of(lag(0))

    def test_mixed_lag_and_outcome_entries(self):
        outcome = [
            check_view.OutcomeEntry(
                "worker_send", f"tank/o{i}", health.STATE_CRIT if i == 120 else health.STATE_OK,
                "send", peer="offsite", kind="send", consecutive_failures=5 if i == 120 else 0,
                warn_failures=2.0, crit_failures=4.0)
            for i in range(150)
        ]
        daemon = [health.CheckEntry("daemon", f"d{i}", health.STATE_OK, "fine") for i in range(10)]
        entries = [lag(i) for i in range(150)] + outcome + daemon       # 300 perfdata items
        out = render(entries)
        assert nbytes(out) <= 4000
        status, items, omitted = split_nagios(out)
        assert items[0] == item_of(lag(0))                                    # the entries' order ...
        assert items[-1] == "'worker_send:tank/o120@offsite'=5;2;4;0"         # ... the CRIT outcome kept, last
        assert items[:-1] == [item_of(lag(i)) for i in range(len(items) - 1)]
        assert omitted == 300 - len(items)                                    # daemon entries have no perfdata
        assert status.startswith("ZFSBACKUP CRIT - CRIT worker_send: tank/o120 (offsite)")

    def test_an_entry_without_perfdata_never_counts_as_omitted(self):
        entries = [health.CheckEntry("daemon", f"d{i}", health.STATE_OK, "fine") for i in range(400)]
        assert render(entries) == "ZFSBACKUP OK - 400 checks OK\n"

    def test_non_ascii_labels_are_counted_in_bytes(self):
        entries = [lag(i, subject=f"tank/é漢字{i:04d}") for i in range(300)]
        out = render(entries)
        assert nbytes(out) <= 4000
        assert len(out) < nbytes(out) - 300                  # a character count would have admitted more
        assert nbytes(out) > 4000 - 120                      # ... and the byte budget is nearly used
        _, items, omitted = split_nagios(out)
        assert omitted == 300 - len(items)
        out.encode("utf-8").decode("utf-8")

    @pytest.mark.parametrize("pad", [0, 1, 2, 3])
    def test_a_huge_status_line_is_clipped_with_omitted_still_present(self, pad):
        # 3-byte characters at every alignment: the cut lands inside one for some `pad`
        reason = "a" * pad + "漢" * 5000
        entries = [lag(0, health.STATE_CRIT, reason=reason)] + [lag(i) for i in range(1, 51)]
        out = render(entries)
        assert nbytes(out) <= 4000
        status, items, omitted = split_nagios(out)
        assert status.endswith("...") and "�" not in out
        assert status.startswith("ZFSBACKUP CRIT - CRIT anchor: tank/vm.0000 (backup): ")
        full = f"ZFSBACKUP CRIT - CRIT anchor: tank/vm.0000 (backup): {reason}"
        assert full.startswith(status[:-3])                               # a prefix, cut on a character
        assert omitted == 51 - len(items) and omitted >= 1
        assert out.rstrip("\n").split(" ")[-1] == f"omitted={omitted}"

    def test_a_huge_status_line_without_perfdata_is_clipped_too(self):
        entries = [health.CheckEntry("daemon", "process", health.STATE_CRIT, "é" * 9000)]
        out = render(entries)
        assert nbytes(out) <= 4000 and out.endswith("...\n") and out.count("\n") == 1
        assert " | " not in out and "omitted" not in out

    def test_a_long_status_line_that_still_fits_is_not_clipped(self):
        reason = "x" * 1500
        entries = [lag(0, health.STATE_CRIT, reason=reason)] + [lag(i) for i in range(1, 300)]
        out = render(entries)
        status, items, omitted = split_nagios(out)
        assert reason in status and "..." not in status
        assert nbytes(out) <= 4000 and omitted == 300 - len(items)

    def test_the_exact_fit_boundary(self):
        # Find how many OK items still fit with no `omitted=`, then pad the last
        # label so the output is exactly 4000 bytes: still whole, nothing omitted.
        for count in range(1, 200):                # bounded: it must stop well before 200
            if split_nagios(render([lag(i) for i in range(count + 1)]))[2] is not None:
                break
        else:
            pytest.fail("never reached the cap")
        entries = [lag(i) for i in range(count)]
        assert split_nagios(render(entries))[2] is None
        slack = 4000 - nbytes(render(entries))
        assert 0 <= slack < 80
        entries[-1] = lag(count - 1, subject=entries[-1].subject + "p" * slack)
        out = render(entries)
        assert nbytes(out) == 4000                                 # the boundary, inclusive
        status, items, omitted = split_nagios(out)
        assert omitted is None and len(items) == count
        # one more byte: the last item no longer fits and is reported, not cut
        entries[-1] = lag(count - 1, subject=entries[-1].subject + "p")
        out = render(entries)
        status, items, omitted = split_nagios(out)
        assert nbytes(out) <= 4000 and omitted == 1 and len(items) == count - 1
        assert items == [item_of(lag(i)) for i in range(count - 1)]

    def test_the_status_line_and_state_do_not_depend_on_the_cap(self):
        entries = [lag(i, health.STATE_CRIT if i % 2 else health.STATE_OK) for i in range(1000)]
        out = render(entries)
        first = ("ZFSBACKUP CRIT - CRIT anchor: tank/vm.0001 (backup): late; "
                 "CRIT anchor: tank/vm.0003 (backup): late; CRIT anchor: tank/vm.0005 (backup): late; "
                 "and 497 more | ")
        assert out.startswith(first)

    def test_json_and_prometheus_are_not_capped(self, monkeypatch, healthy):
        entries = [lag(i) for i in range(300)]
        monkeypatch.setattr(check_view, "gather", lambda *a, **k: (health, entries))
        data = json.loads(healthy.check("--json").stdout)
        assert len(data["entries"]) == 300
        text = healthy.check("--prometheus").stdout
        assert nbytes(text) > 4000
        assert text.count("zfsbackup_anchor_check_state{") == 300

    def test_the_exit_code_is_the_verdict_not_the_cap(self, monkeypatch, healthy):
        entries = [lag(0, health.STATE_CRIT)] + [lag(i) for i in range(1, 3000)]
        monkeypatch.setattr(check_view, "gather", lambda *a, **k: (health, entries))
        result = healthy.check("--nagios")
        assert result.exit_code == 2
        assert nbytes(result.stdout) <= 4000 and result.stdout.count("\n") == 1
        assert result.stdout.startswith("ZFSBACKUP CRIT - CRIT anchor: tank/vm.0000")
        assert split_nagios(result.stdout)[1][0] == item_of(lag(0))
        entries[0] = lag(0, health.STATE_WARN)
        assert healthy.check("--nagios").exit_code == 1
        entries[0] = lag(0)
        assert healthy.check("--nagios").exit_code == 0

    def test_the_help_texts_state_the_cap(self, healthy):
        text = " ".join(healthy.run("check", "--help").stdout.split())
        assert "The whole output is capped (see --nagios-max-bytes)" in text
        assert ("when the perfdata does not fit, failing entries are kept first, then OK ones, "
                "and the last item is omitted=N; the kept items stay in their usual order") in text
        assert "at most --nagios-max-bytes bytes in all, 4000 by default" in text
        assert ("when the perfdata does not fit, the most severe items are kept, in their usual order, "
                "and the last item is omitted=N") in text
        assert "--json and --prometheus list every entry" in text
        doc = " ".join(check_view.render_nagios.__doc__.split())
        assert "4000" in doc and "omitted=N" in doc and "original order" in doc


# --------------------------------------------------------------------------
# 10. A3: `check` grades the expanded descendants of a recursive dataset
# --------------------------------------------------------------------------

#: A fake `zfs` of this section's own (not `_fake_zfs_check.py`, which other test
#: files share): the datasets of a tree, their snapshots and properties, and the
#: reads `check` makes over a recursive dataset, answered the way zfs prints them:
#: the name-only recursive listing, the value-free backup-copy marker read (local,
#: received or `inherited from <ancestor>`), and the BATCH reads of one parent's
#: descendants (the recursive snapshot listing; phase 1 `all` with `-s`, no value
#: column; phase 2 `name,type,<props>`) plus the one-property reads of a parent's
#: own anchor and pin. Faults, per parent: `fail_names` (the name-only listing),
#: `fail_markers`, `fail_tree`, `fail_props1`, `fail_rows`; `fail_get` (a
#: one-property read, `dataset` or `dataset::property`); `slow` is a list of
#: `[substring of the argv, seconds]` (a hung pool: sleep first).
RECURSIVE_FAKE = r'''
import json, os, sys, time
args = sys.argv[1:]
with open(os.environ["FAKECHECK_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
with open(os.environ["FAKECHECK_SPEC"]) as handle:
    spec = json.load(handle)
tree = spec["datasets"]
line = " ".join(args)
for needle, seconds in spec.get("slow", []):
    if needle in line:
        time.sleep(seconds)


def flag(name):
    return args[args.index(name) + 1] if name in args else None


def under(root, recursive):
    return [n for n in tree if n == root or (recursive and n.startswith(root + "/"))]


def refuse(key, root):
    if root in spec.get(key, []):
        sys.stderr.write(f"cannot open '{root}': pool I/O is currently suspended\n")
        sys.exit(1)


def marker_source(name, prop):
    markers = spec.get("markers", {})
    if prop in markers.get(name, {}):
        return markers[name][prop]
    parent = name.rpartition("/")[0]
    while parent:
        if prop in markers.get(parent, {}):
            return f"inherited from {parent}"
        parent = parent.rpartition("/")[0]
    return None


if args[:1] == ["list"] and flag("-o") == "name,type":
    root = args[-1]
    if "-r" in args:
        refuse("fail_names", root)
    for name in under(root, "-r" in args):
        print(f"{name}\t{tree[name]}")
    sys.exit(0)
if args[:3] == ["get", "-H", "-o"] and flag("-o") == "name,property,source":
    root, wanted = args[-1], flag("-s").split(",")
    if args[-2] == "all":
        # phase 1 of the descendants' property read: every local property, no value column
        refuse("fail_props1", root)
        for name in under(root, True):
            for prop in spec["props"].get(name, {}):
                if "local" in wanted:
                    print(f"{name}\t{prop}\tlocal")
        sys.exit(0)
    refuse("fail_markers", root)                       # the backup-copy marker read
    for name in under(root, True):
        for prop in args[-2].split(","):
            source = marker_source(name, prop)
            if source is not None and source.split(" ")[0] in wanted:
                print(f"{name}\t{prop}\t{source}")
    sys.exit(0)
if args[:3] == ["get", "-H", "-o"] and flag("-o") == "name,property,value,source":
    # phase 2: `name`, `type`, then each requested property in order, `-` when unset
    root, requested = args[-1], args[-2].split(",")
    refuse("fail_rows", root)
    for name in under(root, True):
        print(f"{name}\tname\t{name}\t-")
        print(f"{name}\ttype\t{tree[name]}\t-")
        for prop in requested[2:]:
            value = spec["props"].get(name, {}).get(prop)
            print(f"{name}\t{prop}\t{value}\tlocal" if value is not None else f"{name}\t{prop}\t-\t-")
    sys.exit(0)
if args[:4] == ["list", "-H", "-r", "-t"] and flag("-t") == "snapshot":
    # the batch listing of one recursive parent's whole tree: one exec per parent
    root = args[-1]
    refuse("fail_tree", root)
    for number, dataset in enumerate(under(root, True)):
        for name, txg in spec["snapshots"].get(dataset, []):
            print("\t".join([f"{dataset}@{name}", "snapshot", str(txg), str(1000 + txg + 10000 * number),
                             "0", "-", "off", "none"]))
    sys.exit(0)
if args[:4] == ["list", "-H", "-t", "snapshot"]:
    dataset = args[-1]
    for name, txg in spec["snapshots"].get(dataset, []):
        print("\t".join([f"{dataset}@{name}", "snapshot", str(txg), str(1000 + txg), "0", "-", "off", "none"]))
    sys.exit(0)
if args[:4] == ["get", "-H", "-o", "value"]:
    prop, dataset = args[-2], args[-1]
    if dataset in spec.get("fail_get", []) or f"{dataset}::{prop}" in spec.get("fail_get", []):
        sys.stderr.write(f"cannot open '{dataset}': pool I/O is currently suspended\n")
        sys.exit(1)
    print(spec["props"].get(dataset, {}).get(prop, "-"))
    sys.exit(0)
sys.stderr.write(f"fake zfs (recursive check): unsupported command {args}\n")
sys.exit(2)
'''

RECURSIVE_YAML = """\
snapshot_prefix: "autosnap"
check_interval: "5m"
datasets:
  - name: "tank/rec"
    frequency: "1h"
    recursive: true
    retention:
      "1d": "30d"
    remote:
      - destination: "offsite"
destinations:
  offsite:
    url: "https://offsite:8080"
"""

CHILDREN = ("tank/rec/a", "tank/rec/b", "tank/rec/b/c")


TWO_PARENTS_YAML = RECURSIVE_YAML.replace(
    '''destinations:''',
    '''  - name: "tank/two"
    frequency: "1h"
    recursive: true
    retention:
      "1d": "30d"
    remote:
      - destination: "offsite"
destinations:''')


def install_recursive_fake(zfs, tmp_path):
    """Point this test's `zfs` wrapper at `RECURSIVE_FAKE` (same spec file and log)."""
    script = tmp_path / "recursive-fake-zfs.py"
    script.write_text(RECURSIVE_FAKE)
    (tmp_path / "zfs-check").write_text(
        f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(script))} \"$@\"\n")


@pytest.fixture
def rec_world(world, zfs, tmp_path):
    """A recursive `tank/rec` with the descendants a, b, b/c, every dataset
    snapshotted 10 minutes ago and anchored on it (all OK), behind a fake
    `zfs` that has the A3 discovery calls."""
    install_recursive_fake(zfs, tmp_path)
    zfs.spec.update(datasets={n: "filesystem" for n in ("tank/rec", *CHILDREN)}, markers={},
                    fail_names=[], fail_markers=[], fail_tree=[], fail_props1=[], fail_rows=[],
                    fail_get=[], slow=[])
    world.build(RECURSIVE_YAML, snapshot_age=600)
    zfs.save()
    world.spec = zfs.spec
    world.anchor = lambda dataset, name: zfs.prop(dataset, ANCHOR_PROP, name)
    names = {}
    for dataset in ("tank/rec", *CHILDREN):
        names[dataset] = zfs.snapshots(dataset, 600, 600 + 3600)
        world.anchor(dataset, names[dataset][0])
    world.names = names

    def mark(dataset, source="local", prop="org.zfsbackup:client_id"):
        """`dataset` carries a backup-copy marker (a copy of another host's data)."""
        zfs.spec["markers"].setdefault(dataset, {})[prop] = source
        zfs.save()

    def add_child(dataset, *ages):
        """A new descendant of tank/rec with snapshots `ages` old, anchored on the newest."""
        zfs.spec["datasets"][dataset] = "filesystem"
        snaps = zfs.snapshots(dataset, *(ages or (600, 600 + 3600)))
        world.anchor(dataset, snaps[0])
        return snaps

    world.mark, world.add_child = mark, add_child
    return world


def rec_entries(world, *flags):
    return {(e["source"], e["subject"], e.get("peer")): e
            for e in json.loads(world.check("--json", *flags).stdout)["entries"]}


class TestRecursiveDescendants:
    def test_a_child_has_an_anchor_entry_and_no_snapshot_entry(self, rec_world):
        result = rec_world.check("--json")
        assert result.exit_code == 0, (result.stdout, result.stderr)
        entries = rec_entries(rec_world)
        for child in CHILDREN:
            entry = entries[("anchor", child, "offsite")]
            assert entry["state"] == "OK" and entry["age_seconds"] == pytest.approx(600, abs=30)
            # a child's expectation is the parent's (1h): thresholds 2h / 4h
            assert (entry["warn_seconds"], entry["crit_seconds"]) == (7200, 14400)
            assert ("snapshot", child, None) not in entries     # the parent's covers the `-r` snapshot
        assert ("snapshot", "tank/rec", None) in entries
        assert ("anchor", "tank/rec", "offsite") in entries
        assert not [k for k in entries if k[0] == "descendants"]

    def test_a_stale_child_anchor_is_graded_on_its_own_dataset_and_destination(self, rec_world):
        # b is 3 h behind (WARN: between 2 h and 4 h), b/c 5 h behind (CRIT)
        for dataset, hours in (("tank/rec/b", 3), ("tank/rec/b/c", 5)):
            late = rec_world.zfs.snapshots(dataset, hours * 3600, hours * 3600 + 3600)
            rec_world.anchor(dataset, late[0])
        result = rec_world.check("--json")
        assert result.exit_code == 1
        entries = rec_entries(rec_world)
        assert entries[("anchor", "tank/rec/b", "offsite")]["state"] == "WARN"
        assert entries[("anchor", "tank/rec/b/c", "offsite")]["state"] == "CRIT"
        assert "5h" in entries[("anchor", "tank/rec/b/c", "offsite")]["reason"]
        assert entries[("anchor", "tank/rec/a", "offsite")]["state"] == "OK"
        assert entries[("anchor", "tank/rec", "offsite")]["state"] == "OK"
        assert json.loads(result.stdout)["state"] == "CRIT"

    def test_prometheus_labels_a_child_by_dataset_and_destination(self, rec_world):
        late = rec_world.zfs.snapshots("tank/rec/b", 3 * 3600, 4 * 3600)
        rec_world.anchor("tank/rec/b", late[0])
        result = rec_world.check("--prometheus")
        assert result.exit_code == 1
        parsed = Exposition(result.stdout)
        for child, state in (("tank/rec/a", 0), ("tank/rec/b", 1), ("tank/rec/b/c", 0)):
            assert parsed.one("zfsbackup_anchor_check_state", dataset=child, destination="offsite") == state
        assert parsed.one("zfsbackup_anchor_age_seconds", dataset="tank/rec/b",
                          destination="offsite") == pytest.approx(3 * 3600, abs=30)
        assert not [s for s in parsed.samples if s[0] == "zfsbackup_snapshot_check_state"
                    and s[1].get("dataset", "").startswith("tank/rec/")]

    def test_nagios_carries_a_perfdata_item_per_child(self, rec_world):
        late = rec_world.zfs.snapshots("tank/rec/b/c", 5 * 3600, 6 * 3600)
        rec_world.anchor("tank/rec/b/c", late[0])
        result = rec_world.check("--nagios")
        assert result.exit_code == 2
        status, items, omitted = split_nagios(result.stdout)
        assert status.startswith("ZFSBACKUP CRIT - CRIT anchor: tank/rec/b/c (offsite): ")
        assert omitted is None
        labels = [i.split("'=")[0] + "'" for i in items]
        # everything fits: the entries' own order (parent, then each child as listed), the
        # failing child NOT moved to the front
        assert labels == [
            "'snapshot:tank/rec'", "'anchor:tank/rec@offsite'",
            "'anchor:tank/rec/a@offsite'", "'anchor:tank/rec/b@offsite'",
            "'anchor:tank/rec/b/c@offsite'",
        ]
        assert items[-1].startswith("'anchor:tank/rec/b/c@offsite'=") and ";7200;14400;0" in items[-1]

    def test_a_failed_discovery_is_one_warn_entry_never_silence(self, rec_world):
        rec_world.spec["fail_names"].append("tank/rec")
        rec_world.zfs.save()
        result = rec_world.check("--json")
        assert result.exit_code == 1
        entries = rec_entries(rec_world)
        entry = entries[("descendants", "tank/rec", None)]
        assert entry["state"] == "WARN"
        assert "could not be listed" in entry["reason"] and "their copies are not checked" in entry["reason"]
        assert "pool I/O is currently suspended" in entry["reason"]
        assert not [k for k in entries if k[1].startswith("tank/rec/")]     # no child is invented
        # the parent itself is still graded
        assert entries[("snapshot", "tank/rec", None)]["state"] == "OK"
        assert entries[("anchor", "tank/rec", "offsite")]["state"] == "OK"
        assert json.loads(result.stdout)["state"] == "WARN"

    def test_a_failed_discovery_in_the_other_formats(self, rec_world):
        rec_world.spec["fail_names"].append("tank/rec")
        rec_world.zfs.save()
        text = rec_world.check()
        assert text.exit_code == 1
        assert re.search(r"^WARN descendants: tank/rec: the descendants of tank/rec could not be listed",
                         text.stdout, re.M)
        prom = Exposition(rec_world.check("--prometheus").stdout)
        assert prom.one("zfsbackup_check_state", source="descendants", subject="tank/rec") == 1
        nagios = rec_world.check("--nagios")
        assert nagios.exit_code == 1
        assert nagios.stdout.startswith(
            "ZFSBACKUP WARN - WARN descendants: tank/rec: the descendants of tank/rec could not be listed")

    def test_the_discovery_error_goes_to_stderr_and_never_into_machine_output(self, rec_world):
        rec_world.spec["fail_names"].append("tank/rec")
        rec_world.zfs.save()
        for flag in ("--nagios", "--json", "--prometheus"):
            result = rec_world.check(flag)
            assert result.exit_code == 1, flag
            assert "Cannot list the descendants of tank/rec" in result.stderr, (flag, result.stderr)
            assert "Cannot list the descendants" not in result.stdout
            assert "ERROR" not in result.stdout and "error" not in result.stdout.split("|")[0].lower()
        # stdout stays a pure document
        assert json.loads(rec_world.check("--json").stdout)["schema"] == 1
        Exposition(rec_world.check("--prometheus").stdout)
        assert rec_world.check("--nagios").stdout.count("\n") == 1

    @pytest.mark.parametrize("fault", ["fail_tree", "fail_props1", "fail_rows"])
    def test_a_failed_batch_makes_every_child_of_the_parent_crit(self, rec_world, fault):
        # the descendants are read in batches: a batch that cannot be read is the error of
        # EVERY child it was to answer, never "not sent yet" and never a smaller answer
        rec_world.spec[fault].append("tank/rec")
        rec_world.zfs.save()
        result = rec_world.check("--json")
        assert result.exit_code == 1
        entries = rec_entries(rec_world)
        for child in CHILDREN:
            entry = entries[("anchor", child, "offsite")]
            assert entry["state"] == "CRIT" and entry["reason"].startswith("cannot read: "), (child, entry)
            assert "pool I/O is currently suspended" in entry["reason"]
            assert "not yet sent" not in entry["reason"]
        # the parent's own reads are separate execs and stand on their own
        assert entries[("snapshot", "tank/rec", None)]["state"] == "OK"
        assert entries[("anchor", "tank/rec", "offsite")]["state"] == "OK"
        assert json.loads(result.stdout)["state"] == "CRIT"

    def test_a_failed_property_batch_says_the_anchor_could_not_be_read(self, rec_world):
        rec_world.spec["fail_rows"].append("tank/rec")
        rec_world.zfs.save()
        entry = rec_entries(rec_world)[("anchor", "tank/rec/a", "offsite")]
        assert "the anchor could not be read" in entry["reason"]

    def test_the_other_parents_batches_are_unaffected(self, tmp_path, world, zfs):
        install_recursive_fake(zfs, tmp_path)
        zfs.spec.update(datasets={n: "filesystem" for n in ("tank/rec", "tank/rec/a", "tank/two", "tank/two/x")},
                        markers={}, fail_names=[], fail_markers=[], fail_tree=["tank/two"],
                        fail_props1=[], fail_rows=[], fail_get=[], slow=[])
        world.build(TWO_PARENTS_YAML, snapshot_age=600)
        for dataset in ("tank/rec", "tank/rec/a", "tank/two", "tank/two/x"):
            zfs.prop(dataset, ANCHOR_PROP, zfs.snapshots(dataset, 600, 4200)[0])
        entries = rec_entries(world)
        assert entries[("anchor", "tank/rec/a", "offsite")]["state"] == "OK"
        assert entries[("anchor", "tank/two/x", "offsite")]["state"] == "CRIT"

    def test_an_unreadable_own_anchor_of_the_parent_is_crit_and_the_children_are_unaffected(self, rec_world):
        rec_world.spec["fail_get"].append(f"tank/rec::{ANCHOR_PROP}")
        rec_world.zfs.save()
        entries = rec_entries(rec_world)
        entry = entries[("anchor", "tank/rec", "offsite")]
        assert entry["state"] == "CRIT" and entry["reason"].startswith("cannot read: ")
        for child in CHILDREN:
            assert entries[("anchor", child, "offsite")]["state"] == "OK"

    def test_an_unreadable_own_pin_of_the_parent_does_not_hide_its_anchor(self, rec_world):
        rec_world.spec["fail_get"].append("tank/rec::org.zfsbackup:inflight.offsite")
        rec_world.zfs.save()
        entry = rec_entries(rec_world)[("anchor", "tank/rec", "offsite")]
        assert entry["state"] == "OK" and entry["age_seconds"] == pytest.approx(600, abs=30)
        assert "send pin could not be read" in entry["reason"]

    def test_a_child_carrying_a_copy_marker_is_not_graded_but_is_reported(self, rec_world):
        # a received copy below the parent is not a source: no anchor entry for it or for
        # anything below it, no send; but the operator is told, once, as a WARN `copies` entry
        rec_world.mark("tank/rec/b", "received")
        result = rec_world.check("--json")
        assert result.exit_code == 1
        entries = rec_entries(rec_world)
        assert ("anchor", "tank/rec/a", "offsite") in entries
        assert not [k for k in entries if k[0] == "anchor" and k[1] in ("tank/rec/b", "tank/rec/b/c")]
        copies = [e for k, e in entries.items() if k[0] == "copies"]
        assert [(e["subject"], e["state"]) for e in copies] == [("tank/rec/b", "WARN")]   # b/c is not a second root
        reason = copies[0]["reason"]
        assert "backup copy below the recursive dataset tank/rec" in reason
        assert "not snapshotted, sent or pruned with the tree" in reason
        assert "zfs rename tank/rec/b" in reason
        assert json.loads(result.stdout)["state"] == "WARN"

    def test_check_stays_read_only_over_the_recursive_tree(self, rec_world):
        assert rec_world.check("--json").exit_code == 0
        verbs = {call[0] for call in rec_world.zfs.calls}
        assert verbs <= {"list", "get"}
        # a recursive SNAPSHOT listing is the batch read (one per parent), still a read
        batch = [c for c in rec_world.zfs.calls if c[0] == "list" and "-r" in c and "snapshot" in c]
        assert len(batch) == 1 and batch[0][-1] == "tank/rec"


def patch_gather(monkeypatch, entries):
    """`check` over `entries` without reading anything; -> the list of bound `gather`
    arguments (`time_limit`, `per_call`, ...) of every call."""
    import inspect

    signature = inspect.signature(check_view.gather)
    seen = []

    def fake(*args, **kw):
        bound = signature.bind(*args, **kw)
        bound.apply_defaults()
        seen.append(dict(bound.arguments))
        return health, entries

    monkeypatch.setattr(check_view, "gather", fake)
    return seen


# --------------------------------------------------------------------------
# 11. `--nagios-max-bytes`
# --------------------------------------------------------------------------


class TestNagiosMaxBytes:
    def mixed(self, count=300):
        return [lag(i, (health.STATE_OK, health.STATE_WARN, health.STATE_OK)[i % 3]) for i in range(count)]

    def test_the_constants(self):
        assert check_view.NAGIOS_MIN_OUTPUT_LIMIT == 64
        assert check_view.NAGIOS_OUTPUT_LIMIT == 4000

    @pytest.mark.parametrize("value", ["63", "1", "0", "-5"])
    def test_below_the_minimum_is_unknown_with_nagios(self, healthy, value):
        line = one_unknown_line(healthy.check("--nagios", "--nagios-max-bytes", value))
        assert line.startswith("ZFSBACKUP UNKNOWN - usage: ")
        assert "at least 64" in line and "--nagios-max-bytes" in line

    @pytest.mark.parametrize("value", ["63", "0"])
    def test_below_the_minimum_is_a_usage_error_without_nagios_too(self, healthy, value):
        result = healthy.check("--nagios-max-bytes", value)
        assert result.exit_code == 2
        assert result.stdout == "" and "at least 64" in result.stderr
        assert "Traceback" not in result.stderr

    def test_not_a_number_is_a_usage_error(self, healthy):
        assert healthy.check("--nagios-max-bytes", "lots").exit_code == 2
        line = one_unknown_line(healthy.check("--nagios", "--nagios-max-bytes", "lots"))
        assert "usage" in line.lower()

    @pytest.mark.parametrize("flag", [[], ["--json"], ["--prometheus"]])
    @pytest.mark.parametrize("value", ["64", "1000", "4000"])
    def test_without_nagios_it_is_refused(self, healthy, flag, value):
        # even the default value, when given explicitly: it says what was meant
        before = len(healthy.zfs.calls)
        result = healthy.check(*flag, "--nagios-max-bytes", value)
        assert result.exit_code == 2
        assert result.stdout == ""
        assert "--nagios-max-bytes only applies with --nagios" in result.stderr
        assert len(healthy.zfs.calls) == before                 # refused before any ZFS read

    def test_64_is_accepted(self, healthy):
        result = healthy.check("--nagios", "--nagios-max-bytes", "64")
        assert result.exit_code == 0, result.stderr
        assert nbytes(result.stdout) <= 64 and result.stdout.count("\n") == 1
        # the 5 entries (2 with perfdata) do not fit in 64: the verdict stays, the items go
        assert result.stdout == "ZFSBACKUP OK - 5 checks OK | omitted=2\n"

    def test_a_tiny_limit_clips_the_status_line_and_keeps_omitted(self):
        entries = [lag(0, health.STATE_CRIT, reason="x" * 500)] + [lag(i) for i in range(1, 21)]
        out = check_view.render_nagios(health, entries, max_bytes=64)
        assert nbytes(out) <= 64 and out.endswith("\n") and out.count("\n") == 1
        status, items, omitted = split_nagios(out)
        assert status.startswith("ZFSBACKUP CRIT - CRIT anchor: tank/vm.0000") and status.endswith("...")
        assert items == [] and omitted == 21
        assert out.rstrip("\n").split(" ")[-1] == "omitted=21"
        assert nbytes(out) == 64                                  # the budget is used exactly

    def test_a_tiny_limit_without_perfdata_clips_the_line(self):
        entries = [health.CheckEntry("daemon", "process", health.STATE_CRIT, "é" * 500)]
        out = check_view.render_nagios(health, entries, max_bytes=64)
        assert nbytes(out) <= 64 and out.endswith("...\n") and "omitted" not in out and "�" not in out

    @pytest.mark.parametrize("limit", [1, 10, 20, 30])
    def test_below_what_the_cli_accepts_the_function_still_returns_one_line(self, limit):
        # the CLI refuses < 64; the function is public and must not misbehave: the status line
        # gives way entirely and `omitted=N` stays the last token
        out = check_view.render_nagios(health, [lag(i) for i in range(5)], max_bytes=limit)
        assert out.count("\n") == 1 and out.endswith(" | omitted=5\n") and "..." in out

    @pytest.mark.parametrize("limit", [64, 65, 100, 200, 500, 1000, 4000, 8000, 100_000])
    def test_the_output_never_exceeds_the_limit_and_keeps_whole_items(self, limit):
        entries = self.mixed()
        out = check_view.render_nagios(health, entries, max_bytes=limit)
        assert nbytes(out) <= limit
        status, items, omitted = split_nagios(out)
        assert status.startswith("ZFSBACKUP WARN - ")
        if limit >= 20_000:
            assert omitted is None and items == [item_of(e) for e in entries]
        else:
            assert omitted == 300 - len(items) and omitted > 0
            assert out.rstrip("\n").split(" ")[-1] == f"omitted={omitted}"

    def test_a_larger_limit_keeps_at_least_as_many_items(self):
        entries = self.mixed()
        counts = [len(split_nagios(check_view.render_nagios(health, entries, max_bytes=n))[1])
                  for n in (64, 200, 1000, 4000, 8000)]
        assert counts == sorted(counts) and counts[0] < counts[2] < counts[3] < counts[4]

    def test_1000_caps_through_the_cli(self, monkeypatch, healthy):
        entries = [lag(i) for i in range(300)]
        patch_gather(monkeypatch, entries)
        result = healthy.check("--nagios", "--nagios-max-bytes", "1000")
        assert result.exit_code == 0
        assert nbytes(result.stdout) <= 1000 and nbytes(result.stdout) > 1000 - 60   # the budget is used
        status, items, omitted = split_nagios(result.stdout)
        assert omitted == 300 - len(items) and items == [item_of(lag(i)) for i in range(len(items))]

    def test_the_default_is_4000_and_unchanged(self, monkeypatch, healthy):
        entries = self.mixed(3000)
        patch_gather(monkeypatch, entries)
        implicit = healthy.check("--nagios").stdout
        explicit = healthy.check("--nagios", "--nagios-max-bytes", "4000").stdout
        assert implicit == explicit == check_view.render_nagios(health, entries) == render(entries)
        assert nbytes(implicit) <= 4000 and nbytes(implicit) > 4000 - 60

    def test_a_larger_limit_admits_more_through_the_cli(self, monkeypatch, healthy):
        patch_gather(monkeypatch, self.mixed(3000))
        out = healthy.check("--nagios", "--nagios-max-bytes", "8000").stdout
        assert 4000 < nbytes(out) <= 8000

    def test_the_exact_fit_boundary_at_a_custom_limit(self):
        limit = 1000
        for count in range(1, 200):
            if split_nagios(check_view.render_nagios(
                    health, [lag(i) for i in range(count + 1)], max_bytes=limit))[2] is not None:
                break
        else:
            pytest.fail("never reached the cap")
        entries = [lag(i) for i in range(count)]
        out = check_view.render_nagios(health, entries, max_bytes=limit)
        assert split_nagios(out)[2] is None and nbytes(out) <= limit
        entries[-1] = lag(count - 1, subject=entries[-1].subject + "p" * (limit - nbytes(out)))
        out = check_view.render_nagios(health, entries, max_bytes=limit)
        assert nbytes(out) == limit and split_nagios(out)[2] is None          # inclusive
        entries[-1] = lag(count - 1, subject=entries[-1].subject + "p")
        out = check_view.render_nagios(health, entries, max_bytes=limit)
        assert nbytes(out) <= limit and split_nagios(out)[2] == 1

    def test_the_verdict_and_exit_code_do_not_depend_on_the_limit(self, monkeypatch, healthy):
        entries = [lag(0, health.STATE_CRIT)] + [lag(i) for i in range(1, 500)]
        patch_gather(monkeypatch, entries)
        for limit in ("64", "200", "4000", "100000"):
            result = healthy.check("--nagios", "--nagios-max-bytes", limit)
            assert result.exit_code == 2, limit
            assert result.stdout.startswith("ZFSBACKUP CRIT - CRIT anchor: tank/vm.0000 (ba")
            assert nbytes(result.stdout) <= int(limit)

    def test_the_status_line_is_the_same_whenever_it_fits(self):
        entries = self.mixed(900)
        small = check_view.render_nagios(health, entries, max_bytes=500)
        large = check_view.render_nagios(health, entries, max_bytes=4000)
        assert small.split(" | ")[0] == large.split(" | ")[0]

    def test_the_help_text(self, healthy):
        text = " ".join(healthy.run("check", "--help").stdout.split())
        assert "--nagios-max-bytes N" in text
        assert "[default: 4000]" in text
        assert "at least 64" in text
        assert "NRPE v2 carries only about 1 KiB of output per packet (use 1000 there)" in text
        assert "Nagios Core 4 reads about 8 KiB" in text
        assert "The status line and the exit code are never cut for the perfdata" in text
        doc = " ".join(check_view.render_nagios.__doc__.split())
        assert "--nagios-max-bytes" in doc and "max_bytes" in doc


# --------------------------------------------------------------------------
# 12. `--time-limit` and `--per-call-timeout`
# --------------------------------------------------------------------------

SLOW_PARENT_ANCHOR = f"value -s local {ANCHOR_PROP} tank/rec"     # `get`: the parent's own anchor read
TIME_LIMIT_REASON = "not checked within the time limit (1 s): tank/rec, tank/rec (descendants)"


class TestTimeLimitOptions:
    @pytest.mark.parametrize("option, index", [("--time-limit", 4), ("--per-call-timeout", 5)])
    @pytest.mark.parametrize("text, expected", [
        ("0", None), ("0.0", None), ("none", None), ("None", None), ("NONE", None), ("off", None),
        ("OFF", None), ("unlimited", None), (" off ", None),
        ("5", 5.0), ("0.5", 0.5), ("1e3", 1000.0), ("90", 90.0), ("150.25", 150.25),
    ])
    def test_the_parse_matrix(self, healthy, monkeypatch, option, index, text, expected):
        seen = patch_gather(monkeypatch, [lag(0)])
        result = healthy.check(option, text)
        assert result.exit_code == 0, result.stderr
        key = ("time_limit", "per_call")[index - 4]
        assert seen[0][key] == expected
        assert (seen[0][key] is None) == (expected is None)
        assert seen[0]["per_call" if key == "time_limit" else "time_limit"] == (
            check_view.DEFAULT_PER_CALL_TIMEOUT if key == "time_limit" else check_view.DEFAULT_TIME_LIMIT)

    @pytest.mark.parametrize("option", ["--time-limit", "--per-call-timeout"])
    @pytest.mark.parametrize("text", ["abc", "-1", "nan", "inf", "-inf", "", "5s", "1,5", "-0.5"])
    def test_a_bad_value_is_a_usage_error(self, healthy, monkeypatch, option, text):
        seen = patch_gather(monkeypatch, [lag(0)])
        result = healthy.check(f"{option}={text}")
        assert result.exit_code == 2, (result.stdout, result.stderr)
        assert result.stdout == "" and seen == []                   # nothing ran
        assert "Traceback" not in result.stderr and option in result.stderr
        assert "(use 0 or none for no limit)" in result.stderr
        if text in ("abc", "", "5s", "1,5"):
            assert "is not a number of seconds" in result.stderr
        else:
            assert "must be a positive number of seconds" in result.stderr

    def test_the_messages_name_the_value(self, healthy):
        assert "'abc' is not a number of seconds (use 0 or none for no limit)" in healthy.check(
            "--time-limit", "abc").stderr
        assert "'-1' must be a positive number of seconds (use 0 or none for no limit)" in healthy.check(
            "--per-call-timeout=-1").stderr

    @pytest.mark.parametrize("option", ["--time-limit", "--per-call-timeout"])
    @pytest.mark.parametrize("text", ["abc", "-1", "nan", "inf"])
    def test_a_bad_value_is_unknown_with_nagios(self, healthy, option, text):
        line = one_unknown_line(healthy.check("--nagios", f"{option}={text}"))
        assert line.startswith("ZFSBACKUP UNKNOWN - usage: ") and option in line

    @pytest.mark.parametrize("flag", [[], ["--json"], ["--prometheus"], ["--nagios"]])
    def test_the_options_are_valid_in_every_format(self, healthy, flag):
        result = healthy.check(*flag, "--time-limit", "30", "--per-call-timeout", "3")
        assert result.exit_code == 0, result.stderr

    def test_the_options_are_valid_with_output(self, healthy, tmp_path):
        target = tmp_path / "check.prom"
        assert healthy.check("--prometheus", "--output", target, "--time-limit", "30").exit_code == 0
        assert target.read_text().startswith("# HELP")

    def test_the_defaults(self, healthy, monkeypatch):
        from zfsbackup import health_collect

        assert (check_view.DEFAULT_TIME_LIMIT, check_view.DEFAULT_PER_CALL_TIMEOUT) == (60.0, 10.0)
        assert check_view.DEFAULT_PER_CALL_TIMEOUT == health_collect.DEFAULT_PER_CALL_SECONDS
        seen = patch_gather(monkeypatch, [lag(0)])
        assert healthy.check().exit_code == 0
        assert (seen[0]["time_limit"], seen[0]["per_call"]) == (60.0, 10.0)

    def test_the_two_options_are_independent(self, healthy, monkeypatch):
        seen = patch_gather(monkeypatch, [lag(0)])
        healthy.check("--time-limit", "none", "--per-call-timeout", "2")
        healthy.check("--time-limit", "7", "--per-call-timeout", "none")
        assert [(s["time_limit"], s["per_call"]) for s in seen] == [(None, 2.0), (7.0, None)]

    def test_the_help_texts(self, healthy):
        text = " ".join(healthy.run("check", "--help").stdout.split())
        assert ("Time bound: the ZFS reads stop after --time-limit seconds (default 60) in all, and any "
                "one zfs command after --per-call-timeout seconds (default 10), except the batched reads "
                "of a large tree") in text
        assert ("which may use all the time left of --time-limit; what was not checked in time is "
                "reported as one WARN entry '(time limit)', never as OK.") in text
        assert ("--time-limit may be overshot by a few seconds at most (a timed-out zfs child needs up "
                "to about 6 s to be killed). 0 or none turns a limit off.") in text
        assert "--time-limit SECONDS" in text and "[default: 60]" in text
        assert "--per-call-timeout SECONDS" in text and "[default: 10]" in text
        assert "one WARN entry '(time limit)'" in text
        assert "0 or none: no limit" in text and "0 or none: no per-command limit" in text
        assert "NRPE's -t (command_timeout) must be larger than it" in text
        assert "TimeoutStartSec=150" in text and "2 x the limit + 30 s" in text
        assert "the shipped zfsbackup-check.service assumes the default of 60" in text
        assert "a hung pool then costs this, not the whole --time-limit" in text

    def test_the_shipped_unit_matches_the_default_limit(self):
        unit = (REPO / "packaging" / "systemd" / "zfsbackup-check.service").read_text()
        timeout = re.search(r"^TimeoutStartSec=(\d+)$", unit, re.M)
        assert timeout and int(timeout.group(1)) == 2 * check_view.DEFAULT_TIME_LIMIT + 30


class TestTimeLimitPassedToCollect:
    """`gather` hands both limits to `health_collect.collect` as keyword arguments."""

    @pytest.fixture
    def collected(self, monkeypatch):
        calls = []

        def collect(config, **kw):
            calls.append((config, kw))
            return []

        monkeypatch.setattr("zfsbackup.health_collect.collect", collect)
        return calls

    def test_the_defaults(self, healthy, collected):
        healthy.check("--json")
        (config, kw), = collected
        assert kw == {"deadline": 60.0, "per_call": 10.0}
        assert config.datasets[0].name == "tank/a"

    def test_custom_values(self, healthy, collected):
        healthy.check("--json", "--time-limit", "7", "--per-call-timeout", "2.5")
        assert collected[0][1] == {"deadline": 7.0, "per_call": 2.5}

    @pytest.mark.parametrize("text", ["none", "0", "off"])
    def test_no_limit_is_none(self, healthy, collected, text):
        healthy.check("--json", "--time-limit", text, "--per-call-timeout", text)
        assert collected[0][1] == {"deadline": None, "per_call": None}

    def test_with_nagios_and_prometheus(self, healthy, collected):
        healthy.check("--nagios", "--time-limit", "9")
        healthy.check("--prometheus", "--per-call-timeout", "4")
        assert [kw for _, kw in collected] == [
            {"deadline": 9.0, "per_call": 10.0}, {"deadline": 60.0, "per_call": 4.0}]

    def test_an_invalid_factor_still_fails_before_the_collection(self, healthy, collected):
        assert healthy.check("--warn-factor", "5", "--crit-factor", "1", "--time-limit", "9").exit_code == 2
        assert collected == []


class TestTimeLimitExceeded:
    """A run that outlasts `--time-limit` says so in every format, as one WARN, never as OK."""

    @pytest.fixture
    def slow_world(self, rec_world):
        rec_world.spec["slow"].append([SLOW_PARENT_ANCHOR, 30])       # a hung pool, killed at the limit
        rec_world.zfs.save()
        return rec_world

    def test_json(self, slow_world):
        result = slow_world.check("--json", "--time-limit", "1")
        assert result.exit_code == 1, (result.stdout, result.stderr)
        data = json.loads(result.stdout)
        entries = {(e["source"], e["subject"], e.get("peer")): e for e in data["entries"]}
        entry = entries[("check", "(time limit)", None)]
        assert entry["state"] == "WARN" and entry["reason"] == TIME_LIMIT_REASON
        assert data["state"] == "WARN"
        # what was cut off contributes nothing (no half-answer), the rest is still graded
        assert not [k for k in entries if k[0] in ("snapshot", "anchor")]
        assert entries[("daemon", "process", None)]["state"] == "OK"

    def test_text(self, slow_world):
        result = slow_world.check("--time-limit", "1")
        assert result.exit_code == 1
        assert f"WARN check: (time limit): {TIME_LIMIT_REASON}" in result.stdout
        assert result.stdout.splitlines()[-1].startswith("overall: WARN")

    def test_prometheus(self, slow_world):
        result = slow_world.check("--prometheus", "--time-limit", "1")
        assert result.exit_code == 1
        assert Exposition(result.stdout).one("zfsbackup_check_state", source="check", subject="(time limit)") == 1
        assert Exposition(result.stdout).one("zfsbackup_check_overall_state") == 1

    def test_nagios(self, slow_world):
        result = slow_world.check("--nagios", "--time-limit", "1")
        assert result.exit_code == 1
        assert result.stdout.startswith(f"ZFSBACKUP WARN - WARN check: (time limit): {TIME_LIMIT_REASON}")
        assert result.stdout.count("\n") == 1

    def test_the_limit_ends_the_run_at_about_the_limit(self, slow_world):
        started = time.monotonic()
        result = slow_world.check("--json", "--time-limit", "1")
        assert result.exit_code == 1
        assert time.monotonic() - started < 20                   # not the 30 s the read would have taken

    def test_a_fast_run_has_no_time_limit_entry(self, rec_world):
        entries = rec_entries(rec_world, "--time-limit", "30")
        assert not [k for k in entries if k[0] == "check"]
        assert rec_world.check("--time-limit", "30").exit_code == 0

    def test_the_per_call_timeout_makes_one_unreadable_read_not_a_time_limit(self, slow_world):
        result = slow_world.check("--json", "--per-call-timeout", "1")
        assert result.exit_code == 1
        entries = rec_entries(slow_world, "--per-call-timeout", "1")
        assert not [k for k in entries if k[0] == "check"]       # 60 s were not spent: no limit entry
        entry = entries[("anchor", "tank/rec", "offsite")]
        assert entry["state"] == "CRIT" and entry["reason"].startswith("cannot read: ")
        for child in CHILDREN:
            assert entries[("anchor", child, "offsite")]["state"] == "OK"


# --------------------------------------------------------------------------
# 13. a worker record's `warning`
# --------------------------------------------------------------------------

COPIES_WARNING = "2 backup copies below tank/rec were left out of the snapshot"


def warned(record, text=COPIES_WARNING):
    """`record` (a `rec()` dict) with a `warning`, the way the writer stores it."""
    record["warning"] = text
    return record


def entry_of(world, kind_source, subject="tank/a", peer=None, *args):
    return world.entries(*args)[(kind_source, subject, peer)]


class TestWorkerWarning:
    """`failures == 0` and an attempt and a `warning`: WARN, never CRIT; a failing record keeps its grading."""

    def test_a_successful_attempt_with_a_warning_is_warn(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a"))])
        entry = entry_of(at_rest, "worker_snapshot")
        assert entry["state"] == "WARN"
        assert entry["consecutive_failures"] == 0
        assert re.fullmatch(r"last attempt succeeded \d+m ago, but: " + re.escape(COPIES_WARNING),
                            entry["reason"])
        assert entry["warning"] == COPIES_WARNING
        result = at_rest.check()
        assert result.exit_code == 1
        assert re.search(rf"^WARN worker_snapshot: tank/a: last attempt succeeded \d+m ago, "
                         rf"but: {re.escape(COPIES_WARNING)}$", result.stdout, re.M)
        assert result.stdout.splitlines()[-1].startswith("overall: WARN")

    def test_the_detail_comes_before_the_warning(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a", detail="created autosnap_20260101000000"))])
        entry = entry_of(at_rest, "worker_snapshot")
        assert re.fullmatch(
            r"last attempt succeeded \d+m ago \(created autosnap_20260101000000\), but: "
            + re.escape(COPIES_WARNING), entry["reason"])

    def test_every_worker_kind_can_warn(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a"), "s")])
        write_worker(at_rest.db, PRUNE, [warned(rec("prune", "tank/a"), "p")])
        write_worker(at_rest.db, SEND, [warned(rec("send", "tank/a", "offsite"), "d")])
        entries = worker_entries_of(at_rest)
        assert {k: v["state"] for k, v in entries.items()} == {
            ("worker_snapshot", "tank/a", None): "WARN", ("worker_prune", "tank/a", None): "WARN",
            ("worker_send", "tank/a", "offsite"): "WARN"}

    @pytest.mark.parametrize("factors", [[], ["--warn-factor", "0.1", "--crit-factor", "0.2"],
                                         ["--warn-factor", "1", "--crit-factor", "1"]])
    def test_a_warning_is_never_crit_whatever_the_factors(self, at_rest, factors):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a"))])
        assert entry_of(at_rest, "worker_snapshot", "tank/a", None, *factors)["state"] == "WARN"

    @pytest.mark.parametrize("failures, state", [(1, "OK"), (2, "WARN"), (3, "WARN"), (4, "CRIT"), (9, "CRIT")])
    def test_a_failing_record_keeps_its_failure_grading_and_carries_no_warning(self, at_rest, failures, state):
        write_worker(at_rest.db, SEND, [warned(rec("send", "tank/a", "offsite", failures=failures))])
        entry = entry_of(at_rest, "worker_send", "tank/a", "offsite")
        assert entry["state"] == state, entry
        assert COPIES_WARNING not in entry["reason"] and "but:" not in entry["reason"]
        assert entry["warning"] is None
        assert entry["consecutive_failures"] == failures

    def test_a_record_without_an_attempt_is_not_warned(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a", result=None, attempt=None))])
        entry = entry_of(at_rest, "worker_snapshot")
        assert entry["state"] == "OK" and entry["reason"] == "no attempt recorded yet"
        # (the JSON still carries the stored text: nothing was graded on it)
        assert "but:" not in entry["reason"] and COPIES_WARNING not in entry["reason"]
        assert at_rest.check().exit_code == 0

    @pytest.mark.parametrize("stored", ["", None, 5, ["a"], {"a": 1}])
    def test_an_empty_or_malformed_warning_is_no_warning(self, at_rest, stored):
        record = rec("snapshot", "tank/a")
        record["warning"] = stored
        write_worker(at_rest.db, SNAP, [record])
        entry = entry_of(at_rest, "worker_snapshot")
        assert entry["state"] == "OK" and entry["warning"] is None and "but:" not in entry["reason"]

    def test_a_file_from_an_older_daemon_has_no_warning_key(self, at_rest):
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a")])
        entry = entry_of(at_rest, "worker_snapshot")
        assert entry["state"] == "OK" and "warning" in entry and entry["warning"] is None
        assert at_rest.check().exit_code == 0

    def test_the_next_clean_attempt_clears_it(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a"))])
        assert at_rest.check().exit_code == 1
        write_worker(at_rest.db, SNAP, [rec("snapshot", "tank/a")])
        assert at_rest.check().exit_code == 0

    def test_newlines_and_controls_are_flattened(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a"), "first\nsecond\r\n\tthird \x1b[31mred")])
        text = at_rest.check().stdout
        line = next(l for l in text.splitlines() if l.startswith("WARN worker_snapshot"))
        assert "but: first second third" in line and "\x1b" not in text
        nagios = at_rest.check("--nagios").stdout
        assert nagios.count("\n") == 1 and "but: first second third" in nagios

    def test_the_view_flattens_whitespace_itself(self):
        record = health.OutcomeRecord(
            kind="snapshot", dataset="tank/a", result="ok", last_attempt=time.time() - 60,
            warning="line one\nline  two\t\tthree")
        entry = check_view._outcome_entry(health, record, time.time(), 2.0, 4.0)
        assert entry.state == health.STATE_WARN
        assert entry.reason.endswith(", but: line one line two three") and "\n" not in entry.reason
        assert entry.warning == "line one\nline  two\t\tthree"       # the stored text is kept as is

    def test_the_view_treats_none_and_empty_alike(self):
        for text in (None, ""):
            record = health.OutcomeRecord(kind="snapshot", dataset="tank/a", result="ok",
                                          last_attempt=time.time() - 60, warning=text)
            entry = check_view._outcome_entry(health, record, time.time(), 2.0, 4.0)
            assert entry.state == health.STATE_OK and entry.warning is None
        assert check_view.OutcomeEntry("worker_snapshot", "d", health.STATE_OK, "r").warning is None

    def test_json_has_the_warning_string_or_null(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a"))])
        write_worker(at_rest.db, PRUNE, [rec("prune", "tank/a")])
        write_worker(at_rest.db, SEND, [warned(rec("send", "tank/a", "offsite", failures=2))])
        data = json.loads(at_rest.check("--json").stdout)
        by = {e["source"]: e for e in data["entries"] if e["source"].startswith("worker")}
        assert by["worker_snapshot"]["warning"] == COPIES_WARNING
        assert by["worker_prune"]["warning"] is None and by["worker_send"]["warning"] is None
        others = [e for e in data["entries"] if not e["source"].startswith("worker")]
        assert others and all("warning" not in e for e in others)
        assert data["schema"] == 1                                    # additive

    def test_nagios_is_warn_with_the_perfdata_unchanged(self, at_rest):
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite")])
        clean = at_rest.check("--nagios")
        write_worker(at_rest.db, SEND, [warned(rec("send", "tank/a", "offsite"))])
        loud = at_rest.check("--nagios")
        assert clean.exit_code == 0 and loud.exit_code == 1
        assert loud.stdout.startswith("ZFSBACKUP WARN - WARN worker_send: tank/a (offsite): last attempt succeeded")
        assert "but: " + COPIES_WARNING in loud.stdout
        assert "'worker_send:tank/a@offsite'=0;2;4;0" in loud.stdout
        # the perfdata is the same (the lag items' ages move with the clock: compare them blurred)
        blur = lambda text: re.sub(r"=\d+s;", "=Ns;", text.split(" | ")[1])           # noqa: E731
        assert blur(loud.stdout) == blur(clean.stdout)

    def test_prometheus_has_state_one_and_zero_failures(self, at_rest):
        write_worker(at_rest.db, SEND, [warned(rec("send", "tank/a", "offsite"))])
        parsed = Exposition(at_rest.check("--prometheus").stdout)
        assert parsed.one("zfsbackup_check_state", source="worker_send", subject="tank/a", peer="offsite") == 1
        assert parsed.one("zfsbackup_worker_consecutive_failures", kind="send", dataset="tank/a",
                          destination="offsite") == 0
        assert parsed.one("zfsbackup_check_overall_state") == 1

    def test_a_stale_file_gives_only_the_status_warning(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a"))], pid=dead_pid())
        entries = worker_entries_of(at_rest)
        assert set(entries) == {("worker_status", SNAP, None)}
        assert entries[("worker_status", SNAP, None)]["state"] == "WARN"
        assert "warning" not in entries[("worker_status", SNAP, None)]
        result = at_rest.check()
        assert COPIES_WARNING not in result.stdout and result.exit_code == 1

    def test_a_stopped_daemons_file_is_stale_too(self, world):
        world.build(daemon=False)
        write_worker(world.db, SNAP, [warned(rec("snapshot", "tank/a"))])
        assert set(worker_entries_of(world)) == {("worker_status", SNAP, None)}

    def test_a_warned_record_beside_a_clean_one_is_warn_overall(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a")), rec("snapshot", "tank/b")])
        entries = worker_entries_of(at_rest)
        assert entries[("worker_snapshot", "tank/a", None)]["state"] == "WARN"
        assert entries[("worker_snapshot", "tank/b", None)]["state"] == "OK"

    def test_a_warned_record_does_not_hide_a_crit_elsewhere(self, at_rest):
        write_worker(at_rest.db, SNAP, [warned(rec("snapshot", "tank/a"))])
        write_worker(at_rest.db, SEND, [rec("send", "tank/a", "offsite", failures=5)])
        assert json.loads(at_rest.check("--json").stdout)["state"] == "CRIT"
        assert at_rest.check("--nagios").exit_code == 2

    def test_a_warned_record_alongside_copies_entries_is_warn_overall(self, rec_world):
        rec_world.mark("tank/rec/b")
        write_worker(rec_world.db, SNAP, [warned(rec("snapshot", "tank/rec"), "1 backup copy left out")])
        data = json.loads(rec_world.check("--json").stdout)
        sources = {e["source"]: e["state"] for e in data["entries"] if e["state"] != "OK"}
        assert sources == {"copies": "WARN", "worker_snapshot": "WARN"}
        assert data["state"] == "WARN"
        assert rec_world.check().exit_code == 1 and rec_world.check("--nagios").exit_code == 1
        assert rec_world.check("--prometheus").exit_code == 1

    def test_the_help_text(self, healthy):
        text = " ".join(healthy.run("check", "--help").stdout.split())
        assert ("a last attempt that succeeded but recorded a warning (for example a recursive "
                "snapshot that left backup copies out) is WARN until the next attempt is clean") in text
        doc = " ".join(check_view.__doc__.split())
        assert "unless the record carries a `warning`" in doc and "`warning` (null when none)" in doc


# --------------------------------------------------------------------------
# 14. `copies` entries: backup copies below a recursive dataset
# --------------------------------------------------------------------------


def copies_of(entries):
    return {k: v for k, v in entries.items() if k[0] == "copies"}


class TestCopiesEntries:
    def test_text(self, rec_world):
        rec_world.mark("tank/rec/b", "received")
        result = rec_world.check()
        assert result.exit_code == 1
        assert re.search(
            r"^WARN copies: tank/rec/b: backup copy below the recursive dataset tank/rec "
            r"\(.+\): it is not snapshotted, sent or pruned with the tree; move it out "
            r"\(`zfs rename tank/rec/b <a name outside tank/rec>`\)$", result.stdout, re.M)
        assert result.stdout.splitlines()[-1].startswith("overall: WARN")

    def test_json(self, rec_world):
        rec_world.mark("tank/rec/b")
        result = rec_world.check("--json")
        assert result.exit_code == 1
        data = json.loads(result.stdout)
        (entry,) = [e for e in data["entries"] if e["source"] == "copies"]
        assert (entry["subject"], entry["state"]) == ("tank/rec/b", "WARN")
        assert entry.get("peer") is None and "age_seconds" not in entry
        assert data["state"] == "WARN"

    def test_prometheus(self, rec_world):
        rec_world.mark("tank/rec/b")
        result = rec_world.check("--prometheus")
        assert result.exit_code == 1
        parsed = Exposition(result.stdout)
        assert parsed.one("zfsbackup_check_state", source="copies", subject="tank/rec/b") == 1
        assert parsed.one("zfsbackup_check_overall_state") == 1
        # no copy gets an anchor series: it is not a source
        assert not [s for s in parsed.samples if s[0].startswith("zfsbackup_anchor_")
                    and s[1].get("dataset", "").startswith("tank/rec/b")]

    def test_nagios(self, rec_world):
        rec_world.mark("tank/rec/b")
        result = rec_world.check("--nagios")
        assert result.exit_code == 1
        assert result.stdout.startswith(
            "ZFSBACKUP WARN - WARN copies: tank/rec/b: backup copy below the recursive dataset tank/rec")
        perf = result.stdout.split(" | ", 1)[1]
        assert "copies" not in perf and "tank/rec/b" not in perf       # no age, so no perfdata item
        assert "'anchor:tank/rec/a@offsite'" in perf

    def test_the_other_children_are_still_graded(self, rec_world):
        rec_world.mark("tank/rec/b")
        entries = rec_entries(rec_world)
        assert entries[("anchor", "tank/rec/a", "offsite")]["state"] == "OK"
        assert entries[("anchor", "tank/rec", "offsite")]["state"] == "OK"
        assert not [k for k in entries if k[0] == "anchor" and k[1].startswith("tank/rec/b")]

    @pytest.mark.parametrize("flag", [None, "--json", "--prometheus", "--nagios"])
    def test_no_error_goes_to_stderr_for_a_copy_during_check(self, rec_world, flag):
        # the daemon logs the copy once as an ERROR; `check` reports it as an entry instead
        rec_world.mark("tank/rec/b", "received")
        result = rec_world.check(*([flag] if flag else []))
        assert result.exit_code == 1
        assert result.stderr == "", result.stderr
        assert "ERROR" not in result.stdout

    def test_an_inherited_marker_reports_the_copy_root_once(self, rec_world):
        rec_world.mark("tank/rec/b")                      # b/c inherits it
        entries = copies_of(rec_entries(rec_world))
        assert list(entries) == [("copies", "tank/rec/b", None)]

    def test_two_independent_copies_get_an_entry_each(self, rec_world):
        rec_world.mark("tank/rec/a")
        rec_world.mark("tank/rec/b", "received")
        assert sorted(k[1] for k in copies_of(rec_entries(rec_world))) == ["tank/rec/a", "tank/rec/b"]

    def test_a_source_dataset_marker_counts_too(self, rec_world):
        rec_world.mark("tank/rec/a", prop="org.zfsbackup:source_dataset")
        assert [k[1] for k in copies_of(rec_entries(rec_world))] == ["tank/rec/a"]

    def test_the_parent_inside_a_copy_is_one_entry_about_the_parent(self, rec_world):
        rec_world.mark("tank/rec")                        # every child inherits: none is a source
        entries = rec_entries(rec_world)
        copies = copies_of(entries)
        assert [k[1] for k in copies] == ["tank/rec"]
        assert "is itself inside a backup copy or receive target" in copies[("copies", "tank/rec", None)]["reason"]
        assert not [k for k in entries if k[0] == "anchor" and k[1].startswith("tank/rec/")]

    def test_no_copy_no_entry(self, rec_world):
        assert copies_of(rec_entries(rec_world)) == {}
        assert rec_world.check().exit_code == 0

    @pytest.mark.parametrize("count, shown, more", [(19, 19, 0), (20, 20, 0), (21, 20, 1), (25, 20, 5), (60, 20, 40)])
    def test_the_cap_is_twenty_then_and_n_more(self, rec_world, count, shown, more):
        names = [f"tank/rec/k{i:02d}" for i in range(count)]
        for name in names:
            rec_world.add_child(name)
            rec_world.spec["markers"].setdefault(name, {})["org.zfsbackup:client_id"] = "local"
        rec_world.zfs.save()
        result = rec_world.check("--json")
        assert result.exit_code == 1
        entries = copies_of(rec_entries(rec_world))
        assert len(entries) == shown + (1 if more else 0)
        tail_subject = f"tank/rec (+{more} more)"
        assert sorted(k[1] for k in entries if k[1] != tail_subject) == sorted(names[:shown])
        if more:
            tail = entries[("copies", tail_subject, None)]
            assert tail["state"] == "WARN"
            assert tail["reason"].startswith(
                f"and {more} more backup copies below the recursive dataset tank/rec (the first 20 are listed)")
        else:
            assert not [k for k in entries if "more)" in k[1]]
        # the copies beyond the cap are neither graded nor reported by name
        assert not [k for k in rec_entries(rec_world) if k[0] == "anchor" and k[1] in names]
        assert result.stderr == ""

    def test_the_cap_in_nagios_is_still_one_line_with_the_verdict(self, rec_world):
        for i in range(30):
            name = f"tank/rec/k{i:02d}"
            rec_world.add_child(name)
            rec_world.spec["markers"][name] = {"org.zfsbackup:client_id": "local"}
        rec_world.zfs.save()
        result = rec_world.check("--nagios")
        assert result.exit_code == 1 and result.stdout.count("\n") == 1
        assert result.stdout.startswith("ZFSBACKUP WARN - WARN copies: tank/rec/k00")
        # 20 copies entries and the "and 10 more" one are not OK: the line names 3, then counts 18
        assert result.stdout.split(" | ")[0].endswith("; and 18 more")


# --------------------------------------------------------------------------
# 15. batching end to end: O(parents) `zfs` execs, not O(children)
# --------------------------------------------------------------------------


def kinds(calls):
    """The execs of a `check`, by kind."""
    def is_(prefix):
        return [c for c in calls if c[: len(prefix)] == prefix]

    return {
        "tree": [c for c in calls if c[0] == "list" and "-r" in c and "snapshot" in c],
        "own_lists": [c for c in calls if c[0] == "list" and "-r" not in c and "snapshot" in c],
        "phase1": [c for c in calls if c[0] == "get" and "name,property,source" in c and "all" in c],
        "phase2": [c for c in calls if c[0] == "get" and "name,property,value,source" in c],
        "markers": [c for c in calls if c[0] == "get" and "name,property,source" in c and "all" not in c],
        "values": [c for c in calls if c[0] == "get" and c[1:4] == ["-H", "-o", "value"]],
        "names": [c for c in calls if c[0] == "list" and flag_value(c, "-o") == "name,type"],
    }


def flag_value(call, name):
    return call[call.index(name) + 1] if name in call else None


class TestBatching:
    def test_forty_children_cost_the_same_execs_as_three(self, rec_world):
        rec_world.zfs.log_path.write_text("")                     # (the log also holds the setup's calls)
        assert rec_world.check("--json").exit_code == 0
        small = list(rec_world.zfs.calls)
        rec_world.zfs.log_path.write_text("")
        names = [f"tank/rec/k{i:02d}" for i in range(40)]
        for name in names:
            rec_world.add_child(name)
        result = rec_world.check("--json")
        assert result.exit_code == 0, result.stdout
        big = list(rec_world.zfs.calls)
        assert len(big) == len(small)                               # O(parents), not O(children)
        assert Counter_of(big) == Counter_of(small)
        entries = rec_entries(rec_world)
        assert all(entries[("anchor", n, "offsite")]["state"] == "OK" for n in names)
        assert len([k for k in entries if k[0] == "anchor"]) == 1 + len(CHILDREN) + 40

    def test_one_batch_per_parent(self, rec_world):
        for i in range(40):
            rec_world.add_child(f"tank/rec/k{i:02d}")
        rec_world.zfs.log_path.write_text("")                     # (the log also holds the setup's calls)
        assert rec_world.check("--json").exit_code == 0
        by = kinds(rec_world.zfs.calls)
        assert len(by["tree"]) == 1 and by["tree"][0][-1] == "tank/rec"
        assert len(by["phase1"]) == 1 and len(by["phase2"]) == 1
        assert len(by["markers"]) == 1
        # the one-property reads and the non-recursive snapshot lists are the PARENT's, never a child's
        assert {c[-1] for c in by["values"]} == {"tank/rec"} and len(by["values"]) == 2
        assert {c[-1] for c in by["own_lists"]} == {"tank/rec"} and len(by["own_lists"]) == 1
        # the whole run: 9 execs for one parent, however many children: its own snapshot list and
        # two one-property reads, the discovery (listing, copy markers), the batch (snapshot tree,
        # phase 1, phase 2) and the re-listing that drops a vanished child
        assert len(rec_world.zfs.calls) == 9
        assert {c[0] for c in rec_world.zfs.calls} == {"list", "get"}

    def test_the_batch_is_strict_and_asks_for_the_six_listing_columns(self, rec_world):
        assert rec_world.check("--json").exit_code == 0
        (tree,) = kinds(rec_world.zfs.calls)["tree"]
        assert flag_value(tree, "-o") == "name,type,createtxg,guid,userrefs,clones,encryption,keystatus"
        assert tree[:5] == ["list", "-H", "-r", "-t", "snapshot"]
        (phase1,) = kinds(rec_world.zfs.calls)["phase1"]
        assert flag_value(phase1, "-s") == "local,received" and "-r" in phase1
        assert flag_value(phase1, "-t") == "filesystem,volume"
        (phase2,) = kinds(rec_world.zfs.calls)["phase2"]
        assert phase2[-2] == "name,type,org.zfsbackup:anchor.offsite" and phase2[-1] == "tank/rec"

    def test_two_parents_cost_two_batches_whatever_the_children(self, tmp_path, world, zfs):
        install_recursive_fake(zfs, tmp_path)
        zfs.spec.update(datasets={"tank/rec": "filesystem", "tank/two": "filesystem"}, markers={},
                        fail_names=[], fail_markers=[], fail_tree=[], fail_props1=[], fail_rows=[],
                        fail_get=[], slow=[])
        world.build(TWO_PARENTS_YAML, snapshot_age=600)
        for parent in ("tank/rec", "tank/two"):
            zfs.prop(parent, ANCHOR_PROP, zfs.snapshots(parent, 600, 4200)[0])
        counts = []
        for children in (1, 41):
            for parent in ("tank/rec", "tank/two"):
                for i in range(children if children == 1 else 40):
                    name = f"{parent}/c{i:02d}"
                    zfs.spec["datasets"][name] = "filesystem"
                    zfs.prop(name, ANCHOR_PROP, zfs.snapshots(name, 600, 4200)[0])
            zfs.log_path.write_text("")
            assert world.check("--json").exit_code == 0
            counts.append(len(zfs.calls))
        assert counts[0] == counts[1] == 18                       # 9 per parent
        by = kinds(zfs.calls)
        assert sorted(c[-1] for c in by["tree"]) == ["tank/rec", "tank/two"]
        assert len(by["phase1"]) == len(by["phase2"]) == 2

    def test_stale_children_are_graded_each_on_its_own_anchor(self, rec_world):
        names = [f"tank/rec/k{i:02d}" for i in range(40)]
        for name in names:
            rec_world.add_child(name)
        for name, hours in (("tank/rec/k07", 3), ("tank/rec/k31", 5)):
            late = rec_world.zfs.snapshots(name, hours * 3600, hours * 3600 + 3600)
            rec_world.anchor(name, late[0])
        entries = rec_entries(rec_world)
        states = {n: entries[("anchor", n, "offsite")]["state"] for n in names}
        assert states["tank/rec/k07"] == "WARN" and states["tank/rec/k31"] == "CRIT"
        assert [n for n, st in states.items() if st == "OK"] == [
            n for n in names if n not in ("tank/rec/k07", "tank/rec/k31")]

    def test_a_child_with_no_anchor_is_not_yet_sent(self, rec_world):
        rec_world.spec["datasets"]["tank/rec/new"] = "filesystem"
        rec_world.zfs.snapshots("tank/rec/new", 300)               # snapshotted, never sent
        entry = rec_entries(rec_world)[("anchor", "tank/rec/new", "offsite")]
        assert entry["state"] == "WARN" and "not yet sent" in entry["reason"]

    def test_a_child_without_a_snapshot_yet_is_not_yet_sent_not_an_error(self, rec_world):
        rec_world.spec["datasets"]["tank/rec/fresh"] = "filesystem"
        rec_world.zfs.save()
        entry = rec_entries(rec_world)[("anchor", "tank/rec/fresh", "offsite")]
        assert entry["state"] == "WARN" and "not yet sent" in entry["reason"]
        assert not entry["reason"].startswith("cannot read")

    def test_an_inflight_pin_of_a_child_is_noted_from_the_batch(self, rec_world):
        pin = f"{rec_world.names['tank/rec/a'][1]},{rec_world.names['tank/rec/a'][0]}"
        rec_world.zfs.prop("tank/rec/a", "org.zfsbackup:inflight.offsite", pin)
        entry = rec_entries(rec_world)[("anchor", "tank/rec/a", "offsite")]
        assert entry["state"] == "OK" and "send in flight" in entry["reason"]
        (phase2,) = kinds(rec_world.zfs.calls)["phase2"][-1:]
        assert phase2[-2] == "name,type,org.zfsbackup:anchor.offsite,org.zfsbackup:inflight.offsite"


def Counter_of(calls):
    import collections

    return collections.Counter(tuple(c) for c in calls)

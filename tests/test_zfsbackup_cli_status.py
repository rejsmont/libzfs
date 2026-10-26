"""`zfsbackup-config status` (plan item 19.2): is the running daemon on the stored
configuration?

Read-only and signal-free. The stored `(store_id, generation)` key comes from a
real database written through the real CLI; the daemon is a `FakeDaemon` (the
daemon lock held in this process with a hand-written pid file), so every verdict
is produced by the real `daemonlock.probe` reading a real file.

Sections:

- ``TestVerdicts``            every verdict, and the comparison rules (same store
                              ordered, different stores never).
- ``TestTextOutput``          the fields of the human form.
- ``TestJsonShape``           the stable machine form, with and without a daemon.
- ``TestExitCodes``           0 / 78 / 1 without `--wait`; the `--wait` codes.
- ``TestWaitFor``             the polling loop on an injected clock: re-reads the
                              stored key, applying -> in sync, timeouts.
- ``TestStatusNeverSignals``  no signal of any kind, no `request_reload`.
- ``TestWorkerRows``          (S-7b) the ``workers:`` rows and the JSON ``workers`` key from the
                              worker status files; staleness; the verdict and exit codes never move.
"""

import contextlib
import errno
import json
import os
import signal
import sqlite3
import subprocess
import sys
import re
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from _cli_support import (
    PLAIN_YAML,
    db,
    db_state,
    live_daemon,
    generation,
    isolated,
    make_db,
    run,
    store_key,
)
from _worker_status_support import LIVE_PID, iso, make_dir, rec, status_dir, write_worker
from zfsbackup import daemonlock, health, procident
from zfsbackup.cli import errors as cli_errors
from zfsbackup.cli import status as status_view
from zfsbackup.config.store import dispose_all, resolve_config_path


def _resolved(database: Path):
    return resolve_config_path(str(database))


@pytest.fixture
def plain(run, db):
    return make_db(run, db, PLAIN_YAML)


#: The tail of every "behind" message.
SEE_LOG = "if it does not, see the daemon log"


def dead_pid() -> int:
    """The pid of a process that has exited and been reaped."""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def status(run, db, *args):
    return run("status", *args, "-c", db)


def as_json(result) -> dict:
    return json.loads(result.stdout)


def verdict(run, db, *args) -> str:
    result = status(run, db, "--json", *args)
    assert result.exit_code == 0, result.stderr
    return as_json(result)["verdict"]


def message(run, db) -> str:
    return as_json(status(run, db, "--json"))["message"]


def bump(run, db, value="10m") -> tuple:
    """One more committed generation; -> the new stored key."""
    assert run("set", "check_interval", value, "-c", db).exit_code == 0
    return store_key(db)


# --------------------------------------------------------------------------
# verdicts
# --------------------------------------------------------------------------


class TestVerdicts:
    def test_no_daemon(self, run, plain, db):
        result = status(run, db, "--json")
        assert as_json(result)["verdict"] == "no-daemon"
        assert as_json(result)["message"] == "no running daemon"

    def test_a_stale_pid_file_nobody_holds_is_no_daemon(self, run, plain, db):
        daemonlock.lock_path(_resolved(db)).write_text("4321\n0\nstart=tok\napplied=1:1\n")
        assert verdict(run, db) == "no-daemon"

    def test_in_sync(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        assert verdict(run, db) == "in-sync"
        assert message(run, db) == "in sync"

    def test_a_newer_applied_generation_of_the_same_store_is_in_sync(self, run, plain, db, live_daemon):
        sid, gen = store_key(db)
        live_daemon(db, applied=(sid, gen + 5))
        assert verdict(run, db) == "in-sync"

    def test_applying(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid, gen))
        assert verdict(run, db) == "applying"
        assert message(run, db) == f"applying generation {gen}"

    def test_a_newer_target_of_the_same_store_is_applying(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid, gen + 3))
        assert verdict(run, db) == "applying"

    def test_applied_beats_target(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen), target=(sid, gen))
        assert verdict(run, db) == "in-sync"

    def test_rejected(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), rejected=(sid, gen))
        assert verdict(run, db) == "rejected"
        assert message(run, db) == f"daemon rejected generation {gen}"

    def test_a_newer_rejected_key_says_nothing_about_ours(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), rejected=(sid, gen + 1))
        assert verdict(run, db) == "behind"

    def test_an_older_rejected_key_says_nothing_about_ours(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), rejected=(sid, gen - 1))
        assert verdict(run, db) == "behind"

    def test_applied_in_sync_wins_over_an_old_rejected_key(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen), rejected=(sid, gen))
        assert verdict(run, db) == "in-sync"

    def test_behind(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        assert verdict(run, db) == "behind"
        assert message(run, db) == (
            f"daemon on generation {gen - 1}, stored {gen} (it will pick it up; {SEE_LOG})"
        )

    def test_a_reload_capable_daemon_that_recorded_no_applied_key_yet(self, run, plain, db, live_daemon):
        live_daemon(db, generation=None, applied=None)
        assert verdict(run, db) == "behind"
        assert message(run, db).startswith("daemon on an unknown generation, stored 0 ")
        assert "None" not in message(run, db)

    def test_without_an_applied_key_line_2_names_the_generation(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, generation=gen - 1, applied=None)
        assert message(run, db) == f"daemon on generation {gen - 1}, stored {gen} (it will pick it up; {SEE_LOG})"

    # -- keys of different stores are never ordered ------------------------

    @pytest.mark.parametrize("offset", [-1, 0, 1, 50], ids=["older", "same", "newer", "far-newer"])
    def test_behind_across_stores_whatever_the_generation(self, run, plain, db, live_daemon, offset):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid + 1, max(gen + offset, 0)))
        assert verdict(run, db) == "behind"
        assert message(run, db) == (
            f"daemon on store {sid + 1}, generation {max(gen + offset, 0)}, stored "
            f"store {sid}, generation {gen} (the database file was replaced; it will pick it up; {SEE_LOG})"
        )

    def test_across_stores_with_an_unstamped_applied_store_does_not_claim_a_replacement(
        self, run, plain, db, live_daemon
    ):
        """Store id 0 means the daemon's database was a legacy (never stamped)
        file: nothing was "replaced" as far as it can tell."""
        sid, gen = bump(run, db)
        live_daemon(db, applied=(0, gen + 4))
        assert verdict(run, db) == "behind"
        assert message(run, db) == (
            f"daemon on store 0, generation {gen + 4}, stored store {sid}, generation {gen} "
            f"(it will pick it up; {SEE_LOG})"
        )
        assert "replaced" not in message(run, db)

    def test_a_target_of_another_store_is_not_applying(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid + 1, gen + 9))
        assert verdict(run, db) == "behind"

    def test_a_rejected_key_of_another_store_is_not_rejected(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), rejected=(sid + 1, gen))
        assert verdict(run, db) == "behind"

    def test_the_same_generation_of_another_store_is_not_in_sync(self, run, plain, db, live_daemon):
        sid, gen = store_key(db)
        live_daemon(db, applied=(sid + 7, gen))
        assert verdict(run, db) == "behind"

    # -- a daemon that predates live reload --------------------------------

    def test_a_legacy_daemon_on_the_same_generation_is_in_sync(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, generation=gen, start=None)
        assert verdict(run, db) == "in-sync"
        assert message(run, db) == "in sync"

    def test_a_legacy_daemon_on_another_generation_is_behind_and_must_be_restarted(
        self, run, plain, db, live_daemon
    ):
        sid, gen = bump(run, db)
        live_daemon(db, generation=gen - 1, start=None)
        assert verdict(run, db) == "behind"
        assert message(run, db) == (
            f"daemon on generation {gen - 1}, stored {gen} "
            "(it predates live reload; restart it to apply this)"
        )

    def test_a_legacy_daemon_that_is_ahead_is_still_behind(self, run, plain, db, live_daemon):
        """No ordering is assumed for a legacy daemon: unequal is unequal (the
        file may have been restored from a backup)."""
        sid, gen = bump(run, db)
        live_daemon(db, generation=gen + 3, start=None)
        assert verdict(run, db) == "behind"

    def test_a_legacy_daemon_with_an_unknown_generation(self, run, plain, db, live_daemon):
        live_daemon(db, generation=None, start=None)
        assert verdict(run, db) == "behind"
        assert message(run, db) == (
            "daemon on an unknown generation, stored 0 "
            "(it predates live reload; restart it to apply this)"
        )

    def test_a_reload_capable_daemon_is_never_told_to_restart(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        assert "restart" not in message(run, db)

    # -- the supervisor ----------------------------------------------------

    def test_supervisor_gone(self, run, plain, db, live_daemon):
        pid = dead_pid()
        live_daemon(db, pid=pid, applied=store_key(db))
        assert verdict(run, db) == "supervisor-gone"
        assert message(run, db) == (
            f"daemon supervisor pid {pid} is gone, but a process it forked still holds "
            "the daemon lock"
        )

    def test_supervisor_gone_comes_before_the_key_comparison(self, run, plain, db, live_daemon):
        live_daemon(db, pid=dead_pid(), generation=0, start=None)
        assert verdict(run, db) == "supervisor-gone"

    def test_an_unreadable_pid_is_not_a_dead_supervisor(self, run, plain, db, live_daemon):
        live_daemon(db, raw=b"")
        assert verdict(run, db) == "behind"

    def test_a_live_pid_that_is_somebody_elses_is_alive(self, run, plain, db, live_daemon):
        """EPERM from kill(pid, 0) means the process exists."""
        live_daemon(db, pid=1, applied=store_key(db))
        assert verdict(run, db) == "in-sync"


# --------------------------------------------------------------------------
# text and JSON
# --------------------------------------------------------------------------


class TestTextOutput:
    def test_without_a_daemon(self, run, plain, db):
        sid, gen = store_key(db)
        out = status(run, db).stdout
        assert out == (
            f"database: {_resolved(db).resolved_path}\n"
            f"stored:   store {sid}, generation {gen}\n"
            "daemon:   not running\n"
            "status:   no running daemon\n"
        )

    def test_with_a_reload_capable_daemon(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid, gen), rejected=(sid, 0))
        assert status(run, db).stdout == (
            f"database: {_resolved(db).resolved_path}\n"
            f"stored:   store {sid}, generation {gen}\n"
            f"daemon:   pid {os.getpid()} (reload-capable: yes)\n"
            f"applied:  store {sid}, generation {gen - 1}\n"
            f"target:   store {sid}, generation {gen}\n"
            f"rejected: store {sid}, generation 0\n"
            f"status:   applying generation {gen}\n"
        )

    def test_absent_keys_print_a_dash(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        lines = status(run, db).stdout.splitlines()
        assert "target:   -" in lines and "rejected: -" in lines

    def test_with_a_legacy_daemon(self, run, plain, db, live_daemon):
        sid, gen = store_key(db)
        live_daemon(db, generation=gen, start=None)
        assert status(run, db).stdout == (
            f"database: {_resolved(db).resolved_path}\n"
            f"stored:   store {sid}, generation {gen}\n"
            f"daemon:   pid {os.getpid()} (reload-capable: no)\n"
            f"applied:  generation {gen}\n"
            "status:   in sync\n"
        )

    def test_a_legacy_daemon_with_no_generation(self, run, plain, db, live_daemon):
        live_daemon(db, generation=None, start=None)
        assert "applied:  unknown" in status(run, db).stdout.splitlines()

    def test_an_unreadable_pid_prints_pid_unknown(self, run, plain, db, live_daemon):
        live_daemon(db, raw=b"")
        assert "daemon:   pid unknown (reload-capable: no)" in status(run, db).stdout

    def test_the_last_line_is_the_verdict(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        assert status(run, db).stdout.splitlines()[-1] == "status:   in sync"


class TestJsonShape:
    def test_with_a_daemon(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, generation=gen - 1, applied=(sid, gen - 1), target=(sid, gen), rejected=(sid, 0))
        doc = as_json(status(run, db, "--json"))
        assert doc == {
            "database": str(_resolved(db).resolved_path),
            "stored": {"store_id": sid, "generation": gen},
            "daemon": {
                "pid": os.getpid(),
                "reload_capable": True,
                "supervisor_alive": doc["daemon"]["supervisor_alive"],
                "generation": gen - 1,
                "applied": {"store_id": sid, "generation": gen - 1},
                "target": {"store_id": sid, "generation": gen},
                "rejected": {"store_id": sid, "generation": 0},
                "deferred": None,
                "deferred_reason": None,
            },
            "schema_error": None,
            "verdict": "applying",
            "message": f"applying generation {gen}",
        }
        assert doc["daemon"]["supervisor_alive"] is True

    def test_without_a_daemon_the_daemon_is_null(self, run, plain, db):
        doc = as_json(status(run, db, "--json"))
        assert doc["daemon"] is None
        assert list(doc) == ["database", "stored", "daemon", "schema_error", "verdict", "message"]
        assert doc["verdict"] == "no-daemon"

    def test_absent_keys_are_null(self, run, plain, db, live_daemon):
        live_daemon(db, applied=None, generation=None)
        daemon = as_json(status(run, db, "--json"))["daemon"]
        assert daemon["applied"] is None and daemon["target"] is None and daemon["rejected"] is None
        assert daemon["generation"] is None and daemon["reload_capable"] is True

    def test_a_legacy_daemon_is_not_reload_capable(self, run, plain, db, live_daemon):
        live_daemon(db, generation=0, start=None)
        daemon = as_json(status(run, db, "--json"))["daemon"]
        assert daemon["reload_capable"] is False and daemon["generation"] == 0
        assert daemon["applied"] is None

    def test_a_dead_supervisor_is_reported_not_alive(self, run, plain, db, live_daemon):
        live_daemon(db, pid=dead_pid())
        assert as_json(status(run, db, "--json"))["daemon"]["supervisor_alive"] is False

    def test_an_unreadable_pid_is_null(self, run, plain, db, live_daemon):
        live_daemon(db, raw=b"")
        daemon = as_json(status(run, db, "--json"))["daemon"]
        assert daemon["pid"] is None and daemon["supervisor_alive"] is None

    def test_the_top_level_keys_are_stable(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        assert list(as_json(status(run, db, "--json"))) == [
            "database", "stored", "daemon", "schema_error", "verdict", "message",
        ]
        assert list(as_json(status(run, db, "--json"))["daemon"]) == [
            "pid", "reload_capable", "supervisor_alive", "generation", "applied", "target", "rejected",
            "deferred", "deferred_reason",
        ]

    def test_json_is_the_only_thing_on_stdout(self, run, plain, db):
        result = status(run, db, "--json")
        assert result.stdout.startswith("{") and result.stdout.endswith("}\n")
        assert result.stderr == ""

    def test_the_verdict_values_are_the_documented_ones(self):
        assert {
            status_view.IN_SYNC, status_view.APPLYING, status_view.REJECTED,
            status_view.BEHIND, status_view.SUPERVISOR_GONE, status_view.NO_DAEMON,
            status_view.DEFERRED,
        } == {"in-sync", "applying", "rejected", "behind", "supervisor-gone", "no-daemon", "deferred"}


# --------------------------------------------------------------------------
# exit codes
# --------------------------------------------------------------------------


def _delete_settings_row(database: Path) -> None:
    dispose_all()
    conn = sqlite3.connect(database)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DELETE FROM global_settings")
        conn.commit()
    finally:
        conn.close()


class TestExitCodes:
    @pytest.mark.parametrize(
        "daemon",
        [
            None,
            dict(applied=("S", "G")),
            dict(applied=("S", "G-1")),
            dict(applied=("S", "G-1"), target=("S", "G")),
            dict(applied=("S", "G-1"), rejected=("S", "G")),
            dict(applied=("X", 0)),
            dict(generation=99, start=None),
            dict(pid="dead"),
        ],
        ids=["no-daemon", "in-sync", "behind", "applying", "rejected", "other-store", "legacy", "gone"],
    )
    @pytest.mark.parametrize("flag", [(), ("--json",)], ids=["text", "json"])
    def test_without_wait_it_is_always_zero(self, run, plain, db, live_daemon, daemon, flag):
        sid, gen = bump(run, db)
        if daemon is not None:
            kw = dict(daemon)
            if kw.get("pid") == "dead":
                kw["pid"] = dead_pid()
            for name in ("applied", "target", "rejected"):
                if name in kw:
                    s, g = kw[name]
                    kw[name] = (
                        sid + 1 if s == "X" else sid,
                        gen - 1 if g == "G-1" else gen if g == "G" else g,
                    )
            live_daemon(db, **kw)
        assert status(run, db, *flag).exit_code == 0

    def test_a_missing_database_is_78(self, run, tmp_path):
        result = run("status", "-c", tmp_path / "nowhere" / "config.db")
        assert result.exit_code == 78
        assert result.stderr.startswith("error: ")
        assert result.stdout == ""

    def test_a_missing_database_is_never_created(self, run, tmp_path):
        target = tmp_path / "nowhere" / "config.db"
        run("status", "-c", target)
        assert not target.parent.exists()

    def test_a_foreign_sqlite_file_is_78(self, run, tmp_path):
        foreign = tmp_path / "foreign.db"
        conn = sqlite3.connect(foreign)
        conn.execute("CREATE TABLE orders (id INTEGER)")
        conn.commit()
        conn.close()
        before = foreign.read_bytes()
        result = run("status", "-c", foreign)
        assert result.exit_code == 78
        assert foreign.read_bytes() == before

    def test_not_a_database_at_all_is_78(self, run, tmp_path):
        junk = tmp_path / "junk.db"
        junk.write_bytes(b"this is not sqlite" * 100)
        assert run("status", "-c", junk).exit_code == 78

    def test_no_settings_row_is_78(self, run, plain, db):
        _delete_settings_row(db)
        result = run("status", "-c", db)
        assert result.exit_code == 78, result.stderr
        assert "the stored configuration cannot be read" in result.stderr

    def test_no_settings_row_is_78_with_json_and_wait_too(self, run, plain, db):
        _delete_settings_row(db)
        assert run("status", "--json", "-c", db).exit_code == 78
        assert run("status", "--wait", "5", "-c", db).exit_code == 78

    def test_the_unusable_error_type_maps_to_78(self):
        assert cli_errors.exit_code_for(cli_errors.ConfigUnusable("x")) == 78

    @pytest.fixture
    def mismatched(self, run, plain, db):
        dispose_all()
        conn = sqlite3.connect(db)
        conn.execute("UPDATE alembic_version SET version_num = 'ffffffffffff'")
        conn.commit()
        conn.close()
        return db

    def test_a_schema_mismatch_prints_the_daemon_state_first_and_exits_78(self, run, mismatched):
        result = run("status", "-c", mismatched)
        assert result.exit_code == 78
        assert result.stdout.startswith("database: ") and "status:" in result.stdout
        assert "schema:   " in result.stdout
        assert result.stderr.startswith("error: ")

    def test_a_schema_mismatch_in_json_carries_the_error_and_still_exits_78(self, run, mismatched):
        result = run("status", "--json", "-c", mismatched)
        assert result.exit_code == 78
        doc = as_json(result)
        assert isinstance(doc["schema_error"], str) and doc["schema_error"]
        assert doc["verdict"] == "no-daemon"

    def test_a_schema_mismatch_ends_a_wait_at_once(self, run, mismatched, live_daemon):
        live_daemon(mismatched, applied=(1, 1))
        started = time.monotonic()
        assert run("status", "--wait", "60", "-c", mismatched).exit_code == 78
        assert time.monotonic() - started < 10

    def test_a_missing_settings_table_is_78_without_a_status_block(self, run, plain, db):
        dispose_all()
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DROP TABLE global_settings")
        conn.commit()
        conn.close()
        result = run("status", "-c", db)
        assert result.exit_code == 78, result.stderr
        assert result.stdout == "" and result.stderr.startswith("error: ")

    def test_a_foreign_file_prints_no_status_block(self, run, tmp_path):
        foreign = tmp_path / "foreign.db"
        conn = sqlite3.connect(foreign)
        conn.execute("CREATE TABLE orders (id INTEGER)")
        conn.commit()
        conn.close()
        result = run("status", "--json", "-c", foreign)
        assert result.exit_code == 78 and result.stdout == ""

    def test_a_good_schema_has_no_schema_row(self, run, plain, db):
        assert "schema:" not in status(run, db).stdout
        assert as_json(status(run, db, "--json"))["schema_error"] is None

    def test_a_probe_oserror_is_1(self, run, plain, db, monkeypatch):
        def failing(resolved):
            raise daemonlock.DaemonLockError(errno.EACCES, "denied")

        monkeypatch.setattr(daemonlock, "probe", failing)
        result = run("status", "-c", db)
        assert result.exit_code == 1
        assert result.stderr.startswith(
            f"error: could not check whether a daemon is running for {_resolved(db).resolved_path}"
        )
        assert result.stdout == ""

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can open an unreadable file")
    def test_an_unreadable_pid_file_is_1(self, run, plain, db):
        lock = daemonlock.lock_path(_resolved(db))
        lock.write_text("4321\n")
        lock.chmod(0)
        try:
            result = run("status", "-c", db)
        finally:
            lock.chmod(0o600)
        assert result.exit_code == 1 and "Permission denied" in result.stderr

    def test_a_plain_oserror_from_probe_is_1_too(self, run, plain, db, monkeypatch):
        monkeypatch.setattr(daemonlock, "probe", lambda r: (_ for _ in ()).throw(OSError(errno.EIO, "io")))
        assert run("status", "-c", db).exit_code == 1

    # -- --wait ----------------------------------------------------------

    def test_wait_in_sync_is_0(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        result = status(run, db, "--wait", "5")
        assert result.exit_code == 0 and result.stderr == ""
        assert result.stdout.splitlines()[-1] == "status:   in sync"

    def test_wait_with_no_daemon_is_0_at_once(self, run, plain, db, monkeypatch):
        slept = []
        monkeypatch.setattr(time, "sleep", lambda s: slept.append(s))
        started = time.monotonic()
        result = status(run, db, "--wait", "60")
        assert result.exit_code == 0 and result.stderr == ""
        assert time.monotonic() - started < 10
        assert slept == []

    def test_wait_rejected_is_1_with_an_explanation_on_stderr(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), rejected=(sid, gen))
        started = time.monotonic()
        result = status(run, db, "--wait", "60")
        assert result.exit_code == 1
        assert time.monotonic() - started < 10  # it did not wait
        assert result.stderr == (
            "error: the daemon rejected the stored configuration and keeps running an older "
            "one; see its log\n"
        )
        assert result.stdout.splitlines()[-1] == f"status:   daemon rejected generation {gen}"

    def test_wait_timeout_is_75_with_the_verdict_on_stderr(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        result = status(run, db, "--wait", "0.4")
        assert result.exit_code == 75
        assert result.stderr == (
            f"error: the daemon is not yet in sync after 0.4s: daemon on generation {gen - 1}, "
            f"stored {gen} (it will pick it up; {SEE_LOG})\n"
        )
        assert result.stdout.splitlines()[-1].startswith("status:   daemon on generation")

    def test_wait_zero_looks_once_and_reports_75_when_behind(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        started = time.monotonic()
        assert status(run, db, "--wait", "0").exit_code == 75
        assert time.monotonic() - started < 5

    def test_wait_applying_that_never_finishes_is_75(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid, gen))
        result = status(run, db, "--wait", "0.3")
        assert result.exit_code == 75 and "applying generation" in result.stderr

    def test_wait_supervisor_gone_is_1_at_once_not_a_timeout(self, run, plain, db, live_daemon):
        """Nothing will ever pick the change up: waiting would only delay the
        bad news."""
        live_daemon(db, pid=dead_pid())
        started = time.monotonic()
        result = status(run, db, "--wait", "60")
        assert result.exit_code == 1
        assert time.monotonic() - started < 10
        assert result.stderr == (
            "error: the daemon's supervisor is gone, so nothing will pick up the stored "
            "configuration until the daemon is restarted\n"
        )

    def test_wait_for_a_legacy_daemon_that_is_behind_is_1_at_once(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, generation=gen - 1, start=None)
        started = time.monotonic()
        result = status(run, db, "--wait", "60")
        assert result.exit_code == 1 and time.monotonic() - started < 10
        assert result.stderr == (
            "error: the running daemon predates live reload and will not pick up the stored "
            "configuration; restart it\n"
        )

    def test_wait_for_a_legacy_daemon_that_is_in_sync_is_0(self, run, plain, db, live_daemon):
        live_daemon(db, generation=store_key(db)[1], start=None)
        assert status(run, db, "--wait", "5").exit_code == 0

    def test_wait_prints_json_too_and_exits_by_the_same_rule(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        result = status(run, db, "--wait", "0.3", "--json")
        assert result.exit_code == 75
        assert as_json(result)["verdict"] == "behind"

    def test_a_write_landing_during_a_real_wait_is_seen(self, run, plain, db, live_daemon):
        """The daemon catches up from another thread while `--wait` polls."""
        import threading

        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen - 1))
        threading.Timer(0.5, lambda: daemon.set(applied=(sid, gen))).start()
        started = time.monotonic()
        result = status(run, db, "--wait", "30")
        assert result.exit_code == 0, result.stderr
        assert 0.3 < time.monotonic() - started < 15

    @pytest.mark.parametrize("value", ["-1", "nan", "inf", "-inf"])
    def test_a_bad_wait_is_a_usage_error(self, run, plain, db, value):
        result = status(run, db, "--wait", value)
        assert result.exit_code == 2
        assert "finite number of seconds" in result.stderr

    def test_wait_needs_a_number(self, run, plain, db):
        assert status(run, db, "--wait", "soon").exit_code == 2

    def test_the_wait_exit_codes_by_verdict(self):
        def code(verdict, daemon=None):
            return status_view.wait_exit_code(
                status_view.Status("db", None, daemon, verdict, "m")
            )

        modern = daemonlock.DaemonInfo(pid=1, generation=0, start="t")
        legacy = daemonlock.DaemonInfo(pid=1, generation=0, start=None)
        assert code("in-sync") == 0 and code("no-daemon") == 0
        assert code("rejected") == 1
        assert code("supervisor-gone", modern) == 1
        assert code("behind", legacy) == 1
        assert code("behind", modern) == 75 and code("applying", modern) == 75
        assert code("deferred", modern) == 75          # never 1: it re-checks every poll
        assert code("unreadable", modern) == 75


# --------------------------------------------------------------------------
# torn reads, supervisor identity, "will not catch up"
# --------------------------------------------------------------------------


def _torn(pid=None):
    return daemonlock.DaemonInfo(
        pid=pid or os.getpid(), generation=0, start=procident.start_token(os.getpid()),
        applied=None, stable=False,
    )


class TestTornReadsAndIdentity:
    def test_a_pid_file_that_keeps_changing_is_unreadable_not_in_sync(
        self, run, plain, db, monkeypatch
    ):
        sid, gen = store_key(db)
        info = daemonlock.DaemonInfo(
            pid=os.getpid(), generation=gen, start=procident.start_token(os.getpid()),
            applied=(sid, gen), stable=False,
        )  # it even claims to be in sync
        monkeypatch.setattr(daemonlock, "probe", lambda r: info)
        monkeypatch.setattr(time, "sleep", lambda s: None)
        assert verdict(run, db) == "unreadable"
        assert message(run, db) == "pid file unreadable right now (being rewritten)"

    def test_a_torn_read_is_retried_then_read(self, run, plain, db, monkeypatch):
        sid, gen = store_key(db)
        good = daemonlock.DaemonInfo(
            pid=os.getpid(), generation=gen, start=procident.start_token(os.getpid()),
            applied=(sid, gen),
        )
        reads = [_torn(), _torn(), good]
        monkeypatch.setattr(daemonlock, "probe", lambda r: reads.pop(0))
        sleeps = []
        got = status_view.collect(_resolved(db), sleep=sleeps.append)
        assert got.verdict == "in-sync" and reads == []
        assert sleeps == [status_view.STABLE_RETRY_SECONDS] * 2

    def test_it_gives_up_after_three_reads(self, run, plain, db, monkeypatch):
        reads = []

        def probe(resolved):
            reads.append(1)
            return _torn()

        monkeypatch.setattr(daemonlock, "probe", probe)
        sleeps = []
        got = status_view.collect(_resolved(db), sleep=sleeps.append)
        assert got.verdict == "unreadable"
        assert len(reads) == status_view.STABLE_TRIES == 3 and len(sleeps) == 2

    def test_unreadable_is_not_a_reason_to_stop_waiting(self, run, plain, db, monkeypatch):
        monkeypatch.setattr(daemonlock, "probe", lambda r: _torn())
        monkeypatch.setattr(time, "sleep", lambda s: None)
        clock = Clock()
        got = status_view.wait_for(_resolved(db), 0.5, sleep=clock.sleep, clock=clock.monotonic)
        assert got.verdict == "unreadable" and sum(clock.sleeps) == pytest.approx(0.5)
        assert not got.will_not_catch_up

    def test_unreadable_wait_is_75_through_the_cli(self, run, plain, db, monkeypatch):
        monkeypatch.setattr(daemonlock, "probe", lambda r: _torn())
        monkeypatch.setattr(status_view.time, "sleep", lambda s: None)
        result = status(run, db, "--wait", "0.2")
        assert result.exit_code == 75
        assert "pid file unreadable right now" in result.stderr

    def test_a_reused_pid_is_a_gone_supervisor(self, run, plain, db, live_daemon):
        """The pid exists, but it is another process than the one that recorded
        the start token."""
        live_daemon(db, start="some-other-start-token", applied=store_key(db))
        assert verdict(run, db) == "supervisor-gone"
        assert as_json(status(run, db, "--json"))["daemon"]["supervisor_alive"] is False

    def test_a_matching_token_is_alive(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        assert as_json(status(run, db, "--json"))["daemon"]["supervisor_alive"] is True

    def test_without_a_token_the_pid_alone_decides(self, run, plain, db, live_daemon):
        sid, gen = store_key(db)
        live_daemon(db, start=None, generation=gen)
        assert as_json(status(run, db, "--json"))["daemon"]["supervisor_alive"] is True
        live_daemon_dead = dead_pid()
        # (the first daemon still holds the lock: rewrite its recorded pid)
        daemonlock_file = daemonlock.lock_path(_resolved(db))
        daemonlock_file.write_bytes(f"{live_daemon_dead}\n{gen}\n".encode())
        got = as_json(status(run, db, "--json"))
        assert got["daemon"]["supervisor_alive"] is False and got["verdict"] == "supervisor-gone"

    def test_an_unknown_supervisor_is_null_in_json(self, run, plain, db, live_daemon):
        live_daemon(db, raw=b"")
        assert as_json(status(run, db, "--json"))["daemon"]["supervisor_alive"] is None

    def test_will_not_catch_up(self):
        modern = daemonlock.DaemonInfo(pid=1, generation=0, start="t")
        legacy = daemonlock.DaemonInfo(pid=1, generation=0, start=None)

        def wnc(verdict, daemon):
            return status_view.Status("db", None, daemon, verdict, "m").will_not_catch_up

        assert wnc("supervisor-gone", modern) is True
        assert wnc("behind", legacy) is True
        assert wnc("behind", modern) is False
        assert wnc("applying", modern) is False
        assert wnc("behind", None) is False
        assert wnc("in-sync", legacy) is False


# --------------------------------------------------------------------------
# the polling loop
# --------------------------------------------------------------------------


class Clock:
    def __init__(self) -> None:
        self.now = 500.0
        self.sleeps = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class TestWaitFor:
    def test_it_returns_at_once_when_in_sync(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        clock = Clock()
        got = status_view.wait_for(_resolved(db), 5, sleep=clock.sleep, clock=clock.monotonic)
        assert got.verdict == "in-sync" and clock.sleeps == []

    @pytest.mark.parametrize("which", ["no-daemon", "rejected"])
    def test_it_returns_at_once_for_the_other_terminal_verdicts(
        self, run, plain, db, live_daemon, which
    ):
        if which == "rejected":
            sid, gen = bump(run, db)
            live_daemon(db, applied=(sid, gen - 1), rejected=(sid, gen))
        clock = Clock()
        got = status_view.wait_for(_resolved(db), 5, sleep=clock.sleep, clock=clock.monotonic)
        assert got.verdict == which and clock.sleeps == []

    def test_it_polls_every_two_tenths_of_a_second_to_the_deadline(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        clock = Clock()
        got = status_view.wait_for(_resolved(db), 1.0, sleep=clock.sleep, clock=clock.monotonic)
        assert got.verdict == "behind"
        assert sum(clock.sleeps) == pytest.approx(1.0)
        assert all(0 < pause <= 0.2 + 1e-9 for pause in clock.sleeps)
        assert clock.sleeps[:5] == pytest.approx([0.2] * 5)

    def test_the_last_sleep_is_shortened_to_the_deadline(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        clock = Clock()
        status_view.wait_for(_resolved(db), 0.5, sleep=clock.sleep, clock=clock.monotonic)
        assert clock.sleeps == pytest.approx([0.2, 0.2, 0.1])

    def test_zero_looks_exactly_once(self, run, plain, db, live_daemon, monkeypatch):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        looks = []
        real = status_view.collect
        monkeypatch.setattr(status_view, "collect", lambda r: (looks.append(1), real(r))[1])
        clock = Clock()
        got = status_view.wait_for(_resolved(db), 0, sleep=clock.sleep, clock=clock.monotonic)
        assert got.verdict == "behind" and len(looks) == 1 and clock.sleeps == []

    def test_applying_becomes_in_sync(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen - 1), target=(sid, gen))
        clock = Clock()
        steps = []

        def sleep(seconds):
            clock.sleep(seconds)
            steps.append(len(steps))
            if len(steps) == 3:
                daemon.set(applied=(sid, gen), target=None)

        got = status_view.wait_for(_resolved(db), 10, sleep=sleep, clock=clock.monotonic)
        assert got.verdict == "in-sync"
        assert len(steps) == 3  # it noticed on the very next look

    def test_a_write_landing_mid_wait_changes_the_outcome(self, run, plain, db, live_daemon):
        """The stored key is re-read every time: a second write makes the
        daemon's rejection (of the NEW generation) count; a cached key would
        wait out the clock."""
        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen - 1))
        clock = Clock()
        landed = []

        def sleep(seconds):
            clock.sleep(seconds)
            if not landed:
                landed.append(bump(run, db, "20m"))
                daemon.set(rejected=landed[0])

        got = status_view.wait_for(_resolved(db), 30, sleep=sleep, clock=clock.monotonic)
        assert landed[0] == (sid, gen + 1)
        assert got.stored.generation == gen + 1
        assert got.verdict == "rejected"
        assert sum(clock.sleeps) < 1

    def test_a_write_landing_mid_wait_can_also_move_the_goalposts(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen))  # in sync with the first write...
        clock = Clock()
        # ...but we start from a state that is not terminal: behind by one.
        daemon.set(applied=(sid, gen - 1))
        seen = []

        def sleep(seconds):
            clock.sleep(seconds)
            if not seen:
                seen.append(bump(run, db, "20m"))
                daemon.set(applied=seen[0])

        got = status_view.wait_for(_resolved(db), 30, sleep=sleep, clock=clock.monotonic)
        assert got.verdict == "in-sync" and got.stored.generation == gen + 1

    def test_it_returns_the_last_status_on_timeout(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid, gen))
        clock = Clock()
        got = status_view.wait_for(_resolved(db), 0.4, sleep=clock.sleep, clock=clock.monotonic)
        assert got.verdict == "applying"

    def test_it_holds_no_database_handle_between_looks(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        from zfsbackup.config.store import db as store_db

        clock = Clock()

        def sleep(seconds):
            assert store_db._ENGINES == {}, "an engine stayed cached while waiting"
            clock.sleep(seconds)

        status_view.wait_for(_resolved(db), 0.4, sleep=sleep, clock=clock.monotonic)

    def test_a_probe_error_during_the_wait_ends_it(self, run, plain, db, live_daemon, monkeypatch):
        live_daemon(db, applied=None)
        calls = []

        def probe(resolved):
            calls.append(1)
            if len(calls) > 1:
                raise daemonlock.DaemonLockError(errno.EIO, "gone bad")
            return daemonlock.DaemonInfo(
                pid=os.getpid(), generation=None, start=procident.start_token(os.getpid())
            )

        monkeypatch.setattr(daemonlock, "probe", probe)
        with pytest.raises(cli_errors.CliError, match="could not check whether a daemon"):
            status_view.wait_for(_resolved(db), 5, sleep=lambda s: None, clock=Clock().monotonic)


# --------------------------------------------------------------------------
# deferred: a retryable refusal (`deferred=` / `deferred_reason=`)
# --------------------------------------------------------------------------

REASON = "the dataset 'tank/new' does not exist"
POLL = daemonlock.RELOAD_POLL_SECONDS


def deferred_text(gen, reason=REASON):
    return (
        f"daemon has not applied generation {gen} yet: {reason or 'see the daemon log'}; "
        f"it re-checks every poll ({POLL} s)"
    )


class TestDeferred:
    def test_the_poll_interval_in_the_text_is_ten_seconds(self):
        assert POLL == 10

    def test_the_verdict_text_with_a_reason(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        assert verdict(run, db) == "deferred"
        assert message(run, db) == (
            f"daemon has not applied generation {gen} yet: {REASON}; it re-checks every poll (10 s)"
        )

    def test_the_verdict_text_without_a_reason(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen))
        assert verdict(run, db) == "deferred"
        assert message(run, db) == (
            f"daemon has not applied generation {gen} yet: see the daemon log; "
            "it re-checks every poll (10 s)"
        )

    def test_it_is_a_verdict_in_its_own_right_not_behind_or_rejected(
        self, run, plain, db, live_daemon
    ):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        assert verdict(run, db) not in ("behind", "rejected", "applying")

    # -- precedence: applied, target, rejected beat it; another key is behind

    def test_applied_beats_deferred(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen), deferred=(sid, gen), deferred_reason=REASON)
        assert verdict(run, db) == "in-sync"

    def test_target_beats_deferred(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid, gen), deferred=(sid, gen))
        assert verdict(run, db) == "applying"

    def test_rejected_beats_deferred(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), rejected=(sid, gen), deferred=(sid, gen))
        assert verdict(run, db) == "rejected"

    @pytest.mark.parametrize(
        "key",
        [lambda sid, gen: (sid, gen - 1), lambda sid, gen: (sid, gen + 1),
         lambda sid, gen: (sid + 1, gen)],
        ids=["older-generation", "newer-generation", "other-store"],
    )
    def test_deferred_for_another_key_is_behind(self, run, plain, db, live_daemon, key):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=key(sid, gen), deferred_reason=REASON)
        assert verdict(run, db) == "behind"
        assert "has not applied" not in message(run, db)

    # -- rows -------------------------------------------------------------

    def test_the_text_rows_appear_only_when_set(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen - 1))
        result = status(run, db)
        out = result.stdout
        assert result.exit_code == 0 and "rejected: -" in out.splitlines()
        assert "deferred:" not in out and "reason:" not in out
        daemon.set(deferred=(sid, gen))                      # no reason
        result = status(run, db)
        out = result.stdout
        assert result.exit_code == 0
        assert f"deferred: store {sid}, generation {gen}" in out.splitlines()
        assert "reason:" not in out
        daemon.set(deferred_reason=REASON)
        assert status(run, db).stdout == (
            f"database: {_resolved(db).resolved_path}\n"
            f"stored:   store {sid}, generation {gen}\n"
            f"daemon:   pid {os.getpid()} (reload-capable: yes)\n"
            f"applied:  store {sid}, generation {gen - 1}\n"
            "target:   -\n"
            "rejected: -\n"
            f"deferred: store {sid}, generation {gen}\n"
            f"reason:   {REASON}\n"
            f"status:   {deferred_text(gen)}\n"
        )

    def test_clearing_deferred_removes_its_reason_row_too(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        daemon.set(deferred=None, deferred_reason=None)
        result = status(run, db)
        out = result.stdout
        assert result.exit_code == 0 and "rejected: -" in out.splitlines()
        assert "deferred:" not in out and "reason:" not in out

    def test_a_legacy_daemon_has_no_deferred_rows(self, run, plain, db, live_daemon):
        live_daemon(db, generation=store_key(db)[1], start=None)
        result = status(run, db)
        out = result.stdout
        assert result.exit_code == 0 and "applied:  generation" in out
        assert "deferred:" not in out and "reason:" not in out

    # -- JSON ---------------------------------------------------------------

    def test_json_carries_the_key_and_the_reason(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        doc = as_json(status(run, db, "--json"))
        assert doc["daemon"]["deferred"] == {"store_id": sid, "generation": gen}
        assert doc["daemon"]["deferred_reason"] == REASON
        assert doc["verdict"] == "deferred" and doc["message"] == deferred_text(gen)

    def test_json_without_a_reason_has_a_null_reason(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen))
        daemon = as_json(status(run, db, "--json"))["daemon"]
        assert daemon["deferred"] == {"store_id": sid, "generation": gen}
        assert daemon["deferred_reason"] is None

    def test_json_without_deferred_has_nulls(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        daemon = as_json(status(run, db, "--json"))["daemon"]
        assert daemon["deferred"] is None and daemon["deferred_reason"] is None

    # -- --wait ---------------------------------------------------------------

    def test_wait_while_deferred_is_75_at_the_deadline_never_1(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        started = time.monotonic()
        result = status(run, db, "--wait", "0.6")
        assert result.exit_code == 75, (result.stdout, result.stderr)
        assert time.monotonic() - started >= 0.5            # it kept polling
        assert result.stderr.startswith(
            f"error: the daemon is not yet in sync after 0.6s: {deferred_text(gen)}"
        )
        assert result.stdout.splitlines()[-1] == f"status:   {deferred_text(gen)}"

    def test_wait_while_deferred_with_json_is_75_too(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen))
        result = status(run, db, "--wait", "0.3", "--json")
        assert result.exit_code == 75 and as_json(result)["verdict"] == "deferred"

    def test_wait_applied_mid_wait_is_0(self, run, plain, db, live_daemon):
        import threading

        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        timer = threading.Timer(0.5, lambda: daemon.set(applied=(sid, gen), deferred=None))
        timer.start()
        try:
            started = time.monotonic()
            result = status(run, db, "--wait", "30")
        finally:
            timer.cancel()
            timer.join()
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert 0.3 < time.monotonic() - started < 15
        assert result.stdout.splitlines()[-1] == "status:   in sync"

    def test_wait_later_rejected_is_1(self, run, plain, db, live_daemon):
        import threading

        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        timer = threading.Timer(0.5, lambda: daemon.set(rejected=(sid, gen), deferred=None))
        timer.start()
        try:
            result = status(run, db, "--wait", "30")
        finally:
            timer.cancel()
            timer.join()
        assert result.exit_code == 1, (result.stdout, result.stderr)
        assert "rejected the stored configuration" in result.stderr

    def test_wait_for_keeps_polling_through_deferred_and_returns_the_last_status(
        self, run, plain, db, live_daemon
    ):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        clock = Clock()
        got = status_view.wait_for(_resolved(db), 1.0, sleep=clock.sleep, clock=clock.monotonic)
        assert got.verdict == "deferred" and got.message == deferred_text(gen)
        assert sum(clock.sleeps) == pytest.approx(1.0)
        assert not got.will_not_catch_up

    def test_wait_for_sees_deferred_become_in_sync(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        daemon = live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        clock = Clock()
        steps = []

        def sleep(seconds):
            clock.sleep(seconds)
            steps.append(1)
            if len(steps) == 2:
                daemon.set(applied=(sid, gen), deferred=None)

        got = status_view.wait_for(_resolved(db), 10, sleep=sleep, clock=clock.monotonic)
        assert got.verdict == "in-sync" and len(steps) == 2   # noticed on the next look

    def test_without_wait_deferred_is_exit_0(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen))
        assert status(run, db).exit_code == 0
        assert status(run, db, "--json").exit_code == 0


# --------------------------------------------------------------------------
# status never signals
# --------------------------------------------------------------------------


class _SignalTrap:
    def __init__(self, monkeypatch) -> None:
        self.monkeypatch = monkeypatch
        self.sent = []

    @contextlib.contextmanager
    def armed(self):
        """From now on any signal but 0 (an existence probe) fails the test, and
        so does a `request_reload` (the only signalling entry point the CLI has).
        Armed only inside the `with` (the test's own setup writes legitimately
        announce, and fixture teardown kills helper processes)."""
        real_kill = os.kill

        def kill(pid, sig):
            if sig == 0:
                return real_kill(pid, 0)
            self.sent.append((pid, sig))
            raise AssertionError(f"status sent signal {sig} to {pid}")

        def forbidden(name):
            def fn(*a, **k):
                self.sent.append((name, a))
                raise AssertionError(f"status called {name}")

            return fn

        with self.monkeypatch.context() as mp:
            mp.setattr(os, "kill", kill)
            mp.setattr(os, "killpg", forbidden("killpg"))
            mp.setattr(signal, "raise_signal", forbidden("raise_signal"))
            if hasattr(signal, "pidfd_send_signal"):
                mp.setattr(signal, "pidfd_send_signal", forbidden("pidfd_send_signal"))
            if hasattr(os, "pidfd_open"):
                mp.setattr(os, "pidfd_open", forbidden("pidfd_open"))
            mp.setattr(daemonlock, "request_reload", forbidden("request_reload"))
            yield self


@pytest.fixture
def no_signals(monkeypatch):
    return _SignalTrap(monkeypatch)


class TestStatusNeverSignals:
    @pytest.mark.parametrize(
        "daemon",
        [
            dict(applied="key"),
            dict(applied="old"),
            dict(applied="old", target="key"),
            dict(applied="old", rejected="key"),
            dict(generation=0, start=None),
            dict(pid="dead"),
            dict(start=""),
            dict(raw=b""),
        ],
        ids=["in-sync", "behind", "applying", "rejected", "legacy", "gone", "no-token", "empty"],
    )
    @pytest.mark.parametrize("args", [(), ("--json",), ("--wait", "0.3")], ids=["text", "json", "wait"])
    def test_no_signal_of_any_kind(self, run, plain, db, live_daemon, no_signals, daemon, args):
        sid, gen = bump(run, db)
        kw = dict(daemon)
        if kw.get("pid") == "dead":
            kw["pid"] = dead_pid()
        for name, value in list(kw.items()):
            if value == "key":
                kw[name] = (sid, gen)
            elif value == "old":
                kw[name] = (sid, gen - 1)
        live_daemon(db, **kw)
        with no_signals.armed():
            status(run, db, *args)
        assert no_signals.sent == []

    def test_a_real_process_holding_the_lock_is_left_alone(self, run, plain, db, daemon_lock_holder, no_signals):
        holder = daemon_lock_holder(db, generation=0)
        with no_signals.armed():
            result = status(run, db)
        assert result.exit_code == 0
        assert f"pid {holder.pid}" in result.stdout
        assert holder.proc.poll() is None and no_signals.sent == []

    def test_status_does_not_touch_the_pid_file_or_the_database(self, run, plain, db, live_daemon):
        daemon = live_daemon(db, applied=store_key(db))
        pid_file = daemonlock.lock_path(_resolved(db))
        before = (pid_file.read_bytes(), db_state(db))
        status(run, db)
        status(run, db, "--json")
        status(run, db, "--wait", "0.2")
        assert (pid_file.read_bytes(), db_state(db)) == before

    def test_status_takes_no_writer_lock(self, run, plain, db):
        from zfsbackup.config.store import writer_lock

        with writer_lock(_resolved(db)):
            assert status(run, db).exit_code == 0

    def test_status_works_when_the_daemon_holds_the_lock_exclusively(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        assert status(run, db).exit_code == 0
        assert status(run, db).exit_code == 0  # and again: probing leaves no shared lock behind


# --------------------------------------------------------------------------
# S-7b: worker outcomes from `<db>.status/`
# --------------------------------------------------------------------------

SEND = "remote-backup-worker"
SNAP = "snapshot-worker"
PRUNE = "pruning-worker"
UPDATED = r"updated \d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ \(\d+[smhd] ago\)"


@pytest.fixture
def live(run, plain, db, live_daemon):
    """A plain database with a live daemon that is in sync."""
    live_daemon(db, applied=store_key(db))
    return db


def rows_of(result):
    """The text lines from the first `workers:` row up to (excluding) `status:`."""
    lines = result.stdout.splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith("workers:"))
    end = next(i for i, l in enumerate(lines) if l.startswith("status:"))
    return lines[start:end]


class TestWorkerRows:
    def test_no_status_directory_changes_nothing(self, run, live):
        text = status(run, live)
        assert "workers" not in text.stdout and text.exit_code == 0
        data = as_json(status(run, live, "--json"))
        assert "workers" not in data
        assert set(data) == {"database", "stored", "daemon", "schema_error", "verdict", "message"}

    def test_the_text_is_byte_identical_without_data(self, run, live):
        lines = status(run, live).stdout.splitlines()
        assert [l.split(":")[0] for l in lines] == [
            "database", "stored", "daemon", "applied", "target", "rejected", "status"]

    def test_an_empty_directory_has_no_rows_but_the_json_key(self, run, live):
        make_dir(live)
        assert "workers" not in status(run, live).stdout
        data = as_json(status(run, live, "--json"))
        assert data["workers"] == {"directory": str(status_dir(live)), "problems": [], "files": []}

    def test_rows_come_before_the_final_status_line(self, run, live):
        write_worker(live, SNAP, [rec("snapshot", "tank/a"), rec("snapshot", "tank/b")])
        write_worker(live, SEND, [rec("send", "tank/a", "offsite")])
        result = status(run, live)
        lines = result.stdout.splitlines()
        assert lines[-1] == "status:   in sync"
        rows = rows_of(result)
        assert len(rows) == 2
        # the files are listed in name order; the first row carries the label, the next none
        assert re.fullmatch(rf"workers:  {SEND}: {UPDATED}, 1 record, none failing", rows[0])
        assert re.fullmatch(rf"          {SNAP}: {UPDATED}, 2 records, none failing", rows[1])
        assert lines.index(rows[0]) > lines.index(next(l for l in lines if l.startswith("rejected:")))

    def test_a_failing_worker_gets_a_worst_row(self, run, live):
        write_worker(live, SEND, [
            rec("send", "tank/a", "offsite", failures=3, error="connection refused"),
            rec("send", "tank/b", "offsite"),
        ])
        rows = rows_of(status(run, live))
        assert re.fullmatch(rf"workers:  {SEND}: {UPDATED}, 2 records, 1 failing", rows[0])
        assert rows[1] == "            worst: send tank/a -> offsite, 3 failure(s) in a row: connection refused"
        assert len(rows) == 2

    def test_a_local_record_has_no_destination_arrow(self, run, live):
        write_worker(live, PRUNE, [rec("prune", "tank/a", failures=1, error="busy")])
        assert "            worst: prune tank/a, 1 failure(s) in a row: busy" in rows_of(status(run, live))

    def test_a_failure_without_an_error_text_has_no_colon(self, run, live):
        record = rec("snapshot", "tank/a", failures=2)
        del record["last_error"], record["last_error_at"]
        write_worker(live, SNAP, [record])
        assert "            worst: snapshot tank/a, 2 failure(s) in a row" in rows_of(status(run, live))

    def test_the_worst_is_the_most_failures(self, run, live):
        now = time.time()
        write_worker(live, SEND, [
            rec("send", "tank/a", "o", failures=2, error="two", error_at=now - 10),
            rec("send", "tank/b", "o", failures=5, error="five", error_at=now - 900),
            rec("send", "tank/c", "o", failures=4, error="four", error_at=now - 5),
        ])
        worst = as_json(status(run, live, "--json"))["workers"]["files"][0]["worst"]
        assert (worst["dataset"], worst["consecutive_failures"], worst["last_error"]) == ("tank/b", 5, "five")

    def test_a_tie_goes_to_the_latest_error(self, run, live):
        now = time.time()
        write_worker(live, SEND, [
            rec("send", "tank/a", "o", failures=3, error="old", error_at=now - 900),
            rec("send", "tank/b", "o", failures=3, error="new", error_at=now - 5),
            rec("send", "tank/c", "o", failures=3, error="mid", error_at=now - 300),
        ])
        worst = as_json(status(run, live, "--json"))["workers"]["files"][0]["worst"]
        assert worst["dataset"] == "tank/b"

    def test_an_error_result_with_zero_failures_counts_as_one(self, run, live):
        write_worker(live, SEND, [rec("send", "tank/a", "o", result="error", failures=0, error="x")])
        (entry,) = as_json(status(run, live, "--json"))["workers"]["files"]
        assert entry["failing"] == 1 and entry["worst"]["consecutive_failures"] == 1

    def test_successes_are_not_failing(self, run, live):
        write_worker(live, SEND, [rec("send", "tank/a", "o", error="old", error_at=time.time() - 99,
                                      failures=0, result="ok")])
        (entry,) = as_json(status(run, live, "--json"))["workers"]["files"]
        assert entry["failing"] == 0 and entry["worst"] is None
        assert len(rows_of(status(run, live))) == 1

    def test_a_heartbeat_file_with_no_records_is_a_row_with_zero_records(self, run, live):
        write_worker(live, SNAP, [])
        rows = rows_of(status(run, live))
        assert len(rows) == 1
        assert re.fullmatch(rf"workers:  {SNAP}: {UPDATED}, 0 records, none failing", rows[0])
        (entry,) = as_json(status(run, live, "--json"))["workers"]["files"]
        assert entry["records"] == 0 and entry["failing"] == 0 and entry["worst"] is None
        assert entry["stale"] is False

    def test_a_discover_failure_is_the_worst(self, run, live):
        write_worker(live, SEND, [
            rec("send", "tank/a", "offsite", failures=2, error="x"),
            rec("discover", "bk", failures=3, error="cannot list the received datasets: pool busy"),
        ])
        rows = rows_of(status(run, live))
        assert rows[1] == ("            worst: discover bk, 3 failure(s) in a row: "
                           "cannot list the received datasets: pool busy")
        worst = as_json(status(run, live, "--json"))["workers"]["files"][0]["worst"]
        assert (worst["kind"], worst["dataset"], worst["destination"]) == ("discover", "bk", None)

    def test_a_mount_point_status_directory_is_a_problem_row(self, run, live, monkeypatch):
        write_worker(live, SNAP, [rec("snapshot", "tank/a")])
        directory = status_dir(live)
        real = os.stat

        def other_device(path, *a, **k):
            result = real(path, *a, **k)
            if not a and not k and str(path) == str(directory.parent):
                fields = list(result)[:10]
                fields[2] = result.st_dev + 1
                return os.stat_result(fields)
            return result

        monkeypatch.setattr(os, "stat", other_device)
        result = status(run, live)
        assert result.exit_code == 0
        assert "workers:  problem: " in result.stdout and "is a mount point" in result.stdout
        assert SNAP not in result.stdout

    def test_a_file_without_an_update_time_prints_a_dash(self, run, live):
        write_worker(live, SNAP, [rec("snapshot", "tank/a")], updated_at=None)
        assert any("updated -," in row for row in rows_of(status(run, live)))

    def test_problem_rows(self, run, live):
        directory = make_dir(live)
        bad = directory / "pruning-worker.json"
        bad.write_text("{ nope")
        os.chmod(bad, 0o640)
        rows = rows_of(status(run, live))
        assert len(rows) == 1 and rows[0].startswith("workers:  problem: ")
        assert "not valid JSON" in rows[0] and "pruning-worker.json" in rows[0]

    def test_problem_rows_follow_the_worker_rows(self, run, live):
        write_worker(live, SNAP, [rec("snapshot", "tank/a")])
        bad = status_dir(live) / "pruning-worker.json"
        bad.write_text("{ nope")
        os.chmod(bad, 0o640)
        rows = rows_of(status(run, live))
        assert rows[0].startswith(f"workers:  {SNAP}:")
        assert rows[1].startswith("          problem: ") and "pruning-worker.json" in rows[1]
        data = as_json(status(run, live, "--json"))["workers"]
        assert len(data["problems"]) == 1 and [f["worker"] for f in data["files"]] == [SNAP]

    def test_a_problem_is_one_line(self):
        workers = status_view.Workers("/d", True, (), ("/d/x.json broke\n  badly\n",))
        rows = status_view._worker_lines(workers, 0.0)
        assert rows == [("workers", "problem: /d/x.json broke badly")]

    def test_a_group_writable_directory_is_a_problem_row_not_an_error(self, run, live):
        write_worker(live, SNAP, [rec("snapshot", "tank/a")])
        os.chmod(status_dir(live), 0o770)
        try:
            result = status(run, live)
        finally:
            os.chmod(status_dir(live), 0o750)
        assert result.exit_code == 0
        assert "workers:  problem: " in result.stdout and "writable by group or others" in result.stdout

    def test_the_json_shape(self, run, live):
        now = time.time()
        write_worker(live, SEND, [
            rec("send", "tank/a", "offsite", failures=3, error="refused", error_at=now - 120),
            rec("send", "tank/b", "offsite"), rec("scrub", "tank/b"),
        ], pid=LIVE_PID, started_at=now - 7200, updated_at=now - 30)
        data = as_json(status(run, live, "--json"))
        assert list(data["workers"]) == ["directory", "problems", "files"]
        assert data["workers"]["directory"] == str(status_dir(live))
        assert data["workers"]["problems"] == []
        (entry,) = data["workers"]["files"]
        assert list(entry) == ["worker", "stale", "stale_reason", "pid", "started_at", "updated_at",
                               "records", "failing", "worst"]
        assert entry["worker"] == SEND and entry["pid"] == LIVE_PID
        assert entry["stale"] is False and entry["stale_reason"] is None
        assert entry["records"] == 3 and entry["failing"] == 1
        assert entry["started_at"] == iso(now - 7200) and entry["updated_at"] == iso(now - 30)
        assert entry["worst"] == {
            "kind": "send", "dataset": "tank/a", "destination": "offsite",
            "consecutive_failures": 3, "last_error": "refused", "last_error_at": iso(now - 120)}

    def test_null_fields_in_the_json(self, run, live):
        write_worker(live, SNAP, [rec("snapshot", "tank/a")], pid=None, started_at=None, updated_at=None)
        (entry,) = as_json(status(run, live, "--json"))["workers"]["files"]
        assert entry["pid"] is None and entry["started_at"] is None and entry["updated_at"] is None
        assert entry["worst"] is None

    def test_the_existing_keys_are_untouched(self, run, live):
        before = as_json(status(run, live, "--json"))
        write_worker(live, SEND, [rec("send", "tank/a", "offsite", failures=9)])
        after = as_json(status(run, live, "--json"))
        assert {k: v for k, v in after.items() if k != "workers"} == before

    def test_json_is_still_the_only_thing_on_stdout(self, run, live):
        write_worker(live, SEND, [rec("send", "tank/a", "offsite", failures=9)])
        result = status(run, live, "--json")
        assert result.stderr == "" and result.stdout.endswith("}\n")
        json.loads(result.stdout)

    @pytest.mark.parametrize("args", [(), ("--json",)])
    def test_exit_code_and_verdict_never_move(self, run, plain, db, live_daemon, args):
        write_worker(db, SEND, [rec("send", "tank/a", "offsite", failures=50)])
        assert status(run, db, *args).exit_code == 0                      # no daemon
        live_daemon(db, applied=store_key(db))
        result = status(run, db, *args)
        assert result.exit_code == 0
        if args:
            assert as_json(result)["verdict"] == "in-sync"
        else:
            assert result.stdout.splitlines()[-1] == "status:   in sync"

    def test_a_behind_daemon_is_still_exit_0_with_rows(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        write_worker(db, SEND, [rec("send", "tank/a", "offsite", failures=9)])
        result = status(run, db)
        assert result.exit_code == 0 and "workers:  " in result.stdout
        assert as_json(status(run, db, "--json"))["verdict"] == "behind"
        assert result.stdout.splitlines()[-1].startswith("status:   daemon on generation")

    def test_wait_is_unaffected(self, run, plain, db, live_daemon):
        write_worker(db, SEND, [rec("send", "tank/a", "offsite", failures=9)])
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid, gen))
        timeout = status(run, db, "--wait", "0")
        assert timeout.exit_code == 75
        assert "workers:  " in timeout.stdout                              # printed before the exit
        assert as_json(status(run, db, "--json"))["verdict"] == "applying"

    def test_wait_in_sync_is_0_with_rows(self, run, live):
        write_worker(live, SEND, [rec("send", "tank/a", "offsite", failures=9)])
        result = status(run, live, "--wait", "1")
        assert result.exit_code == 0 and "workers:  " in result.stdout

    def test_wait_rejected_is_still_1(self, run, plain, db, live_daemon):
        write_worker(db, SEND, [rec("send", "tank/a", "offsite")])
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), rejected=(sid, gen))
        assert status(run, db, "--wait", "1").exit_code == 1

    def test_the_files_are_read_after_the_wait(self, run, plain, db, live_daemon, monkeypatch):
        calls = []
        real = status_view.collect_workers
        monkeypatch.setattr(status_view, "collect_workers",
                            lambda *a, **k: calls.append("workers") or real(*a, **k))
        real_wait = status_view.wait_for
        monkeypatch.setattr(status_view, "wait_for",
                            lambda *a, **k: calls.append("wait") or real_wait(*a, **k))
        live_daemon(db, applied=store_key(db))
        status(run, db, "--wait", "1")
        assert calls == ["wait", "workers"]

    def test_a_reader_that_blows_up_is_a_problem_row_not_a_crash(self, run, live, monkeypatch):
        make_dir(live)

        def boom(*a, **k):
            raise RuntimeError("kaboom")

        monkeypatch.setattr(health, "read_status_dir", boom)
        result = status(run, live)
        assert result.exit_code == 0
        assert "workers:  problem: " in result.stdout and "kaboom" in result.stdout

    def test_status_writes_nothing_into_the_status_directory(self, run, live):
        path = write_worker(live, SEND, [rec("send", "tank/a", "offsite", failures=2)])
        before = (path.read_bytes(), os.stat(path).st_mtime_ns, sorted(os.listdir(status_dir(live))))
        status(run, live)
        status(run, live, "--json")
        assert (path.read_bytes(), os.stat(path).st_mtime_ns, sorted(os.listdir(status_dir(live)))) == before


class TestWorkerStale:
    def test_without_a_daemon_every_file_is_stale(self, run, plain, db):
        write_worker(db, SEND, [rec("send", "tank/a", "offsite", failures=3, error="refused")])
        result = status(run, db)
        assert result.exit_code == 0
        rows = rows_of(result)
        assert re.fullmatch(rf"workers:  {SEND}: STALE \(no recent data\) {UPDATED}, 1 record, 1 failing", rows[0])
        assert rows[1] == "            stale: no daemon is running, so this is the last state of a stopped one"
        assert rows[2] == ("            last known worst: send tank/a -> offsite, "
                           "3 failure(s) in a row: refused")
        assert "worst:" not in "\n".join(r for r in rows if "last known worst" not in r)
        assert result.stdout.splitlines()[-1] == "status:   no running daemon"

    def test_json_without_a_daemon(self, run, plain, db):
        write_worker(db, SEND, [rec("send", "tank/a", "offsite", failures=3)])
        (entry,) = as_json(status(run, db, "--json"))["workers"]["files"]
        assert entry["stale"] is True
        assert entry["stale_reason"] == "no daemon is running, so this is the last state of a stopped one"
        assert entry["worst"]["dataset"] == "tank/a"             # still reported, as the last known

    def test_a_dead_writer_is_stale(self, run, live):
        pid = dead_pid()
        write_worker(live, SEND, [rec("send", "tank/a", "offsite", failures=3)], pid=pid)
        write_worker(live, SNAP, [rec("snapshot", "tank/a")])
        files = {f["worker"]: f for f in as_json(status(run, live, "--json"))["workers"]["files"]}
        assert files[SEND]["stale"] is True and files[SEND]["stale_reason"] == f"its writer (pid {pid}) is gone"
        assert files[SNAP]["stale"] is False and files[SNAP]["stale_reason"] is None
        rows = rows_of(status(run, live))
        assert f"workers:  {SEND}: STALE (no recent data) " in rows[0]
        assert any(r == f"            stale: its writer (pid {pid}) is gone" for r in rows)
        assert sum("STALE" in r for r in rows) == 1

    def test_a_writer_whose_liveness_is_unknown_is_not_stale_by_that(self, run, live, monkeypatch):
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: None)
        write_worker(live, SEND, [rec("send", "tank/a", "offsite")], pid=dead_pid())
        (entry,) = as_json(status(run, live, "--json"))["workers"]["files"]
        assert entry["stale"] is False

    @pytest.mark.parametrize("hours, stale", [(23, False), (25, True)])
    def test_status_uses_24_hours(self, run, live, hours, stale):
        write_worker(live, SEND, [rec("send", "tank/a", "offsite")], updated_at=time.time() - hours * 3600)
        (entry,) = as_json(status(run, live, "--json"))["workers"]["files"]
        assert entry["stale"] is stale
        if stale:
            assert entry["stale_reason"] == "not updated for 1d1h (more than 1d)"

    def test_a_missing_update_time_is_stale(self, run, live):
        write_worker(live, SEND, [rec("send", "tank/a", "offsite")], updated_at=None)
        (entry,) = as_json(status(run, live, "--json"))["workers"]["files"]
        assert entry["stale"] is True and entry["stale_reason"] == "it records no update time"

    def test_a_fresh_file_has_no_stale_row(self, run, live):
        write_worker(live, SEND, [rec("send", "tank/a", "offsite", failures=2)])
        text = status(run, live).stdout
        assert "STALE" not in text and "stale:" not in text and "last known" not in text
        assert "worst: send tank/a -> offsite" in text

    def test_a_supervisor_gone_still_counts_as_a_daemon_for_staleness_of_the_lock(self, run, plain, db, live_daemon):
        # the lock is held (a daemon exists as far as the file is concerned); only the pid decides
        live_daemon(db, pid=dead_pid(), start="tok")
        write_worker(db, SEND, [rec("send", "tank/a", "offsite")])
        (entry,) = as_json(status(run, db, "--json"))["workers"]["files"]
        assert entry["stale"] is False

    def test_collect_workers_functions_directly(self, plain, db):
        resolved = _resolved(db)
        path = write_worker(db, SEND, [rec("send", "tank/a", "offsite")])
        now = time.time()
        assert status_view.collect_workers(resolved, daemon_running=True, now=now).stale == {}
        assert list(status_view.collect_workers(resolved, daemon_running=False, now=now).stale) == [SEND]
        assert status_view.collect_workers(resolved, daemon_running=None, now=now).stale == {}
        later = status_view.collect_workers(resolved, daemon_running=True, now=now + 30 * 3600)
        assert list(later.stale) == [SEND]
        wide = status_view.collect_workers(resolved, daemon_running=True, now=now + 30 * 3600,
                                           interval=10 * 3600)
        assert wide.stale == {}                                           # 4 x 10h = 40h
        assert path.exists()


# --------------------------------------------------------------------------
# draining for an upgrade (S-8: `draining=` / `draining_reason=` in the pid file)
# --------------------------------------------------------------------------

DRAIN_AT = 1_790_000_000
DRAIN_WHY = "zfsbackup upgraded (1.0.0 -> 1.1.0); draining the remote worker, then the API worker, then exiting 76"
UPGRADE_DEFERRED_WHY = "upgrade draining: applied after the daemon restarts, not re-checked"


def after_restart_text(gen):
    return (
        f"daemon is draining for an upgrade and does not reload: generation {gen} "
        "will be applied after the restart, not before"
    )


class TestDraining:
    def drain_kw(self):
        return dict(draining=DRAIN_AT, draining_reason=DRAIN_WHY)

    # -- verdicts ----------------------------------------------------------

    def test_draining_with_the_stored_key_ahead_and_no_deferral_is_behind_after_the_restart(
        self, run, plain, db, live_daemon
    ):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), **self.drain_kw())
        assert verdict(run, db) == "behind"
        msg = message(run, db)
        assert msg == after_restart_text(gen)
        assert "after the restart" in msg and "re-check" not in msg and "daemon log" not in msg

    def test_draining_with_a_deferral_for_the_stored_key_is_deferred(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen),
                    deferred_reason=UPGRADE_DEFERRED_WHY, **self.drain_kw())
        assert verdict(run, db) == "deferred"
        msg = message(run, db)
        assert msg == after_restart_text(gen) and "re-checks every poll" not in msg

    def test_draining_with_a_deferral_for_another_key_is_behind(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen - 1),
                    deferred_reason=UPGRADE_DEFERRED_WHY, **self.drain_kw())
        assert verdict(run, db) == "behind"

    def test_draining_with_applied_equal_to_stored_is_in_sync(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db), **self.drain_kw())
        assert verdict(run, db) == "in-sync"
        assert message(run, db) == "in sync"

    def test_draining_with_a_target_is_applying(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), target=(sid, gen), **self.drain_kw())
        assert verdict(run, db) == "applying"

    def test_draining_with_the_key_rejected_is_still_rejected(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), rejected=(sid, gen), **self.drain_kw())
        assert verdict(run, db) == "rejected"

    def test_not_draining_a_deferral_still_says_it_rechecks_every_poll(
        self, run, plain, db, live_daemon
    ):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen), deferred_reason=REASON)
        assert message(run, db) == deferred_text(gen)
        assert "after the restart" not in message(run, db)

    def test_not_draining_behind_keeps_its_old_wording(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        assert SEE_LOG in message(run, db) and "after the restart" not in message(run, db)

    # -- text ---------------------------------------------------------------

    def test_the_text_has_a_draining_row_before_the_final_status_line(
        self, run, plain, db, live_daemon
    ):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), **self.drain_kw())
        lines = status(run, db).stdout.splitlines()
        assert lines[-1] == f"status:   {after_restart_text(gen)}"
        row = lines[-2]
        since = datetime_utc(DRAIN_AT)
        assert row.startswith(f"draining: since {since} (") and row.endswith(f"): {DRAIN_WHY}")
        assert re.search(r"\((\d+d) ago\)", row), row          # 2026 vs 2026-09: days ago

    def test_the_row_is_absent_when_not_draining(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        assert "draining" not in status(run, db).stdout

    def test_the_row_disappears_when_the_drain_ends(self, run, plain, db, live_daemon):
        daemon = live_daemon(db, applied=store_key(db), **self.drain_kw())
        assert "draining:" in status(run, db).stdout
        daemon.set(draining=None, draining_reason=None)
        assert "draining" not in status(run, db).stdout

    def test_a_drain_without_a_reason_has_no_trailing_colon(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db), draining=DRAIN_AT)
        row = [l for l in status(run, db).stdout.splitlines() if l.startswith("draining:")][0]
        assert not row.endswith(":") and "): " not in row

    def test_the_age_is_rendered_against_the_given_clock(self):
        info = daemonlock.DaemonInfo(
            pid=1, generation=0, start="t", applied=(1, 0), draining=DRAIN_AT, draining_reason="why")
        st = status_view.Status(
            "db", SimpleNamespace(store_id=1, generation=0), info, "in-sync", "in sync")
        out = status_view.render_text(st, now=DRAIN_AT + 125)
        assert f"draining: since {datetime_utc(DRAIN_AT)} (2m ago): why" in out
        assert f"(7s ago)" in status_view.render_text(st, now=DRAIN_AT + 7)
        assert "(0s ago)" in status_view.render_text(st, now=DRAIN_AT - 50)       # never negative

    # -- JSON ---------------------------------------------------------------

    def test_json_has_the_two_keys_only_while_draining(self, run, plain, db, live_daemon):
        daemon = live_daemon(db, applied=store_key(db), **self.drain_kw())
        doc = as_json(status(run, db, "--json"))["daemon"]
        assert doc["draining"] == DRAIN_AT and isinstance(doc["draining"], int)
        assert doc["draining_reason"] == DRAIN_WHY
        daemon.set(draining=None, draining_reason=None)
        doc = as_json(status(run, db, "--json"))["daemon"]
        assert "draining" not in doc and "draining_reason" not in doc

    def test_json_reason_is_null_when_absent(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db), draining=DRAIN_AT)
        doc = as_json(status(run, db, "--json"))["daemon"]
        assert doc["draining"] == DRAIN_AT and doc["draining_reason"] is None

    def test_the_existing_exact_shape_is_unchanged_when_not_draining(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db))
        assert set(as_json(status(run, db, "--json"))["daemon"]) == {
            "pid", "reload_capable", "supervisor_alive", "generation", "applied", "target",
            "rejected", "deferred", "deferred_reason",
        }

    # -- --wait ---------------------------------------------------------------

    def test_wait_under_a_drain_is_75_with_the_restart_explanation(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), **self.drain_kw())
        started = time.monotonic()
        result = status(run, db, "--wait", "0.6")
        assert result.exit_code == 75, (result.stdout, result.stderr)
        assert time.monotonic() - started >= 0.5
        assert "after the restart" in result.stderr
        assert "see the daemon log" not in result.stderr and "daemon log" not in result.stderr
        assert result.stderr.startswith(
            f"error: the daemon is not yet in sync after 0.6s: {after_restart_text(gen)}"
        )

    def test_wait_under_a_drain_with_a_deferral_is_75_never_1(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), deferred=(sid, gen),
                    deferred_reason=UPGRADE_DEFERRED_WHY, **self.drain_kw())
        result = status(run, db, "--wait", "0.4")
        assert result.exit_code == 75
        assert "after the restart" in result.stderr and "daemon log" not in result.stderr

    def test_wait_under_a_drain_with_json_is_75_too(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1), **self.drain_kw())
        result = status(run, db, "--wait", "0.3", "--json")
        assert result.exit_code == 75
        assert as_json(result)["verdict"] == "behind"

    def test_wait_under_a_drain_in_sync_is_0(self, run, plain, db, live_daemon):
        live_daemon(db, applied=store_key(db), **self.drain_kw())
        assert status(run, db, "--wait", "5").exit_code == 0

    def test_the_wait_exit_code_for_a_draining_behind_daemon_is_75_not_1(self):
        modern = daemonlock.DaemonInfo(pid=1, generation=0, start="t", draining=DRAIN_AT)
        got = status_view.wait_exit_code(status_view.Status("db", None, modern, "behind", "m"))
        assert got == 75

    def test_a_plain_behind_wait_still_points_at_the_daemon_log(self, run, plain, db, live_daemon):
        sid, gen = bump(run, db)
        live_daemon(db, applied=(sid, gen - 1))
        result = status(run, db, "--wait", "0.3")
        assert result.exit_code == 75 and "daemon log" in result.stderr


def datetime_utc(ts):
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

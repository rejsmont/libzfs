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
"""

import contextlib
import errno
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

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
from zfsbackup import daemonlock, procident
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
        ]

    def test_json_is_the_only_thing_on_stdout(self, run, plain, db):
        result = status(run, db, "--json")
        assert result.stdout.startswith("{") and result.stdout.endswith("}\n")
        assert result.stderr == ""

    def test_the_verdict_values_are_the_documented_ones(self):
        assert {
            status_view.IN_SYNC, status_view.APPLYING, status_view.REJECTED,
            status_view.BEHIND, status_view.SUPERVISOR_GONE, status_view.NO_DAEMON,
        } == {"in-sync", "applying", "rejected", "behind", "supervisor-gone", "no-daemon"}


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

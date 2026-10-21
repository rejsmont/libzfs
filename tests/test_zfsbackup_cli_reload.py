"""The CLI side of live reload (plan item 19.1): after EVERY committed write
`zfsbackup-config` asks the running daemon to reload (`daemonlock.request_reload`,
exactly once), waits briefly for the daemon to name the new key in its pid file,
and says what happened. The exit code never changes: the write is already durable.

Sections:

- ``TestStoreIdConflictMessage``  a replaced database file: wording and exit 75.
- ``TestNoticeWording``           one test per row of the outcome table.
- ``TestEveryOutcomeIsExitZero``  each row through a real command: exit 0, DB written.
- ``TestCallMatrix``              `request_reload` is called exactly once after every
                                  committed write, with the written key and the DB
                                  already at it; and never after anything else.
- ``TestWriterLockIsReleasedDuringTheWait``  nothing waits under the writer lock.
- ``TestNothingPooledAcrossTheWait``  `_await_answer` / `notify_daemon` close every pooled
                                  connection first, on every command's path; a failing
                                  release never fails the write.
- ``TestAckLoop``                 `_await_answer`: what stops it and what does not.
- ``TestReloadWaitOption``        the global `--reload-wait`.
- ``TestRealLockHolderOnLinux``   a genuine v2 lock holder is signalled, a forged pid
                                  is not (Linux only: the lock table is `/proc/locks`).
- ``TestRealDaemonEndToEnd``      a real `python -m zfsbackup.daemon`: `set`, then
                                  `status --wait`.
- ``TestEnrollReloadMatrix`` / ``TestRotateReloadMatrix``
                                  `enroll` and `identity rotate` (TLS plan M13/M14c): the
                                  once-only rule across their TWO possible commits, against
                                  real API servers (`tests/_enroll_support.py`).

Everything here runs against a real SQLite database through the real CLI; only
`request_reload`/`probe` (the daemon side) are replaced, by in-process fakes or by
`FakeDaemon`, a lock held in this process with a hand-written pid file. Nothing
signals a process the test did not start.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests
across this file, `test_zfsbackup_cli_status.py` and the CLI files that exercise
the notice): see the 19.1/19.2 hand-off report.
"""

import errno
import json
import math
import os
import signal
import subprocess
import sys
import textwrap
import time
import types
from pathlib import Path

import click
import pytest

from _cli_support import (
    ANCHOR,
    host_client_id,
    seed_client_id,
    stored,
    RICH_YAML,
    UNVERIFIED_LINUX_ONLY,
    db,
    db_state,
    fake_daemon,
    live_daemon,
    fake_zfs,
    generation,
    isolated,
    make_db,
    run,
    scratch_tmp,
    store_key,
    stub,
    unsignalable,
    write_yaml,
)
from _enroll_support import (
    CID,
    Reply,
    client,
    closed_port,
    do_enroll,
    make_foreign_token,
    make_server,
    reloads,
    zfs_stub,
)
from zfsbackup import daemonlock, identity
from zfsbackup.cli import enroll as enroll_cli
from zfsbackup.cli import errors as cli_errors
from zfsbackup.cli import main as cli_main
from zfsbackup.cli import write as cli_write
from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    StoreIdConflict,
    dispose_all,
    resolve_config_path,
    writer_lock,
)

REPO = Path(__file__).resolve().parent.parent
POLL = daemonlock.RELOAD_POLL_SECONDS


def _resolved(database: Path):
    return resolve_config_path(str(database))


@pytest.fixture
def rich(run, db):
    return make_db(run, db)


@pytest.fixture
def resolved(db):
    db.parent.mkdir(parents=True, exist_ok=True)
    return _resolved(db)


def info(*, pid=4321, generation=None, start="tok", applied=None, target=None, rejected=None,
         deferred=None, deferred_reason=None):
    return daemonlock.DaemonInfo(
        pid=pid, generation=generation, start=start,
        applied=applied, target=target, rejected=rejected,
        deferred=deferred, deferred_reason=deferred_reason,
    )


def reloaded(monkeypatch, outcome, *, pid=4321, detail="", info_=None, code=""):
    """Make `request_reload` answer `outcome` (a ReloadRequest), whatever the pid
    file says. `code` is the request's machine-readable `reason_code`."""
    calls = []

    def fake(r, **k):
        calls.append(k.get("skip_if_applied"))
        return daemonlock.ReloadRequest(outcome, pid, info_, detail, code)

    monkeypatch.setattr(daemonlock, "request_reload", fake)
    return calls


class Clock:
    """A clock the test advances by sleeping. Replaces `write.time` so the ack
    loop runs in no real time at all."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def install(self, monkeypatch) -> "Clock":
        monkeypatch.setattr(
            cli_write, "time", types.SimpleNamespace(monotonic=self.monotonic, sleep=self.sleep)
        )
        return self


def probes(monkeypatch, *answers):
    """`daemonlock.probe` returns each of `answers` in turn (then the last one
    forever); an Exception instance is raised. Returns the list of calls."""
    calls = []
    queue = list(answers)

    def probe(resolved):
        calls.append(resolved)
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, BaseException):
            raise item
        return item

    monkeypatch.setattr(daemonlock, "probe", probe)
    return calls


KEY = (7, 5)  # (store id, generation) of "the write just made" in the wording tests


# --------------------------------------------------------------------------
# 1. a replaced database file
# --------------------------------------------------------------------------


class TestStoreIdConflictMessage:
    """`StoreIdConflict` is a `ConfigConflict` whose generations may be equal or
    absent; the generic conflict text ("generation 0, now generation 0",
    "generation None") would be wrong for it."""

    def test_the_message_says_replaced_and_names_both_ids(self):
        lines = cli_errors.format_error(StoreIdConflict(1001, 1002, 0, 0), None)
        assert "replaced" in lines[0]
        assert "1001" in lines[0] and "1002" in lines[0]

    def test_the_second_line_says_nothing_was_written(self):
        lines = cli_errors.format_error(StoreIdConflict(1001, 1002, 0, 0), None)
        assert lines[1].startswith("Nothing was written.")
        assert "Re-run" in lines[1]

    def test_the_first_line_does_not_repeat_nothing_was_written(self):
        lines = cli_errors.format_error(StoreIdConflict(1001, 1002, 0, 0), None)
        assert "nothing was written" not in lines[0].lower()
        assert len(lines) == 2

    @pytest.mark.parametrize(
        "exc",
        [
            StoreIdConflict(1001, 1002),
            StoreIdConflict(1001, 1002, None, None),
            StoreIdConflict(1001, 1002, 0, 0),
            StoreIdConflict(1001, 1002, 3, None),
        ],
        ids=["no-gens", "none-gens", "equal-gens", "actual-none"],
    )
    def test_it_never_prints_a_generation_that_is_not_one(self, exc):
        text = "\n".join(cli_errors.format_error(exc, None))
        assert "generation None" not in text
        assert "generation 0, now generation 0" not in text
        assert "underneath you" not in text

    def test_notes_are_kept(self):
        exc = StoreIdConflict(1, 2)
        exc.add_note("a note")
        assert cli_errors.format_error(exc, None)[-1] == "a note"

    def test_report_prints_it_and_exits_75(self):
        import io

        buf = io.StringIO()
        assert cli_errors.report(StoreIdConflict(1001, 1002, 0, 0), None, stream=buf) == 75
        out = buf.getvalue()
        assert out.startswith("error: ") and "replaced" in out.splitlines()[0]
        assert "internal error" not in out

    def test_a_plain_conflict_keeps_its_generation_wording(self):
        lines = cli_errors.format_error(ConfigConflict(3, 4), None)
        assert lines[0] == "config changed underneath you (generation 3, now generation 4)"

    def test_through_the_cli_a_replaced_file_is_75_with_the_replaced_text(
        self, run, rich, db, monkeypatch
    ):
        def replaced(*a, **k):
            raise StoreIdConflict(1001, 1002, 0, 0)

        monkeypatch.setattr(cli_write, "write_config", replaced)
        before = db_state(db)
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 75, result.stderr
        lines = result.stderr.splitlines()
        assert "replaced" in lines[0] and "1001" in lines[0] and "1002" in lines[0]
        assert lines[1].startswith("Nothing was written.")
        assert "generation None" not in result.stderr
        assert "config updated" not in result.stdout
        assert db_state(db) == before

    def test_a_really_replaced_file_is_refused_with_that_text(self, run, rich, db, tmp_path):
        """No patching: re-import over the file between the read and the write
        (a new store id), then commit from the old snapshot."""
        resolved = _resolved(db)
        snap = cli_write.read_snapshot(resolved)
        other = write_yaml(tmp_path, "datasets:\n  - name: tank/theirs\n", "other.yaml")
        assert run("import", other, "-c", db).exit_code == 0
        dispose_all()
        with pytest.raises(ConfigConflict) as excinfo:
            cli_write.commit_config(resolved, snap, snap.config, announce=False)
        if isinstance(excinfo.value, StoreIdConflict):
            text = "\n".join(cli_errors.format_error(excinfo.value, resolved))
            assert "replaced" in text and "generation None" not in text


# --------------------------------------------------------------------------
# 2. the wording table, row by row
# --------------------------------------------------------------------------


class TestNoticeWording:
    """`_daemon_notice(resolved, 5, 7, wait)` -> `(stdout line, stderr warning)`.
    Every line begins `config updated (generation 5)`."""

    HEAD = "config updated (generation 5)"

    @pytest.fixture(autouse=True)
    def _isolated_probe(self, monkeypatch):
        # A wording test that forgot to say what the daemon's pid file holds
        # must fail, not read a real file.
        monkeypatch.setattr(
            daemonlock, "probe", lambda r: pytest.fail("probe reached without being set up")
        )

    def notice(self, resolved, wait=3.0):
        return cli_write._daemon_notice(resolved, 5, 7, wait)

    # -- no-daemon / already-applied -------------------------------------

    def test_no_daemon(self, resolved):
        reloaded_line, warning = cli_write._daemon_notice(resolved, 5, 7, 3.0)
        assert reloaded_line == f"{self.HEAD}; no running daemon detected for {resolved.resolved_path}"
        assert warning is None

    def test_already_applied(self, resolved, db, live_daemon):
        live_daemon(db, applied=KEY)
        assert self.notice(resolved) == (
            f"{self.HEAD}; running daemon pid {os.getpid()} is already on generation 5", None
        )

    @pytest.mark.parametrize(
        "identity, skipped",
        [("match", True), ("unknown", True), ("gone", False), ("mismatch", False)],
    )
    def test_already_applied_needs_a_supervisor_that_is_not_known_to_be_gone(
        self, resolved, db, fake_daemon, monkeypatch, identity, skipped
    ):
        """`applied == key` only means "nothing to do" while the recorded
        supervisor is not known to be dead or a reused pid: a dead supervisor's
        file still says what it last applied."""
        fake_daemon(db, pid=4321, applied=KEY)
        monkeypatch.setattr(daemonlock.procident, "identity", lambda pid, start: identity)
        got = daemonlock.request_reload(resolved, skip_if_applied=KEY)
        assert (got.outcome == "already-applied") is skipped, got

    def test_already_applied_asks_request_reload_to_skip_exactly_the_key(
        self, resolved, monkeypatch
    ):
        seen = []

        def spy(r, *, skip_if_applied=None):
            seen.append(skip_if_applied)
            return daemonlock.ReloadRequest("no-daemon")

        monkeypatch.setattr(daemonlock, "request_reload", spy)
        self.notice(resolved)
        cli_write._daemon_notice(resolved, 5)          # no store id: nothing to skip on
        cli_write._daemon_notice(resolved, None, 7)    # no generation: nothing to skip on
        assert seen == [(7, 5), None, None]

    # -- signalled -------------------------------------------------------

    def test_signalled_and_applied_equals_the_key(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=KEY))
        assert self.notice(resolved) == (
            f"{self.HEAD}; reload requested from daemon pid 4321: it is now on generation 5", None
        )

    def test_signalled_and_target_equals_the_key(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 4), target=KEY))
        assert self.notice(resolved) == (
            f"{self.HEAD}; reload requested from daemon pid 4321: it accepted generation 5 "
            "and is switching its workers over (in-flight transfers finish first)",
            None,
        )

    def test_signalled_and_rejected_equals_the_key_goes_to_stderr(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 3), rejected=KEY))
        assert self.notice(resolved) == (
            f"{self.HEAD}; reload requested from daemon pid 4321",
            "warning: daemon pid 4321 rejected generation 5 and keeps running generation 3; "
            "see its log",
        )

    def test_rejected_falls_back_to_the_pid_files_second_line(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(generation=2, applied=None, rejected=KEY))
        _, warning = self.notice(resolved)
        assert warning == (
            "warning: daemon pid 4321 rejected generation 5 and keeps running generation 2; "
            "see its log"
        )

    def test_rejected_with_nothing_known_says_its_previous_configuration(
        self, resolved, monkeypatch
    ):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(generation=None, applied=None, rejected=KEY))
        _, warning = self.notice(resolved)
        assert warning == (
            "warning: daemon pid 4321 rejected generation 5 and keeps running its previous "
            "configuration; see its log"
        )
        assert "None" not in warning

    def test_rejected_prefers_applied_over_the_second_line(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(generation=1, applied=(7, 4), rejected=KEY))
        assert "keeps running generation 4;" in self.notice(resolved)[1]

    def test_a_newer_applied_generation_of_the_same_store(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 9)))
        assert self.notice(resolved) == (
            f"{self.HEAD}; reload requested from daemon pid 4321: it is already on "
            "(or applying) newer generation 9",
            None,
        )

    def test_a_newer_target_generation_of_the_same_store(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 3), target=(7, 8)))
        assert self.notice(resolved)[0].endswith("newer generation 8")

    def test_the_newest_of_applied_and_target_is_named(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 9), target=(7, 11)))
        assert self.notice(resolved)[0].endswith("newer generation 11")
        probes(monkeypatch, info(applied=(7, 12), target=(7, 8)))
        assert self.notice(resolved)[0].endswith("newer generation 12")

    def test_signalled_without_an_answer_and_a_wait(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 3)))
        assert self.notice(resolved, wait=3.0) == (
            f"{self.HEAD}; reload requested from daemon pid 4321 (SIGHUP); not yet "
            f"acknowledged; it picks up the change at its next check (within {POLL}s); "
            "if it does not, see the daemon log",
            None,
        )

    def test_signalled_without_a_wait_has_no_acknowledgement_clause(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        calls = probes(monkeypatch, info(applied=KEY))
        assert self.notice(resolved, wait=0) == (
            f"{self.HEAD}; reload requested from daemon pid 4321 (SIGHUP)", None
        )
        assert calls == []  # not even one look

    def test_signalled_with_no_key_never_waits(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        calls = probes(monkeypatch, info(applied=KEY))
        assert cli_write._daemon_notice(resolved, 5, None, 3.0) == (
            f"{self.HEAD}; reload requested from daemon pid 4321 (SIGHUP)", None
        )
        assert calls == []

    # -- not signalled ---------------------------------------------------

    def test_not_permitted(self, resolved, monkeypatch):
        reloaded(monkeypatch, "not-permitted", pid=777)
        assert self.notice(resolved) == (
            f"{self.HEAD}; daemon pid 777 runs as another user and was not signalled; "
            f"it picks up the change within {POLL}s",
            None,
        )

    def test_unverified_with_a_reason(self, resolved, monkeypatch):
        reloaded(monkeypatch, "unverified", pid=777, detail="pid file owned by uid 5")
        assert self.notice(resolved) == (
            f"{self.HEAD}; could not verify that pid 777 is the daemon (pid file owned by "
            f"uid 5), so it was not signalled; it picks up the change within {POLL}s",
            None,
        )

    def test_unverified_because_the_fd_table_cannot_be_read(self, resolved, monkeypatch):
        detail = "cannot read the open files of pid 777 to confirm it holds the daemon lock"
        reloaded(monkeypatch, "unverified", pid=777, detail=detail, code="fd-table-unreadable")
        assert self.notice(resolved) == (
            f"{self.HEAD}; could not verify that pid 777 is the daemon ({detail}), so it was "
            f"not signalled; it picks up the change within {POLL}s",
            None,
        )

    def test_not_permitted_because_it_is_another_users_process(self, resolved, monkeypatch):
        reloaded(
            monkeypatch, "not-permitted", pid=777,
            detail="pid 777 runs as another user; not permitted to signal it", code="other-user",
        )
        assert self.notice(resolved) == (
            f"{self.HEAD}; daemon pid 777 runs as another user and was not signalled; "
            f"it picks up the change within {POLL}s",
            None,
        )

    def test_unverified_without_a_reason(self, resolved, monkeypatch):
        reloaded(monkeypatch, "unverified", pid=777, detail="")
        assert "(no reason given)" in self.notice(resolved)[0]

    def test_unverified_without_a_pid(self, resolved, monkeypatch):
        reloaded(monkeypatch, "unverified", pid=None, detail="torn read")
        assert self.notice(resolved)[0] == (
            f"{self.HEAD}; could not verify that an unknown pid is the daemon (torn read), "
            f"so it was not signalled; it picks up the change within {POLL}s"
        )

    def test_unverified_because_the_platform_cannot_verify(self, resolved, monkeypatch):
        reloaded(
            monkeypatch, "unverified", pid=777,
            detail="signals are verified only on Linux", code="not-linux",
        )
        assert self.notice(resolved) == (
            f"{self.HEAD}; " + UNVERIFIED_LINUX_ONLY.format(pid=777), None
        )

    def test_the_platform_wording_is_chosen_by_the_code_not_the_detail_text(
        self, resolved, monkeypatch
    ):
        reloaded(monkeypatch, "unverified", pid=777, detail="anything at all", code="not-linux")
        assert "can be verified only on Linux" in self.notice(resolved)[0]

    @pytest.mark.parametrize(
        "code, detail",
        [
            ("no-identity", "the daemon did not record its identity on this platform"),
            ("locks-unreadable", "/proc/locks is not readable"),
            ("mount-device", "cannot resolve the pid file's mount device"),
            ("lock-holders-unknown", "the daemon lock's holders could not be determined"),
            ("pid-file-owner", "pid file owned by uid 5"),
            ("proc-hidden", "cannot verify the identity of pid 777 (/proc hidden?)"),
            ("identity-unknown", "cannot verify the identity of pid 777"),
            ("torn-read", "the pid file could not be read consistently"),
        ],
    )
    def test_every_other_unverified_code_shows_the_daemons_own_detail(
        self, resolved, monkeypatch, code, detail
    ):
        """On Linux the detail says what failed; it must not be hidden behind
        the macOS wording (even if the text says "platform")."""
        reloaded(monkeypatch, "unverified", pid=777, detail=detail, code=code)
        line, _ = self.notice(resolved)
        assert line == (
            f"{self.HEAD}; could not verify that pid 777 is the daemon ({detail}), so it "
            f"was not signalled; it picks up the change within {POLL}s"
        )
        assert "only on Linux" not in line

    def test_an_empty_code_falls_back_to_the_platform_and_the_detail_text(
        self, resolved, monkeypatch
    ):
        reloaded(monkeypatch, "unverified", pid=777, detail="signals are verified only on Linux")
        monkeypatch.setattr(cli_write.sys, "platform", "darwin")
        assert "can be verified only on Linux" in self.notice(resolved)[0]
        monkeypatch.setattr(cli_write.sys, "platform", "linux")
        assert "can be verified only on Linux" not in self.notice(resolved)[0]

    def test_unverified_pid_1_is_a_container(self, resolved, monkeypatch):
        reloaded(monkeypatch, "unverified", pid=1, detail="refusing to signal pid 1", code="refused-pid")
        assert self.notice(resolved) == (
            f"{self.HEAD}; running daemon pid 1 was not signalled (pid 1 is never signalled "
            f"from outside; probably a container); it picks up the change within {POLL}s",
            None,
        )

    def test_pid_1_with_another_reason_is_not_called_a_container(self, resolved, monkeypatch):
        reloaded(monkeypatch, "unverified", pid=1, detail="pid file owned by uid 5", code="pid-file-owner")
        assert "container" not in self.notice(resolved)[0]
        assert "could not verify that pid 1 is the daemon" in self.notice(resolved)[0]

    def test_pid_1_with_no_code_is_a_container(self, resolved, monkeypatch):
        reloaded(monkeypatch, "unverified", pid=1, detail="refusing to signal pid 1")
        assert "probably a container" in self.notice(resolved)[0]

    @pytest.mark.parametrize("code", ["zombie", "no-such-process", "exited"])
    def test_supervisor_gone_because_the_process_is_gone(self, resolved, monkeypatch, code):
        reloaded(monkeypatch, "supervisor-gone", pid=4321, code=code)
        assert self.notice(resolved) == (
            f"{self.HEAD}; "
            + daemonlock.stale_supervisor_message(daemonlock.lock_path(resolved), 4321),
            None,
        )

    @pytest.mark.parametrize("code", ["not-lock-holder", "reused"])
    def test_supervisor_gone_because_the_pid_is_not_the_lock_holder(self, resolved, monkeypatch, code):
        """The process exists (it may be somebody else's): not "gone"."""
        reloaded(monkeypatch, "supervisor-gone", pid=4321, code=code)
        assert self.notice(resolved) == (
            f"{self.HEAD}; pid 4321 is not the process holding the daemon lock, so it was "
            f"not signalled; a running daemon picks up the change within {POLL}s",
            None,
        )

    def test_file_not_open_is_not_worded_as_a_dead_supervisor_while_the_pid_exists(
        self, resolved, monkeypatch
    ):
        """Step 8b: the pid exists and is signalable, it just does not hold the
        lock file (a reused pid). The operator must not be told the supervisor
        is gone, whatever `pid_is_alive` says about a pid the CLI cannot see."""
        reloaded(monkeypatch, "supervisor-gone", pid=4321, code="file-not-open")
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: True)
        assert "is not the process holding the daemon lock" in self.notice(resolved)[0]
        assert "supervisor pid" not in self.notice(resolved)[0]

    def test_file_not_open_is_worded_like_not_lock_holder_whatever_pid_is_alive_says(
        self, resolved, monkeypatch
    ):
        reloaded(monkeypatch, "supervisor-gone", pid=4321, code="file-not-open")
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: False)
        assert "is not the process holding the daemon lock" in self.notice(resolved)[0]

    def test_supervisor_gone_without_a_code_asks_whether_the_pid_exists(self, resolved, monkeypatch):
        reloaded(monkeypatch, "supervisor-gone", pid=4321)
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: False)
        assert "supervisor pid 4321 is gone" in self.notice(resolved)[0]
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: True)
        assert "is not the process holding the daemon lock" in self.notice(resolved)[0]
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: None)
        assert "is not the process holding the daemon lock" in self.notice(resolved)[0]

    def test_supervisor_gone_with_a_pid_and_no_code_for_a_dead_pid(self, resolved, monkeypatch):
        reloaded(monkeypatch, "supervisor-gone", pid=4321)
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: False)
        assert self.notice(resolved) == (
            f"{self.HEAD}; "
            + daemonlock.stale_supervisor_message(daemonlock.lock_path(resolved), 4321),
            None,
        )

    def test_supervisor_gone_without_a_pid(self, resolved, monkeypatch):
        reloaded(monkeypatch, "supervisor-gone", pid=None)
        assert self.notice(resolved) == (
            f"{self.HEAD}; the daemon lock is held but its supervisor is gone", None
        )

    def test_legacy_with_a_known_generation(self, resolved, monkeypatch):
        reloaded(monkeypatch, "legacy", info_=info(generation=2, start=None))
        assert self.notice(resolved) == (
            f"{self.HEAD}; running daemon pid 4321 is on generation 2 and predates live "
            "reload; restart it to apply this",
            None,
        )

    def test_legacy_with_an_unknown_generation(self, resolved, monkeypatch):
        reloaded(monkeypatch, "legacy", info_=info(generation=None, start=None))
        line, _ = self.notice(resolved)
        assert line == (
            f"{self.HEAD}; running daemon pid 4321 is on an unknown generation and predates "
            "live reload; restart it to apply this"
        )
        assert "None" not in line

    def test_legacy_without_any_info(self, resolved, monkeypatch):
        reloaded(monkeypatch, "legacy", info_=None)
        assert "on an unknown generation and predates live reload" in self.notice(resolved)[0]

    def test_only_a_legacy_daemon_is_told_to_restart(self, resolved, monkeypatch):
        for outcome in ("not-permitted", "unverified", "supervisor-gone", "signalled"):
            reloaded(monkeypatch, outcome, detail="x")
            Clock().install(monkeypatch)
            probes(monkeypatch, info(applied=(7, 3)))
            line, warning = self.notice(resolved)
            assert "restart" not in line and "restart" not in (warning or ""), outcome

    # -- error -----------------------------------------------------------

    def test_error_goes_to_stderr_and_leaves_the_stdout_line_plain(self, resolved, monkeypatch):
        reloaded(monkeypatch, "error", pid=None, detail="cannot lock x: Input/output error")
        assert self.notice(resolved) == (
            self.HEAD,
            f"warning: could not check whether a daemon is running for {resolved.resolved_path}: "
            "cannot lock x: Input/output error",
        )

    def test_error_without_a_detail_names_the_outcome(self, resolved, monkeypatch):
        reloaded(monkeypatch, "error", detail="")
        assert self.notice(resolved)[1].endswith(": error")

    def test_an_unknown_outcome_is_treated_as_an_error(self, resolved, monkeypatch):
        reloaded(monkeypatch, "teleported", detail="")
        line, warning = self.notice(resolved)
        assert line == self.HEAD
        assert warning == (
            f"warning: could not check whether a daemon is running for {resolved.resolved_path}: "
            "teleported"
        )

    def test_an_unknown_outcome_does_not_wait_or_signal(self, resolved, monkeypatch):
        reloaded(monkeypatch, "teleported")
        self.notice(resolved)  # probe is the failing stub: reaching it would fail

    # -- notify_daemon prints them ---------------------------------------

    def test_notify_prints_the_line_on_stdout_and_the_warning_on_stderr(
        self, resolved, monkeypatch, capsys
    ):
        reloaded(monkeypatch, "error", detail="boom")
        cli_write.notify_daemon(resolved, 5, 7)
        out = capsys.readouterr()
        assert out.out == "config updated (generation 5)\n"
        assert out.err == (
            f"warning: could not check whether a daemon is running for {resolved.resolved_path}: "
            "boom\n"
        )

    def test_a_rejected_reload_prints_two_streams(self, resolved, monkeypatch, capsys):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 3), rejected=KEY))
        cli_write.notify_daemon(resolved, 5, 7)
        out = capsys.readouterr()
        assert out.out == "config updated (generation 5); reload requested from daemon pid 4321\n"
        assert out.err == (
            "warning: daemon pid 4321 rejected generation 5 and keeps running generation 3; "
            "see its log\n"
        )

    def test_the_real_poll_constant_is_ten_seconds(self):
        assert POLL == 10


# --------------------------------------------------------------------------
# 2b. every outcome, through a real command
# --------------------------------------------------------------------------

_ROWS = [
    ("no-daemon", {}, None, "; no running daemon detected for"),
    ("already-applied", {}, None, "; running daemon pid 4321 is already on generation"),
    ("signalled", {}, info(applied=(0, 0)), "; reload requested from daemon pid 4321"),
    ("not-permitted", {}, None, "runs as another user and was not signalled"),
    ("unverified", {"detail": "some doubt"}, None, "could not verify that pid 4321"),
    ("unverified", {"detail": "verified only on Linux"}, None, "can be verified only on Linux"),
    ("supervisor-gone", {}, None, "is gone, but processes it forked"),
    ("legacy", {}, None, "predates live reload; restart it to apply this"),
    ("error", {"detail": "disk"}, None, ""),
    ("not-an-outcome", {"detail": "wat"}, None, ""),
]


class TestEveryOutcomeIsExitZero:
    @pytest.mark.parametrize("outcome, kw, answer, fragment", _ROWS, ids=[r[0] + str(i) for i, r in enumerate(_ROWS)])
    def test_the_write_is_durable_and_the_exit_code_is_zero(
        self, run, rich, db, monkeypatch, outcome, kw, answer, fragment
    ):
        reloaded(monkeypatch, outcome, **kw)
        Clock().install(monkeypatch)
        probes(monkeypatch, answer)
        gen = generation(db)
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert generation(db) == gen + 1
        last = result.stdout.splitlines()[-1]
        assert last.startswith(f"config updated (generation {gen + 1})")
        if sys.platform == "linux" and fragment == "can be verified only on Linux":
            # The platform wording is for non-Linux hosts; on Linux the daemon's own detail is shown.
            fragment = "(verified only on Linux)"
        assert fragment in last

    def test_a_broken_stdout_does_not_change_the_exit_code(self, run, rich, db, monkeypatch):
        reloaded(monkeypatch, "no-daemon")
        real = click.echo

        def echo(message=None, *a, **k):
            if isinstance(message, str) and message.startswith("config updated"):
                raise BrokenPipeError(errno.EPIPE, "Broken pipe")
            return real(message, *a, **k)

        monkeypatch.setattr(cli_write.click, "echo", echo)
        assert run("set", "check_interval", "10m", "-c", db).exit_code == 0


# --------------------------------------------------------------------------
# 3. exactly one request_reload per committed write
# --------------------------------------------------------------------------


@pytest.fixture
def reload_calls(db, monkeypatch):
    """Replace `request_reload` with a recorder. Each call records the
    `skip_if_applied` it was given and the key the DATABASE holds at that
    moment (so "already at the new generation" is checked, not assumed)."""
    calls = []

    def recorder(resolved, *, skip_if_applied=None):
        calls.append(types.SimpleNamespace(skip=skip_if_applied, stored=store_key(db)))
        return daemonlock.ReloadRequest("no-daemon")

    monkeypatch.setattr(daemonlock, "request_reload", recorder)
    return calls


class Ctx:
    """What a matrix case needs to run one command."""

    def __init__(self, run, db, stub, fake_zfs, fake_daemon, tmp_path):
        self.run, self.db, self.stub = run, db, stub
        self.fake_zfs, self.fake_daemon, self.tmp_path = fake_zfs, fake_daemon, tmp_path

    def edit(self, *args, plan=()):
        self.stub.plan(list(plan))
        return self.run("edit", *args, "-c", self.db, env=self.stub.env(self.db))


@pytest.fixture
def ctx(run, rich, db, stub, fake_zfs, fake_daemon, scratch_tmp, tmp_path):
    return Ctx(run, db, stub, fake_zfs, fake_daemon, tmp_path)


def _replace(old, new):
    return {"replace": [old, new]}


def _import(c):
    src = write_yaml(c.tmp_path, "datasets:\n  - name: tank/new\n    frequency: 1h\n", "new.yaml")
    return c.run("import", src, "-c", c.db)


def _rename_dataset_clear_stale(c):
    # OLD carries our properties and NEW exists; a daemon holds the lock, so
    # the clear is refused AFTER the write (a CliError): the daemon must still
    # be told about the write.
    c.fake_zfs.add("tank/vm.01", {"org.zfsbackup:config": "cfg", ANCHOR + "offsite": "s"})
    c.fake_zfs.add("tank/vm.02", {})
    c.fake_daemon(c.db, pid=4321, generation=0)
    return c.run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", "-c", c.db)


def _rename_destination_with_copies(c):
    c.fake_zfs.add("tank/data", {ANCHOR + "offsite": "snap"})
    c.fake_zfs.add("tank/other", {})
    return c.run("rename", "destination", "offsite", "offsite2", "-c", c.db)


WRITES = [
    ("set", 0, lambda c: c.run("set", "check_interval", "10m", "-c", c.db)),
    ("unset", 0, lambda c: c.run("unset", "datasets[tank/data].retention.1w", "-c", c.db)),
    ("edit-whole", 0, lambda c: c.edit("--no-prompt", plan=[_replace('check_interval: "5m"', 'check_interval: "10m"')])),
    ("edit-dataset", 0, lambda c: c.edit("dataset", "tank/vm.01", "--no-prompt", plan=[_replace('frequency: "15m"', 'frequency: "20m"')])),
    ("edit-destination", 0, lambda c: c.edit("destination", "spare", "--no-prompt", plan=[_replace("https://spare:8080", "https://spare:9090")])),
    ("rename-dataset", 0, lambda c: c.run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", c.db)),
    ("rename-destination", 0, lambda c: c.run("rename", "destination", "spare", "spare2", "-c", c.db)),
    ("rename-destination-with-anchor-copies", 0, _rename_destination_with_copies),
    ("rename-dataset-clear-stale-cli-error", 1, _rename_dataset_clear_stale),
    ("import", 0, _import),
]


class TestCallMatrix:
    @pytest.mark.parametrize("name, code, command", WRITES, ids=[w[0] for w in WRITES])
    def test_one_call_with_the_written_key_after_the_commit(
        self, ctx, db, reload_calls, name, code, command
    ):
        before = store_key(db)
        result = command(ctx)
        assert result.exit_code == code, result.stderr
        assert len(reload_calls) == 1, f"{name}: {len(reload_calls)} request_reload calls"
        (call,) = reload_calls
        store_id, gen = call.skip
        assert (store_id, gen) == call.stored, "the DB was not yet at the key being announced"
        if name == "import":
            assert gen == before[1] + 1 and store_id != 0
        else:
            assert (store_id, gen) == (before[0], before[1] + 1)

    @pytest.mark.parametrize("name, code, command", WRITES, ids=[w[0] for w in WRITES])
    def test_the_notice_is_printed_once(self, ctx, db, reload_calls, name, code, command):
        result = command(ctx)
        assert result.stdout.count("config updated (generation") == 1

    def test_a_first_import_announces_generation_0(self, run, db, tmp_path, reload_calls):
        src = write_yaml(tmp_path, "datasets:\n  - name: tank/new\n", "new.yaml")
        assert run("import", src, "-c", db).exit_code == 0
        (call,) = reload_calls
        assert call.skip[1] == 0 and call.skip == call.stored

    def test_a_re_import_announces_the_next_generation_of_the_same_file(
        self, run, rich, db, tmp_path, reload_calls
    ):
        """An in-place replace keeps the file (and so the store id); only a
        deleted-and-recreated file gets a new one."""
        old = store_key(db)
        src = write_yaml(tmp_path, "datasets:\n  - name: tank/new\n", "new.yaml")
        assert run("import", src, "-c", db).exit_code == 0
        (call,) = reload_calls
        assert call.skip == call.stored == (old[0], old[1] + 1)

    def test_a_recreated_file_announces_its_new_store_id(self, run, rich, db, tmp_path, reload_calls):
        old = store_key(db)
        dispose_all()
        for suffix in ("", "-wal", "-shm"):
            Path(str(db) + suffix).unlink(missing_ok=True)
        src = write_yaml(tmp_path, "datasets:\n  - name: tank/new\n", "new.yaml")
        assert run("import", src, "-c", db).exit_code == 0
        (call,) = reload_calls
        assert call.skip == call.stored
        assert call.skip[0] != old[0] and call.skip[1] == 0

    # -- never ----------------------------------------------------------

    def test_commit_config_refuses_announce_with_a_caller_held_lock(self, rich, db, reload_calls):
        resolved = _resolved(db)
        snap = cli_write.read_snapshot(resolved)
        with writer_lock(resolved) as held:
            with pytest.raises(ValueError, match="announce=True"):
                cli_write.commit_config(resolved, snap, snap.config, lock=held)
        assert reload_calls == []
        assert store_key(db)[1] == snap.generation  # nothing was written either

    def test_commit_config_announce_false_never_calls(self, rich, db, reload_calls):
        resolved = _resolved(db)
        snap = cli_write.read_snapshot(resolved)
        cli_write.commit_config(resolved, snap, snap.config, announce=False)
        assert reload_calls == []

    def test_a_held_lock_with_announce_false_is_allowed(self, rich, db, reload_calls):
        resolved = _resolved(db)
        snap = cli_write.read_snapshot(resolved)
        with writer_lock(resolved) as held:
            result = cli_write.commit_config(resolved, snap, snap.config, announce=False, lock=held)
        assert result.generation == snap.generation + 1 and reload_calls == []

    @pytest.mark.parametrize(
        "args",
        [
            ("rename", "dataset", "tank/vm.01", "tank/vm.02", "--dry-run"),
            ("rename", "destination", "spare", "spare2", "--dry-run"),
            ("rename", "dataset", "tank/vm.01", "tank/vm.02", "-d"),
        ],
        ids=["dataset", "destination", "dataset-short"],
    )
    def test_rename_dry_run_never_calls(self, run, rich, db, fake_zfs, reload_calls, args):
        state = db_state(db)
        result = run(*args, "-c", db)
        assert result.exit_code == 0, result.stderr
        assert reload_calls == [] and db_state(db) == state
        assert "config updated" not in result.stdout

    def test_rename_destination_dry_run_with_anchor_copies_never_calls(
        self, run, rich, db, fake_zfs, reload_calls
    ):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snap"})
        fake_zfs.add("tank/other", {})
        result = run("rename", "destination", "offsite", "offsite2", "--dry-run", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert reload_calls == [] and fake_zfs.mutations == []

    def test_a_validation_refusal_never_calls(self, run, rich, db, reload_calls):
        state = db_state(db)
        result = run("set", "api_port", "70000", "-c", db)
        assert result.exit_code == 1
        assert reload_calls == [] and db_state(db) == state

    def test_a_validate_config_refusal_never_calls(self, ctx, db, reload_calls):
        """The refusal comes from `validate_config` inside `commit_config`
        (a dataset name the daemon could not manage), not from the dot-path."""
        state = db_state(db)
        result = ctx.edit(
            "--no-prompt", plan=[{"append": '  - name: "bad name"\n    frequency: "1h"\n'}]
        )
        assert result.exit_code == 1, result.stderr
        assert reload_calls == [] and db_state(db) == state

    def test_a_rename_shaped_edit_refusal_never_calls(self, ctx, db, reload_calls):
        state = db_state(db)
        result = ctx.edit("--no-prompt", plan=[_replace('- name: "a"\n', '- name: "a9"\n')])
        assert result.exit_code == 1 and "looks like a rename" in result.stderr
        assert reload_calls == [] and db_state(db) == state

    @pytest.mark.parametrize(
        "exc, code",
        [
            (ConfigConflict(0, 1), 75),
            (StoreIdConflict(1001, 1002, 0, 0), 75),
            (ConfigBusy("held by someone"), 75),
        ],
        ids=["ConfigConflict", "StoreIdConflict", "ConfigBusy"],
    )
    @pytest.mark.parametrize(
        "args",
        [
            ("set", "check_interval", "10m"),
            ("unset", "datasets[tank/data].retention.1w"),
            ("rename", "dataset", "tank/vm.01", "tank/vm.02"),
            ("rename", "destination", "spare", "spare2"),
        ],
        ids=["set", "unset", "rename-dataset", "rename-destination"],
    )
    def test_a_lost_write_never_calls(
        self, run, rich, db, fake_zfs, reload_calls, monkeypatch, args, exc, code
    ):
        def lost(*a, **k):
            raise exc

        monkeypatch.setattr(cli_write, "write_config", lost)
        state = db_state(db)
        result = run(*args, "-c", db)
        assert result.exit_code == code, result.stderr
        assert reload_calls == [] and db_state(db) == state
        assert "config updated" not in result.stdout

    def test_a_really_lost_race_never_calls(self, ctx, db, reload_calls):
        """No patching: another CLI writes while `$EDITOR` is open."""
        ctx.stub.plan([
            {"cli": ["-c", str(db), "set", "check_interval", "9m"]},
            _replace('check_interval: "5m"', 'check_interval: "7m"'),
        ])
        result = ctx.run("edit", "--no-prompt", "-c", db, env=ctx.stub.env(db))
        assert result.exit_code == 75, result.stderr
        # the OTHER writer is a subprocess (a real daemonless CLI): it did its
        # own announce, in its own process; this process announced nothing.
        assert reload_calls == []

    def test_an_edit_abort_never_calls(self, ctx, db, reload_calls, monkeypatch):
        from zfsbackup.cli import editor

        monkeypatch.setattr(editor, "_stdin_is_tty", lambda: True)
        state = db_state(db)
        ctx.stub.plan([_replace('check_interval: "5m"', 'check_interval: "bogus"')])
        result = ctx.run("edit", "-c", db, env=ctx.stub.env(db), input="a\n")
        assert result.exit_code == 1 and "edit abandoned" in result.stderr
        assert reload_calls == [] and db_state(db) == state

    def test_an_edit_with_no_change_never_calls(self, ctx, db, reload_calls):
        state = db_state(db)
        result = ctx.edit("--no-prompt", plan=[])
        assert result.exit_code == 0 and "No changes" in result.stdout
        assert reload_calls == [] and db_state(db) == state

    def test_an_edit_entry_with_no_change_never_calls(self, ctx, db, reload_calls):
        result = ctx.edit("dataset", "tank/data", "--no-prompt", plan=[])
        assert result.exit_code == 0 and "No changes" in result.stdout
        assert reload_calls == []

    def test_a_set_with_no_change_never_calls(self, run, rich, db, reload_calls):
        state = db_state(db)
        result = run("set", "check_interval", "5m", "-c", db)
        assert result.exit_code == 0 and "nothing changed" in result.stdout
        assert reload_calls == [] and db_state(db) == state

    def test_an_unset_of_something_already_unset_never_calls(self, run, rich, db, reload_calls):
        state = db_state(db)
        result = run("unset", "prune_interval", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert reload_calls == [] and db_state(db) == state

    def test_a_failed_import_never_calls(self, run, rich, db, tmp_path, reload_calls):
        state = db_state(db)
        bad = write_yaml(tmp_path, "datasets:\n  - name: 'bad name'\n", "bad.yaml")
        result = run("import", bad, "-c", db)
        assert result.exit_code == 1, result.stderr
        assert reload_calls == [] and db_state(db) == state

    @pytest.mark.parametrize(
        "args",
        [
            ("export",),
            ("list", "datasets"),
            ("list", "destinations"),
            ("get", "check_interval"),
            ("status",),
            ("status", "--json"),
            ("status", "--wait", "0"),
            ("migrate", "--check"),
            ("migrate",),
        ],
        ids=lambda a: " ".join(a),
    )
    def test_reading_commands_never_call(self, run, rich, db, reload_calls, args):
        result = run(*args, "-c", db)
        assert result.exit_code == 0, result.stderr
        assert reload_calls == []

    def test_a_daemon_present_makes_no_difference_to_the_dry_run(
        self, run, rich, db, fake_zfs, fake_daemon, reload_calls
    ):
        fake_daemon(db, applied=(1, 1))
        run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--dry-run", "-c", db)
        assert reload_calls == []


# --------------------------------------------------------------------------
# 4. nothing waits while the writer lock is held
# --------------------------------------------------------------------------


def _writer_lock_is_free(db: Path) -> bool:
    """Can another open file description take the writer lock right now? (A
    separate descriptor is what a second `zfsbackup-config` would use.)"""
    import fcntl

    path = Path(str(_resolved(db).resolved_path) + ".writer.lock")
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o660)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    finally:
        os.close(fd)


class TestWriterLockIsReleasedDuringTheWait:
    @pytest.fixture
    def watched(self, db, monkeypatch):
        """`request_reload` says `signalled`; `probe` records whether the writer
        lock was free at every look, and never answers (so the loop runs to its
        deadline on a fake clock)."""
        monkeypatch.setattr(
            daemonlock, "request_reload",
            lambda r, **k: daemonlock.ReloadRequest("signalled", 4321, None, ""),
        )
        Clock().install(monkeypatch)
        seen = []

        def probe(resolved):
            seen.append(_writer_lock_is_free(db))
            return info(applied=(1, 0))

        monkeypatch.setattr(daemonlock, "probe", probe)
        return seen

    def test_the_lock_probe_itself_detects_a_held_lock(self, rich, db):
        resolved = _resolved(db)
        with writer_lock(resolved):
            assert _writer_lock_is_free(db) is False
        assert _writer_lock_is_free(db) is True

    @pytest.mark.parametrize(
        "name, code, command",
        [w for w in WRITES if w[0] != "rename-dataset-clear-stale-cli-error"],
        ids=[w[0] for w in WRITES if w[0] != "rename-dataset-clear-stale-cli-error"],
    )
    def test_every_write_waits_without_the_lock(self, ctx, db, watched, name, code, command):
        result = command(ctx)
        assert result.exit_code == code, result.stderr
        assert watched, "the acknowledgement wait never looked at the pid file"
        assert all(watched), "the writer lock was held while waiting for the daemon"

    def test_the_wait_after_a_clear_stale_cli_error_holds_no_lock_either(
        self, ctx, db, watched
    ):
        result = _rename_dataset_clear_stale(ctx)
        assert result.exit_code == 1
        assert watched and all(watched)

    def test_the_same_through_notify_daemon_directly(self, rich, db, watched):
        cli_write.notify_daemon(_resolved(db), 3, 7)
        assert watched and all(watched)


# --------------------------------------------------------------------------
# 4b. nothing pooled stays open across the post-commit wait
# --------------------------------------------------------------------------


class Leak:
    """Instruments one test so it can say what was still open when the post-commit notice
    began (`_daemon_notice`) and at every look at the pid file (`probe`).

    `install()` replaces `request_reload` with a fake that LEAKS a pooled engine (as if
    something after the commit had touched the database) and answers `signalled`, so the
    notice waits; `probe` answers nothing final on a fake clock, so the wait runs to its
    deadline. Call it after the test's database is built: building it must not be
    instrumented."""

    def __init__(self, monkeypatch, answer="signalled") -> None:
        self.monkeypatch = monkeypatch
        self.answer = answer
        self.events = []             # "reload" / "dispose" / "notice" / "probe", in order
        self.open_at_notice = []     # cached engines when the notice started
        self.open_at_probe = []      # cached engines at each look at the pid file
        self.wait_seen = []          # the `wait` the notice was given
        self.armed = False           # a request was made: the commit is behind us
        self.dispose_fails = None    # an exception to raise from `dispose_all` once armed

    @staticmethod
    def engines() -> int:
        from zfsbackup.config.store import db as store_db

        return len(store_db._ENGINES)

    def install(self) -> "Leak":
        from zfsbackup.config.store import db as store_db

        mp = self.monkeypatch

        def request_reload(resolved, *, skip_if_applied=None):
            self.events.append("reload")
            self.armed = True
            url = store_db.url_for_path(resolved.resolved_path)
            store_db.get_engine(url).connect().close()      # a pooled connection, left cached
            return daemonlock.ReloadRequest(self.answer, 4321, None, "")

        real_dispose = cli_write.dispose_all

        def dispose_all():
            self.events.append("dispose")
            if self.armed and self.dispose_fails is not None:
                raise self.dispose_fails
            real_dispose()

        real_notice = cli_write._daemon_notice

        def daemon_notice(resolved, generation, store_id=None, wait=3.0, request=None):
            self.events.append("notice")
            self.open_at_notice.append(self.engines())
            self.wait_seen.append(wait)
            return real_notice(resolved, generation, store_id, wait, request)

        def probe(resolved):
            self.events.append("probe")
            self.open_at_probe.append(self.engines())
            return info(applied=(1, 0))

        mp.setattr(daemonlock, "request_reload", request_reload)
        mp.setattr(daemonlock, "probe", probe)
        mp.setattr(cli_write, "dispose_all", dispose_all)
        mp.setattr(cli_write, "_daemon_notice", daemon_notice)
        Clock().install(mp)
        return self

    def leak_now(self, database: Path) -> None:
        """Leave a pooled engine cached, as a caller that forgot to release it would."""
        from zfsbackup.config.store import db as store_db

        store_db.get_engine(store_db.url_for_path(_resolved(database).resolved_path)).connect().close()
        assert self.engines() == 1


@pytest.fixture
def leak(monkeypatch):
    dispose_all()
    yield Leak(monkeypatch)
    dispose_all()


class TestNothingPooledAcrossTheWait:
    """`_await_answer` and `notify_daemon` close every pooled connection first. The commit
    paths already do (`commit_config`), but `enroll`, `import` and `identity` reach the notice
    through their own `begin_reload`, and a pooled WAL reader would pin the checkpoint for
    as long as the daemon takes to answer."""

    # -- the two functions, called directly

    def test_the_ack_loop_releases_before_its_first_look(self, rich, db, resolved, leak):
        leak.install()
        leak.leak_now(db)
        answer, _ = cli_write._await_answer(resolved, KEY, 1.0)
        assert leak.events[:2] == ["dispose", "probe"]
        assert leak.open_at_probe and set(leak.open_at_probe) == {0}
        assert answer is None                                  # nothing in the file named KEY

    def test_every_look_of_a_long_wait_is_after_the_release(self, rich, db, resolved, leak):
        leak.install()
        leak.leak_now(db)
        cli_write._await_answer(resolved, KEY, 5.0)
        assert leak.events.count("probe") > 3                  # it really did poll
        assert leak.events.index("dispose") < leak.events.index("probe")
        assert set(leak.open_at_probe) == {0}

    def test_await_applied_releases_too(self, rich, db, resolved, leak):
        leak.install()
        leak.leak_now(db)
        cli_write.await_applied(resolved, KEY, 1.0)
        assert leak.events[:2] == ["dispose", "probe"] and set(leak.open_at_probe) == {0}

    def test_the_ack_loop_releases_even_when_the_wait_is_zero(self, rich, db, resolved, leak):
        leak.install()
        leak.leak_now(db)
        cli_write._await_answer(resolved, KEY, 0)
        assert leak.events[:2] == ["dispose", "probe"] and set(leak.open_at_probe) == {0}

    @pytest.mark.parametrize("wait", [0, 3.0])
    def test_notify_daemon_releases_before_it_computes_the_notice(
        self, rich, db, resolved, leak, wait
    ):
        leak.install()
        leak.leak_now(db)
        request = daemonlock.ReloadRequest("no-daemon")
        cli_write.notify_daemon(resolved, 3, 7, reload_wait=wait, request=request)
        assert leak.events == ["dispose", "notice"]
        assert leak.open_at_notice == [0] and leak.wait_seen == [wait]

    def test_notify_daemon_without_a_request_releases_before_asking_the_daemon(
        self, rich, db, resolved, leak
    ):
        leak.install()
        cli_write.notify_daemon(resolved, 3, 7, reload_wait=0)
        assert leak.events[:3] == ["dispose", "notice", "reload"]

    # -- a release that fails never fails a committed write

    def test_a_failing_release_in_notify_daemon_is_swallowed_and_the_notice_still_prints(
        self, rich, db, resolved, leak, capsys
    ):
        leak.install()
        leak.armed = True
        leak.dispose_fails = OSError("engine is wedged")
        request = daemonlock.ReloadRequest("no-daemon")
        cli_write.notify_daemon(resolved, 3, 7, reload_wait=0, request=request)    # no raise
        out = capsys.readouterr()
        assert "config updated (generation 3); no running daemon detected" in out.out
        assert leak.events == ["dispose", "notice"]

    def test_a_failing_release_before_the_wait_is_a_warning_not_an_error(
        self, rich, db, resolved, leak, capsys
    ):
        leak.install()
        leak.armed = True
        leak.dispose_fails = RuntimeError("engine is wedged")
        request = daemonlock.ReloadRequest("signalled", 4321, None, "")
        cli_write.notify_daemon(resolved, 3, 7, reload_wait=1.0, request=request)  # no raise
        out = capsys.readouterr()
        assert out.out == "config updated\n"       # the plain line: the check itself failed
        assert "could not check for a running daemon: engine is wedged" in out.err

    def test_an_interrupt_in_the_release_is_not_swallowed(self, rich, db, resolved, leak):
        leak.install()
        leak.armed = True
        leak.dispose_fails = KeyboardInterrupt()
        with pytest.raises(KeyboardInterrupt):
            cli_write.notify_daemon(
                resolved, 3, 7, reload_wait=0, request=daemonlock.ReloadRequest("no-daemon")
            )

    # -- the commands

    def _assert_clean_wait(self, leak, *, waited=True):
        assert leak.events.count("reload") == 1
        assert leak.open_at_notice == [0], "a pooled connection was open when the notice began"
        if waited:
            assert leak.open_at_probe and set(leak.open_at_probe) == {0}
        last = len(leak.events) - 1 - leak.events[::-1].index("dispose")
        assert last > leak.events.index("reload"), "nothing was released after the commit"

    def test_set_releases_before_the_wait(self, run, rich, db, leak):
        leak.install()
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert "config updated" in result.stdout
        self._assert_clean_wait(leak)

    def test_identity_set_client_id_releases_before_the_wait(self, run, rich, db, leak):
        leak.install()
        result = run("identity", "set-client-id", "-c", db, "--", "host-9")
        assert result.exit_code == 0, result.stderr
        assert host_client_id(db) == "host-9"
        self._assert_clean_wait(leak)

    def test_import_releases_before_the_wait(self, run, rich, db, leak, tmp_path):
        src = write_yaml(tmp_path, "datasets:\n  - name: tank/new\n    frequency: 1h\n", "new.yaml")
        leak.install()
        result = run("import", src, "-c", db)
        assert result.exit_code == 0, result.stderr
        self._assert_clean_wait(leak)

    def test_enroll_releases_before_the_wait(self, run, client, db, leak, make_server):
        srv = make_server()
        leak.install()
        result = do_enroll(run, client, srv, srv.token())
        assert result.exit_code == 0, result.stderr
        assert "config updated" in result.stdout
        self._assert_clean_wait(leak)

    def test_enroll_releases_on_the_failure_path_that_does_not_wait(
        self, run, db, leak, make_server
    ):
        """The generated client id is committed, then the server is unreachable: the notice
        is made with `reload_wait=0`, and the release comes before it all the same."""
        make_db(run, db, "datasets:\n  - name: tank/a\n    frequency: 1h\n")
        srv = make_server()
        token = srv.token()
        leak.install()
        result = do_enroll(run, db, _unreachable(), token)
        assert result.exit_code == 75, result.stderr
        assert host_client_id(db)                               # the id was committed
        assert leak.wait_seen == [0] and leak.open_at_probe == []
        self._assert_clean_wait(leak, waited=False)

    @pytest.mark.parametrize("command", ["set", "identity", "enroll"])
    def test_a_failing_release_never_fails_the_command(
        self, run, rich, db, leak, make_server, command
    ):
        srv = make_server()
        token = srv.token()
        leak.install()
        leak.dispose_fails = OSError("engine is wedged")        # raised once a request was made
        gen = generation(db)
        if command == "set":
            result = run("set", "check_interval", "10m", "-c", db)
        elif command == "identity":
            result = run("identity", "set-client-id", "-c", db, "--", "host-9")
        else:
            result = do_enroll(run, db, srv, token)
        leak.dispose_fails = None
        assert result.exit_code == 0, result.stderr
        assert generation(db) > gen                             # the write is durable
        assert "config updated" in result.stdout
        assert leak.armed and leak.events.count("reload") == 1


# --------------------------------------------------------------------------
# 5. the acknowledgement loop
# --------------------------------------------------------------------------


class TestAckLoop:
    KEY = (7, 5)

    def wait(self, resolved, wait=3.0):
        return cli_write._await_answer(resolved, self.KEY, wait)

    @pytest.mark.parametrize(
        "daemon, expected",
        [
            (dict(applied=(7, 5)), "applied"),
            (dict(applied=(7, 4), target=(7, 5)), "accepted"),
            (dict(applied=(7, 4), rejected=(7, 5)), "rejected"),
            (dict(applied=(7, 6)), "newer"),
            (dict(applied=(7, 4), target=(7, 6)), "newer"),
            (dict(applied=(7, 99)), "newer"),
        ],
        ids=["applied", "target", "rejected", "newer-applied", "newer-target", "far-newer-applied"],
    )
    def test_it_stops_on_the_first_look_that_answers(self, resolved, monkeypatch, daemon, expected):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(**daemon))
        answer, seen = self.wait(resolved)
        assert answer == expected
        assert seen.applied == daemon.get("applied")
        assert len(calls) == 1 and clock.sleeps == []

    def test_it_polls_every_tenth_of_a_second_until_the_answer_arrives(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(
            monkeypatch,
            info(applied=(7, 3)), info(applied=(7, 3)), info(applied=(7, 4), target=(7, 5)),
        )
        answer, _ = self.wait(resolved)
        assert answer == "accepted"
        assert len(calls) == 3
        assert clock.sleeps == [0.1, 0.1]

    def test_it_goes_on_from_accepted_to_applied_only_if_asked_to_wait_longer(
        self, resolved, monkeypatch
    ):
        """`accepted` is an answer: the loop does not keep waiting for `applied`
        (in-flight transfers may take hours)."""
        Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(target=self.KEY), info(applied=self.KEY))
        assert self.wait(resolved)[0] == "accepted" and len(calls) == 1

    @pytest.mark.parametrize(
        "daemon",
        [
            dict(applied=(8, 5)),
            dict(applied=(8, 99)),
            dict(applied=(0, 99)),
            dict(applied=(7, 4), target=(8, 5)),
            dict(applied=(7, 4), target=(8, 99)),
            dict(applied=(7, 4), rejected=(8, 5)),
            dict(applied=(7, 4), rejected=(7, 4)),
            dict(applied=(7, 4)),
            dict(generation=5),
            dict(),
        ],
        ids=[
            "other-store-same-gen", "other-store-newer-gen", "unstamped-store-newer",
            "other-store-target", "other-store-newer-target", "other-store-rejected",
            "older-rejected", "older-applied", "line-2-only", "nothing",
        ],
    )
    def test_it_does_not_stop_on_anything_that_is_not_about_this_key(
        self, resolved, monkeypatch, daemon
    ):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(**daemon))
        answer, last = self.wait(resolved, wait=1.0)
        assert answer is None
        assert len(calls) > 5                       # it kept looking...
        assert sum(clock.sleeps) == pytest.approx(1.0)  # ...for the whole wait
        assert last is not None

    def test_a_zombie_ends_at_the_deadline(self, resolved, monkeypatch):
        """A signalled supervisor that never answers costs the wait, not forever."""
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=(7, 4)))
        answer, last = self.wait(resolved, wait=3.0)
        assert answer is None and last.applied == (7, 4)
        assert sum(clock.sleeps) == pytest.approx(3.0)
        assert clock.now - 1000.0 == pytest.approx(3.0)
        assert max(clock.sleeps) <= 0.1 + 1e-9
        assert 30 <= len(calls) <= 32

    def test_the_last_sleep_is_shortened_to_the_deadline(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4)))
        self.wait(resolved, wait=0.25)
        assert clock.sleeps == pytest.approx([0.1, 0.1, 0.05])

    def test_it_looks_at_least_once_even_with_a_tiny_wait(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=(7, 4)))
        assert self.wait(resolved, wait=0.001)[0] is None
        assert len(calls) >= 1

    def test_a_newer_rejected_generation_of_the_same_store_means_ours_was_superseded(
        self, resolved, monkeypatch
    ):
        """It rejected a LATER generation of this store: ours was folded into
        that one and will never be named on its own, so waiting is pointless."""
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=(7, 4), rejected=(7, 6)))
        answer, seen = self.wait(resolved)
        assert answer == "superseded" and seen.rejected == (7, 6)
        assert len(calls) == 1 and clock.sleeps == []

    def test_a_newer_rejected_key_of_another_store_is_not_superseded(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4), rejected=(8, 6)))
        assert self.wait(resolved, wait=0.5)[0] is None

    def test_it_stops_when_the_daemon_is_gone(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=(7, 4)), None)
        answer, last = self.wait(resolved)
        assert answer == "gone" and len(calls) == 2
        assert last.applied == (7, 4)  # the last thing it did see
        assert clock.sleeps == [0.1]

    def test_it_stops_when_the_daemon_is_gone_at_the_first_look(self, resolved, monkeypatch):
        calls = probes(monkeypatch, None)
        Clock().install(monkeypatch)
        assert self.wait(resolved) == ("gone", None) and len(calls) == 1

    def test_it_stops_when_the_pid_file_cannot_be_read(self, resolved, monkeypatch):
        calls = probes(
            monkeypatch, info(applied=(7, 4)), daemonlock.DaemonLockError(errno.EACCES, "denied")
        )
        Clock().install(monkeypatch)
        answer, last = self.wait(resolved)
        assert answer == "unreadable" and len(calls) == 2 and last.applied == (7, 4)

    def test_a_plain_oserror_from_probe_ends_the_wait_too(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        calls = probes(monkeypatch, OSError(errno.EIO, "io"))
        assert self.wait(resolved) == ("unreadable", None) and len(calls) == 1

    # -- torn reads ------------------------------------------------------

    @pytest.mark.parametrize(
        "daemon",
        [
            dict(applied=KEY), dict(target=KEY), dict(rejected=KEY),
            dict(applied=(7, 9)), dict(target=(7, 9)), dict(rejected=(7, 9)),
        ],
        ids=["applied", "target", "rejected", "newer-applied", "newer-target", "superseded"],
    )
    def test_a_torn_read_answers_nothing_whatever_it_claims(self, resolved, monkeypatch, daemon):
        """A torn read may be a mix of two states: it is "not yet", and the
        loop reads again."""
        clock = Clock().install(monkeypatch)
        torn = daemonlock.DaemonInfo(pid=4321, generation=None, start="t", stable=False, **daemon)
        calls = probes(monkeypatch, torn, torn, info(applied=(7, 3)))
        answer, last = self.wait(resolved, wait=0.25)
        assert answer is None
        assert len(calls) >= 3, "it stopped on a torn read"
        assert last.applied == (7, 3), "a torn read became the last info"

    def test_it_answers_from_the_first_stable_read_after_a_torn_one(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        torn = daemonlock.DaemonInfo(pid=4321, generation=None, start="t", applied=KEY, stable=False)
        calls = probes(monkeypatch, torn, torn, info(applied=KEY))
        answer, seen = self.wait(resolved)
        assert answer == "applied" and len(calls) == 3 and clock.sleeps == [0.1, 0.1]

    def test_a_torn_read_is_never_the_last_info_when_nothing_stable_was_seen(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        torn = daemonlock.DaemonInfo(pid=4321, generation=None, start="t", applied=(7, 3), stable=False)
        probes(monkeypatch, torn)
        assert self.wait(resolved, wait=0.3) == (None, None)

    def test_answer_ignores_an_unstable_info_directly(self):
        torn = daemonlock.DaemonInfo(pid=1, generation=None, start="t", applied=KEY, stable=False)
        assert cli_write._answer(torn, KEY) is None
        assert cli_write._answer(info(applied=KEY), KEY) == "applied"

    # -- the notice for each way the wait can end ----------------------------

    def test_the_notice_after_a_vanished_daemon(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        probes(monkeypatch, None)
        assert cli_write._daemon_notice(resolved, 5, 7, 3.0) == (
            "config updated (generation 5); reload requested from daemon pid 4321; the daemon "
            "exited before acknowledging; the next daemon start reads generation 5",
            None,
        )

    def test_the_notice_when_the_pid_file_became_unreadable(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        probes(monkeypatch, OSError(errno.EIO, "io"))
        assert cli_write._daemon_notice(resolved, 5, 7, 3.0) == (
            "config updated (generation 5); reload requested from daemon pid 4321; could not "
            f"read its pid file while waiting; it picks up the change within {POLL}s if it is running",
            None,
        )

    def test_the_notice_when_a_later_generation_was_rejected(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 3), rejected=(7, 9)))
        assert cli_write._daemon_notice(resolved, 5, 7, 3.0) == (
            "config updated (generation 5); reload requested from daemon pid 4321; superseded "
            "by generation 9, which daemon pid 4321 rejected; see its log",
            None,
        )

    def test_a_wait_of_zero_never_reports_gone_or_unreadable(self, resolved, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, None)
        assert cli_write._daemon_notice(resolved, 5, 7, 0)[0].endswith("(SIGHUP)")

    INTERRUPTED = (
        "config updated (generation 5); interrupted while waiting for daemon pid 4321 to "
        f"acknowledge (the change is saved; it picks up the change within {POLL}s)\n"
    )

    @staticmethod
    def _sleep_raises(monkeypatch, exc):
        def sleeper(seconds):
            raise exc

        monkeypatch.setattr(
            cli_write, "time",
            types.SimpleNamespace(monotonic=time.monotonic, sleep=sleeper),
        )

    def test_ctrl_c_during_the_wait_says_the_change_is_saved_then_propagates(
        self, resolved, monkeypatch, capsys
    ):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 4)))
        self._sleep_raises(monkeypatch, KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt) as excinfo:
            cli_write.notify_daemon(resolved, 5, 7, reload_wait=3.0)
        out = capsys.readouterr()
        assert out.out == self.INTERRUPTED and out.err == ""
        assert getattr(excinfo.value, cli_write.COMMITTED_MARK) is True

    def test_a_system_exit_during_the_wait_is_marked_and_propagates(self, resolved, monkeypatch, capsys):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 4)))
        self._sleep_raises(monkeypatch, SystemExit(143))
        with pytest.raises(SystemExit) as excinfo:
            cli_write.notify_daemon(resolved, 5, 7, reload_wait=3.0)
        assert excinfo.value.code == 143
        assert capsys.readouterr().out == self.INTERRUPTED
        assert getattr(excinfo.value, cli_write.COMMITTED_MARK) is True

    def test_the_mark_name_is_exported_and_is_an_attribute_name(self):
        assert "COMMITTED_MARK" in cli_write.__all__
        assert isinstance(cli_write.COMMITTED_MARK, str) and cli_write.COMMITTED_MARK.isidentifier()

    def test_an_ordinary_error_in_the_wait_is_not_an_interrupt(self, resolved, monkeypatch, capsys):
        """A plain Exception is the notice's own failure: it degrades to the
        plain update line plus a warning, and says nothing about interrupts."""
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(7, 4)))
        self._sleep_raises(monkeypatch, RuntimeError("kaboom"))
        cli_write.notify_daemon(resolved, 5, 7, reload_wait=3.0)  # must not raise
        out = capsys.readouterr()
        assert out.out == "config updated\n"
        assert "kaboom" in out.err and "interrupted" not in out.out

    def test_a_normal_wait_leaves_no_mark_and_no_interrupt_line(self, resolved, monkeypatch, capsys):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=KEY))
        cli_write.notify_daemon(resolved, 5, 7, reload_wait=3.0)
        assert "interrupted" not in capsys.readouterr().out

    def test_ctrl_c_through_a_real_command_leaves_the_write_and_says_so_once(
        self, run, rich, db, monkeypatch
    ):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(1, 0)))
        self._sleep_raises(monkeypatch, KeyboardInterrupt())
        gen = generation(db)
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 130, (result.exit_code, result.stderr)
        assert result.stdout.splitlines()[-1] == (
            f"config updated (generation {gen + 1}); interrupted while waiting for daemon pid "
            f"4321 to acknowledge (the change is saved; it picks up the change within {POLL}s)"
        )
        assert result.stderr == "interrupted (the change is saved)\n"
        assert result.stdout.count("interrupted while waiting") == 1
        assert generation(db) == gen + 1  # durable

    def test_sigterm_through_a_real_command_says_terminated_and_exits_143(
        self, run, rich, db, monkeypatch
    ):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(1, 0)))
        self._sleep_raises(monkeypatch, cli_main._Terminated())
        gen = generation(db)
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 143, (result.exit_code, result.stderr)
        assert result.stderr == "terminated (the change is saved)\n"
        assert generation(db) == gen + 1

    def test_an_import_interrupted_in_the_wait_does_not_claim_it_may_not_have_applied(
        self, run, db, tmp_path, monkeypatch
    ):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(1, 0)))
        self._sleep_raises(monkeypatch, KeyboardInterrupt())
        src = write_yaml(tmp_path, "datasets:\n  - name: tank/new\n", "new.yaml")
        result = run("import", src, "-c", db)
        assert result.exit_code == 130
        assert result.stderr.startswith("interrupted (the change is saved)")
        assert "may or may not" not in result.stderr
        assert generation(db) == 0

    def test_an_import_interrupted_before_the_commit_still_says_may_or_may_not(
        self, run, db, tmp_path, monkeypatch
    ):
        def interrupted(*a, **k):
            raise KeyboardInterrupt

        monkeypatch.setattr(cli_main, "import_yaml", interrupted)
        src = write_yaml(tmp_path, "datasets:\n  - name: tank/new\n", "new.yaml")
        result = run("import", src, "-c", db)
        assert result.exit_code == 130
        assert "the import may or may not have been applied" in result.stderr
        assert "the change is saved" not in result.stderr

    def test_a_set_interrupted_before_the_commit_is_a_plain_interrupt(self, run, rich, db, monkeypatch):
        def interrupted(*a, **k):
            raise KeyboardInterrupt

        monkeypatch.setattr(cli_write, "write_config", interrupted)
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 130
        assert "the change is saved" not in result.stderr and "config updated" not in result.stdout

    def test_a_rejection_does_not_end_up_as_a_failure(self, run, rich, db, monkeypatch):
        reloaded(monkeypatch, "signalled")
        probes(monkeypatch, info(applied=(0, 0)))  # never answers
        Clock().install(monkeypatch)
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 0

    def test_the_real_poll_interval_is_a_tenth_of_a_second(self):
        assert cli_write.ACK_POLL_SECONDS == 0.1


class TestDeferredAnswers:
    """`deferred=` / `deferred_reason=`: the daemon refused the key for a reason
    that can pass (it re-checks every poll). Not final, unlike `rejected`: the
    loops keep waiting, and only at the deadline report `deferred`."""

    KEY = (7, 5)
    REASON = "the dataset 'tank/new' does not exist"

    def deferred(self, **kw):
        kw.setdefault("applied", (7, 4))
        return info(deferred=self.KEY, deferred_reason=self.REASON, **kw)

    # -- _answer ----------------------------------------------------------

    def test_answer_is_deferred_for_this_key(self):
        assert cli_write._answer(self.deferred(), self.KEY) == "deferred"
        assert cli_write._answer(info(applied=(7, 4), deferred=self.KEY), self.KEY) == "deferred"

    @pytest.mark.parametrize(
        "daemon, expected",
        [
            (dict(applied=(7, 5)), "applied"),
            (dict(applied=(7, 6)), "newer"),
            (dict(applied=(7, 4), target=(7, 6)), "newer"),
            (dict(applied=(7, 4), target=(7, 5)), "accepted"),
            (dict(applied=(7, 4), rejected=(7, 5)), "rejected"),
            (dict(applied=(7, 4), rejected=(7, 6)), "superseded"),
        ],
        ids=["applied", "newer-applied", "newer-target", "target", "rejected", "superseded"],
    )
    def test_every_other_answer_beats_deferred(self, daemon, expected):
        both = info(deferred=self.KEY, deferred_reason=self.REASON, **daemon)
        assert cli_write._answer(both, self.KEY) == expected

    @pytest.mark.parametrize("key", [(7, 4), (8, 5), (8, 9), (0, 5)], ids=["older", "other-store", "other-store-newer", "unstamped"])
    def test_deferred_for_another_key_answers_nothing(self, key):
        assert cli_write._answer(info(applied=(7, 4), deferred=key), self.KEY) is None

    def test_a_torn_read_never_answers_deferred(self):
        torn = daemonlock.DaemonInfo(
            pid=1, generation=None, start="t", deferred=self.KEY, stable=False
        )
        assert cli_write._answer(torn, self.KEY) is None

    # -- _await_answer ------------------------------------------------------

    def wait(self, resolved, wait=3.0, **kw):
        return cli_write._await_answer(resolved, self.KEY, wait, **kw)

    def test_deferred_then_applied_is_applied(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(
            monkeypatch, self.deferred(), self.deferred(), info(applied=self.KEY)
        )
        answer, seen = self.wait(resolved)
        assert answer == "applied" and len(calls) == 3 and clock.sleeps == [0.1, 0.1]
        assert seen.deferred is None

    def test_deferred_at_the_deadline_is_returned_with_its_reason(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, self.deferred())
        answer, seen = self.wait(resolved, wait=1.0)
        assert answer == "deferred"
        assert seen.deferred == self.KEY and seen.deferred_reason == self.REASON
        assert len(calls) > 5 and sum(clock.sleeps) == pytest.approx(1.0)   # it did keep waiting

    def test_deferred_without_a_reason_at_the_deadline(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4), deferred=self.KEY))
        answer, seen = self.wait(resolved, wait=0.5)
        assert answer == "deferred" and seen.deferred_reason is None

    def test_deferred_then_not_deferred_is_no_answer(self, resolved, monkeypatch):
        """The daemon dropped the deferral (a newer write replaced the key, say)
        without applying: nothing is "deferred" at the deadline any more."""
        Clock().install(monkeypatch)
        probes(monkeypatch, self.deferred(), self.deferred(), info(applied=(7, 4)))
        answer, last = self.wait(resolved, wait=0.5)
        assert answer is None and last.deferred is None

    def test_a_torn_read_claiming_deferred_leaves_no_flag(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        torn = daemonlock.DaemonInfo(
            pid=4321, generation=None, start="t", deferred=self.KEY, stable=False
        )
        probes(monkeypatch, info(applied=(7, 4)), torn, torn)
        answer, last = self.wait(resolved, wait=0.3)
        assert answer is None and last.deferred is None

    def test_a_torn_read_does_not_erase_the_last_stable_deferred(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        torn = daemonlock.DaemonInfo(pid=4321, generation=None, start="t", stable=False)
        probes(monkeypatch, self.deferred(), torn, torn)
        answer, seen = self.wait(resolved, wait=0.3)
        assert answer == "deferred" and seen.deferred_reason == self.REASON

    def test_deferred_then_rejected_is_rejected(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        probes(monkeypatch, self.deferred(), info(applied=(7, 4), rejected=self.KEY))
        assert self.wait(resolved)[0] == "rejected" and clock.sleeps == [0.1]

    def test_deferred_then_accepted_is_accepted_for_the_plain_loop(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, self.deferred(), info(applied=(7, 4), target=self.KEY))
        assert self.wait(resolved)[0] == "accepted"

    def test_deferred_then_gone_is_gone(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, self.deferred(), None)
        assert self.wait(resolved)[0] == "gone"

    # -- await_applied ------------------------------------------------------

    def test_await_applied_deferred_then_applied(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, self.deferred(), info(applied=self.KEY))
        assert cli_write.await_applied(resolved, self.KEY, 3.0)[0] == "applied"

    def test_await_applied_deferred_at_the_deadline(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        probes(monkeypatch, self.deferred())
        answer, seen = cli_write.await_applied(resolved, self.KEY, 1.0)
        assert answer == "deferred" and seen.deferred_reason == self.REASON
        assert sum(clock.sleeps) == pytest.approx(1.0)

    def test_await_applied_deferred_then_accepted_ends_as_none(self, resolved, monkeypatch):
        """`accepted` is not applied: at the deadline the last read was not
        deferred any more, so the answer is the plain "not applied in time"."""
        Clock().install(monkeypatch)
        probes(monkeypatch, self.deferred(), info(applied=(7, 4), target=self.KEY))
        answer, last = cli_write.await_applied(resolved, self.KEY, 0.5)
        assert answer is None and last.target == self.KEY

    def test_await_applied_deferred_then_rejected(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, self.deferred(), info(applied=(7, 4), rejected=self.KEY))
        assert cli_write.await_applied(resolved, self.KEY, 3.0)[0] == "rejected"

    @pytest.mark.parametrize("other", [(7, 4), (8, 6), (0, 6)], ids=["older", "other-store", "unstamped"])
    def test_await_applied_ignores_a_deferral_of_another_key(self, resolved, monkeypatch, other):
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4), deferred=other))
        assert cli_write.await_applied(resolved, self.KEY, 0.5)[0] is None

    # -- the notice ---------------------------------------------------------

    def notice(self, resolved, monkeypatch, daemon, wait=3.0):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        probes(monkeypatch, daemon)
        return cli_write._daemon_notice(resolved, 5, 7, wait)

    def test_the_notice_with_a_reason(self, resolved, monkeypatch):
        line, warning = self.notice(
            resolved, monkeypatch,
            info(applied=(7, 4), deferred=(7, 5), deferred_reason=self.REASON),
        )
        assert line == (
            "config updated (generation 5); reload requested from daemon pid 4321; the daemon "
            f"has not applied generation 5 yet: {self.REASON}; it re-checks every poll (10 s)"
        )
        assert warning is None

    def test_the_notice_without_a_reason(self, resolved, monkeypatch):
        line, warning = self.notice(resolved, monkeypatch, info(applied=(7, 4), deferred=(7, 5)))
        assert line == (
            "config updated (generation 5); reload requested from daemon pid 4321; the daemon "
            "has not applied generation 5 yet: see the daemon log; it re-checks every poll (10 s)"
        )
        assert warning is None

    def test_the_notice_after_a_deferral_that_cleared_is_the_unacknowledged_line(
        self, resolved, monkeypatch
    ):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4), deferred=(7, 5)), info(applied=(7, 4)))
        line, warning = cli_write._daemon_notice(resolved, 5, 7, 0.3)
        assert "not yet acknowledged" in line and "has not applied" not in line and warning is None

    def test_a_deferred_write_is_exit_0_with_stdout_only(self, run, rich, db, monkeypatch):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        sid, gen = store_key(db)
        probes(
            monkeypatch,
            info(applied=(sid, gen), deferred=(sid, gen + 1), deferred_reason=self.REASON),
        )
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert result.stdout.splitlines()[-1] == (
            f"config updated (generation {gen + 1}); reload requested from daemon pid 4321; "
            f"the daemon has not applied generation {gen + 1} yet: {self.REASON}; "
            "it re-checks every poll (10 s)"
        )
        assert "warning" not in result.stderr and "rejected" not in result.stderr

    def test_a_rejection_still_warns_on_stderr(self, run, rich, db, monkeypatch):
        """Unchanged: only `rejected` is final and warned about."""
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        sid, gen = store_key(db)
        probes(monkeypatch, info(applied=(sid, gen), rejected=(sid, gen + 1)))
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 0
        assert f"rejected generation {gen + 1}" in result.stderr


class TestSupersededDeferred:
    """A LATER generation of this store is deferred: ours was folded into it.
    Not final either -- the daemon re-checks every poll -- so the loops keep
    waiting and only at the deadline report `superseded-deferred`."""

    KEY = (7, 5)
    LATER = (7, 6)
    REASON = "the dataset 'tank/new' does not exist"

    def later(self, **kw):
        kw.setdefault("applied", (7, 4))
        kw.setdefault("deferred", self.LATER)
        kw.setdefault("deferred_reason", self.REASON)
        return info(**kw)

    # -- _answer ----------------------------------------------------------

    @pytest.mark.parametrize("later", [(7, 6), (7, 99)], ids=["next", "far"])
    def test_answer_for_a_later_deferred_key_of_this_store(self, later):
        assert cli_write._answer(self.later(deferred=later), self.KEY) == "superseded-deferred"

    @pytest.mark.parametrize("other", [(7, 4), (8, 6), (8, 5), (0, 99)], ids=["older", "other-store", "other-store-same", "unstamped"])
    def test_older_or_other_store_deferrals_answer_nothing(self, other):
        assert cli_write._answer(self.later(deferred=other), self.KEY) is None

    def test_a_deferral_of_this_exact_key_is_deferred_not_superseded(self):
        assert cli_write._answer(self.later(deferred=self.KEY), self.KEY) == "deferred"

    @pytest.mark.parametrize(
        "daemon, expected",
        [
            (dict(applied=(7, 5)), "applied"),
            (dict(applied=(7, 6)), "newer"),
            (dict(applied=(7, 4), target=(7, 5)), "accepted"),
            (dict(applied=(7, 4), target=(7, 6)), "newer"),
            (dict(applied=(7, 4), rejected=(7, 5)), "rejected"),
            (dict(applied=(7, 4), rejected=(7, 7)), "superseded"),
        ],
        ids=["applied", "applied-later", "target", "target-later", "rejected", "rejected-later"],
    )
    def test_every_other_answer_beats_it(self, daemon, expected):
        assert cli_write._answer(self.later(**daemon), self.KEY) == expected

    def test_a_torn_read_never_answers_it(self):
        torn = daemonlock.DaemonInfo(
            pid=1, generation=None, start="t", deferred=self.LATER, stable=False
        )
        assert cli_write._answer(torn, self.KEY) is None

    # -- _await_answer --------------------------------------------------------

    def wait(self, resolved, wait=3.0):
        return cli_write._await_answer(resolved, self.KEY, wait)

    def test_the_deadline_returns_it_with_the_reason(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, self.later())
        answer, seen = self.wait(resolved, wait=1.0)
        assert answer == "superseded-deferred"
        assert seen.deferred == self.LATER and seen.deferred_reason == self.REASON
        assert len(calls) > 5 and sum(clock.sleeps) == pytest.approx(1.0)   # it kept waiting

    def test_it_resolves_to_applied_mid_wait(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        probes(monkeypatch, self.later(), self.later(), info(applied=self.KEY))
        assert self.wait(resolved)[0] == "applied" and clock.sleeps == [0.1, 0.1]

    def test_it_resolves_to_newer_mid_wait(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, self.later(), info(applied=self.LATER))
        assert self.wait(resolved)[0] == "newer"

    def test_it_resolves_to_rejected_mid_wait(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, self.later(), info(applied=(7, 4), rejected=self.LATER))
        assert self.wait(resolved)[0] == "superseded"

    @pytest.mark.parametrize(
        "sequence, expected",
        [
            (("later", "own"), "deferred"),
            (("own", "later"), "superseded-deferred"),
            (("later", "own", "later"), "superseded-deferred"),
        ],
        ids=["to-own", "to-later", "there-and-back"],
    )
    def test_flipping_between_deferred_and_superseded_returns_the_last_word(
        self, resolved, monkeypatch, sequence, expected
    ):
        Clock().install(monkeypatch)
        states = {"own": self.later(deferred=self.KEY), "later": self.later()}
        probes(monkeypatch, *[states[name] for name in sequence])
        assert self.wait(resolved, wait=0.5)[0] == expected

    def test_a_deferral_that_goes_away_is_no_answer(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, self.later(), info(applied=(7, 4)))
        answer, last = self.wait(resolved, wait=0.5)
        assert answer is None and last.deferred is None

    def test_a_torn_read_leaves_no_flag(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        torn = daemonlock.DaemonInfo(
            pid=4321, generation=None, start="t", deferred=self.LATER, stable=False
        )
        probes(monkeypatch, info(applied=(7, 4)), torn, torn)
        assert self.wait(resolved, wait=0.3)[0] is None

    def test_await_applied_keeps_waiting_and_reports_it_at_the_deadline(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        probes(monkeypatch, self.later())
        answer, seen = cli_write.await_applied(resolved, self.KEY, 1.0)
        assert answer == "superseded-deferred" and seen.deferred_reason == self.REASON
        assert sum(clock.sleeps) == pytest.approx(1.0)

    def test_await_applied_resolves_to_applied(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, self.later(), info(applied=self.KEY))
        assert cli_write.await_applied(resolved, self.KEY, 3.0)[0] == "applied"

    # -- the notice -----------------------------------------------------------

    def notice(self, resolved, monkeypatch, daemon):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        probes(monkeypatch, daemon)
        return cli_write._daemon_notice(resolved, 5, 7, 3.0)

    def test_the_notice_with_a_reason(self, resolved, monkeypatch):
        line, warning = self.notice(resolved, monkeypatch, self.later())
        assert line == (
            "config updated (generation 5); reload requested from daemon pid 4321; superseded "
            f"by generation 6, which the daemon has not applied yet: {self.REASON}; "
            "it re-checks every poll (10 s)"
        )
        assert warning is None

    def test_the_notice_without_a_reason(self, resolved, monkeypatch):
        line, warning = self.notice(resolved, monkeypatch, self.later(deferred_reason=None))
        assert line == (
            "config updated (generation 5); reload requested from daemon pid 4321; superseded "
            "by generation 6, which the daemon has not applied yet: see the daemon log; "
            "it re-checks every poll (10 s)"
        )
        assert warning is None

    def test_a_superseded_deferred_write_is_exit_0_with_stdout_only(self, run, rich, db, monkeypatch):
        reloaded(monkeypatch, "signalled")
        Clock().install(monkeypatch)
        sid, gen = store_key(db)
        probes(
            monkeypatch,
            info(applied=(sid, gen), deferred=(sid, gen + 2), deferred_reason=self.REASON),
        )
        result = run("set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert result.stdout.splitlines()[-1] == (
            f"config updated (generation {gen + 1}); reload requested from daemon pid 4321; "
            f"superseded by generation {gen + 2}, which the daemon has not applied yet: "
            f"{self.REASON}; it re-checks every poll (10 s)"
        )
        assert "warning" not in result.stderr and "rejected" not in result.stderr


class TestAwaitApplied:
    """`write.await_applied`: the ack loop that settles only on `applied`.
    `accepted` ("it is switching over") and a newer `target` alone keep it
    polling -- `rename --clear-stale-props` may touch ZFS only once every
    worker runs the rename."""

    KEY = (7, 5)

    def wait(self, resolved, wait=3.0):
        return cli_write.await_applied(resolved, self.KEY, wait)

    def test_it_is_public(self):
        assert "await_applied" in cli_write.__all__

    def test_applied_returns_at_once(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=self.KEY))
        answer, seen = self.wait(resolved)
        assert answer == "applied" and seen.applied == self.KEY
        assert len(calls) == 1 and clock.sleeps == []

    def test_accepted_keeps_polling_until_the_deadline(self, resolved, monkeypatch):
        """Where `_await_answer` stops on `accepted`, this does not."""
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=(7, 4), target=self.KEY))
        answer, last = self.wait(resolved, wait=1.0)
        assert answer is None
        assert last.target == self.KEY and last.applied == (7, 4)
        assert len(calls) > 5 and sum(clock.sleeps) == pytest.approx(1.0)

    def test_accepted_then_applied_returns_applied(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(
            monkeypatch,
            info(applied=(7, 4), target=self.KEY), info(applied=(7, 4), target=self.KEY),
            info(applied=self.KEY),
        )
        answer, seen = self.wait(resolved)
        assert answer == "applied" and len(calls) == 3 and clock.sleeps == [0.1, 0.1]

    def test_the_same_state_is_an_answer_for_the_plain_loop(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4), target=self.KEY))
        assert cli_write._await_answer(resolved, self.KEY, 3.0)[0] == "accepted"

    @pytest.mark.parametrize("applied", [(7, 6), (7, 99)], ids=["next", "far"])
    def test_a_newer_applied_generation_is_newer(self, resolved, monkeypatch, applied):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=applied))
        answer, seen = self.wait(resolved)
        assert answer == "newer" and seen.applied == applied
        assert len(calls) == 1 and clock.sleeps == []

    @pytest.mark.parametrize(
        "daemon",
        [dict(applied=(7, 4), target=(7, 6)), dict(applied=(7, 4), target=(7, 99))],
        ids=["next", "far"],
    )
    def test_a_newer_target_alone_is_not_newer(self, resolved, monkeypatch, daemon):
        """It is only switching to a later generation: our key is not applied."""
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(**daemon))
        answer, last = self.wait(resolved, wait=1.0)
        assert answer is None and last.target == daemon["target"]
        assert len(calls) > 5 and sum(clock.sleeps) == pytest.approx(1.0)

    def test_a_newer_target_then_a_newer_applied_is_newer(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4), target=(7, 6)), info(applied=(7, 6)))
        assert self.wait(resolved)[0] == "newer"

    def test_rejected_returns_at_once(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=(7, 4), rejected=self.KEY))
        answer, seen = self.wait(resolved)
        assert answer == "rejected" and seen.rejected == self.KEY
        assert len(calls) == 1 and clock.sleeps == []

    def test_a_later_rejected_generation_returns_superseded_at_once(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=(7, 4), rejected=(7, 6)))
        assert self.wait(resolved)[0] == "superseded"
        assert len(calls) == 1 and clock.sleeps == []

    def test_gone_and_unreadable_end_the_wait(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4)), None)
        answer, last = self.wait(resolved)
        assert answer == "gone" and last.applied == (7, 4)
        probes(monkeypatch, OSError(errno.EIO, "io"))
        assert self.wait(resolved) == ("unreadable", None)

    @pytest.mark.parametrize(
        "daemon",
        [
            dict(applied=(8, 5)), dict(applied=(8, 99)), dict(applied=(0, 99)),
            dict(applied=(7, 4), target=(8, 5)), dict(applied=(7, 4), target=(8, 99)),
            dict(applied=(7, 4), rejected=(8, 5)), dict(applied=(7, 4), rejected=(8, 99)),
            dict(applied=(8, 5), target=(8, 5)), dict(),
        ],
        ids=[
            "other-store-same-gen", "other-store-newer-gen", "unstamped-store",
            "other-target", "other-newer-target", "other-rejected", "other-newer-rejected",
            "other-applied-and-target", "nothing",
        ],
    )
    def test_a_different_store_id_never_answers(self, resolved, monkeypatch, daemon):
        """A re-import picks a random store id: keys of different stores are
        not ordered, so none of them is applied, newer, rejected or superseded."""
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(**daemon))
        answer, last = self.wait(resolved, wait=1.0)
        assert answer is None and last is not None
        assert len(calls) > 5 and sum(clock.sleeps) == pytest.approx(1.0)

    def test_a_torn_read_answers_nothing(self, resolved, monkeypatch):
        Clock().install(monkeypatch)
        torn = daemonlock.DaemonInfo(pid=4321, generation=None, start="t", applied=self.KEY, stable=False)
        calls = probes(monkeypatch, torn, torn, info(applied=self.KEY))
        answer, seen = self.wait(resolved)
        assert answer == "applied" and len(calls) == 3

    def test_a_zero_wait_looks_once(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        calls = probes(monkeypatch, info(applied=(7, 4), target=self.KEY))
        assert self.wait(resolved, wait=0)[0] is None
        assert len(calls) == 1 and clock.sleeps == []

    def test_the_default_wait_is_reload_wait(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4)))
        assert cli_write.await_applied(resolved, self.KEY)[0] is None
        assert sum(clock.sleeps) == pytest.approx(cli_write.DEFAULT_RELOAD_WAIT)

    def test_the_default_wait_follows_the_group_option(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4)))
        with click.Context(click.Command("x")) as ctx:
            ctx.obj = types.SimpleNamespace(reload_wait=0.5)
            assert cli_write.await_applied(resolved, self.KEY)[0] is None
        assert sum(clock.sleeps) == pytest.approx(0.5)

    def test_an_explicit_wait_beats_the_option(self, resolved, monkeypatch):
        clock = Clock().install(monkeypatch)
        probes(monkeypatch, info(applied=(7, 4)))
        with click.Context(click.Command("x")) as ctx:
            ctx.obj = types.SimpleNamespace(reload_wait=9.0)
            assert cli_write.await_applied(resolved, self.KEY, 0.25)[0] is None
        assert sum(clock.sleeps) == pytest.approx(0.25)

    def test_it_asks_for_applied_only(self, resolved, monkeypatch):
        seen = []

        def spy(resolved, key, wait, **kwargs):
            seen.append((key, wait, kwargs))
            return "applied", None

        monkeypatch.setattr(cli_write, "_await_answer", spy)
        assert cli_write.await_applied(resolved, self.KEY, 2.0) == ("applied", None)
        assert seen == [(self.KEY, 2.0, {"applied_only": True})]

    def test_it_reads_the_pid_file_of_a_real_daemon(self, resolved, db, fake_daemon):
        """No patching: a real pid file, a real held lock, the real clock."""
        daemon = fake_daemon(db, applied=(7, 4), target=self.KEY)
        started = time.monotonic()
        assert cli_write.await_applied(resolved, self.KEY, 0.3)[0] is None
        assert time.monotonic() - started >= 0.25
        daemon.set(applied=self.KEY)
        assert cli_write.await_applied(resolved, self.KEY, 0.3)[0] == "applied"


# --------------------------------------------------------------------------
# 6. --reload-wait
# --------------------------------------------------------------------------


@pytest.fixture
def wait_spy(monkeypatch):
    """`request_reload` says `signalled`; `_await_answer` records the wait it
    was given and returns no answer."""
    seen = []
    monkeypatch.setattr(
        daemonlock, "request_reload",
        lambda r, **k: daemonlock.ReloadRequest("signalled", 4321, None, ""),
    )

    def spy(resolved, key, wait):
        seen.append(wait)
        return None, None

    monkeypatch.setattr(cli_write, "_await_answer", spy)
    return seen


class TestReloadWaitOption:
    def test_the_default_is_three_seconds(self, run, rich, db, wait_spy):
        assert run("set", "check_interval", "10m", "-c", db).exit_code == 0
        assert wait_spy == [3.0] == [cli_write.DEFAULT_RELOAD_WAIT]

    def test_the_default_without_a_click_context(self):
        assert click.get_current_context(silent=True) is None
        assert cli_write._reload_wait() == 3.0

    def test_the_default_when_the_context_carries_no_state(self):
        with click.Context(click.Command("x")) as ctx:
            ctx.obj = None
            assert cli_write._reload_wait() == 3.0

    def test_a_value_is_passed_through(self, run, rich, db, wait_spy):
        assert run("--reload-wait", "2.5", "set", "check_interval", "10m", "-c", db).exit_code == 0
        assert wait_spy == [2.5]

    def test_notify_daemon_argument_beats_the_option(self, resolved, wait_spy):
        cli_write.notify_daemon(resolved, 5, 7, reload_wait=0.75)
        assert wait_spy == [0.75]

    def test_zero_means_no_wait_and_probe_is_never_called(self, run, rich, db, monkeypatch):
        monkeypatch.setattr(
            daemonlock, "request_reload",
            lambda r, **k: daemonlock.ReloadRequest("signalled", 4321, None, ""),
        )
        monkeypatch.setattr(daemonlock, "probe", lambda r: pytest.fail("probe called with --reload-wait 0"))
        result = run("--reload-wait", "0", "set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 0, result.stderr
        last = result.stdout.splitlines()[-1]
        assert last.endswith("reload requested from daemon pid 4321 (SIGHUP)")
        assert "not yet acknowledged" not in last

    def test_zero_does_not_change_non_signalled_rows(self, run, rich, db, monkeypatch):
        monkeypatch.setattr(daemonlock, "request_reload", lambda r, **k: daemonlock.ReloadRequest("no-daemon"))
        result = run("--reload-wait", "0", "set", "check_interval", "10m", "-c", db)
        assert "no running daemon detected" in result.stdout

    @pytest.mark.parametrize("value", ["-1", "-0.5", "nan", "inf", "-inf", "NaN", "Infinity"])
    def test_a_bad_value_is_a_usage_error_and_writes_nothing(self, run, rich, db, wait_spy, value):
        state = db_state(db)
        result = run("--reload-wait", value, "set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 2, (value, result.stdout, result.stderr)
        assert "--reload-wait" in result.stderr and "finite number of seconds" in result.stderr
        assert db_state(db) == state and wait_spy == []

    def test_not_a_number_is_a_usage_error(self, run, rich, db):
        assert run("--reload-wait", "soon", "set", "check_interval", "10m", "-c", db).exit_code == 2

    def test_the_option_is_not_accepted_after_the_subcommand(self, run, rich, db, wait_spy):
        state = db_state(db)
        result = run("set", "check_interval", "10m", "--reload-wait", "0", "-c", db)
        assert result.exit_code == 2 and "No such option" in result.stderr
        assert db_state(db) == state

    def test_it_is_documented(self, run):
        out = run("--help").stdout
        assert "--reload-wait" in out and "SECONDS" in out

    @pytest.mark.parametrize("name, code, command", WRITES, ids=[w[0] for w in WRITES])
    def test_it_reaches_every_writing_command(self, ctx, db, wait_spy, name, code, command):
        """Each writer's notice reads the group option (not the default)."""
        original = ctx.run

        def with_wait(*args, **kw):
            return original("--reload-wait", "1.5", *args, **kw)

        ctx.run = with_wait
        result = command(ctx)
        assert result.exit_code == code, result.stderr
        assert wait_spy == [1.5], name


# --------------------------------------------------------------------------
# 3b. the signal does not depend on the output
# --------------------------------------------------------------------------


class TestBeginReload:
    """`begin_reload` asks for the reload; `notify_daemon(request=...)` reports
    it without asking again."""

    def test_it_asks_once_with_the_key(self, resolved, monkeypatch):
        asked = reloaded(monkeypatch, "no-daemon")
        got = cli_write.begin_reload(resolved, 5, 7)
        assert got.outcome == "no-daemon" and asked == [(7, 5)]

    @pytest.mark.parametrize("generation, store_id", [(5, None), (None, 7), (None, None)])
    def test_without_a_complete_key_it_skips_nothing(self, resolved, monkeypatch, generation, store_id):
        asked = reloaded(monkeypatch, "no-daemon")
        cli_write.begin_reload(resolved, generation, store_id)
        assert asked == [None]

    def test_a_store_id_of_zero_is_still_a_key(self, resolved, monkeypatch):
        asked = reloaded(monkeypatch, "no-daemon")
        cli_write.begin_reload(resolved, 0, 0)
        assert asked == [(0, 0)]

    def test_it_never_raises(self, resolved, monkeypatch):
        def boom(r, **k):
            raise RuntimeError("boom")

        monkeypatch.setattr(daemonlock, "request_reload", boom)
        got = cli_write.begin_reload(resolved, 5, 7)
        assert got.outcome == "error" and got.detail == "RuntimeError: boom"

    def test_a_failing_request_is_a_warning_naming_the_exception(self, resolved, monkeypatch, capsys):
        monkeypatch.setattr(
            daemonlock, "request_reload", lambda r, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        cli_write.notify_daemon(resolved, 5, 7)
        out = capsys.readouterr()
        assert out.out == "config updated (generation 5)\n"
        assert out.err == (
            f"warning: could not check whether a daemon is running for {resolved.resolved_path}: "
            "RuntimeError: boom\n"
        )

    def test_it_is_exported(self):
        assert "begin_reload" in cli_write.__all__

    def test_notify_with_a_request_does_not_ask_again(self, resolved, monkeypatch, capsys):
        asked = reloaded(monkeypatch, "no-daemon")
        request = daemonlock.ReloadRequest("not-permitted", 777)
        cli_write.notify_daemon(resolved, 5, 7, request=request)
        assert asked == []
        assert "daemon pid 777 runs as another user" in capsys.readouterr().out

    def test_notify_without_a_request_asks_exactly_once(self, resolved, monkeypatch):
        asked = reloaded(monkeypatch, "no-daemon")
        cli_write.notify_daemon(resolved, 5, 7)
        assert asked == [(7, 5)]

    def test_a_signalled_request_still_waits_for_its_answer(self, resolved, monkeypatch, capsys):
        asked = reloaded(monkeypatch, "no-daemon")
        probes(monkeypatch, info(applied=KEY))
        cli_write.notify_daemon(
            resolved, 5, 7, request=daemonlock.ReloadRequest("signalled", 4321)
        )
        assert asked == []
        assert capsys.readouterr().out.strip().endswith("it is now on generation 5")


class _Dead:
    """A stdout that is gone: every non-error echo raises like a closed pipe."""

    def install(self, monkeypatch):
        self.echoes = []
        real = click.echo

        def echo(message=None, file=None, nl=True, err=False, color=None):
            if err:
                return real(message, file=file, nl=nl, err=err, color=color)
            self.echoes.append(message)
            raise BrokenPipeError(errno.EPIPE, "Broken pipe")

        monkeypatch.setattr(click, "echo", echo)
        return self


class TestSignalIsIndependentOfOutput:
    @pytest.mark.parametrize("name, code, command", WRITES, ids=[w[0] for w in WRITES])
    def test_a_closed_stdout_still_costs_neither_the_write_nor_the_signal(
        self, ctx, db, reload_calls, monkeypatch, name, code, command
    ):
        before = store_key(db)
        dead = _Dead().install(monkeypatch)
        result = command(ctx)
        assert dead.echoes, "the premise: the command tried to print"
        if name == "rename-destination-with-anchor-copies":
            # It reports each anchor copy BEFORE the commit: a dead stdout
            # stops it there, so there is no write to announce.
            assert reload_calls == [] and store_key(db) == before
            return
        assert len(reload_calls) == 1, f"{name}: the reload was requested {len(reload_calls)} times"
        # the write is durable, and the call saw the DB at the written key
        (call,) = reload_calls
        assert call.skip == call.stored
        assert store_key(db)[1] >= before[1] + 1 or name == "import"
        assert result.exit_code != 0 or name.startswith("rename-dataset-clear")  # EPIPE is not hidden

    @pytest.mark.parametrize("name, code, command", WRITES, ids=[w[0] for w in WRITES])
    def test_the_reload_is_requested_right_after_the_commit_before_any_output(
        self, ctx, db, monkeypatch, name, code, command
    ):
        events = []
        monkeypatch.setattr(
            daemonlock, "request_reload",
            lambda r, **k: (events.append("reload"), daemonlock.ReloadRequest("no-daemon"))[1],
        )
        for module, attr in ((cli_write, "write_config"), (cli_main, "import_yaml")):
            real_fn = getattr(module, attr)

            def wrapped(*a, _real=real_fn, **k):
                result = _real(*a, **k)
                events.append("commit")
                return result

            monkeypatch.setattr(module, attr, wrapped)
        real = click.echo

        def echo(message=None, file=None, nl=True, err=False, color=None):
            if not err:
                events.append("echo")
            return real(message, file=file, nl=nl, err=err, color=color)

        monkeypatch.setattr(click, "echo", echo)
        result = command(ctx)
        assert result.exit_code == code, result.stderr
        assert events.count("commit") == 1 and events.count("reload") == 1, events
        after_commit = events[events.index("commit") + 1:]
        assert after_commit[0] == "reload", f"{name}: output before the reload: {events}"
        assert "echo" in after_commit[1:], "no outcome line after the request"

    def test_rename_dataset_asks_before_clearing_stale_properties(self, run, rich, db, fake_zfs, monkeypatch):
        fake_zfs.add("tank/vm.01", {"org.zfsbackup:config": "cfg", ANCHOR + "offsite": "s"})
        fake_zfs.add("tank/vm.02", {})
        seen = []

        def request(r, **k):
            seen.append(list(fake_zfs.mutations))
            return daemonlock.ReloadRequest("no-daemon")

        monkeypatch.setattr(daemonlock, "request_reload", request)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert seen == [[]], "the reload was requested after zfs state had been changed"
        assert fake_zfs.mutations, "the premise: the stale properties were cleared afterwards"

    def test_import_makes_exactly_one_call_with_the_import_results_key(
        self, run, rich, db, tmp_path, reload_calls
    ):
        before = store_key(db)
        src = write_yaml(tmp_path, "datasets:\n  - name: tank/new\n", "new.yaml")
        assert run("import", src, "-c", db).exit_code == 0
        (call,) = reload_calls
        assert call.skip == call.stored == (before[0], before[1] + 1)

    def test_import_with_a_closed_stdout_still_asks_once(self, run, rich, db, tmp_path, reload_calls, monkeypatch):
        _Dead().install(monkeypatch)
        src = write_yaml(tmp_path, "datasets:\n  - name: tank/new\n", "new.yaml")
        run("import", src, "-c", db)
        assert len(reload_calls) == 1 and store_key(db)[1] == reload_calls[0].skip[1]


# --------------------------------------------------------------------------
# 3c. `rename destination` re-checks the store before touching ZFS
# --------------------------------------------------------------------------


class TestRenameDestinationStoreCheck:
    @pytest.fixture
    def anchored(self, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snap"})
        fake_zfs.add("tank/other", {})
        return fake_zfs

    @staticmethod
    def rename_with(snapshot, resolved):
        from zfsbackup.cli import rename

        rename.rename_destination(
            resolved, snapshot, "offsite", "offsite2", warn=lambda message: None
        )

    def test_a_replaced_file_is_a_store_id_conflict_before_any_anchor_is_copied(
        self, run, rich, db, anchored, tmp_path
    ):
        resolved = _resolved(db)
        snapshot = cli_write.read_snapshot(resolved)
        dispose_all()
        for suffix in ("", "-wal", "-shm"):
            Path(str(db) + suffix).unlink(missing_ok=True)
        assert run("import", write_yaml(tmp_path, RICH_YAML, "again.yaml"), "-c", db).exit_code == 0
        replaced = store_key(db)
        assert replaced[0] != snapshot.store_id and replaced[1] == snapshot.generation  # same generation!
        with pytest.raises(StoreIdConflict) as excinfo:
            self.rename_with(snapshot, resolved)
        assert (excinfo.value.expected_store_id, excinfo.value.actual_store_id) == (
            snapshot.store_id, replaced[0],
        )
        assert anchored.mutations == []
        assert "offsite2" not in cli_write.read_snapshot(resolved).config.destinations

    def test_a_moved_generation_is_a_config_conflict_before_any_anchor_is_copied(
        self, run, rich, db, anchored
    ):
        resolved = _resolved(db)
        snapshot = cli_write.read_snapshot(resolved)
        assert run("set", "check_interval", "10m", "-c", db).exit_code == 0
        with pytest.raises(ConfigConflict) as excinfo:
            self.rename_with(snapshot, resolved)
        assert not isinstance(excinfo.value, StoreIdConflict)
        assert (excinfo.value.expected, excinfo.value.actual) == (snapshot.generation, snapshot.generation + 1)
        assert anchored.mutations == []

    def test_a_snapshot_of_an_unstamped_database_compares_generations_only(
        self, run, rich, db, anchored
    ):
        import dataclasses

        resolved = _resolved(db)
        snapshot = dataclasses.replace(cli_write.read_snapshot(resolved), store_id=0)
        self.rename_with(snapshot, resolved)  # the file really has a non-zero id: not compared
        assert anchored.mutations, "the anchor copy should have run"
        assert "offsite2" in cli_write.read_snapshot(resolved).config.destinations

    def test_an_unstamped_snapshot_still_refuses_a_moved_generation(self, run, rich, db, anchored):
        import dataclasses

        resolved = _resolved(db)
        snapshot = dataclasses.replace(cli_write.read_snapshot(resolved), store_id=0)
        assert run("set", "check_interval", "10m", "-c", db).exit_code == 0
        with pytest.raises(ConfigConflict):
            self.rename_with(snapshot, resolved)
        assert anchored.mutations == []

    def test_an_unchanged_store_proceeds_and_announces_once(self, run, rich, db, anchored, reload_calls):
        assert run("rename", "destination", "offsite", "offsite2", "-c", db).exit_code == 0
        assert anchored.mutations
        assert len(reload_calls) == 1

    def test_through_the_cli_a_replaced_file_is_75_and_touches_no_zfs(
        self, run, rich, db, anchored, tmp_path, monkeypatch
    ):
        """End to end: a re-import (a new file) lands between the CLI's read
        of the configuration and its writer-locked check."""
        from zfsbackup.cli import rename

        real = rename.rename_destination

        def after_a_replacement(resolved, snapshot, *args, **kwargs):
            dispose_all()
            for suffix in ("", "-wal", "-shm"):
                Path(str(db) + suffix).unlink(missing_ok=True)
            assert run("import", write_yaml(tmp_path, RICH_YAML, "r.yaml"), "-c", db).exit_code == 0
            return real(resolved, snapshot, *args, **kwargs)

        monkeypatch.setattr(rename, "rename_destination", after_a_replacement)
        result = run("rename", "destination", "offsite", "offsite2", "-c", db)
        assert result.exit_code == 75, result.stderr
        assert "replaced" in result.stderr.splitlines()[0]
        assert anchored.mutations == []
        assert "Renamed destination" not in result.stdout


# --------------------------------------------------------------------------
# 6b. the option's help says where it goes
# --------------------------------------------------------------------------


class TestReloadWaitHelp:
    def test_the_group_help_says_it_goes_before_the_subcommand(self, run):
        out = "".join(run("--help").stdout.split())
        assert "--reload-waitisaglobaloptionandgoesBEFOREthesubcommand" in out

    def test_the_option_help_starts_with_the_placement(self, run):
        out = "".join(run("--help").stdout.split())
        assert "--reload-waitSECONDSGlobaloption:goesbeforethesubcommand." in out

    def test_a_subcommand_does_not_list_it(self, run):
        assert "--reload-wait" not in run("set", "--help").stdout

    def test_after_the_subcommand_it_is_a_usage_error_naming_the_option(self, run, rich, db):
        result = run("set", "check_interval", "10m", "--reload-wait", "0", "-c", db)
        assert result.exit_code == 2 and "--reload-wait" in result.stderr


# --------------------------------------------------------------------------
# 11. a genuine lock holder (Linux)
# --------------------------------------------------------------------------

#: A stand-in daemon: takes the real v2 lock (`daemonlock.acquire`), records
#: every SIGHUP it receives in `<db>.hups`, and on SIGHUP writes `target=` to
#: its pid file the way the real supervisor does after accepting a reload.
_HOLDER = textwrap.dedent(
    """
    import os, signal, sys, time
    from zfsbackup import daemonlock
    from zfsbackup.config.store import read_store_state, resolve_config_path, dispose_all

    db = sys.argv[1]
    resolved = resolve_config_path(db)
    lock = daemonlock.acquire(resolved, generation=0)
    state = read_store_state(resolved)
    dispose_all()
    lock.set_state(applied=(state.store_id, state.generation))
    hups = db + ".hups"
    pending = []

    def on_hup(signum, frame):
        with open(hups, "a") as fh:
            fh.write("HUP\\n")
        pending.append(1)

    signal.signal(signal.SIGHUP, on_hup)
    print("ready", os.getpid(), flush=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if pending:
            pending.clear()
            state = read_store_state(resolved)
            dispose_all()
            lock.set_state(target=(state.store_id, state.generation))
        time.sleep(0.02)
    """
)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the lock holder is verified through /proc/locks")
@pytest.mark.integration
class TestRealLockHolderOnLinux:
    @pytest.fixture
    def holder(self, rich, db):
        from _hermetic import hermetic_env

        proc = subprocess.Popen(
            [sys.executable, "-c", _HOLDER, str(db)], stdout=subprocess.PIPE, text=True,
            cwd=str(REPO), env=hermetic_env(), stderr=subprocess.DEVNULL,
        )
        try:
            line = proc.stdout.readline().split()
            assert line and line[0] == "ready", line
            yield proc
        finally:
            proc.kill()
            proc.wait(timeout=10)
            proc.stdout.close()

    def test_set_signals_it_and_reports_accepted(self, run, db, holder):
        result = run("--reload-wait", "10", "set", "check_interval", "10m", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert (
            f"reload requested from daemon pid {holder.pid}: it accepted generation 1 "
            "and is switching its workers over (in-flight transfers finish first)"
        ) in result.stdout
        assert Path(str(db) + ".hups").read_text() == "HUP\n"

    def test_a_forged_pid_is_never_signalled(self, run, rich, db, fake_daemon):
        decoy = subprocess.Popen(["sleep", "60"])
        try:
            # A pid file that names the decoy, with a plausible start token,
            # while the lock is held by THIS process.
            from zfsbackup import procident

            fake_daemon(db, pid=decoy.pid, start=procident.start_token(decoy.pid) or "x", applied=(1, 0))
            result = run("--reload-wait", "0", "set", "check_interval", "10m", "-c", db)
            assert result.exit_code == 0, result.stderr
            assert "reload requested" not in result.stdout
            time.sleep(0.3)
            assert decoy.poll() is None  # alive: SIGHUP's default action would have killed it
        finally:
            decoy.kill()
            decoy.wait(timeout=10)


# --------------------------------------------------------------------------
# 12. a real daemon, end to end
# --------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.integration
class TestRealDaemonEndToEnd:
    """`python -m zfsbackup.daemon --dry-run` against stub `zfs`, with the CLI
    running beside it. On macOS the CLI cannot verify the lock holder, so it
    does not signal ('unverified' wording) and the daemon applies the change at
    its next poll; on Linux it is signalled and acknowledges. Either way
    `status --wait` ends 0."""

    STARTUP = 60.0

    @pytest.fixture
    def daemon(self, rich, db, tmp_path, run):
        import socket
        import uuid

        from _hermetic import hermetic_env

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        src = write_yaml(
            tmp_path,
            "datasets:\n"
            f"  - name: zfsbackup_cli_reload_{uuid.uuid4().hex[:8]}/nonexistent\n"
            "api_host: 127.0.0.1\n"
            f"api_port: {port}\n",
            "daemon.yaml",
        )
        assert run("import", src, "-c", db).exit_code == 0
        dispose_all()
        log_path = tmp_path / "daemon.log"
        log = open(log_path, "wb")
        proc = subprocess.Popen(
            [sys.executable, "-m", "zfsbackup.daemon", "-c", str(db), "--dry-run"],
            stdout=log, stderr=subprocess.STDOUT, cwd=str(REPO),
            env=hermetic_env(PYTHONUNBUFFERED="1"), start_new_session=True,
        )
        try:
            deadline = time.monotonic() + self.STARTUP
            resolved = _resolved(db)
            while True:
                if proc.poll() is not None:
                    pytest.fail(f"daemon exited early ({proc.returncode}):\n{log_path.read_text(errors='replace')}")
                got = daemonlock.probe(resolved)
                if got is not None and got.pid == proc.pid and got.applied == store_key(db):
                    break
                if time.monotonic() > deadline:
                    pytest.fail(f"daemon never applied the stored key: {got}\n{log_path.read_text(errors='replace')}")
                time.sleep(0.1)
            while "ZFS Backup Daemon Starting" not in log_path.read_text(errors="replace"):
                if time.monotonic() > deadline:
                    pytest.fail(f"daemon never finished starting:\n{log_path.read_text(errors='replace')}")
                time.sleep(0.1)
            yield proc, log_path
        finally:
            log.close()
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass

    def test_set_then_status_wait_ends_in_sync(self, run, db, daemon):
        proc, log_path = daemon
        before = store_key(db)
        result = run("set", "check_interval", "2m", "-c", db)
        assert result.exit_code == 0, result.stderr
        last = result.stdout.splitlines()[-1]
        assert last.startswith(f"config updated (generation {before[1] + 1})")
        if sys.platform.startswith("linux"):
            assert f"reload requested from daemon pid {proc.pid}" in last
        else:
            assert UNVERIFIED_LINUX_ONLY.format(pid=proc.pid) in last
            assert proc.poll() is None and "Received signal" not in log_path.read_text(errors="replace")
        status = run("status", "--wait", "30", "-c", db)
        assert status.exit_code == 0, (status.stdout, status.stderr, log_path.read_text(errors="replace"))
        assert status.stdout.rstrip().endswith("in sync")
        assert daemonlock.probe(_resolved(db)).applied == store_key(db)
        assert proc.poll() is None


# --------------------------------------------------------------------------
# enroll / identity rotate: the once-only rule (TLS/mTLS plan M13, M14c)
#
# `enroll` can commit twice (the generated client id, then the destination), and must ask
# the daemon to reload exactly ONCE, after the last commit that happened -- including on a
# failure path where only the first one did. `identity rotate` commits only for configured
# certificate paths. `reloads` records `(skip_if_applied, key the database held at the call)`.
# --------------------------------------------------------------------------


def _unreachable():
    return f"https://127.0.0.1:{closed_port()}"


def _token_for(fp="ab" * 32):
    return str(identity.make_token(fp)[0])


@pytest.mark.integration
class TestEnrollReloadMatrix:
    @pytest.fixture
    def fresh(self, run, db, reloads):
        """No client id stored: a command that gets past step 3 commits one."""
        make_db(run, db, "datasets:\n  - name: tank/a\n    frequency: 1h\n")
        reloads.clear()
        return db

    # -- nothing committed -> no request
    @pytest.mark.parametrize("how", [
        "usage-no-url", "bad-token", "http-url", "unreachable", "refused", "pin-mismatch",
        "server-400", "server-503",
    ])
    def test_nothing_committed_means_no_request(self, run, client, reloads, make_server, how):
        srv = make_server()
        url, token, extra = srv.url, srv.token(), ()
        if how == "usage-no-url":
            result = do_enroll(run, client, srv, token, url=False)
        else:
            if how == "bad-token":
                token = "nonsense"
            elif how == "http-url":
                url = "http://127.0.0.1:1"
            elif how == "unreachable":
                url = _unreachable()
            elif how == "refused":
                token = make_foreign_token(srv)
            elif how == "pin-mismatch":
                token = _token_for("ee" * 32)
            elif how == "server-400":
                srv.on("/enroll", Reply(400, {"error": "x"}))
            elif how == "server-503":
                srv.on("/enroll", Reply(503, {"retry": True}))
            result = do_enroll(run, client, url, token)
        assert result.exit_code != 0
        assert reloads == []
        assert "generated this host's client id" not in result.stdout

    # -- only the generated client id committed, then a failure -> exactly one
    @pytest.mark.parametrize("how", ["unreachable", "refused", "pin-mismatch", "server-500", "bad-label"])
    def test_only_the_client_id_committed_then_failure_is_one_request(
        self, run, fresh, reloads, make_server, monkeypatch, how
    ):
        srv = make_server()
        url, token, extra = srv.url, srv.token(), ()
        if how == "unreachable":
            url = _unreachable()
        elif how == "refused":
            token = make_foreign_token(srv)
        elif how == "pin-mismatch":
            token = _token_for("ee" * 32)
        elif how == "server-500":
            srv.on("/enroll", Reply(500, "x"))
        elif how == "bad-label":
            extra = ("--label", "x" * 200)          # rejected BEFORE step 3: nothing committed
        notified = []
        real = enroll_cli.notify_daemon
        monkeypatch.setattr(
            enroll_cli, "notify_daemon",
            lambda *a, **k: (notified.append(k), real(*a, **k))[1],
        )
        gen = generation(fresh)
        result = do_enroll(run, fresh, url, token, *extra)
        assert result.exit_code != 0
        if how == "bad-label":
            assert reloads == [] and generation(fresh) == gen and notified == []
            return
        generated = host_client_id(fresh)
        assert generated and generation(fresh) == gen + 1
        assert len(reloads) == 1
        skip, held = reloads[0]
        assert skip == held == store_key(fresh)
        assert f"generated this host's client id: {generated}" in result.stdout
        assert result.stdout.count("config updated (generation") == 1
        (kw,) = notified
        assert kw["reload_wait"] == 0 and kw["request"] is not None      # reported, not waited for

    # -- both committed -> one
    def test_both_commits_are_announced_once_at_the_last_key(self, run, fresh, reloads, make_server):
        srv = make_server()
        gen = generation(fresh)
        result = do_enroll(run, fresh, srv, srv.token())
        assert result.exit_code == 0, result.stderr
        assert generation(fresh) == gen + 2
        assert len(reloads) == 1
        skip, held = reloads[0]
        assert skip == held == store_key(fresh) == (skip[0], gen + 2)
        assert result.stdout.count("config updated (generation") == 1
        assert f"config updated (generation {gen + 2})" in result.stdout

    # -- only step 6 committed (the id was already there) -> one
    def test_only_the_destination_committed_is_one_request(self, run, client, reloads, make_server):
        srv = make_server()
        gen = generation(client)
        assert do_enroll(run, client, srv, srv.token()).exit_code == 0
        assert generation(client) == gen + 1 and len(reloads) == 1
        assert reloads[0][0] == reloads[0][1] == store_key(client)

    def test_a_rerun_that_changes_nothing_is_no_request(self, run, client, reloads, make_server):
        srv = make_server()
        token = srv.token()
        assert do_enroll(run, client, srv, token).exit_code == 0
        reloads.clear()
        assert do_enroll(run, client, srv, token).exit_code == 0
        assert reloads == []

    def test_a_rerun_that_records_a_second_name_is_one_request(self, run, client, reloads, make_server):
        srv = make_server()
        token = srv.token()
        assert do_enroll(run, client, srv, token).exit_code == 0
        reloads.clear()
        assert do_enroll(run, client, srv, token, name="mirror").exit_code == 0
        assert len(reloads) == 1

    # -- a conflict at step 6 after the id was committed -> the id's one request
    def test_a_conflict_at_the_destination_commit_still_announces_the_id(
        self, run, fresh, reloads, make_server, monkeypatch
    ):
        srv = make_server()
        real = enroll_cli._enroll_with_token
        state = {"armed": True}

        def wrapper(*args, **kwargs):
            real(*args, **kwargs)
            if state["armed"]:
                state["armed"] = False
                assert run("set", "check_interval", "10m", "-c", fresh).exit_code == 0
                reloads.pop()                    # the competing `set`'s own request

        monkeypatch.setattr(enroll_cli, "_enroll_with_token", wrapper)
        gen = generation(fresh)
        result = do_enroll(run, fresh, srv, srv.token())
        assert result.exit_code == 75, result.stderr
        # what remains is enroll's one request: for the generated id's commit, made when the
        # failure surfaced (the database had moved on by then)
        assert len(reloads) == 1
        skip, held = reloads[0]
        assert skip == (held[0], gen + 1) and held == (held[0], gen + 2)
        generated = host_client_id(fresh)
        assert f"generated this host's client id: {generated}" in result.stdout

    # -- enroll --new-key
    def test_new_key_on_generated_paths_with_nothing_to_record_requests_nothing(
        self, run, client, reloads, make_server
    ):
        """Same url and pin: no commit, so no request (the daemon follows the file's mtime)."""
        srv = make_server()
        assert do_enroll(run, client, srv, srv.token()).exit_code == 0
        reloads.clear()
        scoped = srv.token(client_id=CID, replace=True)
        before = db_state(client)
        result = do_enroll(run, client, srv, scoped, "--new-key", fname="s")
        assert result.exit_code == 0, result.stderr
        assert reloads == [] and db_state(client) == before

    def test_new_key_for_a_new_destination_name_requests_once(self, run, client, reloads, make_server):
        srv = make_server()
        assert do_enroll(run, client, srv, srv.token()).exit_code == 0
        reloads.clear()
        scoped = srv.token(client_id=CID, replace=True)
        result = do_enroll(run, client, srv, scoped, "--new-key", name="mirror", fname="s")
        assert result.exit_code == 0, result.stderr
        assert len(reloads) == 1 and reloads[0][0] == reloads[0][1] == store_key(client)

    # -- ordering
    def test_the_request_comes_before_any_output_on_success(self, run, fresh, make_server, monkeypatch):
        import click

        srv = make_server()
        events = []
        real_echo = click.echo

        def echo(message=None, *args, **kwargs):
            events.append(("err" if kwargs.get("err") else "out", str(message)))
            return real_echo(message, *args, **kwargs)

        def recorder(resolved, *, skip_if_applied=None):
            events.append(("reload", skip_if_applied))
            return daemonlock.ReloadRequest("no-daemon")

        monkeypatch.setattr(click, "echo", echo)
        monkeypatch.setattr(daemonlock, "request_reload", recorder)
        assert do_enroll(run, fresh, srv, srv.token()).exit_code == 0
        kinds = [k for k, _ in events]
        assert kinds.count("reload") == 1
        assert kinds.index("reload") < kinds.index("out")

    def test_a_closed_stdout_does_not_cost_the_request(self, run, fresh, reloads, make_server, monkeypatch):
        import click

        srv = make_server()
        real_echo = click.echo

        def echo(message=None, *args, **kwargs):
            if not kwargs.get("err"):
                raise BrokenPipeError(32, "Broken pipe")
            return real_echo(message, *args, **kwargs)

        monkeypatch.setattr(click, "echo", echo)
        result = do_enroll(run, fresh, srv, srv.token())
        assert result.exit_code == 0, result.stderr
        assert len(reloads) == 1
        assert stored(fresh).destinations["offsite"].server_fingerprint == srv.fp

    def test_a_signalled_daemon_is_asked_once_and_the_failure_path_does_not_wait(
        self, run, fresh, db, monkeypatch
    ):
        """On the failure path the ack wait is skipped (`reload_wait=0`): a probe of the pid
        file would be a second look at the daemon."""
        probed = []
        monkeypatch.setattr(cli_write.daemonlock, "probe", lambda r: probed.append(1))
        monkeypatch.setattr(
            daemonlock, "request_reload",
            lambda r, **k: daemonlock.ReloadRequest("signalled", 4242),
        )
        result = do_enroll(run, fresh, _unreachable(), _token_for())
        assert result.exit_code == 75
        assert "reload requested from daemon pid 4242" in result.stdout
        assert probed == []


@pytest.mark.integration
class TestRotateReloadMatrix:
    @pytest.fixture
    def enrolled(self, run, client, make_server, reloads):
        srv = make_server()
        assert do_enroll(run, client, srv, srv.token()).exit_code == 0
        reloads.clear()
        return types.SimpleNamespace(db=client, srv=srv, reloads=reloads)

    def test_default_paths_never_request_a_reload(self, run, enrolled):
        before = db_state(enrolled.db)
        result = run("identity", "rotate", "-c", enrolled.db)
        assert result.exit_code == 0, result.stderr
        assert enrolled.reloads == []
        assert "config updated" not in result.stdout
        assert db_state(enrolled.db) == before

    def test_default_paths_failure_requests_nothing(self, run, enrolled):
        enrolled.srv.on("/rotate", Reply(403, {"reason": "refused"}))
        assert run("identity", "rotate", "-c", enrolled.db).exit_code == 1
        assert enrolled.reloads == []

    @pytest.fixture
    def configured(self, run, db, reloads, make_server, tmp_path):
        srv = make_server()
        old = identity.ensure_identity(tmp_path / "mine", identity.ROLE_CLIENT)
        new = identity.ensure_identity(tmp_path / "next", identity.ROLE_CLIENT)
        make_db(run, db, "datasets:\n  - name: tank/a\n    frequency: 1h\n"
                         f'client_cert_file: "{old.cert_path}"\nclient_key_file: "{old.key_path}"\n')
        seed_client_id(db, CID)
        assert do_enroll(run, db, srv, srv.token()).exit_code == 0
        reloads.clear()
        args = ("--cert", str(new.cert_path), "--key", str(new.key_path))
        return types.SimpleNamespace(db=db, srv=srv, reloads=reloads, args=args)

    def test_configured_paths_request_exactly_one_reload_for_the_one_commit(self, run, configured):
        gen = generation(configured.db)
        result = run("identity", "rotate", *configured.args, "-c", configured.db)
        assert result.exit_code == 0, result.stderr
        assert generation(configured.db) == gen + 1
        assert len(configured.reloads) == 1
        skip, held = configured.reloads[0]
        assert skip == held == store_key(configured.db)
        assert result.stdout.count("config updated (generation") == 1

    def test_configured_paths_failure_requests_nothing(self, run, configured):
        configured.srv.on("/rotate", Reply(503, {"retry": True}))
        before = db_state(configured.db)
        assert run("identity", "rotate", *configured.args, "-c", configured.db).exit_code == 75
        assert configured.reloads == [] and db_state(configured.db) == before

    def test_a_conflict_at_the_commit_requests_nothing_for_rotate(self, run, configured, monkeypatch):
        real = enroll_cli.commit_config

        def racing(*args, **kwargs):
            monkeypatch.setattr(enroll_cli, "commit_config", real)
            assert run("set", "check_interval", "10m", "-c", configured.db).exit_code == 0
            configured.reloads.clear()               # the competing `set`'s own request
            return real(*args, **kwargs)

        monkeypatch.setattr(enroll_cli, "commit_config", racing)
        result = run("identity", "rotate", *configured.args, "-c", configured.db)
        assert result.exit_code == 75, result.stderr
        assert configured.reloads == []

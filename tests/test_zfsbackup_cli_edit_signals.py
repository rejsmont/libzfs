"""`zfsbackup-config edit` and signals (plan item 12, review fix 5).

While the editor runs, SIGINT and SIGQUIT are ignored by the CLI (a ^C at the
terminal reaches the whole foreground process group, so the editor sees it and
decides; the CLI must not die and take the buffer with it), SIGTERM is
forwarded to the editor with a grace period before SIGKILL, the previous
handlers are restored afterwards, a forwarded SIGTERM is re-delivered so the CLI
exits 143, and an editor that died of SIGINT is an interrupt (130).

Mutation counts (scratch copy, one revert at a time; failing tests in the edit
test files): SIGINT not ignored 3; SIGQUIT not ignored 4; SIGTERM not forwarded
4; no SIGKILL after the grace period 3; forwarded SIGTERM not re-delivered 5;
editor death by SIGINT not 130 3; handlers not restored 6; editor started after
the signals were ignored (it inherits SIG_IGN) 3.

Every scenario runs the CLI in a **subprocess in its own session** (so a signal
can never reach pytest) with hard timeouts everywhere; the whole process group
is SIGKILLed at teardown whatever happened.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from _cli_support import PLAIN_YAML, db, db_state, isolated, make_db, run  # noqa: F401

REPO = Path(__file__).resolve().parent.parent
SLOW_EDITOR = Path(__file__).resolve().parent / "_slow_editor.py"

DRIVER = (
    "import sys\n"
    "from zfsbackup.cli import editor, main\n"
    "grace = sys.argv[1]\n"
    "if grace:\n"
    "    editor._TERMINATE_GRACE = float(grace)\n"
    "main.main(sys.argv[2:])\n"
)

START_TIMEOUT = 30
FINISH_TIMEOUT = 60

pytestmark = pytest.mark.integration


class RunningCli:
    def __init__(self, process, log, tmp_dir):
        self.process, self.log, self.tmp_dir = process, log, tmp_dir
        self.started_at = None

    def log_text(self) -> str:
        return self.log.read_text() if self.log.exists() else ""

    def wait_for_editor(self) -> None:
        deadline = time.monotonic() + START_TIMEOUT
        while "start pid=" not in self.log_text():
            assert self.process.poll() is None, "the CLI exited before the editor started"
            assert time.monotonic() < deadline, "the editor never started"
            time.sleep(0.05)

    @property
    def editor_pid(self) -> int:
        return int(self.log_text().split("start pid=")[1].split()[0])

    def finish(self):
        out, err = self.process.communicate(timeout=FINISH_TIMEOUT)
        return self.process.returncode, out, err

    def kept_buffers(self):
        return sorted(self.tmp_dir.glob("zfsbackup-config-edit-*/config.yaml"))


@pytest.fixture
def start(tmp_path, db, run):
    """`start(mode, seconds=..., grace=None)` -> a running CLI whose editor is
    `_slow_editor.py` in that mode, in its own session."""
    make_db(run, db, PLAIN_YAML)
    tmp_dir = tmp_path / "sigtmp"
    tmp_dir.mkdir()
    started = []

    def go(mode, seconds=30, grace=None):
        log = tmp_path / f"slow-{len(started)}.log"
        env = dict(
            os.environ, SLOW_LOG=str(log), SLOW_MODE=mode, SLOW_SECONDS=str(seconds),
            PYTHONPATH=str(REPO), TMPDIR=str(tmp_dir),
        )
        for var in ("VISUAL", "EDITOR"):
            env.pop(var, None)
        process = subprocess.Popen(
            [
                sys.executable, "-c", DRIVER, "" if grace is None else str(grace),
                "-c", str(db), "edit", "--no-prompt",
                "--editor", f"{sys.executable} {SLOW_EDITOR}",
            ],
            env=env, cwd=str(tmp_path), start_new_session=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        cli = RunningCli(process, log, tmp_dir)
        started.append(cli)
        return cli

    yield go

    for cli in started:
        try:
            os.killpg(cli.process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            cli.process.communicate(timeout=10)
        except Exception:  # noqa: BLE001
            pass


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


class TestSigintAndSigquitAreIgnored:
    @pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGQUIT])
    def test_the_cli_survives_and_the_edit_completes(self, start, db, sig):
        before = db_state(db)
        cli = start("default", seconds=2)
        cli.wait_for_editor()
        os.kill(cli.process.pid, sig)  # to the CLI only, not its group
        code, out, err = cli.finish()
        assert code == 0, err
        assert "No changes" in out
        log = cli.log_text()
        assert "exit normally" in log and "got " not in log
        assert db_state(db) == before
        assert cli.kept_buffers() == []

    def test_a_signal_to_the_whole_group_reaches_the_editor_not_the_cli(self, start, db):
        # what a terminal ^C does: the editor (default disposition) dies of it,
        # the CLI ignores it and reports the interrupt, with the buffer kept.
        before = db_state(db)
        cli = start("default", seconds=30)
        cli.wait_for_editor()
        os.killpg(cli.process.pid, signal.SIGINT)
        code, out, err = cli.finish()
        assert code == 130, err
        assert "interrupted" in err
        assert len(cli.kept_buffers()) == 1 and "your edit is kept in" in err
        assert db_state(db) == before

    def test_the_editor_does_not_inherit_the_ignored_dispositions(self, start):
        cli = start("report")
        code, out, err = cli.finish()
        assert code == 0, err
        line = cli.log_text().splitlines()[0]
        assert "int=SIG_IGN" not in line and "quit=SIG_IGN" not in line
        assert "term=SIG_DFL" in line


class TestSigtermIsForwarded:
    def test_a_handling_editor_gets_it_and_the_cli_exits_143(self, start, db):
        before = db_state(db)
        cli = start("handle_exit")
        cli.wait_for_editor()
        t0 = time.monotonic()
        os.kill(cli.process.pid, signal.SIGTERM)
        code, out, err = cli.finish()
        assert code == 143, err
        assert time.monotonic() - t0 < 20
        assert "got SIGTERM" in cli.log_text()
        assert "terminated" in err and "your edit is kept in" in err
        assert len(cli.kept_buffers()) == 1
        assert db_state(db) == before
        assert not alive(cli.editor_pid)

    def test_a_default_editor_dies_of_the_forwarded_term(self, start, db):
        before = db_state(db)
        cli = start("default")
        cli.wait_for_editor()
        os.kill(cli.process.pid, signal.SIGTERM)
        code, out, err = cli.finish()
        assert code == 143, err
        assert db_state(db) == before and len(cli.kept_buffers()) == 1
        assert not alive(cli.editor_pid)

    def test_an_editor_that_ignores_it_is_killed_after_the_grace_period(self, start, db):
        before = db_state(db)
        cli = start("ignore_term", grace=1)
        cli.wait_for_editor()
        t0 = time.monotonic()
        os.kill(cli.process.pid, signal.SIGTERM)
        code, out, err = cli.finish()
        elapsed = time.monotonic() - t0
        assert code == 143, err
        assert 0.9 <= elapsed < 30, elapsed  # waited the grace, then did not hang
        log = cli.log_text()
        assert "got SIGTERM" in log and "exit normally" not in log
        assert not alive(cli.editor_pid)
        assert db_state(db) == before and len(cli.kept_buffers()) == 1

    def test_the_default_grace_period_is_ten_seconds(self):
        from zfsbackup.cli import editor

        assert editor._TERMINATE_GRACE == 10.0

    def test_a_normal_run_leaves_no_editor_behind(self, start):
        cli = start("report")
        code, out, err = cli.finish()
        assert code == 0, err
        assert not alive(cli.editor_pid)


class TestEditorDeathBySigint:
    def test_an_editor_killed_by_sigint_is_exit_130(self, start, db):
        before = db_state(db)
        cli = start("sigint_self")
        code, out, err = cli.finish()
        assert code == 130, err
        assert "interrupted" in err
        assert "was killed by SIGINT" not in err   # an interrupt, not an editor failure
        assert len(cli.kept_buffers()) == 1 and "your edit is kept in" in err
        assert db_state(db) == before


HANDLERS_DRIVER = r"""
import json, os, signal, sys
from pathlib import Path
from zfsbackup.cli import editor

sentinels = {}
for sig in (signal.SIGINT, signal.SIGQUIT, signal.SIGTERM):
    def handler(signum, frame, _sig=sig):
        sentinels.setdefault("fired", []).append(int(signum))
    signal.signal(sig, handler)
before = {int(s): signal.getsignal(s) for s in (signal.SIGINT, signal.SIGQUIT, signal.SIGTERM)}
buffer = Path(sys.argv[1]); buffer.write_text("x")
kicker = ("import os, signal, time; p = os.getppid(); "
          "os.kill(p, signal.SIGINT); os.kill(p, signal.SIGQUIT); time.sleep(0.3)")
editor._run_editor([sys.executable, "-c", kicker], buffer)
after = {int(s): signal.getsignal(s) for s in (signal.SIGINT, signal.SIGQUIT, signal.SIGTERM)}
print(json.dumps({
    "restored": before == after,
    "fired": sentinels.get("fired", []),
}))
"""


class TestHandlersAreRestored:
    def test_signals_sent_during_the_editor_are_ignored_and_the_handlers_come_back(self, tmp_path):
        done = subprocess.run(
            [sys.executable, "-c", HANDLERS_DRIVER, str(tmp_path / "buf")],
            capture_output=True, text=True, timeout=FINISH_TIMEOUT, cwd=str(tmp_path),
            env=dict(os.environ, PYTHONPATH=str(REPO)), start_new_session=True,
        )
        assert done.returncode == 0, done.stderr
        result = json.loads(done.stdout)
        assert result["restored"] is True
        assert result["fired"] == []  # the sentinel handlers never saw the SIGINT / SIGQUIT

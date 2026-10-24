"""Milestone C, item C3: `zfsbackup.logsetup` and what it changed.

Layers, cheapest first:

- ``TestResolveLevel`` / ``TestSyslogPriority`` / ``TestJournalStreamMatches``
  pure functions; the ``dev:ino`` match and every way not to match.
- ``TestJournalFormatter`` every line of a record carries ``<N>`` (message,
  continuation lines, traceback).
- ``TestConfigure`` the root handler: plain vs journal, idempotence, a fork
  child's inherited handler is replaced, foreign handlers survive, the werkzeug
  logger is pinned.
- ``TestLogLevelFlags`` ``--log-level`` / ``-v`` through ``daemon.main()``.
- ``TestWorkersTakeALogLevel`` the numeric level reaches ``_init_child_process``
  in fork and spawn children.
- ``TestJournalFromAThread`` a WARNING from a non-main thread of an
  API-worker-shaped child (fork and spawn) arrives as ``<4>`` with the journal
  stream in force.
- ``TestApiAccessLineDemotion`` ``ApiRequestHandler.log_request`` by status class.
- ``TestImportConfiguresNothing`` importing the daemon or the workers leaves
  the root logger alone.

Nothing here reaches a real ``zfs``; the only sockets are none at all.

Mutation-checked (scratch copy of ``zfsbackup/``), tests failing:

- `JournalFormatter` prefixing only the first line of a record: 18;
- journal mode whenever `$JOURNAL_STREAM` is set (no `dev:ino` comparison): 4;
- no NUL escaping (`_no_nul` returning its input): 4;
- no hard wrap (`JOURNAL_WRAP` huge): 3;
- no clipping (`_clip_for_log` returning its input): 6.
"""

import io
import logging
import multiprocessing
import os
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from zfsbackup import logsetup, workers

ROOT = Path(__file__).resolve().parent.parent

METHODS = ["fork", "spawn"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
# (The root and werkzeug logger state `configure` changes is restored after
# every test by the autouse `_restore_logsetup_state` fixture in conftest.py.)


def journal_env(path):
    """`$JOURNAL_STREAM` as systemd would set it for a stream whose
    ``fstat`` is that of the file `path`."""
    st = os.stat(path)
    return f"{st.st_dev}:{st.st_ino}"


@pytest.fixture
def journal_file(tmp_path):
    """-> (open text stream on a real file, its `JOURNAL_STREAM` value)."""
    path = tmp_path / "journal.out"
    stream = open(path, "w+", buffering=1)
    try:
        yield stream, journal_env(path)
    finally:
        stream.close()


def record(level=logging.INFO, msg="hello", exc_info=None, name="zfsbackup.t"):
    return logging.LogRecord(name, level, __file__, 1, msg, None, exc_info)


def read_lines(stream):
    stream.flush()
    stream.seek(0)
    return stream.read().splitlines()


def traceback_info():
    try:
        raise ValueError("boom\nsecond line of the exception text")
    except ValueError:
        return sys.exc_info()


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


class TestResolveLevel:
    def test_the_default_is_info(self):
        assert logsetup.resolve_level() == logging.INFO
        assert logsetup.resolve_level(None, False) == logging.INFO

    @pytest.mark.parametrize("name, level", [
        ("debug", logging.DEBUG), ("info", logging.INFO),
        ("warning", logging.WARNING), ("error", logging.ERROR),
    ])
    def test_every_name(self, name, level):
        assert logsetup.resolve_level(name) == level
        assert logsetup.resolve_level(name.upper()) == level

    def test_verbose_alone_is_debug(self):
        assert logsetup.resolve_level(None, True) == logging.DEBUG

    @pytest.mark.parametrize("name, level", [
        ("error", logging.DEBUG), ("warning", logging.DEBUG),
        ("info", logging.DEBUG), ("debug", logging.DEBUG),
    ])
    def test_verbose_wins_over_a_coarser_level(self, name, level):
        assert logsetup.resolve_level(name, True) == level

    def test_an_unknown_name_is_refused(self):
        with pytest.raises(ValueError, match="unknown log level 'loud'"):
            logsetup.resolve_level("loud")

    def test_the_choices_match_the_table(self):
        assert logsetup.LEVEL_NAMES == ("debug", "info", "warning", "error")
        for name in logsetup.LEVEL_NAMES:
            logsetup.resolve_level(name)


class TestSyslogPriority:
    @pytest.mark.parametrize("level, priority", [
        (logging.CRITICAL, 2), (logging.ERROR, 3), (logging.WARNING, 4),
        (logging.INFO, 6), (logging.DEBUG, 7),
        # between the named levels: the level below
        (logging.ERROR + 5, 3), (logging.WARNING + 1, 4), (logging.INFO + 1, 6),
        (logging.DEBUG + 1, 7), (1, 7), (logging.CRITICAL + 10, 2),
    ])
    def test_the_sd_daemon_priority(self, level, priority):
        assert logsetup.syslog_priority(level) == priority


class TestJournalStreamMatches:
    def test_a_matching_dev_ino_is_journal(self, journal_file):
        stream, value = journal_file
        assert logsetup.journal_stream_matches(stream, {"JOURNAL_STREAM": value}) is True

    def test_a_mismatching_inode_is_not(self, journal_file):
        stream, _ = journal_file
        st = os.fstat(stream.fileno())
        other = f"{st.st_dev}:{st.st_ino + 1}"
        assert logsetup.journal_stream_matches(stream, {"JOURNAL_STREAM": other}) is False

    def test_a_mismatching_device_is_not(self, journal_file):
        stream, _ = journal_file
        st = os.fstat(stream.fileno())
        other = f"{st.st_dev + 1}:{st.st_ino}"
        assert logsetup.journal_stream_matches(stream, {"JOURNAL_STREAM": other}) is False

    def test_a_variable_that_is_unset(self, journal_file):
        stream, _ = journal_file
        assert logsetup.journal_stream_matches(stream, {}) is False

    @pytest.mark.parametrize("value", [
        "", "garbage", "1", "1:", ":1", "a:b", "1:2:3", "-1:2", "1: 2", "0x1:0x2",
    ])
    def test_a_malformed_variable(self, journal_file, value):
        stream, _ = journal_file
        assert logsetup.journal_stream_matches(stream, {"JOURNAL_STREAM": value}) is False

    def test_a_stream_with_no_descriptor(self):
        # `io.StringIO.fileno()` raises `io.UnsupportedOperation` (an OSError
        # and a ValueError), the case a captured stdout presents.
        assert logsetup.journal_stream_matches(io.StringIO(), {"JOURNAL_STREAM": "1:2"}) is False

    def test_an_object_without_a_fileno_method(self):
        assert logsetup.journal_stream_matches(object(), {"JOURNAL_STREAM": "1:2"}) is False

    def test_a_closed_stream(self, journal_file):
        stream, value = journal_file
        stream.close()
        assert logsetup.journal_stream_matches(stream, {"JOURNAL_STREAM": value}) is False

    def test_it_reads_the_real_environment_by_default(self, journal_file, monkeypatch):
        stream, value = journal_file
        monkeypatch.setenv("JOURNAL_STREAM", value)
        assert logsetup.journal_stream_matches(stream) is True
        monkeypatch.delenv("JOURNAL_STREAM")
        assert logsetup.journal_stream_matches(stream) is False


# ---------------------------------------------------------------------------
# The formatter
# ---------------------------------------------------------------------------


class TestJournalFormatter:
    fmt = logsetup.JournalFormatter()

    def test_no_timestamp_and_the_prefix_leads(self):
        line = self.fmt.format(record(logging.WARNING, "careful"))
        assert line == "<4>zfsbackup.t - WARNING - careful"

    @pytest.mark.parametrize("level, prefix", [
        (logging.CRITICAL, "<2>"), (logging.ERROR, "<3>"), (logging.WARNING, "<4>"),
        (logging.INFO, "<6>"), (logging.DEBUG, "<7>"),
    ])
    def test_the_priority_per_level(self, level, prefix):
        assert self.fmt.format(record(level)).startswith(prefix)

    def test_every_line_of_a_multi_line_message_is_prefixed(self):
        out = self.fmt.format(record(logging.INFO, "one\ntwo\n\nfour"))
        assert out.split("\n") == [
            "<6>zfsbackup.t - INFO - one", "<6>two", "<6>", "<6>four",
        ]

    def test_every_line_of_a_traceback_is_prefixed(self):
        out = self.fmt.format(record(logging.ERROR, "failed", exc_info=traceback_info()))
        lines = out.split("\n")
        assert len(lines) >= 5                       # message, Traceback, frame x2, exception x2
        assert all(line.startswith("<3>") for line in lines), lines
        assert any("Traceback (most recent call last)" in line for line in lines)
        assert any("second line of the exception text" in line for line in lines)

    def test_a_stack_info_block_is_prefixed_too(self):
        rec = record(logging.WARNING, "where")
        rec.stack_info = "Stack (most recent call last):\n  File x, line 1\n    code"
        lines = self.fmt.format(rec).split("\n")
        assert all(line.startswith("<4>") for line in lines), lines
        assert len(lines) == 4

    def test_the_prefix_is_never_doubled_and_the_message_is_not_altered(self):
        out = self.fmt.format(record(logging.INFO, "<3>already looks like one"))
        assert out == "<6>zfsbackup.t - INFO - <3>already looks like one"

    def test_the_formatted_text_ends_without_a_trailing_newline(self):
        assert not self.fmt.format(record(logging.INFO, "x")).endswith("\n")


class TestJournalHardening:
    """Text a peer controls reaches the log. journald cuts a stdout stream at
    `\\n`, at NUL and at 48 KiB and reads a `<N>` prefix at the start of EACH
    piece, so none of those may let peer text start a piece."""

    fmt = logsetup.JournalFormatter()
    header = "zfsbackup.t - INFO - "

    def test_the_wrap_is_far_below_the_journal_line_max(self):
        assert logsetup.JOURNAL_WRAP == 8192
        assert logsetup.JOURNAL_WRAP * 4 < 48 * 1024

    def test_nul_is_escaped_and_never_emitted(self):
        out = self.fmt.format(record(logging.INFO, "a\x00b\x00\x00c"))
        assert "\x00" not in out
        assert out == "<6>" + self.header + "a\\0b\\0\\0c"

    def test_nul_in_a_traceback_is_escaped(self):
        try:
            raise ValueError("bad\x00value")
        except ValueError:
            out = self.fmt.format(record(logging.ERROR, "failed", exc_info=sys.exc_info()))
        assert "\x00" not in out and "bad\\0value" in out
        assert all(line.startswith("<3>") for line in out.split("\n"))

    def test_a_long_line_is_cut_into_pieces_each_with_the_prefix(self):
        out = self.fmt.format(record(logging.WARNING, "y" * 20000))
        pieces = out.split("\n")
        assert len(pieces) == 3                       # 21 + 20000 chars at 8192 each
        assert all(piece.startswith("<4>") for piece in pieces)
        assert all(len(piece) - 3 <= logsetup.JOURNAL_WRAP for piece in pieces)
        body = "".join(piece[3:] for piece in pieces)
        assert body == "zfsbackup.t - WARNING - " + "y" * 20000

    def test_a_line_of_exactly_the_wrap_is_not_cut(self):
        n = logsetup.JOURNAL_WRAP - len("zfsbackup.t - INFO - ")
        out = self.fmt.format(record(logging.INFO, "z" * n))
        assert "\n" not in out and len(out) == 3 + logsetup.JOURNAL_WRAP
        out = self.fmt.format(record(logging.INFO, "z" * (n + 1)))
        assert out.count("\n") == 1

    def test_an_embedded_priority_marker_never_starts_a_piece(self):
        """Peer text placed so that a cut lands right before `<2>`: the piece
        still begins with the record's own prefix, so the `<2>` is mid-line."""
        n = logsetup.JOURNAL_WRAP - len(self.header)
        out = self.fmt.format(record(logging.INFO, "x" * n + "<2>evil<0>worse"))
        first, second = out.split("\n")
        assert second == "<6><2>evil<0>worse"
        assert first.startswith("<6>") and len(first) == 3 + logsetup.JOURNAL_WRAP

    def test_an_embedded_marker_after_a_newline_is_prefixed_too(self):
        out = self.fmt.format(record(logging.INFO, "ok\n<2>forged"))
        assert out.split("\n") == ["<6>" + self.header + "ok", "<6><2>forged"]

    def test_every_piece_stays_under_48_kib_even_with_four_byte_characters(self):
        out = self.fmt.format(record(logging.INFO, "\U0001F600" * 20000))
        pieces = out.split("\n")
        assert len(pieces) >= 3
        assert max(len(piece.encode("utf-8")) for piece in pieces) < 48 * 1024
        assert all(piece.startswith("<6>") for piece in pieces)
        assert "".join(piece[3:] for piece in pieces) == self.header + "\U0001F600" * 20000

    def test_empty_lines_keep_their_prefix(self):
        out = self.fmt.format(record(logging.INFO, "a\n\n\nb"))
        assert out.split("\n") == ["<6>" + self.header + "a", "<6>", "<6>", "<6>b"]

    def test_a_long_line_inside_a_multi_line_message(self):
        out = self.fmt.format(record(logging.ERROR, "short\n" + "q" * 9000 + "\nend"))
        pieces = out.split("\n")
        assert all(piece.startswith("<3>") for piece in pieces)
        assert pieces[-1] == "<3>end"
        assert all(len(piece) - 3 <= logsetup.JOURNAL_WRAP for piece in pieces)


class TestPlainFormatter:
    fmt = logsetup.PlainFormatter()

    def test_the_format_is_unchanged(self):
        line = self.fmt.format(record(logging.WARNING, "plain text"))
        assert line.endswith(" - zfsbackup.t - WARNING - plain text")
        assert line[:4].isdigit() and not line.startswith("<")

    def test_nul_is_escaped(self):
        line = self.fmt.format(record(logging.INFO, "a\x00b"))
        assert "\x00" not in line and line.endswith("a\\0b")

    def test_lines_are_not_prefixed_or_wrapped(self):
        line = self.fmt.format(record(logging.INFO, "one\ntwo\n" + "w" * 20000))
        assert not any(piece.startswith("<") for piece in line.split("\n"))
        assert line.endswith("w" * 20000)
        assert len(line.split("\n")) == 3

    def test_the_traceback_follows_the_message(self):
        line = self.fmt.format(record(logging.ERROR, "failed", exc_info=traceback_info()))
        assert "Traceback (most recent call last)" in line

    def test_configure_uses_it_on_a_mismatch_and_the_journal_formatter_on_a_match(
        self, journal_file, monkeypatch
    ):
        stream, value = journal_file
        monkeypatch.delenv("JOURNAL_STREAM", raising=False)
        logsetup.configure(logging.INFO, stream)
        (h,) = our_handlers()
        assert type(h.formatter) is logsetup.PlainFormatter

        dev, ino = value.split(":")
        monkeypatch.setenv("JOURNAL_STREAM", f"{dev}:{int(ino) + 1}")
        logsetup.configure(logging.INFO, stream)
        (h,) = our_handlers()
        assert type(h.formatter) is logsetup.PlainFormatter

        monkeypatch.setenv("JOURNAL_STREAM", value)
        logsetup.configure(logging.INFO, stream)
        (h,) = our_handlers()
        assert type(h.formatter) is logsetup.JournalFormatter

    def test_a_nul_in_a_plain_mode_record_reaches_the_stream_escaped(self, journal_file, monkeypatch):
        stream, _ = journal_file
        monkeypatch.delenv("JOURNAL_STREAM", raising=False)
        logsetup.configure(logging.INFO, stream)
        logging.getLogger("zfsbackup.t").info("peer sent a\x00b")
        stream.flush()
        stream.seek(0)
        text = stream.read()
        assert "\x00" not in text and "a\\0b" in text


# ---------------------------------------------------------------------------
# configure()
# ---------------------------------------------------------------------------


def our_handlers():
    return [h for h in logging.getLogger().handlers if isinstance(h, logsetup._ZfsbackupHandler)]


class TestConfigure:
    def test_plain_format_when_the_variable_is_unset(self, journal_file, monkeypatch):
        stream, _ = journal_file
        monkeypatch.delenv("JOURNAL_STREAM", raising=False)
        logsetup.configure(logging.INFO, stream)
        logging.getLogger("zfsbackup.t").warning("plain")
        (line,) = read_lines(stream)
        assert not line.startswith("<")
        assert line.endswith(" - zfsbackup.t - WARNING - plain")
        assert line[:4].isdigit()                    # an asctime leads

    def test_journal_format_when_the_stream_is_the_journal(self, journal_file, monkeypatch):
        stream, value = journal_file
        monkeypatch.setenv("JOURNAL_STREAM", value)
        logsetup.configure(logging.INFO, stream)
        logging.getLogger("zfsbackup.t").warning("to the journal")
        assert read_lines(stream) == ["<4>zfsbackup.t - WARNING - to the journal"]

    def test_plain_format_when_the_variable_names_another_stream(self, journal_file, monkeypatch):
        """The variable is inherited by every child of the service; a process
        whose stdout was redirected elsewhere must not print prefixes."""
        stream, value = journal_file
        dev, ino = value.split(":")
        monkeypatch.setenv("JOURNAL_STREAM", f"{dev}:{int(ino) + 1}")
        logsetup.configure(logging.INFO, stream)
        logging.getLogger("zfsbackup.t").warning("elsewhere")
        (line,) = read_lines(stream)
        assert not line.startswith("<")
        assert "elsewhere" in line

    def test_the_default_stream_is_stdout(self, monkeypatch):
        monkeypatch.delenv("JOURNAL_STREAM", raising=False)
        logsetup.configure(logging.INFO)
        (h,) = our_handlers()
        assert h.stream is sys.stdout

    def test_the_level_is_applied(self, journal_file):
        stream, _ = journal_file
        logsetup.configure(logging.WARNING, stream)
        assert logging.getLogger().level == logging.WARNING
        log = logging.getLogger("zfsbackup.t")
        log.info("dropped")
        log.warning("kept")
        (line,) = read_lines(stream)
        assert line.endswith("kept")

    def test_a_second_call_replaces_the_handler_rather_than_stacking(self, journal_file):
        stream, _ = journal_file
        logsetup.configure(logging.INFO, stream)
        logsetup.configure(logging.DEBUG, stream)
        logsetup.configure(logging.INFO, stream)
        assert len(our_handlers()) == 1
        logging.getLogger("zfsbackup.t").info("once")
        assert len(read_lines(stream)) == 1

    def test_foreign_handlers_are_left_alone(self, journal_file):
        stream, _ = journal_file
        foreign = logging.NullHandler()
        logging.getLogger().addHandler(foreign)
        try:
            logsetup.configure(logging.INFO, stream)
            logsetup.configure(logging.INFO, stream)
            assert foreign in logging.getLogger().handlers
            assert len(our_handlers()) == 1
        finally:
            logging.getLogger().removeHandler(foreign)

    def test_a_handler_inherited_across_a_fork_is_replaced(self, journal_file, tmp_path):
        """A fork child inherits the supervisor's handler (bound to the
        supervisor's formatter and stream). The child's `configure` replaces
        it, so there is exactly one and it is the child's."""
        stream, _ = journal_file
        logsetup.configure(logging.INFO, io.StringIO())     # "the supervisor's"
        inherited = our_handlers()[0]
        logsetup.configure(logging.DEBUG, stream)           # "the child's"
        (mine,) = our_handlers()
        assert mine is not inherited
        assert inherited not in logging.getLogger().handlers

    def test_werkzeug_is_pinned_to_the_configured_level(self, journal_file):
        stream, _ = journal_file
        wz = logging.getLogger("werkzeug")
        wz.setLevel(logging.NOTSET)
        for level in (logging.WARNING, logging.DEBUG, logging.ERROR):
            logsetup.configure(level, stream)
            assert wz.level == level

    def test_werkzeug_info_lines_are_dropped_under_warning(self, journal_file):
        stream, _ = journal_file
        logsetup.configure(logging.WARNING, stream)
        logging.getLogger("werkzeug").info("GET / 200")
        logging.getLogger("werkzeug").warning("boom")
        assert [line.split(" - ")[-1] for line in read_lines(stream)] == ["boom"]

    def test_it_configures_nothing_but_the_root_and_werkzeug(self, journal_file):
        stream, _ = journal_file
        other = logging.getLogger("zfsbackup.some.module")
        before = (other.level, other.handlers[:], other.propagate)
        logsetup.configure(logging.ERROR, stream)
        assert (other.level, other.handlers[:], other.propagate) == before


# ---------------------------------------------------------------------------
# Importing configures nothing
# ---------------------------------------------------------------------------


class TestImportConfiguresNothing:
    @pytest.mark.parametrize("module", ["zfsbackup.daemon", "zfsbackup.workers", "zfsbackup.logsetup"])
    def test_the_root_logger_is_untouched_by_the_import(self, module):
        """A fresh interpreter, so this holds whatever the suite already
        imported: no handler, level WARNING (the stdlib default), werkzeug unset."""
        code = (
            "import logging, sys\n"
            "root = logging.getLogger()\n"
            "before = (list(root.handlers), root.level)\n"
            f"import {module}\n"
            "assert root.handlers == before[0], root.handlers\n"
            "assert root.level == before[1] == logging.WARNING, root.level\n"
            "assert logging.getLogger('werkzeug').level == logging.NOTSET\n"
            "print('clean')\n"
        )
        from _hermetic import hermetic_env

        env = hermetic_env(PYTHONPATH=str(ROOT))
        env.pop("JOURNAL_STREAM", None)
        result = subprocess.run(
            [sys.executable, "-c", code], cwd=ROOT, env=env,
            capture_output=True, text=True, timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stdout.strip() == "clean"

    def test_the_daemon_module_no_longer_calls_basic_config(self):
        source = (ROOT / "zfsbackup" / "daemon.py").read_text()
        assert "basicConfig" not in source


# ---------------------------------------------------------------------------
# --log-level / -v through the daemon's main()
# ---------------------------------------------------------------------------


class TestLogLevelFlags:
    @pytest.fixture
    def run_main(self, mocker, monkeypatch):
        """-> `go(*argv)` -> (the level `configure` got, the Namespace `_run` got).
        `_run` is stubbed: nothing is read, nothing is started."""
        import zfsbackup.daemon as daemon

        configured = []
        mocker.patch.object(daemon.logsetup, "configure", side_effect=configured.append)
        seen = []

        def fake_run(args, abandoned=None):
            seen.append(args)
            return 0

        mocker.patch.object(daemon, "_run", side_effect=fake_run)
        mocker.patch.object(daemon, "dispose_all")

        def go(*argv):
            monkeypatch.setattr(sys, "argv", ["zfsbackupd", *argv])
            daemon.main()
            return configured[-1], seen[-1]

        return go

    def test_default_is_info(self, run_main):
        level, args = run_main()
        assert level == logging.INFO
        assert args.log_level is None and args.verbose is False

    @pytest.mark.parametrize("name, level", [
        ("debug", logging.DEBUG), ("info", logging.INFO),
        ("warning", logging.WARNING), ("error", logging.ERROR),
    ])
    def test_log_level(self, run_main, name, level):
        assert run_main("--log-level", name)[0] == level

    def test_verbose_is_debug(self, run_main):
        assert run_main("-v")[0] == logging.DEBUG
        assert run_main("--verbose")[0] == logging.DEBUG

    def test_the_finer_of_the_two_wins(self, run_main):
        assert run_main("-v", "--log-level", "error")[0] == logging.DEBUG
        assert run_main("--log-level", "debug", "-v")[0] == logging.DEBUG

    def test_an_unknown_level_is_a_usage_error(self, run_main, capsys):
        with pytest.raises(SystemExit) as exc:
            run_main("--log-level", "loud")
        assert exc.value.code == 2
        assert "invalid choice" in capsys.readouterr().err

    @pytest.mark.parametrize("argv, quiet", [
        ([], True), (["--log-level", "warning"], True), (["--log-level", "info"], True),
        (["--log-level", "debug"], False), (["-v"], False), (["-v", "--log-level", "error"], False),
    ])
    def test_alembic_is_quiet_unless_the_resolved_level_is_debug(self, mocker, argv, quiet):
        import zfsbackup.daemon as daemon

        mocker.patch.object(daemon, "_run_startup", return_value=0)
        alembic = logging.getLogger("alembic")
        saved = alembic.level
        alembic.setLevel(logging.NOTSET)
        try:
            args = SimpleNamespace(
                test_config=True, verbose="-v" in argv,
                log_level=argv[argv.index("--log-level") + 1] if "--log-level" in argv else None,
            )
            daemon._run(args)
            assert alembic.level == (logging.WARNING if quiet else logging.NOTSET)
        finally:
            alembic.setLevel(saved)

    def test_the_daemon_helper_agrees_with_resolve_level(self):
        import zfsbackup.daemon as daemon

        ns = SimpleNamespace
        assert daemon._log_level(ns()) == logging.INFO               # an old-style Namespace
        assert daemon._log_level(ns(verbose=True)) == logging.DEBUG
        assert daemon._log_level(ns(log_level="warning", verbose=False)) == logging.WARNING
        assert daemon._log_level(ns(log_level="warning", verbose=True)) == logging.DEBUG


# ---------------------------------------------------------------------------
# Workers: the numeric level, configured in the child
# ---------------------------------------------------------------------------


def _child_configure_and_report(out_path, level):
    """Runs in a child: exactly what a worker's `run()` does first."""
    import json

    workers._init_child_process(level)
    root = logging.getLogger()
    with open(out_path, "w") as fh:
        json.dump({
            "level": root.level,
            "ours": sum(isinstance(h, logsetup._ZfsbackupHandler) for h in root.handlers),
            "werkzeug": logging.getLogger("werkzeug").level,
        }, fh)


def _run_child(method, target, args, timeout=60):
    ctx = multiprocessing.get_context(method)
    proc = ctx.Process(target=target, args=args)
    proc.start()
    try:
        proc.join(timeout)
        assert not proc.is_alive(), "the child did not finish"
        assert proc.exitcode == 0, f"child exit {proc.exitcode}"
    finally:
        if proc.is_alive():
            proc.kill()
            proc.join()


class TestWorkersTakeALogLevel:
    @pytest.mark.parametrize("method", METHODS)
    @pytest.mark.parametrize("level", [logging.DEBUG, logging.WARNING, logging.ERROR])
    def test_the_child_configures_the_level_it_was_given(self, tmp_path, method, level):
        import json

        if method not in multiprocessing.get_all_start_methods():
            pytest.skip(f"no {method} start method here")
        # Under fork the child inherits the parent's handlers; give it a
        # ZfsbackupHandler of the parent's to prove it is replaced, not stacked.
        logsetup.configure(logging.INFO, io.StringIO())
        out = tmp_path / "report.json"
        _run_child(method, _child_configure_and_report, (str(out), level))
        report = json.loads(out.read_text())
        assert report == {"level": level, "ours": 1, "werkzeug": level}

    def test_init_child_process_configures_after_the_signal_resets(self, mocker):
        order = []
        mocker.patch.object(workers.signal, "signal", side_effect=lambda *a: order.append("signal"))
        mocker.patch.object(workers.logsetup, "configure", side_effect=lambda lvl: order.append(("log", lvl)))
        workers._init_child_process(logging.WARNING)
        assert order == ["signal", "signal", "signal", ("log", logging.WARNING)]

    def test_setup_logging_is_logsetup_configure(self, mocker):
        configure = mocker.patch.object(workers.logsetup, "configure")
        workers._setup_logging(logging.ERROR)
        configure.assert_called_once_with(logging.ERROR)

    def test_the_default_is_info(self, mocker):
        configure = mocker.patch.object(workers.logsetup, "configure")
        workers._setup_logging()
        configure.assert_called_once_with(logging.INFO)


# ---------------------------------------------------------------------------
# A non-main-thread WARNING in an API-worker-shaped child
# ---------------------------------------------------------------------------


def _api_shaped_child(log_path):
    """The shape of an `ApiWorker`: stdout is the journal stream (fd 1), the
    worker initialises logging first, and the request handlers log from
    threads other than the main one."""
    fd = os.open(log_path, os.O_WRONLY | os.O_APPEND)
    os.dup2(fd, 1)
    os.close(fd)
    sys.stdout = os.fdopen(1, "w", buffering=1, closefd=False)

    workers._init_child_process(logging.INFO)

    def handler_thread():
        log = logging.getLogger("zfsbackup.api")
        log.warning("API: a websocket reader thread did not leave\nits connection")
        logging.getLogger(logsetup.PEER_LOGGER).warning("peer 10.0.0.9 misbehaves")
        try:
            raise RuntimeError("handler failed")
        except RuntimeError:
            log.exception("API: request failed")

    t = threading.Thread(target=handler_thread, name="api-request-thread")
    t.start()
    t.join()
    logging.getLogger("zfsbackup.workers").info("main thread info")
    sys.stdout.flush()


@pytest.mark.integration
class TestJournalFromAThread:
    @pytest.mark.parametrize("method", METHODS)
    def test_a_warning_from_a_handler_thread_arrives_as_priority_4(
        self, tmp_path, monkeypatch, method
    ):
        if method not in multiprocessing.get_all_start_methods():
            pytest.skip(f"no {method} start method here")
        log_path = tmp_path / "journal.out"
        log_path.write_text("")
        monkeypatch.setenv("JOURNAL_STREAM", journal_env(log_path))
        _run_child(method, _api_shaped_child, (str(log_path),))
        lines = log_path.read_text().splitlines()

        assert lines, "the child wrote nothing"
        # EVERY line carries a priority prefix: a bare line would be stored as info.
        assert all(line.startswith("<") and line[2] == ">" for line in lines), lines
        # No timestamp: the journal has its own.
        assert not any(" - " in line.split(">", 1)[0] for line in lines)
        by_text = {line.split(">", 1)[1]: line[:3] for line in lines}
        assert by_text["zfsbackup.api - WARNING - API: a websocket reader thread did not leave"] == "<4>"
        assert by_text["its connection"] == "<4>"                     # the continuation line
        assert by_text[f"{logsetup.PEER_LOGGER} - WARNING - peer 10.0.0.9 misbehaves"] == "<4>"
        assert by_text["zfsbackup.api - ERROR - API: request failed"] == "<3>"
        assert by_text["zfsbackup.workers - INFO - main thread info"] == "<6>"
        tb = [line for line in lines if "RuntimeError" in line or "Traceback" in line]
        assert tb and all(line.startswith("<3>") for line in tb)

    @pytest.mark.parametrize("method", METHODS)
    def test_without_the_journal_stream_the_same_child_prints_no_prefixes(
        self, tmp_path, monkeypatch, method
    ):
        if method not in multiprocessing.get_all_start_methods():
            pytest.skip(f"no {method} start method here")
        log_path = tmp_path / "plain.out"
        log_path.write_text("")
        monkeypatch.delenv("JOURNAL_STREAM", raising=False)
        _run_child(method, _api_shaped_child, (str(log_path),))
        lines = log_path.read_text().splitlines()
        assert lines
        assert not any(line.startswith("<") for line in lines), lines

    @pytest.mark.parametrize("method", METHODS)
    def test_the_variable_naming_another_file_prints_no_prefixes(
        self, tmp_path, monkeypatch, method
    ):
        """Inherited by every child, but only the stream it names is the journal."""
        if method not in multiprocessing.get_all_start_methods():
            pytest.skip(f"no {method} start method here")
        elsewhere = tmp_path / "elsewhere"
        elsewhere.write_text("")
        log_path = tmp_path / "plain.out"
        log_path.write_text("")
        monkeypatch.setenv("JOURNAL_STREAM", journal_env(elsewhere))
        _run_child(method, _api_shaped_child, (str(log_path),))
        lines = log_path.read_text().splitlines()
        assert lines and not any(line.startswith("<") for line in lines), lines


# ---------------------------------------------------------------------------
# log_request demotion
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def handler_class(tmp_path_factory):
    from _tls_support import LOOPBACK, make_gateway
    from flask import Flask

    gateway, _server, _trust = make_gateway(tmp_path_factory.mktemp("logsetup_pki") / "srv.tls")
    server = workers._api_server_class()(LOOPBACK, 0, Flask("logsetup"), gateway)
    try:
        return server.RequestHandlerClass
    finally:
        server.server_close()


def make_request(handler_class, *, command="GET", path="/health", version="HTTP/1.1"):
    h = handler_class.__new__(handler_class)
    h.command, h.path, h.request_version = command, path, version
    h.requestline = f"{command} {path} {version}"
    h.client_address = ("127.0.0.1", 4242)
    return h


class TestApiAccessLineDemotion:
    @pytest.fixture(autouse=True)
    def _werkzeug_debug(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="werkzeug"):
            yield

    def logged(self, caplog):
        return [r for r in caplog.records if r.name == "werkzeug"]

    @pytest.mark.parametrize("code, level", [
        (100, logging.DEBUG), (101, logging.DEBUG), (200, logging.DEBUG), (204, logging.DEBUG),
        (301, logging.DEBUG), (304, logging.DEBUG), (399, logging.DEBUG),
        (400, logging.INFO), (401, logging.INFO), (403, logging.INFO), (404, logging.INFO),
        (409, logging.INFO), (499, logging.INFO),
        (500, logging.WARNING), (503, logging.WARNING), (599, logging.WARNING),
    ])
    def test_the_level_by_status_class(self, handler_class, caplog, code, level):
        make_request(handler_class).log_request(code, 12)
        (rec,) = self.logged(caplog)
        assert rec.levelno == level
        assert rec.name == "werkzeug"
        assert f'"GET /health HTTP/1.1" {code} 12' in rec.getMessage()

    @pytest.mark.parametrize("code", ["200", "404", "503"])
    def test_a_string_status_is_accepted(self, handler_class, caplog, code):
        make_request(handler_class).log_request(code, "-")
        (rec,) = self.logged(caplog)
        assert rec.levelno == (logging.DEBUG if code == "200" else logging.INFO if code == "404" else logging.WARNING)

    @pytest.mark.parametrize("code", ["-", None, "abc"])
    def test_a_status_that_is_not_a_number_is_info(self, handler_class, caplog, code):
        make_request(handler_class).log_request(code, "-")
        (rec,) = self.logged(caplog)
        assert rec.levelno == logging.INFO

    def test_a_200_is_hidden_at_info_and_a_500_is_not(self, handler_class, caplog):
        caplog.set_level(logging.INFO, logger="werkzeug")
        h = make_request(handler_class)
        h.log_request(200, 1)
        h.log_request(404, 1)
        h.log_request(500, 1)
        assert [r.levelno for r in self.logged(caplog)] == [logging.INFO, logging.WARNING]

    def test_nothing_below_warning_survives_log_level_warning(self, handler_class, caplog):
        caplog.set_level(logging.WARNING, logger="werkzeug")
        h = make_request(handler_class)
        for code in (200, 302, 404):
            h.log_request(code, 1)
        h.log_request(502, 1)
        assert [r.levelno for r in self.logged(caplog)] == [logging.WARNING]

    def test_control_characters_in_the_path_are_escaped(self, handler_class, caplog):
        h = make_request(handler_class, path="/x\x1b[31mred\x00\x7f\x85y")
        h.log_request(404, 0)
        (rec,) = self.logged(caplog)
        text = rec.getMessage()
        for raw in ("\x1b", "\x00", "\x7f", "\x85"):
            assert raw not in text
        assert "\\x1b" in text and "\\x00" in text and "\\x7f" in text and "\\x85" in text

    def test_there_is_no_ansi_colour(self, handler_class, caplog):
        make_request(handler_class).log_request(500, 0)
        (rec,) = self.logged(caplog)
        assert "\x1b" not in rec.getMessage()

    def test_a_missing_path_falls_back_to_the_request_line(self, handler_class, caplog):
        h = handler_class.__new__(handler_class)       # `path` is unset after a bad request line
        h.requestline = "GARBAGE \x01 LINE"
        h.client_address = ("127.0.0.1", 4242)
        h.log_request(400, "-")
        (rec,) = self.logged(caplog)
        assert "GARBAGE" in rec.getMessage()
        assert "\x01" not in rec.getMessage()
        assert rec.levelno == logging.INFO

    def test_the_peer_address_is_in_the_line(self, handler_class, caplog):
        make_request(handler_class).log_request(200, 1)
        (rec,) = self.logged(caplog)
        assert rec.getMessage().startswith("127.0.0.1 - - [")

    # -- clipping the client-controlled text --------------------------------

    def quoted(self, rec):
        """The text between the quotes of the access line."""
        text = rec.getMessage()
        return text[text.index('"') + 1:text.rindex('" ')]

    def test_the_limit_is_1024(self):
        assert workers.MAX_LOGGED_REQUEST == 1024

    def test_clip_for_log_direct(self):
        assert workers._clip_for_log("a" * 1024) == "a" * 1024
        assert workers._clip_for_log("a" * 1025) == "a" * 1024 + "...(1 more)"
        assert workers._clip_for_log("") == ""

    def test_a_5000_character_path_is_clipped_with_a_count(self, handler_class, caplog):
        h = make_request(handler_class, path="/" + "p" * 4999)
        composed = f"GET {h.path} HTTP/1.1"
        h.log_request(404, 0)
        (rec,) = self.logged(caplog)
        quoted = self.quoted(rec)
        assert quoted == composed[:1024] + f"...({len(composed) - 1024} more)"
        assert len(quoted) < 1100
        assert "p" * 2000 not in rec.getMessage()

    def test_a_message_of_exactly_1024_characters_is_unchanged(self, handler_class, caplog):
        path = "/" + "e" * (1024 - len("GET  HTTP/1.1") - 1)
        h = make_request(handler_class, path=path)
        assert len(f"GET {path} HTTP/1.1") == 1024
        h.log_request(404, 0)
        (rec,) = self.logged(caplog)
        assert self.quoted(rec) == f"GET {path} HTTP/1.1"
        assert "more)" not in rec.getMessage()

    def test_one_character_over_is_clipped(self, handler_class, caplog):
        path = "/" + "e" * (1024 - len("GET  HTTP/1.1"))
        h = make_request(handler_class, path=path)
        assert len(f"GET {path} HTTP/1.1") == 1025
        h.log_request(404, 0)
        (rec,) = self.logged(caplog)
        assert self.quoted(rec).endswith("...(1 more)")

    def test_a_bad_request_line_is_clipped_too(self, handler_class, caplog):
        h = handler_class.__new__(handler_class)
        h.requestline = "G" * 5000
        h.client_address = ("127.0.0.1", 4242)
        h.log_request(400, "-")
        (rec,) = self.logged(caplog)
        assert self.quoted(rec) == "G" * 1024 + "...(3976 more)"

    def test_a_priority_marker_past_the_limit_is_never_logged(self, handler_class, caplog):
        path = "/" + "a" * 1100 + "\n<2>forged critical"
        make_request(handler_class, path=path).log_request(404, 0)
        (rec,) = self.logged(caplog)
        assert "<2>" not in rec.getMessage()
        assert "forged" not in rec.getMessage()

    def test_the_clip_comes_before_the_escaping(self, handler_class, caplog):
        """1024 characters of text are kept, then escaped: the marker's count is of
        the original characters, and no escape sequence is cut in half."""
        h = make_request(handler_class, path="/" + "\x01" * 3000)
        h.log_request(404, 0)
        (rec,) = self.logged(caplog)
        quoted = self.quoted(rec)
        assert "\x01" not in quoted
        assert quoted.endswith("...(%d more)" % (len("GET /" + "\x01" * 3000 + " HTTP/1.1") - 1024))
        assert quoted.count("\\x01") == 1024 - len("GET /")

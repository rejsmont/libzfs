"""One `click.testing.CliRunner` for every click the project supports.

The `.deb` depends on the distribution's `python3-click`, which is 8.1.8 on both
release targets (Debian 13 and Ubuntu 26.04), while `poetry.lock` resolves a
click >= 8.2. The two `CliRunner`s differ in three ways that matter here, and
`make_runner()` hides all of them:

* **Separate stderr.** click 8.2 always captures stdout and stderr apart (and
  `result.output` is both, interleaved as a terminal would show them); 8.1 mixes
  the two unless `CliRunner(mix_stderr=False)`, a keyword 8.2 removed. The factory
  passes it exactly where it exists, so `result.stdout` / `result.stderr` mean the
  same thing on both.
* **Ordering across the two streams.** On 8.1 with separated streams `result.output`
  is stdout only. A test that needs "this warning came before that confirmation"
  uses `interleaved(result)`: `result.output` on >= 8.2 (unchanged), and on 8.1 a
  log the runner keeps of every text write to either stream, in order.
* **End of input at a prompt.** On >= 8.2 a prompt that reads EOF raises
  `EOFError`, which `click.prompt` turns into `Abort` (a real terminal's ^D does
  the same on every click). 8.1's `CliRunner` returns an empty string instead, so
  the default is taken and the "EOF aborts" paths cannot be tested; the 8.1 runner
  here raises `EOFError` too. (Checked against the real CLI on a pty under click
  8.1.8: ^D at the retry prompt of `edit` aborts exactly as on 8.3.)

Nothing here is specific to zfsbackup; `tests/_cli_support.py` and the test files
that build their own runner import it.
"""

import contextlib
import inspect
import sys

from click.testing import CliRunner

#: True on click < 8.2, whose `CliRunner` mixes stderr into stdout by default and
#: takes `mix_stderr`.
LEGACY_RUNNER = "mix_stderr" in inspect.signature(CliRunner.__init__).parameters


class _Tee:
    """A text stream that also appends everything written to it to `log`."""

    def __init__(self, stream, log: list) -> None:
        self._stream = stream
        self._log = log

    def write(self, text):
        if isinstance(text, str):
            self._log.append(text)
        return self._stream.write(text)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def __iter__(self):
        return iter(self._stream)


class _LegacyRunner(CliRunner):
    """click 8.1's runner, made to behave like 8.2's where the tests rely on it."""

    @contextlib.contextmanager
    def isolation(self, input=None, env=None, color=False):
        from click import termui

        with super().isolation(input=input, env=env, color=color) as streams:
            log: list = []
            self._interleaved_log = log
            raw_out, raw_err = sys.stdout, sys.stderr
            sys.stdout = tee_out = _Tee(raw_out, log)
            sys.stderr = tee_out if raw_err is raw_out else _Tee(raw_err, log)

            # 8.2: EOF at a prompt raises EOFError (-> click.Abort). 8.1 answers "".
            def visible_input(prompt=None):
                sys.stdout.write(prompt or "")
                line = sys.stdin.readline()
                if not line:
                    raise EOFError
                value = line.rstrip("\r\n")
                sys.stdout.write(f"{value}\n")
                sys.stdout.flush()
                return value

            def hidden_input(prompt=None):
                sys.stdout.write(f"{prompt or ''}\n")
                sys.stdout.flush()
                line = sys.stdin.readline()
                if not line:
                    raise EOFError
                return line.rstrip("\r\n")

            termui.visible_prompt_func = visible_input
            termui.hidden_prompt_func = hidden_input
            yield streams

    def invoke(self, *args, **kwargs):
        self._interleaved_log = []
        result = super().invoke(*args, **kwargs)
        result.interleaved = "".join(self._interleaved_log)
        return result


def make_runner(**kwargs) -> CliRunner:
    """A `CliRunner` whose result has separate `stdout` and `stderr` on every
    supported click (see the module docstring)."""
    if LEGACY_RUNNER:
        kwargs.setdefault("mix_stderr", False)
        return _LegacyRunner(**kwargs)
    return CliRunner(**kwargs)


def interleaved(result) -> str:
    """Everything the command wrote to stdout and stderr, in the order it wrote it
    (what a terminal would show). For asserting the order of two lines that go to
    different streams."""
    if LEGACY_RUNNER:
        return result.interleaved
    return result.output

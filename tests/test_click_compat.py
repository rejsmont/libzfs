"""`tests/_click_compat.py`: the one `CliRunner` factory the CLI tests share. These run on whichever
click is installed -- 8.1.x (Debian 13, Ubuntu 26.04) or >= 8.2 (poetry.lock) -- and must hold on both."""

from importlib.metadata import version

import click
import pytest
from packaging.version import Version

from _click_compat import LEGACY_RUNNER, interleaved, make_runner

pytestmark = pytest.mark.unit


@click.command()
def chatty():
    click.echo("one-out")
    click.echo("two-err", err=True)
    click.echo("three-out")
    click.echo("four-err", err=True)


@click.command()
def asks():
    answer = click.prompt("name?", default="dflt", err=True)
    click.echo(f"got {answer}")


@click.command()
def asks_hidden():
    answer = click.prompt("secret?", hide_input=True, err=True)
    click.echo(f"got {answer}")


def test_the_legacy_runner_is_used_exactly_below_click_8_2():
    assert LEGACY_RUNNER == (Version(version("click")) < Version("8.2"))


def test_stdout_and_stderr_are_captured_separately():
    result = make_runner().invoke(chatty, [])
    assert result.stdout == "one-out\nthree-out\n"
    assert result.stderr == "two-err\nfour-err\n"


def test_interleaved_output_keeps_the_order_across_the_two_streams():
    result = make_runner().invoke(chatty, [])
    assert interleaved(result) == "one-out\ntwo-err\nthree-out\nfour-err\n"
    # ...without costing the separation
    assert "two-err" not in result.stdout and "one-out" not in result.stderr


def test_a_command_that_writes_nothing_has_empty_streams():
    @click.command()
    def quiet():
        pass

    result = make_runner().invoke(quiet, [])
    assert (result.stdout, result.stderr, interleaved(result)) == ("", "", "")


def test_a_prompt_takes_the_typed_answer():
    result = make_runner().invoke(asks, [], input="alice\n")
    assert result.exit_code == 0
    assert result.stdout.endswith("got alice\n")
    assert "name?" in result.stderr


def test_end_of_input_at_a_prompt_aborts_instead_of_taking_the_default():
    """A terminal's ^D at a prompt aborts on every click; 8.1's own runner would answer with the default."""
    result = make_runner().invoke(asks, [], input="")
    assert result.exit_code == 1
    assert "got" not in result.stdout
    assert "Aborted!" in result.stderr


def test_an_empty_line_at_a_prompt_still_takes_the_default():
    result = make_runner().invoke(asks, [], input="\n")
    assert result.exit_code == 0
    assert result.stdout.endswith("got dflt\n")


def test_end_of_input_at_a_hidden_prompt_aborts():
    result = make_runner().invoke(asks_hidden, [], input="")
    assert result.exit_code == 1
    assert "Aborted!" in result.stderr


def test_each_invocation_has_its_own_interleaved_log():
    runner = make_runner()
    first = runner.invoke(chatty, [])
    second = runner.invoke(chatty, [])
    assert interleaved(first) == interleaved(second) == "one-out\ntwo-err\nthree-out\nfour-err\n"

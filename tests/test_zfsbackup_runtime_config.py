"""Concurrency regression coverage for
`zfsbackup.runtime_config.load_runtime_config`'s torn-read protection.

Follow-up to item 8 Phase C, coordinator-reported defect 1: `load_config`
issues five-plus independent statements, and reader engines deliberately
begin no transaction of their own (`db.py`'s reader rule), so a
`zfsbackup-config import` (`import_yaml`) committing mid-read used to be
seen not as an error but as a *different, spliced* config -- half from
before the commit, half from after. Measured before the fix: two
datasets' whole retention policies swapped, silently, at a 13.99% rate
over a reader/writer soak with a ~1.6ms race window -- narrow, but a
worker hits it on every start and every crash restart, forever.

`load_runtime_config` now wraps `load_config` in one explicit
`BEGIN`/`COMMIT` pair, scoped to the single call (never to the session's
or connection's lifetime -- see that function's own docstring for why a
longer-lived snapshot would violate `db.py`'s separate "never adopt
BEGIN-on-connect for reader engines" rule instead).

**"No exception was raised" proves nothing here** -- every one of the
1052 torn reads in the pre-fix soak was exception-free. The only sound
test is a real concurrency soak with a per-dataset invariant check, which
is what this file is. It is deliberately marked `integration`/`slow` and
kept short (a bounded real-time window, not an unbounded run) -- the
measured rate is high enough that a sub-two-second window is already
"a few hundred" reads, comfortably enough to make the control positive
and the fix negative without turning this into a multi-second addition
to the default `pytest` run.
"""

import multiprocessing as mp
import time
from pathlib import Path

import pytest

from zfsbackup.config.store import (
    dispose_all,
    import_yaml,
    load_config,
    open_config_session,
    resolve_config_path,
)
from zfsbackup.runtime_config import load_runtime_config

pytestmark = [pytest.mark.integration, pytest.mark.slow]

# Two datasets, swapped order between the two YAMLs -- the writer
# alternates between them so every commit either preserves or reverses
# which dataset comes "first" in the underlying tables. A torn read
# across such a commit is what hands a reader `tank/archive`'s name with
# `tank/scratch`'s retention rule, or vice versa.
_YAML_A = """
snapshot_prefix: autosnap
datasets:
  - name: tank/archive
    frequency: 1d
    retention:
      1d: 10y
  - name: tank/scratch
    frequency: 1h
    retention:
      1h: 2h
"""
_YAML_B = """
snapshot_prefix: autosnap
datasets:
  - name: tank/scratch
    frequency: 1h
    retention:
      1h: 2h
  - name: tank/archive
    frequency: 1d
    retention:
      1d: 10y
"""
_EXPECTED = {
    "tank/archive": [("1 day, 0:00:00", "3650 days, 0:00:00")],
    "tank/scratch": [("1:00:00", "2:00:00")],
}

# Real-time window each writer/reader pair runs for. Short on purpose --
# see module docstring.
_SOAK_SECONDS = 1.5
_JOIN_TIMEOUT = 20


def _seen(config) -> dict:
    return {
        ds.name: [(str(r.age), str(r.keep_for)) for r in ds.retention_rules]
        for ds in config.datasets
    }


def _writer_loop(db_path: str, stop, commit_count) -> None:
    """Runs in its own process. Alternates `import_yaml` between the two
    YAMLs until `stop` is set -- every iteration is a full, independent
    writer transaction (schema phase + data phase), exactly the shape a
    real `zfsbackup-config import` produces.

    `commit_count` (a `multiprocessing.Value('l', 0)`) is incremented
    after every SUCCESSFUL `import_yaml` call -- read back by `_run_soak`
    so a caller can tell "this writer imported N times" from "this writer
    died on its first iteration and the reader's ok/torn tally reflects a
    database nothing ever wrote to during the soak". Deliberately NOT a
    bare local counter reported only via `print`/the process's own return
    value: `Process.run()`'s return value is discarded by
    `multiprocessing`, and a counter this test cannot read back is not
    meaningfully different from not counting at all.
    """
    import logging
    logging.disable(logging.CRITICAL)
    a, b = Path(db_path).with_name("a.yaml"), Path(db_path).with_name("b.yaml")
    resolved = resolve_config_path(db_path)
    i = 0
    while not stop.is_set():
        import_yaml(a if i % 2 == 0 else b, resolved)
        i += 1
        with commit_count.get_lock():
            commit_count.value = i
    dispose_all()


def _load_runtime_config_no_snapshot(resolved):
    """Reimplements the PRE-FIX shape of `load_runtime_config`,
    independently of the real (fixed) function -- a bare read-only
    session with no surrounding `BEGIN`/`COMMIT`, so each of
    `load_config`'s five-plus statements is its own read transaction.

    This exists ONLY as the harness's own negative control (see
    `TestLoadRuntimeConfigTornReadControl` below) -- it must never be
    confused with, imported by, or substituted for the real
    `load_runtime_config`, and nothing outside this test file may use it.
    """
    with open_config_session(resolved, readonly=True) as session:
        return load_config(session)


def _reader_loop(db_path: str, stop, out, use_snapshot: bool) -> None:
    """Runs in its own process. Loops the given reader shape until `stop`
    is set (set either by the caller's timer or by this function itself
    once its own window has run out), tallying `ok`/`torn`/`exc` against
    the `_EXPECTED` per-dataset invariant.
    """
    import logging
    logging.disable(logging.CRITICAL)
    resolved = resolve_config_path(db_path)
    ok = torn = exc = 0
    samples = []
    while not stop.is_set():
        try:
            if use_snapshot:
                config = load_runtime_config(resolved, verify_schema=False)
            else:
                config = _load_runtime_config_no_snapshot(resolved)
        except Exception as e:  # pragma: no cover - diagnostic only
            exc += 1
            if len(samples) < 3:
                samples.append(f"EXC {type(e).__name__}: {e}")
            continue
        seen = _seen(config)
        if seen == _EXPECTED:
            ok += 1
        else:
            torn += 1
            if len(samples) < 3:
                samples.append(f"TORN {seen}")
    dispose_all()
    out.put((ok, torn, exc, samples))


def _run_soak(tmp_path: Path, *, use_snapshot: bool, seconds: float = _SOAK_SECONDS):
    """Seeds a fresh config DB, runs one writer + one reader process
    concurrently for `seconds` of real wall-clock time, and returns
    `(ok, torn, exc, samples, writer_exitcode, commits)`.

    `writer_exitcode`/`commits` exist so a caller can tell "the writer
    ran healthily and actually wrote N times" from "the writer died on
    its first iteration and the reader's ok/torn tally reflects a
    database nothing was concurrently written to" -- without them,
    `ok > 20, torn == 0` is equally consistent with a working
    torn-read fix AND with a dead writer plus a reader reading one static
    (never mutated) config over and over, which is not a soak at all.
    """
    (tmp_path / "a.yaml").write_text(_YAML_A)
    (tmp_path / "b.yaml").write_text(_YAML_B)
    db_path = str(tmp_path / "config.db")
    resolved = resolve_config_path(db_path)
    import_yaml(tmp_path / "a.yaml", resolved)
    dispose_all()

    ctx = mp.get_context("spawn")
    stop = ctx.Event()
    out = ctx.Queue()
    commit_count = ctx.Value("l", 0)
    writer = ctx.Process(target=_writer_loop, args=(db_path, stop, commit_count))
    reader = ctx.Process(target=_reader_loop, args=(db_path, stop, out, use_snapshot))
    writer.start()
    reader.start()
    time.sleep(seconds)
    stop.set()
    try:
        ok, torn, exc, samples = out.get(timeout=_JOIN_TIMEOUT)
    finally:
        writer.join(_JOIN_TIMEOUT)
        reader.join(_JOIN_TIMEOUT)
        for proc in (writer, reader):
            if proc.is_alive():  # pragma: no cover - safety net, not the assertion
                proc.terminate()
                proc.join(5)
    return ok, torn, exc, samples, writer.exitcode, commit_count.value


class TestLoadRuntimeConfigTornReadFix:
    """Mutation-checked: replacing `load_runtime_config`'s
    `BEGIN`/`load_config`/`COMMIT` sequence with a bare
    `return load_config(session)` (i.e. reverting the fix) fails 1 of the
    2 tests in this module -- this class's own
    `test_fixed_load_runtime_config_never_tears`, which caught 131 torn
    reads out of a few hundred in one 1.5s run under that mutation. The
    control test in `TestLoadRuntimeConfigTornReadControl` is unaffected
    by this mutation (it already calls the unscoped shape directly) and
    correctly keeps passing either way.
    """

    def test_fixed_load_runtime_config_never_tears(self, tmp_path):
        ok, torn, exc, samples, writer_exitcode, commits = _run_soak(
            tmp_path, use_snapshot=True
        )
        # Without these two, `ok > 20, torn == 0` is equally consistent
        # with a working fix AND with a writer that died on its first
        # `import_yaml` and a reader reading one never-mutated config
        # over and over -- which is not a concurrency soak at all, and
        # would pass this test for a completely unrelated reason. The
        # control test's writer is independent (its own soak, its own
        # process) and would not catch this: a dead writer here says
        # nothing about whether the control's writer ran either.
        assert writer_exitcode == 0, "writer process crashed during the soak"
        assert commits > 20, "writer performed too few imports to be a meaningful soak"
        assert exc == 0, samples
        assert torn == 0, samples
        # A weak sanity check that the soak actually ran enough reads to
        # be meaningful -- not the load-bearing assertion, but a torn==0
        # result over zero reads would prove nothing at all.
        assert ok > 20, "soak produced too few reads to be a meaningful check"


class TestLoadRuntimeConfigTornReadControl:
    """The harness's own sensitivity control (mandated alongside the fix
    itself): the SAME writer, the SAME invariant check, the SAME real
    concurrency -- the only difference is the reader using the pre-fix,
    unscoped shape. If this does not detect torn reads, the fix test
    above proves nothing except that the harness never looks.
    """

    def test_unscoped_read_does_tear(self, tmp_path):
        ok, torn, exc, samples, writer_exitcode, commits = _run_soak(
            tmp_path, use_snapshot=False
        )
        assert writer_exitcode == 0, "writer process crashed during the soak"
        assert commits > 20, "writer performed too few imports to be a meaningful soak"
        assert exc == 0, samples
        assert torn > 0, (
            "control did not detect any torn read -- the harness is "
            "insensitive, so the fixed-path test above proves nothing"
        )

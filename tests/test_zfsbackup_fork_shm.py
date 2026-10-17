"""A-6.1 -- a forked child must not let its parent delete `-wal`/`-shm` out
from under it.

The hazard (finding C1, measured): the pid-keyed engine cache in
`db.get_engine` alone is not fork-safe. A child that merely calls
`get_engine()` discards and rebuilds the engine it inherited, but
`dispose(close=False)` leaves the old pool in a reference cycle, so the
inherited `sqlite3.Connection` -- and with it SQLite's inherited
*per-process* lock bookkeeping -- stays alive until the garbage collector
happens to run. A connection the child opens in that window reuses the
inherited bookkeeping and takes no kernel locks, so the parent can no
longer see it: the parent's last close believes it is the last connection,
checkpoints, and DELETES `-wal`/`-shm` while the child is still connected.
The fix is an explicit `gc.collect()` after the discard (in `get_engine` and
in `dispose_all`'s non-owner branch).

The scenario is the one from the C1 reproduction (scratchpad `fork_shm*.py`):

1. the parent holds a pooled reader connection (as `main()` does after
   reading the config) and forks WITHOUT `dispose_all()` -- the failure mode
   the backstop exists for;
2. the child, with the **garbage collector disabled** (so only an explicit
   collect can save it), rebuilds its engine and keeps a connection open;
3. the parent closes everything; an unrelated third connection opens and
   closes; the sidecars must still be on disk.

**These can hang if the fix regresses** (a wedged lock wait in a forked
child), so the risky part never runs on the test's own thread: it runs in a
driver subprocess in its own session, with a hard timeout and a process-group
kill, and the driver arms `SIGALRM` as a second backstop.

Mutation counts (scratch copy of the tree, one revert at a time): removing the
`gc.collect()` in `get_engine` fails 1 test
(`test_inherited_engine_rebuild_keeps_the_sidecars_for_a_connected_child`);
removing it from `dispose_all`'s non-owner branch fails 1
(`test_child_dispose_all_keeps_the_sidecars_for_a_connected_child`); removing
`dispose_all()` from `BackupDaemon._spawn` fails 1
(`test_spawn_disposes_every_engine_before_the_worker_starts`).

A-13 (every inherited entry purged, one collect, re-entrant lock), same method:
purging only the requested key fails 5; no `gc.collect()` after the purge 7; a
collect on every miss 2; one collect per purged entry 2; a plain `Lock` at
creation 2 (the re-entrant-finalizer test fails cleanly rather than hanging: it
runs against a throwaway lock); a plain `Lock` in `_reinit_lock_after_fork` 1;
building before collecting 1; the loop variables left bound 1.

A-21.5 (`dispose_all` collects inside the lock), same method: the collect moved
out of the lock 2; no collect in `dispose_all` 4 (3 here plus the pre-existing
`test_child_dispose_all_keeps_the_sidecars_for_a_connected_child`); a plain
`Lock` 4 (2 here plus 2 pre-existing).
"""

import gc
import json
import multiprocessing
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
import weakref
from pathlib import Path

import pytest
from sqlalchemy import text as store_text

from zfsbackup.config.store import db as store_db

pytestmark = pytest.mark.unit

FORK_AVAILABLE = "fork" in multiprocessing.get_all_start_methods() and hasattr(os, "fork")
ROOT = Path(__file__).resolve().parent.parent
DRIVER_TIMEOUT = 90

DRIVER = textwrap.dedent(
    '''
    import gc, json, os, select, signal, sqlite3, sys
    from pathlib import Path

    signal.alarm(60)  # self-destruct: never outlive the test's hard timeout

    from sqlalchemy import text
    from zfsbackup.config.store import db as dbmod
    from zfsbackup.config.store.importer import import_yaml
    from zfsbackup.config.store.paths import resolve_config_path

    workdir = Path(sys.argv[1])
    # "get_engine" | "dispose_all" | "parent_disposes" | "other_key"
    scenario = sys.argv[2]
    yaml_path = Path(sys.argv[3])

    db_path = workdir / "config.db"
    resolved = resolve_config_path(str(db_path))
    import_yaml(yaml_path, resolved)
    dbmod.dispose_all()
    url = resolved.url()


    def sidecars():
        return sorted(p.name for p in workdir.iterdir())


    def wait_readable(fd, seconds=30):
        ready, _, _ = select.select([fd], [], [], seconds)
        return bool(ready)


    # The parent holds a pooled connection at fork time. "other_key" (A-13):
    # it is cached under a DIFFERENT key (the writer's) from the one the child
    # asks for (the reader's), so rebuilding "the requested key" alone would
    # leave the inherited engine strongly referenced forever.
    engine = dbmod.get_engine(url, readonly=(scenario != "other_key"))
    with engine.connect() as conn:
        conn.execute(text("select count(*) from datasets")).fetchall()
    if scenario == "parent_disposes":
        dbmod.dispose_all()  # what BackupDaemon._spawn does: the control

    ready_r, ready_w = os.pipe()
    go_r, go_w = os.pipe()
    result_r, result_w = os.pipe()

    pid = os.fork()
    if pid == 0:
        os.close(ready_r); os.close(go_w); os.close(result_r)
        status = {"child": "started"}
        try:
            gc.disable()  # only an EXPLICIT collect may rescue us
            if scenario == "dispose_all":
                dbmod.dispose_all()
            e2 = dbmod.get_engine(url, readonly=True)  # discard + rebuild
            child_conn = e2.connect()
            child_conn.execute(text("select count(*) from datasets")).fetchall()
            os.write(ready_w, b"R")
            if not wait_readable(go_r):
                status["child"] = "timed out waiting for the parent"
            else:
                os.read(go_r, 1)
                child_conn.execute(text("select count(*) from datasets")).fetchall()
                status["child"] = "ok"
        except BaseException as exc:  # report, never hang
            status["child"] = f"error: {type(exc).__name__}: {exc}"
        os.write(result_w, json.dumps(status).encode())
        os._exit(0)

    os.close(ready_w); os.close(go_r); os.close(result_w)
    out = {}
    try:
        if not wait_readable(ready_r):
            out["error"] = "child never became ready"
        else:
            os.read(ready_r, 1)
            dbmod.dispose_all()   # the parent closes everything it has
            gc.collect()
            out["after_parent_close"] = sidecars()
            third = sqlite3.connect(str(db_path))   # an unrelated opener
            third.execute("select count(*) from datasets").fetchall()
            third.close()
            out["after_third_connection"] = sidecars()
    finally:
        os.write(go_w, b"G")
        out["child"] = (
            json.loads(os.read(result_r, 4096).decode())["child"]
            if wait_readable(result_r) else "no result"
        )
        os.waitpid(pid, 0)
    print("RESULT " + json.dumps(out), flush=True)
    '''
)

YAML = "datasets:\n  - name: pool/data\n    frequency: 1h\n"


def run_driver(tmp_path, scenario):
    script = tmp_path / "driver.py"
    script.write_text(DRIVER)
    yaml_path = tmp_path / "cfg.yaml"
    yaml_path.write_text(YAML)
    workdir = tmp_path / "work"
    workdir.mkdir()
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    proc = subprocess.Popen(
        [sys.executable, str(script), str(workdir), scenario, str(yaml_path)],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=DRIVER_TIMEOUT)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)  # the driver AND its forked child
        proc.communicate()
        pytest.fail(f"fork driver hung for {DRIVER_TIMEOUT}s (scenario {scenario})")
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)  # never leave a child behind
        except ProcessLookupError:
            pass
    assert proc.returncode == 0, stderr
    lines = [line for line in stdout.splitlines() if line.startswith("RESULT ")]
    assert lines, f"driver printed no result:\n{stdout}\n{stderr}"
    return json.loads(lines[-1][len("RESULT "):])


@pytest.mark.skipif(not FORK_AVAILABLE, reason="needs os.fork")
class TestChildDoesNotLetParentDeleteSidecars:
    # The setup import (item 15.4) leaves its empty, never-deleted writer lock
    # beside the database; the point here is that the three SQLite files stay.
    EXPECTED = ["config.db", "config.db-shm", "config.db-wal", "config.db.writer.lock"]

    def test_inherited_engine_rebuild_keeps_the_sidecars_for_a_connected_child(
        self, tmp_path
    ):
        """No `dispose_all()` before the fork; the child (GC disabled)
        rebuilds via `get_engine`'s pid check and stays connected."""
        result = run_driver(tmp_path, "get_engine")
        assert result["child"] == "ok", result
        assert result["after_parent_close"] == self.EXPECTED, result
        assert result["after_third_connection"] == self.EXPECTED, result

    def test_child_dispose_all_keeps_the_sidecars_for_a_connected_child(self, tmp_path):
        """Same, but the child calls `dispose_all()` (the non-owner branch)
        before opening its own connection."""
        result = run_driver(tmp_path, "dispose_all")
        assert result["child"] == "ok", result
        assert result["after_parent_close"] == self.EXPECTED, result
        assert result["after_third_connection"] == self.EXPECTED, result

    def test_other_key_inherited_engine_is_purged_with_gc_disabled_in_the_child(
        self, tmp_path
    ):
        """A-13 (R4): the parent cached the WRITER key, the child (GC disabled)
        asks for the READER key. Every inherited entry must go, not just the
        requested key's, or the inherited connection stays referenced by the
        cache and nothing can free it."""
        result = run_driver(tmp_path, "other_key")
        assert result["child"] == "ok", result
        assert result["after_parent_close"] == self.EXPECTED, result
        assert result["after_third_connection"] == self.EXPECTED, result

    def test_control_parent_disposing_before_the_fork_is_always_safe(self, tmp_path):
        """What `BackupDaemon._spawn` does. Passes with or without the gc
        fix, which is the point: the fix is the backstop, this is the rule."""
        result = run_driver(tmp_path, "parent_disposes")
        assert result["child"] == "ok", result
        assert result["after_third_connection"] == self.EXPECTED, result


class TestSupervisorHoldsNoEngineAtSpawn:
    def test_spawn_disposes_every_engine_before_the_worker_starts(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        """After `main()` read the config the supervisor's pool holds a live
        connection; `_spawn` must have closed it by the time `start()` forks."""
        from zfsbackup.daemon import BackupDaemon
        from zfsbackup.runtime_config import load_runtime_config

        load_runtime_config(sqlite_config_db)
        assert store_db._ENGINES, "precondition: the load cached an engine"

        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        seen = {}
        worker = mocker.MagicMock(pid=1)
        worker.start.side_effect = lambda: seen.update(engines=dict(store_db._ENGINES))
        mocker.patch.object(daemon, "_new_worker", return_value=worker)

        daemon._spawn("snapshot")

        assert seen == {"engines": {}}

    def test_the_engine_cache_is_keyed_by_the_owning_pid(self, tmp_path):
        url = f"sqlite:///{tmp_path / 'x.db'}"
        store_db.get_engine(url)
        ((owner_pid, _engine),) = store_db._ENGINES.values()
        assert owner_pid == os.getpid()

    @pytest.mark.skipif(not FORK_AVAILABLE, reason="needs os.fork")
    def test_a_forked_child_gets_a_rebuilt_engine_not_the_inherited_one(self, tmp_path):
        url = f"sqlite:///{tmp_path / 'x.db'}"
        parent_engine = store_db.get_engine(url)
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:  # child: report on a pipe, leave via os._exit, never raise
            code = 1
            try:
                os.close(read_fd)
                child_engine = store_db.get_engine(url)
                ((owner, _),) = store_db._ENGINES.values()
                os.write(
                    write_fd,
                    json.dumps(
                        {"rebuilt": child_engine is not parent_engine,
                         "owner_is_child": owner == os.getpid()}
                    ).encode(),
                )
                code = 0
            finally:
                os._exit(code)
        os.close(write_fd)
        try:
            data = os.read(read_fd, 4096)
        finally:
            os.close(read_fd)
            os.waitpid(pid, 0)
        assert json.loads(data) == {"rebuilt": True, "owner_is_child": True}
        assert store_db.get_engine(url) is parent_engine  # the parent's is untouched


# ---------------------------------------------------------------------------
# A-13 (R4): `get_engine` purges EVERY inherited entry, collects once
# ---------------------------------------------------------------------------


class TestGetEnginePurgesEveryInheritedEntry:
    """Whitebox, without a fork: `os.getpid` is patched so entries the test
    built read as inherited ("owner pid is not mine"). The real-fork proof is
    the `other_key` driver scenario above."""

    @pytest.fixture
    def urls(self, tmp_path):
        return [f"sqlite:///{tmp_path / name}" for name in ("a.db", "b.db", "c.db")]

    @pytest.fixture
    def collects(self, monkeypatch):
        """Count `gc.collect()` calls made by `db.get_engine`, still collecting."""
        calls = []
        real = store_db.gc.collect

        def counting(*args):
            calls.append(args)
            return real(*args)

        monkeypatch.setattr(store_db.gc, "collect", counting)
        return calls

    def _as_inherited(self, monkeypatch):
        fake = os.getpid() + 1
        monkeypatch.setattr(store_db.os, "getpid", lambda: fake)
        return fake

    def test_one_miss_purges_all_other_key_entries_and_collects_exactly_once(
        self, urls, monkeypatch, collects
    ):
        inherited = [
            store_db.get_engine(urls[0], readonly=True),
            store_db.get_engine(urls[0], readonly=False),
            store_db.get_engine(urls[1], readonly=True),
        ]
        disposals = []
        for engine in inherited:
            real = engine.dispose
            monkeypatch.setattr(
                engine, "dispose",
                lambda close=True, _real=real, _e=engine: (disposals.append((_e, close)), _real(close=close))[1],
            )
        assert len(store_db._ENGINES) == 3
        collects.clear()  # building the above collected nothing, but be explicit
        fake = self._as_inherited(monkeypatch)

        fresh = store_db.get_engine(urls[2], readonly=True)  # a key nobody cached

        # Every inherited engine was dropped WITHOUT closing, and only those.
        assert sorted(id(e) for e, _ in disposals) == sorted(id(e) for e in inherited)
        assert {close for _, close in disposals} == {False}
        assert list(store_db._ENGINES) == [(urls[2], True, True)]
        assert store_db._ENGINES[(urls[2], True, True)] == (fake, fresh)
        assert len(collects) == 1  # one collect for the whole purge, not one per entry
        for engine in inherited:  # hygiene: these were really built here
            engine.dispose()
        fresh.dispose()

    # Freeing an inherited connection that was dropped with `close=False` IS
    # an "unclosed database" ResourceWarning by design. Ignored (not recorded):
    # a recorded ResourceWarning keeps its `source` -- the connection itself --
    # alive, which would defeat the very count this test makes (`-X dev`).
    @pytest.mark.filterwarnings("ignore::ResourceWarning")
    @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
    def test_the_inherited_connection_is_actually_freed_before_the_call_returns(
        self, urls, monkeypatch
    ):
        # The functional claim behind the collect (C1): with the cache holding
        # the only strong reference to an OTHER key's engine, the purge makes
        # its pooled sqlite3 connection collectable and the collect frees it
        # before `get_engine` returns. (`sqlite3.Connection` cannot be weakly
        # referenced, so count live instances instead.)
        def live_connections():
            return sum(isinstance(o, sqlite3.Connection) for o in gc.get_objects())

        engine = store_db.get_engine(urls[0], readonly=False)
        with engine.connect() as conn:
            conn.execute(store_text("select 1")).fetchall()
        engine_ref = weakref.ref(engine)
        del engine, conn
        gc.collect()
        with_inherited = live_connections()
        assert engine_ref() is not None  # precondition: the cache holds it
        self._as_inherited(monkeypatch)

        fresh = store_db.get_engine(urls[0], readonly=True)  # a DIFFERENT key

        assert engine_ref() is None
        assert live_connections() == with_inherited - 1, (
            "the inherited connection outlived the purge"
        )
        fresh.dispose()

    def test_a_same_key_stale_entry_is_purged_and_rebuilt(
        self, urls, monkeypatch, collects
    ):
        stale = store_db.get_engine(urls[0])
        fake = self._as_inherited(monkeypatch)
        collects.clear()

        rebuilt = store_db.get_engine(urls[0])

        assert rebuilt is not stale
        assert store_db._ENGINES[(urls[0], False, True)] == (fake, rebuilt)
        assert len(collects) == 1
        stale.dispose()
        rebuilt.dispose()

    def test_no_collect_when_nothing_was_inherited(self, urls, collects):
        first = store_db.get_engine(urls[0])
        assert store_db.get_engine(urls[0]) is first  # a hit
        store_db.get_engine(urls[1])  # a miss with only own-pid entries around
        assert collects == []

    def test_an_own_pid_entry_is_never_purged_by_a_miss(self, urls):
        mine = store_db.get_engine(urls[0])
        store_db.get_engine(urls[1])
        assert store_db._ENGINES[(urls[0], False, True)] == (os.getpid(), mine)

    def test_the_lock_is_an_rlock(self):
        assert isinstance(store_db._ENGINES_LOCK, type(threading.RLock()))

    def test_the_lock_is_still_an_rlock_after_the_fork_handler_replaces_it(
        self, monkeypatch
    ):
        monkeypatch.setattr(store_db, "_ENGINES_LOCK", store_db._ENGINES_LOCK)  # restore
        before = store_db._ENGINES_LOCK

        store_db._reinit_lock_after_fork()

        assert store_db._ENGINES_LOCK is not before
        assert isinstance(store_db._ENGINES_LOCK, type(threading.RLock()))

    def test_a_finalizer_that_reenters_the_cache_during_the_collect_does_not_deadlock(
        self, urls, monkeypatch
    ):
        """`gc.collect()` runs arbitrary finalizers on the calling thread while
        `get_engine` holds the lock. One that reaches `get_engine`/
        `dispose_all` would self-deadlock on a plain `Lock`. Run in a thread
        with a timeout so a regression fails instead of hanging the suite."""
        # A throwaway lock of the module's own type: on a regression the
        # stuck thread would hold it forever, and it must not be the real one
        # (every later test's `dispose_all()` would then hang the whole run).
        monkeypatch.setattr(store_db, "_ENGINES_LOCK", type(store_db._ENGINES_LOCK)())
        store_db.get_engine(urls[0], readonly=True)
        self._as_inherited(monkeypatch)
        real_collect = store_db.gc.collect
        seen = {}

        def collect_with_reentrant_finalizer(*args):
            seen["inner"] = store_db.get_engine(urls[1])  # re-enters the lock
            return real_collect(*args)

        monkeypatch.setattr(store_db.gc, "collect", collect_with_reentrant_finalizer)
        outcome = {}

        def work():
            try:
                outcome["engine"] = store_db.get_engine(urls[2])
            except BaseException as exc:  # report, never hang
                outcome["error"] = exc

        thread = threading.Thread(target=work, daemon=True)
        thread.start()
        thread.join(timeout=15)

        assert not thread.is_alive(), "get_engine deadlocked re-entering _ENGINES_LOCK"
        assert "error" not in outcome, outcome
        assert (urls[2], False, True) in store_db._ENGINES
        assert seen["inner"] is not None

    def test_the_lock_is_held_across_the_purge_collect_and_build(self, urls, monkeypatch):
        # gc.collect and the build both run INSIDE the lock (moving either out
        # hands out an engine before the inherited connection is gone).
        store_db.get_engine(urls[0], readonly=True)
        self._as_inherited(monkeypatch)
        real_collect = store_db.gc.collect
        real_make = store_db.make_engine
        held = {}

        def lock_is_held():
            # Another thread cannot take it while this thread holds it.
            # Acquire AND release on the probe's own thread: an RLock cannot
            # be released by a thread that does not own it, and a lock left
            # held by a dead thread would hang every later `dispose_all`.
            result = {}

            def probe_body():
                result["got"] = store_db._ENGINES_LOCK.acquire(blocking=False)
                if result["got"]:
                    store_db._ENGINES_LOCK.release()

            probe = threading.Thread(target=probe_body)
            probe.start()
            probe.join()
            return not result["got"]

        order = []
        monkeypatch.setattr(
            store_db.gc, "collect",
            lambda *a: (
                order.append("collect"), held.setdefault("collect", lock_is_held()),
                real_collect(*a),
            )[-1],
        )
        monkeypatch.setattr(
            store_db, "make_engine",
            lambda *a, **k: (
                order.append("build"), held.setdefault("build", lock_is_held()),
                real_make(*a, **k),
            )[-1],
        )

        store_db.get_engine(urls[1]).dispose()

        assert held == {"collect": True, "build": True}
        # ...and the inherited connection is gone BEFORE anything is built.
        assert order == ["collect", "build"]


# ---------------------------------------------------------------------------
# A-21.5: `dispose_all` collects INSIDE the (re-entrant) lock, like `get_engine`
# ---------------------------------------------------------------------------


class TestDisposeAllCollectsInsideTheLock:
    """Whitebox, without a fork (`os.getpid` patched so the entries read as
    inherited). If the collect ran after the lock was released, another
    thread's `get_engine` could build a fresh engine -- and open its own
    connection -- while the inherited connection was still alive."""

    @pytest.fixture
    def urls(self, tmp_path):
        return [f"sqlite:///{tmp_path / name}" for name in ("a.db", "b.db", "c.db")]

    @staticmethod
    def _as_inherited(monkeypatch):
        fake = os.getpid() + 1
        monkeypatch.setattr(store_db.os, "getpid", lambda: fake)

    @staticmethod
    def _another_thread_is_blocked():
        """True if the module lock is held by THIS thread (a second thread
        cannot take it)."""
        result = {}

        def probe_body():
            # Acquire AND release on the probe's own thread: an RLock cannot
            # be released by a thread that does not own it, and a lock left
            # held by a dead thread would hang every later test.
            result["got"] = store_db._ENGINES_LOCK.acquire(blocking=False)
            if result["got"]:
                store_db._ENGINES_LOCK.release()

        probe = threading.Thread(target=probe_body)
        probe.start()
        probe.join()
        return not result["got"]

    def test_the_collect_runs_while_the_lock_is_held(self, urls, monkeypatch):
        store_db.get_engine(urls[0], readonly=True)
        store_db.get_engine(urls[1], readonly=False)
        self._as_inherited(monkeypatch)
        real_collect = store_db.gc.collect
        seen = []

        def collect(*args):
            seen.append({
                "owned": store_db._ENGINES_LOCK._is_owned(),
                "blocks_others": self._another_thread_is_blocked(),
                # consistent BEFORE the collect, so a re-entrant finalizer is safe
                "cache": dict(store_db._ENGINES),
            })
            return real_collect(*args)

        monkeypatch.setattr(store_db.gc, "collect", collect)

        store_db.dispose_all()

        assert seen == [{"owned": True, "blocks_others": True, "cache": {}}]
        # ...and the lock is released again afterwards.
        assert not store_db._ENGINES_LOCK._is_owned()
        assert not self._another_thread_is_blocked()
        assert store_db._ENGINES == {}

    def test_no_collect_when_every_entry_is_this_processs_own(self, urls, monkeypatch):
        store_db.get_engine(urls[0])
        calls = []
        monkeypatch.setattr(store_db.gc, "collect", lambda *a: calls.append(a) or 0)
        store_db.dispose_all()
        assert calls == []
        assert store_db._ENGINES == {}

    def test_a_finalizer_that_reenters_the_cache_during_the_collect_does_not_deadlock(
        self, urls, monkeypatch
    ):
        """The collect now runs under the lock, so a finalizer that reaches
        `get_engine`/`dispose_all` would self-deadlock on a plain `Lock`. Run
        on a thread with a timeout, against a throwaway lock of the module's
        own type, so a regression fails instead of hanging the suite."""
        monkeypatch.setattr(store_db, "_ENGINES_LOCK", type(store_db._ENGINES_LOCK)())
        store_db.get_engine(urls[0], readonly=True)
        self._as_inherited(monkeypatch)
        real_collect = store_db.gc.collect
        seen = {}

        def collect_with_reentrant_finalizer(*args):
            seen["inner"] = store_db.get_engine(urls[1])  # re-enters the lock
            store_db.dispose_all()                        # and again, nested
            return real_collect(*args)

        monkeypatch.setattr(store_db.gc, "collect", collect_with_reentrant_finalizer)
        outcome = {}

        def work():
            try:
                store_db.dispose_all()
                outcome["done"] = True
            except BaseException as exc:  # report, never hang
                outcome["error"] = exc

        thread = threading.Thread(target=work, daemon=True)
        thread.start()
        thread.join(timeout=15)

        assert not thread.is_alive(), "dispose_all deadlocked re-entering _ENGINES_LOCK"
        assert "error" not in outcome, outcome
        assert outcome == {"done": True}
        assert seen["inner"] is not None

    def test_a_concurrent_get_engine_waits_for_the_collect(self, urls, monkeypatch):
        """The point of the change: while `dispose_all` is collecting, another
        thread's `get_engine` cannot build (and so cannot open a connection)."""
        store_db.get_engine(urls[0], readonly=True)
        self._as_inherited(monkeypatch)
        real_collect = store_db.gc.collect
        in_collect, release = threading.Event(), threading.Event()
        order = []
        real_make = store_db.make_engine

        def slow_collect(*args):
            in_collect.set()
            assert release.wait(timeout=15)
            order.append("collect-done")
            return real_collect(*args)

        def recording_make(*a, **k):
            order.append("build")
            return real_make(*a, **k)

        monkeypatch.setattr(store_db.gc, "collect", slow_collect)
        monkeypatch.setattr(store_db, "make_engine", recording_make)
        disposer = threading.Thread(target=store_db.dispose_all, daemon=True)
        built = {}
        builder = threading.Thread(
            target=lambda: built.update(engine=store_db.get_engine(urls[1])), daemon=True
        )
        disposer.start()
        try:
            assert in_collect.wait(timeout=15)
            builder.start()
            builder.join(timeout=0.5)
            assert builder.is_alive(), "get_engine did not wait for dispose_all's collect"
            assert order == []
        finally:
            release.set()
        disposer.join(timeout=15)
        builder.join(timeout=15)
        assert not disposer.is_alive() and not builder.is_alive()
        assert order == ["collect-done", "build"]
        built["engine"].dispose()

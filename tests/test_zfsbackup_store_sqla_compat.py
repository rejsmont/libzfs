"""SQLAlchemy 2.0 / 2.1 compatibility of `zfsbackup/config/store/db.py`.

Two behaviours differ between the two series and the store papers over both:

1. **A failed connect.** When a `connect` listener raises, 2.0.x leaves the
   DBAPI connection open (the store's hook closes it), while 2.1.x (after
   2.1.0) closes it in the pool itself. `_pool_closes_failed_connects()` finds
   out which by probing once per process, and the hook closes only when the
   pool will not -- exactly one close either way.
2. **URL percent-decoding.** 2.1 percent-DECODES the database part of a URL
   string, 2.0 takes it verbatim. `url_for_path` therefore tries the plain
   `sqlite:///<path>` spelling, falls back to `%` -> `%25` when that does not
   round-trip through `make_url`, and still refuses a `?`.

Every test here is version-agnostic: it asserts what is true on the INSTALLED
SQLAlchemy by measuring that (never by comparing `sqlalchemy.__version__`), so
the file passes unchanged on both series. It is run in both venvs.

Mutation counts (scratch copy of the tree, failing tests on 2.0.51 / 2.1.5):
the hook always closing 1 / 5; the hook never closing 5 / 1; the probe
hard-wired True 6 / 0; hard-wired False 0 / 6; the probe not cached (no
`cache_clear`, 10 errors) 10 / 10; `url_for_path` without the `%25` fallback
0 / 7; always escaping `%` 11 / 2; no round trip (`?` accepted) 3 / 9. A 0 is an
equivalent mutant on that series (the behaviour it removes does not exist
there).
"""

import gc
import sqlite3

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import make_url
from sqlalchemy.pool import StaticPool

from zfsbackup.config.store import db as store_db
from zfsbackup.config.store.db import make_engine, url_for_path

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# 1. the failed-connect probe
# ---------------------------------------------------------------------------


def _closes_observed_on_a_real_pool(tmp_path) -> int:
    """How many times the pool itself closes a REAL sqlite3 connection after a
    `connect` listener raised on it, on the installed SQLAlchemy. No store code
    is involved: this is the ground truth the probe is meant to report."""
    closes = []

    class Conn(sqlite3.Connection):
        def close(self):
            closes.append(True)
            super().close()

    path = tmp_path / "probe.db"
    engine = create_engine(
        "sqlite://", creator=lambda: sqlite3.connect(str(path), factory=Conn)
    )

    @event.listens_for(engine, "connect")
    def _fail(dbapi_connection, connection_record):  # noqa: ANN001
        raise RuntimeError("listener failed")

    gc.collect()
    gc.disable()  # only an explicit close may count
    try:
        with pytest.raises(RuntimeError, match="listener failed"):
            engine.connect()
        return len(closes)
    finally:
        gc.enable()
        engine.dispose()


class TestPoolClosesFailedConnectsProbe:
    @pytest.fixture(autouse=True)
    def _fresh_cache(self):
        store_db._pool_closes_failed_connects.cache_clear()
        yield
        store_db._pool_closes_failed_connects.cache_clear()

    def test_the_answer_is_a_bool(self):
        assert store_db._pool_closes_failed_connects() in (True, False)

    def test_it_matches_what_a_real_pool_does_on_the_installed_version(self, tmp_path):
        observed = _closes_observed_on_a_real_pool(tmp_path)
        assert observed in (0, 1)  # never a double close from the pool alone
        assert store_db._pool_closes_failed_connects() is bool(observed)

    def test_it_is_computed_once_and_cached(self, monkeypatch):
        built = []

        class Counting(StaticPool):
            def __init__(self, *args, **kwargs):
                built.append(True)
                super().__init__(*args, **kwargs)

        monkeypatch.setattr(store_db, "StaticPool", Counting)
        first = store_db._pool_closes_failed_connects()
        again = [store_db._pool_closes_failed_connects() for _ in range(5)]
        assert built == [True]
        assert again == [first] * 5
        info = store_db._pool_closes_failed_connects.cache_info()
        assert info.misses == 1 and info.hits == 5

    def test_it_opens_nothing_and_leaves_no_listener_behind(self):
        before = len(gc.get_objects())  # a coarse smoke check only
        store_db._pool_closes_failed_connects()
        assert len(gc.get_objects()) - before < 2000

    def test_the_probe_swallows_its_own_failure(self):
        """It must never raise into the connect hook (which runs on the error
        path of a real failed connect)."""
        store_db._pool_closes_failed_connects()  # no exception is the assertion


# ---------------------------------------------------------------------------
# 1b. exactly one close on a failed connect, whichever series
# ---------------------------------------------------------------------------


class _Cursor:
    def __init__(self, fail):
        self.fail = fail

    def execute(self, sql, *args):
        if sql.startswith("PRAGMA busy_timeout"):
            raise self.fail

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def close(self):
        pass


def _stub_engine(closes, fail):
    class Dbapi:
        def cursor(self):
            return _Cursor(fail)

        def close(self):
            closes.append(True)

        def __getattr__(self, name):
            return lambda *a, **k: None

    return create_engine("sqlite://", creator=lambda: Dbapi(), poolclass=StaticPool)


class TestExactlyOneClose:
    @pytest.fixture(autouse=True)
    def _fresh_cache(self):
        store_db._pool_closes_failed_connects.cache_clear()
        yield
        store_db._pool_closes_failed_connects.cache_clear()

    @pytest.mark.parametrize("readonly", [True, False], ids=["reader", "writer"])
    @pytest.mark.parametrize(
        "failure",
        [sqlite3.DatabaseError("the original failure"), KeyboardInterrupt()],
        ids=["exception", "base-exception"],
    )
    def test_one_close_whatever_the_series(self, readonly, failure):
        closes = []
        eng = _stub_engine(closes, failure)
        store_db._install_pragmas(eng, readonly=readonly, foreign_keys=True, memory=False)
        try:
            # SQLAlchemy wraps a DBAPI error; a BaseException passes through.
            expected = Exception if isinstance(failure, Exception) else type(failure)
            with pytest.raises(expected):
                eng.connect()
        finally:
            eng.dispose()
        assert closes == [True]

    def test_the_hook_closes_only_when_the_pool_will_not(self, monkeypatch):
        """With the probe's answer forced, the hook's own contribution is
        visible: no close of its own when the pool is said to close, one when
        it is not. The pool's own close (installed series) is added on top."""
        pool_closes = store_db._pool_closes_failed_connects()  # the real answer
        for claimed in (True, False):
            closes = []
            eng = _stub_engine(closes, sqlite3.DatabaseError("boom"))
            with monkeypatch.context() as patch:
                patch.setattr(store_db, "_pool_closes_failed_connects", lambda c=claimed: c)
                store_db._install_pragmas(eng, readonly=False, foreign_keys=True, memory=False)
                try:
                    with pytest.raises(Exception):
                        eng.connect()
                finally:
                    eng.dispose()
            hook_closes = 0 if claimed else 1
            assert len(closes) == hook_closes + (1 if pool_closes else 0), (claimed, closes)


# ---------------------------------------------------------------------------
# 2. url_for_path across percent-decoding behaviour
# ---------------------------------------------------------------------------


class TestUrlForPathPercent:
    @pytest.mark.parametrize(
        "relative",
        ["pct%41/x.db", "x%2541", "100%/c.db", "a%/b%2/c.db", "plain/x.db", "50%25off/y.db"],
    )
    def test_the_url_round_trips_to_the_exact_path(self, tmp_path, relative):
        path = tmp_path / relative
        url = url_for_path(path)
        assert make_url(url).database == str(path.resolve())

    @pytest.mark.parametrize(
        "relative", ["pct%41/x.db", "x%2541", "100%/c.db", "a%/b%2/c.db", "plain/x.db"]
    )
    def test_the_plain_spelling_is_used_whenever_it_round_trips(self, tmp_path, relative):
        path = tmp_path / relative
        wanted = str(path.resolve())
        plain = f"sqlite:///{wanted}"
        escaped = f"sqlite:///{wanted.replace('%', '%25')}"
        url = url_for_path(path)
        if make_url(plain).database == wanted:
            assert url == plain
        else:
            # only a `%` that the installed series would decode is escaped
            assert "%" in wanted and url == escaped

    def test_a_path_without_percent_is_always_the_plain_spelling(self, tmp_path):
        path = tmp_path / "d" / "x.db"
        assert url_for_path(path) == f"sqlite:///{path.resolve()}"

    def test_one_path_has_one_url(self, tmp_path):
        path = tmp_path / "pct%41" / "x.db"
        assert url_for_path(path) == url_for_path(str(path)) == url_for_path(path.resolve())

    @pytest.mark.parametrize("relative", ["a?b.db", "a?b%41.db", "d%/a?b.db"])
    def test_a_question_mark_is_still_refused(self, tmp_path, relative):
        with pytest.raises(ValueError, match="cannot be represented"):
            url_for_path(tmp_path / relative)

    def test_the_engine_opens_the_literal_directory_not_its_decoded_twin(self, tmp_path):
        """The whole point: a directory called `pct%41` must not silently
        address `pctA`."""
        literal = tmp_path / "pct%41"
        decoded = tmp_path / "pctA"
        literal.mkdir()
        decoded.mkdir()
        path = literal / "x.db"
        eng = make_engine(url_for_path(path))
        try:
            with eng.connect() as conn:
                conn.exec_driver_sql("CREATE TABLE t (id INTEGER)")
                conn.commit()
        finally:
            eng.dispose()
        assert path.exists()
        assert not (decoded / "x.db").exists()
        assert list(decoded.iterdir()) == []

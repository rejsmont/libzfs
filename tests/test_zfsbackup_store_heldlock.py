"""Store additions of the Phase 2 close-out:

- ``read_generation``                  just the generation, no config load, nothing left open;
- ``write_config(..., lock=held)``     a multi-step edit holds the writer lock across its side
                                       effects and hands it in; ``require_writer_lock_held``
                                       is what makes that safe;
- the anchor-name WARNING              a destination name that cannot be a ZFS user-property
                                       suffix (``validate_config`` / import / ``write_config``);
- the reworded never-prune load warning (the same text as ``validate_config``'s, no ``id=``).

Same conventions as ``test_zfsbackup_store_access.py`` (whose fixtures it reuses): real files
under ``tmp_path``, always opened through ``store.db``.
"""

import logging
import sqlite3
import threading
from contextlib import contextmanager

import pytest

from test_zfsbackup_store_access import cfg, db_path, resolved, seeded, store_dir  # noqa: F401
from zfsbackup.config import BackupConfig, DatasetConfig, Destination, Duration, RemoteDatasetConfig
from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    ConfigDbNotFound,
    destination_name_problem,
    dispose_all,
    import_yaml,
    load_config,
    open_config_session,
    read_config,
    read_generation,
    require_writer_lock_held,
    resolve_config_path,
    validate_config,
    write_config,
    writer_lock,
    writer_lock_path,
)
from zfsbackup.config.store import access as store_access
from zfsbackup.config.store import db as store_db
from zfsbackup.config.store import mapper as store_mapper
from zfsbackup.config.store import paths as store_paths


def names(resolved):
    snap = read_config(resolved)
    dispose_all()
    return [d.name for d in snap.config.datasets]


def generation_of(resolved):
    snap = read_config(resolved)
    dispose_all()
    return snap.generation


class TestReadGeneration:
    def test_it_returns_the_stored_generation_and_follows_writes(self, seeded):
        assert read_generation(seeded) == 0
        write_config(seeded, cfg(), expected_generation=0)
        assert read_generation(seeded) == 1
        write_config(seeded, cfg("tank/y"), expected_generation=1)
        assert read_generation(seeded) == 2 == generation_of(seeded)

    def test_it_returns_an_int(self, seeded):
        assert type(read_generation(seeded)) is int

    def test_an_uninitialised_store_is_a_value_error(self, seeded, db_path):
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("DELETE FROM global_settings")
        conn.commit()
        conn.close()
        with pytest.raises(ValueError, match="uninitialized"):
            read_generation(seeded)

    def test_it_does_not_check_the_schema(self, seeded, db_path):
        """`read_config` refuses an unknown revision; the generation read does not
        (the write that follows re-checks under its own lock)."""
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE alembic_version SET version_num = 'deadbeef0000'")
        conn.commit()
        conn.close()
        assert read_generation(seeded) == 0
        with pytest.raises(Exception):
            read_config(seeded)
        dispose_all()

    def test_a_missing_database_is_refused_and_nothing_is_created(self, tmp_path):
        missing = resolve_config_path(str(tmp_path / "nope" / "config.db"))
        with pytest.raises(ConfigDbNotFound):
            read_generation(missing)
        assert not (tmp_path / "nope").exists()

    def test_it_loads_no_configuration(self, seeded, monkeypatch):
        def boom(*args, **kwargs):
            raise AssertionError("read_generation must not load the configuration")

        monkeypatch.setattr(store_access, "load_config_snapshot", boom, raising=False)
        monkeypatch.setattr(store_mapper, "load_config", boom)
        assert read_generation(seeded) == 0

    def test_it_takes_no_writer_lock(self, seeded):
        # works while ANOTHER thread holds it (a writer would have to wait)
        ready, leave = threading.Event(), threading.Event()

        def holder():
            with writer_lock(seeded):
                ready.set()
                leave.wait(30)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            assert read_generation(seeded) == 0
        finally:
            leave.set()
            thread.join(10)

    def test_it_creates_no_lock_file(self, seeded):
        writer_lock_path(seeded).unlink()          # the import made one
        read_generation(seeded)
        assert not writer_lock_path(seeded).exists()

    def test_it_works_inside_the_writer_lock(self, seeded):
        with writer_lock(seeded):
            assert read_generation(seeded) == 0

    def test_no_transaction_is_left_open(self, seeded, db_path, monkeypatch):
        seen = []
        real = store_access.open_config_connection

        @contextmanager
        def watching(resolved, **kwargs):
            with real(resolved, **kwargs) as connection:
                yield connection
                # SQLite's own state (SQLAlchemy's autobegun Transaction is not the point)
                seen.append(connection.connection.dbapi_connection.in_transaction)

        monkeypatch.setattr(store_access, "open_config_connection", watching)
        assert read_generation(seeded) == 0
        assert seen == [False]
        # ...and a writer is not held up by a lingering read snapshot
        other = sqlite3.connect(db_path, timeout=0)
        try:
            other.execute("BEGIN IMMEDIATE")
            other.execute("ROLLBACK")
        finally:
            other.close()

    def test_a_failure_rolls_back_and_is_reraised(self, seeded, monkeypatch):
        # a database without the table: the OperationalError propagates, nothing stays open
        real = store_access.open_config_connection
        seen = []

        @contextmanager
        def watching(resolved, **kwargs):
            with real(resolved, **kwargs) as connection:
                real_execute = connection.execute

                def execute(statement, *a, **k):
                    if "SELECT generation" in str(statement):
                        raise sqlite3.OperationalError("no such table: global_settings")
                    return real_execute(statement, *a, **k)

                connection.execute = execute
                try:
                    yield connection
                finally:
                    connection.execute = real_execute
                    seen.append(connection.connection.dbapi_connection.in_transaction)

        monkeypatch.setattr(store_access, "open_config_connection", watching)
        with pytest.raises(sqlite3.OperationalError):
            read_generation(seeded)
        assert seen == [False]
        monkeypatch.undo()
        assert read_generation(seeded) == 0     # and the next call works


class TestRequireWriterLockHeld:
    def test_the_object_the_with_block_yielded_passes(self, seeded):
        with writer_lock(seeded) as held:
            require_writer_lock_held(seeded, held)

    def test_it_takes_and_releases_nothing(self, seeded):
        with writer_lock(seeded) as held:
            require_writer_lock_held(seeded, held)
            require_writer_lock_held(seeded, held)
            with pytest.raises(RuntimeError):       # still held exactly once
                with writer_lock(seeded):
                    pass

    @pytest.mark.parametrize("bad", [None, True, "x", 3, object()])
    def test_anything_but_the_yielded_path_is_a_value_error(self, seeded, bad):
        with writer_lock(seeded):
            with pytest.raises(ValueError, match="not the writer lock"):
                require_writer_lock_held(seeded, bad)

    def test_a_path_of_another_name_is_a_value_error(self, seeded, tmp_path):
        with writer_lock(seeded) as held:
            with pytest.raises(ValueError, match="not the writer lock"):
                require_writer_lock_held(seeded, tmp_path / "other.writer.lock")

    def test_a_stale_handle_is_a_value_error(self, seeded):
        with writer_lock(seeded) as held:
            pass
        with pytest.raises(ValueError, match="does not hold the writer lock"):
            require_writer_lock_held(seeded, held)

    def test_without_a_lock_file(self, seeded):
        writer_lock_path(seeded).unlink()
        with pytest.raises(ValueError, match="cannot be inspected"):
            require_writer_lock_held(seeded, writer_lock_path(seeded))

    def test_another_databases_lock_is_a_value_error(self, seeded, tmp_path):
        other = resolve_config_path(str(tmp_path / "other" / "config.db"))
        other.resolved_path.parent.mkdir()
        with writer_lock(other) as other_held:
            with pytest.raises(ValueError, match="not the writer lock"):
                require_writer_lock_held(seeded, other_held)

    def test_another_threads_lock_is_a_value_error(self, seeded):
        ready, leave = threading.Event(), threading.Event()
        box = {}

        def holder():
            with writer_lock(seeded) as held:
                box["held"] = held
                ready.set()
                leave.wait(30)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            with pytest.raises(ValueError, match="does not hold the writer lock"):
                require_writer_lock_held(seeded, box["held"])
        finally:
            leave.set()
            thread.join(10)


class TestWriteConfigWithAHeldLock:
    def test_a_write_under_a_held_lock_commits_and_returns_plus_one(self, seeded):
        with writer_lock(seeded) as held:
            gen = read_generation(seeded)
            result = write_config(seeded, cfg("tank/held"), expected_generation=gen, lock=held)
        assert result.generation == gen + 1
        assert names(seeded) == ["tank/held"]

    def test_it_does_not_take_the_lock_again(self, seeded, monkeypatch):
        def forbidden(*args, **kwargs):
            raise AssertionError("write_config must not re-acquire a lock it was handed")

        with writer_lock(seeded) as held:
            monkeypatch.setattr(store_access, "writer_lock", forbidden)
            write_config(seeded, cfg(), expected_generation=0, lock=held)

    def test_without_lock_it_still_takes_its_own(self, seeded, monkeypatch):
        calls = []
        real = store_access.writer_lock

        @contextmanager
        def counting(resolved, **kwargs):
            calls.append(kwargs)
            with real(resolved, **kwargs) as held:
                yield held

        monkeypatch.setattr(store_access, "writer_lock", counting)
        write_config(seeded, cfg(), expected_generation=0)
        assert len(calls) == 1

    def test_it_does_not_release_the_lock(self, seeded):
        with writer_lock(seeded) as held:
            write_config(seeded, cfg("tank/one"), expected_generation=0, lock=held)
            require_writer_lock_held(seeded, held)                      # still held
            write_config(seeded, cfg("tank/two"), expected_generation=1, lock=held)  # chains
            with pytest.raises(ConfigBusy):                             # another thread cannot get in
                box = {}

                def other():
                    try:
                        with writer_lock(seeded, timeout=0.2):
                            box["got"] = True
                    except BaseException as exc:  # noqa: BLE001
                        box["exc"] = exc

                thread = threading.Thread(target=other)
                thread.start()
                thread.join(10)
                raise box["exc"]
        assert names(seeded) == ["tank/two"]
        with writer_lock(seeded, timeout=0.2):                          # released by the with block
            pass

    def test_a_second_plain_writer_lock_in_the_same_thread_still_raises(self, seeded):
        with writer_lock(seeded) as held:
            write_config(seeded, cfg(), expected_generation=0, lock=held)
            with pytest.raises(RuntimeError):
                with writer_lock(seeded):
                    pass

    def test_lock_timeout_is_ignored(self, seeded):
        with writer_lock(seeded) as held:
            write_config(seeded, cfg(), expected_generation=0, lock=held, lock_timeout=0)

    def test_a_stale_lock_is_a_value_error_and_nothing_is_written(self, seeded):
        with writer_lock(seeded) as held:
            pass
        before = names(seeded)
        with pytest.raises(ValueError, match="does not hold the writer lock"):
            write_config(seeded, cfg("tank/stale"), expected_generation=0, lock=held)
        assert names(seeded) == before and generation_of(seeded) == 0

    def test_another_databases_lock_is_a_value_error_and_nothing_is_written(self, seeded, tmp_path):
        other = resolve_config_path(str(tmp_path / "other" / "config.db"))
        other.resolved_path.parent.mkdir()
        before = names(seeded)
        with writer_lock(other) as other_held:
            with pytest.raises(ValueError, match="not the writer lock"):
                write_config(seeded, cfg("tank/wrong"), expected_generation=0, lock=other_held)
        assert names(seeded) == before and generation_of(seeded) == 0

    def test_another_threads_lock_is_a_value_error_and_nothing_is_written(self, seeded):
        ready, leave = threading.Event(), threading.Event()
        box = {}

        def holder():
            with writer_lock(seeded) as held:
                box["held"] = held
                ready.set()
                leave.wait(30)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert ready.wait(10)
            before = names(seeded)
            with pytest.raises(ValueError, match="does not hold the writer lock"):
                write_config(seeded, cfg("tank/thief"), expected_generation=0, lock=box["held"])
            assert names(seeded) == before and generation_of(seeded) == 0
        finally:
            leave.set()
            thread.join(10)

    @pytest.mark.parametrize("bad", ["x", True, 0])
    def test_a_lock_that_is_not_a_path_is_a_value_error(self, seeded, bad):
        with pytest.raises(ValueError, match="not the writer lock"):
            write_config(seeded, cfg(), expected_generation=0, lock=bad)
        assert generation_of(seeded) == 0

    def test_an_invalid_lock_is_detected_before_anything_is_opened(self, tmp_path):
        missing = resolve_config_path(str(tmp_path / "nope" / "config.db"))
        with pytest.raises(ValueError):                 # not ConfigDbNotFound
            write_config(missing, cfg(), expected_generation=None, lock=tmp_path / "x.lock")
        assert not (tmp_path / "nope").exists()

    def test_a_conflict_under_a_held_lock_leaves_it_held_and_a_retry_works(self, seeded):
        with writer_lock(seeded) as held:
            with pytest.raises(ConfigConflict) as excinfo:
                write_config(seeded, cfg("tank/late"), expected_generation=7, lock=held)
            assert (excinfo.value.expected, excinfo.value.actual) == (7, 0)
            require_writer_lock_held(seeded, held)               # untouched
            assert names(seeded) != ["tank/late"]
            write_config(seeded, cfg("tank/late"), expected_generation=0, lock=held)
        assert names(seeded) == ["tank/late"]

    def test_an_invalid_config_under_a_held_lock_leaves_it_held(self, seeded):
        bad = BackupConfig(datasets=[])
        with writer_lock(seeded) as held:
            with pytest.raises(ValueError, match="No datasets"):
                write_config(seeded, bad, expected_generation=0, lock=held)
            require_writer_lock_held(seeded, held)

    def test_the_engine_cache_is_empty_after_a_held_lock_write(self, seeded):
        with writer_lock(seeded) as held:
            read_generation(seeded)
            write_config(seeded, cfg(), expected_generation=0, lock=held)
            assert not store_db._ENGINES

    def test_the_engine_cache_is_empty_after_a_refused_held_lock_write(self, seeded):
        with writer_lock(seeded) as held:
            with pytest.raises(ConfigConflict):
                write_config(seeded, cfg(), expected_generation=5, lock=held)
            assert not store_db._ENGINES

    def test_the_schema_is_still_checked_under_a_held_lock(self, seeded, db_path):
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE alembic_version SET version_num = 'deadbeef0000'")
        conn.commit()
        conn.close()
        with writer_lock(seeded) as held:
            with pytest.raises(Exception, match="deadbeef0000|revision"):
                write_config(seeded, cfg(), expected_generation=0, lock=held)
        dispose_all()

    def test_a_multi_step_edit_sees_a_stable_generation(self, seeded):
        """The point of the pair: under the lock the generation read first is the
        one the write compares against, whatever happens in between."""
        with writer_lock(seeded) as held:
            gen = read_generation(seeded)
            # ... side effects (zfs set ...) would go here ...
            assert read_generation(seeded) == gen
            result = write_config(seeded, cfg("tank/edit"), expected_generation=gen, lock=held)
        assert result.generation == gen + 1


class TestDestinationNameWarning:
    PROBLEM_NAMES = ["bad name", "a/b", "a@b", "café", "a=b"]

    def config_with(self, destination):
        return BackupConfig(
            datasets=[DatasetConfig(name="tank/x", frequency=Duration("1h"),
                                    remote=[RemoteDatasetConfig(destination=destination)])],
            destinations={destination: Destination("http://h")},
        )

    @pytest.mark.parametrize("name", PROBLEM_NAMES)
    def test_validate_config_warns_and_does_not_refuse(self, name):
        warnings = validate_config(self.config_with(name))
        matching = [w for w in warnings if w.startswith(f"destinations[{name}]: the name cannot hold a ZFS anchor")]
        assert len(matching) == 1
        assert "anchors can never be stored for it" in matching[0]
        assert "not protected from pruning" in matching[0]
        assert destination_name_problem(name) in matching[0]

    @pytest.mark.parametrize("name", ["offsite", "dc.two", "a-b_c:d", "X1", "x" * 200])
    def test_a_usable_name_is_silent(self, name):
        assert [w for w in validate_config(self.config_with(name)) if "ZFS anchor" in w] == []

    def test_too_long(self):
        name = "x" * 300
        warned = [w for w in validate_config(self.config_with(name)) if "ZFS anchor" in w]
        assert len(warned) == 1 and "too long" in warned[0]

    def test_each_bad_destination_warns_once(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name="tank/x", frequency=Duration("1h"))],
            destinations={"bad one": Destination("http://a"), "bad/two": Destination("http://b"),
                          "good": Destination("http://c")},
        )
        warned = [w for w in validate_config(config) if "ZFS anchor" in w]
        assert len(warned) == 2

    def test_write_config_reports_it_in_the_result(self, seeded):
        config = self.config_with("bad name")
        result = write_config(seeded, config, expected_generation=0)
        assert any("destinations[bad name]" in w for w in result.warnings)
        assert result.warnings == tuple(validate_config(config))
        assert "bad name" in read_config(seeded).config.destinations      # stored regardless
        dispose_all()

    def test_import_logs_it_once(self, tmp_path, caplog):
        source = tmp_path / "in.yaml"
        source.write_text(
            'destinations:\n  "bad name":\n    url: "http://b"\n'
            'datasets:\n  - name: "tank/x"\n    remote:\n      - destination: "bad name"\n'
        )
        resolved = resolve_config_path(str(tmp_path / "store" / "config.db"))
        with caplog.at_level(logging.WARNING):
            import_yaml(source, resolved)
        warned = [r.getMessage() for r in caplog.records if "ZFS anchor" in r.getMessage()]
        assert len(warned) == 1 and "destinations[bad name]" in warned[0]

    def test_the_function_is_public_and_the_cli_uses_the_same_one(self):
        from zfsbackup.cli import dotpath

        assert destination_name_problem is store_mapper.destination_name_problem
        assert dotpath.destination_name_problem is destination_name_problem

    @pytest.mark.parametrize(
        "name, fragment",
        [
            ("", "it is empty"),
            ("a b", "it contains ' '"),
            ("a/b@c", "it contains '/' '@'"),
            ("x" * 236, "too long"),
        ],
    )
    def test_destination_name_problem_wording(self, name, fragment):
        assert fragment in destination_name_problem(name)

    def test_the_length_boundary_is_the_whole_property_name(self):
        limit = 256 - len("org.zfsbackup:anchor.")
        assert destination_name_problem("x" * limit) is None
        assert destination_name_problem("x" * (limit + 1)) is not None


class TestNeverPruneLoadWarning:
    NEVER = BackupConfig(
        datasets=[DatasetConfig(name="tank/n", frequency=Duration("1h"), retention_rules=[])]
    )

    def load_warnings(self, tmp_path, caplog):
        resolved = resolve_config_path(str(tmp_path / "s" / "config.db"))
        source = tmp_path / "forever.yaml"
        source.write_text('datasets:\n  - name: "tank/n"\n    retention: forever\n')
        import_yaml(source, resolved)
        caplog.clear()
        with open_config_session(resolved, readonly=True) as session:
            with caplog.at_level(logging.WARNING):
                load_config(session)
        return [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_it_is_the_validate_config_wording_without_a_row_id(self, tmp_path, caplog):
        records = self.load_warnings(tmp_path, caplog)
        assert len(records) == 1
        message = records[0].getMessage()
        assert message == (
            "datasets[tank/n].retention is empty ('retention: forever'): this dataset's snapshots "
            "will never be pruned and will accumulate until the pool is full"
        )
        assert "id=" not in message
        assert message in validate_config(self.NEVER)          # one wording, two places

    def test_it_comes_from_the_mapper_logger(self, tmp_path, caplog):
        assert self.load_warnings(tmp_path, caplog)[0].name.endswith("mapper")

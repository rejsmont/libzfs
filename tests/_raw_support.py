"""Shared helpers for the per-destination `raw` tests (milestone B, decision B3).

Used by `test_zfsbackup_store_raw.py` and `test_zfsbackup_cli_raw.py`: a real
database at the TLS revision (`7c3e9d51b2a8`, the head before `raw` existed),
built by the real migrations -- never by `create_all`, which would pass even if
the migration script had been left out -- holding rows in every table, and
the raw-SQL readers that look at the file without going through the mapper.

Nothing here reaches a real `zfs`.
"""

import sqlite3
from pathlib import Path

from alembic import command

from zfsbackup.config.store import dispose_all, open_config_connection, resolve_config_path
from zfsbackup.config.store import migrate as migrate_module
from zfsbackup.config.store import paths as store_paths

BASELINE_REVISION = "a1c28e1b3f34"
TLS_REVISION = "7c3e9d51b2a8"
RAW_REVISION = "5d2f8a60c1e4"          # the head

USER_VERSION = 424242                  # a distinctive store id the rebuilds must keep

#: Rows of a TLS-head database, one tuple per row, in column order of the table
#: AS IT WAS at that revision (so `dataset_remotes` has no `raw` yet).
TLS_ROWS = {
    "datasets": [(1, "tank/a", 0, 3600.0, "1h", 1), (2, "tank/b", 1, 60.0, None, 1)],
    "dataset_remotes": [
        (1, 1, "offsite", 7200.0, "2h"),
        (2, 1, "dc2", None, None),
        (3, 2, "offsite", None, None),
    ],
    # (id, dataset_id, dataset_remote_id, age_s, age_lit, keep_s, keep_lit):
    # rows 2 and 4 are per-destination overrides, scoped to remotes 1 and 3.
    "retention_rules": [
        (1, 1, None, 86400.0, "1d", 2592000.0, "30d"),
        (2, 1, 1, 3600.0, "1h", 86400.0, "1d"),
        (3, 2, None, 60.0, None, 600.0, None),
        (4, 2, 3, 120.0, "2m", 1200.0, "20m"),
    ],
    "remote_server": [(1, "tank/recv", 1)],
}


def alembic_run(resolved, direction: str, revision: str) -> list:
    """`alembic upgrade|downgrade <revision>` on the file the way
    `upgrade_schema` does it (FK enforcement off, one explicit transaction).
    Returns the `PRAGMA foreign_key_check` rows taken before the commit."""
    with open_config_connection(resolved, readonly=False, foreign_keys=False,
                                allow_new=True) as conn:
        trans = conn.begin()
        cfg = migrate_module._build_config(conn)
        getattr(command, direction)(cfg, revision)
        violations = conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
        trans.commit()
    dispose_all()
    return violations


def make_tls_head_db(tmp_path: Path):
    """A database at `7c3e9d51b2a8` with rows in every config table, a host
    identity, and a distinctive `user_version`. Returns `(resolved, db_path)`."""
    db_path = tmp_path / "tlsdb" / "config.db"
    resolved = resolve_config_path(str(db_path))
    store_paths.ensure_config_dir(db_path.parent)
    store_paths.create_config_db_file(db_path)
    assert alembic_run(resolved, "upgrade", TLS_REVISION) == []
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            "INSERT INTO global_settings (id, snapshot_prefix, check_interval_seconds, "
            "check_interval_literal, prune_interval_seconds, prune_interval_literal, "
            "api_host, api_port, dry_run, generation) "
            "VALUES (1,'autosnap',300,'5m',3600,'1h','127.0.0.1',8080,0,7)"
        )
        conn.executemany("INSERT INTO datasets VALUES (?,?,?,?,?,?)", TLS_ROWS["datasets"])
        conn.execute("INSERT INTO destinations (name, url, server_fingerprint) "
                     "VALUES ('dc2', 'https://dc2.example:8443', NULL)")
        conn.execute("INSERT INTO destinations (name, url, server_fingerprint) "
                     "VALUES ('offsite', 'https://offsite.example:8443', ?)", ("ab" * 32,))
        conn.executemany("INSERT INTO dataset_remotes VALUES (?,?,?,?,?)",
                         TLS_ROWS["dataset_remotes"])
        conn.executemany("INSERT INTO retention_rules VALUES (?,?,?,?,?,?,?)",
                         TLS_ROWS["retention_rules"])
        conn.executemany("INSERT INTO remote_server VALUES (?,?,?)", TLS_ROWS["remote_server"])
        conn.execute("INSERT INTO host_identity (id, client_id, created_at) "
                     "VALUES (1, 'host-1', 1.0)")
        conn.execute(f"PRAGMA user_version = {USER_VERSION}")
        conn.commit()
    finally:
        conn.close()
    return resolved, db_path


def q(db_path, statement: str, *params) -> list:
    """Rows of one statement against the file, read with a plain `sqlite3`
    connection (no mapper, no type coercion)."""
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(statement, params).fetchall()
    finally:
        conn.close()


def table_rows(db_path, table: str, columns: str = "*") -> list:
    return q(db_path, f"SELECT {columns} FROM {table} ORDER BY 1")


def column_names(db_path, table: str) -> list:
    return [r[1] for r in q(db_path, f"PRAGMA table_info({table})")]


def raw_cells(db_path) -> list:
    """`(destination_name, raw, typeof(raw))` for every `dataset_remotes` row,
    in dataset then destination order: the stored value and its SQLite storage
    class, which tell `NULL` (auto) from `0` (false) from `1` (true)."""
    return q(db_path, "SELECT destination_name, raw, typeof(raw) FROM dataset_remotes "
                      "ORDER BY dataset_id, id")


def drop_stamp(db_path) -> None:
    """Lose the `alembic_version` table: the tables stay, no revision is recorded."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("DROP TABLE alembic_version")
        conn.commit()
    finally:
        conn.close()


def stamp(db_path, revision: str) -> None:
    """Record `revision` the way `alembic stamp` does (replacing any row; creating
    the table if it is gone). Touches nothing else."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS alembic_version "
                     "(version_num VARCHAR(32) NOT NULL PRIMARY KEY)")
        conn.execute("DELETE FROM alembic_version")
        conn.execute("INSERT INTO alembic_version VALUES (?)", (revision,))
        conn.commit()
    finally:
        conn.close()


def make_headstamped_without_raw(tmp_path: Path):
    """The wrong-stamp damage: a TLS-revision database (no `raw` column) whose
    stamp says the head. Returns `(resolved, db_path)`."""
    resolved, db_path = make_tls_head_db(tmp_path)
    stamp(db_path, RAW_REVISION)
    return resolved, db_path


def schema_snapshot(db_path) -> list:
    """Everything the schema is, for a "nothing was changed" comparison."""
    return q(db_path, "SELECT type, name, sql FROM sqlite_master ORDER BY name")


def stub_table_ddl(name: str) -> str:
    """`CREATE TABLE` for a bare stand-in of one of the store's tables that
    still carries every column some revision ADDED to it
    (`migrate._COLUMNS_ADDED_AT_REVISION`), so a stub set of "all of ours"
    is the right shape for the unstamped-database table/column match. A name
    that is not one of ours gets just an `id`."""
    columns = ["id INTEGER PRIMARY KEY"]
    for pairs in migrate_module._COLUMNS_ADDED_AT_REVISION.values():
        columns += [column for table, column in pairs if table == name]
    return f"CREATE TABLE {name} ({', '.join(columns)})"


def drop_server_fingerprint(db_path) -> None:
    """Remove `destinations.server_fingerprint` (a column the TLS revision added)
    by rebuilding the table without it -- SQLite cannot drop a column that a
    CHECK constraint mentions. Rows, the other columns and the stamp are kept."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("CREATE TABLE destinations_rebuilt (name VARCHAR NOT NULL PRIMARY KEY, "
                     "url VARCHAR NOT NULL)")
        conn.execute("INSERT INTO destinations_rebuilt SELECT name, url FROM destinations")
        conn.execute("DROP TABLE destinations")
        conn.execute("ALTER TABLE destinations_rebuilt RENAME TO destinations")
        conn.commit()
    finally:
        conn.close()

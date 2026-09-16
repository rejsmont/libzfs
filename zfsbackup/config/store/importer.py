"""YAML -> SQLite config store import (plan item 8, phase A / 8b).

`import_yaml(yaml_path, resolved, *, create=True) -> ImportResult` is the
one supported way to bring a `BackupConfig.from_file`-shaped YAML config
into the store this package defines. It is a **destructive full replace**:
every dataset, destination, retention rule, and global setting already in
the target database is discarded and replaced by the YAML's contents (see
`mapper.save_config`'s own docstring for why wipe-and-reinsert, not
diff-and-merge, is the only sane semantics for a config with no surrogate
ids of its own).

Three phases, strictly sequential, never nested -- see `db.py`'s "Sequence
it, do not nest it" and `migrate.py`'s binding note, both measured
directly against this repo's own engine -- plus one guard in front of
them, not a fourth phase:

0. Parse the YAML (`BackupConfig.from_file`). **Nothing touches the
   filesystem target before this succeeds.** A bad YAML never creates,
   truncates, or wipes anything -- this ordering is the entire safety
   property of a destructive full replace, given item 10's pre-import
   snapshot (plan decision D-5 / 8c) is deferred.

   **The same "misuse creates nothing" property extends to a bad
   *target*, not just a bad source.** Immediately after phase 0,
   `check_config_db_suffix(resolved)` (`paths.py`) raises `ConfigDbIsYaml`
   before phase 1 runs at all, for a `.yaml`/`.yml`-suffixed `-c`. Without
   this guard, an operator typo'ing `-c config.yaml` instead of `-c
   config.db` would still get refused -- but only after phase 1 had
   already created a stray zero-byte file at that path (`check_config_db`
   itself refuses it, from inside `open_config_connection`, in phase 2 --
   too late to prevent phase 1's side effect). That stray file is
   indistinguishable from the zero-byte artifact `paths.py`'s own
   docstring describes elsewhere, so it would also confuse the NEXT
   preflight against the same path. `check_config_db_suffix` is exactly
   `check_config_db`'s own step-3 suffix decision, extracted so this
   guard and that check share one decision site and cannot drift apart --
   see its docstring for why it needs no I/O and is therefore safe to
   call before the target exists at all.
1. Bring the target file into existence (or fix its mode if it already
   exists), via `paths.py`'s `create_config_db_file`/`ensure_config_db_mode`
   -- never a raw `open()`/`os.chmod()` here. The `create=False` refusal
   (`ConfigDbNotFound`) is decided FIRST, before this phase touches
   anything, for the same "misuse creates nothing" reason as the suffix
   guard above: a `create=False` call against a missing target must not
   leave a freshly-`mkdir`'d directory behind either. The directory itself
   is only created/chmod'd by `_prepare_target_dir` on the branch that
   will actually create the file -- unlike `paths.py`'s `ensure_config_dir`
   (which chmods an existing directory unconditionally, correct for the
   one directory this package owns, `DEFAULT_CONFIG_DB.parent`), this
   module must not silently widen the permissions of an arbitrary
   operator-supplied `-c`'s containing directory just because it already
   existed -- see `_prepare_target_dir`'s own docstring for the measured
   privilege-escalation shape that guards against.
2. **Schema phase, its own connection, closed before phase 3 opens
   anything.** `open_config_connection(resolved, readonly=False,
   foreign_keys=False, allow_new=created)` (the `allow_new=True` case is
   for a database this call just created at step 1 -- it is exactly zero
   bytes until Alembic's first `CREATE TABLE`, which `check_config_db`
   would otherwise refuse as `ConfigDbNotADatabase`; see `paths.py`).
   `ensure_schema(conn)` migrates to head, and `conn.commit()` is the
   **last statement issued on this connection** -- nothing else runs on
   it afterwards. Both hazards this ordering avoids are measured, not
   theoretical:

   - Nesting this inside the phase-3 writer session self-deadlocks: the
     FK-off migration engine is a second *writer* connection on the same
     file, and `BEGIN IMMEDIATE` fires at the first statement of any
     transaction on either one -- measured, `database is locked` after a
     deterministic 5.2s `busy_timeout` stall.
   - Skipping or reordering `commit()` away from being the last statement
     leaves a split-brain database: pysqlite auto-commits each `CREATE
     TABLE` immediately (no transaction open when DDL runs), but the
     `alembic_version` stamp is the one DML statement in the sequence and
     is what actually opens a transaction -- so a missing/early commit can
     land full schema with an empty version table, and every subsequent
     `ensure_schema` call then fails forever with a bare `table datasets
     already exists`. `SchemaSplitBrain` detects that state but cannot
     undo it.
3. **Data phase, a brand new writer session, opened only after phase 2's
   connection is fully closed.** `open_config_session(resolved,
   readonly=False)`. Reads the outgoing row counts on this same session
   (before anything is wiped, so the WARNING it logs is accurate), then
   `save_config(session, config)`. `session_for_engine` commits exactly
   once, on clean exit; any exception rolls the whole phase back, leaving
   whatever was there before this call intact. Duration literals survive:
   `save_config` writes `(total_seconds, literal)` pairs, so `"1M"` in the
   YAML round-trips as `"1M"`, not a re-synthesized `"30d"` -- verified
   against `zfsbackup/config.example.yaml`.

**Never route through `DatasetConfig.from_dict` or `.from_property`.**
See `mapper.py`'s module docstring, invariant 3 -- this module calls
`BackupConfig.from_file` exactly once, at phase 0, and every dataclass
downstream of that point is already a `BackupConfig`; nothing here
re-parses anything.

This module has exactly one caller of `check_config_db`'s `allow_new`
escape hatch (via `open_config_connection`, phase 2 above) in the entire
codebase. It must stay that way -- `allow_new` must never be reachable
from the daemon; see `paths.py`'s docstring.

8d -- interim CLI
------------------
`python -m zfsbackup.config.store.importer <yaml> [-c PATH]` (the `_main`
function below) is a deliberately throwaway ~25-line argparse wrapper.
**Item 10 deletes it** and replaces it with `zfsbackup-config import`, an
operator-facing CLI entry point (`zfsbackup/cli/`, not owned by this
package) that must call `import_yaml` directly -- exactly as this
function does -- rather than reimplementing any of its phases. This
exists only so the item-8 commit does not leave a tree with a daemon that
refuses to start (per item 6a's hard YAML refusal) and no way at all to
make it a database, which would break `scenarios/test_two_vm_backup.sh`
and the smoke commands in `CLAUDE.md`.
"""

from __future__ import annotations

import argparse
import logging
import os
import stat as stat_module
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

import yaml
from sqlalchemy import select

from zfsbackup.config import BackupConfig
from zfsbackup.config.store.mapper import save_config
from zfsbackup.config.store.migrate import SchemaError, ensure_schema
from zfsbackup.config.store.models import Dataset, Destination
from zfsbackup.config.store.paths import (
    CONFIG_DIR_MODE,
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    ConfigDbNotFound,
    ConfigPathError,
    ResolvedConfigPath,
    check_config_db_suffix,
    create_config_db_file,
    ensure_config_db_mode,
    ensure_config_dir,
    open_config_connection,
    open_config_session,
    resolve_config_path,
)

logger = logging.getLogger(__name__)

__all__ = ["ImportResult", "import_yaml"]


@dataclass(frozen=True)
class ImportResult:
    """The outcome of one `import_yaml` call.

    `created` is True only when phase 1 had to create the target file
    itself (a brand-new database); False for a re-import over an existing
    one. `head_revision` is the Alembic head the schema phase left the
    database at. `replaced_datasets`/`replaced_destinations` are the row
    counts phase 3 observed BEFORE wiping them -- both are 0 for a
    brand-new (`created=True`) database.
    """

    created: bool
    head_revision: str
    replaced_datasets: int
    replaced_destinations: int


def _target_exists(path: Path) -> bool:
    """`os.stat(path)`-based existence check, so a symlink to a real
    target reads as "exists" the same way `check_config_db` itself
    decides existence (`paths.py`'s step 1) -- never `Path.exists()`,
    which folds every `OSError` (not just `ENOENT`) into `False`. Any
    `OSError` other than "missing" is a real problem this function is not
    positioned to diagnose (that is `check_config_db`'s job, reached a few
    lines later via `open_config_connection`'s own preflight), so it is
    left to propagate rather than guessed at here.
    """
    try:
        os.stat(path)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False


def _prepare_target_dir(directory: Path) -> None:
    """Create `directory` (at `CONFIG_DIR_MODE`) if it does not exist;
    otherwise touch nothing but a log line.

    `paths.py`'s `ensure_config_dir` chmods **unconditionally**, on
    purpose -- correct for the one directory this package actually owns,
    `DEFAULT_CONFIG_DB.parent` (`/var/lib/zfsbackup`), where "fix a stale
    mode on re-import" is exactly the intended behaviour its own
    docstring describes. It is the wrong behaviour for an arbitrary
    operator-supplied `-c` path: measured, `python -m zfsbackup.config.
    store.importer config.yaml -c ~/x.db` against an existing `$HOME`
    (mode `0755`) silently widened it to `0770` -- a privilege escalation
    (group-writable `$HOME`, on many systems a *shared* group like
    `staff`/`users`) and a world-read drop, from a command whose stated
    job is "import a YAML file".

    So: the owned default directory is always created/fixed, matching
    `ensure_config_dir`'s own re-import-friendly intent (this call did
    not necessarily create it, but this package DOES own it). Anything
    else is only created (and therefore only chmod'd) when it does not
    already exist -- this call is then the one bringing it into being, so
    setting its mode is this call's own business. An existing, non-default
    directory is left untouched; if its mode does not already satisfy
    what a WAL sidecar needs, this logs a WARNING naming the actual vs.
    target mode instead of silently changing permissions on a directory
    this call did not create -- `check_config_db` (reached a few lines
    later, via `open_config_connection`) is what actually enforces
    writability, and gives the operator-actionable failure if the
    mismatch turns out to matter.
    """
    if directory == DEFAULT_CONFIG_DB.parent or not _target_exists(directory):
        ensure_config_dir(directory)
        return

    try:
        actual_mode = stat_module.S_IMODE(os.stat(directory).st_mode)
    except OSError:
        # Let the real preflight (`check_config_db`, reached a few lines
        # later) diagnose this properly -- it already turns a stat
        # failure into a named ConfigPathError subclass.
        return
    if actual_mode != CONFIG_DIR_MODE:
        logger.warning(
            "%s already exists (mode %04o) and was not created by this "
            "import -- leaving its permissions unchanged rather than "
            "widening them. A WAL config database needs %04o (group-write "
            "and group-search) on its containing directory to (re)create "
            "its -wal/-shm sidecars; fix this yourself if the import "
            "below, or a later daemon start, fails because of it.",
            directory, actual_mode, CONFIG_DIR_MODE,
        )


def import_yaml(
    yaml_path: Union[str, Path],
    resolved: ResolvedConfigPath,
    *,
    create: bool = True,
) -> ImportResult:
    """Import `yaml_path` (a `BackupConfig.from_file`-shaped YAML config)
    into the config database `resolved` addresses, replacing its entire
    contents. See this module's docstring for the full phase breakdown
    and the two nesting/commit hazards phases 2 and 3 avoid.

    `create=True` (the default): create the target file (and its
    containing directory, at `CONFIG_DIR_MODE`) if nothing exists there
    yet. `create=False`: require an existing target and raise
    `ConfigDbNotFound` if there is none -- for a caller that wants
    "re-import only", never "import or create".
    """
    # Phase 0 -- parse first. Nothing below this line runs if the YAML
    # itself is invalid, so a bad YAML never touches the filesystem
    # target at all.
    config = BackupConfig.from_file(Path(yaml_path))

    # Guard, ahead of phase 1: a `.yaml`/`.yml`-suffixed TARGET is the
    # same class of misuse as a bad source YAML, just reached the other
    # way around -- an operator typo'ing `-c config.yaml` instead of
    # `-c config.db` must get refused before anything is created, not
    # after phase 1 has already left a stray zero-byte file at the path
    # they will likely retry (which would then look exactly like the
    # zero-byte artifact `check_config_db`'s own docstring describes,
    # confusing the NEXT preflight too). `check_config_db_suffix` is the
    # same decision `check_config_db`'s step 3 makes -- see its docstring
    # for why this is safe to call before the target exists, and why it
    # is not a duplicate check: it is the one place both this guard and
    # `check_config_db` call, so they cannot drift apart. Phases 1-3 keep
    # their exact sequence below; this is a guard in front of them, not a
    # change to their order.
    check_config_db_suffix(resolved)

    # Phase 1 -- bring the target file into existence, or fix its mode.
    # The create=False refusal is decided FIRST, before anything below it
    # touches the filesystem: a `create=False` call against a missing
    # target (and a missing containing directory) must leave no directory
    # behind either -- the same "misuse creates nothing" property the
    # suffix guard above gives for a `.yaml` target. Measured before this
    # ordering: `create=False` against `<tmp>/nodir/config.db` raised
    # `ConfigDbNotFound` correctly but still left `<tmp>/nodir` created at
    # `0770` -- `ensure_config_dir` had already run on the branch that was
    # about to fail anyway.
    target = resolved.resolved_path
    existing = _target_exists(target)

    if not existing and not create:
        raise ConfigDbNotFound(
            f"No config database exists at {target}, and create=False was "
            "passed to import_yaml() -- this call only replaces an "
            "EXISTING database. Pass create=True (the default) to create "
            "one, or point at an existing database to re-import over it."
        )

    # Only the branch that will actually create/touch the target may also
    # create/touch its containing directory -- see _prepare_target_dir's
    # own docstring for why that call is gated the way it is.
    _prepare_target_dir(target.parent)

    if existing:
        ensure_config_db_mode(target)
        created = False
    else:
        create_config_db_file(target)
        created = True

    # Phase 2 -- schema, its own connection, committed and closed before
    # phase 3 opens anything. `allow_new=created`: only a database THIS
    # call just created is the zero-byte file `check_config_db` would
    # otherwise refuse; an existing target re-imported over is already a
    # real SQLite file and must still be refused if it is not one.
    with open_config_connection(
        resolved, readonly=False, foreign_keys=False, allow_new=created
    ) as conn:
        head_revision = ensure_schema(conn)
        # LAST statement on this connection. See module docstring.
        conn.commit()

    # Phase 3 -- data, in a brand-new writer session opened only now that
    # phase 2's connection has fully closed.
    with open_config_session(resolved, readonly=False) as session:
        replaced_datasets = len(session.scalars(select(Dataset)).all())
        replaced_destinations = len(session.scalars(select(Destination)).all())
        if replaced_datasets or replaced_destinations:
            logger.warning(
                "Replacing existing config store at %s: %d dataset(s) and "
                "%d destination(s) will be discarded and replaced by the "
                "contents of %s",
                target, replaced_datasets, replaced_destinations, yaml_path,
            )
        save_config(session, config)

    return ImportResult(
        created=created,
        head_revision=head_revision,
        replaced_datasets=replaced_datasets,
        replaced_destinations=replaced_destinations,
    )


def _main(argv: Optional[List[str]] = None) -> int:
    """See this module's "8d -- interim CLI" docstring section."""
    parser = argparse.ArgumentParser(
        prog="python -m zfsbackup.config.store.importer",
        description="INTERIM tool, deleted by item 10 -- import a YAML "
        "zfsbackup config into the SQLite config store.",
    )
    parser.add_argument("yaml_path", type=Path, help="YAML config to import.")
    # Deliberately NOT type=Path: str(Path("")) == ".", which would turn
    # `-c ""` into "the current directory" and defeat resolve_config_path's
    # documented fallthrough to ZFSBACKUP_CONFIG. See paths.py's F3.
    parser.add_argument(
        "-c", "--config", default=None,
        help=f"Config database path (default: ${CONFIG_PATH_ENV}, or {DEFAULT_CONFIG_DB}).",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    try:
        resolved = resolve_config_path(args.config)
        result = import_yaml(args.yaml_path, resolved)
    except (
        ConfigPathError,
        SchemaError,
        ValueError,
        TypeError,
        FileNotFoundError,
        yaml.YAMLError,
    ) as exc:
        # ValueError/TypeError/yaml.YAMLError: BackupConfig.from_file's own
        # failure modes for a malformed YAML -- a bad parse
        # (yaml.YAMLError), a value of the wrong shape rejected by
        # config/model.py's own guards (ValueError), or something coerced
        # with the wrong type, e.g. `int(api_port)` against a mapping/list
        # (TypeError). A malformed YAML is *the* likely error for an
        # import tool, so these must produce this tool's own clean
        # "error: ..." line, not a bare traceback.
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Imported {args.yaml_path} into {resolved.resolved_path}: "
        f"created={result.created} head={result.head_revision} "
        f"replaced {result.replaced_datasets} dataset(s), "
        f"{result.replaced_destinations} destination(s)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(_main())

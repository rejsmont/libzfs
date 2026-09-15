"""Converts between the ORM rows in `zfsbackup/store/models.py` and the
plain `@dataclass` config model in `zfsbackup/config.py`.

See `zfsbackup/store/__init__.py` for why the two are kept as separate
layers rather than one declarative model; this module is the "later item"
that docstring pointed at. It is a NEW coupling: `config.py` does not import
anything from `store`, but this module (and therefore the `store` package)
now imports `zfsbackup.config`. That direction is deliberate and safe --
`config.py` has no reason to know the store exists -- and should stay
one-directional.

Three invariants hold across every function here:

1. **`*_seconds` is authoritative for VALUE; `*_literal` is authoritative for
   TEXT only, and only when it agrees with `*_seconds`.** A DB row's stored
   literal can disagree with its stored seconds value (hand-edited row,
   future migration, bit rot); when it does, the seconds value wins and the
   literal is discarded with a WARNING log rather than silently trusted.
   See `_duration_from_row`.
2. **`Duration(d.literal) == d` must hold for every `Duration` this module
   produces**, whenever `d.literal is not None`. This is what rules out
   force-attaching a disagreeing stored literal via `Duration.__setstate__`
   (which would "work" but produce a `Duration` whose `.render()` describes
   a different value than the one it holds -- every downstream YAML render
   would then emit a config that reloads to a different schedule).
3. **Neither `DatasetConfig.from_dict` nor `DatasetConfig.from_property` is
   the DB->dataclass path, and both are actively wrong if reused as one:**
   `from_dict` is a YAML-boundary parser with a Phase-0 type guard
   (`config.py`'s `_duration_from_config`) that *rejects* any duration value
   that is not already a `str` -- exactly what every column in this store
   is not (`*_seconds` is a `float`, and a DB-sourced `Duration` is a typed
   object, not YAML text). Round-tripping a DB row through a YAML-shaped
   dict of literal strings does not fix this either: a NULL `*_literal` has
   no string form, and it would re-derive the value from the literal rather
   than the authoritative `*_seconds`, inverting invariant 1 above.
   `from_property` is a *wire* decoder for the `org.zfsbackup:config`
   ZFS user property, not a round-trip inverse of anything -- it drops
   per-destination retention rules entirely, and `config.py` says so
   in its own docstring, naming this exact use as something not to do.
   Every dataclass constructed here therefore uses direct keyword
   construction with already-typed `Duration`s, matching `from_property`'s
   *style* (direct construction) without reusing its *behaviour* (lossy
   wire decoding).

`load_config` replicates `BackupConfig.from_file`'s defaulting behaviour
exactly for everything the schema cannot itself default (see the table in
the docstring of `load_config` below) -- but NOT for `prune_interval`
falling back to `check_interval`, and NOT for the `client_id_file` default.
Both `GlobalSettings.prune_interval_seconds` and `.client_id_file` are
nullable specifically so that "derive this at load time" is representable
in the DB as `NULL` rather than flattened into a stored value by whichever
process happens to write it (a `zfsbackup-config import` run under `sudo`
freezing `/root/.config/...` into the DB was the motivating bug -- see
`models.py`'s module docstring and `GlobalSettings`). This module therefore
passes a `NULL` row straight through as `None` on both `BackupConfig`
fields rather than deriving a value here: `BackupConfig.effective_prune_interval`
and `.effective_client_id_file` (`config.py`) own the actual
`check_interval`/`$HOME` derivation, resolved lazily on each access in
whatever process calls them, not this mapper. `load_config` must not
reimplement that derivation itself: doing so would re-flatten the very
value this item made representable, one layer earlier than the accessor
that now owns it.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
    RemoteServerConfig,
    RetentionRule,
    validate_retention_uniqueness,
)
from zfsbackup.store import models

logger = logging.getLogger(__name__)


# Dataset-level default applied when a dataset has zero dataset-level
# retention rows -- mirrors `DatasetConfig.from_dict`'s default
# (`config.py:552-572`), which means a `BackupConfig` produced by
# `from_file` can never have an empty dataset-level retention set. Built as
# `Duration` literals (not bare `timedelta`s) so the literal matches
# `from_file`'s exactly ("1d"/"30d", not a re-synthesized equivalent).
_DEFAULT_RETENTION_AGE = Duration("1d")
_DEFAULT_RETENTION_KEEP_FOR = Duration("30d")


def _duration_from_row(seconds: float, literal: Optional[str], key: str) -> Duration:
    """Build a `Duration` from a `(*_seconds, *_literal)` column pair.

    `seconds` (a Python `float`, as SQLite's REAL affinity yields) is
    authoritative for value; `literal` is authoritative for text only, and
    only when `Duration(literal).total_seconds() == seconds`. See the module
    docstring's invariant 1/2 for why a disagreeing literal is discarded
    (with a WARNING naming `key`) rather than force-attached.

    `literal is None` re-synthesizes a best-effort literal from `seconds`
    (`Duration._synthesize`) -- deterministic, so nothing is lost, and it is
    `None` again only for the sub-second values the grammar cannot express
    (see `models.py`'s module docstring).
    """
    if literal is None:
        return Duration(seconds=seconds)

    try:
        candidate = Duration(literal)
    except ValueError:
        logger.warning(
            "%s: stored literal %r is not a parseable duration; using the "
            "stored seconds value %.0fs instead",
            key, literal, seconds,
        )
        return Duration(seconds=seconds)

    if candidate.total_seconds() == seconds:
        return candidate

    logger.warning(
        "%s: stored literal %r (%.0fs) disagrees with the stored seconds "
        "value (%.0fs); using the seconds value and discarding the literal",
        key, literal, candidate.total_seconds(), seconds,
    )
    return Duration(seconds=seconds)


def _rule_to_dataclass(row: models.RetentionRule) -> RetentionRule:
    """Map one `RetentionRule` row to `config.RetentionRule`, independent of
    scope -- the caller (`_dataset_level_rules`/`_scoped_rules`) is what
    filters by scope; this just converts one already-selected row.
    """
    return RetentionRule(
        age=_duration_from_row(
            row.age_seconds, row.age_literal, f"retention_rules[id={row.id}].age"
        ),
        keep_for=_duration_from_row(
            row.keep_for_seconds, row.keep_for_literal,
            f"retention_rules[id={row.id}].keep_for",
        ),
    )


def _dataset_level_rules(ds: models.Dataset) -> List[RetentionRule]:
    """The dataset-level (local pruning) retention set for `ds`.

    THE TRAP (see module docstring on `store/__init__.py` and this file's
    own module docstring): `ds.retention_rules` returns BOTH dataset-level
    rows (`dataset_remote_id IS NULL`) and every per-destination override
    for this dataset mixed together, because the relationship joins on
    `dataset_id` alone. Filtering `dataset_remote_id is None` here is not
    optional -- omitting it silently promotes per-destination overrides
    into local pruning, with no error and no log.

    Zero dataset-level rows is ambiguous in principle (a config a user
    actually wants with no local retention at all is not representable
    here) but `from_dict` can never produce it -- an absent or empty
    `retention:` mapping defaults to `{'1d': '30d'}` before it ever reaches
    a `RetentionRule`. Mirror that default here too (rather than raising or
    passing an empty list through), logging a WARNING naming the dataset so
    a zero-row dataset (e.g. one written directly against the DB, bypassing
    `save_config`) is visible rather than silently defaulted.
    """
    rules = [
        _rule_to_dataclass(r) for r in ds.retention_rules if r.dataset_remote_id is None
    ]
    rules.sort(key=lambda r: r.age)
    if not rules:
        logger.warning(
            "dataset %r (id=%s) has zero dataset-level retention rules; "
            "applying the default 1d -> 30d tier",
            ds.name, ds.id,
        )
        return [RetentionRule(age=_DEFAULT_RETENTION_AGE, keep_for=_DEFAULT_RETENTION_KEEP_FOR)]
    return rules


def _scoped_rules(remote: models.DatasetRemote) -> List[RetentionRule]:
    """The per-destination override retention set for `remote`.

    `remote.retention_rules` is already correctly scoped -- its
    relationship's `primaryjoin` is narrowed to `DatasetRemote.id ==
    RetentionRule.dataset_remote_id` (`models.py`), so unlike
    `Dataset.retention_rules` there is no promotion trap here. Zero rows
    means "inherit the dataset-level set" (`config.py:414-416`,
    `effective_retention_rules`) and is returned as `[]` UNCHANGED -- this
    is deliberately the opposite of `_dataset_level_rules`' empty-set
    handling; see that function's docstring for why the asymmetry is
    correct and must not be unified.
    """
    rules = [_rule_to_dataclass(r) for r in remote.retention_rules]
    rules.sort(key=lambda r: r.age)
    return rules


def _assert_scope_integrity(ds: models.Dataset) -> None:
    """Raise if any of `ds`'s retention rows disagrees with itself about
    which dataset and which remote it is scoped to.

    The composite FK (`fk_retention_rules_dataset_remote`, `models.py`)
    normally makes this impossible -- but only when `PRAGMA
    foreign_keys=ON`, which is per-connection in SQLite and off by default.
    Since item 5, `zfsbackup/store/db.py` sets that pragma on every
    connection **it** creates. **This check stays regardless, and must not
    be deleted as obsolete**, for two independent reasons:

    - Not every connection comes from `db.py`. Alembic's SQLite batch
      migrations recreate tables and must run with FK enforcement OFF
      (`db.py`'s `foreign_keys=False` opt-out exists for exactly that);
      the `sqlite3` CLI, a hand-rolled `create_engine` in a fixture, or a
      restored/hand-edited database file are all FK-off paths into these
      same rows.
    - The second loop below catches a case **no FK covers at all** -- see
      the paragraph on `DatasetRemote.retention_rules`: a row whose
      `dataset_remote_id` is perfectly valid but whose `dataset_id` names a
      different or nonexistent dataset satisfies every FK in the schema
      while appearing in no dataset's `.retention_rules` collection.

    Without this check, a row like that would silently vanish from dataset
    A's config and reappear scoped under dataset B's -- a retention change
    with no error and no log, exactly the failure class the FK exists to
    prevent structurally. This is the belt to that braces.

    Two independent joins have to be checked, not one, because
    `Dataset.retention_rules` and `DatasetRemote.retention_rules` filter on
    different halves of the composite key and each is blind to the other:

    - `Dataset.retention_rules` (`models.py`) joins on `dataset_id` alone.
      A row with a `dataset_remote_id` pointing outside `ds.remotes` is
      reachable here but scoped to the wrong remote (or a remote of a
      different dataset entirely) -- checked by the first loop below.
    - `DatasetRemote.retention_rules` (`models.py`) joins on
      `dataset_remote_id` alone, via a `primaryjoin` narrowed to
      `DatasetRemote.id == RetentionRule.dataset_remote_id`, and never
      re-checks `RetentionRule.dataset_id`. A row whose `dataset_remote_id`
      correctly names one of `ds`'s remotes but whose `dataset_id` names a
      DIFFERENT (or nonexistent) dataset is therefore invisible to the
      first loop -- it never appears in ANY dataset's `.retention_rules`,
      so `_dataset_to_dataclass` would otherwise ingest it silently as a
      genuine override of `ds`. The second loop below walks that other
      direction explicitly to close the gap.
    """
    own_remote_ids = {r.id for r in ds.remotes}
    for rule in ds.retention_rules:
        if rule.dataset_remote_id is not None and rule.dataset_remote_id not in own_remote_ids:
            remote = rule.dataset_remote
            actual_dataset_id = remote.dataset_id if remote is not None else None
            raise ValueError(
                f"dataset {ds.name!r} (id={ds.id}): retention_rules row "
                f"id={rule.id} has dataset_remote_id={rule.dataset_remote_id}, "
                f"which belongs to dataset_id={actual_dataset_id!r}, not this "
                f"dataset's id ({ds.id})"
            )

    for remote in ds.remotes:
        for rule in remote.retention_rules:
            if rule.dataset_id != ds.id:
                raise ValueError(
                    f"dataset {ds.name!r} (id={ds.id}): retention_rules row "
                    f"id={rule.id} is scoped to this dataset's remote "
                    f"(dataset_remote_id={remote.id}, destination "
                    f"{remote.destination_name!r}) but has "
                    f"dataset_id={rule.dataset_id!r}, not this dataset's id "
                    f"({ds.id})"
                )


def _dataset_to_dataclass(
    ds: models.Dataset, known_destinations: Dict[str, Destination]
) -> DatasetConfig:
    """Map one `Dataset` row (with its `retention_rules` and `remotes`
    relationships) to a fully detached `config.DatasetConfig`.

    `known_destinations` is the already-loaded `{name: Destination}` map
    from the whole `destinations` table (see `load_config`) -- used only to
    re-check that every `DatasetRemote.destination_name` is declared,
    mirroring `config.py:715-728`'s load-time check. The FK on
    `destination_name` normally makes an undeclared reference impossible,
    but (as with `_assert_scope_integrity`) that guarantee is conditional on
    `PRAGMA foreign_keys=ON`. Item 5's `zfsbackup/store/db.py` now sets that
    pragma on every connection it creates, and **this re-check still stays**:
    connections that do not come from `db.py` (Alembic batch migrations,
    which must run FK-off; the `sqlite3` CLI; a fixture building its own
    engine) can leave rows behind that no FK ever vetted, and this is the
    only place a `BackupConfig` built from them would otherwise acquire a
    `remote` entry pointing at a destination it does not contain.
    """
    _assert_scope_integrity(ds)

    frequency = _duration_from_row(
        ds.frequency_seconds, ds.frequency_literal, f"datasets[{ds.name}].frequency"
    )

    remote_configs: List[RemoteDatasetConfig] = []
    for remote in sorted(ds.remotes, key=lambda r: r.id):
        if remote.destination_name not in known_destinations:
            raise ValueError(
                f"datasets[{ds.name}].remote[{remote.destination_name}] "
                f"references destination '{remote.destination_name}', which "
                "is not declared in 'destinations'"
            )
        # NULL frequency_seconds means "inherit the dataset's own
        # frequency" (`RemoteDatasetConfig.frequency = None`,
        # `models.py:220-226`) -- disambiguated from the sub-second
        # "re-synthesize on read" NULL-literal case by checking
        # frequency_seconds itself, not frequency_literal.
        remote_frequency = (
            None if remote.frequency_seconds is None
            else _duration_from_row(
                remote.frequency_seconds, remote.frequency_literal,
                f"datasets[{ds.name}].remote[{remote.destination_name}].frequency",
            )
        )
        remote_configs.append(RemoteDatasetConfig(
            destination=remote.destination_name,
            frequency=remote_frequency,
            retention_rules=_scoped_rules(remote),
        ))

    return DatasetConfig(
        name=ds.name,
        # SQLite can hand back 0/1 ints for boolean columns depending on
        # path; coerce so `==` against `from_file`'s real `bool` holds.
        recursive=bool(ds.recursive),
        frequency=frequency,
        retention_rules=_dataset_level_rules(ds),
        enabled=bool(ds.enabled),
        remote=remote_configs,
    )


def load_config(session: Session) -> BackupConfig:
    """Build a fully detached `BackupConfig` from the config store.

    Replicates `BackupConfig.from_file`'s defaulting behaviour exactly for
    everything the schema itself cannot default -- see the module
    docstring for the two fields (`prune_interval`, `client_id_file`) that
    are deliberately NOT defaulted/derived here even though `from_file`
    would: `GlobalSettings` makes both columns nullable specifically so a
    DB row can represent "derive this elsewhere", and this function passes
    that `NULL` through as `BackupConfig.prune_interval is None` /
    `.client_id_file is None` rather than resolving it.

    | `from_file` default/behaviour | Who owns it on the DB path |
    |---|---|
    | `snapshot_prefix="autosnap"` | Schema `default=` |
    | `check_interval="5m"` | Writer (`NOT NULL`); this function reads |
    | `prune_interval` <- `check_interval` | NOT reimplemented; NULL row yields `None` |
    | `api_host`/`api_port`/`dry_run` | Schema `default=` |
    | `destinations={}` | Emergent -- empty table yields `{}` |
    | `remote_backup=None` | This function -- absent `RemoteServer` row |
    | `client_id_file` default | NOT reimplemented; NULL row yields `None` |
    | "No datasets configured" | This function |
    | retention `{'1d': '30d'}` default | `_dataset_level_rules` |
    | retention sorted by age | `_dataset_level_rules`/`_scoped_rules` |
    | undeclared destination rejected | `_dataset_to_dataclass` (belt to the FK) |

    Every dataclass is built eagerly, before this function returns, so the
    result is safe to hold for the life of a `BackupDaemon` well past this
    session's lifetime -- no lazy-loaded ORM attribute is ever touched
    after `load_config` returns (see `store/__init__.py`'s
    `DetachedInstanceError` rationale).

    `validate_retention_uniqueness` and `_collapse_retention_rules`
    (`config.py`) are deliberately NOT called here: the former has zero
    production callers on the load path today (`config.py:573-577`), and
    the latter is a pruning-time function, not a load-path one -- see this
    module's `save_config` for where uniqueness validation belongs instead.
    """
    gs = session.get(models.GlobalSettings, 1)
    if gs is None:
        raise ValueError(
            "config store is uninitialized: no GlobalSettings row (id=1) "
            "found; run 'zfsbackup-config import' to populate it"
        )

    destination_rows = session.scalars(select(models.Destination)).all()
    # ALL declared destinations, not just ones referenced by a dataset --
    # `BackupConfig.destinations` is every declared destination
    # (`config.py:705-713`), so an unreferenced one must still round-trip.
    destinations: Dict[str, Destination] = {
        row.name: Destination(url=row.url) for row in destination_rows
    }

    dataset_rows = session.scalars(
        select(models.Dataset).order_by(models.Dataset.id)
    ).all()
    if not dataset_rows:
        raise ValueError("No datasets configured")

    datasets = [_dataset_to_dataclass(ds, destinations) for ds in dataset_rows]

    remote_server = session.get(models.RemoteServer, 1)
    remote_backup = (
        None if remote_server is None
        else RemoteServerConfig(
            target_dataset=remote_server.target_dataset,
            enabled=bool(remote_server.enabled),
        )
    )

    return BackupConfig(
        datasets=datasets,
        snapshot_prefix=gs.snapshot_prefix,
        check_interval=_duration_from_row(
            gs.check_interval_seconds, gs.check_interval_literal, "check_interval"
        ),
        # NULL prune_interval_seconds means "derive from check_interval at
        # read time" (models.py's module docstring) -- pass None straight
        # through rather than calling _duration_from_row, which would reach
        # Duration(seconds=None) and raise. The CHECK constraint in
        # GlobalSettings guarantees prune_interval_literal is also NULL
        # whenever prune_interval_seconds is, so there is no literal to lose
        # here.
        prune_interval=(
            None if gs.prune_interval_seconds is None
            else _duration_from_row(
                gs.prune_interval_seconds, gs.prune_interval_literal, "prune_interval"
            )
        ),
        api_host=gs.api_host,
        api_port=gs.api_port,
        dry_run=bool(gs.dry_run),
        destinations=destinations,
        remote_backup=remote_backup,
        # NULL means "resolve $HOME's default in the process that uses the
        # file" -- see models.py's GlobalSettings docstring. Pass None
        # through rather than resolving it here.
        client_id_file=(
            None if gs.client_id_file is None else Path(gs.client_id_file)
        ),
    )


def _assert_positive_duration_rule(rule: RetentionRule, key: str) -> None:
    """Reject a non-positive `age`/`keep_for` before it reaches the DB.

    Mirrors `config.py`'s `_positive_duration_from_config` (age 0 is a
    division-by-zero in the slot-based retention algorithm,
    `DatasetManager.needs_prunning`; either duration <= 0 has no meaningful
    retention semantics) -- but that guard runs only on the YAML-boundary
    path, `DatasetConfig.from_dict`. A `BackupConfig` built directly
    (bypassing `from_dict`, as a hand-built config or the item 8 CLI
    importer might) never passes through it, so without this check the
    DB's `CHECK(age_seconds > 0)`/`CHECK(keep_for_seconds > 0)` constraints
    (`models.py`) are the only thing that would catch it -- as a bare
    `CHECK constraint failed` raised mid-insert, with the wipe already
    flushed.
    """
    if rule.age.total_seconds() <= 0:
        raise ValueError(
            f"{key}: age must be positive, got {rule.age.total_seconds():.0f}s"
        )
    if rule.keep_for.total_seconds() <= 0:
        raise ValueError(
            f"{key}: keep_for must be positive, got "
            f"{rule.keep_for.total_seconds():.0f}s"
        )


def _dedupe_exact_duplicates(rules: List[RetentionRule]) -> List[RetentionRule]:
    """Drop rules that are exact duplicates (same age AND same keep_for),
    preserving first-seen order.

    `validate_retention_uniqueness` deliberately PERMITS exact duplicates
    (`config.py:300-302`, `test_exact_duplicates_are_silent`), but the DB's
    unique indexes on `age_seconds`/`keep_for_seconds` per scope do not --
    inserting two identical rules raises a bare `IntegrityError`. This is
    the gap between what validation tolerates and what the schema accepts;
    `save_config` must close it before insert, not just validate.
    """
    seen = set()
    deduped = []
    for rule in rules:
        key = (rule.age.total_seconds(), rule.keep_for.total_seconds())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rule)
    return deduped


def save_config(session: Session, config: BackupConfig) -> None:
    """Replace the entire config store with `config`'s contents.

    Wipe-and-reinsert, not diff-and-merge: diffing would have to reconcile
    surrogate ids across a config with none of its own, and every
    reconciliation error would be a silent config change. Full replace has
    exactly one failure mode -- all or nothing.

    Does NOT `commit()` or `begin()` -- operates in the caller's existing
    session/transaction. This keeps `save_config` out of transaction policy
    (item 8's "one transaction" requirement depends on that) and is why
    every validation that must run before the first `DELETE` is written
    that way explicitly below: once a `DELETE` has been flushed there is no
    undoing it short of the caller rolling back the whole session.

    Steps, in order (see `models.py` for why the delete order and flush
    points are load-bearing, not stylistic):

    1. Validate, all of it BEFORE any DELETE runs, so a bad input never
       wipes a good DB: `config.datasets` must be non-empty
       (`config.py:667-671`'s "No datasets configured", mirrored here so
       the store and the YAML loader agree on what a valid config is);
       dataset names must be unique (`config.py:677-688`'s check, which
       exists in `from_file` specifically because the store's
       `datasets.name UNIQUE` constraint is stricter than `from_dict`);
       every declared destination must have a non-empty `url`
       (`config.py:710-712`'s check -- `destinations.url` is `NOT NULL`
       but not `CHECK`'d non-empty, so a `None` OR `""` url would
       otherwise reach the DB, the former as a bare `NOT NULL constraint
       failed`, the latter inserting silently); `config.remote_backup`, if
       present, must have a non-empty `target_dataset`
       (`config.py:735-737`'s check, same reasoning as `url` above); within
       each dataset, destinations referenced by `remote` entries must be
       unique per dataset (`dataset_remotes`'s composite unique constraint,
       `models.py`, is stricter than `from_dict` -- see `config.py:624-626`,
       which never checked this) and each must be a key of
       `config.destinations`; and every retention rule's `age`/
       `keep_for`, dataset-level and per-destination, must be positive
       (`_assert_positive_duration_rule`, mirroring
       `config.py`'s `_positive_duration_from_config`) plus
       `validate_retention_uniqueness` per scope. Skipping any of these
       does not fail loudly -- it fails as a bare `IntegrityError`/`CHECK
       constraint failed` raised mid-insert, well after the wipe.

       **None of step 1 is FK-related**, so item 5's `store/db.py` (which
       enables `PRAGMA foreign_keys=ON` on every connection it creates)
       makes none of it redundant: these checks run *before* any DELETE,
       against in-memory dataclasses, precisely so a rejected config never
       reaches the point where a constraint -- FK or otherwise -- could fire
       at all. The whole value is in failing before the wipe, which no
       DB-side constraint can do.
    2. Drop exact `(age, keep_for)` duplicates per scope (see
       `_dedupe_exact_duplicates`) -- the DB rejects what step 1 permits.
    3. Read the current `GlobalSettings.generation` (default -1 if the
       table is empty/absent) so the fresh row below is `previous + 1`, not
       reset to 0 -- items 15/17 depend on `generation` as a
       compare-and-swap token surviving a save.
    4. Delete in order **retention_rules -> dataset_remotes -> datasets ->
       destinations -> global_settings -> remote_server**, flushing between
       each. This is a superset of the FK-implied order and does NOT rely
       on `ON DELETE CASCADE` at all: an ORM-enabled bulk `delete()` never
       applies relationship cascades (only the DB's `ON DELETE CASCADE`
       does that, and only when `PRAGMA foreign_keys=ON`, which is
       per-connection and off by default; since item 5, `store/db.py` sets
       it on every connection *it* creates, but Alembic batch migrations
       deliberately run FK-off, and the `sqlite3` CLI and hand-built
       engines never set it -- so this module still assumes nothing about
       it). Deleting only
       `datasets`/`destinations` with the pragma off leaves
       `dataset_remotes` and `retention_rules` rows behind with a
       `dataset_id` SQLite will reassign to the next inserted dataset's
       rowid -- silently re-adopting a stale destination's retention tiers
       into an unrelated dataset, with no error. Deleting every child table
       explicitly, in dependency order, is correct whether the pragma is on
       or off. `destinations` still comes after `datasets`/`dataset_remotes`
       for the same reason as before: `DatasetRemote.destination_name` has
       no `ON DELETE` clause (a deliberate RESTRICT -- see `models.py`'s
       `Destination` docstring), so deleting `destinations` first raises
       `IntegrityError: FOREIGN KEY constraint failed` whenever the pragma
       IS on.
    5. Insert every destination, then each dataset in `config.datasets`
       list order (list order is the only ordering `load_config` can
       recover later, via `ORDER BY id` -- see `models.py`/this module's
       `load_config`), flushing after each dataset row to obtain its `id`
       before inserting its dataset-level retention rows and its remotes,
       then flushing again after each remote row to obtain ITS `id` before
       inserting that remote's scoped retention rows
       (`RetentionRule.dataset_remote_id` needs it).
    6. Insert the new `GlobalSettings` singleton (`generation = previous +
       1`), and a `RemoteServer` singleton only if `config.remote_backup is
       not None` -- an absent row is the correct encoding of
       `remote_backup=None` (`models.py`'s `RemoteServer` docstring).

    Every duration is written as `(d.total_seconds(), getattr(d, 'literal',
    None))`. The `getattr` (rather than `d.literal`) matters: a hand-built
    `BackupConfig` (e.g. `tests/conftest.py`'s fixtures) may hold a plain
    `timedelta`, which has no `.literal` attribute at all -- `getattr(...,
    None)` degrades that to "no literal on record" (re-synthesize on next
    read) instead of raising `AttributeError`.
    """
    # --- 1. Validate, before any DELETE runs. ---
    # Mirrors `BackupConfig.from_file`'s "No datasets configured"
    # (`config.py:667-671`) -- see this function's docstring on why the
    # empty-retention half of this asymmetry is deliberately NOT mirrored
    # here (that half is the CLI's job, decision 2 / boundary item B2).
    if not config.datasets:
        raise ValueError("No datasets configured")

    # Mirrors `from_file`'s duplicate-dataset-name check
    # (`config.py:677-688`), which exists there for exactly this store's
    # `datasets.name UNIQUE` constraint -- without it here too, a
    # `BackupConfig` built directly (bypassing `from_dict`, e.g. by a
    # future CLI importer) hits a bare `IntegrityError` after the wipe.
    seen_dataset_names: Dict[str, int] = {}
    for idx, ds in enumerate(config.datasets):
        if ds.name in seen_dataset_names:
            raise ValueError(
                f"datasets[{idx}]: duplicate dataset name {ds.name!r} "
                f"(already used by datasets[{seen_dataset_names[ds.name]}])"
            )
        seen_dataset_names[ds.name] = idx

    # Mirrors `from_file`'s per-destination url requirement
    # (`config.py:710-712`, `"Destination '{name}' requires 'url'"`).
    # `destinations.url` (`models.py`) is `NOT NULL` but not `CHECK`'d
    # non-empty, so without this check a `None` url reaches the DB as a
    # bare `NOT NULL constraint failed` mid-insert (after the wipe), and an
    # empty-string url -- which the NOT NULL column happily accepts --
    # inserts silently and produces a destination nothing can ever connect
    # to. `not dest.url` rejects both the same way `from_file` does.
    for name, dest in config.destinations.items():
        if not dest.url:
            raise ValueError(f"Destination {name!r} requires 'url'")

    # Mirrors `from_file`'s `remote_backup.target_dataset` requirement
    # (`config.py:735-737`) the same way the `url` loop above mirrors its
    # `Destination.url` requirement: `remote_server.target_dataset`
    # (`models.py`) is `NOT NULL` but not `CHECK`'d non-empty, so `None`
    # would otherwise reach the final flush as a bare `NOT NULL constraint
    # failed` (after the wipe -- the caller's session, not this function,
    # is what a failed flush leaves to roll back), and `""` would insert
    # and load back as a value `from_file` can never produce.
    if config.remote_backup is not None and not config.remote_backup.target_dataset:
        raise ValueError("remote_backup requires 'target_dataset'")

    for ds in config.datasets:
        for rule in ds.retention_rules:
            _assert_positive_duration_rule(rule, f"datasets[{ds.name}].retention")
        validate_retention_uniqueness(
            ds.retention_rules, f"datasets[{ds.name}].retention"
        )
        # `from_dict` (`config.py:624-626`) never checks for two `remote`
        # entries naming the same destination -- a YAML `from_file` loads
        # cleanly here fails only at insert, as a bare `IntegrityError`,
        # against `dataset_remotes`'s `(dataset_id, destination_name)`
        # unique constraint (`models.py`).
        seen_destinations: Dict[str, int] = {}
        for idx, remote in enumerate(ds.remote):
            if remote.destination not in config.destinations:
                raise ValueError(
                    f"datasets[{ds.name}].remote[{remote.destination}] "
                    f"references destination '{remote.destination}', which "
                    "is not declared in 'destinations'"
                )
            if remote.destination in seen_destinations:
                raise ValueError(
                    f"datasets[{ds.name}].remote[{idx}]: duplicate "
                    f"destination {remote.destination!r} (already used by "
                    f"datasets[{ds.name}].remote"
                    f"[{seen_destinations[remote.destination]}])"
                )
            seen_destinations[remote.destination] = idx
            for rule in remote.retention_rules:
                _assert_positive_duration_rule(
                    rule,
                    f"datasets[{ds.name}].remote[{remote.destination}].retention",
                )
            validate_retention_uniqueness(
                remote.retention_rules,
                f"datasets[{ds.name}].remote[{remote.destination}].retention",
            )

    # --- 3. Read the generation to carry forward (before any DELETE). ---
    # This read and the `previous + 1` written in step 6 are a read-modify-
    # write with no CAS predicate of their own (see `GlobalSettings.
    # generation` in `models.py`). It is safe only because the caller's
    # session runs on a writer connection that took SQLite's write lock at
    # `BEGIN IMMEDIATE` -- `store/db.py` installs that on every file-backed
    # writer engine precisely so this read cannot race another writer's.
    # Two writers without it both read N, both write N+1, and items 15/17
    # never notice the second config. Do not "optimise" this read onto a
    # separate connection or a read-only session.
    existing_gs = session.get(models.GlobalSettings, 1)
    previous_generation = existing_gs.generation if existing_gs is not None else -1

    # --- 4. Delete in dependency order, flushing between each. ---
    # Every child table is deleted explicitly -- do NOT shrink this back to
    # just `Dataset`/`Destination` and rely on `ON DELETE CASCADE`. A bulk
    # `delete()` issued through the ORM does not apply relationship
    # cascades; only the DB's own `ON DELETE CASCADE` does, and that only
    # fires when `PRAGMA foreign_keys=ON` -- which `store/db.py` sets on
    # every connection it creates (item 5), but which is per-connection and
    # off by default everywhere else: Alembic batch migrations must run
    # FK-off, and the `sqlite3` CLI and any hand-built engine never set it.
    # This module assumes nothing about it. With the pragma off, deleting only
    # `datasets` leaves `dataset_remotes`/`retention_rules` rows behind
    # with a `dataset_id` that SQLite will hand to the next inserted
    # dataset's reused rowid -- silently re-adopting stale retention tiers
    # and remote overrides into an unrelated dataset on the very next
    # `save_config` call. See this function's docstring, step 4.
    session.execute(delete(models.RetentionRule))
    session.flush()
    session.execute(delete(models.DatasetRemote))
    session.flush()
    session.execute(delete(models.Dataset))
    session.flush()
    session.execute(delete(models.Destination))
    session.flush()
    session.execute(delete(models.GlobalSettings))
    session.flush()
    session.execute(delete(models.RemoteServer))
    session.flush()

    # --- 5. Insert destinations, then datasets (with their rules/remotes). ---
    for name, dest in config.destinations.items():
        session.add(models.Destination(name=name, url=dest.url))
    session.flush()

    for ds in config.datasets:
        dataset_row = models.Dataset(
            name=ds.name,
            recursive=ds.recursive,
            frequency_seconds=ds.frequency.total_seconds(),
            frequency_literal=getattr(ds.frequency, "literal", None),
            enabled=ds.enabled,
        )
        session.add(dataset_row)
        session.flush()  # obtain dataset_row.id for the FK below

        # --- 2. Dedupe exact duplicates before insert (per scope). ---
        for rule in _dedupe_exact_duplicates(ds.retention_rules):
            session.add(models.RetentionRule(
                dataset_id=dataset_row.id,
                dataset_remote_id=None,
                age_seconds=rule.age.total_seconds(),
                age_literal=getattr(rule.age, "literal", None),
                keep_for_seconds=rule.keep_for.total_seconds(),
                keep_for_literal=getattr(rule.keep_for, "literal", None),
            ))

        for remote in ds.remote:
            remote_row = models.DatasetRemote(
                dataset_id=dataset_row.id,
                destination_name=remote.destination,
                frequency_seconds=(
                    None if remote.frequency is None else remote.frequency.total_seconds()
                ),
                frequency_literal=(
                    None if remote.frequency is None
                    else getattr(remote.frequency, "literal", None)
                ),
            )
            session.add(remote_row)
            session.flush()  # obtain remote_row.id for the scoped rules' FK

            for rule in _dedupe_exact_duplicates(remote.retention_rules):
                session.add(models.RetentionRule(
                    dataset_id=dataset_row.id,
                    dataset_remote_id=remote_row.id,
                    age_seconds=rule.age.total_seconds(),
                    age_literal=getattr(rule.age, "literal", None),
                    keep_for_seconds=rule.keep_for.total_seconds(),
                    keep_for_literal=getattr(rule.keep_for, "literal", None),
                ))
        session.flush()

    # --- 6. Singletons. `generation` survives the wipe. ---
    session.add(models.GlobalSettings(
        id=1,
        snapshot_prefix=config.snapshot_prefix,
        check_interval_seconds=config.check_interval.total_seconds(),
        check_interval_literal=getattr(config.check_interval, "literal", None),
        # None means "derive from check_interval at read time" -- write
        # NULL to both columns (never a NULL-seconds/non-NULL-literal row,
        # which the prune_literal_requires_seconds CHECK rejects).
        prune_interval_seconds=(
            None if config.prune_interval is None
            else config.prune_interval.total_seconds()
        ),
        prune_interval_literal=(
            None if config.prune_interval is None
            else getattr(config.prune_interval, "literal", None)
        ),
        api_host=config.api_host,
        api_port=config.api_port,
        dry_run=config.dry_run,
        # None means "resolve $HOME's default in the process that uses the
        # file" -- write NULL rather than materialising this process's
        # $HOME into the DB.
        client_id_file=(
            None if config.client_id_file is None else str(config.client_id_file)
        ),
        generation=previous_generation + 1,
    ))

    if config.remote_backup is not None:
        session.add(models.RemoteServer(
            id=1,
            target_dataset=config.remote_backup.target_dataset,
            enabled=config.remote_backup.enabled,
        ))

    session.flush()

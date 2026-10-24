"""Converts between the ORM rows in `zfsbackup/config/store/models.py` and the
plain `@dataclass` config model in `zfsbackup/config/model.py`.

See `zfsbackup/config/store/__init__.py` for why the two are kept as separate
layers rather than one declarative model; this module is the "later item"
that docstring pointed at. It is a NEW coupling: `config/model.py` does not import
anything from `store`, but this module (and therefore the `store` package)
now imports `zfsbackup.config`. That direction is deliberate and safe --
`config/model.py` has no reason to know the store exists -- and should stay
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
   (`config/model.py`'s `_duration_from_config`) that *rejects* any duration value
   that is not already a `str` -- exactly what every column in this store
   is not (`*_seconds` is a `float`, and a DB-sourced `Duration` is a typed
   object, not YAML text). Round-tripping a DB row through a YAML-shaped
   dict of literal strings does not fix this either: a NULL `*_literal` has
   no string form, and it would re-derive the value from the literal rather
   than the authoritative `*_seconds`, inverting invariant 1 above.
   `from_property` is a *wire* decoder for the `org.zfsbackup:config`
   ZFS user property, not a round-trip inverse of anything -- it drops
   per-destination retention rules entirely, and `config/model.py` says so
   in its own docstring, naming this exact use as something not to do.
   Every dataclass constructed here therefore uses direct keyword
   construction with already-typed `Duration`s, matching `from_property`'s
   *style* (direct construction) without reusing its *behaviour* (lossy
   wire decoding).

`load_config` replicates `BackupConfig.from_file`'s defaulting behaviour
exactly for everything the schema cannot itself default (see the table in
the docstring of `load_config` below) -- but NOT for `prune_interval`
falling back to `check_interval`.
`GlobalSettings.prune_interval_seconds` is nullable specifically so that
"derive this at load time" is representable in the DB as `NULL` rather than
flattened into a stored value by whichever process happens to write it. This
module therefore passes a `NULL` row straight through as `None` rather than
deriving a value here: `BackupConfig.effective_prune_interval`
(`config/model.py`) owns the `check_interval` derivation, resolved lazily on
each access in whatever process calls it, not this mapper.

**The client id is not configuration** (TLS/mTLS plan D2). It lives in the
`host_identity` table, is written only by `zfsbackup-config identity ...`
(`store/trust.py`), and is NOT a `BackupConfig` field -- so YAML, `render` and
`BackupConfig.same_as` stay config-only. `load_config_snapshot` reads it in
the same read transaction as the config (`ConfigSnapshot.client_id`).
The pre-D2 `client_id_file` setting is gone: `BackupConfig.from_file` refuses
the key and the column was dropped in revision `7c3e9d51b2a8`. (The old design stored it nullable, "NULL = derive `$HOME` at read
time", because a `sudo` import freezing `/root/.config/...` orphaned the
whole server-side dataset tree; keeping the id in the DB removes the
`$HOME`-dependent path altogether.)

**`save_config` only ever deletes the six config tables** (`CONFIG_TABLES`).
`host_identity` and the trust tables (`PRESERVED_TABLES`) are not config:
an `import` or any other config write must keep the client id and every
pinned client certificate. A module-level assertion classifies every table in
`Base.metadata` as exactly one of the two, so a new table cannot be added
without deciding which side of that line it is on.
"""

from __future__ import annotations

import logging
import os
import re
import secrets
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

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
    client_tls_files_problem,
    destination_url_problem,
    normalize_fingerprint,
    validate_retention_uniqueness,
)
from zfsbackup.config.store import models
from zfsbackup.config.store.errors import ConfigConflict
from zfsbackup.config.store.migrate import check_schema

logger = logging.getLogger(__name__)


# --- Which tables `save_config` may wipe (TLS/mTLS plan M6) ------------------
#
# `save_config` is a wipe-and-reinsert of the CONFIG, and `import` calls it.
# It must never delete the PRESERVED tables: `host_identity` holds this host's
# client id (wiping it on an `import` would mint a new id later and silently
# orphan the whole server-side dataset tree -- the failure the old
# `client_id_file` design existed to dodge), and the trust tables hold every
# enrolled client's pinned certificate, every revoked fingerprint and the
# outstanding enrollment token (wiping them would lock every client out at
# once, and un-revoke every revoked key). Both lists are spelled
# out as plain strings, not derived from the models, so adding a table forces
# a decision here instead of a silent default.

# In FK-safe delete order (children first); see `save_config` step 4.
CONFIG_TABLES: Tuple[str, ...] = (
    "retention_rules",
    "dataset_remotes",
    "datasets",
    "destinations",
    "global_settings",
    "remote_server",
)
PRESERVED_TABLES: frozenset = frozenset({
    "host_identity",
    "clients",
    "trusted_clients",
    # Revoked/retired client fingerprints: wiping them on an `import` would
    # let every revoked key be re-enrolled again (see models.RevokedFingerprint).
    "revoked_fingerprints",
    "enrollment_token",
    "trust_state",
})
assert set(CONFIG_TABLES).isdisjoint(PRESERVED_TABLES)
assert set(CONFIG_TABLES) | PRESERVED_TABLES == set(models.Base.metadata.tables), (
    "a table was added to or removed from models.py without classifying it in "
    "mapper.CONFIG_TABLES / PRESERVED_TABLES: "
    f"{sorted(set(models.Base.metadata.tables) ^ (set(CONFIG_TABLES) | PRESERVED_TABLES))}"
)
# The models `save_config` deletes from, in `CONFIG_TABLES` order.
_CONFIG_MODELS_BY_TABLE = {
    m.__table__.name: m for m in (
        models.RetentionRule, models.DatasetRemote, models.Dataset,
        models.Destination, models.GlobalSettings, models.RemoteServer,
    )
}
_CONFIG_MODELS = tuple(_CONFIG_MODELS_BY_TABLE[name] for name in CONFIG_TABLES)
assert tuple(m.__table__.name for m in _CONFIG_MODELS) == CONFIG_TABLES


# --- ZFS dataset-name syntax (one source: libzfseasy's `Validate.dataset`) ---
#
# A name `libzfseasy.Dataset(name)` rejects makes every worker raise
# `ValueError` the moment it builds its `DatasetManager`, and the supervisor
# then restarts it forever (a crash loop). Catching it at the write boundary
# (`edit`, `set`, `import`) and at the daemon's own startup/reload check is
# the point of `_dataset_name_problem` below. The regex is NOT copied: it is
# `libzfseasy.types.Validate.dataset`.
#
# `libzfseasy/__init__` resolves the `zfs` binary at import time and raises
# `RuntimeError` on a host without one, but `zfsbackup-config` is documented
# to work on such a host (a box that only manages configuration). So the
# validator is taken from `sys.modules` when libzfseasy is already loaded (the
# daemon), and otherwise loaded straight from `libzfseasy/types.py` by path --
# that file imports only `re`/`typing` -- WITHOUT executing the package
# `__init__`. Nothing here ever runs a zfs command.

_dataset_validator: Optional[Callable[[str], None]] = None


def _get_dataset_validator() -> Callable[[str], None]:
    global _dataset_validator
    if _dataset_validator is None:
        loaded = sys.modules.get("libzfseasy.types")
        if loaded is None:
            import importlib.util  # noqa: PLC0415

            package = importlib.util.find_spec("libzfseasy")  # not executed
            if package is None or not package.submodule_search_locations:
                raise ImportError("libzfseasy is not importable")
            types_path = Path(list(package.submodule_search_locations)[0]) / "types.py"
            spec = importlib.util.spec_from_file_location(
                "_zfsbackup_libzfseasy_types", types_path
            )
            assert spec is not None and spec.loader is not None
            loaded = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(loaded)
        _dataset_validator = loaded.Validate.dataset
    return _dataset_validator


def _dataset_name_problem(name: str) -> Optional[str]:
    """None if `libzfseasy` accepts `name` as a dataset name, else a short
    reason-bearing sentence fragment. Accepts exactly what
    `Validate.dataset` accepts, which is STRICTER than what this code
    accepted before: non-ASCII names, names with a trailing newline and names
    longer than 255 characters used to pass and are now refused.

    The wording is about what zfsbackup can manage, not about ZFS itself:
    `Validate.dataset` is narrower than OpenZFS (whose valid characters are
    ASCII `[a-zA-Z0-9-_.: ]`), so it refuses some names ZFS would take, for
    example a leading or trailing space in a component, a space or ':' in the
    pool name, or a component starting with '.', '-' or ':'; the daemon
    cannot build such names.

    The rule it enforces, and the text below describes: ASCII only; the pool
    name starts with a letter and uses letters, digits and `_ . -` only (no
    ':' or spaces); every `/`-separated component starts with a letter, digit
    or '_', may contain ':' and inner spaces, and does not end with a space;
    no leading, trailing or doubled '/'; no '@' or '#'; no newline or other
    control character; at most 255 characters in all."""
    try:
        _get_dataset_validator()(name)
    except ValueError:
        return (
            "cannot be managed by zfsbackup (its ZFS library accepts "
            "pool[/component...] in ASCII only, at most 255 characters: the "
            "pool name starts with a letter and uses letters, digits and "
            "_ . - only, with no ':' or spaces; each component starts with a "
            "letter, digit or _ (so '.hidden', '-x' and ':x' are refused) "
            "and may contain ':' and inner spaces but not end with a space; "
            "no leading, trailing or doubled '/'; no '@' or '#'; no newlines "
            "or other control characters)"
        )
    return None


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


def _dataset_level_rules(
    ds: models.Dataset, *, warn_never_prune: bool = True
) -> List[RetentionRule]:
    """The dataset-level (local pruning) retention set for `ds`.

    THE TRAP (see module docstring on `store/__init__.py` and this file's
    own module docstring): `ds.retention_rules` returns BOTH dataset-level
    rows (`dataset_remote_id IS NULL`) and every per-destination override
    for this dataset mixed together, because the relationship joins on
    `dataset_id` alone. Filtering `dataset_remote_id is None` here is not
    optional -- omitting it silently promotes per-destination overrides
    into local pruning, with no error and no log.

    Zero dataset-level rows means **never prune** and loads back as `[]`
    (plus a WARNING naming the dataset). That is what an in-memory
    `DatasetConfig.retention_rules == []` means to the pruner
    (`DatasetManager.needs_prunning`), what `retention: forever` in YAML
    produces, and what `validate_config` accepts -- so the load path must
    not turn it into something else. It used to substitute the `from_dict`
    default (1d -> 30d) here, which silently converted "keep everything"
    into "destroy everything older than 30 days" on the first reload after
    a round trip. The default belongs to the YAML boundary
    (`DatasetConfig.from_dict`), which maps an absent/`null`/`{}`
    `retention:` key to 1d -> 30d *before* a rule exists; by the time a
    `DatasetConfig` is being saved, `[]` is a deliberate value.

    The WARNING is not noise: zero rows is also what a dataset written
    directly against the DB (bypassing `save_config`) looks like, and
    "this dataset is never pruned" is an adverse behaviour that should be
    visible in the log of whichever process loads it. `warn_never_prune=False`
    silences it for the one caller that loads a config only to discard it
    (`importer._backup_outgoing`): the new config is validated and warned
    about separately, and repeating the outgoing one on every re-import is
    noise.
    """
    rules = [
        _rule_to_dataclass(r) for r in ds.retention_rules if r.dataset_remote_id is None
    ]
    rules.sort(key=lambda r: r.age)
    if not rules and warn_never_prune:
        # Same wording as `validate_config`'s warning for the same condition,
        # and no surrogate `ds.id` (an operator cannot act on a DB row id).
        logger.warning(
            "datasets[%s].retention is empty ('retention: forever'): this "
            "dataset's snapshots will never be pruned and will accumulate "
            "until the pool is full",
            ds.name,
        )
    return rules


def _scoped_rules(remote: models.DatasetRemote) -> List[RetentionRule]:
    """The per-destination override retention set for `remote`.

    `remote.retention_rules` is already correctly scoped -- its
    relationship's `primaryjoin` is narrowed to `DatasetRemote.id ==
    RetentionRule.dataset_remote_id` (`models.py`), so unlike
    `Dataset.retention_rules` there is no promotion trap here. Zero rows
    means "inherit the dataset-level set" (`config/model.py:414-416`,
    `effective_retention_rules`) and is returned as `[]` UNCHANGED -- this
    is NOT the same meaning as `_dataset_level_rules`' empty set (there, `[]`
    is "never prune"; here it is "inherit the dataset-level set"), even
    though both load as `[]`. The two scopes must not be unified: the
    meaning belongs to where the list sits, not to the list.
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
    Since item 5, `zfsbackup/config/store/db.py` sets that pragma on every
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
    ds: models.Dataset,
    known_destinations: Dict[str, Destination],
    *,
    warn_never_prune: bool = True,
) -> DatasetConfig:
    """Map one `Dataset` row (with its `retention_rules` and `remotes`
    relationships) to a fully detached `config.DatasetConfig`.

    `known_destinations` is the already-loaded `{name: Destination}` map
    from the whole `destinations` table (see `load_config`) -- used only to
    re-check that every `DatasetRemote.destination_name` is declared,
    mirroring `config/model.py:715-728`'s load-time check. The FK on
    `destination_name` normally makes an undeclared reference impossible,
    but (as with `_assert_scope_integrity`) that guarantee is conditional on
    `PRAGMA foreign_keys=ON`. Item 5's `zfsbackup/config/store/db.py` now sets that
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
            # NULL = auto = None, passed straight through (never coerced to
            # False). The Boolean column type already converts 0/1 to bool;
            # the `is None` test only keeps NULL from becoming False.
            raw=remote.raw,
        ))

    return DatasetConfig(
        name=ds.name,
        # SQLite can hand back 0/1 ints for boolean columns depending on
        # path; coerce so `==` against `from_file`'s real `bool` holds.
        recursive=bool(ds.recursive),
        frequency=frequency,
        retention_rules=_dataset_level_rules(ds, warn_never_prune=warn_never_prune),
        enabled=bool(ds.enabled),
        remote=remote_configs,
    )


def load_config(session: Session, *, warn_never_prune: bool = True) -> BackupConfig:
    """Build a fully detached `BackupConfig` from the config store.

    Replicates `BackupConfig.from_file`'s defaulting behaviour exactly for
    everything the schema itself cannot default -- see the module
    docstring for `prune_interval`, which is deliberately NOT derived here
    even though `from_file` would: `GlobalSettings` makes the column nullable
    specifically so a DB row can represent "derive this elsewhere", and this
    function passes that `NULL` through as `BackupConfig.prune_interval is
    None` rather than resolving it. The client id is not part of the result
    at all (`ConfigSnapshot.client_id`, module docstring).

    | `from_file` default/behaviour | Who owns it on the DB path |
    |---|---|
    | `snapshot_prefix="autosnap"` | Writer (`NOT NULL`, no schema default); this function reads |
    | `check_interval="5m"` | Writer (`NOT NULL`); this function reads |
    | `prune_interval` <- `check_interval` | NOT reimplemented; NULL row yields `None` |
    | `api_host`/`api_port`/`dry_run` | Writer (`NOT NULL`, no schema default); this function reads |
    | `destinations={}` | Emergent -- empty table yields `{}` |
    | `remote_backup=None` | This function -- absent `RemoteServer` row |
    | `client_cert_file`/`client_key_file` | Read as given (`None` = unset; the CHECK keeps the pair together) |
    | `Destination.server_fingerprint` | Read as given (`None` = not enrolled), through `normalize_fingerprint` |
    | "No datasets configured" | This function |
    | retention `{'1d': '30d'}` default | NOT reimplemented -- YAML boundary only; zero rows load as `[]` (never prune) + WARNING, `_dataset_level_rules` |
    | retention sorted by age | `_dataset_level_rules`/`_scoped_rules` |
    | undeclared destination rejected | `_dataset_to_dataclass` (belt to the FK) |

    Every dataclass is built eagerly, before this function returns, so the
    result is safe to hold for the life of a `BackupDaemon` well past this
    session's lifetime -- no lazy-loaded ORM attribute is ever touched
    after `load_config` returns (see `store/__init__.py`'s
    `DetachedInstanceError` rationale).

    `validate_retention_uniqueness` and `_collapse_retention_rules`
    (`config/model.py`) are deliberately NOT called here: the former has zero
    production callers on the load path today (`config/model.py:573-577`), and
    the latter is a pruning-time function, not a load-path one -- see this
    module's `validate_config` for where uniqueness validation belongs
    instead. `validate_config` is likewise not run on load: a load must
    reflect what is stored (a hand-edited row is the operator's to see in the
    logs, not something to refuse to start over).

    `warn_never_prune=False` suppresses the per-dataset "never be pruned"
    WARNING for a caller that discards the result (the importer's
    outgoing-config backup). Leave it True everywhere else.
    """
    gs = session.get(models.GlobalSettings, 1)
    if gs is None:
        raise ValueError(
            "config store is uninitialized: no GlobalSettings row (id=1) "
            "found; run 'zfsbackup-config import' to populate it"
        )

    # Ordered: `BackupConfig.destinations` is a dict, so insertion order is
    # what a renderer (`zfsbackup-config show`/`export`, `$EDITOR`) emits.
    # An unordered SELECT is free to return rowid order, index order, or
    # whatever the planner picks -- and "no changes" detection compares
    # post-normalisation renders, so the order must be deterministic.
    destination_rows = session.scalars(
        select(models.Destination).order_by(models.Destination.name)
    ).all()
    # ALL declared destinations, not just ones referenced by a dataset --
    # `BackupConfig.destinations` is every declared destination
    # (`config/model.py:705-713`), so an unreferenced one must still round-trip.
    destinations: Dict[str, Destination] = {
        # `Destination.__post_init__` routes the pin through
        # `normalize_fingerprint`; the column CHECK already guarantees the
        # canonical form, so this never rewrites a stored value.
        row.name: Destination(url=row.url, server_fingerprint=row.server_fingerprint)
        for row in destination_rows
    }

    dataset_rows = session.scalars(
        select(models.Dataset).order_by(models.Dataset.id)
    ).all()
    if not dataset_rows:
        raise ValueError("No datasets configured")

    datasets = [
        _dataset_to_dataclass(ds, destinations, warn_never_prune=warn_never_prune)
        for ds in dataset_rows
    ]

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
        client_cert_file=(
            None if gs.client_cert_file is None else Path(gs.client_cert_file)
        ),
        client_key_file=(
            None if gs.client_key_file is None else Path(gs.client_key_file)
        ),
    )


# --- Store id (plan item 17.0, decision D8) ---------------------------------
#
# `GlobalSettings.generation` alone is not an identity: a fresh import is
# generation 0, so deleting the database and importing again repeats keys a
# running daemon or a stale editor already holds (the ABA hole). The store id
# is a random per-FILE number kept in SQLite's `PRAGMA user_version` header
# field, so `(store_id, generation)` is unique across file replacements.
#
# Why `user_version` and not a table column: it survives `upgrade_schema` and
# every Alembic batch migration untouched (they rebuild tables, never the
# header), it is readable in the same cheap snapshot as `generation`, and it
# needs no migration. 0 means "never stamped" (a legacy database, or one a
# rolled-back first save left alone). Foreign-database detection never
# consults it -- an unrelated SQLite file with a non-zero value must still be
# refused by the schema checks alone, not trusted or distrusted by this.

_STORE_ID_BOUND = 2**31 - 2  # user_version is a signed 32-bit int; keep > 0


def read_store_id(session: Session) -> int:
    """The database's `PRAGMA user_version` (the store id; 0 = never stamped),
    read on `session`'s connection and therefore inside whatever transaction
    that session is in.
    """
    value = session.connection().exec_driver_sql("PRAGMA user_version").scalar()
    return int(value or 0)


class StoreIdConflict(ConfigConflict):
    """A `ConfigConflict` for a replaced database file rather than a moved
    generation: the store id the caller read at is not the one stored now.
    Nothing was written. Re-reading finds the new file, so the right handling
    is the same as for any `ConfigConflict` (exit code 75, retry).

    Attributes:

    - `expected_store_id` / `actual_store_id`: the two store ids.
    - `expected` / `actual`: the INHERITED attributes keep their
      `ConfigConflict` meaning, a generation -- `expected` is the caller's
      `expected_generation` (`None` if it passed none) and `actual` the stored
      generation (`None` if the store holds no `GlobalSettings` row) -- so a
      generic `ConfigConflict` consumer that formats them as generations
      stays truthful instead of printing store ids as generations. A consumer
      that wants the real reason checks `isinstance(exc, StoreIdConflict)`
      first, or uses `str(exc)`.
    """

    def __init__(
        self,
        expected_store_id: int,
        actual_store_id: int,
        expected_generation: Optional[int] = None,
        actual_generation: Optional[int] = None,
    ) -> None:
        # Deliberately bypass `ConfigConflict.__init__`, whose message is
        # about generations.
        Exception.__init__(
            self,
            f"the configuration database was replaced since it was read "
            f"(store id {expected_store_id}, now {actual_store_id}); "
            f"nothing was written. Re-run.",
        )
        self.expected_store_id = expected_store_id
        self.actual_store_id = actual_store_id
        self.expected = expected_generation  # type: ignore[assignment]
        self.actual = actual_generation


@dataclass(frozen=True)
class ConfigSnapshot:
    """A configuration together with the facts it was read at, all from ONE
    read transaction.

    `generation` is `GlobalSettings.generation` -- the compare-and-swap token
    a writer hands back to `save_config`/`write_config` as
    `expected_generation`. `schema_rev` is the Alembic revision the schema
    check verified (always `EXPECTED_HEAD`; a snapshot of any other schema is
    refused with a `SchemaError` before it exists). `config` is a fully
    detached `BackupConfig` (see `load_config`): safe to hold for the life of
    the process.

    `store_id` is the file's `PRAGMA user_version` (0 = a legacy database no
    save has stamped yet), read in the same transaction as everything else;
    `(store_id, generation)` is the reload key. Defaulted so the three-field
    positional constructor keeps working.

    `client_id` is `host_identity.client_id`, read in the SAME transaction as
    the config (plan D2), or `None` when no id has been generated yet (a
    fresh store: an expected state, not corruption). It is deliberately NOT a
    `BackupConfig` field: YAML, `render` and `BackupConfig.same_as` stay
    config-only. Anything that stores the id writes it with a generation bump
    (`store/trust.py`), so a consumer that compares snapshots must compare
    `client_id` as well as the config -- an identical config with a changed id
    is a different snapshot.
    """

    config: BackupConfig
    generation: int
    schema_rev: str
    store_id: int = 0
    client_id: Optional[str] = None


def load_config_snapshot(
    session: Session,
    *,
    schema_rev: Optional[str] = None,
    warn_never_prune: bool = True,
) -> ConfigSnapshot:
    """`load_config` plus the generation and schema revision, as one read.

    The CALLER owns the transaction: `session` must already be inside the one
    read transaction the snapshot is meant to be consistent with (for a
    reader, an explicit `BEGIN` -- see `access.read_config`; for a writer
    session, the `BEGIN IMMEDIATE` it opened with). Everything here is read
    through it, so the config rows, the generation and the schema revision
    cannot come from different commits. `generation` is deliberately not read
    by a second, later query on a fresh transaction: that would let a write
    land between the config and its token, and the token would then
    authorise overwriting a config the caller never saw.

    `schema_rev`: pass the revision a `check_schema` run on this same
    transaction already returned. Left `None`, `check_schema` runs here, on
    `session.connection()`, i.e. inside the same transaction -- so a snapshot
    is never handed out for a schema this package cannot read. Raises
    whatever `check_schema` / `load_config` raise.

    `warn_never_prune` is passed through to `load_config` (a periodic reader,
    e.g. a reload, has already logged the per-dataset warning once and must
    not repeat it on every read).
    """
    if schema_rev is None:
        schema_rev = check_schema(session.connection())
    config = load_config(session, warn_never_prune=warn_never_prune)
    # `load_config` raised if this row were absent, and it is already in the
    # session's identity map from that very read.
    gs = session.get(models.GlobalSettings, 1)
    assert gs is not None
    # Column select (no ORM instance), same transaction as the rows above, so
    # the id and the config come from one commit. No row = no id yet.
    client_id = session.scalar(
        select(models.HostIdentity.client_id).where(models.HostIdentity.id == 1)
    )
    return ConfigSnapshot(
        config=config,
        generation=gs.generation,
        schema_rev=schema_rev,
        # Same transaction as the rows above (see read_store_id).
        store_id=read_store_id(session),
        client_id=client_id,
    )


_MAX_PORT = 65535


def _describe(value: object) -> str:
    return f"{type(value).__name__} {value!r}"


def _require_bool(value: object, key: str) -> None:
    """Reject anything that is not exactly `bool`.

    `type(value) is bool`, not `isinstance`: `bool` is a subclass of `int`,
    but the reverse matters here -- an `int` 0/1, a `None`, or a YAML string
    like `"false"` (truthy!) is the shape of the bugs this guards. The ORM
    would coerce `1`/`0` silently and raise a bare `StatementError` for a
    string, mid-insert, after the wipe.
    """
    if type(value) is not bool:
        raise ValueError(f"{key} must be a boolean, got {_describe(value)}")


def _require_str(value: object, key: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string, got {_describe(value)}")


def _require_positive_duration(value: object, key: str, what: str = "") -> float:
    """Reject a non-`timedelta`, or a non-positive one; return its seconds.

    Mirrors `config/model.py`'s `_positive_duration_from_config` (age 0 is a
    division-by-zero in the slot-based retention algorithm,
    `DatasetManager.needs_prunning`; an interval <= 0 makes
    `StopFlag.wait(<=0)` return immediately, a worker hot loop shelling out
    to `zfs list` per dataset) -- but that guard runs only on the
    YAML-boundary path. A `BackupConfig` built directly (the Phase 2 CLI, a
    test) never passes through it, so without this the DB's `CHECK (... > 0)`
    constraints would be the only thing that catches it, as a bare `CHECK
    constraint failed` raised mid-insert with the wipe already flushed.
    """
    if not isinstance(value, timedelta):
        raise ValueError(f"{key} must be a duration, got {_describe(value)}")
    seconds = value.total_seconds()
    if seconds <= 0:
        raise ValueError(f"{key}: {what or 'duration'} must be positive, got {seconds:.0f}s")
    return seconds


def _literal(d: timedelta) -> str:
    literal = getattr(d, "literal", None)
    return literal if literal is not None else str(d)


def _validate_rules(
    rules: List[RetentionRule], key: str, warnings: List[str]
) -> None:
    """Validate one scope's rule list (dataset-level or one destination's).

    Raises on a non-positive/mistyped duration or an age/keep_for collision
    (`validate_retention_uniqueness`); appends a warning for each exact
    duplicate, which `save_config` is about to drop (see
    `_dedupe_exact_duplicates` for why the DB cannot store them).
    """
    for rule in rules:
        _require_positive_duration(rule.age, f"{key}", "age")
        _require_positive_duration(rule.keep_for, f"{key}", "keep_for")
    validate_retention_uniqueness(rules, key)

    seen: Dict[Tuple[float, float], RetentionRule] = {}
    for rule in rules:
        sig = (rule.age.total_seconds(), rule.keep_for.total_seconds())
        first = seen.get(sig)
        if first is None:
            seen[sig] = rule
            continue
        warnings.append(
            f"{key}: rules {_literal(first.age)}:{_literal(first.keep_for)} "
            f"and {_literal(rule.age)}:{_literal(rule.keep_for)} are the same "
            f"rule ({sig[0]:.0f}s -> {sig[1]:.0f}s); keeping "
            f"{_literal(first.age)}:{_literal(first.keep_for)} and dropping "
            "the duplicate"
        )


_PROPERTY_NAME_CHARS = re.compile(r"[A-Za-z0-9_.:-]+")
_ANCHOR_PROPERTY_PREFIX = "org.zfsbackup:anchor."
_MAX_PROPERTY_NAME = 256  # ZAP_MAXNAMELEN


def destination_name_problem(name: str) -> Optional[str]:
    """Why `name` cannot hold a ZFS anchor, or None if it can. Pure.

    A destination's name is part of a ZFS user-property name,
    `org.zfsbackup:anchor.<name>`. OpenZFS accepts ASCII letters, digits and
    `-` `_` `.` `:` in user-property names (`zprop_valid_char`; the manual
    recommends lowercase) and at most 256 characters in all. This is the
    conservative form of that rule. A name outside it can never have an
    anchor stored, so the incremental base of every transfer to that
    destination is unprotected from pruning.

    Public: `validate_config` reports it as a WARNING (existing configs may
    legitimately contain such names), and `zfsbackup-config rename
    destination` uses it as a hard refusal for the NEW name.
    """
    if not name:
        return "it is empty"
    if not _PROPERTY_NAME_CHARS.fullmatch(name):
        bad = sorted({c for c in name if not _PROPERTY_NAME_CHARS.fullmatch(c)})
        return (
            f"it contains {' '.join(repr(c) for c in bad)}; the name becomes part of the ZFS "
            f"property {_ANCHOR_PROPERTY_PREFIX}<name>, which allows only letters, digits "
            "and - _ . :"
        )
    if len(_ANCHOR_PROPERTY_PREFIX) + len(name) > _MAX_PROPERTY_NAME:
        return (
            f"it is too long: {_ANCHOR_PROPERTY_PREFIX}<name> may be at most "
            f"{_MAX_PROPERTY_NAME} characters"
        )
    return None


def _dataset_components(name: str) -> List[str]:
    """Split a dataset name into path components (a trailing `/` is ignored)."""
    return name.rstrip("/").split("/")


def _require_outside_target(ds: DatasetConfig, key: str, remote_backup: object) -> None:
    """Refuse a configured dataset that overlaps `remote_backup.target_dataset`.

    What bug this prevents: the server enumerates EVERYTHING under the target
    as a received dataset. A configured dataset at or below the target would
    be snapshotted and pruned by this host's own workers inside received
    data (the next incremental receive then fails, and an aligned
    `{prefix}_{ts}` name makes the prune destroy received snapshots); a
    `recursive` ancestor reaches the same datasets through `zfs snapshot -r`
    and `zfs destroy -r`. Enabled or not, so enabling a dataset later cannot
    open the hole.

    Compared by path COMPONENTS, never by string prefix: `tank/b` is not an
    ancestor of `tank/backup`, and `tank/backup` is not the target
    `tank/backups`. A non-recursive ancestor only touches its own snapshots,
    so it stays allowed. No check without a remote_backup target (disabled or
    unset: nothing is received, so nothing can overlap).
    """
    if remote_backup is None or not getattr(remote_backup, "enabled", False):
        return
    target = getattr(remote_backup, "target_dataset", None)
    if not isinstance(target, str) or not target or not isinstance(ds.name, str):
        return
    t = _dataset_components(target)
    d = _dataset_components(ds.name)
    if d[: len(t)] == t:
        where = "is" if len(d) == len(t) else "is below"
        raise ValueError(
            f"{key}: the dataset {where} remote_backup.target_dataset "
            f"{target!r}. The server treats everything under the target as "
            "received backups, so snapshotting or pruning a dataset there "
            "would act on received data. Move the target to a dedicated "
            "dataset that no configured dataset is at or below."
        )
    if ds.recursive is True and len(d) < len(t) and t[: len(d)] == d:
        raise ValueError(
            f"{key}: the dataset is recursive and is an ancestor of "
            f"remote_backup.target_dataset {target!r}. Its recursive "
            "snapshots (zfs snapshot -r) and prunes (zfs destroy -r) would "
            "reach the received datasets under the target. Make the dataset "
            "non-recursive, or move the target outside it."
        )


def validate_config(config: BackupConfig) -> List[str]:
    """Check that `config` is storable and means what it says; pure.

    Session-free and side-effect-free: no DB, no filesystem, and in
    particular it never evaluates `effective_prune_interval`. Returns a list of
    human-readable WARNINGS (adverse-but-accepted behaviour -- see below);
    raises `ValueError` naming the offending key for anything that cannot
    or must not be stored. Callers decide what to do with the warnings
    (`save_config` does not; the importer logs them before it touches the
    target and the Phase 2 CLI prints them).

    This is the write-side twin of the YAML-boundary guards in
    `config/model.py` (`DatasetConfig.from_dict` / `BackupConfig.from_file`),
    and it must stay a *separate* check: a `BackupConfig` built in code
    never passes through those guards, and the schema's own constraints can
    only fail mid-insert, after `save_config`'s wipe -- which is why every
    check here runs before the first DELETE. None of it is FK-related, so
    `PRAGMA foreign_keys=ON` makes none of it redundant.

    Raises (`ValueError`):

    - no datasets; duplicate dataset names (`datasets.name UNIQUE`);
    - an ENABLED dataset whose name `libzfseasy`'s `Validate.dataset`
      rejects (a leading/trailing space in a component, a trailing `/`, ...), and an ENABLED
      `remote_backup`'s `target_dataset` likewise -- either would raise in
      every worker and crash-loop the daemon. The same names on a DISABLED
      dataset / `remote_backup` only warn (they were never built, so they
      loaded fine before);
    - wrong types: a bool field that is not exactly `bool`, a string field
      that is not a `str`, `api_port` not an `int` (`bool` excluded) in
      0..65535, a duration that is not a `timedelta`. A `None` here used to
      be silently replaced by an ORM default at INSERT time -- `enabled:
      null` (disabled under YAML) was stored as enabled, and
      `remote_backup.enabled: null` switched the unauthenticated receive
      path ON. `snapshot_prefix: None` is rejected rather than defaulted
      (the runtime never worked with it: `None_*` snapshots); the message
      tells an operator whose snapshots really are named `None_*` to set
      `"None"` explicitly;
    - `check_interval`, `prune_interval` (when set), dataset `frequency`
      and destination `frequency` (when set) <= 0 (hot loop; mirrored by
      CHECK constraints in the schema);
    - a destination with an empty `url`; a `remote_backup` with an empty
      `target_dataset`; a dataset `remote` entry naming an undeclared
      destination or naming the same destination twice
      (`dataset_remotes`' composite unique constraint; `from_dict` rejects
      the same thing at the YAML boundary, so this is the backstop for
      configs built in code);
    - a destination `url` that is not `https://` with a host
      (`destination_url_problem`: plaintext `http://` has no downgrade
      path) or that has leading/trailing whitespace (`from_file` strips it;
      the store does not rewrite values), a `server_fingerprint` that is not canonical (64 lowercase
      hex), a `client_cert_file`/`client_key_file` that is not an absolute
      path or is set without the other (`client_tls_files_problem`);
    - any retention rule with a non-positive `age`/`keep_for`, and any
      `validate_retention_uniqueness` collision, per scope.
    - with remote backup ENABLED: a configured dataset (enabled or not) at or
      below `remote_backup.target_dataset`, or a `recursive` one that is a
      strict ancestor of it (`_require_outside_target`).

    Warns (returned, not raised):

    This is the SINGLE source of the semantic warnings below:
    `BackupConfig.from_file` / `DatasetConfig.from_dict` no longer emit them
    (they keep only parse-boundary warnings), so a config is warned about
    exactly once, here, whichever way it was built.

    - a dataset with no dataset-level retention rules (`retention: forever`):
      `[]` means NEVER PRUNE, so its snapshots accumulate until the pool is
      full;
    - `api_port` 0 (the API binds an ephemeral port that changes on every
      start);
    - a destination name that cannot be a ZFS user-property suffix
      (`destination_name_problem`): its `org.zfsbackup:anchor.<name>` anchor
      can never be stored;
    - exact `(age, keep_for)` duplicates within a scope, naming both
      literals -- `save_config` drops the later one because the DB's
      unique indexes cannot hold them.
    """
    warnings: List[str] = []

    if not config.datasets:
        raise ValueError("No datasets configured")

    # Mirrors `from_file`'s duplicate-dataset-name check, which exists there
    # for exactly this store's `datasets.name UNIQUE` constraint -- without
    # it here too, a `BackupConfig` built directly hits a bare
    # `IntegrityError` after the wipe.
    seen_dataset_names: Dict[str, int] = {}
    for idx, ds in enumerate(config.datasets):
        _require_str(ds.name, f"datasets[{idx}].name")
        if not ds.name:
            raise ValueError(f"datasets[{idx}]: Dataset 'name' is required")
        if ds.name in seen_dataset_names:
            raise ValueError(
                f"datasets[{idx}]: duplicate dataset name {ds.name!r} "
                f"(already used by datasets[{seen_dataset_names[ds.name]}])"
            )
        seen_dataset_names[ds.name] = idx

    # --- global settings ---
    if config.snapshot_prefix is None:
        raise ValueError(
            "snapshot_prefix must be a string, got NoneType None. (The "
            "runtime never worked with a null prefix: it named snapshots "
            "'None_<timestamp>' and never pruned them. If your existing "
            "snapshots really are named 'None_*', set snapshot_prefix to the "
            "string \"None\" explicitly.)"
        )
    _require_str(config.snapshot_prefix, "snapshot_prefix")

    _require_positive_duration(config.check_interval, "check_interval", "interval")
    if config.prune_interval is not None:
        _require_positive_duration(config.prune_interval, "prune_interval", "interval")

    _require_str(config.api_host, "api_host")
    # `bool` is an `int` subclass: `api_port: true` would otherwise pass as 1.
    if not isinstance(config.api_port, int) or isinstance(config.api_port, bool):
        raise ValueError(f"api_port must be an integer, got {_describe(config.api_port)}")
    if not 0 <= config.api_port <= _MAX_PORT:
        raise ValueError(
            f"api_port must be between 0 and {_MAX_PORT}, got {config.api_port}"
        )
    if config.api_port == 0:
        warnings.append(
            "api_port is 0: the API will bind an ephemeral port that "
            "changes on every start"
        )
    _require_bool(config.dry_run, "dry_run")

    # Optional absolute client certificate/key paths, both or neither. Type
    # first so `client_tls_files_problem` (pure string handling) never sees a
    # non-path; `global_settings.client_cert_key_together` is the DB backstop.
    for key in ("client_cert_file", "client_key_file"):
        value = getattr(config, key)
        if value is not None and not isinstance(value, (str, os.PathLike)):
            raise ValueError(f"{key} must be a path, got {_describe(value)}")
    problem = client_tls_files_problem(config.client_cert_file, config.client_key_file)
    if problem is not None:
        raise ValueError(problem)

    # Mirrors `from_file`'s per-destination url requirement
    # (`"Destination '{name}' requires 'url'"`). `destinations.url` is NOT NULL
    # but not CHECK'd non-empty, so an empty-string url would insert silently
    # and produce a destination nothing can ever connect to. `not dest.url`
    # rejects `None` and `""` the same way `from_file` does.
    for name, dest in config.destinations.items():
        _require_str(name, f"destinations key {name!r}")
        if not dest.url:
            raise ValueError(f"Destination {name!r} requires 'url'")
        _require_str(dest.url, f"destinations[{name}].url")
        # TLS only: `http://` (and any other scheme) is refused. This is NOT
        # in `Destination.__post_init__` so a legacy plaintext row can still
        # be LOADED and fixed with the CLI; the daemon (which runs this on
        # every start and reload) refuses to run it.
        problem = destination_url_problem(dest.url)
        if problem is not None:
            raise ValueError(f"destinations[{name}]: {problem}")
        # `destination_url_problem` judges the STRIPPED url, so it accepted
        # "  https://h " -- which `save_config` then stored verbatim, while
        # `from_file` strips. Refused rather than stripped, for the same
        # reason as a non-canonical `server_fingerprint` below: the store
        # never rewrites a value, so `load_config(save_config(c)) == c`.
        if dest.url != dest.url.strip():
            raise ValueError(
                f"destinations[{name}].url {dest.url!r} has leading or "
                f"trailing whitespace; expected {dest.url.strip()!r}"
            )
        # A pin must already be canonical (64 lowercase hex): `save_config`
        # writes it verbatim, and the DB CHECK would reject anything else
        # only mid-insert, after the wipe. Rewriting it silently here would
        # make `load_config(save_config(c)) != c`.
        if dest.server_fingerprint is not None:
            try:
                canonical = normalize_fingerprint(dest.server_fingerprint)
            except ValueError as e:
                raise ValueError(
                    f"destinations[{name}].server_fingerprint: {e}"
                ) from None
            if canonical != dest.server_fingerprint:
                raise ValueError(
                    f"destinations[{name}].server_fingerprint "
                    f"{dest.server_fingerprint!r} is not canonical; "
                    f"expected {canonical!r} (64 lowercase hex digits)"
                )
        # WARNING, not an error: configs that predate this check may already
        # carry such names, and refusing to load/import them would strand
        # the operator. The cost of the name is real but silent -- no anchor
        # can ever be written for it, so the snapshot an incremental send
        # depends on is not protected from pruning.
        problem = destination_name_problem(name)
        if problem is not None:
            warnings.append(
                f"destinations[{name}]: the name cannot hold a ZFS anchor "
                f"({problem}); anchors can never be stored for it, so the "
                "incremental base for this destination is not protected "
                "from pruning"
            )

    # Mirrors `from_file`'s `remote_backup.target_dataset` requirement, same
    # reasoning as `url` above.
    if config.remote_backup is not None:
        if not config.remote_backup.target_dataset:
            raise ValueError("remote_backup requires 'target_dataset'")
        _require_str(config.remote_backup.target_dataset, "remote_backup.target_dataset")
        # Exactly bool: an enabled flag that is `None` is DISABLED under YAML.
        _require_bool(config.remote_backup.enabled, "remote_backup.enabled")
        # `received_datasets()` and the API build `Dataset(target_dataset)`
        # when remote backup is enabled; a name libzfseasy rejects there
        # fails every receive. A DISABLED remote_backup never builds it, so
        # that stays accepted (it loaded fine before) with a warning.
        problem = _dataset_name_problem(config.remote_backup.target_dataset)
        if problem is not None:
            message = (
                f"remote_backup.target_dataset "
                f"{config.remote_backup.target_dataset!r} {problem}"
            )
            if config.remote_backup.enabled:
                raise ValueError(message)
            warnings.append(
                message + "; remote_backup is disabled so it is ignored, "
                "but enabling it will be refused"
            )

    # --- datasets ---
    for ds in config.datasets:
        key = f"datasets[{ds.name}]"
        _require_bool(ds.recursive, f"{key}.recursive")
        _require_bool(ds.enabled, f"{key}.enabled")
        # Only ENABLED datasets are turned into `libzfseasy.Dataset` objects
        # (`DatasetManager._get_datasets`), where an invalid name raises in
        # every worker and crash-loops the daemon. A disabled dataset with
        # such a name is never built, so it loaded fine before this check and
        # stays accepted -- with a warning, since enabling it will be refused.
        problem = _dataset_name_problem(ds.name)
        if problem is not None:
            message = f"{key}: dataset name {ds.name!r} {problem}"
            if ds.enabled:
                raise ValueError(message)
            warnings.append(
                message + "; the dataset is disabled so it is ignored, but "
                "enabling it will be refused"
            )
        _require_positive_duration(ds.frequency, f"{key}.frequency", "frequency")
        _require_outside_target(ds, key, config.remote_backup)

        if not ds.retention_rules:
            warnings.append(
                f"{key}.retention is empty ('retention: forever'): this "
                "dataset's snapshots will never be pruned and will "
                "accumulate until the pool is full"
            )
        _validate_rules(ds.retention_rules, f"{key}.retention", warnings)

        # `DatasetConfig.from_dict` now rejects two `remote` entries naming
        # the same destination (including exact duplicates) at the YAML
        # boundary, with this same wording, so this check is the BACKSTOP
        # for configs built in code, `from_property` and the like. Without
        # it a duplicate would fail only at insert, as a bare
        # `IntegrityError`, against `dataset_remotes`'s
        # `(dataset_id, destination_name)` unique constraint.
        seen_destinations: Dict[str, int] = {}
        for idx, remote in enumerate(ds.remote):
            _require_str(remote.destination, f"{key}.remote[{idx}].destination")
            if remote.destination not in config.destinations:
                raise ValueError(
                    f"{key}.remote[{remote.destination}] "
                    f"references destination '{remote.destination}', which "
                    "is not declared in 'destinations'"
                )
            if remote.destination in seen_destinations:
                raise ValueError(
                    f"{key}.remote[{idx}]: duplicate "
                    f"destination {remote.destination!r} (already used by "
                    f"{key}.remote"
                    f"[{seen_destinations[remote.destination]}])"
                )
            seen_destinations[remote.destination] = idx
            if remote.frequency is not None:
                _require_positive_duration(
                    remote.frequency,
                    f"{key}.remote[{remote.destination}].frequency",
                    "frequency",
                )
            _validate_rules(
                remote.retention_rules,
                f"{key}.remote[{remote.destination}].retention",
                warnings,
            )

    return warnings


def _dedupe_exact_duplicates(rules: List[RetentionRule]) -> List[RetentionRule]:
    """Drop rules that are exact duplicates (same age AND same keep_for),
    preserving first-seen order.

    `validate_retention_uniqueness` deliberately PERMITS exact duplicates
    (`config/model.py:300-302`, `test_exact_duplicates_are_silent`), but the DB's
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


def save_config(
    session: Session,
    config: BackupConfig,
    *,
    expected_generation: Optional[int] = None,
    expected_store_id: Optional[int] = None,
) -> int:
    """Replace the entire config store with `config`'s contents and return the
    new `GlobalSettings.generation`.

    **Store id (item 17.0).** Inside the caller's transaction this reads
    `PRAGMA user_version` before the compare-and-swap. `expected_store_id`,
    when not `None`, must equal it, else `StoreIdConflict` (a `ConfigConflict`;
    "the configuration database was replaced since it was read") with nothing
    written -- checked FIRST, so a replaced file reports the replacement and
    not a coincidentally-equal generation. After all the rows are written, a
    stored value of 0 is stamped with a random non-zero id
    (`secrets.randbelow`); a non-zero value is never re-stamped. Stamping is
    the last statement, in the same transaction as the data, so a rollback
    (any failure above, a lost commit) leaves 0 and the next save stamps. The
    id after this call is `read_store_id(session)`. Only `save_config` stamps:
    `create_config_db_file` must not open SQLite, and readers never write.

    `expected_generation` makes this a compare-and-swap: when given, the
    stored generation is compared with it inside the caller's transaction,
    BEFORE any `DELETE`, and a mismatch raises `ConfigConflict(expected,
    actual)` with nothing written (`actual` is `None` if the store holds no
    `GlobalSettings` row). Pass the `ConfigSnapshot.generation` the edit was
    based on. `None` (the default) is an unconditional replace -- `import`'s
    semantics: the operator is deliberately discarding whatever is there.
    The comparison is only meaningful if the caller's transaction already
    holds SQLite's write lock (`db.py`'s writer sessions open with `BEGIN
    IMMEDIATE`); on a connection that does not, the read and the later
    write are not atomic.

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

    1. `validate_config(config)`, BEFORE any DELETE runs, so a bad input
       never wipes a good DB -- the full list of what it rejects (and why
       each check exists, and why none of it is made redundant by
       `PRAGMA foreign_keys=ON`) is in its docstring. Its warnings are
       deliberately NOT logged here: `save_config` has no way to show them
       to an operator, and the callers that do (the importer's phase 0, the
       CLI's write path) validate earlier -- before they open a write
       transaction -- and report them themselves. Calling `save_config`
       directly therefore skips the never-prune and duplicate-rule
       warnings; call `validate_config` yourself to get them.
    2. Drop exact `(age, keep_for)` duplicates per scope (see
       `_dedupe_exact_duplicates`) -- the DB rejects what step 1 permits
       (and `validate_config` has already warned, naming both literals).
    3. Read the current `GlobalSettings.generation` (default -1 if the
       table is empty/absent) so the fresh row below is `previous + 1`, not
       reset to 0 -- the generation is a compare-and-swap token surviving a
       save. If `expected_generation` was given, compare it to this value
       now, before anything is deleted (`ConfigConflict`).
    4. Delete in order **retention_rules -> dataset_remotes -> datasets ->
       destinations -> global_settings -> remote_server** (`CONFIG_TABLES`;
       `host_identity` and the trust tables are deliberately left alone --
       see the module docstring), flushing between each. This is a superset of the FK-implied order and does NOT rely
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
    validate_config(config)

    # --- 3. Read the generation, compare it, carry it forward. ---
    # All before any DELETE: a conflict must leave the store byte-for-byte
    # as it was, and the DELETEs below are not undoable short of the
    # caller rolling back the whole session.
    #
    # This is the CAS the `GlobalSettings.generation` comment in `models.py`
    # describes. Its atomicity comes from the caller's transaction holding
    # SQLite's write lock from before this read until commit (`store/db.py`
    # starts every file-backed writer transaction with `BEGIN IMMEDIATE`), so
    # no one can commit between the compare here and the `previous + 1` write
    # in step 6. Do not move this read onto a separate connection or a
    # read-only session.
    stored_store_id = read_store_id(session)
    existing_gs = session.get(models.GlobalSettings, 1)
    # `expected_store_id == 0` means the caller read a legacy, never-stamped
    # database. If it has been stamped since, that is NOT a replacement: a
    # stamp only ever happens inside a save, which bumps the generation, so the
    # generation CAS below refuses with the true reason ("the config changed")
    # -- and, with no expected generation, the write is unconditional anyway.
    # Only a non-zero expected id that differs means "this is another file".
    if (
        expected_store_id is not None
        and expected_store_id != 0
        and expected_store_id != stored_store_id
    ):
        raise StoreIdConflict(
            expected_store_id,
            stored_store_id,
            expected_generation,
            None if existing_gs is None else existing_gs.generation,
        )

    previous_generation = existing_gs.generation if existing_gs is not None else -1
    if expected_generation is not None and (
        existing_gs is None or existing_gs.generation != expected_generation
    ):
        raise ConfigConflict(
            expected_generation,
            None if existing_gs is None else existing_gs.generation,
        )

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
    # Order: retention_rules -> dataset_remotes -> datasets -> destinations ->
    # global_settings -> remote_server (`CONFIG_TABLES`). NEVER widen this to
    # `host_identity` or a trust table: those are not config, and an import
    # must keep the client id and every pinned certificate (see the comment on
    # `CONFIG_TABLES`).
    for model in _CONFIG_MODELS:
        session.execute(delete(model))
        session.flush()

    # --- 5. Insert destinations, then datasets (with their rules/remotes). ---
    for name, dest in config.destinations.items():
        session.add(models.Destination(
            name=name,
            url=dest.url,
            server_fingerprint=dest.server_fingerprint,
        ))
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
                # None (auto) stays NULL; True/False stay 1/0.
                raw=remote.raw,
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
        # The client id lives in `host_identity`, which this function never
        # touches (plan D2).
        client_cert_file=(
            None if config.client_cert_file is None else str(config.client_cert_file)
        ),
        client_key_file=(
            None if config.client_key_file is None else str(config.client_key_file)
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

    # --- Stamp the store id (item 17.0). ---
    # Last, after every row: the DELETEs above have opened the transaction
    # even on a legacy-mode (in-memory) connection, so the pragma is part of
    # it and a rollback restores 0. `PRAGMA user_version` takes no bound
    # parameter; the value is an int WE generated, formatted in.
    if stored_store_id == 0:
        new_id = secrets.randbelow(_STORE_ID_BOUND) + 1
        session.connection().exec_driver_sql(f"PRAGMA user_version = {new_id}")
    return previous_generation + 1

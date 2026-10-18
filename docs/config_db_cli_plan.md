# zfsbackup: SQLite Config Store + Config CLI + Live Reload

**Status (2026-10-18):** Phases 0, 1 and 2 **implemented** on branch `config-db-cli` (Checkpoint A held
2026-10-17 with fix-up A-1…A-21; Phase 2 items 10–16 landed 2026-10-17/18); mocked suite 3810 passed.
**Checkpoint B review held 2026-10-18** (Phase 2 sound; close-out fixes landed). **Phase 3 (live
reload) decided 2026-10-18 (D1–D11; D4 changed to "the CLI SIGHUPs the daemon") and in implementation.** Sections below still read as the original proposal except where an item
carries a status note or "as landed" text — where those disagree with the proposal, the note wins.
**Scope:** `zfsbackup/` only. No `libzfseasy/` changes.
**Author:** zfsbackup-implementation-planner

### Progress

| Item | State | Notes |
|---|---|---|
| 1 Dependencies | done | `sqlalchemy`, `alembic`, `click`, `requests` declared |
| 2 `Duration` | done | |
| 2b `keep_for` collapse | done | |
| 3b Per-destination retention | done | |
| 3 Schema | done | `retention_rules` shape superseded during implementation — see `models.py` |
| 4 Mapper | done | |
| 3c Representable "unset" | done | Added after item 4 |
| 7 Alembic | done | "As landed" text in item 7 |
| 5 Engine/session, fork safety | done | |
| 6a Path resolution, permissions | done | 6b (CLI wiring) folds into item 10 |
| 8 DB canonical | done, **with carry-overs** | See item 8's status note |
| 8x `StopFlag` | done | Found during item 8 |
| 9 Phase 1 tests | landed with 8 | Coverage audit against item 9's list is part of Checkpoint A |
| Checkpoint A review | done | Findings, decisions and audit under "Checkpoint A" |
| A-1…A-8, A-10…A-20 fix-up | done | Reviewed clean by both reviewers; mocked suite 1557 passed |
| A-21 follow-up | done | Five low items from the re-review, plus a corrected fingerprint placement found by the concurrency re-review |
| A-9 scenarios | done | Two-VM harness 15/15 on the DB-backed config; `test_config_permissions.sh` 61/61 on real Linux. Startup exits 1 (not 78) on a sidecar permission error; 78 is the worker code |
| Item 10.1 + 10.2 | done (cfa9d54) | `zfsbackup-config import/export/migrate`; reviewed by both reviewers |
| Items 10.4–10.6 review fixes | done | Foreign-DB refusal, exit-code contract, ^C/SIGTERM/backup hygiene, connect-failure fd leak |
| Item 10.3 | done | Interim `python -m zfsbackup.config.store.importer` deleted; scenarios and docs use `zfsbackup-config import` |
| Item 15 (15.1–15.5) + 14.1 | done | See the "As landed" note under item 15 |
| Items 13, 11, 12, 14.2 | done | `list`, `get`/`set`/`unset`, `edit` (whole + per entry), `rename dataset|destination`; mocked suite 3787 |
| Warnings consolidation | done | `validate_config` is the single source of semantic warnings; `from_file` keeps parse-boundary ones; `load_config(warn_never_prune=)` |
| Checkpoint B | held 2026-10-18 | Phase 2 reviewed end to end (incl. a 5-minute concurrent stress test); close-out fixes landed with it |
| **Phase 3 (live reload)** | **in progress** | Decisions D1–D11 recorded 2026-10-18; revised items 17.0–23 below |
| 10–16 Config CLI | done | Landed in order 10 → 15 → 13 → 11 → 12 → 14 → 16; see the per-item "as landed" notes |
| 17–20 Live reload | in progress | Lanes: store 17.0 → G1; daemon 18.0 → 17.1 → 18.1–18.5 → G2 → 17.2 → G3; CLI 19.x → G4 |
| 21–23 Docs, scenarios, final review | not started | |

> *(Historical — written for Phases 0–2; the reload design described here is superseded by the revised Phase 3 section.)* **All design questions are settled; no open decisions remain.** Items 2 (duration handling) and 5 (fork
> safety) were the gating pair: **item 2 = full `Duration` value type** subclassing `timedelta`, with
> `__str__` left untouched and `.literal`/`.timedelta` as read-only accessors; **item 5 = pid-keyed,
> created-after-fork engine cache**. Reload uses an event-based immediate wake (item 17) and the CLI
> auto-reloads by default (item 19). See "For your approval" at the end for the seven recorded decisions
> and what still needs sign-off at each checkpoint.

> **Library findings independently verified.** The duration-library table in item 2 was re-run from
> scratch in a clean venv by the main session and reproduces exactly, cell for cell — including
> `pytimeparse` and `humanfriendly` both parsing `1M` as 60 seconds.

**Three phases, three checkpoints.** Phase 1 = SQLite store. Phase 2 = config CLI. Phase 3 = live reload
(promoted from the deferral list at user request, structured as its own phase because it touches
multiprocessing, signals, and IPC and deserves an independent review pass).

---

## Context established from source

These facts, read from the code rather than the docs, shape every item below.

- **Workers already load config independently, after fork.** `zfsbackup/workers.py:62` and
  `zfsbackup/workers.py:196` each call `BackupConfig.from_file(self.config_path)` inside `run()`. The
  supervisor passes only a `Path` across the process boundary (`zfsbackup/daemon.py:54`), never a
  `BackupConfig`. This existing contract is already "connect-after-fork" — exactly what SQLite needs.
- **Config objects outlive any plausible DB session.** `create_app(config)` holds config for the process
  lifetime (`zfsbackup/api.py:20-26`), `RemoteBackupManager` holds it (`zfsbackup/remote.py:56`),
  `DatasetManager` holds it (`zfsbackup/backup_manager.py:63`).
- **`DatasetConfig` is also a wire format.** `to_property()`/`from_property()`
  (`zfsbackup/config/model.py:88-132`) base64-JSON-encode it into the ZFS user property
  `org.zfsbackup:config`, and it is transmitted to remote servers (`zfsbackup/remote.py:108`).
  Instances are reconstructed from remote data with no DB backing and **no duration literals**.
- **Retention is a YAML mapping, not a list** (`zfsbackup/config/model.py:148-155`): keys are unique age
  strings, sorted by age via `retention_rules.sort(key=lambda r: r.age)` at `config/model.py:155`.
- **Workers load config once and never re-read it** (`workers.py:62`, loop begins `workers.py:73`).
  This is the fact Phase 3 exists to address.
- **Dependency reality:** `sqlalchemy`, `alembic`, `typer` are not installed. `click` 8.3.3 **is**
  installed but only transitively via Flask (`flask >=3.1.3` → `click >=8.1.3`); it is not in
  `pyproject.toml`. `requests` is importable but undeclared.

### Two incidental findings (re-flagged)

1. **`CLAUDE.md` documents a test file that does not exist.** It references `zfsbackup/test_basic.py`;
   `ls zfsbackup/` shows no such file. All `zfsbackup` tests live in `tests/test_zfsbackup_*.py`.
   Corrected in item 21.
2. **`click` is relied upon transitively.** Building the CLI on an undeclared transitive Flask
   dependency is precisely the failure mode recorded in `docs/production_readiness_report.md` and noted
   in `CLAUDE.md` about `requests`. Item 1 promotes it to a direct dependency.

### A third finding, surfaced while designing Phase 3

**Bad config produces an infinite restart loop today.** `workers.py:66-67` makes a worker `return`
(exit cleanly) when config loading fails. `_check_workers` (`daemon.py:69-80`) sees a dead worker and
respawns it unconditionally every `WORKER_RESTART_DELAY` seconds. A malformed config therefore yields a
silent, permanent respawn loop with no backoff and no escalation. This is pre-existing and unrelated to
the requested features; item 17 fixes it because live reload would otherwise make it much easier to
trigger.

---

## Recommended architecture

**Keep the dataclasses as the runtime domain model; add a separate SQLAlchemy ORM layer plus an explicit
mapper.** New package `zfsbackup/config/store/` with `models.py` (declarative ORM rows), `db.py`
(engine/session), `mapper.py` (ORM ⇄ dataclass conversion). `BackupConfig`/`DatasetConfig` stay plain
dataclasses and remain the only types any consumer sees.

Justification, tied to source:

1. `DatasetConfig.from_property()` (`config/model.py:110`) constructs instances from *remote server data* with
   no DB behind them. If `DatasetConfig` were a declarative model, the same class would be
   simultaneously persistent, transient, and detached depending on origin — a session-identity hazard,
   and `session.add()` of a remote-derived object would silently persist foreign config.
2. Config outlives the load session (`api.py:20`, `remote.py:56`). Detached ORM instances would raise
   `DetachedInstanceError` on `config.retention_rules` access — lazily, inside a worker loop, rather
   than at load time.
3. Eight test files construct `BackupConfig(...)` directly with no DB. A pure-dataclass domain model
   keeps every one of them working unchanged.

**Alternative considered and rejected:** imperative mapping (`registry.map_imperatively`) onto the
existing dataclasses. Avoids the extra layer, but does not solve (1) or (2) — the same class is still
both persisted and transient.

**Migrations: Alembic.** A hand-rolled version table is tempting for a schema this small, but the config
schema will churn (every new config key is a column), and Alembic's `batch_alter_table` is the only sane
way to `ALTER` on SQLite, which does not support most in-place column changes.

---

## Phase 0 — Prerequisites

### Item 1 — Declare dependencies in `pyproject.toml`

- **Owner:** `zfsbackup-developer` · **Tag:** `low-risk`
- **Files:** `pyproject.toml`
- **Basis:** `pyproject.toml:9-16` lists no `sqlalchemy`, `alembic`, or `click`.
- **Changes** — add to `[tool.poetry.dependencies]`:
  - `sqlalchemy = "^2.0"` (Feature 1; 2.0 typed `Mapped[]` API)
  - `alembic = "^1.13"` (Feature 1 migrations)
  - `click = "^8.1"` (Feature 2 — promote from transitive to direct)
  - `requests = "^2.31"` — **pre-existing gap**, not caused by this work; listed separately so it can be
    accepted or rejected independently.
- **No duration library is added.** See item 2 for the empirical justification.
- **Verification:** `poetry lock && poetry install --with dev`; `pytest tests/test_zfsbackup_*.py` green.
- **CLI framework note:** `click` over `typer` — click is already in the resolved dependency tree via
  Flask, so this adds zero new transitive weight. Argparse (already used at `daemon.py:120`) works but
  has no ergonomic subcommand grouping for the ~10-command surface in Phase 2.

---

### Item 2 — `Duration` value type (full type, per user decision)

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(config round-trip format)*
- **Files:** `zfsbackup/config/model.py`
- **Basis:** `zfsbackup/config/model.py:12-45`. `parse_time_duration("1M")` and `parse_time_duration("30d")`
  both return `timedelta(days=30)`; `1y` and `365d` both return `timedelta(days=365)`. Verified:

  ```
     1M -> 30 days, 0:00:00   total_seconds=2592000.0
    30d -> 30 days, 0:00:00   total_seconds=2592000.0
     1y -> 365 days, 0:00:00  total_seconds=31536000.0
   365d -> 365 days, 0:00:00  total_seconds=31536000.0
  COLLISION 1M == 30d : True
  COLLISION 1y == 365d: True
  ```

  Without literal preservation, a user who wrote `"1M": "1y"` (`config.example.yaml:38-40`) gets
  `"30d": "365d"` back after a Phase 2 round-trip — a silently destructive edit.

#### Library evaluation — measured, not assumed

The user asked whether an existing package can render a `timedelta` to a string and parse it back.
The four realistic candidates were installed and tested against this repo's actual format. **None works,
and two are actively dangerous.** (Table independently re-verified in a clean venv.)

| Input | `pytimeparse` | `humanfriendly` (parse → render) | `durationpy` (parse → render) | `isodate` |
|---|---|---|---|---|
| `5m` | 300 ✓ | 300.0 → `'5 minutes'` | `0:05:00` → `'5m'` ✓ | rejects |
| `1h` | 3600 ✓ | 3600.0 → `'1 hour'` | `1:00:00` → `'1h'` ✓ | rejects |
| `1d` | 86400 ✓ | 86400.0 → `'1 day'` | `1 day` → `'24h'` | rejects |
| `1w` | 604800 ✓ | 604800.0 → `'1 week'` | `7 days` → `'168h'` | rejects |
| **`1M`** | **60 ⚠ (one minute)** | **60.0 → `'1 minute'` ⚠** | `DurationError` | rejects |
| `30d` | 2592000 ✓ | 2592000.0 → `'4 weeks and 2 days'` | `30 days` → `'720h'` | rejects |
| **`1y`** | **`None` ⚠** | **31449600 ⚠ (≠ 31536000)** | `365 days` → `'8760h'` | rejects |
| `10y` | `None` ⚠ | 314496000 ⚠ | `3650 days` → `'87600h'` | rejects |
| `365d` | 31536000 ✓ | 31536000 → `'1 year and 1 day'` | `365 days` → `'8760h'` | rejects |

Findings:

- **`pytimeparse` and `humanfriendly` silently mis-parse `1M` as one minute.** They are
  case-insensitive, so the `m` = minutes / `M` = months collision that this repo depends on is invisible
  to them. In a retention rule, `"1M": "1y"` would become "keep for 1 minute" — snapshots pruned
  essentially immediately. This is a catastrophic silent-data-loss class of bug, not a formatting
  inconvenience.
- **`humanfriendly` also disagrees on `1y`**: 31449600s (52 weeks) vs this repo's 31536000s (365 days),
  and renders prose (`'4 weeks and 2 days'`) that is not re-parseable into the repo's format.
- **`durationpy` rejects `1M` outright** and canonicalizes everything to hours on render (`1d` → `24h`),
  so it cannot round-trip even the cases it parses.
- **`isodate` implements ISO-8601 only** (`PT5M`, `P1M`) and rejects every native literal. It is the one
  library that *does* preserve `P1M` vs `P30D` distinctly — but only by adopting a completely different
  user-facing syntax, which would break `config.example.yaml`, `config.test.yaml`, and every existing
  user config.

**Recommendation: hand-roll a small parse/render pair. Add no dependency, not even for the parse
direction.** The parse direction is exactly where the `M`/`m` collision bites, and `parse_time_duration`
already handles it correctly and is already covered by `tests/test_zfsbackup_config.py`. A library
would replace working, tested code with something measurably wrong. The render direction is ~15 lines.
No candidate earns its place.

#### Implementation: subclass `timedelta`, do not wrap it

**`Duration` must subclass `datetime.timedelta`, not wrap it.** This is the single most important
implementation decision in the item. `timedelta` is immutable and subclassable via `__new__`;
subclassing means every arithmetic operation, every comparison (including reflected comparison),
truthiness, hashing, and `total_seconds()` work natively and correctly with zero new code. A wrapper
class would have to reimplement all of them and would break on reflected operations in ways that are
easy to miss.

```
class Duration(timedelta):
    literal: str | None     # None for programmatically-constructed values
```

**Public API — decided.** `Duration` exposes the literal and the plain value as explicit read-only
accessors, and does **not** override `__str__`:

- **`Duration(...)`** — the single construction entry point *(user decision)*, accepting **three** forms:

  | Form | Example | `literal` |
  |---|---|---|
  | Literal string, single or compound | `Duration("1M")`, `Duration("6d12h")` | as written |
  | Existing `timedelta` | `Duration(timedelta(hours=1))` | synthesized — `"1h"` |
  | **Native `timedelta` kwargs** | `Duration(days=6, hours=12)` | synthesized — `"6d12h"` |

  The third form keeps `Duration` a **drop-in replacement for `timedelta`** — the full original signature
  (`days, seconds, microseconds, milliseconds, minutes, hours, weeks`) is delegated straight to
  `timedelta.__new__`. There is deliberately **no separate `Duration.parse()`** — one operation, one entry
  point, so the two cannot drift.

  **Dispatch is unambiguous:** `timedelta`'s first positional parameter is `days`, a number, so a single
  positional `str` or `timedelta` can never collide with the native signature. `Duration(30)` means
  `days=30`, exactly as `timedelta(30)` does today.
- **`Duration._parse(s)` — private** *(user decision)*. The parsing logic currently at
  `config/model.py:12-45` moves into `Duration` as a private static method **and is rewritten** to support
  compound literals (see below). Single-term behaviour and every existing error case are preserved; the
  vestigial leading-`m` branch is dropped. This makes `Duration` self-contained: the type owns both
  directions of its own conversion.
- **`Duration.literal -> str | None`** — read-only property returning the source literal exactly as
  written (`"1M"`, `"6d12h"`), or the **synthesized** best-effort literal for values built from a
  `timedelta` or native kwargs. It is `None` only for values with sub-second precision, which have no
  representable form.
- **`Duration.timedelta -> timedelta`** — read-only property returning a **pure `timedelta`** with the
  same value and no literal attached. This is the explicit way to shed the subclass: use it wherever a
  value is serialized, compared for exact type, or handed to code that must not see a `Duration`.
- `Duration.render() -> str` — returns `literal`, which is populated for every value the grammar can
  express, because the constructor synthesizes one (greedy largest-unit-first) whenever none was
  supplied. **If `literal` is `None` it raises `ValueError`.** `literal is None` happens only for
  sub-second values, which have no representable form — see the sub-second bullet below. This is what
  the YAML writer calls; nothing else needs it.

  > *Corrected after the Phase 0 review, which caught this bullet contradicting the sub-second rule
  > below. An earlier draft said `render()` fell back to a canonical formatter when `literal` was unset —
  > but synthesis means `literal` is unset only for sub-second values, which is exactly the case that must
  > raise. Reading the old wording, one would conclude item 12's Phase 2 blocker did not exist.*

#### Compound literals *(user decision — this is a parser extension, not a relocation)*

`Duration` must accept **multi-term literals**: `6d12h`, `1h30m`, `1y2M3d`, `1w2d`. The current parser is
strictly single-term — `value = int(duration_str[:-1])` at `config/model.py:26` takes everything but the last
character as the number — so this is genuinely new capability and the parser is **rewritten, not moved**.

**Grammar:** an optional leading `-`, then one or more `(integer)(unit)` terms with no separators and no
whitespace. The value is the sum of the terms.

- **Unit case-sensitivity must be preserved exactly as today**, and it is not uniform:

  | Unit | Meaning | Case |
  |---|---|---|
  | `s` | seconds | insensitive (`s`/`S`) |
  | **`m`** | **minutes** | **sensitive** |
  | **`M`** | **months (30d)** | **sensitive** |
  | `h` | hours | insensitive (`h`/`H`) |
  | `d` | days | insensitive |
  | `w` | weeks | insensitive |
  | `y` | years (365d) | insensitive |

  Verified against the running parser: `5m` → 5 minutes, `5M` → 150 days, `2H` → 2 hours. **`m`/`M` is
  the collision that defeated every third-party library** (see the table above) — a compound parser that
  lowercases the unit before dispatch reintroduces exactly that bug, in a codebase that currently does
  not have it.
- **Negative durations stay supported.** `parse_time_duration("-5d")` returns `-5 days` today. The sign is
  **whole-duration only** — a leading `-` negates the entire sum. Per-term signs (`6d-12h`) are rejected;
  they are almost certainly a typo, and their meaning is not obvious.
- **Duplicate units are rejected** (`1d1d`, `1h30m15m`) — recommended, because the plausible cause is a
  typo for a different unit and silently summing hides it. **Term order is not constrained** (`12h6d` is
  legal and equals `6d12h`); enforcing descending order buys nothing and rejects harmless input.
- **All existing error cases must still raise `ValueError`:** `""`, `"xh"`, `"5z"`, `"M5"`, `"mi5"`,
  `"mi"`. A full-string anchored match satisfies every one of these naturally.
- **The vestigial leading-`m` branch disappears, and that is safe.** Lines `config/model.py:17-23` compute a
  `unit` that is then always discarded, because `int(duration_str[:-1])` raises for every input that
  reaches the branch — verified: `m5`, `M5`, `mi5`, and `mi` all raise `Invalid duration format`. The two
  tests covering it (`tests/test_zfsbackup_config.py:60-76`) assert only that a `ValueError` is raised,
  which an anchored compound grammar also does. **Delete the branch; keep both tests passing unmodified.**

#### Literal synthesis *(user decision — best-effort literal when none was supplied)*

When a `Duration` is constructed without a literal — from a `timedelta` or from native kwargs — it
**synthesizes one** rather than leaving `literal` as `None`.

- **Algorithm:** greedy, largest-unit-first over `y(365d) → M(30d) → w(7d) → d → h → m → s`, emitting only
  non-zero terms. `Duration(days=6, hours=12)` → `"6d12h"`; `Duration(days=400)` → `"1y1M5d"`;
  `Duration(hours=36)` → `"1d12h"`.
- **Synthesis is exact, never approximate.** Every unit is a whole number of seconds and `M`/`y` are exact
  multiples of days in this scheme, so the synthesized literal always re-parses to the identical value.
  `"1y1M5d"` = 365 + 30 + 5 = 400 days exactly. Verify this as a property, not a table: for any whole-second
  value `v`, `Duration(Duration(v).literal) == v`.
- **Determinism matters.** The same value must always synthesize the same literal, or item 12's
  "content byte-identical → no changes" check would report spurious diffs on an untouched config. Greedy
  decomposition is deterministic.
- **Zero** → `"0s"`. **Negative** → leading `-` on the greedy decomposition of the absolute value.
- **Sub-second values cannot be expressed.** The grammar has no sub-second unit and YAML configs have no
  syntax for one. Construction stays permissive so `Duration` remains a drop-in `timedelta`
  (`Duration(microseconds=1)` is legal, `.literal` is `None`), but **`render()` raises `ValueError` naming
  the value** rather than silently truncating — truncation in a retention rule is a data-loss bug. Nothing
  in `zfsbackup/` produces sub-second durations, so this is a corner case that only needs to be *defined*.

  > **⚠ Phase 2 must handle this on the remote path** *(found in the Phase 0 review)*. `from_property`
  > (`config/model.py:110-132`) feeds `total_seconds()` **floats** off the wire into `Duration(seconds=...)`.
  > Whole-second values are fine, but a remote peer sending `"frequency": 0.5` produces a `Duration` with
  > `literal is None` — harmless today because nothing calls `render()`, but **Phase 2's `export` and
  > `edit` would crash on remote-derived config**. Before item 12 lands, either round to whole seconds at
  > `from_property` or reject fractional seconds there with an explicit error. Tracked here rather than
  > fixed in Phase 0, since no current code path can reach it.

- **Input tolerance narrowed relative to the old parser** *(noted in the Phase 0 review — accepted as
  deliberate)*. The anchored `fullmatch` rejects three forms the old `int()`-based parser accepted:
  `" 1h"` (leading whitespace), `"+5d"` (explicit plus), and `"1_0d"` (PEP 515 digit separator, which
  `int()` honours). None appears in any shipped config, YAML strips unquoted leading whitespace anyway,
  and rejecting them is the better behaviour for a config grammar. Recorded so the narrowing is a
  decision rather than an accident.
- **⚠ Consequence — `.literal` no longer means "the user wrote this".** After synthesis it is populated for
  essentially every value, so it answers "what text represents this duration", not "what did the config
  say". Nothing in this plan needs the provenance distinction; if a later feature does, it needs a separate
  flag rather than an `is None` check.
- **Upside for the remote path:** `from_property()` (`config/model.py:110-132`) rebuilds durations from
  `timedelta(seconds=...)` with no literal available. Synthesis means remote-derived configs now render as
  `1M` rather than failing or emitting a raw timedelta — a real improvement over the `literal=None` design.
- **Deliberate and acceptable:** a *reconstructed* 30-day value synthesizes `"1M"`, not `"30d"`. Values
  parsed from a literal keep their original text, so a user's `30d` is never rewritten — only values that
  never had a literal in the first place get the greedy form.

**⚠ `parse_time_duration` cannot simply disappear — it is public API with live consumers.** It is
imported and asserted against **22 times across two test files** (`tests/test_zfsbackup_config.py:8` and
`tests/test_zfsbackup_basic.py:9`), including the deliberate leading-`m` edge-case coverage at
`tests/test_zfsbackup_config.py:60-76`. **Recommended: keep `parse_time_duration` as a thin public
module-level shim** in `config/model.py` that delegates to `Duration._parse` and returns a plain `timedelta`:

```
def parse_time_duration(duration_str: str) -> timedelta:
    """Backwards-compatible shim; parsing logic now lives in Duration."""
    return Duration(duration_str).timedelta
```

This satisfies the intent — the logic genuinely lives in `Duration`, and nothing in `zfsbackup/`
calls the shim — while leaving all 22 existing assertions passing unmodified. The alternative (delete it
outright) forces `pytest-test-author` to rewrite both files in the same cycle, and would lose the
leading-`m` edge-case tests unless they are consciously ported. **Confirm which you want.**

**Benefit of the constructor change worth noting:** the six internal parse sites at `config/model.py:145`,
`config/model.py:152-153`, `config/model.py:162`, `config/model.py:212`, and `config/model.py:215` become `Duration(...)`
construction directly, so every duration loaded by `from_file` carries its literal with no separate
wiring step. The `field(default_factory=...)` defaults at `config/model.py:83`, `config/model.py:180`, and
`config/model.py:181` become `Duration("1h")`, `Duration("5m")`, `Duration("1h")` respectively.

**Details that must be got right:**

- `__new__` override to accept and attach `literal`; `timedelta` is immutable so `__init__` cannot set it.
  It must dispatch on the three forms above and delegate the native-kwargs path unchanged to
  `timedelta.__new__`.
- **⚠ Pickling — the failure mode is now silent, which makes `__reduce__` more important, not less.**
  A `timedelta` subclass with extra state needs `__reduce__`, or `literal` is lost across pickle and
  `copy.deepcopy`. Relevant because `BackupConfig` is held on `BackupDaemon` (`daemon.py:34`) and
  dataclasses are compared in tests. **Note the interaction with native-kwargs support:** had the
  constructor accepted *only* a literal or `timedelta`, a missing `__reduce__` would crash loudly —
  `timedelta.__reduce__` reconstructs via `(cls, (days, seconds, microseconds))`, which that narrower
  signature would reject with a `TypeError`. Because `Duration` now *accepts* the numeric form, that same
  path **succeeds**, and — because literal synthesis then kicks in — returns a `Duration` whose literal has
  been **silently replaced by the canonical form**. That is subtler than losing it outright: literals that
  are already canonical (`1M`, `1h`) survive the round trip untouched and look fine, while every
  non-canonical one is quietly rewritten — `30d` → `1M`, `365d` → `1y`, `24h` → `1d`, `12h6d` → `6d12h`.
  A bug that corrupts only *some* values while leaving the obvious test cases passing is the worst kind, so
  the item-9 pickle/deepcopy assertion must use a **non-canonical** literal (`30d`) — asserting on `1M`
  would pass even with `__reduce__` missing entirely.
- **Arithmetic results are plain `timedelta`.** `Duration + timedelta` returns `timedelta`, because
  `timedelta.__add__` constructs the base class. This is correct and desirable — the result of
  arithmetic has no meaningful literal. Do not fight it.
- **`__str__` is NOT overridden** *(user decision)*. It stays `timedelta.__str__`, so
  `str(Duration("1M"))` remains `"30 days, 0:00:00"`. **Log output at `backup_manager.py:97`,
  `backup_manager.py:101`, and `backup_manager.py:208` is therefore byte-identical to today** — no
  user-visible log change, and nothing grepping those lines breaks. The literal is reachable only via the
  explicit `.literal` property or `.render()`.
- **`__repr__`** stays informative: `Duration('1M', 30 days, 0:00:00)`. Debug-only; no consumer parses it.
- **Consequence to keep in mind:** because `__str__` is unchanged, any place that *should* show the
  literal must ask for it explicitly. The YAML writer (items 12/14) and `list` output (item 13) are the
  two that must, and item 16 asserts it. A future f-string that forgets will silently print
  `30 days, 0:00:00` rather than `1M` — annoying, but never wrong, which is the point of this choice.

#### Complete consumer surface that must keep working

Every operation performed on these values today, with the exact call site:

| Operation | Site | Requirement |
|---|---|---|
| Comparison against bare `timedelta` | `backup_manager.py:47` — `self.config.frequency <= timedelta(0)` | Direct comparison |
| `.total_seconds()` | `backup_manager.py:49` | Inherited |
| Integer division on seconds | `backup_manager.py:53` — `(timestamp // int(interval)) * int(interval)` | Operates on float, unaffected |
| **Reflected comparison** | `backup_manager.py:205-206` — `age >= dsi.frequency`, where `age` is a **bare `timedelta`** from `SnapshotInfo.age` (`backup_manager.py:346`) | `timedelta.__ge__(Duration)` must resolve. Subclassing makes this work natively; a wrapper would return `NotImplemented` and fall through to a reflected call that must be hand-written. **This is the case that kills the wrapper design.** |
| `.total_seconds()` as dict key | `backup_manager.py:217` — `{rule.keep_for.total_seconds(): rule.age.total_seconds()}` | Floats, unaffected |
| f-string interpolation | `backup_manager.py:97,101,208` | `__str__` **not overridden** — output byte-identical to today. Assert this in item 9 |
| Sorting | `config/model.py:155` — `retention_rules.sort(key=lambda r: r.age)` | `__lt__` between `Duration`s |
| `.total_seconds()` for JSON | `config/model.py:93,96,102` (`to_property`) | Inherited; **output must be byte-identical** |
| Dataclass default | `config/model.py:83` — `field(default_factory=lambda: timedelta(hours=1))` | Becomes `Duration('1h')` |
| `.total_seconds()` | `workers.py:100` (`check_interval`), `workers.py:122` (`prune_interval`) | Inherited |
| **Truthiness in `or`** | `workers.py:146-150` — `(r.frequency or ds.frequency).total_seconds()` | `__bool__`. `timedelta(0)` is falsy; `Duration` wrapping zero **must remain falsy** to preserve today's behaviour. Inherited correctly by subclassing. |
| **Truthiness in `or`** | `workers.py:161` — `(remote_cfg.frequency or dsi.config.frequency)` | Same |
| Comparison against float | `workers.py:166` — `anchor.age.total_seconds() < freq` | Operates on floats |
| Equality in tests | 8 test files constructing `BackupConfig(...)` | `__eq__` inherited; `Duration('1M') == timedelta(days=30)` is `True`, which is correct — literal is metadata, not identity |
| Hashability | implied by dataclass/dict use | Inherited |

- **Edge cases to preserve:** the leading-`m` branch at `config/model.py:17-23` is dead code and is deleted by
  the compound-parser rewrite; its two tests assert only `ValueError` and must keep passing.
  **Negative durations (`-5d`) must keep working** — they parse today. `s` (seconds) is accepted at `config/model.py:30` but undocumented in
  `config.example.yaml:84-90`; keep accepting it. `from_property()` (`config/model.py:110-132`) reconstructs
  durations from remote JSON with **no literal available** — those `Duration`s synthesize one from the
  value and must render without error.
- **Verification:** `pytest tests/test_zfsbackup_config.py tests/test_zfsbackup_basic.py` — **with the
  recommended shim, all 22 existing `parse_time_duration` assertions pass unmodified**; without it, both
  files must be rewritten by `pytest-test-author` in this same cycle and the leading-`m` edge cases at
  `tests/test_zfsbackup_config.py:60-76` consciously ported. Plus
  `pytest tests/test_zfsbackup_manager.py tests/test_zfsbackup_workers.py` to exercise the full
  consumer surface. New round-trip table in item 9.

---

## Phase 1 — SQLite config store

### Item 2b — Retention `keep_for` collapse: reject on write, min + warn on read

Phase 1 opens with this because it is a **pre-existing data-loss bug**, independent of the DB work, and
item 3's `UNIQUE(dataset_id, keep_for_seconds)` is the schema half of the same decision. It can land and
ship on its own.

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(retention logic)* — **approved by the
  user**; the strategy below is settled, the implementation still gets a review pass.
- **Files:** `zfsbackup/backup_manager.py`, `zfsbackup/config/model.py`
- **Basis:** `backup_manager.py:216-219` builds `plan = {keep_for: age}` by dict comprehension, so two
  rules sharing a `keep_for` collapse to whichever is written last — **silently**, with no error and no
  log line.

**Why this matters more than the contrived `1M`/`30d` case.** The realistic trigger is mixed notation:

```yaml
retention:
  "1h": "7d"     # hourly, kept a week
  "1d": "1w"     # daily, kept a week      <- same expiry, 604800s
```
Verified: this yields `{604800.0: 86400.0}` — **one tier**. The hourly rule is gone; you asked for hourly
granularity for a week and get daily.

**The tie-break is systematically the worst available.** `config/model.py:155` sorts rules by `age` **ascending**,
so the dict comprehension's last write is always the **largest interval**. On collision the code reliably
discards the finer tier and keeps the coarser one — pruning *more* aggressively than either rule alone
implies. Confirmed across all measured collision cases.

**Strategy — strict on write, lenient but loud on read** *(user decision)*:

1. **Reject on write.** Two rules with equal `keep_for` is a config error. Raise `ValueError` naming both
   rules and both literals (e.g. `retention: "7d" and "1w" are the same duration (604800s); one rule per
   keep_for`). Applies to the Phase 2 CLI `set`/`edit` paths, item 8's `import`, and is enforced
   structurally by item 3's `UNIQUE(dataset_id, keep_for_seconds)`.
2. **Resolve toward retaining more, and warn, on read.** If a collision reaches `needs_prunning` anyway,
   never silently collapse. The governing principle on **both** axes is *keep whichever rule retains more
   data*:

   | collision | resolution | why |
   |---|---|---|
   | same `keep_for`, different `age` | take **min** `age` | finer interval keeps more snapshots, and subsumes the coarser |
   | same `age`, different `keep_for` | take **max** `keep_for` | longer expiry keeps snapshots longer, and subsumes the shorter |

   ```python
   # 1. collapse on keep_for (min interval), 2. then on age (max expiry) -- order is significant
   plan = {}
   for rule in dsi.config.retention_rules:
       expiry, interval = rule.keep_for.total_seconds(), rule.age.total_seconds()
       if expiry in plan and plan[expiry] != interval:
           logger.warning(...)          # dataset, both literals, chosen interval
       plan[expiry] = min(plan.get(expiry, interval), interval)
   ```

   **⚠ The two dedupe passes do not commute — fix the order and test it.** Given
   `{(1d,1w), (1d,1M), (1h,1M)}`: collapsing `age` first yields one rule `{(1h,1M)}`; collapsing
   `keep_for` first yields two, `{(1d,1w), (1h,1M)}`. Specify **`keep_for` first, then `age`** — it is the
   branch that retains more rules, consistent with the principle above. (In this example both outcomes
   retain the same *snapshots*, since hourly-for-a-month subsumes daily-for-a-week, but the rule sets
   differ and determinism matters for the item-3 constraints.)

**Load never hard-fails** *(user decision — settled)*. A daemon running happily on such a config must not
refuse to start after an upgrade; backups silently stopping is worse than a resolved-and-logged ambiguity.
The read path stays permissive and loud; the write paths (CLI, `import`, item 3's constraints) ensure no
*new* config can acquire the defect.

- **Edge cases:** the warning must fire once per dataset per load, not once per snapshot evaluated
  (`needs_prunning` runs every pruning cycle — see `workers.py:122`). Equal `keep_for` **and** equal
  interval is a true duplicate, not a conflict: dedupe silently, no warning.
- **Deliberately unchanged:** the tightest-tier-only semantics of step 3 (each snapshot is claimed by
  exactly one tier, not by every applicable tier). Whether that or znapzend-style union-of-tiers was
  intended is a **separate question** affecting every config, not just colliding ones — out of scope here.
- **Verification:** `pytest tests/test_zfsbackup_manager.py -k retention` — the `7d`/`1w` config keeps the
  hourly tier; the warning fires once and names both literals; equal-interval duplicates are silent;
  non-colliding configs produce a byte-identical plan to today.
- **Tests:** `pytest-test-author`, same cycle.

### Item 3 — Schema + ORM models

- **Owner:** `zfsbackup-developer` · **Tag:** `low-risk` *(new files, nothing wired in yet)*
- **Files:** new `zfsbackup/config/store/__init__.py`, `zfsbackup/config/store/models.py`
- **Basis:** the dataclass set at `config/model.py:48-189`.
- **Schema:**
  - `global_settings` — single row (`CHECK (id = 1)`), mirroring `BackupConfig` scalars
    (`config/model.py:179-189`): `snapshot_prefix`, `check_interval_seconds` + `check_interval_literal`,
    `prune_interval_*`, `api_host`, `api_port`, `dry_run`, `client_id_file`, plus `generation INTEGER`
    (items 15 and 17). **Superseded in part by item 3c:** `prune_interval_seconds` and
    `client_id_file` are nullable, where NULL means "derive at read time".
  - `datasets` — `id`, `name UNIQUE NOT NULL`, `recursive`, `frequency_seconds`, `frequency_literal`,
    `enabled` (`config/model.py:79-86`).
  - `retention_rules` — **superseded during implementation; see `models.py` for the landed shape.**
    This draft scoped rules by a nullable `destination_name FK → destinations(name)`, which review
    rejected: the relationship had `passive_deletes` but no delete cascade, so the ORM disassociated
    children by nulling their FK — and `destination_name IS NULL` is exactly the dataset-level scope
    that governs local pruning. Deleting a destination whose rules had been traversed **promoted its
    overrides to dataset-level**, silently adding a retention tier nobody configured.

    The landed schema scopes by a nullable `dataset_remote_id FK → dataset_remotes`, with a composite
    FK `(dataset_id, dataset_remote_id) → dataset_remotes(dataset_id, id)` making an orphan override
    unrepresentable. NULL still means the dataset-level set. Uniqueness on both axes needs **partial**
    unique indexes (`WHERE dataset_remote_id IS NULL`), because SQLite treats NULLs as distinct and a
    plain `UNIQUE` over the nullable scope column constrains the dataset-level rows not at all.

    > **⚠ SQLite treats NULLs as distinct in UNIQUE constraints**, so `UNIQUE(dataset_id,
    > destination_name, age_seconds)` does **not** constrain the dataset-level rows at all — every
    > `NULL` destination compares unequal to every other. This is the classic nullable-column-in-a-unique-key
    > trap and it silently defeats the item-2b invariant exactly where it matters most (local pruning).
    > Use a **partial unique index** for the NULL case, or store a sentinel (`''`) rather than NULL:
    > ```sql
    > CREATE UNIQUE INDEX ... ON retention_rules(dataset_id, age_seconds)
    >   WHERE destination_name IS NULL;
    > CREATE UNIQUE INDEX ... ON retention_rules(dataset_id, keep_for_seconds)
    >   WHERE destination_name IS NULL;
    > ```
    > Verify with an actual duplicate insert in item 9 — this is not something to take on faith.

    > **Both axes are unique** *(user decision — settled)*. Retention is a **bijection between intervals
    > and expiries**: one rule per `age`, one rule per `keep_for`. Two constraints, not one.
    >
    > `keep_for` uniqueness is what `needs_prunning` structurally requires — `plan[applicable_expiry]`
    > (`backup_manager.py:237`) is a single lookup, so expiry → interval must be a function.
    >
    > `age` uniqueness is a **semantic** requirement rather than a mechanical one: two rules sharing an
    > interval differ only in expiry, and the longer expiry entirely subsumes the shorter.
    > `{"1d": "1w", "24h": "1M"}` is not two tiers — it is "daily for a month", written confusingly, with a
    > redundant clause. The runtime tolerates it today (2 distinct expiry keys, no collapse), which is why
    > it is a config-hygiene rule rather than a bug fix.
    >
    > Earlier drafts of this plan got this wrong twice, recorded so neither is re-proposed: constraining
    > **only** `age` (wrong — it is `keep_for` that the algorithm structurally depends on), and rekeying
    > `plan` on `(keep_for, age)` (wrong on mechanics — a tuple key breaks the `sorted_expiries` scan and
    > the `plan[...]` lookup in step 3). A third draft then argued duplicate `age` was *legitimate* — also
    > wrong, per the subsumption argument above.
  - `destinations` — `name PRIMARY KEY`, `url NOT NULL` (`config/model.py:220-225`).
  - `dataset_remotes` — `id`, `dataset_id FK CASCADE`, `destination_name FK`, `frequency_seconds NULL`,
    `frequency_literal NULL`. NULL frequency = inherit, per `config/model.py:68`.
  - `remote_server` — single row, `target_dataset`, `enabled` (`config/model.py:71-75`).
  - A key/value EAV table is rejected: it defeats typing and makes dot-path coercion guesswork.
- **Edge cases:** `PRAGMA foreign_keys=ON` must be set per-connection (SQLite defaults it off) — item 5.
  No ordering column needed for retention; rules are sorted by age at read time (`config/model.py:155`).
- **Verification:** `pytest tests/test_zfsbackup_store.py -k schema`

### Item 3b — Per-destination retention rules

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(retention + remote/wire behaviour)*
- **Files:** `zfsbackup/config/model.py`, `zfsbackup/remote.py`, `zfsbackup/backup_manager.py`
- **Basis:** user decision. Today `RemoteDatasetConfig` (`config/model.py:66-69`) carries only `destination` and
  an optional `frequency`; retention is dataset-wide. The requirement is that a client can keep, say,
  hourly-for-a-month locally but daily-for-five-years at an offsite destination.

**The transport already supports this with no wire-schema change — this is the key finding.** Config does
**not** travel via `zfs send -p`. The client POSTs it explicitly, **per destination**, at negotiate time
(`remote.py:102-110`), and the server stores the blob verbatim on the received dataset and prunes with it
(`api.py` negotiate handler → `backup_manager.py:188` → `workers.py:125`). Therefore:

- **The receiving machine never needs to identify itself.** It does not need to know it is called
  `offsite` by this client. The client already knows which destination it is negotiating with and sends
  that destination's *effective* rules.
- **`to_property()`'s output shape is unchanged** — still `retention_rules: [{age, keep_for}]`. Only the
  *contents* differ per destination. An existing server understands it as-is, so this is
  **backward compatible with already-deployed servers**, which the plan otherwise treats as frozen.

**Changes:**
- `RemoteDatasetConfig` gains `retention_rules: List[RetentionRule]` (default empty).
- **`to_property(destination: str | None = None)`** — emits the effective rules for that destination.
  `remote.py:108` passes the destination it is negotiating with. `sync_config_property`
  (`backup_manager.py:118`), which describes the *local* dataset, keeps calling it with no argument and so
  emits the dataset-level rules. `from_property` is unchanged.
- Local pruning uses dataset-level rules only (`destination_name IS NULL`).

**Inheritance — override, not merge** *(user decision)*. A destination with no rules of its own inherits
the dataset-level set; a destination that declares **any** rule replaces the set entirely. This is the same
principle the user stated for the receive side — *"if new rules are received, these rules overwrite the
whole ruleset for the received dataset"* — applied consistently at both ends: **whatever ruleset applies to
a scope is complete and replaces what was there**, never merged.

Consequences:
- The client computes the effective ruleset for a destination and sends it complete; the server adopts it
  wholesale, discarding any previous rules for that received dataset. That is what `api.py`'s negotiate
  handler already does (it `zfs set`s `PROP_CONFIG` verbatim), so **the receive side needs no change**.
- A destination override must restate every tier it still wants. Predictable, and it matches the existing
  `frequency` precedent at `config/model.py:68` (`None = inherit from DatasetConfig`).

**Proposed YAML shape** (extends `config.example.yaml:43-47`):
```yaml
    retention:              # dataset-level: local pruning, and the inherited default
      "1h": "1d"
      "1d": "1M"
    remote:
      - destination: offsite
        frequency: "4h"
        retention:          # overrides the above for this destination only
          "1d": "5y"
      - destination: datacenter2
        # no retention key -> inherits the dataset-level set
```

- **Item 2b applies per scope:** uniqueness and the collision fallback are evaluated within each
  `(dataset, destination)` rule set independently, not across the union.
- **Edge cases:** a destination declaring an empty `retention: {}` is **rejected** *(user decision)* — it
  could plausibly mean "inherit" or "keep everything forever", and guessing the second direction fills the
  pool. Error must state both alternatives: omit the key to inherit, or declare rules. Note there is
  consequently **no way to express "never prune at this destination"**; if that is wanted later it needs an
  explicit spelling (`retention: none`), not an empty map. Deleting a
  destination must cascade its retention rows (item 3's FK). A destination referenced with retention but
  never declared in `destinations:` must fail validation.
- **Verification:** `pytest tests/test_zfsbackup_manager.py -k "per_destination or retention"` — a
  destination with rules emits those in `to_property(dest)`; one without emits the dataset-level set;
  local pruning is unaffected by destination rules; `to_property()` with no argument is byte-identical to
  today for a config with no per-destination rules (**wire-compatibility assertion — required**).
- **Tests:** `pytest-test-author`, same cycle.

### Item 3c — Representable "unset" for derived global settings *(added after item 4 landed)*

- **Owner:** **split** — `zfsbackup-store-developer` (3c.1, schema + mapper) → `zfsbackup-developer`
  (3c.2, dataclass + consumers) → `pytest-test-author` (3c.3) → main session (3c.4, docs)
  · **Tag:** `needs-approval` *(schema change + public dataclass shape + config load path)*
- **Not in the original plan.** Added on the user's proposal after item 4 landed, and **must precede
  item 7** — once Alembic baselines the schema, this needs an `ALTER` migration plus a backfill that
  asks an unanswerable question of each existing row ("was this value derived or chosen?").
- **Basis:** two global settings mean *derive at load time* when absent from YAML —
  `prune_interval` (`config/model.py:696`, absent means follow `check_interval`) and `client_id_file`
  (`config/model.py:742-743`, absent means resolve `$HOME` in the process that will use the file).
  `from_file` resolved both eagerly and `models.py:120`/`:125` stored them `NOT NULL`, flattening the
  derivation into whichever process wrote the row.
- **Why it matters.** `zfsbackup-config import` run under `sudo` baked `/root/.config/...` into the
  DB. `ClientIdentity._load_or_generate` (`remote.py:36-43`) does `mkdir(parents=True)` +
  `write_text` on a miss, so a wrong `$HOME` silently **generates a brand-new client ID** — orphaning
  the entire server-side `target/<client_id>/...` dataset tree from the previous identity. Separately,
  a stored `prune_interval` stops tracking a later `check_interval` change.
- **Second defect collapsed.** `config/model.py:644` defaulted `prune_interval` to `1h` for a directly
  constructed `BackupConfig`, while `from_file` with the key absent gave `check_interval` (5m). Two
  disagreeing defaults for one field, with no DB involved. Verified by execution before the change.
- **Changes:** both columns nullable, NULL meaning *derive*, with a `CHECK (prune_interval_seconds
  IS NOT NULL OR prune_interval_literal IS NULL)` for coherence; both dataclass fields `Optional`,
  `None` meaning *derive*; two read-only accessors `BackupConfig.effective_prune_interval` (returns
  the `check_interval` object itself, so the literal carries through) and `effective_client_id_file`
  (**recomputed per call, never memoized** — that is the entire fix); `from_file` stops resolving
  eagerly; `workers.py:122`, `api.py:50` and `remote.py:59` move to the accessors.
- **`check_interval` deliberately stays `NOT NULL`** — its default is the constant `'5m'`, so there
  is nothing to derive from and no second process can change the answer.
- **Edge case:** a present-but-zero `prune_interval: "0m"` must stay distinct from absent. Every
  branch tests `is None`, never truthiness.
- **Downstream amendments** recorded on items 11 and 12: the CLI needs a way to express "unset" and
  must render a derived value distinguishably, and the `$EDITOR` buffer must **omit** a derived key
  rather than render its effective value — otherwise the next save re-freezes the derivation.
- **Verification:** `pytest tests/ -q` — 788 before, 803 after.

### Item 4 — Mapper layer (ORM ⇄ dataclass)

- **Owner:** `zfsbackup-store-developer` · **Tag:** `needs-approval` *(defines the config load contract)*
- **Files:** new `zfsbackup/config/store/mapper.py`
- **Basis:** `BackupConfig.from_file` (`config/model.py:191-252`) is the only load path today; the DB path must
  produce an equivalent object graph or every downstream consumer shifts behaviour.
- **⚠ Do not reuse `from_dict` for the DB→dataclass direction** *(found in the Phase 0 review)*. Phase 0
  added a YAML-boundary type guard that rejects any non-`str` duration, so `from_dict` now raises on a
  value that is already a `Duration` or `timedelta` (`got Duration Duration('1h', 1:00:00)`). That is
  correct for a documented YAML boundary, but it means the mapper — and item 8's importer, if it takes the
  same path — must construct dataclasses directly (`from_property`-style, with typed values) rather than
  routing DB rows back through `from_dict`.
- **Changes:** `load_config(session) -> BackupConfig` and `save_config(session, config)`. `load_config`
  replicates `from_file`'s defaulting exactly: `snapshot_prefix` default `"autosnap"`,
  retention defaulting to `{'1d': '30d'}` when empty, retention sorted by age, and the
  "No datasets configured" error.
  **Corrected during implementation** (line citations above were stale, and two claims were wrong):
  `prune_interval` falling back to `check_interval` and the `client_id_file` default are **not**
  `load_config`'s job. Item 3c makes both columns nullable and both dataclass fields `Optional`, so
  the derivation happens in `BackupConfig.effective_prune_interval` /
  `effective_client_id_file`, in the process that consumes the value.
- **Target behaviour:** `load_config(session_from(yaml_imported(P))) == BackupConfig.from_file(P)` for
  every YAML in the repo — an equivalence property test in item 9.
- **Verification:** `pytest tests/test_zfsbackup_store.py -k mapper`

### Item 5 — Engine/session management, WAL, fork safety *(settled gating decision)*

- **Owner:** `zfsbackup-store-developer` *(run at **opus**; + `concurrency-reviewer`)* · **Tag:** `needs-approval` *(multiprocessing)*
- **Files:** new `zfsbackup/config/store/db.py`
- **Basis:** `daemon.py:62-68` spawns 3-4 `multiprocessing.Process` workers; `workers.py:62` and
  `workers.py:196` each open config inside the child. On Linux the default start method is `fork`, so
  **any engine or connection created in the supervisor before `_start_workers()` would be inherited by
  every child** — shared SQLite file descriptors corrupt the database, they do not merely error.
  Highest-risk item in the plan.
- **Concurrency story** *(corrected during implementation — see the notes after each item):*
  - **Engine created lazily, per-process, never before fork.** Module-level engine cache keyed by
    `(url, readonly, foreign_keys)` with the owning pid stored alongside; if the recorded pid differs
    from `os.getpid()`, discard and rebuild. This makes an accidental pre-fork engine harmless rather
    than catastrophic. **The discard must use `dispose(close=False)`** — a plain `dispose()` in the
    child runs pysqlite's close path, including POSIX advisory-lock release which is process-wide on
    any fd close, against a file the parent believes it owns.
  - ~~**Supervisor holds no open engine while spawning.**~~ **Wrong on two counts.** There is no
    session at `daemon.py:153` — it is `BackupConfig.from_file`, pure YAML; nothing outside
    `zfsbackup/config/store/` imports the store at all yet, so that sentence describes the post-item-8 world.
    And more importantly, **there are two fork points, not one**: `_check_workers` re-forks on every
    worker crash at `daemon.py:77-78`, arbitrarily far into the supervisor's life. A one-time
    dispose-before-`run()` ordering discipline is therefore **structurally insufficient** — only the
    pid-keyed cache is a real guarantee. Daemon-side `dispose_all()` is deferred to item 8, where it
    becomes belt to the cache's braces and must cover **both** fork points.
  - **WAL + pragmas** via a `connect` event listener: `journal_mode=WAL`, `synchronous=NORMAL`,
    `busy_timeout=5000`, `foreign_keys=ON`.
  - **Workers are read-only.** Each reads config once at startup (`workers.py:62`) and never writes it.
    Under WAL, readers never block and are never blocked by the CLI writer, so contention is near-zero
    by construction. Enforce it: workers open the DB through a read-only session factory.
  - **The CLI is the only writer**, a separate short-lived process. Single-writer SQLite is adequate.
- **Edge cases:** `sqlite:///:memory:` must still work for tests (`StaticPool` +
  `check_same_thread=False`); WAL requires a real file, so the pragma listener skips WAL for in-memory
  URLs — though that skip is **cosmetic, not a correctness requirement**: `journal_mode=WAL` on
  `:memory:` returns `('memory',)` with no error.
- **Two measured corrections with consequences for item 6:**
  - **`PRAGMA journal_mode=WAL` succeeds under `query_only=ON`** — `query_only` governs database
    *content*, not file-format state. So a "read-only" engine still opens the file read-write and can
    flip a delete-journal DB to WAL. **The DB file and its containing directory must be writable by
    the daemon user even though only the CLI writes config.**
  - **item 6's "a read-only user cannot open a WAL database at all" is overstated.** Measured: with
    both `-wal` and `-shm` present, a read-only open succeeds even on read-only files in a read-only
    directory. The real rule is narrower and sharper — **a WAL database needs write permission on the
    containing directory whenever the `-shm` must be created**, which is the post-crash and
    first-reader-after-writer-exit case. That is the actual justification for the `0770` directory.
- **Sidecars:** WAL creates `-wal`/`-shm` next to the DB. They are checkpointed away when the last
  connection closes cleanly, so they do not persist in steady state. Consequences flagged for items 6
  and 22: any copy or backup of the DB must take all three files or use `VACUUM INTO` — copying
  `config.db` alone with a pending `-wal` **silently loses the most recent commits** — and `scenarios/`
  cleanup must remove all three, since a stale `-wal` from a killed daemon is a plausible source of a
  confusing "the old config came back" failure.
- **Verification:** `pytest tests/test_zfsbackup_store.py -k "fork or concurren"`

### Item 6 — DB path resolution, creation policy, permissions

- **Owner:** **split** — `zfsbackup-store-developer` (`store/db.py`) → `zfsbackup-developer` (`daemon.py`) → `zfsbackup-cli-developer` (`cli/main.py`) · **Tag:** `needs-approval` *(daemon config-loading contract)*
- **Files:** `zfsbackup/config/store/db.py`, `zfsbackup/daemon.py`, `zfsbackup/cli/main.py`
- **Basis:** `daemon.py:123-128` defaults `-c` to `/etc/zfsbackup/config.yaml`;
  `config.example.yaml:17-18` documents `/var/lib/zfsbackup/` as the daemon state directory.
- **Default DB path: `/var/lib/zfsbackup/config.db`** (per user decision).
- **Resolution order** (first match wins), used identically by daemon and CLI:
  1. Explicit `-c/--config` argument.
  2. `ZFSBACKUP_CONFIG` environment variable — makes the CLI usable in containers and test harnesses
     without repeating `-c`, and `scenarios/` will want it.
  3. `/var/lib/zfsbackup/config.db` if it exists.
  4. Otherwise: error, listing every path tried. **`/etc/zfsbackup/config.yaml` is no longer a daemon
     source** — see item 8; the error message must say so and point at `zfsbackup-config import`.
- **Creation policy — daemon errors, CLI creates only on explicit import.**
  - The **daemon must never auto-create** a config DB. An empty config means "back up nothing", silently.
    `config/model.py:204-205` already errors on no datasets; preserve that posture.
  - The **CLI creates** only via `zfsbackup-config import` (item 8), making directory and file creation an
    explicit, intentional act. All other CLI commands error with a message naming the default path and
    suggesting `import`.

- **Run-as model — dedicated non-root daemon user, `zfsbackup` group for the CLI** *(user decision)*.
  - Daemon runs as user `zfsbackup`, group `zfsbackup`.
  - Directory `/var/lib/zfsbackup/` → `0770 zfsbackup:zfsbackup`; `config.db` → `0660
    zfsbackup:zfsbackup`. **Directory must be group-writable**, not `0750` — see the WAL trap below.
  - Operators who edit config join the `zfsbackup` group. The CLI must produce a clear diagnostic on
    `EACCES` — "you are not in the `zfsbackup` group" — rather than a raw
    `sqlite3.OperationalError: unable to open database file`.
  - **The WAL trap is why the directory is `0770`:** a WAL-mode SQLite *reader* needs write permission on
    **both the database file and its containing directory**, because it may create or update `-wal` and
    `-shm`. A read-only user cannot open a WAL database at all. Document prominently.

  > **⚠ Running the daemon non-root is a real deployment project on Linux, not a permissions tweak.**
  > Flagging it here because the plan cannot deliver it by choosing file modes, and discovering it during
  > Phase 3 would be expensive. Everything the daemon does to ZFS needs delegation via `zfs allow`:
  >
  > | operation | code | delegation needed |
  > |---|---|---|
  > | create snapshots | `SnapshotWorker` | `snapshot` |
  > | prune | `prune_snapshots` | `destroy`, `mount` |
  > | send | `remote.py` | `send` |
  > | receive (server) | `api.py` | `create`, `receive`, `mount` |
  > | config + anchor properties | `backup_manager.py:118,162` (`org.zfsbackup:*`) | `userprop` |
  >
  > **The hard part is `mount`.** On Linux, `zfs allow mount` is necessary but **not sufficient** —
  > `mount(2)` itself is privileged, so a delegated non-root user still cannot mount a filesystem.
  > `destroy` on a mounted dataset and `receive` that mounts both hit this. Practical mitigations:
  > receive with `-u` so the dataset is never mounted, keep received datasets `canmount=noauto`, or grant
  > the daemon `CAP_SYS_ADMIN` — each with its own trade-offs.
  >
  > **Decision — non-root is DEFERRED to its own workstream** *(user decision: `zfs allow` is not currently
  > set up)*. Phase 1 ships the **file layout only**: `zfsbackup:zfsbackup` ownership, `0770` directory,
  > `0660` DB, and group-based CLI access. **The daemon continues to run as root for now.** The permission
  > model above is correct for both cases — only the ZFS delegation differs — so nothing here needs redoing
  > when non-root lands.
  >
  > Tracked in the deferrals table. That workstream owns: the `zfs allow` recipe per operation, the Linux
  > `mount` restriction and which mitigation to adopt, a real-pool scenario proving each delegated
  > operation, and a systemd unit running as `zfsbackup`.

- **Verification:** `pytest tests/test_zfsbackup_store.py -k "resolution or permission"` (permissions via
  `tmp_path` + `chmod`, skipped when running as root). Non-root ZFS delegation is out of scope for pytest
  and belongs in a `scenarios/` script (item 22).

### Item 7 — Alembic scaffolding + initial revision

- **Owner:** `zfsbackup-store-developer` · **Tag:** `low-risk`
- **Files:** new `zfsbackup/config/store/migrations/` (env.py, versions/), `alembic.ini`
- **Basis:** judgment call per the migrations recommendation above; new infrastructure, no existing code.
- **Changes (as landed — this section's original text was superseded during implementation):**
  - **`ensure_schema(connection)`, not `ensure_schema(url)`.** It takes an open `Connection` and never
    constructs an `Engine`. That is what let item 7 land *before* item 5 without colliding with item 5's
    pid-keyed, created-after-fork engine cache. Item 5 later adds the URL-taking wrapper — and **must
    inject a connection from its own cached engine**, because setting `sqlalchemy.url` instead would run
    the migration on a second uncached engine that bypasses every `connect`-event pragma (`foreign_keys`,
    WAL, `busy_timeout`) silently.
  - **Always `upgrade head`, never `create_all` + `stamp`.** The original "stamps a fresh DB at head" is
    ambiguous and, read literally, wrong — `stamp` alone on an empty file records a head with no tables.
  - **The env resolves no application path.** No `-c`, no `ZFSBACKUP_CONFIG` — item 6 owns that, and its
    output reaches the env only as an opened connection. A developer fallback chain (`-x db_url=` →
    `ZFSBACKUP_DB_URL` → `sqlalchemy.url`) exists for the bare CLI; `ZFSBACKUP_DB_URL` is deliberately a
    different name so the two are never conflated.
  - One initial revision creating the landed schema, left byte-for-byte as autogenerated. The
    forward-compatibility guard raises a named error quoting both the DB's revision and the code's head.
  - **No auto-upgrade path and no caller.** Recorded for item 8: the CLI's `import` upgrades; the daemon
    only *checks* and refuses to start on a mismatch. The daemon stays barred from writing, which is what
    item 5's fork-safety and WAL design depend on.
  - **Transaction contract, found in review:** pysqlite emits `BEGIN` before DML but never before DDL, so
    `CREATE TABLE` escapes an unopened transaction by autocommit while `INSERT INTO alembic_version` does
    not. A caller that rolls back after `ensure_schema` can leave a split-brain DB — full schema, empty
    version table — which then fails forever with `table datasets already exists`. **The caller must
    commit immediately after `ensure_schema`, and must not call it inside a wider transaction it may roll
    back.** Binding on item 8.
  - Two defects in the stock Alembic template had to be fixed: it ignores an injected connection (and
    silently migrates whatever `sqlalchemy.url` points at instead), and `fileConfig()` disables every
    `zfsbackup` logger.
- **Verification:** `pytest tests/test_zfsbackup_store.py -k migration`

### Item 8 — DB as canonical config source; YAML demoted to import + edit buffer

- **Owner:** **split** — `zfsbackup-store-developer` (store half) → `zfsbackup-developer` (daemon wiring, leads) *(+ `concurrency-reviewer`)* · **Tag:** `needs-approval` *(daemon config-loading contract + IPC payload)*
- **Files:** `zfsbackup/config/model.py`, `zfsbackup/daemon.py`, `zfsbackup/workers.py`, new
  `zfsbackup/config/store/importer.py`
- **Basis:** `daemon.py:153`, `daemon.py:54`, `workers.py:62`, `workers.py:196`.

- **Policy — the DB is canonical; YAML is not a runtime config source** *(user decision — this reverses an
  earlier draft that kept YAML first-class)*. YAML survives in exactly two roles:
  1. **Import**, for migrating an existing deployment or bootstrapping a new one.
  2. **The transient edit buffer** — the CLI renders YAML from the DB, opens `$EDITOR`, validates on exit,
     and writes back (item 12). The file is scratch, not a source of truth.

  **The daemon reads only the DB.** `python -m zfsbackup.daemon -c config.yaml` stops working.

  > **⚠ This has real churn beyond the daemon, all of which lands in this item's cycle.** The following
  > currently obtain config from YAML and must move to import-then-point-at-DB:
  >
  > | consumer | current | needed |
  > |---|---|---|
  > | `tests/conftest.py:201` | calls `from_file` directly | fixture imports YAML → temp DB |
  > | `tests/test_zfsbackup_real.py:415` | `from_file` on a YAML path | same |
  > | `tests/test_zfsbackup_daemon.py:211,231`, `tests/test_zfsbackup_workers.py:228` | patch `from_file` by name | patch the new loader |
  > | `scenarios/test_two_vm_backup.sh` | writes YAML, starts daemon on it | import step before daemon start |
  > | `config.example.yaml` / `config.test.yaml` | documented daemon inputs | documented **import** inputs |
  >
  > This is the largest single source of churn in Phase 1 and the reason item 8 and item 9 must land
  > together. It is worth doing — a single source of truth is what makes Phase 2's transactional editing
  > and Phase 3's reload coherent — but it should be a conscious cost, not a surprise.

- **Changes:**
  - `BackupConfig.from_db(url)` (or `store.load_config`) becomes the daemon's only loader. Replace the
    three `from_file` call sites (`daemon.py:153`, `workers.py:62`, `workers.py:196`).
  - **`from_file` is retained and stays public** — it remains the single YAML *parser*, used by `import`
    and by the item-12 edit-buffer validation. It simply stops being a daemon config source. Its existing
    tests keep passing unchanged; only its callers move.
  - **`import_yaml(yaml_path, db_url)` — always a FULL REPLACE** *(user decision)*. Parses via `from_file`
    (single source of validation truth), then wipes and reinserts in **one transaction**. There is no
    `--replace` flag, because replacement is the only semantic: `import` means "make the DB be this file".
    **If the DB already holds a config, emit a WARNING** naming what is being replaced (dataset count, and
    the destination count) — it proceeds, it does not prompt, so the config-as-code flow below stays
    non-interactive.
  - **This makes `import` destructive by design**, which is correct for its purpose but worth one safety
    net: **auto-export the outgoing config to a timestamped file** (e.g.
    `/var/lib/zfsbackup/config.pre-import-<ts>.yaml`) before wiping, and name that path in the warning.
    Cheap, and it turns "I imported the wrong file" from data loss into a one-command recovery.
    *(Recommended — not explicitly requested; drop it if you'd rather not have the CLI writing extra files.)*
  - **Migration ergonomics:** when the daemon is given a `.yaml`/`.yml` path, do **not** print a generic
    "unsupported"; detect it and emit the exact `zfsbackup-config import <path>` command to run. This is
    the single most likely upgrade stumble.
  - **Deployment flows this supports** *(user decision — explicit import, nothing implicit)*:
    ```
    # one-time upgrade of an existing deployment
    zfsbackup-config import /etc/zfsbackup/config.yaml

    # config-as-code: YAML stays in git, DB is a derived artifact
    zfsbackup-config import cfg.yaml     # warns that it is replacing
    systemctl reload zfsbackup           # picks it up (phase 3)
    ```
    **The daemon never imports anything**, so "the CLI is the only writer" — which item 5's fork-safety and
    WAL design depends on — stays true.
- **Target behaviour:** `-c /var/lib/zfsbackup/config.db` loads; `--test-config` (`daemon.py:158-167`)
  validates the DB. The value crossing the process boundary remains a plain path/URL string —
  pickle-safe, no engine, no session. **Do not pass a loaded `BackupConfig` to workers**; that would break
  item 5's fork safety and defeat Phase 3.
  *(Superseded by Checkpoint A decision Q4 / item 15.2: workers now receive the supervisor's
  detached `BackupConfig` — a dataclass holding no file handle — and never read the DB.)*
- **Consequence worth noting:** because the DB is now canonical and every edit buffer is regenerated from
  it, **duration literal preservation (item 2) becomes load-bearing rather than a nicety.** Every `edit`
  round-trip reads literals out of `age_literal`/`keep_for_literal`/`frequency_literal`; if those were not
  stored, a user's `30d` would silently become `1M` on their next edit.
- **Edge cases:** durations import as literals (item 2), so `"1M"` from `config.example.yaml:38` survives.
  Commented-out sections (`config.example.yaml:21-30`) import as absent, not empty. The `config_path`
  attribute name is asserted at `tests/test_zfsbackup_daemon.py:25`; `from_file` patch targets at
  `tests/test_zfsbackup_workers.py:228` and `tests/test_zfsbackup_daemon.py:211,231` need coordinated
  updates — **this is the fix/test coordination point of Phase 1**; item 9 must land in the same cycle.
- **Verification:** `pytest tests/test_zfsbackup_store.py -k import`, then the full
  `pytest tests/test_zfsbackup_*.py`.

- **Status: implemented** (`ecd7ecc`), with these deviations and carry-overs:
  - The loader is `zfsbackup/runtime_config.py`'s `load_runtime_config()`, not `BackupConfig.from_db` —
    a store-aware method on the model would make `config/model.py` import from `store`, inverting the
    dependency. `daemon.py` and `workers.py` reach the store only through it.
  - **The pre-import auto-export safety net was deferred to item 10** (`importer.py` docstring, D-5 /
    8c). Until then `import` is destructive with no backup of the outgoing config.
  - **Interim import tool:** `python -m zfsbackup.config.store.importer <yaml> -c <db>`. It is marked
    for deletion by item 10.
  - **The daemon's `.yaml` refusal names `zfsbackup-config import <path>`, a command that does not exist
    until item 10.** Correct once Phase 2 lands; misleading until then.
  - **`scenarios/test_two_vm_backup.sh` was not migrated.** It still installs YAML at
    `/etc/zfsbackup/config.yaml` and starts the daemon with `-c` on it, which the daemon now refuses —
    the two-VM harness is broken on this branch until it gains an import step. This row of the
    migration table above is open; it is a Checkpoint A item for `real-zfs-scenario-dev`.

### Item 8x — A dying worker wedged the supervisor's stop path *(DONE; found during item 8, not part of it)*

- **Owner:** `zfsbackup-developer` + `pytest-test-author` · **Tag:** `needs-approval` *(signals,
  multiprocessing)* · reviewed by `zfs-code-reviewer` and `concurrency-reviewer`
- **Status: implemented.** New `zfsbackup/stopflag.py`; wired into `daemon.py`/`workers.py`; covered
  by `tests/test_zfsbackup_stopflag.py` and `tests/test_zfsbackup_stop_signals.py`.

**The defect.** `Condition.wait` does `_sleeping_count.release()`, blocks in
`_wait_semaphore.acquire(...)`, and only `finally:` does `_woken_count.release()`. `Condition.notify`
drains `_woken_count`, consumes one `_sleeping_count` token per sleeper, then **blocks** on
`_woken_count.acquire()` once per woken sleeper — and `Event.set` wraps all of it in `with self._cond`.
A process killed inside `_wait_semaphore.acquire` never runs the `finally`, so the ghost is permanent:
**every later `set()` blocks forever, holding the Condition's lock.** Workers spend essentially all
their idle time in `stop_event.wait()`, so the window was not narrow — it was the common case.

**Severity, as finally established.** Three escalations over the course of diagnosis, each correcting
the previous record:

1. It was first written up as an *orphaned lock*. **Wrong**, and its own evidence disproved it:
   `Event.is_set()` is `with self._cond:` and returned in 0.02 ms, which it could not have done
   against an orphaned lock. The dying child *has* released the lock.
2. It was then recorded as *macOS-only, unverified on Linux*. **Wrong** — `multiprocessing/
   synchronize.py` is pure Python and `SemLock` is a POSIX semaphore on both. Reproduced under
   `fork`, `spawn` **and** `forkserver`.
3. It was recorded as reachable via *the OOM killer or `kill -9`*. **Too narrow** — exit codes `-2`
   and `-15` produce the ghost identically, so an ordinary **Ctrl-C hung the daemon on every
   platform**, and so did `systemctl stop`.

And it was worse than "cannot be stopped": after the first wedged `set()`, `is_set()` blocks too, so
the supervisor's poll loop froze, `_check_workers` never ran again and crashed workers were never
restarted. **The daemon stopped supervising**, not merely stopped stopping.

**Item 8's SIG_DFL reset is what made Ctrl-C (as opposed to SIGTERM) a wedge**, and this must not be
misread as a regression in item 8. With Python's default SIGINT disposition a child raises
`KeyboardInterrupt`, which unwinds through `Condition.wait`'s `finally` and releases the token;
`SIG_DFL` does not. Measured, same harness, only the disposition differing:

    children at default_int_handler:  exitcodes=[1,1,1]     set() returned in 6.9 ms
    children at SIG_DFL (item 8):     exitcodes=[-2,-2,-2]  set() WEDGED

SIGTERM wedged either way. Item 8 correctly removed the fork inherited-handler deadlock and in doing
so converted a survivable Ctrl-C into a wedging one. **Item 8 and 8x together** are what make the
fork case safe; neither alone does.

**The fix: `StopFlag`**, a `multiprocessing.Value('b', 0, lock=False)` behind a class exposing exactly
the `Event` surface the daemon uses (`set`/`clear`/`is_set`/`wait`). `set()` is one shared-memory
store — no lock, no condition variable, no sleeper accounting, nothing a dying process can leave
behind — which also makes it **legal from a signal handler**, the second and independent reason the
old design was wrong (`Event.set()` is not async-signal-safe even with no ghost sleeper).

Measured on the full daemon shape (supervisor + 2 loop workers + 1 untimed waiter; kill a worker
abruptly, then signal the supervisor): **12/12 clean exits in 0.50–0.52 s** across
fork/spawn/forkserver × {SIGKILL, SIGINT} × {SIGTERM, SIGINT}, where `mp.Event` wedged permanently.
Live daemon after wiring: SIGKILL a worker then SIGTERM → **exit 0 in 0.265–0.554 s**, and
supervision still works (SIGKILL pruning → restart 5.0 s later → SIGTERM → exit 0).

`StopFlag` is also cheaper than what it replaces — a byte versus four semaphores — which matters
because nearly every test in `test_zfsbackup_daemon.py` constructs a `BackupDaemon`.

**Alternatives rejected, with the measurements:**
- **A pipe + `selectors`** (30x faster wakeup) — three hazards, all of the silently-different-per-
  start-method class this item existed to remove: EOF-broadcast **never woke `fork` children** (joins
  timed out at 8/16/24 s) because siblings inherit each other's write ends and CLOEXEC does not apply
  to fork; `Connection.recv_bytes` expects a 4-byte header so an async-signal-safe `os.write(fd,
  b'\x01')` makes `poll()` return True and then blocks forever; and writing to a dead worker's pipe
  raises `BrokenPipeError` (3/3), inside a signal handler.
- **`Semaphore.release()`×N** — must know N, not idempotent, no token for a later-started worker, no
  natural `is_set()`.
- **"Detect `exitcode == -SIGKILL` and rebuild the Event"** — *unsound*, and was recorded here before
  being struck: wrong trigger (-2 and -15 do it too), live workers hold the *old* Event from
  fork/spawn time and would never see a new one, and detection only runs every 5 s while a signal can
  arrive at any instant.

**Also landed (8x-3b):** `BackupDaemon._supervisor_sleep` sleeps in ≤0.25 s slices and returns early
on the flag, cutting SIGTERM-to-shutdown from ≤5 s. PEP 475 means `time.sleep` is *retried* with the
remaining time after a handler returns — measured, handler at 0.50 s, `time.sleep(5.0)` still
returned at 5.01 s — so the healthy path already cost a full `SUPERVISOR_POLL`. The named method also
gives tests a stable seam instead of patching raw `time.sleep`.

**Retired by this item:** the "no test may SIGKILL a worker" constraint, and `daemon.py`'s 31-line
block recording 8i's dependency on 8x. **Item 8i is un-inverted** — its `set()` fires exactly when
workers have died, which is now the case `StopFlag` is built for.

**Not fixed here, and still item 18's:** a worker blocked in a long `zfs send` does not notice a stop
until the subprocess returns, under *any* of these mechanisms — `terminate()` remains the answer.
`StopFlag` does not foreclose item 18: add a second `Value('l')` reload generation read in the same
poll and check it between datasets, which is where 18 must act anyway. A pipe can be added alongside
later without changing `StopFlag`'s contract — record the three hazards above so they are not
rediscovered.

**Test-design traps worth keeping** (both cost real time here):
- A readiness marker is **not** enough. Killing a worker on the supervisor's `Started … (pid=…)` line
  kills it during config load, before it has entered `wait()` — no sleeper, no ghost, and the control
  *passes against broken code*. Wait for the worker's own log output, with margin.
- A regression here **hangs rather than fails**, so the candidate `set()` must run in a child-process
  canary with a hard `join(timeout)` + `kill()`, never on the test's own main thread.


### Item 9 — Tests for Phase 1

- **Owner:** `pytest-test-author` · **Tag:** `low-risk`
- **Files:** new `tests/test_zfsbackup_store.py`; edits to `tests/conftest.py`,
  `tests/test_zfsbackup_daemon.py`, `tests/test_zfsbackup_workers.py`
- **Basis:** items 2-8, plus the patch targets item 8 invalidates.
- **Coverage required:**
  - Schema constraints: duplicate dataset name rejected; duplicate `(dataset, age)` retention rejected
    **at each scope separately** — the dataset-level case needs the partial unique index, and a test
    covering only the scoped rows passes against a plain `UNIQUE` that constrains nothing; cascade
    delete removes retention rules and dataset_remotes.
  - Mapper equivalence: for `config.example.yaml` and `config.test.yaml`,
    `load_config(imported) == from_file(yaml)` field-by-field including retention ordering.
  - **`Duration` consumer-surface tests**, one per row of the item-2 table — especially the reflected
    comparison at `backup_manager.py:205` (`bare_timedelta >= Duration`), the `or`-truthiness at
    `workers.py:146-150` with a zero `Duration`, pickle/deepcopy literal preservation, and byte-identical
    `to_property()` output.
  - **`__str__` is unchanged** — `str(Duration("1M")) == "30 days, 0:00:00"`, asserted directly, so
    a later refactor cannot silently alter daemon log output.
  - **`.literal` and `.timedelta` accessors** — `.literal` returns the source string (for values
    constructed from a `timedelta`, a synthesized literal — see the constructor-forms bullet); `.timedelta` returns a value equal to the `Duration` but of
    **exactly** type `timedelta` (`type(d.timedelta) is timedelta`, not merely `isinstance`), carrying no
    literal.
  - **All three constructor forms** — `Duration("1M")` parses and sets `.literal`;
    `Duration(timedelta(days=30))` and `Duration(days=30)` both take the value and **synthesize** `"1M"`;
    all three compare equal. Also assert `Duration(30) == Duration(days=30)` to pin the positional-`days`
    dispatch against a future refactor that might try to read a bare number as seconds.
  - **Pickle/deepcopy preserves `.literal`** — asserted explicitly, because with native-kwargs support a
    missing `__reduce__` no longer raises — synthesis silently substitutes the canonical literal. **Use a
    non-canonical literal such as `30d` for this assertion**; `1M` would pass even with `__reduce__` absent,
    because it is already what synthesis produces.
  - **`parse_time_duration` shim** (if retained) — still importable from `zfsbackup.config`, still returns
    a plain `timedelta` of **exactly** that type, so the 22 existing assertions are genuinely unaffected
    rather than merely passing by `isinstance` coincidence.
  - **Duration round-trip table:**

    | literal | seconds | re-rendered |
    |---|---|---|
    | `5m` | 300 | `5m` |
    | `1h` | 3600 | `1h` |
    | `1d` | 86400 | `1d` |
    | `1w` | 604800 | `1w` |
    | `1M` | 2592000 | `1M` (**not** `30d`) |
    | `30d` | 2592000 | `30d` (**not** `1M`) |
    | `1y` | 31536000 | `1y` |
    | `10y` | 315360000 | `10y` |
    | `365d` | 31536000 | `365d` (**not** `1y`) |
    | `6d12h` | 561600 | `6d12h` |
    | `1h30m` | 5400 | `1h30m` |
    | `1y2M3d` | 36979200 | `1y2M3d` |
    | `12h6d` | 561600 | `12h6d` (order preserved as written) |
    | `-5d` | -432000 | `-5d` |

  - **Compound parsing rejects** `1d1d`, `1h30m15m` (duplicate units) and `6d-12h` (per-term sign), while
    `""`, `"xh"`, `"5z"`, `"M5"`, `"mi5"`, `"mi"` still raise `ValueError` — the last two are the dead
    leading-`m` branch's tests, which must pass against the rewritten parser unmodified.
  - **Unit case-sensitivity** — `5m` is 5 minutes, `5M` is 150 days, `2H` is 2 hours, `1M30m` is
    30 days + 30 minutes. This is the `m`/`M` collision that defeated every third-party library; assert it
    directly so a future "simplification" that lowercases units fails loudly.
  - **Literal synthesis** — `Duration(days=6, hours=12).literal == "6d12h"`; `Duration(days=400).literal ==
    "1y1M5d"`; `Duration(hours=36).literal == "1d12h"`; `Duration(0).literal == "0s"`;
    `Duration(days=-5).literal == "-5d"`.
  - **Synthesis is exact and deterministic** — as a property test over whole-second values rather than a
    table: `Duration(Duration(v).literal) == v`, and the same `v` always yields the same literal (or item
    12's "no changes" check would report spurious diffs on an untouched config).
  - **Sub-second** — `Duration(microseconds=1)` constructs with `.literal is None`, and `.render()` raises
    `ValueError` rather than truncating.

  - The `from_property` path (`config/model.py:110-132`) rebuilds from raw seconds and now yields a synthesized
    literal rather than `None` — assert it renders (e.g. 2592000s → `"1M"`) instead of failing.
  - Fork safety: parent opens DB, forks, child reads independently; assert no corruption.
  - Concurrent write-while-read under WAL.
  - Path resolution order and the create-vs-error policy (item 6).
  - New `sqlite_config_path` conftest fixture mirroring `config_yaml_path` (`tests/conftest.py:206-217`).
- **Verification:** `pytest tests/test_zfsbackup_*.py` fully green.

> ### ⛳ CHECKPOINT A — held 2026-10-17
> Reviewed by two `zfs-code-reviewer` passes (config/store layer; daemon side) and `concurrency-reviewer`.
> **Verdict: Phase 1 is safe in production as it stands** — no reachable path corrupts the DB or tears
> a backup. Defects that Phase 2 would build on are fixed in the fix-up cycle below before any CLI work.
> The de-duplicated findings (IDs A1–A5, B1–B5, C1–C3, D1–D7) are cited by the items that follow.
>
> **Verified clean:** Duration literal round-trip at every level including per-destination; override vs
> inherit; tri-states; schema constraints (partial unique indexes, FK RESTRICT/cascades); `migrate.py`
> revision cases; wire format byte-identical; torn-read protection; no engine at import time;
> `dispose_all()` before every fork; read-only worker sessions; no checkpoint starvation; retention and
> anchor behaviour unchanged for configs without per-destination rules; item 9 coverage complete except
> the gaps listed in A-8.

### Checkpoint A findings (summary)

| ID | Sev. | Finding | Fixed in |
|---|---|---|---|
| A1 | MED | `None` bool/str fields replaced by ORM Python defaults on save — `enabled: null` (disabled under YAML) imports as enabled; `remote_backup.enabled: null` enables the unauthenticated receive path; `dry_run: "false"` crashes import with a traceback | A-1, A-2 |
| A2 | MED | Validation only inside `save_config` (needs a session); importer validates after creating + migrating the target, leaving an empty stamped DB | A-2 |
| A3 | MED | Empty dataset-level retention (`[]` = never prune) reloads as 1d→30d | A-1, A-2 |
| A4 | MED-HIGH | Permission model broken for non-root operators: re-import widens a 0600 file to 0660; non-owner chmod is EPERM; chmod strips setgid; operator-owned `-wal`/`-shm` lock out the daemon | A-4 |
| A5 | LOW-MED | Zero/negative intervals accepted → worker hot loop | A-1, A-2, A-3, A-5 |
| B1 | MED | Schema check compares DB head with on-disk scripts (not in-memory code) on a separate connection | 10.1, 15.2 |
| B2 | — | No generation CAS; FK-off migration connection never runs `foreign_key_check` | 10.1, 15.1 |
| B3 | MED | Workers run on different config generations after a CLI edit | 15.2 |
| B4 | MED | No-backoff respawn loop for non-EX_CONFIG load failures | A-5 |
| B5 | — | `rename --clear-stale-props` while a daemon runs can destroy the incremental base | 14.2 |
| C1 | HIGH (latent) | pid-keyed engine cache alone is not fork-safe; `dispose_all()` at the fork point is what protects | A-6.1 |
| C2 | MED | Abandoned worker wedges interpreter exit; exception in the poll loop skips graceful shutdown | A-6.2 |
| C3 | LOW | SIGHUP default-terminates; startup ignores the stop flag between spawns; systemd `KillMode`; websocket timeouts | A-6.2 (part), items 18/21 |
| D1–D7 | LOW | Lossy duplicate dedupe; unordered destinations; tracebackless startup errors; YAML hint misses `-c`; stale docs; missing `packages`; two-VM scenario on YAML | A-2, A-5, A-7, 10.2, A-9 |

### Checkpoint A decisions *(user, 2026-10-17)*

- **(a) Empty values keep their YAML meaning.** If YAML allowed an empty value that produced a runtime
  default or behaviour, the DB reproduces it; behaviours with adverse effects come with a WARNING
  rather than a rejection. Values whose YAML behaviour was simply broken (a TypeError every cycle, a
  hot loop) are rejected — see Q2 and Q3.
- **(b) Non-root operators are in scope now** — group `zfsbackup` operators editing a DB owned by user
  `zfsbackup`. Fixed in A-4, before Phase 2's write path is built.
- **Q1** Never-prune YAML spelling: `retention: forever` (dataset level only). `null`, `{}` and an
  absent key keep meaning the documented 1d→30d default.
- **Q2** `snapshot_prefix: null` is rejected (the runtime never worked with it). The error tells an
  operator with `None_*` snapshots to set `"None"` explicitly.
- **Q3** Intervals ≤ 0 are rejected on write; the runtime also floors the worker interval at 1s.
- **Q4** Workers receive the supervisor's detached `BackupConfig` and never read the DB. This
  **reverses item 8's "do not pass a loaded `BackupConfig` to workers"**: a detached dataclass holds no
  file handle, so fork safety is intact.
- **Q5** Regenerate the baseline revision under a new id with the A5 CHECKs, and freeze it at the merge
  of `config-db-cli` to master. Development DBs must be deleted and re-imported.
- **Q6** CLI commands use "destination" (`list destinations`, `edit destination`, `rename destination`);
  `remote:` survives only as the YAML / dot-path key under a dataset.
- **Q7** A root import creating the default directory/file chgrps it to `zfsbackup` when that group
  exists; otherwise it warns that operators will be locked out.
- **Q8** A conftest hook marks unmarked tests `unit`.
- **Q9** The pre-import backup is a YAML file, with `--no-backup` to opt out.

### Decision (a) — field audit

"Normalize" means `from_file` turns the value into the concrete value the YAML runtime already behaved
as. "Reject" means a clean `ValueError` naming the key, raised before anything is written.

| Field | YAML today (`from_file` → runtime) | DB today | Fix |
|---|---|---|---|
| `datasets[].enabled: null` | falsy → **disabled** | default **True** | Normalize `False` + WARNING |
| `enabled`/`recursive`/`dry_run`/`remote_backup.enabled` as a string | truthy | `StatementError` traceback | Reject (type) |
| `datasets[].recursive: null` | falsy | default False | Normalize `False`, silently |
| `datasets[].frequency` ≤ 0 | raises each cycle / remote worker hot loop | stored | Reject |
| `retention` absent / `null` / `{}` | default 1d→30d | one row | Unchanged |
| dataset-level `[]` (direct, CLI, `forever`) | **never prune** | reloads as 1d→30d | Zero rows load as `[]` + WARNING |
| `remote[].frequency: null` | inherit | NULL | Unchanged |
| `remote[].frequency: 0m` | falsy → inherit | stored 0 | Normalize `None` (inherit) + WARNING |
| `remote[].frequency` < 0 | negative interval, hot loop | stored | Reject |
| `snapshot_prefix: null` / non-string | TypeError every cycle; `None_*` snapshots never pruned | silently `autosnap` | Reject (Q2) |
| `snapshot_prefix` containing `_` | nothing is ever managed or pruned (pre-existing parser bug) | stored | WARNING; parser fix deferred |
| `check_interval` / `prune_interval` ≤ 0 | hot loop | stored | Reject (Q3) |
| `prune_interval: null` | derive (3c) | NULL | Unchanged |
| `api_host: null` | Flask binds 127.0.0.1 | default 127.0.0.1 | Normalize `"127.0.0.1"`, silently |
| `api_port` null / bool / float / out of range | TypeError / `true`→1 / truncation | n/a | Reject; digit strings accepted; 0 warns |
| `dry_run: null` | falsy → live | default False | Normalize `False`, silently |
| `client_id_file` non-string | TypeError | n/a | Reject |
| dataset `name` / destination key not a string | flows through | type changes | Reject |
| `remote_backup.enabled: null` | falsy → **disabled** | **True** (unauthenticated receive) | Normalize `False` + WARNING |
| bool fields as integer 0/1 *(A-15)* | truthiness — behaves as written | n/a | Normalize to `False`/`True` + WARNING; other ints rejected |
| duplicate `remote:` destination on a dataset *(A-16)* | second entry's override silently ignored | rejected by `validate_config` | Reject (incl. exact duplicates) |
| `client_id_file: ""` in-code *(A-14.1)* | YAML `""` = derive | reloads as `Path(".")` | Store NULL + WARNING; `"."` / `Path("")` rejected |

---

## Checkpoint A fix-up *(approved 2026-10-17; lands before Phase 2)*

Two developer lanes on disjoint files, run in parallel: **store** A-2 → A-3 → A-4 → A-6.1 → A-7(store);
**daemon** A-1 → A-5 → A-6.2 → A-7(dev). Contract between the lanes: a dataset-level `[]` means never
prune, and `validate_config(config) -> list[str]` returns warnings and raises `ValueError`. A-8 tests
follow; A-9 runs alongside. Gates: `zfs-code-reviewer` on every item, `concurrency-reviewer` on A-4,
A-5, A-6.

### A-1 — YAML-boundary empty-value and type semantics
- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (approved) · **Basis:** A1, A3, A5,
  decision (a); `model.py:543-547, 595-598, 691-692, 703-704, 743-747`.
- **Files:** `zfsbackup/config/model.py`
- **Changes:** guards in the style of `_duration_from_config` — `_bool_from_config`,
  `_str_from_config`, `_port_from_config`, a key-named positive-interval guard. Apply every Normalize
  and Reject row of the audit. Accept `retention: forever` at dataset level only → `retention_rules=[]`
  + WARNING; destination-level `{}` stays rejected (item-3b deferral of destination never-prune stands).
  Module logger for the WARNINGs.
- **Edge cases:** `enabled: null → False` doesn't change the wire format (disabled datasets never reach
  `to_property`). Item 9's equivalence test still holds. Both repo YAMLs are unaffected. Item 3c's
  `is None` discipline is unchanged.
- **Verify:** `pytest tests/test_zfsbackup_config.py`, then `pytest tests/test_zfsbackup_*.py`.

### A-2 — Session-free validation, never-prune load, import ordering
- **Owner:** `zfsbackup-store-developer` · **Tag:** `needs-approval` (approved) · **Basis:** A1, A2,
  A3, A5, D1, D2; `mapper.py:177-183, 386, 597-672`; `models.py:141,149-151,194,197,488`;
  `importer.py:17-21, 262-311, 368-375`.
- **Files:** `mapper.py`, `models.py`, `importer.py`, `store/__init__.py`
- **Changes:**
  1. Public pure `validate_config(config) -> list[str]` taking over `save_config` step 1, plus exact type
     checks (`type(x) is bool`, str fields, int `api_port` excluding bool, port range) and rejection of
     intervals ≤ 0 and remote frequency ≤ 0. Warnings: never-prune datasets, prefix containing `_`,
     exact-duplicate rules about to be deduped (D1, naming both literals). `save_config` calls it first.
  2. A3: zero dataset-level rows load as `[]` + WARNING; rewrite the docstrings that describe the old
     default (`mapper.py:164-171`, `:363`, the asymmetry note in `_scoped_rules`).
  3. A1 defence: drop the Python `default=` on non-key NOT NULL columns. DDL-neutral — the autogenerate
     diff against the baseline stays empty.
  4. D2: `order_by(models.Destination.name)` at `mapper.py:386`.
  5. Importer: phase 0 is `from_file` + `validate_config` (warnings logged) before anything touches the
     target; if `created` and a later phase fails, unlink the DB and any `-wal`/`-shm`/`-journal` this
     call created (lstat-checked) *(superseded by A-11: never unlink a sidecar, never a DB in use)*; add `SQLAlchemyError` to `_main`'s except list until 10.3 deletes it;
     correct the docstring at `importer.py:17-21`.
- **Edge cases:** `validate_config` must not evaluate `effective_client_id_file` (reads `$HOME`).
  Destination-scope `[]` still means inherit.
- **Verify:** `pytest tests/test_zfsbackup_store.py`

### A-3 — Baseline positivity CHECKs (Q5)
- **Owner:** `zfsbackup-store-developer` · **Tag:** `needs-approval` (approved) · **Basis:** A5,
  following `ck_retention_rules_*_positive`.
- **Files:** `models.py`; delete and regenerate `migrations/versions/20260915_2e8aa45e2667_…py` under a
  new revision id.
- **Changes:** CHECKs `check_interval_seconds > 0`; `prune_interval_seconds IS NULL OR > 0`;
  `datasets.frequency_seconds > 0`; `dataset_remotes.frequency_seconds IS NULL OR > 0`. Regenerate the
  revision from autogenerate. Docstring: "frozen from the merge of config-db-cli to master" (D5).
- **Edge case:** existing dev DBs fail loudly with `SchemaVersionMismatch`; delete and re-import. Say so
  in the commit message.
- **Verify:** `pytest tests/test_zfsbackup_store.py -k "migration or check_constraint"`

### A-4 — Permission model for group operators
- **Owner:** `zfsbackup-store-developer` · **Tag:** `needs-approval` (approved) · **Gate:** +
  `concurrency-reviewer` · **Basis:** A4, decision (b); `paths.py:271, 1195-1222, 1403, 1489,
  1499-1501`; `importer.py:219, 306-307`.
- **Files:** `paths.py`, `importer.py`
- **Changes:**
  1. `CONFIG_DIR_MODE = 0o2770`.
  2. One helper `_ensure_mode(path, mode)` for the directory, the DB and the sidecars: lstat; equal →
     nothing; owned (`st_uid == geteuid()`, or this process is root — A-14.2) → chmod then re-stat (Linux silently drops setgid for a
     non-member: WARNING, not error); not owned but usable → WARNING naming owner, mode and fix;
     otherwise `ConfigDbPermissionError` saying the owner must run `chmod`.
  3. `ensure_config_db_mode` only adjusts the default path; an existing file at a non-default path is
     never chmodded (0600 stays 0600); warn if the group can't read and write it.
  4. Sidecars via `_sidecar_lstat`, never `Path.exists()`; never chmod a sidecar we don't own.
  5. `check_config_db` checks sidecars whether or not the DB is writable: an existing sidecar this
     process can't write is a `ConfigDbPermissionError` (→ EX_CONFIG); a sidecar whose group differs
     from the DB's is a WARNING.
  6. `importer.py:219`: compare directories by `(st_dev, st_ino)`.
  7. (Q7) Running as root and *creating* the default dir/file: `chown(-1, gid(zfsbackup))` if the group
     exists, else WARN.
- **Edge cases:** macOS assigns the directory's group automatically, so Linux behaviour is mocked in
  pytest and proven in A-9. Root passes every `os.access` check. Never touch `-journal`.
- **Verify:** `pytest tests/test_zfsbackup_store.py -k "permission or mode or sidecar or setgid"`

### A-5 — Daemon: restart backoff, failure classification, interval floor
- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (approved) · **Gate:** +
  `concurrency-reviewer` · **Basis:** B4, A4 (daemon half), A5, D3; `daemon.py:271-302, 597-611`;
  `workers.py:152-160, 221`; `runtime_config.py:272-276`.
- **Files:** `daemon.py`, `workers.py`, `runtime_config.py`
- **Changes:**
  1. Replace the blocking `time.sleep(WORKER_RESTART_DELAY)` with a per-worker `restart_at` deadline
     checked every poll. Consecutive failures per worker: delay 5s·2^(n-1), capped at 300s; reset after
     ≥10 min up; six consecutive failures → treated as EX_CONFIG (loud log, daemon stops, exit 1).
  2. *(Failure classification is dropped: under Q4 the workers' `_load_config` is deleted in 15.2.)*
  3. `config_error_message` says "Could not open" only for real open failures.
  4. Floor the worker interval at 1s with a single WARNING (covers a hand-edited DB).
  5. D3: `exc_info=True` in the `daemon.py:597-611` arm for non-OperationalError, non-config exceptions.
- **Edge cases:** backoff never delays shutdown; EX_CONFIG stays fatal; tests patching raw `time.sleep`
  in `_check_workers` move to the new deadline seam.
- **Verify:** `pytest tests/test_zfsbackup_daemon.py tests/test_zfsbackup_workers.py`
- **Note:** delivers item 17's "fix the infinite respawn" bullet; strike it when Phase 3 is revised.

### A-6 — Fork and shutdown hardening (C1, C2, part of C3)
- **A-6.1** — `zfsbackup-store-developer` · `needs-approval` (approved) · gate `concurrency-reviewer`.
  `db.py`: `gc.collect()` after `dispose(close=False)` + `del _ENGINES[key]` (`:721-722`) and in
  `dispose_all`'s non-owner branch. Correct docstrings `db.py:15-17, 635-650, 709-715` (the pid cache
  alone is not enough; `dispose_all()` at the fork point is mandatory; a child closing an inherited fd
  does not release the parent's locks). Fix stale refs at `:643-645`; `:476` names `open_config_db`,
  which no longer exists. **Verify:** `pytest tests/test_zfsbackup_store.py -k fork`.
- **A-6.2** — `zfsbackup-developer` · `needs-approval` (approved) · gate `concurrency-reviewer`.
  Poll loop (`daemon.py:461-466`) in try/finally that sets the stop flag and calls `_shutdown_workers`.
  Record abandoned workers (`:366-370`); if any, `main()` flushes logging and calls `os._exit(code)`
  after `dispose_all()`, skipping multiprocessing's untimed join. `_start_workers` checks the stop flag
  between spawns (`:222-224`). SIGHUP stopgap: the supervisor's handler only sets a flag and the poll
  loop logs "live reload not implemented; restart to apply"; workers `SIG_IGN` SIGHUP in
  `_init_child_process`. Correct the C1 claim in `_spawn`'s docstring. **Verify:**
  `pytest tests/test_zfsbackup_daemon.py tests/test_zfsbackup_stop_signals.py`.
- **Deferred from C3:** getppid parent-death detection → item 18; `KillMode=mixed` → item 21 docs;
  websocket timeouts (`remote.py:185-202`) → item 18 + remote-security workstream.

### A-7 — Stale docs and messages
- **Tag:** `low-risk` · **Basis:** D4, D5.
- **Store developer:** `_yaml_message` (`paths.py:715-722`) gives both steps — import into `<db>`, and
  point the daemon's `-c` at `<db>`; `paths.py:56-57, 433-438` Python ^3.10 → ^3.11;
  `store/__init__.py:86-87` and `migrate.py:11-15` drop "not wired".
- **zfsbackup-developer:** `config/__init__.py:20-23` names `zfsbackup.store` as the reserved package.
- **Main session:** item 9's `.literal` bullet (done with this insertion).

### A-8 — Tests
- **Owner:** `pytest-test-author` · **Tag:** `low-risk`
- **Coverage:** one test per audit row through both `from_file` and import → `load_config`; A1–A3
  reproductions (`enabled: null`, `remote_backup.enabled: null`, `dry_run: "false"`, a
  `{24h: 1M, 7d: 30d}` import leaving no stray DB, `[]` round-trip, `retention: forever`); A-4 (mode
  preservation on re-import, non-owner no-chmod, setgid, sidecar ownership refusal); A-5 (backoff
  schedule, escalation, deadline not blocking shutdown); A-6 (GC-disabled fork child, try/finally,
  abandon → `os._exit`, SIGHUP ignored); listed gaps — non-canonical per-destination literals (`90m`
  remote frequency, `30d` scoped rules), the `frequency <= 0` consumer row, `config_error_message`'s
  OperationalError → diagnose branch and `describe_config_source`, daemon "missing DB exits 1, creates
  nothing", repeated non-EX_CONFIG respawn. Use or delete `source_config_db`
  (`test_zfsbackup_real.py:177`); fix the stale docstring at `test_zfsbackup_store.py:7`; Q8 conftest
  hook marking unmarked tests `unit`.
- **Verify:** `pytest tests/test_zfsbackup_*.py`; `pytest -m unit` + `pytest -m integration` cover the
  whole mocked suite.

### A-9 — Scenarios
- **Owner:** `real-zfs-scenario-dev` · **Tag:** `low-risk`
- **D7** (done, uncommitted at time of writing): `test_two_vm_backup.sh` imports via the interim
  importer and runs the daemon on the DB. It must switch to `zfsbackup-config import` before 10.3.
- **New `scenarios/test_config_permissions.sh`** on a Linux Multipass VM: users `zfsbackup` and
  `alice` (in group `zfsbackup`); root's first import; alice re-imports with the daemon running; a
  SIGHUP-killed CLI leaves sidecars still in group `zfsbackup`; a non-default 0600 `-c` stays 0600.
  Starts after A-4. **Verify:** manual run.

### Fix-up review round *(A-10…A-20, approved 2026-10-17)*

The A-1…A-8 diff was reviewed by `zfs-code-reviewer` and `concurrency-reviewer` (findings R1–R7:
scratchpad `fixup_review_findings.md`). No new hang, wedge or corruption path in a running deployment;
A-6.1 confirmed on Python 3.11–3.14. One reproduced data-loss path (R2) and one daemon-stopping race
(R1) must be fixed before the fix-up is committed. Lanes: **store** A-10 → A-12 → A-11 (+A-11.2) →
A-13 → A-14; **daemon** A-15 → A-16 → A-17 → A-18 (parallel, disjoint files); then A-19; re-review by
both reviewers (concurrency-reviewer on A-10, A-11, A-11.2, A-13); then one commit for the whole fix-up.

**Decisions (user, 2026-10-17):** integers 0/1 in boolean fields are accepted with a WARNING; A-11.2
approved; exact-duplicate remotes rejected; A-17's exit 1 approved; item 15 gains a CLI writer lock.

#### A-10 — A sidecar that vanishes mid-check stops the daemon (R1)
- `zfsbackup-store-developer` · `needs-approval` · gate + `concurrency-reviewer` · `paths.py`
- Three sites: `check_config_db` step 7 (`paths.py:1448-1456`, daemon path → EX_CONFIG → daemon exit 1);
  `_ensure_mode` not-owned branch (`:1113`); `_ensure_mode` owned branch (`:1089-1096`) where `os.chmod`
  on a just-checkpointed sidecar gets ENOENT — root always takes this branch.
- Step 7: on `_can_access` failure, `_sidecar_lstat` again — `None` → continue; present → one more
  `_can_access`; still failing → raise from the fresh stat. Not-owned branch: same with `os.stat`
  (keeps follow-symlink semantics; F6 stays deferred); ENOENT/ENOTDIR → return. Owned branch:
  `FileNotFoundError` from chmod → return (debug log); other errnos unchanged.
- A real permission problem still raises: a skip needs a positive ENOENT from a stat of that path (step 5
  already proved the directory searchable), an existing-but-unusable sidecar still raises, no loop.

#### A-11 — Failed-first-import cleanup never deletes a DB someone else uses (R2)
- `zfsbackup-store-developer` · `needs-approval` (config write) · gate + `concurrency-reviewer` ·
  `importer.py` (replaces `_remove_created_target`, `importer.py:276-321`, called `:433-436`).
- Reproduced (`a4/cleanup_race.py`): a concurrent re-import that committed is deleted and its author was
  told it succeeded; a connected reader has DB, `-wal` and `-shm` unlinked under it.
- Algorithm:
  1. `dispose_all()`; if it raises, remove nothing (WARNING).
  2. `_file_id(target) != created_id` → remove nothing (replaced).
  3. Any `-wal`/`-shm`/`-journal` present (lstat) → remove nothing: in WAL mode the last closer deletes the
     sidecars, so after our own dispose a surviving sidecar means another connection is attached.
  4. If `st_size > 0`, probe with a raw `sqlite3.connect(target.as_uri() + "?mode=rw", uri=True,
     timeout=1, isolation_level=None)` (never creates; bypasses `get_engine`): `BEGIN IMMEDIATE`; empty
     iff `global_settings` is absent or has zero rows (it is written in the same transaction as all
     data, so it is the commit marker); `ROLLBACK`; close. Busy, any `sqlite3.Error`, or a row present
     → remove nothing. Zero-byte files skip the probe (nobody else can have written to one).
  5. Release the connection **before** unlinking (its last close deletes sidecars by path, which after
     an unlink may belong to a new DB). Re-check 2 and 3 immediately before `os.unlink(target)`. The
     remaining window (an open between that lstat and the unlink, or an open that has run no statement)
     is named in a comment; item 15's writer lock closes it.
  6. Never unlink a sidecar. Remove the `sidecar_baseline` plumbing (`:396`, `:402`).
  7. Operator messages: success → INFO "removed <db>, created by this failed import"; skip → one WARNING
     naming <db>, the reason and the next step (schema-only DB: "re-running the import reuses it";
     zero-byte: "remove the empty <db> once nothing has it open, then retry"; another import present:
     "another process has since imported a configuration into <db>; it was left intact; this import
     was NOT applied"; always "never delete -wal/-shm on their own"). Cleanup errors are logged, never
     raised. Rewrite the module docstring (`importer.py:27-34`).

#### A-11.2 — Refuse a first import next to orphaned sidecars
- `zfsbackup-store-developer` · `needs-approval` (approved) · gate + `concurrency-reviewer` · `importer.py`
- Before `create_config_db_file` (`importer.py:398-405`), if any `-wal`/`-shm`/`-journal` is present
  while the DB is absent, raise a `ConfigPathError` subclass naming the files: "remove them once no
  process has them open (`fuser`/`lsof`), then retry". Nothing is created; the orphans are untouched.
  (An orphaned `-wal`/`-journal` would otherwise be replayed into the new file; a live orphan's `-shm` is
  shared by path.)

#### A-12 — `create_config_db_file` either fully succeeds or leaves nothing (R3)
- `zfsbackup-store-developer` · `needs-approval` · `paths.py:1662-1701`
- On any `BaseException` after `O_EXCL` succeeds: if `fstat(fd)` and `lstat(target)` agree on (dev, ino),
  unlink while the fd is open (best effort, logged), close, re-raise. Wrap `os.fchmod`'s `OSError` in
  `ConfigDbPermissionError`. Kept in `paths.py` so the function is atomic for item 10's CLI as well.

#### A-13 — Engine cache purges every inherited entry (R4)
- `zfsbackup-store-developer` · `needs-approval` · gate + `concurrency-reviewer` · `db.py`
- In `get_engine`, under the lock: for every entry with `owner_pid != pid`, `dispose(close=False)` and
  delete; drop references; one `gc.collect()`; then the existing lookup/build. Lazy purge in
  `get_engine`, not a `register_at_fork` hook (a hook would close connections and collect at the child's
  most fragile moment).
- `gc.collect()` stays inside the lock and before the build (moving it out would hand out the new engine
  before the collect, or let another thread build in between — C1 reopens). Make `_ENGINES_LOCK` a
  `threading.RLock()` (both at creation and in `_reinit_lock_after_fork`) so a finalizer can't
  self-deadlock; comment why this differs from `dispose_all`.
- Docstrings: "survivable" holds for every key unless something calls `gc.freeze()`; any future
  `gc.freeze()` must come after `dispose_all()`.

#### A-14 — Small store-side fixes
- **A-14.1** `client_id_file` (`needs-approval`): in-code `""` → `validate_config` WARNING ("means derive
  from $HOME; stored unset") and `save_config` writes NULL; reject any value whose `os.fspath` is `"."`
  (`Path("")`, `Path(".")`, `"."` — they name a directory and never worked). YAML `client_id_file: "."`
  is therefore refused at import phase 0.
- **A-14.2** (`low-risk`): `_ensure_mode` and `ensure_config_db_mode` docstrings say "owned by this
  process, or this process is root"; the directory keeps `S_ISVTX` (compare/chmod to
  `mode | (actual & S_ISVTX)`); files keep the exact compare; nothing is ever widened.
- **A-14.3** (`low-risk`) stale docs: `importer.py:347` phantom `log_warnings`; `importer.py:52-53` (modes
  adjusted on the default path only); `paths.py:1315` (`require_writable=False` skips steps 5–7; add step
  7 to the list); `paths.py:297-300` `CONFIG_OWNER` vs Q7; migration docstring "eight" → "nine" CHECKs
  (docstring only); `db.py:31-33` aligned with `:732-738`.

#### A-15 — Integer 0/1 in boolean fields: accept with a WARNING
- `zfsbackup-developer` · `needs-approval` (approved) · `model.py:253` `_bool_from_config`
- `None` unchanged; `type(v) is bool` → v; `type(v) is int and v in (0, 1)` → `bool(v)` + WARNING
  ("<key> is the integer N; treating it as true/false — write true/false"; for `remote_backup.enabled: 1`
  say it enables the unauthenticated receive endpoint); anything else → `ValueError`.
- `validate_config`'s `_require_bool` stays strict: YAML-sourced configs arrive normalized; an int in an
  in-code config is a caller bug. Item 11's `set` parses through this same guard.

#### A-16 — Reject duplicate `remote:` destinations at the YAML boundary (R5)
- `zfsbackup-developer` · `needs-approval` · `model.py:710-754`
- Reject duplicates, including exact duplicates, in `DatasetConfig.from_dict` with `validate_config`'s
  wording (`mapper.py:703-709`). Under decision (a) this is rejected because the YAML never meant what it
  said (the second entry's retention was silently ignored). Import already refused it, so nothing that
  imports today changes outcome. `from_property` untouched.

#### A-17 — Supervisor exit path
- `zfsbackup-developer` · `needs-approval` (approved) · `daemon.py`
- `_hard_exit` exits `code or 1` when workers were abandoned (`daemon.py:691-699`, `:714`).
- `_spawn`: record `_workers[name]` and `_started_at` right after `start()`, before the log line
  (`:268-270`).

#### A-18 — Stale daemon-side docs (`low-risk`)
- `model.py:929-931` (`"0m"` is now rejected); `_spawn` docstring `daemon.py:249-255` aligned with
  `db.py:732-738`; `daemon.py:45-49` — the 300s cap is unreachable with 6 failures (largest delay 80s);
  keep it.

#### A-19 — Tests (`pytest-test-author`, `low-risk`)
- R1: lstat stat→None with access False passes; stat→stat raises; second lstat `PermissionError` →
  `ConfigDbPermissionError`; `_ensure_mode` not-owned vanish and owned chmod-ENOENT return quietly.
- R2 in-process: (a) A's phase 3 first runs B's full `import_yaml` on the same path, then raises
  "database is locked" → DB survives with B's config and the WARNING says so; (b) a raw sqlite3 reader
  that has read → DB, `-wal`, `-shm` keep their inodes and the reader still reads; (c) a zero-byte file
  from a phase-2 failure is removed; (d) a busy probe removes nothing; existing cleanup tests stay green;
  optional cross-process variant (`integration`).
- A-11.2: refused, nothing created, orphan untouched.
- R3: `fchown`/`fchmod` failure or KeyboardInterrupt leaves nothing; retry gives `created=True`.
- R4: one `get_engine` purges other-key foreign-pid entries and collects once (patched `os.getpid`);
  other-key fork test with GC disabled in the child.
- R5: duplicate and exact-duplicate remotes rejected by `from_file`; importing one creates nothing.
- R6: `client_id_file` `""` → NULL + warning, reloads `None`; `Path("")`/`"."` rejected; 3770 directory
  keeps its sticky bit; abandoned workers with code 0 exit 1; `_spawn` records the worker even if the
  log call raises.
- Bools: four fields × 0/1 → bool + warning through both doors; `2`, `-1`, `1.0`, `"1"` rejected;
  in-code int still rejected by `validate_config`; flip `dataset-enabled-int`
  (`test_zfsbackup_config_audit.py:276`) and `enabled-int`/`dry-run-int` (`:509`).
- Leave `test_require_writable_false_skips_the_sidecar_check` as is.

#### A-21 — Re-review follow-up *(approved 2026-10-17; separate commit)*
Both re-reviews found the round correct and ready to commit; these low items follow in their own commit,
straight to the developers (fixes fully specified by the reviewers), then tests and a short re-review.
- **Store** (`importer.py`, `mapper.py`, `paths.py`, `db.py`):
  1. A-11 residual window is wider than documented (reproduced with a forced stall): a whole concurrent
     re-import can run between the probe's ROLLBACK and the unlink. Fingerprint the target with `lstat`
     `(st_ino, st_size, st_mtime_ns, st_ctime_ns)` right after the probe closes (for zero-byte files, at
     the top of `_remove_if_unused`) and require it unchanged at step 5. Size alone is not enough. Correct
     the window description at `importer.py:48-51`, `:402-406` and the `_probe_config_empty` docstring.
  2. A-11.2 wrong advice when two first imports race: after finding sidecars, `os.stat(target)` again;
     if it now exists, raise "created concurrently by another process; retry" with no removal advice.
  3. Skip WARNING wording: "… exist (another process may have it open)".
  4. `validate_config`'s `client_id_file` check normalises before comparing (`os.path.normpath(raw) ==
     "."`), so `"./"` and `"a/.."` are refused too.
  5. `dispose_all`: run `gc.collect()` inside the (now re-entrant) lock, as `get_engine` does.
  6. Stale header `paths.py:~1694-1695` ("record-only. Nothing here is called yet").
- **Daemon** (`model.py`): reject `client_id_file` naming the current directory in `from_file`, using the
  same normalised rule; update the stale comment at `model.py:~710-714` (import exists and calls
  `validate_retention_uniqueness`).
- **Tests:** each of the above, including a forced-stall test for (1) and the concurrent-create message
  for (2).
- **Documented exception:** the one remaining `from_file`-accepts / `validate_config`-rejects class is
  any retention age/keep_for collision (both axes, both scopes), not only same-seconds keep_for.

- **As landed:** the fingerprint is taken once, at the top of `_remove_if_unused`, before the probe —
  not after it. A first version re-took it after the probe closed; the concurrency re-review reproduced a
  whole concurrent import landing between the probe's close and that retake and becoming the baseline.
  The probe never changes the fingerprint (no WAL frames exist at entry; measured 0/200 on WAL and
  rollback-journal DBs). Remaining residual, closed by item 15.4: an opener that has run no statement as
  of the final sidecar check, and on a non-empty file a writer whose commit and checkpoint land within
  one filesystem timestamp tick of the baseline `lstat`.

#### A-20 — Plan doc (main session, `low-risk`)
- Audit-table rows, A-4.2 owner wording, item 10.2/11/15 amendments — done with this insertion.


---

## Phase 2 — Config CLI *(revised after Checkpoint A, approved 2026-10-17)*

**Order: 10 → 15 → 13 → 11 → 12 → 14 → 16.** Item 15 precedes every write command. Lanes: store
10.1 → 15.1 → 10.3; daemon 15.2 → 14.1 (parallel with the store lane; 15.2 needs 10.1's constants);
CLI 10.2 → 15.3 → 13 → 11 → 12 → 14.2 (critical path). Item 16 follows each CLI item.
`concurrency-reviewer` runs alongside on 10.1, 15.1 and 15.2.

> **Corrections to the original Phase 2 text:** `global_settings.generation` already exists in the
> baseline (`models.py:178`), so item 15 needs a CAS API, not a migration. No Phase 2 item needs a
> schema change: every CLI write goes through the full-replace writer, including destination rename
> (the `dataset_remotes.destination_name` FK has no ON UPDATE action). Renaming a destination does not
> orphan server data — `_server_dataset` builds the path from client id + dataset name — but it does
> drop anchor protection, because `get_anchors` reads only `anchor.<configured destination>`.

### Item 10 — CLI skeleton, path wiring, `import` / `export` / `migrate`
- **10.1** — `zfsbackup-store-developer` · `needs-approval` · gate + `concurrency-reviewer`.
  - **B1, store half:** `EXPECTED_HEAD` and `KNOWN_REVISIONS` constants in `migrate.py`;
    `check_schema` decides from them (no `ScriptDirectory` read on the check path, `migrate.py:305-307`);
    the upgrade path first asserts the script head equals `EXPECTED_HEAD`.
  - **`upgrade_schema(resolved) -> SchemaUpgrade(from_revs, to_rev, changed)`:** its own FK-off
    connection with no surrounding transaction, opened with an explicit `BEGIN IMMEDIATE` (SQLAlchemy's
    pysqlite recipe) so DDL is transactional. **B2:** `PRAGMA foreign_key_check` before commit — any
    row rolls back and raises `SchemaIntegrityError`. Commit is the last statement; the connection is
    closed before returning. `ensure_schema` gets the same check.
  - `SchemaOutOfDate` docstring and message (`migrate.py:125-150, 362-370`) → `zfsbackup-config
    migrate` (today it points at `import`, a full replace).
  - **Pre-import backup contract (D-5 / 8c):** `import_yaml(yaml_path, resolved, *, backup=None,
    allow_unreadable_outgoing=False) -> ImportResult(…, backup_path)`. Phase 3 loads the outgoing config
    inside the same `BEGIN IMMEDIATE` before `save_config` and passes it to `backup`; if `backup`
    raises, everything rolls back. An unreadable outgoing config raises `OutgoingConfigUnreadable`
    unless allowed.
  - **Verify:** `pytest tests/test_zfsbackup_store.py -k "expected_head or upgrade_schema or
    foreign_key_check or import_backup"`, using a synthetic failing revision in a temp script dir.
- **10.2** — `zfsbackup-cli-developer` · `needs-approval`.
  - **Files:** new `zfsbackup/cli/{__init__,__main__,main,render,errors}.py`; `pyproject.toml`.
  - **Global:** `zfsbackup-config [-c DB] [-v]`, path from `resolve_config_path` with the daemon's order
    (6b). Only `import` may create a DB. Show A-11's "cleanup skipped" WARNING together with the import
    error. A-11 currently only logs it; 10.1 adds the store side — `_remove_created_target` attaches the
    same text to the in-flight exception with `exc.add_note` — so the CLI never has to capture log records. One error-mapping module; exit codes 0 ok, 1 invalid/refused,
    2 usage, 75 conflict/busy (retryable), 78 config DB unusable.
  - **`import <yaml>`:** always a full replace (no `--replace` flag — item 8). Prints warnings from
    `from_file` and `validate_config`. Writes `<dbdir>/<dbname>.pre-import-<UTC>.yaml` (O_EXCL, 0660)
    and names it in the replace warning; `--no-backup` opts out and is the only way past an unreadable
    outgoing DB (Q9).
  - **`export [-o FILE]`:** stdout by default; `-o` refuses to overwrite without `--force`.
  - **`migrate [--check]`:** prints from→to or "up to date"; `--check` exits 1 when behind; after a
    change, says to restart the daemon.
  - **`render.py` lives here** (export, import backup and the editor all use it). Contract:
    `from_file(render(c))` reproduces `c` in value and literal, apart from D1's dedupe; 3c-derived
    fields omitted; `enabled`/`recursive`/`dry_run` always explicit booleans; never-prune rendered as
    `retention: forever`; a destination's `retention` only when it overrides; destinations sorted by
    name; durations as quoted literals.
  - **D6:** `packages = [{include="libzfseasy"},{include="zfsbackup"}]`; console script
    `zfsbackup-config = "zfsbackup.cli.main:main"`; check the wheel contains `migrations/script.py.mako`
    and `versions/*.py`.
  - **Verify:** `pytest tests/test_zfsbackup_cli.py -k "skeleton or import or export or migrate or
    render"`; `poetry build && unzip -l dist/*.whl | grep migrations`.
- **10.3** — `zfsbackup-store-developer` · `low-risk`. Delete the interim `_main`, `__main__` and the
  "8d — interim CLI" docstring (`importer.py:101-112, 347-397`). Only after 10.2 and the A-9/D7 switch.
  Main session updates CLAUDE.md and `zfsbackup/README.md` in the same cycle. **Verify:**
  `grep -rn config.store.importer zfsbackup scenarios CLAUDE.md` is empty; full suite green.

- **10.4 — Review fixes** *(after the item-10 review, 2026-10-17)* — store + CLI developers in parallel.
  - **Store:** (1) never modify a database that isn't ours: when no revision is recorded, `check_schema`
    and `upgrade_schema` refuse if `sqlite_master` holds any table (new `SchemaError` subclass, "not a
    zfsbackup config database"), and `upgrade_schema` refuses `from_revs == ()` entirely, pointing at
    `import` — `migrate` only upgrades a known, recorded revision; the no-revision `SchemaOutOfDate`
    wording points at `import`. (2) Read-only engines no longer set `journal_mode=WAL`; they query it and
    warn if it isn't `wal` (writers still switch). (3) `MigrationPackagingError(RuntimeError)` (not a
    `SchemaError`) raised by `_assert_scripts_match_constants`. (4) Neutral `SchemaOutOfDate` text (the
    daemon appends its own sentence); `OutgoingConfigUnreadable` ends at "Nothing was changed." (the CLI
    adds `--no-backup`); "the stored configuration was not changed (the schema may have been upgraded)"
    instead of "nothing was changed / untouched". (5) `assert connection.in_transaction()` before
    `command.upgrade`, and document that migrations must not use `autocommit_block()`. (6) Test or drop the
    `session.expunge_all()` claim. (7) Write the migration-constants rule into `migrations/README`.
  - **CLI:** (1) `sqlalchemy.exc.DatabaseError` (not `IntegrityError`) → 78 via `config_error_message`
    (a valid header with a corrupt body exits 1 today). (2) Only `MigrationPackagingError` is an
    "installation error"; a bare `RuntimeError` is an internal error. (3) Don't print the A-11 cleanup
    text twice. (4) ^C: print `__notes__`; say "the import may or may not have been applied — run `export`
    to check" rather than asserting either way. (5) `write_backup`: close the fd if `fchmod` fails, unlink
    on any `BaseException`, fsync the directory after creating the file. (6) `export -o` writes a temp file
    and renames it into place. (7) `migrate` success text: a running daemon will stop at its next worker
    restart — restart it now.
  - **Daemon:** `runtime_config.py:45-49` no longer claims `check_schema` raises the multi-head
    `RuntimeError`; the daemon's out-of-date message appends its own restart sentence.
  - **Deferred:** a single source for semantic config warnings (`validate_config`; `from_file` keeps only
    parse-boundary warnings) — needs a model/store contract change, do it with items 11/12 (the CLI
    currently prints e.g. the `forever` warning twice in different words); a raw `.db` copy when the
    outgoing config is unreadable; a first import's transient "uninitialized" window mapped as a refusal.

- **10.5 / 10.6 — Follow-up rounds** *(done)*. 10.4's review found the writer still switched a foreign
  Alembic-managed DB to WAL and `export -o --force` replaced FIFOs/devices. As landed:
  - One rule decides both "may this file be switched to WAL" and "is this file ours":
    `migrate.may_switch_to_wal` + `_unknown_revisions`. Switch only when the file has no tables, or
    `alembic_version` has ≥1 row and every row is in `KNOWN_REVISIONS`; an empty `alembic_version`
    never switches. Only "no such table/column" errors are read as "not ours"; locked/corrupt propagate.
  - `SchemaSplitBrain` only when *all* of our tables are present with no stamp; any subset → `NotAConfigDatabase`.
  - The connect listener closes the raw DBAPI connection when it raises — SQLAlchemy 2.0.51 doesn't, so
    each failed connect (e.g. a corrupt-body file) used to leak an fd invisible to `dispose_all()`.
  - CLI: `--force` renames only over regular files (others written in place); `mkstemp` temp names;
    owner/mode kept; directory fsync; one-line damaged-DB message (78); other SQLAlchemy errors are
    internal errors (1); SIGTERM handled like ^C (exit 143); the ^C "may or may not have been applied"
    verdict printed once without dropping the "Never delete its -wal/-shm files" warning.
  - **Deferred:** a WAL switch on a never-WAL file fails immediately against another process's RESERVED
    lock (SQLite skips the busy handler there; pre-existing, rare); the daemon still words a damaged DB
    with the generic "Failed to load configuration" text (fix with 15.1's shared `config_error_message`);
    workers keep one idle pooled connection for life (moot after 15.2(b): workers stop reading the DB).

### Item 15 — Generation CAS, daemon presence, consistent worker config *(moved ahead of 11/12/14)*
- **15.1** — `zfsbackup-store-developer` · `needs-approval` · gate + `concurrency-reviewer`. **Basis:**
  B2, `mapper.py:684-685`. No migration.
  - `load_config_snapshot(session) -> ConfigSnapshot(config, generation)` from one read transaction.
  - `save_config(…, expected_generation=None)` compares inside the caller's `BEGIN IMMEDIATE`, before
    any DELETE, raising `ConfigConflict`.
  - `write_config(resolved, config, *, expected_generation) -> new_gen`: preflight → `validate_config`
    → writer session → CAS → save → commit. Maps "database is locked" after `busy_timeout` to
    `ConfigBusy`. Never returns a session.
  - Rewrite the generation comment at `models.py:162-177`. `import` bumps the generation too, so an
    import between render and save is a conflict.
  - **Shared read helper** *(added after the item-10 review)*: `read_config(resolved) ->
    ConfigSnapshot(config, generation, schema_rev)` in the store's public API — one read-only connection,
    `BEGIN`, `check_schema`, `load_config_snapshot`, `COMMIT`, close. Move `check_config_schema`,
    `config_error_message` and `describe_config_source` from `runtime_config.py` into the store. The CLI
    switches `export`/`migrate --check` to it; `load_runtime_config` becomes a thin daemon wrapper (15.2).
    This lands 15.2(a)'s schema-check-inside-the-snapshot once, for both tools.
  - **Verify:** `pytest tests/test_zfsbackup_store.py -k "generation or cas or busy"`
- **15.2** — `zfsbackup-developer` · `needs-approval` · gate `concurrency-reviewer`.
  - **(a) B1, daemon half:** `load_runtime_config` runs `check_schema(session.connection())` inside its
    existing BEGIN (`runtime_config.py:183-232`), closing the residual window that comment describes;
    returns the generation; the supervisor logs "generation N, schema R" at startup.
  - **(b) B3 — config handoff (Q4):** `BaseWorker`/`ApiWorker` take the supervisor's detached
    `BackupConfig` with dry-run already applied (`daemon.py:569-570`); `_load_config`
    (`workers.py:123-163`) is deleted and workers never touch the DB. Under `spawn`,
    `Duration.__reduce__` preserves literals. Phase 3's item 18 restarts workers with the new object.
  - **(c) Daemon presence lock** — new `zfsbackup/daemonlock.py` (B5, 15.3's notice, basis for item
    19). After config load and before the first fork, the supervisor opens `<db>.daemon.pid` (0660),
    takes `flock(LOCK_EX|LOCK_NB)` (held → exit 1 naming the other pid, which also blocks two daemons on
    one DB), writes `pid\ngeneration\n`, holds the lock for life, removes the file on clean exit.
    `probe(resolved) -> Optional[(pid, generation)]` tests with `LOCK_SH|LOCK_NB` on a fresh fd; a stale
    file is harmless because "running" means the lock is held. Under `fork`, workers inherit the lock
    (orphans still count as running); under `spawn` they don't — document it.
  - **Verify:** `pytest tests/test_zfsbackup_daemon.py tests/test_zfsbackup_workers.py
    tests/test_zfsbackup_runtime_config.py -k "generation or handoff or daemon_lock or
    schema_in_snapshot"`
- **15.4 — CLI writer lock** *(added by the A-11 review round, approved)* — `zfsbackup-store-developer`
  (lock helper) → `zfsbackup-cli-developer` (use). `flock(LOCK_EX)` on `<db>.writer.lock` (0660, in the
  setgid directory, never deleted; not `<db>.lock`, which SQLite's unix-dotfile VFS uses). Every CLI write
  takes it, including `import`, which takes it before phase 1 decides whether the file exists and holds it
  through cleanup — serialising a first import against a concurrent re-import so A-11's unlink happens
  under the lock. A-11's cleanup also skips the unlink when 15.2(c)'s `daemonlock.probe()` finds a daemon.
- **15.3** — `zfsbackup-cli-developer` · `needs-approval`. Every write: snapshot → mutate/parse →
  `validate_config` (print warnings) → `write_config(expected_generation)`. `dispose_all()` before
  launching `$EDITOR` or prompting — nothing stays open. `ConfigConflict` → exit 75 "config changed
  underneath you (generation A, now B); re-run"; `ConfigBusy` → exit 75 "another write in progress;
  retry". On success, `probe`: daemon found → "config updated (generation N); running daemon pid P is on
  generation M and will not apply this until restarted"; otherwise "no running daemon detected for
  <db>". The CLI never sends a signal. **Verify:** `pytest tests/test_zfsbackup_cli.py -k "conflict or
  busy or daemon_notice"`

- **Item 15 as landed** *(reviewed by both reviewers)*:
  - `write_config` returns `WriteResult(generation, warnings)`; it runs a read-only schema pre-check
    before the writer lock and the authoritative `check_schema` inside the write transaction.
  - `import_yaml` runs a read-only foreign-DB pre-check before the writer lock (zero-byte targets skip
    it — they are judged under the lock — so a zero-byte target can still get a lock file); refusals
    before the lock create nothing.
  - `EX_CONFIG` is gone: workers can no longer hit a config error; the supervisor refuses a bad config
    with exit 1 before any worker exists.
  - `SchemaOutOfDate.recorded_revisions` lets `migrate` refuse a file with nothing recorded before
    locking.
  - The 15.4 line "A-11 cleanup skips the unlink when `daemonlock.probe()` finds a daemon" was not
    implemented: the writer lock already serialises zfsbackup writers, and a running daemon holds an
    inode the cleanup can't touch. **Revisit in Phase 3**, when reload reopens the path.
  - `daemonlock.acquire` retries contention for up to 1 s, so a CLI `probe()` can't make a starting
    daemon refuse.
  - **15.5 review fixes:** the daemon takes its presence lock *before* reading the config (so "no
    running daemon detected" can't race a starting daemon); a dead supervisor pid whose forked workers
    still hold the lock is reported as such; `read_zfsbackup_properties` filters to `org.zfsbackup:`
    before building libzfseasy objects (unknown built-in props such as zvol `volthreading` used to raise);
    `ImportResult.generation`; `write_config`/`commit_config` release connections; stale docs.
  - `read_zfsbackup_properties` reads raw `zfs get` lines through libzfseasy's private `_zfs_cmd` and
    `zfs.get._exec_out` — a private-API dependency; replace it when libzfseasy's `get` parser learns to
    skip unknown built-in properties (a `needs-approval` libzfseasy change).
  - **Known residual:** generations are per file — deleting and re-importing the DB restarts at 0, so an
    edit read from the old file could pass CAS against the new one (ABA). Rare; a store id in the
    snapshot would close it.

- **Items 11–14.2 as landed** *(reviewed by both reviewers; fix round M1–M3, L1–L11)*:
  - **Anchor decision (user, 2026-10-18 — "overwrite + protect all"):** `rename destination` overwrites a
    differing pre-existing `anchor.NEW` with `anchor.OLD` (both values printed; the snapshot the old
    value named is no longer pinned by it) via `copy_anchor_property(..., overwrite=True)` →
    `no-source | copied | exists | same | overwritten | dry-run`; `DatasetManager.get_anchors` protects
    the snapshot of every local or received `org.zfsbackup:anchor.*` (not only configured
    destinations); a failed anchor read skips pruning that dataset for the cycle. Advice after a rename:
    restart the daemon; keep `anchor.OLD` until a send to NEW succeeded, then `zfs inherit` it. This
    supersedes the item-15 note that a renamed destination drops anchor protection.
  - `rename dataset --clear-stale-props`: refused up front when ZFS is unavailable or NEW is absent
    while OLD exists; the daemon check runs AFTER the write (originally a shared `flock` on
    `<db>.daemon.pid` held through every `zfs inherit`; since 08e209e a single `daemonlock.probe()`,
    no lock held — replaces B5's probe-before-write); `rename destination
    --skip-missing`; destination names validated as ZFS property names.
  - `edit`: unknown keys rejected at every level; rename-shaped and recreate-with-shared-destination
    detection; a removed destination's anchor "stays behind and keeps pinning its snapshot until cleared
    with `zfs inherit`"; editor signal handling like git (SIGINT/SIGQUIT ignored, SIGTERM forwarded,
    10 s grace).
  - **Deferred:** recursive pruning destroys `@snap` on child datasets without consulting the children's
    anchors (pre-existing); a modified client could send a `-p` stream with `anchor.*` properties that
    pin snapshots on the server (no new exposure — `retention: forever` already does; note it in the
    readiness report); the no-real-zfs test guard covers only the pytest process.

### Item 11 — Dot-path `get` / `set` / `unset`
- **Owner:** `zfsbackup-cli-developer` · **Tag:** `needs-approval` · **Files:** `cli/dotpath.py`,
  `cli/main.py`
- **Dotted names** (unchanged): schema-directed longest match; `datasets[tank/vm.01]` bracket form
  always accepted and emitted; ambiguity is an error, never a guess.
- **Addressable paths:**
  - Globals: `snapshot_prefix`, `check_interval`, `api_host`, `api_port`, `dry_run`; tri-state
    `prune_interval` and `client_id_file` (`unset` = derive; `get` shows `derived (5m)`).
  - `datasets.<n>.{enabled,recursive,frequency}`.
  - `datasets.<n>.retention`: `set forever` = never prune + WARNING; `unset` = default 1d→30d.
  - `datasets.<n>.retention.<age>`: `set` adds/updates (matched by seconds, literal stored as typed);
    `unset` removes; removing the last rule = never prune + WARNING.
  - `datasets.<n>.remote.<dest>.frequency`: `set` overrides, `unset` inherits, `get` shows
    `inherit (1h)`.
  - `datasets.<n>.remote.<dest>.retention[.<age>]`: the first override rule replaces the inherited set
    (print the resulting effective set); removing the last override rule or `unset` on the key =
    inherit.
  - `destinations.<name>.url`.
  - `remote_backup.{target_dataset,enabled}`: setting `target_dataset` with no server role creates it
    with `enabled=true` + WARNING that the receive API has no authentication; `unset remote_backup`
    removes the role.
- **Not addressable:** names (`rename`) and membership — adding/removing datasets, attaching/detaching
  destinations (`edit`). The error points there.
- **Coercion:** values are parsed through the same YAML-boundary guards as `from_file` (so `set … enabled 1`
  gets A-15's accept-with-warning); durations and ports via A-1's guards; ≤ 0 rejected; `client_id_file ""`
  means unset (A-14.1).
- **Verify:** `pytest tests/test_zfsbackup_cli.py -k dotpath`, including dotted names.

### Item 12 — Whole-config `$EDITOR` round-trip
- **Owner:** `zfsbackup-cli-developer` · **Tag:** `needs-approval` · **Basis:** the import/`from_file`
  round-trip contract of 10.2 (the old "drop-in with `-c file.yaml`" basis is obsolete).
- **Flow:** (0) snapshot `(config, G)`, then `dispose_all()`. (1) render to a 0600 file in a 0700 dir.
  (2–3) `$VISUAL` → `$EDITOR` → `vi`, `shlex.split`, inherited stdio; non-zero exit aborts. (4) "No
  changes" is a canonical value + literal comparison after D1 dedupe — not bytes, and not dataclass `==`
  (`Duration.__eq__` ignores literals, so a `30d→1M` edit would be lost). (5) `from_file` +
  `validate_config`; show warnings; on error Retry/Abort, buffer preserved. (6) Names are set
  membership; rename-shaped diffs refused unless `--allow-recreate`. **New:** dropping a destination
  from a dataset's `remote:`, or deleting a destination, warns that its `anchor.<dest>` stops
  protecting snapshots. (7) `write_config(expected_generation=G)`; on conflict keep the buffer at a
  stable path.
- **Edge cases:** a CLI killed in the editor holds no DB handles; non-tty stdin is refused with a
  pointer to export/import.
- **Verify:** `pytest tests/test_zfsbackup_cli.py -k editor`

### Item 13 — `list datasets` / `list destinations`
- **Owner:** `zfsbackup-cli-developer` · **Tag:** `low-risk`. Read-only via the snapshot reader;
  columns follow `backup_manager.py:99-110`; retention shows `never` or a rule count; remote frequency
  shows `inherit (X)`; `--json`, `--enabled-only`; datasets by id, destinations by name; the
  destinations list shows name, URL and referencing datasets. **Verify:** `-k list`

### Item 14 — Per-dataset / per-destination edit and `rename`
- **14.1** — `zfsbackup-developer` · `needs-approval`. Module-level helpers in `backup_manager.py`
  needing no `BackupConfig`: `read_zfsbackup_properties(ds)`; `copy_anchor_property(ds, old_dest,
  new_dest)` (sets `anchor.<new>` only if absent; never deletes);
  `clear_zfsbackup_properties(ds, *, config, anchors)` via `zfs inherit`. All take `dry_run`.
  **Verify:** `pytest tests/test_zfsbackup_manager.py -k anchor_property`
- **14.2** — `zfsbackup-cli-developer` · `needs-approval`; after 14.1 and 15.
  - **`edit dataset <n>` / `edit destination <n>`:** no `name:` in the buffer (a present `name:` is
    rejected with a pointer to `rename`); the fragment is parsed with `DatasetConfig.from_dict`, merged
    into the snapshot, validated as a whole config, written with CAS.
  - **`rename dataset <old> <new>`:** stale-property warning; `--force` when remotes are configured
    (the server-side path changes, so the next send is full); refuse if `<new>` exists. **New:** if
    `<old>` no longer exists in ZFS and `<new>` carries `org.zfsbackup:*` properties, the dataset was
    `zfs rename`d — no stale-property warning, but `--force` still applies.
  - **`rename destination <old> <new>`:** one full-replace write. Before it, `copy_anchor_property` on
    every referencing dataset — copy, not move, so a daemon still on the old config stays protected.
    Print the stale `anchor.<old>` properties. No `--force`: the incremental base is preserved and the
    server path is unchanged.
  - **`--clear-stale-props`:** refused while `probe()` reports a running daemon (B5).
  - Deleting a still-referenced destination is refused, listing the referencing datasets.
  - **Verify:** `pytest tests/test_zfsbackup_cli.py -k "edit_dataset or edit_destination or rename"`

### Item 16 — Tests for Phase 2
- **Owner:** `pytest-test-author` · **Tag:** `low-risk`; written alongside each item, not at the end.
- **Coverage:** `CliRunner` for every subcommand; every item-11 path including every tri-state and both
  dotted-name forms; the decision-(a) audit rows through the CLI; the render round-trip property (repo
  YAMLs, never-prune, non-canonical per-destination literals); a bad value leaves the DB byte-identical
  (hash); `$EDITOR` stub — no-op, literal-only edit persists, abort, invalid, rename refusal and
  `--allow-recreate`, anchor-loss warning; CAS (write between render and apply rejected; busy mapped);
  daemon lock (held, free, stale file); handoff (a respawned worker keeps the supervisor's generation
  after a DB write); destination rename copies anchors and never deletes; the B5 refusal; `migrate`
  (no-op, synthetic upgrade, `foreign_key_check` failure rolls back); the import backup (name, mode,
  O_EXCL; refusal on an unreadable outgoing DB); the console script resolves (`-m slow`).
- **Verify:** `pytest tests/test_zfsbackup_*.py`

**Deferred:** the `snapshot_prefix`-with-`_` parser bug (`backup_manager.py:373-382`; needs
`name[len(prefix)+1:]`, which is retention logic — warning only for now); never-prune at destination
level; C3's getppid detection, `KillMode=mixed`, websocket timeouts; lenient `remote:`/`destinations:`
null (YAML never allowed it).

> ### ⛳ CHECKPOINT B — review before Phase 3
> The CLI is fully usable at this point, with the item-15 "restart required" notice as the honest
> stopgap. Phase 3 is where multiprocessing risk concentrates; confirm the reload design below before
> it starts.
>
> **Held 2026-10-18.** Phase 2 was found complete. The concurrency stress run used 3,294 CLI
> invocations; generations were consecutive and nothing was corrupted. Close-out fixes:
> - SIGHUP is caught during daemon startup, and the hand-over only ever sets the flag.
> - Engines are disposed before the startup SIGHUP disposition is restored.
> - Orphaned workers exit on their own.
> - An API bind failure exits cleanly.
> - Dry-run never writes properties.
> - The anchor copies for `rename destination` run under the writer lock.
> - Writer-lock identity is compared by inode.
> - The console-script CLI ignores SIGINT/SIGTERM after `main()`.
> - Tests are hermetic: a stub zfs, `ZFSB_REAL_ZFS=1` opt-in for the real tier, and the orphan
>   safety net.
>
> **Carried into Phase 3's worker-lifecycle design (item 18):**
> - **Forkserver orphan plus pid reuse.** An orphaned worker's parent-death check compares pids
>   only. Key it on pid + start time.
> - **The spawn/forkserver orphan overlap.** Under spawn/forkserver, workers don't inherit the
>   presence lock. A `<db>.workers.lock` held `LOCK_SH` by every worker would let a restarted
>   daemon see survivors of its predecessor.
> - **`ApiWorker` shutdown.** It should call `server.shutdown()` on stop rather than rely on
>   process exit.
> - **`_wait` returns only a bool.** Reload needs to know *why* a wait ended (stop or reload), so
>   it should return a reason.

---

## Phase 3 — Live config reload

*(Revised at Checkpoint B; decisions recorded 2026-10-18. Supersedes the original Phase 3 text — the
event-based wake, per-worker `multiprocessing.Event` control events, the separate pidfile, and 15.3's
"the CLI never signals". Line references are to `80604de`.)*

### Decisions (user, 2026-10-18)

| # | Decision |
|---|---|
| D1 | Two triggers — **SIGHUP and a generation poll** — feeding one code path. |
| D2 | Poll every **10 s** (`daemonlock.RELOAD_POLL_SECONDS`), no debounce; reloads coalesce. |
| D3 | **`--no-reload` dropped.** Every committed write is applied; atomic multi-change goes through `edit` / `import`. |
| D4 | After every committed write the CLI **SIGHUPs the running daemon**, only after verifying it (17.1). EPERM or "cannot verify" falls back to the poll — not an error, exit code unchanged. |
| D5 | In-flight work is **never killed on reload**; the switch is deferred indefinitely, with logging. |
| D6 | **Staggered** turnover: each worker switches at its own safe point. |
| D7 | Websocket per-operation **timeouts land now** (18.4). |
| D8 | Per-file **store id in `PRAGMA user_version`**; reload key and CAS use `(store_id, generation)`. |
| D9 | A crash loop after a reload **exits 1** as today (no rollback); the message names the generation. |
| D10 | `validate_config` runs at **startup**, `--test-config` and every reload. |
| D11 | Start method stays the **platform default** (spawn on macOS, fork on Linux ≤3.13, forkserver on Linux ≥3.14). |

Fixed inputs: `--dry-run` survives every reload; the supervisor `dispose_all()`s after every poll and every
reload read; a worker receives its config and its epoch together, from one `Handoff` object.

### Contracts (every item refers back to these)

1. **Reload key** — `(store_id, generation)`. The poll triggers when the stored key differs from both the
   running key and the last rejected key; SIGHUP always forces a full read.
2. **Epoch** — a supervisor-local `int`, +1 per accepted reload; never the DB generation. It is shared through
   `stopflag.ReloadEpoch`. A worker whose `born_epoch` differs from the shared epoch exits at its next safe
   point.
3. **Worker exit codes** — `0` stop or supervisor gone; `1` crash or API bind failure; `3`
   `WORKER_EXIT_RELOAD`. Code 3 counts as a turnover only when `born_epoch < current epoch`; otherwise it is a
   crash.
4. **Safe points** — loop top, between datasets, between remote destinations, and each `_wait` slice. Never
   inside `_process_dataset`, never inside a stream.
5. **Pid file v2** (`<db>.daemon.pid`) — lines 1–2 unchanged (pid, applied generation). From line 3 on,
   `key=value` lines:
   - `start=<token>` — always written by a v2 daemon; its presence means "supports live reload";
   - `applied=<store>:<gen>`;
   - `target=<store>:<gen>` — only while a reload is being applied;
   - `rejected=<store>:<gen>`.

   Unknown keys are ignored, so older readers keep working.
6. **New file** — `<db>.workers.lock` (0660, never deleted, like `.writer.lock`).

### Order and review gates

Two lanes run in parallel (disjoint owners); everything inside the daemon lane is serial (one owner).

- **Store lane:** 17.0 → **G1**
- **Daemon lane:** 18.0 → 17.1 → 18.1 → 18.2 → 18.3 → 18.4 → 18.5 → **G2** → 17.2 → **G3**
- **CLI:** 19.2 may start after G1 and 17.1 (read-only). 19.1 is implemented after G2 and lands after G3 → **G4**.
- **Tests (20):** written alongside every item. **21** after G3; **22** after G4; **23** last.

| Gate | After | Reviewers | Must clear before | Concurrency-reviewer focus |
|---|---|---|---|---|
| G1 | 17.0 | concurrency + zfs-code | 17.2, 19.1 | `user_version` stamp is transactional; `read_store_state` leaves no read mark (WAL growth) |
| G2 | 18.0–18.5, 17.1 | concurrency + zfs-code (18.4: remote/security) | 17.2 | signal targeting (lock-holder proof, pidfd race, torn pid-file reads); `ReloadEpoch` shared memory; exit protocol vs. safe points; API gate threads and `server.shutdown()`; `workers.lock` fd inheritance (`O_CLOEXEC`, no supervisor fd across fork); startup waits run with default SIGTERM |
| G3 | 17.2 | concurrency + zfs-code | 19.1 landing | handler stays one store; sleep wakes on the HUP flag; no fork with a session open; dispose after every read; `Handoff`-then-epoch ordering; stop beats reload everywhere; turnover vs. A-5 accounting |
| G4 | 19.x | zfs-code + concurrency | 22 | `request_reload` used exactly once, post-commit; ack loop holds no lock |

---

### Item 17.0 — Store id and cheap store state *(store)*

- **Owner:** `zfsbackup-store-developer` · **Tag:** `needs-approval` (config load/write contract) · **Gate:** G1
- **Files:** `config/store/mapper.py`, `access.py`, `importer.py`, `__init__.py`
- **Basis:** D8. A fresh import is generation 0 (`mapper.py:977,1097`), so delete + re-import repeats keys. The
  ABA residual is recorded under "Item 15 as landed". `read_generation` is at `access.py:~153-193`.

**Behaviour:**
1. **Stamping (`save_config`).** Inside the caller's write transaction, read `PRAGMA user_version` before the
   CAS.
   - If `expected_store_id is not None` and it differs, raise `ConfigConflict`: "the configuration database was
     replaced since it was read (store id A, now B); re-run".
   - After the CAS, if the value is 0, set `PRAGMA user_version = <secrets.randbelow(2**31 - 2) + 1>` (an int we
     generated, formatted into the statement).
   - Never re-stamp a non-zero value. Only `save_config` stamps; `create_config_db_file` must not open SQLite.
2. **Snapshot field.** `ConfigSnapshot.store_id: int = 0` (default keeps positional constructors working), read
   in `load_config_snapshot`'s transaction.
3. **Never-prune flag.** `read_config(resolved, *, warn_never_prune=True)` and
   `load_config_snapshot(..., warn_never_prune=True)` pass it through to `load_config`.
4. **Write path.** `write_config(..., expected_store_id: Optional[int] = None)`. `WriteResult.store_id: int = 0`
   is the id after the write. `ImportResult.store_id` likewise.
5. **Poll read.** New frozen `StoreState(store_id, generation)` and `read_store_state(resolved)`:
   - one read-only connection: `BEGIN`; `PRAGMA user_version`; `SELECT generation FROM global_settings WHERE id = 1`;
     `COMMIT`; close;
   - no schema check; does not dispose (the caller does);
   - raises the same errors as `read_generation`, which itself is unchanged.
6. **Exports.** Add `StoreState` and `read_store_state` to `__all__` and to the "Public API" docstring.

**Edge cases:**
- A rollback after stamping leaves 0.
- `upgrade_schema` and every migration preserve `user_version`.
- Foreign-DB detection never consults it.

**Tests** (`test_zfsbackup_store_access.py`, `_store.py`, `_store_upgrade.py`):
- first save stamps a non-zero id; later saves keep it; a legacy DB (0) is stamped on its next write;
- a failure after the stamp leaves 0; delete + re-import gives a new id;
- a stale `expected_store_id` → `ConfigConflict` with the DB hash unchanged; `None` skips the compare;
- `read_store_state` on missing, uninitialised and unmigrated DBs raises the documented errors, leaves nothing
  checked out and no read mark (`PRAGMA wal_checkpoint(PASSIVE)` is not blocked afterwards);
- `upgrade_schema` keeps the id;
- `read_config(warn_never_prune=False)` logs no never-prune line;
- `ConfigSnapshot(config, generation, schema_rev)` still constructs.

**Verify:** `pytest tests/test_zfsbackup_store*.py -k "store_id or store_state or never_prune"`

### Item 18.0 — `zfsbackup/procident.py`: process identity and flock holders *(new module, stdlib only)*

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (signal targeting) · **Gate:** G2
- **Basis:** Checkpoint B carry-over "key it on pid + start time"; D4(a). This module is shared by `daemonlock`
  (17.1) and `workers` (18.2), and must not import `zfsbackup.config.store`.

**API:**
- `start_token(pid) -> Optional[str]`
- `identity(pid, token) -> 'match' | 'mismatch' | 'gone' | 'unknown'` — never raises.
- `flock_write_holders(inode) -> Optional[Set[int]]` — Linux only; `None` means unavailable.

**Platforms:**
- **Linux:**
  - **Start token:** read `{_PROC_ROOT}/{pid}/stat` (`_PROC_ROOT = "/proc"`, patchable). Take the text after the
    **last** `)`, split on whitespace; index 19 is field 22, `starttime`. Token: `linux:<starttime>`.
    `FileNotFoundError`/`ProcessLookupError` → gone; `PermissionError` (`hidepid`), other `OSError` or a parse
    error → unknown.
  - **Lock holders:** parse `{_PROC_ROOT}/locks`. Keep only lines whose type is `FLOCK` and access is `WRITE`.
    Skip blocked-waiter lines (`N: -> …`), `POSIX`/`OFDLCK`/`LEASE` and `READ`. Match on the decimal inode in
    `MAJ:MIN:INODE` only: the device can legitimately differ from `st_dev` on btrfs subvolumes and overlayfs.
    Return the set of pids (pid 0 means another namespace, so drop it).
- **macOS:** start token via ctypes `sysctl([1 CTL_KERN, 14 KERN_PROC, 1 KERN_PROC_PID, pid])` into a 1024-byte
  buffer.
  - non-zero return code → unknown; returned size 0 → gone;
  - `kinfo_proc.kp_proc.p_un.__p_starttime` sits at offset 0: `int64 tv_sec`, `int32 tv_usec` at offset 8;
  - token: `darwin:<sec>.<usec:06d>`;
  - `flock_write_holders` → `None`.
- **Other platforms:** `start_token` → `None`; `identity` → `'unknown'`.

`identity`:
- pid `None`/≤1, empty token, or unreadable → `'unknown'`;
- process absent → `'gone'`;
- tokens equal → `'match'`, else `'mismatch'`.

No boot id is needed: tokens are only compared while the presence lock is held, and no holder survives a reboot.

**Tests** (new `tests/test_zfsbackup_procident.py`):
- own token stable and non-`None` on Linux and macOS;
- a reaped child → `'gone'`; a child's token differs from the parent's;
- Linux parser fixtures in a temp `_PROC_ROOT`: `comm` containing `a) (b c` and spaces, a truncated file;
- `/proc/locks` fixture with FLOCK/POSIX/OFDLCK, READ/WRITE and `->` lines; on Linux only, a child holding
  `flock(LOCK_EX)` shows up for that inode;
- macOS only: `start_token` seconds within 1 s of `LC_ALL=C ps -o lstart= -p <pid>`;
- `sys.platform` patched to `freebsd` → `None`/`'unknown'`;
- `identity(1, t)` and `identity(None, t)` → `'unknown'`.

### Item 17.1 — Pid file v2, verified reload request, workers-lock helpers

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (signals) · **Gate:** G2
- **Files:** `zfsbackup/daemonlock.py`
- **Basis:** `_format`/`_parse` (`:219-235`), `DaemonLock` (`:251-336`), `acquire` (`:395`), `probe` (`:483`);
  D4; contract 5; Checkpoint B carry-over 2. The file is `0660` in a `2770` directory, which is why the
  lock-holder proof is required.

**Behaviour:**
1. **Writing the file.**
   - `DaemonLock.__init__` records `self.start = procident.start_token(os.getpid()) or ""`.
   - `_write` emits both lines plus `start=` and whichever of `applied`/`target`/`rejected` are set.
   - New `set_state(*, applied=_KEEP, target=_KEEP, rejected=_KEEP)`; each value is
     `Optional[Tuple[int, int]]`. Setting `applied` also rewrites line 2 with its generation.
   - `set_generation` is kept.
   - Writes stay `pwrite` + `ftruncate` + `fsync`. Failures log a WARNING and never raise.
2. **Parsing.**
   - `_parse` returns `pid`, `generation`, `start` (`None` when the key is absent, i.e. a legacy daemon),
     `applied`, `target`, `rejected`. Malformed values become `None`.
   - `DaemonInfo` gains those fields plus a `reload_capable` property; `DaemonAlreadyRunning` gains `start`.
3. **Stable reads.** `_read_stable(fd)` re-reads up to 3 times until two consecutive reads are byte-identical;
   otherwise the read is flagged unstable. `probe` and `request_reload` both use it.
4. **Shared constant.** `RELOAD_POLL_SECONDS = 10`. The daemon imports it; the CLI's wording uses it.
5. **`request_reload(resolved, *, skip_if_applied=None) -> ReloadRequest(outcome, pid, info, detail)`.**
   Never raises.
   - **Outcomes:** `no-daemon`, `already-applied`, `signalled`, `not-permitted`, `legacy`, `supervisor-gone`,
     `unverified`, `error`.
   - **Steps:**
     1. Open the file `O_RDONLY|O_CLOEXEC|O_NOFOLLOW`. Missing → `no-daemon`. Any other open error → `error`.
     2. `LOCK_SH|LOCK_NB` succeeds → unlock → `no-daemon`.
     3. Stable read. Unstable or pid `None` → `unverified`. No `start` key → `legacy`. Empty `start` →
        `unverified` ("identity not recorded on this platform").
     4. `applied == skip_if_applied` → `already-applied`, with no signal.
     5. Never target pid ≤1 or `os.getpid()`.
     6. If `os.pidfd_open` and `signal.pidfd_send_signal` exist (Linux ≥5.3), open the pidfd first.
        `ProcessLookupError` → `supervisor-gone`.
     7. **Lock-holder proof:** `procident.flock_write_holders(fstat(fd).st_ino)` must contain pid.
        - `None` (macOS/other, or `/proc/locks` unreadable) → `unverified` ("signals are verified only on
          Linux").
        - pid not in the set → `supervisor-gone`. This covers fork orphans whose supervisor died and a forged
          file; a forged file can only ever name the forger's own process.
     8. **Identity:** `procident.identity(pid, start)`. `'gone'`/`'mismatch'` → `supervisor-gone`;
        `'unknown'` → `unverified`.
     9. **Send SIGHUP:** `pidfd_send_signal(pfd, SIGHUP)`, or `os.kill(pid, SIGHUP)` without pidfd (the
        check-then-kill race of microseconds is documented).
        - `PermissionError` → `not-permitted`.
        - `ProcessLookupError` → `supervisor-gone`.
        - success → `signalled`.
   - Only SIGHUP is ever sent, never to a process group.
6. **Workers lock helpers.**
   - `workers_lock_path(resolved)` → `<db>.workers.lock`.
   - `ensure_workers_lock(resolved)`: create-if-absent, mode 0660, using the same `_fix_permissions` rules,
     `O_NOFOLLOW|O_CLOEXEC`, then close.
   - `hold_workers_lock(path) -> Optional[int]`:
     - opens `O_RDONLY|O_CLOEXEC|O_NOFOLLOW` and takes `LOCK_SH|LOCK_NB`, retrying for 1 s every 25 ms;
     - returns the fd, or `None` plus a WARNING.
   - `workers_present(resolved) -> bool`:
     - takes `LOCK_EX|LOCK_NB` on a fresh read-only fd; EWOULDBLOCK → `True`;
     - always unlocks and closes before returning;
     - a missing file → `False`.
7. **Module docstring.** "The CLI signals, after verifying" replaces "never signals". Add the start-method table
   for `.workers.lock`.

**Tests** (`test_zfsbackup_daemonlock.py`):
- **Format and parsing:**
  - the format round-trips, including empty `start=`;
  - a two-line legacy file parses as `start=None`;
  - unknown and malformed keys are ignored;
  - `set_state` keeps keys it was not given; `applied` rewrites line 2;
  - a torn read (patched `pread`) → `unverified`.
- **`request_reload` matrix**, using a real helper process that holds `LOCK_EX`, writes v2 content and records
  SIGHUP in a marker file:
  - everything genuine → `signalled`, marker written;
  - wrong `start` → `supervisor-gone`, helper not signalled and still alive;
  - file pid set to a decoy `sleep` that does not hold the lock, with the decoy's real start token →
    `supervisor-gone`, decoy alive (the forgery case);
  - pid gone → `supervisor-gone`; no `start` key → `legacy`; empty `start` → `unverified`;
  - `flock_write_holders` patched to `None` → `unverified`;
  - kill / `pidfd_send_signal` patched to raise `PermissionError` → `not-permitted`;
  - `skip_if_applied` equal to `applied` → `already-applied`, nothing signalled;
  - lock free → `no-daemon`; missing file → `no-daemon`; `EACCES` on open → `error`;
  - both the pidfd path and the `os.kill` path (patch `os.pidfd_open` away);
  - pid 1 and the test's own pid are never signalled.
- **Workers lock:** two `LOCK_SH` holders coexist; `workers_present` is `True` while one is held and `False`
  after the holder exits, including after SIGKILL; mode 0660; no fd leaks.

### Item 18.1 — `ReloadEpoch`

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (IPC) · **Gate:** G2
- **Files:** `zfsbackup/stopflag.py`
- **Basis:** `stopflag.py:30-40` (never recreate a shared block: heap blocks are recycled), `:108-129` (the
  pickling dict).

**Behaviour:**
- `class ReloadEpoch` wraps `multiprocessing.Value('i', 0, lock=False)`, with `get() -> int` and
  `set(value: int)`.
- `'i'` keeps every store a single aligned 32-bit write on all supported platforms, so reads cannot tear.
- Picklable through the `__getstate__`/`__setstate__` dict, exactly like `StopFlag`.
- Created once in `BackupDaemon.__init__` and never recreated. It is never touched from a signal handler.

**Tests** (`test_zfsbackup_stopflag.py`):
- a child started under fork, spawn and forkserver sees a value the parent sets after the child started;
- `ForkingPickler` shares the memory rather than copying it; plain `pickle` raises;
- no lock object exists.

### Item 18.2 — Worker exit protocol, `_wait` reason, orphan identity

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (multiprocessing) · **Gate:** G2
- **Files:** `zfsbackup/workers.py`
- **Basis:** `_SupervisorWatch` (`:157-266`), `BaseWorker.run` (`:304-334`), the destination loop (`:403-418`);
  Checkpoint B carry-overs 1 and 4.

**Behaviour:**
1. `class ExitReason(enum.Enum): STOP, SUPERVISOR_GONE, RELOAD` and `WORKER_EXIT_RELOAD = 3`.
2. **Constructors.**
   - `SnapshotWorker`, `PruningWorker`, `RemoteBackupWorker` and `ApiWorker` take
     `(config, stop_event, verbose=False, *, reload_epoch=None, born_epoch=0, workers_lock=None)`.
   - `BaseWorker` takes `(worker_name, config, stop_event, verbose=False, *, …same)`.
   - The defaults reproduce today's behaviour, so existing call sites and tests are unchanged.
3. **`_exit_reason() -> Optional[ExitReason]`**, in priority order:
   - STOP — the flag is set;
   - SUPERVISOR_GONE — `_supervisor_gone()`;
   - RELOAD — `reload_epoch is not None and reload_epoch.get() != born_epoch`.

   `_should_stop()` is kept as `_exit_reason() is not None`.
4. **`_wait(timeout) -> Optional[ExitReason]`.** Same slicing as today; it returns the reason that ended the
   wait, or `None` on timeout. A zero, negative or NaN timeout returns the current reason.
5. **Orphan identity.**
   - `_watch_init` also records `self.supervisor_start = procident.start_token(os.getpid())`.
   - `_supervisor_gone` keeps rules 1–2. Rule 3 becomes `procident.identity(supervisor_pid, supervisor_start)`:
     - `'gone'` → gone ("no such process");
     - `'mismatch'` → gone ("pid N now belongs to another process");
     - `'unknown'` → fall back to `_pid_exists`.
6. **`BaseWorker.run`.**
   - After `_arm_supervisor_watch`, hold `daemonlock.hold_workers_lock(workers_lock)` when it is set (item 18.5).
   - Loop: compute `reason = _exit_reason()` at the top and before each dataset; when a cycle completes,
     `reason = _wait(interval)`.
   - On exit:
     - STOP → log "stopped", return (exit 0);
     - SUPERVISOR_GONE → return (already logged once at ERROR);
     - RELOAD → log INFO "`<name>` finished its current work; exiting to switch configuration", then
       `sys.exit(WORKER_EXIT_RELOAD)`.
7. **`RemoteBackupWorker._process_dataset`.** Before every destination after the first, return if
   `_exit_reason()` is not `None`. No stream is open at that point, so it is a safe point.

**Edge cases:**
- A reload never interrupts `_process_dataset`.
- Stop beats reload.
- A failure in `_before_loop` is still a crash (exit 1).

**Tests** (`test_zfsbackup_workers.py`; update `test_zfsbackup_daemon_closeout.py::TestSupervisorWatch`):
- **Reasons:**
  - the full priority table of `_exit_reason`;
  - `_wait` returns each reason, returns `None` on timeout, and notices an epoch bump within one `STOP_POLL`.
- **Real worker process** under each available start method, whose `_process_dataset` blocks on an `Event`:
  - bump the epoch → it is not terminated, finishes that dataset, starts no other, exits 3;
  - the same with the stop flag → exit 0.
- **Remote and legacy paths:**
  - remote worker with two destinations, epoch bumped during the first → the second never starts, exit 3;
  - `reload_epoch=None` never yields RELOAD;
  - the old positional constructors still work.
- **Orphan identity:**
  - `identity` → `'mismatch'` under the forkserver flag counts as gone, logged once at ERROR;
  - `'unknown'` falls back to `_pid_exists`.

### Item 18.3 — `ApiWorker`: stream gate, drain, `server.shutdown()`

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (API) · **Gate:** G2
- **Files:** `zfsbackup/api.py`, `zfsbackup/workers.py`
- **Basis:** `api.py:108-212` — the API worker hosts the server-side `zfs receive -s`, so cycling it mid-receive
  tears an inbound stream. Also `ApiWorker.run` (`workers.py:470-506`) and Checkpoint B carry-over 3.

**`api.py`:**
- **`class StreamGate`** (a `threading.Condition`):
  - `begin() -> bool` — `False` while draining, otherwise count + 1;
  - `end()`;
  - `start_draining()`;
  - `draining` and `active` properties;
  - `wait_idle(timeout: Optional[float], abort: Callable[[], bool]) -> bool` — slices of ≤0.5 s; `True` when the
    count is 0.
- `create_app` stores one at `app.extensions['zfsbackup.stream_gate']`.
- **`backup_negotiate`**, before any ZFS call: while draining, return
  `503 {'error': 'server is reloading; retry later', 'retry': True}`.
- **`backup_stream`**, right after the enabled check: if `not gate.begin()`, send that same JSON error and
  return. Otherwise wrap the rest in `try/finally: gate.end()`.

**`ApiWorker.run`, after the bind:**
1. Loop `reason = self._wait(STOP_POLL)`. If the server thread dies → exit 1, as today.
2. `gate.start_draining()`. Read-only endpoints keep serving throughout.
3. On RELOAD with active streams: log INFO "waiting for N in-flight receive(s) to finish before reloading",
   then `gate.wait_idle(None, abort=lambda: self._exit_reason() in (STOP, SUPERVISOR_GONE))`. If aborted,
   reason becomes the new `_exit_reason()`.
4. On STOP or SUPERVISOR_GONE with active streams: `gate.wait_idle(API_STOP_DRAIN)` with
   `API_STOP_DRAIN = 20`, inside `_shutdown_workers`' shared 30 s budget. On timeout, WARNING "abandoning N
   in-flight receive(s); the sender resumes them (zfs receive -s)".
5. `if thread.is_alive(): server.shutdown()`, then `server.server_close()`, then `thread.join(timeout=5)`.
6. RELOAD → `sys.exit(WORKER_EXIT_RELOAD)`; otherwise return.

**Edge cases:**
- The replacement API worker is spawned only after this process has exited, so the port is free.
- A bind failure still exits 1 without `shutdown()`.

**Tests** (`test_zfsbackup_api.py`, `test_zfsbackup_workers.py`):
- gate accounting under threads;
- `negotiate` returns 503 while draining;
- `stream` refuses while draining and never starts `zfs receive` (mocked);
- an in-flight stream (mocked, blocking receive) keeps the worker alive after an epoch bump, and it exits 3
  once released;
- a stop during a RELOAD drain switches to the bounded drain and exits 0;
- a stop with an in-flight stream waits at most `API_STOP_DRAIN`, exits 0 and logs the abandon;
- `shutdown()` and `server_close()` run on every exit path except a bind failure (fake server via the
  `_bind_api_server` seam);
- a dead server thread still exits 1.

### Item 18.4 — Websocket timeouts (D7)

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (remote/API security) · **Gate:** G2
  (zfs-code-reviewer)
- **Files:** `zfsbackup/remote.py`, `zfsbackup/api.py`
- **Basis:** the client has no timeouts (`remote.py:185-202`); the server's `ws.receive()` has none either
  (`api.py:~187`). A hung peer would otherwise turn D5's "deferred indefinitely" into "never".

**Behaviour:**
- **Constants**, defined in `remote.py` and imported by `api.py` (`remote` must not import `api`):
  - `WS_CONNECT_TIMEOUT = 30`;
  - `WS_IDLE_TIMEOUT = 600` — no progress on one socket operation;
  - `WS_FINAL_ACK_TIMEOUT = 3600` — the client waiting for the server's status after `done`, while `zfs receive`
    may still be committing.
- **Client** (`_transfer`):
  - `ws.connect(ws_url, timeout=WS_CONNECT_TIMEOUT)`, then `ws.settimeout(WS_IDLE_TIMEOUT)`;
  - `ws.settimeout(WS_FINAL_ACK_TIMEOUT)` before the final `ws.recv()`;
  - a timeout lands in the existing `except Exception`: "Transfer failed", returns `False`, no anchor update.
  - Leaving `with stream:` closes the read end first, so `zfs send` dies with EPIPE and its non-zero close is
    swallowed. Verify this cannot hang.
- **Server:** `data = ws.receive(timeout=WS_IDLE_TIMEOUT)`. `None` → raise
  `TimeoutError(f"no data from the client for {WS_IDLE_TIMEOUT}s")`. The existing handler closes the writer
  (`-s` keeps resume state) and reports the error; `gate.end()` still runs.

**Tests** (`test_zfsbackup_remote.py`, `test_zfsbackup_api.py`):
- `connect` gets the timeout; `settimeout` values are applied in order;
- a fake ws whose `send_binary` times out → `False`, anchor unchanged, stream closed;
- server `receive` returns `None` → writer closed, error sent, gate released;
- the normal path is unchanged.

### Item 18.5 — Workers lock and predecessor survivors

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` (multiprocessing/startup) · **Gate:** G2
- **Files:** `zfsbackup/daemon.py` (`_run_startup`), `zfsbackup/workers.py`
- **Basis:** Checkpoint B carry-over 2. Under spawn/forkserver, orphans hold no presence lock
  (`daemonlock.py` docstring), so a restarted daemon can overlap with survivors finishing their last dataset.

**Behaviour:**
1. **Workers lock first.** Right after `daemonlock.acquire` succeeds (not under `--test-config`), call
   `ensure_workers_lock(resolved)`.
   - While `workers_present(resolved)`: sleep 1 s.
   - Log a WARNING at the start and every 60 s: "workers of a previous daemon still hold
     `<db>.workers.lock`; waiting for them to finish — they exit at their next safe point and a transfer in
     progress is never interrupted. Find them with `fuser <path>`".
   - Only then read the config.
2. **Startup signals during the wait.** SIGINT/SIGTERM keep their default dispositions here (no workers exist, so
   the process stays killable). SIGHUP goes to `_StartupHup`.
3. **Dead supervisor still holding the lock.** If `acquire` raises `DaemonAlreadyRunning` and
   `procident.identity(e.pid, e.start)` is `'gone'` or `'mismatch'`, fork orphans of a dead supervisor hold the
   lock. Retry `acquire` every 1 s, with the same 60 s WARNING plus `stale_supervisor_message`, instead of
   exiting. `'match'` or `'unknown'` → exit 1 as today.
4. **Worker side.** Every worker holds `LOCK_SH` for life via 18.2 step 6. The supervisor passes
   `workers_lock=path` to every worker. It never keeps a `workers.lock` fd open across a fork.

**Edge cases:**
- A missing or unopenable `workers.lock` produces a WARNING and the worker continues: the lock is a detection
  aid, not a gate.
- `zfs` children never inherit the fd (`O_CLOEXEC`).

**Tests** (`test_zfsbackup_daemon_closeout.py`, `_workers.py`):
- startup waits while a helper process holds `LOCK_SH`, and proceeds within about 1 s after it exits;
- the WARNING cadence;
- SIGTERM during the wait exits with no worker started;
- a verified-dead supervisor makes the new daemon wait;
- a live matching supervisor is still refused with exit 1;
- workers hold `LOCK_SH` under each start method (checked with `workers_present`);
- the fd's `FD_CLOEXEC` bit is set.

### Item 17.2 — Supervisor reload state machine

- **Owner:** `zfsbackup-developer` (run at opus) · **Tag:** `needs-approval` (signals/supervisor) · **Gate:** G3
- **Files:** `zfsbackup/daemon.py`, `zfsbackup/runtime_config.py`, `zfsbackup/backup_manager.py` (only the
  `should_stop` parameter)
- **Basis:**
  - `daemon.py`: `_sighup_handler` `:145-157`, `_new_worker` `:229-241`, `_spawn` `:249-288`,
    `_check_workers` `:351-423`, `_supervisor_sleep` `:478-521`, `run` `:523-589`, `_report_hup` `:591-595`,
    `_run_startup` `:772-933`;
  - `backup_manager.py:326-329`; D1, D2, D5, D6, D9, D10.

**State:**
- `@dataclass(frozen=True) class Handoff: config; epoch: int; key: Tuple[int, int]`.
- `BackupDaemon.__init__(config, resolved_config, verbose=False, *, key=(0, 0), presence_lock=None,
  dry_run_override=False, workers_lock=None)` sets:
  - `self._handoff = Handoff(config, 0, key)`; `self.config` becomes a read-only property returning
    `self._handoff.config`;
  - `self._reload_epoch = ReloadEpoch()`;
  - `_worker_epoch: Dict[str, int]`, `_rejected = None`, `_applied_epoch = 0`;
  - `_next_poll_at = monotonic + RELOAD_POLL_SECONDS`;
  - `_reload_started_at`, `_defer_logged_at: Dict[str, float]`, `_poll_problem = None`.

**Hand-over (config and epoch together):**
- `_new_worker` reads `h = self._handoff` once and passes `h.config` and `born_epoch=h.epoch` together, plus
  `reload_epoch` and `workers_lock`.
- `_spawn` records `_worker_epoch[name] = h.epoch`.

**Signals:**
- `_sighup_handler` stays a single store.
- `_supervisor_sleep` also returns early when `self._hup_requested` is set (one attribute read per slice), so
  SIGHUP latency is at most `SUPERVISOR_SLEEP_SLICE`.
- `_report_hup` and the stopgap message are deleted.

**`run()`:**
1. After the startup sync, call `self._reload_tick(startup=True)` once before `_start_workers`. A CLI SIGHUP
   carried by `_StartupHup`, or a write that landed during the startup sync, is applied before any worker exists.
2. Loop: `_supervisor_sleep` → `_reload_tick()` → `_check_workers()`. The stop flag is re-checked between steps.

**`_reload_tick()`:**
1. `forced = self._hup_requested`; if set, clear it **before** reading. A SIGHUP that lands later forces another
   tick, and the write behind a SIGHUP was committed before that signal was sent, so the read below sees it.
2. Unless forced or the poll is due, return. Set `_next_poll_at = now + RELOAD_POLL_SECONDS`.
3. Unforced path:
   - `read_store_state(resolved)` in `try/finally: dispose_all()`.
   - Any `Exception` → `_note_poll_problem(e)`: WARNING "cannot check the configuration database for changes:
     …; still running generation G" when the message changes, INFO when it clears, DEBUG otherwise. Then return.
   - Return if the key equals `_handoff.key` or `_rejected`.
4. Then `_try_reload(trigger)`, where `trigger` is `"SIGHUP"` or `"poll"`.

**`_try_reload(trigger)`:**
1. **Read and validate.**
   - `snap = load_runtime_config(resolved)`, which becomes `read_config(resolved, warn_never_prune=False)` with
     the schema check inside the snapshot.
   - `key = (snap.store_id, snap.generation)`; `warnings = validate_config(snap.config)`.
   - `dispose_all()` in a `finally`.
2. **On any `Exception`:**
   - log ERROR "configuration generation `<G' | unknown>` rejected (trigger: …):
     `<config_error_message(resolved, e)>`; still running generation G" — only when `(key, message)` differs
     from the last rejection logged;
   - if the key is known, set `_rejected = key` and `presence_lock.set_state(rejected=key)`;
   - return.
3. If the key equals the running key: when forced, INFO "SIGHUP: configuration unchanged (generation G)".
   Return.
4. Re-apply `--dry-run` (`config.dry_run = True` when `dry_run_override`). Log the warnings.
5. INFO "configuration generation G' accepted (was G; trigger: SIGHUP|poll)", then
   `presence_lock.set_state(target=key)`.
6. `m = DatasetManager(config)`; `m.dataset_report()`; `m.verify_datasets()`;
   `m.sync_all_config_properties(should_stop=self._stop_event.is_set)`. If the stop flag is set, return:
   shutdown proceeds and `target` stays recorded.
7. `self._handoff = Handoff(config, epoch + 1, key)` **first**, then `self._reload_epoch.set(epoch + 1)`. Clear
   `_rejected`. Set `_reload_started_at`. Clear `_defer_logged_at`.
8. INFO "Reloading: generation G -> G' (+added, -removed, ~changed datasets); workers switch over at their next
   safe point".
9. Spawn newly active workers that are neither live nor in backoff (guarded by the stop flag). Drop the backoff
   bookkeeping of workers that are no longer active.
10. `_maybe_complete()`.

**`_check_workers()` changes:**
- **First pass:** a dead worker whose `exitcode == WORKER_EXIT_RELOAD` and whose `_worker_epoch[name]` is below
  the current epoch is a **turnover**. It gets no failure count and no backoff. Everything else takes the A-5
  crash path unchanged; that includes code 3 with a current epoch.
- **Second pass** (skipped if anything crash-looped; stop-guarded): for each turnover, drop its bookkeeping, then
  - if the name is still active → `_spawn(name, verb='Reloaded')` immediately, logging "Reloaded `<name>` worker
    (pid=N) on generation G'";
  - otherwise → INFO "`<name>` worker retired: not needed by generation G'".
- **Deferral logging:** for each live worker whose epoch is below the current one, once 60 s
  (`RELOAD_DEFER_NOTICE`) have passed since the reload, and then every 900 s (`RELOAD_DEFER_REPEAT`): WARNING
  "`<name>` worker (pid=N) is still finishing work begun under generation G; it switches to G' at its next safe
  point (in-flight transfers are never interrupted)".
- **`_maybe_complete()`:** when every live worker is on the current epoch (dead-in-backoff workers will spawn on
  it):
  - set `_applied_epoch`;
  - `presence_lock.set_state(applied=key, target=None)`;
  - INFO "generation G' applied by all workers".
- **D9:** the fatal crash-loop message adds "running configuration generation G' (accepted at HH:MM:SS)" once the
  epoch is above 0.

**Startup (`_run_startup`):**
- After the read, run `validate_config`. Log its warnings. A `ValueError` → ERROR "configuration generation N is
  invalid: …" (no traceback), `_release_refused`, exit 1. The same applies under `--test-config`.
- Replace `set_generation` with `set_state(applied=key)`.
- Construct `BackupDaemon(config, resolved, verbose, key=key, presence_lock=lock,
  dry_run_override=args.dry_run, workers_lock=path)`.

**`sync_all_config_properties(self, should_stop=None)`:** break between datasets when `should_stop()` is true.

**Docstrings to correct:** `daemon.py:95-106`, `:145-157`; `workers.py:1-20` and `_init_child_process` (its
SIGHUP wording); `runtime_config.py` ("once" → "at startup and on every reload"). Add to the
`SUPERVISOR_SLEEP_SLICE` note: a reload tick can hold the loop for up to `busy_timeout` (5 s) plus the sync of a
changed config.

**Edge cases:**
- Stop beats everything, and nothing is ever forked into a shutdown.
- No session is open at any fork.
- Two reloads during a drain coalesce: draining workers respawn straight onto the newest config.
- A worker crash during a drain takes the crash path on the new config.
- Nothing is respawned while `_crash_looped` is non-empty.

**Tests** (`test_zfsbackup_daemon.py`, `_daemon_resilience.py`, `_daemon_closeout.py`):
- **Signals and triggers:**
  - the SIGHUP handler is one store; `_supervisor_sleep` returns within one slice of a HUP;
  - forced read when unchanged → "unchanged", no worker touched;
  - the poll triggers on a generation change and on a **store-id change at the same generation**;
  - the poll is not re-triggered by a rejected key, but SIGHUP retries it.
- **Rejection and failures:**
  - an invalid candidate (`validate_config` `ValueError`, `SchemaOutOfDate`, missing file) leaves worker pids
    unchanged and records `rejected=`;
  - the log is deduplicated;
  - a poll failure is logged once per state change, and the daemon keeps running.
- **`--dry-run`:** it survives a reload.
- **Workers:**
  - hand-over: every spawn gets config and `born_epoch` from the same `Handoff`;
  - growing and shrinking the worker set, including a backoff-pending worker that is no longer active;
  - turnover bypasses backoff and the failure count; code 3 with a current epoch counts as a crash.
- **Draining and completion:**
  - deferral logging cadence (fake `_monotonic`);
  - completion writes `applied` and clears `target`;
  - coalescing two reloads;
  - a stop during the sync, and during a drain.
- **Startup:**
  - a pending SIGHUP from `_StartupHup` is applied before the first spawn (no double cycle);
  - startup `validate_config` refusal exits 1;
  - the D9 message names the generation;
  - `dispose_all` is called after every poll and every reload read.
- **Old tests to replace:** the stopgap tests in `test_zfsbackup_daemon_resilience.py`, and the `_report_hup`
  and `_wait`-bool tests in `test_zfsbackup_daemon_closeout.py`.

### Item 19.1 — CLI: signal after every committed write, acknowledgement, notice

- **Owner:** `zfsbackup-cli-developer` · **Tag:** `needs-approval` (config-writing CLI path) · **Gate:** G4;
  lands after G3
- **Files:** `cli/write.py`, `cli/main.py`, `cli/editor.py`, `cli/rename.py`
- **Basis:** `write.py:88-179` (`commit_config`, `_daemon_notice`, `notify_daemon`); call sites at
  `main.py:530,918`, `editor.py:694`, `rename.py:495,498,699`; D3, D4.

**Behaviour:**
1. `commit_config` passes `expected_store_id=snapshot.store_id`. A store-id `ConfigConflict` maps to exit 75,
   like any other conflict.
2. **`notify_daemon(resolved, generation, store_id=None)`.**
   - Call sites pass `result.store_id`; `import` passes `ImportResult.store_id`.
   - It calls `daemonlock.request_reload(resolved, skip_if_applied=(store_id, generation))` **exactly once**.
   - It runs only after a commit. Every refusal path already raises before it, and `rename --dry-run` returns
     before it.
3. **Acknowledgement wait.** Only when the outcome is `signalled` and the wait is above 0: poll
   `daemonlock.probe()` every 0.1 s until the wait elapses or the daemon answers.
   - New global option `zfsbackup-config --reload-wait SECONDS` (float ≥ 0; default 3; 0 = don't wait).
   - `notify_daemon` reads it from the current Click context, defaulting to 3.0 when there is none.
4. **Wording.** Every line starts `config updated (generation N)`. stdout unless marked; the exit code never
   changes.

| Outcome | Rest of the line |
|---|---|
| `no-daemon` | `; no running daemon detected for <db>` (unchanged) |
| `already-applied` | `; running daemon pid P is already on generation N` |
| `signalled` + `applied == (S,N)` | `; reload requested from daemon pid P: it is now on generation N` |
| `signalled` + `target == (S,N)` | `; reload requested from daemon pid P: it accepted generation N and is switching its workers over (in-flight transfers finish first)` |
| `signalled` + `rejected == (S,N)` | `; reload requested from daemon pid P` + stderr `warning: daemon pid P rejected generation N and keeps running generation M; see its log` |
| `signalled` + a newer key | `; reload requested from daemon pid P: it is already on (or applying) newer generation K` |
| `signalled`, no answer | `; reload requested from daemon pid P (SIGHUP)`; when the wait was above 0, add `; not yet acknowledged; it applies the change at its next check (within 10s)` |
| `not-permitted` | `; daemon pid P runs as another user and was not signalled; it picks up the change within 10s` |
| `unverified` | `; could not verify that pid P is the daemon (<detail>), so it was not signalled; it picks up the change within 10s` |
| `supervisor-gone` | `; ` + `stale_supervisor_message(...)` (existing) |
| `legacy` | `; running daemon pid P is on generation M and predates live reload; restart it to apply this` |
| `error` | stderr warning (existing wording) |

   "10s" is `daemonlock.RELOAD_POLL_SECONDS`.
5. **Other wording.** `rename destination`'s "Restart the daemon now so it uses the new name" (`rename.py:~689`)
   becomes "The running daemon reloads the configuration (see below); until then and after, it protects every …".
   The `write.py` module docstring states the new contract.

**Tests** (`test_zfsbackup_cli.py`, `_cli_rename.py`, `_cli_edit.py`, `_cli_dotpath.py`):
- every wording row, with `request_reload` and `probe` patched; exit 0 for every row;
- `request_reload` is called exactly once after each successful `set`, `unset`, `edit` (whole, dataset,
  destination), `rename dataset`/`rename destination` and `import`;
- it is never called for `rename --dry-run`, a validation refusal, `ConfigConflict` (generation or store id),
  `ConfigBusy`, `edit` abort or no-change, `migrate`, `export`, `list` or `get`;
- the patched function sees the DB already at the new generation (post-commit);
- the ack loop stops as soon as there is an answer; `--reload-wait 0` never probes after signalling;
- the "until restarted" assertions are updated (`test_zfsbackup_cli.py:~4471,4482`,
  `test_zfsbackup_daemon.py:~1383`); only the `legacy` row keeps "restart";
- **Linux-only integration test:** a real helper process holding a genuine v2 lock records SIGHUP and writes
  `target=`, and the CLI `set` reports "accepted". The same helper with a forged pid (a decoy `sleep`) leaves the
  decoy alive.

### Item 19.2 — `zfsbackup-config status [--wait SECONDS] [--json]`

- **Owner:** `zfsbackup-cli-developer` · **Tag:** `low-risk` (read-only) · **Gate:** G4
- **Files:** `cli/main.py`

**Behaviour:**
- Reads `read_store_state` (then disposes) and `daemonlock.probe()`.
- Prints the stored store id and generation; the daemon pid; applied, target and rejected; whether the daemon is
  reload-capable.
- Verdict: `in sync` / `applying generation N` / `daemon rejected generation N` /
  `daemon on M, stored N (it will pick it up)` / `no running daemon`.
- Exit 0; 78 when the DB is unusable (existing mapping).
- `--wait S`: exit 0 once applied equals stored (or when no daemon is running, with a message); 1 if the stored
  key was rejected; 75 on timeout.
- Never signals.

**Tests:** every verdict; the JSON shape; every `--wait` outcome; no signal is ever sent (`os.kill` and
`pidfd_send_signal` patched to fail the test).

### Item 20 — Tests for Phase 3

- **Owner:** `pytest-test-author` · **Tag:** `low-risk`
- **Basis:** items 17.0–19.2. Per-item lists are above, each written alongside its item.
- **Cross-cutting real-process tests** (patterned on `test_zfsbackup_stop_signals.py` / `_daemon_closeout.py`: a
  real daemon subprocess, SQLite fixture, stub `zfs`; must pass the orphan safety net):
  1. CLI `set` → the daemon logs "accepted … trigger: SIGHUP" within 2 s; worker pids change; the supervisor pid
     does not; the pid file reads `applied=` the new key.
  2. SIGHUP with an unchanged DB → "unchanged", and no worker pid changes.
  3. A DB write with no signal → reload with "trigger: poll" within 15 s (`-m slow`).
  4. A stored config `validate_config` rejects (raw `sqlite3` UPDATE that bumps the generation) → rejected, worker
     pids unchanged, `rejected=` in the pid file.
  5. A `--dry-run` daemon stays dry-run after a reload (API `/config` shows `dry_run: true`).
  6. A CLI write inside the startup window → the daemon comes up on the new generation, with exactly one spawn per
     worker.
- **Verify:** `pytest tests/test_zfsbackup_*.py`, and `pytest -m slow -k reload`.

### Item 21 — Documentation

- **Owners:** each surface's developer; the main session for `CLAUDE.md` · **Tag:** `low-risk`
- **`zfsbackup/README.md`:**
  - the reload contract: two triggers, at most 10 s for unsignalled writes, staggered turnover, indefinite
    deferral of in-flight transfers, rejection behaviour, `status`;
  - CLI signalling: Linux-only verification, and the EPERM → poll fallback for group operators;
  - the files next to the DB, now including `.workers.lock`;
  - systemd guidance: `ExecReload=/bin/kill -HUP $MAINPID`, **`KillMode=mixed`** (the default `control-group`
    sends SIGTERM straight to `SIG_DFL` workers and tears a `zfs send`), and how to size `TimeoutStopSec`.
- **`CLAUDE.md`:** update lines 126-127 and the daemonlock paragraph.
- **`.claude/agents/zfsbackup-developer.md`:** list `procident.py`, `daemonlock.py`, `stopflag.py` and
  `runtime_config.py` as owned files.
- `config.example.yaml` and `config.test.yaml` stay untouched.

### Item 22 — Shell scenario coverage

- **Owner:** `real-zfs-scenario-dev` · **Tag:** `low-risk` · **Starts:** after G4

**22.1 — `scenarios/test_config_permissions.sh`, new group (f) "live reload"** (on the `zfsb-perm` VM; the daemon
runs as `zfsbackup` under its transient unit):
- **Setup:**
  - record `multiprocessing.get_start_method()` on the VM;
  - `reset_default_state` also removes `$DB.workers.lock`;
  - the `worker_pid` regex accepts `Reloaded`.
- **(f1) root writes:** root `set check_interval 2m` →
  - stdout `reload requested from daemon pid $P` plus `accepted|now on`;
  - log line "accepted … trigger: SIGHUP" within 5 s;
  - the snapshot, pruning and api worker pids all change; the supervisor pid does not;
  - the pid file shows `applied=…:$g1` and line 2 == `$g1`.
- **(f2) group operator (EPERM):** alice `set …` →
  - stdout `runs as another user and was not signalled; it picks up the change within 10s`, exit 0;
  - log "accepted … trigger: poll" within 15 s; worker pids change.
- **(f3) same uid:** user `zfsbackup` writes → signalled.
- **(f4) forgery:** stop the daemon. A holder (alice) takes `LOCK_EX` on `$PIDFILE` and writes the pid **and
  correct start token** of a decoy `sleep 600` that root owns and that does not hold the lock. Root `set …` →
  - the notice says the supervisor is gone or the daemon was not signalled;
  - the decoy is still alive (`kill -0`).
- **(f5) rejected config:** a raw `sqlite3` UPDATE makes the stored config invalid and bumps the generation, then
  `systemctl kill -s HUP --kill-whom=main $UNIT` →
  - log "rejected";
  - worker pids unchanged; `rejected=` in the pid file; the unit is still active.
- **(f6) startup race:** a CLI write during daemon startup (the measured 8/8 case) → the daemon stays up and
  applies the new generation.
- **(b) update:** replace "keeps the startup generation after a re-import" and "will not apply this until
  restarted" with the new outcomes.

**22.2 — `scenarios/test_two_vm_backup.sh`:**
- Reload the **client** during a large send (e.g. a 2 GiB random file): the transfer completes ("Backup
  complete"), the remote worker's pid changes only after it, and the deferral WARNING appears if the send takes
  ≥60 s.
- Reload the **server** during an inbound receive: the receive completes, and the API worker's pid changes only
  after it.
- `kill -9` the client supervisor during a send and start a new daemon at once: the new daemon logs the
  "waiting" line until the survivor finishes, then starts.

- **Verify:** manual runs; record the pass counts in the Progress table.

### Item 23 — Final review

- **Owner:** `zfs-code-reviewer` + `concurrency-reviewer` · **Tag:** `low-risk`
- **Scope:** the full Phase 3 diff. In particular:
  - no `terminate()`/`kill()` anywhere on the reload path;
  - the CLI can never signal anything but a verified lock holder (forgery, recycled pid, fork orphans);
  - no fork with a session open; dispose after every read;
  - turnover vs. A-5 accounting; `--dry-run` persistence;
  - API drain and `server.shutdown()`;
  - websocket timeouts cannot hang `zfs send`/`receive`;
  - the pid-file v2 stays compatible with two-line readers;
  - the items carried over from the original item 23.

---

## Risk map

| Risk | Where | Mitigation |
|---|---|---|
| **Pre-fork engine inherited by workers** — SQLite fd sharing corrupts the DB, not just errors | `daemon.py:62-68`, `store/db.py` | pid-keyed engine cache (item 5); supervisor disposes engine before `_start_workers()`; explicit fork test (item 9) |
| **A duration library silently mis-parses `1M` as 1 minute** — retention collapses, snapshots pruned immediately | `config/model.py:12-45` | **Verified failure in `pytimeparse` and `humanfriendly`.** No library added; hand-rolled parse/render reusing the existing tested parser (item 2) |
| **Duplicate retention `keep_for` silently drops a tier** — pre-existing. `{"1h":"7d","1d":"1w"}` collapses to one tier, and the tie-break systematically keeps the **coarser** interval, pruning harder than configured | `backup_manager.py:216-219`, tie-break via `config/model.py:155` | Reject on write (CLI, import, `UNIQUE(dataset_id, keep_for_seconds)`); `min` + `WARNING` on read so a collision can never be silent (item 2b) |
| **`Duration` wrapper breaks reflected comparison** — `age >= dsi.frequency` silently misbehaves | `backup_manager.py:205-206` | Subclass `timedelta` rather than wrap (item 2); explicit reflected-comparison test (item 9) |
| **`Duration` breaks `or`-truthiness** — a zero duration stops being falsy, changing inherited-frequency logic | `workers.py:146-150,161` | Inherited `__bool__` via subclassing; explicit zero-duration test (item 9) |
| **Literal rewritten across pickle/deepcopy** — a missing `__reduce__` no longer raises (native-kwargs support) and no longer yields `None` (synthesis); it **silently substitutes the canonical literal**, so `30d`→`1M` and `12h6d`→`6d12h` while canonical values look correct | `daemon.py:34`, test comparisons | `__reduce__` on the subclass (item 2); item-9 assertion **must use a non-canonical literal** — testing `1M` passes even with the bug present |
| **Wire-format breakage** — `to_property()` is sent to remote servers | `config/model.py:88-107`, `remote.py:108` | Base64-JSON format frozen; byte-equality assertion (item 9) |
| **Duration round-trip corrupts retention** — `1M` → `30d` rewrites a user's policy | `config/model.py:12-45` vs. item 12 | Literal preservation (item 2), 9-row table (item 9), real-file diff (item 22) |
| **Untyped YAML scalars reaching `Duration` directly** — `check_interval: 300` (meaning seconds) dispatches to native kwargs and silently becomes **300 days**, where the old parser raised. Found in the Phase 0 review | `config/model.py` load sites | Type guard at the config-loading boundary rejecting non-`str` with the offending key named; `Duration`'s constructor left unchanged. Regression test required |
| **Dot-path ambiguity with dotted ZFS names** — silent wrong-target writes | Item 11 | Schema-directed greedy matching + bracket syntax + hard error on ambiguity |
| **Half-updated DB on bad edit** | Items 11, 12, 14 | Validate fully in memory *before* opening a write transaction; single `session.begin()`; DB-hash-unchanged assertions |
| **Rename orphans server-side backups and forces a full send** | `api.py:34-38`, `workers.py:162`, `daemon.py:100` | `rename` warns loudly, requires `--force` when remotes are configured, offers opt-in `--clear-stale-props` (item 14) |
| **Whole-config edit turns a rename into delete+create**, orphaning ZFS properties | Item 12 | Rename-shaped diffs detected and refused, pointing at `rename`; `--allow-recreate` to override |
| **Worker torn down mid send/receive** — partial dataset on the server, inconsistent anchor | `workers.py:171`, `remote.py` | Cooperative cycling only; checks at cycle/dataset boundaries; **no hard kill**; reload defers indefinitely for an in-flight transfer (item 18) |
| **Bad config → infinite respawn loop** (pre-existing) | `workers.py:66-67` + `daemon.py:69-80` | Supervisor validates before cycling; per-worker backoff and escalation (item 17) |
| **Reload exit misread as a crash** — spurious error logs, restart delay, backoff pollution | `daemon.py:74` | Distinguished exit code (item 18) |
| **SIGHUP latency** — handler flag not seen for up to 5s | `daemon.py:109` (PEP 475 sleep retry) | Event-based wake replaces `time.sleep` (item 17) |
| **Shutdown-latency regression** — restructuring the poll loop for reload could add up to 5s to `SIGINT`/`SIGTERM` if only SIGHUP sets the wake event | `daemon.py:39-45,109` (item 17) | Existing stop handlers must set the wake event too; asserted in item 20 |
| **Reload is not atomic across workers** | Item 18 | Safe because workers share no in-memory state; stated explicitly so it is not assumed otherwise |
| **WAL requires write access even to read** — CLI users get an opaque error | Item 6 | Dir `0770` (not `0750`) + `0660 zfsbackup:zfsbackup`, group membership documented; CLI translates `EACCES` into a clear message |
| **Non-root daemon needs ZFS delegation the plan cannot grant** — `zfs allow` for snapshot/destroy/send/receive/userprop, and on Linux `mount` stays privileged even when delegated | Item 6 | Treated as its own workstream with a documented recipe and a real-pool scenario; DB permissions shipped independently so the layout is right either way |
| **DB-canonical migration breaks every YAML consumer** — conftest, real-ZFS tests, daemon/worker patch targets, and the two-VM scenario all obtain config from YAML today | Item 8 | Enumerated migration table in item 8; items 8 and 9 land in one cycle; daemon detects a `.yaml` argument and prints the exact `import` command |
| **Stale pidfile → SIGHUP to an unrelated recycled pid** | Item 19 | Verify the pid is actually a zfsbackup daemon before signalling |
| **Test patch targets break** — `from_file` patched by name in 3 places | `test_zfsbackup_workers.py:228`, `test_zfsbackup_daemon.py:211,231` | Items 8 and 9 land in the same cycle |
| **Undeclared deps** — repeat of the `requests` problem | `pyproject.toml:9-16` | Item 1 declares everything before any code uses it |
| **WAL sidecars + pidfile** in packaging/cleanup | `store/db.py`, item 19 | Documented (item 21), cleaned (item 22) |

---

## Sequencing and parallelism

- **Phase 0:** item 1 → item 2. Item 2 gates everything; nothing in Phase 1 should start before it lands.
- **Item 2b is independent** of the whole DB effort — a standalone pre-existing bug fix that can land,
  review, and ship on its own before anything else in Phase 1 starts.
- **Item 3b must precede item 3**, despite its number. 3b adds per-destination retention to the *dataclass*
  model, and item 3's schema mirrors that model — designing the tables before the dataclasses exist would
  mean revising them immediately. Order: **2b → 3b → 3 → 4 → 3c → 7 → 5 → 6 → 8**. (3b's `remote.py`
  half — passing the destination to `to_property` — is independent of the schema and can land with
  either.)
- Both 2b and 3b are pure `zfsbackup` changes with **no DB dependency at all**, so both can be implemented,
  reviewed, and merged before any SQLAlchemy code is written. Recommended: ship them as their own cycle.
- **Phase 1 serial chain:** 3 → 4 → 5 → 6 → 8. Item 8 needs 4 and 6.
- **Item 7 (Alembic) is no longer free-floating.** It baselines the schema, so **every intended schema
  change must land before it** — concretely, item 3c. Item 7 may run parallel to 5 and 6, but not
  before 3c. Ordering: **3c → 7**, and 7 before any later item that would otherwise need a migration.
- **Item 6 reaches into Phase 2.** Its files are `store/db.py`, `daemon.py` *and* `cli/main.py`, but
  `zfsbackup/cli/` does not exist until item 10. Split it: **6a** (resolution logic + daemon) stays in
  the Phase 1 chain; **6b** (CLI wiring) folds into item 10 and inherits 6a's settled resolution order.
- **Phase 1 coordination point:** items 8 and 9 must be reviewed together (patch-target breakage).
- **Phase 2 parallel:** items 11, 12, 13 are independent once 10 lands. Item 14 needs 12's editor
  machinery. Item 15 is independent of 11-14 and can start immediately after 10.
- **Phase 3 serial:** 17 → 18 → 19. Item 18 is the risky one and should be reviewed on its own before 19.
- **Cross-phase parallel:** item 21 (docs) can be drafted any time after Checkpoint A. Item 22 (scenarios)
  can be written after Phase 2 and extended after Phase 3. Test items 9, 16, 20 each follow their phase.
- **Checkpoints:** A after item 9, B after item 16, and a final review at item 23. Phase 3 may be deferred
  or dropped entirely without affecting Phases 1-2 — that independence is the reason for the split.

---

## Recommended deferrals

*(Live reload has been removed from this list and promoted to Phase 3.)*

| Deferred | Reason |
|---|---|
| **DB-backed write endpoints in the HTTP API** | `api.py` has no auth or TLS (`docs/production_readiness_report.md`). Exposing config *mutation* over an unauthenticated API would be a security regression. Blocked on remote-security work |
| **Encrypting destination credentials in the DB** | `Destination` is URL-only today (`config/model.py:58-61`); no secrets exist to protect yet. Revisit when auth lands |
| **Replacing the base64 user-property format** with a DB-derived one | Would break client/server compatibility (`remote.py:108`). Out of scope |
| **Multi-host / shared config DB** | SQLite over NFS is unsafe. If needed, that is a Postgres conversation — and the item-4 mapper layer is exactly what makes that migration possible later |
| **Running the daemon as a non-root user** | `zfs allow` delegation is not set up in the target environment, and on Linux `mount` stays privileged even when delegated — so `destroy` on a mounted dataset and `receive` both need a mitigation (`receive -u`, `canmount=noauto`, or `CAP_SYS_ADMIN`). Phase 1 ships the `zfsbackup:zfsbackup` file layout so nothing needs redoing; the daemon stays root until this workstream lands (item 6) |
| **Auto-cleanup of orphaned server-side datasets after a rename** | Requires deleting data on a remote machine based on a local config edit. Too dangerous to automate; item 14 prints the path instead |
| ~~**Fixing the odd leading-`m` branch** at `config/model.py:17-23`~~ — **no longer deferred** | Compound-literal support (item 2) rewrites the parser, and this branch is **dead code**: it computes a `unit` that `int(duration_str[:-1])` then always discards by raising. Verified — `m5`, `M5`, `mi5`, `mi` all raise. It is deleted as a side effect; both covering tests assert only `ValueError` and keep passing |
| **Making `to_property()` carry duration literals** | Would change the wire format and break compatibility with already-deployed servers. Remote-derived `Duration`s render canonically instead (item 2) |

---

## For your approval

**Proposal in brief.** Three phases. Phase 1 adds a SQLAlchemy-backed SQLite config store alongside (not
replacing) YAML, keeping `BackupConfig`/`DatasetConfig` as plain dataclasses with a mapper layer, because
those classes are also a remote wire format and outlive any session. Phase 2 adds a `click`-based
`zfsbackup-config` CLI with dot-path get/set and `$EDITOR` round-trip editing, with validate-before-transact
rollback. Phase 3 adds SIGHUP-triggered live reload via cooperative worker cycling. 23 items, 3 checkpoints.

**All seven decisions are incorporated**, one of which changed a recommendation on the evidence:

1. **Full `Duration` type — adopted, but with no new dependency.** `pytimeparse`, `humanfriendly`,
   `durationpy`, and `isodate` were tested against the actual format rather than assumed. None
   round-trips it, and **`pytimeparse` and `humanfriendly` silently parse `1M` as one minute** — in a
   retention rule that means "keep for 1 minute", which would prune everything. `durationpy` rejects `1M`
   and canonicalizes `1d` to `24h`. `isodate` handles only ISO-8601 and rejects every native literal. The
   honest recommendation is a ~15-line hand-rolled render plus the existing tested parser. `Duration` must
   **subclass** `timedelta` rather than wrap it, because `backup_manager.py:205` performs
   `bare_timedelta >= frequency` and `workers.py:146-150` relies on `or`-truthiness — a wrapper breaks
   both in ways that are easy to miss.
2. **No identity keys in any edit buffer**, `rename` as an explicit command, and names in the
   **whole-config** buffer treated as set-membership identity, with rename-shaped diffs actively detected
   and refused rather than silently applied as delete+create. The stale-ZFS-property problem belongs to
   `rename`, including the consequence that matters most: after a rename the server-side path changes and
   the anchor is gone, so **the next remote backup is a full send**. That is `--force`-gated.
3. **Live reload is in scope as Phase 3**, with SIGHUP + flag-setting handler, supervisor-side validation
   before any disruption, graceful worker *restart* rather than in-place re-read, per-worker control events
   (a real IPC change to `workers.py:32`), a distinguished reload exit code, and a hard rule that a worker
   mid send/receive is **never** killed — reload defers indefinitely for an in-flight transfer. Structured
   as its own phase after the CLI, so it can be dropped without affecting Phases 1-2.
4. **`/var/lib/zfsbackup/config.db`** with resolution order explicit `-c` > `ZFSBACKUP_CONFIG` > default DB
   > error. Daemon never auto-creates; CLI creates only on `import`. Daemon runs as a dedicated non-root
   `zfsbackup` user with operators in the `zfsbackup` group; dir `0770`, DB `0660` — **`0770`, not `0750`,
   because a WAL reader needs write access to the file *and its directory***. Non-root operation also
   requires `zfs allow` delegation, and on Linux `mount` remains privileged even when delegated — item 6
   flags this as its own workstream rather than something file modes can deliver.
8. **The DB is canonical; YAML is import + transient edit buffer only.** The daemon no longer reads YAML.
   `from_file` is retained as the single YAML parser (used by `import` and edit-buffer validation) but
   stops being a config source. This is the largest churn in Phase 1 — conftest, the real-ZFS tests, the
   daemon/worker patch targets, and the two-VM scenario all move to import-then-point-at-DB — and it makes
   item 2's literal preservation load-bearing, since every edit buffer is regenerated from the DB.
9. **Per-destination retention** (item 3b), with override-not-merge semantics at both ends. The transport
   needs no wire-schema change: config is POSTed per destination at negotiate time, so the receiving
   machine never has to identify itself.
5. **`Duration` is a drop-in `timedelta`.** Its constructor accepts a literal (`Duration("1M")`), an
   existing `timedelta`, **or** the full native `timedelta` kwargs (`Duration(days=30)`). It additionally
   parses **compound literals** (`6d12h`, `1y2M3d`) — a genuine parser rewrite, not a relocation, which
   also deletes the dead leading-`m` branch — and **synthesizes a best-effort literal** by greedy
   largest-unit decomposition whenever none was supplied. `parse_time_duration` is recommended to survive
   as a thin public shim so the 22 existing test assertions keep passing. **`Duration.__str__` is not
   overridden** — daemon log output at `backup_manager.py:97,101,208` stays
   byte-identical to today. The literal is exposed as a read-only **`.literal`** property, and a read-only
   **`.timedelta`** property returns a pure `timedelta` with no literal attached for code that must not
   see the subclass. Item 9 asserts `__str__` is unchanged so a later refactor cannot silently alter logs.
6. **Reload uses an event-based immediate wake**, replacing `time.sleep(SUPERVISOR_POLL)` at
   `daemon.py:109`, so SIGHUP takes effect at once rather than after up to 5s. This restructures the
   supervisor's main loop, which is why item 17 is `needs-approval` — and the existing `SIGINT`/`SIGTERM`
   handlers must set the wake event too, or the change silently adds 5s of shutdown latency it does not
   have today. Item 20 tests for exactly that regression.
7. **CLI auto-reloads by default**, with `--no-reload` to suppress, on the grounds that config written but
   silently unapplied is the more dangerous default.

**New dependencies** (item 1): `sqlalchemy ^2.0`, `alembic ^1.13`, `click ^8.1` (currently transitive via
Flask), plus separately `requests ^2.31` (pre-existing gap, independently rejectable). **No duration
library** — none earned its place.

**Three findings worth attention regardless of approval:** `CLAUDE.md` documents a nonexistent
`zfsbackup/test_basic.py`; `click` is already relied on transitively, the same pattern that produced the
`requests` problem; and **a malformed config today causes a silent infinite worker-respawn loop**
(`workers.py:66-67` + `daemon.py:69-80`, no backoff, no escalation) — pre-existing, unrelated to these
features, and fixed in item 17 because live reload would make it easier to hit.

**No open design questions remain.** The three points previously left open — `Duration.__str__`, SIGHUP
latency, and CLI auto-reload — are settled above as decisions 5, 6, and 7.

**What still requires explicit sign-off before code is written**, because the plan is a proposal and the
`needs-approval` tags mean what they say:

- **Checkpoint A** (after item 9) — the schema, the fork-safety approach, and the `Duration` semantics.
- **Checkpoint B** (after item 16) — before Phase 3 touches multiprocessing at all.
- **`requests ^2.31`** in item 1 — a pre-existing gap being fixed opportunistically; reject it if you would
  rather keep that change out of this diff.
- **Phase 3 in principle** — it is deliberately severable. Phases 1-2 deliver the two requested features in
  full; Phase 3 only removes the daemon restart.

**Phases 0–1 are implemented and Checkpoint A is held (see Progress at the top). The fix-up cycle and the
revised Phase 2 are approved; Phase 3 remains a proposal.**

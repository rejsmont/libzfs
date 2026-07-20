# zfsbackup: SQLite Config Store + Config CLI + Live Reload

**Status:** proposal, revised after user decisions on the four open questions
**Scope:** `zfsbackup/` only. No `libzfseasy/` changes.
**Author:** zfsbackup-implementation-planner

> **All design questions are settled; no open decisions remain.** Items 2 (duration handling) and 5 (fork
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
  (`zfsbackup/config.py:88-132`) base64-JSON-encode it into the ZFS user property
  `org.zfsbackup:config`, and it is transmitted to remote servers (`zfsbackup/remote.py:108`).
  Instances are reconstructed from remote data with no DB backing and **no duration literals**.
- **Retention is a YAML mapping, not a list** (`zfsbackup/config.py:148-155`): keys are unique age
  strings, sorted by age via `retention_rules.sort(key=lambda r: r.age)` at `config.py:155`.
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
mapper.** New package `zfsbackup/store/` with `models.py` (declarative ORM rows), `db.py`
(engine/session), `mapper.py` (ORM ⇄ dataclass conversion). `BackupConfig`/`DatasetConfig` stay plain
dataclasses and remain the only types any consumer sees.

Justification, tied to source:

1. `DatasetConfig.from_property()` (`config.py:110`) constructs instances from *remote server data* with
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
- **Files:** `zfsbackup/config.py`
- **Basis:** `zfsbackup/config.py:12-45`. `parse_time_duration("1M")` and `parse_time_duration("30d")`
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
  `config.py:12-45` moves into `Duration` as a private static method **and is rewritten** to support
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
strictly single-term — `value = int(duration_str[:-1])` at `config.py:26` takes everything but the last
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
- **The vestigial leading-`m` branch disappears, and that is safe.** Lines `config.py:17-23` compute a
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
  > (`config.py:110-132`) feeds `total_seconds()` **floats** off the wire into `Duration(seconds=...)`.
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
- **Upside for the remote path:** `from_property()` (`config.py:110-132`) rebuilds durations from
  `timedelta(seconds=...)` with no literal available. Synthesis means remote-derived configs now render as
  `1M` rather than failing or emitting a raw timedelta — a real improvement over the `literal=None` design.
- **Deliberate and acceptable:** a *reconstructed* 30-day value synthesizes `"1M"`, not `"30d"`. Values
  parsed from a literal keep their original text, so a user's `30d` is never rewritten — only values that
  never had a literal in the first place get the greedy form.

**⚠ `parse_time_duration` cannot simply disappear — it is public API with live consumers.** It is
imported and asserted against **22 times across two test files** (`tests/test_zfsbackup_config.py:8` and
`tests/test_zfsbackup_basic.py:9`), including the deliberate leading-`m` edge-case coverage at
`tests/test_zfsbackup_config.py:60-76`. **Recommended: keep `parse_time_duration` as a thin public
module-level shim** in `config.py` that delegates to `Duration._parse` and returns a plain `timedelta`:

```
def parse_time_duration(duration_str: str) -> timedelta:
    """Backwards-compatible shim; parsing logic now lives in Duration."""
    return Duration(duration_str).timedelta
```

This satisfies the intent — the logic genuinely lives in `Duration`, and nothing in `zfsbackup/`
calls the shim — while leaving all 22 existing assertions passing unmodified. The alternative (delete it
outright) forces `pytest-test-author` to rewrite both files in the same cycle, and would lose the
leading-`m` edge-case tests unless they are consciously ported. **Confirm which you want.**

**Benefit of the constructor change worth noting:** the six internal parse sites at `config.py:145`,
`config.py:152-153`, `config.py:162`, `config.py:212`, and `config.py:215` become `Duration(...)`
construction directly, so every duration loaded by `from_file` carries its literal with no separate
wiring step. The `field(default_factory=...)` defaults at `config.py:83`, `config.py:180`, and
`config.py:181` become `Duration("1h")`, `Duration("5m")`, `Duration("1h")` respectively.

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
| Sorting | `config.py:155` — `retention_rules.sort(key=lambda r: r.age)` | `__lt__` between `Duration`s |
| `.total_seconds()` for JSON | `config.py:93,96,102` (`to_property`) | Inherited; **output must be byte-identical** |
| Dataclass default | `config.py:83` — `field(default_factory=lambda: timedelta(hours=1))` | Becomes `Duration('1h')` |
| `.total_seconds()` | `workers.py:100` (`check_interval`), `workers.py:122` (`prune_interval`) | Inherited |
| **Truthiness in `or`** | `workers.py:146-150` — `(r.frequency or ds.frequency).total_seconds()` | `__bool__`. `timedelta(0)` is falsy; `Duration` wrapping zero **must remain falsy** to preserve today's behaviour. Inherited correctly by subclassing. |
| **Truthiness in `or`** | `workers.py:161` — `(remote_cfg.frequency or dsi.config.frequency)` | Same |
| Comparison against float | `workers.py:166` — `anchor.age.total_seconds() < freq` | Operates on floats |
| Equality in tests | 8 test files constructing `BackupConfig(...)` | `__eq__` inherited; `Duration('1M') == timedelta(days=30)` is `True`, which is correct — literal is metadata, not identity |
| Hashability | implied by dataclass/dict use | Inherited |

- **Edge cases to preserve:** the leading-`m` branch at `config.py:17-23` is dead code and is deleted by
  the compound-parser rewrite; its two tests assert only `ValueError` and must keep passing.
  **Negative durations (`-5d`) must keep working** — they parse today. `s` (seconds) is accepted at `config.py:30` but undocumented in
  `config.example.yaml:84-90`; keep accepting it. `from_property()` (`config.py:110-132`) reconstructs
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
- **Files:** `zfsbackup/backup_manager.py`, `zfsbackup/config.py`
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

**The tie-break is systematically the worst available.** `config.py:155` sorts rules by `age` **ascending**,
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
- **Files:** new `zfsbackup/store/__init__.py`, `zfsbackup/store/models.py`
- **Basis:** the dataclass set at `config.py:48-189`.
- **Schema:**
  - `global_settings` — single row (`CHECK (id = 1)`), mirroring `BackupConfig` scalars
    (`config.py:179-189`): `snapshot_prefix`, `check_interval_seconds` + `check_interval_literal`,
    `prune_interval_*`, `api_host`, `api_port`, `dry_run`, `client_id_file`, plus `generation INTEGER`
    (items 15 and 17).
  - `datasets` — `id`, `name UNIQUE NOT NULL`, `recursive`, `frequency_seconds`, `frequency_literal`,
    `enabled` (`config.py:79-86`).
  - `retention_rules` — `id`, `dataset_id FK ON DELETE CASCADE`,
    **`destination_name FK → destinations(name) ON DELETE CASCADE, NULL`**, `age_seconds`, `age_literal`,
    `keep_for_seconds`, `keep_for_literal`, with
    **`UNIQUE(dataset_id, destination_name, age_seconds)`** and
    **`UNIQUE(dataset_id, destination_name, keep_for_seconds)`** — see item 2b and item 3b.

    `destination_name IS NULL` means the **dataset-level** rule set: what governs local pruning, and what
    a destination with no rules of its own inherits. A non-NULL `destination_name` scopes the rule to that
    destination only.

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
  - `destinations` — `name PRIMARY KEY`, `url NOT NULL` (`config.py:220-225`).
  - `dataset_remotes` — `id`, `dataset_id FK CASCADE`, `destination_name FK`, `frequency_seconds NULL`,
    `frequency_literal NULL`. NULL frequency = inherit, per `config.py:68`.
  - `remote_server` — single row, `target_dataset`, `enabled` (`config.py:71-75`).
  - A key/value EAV table is rejected: it defeats typing and makes dot-path coercion guesswork.
- **Edge cases:** `PRAGMA foreign_keys=ON` must be set per-connection (SQLite defaults it off) — item 5.
  No ordering column needed for retention; rules are sorted by age at read time (`config.py:155`).
- **Verification:** `pytest tests/test_zfsbackup_store.py -k schema`

### Item 3b — Per-destination retention rules

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(retention + remote/wire behaviour)*
- **Files:** `zfsbackup/config.py`, `zfsbackup/remote.py`, `zfsbackup/backup_manager.py`
- **Basis:** user decision. Today `RemoteDatasetConfig` (`config.py:66-69`) carries only `destination` and
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
  `frequency` precedent at `config.py:68` (`None = inherit from DatasetConfig`).

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

### Item 4 — Mapper layer (ORM ⇄ dataclass)

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(defines the config load contract)*
- **Files:** new `zfsbackup/store/mapper.py`
- **Basis:** `BackupConfig.from_file` (`config.py:191-252`) is the only load path today; the DB path must
  produce an equivalent object graph or every downstream consumer shifts behaviour.
- **⚠ Do not reuse `from_dict` for the DB→dataclass direction** *(found in the Phase 0 review)*. Phase 0
  added a YAML-boundary type guard that rejects any non-`str` duration, so `from_dict` now raises on a
  value that is already a `Duration` or `timedelta` (`got Duration Duration('1h', 1:00:00)`). That is
  correct for a documented YAML boundary, but it means the mapper — and item 8's importer, if it takes the
  same path — must construct dataclasses directly (`from_property`-style, with typed values) rather than
  routing DB rows back through `from_dict`.
- **Changes:** `load_config(session) -> BackupConfig` and `save_config(session, config)`. `load_config`
  replicates `from_file`'s defaulting exactly: `snapshot_prefix` default `"autosnap"` (`config.py:208`),
  retention defaulting to `{'1d': '30d'}` when empty (`config.py:149-150`), retention sorted by age
  (`config.py:155`), `prune_interval` falling back to `check_interval` (`config.py:214`),
  `client_id_file` default (`config.py:238-239`), and the "No datasets configured" error
  (`config.py:204-205`).
- **Target behaviour:** `load_config(session_from(yaml_imported(P))) == BackupConfig.from_file(P)` for
  every YAML in the repo — an equivalence property test in item 9.
- **Verification:** `pytest tests/test_zfsbackup_store.py -k mapper`

### Item 5 — Engine/session management, WAL, fork safety *(settled gating decision)*

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(multiprocessing)*
- **Files:** new `zfsbackup/store/db.py`
- **Basis:** `daemon.py:62-68` spawns 3-4 `multiprocessing.Process` workers; `workers.py:62` and
  `workers.py:196` each open config inside the child. On Linux the default start method is `fork`, so
  **any engine or connection created in the supervisor before `_start_workers()` would be inherited by
  every child** — shared SQLite file descriptors corrupt the database, they do not merely error.
  Highest-risk item in the plan.
- **Concurrency story:**
  - **Engine created lazily, per-process, never before fork.** Module-level engine cache keyed by
    `(url, os.getpid())`; if the recorded pid differs from `os.getpid()`, discard and rebuild. This makes
    an accidental pre-fork engine harmless rather than catastrophic.
  - **Supervisor holds no open engine while spawning.** `daemon.py:153` loads config in `main()`; that
    session is opened, read, closed, and the engine disposed before `BackupDaemon.run()` (`daemon.py:90`)
    starts workers.
  - **WAL + pragmas** via a `connect` event listener: `journal_mode=WAL`, `synchronous=NORMAL`,
    `busy_timeout=5000`, `foreign_keys=ON`.
  - **Workers are read-only.** Each reads config once at startup (`workers.py:62`) and never writes it.
    Under WAL, readers never block and are never blocked by the CLI writer, so contention is near-zero
    by construction. Enforce it: workers open the DB through a read-only session factory.
  - **The CLI is the only writer**, a separate short-lived process. Single-writer SQLite is adequate.
- **Edge cases:** `sqlite:///:memory:` must still work for tests (`StaticPool` +
  `check_same_thread=False`); WAL requires a real file, so the pragma listener skips WAL for in-memory
  URLs. WAL creates `-wal`/`-shm` sidecars next to the DB — relevant to packaging, permissions (item 6),
  and `scenarios/` cleanup.
- **Verification:** `pytest tests/test_zfsbackup_store.py -k "fork or concurren"`

### Item 6 — DB path resolution, creation policy, permissions

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(daemon config-loading contract)*
- **Files:** `zfsbackup/store/db.py`, `zfsbackup/daemon.py`, `zfsbackup/cli/main.py`
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
    `config.py:204-205` already errors on no datasets; preserve that posture.
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

- **Owner:** `zfsbackup-developer` · **Tag:** `low-risk`
- **Files:** new `zfsbackup/store/migrations/` (env.py, versions/), `alembic.ini`
- **Basis:** judgment call per the migrations recommendation above; new infrastructure, no existing code.
- **Changes:** Alembic env reads the DB URL from the app rather than `alembic.ini`, so it works against
  whatever `-c` resolves to. One initial revision creating the item-3 schema. An `ensure_schema(url)`
  helper that stamps a fresh DB at head and **refuses to open a DB whose revision is newer than the
  code** (forward-compatibility guard).
- **Verification:** `pytest tests/test_zfsbackup_store.py -k migration`

### Item 8 — DB as canonical config source; YAML demoted to import + edit buffer

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(daemon config-loading contract + IPC payload)*
- **Files:** `zfsbackup/config.py`, `zfsbackup/daemon.py`, `zfsbackup/workers.py`, new
  `zfsbackup/store/importer.py`
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

### Item 9 — Tests for Phase 1

- **Owner:** `pytest-test-author` · **Tag:** `low-risk`
- **Files:** new `tests/test_zfsbackup_store.py`; edits to `tests/conftest.py`,
  `tests/test_zfsbackup_daemon.py`, `tests/test_zfsbackup_workers.py`
- **Basis:** items 2-8, plus the patch targets item 8 invalidates.
- **Coverage required:**
  - Schema constraints: duplicate dataset name rejected; duplicate `(dataset, age)` retention rejected;
    cascade delete removes retention rules and dataset_remotes.
  - Mapper equivalence: for `config.example.yaml` and `config.test.yaml`,
    `load_config(imported) == from_file(yaml)` field-by-field including retention ordering.
  - **`Duration` consumer-surface tests**, one per row of the item-2 table — especially the reflected
    comparison at `backup_manager.py:205` (`bare_timedelta >= Duration`), the `or`-truthiness at
    `workers.py:146-150` with a zero `Duration`, pickle/deepcopy literal preservation, and byte-identical
    `to_property()` output.
  - **`__str__` is unchanged** — `str(Duration("1M")) == "30 days, 0:00:00"`, asserted directly, so
    a later refactor cannot silently alter daemon log output.
  - **`.literal` and `.timedelta` accessors** — `.literal` returns the source string (`None` for
    values constructed from a `timedelta`); `.timedelta` returns a value equal to the `Duration` but of
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

  - The `from_property` path (`config.py:110-132`) rebuilds from raw seconds and now yields a synthesized
    literal rather than `None` — assert it renders (e.g. 2592000s → `"1M"`) instead of failing.
  - Fork safety: parent opens DB, forks, child reads independently; assert no corruption.
  - Concurrent write-while-read under WAL.
  - Path resolution order and the create-vs-error policy (item 6).
  - New `sqlite_config_path` conftest fixture mirroring `config_yaml_path` (`tests/conftest.py:206-217`).
- **Verification:** `pytest tests/test_zfsbackup_*.py` fully green.

> ### ⛳ CHECKPOINT A — review before Phase 2
> Phase 1 is independently useful and independently revertible. Confirm the schema, fork-safety approach,
> `Duration` semantics (especially the `__str__` log-output change), and the path/permissions policy
> before any CLI work. Phase 2's dot-path semantics depend directly on the item-3 schema.

---

## Phase 2 — Config CLI

### Item 10 — CLI skeleton + entry point

- **Owner:** `zfsbackup-developer` · **Tag:** `low-risk`
- **Files:** new `zfsbackup/cli/__init__.py`, `zfsbackup/cli/__main__.py`, `zfsbackup/cli/main.py`;
  `pyproject.toml`
- **Basis:** `daemon.py:118-145` establishes flag conventions (`-c/--config`, `-v/--verbose`,
  `-d/--dry-run`); the CLI should match.
- **Command surface** (revised per decision 2 — `rename` added; no identity keys in edit buffers):

  ```
  zfsbackup-config [-c <db>] get  <dot.path>
  zfsbackup-config [-c <db>] set  <dot.path> <value>
  zfsbackup-config [-c <db>] edit                        # whole config in $EDITOR
  zfsbackup-config [-c <db>] list datasets
  zfsbackup-config [-c <db>] list remotes
  zfsbackup-config [-c <db>] edit dataset <name>         # buffer has NO name field
  zfsbackup-config [-c <db>] edit remote  <name>         # buffer has NO name field
  zfsbackup-config [-c <db>] rename dataset <old> <new>
  zfsbackup-config [-c <db>] rename remote  <old> <new>
  zfsbackup-config [-c <db>] import <yaml> [--replace]
  zfsbackup-config [-c <db>] export [-o <file>]          # DB -> YAML, no editor
  zfsbackup-config [-c <db>] reload                      # Phase 3, item 19
  ```

- **Entry point: both.** `[tool.poetry.scripts]` console script
  `zfsbackup-config = "zfsbackup.cli.main:main"` for ergonomics, plus `python -m zfsbackup.cli` via
  `__main__.py` for parity with `python -m zfsbackup.daemon` (`daemon.py:182`) and so it works without an
  installed entry point.
- **Packaging note:** `pyproject.toml:1-7` declares the package as `libzfseasy` with no explicit
  `packages` list. A console script pointing into `zfsbackup` may require an explicit `packages = [...]`
  entry — the developer must verify `poetry install` actually exposes the script.
- **Verification:** `pytest tests/test_zfsbackup_cli.py -k skeleton`

### Item 11 — Dot-path get/set with type coercion

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(config write path)*
- **Files:** new `zfsbackup/cli/dotpath.py`
- **Basis:** the requested `datasets.tank/data.frequency=1h` form, resolved against the item-3 schema.
- **⚠ Critical edge case — dot-splitting is unsafe.** ZFS dataset names legally contain dots
  (`tank/vm.01`, `tank/data.old`). Naive `path.split('.')` on `datasets.tank/vm.01.frequency` yields
  `['datasets','tank/vm','01','frequency']` — wrong, and it fails *silently* into "no such dataset".
  Two-part fix:
  1. **Schema-directed resolution.** Walk left-to-right against the known schema. At `datasets.`, match
     greedily against dataset names actually in the DB, longest-first, then parse the remainder. Same for
     `destinations.`.
  2. **Bracket escape hatch:** `datasets[tank/vm.01].frequency`. Always accepted, and it is what `export`
     and error messages emit.
  On genuine ambiguity (a dataset `a.b` and another `a` with field `b`), error and demand the bracket
  form. **Never guess.**
- **Type coercion by schema field type:** `str` verbatim; `int` via `int()` with range check for
  `api_port`; `bool` accepting `true/false/yes/no/on/off/1/0` case-insensitively; duration via
  `Duration(...)` (item 2), **storing the literal as typed**; `Path` for `client_id_file`. Unknown path →
  non-zero exit listing valid siblings.
- **Addressable paths:** `snapshot_prefix`, `check_interval`, `prune_interval`, `api_host`, `api_port`,
  `dry_run`, `client_id_file`, `datasets.<n>.{frequency,recursive,enabled}`,
  `datasets.<n>.retention.<age>`, `datasets.<n>.remote.<dest>.frequency`, `destinations.<name>.url`,
  `remote_backup.{target_dataset,enabled}`. **`datasets.<n>.name` is not addressable** — renaming is
  `rename` only (item 14).
- **Target behaviour:** `set` is a single transaction; it re-validates the whole resulting config by
  round-tripping through `load_config` before committing, and rolls back on failure.
- **Verification:** `pytest tests/test_zfsbackup_cli.py -k dotpath` — including dotted-dataset-name cases.

### Item 12 — Whole-config `$EDITOR` round-trip

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(config round-trip format)*
- **Files:** new `zfsbackup/cli/editor.py`, `zfsbackup/cli/render.py`
- **Basis:** requested feature; render target is the format at `config.example.yaml:9-80` so generated
  YAML is drop-in compatible with `-c file.yaml`.
- **Names in the whole-config buffer — decided (per decision 2 extension).** This buffer *is* keyed by
  name (`datasets:` is a list of mappings each with `name:`, `config.example.yaml:35`). Names here are
  **identity, and are treated as set-membership, not editable content**:
  - a name present that was not before → **create**
  - a name absent that was there → **delete**
  - a name changed in place → interpreted as **delete + create**, not rename

  The CLI **detects the delete+create-looks-like-a-rename case** (one deletion and one creation in the
  same apply, with otherwise-identical field values) and **refuses**, printing:
  *"this looks like a rename; use `zfsbackup-config rename dataset <old> <new>` instead, or pass
  `--allow-recreate` if you really mean delete-and-create."* Rationale: silently treating it as
  delete+create orphans ZFS user properties exactly as described in item 14, and the user is very
  unlikely to want that.
  Same rule for `destinations:`.
- **Flow:**
  1. `render_yaml(load_config(db))` → temp file, `0600`, in a private temp dir.
  2. Launch `$VISUAL` → `$EDITOR` → fall back to `vi`. Split with `shlex.split` so `EDITOR="code -w"`
     works. Run with **inherited stdio, not captured** — a captured-stdio editor hangs. This is the same
     class of bug as the recorded multipass `/dev/null` wedge.
  3. Editor exits non-zero → abort, DB untouched.
  4. Content byte-identical → "no changes", exit 0, no transaction.
  5. Parse and validate the edited YAML **fully in memory** before opening any write transaction.
  6. On validation failure: print the error with line context, preserve the user's buffer at a stable
     path, offer re-edit (`Retry / Abort`). **The DB is never opened for write until validation passes.**
  7. On success: one transaction — wipe and reinsert — then commit.
- **Rollback guarantee:** validate-before-transaction means a bad edit cannot half-update; the write is a
  single `session.begin()` block, so even a mid-write failure (disk full, FK violation) rolls back whole.
- **Edge cases:** `0600` + private dir so config does not leak on a shared `/tmp`; editor killed by signal
  = abort; a concurrent `set` between render and apply is caught by the item-15 generation check.
- **Verification:** `pytest tests/test_zfsbackup_cli.py -k editor` — `$EDITOR` stubbed with a mutating
  script; assert no-op, abort, invalid-then-abort, rename-detection, and success paths.

### Item 13 — `list datasets` / `list remotes`

- **Owner:** `zfsbackup-developer` · **Tag:** `low-risk`
- **Files:** `zfsbackup/cli/main.py`
- **Basis:** requested feature; columns mirror the existing report at `backup_manager.py:88-100` (name,
  enabled, frequency, recursive, retention-rule count) so CLI and daemon log agree.
- **Changes:** human-readable table by default; `--json` for scripting; `--enabled-only` matching
  `BackupConfig.enabled_datasets` (`config.py:254-256`). Remotes list shows destination name, URL, and
  which datasets reference it.
- **Verification:** `pytest tests/test_zfsbackup_cli.py -k list`

### Item 14 — Per-dataset / per-remote edit *(revised: no identity key in buffer)* + `rename`

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(config round-trip + ZFS property side effects)*
- **Files:** `zfsbackup/cli/render.py`, `zfsbackup/cli/main.py`
- **Basis:** requested feature; user decision 2.
- **Per-dataset buffer contains no `name` field.** `edit dataset <name>` renders only
  `enabled`, `frequency`, `recursive`, `retention:`, `remote:` — the shape at
  `config.example.yaml:36-46` minus `name:`. The dataset being edited is shown as a YAML comment header
  (`# dataset: tank/data  — to rename, use: zfsbackup-config rename dataset ...`) so the user knows what
  they are editing, and the comment is ignored on parse. Identity comes from the command line, never the
  buffer. If a `name:` key is present on parse, **reject** with a pointer to `rename`.
- **Per-remote buffer likewise contains no name.** `edit remote <name>` renders only `url:`.
- **`rename dataset <old> <new>` / `rename remote <old> <new>`** — the only way to change an identity.
  Single transaction: rename the row; FK cascades keep retention rules and dataset_remotes attached.
- **The stale-ZFS-property problem belongs here.** Renaming a *config entry* does **not** rename the ZFS
  dataset. Consequences, all of which `rename` must handle explicitly:
  1. The old ZFS dataset retains a stale `org.zfsbackup:config` user property, written by
     `sync_all_config_properties()` (`daemon.py:100`) and read back by `DatasetConfig.from_property()`
     (`config.py:110`). It will not be cleaned up by anything.
  2. The old dataset retains stale `org.zfsbackup:anchor.<destination>` properties, read at
     `workers.py:162` via `manager.get_anchor()`. These are what keep the last-transferred snapshot
     exempt from pruning.
  3. **Most seriously:** the server-side destination path is derived from the client dataset name
     (`_server_dataset()` in `api.py:34-38` splits the client dataset name to build
     `<target>/<client_id>/<relative>`). After a rename, the new name maps to a **different server-side
     dataset**, and there is **no anchor** for it — so the next remote backup is a **full send, not an
     incremental**. On a large dataset that can mean hours of transfer and a duplicated copy on the
     server.
- **Therefore `rename` must:**
  - Always print a warning naming the stale properties left on the old dataset.
  - **Require `--force` when the dataset has any configured remote destinations** (`config.py:86`),
    because of consequence (3). The warning must state explicitly that the next backup will be a full
    send and that the old server-side copy will be orphaned.
  - Offer `--clear-stale-props` to remove `org.zfsbackup:config` and `org.zfsbackup:anchor.*` from the
    old dataset. **Not the default** — destroying anchors is itself destructive, and the old dataset may
    still be wanted.
  - Never touch the remote server. Cleaning up the orphaned server-side dataset is a manual, deliberate
    act; the CLI prints the path it would be at.
  - Refuse if `<new>` already exists in the config.
- **Destination deletion:** rejected while still referenced by a dataset's `remote:` list, with the
  referencing datasets listed. The item-3 FK enforces this regardless.
- **Verification:** `pytest tests/test_zfsbackup_cli.py -k "edit_dataset or edit_remote or rename"`

### Item 15 — Generation counter and concurrent-write detection

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(config write path)*
- **Files:** `zfsbackup/cli/main.py`, `zfsbackup/store/db.py`
- **Basis:** `workers.py:62` and `workers.py:196` load config once, before the loop at `workers.py:73`;
  nothing re-reads it. Retained from the original plan — the counter is still required for
  concurrent-CLI-write detection independently of Phase 3.
- **Changes:**
  1. `generation` counter on `global_settings`, incremented on **every** write.
  2. The CLI reads it before rendering and re-checks inside the write transaction; a mismatch means a
     concurrent write → abort with "config changed underneath you, re-run". This is what makes the
     read-edit-write window of item 12 safe.
  3. Until Phase 3 lands, after a successful write the CLI detects a running daemon and prints:
     *"config updated; running daemon (pid N) will not pick this up until restarted."* Phase 3 replaces
     this notice with an actual reload.
- **Writes are safe regardless:** WAL + single-writer means a CLI write never disturbs a reading worker.
- **Verification:** `pytest tests/test_zfsbackup_cli.py -k generation`

### Item 16 — Tests for Phase 2

- **Owner:** `pytest-test-author` · **Tag:** `low-risk`
- **Files:** new `tests/test_zfsbackup_cli.py`; `tests/conftest.py`
- **Basis:** items 10-15.
- **Coverage required:** `click.testing.CliRunner` for every subcommand; get/set for each addressable
  path and coerced type; **dotted dataset names** (`tank/vm.01`) in both greedy and bracket forms;
  ambiguous-name error; bad value leaves DB byte-identical (assert via file hash); `$EDITOR` stub covering
  no-op / abort / invalid-YAML / valid-change; **`name:` present in a per-dataset buffer is rejected**;
  **whole-config rename-detection refusal and `--allow-recreate` override**; `rename` warning text,
  `--force` gate when remotes are configured, and `--clear-stale-props`; generation-mismatch abort;
  `list --json` shape.
- **Verification:** `pytest tests/test_zfsbackup_*.py`

> ### ⛳ CHECKPOINT B — review before Phase 3
> The CLI is fully usable at this point, with the item-15 "restart required" notice as the honest
> stopgap. Phase 3 is where multiprocessing risk concentrates; confirm the reload design below before
> it starts.

---

## Phase 3 — Live config reload

Promoted from the deferral list at user request. **Structured as its own phase, after the CLI lands** —
recommended, because it modifies `daemon.py` signal handling, the `workers.py` IPC contract, and the
supervisor restart loop, none of which the CLI depends on. Keeping it separate means Phase 2 can ship and
be reviewed while this is still in design, and a problem here cannot destabilise the CLI.

### Item 17 — Reload trigger and supervisor state machine

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(signals, multiprocessing, supervisor)*
- **Files:** `zfsbackup/daemon.py`
- **Basis:** `daemon.py:39-45` (existing `SIGINT`/`SIGTERM` handlers), `daemon.py:47-60`
  (`_new_worker`/`_active_worker_names`), `daemon.py:69-80` (`_check_workers`), `daemon.py:108-111`
  (poll loop).
- **Recommended trigger: SIGHUP to the supervisor, with the generation counter as the correctness
  check.** Rationale:
  - The supervisor already installs signal handlers (`daemon.py:39-40`); SIGHUP-means-reload is the
    conventional daemon idiom and costs one more `signal.signal` call.
  - Polling the DB generation on every supervisor tick was considered and **rejected as the primary
    trigger**: it makes the daemon open the config DB every 5 seconds forever, which is exactly the
    steady-state read load item 5 was designed to avoid, and it introduces a DB dependency into the
    liveness loop.
  - The generation counter is still read *at reload time* to confirm something actually changed and to
    log the transition — cheap, and it makes a spurious SIGHUP a no-op.
- **Handler discipline:** the SIGHUP handler sets a flag only — `self._reload_requested = True` — exactly
  mirroring the existing `_stop_event.set()` pattern at `daemon.py:45`. No work in the handler.
- **Latency — event-based immediate wake** *(user decision)*. The poll loop currently uses
  `time.sleep(SUPERVISOR_POLL)` (`daemon.py:109`). Under PEP 475, `time.sleep` is *retried* after `EINTR`
  unless the handler raises, so a SIGHUP would **not** cut the sleep short — the flag would sit unseen for
  up to 5s. Replace the sleep with a wait on a dedicated wake event that the handler sets, so reload takes
  effect immediately:

  ```
  def _handle_hup(self, signum, frame):
      self._reload_requested = True
      self._wake.set()          # flag + wake only; no work in the handler

  while not self._stop_event.is_set():
      self._check_workers()
      if self._reload_requested:
          self._do_reload()
      self._wake.wait(SUPERVISOR_POLL)
      self._wake.clear()
  ```

  **This restructures the supervisor's main loop**, which also drives shutdown and worker restart — so it
  is not a free change, and it is why this item is `needs-approval`. Points to get right:
  - Use a `threading.Event` (the supervisor waits on it in its own process; workers are signalled via
    their own control events in item 18) — a `multiprocessing.Event` is unnecessary here and its
    semaphore-based `wait` has its own EINTR subtleties.
  - **Set the wake event from the existing `SIGINT`/`SIGTERM` handlers too** (`daemon.py:39-45`), not just
    SIGHUP. Otherwise shutdown gains up to 5s of latency it does not have today — a regression introduced
    by the very change meant to reduce latency.
  - Clear the event *after* waking and *before* acting, so a signal arriving mid-iteration is not lost.
  - `SUPERVISOR_POLL` remains as the wait timeout, so the dead-worker check still runs on its old cadence
    when nothing signals.
- **Validate before disrupting anything.** On reload the supervisor **first** loads and validates the new
  config in its own process. If loading fails, it logs the error, keeps the existing workers running
  untouched, and clears the flag. This is essential: workers `return` on bad config
  (`workers.py:66-67`), so cycling them into an invalid config would produce the respawn loop described
  next.
- **Fix the pre-existing infinite-respawn bug** (finding 3 above). `_check_workers` (`daemon.py:69-80`)
  restarts any dead worker unconditionally with a fixed 5s delay. Add: a restart counter per worker,
  exponential backoff, and escalation to a loud error (and supervisor exit) after N consecutive rapid
  failures. Without this, Phase 3 makes a bad-config respawn storm much easier to trigger.
- **Worker set may change across reload.** `_active_worker_names()` (`daemon.py:56-60`) derives the
  worker list from config — specifically whether any enabled dataset has remotes. A reload must therefore
  handle **adding** a `remote` worker that did not exist and **removing** one no longer needed, not just
  cycling the existing set.
- **Note:** `_check_workers` calls a blocking `time.sleep(WORKER_RESTART_DELAY)` at `daemon.py:76`, which
  stalls the entire supervisor loop including reload handling. Worth converting to a non-blocking
  deadline per worker as part of this item.
- **Verification:** `pytest tests/test_zfsbackup_daemon.py -k reload`

### Item 18 — Cooperative worker cycling (the mid-stream safety problem)

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(IPC contract change)*
- **Files:** `zfsbackup/workers.py`, `zfsbackup/daemon.py`
- **Basis:** `workers.py:32` (`stop_event` param), `workers.py:62`/`workers.py:196` (config loaded once),
  `workers.py:70-71` (`interval` and `_before_loop` computed once), `workers.py:73-82` (the loop),
  `workers.py:152-154` (`RemoteBackupManager` constructed in `_before_loop`), `daemon.py:37`/`daemon.py:54`
  (single shared `_stop_event`).
- **Mechanism: graceful worker restart, not in-place re-read.** Two designs were considered:
  - **(a) Restart the worker process.** Reuses the existing `_new_worker` path (`daemon.py:47-54`) and
    gets a fresh config, a fresh `DatasetManager`, a fresh `interval` (`workers.py:70`), and a fresh
    `_before_loop` (`workers.py:71`, which constructs `RemoteBackupManager`) for free.
  - **(b) Signal workers to re-read in place.** Requires restructuring the loop, rebuilding
    `DatasetManager` mid-flight, and re-running both `_get_interval` and `_before_loop` — i.e.
    reimplementing by hand everything a restart does for free.

  **Recommend (a).** The only thing (b) saves is process-spawn cost, which is irrelevant at config-change
  frequency.
- **IPC change — per-worker control events.** Today all workers share one `_stop_event`
  (`daemon.py:37,54`), so there is no way to cycle one worker without stopping all. Add a per-worker
  `control_event` alongside the shared `stop_event`:
  - `BaseWorker.__init__` (`workers.py:27-39`) gains a `control_event` parameter.
  - The loop's sleep at `workers.py:82` becomes `self.control_event.wait(timeout=interval)`.
  - On wake, and at the top of each cycle (where `stop_event` is already checked at `workers.py:73` and
    `workers.py:76`), the worker checks **both**: `stop_event` set → normal shutdown; `control_event` set
    → clean exit with a **distinguished exit code** meaning "reload me".
  - Global shutdown must set every `control_event` as well as `stop_event`, so workers wake promptly
    rather than sitting out a long `interval` — a latent shutdown-latency improvement in its own right.
  - `ApiWorker` (`workers.py:174-215`) blocks on `self.stop_event.wait()` at `workers.py:214` rather than
    running the `BaseWorker` loop; it needs the same both-events treatment. Its Flask thread is a daemon
    thread, so process exit tears it down cleanly.
- **⚠ Mid-operation safety — the hard requirement.** `RemoteBackupWorker` may be inside a `send`/`receive`
  stream (`workers.py:171` → `remote.py`). Tearing that down mid-stream risks a partial dataset on the
  receiving server and an inconsistent anchor property (`workers.py:162`). Therefore:
  - **Reload must never call `terminate()`.** It is strictly cooperative.
  - The control check happens at **cycle boundaries and between datasets** (`workers.py:75-81`), never
    inside `_process_dataset`. A worker mid-transfer finishes that dataset, then exits.
  - **No hard grace-period kill.** A legitimate full send can take hours (and item 14 shows renames can
    *cause* full sends). If a worker has not exited within a grace period, the supervisor logs
    *"remote-backup-worker still finishing a transfer; reload deferred"* and **keeps waiting**, retrying
    the check each poll. Reload is best-effort per worker: snapshot, pruning, and API workers turn over in
    seconds; the remote worker turns over when its current transfer completes. This is the correct
    trade-off — a stale config for one more cycle is vastly cheaper than a corrupt received stream.
  - Consequence to state plainly: **reload is not atomic across workers.** For a period, some workers run
    the new config and some the old. Given workers are independent per-dataset loops with no shared
    in-memory state, this is safe; it is called out so nobody assumes otherwise.
- **Distinguish reload-exit from crash.** `_check_workers` reads `worker.exitcode` (`daemon.py:74`) and
  logs an error. A reload exit must use a distinguished code so it is logged as an expected transition,
  does not incur `WORKER_RESTART_DELAY` (`daemon.py:76`), and does not count toward the item-17 crash
  backoff counter.
- **Verification:** `pytest tests/test_zfsbackup_workers.py -k "control or reload"` — including a test
  that a worker with a simulated long-running `_process_dataset` is **not** killed mid-operation.

### Item 19 — CLI reload trigger

- **Owner:** `zfsbackup-developer` · **Tag:** `needs-approval` *(daemon interaction)*
- **Files:** `zfsbackup/cli/main.py`, `zfsbackup/daemon.py`
- **Basis:** item 15's stopgap notice; `daemon.py:90` (`run()` is where a pidfile would be written).
- **Changes:**
  - The supervisor writes a **pidfile** (default `/var/lib/zfsbackup/daemon.pid`, alongside the DB from
    item 6) on startup and removes it on clean shutdown. Stale-pidfile detection: verify the pid exists
    *and* is actually a zfsbackup daemon before signalling — never SIGHUP an arbitrary recycled pid.
  - **Automatic reload by default, with `--no-reload` to suppress** *(user decision — settled)*. After any
    successful write, if a running daemon is detected, the CLI sends SIGHUP and reports
    *"signalled daemon (pid N) to reload"*. Rationale: the alternative default — config silently written
    but not applied — is the surprising and dangerous behaviour, and it is the exact failure mode item 15
    exists to paper over. `--no-reload` covers scripted batch edits where one reload at the end is wanted.
  - An explicit `zfsbackup-config reload` subcommand for that batch case and for manual use.
  - If no daemon is detected, say so plainly rather than silently doing nothing.
  - If the CLI lacks permission to signal the daemon (non-root user, root daemon), report that clearly —
    this is a realistic case given the item-6 group-based permissions, where a user can *write* the config
    but not *signal* the daemon. Suggest `systemctl reload zfsbackup` as the alternative.
- **Verification:** `pytest tests/test_zfsbackup_cli.py -k "reload or pidfile"`

### Item 20 — Tests for Phase 3

- **Owner:** `pytest-test-author` · **Tag:** `low-risk`
- **Files:** `tests/test_zfsbackup_daemon.py`, `tests/test_zfsbackup_workers.py`,
  `tests/test_zfsbackup_cli.py`
- **Basis:** items 17-19.
- **Coverage required:** SIGHUP sets the flag and wake event without doing work in the handler; **reload
  takes effect without waiting out a `SUPERVISOR_POLL` interval** (assert the loop wakes early, not after
  the timeout); **`SIGINT`/`SIGTERM` also wake the loop immediately** — a shutdown-latency regression test,
  since that is the failure mode the item-17 loop restructuring can introduce; a signal arriving
  mid-iteration is not lost to the event `clear()`; invalid new config leaves
  workers untouched and running; worker set grows/shrinks when `_active_worker_names()` changes across
  reload; reload exit code is distinguished from crash and skips the restart delay and backoff counter;
  crash backoff escalates and eventually exits rather than looping forever; **a worker
  mid-`_process_dataset` is not terminated and reload waits for it**; global shutdown sets all control
  events and workers exit promptly; pidfile lifecycle including stale-pid rejection; CLI auto-reload,
  `--no-reload`, and the no-daemon and permission-denied paths.
- **Verification:** `pytest tests/test_zfsbackup_*.py` fully green.

### Item 21 — Documentation

- **Owner:** `zfsbackup-developer` · **Tag:** `low-risk`
- **Files:** `zfsbackup/README.md`, `CLAUDE.md`, `zfsbackup/config.example.yaml`
- **Basis + stale-doc flag:** `CLAUDE.md` documents `zfsbackup/test_basic.py`, which **does not exist**;
  all zfsbackup tests live in `tests/test_zfsbackup_*.py`. Correct it.
- **Changes:** document `zfsbackup/store/`, the CLI, YAML-or-SQLite config sources, the item-6 path
  resolution order and the WAL write-permission trap, and the SIGHUP reload contract. Add a header comment
  to `config.example.yaml` noting it can be imported with `zfsbackup-config import`.
  **Do not otherwise alter `config.example.yaml` or `config.test.yaml`** — they are user-visible reference
  files and `tests/test_zfsbackup_real.py:415` reads one.
- **Verification:** review only.

### Item 22 — Shell scenario coverage

- **Owner:** `real-zfs-scenario-dev` · **Tag:** `low-risk`
- **Files:** new `scenarios/test_config_db.sh`
- **Basis:** `scenarios/test_pool.sh` and `scenarios/test_two_vm_backup.sh` establish the pattern;
  `config.example.yaml` targets the pool from `scenarios/create_test_pool.sh`.
- **Scope:** import `config.example.yaml` into a DB; run
  `python -m zfsbackup.daemon --test-config -c config.db` and assert it reports the same 5 datasets as the
  YAML path; exercise `set`/`get`/`export`; **diff exported YAML against the original to prove round-trip
  fidelity** — this is the end-to-end proof for item 2, and the one place `1M` staying `1M` is verified
  against a real file rather than a unit test. Then start the daemon, `set` a value, SIGHUP, and assert
  from the log that workers cycled and picked up the new value (item 18). Non-interactive `EDITOR=/bin/true`
  plus a `sed`-based stub editor. **Must clean up `-wal`/`-shm` sidecars and the pidfile.**
- **Verification:** manual run against a test pool; not part of default `pytest`.

### Item 23 — Final review

- **Owner:** `zfs-code-reviewer` · **Tag:** `low-risk`
- **Scope:** full diff, with specific attention to: the fork/engine lifecycle (item 5); `Duration`'s
  reflected comparison and truthiness against every consumer in the item-2 table; transactional guarantees
  in items 11/12/14; the cooperative-cycling guarantee that no worker is killed mid-stream (item 18); and
  whether `DatasetConfig.to_property()` (`config.py:88-107`) output is genuinely byte-unchanged.

---

## Risk map

| Risk | Where | Mitigation |
|---|---|---|
| **Pre-fork engine inherited by workers** — SQLite fd sharing corrupts the DB, not just errors | `daemon.py:62-68`, `store/db.py` | pid-keyed engine cache (item 5); supervisor disposes engine before `_start_workers()`; explicit fork test (item 9) |
| **A duration library silently mis-parses `1M` as 1 minute** — retention collapses, snapshots pruned immediately | `config.py:12-45` | **Verified failure in `pytimeparse` and `humanfriendly`.** No library added; hand-rolled parse/render reusing the existing tested parser (item 2) |
| **Duplicate retention `keep_for` silently drops a tier** — pre-existing. `{"1h":"7d","1d":"1w"}` collapses to one tier, and the tie-break systematically keeps the **coarser** interval, pruning harder than configured | `backup_manager.py:216-219`, tie-break via `config.py:155` | Reject on write (CLI, import, `UNIQUE(dataset_id, keep_for_seconds)`); `min` + `WARNING` on read so a collision can never be silent (item 2b) |
| **`Duration` wrapper breaks reflected comparison** — `age >= dsi.frequency` silently misbehaves | `backup_manager.py:205-206` | Subclass `timedelta` rather than wrap (item 2); explicit reflected-comparison test (item 9) |
| **`Duration` breaks `or`-truthiness** — a zero duration stops being falsy, changing inherited-frequency logic | `workers.py:146-150,161` | Inherited `__bool__` via subclassing; explicit zero-duration test (item 9) |
| **Literal rewritten across pickle/deepcopy** — a missing `__reduce__` no longer raises (native-kwargs support) and no longer yields `None` (synthesis); it **silently substitutes the canonical literal**, so `30d`→`1M` and `12h6d`→`6d12h` while canonical values look correct | `daemon.py:34`, test comparisons | `__reduce__` on the subclass (item 2); item-9 assertion **must use a non-canonical literal** — testing `1M` passes even with the bug present |
| **Wire-format breakage** — `to_property()` is sent to remote servers | `config.py:88-107`, `remote.py:108` | Base64-JSON format frozen; byte-equality assertion (item 9) |
| **Duration round-trip corrupts retention** — `1M` → `30d` rewrites a user's policy | `config.py:12-45` vs. item 12 | Literal preservation (item 2), 9-row table (item 9), real-file diff (item 22) |
| **Untyped YAML scalars reaching `Duration` directly** — `check_interval: 300` (meaning seconds) dispatches to native kwargs and silently becomes **300 days**, where the old parser raised. Found in the Phase 0 review | `config.py` load sites | Type guard at the config-loading boundary rejecting non-`str` with the offending key named; `Duration`'s constructor left unchanged. Regression test required |
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
  mean revising them immediately. Order: **2b → 3b → 3 → 4 → 5 → 6 → 8**. (3b's `remote.py` half —
  passing the destination to `to_property` — is independent of the schema and can land with either.)
- Both 2b and 3b are pure `zfsbackup` changes with **no DB dependency at all**, so both can be implemented,
  reviewed, and merged before any SQLAlchemy code is written. Recommended: ship them as their own cycle.
- **Phase 1 serial chain:** 3 → 4 → 5 → 6 → 8. Item 7 (Alembic) runs parallel to 4-6. Item 8 needs 4 and 6.
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
| **Encrypting destination credentials in the DB** | `Destination` is URL-only today (`config.py:58-61`); no secrets exist to protect yet. Revisit when auth lands |
| **Replacing the base64 user-property format** with a DB-derived one | Would break client/server compatibility (`remote.py:108`). Out of scope |
| **Multi-host / shared config DB** | SQLite over NFS is unsafe. If needed, that is a Postgres conversation — and the item-4 mapper layer is exactly what makes that migration possible later |
| **Running the daemon as a non-root user** | `zfs allow` delegation is not set up in the target environment, and on Linux `mount` stays privileged even when delegated — so `destroy` on a mounted dataset and `receive` both need a mitigation (`receive -u`, `canmount=noauto`, or `CAP_SYS_ADMIN`). Phase 1 ships the `zfsbackup:zfsbackup` file layout so nothing needs redoing; the daemon stays root until this workstream lands (item 6) |
| **Auto-cleanup of orphaned server-side datasets after a rename** | Requires deleting data on a remote machine based on a local config edit. Too dangerous to automate; item 14 prints the path instead |
| ~~**Fixing the odd leading-`m` branch** at `config.py:17-23`~~ — **no longer deferred** | Compound-literal support (item 2) rewrites the parser, and this branch is **dead code**: it computes a `unit` that `int(duration_str[:-1])` then always discards by raising. Verified — `m5`, `M5`, `mi5`, `mi` all raise. It is deleted as a side effect; both covering tests assert only `ValueError` and keep passing |
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

**Nothing has been implemented — this is a proposal.**

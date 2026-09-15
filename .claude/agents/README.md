# Agent team & development workflow

This directory holds the project-scoped agents for the `libzfs` repo. This file is the **authoritative
description of the development pipeline** — the individual agent prompts carry only their local role and
domain knowledge; the cross-agent workflow lives here (and is summarized in
[../../CLAUDE.md](../../CLAUDE.md)).

> **Orchestration note.** Subagents cannot call other subagents. Every "hand off to X" in an agent
> prompt is actually performed by the **main Claude session**, which drives the whole cycle. Read the
> pipeline below as instructions to that orchestrator, not as agent-to-agent calls.

## Roster

| Agent | Model / effort | Tools | Scope |
|---|---|---|---|
| `zfs-code-reviewer` | opus / high | read-only | Reviews the current diff (both packages); reports ranked findings, no edits |
| `concurrency-reviewer` | opus / high | read-only | Depth review of fork-safety, SQLite/WAL, IPC and signal hazards only |
| `libzfseasy-implementation-planner` | opus / high | read-only | Findings → ordered plan for `libzfseasy/` |
| `zfsbackup-implementation-planner` | opus / high | read-only | Findings → ordered plan for `zfsbackup/` (incl. `store/`, `cli/`) |
| `libzfseasy-developer` | sonnet / high | edit | `libzfseasy/types.py`, `zfs.py` |
| `zfsbackup-developer` | sonnet / high | edit | Daemon core: `config.py`, `backup_manager.py`, `daemon.py`, `workers.py`, `remote.py`, `api.py` |
| `zfsbackup-store-developer` | sonnet / high | edit | `zfsbackup/store/**` (incl. `migrations/`), `alembic.ini` — models, mapper, engine/session, migrations |
| `zfsbackup-cli-developer` | sonnet / high | edit | `zfsbackup/cli/**` — the `zfsbackup-config` Click application |
| `pytest-test-author` | sonnet / high | edit | Owns **all** pytest tests + `conftest.py` fixtures (both packages) |
| `real-zfs-scenario-dev` | sonnet / high | edit | Owns the shell scenarios under `scenarios/` |

Read-only agents (both reviewers, both planners) have no `Edit`/`Write` by design. Developers never
write tests — `pytest-test-author` owns them.

## File ownership is exclusive

Each source file has exactly one owning developer. No file is co-edited, and no developer reaches
into another's directory to save a handoff.

| Path | Owner |
|---|---|
| `libzfseasy/**` | `libzfseasy-developer` |
| `zfsbackup/{config,backup_manager,daemon,workers,remote,api}.py` | `zfsbackup-developer` |
| `zfsbackup/store/**` (incl. `store/migrations/`), `alembic.ini` | `zfsbackup-store-developer` |
| `zfsbackup/cli/**` | `zfsbackup-cli-developer` |
| `tests/**`, `conftest.py` | `pytest-test-author` |
| `scenarios/**` | `real-zfs-scenario-dev` |

**A plan item that spans two owners is split by the planner** into sequenced sub-items, one per
owner, each independently verifiable. The planner names which half lands first and what contract the
second half consumes. Known boundary-spanning items: DB path resolution (store + daemon + CLI),
DB-as-canonical-config (store + daemon), and `rename` (CLI + the daemon's ZFS user-property side
effect).

Two architectural boundaries the reviewers enforce:

- **Daemon and CLI reach the database only through `zfsbackup.store.mapper`** — never through ORM
  models. Config objects outlive any session; a detached instance raises `DetachedInstanceError`
  lazily, deep inside a worker loop. The mapper is also what keeps a later Postgres move possible.
- **The CLI is the only writer.** Workers open the store read-only, by construction rather than by
  convention.

## The cycle

```
                ┌─────────────────────────────────────────────────────────┐
                │                                                         (loop back)
                ▼                                                             │
  zfs-code-reviewer ──findings──▶ <pkg>-implementation-planner ──plan──▶ [ approval ] ──▶ <pkg>-developer
      ▲                                                                                       │
      │                                                                                       ▼
      └──────────────── re-review ◀── pytest-test-author  +  real-zfs-scenario-dev ◀──── implement
                                        (tests / scenarios)
```

1. **Review** — `zfs-code-reviewer` inspects the working diff and reports findings, most-severe first.
2. **Plan** — the package's implementation-planner turns findings (or a bug report / feature request)
   into an ordered, dependency-sequenced plan. Every item cites its basis, is independently approvable,
   and carries a **risk tier** (see below).
3. **Approve** — the plan is a *proposal*. See loop behavior for how approval works interactively vs.
   under `/loop`.
4. **Implement** — the package's developer edits only its own files, keeping changes minimal and
   idiomatic. `libzfseasy/` → `libzfseasy-developer`; `zfsbackup/` → `zfsbackup-developer`.
5. **Test** — the developer notes coverage gaps; `pytest-test-author` writes/updates pytest tests and
   `real-zfs-scenario-dev` handles shell-based real-world coverage (pipe/deadlock/two-VM cases that
   mocked tests cannot prove).
6. **Re-review** — `zfs-code-reviewer` reviews the resulting diff. For changes to engine/session
   setup, daemon config loading, the reload state machine, signal handlers, IPC payloads, or worker
   cycling, `concurrency-reviewer` reviews the same diff in parallel. The two are complementary:
   breadth and depth on one hazard class. Run the depth pass **only** on those items — it is not a
   second opinion on ordinary changes.

Planners coordinate directly when a `libzfseasy` stream-contract change would surface in `zfsbackup`'s
`remote.py`.

**Parallelism.** Independent lanes run as concurrent `Agent` calls in a single message. In the
current config-store workstream that means: Alembic scaffolding runs alongside the mapper and
engine work, and the CLI's dot-path, editor, list, and generation-counter items all run in parallel
once the CLI skeleton lands. Serial chains stay serial — schema → mapper → engine → DB path →
DB-as-canonical, and reload trigger → worker cycling → CLI trigger.

**Model overrides.** Developers default to sonnet. Pass `model: opus` on the `Agent` call for the
items where being wrong corrupts data rather than failing a test: engine/session fork safety, the
reload state machine, and cooperative worker cycling. Three items, not the whole plan.

## Stop conditions (one iteration)

- **Done:** re-review returns no material findings and the relevant test selection passes.
- **Loop back:** re-review returns findings → feed them to the planner and run another cycle.
- **Escalate:** a `needs-approval` item, an ambiguous requirement, or a finding that reopens a design
  question → pause and surface to the user rather than guessing.

## Loop-style development (`/loop`)

The plan step is a human gate by default. Under `/loop` there is no user to approve each cycle, so
approval is **risk-tiered**:

- Planners tag every item `low-risk` or `needs-approval`.
- `needs-approval` covers: in `libzfseasy` — exec/stream-contract changes, breaking public-API changes
  (renaming a `zfs.*` singleton, altering `Validate`/error conventions, changing the integer-index
  property storage), and pipe/stream/process-cleanup hazards; in `zfsbackup` — retention/timeslot
  logic, multiprocessing/IPC/signals, remote-transfer/API/security, and config round-trip/user-property
  format changes. Anything a planner is genuinely unsure about is also `needs-approval`.
- **Interactive run:** present the whole plan and wait for approval (unchanged).
- **`/loop` run:** `low-risk` items proceed to the developer automatically — the `zfs-code-reviewer`
  pass and the test suite are the safety net — while `needs-approval` items pause and surface to the
  user. The loop's stop condition per iteration is the "Done" case above.

## Conventions maintained across the team

- Default branch is `master`; branch before committing.
- The `zfs`/`zpool` exec contract (argv-as-list, never `shell=True`; `ValueError` for validation, bare
  `Exception('\n'.join(errors))` for subprocess failure; `-H` for machine-readable output) is preserved
  by developers and checked by the reviewer.
- Trust the source when docs and code disagree, and flag the stale doc.

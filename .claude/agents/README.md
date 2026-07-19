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
| `libzfseasy-implementation-planner` | opus / high | read-only | Findings → ordered plan for `libzfseasy/` |
| `zfsbackup-implementation-planner` | opus / high | read-only | Findings → ordered plan for `zfsbackup/` |
| `libzfseasy-developer` | sonnet / high | edit | Implements plans in `libzfseasy/types.py`, `zfs.py` |
| `zfsbackup-developer` | sonnet / high | edit | Implements plans in `zfsbackup/` (config, retention, workers, daemon, remote, api) |
| `pytest-test-author` | sonnet / high | edit | Owns **all** pytest tests + `conftest.py` fixtures (both packages) |
| `real-zfs-scenario-dev` | sonnet / high | edit | Owns the shell scenarios under `scenarios/` |

Read-only agents (reviewer, both planners) have no `Edit`/`Write` by design. Developers never write
tests — `pytest-test-author` owns them.

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
6. **Re-review** — `zfs-code-reviewer` reviews the resulting diff.

Planners coordinate directly when a `libzfseasy` stream-contract change would surface in `zfsbackup`'s
`remote.py`.

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

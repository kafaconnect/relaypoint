---
name: openspec-apply-change
description: Implement tasks from an OpenSpec change. Use when the user wants to start implementing, continue implementation, work through tasks, or update task status while keeping GitHub Projects board state in sync.
license: MIT
compatibility: Requires openspec CLI.
metadata:
  author: openspec
  version: "1.1"
  generatedBy: "1.4.1"
---

Implement tasks from an OpenSpec change.

**Input**: Optionally specify a change name. If omitted, infer only when safe; if
ambiguous, prompt for available changes.

## Steps

1. **Select the change**

   If a name is provided, use it. Otherwise:
   - infer from conversation context if the user named a change;
   - infer from current branch `change/<name>`;
   - auto-select only if one active change exists;
   - if ambiguous, run `openspec list --json` and ask the user.

   Announce: `Using change: <name>`.

2. **Check status**

   ```bash
   openspec status --change "<name>" --json
   ```

   Parse `schemaName`, `planningHome`, `changeRoot`, `actionContext`, and the task
   artifact paths.

   If `actionContext.mode` is `workspace-planning` and `allowedEditRoots` is empty,
   STOP before editing and explain that full workspace apply is not supported here.

3. **Get apply instructions**

   ```bash
   openspec instructions apply --change "<name>" --json
   ```

   Handle states:
   - `blocked`: show the missing artifact/blocker and stop.
   - `all_done`: say all tasks are complete and suggest archive gates.
   - otherwise continue.

4. **Read context files**

   Read every concrete path listed under `contextFiles`.

5. **Show current progress**

   Display schema, progress `N/M`, remaining task overview, and the dynamic instruction.

6. **Ensure board linkage before implementation**

   Tasks are per-file under `openspec/changes/<name>/tasks/`. If task files do not have
   `issue:` frontmatter, run the `change-planning` skill in board-link mode before
   coding. Required values are Release Train, Iteration, assignee, and optional
   Capability/Risk. If those cannot be inferred from `.github/project.yml`, the branch,
   existing issues, or the user request, ask before creating issues.

7. **Implement tasks**

   Discovery commands:

   ```bash
   grep -l "^status: in_progress" openspec/changes/<name>/tasks/*.md
   grep -l "^status: todo" openspec/changes/<name>/tasks/*.md | sort | head -1
   ```

   For each task:
   - set frontmatter `status: in_progress`;
   - append a dated `## Log` line for the start;
   - regenerate `tasks.md` with `scripts/tasks-index.sh <name>` if present;
   - run `change-planning` board-sync so the task issue moves to `In Progress`;
   - make the minimal code changes for that task;
   - append short log entries only for decisions, evidence, or blockers;
   - on completion, set `status: done`, squash the task log to one outcome line with
     evidence such as test command or commit hash, regenerate `tasks.md`, and run
     board-sync so the task issue closes and moves to `Done`;
   - commit status/index/code together when committing is part of the workflow.

   If blocked, set `status: blocked`, name the blocker in the task log, regenerate
   `tasks.md`, run board-sync, then report the blocker.

8. **Pause conditions**

   Pause and ask when the task is unclear, implementation reveals a design/spec issue,
   required board values are missing, tests expose unrelated breakage, or the user
   interrupts.

9. **On completion or pause**

   Show tasks completed this session, overall progress, verification commands run, and
   any board-sync failures that need retry.

## Guardrails

- Keep going through tasks until done or blocked.
- Always read context files before starting.
- Do not implement legacy checkbox-only `tasks.md` as the canonical task source; convert
  to per-task files first.
- Keep changes minimal and scoped to the active task.
- Do not mark a task `done` without evidence.
- Board state follows task frontmatter; do not edit board scope directly.

## Fluid workflow integration

This skill can run before all artifacts are final, after partial implementation, or
interleaved with proposal/design updates. If implementation changes scope, update
OpenSpec first, then rerun board-link/board-sync.

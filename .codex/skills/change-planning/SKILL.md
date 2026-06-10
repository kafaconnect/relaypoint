---
name: change-planning
description: Human-in-the-loop planning for an OpenSpec change, plus idempotent GitHub Projects board sync for existing or new changes. Use when starting a change, creating Story/Task/Bug issues, syncing tasks to the board, linking Epic -> Story -> Task hierarchy, assigning issues, setting milestone/iteration/release train, or updating board status during apply.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
  github_cli: ">=2.45.0"
  agents: [claude-code, codex]
metadata:
  version: "1.1.0"
  owner: platform
  modes: [plan, board-link, board-sync]
  board: { source: ".github/project.yml or explicit CLI args", project_type: org-project-v2 }
---

# change-planning

OpenSpec owns **definition**; the GitHub board owns **delivery state**. Sync is
one-way: OpenSpec -> board. Never copy spec text into issues; link to OpenSpec files.

## Canonical decomposition

- **Epic** = exactly one OpenSpec change. One GitHub issue.
- **Story** = one vertical, demoable slice. In this repo, story is normally derived
  from task frontmatter `slice` (`V0`, `V1`, etc.) unless the user supplied a clearer
  story split.
- **Task** = one executable unit. It MUST be one file under
  `openspec/changes/<change>/tasks/<SLICE>-<NN>-<slug>.md` with frontmatter:
  `id`, `slice`, `title`, `status`, optional `issue`, and `specs`.
- **Bug** = a defect against an accepted spec.

Every task gets its own GitHub issue. Use native GitHub sub-issues for hierarchy:
Epic -> Story -> Task. `tasks.md` is generated from task files only; never hand-edit it.

## Mode: plan

1. Present a short planning brief and STOP for the user before writing artifacts.
   Cover scope, explicit out-of-scope, design choices, test strategy, contract/data/RLS
   impact, UI/prototype impact, Release Train, Iteration, assignees, and Story/Task/Bug
   breakdown.
2. After alignment, create `proposal.md`, `design.md` when needed, delta specs, and
   per-task files under `tasks/`.
3. Run `openspec validate <change> --strict`.
4. Run board-link to create/link the board work items.

If asked only to sync board state for an existing change, skip plan and use board-link.

## Mode: board-link

Use this when the user says "sync task lên board", "link epic/story/task",
"kéo task vào story/epic/milestone", "assign tasks", or similar.

Fresh-session resolution order:

1. Change id from the user.
2. Current branch `change/<change>`.
3. The only active directory under `openspec/changes/`.
4. If still ambiguous, ask.

Board config resolution order:

1. `.github/project.yml` for `github.org`, `github.repo`, `project.number`, and
   `project.id`.
2. Explicit `ORG`, `REPO`, `PROJECT_NUMBER`, or script CLI args.
3. If missing, STOP and tell the user to run `board-bootstrap` first. Create no issue.

Required delivery value resolution order:

1. Explicit user request or command/env value.
2. Existing linked Epic/Story/Task metadata on the board (milestone, assignee, project
   fields) when this is a re-sync.
3. Repo defaults documented in `.github/project.yml` if present.
4. If `Release Train`, `Iteration`, or `Assignee` is still missing, ask the user before
   running the helper. Do not invent values. `Capability` and `Risk` are optional, but
   ask when the board requires them or the user asked to set them.

Run the bundled helper from this skill directory:

```sh
python scripts/sync_board.py \
  --change "$CHANGE" \
  --release-train "$RELEASE_TRAIN" \
  --iteration "$ITERATION_TITLE" \
  --assignee "$ASSIGNEE" \
  --capability "$CAPABILITY" \
  --risk "$RISK"
```

The helper is idempotent. It MUST:

- reuse task frontmatter `issue:` values;
- create missing `type:epic`, `type:story`, `type:task`, and
  `openspec-change:<change>` labels;
- ensure the milestone exists;
- create or reuse one Epic issue for the change;
- create or reuse one Story issue per `slice`;
- create or reuse one Task issue per task file, then write `issue: <number>` back
  to task frontmatter;
- add all issues to the org Project;
- set project fields when present: `Status`, `Iteration`, `Release Train`, `Mstone`,
  `Capability`, `Risk`, `OpenSpec Change`;
- assign all issues to the requested assignee;
- close task issues whose frontmatter status is `done`;
- link native GitHub sub-issues Epic -> Stories and Story -> Tasks;
- regenerate `tasks.md` with `scripts/tasks-index.sh <change>` when the repo has it;
- print a verification summary with counts and missing fields.

Status mapping:

- `todo` -> Project Status `Ready`, issue open.
- `in_progress` -> `In Progress`, issue open.
- `blocked` -> `Blocked`, issue open.
- `done` -> `Done`, issue closed.

If a change still uses legacy checkbox-only `tasks.md` and has no `tasks/*.md`, STOP
and ask to convert to per-task files first. Do not create a parallel board model from
legacy checkboxes.

## Mode: board-sync

Use board-sync during implementation. It is the same helper as board-link, rerun after
task status changes.

Trigger points:

- Start a task: set task frontmatter `status: in_progress`, then board-sync.
- Block a task: set `status: blocked` with a dated log line naming the blocker, then
  board-sync.
- Complete a task: set `status: done`, record evidence in the task log, regenerate
  `tasks.md`, then board-sync.
- Open PR: move related Story/Epic to `Review` if the work is ready for review.
- QA GO + merge: close done tasks and move completed parent items to `Done`.

## Verification checklist

After board-link/board-sync, verify:

- `openspec validate <change> --strict` passes.
- Number of Story issues equals number of task slices.
- Number of Task issues equals number of task files.
- Every task file has `issue:`.
- Every created/reused issue has assignee and milestone.
- Epic has all Story sub-issues; every Story has its Task sub-issues.
- Project sample items show expected `Status`, `Iteration`, `Release Train`/`Mstone`,
  and `OpenSpec Change`.

## Guardrails

- Never create issues before the board is resolved.
- Never duplicate an issue when `issue:` exists or a matching labelled issue exists.
- Never change OpenSpec scope from the board. Update OpenSpec first.
- On GitHub rate limit or partial project-field failure, print the unsynced issue
  numbers and rerun the helper after the limit resets.

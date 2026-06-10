---
name: "OPSX: Board Sync"
description: Sync OpenSpec task files to GitHub Projects hierarchy
category: Workflow
tags: [workflow, github, board, openspec]
---

Sync an existing OpenSpec change to the GitHub Projects board.

Use the **change-planning** skill in `board-link` mode for initial linkage and
`board-sync` mode after task status changes.

Required behavior:

- Resolve the change from the argument, current `change/<name>` branch, or ask.
- Read `.github/project.yml` first. If missing, require org/repo/project number or stop
  and tell the user to run `board-bootstrap`.
- Read per-task files from `openspec/changes/<change>/tasks/*.md`.
- Create/reuse one Epic issue, one Story issue per task `slice`, and one Task issue per
  task file.
- Link native GitHub sub-issues Epic -> Story -> Task.
- Set milestone, Iteration, Release Train/Mstone, optional Capability/Risk, and
  OpenSpec Change project fields when present.
- Assign all issues to the requested assignee.
- Write `issue: <number>` into task frontmatter and regenerate `tasks.md`.

If the change has only legacy checkbox `tasks.md` and no per-task files, stop and ask to
convert to per-task files first.

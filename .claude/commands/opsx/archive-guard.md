---
name: "OPSX: Archive guard"
description: Pre-archive gate — cross-review blocker-free + every linked issue GO
category: Workflow
tags: [workflow, archive, qa]
---

Gate a change before archiving. Run this BEFORE `/opsx:archive` — the built-in
archive command does NOT call it for you.

Use the **archive-guard** skill: for the active change, verify a
`reviews/cross-review-*.md` exists with no unresolved `[BLOCKER]`, and that every
linked Story/Task/Bug issue (from task-file frontmatter `issue:` refs or the
`openspec-change:<change>` label) has a `GO` report under `qa/`. On any failure it
BLOCKS and prints the precise reasons; if all pass it states archive is allowed.

Argument after the command (optional): the change id. If omitted, infer from context
or ask. Only run `/opsx:archive` after this returns ALLOW.

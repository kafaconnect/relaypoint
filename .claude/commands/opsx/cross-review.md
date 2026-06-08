---
name: "OPSX: Cross-review"
description: Independent AI peer review of an OpenSpec change (builder ≠ reviewer)
category: Workflow
tags: [workflow, review, qa]
---

Run an independent cross-review of the current OpenSpec change before archiving.

Use the **cross-review** skill: identify the host agent, delegate to the other
local agent CLIs (claude/codex/agy) as independent reviewers, and record findings
vs the spec delta + Definition of Done in `openspec/changes/<change>/reviews/`.

Argument after the command (optional): the change id. If omitted, infer from
context or ask. Reviewers produce findings only — the human owns the verdict.

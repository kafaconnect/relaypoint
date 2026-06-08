---
name: "OPSX: Plan"
description: Human-in-the-loop change planning + GitHub Projects board sync
category: Workflow
tags: [workflow, planning, github]
---

Plan a new OpenSpec change with the user, then create the board work items.

Use the **change-planning** skill (mode: plan): FORCE alignment on gray areas,
design, and test strategy via AskUserQuestion BEFORE finalizing artifacts; then
create the Epic/Story/Task/Bug issues, set Iteration + Release Train, assign, and
write issue numbers back into `tasks.md`. OpenSpec owns definition; the board owns
state (one-way sync).

Run this before `/opsx:propose`. Argument after the command (optional): a short
description of the change.

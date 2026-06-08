---
name: "OPSX: QA verify"
description: GO / NO-GO gate for a Story/Task/Bug (scenarios + prototype fidelity)
category: Workflow
tags: [workflow, qa, testing]
---

Verify a completed Story/Task/Bug and produce a GO / NO-GO verdict.

Use the **qa-verify** skill: check the OpenSpec scenarios via `// @spec:` tagged
tests, verify UI fidelity against the playground prototype at
`docs/prototype`, then record a verdict to
`openspec/changes/<change>/qa/` and comment it on the issue. A NO-GO blocks the
board item from moving to Done and blocks archive.

Argument after the command (optional): the issue number to verify.

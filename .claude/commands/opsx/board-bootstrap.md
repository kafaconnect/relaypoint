---
name: "OPSX: Board bootstrap"
description: One-time, idempotent setup of the org GitHub Projects v2 board
category: Workflow
tags: [workflow, github, setup]
---

Provision the GitHub Projects v2 delivery board for `kafaconnect/desk` — once.

Use the **board-bootstrap** skill: create or detect the org Project v2 (print
`PROJECT_NUMBER` + `PROJECT_ID`), create the custom fields (Status, Iteration,
Release Train, Capability, Risk, OpenSpec Change) with their single-select options
via GraphQL, and create the `type:`/`domain:`/`channel:`/`ci:`/`risk:` labels. Every
step is check-then-create, so it is safe to re-run.

Run this once before the first `/opsx:plan`. When it finishes, record the printed
`PROJECT_NUMBER` (e.g. as a repo/org variable) so `change-planning` can consume it.

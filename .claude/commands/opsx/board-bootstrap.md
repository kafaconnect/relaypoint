---
name: "OPSX: Board bootstrap"
description: One-time, idempotent setup of the org GitHub Projects v2 board
category: Workflow
tags: [workflow, github, setup]
---

Provision the GitHub Projects v2 delivery board for the current repository — once.

Use the **board-bootstrap** skill: create or detect the org Project v2 (print
`PROJECT_NUMBER` + `PROJECT_ID`), create the custom fields (Status, Iteration,
Release Train, Mstone, Capability, Risk, OpenSpec Change) with their single-select
options via GraphQL, create the `type:`/`domain:`/`channel:`/`ci:`/`risk:` labels, and
write `.github/project.yml`. Every step is check-then-create, so it is safe to re-run.

Run this once before the first `/opsx:plan` or `/opsx:board-sync`. Future sessions
must read `.github/project.yml` instead of guessing board ids.

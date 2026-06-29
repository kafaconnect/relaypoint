# AGENTS.md — relaypoint

Guidance for any AI agent (Claude, Codex, agy) working in this repo. RelayPoint is a
NATS web **signaling backbone** (the signaling service for KafaConnect Desk; standalone +
reusable). See `openspec/project.md`.

## Operating rules — Karpathy's 4 (MUST follow)

1. **Think before coding.** State assumptions; when ambiguous, surface interpretations and ASK — never silently pick one. Surface trade-offs, push back when warranted.
2. **Simplicity first.** Minimum code that solves the stated problem. No speculative features/abstractions. If 200 lines could be 50, rewrite.
3. **Surgical changes.** Touch only what the task requires; every changed line traces to the request; clean up dead code.
4. **Goal-driven execution.** Turn vague asks into verifiable success criteria first; "fix the bug" → a failing test, then make it pass.

**Sub-agents — at most 2 concurrent.** Delegate heavy work to keep the orchestrator's context lean, but never run more than 2 at once.

The comment rule under **Code style** is also mandatory.

## How we work

Spec-driven via **OpenSpec** (`openspec/`). Behavior in `openspec/specs/`; proposed work in
`openspec/changes/<name>/`. Architecture + decision rationale in `docs/architecture/` (C4,
**HTML**) + `docs/architecture/decisions/` (ADRs, Markdown). OpenSpec/AGENTS/skill files
stay Markdown (tooling); human-facing docs are **HTML** (see the `docs-writer` skill).

Skills: built-in OpenSpec (`explore/propose/apply/archive/sync`) + custom
(`change-planning`, `cross-review`, `qa-verify`, `archive-guard`, `board-bootstrap`,
`docs-writer`). Independent **cross-review** (builder ≠ reviewer) before archive.

## Definition of Done (every change)

`openspec validate --strict`; lint/typecheck/test/coverage green; every `#### Scenario:`
has a `// @spec:<id>` test; an independent cross-review is recorded; an ADR is added if
architecture changed. A subject/transport change must name the exact NATS subject(s)/stream(s).

## Loose coupling — design for replacement (HARD RULE)

Core logic MUST depend on **abstractions (ports/interfaces) it owns, not on NATS or any
concrete client/SDK**. NATS is the chosen backbone, but it is a swappable **adapter**
behind a port the core defines (e.g. the router depends on a `LogStore` port, not on
`nats.JetStreamContext`). The core MUST be **unit-testable with a fake/in-memory
adapter** — no live NATS to test behaviour — and the backbone MUST be replaceable
without rewriting the core. A core package importing a concrete client into its logic
is a review defect. This is a **MANDATORY cross-review criterion**; the only exception
is a documented, justified one, confined to an adapter/edge — never the domain core.

## Project invariants

- **Media never touches NATS** — only SDP/ICE transit it; A/V are WebRTC P2P (coturn for NAT).
- Subjects: tenant-prefixed, dot-separated, lowercase; offer ≠ updates; QoS split (`.log` JetStream vs `.signal` core NATS); medium is a payload field, never a subject.

## Code style

Full conventions: **`docs/conventions.md`**. **Comments — DEFAULT ZERO.** Exactly TWO kinds
are allowed: a **WHY** comment (a non-obvious rationale/constraint/trade-off/gotcha the code
cannot express — may cite a change/issue) and a **TODO**. Nothing else: no "what"/how
comments, no doc-essays restating a signature/type/test, no step narration, no banners, no
commented-out code. A non-WHY/non-TODO comment is a review DEFECT that BLOCKS merge — delete
it or fix the code (rename/extract). Machine tags (`// @spec:<id>`, `//go:…`, build/struct
tags) are exempt. Applies to every file and language; when delegating, state this rule and
`grep '^\+\s*//'` the diff before committing — sub-agents are the repeat offenders.

## Licensing

The project is **source-available** (free internal use; commercial distribution needs a
license — see `LICENSE`). Third-party **dependencies** must be 100% open source.

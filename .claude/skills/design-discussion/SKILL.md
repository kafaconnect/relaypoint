---
name: design-discussion
description: BEFORE writing implementation code for an OpenSpec change, discuss the design pattern + pseudocode with independent agents (codex/agy if present, else 1-2 sub-agents), reach a consolidated decision, and record it in the change's design.md. The before-code control gate — "control before code, not fix after code". Use after propose/spec and BEFORE apply.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
  agents: [claude-code, codex]
metadata:
  version: "1.0.0"
  owner: platform
  modes: [discuss]
---

# design-discussion

**Control before code, not fix after code.** The cross-review skill catches defects *after*
they are written; this skill prevents them by agreeing the **design pattern + pseudocode**
*before* a single line is implemented. It runs AFTER the spec is written (propose/spec) and is
a **hard gate before `apply`** (writing code). It is cheaper and safer to change a pseudocode
block than a coded, tested, reviewed implementation.

This is NOT cross-review (that stays, after code, as the confirmation). This is the design
agreement that makes cross-review boring.

## When to run

- After the change's `proposal.md` + `specs/` exist and BEFORE `apply` writes code.
- Required for **T1 (Foundational)** and **T2 (Core)** changes; recommended for T3 with any
  non-trivial control flow. A pure-docs/config change with no code may skip it (note why).
- Re-run for a slice if its implementation approach materially diverges from the agreed design.

## 1. Resolve the change + frame the design question (the builder does this)

```sh
export REPO_ROOT="$(git rev-parse --show-toplevel)"; cd "$REPO_ROOT"
export CHANGE="${CHANGE:?openspec change id}"
export TS="$(date -u +%Y%m%dT%H%M%SZ)"
export REVIEW_DIR="openspec/changes/$CHANGE/reviews"; mkdir -p "$REVIEW_DIR"
export SCRATCH="$(mktemp -d -t oc-design.XXXXXX)"
openspec validate "$CHANGE" --strict
```

The builder writes `$SCRATCH/frame.md`: for each non-trivial unit of work in the change,
(a) the **candidate design pattern(s)** (ports/seams, state machine, data flow, concurrency
model, failure/idempotency strategy — reference existing repo patterns + the ADRs the change
touches), and (b) **PSEUDOCODE** for the load-bearing paths (the happy path AND the failure /
race / retry / idempotency edges — not just the sunny day). Frame the OPEN design questions
explicitly (what's genuinely undecided), and name the alternatives considered.

## 2. Pick the discussants (independent; NOT the builder)

Same rule as cross-review — the builder does not rubber-stamp its own design. Discussants are
the OTHER local agent CLIs; if none is available, spawn 1–2 **sub-agents** to discuss.

```sh
export SELF="${OPENSPEC_AGENT:?set OPENSPEC_AGENT=claude|codex|agy}"
discussants=""
[ "$SELF" != codex ] && command -v codex >/dev/null && discussants="$discussants codex"
[ "$SELF" != agy   ] && command -v agy   >/dev/null && discussants="$discussants agy"
[ "$SELF" != claude ] && command -v claude >/dev/null && discussants="$discussants claude"
# Fallback: if no independent CLI is present, spawn 1-2 sub-agents (Task/Agent tool) as discussants.
```

## 3. The discussion prompt (what to ask the discussants)

Hand each discussant `$SCRATCH/frame.md` + the change folder, READ-ONLY (no edits). Ask them to
**engage with the design, not grade prose** — for each unit:

- Is the **pattern** the right one? Name a better-fitting pattern if so, with the trade-off.
  Does it honour the touched ADRs + the loose-coupling / business-logic-in-services HARD RULES?
- Walk the **pseudocode** and find the holes: an unhandled state/transition, a race with no
  resolver, a non-idempotent effect, a failure path that drops or double-acts, a contract the
  pseudocode implies but doesn't exist. Propose the concrete fix in pseudocode terms.
- Is anything **over-engineered** (simpler pattern suffices) or **under-specified** (a decision
  punted to code that should be made now)?
- Answer each OPEN question with a recommendation + reason.

Required output per discussant:

```
DESIGN-VERDICT: AGREE | AGREE_WITH_CHANGES | REWORK
PER-UNIT:                # one line per unit: pattern OK|CHANGE — pseudocode OK|GAP — the fix
ALTERNATIVES:            # better patterns proposed, with the trade-off, or "none"
PSEUDOCODE-HOLES:        # unhandled state/race/failure/idempotency/contract gaps, or "none"
OVER/UNDER:              # over-engineered or under-specified spots, or "none"
OPEN-QUESTIONS:          # an answer + reason for each framed question
```

Invoke read-only, timeout-bounded (mirror cross-review's recipes): `codex exec --sandbox
read-only`, `agy -p --print-timeout`, `claude -p --permission-mode plan`. Run discussants in
parallel; capture each output.

## 4. Consolidate + DECIDE (the builder owns the decision)

The builder reads the discussants' outputs, resolves disagreements, and **decides** — this is a
discussion to converge, not a vote. Record the outcome in TWO places:

1. **The change's `design.md`** — a `## Design discussion (agreed <TS>)` section: the chosen
   pattern per unit, the agreed PSEUDOCODE for the load-bearing paths, the alternatives
   rejected (+ why), and any residual risk to watch during implementation. This is what `apply`
   builds from.
2. **`$REVIEW_DIR/design-discussion-$TS.md`** — the artifact: discussants, each raw output, and
   the consolidated decision. (The audit trail, like a cross-review record.)

Any unresolved REWORK-level disagreement on a load-bearing path BLOCKS `apply` until resolved
(re-frame + re-discuss the contested unit, or escalate to the owner).

## 5. Gate

`apply` (writing implementation code) MUST NOT start until the `## Design discussion` section
exists in `design.md` for the change (and the discussion artifact is recorded). The change
lifecycle is: propose → spec → **design-discussion (this gate)** → apply → cross-review →
qa-verify → archive. cross-review then confirms the code matches the agreed design; it should
find far less, because the design was controlled first.

## Failure modes / guardrails

- No independent CLI present → spawn 1–2 sub-agents as discussants; never skip with "I agree
  with myself" (the whole point is independence).
- A discussant tries to edit files → discard; the recipes are read-only by construction.
- Frame too large → discuss the highest-blast-radius units first (state machines, concurrency,
  contracts, tenancy); a leaf unit with obvious control flow can be noted as "trivial, no
  discussion needed".
- Do NOT let this become code-in-prose: pseudocode states intent + the hard edges; it is not
  the implementation. Stop at the level where the pattern + the edge-handling are unambiguous.

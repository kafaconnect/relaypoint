---
name: cross-review
description: Run an INDEPENDENT AI peer review of an OpenSpec change — the builder must never review its own work. Detects the host agent, delegates to the other local agent CLIs (claude/codex/agy), and records findings vs the spec delta + Definition of Done in the change folder. Use before archiving a change or on a PR.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
  agents: [claude-code, codex, agy]
metadata:
  version: "1.0.0"
  owner: platform
  hooks: [before-archive, pull-request]
  outputs:
    report_dir: "openspec/changes/<change>/reviews"
---

# cross-review

Goal: an independent verdict. The agent that wrote the code is the *builder* and
must NOT be a reviewer. Reviewers produce **findings only** — a human owns the
merge/archive decision.

## 1. Resolve the change + diff (read-only)

Each change is developed on its own branch (branch-per-change).

```sh
export REPO_ROOT="$(git rev-parse --show-toplevel)"; cd "$REPO_ROOT"
export CHANGE="${CHANGE:?openspec change id, e.g. m1-unified-inbox}"
export BASE="${BASE_REF:-origin/main}"
export TS="$(date -u +%Y%m%dT%H%M%SZ)"
export REVIEW_DIR="openspec/changes/$CHANGE/reviews"; mkdir -p "$REVIEW_DIR"
export SCRATCH="$(mktemp -d -t oc-review.XXXXXX)"

openspec validate "$CHANGE" --strict
git diff "$BASE"...HEAD | grep -q . || { echo "no diff vs $BASE — check out the change branch first"; exit 2; }
git diff --stat "$BASE"...HEAD            > "$SCRATCH/diff.stat"
git diff "$BASE"...HEAD                   > "$SCRATCH/diff.patch"
```

## 2. Identify the host (builder) agent — do not guess silently

`OPENSPEC_AGENT` is the REQUIRED signal. The `ps`-based detection is advisory ONLY:
if `OPENSPEC_AGENT` is unset you may derive a guess, but you MUST require explicit
human confirmation of "builder = X" before delegating.

```sh
export SELF="${OPENSPEC_AGENT:-}"
if [ -z "$SELF" ]; then
  GUESS="$(ps -o args= -p "$PPID" 2>/dev/null | tr A-Z a-z | grep -Eo 'claude|codex|agy' | head -1 || true)"
  echo "OPENSPEC_AGENT unset; ps guess: builder = ${GUESS:-unknown}."
  echo "Require explicit human confirmation of \"builder = X\" before delegating, then set OPENSPEC_AGENT=claude|codex|agy."
  exit 2
fi
case "$SELF" in claude|codex|agy) ;; *) echo "Set OPENSPEC_AGENT=claude|codex|agy"; exit 2;; esac
```

## 3. Pick the independent reviewers (exclude the builder)

```sh
reviewers=""
[ "$SELF" != claude ] && command -v claude >/dev/null && reviewers="$reviewers claude"
[ "$SELF" != codex  ] && command -v codex  >/dev/null && reviewers="$reviewers codex"
[ "$SELF" != agy    ] && command -v agy    >/dev/null && reviewers="$reviewers agy"
[ -z "$(echo $reviewers)" ] && { echo "No independent reviewer available — DoD cross-review cannot pass."; exit 2; }
```

## 4. Reviewer recipe table

| Reviewer | Invoke (run from `$SCRATCH`) | Isolation | ~Wait |
|---|---|---|---|
| codex  | `timeout "${REVIEW_TIMEOUT:-12m}" codex exec --sandbox read-only "$PROMPT" > out.md` | read-only sandbox | 3–8 min |
| agy    | `timeout "${REVIEW_TIMEOUT:-12m}" agy -p "$PROMPT" --print-timeout 8m > out.md` | print mode; **never** `--dangerously-skip-permissions` | 3–6 min |
| claude | `timeout "${REVIEW_TIMEOUT:-12m}" claude -p --permission-mode plan "$PROMPT" > out.md` | plan mode = read-only | 3–6 min |

## 5. Review rubric (MANDATORY — enumerate, don't spot-check)

This is the heart of the review, and it is **domain-agnostic** — it asks *how* to
review, never *what* this repo does. A reviewer that only reads what's written and
nods is worthless: a "read and nod" pass passes a design that is missing a state
exit, an unhandled race, or a forged-write path, because nobody drew the table to
see the blank. **Review the negative space:** for every dimension below the reviewer
MUST *build the artifact itself* (the state diagram, the actor-pair matrix, the
failure list, the contract inventory) from the change, then report the **blanks** —
the cases that are NOT specified — not just grade what is. A dimension is `OK` only
once the reviewer has enumerated it and found no blank.

Both modes use this rubric. **Plan/design review** (proposal + design + spec delta,
before code): evidence = "is it specified in a Requirement/Scenario?". **Code review**
(the diff): evidence = "is it implemented AND covered by a `// @spec:` test?". Same
dimensions, different evidence bar.

| # | Dimension | The reviewer MUST enumerate and check |
|---|---|---|
| R1 | **State machines** | For each stateful entity touched by the change, list ALL states and EVERY transition incl. terminal / error / timeout / cancel paths. Flag any state with no exit, any happy path with no failure/abort sibling, any transition that is implied but never written down. |
| R2 | **Lifecycle edges** | For each happy path, derive its variants — abort mid-flight, timeout, retry, reconnect/resume, duplicate, out-of-order, expiry, empty/zero/max case — and check each is specified. A missing variant is a blank. |
| R3 | **Races / concurrency** | Enumerate every pair of actors that can touch shared state at once (two writers; reader vs writer; retry vs original; cancel vs complete). Each MUST name its resolver: single authority, CAS/optimistic lock, idempotency key, or defined ordering. "Probably fine" = BLOCKER. |
| R4 | **Failure / recovery** | Crash, restart, network partition, credential/lease expiry, peer offline, lost/duplicate/reordered message, dependency down/timeout. For each: is the recovery path defined and idempotent? Is stuck state swept/reaped? |
| R5 | **Authority / security / tenancy** | For each write/effect: who is allowed to produce it, and can a caller forge it (actor == identity)? Least-privilege grants, tenant/data isolation, privileged-action audit, secrets handling, input validation. |
| R6 | **Contract / SSoT integrity** | Every behavior change has a `### Requirement:` + `#### Scenario:` with a stable id; and every contract surface that EXISTS in this repo stays consistent end-to-end — whichever apply of: data schema/migrations, sync API, service/event contracts, topics/subjects, cache keys, UI states. Flag drift, a breaking change with no version, or a rollback gap. |
| R7 | **Idempotency / ordering / delivery** | Dedup key present, sequence/ordering defined, exactly-once *effect* (not just delivery), replay-safe. |
| R8 | **Test traceability + project DoD** | (code review) every Scenario id has its tracing test (e.g. `// @spec:<id>`); lint/typecheck/test green; the Definition of Done in `openspec/config.yaml` is met; repo dependency/licensing rules honored. |

The dimensions are fixed; the *examples* in each row are illustrative, not a
checklist of this repo's features. If a dimension genuinely does not apply to a
change, the reviewer says so with a one-line reason — it may NOT silently skip a row.

## 6. Build the review prompt

Write `$SCRATCH/prompt.md`. It MUST: state the reviewer is independent and NOT the
builder; forbid file edits (read-only); hand it the rubric above and require it to
work every row by enumeration; and judge against the change's spec delta
(`openspec/changes/$CHANGE/specs`) + the Definition of Done in `openspec/config.yaml`.
Inputs to reference: `$SCRATCH/diff.patch`, the change folder. Require this exact
output shape:

```
VERDICT: PASS | PASS_WITH_FINDINGS | BLOCKED
RUBRIC:               # one line per row R1..R8: <id> OK | GAP | N/A — <evidence: the table you built, or the blank you found>
FINDINGS:
- [BLOCKER|HIGH|MEDIUM|LOW] <file:line or artifact> — <issue> — <required fix> — <rubric id>
MISSING_TESTS:        # scenario-ids with no `// @spec:` test, or "none"
CONTRACT_RISKS:       # DB/OpenAPI/proto/AsyncAPI/topic/cache-key/UI drift, or "none"
ARCHITECTURE_ADR_NEEDED:  # yes/no + reason
QUESTIONS:            # or "none"
```

A `RUBRIC:` line that asserts `OK` without showing the enumeration it built is itself
a `[BLOCKER]` finding — consolidation (step 8) MUST reject bare `OK`s.

## 7. Run each reviewer, capture output

```sh
export PROMPT="$(cat "$SCRATCH/prompt.md")"
for r in $reviewers; do ( cd "$SCRATCH"
  case "$r" in
    codex)  timeout "${REVIEW_TIMEOUT:-12m}" codex exec --sandbox read-only "$PROMPT" > "$SCRATCH/$r.md" 2>"$SCRATCH/$r.err";;
    agy)    timeout "${REVIEW_TIMEOUT:-12m}" agy -p "$PROMPT" --print-timeout 8m       > "$SCRATCH/$r.md" 2>"$SCRATCH/$r.err";;
    claude) timeout "${REVIEW_TIMEOUT:-12m}" claude -p --permission-mode plan "$PROMPT" > "$SCRATCH/$r.md" 2>"$SCRATCH/$r.err";;
  esac )
  { [ "$?" -ne 0 ] || [ ! -s "$SCRATCH/$r.md" ]; } && echo "[BLOCKER] reviewer unavailable — $r (non-zero exit, timeout, or empty output)" >> "$SCRATCH/$r.md"
done
```

## 8. Consolidate into the change folder

Write `$REVIEW_DIR/cross-review-$TS.md`: builder, reviewers, base, timestamp, the
diff stat, then each reviewer's full output, then a consolidated list of every
`VERDICT:` and `[BLOCKER]/[HIGH]` finding. Cross-check the `RUBRIC:` lines: any row
marked `OK` with no enumeration shown, or marked `GAP` by any reviewer, is escalated
to a `[BLOCKER]`. Print the path.

## 9. Verdict handling (human owns it)

- Any `[BLOCKER]`, a missing required-scenario test, a failed strict validation, or
  a contract-compat concern ⇒ the change **cannot be archived**.
- `[HIGH]` findings require explicit human acknowledgement before archive.
- `PASS`/`PASS_WITH_FINDINGS` is **not** auto-approval — surface findings to the user.

## Hooks
Run after `apply` and before `archive`; re-run on a PR if code changed since the last
review. `archive` is blocked unless a `reviews/cross-review-*.md` exists with blockers
resolved or explicitly accepted by a human.

## Failure modes / guardrails
- Only one other agent present → run with it; note reduced coverage.
- A reviewer times out or errors → record as `[BLOCKER] reviewer unavailable`, do not silently pass.
- A reviewer tries to modify files → discard, mark its review invalid (recipes are read-only by construction).
- Prompt too large → pass the spec delta + `diff.stat` + a focused diff of touched paths and tell the reviewer to inspect the repo read-only.
- Guard against reviewer self-correction loops: the prompt forbids fixing code and demands only a report + verdict; `agy` is always bounded by `--print-timeout`.

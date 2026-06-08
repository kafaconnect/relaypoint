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

## 5. Build the review prompt

Write `$SCRATCH/prompt.md`. It MUST: state the reviewer is independent and NOT the
builder; forbid file edits (read-only); and ask it to judge the diff against the
change's spec delta (`openspec/changes/$CHANGE/specs`) + the Definition of Done in
`openspec/config.yaml`. Inputs to reference: `$SCRATCH/diff.patch`, the change folder.
Require this exact output shape:

```
VERDICT: PASS | PASS_WITH_FINDINGS | BLOCKED
FINDINGS:
- [BLOCKER|HIGH|MEDIUM|LOW] <file:line or artifact> — <issue> — <required fix>
MISSING_TESTS:        # scenario-ids with no `// @spec:` test, or "none"
CONTRACT_RISKS:       # OpenAPI/proto/AsyncAPI compat, or "none"
ARCHITECTURE_ADR_NEEDED:  # yes/no + reason
QUESTIONS:            # or "none"
```

Checks the reviewer must cover: spec correctness (every behavior change has a
`### Requirement:` + `#### Scenario:` with a stable id); test traceability
(`// @spec:<id>`); DoD; code risk (regressions, concurrency, security, RLS,
NATS/gRPC/OpenAPI/migrations/rollback); and the 100%-OSS dependency rule.

## 6. Run each reviewer, capture output

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

## 7. Consolidate into the change folder

Write `$REVIEW_DIR/cross-review-$TS.md`: builder, reviewers, base, timestamp, the
diff stat, then each reviewer's full output, then a consolidated list of every
`VERDICT:` and `[BLOCKER]/[HIGH]` finding. Print the path.

## 8. Verdict handling (human owns it)

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

---
name: qa-verify
description: Produce an explicit GO / NO-GO quality gate for a Story/Task/Bug before its board item moves to Done and before a change is archived. Verifies that every in-scope OpenSpec scenario has a passing `// @spec:` test, plus a conditional Verification Matrix over every surface a change can touch — DB schema, API/gRPC/event contracts, cache keys, object storage, observability, tenancy — and UI fidelity against the in-repo prototype at docs/prototype.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
  github_cli: ">=2.45.0"
metadata:
  version: "1.0.0"
  owner: platform
  prototype_path: "docs/prototype"
  hooks: [before-board-done, before-archive]
  outputs:
    report_dir: "openspec/changes/<change>/qa"
---

# qa-verify

GO / NO-GO gate. A NO-GO blocks `Done` and `archive`. Partial evidence is NO-GO, never "inconclusive".

Each change is developed on its own branch (branch-per-change); `BASE="${BASE_REF:-origin/main}"`.

## 0. Tool preflight
```sh
for t in jq rg gh; do command -v "$t" >/dev/null || { echo "NO-GO: install $t"; exit 2; }; done
command -v openspec >/dev/null || command -v npx >/dev/null || { echo "NO-GO: install openspec (or npx)"; exit 2; }
```
Per-surface tools (oasdiff, buf, migrate, playwright) are checked at their surface: a
missing one degrades THAT surface's row to "SKIPPED (tool X missing)" — and a SKIPPED
required surface is NO-GO (never a silent pass).

## 1. Resolve issue + change
```sh
export REPO_ROOT="$(git rev-parse --show-toplevel)"; cd "$REPO_ROOT"
export REPO="${REPO:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"
export ISSUE="${ISSUE_NUMBER:?Story/Task/Bug issue number}"; export TS="$(date -u +%Y%m%dT%H%M%SZ)"
export WORK="$(mktemp -d -t oc-qa.XXXXXX)"
gh issue view "$ISSUE" -R "$REPO" --json number,title,body,labels,url > "$WORK/issue.json"
export CHANGE="$(jq -r '.labels[].name|select(startswith("openspec-change:"))|split(":")[1]' "$WORK/issue.json")"
[ -z "$CHANGE" -o "$CHANGE" = null ] && { echo "NO-GO: issue missing openspec-change:<change> label"; exit 2; }
export QA_DIR="openspec/changes/$CHANGE/qa"; mkdir -p "$QA_DIR"
```

## 2. Requirements verification (business logic)
```sh
openspec validate "$CHANGE" --strict
```
Determine the scenario ids in scope: prefer ids listed in the issue body, else the
`specs:` frontmatter of the task files under `openspec/changes/$CHANGE/tasks/`
(`grep -h "^specs:" tasks/*.md`), else the spec delta under
`openspec/changes/$CHANGE/specs`. A task with `status: done` whose Log carries no
evidence (commit hash / test command) is treated as NOT done — flag it.
If the mapping is ambiguous → ask the human; do NOT mark GO.

Every scenario id MUST have a tagged automated test, then run it:
```sh
export SCENARIOS="${SCENARIOS:?space-separated scenario ids}"
for sid in $SCENARIOS; do
  rg -nq "// @spec:${sid}\b" . || { echo "NO-GO: no test tagged // @spec:$sid"; exit 2; }
done
# Run the project's test/lint/typecheck (use scripts from `openspec instructions --json` if named):
go test ./... -race -coverprofile="$WORK/go.cover"
export COVERAGE_MIN="${COVERAGE_MIN:-0}"
COVER="$(go tool cover -func="$WORK/go.cover" | awk '/^total:/{sub(/%/,"",$3); print $3}')"
awk -v c="$COVER" -v m="$COVERAGE_MIN" 'BEGIN{exit !(c+0 >= m+0)}' || { echo "NO-GO: coverage $COVER% < COVERAGE_MIN $COVERAGE_MIN%"; exit 2; }
pnpm -r lint && pnpm -r typecheck && pnpm -r test
```
A failing scenario test, missing tag, or missing lint/typecheck/coverage command is NO-GO.

## 3. Verification Matrix (conditional — verify only the surfaces this change touched)
Detect the touched surfaces from the diff, the spec delta, and which contracts changed:
```sh
export BASE="${BASE_REF:-origin/main}"; git diff --name-only "$BASE"...HEAD > "$WORK/changed.txt"
hit(){ grep -Eq "$1" "$WORK/changed.txt"; }   # usage: hit 'pattern' && verify
```
For each surface that was touched, the NO-GO condition is binding.

| Surface | Touched when | Verify (command / check) | NO-GO if |
|---|---|---|---|
| **Business scenarios** | always | §2: each in-scope id has a passing `// @spec:<id>` test | any scenario test missing/failing |
| **DB schema** (PG17, golang-migrate, sqlc) | `hit 'db/migrations/\|sqlc'` | `migrate ... up && migrate ... down` apply clean; new tenant-owned table has `tenant_id` + RLS `ENABLE`/policy; indexes on hot paths; `sqlc generate` re-run, no diff | down fails; missing `tenant_id`/RLS; missing hot-path index; destructive/irreversible migration with no documented note; stale generated sqlc |
| **API contract** (OpenAPI 3) | `hit 'contracts/openapi'` | spec updated; `oapi-codegen` (Go) + `openapi-typescript` (web `packages/api-client`) re-run with no diff; `oasdiff breaking <base> <head>` clean; every operation has a handler; `openapi validate` ok | unintended breaking change; stale generated client; orphan operation; invalid schema/example |
| **gRPC / protobuf** (`contracts/proto`) | `hit 'contracts/proto'` | `buf lint` + `buf breaking --against "$BASE"` pass; stubs regenerated and committed; service/command signatures match | lint/breaking fail; stale stubs; signature mismatch |
| **NATS JetStream + events** (`contracts/asyncapi`) | `hit 'contracts/asyncapi'` | new/changed subjects declared; naming convention + tenant-scoped where needed; payload schema versioned; producer/consumer agree | undeclared subject; breaking event-shape change without a version bump; convention violation |
| **Cache keys** (Redis) | `hit 'redis\|cache'` | key naming + namespace; tenant-scoped (no cross-tenant collision); TTL set; invalidation path exists | unbounded/never-expiring key; cross-tenant key; no invalidation |
| **Object storage** (SeaweedFS / S3) | `hit 's3\|seaweed\|storage'` (media/uploads) | bucket/path/prefix convention; tenant-scoped prefixes; access scoped; content-type + size limits | cross-tenant prefix; unscoped access; no size/type limit on uploads |
| **UI fidelity** | UI files changed | §4 vs `docs/prototype` | mismatch / missing-ambiguous prototype mapping / unapproved drift |
| **Observability** | new code paths | slog logs + OTel spans/metrics per convention on new paths | new path with no logs/traces where convention requires them |
| **Security / tenancy** | always | RLS enforced (no cross-tenant read/write path); authz on new endpoints; no secrets logged | any cross-tenant path; unauthenticated/unauthorized endpoint; secret in logs |

## 4. UI fidelity vs the in-repo prototype (UI work only)
```sh
export PROTO="docs/prototype"
[ -d "$PROTO" ] || { echo "NO-GO: prototype path missing: $PROTO"; exit 2; }
```
Locate the reference screen/flow: prefer a `Prototype route:` in the issue body, else
a `UI flow:` line in the task file, else the screen named in the scenario text, else
search `docs/prototype`. If several screens match → ask the human (ambiguous = NO-GO).
`IMPL_ROUTE`: read `Impl route:` from the issue body, else default to `$PROTO_ROUTE`.
```sh
export IMPL_ROUTE="${IMPL_ROUTE:-$(jq -r '.body' "$WORK/issue.json" | sed -n 's/^Impl route:[[:space:]]*//p' | head -1)}"
[ -z "$IMPL_ROUTE" ] && export IMPL_ROUTE="$PROTO_ROUTE"
```

Start both servers, wait for readiness, and tear them down after:
```sh
( cd "$PROTO" && python3 -m http.server 4177 >/dev/null 2>&1 ) & PROTO_PID=$!
pnpm --filter "${APP:?web app workspace name}" dev >/dev/null 2>&1 & IMPL_PID=$!
trap 'kill "$PROTO_PID" "$IMPL_PID" 2>/dev/null' EXIT
for u in "http://127.0.0.1:4177" "http://127.0.0.1:5173"; do
  for _ in $(seq 1 60); do curl -fsS "$u" >/dev/null 2>&1 && break; sleep 1; done
done
```

For visual checks, screenshot both and diff (best-effort). Gate playwright behind
`command -v`; if unavailable, degrade to a DOM/text comparison + a recorded note — do
NOT auto-NO-GO solely because playwright is missing:
```sh
if command -v playwright >/dev/null || pnpm exec playwright --version >/dev/null 2>&1; then
  pnpm exec playwright screenshot "http://127.0.0.1:4177$PROTO_ROUTE" "$WORK/proto.png"
  pnpm exec playwright screenshot "http://127.0.0.1:5173$IMPL_ROUTE"  "$WORK/impl.png"
else
  curl -fsS "http://127.0.0.1:4177$PROTO_ROUTE" > "$WORK/proto.html"
  curl -fsS "http://127.0.0.1:5173$IMPL_ROUTE"  > "$WORK/impl.html"
  echo "note: playwright unavailable — degraded to DOM/text comparison" >> "$WORK/ui-note.txt"
fi
```
Judge **behavior + structure, not pixel-perfection** (avoids false NO-GO): same
primary workflow steps, same visible state transitions, same empty/loading/error
states when specified, same critical labels/controls/hierarchy, shadcn/Tailwind/Geist
styling consistent, no overlapping text or broken responsive layout. A divergence is
acceptable ONLY if OpenSpec or the issue explicitly approved it. Subtle pixel nits are
left to human eyes in `cross-review`, not auto-failed here.

## 5. Verdict report → change folder + issue
Write `$QA_DIR/issue-$ISSUE-qa-$TS.md`: change, scenario ids, the touched-surface rows
from §3 with evidence (validation, spec tags, test logs, migrate up/down, oasdiff /
buf breaking output, screenshots, ui-diff), then `VERDICT: GO | NO-GO` and a Blockers
list. Post it on the issue: `gh issue comment "$ISSUE" -R "$REPO" -F "$REPORT"`.

## 6. Board gate
- `GO` → allows board Status **Done**.
- `NO-GO` → keep in **Review** (or **Blocked** if that option exists).
- Blocks Done: any failing/absent scenario test, failed strict validation, any
  triggered Verification Matrix NO-GO, UI mismatch, missing/ambiguous prototype
  mapping, or unapproved design drift.

## Hooks
Run after `apply` + PR review, before the board item moves to **Done**, and before
`archive`. `archive` is blocked unless every linked Story/Task/Bug has a `GO` report
in `openspec/changes/<change>/qa/`.

## Failure modes / guardrails
- Prototype path absent or app server won't start → NO-GO (record which).
- Ambiguous prototype route → ask the human; never infer.
- Flaky test → rerun once; a second failure is NO-GO.
- Partial evidence is NO-GO, never "inconclusive" — an unverifiable touched surface fails.
- High pixel diff but behavior approved → require an explicit approval note in the issue/OpenSpec; judge behavior, not pixels; do not silently pass.

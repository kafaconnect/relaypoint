---
name: change-planning
description: Human-in-the-loop planning for an OpenSpec change, then one-way sync to the GitHub Projects v2 board. FORCES alignment on gray areas, design, and test strategy with the user BEFORE finalizing artifacts; then creates the Epic/Story/Task/Bug issues, sets Iteration + Release Train, and assigns. Includes a board-sync mode that updates issue status as work proceeds. Use when starting a new change or implementing tasks.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
  github_cli: ">=2.45.0"
  agents: [claude-code, codex]
metadata:
  version: "1.0.0"
  owner: platform
  modes: [plan, board-sync]
  board: { owner: kafaconnect, repo: kafaconnect/desk, project_type: org-project-v2 }
---

# change-planning

OpenSpec owns **definition**; the GitHub board owns **delivery state**. Sync is
**one-way** (OpenSpec → board). Never copy spec text into issues — link to the files.

## Decomposition principles (Story / Task / Bug)

Decompose every change the same way. Start from the OpenSpec delta; the spec is the
source of truth, and the Definition of Done (DoD) is the gate.

**Taxonomy**
- **Epic** = exactly one OpenSpec change (one capability slice). One GitHub issue; `tasks.md` is one-way synced into it as a tasklist.
- **Story** = a user-visible, vertically-sliced behavior that delivers value on its own. Maps to one or more `### Requirement:` / `#### Scenario:`. Demoable against the prototype.
- **Task** = an executable unit inside a story (few hours to ~1 day): migration, contract change, handler, UI screen, test. A checkbox in `tasks.md`; promoted to its own issue ONLY when it needs a different owner.
- **Bug** = a defect against an already-accepted spec.

**Principles**
- **Slice vertically, not horizontally** — a story crosses DB → API → UI as needed so it ships and demos. Never "do all the backend" as a story.
- **Stories are INVEST** (Independent, Negotiable, Valuable, Estimable, Small, Testable).
- **Acceptance is mechanical**: story done = its scenario ids pass (`// @spec:<id>` tests) + DoD met.
- **One outcome per story**; if a story has >~5 tasks or >1 deployable's worth of risk, split it.
- **Every task names its scenario id(s)**; a task advancing no scenario is suspect (chore/infra excepted).
- **Avoid issue explosion** — tasks stay as the Epic's tasklist by default; promote to a standalone issue only when assignable to a different person.

**Sequence**: derive stories from the delta's requirements/scenarios → list tasks per
story → set each story's acceptance = its scenario ids + DoD.

## Mode: plan

### 1. Force human alignment BEFORE writing artifacts
Present a short planning brief and STOP for the user. Cover: gray areas, design
choices, test strategy, contract impact, data/migration/RLS impact, UI/prototype
impact, open questions, and the proposed Story/Task/Bug breakdown. Use
**AskUserQuestion**. Do NOT finalize `proposal.md` / `design.md` / `tasks.md` /
spec deltas until the user has answered. Required questions:
1. What behavior is in scope (and explicitly out)?
2. How to resolve each unresolved design choice?
3. Which Release Train, Iteration, and assignees?
4. Which prototype screens/flows under `docs/prototype` must this match?

### 2. Create the change + artifacts (only after alignment)
```sh
export CHANGE="${CHANGE:?aligned change id}"; export TITLE="${TITLE:?human title}"
openspec new change "$CHANGE"
```
Generate `proposal.md`, `design.md` (if architecture/contract/storage/security changed),
`tasks.md`, and `specs/<capability>/spec.md`. Every behavior change gets a
`### Requirement:` + `#### Scenario:` with a stable id; every task names its expected
scenario ids. Then: `openspec validate "$CHANGE" --strict`.

### Precondition: confirm the board exists (before any issue creation)
Verify the board BEFORE resolving ids or creating any issue: `PROJECT_NUMBER` set AND
`gh project view` succeeds AND required fields present. If not, STOP and tell the user
to run the `board-bootstrap` skill first. Create NO issue before the board is confirmed.
```sh
export ORG=kafaconnect
export PROJECT_NUMBER="${PROJECT_NUMBER:?run board-bootstrap first — PROJECT_NUMBER unset}"
gh project view "$PROJECT_NUMBER" --owner "$ORG" --format json >/dev/null 2>&1 \
  || { echo "board not found — run the board-bootstrap skill first"; exit 2; }
gh project field-list "$PROJECT_NUMBER" --owner "$ORG" --format json --jq '[.fields[].name]' \
  | grep -q '"Status"' && gh project field-list "$PROJECT_NUMBER" --owner "$ORG" --format json --jq '[.fields[].name]' | grep -q '"Iteration"' \
  || { echo "board missing required fields — run the board-bootstrap skill first"; exit 2; }
```

### 3. Resolve board ids (ask for PROJECT_NUMBER; do not infer)
```sh
export ORG=kafaconnect REPO=kafaconnect/desk
export PROJECT_NUMBER="${PROJECT_NUMBER:?org Projects v2 number}"
export RELEASE_TRAIN="${RELEASE_TRAIN:?e.g. M1}"  export ITERATION_TITLE="${ITERATION_TITLE:?}"
export PROJECT_ID="$(gh project view "$PROJECT_NUMBER" --owner "$ORG" --format json --jq '.id')"
gh project field-list "$PROJECT_NUMBER" --owner "$ORG" --format json > "$(mktemp)"/f.json   # capture path
# from f.json extract: Status field id + option ids (Ready/In Progress/Review/Done),
# Iteration field id + iteration id (by title), Release-Train field id + option id.
```

### 4. Ensure the milestone, create the Epic (one Epic per change)
```sh
gh api "repos/$REPO/milestones" --jq ".[]|select(.title==\"$RELEASE_TRAIN\")|.number" | grep -q . \
  || gh api "repos/$REPO/milestones" -f title="$RELEASE_TRAIN"

EPIC_URL="$(gh issue create -R "$REPO" -t "[Epic] $CHANGE: $TITLE" \
  -b "OpenSpec change: \`$CHANGE\`. Source of truth: openspec/changes/$CHANGE/. Board = state only; change scope in OpenSpec first." \
  -l type:epic -l "openspec-change:$CHANGE" -m "$RELEASE_TRAIN")"
EPIC_NO="${EPIC_URL##*/}"
EPIC_ITEM="$(gh project item-add "$PROJECT_NUMBER" --owner "$ORG" --url "$EPIC_URL" --format json --jq '.id')"
# gh project item-edit ... set Status=Ready, Iteration, Release Train (see step 6 for the field-set call)
```

### 5. Create Story/Task/Bug issues from tasks.md, write the numbers back
For each task line create ONE issue (`type:story|task|bug` + `openspec-change:$CHANGE`
labels, milestone, assignee), add it to the project, set Status=Ready + Iteration +
Release Train, then rewrite the task line in `tasks.md` to `- [ ] #<n> <summary>`.
The issue body links to `openspec/changes/$CHANGE/tasks.md` + names its scenario ids;
it does NOT duplicate the spec. Comment a checklist line on the Epic.

### 6. Field-set helper (gh CLI, with GraphQL fallback)
```sh
gh project item-edit --id "$ITEM" --project-id "$PROJECT_ID" \
  --field-id "$STATUS_FIELD" --single-select-option-id "$STATUS_READY"
gh project item-edit --id "$ITEM" --project-id "$PROJECT_ID" \
  --field-id "$ITER_FIELD" --iteration-id "$ITER_ID"
gh issue edit "$N" -R "$REPO" --milestone "$RELEASE_TRAIN" --add-assignee "$ASSIGNEE"
```
GraphQL fallback when `gh project` can't set a value:
`updateProjectV2ItemFieldValue(input:{projectId,itemId,fieldId,value:{singleSelectOptionId|iterationId}})`.

## Mode: board-sync (during apply / implementation)
Trigger points → Status transition (resolve `$ITEM` via `gh project item-list … --jq '.items[]|select(.content.number==N)|.id'`):
- Start a task → assign `@me`, Status **In Progress**.
- Open a PR → Status **Review**, comment the PR URL on the issue.
- CI green **and** `qa-verify` GO → comment "ready to merge".
- Merge → Status **Done**, close the issue, tick `- [x] #<n>` in `tasks.md`.

## Hooks
explore → **change-planning(plan)** → propose (finalize artifacts) → board-link (ensure
task↔issue links) → apply (board-sync: In Progress) → PR (Review) → qa-verify GO +
CI green (Done) → archive (only after all linked tasks checked off).

## Failure modes / guardrails
- Missing `PROJECT_NUMBER`/field/option → ask the user; STOP before a partial sync.
- Issue created but project update failed → comment `board-sync:partial` on the issue; retry from the issue number (idempotent).
- A task already has `#<n>` in tasks.md → reuse it; never create a duplicate.
- Scope changed on the board (not in OpenSpec) → reject; update OpenSpec first (one-way rule).
- GitHub rate limit → check `gh api rate_limit`, retry once, else record unsynced items and continue (file generation must not be blocked by the network).

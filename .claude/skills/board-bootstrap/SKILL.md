---
name: board-bootstrap
description: One-time, idempotent setup of the org GitHub Projects v2 board for kafaconnect/desk — creates (or detects) the Project, its custom fields with single-select options, and the repo/org labels that change-planning consumes. Check-then-create everywhere; safe to re-run. Run once before the first change-planning sync, then record PROJECT_NUMBER.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
  github_cli: ">=2.45.0"
metadata:
  version: "1.0.0"
  owner: platform
  board: { owner: kafaconnect, repo: kafaconnect/desk, project_type: org-project-v2 }
---

# board-bootstrap

One-time provisioning of the delivery board. **Idempotent**: every step is
check-then-create, so re-running converges instead of duplicating. The board owns
delivery state; OpenSpec owns definition. This skill only builds the board shell —
`change-planning` fills it with Epics/Stories/Tasks.

## 0. Preconditions

```sh
export ORG=kafaconnect REPO=kafaconnect/desk
gh auth status >/dev/null || { echo "run: gh auth login (scopes: project, read:org, repo)"; exit 2; }
gh auth refresh -s project -s read:org >/dev/null 2>&1 || true
```

## 1. Create or detect the org Project v2

```sh
export PROJECT_TITLE="kafaconnect/desk delivery"
PROJECT_NUMBER="$(gh project list --owner "$ORG" --format json \
  --jq ".projects[]|select(.title==\"$PROJECT_TITLE\")|.number" | head -1)"
[ -z "$PROJECT_NUMBER" ] && PROJECT_NUMBER="$(gh project create --owner "$ORG" \
  --title "$PROJECT_TITLE" --format json --jq '.number')"
export PROJECT_NUMBER
export PROJECT_ID="$(gh project view "$PROJECT_NUMBER" --owner "$ORG" --format json --jq '.id')"
echo "PROJECT_NUMBER=$PROJECT_NUMBER"
echo "PROJECT_ID=$PROJECT_ID"
```

## 2. Fields

Snapshot the existing fields once; the helpers below read it to decide create-vs-skip.

```sh
export FJSON="$(mktemp -t oc-fields.XXXXXX.json)"
refresh_fields(){ gh project field-list "$PROJECT_NUMBER" --owner "$ORG" --format json > "$FJSON"; }
field_id(){ jq -r --arg n "$1" '.fields[]|select(.name==$n)|.id' "$FJSON"; }
field_exists(){ [ -n "$(field_id "$1")" ]; }
refresh_fields
```

`gh project field-create` makes plain TEXT/ITERATION fields but **cannot** create
single-select options — those need the `createProjectV2Field` GraphQL mutation. Use
the right path per field type:

```sh
# TEXT — OpenSpec Change
field_exists "OpenSpec Change" || gh project field-create "$PROJECT_NUMBER" --owner "$ORG" \
  --name "OpenSpec Change" --data-type TEXT

# ITERATION — Iteration
field_exists "Iteration" || gh project field-create "$PROJECT_NUMBER" --owner "$ORG" \
  --name "Iteration" --data-type ITERATION

# SINGLE_SELECT (incl. options) — GraphQL, since the CLI can't seed options.
create_single_select(){            # $1=field name, $2..=options
  local name="$1"; shift
  field_exists "$name" && return 0
  local opts="" o
  for o in "$@"; do opts="$opts{name:\"$o\",color:GRAY,description:\"\"},"; done
  gh api graphql -f query='
    mutation($pid:ID!,$name:String!,$opts:[ProjectV2SingleSelectFieldOptionInput!]!){
      createProjectV2Field(input:{projectId:$pid,dataType:SINGLE_SELECT,name:$name,singleSelectOptions:$opts}){
        projectV2Field{ ... on ProjectV2SingleSelectField { id name } } } }' \
    -f pid="$PROJECT_ID" -f name="$name" -F opts="[$opts]" >/dev/null
}

create_single_select "Status"        Backlog "Spec Review" Ready "In Progress" Review Blocked Done Archived
create_single_select "Release Train" M1 M2 M3
create_single_select "Capability"    inbox identity channels campaigns automation integrations sysadmin
create_single_select "Risk"          Low Medium High
refresh_fields
```

Newly-created Projects ship with a default **Status** field (Todo/In Progress/Done).
If `field_exists "Status"` was already true, reconcile its options to the eight above
with `updateProjectV2Field` instead of creating a second field — append the missing
options by their option-input list (the mutation replaces the option set, so pass the
full desired list, reusing existing option ids where present):

```sh
# Only if Status pre-existed with the wrong options. STATUS_FIELD_ID from field_id "Status".
gh api graphql -f query='
  mutation($fid:ID!,$opts:[ProjectV2SingleSelectFieldOptionInput!]!){
    updateProjectV2Field(input:{fieldId:$fid,singleSelectOptions:$opts}){
      projectV2Field{ ... on ProjectV2SingleSelectField { id options { id name } } } } }' \
  -f fid="$(field_id Status)" -F opts='[{name:"Backlog",color:GRAY,description:""},{name:"Spec Review",color:GRAY,description:""},{name:"Ready",color:GRAY,description:""},{name:"In Progress",color:GRAY,description:""},{name:"Review",color:GRAY,description:""},{name:"Blocked",color:GRAY,description:""},{name:"Done",color:GRAY,description:""},{name:"Archived",color:GRAY,description:""}]' >/dev/null
```

Verify all six fields resolved before moving on:

```sh
for f in "Status" "Iteration" "Release Train" "Capability" "Risk" "OpenSpec Change"; do
  field_exists "$f" || { echo "field missing after create: $f"; exit 2; }
done
```

## 3. Labels (repo-scoped, `--force` makes it idempotent)

`gh label create --force` updates an existing label instead of erroring, so the whole
block is safe to re-run.

```sh
mklabel(){ gh label create "$1" -R "$REPO" --color "$2" --description "$3" --force >/dev/null; }

mklabel type:epic  3E4B9E "OpenSpec change (one capability slice)"
mklabel type:story 1D76DB "User-visible vertical slice"
mklabel type:task  0E8A16 "Executable unit inside a story"
mklabel type:bug   D73A4A "Defect against an accepted spec"
mklabel type:adr   5319E7 "Architecture decision record"
mklabel ci:blocker B60205 "Blocks CI / merge"
mklabel risk:tenant-isolation B60205 "Touches cross-tenant isolation / RLS"

for d in inbox identity channels campaigns automation integrations sysadmin; do
  mklabel "domain:$d" C5DEF5 "Capability domain: $d"
done
for c in zalo whatsapp messenger shopee email; do
  mklabel "channel:$c" FEF2C0 "Channel: $c"
done
```

## 4. Record PROJECT_NUMBER so change-planning can consume it

`change-planning` asks for `PROJECT_NUMBER` and refuses to infer it. Persist it as a
variable so the value is shared, not re-discovered:

```sh
gh variable set PROJECT_NUMBER -R "$REPO" --body "$PROJECT_NUMBER"   # repo scope, or:
gh variable set PROJECT_NUMBER --org "$ORG" --visibility all --body "$PROJECT_NUMBER"
```

Then tell the user: the board is provisioned; **record `PROJECT_NUMBER=<n>`** (and
`PROJECT_ID`) — `change-planning` reads `PROJECT_NUMBER` to attach issues. Print both
values one last time.

## Hooks
Run **once** at project setup, before the first `change-planning(plan)` sync. Re-run
only to add a new field option, label, or to repair drift — every step converges.

## Failure modes / guardrails
- Missing `project`/`read:org` scope → `gh auth refresh -s project -s read:org`; STOP.
- A single-select field already exists with wrong options → reconcile via
  `updateProjectV2Field` (pass the full desired option list); never create a duplicate field.
- Two projects share the title → `gh project list` returns both; pick the lowest number
  (`head -1` above) and delete the stray manually — do not auto-delete.
- GraphQL rate limit → check `gh api rate_limit`, retry once, else stop and report what was created.
- The CLI cannot seed single-select options — always use the GraphQL mutation for those fields.

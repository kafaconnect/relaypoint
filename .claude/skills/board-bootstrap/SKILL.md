---
name: board-bootstrap
description: One-time, idempotent setup of the org GitHub Projects v2 board for the current repo. Creates or detects the Project, required custom fields/options, repo labels, and writes .github/project.yml for change-planning/board-sync. Check-then-create everywhere; safe to re-run before the first board sync.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
  github_cli: ">=2.45.0"
metadata:
  version: "1.1.0"
  owner: platform
  board: { source: "current git remote", project_type: org-project-v2 }
---

# board-bootstrap

One-time provisioning of the delivery board. Idempotent: every step is
check-then-create. The board owns delivery state; OpenSpec owns definition.

## 0. Preconditions

Resolve the org/repo from the current repository; do not hard-code `desk` or
`relaypoint`.

```sh
export REPO="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
export ORG="${REPO%%/*}"
gh auth status >/dev/null || { echo "run: gh auth login (scopes: project, read:org, repo)"; exit 2; }
gh auth refresh -s project -s read:org -s repo >/dev/null 2>&1 || true
```

## 1. Create or detect the org Project v2

```sh
export PROJECT_TITLE="$REPO delivery"
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

Snapshot fields, then create/reconcile the required set.

```sh
export FJSON="$(mktemp -t oc-fields.XXXXXX.json)"
refresh_fields(){ gh project field-list "$PROJECT_NUMBER" --owner "$ORG" --format json > "$FJSON"; }
field_id(){ jq -r --arg n "$1" '.fields[]|select(.name==$n)|.id' "$FJSON"; }
field_exists(){ [ -n "$(field_id "$1")" ]; }
refresh_fields
```

`gh project field-create` can create TEXT and ITERATION. Use GraphQL for
SINGLE_SELECT options.

```sh
field_exists "OpenSpec Change" || gh project field-create "$PROJECT_NUMBER" --owner "$ORG" \
  --name "OpenSpec Change" --data-type TEXT

field_exists "Iteration" || gh project field-create "$PROJECT_NUMBER" --owner "$ORG" \
  --name "Iteration" --data-type ITERATION

create_single_select(){
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
create_single_select "Mstone"        M1 M2 M3
create_single_select "Capability"    inbox identity channels campaigns automation integrations sysadmin
create_single_select "Risk"          Low Medium High
refresh_fields
```

If a default Status field already exists with `Todo/In Progress/Done`, reconcile it to
the desired option set with `updateProjectV2Field`; do not create a second Status field.

Verify before continuing:

```sh
for f in "Status" "Iteration" "Release Train" "Mstone" "Capability" "Risk" "OpenSpec Change"; do
  field_exists "$f" || { echo "field missing after create: $f"; exit 2; }
done
```

## 3. Labels

```sh
mklabel(){ gh label create "$1" -R "$REPO" --color "$2" --description "$3" --force >/dev/null; }

mklabel type:epic  3E4B9E "OpenSpec change"
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

## 4. Write `.github/project.yml`

`change-planning` and `sync_board.py` read this file first. Write it after the board
exists so future no-context sessions do not ask for Project ids.

```sh
mkdir -p .github
gh project field-list "$PROJECT_NUMBER" --owner "$ORG" --format json > "$FJSON"
cat > .github/project.yml <<EOF
# Shared project coordinates - committed, non-secret.
github:
  org: $ORG
  repo: $REPO
project:
  number: $PROJECT_NUMBER
  id: $PROJECT_ID
  url: https://github.com/orgs/$ORG/projects/$PROJECT_NUMBER
EOF
```

Then append field ids/options from `$FJSON` if needed, or rerun `board-bootstrap` with
an existing project and update the file. At minimum, `github.*`, `project.number`, and
`project.id` must be present.

Optionally persist `PROJECT_NUMBER`:

```sh
gh variable set PROJECT_NUMBER -R "$REPO" --body "$PROJECT_NUMBER"
```

## Hooks

Run once before the first `change-planning` board-link. Re-run only to add options,
labels, or repair drift.

## Guardrails

- Missing `project`/`read:org`/`repo` scope -> refresh auth and STOP.
- A single-select field already exists with wrong options -> reconcile it; never create
  duplicates.
- Two Projects share the title -> choose explicitly; do not auto-delete.
- GraphQL rate limit -> check `gh api rate_limit`, stop with created resources listed.

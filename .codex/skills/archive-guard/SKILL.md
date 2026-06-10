---
name: archive-guard
description: Pre-archive gate for an OpenSpec change — verifies a cross-review exists with no unresolved [BLOCKER], and that every linked Story/Task/Bug issue has a GO report under the change's qa/ folder. BLOCK with precise reasons on any failure; otherwise state archive is allowed. The built-in /opsx:archive does NOT call this — a human or agent must run it first.
license: Proprietary
compatibility:
  openspec: ">=1.4.1"
  github_cli: ">=2.45.0"
metadata:
  version: "1.0.0"
  owner: platform
  hooks: [before-archive]
  inputs:
    review_dir: "openspec/changes/<change>/reviews"
    qa_dir: "openspec/changes/<change>/qa"
---

# archive-guard

Last gate before `/opsx:archive`. **The built-in archive command does NOT auto-call
this** — a human or agent must run `archive-guard` first and only proceed to archive
on an explicit ALLOW. The guard is read-only: it blocks, it never archives.

A change may be archived only when BOTH hold:
1. a `reviews/cross-review-*.md` exists and carries **no unresolved `[BLOCKER]`**, and
2. **every** linked Story/Task/Bug issue has a `GO` report under `qa/`.

Any failure ⇒ **BLOCK** and print the precise reason(s). Partial/ambiguous ⇒ BLOCK,
never "probably fine".

## 1. Resolve the change

```sh
export REPO_ROOT="$(git rev-parse --show-toplevel)"; cd "$REPO_ROOT"
export REPO="${REPO:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"
export CHANGE="${CHANGE:?openspec change id, e.g. m1-unified-inbox}"
export CHDIR="openspec/changes/$CHANGE"
[ -d "$CHDIR" ] || { echo "BLOCK: no such change folder: $CHDIR"; exit 2; }
export REVIEW_DIR="$CHDIR/reviews" QA_DIR="$CHDIR/qa"
export BLOCKS="$(mktemp -t oc-archguard.XXXXXX)"; : > "$BLOCKS"
```

## 2. Cross-review present and blocker-free

```sh
latest="$(ls -1t "$REVIEW_DIR"/cross-review-*.md 2>/dev/null | head -1)"
if [ -z "$latest" ]; then
  echo "BLOCK: no cross-review under $REVIEW_DIR/cross-review-*.md — run /opsx:cross-review" >> "$BLOCKS"
else
  # An unresolved blocker is a [BLOCKER] line not marked resolved/accepted on the same line.
  if grep -nE '\[BLOCKER\]' "$latest" | grep -vEi 'resolved|accepted|waived' | grep -q .; then
    echo "BLOCK: unresolved [BLOCKER] in $latest:" >> "$BLOCKS"
    grep -nE '\[BLOCKER\]' "$latest" | grep -vEi 'resolved|accepted|waived' >> "$BLOCKS"
  fi
fi
```

## 3. Enumerate linked Story/Task/Bug issues

Two sources, unioned: `issue:` frontmatter in `tasks/*.md` (legacy: `#<n>` refs in tasks.md), and issues carrying the
`openspec-change:<change>` label.

```sh
from_tasks="$( { grep -hoE '^issue: *[0-9]+' "$CHDIR"/tasks/*.md 2>/dev/null | grep -oE '[0-9]+'; grep -oE '#[0-9]+' "$CHDIR/tasks.md" 2>/dev/null | tr -d '#'; } | sort -u)"
from_label="$(gh issue list -R "$REPO" --label "openspec-change:$CHANGE" --state all \
  --json number --jq '.[].number' 2>/dev/null | sort -u)"
LINKED="$(printf '%s\n%s\n' "$from_tasks" "$from_label" | grep -E '^[0-9]+$' | sort -un)"
[ -z "$LINKED" ] && echo "BLOCK: no linked Story/Task/Bug issues found (task-file issue: frontmatter, tasks.md #refs, or openspec-change:$CHANGE label)" >> "$BLOCKS"
```

Drop the Epic itself from the GO requirement — it is the container, not a deliverable:

```sh
keep=""
for n in $LINKED; do
  labels="$(gh issue view "$n" -R "$REPO" --json labels --jq '[.labels[].name]|join(" ")' 2>/dev/null)"
  case "$labels" in *type:epic*) continue;; esac
  keep="$keep $n"
done
LINKED="$keep"
```

## 4. Every linked issue has a GO report

A report is any `qa/*.md` for that issue whose verdict line is `GO` (not `NO-GO`).
`qa-verify` writes `qa/issue-<n>-qa-<ts>.md` with `VERDICT: GO | NO-GO`.

```sh
for n in $LINKED; do
  rpt="$(ls -1t "$QA_DIR"/issue-"$n"-qa-*.md 2>/dev/null | head -1)"
  if [ -z "$rpt" ]; then
    echo "BLOCK: issue #$n has no QA report under $QA_DIR/issue-$n-qa-*.md" >> "$BLOCKS"
  elif ! grep -qE '^VERDICT:[[:space:]]*GO[[:space:]]*$' "$rpt"; then
    echo "BLOCK: issue #$n latest QA report is not GO: $rpt" >> "$BLOCKS"
  fi
done
```

## 5. Verdict

```sh
if [ -s "$BLOCKS" ]; then
  echo "ARCHIVE BLOCKED for $CHANGE — fix the following, then re-run archive-guard:"
  cat "$BLOCKS"
  exit 2
fi
echo "ARCHIVE ALLOWED for $CHANGE: cross-review is blocker-free and every linked issue has a GO. You may now run /opsx:archive $CHANGE."
```

## Hooks
Run immediately before `/opsx:archive`, after `cross-review` and `qa-verify` have
recorded their artifacts. Not auto-invoked by the archive command — gate manually.

## Failure modes / guardrails
- `gh` unauthenticated / rate-limited → the label query returns nothing; do NOT treat
  empty as "no issues" silently — surface the `gh` error and BLOCK.
- A `[BLOCKER]` marked `resolved`/`accepted`/`waived` on its own line is treated as
  cleared; anything ambiguous stays a blocker.
- Multiple QA reports for one issue → newest by mtime wins; an older GO does not
  override a newer NO-GO.
- A linked `#<n>` that is actually a PR, not an issue → `gh issue view` errors; record
  it as a block rather than skipping.
- This guard never writes or archives — on ALLOW, the human/agent runs `/opsx:archive`.

#!/usr/bin/env bash
# Regenerate openspec/changes/<id>/tasks.md from the per-task files' frontmatter.
# Frontmatter is the single source of truth (docs/conventions.md "Tasks & progress");
# the index exists only so the openspec CLI's checkbox counting keeps working.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

id="${1:?usage: tasks-index.sh <change-id>}"
dir="openspec/changes/$id"
[ -d "$dir/tasks" ] || { echo "no $dir/tasks" >&2; exit 1; }

fm() { # $1=file $2=key — first frontmatter value
  awk -v k="$2" 'NR==1&&$0!="---"{exit} /^---$/{c++; next} c==1 && $1==k":" {sub("^"k": *",""); print; exit}' "$1"
}

{
  echo "# Tasks — $id"
  echo
  echo "> GENERATED from tasks/*.md frontmatter by scripts/tasks-index.sh — do not edit."
  slice=""
  for f in $(ls "$dir"/tasks/*.md | sort); do
    s="$(fm "$f" slice)"; t="$(fm "$f" title)"; st="$(fm "$f" status)"; tid="$(fm "$f" id)"
    if [ "$s" != "$slice" ]; then slice="$s"; echo; echo "## $slice"; echo; fi
    box=" "; [ "$st" = "done" ] && box="x"
    suffix=""; [ "$st" = "in_progress" ] && suffix=" — IN PROGRESS"
    [ "$st" = "blocked" ] && suffix=" — BLOCKED"
    echo "- [$box] $tid — $t ([tasks/$(basename "$f")](tasks/$(basename "$f")))$suffix"
  done
} > "$dir/tasks.md"
echo "regenerated $dir/tasks.md"

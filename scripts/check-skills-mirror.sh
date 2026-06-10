#!/usr/bin/env bash
# .codex/skills is a hand-synced copy of .claude/skills (codex reads its own tree).
# Any edit to a .claude skill that isn't mirrored leaves codex following stale process —
# this is the drift gate. Fails listing every diverged or missing skill.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

fail=0
for src in .claude/skills/*/SKILL.md; do
  name="$(basename "$(dirname "$src")")"
  dst=".codex/skills/$name/SKILL.md"
  if [ ! -f "$dst" ]; then
    echo "MISSING in .codex: $name"; fail=1; continue
  fi
  if ! diff -q "$src" "$dst" >/dev/null; then
    echo "DRIFT: $name (.claude vs .codex differ)"; fail=1
  fi
done
for dst in .codex/skills/*/SKILL.md; do
  name="$(basename "$(dirname "$dst")")"
  [ -f ".claude/skills/$name/SKILL.md" ] || { echo "ORPHAN in .codex: $name"; fail=1; }
done

[ "$fail" -eq 0 ] && echo "skills mirror: OK" || { echo "skills mirror: DRIFTED — run: for s in .claude/skills/*/; do n=\$(basename \"\$s\"); cp \".claude/skills/\$n/SKILL.md\" \".codex/skills/\$n/SKILL.md\"; done"; exit 1; }

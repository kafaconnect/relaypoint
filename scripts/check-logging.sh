#!/usr/bin/env bash
# @spec:obs.no-adhoc-logging
# Fails if production Go code logs via fmt.Print*/println/log.Print* instead of the shared
# obs/slog setup (AGENTS.md Logging hard rule; ADR-0011 §6). Scope: internal/, cmd/ —
# excluding _test.go (relaypoint has no services/ tree; otherwise byte-identical to desk's
# gate). A genuine program-output line is allowlisted with a trailing `// program-output`
# marker (e.g. a cmd emitting env values for shell capture).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Ad-hoc logging: fmt.Print*, println(, log.Print*, and fmt.Fprint* aimed at os.Stdout
# (Fprint* to os.Stderr stays allowed — CLI fatal diagnostics). console.* is JS/web logging,
# enforced separately by the web (client-ingest) slice, not this Go gate.
pattern='(fmt\.Print(ln|f)?\(|(^|[^.[:alnum:]])println\(|log\.Print(ln|f)?\(|fmt\.Fprint(ln|f)?\([[:space:]]*os\.Stdout)'

# Allowlist: a matched line is exempt ONLY with an explicit trailing `// program-output`
# comment (visible in review), not by the string appearing anywhere.
hits="$(grep -REn "$pattern" internal cmd \
  --include='*.go' \
  | grep -v '_test\.go:' \
  | grep -vE '//[[:space:]]*program-output' || true)"

if [ -n "$hits" ]; then
  echo "::error::ad-hoc logging is banned (use internal/obs / slog) — ADR-0011 §6:"
  echo "$hits"
  exit 1
fi
echo "no ad-hoc logging found."

#!/usr/bin/env sh
# Smoke-check the RelayPoint signaling plane via the NATS monitoring port. Exits non-zero on the
# first failed check so it is usable in CI / a post-`up` gate. Override the endpoint with MON_URL.
#   ./deploy/verify.sh            # checks http://127.0.0.1:8222
#   MON_URL=http://host:8222 ./deploy/verify.sh
set -eu

MON_URL="${MON_URL:-http://127.0.0.1:8222}"
STREAM="${STREAM:-INTERACTION_LOGS}"
ROUTER_NAME="${ROUTER_NAME:-relaypoint-router}"

fetch() {
  if command -v curl >/dev/null 2>&1; then curl -fsS "$1"; else wget -qO- "$1"; fi
}

check() { # <label> <url> <substring-that-must-be-present>
  printf '  %-28s' "$1"
  body="$(fetch "$2" 2>/dev/null || true)"
  if printf '%s' "$body" | grep -q "$3"; then
    echo "OK"
  else
    echo "FAIL"
    echo "    expected to find '$3' at $2" >&2
    return 1
  fi
}

echo "Verifying RelayPoint plane at $MON_URL"
rc=0
check "NATS healthy"        "$MON_URL/healthz"           '"status":"ok"'   || rc=1
check "JetStream enabled"   "$MON_URL/jsz"               '"memory"'        || rc=1
check "stream $STREAM"      "$MON_URL/jsz?streams=1"     "$STREAM"         || rc=1
check "router connected"    "$MON_URL/connz?auth=1"      "$ROUTER_NAME"    || rc=1

if [ "$rc" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks failed." >&2
fi
exit "$rc"

#!/usr/bin/env bash
# ADR-0002 protobuf cutover: delete + recreate the INTERACTION_LOGS JetStream stream so no
# JSON-era fact survives into the protobuf router (it would fail proto.Unmarshal and brick the
# interaction). Destructive dev reset — there is no production history to retain.
#
# Run ONCE against the target NATS before starting the protobuf router, OR start the router with
# RP_RESET_LOG_STREAM=1 for the first boot (cmd/router/main.go runs signaling.ResetLogStream).
#
#   NATS_URL=nats://router:router-dev@localhost:14222 scripts/reset-log-stream.sh
set -euo pipefail

URL="${NATS_URL:-nats://router:router-dev@localhost:4222}"
SUBJECTS="tenant.*.interaction.*.log"

if ! command -v nats >/dev/null 2>&1; then
  echo "this script needs the 'nats' CLI; or boot the router once with RP_RESET_LOG_STREAM=1" >&2
  exit 1
fi

nats --server "$URL" stream rm INTERACTION_LOGS --force 2>/dev/null || true
nats --server "$URL" stream add INTERACTION_LOGS \
  --subjects "$SUBJECTS" \
  --storage file --retention limits --discard old \
  --max-msgs-per-subject=-1 \
  --max-msgs=-1 --max-bytes=-1 --max-age=0s --dupe-window=2m \
  --replicas 1 --no-allow-rollup --no-deny-delete --no-deny-purge
echo "INTERACTION_LOGS reset (protobuf cutover)"

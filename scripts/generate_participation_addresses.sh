#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go_output="$repo_root/gen/go/relaypoint/interaction/v1/subjects.go"
ts_output="$repo_root/clients/typescript/src/gen/relaypoint/interaction/v1/subjects.ts"

mkdir -p "$(dirname "$go_output")" "$(dirname "$ts_output")"
cat > "$go_output" <<'EOF'
// Code generated from contracts/asyncapi/participation.yaml by scripts/generate_participation_addresses.sh. DO NOT EDIT.

package interactionpb

import (
	"fmt"
	"regexp"
)

var participationSubjectTokenPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func MutateDesiredParticipationAddress(tenant, interaction string) (string, error) {
	return participationAddress("rpc.corex.participation-mutate.v1", tenant, interaction)
}

func ParticipationCommandAddress(tenant, interaction string) (string, error) {
	return participationAddress("corex.participation.commands.v1", tenant, interaction)
}

func ReplayParticipationAddress(tenant, interaction string) (string, error) {
	return participationAddress("rpc.corex.participation-replay.v1", tenant, interaction)
}

func DesiredParticipationSnapshotAddress(tenant, interaction string) (string, error) {
	return participationAddress("rpc.corex.participation-snapshot.v1", tenant, interaction)
}

func participationAddress(prefix, tenant, interaction string) (string, error) {
	if !participationSubjectTokenPattern.MatchString(tenant) || !participationSubjectTokenPattern.MatchString(interaction) {
		return "", fmt.Errorf("invalid participation address")
	}
	return fmt.Sprintf("%s.%s.%s", prefix, tenant, interaction), nil
}
EOF

cat > "$ts_output" <<'EOF'
// Code generated from contracts/asyncapi/participation.yaml by scripts/generate_participation_addresses.sh. DO NOT EDIT.

const uuidv7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function mutateDesiredParticipationAddress(tenant: string, interaction: string): string {
  return participationAddress("rpc.corex.participation-mutate.v1", tenant, interaction);
}

export function participationCommandAddress(tenant: string, interaction: string): string {
  return participationAddress("corex.participation.commands.v1", tenant, interaction);
}

export function replayParticipationAddress(tenant: string, interaction: string): string {
  return participationAddress("rpc.corex.participation-replay.v1", tenant, interaction);
}

export function desiredParticipationSnapshotAddress(tenant: string, interaction: string): string {
  return participationAddress("rpc.corex.participation-snapshot.v1", tenant, interaction);
}

function participationAddress(prefix: string, tenant: string, interaction: string): string {
  if (!uuidv7.test(tenant) || !uuidv7.test(interaction)) throw new Error("invalid participation address");
  return `${prefix}.${tenant}.${interaction}`;
}
EOF

if [[ "${1:-}" == "--verify" ]]; then
    grep -Fq 'rpc.corex.participation-mutate.v1.{tenant}.{interaction}:' "$repo_root/contracts/asyncapi/participation.yaml"
    grep -Fq 'corex.participation.commands.v1.{tenant}.{interaction}:' "$repo_root/contracts/asyncapi/participation.yaml"
    grep -Fq 'rpc.corex.participation-replay.v1.{tenant}.{interaction}:' "$repo_root/contracts/asyncapi/participation.yaml"
    grep -Fq 'rpc.corex.participation-snapshot.v1.{tenant}.{interaction}:' "$repo_root/contracts/asyncapi/participation.yaml"
fi

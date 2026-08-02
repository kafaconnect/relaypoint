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

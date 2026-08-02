package contract

import (
	"testing"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// @spec:service-extraction.participation.replay-and-reconcile
func TestParticipationContractCarriesOrderedIdentityAndRecovery(t *testing.T) {
	file := interactionv1.File_relaypoint_interaction_v1_interaction_proto
	messages := file.Messages()
	command := messages.ByName("ParticipationCommand")
	if command == nil {
		t.Fatal("missing ParticipationCommand")
	}
	for number, name := range []protoreflect.Name{
		"event_id", "aggregate_version", "tenant_id", "interaction_id", "participant_id",
		"desired_state", "occurred_at", "traceparent", "capability",
	} {
		field := command.Fields().ByNumber(protoreflect.FieldNumber(number + 1))
		if field == nil || field.Name() != name {
			t.Fatalf("command field %d = %v", number+1, field)
		}
	}
	for _, name := range []protoreflect.Name{
		"ReplayParticipationRequest", "ReplayParticipationResponse",
		"GetDesiredParticipationSnapshotRequest", "GetDesiredParticipationSnapshotResponse",
	} {
		if messages.ByName(name) == nil {
			t.Fatalf("missing %s", name)
		}
	}
	service := file.Services().ByName("ParticipationAuthorityService")
	if service == nil || service.Methods().Len() != 2 ||
		service.Methods().Get(0).Name() != "ReplayParticipation" ||
		service.Methods().Get(1).Name() != "GetDesiredParticipationSnapshot" {
		t.Fatalf("participation service = %v", service)
	}
}

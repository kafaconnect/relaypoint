package contract

import (
	"os"
	"strings"
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
		"desired_state", "occurred_at", "traceparent",
	} {
		field := command.Fields().ByNumber(protoreflect.FieldNumber(number + 1))
		if field == nil || field.Name() != name {
			t.Fatalf("command field %d = %v", number+1, field)
		}
	}
	if command.Fields().ByNumber(9) != nil || command.Fields().ByName("capability") != nil {
		t.Fatal("ParticipationCommand capability must not be payload authority")
	}
	if command.ReservedRanges().Len() != 1 || command.ReservedRanges().Get(0) != [2]protoreflect.FieldNumber{9, 10} {
		t.Fatalf("reserved ranges = %v", command.ReservedRanges())
	}
	if command.ReservedNames().Len() != 1 || command.ReservedNames().Get(0) != "capability" {
		t.Fatalf("reserved names = %v", command.ReservedNames())
	}
	for _, name := range []protoreflect.Name{
		"MutateDesiredParticipationRequest", "MutateDesiredParticipationResponse",
		"ReplayParticipationRequest", "ReplayParticipationResponse",
		"GetDesiredParticipationSnapshotRequest", "GetDesiredParticipationSnapshotResponse",
		"ReplayParticipationReply", "DesiredParticipationSnapshotReply",
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
	tenant := "018f1000-0000-7000-8000-000000000001"
	interaction := "018f1000-0000-7000-8000-000000000002"
	addresses := []struct {
		build func(string, string) (string, error)
		want  string
	}{
		{interactionv1.MutateDesiredParticipationAddress, "rpc.corex.participation-mutate.v1"},
		{interactionv1.ParticipationCommandAddress, "corex.participation.commands.v1"},
		{interactionv1.ReplayParticipationAddress, "rpc.corex.participation-replay.v1"},
		{interactionv1.DesiredParticipationSnapshotAddress, "rpc.corex.participation-snapshot.v1"},
	}
	for _, address := range addresses {
		got, err := address.build(tenant, interaction)
		if err != nil || !strings.HasPrefix(got, address.want+".") {
			t.Fatalf("address=%q error=%v", got, err)
		}
	}
	asyncAPI, err := os.ReadFile("../../contracts/asyncapi/participation.yaml")
	if err != nil {
		t.Fatal(err)
	}
	authority := string(asyncAPI)
	for value, count := range map[string]int{
		"source: authenticated-nats-acl-grant":   4,
		"tenant-binding: subject-equals-payload": 4,
		"capability: Corex-participation-write":  2,
		"capability: Corex-participation-read":   2,
		"service: desk":                          1,
		"service: corex":                         1,
		"service: relaypoint":                    2,
	} {
		if strings.Count(authority, value) != count {
			t.Fatalf("AsyncAPI authority %q count=%d want=%d", value, strings.Count(authority, value), count)
		}
	}
}

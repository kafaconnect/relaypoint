package participationclientgrpc

import (
	"context"
	"testing"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/participation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type wireClient struct {
	metadata metadata.MD
}

func (c *wireClient) ReplayParticipation(ctx context.Context, _ *interactionv1.ReplayParticipationRequest, _ ...grpc.CallOption) (*interactionv1.ReplayParticipationResponse, error) {
	c.metadata, _ = metadata.FromOutgoingContext(ctx)
	return &interactionv1.ReplayParticipationResponse{}, nil
}

func (c *wireClient) GetDesiredParticipationSnapshot(ctx context.Context, _ *interactionv1.GetDesiredParticipationSnapshotRequest, _ ...grpc.CallOption) (*interactionv1.GetDesiredParticipationSnapshotResponse, error) {
	c.metadata, _ = metadata.FromOutgoingContext(ctx)
	return &interactionv1.GetDesiredParticipationSnapshotResponse{}, nil
}

// @spec:service-extraction.participation.outside-lock-reconciliation
func TestClientPropagatesTenantScopedOpaqueCapability(t *testing.T) {
	wire := new(wireClient)
	client := New(wire)
	principal := participation.Principal{ServiceID: "relaypoint", TenantID: "018f4000-0000-7000-8000-000000000001", Capability: participation.CapabilityRead}
	_, err := client.Replay(context.Background(), principal, &interactionv1.ReplayParticipationRequest{})
	if err != nil || wire.metadata.Get("x-service-id")[0] != "relaypoint" ||
		wire.metadata.Get("x-tenant-id")[0] != principal.TenantID ||
		wire.metadata.Get("x-capability")[0] != participation.CapabilityRead {
		t.Fatalf("metadata=%v err=%v", wire.metadata, err)
	}
}

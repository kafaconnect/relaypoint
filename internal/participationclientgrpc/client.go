package participationclientgrpc

import (
	"context"
	"errors"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/participation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Client struct {
	client interactionv1.ParticipationAuthorityServiceClient
}

func New(client interactionv1.ParticipationAuthorityServiceClient) *Client {
	return &Client{client: client}
}

func (c *Client) Replay(ctx context.Context, principal participation.Principal, request *interactionv1.ReplayParticipationRequest) (*interactionv1.ReplayParticipationResponse, error) {
	if c == nil || c.client == nil || !validPrincipal(principal) {
		return nil, participation.ErrPermissionDenied
	}
	response, err := c.client.ReplayParticipation(outgoingContext(ctx, principal), request)
	return response, mapError(err)
}

func (c *Client) Snapshot(ctx context.Context, principal participation.Principal, request *interactionv1.GetDesiredParticipationSnapshotRequest) (*interactionv1.GetDesiredParticipationSnapshotResponse, error) {
	if c == nil || c.client == nil || !validPrincipal(principal) {
		return nil, participation.ErrPermissionDenied
	}
	response, err := c.client.GetDesiredParticipationSnapshot(outgoingContext(ctx, principal), request)
	return response, mapError(err)
}

func outgoingContext(ctx context.Context, principal participation.Principal) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		"x-service-id", principal.ServiceID,
		"x-tenant-id", principal.TenantID,
		"x-capability", principal.Capability,
	)
}

func validPrincipal(principal participation.Principal) bool {
	return principal.ServiceID == "relaypoint" && principal.TenantID != "" &&
		principal.Capability == participation.CapabilityRead && principal.Role == ""
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.NotFound:
		return errors.Join(participation.ErrUnknownHistory, err)
	case codes.Unauthenticated, codes.PermissionDenied:
		return errors.Join(participation.ErrPermissionDenied, err)
	default:
		return err
	}
}

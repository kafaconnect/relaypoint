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
	grant  participation.TransportGrant
}

func New(client interactionv1.ParticipationAuthorityServiceClient, grant participation.TransportGrant) (*Client, error) {
	if client == nil || grant.ServiceID != "relaypoint" ||
		!participation.ValidTransportGrant(grant, participation.CapabilityRead) {
		return nil, participation.ErrPermissionDenied
	}
	return &Client{client: client, grant: grant}, nil
}

func (c *Client) Replay(ctx context.Context, request *interactionv1.ReplayParticipationRequest) (*interactionv1.ReplayParticipationResponse, error) {
	if c == nil || c.client == nil || request == nil {
		return nil, participation.ErrPermissionDenied
	}
	response, err := c.client.ReplayParticipation(outgoingContext(ctx, c.grant, request.GetTenantId()), request)
	return response, mapError(err)
}

func (c *Client) Snapshot(ctx context.Context, request *interactionv1.GetDesiredParticipationSnapshotRequest) (*interactionv1.GetDesiredParticipationSnapshotResponse, error) {
	if c == nil || c.client == nil || request == nil {
		return nil, participation.ErrPermissionDenied
	}
	response, err := c.client.GetDesiredParticipationSnapshot(outgoingContext(ctx, c.grant, request.GetTenantId()), request)
	return response, mapError(err)
}

func outgoingContext(ctx context.Context, grant participation.TransportGrant, tenantID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		"x-service-id", grant.ServiceID,
		"x-tenant-id", tenantID,
		"x-capability", grant.Capability,
	)
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

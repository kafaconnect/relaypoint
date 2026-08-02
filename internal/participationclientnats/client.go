package participationclientnats

import (
	"context"
	"errors"
	"time"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/participation"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	connection *nats.Conn
	timeout    time.Duration
	grant      participation.TransportGrant
}

func New(connection *nats.Conn, timeout time.Duration, grant participation.TransportGrant) (*Client, error) {
	if connection == nil || !connection.IsConnected() || timeout <= 0 || !authenticated(connection.Opts) ||
		grant.ServiceID != "relaypoint" || !participation.ValidTransportGrant(grant, participation.CapabilityRead) {
		return nil, participation.ErrPermissionDenied
	}
	return &Client{connection: connection, timeout: timeout, grant: grant}, nil
}

func (c *Client) Replay(ctx context.Context, request *interactionv1.ReplayParticipationRequest) (*interactionv1.ReplayParticipationResponse, error) {
	if c == nil || request == nil || !participation.ValidTransportGrant(c.grant, participation.CapabilityRead) {
		return nil, participation.ErrInvalid
	}
	subject, err := interactionv1.ReplayParticipationAddress(request.GetTenantId(), request.GetInteractionId())
	if err != nil {
		return nil, participation.ErrInvalid
	}
	reply := new(interactionv1.ReplayParticipationReply)
	if err := c.request(ctx, subject, request, reply); err != nil {
		return nil, err
	}
	if reply.GetRequestId() != request.GetRequestId() {
		return nil, participation.ErrInvalid
	}
	if response := reply.GetResponse(); response != nil {
		return response, nil
	}
	return nil, mapWireError(reply.GetError())
}

func (c *Client) Snapshot(ctx context.Context, request *interactionv1.GetDesiredParticipationSnapshotRequest) (*interactionv1.GetDesiredParticipationSnapshotResponse, error) {
	if c == nil || request == nil || !participation.ValidTransportGrant(c.grant, participation.CapabilityRead) {
		return nil, participation.ErrInvalid
	}
	subject, err := interactionv1.DesiredParticipationSnapshotAddress(request.GetTenantId(), request.GetInteractionId())
	if err != nil {
		return nil, participation.ErrInvalid
	}
	reply := new(interactionv1.DesiredParticipationSnapshotReply)
	if err := c.request(ctx, subject, request, reply); err != nil {
		return nil, err
	}
	if reply.GetRequestId() != request.GetRequestId() {
		return nil, participation.ErrInvalid
	}
	if response := reply.GetResponse(); response != nil {
		return response, nil
	}
	return nil, mapWireError(reply.GetError())
}

func (c *Client) request(ctx context.Context, subject string, request, response proto.Message) error {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return participation.ErrInvalid
	}
	message := nats.NewMsg(subject)
	message.Data = payload
	obs.InjectTraceparent(ctx, message.Header.Set)
	requestCtx, cancel := requestContext(ctx, c.timeout)
	defer cancel()
	reply, err := c.connection.RequestMsgWithContext(requestCtx, message)
	if err != nil {
		return err
	}
	if _, ok := obs.ParseTraceparent(reply.Header.Get("traceparent")); !ok {
		return participation.ErrInvalid
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(reply.Data, response); err != nil || len(response.ProtoReflect().GetUnknown()) != 0 {
		return participation.ErrInvalid
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil || string(canonical) != string(reply.Data) {
		return participation.ErrInvalid
	}
	return nil
}

func requestContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func authenticated(options nats.Options) bool {
	return (options.User != "" && options.Password != "") ||
		(options.Nkey != "" && options.SignatureCB != nil) || options.Token != "" || options.UserJWT != nil
}

func mapWireError(value *interactionv1.ParticipationError) error {
	if value == nil {
		return participation.ErrInvalid
	}
	switch value.GetCode() {
	case interactionv1.ParticipationError_CODE_UNKNOWN_HISTORY:
		return participation.ErrUnknownHistory
	case interactionv1.ParticipationError_CODE_PERMISSION_DENIED:
		return participation.ErrPermissionDenied
	case interactionv1.ParticipationError_CODE_INVALID:
		return participation.ErrInvalid
	case interactionv1.ParticipationError_CODE_TRANSIENT:
		return errors.New("participation authority transient")
	default:
		return errors.New("participation authority failure")
	}
}

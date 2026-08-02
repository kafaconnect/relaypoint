package deliverygrpc

import (
	"context"
	"errors"

	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
	"github.com/kafaconnect/relaypoint/internal/delivery"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PrincipalResolver interface {
	Resolve(context.Context) (delivery.Principal, error)
}

type Acceptor interface {
	AcceptDelivery(context.Context, delivery.Principal, *deliverypb.DeliveryAuthorization) (*deliverypb.DeliveryReceipt, error)
}

type Server struct {
	deliverypb.UnimplementedDeliveryServiceServer
	principals PrincipalResolver
	acceptor   Acceptor
}

func New(principals PrincipalResolver, acceptor Acceptor) *Server {
	return &Server{principals: principals, acceptor: acceptor}
}

func (s *Server) AcceptDelivery(ctx context.Context, authorization *deliverypb.DeliveryAuthorization) (*deliverypb.DeliveryReceipt, error) {
	principal, err := s.principals.Resolve(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	receipt, err := s.acceptor.AcceptDelivery(ctx, principal, authorization)
	if err == nil {
		return receipt, nil
	}
	switch {
	case errors.Is(err, delivery.ErrUnauthenticated):
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	case errors.Is(err, delivery.ErrPermissionDenied):
		return nil, status.Error(codes.PermissionDenied, "delivery capability denied")
	case errors.Is(err, delivery.ErrInvalidAuthorization):
		return nil, status.Error(codes.InvalidArgument, "invalid delivery authorization")
	case errors.Is(err, delivery.ErrIdempotencyConflict):
		return nil, status.Error(codes.AlreadyExists, "delivery authorization idempotency conflict")
	default:
		return nil, status.Error(codes.Internal, "delivery acceptance failed")
	}
}

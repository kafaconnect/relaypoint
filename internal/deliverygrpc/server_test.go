package deliverygrpc

import (
	"context"
	"errors"
	"testing"

	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
	"github.com/kafaconnect/relaypoint/internal/delivery"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type principalResolver struct {
	principal delivery.Principal
	err       error
}

func (r principalResolver) Resolve(context.Context) (delivery.Principal, error) {
	return r.principal, r.err
}

type acceptor struct {
	err error
}

func (a acceptor) AcceptDelivery(context.Context, delivery.Principal, *deliverypb.DeliveryAuthorization) (*deliverypb.DeliveryReceipt, error) {
	return &deliverypb.DeliveryReceipt{}, a.err
}

func TestServerMapsBoundaryFailures(t *testing.T) {
	tests := []struct {
		name     string
		resolver principalResolver
		acceptor acceptor
		code     codes.Code
	}{
		{name: "auth", resolver: principalResolver{err: errors.New("missing")}, code: codes.Unauthenticated},
		{name: "tenant", acceptor: acceptor{err: delivery.ErrPermissionDenied}, code: codes.PermissionDenied},
		{name: "invalid", acceptor: acceptor{err: delivery.ErrInvalidAuthorization}, code: codes.InvalidArgument},
		{name: "conflict", acceptor: acceptor{err: delivery.ErrIdempotencyConflict}, code: codes.AlreadyExists},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := New(test.resolver, test.acceptor)
			_, err := server.AcceptDelivery(context.Background(), &deliverypb.DeliveryAuthorization{})
			if status.Code(err) != test.code {
				t.Fatalf("code=%s err=%v", status.Code(err), err)
			}
		})
	}
}

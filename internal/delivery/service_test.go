package delivery

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	tenantID        = "018f08f6-b27d-7d8b-a4d8-7dc23620f13d"
	reservationID   = "018f08f6-b27e-73d3-a138-92285f8c2297"
	interactionID   = "018f08f6-b27f-77bb-8513-8a7d3535eeef"
	authorizationID = "018f08f6-b280-7520-97f1-d6f879f4b8a2"
	eventID         = "018f08f6-b281-70ef-b3f5-fba9e501b32b"
	receiptID       = "018f08f6-b282-7e55-8946-748055a2f719"
	fencingToken    = "018f08f6-b283-7682-b656-c144743580b1"
	subscriberID    = "018f08f6-b284-7fd7-a892-0db0b3279179"
)

type appendStore struct {
	acceptedAt time.Time
	row        *AppendResult
	hash       [32]byte
	calls      int
}

func (s *appendStore) Append(_ context.Context, in AppendInput) (AppendResult, error) {
	s.calls++
	if s.row != nil {
		if s.hash != in.RequestHash {
			return AppendResult{}, ErrIdempotencyConflict
		}
		return *s.row, nil
	}
	s.hash = in.RequestHash
	s.row = &AppendResult{ReceiptID: in.ReceiptID, AcceptedAt: s.acceptedAt, LogSequence: 41}
	return *s.row, nil
}

func authorization() *deliverypb.DeliveryAuthorization {
	return &deliverypb.DeliveryAuthorization{
		DeliveryAuthorizationId: authorizationID,
		EventId:                 eventID,
		TenantId:                tenantID,
		ReservationId:           reservationID,
		InteractionId:           interactionID,
		TargetSubscriberId:      subscriberID,
		FencingToken:            fencingToken,
		RouteVersion:            7,
		DeliveryDeadline:        timestamppb.New(time.Date(2026, 8, 2, 12, 1, 0, 0, time.UTC)),
		Traceparent:             "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
}

// @spec:service-extraction.relaypoint.authorization-outbox-survives-crash
func TestAuthorizationAppendAndAckOutboxSurviveResponseLoss(t *testing.T) {
	acceptedAt := time.Date(2026, 8, 2, 12, 0, 0, 123456000, time.UTC)
	store := &appendStore{acceptedAt: acceptedAt}
	service := NewService(store, func() string { return receiptID })
	principal := Principal{ServiceID: "corex", TenantID: tenantID, Capabilities: []string{CapabilityDeliveryWrite}}

	first, err := service.AcceptDelivery(context.Background(), principal, authorization())
	if err != nil {
		t.Fatalf("first accept: %v", err)
	}
	second, err := service.AcceptDelivery(context.Background(), principal, authorization())
	if err != nil {
		t.Fatalf("retry accept: %v", err)
	}
	if !proto.Equal(first, second) {
		t.Fatalf("response-loss retry changed receipt: first=%v second=%v", first, second)
	}
	if got := first.GetAcceptedAt().AsTime(); !got.Equal(acceptedAt) {
		t.Fatalf("accepted_at=%s want database server time %s", got, acceptedAt)
	}
	if first.GetVisibility() != deliverypb.DeliveryVisibility_DELIVERY_VISIBILITY_PENDING_NOT_VISIBLE {
		t.Fatalf("visibility=%s", first.GetVisibility())
	}
	if store.calls != 2 {
		t.Fatalf("append calls=%d want 2", store.calls)
	}
}

func TestAuthorizationDivergentReuseConflicts(t *testing.T) {
	store := &appendStore{acceptedAt: time.Now()}
	service := NewService(store, func() string { return receiptID })
	principal := Principal{ServiceID: "corex", TenantID: tenantID, Capabilities: []string{CapabilityDeliveryWrite}}
	if _, err := service.AcceptDelivery(context.Background(), principal, authorization()); err != nil {
		t.Fatalf("first accept: %v", err)
	}
	changed := authorization()
	changed.TargetSubscriberId = "018f08f6-b285-71f5-a0d4-a318b144d12f"
	if _, err := service.AcceptDelivery(context.Background(), principal, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("divergent reuse error=%v", err)
	}
}

func TestAuthorizationHashBindsMethodAndTenant(t *testing.T) {
	a := authorization()
	first, err := HashAuthorization(a)
	if err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(a).(*deliverypb.DeliveryAuthorization)
	changed.TenantId = "018f08f6-b286-7326-a04d-d20bda87644d"
	second, err := HashAuthorization(changed)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("tenant was not bound into request hash")
	}
	if got := hex.EncodeToString(first[:]); got != "68a8b48826149e638e1d2604fa422581a0b4bc50d66b92f935db7c888723793a" {
		t.Fatalf("authorization hash=%s", got)
	}
}

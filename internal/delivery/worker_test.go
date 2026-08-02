package delivery

import (
	"context"
	"errors"
	"testing"

	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
)

type ackStore struct {
	claim       AckClaim
	completeErr error
	completed   bool
	completions int
}

func (s *ackStore) Claim(context.Context, string) (AckClaim, bool, error) {
	if s.completed {
		return AckClaim{}, false, nil
	}
	return s.claim, true, nil
}

func (s *ackStore) Complete(_ context.Context, _, _, _ string, _ AckResult) error {
	s.completions++
	if s.completeErr != nil {
		err := s.completeErr
		s.completeErr = nil
		return err
	}
	s.completed = true
	return nil
}

func (s *ackStore) Retry(context.Context, string, string, string, error) error { return nil }

type routerRecorder struct {
	requests []AcknowledgeRequest
	result   AckResult
}

func ackClaim(receipt *deliverypb.DeliveryReceipt) AckClaim {
	claim := AckClaim{Receipt: receipt, ClaimToken: "018f08f6-b287-76e8-bb83-5d3e95a808a8", ExpectedRouteVersion: 7, FencingToken: fencingToken}
	request := AcknowledgeRequest{RequestID: receipt.GetReceiptId(), TenantID: receipt.GetTenantId(), ReservationID: receipt.GetReservationId(), InteractionID: receipt.GetInteractionId(), ExpectedRouteVersion: claim.ExpectedRouteVersion, FencingToken: claim.FencingToken, Receipt: receipt}
	claim.RequestHash, _ = request.Hash()
	return claim
}

func (r *routerRecorder) AcknowledgeDelivery(_ context.Context, req AcknowledgeRequest) (AckResult, error) {
	r.requests = append(r.requests, req)
	return r.result, nil
}

// @spec:service-extraction.relaypoint.ack-outbox-response-loss
func TestAckOutboxReplaysExactReceiptAfterCompletionLoss(t *testing.T) {
	receipt := &deliverypb.DeliveryReceipt{ReceiptId: receiptID, DeliveryAuthorizationId: authorizationID, EventId: eventID, TenantId: tenantID, ReservationId: reservationID, InteractionId: interactionID, TargetSubscriberId: subscriberID, IssuerServiceId: "relaypoint", Visibility: deliverypb.DeliveryVisibility_DELIVERY_VISIBILITY_PENDING_NOT_VISIBLE}
	store := &ackStore{claim: ackClaim(receipt), completeErr: errors.New("commit lost")}
	router := &routerRecorder{result: AckResult{Disposition: AckApplied, RouteVersion: 8}}
	worker := NewAckWorker(store, router, tenantID)

	if err := worker.ProcessOne(context.Background()); err == nil {
		t.Fatal("first completion loss unexpectedly succeeded")
	}
	if err := worker.ProcessOne(context.Background()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(router.requests) != 2 {
		t.Fatalf("router calls=%d want 2", len(router.requests))
	}
	if !router.requests[0].Equal(router.requests[1]) {
		t.Fatalf("ack body changed: first=%+v second=%+v", router.requests[0], router.requests[1])
	}
	if router.requests[0].RequestID != receiptID {
		t.Fatalf("request id=%s want receipt id", router.requests[0].RequestID)
	}
}

// @spec:service-extraction.relaypoint.late-ack-is-tombstoned
func TestTerminalAckCompletesOutboxWithoutGrantingVisibility(t *testing.T) {
	receipt := &deliverypb.DeliveryReceipt{ReceiptId: receiptID, DeliveryAuthorizationId: authorizationID, EventId: eventID, TenantId: tenantID, ReservationId: reservationID, InteractionId: interactionID, TargetSubscriberId: subscriberID, IssuerServiceId: "relaypoint", Visibility: deliverypb.DeliveryVisibility_DELIVERY_VISIBILITY_PENDING_NOT_VISIBLE}
	store := &ackStore{claim: ackClaim(receipt)}
	router := &routerRecorder{result: AckResult{Disposition: AckTerminalExpired, RouteVersion: 8}}
	worker := NewAckWorker(store, router, tenantID)
	if err := worker.ProcessOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !store.completed || store.completions != 1 {
		t.Fatalf("completed=%v completions=%d", store.completed, store.completions)
	}
}

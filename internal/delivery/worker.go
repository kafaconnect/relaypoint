package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
	"google.golang.org/protobuf/proto"
)

const acknowledgeDeliveryMethod = "/kafaconnect.routing.v1.ReservationService/AcknowledgeDelivery"

var ErrUnknownAcknowledgement = errors.New("delivery: unknown acknowledgement disposition")
var ErrAcknowledgementHash = errors.New("delivery: acknowledgement hash mismatch")

type AckDisposition uint8

const (
	AckApplied AckDisposition = iota + 1
	AckAlreadyApplied
	AckTerminalExpired
)

type AckResult struct {
	Disposition  AckDisposition
	RouteVersion uint64
	EventID      string
}

type AckClaim struct {
	Receipt              *deliverypb.DeliveryReceipt
	ClaimToken           string
	ExpectedRouteVersion uint64
	FencingToken         string
	RequestHash          [32]byte
}

type AcknowledgeRequest struct {
	RequestID            string
	TenantID             string
	ReservationID        string
	InteractionID        string
	ExpectedRouteVersion uint64
	FencingToken         string
	Receipt              *deliverypb.DeliveryReceipt
}

func (r AcknowledgeRequest) Equal(other AcknowledgeRequest) bool {
	return r.RequestID == other.RequestID && r.TenantID == other.TenantID && r.ReservationID == other.ReservationID && r.InteractionID == other.InteractionID && r.ExpectedRouteVersion == other.ExpectedRouteVersion && r.FencingToken == other.FencingToken && proto.Equal(r.Receipt, other.Receipt)
}

func (r AcknowledgeRequest) Hash() ([32]byte, error) {
	receipt, err := proto.MarshalOptions{Deterministic: true}.Marshal(r.Receipt)
	if err != nil {
		return [32]byte{}, err
	}
	hasher := sha256.New()
	for _, part := range [][]byte{[]byte(acknowledgeDeliveryMethod), []byte(r.TenantID), []byte(r.RequestID), []byte(r.ReservationID), []byte(r.InteractionID)} {
		writeHashPart(hasher, part)
	}
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], r.ExpectedRouteVersion)
	writeHashPart(hasher, version[:])
	writeHashPart(hasher, []byte(r.FencingToken))
	writeHashPart(hasher, receipt)
	var hash [32]byte
	copy(hash[:], hasher.Sum(nil))
	return hash, nil
}

type AckStore interface {
	Claim(context.Context, string) (AckClaim, bool, error)
	Complete(context.Context, string, string, string, AckResult) error
	Retry(context.Context, string, string, string, error) error
}

type RouterPort interface {
	AcknowledgeDelivery(context.Context, AcknowledgeRequest) (AckResult, error)
}

type AckWorker struct {
	store    AckStore
	router   RouterPort
	tenantID string
}

func NewAckWorker(store AckStore, router RouterPort, tenantID string) *AckWorker {
	return &AckWorker{store: store, router: router, tenantID: tenantID}
}

func (w *AckWorker) ProcessOne(ctx context.Context) error {
	claim, ok, err := w.store.Claim(ctx, w.tenantID)
	if err != nil || !ok {
		return err
	}
	request := AcknowledgeRequest{
		RequestID: claim.Receipt.GetReceiptId(), TenantID: claim.Receipt.GetTenantId(), ReservationID: claim.Receipt.GetReservationId(), InteractionID: claim.Receipt.GetInteractionId(),
		ExpectedRouteVersion: claim.ExpectedRouteVersion, FencingToken: claim.FencingToken, Receipt: proto.Clone(claim.Receipt).(*deliverypb.DeliveryReceipt),
	}
	hash, err := request.Hash()
	if err != nil || hash != claim.RequestHash {
		if err == nil {
			err = ErrAcknowledgementHash
		}
		if retryErr := w.store.Retry(ctx, w.tenantID, request.RequestID, claim.ClaimToken, err); retryErr != nil {
			return retryErr
		}
		return err
	}
	result, err := w.router.AcknowledgeDelivery(ctx, request)
	if err != nil {
		if retryErr := w.store.Retry(ctx, w.tenantID, request.RequestID, claim.ClaimToken, err); retryErr != nil {
			return retryErr
		}
		return err
	}
	if result.Disposition != AckApplied && result.Disposition != AckAlreadyApplied && result.Disposition != AckTerminalExpired {
		err = ErrUnknownAcknowledgement
		if retryErr := w.store.Retry(ctx, w.tenantID, request.RequestID, claim.ClaimToken, err); retryErr != nil {
			return retryErr
		}
		return err
	}
	return w.store.Complete(ctx, w.tenantID, request.RequestID, claim.ClaimToken, result)
}

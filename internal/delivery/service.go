package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	CapabilityDeliveryWrite = "corex-delivery-write"
	acceptDeliveryMethod    = "/relaypoint.delivery.v1.DeliveryService/AcceptDelivery"
	issuerServiceID         = "relaypoint"
)

var (
	ErrUnauthenticated      = errors.New("delivery: unauthenticated")
	ErrPermissionDenied     = errors.New("delivery: permission denied")
	ErrInvalidAuthorization = errors.New("delivery: invalid authorization")
	ErrIdempotencyConflict  = errors.New("delivery: idempotency conflict")
	traceparentPattern      = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$`)
)

type Principal struct {
	ServiceID    string
	TenantID     string
	Capabilities []string
}

type AppendInput struct {
	Authorization *deliverypb.DeliveryAuthorization
	ReceiptID     string
	IssuerService string
	RequestHash   [32]byte
	RequestBody   []byte
}

type AppendResult struct {
	ReceiptID   string
	AcceptedAt  time.Time
	LogSequence uint64
}

type AuthorizationStore interface {
	Append(context.Context, AppendInput) (AppendResult, error)
}

type Service struct {
	store AuthorizationStore
	newID func() string
}

func NewService(store AuthorizationStore, newID func() string) *Service {
	if newID == nil {
		newID = func() string { return uuid.Must(uuid.NewV7()).String() }
	}
	return &Service{store: store, newID: newID}
}

func (s *Service) AcceptDelivery(ctx context.Context, principal Principal, authorization *deliverypb.DeliveryAuthorization) (*deliverypb.DeliveryReceipt, error) {
	if principal.ServiceID != "corex" {
		return nil, ErrUnauthenticated
	}
	if !hasCapability(principal.Capabilities, CapabilityDeliveryWrite) {
		return nil, ErrPermissionDenied
	}
	if authorization == nil || principal.TenantID == "" || principal.TenantID != authorization.GetTenantId() {
		return nil, ErrPermissionDenied
	}
	if err := validateAuthorization(authorization); err != nil {
		return nil, err
	}
	normalized := normalizeAuthorization(authorization)
	hash, body, err := authorizationHashAndBody(normalized)
	if err != nil {
		return nil, err
	}
	receiptID := s.newID()
	if err := requireUUIDv7("receipt_id", receiptID); err != nil {
		return nil, err
	}
	result, err := s.store.Append(ctx, AppendInput{Authorization: normalized, ReceiptID: receiptID, IssuerService: issuerServiceID, RequestHash: hash, RequestBody: body})
	if err != nil {
		return nil, err
	}
	return &deliverypb.DeliveryReceipt{
		ReceiptId:               result.ReceiptID,
		DeliveryAuthorizationId: normalized.GetDeliveryAuthorizationId(),
		EventId:                 normalized.GetEventId(),
		TenantId:                normalized.GetTenantId(),
		ReservationId:           normalized.GetReservationId(),
		InteractionId:           normalized.GetInteractionId(),
		TargetSubscriberId:      normalized.GetTargetSubscriberId(),
		AcceptedAt:              timestamppb.New(result.AcceptedAt.UTC()),
		DeliveryDeadline:        normalized.GetDeliveryDeadline(),
		LogSequence:             result.LogSequence,
		IssuerServiceId:         issuerServiceID,
		Visibility:              deliverypb.DeliveryVisibility_DELIVERY_VISIBILITY_PENDING_NOT_VISIBLE,
	}, nil
}

func HashAuthorization(authorization *deliverypb.DeliveryAuthorization) ([32]byte, error) {
	if err := validateAuthorization(authorization); err != nil {
		return [32]byte{}, err
	}
	hash, _, err := authorizationHashAndBody(normalizeAuthorization(authorization))
	return hash, err
}

func authorizationHashAndBody(authorization *deliverypb.DeliveryAuthorization) ([32]byte, []byte, error) {
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(authorization)
	if err != nil {
		return [32]byte{}, nil, fmt.Errorf("marshal authorization: %w", err)
	}
	hasher := sha256.New()
	writeHashPart(hasher, []byte(acceptDeliveryMethod))
	writeHashPart(hasher, []byte(authorization.GetTenantId()))
	writeHashPart(hasher, body)
	var hash [32]byte
	copy(hash[:], hasher.Sum(nil))
	return hash, body, nil
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func writeHashPart(writer hashWriter, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write(value)
}

func validateAuthorization(authorization *deliverypb.DeliveryAuthorization) error {
	if authorization == nil {
		return fmt.Errorf("%w: required", ErrInvalidAuthorization)
	}
	for field, value := range map[string]string{
		"delivery_authorization_id": authorization.GetDeliveryAuthorizationId(),
		"event_id":                  authorization.GetEventId(),
		"tenant_id":                 authorization.GetTenantId(),
		"reservation_id":            authorization.GetReservationId(),
		"interaction_id":            authorization.GetInteractionId(),
		"target_subscriber_id":      authorization.GetTargetSubscriberId(),
		"fencing_token":             authorization.GetFencingToken(),
	} {
		if err := requireUUIDv7(field, value); err != nil {
			return err
		}
	}
	if authorization.GetRouteVersion() == 0 {
		return fmt.Errorf("%w: route_version must be positive", ErrInvalidAuthorization)
	}
	if authorization.GetDeliveryDeadline() == nil || !authorization.GetDeliveryDeadline().IsValid() {
		return fmt.Errorf("%w: valid delivery_deadline required", ErrInvalidAuthorization)
	}
	if !traceparentPattern.MatchString(authorization.GetTraceparent()) {
		return fmt.Errorf("%w: canonical traceparent required", ErrInvalidAuthorization)
	}
	return nil
}

func requireUUIDv7(field, value string) error {
	id, err := uuid.Parse(value)
	if err != nil || id.Version() != 7 || id.Variant() != uuid.RFC4122 || id.String() != value {
		return fmt.Errorf("%w: %s must be canonical UUIDv7", ErrInvalidAuthorization, field)
	}
	return nil
}

func normalizeAuthorization(authorization *deliverypb.DeliveryAuthorization) *deliverypb.DeliveryAuthorization {
	normalized := proto.Clone(authorization).(*deliverypb.DeliveryAuthorization)
	discardUnknown(normalized.ProtoReflect())
	fields := normalized.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() != protoreflect.StringKind || !strings.HasSuffix(string(field.Name()), "_id") {
			continue
		}
		value := normalized.ProtoReflect().Get(field).String()
		if id, err := uuid.Parse(value); err == nil {
			normalized.ProtoReflect().Set(field, protoreflect.ValueOfString(id.String()))
		}
	}
	return normalized
}

func discardUnknown(message protoreflect.Message) {
	message.SetUnknown(nil)
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() != protoreflect.MessageKind || !message.Has(field) {
			continue
		}
		discardUnknown(message.Get(field).Message())
	}
}

func hasCapability(capabilities []string, wanted string) bool {
	for _, capability := range capabilities {
		if capability == wanted {
			return true
		}
	}
	return false
}

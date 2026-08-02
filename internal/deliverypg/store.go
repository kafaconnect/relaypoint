package deliverypg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
	"github.com/kafaconnect/relaypoint/internal/delivery"
	"github.com/kafaconnect/relaypoint/internal/routeprojection"
)

const defaultClaimTTL = 30 * time.Second

type Store struct {
	pool     *pgxpool.Pool
	newID    func() string
	claimTTL time.Duration
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, newID: func() string { return uuid.Must(uuid.NewV7()).String() }, claimTTL: defaultClaimTTL}
}

func (s *Store) Append(ctx context.Context, in delivery.AppendInput) (delivery.AppendResult, error) {
	var result delivery.AppendResult
	err := s.withTenantTx(ctx, in.Authorization.GetTenantId(), func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, in.Authorization.GetTenantId()+"/"+in.Authorization.GetDeliveryAuthorizationId()); err != nil {
			return err
		}
		var retainedHash []byte
		err := tx.QueryRow(ctx, `
SELECT request_hash, receipt_id::text, accepted_at, log_sequence
FROM delivery_authorizations
WHERE tenant_id = $1 AND delivery_authorization_id = $2`, in.Authorization.GetTenantId(), in.Authorization.GetDeliveryAuthorizationId()).Scan(&retainedHash, &result.ReceiptID, &result.AcceptedAt, &result.LogSequence)
		if err == nil {
			if !bytes.Equal(retainedHash, in.RequestHash[:]) {
				return delivery.ErrIdempotencyConflict
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		err = tx.QueryRow(ctx, `
INSERT INTO interaction_delivery_log (
    tenant_id, interaction_id, delivery_authorization_id, receipt_id, request_hash, authorization_body
) VALUES ($1, $2, $3, $4, $5, $6)
RETURNING log_sequence, accepted_at`,
			in.Authorization.GetTenantId(), in.Authorization.GetInteractionId(), in.Authorization.GetDeliveryAuthorizationId(), in.ReceiptID, in.RequestHash[:], in.RequestBody,
		).Scan(&result.LogSequence, &result.AcceptedAt)
		if err != nil {
			return err
		}
		result.ReceiptID = in.ReceiptID
		receipt := receiptFromAppend(in, result)
		receiptBody, err := proto.MarshalOptions{Deterministic: true}.Marshal(receipt)
		if err != nil {
			return err
		}
		ack := delivery.AcknowledgeRequest{
			RequestID: result.ReceiptID, TenantID: in.Authorization.GetTenantId(), ReservationID: in.Authorization.GetReservationId(), InteractionID: in.Authorization.GetInteractionId(),
			ExpectedRouteVersion: in.Authorization.GetRouteVersion(), FencingToken: in.Authorization.GetFencingToken(), Receipt: receipt,
		}
		ackHash, err := ack.Hash()
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `
INSERT INTO delivery_authorizations (
    tenant_id, delivery_authorization_id, event_id, reservation_id, interaction_id,
    target_subscriber_id, fencing_token, route_version, delivery_deadline, traceparent,
    request_hash, request_body, receipt_id, accepted_at, log_sequence, issuer_service_id, visibility
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'PENDING_NOT_VISIBLE')`,
			in.Authorization.GetTenantId(), in.Authorization.GetDeliveryAuthorizationId(), in.Authorization.GetEventId(), in.Authorization.GetReservationId(), in.Authorization.GetInteractionId(),
			in.Authorization.GetTargetSubscriberId(), in.Authorization.GetFencingToken(), in.Authorization.GetRouteVersion(), in.Authorization.GetDeliveryDeadline().AsTime(), in.Authorization.GetTraceparent(),
			in.RequestHash[:], in.RequestBody, result.ReceiptID, result.AcceptedAt, result.LogSequence, in.IssuerService,
		); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
INSERT INTO delivery_ack_outbox (
    tenant_id, receipt_id, delivery_authorization_id, reservation_id, interaction_id,
    target_subscriber_id, expected_route_version, fencing_token, receipt_body, request_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			in.Authorization.GetTenantId(), result.ReceiptID, in.Authorization.GetDeliveryAuthorizationId(), in.Authorization.GetReservationId(), in.Authorization.GetInteractionId(),
			in.Authorization.GetTargetSubscriberId(), in.Authorization.GetRouteVersion(), in.Authorization.GetFencingToken(), receiptBody, ackHash[:],
		)
		return err
	})
	if err != nil {
		return delivery.AppendResult{}, fmt.Errorf("append delivery authorization: %w", err)
	}
	return result, nil
}

func receiptFromAppend(in delivery.AppendInput, result delivery.AppendResult) *deliverypb.DeliveryReceipt {
	return &deliverypb.DeliveryReceipt{
		ReceiptId: result.ReceiptID, DeliveryAuthorizationId: in.Authorization.GetDeliveryAuthorizationId(), EventId: in.Authorization.GetEventId(),
		TenantId: in.Authorization.GetTenantId(), ReservationId: in.Authorization.GetReservationId(), InteractionId: in.Authorization.GetInteractionId(), TargetSubscriberId: in.Authorization.GetTargetSubscriberId(),
		AcceptedAt: timestamppb.New(result.AcceptedAt.UTC()), DeliveryDeadline: in.Authorization.GetDeliveryDeadline(), LogSequence: result.LogSequence, IssuerServiceId: in.IssuerService,
		Visibility: deliverypb.DeliveryVisibility_DELIVERY_VISIBILITY_PENDING_NOT_VISIBLE,
	}
}

func (s *Store) Claim(ctx context.Context, tenantID string) (delivery.AckClaim, bool, error) {
	var claim delivery.AckClaim
	var receiptBody []byte
	var requestHash []byte
	claimToken := s.newID()
	err := s.withTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
WITH candidate AS (
    SELECT receipt_id
    FROM delivery_ack_outbox
    WHERE tenant_id = $1
      AND next_attempt_at <= clock_timestamp()
      AND (status = 'pending' OR (status = 'claimed' AND claim_until <= clock_timestamp()))
    ORDER BY next_attempt_at, receipt_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE delivery_ack_outbox AS o
SET status = 'claimed', claim_token = $2, claim_until = clock_timestamp() + $3::interval
FROM candidate
WHERE o.tenant_id = $1 AND o.receipt_id = candidate.receipt_id
RETURNING o.receipt_body, o.expected_route_version, o.fencing_token::text, o.request_hash`, tenantID, claimToken, s.claimTTL.String()).Scan(&receiptBody, &claim.ExpectedRouteVersion, &claim.FencingToken, &requestHash)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return delivery.AckClaim{}, false, nil
	}
	if err != nil {
		return delivery.AckClaim{}, false, fmt.Errorf("claim delivery acknowledgement: %w", err)
	}
	claim.ClaimToken = claimToken
	copy(claim.RequestHash[:], requestHash)
	claim.Receipt = new(deliverypb.DeliveryReceipt)
	if err := proto.Unmarshal(receiptBody, claim.Receipt); err != nil {
		return delivery.AckClaim{}, false, fmt.Errorf("decode delivery receipt: %w", err)
	}
	return claim, true, nil
}

func (s *Store) Complete(ctx context.Context, tenantID, receiptID, claimToken string, result delivery.AckResult) error {
	return s.withTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
UPDATE delivery_ack_outbox
SET status = 'completed', result_disposition = $4, result_route_version = $5,
    result_event_id = NULLIF($6, '')::uuid, completed_at = clock_timestamp(), claim_token = NULL, claim_until = NULL
WHERE tenant_id = $1 AND receipt_id = $2 AND claim_token = $3 AND status = 'claimed'`, tenantID, receiptID, claimToken, ackDisposition(result.Disposition), result.RouteVersion, result.EventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return pgx.ErrNoRows
		}
		return nil
	})
}

func (s *Store) Retry(ctx context.Context, tenantID, receiptID, claimToken string, cause error) error {
	message := cause.Error()
	if len(message) > 512 {
		message = message[:512]
	}
	return s.withTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
UPDATE delivery_ack_outbox
SET status = 'pending', attempts = attempts + 1, next_attempt_at = clock_timestamp(),
    last_error = $4, claim_token = NULL, claim_until = NULL
WHERE tenant_id = $1 AND receipt_id = $2 AND claim_token = $3 AND status = 'claimed'`, tenantID, receiptID, claimToken, message)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return pgx.ErrNoRows
		}
		return nil
	})
}

func ackDisposition(disposition delivery.AckDisposition) string {
	switch disposition {
	case delivery.AckApplied:
		return "APPLIED"
	case delivery.AckAlreadyApplied:
		return "ALREADY_APPLIED"
	case delivery.AckTerminalExpired:
		return "TERMINAL_EXPIRED"
	default:
		return "UNKNOWN"
	}
}

func (s *Store) withTenantTx(ctx context.Context, tenantID string, operation func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
		return err
	}
	if err = operation(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func marshalFact(fact routeprojection.RouteFact) ([]byte, error) {
	return json.Marshal(fact)
}

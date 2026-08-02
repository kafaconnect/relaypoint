package deliverypg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kafaconnect/relaypoint/internal/routeprojection"
)

func (s *Store) Load(ctx context.Context, tenantID, interactionID string, version uint64) (routeprojection.Projection, *routeprojection.Identity, error) {
	projection := routeprojection.Projection{TenantID: tenantID, InteractionID: interactionID}
	var retained *routeprojection.Identity
	err := s.withTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&projection.DatabaseNow); err != nil {
			return err
		}
		var hash []byte
		var visibility string
		var leaseUntil *time.Time
		err := tx.QueryRow(ctx, `
SELECT route_version, COALESCE(event_id::text, ''), fact_hash, visibility,
       COALESCE(delivery_authorization_id::text, ''), COALESCE(receipt_id::text, ''),
       visibility_generation, lease_until
FROM route_projection_heads
WHERE tenant_id = $1 AND interaction_id = $2`, tenantID, interactionID).Scan(
			&projection.Version, &projection.EventID, &hash, &visibility,
			&projection.DeliveryAuthorizationID, &projection.ReceiptID, &projection.VisibilityGeneration, &leaseUntil,
		)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		copy(projection.Hash[:], hash)
		if visibility == "visible" {
			projection.Visibility = routeprojection.Visible
		}
		if leaseUntil != nil {
			projection.LeaseUntil = *leaseUntil
		}
		if version == 0 {
			return nil
		}
		identity := routeprojection.Identity{}
		var identityHash []byte
		err = tx.QueryRow(ctx, `
SELECT event_id::text, fact_hash
FROM route_fact_version_ledger
WHERE tenant_id = $1 AND interaction_id = $2 AND route_version = $3`, tenantID, interactionID, version).Scan(&identity.EventID, &identityHash)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		copy(identity.Hash[:], identityHash)
		retained = &identity
		return nil
	})
	if err != nil {
		return routeprojection.Projection{}, nil, fmt.Errorf("load route projection: %w", err)
	}
	return projection, retained, nil
}

func (s *Store) Authorization(ctx context.Context, tenantID, authorizationID string) (routeprojection.AuthorizationBinding, bool, error) {
	var binding routeprojection.AuthorizationBinding
	err := s.withTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
SELECT interaction_id::text, receipt_id::text
FROM delivery_authorizations
WHERE tenant_id = $1 AND delivery_authorization_id = $2 AND visibility = 'PENDING_NOT_VISIBLE'`, tenantID, authorizationID).Scan(&binding.InteractionID, &binding.ReceiptID)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return routeprojection.AuthorizationBinding{}, false, nil
	}
	if err != nil {
		return routeprojection.AuthorizationBinding{}, false, fmt.Errorf("load delivery authorization binding: %w", err)
	}
	return binding, true, nil
}

func (s *Store) InstallBatch(ctx context.Context, expected uint64, facts []routeprojection.RouteFact, final routeprojection.FoldedState) (bool, error) {
	if len(facts) == 0 {
		return false, routeprojection.ErrInvalidReplay
	}
	installed := false
	err := s.withTenantTx(ctx, facts[0].TenantID, func(tx pgx.Tx) error {
		ok, err := s.installBatchTx(ctx, tx, facts[0].TenantID, facts[0].InteractionID, expected, facts, final, "")
		installed = ok
		return err
	})
	return installed, err
}

func (s *Store) InstallReconcileBatch(ctx context.Context, intent routeprojection.ReconcileIntent, facts []routeprojection.RouteFact, final routeprojection.FoldedState) (bool, error) {
	if len(facts) == 0 {
		return false, routeprojection.ErrInvalidReplay
	}
	installed := false
	err := s.withTenantTx(ctx, intent.TenantID, func(tx pgx.Tx) error {
		ok, err := s.installBatchTx(ctx, tx, intent.TenantID, intent.InteractionID, intent.ObservedVersion, facts, final, intent.Token)
		installed = ok
		return err
	})
	return installed, err
}

func (s *Store) installBatchTx(ctx context.Context, tx pgx.Tx, tenantID, interactionID string, expected uint64, facts []routeprojection.RouteFact, final routeprojection.FoldedState, reconcileToken string) (bool, error) {
	if err := ensureProjectionHead(ctx, tx, tenantID, interactionID); err != nil {
		return false, err
	}
	var current uint64
	if err := tx.QueryRow(ctx, `SELECT route_version FROM route_projection_heads WHERE tenant_id = $1 AND interaction_id = $2 FOR UPDATE`, tenantID, interactionID).Scan(&current); err != nil {
		return false, err
	}
	if current != expected {
		return false, nil
	}
	if reconcileToken != "" {
		ok, err := pendingIntentMatches(ctx, tx, tenantID, reconcileToken, interactionID, expected)
		if err != nil || !ok {
			return false, err
		}
	}
	for _, fact := range facts {
		if err := insertFactLedgers(ctx, tx, fact); err != nil {
			return false, err
		}
	}
	if _, err := tx.Exec(ctx, `
UPDATE route_projection_heads
SET route_version = $3, event_id = $4, fact_hash = $5, visibility = $6,
    delivery_authorization_id = NULLIF($7, '')::uuid, receipt_id = NULLIF($8, '')::uuid,
    visibility_generation = $9, lease_until = $10, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND interaction_id = $2`, tenantID, interactionID, final.Version, final.EventID, final.Hash[:], visibilityName(final.Visibility), final.DeliveryAuthorizationID, final.ReceiptID, final.VisibilityGeneration, nullableTime(final.LeaseUntil)); err != nil {
		return false, err
	}
	if reconcileToken != "" {
		if _, err := tx.Exec(ctx, `
UPDATE projection_reconcile_intents
SET status = 'installed', completed_at = clock_timestamp()
WHERE tenant_id = $1 AND reconcile_token = $2 AND status = 'pending'`, tenantID, reconcileToken); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (s *Store) BeginReconcile(ctx context.Context, proposed routeprojection.ReconcileIntent) (routeprojection.ReconcileIntent, bool, error) {
	retained := proposed
	started := false
	err := s.withTenantTx(ctx, proposed.TenantID, func(tx pgx.Tx) error {
		if err := ensureProjectionHead(ctx, tx, proposed.TenantID, proposed.InteractionID); err != nil {
			return err
		}
		var current uint64
		if err := tx.QueryRow(ctx, `SELECT route_version FROM route_projection_heads WHERE tenant_id = $1 AND interaction_id = $2 FOR UPDATE`, proposed.TenantID, proposed.InteractionID).Scan(&current); err != nil {
			return err
		}
		if current != proposed.ObservedVersion {
			return nil
		}
		var hash []byte
		err := tx.QueryRow(ctx, `
SELECT reconcile_token::text, observed_version, requested_from, requested_to, held_event_id::text, held_fact_hash
FROM projection_reconcile_intents
WHERE tenant_id = $1 AND interaction_id = $2 AND status = 'pending'`, proposed.TenantID, proposed.InteractionID).Scan(
			&retained.Token, &retained.ObservedVersion, &retained.RequestedFrom, &retained.RequestedTo, &retained.HeldEventID, &hash,
		)
		if err == nil {
			retained.TenantID = proposed.TenantID
			retained.InteractionID = proposed.InteractionID
			copy(retained.HeldHash[:], hash)
			started = retained.ObservedVersion == proposed.ObservedVersion && retained.RequestedFrom == proposed.RequestedFrom && retained.RequestedTo == proposed.RequestedTo && retained.HeldEventID == proposed.HeldEventID && retained.HeldHash == proposed.HeldHash
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		_, err = tx.Exec(ctx, `
INSERT INTO projection_reconcile_intents (
    tenant_id, reconcile_token, interaction_id, observed_version, requested_from, requested_to, held_event_id, held_fact_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, proposed.TenantID, proposed.Token, proposed.InteractionID, proposed.ObservedVersion, proposed.RequestedFrom, proposed.RequestedTo, proposed.HeldEventID, proposed.HeldHash[:])
		started = err == nil
		return err
	})
	if err != nil {
		return routeprojection.ReconcileIntent{}, false, fmt.Errorf("begin route reconciliation: %w", err)
	}
	return retained, started, nil
}

func (s *Store) InstallHistoricalIdentity(ctx context.Context, intent routeprojection.ReconcileIntent, fact routeprojection.RouteFact) (bool, error) {
	installed := false
	err := s.withTenantTx(ctx, intent.TenantID, func(tx pgx.Tx) error {
		if err := ensureProjectionHead(ctx, tx, intent.TenantID, intent.InteractionID); err != nil {
			return err
		}
		var current uint64
		if err := tx.QueryRow(ctx, `SELECT route_version FROM route_projection_heads WHERE tenant_id = $1 AND interaction_id = $2 FOR UPDATE`, intent.TenantID, intent.InteractionID).Scan(&current); err != nil {
			return err
		}
		if current != intent.ObservedVersion {
			return nil
		}
		ok, err := pendingIntentMatches(ctx, tx, intent.TenantID, intent.Token, intent.InteractionID, intent.ObservedVersion)
		if err != nil || !ok {
			return err
		}
		if err := insertFactLedgers(ctx, tx, fact); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE projection_reconcile_intents SET status = 'installed', completed_at = clock_timestamp() WHERE tenant_id = $1 AND reconcile_token = $2`, intent.TenantID, intent.Token)
		installed = err == nil
		return err
	})
	return installed, err
}

func (s *Store) InstallSnapshotAndAudit(ctx context.Context, intent routeprojection.ReconcileIntent, snapshot routeprojection.Snapshot, held routeprojection.RouteFact) (bool, error) {
	installed := false
	err := s.withTenantTx(ctx, intent.TenantID, func(tx pgx.Tx) error {
		if err := ensureProjectionHead(ctx, tx, intent.TenantID, intent.InteractionID); err != nil {
			return err
		}
		var current uint64
		if err := tx.QueryRow(ctx, `SELECT route_version FROM route_projection_heads WHERE tenant_id = $1 AND interaction_id = $2 FOR UPDATE`, intent.TenantID, intent.InteractionID).Scan(&current); err != nil {
			return err
		}
		if current != intent.ObservedVersion {
			return nil
		}
		ok, err := pendingIntentMatches(ctx, tx, intent.TenantID, intent.Token, intent.InteractionID, intent.ObservedVersion)
		if err != nil || !ok {
			return err
		}
		projection := snapshot.Projection
		if projection.TenantID == "" {
			projection.TenantID, projection.InteractionID = intent.TenantID, intent.InteractionID
		}
		if _, err := tx.Exec(ctx, `
UPDATE route_projection_heads
SET route_version = $3, event_id = $4, fact_hash = $5, visibility = $6,
    delivery_authorization_id = NULLIF($7, '')::uuid, receipt_id = NULLIF($8, '')::uuid,
    visibility_generation = $9, lease_until = $10, snapshot_history_floor = $11,
    snapshot_provenance = $12, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND interaction_id = $2`, intent.TenantID, intent.InteractionID, projection.Version, projection.EventID, projection.Hash[:], visibilityName(projection.Visibility), projection.DeliveryAuthorizationID, projection.ReceiptID, projection.VisibilityGeneration, nullableTime(projection.LeaseUntil), snapshot.HistoryFloor, snapshot.Provenance); err != nil {
			return err
		}
		snapshotBody, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `
INSERT INTO route_fact_version_ledger (tenant_id, interaction_id, route_version, event_id, fact_hash, fact_body)
VALUES ($1,$2,$3,$4,$5,$6)`, intent.TenantID, intent.InteractionID, projection.Version, projection.EventID, projection.Hash[:], snapshotBody); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `
INSERT INTO route_fact_event_ledger (tenant_id, event_id, interaction_id, route_version, fact_hash)
VALUES ($1,$2,$3,$4,$5)`, intent.TenantID, projection.EventID, intent.InteractionID, projection.Version, projection.Hash[:]); err != nil {
			return err
		}
		heldBody, err := marshalFact(held)
		if err != nil {
			return err
		}
		dlqID, alertID := s.newID(), s.newID()
		heldHash := routeprojection.HashRouteFact(held)
		if _, err = tx.Exec(ctx, `
INSERT INTO route_projection_dlq (tenant_id, dlq_id, reconcile_token, interaction_id, event_id, route_version, fact_hash, reason, fact_body)
VALUES ($1,$2,$3,$4,$5,$6,$7,'UNKNOWN_STALE_HISTORY',$8)
ON CONFLICT (tenant_id, interaction_id, event_id, reason) DO NOTHING`, intent.TenantID, dlqID, intent.Token, intent.InteractionID, held.EventID, held.Version, heldHash[:], heldBody); err != nil {
			return err
		}
		alertBody, err := jsonMarshalAlert(snapshot, held)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `
INSERT INTO projection_invariant_alert_outbox (tenant_id, alert_id, reconcile_token, interaction_id, reason, payload)
VALUES ($1,$2,$3,$4,'UNKNOWN_STALE_HISTORY',$5)
ON CONFLICT (tenant_id, reconcile_token) DO NOTHING`, intent.TenantID, alertID, intent.Token, intent.InteractionID, alertBody); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE projection_reconcile_intents SET status = 'audited', completed_at = clock_timestamp() WHERE tenant_id = $1 AND reconcile_token = $2 AND status = 'pending'`, intent.TenantID, intent.Token)
		installed = err == nil
		return err
	})
	return installed, err
}

func (s *Store) RecordPoison(ctx context.Context, fact routeprojection.RouteFact, reason string) error {
	body, err := marshalFact(fact)
	if err != nil {
		return err
	}
	hash := routeprojection.HashRouteFact(fact)
	return s.withTenantTx(ctx, fact.TenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO route_projection_dlq (tenant_id, dlq_id, interaction_id, event_id, route_version, fact_hash, reason, fact_body)
VALUES ($1,$2,$3,$4,$5,$6,'DIVERGENT_HISTORY',$7)
ON CONFLICT (tenant_id, interaction_id, event_id, reason) DO NOTHING`, fact.TenantID, s.newID(), fact.InteractionID, fact.EventID, fact.Version, hash[:], body)
		return err
	})
}

func ensureProjectionHead(ctx context.Context, tx pgx.Tx, tenantID, interactionID string) error {
	_, err := tx.Exec(ctx, `INSERT INTO route_projection_heads (tenant_id, interaction_id, route_version) VALUES ($1,$2,0) ON CONFLICT (tenant_id, interaction_id) DO NOTHING`, tenantID, interactionID)
	return err
}

func insertFactLedgers(ctx context.Context, tx pgx.Tx, fact routeprojection.RouteFact) error {
	body, err := marshalFact(fact)
	if err != nil {
		return err
	}
	hash := routeprojection.HashRouteFact(fact)
	if _, err = tx.Exec(ctx, `
INSERT INTO route_fact_version_ledger (tenant_id, interaction_id, route_version, event_id, fact_hash, fact_body)
VALUES ($1,$2,$3,$4,$5,$6)`, fact.TenantID, fact.InteractionID, fact.Version, fact.EventID, hash[:], body); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
INSERT INTO route_fact_event_ledger (tenant_id, event_id, interaction_id, route_version, fact_hash)
VALUES ($1,$2,$3,$4,$5)`, fact.TenantID, fact.EventID, fact.InteractionID, fact.Version, hash[:])
	return err
}

func pendingIntentMatches(ctx context.Context, tx pgx.Tx, tenantID, token, interactionID string, observed uint64) (bool, error) {
	var found bool
	err := tx.QueryRow(ctx, `
SELECT true
FROM projection_reconcile_intents
WHERE tenant_id = $1 AND reconcile_token = $2 AND interaction_id = $3 AND observed_version = $4 AND status = 'pending'`, tenantID, token, interactionID, observed).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return found, err
}

func visibilityName(visibility routeprojection.Visibility) string {
	if visibility == routeprojection.Visible {
		return "visible"
	}
	return "hidden"
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func jsonMarshalAlert(snapshot routeprojection.Snapshot, held routeprojection.RouteFact) ([]byte, error) {
	return marshalJSON(struct {
		Snapshot routeprojection.Snapshot  `json:"snapshot"`
		Held     routeprojection.RouteFact `json:"held"`
	}{Snapshot: snapshot, Held: held})
}

func marshalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

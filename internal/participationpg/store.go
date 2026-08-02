package participationpg

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/participation"
)

type Store struct {
	pool  *pgxpool.Pool
	newID func() string
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, newID: func() string { return uuid.Must(uuid.NewV7()).String() }}
}

func (s *Store) Load(ctx context.Context, key participation.Key, version uint64) (participation.Fold, *participation.Identity, error) {
	var fold participation.Fold
	var identity *participation.Identity
	err := s.withTenantTx(ctx, key.TenantID, func(tx pgx.Tx) error {
		var err error
		fold, err = loadFold(ctx, tx, key)
		if err != nil {
			return err
		}
		if version == 0 {
			return nil
		}
		var eventID string
		var hash, body []byte
		err = tx.QueryRow(ctx, `
			SELECT event_id::text, command_hash, command_body
			FROM participation_version_ledger
			WHERE tenant_id=$1 AND interaction_id=$2 AND aggregate_version=$3
		`, key.TenantID, key.InteractionID, version).Scan(&eventID, &hash, &body)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil || len(hash) != 32 {
			return err
		}
		value := participation.Identity{EventID: eventID, Body: append([]byte(nil), body...)}
		copy(value.Hash[:], hash)
		identity = &value
		return nil
	})
	return fold, identity, err
}

func (s *Store) Install(ctx context.Context, expected uint64, records []participation.Record, final participation.Fold) (bool, error) {
	installed := false
	err := s.withTenantTx(ctx, final.TenantID, func(tx pgx.Tx) error {
		current, err := lockHead(ctx, tx, participation.Key{TenantID: final.TenantID, InteractionID: final.InteractionID})
		if err != nil || current != expected {
			return err
		}
		if err := installRecords(ctx, tx, records, final); err != nil {
			return err
		}
		installed = true
		return nil
	})
	return installed, err
}

func (s *Store) BeginReconcile(ctx context.Context, intent participation.Intent, held participation.Record) (participation.Intent, bool, error) {
	started := false
	err := s.withTenantTx(ctx, intent.Key.TenantID, func(tx pgx.Tx) error {
		current, err := lockHead(ctx, tx, intent.Key)
		if err != nil || current != intent.ObservedVersion {
			return err
		}
		var token string
		var observed, from, to uint64
		var heldEventID string
		var heldHash []byte
		err = tx.QueryRow(ctx, `
			SELECT reconcile_token::text, observed_version, requested_from, requested_to,
				held_event_id::text, held_hash
			FROM participation_reconcile_intents
			WHERE tenant_id=$1 AND interaction_id=$2 AND status='pending'
		`, intent.Key.TenantID, intent.Key.InteractionID).Scan(&token, &observed, &from, &to, &heldEventID, &heldHash)
		if err == nil {
			if observed != intent.ObservedVersion || heldEventID != held.EventID || len(heldHash) != len(held.Hash) || !bytesEqual(heldHash, held.Hash[:]) {
				return participation.ErrDivergentHistory
			}
			intent.Token, intent.ObservedVersion, intent.RequestedFrom, intent.RequestedTo = token, observed, from, to
			started = true
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO participation_pending_inputs (
				tenant_id, event_id, interaction_id, aggregate_version, command_hash, command_body
			) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING
		`, intent.Key.TenantID, held.EventID, intent.Key.InteractionID,
			held.Command.GetAggregateVersion(), held.Hash[:], held.Body)
		if err != nil || tag.RowsAffected() != 1 {
			return err
		}
		tag, err = tx.Exec(ctx, `
			INSERT INTO participation_reconcile_intents (
				tenant_id, reconcile_token, interaction_id, observed_version,
				requested_from, requested_to, held_event_id, held_hash
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING
		`, intent.Key.TenantID, intent.Token, intent.Key.InteractionID, intent.ObservedVersion,
			intent.RequestedFrom, intent.RequestedTo, held.EventID, held.Hash[:])
		if err != nil || tag.RowsAffected() != 1 {
			return err
		}
		started = true
		return nil
	})
	return intent, started, err
}

func (s *Store) InstallReconcile(ctx context.Context, intent participation.Intent, records []participation.Record, final participation.Fold) (bool, error) {
	installed := false
	err := s.withTenantTx(ctx, intent.Key.TenantID, func(tx pgx.Tx) error {
		ok, err := lockIntentAndHead(ctx, tx, intent)
		if err != nil || !ok {
			return err
		}
		if err := installRecords(ctx, tx, records, final); err != nil {
			return err
		}
		if err := completeReconcile(ctx, tx, intent, "installed", "installed"); err != nil {
			return err
		}
		installed = true
		return nil
	})
	return installed, err
}

func (s *Store) InstallHistorical(ctx context.Context, intent participation.Intent, record participation.Record) (bool, error) {
	installed := false
	err := s.withTenantTx(ctx, intent.Key.TenantID, func(tx pgx.Tx) error {
		ok, err := lockIntentAndHead(ctx, tx, intent)
		if err != nil || !ok {
			return err
		}
		if err := insertLedgers(ctx, tx, record, "command"); err != nil {
			return err
		}
		if err := completeReconcile(ctx, tx, intent, "installed", "installed"); err != nil {
			return err
		}
		installed = true
		return nil
	})
	return installed, err
}

func (s *Store) InstallSnapshot(ctx context.Context, intent participation.Intent, snapshot participation.Snapshot, held participation.Record) (bool, error) {
	installed := false
	err := s.withTenantTx(ctx, intent.Key.TenantID, func(tx pgx.Tx) error {
		ok, err := lockIntentAndHead(ctx, tx, intent)
		if err != nil || !ok {
			return err
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM participation_folded_participants
			WHERE tenant_id=$1 AND interaction_id=$2
		`, intent.Key.TenantID, intent.Key.InteractionID); err != nil {
			return err
		}
		for participantID := range snapshot.Fold.Participants {
			if _, err := tx.Exec(ctx, `
				INSERT INTO participation_folded_participants (
					tenant_id, interaction_id, participant_id, aggregate_version, event_id, command_hash
				) VALUES ($1,$2,$3,$4,$5,$6)
			`, intent.Key.TenantID, intent.Key.InteractionID, participantID, snapshot.Fold.Version,
				snapshot.Fold.Identity.EventID, snapshot.Fold.Identity.Hash[:]); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO participation_version_ledger (
				tenant_id, interaction_id, aggregate_version, event_id, command_hash, source_kind
			) VALUES ($1,$2,$3,$4,$5,'snapshot')
		`, intent.Key.TenantID, intent.Key.InteractionID, snapshot.Fold.Version,
			snapshot.Fold.Identity.EventID, snapshot.Fold.Identity.Hash[:]); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO participation_event_ledger (
				tenant_id, event_id, interaction_id, aggregate_version, command_hash
			) VALUES ($1,$2,$3,$4,$5)
		`, intent.Key.TenantID, snapshot.Fold.Identity.EventID, intent.Key.InteractionID,
			snapshot.Fold.Version, snapshot.Fold.Identity.Hash[:]); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE participation_fold_heads SET aggregate_version=$3, event_id=$4, command_hash=$5,
				snapshot_source_event_id=$4, snapshot_source_hash=$5, snapshot_provenance=$6,
				updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND interaction_id=$2 AND aggregate_version=$7
		`, intent.Key.TenantID, intent.Key.InteractionID, snapshot.Fold.Version,
			snapshot.Fold.Identity.EventID, snapshot.Fold.Identity.Hash[:], snapshot.Provenance,
			intent.ObservedVersion); err != nil {
			return err
		}
		if err := s.insertAudit(ctx, tx, intent, held, "UNKNOWN_STALE_HISTORY"); err != nil {
			return err
		}
		if err := completeReconcile(ctx, tx, intent, "audited", "dlq"); err != nil {
			return err
		}
		installed = true
		return nil
	})
	return installed, err
}

func (s *Store) RecordPoison(ctx context.Context, record participation.Record, reason string) error {
	intent := participation.Intent{Key: participation.Key{TenantID: record.Command.GetTenantId(), InteractionID: record.Command.GetInteractionId()}}
	return s.withTenantTx(ctx, intent.Key.TenantID, func(tx pgx.Tx) error {
		return s.insertAudit(ctx, tx, intent, record, reason)
	})
}

func (s *Store) FailReconcile(ctx context.Context, intent participation.Intent, held participation.Record, _ error) (bool, error) {
	exhausted := false
	err := s.withTenantTx(ctx, intent.Key.TenantID, func(tx pgx.Tx) error {
		ok, err := lockIntentAndHead(ctx, tx, intent)
		if err != nil || !ok {
			return err
		}
		var attempts int
		if err := tx.QueryRow(ctx, `
			SELECT attempts FROM participation_pending_inputs
			WHERE tenant_id=$1 AND event_id=$2 AND status='pending' FOR UPDATE
		`, intent.Key.TenantID, held.EventID).Scan(&attempts); err != nil {
			return err
		}
		attempts++
		if attempts < 3 {
			_, err := tx.Exec(ctx, `
				UPDATE participation_pending_inputs SET attempts=$3
				WHERE tenant_id=$1 AND event_id=$2 AND status='pending'
			`, intent.Key.TenantID, held.EventID, attempts)
			return err
		}
		if err := s.insertAudit(ctx, tx, intent, held, "RECONCILE_EXHAUSTED"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE participation_pending_inputs SET attempts=$3, status='dlq', completed_at=clock_timestamp()
			WHERE tenant_id=$1 AND event_id=$2 AND status='pending'
		`, intent.Key.TenantID, held.EventID, attempts); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE participation_reconcile_intents SET status='audited', completed_at=clock_timestamp()
			WHERE tenant_id=$1 AND reconcile_token=$2 AND status='pending'
		`, intent.Key.TenantID, intent.Token); err != nil {
			return err
		}
		exhausted = true
		return nil
	})
	return exhausted, err
}

func (s *Store) insertAudit(ctx context.Context, tx pgx.Tx, intent participation.Intent, record participation.Record, reason string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO participation_dlq (
			tenant_id, dlq_id, reconcile_token, interaction_id, event_id,
			aggregate_version, command_hash, reason, command_body
		) VALUES ($1,$2,NULLIF($3,'')::uuid,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (tenant_id, interaction_id, event_id, reason) DO NOTHING
	`, intent.Key.TenantID, s.newID(), intent.Token, intent.Key.InteractionID, record.EventID,
		record.Command.GetAggregateVersion(), record.Hash[:], reason, record.Body); err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{
		"event_id": record.EventID, "aggregate_version": record.Command.GetAggregateVersion(), "reason": reason,
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO participation_alert_outbox (
			tenant_id, alert_id, reconcile_token, interaction_id, event_id, reason, payload
		) VALUES ($1,$2,NULLIF($3,'')::uuid,$4,$5,$6,$7)
		ON CONFLICT (tenant_id, interaction_id, event_id, reason) DO NOTHING
	`, intent.Key.TenantID, s.newID(), intent.Token, intent.Key.InteractionID, record.EventID, reason, payload)
	return err
}

func (s *Store) withTenantTx(ctx context.Context, tenantID string, operation func(pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return errors.New("participation postgres unavailable")
	}
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

func loadFold(ctx context.Context, tx pgx.Tx, key participation.Key) (participation.Fold, error) {
	fold := participation.Fold{TenantID: key.TenantID, InteractionID: key.InteractionID, Participants: map[string]struct{}{}}
	var hash []byte
	var eventID, provenance *string
	err := tx.QueryRow(ctx, `
		SELECT aggregate_version, event_id::text, command_hash, snapshot_provenance
		FROM participation_fold_heads WHERE tenant_id=$1 AND interaction_id=$2
	`, key.TenantID, key.InteractionID).Scan(&fold.Version, &eventID, &hash, &provenance)
	if errors.Is(err, pgx.ErrNoRows) {
		return fold, nil
	}
	if err != nil {
		return participation.Fold{}, err
	}
	if fold.Version > 0 {
		if eventID == nil || len(hash) != 32 {
			return participation.Fold{}, participation.ErrInvalid
		}
		fold.Identity.EventID = *eventID
		copy(fold.Identity.Hash[:], hash)
	}
	if provenance != nil {
		fold.SnapshotProvenance = *provenance
	}
	rows, err := tx.Query(ctx, `
		SELECT participant_id::text FROM participation_folded_participants
		WHERE tenant_id=$1 AND interaction_id=$2
	`, key.TenantID, key.InteractionID)
	if err != nil {
		return participation.Fold{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var participantID string
		if err := rows.Scan(&participantID); err != nil {
			return participation.Fold{}, err
		}
		fold.Participants[participantID] = struct{}{}
	}
	return fold, rows.Err()
}

func lockHead(ctx context.Context, tx pgx.Tx, key participation.Key) (uint64, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO participation_fold_heads (tenant_id, interaction_id)
		VALUES ($1,$2) ON CONFLICT (tenant_id, interaction_id) DO NOTHING
	`, key.TenantID, key.InteractionID); err != nil {
		return 0, err
	}
	var version uint64
	err := tx.QueryRow(ctx, `
		SELECT aggregate_version FROM participation_fold_heads
		WHERE tenant_id=$1 AND interaction_id=$2 FOR UPDATE
	`, key.TenantID, key.InteractionID).Scan(&version)
	return version, err
}

func lockIntentAndHead(ctx context.Context, tx pgx.Tx, intent participation.Intent) (bool, error) {
	current, err := lockHead(ctx, tx, intent.Key)
	if err != nil || current != intent.ObservedVersion {
		return false, err
	}
	var found bool
	err = tx.QueryRow(ctx, `
		SELECT true FROM participation_reconcile_intents
		WHERE tenant_id=$1 AND reconcile_token=$2 AND interaction_id=$3
			AND observed_version=$4 AND status='pending' FOR UPDATE
	`, intent.Key.TenantID, intent.Token, intent.Key.InteractionID, intent.ObservedVersion).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return found, err
}

func installRecords(ctx context.Context, tx pgx.Tx, records []participation.Record, final participation.Fold) error {
	for _, record := range records {
		if err := insertLedgers(ctx, tx, record, "command"); err != nil {
			return err
		}
		command := record.Command
		if command.GetDesiredState() == interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED {
			if _, err := tx.Exec(ctx, `
				INSERT INTO participation_folded_participants (
					tenant_id, interaction_id, participant_id, aggregate_version, event_id, command_hash
				) VALUES ($1,$2,$3,$4,$5,$6)
				ON CONFLICT (tenant_id, interaction_id, participant_id) DO UPDATE SET
					aggregate_version=EXCLUDED.aggregate_version, event_id=EXCLUDED.event_id,
					command_hash=EXCLUDED.command_hash, updated_at=clock_timestamp()
			`, command.GetTenantId(), command.GetInteractionId(), command.GetParticipantId(),
				command.GetAggregateVersion(), command.GetEventId(), record.Hash[:]); err != nil {
				return err
			}
		} else if _, err := tx.Exec(ctx, `
			DELETE FROM participation_folded_participants
			WHERE tenant_id=$1 AND interaction_id=$2 AND participant_id=$3
		`, command.GetTenantId(), command.GetInteractionId(), command.GetParticipantId()); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `
		UPDATE participation_fold_heads SET aggregate_version=$3, event_id=$4, command_hash=$5,
			snapshot_source_event_id=NULL, snapshot_source_hash=NULL, snapshot_provenance=NULL,
			updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND interaction_id=$2
	`, final.TenantID, final.InteractionID, final.Version, final.Identity.EventID, final.Identity.Hash[:])
	return err
}

func insertLedgers(ctx context.Context, tx pgx.Tx, record participation.Record, source string) error {
	body := any(record.Body)
	if source == "snapshot" {
		body = nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO participation_version_ledger (
			tenant_id, interaction_id, aggregate_version, event_id, command_hash, command_body, source_kind
		) VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, record.Command.GetTenantId(), record.Command.GetInteractionId(), record.Command.GetAggregateVersion(),
		record.EventID, record.Hash[:], body, source); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO participation_event_ledger (
			tenant_id, event_id, interaction_id, aggregate_version, command_hash
		) VALUES ($1,$2,$3,$4,$5)
	`, record.Command.GetTenantId(), record.EventID, record.Command.GetInteractionId(),
		record.Command.GetAggregateVersion(), record.Hash[:])
	return err
}

func completeReconcile(ctx context.Context, tx pgx.Tx, intent participation.Intent, intentStatus, pendingStatus string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE participation_reconcile_intents SET status=$3, completed_at=clock_timestamp()
		WHERE tenant_id=$1 AND reconcile_token=$2 AND status='pending'
	`, intent.Key.TenantID, intent.Token, intentStatus); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE participation_pending_inputs SET status=$3, completed_at=clock_timestamp()
		WHERE tenant_id=$1 AND event_id=$2 AND status='pending'
	`, intent.Key.TenantID, intent.Held.EventID, pendingStatus)
	return err
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

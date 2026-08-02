package participationpg

import (
	"os"
	"strings"
	"testing"
)

// @spec:service-extraction.participation.replay-and-reconcile
func TestBaselineOwnsParticipationFoldRecoveryAndAuditWithForceRLS(t *testing.T) {
	body, err := os.ReadFile("../../db/migrations/000001_baseline.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, table := range []string{
		"participation_fold_heads", "participation_folded_participants",
		"participation_version_ledger", "participation_event_ledger",
		"participation_pending_inputs", "participation_reconcile_intents",
		"participation_dlq", "participation_alert_outbox",
	} {
		if !strings.Contains(sql, "create table "+table) ||
			!strings.Contains(sql, "alter table "+table+" force row level security") ||
			!strings.Contains(sql, "create policy "+table+"_tenant") {
			t.Errorf("missing tenant FORCE-RLS table %s", table)
		}
	}
	for _, identity := range []string{
		"primary key (tenant_id, interaction_id, aggregate_version)",
		"primary key (tenant_id, event_id)",
	} {
		if !strings.Contains(sql, identity) {
			t.Errorf("missing immutable participation identity %s", identity)
		}
	}
}

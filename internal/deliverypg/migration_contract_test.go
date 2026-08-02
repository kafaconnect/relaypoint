package deliverypg

import (
	"os"
	"strings"
	"testing"
)

func TestBaselineOwnsDurableDeliveryAndProjectionStateWithForceRLS(t *testing.T) {
	body, err := os.ReadFile("../../db/migrations/000001_baseline.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(body))
	for _, table := range []string{
		"delivery_authorizations",
		"interaction_delivery_log",
		"delivery_ack_outbox",
		"route_projection_heads",
		"route_fact_version_ledger",
		"route_fact_event_ledger",
		"projection_reconcile_intents",
		"route_projection_dlq",
		"projection_invariant_alert_outbox",
	} {
		if !strings.Contains(sql, "create table "+table) {
			t.Errorf("missing table %s", table)
		}
		if !strings.Contains(sql, "alter table "+table+" force row level security") {
			t.Errorf("missing FORCE RLS for %s", table)
		}
	}
	if strings.Contains(sql, "desk.") || strings.Contains(sql, "router.") || strings.Contains(sql, "references reservations") {
		t.Fatal("baseline contains cross-owner SQL")
	}
	for _, identity := range []string{
		"primary key (tenant_id, interaction_id, route_version)",
		"primary key (tenant_id, event_id)",
		"primary key (tenant_id, receipt_id)",
	} {
		if !strings.Contains(sql, identity) {
			t.Errorf("missing immutable identity %s", identity)
		}
	}
}

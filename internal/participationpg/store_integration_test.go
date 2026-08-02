//go:build integration

package participationpg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/participation"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	integrationTenant      = "018f5000-0000-7000-8000-000000000001"
	integrationOtherTenant = "018f5000-0000-7000-8000-000000000002"
	integrationInteraction = "018f5000-0000-7000-8000-000000000003"
	integrationAlice       = "018f5000-0000-7000-8000-000000000004"
	integrationBob         = "018f5000-0000-7000-8000-000000000005"
)

type integrationAuthority struct {
	pool       *pgxpool.Pool
	replay     *interactionv1.ReplayParticipationResponse
	replayErr  error
	snapshot   *interactionv1.GetDesiredParticipationSnapshotResponse
	lockFailed bool
}

func (a *integrationAuthority) Replay(ctx context.Context, _ participation.Principal, _ *interactionv1.ReplayParticipationRequest) (*interactionv1.ReplayParticipationResponse, error) {
	a.checkUnlocked(ctx)
	return a.replay, a.replayErr
}

func (a *integrationAuthority) Snapshot(ctx context.Context, _ participation.Principal, _ *interactionv1.GetDesiredParticipationSnapshotRequest) (*interactionv1.GetDesiredParticipationSnapshotResponse, error) {
	a.checkUnlocked(ctx)
	return a.snapshot, nil
}

func (a *integrationAuthority) checkUnlocked(ctx context.Context) {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		a.lockFailed = true
		return
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`, integrationTenant); err != nil {
		a.lockFailed = true
		return
	}
	if _, err := tx.Exec(ctx, `
		SELECT aggregate_version FROM participation_fold_heads
		WHERE tenant_id=$1 AND interaction_id=$2 FOR UPDATE NOWAIT
	`, integrationTenant, integrationInteraction); err != nil {
		a.lockFailed = true
	}
}

// @spec:service-extraction.participation.replay-and-reconcile
// @spec:service-extraction.participation.outside-lock-reconciliation
func TestPostgresParticipationExactNextReplaySnapshotCASAndRLS(t *testing.T) {
	admin, app := participationPools(t)
	defer admin.Close()
	defer app.Close()
	ctx := context.Background()
	store := New(app)
	ids := []string{
		"018f5000-0000-7000-8000-000000000020",
		"018f5000-0000-7000-8000-000000000021",
	}
	index := 0
	store.newID = func() string {
		id := "018f5000-0000-7000-8000-00000000003" + strconv.Itoa(index)
		index++
		return id
	}
	one := integrationCommand(1, integrationAlice)
	two := integrationCommand(2, integrationBob)
	twoRecord, err := participation.Canonical(two)
	if err != nil {
		t.Fatal(err)
	}
	authority := &integrationAuthority{pool: app, replay: &interactionv1.ReplayParticipationResponse{
		Commands: []*interactionv1.ParticipationCommand{one, two}, HeadVersion: 2,
		HeadEventId: two.GetEventId(), HeadHash: twoRecord.Hash[:], HistoryFloor: 1,
		Provenance: "corex-participation-history-v1",
	}}
	projector := participation.NewProjector(store, authority, func() string {
		id := ids[0]
		ids = ids[1:]
		return id
	})
	result, err := projector.Apply(ctx, participation.Principal{TenantID: integrationTenant, Capability: participation.CapabilityWrite}, two)
	if err != nil || result != participation.Applied || authority.lockFailed {
		t.Fatalf("result=%v lock_failed=%v err=%v", result, authority.lockFailed, err)
	}
	if got := participationTenantCount(t, app, integrationTenant, `SELECT count(*) FROM participation_version_ledger`); got != 2 {
		t.Fatalf("version ledger=%d", got)
	}
	if got := participationTenantCount(t, app, integrationTenant, `SELECT count(*) FROM participation_event_ledger`); got != 2 {
		t.Fatalf("event ledger=%d", got)
	}
	if got := participationTenantCount(t, app, integrationOtherTenant, `SELECT count(*) FROM participation_folded_participants`); got != 0 {
		t.Fatalf("cross-tenant participants=%d", got)
	}
	if _, err := admin.Exec(ctx, `DELETE FROM participation_version_ledger WHERE aggregate_version=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `DELETE FROM participation_event_ledger WHERE aggregate_version=1`); err != nil {
		t.Fatal(err)
	}
	four := integrationCommand(4, integrationBob)
	fourRecord, err := participation.Canonical(four)
	if err != nil {
		t.Fatal(err)
	}
	authority.replay, authority.replayErr = nil, participation.ErrUnknownHistory
	authority.snapshot = &interactionv1.GetDesiredParticipationSnapshotResponse{
		TenantId: integrationTenant, InteractionId: integrationInteraction, AggregateVersion: 4,
		HeadEventId: four.GetEventId(), HeadHash: fourRecord.Hash[:], ParticipantIds: []string{integrationBob},
		HistoryFloor: 1, Provenance: "corex-participation-history-v1",
	}
	result, err = projector.Apply(ctx, participation.Principal{TenantID: integrationTenant, Capability: participation.CapabilityWrite}, one)
	if err != nil || result != participation.AuditedSnapshot || authority.lockFailed {
		t.Fatalf("snapshot result=%v lock_failed=%v err=%v", result, authority.lockFailed, err)
	}
	if got := participationTenantCount(t, app, integrationTenant, `SELECT count(*) FROM participation_folded_participants WHERE participant_id=$1`, integrationBob); got != 1 {
		t.Fatalf("snapshot bob=%d", got)
	}
	if got := participationTenantCount(t, app, integrationTenant, `SELECT count(*) FROM participation_folded_participants WHERE participant_id=$1`, integrationAlice); got != 0 {
		t.Fatalf("snapshot retained alice=%d", got)
	}
	if got := participationTenantCount(t, app, integrationTenant, `SELECT count(*) FROM participation_dlq`); got != 1 {
		t.Fatalf("dlq=%d", got)
	}
	if got := participationTenantCount(t, app, integrationTenant, `SELECT count(*) FROM participation_alert_outbox`); got != 1 {
		t.Fatalf("alerts=%d", got)
	}
}

func integrationCommand(version uint64, participantID string) *interactionv1.ParticipationCommand {
	return &interactionv1.ParticipationCommand{
		EventId: fmt.Sprintf("018f5000-0000-7000-8000-%012d", 10+version), AggregateVersion: version,
		TenantId: integrationTenant, InteractionId: integrationInteraction, ParticipantId: participantID,
		DesiredState: interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED,
		OccurredAt:   timestamppb.New(time.Unix(int64(100+version), 0).UTC()),
		Traceparent:  "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", Capability: participation.CapabilityWrite,
	}
}

func participationPools(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var versionText string
	if err := admin.QueryRow(ctx, `SHOW server_version_num`).Scan(&versionText); err != nil {
		t.Fatal(err)
	}
	version, err := strconv.Atoi(versionText)
	if err != nil || version < 170000 || version >= 180000 {
		t.Fatalf("PostgreSQL 17 required: %s", versionText)
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	down, err := os.ReadFile(filepath.Join(root, "db", "migrations", "000001_baseline.down.sql"))
	if err != nil {
		t.Fatal(err)
	}
	var installed bool
	if err := admin.QueryRow(ctx, `
		SELECT to_regclass('participation_fold_heads') IS NOT NULL
			OR to_regclass('delivery_authorizations') IS NOT NULL
	`).Scan(&installed); err != nil {
		t.Fatal(err)
	}
	if installed {
		if _, err := admin.Exec(ctx, string(down)); err != nil {
			t.Fatal(err)
		}
	}
	up, err := os.ReadFile(filepath.Join(root, "db", "migrations", "000001_baseline.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, string(up)); err != nil {
		t.Fatal(err)
	}
	role := "rp_" + strings.ReplaceAll(uuid.Must(uuid.NewV7()).String(), "-", "")
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'relaypoint_test'", role)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %s", role, role)); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = role, "relaypoint_test"
	app, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	return admin, app
}

func participationTenantCount(t *testing.T, pool *pgxpool.Pool, tenantID, query string, args ...any) int {
	t.Helper()
	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(context.Background(), `SELECT set_config('app.tenant_id',$1,true)`, tenantID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

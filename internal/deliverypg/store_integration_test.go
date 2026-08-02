//go:build integration

package deliverypg

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
	"github.com/kafaconnect/relaypoint/internal/delivery"
	"github.com/kafaconnect/relaypoint/internal/routeprojection"
)

const (
	integrationTenant      = "018f08f6-b2a0-794b-b2a1-0337ece9a596"
	integrationOtherTenant = "018f08f6-b2a1-799e-8614-16986a6a60a1"
	integrationInteraction = "018f08f6-b2a2-73aa-a19a-7df2083f74ac"
)

type integrationRouter struct {
	snapshot routeprojection.Snapshot
}

func (r integrationRouter) ReplayRouteFacts(context.Context, routeprojection.ProjectionPrincipal, routeprojection.ReplayRequest) (routeprojection.ReplayResult, error) {
	return routeprojection.ReplayResult{}, routeprojection.ErrUnknownHistory
}

func (r integrationRouter) GetRouteSnapshot(context.Context, routeprojection.ProjectionPrincipal, routeprojection.SnapshotRequest) (routeprojection.Snapshot, error) {
	return r.snapshot, nil
}

// @spec:service-extraction.relaypoint.authorization-outbox-survives-crash
// @spec:service-extraction.projection-recovery.unknown-history-is-audited
func TestPostgresAtomicDeliveryRLSAndProjectionRecovery(t *testing.T) {
	adminURL, cleanup := startPostgres17(t)
	defer cleanup()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	migration, err := os.ReadFile("../../db/migrations/000001_baseline.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	role := "rp_" + strings.ReplaceAll(uuid.Must(uuid.NewV7()).String(), "-", "")
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'relaypoint_test'", role)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %s; GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %s", role, role, role)); err != nil {
		t.Fatal(err)
	}
	appURL := strings.Replace(adminURL, "postgres:relaypoint_test@", role+":relaypoint_test@", 1)
	pool, err := pgxpool.New(ctx, appURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := New(pool)
	authorization := &deliverypb.DeliveryAuthorization{
		DeliveryAuthorizationId: "018f08f6-b2a3-7f37-8a19-257e3f675625", EventId: "018f08f6-b2a4-7f95-a409-a937723874b9", TenantId: integrationTenant,
		ReservationId: "018f08f6-b2a5-7523-b2e8-a35ed0f81c83", InteractionId: integrationInteraction, TargetSubscriberId: "018f08f6-b2a6-72ff-b935-d956d114967a",
		FencingToken: "018f08f6-b2a7-7de5-9259-9f150b9fb230", RouteVersion: 7, DeliveryDeadline: timestamppb.New(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)),
		Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	service := delivery.NewService(store, func() string { return "018f08f6-b2a8-7c9d-8de3-e03587f71771" })
	principal := delivery.Principal{ServiceID: "corex", TenantID: integrationTenant, Capabilities: []string{delivery.CapabilityDeliveryWrite}}
	first, err := service.AcceptDelivery(ctx, principal, authorization)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.AcceptDelivery(ctx, principal, authorization)
	if err != nil || first.GetReceiptId() != second.GetReceiptId() || !first.GetAcceptedAt().AsTime().Equal(second.GetAcceptedAt().AsTime()) {
		t.Fatalf("unstable retry: first=%v second=%v err=%v", first, second, err)
	}
	if got := tenantCount(t, pool, integrationTenant, `SELECT count(*) FROM delivery_ack_outbox`); got != 1 {
		t.Fatalf("ack rows=%d", got)
	}
	if got := tenantCount(t, pool, integrationOtherTenant, `SELECT count(*) FROM delivery_authorizations WHERE tenant_id = $1`, integrationTenant); got != 0 {
		t.Fatalf("cross-tenant rows=%d", got)
	}
	claim, ok, err := store.Claim(ctx, integrationTenant)
	if err != nil || !ok || claim.Receipt.GetReceiptId() != first.GetReceiptId() {
		t.Fatalf("claim=%+v ok=%v err=%v", claim, ok, err)
	}

	ringing := routeprojection.RouteFact{
		TenantID: integrationTenant, InteractionID: integrationInteraction, EventID: "018f08f6-b2a9-754d-8438-a2949fd793ee", Version: 1, Kind: routeprojection.FactRinging,
		DeliveryAuthorizationID: authorization.GetDeliveryAuthorizationId(), ReceiptID: first.GetReceiptId(), VisibilityGeneration: 1, LeaseUntil: time.Date(2099, 1, 1, 0, 1, 0, 0, time.UTC),
	}
	projector := routeprojection.New(store, integrationRouter{}, func() string { return "018f08f6-b2aa-79ea-ac66-6b52a53d790a" })
	result, err := projector.Apply(ctx, ringing)
	visible, visibilityErr := projector.Visible(ctx, integrationTenant, integrationInteraction)
	if err != nil || result != routeprojection.Applied || visibilityErr != nil || !visible {
		t.Fatalf("ringing result=%v visible=%v apply_err=%v visibility_err=%v", result, visible, err, visibilityErr)
	}
	held := routeprojection.RouteFact{TenantID: integrationTenant, InteractionID: integrationInteraction, EventID: "018f08f6-b2ab-7767-bdd4-42a598ff5a92", Version: 3, Kind: routeprojection.FactTerminal, VisibilityGeneration: 1}
	snapshotFact := routeprojection.RouteFact{TenantID: integrationTenant, InteractionID: integrationInteraction, EventID: "018f08f6-b2ac-7dbf-a638-1fa25cc37628", Version: 4, Kind: routeprojection.FactTerminal, VisibilityGeneration: 1}
	projector = routeprojection.New(store, integrationRouter{snapshot: routeprojection.Snapshot{Projection: routeprojection.Projection{TenantID: integrationTenant, InteractionID: integrationInteraction, Version: 4, EventID: snapshotFact.EventID, Hash: routeprojection.HashRouteFact(snapshotFact), Visibility: routeprojection.Hidden}, HistoryFloor: 1, Provenance: "router-snapshot-v1"}}, func() string { return "018f08f6-b2ad-7c65-bb38-9718312febc1" })
	if result, err := projector.Apply(ctx, held); err != nil || result != routeprojection.AuditedUnknownHistory {
		t.Fatalf("unknown result=%v err=%v", result, err)
	}
	if got := tenantCount(t, pool, integrationTenant, `SELECT count(*) FROM route_projection_dlq WHERE reason = 'UNKNOWN_STALE_HISTORY'`); got != 1 {
		t.Fatalf("unknown DLQ rows=%d", got)
	}
	if got := tenantCount(t, pool, integrationTenant, `SELECT count(*) FROM projection_invariant_alert_outbox`); got != 1 {
		t.Fatalf("alert rows=%d", got)
	}
}

func tenantCount(t *testing.T, pool *pgxpool.Pool, tenantID, query string, args ...any) int {
	t.Helper()
	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(context.Background(), `SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func startPostgres17(t *testing.T) (string, func()) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker unavailable")
	}
	name := "relaypoint-cc5d-" + strings.ReplaceAll(uuid.Must(uuid.NewV7()).String(), "-", "")
	command := exec.Command("docker", "run", "--rm", "-d", "--name", name, "-e", "POSTGRES_PASSWORD=relaypoint_test", "-p", "127.0.0.1::5432", "postgres:17-alpine")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("start postgres: %v: %s", err, output)
	}
	cleanup := func() { _ = exec.Command("docker", "rm", "-f", name).Run() }
	t.Cleanup(cleanup)
	var address string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		output, err := exec.Command("docker", "port", name, "5432/tcp").Output()
		if err == nil {
			address = strings.TrimSpace(string(output))
			if strings.HasPrefix(address, "127.0.0.1:") {
				if connection, dialErr := net.DialTimeout("tcp", address, 250*time.Millisecond); dialErr == nil {
					_ = connection.Close()
					url := "postgres://postgres:relaypoint_test@" + address + "/postgres?sslmode=disable"
					pool, poolErr := pgxpool.New(context.Background(), url)
					if poolErr == nil {
						pingErr := pool.Ping(context.Background())
						pool.Close()
						if pingErr == nil {
							return url, cleanup
						}
					}
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	cleanup()
	t.Fatalf("postgres 17 did not become ready at %s", address)
	return "", func() {}
}

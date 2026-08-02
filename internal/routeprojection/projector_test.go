package routeprojection

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"
)

const (
	projectionTenant      = "018f08f6-b27d-7d8b-a4d8-7dc23620f13d"
	projectionInteraction = "018f08f6-b27f-77bb-8513-8a7d3535eeef"
	projectionAuth        = "018f08f6-b280-7520-97f1-d6f879f4b8a2"
	projectionReceipt     = "018f08f6-b282-7e55-8946-748055a2f719"
)

type projectionStore struct {
	state                Projection
	ledgers              map[uint64]Identity
	intent               ReconcileIntent
	transactionOpen      bool
	installExpected      uint64
	installBatch         []RouteFact
	audited              bool
	alerted              bool
	failCompareAndSet    bool
	authorizationMissing bool
}

func (s *projectionStore) Load(_ context.Context, _ string, _ string, version uint64) (Projection, *Identity, error) {
	identity, ok := s.ledgers[version]
	if !ok {
		return s.state, nil, nil
	}
	return s.state, &identity, nil
}

func (s *projectionStore) Authorization(_ context.Context, _, authorizationID string) (AuthorizationBinding, bool, error) {
	if s.authorizationMissing || authorizationID != projectionAuth {
		return AuthorizationBinding{}, false, nil
	}
	return AuthorizationBinding{InteractionID: projectionInteraction, ReceiptID: projectionReceipt}, true, nil
}

func (s *projectionStore) InstallBatch(_ context.Context, expected uint64, facts []RouteFact, final FoldedState) (bool, error) {
	s.transactionOpen = true
	defer func() { s.transactionOpen = false }()
	s.installExpected = expected
	s.installBatch = append([]RouteFact(nil), facts...)
	if s.failCompareAndSet || s.state.Version != expected {
		return false, nil
	}
	for _, fact := range facts {
		s.ledgers[fact.Version] = Identity{EventID: fact.EventID, Hash: HashRouteFact(fact)}
	}
	s.state.Version = final.Version
	s.state.EventID = final.EventID
	s.state.Hash = final.Hash
	s.state.Visibility = final.Visibility
	s.state.DeliveryAuthorizationID = final.DeliveryAuthorizationID
	s.state.ReceiptID = final.ReceiptID
	s.state.VisibilityGeneration = final.VisibilityGeneration
	s.state.LeaseUntil = final.LeaseUntil
	return true, nil
}

func (s *projectionStore) InstallReconcileBatch(ctx context.Context, intent ReconcileIntent, facts []RouteFact, final FoldedState) (bool, error) {
	return s.InstallBatch(ctx, intent.ObservedVersion, facts, final)
}

func (s *projectionStore) BeginReconcile(_ context.Context, intent ReconcileIntent) (ReconcileIntent, bool, error) {
	s.transactionOpen = true
	defer func() { s.transactionOpen = false }()
	if s.state.Version != intent.ObservedVersion {
		return ReconcileIntent{}, false, nil
	}
	s.intent = intent
	return intent, true, nil
}

func (s *projectionStore) InstallHistoricalIdentity(_ context.Context, intent ReconcileIntent, fact RouteFact) (bool, error) {
	if s.failCompareAndSet || s.state.Version != intent.ObservedVersion {
		return false, nil
	}
	s.ledgers[fact.Version] = Identity{EventID: fact.EventID, Hash: HashRouteFact(fact)}
	return true, nil
}

func (s *projectionStore) InstallSnapshotAndAudit(_ context.Context, _ ReconcileIntent, snapshot Snapshot, _ RouteFact) (bool, error) {
	s.transactionOpen = true
	defer func() { s.transactionOpen = false }()
	if s.failCompareAndSet || s.state.Version != s.intent.ObservedVersion {
		return false, nil
	}
	s.state = snapshot.Projection
	s.audited = true
	s.alerted = true
	return true, nil
}

func (s *projectionStore) RecordPoison(context.Context, RouteFact, string) error {
	s.audited = true
	return nil
}

type projectionRouter struct {
	store          *projectionStore
	replay         ReplayResult
	replayErr      error
	snapshot       Snapshot
	principals     []ProjectionPrincipal
	remoteWithLock bool
}

func (r *projectionRouter) ReplayRouteFacts(_ context.Context, principal ProjectionPrincipal, _ ReplayRequest) (ReplayResult, error) {
	r.principals = append(r.principals, principal)
	r.remoteWithLock = r.remoteWithLock || r.store.transactionOpen
	return r.replay, r.replayErr
}

func (r *projectionRouter) GetRouteSnapshot(_ context.Context, principal ProjectionPrincipal, _ SnapshotRequest) (Snapshot, error) {
	r.principals = append(r.principals, principal)
	r.remoteWithLock = r.remoteWithLock || r.store.transactionOpen
	return r.snapshot, nil
}

func routeFact(version uint64, kind FactKind) RouteFact {
	fact := RouteFact{TenantID: projectionTenant, InteractionID: projectionInteraction, EventID: eventForVersion(version), Version: version, Kind: kind}
	if kind == FactRinging {
		fact.DeliveryAuthorizationID = projectionAuth
		fact.ReceiptID = projectionReceipt
		fact.VisibilityGeneration = version
		fact.LeaseUntil = time.Date(2026, 8, 2, 12, 5, 0, 0, time.UTC)
	}
	if kind == FactTerminal {
		fact.VisibilityGeneration = version - 1
	}
	return fact
}

func eventForVersion(version uint64) string {
	return map[uint64]string{
		1: "018f08f6-b291-72b0-ae70-0c2f65cf329c",
		2: "018f08f6-b292-73bd-85de-fe1cfbd0e7b7",
		3: "018f08f6-b293-78ee-8c33-ad8f95377e6e",
		4: "018f08f6-b294-7eb8-bbc0-c74128c62c94",
	}[version]
}

// @spec:service-extraction.projection-recovery.lower-version-divergence-poison
func TestLowerVersionDivergenceIsPoison(t *testing.T) {
	current := routeFact(2, FactRinging)
	store := &projectionStore{state: Projection{Version: 2}, ledgers: map[uint64]Identity{2: {EventID: current.EventID, Hash: HashRouteFact(current)}}}
	projector := New(store, &projectionRouter{store: store}, func() string { return "018f08f6-b295-7cd5-807e-4c09f7f4caba" })
	divergent := current
	divergent.ReceiptID = "018f08f6-b296-7e56-934c-2c6d75371d1e"

	result, err := projector.Apply(context.Background(), divergent)
	if !errors.Is(err, ErrDivergentHistory) || result != Poisoned || !store.audited {
		t.Fatalf("result=%v err=%v audited=%v", result, err, store.audited)
	}
}

// @spec:service-extraction.projection-recovery.authorized-retained-ledger
// @spec:service-extraction.projection-recovery.outside-lock-cas
func TestGapRecoveryUsesAuthorizedRPCOutsideTransactionAndCAS(t *testing.T) {
	one := routeFact(1, FactReserved)
	two := routeFact(2, FactRinging)
	three := routeFact(3, FactTerminal)
	store := &projectionStore{state: Projection{}, ledgers: map[uint64]Identity{}}
	router := &projectionRouter{store: store, replay: ReplayResult{Facts: []RouteFact{one, two, three}, RouterHead: 3}}
	projector := New(store, router, func() string { return "018f08f6-b295-7cd5-807e-4c09f7f4caba" })

	result, err := projector.Apply(context.Background(), three)
	if err != nil || result != Applied {
		t.Fatalf("result=%v err=%v", result, err)
	}
	if router.remoteWithLock {
		t.Fatal("projection RPC ran while store transaction was open")
	}
	if len(router.principals) != 1 || router.principals[0].ServiceID != "relaypoint" || router.principals[0].TenantID != projectionTenant || router.principals[0].Capability != CapabilityProjectionRead {
		t.Fatalf("principal=%+v", router.principals)
	}
	if store.installExpected != 0 || len(store.installBatch) != 3 {
		t.Fatalf("expected=%d batch=%d", store.installExpected, len(store.installBatch))
	}
	if store.state.Visibility != Hidden {
		t.Fatalf("intermediate ringing escaped batch: visibility=%v", store.state.Visibility)
	}
}

// @spec:service-extraction.projection-recovery.outside-lock-cas
func TestGapRecoveryRejectsChangedHeadAtInstall(t *testing.T) {
	store := &projectionStore{state: Projection{}, ledgers: map[uint64]Identity{}, failCompareAndSet: true}
	held := routeFact(2, FactTerminal)
	router := &projectionRouter{store: store, replay: ReplayResult{Facts: []RouteFact{routeFact(1, FactReserved), held}, RouterHead: 2}}
	projector := New(store, router, func() string { return "018f08f6-b295-7cd5-807e-4c09f7f4caba" })
	result, err := projector.Apply(context.Background(), held)
	if err != nil || result != CompareAndSetLost || store.state.Version != 0 {
		t.Fatalf("result=%v version=%d err=%v", result, store.state.Version, err)
	}
}

// @spec:service-extraction.projection-recovery.lower-version-divergence-poison
func TestMissingLowerLedgerRequiresAuthenticatedReconciliation(t *testing.T) {
	one := routeFact(1, FactReserved)
	store := &projectionStore{state: Projection{Version: 2}, ledgers: map[uint64]Identity{}}
	router := &projectionRouter{store: store, replay: ReplayResult{Facts: []RouteFact{one}, RouterHead: 2}}
	projector := New(store, router, func() string { return "018f08f6-b295-7cd5-807e-4c09f7f4caba" })
	result, err := projector.Apply(context.Background(), one)
	if err != nil || result != Duplicate {
		t.Fatalf("result=%v err=%v", result, err)
	}
	if _, ok := store.ledgers[1]; !ok || len(router.principals) != 1 {
		t.Fatalf("ledger=%v principals=%v", store.ledgers, router.principals)
	}
}

// @spec:service-extraction.projection-recovery.unknown-history-is-audited
func TestUnknownRetainedHistoryCommitsSnapshotDLQAndAlert(t *testing.T) {
	held := routeFact(3, FactTerminal)
	snapshotFact := routeFact(4, FactTerminal)
	store := &projectionStore{state: Projection{Version: 1}, ledgers: map[uint64]Identity{}}
	router := &projectionRouter{store: store, replayErr: ErrUnknownHistory, snapshot: Snapshot{Projection: Projection{TenantID: projectionTenant, InteractionID: projectionInteraction, Version: 4, EventID: snapshotFact.EventID, Hash: HashRouteFact(snapshotFact), Visibility: Hidden}, HistoryFloor: 1, Provenance: "router-snapshot-v1"}}
	projector := New(store, router, func() string { return "018f08f6-b295-7cd5-807e-4c09f7f4caba" })

	result, err := projector.Apply(context.Background(), held)
	if err != nil || result != AuditedUnknownHistory {
		t.Fatalf("result=%v err=%v", result, err)
	}
	if !store.audited || !store.alerted || store.state.Version != 4 {
		t.Fatalf("audited=%v alerted=%v version=%d", store.audited, store.alerted, store.state.Version)
	}
}

// @spec:service-extraction.relaypoint.routefact-only-visibility
func TestOnlyRouteFactAndLocalLeaseGrantVisibility(t *testing.T) {
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	store := &projectionStore{state: Projection{DatabaseNow: now}, ledgers: map[uint64]Identity{}}
	projector := New(store, &projectionRouter{store: store}, func() string { return "018f08f6-b295-7cd5-807e-4c09f7f4caba" })

	if visible, err := projector.Visible(context.Background(), projectionTenant, projectionInteraction); err != nil || visible {
		t.Fatal("authorization/receipt/ACK response made offer visible")
	}
	store.authorizationMissing = true
	if _, err := projector.Apply(context.Background(), routeFact(1, FactRinging)); !errors.Is(err, ErrAuthorizationPending) || store.state.Version != 0 {
		t.Fatalf("unbound ringing err=%v version=%d", err, store.state.Version)
	}
	store.authorizationMissing = false
	if result, err := projector.Apply(context.Background(), routeFact(1, FactRinging)); err != nil || result != Applied {
		t.Fatalf("ringing result=%v err=%v", result, err)
	}
	store.state.DatabaseNow = now
	if visible, err := projector.Visible(context.Background(), projectionTenant, projectionInteraction); err != nil || !visible {
		t.Fatal("current live ringing fact was hidden")
	}
	store.state.DatabaseNow = store.state.LeaseUntil
	if visible, err := projector.Visible(context.Background(), projectionTenant, projectionInteraction); err != nil || visible {
		t.Fatal("offer visible at lease deadline")
	}
	store.state.DatabaseNow = now
	if result, err := projector.Apply(context.Background(), routeFact(2, FactTerminal)); err != nil || result != Applied {
		t.Fatalf("terminal result=%v err=%v", result, err)
	}
	if visible, err := projector.Visible(context.Background(), projectionTenant, projectionInteraction); err != nil || visible {
		t.Fatal("terminal head remained visible")
	}
}

func TestRouteFactHashVector(t *testing.T) {
	fact := routeFact(2, FactRinging)
	hash := HashRouteFact(fact)
	if got := hex.EncodeToString(hash[:]); got != "91df089ffcac4b0ea6386355c57f0c8d090c42e533e76c5ba7641bf9fb2356c1" {
		t.Fatalf("route fact hash=%s", got)
	}
}

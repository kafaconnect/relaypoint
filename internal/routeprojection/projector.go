package routeprojection

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const CapabilityProjectionRead = "router.projection.read"

var (
	ErrDivergentHistory     = errors.New("route projection: divergent history")
	ErrUnknownHistory       = errors.New("route projection: unknown retained history")
	ErrInvalidFact          = errors.New("route projection: invalid fact")
	ErrInvalidReplay        = errors.New("route projection: invalid replay")
	ErrAuthorizationPending = errors.New("route projection: delivery authorization pending")
)

type FactKind uint8

const (
	FactReserved FactKind = iota + 1
	FactRinging
	FactTerminal
)

type Visibility uint8

const (
	Hidden Visibility = iota
	Visible
)

type RouteFact struct {
	TenantID                string
	InteractionID           string
	EventID                 string
	Version                 uint64
	Kind                    FactKind
	DeliveryAuthorizationID string
	ReceiptID               string
	VisibilityGeneration    uint64
	LeaseUntil              time.Time
}

type Identity struct {
	EventID string
	Hash    [32]byte
}

type Projection struct {
	TenantID                string
	InteractionID           string
	Version                 uint64
	EventID                 string
	Hash                    [32]byte
	Visibility              Visibility
	DeliveryAuthorizationID string
	ReceiptID               string
	VisibilityGeneration    uint64
	LeaseUntil              time.Time
	DatabaseNow             time.Time
}

type FoldedState struct {
	Version                 uint64
	EventID                 string
	Hash                    [32]byte
	Visibility              Visibility
	DeliveryAuthorizationID string
	ReceiptID               string
	VisibilityGeneration    uint64
	LeaseUntil              time.Time
}

type ReconcileIntent struct {
	Token           string
	TenantID        string
	InteractionID   string
	ObservedVersion uint64
	RequestedFrom   uint64
	RequestedTo     uint64
	HeldEventID     string
	HeldHash        [32]byte
}

type ProjectionPrincipal struct {
	ServiceID  string
	TenantID   string
	Capability string
}

type ReplayRequest struct {
	TenantID      string
	InteractionID string
	FromVersion   uint64
	ToVersion     uint64
}

type ReplayResult struct {
	Facts      []RouteFact
	RouterHead uint64
}

type SnapshotRequest struct {
	TenantID      string
	InteractionID string
}

type Snapshot struct {
	Projection   Projection
	HistoryFloor uint64
	Provenance   string
}

type AuthorizationBinding struct {
	InteractionID string
	ReceiptID     string
}

type Store interface {
	Load(context.Context, string, string, uint64) (Projection, *Identity, error)
	Authorization(context.Context, string, string) (AuthorizationBinding, bool, error)
	InstallBatch(context.Context, uint64, []RouteFact, FoldedState) (bool, error)
	BeginReconcile(context.Context, ReconcileIntent) (ReconcileIntent, bool, error)
	InstallReconcileBatch(context.Context, ReconcileIntent, []RouteFact, FoldedState) (bool, error)
	InstallHistoricalIdentity(context.Context, ReconcileIntent, RouteFact) (bool, error)
	InstallSnapshotAndAudit(context.Context, ReconcileIntent, Snapshot, RouteFact) (bool, error)
	RecordPoison(context.Context, RouteFact, string) error
}

type RouterPort interface {
	ReplayRouteFacts(context.Context, ProjectionPrincipal, ReplayRequest) (ReplayResult, error)
	GetRouteSnapshot(context.Context, ProjectionPrincipal, SnapshotRequest) (Snapshot, error)
}

type ApplyResult uint8

const (
	Applied ApplyResult = iota + 1
	Duplicate
	CompareAndSetLost
	Poisoned
	AuditedUnknownHistory
)

type Projector struct {
	store  Store
	router RouterPort
	newID  func() string
}

func New(store Store, router RouterPort, newID func() string) *Projector {
	if newID == nil {
		newID = func() string { return uuid.Must(uuid.NewV7()).String() }
	}
	return &Projector{store: store, router: router, newID: newID}
}

func (p *Projector) Apply(ctx context.Context, fact RouteFact) (ApplyResult, error) {
	if err := validateFact(fact); err != nil {
		return Poisoned, err
	}
	projection, retained, err := p.store.Load(ctx, fact.TenantID, fact.InteractionID, fact.Version)
	if err != nil {
		return 0, err
	}
	hash := HashRouteFact(fact)
	if fact.Version <= projection.Version {
		if retained == nil {
			return p.reconcile(ctx, projection, fact, true)
		}
		if retained.EventID == fact.EventID && retained.Hash == hash {
			return Duplicate, nil
		}
		if err := p.store.RecordPoison(ctx, fact, ErrDivergentHistory.Error()); err != nil {
			return Poisoned, err
		}
		return Poisoned, ErrDivergentHistory
	}
	if fact.Version > projection.Version+1 {
		return p.reconcile(ctx, projection, fact, false)
	}
	if err := p.validateAuthorizationBindings(ctx, []RouteFact{fact}); err != nil {
		return 0, err
	}
	final, err := FoldBatch(projection, []RouteFact{fact})
	if err != nil {
		return Poisoned, err
	}
	installed, err := p.store.InstallBatch(ctx, projection.Version, []RouteFact{fact}, final)
	if err != nil {
		return 0, err
	}
	if !installed {
		return CompareAndSetLost, nil
	}
	return Applied, nil
}

func (p *Projector) Visible(ctx context.Context, tenantID, interactionID string) (bool, error) {
	projection, _, err := p.store.Load(ctx, tenantID, interactionID, 0)
	if err != nil {
		return false, err
	}
	if projection.Visibility != Visible || projection.DatabaseNow.IsZero() {
		return false, nil
	}
	return projection.DatabaseNow.Before(projection.LeaseUntil), nil
}

func (p *Projector) reconcile(ctx context.Context, projection Projection, held RouteFact, historical bool) (ApplyResult, error) {
	token := p.newID()
	if err := validateUUIDv7("reconcile token", token); err != nil {
		return 0, err
	}
	from, to := projection.Version+1, held.Version
	if historical {
		from, to = held.Version, held.Version
	}
	intent := ReconcileIntent{Token: token, TenantID: held.TenantID, InteractionID: held.InteractionID, ObservedVersion: projection.Version, RequestedFrom: from, RequestedTo: to, HeldEventID: held.EventID, HeldHash: HashRouteFact(held)}
	intent, started, err := p.store.BeginReconcile(ctx, intent)
	if err != nil {
		return 0, err
	}
	if !started {
		return CompareAndSetLost, nil
	}
	principal := ProjectionPrincipal{ServiceID: "relaypoint", TenantID: held.TenantID, Capability: CapabilityProjectionRead}
	replay, err := p.router.ReplayRouteFacts(ctx, principal, ReplayRequest{TenantID: held.TenantID, InteractionID: held.InteractionID, FromVersion: from, ToVersion: to})
	if err != nil {
		if !errors.Is(err, ErrUnknownHistory) {
			return 0, err
		}
		return p.snapshotUnknown(ctx, principal, intent, held)
	}
	if historical {
		if len(replay.Facts) != 1 || replay.Facts[0].Version != held.Version {
			return Poisoned, ErrInvalidReplay
		}
		replayed := replay.Facts[0]
		if replayed.EventID != held.EventID || HashRouteFact(replayed) != HashRouteFact(held) {
			if poisonErr := p.store.RecordPoison(ctx, held, ErrDivergentHistory.Error()); poisonErr != nil {
				return Poisoned, poisonErr
			}
			return Poisoned, ErrDivergentHistory
		}
		if err := p.validateAuthorizationBindings(ctx, replay.Facts); err != nil {
			return 0, err
		}
		installed, installErr := p.store.InstallHistoricalIdentity(ctx, intent, replayed)
		if installErr != nil {
			return 0, installErr
		}
		if !installed {
			return CompareAndSetLost, nil
		}
		return Duplicate, nil
	}
	for _, fact := range replay.Facts {
		if fact.TenantID != held.TenantID || fact.InteractionID != held.InteractionID {
			return Poisoned, ErrInvalidReplay
		}
	}
	if err := p.validateAuthorizationBindings(ctx, replay.Facts); err != nil {
		return 0, err
	}
	final, err := FoldBatch(projection, replay.Facts)
	if err != nil || len(replay.Facts) == 0 || replay.Facts[len(replay.Facts)-1].Version < held.Version || replay.RouterHead < replay.Facts[len(replay.Facts)-1].Version {
		return Poisoned, ErrInvalidReplay
	}
	heldFound := false
	for _, fact := range replay.Facts {
		if fact.Version == held.Version && fact.EventID == held.EventID && HashRouteFact(fact) == HashRouteFact(held) {
			heldFound = true
		}
	}
	if !heldFound {
		return Poisoned, ErrInvalidReplay
	}
	installed, err := p.store.InstallReconcileBatch(ctx, intent, replay.Facts, final)
	if err != nil {
		return 0, err
	}
	if !installed {
		return CompareAndSetLost, nil
	}
	return Applied, nil
}

func (p *Projector) validateAuthorizationBindings(ctx context.Context, facts []RouteFact) error {
	for _, fact := range facts {
		if fact.Kind != FactRinging {
			continue
		}
		binding, ok, err := p.store.Authorization(ctx, fact.TenantID, fact.DeliveryAuthorizationID)
		if err != nil {
			return err
		}
		if !ok || binding.InteractionID != fact.InteractionID || binding.ReceiptID != fact.ReceiptID {
			return ErrAuthorizationPending
		}
	}
	return nil
}

func (p *Projector) snapshotUnknown(ctx context.Context, principal ProjectionPrincipal, intent ReconcileIntent, held RouteFact) (ApplyResult, error) {
	snapshot, err := p.router.GetRouteSnapshot(ctx, principal, SnapshotRequest{TenantID: held.TenantID, InteractionID: held.InteractionID})
	if err != nil {
		return 0, err
	}
	if err := validateSnapshot(snapshot, intent, held); err != nil {
		return Poisoned, ErrInvalidReplay
	}
	if snapshot.Projection.Visibility == Visible {
		fact := RouteFact{TenantID: snapshot.Projection.TenantID, InteractionID: snapshot.Projection.InteractionID, DeliveryAuthorizationID: snapshot.Projection.DeliveryAuthorizationID, ReceiptID: snapshot.Projection.ReceiptID, Kind: FactRinging}
		if err := p.validateAuthorizationBindings(ctx, []RouteFact{fact}); err != nil {
			return 0, err
		}
	}
	installed, err := p.store.InstallSnapshotAndAudit(ctx, intent, snapshot, held)
	if err != nil {
		return 0, err
	}
	if !installed {
		return CompareAndSetLost, nil
	}
	return AuditedUnknownHistory, nil
}

func validateSnapshot(snapshot Snapshot, intent ReconcileIntent, held RouteFact) error {
	projection := snapshot.Projection
	if projection.TenantID != held.TenantID || projection.InteractionID != held.InteractionID || projection.Version == 0 || projection.Version < intent.ObservedVersion {
		return ErrInvalidReplay
	}
	if snapshot.HistoryFloor == 0 || snapshot.HistoryFloor > projection.Version || snapshot.Provenance == "" {
		return ErrInvalidReplay
	}
	if err := validateUUIDv7("snapshot event_id", projection.EventID); err != nil || projection.Hash == [32]byte{} {
		return ErrInvalidReplay
	}
	if projection.Visibility != Hidden && projection.Visibility != Visible {
		return ErrInvalidReplay
	}
	if projection.Visibility == Visible {
		if err := validateUUIDv7("snapshot delivery_authorization_id", projection.DeliveryAuthorizationID); err != nil {
			return ErrInvalidReplay
		}
		if err := validateUUIDv7("snapshot receipt_id", projection.ReceiptID); err != nil || projection.VisibilityGeneration != projection.Version || projection.LeaseUntil.IsZero() {
			return ErrInvalidReplay
		}
	}
	return nil
}

func FoldBatch(projection Projection, facts []RouteFact) (FoldedState, error) {
	state := FoldedState{Version: projection.Version, EventID: projection.EventID, Hash: projection.Hash, Visibility: projection.Visibility, DeliveryAuthorizationID: projection.DeliveryAuthorizationID, ReceiptID: projection.ReceiptID, VisibilityGeneration: projection.VisibilityGeneration, LeaseUntil: projection.LeaseUntil}
	tenantID, interactionID := projection.TenantID, projection.InteractionID
	if tenantID == "" && len(facts) > 0 {
		tenantID, interactionID = facts[0].TenantID, facts[0].InteractionID
	}
	for _, fact := range facts {
		if err := validateFact(fact); err != nil || fact.Version != state.Version+1 {
			return FoldedState{}, ErrInvalidReplay
		}
		if fact.TenantID != tenantID || fact.InteractionID != interactionID {
			return FoldedState{}, ErrInvalidReplay
		}
		state.Version = fact.Version
		state.EventID = fact.EventID
		state.Hash = HashRouteFact(fact)
		switch fact.Kind {
		case FactRinging:
			state.Visibility = Visible
			state.DeliveryAuthorizationID = fact.DeliveryAuthorizationID
			state.ReceiptID = fact.ReceiptID
			state.VisibilityGeneration = fact.VisibilityGeneration
			state.LeaseUntil = fact.LeaseUntil
		case FactReserved, FactTerminal:
			state.Visibility = Hidden
			if fact.Kind == FactTerminal {
				state.LeaseUntil = time.Time{}
			}
		}
	}
	return state, nil
}

func HashRouteFact(fact RouteFact) [32]byte {
	hasher := sha256.New()
	for _, part := range [][]byte{[]byte(fact.TenantID), []byte(fact.InteractionID), []byte(fact.EventID)} {
		writePart(hasher, part)
	}
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], fact.Version)
	writePart(hasher, number[:])
	writePart(hasher, []byte{byte(fact.Kind)})
	writePart(hasher, []byte(fact.DeliveryAuthorizationID))
	writePart(hasher, []byte(fact.ReceiptID))
	binary.BigEndian.PutUint64(number[:], fact.VisibilityGeneration)
	writePart(hasher, number[:])
	binary.BigEndian.PutUint64(number[:], uint64(fact.LeaseUntil.UTC().UnixNano()))
	writePart(hasher, number[:])
	var hash [32]byte
	copy(hash[:], hasher.Sum(nil))
	return hash
}

func writePart(writer interface{ Write([]byte) (int, error) }, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write(value)
}

func validateFact(fact RouteFact) error {
	for field, value := range map[string]string{"tenant_id": fact.TenantID, "interaction_id": fact.InteractionID, "event_id": fact.EventID} {
		if err := validateUUIDv7(field, value); err != nil {
			return err
		}
	}
	if fact.Version == 0 || fact.Kind < FactReserved || fact.Kind > FactTerminal {
		return ErrInvalidFact
	}
	if fact.Kind == FactRinging {
		if err := validateUUIDv7("delivery_authorization_id", fact.DeliveryAuthorizationID); err != nil {
			return err
		}
		if err := validateUUIDv7("receipt_id", fact.ReceiptID); err != nil {
			return err
		}
		if fact.VisibilityGeneration != fact.Version || fact.LeaseUntil.IsZero() {
			return ErrInvalidFact
		}
	}
	if fact.Kind == FactTerminal && (fact.VisibilityGeneration == 0 || fact.VisibilityGeneration >= fact.Version) {
		return ErrInvalidFact
	}
	return nil
}

func validateUUIDv7(field, value string) error {
	id, err := uuid.Parse(value)
	if err != nil || id.Version() != 7 || id.Variant() != uuid.RFC4122 || id.String() != value {
		return fmt.Errorf("%w: %s must be canonical UUIDv7", ErrInvalidFact, field)
	}
	return nil
}

package participation

import (
	"context"
	"errors"
	"testing"
	"time"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	partTenant      = "018f4000-0000-7000-8000-000000000001"
	partInteraction = "018f4000-0000-7000-8000-000000000002"
	partAlice       = "018f4000-0000-7000-8000-000000000003"
	partBob         = "018f4000-0000-7000-8000-000000000004"
)

type foldStore struct {
	state           Fold
	versionLedger   map[uint64]Identity
	eventLedger     map[string]VersionIdentity
	pending         map[string]Record
	intent          Intent
	transactionOpen bool
	casLost         bool
	dlq             int
	alert           int
	snapshotSource  string
	failures        int
}

func (s *foldStore) Load(_ context.Context, _ Key, version uint64) (Fold, *Identity, error) {
	identity, ok := s.versionLedger[version]
	if !ok {
		return s.state.Clone(), nil, nil
	}
	return s.state.Clone(), &identity, nil
}

func (s *foldStore) Install(_ context.Context, expected uint64, records []Record, final Fold) (bool, error) {
	s.transactionOpen = true
	defer func() { s.transactionOpen = false }()
	if s.casLost || s.state.Version != expected {
		return false, nil
	}
	s.install(records, final)
	return true, nil
}

func (s *foldStore) BeginReconcile(_ context.Context, intent Intent, held Record) (Intent, bool, error) {
	s.transactionOpen = true
	defer func() { s.transactionOpen = false }()
	if s.casLost || s.state.Version != intent.ObservedVersion {
		return Intent{}, false, nil
	}
	s.intent = intent
	s.pending[held.Command.GetEventId()] = held
	return intent, true, nil
}

func (s *foldStore) InstallReconcile(_ context.Context, intent Intent, records []Record, final Fold) (bool, error) {
	if s.casLost || s.state.Version != intent.ObservedVersion || s.intent.Token != intent.Token {
		return false, nil
	}
	s.install(records, final)
	delete(s.pending, intent.Held.EventID)
	return true, nil
}

func (s *foldStore) InstallHistorical(_ context.Context, intent Intent, record Record) (bool, error) {
	if s.casLost || s.state.Version != intent.ObservedVersion || s.intent.Token != intent.Token {
		return false, nil
	}
	s.versionLedger[record.Command.GetAggregateVersion()] = record.Identity
	s.eventLedger[record.Command.GetEventId()] = VersionIdentity{Version: record.Command.GetAggregateVersion(), Hash: record.Hash}
	delete(s.pending, intent.Held.EventID)
	return true, nil
}

func (s *foldStore) InstallSnapshot(_ context.Context, intent Intent, snapshot Snapshot, _ Record) (bool, error) {
	s.transactionOpen = true
	defer func() { s.transactionOpen = false }()
	if s.casLost || s.state.Version != intent.ObservedVersion || s.intent.Token != intent.Token {
		return false, nil
	}
	s.state = snapshot.Fold.Clone()
	s.versionLedger[snapshot.Fold.Version] = snapshot.Fold.Identity
	s.eventLedger[snapshot.Fold.Identity.EventID] = VersionIdentity{Version: snapshot.Fold.Version, Hash: snapshot.Fold.Identity.Hash}
	s.snapshotSource = snapshot.Provenance
	s.dlq++
	s.alert++
	delete(s.pending, intent.Held.EventID)
	return true, nil
}

func (s *foldStore) RecordPoison(context.Context, Record, string) error {
	s.dlq++
	s.alert++
	return nil
}

func (s *foldStore) FailReconcile(context.Context, Intent, Record, error) (bool, error) {
	s.failures++
	if s.failures < 3 {
		return false, nil
	}
	s.dlq++
	s.alert++
	return true, nil
}

func (s *foldStore) install(records []Record, final Fold) {
	for _, record := range records {
		s.versionLedger[record.Command.GetAggregateVersion()] = record.Identity
		s.eventLedger[record.Command.GetEventId()] = VersionIdentity{Version: record.Command.GetAggregateVersion(), Hash: record.Hash}
	}
	s.state = final.Clone()
}

type authorityPort struct {
	store          *foldStore
	replay         *interactionv1.ReplayParticipationResponse
	replayErr      error
	snapshot       *interactionv1.GetDesiredParticipationSnapshotResponse
	remoteWithLock bool
}

func (p *authorityPort) Replay(_ context.Context, _ *interactionv1.ReplayParticipationRequest) (*interactionv1.ReplayParticipationResponse, error) {
	p.remoteWithLock = p.remoteWithLock || p.store.transactionOpen
	return p.replay, p.replayErr
}

func (p *authorityPort) Snapshot(_ context.Context, _ *interactionv1.GetDesiredParticipationSnapshotRequest) (*interactionv1.GetDesiredParticipationSnapshotResponse, error) {
	p.remoteWithLock = p.remoteWithLock || p.store.transactionOpen
	return p.snapshot, nil
}

func partCommand(version uint64, participant string, state interactionv1.ParticipationDesiredState) *interactionv1.ParticipationCommand {
	return &interactionv1.ParticipationCommand{
		EventId: map[uint64]string{
			1: "018f4000-0000-7000-8000-000000000011",
			2: "018f4000-0000-7000-8000-000000000012",
			3: "018f4000-0000-7000-8000-000000000013",
			4: "018f4000-0000-7000-8000-000000000014",
		}[version],
		AggregateVersion: version, TenantId: partTenant, InteractionId: partInteraction,
		ParticipantId: participant, DesiredState: state,
		OccurredAt:  timestamppb.New(time.Unix(int64(100+version), 0).UTC()),
		Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
}

func partWritePrincipal() Principal {
	return Principal{TenantID: partTenant, Grant: TransportGrant{ServiceID: "corex", Capability: CapabilityWrite}}
}

func newFoldStore() *foldStore {
	return &foldStore{state: Fold{Participants: map[string]struct{}{}}, versionLedger: map[uint64]Identity{}, eventLedger: map[string]VersionIdentity{}, pending: map[string]Record{}}
}

// @spec:service-extraction.participation.capability-not-role
func TestParticipationSubscriptionUsesTenantCapabilityNotRole(t *testing.T) {
	store := newFoldStore()
	port := &authorityPort{store: store}
	projector := NewProjector(store, port, func() string { return "018f4000-0000-7000-8000-000000000020" })
	command := partCommand(1, partAlice, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED)
	result, err := projector.Apply(context.Background(), Principal{TenantID: partTenant, Grant: TransportGrant{ServiceID: "corex", Capability: CapabilityWrite, Role: "desk-admin"}}, command)
	if !errors.Is(err, ErrPermissionDenied) || result != Poisoned || store.state.Version != 0 {
		t.Fatalf("role result=%v err=%v state=%+v", result, err, store.state)
	}
	result, err = projector.Apply(context.Background(), partWritePrincipal(), command)
	if err != nil || result != Applied || store.state.Version != 1 {
		t.Fatalf("capability result=%v err=%v state=%+v", result, err, store.state)
	}
}

// @spec:service-extraction.participation.replay-and-reconcile
// @spec:service-extraction.participation.outside-lock-reconciliation
func TestParticipationGapReplaysExactNextOutsideLockAndCAS(t *testing.T) {
	store := newFoldStore()
	one := partCommand(1, partAlice, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED)
	two := partCommand(2, partBob, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED)
	port := &authorityPort{store: store, replay: replayResponse(t, one, two)}
	projector := NewProjector(store, port, func() string { return "018f4000-0000-7000-8000-000000000020" })
	result, err := projector.Apply(context.Background(), partWritePrincipal(), two)
	if err != nil || result != Applied || store.state.Version != 2 || len(store.state.Participants) != 2 {
		t.Fatalf("result=%v state=%+v err=%v", result, store.state, err)
	}
	if port.remoteWithLock || len(store.pending) != 0 || len(store.versionLedger) != 2 || len(store.eventLedger) != 2 {
		t.Fatalf("lock=%v pending=%d versions=%d events=%d", port.remoteWithLock, len(store.pending), len(store.versionLedger), len(store.eventLedger))
	}
}

// @spec:service-extraction.participation.replay-and-reconcile
func TestParticipationDuplicateDivergenceAndSnapshotReplacement(t *testing.T) {
	store := newFoldStore()
	one := partCommand(1, partAlice, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED)
	record, err := Canonical(one)
	if err != nil {
		t.Fatal(err)
	}
	store.install([]Record{record}, Fold{Version: 1, Identity: record.Identity, Participants: map[string]struct{}{partAlice: {}}})
	projector := NewProjector(store, &authorityPort{store: store}, func() string { return "018f4000-0000-7000-8000-000000000020" })
	if result, applyErr := projector.Apply(context.Background(), partWritePrincipal(), one); applyErr != nil || result != Duplicate {
		t.Fatalf("duplicate result=%v err=%v", result, applyErr)
	}
	divergent := proto.Clone(one).(*interactionv1.ParticipationCommand)
	divergent.ParticipantId = partBob
	if result, applyErr := projector.Apply(context.Background(), partWritePrincipal(), divergent); !errors.Is(applyErr, ErrDivergentHistory) || result != Poisoned || store.dlq != 1 || store.alert != 1 {
		t.Fatalf("divergent result=%v dlq=%d alert=%d err=%v", result, store.dlq, store.alert, applyErr)
	}
	store.versionLedger = map[uint64]Identity{}
	three := partCommand(3, partBob, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED)
	snapshotHead := partCommand(4, partBob, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED)
	snapshotRecord, err := Canonical(snapshotHead)
	if err != nil {
		t.Fatal(err)
	}
	port := &authorityPort{store: store, replayErr: ErrUnknownHistory, snapshot: &interactionv1.GetDesiredParticipationSnapshotResponse{
		TenantId: partTenant, InteractionId: partInteraction, AggregateVersion: 4,
		HeadEventId: snapshotHead.GetEventId(), HeadHash: snapshotRecord.Hash[:], ParticipantIds: []string{partBob},
		HistoryFloor: 1, Provenance: "corex-participation-history-v1",
	}}
	projector = NewProjector(store, port, func() string { return "018f4000-0000-7000-8000-000000000021" })
	result, err := projector.Apply(context.Background(), partWritePrincipal(), three)
	if err != nil || result != AuditedSnapshot || store.state.Version != 4 || len(store.state.Participants) != 1 {
		t.Fatalf("snapshot result=%v state=%+v err=%v", result, store.state, err)
	}
	if _, ok := store.state.Participants[partBob]; !ok || store.snapshotSource == "" || port.remoteWithLock || store.dlq != 2 || store.alert != 2 {
		t.Fatalf("participants=%v source=%q lock=%v dlq=%d alert=%d", store.state.Participants, store.snapshotSource, port.remoteWithLock, store.dlq, store.alert)
	}
}

// @spec:service-extraction.participation.outside-lock-reconciliation
func TestParticipationReconcileRejectsChangedHeadAtInstall(t *testing.T) {
	store := newFoldStore()
	store.casLost = true
	two := partCommand(2, partBob, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED)
	port := &authorityPort{store: store, replay: replayResponse(t,
		partCommand(1, partAlice, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED), two)}
	projector := NewProjector(store, port, func() string { return "018f4000-0000-7000-8000-000000000020" })
	result, err := projector.Apply(context.Background(), partWritePrincipal(), two)
	if err != nil || result != CompareAndSetLost || store.state.Version != 0 || port.remoteWithLock {
		t.Fatalf("result=%v state=%+v lock=%v err=%v", result, store.state, port.remoteWithLock, err)
	}
}

// @spec:service-extraction.participation.replay-and-reconcile
func TestParticipationReconcileExhaustionIsVisible(t *testing.T) {
	store := newFoldStore()
	two := partCommand(2, partBob, interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED)
	port := &authorityPort{store: store, replayErr: errors.New("authority unavailable")}
	projector := NewProjector(store, port, func() string { return "018f4000-0000-7000-8000-000000000020" })
	for attempt := 1; attempt <= 3; attempt++ {
		result, err := projector.Apply(context.Background(), partWritePrincipal(), two)
		if attempt < 3 && (result != 0 || err == nil) {
			t.Fatalf("attempt %d result=%v err=%v", attempt, result, err)
		}
		if attempt == 3 && (!errors.Is(err, ErrReconcileExhausted) || result != Poisoned) {
			t.Fatalf("exhausted result=%v err=%v", result, err)
		}
	}
	if store.dlq != 1 || store.alert != 1 {
		t.Fatalf("dlq=%d alert=%d", store.dlq, store.alert)
	}
}

func replayResponse(t *testing.T, commands ...*interactionv1.ParticipationCommand) *interactionv1.ReplayParticipationResponse {
	t.Helper()
	head, err := Canonical(commands[len(commands)-1])
	if err != nil {
		t.Fatal(err)
	}
	return &interactionv1.ReplayParticipationResponse{
		Commands: commands, HeadVersion: commands[len(commands)-1].GetAggregateVersion(),
		HeadEventId: commands[len(commands)-1].GetEventId(), HeadHash: head.Hash[:],
		HistoryFloor: 1, Provenance: "corex-participation-history-v1",
	}
}

package participation

import (
	"context"
	"crypto/sha256"
	"errors"
	"regexp"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/obs"
	"google.golang.org/protobuf/proto"
)

const (
	CapabilityWrite = "Corex-participation-write"
	CapabilityRead  = "Corex-participation-read"
)

var (
	ErrInvalid            = errors.New("invalid participation")
	ErrPermissionDenied   = errors.New("participation permission denied")
	ErrDivergentHistory   = errors.New("divergent participation history")
	ErrUnknownHistory     = errors.New("unknown participation history")
	ErrReconcileExhausted = errors.New("participation reconcile exhausted")
	uuidv7Pattern         = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

type Key struct {
	TenantID      string
	InteractionID string
}

type Identity struct {
	EventID string
	Hash    [sha256.Size]byte
	Body    []byte
}

type VersionIdentity struct {
	Version uint64
	Hash    [sha256.Size]byte
}

type Record struct {
	Command *interactionv1.ParticipationCommand
	Identity
}

type Fold struct {
	TenantID           string
	InteractionID      string
	Version            uint64
	Identity           Identity
	Participants       map[string]struct{}
	SnapshotProvenance string
}

func (f Fold) Clone() Fold {
	clone := f
	clone.Identity.Body = append([]byte(nil), f.Identity.Body...)
	clone.Participants = make(map[string]struct{}, len(f.Participants))
	for participantID := range f.Participants {
		clone.Participants[participantID] = struct{}{}
	}
	return clone
}

type Intent struct {
	Token           string
	Key             Key
	ObservedVersion uint64
	RequestedFrom   uint64
	RequestedTo     uint64
	Held            Identity
}

type Snapshot struct {
	Fold         Fold
	HistoryFloor uint64
	Provenance   string
}

type TransportGrant struct {
	ServiceID  string
	Capability string
	Role       string
}

type Principal struct {
	TenantID string
	Grant    TransportGrant
}

type Store interface {
	Load(context.Context, Key, uint64) (Fold, *Identity, error)
	Install(context.Context, uint64, []Record, Fold) (bool, error)
	BeginReconcile(context.Context, Intent, Record) (Intent, bool, error)
	InstallReconcile(context.Context, Intent, []Record, Fold) (bool, error)
	InstallHistorical(context.Context, Intent, Record) (bool, error)
	InstallSnapshot(context.Context, Intent, Snapshot, Record) (bool, error)
	FailReconcile(context.Context, Intent, Record, error) (bool, error)
	RecordPoison(context.Context, Record, string) error
}

type AuthorityPort interface {
	Replay(context.Context, *interactionv1.ReplayParticipationRequest) (*interactionv1.ReplayParticipationResponse, error)
	Snapshot(context.Context, *interactionv1.GetDesiredParticipationSnapshotRequest) (*interactionv1.GetDesiredParticipationSnapshotResponse, error)
}

type Result uint8

const (
	Applied Result = iota + 1
	Duplicate
	CompareAndSetLost
	Poisoned
	AuditedSnapshot
)

type Projector struct {
	store     Store
	authority AuthorityPort
	ids       func() string
}

func NewProjector(store Store, authority AuthorityPort, ids func() string) *Projector {
	return &Projector{store: store, authority: authority, ids: ids}
}

func (p *Projector) Apply(ctx context.Context, principal Principal, command *interactionv1.ParticipationCommand) (Result, error) {
	if p == nil || p.store == nil || p.authority == nil || p.ids == nil {
		return Poisoned, ErrInvalid
	}
	record, err := Canonical(command)
	if err != nil || principal.TenantID != command.GetTenantId() ||
		!ValidTransportGrant(principal.Grant, CapabilityWrite) {
		return Poisoned, ErrPermissionDenied
	}
	key := Key{TenantID: command.GetTenantId(), InteractionID: command.GetInteractionId()}
	fold, retained, err := p.store.Load(ctx, key, command.GetAggregateVersion())
	if err != nil {
		return 0, err
	}
	if command.GetAggregateVersion() <= fold.Version {
		if retained == nil {
			return p.reconcile(ctx, fold, record, true)
		}
		if sameIdentity(*retained, record.Identity) {
			return Duplicate, nil
		}
		if err := p.store.RecordPoison(ctx, record, "DIVERGENT_HISTORY"); err != nil {
			return Poisoned, err
		}
		return Poisoned, ErrDivergentHistory
	}
	if command.GetAggregateVersion() > fold.Version+1 {
		return p.reconcile(ctx, fold, record, false)
	}
	final, err := foldRecords(fold, []Record{record})
	if err != nil {
		return Poisoned, err
	}
	installed, err := p.store.Install(ctx, fold.Version, []Record{record}, final)
	if err != nil {
		return 0, err
	}
	if !installed {
		return CompareAndSetLost, nil
	}
	return Applied, nil
}

func (p *Projector) reconcile(ctx context.Context, fold Fold, held Record, historical bool) (Result, error) {
	from, to := fold.Version+1, held.Command.GetAggregateVersion()
	if historical {
		from, to = held.Command.GetAggregateVersion(), held.Command.GetAggregateVersion()
	}
	intent := Intent{
		Token: p.ids(), Key: Key{TenantID: held.Command.GetTenantId(), InteractionID: held.Command.GetInteractionId()},
		ObservedVersion: fold.Version, RequestedFrom: from, RequestedTo: to, Held: held.Identity,
	}
	if !uuidv7Pattern.MatchString(intent.Token) {
		return Poisoned, ErrInvalid
	}
	intent, started, err := p.store.BeginReconcile(ctx, intent, held)
	if err != nil {
		return 0, err
	}
	if !started {
		return CompareAndSetLost, nil
	}
	replay, err := p.authority.Replay(ctx, &interactionv1.ReplayParticipationRequest{
		TenantId: intent.Key.TenantID, InteractionId: intent.Key.InteractionID,
		FromVersion: intent.RequestedFrom, ToVersion: intent.RequestedTo, RequestId: intent.Token,
	})
	if errors.Is(err, ErrUnknownHistory) {
		return p.snapshot(ctx, intent, held)
	}
	if err != nil {
		return p.failReconcile(ctx, intent, held, err)
	}
	records, err := validateReplay(replay, intent, held)
	if err != nil {
		return Poisoned, err
	}
	if historical {
		installed, installErr := p.store.InstallHistorical(ctx, intent, records[0])
		if installErr != nil {
			return 0, installErr
		}
		if !installed {
			return CompareAndSetLost, nil
		}
		return Duplicate, nil
	}
	final, err := foldRecords(fold, records)
	if err != nil {
		return Poisoned, err
	}
	installed, err := p.store.InstallReconcile(ctx, intent, records, final)
	if err != nil {
		return 0, err
	}
	if !installed {
		return CompareAndSetLost, nil
	}
	return Applied, nil
}

func (p *Projector) snapshot(ctx context.Context, intent Intent, held Record) (Result, error) {
	wire, err := p.authority.Snapshot(ctx, &interactionv1.GetDesiredParticipationSnapshotRequest{
		TenantId: intent.Key.TenantID, InteractionId: intent.Key.InteractionID, MinimumVersion: intent.ObservedVersion, RequestId: intent.Token,
	})
	if err != nil {
		return p.failReconcile(ctx, intent, held, err)
	}
	snapshot, err := validateSnapshot(wire, intent, held)
	if err != nil {
		return Poisoned, err
	}
	installed, err := p.store.InstallSnapshot(ctx, intent, snapshot, held)
	if err != nil {
		return 0, err
	}
	if !installed {
		return CompareAndSetLost, nil
	}
	return AuditedSnapshot, nil
}

func (p *Projector) failReconcile(ctx context.Context, intent Intent, held Record, cause error) (Result, error) {
	exhausted, err := p.store.FailReconcile(ctx, intent, held, cause)
	if err != nil {
		return 0, errors.Join(cause, err)
	}
	if exhausted {
		return Poisoned, errors.Join(ErrReconcileExhausted, cause)
	}
	return 0, cause
}

func Canonical(command *interactionv1.ParticipationCommand) (Record, error) {
	if command == nil || !uuidv7Pattern.MatchString(command.GetEventId()) || !uuidv7Pattern.MatchString(command.GetTenantId()) ||
		!uuidv7Pattern.MatchString(command.GetInteractionId()) || !uuidv7Pattern.MatchString(command.GetParticipantId()) ||
		command.GetAggregateVersion() == 0 ||
		(command.GetDesiredState() != interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED &&
			command.GetDesiredState() != interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_UNASSIGNED) ||
		command.GetOccurredAt() == nil || command.GetOccurredAt().CheckValid() != nil {
		return Record{}, ErrInvalid
	}
	if _, ok := obs.ParseTraceparent(command.GetTraceparent()); !ok {
		return Record{}, ErrInvalid
	}
	normalized := proto.Clone(command).(*interactionv1.ParticipationCommand)
	normalized.ProtoReflect().SetUnknown(nil)
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	if err != nil {
		return Record{}, ErrInvalid
	}
	hash := sha256.Sum256(body)
	return Record{Command: normalized, Identity: Identity{EventID: normalized.GetEventId(), Hash: hash, Body: body}}, nil
}

func ValidTransportGrant(grant TransportGrant, capability string) bool {
	return grant.ServiceID != "" && grant.Role == "" && grant.Capability == capability
}

func foldRecords(current Fold, records []Record) (Fold, error) {
	next := current.Clone()
	if next.Participants == nil {
		next.Participants = map[string]struct{}{}
	}
	for _, record := range records {
		command := record.Command
		if command.GetAggregateVersion() != next.Version+1 ||
			(next.TenantID != "" && (next.TenantID != command.GetTenantId() || next.InteractionID != command.GetInteractionId())) {
			return Fold{}, ErrDivergentHistory
		}
		if command.GetDesiredState() == interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED {
			next.Participants[command.GetParticipantId()] = struct{}{}
		} else {
			delete(next.Participants, command.GetParticipantId())
		}
		next.TenantID, next.InteractionID = command.GetTenantId(), command.GetInteractionId()
		next.Version, next.Identity = command.GetAggregateVersion(), record.Identity
		next.SnapshotProvenance = ""
	}
	return next, nil
}

func validateReplay(replay *interactionv1.ReplayParticipationResponse, intent Intent, held Record) ([]Record, error) {
	if replay == nil || replay.GetHistoryFloor() == 0 || replay.GetProvenance() == "" ||
		replay.GetHeadVersion() < intent.RequestedTo || len(replay.GetCommands()) != int(intent.RequestedTo-intent.RequestedFrom+1) {
		return nil, ErrUnknownHistory
	}
	records := make([]Record, 0, len(replay.GetCommands()))
	for index, command := range replay.GetCommands() {
		record, err := Canonical(command)
		if err != nil || command.GetTenantId() != intent.Key.TenantID || command.GetInteractionId() != intent.Key.InteractionID ||
			command.GetAggregateVersion() != intent.RequestedFrom+uint64(index) {
			return nil, ErrDivergentHistory
		}
		records = append(records, record)
	}
	last := records[len(records)-1]
	if replay.GetHeadVersion() == last.Command.GetAggregateVersion() &&
		(replay.GetHeadEventId() != last.EventID || len(replay.GetHeadHash()) != len(last.Hash) || !bytesEqual(replay.GetHeadHash(), last.Hash[:])) {
		return nil, ErrDivergentHistory
	}
	found := false
	for _, record := range records {
		if record.Command.GetAggregateVersion() == held.Command.GetAggregateVersion() && sameIdentity(record.Identity, held.Identity) {
			found = true
		}
	}
	if !found {
		return nil, ErrDivergentHistory
	}
	return records, nil
}

func validateSnapshot(wire *interactionv1.GetDesiredParticipationSnapshotResponse, intent Intent, held Record) (Snapshot, error) {
	if wire == nil || wire.GetTenantId() != intent.Key.TenantID || wire.GetInteractionId() != intent.Key.InteractionID ||
		wire.GetAggregateVersion() < intent.ObservedVersion || wire.GetAggregateVersion() < held.Command.GetAggregateVersion() ||
		!uuidv7Pattern.MatchString(wire.GetHeadEventId()) || len(wire.GetHeadHash()) != sha256.Size ||
		wire.GetHistoryFloor() == 0 || wire.GetHistoryFloor() > wire.GetAggregateVersion() || wire.GetProvenance() == "" {
		return Snapshot{}, ErrUnknownHistory
	}
	participants := make(map[string]struct{}, len(wire.GetParticipantIds()))
	for _, participantID := range wire.GetParticipantIds() {
		if !uuidv7Pattern.MatchString(participantID) {
			return Snapshot{}, ErrInvalid
		}
		participants[participantID] = struct{}{}
	}
	var hash [sha256.Size]byte
	copy(hash[:], wire.GetHeadHash())
	identity := Identity{EventID: wire.GetHeadEventId(), Hash: hash}
	return Snapshot{
		Fold:         Fold{TenantID: wire.GetTenantId(), InteractionID: wire.GetInteractionId(), Version: wire.GetAggregateVersion(), Identity: identity, Participants: participants, SnapshotProvenance: wire.GetProvenance()},
		HistoryFloor: wire.GetHistoryFloor(), Provenance: wire.GetProvenance(),
	}, nil
}

func sameIdentity(left, right Identity) bool {
	return left.EventID == right.EventID && left.Hash == right.Hash
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

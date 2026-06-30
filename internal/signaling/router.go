package signaling

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kafaconnect/relaypoint/internal/obs"
)

// Router is the SOLE authoritative writer of every `.log` fact; pure logic over the LogStore port (no NATS), state rebuilt lazily from the durable log.
type Router struct {
	store   LogStore
	now     func() time.Time
	id      func() string
	devMode bool // when true an unauthenticated command runs in the permissive shared-`client` posture; prod leaves this false → fail-closed (A1)

	// Bounded in-process cache (RH-07): the durable log is the source of truth, so `inter` is an
	// idle-TTL + LRU cache and each interaction's `results` a FIFO dedup cache. Both are rebuilt from
	// the log on next access — that is why evicting either is safe (a retry re-folds + re-checks dedup,
	// and the broker dedups the append regardless of the in-memory cache).
	maxInter   int
	idleTTL    time.Duration
	maxResults int
	// RH-07: the now-cheap rebuild lets an OCC loser re-fold this many times before poisoning. The
	// old 250ms-rebuild tail was an accidental backoff that kept the 1-retry budget enough; a tailless
	// rebuild removes it, so without a wider budget tight contention exhausts the retry and poisons.
	maxRefold int

	mu    sync.Mutex
	inter map[string]*interactionState
	load  singleflight.Group // one rebuild per key: concurrent callers share it, no stale re-insert
}

type interactionState struct {
	mu          sync.Mutex
	seq         int64
	streamSeq   uint64 // subject's last STREAM sequence — the OCC token for the next append (≠ seq)
	status      string
	results     map[string]storedResult
	resultOrder []string                 // FIFO of command_ids backing results' bounded eviction; rebuildable from the log
	part        *ParticipationView       // folded membership, re-checked after every OCC rebuild (A3)
	parents     map[string]parentBinding // privileged parent command_id -> the sub-facts it produced (A5/A7)
	poisoned    bool                     // seq untrustworthy → callers must rebuild, not reuse it
	lastUsed    time.Time                // r.mu-guarded access stamp driving idle-TTL + LRU eviction of inter
}

// putResult records a command's dedup entry, evicting the oldest by insertion order once the cache
// exceeds max; the dropped entries are rebuildable from the log, so a later retry still dedups via
// the broker + a re-fold.
func (st *interactionState) putResult(id string, sr storedResult, max int) {
	if _, exists := st.results[id]; !exists {
		st.resultOrder = append(st.resultOrder, id)
	}
	st.results[id] = sr
	for max > 0 && len(st.results) > max {
		oldest := st.resultOrder[0]
		st.resultOrder = st.resultOrder[1:]
		delete(st.results, oldest)
	}
}

// parentBinding records a privileged command's sub-facts so a divergent retry of the same parent command_id is rejected before re-emitting any sub-fact (A5); rebuilt from each sub-fact's CausedBy.
type parentBinding struct {
	subIDs map[string]bool
}

type storedResult struct {
	payloadHash string // "" only for legacy facts written before payload_hash existed
	result      *CommandResult
}

type Option func(*Router)

func WithClock(now func() time.Time) Option { return func(r *Router) { r.now = now } }
func WithIDGen(gen func() string) Option    { return func(r *Router) { r.id = gen } }

// WithDevMode opts into the permissive shared-`client` posture (subject suffix as advisory author, role/participation gates off); MUST stay opt-in so production fails closed (A1).
func WithDevMode() Option { return func(r *Router) { r.devMode = true } }

// WithStateLimits overrides the bounded-cache tunables (RH-07): max cached interactions, max
// per-interaction dedup entries, and the idle-eviction TTL. A non-positive value keeps the default.
func WithStateLimits(maxInter, maxResults int, idleTTL time.Duration) Option {
	return func(r *Router) {
		if maxInter > 0 {
			r.maxInter = maxInter
		}
		if maxResults > 0 {
			r.maxResults = maxResults
		}
		if idleTTL > 0 {
			r.idleTTL = idleTTL
		}
	}
}

func defaultID() string {
	return uuid.Must(uuid.NewV7()).String()
}

const (
	defaultMaxInter   = 4096
	defaultMaxResults = 1024
	defaultIdleTTL    = 30 * time.Minute
	defaultMaxRefold  = 32
)

func NewRouter(store LogStore, opts ...Option) *Router {
	r := &Router{
		store: store, now: time.Now, id: defaultID, inter: map[string]*interactionState{},
		maxInter: defaultMaxInter, maxResults: defaultMaxResults, idleTTL: defaultIdleTTL,
		maxRefold: defaultMaxRefold,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

func legalTransition(status, cmdType string) bool {
	switch cmdType {
	case "interaction.started":
		return status == ""
	case "interaction.ended":
		return status == "started"
	default:
		// RP gates on delivery STRUCTURE, not a domain-verb census: any annotation is an opaque type, legal only on an open (started, not-yet-ended) interaction.
		return status == "started"
	}
}

func applyTransition(st *interactionState, cmdType string) {
	switch cmdType {
	case "interaction.started":
		st.status = "started"
	case "interaction.ended":
		st.status = "ended"
	}
}

// NOT proto.Marshal: its output isn't stable across protobuf-lib upgrades, and this hash is persisted then recompared after a restart/upgrade.
func hashPayload(c *Command) string {
	h := sha256.New()
	put := func(b []byte) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(b)))
		h.Write(n[:])
		h.Write(b)
	}
	put([]byte(c.TenantId))
	put([]byte(c.ActorId))
	put([]byte(c.Type))
	put([]byte(c.Medium))
	put([]byte(c.RefId))
	put(c.Data)
	return hex.EncodeToString(h.Sum(nil))
}

func logSubjectFor(tenant, iid string) string {
	return fmt.Sprintf("tenant.%s.interaction.%s.log", tenant, iid)
}

// Rebuild runs under singleflight so concurrent callers share one state object (no stale re-insert); a replay failure propagates so callers fail closed.
func (r *Router) getState(tenant, iid string) (*interactionState, error) {
	key := tenant + "/" + iid
	r.mu.Lock()
	st := r.inter[key]
	if st != nil {
		st.lastUsed = r.now()
	}
	r.mu.Unlock()
	if st != nil {
		return st, nil
	}
	v, err, _ := r.load.Do(key, func() (any, error) {
		r.mu.Lock()
		if e := r.inter[key]; e != nil {
			e.lastUsed = r.now()
			r.mu.Unlock()
			return e, nil
		}
		r.mu.Unlock()
		built, berr := r.rebuild(tenant, iid)
		if berr != nil {
			return nil, berr
		}
		r.mu.Lock()
		built.lastUsed = r.now()
		r.inter[key] = built
		r.pruneLocked()
		r.mu.Unlock()
		return built, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*interactionState), nil
}

// pruneLocked bounds inter: it first evicts entries idle past idleTTL, then the least-recently-used
// until at/under maxInter. The just-inserted entry carries the freshest lastUsed, so it is never the
// one dropped. Caller holds r.mu.
func (r *Router) pruneLocked() {
	if r.idleTTL > 0 {
		cutoff := r.now().Add(-r.idleTTL)
		for k, st := range r.inter {
			if st.lastUsed.Before(cutoff) {
				delete(r.inter, k)
			}
		}
	}
	for r.maxInter > 0 && len(r.inter) > r.maxInter {
		var lruKey string
		var lruAt time.Time
		for k, st := range r.inter {
			if lruKey == "" || st.lastUsed.Before(lruAt) {
				lruKey, lruAt = k, st.lastUsed
			}
		}
		delete(r.inter, lruKey)
	}
}

func (r *Router) rebuild(tenant, iid string) (*interactionState, error) {
	st := &interactionState{results: map[string]storedResult{}}
	facts, lastSubjSeq, err := r.store.Replay(logSubjectFor(tenant, iid))
	if err != nil {
		return nil, err
	}
	st.streamSeq = lastSubjSeq
	st.part = FoldParticipation(facts)
	st.parents = map[string]parentBinding{}
	for _, e := range facts {
		if e.Sequence > st.seq {
			st.seq = e.Sequence
		}
		applyTransition(st, e.EventType)
		if e.CommandId != "" {
			st.putResult(e.CommandId, storedResult{payloadHash: e.PayloadHash, result: &CommandResult{
				CommandId: e.CommandId, Status: statusAccepted, CausedBy: e.CommandId,
			}}, r.maxResults)
		}
		if isParticipationFact(e.EventType) && e.CausedBy != "" {
			b, ok := st.parents[e.CausedBy]
			if !ok {
				b = parentBinding{subIDs: map[string]bool{}}
			}
			b.subIDs[e.CommandId] = true
			st.parents[e.CausedBy] = b
		}
	}
	return st, nil
}

// HandleCommand validates subject/payload against the authenticated Identity on ctx (never trusting them alone), then appends under per-subject OCC.
func (r *Router) HandleCommand(ctx context.Context, subject string, data []byte) (res *CommandResult) {
	// one boundary log line per command, carrying ctx trace correlation (ADR-0011)
	defer func() {
		obs.Logger(ctx).Info("router.command",
			"subject", subject, "command_id", res.GetCommandId(),
			"status", res.GetStatus().String(), "reason", res.GetReason())
	}()

	id := IdentityFrom(ctx)
	tenant, iid, suffix, ok := parseCmdSubject(subject)
	if !ok {
		return &CommandResult{Status: statusRejected, Reason: "bad subject"}
	}
	cmd := &Command{}
	if err := proto.Unmarshal(data, cmd); err != nil {
		return &CommandResult{Status: statusRejected, Reason: "bad payload"}
	}

	// Outside dev mode an unauthenticated command is rejected: the suffix alone is not a trusted author, accepting it would skip the role/participation gates (A1).
	if !isAuthenticated(id) && !r.devMode {
		return &CommandResult{CommandId: cmd.CommandId, Status: statusRejected, Reason: "unauthenticated"}
	}

	authTenant := id.TenantID
	if authTenant == "" {
		authTenant = tenant
	}
	res = &CommandResult{CommandId: cmd.CommandId}
	switch {
	case cmd.CommandId == "":
		res.Status, res.Reason = statusRejected, "missing command_id"
		return res
	case tenant != authTenant:
		res.Status, res.Reason = statusRejected, "subject tenant != authenticated tenant"
		return res
	case cmd.TenantId != authTenant:
		res.Status, res.Reason = statusRejected, "payload tenant_id != authenticated tenant"
		return res
	}

	// Actor binding: the payload actor must equal the authenticated suffix (forged-author rejected, A1), EXCEPT a trusted backend acts for others so may carry an arbitrary or empty actor_id.
	if RoleOf(id) != RoleTrustedBackend {
		switch {
		case cmd.ActorId == "":
			res.Status, res.Reason = statusRejected, "missing actor_id"
			return res
		case cmd.ActorId != suffix:
			res.Status, res.Reason = statusRejected, "actor_mismatch"
			return res
		case id.UserID != "" && suffix != id.UserID:
			res.Status, res.Reason = statusRejected, "actor_id != authenticated user"
			return res
		}
	}

	if isParticipationCommand(cmd.Type) {
		return r.handleParticipation(ctx, tenant, iid, suffix, RoleOf(id), cmd)
	}

	// Participation FACTS may ONLY come from the privileged path, never a direct command — else an agent could forge an unaudited join/leave/assign (A2b).
	if isParticipationFact(cmd.Type) {
		res.Status, res.Reason = statusRejected, "participation fact requires a privileged command"
		return res
	}

	// Every agent write except interaction.started requires an OPEN membership interval, re-checked after every OCC rebuild so a racing participant.left can't slip through (A2a/A3); trusted backend exempt.
	gateParticipation := isAuthenticated(id) && RoleOf(id) == RoleAgent && cmd.Type != "interaction.started"

	st, err := r.getState(tenant, iid)
	if err != nil {
		res.Status, res.Reason = statusRejected, "state unavailable (log replay failed) — retry"
		return res
	}
	st.mu.Lock()
	defer st.mu.Unlock()

	// A concurrent goroutine poisoned this state (seq untrustworthy) — fail closed so the caller retries against a fresh rebuild.
	if st.poisoned {
		res.Status, res.Reason = statusRejected, "state unavailable — retry"
		return res
	}

	ph := hashPayload(cmd)

	// Append under per-subject OCC; a loser re-folds and retries ONCE (a concurrent writer may have advanced the subject, ended the interaction, or committed this command_id). See router-occ.
	var dup bool
	var committedSeq uint64
	for attempt := 0; ; attempt++ {
		// A command_id is bound to its first payload, re-checked each attempt (a re-fold can reveal a concurrent writer committed it).
		if prev, seen := st.results[cmd.CommandId]; seen {
			switch {
			case prev.payloadHash == "": // legacy fact, hash unknown
				return proto.Clone(prev.result).(*CommandResult)
			case prev.payloadHash != ph:
				res.Status, res.Reason = statusRejected, "conflict: command_id reused with a different payload"
				return res
			case prev.result.Status == statusAccepted:
				return proto.Clone(prev.result).(*CommandResult)
				// a previously-rejected same payload falls through: a transient rejection (e.g. illegal transition) may now be legal
			}
		}

		if gateParticipation && (st.part == nil || !st.part.IsParticipantNow(suffix)) {
			res.Status, res.Reason = statusRejected, "not_a_participant"
			return res
		}

		if !legalTransition(st.status, cmd.Type) {
			res.Status, res.Reason = statusRejected, fmt.Sprintf("illegal transition %q from state %q", cmd.Type, st.status)
			st.putResult(cmd.CommandId, storedResult{payloadHash: ph, result: res}, r.maxResults)
			return res
		}

		seq := st.seq + 1
		ev := &Event{
			Schema: SchemaV1, EventType: cmd.Type, EventId: r.id(), Sequence: seq,
			OccurredAt: timestamppb.New(r.now().UTC()), TenantId: tenant, ActorId: cmd.ActorId,
			Medium: orDefault(cmd.Medium, "chat"), CommandId: cmd.CommandId,
			PayloadHash: ph, CausedBy: cmd.CommandId, RefId: cmd.RefId, Data: cmd.Data,
		}
		payload, _ := proto.Marshal(ev)
		// dedupID is deterministic per (tenant,interaction,command) so a retry is exactly-once even if a prior ack was lost.
		d, cseq, aerr := r.store.Append(ctx, logSubjectFor(tenant, iid), payload, tenant+"."+iid+"."+cmd.CommandId, st.streamSeq)
		if errors.Is(aerr, ErrOCCConflict) {
			// Lost the race: re-fold and retry (up to maxRefold); if we still lose, surface a retryable rejection (never append behind a stale seq).
			fresh, ferr := r.rebuild(tenant, iid)
			if ferr != nil || attempt >= r.maxRefold {
				st.poisoned = true
				r.mu.Lock()
				delete(r.inter, tenant+"/"+iid)
				r.mu.Unlock()
				res.Status, res.Reason = statusRejected, "lost concurrent append — retry"
				return res
			}
			st.seq, st.streamSeq, st.status, st.part, st.parents = fresh.seq, fresh.streamSeq, fresh.status, fresh.part, fresh.parents
			for k, v := range fresh.results {
				if _, ok := st.results[k]; !ok {
					st.putResult(k, v, r.maxResults)
				}
			}
			continue
		}
		if aerr != nil {
			// The append may have committed before the ack was lost — reconcile from the log and accept if the fact is now present.
			if fresh, ferr := r.rebuild(tenant, iid); ferr == nil {
				if prev, committed := fresh.results[cmd.CommandId]; committed {
					st.seq, st.streamSeq, st.status = fresh.seq, fresh.streamSeq, fresh.status
					return proto.Clone(prev.result).(*CommandResult)
				}
			}
			st.poisoned = true // seq may be stale; force a rebuild rather than append behind it
			r.mu.Lock()
			delete(r.inter, tenant+"/"+iid)
			r.mu.Unlock()
			res.Status, res.Reason = statusRejected, "log append failed — retry"
			return res
		}
		dup, committedSeq = d, cseq
		break
	}
	res.Status, res.CausedBy = statusAccepted, cmd.CommandId
	if dup {
		// Store already had this command_id: reconcile and compare the COMMITTED payload hash — a divergent reuse conflicts, a match replays; never clobber the committed entry.
		fresh, ferr := r.rebuild(tenant, iid)
		if ferr != nil {
			st.poisoned = true // seq may be stale; force a rebuild rather than append behind it
			r.mu.Lock()
			delete(r.inter, tenant+"/"+iid)
			r.mu.Unlock()
			return res
		}
		st.seq, st.streamSeq, st.status, st.part, st.parents = fresh.seq, fresh.streamSeq, fresh.status, fresh.part, fresh.parents
		for k, v := range fresh.results {
			if _, ok := st.results[k]; !ok {
				st.putResult(k, v, r.maxResults)
			}
		}
		if committed, ok := st.results[cmd.CommandId]; ok {
			if committed.payloadHash != "" && committed.payloadHash != ph {
				return &CommandResult{CommandId: cmd.CommandId, Status: statusRejected, Reason: "conflict: command_id reused with a different payload"}
			}
			return committed.result
		}
		st.putResult(cmd.CommandId, storedResult{payloadHash: ph, result: res}, r.maxResults)
		return res
	}
	st.seq++
	// OCC token = committed stream seq, not prev+1: shared stream (RH-01)
	st.streamSeq = committedSeq
	applyTransition(st, cmd.Type)
	st.putResult(cmd.CommandId, storedResult{payloadHash: ph, result: res}, r.maxResults)

	if st.status == "ended" { // the durable log rebuilds state if a late command arrives
		r.mu.Lock()
		delete(r.inter, tenant+"/"+iid)
		r.mu.Unlock()
	}
	return res
}

// Both Identity fields are empty only in the shared-`client` dev posture, so either being set means a real auth-callout-minted identity.
func isAuthenticated(id Identity) bool { return id.UserID != "" || id.Role != "" }

func isParticipationCommand(t string) bool {
	switch t {
	case "participant.assign", "participant.unassign", "participant.transfer":
		return true
	}
	return false
}

func isParticipationFact(t string) bool {
	switch t {
	case "participant.joined", "participant.left", "interaction.assigned":
		return true
	}
	return false
}

type participationData struct {
	Agent     string `json:"agent"` // assign/unassign target; transfer = the NEW agent
	From      string `json:"from"`  // transfer only: the agent being transferred away
	Reason    string `json:"reason"`
	RequestID string `json:"request_id"`
}

// handleParticipation lands participation facts from a role-gated trusted-backend command; transfer writes joined(new) BEFORE left(old) so the interaction is never absent from both memberships (no-gap, Decision 2a/6).
func (r *Router) handleParticipation(ctx context.Context, tenant, iid, actor string, role Role, cmd *Command) *CommandResult {
	res := &CommandResult{CommandId: cmd.CommandId}
	if role != RoleTrustedBackend {
		res.Status, res.Reason = statusRejected, "privileged command requires trusted-backend actor"
		return res
	}
	pd := participationData{}
	if len(cmd.Data) > 0 {
		if err := json.Unmarshal(cmd.Data, &pd); err != nil {
			res.Status, res.Reason = statusRejected, "bad participation payload"
			return res
		}
	}

	type fact struct {
		eventType string
		agent     string
	}
	var facts []fact
	switch cmd.Type {
	case "participant.assign":
		if pd.Agent == "" {
			res.Status, res.Reason = statusRejected, "missing agent"
			return res
		}
		facts = []fact{{"participant.joined", pd.Agent}}
	case "participant.unassign":
		if pd.Agent == "" {
			res.Status, res.Reason = statusRejected, "missing agent"
			return res
		}
		facts = []fact{{"participant.left", pd.Agent}}
	case "participant.transfer":
		if pd.Agent == "" || pd.From == "" {
			res.Status, res.Reason = statusRejected, "missing from/agent"
			return res
		}
		// joined(new) BEFORE left(old): new leg opens before the old is revoked (no-gap).
		facts = []fact{{"participant.joined", pd.Agent}, {"participant.left", pd.From}}
	}

	subIDs := make([]string, len(facts))
	for i, f := range facts {
		subIDs[i] = cmd.CommandId + ":" + f.eventType + ":" + f.agent
	}

	st, err := r.getState(tenant, iid)
	if err != nil {
		res.Status, res.Reason = statusRejected, "state unavailable (log replay failed) — retry"
		return res
	}
	st.mu.Lock()
	if b, seen := st.parents[cmd.CommandId]; seen {
		want := map[string]bool{}
		for _, s := range subIDs {
			want[s] = true
		}
		// subset, not exact-equality: a partial transfer re-drives; a divergent payload is still rejected (RH-03)
		if !recordedSubsetOf(b.subIDs, want) {
			st.mu.Unlock()
			res.Status, res.Reason = statusRejected, "conflict: command_id reused with a different payload"
			return res
		}
	}
	st.mu.Unlock()

	for i, f := range facts {
		// Each fact is its own command_id so dedup/OCC stay per-fact; the transfer's two must land in order (joined then left), so a partial failure on the second is surfaced.
		fcmd := &Command{
			CommandId: subIDs[i],
			TenantId:  tenant,
			ActorId:   f.agent, // the fact's subject is the affected agent
			Type:      f.eventType,
			Medium:    cmd.Medium,
		}
		ev := r.appendParticipationFact(ctx, tenant, iid, actor, cmd.CommandId, &pd, fcmd)
		if ev.Status != statusAccepted {
			if i > 0 {
				ev.Reason = "transfer partially applied (" + ev.Reason + ")"
			}
			ev.CommandId = cmd.CommandId
			return ev
		}
	}

	st.mu.Lock()
	b := st.parents[cmd.CommandId]
	if b.subIDs == nil {
		b.subIDs = map[string]bool{}
	}
	for _, s := range subIDs {
		b.subIDs[s] = true
	}
	st.parents[cmd.CommandId] = b
	st.mu.Unlock()

	res.Status, res.CausedBy = statusAccepted, cmd.CommandId
	return res
}

func recordedSubsetOf(recorded, want map[string]bool) bool {
	for k := range recorded {
		if !want[k] {
			return false
		}
	}
	return true
}

// appendParticipationFact writes ONE participation fact under the same per-subject OCC+dedup as ordinary commands; it re-folds and retries once, then fails closed (never appends behind a stale sequence).
func (r *Router) appendParticipationFact(ctx context.Context, tenant, iid, commandedBy, parentID string, pd *participationData, fcmd *Command) *CommandResult {
	res := &CommandResult{CommandId: fcmd.CommandId}
	st, err := r.getState(tenant, iid)
	if err != nil {
		res.Status, res.Reason = statusRejected, "state unavailable (log replay failed) — retry"
		return res
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.poisoned {
		res.Status, res.Reason = statusRejected, "state unavailable — retry"
		return res
	}

	ph := hashPayload(fcmd)
	for attempt := 0; ; attempt++ {
		if prev, seen := st.results[fcmd.CommandId]; seen {
			if prev.payloadHash != "" && prev.payloadHash != ph {
				res.Status, res.Reason = statusRejected, "conflict: command_id reused with a different payload"
				return res
			}
			if prev.result.Status == statusAccepted {
				if st.status == "ended" {
					r.evict(tenant, iid)
				}
				return proto.Clone(prev.result).(*CommandResult)
			}
		}
		if !legalTransition(st.status, fcmd.Type) {
			res.Status, res.Reason = statusRejected, fmt.Sprintf("illegal transition %q from state %q", fcmd.Type, st.status)
			return res
		}
		seq := st.seq + 1
		ev := &Event{
			Schema: SchemaV1, EventType: fcmd.Type, EventId: r.id(), Sequence: seq,
			OccurredAt: timestamppb.New(r.now().UTC()), TenantId: tenant, ActorId: fcmd.ActorId,
			Medium: orDefault(fcmd.Medium, "chat"), CommandId: fcmd.CommandId, PayloadHash: ph,
			CausedBy: parentID, CommandedBy: commandedBy, Reason: pd.Reason, RequestId: pd.RequestID,
		}
		payload, _ := proto.Marshal(ev)
		_, cseq, aerr := r.store.Append(ctx, logSubjectFor(tenant, iid), payload, tenant+"."+iid+"."+fcmd.CommandId, st.streamSeq)
		if errors.Is(aerr, ErrOCCConflict) {
			fresh, ferr := r.rebuild(tenant, iid)
			if ferr != nil || attempt >= r.maxRefold {
				st.poisoned = true
				r.evict(tenant, iid)
				res.Status, res.Reason = statusRejected, "lost concurrent append — retry"
				return res
			}
			st.seq, st.streamSeq, st.status, st.part, st.parents = fresh.seq, fresh.streamSeq, fresh.status, fresh.part, fresh.parents
			for k, v := range fresh.results {
				if _, ok := st.results[k]; !ok {
					st.putResult(k, v, r.maxResults)
				}
			}
			continue
		}
		if aerr != nil {
			st.poisoned = true
			r.evict(tenant, iid)
			res.Status, res.Reason = statusRejected, "log append failed — retry"
			return res
		}
		st.seq++
		st.streamSeq = cseq
		applyTransition(st, fcmd.Type)
		st.part.ApplyFact(ev)
		res.Status, res.CausedBy = statusAccepted, fcmd.CommandId
		st.putResult(fcmd.CommandId, storedResult{payloadHash: ph, result: res}, r.maxResults)
		if st.status == "ended" {
			r.evict(tenant, iid)
		}
		return res
	}
}

func (r *Router) evict(tenant, iid string) {
	r.mu.Lock()
	delete(r.inter, tenant+"/"+iid)
	r.mu.Unlock()
}

func parseCmdSubject(s string) (tenant, iid, identity string, ok bool) {
	p := strings.Split(s, ".")
	if len(p) != 6 || p[0] != "tenant" || p[2] != "interaction" || p[4] != "cmd" {
		return "", "", "", false
	}
	if p[1] == "" || p[3] == "" || p[5] == "" {
		return "", "", "", false
	}
	return p[1], p[3], p[5], true
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

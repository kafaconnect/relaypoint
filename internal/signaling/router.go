package signaling

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kafaconnect/relaypoint/internal/obs"
)

// Router is the sole authoritative writer of every `.log` fact. Pure logic over the LogStore
// port (no NATS); per-interaction state is rebuilt lazily from the durable log on first access
// or after a restart.
type Router struct {
	store LogStore
	now   func() time.Time
	id    func() string

	mu    sync.Mutex
	inter map[string]*interactionState
	load  singleflight.Group // one rebuild per key: concurrent callers share it, no stale re-insert
}

type interactionState struct {
	mu        sync.Mutex
	seq       int64
	streamSeq uint64 // subject's last STREAM sequence — the OCC token for the next append (≠ seq)
	status    string // "" | started | ended
	results   map[string]storedResult
	poisoned  bool // seq untrustworthy → callers must rebuild, not reuse it
}

type storedResult struct {
	payloadHash string // "" only for legacy facts written before payload_hash existed
	result      *CommandResult
}

type Option func(*Router)

func WithClock(now func() time.Time) Option { return func(r *Router) { r.now = now } }
func WithIDGen(gen func() string) Option    { return func(r *Router) { r.id = gen } }

// Random UUIDv4, not sequential: a process-local counter would reset on restart and collide with
// event_ids already in the durable log. Tests inject deterministic ids via WithIDGen.
func defaultID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("relaypoint: crypto/rand failed: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func NewRouter(store LogStore, opts ...Option) *Router {
	r := &Router{store: store, now: time.Now, id: defaultID, inter: map[string]*interactionState{}}
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
	case "message.created", "message.updated", "message.deleted",
		"participant.joined", "participant.left", "interaction.assigned", "interaction.context.updated":
		return status == "started"
	default:
		return false
	}
}

func requiresRefID(cmdType string) bool {
	return cmdType == "message.updated" || cmdType == "message.deleted"
}

func applyTransition(st *interactionState, cmdType string) {
	switch cmdType {
	case "interaction.started":
		st.status = "started"
	case "interaction.ended":
		st.status = "ended"
	}
}

// NOT proto.Marshal: its deterministic output isn't stable across protobuf-lib upgrades, and this
// hash is persisted on the fact then recompared after a restart/upgrade.
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

// Rebuild runs under singleflight so concurrent callers share one state object (no stale
// re-insert after a concurrent eviction); a replay failure propagates so callers fail closed.
func (r *Router) getState(tenant, iid string) (*interactionState, error) {
	key := tenant + "/" + iid
	r.mu.Lock()
	st := r.inter[key]
	r.mu.Unlock()
	if st != nil {
		return st, nil
	}
	v, err, _ := r.load.Do(key, func() (any, error) {
		r.mu.Lock()
		if e := r.inter[key]; e != nil {
			r.mu.Unlock()
			return e, nil
		}
		r.mu.Unlock()
		built, berr := r.rebuild(tenant, iid)
		if berr != nil {
			return nil, berr
		}
		r.mu.Lock()
		r.inter[key] = built
		r.mu.Unlock()
		return built, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*interactionState), nil
}

func (r *Router) rebuild(tenant, iid string) (*interactionState, error) {
	st := &interactionState{results: map[string]storedResult{}}
	facts, lastSubjSeq, err := r.store.Replay(logSubjectFor(tenant, iid))
	if err != nil {
		return nil, err
	}
	st.streamSeq = lastSubjSeq
	for _, e := range facts {
		if e.Sequence > st.seq {
			st.seq = e.Sequence
		}
		applyTransition(st, e.EventType)
		if e.CommandId != "" {
			st.results[e.CommandId] = storedResult{payloadHash: e.PayloadHash, result: &CommandResult{
				CommandId: e.CommandId, Status: statusAccepted, CausedBy: e.CommandId,
			}}
		}
	}
	return st, nil
}

// The trusted tenant/actor come from the authenticated Identity on ctx; the subject and payload
// are validated against it, never trusted on their own.
func (r *Router) HandleCommand(ctx context.Context, subject string, data []byte) (res *CommandResult) {
	// One boundary line per command, carrying ctx's correlation fields (trace_id/span_id seeded
	// from the publisher's traceparent) so a command is followable end-to-end (ADR-0011).
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
	// Subject-tenant fallback applies only when the transport authenticated no Identity (Phase-1,
	// pre-auth-callout — NOT production-safe for authorship; prod MUST populate Identity).
	authTenant := id.TenantID
	if authTenant == "" {
		authTenant = tenant
	}
	res = &CommandResult{CommandId: cmd.CommandId}
	switch {
	case cmd.CommandId == "":
		res.Status, res.Reason = statusRejected, "missing command_id"
		return res
	case cmd.ActorId == "":
		res.Status, res.Reason = statusRejected, "missing actor_id"
		return res
	case tenant != authTenant:
		res.Status, res.Reason = statusRejected, "subject tenant != authenticated tenant"
		return res
	case cmd.TenantId != authTenant:
		res.Status, res.Reason = statusRejected, "payload tenant_id != authenticated tenant"
		return res
	// The publisher identity is the subject suffix (ACL-pinned author), NEVER the payload. A
	// payload actor_id that disagrees is a forgery attempt → actor_mismatch.
	case cmd.ActorId != suffix:
		res.Status, res.Reason = statusRejected, "actor_mismatch"
		return res
	// Once the auth-callout mints a per-connection identity, the suffix MUST equal it (the ACL
	// pins <self>). Pre-auth-callout (empty UserID) the suffix is the dev identity source.
	case id.UserID != "" && suffix != id.UserID:
		res.Status, res.Reason = statusRejected, "actor_id != authenticated user"
		return res
	}

	// Privileged participation commands (participant.assign/unassign/transfer) are a separate path:
	// role-gated to a trusted backend, they WRITE participant.* / interaction.assigned facts with
	// audit fields. Agent-role connections are rejected here (Decision 2a).
	if isParticipationCommand(cmd.Type) {
		return r.handleParticipation(ctx, tenant, iid, suffix, RoleOf(id), cmd)
	}

	if requiresRefID(cmd.Type) && cmd.RefId == "" {
		res.Status, res.Reason = statusRejected, "missing ref_id"
		return res
	}

	// Agent-role authz: the publishing agent must be a CURRENT participant of the target
	// interaction (an OPEN membership interval), checked against the SAME .log-derived
	// ParticipationView the fan-out projector uses (Decision 2b). Trusted-backend identities are
	// exempt (they reach this path only for non-participation facts, e.g. message.created).
	// Enforced only for an AUTHENTICATED identity: the shared-`client` dev posture leaves the
	// suffix advisory (Decision 1 precondition), so it does not gate participation. Interaction
	// lifecycle (started/ended) is not participant-gated — an interaction has no participants
	// before it is started (chicken-and-egg).
	if isAuthenticated(id) && RoleOf(id) == RoleAgent && !isLifecycleCommand(cmd.Type) {
		facts, _, ferr := r.store.Replay(logSubjectFor(tenant, iid))
		if ferr != nil {
			res.Status, res.Reason = statusRejected, "state unavailable (log replay failed) — retry"
			return res
		}
		if !FoldParticipation(facts).IsParticipantNow(suffix) {
			res.Status, res.Reason = statusRejected, "not_a_participant"
			return res
		}
	}

	st, err := r.getState(tenant, iid)
	if err != nil {
		res.Status, res.Reason = statusRejected, "state unavailable (log replay failed) — retry"
		return res
	}
	st.mu.Lock()
	defer st.mu.Unlock()

	// A concurrent goroutine poisoned this state (an append/reconcile failure left its seq
	// untrustworthy) — fail closed so the caller retries against a fresh rebuild.
	if st.poisoned {
		res.Status, res.Reason = statusRejected, "state unavailable — retry"
		return res
	}

	ph := hashPayload(cmd)

	// Append under per-subject OCC (store rejects unless st.streamSeq is still the subject's last).
	// A loser re-folds and retries ONCE: a concurrent writer may have advanced the subject, ended
	// the interaction, or already committed THIS command_id. See openspec change router-occ.
	var dup bool
	for attempt := 0; ; attempt++ {
		// A command_id is bound to its first payload (re-checked each attempt: a re-fold can reveal
		// a concurrent writer committed it).
		if prev, seen := st.results[cmd.CommandId]; seen {
			switch {
			case prev.payloadHash == "": // legacy fact, hash unknown
				return proto.Clone(prev.result).(*CommandResult)
			case prev.payloadHash != ph:
				res.Status, res.Reason = statusRejected, "conflict: command_id reused with a different payload"
				return res
			case prev.result.Status == statusAccepted:
				return proto.Clone(prev.result).(*CommandResult)
				// a previously-rejected same payload falls through: a transient rejection
				// (e.g. an illegal transition) may now be legal
			}
		}

		if !legalTransition(st.status, cmd.Type) {
			res.Status, res.Reason = statusRejected, fmt.Sprintf("illegal transition %q from state %q", cmd.Type, st.status)
			st.results[cmd.CommandId] = storedResult{payloadHash: ph, result: res}
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
		// dedupID is deterministic per (tenant,interaction,command) so a retry is exactly-once even
		// if a prior append's ack was lost.
		d, aerr := r.store.Append(logSubjectFor(tenant, iid), payload, tenant+"."+iid+"."+cmd.CommandId, st.streamSeq)
		if errors.Is(aerr, ErrOCCConflict) {
			// Lost the race: another writer advanced the subject. Re-fold once from the log and
			// retry; if we still lose, surface a retryable rejection (never append behind a stale seq).
			fresh, ferr := r.rebuild(tenant, iid)
			if ferr != nil || attempt >= 1 {
				st.poisoned = true
				r.mu.Lock()
				delete(r.inter, tenant+"/"+iid)
				r.mu.Unlock()
				res.Status, res.Reason = statusRejected, "lost concurrent append — retry"
				return res
			}
			st.seq, st.streamSeq, st.status = fresh.seq, fresh.streamSeq, fresh.status
			for k, v := range fresh.results {
				if _, ok := st.results[k]; !ok {
					st.results[k] = v
				}
			}
			continue
		}
		if aerr != nil {
			// The append may have committed before the ack was lost — reconcile from the log and
			// accept if the fact is now present.
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
		dup = d
		break
	}
	res.Status, res.CausedBy = statusAccepted, cmd.CommandId
	if dup {
		// The store already had this command_id (a retry, or a concurrent writer). Reconcile from
		// the log and compare the COMMITTED fact's payload hash: a divergent reuse is a conflict;
		// a matching one replays the committed result. Never clobber the committed entry.
		fresh, ferr := r.rebuild(tenant, iid)
		if ferr != nil {
			st.poisoned = true // seq may be stale; force a rebuild rather than append behind it
			r.mu.Lock()
			delete(r.inter, tenant+"/"+iid)
			r.mu.Unlock()
			return res
		}
		st.seq, st.streamSeq, st.status = fresh.seq, fresh.streamSeq, fresh.status
		for k, v := range fresh.results {
			if _, ok := st.results[k]; !ok {
				st.results[k] = v
			}
		}
		if committed, ok := st.results[cmd.CommandId]; ok {
			if committed.payloadHash != "" && committed.payloadHash != ph {
				return &CommandResult{CommandId: cmd.CommandId, Status: statusRejected, Reason: "conflict: command_id reused with a different payload"}
			}
			return committed.result
		}
		st.results[cmd.CommandId] = storedResult{payloadHash: ph, result: res}
		return res
	}
	st.seq++
	st.streamSeq++ // we committed exactly one fact under OCC: the subject advanced by one
	applyTransition(st, cmd.Type)
	st.results[cmd.CommandId] = storedResult{payloadHash: ph, result: res}

	if st.status == "ended" { // the durable log rebuilds state if a late command arrives
		r.mu.Lock()
		delete(r.inter, tenant+"/"+iid)
		r.mu.Unlock()
	}
	return res
}

// parseCmdSubject reads tenant.<tid>.interaction.<iid>.cmd.<identity>. The publisher identity is
// the LAST subject token — the ACL-pinned author (mirrors .signal.<userId>), NEVER the payload.
// Until the auth-callout mints a per-connection identity, this suffix is the dev identity source
// (see openspec change agent-feed-fanout, Decision 1: write-identity precondition). The bare
// 5-token .cmd subject is retired — the router subscribes tenant.*.interaction.*.cmd.* now.
//
// isAuthenticated reports whether the transport bound a real identity (auth-callout minted). The
// shared-`client` dev posture leaves both fields empty; in that posture the suffix is advisory and
// the participant/role gates are not enforced (Decision 1 write-identity precondition).
func isAuthenticated(id Identity) bool { return id.UserID != "" || id.Role != "" }

func isLifecycleCommand(t string) bool {
	return t == "interaction.started" || t == "interaction.ended"
}

func isParticipationCommand(t string) bool {
	switch t {
	case "participant.assign", "participant.unassign", "participant.transfer":
		return true
	}
	return false
}

// participationData is the audit-carrying payload of a privileged participation command. Desk
// sends it in Command.data; the router copies the audit fields onto each fact it writes.
type participationData struct {
	Agent     string `json:"agent"`      // assign/unassign target; transfer = the NEW agent
	From      string `json:"from"`       // transfer only: the agent being transferred away
	Reason    string `json:"reason"`     // audit
	RequestID string `json:"request_id"` // audit
}

// handleParticipation lands participant.* / interaction.assigned facts from a privileged command,
// role-gated to a trusted backend. participant.transfer writes participant.joined(new) BEFORE
// participant.left(old) so the interaction is never absent from both agents' membership at once
// (no-gap, Decision 2a / Decision 6). Audit fields (actor=suffix, reason, request_id) ride on each
// fact via the Command payload.
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

	for i, f := range facts {
		// Each fact is its own command_id so dedup/OCC stay per-fact; the transfer's two facts must
		// land in order (joined then left), so a partial failure on the second is surfaced.
		fcmd := &Command{
			CommandId: cmd.CommandId + ":" + f.eventType + ":" + f.agent,
			TenantId:  tenant,
			ActorId:   f.agent, // the fact's subject is the affected agent
			Type:      f.eventType,
			Medium:    cmd.Medium,
		}
		ev := r.appendParticipationFact(ctx, tenant, iid, actor, &pd, fcmd)
		if ev.Status != statusAccepted {
			if i > 0 {
				ev.Reason = "transfer partially applied (" + ev.Reason + ")"
			}
			ev.CommandId = cmd.CommandId
			return ev
		}
	}
	res.Status, res.CausedBy = statusAccepted, cmd.CommandId
	return res
}

// appendParticipationFact writes ONE participation fact (participant.joined/left,
// interaction.assigned) under the same per-subject OCC + dedup discipline as ordinary commands,
// carrying audit fields (commanded_by/reason/request_id). It re-folds and retries once on an OCC
// conflict, then fails closed — never appends behind a stale sequence.
func (r *Router) appendParticipationFact(ctx context.Context, tenant, iid, commandedBy string, pd *participationData, fcmd *Command) *CommandResult {
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
		if prev, seen := st.results[fcmd.CommandId]; seen && prev.result.Status == statusAccepted {
			return proto.Clone(prev.result).(*CommandResult)
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
			CausedBy: fcmd.CommandId, CommandedBy: commandedBy, Reason: pd.Reason, RequestId: pd.RequestID,
		}
		payload, _ := proto.Marshal(ev)
		_, aerr := r.store.Append(logSubjectFor(tenant, iid), payload, tenant+"."+iid+"."+fcmd.CommandId, st.streamSeq)
		if errors.Is(aerr, ErrOCCConflict) {
			fresh, ferr := r.rebuild(tenant, iid)
			if ferr != nil || attempt >= 1 {
				st.poisoned = true
				r.mu.Lock()
				delete(r.inter, tenant+"/"+iid)
				r.mu.Unlock()
				res.Status, res.Reason = statusRejected, "lost concurrent append — retry"
				return res
			}
			st.seq, st.streamSeq, st.status = fresh.seq, fresh.streamSeq, fresh.status
			for k, v := range fresh.results {
				if _, ok := st.results[k]; !ok {
					st.results[k] = v
				}
			}
			continue
		}
		if aerr != nil {
			st.poisoned = true
			r.mu.Lock()
			delete(r.inter, tenant+"/"+iid)
			r.mu.Unlock()
			res.Status, res.Reason = statusRejected, "log append failed — retry"
			return res
		}
		st.seq++
		st.streamSeq++
		applyTransition(st, fcmd.Type)
		res.Status, res.CausedBy = statusAccepted, fcmd.CommandId
		st.results[fcmd.CommandId] = storedResult{payloadHash: ph, result: res}
		return res
	}
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

package signaling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Router is the authoritative writer of every `interaction.<id>.log` fact (chat
// subset). It is pure logic over a LogStore port — it has NO knowledge of NATS.
// Clients publish intents on `.cmd`; the router validates, assigns a monotonic
// `sequence`, appends the fact, and returns a CommandResult for the issuer. State is
// in-memory per interaction and rebuilt lazily from the durable log (the source of
// truth) on first access / after a restart.
type Router struct {
	store LogStore
	now   func() time.Time
	id    func() string

	mu    sync.Mutex // guards the inter map only (brief)
	inter map[string]*interactionState
	load  singleflight.Group // one rebuild per key (no concurrent stale inserts)
}

type interactionState struct {
	mu      sync.Mutex // guards this one interaction (incl. its log append)
	seq     int64
	status  string                  // "" | started | ended
	results map[string]storedResult // command_id → result (idempotency)
}

type storedResult struct {
	payloadHash string // "" when rebuilt from the log (payload not recorded in facts)
	result      CommandResult
}

type Option func(*Router)

func WithClock(now func() time.Time) Option { return func(r *Router) { r.now = now } }
func WithIDGen(gen func() string) Option    { return func(r *Router) { r.id = gen } }

var idCounter struct {
	mu sync.Mutex
	n  uint64
}

func defaultID() string {
	idCounter.mu.Lock()
	idCounter.n++
	n := idCounter.n
	idCounter.mu.Unlock()
	return fmt.Sprintf("ev-%d", n)
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
		"participant.joined", "participant.left", "interaction.context.updated":
		return status == "started"
	default:
		return false
	}
}

// requiresRefID: an edit/delete must name the message it edits.
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

func hashPayload(c Command) string {
	c.CommandID = ""
	b, _ := json.Marshal(c)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func logSubjectFor(tenant, iid string) string {
	return fmt.Sprintf("tenant.%s.interaction.%s.log", tenant, iid)
}

// getState returns the (lazily rebuilt) state for an interaction. The rebuild runs
// under singleflight so concurrent callers share ONE state object (preventing a
// stale state being re-inserted after a concurrent eviction). A replay failure is
// propagated so the caller fails closed rather than acting on partial state.
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
	facts, err := r.store.Replay(logSubjectFor(tenant, iid))
	if err != nil {
		return nil, err
	}
	for _, e := range facts {
		if e.Sequence > st.seq {
			st.seq = e.Sequence
		}
		applyTransition(st, e.EventType)
		if e.CommandID != "" {
			st.results[e.CommandID] = storedResult{result: CommandResult{
				CommandID: e.CommandID, Status: "accepted", CausedBy: e.CommandID,
			}}
		}
	}
	return st, nil
}

// HandleCommand processes one `.cmd` request and returns the CommandResult to reply.
// The trusted tenant/actor come from the authenticated Identity on ctx — the subject
// and the payload are validated AGAINST it (never trusted on their own).
func (r *Router) HandleCommand(ctx context.Context, subject string, data []byte) CommandResult {
	id := IdentityFrom(ctx)
	tenant, iid, ok := parseCmdSubject(subject)
	if !ok {
		return CommandResult{Status: "rejected", Reason: "bad subject"}
	}
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return CommandResult{Status: "rejected", Reason: "bad payload"}
	}
	// the authenticated tenant is the trust anchor; the subject fallback applies only
	// when the transport could not authenticate one (Phase-1 dev, before auth-callout —
	// NOT production-safe for authorship; auth-callout MUST populate Identity in prod).
	authTenant := id.TenantID
	if authTenant == "" {
		authTenant = tenant
	}
	res := CommandResult{CommandID: cmd.CommandID}
	switch {
	case cmd.CommandID == "":
		res.Status, res.Reason = "rejected", "missing command_id"
		return res
	case cmd.ActorID == "":
		res.Status, res.Reason = "rejected", "missing actor_id"
		return res
	case tenant != authTenant:
		res.Status, res.Reason = "rejected", "subject tenant != authenticated tenant"
		return res
	case cmd.TenantID != authTenant:
		res.Status, res.Reason = "rejected", "payload tenant_id != authenticated tenant"
		return res
	case id.UserID != "" && cmd.ActorID != id.UserID:
		res.Status, res.Reason = "rejected", "actor_id != authenticated user"
		return res
	case requiresRefID(cmd.Type) && cmd.RefID == "":
		res.Status, res.Reason = "rejected", "missing ref_id"
		return res
	}

	st, err := r.getState(tenant, iid)
	if err != nil {
		res.Status, res.Reason = "rejected", "state unavailable (log replay failed) — retry"
		return res
	}
	st.mu.Lock()
	defer st.mu.Unlock()

	// idempotency / conflict: a command_id is bound to its first payload.
	if prev, seen := st.results[cmd.CommandID]; seen {
		switch {
		case prev.payloadHash == "": // rebuilt accepted fact — payload unknown → replay
			return prev.result
		case prev.payloadHash != hashPayload(cmd): // different payload → conflict
			res.Status, res.Reason = "rejected", "conflict: command_id reused with a different payload"
			return res
		case prev.result.Status == "accepted": // same payload, accepted → replay
			return prev.result
			// same payload, previously rejected → fall through to re-evaluate (a
			// transient rejection like an illegal transition may now be legal)
		}
	}

	if !legalTransition(st.status, cmd.Type) {
		res.Status, res.Reason = "rejected", fmt.Sprintf("illegal transition %q from state %q", cmd.Type, st.status)
		st.results[cmd.CommandID] = storedResult{payloadHash: hashPayload(cmd), result: res}
		return res
	}

	seq := st.seq + 1
	ev := Event{
		Schema: SchemaV1, EventType: cmd.Type, EventID: r.id(), Sequence: seq,
		OccurredAt: r.now().UTC(), TenantID: tenant, ActorID: cmd.ActorID,
		Medium: orDefault(cmd.Medium, "chat"), CommandID: cmd.CommandID,
		CausedBy: cmd.CommandID, RefID: cmd.RefID, Data: cmd.Data,
	}
	payload, _ := json.Marshal(ev)
	// Deterministic dedupID per (tenant,interaction,command): the store dedups a retry
	// even if a prior append's ack was lost — exactly-once append across crashes.
	dup, aerr := r.store.Append(logSubjectFor(tenant, iid), payload, tenant+"."+iid+"."+cmd.CommandID)
	if aerr != nil {
		res.Status, res.Reason = "rejected", "log append failed"
		return res
	}
	res.Status, res.CausedBy = "accepted", cmd.CommandID
	if dup {
		// a prior ack was lost / a concurrent writer wrote it — resync seq/status from
		// the durable log so this router's view is not behind.
		if fresh, ferr := r.rebuild(tenant, iid); ferr == nil {
			st.seq, st.status = fresh.seq, fresh.status
		}
	} else {
		st.seq = seq
		applyTransition(st, cmd.Type)
	}
	st.results[cmd.CommandID] = storedResult{payloadHash: hashPayload(cmd), result: res}

	if st.status == "ended" { // evict; the durable log rebuilds state if a late command arrives
		r.mu.Lock()
		delete(r.inter, tenant+"/"+iid)
		r.mu.Unlock()
	}
	return res
}

func parseCmdSubject(s string) (tenant, iid string, ok bool) {
	p := strings.Split(s, ".")
	if len(p) != 5 || p[0] != "tenant" || p[2] != "interaction" || p[4] != "cmd" {
		return "", "", false
	}
	return p[1], p[3], true
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

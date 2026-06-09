package signaling

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
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
	mu       sync.Mutex
	seq      int64
	status   string // "" | started | ended
	results  map[string]storedResult
	poisoned bool // seq untrustworthy → callers must rebuild, not reuse it
}

type storedResult struct {
	payloadHash string // "" only for legacy facts written before payload_hash existed
	result      CommandResult
}

type Option func(*Router)

func WithClock(now func() time.Time) Option { return func(r *Router) { r.now = now } }
func WithIDGen(gen func() string) Option    { return func(r *Router) { r.id = gen } }

// Random, not sequential: a process-local counter would reset on restart and collide with
// event_ids already in the durable log. Tests inject deterministic ids via WithIDGen.
func defaultID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("relaypoint: crypto/rand failed: " + err.Error())
	}
	return "ev_" + hex.EncodeToString(b[:])
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
			st.results[e.CommandID] = storedResult{payloadHash: e.PayloadHash, result: CommandResult{
				CommandID: e.CommandID, Status: "accepted", CausedBy: e.CommandID,
			}}
		}
	}
	return st, nil
}

// The trusted tenant/actor come from the authenticated Identity on ctx; the subject and payload
// are validated against it, never trusted on their own.
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
	// Subject-tenant fallback applies only when the transport authenticated no Identity (Phase-1,
	// pre-auth-callout — NOT production-safe for authorship; prod MUST populate Identity).
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

	// A concurrent goroutine poisoned this state (an append/reconcile failure left its seq
	// untrustworthy) — fail closed so the caller retries against a fresh rebuild.
	if st.poisoned {
		res.Status, res.Reason = "rejected", "state unavailable — retry"
		return res
	}

	// A command_id is bound to its first payload.
	if prev, seen := st.results[cmd.CommandID]; seen {
		switch {
		case prev.payloadHash == "": // legacy fact, hash unknown
			return prev.result
		case prev.payloadHash != hashPayload(cmd):
			res.Status, res.Reason = "rejected", "conflict: command_id reused with a different payload"
			return res
		case prev.result.Status == "accepted":
			return prev.result
			// a previously-rejected same payload falls through: a transient rejection
			// (e.g. an illegal transition) may now be legal
		}
	}

	if !legalTransition(st.status, cmd.Type) {
		res.Status, res.Reason = "rejected", fmt.Sprintf("illegal transition %q from state %q", cmd.Type, st.status)
		st.results[cmd.CommandID] = storedResult{payloadHash: hashPayload(cmd), result: res}
		return res
	}

	seq := st.seq + 1
	ph := hashPayload(cmd)
	ev := Event{
		Schema: SchemaV1, EventType: cmd.Type, EventID: r.id(), Sequence: seq,
		OccurredAt: r.now().UTC(), TenantID: tenant, ActorID: cmd.ActorID,
		Medium: orDefault(cmd.Medium, "chat"), CommandID: cmd.CommandID,
		PayloadHash: ph, CausedBy: cmd.CommandID, RefID: cmd.RefID, Data: cmd.Data,
	}
	payload, _ := json.Marshal(ev)
	// dedupID is deterministic per (tenant,interaction,command) so a retry is exactly-once even
	// if a prior append's ack was lost.
	dup, aerr := r.store.Append(logSubjectFor(tenant, iid), payload, tenant+"."+iid+"."+cmd.CommandID)
	if aerr != nil {
		// The append may have committed before the ack was lost — reconcile from the log and
		// accept if the fact is now present.
		if fresh, ferr := r.rebuild(tenant, iid); ferr == nil {
			if prev, committed := fresh.results[cmd.CommandID]; committed {
				st.seq, st.status = fresh.seq, fresh.status
				return prev.result
			}
		}
		st.poisoned = true // seq may be stale; force a rebuild rather than append behind it
		r.mu.Lock()
		delete(r.inter, tenant+"/"+iid)
		r.mu.Unlock()
		res.Status, res.Reason = "rejected", "log append failed — retry"
		return res
	}
	res.Status, res.CausedBy = "accepted", cmd.CommandID
	if dup {
		fresh, ferr := r.rebuild(tenant, iid)
		if ferr != nil {
			st.poisoned = true // seq may be stale; force a rebuild rather than append behind it
			r.mu.Lock()
			delete(r.inter, tenant+"/"+iid)
			r.mu.Unlock()
			return res
		}
		st.seq, st.status = fresh.seq, fresh.status
		for k, v := range fresh.results {
			if _, ok := st.results[k]; !ok { // don't clobber this router's payload-hashed entries
				st.results[k] = v
			}
		}
	} else {
		st.seq = seq
		applyTransition(st, cmd.Type)
	}
	st.results[cmd.CommandID] = storedResult{payloadHash: ph, result: res}

	if st.status == "ended" { // the durable log rebuilds state if a late command arrives
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

package signaling

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Router is the authoritative writer of every `interaction.<id>.log` fact (chat
// subset). Clients publish intents on `.cmd`; the router validates, assigns a
// monotonic `sequence`, appends the fact to JetStream, and replies a CommandResult
// to the issuer's inbox only. State is in-memory per interaction (single-node
// Phase 1); the durable `.log` is the source of truth on restart.
type Router struct {
	nc  *nats.Conn
	js  nats.JetStreamContext
	now func() time.Time
	id  func() string

	mu    sync.Mutex
	inter map[string]*interactionState // key: tenant/interaction
}

type interactionState struct {
	seq     int64
	status  string                  // "" | started | ended
	results map[string]storedResult // command_id → result (idempotency)
}

type storedResult struct {
	payloadHash string
	result      CommandResult
}

func NewRouter(nc *nats.Conn, js nats.JetStreamContext) *Router {
	return &Router{
		nc: nc, js: js,
		now:   time.Now,
		id:    func() string { return nats.NewInbox() }, // unique enough for event ids
		inter: map[string]*interactionState{},
	}
}

// legalTransition reports whether a command type is allowed from the current state.
// Chat subset: started → message/participant/context activity → ended.
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

func applyTransition(st *interactionState, cmdType string) {
	switch cmdType {
	case "interaction.started":
		st.status = "started"
	case "interaction.ended":
		st.status = "ended"
	}
}

func hashPayload(c Command) string {
	c.CommandID = "" // the key is excluded; only the rest of the payload binds it
	b, _ := json.Marshal(c)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// HandleCommand processes one `.cmd` request and returns the CommandResult to reply.
func (r *Router) HandleCommand(subject string, data []byte) CommandResult {
	tenant, iid, ok := parseCmdSubject(subject)
	if !ok {
		return CommandResult{Status: "rejected", Reason: "bad subject"}
	}
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return CommandResult{Status: "rejected", Reason: "bad payload"}
	}
	res := CommandResult{CommandID: cmd.CommandID}
	if cmd.CommandID == "" {
		res.Status, res.Reason = "rejected", "missing command_id"
		return res
	}
	if cmd.ActorID == "" {
		res.Status, res.Reason = "rejected", "missing actor_id"
		return res
	}
	// security: payload tenant_id MUST equal the subject tenant (even if ACL passed).
	if cmd.TenantID != tenant {
		res.Status, res.Reason = "rejected", "payload tenant_id != subject tenant"
		return res
	}

	key := tenant + "/" + iid
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.inter[key]
	if st == nil {
		st = &interactionState{results: map[string]storedResult{}}
		r.inter[key] = st
	}

	// idempotency: same command_id replays its result; different payload = conflict.
	if prev, seen := st.results[cmd.CommandID]; seen {
		if prev.payloadHash == hashPayload(cmd) {
			return prev.result // exactly-once: no second fact
		}
		res.Status, res.Reason = "rejected", "conflict: command_id reused with a different payload"
		return res
	}

	if !legalTransition(st.status, cmd.Type) {
		res.Status, res.Reason = "rejected", fmt.Sprintf("illegal transition %q from state %q", cmd.Type, st.status)
		// a rejection is NOT memoised — the client may legitimately retry later
		return res
	}

	// assign sequence + append the authoritative fact to the durable `.log`.
	seq := st.seq + 1
	ev := Event{
		Schema: SchemaV1, EventType: cmd.Type, EventID: r.id(), Sequence: seq,
		OccurredAt: r.now().UTC(), TenantID: tenant, ActorID: cmd.ActorID,
		Medium: orDefault(cmd.Medium, "chat"), CommandID: cmd.CommandID,
		CausedBy: cmd.CommandID, RefID: cmd.RefID, Data: cmd.Data,
	}
	payload, _ := json.Marshal(ev)
	logSubject := fmt.Sprintf("tenant.%s.interaction.%s.log", tenant, iid)
	// Nats-Msg-Id dedups at the stream layer too (belt and braces with our memo).
	if _, err := r.js.Publish(logSubject, payload, nats.MsgId(ev.EventID)); err != nil {
		res.Status, res.Reason = "rejected", "log append failed"
		return res
	}
	st.seq = seq
	applyTransition(st, cmd.Type)

	res.Status, res.CausedBy = "accepted", ev.EventID
	st.results[cmd.CommandID] = storedResult{payloadHash: hashPayload(cmd), result: res}
	return res
}

func parseCmdSubject(s string) (tenant, iid string, ok bool) {
	// tenant.<tenantId>.interaction.<id>.cmd
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

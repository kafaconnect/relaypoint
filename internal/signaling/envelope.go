package signaling

import "time"

// Event is the authoritative `.log` fact envelope (signaling-core design). Chat and
// call share one envelope; `medium` is a payload field, never a subject.
type Event struct {
	Schema       string         `json:"schema"`
	EventType    string         `json:"event_type"`
	EventID      string         `json:"event_id"`
	Sequence     int64          `json:"sequence"`
	OccurredAt   time.Time      `json:"occurred_at"`
	TenantID     string         `json:"tenant_id"`
	ActorID      string         `json:"actor_id"`
	Medium       string         `json:"medium"`                  // 'chat' | 'call' (chat subset: 'chat')
	MediaProfile string         `json:"media_profile,omitempty"` // call facts (e.g. 'webrtc-p2p')
	CommandID    string         `json:"command_id,omitempty"`
	CausedBy     string         `json:"caused_by,omitempty"` // the command_id that produced this fact
	RefID        string         `json:"ref_id,omitempty"`    // client-stable id (message edits/deletes)
	Data         map[string]any `json:"data,omitempty"`
}

// Command is a client intent published on `interaction.<id>.cmd` (request/reply).
type Command struct {
	CommandID string         `json:"command_id"`
	TenantID  string         `json:"tenant_id"`
	ActorID   string         `json:"actor_id"`
	Type      string         `json:"type"` // an event_type the client may request
	Medium    string         `json:"medium"`
	RefID     string         `json:"ref_id,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

// CommandResult is the ephemeral ack the router replies to the issuer's inbox only.
type CommandResult struct {
	CommandID string `json:"command_id"`
	Status    string `json:"status"`              // 'accepted' | 'rejected'
	CausedBy  string `json:"caused_by,omitempty"` // accepted: the command_id (the fact's caused_by)
	Reason    string `json:"reason,omitempty"`    // rejected: why
}

const SchemaV1 = "relaypoint.interaction.v1"

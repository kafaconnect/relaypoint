package signaling

import "time"

// Event is the `.log` fact envelope. `medium` is a payload field, never a subject (one
// envelope for chat and call).
type Event struct {
	Schema       string         `json:"schema"`
	EventType    string         `json:"event_type"`
	EventID      string         `json:"event_id"`
	Sequence     int64          `json:"sequence"`
	OccurredAt   time.Time      `json:"occurred_at"`
	TenantID     string         `json:"tenant_id"`
	ActorID      string         `json:"actor_id"`
	Medium       string         `json:"medium"`                  // 'chat' | 'call' (chat subset: 'chat')
	MediaProfile string         `json:"media_profile,omitempty"` // 'webrtc-p2p' (call facts only)
	CommandID    string         `json:"command_id,omitempty"`
	PayloadHash  string         `json:"payload_hash,omitempty"` // for cross-restart conflict detection
	CausedBy     string         `json:"caused_by,omitempty"`    // = the producing command_id
	RefID        string         `json:"ref_id,omitempty"`
	Data         map[string]any `json:"data,omitempty"`
}

type Command struct {
	CommandID string         `json:"command_id"`
	TenantID  string         `json:"tenant_id"`
	ActorID   string         `json:"actor_id"`
	Type      string         `json:"type"`
	Medium    string         `json:"medium"`
	RefID     string         `json:"ref_id,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

// CommandResult is replied only to the issuer's inbox (ephemeral, never persisted).
type CommandResult struct {
	CommandID string `json:"command_id"`
	Status    string `json:"status"` // 'accepted' | 'rejected'
	CausedBy  string `json:"caused_by,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

const SchemaV1 = "relaypoint.interaction.v1"

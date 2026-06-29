package signaling

import interactionpb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"

// Aliases to the generated protobuf wire contract (ADR-0002) so router/store read signaling.*; the types live in interactionpb (Desk imports the same ones).
type (
	Event         = interactionpb.Event
	Command       = interactionpb.Command
	CommandResult = interactionpb.CommandResult
	SignalEvent   = interactionpb.SignalEvent
	ChatMessage   = interactionpb.ChatMessage
	FeedControl   = interactionpb.FeedControl
)

const SchemaV1 = "relaypoint.interaction.v1"

const (
	statusAccepted = interactionpb.CommandResult_STATUS_ACCEPTED
	statusRejected = interactionpb.CommandResult_STATUS_REJECTED
)

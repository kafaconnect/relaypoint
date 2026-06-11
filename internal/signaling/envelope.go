package signaling

import interactionpb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"

// The wire envelope is the generated protobuf contract (ADR-0002); these aliases keep the
// router/store code reading against `signaling.Event`/`Command`/`CommandResult` while the
// types themselves live in the public `interactionpb` package (Desk imports the same ones).
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

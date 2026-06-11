# Delta for Wire Format

The RP↔client signaling wire is **protobuf** (ADR-0002), defined in
`proto/relaypoint/interaction/v1/interaction.proto`. The NATS message `.Data` on every plane —
`tenant.*.interaction.*.log` (JetStream), `tenant.*.interaction.*.cmd` + its `CommandResult`
reply, and `tenant.*.interaction.*.signal.*` (core NATS) — is the protobuf-marshaled bytes of
the matching message. This SUPERSEDES the JSON envelope: there is no JSON-compat shim.

## ADDED Requirements

### Requirement: Protobuf is the signaling wire format

All RelayPoint↔client signaling MUST be encoded as protobuf using the generated
`relaypoint.interaction.v1` messages. The router MUST `proto.Marshal` every `.log` `Event` and
every `CommandResult` reply, and MUST `proto.Unmarshal` every inbound `.cmd` `Command`; a payload
that fails to unmarshal MUST be rejected (`bad payload`) and a corrupt durable fact MUST fail the
replay closed. The SDK MUST encode/decode the same messages. The Go generated types MUST live in
a PUBLIC package (`gen/go/relaypoint/interaction/v1`, NOT under `internal/`) so other Go
consumers (Desk) import the identical contract.

The opaque `data` field MUST be decoded by the payload registry documented in the `.proto`: a
chat fact/command (`medium = chat`, `event_type`/`type` = `message.*`) carries a `ChatMessage` in
`data`; payloads without a registry message yet (context, SDP) stay raw `bytes`. The envelope MUST
NOT change as payload types are added.

`occurred_at` MUST be a `google.protobuf.Timestamp` and `CommandResult.status` MUST be the
`STATUS_ACCEPTED`/`STATUS_REJECTED` enum. The SDK's public surface MAY stay camelCase as a thin
projection over the generated message, but the wire bytes MUST be protobuf.

#### Scenario: Every wire message round-trips through protobuf
- **id:** `wire.protobuf.round-trip`
- **GIVEN** an `Event`, `Command`, `CommandResult`, `SignalEvent`, and `ChatMessage` with populated fields
- **WHEN** each is `proto.Marshal`'d and then `proto.Unmarshal`'d (Go) / encoded and decoded (SDK)
- **THEN** the decoded message equals the original (fields, the chat `ChatMessage` payload, the timestamp, and the status enum all preserved)
- **AND** the SDK's camelCase projection (`LogEvent`/`Command`/`CommandResult`) maps 1:1 onto the generated message and back

#### Scenario: Router speaks protobuf end-to-end and preserves authoritative semantics
- **id:** `wire.protobuf.router-end-to-end`
- **GIVEN** the protobuf router subscribed on `tenant.*.interaction.*.cmd` over live NATS
- **WHEN** a client sends protobuf `Command`s (start, then a chat `message.created` carrying a `ChatMessage` in `data`)
- **THEN** the router replies a protobuf `CommandResult` and appends protobuf `Event` facts to `tenant.<t>.interaction.<id>.log` with router-assigned monotonic `sequence`
- **AND** `command_id` dedup, divergent-payload conflict, and illegal-transition rejection still hold over the protobuf wire (only the codec changed)

### Requirement: Protobuf cutover purges the JSON log stream

The cutover MUST delete and recreate the `INTERACTION_LOGS` JetStream stream (subjects
`tenant.*.interaction.*.log`) before the protobuf router serves traffic, because a protobuf
router calling `proto.Unmarshal` on a JSON-era fact fails closed (`Replay()` returns a
corrupt-fact error and bricks that interaction). This is a destructive dev reset; no production
history exists to retain. The reset MUST be an explicit, opt-in step — never a silent wipe on a
normal restart.

#### Scenario: Cutover starts from a clean INTERACTION_LOGS stream
- **id:** `wire.protobuf.stream-reset`
- **GIVEN** an `INTERACTION_LOGS` stream that may hold JSON-era facts
- **WHEN** the cutover reset runs (`signaling.ResetLogStream`, gated `RP_RESET_LOG_STREAM=1` on first boot, or `scripts/reset-log-stream.sh`)
- **THEN** the stream is deleted and recreated with the same subjects/config, holding zero facts
- **AND** the protobuf router then replays an empty log without a corrupt-fact failure, and a normal restart WITHOUT the opt-in flag never wipes the stream

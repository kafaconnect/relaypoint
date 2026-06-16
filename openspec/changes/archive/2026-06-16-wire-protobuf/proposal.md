# Change: wire-protobuf

## From

The shipped RelayPoint↔client signaling wire is **JSON**: the router marshals every `.log`
fact, `CommandResult`, and the SDK's `.cmd`/`.signal` payloads with `encoding/json` /
`JSON.stringify`, against hand-written Go structs and a manual camelCase↔snake_case projection
in the TypeScript SDK. The desk↔connector contract on the rest of the platform is already
protobuf over NATS (desk ADR-0004), so RP↔client being JSON is the one inconsistency, and the
hand-mapped field names are a drift risk a generated contract removes.

## To

**All RP↔client signaling is protobuf**, defined once in
`proto/relaypoint/interaction/v1/interaction.proto` and generated to:

- a **public** Go package `gen/go/relaypoint/interaction/v1` (`interactionpb`) — importable by
  Desk, NOT under `internal/`;
- the SDK's `clients/typescript/src/gen/` via `@bufbuild/protobuf` (`protoc-gen-es`).

The router and store swap `json.*` for `proto.*` of `Event`/`Command`/`CommandResult`; the SDK
codec swaps `JSON.*` for protobuf encode/decode while keeping the public camelCase API as a thin
projection. The chat payload (`message.created`/`message.updated`) is a `ChatMessage` proto
marshaled into the opaque `data` bytes (the payload registry); `occurred_at` is a
`google.protobuf.Timestamp`; `CommandResult.status` is the `ACCEPTED`/`REJECTED` enum.

## Reason

One binary contract across the whole platform (ADR-0002): simpler to reason about and tool,
compact + fast on the high-frequency `.signal`/voice plane, and a strict codegen'd schema that
cannot drift the way two hand-mapped JSON sides can. This is a **breaking** wire change, accepted
now because RelayPoint has **no production consumer** (desk has not integrated; the SDK is the
only client) — so JSON is replaced outright, no dual-encode.

## Impact

- `internal/signaling/{envelope.go,router.go,store.go}`, `cmd/router/main.go`,
  `clients/typescript/src/{codec.ts,types.ts}` swap codecs; every Go + TS test moves to protobuf
  fixtures.
- **Subjects/streams:** unchanged subjects (`tenant.*.interaction.*.{log,cmd,signal.*}`); the
  `INTERACTION_LOGS` JetStream stream MUST be **deleted + recreated** at cutover (a protobuf
  router fails `proto.Unmarshal` on a JSON-era fact and `Replay()` fails closed). Handled by
  `signaling.ResetLogStream` (gated `RP_RESET_LOG_STREAM=1` on first boot) +
  `scripts/reset-log-stream.sh`.
- Router LOG-AUTHORITATIVE semantics (router-assigned sequence, `command_id` dedup via payload
  hash, conflict detection, illegal-transition rejection, sole-writer, replay/fold) are
  **unchanged** — only the codec changed.
- `go.mod` gains `google.golang.org/protobuf`; the SDK gains `@bufbuild/protobuf` (100% OSS).

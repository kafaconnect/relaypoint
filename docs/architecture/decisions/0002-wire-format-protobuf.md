# ADR-0002: Wire format is protobuf for ALL RP↔client signaling (supersedes JSON)

- Status: Accepted
- Date: 2026-06-11
- Scope: the RelayPoint signaling wire — `.log` facts, `.cmd` commands + `CommandResult`, and the
  ephemeral `.signal` events. Relates to: ADR-0001 (SDK transport port — unaffected), signaling-core.

## Context

The shipped signaling wire is **JSON** (`internal/signaling/router.go` `json.Marshal`,
`clients/typescript/src/codec.ts` `JSON.stringify`). The owner's standing decision is that **the
whole platform speaks protobuf on the wire** — the desk↔connector contract is already protobuf over
NATS (desk ADR-0004), and RP↔client must match: **one binary contract, everywhere.** JSON on the
signaling plane is an MVP shortcut to correct, not the target.

Why protobuf here (not JSON):
- **One contract across the platform.** Connectors (ADR-0004) are protobuf; RP↔client being JSON is
  an inconsistency. A single binary contract is simpler to reason about and tool.
- **Efficiency on the hot plane.** `.signal` carries high-frequency `webrtc.ice`/typing and (later)
  voice; JSON is wasteful in bytes and parse cost. Protobuf is compact + fast.
- **A strict, codegen'd, versioned schema** (`.proto`) is harder to drift than hand-mapped JSON
  field names (the client-sdk already maintains a manual camelCase↔snake_case projection — a
  drift risk a generated contract removes).

## Decision

**All RelayPoint↔client signaling is protobuf**, defined in
`proto/relaypoint/interaction/v1/interaction.proto`:
- `Event` (the `.log` fact), `Command` + `CommandResult` (the `.cmd` plane), `SignalEvent` (the
  ephemeral `.signal` plane). The NATS message `.Data` is the protobuf-marshaled bytes.
- **`data` is `bytes`** — opaque per `event_type`/`medium`, matching "the router never parses the
  payload / the media descriptor is an opaque blob" (signaling-core). The first typed payload,
  `ChatMessage`, is protobuf-encoded into `data` for `message.created`; more typed payloads
  (call/webrtc) are added incrementally **without** an envelope schema change.
- `occurred_at` is `google.protobuf.Timestamp`; `CommandResult.status` is an enum
  (`ACCEPTED`/`REJECTED`); `schema` stays `"relaypoint.interaction.v1"`.
- **`data` is decoded by a payload registry** (`event_type`/`medium` → message), documented in the
  `.proto`: chat uses `ChatMessage`; typing/ICE/WebRTC payloads are added with their slices;
  intentionally-opaque blobs (SDP) stay raw `bytes`. The envelope never changes as payloads grow.
- Codegen: **Go `protoc-gen-go` into a PUBLIC package** `gen/go/relaypoint/interaction/v1`
  (NOT under `internal/` — **Desk is a Go consumer and must import the generated types**); **TS via
  `@bufbuild/protobuf`** (modern, tree-shakeable, no separate runtime codegen step). Both pinned via
  `buf.gen.yaml` + `buf.yaml` (added in the rework), with a `buf lint`/`buf breaking` CI gate.
- proto3 string fields are plain (no `optional`) — empty strings are omitted on the wire already and
  `*string` pointers would only worsen Go ergonomics.

## Migration (breaking, and that is acceptable now)

This is a **breaking wire change** (JSON → protobuf). It is acceptable to break with no
JSON-compat shim because: RelayPoint has **no production consumer yet** — desk's submodule was just
bumped and desk has **not** integrated (it still runs its transitional path), and the SDK is the
only client. So we **replace** JSON outright (no dual-encode): the router marshals/unmarshals
protobuf; the SDK codec encodes/decodes protobuf; every unit + integration test moves to protobuf
fixtures. The desk integration (`rp1-realtime-via-relaypoint`) adapts its adapter to protobuf from
the start.

**Existing JSON facts in JetStream MUST be purged (the one operational must-do).** Any dev
`INTERACTION_LOGS` stream already holds JSON facts; a protobuf router calling `proto.Unmarshal` on
them fails, and `Replay()` fails closed ("corrupt fact"), bricking those interactions. The cutover
step therefore **deletes + recreates `INTERACTION_LOGS`** (a clean dev reset — acceptable pre-prod)
before the protobuf router runs. (If any environment ever needs to retain history across the switch,
version the stream/subject instead — out of scope now, no prod data exists.)

## Consequences

- `internal/signaling/{envelope.go → generated pb, router.go, store.go}` and `cmd/router/main.go`
  swap `json.*` for `proto.*`; `clients/typescript/src/codec.ts` swaps `JSON.*` for protobuf.
- `data` being `bytes` keeps the envelope stable as payload types grow; consumers decode `data` by
  `event_type`/`medium` (chat first via `ChatMessage`).
- The SDK's manual camelCase↔snake_case projection is **replaced** by generated types (the projection
  to a camelCase public API can stay as a thin mapping over the generated message).
- `go.mod` gains `google.golang.org/protobuf`; the SDK gains a protobuf runtime dep (100% OSS).
- A `buf` lint/breaking-change gate guards the contract in CI (future hardening).

## Alternatives considered

- **`google.protobuf.Struct` for `data`** (JSON-in-protobuf): faithful to the current `map[string]any`
  but defeats the efficiency goal (it is essentially JSON) — rejected in favour of `bytes` + typed
  payloads.
- **Keep JSON, add protobuf only on `.signal`**: leaves the inconsistency the owner rejected ("ALL
  protobuf") and keeps two codecs — rejected.

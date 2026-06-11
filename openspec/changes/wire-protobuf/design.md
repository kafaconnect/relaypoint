# Design: wire-protobuf

Implements **ADR-0002** (`docs/architecture/decisions/0002-wire-format-protobuf.md`) over the
locked contract `proto/relaypoint/interaction/v1/interaction.proto`. The ADR is the rationale of
record; this design covers only the mechanics. The contract itself is NOT re-litigated here.

## Codegen

`buf.yaml` (module `proto/`, STANDARD lint) + `buf.gen.yaml` generate:
- **Go** via `protoc-gen-go` into `gen/go/relaypoint/interaction/v1` (package `interactionpb`),
  PUBLIC so Desk can import the same types (ADR-0002, loose coupling — Desk depends on the
  generated contract, not on RelayPoint internals).
- **TypeScript** via `@bufbuild/protoc-gen-es` into `clients/typescript/src/gen/`.

Generated code is **committed** so neither build requires `buf` at compile time. A `buf
lint`/`buf breaking` gate guards the contract (future CI hardening).

## Go

`envelope.go` is reduced to type aliases over `interactionpb` (`Event`/`Command`/
`CommandResult`/`SignalEvent`/`ChatMessage`) plus the `SchemaV1` const and the status-enum
shorthands. The router/store read against `signaling.Event` etc. unchanged in spirit; the wire
codec is now `proto.Marshal`/`proto.Unmarshal`.

The LOG-AUTHORITATIVE logic is preserved verbatim — sequence assignment, the `command_id` →
payload-hash dedup map, conflict detection, illegal-transition rejection, the dup-append
reconcile, the poison/evict paths, and `Replay`-fold rebuild. Only two semantic touch-points
change shape:
- `hashPayload` clears `command_id` then marshals the Command **deterministically**
  (`proto.MarshalOptions{Deterministic:true}`) before SHA-256, so a retry hashes stably.
- The chat payload moves from a JSON `map[string]any` into a `ChatMessage` marshaled into the
  `data` bytes (the payload registry).

`LogStore.Replay` returns `[]*Event` (proto messages are used by pointer); `HandleCommand`
returns `*CommandResult`.

## TypeScript

`codec.ts` is the only module touching the generated messages. The public surface
(`LogEvent`/`Command`/`CommandResult` in `types.ts`) stays camelCase — a thin projection over
the generated message. `data` handling follows the registry: a chat `message.*` `data` is a
`ChatPayload {text, attachmentRefs}` ↔ `ChatMessage`; every other payload (context, SDP, future
typing/ICE) stays an opaque `Uint8Array` until its own registry message lands. `sequence`
(`int64`/`bigint`) projects to `number`; `occurred_at` projects to an ISO string.

## Migration — stream purge (the one operational must-do)

Per ADR-0002, any `INTERACTION_LOGS` stream holding JSON facts MUST be deleted + recreated
before the protobuf router runs (else `proto.Unmarshal` fails and `Replay()` fails closed,
bricking those interactions). `signaling.ResetLogStream(js)` deletes then re-adds the stream;
`cmd/router/main.go` runs it on first boot when `RP_RESET_LOG_STREAM=1`; `scripts/reset-log-stream.sh`
is the standalone step. The Go + TS integration tests reset the stream so each run starts clean.
No production history exists to retain (ADR migration note).

## Out of scope

The typed `.signal` payloads (TypingSignal/IceCandidate/SessionDescription) and call/webrtc
facts — added incrementally with their own slices, without an envelope schema change (the
registry grows; `data` stays `bytes`).

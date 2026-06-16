---
id: V1-01
slice: V1
title: Go router/store/main on protobuf (json.* -> proto.*)
status: done
specs: [wire.protobuf.round-trip, wire.protobuf.router-end-to-end]
---

Alias the generated types in `envelope.go`; swap `json.*` for `proto.*` in router/store/main.
Chat payload -> `ChatMessage` in `data`; `occurred_at` -> timestamppb; `CommandResult.status` ->
enum; `hashPayload` clears command_id then deterministic-marshals. LOG-AUTHORITATIVE semantics
(sequence, dedup, conflict, illegal-transition, sole-writer, replay/fold) UNCHANGED. Every Go test
rebuilt on protobuf fixtures + a per-message round-trip test.

## Log
- 2026-06-11 done: router/store/main + tests on protobuf; `go test ./...` + `-tags integration` green (router-end-to-end live)

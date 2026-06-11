---
id: V4-01
slice: V4
title: OpenSpec change — proposal/design + wire-format spec delta
status: done
specs: [wire.protobuf.round-trip, wire.protobuf.router-end-to-end, wire.protobuf.stream-reset]
---

Author the `wire-protobuf` change: proposal + design referencing ADR-0002 and the `.proto`; a
`wire-format` spec delta with `### Requirement:` + `#### Scenario:` (stable ids), each backed by a
`// @spec:` test. `openspec validate wire-protobuf --strict` passes.

## Log
- 2026-06-11 done: proposal/design/spec delta + per-task files; openspec validate --strict passes

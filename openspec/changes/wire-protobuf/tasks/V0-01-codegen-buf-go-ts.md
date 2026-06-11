---
id: V0-01
slice: V0
title: buf codegen — public Go interactionpb + TS @bufbuild/protobuf
status: done
specs: [wire.protobuf.round-trip]
---

Add `buf.yaml` + `buf.gen.yaml`. Generate the locked `.proto` to a PUBLIC Go package
`gen/go/relaypoint/interaction/v1` (`interactionpb`, importable by Desk) via `protoc-gen-go`, and
to `clients/typescript/src/gen/` via `@bufbuild/protoc-gen-es`. Commit the generated code so the
build needs no `buf`. Add `google.golang.org/protobuf` (go.mod) + `@bufbuild/protobuf` (+ dev
`protoc-gen-es`, `buf`) to the SDK. `buf lint` clean.

## Log
- 2026-06-11 done: buf.yaml/buf.gen.yaml + committed gen/go + src/gen; `buf lint` clean; deps added (commit codegen)

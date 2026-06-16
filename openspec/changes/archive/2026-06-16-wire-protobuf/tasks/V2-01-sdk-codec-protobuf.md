---
id: V2-01
slice: V2
title: TS SDK codec on protobuf, camelCase public API preserved
status: done
specs: [wire.protobuf.round-trip]
---

`codec.ts` encodes/decodes the generated messages; the public `LogEvent`/`Command`/`CommandResult`
stay camelCase as a thin projection. chat `data` <-> `ChatMessage`; non-chat payloads stay opaque
`Uint8Array`. Tests + helpers build protobuf fixtures; round-trip tests per message. typecheck +
test + build green.

## Log
- 2026-06-11 done: codec/types/helpers + tests on protobuf; pnpm typecheck/test/build green

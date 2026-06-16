---
id: V3-01
slice: V3
title: INTERACTION_LOGS stream-purge cutover (delete + recreate)
status: done
specs: [wire.protobuf.stream-reset]
---

ADR-0002: a protobuf router fails closed on JSON-era facts. `signaling.ResetLogStream` deletes +
recreates the stream; `cmd/router/main.go` runs it on first boot under `RP_RESET_LOG_STREAM=1`
(never a silent wipe); `scripts/reset-log-stream.sh` is the standalone step; both integration
suites start from a clean stream. Integration test asserts the purge.

## Log
- 2026-06-11 done: ResetLogStream + main gate + script; TestResetLogStreamPurgesFacts green on live NATS

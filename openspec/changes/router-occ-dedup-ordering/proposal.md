# Change: router-occ-dedup-ordering

## From

The merged `router-occ` change added per-subject optimistic concurrency to the log append and
documented (in `internal/signaling/store.go` and its design) that JetStream evaluates `Nats-Msg-Id`
**dedup BEFORE** the `Nats-Expected-Last-Subject-Sequence` (OCC) check — so a genuine retry of an
already-committed `command_id` was assumed to always come back as `duplicate=true`, never as
`ErrOCCConflict`. That ordering is **not guaranteed by NATS**: on a single-server (R1) JetStream
the expected-subject check runs **before** dedup, so a retry of an already-committed command can
surface as `ErrOCCConflict` instead of `duplicate=true` (a clustered broker may order them
differently again). A cross-review (codex) flagged the `LogStore` contract comment as a misleading
hard guarantee.

The router's runtime **behaviour is already correct**: on `ErrOCCConflict` it re-folds from the
durable log and re-checks `command_id` dedup against the fresh fold, so a retry of an
already-committed command replays the cached accepted result (no spurious rejection, no second
fact). But this OCC-before-dedup path was **undocumented and untested** — a future refactor could
silently break it.

## To

`internal/signaling/store.go` documents the dedup-vs-OCC ordering as **broker-dependent**, not a
hard guarantee: the comment states that on R1 the expected-subject check precedes dedup, so callers
MUST treat `ErrOCCConflict` as "rebuild + re-check `command_id` dedup" — which the router already
does. A unit test pins this path: a fake `LogStore` whose initial fold is one sequence behind the
true tail returns `ErrOCCConflict` on the first `Append` for an already-committed `command_id`
(simulating R1 OCC-before-dedup); the router must re-fold, recognise the committed command, and
return the original cached **accepted** `CommandResult` (same `caused_by`) — appending **no** second
fact.

## Reason

The contract comment is load-bearing for any future `LogStore` adapter or router refactor; stating
an unguaranteed broker ordering as fact invites a regression that turns a legitimate retry into a
spurious rejection. The fix is documentation + a regression test only — the behaviour is unchanged.

## Impact

- `internal/signaling/store.go` (`LogStore.Append` contract comment — ordering is broker-dependent).
- `internal/signaling/router_unit_test.go` (one new unit test + a fake that exercises the
  OCC-before-dedup path; no NATS — loose coupling preserved).
- **Subjects/streams/protobuf:** unchanged. No contract change; the desk repo is untouched.

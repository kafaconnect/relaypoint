# Design: router-occ-dedup-ordering

Documentation + regression-test only. No runtime behaviour changes; the router already handles the
OCC-before-dedup case correctly. This change makes the contract honest and pins the path with a test.

## The broker-ordering assumption that was wrong

The `router-occ` change assumed JetStream evaluates `Nats-Msg-Id` dedup **before**
`Nats-Expected-Last-Subject-Sequence`, so a retry of an already-committed `command_id` would always
return `duplicate=true`. NATS does not guarantee this. On a **single-server (R1)** JetStream the
expected-subject (OCC) check runs **first**: a stale-fold retry of an already-committed command
fails the expected-sequence test and is returned as `ErrOCCConflict` before dedup is ever consulted.
A clustered deployment may order the two checks differently. The ordering is therefore an adapter
implementation detail callers MUST NOT depend on.

## What the contract now says

`LogStore.Append`'s comment no longer claims dedup precedes OCC as a guarantee. It documents the
ordering as broker-dependent (R1 = expected-subject first) and states the caller obligation:
`ErrOCCConflict` MUST be treated as "rebuild + re-check `command_id` dedup", never as proof the
command was not committed. This is exactly the router's existing `ErrOCCConflict` handling — the
re-fold reveals the committed `command_id` and the dedup check on the fresh fold replays the cached
accepted result.

## Why the behaviour is already correct (no code change)

In `HandleCommand`'s bounded append loop, the `command_id` dedup check sits at the **top of each
attempt**, re-evaluated against the current fold. On `ErrOCCConflict` the router `rebuild`s from the
durable log (the true tail), merges the fresh `results`, and loops once more. If the conflict was a
retry of an already-committed command, the fresh fold now contains that `command_id`, so the next
attempt's dedup check returns the cached **accepted** result and the loop appends nothing further.
A genuine race (a *different* command advancing the subject) instead lands the next sequence on the
retry. Both outcomes are pre-existing and unchanged.

## Test — OCC-before-dedup replays accepted

A new unit test uses an in-memory fake `LogStore` (no NATS) that:

- returns the interaction's facts **without** the already-committed `command_id` on the **first**
  `Replay` (the stale fold the caller retries with is one sequence behind the true tail), and **with**
  it on subsequent replays (the re-fold reveals it);
- returns `ErrOCCConflict` on the **first** `Append`, modelling R1 OCC-before-dedup.

It asserts the router returns the original cached **accepted** `CommandResult` (status accepted,
`caused_by` == the command_id), and that `Append` is called **exactly once** — proving the retry is
satisfied by the re-fold's dedup hit, not by a spurious rejection or a second fact. This is the path
the misleading comment glossed over.

## Loose coupling

The test uses the in-memory fake only; the router core stays NATS-free and unit-testable. The
broker-ordering nuance is confined to the `LogStore` port comment and the JetStream adapter — the
core depends only on the `ErrOCCConflict` sentinel and the documented caller obligation.

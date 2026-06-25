# ADR-0006: Signaling gates on delivery structure, not domain vocabulary

- Status: Accepted
- Date: 2026-06-25
- Relates to: ADR-0003 (agent-fanout-feed — the projector delivery this annotation log feeds),
  ADR-0005 (presence-and-call-signaling — presence and ephemeral signal are SEPARATE primitives,
  not interaction-gated). Supersedes the never-implemented interaction MUSTs in the archived
  `signaling-core` spec (see Consequences).

## Context

RelayPoint is a UNIFIED signaling plane: it owns DELIVERY (get a payload to a destination feed,
ordered, deduped, exactly-once), not the producer's domain. But the signaling router's
`legalTransition` (internal/signaling/router.go) was a CLOSED switch that enumerated the producer's
event vocabulary — `message.*`, `call.*`, `participant.*`, `interaction.assigned`,
`interaction.context.updated` — and rejected everything else via `default: return false`. Plus
`requiresRefID` encoded the chat rule "an edit names its target message".

When `desk-router` (the M2 routing engine) began emitting `routing.offered` / `routing.assigned` /
`routing.no_candidates`, they hit `default` and were rejected with `illegal transition
"routing.offered" from state "started"` — realtime offers failed in production. The closed enum is a
Go-only allow-list that exists in no contract (`Command.type` is a free-form string, `Data` is
opaque bytes), and it levies a per-verb re-pin tax: `call.*` had to be added to this same enum three
commits before `routing.*` would have needed the next addition.

## Decision

RelayPoint gates on STRUCTURE, never on a census of domain verbs.

1. `legalTransition` collapses to three structural arms: `interaction.started` legal iff
   `status==""`; `interaction.ended` legal iff `status=="started"`; **default** (any other
   command — an opaque annotation) legal iff `status=="started"`.
2. `requiresRefID` and its rejection are removed. `ref_id` stays as opaque envelope plumbing (still
   hashed into the dedup payloadHash); enforcing referential integrity for message edit/delete is
   relocated to the producing domain (Desk).
3. RP keeps only the structural gates it legitimately owns: the lifecycle open/closed bit
   (`applyTransition`, post-end eviction), per-subject OCC ordering + dense sequence, the
   `command_id`+payloadHash dedup / exactly-once binding, tenancy + actor-suffix security, the
   participation MEMBERSHIP carve-out (the fan-out addressing model — `isParticipationFact` rejection
   stays ordered AHEAD of the generic arm so a forged join/leave is still refused), and the
   `participant.left` revocation tombstone in the projector.
4. `Command.type` and `Data` are opaque to RP; the meaning (`online`? `offered`? `ringing`?) lives in
   Desk/Router. Presence (`tenant.<t>.presence.<user>.>`) and ephemeral signal (`.signal.<user>`)
   are separate, non-interaction primitives and are unaffected (ADR-0005).

## Consequences

- Every future verb (`routing.*`, `escalation.*`, `sla.*`, …) flows as an opaque annotation on a
  started interaction with ZERO RelayPoint change — the per-verb re-pin tax is eliminated.
- Forward-compatible: every previously-enumerated verb keeps the identical `status=="started"`
  predicate, so an old Desk against a new rp-router is unaffected and a new rp-router against the
  current Desk simply flips `routing.*` from rejected to accepted. No proto/contract change.
- The safety net moves from rejection to observability: accepted commands are already logged
  (`router.command status=STATUS_ACCEPTED` with `type`), so an unexpected type stays visible.
- The lifecycle gate (annotations legal only on a started, not-yet-ended interaction) applies ONLY to
  the durable interaction log — NOT to presence/signal. Producers must `interaction.started` before
  annotating; a post-`ended` retry is rejected and dead-lettered (unchanged from the prior behaviour
  for `message.created`).
- SUPERSEDED: MUSTs the archived `signaling-core` spec ascribed to RP but which `router.go` never
  implemented — single-transfer rejection, the 1:1 WebRTC call state machine + glare, and
  recording-consent idempotency. That legality, where needed, belongs to the producer, not RP. This
  ADR formalises an existing gap rather than removing a working guard.

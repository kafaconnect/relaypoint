# Delta for Signaling Core

All subjects are prefixed `tenant.<tenantId>.` (omitted below for brevity). Subjects are
dot-separated, lowercase; ids are ULID/UUID (no dots/slashes). Event envelope:
`{ schema, event_type, event_id, sequence, occurred_at, tenant_id, actor_id, medium, media_profile?, command_id?, caused_by?, ref_id?, data }`
(`command_id` rides on COMMANDS; the resulting `.log` fact carries `caused_by = command_id`).
Event-specific fields (e.g. `negotiation_id`, `object_ref`, `failure_reason`) ride INSIDE `data`,
never as top-level envelope fields.
Wire fields are **snake_case** — this envelope is the authoritative wire contract; SDKs MAY
project these fields into their own idiom (e.g. a camelCase TS surface) but the wire form is
normative.
The **router/interaction service** is the authoritative writer of every
`interaction.<id>.log` fact; clients are READ-only on `.log` and publish COMMANDS on
`interaction.<id>.cmd`. Write access to `.log` is a **trusted-server** capability, never a
client one (see the authority model below).

## ADDED Requirements

### Requirement: Router-authoritative command plane
Clients MUST NOT write `interaction.<id>.log` and MUST NOT assign `sequence`. A stateful
**router/interaction service** MUST be the single authoritative writer of every `.log`
fact. Clients MUST publish intents as COMMANDS on `interaction.<id>.cmd` (write-only there,
read-only on `.log`). For each command the router MUST validate tenant scope, actor identity
and role, state-machine legality, and `author == the connection's authenticated identity`,
then assign a monotonic per-interaction `sequence` and append the authoritative `.log` fact.
Illegal transitions and forged authorship MUST be rejected. Write access to `.log` MUST be a
**trusted-server** capability, never a client one: Phase-1's sole writer is the router, but the
authority model MUST permit additional trusted server-side authorities (e.g. a future
media/recording egress) to be granted write of the media-plane facts they alone observe, rather
than forcing those through the router — clients remain non-writers regardless.

Each COMMAND MUST carry a client-generated `command_id` (idempotency key). The router MUST
dedup on `command_id`: a retried identical command MUST NOT append a second `.log` fact. The
authoritative `.log` fact the command produces MUST carry `caused_by = command_id`, so the
issuer correlates the exactly-once effect of its command.

Each command MUST be issued as a NATS **request/reply** on `interaction.<id>.cmd` carrying a
reply `_INBOX` (the same pattern as the offer ring). The router MUST reply with a
`CommandResult { command_id, status: "accepted" | "rejected", caused_by?, reason? }`. On
`accepted` the result MUST carry `caused_by` = the correlation of the produced `.log` fact (i.e.
`caused_by = command_id`, the key the authoritative fact is stamped with), so the issuer can
correlate the effect; on `rejected` the result MUST carry a `reason`. The `CommandResult` is an
EPHEMERAL ack on core NATS — it MUST NEVER be persisted to JetStream — and the authoritative
effect remains the `.log` fact (the result is a correlation/ack, NOT the source of truth). The
reply MUST go only to the issuer's own reply `_INBOX`; it MUST NOT leak to any other user.

A retried command reusing a `command_id` with an IDENTICAL payload MUST replay the original
`CommandResult` (idempotent — no second `.log` fact, the same `accepted`/`caused_by`). Reusing
the SAME `command_id` with a DIFFERENT payload MUST be rejected as `conflict` (`status =
"rejected"`, `reason = conflict`): the key is bound to its original request, so a divergent
payload under a reused key is a client bug, never a silent second effect.

**Phase-1 security posture (what is enforced NOW vs deferred).** The NATS account ACL is the
enforced backstop today: clients are DENIED publish on `.log`, so a client cannot write a fact
directly — this holds in Phase-1. The stronger guarantees in this requirement — `author == the
connection's authenticated identity`, per-tenant/per-user scoping, and own-author-only
`.signal.<userId>` — REQUIRE a per-connection identity that Phase-1 does NOT yet mint: it runs a
shared `client` NATS user, so the router receives an EMPTY identity and falls back to validating
against the subject tenant (documented dev-only in `cmd/router/main.go`). Until the deferred
**auth-callout (NKEY/JWT)** mints a per-connection `Identity{tenant,user}`, a client sharing that
user CAN still forge another `actor_id` via `.cmd` and publish another user's `.signal.*`. The
router's forged-author rejection (`signaling.cmd.forged-author-rejected`) is therefore active
ONLY once a non-empty identity is present. This posture MUST NOT be relied on as production
authorship security; closing it is a blocking precondition for production, tracked with the
auth-callout change.

As the single authority, the router MUST serialize **interaction-level** commands against the
current state via a state-guard / compare-and-set, so concurrent commands from different actors
do not race. A second `interaction.transfer.requested` while the interaction is already
`transferring` MUST be rejected (one transfer in flight at a time). A `recording.started` while
recording is already in effect MUST be idempotent or rejected (never a second concurrent
recording). These are distinct from `command_id` retry-dedup: they guard DIFFERENT commands that
target the same interaction state.

#### Scenario: Client cannot write the log directly
- **id:** `signaling.cmd.log-write-only-router`
- **GIVEN** a connected client authorized for `interaction.<id>`
- **WHEN** it attempts to publish `message.created` or `interaction.ended` directly on `tenant.<tid>.interaction.<id>.log`
- **THEN** NATS denies the publish (clients hold no write ACL on `.log`)
- **AND** the client's only path to record a fact is a command on `tenant.<tid>.interaction.<id>.cmd`

#### Scenario: Router validates a command and assigns sequence
- **id:** `signaling.cmd.router-assigns-sequence`
- **GIVEN** a client publishes a valid `send_message` command on `interaction.<id>.cmd`
- **WHEN** the router validates tenant, actor role, state, and author
- **THEN** the router appends one `message.created` fact on `interaction.<id>.log` with a router-assigned monotonic `sequence`
- **AND** the client never set `sequence`

#### Scenario: Forged authorship is rejected
- **id:** `signaling.cmd.forged-author-rejected`
- **GIVEN** actor `alice` is connected with authenticated identity `alice`
- **WHEN** she publishes a command whose payload `actor_id` is `bob`
- **THEN** the router rejects the command (author != connection identity) and writes no `.log` fact

#### Scenario: Illegal state transition is rejected
- **id:** `signaling.cmd.illegal-transition-rejected`
- **GIVEN** an interaction already in terminal state `ended`
- **WHEN** a client sends a `resume`/`send_message` command for it
- **THEN** the router rejects the command as an illegal transition and writes no `.log` fact

#### Scenario: Retried command with the same command_id is idempotent
- **id:** `signaling.cmd.idempotent-command-id`
- **GIVEN** a client publishes a `send_message` command carrying `command_id = K`
- **WHEN** the client times out and retries the same command with the SAME `command_id = K`
- **THEN** the router dedups on `command_id` and appends exactly ONE `.log` fact (no second fact for the retry)
- **AND** that fact carries `caused_by = K`, so the client correlates the single exactly-once effect of its command
- **AND** a command the router rejects returns a result to the issuer carrying `command_id = K` and a reason

#### Scenario: Command result is an ephemeral request/reply ack on the issuer's inbox
- **id:** `signaling.cmd.result-transport`
- **GIVEN** a client that publishes a command as a NATS request on `interaction.<id>.cmd` carrying a reply `_INBOX`
- **WHEN** the router processes it
- **THEN** for an accepted command the router replies on that `_INBOX` with `CommandResult { command_id, status: "accepted", caused_by }` where `caused_by` references the produced `.log` fact (= `command_id`), and the authoritative effect is the `.log` fact (the result is a correlation/ack, not the source of truth)
- **AND** for a rejected/illegal/forged/conflict command the router replies on the issuer's `_INBOX` with `CommandResult { command_id, status: "rejected", reason }` and writes no `.log` fact
- **AND** the `CommandResult` is ephemeral on core NATS (never JetStream) and is delivered only to the issuer's own reply `_INBOX` — it does NOT leak to any other user

#### Scenario: Reused command_id with a divergent payload is a conflict
- **id:** `signaling.cmd.command-id-conflict`
- **GIVEN** a command accepted under `command_id = K` with payload P
- **WHEN** the client retries with `command_id = K` and an IDENTICAL payload P, versus reuses `command_id = K` with a DIFFERENT payload P'
- **THEN** the identical retry replays the ORIGINAL `CommandResult` (idempotent — no second `.log` fact, same `accepted`/`caused_by = K`)
- **AND** the divergent reuse is rejected with `CommandResult { command_id: K, status: "rejected", reason: "conflict" }` (the key is bound to its original request — a client bug), writing no `.log` fact and producing no second effect

#### Scenario: Concurrent interaction-level commands are serialized by a state guard
- **id:** `signaling.cmd.concurrent-interaction-guard`
- **GIVEN** an interaction whose state the router guards via compare-and-set
- **WHEN** a second `interaction.transfer.requested` arrives while the interaction is already `transferring`, or a `recording.started` arrives while recording is already in effect
- **THEN** the router rejects the second transfer (one transfer in flight at a time) and treats the duplicate recording-start as idempotent-or-rejected (never a second concurrent recording), each with a `command_id`-bearing rejection result where rejected
- **AND** these state-guard rejections are distinct from `command_id` retry-dedup (they guard different commands targeting the same interaction state)

### Requirement: Single-node NATS backbone (JetStream + WebSocket + MQTT + $SYS)
The system MUST run one NATS server with JetStream enabled, the WebSocket and MQTT (3.1.1)
listeners active, and a system account exporting `$SYS` connection events.

#### Scenario: Web client connects over WebSocket
- **id:** `signaling.nats-ws-connect`
- **GIVEN** a NATS server with a `websocket{}` listener
- **WHEN** a `nats.ws` client connects from the browser
- **THEN** the connection succeeds and the client can publish and subscribe within its ACL

#### Scenario: MQTT listener configured (mobile-ready, unused now)
- **id:** `signaling.mqtt-listener-ready`
- **WHEN** the server starts with `mqtt{}` (3.1.1) enabled
- **THEN** the MQTT listener accepts connections (no mobile client uses it yet)

### Requirement: Unified Interaction — medium is a payload property
The system MUST model every chat / voice / video / email / social exchange as one
**Interaction**. The **medium is a field in the event payload**, NEVER part of the subject.
There MUST be no medium-specific subjects (no `call.invite`, no `chat.offer`).

#### Scenario: A chat and a call share the same interaction subjects
- **id:** `signaling.unified-interaction`
- **GIVEN** an interaction that starts as chat and escalates to a voice call
- **WHEN** chat messages and call events are recorded
- **THEN** they use the same `interaction.<id>.log` / `.cmd` / `.signal` subjects, differing only by `event_type`/`medium` in the payload

### Requirement: Offer lifecycle is target-addressed and fully terminal
The router MUST deliver an interaction **offer** to a target user who is NOT yet in the
interaction on a separate routing tree, distinct from interaction subjects and from any
JetStream stream that carries interaction facts. An offer MUST progress through an explicit
state machine: `offered → ringing → terminal`, where terminal is exactly one of
`accepted | rejected | cancelled | withdrawn | accepted_elsewhere | timed_out_rona |
expired | no_responder_fast_rona`. The router MUST publish every terminal transition to the
offer-control subject `routing.offer.user.<userId>.control` (`{ offer_id, terminal, reason }`)
that the ringing client subscribes to, so the client stops ringing immediately. The offer is a
NATS request/reply on an `_INBOX` whose SINGLE reply is the terminal `accept`/`reject`;
`ringing` is implicit once the request is published with no `503 No Responders` and there is NO
separate ringing reply (it would consume the inbox). Non-reply terminals
(cancel/withdraw/accepted_elsewhere) MUST be pushed on `routing.offer.user.<userId>.control`,
NOT via the reply. The reply `_INBOX` MUST carry a one-time nonce bound to
`tenant_id`/`offer_id`/`target`.

The offer ring is pure **routing**: the media engine/vendor is NOT in the offer. The ring
payload MUST carry the interaction `medium` (chat/voice/video) and an OPTIONAL opaque
`context_preview` (a router-supplied trimmed projection of the interaction's opaque `context`,
e.g. a customer display name) alongside `offer_id`/`timeout_ms`/nonce, so the agent knows what
kind of interaction it is BEFORE accepting. RelayPoint never parses `context_preview`. The media
engine / `media_profile` MUST NOT be carried in the offer; it is bound only at media-setup
(`media_profile` + MediaAdapter + MediaCredentials).

#### Scenario: Offer ring carries medium and an opaque context preview
- **id:** `signaling.offer.medium-context-preview`
- **GIVEN** the router rings `routing.offer.user.<userId>` with `offer_id`, `timeout_ms`, and a nonce
- **WHEN** the ring payload is delivered to the agent
- **THEN** it also carries the interaction `medium` (chat/voice/video) and an OPTIONAL opaque `context_preview` (router-supplied trimmed projection of the interaction's opaque `context`, e.g. a customer display name, which RelayPoint never parses), so the agent knows the kind of interaction before deciding
- **AND** the media engine / `media_profile` is NOT in the offer; it is bound only later at media-setup (`media_profile` + MediaAdapter + MediaCredentials)

#### Scenario: Offer accepted within timeout
- **id:** `signaling.offer.accept`
- **GIVEN** the router rings `routing.offer.user.<userId>` with `offer_id`, `timeout_ms`, and a nonce
- **WHEN** the client replies `accept` with the matching nonce before the timeout
- **THEN** the router records the accept, drives the offer to `accepted`, grants the user the interaction ACL, and returns the `interaction_id`

#### Scenario: Reject and no-answer (RONA) terminate the offer
- **id:** `signaling.offer.reject-and-rona`
- **GIVEN** a ringing offer with a timeout
- **WHEN** the client replies `reject` (→ `rejected`) OR no valid accept arrives before the timeout (→ `timed_out_rona`)
- **THEN** the router drives the offer to that terminal state, publishes it on `routing.offer.user.<userId>.control`, requeues the interaction, and does not join the user

#### Scenario: Offer expiry is distinct from no-answer RONA
- **id:** `signaling.offer.expired-vs-rona`
- **GIVEN** an offer whose TTL elapses BEFORE the ring is delivered/accepted (e.g. the request is published but the target never effectively rings within the offer's lifetime)
- **WHEN** the offer TTL passes with no accept
- **THEN** the router drives the offer to `expired` and pushes it on `routing.offer.user.<userId>.control`, distinct from `timed_out_rona` (the target rang but did not answer within the answer timeout)

#### Scenario: Fast-RONA when the target has no subscriber
- **id:** `signaling.offer.no-responder-fast-rona`
- **GIVEN** the target user has no subscriber on `routing.offer.user.<userId>` (offline / never-subscribed)
- **WHEN** the router publishes the offer request and NATS returns `503 no responders`
- **THEN** the router drives the offer to `no_responder_fast_rona` immediately (without waiting the full timeout) and requeues — distinct from a no-answer timeout

#### Scenario: Double-accept is first-writer-wins
- **id:** `signaling.offer.double-accept-cas`
- **GIVEN** the same offer fanned out to multiple devices/agents with `offer_id`/`route_attempt_id`
- **WHEN** two `accept` replies arrive
- **THEN** the router performs a compare-and-set so the first valid accept wins (`accepted`) and every later accept is answered `accepted_elsewhere` / 409
- **AND** a repeated accept from the same winning device is idempotent (no second join)

#### Scenario: Only originator or router may cancel or withdraw a ringing offer
- **id:** `signaling.offer.cancel-withdraw-authorized`
- **GIVEN** a ringing offer originated by caller `alice`
- **WHEN** the originator sends a `cancel` command (→ `cancelled`) or the router withdraws it (→ `withdrawn`, e.g. the customer abandoned)
- **THEN** the router publishes the terminal state on `routing.offer.user.<target>.control` and the target stops ringing immediately
- **AND** a cancel/withdraw from any other actor is rejected by ACL + author check

#### Scenario: Accept and withdraw crossing in flight resolve to one terminal
- **id:** `signaling.offer.accept-withdraw-cross`
- **GIVEN** a ringing offer for which the target's `accept` reply and the router's own `withdraw` are in flight at the same instant
- **WHEN** both reach the router's single per-offer state machine
- **THEN** the router applies the same compare-and-set on `offer_id`: whichever transition commits first wins and the offer reaches exactly one terminal state
- **AND** if `withdrawn` commits first, the late `accept` loses the CAS and is answered `accepted_elsewhere` / 409, the user is NOT joined, and its optimistic UI rolls back because no interaction-scoped grant is issued
- **AND** if `accepted` commits first, the withdraw is a no-op against the already-accepted offer; a genuine customer abandon is then handled as an `interaction.abandoned` transition that tears down the established call via the call state machine, never a phantom half-join

#### Scenario: Reconnect during ring reconstructs active offers
- **id:** `signaling.offer.reconnect-during-ring`
- **GIVEN** an active offer persisted by the router in NATS KV `offer.active.<userId>` per fanned-out user (a team/queue offer writes one `offer.active.<userId>` for EACH user it is fanned out to; there is no team-level KV the client reads)
- **WHEN** the target client drops and reconnects while still ringing
- **THEN** the client reconstructs only its own pending offers from `offer.active.<self>` and resumes ringing
- **AND** on accept/withdraw/terminal the router clears those per-user KV entries
- **AND** a client-local ring timeout acts as a backstop if no control message arrives

### Requirement: Interaction events split by QoS (durable log vs ephemeral signal)
Per interaction the system MUST expose a **durable, ordered** `interaction.<id>.log`
(JetStream) for facts, a write-only **command** subject `interaction.<id>.cmd` for client
intents, and an **ephemeral** per-publisher `interaction.<id>.signal.<userId>` (core NATS,
NEVER JetStream) for high-rate transient signals. The publisher's id is in the SUBJECT, so the
NATS publish-ACL enforces authorship; subscribers read `interaction.<id>.signal.*`. `sequence`
on `.log` is router-assigned; clients never set it.

#### Scenario: Log events are durable and replayable
- **id:** `signaling.log-durable`
- **GIVEN** a router-written `message.created` (or `call.connected`, `webrtc.offer`, `interaction.ended`) on `interaction.<id>.log` while a participant is offline
- **WHEN** the participant reconnects and consumes the stream
- **THEN** the event is delivered in order with its router-assigned `sequence` (durable, not lost)

#### Scenario: Signal events never persist
- **id:** `signaling.signal-ephemeral`
- **WHEN** `webrtc.ice` / `typing` events are published on `interaction.<id>.signal.<userId>` (subscribers read `interaction.<id>.signal.*`)
- **THEN** they are delivered at-most-once on core NATS and are NOT stored by JetStream

### Requirement: Media stays off NATS; the media descriptor is opaque
The system MUST carry only the **media-negotiation descriptor** (SDP today) and ICE over NATS
(descriptor as durable state on `.log`; ICE candidates ephemerally on the per-publisher
`.signal.<userId>`); audio/video MUST flow over WebRTC peer-to-peer (relayed by coturn only when
NAT requires it). The media-negotiation descriptor MUST be treated as an **opaque blob** that
neither NATS nor the router parses; the carrying event MUST include a `media_profile`
discriminator (Phase-1: `webrtc-p2p`) so an alternative media engine can ride the same signaling
plane — or bring its own — WITHOUT baking SDP-as-format into the universal contract.

#### Scenario: Call media bypasses the broker
- **id:** `signaling.media-bypass-broker`
- **GIVEN** two browser tabs in an established call
- **WHEN** audio/video flows between them
- **THEN** no media packet passes through NATS (only the SDP descriptor + ICE did, during setup)

#### Scenario: Router records the media descriptor without parsing it
- **id:** `signaling.media-descriptor-opaque`
- **GIVEN** a call command whose payload carries a media-negotiation descriptor and `media_profile = webrtc-p2p`
- **WHEN** the router records it on `.log`
- **THEN** the router stores the descriptor as an opaque blob (it neither parses nor validates SDP) and orders it by `sequence`, so a different `media_profile` can carry a non-SDP descriptor on the same plane

### Requirement: 1:1 call / WebRTC lifecycle
The system MUST support a 1:1 WebRTC call state machine:
`idle → setup_offered → answered → ice_connecting → connected →
{ renegotiating | held | transferring | reconnecting } → connected`, with terminal states
`{ cancelled | ended | media_failed | setup_failed }`. Multi-party / conference is
explicitly DEFERRED (Phase 1 is 1:1). Transfer in M1 is **cold/blind only** and is interaction
**re-routing**, not a media-call property. The router MUST record the cold-transfer lifecycle as
ordered `.log` facts: `interaction.transfer.requested`, `interaction.transfer.accepted`,
`interaction.transfer.rejected`, `interaction.transfer.cancelled`,
`interaction.transfer.failed`, and the existing `interaction.transferred`. On a request the
router offers the new target on the routing tree; **on accept it grants the new leg's ACL FIRST
and only THEN revokes the old leg's** (new-active-before-old-revoked, so there is NO media gap),
emitting `interaction.transfer.accepted` then `interaction.transferred`. On target
reject/RONA/cancel/fail (`interaction.transfer.rejected`/`.cancelled`/`.failed`) the router
**retains the ORIGINAL leg** — the old ACL is never revoked and the original call continues.
Warm/consultative transfer and multiparty are DEFERRED to a future SFU adapter. Glare MUST be resolved by perfect-negotiation with a
deterministic polite/impolite role. Incoming ICE MUST be buffered until the matching SDP is
applied. Renegotiation/ICE-restart MUST carry a `negotiation_id`/generation and discard
stale (lower-generation) signaling. The `webrtc.*` event types and the
perfect-negotiation/glare/ICE-buffering **choreography are the `webrtc-p2p` media profile**,
owned by the client/SDK; the router only records and orders the (opaque) media descriptors and
MUST NOT enforce negotiation policy.

#### Scenario: Setup, answer, connect
- **id:** `signaling.call.setup-connect`
- **GIVEN** caller publishes a `webrtc.offer` command and the router records it on `.log`
- **WHEN** the callee answers (`webrtc.answer`) and ICE connectivity is established
- **THEN** the call advances `setup_offered → answered → ice_connecting → connected`

#### Scenario: Glare resolved by perfect negotiation
- **id:** `signaling.call.glare-perfect-negotiation`
- **GIVEN** both peers emit a `webrtc.offer` at the same time (glare)
- **WHEN** the colliding offers are detected via deterministic polite/impolite roles (e.g. caller=impolite, else lexical userId tie-break)
- **THEN** the polite peer rolls back its local offer and accepts the remote offer, the impolite peer ignores the colliding incoming offer and keeps its own, and the call still reaches `connected` with one agreed direction

#### Scenario: ICE buffered until SDP applied
- **id:** `signaling.call.ice-buffered-until-sdp`
- **GIVEN** ICE candidates arrive on `.signal` before the matching SDP is applied from `.log`
- **WHEN** the peer receives those candidates
- **THEN** it buffers them and adds them only after the corresponding offer/answer SDP is set (different transports, ordered by SDP)

#### Scenario: Renegotiation / ICE-restart discards stale generation
- **id:** `signaling.call.renegotiation-generation`
- **GIVEN** a connected call where the router records a `webrtc.renegotiation.offer` with `negotiation_id` generation N
- **WHEN** a peer later receives signaling tagged with a generation lower than N
- **THEN** it discards the stale signaling and applies only the current generation

#### Scenario: Hold and resume
- **id:** `signaling.call.hold-resume`
- **GIVEN** a connected call
- **WHEN** a participant issues hold (→ `call.held`) and later resume (→ `call.resumed`)
- **THEN** the SDP direction changes accordingly (local/remote/both) and the state returns to `connected` on resume

#### Scenario: Cold/blind transfer re-routes the interaction
- **id:** `signaling.call.transfer`
- **GIVEN** a connected call and an `interaction.transfer.requested` command (cold/blind re-route)
- **WHEN** the router offers the new target on the routing tree and the target accepts
- **THEN** the router grants the new leg's ACL and revokes the old leg's ACL with NO warm overlap (the old leg is torn down as the new leg joins), and emits `interaction.transfer.accepted` then `interaction.transferred`
- **AND** warm/consultative and multiparty transfer are not part of M1 (deferred to a future SFU adapter)

#### Scenario: Transfer non-accept retains the original leg
- **id:** `signaling.call.transfer-non-accept`
- **GIVEN** a connected call and an `interaction.transfer.requested` whose offered target rejects, RONAs, is cancelled by the originator, or fails
- **WHEN** the router records the corresponding terminal (`interaction.transfer.rejected` / `interaction.transfer.cancelled` / `interaction.transfer.failed`)
- **THEN** the router does NOT revoke the original leg's ACL and the original call continues uninterrupted (no `interaction.transferred` is emitted)

#### Scenario: Transfer grants the new leg before revoking the old
- **id:** `signaling.call.transfer-leg-handover`
- **GIVEN** an `interaction.transfer.requested` whose target accepts
- **WHEN** the router commits the handover
- **THEN** it grants the new leg's ACL FIRST and only THEN revokes the old leg's ACL (new-active-before-old-revoked), so there is no window in which neither leg is media-authorized (no media gap)

#### Scenario: Setup cancel before connect
- **id:** `signaling.call.setup-cancel`
- **GIVEN** a call in `setup_offered`/`answered` (not yet `connected`)
- **WHEN** the caller hangs up before media connects
- **THEN** the router drives the call to `call.cancelled` / `call.setup_failed` and the peer rejects any late SDP/ICE for that attempt

#### Scenario: Media failure falls back then fails
- **id:** `signaling.call.media-failed-fallback`
- **GIVEN** a connected call whose ICE transitions to `failed`, or coturn is unavailable
- **WHEN** the peer enters `reconnecting` (grace) and attempts fallback ICE servers
- **THEN** if connectivity is not restored the router records `call.media_failed`; coturn-unavailable yields `media_failed` with the fallback ICE servers tried

### Requirement: Recording consent and retention are first-class facts
The router MUST record the recording lifecycle on `.log` as ordered facts:
`recording.consent.requested`, `recording.consent.granted`, `recording.consent.denied`,
`recording.started`, `recording.stopped`, `recording.upload.completed`, and
`recording.upload.failed`. Each fact's `data` MUST carry, as applicable, `retention_policy`,
`recorder_id`, `object_ref?` (the stored artifact reference) and `failure_reason?`. The
**capture mechanism is NOT core**: producing the media bytes (e.g. a client-side `MediaRecorder`
for `webrtc-p2p`, or a future server/egress recorder) is the SDK/adapter's job and is
profile-specific. Core owns only the facts — it records and orders them, never the bytes.

The router MUST enforce recording **state legality** on `.log`: `recording.started` MUST be
rejected unless a `recording.consent.granted` is in effect; after a `recording.consent.denied`
a `recording.started` MUST be rejected; `recording.stopped` and the upload facts
(`recording.upload.completed`/`recording.upload.failed`) are valid only for a recording that was
started; and a retried start/stop (same `command_id`) is idempotent (no second fact).

#### Scenario: Consent lifecycle recorded as facts
- **id:** `signaling.recording.consent-facts`
- **GIVEN** a recording flow on an interaction
- **WHEN** consent is requested, then granted (or denied), and recording starts then stops
- **THEN** the router records `recording.consent.requested`, then `recording.consent.granted` (or `recording.consent.denied`), `recording.started`, and `recording.stopped` as ordered `.log` facts carrying `retention_policy` and `recorder_id`
- **AND** core records only the facts; it does not capture or store the media bytes (that is the profile-specific adapter's job)

#### Scenario: Upload status recorded as facts
- **id:** `signaling.recording.upload-status-facts`
- **GIVEN** a stopped recording whose bytes are being uploaded by the profile-specific recorder
- **WHEN** the upload completes (or fails)
- **THEN** the router records `recording.upload.completed` carrying `object_ref` (or `recording.upload.failed` carrying `failure_reason`) as an ordered `.log` fact

#### Scenario: Recording state legality is enforced
- **id:** `signaling.recording.state-legality`
- **GIVEN** the recording fact lifecycle on an interaction
- **WHEN** a `recording.started` arrives with no `recording.consent.granted` in effect, or after a `recording.consent.denied`, or a `recording.stopped`/upload fact arrives for a recording that was never started
- **THEN** the router rejects each as an illegal recording transition and writes no `.log` fact
- **AND** a `recording.started` only after `recording.consent.granted` is accepted, and a retried start/stop carrying the same `command_id` is idempotent (no second fact)

### Requirement: Interaction carries opaque context (metadata)
The router MUST record `interaction.context.updated` facts on `.log` carrying an opaque
`context` object that RelayPoint NEVER parses (Desk populates it with customer / integration /
custom data). The `context` is medium-agnostic and is ordered and replayable like any other
fact; the latest applied `interaction.context.updated` is the current context. The router treats
`context` as an opaque blob — it neither validates nor interprets its shape — exactly as it
treats the media descriptor.

#### Scenario: Context updates recorded as ordered facts
- **id:** `signaling.interaction.context-updated`
- **GIVEN** an interaction whose Desk-supplied customer/integration data changes over time
- **WHEN** a client issues a command to update the context
- **THEN** the router records an `interaction.context.updated` fact on `.log` carrying the opaque `context` object, ordered by `sequence`
- **AND** the router never parses or validates the `context` shape (opaque blob), and a replaying consumer reconstructs the latest context from these facts

### Requirement: Interaction lifecycle state machine
The interaction MUST follow an explicit state machine with the enumerated states
`new → routing → active → { transferring } → ended`, where `transferring` is a sub-state of
`active` that returns to `active` on transfer success/failure (the interaction itself does not
end on a cold transfer; only the transferred media leg is torn down — see the call state
machine). `abandoned` is a terminal reason reachable from `routing` (customer abandons
pre-assignment) or from `active` (customer abandons post-assignment), and the router drives the
interaction to `ended[abandoned]`. Participant presence is tracked as the `offline` (transient)
and `left` (permanent) sub-states of `active`, which do NOT themselves end the interaction. The
ONLY legal transitions are: `new → routing`, `new → active` (direct assignment),
`routing → active` (offer accepted), `routing → ended` (abandoned/no-route), `active →
transferring`, `transferring → active`, and `active → ended` (ended/abandoned/orphaned). The
router MUST reject any transition outside this set (e.g. no resume/transfer/message after
`ended`). When the customer leaves before/after assignment (`interaction.abandoned`) the router
MUST withdraw any ringing offers. The router MUST run an **orphaned reaper**: if all
participants are offline (per presence) for more than N minutes it MUST inject
`interaction.ended` with reason `orphaned`. `participant.offline` (transient, presence-driven)
MUST be distinguished from `participant.left` (explicit/permanent).

#### Scenario: Interaction state machine enumerates legal states and rejects others
- **id:** `signaling.interaction.state-machine`
- **GIVEN** the interaction state machine `new → routing → active → { transferring } → ended` with `abandoned` reachable pre/post-assignment and `offline`/`left` as `active` sub-states
- **WHEN** the router evaluates a transition request
- **THEN** it admits only the legal set (`new→routing`, `new→active`, `routing→active`, `routing→ended`, `active→transferring`, `transferring→active`, `active→ended`) and `transferring` resolves back to `active` (the interaction does not end on a cold transfer)
- **AND** any transition outside that set (e.g. `ended→active`, `routing→transferring`) is rejected and the `.log` is unchanged

#### Scenario: Invalid transition rejected
- **id:** `signaling.interaction.invalid-transition`
- **GIVEN** an interaction in `ended`
- **WHEN** any actor attempts to drive it back to an active state
- **THEN** the router rejects the transition and the `.log` is unchanged

#### Scenario: Customer abandon withdraws ringing offers
- **id:** `signaling.interaction.abandoned-withdraws-offers`
- **GIVEN** an interaction with a ringing offer to an agent
- **WHEN** the customer abandons (`interaction.abandoned`)
- **THEN** the router withdraws the ringing offer (terminal `withdrawn`) and the agent stops ringing

#### Scenario: Orphaned reaper ends a fully-offline interaction
- **id:** `signaling.interaction.orphaned-reaper`
- **GIVEN** an active interaction whose every participant is offline per presence for more than N minutes
- **WHEN** the reaper runs
- **THEN** the router injects `interaction.ended` with reason `orphaned`

#### Scenario: Transient offline distinct from explicit leave
- **id:** `signaling.interaction.offline-vs-left`
- **GIVEN** a participant in an active interaction
- **WHEN** their connection drops transiently (presence) versus they explicitly leave
- **THEN** the router records `participant.offline` (transient, recoverable) versus `participant.left` (permanent), and only `left` removes them from the interaction

### Requirement: Delivery, ordering, and idempotency
The router MUST assign `sequence` on every `.log` fact; clients MUST never set it. The broker
publish-dedup key MUST be a deterministic **per-command** id (`Nats-Msg-Id =
<tenant>.<interactionId>.<command_id>`), so a retried command never appends a second fact — a
fresh `event_id` per attempt would NOT dedup and is therefore NOT the publish key. Clients MUST
order and dedup by the router-assigned `sequence` (strictly monotonic per interaction);
`event_id` is the fact's stable identity, not the broker dedup key. `message.updated`/
`message.deleted` MUST carry `ref_id` (the target `event_id`), with tombstone vs redaction
defined. Consumers MUST track the last durable sequence and, on a detected gap, pause live apply
and replay from JetStream. `notify.<userId>` is advisory and reconciled by `.log` replay.

#### Scenario: Duplicate command publish is deduped by the per-command id
- **id:** `signaling.delivery.msgid-dedup`
- **GIVEN** a fact appended with publish id `Nats-Msg-Id = <tenant>.<interactionId>.<command_id>`
- **WHEN** the same `command_id` is published again
- **THEN** JetStream stores the fact once; clients order and dedup by the router-assigned `sequence`

#### Scenario: Update/delete reference the target by ref_id
- **id:** `signaling.delivery.ref-id-update-delete`
- **GIVEN** a `message.created` with `event_id` E
- **WHEN** a `message.updated` or `message.deleted` is recorded
- **THEN** it carries `ref_id = E` and applies as redaction (edit) or tombstone (delete) against E

#### Scenario: Gap detection triggers replay
- **id:** `signaling.delivery.gap-replay`
- **GIVEN** a consumer tracking the last applied `sequence`
- **WHEN** it observes a gap in router-assigned `sequence`
- **THEN** it pauses live apply and replays from JetStream until the gap is filled, then resumes

### Requirement: Time authority and clock-skew immunity
All ordering, staleness, and expiry decisions MUST use the router-assigned monotonic `sequence`
and the negotiation `generation` and server-authoritative timers — NEVER client wall-clock time.
`occurred_at` is **informational/display only**; it MUST NOT drive ordering, staleness, dedup, or
any security/authorization decision. Token/ticket/credential expiry MUST be expressed as a
server-issued **relative TTL** (or validated against server time), so a skewed client clock
cannot cause a wrong accept/ignore; server rejection (max-connection-lifetime / auth-callout) is
the authoritative backstop. Clients/SDKs SHOULD derive a server-clock offset from server
responses rather than trusting the local wall-clock.

#### Scenario: occurred_at is informational and never flips ordering
- **id:** `signaling.time.occurred-at-informational`
- **GIVEN** `.log` facts whose `occurred_at` timestamps are out of order with respect to their router-assigned `sequence` (e.g. a skewed writer or display clock)
- **WHEN** a consumer orders and applies the facts
- **THEN** they are ordered strictly by `sequence` and `occurred_at` never flips the ordering, staleness, dedup, or any security/authorization decision (it is display-only)

#### Scenario: Expiry is enforced by a server relative-TTL / server-authoritative timer
- **id:** `signaling.time.relative-ttl-expiry`
- **GIVEN** a token/ticket/credential whose expiry is expressed as a server-issued relative TTL (or validated against server time) and a client whose wall-clock is skewed
- **WHEN** the client evaluates whether the item is still valid
- **THEN** expiry is enforced by the server via the relative TTL / server-authoritative timer (max-connection-lifetime / auth-callout rejection is authoritative), and the skewed client clock does not bypass nor prematurely trigger expiry

### Requirement: Failure-mode handling
The system MUST bound token-expiry exposure: because auth-callout runs only at CONNECT, the
server MUST enforce a **maximum NATS connection lifetime** (or actively kill connections whose
token expired); the client refreshes its scoped token and reconnects. Presence MUST debounce
(~5s) before broadcasting disconnect and track session/device counts to avoid false RONA on
flap. Agents who RONA/reject repeatedly MUST be placed in a **penalty-box** (backoff) that
suspends offers to them. On router crash, offer/interaction state MUST survive in NATS KV with
a TTL sweeper and idempotent terminal transitions so there are no stuck-ringing or
phantom-assignment states.

#### Scenario: Expired token connection is bounded and reconnects
- **id:** `signaling.failure.token-expiry-max-lifetime`
- **GIVEN** a connected client whose scoped token will expire mid-session
- **WHEN** the token expires
- **THEN** the connection is terminated at the enforced max lifetime (or killed by the ledger), and the client refreshes the token and reconnects transparently

#### Scenario: Presence debounce avoids false RONA on flap
- **id:** `signaling.failure.presence-debounce`
- **GIVEN** an agent whose connection flaps briefly
- **WHEN** the disconnect lasts less than the debounce window (~5s) and other sessions/devices remain
- **THEN** presence does not broadcast offline and the router does not RONA offers to that agent

#### Scenario: Repeated RONA puts the agent in the penalty-box
- **id:** `signaling.failure.rona-penalty-box`
- **GIVEN** an agent who RONAs/rejects repeatedly
- **WHEN** the threshold is crossed
- **THEN** the router suspends offers to that agent (backoff/penalty-box) until the box expires

#### Scenario: Router crash recovery leaves no stuck offers
- **id:** `signaling.failure.router-crash-recovery`
- **GIVEN** offer/interaction state persisted in NATS KV with TTLs
- **WHEN** the router crashes and restarts (or a TTL sweeper runs)
- **THEN** terminal transitions are reapplied idempotently and no offer remains stuck-ringing and no interaction is phantom-assigned

### Requirement: Presence is published only by the presence service
The presence service MUST derive presence from `$SYS.ACCOUNT.*.{CONNECT,DISCONNECT}` (not
MQTT LWT) and be the ONLY publisher of `presence.<userId>`; clients MUST be subscribe-only on
presence subjects.

#### Scenario: Presence updates on connect/disconnect
- **id:** `signaling.presence-from-sys`
- **WHEN** a user's connection opens or closes
- **THEN** the presence service publishes the new state on `presence.<userId>` and clients only read it

#### Scenario: Clients cannot publish presence
- **id:** `signaling.presence-publish-restricted`
- **GIVEN** a connected client
- **WHEN** it attempts to publish `presence.<userId>`
- **THEN** NATS denies the publish (only the presence service holds that write ACL)

### Requirement: Durable notifications on JetStream
The system MUST deliver notifications durably via JetStream on `notify.<userId>`, so a
disconnected user receives them on reconnect.

#### Scenario: Notification survives a reconnect
- **id:** `signaling.notify-durable`
- **GIVEN** a notification published while the user is offline
- **WHEN** the user reconnects and consumes the stream
- **THEN** the notification is delivered (durable)

### Requirement: Router-authoritative security and tenancy
Every command/control subject MUST be tenant-prefixed, and the router MUST reject any payload
whose `tenant_id` mismatches the subject even if the subject ACL passes. `.log` MUST be
router-authored only; the router MUST validate `author == the connection's authenticated
identity`. `.signal` MUST be per-publisher (`interaction.<id>.signal.<userId>`) so the NATS
publish-ACL binds the author to the subject; it MUST be rate-limited per user/interaction with
a cap on ICE candidates per negotiation. Per-interaction grants MUST be enforced dynamically via the NATS **auth
callout**: the callout mints a connection's subjects from a short-lived, nonce/audience-bound,
revocable, actor+interaction-scoped token; on offer-accept the user reconnects with a token
that adds ONLY `tenant.<tid>.interaction.<id>.>`. Privileged controls
(cancel/withdraw/transfer/ACL grant-revoke) MUST be audited with actor and reason.

#### Scenario: Cross-tenant subscription denied
- **id:** `signaling.tenant-isolation`
- **WHEN** a user authenticated for tenant A subscribes to `tenant.<B>.interaction.*.>`
- **THEN** NATS denies the subscription

#### Scenario: Payload tenant mismatch rejected even when subject ACL passes
- **id:** `signaling.security.payload-tenant-match`
- **GIVEN** a command published on a correctly tenant-prefixed `.cmd` subject the client may write
- **WHEN** the payload `tenant_id` does not match the subject's tenant
- **THEN** the router rejects the command and writes no `.log` fact

#### Scenario: Interaction grant is scoped to the accepted interaction only
- **id:** `signaling.acl-interaction-scoped`
- **GIVEN** a user whose auth-callout token was issued on accepting interaction X, granting only `tenant.<tid>.interaction.X.>`
- **WHEN** they subscribe to a different `tenant.<tid>.interaction.Y.log` they did not accept
- **THEN** NATS denies the subscription (the grant does not include `interaction.*` or interaction Y)

#### Scenario: Grant delivered via reconnect on accept
- **id:** `signaling.acl-after-accept`
- **GIVEN** a connected user who has not accepted an interaction's offer and is denied its `interaction.<id>.log`
- **WHEN** they accept the offer
- **THEN** the router issues a short-lived interaction-scoped token, the client reconnects, and the auth-callout authorizes `tenant.<tid>.interaction.<id>.>` for the new connection (callout authorizes at CONNECT time)

#### Scenario: Signal subject is rate-limited
- **id:** `signaling.security.signal-rate-limit`
- **GIVEN** a client publishing `webrtc.ice`/`typing` on its own `interaction.<id>.signal.<userId>`
- **WHEN** it exceeds the per-user/interaction rate or the per-negotiation ICE-candidate cap
- **THEN** the excess is dropped/throttled and does not reach JetStream (signal is never durable)

#### Scenario: Privileged controls are audited
- **id:** `signaling.security.privileged-audit`
- **GIVEN** a privileged control (cancel/withdraw/transfer/ACL grant-revoke)
- **WHEN** the router executes it
- **THEN** it records an audit entry with the actor identity and reason

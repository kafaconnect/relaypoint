# Delta for Client SDK

Behavior of the two design-first SDKs over the `signaling-core` contract. The SDK is a
**client**: it is ALWAYS a non-writer of `interaction.<id>.log` (write to `.log` is a
trusted-server capability per `signaling-core`). All subjects are prefixed
`tenant.<tenantId>.` (omitted below). The `signaling-core` wire envelope is:
`{ schema, event_type, event_id, sequence, occurred_at, tenant_id, actor_id, medium, media_profile?, command_id?, caused_by?, ref_id?, data }`
(event-specific fields like `negotiation_id`/`object_ref`/`failure_reason` ride inside `data`).
The wire form is **snake_case** (the authoritative contract); the SDK's public TS surface is its
**camelCase projection**. The projection is PRECISE, not blanket-verbatim: `LogEvent` projects
only the envelope fields that apply to **facts** — it carries `causedBy` (the `command_id` that
produced the fact) but NOT `commandId` (which is COMMAND-only); and event-specific fields
(`negotiationId`, `objectRef`, `failureReason`, etc.) live INSIDE `data`, not as top-level
envelope fields on `LogEvent`. `Command` carries `commandId`. Each projected field maps 1:1 to its
snake_case wire counterpart (e.g. `commandId↔command_id`, `causedBy↔caused_by`,
`eventType↔event_type`); the normative mapping lives in `design.md`.
Scenario ids are `clientsdk.<area>.<case>` and each is satisfied by a future automated test
tagged `// @spec:<id>`. Implementation is deferred until a buildable `signaling-core` server exists.

## ADDED Requirements

### Requirement: Connection lifecycle with token-refresh reconnect
The TS SDK MUST connect via `nats.ws` using a token obtained from a `getToken()` callback and
auto-reconnect on transient drops. Because the auth-callout authorizes at CONNECT time and the
server enforces a maximum NATS connection lifetime, on max-lifetime / token-expiry the SDK
MUST refresh the scoped token via `getToken()` and reconnect transparently. After offer-accept
the SDK MUST reconnect with the interaction-scoped token so the new connection is authorized
for `interaction.<id>.>`.

#### Scenario: Connect with a token from the callback
- **id:** `clientsdk.connection.connect-with-token`
- **GIVEN** a `RelayPointClient` configured with `getToken()`
- **WHEN** `connect()` is called
- **THEN** the SDK calls `getToken()` and opens a `nats.ws` connection with that token
- **AND** publish/subscribe succeed only within the connection's auth-callout-minted ACL

#### Scenario: Token-expiry triggers transparent refresh-reconnect
- **id:** `clientsdk.connection.token-refresh-reconnect`
- **GIVEN** a connected client whose scoped token expires at the enforced max connection lifetime
- **WHEN** the connection is terminated for token expiry
- **THEN** the SDK calls `getToken()` again, reconnects transparently, and resumes its subscriptions

#### Scenario: Reconnect with the interaction-scoped token after accept
- **id:** `clientsdk.connection.reconnect-interaction-scoped`
- **GIVEN** a user who has just accepted an offer and received an interaction-scoped token
- **WHEN** the SDK reconnects with that token
- **THEN** the new connection is authorized for `interaction.<id>.>` and the user is treated as joined only then

### Requirement: Connection state is observable and getToken failure is fatal after retries
The TS SDK MUST expose a `ConnectionState`
(`disconnected | connecting | connected | reconnecting | closed | failed`) as a readable
property and via `on("state", ...)`. On `getToken()` failure (at connect or refresh) the SDK
MUST retry with backoff and, after exhausting retries, emit a fatal `auth_failed`, transition to
`failed`, and stay disconnected — it MUST NOT silently loop forever.

#### Scenario: Connection state machine is observable
- **id:** `clientsdk.connection.state-observable`
- **GIVEN** a `RelayPointClient`
- **WHEN** it connects, drops, and reconnects
- **THEN** `client.state` and `on("state")` report the transitions `connecting → connected → reconnecting → connected` (and `closed` on `close()`)

#### Scenario: getToken failure becomes fatal after retries
- **id:** `clientsdk.connection.gettoken-failure`
- **GIVEN** a `RelayPointClient` whose `getToken()` keeps failing
- **WHEN** `connect()` (or a refresh) cannot obtain a token after the SDK's backoff retries are exhausted
- **THEN** the SDK emits a fatal `auth_failed`, transitions to `failed`, and stays disconnected (no infinite silent loop)

### Requirement: Media credential failure and proactive refresh
The TS SDK MUST handle `MediaCredentialProvider.fetch` failure (e.g. Media-IAM down) by raising
a typed error and transitioning the call to `setup_failed` (failure before connect) or
`media_failed` (failure after connect), per timing. The SDK MUST monitor
`MediaCredentials.expiresAt` and proactively refresh credentials BEFORE expiry by issuing a new
`fetch`, so a long call does not drop on credential (TURN-cred) expiry. On an
**expired/invalid signaling-session ticket** the `fetch` MUST fail with a typed error; the SDK
MUST then request a FRESH ticket (via the `relaypoint-go` minter path) and retry the `fetch`; if
recovery is unrecoverable the call MUST transition to `setup_failed` (pre-connect) or
`media_failed` (post-connect).

#### Scenario: Credential fetch failure fails the call with a typed error
- **id:** `clientsdk.creds.fetch-failure`
- **GIVEN** a call setting up media via `MediaCredentialProvider.fetch`
- **WHEN** the fetch fails because the Desk Media-IAM service is down
- **THEN** the SDK surfaces a typed error and the call transitions to `setup_failed` (pre-connect) or `media_failed` (post-connect)

#### Scenario: Credentials refreshed before expiry
- **id:** `clientsdk.creds.refresh-before-expiry`
- **GIVEN** an active call whose `MediaCredentials.expiresAt` approaches
- **WHEN** the SDK detects the impending expiry
- **THEN** it issues a new `fetch` and applies the refreshed credentials via `setCredentials` before expiry, so the call does not drop

#### Scenario: Expired/invalid ticket triggers a fresh-ticket retry
- **id:** `clientsdk.creds.ticket-expired-invalid`
- **GIVEN** a `MediaCredentialProvider.fetch(ticket, mediaProfile)` whose signaling-session ticket has expired or is invalid
- **WHEN** the Desk Media-IAM service rejects the ticket and `fetch` fails with a typed error
- **THEN** the SDK requests a FRESH ticket via the `relaypoint-go` minter path and retries the `fetch`
- **AND** if recovery is unrecoverable the call transitions to `setup_failed` (pre-connect) or `media_failed` (post-connect)

### Requirement: Offer ring controller
The TS SDK MUST subscribe `routing.offer.user.<self>` and surface `ring` events. `accept()`
and `reject()` MUST use the request/reply `_INBOX`+nonce path. The SDK MUST handle EVERY
non-reply core terminal pushed on `routing.offer.user.<self>.control` —
`cancelled | withdrawn | accepted_elsewhere | timed_out_rona | expired | no_responder_fast_rona`
(`rejected`/`accepted` are reply-path) — stopping the ring on each. `expired` (the offer TTL
elapsed before the ring was delivered/accepted) MUST be distinguished from `timed_out_rona` (the
target rang but did not answer within the timeout).
On reconnect the SDK MUST reconstruct only its own pending offers from KV `offer.active.<self>`.
A client-local ring-timeout backstop MUST fire if no control message arrives. Join MUST be
**optimistic-then-confirmed**: the user is joined only after the router's confirming ACL grant,
never on the optimistic `accept()` click.

The surfaced `Offer` MUST expose the interaction `medium` and an OPTIONAL opaque
`contextPreview` (mapping to wire `context_preview`) — the router-supplied trimmed projection of
the interaction context — so the UI can show e.g. "incoming video call from X" BEFORE the agent
answers. The SDK MUST NOT parse `contextPreview`, and the offer MUST NOT carry any media
profile/vendor (the media engine is bound only at media-setup).

#### Scenario: Ring surfaces medium and an opaque context preview pre-answer
- **id:** `clientsdk.offer.medium-context-preview`
- **GIVEN** the SDK subscribed on `routing.offer.user.<self>` receives an offer ring
- **WHEN** it surfaces the `Offer` to the consumer
- **THEN** the `Offer` exposes `medium` and an OPTIONAL opaque `contextPreview` (router-supplied projection of the interaction context, which the SDK never parses) so the UI can render e.g. "incoming video call from X" before answering
- **AND** no media profile/vendor is present on the offer (media is bound only later at media-setup)

#### Scenario: Ring surfaced and accepted via inbox+nonce
- **id:** `clientsdk.offer.ring-accept`
- **GIVEN** the SDK subscribed on `routing.offer.user.<self>` receives an offer request with a nonce
- **WHEN** the consumer calls `accept()`
- **THEN** the SDK replies `accept` with the matching nonce on the `_INBOX`
- **AND** it resolves to an `InteractionHandle` only after the confirming interaction-scoped reconnect

#### Scenario: Reject replies on the inbox
- **id:** `clientsdk.offer.reject`
- **GIVEN** a surfaced ring
- **WHEN** the consumer calls `reject()`
- **THEN** the SDK replies `reject` with the matching nonce and stops ringing, joining no interaction

#### Scenario: Every non-reply terminal stops the ring from control
- **id:** `clientsdk.offer.control-terminal`
- **GIVEN** a ringing offer
- **WHEN** any non-reply terminal (`cancelled` / `withdrawn` / `accepted_elsewhere` / `timed_out_rona` / `expired` / `no_responder_fast_rona`) is pushed on `routing.offer.user.<self>.control`
- **THEN** the SDK emits a `terminal` event carrying that terminal and stops ringing immediately without consuming the reply inbox
- **AND** `expired` (offer TTL elapsed before the ring was delivered/accepted) is surfaced distinctly from `timed_out_rona` (rang but no answer within the timeout)

#### Scenario: Pending offers reconstructed from KV on reconnect
- **id:** `clientsdk.offer.kv-reconstruct`
- **GIVEN** a client that drops while ringing, with the router-persisted entry in KV `offer.active.<self>`
- **WHEN** the client reconnects
- **THEN** the SDK reconstructs only its own pending offers from `offer.active.<self>` and resumes ringing
- **AND** a client-local ring-timeout backstop ends the ring if no control message arrives

#### Scenario: Optimistic accept rolls back when the grant never arrives
- **id:** `clientsdk.offer.optimistic-confirmed`
- **GIVEN** an `accept()` whose offer is lost to a CAS (`accepted_elsewhere` / withdraw)
- **WHEN** no confirming interaction-scoped ACL grant is issued
- **THEN** the SDK does NOT mark the user joined and rolls back any optimistic UI state

### Requirement: Command plane never writes the log
The TS SDK MUST publish intents only as COMMANDS on `interaction.<id>.cmd` and MUST NEVER
write `interaction.<id>.log` and MUST NEVER assign `sequence`. The SDK's only path to record a
fact is a command; the router is the authoritative writer. `send(command)` MUST attach a
client-generated `command_id`; on retry the SDK MUST REUSE the same `command_id` so the router
dedups (no double-append).

`send(command)` MUST be a NATS request on `interaction.<id>.cmd` carrying a reply `_INBOX`, and
MUST return `Promise<CommandResult>` where `CommandResult = { commandId: string; status:
"accepted" | "rejected"; causedBy?: string; reason?: string }` (the camelCase projection of the
router's ephemeral `CommandResult`). On `accepted` the promise MUST resolve with the
`CommandResult` (carrying `causedBy = commandId`, which correlates the resulting `.log` fact via
its `causedBy`); on `rejected` the SDK MUST reject the promise with a typed error carrying the
`reason` (and `commandId`). The SDK MUST correlate the exactly-once outcome via that
`CommandResult` and the `causedBy = commandId` fact on the `.log` stream — the result is an
ack/correlation, NOT the source of truth (the `.log` fact is).

#### Scenario: Send routes to the command subject
- **id:** `clientsdk.cmd.send-to-cmd`
- **GIVEN** an `InteractionHandle`
- **WHEN** the consumer calls `send(command)`
- **THEN** the SDK publishes on `interaction.<id>.cmd` and never on `interaction.<id>.log`

#### Scenario: SDK exposes no log-write path
- **id:** `clientsdk.cmd.no-log-write`
- **GIVEN** the public SDK surface
- **WHEN** a consumer looks for a way to write `.log` or set `sequence`
- **THEN** none exists; `.log` is read-only and `sequence` is router-assigned

#### Scenario: Retried command reuses the same command_id (idempotent)
- **id:** `clientsdk.cmd.idempotent-retry`
- **GIVEN** a `send(command)` that attaches `command_id = K`
- **WHEN** the SDK times out and retries the same command
- **THEN** it republishes with the SAME `command_id = K` so the router dedups (no second `.log` fact)
- **AND** the SDK correlates the single outcome via the resolved `CommandResult` and the `causedBy = K` fact on the `.log` stream (or a typed error from a rejected `CommandResult` carrying `commandId = K`)

#### Scenario: send resolves with the CommandResult and correlates the fact
- **id:** `clientsdk.cmd.result-correlation`
- **GIVEN** a `send(command)` issued as a request on `interaction.<id>.cmd` with a reply `_INBOX`, attaching `commandId = K`
- **WHEN** the router replies with an `accepted` `CommandResult { commandId: K, status: "accepted", causedBy: K }`
- **THEN** `send` resolves with that `CommandResult` and the SDK correlates the resulting fact via the `.log` fact whose `causedBy = K` (the result is an ack/correlation, not the source of truth)
- **AND** when the router replies `rejected` (`CommandResult { commandId: K, status: "rejected", reason }`) `send` rejects with a typed error carrying that `reason` and `commandId = K`, and the SDK never treats the rejected command as applied

#### Scenario: SDK surfaces the router's concurrent-command rejection
- **id:** `clientsdk.handle.concurrent-command-guard`
- **GIVEN** an interaction already `transferring`, or a recording already in effect
- **WHEN** the consumer issues a second concurrent `transfer(target)`, or a duplicate recording `start()`, that the router rejects via its interaction-level state guard
- **THEN** the rejected `CommandResult` (`status: "rejected"`, carrying `commandId`) surfaces as a typed error rejected from `send` rather than a success — the SDK never silently treats the rejected second command as applied

### Requirement: Wire-field naming is a normative camelCase projection
The SDK's public TS API MUST be **camelCase** while the NATS wire/envelope is **snake_case**.
The SDK's `LogEvent`, `Command`, and `CommandResult` types MUST be a PRECISE camelCase projection
of the `signaling-core` wire envelope — each projected field maps 1:1 to exactly one snake_case
wire field and back. The projection MUST respect which fields apply where: `LogEvent` (a FACT)
carries `causedBy` but NOT `commandId` (`command_id` rides on COMMANDS only); event-specific
fields (`negotiationId`, `objectRef`, `failureReason`, etc.) live INSIDE `data`, NOT as top-level
`LogEvent` fields. `Command` carries `commandId`. The `CommandResult` type MUST be
`{ commandId: string; status: "accepted" | "rejected"; causedBy?: string; reason?: string }`
projecting the router's ephemeral `CommandResult { command_id, status, caused_by?, reason? }`.
The normative field mapping MUST include at least: `commandId↔command_id`, `causedBy↔caused_by`,
`eventType↔event_type`, `eventId↔event_id`, `occurredAt↔occurred_at`, `actorId↔actor_id`,
`mediaProfile↔media_profile`, `tenantId↔tenant_id`, `refId↔ref_id`, and (inside `data`)
`negotiationId↔negotiation_id`, `objectRef↔object_ref`, `failureReason↔failure_reason`.

#### Scenario: Public camelCase fields map 1:1 to snake_case wire fields
- **id:** `clientsdk.cmd.wire-field-mapping`
- **GIVEN** a `LogEvent` projected from a wire `.log` fact, a `Command` published to `.cmd`, and a `CommandResult` reply
- **WHEN** the SDK serializes a command to the wire and deserializes a fact / command-result from the wire
- **THEN** each camelCase TS field maps to exactly one snake_case wire field per the normative table (`commandId↔command_id`, `causedBy↔caused_by`, `eventType↔event_type`, `eventId↔event_id`, `occurredAt↔occurred_at`, `actorId↔actor_id`, `mediaProfile↔media_profile`, `tenantId↔tenant_id`, `refId↔ref_id`)
- **AND** the projection is precise: `LogEvent` carries `causedBy` but NOT `commandId` (command-only), and event-specific fields (`negotiationId`, `objectRef`, `failureReason`, …) appear INSIDE `data` per the wire envelope, not as top-level `LogEvent` envelope fields — the SDK neither invents wire fields nor promotes data fields to the envelope

### Requirement: Ordered log delivery with dedup and gap-replay
The TS SDK MUST consume typed `.log` facts ordered by router `sequence`, dedup on
`Nats-Msg-Id = event_id`, and on a detected sequence gap pause live apply, replay from
JetStream until the gap is filled, then resume. When the replay CANNOT fill the gap (JetStream
unavailable / the stream cannot be reached), the SDK MUST surface a typed degraded/fatal delivery
state and retry the replay with backoff; it MUST NEVER silently drop facts past the gap nor loop
forever (it surfaces the degraded state rather than applying out-of-order or resuming live over
an unfilled gap).

#### Scenario: Facts delivered in router-sequence order
- **id:** `clientsdk.delivery.ordered-by-sequence`
- **GIVEN** an `InteractionHandle` consuming `interaction.<id>.log`
- **WHEN** facts arrive
- **THEN** the SDK delivers them to the consumer in ascending router-assigned `sequence` order

#### Scenario: Duplicate event_id is deduped
- **id:** `clientsdk.delivery.dedup-event-id`
- **GIVEN** a fact already applied with a given `event_id`
- **WHEN** the same `event_id` is delivered again
- **THEN** the SDK drops the duplicate and applies it once

#### Scenario: Sequence gap pauses live apply and replays
- **id:** `clientsdk.delivery.gap-replay`
- **GIVEN** the SDK tracking the last applied `sequence`
- **WHEN** it observes a gap in `sequence`
- **THEN** it pauses live apply, replays from JetStream until the gap is filled, then resumes live delivery

#### Scenario: Replay failure surfaces a typed degraded state with backoff
- **id:** `clientsdk.delivery.replay-failure`
- **GIVEN** the SDK paused on a detected sequence gap and attempting a JetStream replay
- **WHEN** the replay cannot fill the gap because JetStream is unavailable / the stream cannot be reached
- **THEN** the SDK surfaces a typed degraded/fatal delivery state and retries the replay with backoff, and it NEVER silently drops the missing facts nor resumes live over the unfilled gap nor loops forever

### Requirement: Time authority and clock-skew immunity
The TS SDK MUST order `.log` facts strictly by the router-assigned `sequence` (and discard stale
renegotiation by `generation`); it MUST treat `occurredAt` as **display-only** and MUST NOT use it
for staleness, ordering, dedup, or any security decision. Credential/ticket refresh MUST be driven
by a server-issued **relative TTL** (or a derived server-clock offset from server responses), not
the local wall-clock, so a skewed client clock cannot refresh too early/late nor wrongly treat a
still-valid ticket as expired; the server's rejection is authoritative. This ties to
`clientsdk.creds.refresh-before-expiry` and `clientsdk.creds.ticket-expired-invalid`.

#### Scenario: SDK orders by sequence and treats occurredAt as display-only
- **id:** `clientsdk.time.occurred-at-display-only`
- **GIVEN** `.log` facts whose `occurredAt` values are out of order with respect to their router `sequence`
- **WHEN** the SDK delivers and applies them
- **THEN** it orders strictly by `sequence` and uses `occurredAt` only for display, never for staleness/ordering/dedup or any security decision

#### Scenario: Credential/ticket refresh uses server relative-TTL, not local wall-clock
- **id:** `clientsdk.time.relative-ttl-refresh`
- **GIVEN** a `MediaCredentials`/signaling-session-ticket expiry expressed as a server-issued relative TTL (or validated against a derived server-clock offset) and a client whose wall-clock is skewed
- **WHEN** the SDK decides when to proactively refresh (`clientsdk.creds.refresh-before-expiry`) or when an expired/invalid ticket triggers a fresh-ticket retry (`clientsdk.creds.ticket-expired-invalid`)
- **THEN** it uses the server-issued relative TTL / server-clock offset rather than the local wall-clock, so a skewed clock does not refresh too early/late nor treat a still-valid ticket as expired, and the server's rejection remains authoritative

### Requirement: Interaction handle with own-author signal
`client.interaction(id)` MUST return a typed handle exposing a `.log` event stream, a
`.send(command)` write-only command path, and the ability to publish the consumer's OWN
`interaction.<id>.signal.<self>` (ICE / typing). The SDK MUST publish signals only under the
consumer's own author subject; it MUST NOT write another user's `signal.<userId>`.

#### Scenario: Handle exposes log stream and command send
- **id:** `clientsdk.handle.stream-and-send`
- **GIVEN** `client.interaction(id)`
- **WHEN** the consumer reads `events()` and calls `send(command)`
- **THEN** it receives ordered `.log` facts and the command is published on `interaction.<id>.cmd`

#### Scenario: Signal published only under own author subject
- **id:** `clientsdk.handle.signal-own-author`
- **GIVEN** a handle for self user `alice`
- **WHEN** the consumer publishes an ICE/typing signal
- **THEN** the SDK publishes on `interaction.<id>.signal.alice` only and never on another user's signal subject

### Requirement: Call controller drives facts via a MediaAdapter port
The `CallController` MUST expose `start` / `hold` / `resume` / `stop` and drive business call
facts via commands (the router writes `call.*` facts), while a `MediaAdapter` port executes
the media side. The controller MUST NOT itself write `.log`. The media-negotiation descriptor
MUST stay opaque to core and be tagged `media_profile`.

#### Scenario: Controller drives call facts through commands
- **id:** `clientsdk.call.facts-via-commands`
- **GIVEN** a `CallController` for an interaction
- **WHEN** the consumer calls `start()` / `hold()` / `resume()` / `stop()`
- **THEN** the SDK issues the corresponding commands on `interaction.<id>.cmd` and the router writes the `call.*` facts; the SDK writes no `.log`

#### Scenario: Adapter handles the opaque descriptor tagged by media_profile
- **id:** `clientsdk.call.opaque-descriptor`
- **GIVEN** a `MediaAdapter` producing a negotiation descriptor
- **WHEN** the descriptor is carried on the signaling plane
- **THEN** it is opaque to core and tagged with `media_profile` (Phase-1: `webrtc-p2p`)

### Requirement: Call media kind, track exposure, and state observability
The `CallController` MUST support both audio and video: `start(opts?: { audio?, video? })`,
`setMicEnabled(on)` (mute/unmute), and `setCameraEnabled(on)` (toggle camera; turning the camera
on during an audio call MUST upgrade audio→video via renegotiation carrying a `negotiationId`
(mapping to wire `negotiation_id`) + generation). The
controller MUST expose a readable `state: CallState` and `on("state", ...)` mirroring
signaling-core's 1:1 call state machine
(`idle | setup_offered | answered | ice_connecting | connected | renegotiating | held |
transferring | reconnecting | cancelled | ended | media_failed | setup_failed`). It MUST expose
local and remote media tracks via `on("track", (track, kind, origin) => ...)` so the UI can
render them. The `MediaAdapter` MUST support these: `createOffer({ audio?, video? })`,
`createAnswer(remote, mediaProfile)`, renegotiation carrying a generation, and `onRemoteTrack`.

#### Scenario: Call starts audio-only or video by media kind
- **id:** `clientsdk.call.media-kind`
- **GIVEN** a `CallController`
- **WHEN** the consumer calls `start()` versus `start({ video: true })`
- **THEN** the adapter creates an audio-only offer versus an audio+video offer (the descriptor stays opaque, tagged `media_profile`)

#### Scenario: Call state is observable
- **id:** `clientsdk.call.state-observable`
- **GIVEN** a `CallController` driving a call
- **WHEN** the call advances through setup, connect, hold, and end
- **THEN** `call.state` and `on("state")` report the `CallState` transitions mirroring signaling-core (e.g. `setup_offered → answered → ice_connecting → connected → held → connected → ended`)

#### Scenario: Local and remote tracks are exposed
- **id:** `clientsdk.call.track-exposure`
- **GIVEN** a connecting call
- **WHEN** local capture starts and the remote answer/tracks arrive
- **THEN** the SDK emits `on("track", (track, kind, origin))` for each local and remote `audio`/`video` track so the UI can render them

#### Scenario: Mic mute and camera toggle (audio→video upgrade)
- **id:** `clientsdk.call.toggle-mic-camera`
- **GIVEN** a connected audio call
- **WHEN** the consumer calls `setMicEnabled(false)` then `setCameraEnabled(true)`
- **THEN** the mic mutes, and enabling the camera upgrades the call audio→video via a renegotiation carrying a generation (stale lower-generation signaling discarded)

### Requirement: Interaction handle surfaces opaque context (metadata)
The `InteractionHandle` MUST surface the interaction's opaque **context** (metadata) as a
readable `metadata: unknown` (the latest applied context) and via `on("metadata", ...)`, fed by
`interaction.context.updated` `.log` facts. The SDK MUST treat the context as opaque — it never
parses it; Desk populates customer / integration / custom data.

#### Scenario: Metadata surfaced and updated from context facts
- **id:** `clientsdk.handle.metadata-observable`
- **GIVEN** an `InteractionHandle` consuming `.log`
- **WHEN** `interaction.context.updated` facts arrive over time
- **THEN** `handle.metadata` reflects the latest opaque context and `on("metadata")` fires with the updated context, which the SDK never parses

### Requirement: Media capability negotiation with graceful degrade
Every `MediaAdapter` MUST declare `MediaCapabilities` flags: `supportsWarmTransfer`,
`supportsMultiparty`, `supportsLocalRecording` (best-effort client-side capture), and
`supportsServerRecording` (compliance-grade server/egress). `supportsLocalRecording` MUST be
DISTINCT from `supportsServerRecording`: for `webrtc-p2p`, `supportsLocalRecording = true` while
`supportsServerRecording = false`. The `CallController` MUST degrade gracefully against those
flags and MUST NEVER silently pretend an unsupported capability.

#### Scenario: Adapter declares capability flags
- **id:** `clientsdk.capability.declared-flags`
- **GIVEN** the default `WebrtcP2pAdapter`
- **WHEN** the controller reads its `capabilities`
- **THEN** `supportsWarmTransfer`, `supportsMultiparty`, and `supportsServerRecording` are all `false`, `supportsLocalRecording` is `true`, and `mediaProfile` is `webrtc-p2p`

#### Scenario: Unsupported capability is refused, not faked
- **id:** `clientsdk.capability.no-silent-pretend`
- **GIVEN** an adapter with `supportsWarmTransfer=false`
- **WHEN** a warm transfer is requested
- **THEN** the controller refuses with a typed capability error and never silently performs a degraded substitute as if it were warm transfer

### Requirement: WebrtcP2pAdapter implements the webrtc-p2p profile choreography
The default `WebrtcP2pAdapter` MUST implement the `webrtc-p2p` media profile: glare resolved
by perfect-negotiation with a deterministic polite/impolite role, incoming ICE buffered until
the matching SDP is applied, and renegotiation carrying a generation so stale (lower-generation)
signaling is discarded. The renegotiation MUST carry a `negotiationId` (mapping to wire
`negotiation_id`) alongside the monotonic generation. This choreography is the profile, owned by
the SDK; the router records and orders only the opaque descriptors.

#### Scenario: Glare resolved by perfect negotiation
- **id:** `clientsdk.webrtcp2p.glare-perfect-negotiation`
- **GIVEN** both peers create an offer at the same time (glare) with deterministic polite/impolite roles
- **WHEN** the colliding offers are detected
- **THEN** the polite peer rolls back its local offer and accepts the remote one, the impolite peer keeps its own, and the call still reaches connected

#### Scenario: ICE buffered until SDP applied
- **id:** `clientsdk.webrtcp2p.ice-buffered-until-sdp`
- **GIVEN** ICE candidates arriving before the matching SDP is applied
- **WHEN** `addIceCandidate` is called
- **THEN** the adapter buffers them and adds them only after the corresponding offer/answer SDP is set

#### Scenario: Stale renegotiation generation discarded
- **id:** `clientsdk.webrtcp2p.renegotiation-generation`
- **GIVEN** a renegotiation tagged generation N
- **WHEN** the adapter later receives signaling tagged with a generation lower than N
- **THEN** it discards the stale signaling and applies only the current generation

### Requirement: Media credentials via ticket exchange
Media credentials MUST be obtained through the `MediaCredentialProvider` port via a ticket
exchange: RelayPoint issues a short-lived, opaque, vendor-agnostic **signaling-session ticket**,
and the client exchanges it at a **Desk-owned Media-IAM service** for the real credentials
(ephemeral coturn TURN creds for `webrtc-p2p`; a vendor/SFU room token for a future profile).
RelayPoint MUST NEVER hold vendor secrets or config; the **app (Desk) implements minting**.
RelayPoint MAY ship a *reference* coturn TURN-cred minter for p2p only.

#### Scenario: Ticket exchanged at the app Media-IAM service
- **id:** `clientsdk.creds.ticket-exchange`
- **GIVEN** an interaction-scoped client holding an opaque signaling-session ticket tagged `media_profile`
- **WHEN** `MediaCredentialProvider.fetch(ticket, mediaProfile)` is called
- **THEN** the Desk Media-IAM service validates the ticket and returns `MediaCredentials` (coturn TURN creds for `webrtc-p2p`)
- **AND** the adapter receives them via `setCredentials`

#### Scenario: RelayPoint never holds vendor secrets
- **id:** `clientsdk.creds.relaypoint-no-vendor-secrets`
- **GIVEN** the RelayPoint SDK surface
- **WHEN** credential minting is examined
- **THEN** RelayPoint defines only the `MediaCredentialProvider` port and issues the opaque ticket; the app implements minting and holds any vendor/SFU secrets

### Requirement: Cold transfer only in M1
The TS SDK MUST expose only **cold/blind transfer** in M1, on `InteractionHandle.transfer`
(transfer is interaction **re-routing**, not a media-call property), capability-gated by
`supportsWarmTransfer`. Warm/consultative and multiparty transfer MUST be deferred to the SFU
adapter. The consultative "talk to C before dropping A" flow MUST be achievable at the app
level (hold A, start a separate call B, blind-transfer A→B on B-accept) and MUST NOT be an SDK
transfer primitive. `InteractionHandle.transfer(target)` MUST resolve ONLY on
`interaction.transfer.accepted`; on `interaction.transfer.rejected`/`.cancelled`/`.failed`
(target reject/RONA/cancel/fail) it MUST reject with a typed error and the ORIGINAL call MUST be
retained (the SDK never assumes success on a non-accept).

#### Scenario: Non-accepted transfer rejects and retains the original call
- **id:** `clientsdk.transfer.non-accept-retains-original`
- **GIVEN** an `InteractionHandle.transfer(target)` in flight on a connected call
- **WHEN** the router emits `interaction.transfer.rejected`, `.cancelled`, or `.failed` (target reject/RONA/cancel/fail) instead of `interaction.transfer.accepted`
- **THEN** the `transfer(target)` promise rejects with a typed error and the original call is retained (not torn down), and the SDK does not mark the transfer successful

#### Scenario: Cold transfer is exposed on the interaction handle and gated
- **id:** `clientsdk.transfer.cold-only`
- **GIVEN** a connected call on a `webrtc-p2p` adapter
- **WHEN** the consumer calls `InteractionHandle.transfer(target)`
- **THEN** the SDK performs a cold/blind transfer (interaction re-route); warm transfer is unavailable because `supportsWarmTransfer=false`

#### Scenario: Consultative is composed at the app level
- **id:** `clientsdk.transfer.app-level-consult`
- **GIVEN** the documented consultative pattern
- **WHEN** Desk holds call A, starts a separate call B, and on B-accept calls `interactionA.transfer(B)` to move A→B
- **THEN** the SDK exposes no warm-transfer primitive and the consult is composed from `hold` + a separate call + `InteractionHandle.transfer`

### Requirement: Recording consent and retention facts with best-effort p2p capture
The SDK MUST expose a recording surface (a `RecordingController` on the `CallController`, or
equivalent methods) for `requestConsent()` / `grantConsent()` / `denyConsent()` / `start()` /
`stop()` and upload-status reporting. Each MUST flow as a **command → router `.log` fact**
(`recording.consent.requested/granted/denied`, `recording.started/stopped`,
`recording.upload.completed/failed` carrying retention-policy ref, recorder identity,
`object_ref`, failure reason) — vendor-agnostic, an M1 requirement; signaling-core owns the fact
vocabulary. Actual p2p capture MUST be a best-effort client-side `MediaRecorder` capability,
gated on `supportsLocalRecording` (true for `webrtc-p2p`), that the SDK EXPLICITLY labels NOT
compliance-grade. The SDK MUST NOT claim p2p client-side recording is compliance-grade.
Compliance-grade server/egress recording MUST be deferred to the SFU adapter
(`supportsServerRecording`, a flag DISTINCT from `supportsLocalRecording`).
The `RecordingController` MUST expose an observable `RecordingState`
(`idle | consent_pending | consent_denied | recording | stopped | failed`) as a readable
property and via `on("state", ...)`, mirroring the router's recording state-legality: `start()`
is legal only after `recording.consent.granted` (from `consent_pending`); after `denyConsent()`
(`consent_denied`) a `start()` MUST be refused. A best-effort capture or upload failure MUST
transition to `failed` and report `recording.upload.failed`.

#### Scenario: Recording state legality is mirrored by the controller
- **id:** `clientsdk.recording.state-legality`
- **GIVEN** a `RecordingController` whose `state` starts `idle`
- **WHEN** the consumer calls `start()` before consent is granted, or calls `start()` after `denyConsent()` drove `state` to `consent_denied`
- **THEN** the controller refuses the illegal `start()` with a typed error (mirroring the router's rejection) and only a `start()` after `grantConsent()` (`consent_pending → recording`) is admitted, with `state` + `on("state")` reporting the transitions

#### Scenario: Capture or upload failure transitions to failed
- **id:** `clientsdk.recording.capture-failure`
- **GIVEN** a `recording` state where best-effort `MediaRecorder` capture or the subsequent upload fails
- **WHEN** the failure is detected (or `reportUpload({ ok: false, failureReason })` is called)
- **THEN** the controller transitions `state` to `failed`, emits `on("state", "failed")`, and the router records `recording.upload.failed` carrying `failure_reason` (the SDK never silently claims success)

#### Scenario: Consent and retention recorded as router facts
- **id:** `clientsdk.recording.consent-retention-facts`
- **GIVEN** a recording flow on an interaction
- **WHEN** the consumer calls `requestConsent()` / `grantConsent()` / `denyConsent()` and `start()` / `stop()`
- **THEN** the SDK issues commands and the router writes the corresponding `.log` facts (`recording.consent.requested/granted/denied`, `recording.started/stopped`) carrying retention-policy ref and recorder identity

#### Scenario: Upload status reported as router facts
- **id:** `clientsdk.recording.upload-status-facts`
- **GIVEN** a stopped best-effort local recording whose bytes are uploaded
- **WHEN** the consumer reports the upload outcome via `reportUpload({ ok, objectRef?, failureReason? })`
- **THEN** the SDK issues a command and the router writes `recording.upload.completed` (with `object_ref`) or `recording.upload.failed` (with `failure_reason`)

#### Scenario: p2p capture is labeled not compliance-grade
- **id:** `clientsdk.recording.p2p-not-compliance-grade`
- **GIVEN** a `webrtc-p2p` adapter where `supportsLocalRecording=true` and `supportsServerRecording=false`
- **WHEN** the consumer uses the best-effort client-side `MediaRecorder` capture
- **THEN** the SDK labels it NOT compliance-grade (browser-tamperable, can lose data on tab crash before upload) and never claims otherwise

### Requirement: Go server SDK publishes offers, reads audit, mints tickets, never media
The Go server SDK (`relaypoint-go`) MUST let the Desk backend submit offers **to the router**
(which owns the ring + state machine), read `routing.audit.>`, and mint scoped tokens and
signaling-session tickets. `Router.PublishOffer` MUST submit the offer to the signaling-core
router; the server SDK MUST NOT publish `routing.offer.user.<target>` directly. The server SDK
MUST NEVER touch media (no `MediaAdapter`).

#### Scenario: Server SDK submits an offer to the router
- **id:** `clientsdk.go.publish-offer`
- **GIVEN** the Desk backend using `relaypoint-go`
- **WHEN** it calls `Router.PublishOffer` for a target user
- **THEN** the offer is submitted to the router, which rings `routing.offer.user.<target>` and owns the resulting state machine
- **AND** the server SDK does NOT publish the `routing.offer.user.<target>` ring subject directly

#### Scenario: Server SDK reads the audit stream
- **id:** `clientsdk.go.read-audit`
- **GIVEN** the Desk backend using `relaypoint-go`
- **WHEN** it reads `routing.audit.>`
- **THEN** it receives privileged-control audit facts (actor + reason) in order

#### Scenario: Server SDK mints a signaling-session ticket
- **id:** `clientsdk.go.mint-ticket`
- **GIVEN** an interaction-scoped user
- **WHEN** the backend calls `TokenMinter.MintSessionTicket`
- **THEN** it returns an opaque, short-lived, vendor-agnostic ticket tagged `media_profile`

#### Scenario: Server SDK has no media surface
- **id:** `clientsdk.go.no-media`
- **GIVEN** the `relaypoint-go` public surface
- **WHEN** a consumer looks for a media adapter or media negotiation
- **THEN** none exists; the server SDK never touches media

### Requirement: Distribution via GitHub Packages
Both SDKs MUST be distributed via a private **GitHub Packages** registry: `@relaypoint/client`
(npm) and `relaypoint-go` (Go module / GitHub-hosted). Consumers MUST authenticate to GitHub
Packages to install.

#### Scenario: Packages resolve from GitHub Packages
- **id:** `clientsdk.dist.github-packages`
- **GIVEN** a consumer authenticated to the private GitHub Packages registry
- **WHEN** it installs `@relaypoint/client` (npm) or imports `relaypoint-go`
- **THEN** the package resolves from GitHub Packages and installs under the source-available license

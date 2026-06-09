# Design: client-sdk

> Decisions + interface skeletons (the "Khung"). Implementation is deferred until a buildable
> `signaling-core` server exists. Full human narrative + C4 diagrams live in
> `docs/architecture/` (HTML); this file records decisions, not prose.

## Scope (locked)

Two design-first SDKs over the `signaling-core` contract, distributed via private **GitHub
Packages**:

- `@relaypoint/client` — TS, browser, framework-agnostic core over `nats.ws` (Desk web app +
  embeddable widget).
- `relaypoint-go` — Go **server** SDK for the Desk backend; NEVER touches media.

The SDK is a **client** of the router-authoritative protocol: it is ALWAYS a non-writer of
`.log` (write to `.log` is a trusted-server capability per `signaling-core`). It reuses the
`signaling-core` subjects and envelope verbatim; it does not redefine them.

## Key decisions (locked)

- **Core wraps the router-authoritative protocol so consumers never reimplement it.** The
  complex, race-prone client logic (token-refresh-reconnect, offer ring control, gap-replay,
  perfect-negotiation) lives once in `@relaypoint/client`, not in each consumer.
- **Connection lifecycle owns token refresh.** Connect via `nats.ws` with a token from a
  `getToken()` callback; auto-reconnect; on max-connection-lifetime / token-expiry, refresh
  the scoped token and reconnect transparently. After offer-accept, reconnect with the
  interaction-scoped token (the auth-callout authorizes at CONNECT time — `signaling.acl-after-accept`).
- **Optimistic-then-confirmed join.** The user is "joined" ONLY after the router's confirming
  ACL grant (the reconnect succeeds on the interaction-scoped token), never on the optimistic
  `accept()` click. A lost CAS (`accepted_elsewhere`/withdraw) rolls the optimistic UI back.
- **Command plane is write-only; `.log` is read-only.** The SDK publishes intents on
  `interaction.<id>.cmd` and NEVER writes `.log`. It consumes typed `.log` facts ordered by
  router `sequence`, dedups on `Nats-Msg-Id = event_id`, and on a sequence gap pauses live
  apply and replays from JetStream, then resumes. If the replay CANNOT fill the gap (JetStream
  unavailable), the SDK surfaces a typed degraded/fatal **delivery state** and retries with
  backoff — it never silently drops facts past the gap nor loops forever.
  - **Implementation note (chat subset).** signaling-core sets the JetStream `Nats-Msg-Id` to
    the *command*-derived publish-dedup id (`tenant.iid.command_id`, for exactly-once append),
    NOT to `event_id`. So the SDK dedups and orders by the authoritative router `sequence`
    (strictly monotonic per interaction) and treats `event_id` as the fact's stable identity. A
    re-delivered fact (same `sequence`) is dropped — the behaviour the `event_id` dedup scenario
    asserts — without depending on the transport header. Do not reintroduce header-based dedup.
- **Command idempotency (C4).** `send(command)` is a request on `interaction.<id>.cmd` via a
  reply `_INBOX`; it attaches a client-generated `command_id` and on retry reuses the same
  `command_id` so the router dedups (no double-append). It returns `Promise<CommandResult>`:
  resolves with the router's `CommandResult` on `accepted` (`causedBy = commandId`, correlating
  the resulting `.log` fact) and rejects with a typed error carrying `reason` on `rejected`. The
  `CommandResult` is an ephemeral ack/correlation; the authoritative effect is the `.log` fact.
- **Observable state machines (C2/C5).** Connection, call, and interaction state are READABLE +
  emitted (`RelayPointClient.state`, `CallController.state`, plus track/metadata events), with
  explicit failure flows (getToken/credential failure, refresh-before-expiry).
- **Media via a `MediaAdapter` port.** The `CallController` drives business call **facts** via
  commands (router writes `call.*` facts); the adapter executes the media side. The default
  `WebrtcP2pAdapter` implements the `webrtc-p2p` profile choreography. A vendor/SFU media
  adapter is a different `media_profile` — deferred, its own ADR.
- **Capability negotiation from day one.** The adapter declares `MediaCapabilities` flags; the
  controller degrades gracefully and NEVER silently pretends an unsupported capability.
- **Credentials via ticket exchange (B3).** RelayPoint issues a short-lived generic
  **signaling-session ticket**; the client exchanges it at a **Desk-owned Media-IAM service**
  for the real credentials. RelayPoint defines only the `MediaCredentialProvider` port and
  never holds vendor secrets; the **app (Desk) implements minting**. RelayPoint MAY ship a
  *reference* coturn TURN-cred minter for p2p only.
- **Transfer: cold/blind only in M1 (C1).** Warm/consultative + multiparty are deferred to the
  SFU adapter. The COLD-transfer lifecycle (`interaction.transfer.requested/accepted/rejected/
  cancelled/failed` + `interaction.transferred`) is normative in `signaling-core` now; warm/
  consult/multiparty remain conceptual/deferred. Only cold is exposed and it is capability-gated.
  `transfer(target)` resolves on `interaction.transfer.accepted`; reject/RONA/cancel/fail reject
  with a typed error and retain the original call.
- **Consultative is an app pattern, not an SDK primitive (C3).** "Talk to C before dropping A"
  = Desk puts call A on local hold, starts a separate call B, and on B-accept triggers a
  **blind transfer of A→B**. Documented as the recommended app pattern; NOT an SDK transfer call.
- **Recording: facts always, capture best-effort (D).** Consent/retention facts flow as
  commands → router `.log` facts (consent requested/granted/denied, recording started/stopped,
  retention-policy ref, recorder identity, upload status, failure) — M1, vendor-agnostic.
  Actual p2p capture is a best-effort client-side `MediaRecorder` capability, **EXPLICITLY
  labeled NOT compliance-grade** (browser-tamperable, can lose data on tab crash before upload).
  Compliance-grade server-side/egress recording is deferred to the SFU adapter
  (`supportsServerRecording`), a flag kept DISTINCT from `supportsLocalRecording` (the
  best-effort p2p capture, true for `webrtc-p2p`).

## Layering

```
+-----------------------------------------------------------+
|  Consumers: Desk web app, embeddable widget               |
|  (later, separate change: @relaypoint/react hooks)        |
+-----------------------------------------------------------+
|  @relaypoint/client  (TS, framework-agnostic core)        |
|                                                           |
|   RelayPointClient  -- connection lifecycle + getToken()  |
|     |-- OfferController   (ring / accept / reject / KV)   |
|     |-- InteractionHandle (.log stream + .send + .signal  |
|     |                      + transfer + metadata)          |
|     `-- CallController     start/hold/resume/stop + state  |
|             `-- RecordingController (consent/start/upload) |
|             |-- MediaAdapter (port) --------------------+  |
|             |     WebrtcP2pAdapter (webrtc-p2p profile) |  |
|             |     [vendor/SFU adapter: deferred, ADR]   |  |
|             `-- MediaCredentialProvider (port) ---------+  |
+-----------------------------------------------------------+
          | nats.ws (WebSocket)                |  ticket exchange
          v                                    v
+-------------------------+        +---------------------------+
|  signaling-core (NATS)  |        |  Desk Media-IAM service   |
|  router-authoritative   |        |  (app mints creds;        |
|  .cmd .log .signal      |        |   coturn TURN / SFU token)|
|  routing.offer/audit    |        +---------------------------+
+-------------------------+
          ^ relaypoint-go (server SDK): publish offer / read audit / mint ticket (NO media)
```

## Subjects consumed (from `signaling-core`, prefix `tenant.<tenantId>.`)

| SDK action | Subject | Role |
|---|---|---|
| Send command (intent) | `interaction.<id>.cmd` | client writes (write-only) |
| Read facts | `interaction.<id>.log` | client reads (NEVER writes) |
| Own ICE/typing | `interaction.<id>.signal.<self>` | client writes own author only |
| Read others' signal | `interaction.<id>.signal.*` | client reads |
| Receive ring | `routing.offer.user.<self>` | req/reply on `_INBOX`+nonce |
| Ring terminals | `routing.offer.user.<self>.control` | client reads ALL non-reply terminals (cancelled/withdrawn/accepted_elsewhere/timed_out_rona/expired/no_responder_fast_rona); `accepted`/`rejected` are reply-path |
| Reconstruct on reconnect | KV `offer.active.<self>` | client reads own pending offers |
| Audit (Go SDK) | `routing.audit.>` | server SDK reads |

Envelope (verbatim from `signaling-core`, snake_case wire form):
`{ schema, event_type, event_id, sequence, occurred_at, tenant_id, actor_id, medium, media_profile?, command_id?, caused_by?, ref_id?, data }`
(event-specific fields like `negotiation_id`/`object_ref`/`failure_reason` live inside `data`, not top-level).

## Wire-field naming (NORMATIVE)

The SDK public TS API is **camelCase**; the NATS wire/envelope is **snake_case**. The SDK's
`LogEvent`, `Command`, and `CommandResult` are a PRECISE camelCase **projection** of the wire
envelope — each projected field maps 1:1 to one snake_case wire field. The projection is precise,
not blanket-verbatim:

- **Envelope-level vs data-level.** `LogEvent` (a FACT) projects only the envelope fields that
  apply to facts: `schema`, `eventType`, `eventId`, `sequence`, `occurredAt`, `tenantId`,
  `actorId`, `medium`, `mediaProfile?`, `causedBy?`, `refId?`, `data`. It carries `causedBy` (the
  `command_id` that produced the fact) but NOT `commandId` (which is COMMAND-only). Event-specific
  fields — `negotiationId`, `objectRef`, `failureReason`, etc. — live INSIDE `data`, NOT as
  top-level `LogEvent` envelope fields.
- **`Command`** carries `commandId` (the idempotency key the SDK attaches).
- **`CommandResult`** = `{ commandId; status: "accepted" | "rejected"; causedBy?; reason? }` —
  the camelCase projection of the router's ephemeral reply `CommandResult { command_id, status,
  caused_by?, reason? }` (core NATS, never persisted; see `signaling-core`).

The wire form is authoritative; the table below is normative.

| TS (camelCase) | Wire (snake_case) |
|---|---|
| `commandId` | `command_id` |
| `causedBy` | `caused_by` |
| `eventType` | `event_type` |
| `eventId` | `event_id` |
| `occurredAt` | `occurred_at` |
| `actorId` | `actor_id` |
| `mediaProfile` | `media_profile` |
| `tenantId` | `tenant_id` |
| `refId` | `ref_id` |
| `negotiationId` | `negotiation_id` |
| `objectRef` | `object_ref` |
| `failureReason` | `failure_reason` |
| `contextPreview` | `context_preview` |

(`schema`, `sequence`, `medium`, `data` are single-word and identical on both sides.)

Which type each field appears on (precise projection, not blanket envelope):
`commandId` is on `Command` and `CommandResult` (NOT on `LogEvent`); `causedBy`/`reason`/`status`
on `CommandResult`; `causedBy`/`refId`/`mediaProfile` on `LogEvent`; `contextPreview` on `Offer`
(wire `context_preview`); and `negotiationId`/`objectRef`/`failureReason` are DATA-level — they
ride inside the wire envelope's `data`, not as top-level `LogEvent` envelope fields.

## TS interface skeletons — `@relaypoint/client`

```ts
export interface RelayPointClientOptions {
  servers: string[];                       // nats.ws WebSocket URLs
  selfUserId: string;
  tenantId: string;
  getToken: () => Promise<string>;         // scoped token; called on connect + refresh
}

export type ConnectionState =
  | "disconnected" | "connecting" | "connected" | "reconnecting" | "closed" | "failed";

export interface RelayPointClient {
  connect(): Promise<void>;
  close(): Promise<void>;
  readonly offers: OfferController;
  readonly state: ConnectionState;
  interaction(id: string): InteractionHandle;
  // emitted on max-lifetime / token-expiry: SDK refreshes via getToken() and reconnects
  on(event: "reconnecting" | "reconnected" | "disconnected", cb: () => void): void;
  // observable connection state machine; "failed" is terminal after getToken() retries exhaust (auth_failed)
  on(event: "state", cb: (s: ConnectionState) => void): void;
}

export interface Offer {
  readonly offerId: string;
  readonly tenantId: string;
  readonly from: string;
  readonly medium: string;                 // payload field, never a subject
  // opaque router-supplied preview of the interaction context (e.g. customer display name);
  // surfaces "incoming <medium> from X" pre-answer. SDK never parses it. NO media profile/vendor.
  readonly contextPreview?: unknown;       // <- maps to wire context_preview
  readonly expiresAt: number;
  // resolves to an InteractionHandle only after the router's confirming ACL grant
  accept(): Promise<InteractionHandle>;
  reject(reason?: string): Promise<void>;
}

export type OfferTerminal =
  | "accepted" | "rejected" | "cancelled" | "withdrawn"
  | "accepted_elsewhere" | "timed_out_rona" | "expired" | "no_responder_fast_rona";

export interface OfferController {
  // subscribes routing.offer.user.<self>; reconstructs from KV offer.active.<self> on reconnect
  on(event: "ring", cb: (offer: Offer) => void): void;
  // non-reply terminals pushed on ...control; also the client-local ring-timeout backstop
  on(event: "terminal", cb: (offerId: string, terminal: OfferTerminal) => void): void;
}

export interface LogEvent {
  readonly schema: string;                 // envelope-verbatim from signaling-core
  readonly tenantId: string;               // envelope-verbatim from signaling-core
  readonly eventType: string;
  readonly eventId: string;
  readonly sequence: number;               // router-assigned; client never sets it
  readonly occurredAt: string;
  readonly actorId: string;
  readonly medium: string;
  readonly mediaProfile?: string;
  readonly causedBy?: string;              // = the command_id that produced this fact (exactly-once correlation)
  readonly refId?: string;
  readonly data: unknown;
}

// A command intent published on interaction.<id>.cmd as a request carrying a reply _INBOX. The
// SDK attaches a client-generated command_id (idempotency key); on retry it REUSES the same
// command_id so the router dedups (no double-append) and the resulting fact carries
// caused_by = command_id. commandId is COMMAND-only; it is NOT projected onto LogEvent.
export interface Command {
  readonly type: string;
  readonly commandId: string;              // client-generated; stable across retries (idempotency)
  readonly data?: unknown;
}

// The router's ephemeral reply to a command (core NATS, never persisted). send(command) resolves
// with this on "accepted" (causedBy = commandId, correlating the resulting .log fact via its
// causedBy) and rejects with a typed error carrying `reason` on "rejected". The result is an
// ack/correlation — the authoritative effect is the .log fact, not this result.
export interface CommandResult {
  readonly commandId: string;
  readonly status: "accepted" | "rejected";
  readonly causedBy?: string;              // on accepted: = commandId (correlates the produced fact)
  readonly reason?: string;                // on rejected: e.g. "conflict", illegal-transition, ...
}

export interface SignalEvent {
  readonly type: string;                   // webrtc.ice | typing.* | ...
  readonly data?: unknown;
}

export interface InteractionHandle {
  readonly id: string;
  // ordered by router sequence; deduped on Nats-Msg-Id=event_id; gap -> pause + JetStream replay
  events(): AsyncIterable<LogEvent>;
  // request on interaction.<id>.cmd via reply _INBOX; resolves with the router's CommandResult on
  // "accepted", rejects with a typed error (carrying reason) on "rejected". NEVER writes .log.
  send(command: Command): Promise<CommandResult>;
  signal(s: SignalEvent): Promise<void>;   // -> interaction.<id>.signal.<self> (own author only)
  // cold/blind transfer = interaction RE-ROUTING (routing, not a media-call property);
  // capability-gated (supportsWarmTransfer=false); warm/multiparty deferred to the SFU adapter.
  // Resolves ONLY on interaction.transfer.accepted; rejects with a typed error on
  // reject/RONA/cancel/fail, and the original call is retained.
  transfer(target: string): Promise<void>;
  // latest opaque context (metadata) from interaction.context.updated facts; SDK never parses it
  readonly metadata: unknown;
  on(event: "metadata", cb: (context: unknown) => void): void;
  readonly call: CallController;
}

// Mirrors signaling-core's 1:1 call state machine.
export type CallState =
  | "idle" | "setup_offered" | "answered" | "ice_connecting" | "connected"
  | "renegotiating" | "held" | "transferring" | "reconnecting"
  | "cancelled" | "ended" | "media_failed" | "setup_failed";

export interface CallController {
  // audio-only by default; { video: true } starts a video call
  start(opts?: { audio?: boolean; video?: boolean }): Promise<void>;
  hold(): Promise<void>;
  resume(): Promise<void>;
  stop(): Promise<void>;
  // mute/unmute the local mic
  setMicEnabled(on: boolean): Promise<void>;
  // toggle camera; turning it on during an audio call upgrades audio->video via renegotiation
  setCameraEnabled(on: boolean): Promise<void>;
  readonly state: CallState;
  on(event: "state", cb: (s: CallState) => void): void;
  // exposes local + remote media tracks so the UI can render/attach them
  on(event: "track", cb: (track: MediaStreamTrack, kind: "audio" | "video", origin: "local" | "remote") => void): void;
  readonly capabilities: MediaCapabilities; // from the active MediaAdapter
  readonly recording: RecordingController;
}

// Recording consent + lifecycle flow as commands -> router .log facts (signaling-core owns the
// fact vocabulary). p2p capture stays best-effort client-side MediaRecorder and is NOT
// compliance-grade. Each method issues a command; outcomes surface as facts on the .log stream.
// Observable recording state machine. start() is legal only from consent_pending after the
// router's recording.consent.granted (deny -> consent_denied blocks start); a capture/upload
// failure surfaces as `failed`. Mirrors the router's recording state-legality enforcement.
export type RecordingState =
  | "idle" | "consent_pending" | "consent_denied" | "recording" | "stopped" | "failed";

export interface RecordingController {
  requestConsent(): Promise<void>;         // -> recording.consent.requested (idle -> consent_pending)
  grantConsent(): Promise<void>;           // -> recording.consent.granted
  denyConsent(): Promise<void>;            // -> recording.consent.denied (-> consent_denied)
  // start legal only after consent granted; gated by supportsLocalRecording / supportsServerRecording
  start(): Promise<void>;                  // -> recording.started (-> recording)
  stop(): Promise<void>;                   // -> recording.stopped (-> stopped)
  // report upload outcome of best-effort local capture -> recording.upload.completed/failed facts
  reportUpload(result: { ok: boolean; objectRef?: string; failureReason?: string }): Promise<void>;
  readonly state: RecordingState;
  on(event: "state", cb: (s: RecordingState) => void): void;
}

export interface MediaCapabilities {
  readonly mediaProfile: string;           // Phase-1: "webrtc-p2p"
  readonly supportsWarmTransfer: boolean;  // false for webrtc-p2p
  readonly supportsMultiparty: boolean;    // false for webrtc-p2p
  // best-effort client-side MediaRecorder capture (true for webrtc-p2p) — NOT compliance-grade
  readonly supportsLocalRecording: boolean;
  // compliance-grade server/egress recording (false for webrtc-p2p) — deferred to the SFU adapter
  readonly supportsServerRecording: boolean;
}

// The media side. WebrtcP2pAdapter implements webrtc-p2p (perfect-negotiation/glare,
// deterministic polite/impolite role, ICE-buffered-until-SDP, renegotiation generation).
// A vendor/SFU adapter is a different media_profile (deferred, its own ADR).
export interface MediaAdapter {
  readonly capabilities: MediaCapabilities;
  // negotiation descriptor stays opaque to core; tagged by media_profile
  createOffer(opts?: { audio?: boolean; video?: boolean }): Promise<{ descriptor: unknown; mediaProfile: string }>;
  createAnswer(remote: unknown, mediaProfile: string): Promise<{ descriptor: unknown; mediaProfile: string }>;
  applyRemote(descriptor: unknown, mediaProfile: string): Promise<void>;
  // re-offer carrying a negotiationId + monotonic generation (e.g. audio->video upgrade);
  // negotiationId maps to wire negotiation_id; stale (lower-generation) signaling discarded
  createRenegotiationOffer(negotiationId: string, generation: number, opts?: { audio?: boolean; video?: boolean }): Promise<{ descriptor: unknown; mediaProfile: string; negotiationId: string; generation: number }>;
  applyRenegotiation(descriptor: unknown, mediaProfile: string, negotiationId: string, generation: number): Promise<void>;
  addIceCandidate(candidate: unknown): void;   // buffered until matching SDP applied
  onLocalIce(cb: (candidate: unknown) => void): void;
  // surfaces local + remote media tracks for the UI to render
  onRemoteTrack(cb: (track: MediaStreamTrack, kind: "audio" | "video") => void): void;
  setCredentials(creds: MediaCredentials): void;
  // best-effort, NOT compliance-grade; present when supportsLocalRecording (NOT supportsServerRecording)
  startLocalRecording?(): Promise<void>;
  stopLocalRecording?(): Promise<void>;
  close(): Promise<void>;
}

export interface MediaCredentials {
  readonly mediaProfile: string;
  readonly iceServers?: unknown;           // ephemeral coturn TURN creds (p2p)
  readonly roomToken?: unknown;            // a future SFU room token (deferred)
  readonly expiresAt: number;
}

// RelayPoint issues an opaque signaling-session ticket; the app (Desk) Media-IAM service
// exchanges it for real creds. RelayPoint NEVER holds vendor secrets — it only defines this port.
export interface MediaCredentialProvider {
  fetch(ticket: string, mediaProfile: string): Promise<MediaCredentials>;
}
```

## Go interface skeletons — `relaypoint-go` (server SDK, NO media)

```go
package relaypoint

// Router submits offers to the signaling-core router and drives server-side intents. It never
// touches media.
type Router interface {
    // PublishOffer submits the offer to the signaling-core ROUTER, which rings
    // routing.offer.user.<target> and owns the ring + offer state machine. The server SDK does
    // NOT publish routing.offer.user.<target> directly.
    PublishOffer(ctx context.Context, o Offer) (offerID string, err error)
}

type Offer struct {
    TenantID string
    Target   string
    Medium   string // payload field, never a subject
    Data     any
}

// AuditReader consumes routing.audit.> (JetStream) for privileged-control audit facts.
type AuditReader interface {
    Read(ctx context.Context, tenantID string, fn func(AuditEvent) error) error
}

type AuditEvent struct {
    EventID    string
    Sequence   uint64
    OccurredAt time.Time
    Actor      string
    Reason     string
    Data       any
}

// TokenMinter mints the scoped tokens / signaling-session tickets the auth-callout consumes.
// The ticket is opaque and vendor-agnostic; the app's Media-IAM service exchanges it for creds.
type TokenMinter interface {
    MintConnectToken(ctx context.Context, c TokenClaims) (token string, err error)
    MintSessionTicket(ctx context.Context, c TicketClaims) (ticket string, err error)
}

type TokenClaims struct {
    TenantID      string
    UserID        string
    InteractionID string // empty until offer-accept; then scopes interaction.<id>.>
    ExpiresIn     time.Duration
}

type TicketClaims struct {
    TenantID      string
    UserID        string
    InteractionID string
    MediaProfile  string // Phase-1: "webrtc-p2p"
    ExpiresIn     time.Duration
}
```

## Credential flow (B3 ticket exchange)

1. On offer-accept the client holds an interaction-scoped connection (per `signaling-core`).
2. `relaypoint-go` `TokenMinter.MintSessionTicket` issues an opaque, short-lived
   **signaling-session ticket** (vendor-agnostic; tagged `media_profile`).
3. The client's `MediaCredentialProvider.fetch(ticket, mediaProfile)` calls the **Desk-owned
   Media-IAM service**, which validates the ticket and mints the real creds: ephemeral coturn
   TURN creds for `webrtc-p2p` (a vendor/SFU room token for a future profile).
4. The adapter receives `MediaCredentials` via `setCredentials`. RelayPoint never sees vendor
   secrets. RelayPoint MAY ship a *reference* coturn TURN-cred minter for p2p only.
5. **Failure / refresh (C5):** if `fetch` fails (Media-IAM down) the SDK raises a typed error and
   the call transitions to `setup_failed` (pre-connect) or `media_failed` (post-connect). The SDK
   monitors `MediaCredentials.expiresAt` and proactively re-`fetch`es before expiry so a long call
   does not drop on TURN-cred expiry.
6. **Expired / invalid ticket:** on an expired/invalid signaling-session ticket the Media-IAM
   service rejects it and `fetch` fails with a typed error. The SDK then requests a FRESH ticket
   (via the `relaypoint-go` `TokenMinter.MintSessionTicket` path) and retries the `fetch`; if
   recovery is unrecoverable the call transitions to `setup_failed`/`media_failed`.

## Transfer (C1 + C3)

- **`transfer` lives on `InteractionHandle`, not `CallController`.** Transfer is interaction
  **re-routing** (a routing operation), not a media-call property; placing it on the handle
  matches `signaling-core`, where the router offers a new target and re-grants ACLs.
- **M1 exposes cold/blind transfer only**, capability-gated (`supportsWarmTransfer=false`).
- **`transfer(target)` resolves only on `interaction.transfer.accepted`.** On
  `interaction.transfer.rejected`/`.cancelled`/`.failed` (target reject/RONA/cancel/fail) it
  REJECTS with a typed error and the ORIGINAL call is retained (the SDK never assumes success or
  tears down the original leg on a non-accept).
- **Consultative is an app pattern (C3), not an SDK call:** Desk puts call A on local hold via
  `CallController.hold()`, starts an entirely separate call B, and on B-accept calls
  `interactionA.transfer(B)` to blind-transfer A→B. This composes existing primitives; the SDK
  exposes no warm-transfer primitive. Warm/consultative + multiparty are deferred to the SFU adapter.
- The COLD-transfer fact lifecycle (`interaction.transfer.requested/accepted/rejected/cancelled/
  failed` + `interaction.transferred`) is normative in `signaling-core`; warm/consult/multiparty
  stay conceptual/deferred so the call-control surface is stable when the SFU adapter lands.

## Recording (D)

- Consent + retention are **facts**: a `RecordingController` (on `CallController`) issues
  commands → router `.log` facts (consent requested/granted/denied, recording started/stopped,
  retention-policy ref, recorder identity, upload status, failure). Vendor-agnostic; an M1
  requirement. signaling-core owns the fact vocabulary; the SDK only issues commands + observes facts.
- **Two distinct capability flags:** `supportsLocalRecording` (best-effort client-side
  `MediaRecorder`, **true** for `webrtc-p2p`) is SEPARATE from `supportsServerRecording`
  (compliance-grade server/egress, **false** for `webrtc-p2p`). The adapter's
  `startLocalRecording?`/`stopLocalRecording?` are present when `supportsLocalRecording` (NOT
  gated on `supportsServerRecording`).
- p2p capture is **best-effort** and **NOT compliance-grade** (browser-tamperable; can lose data
  on a tab crash before upload). Never claimed otherwise. Compliance-grade server/egress
  recording is deferred to the SFU adapter (`supportsServerRecording`). Upload outcome is
  reported via `RecordingController.reportUpload` → `recording.upload.completed/failed` facts.
- **Observable `RecordingState`** (`idle | consent_pending | consent_denied | recording |
  stopped | failed`), readable + `on("state")`. Legal order mirrors the router's state-legality:
  `start()` is rejected unless consent was granted (`consent_pending` reached
  `recording.consent.granted`); after `denyConsent()` (`consent_denied`) a `start()` is refused.
  A best-effort `MediaRecorder` capture or upload failure transitions to `failed` and reports
  `recording.upload.failed`; `reportUpload` retries are the app's responsibility and idempotent
  on the router via `command_id`.

## State machines + failure flows (C2/C5)

- **ConnectionState** `disconnected → connecting → connected → reconnecting → { closed | failed }`,
  observable via `RelayPointClient.state` + `on("state")`. **`getToken()` failure:** the SDK
  retries with backoff; once retries are exhausted it emits a fatal `auth_failed`, transitions
  to `failed`, and stays disconnected (it does not silently loop forever).
- **CallState** mirrors signaling-core's 1:1 call machine
  (`idle | setup_offered | answered | ice_connecting | connected | renegotiating | held |
  transferring | reconnecting | cancelled | ended | media_failed | setup_failed`), observable
  via `CallController.state` + `on("state")`. Local + remote tracks surface via `on("track")`
  so the UI can render audio/video. `setMicEnabled` mutes; `setCameraEnabled(true)` during an
  audio call upgrades audio→video via a renegotiation carrying a `negotiationId` (wire
  `negotiation_id`) + monotonic generation.
- **Credential failure / refresh (C5):** `MediaCredentialProvider.fetch` failure (Media-IAM
  down) surfaces as a typed error; depending on timing the call transitions to `setup_failed`
  (before connect) or `media_failed` (after connect). `MediaCredentials.expiresAt` is monitored;
  the SDK proactively refreshes BEFORE expiry by issuing a new `fetch`, so long calls do not
  drop on TURN-cred expiry.

## Time authority (clock-skew immunity)

- The SDK orders `.log` facts strictly by router `sequence` (and discards stale renegotiation by
  `generation`); `occurredAt` is **display-only** and NEVER used for staleness, ordering, dedup,
  or any security decision — mirroring signaling-core's time-authority invariant.
- Credential/ticket refresh is driven by a **server-issued relative TTL** (or a derived
  server-clock offset from server responses), not the local wall-clock. This ties to
  `clientsdk.creds.refresh-before-expiry` (proactive re-`fetch` before expiry) and
  `clientsdk.creds.ticket-expired-invalid` (fresh-ticket retry): a skewed client clock must not
  refresh too early/late nor wrongly treat a still-valid ticket as expired — the server's
  rejection is authoritative.

## Interaction metadata (C3)

- The interaction ALWAYS carries opaque **context** (customer info, integration data, enriched
  over time). The SDK surfaces it as `InteractionHandle.metadata` (latest) + `on("metadata")`,
  fed by `interaction.context.updated` `.log` facts. The SDK NEVER parses the context — it is an
  opaque blob Desk populates and RelayPoint orders.

## Command idempotency (C4)

- Every `send(command)` is a request on `interaction.<id>.cmd` via a reply `_INBOX` and attaches
  a client-generated `command_id`. On retry the SDK REUSES the same `command_id` so the router
  dedups (no double-append). `send` returns `Promise<CommandResult>` ({ `commandId`, `status`,
  `causedBy?`, `reason?` }): it resolves with the router's `CommandResult` on `accepted`
  (`causedBy = commandId`, correlating the resulting `caused_by = command_id` `.log` fact) and
  rejects with a typed error carrying `reason` on `rejected`. A reused `command_id` with a
  divergent payload comes back `rejected`/`conflict` (per `signaling-core`). The result is an
  ephemeral ack/correlation — never persisted; the authoritative effect is the `.log` fact.

## Distribution: GitHub Packages

Both packages publish to a **private GitHub Packages registry**: `@relaypoint/client` (npm)
and `relaypoint-go` (Go module / GitHub-hosted). Consumers authenticate to GitHub Packages to
install. Source-available licensing per `LICENSE` applies.

## Out of scope (later phases / own changes)

`@relaypoint/react` hooks; mobile SDK; the **vendor/SFU media adapter** (a future
`media_profile`, its own ADR); **warm/consultative + multiparty** transfer; **compliance-grade
server/egress recording**.

# Tasks: client-sdk

> **Implementation deferred until the `signaling-core` server is buildable.** This is a
> design-first change: the work below is authored now but NOT executed until a server exists
> to test against. No SDK code, package, or build files are produced by this change.
>
> Verifiable via code, tests, docs, or CI. Issue numbers added once synced to the board. Each
> behavioral task is tagged with the scenario id its future test must carry.

## Connection lifecycle (TS)
- [ ] `RelayPointClient.connect()` via `nats.ws` using `getToken()` — `// @spec:clientsdk.connection.connect-with-token`
- [ ] Max-lifetime / token-expiry → refresh via `getToken()` + transparent reconnect — `// @spec:clientsdk.connection.token-refresh-reconnect`
- [ ] Reconnect with the interaction-scoped token after accept (auth-callout authorizes at CONNECT) — `// @spec:clientsdk.connection.reconnect-interaction-scoped`
- [ ] Observable `ConnectionState` via `state` + `on("state")` — `// @spec:clientsdk.connection.state-observable`
- [ ] `getToken()` failure → backoff retries → fatal `auth_failed` + `failed`, stays disconnected — `// @spec:clientsdk.connection.gettoken-failure`

## Offer / ring controller (TS)
- [ ] Subscribe `routing.offer.user.<self>`; surface `ring`; `accept()` replies on `_INBOX`+nonce — `// @spec:clientsdk.offer.ring-accept`
- [ ] Surface `Offer.medium` + opaque `Offer.contextPreview?` (wire `context_preview`, never parsed) so UI shows "incoming <medium> from X" pre-answer; no media profile/vendor on the offer — `// @spec:clientsdk.offer.medium-context-preview`
- [ ] `reject()` replies on the inbox; no interaction joined — `// @spec:clientsdk.offer.reject`
- [ ] Handle EVERY non-reply terminal on `...control` (cancelled/withdrawn/accepted_elsewhere/timed_out_rona/expired/no_responder_fast_rona); `expired` distinct from `timed_out_rona` — `// @spec:clientsdk.offer.control-terminal`
- [ ] Reconstruct own pending offers from KV `offer.active.<self>` on reconnect + client-local ring-timeout backstop — `// @spec:clientsdk.offer.kv-reconstruct`
- [ ] Optimistic-then-confirmed join: roll back when no confirming ACL grant arrives — `// @spec:clientsdk.offer.optimistic-confirmed`

## Command plane / delivery (TS)
- [ ] `send(command)` → `interaction.<id>.cmd`; never writes `.log` — `// @spec:clientsdk.cmd.send-to-cmd`
- [ ] No public log-write path; `sequence` is router-assigned — `// @spec:clientsdk.cmd.no-log-write`
- [ ] `send` attaches `command_id`; retry reuses same `command_id` (router dedups); correlate via the resolved `CommandResult` + `causedBy` fact / typed error on rejection — `// @spec:clientsdk.cmd.idempotent-retry`
- [ ] `send(command)` is a req/reply on `interaction.<id>.cmd` via `_INBOX` returning `Promise<CommandResult>`: resolves on `accepted` (correlates the fact via `causedBy = commandId`), rejects with a typed error carrying `reason` on `rejected` — `// @spec:clientsdk.cmd.result-correlation`
- [ ] Surface router's concurrent interaction-command rejection (second transfer while transferring / duplicate recording-start) as a typed error rejected from `send`'s `CommandResult`, never assume success — `// @spec:clientsdk.handle.concurrent-command-guard`
- [ ] `LogEvent`/`Command`/`CommandResult` are a PRECISE camelCase projection (LogEvent carries `causedBy` not `commandId`; `negotiationId`/`objectRef`/`failureReason` live inside `data`); 1:1 normative field mapping — `// @spec:clientsdk.cmd.wire-field-mapping`
- [ ] Deliver `.log` facts ordered by router `sequence` — `// @spec:clientsdk.delivery.ordered-by-sequence`
- [ ] Dedup on `Nats-Msg-Id = event_id` — `// @spec:clientsdk.delivery.dedup-event-id`
- [ ] Sequence-gap → pause live apply + JetStream replay + resume — `// @spec:clientsdk.delivery.gap-replay`
- [ ] Replay-failure (JetStream unavailable) → typed degraded/fatal delivery state + backoff; never silently drop facts or loop forever — `// @spec:clientsdk.delivery.replay-failure`

## Time authority (TS)
- [ ] Order `.log` strictly by router `sequence`; treat `occurredAt` as display-only, never for staleness/ordering/dedup/security — `// @spec:clientsdk.time.occurred-at-display-only`
- [ ] Credential/ticket refresh driven by server-issued relative TTL / derived server-clock offset, not local wall-clock (ties to refresh-before-expiry / ticket-expired-invalid) — `// @spec:clientsdk.time.relative-ttl-refresh`

## Interaction handle (TS)
- [ ] `client.interaction(id)` exposes `events()` stream + `send(command)` — `// @spec:clientsdk.handle.stream-and-send`
- [ ] Publish own ICE/typing only on `interaction.<id>.signal.<self>` — `// @spec:clientsdk.handle.signal-own-author`
- [ ] `InteractionHandle.metadata` + `on("metadata")` fed by `interaction.context.updated` facts (opaque, never parsed) — `// @spec:clientsdk.handle.metadata-observable`

## Call controller + MediaAdapter (TS)
- [ ] `CallController` start/hold/resume/stop drive facts via commands; controller writes no `.log` — `// @spec:clientsdk.call.facts-via-commands`
- [ ] Descriptor stays opaque to core, tagged `media_profile` — `// @spec:clientsdk.call.opaque-descriptor`
- [ ] `start({ audio?, video? })` selects media kind; adapter `createOffer({audio?,video?})` — `// @spec:clientsdk.call.media-kind`
- [ ] Readable `state: CallState` + `on("state")` mirroring signaling-core call machine — `// @spec:clientsdk.call.state-observable`
- [ ] Expose local + remote tracks via `on("track", (track, kind, origin))`; adapter `onRemoteTrack` — `// @spec:clientsdk.call.track-exposure`
- [ ] `setMicEnabled` mutes; `setCameraEnabled(true)` upgrades audio→video via renegotiation generation — `// @spec:clientsdk.call.toggle-mic-camera`
- [ ] MediaAdapter `createAnswer(remote, mediaProfile)` + renegotiation carrying `negotiationId` (wire `negotiation_id`) + generation — `// @spec:clientsdk.webrtcp2p.renegotiation-generation`
- [ ] `WebrtcP2pAdapter` glare / perfect-negotiation (deterministic polite/impolite) — `// @spec:clientsdk.webrtcp2p.glare-perfect-negotiation`
- [ ] `WebrtcP2pAdapter` buffers ICE until matching SDP applied — `// @spec:clientsdk.webrtcp2p.ice-buffered-until-sdp`
- [ ] `WebrtcP2pAdapter` discards stale renegotiation generation — `// @spec:clientsdk.webrtcp2p.renegotiation-generation`

## Capability model (TS)
- [ ] `MediaCapabilities` flags: warm-transfer/multiparty/server-recording = false, `supportsLocalRecording` = true for p2p (distinct from server-recording) — `// @spec:clientsdk.capability.declared-flags`
- [ ] Controller refuses unsupported capability with a typed error; never silently fakes it — `// @spec:clientsdk.capability.no-silent-pretend`

## Credentials (TS + port)
- [ ] `MediaCredentialProvider.fetch(ticket, mediaProfile)` exchanges the ticket at the Desk Media-IAM service; adapter `setCredentials` — `// @spec:clientsdk.creds.ticket-exchange`
- [ ] RelayPoint defines only the port + issues the opaque ticket; app mints; no vendor secrets in RelayPoint — `// @spec:clientsdk.creds.relaypoint-no-vendor-secrets`
- [ ] `fetch` failure (Media-IAM down) → typed error → call `setup_failed`/`media_failed` per timing — `// @spec:clientsdk.creds.fetch-failure`
- [ ] Monitor `expiresAt`; proactively re-`fetch` before expiry + `setCredentials` — `// @spec:clientsdk.creds.refresh-before-expiry`
- [ ] Expired/invalid ticket → typed `fetch` error → request fresh ticket (relaypoint-go minter) + retry; unrecoverable → `setup_failed`/`media_failed` — `// @spec:clientsdk.creds.ticket-expired-invalid`
- [ ] (Optional, p2p only) reference coturn TURN-cred minter — no vendor/SFU secrets

## Transfer (TS)
- [ ] Expose cold/blind `InteractionHandle.transfer(target)` (re-routing) gated by `supportsWarmTransfer` — `// @spec:clientsdk.transfer.cold-only`
- [ ] `transfer(target)` resolves only on `interaction.transfer.accepted`; on reject/RONA/cancel/fail rejects with a typed error and retains the original call — `// @spec:clientsdk.transfer.non-accept-retains-original`
- [ ] Document app-level consult pattern (hold A + separate call B + `InteractionHandle.transfer` A→B); no warm primitive — `// @spec:clientsdk.transfer.app-level-consult`
- [ ] Cold-transfer lifecycle facts (`interaction.transfer.requested/accepted/rejected/cancelled/failed` + `interaction.transferred`) are core-normative; warm/consult/multiparty stay conceptual/deferred

## Recording (TS)
- [ ] `RecordingController` (on `CallController`): `requestConsent/grantConsent/denyConsent/start/stop` → commands → router consent/started/stopped facts — `// @spec:clientsdk.recording.consent-retention-facts`
- [ ] `reportUpload({ok, objectRef?, failureReason?})` → `recording.upload.completed/failed` facts — `// @spec:clientsdk.recording.upload-status-facts`
- [ ] Best-effort client-side `MediaRecorder` capture gated on `supportsLocalRecording`, EXPLICITLY labeled NOT compliance-grade — `// @spec:clientsdk.recording.p2p-not-compliance-grade`
- [ ] Observable `RecordingState` (idle/consent_pending/consent_denied/recording/stopped/failed): start only after consent granted; deny blocks start — `// @spec:clientsdk.recording.state-legality`
- [ ] Capture/upload failure → `failed` state + `recording.upload.failed`; never silently claim success — `// @spec:clientsdk.recording.capture-failure`

## Go server SDK (`relaypoint-go`)
- [ ] `Router.PublishOffer` rings `routing.offer.user.<target>`; router owns the state machine — `// @spec:clientsdk.go.publish-offer`
- [ ] `AuditReader` reads `routing.audit.>` (actor + reason, ordered) — `// @spec:clientsdk.go.read-audit`
- [ ] `TokenMinter.MintSessionTicket` returns an opaque, short-lived, vendor-agnostic ticket tagged `media_profile` — `// @spec:clientsdk.go.mint-ticket`
- [ ] No media surface on the server SDK — `// @spec:clientsdk.go.no-media`

## Packaging / distribution
- [ ] Publish `@relaypoint/client` (npm) + `relaypoint-go` to the private GitHub Packages registry under the source-available license — `// @spec:clientsdk.dist.github-packages`

## Docs
- [ ] Architecture doc (HTML, `docs/architecture/`) for the SDK layering + ports + credential flow
- [ ] ADR for the `MediaCredentialProvider` ticket-exchange (B3) boundary
- [ ] ADR placeholder for the future vendor/SFU media adapter (`media_profile`)

## Validation (Definition of Done)
- [ ] `openspec validate client-sdk --strict` passes
- [ ] Every `#### Scenario:` carries a `// @spec:<id>` test (when implementation lands)
- [ ] lint/typecheck/test/coverage green (when implementation lands)
- [ ] Independent cross-review recorded (builder ≠ reviewer)

## Deferred (own changes / ADRs)
- `@relaypoint/react` hooks bindings — later, separate change.
- Mobile SDK — deferred to the MQTT/mobile phase.
- The vendor/SFU media adapter — a future `media_profile`, its own ADR.
- Warm / consultative + multiparty transfer — deferred to the SFU adapter.
- Compliance-grade server/egress recording (`supportsServerRecording`) — deferred to the SFU adapter.

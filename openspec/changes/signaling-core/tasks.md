# Tasks: signaling-core

> **S3.1 chat-subset implemented (desk board relaypoint#2):** NATS single-node config
> (JetStream/websocket/mqtt/$SYS + router/client ACLs), the router/interaction service
> (sole `.log` writer: validate → assign `sequence` → append; command_id idempotency +
> conflict; payload-tenant-match; illegal-transition; durable replayable `.log`; client
> ACL-denied from `.log`), and `deploy/docker-compose.yml` (nats + coturn + router).
> 9 scenarios green vs live NATS (`internal/signaling/*_test.go`, `// @spec:` tagged).
> Deferred to later stories: forged-author (needs auth-callout), signal-ephemeral
> rate-limit, offer/call/WebRTC lifecycle, presence, notifications.


> Verifiable via config, code, tests, or CI. Issue numbers added once synced to the board.
> Each behavioral task is tagged with the scenario id its test must carry.

## NATS server (single node)
- [ ] `nats-server.conf`: JetStream; `websocket{}`; `mqtt{}` (3.1.1); system account exporting `$SYS`; auth callout; KV bucket for offer state
- [ ] Tenant-scoped subject ACLs: clients READ-only on `.log`, WRITE-only on `.cmd`, subscribe-only on `presence.*`
- [ ] `deploy/docker-compose.yml`: nats + coturn (+ surveyor/exporter)
- [ ] Web client connects via `nats.ws` — `// @spec:signaling.nats-ws-connect`
- [ ] MQTT listener accepts connections (mobile-ready, unused) — `// @spec:signaling.mqtt-listener-ready`

## Router / interaction service (authoritative writer + state-machine owner)
- [ ] Service skeleton (NATS micro): sole writer of `interaction.<id>.log`; clients denied `.log` write — `// @spec:signaling.cmd.log-write-only-router`
- [ ] Command handler validates tenant/actor/role/state/author, assigns monotonic `sequence`, appends fact — `// @spec:signaling.cmd.router-assigns-sequence`
- [ ] Reject command whose payload `actor_id` != connection identity — `// @spec:signaling.cmd.forged-author-rejected`
- [ ] Reject illegal state transitions — `// @spec:signaling.cmd.illegal-transition-rejected`
- [ ] Reject payload `tenant_id` != subject tenant even when ACL passes — `// @spec:signaling.security.payload-tenant-match`

## Unified interaction
- [ ] Event envelope `{ schema, event_type, event_id, sequence, occurred_at, tenant_id, actor_id, medium, media_profile?, command_id?, caused_by?, ref_id?, data }`
- [ ] Chat + call share `interaction.<id>.log`/`.cmd`/`.signal.<userId>`; medium in payload — `// @spec:signaling.unified-interaction`
- [ ] Command idempotency: commands carry client-generated `command_id`; router dedups (no double-append), stamps fact `caused_by = command_id`, returns rejection result with `command_id`+reason — `// @spec:signaling.cmd.idempotent-command-id`
- [ ] Command result transport: command is a req/reply on `interaction.<id>.cmd` via a reply `_INBOX`; router replies ephemeral `CommandResult{command_id,status,caused_by?,reason?}` (accepted→`caused_by` references the `.log` fact; rejected/illegal/forged/conflict→`reason`) to the issuer's inbox ONLY (core NATS, never JetStream, no leak to other users); authoritative effect stays the `.log` fact — `// @spec:signaling.cmd.result-transport`
- [ ] `command_id` conflict: identical-payload retry replays the original `CommandResult` (idempotent, no second fact); SAME `command_id` with a DIFFERENT payload rejected as `conflict` (key bound to its original request) — `// @spec:signaling.cmd.command-id-conflict`
- [ ] Serialize interaction-level commands via state-guard/CAS: second `transfer.requested` while `transferring` rejected; `recording.started` while recording idempotent/rejected — `// @spec:signaling.cmd.concurrent-interaction-guard`
- [ ] `interaction.context.updated` records opaque Desk-supplied `context` (router never parses), ordered + replayable — `// @spec:signaling.interaction.context-updated`

## Offer lifecycle (full)
- [ ] Offer state machine + ring on `routing.offer.user.<userId>` (req/reply via `_INBOX`+nonce); accept — `// @spec:signaling.offer.accept`
- [ ] Ring payload carries `medium` + opaque `context_preview?` (router-supplied projection of `context`, never parsed); media engine/`media_profile` NOT in the offer (bound only at media-setup) — `// @spec:signaling.offer.medium-context-preview`
- [ ] Reject + no-answer RONA terminal + requeue — `// @spec:signaling.offer.reject-and-rona`
- [ ] Offer-TTL `expired` (before ring delivered/accepted) distinct from `timed_out_rona` — `// @spec:signaling.offer.expired-vs-rona`
- [ ] Fast-RONA on NATS `503 no responders` (offline/never-subscribed) — `// @spec:signaling.offer.no-responder-fast-rona`
- [ ] Double-accept CAS on `offer_id`/`route_attempt_id`; losers `accepted_elsewhere`; idempotent re-accept — `// @spec:signaling.offer.double-accept-cas`
- [ ] Cancel (originator) / withdraw (router) push terminal on `...user.<target>.control`; others denied — `// @spec:signaling.offer.cancel-withdraw-authorized`
- [ ] Accept vs withdraw crossing in flight → same CAS, one terminal; late accept `accepted_elsewhere`/409, no phantom join — `// @spec:signaling.offer.accept-withdraw-cross`
- [ ] KV `offer.active.<userId>` reconstruct ringing on reconnect + client-local ring backstop — `// @spec:signaling.offer.reconnect-during-ring`

## Interaction QoS split
- [ ] `interaction.<id>.log` on JetStream, ordered, durable/replayable; router-assigned `sequence` — `// @spec:signaling.log-durable`
- [ ] `interaction.<id>.signal.<userId>` (subscribers read `.signal.*`) on core NATS, never JetStream (ICE/typing) — `// @spec:signaling.signal-ephemeral`
- [ ] Media stays WebRTC P2P; only SDP/ICE on NATS — `// @spec:signaling.media-bypass-broker`
- [ ] Media descriptor stored opaque (router never parses SDP) + `media_profile` discriminator — `// @spec:signaling.media-descriptor-opaque`

## 1:1 call / WebRTC lifecycle
- [ ] Call state machine setup→answer→connect — `// @spec:signaling.call.setup-connect`
- [ ] Glare resolved by perfect-negotiation (caller=impolite / lexical tie-break) — `// @spec:signaling.call.glare-perfect-negotiation`
- [ ] Buffer ICE until matching SDP applied (SDP on `.log`, ICE on `.signal`) — `// @spec:signaling.call.ice-buffered-until-sdp`
- [ ] Renegotiation/ICE-restart with `negotiation_id`/generation; discard stale — `// @spec:signaling.call.renegotiation-generation`
- [ ] Hold/resume (SDP direction changes) — `// @spec:signaling.call.hold-resume`
- [ ] Cold/blind transfer (M1): re-route interaction — offer new target, on accept grant new leg + revoke old (no warm overlap), emit `interaction.transfer.accepted`+`interaction.transferred`; warm/multiparty deferred to SFU — `// @spec:signaling.call.transfer`
- [ ] Transfer non-accept (reject/RONA/cancel/fail) retains the ORIGINAL leg; no `interaction.transferred` — `// @spec:signaling.call.transfer-non-accept`
- [ ] Transfer handover ordering: grant new leg's ACL FIRST then revoke old (new-active-before-old-revoked, no media gap) — `// @spec:signaling.call.transfer-leg-handover`
- [ ] Setup-cancel before connect; reject late SDP/ICE — `// @spec:signaling.call.setup-cancel`
- [ ] Media failure → reconnecting grace → fallback ICE → `media_failed`; coturn-down fallback — `// @spec:signaling.call.media-failed-fallback`

## Recording facts (capture is profile-specific, NOT core)
- [ ] Consent lifecycle facts `recording.consent.requested/granted/denied` + `recording.started/stopped` (carry `retention_policy`/`recorder_id`) — `// @spec:signaling.recording.consent-facts`
- [ ] Upload-status facts `recording.upload.completed` (carry `object_ref`) / `recording.upload.failed` (carry `failure_reason`) — `// @spec:signaling.recording.upload-status-facts`
- [ ] Recording state legality: start requires consent.granted; denied blocks start; stop/upload valid only for a started recording; retried start/stop idempotent — `// @spec:signaling.recording.state-legality`

## Interaction lifecycle
- [ ] Enumerated state machine `new→routing→active→{transferring}→ended` (abandoned pre/post-assign; offline/left active sub-states); admit only the legal transition set, reject the rest — `// @spec:signaling.interaction.state-machine`
- [ ] Explicit state machine rejects invalid transitions (no resume after ended) — `// @spec:signaling.interaction.invalid-transition`
- [ ] `interaction.abandoned` withdraws ringing offers — `// @spec:signaling.interaction.abandoned-withdraws-offers`
- [ ] Orphaned reaper: all offline > N min → `interaction.ended[orphaned]` — `// @spec:signaling.interaction.orphaned-reaper`
- [ ] `participant.offline` (transient) vs `participant.left` (permanent) — `// @spec:signaling.interaction.offline-vs-left`

## Delivery / ordering / idempotency
- [ ] `Nats-Msg-Id = event_id` dedup + client-side dedup beyond window — `// @spec:signaling.delivery.msgid-dedup`
- [ ] `message.updated/deleted` carry `ref_id`; redaction vs tombstone — `// @spec:signaling.delivery.ref-id-update-delete`
- [ ] Gap detection pauses live apply + replays from JetStream — `// @spec:signaling.delivery.gap-replay`

## Time authority
- [ ] Order/apply strictly by router `sequence`; `occurred_at` is display-only and never flips ordering/staleness/dedup/security — `// @spec:signaling.time.occurred-at-informational`
- [ ] Token/ticket/credential expiry enforced via server-issued relative TTL / server-authoritative timer; skewed client clock does not bypass nor prematurely trigger expiry — `// @spec:signaling.time.relative-ttl-expiry`

## Failure modes
- [ ] Max NATS connection lifetime / kill-on-expiry + client refresh+reconnect — `// @spec:signaling.failure.token-expiry-max-lifetime`
- [ ] Presence debounce (~5s) + session/device counts avoid false RONA — `// @spec:signaling.failure.presence-debounce`
- [ ] RONA penalty-box / backoff suspends offers — `// @spec:signaling.failure.rona-penalty-box`
- [ ] Router crash recovery via KV + TTL sweeper + idempotent terminals — `// @spec:signaling.failure.router-crash-recovery`

## Presence + notify
- [ ] Presence service `$SYS.ACCOUNT.*.{CONNECT,DISCONNECT}` → `presence.<userId>` — `// @spec:signaling.presence-from-sys`
- [ ] Clients cannot publish presence (presence service is sole publisher) — `// @spec:signaling.presence-publish-restricted`
- [ ] Durable `notify.<userId>` survives reconnect — `// @spec:signaling.notify-durable`

## Security / tenancy
- [ ] Tenant-prefixed subjects + per-tenant ACLs; cross-tenant denied — `// @spec:signaling.tenant-isolation`
- [ ] Auth-callout interaction grant scoped to accepted interaction only — `// @spec:signaling.acl-interaction-scoped`
- [ ] On accept: short-lived scoped token + reconnect grants `interaction.<id>.>` — `// @spec:signaling.acl-after-accept`
- [ ] Rate-limit `.signal` per user/interaction + ICE-candidate cap per negotiation — `// @spec:signaling.security.signal-rate-limit`
- [ ] Audit privileged controls (cancel/withdraw/transfer/grant-revoke) with actor + reason — `// @spec:signaling.security.privileged-audit`

## JetStream streams
- [ ] `INTERACTION_LOGS` (`tenant.*.interaction.*.log`), `NOTIFICATIONS` (`tenant.*.notify.*`), `ROUTING_AUDIT` (`tenant.*.routing.audit.>`); none for `.cmd`/`.signal`/presence/offer/control

## NAT traversal
- [ ] coturn (STUN/TURN) deployed; client uses it as ICE server; fallback ICE servers configured

## Docs (HTML) + verification
- [ ] `docs/architecture/` C4 + subject-model + state-machine docs (HTML, via docs-writer)
- [ ] `openspec validate signaling-core --strict`
- [ ] Tests for every scenario id; lint/typecheck/test green
- [ ] Independent cross-review recorded

## Deferred (own changes/ADRs)
- [ ] Multi-party / conference calls
- [ ] Mobile via MQTT bridge
- [ ] NKEY/JWT auth
- [ ] 3-node JetStream RAFT HA cluster + router HA
- [ ] SFU/media-server

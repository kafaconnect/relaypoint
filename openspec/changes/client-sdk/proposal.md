# Change: client-sdk

## From

`signaling-core` defines the Phase-1 router-authoritative protocol — the subject layout,
the event envelope, the offer/call/interaction state machines, and the security boundary —
but it ships no client. Every consumer (the Desk web app, the embeddable widget, the Desk
backend) would have to reimplement the hard, race-prone client half of that contract by hand:
the `nats.ws` connection + token-refresh-reconnect dance, the offer ring/accept/control
choreography with `_INBOX`+nonce and KV reconstruction, the optimistic-then-confirmed join
rule, ordered `.log` consumption with `Nats-Msg-Id` dedup and gap-replay, and the entire
`webrtc-p2p` perfect-negotiation/glare/ICE-buffer choreography that lives client-side. Each
reimplementation reintroduces the exact races the router-authoritative design eliminated.

## To

Two **design-first** SDKs over the `signaling-core` contract, distributed via a private
**GitHub Packages** registry:

- **`@relaypoint/client`** — a TypeScript, browser, framework-agnostic core over `nats.ws`,
  consumed by the Desk web app and the embeddable widget. It encapsulates the connection
  lifecycle, the offer/ring controller, the write-only command plane + ordered `.log`
  delivery, the per-agent fan-out feed consumer introduced by ADR-0003, a typed interaction handle,
  and a `CallController` driven through a `MediaAdapter`
  port. The default `WebrtcP2pAdapter` implements the `webrtc-p2p` media profile. Media
  credentials are obtained through a `MediaCredentialProvider` port via a ticket exchange —
  RelayPoint never holds vendor secrets.
- **`relaypoint-go`** — a Go **server** SDK for the Desk backend: publish offers, read
  `routing.audit.>`, mint scoped tokens and media-session tickets. The server SDK NEVER
  touches media (no `MediaAdapter`).

This change authors the proposal, the locked design + interface skeletons (the "Khung"), the
behavior specs, and a deferred task checklist. No SDK code is written; implementation follows
a buildable `signaling-core` server.

## Reason

The protocol is router-authoritative precisely so a single trusted writer owns the races.
That guarantee only holds if clients also speak the protocol correctly — and the client half
(reconnect-with-new-token, ring control terminals, gap-replay, perfect-negotiation) is
complex enough that ad-hoc reimplementation is where the races would creep back in. A shared
SDK makes the correct client behavior the default and the only path consumers see. Defining
the **port abstractions** (`MediaAdapter`, `MediaCapabilities`, `MediaCredentialProvider`)
and the full **transfer / recording fact vocabulary** now — even though the SFU adapter,
warm transfer, and compliance-grade recording are deferred — is the cheap moment: these are
expensive to retrofit because they shape the core's call-control and audit surfaces.

## Impact

- New design-first change defining `@relaypoint/client` (TS) + `relaypoint-go` (Go) SDKs and
  the `MediaAdapter` / `MediaCapabilities` / `MediaCredentialProvider` ports.
- Consumes the current `signaling-core` / agent-feed subjects/envelope: `interaction.<id>.cmd.<self>`
  (SDK writes), `agent.<self>.feed.>` (SDK inbox reads), `interaction.<id>.log` (narrow per-interaction
  handle reads only when authorized), `interaction.<id>.signal.<self>`
  (SDK writes own ICE/typing), `routing.offer.user.<self>(.control)`, `routing.audit.>` (Go
  SDK reads), KV `offer.active.<self>` (SDK reconstructs).
- Establishes the credential boundary: RelayPoint issues an opaque **signaling-session ticket**;
  the **app (Desk)** implements minting at a Desk-owned Media-IAM service. RelayPoint MAY ship
  a *reference* coturn TURN-cred minter for p2p only and never holds vendor/SFU secrets.
- Locks the capability-negotiation model and the transfer + recording fact vocabulary as core
  concepts so a future SFU adapter and warm transfer drop in without reshaping core.
- Distribution: both packages publish to a private GitHub Packages registry.

## Non-goals

- **No implementation now** — this change is design-only; SDK code waits on a buildable server.
- **`@relaypoint/react`** hooks layer — a later, separate concern (mentioned as after).
- **Mobile SDK** — deferred to the MQTT/mobile phase.
- **The vendor/SFU media adapter** — a future `media_profile`, its own ADR; named generically
  here as "a vendor/SFU media adapter (deferred, its own ADR)".
- **Warm / consultative transfer and multiparty** — deferred to the SFU adapter; M1 exposes
  only cold/blind transfer, and consultative is an app-level pattern, not an SDK primitive.
- **Compliance-grade recording** — deferred to the SFU adapter (`supportsServerRecording`);
  p2p client-side capture is best-effort and explicitly NOT compliance-grade.

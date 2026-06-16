// Package authcallout is the RelayPoint NATS auth-callout responder: it verifies a connection's
// token, derives the authenticated Identity, and mints that connection's identity-pinned NATS
// permissions (openspec change agent-feed-fanout, Decisions 1/2b/4/9). The grant policy (this file)
// carries no NATS/jwt types so it is unit-testable and stays the only spec of the ACLs; the
// responder adapter is the only place that touches the wire encoding (loose-coupling HARD RULE).
package authcallout

import (
	"fmt"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

type Grant struct {
	PubAllow []string
	PubDeny  []string
	SubAllow []string
	SubDeny  []string
	// AllowResponses, when true, grants a NATS dynamic response permission (publish once to a received
	// message's reply subject). Agent/trusted-backend connections do request/reply, so they keep it; a
	// VISITOR is strictly subscribe-only — without it the static PubDeny `>` cannot be bypassed by an
	// inbound message carrying a `reply` (cross-review: response perms otherwise re-open a publish path).
	AllowResponses bool
}

// GrantsFor mints the per-connection permissions for an authenticated Identity; self is the ACL-pinned
// `<self>` suffix and conn the responder-minted reply-inbox prefix. An AGENT connection may publish
// ONLY `…interaction.*.cmd.<self>` and subscribe ONLY its own feed/offer/notify/presence + its minted
// `_INBOX_<conn>.>` — no `.log`, no feed publish, no JetStream API, no tenant-wide read. A TRUSTED
// BACKEND additionally reads tenant-wide logs/routing and may drive the JetStream API.
func GrantsFor(id signaling.Identity, conn string) (Grant, error) {
	if err := validSubjectToken(id.TenantID); err != nil {
		return Grant{}, fmt.Errorf("authcallout: invalid tenant: %w", err)
	}
	if err := validSubjectToken(id.UserID); err != nil {
		return Grant{}, fmt.Errorf("authcallout: invalid user: %w", err)
	}
	if conn == "" {
		return Grant{}, fmt.Errorf("authcallout: empty connection id")
	}
	t, self := id.TenantID, id.UserID
	inbox := "_INBOX_" + conn + ".>"

	switch signaling.RoleOf(id) {
	case signaling.RoleVisitor:
		// A widget visitor reads exactly ONE conversation: its interaction `.log` (the SDK chat slice,
		// rp1-web-feed-consumer) + the transitional events plane. cid is mint-bound (== interaction id,
		// ADR-0009). NO $JS.API: a LIVE core subscribe can't read the shared INTERACTION_LOGS stream
		// wholesale; the per-subject ACL confines reach. Other conversations/feeds are absent ⇒ denied.
		cid := id.ConversationID
		if err := validSubjectToken(cid); err != nil {
			return Grant{}, fmt.Errorf("authcallout: invalid conversation: %w", err)
		}
		return Grant{
			PubAllow: nil,
			PubDeny:  []string{">"}, // a visitor publishes NOTHING
			SubAllow: []string{
				"tenant." + t + ".interaction." + cid + ".log",
				"tenant." + t + ".conversation." + cid + ".events",
				inbox,
			},
			// No `tenant.*.interaction.*.log` deny: NATS deny outranks allow, so it would shadow the
			// literal `.log` allow above (same rule as events).
			SubDeny: []string{
				"_INBOX.>",
				"tenant.*.agent.*.feed.>",
			},
		}, nil
	case signaling.RoleTrustedBackend:
		return Grant{
			PubAllow: []string{
				"tenant." + t + ".interaction.*.cmd." + self,
				inbox,
				"$JS.API.>",
			},
			PubDeny: []string{"tenant.*.interaction.*.log"},
			SubAllow: []string{
				"tenant." + t + ".interaction.*.log",
				"tenant." + t + ".notify.>",
				"tenant." + t + ".routing.>",
				inbox,
			},
			SubDeny:        []string{"_INBOX.>"},
			AllowResponses: true,
		}, nil
	default: // RoleAgent
		// No $JS.API grant: a broad $JS.API.CONSUMER.> would let the agent pull-read raw .log or
		// another agent's feed through the consumer API, bypassing the subject denies below; the
		// browser reads its own feed via a core subscribe only (A4).
		return Grant{
			PubAllow: []string{
				"tenant." + t + ".interaction.*.cmd." + self,
				// Presence/typing hints, identity-pinned to <self>: the agent publishes its OWN
				// presence state (presence.<self>.state) + per-conversation typing
				// (presence.<self>.typing.<cid>) and CANNOT forge another identity's (the `<self>`
				// segment is ACL-fixed). Without this the desk console floods the bus with Publish
				// Violations on every keystroke (F1 grant gap).
				"tenant." + t + ".presence." + self + ".>",
				inbox,
			},
			PubDeny: []string{
				"tenant.*.interaction.*.log",
				"tenant.*.agent.*.feed.>",
			},
			SubAllow: []string{
				"tenant." + t + ".agent." + self + ".feed.>",
				"tenant." + t + ".routing.offer.user." + self,
				"tenant." + t + ".routing.offer.user." + self + ".control",
				"tenant." + t + ".notify." + self,
				// The tenant presence roster + per-conversation typing of OTHER participants
				// (presence.*.state / presence.*.typing.<cid>). Tenant-scoped (the `tenant.<t>`
				// prefix is ACL-fixed), non-sensitive hints. Replaces the prior own-only
				// `presence.<self>` literal, which never matched the `presence.*.…` the console reads.
				"tenant." + t + ".presence.*.>",
				inbox,
			},
			SubDeny: []string{
				"_INBOX.>",
				"tenant.*.interaction.*.log",
			},
			AllowResponses: true,
		}, nil
	}
}

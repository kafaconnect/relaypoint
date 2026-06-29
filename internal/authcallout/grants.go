// Package authcallout is RelayPoint's NATS auth-callout responder: it verifies a connection token and mints that identity's pinned NATS permissions (change agent-feed-fanout).
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
	// AllowResponses grants the NATS dynamic response perm (one publish to a msg's reply subject); a VISITOR must NOT get it or an inbound reply re-opens a publish path past PubDeny (cross-review).
	AllowResponses bool
}

// conn is the responder-minted reply-inbox prefix (not client-chosen); self is the ACL-pinned `<self>` suffix interpolated into every grant.
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
		// A visitor reads exactly ONE conversation: its interaction `.log` + the transitional events plane; cid is mint-bound (== interaction id, ADR-0009). No $JS.API confines reach to the per-subject ACL.
		cid := id.ConversationID
		if err := validSubjectToken(cid); err != nil {
			return Grant{}, fmt.Errorf("authcallout: invalid conversation: %w", err)
		}
		return Grant{
			PubAllow: nil,
			PubDeny:  []string{">"},
			SubAllow: []string{
				"tenant." + t + ".interaction." + cid + ".log",
				"tenant." + t + ".conversation." + cid + ".events",
				inbox,
			},
			// No `tenant.*.interaction.*.log` deny: NATS deny outranks allow, so it would shadow the literal `.log` allow above.
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
	default:
		// No $JS.API grant: a broad $JS.API.CONSUMER.> would let the agent pull-read raw .log or another feed past the subject denies; the browser uses a core subscribe (A4).
		return Grant{
			PubAllow: []string{
				"tenant." + t + ".interaction.*.cmd." + self,
				// Presence/typing pinned to <self> (the agent can't forge another identity); without it the console floods Publish Violations on every keystroke (F1 grant gap).
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
				// Tenant presence roster + others' per-conversation typing (presence.*.…); replaces the prior own-only `presence.<self>` literal that never matched what the console reads.
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

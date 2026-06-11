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
			SubDeny: []string{"_INBOX.>"},
		}, nil
	default: // RoleAgent
		// No $JS.API grant: a broad $JS.API.CONSUMER.> would let the agent pull-read raw .log or
		// another agent's feed through the consumer API, bypassing the subject denies below; the
		// browser reads its own feed via a core subscribe only (A4).
		return Grant{
			PubAllow: []string{
				"tenant." + t + ".interaction.*.cmd." + self,
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
				"tenant." + t + ".presence." + self,
				inbox,
			},
			SubDeny: []string{
				"_INBOX.>",
				"tenant.*.interaction.*.log",
			},
		}, nil
	}
}

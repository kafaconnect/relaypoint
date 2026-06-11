// Package authcallout is the RelayPoint NATS auth-callout responder: it verifies a connection's
// presented token, derives the authenticated Identity, and mints that connection's per-connection,
// identity-pinned NATS permissions. It replaces the shared-`client` dev user, making the
// `.cmd.<self>` / `feed.<self>` / `_INBOX_<conn>` suffix-ACLs airtight (openspec change
// agent-feed-fanout, Decisions 1/2b/4/9). The grant POLICY (this file) is pure and depends on no
// NATS client; the responder adapter maps a Grant onto the wire permission type.
package authcallout

import (
	"fmt"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// Grant is the policy output: the allow/deny subject sets for one connection. It is a plain value
// (no NATS/jwt types) so the policy is unit-testable without any client and the responder is the
// only place that touches the wire encoding (loose-coupling HARD RULE).
type Grant struct {
	PubAllow []string
	PubDeny  []string
	SubAllow []string
	SubDeny  []string
}

// GrantsFor mints the per-connection permissions for an authenticated Identity. self is the
// authenticated user (the ACL-pinned `<self>` suffix); conn is the high-entropy per-connection
// token the responder minted for this connection's reply-inbox.
//
// An AGENT inbox connection (Decisions 1/4/9):
//   - subscribe ONLY its own feed `tenant.<tid>.agent.<self>.feed.>`;
//   - publish ONLY `tenant.<tid>.interaction.*.cmd.<self>` (wildcard interaction, FIXED <self>
//     suffix) — `*.cmd.<other>` is denied because no other suffix is allowed;
//   - subscribe+publish ONLY its minted `_INBOX_<conn>.>` reply prefix; broad `_INBOX.>` denied;
//   - read its own offer/notify/presence;
//   - NO `.log` subscribe, NO feed publish, NO tenant-wide read.
//
// A TRUSTED-BACKEND (desk-svc) identity gets the privileged-command publish (any `…cmd.<self>`
// across interactions to land participation facts) plus the broader service grants it needs.
func GrantsFor(id signaling.Identity, conn string) (Grant, error) {
	if id.TenantID == "" || id.UserID == "" {
		return Grant{}, fmt.Errorf("authcallout: identity missing tenant/user")
	}
	if conn == "" {
		return Grant{}, fmt.Errorf("authcallout: empty connection id")
	}
	t, self := id.TenantID, id.UserID
	inbox := "_INBOX_" + conn + ".>"

	switch signaling.RoleOf(id) {
	case signaling.RoleTrustedBackend:
		return Grant{
			// Desk lands participation facts as a privileged command on ANY interaction, pinned to
			// its own identity suffix; it also reads facts/notify across the tenant for routing.
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
	default: // RoleAgent — the locked-down inbox connection
		return Grant{
			PubAllow: []string{
				"tenant." + t + ".interaction.*.cmd." + self,
				inbox,
				"$JS.API.CONSUMER.>",
			},
			// Only the explicit `…cmd.<self>` allow stands; any other suffix (`…cmd.<other>`) is
			// already denied by NATS default-deny — a deny rule here would also match <self> and
			// (deny-wins) block the agent's own command.
			PubDeny: []string{
				"tenant.*.interaction.*.log", // never forge a fact
				"tenant.*.agent.*.feed.>",    // never publish a feed (server-write only)
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
				"_INBOX.>",                   // no broad reply-inbox snooping
				"tenant.*.interaction.*.log", // no raw conversation-log read
			},
		}, nil
	}
}

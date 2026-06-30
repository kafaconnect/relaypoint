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

	// Switch on the raw role, not signaling.RoleOf: RoleOf's empty→agent default is the strict direction for the router's gates but fail-OPEN here (it would mint agent perms for an unknown/empty role); the grant layer authorizes nothing it does not explicitly recognise (RH-08).
	switch id.Role {
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
				// Least-privilege JS.API (was account-wide `$JS.API.>`): only the consumer lifecycle + stream info on the one stream it reads `.log` from, never account-wide JS admin (other streams, KV, stream create/delete/purge) (RH-08).
				"$JS.API.STREAM.INFO." + signaling.LogStreamName,
				"$JS.API.CONSUMER.CREATE." + signaling.LogStreamName + ".>",
				"$JS.API.CONSUMER.DURABLE.CREATE." + signaling.LogStreamName + ".>",
				"$JS.API.CONSUMER.INFO." + signaling.LogStreamName + ".>",
				"$JS.API.CONSUMER.MSG.NEXT." + signaling.LogStreamName + ".>",
				// nats.go deletes its ephemeral consumers on Unsubscribe/Drain; without DELETE the backend leaks consumers + hits permission errors. Scoped to the one stream, never account-wide (RH-08).
				"$JS.API.CONSUMER.DELETE." + signaling.LogStreamName + ".>",
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
	case signaling.RoleAgent:
		return Grant{
			PubAllow: []string{
				"tenant." + t + ".interaction.*.cmd." + self,
				// Presence/typing pinned to <self> (the agent can't forge another identity); scoped to the documented state + per-conversation typing subjects, not a tail-wildcard (RH-08).
				"tenant." + t + ".presence." + self + ".state",
				"tenant." + t + ".presence." + self + ".typing.>",
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
				// Tenant presence roster + others' per-conversation typing, scoped to state + typing (not the broader `presence.*.>` tail-wildcard) (RH-08).
				"tenant." + t + ".presence.*.state",
				"tenant." + t + ".presence.*.typing.>",
				inbox,
			},
			SubDeny: []string{
				"_INBOX.>",
				"tenant.*.interaction.*.log",
			},
			AllowResponses: true,
		}, nil
	default:
		// Fail closed: an unknown or empty role authorizes nothing (was a fall-through agent grant — fail-open) (RH-08).
		return Grant{}, fmt.Errorf("authcallout: role %q authorizes no grant", id.Role)
	}
}

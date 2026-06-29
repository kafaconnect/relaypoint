package projector

import (
	"context"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// Core depends ONLY on these ports, never on nats.JetStreamContext, so the fold logic is unit-testable with in-memory fakes (loose-coupling HARD RULE; Decision 7).

// StreamSeq is the JetStream cursor unit (snapshot/fold order), distinct from Event.Sequence (the dense per-interaction router sequence); iid comes from the delivery subject (the Event has none).
type Fact struct {
	Event     *signaling.Event
	iid       string
	StreamSeq uint64
	// Seeds the publish ctx so the fanned-out feed message stays on the inbound trace (F5b continuity).
	traceparent string
	// opaque delivery handle (e.g. *nats.Msg) as `any` so the core imports no transport type
	msg any
}

func NewFact(e *signaling.Event, iid string, streamSeq uint64) Fact {
	return Fact{Event: e, iid: iid, StreamSeq: streamSeq}
}

func (f Fact) Iid() string { return f.iid }

func (f Fact) Traceparent() string { return f.traceparent }

// Serial consumer (MaxAckPending=1): exactly ONE fact is in flight — the core fully processes a Fact before the next Deliver returns.
type LogSource interface {
	// The same un-acked fact is redelivered after Nak / on takeover.
	Deliver(ctx context.Context) (Fact, error)
	// Ack is called ONLY after all intended feed publishes succeed; Nak triggers backoff redelivery.
	Ack(f Fact) error
	Nak(f Fact) error
	InProgress(f Fact) error
	Delivered(f Fact) int
	AckFloor(ctx context.Context) (uint64, error)
	// Read-only fold of (lo, hi] WITHOUT acking — hydration rebuilds the view from a snapshot up to the ack floor.
	FoldRange(ctx context.Context, lo, hi uint64) ([]Fact, error)
}

// Deterministic dedup id gives at-most-once per (agent, interaction, sequence); Publish MUST return only after the publish is acked.
type FeedSink interface {
	Publish(ctx context.Context, tenant, agent, iid, dedupID string, payload []byte) error
	Dlq(ctx context.Context, tenant, reason string, eventID string, seq int64) error
}

// Leader lease: one active worker holds it, standbys block in Acquire.
type LeaseStore interface {
	Acquire(ctx context.Context) error
	Renew(ctx context.Context) error
	Release(ctx context.Context) error
}

// The stored snapshot is ALWAYS an acked prefix (seq <= durable ack floor): Save is called only after the matching Ack.
type SnapshotStore interface {
	Load(ctx context.Context, maxSeq uint64) (*Snapshot, uint64, error)
	Save(ctx context.Context, seq uint64, s *Snapshot) error
}

// Tenant-shared fan-out recipient set (M1: every agent sees every interaction); a port, not desk's HTTP, for loose coupling; a transient error Naks the fact (redelivery), so a roster outage never drops one.
type Roster interface {
	Agents(ctx context.Context, tenantID string) ([]string, error)
}

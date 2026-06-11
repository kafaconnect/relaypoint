package projector

import (
	"context"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// The projector core depends ONLY on the ports below — never on nats.JetStreamContext — so the
// fold/project/revoke/hydrate logic is unit-testable with in-memory fakes (loose-coupling HARD
// RULE). NATS is the Phase-1 adapter behind each port (nats.go). See openspec change
// agent-feed-fanout, Decision 7.

// Fact is one delivered `.log` message: the decoded Event, the interaction id parsed from the
// delivery subject (the Event envelope carries no interaction id), and the JetStream stream
// sequence the durable consumer assigns it (the cursor unit — distinct from Event.Sequence, the
// dense per-interaction router sequence). The projector folds in StreamSeq order and snapshots by it.
type Fact struct {
	Event     *signaling.Event
	iid       string
	StreamSeq uint64
	// msg is the adapter's opaque delivery handle (e.g. *nats.Msg), kept as `any` so the core never
	// imports a transport type (loose coupling). The adapter type-asserts it in Ack/Nak/Delivered.
	msg any
}

// NewFact constructs a Fact (the adapter parses the interaction id from the delivery subject).
// Exposed so unit-test fakes build facts the same way the live adapter does.
func NewFact(e *signaling.Event, iid string, streamSeq uint64) Fact {
	return Fact{Event: e, iid: iid, StreamSeq: streamSeq}
}

// Iid is the interaction id this fact belongs to (from the delivery subject).
func (f Fact) Iid() string { return f.iid }

// LogSource is the durable, serial (MaxAckPending=1) consumer of `tenant.*.interaction.*.log`.
// Exactly ONE fact is in flight at a time: the core fully processes a Fact (fold + fan-out +
// Ack/Nak/Dlq) before the next Deliver returns. AckFloor is the durable recovery cursor.
type LogSource interface {
	// Deliver blocks until the next un-acked fact is available (the single in-flight delivery), or
	// the context is cancelled. The same un-acked fact is redelivered after Nak / on takeover.
	Deliver(ctx context.Context) (Fact, error)
	// Ack advances the durable cursor past this fact (called ONLY after all intended feed publishes
	// succeed). Nak triggers redelivery with backoff. Delivered reports the current delivery count
	// (1 on first delivery) so the core can DLQ past max_deliver.
	Ack(f Fact) error
	Nak(f Fact) error
	Delivered(f Fact) int
	// AckFloor is the durable consumer's ack floor — the recovery cursor. Hydration loads the
	// snapshot at/below it and read-only-folds (snapshot_seq, AckFloor].
	AckFloor(ctx context.Context) (uint64, error)
	// FoldRange read-only-folds the tail (lo, hi] (by stream sequence) WITHOUT acking — used by
	// hydration to rebuild the participation view from a snapshot up to the ack floor.
	FoldRange(ctx context.Context, lo, hi uint64) ([]Fact, error)
}

// FeedSink publishes a projection (a verbatim Event copy, or a FeedControl tombstone) into ONE
// agent's feed subject with a deterministic dedup id — the at-most-once guarantee per
// (agent, interaction, sequence). It MUST return only after the publish is acknowledged.
type FeedSink interface {
	// Publish writes `payload` to tenant.<tid>.agent.<aid>.feed.<iid> with Nats-Msg-Id=dedupID.
	Publish(ctx context.Context, tenant, agent, iid, dedupID string, payload []byte) error
	// Dlq routes a poison fact to tenant.<tid>.agent.dlq.feed with the failure reason + source ids.
	Dlq(ctx context.Context, tenant, reason string, eventID string, seq int64) error
}

// LeaseStore is the NATS KV leader lease: one active worker holds it; standbys wait. Acquire is the
// blocking contention loop; Renew heartbeats; Release relinquishes on shutdown.
type LeaseStore interface {
	Acquire(ctx context.Context) error
	Renew(ctx context.Context) error
	Release(ctx context.Context) error
}

// SnapshotStore persists the participation view keyed by stream sequence. The stored snapshot is
// ALWAYS an acked prefix (seq <= durable ack floor): Save is called only after the matching Ack.
type SnapshotStore interface {
	// Load returns the latest snapshot whose Seq <= maxSeq, or (nil, 0, nil) if none.
	Load(ctx context.Context, maxSeq uint64) (*Snapshot, uint64, error)
	Save(ctx context.Context, seq uint64, s *Snapshot) error
}

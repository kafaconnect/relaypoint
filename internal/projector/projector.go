// Package projector is the Participation/Fan-out service core: a leased single-active worker that
// tails the canonical `tenant.*.interaction.*.log`, folds participation, and projects every fact
// into the feed of each currently-participating agent — effectively-once (at-least-once delivery +
// idempotent feed publish). It depends only on owned ports (ports.go); NATS is the adapter
// (nats.go). See openspec change agent-feed-fanout, Decisions 3/6/7/8.
package projector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/signaling"
)

const (
	feedControlSchema = signaling.SchemaV1
	controlRevoked    = "feed.revoked"
)

// Snapshot is the participation view serialized by stream sequence — an ACKED-PREFIX state (its
// Seq <= the durable ack floor when stored). On takeover the worker loads the latest snapshot at/
// below the ack floor, then read-only-folds (snapshot_seq, ack_floor] to go live.
type Snapshot struct {
	// interaction id -> agent -> its membership intervals in .log order.
	Intervals map[string]map[string][]signaling.Interval
}

// state is the in-memory participation across all interactions; the serial MaxAckPending=1 discipline
// protects this fold from concurrent mutation.
type state struct {
	views map[string]*signaling.ParticipationView // keyed by interaction id
}

func newState() *state { return &state{views: map[string]*signaling.ParticipationView{}} }

func (s *state) view(iid string) *signaling.ParticipationView {
	v := s.views[iid]
	if v == nil {
		v = signaling.NewParticipationView()
		s.views[iid] = v
	}
	return v
}

func (s *state) snapshot() *Snapshot {
	snap := &Snapshot{Intervals: map[string]map[string][]signaling.Interval{}}
	for iid, v := range s.views {
		agents := map[string][]signaling.Interval{}
		for _, a := range v.Agents() {
			agents[a] = v.Intervals(a)
		}
		snap.Intervals[iid] = agents
	}
	return snap
}

func (s *state) restore(snap *Snapshot) {
	if snap == nil {
		return
	}
	for iid, agents := range snap.Intervals {
		v := s.view(iid)
		for a, in := range agents {
			v.SetIntervals(a, in)
		}
	}
}

// Config tunes the worker; zero values fall back to sane defaults.
type Config struct {
	MaxDeliver    int           // DLQ a fact past this delivery count (default 5)
	SnapshotEvery int           // save a snapshot every N acked facts (default 50)
	PublishRetry  int           // per-feed publish attempts before Nak (default 4)
	RetryBackoff  time.Duration // base backoff between publish attempts (default 50ms)
	LeaseRenew    time.Duration // lease heartbeat interval (default 2s; lease TTL ~5s)

	// TenantWideAgents is a DEV/TEST shortcut, off by default (nil): for a tenant present here, every
	// fact of that tenant fans out to the listed agents' feeds, bypassing the participation gate. This
	// exists because desk M1 emits no participation facts yet, so the stock per-participation fan-out
	// leaves every feed empty. Participation is still folded (snapshots stay correct); only the
	// recipient set is overridden. Production leaves this nil → strict per-participation fan-out.
	TenantWideAgents map[string][]string

	// Roster is the PRODUCTION tenant-shared fan-out source (off by default, nil): when set, every
	// fact of a tenant fans out to ALL agents the roster reports for that tenant (sourced from desk's
	// real Zitadel roster, no hardcode). It is the authoritative successor to TenantWideAgents and
	// takes precedence over it. Participation is still folded (snapshots stay correct); only the
	// recipient set is overridden, exactly like TenantWideAgents. Future per-participation mode leaves
	// this nil. A roster lookup error Naks the fact (redelivery), never drops it.
	Roster Roster
}

func (c Config) withDefaults() Config {
	if c.MaxDeliver <= 0 {
		c.MaxDeliver = 5
	}
	if c.SnapshotEvery <= 0 {
		c.SnapshotEvery = 50
	}
	if c.PublishRetry <= 0 {
		c.PublishRetry = 4
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 50 * time.Millisecond
	}
	if c.LeaseRenew <= 0 {
		c.LeaseRenew = 2 * time.Second
	}
	return c
}

// Projector is the single-active worker core. Constructed against the owned ports and driven by
// Run; the fold/project/revoke/hydrate logic is exercised directly in unit tests via the same
// ports backed by in-memory fakes (no live NATS).
type Projector struct {
	src   LogSource
	sink  FeedSink
	lease LeaseStore
	snaps SnapshotStore
	cfg   Config

	st         *state
	sinceSnap  int
	lastAckSeq uint64
}

func New(src LogSource, sink FeedSink, lease LeaseStore, snaps SnapshotStore, cfg Config) *Projector {
	return &Projector{src: src, sink: sink, lease: lease, snaps: snaps, cfg: cfg.withDefaults(), st: newState()}
}

// Run acquires the leader lease, hydrates from the acked-prefix snapshot, then serially processes
// facts until ctx is cancelled. Exactly one fact is in flight at a time (MaxAckPending=1), so the
// stateful fold is never concurrent. Lease renewal runs in the background; on its failure Run
// returns so a standby can take over.
func (p *Projector) Run(ctx context.Context) error {
	if err := p.lease.Acquire(ctx); err != nil {
		return fmt.Errorf("acquire lease: %w", err)
	}
	defer p.lease.Release(context.WithoutCancel(ctx))

	// Takeover ordering (PINNED): lease acquired → the prior holder's in-flight delivery settles
	// (MaxAckPending=1 makes Deliver itself wait for redelivery) → read ack_floor + hydrate → live.
	// Reading the floor before that settle is forbidden; here Hydrate reads it only after Acquire,
	// and the next Deliver blocks until the single un-acked fact is redelivered.
	if err := p.Hydrate(ctx); err != nil {
		return fmt.Errorf("hydrate: %w", err)
	}

	renewCtx, stopRenew := context.WithCancel(ctx)
	defer stopRenew()
	renewErr := make(chan error, 1)
	go p.renewLoop(renewCtx, renewErr)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-renewErr:
			return fmt.Errorf("lease lost: %w", err)
		default:
		}
		f, err := p.src.Deliver(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			return fmt.Errorf("deliver: %w", err)
		}
		if err := p.process(ctx, f); err != nil {
			return err
		}
	}
}

func (p *Projector) renewLoop(ctx context.Context, out chan<- error) {
	t := time.NewTicker(p.cfg.LeaseRenew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := p.lease.Renew(ctx); err != nil {
				out <- err
				return
			}
		}
	}
}

// Hydrate loads the latest snapshot whose seq <= durable ack floor and read-only-folds
// (snapshot_seq, ack_floor] to rebuild the live participation view — never a replay from zero.
func (p *Projector) Hydrate(ctx context.Context) error {
	floor, err := p.src.AckFloor(ctx)
	if err != nil {
		return err
	}
	snap, snapSeq, err := p.snaps.Load(ctx, floor)
	if err != nil {
		return err
	}
	p.st = newState()
	p.st.restore(snap)
	p.lastAckSeq = floor
	if floor > snapSeq {
		tail, err := p.src.FoldRange(ctx, snapSeq, floor)
		if err != nil {
			return err
		}
		for _, f := range tail {
			p.st.view(f.Event.TenantId + "/" + interactionOf(f)).ApplyFact(f.Event)
		}
	}
	return nil
}

// process folds ONE fact, fans it out to every participating agent, then acks ONLY after all
// intended publishes succeed (ack-after-publish). A poison fact past max_deliver is DLQ'd + acked
// so the consumer is not wedged; an open-interval revocation also writes the feed.revoked tombstone.
func (p *Projector) process(ctx context.Context, f Fact) error {
	// Continue the trace carried on the .log fact (F5b): every downstream publish (feed fan-out +
	// the revoked tombstone) and log line for this fact stays on the originating command's trace.
	// Only when the fact actually carries one — a trace-less fact is NOT given a fabricated trace.
	if tp := f.Traceparent(); tp != "" {
		ctx = obs.ContextFromTraceparent(ctx, tp)
	}
	log := obs.Logger(ctx)
	e := f.Event
	if e == nil || e.TenantId == "" || e.EventType == "" {
		return p.poison(ctx, f, "malformed envelope")
	}
	iid := interactionOf(f)
	if iid == "" {
		return p.poison(ctx, f, "unresolved interaction id")
	}
	key := e.TenantId + "/" + iid
	view := p.st.view(key)

	// Fold FIRST so the closing left fact's own interval is consulted, then pick recipients by the
	// interval covering S (epoch guard, Decision 6): join ≤ S ≤ left, so join@J and left@L each reach
	// their agent but no fact at S > L reaches an already-left agent.
	view.ApplyFact(e)
	recipients := coveredBy(view, e.Sequence)
	switch {
	case p.cfg.Roster != nil:
		// Production tenant-shared fan-out: ALL agents of the tenant per desk's real roster. A
		// roster outage Naks (redelivery) rather than dropping the fact or fanning to a stale set.
		agents, rerr := p.cfg.Roster.Agents(ctx, e.TenantId)
		if rerr != nil {
			log.Warn("projector.roster-failed", "tenant", e.TenantId, "subject_iid", iid,
				"sequence", e.Sequence, "err", rerr.Error())
			return p.src.Nak(f)
		}
		recipients = agents
		log.Info("projector.roster-fanout", "tenant", e.TenantId, "subject_iid", iid,
			"sequence", e.Sequence, "agents", agents)
	case len(p.cfg.TenantWideAgents[e.TenantId]) > 0:
		recipients = p.cfg.TenantWideAgents[e.TenantId] // dev/test shortcut, no participation gate
	}

	payload, err := proto.Marshal(e)
	if err != nil {
		return p.poison(ctx, f, "marshal event")
	}
	for _, agent := range recipients {
		dedup := fmt.Sprintf("%s.%s.%s.%d", e.TenantId, agent, iid, e.Sequence)
		if perr := p.publishWithRetry(ctx, e.TenantId, agent, iid, dedup, payload); perr != nil {
			// Leave the source un-acked: Nak schedules redelivery; dedup makes the re-projection of
			// already-published feeds a no-op (no drop, at-most-once per feed).
			log.Warn("projector.publish-failed", "subject_iid", iid, "agent", agent,
				"sequence", e.Sequence, "err", perr.Error())
			return p.src.Nak(f)
		}
	}

	if e.EventType == "participant.left" {
		if terr := p.tombstone(ctx, e.TenantId, e.ActorId, iid, e.Sequence); terr != nil {
			log.Warn("projector.tombstone-failed", "subject_iid", iid, "agent", e.ActorId,
				"sequence", e.Sequence, "err", terr.Error())
			return p.src.Nak(f)
		}
	}

	if err := p.src.Ack(f); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	p.lastAckSeq = f.StreamSeq
	p.maybeSnapshot(ctx, f.StreamSeq)
	return nil
}

func (p *Projector) publishWithRetry(ctx context.Context, tenant, agent, iid, dedup string, payload []byte) error {
	var err error
	for attempt := 0; attempt < p.cfg.PublishRetry; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.cfg.RetryBackoff << (attempt - 1)):
			}
		}
		if err = p.sink.Publish(ctx, tenant, agent, iid, dedup, payload); err == nil {
			return nil
		}
	}
	return err
}

// tombstone writes the terminal feed.revoked feed-control marker into the revoked agent's feed so a
// reconnecting client deterministically drops the interaction even if it missed participant.left.
// Deterministic dedup id keeps it at-most-once across redelivery.
func (p *Projector) tombstone(ctx context.Context, tenant, agent, iid string, atSeq int64) error {
	ctrl := &signaling.FeedControl{
		Schema: feedControlSchema, Control: controlRevoked, InteractionId: iid, AtSequence: atSeq,
	}
	payload, err := proto.Marshal(ctrl)
	if err != nil {
		return err
	}
	dedup := fmt.Sprintf("%s.%s.%s.%d.revoked", tenant, agent, iid, atSeq)
	return p.publishWithRetry(ctx, tenant, agent, iid, dedup, payload)
}

func (p *Projector) poison(ctx context.Context, f Fact, reason string) error {
	if p.src.Delivered(f) < p.cfg.MaxDeliver {
		obs.Logger(ctx).Warn("projector.poison-retry", "reason", reason, "delivered", p.src.Delivered(f))
		return p.src.Nak(f)
	}
	tenant, evID, seq := "", "", int64(0)
	if f.Event != nil {
		tenant, evID, seq = f.Event.TenantId, f.Event.EventId, f.Event.Sequence
	}
	if err := p.sink.Dlq(ctx, tenant, reason, evID, seq); err != nil {
		return p.src.Nak(f) // DLQ unavailable — keep the source un-acked, retry later
	}
	obs.Logger(ctx).Error("projector.dlq", "reason", reason, "event_id", evID, "sequence", seq)
	if err := p.src.Ack(f); err != nil { // ack so the consumer is not wedged
		return fmt.Errorf("ack poison: %w", err)
	}
	p.lastAckSeq = f.StreamSeq
	return nil
}

func (p *Projector) maybeSnapshot(ctx context.Context, seq uint64) {
	p.sinceSnap++
	if p.sinceSnap < p.cfg.SnapshotEvery {
		return
	}
	// Save only AFTER the Ack that produced this seq → the snapshot is always an acked prefix
	// (seq <= durable ack floor), never ahead of the cursor.
	if err := p.snaps.Save(ctx, seq, p.st.snapshot()); err != nil {
		obs.Logger(ctx).Warn("projector.snapshot-failed", "sequence", seq, "err", err.Error())
		return // a failed snapshot is non-fatal: hydration falls back to an older snapshot + a longer tail fold
	}
	p.sinceSnap = 0
}

// coveredBy returns the agents whose membership interval covers sequence S (join ≤ S ≤ left, or
// join ≤ S for an open interval) — the recipient set of the fact at S.
func coveredBy(v *signaling.ParticipationView, s int64) []string {
	var out []string
	for _, a := range v.Agents() {
		for _, in := range v.Intervals(a) {
			if in.JoinSeq <= s && (in.LeftOpen || s <= in.LeftSeq) {
				out = append(out, a)
				break
			}
		}
	}
	return out
}

// interactionOf returns the iid the adapter parsed from the delivery subject (the Event has no iid).
func interactionOf(f Fact) string { return f.iid }

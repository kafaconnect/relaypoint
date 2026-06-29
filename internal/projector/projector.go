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
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/signaling"
)

var errRosterUnavailable = errors.New("projector: roster unavailable after in-process retry window")

const (
	feedControlSchema = signaling.SchemaV1
	controlRevoked    = "feed.revoked"

	// fanoutConcurrency bounds the per-fact recipient fan-out. Only one fact is in flight at a
	// time (MaxAckPending=1), so this caps total concurrent feed publishes; a tenant's agent set is
	// small, so it mainly collapses N sequential publish RTTs into ~one. @spec: RDL-01
	fanoutConcurrency = 32

	// @spec:RDL-03
	leaseRenewAttempts     = 3
	leaseRenewRetryBackoff = 300 * time.Millisecond
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
	MaxDeliver        int           // DLQ a fact past this delivery count (default 5)
	SnapshotEvery     int           // save a snapshot every N acked facts (default 50)
	PublishRetry      int           // per-feed publish attempts before Nak (default 4)
	RetryBackoff      time.Duration // base backoff between publish attempts (default 50ms)
	LeaseRenew        time.Duration // lease heartbeat interval (default 2s; lease TTL ~5s)
	LeaseTTL          time.Duration // lease expiry; the renew budget derives from (LeaseTTL − LeaseRenew) (default 5s)
	RosterRetryWindow time.Duration // in-process roster-retry cap before Nak fallback (default 90s)

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
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 5 * time.Second
	}
	if c.LeaseRenew >= c.LeaseTTL {
		c.LeaseRenew = c.LeaseTTL / 2 // keep a positive fencing slack
	}
	if c.RosterRetryWindow <= 0 {
		c.RosterRetryWindow = 90 * time.Second
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

	fence := newFence(ctx)
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer stopRenew()
	go p.renewLoop(renewCtx, fence)

	for {
		dctx := fence.begin()
		if dctx == nil {
			return fence.exitErr() // parent cancelled or lease confirmed lost
		}
		f, err := p.src.Deliver(dctx)
		if err != nil {
			if dctx.Err() != nil {
				continue // fenced (paused/lost) or shutting down mid-deliver — re-evaluate via begin
			}
			return fmt.Errorf("deliver: %w", err)
		}
		if err := p.process(dctx, f); err != nil {
			if dctx.Err() != nil {
				continue // fenced mid-process — the fact stays un-acked (Nak) for redelivery
			}
			return err
		}
	}
}

func (p *Projector) renewLoop(ctx context.Context, fence *fence) {
	t := time.NewTicker(p.cfg.LeaseRenew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := p.renewWithRetry(ctx, fence); err != nil {
				if ctx.Err() != nil {
					return // shutting down, not a lease loss
				}
				fence.fail(err) // confirmed loss → the data loop exits and a standby takes over
				return
			}
		}
	}
}

// @spec:RDL-03
func (p *Projector) renewWithRetry(ctx context.Context, fence *fence) error {
	perAttempt, attempts, backoff := renewBudget(p.cfg.LeaseTTL, p.cfg.LeaseRenew)
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		actx, cancel := context.WithTimeout(ctx, perAttempt)
		err = p.lease.Renew(actx)
		cancel()
		if err == nil {
			fence.resume()
			return nil
		}
		fence.pause()
		obs.Logger(ctx).Warn("projector.lease-renew-retry", "attempt", attempt+1, "err", err.Error())
	}
	return err
}

// @spec:RDL-03
func renewBudget(ttl, renewInterval time.Duration) (perAttempt time.Duration, attempts int, backoff time.Duration) {
	slack := ttl - renewInterval
	if slack <= 0 {
		return time.Millisecond, 1, 0
	}
	attempts, backoff = leaseRenewAttempts, leaseRenewRetryBackoff
	budget := slack * 9 / 10
	if time.Duration(attempts-1)*backoff >= budget {
		backoff = budget / time.Duration(attempts) / 2
	}
	perAttempt = (budget - time.Duration(attempts-1)*backoff) / time.Duration(attempts)
	if perAttempt <= 0 {
		return budget, 1, 0
	}
	return perAttempt, attempts, backoff
}

// @spec:RDL-03
type fence struct {
	parent context.Context
	mu     sync.Mutex
	paused bool
	lost   error
	cancel context.CancelFunc
	wakeup chan struct{}
}

func newFence(parent context.Context) *fence {
	return &fence{parent: parent, wakeup: make(chan struct{})}
}

func (f *fence) begin() context.Context {
	for {
		f.mu.Lock()
		if f.lost != nil || f.parent.Err() != nil {
			f.mu.Unlock()
			return nil
		}
		if !f.paused {
			if f.cancel != nil {
				f.cancel()
			}
			dctx, cancel := context.WithCancel(f.parent)
			f.cancel = cancel
			f.mu.Unlock()
			return dctx
		}
		wake := f.wakeup
		f.mu.Unlock()
		select {
		case <-f.parent.Done():
		case <-wake:
		}
	}
}

func (f *fence) pause() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.paused || f.lost != nil {
		return
	}
	f.paused = true
	if f.cancel != nil {
		f.cancel() // stop-the-world: cancel the in-flight Deliver/process
	}
}

func (f *fence) resume() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.paused || f.lost != nil {
		return
	}
	f.paused = false
	close(f.wakeup)
	f.wakeup = make(chan struct{})
}

func (f *fence) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lost != nil {
		return
	}
	f.lost = err
	if f.cancel != nil {
		f.cancel()
	}
	close(f.wakeup) // wake a begin() blocked while paused so the loop can exit
	f.wakeup = make(chan struct{})
}

func (f *fence) exitErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lost != nil {
		return fmt.Errorf("lease lost: %w", f.lost)
	}
	return f.parent.Err()
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

func (p *Projector) process(ctx context.Context, f Fact) error {
	if tp := f.Traceparent(); tp != "" {
		ctx = obs.ContextFromTraceparent(ctx, tp)
	}
	log := obs.Logger(ctx)
	e := f.Event
	if e == nil || e.TenantId == "" || e.EventType == "" {
		return p.dlqOrNak(ctx, f, "malformed envelope")
	}
	iid := interactionOf(f)
	if iid == "" {
		return p.dlqOrNak(ctx, f, "unresolved interaction id")
	}
	key := e.TenantId + "/" + iid
	view := p.st.view(key)

	// Fold before picking recipients so a closing left@L is itself projected but no fact at S>L is.
	view.ApplyFact(e)
	recipients := coveredBy(view, e.Sequence)
	switch {
	case p.cfg.Roster != nil:
		agents, rerr := p.resolveRoster(ctx, f, e, iid, log)
		if rerr != nil {
			return p.src.Nak(f)
		}
		recipients = agents
		log.Info("projector.roster-fanout", "tenant", e.TenantId, "subject_iid", iid,
			"sequence", e.Sequence, "agents", agents)
	case len(p.cfg.TenantWideAgents[e.TenantId]) > 0:
		recipients = p.cfg.TenantWideAgents[e.TenantId]
	}

	payload, err := proto.Marshal(e)
	if err != nil {
		return p.dlqOrNak(ctx, f, "marshal event")
	}
	if perr := p.fanout(ctx, e, iid, recipients, payload); perr != nil {
		if ctx.Err() != nil {
			return p.src.Nak(f)
		}
		return p.dlqOrNak(ctx, f, fmt.Sprintf("feed publish failed: %v", perr))
	}

	if e.EventType == "participant.left" {
		if terr := p.tombstone(ctx, e.TenantId, e.ActorId, iid, e.Sequence); terr != nil {
			if ctx.Err() != nil {
				return p.src.Nak(f)
			}
			return p.dlqOrNak(ctx, f, fmt.Sprintf("revoked tombstone failed: %v", terr))
		}
	}

	// WHY: a fence (overdue/lost lease) cancels ctx mid-fact; a stale holder must Nak, never ack/snapshot (RH-02).
	if ctx.Err() != nil {
		return p.src.Nak(f)
	}
	if err := p.src.Ack(f); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	p.lastAckSeq = f.StreamSeq
	p.maybeSnapshot(ctx, f.StreamSeq)
	return nil
}

// @spec:projector.roster.unbounded-retry
func (p *Projector) resolveRoster(ctx context.Context, f Fact, e *signaling.Event, iid string, log *slog.Logger) ([]string, error) {
	deadline := time.Now().Add(p.cfg.RosterRetryWindow)
	backoff := p.cfg.RetryBackoff
	for {
		agents, err := p.cfg.Roster.Agents(ctx, e.TenantId)
		switch {
		case err == nil && len(agents) > 0:
			return agents, nil
		case err != nil:
			log.Warn("projector.roster-failed", "tenant", e.TenantId, "subject_iid", iid,
				"sequence", e.Sequence, "err", err.Error())
		default:
			log.Warn("projector.roster-empty", "tenant", e.TenantId, "subject_iid", iid, "sequence", e.Sequence)
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return nil, errRosterUnavailable
		}
		// WHY: hold the MaxAckPending=1 slot via InProgress so a roster blip doesn't burn the MaxDeliver budget (RH-04).
		if ierr := p.src.InProgress(f); ierr != nil {
			return nil, errRosterUnavailable
		}
		select {
		case <-ctx.Done():
			return nil, errRosterUnavailable
		case <-time.After(backoff):
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
}

// @spec: RDL-01
func (p *Projector) fanout(ctx context.Context, e *signaling.Event, iid string, recipients []string, payload []byte) error {
	if len(recipients) == 0 {
		return nil
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(fanoutConcurrency)
	for _, agent := range recipients {
		agent := agent
		dedup := fmt.Sprintf("%s.%s.%s.%d", e.TenantId, agent, iid, e.Sequence)
		g.Go(func() error {
			return p.publishWithRetry(gctx, e.TenantId, agent, iid, dedup, payload)
		})
	}
	return g.Wait()
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

// @spec:projector.delivery.exhausted-to-dlq
func (p *Projector) dlqOrNak(ctx context.Context, f Fact, reason string) error {
	if p.src.Delivered(f) < p.cfg.MaxDeliver {
		obs.Logger(ctx).Warn("projector.delivery-retry", "reason", reason, "delivered", p.src.Delivered(f))
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
		return fmt.Errorf("ack dlq: %w", err)
	}
	p.lastAckSeq = f.StreamSeq
	return nil
}

func (p *Projector) maybeSnapshot(ctx context.Context, seq uint64) {
	p.sinceSnap++
	if p.sinceSnap < p.cfg.SnapshotEvery {
		return
	}
	if err := p.snaps.Save(ctx, seq, p.st.snapshot()); err != nil {
		obs.Logger(ctx).Warn("projector.snapshot-failed", "sequence", seq, "err", err.Error())
		return
	}
	p.sinceSnap = 0
}

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

func interactionOf(f Fact) string { return f.iid }

package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// traceparentOf reads the inbound .log message's W3C trace header (nil-safe — a header-less message
// yields ""), so the projector can re-inject it onto the agent feed (F5b trace continuity).
func traceparentOf(m *nats.Msg) string {
	if m == nil || m.Header == nil {
		return ""
	}
	return m.Header.Get("traceparent")
}

// The NATS adapters — the only code here importing nats.go — implement the owned ports so the core
// never sees a NATS type (loose-coupling HARD RULE).

const (
	feedStream   = "AGENT_FEED"
	feedSubjects = "tenant.*.agent.*.feed.*"
	dlqSubjects  = "tenant.*.agent.dlq.feed"
	durableName  = "fanout-projector"
	kvLeaseName  = "projector-lease"
	kvSnapName   = "projector-snapshot"
)

// EnsureFeedStream creates/updates the EPHEMERAL agent-feed stream: a short max_age live
// disconnect-gap bridge (NOT the audit store — the canonical .log is). Per-subject dedup over a
// window >= the redelivery/restart horizon makes the deterministic Nats-Msg-Id at-most-once per
// (agent, interaction, sequence). Decision 8.
func EnsureFeedStream(js nats.JetStreamContext, maxAge, dedupWindow time.Duration) error {
	if maxAge <= 0 {
		maxAge = time.Hour
	}
	if dedupWindow <= 0 {
		dedupWindow = 10 * time.Minute
	}
	cfg := &nats.StreamConfig{
		Name:              feedStream,
		Subjects:          []string{feedSubjects, dlqSubjects},
		Storage:           nats.FileStorage,
		Retention:         nats.LimitsPolicy,
		Discard:           nats.DiscardOld,
		MaxAge:            maxAge,
		MaxMsgsPerSubject: 256,
		Duplicates:        dedupWindow,
	}
	if _, err := js.AddStream(cfg); err != nil {
		if _, uerr := js.UpdateStream(cfg); uerr != nil {
			return err
		}
	}
	return nil
}

type jsLogSource struct {
	js      nats.JetStreamContext
	sub     *nats.Subscription
	durable string
}

// NewLogSource binds the durable pull consumer with MaxAckPending=1 (one in-flight fact, no
// prefetch) so the stateful fold is strictly serial and a lease takeover never overlaps in-flight
// processing. maxDeliver bounds redelivery before the core DLQs the poison fact.
func NewLogSource(js nats.JetStreamContext, maxDeliver int, ackWait time.Duration) (LogSource, error) {
	if ackWait <= 0 {
		ackWait = 30 * time.Second
	}
	sub, err := js.PullSubscribe("tenant.*.interaction.*.log", durableName,
		nats.ManualAck(), nats.AckExplicit(), nats.MaxAckPending(1),
		nats.MaxDeliver(maxDeliver), nats.AckWait(ackWait), nats.DeliverAll())
	if err != nil {
		return nil, err
	}
	return &jsLogSource{js: js, sub: sub, durable: durableName}, nil
}

func (s *jsLogSource) Deliver(ctx context.Context) (Fact, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Fact{}, err
		}
		msgs, err := s.sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(err, nats.ErrTimeout) || (err == nil && len(msgs) == 0) {
			continue
		}
		if err != nil {
			return Fact{}, err
		}
		m := msgs[0]
		meta, merr := m.Metadata()
		if merr != nil {
			return Fact{}, fmt.Errorf("delivery metadata: %w", merr)
		}
		e := &signaling.Event{}
		// A corrupt envelope is still a delivered fact: carry a nil Event so the core DLQs it past
		// max_deliver rather than wedging the consumer.
		if uerr := proto.Unmarshal(m.Data, e); uerr != nil {
			e = nil
		}
		return Fact{Event: e, iid: iidFromLogSubject(m.Subject), StreamSeq: meta.Sequence.Stream, msg: m, traceparent: traceparentOf(m)}, nil
	}
}

func (s *jsLogSource) Ack(f Fact) error {
	return ack(f, func(m *nats.Msg) error { return m.AckSync() })
}
func (s *jsLogSource) Nak(f Fact) error { return ack(f, func(m *nats.Msg) error { return m.Nak() }) }

func (s *jsLogSource) Delivered(f Fact) int {
	m, ok := f.msg.(*nats.Msg)
	if !ok || m == nil {
		return 0
	}
	meta, err := m.Metadata()
	if err != nil {
		return 0
	}
	return int(meta.NumDelivered)
}

func (s *jsLogSource) AckFloor(_ context.Context) (uint64, error) {
	info, err := s.sub.ConsumerInfo()
	if err != nil {
		return 0, err
	}
	return info.AckFloor.Stream, nil
}

// FoldRange replays (lo, hi] by stream sequence with an ephemeral AckNone reader — read-only, no
// cursor mutation — so hydration rebuilds the tail above the snapshot without touching the durable.
func (s *jsLogSource) FoldRange(_ context.Context, lo, hi uint64) ([]Fact, error) {
	if hi <= lo {
		return nil, nil
	}
	sub, err := s.js.PullSubscribe("tenant.*.interaction.*.log", "",
		nats.StartSequence(lo+1), nats.AckNone())
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	var out []Fact
	for {
		msgs, ferr := sub.Fetch(128, nats.MaxWait(500*time.Millisecond))
		if errors.Is(ferr, nats.ErrTimeout) || (ferr == nil && len(msgs) == 0) {
			break
		}
		if ferr != nil {
			return nil, ferr
		}
		done := false
		for _, m := range msgs {
			meta, merr := m.Metadata()
			if merr != nil {
				return nil, merr
			}
			if meta.Sequence.Stream > hi {
				done = true
				break
			}
			e := &signaling.Event{}
			if uerr := proto.Unmarshal(m.Data, e); uerr != nil {
				continue // a corrupt fact below the ack floor was already DLQ'd; skip on re-fold
			}
			out = append(out, Fact{Event: e, iid: iidFromLogSubject(m.Subject), StreamSeq: meta.Sequence.Stream})
		}
		if done {
			break
		}
	}
	return out, nil
}

func ack(f Fact, fn func(*nats.Msg) error) error {
	m, ok := f.msg.(*nats.Msg)
	if !ok || m == nil {
		return nil
	}
	return fn(m)
}

func iidFromLogSubject(s string) string {
	p := strings.Split(s, ".")
	if len(p) == 5 && p[0] == "tenant" && p[2] == "interaction" && p[4] == "log" {
		return p[3]
	}
	return ""
}

type jsFeedSink struct{ js nats.JetStreamContext }

func NewFeedSink(js nats.JetStreamContext) FeedSink { return &jsFeedSink{js: js} }

func (s *jsFeedSink) Publish(ctx context.Context, tenant, agent, iid, dedupID string, payload []byte) error {
	subj := fmt.Sprintf("tenant.%s.agent.%s.feed.%s", tenant, agent, iid)
	// Publish via a message so the fact's trace rides onto the feed event (F5b). MsgId keeps the
	// at-most-once dedup; the traceparent header is outside the dedup identity.
	msg := nats.NewMsg(subj)
	msg.Data = payload
	obs.InjectTraceparent(ctx, func(k, v string) { msg.Header.Set(k, v) })
	_, err := s.js.PublishMsg(msg, nats.MsgId(dedupID))
	return err
}

func (s *jsFeedSink) Dlq(_ context.Context, tenant, reason, eventID string, seq int64) error {
	subj := fmt.Sprintf("tenant.%s.agent.dlq.feed", tenant)
	body, _ := json.Marshal(map[string]any{"reason": reason, "event_id": eventID, "sequence": seq})
	_, err := s.js.Publish(subj, body)
	return err
}

type kvLease struct {
	kv     nats.KeyValue
	key    string
	holder string // this worker's unique id
	rev    uint64
}

// NewLeaseStore opens (or creates) the lease KV with a per-key TTL and returns a lease for `holder`.
// Acquire wins by Create (key absent) or Update (the holder is already us); the TTL expiry lets a
// standby take over after the prior holder dies.
func NewLeaseStore(js nats.JetStreamContext, holder string, ttl time.Duration) (LeaseStore, error) {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	kv, err := js.KeyValue(kvLeaseName)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: kvLeaseName, TTL: ttl, History: 1})
		if err != nil {
			return nil, err
		}
	}
	return &kvLease{kv: kv, key: "leader", holder: holder}, nil
}

func (l *kvLease) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rev, err := l.kv.Create(l.key, []byte(l.holder))
		if err == nil {
			l.rev = rev
			return nil
		}
		// Key present: the prior holder may have died (TTL not yet expired) or it's us. Read it; if
		// it's us, adopt the revision; otherwise wait out the TTL and retry.
		entry, gerr := l.kv.Get(l.key)
		if gerr == nil && string(entry.Value()) == l.holder {
			l.rev = entry.Revision()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (l *kvLease) Renew(_ context.Context) error {
	rev, err := l.kv.Update(l.key, []byte(l.holder), l.rev)
	if err != nil {
		return err // lost the lease (expired + reclaimed, or a wrong-revision write)
	}
	l.rev = rev
	return nil
}

func (l *kvLease) Release(_ context.Context) error {
	entry, err := l.kv.Get(l.key)
	if err != nil || string(entry.Value()) != l.holder {
		return nil // not ours (or already gone) — nothing to release
	}
	return l.kv.Delete(l.key)
}

type kvSnapshot struct{ kv nats.KeyValue }

// NewSnapshotStore opens (or creates) the snapshot KV. History>1 keeps a few prior snapshots so a
// failed-mid-save worker can still hydrate from an older acked-prefix snapshot.
func NewSnapshotStore(js nats.JetStreamContext) (SnapshotStore, error) {
	kv, err := js.KeyValue(kvSnapName)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: kvSnapName, History: 8})
		if err != nil {
			return nil, err
		}
	}
	return &kvSnapshot{kv: kv}, nil
}

// snapEnvelope is the wire form of a stored snapshot (the seq + the serialized view).
type snapEnvelope struct {
	Seq  uint64    `json:"seq"`
	View *Snapshot `json:"view"`
}

func (s *kvSnapshot) Save(_ context.Context, seq uint64, snap *Snapshot) error {
	body, err := json.Marshal(snapEnvelope{Seq: seq, View: snap})
	if err != nil {
		return err
	}
	_, err = s.kv.Put("latest", body)
	return err
}

// Load returns the most recent snapshot whose Seq <= maxSeq. The KV history is walked newest-first
// so a snapshot saved AHEAD of a rolled-back ack floor (should not happen, but defensively) is
// skipped in favour of an older acked-prefix one.
func (s *kvSnapshot) Load(_ context.Context, maxSeq uint64) (*Snapshot, uint64, error) {
	hist, err := s.kv.History("latest")
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	for i := len(hist) - 1; i >= 0; i-- {
		if hist[i].Operation() != nats.KeyValuePut {
			continue
		}
		var env snapEnvelope
		if uerr := json.Unmarshal(hist[i].Value(), &env); uerr != nil {
			continue
		}
		if env.Seq <= maxSeq {
			return env.View, env.Seq, nil
		}
	}
	return nil, 0, nil
}

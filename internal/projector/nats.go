package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/signaling"
)

func traceparentOf(m *nats.Msg) string {
	if m == nil || m.Header == nil {
		return ""
	}
	return m.Header.Get("traceparent")
}

// NATS adapters implement the owned ports so the core never imports a NATS type (loose-coupling HARD RULE).

const (
	feedStream   = "AGENT_FEED"
	feedSubjects = "tenant.*.agent.*.feed.*"
	dlqSubjects  = "tenant.*.agent.dlq.feed"
	durableName  = "fanout-projector"
	kvLeaseName  = "projector-lease"
	kvSnapName   = "projector-snapshot"
)

// EPHEMERAL gap-bridge stream (NOT the audit store — .log is); the dedup window >= the redelivery/restart horizon makes the Nats-Msg-Id at-most-once per (agent, interaction, sequence). Decision 8.
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

// MaxAckPending=1 (no prefetch) keeps the fold strictly serial so a lease takeover never overlaps in-flight processing.
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
		// A corrupt envelope is still a delivered fact: carry a nil Event so the core DLQs it past max_deliver instead of wedging the consumer.
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
func (s *jsLogSource) InProgress(f Fact) error {
	return ack(f, func(m *nats.Msg) error { return m.InProgress() })
}

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

// Ephemeral AckNone reader: read-only, no cursor mutation, so hydration rebuilds the tail without touching the durable.
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
	msg := nats.NewMsg(subj)
	msg.Data = payload
	obs.InjectTraceparent(ctx, func(k, v string) { msg.Header.Set(k, v) })
	// WHY: ctx-bound the publish so a fence cancelling the data ctx aborts an in-flight fan-out (RH-02).
	_, err := s.js.PublishMsg(msg, nats.MsgId(dedupID), nats.Context(ctx))
	return err
}

func (s *jsFeedSink) Dlq(_ context.Context, tenant, reason, eventID string, seq int64) error {
	subj := fmt.Sprintf("tenant.%s.agent.dlq.feed", tenant)
	body, _ := json.Marshal(map[string]any{"reason": reason, "event_id": eventID, "sequence": seq})
	_, err := s.js.Publish(subj, body)
	return err
}

type kvLease struct {
	kv     jetstream.KeyValue
	key    string
	holder string
	rev    uint64
}

func NewLeaseStore(js jetstream.JetStream, holder string, ttl time.Duration) (LeaseStore, error) {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, kvLeaseName)
	if err != nil {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: kvLeaseName, TTL: ttl, History: 1})
		if err != nil {
			return nil, err
		}
	}
	st, err := kv.Status(ctx)
	if err != nil {
		return nil, err
	}
	// WHY: the renew budget derives from the configured TTL, so a drifted bucket TTL must fail closed.
	if st.TTL() != ttl {
		return nil, fmt.Errorf("projector: lease bucket %q TTL %v != configured %v; recreate the bucket or align LeaseTTL", kvLeaseName, st.TTL(), ttl)
	}
	return &kvLease{kv: kv, key: "leader", holder: holder}, nil
}

func (l *kvLease) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rev, err := l.kv.Create(ctx, l.key, []byte(l.holder))
		if err == nil {
			l.rev = rev
			return nil
		}
		entry, gerr := l.kv.Get(ctx, l.key)
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

// @spec:RDL-03
func (l *kvLease) Renew(ctx context.Context) error {
	rev, err := l.kv.Update(ctx, l.key, []byte(l.holder), l.rev)
	if err != nil {
		return err
	}
	l.rev = rev
	return nil
}

func (l *kvLease) Release(ctx context.Context) error {
	entry, err := l.kv.Get(ctx, l.key)
	if err != nil || string(entry.Value()) != l.holder {
		return nil
	}
	return l.kv.Delete(ctx, l.key)
}

type kvSnapshot struct{ kv jetstream.KeyValue }

func NewSnapshotStore(js jetstream.JetStream) (SnapshotStore, error) {
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, kvSnapName)
	if err != nil {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: kvSnapName, History: 8})
		if err != nil {
			return nil, err
		}
	}
	return &kvSnapshot{kv: kv}, nil
}

type snapEnvelope struct {
	Seq  uint64    `json:"seq"`
	View *Snapshot `json:"view"`
}

func (s *kvSnapshot) Save(ctx context.Context, seq uint64, snap *Snapshot) error {
	body, err := json.Marshal(snapEnvelope{Seq: seq, View: snap})
	if err != nil {
		return err
	}
	_, err = s.kv.Put(ctx, "latest", body)
	return err
}

func (s *kvSnapshot) Load(ctx context.Context, maxSeq uint64) (*Snapshot, uint64, error) {
	hist, err := s.kv.History(ctx, "latest")
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	for i := len(hist) - 1; i >= 0; i-- {
		if hist[i].Operation() != jetstream.KeyValuePut {
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

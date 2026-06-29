package signaling

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/obs"
)

// ErrOCCConflict: the append's expected last-subject-sequence no longer matched (another writer advanced the subject); retryable — re-fold and retry, never blindly append behind it (router-occ).
var ErrOCCConflict = errors.New("optimistic-concurrency conflict: expected last-subject-sequence mismatch")

// LogStore is the core's only infrastructure dependency — a port, not NATS (loose-coupling HARD RULE); JetStream is the Phase-1 adapter.
type LogStore interface {
	// Append publishes under per-subject OCC: rejects ErrOCCConflict unless the subject's last STREAM seq == expectedLastSubjSeq (0 = empty); dedupID makes a retry idempotent (duplicate=true, no second fact).
	// committedSeq = broker ack.Sequence; the next same-subject append echoes it, not prev+1 (RH-01).
	// dedup-vs-OCC ordering is BROKER-DEPENDENT: on single-server (R1) JetStream the OCC check runs BEFORE dedup, so a retry of a committed command_id can surface as ErrOCCConflict — callers MUST treat it as "rebuild + re-check dedup", never "not committed".
	// ctx carries the inbound W3C trace, published as a `traceparent` header so a trace spans router→.log→projector→feed (F5b).
	Append(ctx context.Context, subject string, data []byte, dedupID string, expectedLastSubjSeq uint64) (duplicate bool, committedSeq uint64, err error)
	// Replay MUST error (not a short slice) if the log can't be fully read, so callers fail closed; it also returns the subject's last STREAM seq (the OCC token, distinct from the dense per-interaction sequence).
	Replay(subject string) (events []*Event, lastSubjSeq uint64, err error)
}

type jetstreamStore struct{ js nats.JetStreamContext }

func NewJetStreamStore(js nats.JetStreamContext) LogStore { return &jetstreamStore{js: js} }

func (s *jetstreamStore) Append(ctx context.Context, subject string, data []byte, dedupID string, expectedLastSubjSeq uint64) (bool, uint64, error) {
	// Publish via a message so the inbound trace rides onto the fact as a `traceparent` header (F5b); MsgId + ExpectLastSequencePerSubject preserve dedup + OCC.
	msg := nats.NewMsg(subject)
	msg.Data = data
	obs.InjectTraceparent(ctx, func(k, v string) { msg.Header.Set(k, v) })
	ack, err := s.js.PublishMsg(msg,
		nats.MsgId(dedupID), nats.ExpectLastSequencePerSubject(expectedLastSubjSeq))
	if err != nil {
		var apiErr *nats.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == nats.JSErrCodeStreamWrongLastSequence {
			return false, 0, ErrOCCConflict
		}
		return false, 0, err
	}
	return ack.Duplicate, ack.Sequence, nil
}

func (s *jetstreamStore) Replay(subject string) ([]*Event, uint64, error) {
	// Empty durable name → an ephemeral consumer: a throwaway reader that leaves no durable consumer state on the server.
	sub, err := s.js.PullSubscribe(subject, "", nats.DeliverAll(), nats.AckNone())
	if err != nil {
		return nil, 0, err
	}
	defer sub.Unsubscribe()
	var out []*Event
	var lastSubjSeq uint64
	for {
		msgs, ferr := sub.Fetch(128, nats.MaxWait(250*time.Millisecond))
		if errors.Is(ferr, nats.ErrTimeout) || (ferr == nil && len(msgs) == 0) {
			break
		}
		if ferr != nil {
			return nil, 0, ferr // fail closed — never return partial state
		}
		for _, m := range msgs {
			meta, merr := m.Metadata()
			if merr != nil {
				return nil, 0, fmt.Errorf("replay %s: no stream metadata: %w", subject, merr)
			}
			lastSubjSeq = meta.Sequence.Stream
			e := &Event{}
			if err := proto.Unmarshal(m.Data, e); err != nil {
				return nil, 0, fmt.Errorf("replay %s: corrupt fact: %w", subject, err) // fail closed
			}
			out = append(out, e)
		}
	}
	return out, lastSubjSeq, nil
}

func logStreamConfig() *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:              "INTERACTION_LOGS",
		Subjects:          []string{"tenant.*.interaction.*.log"},
		Storage:           nats.FileStorage,
		Retention:         nats.LimitsPolicy,
		Discard:           nats.DiscardOld,
		MaxMsgsPerSubject: -1,
	}
}

func EnsureLogStream(js nats.JetStreamContext) error {
	cfg := logStreamConfig()
	if _, err := js.AddStream(cfg); err != nil {
		if _, uerr := js.UpdateStream(cfg); uerr != nil {
			return err
		}
	}
	return nil
}

// ResetLogStream is the ADR-0002 protobuf-cutover step: deletes+recreates INTERACTION_LOGS so no JSON-era fact survives (a protobuf router unmarshalling a JSON fact bricks that interaction); destructive dev-only, gated by RP_RESET_LOG_STREAM.
func ResetLogStream(js nats.JetStreamContext) error {
	if err := js.DeleteStream("INTERACTION_LOGS"); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err := js.AddStream(logStreamConfig())
	return err
}

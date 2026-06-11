package signaling

import (
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// ErrOCCConflict means the append's expected last-subject-sequence no longer matched: another
// writer (or a stale rebuilt state) advanced the subject. It is retryable — the caller must
// re-fold and retry, never blindly append behind it. See openspec change router-occ.
var ErrOCCConflict = errors.New("optimistic-concurrency conflict: expected last-subject-sequence mismatch")

// LogStore is the router core's only infrastructure dependency — the core depends on this port,
// not on NATS (loose-coupling HARD RULE). JetStream is the Phase-1 adapter.
type LogStore interface {
	// Append publishes a fact under per-subject optimistic concurrency: the store rejects with
	// ErrOCCConflict unless the subject's current last STREAM sequence equals expectedLastSubjSeq
	// (0 = the subject is expected empty). dedupID makes the append idempotent: a retry with the
	// same dedupID returns duplicate=true and writes no second fact. newSubjSeq is the STREAM
	// sequence the store actually assigned (GLOBAL, not per-subject contiguous) — the OCC token the
	// caller MUST echo on its next append, never a naive +1.
	Append(subject string, data []byte, dedupID string, expectedLastSubjSeq uint64) (newSubjSeq uint64, duplicate bool, err error)
	// Replay MUST error (not return a short slice) if the log can't be fully read, so callers
	// fail closed. It also returns the subject's current last STREAM sequence (0 if empty) — the
	// OCC token a subsequent Append must echo, distinct from the dense per-interaction sequence.
	Replay(subject string) (events []*Event, lastSubjSeq uint64, err error)
}

type jetstreamStore struct{ js nats.JetStreamContext }

func NewJetStreamStore(js nats.JetStreamContext) LogStore { return &jetstreamStore{js: js} }

func (s *jetstreamStore) Append(subject string, data []byte, dedupID string, expectedLastSubjSeq uint64) (uint64, bool, error) {
	ack, err := s.js.Publish(subject, data,
		nats.MsgId(dedupID), nats.ExpectLastSequencePerSubject(expectedLastSubjSeq))
	if err != nil {
		var apiErr *nats.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == nats.JSErrCodeStreamWrongLastSequence {
			return 0, false, ErrOCCConflict
		}
		return 0, false, err
	}
	return ack.Sequence, ack.Duplicate, nil
}

func (s *jetstreamStore) Replay(subject string) ([]*Event, uint64, error) {
	// Empty durable name → an ephemeral consumer: a throwaway one-shot reader to drain the
	// subject for this replay, leaving no durable consumer state on the server.
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

// ResetLogStream is the ADR-0002 protobuf-cutover step: it deletes and recreates INTERACTION_LOGS
// so no JSON-era fact survives (a protobuf router calling proto.Unmarshal on a JSON fact fails
// closed and bricks that interaction). It is a destructive dev reset — there is no production
// history to retain. Gated behind RP_RESET_LOG_STREAM in main; integration tests call it to start
// from a clean stream.
func ResetLogStream(js nats.JetStreamContext) error {
	if err := js.DeleteStream("INTERACTION_LOGS"); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err := js.AddStream(logStreamConfig())
	return err
}

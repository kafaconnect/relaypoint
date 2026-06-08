package signaling

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
)

// LogStore is the durable, ordered fact log the router appends to and replays from.
// It is the ONLY infrastructure dependency of the router core — the router knows
// nothing about NATS. NATS JetStream is the Phase-1 adapter; another backend can be
// dropped in by implementing this port.
type LogStore interface {
	// Append writes one fact. dedupID makes the append idempotent: a retry with the
	// same dedupID returns duplicate=true and does NOT write a second fact.
	Append(subject string, data []byte, dedupID string) (duplicate bool, err error)
	// Replay returns every fact on subject, in order (for state rebuild after restart).
	Replay(subject string) ([]Event, error)
}

// --- NATS JetStream adapter ---------------------------------------------------

type jetstreamStore struct{ js nats.JetStreamContext }

// NewJetStreamStore adapts a NATS JetStream context to the LogStore port.
func NewJetStreamStore(js nats.JetStreamContext) LogStore { return &jetstreamStore{js: js} }

func (s *jetstreamStore) Append(subject string, data []byte, dedupID string) (bool, error) {
	ack, err := s.js.Publish(subject, data, nats.MsgId(dedupID))
	if err != nil {
		return false, err
	}
	return ack.Duplicate, nil
}

func (s *jetstreamStore) Replay(subject string) ([]Event, error) {
	sub, err := s.js.PullSubscribe(subject, "", nats.DeliverAll(), nats.AckNone())
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	var out []Event
	for {
		msgs, ferr := sub.Fetch(128, nats.MaxWait(250*time.Millisecond))
		if ferr != nil || len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			var e Event
			if json.Unmarshal(m.Data, &e) == nil {
				out = append(out, e)
			}
		}
	}
	return out, nil
}

// EnsureLogStream creates (or updates) the durable JetStream stream that captures
// every interaction `.log` fact — ordered, replayable (signaling.log-durable).
func EnsureLogStream(js nats.JetStreamContext) error {
	cfg := &nats.StreamConfig{
		Name:              "INTERACTION_LOG",
		Subjects:          []string{"tenant.*.interaction.*.log"},
		Storage:           nats.FileStorage,
		Retention:         nats.LimitsPolicy,
		Discard:           nats.DiscardOld,
		MaxMsgsPerSubject: -1,
	}
	if _, err := js.AddStream(cfg); err != nil {
		if _, uerr := js.UpdateStream(cfg); uerr != nil {
			return err
		}
	}
	return nil
}

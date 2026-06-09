package signaling

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// LogStore is the router core's only infrastructure dependency — the core depends on this port,
// not on NATS (loose-coupling HARD RULE). JetStream is the Phase-1 adapter.
type LogStore interface {
	// dedupID makes the append idempotent: a retry with the same dedupID returns duplicate=true
	// and writes no second fact.
	Append(subject string, data []byte, dedupID string) (duplicate bool, err error)
	// Replay MUST error (not return a short slice) if the log can't be fully read, so callers
	// fail closed.
	Replay(subject string) ([]Event, error)
}

type jetstreamStore struct{ js nats.JetStreamContext }

func NewJetStreamStore(js nats.JetStreamContext) LogStore { return &jetstreamStore{js: js} }

func (s *jetstreamStore) Append(subject string, data []byte, dedupID string) (bool, error) {
	ack, err := s.js.Publish(subject, data, nats.MsgId(dedupID))
	if err != nil {
		return false, err
	}
	return ack.Duplicate, nil
}

func (s *jetstreamStore) Replay(subject string) ([]Event, error) {
	// Empty durable name → an ephemeral consumer: a throwaway one-shot reader to drain the
	// subject for this replay, leaving no durable consumer state on the server.
	sub, err := s.js.PullSubscribe(subject, "", nats.DeliverAll(), nats.AckNone())
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	var out []Event
	for {
		msgs, ferr := sub.Fetch(128, nats.MaxWait(250*time.Millisecond))
		if errors.Is(ferr, nats.ErrTimeout) || (ferr == nil && len(msgs) == 0) {
			break
		}
		if ferr != nil {
			return nil, ferr // fail closed — never return partial state
		}
		for _, m := range msgs {
			var e Event
			if err := json.Unmarshal(m.Data, &e); err != nil {
				return nil, fmt.Errorf("replay %s: corrupt fact: %w", subject, err) // fail closed
			}
			out = append(out, e)
		}
	}
	return out, nil
}

func EnsureLogStream(js nats.JetStreamContext) error {
	cfg := &nats.StreamConfig{
		Name:              "INTERACTION_LOGS",
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

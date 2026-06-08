package signaling

import "github.com/nats-io/nats.go"

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
		Duplicates:        0,
	}
	if _, err := js.AddStream(cfg); err != nil {
		// already exists with a compatible config → update is idempotent
		if _, uerr := js.UpdateStream(cfg); uerr != nil {
			return err
		}
	}
	return nil
}

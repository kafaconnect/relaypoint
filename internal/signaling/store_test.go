package signaling

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

// @spec:signaling.stream.retention-ceiling
func TestLogStreamRetentionCeiling(t *testing.T) {
	cfg := logStreamConfig()
	if cfg.MaxAge <= 0 {
		t.Error("INTERACTION_LOGS must carry a MaxAge ceiling (operability backstop)")
	}
	if cfg.MaxBytes <= 0 {
		t.Error("INTERACTION_LOGS must carry a MaxBytes ceiling (operability backstop)")
	}
	// Per-subject discard would silently drop an open interaction's HEAD facts and corrupt OCC/replay.
	if cfg.MaxMsgsPerSubject != -1 {
		t.Errorf("MaxMsgsPerSubject = %d, want -1 (unlimited; per-subject cap forbidden on the source-of-truth log)", cfg.MaxMsgsPerSubject)
	}
	if cfg.DiscardNewPerSubject {
		t.Error("DiscardNewPerSubject must never be enabled on INTERACTION_LOGS")
	}
	if cfg.Discard != nats.DiscardOld {
		t.Errorf("Discard = %v, want DiscardOld", cfg.Discard)
	}
}

// @spec:signaling.stream.retention-ceiling
func TestEnsureStreamReturnsUpdateErrWhenBothFail(t *testing.T) {
	addErr := errors.New("add failed")
	updErr := errors.New("update failed")
	add := func(*nats.StreamConfig, ...nats.JSOpt) (*nats.StreamInfo, error) { return nil, addErr }
	upd := func(*nats.StreamConfig, ...nats.JSOpt) (*nats.StreamInfo, error) { return nil, updErr }

	got := ensureStream(add, upd, logStreamConfig())
	if !errors.Is(got, updErr) {
		t.Fatalf("both-fail err = %v, want the UpdateStream err (it describes why the EXISTING stream couldn't be reconciled)", got)
	}
}

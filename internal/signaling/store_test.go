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
	if cfg.MaxBytes != -1 {
		t.Errorf("MaxBytes = %d, want -1 (account-bounded; the byte ceiling is the JetStream account max_storage, not a per-stream cap that would exceed or over-reserve a shared account)", cfg.MaxBytes)
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

// @spec:signaling.stream.dedup-window
func TestLogStreamDedupWindow(t *testing.T) {
	cfg := logStreamConfig()
	// The in-memory results cache is bounded (RH-07), so the BROKER dedup window is the durable
	// exactly-once authority for a retried committed command_id; it must be set (not the 2-min default).
	if cfg.Duplicates != logStreamDedupWindow {
		t.Errorf("Duplicates = %v, want %v (the exactly-once boundary)", cfg.Duplicates, logStreamDedupWindow)
	}
	if cfg.Duplicates <= 0 {
		t.Error("INTERACTION_LOGS must carry a non-zero Duplicates window (broker exactly-once authority)")
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

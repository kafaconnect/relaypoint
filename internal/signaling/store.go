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

// Replay drains up to the subject's last STREAM seq (GetLastMsg) and re-checks it, so an empty subject
// returns before any consumer and a drained one stops the instant it has read the last fact — neither
// pays the ~250ms MaxWait tail the old Fetch(128, 250ms) bled on every first-access/OCC-conflict/dup
// rebuild (RH-07). GetLastMsg reads the live store (no lag), so the re-check both catches a straggler
// that landed mid-drain and gives a current OCC token under concurrent appends. replayFetchMaxWait is
// only a safety bound on a hung server — every productive fetch returns immediately.
const (
	replayFetchBatch   = 256
	replayFetchMaxWait = 2 * time.Second
)

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
	var out []*Event
	var lastSubjSeq uint64
	var sub *nats.Subscription
	for {
		last, err := s.js.GetLastMsg(LogStreamName, subject)
		if err != nil {
			if errors.Is(err, nats.ErrMsgNotFound) {
				return out, lastSubjSeq, nil // never-written/drained subject: return immediately, no consumer, no MaxWait tail
			}
			return nil, 0, err
		}
		if lastSubjSeq >= last.Sequence {
			return out, lastSubjSeq, nil // caught up to the live last seq — nothing new landed mid-drain
		}
		if sub == nil {
			// Empty durable name → an ephemeral consumer: a throwaway reader that leaves no durable consumer state on the server.
			sub, err = s.js.PullSubscribe(subject, "", nats.DeliverAll(), nats.AckNone())
			if err != nil {
				return nil, 0, err
			}
			defer sub.Unsubscribe()
		}
		for lastSubjSeq < last.Sequence {
			msgs, ferr := sub.Fetch(replayFetchBatch, nats.MaxWait(replayFetchMaxWait))
			if ferr != nil {
				return nil, 0, ferr // fail closed — never return partial state
			}
			if len(msgs) == 0 {
				return nil, 0, fmt.Errorf("replay %s: drained before target seq %d (got %d facts)", subject, last.Sequence, len(out))
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
	}
}

// LogStreamName is the JetStream stream capturing every `tenant.*.interaction.*.log` fact; exported so least-privilege callers (e.g. the auth-callout trusted-backend JS.API grant, RH-08) scope to exactly this stream, not account-wide `$JS.API.>`.
const LogStreamName = "INTERACTION_LOGS"

// Operability ceiling (RH-11c, revised at deploy): MaxAge bounds the `.log` by TIME. The BYTE ceiling
// is the JetStream ACCOUNT max_storage (infra-provisioned), NOT a per-stream MaxBytes — on a shared
// account a per-stream MaxBytes above the account is rejected ("insufficient storage"), and one within
// it would reserve the whole pool and starve the co-tenant streams (which are all -1/account-bounded).
// A monitor MUST alert on account storage; the alert — not a hard per-stream byte cap — is the real
// safeguard (DiscardOld is a last resort that corrupts an open interaction's replay by dropping its head).
const logStreamMaxAge = 365 * 24 * time.Hour // > any realistic open-interaction lifetime

// logStreamDedupWindow is the EXACTLY-ONCE BOUNDARY for an interaction `.log` append: JetStream rejects
// a duplicate MsgId (tenant.iid.command_id) seen within it. RH-07 bounded the in-memory `results` cache,
// so once that fast-path entry is evicted the broker window is the SOLE authority that stops a retried
// ACCEPTED command_id (still legal in the current state, on a >maxResults-long interaction) double-
// appending. Hence a window far wider than any realistic client/reconnect retry, not the NATS 2-min
// default which a long interaction's eviction could outlive. 1h >> any reconnect/backoff burst and the
// per-stream dedup index stays memory-bounded. Unbounded per-command dedup is a deferred future option;
// this window is the current guarantee boundary (defaulted in code, never env — RH-11c convention).
const logStreamDedupWindow = time.Hour

func logStreamConfig() *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:      LogStreamName,
		Subjects:  []string{"tenant.*.interaction.*.log"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardOld, // whole-stream, NEVER DiscardNewPerSubject: per-subject discard would drop an open interaction's head facts
		MaxAge:    logStreamMaxAge,
		// account-bounded (-1): the byte ceiling is the JetStream account max_storage, not a per-stream cap — a per-stream cap above a shared account is rejected (RH-11c, revised at deploy).
		MaxBytes: -1,
		// Broker is the durable exactly-once authority within this window; the in-memory results cache is a bounded fast-path that may evict sooner (RH-07).
		Duplicates: logStreamDedupWindow,
		// -1 = unlimited per subject: a single interaction's `.log` is never capped (its head must survive for replay)
		MaxMsgsPerSubject: -1,
	}
}

type addUpdateStreamFn func(*nats.StreamConfig, ...nats.JSOpt) (*nats.StreamInfo, error)

// ensureStream creates-or-reconfigures the stream; when BOTH the create and the reconcile fail it
// returns the UpdateStream error — that is the one describing why the EXISTING stream couldn't be
// brought to spec, the actionable failure (a create error on an already-present stream is just "exists").
func ensureStream(add, update addUpdateStreamFn, cfg *nats.StreamConfig) error {
	if _, err := add(cfg); err != nil {
		if _, uerr := update(cfg); uerr != nil {
			return uerr
		}
	}
	return nil
}

func EnsureLogStream(js nats.JetStreamContext) error {
	return ensureStream(js.AddStream, js.UpdateStream, logStreamConfig())
}

// ResetLogStream is the ADR-0002 protobuf-cutover step: deletes+recreates INTERACTION_LOGS so no JSON-era fact survives (a protobuf router unmarshalling a JSON fact bricks that interaction); destructive dev-only, gated by RP_RESET_LOG_STREAM.
func ResetLogStream(js nats.JetStreamContext) error {
	if err := js.DeleteStream(LogStreamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err := js.AddStream(logStreamConfig())
	return err
}

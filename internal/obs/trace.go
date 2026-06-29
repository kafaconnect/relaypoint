package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Mirrors an OTel SpanContext (ADR-0011 §3) so the real SDK can replace this hand-rolled type behind StartSpan without touching call-sites.
type TraceContext struct {
	TraceID string
	SpanID  string
	Sampled bool
}

const (
	zeroTraceID = "00000000000000000000000000000000"
	zeroSpanID  = "0000000000000000"
	// maxTraceparentLen bounds an untrusted inbound header (version-traceid-spanid-flags).
	maxTraceparentLen = 55
)

// Trust boundary (ADR-0011 §8): on anything malformed/oversized/all-zero the caller MUST treat the input as absent and never reuse or echo the raw value (forecloses log injection).
func ParseTraceparent(raw string) (TraceContext, bool) {
	if len(raw) == 0 || len(raw) > maxTraceparentLen {
		return TraceContext{}, false
	}
	if len(raw) != 55 || raw[2] != '-' || raw[35] != '-' || raw[52] != '-' {
		return TraceContext{}, false
	}
	version, traceID, spanID, flags := raw[0:2], raw[3:35], raw[36:52], raw[53:55]
	if version != "00" || !isHex(version) || !isHex(traceID) || !isHex(spanID) || !isHex(flags) {
		return TraceContext{}, false
	}
	if traceID == zeroTraceID || spanID == zeroSpanID {
		return TraceContext{}, false
	}
	return TraceContext{TraceID: traceID, SpanID: spanID, Sampled: flags == "01"}, true
}

func (tc TraceContext) Traceparent() string {
	flags := "00"
	if tc.Sampled {
		flags = "01"
	}
	return "00-" + tc.TraceID + "-" + tc.SpanID + "-" + flags
}

// Valid inbound: reuse the trace id with a fresh child span id (the inbound span is the parent, never ours); else a brand-new trace. Never fails, never reuses an unvalidated value (ADR-0011 §8/§9).
func FromInbound(raw string) TraceContext {
	if parent, ok := ParseTraceparent(raw); ok {
		return TraceContext{TraceID: parent.TraceID, SpanID: newSpanID(), Sampled: parent.Sampled}
	}
	return TraceContext{TraceID: newTraceID(), SpanID: newSpanID(), Sampled: true}
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// crypto/rand-backed; on the implausible read failure we force a non-zero byte rather than panic (fail-open), so we never emit a W3C-invalid all-zero id.
func newTraceID() string { return randHex(16) }
func newSpanID() string  { return randHex(8) }

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		b[0] = 1
	}
	if allZero(b) {
		b[0] = 1
	}
	return hex.EncodeToString(b)
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

type traceKey struct{}

func WithTrace(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceKey{}, tc)
}

func TraceFromContext(ctx context.Context) (TraceContext, bool) {
	tc, ok := ctx.Value(traceKey{}).(TraceContext)
	return tc, ok && tc.TraceID != ""
}

// Returns an idempotent end the caller MUST defer: once the OTLP exporter lands behind this seam a missed end leaks a span, so deferring is the contract now (ADR-0011 §10).
func StartSpan(ctx context.Context, name string) (context.Context, func()) {
	if tracer != nil {
		return startOTelSpan(ctx, name)
	}
	parent, ok := TraceFromContext(ctx)
	traceID := parent.TraceID
	if !ok || traceID == "" {
		traceID = newTraceID()
	}
	child := TraceContext{TraceID: traceID, SpanID: newSpanID(), Sampled: parent.Sampled || !ok}
	ctx = WithTrace(ctx, child)
	start := time.Now()
	done := false
	end := func() {
		if done {
			return
		}
		done = true
		Logger(ctx).Debug("span.end", "span_name", name, "dur_ms", time.Since(start).Milliseconds())
	}
	return ctx, end
}

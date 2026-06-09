package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// TraceContext mirrors the shape of an OpenTelemetry SpanContext (ADR-0011 §3) so the
// real OTel SDK can replace this hand-rolled type later behind StartSpan without touching
// call-sites. It carries only what the W3C `traceparent` wire format needs.
type TraceContext struct {
	TraceID string // 32 lowercase hex
	SpanID  string // 16 lowercase hex
	Sampled bool
}

const (
	zeroTraceID = "00000000000000000000000000000000"
	zeroSpanID  = "0000000000000000"
	// maxTraceparentLen bounds an untrusted inbound header (version-traceid-spanid-flags).
	maxTraceparentLen = 55
)

// ParseTraceparent validates an untrusted inbound `traceparent` against the W3C format
// (version 00, non-zero 32-hex trace id, non-zero 16-hex span id, 2-hex flags) and returns
// it. ok is false for anything malformed, oversized, or all-zero — the caller MUST then
// treat the input as absent and generate a fresh context (never reuse or echo the raw
// value). This is the trust boundary that forecloses log injection (ADR-0011 §8).
func ParseTraceparent(raw string) (TraceContext, bool) {
	if len(raw) == 0 || len(raw) > maxTraceparentLen {
		return TraceContext{}, false
	}
	// version(2) "-" trace(32) "-" span(16) "-" flags(2)
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

// Traceparent renders the context back to the W3C wire format.
func (tc TraceContext) Traceparent() string {
	flags := "00"
	if tc.Sampled {
		flags = "01"
	}
	return "00-" + tc.TraceID + "-" + tc.SpanID + "-" + flags
}

// FromInbound builds the trace context for a request: a VALID inbound `traceparent` has its
// trace id reused with a freshly minted child span id (an inbound span id is the parent,
// never our own); anything else yields a brand-new sampled trace. It never fails and never
// reuses an unvalidated value (ADR-0011 §8, §9 fail-open).
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

// newTraceID / newSpanID draw from crypto/rand; each span id is unique by construction
// (ADR-0011 §3). A crypto/rand read failure is implausible, but we never panic the request
// path over telemetry (fail-open) — and we guarantee a non-zero id so we can never emit a
// W3C-invalid all-zero traceparent: on the impossible failure path one bit is forced on.
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

// WithTrace stores the trace context for the request.
func WithTrace(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceKey{}, tc)
}

// TraceFromContext returns the request's trace context, ok=false if none is bound.
func TraceFromContext(ctx context.Context) (TraceContext, bool) {
	tc, ok := ctx.Value(traceKey{}).(TraceContext)
	return tc, ok && tc.TraceID != ""
}

// StartSpan opens a child span under the context's trace (or a fresh trace if none) and
// returns the child context plus an idempotent end function the caller MUST defer. Today it
// only mints a child span id (no exporter); once the OTLP exporter lands behind this seam a
// missed end leaks an unfinished span — hence "defer it" is the contract now (ADR-0011 §10).
func StartSpan(ctx context.Context, name string) (context.Context, func()) {
	parent, ok := TraceFromContext(ctx)
	traceID := parent.TraceID
	if !ok || traceID == "" {
		traceID = newTraceID()
	}
	child := TraceContext{TraceID: traceID, SpanID: newSpanID(), Sampled: parent.Sampled || !ok}
	ctx = WithTrace(ctx, child)
	start := time.Now()
	done := false
	// Idempotent per the documented single-goroutine defer contract (ADR-0011 §10): the
	// caller MUST `defer endFn()`; a second call is a safe no-op.
	end := func() {
		if done {
			return
		}
		done = true
		Logger(ctx).Debug("span.end", "span_name", name, "dur_ms", time.Since(start).Milliseconds())
	}
	return ctx, end
}

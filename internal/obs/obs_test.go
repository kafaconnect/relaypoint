package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// decodes the single JSON log line in buf.
func lastLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &m); err != nil {
		t.Fatalf("log line is not JSON: %v\n%q", err, lines[len(lines)-1])
	}
	return m
}

// @spec:obs.cross-module-golden-vector
func TestGoldenVector(t *testing.T) {
	raw, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g struct {
		Traceparent      string            `json:"traceparent"`
		TraceID          string            `json:"trace_id"`
		SpanID           string            `json:"span_id"`
		Sampled          bool              `json:"sampled"`
		CanonicalLogKeys []string          `json:"canonical_log_keys"`
		CanonicalLog     map[string]string `json:"canonical_log"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	tc, ok := ParseTraceparent(g.Traceparent)
	if !ok || tc.TraceID != g.TraceID || tc.SpanID != g.SpanID || tc.Sampled != g.Sampled {
		t.Fatalf("golden parse mismatch: %+v ok=%v", tc, ok)
	}
	if got := tc.Traceparent(); got != g.Traceparent {
		t.Fatalf("traceparent round-trip: got %q want %q", got, g.Traceparent)
	}

	// Emit the canonical log line through THIS module's obs and assert it matches the golden
	// object byte-for-byte (modulo the dynamic ts). The desk twin runs the same check against
	// the same file — any field rename/level-case drift fails in one of the two.
	t.Setenv("OBS_ENV", g.CanonicalLog["env"])
	var lbuf bytes.Buffer
	newJSON(g.CanonicalLog["service"], &lbuf).With(
		keyTraceID, g.CanonicalLog["trace_id"],
		keySpanID, g.CanonicalLog["span_id"],
		keyRequestID, g.CanonicalLog["request_id"],
		keyTenantID, g.CanonicalLog["tenant_id"],
		keyUser, g.CanonicalLog["user"],
	).Info(g.CanonicalLog["msg"])
	got := lastLine(t, &lbuf)
	delete(got, "ts")
	if len(got) != len(g.CanonicalLog) {
		t.Errorf("canonical log key set drift: got %v want %v", got, g.CanonicalLog)
	}
	for k, want := range g.CanonicalLog {
		if gv, _ := got[k].(string); gv != want {
			t.Errorf("canonical log %q = %q, want %q", k, got[k], want)
		}
	}
	want := map[string]bool{
		keyService: true, keyEnv: true, keyTraceID: true, keySpanID: true,
		keyRequestID: true, keyTenantID: true, keyUser: true,
		"ts": true, "level": true, "msg": true,
	}
	for _, k := range g.CanonicalLogKeys {
		if !want[k] {
			t.Errorf("golden key %q not in canonical schema", k)
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Errorf("canonical keys missing from golden vector: %v", want)
	}
}

// @spec:obs.log-is-json-schema
func TestLogIsJSONSchema(t *testing.T) {
	var buf bytes.Buffer
	newJSON("relaypoint-router", &buf).Info("router.up")
	m := lastLine(t, &buf)
	for _, k := range []string{"ts", "level", "msg", keyService, keyEnv} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing canonical field %q in %v", k, m)
		}
	}
	if m["level"] != "info" {
		t.Errorf("level should be lowercase per the canonical schema, got %v", m["level"])
	}
	if msg, _ := m["msg"].(string); strings.ContainsAny(msg, " ") {
		t.Errorf("msg should be a static id-free token, got %q", msg)
	}
}

// @spec:obs.label-cardinality-split
func TestLabelCardinalitySplit(t *testing.T) {
	var buf bytes.Buffer
	ctx := WithRequestID(WithTrace(context.Background(),
		TraceContext{TraceID: "t", SpanID: "s"}), "req-1")
	ctx = WithCorrelation(ctx, newJSON("relaypoint-router", &buf))
	Logger(ctx).With(keyTenantID, "tnt", keyUser, "usr").Info("x")
	m := lastLine(t, &buf)
	// low-cardinality label-eligible fields + high-cardinality body ids all present
	for _, k := range []string{keyService, keyEnv, keyTenantID, keyTraceID, keyRequestID, keyUser} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing %q", k)
		}
	}
}

// @spec:obs.traceparent-malformed-treated-as-absent
func TestTraceparentMalformedTreatedAsAbsent(t *testing.T) {
	for _, bad := range []string{
		"garbage",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",    // missing flags
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01", // zero trace
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // zero span
		strings.Repeat("0", 200),                                  // oversized
		"99-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // bad version
	} {
		if _, ok := ParseTraceparent(bad); ok {
			t.Errorf("malformed traceparent accepted: %q", bad)
		}
	}
	// a fresh trace is generated; the raw bad value is never reused or echoed into a log field
	ctx := ContextFromTraceparent(context.Background(), "garbage")
	tc, ok := TraceFromContext(ctx)
	if !ok {
		t.Fatal("malformed inbound should still yield a usable trace")
	}
	if strings.Contains(tc.TraceID, "garbage") {
		t.Error("raw inbound value reached the trace id")
	}
}

// @spec:obs.logger-fallback
func TestLoggerFallback(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Logger panicked on a bare context: %v", r)
		}
	}()
	if Logger(context.Background()) == nil {
		t.Fatal("fallback logger is nil")
	}
	Logger(context.Background()).Info("no panic")
}

// @spec:obs.span-end-idempotent
func TestSpanEndIdempotent(t *testing.T) {
	parent := TraceContext{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7"}
	ctx, end := StartSpan(WithTrace(context.Background(), parent), "work")
	child, _ := TraceFromContext(ctx)
	if child.TraceID != parent.TraceID {
		t.Error("child span left the parent trace")
	}
	if child.SpanID == parent.SpanID {
		t.Error("child span id should differ from parent")
	}
	end()
	end() // second call must be a safe no-op
}

// @spec:obs.nats-traceparent-propagated
func TestNATSTraceparentPropagated(t *testing.T) {
	pubCtx := WithTrace(context.Background(),
		TraceContext{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7", Sampled: true})
	hdr := map[string]string{}
	InjectTraceparent(pubCtx, func(k, v string) { hdr[k] = v })
	if hdr["traceparent"] == "" {
		t.Fatal("publisher set no traceparent header")
	}
	// subscriber side
	subCtx := ContextFromTraceparent(context.Background(), hdr["traceparent"])
	sub, ok := TraceFromContext(subCtx)
	if !ok || sub.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("subscriber trace id = %q", sub.TraceID)
	}
	if sub.SpanID == "00f067aa0ba902b7" {
		t.Error("subscriber should mint a child span, not reuse the publisher span")
	}
	// missing header → fresh trace, not a drop
	if _, ok := TraceFromContext(ContextFromTraceparent(context.Background(), "")); !ok {
		t.Error("missing header should still yield a usable trace")
	}
}

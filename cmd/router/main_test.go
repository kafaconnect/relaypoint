package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/kafaconnect/relaypoint/internal/obs"
)

// replays the QueueSubscribe handler's context plumbing so propagation is tested without a live NATS connection.
func seedCtx(m *nats.Msg, base *slog.Logger) context.Context {
	ctx := obs.ContextFromTraceparent(context.Background(), traceparentOf(m))
	return obs.WithCorrelation(ctx, base)
}

// @spec:obs.nats-traceparent-propagated (subscribe side)
func TestSubscribeTraceparentPropagated(t *testing.T) {
	const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var buf bytes.Buffer
	m := nats.NewMsg("tenant.t1.interaction.iX.cmd.u1")
	m.Header.Set("traceparent", inbound)
	ctx := seedCtx(m, captureLogger(&buf))

	tc, ok := obs.TraceFromContext(ctx)
	if !ok || tc.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("handler ctx trace id = %q (want the publisher's)", tc.TraceID)
	}
	if tc.SpanID == "00f067aa0ba902b7" {
		t.Error("handler should mint a child span, not reuse the publisher span")
	}

	obs.Logger(ctx).Info("router.command")
	if got := lineField(t, &buf, "trace_id"); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("log trace_id = %q, want the publisher's", got)
	}
}

// @spec:obs.nats-traceparent-propagated (missing header → fresh trace, not a drop)
func TestSubscribeMissingHeaderFreshTrace(t *testing.T) {
	var buf bytes.Buffer
	m := nats.NewMsg("tenant.t1.interaction.iX.cmd.u1")
	ctx := seedCtx(m, captureLogger(&buf))

	tc, ok := obs.TraceFromContext(ctx)
	if !ok {
		t.Fatal("missing header must still yield a usable trace, not a drop")
	}
	if len(tc.TraceID) != 32 || len(tc.SpanID) != 16 {
		t.Fatalf("fresh trace ids malformed: trace=%q span=%q", tc.TraceID, tc.SpanID)
	}

	obs.Logger(ctx).Info("router.command")
	if got := lineField(t, &buf, "trace_id"); len(got) != 32 {
		t.Errorf("log trace_id = %q, want a fresh 32-hex id", got)
	}
}

func TestTraceparentOfNilHeader(t *testing.T) {
	if got := traceparentOf(&nats.Msg{}); got != "" {
		t.Errorf("traceparentOf(nil header) = %q, want empty", got)
	}
}

func captureLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, nil))
}

func lineField(t *testing.T, buf *bytes.Buffer, key string) string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &m); err != nil {
		t.Fatalf("log line not JSON: %v", err)
	}
	s, _ := m[key].(string)
	return s
}

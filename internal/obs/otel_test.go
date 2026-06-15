package obs

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// @spec:obs.otlp-exporter-behind-seam
func TestStartSpanLogOnlyWhenNoExporter(t *testing.T) {
	tracer = nil
	ctx, end := StartSpan(context.Background(), "op")
	tc, ok := TraceFromContext(ctx)
	if !ok || tc.TraceID == "" {
		t.Fatal("expected a bound trace context even with no exporter")
	}
	end()
	end()
}

// @spec:obs.otlp-exporter-behind-seam
// @spec:obs.trace-spans-nats-hops
func TestStartSpanExportsAndPreservesTraceID(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := tracer
	tracer = tp.Tracer("test")
	t.Cleanup(func() { tracer = prev; _ = tp.Shutdown(context.Background()) })

	// A trace seeded from an inbound traceparent (a .cmd / .log NATS hop): the exported span must
	// CONTINUE that trace_id, and the returned context's TraceContext must match the exported span.
	parent := TraceContext{TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdef", Sampled: true}
	ctx := WithTrace(context.Background(), parent)
	ctx, end := StartSpan(ctx, "router")
	end()

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	got := spans[0].SpanContext()
	if got.TraceID().String() != parent.TraceID {
		t.Fatalf("exported trace_id=%s, want %s (cross-hop continuity)", got.TraceID(), parent.TraceID)
	}
	tc, _ := TraceFromContext(ctx)
	if tc.TraceID != got.TraceID().String() || tc.SpanID != got.SpanID().String() {
		t.Fatalf("ctx trace %s/%s != exported %s/%s", tc.TraceID, tc.SpanID, got.TraceID(), got.SpanID())
	}
}

// @spec:obs.trace-spans-nats-hops
func TestStartSpanLocalChildIsLocalNotRemote(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := tracer
	tracer = tp.Tracer("test")
	t.Cleanup(func() { tracer = prev; _ = tp.Shutdown(context.Background()) })

	ctx, end1 := StartSpan(context.Background(), "parent")
	_, end2 := StartSpan(ctx, "child")
	end2()
	end1()

	var child, parent interface {
		Parent() oteltrace.SpanContext
		SpanContext() oteltrace.SpanContext
		Name() string
	}
	for _, s := range rec.Ended() {
		switch s.Name() {
		case "child":
			child = s
		case "parent":
			parent = s
		}
	}
	if child == nil || parent == nil {
		t.Fatalf("missing spans (parent=%v child=%v)", parent != nil, child != nil)
	}
	if child.Parent().IsRemote() {
		t.Fatal("a local child span's parent must NOT be marked remote")
	}
	if child.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatal("the local child's parent should be the local parent span")
	}
	if child.SpanContext().TraceID() != parent.SpanContext().TraceID() {
		t.Fatal("local child must share the parent's trace_id")
	}
}

// @spec:obs.sampling-config
func TestSamplerRatioFromEnv(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	if r := samplerRatio(); r != 0.25 {
		t.Fatalf("samplerRatio = %v, want 0.25", r)
	}
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "bogus")
	if r := samplerRatio(); r != 1.0 {
		t.Fatalf("samplerRatio(bogus) = %v, want 1.0 default", r)
	}
}

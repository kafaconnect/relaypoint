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

// @spec:obs.otlp.export-wired
// InitTracer wires the OTLP exporter ONLY when an endpoint is configured; a span seeded by an inbound
// traceparent continues that trace_id either way (exported when wired, log-only when not).
func TestInitTracerWiresExportWhenEndpointSet(t *testing.T) {
	const parentTrace = "0123456789abcdef0123456789abcdef"
	const tp = "00-" + parentTrace + "-0123456789abcdef-01"

	// unset endpoint → exporter stays dormant, span path is log-only but still continues the trace id.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	tracer = nil
	offShutdown, err := InitTracer(context.Background(), "rp-test-off")
	if err != nil {
		t.Fatalf("InitTracer(endpoint unset) err = %v", err)
	}
	if tracer != nil {
		t.Fatal("no endpoint must leave the exporter dormant (tracer nil ⇒ log-only)")
	}
	offCtx, offEnd := StartSpan(ContextFromTraceparent(context.Background(), tp), "router")
	offEnd()
	if tc, _ := TraceFromContext(offCtx); tc.TraceID != parentTrace {
		t.Fatalf("log-only span dropped inbound continuity: trace_id=%s, want %s", tc.TraceID, parentTrace)
	}
	_ = offShutdown(context.Background())

	// endpoint set → exporter wired; a span under the inbound traceparent continues that trace id.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	onShutdown, err := InitTracer(context.Background(), "rp-test-on")
	if err != nil {
		t.Fatalf("InitTracer(endpoint set) err = %v", err)
	}
	t.Cleanup(func() {
		tracer = nil
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = onShutdown(cctx)
	})
	if tracer == nil {
		t.Fatal("a configured endpoint must wire the exporter (tracer non-nil)")
	}
	onCtx, onEnd := StartSpan(ContextFromTraceparent(context.Background(), tp), "router")
	onEnd()
	if tc, _ := TraceFromContext(onCtx); tc.TraceID != parentTrace {
		t.Fatalf("exported span broke the desk→RP waterfall: trace_id=%s, want %s", tc.TraceID, parentTrace)
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

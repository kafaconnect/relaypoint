package obs

import (
	"context"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// tracer is the OTel tracer wired by InitTracer. nil ⇒ no exporter configured ⇒ StartSpan stays
// log-only (the ADR-0011 §9 fail-open default). Set once at startup, read-only thereafter.
//
// This file is the relaypoint twin of desk's internal/obs/otel.go (ADR-0011 keeps the two obs
// copies in lockstep). Same fail-open + trace_id-continuity behaviour.
var tracer oteltrace.Tracer

// InitTracer wires an OTLP (gRPC) span exporter + TracerProvider from the environment and returns a
// shutdown func to flush on exit. It is a NO-OP when OTEL_EXPORTER_OTLP_ENDPOINT is unset — it leaves
// StartSpan log-only, so telemetry is never required for the service to run (fail-open). The exporter
// honors the standard OTEL_EXPORTER_OTLP_* env. OTEL_TRACES_SAMPLER_ARG (0..1, default 1.0) sets a
// parent-based ratio sampler so a sampled inbound trace keeps the whole cross-hop trace sampled.
func InitTracer(ctx context.Context, service string) (func(context.Context) error, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", service),
		attribute.String("deployment.environment", env()),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(samplerRatio()))),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer(service)
	return tp.Shutdown, nil
}

func samplerRatio() float64 {
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			return f
		}
	}
	return 1.0
}

// startOTelSpan is the StartSpan path when an exporter is wired. It continues the request's trace and
// stitches the span's ids back into a fresh TraceContext so logs/wire/export all agree. It seeds a
// REMOTE parent ONLY when the context holds no OTel span (a trace seeded from an inbound traceparent
// across a process/NATS boundary); local nesting uses the in-context span as a LOCAL parent.
func startOTelSpan(ctx context.Context, name string) (context.Context, func()) {
	if !oteltrace.SpanFromContext(ctx).SpanContext().IsValid() {
		if parent, ok := TraceFromContext(ctx); ok {
			if sc := otelSpanContext(parent); sc.IsValid() {
				ctx = oteltrace.ContextWithRemoteSpanContext(ctx, sc)
			}
		}
	}
	ctx, span := tracer.Start(ctx, name)
	sc := span.SpanContext()
	ctx = WithTrace(ctx, TraceContext{
		TraceID: sc.TraceID().String(),
		SpanID:  sc.SpanID().String(),
		Sampled: sc.IsSampled(),
	})
	start := time.Now()
	done := false
	end := func() {
		if done {
			return
		}
		done = true
		span.End()
		Logger(ctx).Debug("span.end", "span_name", name, "dur_ms", time.Since(start).Milliseconds())
	}
	return ctx, end
}

// otelSpanContext maps our hand-rolled TraceContext onto an OTel (remote) SpanContext so a span can
// continue an existing trace; an unparseable id yields an invalid context (caller falls back to a
// fresh root span).
func otelSpanContext(tc TraceContext) oteltrace.SpanContext {
	tid, err := oteltrace.TraceIDFromHex(tc.TraceID)
	if err != nil {
		return oteltrace.SpanContext{}
	}
	sid, err := oteltrace.SpanIDFromHex(tc.SpanID)
	if err != nil {
		return oteltrace.SpanContext{}
	}
	var flags oteltrace.TraceFlags
	if tc.Sampled {
		flags = oteltrace.FlagsSampled
	}
	return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: flags,
		Remote:     true,
	})
}

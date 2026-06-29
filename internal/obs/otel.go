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

// nil tracer ⇒ no exporter ⇒ StartSpan stays log-only (ADR-0011 §9 fail-open); set once at startup, read-only after.
var tracer oteltrace.Tracer

// NO-OP when OTEL_EXPORTER_OTLP_ENDPOINT is unset (fail-open: telemetry is never required to run); the parent-based ratio sampler keeps a sampled inbound trace sampled across hops.
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

// Stitches the span's ids back into TraceContext so logs/wire/export agree; seeds a REMOTE parent ONLY when the context holds no OTel span (inbound cross-boundary trace), else nests as a LOCAL parent.
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

// An unparseable id yields an invalid SpanContext, so the caller falls back to a fresh root span.
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

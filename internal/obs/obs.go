// Package obs is the shared logging + trace-correlation foundation (ADR-0011); it fails open (telemetry never breaks the request path) and stays in lockstep with its twin via testdata/golden.json.
package obs

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

// Canonical field keys (ADR-0011 §2); low-cardinality ones are Loki-label-eligible, high-cardinality ids stay in the body.
const (
	keyService   = "service"
	keyEnv       = "env"
	keyTraceID   = "trace_id"
	keySpanID    = "span_id"
	keyRequestID = "request_id"
	keyTenantID  = "tenant_id"
	keyUser      = "user"
)

func New(service string) *slog.Logger { return newJSON(service, os.Stdout) }

func newJSON(service string, w io.Writer) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) != 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				return slog.String("ts", a.Value.Time().UTC().Format(time.RFC3339Nano))
			case slog.LevelKey:
				// canonical schema uses lowercase levels (ADR-0011 §2): debug/info/warn/error
				return slog.String(slog.LevelKey, strings.ToLower(a.Value.String()))
			}
			return a
		},
	})
	return slog.New(h).With(keyService, service, keyEnv, env())
}

func env() string {
	if v := os.Getenv("OBS_ENV"); v != "" {
		return v
	}
	return "dev"
}

type loggerKey struct{}
type requestIDKey struct{}

func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// Falls back to slog.Default when none is bound — never nil, never a panic (ADR-0011 §9).
func Logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// The caller supplies the setter so obs takes no transport dependency (e.g. nats.go) — loose coupling.
func InjectTraceparent(ctx context.Context, set func(key, value string)) {
	if tc, ok := TraceFromContext(ctx); ok {
		set("traceparent", tc.Traceparent())
	}
}

func ContextFromTraceparent(ctx context.Context, raw string) context.Context {
	return WithTrace(ctx, FromInbound(raw))
}

func WithCorrelation(ctx context.Context, base *slog.Logger) context.Context {
	l := base
	if tc, ok := TraceFromContext(ctx); ok {
		l = l.With(keyTraceID, tc.TraceID, keySpanID, tc.SpanID)
	}
	if id := RequestID(ctx); id != "" {
		l = l.With(keyRequestID, id)
	}
	return WithLogger(ctx, l)
}

// Package obs is the shared logging + trace-correlation foundation (ADR-0011). It emits one
// JSON object per line on stdout with the canonical field schema, carries a W3C traceparent
// across HTTP and NATS so a request is followable end-to-end, and fails open — a telemetry
// failure never breaks the request path. It is the ONLY place log/trace setup lives; no
// service logs via fmt.Print*/log.Print*.
//
// Cross-module note: relaypoint carries its own conforming copy (the contract is ADR-0011 +
// the field schema + testdata/golden.json, not an imported package — the modules are split
// per ADR-0003). Keep this package and its relaypoint twin in lockstep via the golden vector.
package obs

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

// Canonical field keys (ADR-0011 §2). Low-cardinality (service, level, env, tenant_id) are
// Loki-label-eligible; high-cardinality ids (trace_id, request_id, user) stay in the body.
const (
	keyService   = "service"
	keyEnv       = "env"
	keyTraceID   = "trace_id"
	keySpanID    = "span_id"
	keyRequestID = "request_id"
	keyTenantID  = "tenant_id"
	keyUser      = "user"
)

// New builds the process base logger: JSON to stdout, `ts` (RFC3339Nano UTC) / `level` /
// `msg` plus the constant `service` and `env`. Wire it once with slog.SetDefault(New(...)).
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

// WithLogger binds a request-scoped logger (pre-bound with correlation fields) to the context.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// Logger returns the context's request-scoped logger, or the process base logger
// (slog.Default) when none is bound — never nil, never a panic (ADR-0011 §9). Correlation
// fields are simply absent on lines logged through the fallback.
func Logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

// WithRequestID stores the (chi) request id; surfaced as the request_id log field.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the bound request id, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// InjectTraceparent calls set("traceparent", ...) with the context's trace, if any. The
// caller supplies the setter so obs stays free of any transport dependency (e.g. nats.go) —
// loose coupling. A no-op when no trace is bound.
func InjectTraceparent(ctx context.Context, set func(key, value string)) {
	if tc, ok := TraceFromContext(ctx); ok {
		set("traceparent", tc.Traceparent())
	}
}

// ContextFromTraceparent seeds a context's trace from an inbound (untrusted) traceparent —
// reusing a valid one, generating fresh otherwise (missing header included). Use it on the
// consuming side of a transport (e.g. a NATS subscriber) so its logs share the publisher's
// trace_id.
func ContextFromTraceparent(ctx context.Context, raw string) context.Context {
	return WithTrace(ctx, FromInbound(raw))
}

// WithCorrelation binds a logger pre-loaded with the trace + request-id fields and returns
// the updated context. Used by the HTTP middleware and any subscriber entry point.
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

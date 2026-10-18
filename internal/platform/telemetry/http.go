package telemetry

import (
	"context"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type routeKey struct{}
type routeState struct{ route string }

func SetRoute(ctx context.Context, pattern string) {
	if state, ok := ctx.Value(routeKey{}).(*routeState); ok {
		_, route, found := strings.Cut(pattern, " ")
		if !found {
			route = pattern
		}
		state.route = route
	}
}

func SetTenant(ctx context.Context, tenant string) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("orbit.tenant_id", tenant))
}

func SetRun(ctx context.Context, run, member string, segment, attempt int) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("orbit.run_id", run), attribute.String("orbit.member", member),
		attribute.Int("orbit.segment", segment), attribute.Int("orbit.attempt", attempt))
}

// statusWriter preserves streaming through ResponseController without buffering.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
func (w *statusWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func HTTPMiddleware(listener string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		ctx := r.Context()
		options := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindServer)}
		if listener == "internal" {
			ctx = (TraceparentOnly{}).Extract(ctx, propagation.HeaderCarrier(r.Header))
		} else {
			options = append(options, trace.WithNewRoot())
		}
		ctx, span := otel.Tracer("orbit-control/http").Start(ctx, r.Method, options...)
		defer span.End()
		state := &routeState{route: "_unmatched"}
		ctx = context.WithValue(ctx, routeKey{}, state)
		r = r.WithContext(ctx)
		rec := &statusWriter{ResponseWriter: w}
		defer func() {
			if listener != "public" && r.Pattern != "" {
				SetRoute(ctx, r.Pattern)
			}
			if rec.status == 0 {
				rec.status = http.StatusOK
			}
			attrs := []attribute.KeyValue{attribute.String("http.request.method", r.Method),
				attribute.String("http.route", state.route), attribute.Int("http.response.status_code", rec.status),
				attribute.String("orbit.listener", listener)}
			span.SetName(r.Method + " " + state.route)
			span.SetAttributes(attrs...)
			if listener == "public" {
				span.SetAttributes(attribute.String("orbit.request_id", w.Header().Get("X-Request-ID")))
			}
			if rec.status >= 500 {
				span.SetStatus(codes.Error, "")
			}
			HistogramFor(httpMeter(listener), "orbit.http.server.request.duration").Record(ctx, time.Since(started).Seconds(), attrs...)
		}()
		next.ServeHTTP(rec, r)
	})
}

func httpMeter(listener string) string {
	if listener == "stream" {
		return "stream-gateway"
	}
	return "orbit-control"
}

package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TraceparentOnly excludes tracestate and baggage at every trust boundary.
// TraceContext alone also propagates tracestate, which can contain content.
type TraceparentOnly struct{}

func (TraceparentOnly) Fields() []string { return []string{"traceparent"} }

func (TraceparentOnly) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	sc := trace.SpanContextFromContext(ctx).WithTraceState(trace.TraceState{})
	propagation.TraceContext{}.Inject(trace.ContextWithSpanContext(ctx, sc), carrier)
}

func (TraceparentOnly) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	ctx = propagation.TraceContext{}.Extract(ctx, carrier)
	sc := trace.SpanContextFromContext(ctx).WithTraceState(trace.TraceState{})
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

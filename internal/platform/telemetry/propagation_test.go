package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

func TestTraceparentOnly(t *testing.T) {
	carrier := propagation.MapCarrier{
		"traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		"tracestate":  "vendor=ORBIT-CANARY", "baggage": "prompt=ORBIT-CANARY",
	}
	p := TraceparentOnly{}
	require.Equal(t, []string{"traceparent"}, p.Fields())
	ctx := p.Extract(context.Background(), carrier)
	sc := trace.SpanContextFromContext(ctx)
	require.True(t, sc.IsValid())
	require.True(t, sc.IsRemote())
	require.Empty(t, sc.TraceState().String())
	out := propagation.MapCarrier{}
	p.Inject(ctx, out)
	require.Equal(t, propagation.MapCarrier{"traceparent": carrier["traceparent"]}, out)
	// Injection strips state even if the context was not obtained via Extract.
	unfiltered := propagation.TraceContext{}.Extract(context.Background(), carrier)
	require.NotEmpty(t, trace.SpanContextFromContext(unfiltered).TraceState().String())
	out = propagation.MapCarrier{}
	p.Inject(unfiltered, out)
	require.NotContains(t, out, "tracestate")
	propagation.TraceContext{}.Inject(unfiltered, out)
	require.Contains(t, out, "tracestate", "negative control must detect plain TraceContext")
}

func TestTemporalTraceparentOnly(t *testing.T) {
	provider, _, _ := testProviders(t)
	tracer, err := opentelemetry.NewTracer(opentelemetry.TracerOptions{
		Tracer: provider.Tracer("test"), TextMapPropagator: TraceparentOnly{}, DisableBaggage: true,
	})
	require.NoError(t, err)
	parent, err := tracer.UnmarshalSpan(map[string]string{
		"traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", "tracestate": "vendor=secret",
	})
	require.NoError(t, err)
	span, err := tracer.StartSpan(&interceptor.TracerStartSpanOptions{Parent: parent, Operation: "RunActivity", Name: "test"})
	require.NoError(t, err)
	ctx := tracer.ContextWithSpan(context.Background(), span)
	require.Empty(t, trace.SpanContextFromContext(ctx).TraceState().String())
	out, err := tracer.MarshalSpan(span)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Contains(t, out, "traceparent")
	span.Finish(&interceptor.TracerFinishSpanOptions{})
}

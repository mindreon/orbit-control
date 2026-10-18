package telemetry

import (
	"context"
	"encoding/json/v2"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func testProviders(t *testing.T) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter, *sdkmetric.ManualReader) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(AllowlistExporter(exporter)))
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(append(MeterOptions(), sdkmetric.WithReader(reader))...)
	oldTrace, oldMeter := otel.GetTracerProvider(), otel.GetMeterProvider()
	otel.SetTracerProvider(provider)
	otel.SetMeterProvider(meter)
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTrace)
		otel.SetMeterProvider(oldMeter)
		require.NoError(t, meter.Shutdown(context.Background()))
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	return provider, exporter, reader
}
func TestAllowlistExporter(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(AllowlistExporter(exporter)), sdktrace.WithResource(
		resource.NewSchemaless(attribute.String("service.name", "control"), attribute.String("secret", "ORBIT-CANARY"))))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("test").Start(t.Context(), "bad\nORBIT-CANARY", trace.WithLinks(trace.Link{
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}}),
		Attributes:  []attribute.KeyValue{attribute.String("secret", "ORBIT-CANARY")},
	}))
	span.SetAttributes(attribute.String("orbit.run_id", "safe"), attribute.String("orbit.member", strings.Repeat("x", 257)))
	for _, key := range []string{"gen_ai.prompt", "gen_ai.completion", "gen_ai.input.messages", "gen_ai.output.messages",
		"gen_ai.system_instructions", "gen_ai.tool.call.arguments", "gen_ai.tool.call.result", "gen_ai.tool.definitions",
		"openjiuwen.span.input", "openjiuwen.span.output", "exception.message", "exception.stacktrace", "http.request.header.authorization",
		"url.full", "server.address", "file.path", "authorization"} {
		span.SetAttributes(attribute.String(key, "ORBIT-CANARY"))
	}
	span.AddEvent("exception", trace.WithAttributes(attribute.String("exception.message", "ORBIT-CANARY")))
	span.SetStatus(codes.Error, "ORBIT-CANARY")
	span.End()
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	got := spans[0]
	require.Equal(t, "redacted", got.Name)
	require.Equal(t, []attribute.KeyValue{attribute.String("orbit.run_id", "safe")}, got.Attributes)
	require.Empty(t, got.Events)
	require.Empty(t, got.Status.Description)
	require.Equal(t, codes.Error, got.Status.Code)
	require.Empty(t, got.Links[0].Attributes)
	require.True(t, got.Links[0].SpanContext.IsValid())
	require.Equal(t, []attribute.KeyValue{attribute.String("service.name", "control")}, got.Resource.Attributes())
	raw, err := json.Marshal(got)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "ORBIT-CANARY")
}
func TestSpanAllowlistGolden(t *testing.T) {
	raw, err := os.ReadFile("../../../contract/v2/fixtures/span-allowlist.json")
	require.NoError(t, err)
	var expected []string
	require.NoError(t, json.Unmarshal(raw, &expected))
	require.ElementsMatch(t, expected, SpanAttributes)
	for _, key := range SpanAttributes {
		require.False(t, strings.HasPrefix(key, "url.") || strings.HasPrefix(key, "gen_ai.prompt") ||
			strings.HasPrefix(key, "gen_ai.completion") || strings.Contains(key, "arguments") || strings.Contains(key, "stack"))
	}
}
func TestAllowlistClearsTraceState(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	state, err := trace.ParseTraceState("vendor=ORBIT-CANARY")
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceState: state})
	span := tracetest.SpanStub{Name: "safe", SpanContext: sc, Parent: sc,
		Links: []sdktrace.Link{{SpanContext: sc}}, Resource: resource.Empty()}.Snapshot()
	require.NoError(t, AllowlistExporter(exporter).ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{span}))
	got := exporter.GetSpans()[0]
	require.Empty(t, got.SpanContext.TraceState().String())
	require.Empty(t, got.Parent.TraceState().String())
	require.Empty(t, got.Links[0].SpanContext.TraceState().String())
	require.Equal(t, sc.TraceID(), got.SpanContext.TraceID())
	require.NotEmpty(t, span.SpanContext().TraceState().String(), "original remains unmodified")
}

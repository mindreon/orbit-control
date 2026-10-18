package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestSetupDisabledAndConfig(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	old := otel.GetTracerProvider()
	provider := noop.NewTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(old) })
	shutdown, err := Setup(t.Context(), Config{}, logger)
	require.NoError(t, err)
	require.Equal(t, provider, otel.GetTracerProvider())
	require.IsType(t, TraceparentOnly{}, otel.GetTextMapPropagator())
	require.NoError(t, shutdown(t.Context()))
	for _, value := range []string{"NaN", "-1", "1.1", "bad"} {
		t.Setenv("OTEL_TRACES_SAMPLER_ARG", value)
		_, err := FromEnv("orbit-control", "test")
		require.Error(t, err)
	}
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.5")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "1000")
	config, err := FromEnv("orbit-control", "test")
	require.NoError(t, err)
	require.Equal(t, .5, config.SampleRatio)
	require.Equal(t, time.Second, config.MetricInterval)
}

func TestSetupSDKAndNonblockingEndpoint(t *testing.T) {
	oldTrace, oldMeter := otel.GetTracerProvider(), otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTrace)
		otel.SetMeterProvider(oldMeter)
	})
	started := time.Now()
	shutdown, err := Setup(t.Context(), Config{Service: "orbit-control", Endpoint: "http://127.0.0.1:1",
		SampleRatio: 1, MetricInterval: time.Hour}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	require.NoError(t, err)
	require.Less(t, time.Since(started), 5*time.Second)
	_, span := otel.Tracer("test").Start(t.Context(), "test")
	require.True(t, span.IsRecording())
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = shutdown(ctx)
}

func TestPreexistingGlobalCounterDelegates(t *testing.T) {
	// The OTel delegate only binds once; use a subprocess so other tests cannot
	// replace the global provider before this WP7 startup-order assertion.
	if !runningDelegateChild() {
		runDelegateChild(t)
		return
	}
	counter, err := otel.Meter("wp7-before-setup").Int64Counter("sink_messages_total")
	require.NoError(t, err)
	_, _, reader := testProviders(t)
	counter.Add(t.Context(), 3)
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var total int64
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name == "sink_messages_total" {
				for _, point := range item.Data.(metricdata.Sum[int64]).DataPoints {
					total += point.Value
				}
			}
		}
	}
	require.Equal(t, int64(3), total)
}

func runningDelegateChild() bool { return os.Getenv("ORBIT_TELEMETRY_DELEGATE_CHILD") == "1" }
func runDelegateChild(t *testing.T) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPreexistingGlobalCounterDelegates$")
	cmd.Env = append(os.Environ(), "ORBIT_TELEMETRY_DELEGATE_CHILD=1")
	raw, err := cmd.CombinedOutput()
	require.NoError(t, err, string(raw))
}

func TestParentBasedSampling(t *testing.T) {
	old := otel.GetTracerProvider()
	oldMeter := otel.GetMeterProvider()
	defer func() {
		otel.SetTracerProvider(old)
		otel.SetMeterProvider(oldMeter)
	}()
	for _, ratio := range []float64{0, 1} {
		shutdown, err := Setup(t.Context(), Config{Service: "orbit-control", Endpoint: "http://127.0.0.1:1", SampleRatio: ratio,
			MetricInterval: time.Hour}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		require.NoError(t, err)
		for _, sampled := range []bool{false, true} {
			flags := trace.TraceFlags(0)
			if sampled {
				flags = trace.FlagsSampled
			}
			ctx := trace.ContextWithRemoteSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
				TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: flags, Remote: true}))
			_, span := otel.Tracer("test").Start(ctx, "child")
			require.Equal(t, sampled, span.IsRecording())
			span.End()
		}
		_, root := otel.Tracer("test").Start(t.Context(), "root")
		require.Equal(t, ratio == 1, root.IsRecording())
		root.End()
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_ = shutdown(ctx)
		cancel()
	}
}

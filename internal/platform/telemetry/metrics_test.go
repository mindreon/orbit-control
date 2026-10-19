package telemetry

import (
	"context"
	"encoding/json/v2"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

// A fresh process preserves OTel's initial delegating global provider; callbacks
// registered on it would carry over at the first SetMeterProvider and double-count.
func TestLabelOverflowAfterProviderSetup(t *testing.T) {
	if os.Getenv("ORBIT_TEST_FIRST_METER_PROVIDER") != "1" {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestLabelOverflowAfterProviderSetup$", "-test.v")
		cmd.Env = append(os.Environ(), "ORBIT_TEST_FIRST_METER_PROVIDER=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(append(MeterOptions(), sdkmetric.WithReader(reader))...)
	otel.SetMeterProvider(provider)
	InitializeMetrics()
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	ConfigureLimits(1, 1)
	labelLimiters["orbit.tenant_id"].Value("one")
	labelLimiters["orbit.tenant_id"].Value("two")
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var overflow int64
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name == "orbit.telemetry.label_overflow" {
				for _, point := range item.Data.(metricdata.Sum[int64]).DataPoints {
					label, _ := point.Attributes.Value("orbit.label")
					if label.AsString() == "tenant_id" {
						overflow += point.Value
					}
				}
			}
		}
	}
	require.Equal(t, int64(1), overflow)
}

func TestMetricsGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/metrics.golden.json")
	require.NoError(t, err)
	var golden []Spec
	require.NoError(t, json.Unmarshal(raw, &golden))
	encoded, err := json.Marshal(Metrics)
	require.NoError(t, err)
	var exported []Spec
	require.NoError(t, json.Unmarshal(encoded, &exported))
	require.Equal(t, golden, exported)
	InitializeMetrics()
	for _, spec := range Metrics {
		if spec.TenantAllowed {
			require.Contains(t, []string{"orbit.runs.active", "orbit.usage.tokens"}, spec.Name)
		}
		for _, label := range spec.Labels {
			require.NotContains(t, []string{"orbit.run_id", "orbit.session_id", "trace_id", "span_id", "request_id",
				"principal_id", "interaction_id", "message_id", "event_id", "workspace_id", "url.full"}, label)
		}
	}
	require.Panics(t, func() { Counter("orbit.runs.started").Add(t.Context(), 1, attribute.String("run_id", "bad")) })
}

func TestObservableMetricsRejectSynchronousAccess(t *testing.T) {
	InitializeMetrics()
	for _, spec := range Metrics {
		if spec.Observable {
			require.PanicsWithValue(t, "observable metric requested as synchronous: "+spec.Name, func() { cached(spec) })
		}
	}
}

func TestExemplarsDisabled(t *testing.T) {
	for _, protected := range []bool{true, false} {
		reader := sdkmetric.NewManualReader()
		options := []sdkmetric.Option{sdkmetric.WithReader(reader)}
		if protected {
			options = append(options, MeterOptions()...)
		}
		provider := sdkmetric.NewMeterProvider(options...)
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
		ctx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
		}))
		counter, err := provider.Meter("test").Int64Counter("orbit.runs.started")
		require.NoError(t, err)
		hist, err := provider.Meter("test").Float64Histogram("orbit.run.duration")
		require.NoError(t, err)
		attrs := metric.WithAttributes(attribute.String("orbit.entry_kind", "agent"),
			attribute.String("orbit.run.status", "succeeded"), attribute.String("orbit.run_id", "forbidden"))
		counter.Add(ctx, 1, attrs)
		hist.Record(ctx, 2, attrs)
		var data metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(ctx, &data))
		var exemplars int
		for _, scope := range data.ScopeMetrics {
			for _, item := range scope.Metrics {
				switch values := item.Data.(type) {
				case metricdata.Sum[int64]:
					for _, point := range values.DataPoints {
						exemplars += len(point.Exemplars)
					}
				case metricdata.Histogram[float64]:
					for _, point := range values.DataPoints {
						exemplars += len(point.Exemplars)
						if protected {
							require.Equal(t, metricSpec(item.Name).Buckets, point.Bounds)
							_, present := point.Attributes.Value("orbit.run_id")
							require.False(t, present)
						}
					}
				}
			}
		}
		if protected {
			require.Zero(t, exemplars)
		} else {
			require.Positive(t, exemplars, "negative control must detect the default trace-based exemplar filter")
		}
	}
}

func TestLimiterStableAndConcurrent(t *testing.T) {
	_, _, reader := testProviders(t)
	for _, label := range []string{"tenant_id", "model"} {
		limiter := NewLimiter(label, 2)
		value, outcome := limiter.Value("a")
		require.Equal(t, "a", value)
		require.True(t, outcome.Registered)
		value, outcome = limiter.Value("b")
		require.Equal(t, "b", value)
		require.True(t, outcome.Registered)
		for range 3 {
			value, outcome = limiter.Value("a")
			require.Equal(t, "a", value)
			require.Equal(t, MappingOutcome{}, outcome)
			value, outcome = limiter.Value("c")
			require.Equal(t, "_other", value)
			require.True(t, outcome.Overflow)
			var data metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &data))
		}
		var wg sync.WaitGroup
		for range 100 {
			wg.Go(func() { limiter.Value("new") })
		}
		wg.Wait()
		require.Len(t, limiter.seen, 2)
	}
}

func TestTokenLimiterPreservesCumulativeTotals(t *testing.T) {
	_, _, reader := testProviders(t)
	old := labelLimiters["orbit.tenant_id"]
	labelLimiters["orbit.tenant_id"] = NewLimiter("tenant_id", 1)
	defer func() { labelLimiters["orbit.tenant_id"] = old }()
	for iteration := range 3 {
		for _, tenant := range []string{"one", "two", "three"} {
			Counter("orbit.usage.tokens").Add(t.Context(), 2, attribute.String("orbit.tenant_id", tenant),
				attribute.String("gen_ai.request.model", "model"), attribute.String("orbit.token.type", "input"))
		}
		var data metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &data))
		got := map[string]int64{}
		var overflow int64
		for _, scope := range data.ScopeMetrics {
			for _, item := range scope.Metrics {
				if item.Name == "orbit.usage.tokens" {
					for _, point := range item.Data.(metricdata.Sum[int64]).DataPoints {
						tenant, _ := point.Attributes.Value("orbit.tenant_id")
						got[tenant.AsString()] += point.Value
					}
				}
				if item.Name == "orbit.telemetry.label_overflow" {
					for _, point := range item.Data.(metricdata.Sum[int64]).DataPoints {
						label, _ := point.Attributes.Value("orbit.label")
						if label.AsString() == "tenant_id" {
							overflow += point.Value
						}
					}
				}
			}
		}
		require.Equal(t, map[string]int64{"one": int64(iteration+1) * 2, "_other": int64(iteration+1) * 4}, got)
		require.Equal(t, int64(iteration+1)*2, overflow)
	}
}

func TestInstrumentCacheAndMeters(t *testing.T) {
	_, _, reader := testProviders(t)
	first := Histogram("orbit.http.server.request.duration")
	require.Equal(t, first.next, Histogram("orbit.http.server.request.duration").next)
	provider := otel.GetMeterProvider()
	instrumentCache.Lock()
	count := len(instrumentCache.values)
	instrumentCache.Unlock()
	for range 10 {
		Histogram("orbit.http.server.request.duration")
	}
	instrumentCache.Lock()
	require.Equal(t, count, len(instrumentCache.values))
	instrumentCache.Unlock()
	require.Equal(t, "orbit-control", first.spec.Meter)
	stream := HistogramFor("stream-gateway", "orbit.http.server.request.duration")
	require.Equal(t, "stream-gateway", stream.spec.Meter)
	labels := []attribute.KeyValue{attribute.String("http.request.method", "GET"), attribute.String("http.route", "_unmatched"),
		attribute.Int("http.response.status_code", 200), attribute.String("orbit.listener", "stream")}
	stream.Record(t.Context(), 1, labels...)
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	found := false
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name == "orbit.http.server.request.duration" && scope.Scope.Name == "stream-gateway" {
				found = true
			}
		}
	}
	require.True(t, found)
	require.Equal(t, provider, otel.GetMeterProvider())
}

package runworker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/mindreon/orbit-control/internal/platform/telemetry"
)

func TestObserveSnapshotWithoutDB(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(append(telemetry.MeterOptions(), sdkmetric.WithReader(reader))...)
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	telemetry.InitializeMetrics()
	t.Cleanup(func() {
		otel.SetMeterProvider(old)
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	// A nil DB deliberately makes any callback DB access fail immediately.
	now := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	sampledAt := now
	metrics, unregister, err := observe(nil, func() time.Time { return now })
	require.NoError(t, err)
	defer unregister()
	metrics.snapshot = metricsSnapshot{
		sampledAt: sampledAt,
		runs:      map[runKey]int64{{tenant: "one", status: "running"}: 3, {tenant: "_other", status: "paused"}: 7},
		interactions: []interactionCount{
			{kind: "ask_user", n: 2, oldest: now.Add(-42500 * time.Millisecond)},
			{kind: "approve", n: 4, oldest: now.Add(-12250 * time.Millisecond)},
		},
	}
	for _, elapsed := range []time.Duration{0, 30 * time.Second, 2 * metricsSampleInterval, 2*metricsSampleInterval + time.Nanosecond} {
		now = sampledAt.Add(elapsed)
		var data metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &data))
		runs, pending, ages := map[string]int64{}, map[string]int64{}, map[string]float64{}
		for _, scope := range data.ScopeMetrics {
			for _, item := range scope.Metrics {
				switch item.Name {
				case "orbit.runs.active":
					for _, point := range item.Data.(metricdata.Gauge[int64]).DataPoints {
						tenant, _ := point.Attributes.Value("orbit.tenant_id")
						status, _ := point.Attributes.Value("orbit.run.status")
						runs[tenant.AsString()+":"+status.AsString()] = point.Value
					}
				case "orbit.interactions.pending":
					for _, point := range item.Data.(metricdata.Gauge[int64]).DataPoints {
						kind, _ := point.Attributes.Value("orbit.interaction.kind")
						pending[kind.AsString()] = point.Value
					}
				case "orbit.interactions.oldest_pending.age":
					for _, point := range item.Data.(metricdata.Gauge[float64]).DataPoints {
						kind, _ := point.Attributes.Value("orbit.interaction.kind")
						ages[kind.AsString()] = point.Value
					}
				}
			}
		}
		if elapsed > 2*metricsSampleInterval {
			require.Empty(t, runs)
			require.Empty(t, pending)
			require.Empty(t, ages)
			continue
		}
		require.Equal(t, map[string]int64{"one:running": 3, "_other:paused": 7}, runs)
		require.Equal(t, map[string]int64{"ask_user": 2, "approve": 4}, pending)
		require.Equal(t, map[string]float64{"ask_user": 42.5 + elapsed.Seconds(), "approve": 12.25 + elapsed.Seconds()}, ages)
	}
}

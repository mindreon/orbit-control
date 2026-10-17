package ctl

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestOutboxFailureStagesAreExplicit(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(t.Context())) })
	a := &Activities{Meter: provider.Meter("ctl-test")}
	for _, stage := range []string{"publish", "end_marker", "mark_published", "end_marker"} {
		a.logOutboxFailure(t.Context(), stage, "run", "event", 1, errors.New("failure"))
	}
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	got := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			for _, point := range item.Data.(metricdata.Sum[int64]).DataPoints {
				stage, ok := point.Attributes.Value("stage")
				require.True(t, ok)
				got[stage.AsString()] += point.Value
			}
		}
	}
	require.Equal(t, map[string]int64{"publish": 1, "end_marker": 2, "mark_published": 1}, got)
}

func TestOutboxFailureCounterConcurrentUse(t *testing.T) {
	// Exercise fresh instances so another test or an earlier iteration cannot
	// pre-initialize the counter and hide an unsynchronized first-use write.
	for range 16 {
		a := &Activities{}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 8 {
			wg.Go(func() {
				<-start
				a.logOutboxFailure(t.Context(), "publish", "run", "event", 1, errors.New("failure"))
			})
		}
		close(start)
		wg.Wait()
	}
}

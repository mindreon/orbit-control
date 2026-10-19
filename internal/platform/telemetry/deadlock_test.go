package telemetry

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Isolate regressions: a stuck SDK collection also blocks provider shutdown.
func TestCollectNewTenantConcurrentCounter(t *testing.T) {
	if os.Getenv("ORBIT_METRICS_DEADLOCK_CHILD") == "" {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCollectNewTenantConcurrentCounter$", "-test.timeout=3s")
		cmd.Env = append(os.Environ(), "ORBIT_METRICS_DEADLOCK_CHILD=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	_, _, reader := testProviders(t)
	limiter := NewLimiter("tenant_id", 1)
	meter := otel.Meter("orbit-control")
	gauge, err := meter.Int64ObservableGauge("orbit.runs.active")
	require.NoError(t, err)
	entered := make(chan struct{})
	proceed := make(chan struct{})
	registration, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		close(entered)
		<-proceed
		value, outcome := limiter.Value("new-tenant")
		if value != "new-tenant" || !outcome.Registered {
			return os.ErrInvalid
		}
		observer.ObserveInt64(gauge, 1, metric.WithAttributes(attribute.String("orbit.tenant_id", value)))
		return nil
	}, gauge)
	require.NoError(t, err)
	done := make(chan error, 2)
	go func() {
		var data metricdata.ResourceMetrics
		done <- reader.Collect(t.Context(), &data)
	}()
	<-entered
	// Mapping must not touch the instrument cache, even when another caller
	// holds it. This also detects synchronous recording reintroduced in Value.
	instrumentCache.Lock()
	httpDone := make(chan struct{})
	go func() {
		Counter("orbit.runs.started").Add(t.Context(), 1, attribute.String("orbit.run.trigger", "interactive"),
			attribute.String("orbit.entry_kind", "agent"))
		close(httpDone)
	}()
	close(proceed)
	select {
	case err := <-done:
		instrumentCache.Unlock()
		require.NoError(t, err)
	case <-time.After(time.Second):
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		t.Fatalf("collection/limiter deadlocked:\n%s", stack[:n])
	}
	select {
	case <-httpDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP-style Counter.Add deadlocked")
	}
	require.NoError(t, registration.Unregister())
	// Keep collection active while an HTTP-style caller obtains its counter.
	registration, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(gauge, 1)
		return nil
	}, gauge)
	require.NoError(t, err)
	go func() {
		var data metricdata.ResourceMetrics
		done <- reader.Collect(t.Context(), &data)
	}()
	go func() {
		Counter("orbit.runs.started").Add(t.Context(), 1, attribute.String("orbit.run.trigger", "interactive"),
			attribute.String("orbit.entry_kind", "agent"))
		done <- nil
	}()
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("collection/counter deadlocked")
		}
	}
	require.NoError(t, registration.Unregister())
}

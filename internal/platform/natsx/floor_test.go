package natsx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestEventFloorCacheThrottleStaleAndGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		defer provider.Shutdown(context.Background())
		counter, err := provider.Meter("floor-test").Int64Counter("history_floor_clamped_total")
		require.NoError(t, err)
		calls := 0
		var currentErr error
		created, floor, first := time.Unix(1, 0), uint64(3), uint64(10)
		f := &EventFloor{clamped: counter, read: func(ctx context.Context) (time.Time, uint64, uint64, error) {
			calls++
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, 500*time.Millisecond, time.Until(deadline))
			return created, floor, first, currentErr
		}}
		got, err := f.Floor(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 9, got, "FirstSeq-1 clamps an AckFloor below retention")
		time.Sleep(999 * time.Millisecond)
		_, err = f.Floor(t.Context())
		require.NoError(t, err)
		require.Equal(t, 1, calls, "refreshes are throttled for a full second")
		time.Sleep(time.Millisecond)
		currentErr = errors.New("NATS unavailable")
		got, err = f.Floor(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 9, got, "refresh failure retains the last safe floor")
		require.Equal(t, 2, calls)
		time.Sleep(time.Hour)
		got, err = f.Floor(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 9, got, "stale floors are safe regardless of age")
		time.Sleep(time.Second)
		created = time.Unix(2, 0)
		_, err = f.Floor(t.Context())
		require.Error(t, err, "new consumer invalidates the old generation even if stream info fails")
		time.Sleep(time.Second)
		currentErr, floor, first = nil, 2, 1
		got, err = f.Floor(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 2, got, "a recreated consumer resets rather than retaining the old maximum")
		var data metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &data))
		var count int64
		for _, scope := range data.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name == "history_floor_clamped_total" {
					for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
						require.Empty(t, point.Attributes.ToSlice())
						count += point.Value
					}
				}
			}
		}
		require.EqualValues(t, 1, count)
	})
}

func TestEventFloorCancellationAndMonotonicGuard(t *testing.T) {
	release, started := make(chan struct{}), make(chan struct{})
	calls := 0
	f := &EventFloor{read: func(ctx context.Context) (time.Time, uint64, uint64, error) {
		calls++
		close(started)
		select {
		case <-release:
			return time.Unix(1, 0), 3, 1, nil
		case <-ctx.Done():
			return time.Time{}, 0, 0, ctx.Err()
		}
	}}
	firstCtx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		_, _ = f.Floor(firstCtx)
		close(done)
	}()
	<-started
	cancel()
	second := make(chan uint64, 1)
	go func() {
		v, err := f.Floor(t.Context())
		if err == nil {
			second <- v
		}
	}()
	close(release)
	require.Eventually(t, func() bool {
		select {
		case v := <-second:
			return v == 3
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Equal(t, 1, calls)
	f.mu.Lock()
	f.next = time.Time{}
	f.read = func(context.Context) (time.Time, uint64, uint64, error) { return time.Unix(1, 0), 2, 1, nil }
	f.mu.Unlock()
	v, err := f.Floor(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, v, "floor is monotonic within a consumer generation")
}

func TestEventFloorColdStartAndSingleflight(t *testing.T) {
	var calls atomic.Int32
	f := &EventFloor{read: func(ctx context.Context) (time.Time, uint64, uint64, error) {
		calls.Add(1)
		<-ctx.Done()
		return time.Time{}, 0, 0, ctx.Err()
	}}
	var wg sync.WaitGroup
	started := time.Now()
	for range 8 {
		wg.Go(func() {
			_, err := f.Floor(t.Context())
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
	wg.Wait()
	require.Less(t, time.Since(started), time.Second)
	require.EqualValues(t, 1, calls.Load(), "concurrent cold refreshes share one bounded attempt")
	_, err := f.Floor(t.Context())
	require.Error(t, err)
	require.EqualValues(t, 1, calls.Load(), "failed refreshes are throttled too")
}

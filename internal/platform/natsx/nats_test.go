//go:build nats_integration

package natsx

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestRunEventsStream(t *testing.T) {
	url := os.Getenv("ORBIT_TEST_NATS_URL")
	if url == "" {
		t.Skip("ORBIT_TEST_NATS_URL required")
	}
	bus, err := Connect(url, "orbit-v2-test-wp7a")
	require.NoError(t, err)
	defer bus.Close()
	require.NoError(t, bus.EnsureRunEventsStream(t.Context(), 0))
	require.NoError(t, bus.EnsureRunEventsStream(t.Context(), 8<<20))
	stream, err := bus.JS.Stream(t.Context(), "RUN_EVENTS")
	require.NoError(t, err)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	cfg := info.Config
	require.Equal(t, []string{"run.>"}, cfg.Subjects)
	require.Equal(t, 7*24*time.Hour, cfg.MaxAge)
	require.Equal(t, 10*time.Minute, cfg.Duplicates)
	require.EqualValues(t, 500000, cfg.MaxMsgsPerSubject)
	require.EqualValues(t, 65536, cfg.MaxMsgSize)
	require.EqualValues(t, 8<<20, cfg.MaxBytes)
	require.Equal(t, jetstream.FileStorage, cfg.Storage)
	require.Equal(t, jetstream.LimitsPolicy, cfg.Retention)
	require.Equal(t, jetstream.DiscardOld, cfg.Discard)
	require.Equal(t, jetstream.S2Compression, cfg.Compression)
	consumer, err := bus.EnsureSinkConsumer(t.Context())
	require.NoError(t, err)
	_, err = bus.EnsureSinkConsumer(t.Context())
	require.NoError(t, err)
	consumerInfo, err := consumer.Info(t.Context())
	require.NoError(t, err)
	require.Equal(t, "run-events-sink", consumerInfo.Config.Durable)
	require.Equal(t, "run.>", consumerInfo.Config.FilterSubject)
	require.Equal(t, jetstream.AckExplicitPolicy, consumerInfo.Config.AckPolicy)
	require.Equal(t, 10, consumerInfo.Config.MaxDeliver)
	require.Equal(t, 2000, consumerInfo.Config.MaxAckPending)
	require.Equal(t, []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second, 60 * time.Second}, consumerInfo.Config.BackOff)
	for range 2 {
		require.NoError(t, bus.Publish(t.Context(), "run.tenant.session.run", "event-1", []byte(`{}`)))
	}
	info, err = stream.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, info.State.Msgs)
	require.NoError(t, bus.JS.DeleteStream(t.Context(), "RUN_EVENTS"))
}

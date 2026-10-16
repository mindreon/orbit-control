//go:build nats_integration

package natsx

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func TestRunEventsStream(t *testing.T) {
	url := os.Getenv("ORBIT_TEST_NATS_URL")
	if url == "" {
		t.Skip("ORBIT_TEST_NATS_URL required")
	}
	bus, err := Connect(url, "orbit-v2-test-wp7a")
	require.NoError(t, err)
	t.Cleanup(bus.Close)
	for _, mode := range []string{"existing-messages", "legacy-config"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRunEventsStream(t, bus, mode)
		})
	}
}

func testRunEventsStream(t *testing.T, bus *Bus, mode string) {
	t.Helper()
	name := "TEST_" + ids.New()
	subject := "test." + ids.New()
	filter := subject + ".>"
	if mode == "legacy-config" {
		_, err := bus.JS.CreateStream(t.Context(), jetstream.StreamConfig{
			Name: name, Subjects: []string{filter}, Storage: jetstream.FileStorage,
			MaxAge: 7 * 24 * time.Hour, Duplicates: 24 * time.Hour, MaxMsgSize: 64 << 10,
		})
		require.NoError(t, err)
	} else {
		require.NoError(t, bus.ensureRunEventsStream(t.Context(), name, filter, 0))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, bus.JS.DeleteStream(ctx, name))
	})
	// Keep a message through the configuration update; assertions use only our subject.
	require.NoError(t, bus.Publish(t.Context(), subject+".existing", ids.New(), []byte(`{}`)))
	require.NoError(t, bus.ensureRunEventsStream(t.Context(), name, filter, 8<<20))
	stream, err := bus.JS.Stream(t.Context(), name)
	require.NoError(t, err)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	cfg := info.Config
	require.Equal(t, []string{filter}, cfg.Subjects)
	require.Equal(t, 7*24*time.Hour, cfg.MaxAge)
	require.Equal(t, 10*time.Minute, cfg.Duplicates)
	require.EqualValues(t, 500000, cfg.MaxMsgsPerSubject)
	require.EqualValues(t, 65536, cfg.MaxMsgSize)
	require.EqualValues(t, 8<<20, cfg.MaxBytes)
	require.Equal(t, jetstream.FileStorage, cfg.Storage)
	require.Equal(t, jetstream.LimitsPolicy, cfg.Retention)
	require.Equal(t, jetstream.DiscardOld, cfg.Discard)
	require.Equal(t, jetstream.S2Compression, cfg.Compression)
	consumer, err := bus.ensureSinkConsumer(t.Context(), name, filter, "sink")
	require.NoError(t, err)
	_, err = bus.ensureSinkConsumer(t.Context(), name, filter, "sink")
	require.NoError(t, err)
	consumerInfo, err := consumer.Info(t.Context())
	require.NoError(t, err)
	require.Equal(t, "sink", consumerInfo.Config.Durable)
	require.Equal(t, filter, consumerInfo.Config.FilterSubject)
	require.Equal(t, jetstream.AckExplicitPolicy, consumerInfo.Config.AckPolicy)
	require.Equal(t, 10, consumerInfo.Config.MaxDeliver)
	require.Equal(t, 2000, consumerInfo.Config.MaxAckPending)
	require.Equal(t,
		[]time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second, 60 * time.Second},
		consumerInfo.Config.BackOff)
	id := ids.New()
	var firstSequence uint64
	for i := range 2 {
		require.NoError(t, bus.Publish(t.Context(), subject+".dedup", id, []byte(`{}`)))
		msg, err := stream.GetLastMsgForSubject(t.Context(), subject+".dedup")
		require.NoError(t, err)
		if i == 0 {
			firstSequence = msg.Sequence
		}
		require.Equal(t, firstSequence, msg.Sequence)
	}
	info, err = stream.Info(t.Context(), jetstream.WithSubjectFilter(subject+".dedup"))
	require.NoError(t, err)
	require.EqualValues(t, 1, info.State.Subjects[subject+".dedup"])
	_, err = stream.GetLastMsgForSubject(t.Context(), subject+".existing")
	require.NoError(t, err)
}

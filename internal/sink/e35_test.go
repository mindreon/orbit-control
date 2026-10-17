package sink

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestE35SinkPoisonAndMarkerRules(t *testing.T) {
	store := &fakeStore{}
	s := testSink(store)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	var err error
	s.metrics, err = NewMetrics(provider.Meter("e35"))
	require.NoError(t, err)
	m := validMessage(t, "run.status")
	m.meta.NumDelivered = 15
	s.process(t.Context(), m)
	require.Equal(t, 1, m.acks, "healthy high-delivery messages are written, not preemptively poisoned")
	require.Empty(t, store.rejects)
	require.Equal(t, 1, store.writes)
	require.Zero(t, m.terms)

	marker := validMessage(t, "run.status")
	marker.subject += ".end"
	marker.raw = []byte(`{"run_id":"ignored","event_id":"ignored"}`)
	s.process(t.Context(), marker)
	require.Equal(t, 1, marker.acks)
	require.Zero(t, marker.naks)
	require.Empty(t, store.rejects)
	require.Zero(t, marker.terms)
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var skipped int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "sink_skipped_total" {
				for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
					value, ok := p.Attributes.Value("type")
					require.True(t, ok)
					require.Equal(t, "end_marker", value.AsString())
					skipped += p.Value
				}
			}
		}
	}
	require.EqualValues(t, 1, skipped)

	poison := validMessage(t, "run.status")
	store.writeErr = &pgconn.PgError{Code: "22P05"}
	s.process(t.Context(), poison)
	require.Equal(t, 1, poison.acks)
	require.Len(t, store.rejects, 1)
	require.Equal(t, "invalid_payload", store.rejects[0].reason)
	require.Zero(t, poison.terms)
}

func TestE35FailedRejectWriteIsNotAcked(t *testing.T) {
	store := &fakeStore{rejectErr: errors.New("reject database unavailable")}
	s := testSink(store)
	m := validMessage(t, "run.status")
	m.raw = []byte(`{}`)
	s.process(t.Context(), m)
	require.Zero(t, m.acks)
	require.Equal(t, 1, m.naks)
	require.Zero(t, m.terms)
}

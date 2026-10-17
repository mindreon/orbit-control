package sink

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics records into the process MeterProvider. WP-O owns SDK/exporter setup;
// without it these instruments are no-op, with no separate HTTP listener.
type Metrics struct {
	messages, rejected, skipped metric.Int64Counter
	lag, batch                  metric.Float64Histogram
	pending                     metric.Int64Gauge
}

func NewMetrics(meter metric.Meter) (*Metrics, error) {
	if meter == nil {
		meter = otel.Meter("github.com/mindreon/orbit-control/internal/sink")
	}
	m := &Metrics{}
	var err error
	for _, instrument := range []struct {
		name string
		dest *metric.Int64Counter
	}{
		{name: "sink_messages_total", dest: &m.messages},
		{name: "sink_rejected_total", dest: &m.rejected},
		{name: "sink_skipped_total", dest: &m.skipped},
	} {
		*instrument.dest, err = meter.Int64Counter(instrument.name)
		if err != nil {
			return nil, err
		}
	}
	m.lag, err = meter.Float64Histogram("sink_lag_seconds", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	m.batch, err = meter.Float64Histogram("sink_batch_duration_seconds", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	m.pending, err = meter.Int64Gauge("sink_pending")
	return m, err
}

func count(counter metric.Int64Counter, key, value string) {
	counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String(key, value)))
}

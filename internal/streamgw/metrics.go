package streamgw

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Instruments use the process provider (no-op until WP-O installs the SDK).
// Labels are limited to scope, contract EventType and fixed failure reasons.
type metrics struct {
	connections                                                    metric.Int64UpDownCounter
	frames, resumes, expired, slow, auth, invalid, order, rejected metric.Int64Counter
}

func newMetrics(meter metric.Meter) (metrics, error) {
	var m metrics
	var err error
	m.connections, err = meter.Int64UpDownCounter("streamgw_connections")
	if err != nil {
		return m, err
	}
	for _, instrument := range []struct {
		name string
		dest *metric.Int64Counter
	}{
		{name: "streamgw_frames_sent_total", dest: &m.frames},
		{name: "streamgw_resumes_total", dest: &m.resumes},
		{name: "streamgw_cursor_expired_total", dest: &m.expired},
		{name: "streamgw_slow_client_closed_total", dest: &m.slow},
		{name: "streamgw_auth_failures_total", dest: &m.auth},
		{name: "streamgw_invalid_frames_total", dest: &m.invalid},
		{name: "streamgw_order_violation_total", dest: &m.order},
		{name: "streamgw_rejected_total", dest: &m.rejected},
	} {
		*instrument.dest, err = meter.Int64Counter(instrument.name)
		if err != nil {
			return m, err
		}
	}
	return m, nil
}

func add(counter metric.Int64Counter, key, value string) {
	if key == "" {
		counter.Add(context.Background(), 1)
		return
	}
	counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String(key, value)))
}

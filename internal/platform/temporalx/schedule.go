package temporalx

import (
	"context"

	"go.temporal.io/sdk/client"
)

// Schedules owns a reference to the shared Temporal connection. Close it after
// all schedule operations stop; closing it does not close the original client.
type Schedules struct {
	client.ScheduleClient
	close func()
}

func (s *Schedules) Close() { s.close() }

// ScheduleClient deliberately supplies fresh options, never copying tracing
// interceptors or context propagators from the workflow client. Options carry
// the namespace and logger, which the SDK does not inherit from existing.
func ScheduleClient(ctx context.Context, existing client.Client, options client.Options) (*Schedules, error) {
	untraced, err := client.NewClientFromExistingWithContext(ctx, existing, client.Options{
		Namespace: options.Namespace, Logger: options.Logger, MetricsHandler: TemporalMetrics(),
	})
	if err != nil {
		return nil, err
	}
	return &Schedules{ScheduleClient: untraced.ScheduleClient(), close: untraced.Close}, nil
}

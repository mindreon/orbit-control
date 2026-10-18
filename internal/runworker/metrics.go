package runworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/client"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
)

// Observe registers DB gauges only in worker processes. Each callback uses a
// bounded global transaction and calls only the aggregate SECURITY DEFINER API.
func Observe(d *db.DB) (func(), error) {
	meter := otel.Meter("orbit-control")
	runs, err := meter.Int64ObservableGauge("orbit.runs.active", metric.WithUnit("{run}"))
	if err != nil {
		return nil, err
	}
	pending, err := meter.Int64ObservableGauge("orbit.interactions.pending", metric.WithUnit("{interaction}"))
	if err != nil {
		return nil, err
	}
	age, err := meter.Float64ObservableGauge("orbit.interactions.oldest_pending.age", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	registration, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return d.Global(ctx, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, "SELECT tenant_id::text,status,n FROM ops_run_counts()")
			if err != nil {
				return err
			}
			type key struct{ tenant, status string }
			counts := map[key]int64{}
			for rows.Next() {
				var tenant, status string
				var n int64
				if err = rows.Scan(&tenant, &status, &n); err != nil {
					rows.Close()
					return err
				}
				attrs := telemetry.Labels(ctx, "orbit.runs.active", attribute.String("orbit.tenant_id", tenant),
					attribute.String("orbit.run.status", status))
				mapped := attrs[0].Value.AsString()
				counts[key{tenant: mapped, status: status}] += n
			}
			for group, n := range counts {
				observer.ObserveInt64(runs, n, metric.WithAttributes(attribute.String("orbit.tenant_id", group.tenant),
					attribute.String("orbit.run.status", group.status)))
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			rows, err = tx.Query(ctx, "SELECT kind,n,oldest FROM ops_interaction_counts()")
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var kind string
				var n int64
				var oldest time.Time
				if err = rows.Scan(&kind, &n, &oldest); err != nil {
					return err
				}
				attrs := metric.WithAttributes(attribute.String("orbit.interaction.kind", kind))
				observer.ObserveInt64(pending, n, attrs)
				observer.ObserveFloat64(age, time.Since(oldest).Seconds(), attrs)
			}
			return rows.Err()
		})
	}, runs, pending, age)
	if err != nil {
		return nil, err
	}
	return func() { _ = registration.Unregister() }, nil
}

// Sample stops on the worker context and never queries a tenant business row.
func Sample(ctx context.Context, d *db.DB, temporal client.Client, logger *slog.Logger) {
	var lastError time.Time
	backlog := time.NewTicker(30 * time.Second)
	quota := time.NewTicker(time.Minute)
	defer backlog.Stop()
	defer quota.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-backlog.C:
			for _, queue := range []string{"ctl", "ctl-wait", "agent.standard", "sandbox", "asset-check"} {
				call, cancel := context.WithTimeout(ctx, 5*time.Second)
				description, err := temporal.DescribeTaskQueueEnhanced(call, client.DescribeTaskQueueEnhancedOptions{
					TaskQueue: queue, ReportStats: true,
					TaskQueueTypes: []client.TaskQueueType{client.TaskQueueTypeWorkflow, client.TaskQueueTypeActivity},
				})
				cancel()
				if err != nil {
					if time.Since(lastError) >= time.Minute {
						logger.WarnContext(ctx, "telemetry.backlog_sample_failed",
							"task_queue", queue, "operation", "DescribeTaskQueueEnhanced", "report_stats", true, "error_type", fmt.Sprintf("%T", err))
						lastError = time.Now()
					}
					continue
				}
				var n int64
				for _, version := range description.VersionsInfo {
					for _, info := range version.TypesInfo {
						if info.Stats != nil {
							n += info.Stats.ApproximateBacklogCount
						}
					}
				}
				telemetry.Gauge("orbit.temporal.task_queue.backlog").Record(ctx, n, attribute.String("orbit.task_queue", queue))
			}
		case <-quota.C:
			sampleQuota(ctx, d)
		}
	}
}
func sampleQuota(ctx context.Context, d *db.DB) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = d.Global(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT metric,count(*) FILTER (WHERE ratio>=0.8),count(*) FILTER (WHERE ratio>=1)
 FROM ops_quota_utilization() GROUP BY metric`)
		if err != nil {
			return err
		}
		defer rows.Close()
		counts := map[string][2]int64{"millicredits": {}, "tokens": {}}
		for rows.Next() {
			var key string
			var a, b int64
			if err = rows.Scan(&key, &a, &b); err != nil {
				return err
			}
			counts[key] = [2]int64{a, b}
		}
		if err = rows.Err(); err != nil {
			return err
		}
		for key, values := range counts {
			for i, threshold := range []string{"0.8", "1.0"} {
				telemetry.Gauge("orbit.quota.policies_over_threshold").Record(ctx, values[i],
					attribute.String("orbit.quota.metric", key), attribute.String("orbit.threshold", threshold))
			}
		}
		return nil
	})
}

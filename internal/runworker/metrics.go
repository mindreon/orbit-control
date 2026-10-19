package runworker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/client"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
)

const metricsSampleInterval = time.Minute

type runKey struct{ tenant, status string }
type interactionCount struct {
	kind   string
	n      int64
	oldest time.Time
}
type metricsSnapshot struct {
	sampledAt    time.Time
	runs         map[runKey]int64
	interactions []interactionCount
}

// Metrics publishes immutable snapshots; its mutex never covers IO or OTel work.
type Metrics struct {
	d        *db.DB
	now      func() time.Time
	mu       sync.Mutex
	snapshot metricsSnapshot
}

// Observe registers snapshot-only gauges in worker processes.
func Observe(d *db.DB) (*Metrics, func(), error) {
	return observe(d, time.Now)
}

func observe(d *db.DB, now func() time.Time) (*Metrics, func(), error) {
	m := &Metrics{d: d, now: now}
	meter := otel.Meter("orbit-control")
	runs, err := meter.Int64ObservableGauge("orbit.runs.active", metric.WithUnit("{run}"))
	if err != nil {
		return nil, nil, err
	}
	pending, err := meter.Int64ObservableGauge("orbit.interactions.pending", metric.WithUnit("{interaction}"))
	if err != nil {
		return nil, nil, err
	}
	age, err := meter.Float64ObservableGauge("orbit.interactions.oldest_pending.age", metric.WithUnit("s"))
	if err != nil {
		return nil, nil, err
	}
	registration, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		m.mu.Lock()
		snapshot := m.snapshot
		m.mu.Unlock()
		now := m.now()
		if snapshot.sampledAt.IsZero() || now.Sub(snapshot.sampledAt) > 2*metricsSampleInterval {
			return nil
		}
		for group, n := range snapshot.runs {
			observer.ObserveInt64(runs, n, metric.WithAttributes(attribute.String("orbit.tenant_id", group.tenant),
				attribute.String("orbit.run.status", group.status)))
		}
		for _, count := range snapshot.interactions {
			attrs := metric.WithAttributes(attribute.String("orbit.interaction.kind", count.kind))
			observer.ObserveInt64(pending, count.n, attrs)
			observer.ObserveFloat64(age, now.Sub(count.oldest).Seconds(), attrs)
		}
		return nil
	}, runs, pending, age)
	if err != nil {
		return nil, nil, err
	}
	return m, func() { _ = registration.Unregister() }, nil
}

// SampleOnce reads only aggregate SECURITY DEFINER APIs in one bounded global
// transaction. Publish only complete successful samples, after the DB releases
// its transaction. Label mapping and all self-metric recording stay off callbacks.
func (m *Metrics) SampleOnce(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	snapshot := metricsSnapshot{sampledAt: m.now(), runs: map[runKey]int64{}}
	err := m.d.Global(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "SELECT tenant_id::text,status,n FROM ops_run_counts()")
		if err != nil {
			return err
		}
		for rows.Next() {
			var tenant, status string
			var n int64
			if err = rows.Scan(&tenant, &status, &n); err != nil {
				rows.Close()
				return err
			}
			snapshot.runs[runKey{tenant: tenant, status: status}] += n
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
			var count interactionCount
			if err = rows.Scan(&count.kind, &count.n, &count.oldest); err != nil {
				return err
			}
			snapshot.interactions = append(snapshot.interactions, count)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	counts := map[runKey]int64{}
	for group, n := range snapshot.runs {
		attrs := telemetry.Labels(ctx, "orbit.runs.active", attribute.String("orbit.tenant_id", group.tenant),
			attribute.String("orbit.run.status", group.status))
		counts[runKey{tenant: attrs[0].Value.AsString(), status: group.status}] += n
	}
	snapshot.runs = counts
	m.mu.Lock()
	m.snapshot = snapshot
	m.mu.Unlock()
	return nil
}

// Sample stops on the worker context and never queries a tenant business row.
func (m *Metrics) Sample(ctx context.Context, temporal client.Client, logger *slog.Logger) {
	var lastBacklogError, lastDBError time.Time
	backlog := time.NewTicker(30 * time.Second)
	quota := time.NewTicker(metricsSampleInterval)
	defer backlog.Stop()
	defer quota.Stop()
	sampleDB := func() {
		if err := m.SampleOnce(ctx); err != nil && m.now().Sub(lastDBError) >= time.Minute {
			logger.WarnContext(ctx, "telemetry.db_sample_failed", "error_type", fmt.Sprintf("%T", err))
			lastDBError = m.now()
		}
	}
	// Sample once at start so state gauges are not absent for the first interval after a restart.
	sampleDB()
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
					if m.now().Sub(lastBacklogError) >= time.Minute {
						logger.WarnContext(ctx, "telemetry.backlog_sample_failed",
							"task_queue", queue, "operation", "DescribeTaskQueueEnhanced", "report_stats", true, "error_type", fmt.Sprintf("%T", err))
						lastBacklogError = m.now()
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
			sampleDB()
			sampleQuota(ctx, m.d)
			sampleUsageBacklog(ctx, m.d)
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

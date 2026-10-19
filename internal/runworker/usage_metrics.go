package runworker

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
)

func sampleUsageBacklog(ctx context.Context, d *db.DB) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var count int64
	var oldest *time.Time
	err := d.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT dirty_hours,oldest_marked FROM ops_usage_rollup_backlog()").Scan(&count, &oldest)
	})
	if err != nil {
		return
	}
	var age int64
	if oldest != nil {
		age = max(0, int64(time.Since(*oldest).Seconds()))
	}
	telemetry.Gauge("orbit.usage.rollup_backlog").Record(ctx, count)
	telemetry.Gauge("orbit.usage.rollup_oldest_dirty_age").Record(ctx, age)
}

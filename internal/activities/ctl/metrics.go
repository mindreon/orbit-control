package ctl

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"

	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type measurementsKey struct{}
type measurement struct {
	name   string
	value  float64
	count  bool
	labels []attribute.KeyValue
}

// measuredChange holds measurements locally until change returns nil. Nothing
// is recorded on rollback, and change's outbox delivery semantics stay intact.
func (a *Activities) measuredChange(ctx context.Context, in w.RunOp, fn func(context.Context, pgx.Tx) ([]event, error)) error {
	var pending []measurement
	ctx = context.WithValue(ctx, measurementsKey{}, &pending)
	if err := a.change(ctx, in, fn); err != nil {
		return err
	}
	for _, point := range pending {
		if point.count {
			telemetry.Counter(point.name).Add(ctx, int64(point.value), point.labels...)
		} else {
			telemetry.Histogram(point.name).Record(ctx, point.value, point.labels...)
		}
	}
	return nil
}

func stageMeasurement(ctx context.Context, name string, value float64, count bool, labels ...attribute.KeyValue) {
	if pending, ok := ctx.Value(measurementsKey{}).(*[]measurement); ok {
		*pending = append(*pending, measurement{name: name, value: value, count: count, labels: labels})
	}
}

func stageInteractionRows(ctx context.Context, rows pgx.Rows, outcome string) error {
	defer rows.Close()
	for rows.Next() {
		var kind string
		var created, resolved time.Time
		if err := rows.Scan(&kind, &created, &resolved); err != nil {
			return err
		}
		stageMeasurement(ctx, "orbit.interaction.wait.duration", resolved.Sub(created).Seconds(), false,
			attribute.String("orbit.interaction.kind", kind), attribute.String("orbit.interaction.outcome", outcome))
	}
	return rows.Err()
}

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/platform/db"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type failingTelemetryPublisher struct {
	stage string
	admin *pgx.Conn
}

func (p failingTelemetryPublisher) Publish(ctx context.Context, subject, _ string, _ []byte) error {
	if p.stage == "publish" || p.stage == "end_marker" && strings.HasSuffix(subject, ".end") {
		return errors.New("ORBIT-CANARY-error-message")
	}
	if p.stage == "mark_published" && !strings.HasSuffix(subject, ".end") {
		_, err := p.admin.Exec(ctx, `CREATE FUNCTION telemetry_mark_abort() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'fixture'; END $$;
 CREATE TRIGGER telemetry_mark_abort BEFORE UPDATE ON control_events FOR EACH ROW EXECUTE FUNCTION telemetry_mark_abort()`)
		return err
	}
	return nil
}

func testTelemetryOutbox(t *testing.T, d *db.DB, admin *pgx.Conn, reader *sdkmetric.ManualReader) {
	for _, stage := range []string{"publish", "end_marker", "mark_published"} {
		f := newRunFixture(t, d, admin)
		ctl.InitializeMetrics(f.activities)
		f.start(t)
		// Previous events are already delivered; isolate one terminal event.
		_, err := admin.Exec(t.Context(), "UPDATE control_events SET published_at=now() WHERE tenant_id=$1", f.tenant)
		require.NoError(t, err)
		f.activities.NATS = failingTelemetryPublisher{stage: stage, admin: admin}
		require.NoError(t, f.activities.FinalizeRun(t.Context(), w.FinalizeInput{RunOp: f.op(), Status: "succeeded"}),
			"WP7 publish failures must not fail the committed Activity")
		if stage == "mark_published" {
			_, err := admin.Exec(t.Context(), "DROP TRIGGER telemetry_mark_abort ON control_events; DROP FUNCTION telemetry_mark_abort()")
			require.NoError(t, err)
		}
	}
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	stages := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name == "control_outbox_failures_total" {
				for _, point := range item.Data.(metricdata.Sum[int64]).DataPoints {
					stage, _ := point.Attributes.Value("stage")
					stages[stage.AsString()] += point.Value
				}
			}
		}
	}
	require.Equal(t, map[string]int64{"publish": 1, "end_marker": 1, "mark_published": 1}, stages)
}

package integration

import (
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/models"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	"github.com/mindreon/orbit-control/internal/runworker"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func metricTotal(t *testing.T, reader *sdkmetric.ManualReader, name string) float64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var total float64
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name != name {
				continue
			}
			switch values := item.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range values.DataPoints {
					total += float64(point.Value)
				}
			case metricdata.Histogram[float64]:
				for _, point := range values.DataPoints {
					total += float64(point.Count)
				}
			}
		}
	}
	return total
}

func testTelemetry(t *testing.T, d *db.DB, admin *pgx.Conn) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(append(telemetry.MeterOptions(), sdkmetric.WithReader(reader))...)
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	telemetry.InitializeMetrics()
	t.Cleanup(func() {
		otel.SetMeterProvider(old)
		_ = provider.Shutdown(context.Background())
	})
	for _, team := range []bool{false, true} {
		f := newRunFixture(t, d, admin)
		publisher := &telemetryPublisher{}
		f.activities.NATS = publisher
		if team {
			f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, "UPDATE sessions SET entry_kind='team' WHERE tenant_id=$1 AND id=$2", f.tenant, f.session)
				return err
			})
		}
		sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
		ctx := trace.ContextWithSpanContext(t.Context(), sc)
		op := f.op()
		before := metricTotal(t, reader, "orbit.runs.started")
		for attempt := range 2 {
			if attempt == 1 {
				ctx = trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
					TraceID: trace.TraceID{3}, SpanID: trace.SpanID{4}, TraceFlags: trace.FlagsSampled}))
			}
			if team {
				require.NoError(t, f.activities.PrepareTeamRun(ctx, w.PrepareTeamInput{RunOp: op, Budget: 100,
					Team: w.TeamSpec{Roster: []contract.RosterEntry{}}, Board: contract.TeamBoard{Roster: []contract.RosterEntry{}}}))
			} else {
				_, err := f.activities.PrepareRun(ctx, w.PrepareInput{RunOp: op, Budget: 100})
				require.NoError(t, err)
			}
		}
		require.Equal(t, before+1, metricTotal(t, reader, "orbit.runs.started"))
		var saved string
		require.NoError(t, admin.QueryRow(t.Context(), "SELECT trace_id FROM runs WHERE id=$1", f.run).Scan(&saved))
		require.Equal(t, sc.TraceID().String(), saved, "the first ctl Activity owns trace_id permanently")
		start := w.StartSegmentInput{RunOp: f.op()}
		before = metricTotal(t, reader, "orbit.run.start.latency")
		for range 2 {
			require.NoError(t, f.activities.StartSegment(ctx, start))
		}
		require.Equal(t, before+1, metricTotal(t, reader, "orbit.run.start.latency"))
		for range 2 {
			_, err := admin.Exec(t.Context(), `INSERT INTO interactions(id,tenant_id,run_id,member,workflow_id,kind,request,approvers,status)
 VALUES($1,$2,$3,'main','fixture','ask_user','{}','[]','pending')`, ids.New(), f.tenant, f.run)
			require.NoError(t, err)
		}
		finished, duration, waits := metricTotal(t, reader, "orbit.runs.finished"), metricTotal(t, reader, "orbit.run.duration"),
			metricTotal(t, reader, "orbit.interaction.wait.duration")
		final := w.FinalizeInput{RunOp: f.op(), Status: "succeeded"}
		for range 2 {
			if team {
				require.NoError(t, f.activities.FinalizeTeamRun(ctx, w.FinalizeTeamInput{RunOp: final.RunOp, Status: final.Status}))
			} else {
				require.NoError(t, f.activities.FinalizeRun(ctx, final))
			}
		}
		var terminated int
		require.NoError(t, admin.QueryRow(t.Context(), `SELECT count(*) FROM run_segments
 WHERE tenant_id=$1 AND run_id=$2 AND end_reason='run_terminated'`, f.tenant, f.run).Scan(&terminated))
		require.Equal(t, 1, terminated, "both single-agent and team finalization settle still-running segments")
		require.Equal(t, finished+1, metricTotal(t, reader, "orbit.runs.finished"))
		require.Equal(t, duration+1, metricTotal(t, reader, "orbit.run.duration"))
		require.Equal(t, waits+2, metricTotal(t, reader, "orbit.interaction.wait.duration"))
		rows, err := admin.Query(t.Context(), "SELECT payload FROM control_events WHERE tenant_id=$1 AND run_id=$2", f.tenant, f.run)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			require.NoError(t, rows.Scan(&raw))
			var event map[string]any
			require.NoError(t, json.Unmarshal(raw, &event))
			require.NotContains(t, event, "trace")
			require.NotContains(t, string(raw), saved)
		}
		require.NoError(t, rows.Err())
		rows.Close()
		require.NotEmpty(t, publisher.events)
		for _, raw := range publisher.events {
			var published map[string]any
			require.NoError(t, json.Unmarshal(raw, &published))
			require.NotContains(t, published, "trace")
			require.NotContains(t, string(raw), saved)
		}
	}
	t.Run("SamplerReleasesTransaction", func(t *testing.T) {
		f := newRunFixture(t, d, admin)
		f.start(t)
		metrics, unregister, err := runworker.Observe(d)
		require.NoError(t, err)
		defer unregister()
		for range 3 {
			require.NoError(t, metrics.SampleOnce(t.Context()))
			var data metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &data))
			found := false
			for _, scope := range data.ScopeMetrics {
				for _, item := range scope.Metrics {
					if item.Name == "orbit.runs.active" {
						for _, point := range item.Data.(metricdata.Gauge[int64]).DataPoints {
							tenant, _ := point.Attributes.Value("orbit.tenant_id")
							if tenant.AsString() == f.tenant {
								require.Equal(t, int64(1), point.Value)
								found = true
							}
						}
					}
				}
			}
			require.True(t, found, "new tenant must appear in sampled run gauges")
			var idle int
			require.NoError(t, admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity
 WHERE datname=current_database() AND state LIKE 'idle in transaction%'`).Scan(&idle))
			require.Zero(t, idle, "sampler and collection must not strand transactions")
		}
	})
	t.Run("Rollback", func(t *testing.T) {
		f := newRunFixture(t, d, admin)
		_, err := admin.Exec(t.Context(), `CREATE FUNCTION telemetry_abort() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'telemetry rollback fixture'; END $$;
 CREATE TRIGGER telemetry_abort BEFORE INSERT ON control_events FOR EACH ROW EXECUTE FUNCTION telemetry_abort()`)
		require.NoError(t, err)
		defer func() {
			_, err := admin.Exec(context.Background(), "DROP TRIGGER telemetry_abort ON control_events; DROP FUNCTION telemetry_abort()")
			require.NoError(t, err)
		}()
		before := metricTotal(t, reader, "orbit.runs.started")
		_, err = f.activities.PrepareRun(t.Context(), w.PrepareInput{RunOp: f.op(), Budget: 100})
		require.Error(t, err)
		require.Equal(t, before, metricTotal(t, reader, "orbit.runs.started"))
		var started *time.Time
		require.NoError(t, admin.QueryRow(t.Context(), "SELECT started_at FROM runs WHERE id=$1", f.run).Scan(&started))
		require.Nil(t, started)
	})
	t.Run("InteractionRetries", func(t *testing.T) { testTelemetryInteractions(t, d, admin, reader) })
	t.Run("TransitionRollbacks", func(t *testing.T) { testTelemetryRollbacks(t, d, admin, reader) })
	t.Run("OutboxStages", func(t *testing.T) { testTelemetryOutbox(t, d, admin, reader) })
	t.Run("TokensAndRollback", func(t *testing.T) { testTelemetryTokens(t, d, admin, reader) })
	t.Run("Functions", func(t *testing.T) {
		for name, columns := range map[string]string{"ops_run_counts": "tenant_id uuid, status text, n bigint",
			"ops_interaction_counts": "kind text, n bigint, oldest timestamp with time zone",
			"ops_quota_utilization":  "tenant_id uuid, policy_id uuid, metric text, ratio numeric"} {
			var public, app, worker bool
			var result, config string
			require.NoError(t, admin.QueryRow(t.Context(), `SELECT has_function_privilege('orbit_app',oid,'EXECUTE'),
 has_function_privilege('orbit_worker',oid,'EXECUTE'),
 EXISTS(SELECT 1 FROM aclexplode(proacl) acl WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE'),
 pg_get_function_result(oid),array_to_string(proconfig,',') FROM pg_proc WHERE proname=$1`, name).
				Scan(&app, &worker, &public, &result, &config))
			require.True(t, app)
			require.False(t, worker)
			require.False(t, public)
			require.Equal(t, "TABLE("+columns+")", result)
			require.Contains(t, config, "search_path=public, pg_temp")
		}
	})
}

func testTelemetryTokens(t *testing.T, d *db.DB, admin *pgx.Conn, reader *sdkmetric.ManualReader) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	pricing := `{"input_per_mtok_millicredits":1000000,"output_per_mtok_millicredits":1000000,
 "cache_read_per_mtok_millicredits":1000000,"cache_write_per_mtok_millicredits":1000000}`
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "UPDATE run_segments SET model_profiles=$3 WHERE tenant_id=$1 AND run_id=$2",
			f.tenant, f.run, []byte(`{"`+models.BindingKey(1, "test-model")+`":{"pricing":`+pricing+`}}`))
		return err
	})
	report := contract.UsageReport{Items: []contract.UsageItem{{Model: "test-model", InputTokens: 7, OutputTokens: 4,
		CacheReadTokens: 3, CacheWriteTokens: 2, OccurredAt: time.Now().UTC(), IdempotencyKey: "model:" + f.run + ":main:1:1:1"}}}
	token := f.token(t)
	path := "/internal/v1/runs/" + f.run + "/usage"
	before := metricTotal(t, reader, "orbit.usage.tokens")
	// The handler produces measurements successfully, then a deferred constraint
	// fails at commit. Returning points from the route must not publish them yet.
	_, err := admin.Exec(t.Context(), `CREATE FUNCTION telemetry_usage_abort() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'commit fixture'; END $$;
 CREATE CONSTRAINT TRIGGER telemetry_usage_abort AFTER INSERT ON usage_keys
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION telemetry_usage_abort()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(),
			"DROP TRIGGER IF EXISTS telemetry_usage_abort ON usage_keys; DROP FUNCTION IF EXISTS telemetry_usage_abort()")
		require.NoError(t, err)
	})
	status, _ := f.request(t, "POST", path, token, report)
	require.Equal(t, 500, status)
	require.Equal(t, before, metricTotal(t, reader, "orbit.usage.tokens"))
	_, err = admin.Exec(t.Context(), "DROP TRIGGER telemetry_usage_abort ON usage_keys; DROP FUNCTION telemetry_usage_abort()")
	require.NoError(t, err)
	for range 2 {
		status, body := f.request(t, "POST", path, token, report)
		require.Equal(t, 200, status, string(body))
	}

	require.Equal(t, before+16, metricTotal(t, reader, "orbit.usage.tokens"))
	var total int64
	require.NoError(t, admin.QueryRow(t.Context(), "SELECT sum(quantity)::bigint FROM usage_ledger WHERE run_id=$1", f.run).Scan(&total))
	require.Equal(t, int64(16), total)
}

// telemetryPublisher captures only actual RunEvent publications, not end markers.
type telemetryPublisher struct{ events [][]byte }

func (p *telemetryPublisher) Publish(_ context.Context, _ string, _ string, raw []byte) error {
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	if _, ok := event["type"]; ok {
		p.events = append(p.events, append([]byte(nil), raw...))
	}
	return nil
}

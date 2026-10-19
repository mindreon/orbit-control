package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	"github.com/mindreon/orbit-control/internal/usage"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/usagerollup"
	"github.com/mindreon/orbit-control/migrations"
)

func rollupDatabase(t *testing.T) (*db.DB, *pgx.Conn, *db.DB) {
	t.Helper()
	telemetry.InitializeMetrics()
	raw := os.Getenv("ORBIT_TEST_DATABASE_URL")
	if raw == "" {
		if os.Getenv("ORBIT_REQUIRE_PG_TESTS") == "1" {
			t.Fatal("ORBIT_TEST_DATABASE_URL required")
		}
		t.Skip("ORBIT_TEST_DATABASE_URL not set")
	}
	require.NoError(t, migrations.Run(t.Context(), roleURL(t, raw, "orbit_owner"), "up"))
	app, err := db.OpenApp(t.Context(), roleURL(t, raw, "orbit_app"))
	require.NoError(t, err)
	t.Cleanup(app.Close)
	admin, err := pgx.Connect(t.Context(), raw)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close(context.Background())) })
	owner, err := db.Open(t.Context(), raw)
	require.NoError(t, err)
	t.Cleanup(owner.Close)
	return app, admin, owner
}

func usageTenant(t *testing.T, admin *pgx.Conn) string {
	t.Helper()
	id := ids.New()
	_, err := admin.Exec(t.Context(), `INSERT INTO tenants(id,slug,name)VALUES($1,$1::uuid::text,'rollup fixture')`, id)
	require.NoError(t, err)
	return id
}

func recordHour(ctx context.Context, d *db.DB, tenant, key string, at, now time.Time) (bool, error) {
	var added bool
	err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		added, err = (usage.Service{Now: func() time.Time { return now }}).RecordUsage(ctx, tx, usage.Entry{
			TenantID: tenant, Source: "model", Meter: usage.TokensInput, Quantity: "7", UnitPrice: "1", Millicredits: 3,
			IdempotencyKey: key, OccurredAt: at})
		return err
	})
	return added, err
}

func assertRINV(t *testing.T, admin *pgx.Conn, tenants []string) {
	t.Helper()
	var differences int
	require.NoError(t, admin.QueryRow(t.Context(), `WITH ledger AS (
 SELECT tenant_id,date_trunc('hour',occurred_at,'UTC') period_start,
 COALESCE(workspace_id,'00000000-0000-0000-0000-000000000000') workspace_id,
 COALESCE(principal_id,'00000000-0000-0000-0000-000000000000') principal_id,COALESCE(asset_ref,'') asset_ref,
 COALESCE(model_profile_id,'00000000-0000-0000-0000-000000000000') model_profile_id,COALESCE(member,'') member,
 source,meter,sum(quantity) quantity,sum(millicredits)::bigint millicredits,count(*) entries FROM usage_ledger
 WHERE tenant_id=ANY($1::uuid[]) GROUP BY 1,2,3,4,5,6,7,8,9), materialized AS (
 SELECT tenant_id,period_start,workspace_id,principal_id,asset_ref,model_profile_id,member,source,meter,quantity,millicredits,entries
 FROM usage_rollups WHERE tenant_id=ANY($1::uuid[])), clean_ledger AS (
 SELECT l.* FROM ledger l WHERE NOT EXISTS(SELECT 1 FROM usage_rollup_dirty d
 WHERE d.tenant_id=l.tenant_id AND d.period_start=l.period_start)),clean_rollups AS (
 SELECT r.* FROM materialized r WHERE NOT EXISTS(SELECT 1 FROM usage_rollup_dirty d
 WHERE d.tenant_id=r.tenant_id AND d.period_start=r.period_start))
 SELECT count(*) FROM ((SELECT * FROM clean_ledger EXCEPT SELECT * FROM clean_rollups)
 UNION ALL (SELECT * FROM clean_rollups EXCEPT SELECT * FROM clean_ledger)) diff`, tenants).Scan(&differences))
	require.Zero(t, differences, "clean hours must match the ledger in every dimension and both directions")
}

func rollAll(t *testing.T, d *db.DB, before time.Time) {
	t.Helper()
	for {
		page, err := usage.ListDirtyHours(t.Context(), d, usage.DirtyPage{Before: before, Limit: 50})
		require.NoError(t, err)
		if len(page) == 0 {
			return
		}
		for _, h := range page {
			require.NoError(t, usage.RollupHour(t.Context(), d, h, nil))
		}
	}
}

func runUsageWorkflow(t *testing.T, d *db.DB, before time.Time) int {
	t.Helper()
	input := w.UsageRollupInput{Before: before}
	runs := 0
	for {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.SetContinueAsNewSuggested(true)
		env.RegisterWorkflowWithOptions(usagerollup.Run, workflow.RegisterOptions{Name: w.UsageRollupWorkflow})
		env.RegisterActivityWithOptions(func(ctx context.Context, in w.DirtyPage) ([]w.DirtyHour, error) {
			in.Limit = 50
			return usage.ListDirtyHours(ctx, d, in)
		}, activity.RegisterOptions{Name: "ListDirtyHours"})
		env.RegisterActivityWithOptions(func(ctx context.Context, in w.DirtyHour) error { return usage.RollupHour(ctx, d, in, nil) },
			activity.RegisterOptions{Name: "RollupHour"})
		env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "rollup-fixture"})
		env.ExecuteWorkflow(w.UsageRollupWorkflow, input)
		runs++
		var can *workflow.ContinueAsNewError
		err := env.GetWorkflowError()
		if errors.As(err, &can) {
			previous := input
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &input))
			require.True(t, input.AfterStart.After(previous.AfterStart) ||
				input.AfterStart.Equal(previous.AfterStart) && input.AfterTenant > previous.AfterTenant)
			continue
		}
		require.NoError(t, err)
		break
	}
	return runs
}

func TestUsageRollup(t *testing.T) {
	d, admin, owner := rollupDatabase(t)
	tenants := []string{usageTenant(t, admin), usageTenant(t, admin), usageTenant(t, admin)}
	start := time.Date(2000, 10, 31, 22, 0, 0, 0, time.UTC)
	// One month has a real partition; the following month deliberately uses default.
	_, err := admin.Exec(t.Context(), `SET ROLE orbit_owner;
 DO $$ BEGIN
 IF to_regclass('usage_ledger_200010') IS NULL THEN
 CREATE TABLE usage_ledger_200010 (LIKE usage_ledger INCLUDING ALL);
 ALTER TABLE usage_ledger_200010 ADD CHECK(occurred_at >= '2000-10-01T00:00:00Z' AND occurred_at < '2000-11-01T00:00:00Z');
 WITH moved AS (DELETE FROM usage_ledger_default
 WHERE occurred_at >= '2000-10-01T00:00:00Z' AND occurred_at < '2000-11-01T00:00:00Z' RETURNING *)
 INSERT INTO usage_ledger_200010 SELECT * FROM moved;
 ALTER TABLE usage_ledger ATTACH PARTITION usage_ledger_200010
 FOR VALUES FROM ('2000-10-01T00:00:00Z') TO ('2000-11-01T00:00:00Z');
 END IF; END $$;
 ALTER TABLE usage_ledger_200010 ENABLE ROW LEVEL SECURITY;
 ALTER TABLE usage_ledger_200010 FORCE ROW LEVEL SECURITY;
 DROP POLICY IF EXISTS tenant_isolation ON usage_ledger_200010;
 CREATE POLICY tenant_isolation ON usage_ledger_200010
 USING(tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::uuid)
 WITH CHECK(tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::uuid);
 GRANT SELECT,INSERT ON usage_ledger_200010 TO orbit_app;
 RESET ROLE`)
	require.NoError(t, err)
	for h := range 72 {
		at := start.Add(time.Duration(h)*time.Hour + 55*time.Minute)
		for _, tenant := range tenants {
			added, err := recordHour(t.Context(), d, tenant, fmt.Sprint("history-", h), at, at)
			require.NoError(t, err)
			require.True(t, added)
		}
	}
	_, err = recordHour(t.Context(), d, tenants[1], "second-tenant-only", start, start)
	require.NoError(t, err)
	var count int
	require.NoError(t, admin.QueryRow(
		t.Context(), "SELECT count(*) FROM usage_rollup_dirty WHERE tenant_id=ANY($1::uuid[])",
		tenants).Scan(&count))
	require.Equal(t, 216, count)
	var partition string
	require.NoError(t, admin.QueryRow(
		t.Context(), "SELECT tableoid::regclass::text FROM usage_ledger WHERE tenant_id=$1 AND occurred_at >= '2000-11-01' LIMIT 1",
		tenants[0]).Scan(&partition))
	require.Equal(t, "usage_ledger_default", partition)
	runs := runUsageWorkflow(t, d, start.Add(74*time.Hour))
	require.Greater(t, runs, 1, "the catch-up exercise must cross a Continue-As-New boundary")
	require.NoError(t, admin.QueryRow(t.Context(),
		"SELECT count(*) FROM usage_rollup_dirty WHERE tenant_id=ANY($1::uuid[])", tenants).Scan(&count))
	require.Zero(t, count, "the Workflow itself must drain every fixture hour")
	assertRINV(t, admin, tenants)
	snapshot := func() string {
		var rows string
		require.NoError(t, admin.QueryRow(t.Context(), `SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY tenant_id,period_start,meter),'[]')::text
 FROM usage_rollups r WHERE tenant_id=ANY($1::uuid[])`, tenants).Scan(&rows))
		return rows
	}
	before := snapshot()
	runUsageWorkflow(t, d, start.Add(74*time.Hour))
	require.Equal(t, before, snapshot(), "rerunning a clean Workflow must leave rollups unchanged")
	for i, tenant := range tenants {
		require.NoError(t, d.WithTenant(t.Context(), tenant, func(tx pgx.Tx) error {
			var quantity int64
			var tenantIDs []string
			if err := tx.QueryRow(t.Context(), `SELECT sum(quantity)::bigint,array_agg(DISTINCT tenant_id::text)
 FROM usage_rollups`).Scan(&quantity, &tenantIDs); err != nil {
				return err
			}
			want := int64(72 * 7)
			if i == 1 {
				want += 7
			}
			require.Equal(t, want, quantity, "other tenants' distinct usage must never enter this tenant's rollups")
			require.Equal(t, []string{tenant}, tenantIDs)
			return nil
		}))
	}
	t.Run("LateAndClamp", func(t *testing.T) {
		h := start
		added, err := recordHour(t.Context(), d, tenants[0], "late", h.Add(55*time.Minute), h.Add(110*time.Minute))
		require.NoError(t, err)
		require.True(t, added)
		require.NoError(t, usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenants[0], Hour: h}, nil))
		assertRINV(t, admin, tenants)
		now := h.Add(7*24*time.Hour + 30*time.Minute)
		added, err = recordHour(t.Context(), d, tenants[0], "week-old", h, now)
		require.NoError(t, err)
		require.True(t, added)
		var occurred, keyAt, dirty time.Time
		require.NoError(t, admin.QueryRow(t.Context(), `SELECT l.occurred_at,k.created_at,d.period_start
 FROM usage_ledger l JOIN usage_keys k USING(tenant_id,idempotency_key)
 JOIN usage_rollup_dirty d ON d.tenant_id=l.tenant_id AND d.period_start=date_trunc('hour',l.occurred_at,'UTC')
 WHERE l.tenant_id=$1 AND l.idempotency_key='week-old'`, tenants[0]).Scan(&occurred, &keyAt, &dirty))
		require.True(t, now.Equal(occurred))
		require.True(t, now.Equal(keyAt))
		require.True(t, now.Truncate(time.Hour).Equal(dirty))
		var oldDirty bool
		require.NoError(t, admin.QueryRow(
			t.Context(), `SELECT EXISTS(SELECT 1 FROM usage_rollup_dirty WHERE tenant_id=$1 AND period_start=$2)`,
			tenants[0], h).Scan(&oldDirty))
		require.False(t, oldDirty)
		added, err = recordHour(t.Context(), d, tenants[0], "week-old", h.Add(-time.Hour), now)
		require.NoError(t, err)
		require.False(t, added)
		rollAll(t, d, now.Add(time.Hour))
		assertRINV(t, admin, tenants)
	})
	t.Run("MarkAndChecks", func(t *testing.T) {
		tenant := tenants[0]
		_, err := admin.Exec(t.Context(), `INSERT INTO usage_ledger(id,tenant_id,source,meter,quantity,
 unit_price,millicredits,idempotency_key,occurred_at)
 VALUES($1,$2,'model','tokens.output',5,1,4,'manual',$3)`, ids.New(), tenant, start)
		require.NoError(t, err)
		var differs bool
		require.NoError(t, admin.QueryRow(t.Context(), `SELECT
 (SELECT sum(quantity) FROM usage_ledger WHERE tenant_id=$1 AND occurred_at >= $2 AND occurred_at < $2+interval '1 hour')
 IS DISTINCT FROM (SELECT sum(quantity) FROM usage_rollups WHERE tenant_id=$1 AND period_start=$2)`, tenant, start).Scan(&differs))
		require.True(t, differs, "negative control must detect an unmarked ledger row")
		require.NoError(t, usage.MarkRollups(t.Context(), owner, start, start.Add(time.Hour), tenant))
		rollAll(t, d, start.Add(time.Hour))
		assertRINV(t, admin, tenants)
		for _, meter := range []string{"tokens", "unknown"} {
			_, err = admin.Exec(t.Context(), `INSERT INTO usage_ledger(id,tenant_id,source,meter,quantity,
 unit_price,millicredits,idempotency_key,occurred_at)
 VALUES($1,$2,'model',$3,1,1,1,$1::uuid::text,$4)`, ids.New(), tenant, meter, start)
			require.Error(t, err)
		}
	})
}

func TestRollupConcurrency(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	tenant := usageTenant(t, admin)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	var wg sync.WaitGroup
	errors := make(chan error, 10)
	done := make(chan struct{})
	for worker := range 8 {
		wg.Go(func() {
			for i := range 250 {
				at := hour.Add(time.Duration(rand.IntN(3)) * time.Hour)
				_, err := recordHour(t.Context(), d, tenant, fmt.Sprintf("%d-%d", worker, i), at, at)
				if err != nil {
					errors <- err
					return
				}
			}
		})
	}
	var rollups sync.WaitGroup
	for range 2 {
		rollups.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				page, err := usage.ListDirtyHours(t.Context(), d, usage.DirtyPage{Before: hour.Add(4 * time.Hour), Limit: 1000})
				if err != nil {
					errors <- err
					return
				}
				for _, h := range page {
					if h.TenantID == tenant {
						if err = usage.RollupHour(t.Context(), d, h, nil); err != nil {
							errors <- err
							return
						}
					}
				}
			}
		})
	}
	wg.Wait()
	close(done)
	rollups.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	rollAll(t, d, hour.Add(4*time.Hour))
	assertRINV(t, admin, []string{tenant})
	var n, dirty int
	require.NoError(t, admin.QueryRow(t.Context(), "SELECT count(*) FROM usage_ledger WHERE tenant_id=$1", tenant).Scan(&n))
	require.Equal(t, 2000, n)
	require.NoError(t, admin.QueryRow(t.Context(), "SELECT count(*) FROM usage_rollup_dirty WHERE tenant_id=$1", tenant).Scan(&dirty))
	require.Zero(t, dirty)
}

func TestUsageDedupDirty(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	tenant := usageTenant(t, admin)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	for attempt := range 2 {
		added, err := recordHour(t.Context(), d, tenant, "same", hour, hour)
		require.NoError(t, err)
		require.Equal(t, attempt == 0, added)
	}
	var generation int64
	require.NoError(t, admin.QueryRow(t.Context(), "SELECT generation FROM usage_rollup_dirty WHERE tenant_id=$1", tenant).Scan(&generation))
	require.Equal(t, int64(1), generation)
	require.NoError(t, usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: hour}, nil))
	added, err := recordHour(t.Context(), d, tenant, "same", hour, hour)
	require.NoError(t, err)
	require.False(t, added)
	var count int
	require.NoError(t, admin.QueryRow(t.Context(), "SELECT count(*) FROM usage_rollup_dirty WHERE tenant_id=$1", tenant).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: hour}, nil))
	assertRINV(t, admin, []string{tenant})
}

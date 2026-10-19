package integration

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/usage"
)

func waitUsageLock(t *testing.T, admin *pgx.Conn, tenant string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		var waiting bool
		err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
 WHERE NOT l.granted AND a.datname=current_database()
 AND (a.query LIKE '%usage_rollup%' OR a.query LIKE '%pg_advisory_xact_lock%'))`).Scan(&waiting)
		require.NoError(t, err)
		if waiting {
			return
		}
	}
	t.Fatal("writer/rollup did not wait on a real PG lock", tenant)
}

func TestRollupLockProtocol(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	h := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "WriterAfterDelete", true: "WriterAfterRollback"}[rollback], func(t *testing.T) {
			tenant := usageTenant(t, admin)
			_, err := recordHour(t.Context(), d, tenant, "base", h, h)
			require.NoError(t, err)
			holding, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			rdone := make(chan error, 1)
			go func() {
				rdone <- usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: h}, func(pgx.Tx) error {
					close(holding)
					<-release
					if rollback {
						return errors.New("fixture rollback")
					}
					return nil
				})
			}()
			<-holding
			wdone := make(chan error, 1)
			go func() {
				_, err := recordHour(t.Context(), d, tenant, "late", h, h)
				wdone <- err
			}()
			waitUsageLock(t, admin, tenant)
			unblock()
			err = <-rdone
			if rollback {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, <-wdone)
			var generation int64
			require.NoError(t, admin.QueryRow(
				t.Context(), `SELECT generation FROM usage_rollup_dirty WHERE tenant_id=$1 AND period_start=$2`,
				tenant, h).Scan(&generation))
			if rollback {
				require.Equal(t, int64(2), generation)
			} else {
				require.Equal(t, int64(1), generation)
				var entries int64
				require.NoError(t, admin.QueryRow(
					t.Context(), `SELECT sum(entries)::bigint FROM usage_rollups WHERE tenant_id=$1`,
					tenant).Scan(&entries))
				require.Equal(t, int64(1), entries)
			}
			require.NoError(t, usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: h}, nil))
			assertRINV(t, admin, []string{tenant})
		})
	}
	for _, exists := range []bool{false, true} {
		t.Run(map[bool]string{false: "UncommittedNewDirty", true: "UncommittedExistingDirty"}[exists], func(t *testing.T) {
			tenant := usageTenant(t, admin)
			if exists {
				_, err := recordHour(t.Context(), d, tenant, "base", h, h)
				require.NoError(t, err)
			}
			writer, err := pgx.Connect(t.Context(), roleURL(t, os.Getenv("ORBIT_TEST_DATABASE_URL"), "orbit_app"))
			require.NoError(t, err)
			defer writer.Close(t.Context())
			tx, err := writer.Begin(t.Context())
			require.NoError(t, err)
			defer tx.Rollback(t.Context())
			_, err = tx.Exec(t.Context(), "SELECT set_config('app.tenant_id',$1,true)", tenant)
			require.NoError(t, err)
			_, err = (usage.Service{Now: func() time.Time { return h }}).RecordUsage(t.Context(), tx, usage.Entry{TenantID: tenant,
				Source: "model", Meter: usage.TokensInput, Quantity: "1", UnitPrice: "1", Millicredits: 1, IdempotencyKey: "pending", OccurredAt: h})
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: h}, nil) }()
			if exists {
				waitUsageLock(t, admin, tenant)
			} else {
				require.NoError(t, <-done)
			}
			require.NoError(t, tx.Commit(t.Context()))
			if exists {
				require.NoError(t, <-done)
			} else {
				var count int
				require.NoError(t, admin.QueryRow(t.Context(), "SELECT count(*) FROM usage_rollup_dirty WHERE tenant_id=$1", tenant).Scan(&count))
				require.Equal(t, 1, count)
			}
			require.NoError(t, usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: h}, nil))
			assertRINV(t, admin, []string{tenant})
		})
	}
	t.Run("GenerationConflict", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		old := otel.GetMeterProvider()
		otel.SetMeterProvider(provider)
		t.Cleanup(func() {
			otel.SetMeterProvider(old)
			_ = provider.Shutdown(context.Background())
		})
		tenant := usageTenant(t, admin)
		_, err := recordHour(t.Context(), d, tenant, "base", h, h)
		require.NoError(t, err)
		err = usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: h}, func(tx pgx.Tx) error {
			_, err := tx.Exec(t.Context(), "UPDATE usage_rollup_dirty SET generation=generation+1 WHERE tenant_id=$1", tenant)
			return err
		})
		var app *temporal.ApplicationError
		require.ErrorAs(t, err, &app)
		require.True(t, app.NonRetryable())
		require.Equal(t, float64(1), metricTotal(t, reader, "orbit.usage.rollup_conflicts"))
		var count int
		require.NoError(t, admin.QueryRow(t.Context(), "SELECT count(*) FROM usage_rollups WHERE tenant_id=$1", tenant).Scan(&count))
		require.Zero(t, count)
		rollAll(t, d, h.Add(time.Hour))
		assertRINV(t, admin, []string{tenant})
	})
}

func TestUsagePrune(t *testing.T) {
	d, admin, owner := rollupDatabase(t)
	tenant := usageTenant(t, admin)
	var now time.Time
	require.NoError(t, admin.QueryRow(t.Context(), "SELECT now()").Scan(&now))
	h := now.UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	_, err := recordHour(t.Context(), d, tenant, "old", h, h)
	require.NoError(t, err)
	// The prior hour is now-1h-delta: old policy accepted it, the two-hour margin must reject it.
	for _, cutoff := range []time.Time{h.Add(time.Minute), now.UTC().Truncate(time.Hour), now.Add(-time.Hour).UTC().Truncate(time.Hour)} {
		require.ErrorContains(t, usage.Prune(t.Context(), owner, &cutoff), "cutoff must be a UTC hour")
	}
	cutoff := now.Add(-2 * time.Hour).UTC().Truncate(time.Hour)
	require.ErrorContains(t, usage.Prune(t.Context(), owner, &cutoff), "rollup not caught up")
	var count int
	require.NoError(t, admin.QueryRow(t.Context(), "SELECT count(*) FROM usage_ledger WHERE tenant_id=$1", tenant).Scan(&count))
	require.Equal(t, 1, count)
	rollAll(t, d, cutoff)
	require.NoError(t, usage.Prune(t.Context(), owner, &cutoff))
	var before string
	require.NoError(t, admin.QueryRow(
		t.Context(), "SELECT row_to_json(r)::text FROM usage_rollups r WHERE tenant_id=$1",
		tenant).Scan(&before))
	added, err := recordHour(t.Context(), d, tenant, "old", h, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, added)
	require.NoError(t, usage.MarkRollups(t.Context(), owner, h, cutoff, tenant))
	var oldDirty bool
	require.NoError(t, admin.QueryRow(
		t.Context(), "SELECT EXISTS(SELECT 1 FROM usage_rollup_dirty WHERE tenant_id=$1 AND period_start=$2)",
		tenant, h).Scan(&oldDirty))
	require.False(t, oldDirty)
	var after string
	require.NoError(t, admin.QueryRow(
		t.Context(), "SELECT row_to_json(r)::text FROM usage_rollups r WHERE tenant_id=$1 AND period_start=$2",
		tenant, h).Scan(&after))
	require.Equal(t, before, after)
}

func TestRollupMutualExclusion(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	tenant := usageTenant(t, admin)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	_, err := recordHour(t.Context(), d, tenant, "fixture", hour, hour)
	require.NoError(t, err)
	holding, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() {
		first <- usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: hour}, func(pgx.Tx) error {
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	var rewrites atomic.Int32
	go func() {
		second <- usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: tenant, Hour: hour}, func(pgx.Tx) error {
			rewrites.Add(1)
			return nil
		})
	}()
	waitUsageLock(t, admin, tenant)
	unblock()
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.Zero(t, rewrites.Load())
	assertRINV(t, admin, []string{tenant})
}

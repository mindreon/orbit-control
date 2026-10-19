package usage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// DirtyHour contains identifiers only, safe for Temporal history.
type DirtyHour = w.DirtyHour
type DirtyPage = w.DirtyPage

func ListDirtyHours(ctx context.Context, d *db.DB, in DirtyPage) ([]DirtyHour, error) {
	out := []DirtyHour{}
	err := d.Global(ctx, func(tx pgx.Tx) error {
		var after *time.Time
		var tenant *string
		if !in.AfterStart.IsZero() {
			after, tenant = &in.AfterStart, &in.AfterTenant
		}
		rows, err := tx.Query(ctx, `SELECT tenant_id::text,period_start FROM usage_rollup_dirty_page($1,$2,$3,$4)`,
			after, tenant, in.Before, in.Limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var h DirtyHour
			if err = rows.Scan(&h.TenantID, &h.Hour); err != nil {
				return err
			}
			out = append(out, h)
		}
		return rows.Err()
	})
	return out, err
}

// RollupHour recomputes one complete hour while holding its dirty-row lock.
// The optional hook is intended for transaction fault/concurrency tests. It runs
// after materialization and before generation-checked deletion, in the same tx.
func RollupHour(ctx context.Context, d *db.DB, in DirtyHour, hook func(pgx.Tx) error) error {
	conflict := false
	err := d.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error {
		var isolation string
		if err := tx.QueryRow(ctx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
			return err
		}
		if isolation != "read committed" {
			return temporal.NewNonRetryableApplicationError("usage: rollup requires read committed", "RollupIsolation", nil)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,8))`,
			"usage-rollup:"+in.TenantID+":"+in.Hour.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		var generation int64
		err := tx.QueryRow(ctx, `SELECT generation FROM usage_rollup_dirty
 WHERE tenant_id=$1 AND period_start=$2 FOR UPDATE`, in.TenantID, in.Hour).Scan(&generation)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM usage_rollups WHERE tenant_id=$1 AND period_start=$2`, in.TenantID, in.Hour); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO usage_rollups(tenant_id,period_start,workspace_id,principal_id,asset_ref,
 model_profile_id,member,source,meter,quantity,millicredits,entries)
 SELECT tenant_id,$2,COALESCE(workspace_id,'00000000-0000-0000-0000-000000000000'),
 COALESCE(principal_id,'00000000-0000-0000-0000-000000000000'),COALESCE(asset_ref,''),
 COALESCE(model_profile_id,'00000000-0000-0000-0000-000000000000'),COALESCE(member,''),source,meter,
 sum(quantity),sum(millicredits),count(*)
 FROM usage_ledger WHERE tenant_id=$1 AND occurred_at >= $2 AND occurred_at < $2 + interval '1 hour'
 GROUP BY 1,3,4,5,6,7,8,9`, in.TenantID, in.Hour); err != nil {
			return err
		}
		if hook != nil {
			if err = hook(tx); err != nil {
				return err
			}
		}
		n, err := tx.Exec(ctx, `DELETE FROM usage_rollup_dirty WHERE tenant_id=$1 AND period_start=$2 AND generation=$3`,
			in.TenantID, in.Hour, generation)
		if err != nil {
			return err
		}
		if n.RowsAffected() != 1 {
			conflict = true
			return temporal.NewNonRetryableApplicationError("usage: dirty generation changed", "RollupConflict", nil)
		}
		return nil
	})
	if conflict {
		// The rejected transaction has finished; no successful rollup is counted.
		telemetry.Counter("orbit.usage.rollup_conflicts").Add(ctx, 1)
	}
	return err
}

// MarkRollups uses an owner connection and marks only hours still in the ledger.
func MarkRollups(ctx context.Context, d *db.DB, from, to time.Time, tenant string) error {
	if !from.Equal(from.UTC().Truncate(time.Hour)) || !to.Equal(to.UTC().Truncate(time.Hour)) || !from.Before(to) {
		return fmt.Errorf("usage: range must contain complete UTC hours")
	}
	return d.Global(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO usage_rollup_dirty(tenant_id,period_start)
 SELECT DISTINCT tenant_id,date_trunc('hour',occurred_at,'UTC') FROM usage_ledger
 WHERE occurred_at >= $1 AND occurred_at < $2 AND ($3='' OR tenant_id=NULLIF($3,'')::uuid)
 ON CONFLICT(tenant_id,period_start) DO UPDATE SET generation=usage_rollup_dirty.generation+1`, from, to, tenant)
		return err
	})
}

// Package usage records ledger entries with cross-partition deduplication.
package usage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
)

type Entry struct {
	TenantID                                           string
	WorkspaceID, PrincipalID, RunID                    *string
	SegmentSeq                                         *int
	Source, Meter, Quantity, UnitPrice, IdempotencyKey string
	Millicredits                                       int64
	OccurredAt                                         time.Time
}

// RecordUsage claims the key and appends the ledger entry in the caller's tenant
// transaction. A duplicate is a successful no-op, including across months.
// Returning an error requires the caller to roll back the transaction.
func RecordUsage(ctx context.Context, tx pgx.Tx, e Entry) (bool, error) {
	var key string
	err := tx.QueryRow(ctx, `INSERT INTO usage_keys(tenant_id,idempotency_key,created_at)
 VALUES($1,$2,$3)
 ON CONFLICT DO NOTHING
 RETURNING idempotency_key`, e.TenantID, e.IdempotencyKey, e.OccurredAt).
		Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO usage_ledger(id,tenant_id,workspace_id,principal_id,run_id,segment_seq,source,meter,quantity,
 unit_price,millicredits,idempotency_key,occurred_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, ids.New(), e.TenantID, e.WorkspaceID, e.PrincipalID,
		e.RunID, e.SegmentSeq, e.Source, e.Meter, e.Quantity, e.UnitPrice, e.Millicredits, e.IdempotencyKey,
		e.OccurredAt)
	return err == nil, err
}

// Prune uses the owner connection and the same cutoff for ledger rows and keys.
// A nil cutoff only removes expired HTTP replay records.
func Prune(ctx context.Context, d *db.DB, cutoff *time.Time) error {
	var tenants []string
	err := d.Global(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id::text FROM tenants ORDER BY id")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				return err
			}
			tenants = append(tenants, id)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	for _, tenant := range tenants {
		if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "DELETE FROM idempotency_keys WHERE tenant_id=$1 AND expires_at<=now()", tenant); err != nil {
				return err
			}
			if cutoff == nil {
				return nil
			}
			// Lock out concurrent recorders while retaining the key/ledger invariant.
			if _, err := tx.Exec(ctx, "LOCK TABLE usage_keys,usage_ledger IN SHARE ROW EXCLUSIVE MODE"); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "DELETE FROM usage_ledger WHERE tenant_id=$1 AND occurred_at<$2", tenant, *cutoff); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `DELETE
 FROM usage_keys k
 WHERE k.tenant_id=$1
 AND k.created_at<$2
 AND NOT EXISTS(SELECT 1
   FROM usage_ledger l
   WHERE l.tenant_id=k.tenant_id
   AND l.idempotency_key=k.idempotency_key)`, tenant, *cutoff)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// Admit serializes quota admission for the tenant, including queued runs.
func Admit(ctx context.Context, tenant, workspace, principal string, budget int64) error {
	tx := db.Tx(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,7))`, tenant); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT scope_type,scope_id::text,metric,"limit"::bigint,period
 FROM quota_policies
 WHERE tenant_id=$1
 AND action='block'
 AND metric IN ('runs_concurrent','millicredits')
 AND ((scope_type='tenant' AND scope_id=$1) OR (scope_type='workspace' AND scope_id=$2) OR (scope_type='user' AND scope_id=$3))
 ORDER BY id`, tenant, workspace, principal)
	if err != nil {
		return err
	}
	type quota struct {
		scope, id, metric string
		limit             int64
		period            string
	}
	var quotas []quota
	for rows.Next() {
		var q quota
		if err = rows.Scan(&q.scope, &q.id, &q.metric, &q.limit, &q.period); err != nil {
			rows.Close()
			return err
		}
		quotas = append(quotas, q)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, q := range quotas {
		var used int64
		if q.metric == "runs_concurrent" {
			err = tx.QueryRow(ctx, `SELECT count(*)
 FROM runs r
 JOIN sessions s ON s.id=r.session_id
 WHERE r.tenant_id=$1
 AND r.ended_at IS NULL
 AND ($2='tenant' OR ($2='workspace' AND s.workspace_id=$3::uuid) OR ($2='user' AND r.created_by=$3::uuid))`, tenant, q.scope, q.id).
				Scan(&used)
			if err == nil && used >= q.limit {
				telemetry.Counter("orbit.quota.rejections").Add(ctx, 1,
					attribute.String("orbit.quota.metric", q.metric), attribute.String("orbit.quota.scope", q.scope))
				return httpx.E(429, "QUOTA_EXCEEDED", "Concurrent run quota exceeded")
			}
		} else {
			err = tx.QueryRow(ctx, `SELECT COALESCE(sum(millicredits),0)::bigint
 FROM usage_ledger
 WHERE tenant_id=$1
 AND ($2='tenant' OR ($2='workspace' AND workspace_id=$3::uuid) OR ($2='user' AND principal_id=$3::uuid))
 AND ($4='total' OR occurred_at>=date_trunc(CASE $4 WHEN 'hour' THEN 'hour' WHEN 'day' THEN 'day' ELSE 'month' END,now() AT TIME
   ZONE 'UTC') AT TIME ZONE 'UTC')`, tenant, q.scope, q.id, q.period).Scan(&used)
			if err == nil && (budget > q.limit || used > q.limit-budget) {
				telemetry.Counter("orbit.quota.rejections").Add(ctx, 1,
					attribute.String("orbit.quota.metric", q.metric), attribute.String("orbit.quota.scope", q.scope))
				return httpx.E(429, "QUOTA_EXCEEDED", "Credit quota exceeded")
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

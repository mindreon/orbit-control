// Package usage records ledger entries with cross-partition deduplication.
package usage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
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
	err := tx.QueryRow(ctx, `INSERT INTO usage_keys(tenant_id,idempotency_key,created_at) VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING idempotency_key`, e.TenantID, e.IdempotencyKey, e.OccurredAt).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO usage_ledger(id,tenant_id,workspace_id,principal_id,run_id,segment_seq,source,meter,quantity,unit_price,millicredits,idempotency_key,occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, ids.New(), e.TenantID, e.WorkspaceID, e.PrincipalID, e.RunID, e.SegmentSeq, e.Source, e.Meter, e.Quantity, e.UnitPrice, e.Millicredits, e.IdempotencyKey, e.OccurredAt)
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
			_, err := tx.Exec(ctx, "DELETE FROM usage_keys k WHERE k.tenant_id=$1 AND k.created_at<$2 AND NOT EXISTS(SELECT 1 FROM usage_ledger l WHERE l.tenant_id=k.tenant_id AND l.idempotency_key=k.idempotency_key)", tenant, *cutoff)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

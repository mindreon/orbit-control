package sessions

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

type RequeueControlEvents struct {
	Tenant, Run, EventID, Reason string
	DryRun                       bool
}

// Requeue clears only delivery quarantine; immutable payload bytes stay untouched.
func RequeueEvents(ctx context.Context, d *db.DB, q RequeueControlEvents) (int64, error) {
	if !ids.Valid(q.Tenant) || (q.Run != "" && !ids.Valid(q.Run)) {
		return 0, fmt.Errorf("valid tenant and optional run UUID required")
	}
	if q.Reason != "" && q.Reason != "invalid_envelope" && q.Reason != "identity_mismatch" && q.Reason != "subject_unresolvable" {
		return 0, fmt.Errorf("invalid quarantine reason")
	}
	var affected int64
	err := d.WithTenant(ctx, q.Tenant, func(tx pgx.Tx) error {
		var owner bool
		if err := tx.QueryRow(ctx, `SELECT current_user='orbit_owner'`).Scan(&owner); err != nil {
			return err
		}
		if !owner {
			return fmt.Errorf("event maintenance requires orbit_owner")
		}
		const filter = ` WHERE tenant_id=$1 AND ($2='' OR run_id=NULLIF($2,'')::uuid)
 AND ($3='' OR event_id=$3) AND ($4='' OR quarantine_reason=$4)
 AND published_at IS NULL AND quarantined_at IS NOT NULL`
		if q.DryRun {
			return tx.QueryRow(ctx, `SELECT count(*) FROM control_events`+filter,
				q.Tenant, q.Run, q.EventID, q.Reason).Scan(&affected)
		}
		result, err := tx.Exec(ctx, `UPDATE control_events SET quarantined_at=NULL,quarantine_reason=NULL`+filter,
			q.Tenant, q.Run, q.EventID, q.Reason)
		affected = result.RowsAffected()
		return err
	})
	return affected, err
}

// PruneEvents is owner-only maintenance. Whole partitions are dropped only when
// their actual upper bound is at or before the cutoff; the default is retained.
func PruneEvents(ctx context.Context, d *db.DB, events, rejects, outbox *time.Time) error {
	return d.Global(ctx, func(tx pgx.Tx) error {
		var owner bool
		if err := tx.QueryRow(ctx, `SELECT current_user='orbit_owner'`).Scan(&owner); err != nil {
			return err
		}
		if !owner {
			return fmt.Errorf("event maintenance requires orbit_owner")
		}
		if events != nil {
			if _, err := tx.Exec(ctx, `LOCK TABLE run_events IN ACCESS EXCLUSIVE MODE`); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `SELECT c.relname FROM pg_inherits i
 JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE i.inhparent='run_events'::regclass AND n.nspname='public'
 AND substring(pg_get_expr(c.relpartbound,c.oid) FROM $$TO \('([^']+)'\)$$)::timestamptz <= $1`, *events)
			if err != nil {
				return err
			}
			var names []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					rows.Close()
					return err
				}
				names = append(names, name)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, name := range names {
				if _, err := tx.Exec(ctx, `DROP TABLE `+pgx.Identifier{"public", name}.Sanitize()); err != nil {
					return err
				}
			}
		}
		// Explicit cross-tenant owner maintenance; no public API can reach this.
		if rejects != nil {
			if _, err := tx.Exec(ctx, `DELETE FROM run_event_rejects WHERE created_at<$1`, *rejects); err != nil {
				return err
			}
		}
		if outbox != nil {
			if _, err := tx.Exec(ctx, `DELETE FROM control_events WHERE published_at<$1 AND quarantined_at IS NULL`, *outbox); err != nil {
				return err
			}
		}
		return nil
	})
}

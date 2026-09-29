package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

func (s *Store) RegisterSOP(ctx context.Context, p taskruntime.Principal, sop taskruntime.SOP) (taskruntime.SOP, error) {
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		steps, err := json.Marshal(sop.Steps)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO sop_definitions(tenant_id, sop_id, version, steps)
			VALUES ($1,$2,$3,$4::jsonb) ON CONFLICT (tenant_id, sop_id, version) DO NOTHING`,
			p.TenantID, sop.SOPID, sop.Version, steps); err != nil {
			return err
		}
		var stored []byte
		if err := tx.QueryRow(ctx, `SELECT steps, created_at FROM sop_definitions WHERE tenant_id=$1 AND sop_id=$2 AND version=$3`,
			p.TenantID, sop.SOPID, sop.Version).Scan(&stored, &sop.CreatedAt); err != nil {
			return err
		}
		var existing []taskruntime.SOPStep
		if err := json.Unmarshal(stored, &existing); err != nil {
			return err
		}
		if !slices.Equal(existing, sop.Steps) {
			return taskruntime.ErrIdempotencyConflict
		}
		return nil
	})
	if err != nil {
		return taskruntime.SOP{}, projectionErr("register sop", err)
	}
	return sop, nil
}

func (s *Store) ListSOPs(ctx context.Context, p taskruntime.Principal) ([]taskruntime.SOP, error) {
	items := []taskruntime.SOP{}
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT sop_id, version, steps, created_at FROM sop_definitions WHERE tenant_id=$1 ORDER BY sop_id, version`, p.TenantID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item taskruntime.SOP
			var steps []byte
			if err := rows.Scan(&item.SOPID, &item.Version, &steps, &item.CreatedAt); err != nil {
				return err
			}
			if err := json.Unmarshal(steps, &item.Steps); err != nil {
				return errors.New("stored SOP steps are not a list of steps")
			}
			item.Ref = item.SOPID + "@" + strconv.Itoa(item.Version)
			items = append(items, item)
		}
		return rows.Err()
	})
	return items, projectionErr("list sops", err)
}

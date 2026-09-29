package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

type sopDefinitionRow struct {
	TenantID  string `gorm:"primaryKey"`
	SopID     string `gorm:"primaryKey"`
	Version   int    `gorm:"primaryKey"`
	Steps     []byte `gorm:"type:jsonb"`
	CreatedAt time.Time
}

func (sopDefinitionRow) TableName() string { return "sop_definitions" }

func (s *Store) RegisterSOP(ctx context.Context, p taskruntime.Principal, sop taskruntime.SOP) (taskruntime.SOP, error) {
	steps, err := json.Marshal(sop.Steps)
	if err != nil {
		return taskruntime.SOP{}, err
	}
	err = s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		// A version is immutable: inserting it again changes nothing, and the stored steps decide below.
		insert := sopDefinitionRow{TenantID: p.TenantID, SopID: sop.SOPID, Version: sop.Version, Steps: steps}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Select("TenantID", "SopID", "Version", "Steps").Create(&insert).Error; err != nil {
			return err
		}
		var stored sopDefinitionRow
		if err := tx.Where("tenant_id = ? AND sop_id = ? AND version = ?", p.TenantID, sop.SOPID, sop.Version).Take(&stored).Error; err != nil {
			return err
		}
		var existing []taskruntime.SOPStep
		if err := json.Unmarshal(stored.Steps, &existing); err != nil {
			return err
		}
		if !slices.Equal(existing, sop.Steps) {
			return taskruntime.ErrIdempotencyConflict
		}
		sop.CreatedAt = stored.CreatedAt
		return nil
	})
	if err != nil {
		return taskruntime.SOP{}, projectionErr("register sop", err)
	}
	return sop, nil
}

func (s *Store) ListSOPs(ctx context.Context, p taskruntime.Principal) ([]taskruntime.SOP, error) {
	items := []taskruntime.SOP{}
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		var rows []sopDefinitionRow
		if err := tx.Where("tenant_id = ?", p.TenantID).Order("sop_id, version").Find(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			item := taskruntime.SOP{SOPID: row.SopID, Version: row.Version, CreatedAt: row.CreatedAt}
			if err := json.Unmarshal(row.Steps, &item.Steps); err != nil {
				return errors.New("stored SOP steps are not a list of steps")
			}
			item.Ref = row.SopID + "@" + strconv.Itoa(row.Version)
			items = append(items, item)
		}
		return nil
	})
	return items, projectionErr("list sops", err)
}

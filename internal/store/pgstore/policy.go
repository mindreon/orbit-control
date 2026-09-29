package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

type tenantPolicyRow struct {
	TenantID  string `gorm:"primaryKey"`
	Spec      []byte `gorm:"type:jsonb"`
	UpdatedAt time.Time
}

func (tenantPolicyRow) TableName() string { return "tenant_policy" }

func (s *Store) GetTenantPolicy(ctx context.Context, p taskruntime.Principal) (taskruntime.Policy, error) {
	policy := taskruntime.Policy{DeniedTools: []string{}}
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		var row tenantPolicyRow
		err := tx.Where("tenant_id = ?", p.TenantID).Take(&row).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil // no row means nothing is limited
		}
		if err != nil {
			return err
		}
		return json.Unmarshal(row.Spec, &policy)
	})
	if policy.DeniedTools == nil {
		policy.DeniedTools = []string{}
	}
	return policy, projectionErr("get tenant policy", err)
}

func (s *Store) SetTenantPolicy(ctx context.Context, p taskruntime.Principal, policy taskruntime.Policy) (taskruntime.Policy, error) {
	spec, err := json.Marshal(policy)
	if err != nil {
		return taskruntime.Policy{}, err
	}
	err = s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}},
			DoUpdates: clause.Assignments(map[string]any{"spec": spec, "updated_at": gorm.Expr("now()")}),
		}).Select("TenantID", "Spec").Create(&tenantPolicyRow{TenantID: p.TenantID, Spec: spec}).Error
	})
	if err != nil {
		return taskruntime.Policy{}, projectionErr("set tenant policy", err)
	}
	return policy, nil
}

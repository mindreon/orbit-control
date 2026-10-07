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

type userSettingsRow struct {
	TenantID  string `gorm:"primaryKey"`
	UserID    string `gorm:"primaryKey"`
	Spec      []byte `gorm:"type:jsonb"`
	UpdatedAt time.Time
}

func (userSettingsRow) TableName() string { return "user_settings" }

var _ taskruntime.UserSettingsStore = (*Store)(nil)

func (s *Store) GetUserSettings(ctx context.Context, p taskruntime.Principal) (taskruntime.UserSettings, bool, error) {
	var settings taskruntime.UserSettings
	found := false
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		var row userSettingsRow
		err := tx.Where("tenant_id = ? AND user_id = ?", p.TenantID, p.UserID).Take(&row).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil // never saved: the caller falls back to the defaults
		}
		if err != nil {
			return err
		}
		found = true
		return json.Unmarshal(row.Spec, &settings)
	})
	return settings, found, projectionErr("get user settings", err)
}

func (s *Store) SetUserSettings(ctx context.Context, p taskruntime.Principal, settings taskruntime.UserSettings) error {
	spec, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	err = s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "user_id"}},
			DoUpdates: clause.Assignments(map[string]any{"spec": spec, "updated_at": gorm.Expr("now()")}),
		}).Select("TenantID", "UserID", "Spec").Create(&userSettingsRow{TenantID: p.TenantID, UserID: p.UserID, Spec: spec}).Error
	})
	return projectionErr("set user settings", err)
}

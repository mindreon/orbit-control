package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

func (s *Store) CreateTask(ctx context.Context, p taskruntime.Principal, task *taskruntime.Task) error {
	row, err := newTaskRow(p.TenantID, task)
	if err != nil {
		return err
	}
	err = s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error { return tx.Create(&row).Error })
	if err != nil {
		return storageErr("create task", err)
	}
	return nil
}

func (s *Store) GetTask(ctx context.Context, p taskruntime.Principal, id string) (*taskruntime.Task, error) {
	var row taskRow
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Where("id = ? AND tenant_id = ? AND created_by = ? AND deleted_at IS NULL", id, p.TenantID, p.UserID).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, storageErr("get task", err)
	}
	task, err := row.task()
	if err != nil {
		return nil, storageErr("decode task", err)
	}
	return task, nil
}

func (s *Store) ListTasks(ctx context.Context, p taskruntime.Principal) ([]*taskruntime.Task, error) {
	var rows []taskRow
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Where("tenant_id = ? AND created_by = ? AND deleted_at IS NULL", p.TenantID, p.UserID).Order("updated_at DESC").Find(&rows).Error
	})
	if err != nil {
		return nil, storageErr("list tasks", err)
	}
	items := make([]*taskruntime.Task, 0, len(rows))
	for _, row := range rows {
		task, err := row.task()
		if err != nil {
			return nil, storageErr("decode task", err)
		}
		items = append(items, task)
	}
	return items, nil
}

func (s *Store) UpdateTask(ctx context.Context, p taskruntime.Principal, task *taskruntime.Task) error {
	budgets, usage, pending, err := encodeTaskJSON(task)
	if err != nil {
		return err
	}
	err = s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return updateTaskColumns(tx, p.TenantID, task.ID, map[string]any{
			"status": task.Status, "plan_version": task.PlanVersion, "budgets": budgets, "usage": usage,
			"pending_approvals": pending, "updated_at": task.UpdatedAt,
		})
	})
	return projectionErr("update task", err)
}

// updateTaskColumns writes only the granted columns of one task; a key outside taskUpdatable is a programming error.
func updateTaskColumns(tx *gorm.DB, tenantID, taskID string, values map[string]any) error {
	return tx.Model(&taskRow{}).Where("id = ? AND tenant_id = ?", taskID, tenantID).
		Select(taskUpdatable).Updates(values).Error
}

// DeleteTask soft-deletes one task: deleted_at hides it from reads while the
// row stays so late workflow events still apply instead of erroring. The
// granted column lives outside taskUpdatable because a delete is not an
// update of the task's visible state.
func (s *Store) DeleteTask(ctx context.Context, p taskruntime.Principal, id string) error {
	var found bool
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		result := tx.Model(&taskRow{}).
			Where("id = ? AND tenant_id = ? AND created_by = ? AND deleted_at IS NULL", id, p.TenantID, p.UserID).
			Update("deleted_at", time.Now().UTC())
		found = result.Error == nil && result.RowsAffected > 0
		return result.Error
	})
	if err != nil {
		return projectionErr("delete task", err)
	}
	if !found {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) RegisterProfile(ctx context.Context, p taskruntime.Principal, profile taskruntime.Profile) (taskruntime.Profile, error) {
	if p.TenantID == "" || profile.ProfileID == "" || profile.Version < 1 {
		return taskruntime.Profile{}, errors.New("tenant, profile_id and positive version are required")
	}
	profile.Spec = cloneJSONMap(profile.Spec)
	spec, err := json.Marshal(profile.Spec)
	if err != nil {
		return taskruntime.Profile{}, err
	}
	var stored profileRow
	err = s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		// A version is immutable: inserting it again changes nothing, and what is stored decides below.
		insert := profileRow{TenantID: p.TenantID, ProfileID: profile.ProfileID, Version: profile.Version, Spec: spec}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).
			Select("TenantID", "ProfileID", "Version", "Spec").Create(&insert).Error; err != nil {
			return err
		}
		if err := tx.Where("tenant_id = ? AND profile_id = ? AND version = ?", p.TenantID, profile.ProfileID, profile.Version).
			Take(&stored).Error; err != nil {
			return err
		}
		var existing map[string]any
		if err := json.Unmarshal(stored.Spec, &existing); err != nil {
			return err
		}
		if !jsonEqual(existing, profile.Spec) {
			return taskruntime.ErrIdempotencyConflict
		}
		return nil
	})
	if err != nil {
		return taskruntime.Profile{}, projectionErr("register profile", err)
	}
	out, err := stored.profile()
	return out, projectionErr("decode profile", err)
}

func (s *Store) ListProfiles(ctx context.Context, p taskruntime.Principal) ([]taskruntime.Profile, error) {
	var rows []profileRow
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Where("tenant_id = ?", p.TenantID).Order("profile_id, version").Find(&rows).Error
	})
	if err != nil {
		return nil, projectionErr("list profiles", err)
	}
	items := make([]taskruntime.Profile, 0, len(rows))
	for _, row := range rows {
		item, err := row.profile()
		if err != nil {
			return nil, projectionErr("decode profile", err)
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Store) GetProfile(ctx context.Context, p taskruntime.Principal, ref string) (taskruntime.Profile, error) {
	profileID, version, ok := splitRef(ref)
	if !ok {
		return taskruntime.Profile{}, store.ErrNotFound
	}
	var row profileRow
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Where("tenant_id = ? AND profile_id = ? AND version = ?", p.TenantID, profileID, version).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return taskruntime.Profile{}, store.ErrNotFound
	}
	if err != nil {
		return taskruntime.Profile{}, storageErr("get profile", err)
	}
	return row.profile()
}

func (s *Store) ListManifests(ctx context.Context, p taskruntime.Principal, taskID string) ([]taskruntime.ArtifactManifest, error) {
	var rows []manifestRow
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Where("tenant_id = ? AND task_id = ?", p.TenantID, taskID).Order("created_at").Find(&rows).Error
	})
	if err != nil {
		return nil, projectionErr("list task manifests", err)
	}
	items := make([]taskruntime.ArtifactManifest, 0, len(rows))
	for _, row := range rows {
		item, err := row.manifest()
		if err != nil {
			return nil, projectionErr("decode manifest", err)
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Store) GetManifest(ctx context.Context, p taskruntime.Principal, id string) (taskruntime.ArtifactManifest, error) {
	var row manifestRow
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Where("tenant_id = ? AND manifest_id = ?", p.TenantID, id).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return taskruntime.ArtifactManifest{}, store.ErrNotFound
	}
	if err != nil {
		return taskruntime.ArtifactManifest{}, storageErr("get manifest", err)
	}
	return row.manifest()
}

func (s *Store) ListTaskEvents(ctx context.Context, p taskruntime.Principal, taskID string, after uint64) ([]taskruntime.Event, uint64, error) {
	var rows []taskEventRow
	var head uint64
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		if err := tx.Model(&taskEventRow{}).Where("tenant_id = ? AND task_id = ?", p.TenantID, taskID).
			Select("COALESCE(MAX(seq), 0)").Scan(&head).Error; err != nil {
			return err
		}
		return tx.Where("tenant_id = ? AND task_id = ? AND seq > ?", p.TenantID, taskID, after).Order("seq").Find(&rows).Error
	})
	if err != nil {
		return nil, 0, storageErr("list task events", err)
	}
	items := make([]taskruntime.Event, 0, len(rows))
	for _, row := range rows {
		var event taskruntime.Event
		if err := json.Unmarshal(row.Body, &event); err != nil {
			return nil, 0, storageErr("decode task event", err)
		}
		// The column is the authority: the sequence the body was written with is only what one process guessed.
		event.Seq, event.Durable = row.Seq, true
		items = append(items, event)
	}
	return items, head, nil
}

func projectionErr(op string, err error) error {
	if err == nil || errors.Is(err, store.ErrNotFound) || errors.Is(err, taskruntime.ErrIdempotencyConflict) || errors.Is(err, taskruntime.ErrCommandInProgress) || errors.Is(err, store.ErrStorage) {
		return err
	}
	return storageErr(op, err)
}

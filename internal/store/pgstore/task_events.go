package pgstore

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// AppendTaskEvent writes one durable event and applies it to the projections. The event's task is locked first, so
// the duplicate check, the sequence number and the insert are one step for every control replica.
func (s *Store) AppendTaskEvent(ctx context.Context, event taskruntime.Event) (taskruntime.Event, error) {
	if !event.Durable {
		return event, nil
	}
	if event.TenantID == "" || event.TaskID == "" || event.EventID == "" {
		return event, taskruntime.ErrMalformedEvent
	}
	projected := event
	err := s.inTenant(ctx, event.TenantID, func(tx *gorm.DB) error {
		if err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtextextended(?, 0))`, event.TaskID).Error; err != nil {
			return err
		}
		var existing taskEventRow
		switch err := tx.Select("seq").Where("event_id = ? AND tenant_id = ?", event.EventID, event.TenantID).Take(&existing).Error; {
		case err == nil:
			projected.Seq = existing.Seq
			return nil
		case !errors.Is(err, gorm.ErrRecordNotFound):
			return err
		}
		var seq uint64
		if err := tx.Model(&taskEventRow{}).Where("tenant_id = ? AND task_id = ?", event.TenantID, event.TaskID).
			Select("COALESCE(MAX(seq), 0) + 1").Scan(&seq).Error; err != nil {
			return err
		}
		event.Seq = seq
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		row := taskEventRow{TenantID: event.TenantID, TaskID: event.TaskID, Seq: seq, EventID: event.EventID, EventType: event.Type, Body: body, OccurredAt: event.Occurred}
		if err := tx.Select("TenantID", "TaskID", "Seq", "EventID", "EventType", "Body", "OccurredAt").Create(&row).Error; err != nil {
			return err
		}
		if err := tx.Exec(`SELECT pg_notify('task_events', ?)`, event.TaskID).Error; err != nil {
			return err
		}
		if err := applyEvent(tx, event); err != nil {
			return err
		}
		projected.Seq = seq
		return nil
	})
	if err != nil {
		return event, projectionErr("append task event", err)
	}
	return projected, nil
}

// applyEvent updates what an event of the given type changes in the projection tables.
func applyEvent(tx *gorm.DB, event taskruntime.Event) error {
	var payload struct {
		ToStatus    string `json:"to_status"`
		PlanVersion int    `json:"plan_version"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	switch event.Type {
	case "task.status_changed":
		if payload.ToStatus != "" {
			return updateTaskColumns(tx, event.TenantID, event.TaskID, map[string]any{"status": payload.ToStatus, "updated_at": event.Occurred})
		}
	case "task.completed":
		return updateTaskColumns(tx, event.TenantID, event.TaskID, map[string]any{"status": "COMPLETED", "updated_at": event.Occurred})
	case "plan.version_committed":
		if payload.PlanVersion > 0 {
			return updateTaskColumns(tx, event.TenantID, event.TaskID, map[string]any{
				"plan_version": gorm.Expr("GREATEST(plan_version, ?)", payload.PlanVersion), "updated_at": event.Occurred,
			})
		}
	case "message.user":
		return insertMessage(tx, event)
	case "artifact.manifest_created":
		return insertManifest(tx, event)
	case "approval.requested", "approval.decided", "budget.granted", "usage.recorded":
		return updateTaskAccumulators(tx, event)
	}
	return nil
}

func insertMessage(tx *gorm.DB, event taskruntime.Event) error {
	var message struct {
		MessageSeq      int64           `json:"message_seq"`
		ClientMessageID string          `json:"client_message_id"`
		Text            string          `json:"text"`
		Delivery        string          `json:"delivery"`
		Attachments     json.RawMessage `json:"attachments"`
	}
	if json.Unmarshal(event.Payload, &message) != nil || message.MessageSeq <= 0 {
		return nil
	}
	if len(message.Attachments) == 0 {
		message.Attachments = []byte("[]")
	}
	row := taskMessageRow{
		TenantID: event.TenantID, TaskID: event.TaskID, MessageSeq: message.MessageSeq, ClientMessageID: message.ClientMessageID,
		Text: message.Text, Attachments: message.Attachments, Delivery: message.Delivery, CreatedAt: event.Occurred,
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "task_id"}, {Name: "message_seq"}}, DoNothing: true}).
		Select("TenantID", "TaskID", "MessageSeq", "ClientMessageID", "Text", "Attachments", "Delivery", "CreatedAt").Create(&row).Error
}

func insertManifest(tx *gorm.DB, event taskruntime.Event) error {
	var manifest struct {
		ManifestID   string          `json:"manifest_id"`
		AttemptID    string          `json:"attempt_id"`
		ManifestHash string          `json:"manifest_hash"`
		Entries      json.RawMessage `json:"entries"`
	}
	if json.Unmarshal(event.Payload, &manifest) != nil || manifest.ManifestID == "" || manifest.AttemptID == "" || manifest.ManifestHash == "" {
		return nil
	}
	if len(manifest.Entries) == 0 {
		manifest.Entries = []byte("[]")
	}
	row := manifestRow{
		ManifestID: manifest.ManifestID, TenantID: event.TenantID, TaskID: event.TaskID, AttemptID: manifest.AttemptID,
		Entries: manifest.Entries, ManifestHash: manifest.ManifestHash, CreatedAt: event.Occurred,
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "manifest_id"}}, DoNothing: true}).
		Select("ManifestID", "TenantID", "TaskID", "AttemptID", "Entries", "ManifestHash", "CreatedAt").Create(&row).Error
}

// updateTaskAccumulators folds an approval, budget or usage event into the task's pending approvals, budgets or usage.
func updateTaskAccumulators(tx *gorm.DB, event taskruntime.Event) error {
	var row taskRow
	if err := tx.Select("budgets", "usage", "pending_approvals").
		Where("tenant_id = ? AND id = ?", event.TenantID, event.TaskID).Take(&row).Error; err != nil {
		return err
	}
	var budgets, usage map[string]any
	var pending []string
	for _, field := range []struct {
		raw []byte
		to  any
	}{{row.Budgets, &budgets}, {row.Usage, &usage}, {row.PendingApprovals, &pending}} {
		if err := json.Unmarshal(field.raw, field.to); err != nil {
			return err
		}
	}
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	switch event.Type {
	case "approval.requested":
		if approvalID, ok := payload["approval_id"].(string); ok && approvalID != "" && !containsString(pending, approvalID) {
			pending = append(pending, approvalID)
		}
	case "approval.decided":
		if approvalID, ok := payload["approval_id"].(string); ok {
			pending = removeString(pending, approvalID)
		}
	case "budget.granted":
		if delta, ok := payload["delta"].(map[string]any); ok {
			budgets = addJSONNumbers(budgets, delta)
		}
	case "usage.recorded":
		if delta, ok := payload["usage"].(map[string]any); ok {
			usage = addJSONNumbers(usage, delta)
		}
	}
	values := map[string]any{"updated_at": event.Occurred}
	for column, value := range map[string]any{"budgets": budgets, "usage": usage, "pending_approvals": pending} {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		values[column] = encoded
	}
	return updateTaskColumns(tx, event.TenantID, event.TaskID, values)
}

func containsString(items []string, wanted string) bool {
	for _, item := range items {
		if item == wanted {
			return true
		}
	}
	return false
}

func removeString(items []string, wanted string) []string {
	out := items[:0]
	for _, item := range items {
		if item != wanted {
			out = append(out, item)
		}
	}
	return out
}

func addJSONNumbers(base, delta map[string]any) map[string]any {
	if base == nil {
		base = map[string]any{}
	}
	out := make(map[string]any, len(base)+len(delta))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range delta {
		n, ok := value.(float64)
		if !ok {
			out[key] = value
			continue
		}
		if previous, ok := out[key].(float64); ok {
			out[key] = previous + n
		} else {
			out[key] = n
		}
	}
	return out
}

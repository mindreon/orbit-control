package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

func (s *Store) CreateTask(ctx context.Context, p taskruntime.Principal, task *taskruntime.Task) error {
	return s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		budgetsValue := task.Budgets
		if budgetsValue == nil {
			budgetsValue = map[string]any{}
		}
		usageValue := task.Usage
		if usageValue == nil {
			usageValue = map[string]any{}
		}
		pendingValue := task.PendingApprovals
		if pendingValue == nil {
			pendingValue = []string{}
		}
		budgets, err := json.Marshal(budgetsValue)
		if err != nil {
			return err
		}
		usage, err := json.Marshal(usageValue)
		if err != nil {
			return err
		}
		pending, err := json.Marshal(pendingValue)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO tasks
			(id, tenant_id, workflow_id, title, goal, mode, status, profile_ref, plan_version, budgets, usage, pending_approvals, created_by, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14,$14)`,
			task.ID, p.TenantID, task.WorkflowID, task.Title, task.Goal, task.Mode, task.Status,
			task.Profile, task.PlanVersion, budgets, usage, pending, task.CreatedBy, task.CreatedAt)
		if err != nil {
			return storageErr("create task", err)
		}
		return nil
	})
}

func (s *Store) GetTask(ctx context.Context, p taskruntime.Principal, id string) (*taskruntime.Task, error) {
	var task taskruntime.Task
	var budgets, usage, pending []byte
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT id, tenant_id, workflow_id, title, goal, mode, status, profile_ref,
			plan_version, budgets, usage, pending_approvals, created_by, created_at, updated_at
			FROM tasks WHERE id=$1 AND tenant_id=$2 AND created_by=$3`, id, p.TenantID, p.UserID).
			Scan(&task.ID, &task.TenantID, &task.WorkflowID, &task.Title, &task.Goal, &task.Mode, &task.Status,
				&task.Profile, &task.PlanVersion, &budgets, &usage, &pending, &task.CreatedBy, &task.CreatedAt, &task.UpdatedAt)
		return err
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, storageErr("get task", err)
	}
	if err := json.Unmarshal(budgets, &task.Budgets); err != nil {
		return nil, storageErr("decode task budgets", err)
	}
	if err := json.Unmarshal(usage, &task.Usage); err != nil {
		return nil, storageErr("decode task usage", err)
	}
	if err := json.Unmarshal(pending, &task.PendingApprovals); err != nil {
		return nil, storageErr("decode task pending approvals", err)
	}
	return &task, nil
}

func (s *Store) ListTasks(ctx context.Context, p taskruntime.Principal) ([]*taskruntime.Task, error) {
	items := make([]*taskruntime.Task, 0)
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id, tenant_id, workflow_id, title, goal, mode, status, profile_ref,
			plan_version, budgets, usage, pending_approvals, created_by, created_at, updated_at FROM tasks WHERE tenant_id=$1 AND created_by=$2 ORDER BY updated_at DESC`, p.TenantID, p.UserID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			item := &taskruntime.Task{}
			var budgets, usage, pending []byte
			if err := rows.Scan(&item.ID, &item.TenantID, &item.WorkflowID, &item.Title, &item.Goal, &item.Mode, &item.Status,
				&item.Profile, &item.PlanVersion, &budgets, &usage, &pending, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
				return err
			}
			if err := json.Unmarshal(budgets, &item.Budgets); err != nil {
				return err
			}
			if err := json.Unmarshal(usage, &item.Usage); err != nil {
				return err
			}
			if err := json.Unmarshal(pending, &item.PendingApprovals); err != nil {
				return err
			}
			items = append(items, item)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, storageErr("list tasks", err)
	}
	return items, nil
}

func (s *Store) UpdateTask(ctx context.Context, p taskruntime.Principal, task *taskruntime.Task) error {
	budgetsValue := task.Budgets
	if budgetsValue == nil {
		budgetsValue = map[string]any{}
	}
	usageValue := task.Usage
	if usageValue == nil {
		usageValue = map[string]any{}
	}
	pendingValue := task.PendingApprovals
	if pendingValue == nil {
		pendingValue = []string{}
	}
	budgets, err := json.Marshal(budgetsValue)
	if err != nil {
		return err
	}
	usage, err := json.Marshal(usageValue)
	if err != nil {
		return err
	}
	pending, err := json.Marshal(pendingValue)
	if err != nil {
		return err
	}
	err = s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE tasks SET status=$1, plan_version=$2, budgets=$3::jsonb, usage=$4::jsonb,
			pending_approvals=$5::jsonb, updated_at=$6 WHERE id=$7 AND tenant_id=$8`,
			task.Status, task.PlanVersion, budgets, usage, pending, task.UpdatedAt, task.ID, p.TenantID)
		return err
	})
	return projectionErr("update task", err)
}

func (s *Store) AppendTaskEvent(ctx context.Context, event taskruntime.Event) (taskruntime.Event, error) {
	if !event.Durable {
		return event, nil
	}
	if event.TenantID == "" || event.TaskID == "" || event.EventID == "" {
		return event, taskruntime.ErrMalformedEvent
	}
	var projected = event
	if err := s.inTenantTx(ctx, event.TenantID, func(tx pgx.Tx) error {
		// The task's lock comes first: the duplicate check and the insert are then one step for every replica.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, event.TaskID); err != nil {
			return err
		}
		var existingSeq uint64
		if err := tx.QueryRow(ctx, `SELECT seq FROM task_events WHERE event_id=$1 AND tenant_id=$2`, event.EventID, event.TenantID).Scan(&existingSeq); err == nil {
			projected.Seq = existingSeq
			return nil
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var seq uint64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(seq),0)+1 FROM task_events WHERE tenant_id=$1 AND task_id=$2`, event.TenantID, event.TaskID).Scan(&seq); err != nil {
			return err
		}
		event.Seq = seq
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO task_events(tenant_id, task_id, seq, event_id, event_type, body, occurred_at)
			VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)`,
			event.TenantID, event.TaskID, seq, event.EventID, event.Type, body, event.Occurred); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify('task_events', $1)`, event.TaskID); err != nil {
			return err
		}
		var payload struct {
			ToStatus    string `json:"to_status"`
			PlanVersion int    `json:"plan_version"`
		}
		_ = json.Unmarshal(event.Payload, &payload)
		switch event.Type {
		case "task.status_changed":
			if payload.ToStatus != "" {
				_, err = tx.Exec(ctx, `UPDATE tasks SET status=$1, updated_at=$2 WHERE tenant_id=$3 AND id=$4`, payload.ToStatus, event.Occurred, event.TenantID, event.TaskID)
			}
		case "task.completed":
			_, err = tx.Exec(ctx, `UPDATE tasks SET status='COMPLETED', updated_at=$1 WHERE tenant_id=$2 AND id=$3`, event.Occurred, event.TenantID, event.TaskID)
		case "plan.version_committed":
			if payload.PlanVersion > 0 {
				_, err = tx.Exec(ctx, `UPDATE tasks SET plan_version=GREATEST(plan_version,$1), updated_at=$2 WHERE tenant_id=$3 AND id=$4`, payload.PlanVersion, event.Occurred, event.TenantID, event.TaskID)
			}
		case "message.user":
			var message struct {
				MessageSeq      int64           `json:"message_seq"`
				ClientMessageID string          `json:"client_message_id"`
				Text            string          `json:"text"`
				Delivery        string          `json:"delivery"`
				Attachments     json.RawMessage `json:"attachments"`
			}
			if json.Unmarshal(event.Payload, &message) == nil && message.MessageSeq > 0 {
				if len(message.Attachments) == 0 {
					message.Attachments = []byte("[]")
				}
				_, err = tx.Exec(ctx, `INSERT INTO task_messages
					(tenant_id, task_id, message_seq, client_message_id, text, attachments, delivery, created_at)
					VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8)
					ON CONFLICT (task_id, message_seq) DO NOTHING`,
					event.TenantID, event.TaskID, message.MessageSeq, message.ClientMessageID, message.Text, message.Attachments, message.Delivery, event.Occurred)
			}
		case "artifact.manifest_created":
			var manifest struct {
				ManifestID   string          `json:"manifest_id"`
				AttemptID    string          `json:"attempt_id"`
				ManifestHash string          `json:"manifest_hash"`
				Entries      json.RawMessage `json:"entries"`
			}
			if json.Unmarshal(event.Payload, &manifest) == nil && manifest.ManifestID != "" && manifest.AttemptID != "" && manifest.ManifestHash != "" {
				if len(manifest.Entries) == 0 {
					manifest.Entries = []byte("[]")
				}
				_, err = tx.Exec(ctx, `INSERT INTO artifact_manifests
					(manifest_id, tenant_id, task_id, attempt_id, entries, manifest_hash, created_at)
					VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7)
					ON CONFLICT (manifest_id) DO NOTHING`,
					manifest.ManifestID, event.TenantID, event.TaskID, manifest.AttemptID, manifest.Entries, manifest.ManifestHash, event.Occurred)
			}
		}
		if err == nil {
			err = updateTaskAccumulators(ctx, tx, event)
		}
		if err != nil {
			return err
		}
		projected.Seq = seq
		return nil
	}); err != nil {
		return event, projectionErr("append task event", err)
	}
	return projected, nil
}

func updateTaskAccumulators(ctx context.Context, tx pgx.Tx, event taskruntime.Event) error {
	if event.Type != "approval.requested" && event.Type != "approval.decided" && event.Type != "budget.granted" && event.Type != "usage.recorded" {
		return nil
	}
	var budgetsRaw, usageRaw, pendingRaw []byte
	if err := tx.QueryRow(ctx, `SELECT budgets, usage, pending_approvals FROM tasks WHERE tenant_id=$1 AND id=$2`, event.TenantID, event.TaskID).
		Scan(&budgetsRaw, &usageRaw, &pendingRaw); err != nil {
		return err
	}
	var budgets, usage map[string]any
	var pending []string
	if err := json.Unmarshal(budgetsRaw, &budgets); err != nil {
		return err
	}
	if err := json.Unmarshal(usageRaw, &usage); err != nil {
		return err
	}
	if err := json.Unmarshal(pendingRaw, &pending); err != nil {
		return err
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
	b, err := json.Marshal(budgets)
	if err != nil {
		return err
	}
	u, err := json.Marshal(usage)
	if err != nil {
		return err
	}
	p, err := json.Marshal(pending)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE tasks SET budgets=$1::jsonb, usage=$2::jsonb, pending_approvals=$3::jsonb, updated_at=$4 WHERE tenant_id=$5 AND id=$6`, b, u, p, event.Occurred, event.TenantID, event.TaskID)
	return err
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

func (s *Store) RegisterProfile(ctx context.Context, p taskruntime.Principal, profile taskruntime.Profile) (taskruntime.Profile, error) {
	if p.TenantID == "" || profile.ProfileID == "" || profile.Version < 1 {
		return taskruntime.Profile{}, errors.New("tenant, profile_id and positive version are required")
	}
	profile.Ref = profile.ProfileID + "@" + strconv.Itoa(profile.Version)
	profile.Spec = cloneJSONMap(profile.Spec)
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		body, err := json.Marshal(profile.Spec)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO agent_profiles(tenant_id, profile_id, version, spec)
			VALUES ($1,$2,$3,$4::jsonb) ON CONFLICT (tenant_id, profile_id, version) DO NOTHING`,
			p.TenantID, profile.ProfileID, profile.Version, body)
		if err != nil {
			return err
		}
		var spec []byte
		if err := tx.QueryRow(ctx, `SELECT spec, created_at FROM agent_profiles WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`, p.TenantID, profile.ProfileID, profile.Version).Scan(&spec, &profile.CreatedAt); err != nil {
			return err
		}
		var stored map[string]any
		if err := json.Unmarshal(spec, &stored); err != nil {
			return err
		}
		if !jsonEqual(stored, profile.Spec) {
			return taskruntime.ErrIdempotencyConflict
		}
		profile.Spec = stored
		return nil
	})
	if err != nil {
		return taskruntime.Profile{}, projectionErr("register profile", err)
	}
	return profile, nil
}

func (s *Store) ListProfiles(ctx context.Context, p taskruntime.Principal) ([]taskruntime.Profile, error) {
	items := []taskruntime.Profile{}
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT profile_id, version, spec, created_at FROM agent_profiles WHERE tenant_id=$1 ORDER BY profile_id, version`, p.TenantID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item taskruntime.Profile
			var spec []byte
			if err := rows.Scan(&item.ProfileID, &item.Version, &spec, &item.CreatedAt); err != nil {
				return err
			}
			if err := json.Unmarshal(spec, &item.Spec); err != nil {
				return err
			}
			item.Ref = item.ProfileID + "@" + strconv.Itoa(item.Version)
			items = append(items, item)
		}
		return rows.Err()
	})
	return items, projectionErr("list profiles", err)
}

func (s *Store) ListTaskEvents(ctx context.Context, p taskruntime.Principal, taskID string, after uint64) ([]taskruntime.Event, uint64, error) {
	items := make([]taskruntime.Event, 0)
	var head uint64
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(seq),0) FROM task_events WHERE tenant_id=$1 AND task_id=$2`, p.TenantID, taskID).Scan(&head); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT seq, body FROM task_events WHERE tenant_id=$1 AND task_id=$2 AND seq>$3 ORDER BY seq`, p.TenantID, taskID, after)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var seq uint64
			var body []byte
			if err := rows.Scan(&seq, &body); err != nil {
				return err
			}
			var event taskruntime.Event
			if err := json.Unmarshal(body, &event); err != nil {
				return err
			}
			// The column is the authority: the sequence the body was written with is only what one process guessed.
			event.Seq, event.Durable = seq, true
			items = append(items, event)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, 0, storageErr("list task events", err)
	}
	return items, head, nil
}

func projectionErr(op string, err error) error {
	if err == nil || errors.Is(err, store.ErrNotFound) || errors.Is(err, taskruntime.ErrIdempotencyConflict) || errors.Is(err, store.ErrStorage) {
		return err
	}
	return storageErr(op, err)
}

func (s *Store) GetProfile(ctx context.Context, p taskruntime.Principal, ref string) (taskruntime.Profile, error) {
	var out taskruntime.Profile
	profileID, version, ok := splitRef(ref)
	if !ok {
		return out, store.ErrNotFound
	}
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		var spec []byte
		if err := tx.QueryRow(ctx, `SELECT profile_id, version, spec, created_at FROM agent_profiles WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`, p.TenantID, profileID, version).
			Scan(&out.ProfileID, &out.Version, &spec, &out.CreatedAt); err != nil {
			return err
		}
		return json.Unmarshal(spec, &out.Spec)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return out, store.ErrNotFound
	}
	if err != nil {
		return out, storageErr("get profile", err)
	}
	out.Ref = ref
	return out, nil
}

func (s *Store) ListManifests(ctx context.Context, p taskruntime.Principal, taskID string) ([]taskruntime.ArtifactManifest, error) {
	items := []taskruntime.ArtifactManifest{}
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT manifest_id, task_id, attempt_id, entries, manifest_hash, created_at FROM artifact_manifests WHERE tenant_id=$1 AND task_id=$2 ORDER BY created_at`, p.TenantID, taskID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item taskruntime.ArtifactManifest
			var entries []byte
			if err := rows.Scan(&item.ManifestID, &item.TaskID, &item.AttemptID, &entries, &item.Hash, &item.CreatedAt); err != nil {
				return err
			}
			if err := json.Unmarshal(entries, &item.Entries); err != nil {
				return err
			}
			items = append(items, item)
		}
		return rows.Err()
	})
	return items, projectionErr("list task manifests", err)
}

func (s *Store) GetManifest(ctx context.Context, p taskruntime.Principal, id string) (taskruntime.ArtifactManifest, error) {
	var item taskruntime.ArtifactManifest
	err := s.inTenantTx(ctx, p.TenantID, func(tx pgx.Tx) error {
		var entries []byte
		if err := tx.QueryRow(ctx, `SELECT manifest_id, task_id, attempt_id, entries, manifest_hash, created_at FROM artifact_manifests WHERE tenant_id=$1 AND manifest_id=$2`, p.TenantID, id).
			Scan(&item.ManifestID, &item.TaskID, &item.AttemptID, &entries, &item.Hash, &item.CreatedAt); err != nil {
			return err
		}
		return json.Unmarshal(entries, &item.Entries)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return item, store.ErrNotFound
	}
	if err != nil {
		return item, storageErr("get manifest", err)
	}
	return item, nil
}

func splitRef(ref string) (string, int, bool) {
	id, rawVersion, ok := strings.Cut(ref, "@")
	if !ok || id == "" {
		return "", 0, false
	}
	version, err := strconv.Atoi(rawVersion)
	if err != nil || version < 1 {
		return "", 0, false
	}
	return id, version, true
}

func cloneJSONMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	body, err := json.Marshal(in)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if json.Unmarshal(body, &out) != nil {
		return map[string]any{}
	}
	return out
}

func jsonEqual(left, right map[string]any) bool {
	a, err := json.Marshal(left)
	if err != nil {
		return false
	}
	b, err := json.Marshal(right)
	return err == nil && string(a) == string(b)
}

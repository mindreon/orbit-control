package pgstore

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// The rows gorm reads and writes. Timestamps are set by the caller, not by gorm, and a write names its columns:
// orbit_app holds column-level UPDATE grants on these tables.

type taskRow struct {
	ID               string `gorm:"primaryKey"`
	TenantID         string
	WorkflowID       string
	Title            string
	Goal             string
	Mode             string
	Status           string
	ProfileRef       string
	PlanVersion      int
	Budgets          []byte `gorm:"type:jsonb"`
	Usage            []byte `gorm:"type:jsonb"`
	PendingApprovals []byte `gorm:"type:jsonb"`
	Policy           []byte `gorm:"type:jsonb"`
	CreatedBy        string
	CreatedAt        time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt        time.Time `gorm:"autoUpdateTime:false"`
	// DeletedAt is the soft-delete stamp (00022). A plain pointer, not
	// gorm.DeletedAt, so reads only hide rows where the queries say so.
	DeletedAt *time.Time
}

func (taskRow) TableName() string { return "tasks" }

// taskUpdatable are the only columns of tasks that orbit_app may UPDATE.
var taskUpdatable = []string{"status", "plan_version", "budgets", "usage", "pending_approvals", "updated_at"}

type taskEventRow struct {
	TenantID   string
	TaskID     string `gorm:"primaryKey"`
	Seq        uint64 `gorm:"primaryKey"`
	EventID    string
	EventType  string
	Body       []byte    `gorm:"type:jsonb"`
	OccurredAt time.Time `gorm:"autoCreateTime:false"`
}

func (taskEventRow) TableName() string { return "task_events" }

type taskMessageRow struct {
	TenantID        string
	TaskID          string `gorm:"primaryKey"`
	MessageSeq      int64  `gorm:"primaryKey"`
	ClientMessageID string
	Text            string
	Attachments     []byte `gorm:"type:jsonb"`
	Delivery        string
	CreatedAt       time.Time `gorm:"autoCreateTime:false"`
}

func (taskMessageRow) TableName() string { return "task_messages" }

type manifestRow struct {
	ManifestID   string `gorm:"primaryKey"`
	TenantID     string
	TaskID       string
	AttemptID    string
	Entries      []byte `gorm:"type:jsonb"`
	ManifestHash string
	CreatedAt    time.Time `gorm:"autoCreateTime:false"`
}

func (manifestRow) TableName() string { return "artifact_manifests" }

type profileRow struct {
	TenantID  string `gorm:"primaryKey"`
	ProfileID string `gorm:"primaryKey"`
	Version   int    `gorm:"primaryKey"`
	Spec      []byte `gorm:"type:jsonb"`
	CreatedAt time.Time
}

func (profileRow) TableName() string { return "agent_profiles" }

func newTaskRow(tenantID string, t *taskruntime.Task) (taskRow, error) {
	row := taskRow{
		ID: t.ID, TenantID: tenantID, WorkflowID: t.WorkflowID, Title: t.Title, Goal: t.Goal, Mode: t.Mode,
		Status: t.Status, ProfileRef: t.Profile, PlanVersion: t.PlanVersion, CreatedBy: t.CreatedBy,
		CreatedAt: t.CreatedAt, UpdatedAt: t.CreatedAt,
	}
	var err error
	if row.Budgets, row.Usage, row.PendingApprovals, err = encodeTaskJSON(t); err != nil {
		return taskRow{}, err
	}
	row.Policy, err = json.Marshal(t.Policy)
	return row, err
}

// encodeTaskJSON is the JSON of a task's budgets, usage and pending approvals, an absent one being empty, not null.
func encodeTaskJSON(t *taskruntime.Task) (budgets, usage, pending []byte, err error) {
	if t.Budgets == nil {
		t.Budgets = map[string]any{}
	}
	if t.Usage == nil {
		t.Usage = map[string]any{}
	}
	if t.PendingApprovals == nil {
		t.PendingApprovals = []string{}
	}
	if budgets, err = json.Marshal(t.Budgets); err != nil {
		return
	}
	if usage, err = json.Marshal(t.Usage); err != nil {
		return
	}
	pending, err = json.Marshal(t.PendingApprovals)
	return
}

func (r taskRow) task() (*taskruntime.Task, error) {
	t := &taskruntime.Task{
		ID: r.ID, TenantID: r.TenantID, WorkflowID: r.WorkflowID, Title: r.Title, Goal: r.Goal, Mode: r.Mode,
		Status: r.Status, Profile: r.ProfileRef, PlanVersion: r.PlanVersion, CreatedBy: r.CreatedBy,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
	for _, field := range []struct {
		raw []byte
		to  any
	}{{r.Budgets, &t.Budgets}, {r.Usage, &t.Usage}, {r.PendingApprovals, &t.PendingApprovals}, {r.Policy, &t.Policy}} {
		if err := json.Unmarshal(field.raw, field.to); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func (r manifestRow) manifest() (taskruntime.ArtifactManifest, error) {
	item := taskruntime.ArtifactManifest{
		ManifestID: r.ManifestID, TaskID: r.TaskID, AttemptID: r.AttemptID, Hash: r.ManifestHash, CreatedAt: r.CreatedAt,
	}
	return item, json.Unmarshal(r.Entries, &item.Entries)
}

func (r profileRow) profile() (taskruntime.Profile, error) {
	item := taskruntime.Profile{
		ProfileID: r.ProfileID, Version: r.Version, Ref: r.ProfileID + "@" + strconv.Itoa(r.Version), CreatedAt: r.CreatedAt,
	}
	return item, json.Unmarshal(r.Spec, &item.Spec)
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

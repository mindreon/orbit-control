package task

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/store"
)

// seenEventCacheLimit bounds the in-memory event-dedup table so a long-lived process cannot grow it without limit.
const seenEventCacheLimit = 16384

type Principal struct {
	TenantID string
	UserID   string
}

type CreateInput struct {
	Title   string
	Goal    string
	Mode    string
	Profile string
	SOP     string
	Budgets map[string]any
	Policy  Policy
	// Config is what the task runs with (15 M8), already checked and resolved by the caller.
	Config *ConfigInput
}

type Task struct {
	ID               string         `json:"task_id"`
	TenantID         string         `json:"tenant_id"`
	WorkflowID       string         `json:"workflow_id"`
	Title            string         `json:"title"`
	Goal             string         `json:"goal"`
	Mode             string         `json:"mode"`
	Status           string         `json:"status"`
	Profile          string         `json:"profile"`
	PlanVersion      int            `json:"plan_version"`
	CreatedBy        string         `json:"created_by"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
	Budgets          map[string]any `json:"budgets"`
	Usage            map[string]any `json:"usage"`
	PendingApprovals []string       `json:"pending_approvals"`
	Policy           Policy         `json:"policy"`
}

type TaskReconcileDifference struct {
	Field    string `json:"field"`
	Expected any    `json:"expected"`
	Actual   any    `json:"actual"`
}

type TaskReconcileReport struct {
	TaskID      string                    `json:"task_id"`
	Healthy     bool                      `json:"healthy"`
	Repaired    bool                      `json:"repaired"`
	Differences []TaskReconcileDifference `json:"differences,omitempty"`
}

type Profile struct {
	ProfileID string         `json:"profile_id"`
	Version   int            `json:"version"`
	Ref       string         `json:"ref"`
	Spec      map[string]any `json:"spec"`
	CreatedAt time.Time      `json:"created_at"`
}

type ArtifactManifest struct {
	ManifestID string           `json:"manifest_id"`
	TaskID     string           `json:"task_id"`
	AttemptID  string           `json:"attempt_id,omitempty"`
	Entries    []map[string]any `json:"entries"`
	Hash       string           `json:"manifest_hash,omitempty"`
	CreatedAt  time.Time        `json:"created_at"`
}

type ArtifactSigner interface {
	PresignArtifact(context.Context, Principal, ArtifactManifest, map[string]any) (string, error)
}

type Event struct {
	// Seq numbers durable events only. An ephemeral event has Seq 0 and AfterSeq, the durable event it follows (09 §4).
	Seq           uint64          `json:"seq"`
	AfterSeq      uint64          `json:"after_seq,omitempty"`
	EventID       string          `json:"event_id"`
	TaskID        string          `json:"task_id"`
	TenantID      string          `json:"tenant_id,omitempty"`
	Type          string          `json:"type"`
	Source        string          `json:"source"`
	Payload       json.RawMessage `json:"payload"`
	Occurred      time.Time       `json:"occurred_at"`
	Durable       bool            `json:"-"`
	EntityKind    string          `json:"-"`
	EntityID      string          `json:"-"`
	EntityVersion int64           `json:"-"`
	Entity        map[string]any  `json:"entity,omitempty"`
}

type Subscriber struct {
	Events <-chan Event
	Closed <-chan struct{}
	Head   uint64
	Close  func()
}

type TaskClient interface {
	StartTask(context.Context, orch.TaskWorkflowInput) (orch.TaskView, error)
	GetTaskView(context.Context, string, string) (orch.TaskView, error)
	GetTaskPlan(context.Context, string, string) (orch.TaskPlan, error)
	UpdateTask(context.Context, string, string, string, string, any) (json.RawMessage, error)
	SignalTask(context.Context, string, string, string, any) error
}

// ErrNotFound is the repository sentinel: a task that does not exist, or that another tenant or user owns.
var ErrNotFound = store.ErrNotFound
var ErrClosed = errors.New("task is closed")

// ErrMalformedEvent marks an event that can never be projected (no tenant, task or id). Retrying it cannot help.
var ErrMalformedEvent = errors.New("event is missing its tenant, task or id")

var ErrCursor = errors.New("event cursor cannot be resumed")
var ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
var ErrReconcileUnavailable = errors.New("task reconciliation requires an orchestrator")

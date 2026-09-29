// Package task is the control plane's v3 task boundary.
package task

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/orch"
)

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

var ErrNotFound = errors.New("task not found")
var ErrClosed = errors.New("task is closed")

// ErrMalformedEvent marks an event that can never be projected (no tenant, task or id). Retrying it cannot help.
var ErrMalformedEvent = errors.New("event is missing its tenant, task or id")

var ErrCursor = errors.New("event cursor cannot be resumed")
var ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
var ErrReconcileUnavailable = errors.New("task reconciliation requires an orchestrator")

type Service struct {
	mu             sync.Mutex
	appendMu       sync.Mutex
	orch           TaskClient
	projection     ProjectionStore
	artifactSigner ArtifactSigner
	tasks          map[string]*Task
	events         map[string][]Event
	subs           map[string]map[*subscriber]struct{}
	seenMessage    map[string]map[string]uint64
	commandHash    map[string]map[string]string
	commandResult  map[string]map[string]json.RawMessage
	nextSeq        map[string]uint64
	seenEvents     map[string]map[string]struct{}
	entityVersion  map[string]map[string]int64
	profiles       map[string]map[string]Profile
	manifests      map[string]ArtifactManifest
	maxEvents      int
}

type subscriber struct {
	ch     chan Event
	closed chan struct{}
	once   sync.Once
}

func New(client TaskClient) *Service {
	return NewWithProjection(client, nil)
}

func NewWithProjection(client TaskClient, projection ProjectionStore) *Service {
	return &Service{
		orch: client, projection: projection, tasks: map[string]*Task{}, events: map[string][]Event{},
		subs: map[string]map[*subscriber]struct{}{}, seenMessage: map[string]map[string]uint64{},
		commandHash: map[string]map[string]string{}, commandResult: map[string]map[string]json.RawMessage{}, nextSeq: map[string]uint64{}, seenEvents: map[string]map[string]struct{}{}, entityVersion: map[string]map[string]int64{}, profiles: map[string]map[string]Profile{}, manifests: map[string]ArtifactManifest{}, maxEvents: 5000,
	}
}

func (s *Service) SetArtifactSigner(signer ArtifactSigner) {
	s.artifactSigner = signer
}

func (s *Service) RegisterProfile(ctx context.Context, p Principal, profile Profile) (Profile, error) {
	if p.TenantID == "" || p.UserID == "" || profile.ProfileID == "" || profile.Version < 1 {
		return Profile{}, errors.New("tenant, user, profile_id and positive version are required")
	}
	profile.Ref = fmt.Sprintf("%s@%d", profile.ProfileID, profile.Version)
	profile.Spec = cloneMap(profile.Spec)
	if s.projection != nil {
		return s.projection.RegisterProfile(ctx, p, profile)
	}
	profile.CreatedAt = time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.profiles[p.TenantID] == nil {
		s.profiles[p.TenantID] = map[string]Profile{}
	}
	if existing, ok := s.profiles[p.TenantID][profile.Ref]; ok {
		if !jsonEqual(existing.Spec, profile.Spec) {
			return Profile{}, ErrIdempotencyConflict
		}
		return existing, nil
	}
	s.profiles[p.TenantID][profile.Ref] = profile
	return profile, nil
}

func (s *Service) ListProfiles(ctx context.Context, p Principal) ([]Profile, error) {
	if s.projection != nil {
		return s.projection.ListProfiles(ctx, p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]Profile, 0, len(s.profiles[p.TenantID]))
	for _, profile := range s.profiles[p.TenantID] {
		profile.Spec = cloneMap(profile.Spec)
		items = append(items, profile)
	}
	return items, nil
}

func (s *Service) GetProfile(ctx context.Context, p Principal, ref string) (Profile, error) {
	if s.projection != nil {
		return s.projection.GetProfile(ctx, p, ref)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	profile, ok := s.profiles[p.TenantID][ref]
	if !ok {
		return Profile{}, ErrNotFound
	}
	profile.Spec = cloneMap(profile.Spec)
	return profile, nil
}

func PersonaProfileRef(id string) string { return "persona_" + id + "@1" }

func (s *Service) Manifests(ctx context.Context, p Principal, taskID string) ([]ArtifactManifest, error) {
	if s.projection != nil {
		return s.projection.ListManifests(ctx, p, taskID)
	}
	if _, err := s.Get(ctx, p, taskID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]ArtifactManifest, 0)
	for _, manifest := range s.manifests {
		if manifest.TaskID == taskID {
			manifest.Entries = append([]map[string]any(nil), manifest.Entries...)
			items = append(items, manifest)
		}
	}
	return items, nil
}

func (s *Service) GetManifest(ctx context.Context, p Principal, manifestID string) (ArtifactManifest, error) {
	if s.projection != nil {
		return s.projection.GetManifest(ctx, p, manifestID)
	}
	s.mu.Lock()
	manifest, ok := s.manifests[manifestID]
	s.mu.Unlock()
	if !ok {
		return ArtifactManifest{}, ErrNotFound
	}
	if _, err := s.Get(ctx, p, manifest.TaskID); err != nil {
		return ArtifactManifest{}, err
	}
	manifest.Entries = append([]map[string]any(nil), manifest.Entries...)
	return manifest, nil
}

func (s *Service) PresignArtifact(ctx context.Context, p Principal, manifestID, name string) (string, error) {
	if s.artifactSigner == nil {
		return "", errors.New("artifact storage is not configured")
	}
	manifest, err := s.GetManifest(ctx, p, manifestID)
	if err != nil {
		return "", err
	}
	for _, entry := range manifest.Entries {
		if entryName, _ := entry["name"].(string); entryName == name {
			return s.artifactSigner.PresignArtifact(ctx, p, manifest, entry)
		}
	}
	return "", ErrNotFound
}

func (s *Service) Create(ctx context.Context, p Principal, in CreateInput) (*Task, error) {
	if p.TenantID == "" || p.UserID == "" || in.Title == "" || in.Goal == "" {
		return nil, errors.New("tenant, user, title and goal are required")
	}
	if in.Mode == "" {
		in.Mode = "single"
	}
	if in.Mode != "single" && in.Mode != "multi" && in.Mode != "long" {
		return nil, errors.New("mode must be single, multi or long")
	}
	if in.Profile == "" {
		in.Profile = "default@1"
	}
	if in.Budgets == nil {
		in.Budgets = map[string]any{}
	}
	id, err := newID("task")
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	t := &Task{ID: id, TenantID: p.TenantID, WorkflowID: orch.TaskWorkflowID(p.TenantID, id), Title: in.Title,
		Goal: in.Goal, Mode: in.Mode, Status: "CREATED", Profile: in.Profile, PlanVersion: 1,
		CreatedBy: p.UserID, CreatedAt: now, UpdatedAt: now, Budgets: in.Budgets, Usage: map[string]any{}}
	if s.projection != nil {
		if err := s.projection.CreateTask(ctx, p, t); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	s.tasks[id] = t
	s.seenMessage[id] = map[string]uint64{}
	s.commandHash[id] = map[string]string{}
	s.commandResult[id] = map[string]json.RawMessage{}
	s.nextSeq[id] = 0
	s.seenEvents[id] = map[string]struct{}{}
	s.entityVersion[id] = map[string]int64{}
	s.mu.Unlock()
	// The durable task.created event comes from TaskWorkflow through the runtime outbox (09 §3).
	if s.orch != nil {
		_, err := s.orch.StartTask(ctx, orch.TaskWorkflowInput{
			TaskID: id, TenantID: p.TenantID, CreatedBy: map[string]any{"kind": "user", "id": p.UserID},
			Title: in.Title, Goal: in.Goal, Mode: in.Mode, Profile: in.Profile, SOP: in.SOP,
			NodeTypeRegistryVersion: 1, Budgets: in.Budgets,
		})
		if err != nil {
			// The durable task exists even if Temporal is temporarily unavailable;
			// callers receive the task and can observe the retry through events.
			_ = s.appendEvent(ctx, id, "task.start_failed", "control", map[string]any{"message": "workflow unavailable"})
		}
	}
	return cloneTask(t), nil
}

func (s *Service) Get(ctx context.Context, p Principal, id string) (*Task, error) {
	if s.projection != nil {
		task, err := s.projection.GetTask(ctx, p, id)
		if err != nil {
			return nil, err
		}
		s.cacheTask(task)
		return cloneTask(task), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tasks[id]
	if t == nil || t.TenantID != p.TenantID || t.CreatedBy != p.UserID {
		return nil, ErrNotFound
	}
	return cloneTask(t), nil
}

func (s *Service) List(ctx context.Context, p Principal) ([]*Task, error) {
	if s.projection != nil {
		items, err := s.projection.ListTasks(ctx, p)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			s.cacheTask(item)
		}
		return items, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]*Task, 0)
	for _, item := range s.tasks {
		if item.TenantID == p.TenantID && item.CreatedBy == p.UserID {
			items = append(items, cloneTask(item))
		}
	}
	return items, nil
}

func (s *Service) Update(ctx context.Context, p Principal, id, name, commandID string, payload any) (json.RawMessage, error) {
	t, err := s.Get(ctx, p, id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if isClosed(t.Status) && name == "sendMessage" {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	s.mu.Unlock()
	requestBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(requestBytes))
	s.mu.Lock()
	if previous, ok := s.commandHash[id][commandID]; ok {
		if previous != hash {
			s.mu.Unlock()
			return nil, ErrIdempotencyConflict
		}
		cached := append(json.RawMessage(nil), s.commandResult[id][commandID]...)
		s.mu.Unlock()
		return cached, nil
	}
	s.commandHash[id][commandID] = hash
	s.mu.Unlock()
	if s.orch != nil {
		raw, err := s.orch.UpdateTask(ctx, p.TenantID, id, name, commandID, payload)
		if err == nil {
			s.applyUpdateProjection(id, name, raw)
			s.cacheCommandResult(id, commandID, raw)
			return raw, nil
		}
		return nil, err
	}
	// The local projection remains usable in dev and during an orchestrator
	// restart. It is replaced by the durable event projector when connected.
	s.applyLocalUpdate(id, name, payload)
	raw := json.RawMessage(`{"accepted":true}`)
	s.cacheCommandResult(id, commandID, raw)
	return raw, nil
}

func (s *Service) cacheCommandResult(taskID, commandID string, raw json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commandResult[taskID] == nil {
		s.commandResult[taskID] = map[string]json.RawMessage{}
	}
	s.commandResult[taskID][commandID] = append(json.RawMessage(nil), raw...)
}

func (s *Service) Signal(ctx context.Context, p Principal, id, name string, payload any) error {
	if _, err := s.Get(ctx, p, id); err != nil {
		return err
	}
	if s.orch != nil {
		return s.orch.SignalTask(ctx, p.TenantID, id, name, payload)
	}
	return nil
}

func (s *Service) Plan(ctx context.Context, p Principal, id string) (orch.TaskPlan, error) {
	if s.orch == nil {
		return orch.TaskPlan{}, nil
	}
	if _, err := s.Get(ctx, p, id); err != nil {
		return orch.TaskPlan{}, err
	}
	return s.orch.GetTaskPlan(ctx, p.TenantID, id)
}

// Reconcile compares the authoritative workflow Query with the durable task
// projection. With repair=true, the workflow view is written back atomically
// through the projection boundary.
func (s *Service) Reconcile(ctx context.Context, p Principal, id string, repair bool) (TaskReconcileReport, error) {
	if s.orch == nil {
		return TaskReconcileReport{}, ErrReconcileUnavailable
	}
	projected, err := s.Get(ctx, p, id)
	if err != nil {
		return TaskReconcileReport{}, err
	}
	view, err := s.orch.GetTaskView(ctx, p.TenantID, id)
	if err != nil {
		return TaskReconcileReport{}, err
	}
	report := TaskReconcileReport{TaskID: id, Healthy: true, Differences: []TaskReconcileDifference{}}
	addDifference := func(field string, expected, actual any) {
		report.Healthy = false
		report.Differences = append(report.Differences, TaskReconcileDifference{Field: field, Expected: expected, Actual: actual})
	}
	if projected.Status != view.Status {
		addDifference("status", view.Status, projected.Status)
	}
	if projected.PlanVersion != view.PlanVersion {
		addDifference("plan_version", view.PlanVersion, projected.PlanVersion)
	}
	if !jsonEqual(projected.Budgets, view.Budgets) {
		addDifference("budgets", view.Budgets, projected.Budgets)
	}
	if !jsonEqual(projected.Usage, view.Usage) {
		addDifference("usage", view.Usage, projected.Usage)
	}
	if !approvalSetEqual(projected.PendingApprovals, view.PendingApprovals) {
		addDifference("pending_approvals", view.PendingApprovals, projected.PendingApprovals)
	}
	if !repair || report.Healthy {
		return report, nil
	}
	projected.Status = view.Status
	projected.PlanVersion = view.PlanVersion
	projected.Budgets = cloneMap(view.Budgets)
	projected.Usage = cloneMap(view.Usage)
	projected.PendingApprovals = append([]string(nil), view.PendingApprovals...)
	projected.UpdatedAt = time.Now().UTC()
	if s.projection != nil {
		if err := s.projection.UpdateTask(ctx, p, projected); err != nil {
			return TaskReconcileReport{}, err
		}
	}
	s.cacheTask(projected)
	report.Repaired = true
	return report, nil
}

func (s *Service) AppendEvent(event Event) error {
	return s.appendEventRaw(context.Background(), event)
}

func (s *Service) EventsAfter(ctx context.Context, p Principal, id string, after uint64) ([]Event, uint64, error) {
	if s.projection != nil {
		if _, err := s.Get(ctx, p, id); err != nil {
			return nil, 0, err
		}
		items, head, err := s.projection.ListTaskEvents(ctx, p, id, after)
		if err != nil {
			return nil, 0, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		return withLiveEvents(items, s.events[id], after), head, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tasks[id]
	if t == nil || t.TenantID != p.TenantID || t.CreatedBy != p.UserID {
		return nil, 0, ErrNotFound
	}
	items := s.events[id]
	if len(items) > 0 && after != 0 && after < items[0].Seq-1 {
		return nil, items[len(items)-1].Seq, ErrCursor
	}
	durable := make([]Event, 0, len(items))
	var head uint64
	for _, item := range items {
		if item.Durable {
			head = item.Seq
			if item.Seq > after {
				durable = append(durable, item)
			}
		}
	}
	return withLiveEvents(durable, items, after), head, nil
}

// withLiveEvents merges the ephemeral events kept in memory into a durable replay, each right after the durable
// event it followed. Ephemeral events are best effort: what the ring buffer no longer holds is simply not replayed.
func withLiveEvents(durable, buffered []Event, after uint64) []Event {
	merged := append([]Event(nil), durable...)
	for _, item := range buffered {
		if !item.Durable && item.AfterSeq >= after {
			merged = append(merged, item)
		}
	}
	position := func(e Event) uint64 {
		if e.Durable || e.Seq > 0 {
			return e.Seq * 2
		}
		return e.AfterSeq*2 + 1
	}
	sort.SliceStable(merged, func(i, j int) bool { return position(merged[i]) < position(merged[j]) })
	return merged
}

func (s *Service) Subscribe(ctx context.Context, p Principal, id string, after uint64) (*Subscriber, error) {
	if _, err := s.Get(ctx, p, id); err != nil {
		return nil, err
	}
	items, head, err := s.EventsAfter(ctx, p, id, after)
	if err != nil && !errors.Is(err, ErrCursor) {
		return nil, err
	}
	if err != nil {
		items = nil
	}
	sub := &subscriber{ch: make(chan Event, 256), closed: make(chan struct{})}
	s.mu.Lock()
	if s.subs[id] == nil {
		s.subs[id] = map[*subscriber]struct{}{}
	}
	s.subs[id][sub] = struct{}{}
	for _, item := range items {
		sub.ch <- item
	}
	s.mu.Unlock()
	return &Subscriber{Events: sub.ch, Closed: sub.closed, Head: head, Close: func() { s.removeSubscriber(id, sub) }}, nil
}

// CloseAllSubscribers forces active task streams to reconnect. The caller uses
// this when task ownership changes so clients resume from Last-Event-ID on the
// new owner instead of staying attached to a replica that no longer owns the
// task.
func (s *Service) CloseAllSubscribers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for taskID, subscribers := range s.subs {
		for sub := range subscribers {
			delete(subscribers, sub)
			sub.once.Do(func() {
				close(sub.closed)
				close(sub.ch)
			})
		}
		delete(s.subs, taskID)
	}
}

func (s *Service) appendEvent(ctx context.Context, id, typ, source string, payload any) error {
	raw, _ := json.Marshal(payload)
	return s.appendEventRaw(ctx, Event{EventID: "evt_" + id + "_" + fmt.Sprint(time.Now().UnixNano()), TaskID: id, Type: typ, Source: source, Payload: raw, Occurred: time.Now().UTC(), Durable: true})
}

func (s *Service) appendEventRaw(ctx context.Context, event Event) error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.Lock()
	if event.TenantID == "" {
		if task := s.tasks[event.TaskID]; task != nil {
			event.TenantID = task.TenantID
		}
	}
	if s.seenEvents[event.TaskID] == nil {
		s.seenEvents[event.TaskID] = map[string]struct{}{}
	}
	if s.entityVersion[event.TaskID] == nil {
		s.entityVersion[event.TaskID] = map[string]int64{}
	}
	if event.EventID != "" {
		if _, seen := s.seenEvents[event.TaskID][event.EventID]; seen {
			s.mu.Unlock()
			return nil
		}
		s.seenEvents[event.TaskID][event.EventID] = struct{}{}
	}
	if event.EntityKind != "" && event.EntityID != "" {
		key := event.EntityKind + ":" + event.EntityID
		current := s.entityVersion[event.TaskID][key]
		if event.EntityVersion > 0 && event.EntityVersion <= current {
			s.mu.Unlock()
			return nil
		}
		// Version 0 marks an event from a writer that does not own the entity (a worker's attempt.resumed).
		// It is kept in the stream but never moves the entity's version, so the owner's next event still applies.
		if event.EntityVersion > 0 {
			s.entityVersion[event.TaskID][key] = event.EntityVersion
		}
		event.Entity = map[string]any{"kind": event.EntityKind, "id": event.EntityID, "version": max(event.EntityVersion, current)}
	}
	if event.Durable {
		s.nextSeq[event.TaskID]++
		event.Seq = s.nextSeq[event.TaskID]
	} else {
		event.AfterSeq = s.nextSeq[event.TaskID]
	}
	if event.Occurred.IsZero() {
		event.Occurred = time.Now().UTC()
	}
	s.mu.Unlock()
	if s.projection != nil && event.Durable {
		projected, err := s.projection.AppendTaskEvent(ctx, event)
		if err != nil {
			s.mu.Lock()
			delete(s.seenEvents[event.TaskID], event.EventID)
			s.mu.Unlock()
			return err
		}
		if projected.Seq > 0 {
			event.Seq = projected.Seq
		}
	}
	s.mu.Lock()
	if event.Seq > s.nextSeq[event.TaskID] {
		s.nextSeq[event.TaskID] = event.Seq
	}
	items := append(s.events[event.TaskID], event)
	previousTask := cloneTask(s.tasks[event.TaskID])
	if task := s.tasks[event.TaskID]; task != nil {
		var payload map[string]any
		if json.Unmarshal(event.Payload, &payload) == nil {
			switch event.Type {
			case "task.status_changed":
				if status, ok := payload["to_status"].(string); ok {
					task.Status = status
				}
			case "task.completed":
				task.Status = "COMPLETED"
			case "plan.version_committed":
				if version, ok := payload["plan_version"].(float64); ok && int(version) > task.PlanVersion {
					task.PlanVersion = int(version)
				}
			case "approval.requested":
				if approvalID, ok := payload["approval_id"].(string); ok && approvalID != "" && !slicesContains(task.PendingApprovals, approvalID) {
					task.PendingApprovals = append(task.PendingApprovals, approvalID)
				}
			case "approval.decided":
				if approvalID, ok := payload["approval_id"].(string); ok {
					task.PendingApprovals = removeString(task.PendingApprovals, approvalID)
				}
			case "budget.granted":
				if delta, ok := payload["delta"].(map[string]any); ok {
					task.Budgets = addNumbers(task.Budgets, delta)
				}
			case "usage.recorded":
				if usage, ok := payload["usage"].(map[string]any); ok {
					task.Usage = addNumbers(task.Usage, usage)
				}
			}
			task.UpdatedAt = event.Occurred
		}
	}
	cutoff := time.Now().UTC().Add(-15 * time.Minute)
	ephemeral := make([]Event, 0, s.maxEvents)
	for _, item := range items {
		if !item.Durable && item.Occurred.Before(cutoff) {
			continue
		}
		ephemeral = append(ephemeral, item)
	}
	if len(ephemeral) > s.maxEvents {
		keep := map[uint64]struct{}{}
		for _, item := range ephemeral[len(ephemeral)-s.maxEvents:] {
			keep[item.Seq] = struct{}{}
		}
		filtered := make([]Event, 0, len(items))
		for _, item := range items {
			if item.Durable {
				filtered = append(filtered, item)
			} else if _, ok := keep[item.Seq]; ok {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	} else {
		items = append([]Event(nil), items...)
	}
	var updatedTask *Task
	if task := s.tasks[event.TaskID]; task != nil {
		updatedTask = cloneTask(task)
	}
	s.mu.Unlock()
	if s.projection != nil && updatedTask != nil {
		if err := s.projection.UpdateTask(ctx, Principal{TenantID: updatedTask.TenantID, UserID: updatedTask.CreatedBy}, updatedTask); err != nil {
			s.mu.Lock()
			if previousTask == nil {
				delete(s.tasks, event.TaskID)
			} else {
				s.tasks[event.TaskID] = previousTask
			}
			delete(s.seenEvents[event.TaskID], event.EventID)
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Lock()
	if event.Type == "artifact.manifest_created" {
		var manifest ArtifactManifest
		if json.Unmarshal(event.Payload, &manifest) == nil && manifest.ManifestID != "" {
			manifest.TaskID = event.TaskID
			if manifest.CreatedAt.IsZero() {
				manifest.CreatedAt = event.Occurred
			}
			s.manifests[manifest.ManifestID] = manifest
		}
	}
	s.events[event.TaskID] = items
	for sub := range s.subs[event.TaskID] {
		select {
		case sub.ch <- event:
		default:
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) removeSubscriber(id string, sub *subscriber) {
	s.mu.Lock()
	if _, ok := s.subs[id][sub]; ok {
		delete(s.subs[id], sub)
		sub.once.Do(func() { close(sub.closed) })
		close(sub.ch)
	}
	s.mu.Unlock()
}

func (s *Service) applyLocalUpdate(id, name string, payload any) {
	var message any
	s.mu.Lock()
	if t := s.tasks[id]; t != nil {
		switch name {
		case "control":
			if raw, ok := payload.(map[string]any); ok {
				action, _ := raw["action"].(string)
				if action == "cancel" {
					t.Status = "CANCELLED"
				}
				if action == "pause" {
					t.Status = "PAUSED"
				}
				if action == "resume" {
					t.Status = "RUNNING"
				}
			}
		case "sendMessage":
			message = payload
		}
		t.UpdatedAt = time.Now().UTC()
	}
	s.mu.Unlock()
	if message != nil {
		_ = s.appendEvent(context.Background(), id, "message.user", "control", message)
	}
}

func (s *Service) applyUpdateProjection(id, name string, raw json.RawMessage) {
	var body map[string]any
	if json.Unmarshal(raw, &body) != nil {
		return
	}
	s.mu.Lock()
	if t := s.tasks[id]; t != nil {
		if name == "control" {
			if status, ok := body["status"].(string); ok {
				t.Status = status
			}
		}
		if version, ok := body["plan_version"].(float64); ok {
			t.PlanVersion = int(version)
		}
		t.UpdatedAt = time.Now().UTC()
	}
	s.mu.Unlock()
}

func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	copy := *t
	copy.Budgets = cloneMap(t.Budgets)
	copy.Usage = cloneMap(t.Usage)
	copy.PendingApprovals = append([]string(nil), t.PendingApprovals...)
	return &copy
}

func (s *Service) cacheTask(task *Task) {
	if task == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID] = cloneTask(task)
	if s.seenMessage[task.ID] == nil {
		s.seenMessage[task.ID] = map[string]uint64{}
	}
	if s.commandHash[task.ID] == nil {
		s.commandHash[task.ID] = map[string]string{}
	}
	if s.commandResult[task.ID] == nil {
		s.commandResult[task.ID] = map[string]json.RawMessage{}
	}
	if s.seenEvents[task.ID] == nil {
		s.seenEvents[task.ID] = map[string]struct{}{}
	}
	if s.entityVersion[task.ID] == nil {
		s.entityVersion[task.ID] = map[string]int64{}
	}
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	raw, _ := json.Marshal(value)
	var copy map[string]any
	_ = json.Unmarshal(raw, &copy)
	return copy
}

func jsonEqual(a, b map[string]any) bool {
	ra, _ := json.Marshal(a)
	rb, _ := json.Marshal(b)
	return string(ra) == string(rb)
}

func approvalSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]struct{}, len(a))
	for _, item := range a {
		seen[item] = struct{}{}
	}
	for _, item := range b {
		if _, ok := seen[item]; !ok {
			return false
		}
	}
	return true
}

func slicesContains(items []string, wanted string) bool {
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

func addNumbers(base, delta map[string]any) map[string]any {
	out := cloneMap(base)
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

func isClosed(status string) bool {
	return status == "COMPLETED" || status == "FAILED" || status == "CANCELLED"
}

func newID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var encoded [26]byte
	value := raw
	for i := len(encoded) - 1; i >= 0; i-- {
		encoded[i] = alphabet[value[15]&31]
		for j := len(value) - 1; j > 0; j-- {
			value[j] = value[j]>>5 | value[j-1]<<3
		}
		value[0] >>= 5
	}
	return prefix + "_" + string(encoded[:]), nil
}

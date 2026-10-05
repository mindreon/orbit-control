package task

import (
	"context"
	"errors"
	"time"

	"github.com/mindreon/orbit-control/internal/orch"
)

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
	if err := in.Policy.validate(); err != nil {
		return nil, err
	}
	in.Policy = in.Policy.normalized()
	// The orchestrator reads only the policy it is started with, so the tenant's concurrency limit has to be in it: the
	// task keeps its own layer, and the workflow gets the tighter of the two.
	workflowPolicy := in.Policy
	if s.projection != nil {
		tenantPolicy, err := s.projection.GetTenantPolicy(ctx, p)
		if err != nil {
			return nil, err
		}
		workflowPolicy.MaxConcurrency = smallest(in.Policy.MaxConcurrency, tenantPolicy.MaxConcurrency)
	}
	config := ConfigInput{Mode: "default"}
	if in.Config != nil {
		var err error
		if config, err = in.Config.normalized(); err != nil {
			return nil, err
		}
		if in.Profile == "default@1" && config.Expert != "" {
			in.Profile = config.Expert
		}
	}
	id := newID("task")
	now := time.Now().UTC()
	t := &Task{ID: id, TenantID: p.TenantID, WorkflowID: orch.TaskWorkflowID(p.TenantID, id), Title: in.Title,
		Goal: in.Goal, Mode: in.Mode, Status: "CREATED", Profile: in.Profile, PlanVersion: 1,
		CreatedBy: p.UserID, CreatedAt: now, UpdatedAt: now, Budgets: in.Budgets, Usage: map[string]any{}, Policy: in.Policy}
	if s.projection != nil {
		if err := s.projection.CreateTask(ctx, p, t); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	s.tasks[id] = t
	s.seenMessage[id] = map[string]uint64{}
	s.nextSeq[id] = 0
	s.entityVersion[id] = map[string]int64{}
	s.configs[id] = localConfig{version: 1, input: config}
	s.mu.Unlock()
	// The durable task.created event comes from TaskWorkflow through the runtime outbox (09 §3).
	if s.orch != nil {
		_, err := s.orch.StartTask(ctx, orch.TaskWorkflowInput{
			TaskID: id, TenantID: p.TenantID, CreatedBy: map[string]any{"kind": "user", "id": p.UserID},
			Title: in.Title, Goal: in.Goal, Mode: in.Mode, Profile: in.Profile, SOP: in.SOP,
			NodeTypeRegistryVersion: 1, Budgets: in.Budgets, Policy: workflowPolicy, Config: config.workflowConfig(1),
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

// Delete soft-deletes one task: it disappears from Get, List and the event
// stream. The projection row stays (00022), so workflow events that still
// arrive apply to a hidden row instead of wedging the projector. Every task
// gets one best-effort workflow cancel first — including closed ones, whose
// session-open workflow would otherwise stay RUNNING in Temporal after the
// delete. A delete must not fail because the workflow is already gone or
// Temporal is unreachable.
func (s *Service) Delete(ctx context.Context, p Principal, id string) error {
	if _, err := s.Get(ctx, p, id); err != nil {
		return err
	}
	s.cancelInBackground(p, id)
	if s.projection != nil {
		if err := s.projection.DeleteTask(ctx, p, id); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tasks, id)
	delete(s.events, id)
	delete(s.seenMessage, id)
	delete(s.nextSeq, id)
	delete(s.entityVersion, id)
	delete(s.configs, id)
	for manifestID, manifest := range s.manifests {
		if manifest.TaskID == id {
			delete(s.manifests, manifestID)
		}
	}
	for sub := range s.subs[id] {
		delete(s.subs[id], sub)
		sub.once.Do(func() {
			close(sub.closed)
			close(sub.ch)
		})
	}
	delete(s.subs, id)
	return nil
}

// cancelInBackground asks the workflow to stop without blocking the delete on
// Temporal. Errors are logged and otherwise ignored: the workflow may already
// be completed, or the orchestrator down; the hidden task keeps applying
// events. The command id must be a bare UUID — the workflow's TaskControlInput
// rejects prefixed ids, and a rejected cancel would leave the workflow running.
func (s *Service) cancelInBackground(p Principal, id string) {
	if s.orch == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 5*time.Second)
		defer cancel()
		commandID := mustUUIDv7()
		if _, err := s.orch.UpdateTask(ctx, p.TenantID, id, "control", commandID, map[string]any{
			"command_id": commandID, "action": "cancel", "reason": "task deleted",
		}); err != nil && s.log != nil {
			s.log.Printf("task delete: workflow cancel for %s failed: %v", id, err)
		}
	}()
}

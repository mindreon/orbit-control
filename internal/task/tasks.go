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
			NodeTypeRegistryVersion: 1, Budgets: in.Budgets, Policy: in.Policy, Config: config.workflowConfig(1),
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

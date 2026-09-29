package task

import (
	"context"
	"encoding/json"
	"time"
)

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
	copy.Policy.DeniedTools = append([]string{}, t.Policy.DeniedTools...)
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

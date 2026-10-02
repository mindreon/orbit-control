package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

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
	if s.entityVersion[event.TaskID] == nil {
		s.entityVersion[event.TaskID] = map[string]int64{}
	}
	if event.EventID != "" {
		if _, seen := s.seenEvents.Get(eventKey(event.TaskID, event.EventID)); seen {
			s.mu.Unlock()
			return nil
		}
		s.seenEvents.Add(eventKey(event.TaskID, event.EventID), struct{}{})
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
			s.forgetEvent(event.TaskID, event.EventID)
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
	s.mu.Unlock()
	// The projection store has already applied the event to the task row, in the same transaction that stored it. Writing
	// the cached copy back as a whole row would race with a read that refreshed the cache, and put an old list of
	// pending approvals over the new one.
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

func eventKey(taskID, eventID string) string { return taskID + "\x00" + eventID }

func (s *Service) forgetEvent(taskID, eventID string) {
	if eventID == "" {
		return
	}
	s.seenEvents.Remove(eventKey(taskID, eventID))
}

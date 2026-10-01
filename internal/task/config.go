package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mindreon/orbit-control/internal/orch"
)

// What a task runs with beside its goal (15 M8): an expert, skills, connectors and a mode. The task's workflow owns it;
// this package forwards changes and reads it back. Skills and Connectors are the complete sets to use: nil keeps the
// expert's defaults, an empty list removes them. That difference is the reason the payloads below are built by hand
// from maps: the generated contract types drop an empty list as if it were unset.

var ErrConfigConflict = orch.ErrConfigConflict

var validModes = map[string]bool{"default": true, "plan": true, "ask": true}

// ConfigInput is a configuration the caller has already checked and resolved: Connectors are names-only snapshots.
type ConfigInput struct {
	Expert     string
	Skills     []string
	Connectors []map[string]any
	Mode       string
}

// ConfigView is what a caller reads. Connector launch targets stay in the workflow; the ids are what a page needs.
type ConfigView struct {
	Version      int      `json:"config_version"`
	Expert       *string  `json:"expert"`
	Skills       []string `json:"skills"`
	ConnectorIDs []string `json:"connector_ids"`
	Mode         string   `json:"mode"`
}

type ConfigUpdateResult struct {
	Version   int    `json:"config_version"`
	Effective string `json:"effective"`
}

func (c ConfigInput) normalized() (ConfigInput, error) {
	if c.Mode == "" {
		c.Mode = "default"
	}
	if !validModes[c.Mode] {
		return c, errors.New("mode must be default, plan or ask")
	}
	return c, nil
}

// workflowConfig is the TaskConfig of the workflow input, at the given version.
func (c ConfigInput) workflowConfig(version int) map[string]any {
	out := map[string]any{"config_version": version, "mode": c.Mode, "skills": nil, "connectors": nil, "expert": nil}
	if c.Expert != "" {
		out["expert"] = c.Expert
	}
	if c.Skills != nil {
		out["skills"] = c.Skills
	}
	if c.Connectors != nil {
		out["connectors"] = c.Connectors
	}
	return out
}

func (c ConfigInput) view(version int) ConfigView {
	view := ConfigView{Version: version, Skills: c.Skills, Mode: c.Mode}
	if c.Expert != "" {
		expert := c.Expert
		view.Expert = &expert
	}
	if c.Connectors != nil {
		view.ConnectorIDs = make([]string, 0, len(c.Connectors))
		for _, snapshot := range c.Connectors {
			id, _ := snapshot["id"].(string)
			view.ConnectorIDs = append(view.ConnectorIDs, id)
		}
	}
	return view
}

// TaskConfig reads the configuration of a task the caller owns.
func (s *Service) TaskConfig(ctx context.Context, p Principal, id string) (ConfigView, error) {
	if _, err := s.Get(ctx, p, id); err != nil {
		return ConfigView{}, err
	}
	if s.orch != nil {
		view, err := s.orch.GetTaskView(ctx, p.TenantID, id)
		if err != nil {
			return ConfigView{}, err
		}
		return configFromWorkflow(view.Config), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	local, ok := s.configs[id]
	if !ok {
		return ConfigInput{Mode: "default"}.view(1), nil
	}
	return local.input.view(local.version), nil
}

// UpdateTaskConfig replaces the configuration, building on the version the caller read. It applies from the task's
// next attempt. Repeating a command id answers the first result.
func (s *Service) UpdateTaskConfig(ctx context.Context, p Principal, id, commandID string, base int, in ConfigInput) (ConfigUpdateResult, error) {
	in, err := in.normalized()
	if err != nil {
		return ConfigUpdateResult{}, err
	}
	payload := map[string]any{"command_id": commandID, "base_config_version": base}
	for key, value := range in.workflowConfig(0) {
		if key != "config_version" {
			payload[key] = value
		}
	}
	raw, err := s.Update(ctx, p, id, "updateTaskConfig", commandID, payload)
	if err != nil {
		return ConfigUpdateResult{}, err
	}
	var result ConfigUpdateResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return ConfigUpdateResult{}, fmt.Errorf("decode config update result: %w", err)
	}
	return result, nil
}

type localConfig struct {
	version int
	input   ConfigInput
}

// runConfigUpdate sends the change to the workflow, or, without an orchestrator, applies it to the local copy that dev
// keeps. The workflow's validator is what refuses a stale version there; here the same rule is checked directly.
func (s *Service) runConfigUpdate(ctx context.Context, p Principal, id, commandID string, payload map[string]any) (json.RawMessage, error) {
	base, _ := payload["base_config_version"].(int)
	next := base + 1
	if s.orch != nil {
		if _, err := s.orch.UpdateTask(ctx, p.TenantID, id, "updateTaskConfig", commandID, payload); err != nil {
			return nil, err
		}
	} else if err := s.applyLocalConfig(id, base, payload); err != nil {
		return nil, err
	}
	return json.Marshal(ConfigUpdateResult{Version: next, Effective: "next_attempt"})
}

func (s *Service) applyLocalConfig(id string, base int, payload map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := 1
	if local, ok := s.configs[id]; ok {
		current = local.version
	}
	if base != current {
		return ErrConfigConflict
	}
	in := ConfigInput{Mode: fmt.Sprint(payload["mode"])}
	in.Expert, _ = payload["expert"].(string)
	in.Skills, _ = payload["skills"].([]string)
	in.Connectors, _ = payload["connectors"].([]map[string]any)
	s.configs[id] = localConfig{version: current + 1, input: in}
	return nil
}

func configFromWorkflow(raw map[string]any) ConfigView {
	view := ConfigView{Version: 1, Mode: "default"}
	if version, ok := raw["config_version"].(float64); ok {
		view.Version = int(version)
	}
	if mode, ok := raw["mode"].(string); ok && mode != "" {
		view.Mode = mode
	}
	if expert, ok := raw["expert"].(string); ok && expert != "" {
		view.Expert = &expert
	}
	if skills, ok := raw["skills"].([]any); ok {
		view.Skills = make([]string, 0, len(skills))
		for _, item := range skills {
			view.Skills = append(view.Skills, fmt.Sprint(item))
		}
	}
	if connectors, ok := raw["connectors"].([]any); ok {
		view.ConnectorIDs = make([]string, 0, len(connectors))
		for _, item := range connectors {
			snapshot, _ := item.(map[string]any)
			id, _ := snapshot["id"].(string)
			view.ConnectorIDs = append(view.ConnectorIDs, id)
		}
	}
	return view
}

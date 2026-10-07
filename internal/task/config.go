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
	Expert string
	Skills []string
	// Model is the task-level model override; empty keeps the expert's model (an expert without one runs the
	// deployment's default, resolved in the worker).
	Model      string
	Connectors []map[string]any
	Mode       string
	// Permissions is how much the agent may do without asking, resolved (no "custom" without its rules). Nil leaves
	// it out of the workflow config, which the worker reads as the default preset.
	Permissions *Permissions
	// Team is set when Expert was a team (15 M8, T8.6): Expert is then the leader's expert and Team says who the
	// members are. Nil for a single expert, which is also how a change from a team back to one clears it.
	Team *ConfigTeam
}

// ConfigTeam is TaskConfig.team: a leader role and members, each a role and the single expert who does its work. Name is
// the member expert's name for a reader; it is never sent to the workflow.
type ConfigTeam struct {
	// Ref is the team expert's own version ("team_x@3") that was selected.
	Ref     string             `json:"ref,omitempty"`
	Leader  string             `json:"leader"`
	Members []ConfigTeamMember `json:"members"`
}

type ConfigTeamMember struct {
	Role        string `json:"role"`
	Expert      string `json:"expert"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Label       string `json:"label,omitempty"`
}

// workflowTeam is the team as the workflow's TaskConfig.team has it, built from maps like the rest of the payload.
func (t *ConfigTeam) workflowTeam() any {
	if t == nil {
		return nil
	}
	members := make([]any, 0, len(t.Members))
	for _, member := range t.Members {
		entry := map[string]any{"role": member.Role, "expert": member.Expert}
		if member.Description != "" {
			entry["description"] = member.Description
		}
		if member.Label != "" {
			entry["label"] = member.Label
		}
		members = append(members, entry)
	}
	out := map[string]any{"leader": t.Leader, "members": members}
	if t.Ref != "" {
		out["ref"] = t.Ref
	}
	return out
}

// teamFromWorkflow reads a team out of a workflow config (JSON, so lists are []any) or out of an update payload.
func teamFromWorkflow(raw any) *ConfigTeam {
	fields, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	team := &ConfigTeam{}
	team.Leader, _ = fields["leader"].(string)
	team.Ref, _ = fields["ref"].(string)
	items, _ := fields["members"].([]any)
	for _, item := range items {
		entry, _ := item.(map[string]any)
		member := ConfigTeamMember{}
		member.Role, _ = entry["role"].(string)
		member.Expert, _ = entry["expert"].(string)
		member.Description, _ = entry["description"].(string)
		member.Label, _ = entry["label"].(string)
		team.Members = append(team.Members, member)
	}
	return team
}

// ConfigView is what a caller reads. Connector launch targets stay in the workflow; the ids are what a page needs.
type ConfigView struct {
	Version      int      `json:"config_version"`
	Expert       *string  `json:"expert"`
	Skills       []string `json:"skills"`
	ConnectorIDs []string `json:"connector_ids"`
	Mode         string   `json:"mode"`
	// Model is the task-level override; empty means the expert's model, then the deployment's default.
	Model string `json:"model"`
	// Team is set while the task's expert is a team. Its members carry the member experts' names.
	Team *ConfigTeam `json:"team,omitempty"`
	// TeamRef is the selected team expert's own ref ("team_x@3"); send it back as team_ref to keep the team.
	TeamRef string `json:"team_ref,omitempty"`
	// Permissions is how much the agent may do without asking; a task that has none runs the default preset. A
	// "custom" spec carries its rules.
	Permissions *Permissions `json:"permissions"`
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
	// Model goes out even when empty: that is how an update clears a model a task had chosen before.
	out := map[string]any{"config_version": version, "mode": c.Mode, "model": c.Model, "skills": nil, "connectors": nil, "expert": nil, "team": c.Team.workflowTeam()}
	if c.Expert != "" {
		out["expert"] = c.Expert
	}
	if c.Skills != nil {
		out["skills"] = c.Skills
	}
	if c.Connectors != nil {
		out["connectors"] = c.Connectors
	}
	if c.Permissions != nil {
		out["permissions"] = c.Permissions.workflowValue()
	}
	return out
}

func (c ConfigInput) view(version int) ConfigView {
	view := ConfigView{Version: version, Skills: c.Skills, Mode: c.Mode, Model: c.Model, Team: c.Team, Permissions: c.Permissions.orDefault()}
	if c.Team != nil {
		view.TeamRef = c.Team.Ref
	}
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
		return s.withMemberNames(ctx, p, configFromWorkflow(view.Config)), nil
	}
	s.mu.Lock()
	local, ok := s.configs[id]
	s.mu.Unlock()
	if !ok {
		return ConfigInput{Mode: "default"}.view(1), nil
	}
	return s.withMemberNames(ctx, p, local.input.view(local.version)), nil
}

// withMemberNames fills in the names of a team's member experts, so a page can show roles and names from one read. A
// member whose profile cannot be read keeps an empty name; the configuration itself is still right.
func (s *Service) withMemberNames(ctx context.Context, p Principal, view ConfigView) ConfigView {
	if view.Team == nil {
		return view
	}
	team := &ConfigTeam{Ref: view.Team.Ref, Leader: view.Team.Leader, Members: make([]ConfigTeamMember, len(view.Team.Members))}
	for i, member := range view.Team.Members {
		if profile, err := s.GetProfile(ctx, p, member.Expert); err == nil {
			member.Name, _ = profile.Spec["name"].(string)
		}
		team.Members[i] = member
	}
	view.Team = team
	return view
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
	in.Model, _ = payload["model"].(string)
	in.Skills, _ = payload["skills"].([]string)
	in.Connectors, _ = payload["connectors"].([]map[string]any)
	in.Team = teamFromWorkflow(payload["team"])
	in.Permissions = permissionsFromWorkflow(payload["permissions"])
	s.configs[id] = localConfig{version: current + 1, input: in}
	return nil
}

func configFromWorkflow(raw map[string]any) ConfigView {
	view := ConfigView{Version: 1, Mode: "default", Permissions: permissionsFromWorkflow(raw["permissions"]).orDefault()}
	if version, ok := raw["config_version"].(float64); ok {
		view.Version = int(version)
	}
	if mode, ok := raw["mode"].(string); ok && mode != "" {
		view.Mode = mode
	}
	view.Model, _ = raw["model"].(string)
	if expert, ok := raw["expert"].(string); ok && expert != "" {
		view.Expert = &expert
	}
	view.Team = teamFromWorkflow(raw["team"])
	if view.Team != nil {
		view.TeamRef = view.Team.Ref
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

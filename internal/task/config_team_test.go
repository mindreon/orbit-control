package task

import (
	"bytes"
	"encoding/json"
	"testing"

	v3 "github.com/mindreon/orbit-control/internal/contract/v3"
)

// A team goes to the workflow as TaskConfig.team: leader and members with role, expert and description, and no names,
// which are for readers. No team is a null, so a config that replaces a team with a single expert clears it.

func TestWorkflowConfigCarriesTheTeamWithoutNames(t *testing.T) {
	in := ConfigInput{Mode: "default", Expert: "expert_b@1", Team: &ConfigTeam{Leader: "b", Members: []ConfigTeamMember{
		{Role: "a", Expert: "expert_a@1", Name: "A", Description: "first"},
		{Role: "b", Expert: "expert_b@1", Name: "B"},
	}}}
	team, ok := in.workflowConfig(1)["team"].(map[string]any)
	if !ok || team["leader"] != "b" {
		t.Fatalf("team: %#v", in.workflowConfig(1)["team"])
	}
	members := team["members"].([]any)
	first := members[0].(map[string]any)
	if len(members) != 2 || first["role"] != "a" || first["expert"] != "expert_a@1" || first["description"] != "first" {
		t.Errorf("members: %#v", members)
	}
	for _, m := range members {
		if _, named := m.(map[string]any)["name"]; named {
			t.Errorf("the workflow's team takes no names: %#v", m)
		}
	}
	if _, has := members[1].(map[string]any)["description"]; has {
		t.Error("an empty description is left out")
	}
	back := teamFromWorkflow(team)
	if back == nil || back.Leader != "b" || len(back.Members) != 2 || back.Members[1].Expert != "expert_b@1" {
		t.Errorf("round trip: %+v", back)
	}

	none := ConfigInput{Mode: "default", Expert: "expert_a@1"}.workflowConfig(2)
	if value, present := none["team"]; !present || value != nil {
		t.Errorf("no team is an explicit null: %#v present=%v", value, present)
	}
	if teamFromWorkflow(nil) != nil {
		t.Error("a null team reads as none")
	}
}

// The workflow rejects fields its TaskConfig does not have, so what control sends must decode strictly into the contract.
func TestWorkflowTeamDecodesStrictlyIntoTheContract(t *testing.T) {
	in := ConfigInput{Mode: "default", Expert: "expert_b@1", Team: &ConfigTeam{Ref: "team_x@3", Leader: "b", Members: []ConfigTeamMember{
		{Role: "a", Expert: "expert_a@1", Name: "A", Description: "d", Label: "甲"}, {Role: "b", Expert: "expert_b@1"}}}}
	raw, _ := json.Marshal(in.workflowConfig(1))
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var cfg v3.TaskConfig
	if err := dec.Decode(&cfg); err != nil {
		t.Fatalf("TaskConfig does not accept what control sends: %v\n%s", err, raw)
	}
	if cfg.Team == nil || cfg.Team.Members[0].Label == nil || *cfg.Team.Members[0].Label != "甲" {
		t.Errorf("label: %+v", cfg.Team)
	}
}

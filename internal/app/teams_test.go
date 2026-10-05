package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/store/memstore"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// A team expert (15 M8, T8.6) is a leader and up to eight members, each a pinned version of a single expert. A task that
// selects it runs with TaskConfig.team and the leader's expert.
//
// How it can go wrong, written down before the code:
//   - a team with no member, nine members, a repeated or malformed role, or a leader who is nobody's role is stored;
//   - a team carries instructions, a model, connectors or skills the worker would never read;
//   - a member expert that is unknown, another tenant's, a team or the team itself is accepted, or the unknown and
//     foreign answers differ;
//   - an update turns a team into a single expert, or a single expert into a team;
//   - selecting a team leaves config.expert on the team instead of the leader's expert, or loses the members;
//   - selecting a single expert afterwards keeps the old team.

func teamApp() *App {
	return &App{Repo: memstore.New(), Tasks: taskruntime.New(nil)}
}

func teamPrincipal(tenant string) taskruntime.Principal {
	return taskruntime.Principal{TenantID: tenant, UserID: "u-" + tenant}
}

func singleExpert(t *testing.T, a *App, p taskruntime.Principal, name string) Expert {
	t.Helper()
	expert, err := a.CreateExpert(context.Background(), p, ExpertInput{Name: name, Instructions: "do " + name})
	if err != nil {
		t.Fatalf("create expert %s: %v", name, err)
	}
	return expert
}

func teamOf(leader string, members ...TeamMemberInput) ExpertInput {
	return ExpertInput{Name: "Team", Kind: teamKind, Leader: leader, Members: members}
}

func TestTeamInputValidation(t *testing.T) {
	member := func(role string) TeamMemberInput { return TeamMemberInput{Role: role, Expert: "expert_x@1"} }
	nine := make([]TeamMemberInput, 9)
	for i := range nine {
		nine[i] = member("r" + string(rune('a'+i)))
	}
	cases := []struct {
		name string
		in   ExpertInput
		ok   bool
	}{
		{"one member who leads", teamOf("lead", member("lead")), true},
		{"eight members", ExpertInput{Name: "t", Kind: teamKind, Leader: "ra", Members: nine[:8]}, true},
		{"no members", teamOf("lead"), false},
		{"nine members", ExpertInput{Name: "t", Kind: teamKind, Leader: "ra", Members: nine}, false},
		{"duplicate role", teamOf("lead", member("lead"), member("lead")), false},
		{"leader is not a member", teamOf("boss", member("lead")), false},
		{"empty leader", teamOf("", member("lead")), false},
		{"uppercase role", teamOf("Lead", member("Lead")), false},
		{"role over 32 characters", teamOf(strings.Repeat("a", 33), member(strings.Repeat("a", 33))), false},
		{"unversioned member expert", teamOf("lead", TeamMemberInput{Role: "lead", Expert: "expert_x"}), false},
		{"description over 300", teamOf("lead", TeamMemberInput{Role: "lead", Expert: "expert_x@1", Description: strings.Repeat("d", 301)}), false},
		{"empty name", ExpertInput{Name: " ", Kind: teamKind, Leader: "lead", Members: []TeamMemberInput{member("lead")}}, false},
		{"instructions on a team", ExpertInput{Name: "t", Kind: teamKind, Instructions: "x", Leader: "lead", Members: []TeamMemberInput{member("lead")}}, false},
		{"model on a team", ExpertInput{Name: "t", Kind: teamKind, Model: "m", Leader: "lead", Members: []TeamMemberInput{member("lead")}}, false},
		{"connectors on a team", ExpertInput{Name: "t", Kind: teamKind, ConnectorIDs: []string{"c"}, Leader: "lead", Members: []TeamMemberInput{member("lead")}}, false},
		{"skills on a team", ExpertInput{Name: "t", Kind: teamKind, SkillIDs: []string{"a/b"}, Leader: "lead", Members: []TeamMemberInput{member("lead")}}, false},
		{"members on a single expert", ExpertInput{Name: "x", Members: []TeamMemberInput{member("lead")}}, false},
		{"leader on a single expert", ExpertInput{Name: "x", Kind: expertKind, Leader: "lead"}, false},
		{"unknown kind", ExpertInput{Name: "x", Kind: "crew"}, false},
		{"single expert", ExpertInput{Name: "x"}, true},
	}
	for _, c := range cases {
		_, err := c.in.validate()
		if c.ok && err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
		if !c.ok && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: want ErrInvalid, got %v", c.name, err)
		}
	}
}

func TestTeamExpertMembersAreCheckedAgainstTheTenant(t *testing.T) {
	ctx := context.Background()
	a := teamApp()
	mine, theirs := teamPrincipal("t-a"), teamPrincipal("t-b")
	writer := singleExpert(t, a, mine, "Writer")
	reviewer := singleExpert(t, a, mine, "Reviewer")
	foreign := singleExpert(t, a, theirs, "Secret")

	team, err := a.CreateExpert(ctx, mine, teamOf("writer",
		TeamMemberInput{Role: "writer", Expert: writer.Ref, Description: "drafts"},
		TeamMemberInput{Role: "reviewer", Expert: reviewer.Ref}))
	if err != nil {
		t.Fatal(err)
	}
	if team.Kind != teamKind || team.Version != 1 || team.Leader != "writer" || len(team.Members) != 2 {
		t.Fatalf("team: %+v", team)
	}
	if got := team.Members[0]; got.Role != "writer" || got.Expert != writer.Ref || got.Name != "Writer" || got.Description != "drafts" {
		t.Errorf("member: %+v", got)
	}
	if team.Instructions != "" || len(team.ConnectorIDs) != 0 || len(team.SkillIDs) != 0 {
		t.Errorf("a team has no worker configuration: %+v", team)
	}

	_, unknown := a.CreateExpert(ctx, mine, teamOf("lead", TeamMemberInput{Role: "lead", Expert: "expert_nobody@1"}))
	_, other := a.CreateExpert(ctx, mine, teamOf("lead", TeamMemberInput{Role: "lead", Expert: foreign.Ref}))
	if !errors.Is(unknown, ErrInvalid) || !errors.Is(other, ErrInvalid) {
		t.Fatalf("unknown %v, foreign %v: both must be invalid", unknown, other)
	}
	if unknown.Error() != other.Error() || strings.Contains(other.Error(), "Secret") {
		t.Errorf("a foreign member must read as an unknown one: %q vs %q", other, unknown)
	}
	if _, err := a.CreateExpert(ctx, mine, teamOf("lead", TeamMemberInput{Role: "lead", Expert: team.Ref})); !errors.Is(err, ErrInvalid) {
		t.Errorf("a team inside a team: %v", err)
	}
	if _, err := a.CreateExpert(ctx, mine, teamOf("lead", TeamMemberInput{Role: "lead", Expert: "default@1"})); !errors.Is(err, ErrInvalid) {
		t.Errorf("a profile that is not an expert: %v", err)
	}
	if experts, _ := a.ListExperts(ctx, mine); len(experts) != 3 {
		t.Errorf("refused teams must leave nothing behind: %d experts", len(experts))
	}
	if experts, _ := a.ListExperts(ctx, theirs); len(experts) != 1 {
		t.Errorf("the other tenant sees only its own: %d experts", len(experts))
	}
}

func TestTeamExpertVersionsAndKind(t *testing.T) {
	ctx := context.Background()
	a := teamApp()
	p := teamPrincipal("t-a")
	writer := singleExpert(t, a, p, "Writer")
	checker := singleExpert(t, a, p, "Checker")
	team, err := a.CreateExpert(ctx, p, teamOf("writer", TeamMemberInput{Role: "writer", Expert: writer.Ref}))
	if err != nil {
		t.Fatal(err)
	}

	// Kind left out on an update means the expert's own kind.
	v2, err := a.UpdateExpert(ctx, p, team.ID, ExpertInput{Name: "Team 2", Leader: "writer", Members: []TeamMemberInput{
		{Role: "writer", Expert: writer.Ref}, {Role: "checker", Expert: checker.Ref}}})
	if err != nil || v2.Version != 2 || v2.Kind != teamKind || len(v2.Members) != 2 {
		t.Fatalf("update: %+v %v", v2, err)
	}
	if got, _ := a.GetExpert(ctx, p, team.ID); got.Version != 2 {
		t.Errorf("latest version: %d", got.Version)
	}
	// Version 1 is untouched.
	old, err := a.Tasks.GetProfile(ctx, p, team.ID+"@1")
	if err != nil || old.Spec["name"] != "Team" || len(old.Spec["members"].([]any)) != 1 {
		t.Errorf("version 1 changed: %+v %v", old, err)
	}

	if _, err := a.UpdateExpert(ctx, p, team.ID, ExpertInput{Name: "x", Kind: expertKind}); !errors.Is(err, ErrInvalid) {
		t.Errorf("team into a single expert: %v", err)
	}
	if _, err := a.UpdateExpert(ctx, p, writer.ID, teamOf("writer", TeamMemberInput{Role: "writer", Expert: checker.Ref})); !errors.Is(err, ErrInvalid) {
		t.Errorf("single expert into a team: %v", err)
	}
	// Even a ref to the team itself never gets as far as being a member.
	if _, err := a.UpdateExpert(ctx, p, team.ID, ExpertInput{Name: "Loop", Leader: "me", Members: []TeamMemberInput{{Role: "me", Expert: team.ID + "@1"}}}); err == nil || !strings.Contains(err.Error(), "itself") {
		t.Errorf("a team containing itself: %v", err)
	}
}

func TestSelectingATeamResolvesItIntoTheTaskConfig(t *testing.T) {
	ctx := context.Background()
	a := teamApp()
	p := teamPrincipal("t-a")
	writer := singleExpert(t, a, p, "Writer")
	checker := singleExpert(t, a, p, "Checker")
	team, err := a.CreateExpert(ctx, p, teamOf("checker",
		TeamMemberInput{Role: "writer", Expert: writer.Ref, Description: "drafts"},
		TeamMemberInput{Role: "checker", Expert: checker.Ref}))
	if err != nil {
		t.Fatal(err)
	}

	resolved, err := a.ResolveTaskConfig(ctx, p, TaskConfigRequest{Expert: team.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Expert != checker.Ref {
		t.Errorf("config.expert is the leader's expert: %q", resolved.Expert)
	}
	if resolved.Team == nil || resolved.Team.Leader != "checker" || len(resolved.Team.Members) != 2 || resolved.Team.Members[0].Expert != writer.Ref {
		t.Fatalf("team: %+v", resolved.Team)
	}

	task, err := a.Tasks.Create(ctx, p, taskruntime.CreateInput{Title: "t", Goal: "g", Config: &resolved})
	if err != nil {
		t.Fatal(err)
	}
	view, err := a.Tasks.TaskConfig(ctx, p, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Expert == nil || *view.Expert != checker.Ref || view.Team == nil || view.Team.Members[0].Name != "Writer" || view.Team.Members[1].Name != "Checker" {
		t.Fatalf("view: %+v %+v", view, view.Team)
	}

	// The same team through a config update, then a single expert, which clears it.
	if _, err := a.Tasks.UpdateTaskConfig(ctx, p, task.ID, "cmd-1", view.Version, resolved); err != nil {
		t.Fatal(err)
	}
	single, err := a.ResolveTaskConfig(ctx, p, TaskConfigRequest{Expert: writer.Ref})
	if err != nil || single.Team != nil || single.Expert != writer.Ref {
		t.Fatalf("single: %+v %v", single, err)
	}
	if _, err := a.Tasks.UpdateTaskConfig(ctx, p, task.ID, "cmd-2", view.Version+1, single); err != nil {
		t.Fatal(err)
	}
	after, _ := a.Tasks.TaskConfig(ctx, p, task.ID)
	if after.Team != nil || after.Expert == nil || *after.Expert != writer.Ref {
		t.Errorf("a single expert must clear the team: %+v", after)
	}
}

func TestATeamSelectedByAnotherTenantOrMalformedIsRefused(t *testing.T) {
	ctx := context.Background()
	a := teamApp()
	mine, theirs := teamPrincipal("t-a"), teamPrincipal("t-b")
	member := singleExpert(t, a, mine, "Writer")
	team, err := a.CreateExpert(ctx, mine, teamOf("w", TeamMemberInput{Role: "w", Expert: member.Ref}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.ResolveTaskConfig(ctx, theirs, TaskConfigRequest{Expert: team.Ref}); !errors.Is(err, ErrInvalid) {
		t.Errorf("another tenant's team: %v", err)
	}
	// /v1/profiles can register any spec; a team whose members are another tenant's or whose leader is nobody is refused.
	bad := []map[string]any{
		{"kind": "team", "name": "x", "leader": "ghost", "members": []any{map[string]any{"role": "w", "expert": member.Ref}}},
		{"kind": "team", "name": "x", "leader": "w", "members": []any{}},
		{"kind": "team", "name": "x", "leader": "w", "members": []any{map[string]any{"role": "w", "expert": "expert_gone@1"}}},
	}
	for i, spec := range bad {
		profile, err := a.Tasks.RegisterProfile(ctx, mine, taskruntime.Profile{ProfileID: "hand_" + string(rune('a'+i)), Version: 1, Spec: spec})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.ResolveTaskConfig(ctx, mine, TaskConfigRequest{Expert: profile.Ref}); !errors.Is(err, ErrInvalid) {
			t.Errorf("malformed team %d: %v", i, err)
		}
	}
	if err := a.CheckProfile(ctx, mine, team.Ref); !errors.Is(err, ErrInvalid) {
		t.Errorf("a team cannot be a node's profile: %v", err)
	}
	if err := a.CheckProfile(ctx, mine, member.Ref); err != nil {
		t.Errorf("a single expert can: %v", err)
	}
}

func codeOf(err error) (code, field string) {
	var fe *FieldError
	if errors.As(err, &fe) {
		return fe.Code, fe.Field
	}
	return "", ""
}

func TestTeamRefusalsNameTheirCodeAndField(t *testing.T) {
	ctx := context.Background()
	a := teamApp()
	mine, theirs := teamPrincipal("t-a"), teamPrincipal("t-b")
	writer := singleExpert(t, a, mine, "Writer")
	foreign := singleExpert(t, a, theirs, "Secret")
	team, err := a.CreateExpert(ctx, mine, teamOf("w", TeamMemberInput{Role: "w", Expert: writer.Ref}))
	if err != nil {
		t.Fatal(err)
	}
	ok := TeamMemberInput{Role: "w", Expert: writer.Ref}
	nine := make([]TeamMemberInput, 9)
	for i := range nine {
		nine[i] = TeamMemberInput{Role: "r" + string(rune('a'+i)), Expert: writer.Ref}
	}
	cases := []struct {
		name        string
		in          ExpertInput
		code, field string
	}{
		{"no members", teamOf("w"), "TEAM_NO_MEMBERS", "members"},
		{"too many", ExpertInput{Name: "t", Kind: teamKind, Leader: "ra", Members: nine}, "TEAM_TOO_MANY_MEMBERS", "members"},
		{"dup role", teamOf("w", ok, TeamMemberInput{Role: "w", Expert: writer.Ref}), "TEAM_DUPLICATE_ROLE", "members[1].role"},
		{"leader", teamOf("boss", ok), "TEAM_LEADER_NOT_MEMBER", "leader"},
		{"bad role", teamOf("w", ok, TeamMemberInput{Role: "Bad", Expert: writer.Ref}), "ROLE_INVALID", "members[1].role"},
		{"bad ref", teamOf("w", TeamMemberInput{Role: "w", Expert: "writer"}), "TEAM_MEMBER_REF_INVALID", "members[0].expert"},
		{"unknown", teamOf("w", ok, TeamMemberInput{Role: "x", Expert: "expert_none@1"}), "TEAM_MEMBER_NOT_FOUND", "members[1].expert"},
		{"foreign", teamOf("w", ok, TeamMemberInput{Role: "x", Expert: foreign.Ref}), "TEAM_MEMBER_NOT_FOUND", "members[1].expert"},
		{"nested", teamOf("w", ok, TeamMemberInput{Role: "x", Expert: team.Ref}), "TEAM_MEMBER_IS_TEAM", "members[1].expert"},
		{"label", teamOf("w", TeamMemberInput{Role: "w", Expert: writer.Ref, Label: strings.Repeat("字", 41)}), "LABEL_TOO_LONG", "members[0].label"},
		{"instructions", ExpertInput{Name: "t", Kind: teamKind, Instructions: "x", Leader: "w", Members: []TeamMemberInput{ok}}, "FIELD_NOT_ALLOWED", "instructions"},
		{"kind", ExpertInput{Name: "t", Kind: "crew"}, "KIND_INVALID", "kind"},
		{"name", ExpertInput{Name: " "}, "NAME_INVALID", "name"},
		{"connector", ExpertInput{Name: "x", ConnectorIDs: []string{"mcp_none"}}, "CONNECTOR_NOT_FOUND", "connector_ids[0]"},
	}
	for _, c := range cases {
		_, err := a.CreateExpert(ctx, mine, c.in)
		if code, field := codeOf(err); code != c.code || field != c.field {
			t.Errorf("%s: %q %q (%v), want %q %q", c.name, code, field, err, c.code, c.field)
		}
	}
	_, self := a.UpdateExpert(ctx, mine, team.ID, ExpertInput{Name: "L", Leader: "me", Members: []TeamMemberInput{{Role: "me", Expert: team.ID + "@1"}}})
	if code, field := codeOf(self); code != "TEAM_SELF_REFERENCE" || field != "members[0].expert" {
		t.Errorf("self: %q %q", code, field)
	}
	_, kind := a.UpdateExpert(ctx, mine, team.ID, ExpertInput{Name: "x", Kind: expertKind})
	if code, _ := codeOf(kind); code != "KIND_IMMUTABLE" {
		t.Errorf("kind: %v", kind)
	}
}

func TestTeamRefAndLabelsRideInTheTaskConfig(t *testing.T) {
	ctx := context.Background()
	a := teamApp()
	p := teamPrincipal("t-a")
	writer := singleExpert(t, a, p, "Writer")
	checker := singleExpert(t, a, p, "Checker")
	team, err := a.CreateExpert(ctx, p, teamOf("checker",
		TeamMemberInput{Role: "writer", Expert: writer.Ref, Label: "撰稿人"}, TeamMemberInput{Role: "checker", Expert: checker.Ref}))
	if err != nil {
		t.Fatal(err)
	}
	if team.Members[0].Label != "撰稿人" {
		t.Errorf("label read back: %+v", team.Members[0])
	}
	// team_ref alone, with the leader's expert, or with the team itself selects the team; anything else is refused.
	for _, expert := range []string{"", checker.Ref, team.Ref} {
		got, err := a.ResolveTaskConfig(ctx, p, TaskConfigRequest{Expert: expert, TeamRef: team.Ref})
		if err != nil || got.Team == nil || got.Team.Ref != team.Ref || got.Expert != checker.Ref || got.Team.Members[0].Label != "撰稿人" {
			t.Errorf("expert %q: %+v %v", expert, got, err)
		}
	}
	if _, err := a.ResolveTaskConfig(ctx, p, TaskConfigRequest{Expert: writer.Ref, TeamRef: team.Ref}); err == nil {
		t.Error("another expert beside team_ref must be refused")
	} else if code, _ := codeOf(err); code != "EXPERT_TEAM_MISMATCH" {
		t.Error(err)
	}
	_, err = a.ResolveTaskConfig(ctx, p, TaskConfigRequest{TeamRef: writer.Ref})
	if code, field := codeOf(err); code != "TEAM_REF_NOT_TEAM" || field != "team_ref" {
		t.Errorf("team_ref of a single expert: %v", err)
	}
	_, err = a.ResolveTaskConfig(ctx, p, TaskConfigRequest{TeamRef: "expert_none@1"})
	if code, field := codeOf(err); code != "EXPERT_NOT_FOUND" || field != "team_ref" {
		t.Errorf("unknown team_ref: %v", err)
	}

	resolved, _ := a.ResolveTaskConfig(ctx, p, TaskConfigRequest{TeamRef: team.Ref})
	task, err := a.Tasks.Create(ctx, p, taskruntime.CreateInput{Title: "t", Goal: "g", Config: &resolved})
	if err != nil {
		t.Fatal(err)
	}
	view, _ := a.Tasks.TaskConfig(ctx, p, task.ID)
	if view.TeamRef != team.Ref || view.Team == nil || view.Team.Ref != team.Ref {
		t.Errorf("view: %+v", view)
	}
}

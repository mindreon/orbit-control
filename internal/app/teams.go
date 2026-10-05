package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// A team expert (15 M8, T8.6) is a profile of kind "team": a leader role and up to eight members, each a role and a
// pinned version of a single expert. It has no worker configuration of its own. A task that selects it runs with
// TaskConfig.team and the leader's expert (ResolveTaskConfig), so the workflow never reads the team profile.

func (a *App) writeTeam(ctx context.Context, p taskruntime.Principal, expertID string, version int, in ExpertInput) (Expert, error) {
	members := make([]any, 0, len(in.Members))
	for i, member := range in.Members {
		name, err := a.teamMemberName(ctx, p, expertID, member.Expert, fmt.Sprintf("members[%d].expert", i))
		if err != nil {
			return Expert{}, err
		}
		entry := map[string]any{"role": member.Role, "expert": member.Expert, "name": name}
		if member.Description != "" {
			entry["description"] = member.Description
		}
		if member.Label != "" {
			entry["label"] = member.Label
		}
		members = append(members, entry)
	}
	spec := map[string]any{"kind": teamKind, "name": in.Name, "leader": in.Leader, "members": members}
	profile, err := a.Tasks.RegisterProfile(ctx, p, taskruntime.Profile{ProfileID: expertID, Version: version, Spec: spec})
	if err != nil {
		return Expert{}, err
	}
	return expertFrom(profile), nil
}

// teamMemberName accepts ref as a team member only when it is a single expert of the caller's tenant, and returns the
// name that version carries. An unknown ref and another tenant's alike fail as "unknown", so a caller learns nothing
// about experts elsewhere. A team is not a member: teams do not nest (07 §1), and a team cannot contain itself.
func (a *App) teamMemberName(ctx context.Context, p taskruntime.Principal, teamID, ref, field string) (string, error) {
	if memberID, _, _ := strings.Cut(ref, "@"); memberID == teamID {
		return "", invalidField("TEAM_SELF_REFERENCE", field, "a team cannot contain itself")
	}
	profile, err := a.Tasks.GetProfile(ctx, p, ref)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", invalidField("TEAM_MEMBER_NOT_FOUND", field, "not a single expert of this tenant")
		}
		return "", err
	}
	if profile.Spec["kind"] != expertKind {
		return "", invalidField("TEAM_MEMBER_IS_TEAM", field, "a team member must be a single expert, not a team")
	}
	name, _ := profile.Spec["name"].(string)
	return name, nil
}

// teamFromSpec reads a team profile's spec back. A spec that is not a well-formed team (profiles can also be registered
// through /v1/profiles) is an error rather than a team the workflow would have to guess about.
func teamFromSpec(spec map[string]any) (leader string, members []TeamMember, err error) {
	leader, _ = spec["leader"].(string)
	raw, _ := spec["members"].([]any)
	if len(raw) < minTeamMembers || len(raw) > maxTeamMembers {
		return "", nil, invalidField("TEAM_MALFORMED", "expert", "the selected team is malformed")
	}
	roles := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		entry, _ := item.(map[string]any)
		member := TeamMember{}
		member.Role, _ = entry["role"].(string)
		member.Expert, _ = entry["expert"].(string)
		member.Name, _ = entry["name"].(string)
		member.Description, _ = entry["description"].(string)
		member.Label, _ = entry["label"].(string)
		if !teamRolePattern.MatchString(member.Role) || !versionedRef.MatchString(member.Expert) {
			return "", nil, invalidField("TEAM_MALFORMED", "expert", "the selected team is malformed")
		}
		if _, dup := roles[member.Role]; dup {
			return "", nil, invalidField("TEAM_MALFORMED", "expert", "the selected team is malformed")
		}
		roles[member.Role] = struct{}{}
		members = append(members, member)
	}
	if _, ok := roles[leader]; !ok {
		return "", nil, invalidField("TEAM_MALFORMED", "expert", "the selected team is malformed")
	}
	return leader, members, nil
}

// resolveTeam turns a team profile into what TaskConfig.team carries. Each member is checked again against the caller's
// tenant: the member versions are immutable, but the reference in a hand-registered profile is not to be trusted.
func (a *App) resolveTeam(ctx context.Context, p taskruntime.Principal, profile taskruntime.Profile) (*taskruntime.ConfigTeam, error) {
	leader, members, err := teamFromSpec(profile.Spec)
	if err != nil {
		return nil, err
	}
	team := &taskruntime.ConfigTeam{Leader: leader, Members: make([]taskruntime.ConfigTeamMember, 0, len(members))}
	for _, member := range members {
		if _, err := a.teamMemberName(ctx, p, profile.ProfileID, member.Expert, "expert"); err != nil {
			return nil, err
		}
		team.Members = append(team.Members, taskruntime.ConfigTeamMember{Role: member.Role, Expert: member.Expert, Name: member.Name, Description: member.Description, Label: member.Label})
	}
	return team, nil
}

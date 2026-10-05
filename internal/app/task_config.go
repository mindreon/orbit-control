package app

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// A task's configuration as a caller states it (15 M8). Skills and ConnectorIDs are the complete sets to use: nil keeps
// the expert's defaults, an empty list removes them.
type TaskConfigRequest struct {
	Expert string
	// TeamRef is the team expert to keep or select. Expert may then be left out or be the leader's expert, which is what
	// a task's configuration reads back as; any other expert next to it is refused.
	TeamRef      string
	Skills       []string
	ConnectorIDs []string
	Mode         string
}

const maxTaskConfigRefs = 20

var versionedRef = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*@[1-9][0-9]*$`)

// ResolveTaskConfig checks every reference against the caller's tenant and resolves the connectors to names-only
// snapshots. Anything that is not the tenant's own, or does not exist, fails alike, so a caller learns nothing about
// what exists elsewhere.
func (a *App) ResolveTaskConfig(ctx context.Context, p taskruntime.Principal, in TaskConfigRequest) (taskruntime.ConfigInput, error) {
	out := taskruntime.ConfigInput{Expert: in.Expert, Mode: in.Mode}
	if out.Mode == "" {
		out.Mode = "default"
	}
	if out.Mode != "default" && out.Mode != "plan" && out.Mode != "ask" {
		return out, invalidField("MODE_INVALID", "mode", "mode must be default, plan or ask")
	}
	selected := in.Expert
	if in.TeamRef != "" {
		selected = in.TeamRef
	}
	profile, err := a.checkExpert(ctx, p, selected)
	if err != nil {
		var fe *FieldError
		if in.TeamRef != "" && errors.As(err, &fe) {
			fe.Field = "team_ref"
		}
		return out, err
	}
	if in.TeamRef != "" && profile.Spec["kind"] != teamKind {
		return out, invalidField("TEAM_REF_NOT_TEAM", "team_ref", "team_ref is not a team expert")
	}
	if profile.Spec["kind"] == teamKind {
		// A team runs as its leader's expert; the members ride along in config.team (15 M8, T8.6). Choosing a single
		// expert leaves out.Team nil, which replaces any team the task had.
		team, err := a.resolveTeam(ctx, p, profile)
		if err != nil {
			return out, err
		}
		team.Ref = profile.Ref
		out.Team = team
		leader := ""
		for _, member := range team.Members {
			if member.Role == team.Leader {
				leader = member.Expert
			}
		}
		if in.TeamRef != "" && in.Expert != "" && in.Expert != in.TeamRef && in.Expert != leader {
			return out, invalidField("EXPERT_TEAM_MISMATCH", "expert", "expert is neither the team nor its leader's expert")
		}
		out.Expert = leader
	}
	if err := checkRefList("skills", "SKILLS_INVALID", in.Skills); err != nil {
		return out, err
	}
	if err := checkRefList("connector_ids", "CONNECTORS_INVALID", in.ConnectorIDs); err != nil {
		return out, err
	}
	if in.ConnectorIDs != nil {
		snapshots, err := a.connectorSnapshots(ctx, p.TenantID, in.ConnectorIDs)
		if err != nil {
			return out, err
		}
		out.Connectors = snapshots
	}
	if in.Skills != nil {
		out.Skills = append([]string{}, in.Skills...)
		for i, skillID := range in.Skills {
			if err := a.checkSkill(ctx, p.TenantID, skillID, fmt.Sprintf("skills[%d]", i)); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

// checkExpert returns the profile a task names as its expert; the zero Profile for none or the built-in default.
func (a *App) checkExpert(ctx context.Context, p taskruntime.Principal, ref string) (taskruntime.Profile, error) {
	if ref == "" || ref == "default@1" {
		return taskruntime.Profile{}, nil
	}
	if !versionedRef.MatchString(ref) {
		return taskruntime.Profile{}, invalidField("EXPERT_REF_INVALID", "expert", "expert is not a versioned reference (id@version)")
	}
	profile, err := a.Tasks.GetProfile(ctx, p, ref)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return taskruntime.Profile{}, invalidField("EXPERT_NOT_FOUND", "expert", "unknown expert")
		}
		return taskruntime.Profile{}, err
	}
	return profile, nil
}

func checkRefList(field, code string, ids []string) error {
	if len(ids) > maxTaskConfigRefs {
		return invalidField(code, field, fmt.Sprintf("more than %d entries", maxTaskConfigRefs))
	}
	seen := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		if _, dup := seen[id]; dup {
			return invalidField(code, fmt.Sprintf("%s[%d]", field, i), "one entry is listed twice")
		}
		seen[id] = struct{}{}
	}
	return nil
}

// checkSkill accepts a skill only when the worker can actually use it: the catalog has it, its text is filled, it has a
// SKILL.md and no file name could leave the skill's directory. The others are skipped, not half-installed.
func (a *App) checkSkill(ctx context.Context, tenantID, skillID, field string) error {
	if _, err := a.Repo.GetSkill(ctx, tenantID, skillID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return invalidField("SKILL_NOT_FOUND", field, "unknown skill")
		}
		return err
	}
	files, known, err := a.skillFiles(ctx, tenantID, skillID)
	if err != nil {
		return err
	}
	if !known || !skillUsable(files) {
		return invalidField("SKILL_UNUSABLE", field, "skill cannot be used")
	}
	return nil
}

func skillUsable(files []store.SkillFile) bool {
	hasSkillMd := false
	for _, file := range files {
		name := file.Path
		switch {
		case name == "", strings.HasPrefix(name, "/"), strings.ContainsAny(name, "\\\x00"):
			return false
		case path.Clean(name) != name:
			return false
		}
		for _, segment := range strings.Split(name, "/") {
			if segment == ".." {
				return false
			}
		}
		// AgentScope loads a skill from a file named exactly SKILL.md at its root; the worker needs the same.
		if name == "SKILL.md" {
			hasSkillMd = true
		}
	}
	return hasSkillMd
}

// SkillBundle is a skill as the worker stages it: its identity and its text files. Only a usable skill has one.
type SkillBundle struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Files       []SkillTextFile `json:"files"`
}

// maxSkillBundleBytes is what the worker is willing to stage for one skill; a bigger one is refused here already.
const maxSkillBundleBytes = 8 << 20

// SkillBundleForWorker serves the worker the files of a skill a task was given. A skill that is unknown, whose text was
// never filled, that has no SKILL.md, that has a file name leaving its directory, or that is too big all look the same
// to the caller: not found.
func (a *App) SkillBundleForWorker(ctx context.Context, tenantID, handle, slug string) (SkillBundle, error) {
	skillID, ok := skillID(handle, slug)
	if !ok {
		return SkillBundle{}, store.ErrNotFound
	}
	rec, err := a.Repo.GetSkill(ctx, tenantID, skillID)
	if err != nil {
		return SkillBundle{}, err
	}
	files, known, err := a.skillFiles(ctx, tenantID, skillID)
	if err != nil {
		return SkillBundle{}, err
	}
	if !known || !skillUsable(files) {
		return SkillBundle{}, store.ErrNotFound
	}
	size := 0
	out := make([]SkillTextFile, 0, len(files))
	for _, file := range files {
		size += len(file.Body)
		out = append(out, SkillTextFile{Path: file.Path, Body: file.Body})
	}
	if size > maxSkillBundleBytes {
		return SkillBundle{}, store.ErrNotFound
	}
	return SkillBundle{ID: skillID, Name: rec.Name, Description: rec.Description, Files: out}, nil
}

// ErrUnknownProfile marks a profile a caller named that is not a profile of their tenant.
var ErrUnknownProfile = errors.New("unknown profile")

// CheckProfile accepts a profile a task node may be switched to (11 §3): the built-in default, or one of the tenant's own.
// It fails with ErrInvalid when the reference is malformed and with ErrUnknownProfile when the tenant has no such profile
// (what belongs to another tenant looks the same).
func (a *App) CheckProfile(ctx context.Context, p taskruntime.Principal, ref string) error {
	if ref == "default@1" {
		return nil
	}
	if !versionedRef.MatchString(ref) {
		return invalidf("profile is not a versioned reference")
	}
	profile, err := a.Tasks.GetProfile(ctx, p, ref)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrUnknownProfile
		}
		return err
	}
	if profile.Spec["kind"] == teamKind {
		// A team has no agent to run; a node switches to one of its members' experts instead.
		return invalidf("a team cannot be a node's profile")
	}
	return nil
}

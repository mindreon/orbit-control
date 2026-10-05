package app

import (
	"context"
	"errors"
	"path"
	"regexp"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// A task's configuration as a caller states it (15 M8). Skills and ConnectorIDs are the complete sets to use: nil keeps
// the expert's defaults, an empty list removes them.
type TaskConfigRequest struct {
	Expert       string
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
		return out, invalidf("mode must be default, plan or ask")
	}
	if err := a.checkExpert(ctx, p, in.Expert); err != nil {
		return out, err
	}
	if err := checkRefList("skills", in.Skills); err != nil {
		return out, err
	}
	if err := checkRefList("connectors", in.ConnectorIDs); err != nil {
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
		for _, skillID := range in.Skills {
			if err := a.checkSkill(ctx, p.TenantID, skillID); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

func (a *App) checkExpert(ctx context.Context, p taskruntime.Principal, ref string) error {
	if ref == "" || ref == "default@1" {
		return nil
	}
	if !versionedRef.MatchString(ref) {
		return invalidf("expert is not a versioned reference")
	}
	if _, err := a.Tasks.GetProfile(ctx, p, ref); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return invalidf("unknown expert")
		}
		return err
	}
	return nil
}

func checkRefList(what string, ids []string) error {
	if len(ids) > maxTaskConfigRefs {
		return invalidf("more than %d %s", maxTaskConfigRefs, what)
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			return invalidf("%s lists one entry twice", what)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// checkSkill accepts a skill only when the worker can actually use it: the catalog has it, its text is filled, it has a
// SKILL.md and no file name could leave the skill's directory. The others are skipped, not half-installed.
func (a *App) checkSkill(ctx context.Context, tenantID, skillID string) error {
	if _, err := a.Repo.GetSkill(ctx, tenantID, skillID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return invalidf("unknown skill")
		}
		return err
	}
	files, known, err := a.skillFiles(ctx, tenantID, skillID)
	if err != nil {
		return err
	}
	if !known || !skillUsable(files) {
		return invalidf("skill cannot be used")
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
	if _, err := a.Tasks.GetProfile(ctx, p, ref); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrUnknownProfile
		}
		return err
	}
	return nil
}

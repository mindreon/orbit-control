package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// ExpertFileInfo is one file of an expert version's bundle.
type ExpertFileInfo struct {
	Path   string `json:"path"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

// ExpertFileContent is the text of one file.
type ExpertFileContent struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	SHA256  string `json:"sha256"`
}

// expertVersionFiles reads the files of a version of a single expert; version 0 is the latest. A version written before
// bundles has no files and answers with an empty list.
func (a *App) expertVersionFiles(ctx context.Context, p taskruntime.Principal, expertID string, version int) (int, []taskruntime.ProfileFile, error) {
	if version == 0 {
		latest, err := a.GetExpert(ctx, p, expertID)
		if err != nil {
			return 0, nil, err
		}
		version = latest.Version
	}
	profile, err := a.Tasks.GetProfile(ctx, p, fmt.Sprintf("%s@%d", expertID, version))
	if err != nil {
		return 0, nil, err
	}
	if profile.Spec["kind"] != expertKind {
		return 0, nil, store.ErrNotFound
	}
	files, err := a.Tasks.ProfileFiles(ctx, p, profile.Ref)
	return version, files, err
}

// ExpertFiles lists the files of an expert version.
func (a *App) ExpertFiles(ctx context.Context, p taskruntime.Principal, expertID string, version int) (int, []ExpertFileInfo, error) {
	version, files, err := a.expertVersionFiles(ctx, p, expertID, version)
	if err != nil {
		return 0, nil, err
	}
	infos := make([]ExpertFileInfo, 0, len(files))
	for _, f := range files {
		infos = append(infos, ExpertFileInfo{Path: f.Path, Size: len(f.Content), SHA256: f.SHA256})
	}
	return version, infos, nil
}

// ExpertFile reads one file of an expert version.
func (a *App) ExpertFile(ctx context.Context, p taskruntime.Principal, expertID string, version int, filePath string) (ExpertFileContent, error) {
	_, files, err := a.expertVersionFiles(ctx, p, expertID, version)
	if err != nil {
		return ExpertFileContent{}, err
	}
	for _, f := range files {
		if f.Path == filePath {
			return ExpertFileContent{Path: f.Path, Content: f.Content, SHA256: f.SHA256}, nil
		}
	}
	return ExpertFileContent{}, store.ErrNotFound
}

// ExpertSkillBundleForWorker serves the worker one bundle skill of an expert version, in the shape of a catalog skill.
// An unknown tenant, expert, version or skill, a skill the version does not list, and a skill that is not usable all
// look the same: not found.
func (a *App) ExpertSkillBundleForWorker(ctx context.Context, tenantID, expertID string, version int, name string) (SkillBundle, error) {
	p := taskruntime.Principal{TenantID: tenantID}
	ref := fmt.Sprintf("%s@%d", expertID, version)
	if tenantID == "" || version < 1 || name == "" {
		return SkillBundle{}, store.ErrNotFound
	}
	profile, err := a.Tasks.GetProfile(ctx, p, ref)
	if err != nil {
		return SkillBundle{}, err
	}
	listed := false
	if raw, ok := profile.Spec["bundle_skills"].([]any); ok {
		for _, item := range raw {
			listed = listed || item == name
		}
	}
	if !listed {
		return SkillBundle{}, store.ErrNotFound
	}
	files, err := a.Tasks.ProfileFiles(ctx, p, ref)
	if err != nil {
		return SkillBundle{}, err
	}
	prefix := skillsDir + name + "/"
	var skill []store.SkillFile
	size := 0
	for _, f := range files {
		if rest, ok := strings.CutPrefix(f.Path, prefix); ok {
			skill = append(skill, store.SkillFile{Path: rest, Body: f.Content})
			size += len(f.Content)
		}
	}
	if !skillUsable(skill) || size > maxSkillBundleBytes {
		return SkillBundle{}, store.ErrNotFound
	}
	out := make([]SkillTextFile, 0, len(skill))
	for _, f := range skill {
		out = append(out, SkillTextFile{Path: f.Path, Body: f.Body})
	}
	return SkillBundle{ID: ref + "/" + name, Name: name, Description: "", Files: out}, nil
}

package app

import (
	"context"

	"github.com/mindreon/orbit-control/internal/store"
)

// Agent is one locally stored ModelScope agent card. This API does not run it.
type Agent struct {
	ID            string              `json:"id"`
	Handle        string              `json:"handle"`
	Slug          string              `json:"slug"`
	Name          string              `json:"name"`
	Description   string              `json:"description"`
	Framework     string              `json:"framework"`
	License       string              `json:"license"`
	LogoURL       string              `json:"logoUrl"`
	Catalogues    []string            `json:"catalogues"`
	Models        []store.AgentModel  `json:"models"`
	Mcps          []store.AgentRef    `json:"mcps"`
	Skills        []store.AgentRef    `json:"skills"`
	SystemPrompts []store.AgentPrompt `json:"systemPrompts"`
	Readme        string              `json:"readme"`
	Files         []SkillTextFile     `json:"files"`
	Stars         int64               `json:"stars"`
	Downloads     int64               `json:"downloads"`
	Visits        int64               `json:"visits"`
	UpdatedAt     string              `json:"updatedAt"`
	Source        string              `json:"source"`
}

// AgentList is one page of the stored agents.
type AgentList struct {
	Items    []Agent `json:"items"`
	Total    int     `json:"total"`
	Page     int     `json:"page"`
	PageSize int     `json:"pageSize"`
}

// ListAgents reads the stored agent snapshot. It does not call ModelScope.
func (a *App) ListAgents(ctx context.Context, tenantID string, q store.AgentCatalogQuery) (AgentList, error) {
	page, err := a.Repo.ListAgentCatalog(ctx, tenantID, q)
	if err != nil {
		return AgentList{}, err
	}
	items := make([]Agent, 0, len(page.Items))
	for _, rec := range page.Items {
		items = append(items, agentFrom(rec, false))
	}
	return AgentList{Items: items, Total: page.Total, Page: page.Page, PageSize: page.PageSize}, nil
}

// GetAgent reads one stored agent with its files. A missing row and a
// malformed path both look the same.
func (a *App) GetAgent(ctx context.Context, tenantID, handle, slug string) (Agent, error) {
	id, ok := store.SkillPathID(handle, slug)
	if !ok {
		return Agent{}, store.ErrNotFound
	}
	rec, err := a.Repo.GetAgent(ctx, tenantID, id)
	if err != nil {
		return Agent{}, err
	}
	return agentFrom(rec, true), nil
}

// AgentIcon returns the content type and bytes of one agent's logo. A missing
// logo and a logo the sidecar did not ship both 404.
func (a *App) AgentIcon(ctx context.Context, tenantID, handle, slug string) (string, []byte, error) {
	id, ok := store.SkillPathID(handle, slug)
	if !ok {
		return "", nil, store.ErrNotFound
	}
	rec, err := a.Repo.GetAgent(ctx, tenantID, id)
	if err != nil {
		return "", nil, err
	}
	return a.Repo.CatalogIcon(ctx, tenantID, rec.LogoURL)
}

// agentFrom maps the stored row to the API shape. withoutFiles leaves the
// (potentially large) file list out of list responses.
func agentFrom(rec store.AgentRecord, withFiles bool) Agent {
	files := []SkillTextFile{}
	if withFiles && rec.FilesKnown {
		files = make([]SkillTextFile, 0, len(rec.Files))
		for _, file := range rec.Files {
			files = append(files, SkillTextFile{Path: file.Path, Body: file.Body})
		}
	}
	catalogues := rec.Catalogues
	if catalogues == nil {
		catalogues = []string{}
	}
	models := rec.Models
	if models == nil {
		models = []store.AgentModel{}
	}
	mcps := rec.Mcps
	if mcps == nil {
		mcps = []store.AgentRef{}
	}
	skills := rec.Skills
	if skills == nil {
		skills = []store.AgentRef{}
	}
	prompts := rec.SystemPrompts
	if prompts == nil {
		prompts = []store.AgentPrompt{}
	}
	return Agent{
		ID: rec.ID, Handle: rec.Handle, Slug: rec.Slug, Name: rec.Name, Description: rec.Description,
		Framework: rec.Framework, License: rec.License, LogoURL: rec.LogoURL, Catalogues: catalogues,
		Models: models, Mcps: mcps, Skills: skills, SystemPrompts: prompts, Readme: rec.Readme,
		Files: files, Stars: rec.Stars, Downloads: rec.Downloads, Visits: rec.Visits,
		UpdatedAt: stamp(rec.UpdatedAt), Source: rec.Source,
	}
}

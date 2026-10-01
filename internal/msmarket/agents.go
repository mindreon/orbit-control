package msmarket

import (
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

// agentRow is one line of agents.json.gz. Field names are the export
// contract with tools/modelscope-crawler's export-orbit.
type agentRow struct {
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
	Files         []store.SkillFile   `json:"files"`
	Stars         int64               `json:"stars"`
	Downloads     int64               `json:"downloads"`
	Visits        int64               `json:"visits"`
	UpdatedAt     int64               `json:"updatedAt"`
	Source        string              `json:"source"`
}

// loadAgents reads the embedded agents snapshot.
func loadAgents() ([]store.AgentRecord, error) {
	rows, err := readJSONL[agentRow](mustEmbed("agents.json.gz"))
	if err != nil {
		return nil, err
	}
	out := make([]store.AgentRecord, 0, len(rows))
	for _, row := range rows {
		id, ok := store.SkillPathID(row.Handle, row.Slug)
		if !ok || id != row.ID || row.Name == "" {
			continue
		}
		prompts := row.SystemPrompts
		if prompts == nil {
			prompts = []store.AgentPrompt{}
		}
		out = append(out, store.AgentRecord{
			ID: id, Handle: row.Handle, Slug: row.Slug, Name: row.Name, Description: row.Description,
			Framework: row.Framework, License: row.License, LogoURL: row.LogoURL, Catalogues: row.Catalogues,
			Models: row.Models, Mcps: row.Mcps, Skills: row.Skills, SystemPrompts: prompts, Readme: row.Readme,
			Files: row.Files, FilesKnown: row.Files != nil, Stars: row.Stars, Downloads: row.Downloads,
			Visits: row.Visits, UpdatedAt: time.Unix(row.UpdatedAt, 0).UTC(), Source: source(row.Source),
		})
	}
	return out, nil
}

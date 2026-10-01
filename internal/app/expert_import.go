package app

import (
	"context"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// Two ways to get an expert without writing one (15 T8.1): from a catalog agent card, and from an assistant (persona) the
// tenant made before there were experts. Both end as an ordinary expert, one immutable profile version.

// UnmatchedRefs is what a catalog agent asks for that the tenant does not have, by name. Nothing is guessed: it is
// returned so the person can add it, not dropped.
type UnmatchedRefs struct {
	Skills     []string `json:"skills"`
	Connectors []string `json:"connectors"`
}

type ImportedExpert struct {
	Expert    Expert        `json:"expert"`
	Unmatched UnmatchedRefs `json:"unmatched"`
}

// ExpertFromAgent makes an expert of a catalog agent: its prompts become the instructions, and the connectors and skills
// it names are matched, exactly and by name, to the tenant's own.
func (a *App) ExpertFromAgent(ctx context.Context, p taskruntime.Principal, handle, slug string) (ImportedExpert, error) {
	agentID, ok := store.SkillPathID(handle, slug)
	if !ok {
		return ImportedExpert{}, store.ErrNotFound
	}
	agent, err := a.Repo.GetAgent(ctx, p.TenantID, agentID)
	if err != nil {
		return ImportedExpert{}, err
	}
	prompts := make([]string, 0, len(agent.SystemPrompts))
	for _, prompt := range agent.SystemPrompts {
		prompts = append(prompts, prompt.Content)
	}
	connectorIDs, lostConnectors, err := a.matchConnectors(ctx, p.TenantID, refNames(agent.Mcps))
	if err != nil {
		return ImportedExpert{}, err
	}
	skillIDs, lostSkills, err := a.matchSkills(ctx, p.TenantID, refNames(agent.Skills))
	if err != nil {
		return ImportedExpert{}, err
	}
	expert, err := a.writeExpert(ctx, p, id(expertIDPrefix), 1, ExpertInput{
		Name: clip(agent.Name, maxExpertName), Instructions: agentInstructions(prompts, agent.Description),
		ConnectorIDs: connectorIDs, SkillIDs: skillIDs, Source: "agent:" + agentID,
	})
	if err != nil {
		return ImportedExpert{}, err
	}
	return ImportedExpert{Expert: expert, Unmatched: UnmatchedRefs{Skills: lostSkills, Connectors: lostConnectors}}, nil
}

// ImportPersonas turns each assistant of the tenant into an expert, once: one already imported is skipped, so the call can
// be repeated. A connector an assistant listed that no longer exists is left out.
func (a *App) ImportPersonas(ctx context.Context, p taskruntime.Principal) (imported, skipped int, err error) {
	personas, err := a.Repo.ListPersonas(ctx, p.TenantID)
	if err != nil {
		return 0, 0, err
	}
	existing, err := a.latestExperts(ctx, p)
	if err != nil {
		return 0, 0, err
	}
	done := map[string]bool{}
	for _, expert := range existing {
		done[expert.Source] = true
	}
	connectors, err := a.Repo.ListMcpConnectors(ctx, p.TenantID)
	if err != nil {
		return 0, 0, err
	}
	have := map[string]bool{}
	for _, connector := range connectors {
		have[connector.ID] = true
	}
	for _, persona := range personas {
		source := "persona:" + persona.ID
		if done[source] {
			skipped++
			continue
		}
		var ids []string
		for _, connectorID := range persona.McpConnectorIDs {
			if have[connectorID] {
				ids = append(ids, connectorID)
			}
		}
		if _, err := a.writeExpert(ctx, p, id(expertIDPrefix), 1, ExpertInput{
			Name: clip(persona.Name, maxExpertName), Instructions: clip(persona.Instructions, maxExpertInstructions), ConnectorIDs: ids, Source: source,
		}); err != nil {
			return imported, skipped, err
		}
		imported++
	}
	return imported, skipped, nil
}

func (a *App) matchConnectors(ctx context.Context, tenantID string, names []string) (ids, lost []string, err error) {
	records, err := a.Repo.ListMcpConnectors(ctx, tenantID)
	if err != nil {
		return nil, nil, err
	}
	byName := make(map[string]string, len(records))
	for _, rec := range records {
		byName[strings.ToLower(strings.TrimSpace(rec.Name))] = rec.ID
	}
	ids, lost = matchNames(names, byName)
	return ids, lost, nil
}

// matchSkills looks each name up in the catalog and keeps the one whose name is the same and which the worker could use.
func (a *App) matchSkills(ctx context.Context, tenantID string, names []string) (ids, lost []string, err error) {
	ids, lost = []string{}, []string{}
	for _, name := range uniqueNames(names) {
		page, err := a.Repo.ListSkillCatalog(ctx, tenantID, store.SkillCatalogQuery{Keyword: name, PageSize: 10})
		if err != nil {
			return nil, nil, err
		}
		found := ""
		for _, rec := range page.Items {
			if strings.EqualFold(strings.TrimSpace(rec.Name), name) && a.checkSkill(ctx, tenantID, rec.ID) == nil {
				found = rec.ID
				break
			}
		}
		if found == "" {
			lost = append(lost, name)
		} else {
			ids = append(ids, found)
		}
	}
	return ids, lost, nil
}

// matchNames pairs the wanted names with ids by exact name, ignoring case and surrounding spaces. A blank name is not a
// request. What did not match is returned, in the order it was asked for.
func matchNames(wanted []string, have map[string]string) (matched, lost []string) {
	matched, lost = []string{}, []string{}
	for _, name := range uniqueNames(wanted) {
		if matchedID, ok := have[strings.ToLower(name)]; ok {
			matched = append(matched, matchedID)
		} else {
			lost = append(lost, name)
		}
	}
	return matched, lost
}

func uniqueNames(names []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" || seen[strings.ToLower(name)] {
			continue
		}
		seen[strings.ToLower(name)] = true
		out = append(out, name)
	}
	return out
}

func refNames(refs []store.AgentRef) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Name)
	}
	return names
}

// agentInstructions joins an agent's prompt files; an agent without any is described by its description instead.
func agentInstructions(prompts []string, description string) string {
	parts := make([]string, 0, len(prompts))
	for _, prompt := range prompts {
		if text := strings.TrimSpace(prompt); text != "" {
			parts = append(parts, text)
		}
	}
	text := strings.Join(parts, "\n\n")
	if text == "" {
		text = strings.TrimSpace(description)
	}
	return clip(text, maxExpertInstructions)
}

func clip(text string, limit int) string {
	text = strings.TrimSpace(text)
	if runes := []rune(text); len(runes) > limit {
		return string(runes[:limit])
	}
	return text
}

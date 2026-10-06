package app

import (
	"context"
	"fmt"
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

// SkippedFile is a file of a catalog agent that did not come into the bundle, and why.
type SkippedFile struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type ImportedExpert struct {
	Expert    Expert        `json:"expert"`
	Unmatched UnmatchedRefs `json:"unmatched"`
	// McpUnbound are the agent's mcp.json entries that no connector of the tenant matches: kept in the bundle, not loaded.
	McpUnbound []MCPUnbound `json:"mcp_unbound"`
	// SkippedFiles are the files that could not be stored: not text, too big, or over the bundle's limits.
	SkippedFiles []SkippedFile `json:"skipped_files"`
}

// ExpertFromAgent makes an expert of a catalog agent (ADR-0013 §4): the files of the agent become the bundle. AGENTS.md
// is made of its prompts, or of its name and description, when the snapshot has none; skills/ directories are bundle
// skills (nothing is matched to the tenant's catalog by name); mcp.json entries are bound to the tenant's connectors by
// name or url where one fits, and the rest are reported.
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
	records, err := a.Repo.ListMcpConnectors(ctx, p.TenantID)
	if err != nil {
		return ImportedExpert{}, err
	}
	files, skipped := snapshotBundle(agent.Files)
	name := clip(agent.Name, maxExpertName)
	if strings.TrimSpace(files[fileAgents]) == "" {
		files[fileAgents] = agentInstructions(prompts, agent.Name, agent.Description)
	}
	soul, instructions := strings.TrimSpace(files[fileSoul]), strings.TrimSpace(files[fileAgents])
	delete(files, fileSoul)
	delete(files, fileAgents)
	delete(files, fileAgent)
	soul, instructions, clipped := fitPrompt(soul, instructions)
	for _, path := range clipped {
		skipped = append(skipped, SkippedFile{Path: path, Reason: fmt.Sprintf("cut to fit %d characters of soul and instructions", maxExpertInstructions)})
	}
	connectorIDs = a.importMCP(files, records, connectorIDs, &skipped)
	a.importSkills(ctx, p.TenantID, files, &skipped)
	expert, err := a.writeExpert(ctx, p, id(expertIDPrefix), 1, ExpertInput{
		Name: name, Instructions: instructions, Soul: soul, ConnectorIDs: connectorIDs, Source: "agent:" + agentID,
		description: clip(agent.Description, 500), baseFiles: files,
	})
	if err != nil {
		return ImportedExpert{}, err
	}
	if skipped == nil {
		skipped = []SkippedFile{}
	}
	return ImportedExpert{
		Expert: expert, Unmatched: UnmatchedRefs{Skills: []string{}, Connectors: lostConnectors},
		McpUnbound: expert.McpUnbound, SkippedFiles: skipped,
	}, nil
}

// snapshotBundle takes the text files of a snapshot into a bundle, and lists the ones it cannot hold.
func snapshotBundle(files []store.SkillFile) (bundleFiles, []SkippedFile) {
	out := bundleFiles{}
	var skipped []SkippedFile
	total := 0
	for _, file := range files {
		reason := ""
		switch {
		case pathProblem(file.Path) != "":
			reason = pathProblem(file.Path)
		case textProblem(file.Body) != "":
			reason = "binary file"
		case len(file.Body) > maxBundleFileBytes:
			reason = fmt.Sprintf("over %d bytes", maxBundleFileBytes)
		case func() bool { _, dup := out[file.Path]; return dup }():
			reason = "the path is listed twice"
		case len(out) >= maxBundleFiles:
			reason = fmt.Sprintf("the bundle has %d files already", maxBundleFiles)
		case total+len(file.Body) > maxBundleBytes:
			reason = "the bundle is full"
		}
		if reason != "" {
			skipped = append(skipped, SkippedFile{Path: file.Path, Reason: reason})
			continue
		}
		total += len(file.Body)
		out[file.Path] = file.Body
	}
	return out, skipped
}

// fitPrompt cuts soul, then instructions, to what the two may be together; the paths it had to cut are returned.
func fitPrompt(soul, instructions string) (string, string, []string) {
	var clipped []string
	// A soul never takes the room of the instructions: AGENTS.md has to keep something.
	if limit := maxExpertInstructions - 1000; len([]rune(soul)) > limit {
		soul = clip(soul, limit)
		clipped = append(clipped, fileSoul)
	}
	if room := maxExpertInstructions - len([]rune(soul)); len([]rune(instructions)) > room {
		instructions = clip(instructions, room)
		clipped = append(clipped, fileAgents)
	}
	return soul, instructions, clipped
}

// importMCP rewrites the snapshot's mcp.json for the tenant: an entry whose name or url is a connector of the tenant
// is bound to it (its connector id is the tenant's, never the snapshot's), a literal credential is removed, and the file
// is left out when it cannot be read. It returns the connector ids to bind.
func (a *App) importMCP(files bundleFiles, records []store.McpConnectorRecord, ids []string, skipped *[]SkippedFile) []string {
	content, ok := files[fileMCP]
	if !ok {
		return ids
	}
	delete(files, fileMCP)
	servers, err := parseMCPJSON(content)
	if err != nil {
		*skipped = append(*skipped, SkippedFile{Path: fileMCP, Reason: "not a valid mcp.json"})
		return ids
	}
	bound := map[string]bool{}
	for _, connectorID := range ids {
		bound[connectorID] = true
	}
	var kept []mcpServer
	for _, server := range servers {
		delete(server.Obj, "connector_id")
		if removed := server.scrub(); len(removed) > 0 {
			*skipped = append(*skipped, SkippedFile{Path: fileMCP, Reason: fmt.Sprintf("%s of %q held a literal credential and was removed", removed[0], server.Name)})
		}
		if server.str("url") == "" && server.str("command") == "" {
			continue // nothing is left of it to keep
		}
		for _, rec := range records {
			sameName := strings.EqualFold(strings.TrimSpace(rec.Name), server.Name)
			sameURL := rec.URL != "" && rec.URL == server.str("url")
			if (sameName || sameURL) && (bound[rec.ID] || len(bound) < maxExpertConnectors) {
				server.Obj["connector_id"] = rec.ID
				if !bound[rec.ID] {
					bound[rec.ID] = true
					ids = append(ids, rec.ID)
				}
				break
			}
		}
		kept = append(kept, server)
	}
	if len(kept) > 0 {
		files[fileMCP] = renderMCPJSON(kept)
	}
	return ids
}

// importSkills leaves out a skills.json that the tenant cannot use as it is: a catalog entry it does not have, or a bundle
// entry without its directory. The skills/ directories still load.
func (a *App) importSkills(ctx context.Context, tenantID string, files bundleFiles, skipped *[]SkippedFile) {
	content, ok := files[fileSkills]
	if !ok {
		return
	}
	entries, err := parseSkillsJSON(content)
	if err == nil {
		dirs := map[string]bool{}
		for _, name := range files.bundleSkillNames() {
			dirs[name] = true
		}
		for i, entry := range entries {
			if entry.Source == "bundle" && !dirs[entry.Name] {
				err = invalidField("BUNDLE_SKILL_MISSING", "skills.json", entry.Name)
			} else if entry.Source == "catalog" {
				err = a.checkSkill(ctx, tenantID, entry.ID, fmt.Sprintf("skills[%d]", i))
			}
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		delete(files, fileSkills)
		*skipped = append(*skipped, SkippedFile{Path: fileSkills, Reason: "its entries do not match this tenant or the bundle's skills/ directories"})
	}
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
			Name: clip(persona.Name, maxExpertName), Instructions: agentInstructions([]string{persona.Instructions}, persona.Name, ""),
			ConnectorIDs: ids, Source: source,
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

// agentInstructions joins an agent's prompt files; an agent without any is described by its name and description instead,
// so an expert never lacks AGENTS.md.
func agentInstructions(prompts []string, name, description string) string {
	parts := make([]string, 0, len(prompts))
	for _, prompt := range prompts {
		if text := strings.TrimSpace(prompt); text != "" {
			parts = append(parts, text)
		}
	}
	text := strings.Join(parts, "\n\n")
	if text == "" {
		text = strings.TrimSpace(strings.TrimSpace(name) + "\n\n" + strings.TrimSpace(description))
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

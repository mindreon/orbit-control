package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

type Persona struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Instructions    string   `json:"instructions"`
	McpConnectorIDs []string `json:"mcpConnectorIds,omitempty"`
	CreatedAt       string   `json:"createdAt"`
}

type McpConnector struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Command   string   `json:"command"`
	Args      []string `json:"args,omitempty"`
	EnvRefs   []string `json:"envRefs,omitempty"`
	CreatedAt string   `json:"createdAt"`
}

// GrantPublic is the API-safe view (no secret values).
type GrantPublic struct {
	ID        string   `json:"id"`
	EnvNames  []string `json:"envNames"`
	ExpiresAt string   `json:"expiresAt"`
}

type grantRecord struct {
	ID        string
	Env       map[string]string
	EnvNames  []string
	ExpiresAt time.Time
}

type CloudAgentJob struct {
	ID               string `json:"id"`
	RepoURL          string `json:"repoUrl"`
	Branch           string `json:"branch,omitempty"`
	Prompt           string `json:"prompt"`
	PermissionPreset string `json:"permissionPreset"`
	PersonaID        string `json:"personaId,omitempty"`
	State            string `json:"state"`
	CreatedAt        string `json:"createdAt"`
}

type CreateCloudAgentInput struct {
	RepoURL          string
	Prompt           string
	Branch           string
	PermissionPreset string
	PersonaID        string
}

func (a *App) ensureCatalog() {
	if a.Personas == nil {
		a.Personas = map[string]*Persona{}
	}
	if a.McpConnectors == nil {
		a.McpConnectors = map[string]*McpConnector{}
	}
	if a.grants == nil {
		a.grants = map[string]*grantRecord{}
	}
	if a.CloudAgents == nil {
		a.CloudAgents = map[string]*CloudAgentJob{}
	}
	if a.Store == nil {
		a.Store = store.New("")
	}
}

func trimNonEmpty(in []string) []string {
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func cleanNames(in []string) ([]string, error) {
	out := trimNonEmpty(in)
	for _, item := range out {
		// A name that contains "=" is a pasted KEY=value, which must not be stored.
		if strings.Contains(item, "=") {
			return nil, fmt.Errorf("name must not include a value")
		}
	}
	return out, nil
}

func personaFromRecord(rec store.PersonaRecord) *Persona {
	return &Persona{
		ID:              rec.ID,
		Name:            rec.Name,
		Instructions:    rec.Instructions,
		McpConnectorIDs: append([]string(nil), rec.McpConnectorIDs...),
		CreatedAt:       stamp(rec.CreatedAt),
	}
}

func connectorFromRecord(rec store.McpConnectorRecord) *McpConnector {
	return &McpConnector{
		ID:        rec.ID,
		Name:      rec.Name,
		Command:   rec.Command,
		Args:      append([]string(nil), rec.Args...),
		EnvRefs:   append([]string(nil), rec.EnvRefs...),
		CreatedAt: stamp(rec.CreatedAt),
	}
}

func (a *App) ListPersonas(ctx context.Context, tenantID string) ([]*Persona, error) {
	recs, err := a.Repo.ListPersonas(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]*Persona, 0, len(recs))
	next := map[string]*Persona{}
	for _, rec := range recs {
		p := personaFromRecord(rec)
		next[p.ID] = p
		cp := *p
		out = append(out, &cp)
	}
	a.mu.Lock()
	a.ensureCatalog()
	a.Personas = next
	a.mu.Unlock()
	return out, nil
}

func (a *App) CreatePersona(ctx context.Context, tenantID, name, instructions string, mcpIDs []string) (*Persona, error) {
	name = strings.TrimSpace(name)
	instructions = strings.TrimSpace(instructions)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	ids, err := cleanNames(mcpIDs)
	if err != nil {
		return nil, err
	}
	created := time.Now().UTC()
	p := &Persona{
		ID:              id("persona_"),
		Name:            name,
		Instructions:    instructions,
		McpConnectorIDs: ids,
		CreatedAt:       stamp(created),
	}
	if err := a.Repo.CreatePersona(ctx, tenantID, store.PersonaRecord{
		ID: p.ID, Name: p.Name, Instructions: p.Instructions, McpConnectorIDs: ids, CreatedAt: created,
	}); err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.ensureCatalog()
	a.Personas[p.ID] = p
	_ = a.Store.WriteJSON("personas/"+p.ID+".json", p)
	cp := *p
	a.mu.Unlock()
	return &cp, nil
}

func (a *App) ListMcpConnectors(ctx context.Context, tenantID string) ([]*McpConnector, error) {
	recs, err := a.Repo.ListMcpConnectors(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]*McpConnector, 0, len(recs))
	next := map[string]*McpConnector{}
	for _, rec := range recs {
		c := connectorFromRecord(rec)
		next[c.ID] = c
		cp := *c
		out = append(out, &cp)
	}
	a.mu.Lock()
	a.ensureCatalog()
	a.McpConnectors = next
	a.mu.Unlock()
	return out, nil
}

func (a *App) CreateMcpConnector(ctx context.Context, tenantID, name, command string, args, envRefs []string) (*McpConnector, error) {
	name = strings.TrimSpace(name)
	command = strings.TrimSpace(command)
	if name == "" || command == "" {
		return nil, fmt.Errorf("name and command are required")
	}
	cleanArgs := trimNonEmpty(args)
	refs, err := cleanNames(envRefs)
	if err != nil {
		return nil, err
	}
	created := time.Now().UTC()
	c := &McpConnector{
		ID:        id("mcp_"),
		Name:      name,
		Command:   command,
		Args:      cleanArgs,
		EnvRefs:   refs,
		CreatedAt: stamp(created),
	}
	if err := a.Repo.CreateMcpConnector(ctx, tenantID, store.McpConnectorRecord{
		ID: c.ID, Name: c.Name, Command: c.Command, Args: cleanArgs, EnvRefs: refs, CreatedAt: created,
	}); err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.ensureCatalog()
	a.McpConnectors[c.ID] = c
	_ = a.Store.WriteJSON("mcp/"+c.ID+".json", c)
	cp := *c
	a.mu.Unlock()
	return &cp, nil
}

func (a *App) MintGrant(env map[string]string, ttlSeconds int) (*GrantPublic, error) {
	if len(env) == 0 {
		return nil, fmt.Errorf("env is required")
	}
	if ttlSeconds <= 0 {
		ttlSeconds = 900
	}
	names := make([]string, 0, len(env))
	clean := map[string]string{}
	for k, v := range env {
		k = strings.TrimSpace(k)
		if k == "" || v == "" {
			continue
		}
		clean[k] = v
		names = append(names, k)
	}
	if len(clean) == 0 {
		return nil, fmt.Errorf("env must contain at least one non-empty entry")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureCatalog()
	expires := time.Now().UTC().Add(time.Duration(ttlSeconds) * time.Second)
	g := &grantRecord{
		ID:        id("grant_"),
		Env:       clean,
		EnvNames:  names,
		ExpiresAt: expires,
	}
	a.grants[g.ID] = g
	pub := GrantPublic{ID: g.ID, EnvNames: names, ExpiresAt: expires.Format(time.RFC3339Nano)}
	_ = a.Store.WriteJSON("grants/"+g.ID+".json", pub)
	return &pub, nil
}

func (a *App) ListCloudAgents() []*CloudAgentJob {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureCatalog()
	out := make([]*CloudAgentJob, 0, len(a.CloudAgents))
	for _, j := range a.CloudAgents {
		cp := *j
		out = append(out, &cp)
	}
	return out
}

func (a *App) CreateCloudAgent(ctx context.Context, input CreateCloudAgentInput) (*CloudAgentJob, error) {
	_ = ctx
	repo := strings.TrimSpace(input.RepoURL)
	prompt := strings.TrimSpace(input.Prompt)
	if repo == "" || prompt == "" {
		return nil, fmt.Errorf("repoUrl and prompt are required")
	}
	preset, err := normalizePermissionPreset(input.PermissionPreset)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureCatalog()
	if input.PersonaID != "" {
		if _, ok := a.Personas[input.PersonaID]; !ok {
			return nil, fmt.Errorf("persona not found")
		}
	}
	job := &CloudAgentJob{
		ID:               id("caj_"),
		RepoURL:          repo,
		Branch:           strings.TrimSpace(input.Branch),
		Prompt:           prompt,
		PermissionPreset: preset,
		PersonaID:        strings.TrimSpace(input.PersonaID),
		State:            "queued",
		CreatedAt:        now(),
	}
	a.CloudAgents[job.ID] = job
	_ = a.Store.WriteJSON("cloud-agents/"+job.ID+".json", job)
	cp := *job
	return &cp, nil
}

func (a *App) CompositionForRoom(personaID, grantID string) (*Persona, []*McpConnector, map[string]string, error) {
	if personaID != "" {
		a.mu.Lock()
		a.ensureCatalog()
		_, cached := a.Personas[personaID]
		a.mu.Unlock()
		if !cached {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = a.ListPersonas(ctx, a.DefaultTenant)
			_, _ = a.ListMcpConnectors(ctx, a.DefaultTenant)
			cancel()
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureCatalog()
	var persona *Persona
	var connectors []*McpConnector
	var grantEnv map[string]string
	if personaID != "" {
		p, ok := a.Personas[personaID]
		if !ok {
			return nil, nil, nil, fmt.Errorf("persona not found")
		}
		cp := *p
		persona = &cp
		for _, mcpID := range p.McpConnectorIDs {
			if c, ok := a.McpConnectors[mcpID]; ok {
				cc := *c
				connectors = append(connectors, &cc)
			}
		}
	}
	if grantID != "" {
		g, ok := a.grants[grantID]
		if !ok {
			return nil, nil, nil, fmt.Errorf("grant not found")
		}
		if time.Now().UTC().After(g.ExpiresAt) {
			return nil, nil, nil, fmt.Errorf("grant expired")
		}
		grantEnv = map[string]string{}
		for k, v := range g.Env {
			grantEnv[k] = v
		}
	}
	return persona, connectors, grantEnv, nil
}

func (a *App) persistActivity(roomID string, item Envelope) {
	a.ensureCatalog()
	_ = a.Store.AppendJSONL("audit/"+roomID+".jsonl", item)
}

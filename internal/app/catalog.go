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

func (a *App) ListPersonas() []*Persona {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureCatalog()
	out := make([]*Persona, 0, len(a.Personas))
	for _, p := range a.Personas {
		cp := *p
		out = append(out, &cp)
	}
	return out
}

func (a *App) CreatePersona(name, instructions string, mcpIDs []string) (*Persona, error) {
	name = strings.TrimSpace(name)
	instructions = strings.TrimSpace(instructions)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureCatalog()
	p := &Persona{
		ID:              id("persona_"),
		Name:            name,
		Instructions:    instructions,
		McpConnectorIDs: append([]string(nil), mcpIDs...),
		CreatedAt:       now(),
	}
	a.Personas[p.ID] = p
	_ = a.Store.WriteJSON("personas/"+p.ID+".json", p)
	cp := *p
	return &cp, nil
}

func (a *App) ListMcpConnectors() []*McpConnector {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureCatalog()
	out := make([]*McpConnector, 0, len(a.McpConnectors))
	for _, c := range a.McpConnectors {
		cp := *c
		out = append(out, &cp)
	}
	return out
}

func (a *App) CreateMcpConnector(name, command string, args, envRefs []string) (*McpConnector, error) {
	name = strings.TrimSpace(name)
	command = strings.TrimSpace(command)
	if name == "" || command == "" {
		return nil, fmt.Errorf("name and command are required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureCatalog()
	c := &McpConnector{
		ID:        id("mcp_"),
		Name:      name,
		Command:   command,
		Args:      append([]string(nil), args...),
		EnvRefs:   append([]string(nil), envRefs...),
		CreatedAt: now(),
	}
	a.McpConnectors[c.ID] = c
	_ = a.Store.WriteJSON("mcp/"+c.ID+".json", c)
	cp := *c
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
	preset := strings.TrimSpace(input.PermissionPreset)
	if preset == "" {
		preset = PermissionWorkspaceWrite
	}
	if preset != PermissionWorkspaceWrite && preset != PermissionDangerFullAccess {
		return nil, fmt.Errorf("permissionPreset must be workspace-write or danger-full-access")
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

func (a *App) persistActivity(roomID string, item ActivityEvent) {
	a.ensureCatalog()
	_ = a.Store.AppendJSONL("audit/"+roomID+".jsonl", item)
}

func (a *App) persistRoom(room *Room) {
	a.ensureCatalog()
	_ = a.Store.WriteJSON("rooms/"+room.ID+".json", room)
}

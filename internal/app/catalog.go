package app

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

var (
	envNamePattern    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	headerNamePattern = regexp.MustCompile(`^[!#$%&'*+\-.^_` + "`" + `|~0-9A-Za-z]+$`)
)

type Persona struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Instructions    string   `json:"instructions"`
	McpConnectorIDs []string `json:"mcpConnectorIds,omitempty"`
	CreatedAt       string   `json:"createdAt"`
}

// HeaderRef names a request header. Env is the worker environment variable
// that holds the value. The value is never stored.
type HeaderRef struct {
	Name string `json:"name"`
	Env  string `json:"env"`
}

type McpConnector struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Transport   string      `json:"transport"`
	Command     string      `json:"command"`
	Args        []string    `json:"args,omitempty"`
	EnvRefs     []string    `json:"envRefs,omitempty"`
	URL         string      `json:"url,omitempty"`
	HeaderRefs  []HeaderRef `json:"headerRefs,omitempty"`
	DefaultOpen bool        `json:"defaultOpen"`
	CreatedAt   string      `json:"createdAt"`
}

// McpConnectorInput is the create request. Transport defaults to stdio.
type McpConnectorInput struct {
	Name        string
	Transport   string
	Command     string
	Args        []string
	EnvRefs     []string
	URL         string
	HeaderRefs  []HeaderRef
	DefaultOpen bool
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

func cleanEnvNames(in []string) ([]string, error) {
	out, err := cleanNames(in)
	if err != nil {
		return nil, err
	}
	for _, item := range out {
		if !envNamePattern.MatchString(item) {
			return nil, fmt.Errorf("name must not include a value")
		}
	}
	return out, nil
}

func cleanHeaderRefs(in []HeaderRef) ([]HeaderRef, []string, error) {
	out := make([]HeaderRef, 0, len(in))
	encoded := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, item := range in {
		name := strings.TrimSpace(item.Name)
		env := strings.TrimSpace(item.Env)
		if name == "" && env == "" {
			continue
		}
		if !headerNamePattern.MatchString(name) || !envNamePattern.MatchString(env) {
			return nil, nil, fmt.Errorf("header ref must be a header name and an environment name")
		}
		folded := strings.ToLower(name)
		if _, ok := seen[folded]; ok {
			return nil, nil, fmt.Errorf("duplicate header name")
		}
		seen[folded] = struct{}{}
		out = append(out, HeaderRef{Name: name, Env: env})
		encoded = append(encoded, name+":"+env)
	}
	return out, encoded, nil
}

func decodeHeaderRefs(encoded []string) []HeaderRef {
	out := make([]HeaderRef, 0, len(encoded))
	for _, item := range encoded {
		name, env, ok := strings.Cut(item, ":")
		if !ok || name == "" || env == "" {
			continue
		}
		out = append(out, HeaderRef{Name: name, Env: env})
	}
	return out
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
	transport := rec.Transport
	if transport == "" {
		transport = "stdio"
	}
	return &McpConnector{
		ID:          rec.ID,
		Name:        rec.Name,
		Transport:   transport,
		Command:     rec.Command,
		Args:        append([]string(nil), rec.Args...),
		EnvRefs:     append([]string(nil), rec.EnvRefs...),
		URL:         rec.URL,
		HeaderRefs:  decodeHeaderRefs(rec.HeaderRefs),
		DefaultOpen: rec.DefaultOpen,
		CreatedAt:   stamp(rec.CreatedAt),
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

func (a *App) CreateMcpConnector(ctx context.Context, tenantID string, in McpConnectorInput) (*McpConnector, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	transport := strings.TrimSpace(in.Transport)
	if transport == "" {
		transport = "stdio"
	}
	if transport != "stdio" && transport != "streamable_http" {
		return nil, fmt.Errorf("transport must be stdio or streamable_http")
	}
	refs, err := cleanEnvNames(in.EnvRefs)
	if err != nil {
		return nil, err
	}
	headers, encoded, err := cleanHeaderRefs(in.HeaderRefs)
	if err != nil {
		return nil, err
	}
	var command, rawURL string
	var cleanArgs []string
	if transport == "stdio" {
		command = strings.TrimSpace(in.Command)
		if command == "" {
			return nil, fmt.Errorf("name and command are required")
		}
		cleanArgs = trimNonEmpty(in.Args)
	} else {
		rawURL, err = validateMCPHTTPURL(in.URL)
		if err != nil {
			return nil, err
		}
	}
	created := time.Now().UTC()
	c := &McpConnector{
		ID:          id("mcp_"),
		Name:        name,
		Transport:   transport,
		Command:     command,
		Args:        cleanArgs,
		EnvRefs:     refs,
		URL:         rawURL,
		HeaderRefs:  headers,
		DefaultOpen: in.DefaultOpen,
		CreatedAt:   stamp(created),
	}
	if err := a.Repo.CreateMcpConnector(ctx, tenantID, store.McpConnectorRecord{
		ID: c.ID, Name: c.Name, Transport: c.Transport, Command: c.Command, Args: cleanArgs,
		EnvRefs: refs, URL: rawURL, HeaderRefs: encoded, DefaultOpen: in.DefaultOpen, CreatedAt: created,
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

// ConnectorsForRoom is the set the worker should connect: connectors marked
// default-open, plus any connector linked from the persona. Secret values
// are not included.
func (a *App) ConnectorsForRoom(ctx context.Context, tenantID, personaID string) ([]*McpConnector, error) {
	all, err := a.ListMcpConnectors(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	linked := map[string]struct{}{}
	if personaID != "" {
		personas, err := a.ListPersonas(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		var found *Persona
		for _, persona := range personas {
			if persona.ID == personaID {
				found = persona
				break
			}
		}
		if found == nil {
			return nil, fmt.Errorf("persona not found")
		}
		for _, mcpID := range found.McpConnectorIDs {
			linked[mcpID] = struct{}{}
		}
	}
	out := make([]*McpConnector, 0)
	for _, connector := range all {
		_, ok := linked[connector.ID]
		if connector.DefaultOpen || ok {
			cp := *connector
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
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

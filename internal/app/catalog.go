package app

import (
	"context"
	"fmt"
	"regexp"
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
	for _, rec := range recs {
		out = append(out, personaFromRecord(rec))
	}
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
	return p, nil
}

func (a *App) ListMcpConnectors(ctx context.Context, tenantID string) ([]*McpConnector, error) {
	recs, err := a.Repo.ListMcpConnectors(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]*McpConnector, 0, len(recs))
	for _, rec := range recs {
		out = append(out, connectorFromRecord(rec))
	}
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
	return c, nil
}

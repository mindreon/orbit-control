package app

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// A custom expert is a profile whose spec the worker reads (orbit_worker.agent_config): instructions, a model name
// and a snapshot of the MCP connectors it uses. Every version is one immutable profile row, so a task that names
// "<expert>@<n>" runs the same configuration for as long as it lives.

const (
	expertKind            = "expert"
	expertIDPrefix        = "expert_"
	maxExpertName         = 100
	maxExpertInstructions = 20000 // what the worker keeps; a longer text would be cut there
	maxExpertConnectors   = 20
)

var expertModelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$`)

type ExpertInput struct {
	Name         string
	Instructions string
	Model        string
	ConnectorIDs []string
	SkillIDs     []string
	// Source says where an expert came from ("agent:<id>", "persona:<id>"); empty for one written by hand.
	Source string
}

type Expert struct {
	ID           string   `json:"expert_id"`
	Ref          string   `json:"ref"`
	Version      int      `json:"version"`
	Name         string   `json:"name"`
	Instructions string   `json:"instructions"`
	Model        string   `json:"model"`
	ConnectorIDs []string `json:"connector_ids"`
	SkillIDs     []string `json:"skill_ids"`
	Source       string   `json:"source,omitempty"`
	CreatedAt    string   `json:"created_at"`
}

func (in ExpertInput) validate() (ExpertInput, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.Instructions = strings.TrimSpace(in.Instructions)
	in.Model = strings.TrimSpace(in.Model)
	switch {
	case in.Name == "" || len([]rune(in.Name)) > maxExpertName:
		return in, invalidf("name must be 1 to %d characters", maxExpertName)
	case len([]rune(in.Instructions)) > maxExpertInstructions:
		return in, invalidf("instructions are over %d characters", maxExpertInstructions)
	case in.Model != "" && !expertModelPattern.MatchString(in.Model):
		return in, invalidf("model is not a model name")
	case len(in.ConnectorIDs) > maxExpertConnectors:
		return in, invalidf("more than %d connectors", maxExpertConnectors)
	}
	if err := checkRefList("skills", in.SkillIDs); err != nil {
		return in, err
	}
	seen := make(map[string]struct{}, len(in.ConnectorIDs))
	for _, connectorID := range in.ConnectorIDs {
		if _, dup := seen[connectorID]; dup {
			return in, invalidf("a connector is listed twice")
		}
		seen[connectorID] = struct{}{}
	}
	return in, nil
}

func (a *App) CreateExpert(ctx context.Context, p taskruntime.Principal, in ExpertInput) (Expert, error) {
	return a.writeExpert(ctx, p, id(expertIDPrefix), 1, in)
}

// UpdateExpert stores in as the next version. Earlier versions never change. When another update takes that version
// first, the caller gets taskruntime.ErrIdempotencyConflict and can read and retry.
func (a *App) UpdateExpert(ctx context.Context, p taskruntime.Principal, expertID string, in ExpertInput) (Expert, error) {
	latest, err := a.GetExpert(ctx, p, expertID)
	if err != nil {
		return Expert{}, err
	}
	return a.writeExpert(ctx, p, expertID, latest.Version+1, in)
}

func (a *App) writeExpert(ctx context.Context, p taskruntime.Principal, expertID string, version int, in ExpertInput) (Expert, error) {
	in, err := in.validate()
	if err != nil {
		return Expert{}, err
	}
	snapshots, err := a.connectorSnapshots(ctx, p.TenantID, in.ConnectorIDs)
	if err != nil {
		return Expert{}, err
	}
	for _, skillID := range in.SkillIDs {
		if err := a.checkSkill(ctx, p.TenantID, skillID); err != nil {
			return Expert{}, err
		}
	}
	spec := map[string]any{
		"kind":           expertKind,
		"name":           in.Name,
		"instructions":   in.Instructions,
		"connector_ids":  nonNil(in.ConnectorIDs),
		"mcp_connectors": snapshots,
		"skills":         nonNil(in.SkillIDs),
	}
	if in.Model != "" {
		spec["model"] = in.Model
	}
	if in.Source != "" {
		spec["source"] = in.Source
	}
	profile, err := a.Tasks.RegisterProfile(ctx, p, taskruntime.Profile{ProfileID: expertID, Version: version, Spec: spec})
	if err != nil {
		return Expert{}, err
	}
	return expertFrom(profile), nil
}

// GetExpert reads the latest version. A profile that is not an expert is not found here.
func (a *App) GetExpert(ctx context.Context, p taskruntime.Principal, expertID string) (Expert, error) {
	experts, err := a.latestExperts(ctx, p)
	if err != nil {
		return Expert{}, err
	}
	for _, expert := range experts {
		if expert.ID == expertID {
			return expert, nil
		}
	}
	return Expert{}, store.ErrNotFound
}

// ListExperts is the latest version of each expert, newest id last.
func (a *App) ListExperts(ctx context.Context, p taskruntime.Principal) ([]Expert, error) {
	return a.latestExperts(ctx, p)
}

func (a *App) latestExperts(ctx context.Context, p taskruntime.Principal) ([]Expert, error) {
	profiles, err := a.Tasks.ListProfiles(ctx, p)
	if err != nil {
		return nil, err
	}
	latest := map[string]taskruntime.Profile{}
	for _, profile := range profiles {
		if profile.Spec["kind"] != expertKind {
			continue
		}
		if current, ok := latest[profile.ProfileID]; !ok || profile.Version > current.Version {
			latest[profile.ProfileID] = profile
		}
	}
	experts := make([]Expert, 0, len(latest))
	for _, profile := range latest {
		experts = append(experts, expertFrom(profile))
	}
	sortExperts(experts)
	return experts, nil
}

// connectorSnapshots resolves the tenant's own connectors to what the worker connects with: names and launch
// targets, never secret values. An id that is unknown and an id of another tenant fail alike.
func (a *App) connectorSnapshots(ctx context.Context, tenantID string, ids []string) ([]map[string]any, error) {
	snapshots := make([]map[string]any, 0, len(ids))
	if len(ids) == 0 {
		return snapshots, nil
	}
	records, err := a.Repo.ListMcpConnectors(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]store.McpConnectorRecord, len(records))
	for _, rec := range records {
		byID[rec.ID] = rec
	}
	for _, connectorID := range ids {
		rec, ok := byID[connectorID]
		if !ok {
			return nil, invalidf("unknown connector")
		}
		snapshots = append(snapshots, connectorSnapshot(rec))
	}
	return snapshots, nil
}

func connectorSnapshot(rec store.McpConnectorRecord) map[string]any {
	transport := rec.Transport
	if transport == "" {
		transport = "stdio"
	}
	headers := make([]map[string]string, 0, len(rec.HeaderRefs))
	for _, ref := range decodeHeaderRefs(rec.HeaderRefs) {
		headers = append(headers, map[string]string{"name": ref.Name, "env": ref.Env})
	}
	return map[string]any{
		"id":          rec.ID,
		"name":        rec.Name,
		"transport":   transport,
		"command":     rec.Command,
		"args":        nonNil(rec.Args),
		"env_refs":    nonNil(rec.EnvRefs),
		"url":         rec.URL,
		"header_refs": headers,
	}
}

func expertFrom(profile taskruntime.Profile) Expert {
	text := func(key string) string {
		value, _ := profile.Spec[key].(string)
		return value
	}
	list := func(key string) []string {
		var out []string
		if raw, ok := profile.Spec[key].([]any); ok {
			for _, item := range raw {
				out = append(out, fmt.Sprint(item))
			}
		}
		return nonNil(out)
	}
	return Expert{
		ID: profile.ProfileID, Ref: profile.Ref, Version: profile.Version, Name: text("name"),
		Instructions: text("instructions"), Model: text("model"), ConnectorIDs: list("connector_ids"), SkillIDs: list("skills"), Source: text("source"),
		CreatedAt: stamp(profile.CreatedAt),
	}
}

func nonNil(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func sortExperts(experts []Expert) {
	// Ids are UUIDv7 under a fixed prefix, so id order is creation order.
	sort.Slice(experts, func(i, j int) bool { return experts[i].ID < experts[j].ID })
}

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
	teamKind              = "team"
	expertIDPrefix        = "expert_"
	maxExpertName         = 100
	maxExpertInstructions = 20000 // what the worker keeps; a longer text would be cut there
	maxExpertConnectors   = 20
	minTeamMembers        = 1
	maxTeamMembers        = 8   // what TaskConfig.team accepts
	maxTeamDescription    = 300 // likewise
	maxTeamLabel          = 40  // likewise
)

// teamRolePattern is the role a team names its members by; the contract's TeamMember.role has the same shape.
var teamRolePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

var expertModelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$`)

type ExpertInput struct {
	Name         string
	Instructions string
	Model        string
	ConnectorIDs []string
	SkillIDs     []string
	// Source says where an expert came from ("agent:<id>", "persona:<id>"); empty for one written by hand.
	Source string
	// Kind is "expert" (also when empty) or "team". A team is a leader and members of other experts (15 M8, T8.6) and
	// has no instructions, model, connectors or skills of its own.
	Kind    string
	Leader  string
	Members []TeamMemberInput
}

// TeamMemberInput is one member as a caller names it: a role and the single expert, as a versioned reference, who
// does the work given to that role.
type TeamMemberInput struct {
	Role        string
	Expert      string
	Description string
	// Label is what a person sees for the role ("研究员"); optional.
	Label string
}

// TeamMember is a member as it is stored and read: Name is the member expert's name at the version pinned by Expert.
type TeamMember struct {
	Role        string `json:"role"`
	Expert      string `json:"expert"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Label       string `json:"label,omitempty"`
}

type Expert struct {
	ID           string   `json:"expert_id"`
	Kind         string   `json:"kind"`
	Ref          string   `json:"ref"`
	Version      int      `json:"version"`
	Name         string   `json:"name"`
	Instructions string   `json:"instructions"`
	Model        string   `json:"model"`
	ConnectorIDs []string `json:"connector_ids"`
	SkillIDs     []string `json:"skill_ids"`
	Source       string   `json:"source,omitempty"`
	// Leader and Members are set on a team only.
	Leader    string       `json:"leader,omitempty"`
	Members   []TeamMember `json:"members,omitempty"`
	CreatedAt string       `json:"created_at"`
}

func (in ExpertInput) validate() (ExpertInput, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.Instructions = strings.TrimSpace(in.Instructions)
	in.Model = strings.TrimSpace(in.Model)
	switch in.Kind {
	case "":
		in.Kind = expertKind
	case expertKind, teamKind:
	default:
		return in, invalidField("KIND_INVALID", "kind", "kind must be expert or team")
	}
	if in.Name == "" || len([]rune(in.Name)) > maxExpertName {
		return in, invalidField("NAME_INVALID", "name", fmt.Sprintf("name must be 1 to %d characters", maxExpertName))
	}
	if in.Kind == teamKind {
		return in.validateTeam()
	}
	switch {
	case in.Leader != "":
		return in, invalidField("FIELD_NOT_ALLOWED", "leader", "leader belongs to a team")
	case len(in.Members) > 0:
		return in, invalidField("FIELD_NOT_ALLOWED", "members", "members belong to a team")
	case len([]rune(in.Instructions)) > maxExpertInstructions:
		return in, invalidField("INSTRUCTIONS_TOO_LONG", "instructions", fmt.Sprintf("instructions are over %d characters", maxExpertInstructions))
	case in.Model != "" && !expertModelPattern.MatchString(in.Model):
		return in, invalidField("MODEL_INVALID", "model", "model is not a model name")
	case len(in.ConnectorIDs) > maxExpertConnectors:
		return in, invalidField("CONNECTORS_INVALID", "connector_ids", fmt.Sprintf("more than %d connectors", maxExpertConnectors))
	}
	if err := checkRefList("skill_ids", "SKILLS_INVALID", in.SkillIDs); err != nil {
		return in, err
	}
	seen := make(map[string]struct{}, len(in.ConnectorIDs))
	for i, connectorID := range in.ConnectorIDs {
		if _, dup := seen[connectorID]; dup {
			return in, invalidField("CONNECTORS_INVALID", fmt.Sprintf("connector_ids[%d]", i), "a connector is listed twice")
		}
		seen[connectorID] = struct{}{}
	}
	return in, nil
}

// validateTeam checks what needs no lookup: a team is 1 to 8 members with distinct roles, one of them the leader, and it
// carries nothing a worker would have to read, because the worker only ever runs the members' own experts.
func (in ExpertInput) validateTeam() (ExpertInput, error) {
	in.Leader = strings.TrimSpace(in.Leader)
	notAllowed := []struct {
		field string
		set   bool
	}{{"instructions", in.Instructions != ""}, {"model", in.Model != ""}, {"connector_ids", len(in.ConnectorIDs) > 0}, {"skill_ids", len(in.SkillIDs) > 0}}
	for _, f := range notAllowed {
		if f.set {
			return in, invalidField("FIELD_NOT_ALLOWED", f.field, "a team has no "+f.field+" of its own; its members have them")
		}
	}
	switch {
	case len(in.Members) < minTeamMembers:
		return in, invalidField("TEAM_NO_MEMBERS", "members", "a team needs at least one member")
	case len(in.Members) > maxTeamMembers:
		return in, invalidField("TEAM_TOO_MANY_MEMBERS", "members", fmt.Sprintf("a team has at most %d members", maxTeamMembers))
	}
	members := make([]TeamMemberInput, len(in.Members))
	roles := make(map[string]struct{}, len(in.Members))
	for i, member := range in.Members {
		member.Role = strings.TrimSpace(member.Role)
		member.Expert = strings.TrimSpace(member.Expert)
		member.Description = strings.TrimSpace(member.Description)
		member.Label = strings.TrimSpace(member.Label)
		at := func(name string) string { return fmt.Sprintf("members[%d].%s", i, name) }
		switch {
		case !teamRolePattern.MatchString(member.Role):
			return in, invalidField("ROLE_INVALID", at("role"), "a role is a lowercase name of 1 to 32 characters: letters, digits, - and _")
		case !versionedRef.MatchString(member.Expert):
			return in, invalidField("TEAM_MEMBER_REF_INVALID", at("expert"), "a member's expert is not a versioned reference (id@version)")
		case len([]rune(member.Description)) > maxTeamDescription:
			return in, invalidField("DESCRIPTION_TOO_LONG", at("description"), fmt.Sprintf("a member's description is over %d characters", maxTeamDescription))
		case len([]rune(member.Label)) > maxTeamLabel:
			return in, invalidField("LABEL_TOO_LONG", at("label"), fmt.Sprintf("a member's label is over %d characters", maxTeamLabel))
		}
		if _, dup := roles[member.Role]; dup {
			return in, invalidField("TEAM_DUPLICATE_ROLE", at("role"), "a role is listed twice")
		}
		roles[member.Role] = struct{}{}
		members[i] = member
	}
	if _, ok := roles[in.Leader]; !ok {
		return in, invalidField("TEAM_LEADER_NOT_MEMBER", "leader", "the leader must be the role of a member")
	}
	in.Members = members
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
	// A version is of the same kind as the ones before it: a team never turns into a single expert or back.
	if in.Kind == "" {
		in.Kind = latest.Kind
	} else if in.Kind != latest.Kind {
		return Expert{}, invalidField("KIND_IMMUTABLE", "kind", "an expert cannot change its kind: a team stays a team and a single expert stays single")
	}
	return a.writeExpert(ctx, p, expertID, latest.Version+1, in)
}

func (a *App) writeExpert(ctx context.Context, p taskruntime.Principal, expertID string, version int, in ExpertInput) (Expert, error) {
	in, err := in.validate()
	if err != nil {
		return Expert{}, err
	}
	if in.Kind == teamKind {
		return a.writeTeam(ctx, p, expertID, version, in)
	}
	snapshots, err := a.connectorSnapshots(ctx, p.TenantID, in.ConnectorIDs)
	if err != nil {
		return Expert{}, err
	}
	for i, skillID := range in.SkillIDs {
		if err := a.checkSkill(ctx, p.TenantID, skillID, fmt.Sprintf("skill_ids[%d]", i)); err != nil {
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
		if kind := profile.Spec["kind"]; kind != expertKind && kind != teamKind {
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
	for i, connectorID := range ids {
		rec, ok := byID[connectorID]
		if !ok {
			return nil, invalidField("CONNECTOR_NOT_FOUND", fmt.Sprintf("connector_ids[%d]", i), "unknown connector")
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
	expert := Expert{
		ID: profile.ProfileID, Kind: text("kind"), Ref: profile.Ref, Version: profile.Version, Name: text("name"),
		Instructions: text("instructions"), Model: text("model"), ConnectorIDs: list("connector_ids"), SkillIDs: list("skills"), Source: text("source"),
		CreatedAt: stamp(profile.CreatedAt),
	}
	if expert.Kind == teamKind {
		// A malformed spec (only /v1/profiles can write one) reads as a team without members.
		expert.Leader, expert.Members, _ = teamFromSpec(profile.Spec)
	}
	return expert
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

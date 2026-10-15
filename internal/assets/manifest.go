package assets

import (
	"encoding/json/v2"
	"fmt"
	"maps"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/contract"
)

var secretReference = regexp.MustCompile(`\$\{secret:([A-Za-z_][A-Za-z0-9_]*)\}`)

type Dependency struct {
	Kind       contract.AssetKind `json:"kind"`
	Ref        string             `json:"ref"`
	Constraint string             `json:"constraint"`
}
type Manifest struct {
	Agent             *contract.AgentManifest                  `json:"agent,omitempty"`
	Team              *contract.TeamManifest                   `json:"team,omitempty"`
	Skills            *contract.SkillsManifest                 `json:"skills,omitempty"`
	MCP               *contract.McpManifest                    `json:"mcp,omitempty"`
	MemberPermissions map[string]contract.EffectivePermissions `json:"member_permissions,omitempty"`
	Dependencies      []Dependency                             `json:"dependencies,omitempty"`
}

func SplitRef(ref string) (string, string) {
	name, c, ok := strings.Cut(ref, "@")
	if !ok || c == "" {
		c = "*"
	}
	return name, c
}

// ParseManifest only validates contract schemas and extracts references. Semantic
// checks (including missing files, unsafe instructions and credentials) are runtime's responsibility.
func ParseManifest(files []bundle.File) (Manifest, contract.EffectivePermissions, error) {
	var m Manifest
	p := contract.EffectivePermissions{SandboxProfile: "default"}
	if err := m.decode(files); err != nil {
		return m, p, err
	}
	if m.Agent != nil {
		p = AgentPermissions(m.Agent)
	}
	add := func(kind contract.AssetKind, ref, c string) {
		if c == "" {
			c = "*"
		}
		m.Dependencies = append(m.Dependencies, Dependency{Kind: kind, Ref: ref, Constraint: c})
	}
	if m.Skills != nil {
		for _, s := range m.Skills.Skills {
			if s.Source == "catalog" && s.Id != nil {
				c := "*"
				if s.Version != nil {
					c = *s.Version
				}
				add("skill", *s.Id, c)
			}
		}
	}
	if err := m.addMCPPermissions(&p, add); err != nil {
		return m, p, err
	}
	if m.Team != nil {
		for _, r := range m.Team.Roles {
			if r.Agent != "." && !strings.HasPrefix(r.Agent, "./") {
				ref, c := SplitRef(r.Agent)
				add("agent", ref, c)
			}
		}
		for _, s := range m.Team.SharedSkills {
			c := "*"
			if s.Version != nil {
				c = *s.Version
			}
			add("skill", s.Id, c)
		}
	}
	if m.Team != nil {
		m.MemberPermissions = map[string]contract.EffectivePermissions{".": p}
		for _, f := range files {
			if !strings.HasPrefix(f.Path, "members/") || !strings.HasSuffix(f.Path, "/agent.json") {
				continue
			}
			if err := contract.Validate("AgentManifest", f.Data); err != nil {
				return m, p, fmt.Errorf("%s: %w", f.Path, err)
			}
			var agent contract.AgentManifest
			if err := json.Unmarshal(f.Data, &agent); err != nil {
				return m, p, err
			}
			permissions := AgentPermissions(&agent)
			m.MemberPermissions[path.Dir(f.Path)] = permissions
			p = UnionPermissions(p, permissions)
		}
	}
	slices.SortFunc(m.Dependencies, func(a, b Dependency) int {
		return strings.Compare(string(a.Kind)+a.Ref+a.Constraint, string(b.Kind)+b.Ref+b.Constraint)
	})
	return m, p, nil
}

func AgentPermissions(a *contract.AgentManifest) contract.EffectivePermissions {
	p := contract.EffectivePermissions{SandboxProfile: "default"}

	if a.Tools != nil {
		p.ToolsBuiltin = a.Tools.Builtin
	}
	if a.Permissions != nil {
		v := a.Permissions
		p.DataScopes = v.DataScopes
		p.EffectClasses = v.EffectClasses
		if v.Network != nil {
			p.Network.Egress = v.Network.Egress
		}
		if v.Sandbox != nil && v.Sandbox.Profile != nil {
			p.SandboxProfile = *v.Sandbox.Profile
		}
		for _, s := range v.Secrets {
			p.Secrets = append(p.Secrets, contract.SecretRequirement{Name: s.Name})
		}
	}
	return p
}

func (m *Manifest) addMCPPermissions(p *contract.EffectivePermissions, add func(contract.AssetKind, string, string)) error {
	if m.MCP != nil {
		for _, name := range slices.Sorted(maps.Keys(m.MCP.McpServers)) {
			s := m.MCP.McpServers[name]
			for _, tool := range slices.Sorted(maps.Keys(s.Tools)) {
				effect := s.Tools[tool].EffectClass
				if !slices.Contains(p.EffectClasses, effect) {
					p.EffectClasses = append(p.EffectClasses, effect)
				}
			}
			if s.Url != nil {
				if u, err := url.Parse(*s.Url); err == nil && u.Hostname() != "" &&
					!strings.Contains(u.Hostname(), "${") &&
					!slices.Contains(p.Network.Egress, u.Hostname()) {
					p.Network.Egress = append(p.Network.Egress, u.Hostname())
				}
			}
			raw, err := json.Marshal(s)
			if err != nil {
				return err
			}
			for _, match := range secretReference.FindAllSubmatch(raw, -1) {
				requirement := contract.SecretRequirement{Name: string(match[1])}
				if !slices.Contains(p.Secrets, requirement) {
					p.Secrets = append(p.Secrets, requirement)
				}
			}
			if s.Ref != nil {
				ref, c := SplitRef(*s.Ref)
				add("mcp_server", ref, c)
			}
		}
	}
	return nil
}

func (m *Manifest) decode(files []bundle.File) error {
	for _, f := range files {
		var typ string
		var dst any
		switch f.Path {
		case "agent.json":
			m.Agent = new(contract.AgentManifest)
			typ = "AgentManifest"
			dst = m.Agent
		case "team.json":
			m.Team = new(contract.TeamManifest)
			typ = "TeamManifest"
			dst = m.Team
		case "skills.json":
			m.Skills = new(contract.SkillsManifest)
			typ = "SkillsManifest"
			dst = m.Skills
		case "mcp.json":
			m.MCP = new(contract.McpManifest)
			typ = "McpManifest"
			dst = m.MCP
		default:
			continue
		}
		if err := contract.Validate(typ, f.Data); err != nil {
			return fmt.Errorf("%s: %w", f.Path, err)
		}
		if err := json.Unmarshal(f.Data, dst); err != nil {
			return fmt.Errorf("invalid %s", f.Path)
		}
	}
	return nil
}

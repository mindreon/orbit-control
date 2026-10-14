package locks

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

type Entry struct {
	Kind    contract.EntryKind `json:"kind"`
	Ref     string             `json:"ref"`
	Version string             `json:"version,omitempty"`
}
type Scope struct {
	WorkspaceID string
	RunID       string
}
type Resolver struct{ Assets *assets.Service }
type node struct {
	asset   assets.Asset
	version assets.Version
	index   int
	prefix  string
}
type resolution struct {
	r          *Resolver
	ctx        context.Context
	scope      Scope
	lock       contract.RunLock
	nodes      map[string]node
	visiting   map[string]bool
	visibility []Visibility
	caps       []contract.EffectivePermissions
	installed  []node
	shared     []node
	team       *node
}

func (r *Resolver) Resolve(ctx context.Context, tenant string, entry Entry, scope Scope) (contract.RunLock, error) {
	if tenant != auth.Tenant(ctx) {
		return contract.RunLock{}, fmt.Errorf("lock tenant mismatch")
	}
	if entry.Kind != "agent" && entry.Kind != "team" {
		return contract.RunLock{}, assets.Invalid("Entry must be agent or team")
	}
	x := &resolution{r: r, ctx: ctx, scope: scope, nodes: map[string]node{}, visiting: map[string]bool{}, lock: contract.RunLock{V: 1, LockId: ids.New()}}
	if err := x.policies(); err != nil {
		return x.lock, err
	}
	root, err := x.resolve(contract.AssetKind(entry.Kind), entry.Ref, entry.Version)
	if err != nil {
		return x.lock, err
	}
	x.lock.Entry = contract.LockEntryRef{Kind: entry.Kind, Ref: assets.Ref(root.asset), Version: root.version.Version}
	// Installed skills form the inherited skill library. References still undergo
	// the same signature, availability, visibility and constraint checks.
	rows, err := db.Tx(ctx).Query(ctx, `SELECT a.publisher||'/'||a.name FROM asset_installs i JOIN assets a ON a.id=i.asset_id WHERE i.enabled AND a.kind='skill' AND ((i.scope_type='tenant' AND i.scope_id=$1) OR (i.scope_type='workspace' AND i.scope_id=NULLIF($2,'')::uuid)) ORDER BY a.publisher,a.name`, tenant, scope.WorkspaceID)
	if err != nil {
		return x.lock, err
	}
	refs := []string{}
	for rows.Next() {
		var ref string
		if err = rows.Scan(&ref); err != nil {
			rows.Close()
			return x.lock, err
		}
		refs = append(refs, ref)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return x.lock, err
	}
	for _, ref := range refs {
		n, e := x.resolve("skill", ref, "*")
		if e != nil {
			return x.lock, e
		}
		x.installed = append(x.installed, n)
	}
	if entry.Kind == "agent" {
		m, e := x.member(root, "main", "agent")
		if e != nil {
			return x.lock, e
		}
		x.lock.Members = append(x.lock.Members, m)
	} else {
		if root.version.Manifest.Team == nil {
			return x.lock, assets.Invalid("Team manifest missing")
		}
		x.team = &root
		for _, skill := range root.version.Manifest.Team.SharedSkills {
			c := "*"
			if skill.Version != nil {
				c = *skill.Version
			}
			n, e := x.resolve("skill", skill.Id, c)
			if e != nil {
				return x.lock, e
			}
			x.shared = append(x.shared, n)
		}
		for _, role := range root.version.Manifest.Team.Roles {
			var n node
			if assets.IsLocal(role.Agent) {
				n, err = x.embedded(root, strings.TrimPrefix(role.Agent, "./"), "agent")
			} else {
				ref, c := assets.SplitRef(role.Agent)
				n, err = x.resolve("agent", ref, c)
			}
			if err != nil {
				return x.lock, err
			}
			count := 1
			if role.Count != nil {
				count = *role.Count
			}
			if count > 100 {
				return x.lock, assets.Invalid("Team roster too large")
			}
			for i := range count {
				name := role.Name
				if count > 1 {
					name = fmt.Sprintf("%s-%d", name, i+1)
				}
				m, e := x.member(n, name, contract.MemberRole(role.Role))
				if e != nil {
					return x.lock, e
				}
				x.lock.Members = append(x.lock.Members, m)
			}
		}
	}
	if len(x.lock.Members) == 0 {
		return x.lock, assets.Invalid("Empty roster")
	}
	// Run permissions describe the union needed by members; each member retains
	// its own narrower cap. Both have already intersected tenant/workspace policy.
	x.lock.EffectivePermissions = x.lock.Members[0].EffectivePermissions
	for _, m := range x.lock.Members[1:] {
		x.lock.EffectivePermissions = union(x.lock.EffectivePermissions, m.EffectivePermissions)
	}
	raw, err := json.Marshal(x.lock)
	if err != nil {
		return x.lock, err
	}
	x.lock.Digest, err = contract.RunLockDigest(raw)
	if err != nil {
		return x.lock, err
	}
	raw, err = json.Marshal(x.lock)
	if err != nil {
		return x.lock, err
	}
	if err = contract.ValidateStrict("RunLock", raw); err != nil {
		return x.lock, err
	}
	if scope.RunID != "" {
		entries, e := json.Marshal(x.lock.Entries)
		if e != nil {
			return x.lock, e
		}
		p, e := json.Marshal(x.lock.EffectivePermissions)
		if e != nil {
			return x.lock, e
		}
		_, err = db.Tx(ctx).Exec(ctx, `INSERT INTO run_locks(id,tenant_id,run_id,digest,entries,effective_permissions,lock) VALUES($1,$2,$3,$4,$5,$6,$7)`, x.lock.LockId, tenant, scope.RunID, x.lock.Digest, entries, p, raw)
		if err == nil {
			err = assets.Log(ctx, "run.lock_created", "run_lock", x.lock.LockId, map[string]string{"digest": x.lock.Digest})
		}
	}
	return x.lock, err
}
func (x *resolution) policies() error {
	rows, err := db.Tx(x.ctx).Query(x.ctx, `SELECT body FROM policies WHERE status='active' AND kind='tool_rules' AND ((scope_type='tenant' AND scope_id=$1) OR (scope_type='workspace' AND scope_id=NULLIF($2,'')::uuid)) ORDER BY scope_type DESC,id`, auth.Tenant(x.ctx), x.scope.WorkspaceID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		var p struct {
			Permissions *contract.EffectivePermissions `json:"effective_permissions"`
			Visibility  Visibility                     `json:"skills_visibility"`
			Users       map[string]struct {
				Permissions *contract.EffectivePermissions `json:"effective_permissions"`
				Visibility  Visibility                     `json:"skills_visibility"`
			} `json:"users"`
		}
		if err = json.Unmarshal(raw, &p); err != nil {
			rows.Close()
			return err
		}
		x.visibility = append(x.visibility, p.Visibility)
		if p.Permissions != nil {
			x.caps = append(x.caps, *p.Permissions)
		}
		if user, ok := p.Users[auth.From(x.ctx).ID]; ok {
			x.visibility = append(x.visibility, user.Visibility)
			if user.Permissions != nil {
				x.caps = append(x.caps, *user.Permissions)
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if x.scope.WorkspaceID != "" {
		var raw []byte
		if err = db.Tx(x.ctx).QueryRow(x.ctx, `SELECT settings FROM workspaces WHERE id=$1`, x.scope.WorkspaceID).Scan(&raw); err != nil {
			return err
		}
		var settings struct {
			Visibility Visibility `json:"skills_visibility"`
		}
		if err = json.Unmarshal(raw, &settings); err != nil {
			return err
		}
		x.visibility = append(x.visibility, settings.Visibility)
	}
	return nil
}
func (x *resolution) resolve(kind contract.AssetKind, ref, constraint string) (node, error) {
	if len(x.nodes) > 512 {
		return node{}, assets.Invalid("Dependency graph too large")
	}
	if strings.Contains(ref, "@") {
		var c string
		ref, c = assets.SplitRef(ref)
		if constraint == "" {
			constraint = c
		}
	}
	if constraint == "" {
		constraint = "*"
	}
	key := string(kind) + ":" + ref
	if x.visiting[key] {
		return node{}, assets.Invalid("Asset dependency cycle")
	}
	c, err := semver.NewConstraint(constraint)
	if err != nil {
		return node{}, assets.Invalid("Invalid semantic version constraint")
	}
	if n, ok := x.nodes[key]; ok {
		v, e := semver.StrictNewVersion(n.version.Version)
		if e != nil || !c.Check(v) {
			return node{}, assets.Invalid("Conflicting dependency constraints")
		}
		return n, nil
	}
	var id string
	if err = db.Tx(x.ctx).QueryRow(x.ctx, `SELECT id::text FROM assets WHERE tenant_id=$1 AND kind=$2 AND publisher||'/'||name=$3`, auth.Tenant(x.ctx), kind, ref).Scan(&id); err != nil {
		return node{}, err
	}
	a, err := x.r.Assets.Get(x.ctx, id, "asset.use")
	if err != nil {
		return node{}, err
	}
	if a.Status != "active" {
		return node{}, assets.Invalid("Asset is unavailable")
	}
	var pinned, channel, installedConstraint string
	rows, err := db.Tx(x.ctx).Query(x.ctx, `SELECT COALESCE(pinned_version_id::text,''),COALESCE(channel,''),"constraint" FROM asset_installs WHERE asset_id=$1 AND enabled AND ((scope_type='tenant' AND scope_id=$2) OR (scope_type='workspace' AND scope_id=NULLIF($3,'')::uuid)) ORDER BY CASE scope_type WHEN 'workspace' THEN 0 ELSE 1 END LIMIT 1`, id, auth.Tenant(x.ctx), x.scope.WorkspaceID)
	if err != nil {
		return node{}, err
	}
	if rows.Next() {
		err = rows.Scan(&pinned, &channel, &installedConstraint)
	}
	re := rows.Err()
	rows.Close()
	if err != nil {
		return node{}, err
	}
	if re != nil {
		return node{}, re
	}
	ranges := []string{constraint}
	if installedConstraint != "" {
		ranges = append(ranges, installedConstraint)
	}
	v, err := x.r.Assets.SelectVersion(x.ctx, a, ranges, pinned, channel)
	if err != nil {
		return node{}, err
	}
	files, err := x.r.Assets.Files(x.ctx, v.ID)
	if err != nil {
		return node{}, err
	}
	hashes := map[string]string{}
	for _, f := range files {
		hashes[f.Path] = f.Sha256
	}
	digest, err := contract.BundleDigest(hashes)
	if err != nil || digest != v.Digest {
		return node{}, assets.Invalid("Asset file digest mismatch")
	}
	n := node{asset: a, version: v, index: len(x.lock.Entries)}
	x.lock.Entries = append(x.lock.Entries, contract.LockEntry{Kind: kind, Ref: ref, Version: v.Version, Digest: v.Digest, Files: files})
	x.nodes[key] = n
	x.visiting[key] = true
	for _, d := range v.Manifest.Dependencies {
		if _, err = x.resolve(d.Kind, d.Ref, d.Constraint); err != nil {
			return node{}, err
		}
	}
	delete(x.visiting, key)
	return n, nil
}
func (x *resolution) embedded(parent node, prefix string, kind contract.AssetKind) (node, error) {
	if prefix == "." {
		prefix = ""
	}
	if prefix != "" {
		if _, err := bundle.SafePath(prefix); err != nil {
			return node{}, err
		}
		prefix += "/"
	}
	ref := assets.Ref(parent.asset) + "/" + strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		ref += "root"
	}
	key := string(kind) + ":" + ref
	if n, ok := x.nodes[key]; ok {
		return n, nil
	}
	all, err := x.r.Assets.Files(x.ctx, parent.version.ID)
	if err != nil {
		return node{}, err
	}
	files := []contract.LockFile{}
	hashes := map[string]string{}
	content := []bundle.File{}
	for _, f := range all {
		if !strings.HasPrefix(f.Path, prefix) {
			continue
		}
		f.Path = strings.TrimPrefix(f.Path, prefix)
		if f.Path == "team.json" {
			continue
		}
		files = append(files, f)
		hashes[f.Path] = f.Sha256
		if slices.Contains([]string{"agent.json", "skills.json", "mcp.json"}, f.Path) {
			data, _, e := x.r.Assets.File(x.ctx, parent.version.ID, prefix+f.Path)
			if e != nil {
				return node{}, e
			}
			content = append(content, bundle.File{Path: f.Path, Data: data})
		}
	}
	if len(files) == 0 {
		return node{}, assets.Invalid("Embedded bundle missing")
	}
	digest, err := contract.BundleDigest(hashes)
	if err != nil {
		return node{}, err
	}
	m, p, err := assets.ParseManifest(content)
	if err != nil {
		return node{}, err
	}
	n := node{asset: parent.asset, version: parent.version, index: len(x.lock.Entries), prefix: prefix}
	n.version.Manifest = m
	n.version.Permissions = p
	n.version.Digest = digest
	x.lock.Entries = append(x.lock.Entries, contract.LockEntry{Kind: kind, Ref: ref, Version: parent.version.Version, Digest: digest, Files: files})
	x.nodes[key] = n
	for _, d := range m.Dependencies {
		if _, err = x.resolve(d.Kind, d.Ref, d.Constraint); err != nil {
			return node{}, err
		}
	}
	return n, nil
}
func (x *resolution) fileVisibility(n node, name string) (Visibility, error) {
	var v Visibility
	var exists bool
	err := db.Tx(x.ctx).QueryRow(x.ctx, `SELECT EXISTS(SELECT 1 FROM asset_files WHERE version_id=$1 AND path=$2)`, n.version.ID, n.prefix+name).Scan(&exists)
	if err != nil || !exists {
		return v, err
	}
	data, _, err := x.r.Assets.File(x.ctx, n.version.ID, n.prefix+name)
	if err != nil {
		return v, err
	}
	err = json.Unmarshal(data, &v)
	return v, err
}
func (x *resolution) member(n node, name string, role contract.MemberRole) (contract.LockMember, error) {
	m := contract.LockMember{Name: contract.MemberName(name), Role: role, AgentEntry: n.index, EffectivePermissions: n.version.Permissions}
	if n.version.Manifest.Agent != nil && n.version.Manifest.Agent.Description != nil {
		m.Description = *n.version.Manifest.Agent.Description
	}
	layers := slices.Clone(x.visibility)
	if x.team != nil {
		for _, filename := range []string{"skills-visibility.json", "members/" + name + "/skills-visibility.json"} {
			v, e := x.fileVisibility(*x.team, filename)
			if e != nil {
				return m, e
			}
			layers = append(layers, v)
		}
	}
	v, err := x.fileVisibility(n, "skills-visibility.json")
	if err != nil {
		return m, err
	}
	layers = append(layers, v)
	v, err = x.fileVisibility(n, "members/"+name+"/skills-visibility.json")
	if err != nil {
		return m, err
	}
	layers = append(layers, v)
	skills := append(slices.Clone(x.installed), x.shared...)
	for _, d := range n.version.Manifest.Dependencies {
		dep, err := x.resolve(d.Kind, d.Ref, d.Constraint)
		if err != nil {
			return m, err
		}
		if d.Kind == "skill" {
			skills = append(skills, dep)
		}
		if d.Kind == "mcp_server" {
			m.Mcp = append(m.Mcp, dep.index)
		}
	}
	// Bundle skills are represented as immutable derived entries under the signed
	// parent file set, including directories absent from skills.json.
	all, err := x.r.Assets.Files(x.ctx, n.version.ID)
	if err != nil {
		return m, err
	}
	embedded := []string{}
	for _, f := range all {
		rel := strings.TrimPrefix(f.Path, n.prefix)
		if strings.HasPrefix(f.Path, n.prefix) && strings.HasPrefix(rel, "skills/") && strings.HasSuffix(rel, "/SKILL.md") {
			dir := strings.TrimSuffix(rel, "/SKILL.md")
			if strings.Count(dir, "/") == 1 {
				embedded = append(embedded, dir)
			}
		}
	}
	slices.Sort(embedded)
	for _, dir := range embedded {
		child, err := x.embedded(n, n.prefix+dir, "skill")
		if err != nil {
			return m, err
		}
		skills = append(skills, child)
	}
	closure := []node{}
	seen := map[int]bool{}
	var visit func(node) error
	visit = func(n node) error {
		if seen[n.index] {
			return nil
		}
		seen[n.index] = true
		kind := x.lock.Entries[n.index].Kind
		if kind == "skill" {
			closure = append(closure, n)
		}
		if kind == "mcp_server" {
			m.Mcp = append(m.Mcp, n.index)
		}
		for _, d := range n.version.Manifest.Dependencies {
			child, e := x.resolve(d.Kind, d.Ref, d.Constraint)
			if e != nil {
				return e
			}
			if e = visit(child); e != nil {
				return e
			}
		}
		return nil
	}
	for _, skill := range skills {
		if err = visit(skill); err != nil {
			return m, err
		}
	}
	for _, skill := range closure {
		ref := x.lock.Entries[skill.index].Ref
		if Visible(ref, layers...) {
			m.Skills = append(m.Skills, skill.index)
		}
	}
	slices.Sort(m.Skills)
	m.Skills = slices.Compact(m.Skills)
	slices.Sort(m.Mcp)
	m.Mcp = slices.Compact(m.Mcp)
	for _, cap := range x.caps {
		m.EffectivePermissions = Intersect(m.EffectivePermissions, cap)
	}
	if m.EffectivePermissions.SandboxProfile == "" {
		return m, assets.Invalid("Sandbox profile denied by policy")
	}
	return m, nil
}
func union(a, b contract.EffectivePermissions) contract.EffectivePermissions {
	unique := func(v []string) []string { slices.Sort(v); return slices.Compact(v) }
	a.Network.Egress = unique(append(slices.Clone(a.Network.Egress), b.Network.Egress...))
	a.DataScopes = unique(append(slices.Clone(a.DataScopes), b.DataScopes...))
	a.ToolsBuiltin = unique(append(slices.Clone(a.ToolsBuiltin), b.ToolsBuiltin...))
	for _, s := range b.Secrets {
		if !slices.Contains(a.Secrets, s) {
			a.Secrets = append(a.Secrets, s)
		}
	}
	for _, e := range b.EffectClasses {
		if !slices.Contains(a.EffectClasses, e) {
			a.EffectClasses = append(a.EffectClasses, e)
		}
	}
	return a
}

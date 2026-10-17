package locks

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"regexp"
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
}
type Request struct {
	TenantID, LockID string
	Entry            Entry
	Scope            Scope
}
type Resolver interface {
	Resolve(context.Context, Request) (contract.RunLock, error)
}
type BundleResolver struct{ Assets *assets.Service }

func NewResolver(s *assets.Service) *BundleResolver { return &BundleResolver{Assets: s} }

var _ Resolver = (*BundleResolver)(nil)

type node struct {
	asset   assets.Asset
	version assets.Version
	index   int
	prefix  string
}
type resolution struct {
	r          *BundleResolver
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

func (r *BundleResolver) Resolve(ctx context.Context, req Request) (contract.RunLock, error) {
	tenant, entry, scope := req.TenantID, req.Entry, req.Scope
	if !ids.Valid(req.LockID) {
		return contract.RunLock{}, assets.Invalid("Caller must supply a lock UUID")
	}
	if tenant == "" || auth.From(ctx).ID == "" || tenant != auth.Tenant(ctx) {
		return contract.RunLock{}, fmt.Errorf("lock tenant mismatch")
	}
	if entry.Kind != "agent" && entry.Kind != "team" {
		return contract.RunLock{}, assets.Invalid("Entry must be agent or team")
	}
	x := &resolution{
		r:        r,
		ctx:      ctx,
		scope:    scope,
		nodes:    map[string]node{},
		visiting: map[string]bool{},
		lock:     contract.RunLock{V: 1, LockId: req.LockID},
	}
	if err := x.policies(); err != nil {
		return x.lock, err
	}
	root, err := x.resolve(contract.AssetKind(entry.Kind), entry.Ref, entry.Version)
	if err != nil {
		return x.lock, err
	}
	x.lock.Entry = contract.LockEntryRef{Kind: entry.Kind, Ref: assets.Ref(root.asset), Version: root.version.Version}
	if err := x.loadInstalled(); err != nil {
		return x.lock, err
	}
	if err := x.roster(root, entry.Kind); err != nil {
		return x.lock, err
	}
	return x.finish()
}
func (x *resolution) loadInstalled() error {
	// Installed skills form the inherited skill library. References still undergo
	// the same signature, availability, visibility and constraint checks.
	rows, err := db.Tx(x.ctx).
		Query(x.ctx, `SELECT a.publisher||'/'||a.name
 FROM asset_installs i
 JOIN assets a ON a.id=i.asset_id
 WHERE i.enabled
 AND a.kind='skill'
 AND ((i.scope_type='tenant' AND i.scope_id=$1) OR (i.scope_type='workspace' AND i.scope_id=NULLIF($2,'')::uuid))
 ORDER BY a.publisher,a.name`, auth.Tenant(x.ctx), x.scope.WorkspaceID)
	if err != nil {
		return err
	}
	refs := []string{}
	for rows.Next() {
		var ref string
		if err = rows.Scan(&ref); err != nil {
			rows.Close()
			return err
		}
		refs = append(refs, ref)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, ref := range refs {
		n, e := x.resolve("skill", ref, "*")
		if e != nil {
			return e
		}
		x.installed = append(x.installed, n)
	}
	return nil
}
func (x *resolution) roster(root node, kind contract.EntryKind) error {
	var err error
	if kind == "agent" {
		m, e := x.member(root, "main", "main", "agent")
		if e != nil {
			return e
		}
		x.lock.Members = append(x.lock.Members, m)
	} else {
		if root.version.Manifest.Team == nil {
			return assets.Invalid("Team manifest missing")
		}
		x.team = &root
		for _, skill := range root.version.Manifest.Team.SharedSkills {
			c := "*"
			if skill.Version != nil {
				c = *skill.Version
			}
			n, e := x.resolve("skill", skill.Id, c)
			if e != nil {
				return e
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
				return err
			}
			count := 1
			if role.Count != nil {
				count = *role.Count
			}
			if count > 100 {
				return assets.Invalid("Team roster too large")
			}
			for i := range count {
				name := role.Name
				if count > 1 {
					name = fmt.Sprintf("%s-%d", name, i+1)
				}
				m, e := x.member(n, name, role.Name, contract.MemberRole(role.Role))
				if e != nil {
					return e
				}
				x.lock.Members = append(x.lock.Members, m)
			}
		}
	}
	if kind == "team" {
		seen := map[string]bool{}
		leaders := 0
		for _, member := range x.lock.Members {
			name := string(member.Name)
			if seen[name] || name == "main" || name == "team" || !memberPattern.MatchString(name) {
				return assets.Invalid("Invalid expanded team member name")
			}
			seen[name] = true
			if member.Role == "leader" {
				leaders++
			}
		}
		if len(seen) > 16 || leaders != 1 {
			return assets.Invalid("Team requires one leader and at most 16 members")
		}
	}
	return nil
}

var memberPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

func (x *resolution) finish() (contract.RunLock, error) {
	if len(x.lock.Members) == 0 {
		return x.lock, assets.Invalid("Empty roster")
	}
	// Run permissions describe the union needed by members; each member retains
	// its own narrower cap. Both have already intersected tenant/workspace policy.
	x.lock.EffectivePermissions = x.lock.Members[0].EffectivePermissions
	for _, m := range x.lock.Members[1:] {
		x.lock.EffectivePermissions = assets.UnionPermissions(x.lock.EffectivePermissions, m.EffectivePermissions)
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
	return x.lock, nil
}

func (x *resolution) policies() error {
	rows, err := db.Tx(x.ctx).
		Query(x.ctx, `SELECT body
 FROM policies
 WHERE status='active'
 AND kind='tool_rules'
 AND ((scope_type='tenant' AND scope_id=$1) OR (scope_type='workspace' AND scope_id=NULLIF($2,'')::uuid))
 ORDER BY scope_type DESC,id`, auth.Tenant(x.ctx), x.scope.WorkspaceID)
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
	if err = db.Tx(x.ctx).QueryRow(x.ctx,
		(`SELECT id::text
 FROM assets
 WHERE tenant_id=$1
 AND kind=$2
 AND publisher||'/'||name=$3`),
		auth.Tenant(x.ctx),
		kind,
		ref).Scan(&id); err != nil {
		return node{}, err
	}
	a, err := x.r.Assets.Get(x.ctx, id, "asset.use")
	if err != nil {
		return node{}, err
	}
	if a.Status != "active" {
		return node{}, assets.Invalid("Asset is unavailable")
	}
	v, err := x.selectInstalledVersion(a, constraint)
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
	x.lock.Entries = append(
		x.lock.Entries,
		contract.LockEntry{Kind: kind, Ref: ref, Version: parent.version.Version, Digest: digest, Files: files},
	)
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
	err := db.Tx(x.ctx).
		QueryRow(x.ctx, `SELECT EXISTS(SELECT 1 FROM asset_files WHERE version_id=$1 AND path=$2)`, n.version.ID, n.prefix+name).
		Scan(&exists)
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
func (x *resolution) member(n node, name, roleName string, role contract.MemberRole) (contract.LockMember, error) {
	m := contract.LockMember{
		Name:                 contract.MemberName(name),
		Role:                 role,
		AgentEntry:           n.index,
		EffectivePermissions: n.version.Permissions,
	}
	if n.version.Manifest.Agent != nil && n.version.Manifest.Agent.Description != nil {
		m.Description = *n.version.Manifest.Agent.Description
	}
	layers, err := x.memberVisibility(n, roleName)
	if err != nil {
		return m, err
	}
	skills, mcp, err := x.memberSkills(n)
	if err != nil {
		return m, err
	}
	m.Mcp = append(m.Mcp, mcp...)
	closure, mcp, err := x.skillClosure(skills)
	if err != nil {
		return m, err
	}
	m.Mcp = append(m.Mcp, mcp...)
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

func (x *resolution) memberVisibility(n node, roleName string) ([]Visibility, error) {
	layers := slices.Clone(x.visibility)
	if x.team != nil {
		for _, filename := range []string{"skills-visibility.json", "members/" + roleName + "/skills-visibility.json"} {
			v, e := x.fileVisibility(*x.team, filename)
			if e != nil {
				return nil, e
			}
			layers = append(layers, v)
		}
	}
	v, err := x.fileVisibility(n, "skills-visibility.json")
	if err != nil {
		return nil, err
	}
	layers = append(layers, v)
	return layers, nil
}
func (x *resolution) memberSkills(n node) ([]node, []int, error) {
	var mcp []int
	skills := append(slices.Clone(x.installed), x.shared...)
	for _, d := range n.version.Manifest.Dependencies {
		dep, err := x.resolve(d.Kind, d.Ref, d.Constraint)
		if err != nil {
			return nil, nil, err
		}
		if d.Kind == "skill" {
			skills = append(skills, dep)
		}
		if d.Kind == "mcp_server" {
			mcp = append(mcp, dep.index)
		}
	}
	// Bundle skills are represented as immutable derived entries under the signed
	// parent file set, including directories absent from skills.json.
	all, err := x.r.Assets.Files(x.ctx, n.version.ID)
	if err != nil {
		return nil, nil, err
	}
	embedded := []string{}
	for _, f := range all {
		rel := strings.TrimPrefix(f.Path, n.prefix)
		if strings.HasPrefix(f.Path, n.prefix) && strings.HasPrefix(rel, "skills/") &&
			strings.HasSuffix(rel, "/SKILL.md") {
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
			return nil, nil, err
		}
		skills = append(skills, child)
	}
	return skills, mcp, nil
}
func (x *resolution) skillClosure(skills []node) ([]node, []int, error) {
	var mcp []int
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
			mcp = append(mcp, n.index)
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
		if err := visit(skill); err != nil {
			return nil, nil, err
		}
	}
	return closure, mcp, nil
}

func (x *resolution) selectInstalledVersion(a assets.Asset, constraint string) (assets.Version, error) {
	var pinned, channel, installedConstraint string
	rows, err := db.Tx(x.ctx).
		Query(x.ctx, `SELECT COALESCE(pinned_version_id::text,''),COALESCE(channel,''),"constraint"
 FROM asset_installs
 WHERE asset_id=$1
 AND enabled
 AND ((scope_type='tenant' AND scope_id=$2) OR (scope_type='workspace' AND scope_id=NULLIF($3,'')::uuid))
 ORDER BY CASE scope_type WHEN 'workspace' THEN 0 ELSE 1 END
 LIMIT 1`, a.ID, auth.Tenant(x.ctx), x.scope.WorkspaceID)
	if err != nil {
		return assets.Version{}, err
	}
	if rows.Next() {
		err = rows.Scan(&pinned, &channel, &installedConstraint)
	}
	re := rows.Err()
	rows.Close()
	if err != nil {
		return assets.Version{}, err
	}
	if re != nil {
		return assets.Version{}, re
	}
	ranges := []string{constraint}
	if installedConstraint != "" {
		ranges = append(ranges, installedConstraint)
	}
	v, err := x.r.Assets.SelectVersion(x.ctx, a, ranges, pinned, channel)
	if err != nil {
		return assets.Version{}, err
	}
	return v, nil
}

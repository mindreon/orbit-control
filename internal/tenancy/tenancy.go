package tenancy

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
)

type Tenant struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Name string `json:"name"`
}

func Tenants(ctx context.Context, d *db.DB, pid string) ([]Tenant, error) {
	ts := []Tenant{}
	e := d.Global(ctx, func(tx pgx.Tx) error {
		rows, e := tx.Query(ctx, "SELECT id::text,slug,name FROM principal_tenants($1)", pid)
		if e != nil {
			return e
		}
		defer rows.Close()
		for rows.Next() {
			var t Tenant
			if e = rows.Scan(&t.ID, &t.Slug, &t.Name); e != nil {
				return e
			}
			ts = append(ts, t)
		}
		return rows.Err()
	})
	return ts, e
}
func Active(ctx context.Context, tx pgx.Tx, pid string) error {
	var ok bool
	e := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM memberships m JOIN tenants t ON t.id=m.tenant_id WHERE m.principal_id=$1 AND m.status='active' AND t.status='active')", pid).Scan(&ok)
	if e != nil {
		return e
	}
	if !ok {
		return httpx.E(403, "FORBIDDEN", "Active tenant membership required")
	}
	return nil
}
func Bind(ctx context.Context, tx pgx.Tx, p auth.Principal, tenant, subjectType, subject, role, scopeType, scope string) error {
	if !policy.ValidRole(role, scopeType) || !ids.Valid(scope) || !ids.Valid(subject) {
		return httpx.E(422, "INVALID_ROLE", "Invalid role or scope")
	}
	var exists bool
	var e error
	if subjectType == "group" {
		e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM groups WHERE id=$1 AND tenant_id=$2)", subject, tenant).Scan(&exists)
	} else if subjectType == "user" || subjectType == "service" {
		e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM memberships m JOIN principals p ON p.id=m.principal_id WHERE m.principal_id=$1 AND p.kind=$2 AND m.tenant_id=$3)", subject, subjectType, tenant).Scan(&exists)
	} else {
		return httpx.E(422, "INVALID_INPUT", "Invalid principal type")
	}
	if e != nil {
		return e
	}
	if !exists {
		return httpx.E(422, "INVALID_INPUT", "Role subject must belong to tenant")
	}
	switch scopeType {
	case "tenant":
		exists = scope == tenant
	case "workspace":
		e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workspaces WHERE id=$1 AND tenant_id=$2)", scope, tenant).Scan(&exists)
	case "asset":
		e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM assets WHERE id=$1 AND tenant_id=$2)", scope, tenant).Scan(&exists)
	case "session":
		e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1 AND tenant_id=$2)", scope, tenant).Scan(&exists)
	}
	if e != nil {
		return e
	}
	if !exists {
		return httpx.E(422, "INVALID_INPUT", "Role scope must belong to tenant")
	}
	_, e = tx.Exec(ctx, `INSERT INTO role_bindings(id,tenant_id,principal_type,principal_id,role,scope_type,scope_id,created_by) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(tenant_id,principal_type,principal_id,role,scope_type,scope_id) DO NOTHING`, ids.New(), tenant, subjectType, subject, role, scopeType, scope, p.ID)
	if e != nil {
		return e
	}
	return audit.Append(ctx, tx, p, tenant, "role.granted", scopeType, scope, map[string]any{"principal_id": subject, "principal_type": subjectType, "role": role})
}
func AddMember(ctx context.Context, tx pgx.Tx, p auth.Principal, tenant, pid, status, source, role string) error {
	if !ids.Valid(pid) {
		return httpx.E(422, "INVALID_INPUT", "Invalid principal_id")
	}
	if role == "" {
		role = "tenant.member"
	}
	if !policy.ValidRole(role, "tenant") {
		return httpx.E(422, "INVALID_ROLE", "Invalid tenant role")
	}
	_, e := tx.Exec(ctx, `INSERT INTO memberships(tenant_id,principal_id,status,source) VALUES($1,$2,$3,$4)`, tenant, pid, status, source)
	if e != nil {
		return e
	}
	var kind string
	if e = tx.QueryRow(ctx, "SELECT kind FROM principals WHERE id=$1", pid).Scan(&kind); e != nil {
		return e
	}
	if e = Bind(ctx, tx, p, tenant, kind, pid, role, "tenant", tenant); e != nil {
		return e
	}
	return audit.Append(ctx, tx, p, tenant, "member.added", "principal", pid, map[string]any{"status": status, "source": source})
}
func CreateTenant(ctx context.Context, d *db.DB, p auth.Principal, slug, name string) (Tenant, error) {
	t := Tenant{ID: ids.New(), Slug: slug, Name: name}
	if e := httpx.Required(slug, "slug"); e != nil {
		return t, e
	}
	if e := httpx.Required(name, "name"); e != nil {
		return t, e
	}
	e := d.Global(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "SELECT set_config('app.tenant_id',$1,true)", t.ID); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$2,$3)", t.ID, slug, name); e != nil {
			return e
		}
		if e := AddMember(ctx, tx, p, t.ID, p.ID, "active", "jit", "tenant.owner"); e != nil {
			return e
		}
		return audit.Append(ctx, tx, p, t.ID, "tenant.created", "tenant", t.ID, map[string]any{"slug": slug})
	})
	return t, e
}

// Personal tenant provisioning is serialized by principal to prevent two first
// requests from creating independent tenants for the same identity.
func EnsurePersonal(ctx context.Context, d *db.DB, p auth.Principal) ([]Tenant, error) {
	e := d.Global(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,1))", p.ID); e != nil {
			return e
		}
		var n int
		if e := tx.QueryRow(ctx, "SELECT count(*) FROM principal_tenants($1)", p.ID).Scan(&n); e != nil {
			return e
		}
		if n > 0 {
			return nil
		}
		// Suspended/invited existing members must not acquire another tenant via JIT.
		var existing bool
		if e := tx.QueryRow(ctx, "SELECT principal_has_membership($1)", p.ID).Scan(&existing); e != nil {
			return e
		}
		if existing {
			return nil
		}
		t := ids.New()
		if _, e := tx.Exec(ctx, "SELECT set_config('app.tenant_id',$1,true)", t); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$2,$3)", t, "personal-"+p.ID, "Personal workspace"); e != nil {
			return e
		}
		return AddMember(ctx, tx, p, t, p.ID, "active", "jit", "tenant.owner")
	})
	if e != nil {
		return nil, e
	}
	return Tenants(ctx, d, p.ID)
}
func CreateGroup(ctx context.Context, tx pgx.Tx, p auth.Principal, tenant, name string) (string, error) {
	id := ids.New()
	if e := httpx.Required(name, "name"); e != nil {
		return "", e
	}
	if _, e := tx.Exec(ctx, "INSERT INTO groups(id,tenant_id,name) VALUES($1,$2,$3)", id, tenant, name); e != nil {
		return "", e
	}
	return id, audit.Append(ctx, tx, p, tenant, "group.created", "group", id, map[string]any{"name": name})
}
func AddGroupMember(ctx context.Context, tx pgx.Tx, p auth.Principal, tenant, group, pid string) error {
	_, e := tx.Exec(ctx, "INSERT INTO group_members(tenant_id,group_id,principal_id) VALUES($1,$2,$3)", tenant, group, pid)
	if e != nil {
		return e
	}
	return audit.Append(ctx, tx, p, tenant, "group.member_added", "group", group, map[string]any{"principal_id": pid})
}
func CreateProject(ctx context.Context, tx pgx.Tx, p auth.Principal, tenant, workspace, name, kind string) (string, error) {
	id := ids.New()
	if e := httpx.Required(name, "name"); e != nil {
		return "", e
	}
	if _, e := tx.Exec(ctx, "INSERT INTO projects(id,tenant_id,workspace_id,name,kind) VALUES($1,$2,$3,$4,$5)", id, tenant, workspace, name, kind); e != nil {
		return "", e
	}
	return id, audit.Append(ctx, tx, p, tenant, "project.created", "project", id, map[string]any{"workspace_id": workspace, "name": name})
}
func ValidateStatus(status string) error {
	if status != "active" && status != "invited" && status != "suspended" {
		return httpx.E(422, "INVALID_INPUT", "Invalid membership status")
	}
	return nil
}

// CreateUser provisions the identity and its initial membership atomically.
// With no target tenant, it provisions a personal tenant owned by that user.
func CreateUser(ctx context.Context, d *db.DB, email, name, subject, tenant, role string, actor auth.Principal) (auth.Principal, error) {
	if !auth.ValidEmail(email) {
		return auth.Principal{}, fmt.Errorf("invalid email")
	}
	if strings.TrimSpace(name) == "" {
		name = email
	}
	p := auth.Principal{ID: ids.New(), Kind: "user", Email: email, DisplayName: name}
	if tenant != "" && !ids.Valid(tenant) {
		return p, fmt.Errorf("invalid tenant id")
	}
	if tenant != "" && !policy.ValidRole(role, "tenant") {
		return p, fmt.Errorf("invalid tenant role")
	}
	e := d.Global(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "INSERT INTO principals(id,kind,email,display_name,idp_subject) VALUES($1,$2,$3,$4,$5)", p.ID, p.Kind, email, name, subject); e != nil {
			return e
		}
		personal := tenant == ""
		if personal {
			tenant = ids.New()
			role = "tenant.owner"
		}
		if actor.ID == "" {
			actor = p
		}
		if _, e := tx.Exec(ctx, "SELECT set_config('app.tenant_id',$1,true)", tenant); e != nil {
			return e
		}
		if personal {
			if _, e := tx.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$2,$3)", tenant, "personal-"+p.ID, "Personal workspace"); e != nil {
				return e
			}
		}
		if e := AddMember(ctx, tx, actor, tenant, p.ID, "active", "direct", role); e != nil {
			return e
		}
		return audit.Append(ctx, tx, actor, tenant, "principal.created", "principal", p.ID, map[string]any{})
	})
	return p, e
}

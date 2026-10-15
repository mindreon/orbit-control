package httpapi

import (
	"net/http"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

func (h *Handler) listMembers(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if e := h.Authz.Require(ctx, "member.read", tenantResource(ctx)); e != nil {
		return e
	}
	page, e := httpx.Pagination(r)
	if e != nil {
		return e
	}
	rows, e := db.Tx(ctx).
		Query(ctx,
			`SELECT p.id::text,p.kind,p.email,p.display_name,m.status,m.source,COALESCE((SELECT jsonb_agg(role
 ORDER BY role)
 FROM role_bindings rb
 WHERE rb.principal_type IN ('user','service')
 AND rb.principal_id=p.id
 AND rb.scope_type='tenant'),'[]')
 FROM memberships m
 JOIN principals p ON p.id=m.principal_id
 WHERE ($1=''
 OR p.id>NULLIF($1,'')::uuid)
 ORDER BY p.id
 LIMIT $2`, page.Cursor, page.Limit+1)
	if e != nil {
		return e
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var p auth.Principal
		var status, source string
		var roles []string
		if e = rows.Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName, &status, &source, &roles); e != nil {
			return e
		}
		items = append(items, map[string]any{"id": p.ID, "principal": p, "status": status, "source": source, "roles": roles})
	}
	if e = rows.Err(); e != nil {
		return e
	}
	next := ""
	if len(items) > page.Limit {
		items = items[:page.Limit]
		next = httpx.Cursor(items[len(items)-1]["id"].(string))
	}
	httpx.JSON(w, 200, map[string]any{"items": items, "next_cursor": next})
	return nil
}
func (h *Handler) addMember(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if e := h.Authz.Require(ctx, "member.manage", tenantResource(ctx)); e != nil {
		return e
	}
	var in struct {
		PrincipalID string `json:"principal_id"`
		Role        string `json:"role"`
		Status      string `json:"status"`
	}
	if e := httpx.Decode(r, &in); e != nil {
		return e
	}
	if in.Status == "" {
		in.Status = "active"
	}
	if e := tenancy.ValidateStatus(in.Status); e != nil {
		return e
	}
	if in.Role == "tenant.owner" {
		if e := h.Authz.Require(ctx, "tenant.manage", tenantResource(ctx)); e != nil {
			return e
		}
	}
	if e := tenancy.AddMember(ctx, db.Tx(ctx), auth.From(ctx), auth.Tenant(ctx), in.PrincipalID, in.Status, "direct", in.Role); e != nil {
		return e
	}
	httpx.JSON(w, 201, in)
	return nil
}
func (h *Handler) patchMember(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if e := h.Authz.Require(ctx, "member.manage", tenantResource(ctx)); e != nil {
		return e
	}
	pid := r.PathValue("p")
	if !ids.Valid(pid) {
		return httpx.E(404, "NOT_FOUND", "Member not found")
	}
	var in struct {
		Role   *string `json:"role"`
		Status *string `json:"status"`
	}
	if e := httpx.Decode(r, &in); e != nil {
		return e
	}
	if in.Role == nil && in.Status == nil {
		return httpx.E(422, "INVALID_INPUT", "role or status required")
	}
	if in.Status != nil {
		if e := tenancy.ValidateStatus(*in.Status); e != nil {
			return e
		}
	}
	tx := db.Tx(ctx)
	var kind string
	if e := tx.QueryRow(ctx,
		(`SELECT p.kind
 FROM memberships m
 JOIN principals p ON p.id=m.principal_id
 WHERE p.id=$1`),
		pid).Scan(&kind); e != nil {
		return e
	}
	var owner bool
	if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1
 FROM role_bindings
 WHERE principal_id=$1
 AND principal_type IN ('user','service')
 AND role='tenant.owner')`, pid).Scan(&owner); e != nil {
		return e
	}
	if owner || in.Role != nil && *in.Role == "tenant.owner" {
		if e := h.Authz.Require(ctx, "tenant.manage", tenantResource(ctx)); e != nil {
			return e
		}
	}
	// Serialize owner changes and preserve at least one active owner.
	if _, e := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,3))", auth.Tenant(ctx)); e != nil {
		return e
	}
	if owner && (in.Role != nil && *in.Role != "tenant.owner" || in.Status != nil && *in.Status != "active") {
		var count int
		if e := tx.QueryRow(ctx, `SELECT count(*)
 FROM role_bindings rb
 JOIN memberships m ON m.principal_id=rb.principal_id
 AND m.tenant_id=rb.tenant_id
 WHERE rb.role='tenant.owner'
 AND rb.principal_type IN ('user','service')
 AND m.status='active'`).Scan(&count); e != nil {
			return e
		}
		if count <= 1 {
			return httpx.E(409, "LAST_OWNER", "Cannot remove the last active tenant owner")
		}
	}
	if in.Role != nil {
		if !policy.ValidRole(*in.Role, "tenant") {
			return httpx.E(422, "INVALID_ROLE", "Invalid tenant role")
		}
		if _, e := tx.Exec(ctx, `DELETE
 FROM role_bindings
 WHERE principal_type IN ('user','service')
 AND principal_id=$1
 AND scope_type='tenant'`, pid); e != nil {
			return e
		}
		if e := tenancy.Bind(ctx, tx, auth.From(ctx), auth.Tenant(ctx), kind, pid, *in.Role, "tenant", auth.Tenant(ctx)); e != nil {
			return e
		}
	}
	if in.Status != nil {
		if _, e := tx.Exec(ctx, "UPDATE memberships SET status=$2 WHERE principal_id=$1", pid, *in.Status); e != nil {
			return e
		}
	}
	if e := h.logWrite(ctx, "member.updated", "principal", pid, in); e != nil {
		return e
	}
	httpx.JSON(w, 200, map[string]any{"principal_id": pid, "role": in.Role, "status": in.Status})
	return nil
}
func (h *Handler) listKeys(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if e := h.Authz.Require(ctx, "apikey.read", tenantResource(ctx)); e != nil {
		return e
	}
	page, e := httpx.Pagination(r)
	if e != nil {
		return e
	}
	rows, e := db.Tx(ctx).
		Query(ctx, `SELECT id::text,principal_id::text,name,prefix,scopes,expires_at,last_used_at
 FROM api_keys
 WHERE ($1=''
 OR id>NULLIF($1,'')::uuid)
 ORDER BY id
 LIMIT $2`, page.Cursor, page.Limit+1)
	if e != nil {
		return e
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, pid, name, prefix string
		var scopes []string
		var exp time.Time
		var last *time.Time
		if e = rows.Scan(&id, &pid, &name, &prefix, &scopes, &exp, &last); e != nil {
			return e
		}
		items = append(
			items,
			map[string]any{
				"id":           id,
				"principal_id": pid,
				"name":         name,
				"prefix":       prefix,
				"scopes":       scopes,
				"expires_at":   exp,
				"last_used_at": last,
			},
		)
	}
	if e = rows.Err(); e != nil {
		return e
	}
	next := ""
	if len(items) > page.Limit {
		items = items[:page.Limit]
		next = httpx.Cursor(items[len(items)-1]["id"].(string))
	}
	httpx.JSON(w, 200, map[string]any{"items": items, "next_cursor": next})
	return nil
}
func (h *Handler) createKey(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if e := h.Authz.Require(ctx, "apikey.manage", tenantResource(ctx)); e != nil {
		return e
	}
	var in struct {
		PrincipalID string    `json:"principal_id"`
		Name        string    `json:"name"`
		Scopes      []string  `json:"scopes"`
		ExpiresAt   time.Time `json:"expires_at"`
	}
	if e := httpx.Decode(r, &in); e != nil {
		return e
	}
	if in.PrincipalID == "" {
		in.PrincipalID = auth.From(ctx).ID
	}
	if !ids.Valid(in.PrincipalID) {
		return httpx.E(422, "INVALID_INPUT", "Invalid principal_id")
	}
	if e := httpx.Required(in.Name, "name"); e != nil {
		return e
	}
	if !in.ExpiresAt.After(time.Now()) || in.ExpiresAt.After(time.Now().AddDate(1, 0, 0)) {
		return httpx.E(422, "INVALID_INPUT", "expires_at must be in the next year")
	}
	if len(in.Scopes) == 0 {
		return httpx.E(422, "INVALID_INPUT", "At least one scope required")
	}
	if e := h.Authz.RequireKeyIssuance(ctx, in.PrincipalID, in.Scopes); e != nil {
		return e
	}
	token, prefix, hash, e := auth.GenerateAPIKey()
	if e != nil {
		return e
	}
	id := ids.New()
	_, e = db.Tx(ctx).
		Exec(ctx, `INSERT INTO api_keys(id,tenant_id,principal_id,name,prefix,hash,scopes,expires_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, id, auth.Tenant(ctx), in.PrincipalID, in.Name, prefix, hash, in.Scopes,
			in.ExpiresAt)
	if e != nil {
		return e
	}
	if e = h.logWrite(ctx, "apikey.created", "api_key", id, map[string]any{"principal_id": in.PrincipalID, "scopes": in.Scopes}); e != nil {
		return e
	}
	httpx.JSON(w, 201, map[string]any{"id": id, "key": token, "prefix": prefix, "scopes": in.Scopes, "expires_at": in.ExpiresAt})
	return nil
}
func (h *Handler) deleteKey(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if e := h.Authz.Require(ctx, "apikey.manage", tenantResource(ctx)); e != nil {
		return e
	}
	id := r.PathValue("k")
	if !ids.Valid(id) {
		return httpx.E(404, "NOT_FOUND", "API key not found")
	}
	n, e := db.Tx(ctx).Exec(ctx, "DELETE FROM api_keys WHERE id=$1", id)
	if e != nil {
		return e
	}
	if n.RowsAffected() == 0 {
		return httpx.E(404, "NOT_FOUND", "API key not found")
	}
	if e = h.logWrite(ctx, "apikey.deleted", "api_key", id, map[string]any{}); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
func (h *Handler) listAudit(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if e := h.Authz.Require(ctx, "audit.read", tenantResource(ctx)); e != nil {
		return e
	}
	page, e := httpx.Pagination(r)
	if e != nil {
		return e
	}
	rows, e := db.Tx(ctx).
		Query(ctx,
			`SELECT id::text,actor_id::text,actor_kind,action,target_type,target_id,detail,encode(prev_hash,'hex'),encode(hash,
 'hex'),occurred_at
 FROM audit_log
 WHERE ($1=''
 OR id>NULLIF($1,'')::uuid)
 ORDER BY id
 LIMIT $2`, page.Cursor, page.Limit+1)
	if e != nil {
		return e
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, actor, kind, action, typ, target, prev, hash string
		var detail any
		var at time.Time
		if e = rows.Scan(&id, &actor, &kind, &action, &typ, &target, &detail, &prev, &hash, &at); e != nil {
			return e
		}
		items = append(
			items,
			map[string]any{
				"id":          id,
				"actor_id":    actor,
				"actor_kind":  kind,
				"action":      action,
				"target_type": typ,
				"target_id":   target,
				"detail":      detail,
				"prev_hash":   prev,
				"hash":        hash,
				"occurred_at": at,
			},
		)
	}
	if e = rows.Err(); e != nil {
		return e
	}
	next := ""
	if len(items) > page.Limit {
		items = items[:page.Limit]
		next = httpx.Cursor(items[len(items)-1]["id"].(string))
	}
	httpx.JSON(w, 200, map[string]any{"items": items, "next_cursor": next})
	return nil
}

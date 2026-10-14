package httpapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

type Workspace struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	DefaultAgentRef *string         `json:"default_agent_ref"`
	Settings        json.RawMessage `json:"settings"`
	CreatedAt       string          `json:"created_at"`
}

func (h *Handler) workspace(ctx context.Context, id string, action string) (Workspace, error) {
	var ws Workspace
	if !ids.Valid(id) {
		return ws, httpx.E(404, "NOT_FOUND", "Workspace not found")
	}
	e := db.Tx(ctx).QueryRow(ctx, "SELECT id::text,name,default_agent_ref,settings,created_at::text FROM workspaces WHERE id=$1", id).Scan(&ws.ID, &ws.Name, &ws.DefaultAgentRef, &ws.Settings, &ws.CreatedAt)
	if e != nil {
		return ws, e
	}
	return ws, h.Authz.Require(ctx, action, policy.Resource{Type: "workspace", ID: id})
}
func (h *Handler) listWorkspaces(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	page, e := httpx.Pagination(r)
	if e != nil {
		return e
	}
	all, visible, e := h.Authz.WorkspaceFilter(ctx)
	if e != nil {
		return e
	}
	rows, e := db.Tx(ctx).Query(ctx, `SELECT id::text,name,default_agent_ref,settings,created_at::text FROM workspaces WHERE ($1='' OR id>NULLIF($1,'')::uuid) AND ($2 OR id=ANY($3::uuid[])) ORDER BY id LIMIT $4`, page.Cursor, all, visible, page.Limit+1)
	if e != nil {
		return e
	}
	defer rows.Close()
	result := []Workspace{}
	next := ""
	for rows.Next() {
		var ws Workspace
		if e = rows.Scan(&ws.ID, &ws.Name, &ws.DefaultAgentRef, &ws.Settings, &ws.CreatedAt); e != nil {
			return e
		}
		result = append(result, ws)
		if len(result) > page.Limit {
			result = result[:page.Limit]
			next = httpx.Cursor(result[len(result)-1].ID)
			break
		}
	}
	if e = rows.Err(); e != nil {
		return e
	}
	httpx.JSON(w, 200, map[string]any{"items": result, "next_cursor": next})
	return nil
}
func (h *Handler) createWorkspace(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if e := h.Authz.Require(ctx, "workspace.create", tenantResource(ctx)); e != nil {
		return e
	}
	var input struct {
		Name            string         `json:"name"`
		Settings        map[string]any `json:"settings"`
		DefaultAgentRef *string        `json:"default_agent_ref"`
	}
	if e := httpx.Decode(r, &input); e != nil {
		return e
	}
	if e := httpx.Required(input.Name, "name"); e != nil {
		return e
	}
	if input.Settings == nil {
		input.Settings = map[string]any{}
	}
	settings, e := json.Marshal(input.Settings)
	if e != nil {
		return e
	}
	id := ids.New()
	tenant := auth.Tenant(ctx)
	tx := db.Tx(ctx)
	p := auth.From(ctx)
	if _, e = tx.Exec(ctx, "INSERT INTO workspaces(id,tenant_id,name,settings,default_agent_ref) VALUES($1,$2,$3,$4,$5)", id, tenant, input.Name, settings, input.DefaultAgentRef); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO workspace_members(tenant_id,workspace_id,principal_id) VALUES($1,$2,$3)", tenant, id, p.ID); e != nil {
		return e
	}
	if e = tenancy.Bind(ctx, tx, p, tenant, p.Kind, p.ID, "ws.admin", "workspace", id); e != nil {
		return e
	}
	if e = h.logWrite(ctx, "workspace.created", "workspace", id, map[string]any{"name": input.Name}); e != nil {
		return e
	}
	ws, e := h.workspace(ctx, id, "workspace.read")
	if e != nil {
		return e
	}
	httpx.JSON(w, 201, ws)
	return nil
}
func (h *Handler) getWorkspace(w http.ResponseWriter, r *http.Request) error {
	ws, e := h.workspace(r.Context(), r.PathValue("w"), "workspace.read")
	if e != nil {
		return e
	}
	httpx.JSON(w, 200, ws)
	return nil
}
func (h *Handler) patchWorkspace(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	id := r.PathValue("w")
	_, e := h.workspace(ctx, id, "workspace.update")
	if e != nil {
		return e
	}
	var input struct {
		Name            *string         `json:"name"`
		Settings        *map[string]any `json:"settings"`
		DefaultAgentRef *string         `json:"default_agent_ref"`
	}
	if e = httpx.Decode(r, &input); e != nil {
		return e
	}
	if input.Name == nil && input.Settings == nil && input.DefaultAgentRef == nil {
		return httpx.E(422, "INVALID_INPUT", "At least one field required")
	}
	if input.Name != nil {
		if e = httpx.Required(*input.Name, "name"); e != nil {
			return e
		}
	}
	var s []byte
	if input.Settings != nil {
		s, e = json.Marshal(*input.Settings)
		if e != nil {
			return e
		}
	}
	if _, e = db.Tx(ctx).Exec(ctx, "UPDATE workspaces SET name=COALESCE($2,name),settings=COALESCE($3::jsonb,settings),default_agent_ref=COALESCE($4,default_agent_ref) WHERE id=$1", id, input.Name, s, input.DefaultAgentRef); e != nil {
		return e
	}
	if e = h.logWrite(ctx, "workspace.updated", "workspace", id, input); e != nil {
		return e
	}
	ws, e := h.workspace(ctx, id, "workspace.read")
	if e != nil {
		return e
	}
	httpx.JSON(w, 200, ws)
	return nil
}
func (h *Handler) listWorkspaceMembers(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	id := r.PathValue("w")
	if _, e := h.workspace(ctx, id, "workspace.read"); e != nil {
		return e
	}
	page, e := httpx.Pagination(r)
	if e != nil {
		return e
	}
	rows, e := db.Tx(ctx).Query(ctx, `SELECT p.id::text,p.kind,p.email,p.display_name FROM workspace_members wm JOIN principals p ON p.id=wm.principal_id WHERE wm.workspace_id=$1 AND ($2='' OR p.id>NULLIF($2,'')::uuid) ORDER BY p.id LIMIT $3`, id, page.Cursor, page.Limit+1)
	if e != nil {
		return e
	}
	defer rows.Close()
	result := []auth.Principal{}
	for rows.Next() {
		var p auth.Principal
		if e = rows.Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName); e != nil {
			return e
		}
		result = append(result, p)
	}
	if e = rows.Err(); e != nil {
		return e
	}
	next := ""
	if len(result) > page.Limit {
		result = result[:page.Limit]
		next = httpx.Cursor(result[len(result)-1].ID)
	}
	httpx.JSON(w, 200, map[string]any{"items": result, "next_cursor": next})
	return nil
}
func (h *Handler) addWorkspaceMember(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	id := r.PathValue("w")
	if _, e := h.workspace(ctx, id, "workspace.members"); e != nil {
		return e
	}
	var input struct {
		PrincipalID string `json:"principal_id"`
		Role        string `json:"role"`
	}
	if e := httpx.Decode(r, &input); e != nil {
		return e
	}
	if input.Role == "" {
		input.Role = "ws.member"
	}
	if !policy.ValidRole(input.Role, "workspace") || !ids.Valid(input.PrincipalID) {
		return httpx.E(422, "INVALID_INPUT", "Invalid member or workspace role")
	}
	tx := db.Tx(ctx)
	var kind string
	if e := tx.QueryRow(ctx, "SELECT p.kind FROM principals p JOIN memberships m ON m.principal_id=p.id WHERE p.id=$1 AND m.status='active'", input.PrincipalID).Scan(&kind); e != nil {
		return e
	}
	if _, e := tx.Exec(ctx, "INSERT INTO workspace_members(tenant_id,workspace_id,principal_id) VALUES($1,$2,$3)", auth.Tenant(ctx), id, input.PrincipalID); e != nil {
		return e
	}
	if e := tenancy.Bind(ctx, tx, auth.From(ctx), auth.Tenant(ctx), kind, input.PrincipalID, input.Role, "workspace", id); e != nil {
		return e
	}
	if e := h.logWrite(ctx, "workspace.member_added", "workspace", id, input); e != nil {
		return e
	}
	httpx.JSON(w, 201, input)
	return nil
}
func (h *Handler) deleteWorkspaceMember(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	id, pid := r.PathValue("w"), r.PathValue("p")
	if _, e := h.workspace(ctx, id, "workspace.members"); e != nil {
		return e
	}
	if !ids.Valid(pid) {
		return httpx.E(404, "NOT_FOUND", "Member not found")
	}
	tx := db.Tx(ctx)
	n, e := tx.Exec(ctx, "DELETE FROM workspace_members WHERE workspace_id=$1 AND principal_id=$2", id, pid)
	if e != nil {
		return e
	}
	if n.RowsAffected() == 0 {
		return httpx.E(404, "NOT_FOUND", "Member not found")
	}
	if _, e = tx.Exec(ctx, "DELETE FROM role_bindings WHERE scope_type='workspace' AND scope_id=$1 AND principal_type IN ('user','service') AND principal_id=$2", id, pid); e != nil {
		return e
	}
	if e = h.logWrite(ctx, "workspace.member_removed", "workspace", id, map[string]string{"principal_id": pid}); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}

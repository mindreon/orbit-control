package httpapi

import (
	"net/http"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

func (h *Handler) createServicePrincipal(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.Authz.Require(ctx, "tenant.manage", tenantResource(ctx)); err != nil {
		return err
	}
	var in struct {
		DisplayName string `json:"display_name"`
		Role        string `json:"role"`
	}
	if err := httpx.Decode(r, &in); err != nil {
		return err
	}
	if err := httpx.Required(in.DisplayName, "display_name"); err != nil {
		return err
	}
	if in.Role == "" {
		in.Role = "tenant.member"
	}
	if !policy.ValidRole(in.Role, "tenant") {
		return httpx.E(422, "INVALID_ROLE", "Invalid tenant role")
	}
	id := ids.New()
	tx := db.Tx(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'service','',$2,$3)`, id, in.DisplayName, "service:"+id); err != nil {
		return err
	}
	if err := tenancy.AddMember(ctx, tx, auth.From(ctx), auth.Tenant(ctx), id, "active", "direct", in.Role); err != nil {
		return err
	}
	if err := h.logWrite(ctx, "service_principal.created", "principal", id, map[string]any{"role": in.Role}); err != nil {
		return err
	}
	httpx.JSON(w, 201, auth.Principal{ID: id, Kind: "service", DisplayName: in.DisplayName})
	return nil
}
func (h *Handler) listServicePrincipals(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.Authz.Require(ctx, "tenant.manage", tenantResource(ctx)); err != nil {
		return err
	}
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	rows, err := db.Tx(ctx).
		Query(ctx, `SELECT p.id::text,p.kind,p.email,p.display_name
 FROM principals p
 JOIN memberships m ON m.principal_id=p.id
 WHERE p.kind='service'
 AND ($1=''
 OR p.id>NULLIF($1,'')::uuid)
 ORDER BY p.id
 LIMIT $2`, page.Cursor, page.Limit+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []auth.Principal{}
	for rows.Next() {
		var p auth.Principal
		if err = rows.Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName); err != nil {
			return err
		}
		items = append(items, p)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	next := ""
	if len(items) > page.Limit {
		items = items[:page.Limit]
		next = httpx.Cursor(items[len(items)-1].ID)
	}
	httpx.JSON(w, 200, map[string]any{"items": items, "next_cursor": next})
	return nil
}

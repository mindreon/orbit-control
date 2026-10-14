package httpapi

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

type invitation struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	ExpiresAt time.Time `json:"expires_at"`
	Status    string    `json:"status"`
}

func (h *Handler) createInvitation(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.Authz.Require(ctx, "member.manage", tenantResource(ctx)); err != nil {
		return err
	}
	var in struct {
		Email     string    `json:"email"`
		Role      string    `json:"role"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := httpx.Decode(r, &in); err != nil {
		return err
	}
	in.Email = strings.ToLower(strings.TrimSpace(in.Email))
	if !auth.ValidEmail(in.Email) {
		return httpx.E(422, "INVALID_INPUT", "Valid email required")
	}
	if in.Role == "" {
		in.Role = "tenant.member"
	}
	if !policy.ValidRole(in.Role, "tenant") {
		return httpx.E(422, "INVALID_ROLE", "Invalid tenant role")
	}
	if in.Role == "tenant.owner" {
		if err := h.Authz.Require(ctx, "tenant.manage", tenantResource(ctx)); err != nil {
			return err
		}
	}
	if !in.ExpiresAt.After(time.Now()) {
		return httpx.E(422, "INVALID_INPUT", "expires_at must be in the future")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return err
	}
	token := base64.RawURLEncoding.EncodeToString(secret)
	hash := sha256.Sum256([]byte(token))
	id := ids.New()
	if _, err := db.Tx(ctx).Exec(ctx, `INSERT INTO invitations(id,tenant_id,email,role,token_hash,expires_at,created_by) VALUES($1,$2,$3,$4,$5,$6,$7)`, id, auth.Tenant(ctx), in.Email, in.Role, hash[:], in.ExpiresAt, auth.From(ctx).ID); err != nil {
		return err
	}
	if err := h.logWrite(ctx, "invitation.created", "invitation", id, map[string]any{"email": in.Email, "role": in.Role, "expires_at": in.ExpiresAt}); err != nil {
		return err
	}
	httpx.JSON(w, 201, map[string]any{"id": id, "email": in.Email, "role": in.Role, "expires_at": in.ExpiresAt, "status": "pending", "token": token})
	return nil
}
func (h *Handler) listInvitations(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.Authz.Require(ctx, "member.read", tenantResource(ctx)); err != nil {
		return err
	}
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	rows, err := db.Tx(ctx).Query(ctx, `SELECT id::text,email,role,expires_at,status FROM invitations WHERE ($1='' OR id>NULLIF($1,'')::uuid) ORDER BY id LIMIT $2`, page.Cursor, page.Limit+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []invitation{}
	for rows.Next() {
		var i invitation
		if err = rows.Scan(&i.ID, &i.Email, &i.Role, &i.ExpiresAt, &i.Status); err != nil {
			return err
		}
		items = append(items, i)
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
func (h *Handler) deleteInvitation(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.Authz.Require(ctx, "member.manage", tenantResource(ctx)); err != nil {
		return err
	}
	id := r.PathValue("i")
	if !ids.Valid(id) {
		return httpx.E(404, "NOT_FOUND", "Invitation not found")
	}
	var role string
	if err := db.Tx(ctx).QueryRow(ctx, "SELECT role FROM invitations WHERE id=$1 AND status='pending' FOR UPDATE", id).Scan(&role); err != nil {
		return err
	}
	if role == "tenant.owner" {
		if err := h.Authz.Require(ctx, "tenant.manage", tenantResource(ctx)); err != nil {
			return err
		}
	}
	if _, err := db.Tx(ctx).Exec(ctx, "UPDATE invitations SET status='revoked' WHERE id=$1", id); err != nil {
		return err
	}
	if err := h.logWrite(ctx, "invitation.revoked", "invitation", id, map[string]any{}); err != nil {
		return err
	}
	w.WriteHeader(204)
	return nil
}
func (h *Handler) acceptInvitation(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	p := auth.From(ctx)
	if p.APIKey || p.Kind != "user" || !p.EmailVerified {
		return httpx.E(403, "FORBIDDEN", "A user with a verified email is required")
	}
	token, ok := strings.CutSuffix(r.PathValue("token"), ":accept")
	if !ok || len(token) != 43 {
		return httpx.E(404, "NOT_FOUND", "Invitation not found")
	}
	hash := sha256.Sum256([]byte(token))
	var tenant string
	if err := h.DB.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT invitation_tenant($1)::text", hash[:]).Scan(&tenant)
	}); err != nil {
		return err
	}
	return h.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		ctx = auth.Context(db.Context(ctx, tx), p, tenant)
		r = r.WithContext(ctx)
		return httpx.Idempotent(ctx, tx, tenant, p.ID, h.Auth.Replay, w, r, func(w http.ResponseWriter, r *http.Request) error {
			var i invitation
			if err := tx.QueryRow(ctx, "SELECT id::text,email,role,expires_at,status FROM invitations WHERE tenant_id=$1 AND token_hash=$2 AND status='pending' AND expires_at>now() FOR UPDATE", tenant, hash[:]).Scan(&i.ID, &i.Email, &i.Role, &i.ExpiresAt, &i.Status); err != nil {
				return err
			}
			if !strings.EqualFold(i.Email, p.Email) {
				return httpx.E(403, "FORBIDDEN", "Invitation email does not match verified email")
			}
			// Existing/suspended membership cannot be escalated or reactivated by an invitation.
			if err := tenancy.AddMember(ctx, tx, p, tenant, p.ID, "active", "invite", i.Role); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "UPDATE invitations SET status='accepted',accepted_by=$2,accepted_at=now() WHERE id=$1", i.ID, p.ID); err != nil {
				return err
			}
			if err := h.logWrite(ctx, "invitation.accepted", "invitation", i.ID, map[string]any{"principal_id": p.ID}); err != nil {
				return err
			}
			httpx.JSON(w, 200, map[string]any{"tenant_id": tenant})
			return nil
		})
	})
}

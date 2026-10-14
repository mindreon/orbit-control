package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

type Handler struct {
	DB     *db.DB
	Auth   *auth.Service
	Authz  policy.Authorizer
	Config config.Config
	Logger *slog.Logger
}
type endpoint func(http.ResponseWriter, *http.Request) error
type route struct {
	Pattern string
	Public  bool
	Handle  func(*Handler) endpoint
}

var routes = []route{
	{"GET /health", true, func(h *Handler) endpoint { return h.health }},
	{"GET /ready", true, func(h *Handler) endpoint { return h.ready }},
	{"GET /v1/auth/config", true, func(h *Handler) endpoint { return h.authConfig }},
	{"POST /v1/auth/dev/token", true, func(h *Handler) endpoint { return h.devToken }},
	{"GET /v1/me", false, func(h *Handler) endpoint { return h.me }},
	{"GET /v1/tenants", false, func(h *Handler) endpoint { return h.tenants }},
	{"GET /v1/workspaces", false, func(h *Handler) endpoint { return h.listWorkspaces }},
	{"POST /v1/workspaces", false, func(h *Handler) endpoint { return h.createWorkspace }},
	{"GET /v1/workspaces/{w}", false, func(h *Handler) endpoint { return h.getWorkspace }},
	{"PATCH /v1/workspaces/{w}", false, func(h *Handler) endpoint { return h.patchWorkspace }},
	{"GET /v1/workspaces/{w}/members", false, func(h *Handler) endpoint { return h.listWorkspaceMembers }},
	{"POST /v1/workspaces/{w}/members", false, func(h *Handler) endpoint { return h.addWorkspaceMember }},
	{"DELETE /v1/workspaces/{w}/members/{p}", false, func(h *Handler) endpoint { return h.deleteWorkspaceMember }},
	{"GET /v1/admin/members", false, func(h *Handler) endpoint { return h.listMembers }},
	{"POST /v1/admin/members", false, func(h *Handler) endpoint { return h.addMember }},
	{"PATCH /v1/admin/members/{p}", false, func(h *Handler) endpoint { return h.patchMember }},
	{"GET /v1/admin/api-keys", false, func(h *Handler) endpoint { return h.listKeys }},
	{"POST /v1/admin/api-keys", false, func(h *Handler) endpoint { return h.createKey }},
	{"DELETE /v1/admin/api-keys", false, func(h *Handler) endpoint { return h.deleteKey }},
	{"GET /v1/admin/audit", false, func(h *Handler) endpoint { return h.listAudit }},
}

func Routes() []string {
	out := []string{}
	for _, r := range routes {
		out = append(out, r.Pattern)
	}
	return out
}
func (h *Handler) Router() http.Handler {
	mux := http.NewServeMux()
	for _, route := range routes {
		fn := route.Handle(h)
		mux.HandleFunc(route.Pattern, func(w http.ResponseWriter, r *http.Request) {
			ip, _, _ := net.SplitHostPort(r.RemoteAddr)
			r = r.WithContext(audit.Context(r.Context(), ip, r.UserAgent()))
			rec := httpx.NewRecorder()
			var e error
			if route.Public {
				e = fn(rec, r)
			} else {
				e = h.protected(rec, r, fn)
			}
			if e != nil {
				var expected *httpx.Error
				if !errors.As(e, &expected) && !errors.Is(e, pgx.ErrNoRows) {
					code := ""
					var pgerr *pgconn.PgError
					if errors.As(e, &pgerr) {
						code = pgerr.Code
					}
					h.Logger.Error("request failed", "error_type", fmt.Sprintf("%T", e), "sqlstate", code)
				}
				httpx.WriteError(w, e)
				return
			}
			rec.FlushTo(w)
		})
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, httpx.E(404, "NOT_FOUND", "Route not found"))
	})
	return httpx.Middleware(h.Logger, h.Config.CORS, mux)
}
func (h *Handler) protected(w http.ResponseWriter, r *http.Request, fn endpoint) error {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return httpx.E(401, "UNAUTHENTICATED", "Bearer token required")
	}
	tenant := r.Header.Get("X-Orbit-Tenant")
	if tenant != "" && !ids.Valid(tenant) {
		return httpx.E(400, "INVALID_TENANT", "Invalid X-Orbit-Tenant")
	}
	p, e := h.Auth.Authenticate(r.Context(), token, tenant)
	if e != nil {
		return httpx.E(401, "UNAUTHENTICATED", "Invalid credentials")
	}
	ts, e := tenancy.EnsurePersonal(r.Context(), h.DB, p)
	if e != nil {
		return e
	}
	if !p.APIKey && h.Config.AuthMode == "oidc" {
		for _, t := range ts {
			if e = h.DB.WithTenant(r.Context(), t.ID, func(tx pgx.Tx) error {
				hash := sha256.Sum256([]byte(token))
				n, e := tx.Exec(r.Context(), `INSERT INTO idempotency_keys(tenant_id,principal_id,key,namespace,method,path,request_hash,status,headers,response,expires_at) VALUES($1,$2,$3,'auth','AUTH',$4,$5,200,'{}','',now()+interval '24 hours') ON CONFLICT DO NOTHING`, t.ID, p.ID, hex.EncodeToString(hash[:]), h.Config.Issuer, hash[:])
				if e != nil {
					return e
				}
				if n.RowsAffected() == 0 {
					return nil
				}
				return audit.Append(r.Context(), tx, p, t.ID, "login", "principal", p.ID, map[string]any{"mode": "oidc"})
			}); e != nil {
				return e
			}
		}
	}
	ctx := auth.Context(r.Context(), p, tenant)
	r = r.WithContext(ctx)
	if tenant == "" {
		if r.URL.Path != "/v1/me" && r.URL.Path != "/v1/tenants" {
			return httpx.E(403, "TENANT_REQUIRED", "X-Orbit-Tenant is required")
		}
		return fn(w, r)
	}
	return h.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		if e := tenancy.Active(ctx, tx, p.ID); e != nil {
			return e
		}
		ctx = db.Context(ctx, tx)
		r = r.WithContext(ctx)

		return httpx.Idempotent(ctx, tx, tenant, p.ID, h.Auth.Signer.ReplayKey(), w, r, fn)
	})
}
func (h *Handler) health(w http.ResponseWriter, r *http.Request) error {
	httpx.JSON(w, 200, map[string]string{"status": "ok"})
	return nil
}
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) error {
	if e := h.DB.Ping(r.Context()); e != nil {
		err := httpx.E(503, "NOT_READY", "Database unavailable")
		err.Retryable = true
		return err
	}
	return h.health(w, r)
}
func (h *Handler) authConfig(w http.ResponseWriter, r *http.Request) error {
	v := map[string]any{"mode": h.Config.AuthMode}
	if h.Config.AuthMode == "oidc" {
		v["issuer"] = h.Config.Issuer
		v["client_id"] = h.Config.ClientID
		v["audience"] = h.Config.Audience
		v["scopes"] = []string{"openid", "profile", "email"}
	}
	httpx.JSON(w, 200, v)
	return nil
}
func (h *Handler) devToken(w http.ResponseWriter, r *http.Request) error {
	if h.Config.AuthMode != "dev" {
		return httpx.E(404, "NOT_FOUND", "Route not found")
	}
	body, e := io.ReadAll(r.Body)
	if e != nil {
		return httpx.E(413, "BODY_TOO_LARGE", "Request body exceeds limit")
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var input struct {
		Email string `json:"email"`
		Name  string `json:"display_name"`
	}
	if e := httpx.Decode(r, &input); e != nil {
		return e
	}
	input.Email = strings.ToLower(strings.TrimSpace(input.Email))
	if !auth.ValidEmail(input.Email) {
		return httpx.E(422, "INVALID_INPUT", "Valid email required")
	}
	if len(input.Name) > 256 {
		return httpx.E(422, "INVALID_INPUT", "display_name too long")
	}
	p, e := h.Auth.JIT(r.Context(), auth.DevIssuer, input.Email, input.Email, input.Name)
	if e != nil {
		return e
	}
	ts, e := tenancy.EnsurePersonal(r.Context(), h.DB, p)
	if e != nil {
		return e
	}
	if len(ts) == 0 {
		return httpx.E(403, "FORBIDDEN", "No active tenant membership")
	}
	tenant := ts[0].ID
	if requested := r.Header.Get("X-Orbit-Tenant"); requested != "" {
		found := false
		for _, t := range ts {
			if t.ID == requested {
				tenant = requested
				found = true
			}
		}
		if !found {
			return httpx.E(403, "FORBIDDEN", "Active tenant membership required")
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return h.DB.WithTenant(r.Context(), tenant, func(tx pgx.Tx) error {
		return httpx.Idempotent(r.Context(), tx, tenant, p.ID, h.Auth.Signer.ReplayKey(), w, r, func(w http.ResponseWriter, r *http.Request) error {
			if e := audit.Append(r.Context(), tx, p, tenant, "login", "principal", p.ID, map[string]any{"mode": "dev"}); e != nil {
				return e
			}
			token, e := h.Auth.Signer.DevToken(p)
			if e != nil {
				return e
			}
			httpx.JSON(w, 200, map[string]any{"access_token": token, "expires_in": 3600})
			return nil
		})
	})
}
func (h *Handler) me(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	p := auth.From(ctx)
	ts, e := tenancy.Tenants(ctx, h.DB, p.ID)
	if e != nil {
		return e
	}
	roles := []string{}
	var tenant any
	if t := auth.Tenant(ctx); t != "" {
		tenant = t
		b, e := h.Authz.Bindings(ctx)
		if e != nil {
			return e
		}
		for _, v := range b {
			roles = append(roles, v.Role)
		}
		slices.Sort(roles)
		roles = slices.Compact(roles)
	}
	httpx.JSON(w, 200, map[string]any{"principal": p, "tenants": ts, "current_tenant": tenant, "roles": roles})
	return nil
}
func (h *Handler) tenants(w http.ResponseWriter, r *http.Request) error {
	page, e := httpx.Pagination(r)
	if e != nil {
		return e
	}
	ts, e := tenancy.Tenants(r.Context(), h.DB, auth.From(r.Context()).ID)
	if e != nil {
		return e
	}
	result := []tenancy.Tenant{}
	for _, t := range ts {
		if t.ID > page.Cursor {
			result = append(result, t)
		}
	}
	next := ""
	if len(result) > page.Limit {
		result = result[:page.Limit]
		next = httpx.Cursor(result[len(result)-1].ID)
	}
	httpx.JSON(w, 200, map[string]any{"items": result, "next_cursor": next})
	return nil
}
func tenantResource(ctx context.Context) policy.Resource {
	return policy.Resource{Type: "tenant", ID: auth.Tenant(ctx)}
}
func (h *Handler) logWrite(ctx context.Context, action, typ, id string, detail any) error {
	return audit.Append(ctx, db.Tx(ctx), auth.From(ctx), auth.Tenant(ctx), action, typ, id, detail)
}

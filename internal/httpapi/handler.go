package httpapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

type Handler struct {
	Temporal        temporalx.Client
	Objects         objstore.ObjectStore
	Locks           locks.Resolver
	uploadOnce      sync.Once
	uploadSemaphore chan struct{}
	Assets          *assets.Service
	DB              *db.DB
	Auth            *auth.Service
	Authz           policy.Authorizer
	Config          config.Config
	Logger          *slog.Logger
}
type endpoint func(http.ResponseWriter, *http.Request) error
type route struct {
	Pattern string
	Public  bool
	Handle  func(*Handler) endpoint
}

var routes = []route{
	{"POST /v1/sessions", false, func(h *Handler) endpoint { return h.createSession }},
	{"GET /v1/sessions", false, func(h *Handler) endpoint { return h.listSessions }},
	{"GET /v1/sessions/{s}", false, func(h *Handler) endpoint { return h.getSession }},
	{"PATCH /v1/sessions/{s}", false, func(h *Handler) endpoint { return h.patchSession }},
	{"POST /v1/sessions/{s}/archive", false, func(h *Handler) endpoint { return h.archiveSession }},
	{"POST /v1/sessions/{s}/messages", false, func(h *Handler) endpoint { return h.submitMessage }},
	{"GET /v1/sessions/{s}/messages", false, func(h *Handler) endpoint { return h.sessionChildren }},
	{"GET /v1/sessions/{s}/runs", false, func(h *Handler) endpoint { return h.sessionChildren }},
	{"GET /v1/sessions/{s}/artifacts", false, func(h *Handler) endpoint { return h.sessionChildren }},
	{"GET /v1/artifacts/{a}", false, func(h *Handler) endpoint { return h.getArtifact }},
	{"GET /v1/runs/{r}", false, func(h *Handler) endpoint { return h.getRun }},
	{"GET /v1/runs/{r}/segments", false, func(h *Handler) endpoint { return h.runSegments }},
	{"GET /v1/runs/{r}/stream-token", false, func(h *Handler) endpoint { return h.streamToken }},
	{"POST /v1/runs/{r}/cancel", false, func(h *Handler) endpoint { return h.controlRun }},
	{"POST /v1/runs/{r}/pause", false, func(h *Handler) endpoint { return h.controlRun }},
	{"POST /v1/runs/{r}/resume", false, func(h *Handler) endpoint { return h.controlRun }},
	{"POST /v1/runs/{r}/steer", false, func(h *Handler) endpoint { return h.controlRun }},
	{"GET /v1/interactions", false, func(h *Handler) endpoint { return h.listInteractions }},
	{"GET /v1/interactions/{i}", false, func(h *Handler) endpoint { return h.getInteraction }},
	{"POST /v1/interactions/{i}/resolve", false, func(h *Handler) endpoint { return h.resolveInteraction }},

	{"GET /v1/assets", false, func(h *Handler) endpoint { return h.listAssets }},
	{"POST /v1/assets", false, func(h *Handler) endpoint { return h.createAsset }},
	{"GET /v1/assets/{a}", false, func(h *Handler) endpoint { return h.getAsset }},
	{"PATCH /v1/assets/{a}", false, func(h *Handler) endpoint { return h.patchAsset }},
	{"DELETE /v1/assets/{a}", false, func(h *Handler) endpoint { return h.archiveAsset }},
	{"POST /v1/assets/{a}/versions", false, func(h *Handler) endpoint { return h.uploadAssetVersion }},
	{"GET /v1/assets/{a}/versions", false, func(h *Handler) endpoint { return h.listAssetVersions }},
	{"GET /v1/assets/{a}/versions/{v}", false, func(h *Handler) endpoint { return h.getAssetVersion }},
	{"GET /v1/assets/{a}/versions/{v}/files", false, func(h *Handler) endpoint { return h.listAssetFiles }},
	{"GET /v1/assets/{a}/versions/{v}/file", false, func(h *Handler) endpoint { return h.getAssetFile }},
	{"POST /v1/assets/{a}/versions/{v}:submit", false, func(h *Handler) endpoint { return h.submitAssetVersion }},
	{"POST /v1/assets/{a}/versions/{v}:withdraw", false, func(h *Handler) endpoint { return h.withdrawAssetVersion }},
	{"POST /v1/assets/{a}/versions/{v}:transition", false, func(h *Handler) endpoint { return h.transitionAssetVersion }},
	{"GET /v1/assets/{a}/versions/{v}/checks", false, func(h *Handler) endpoint { return h.listAssetChecks }},
	{"GET /v1/assets/{a}/channels", false, func(h *Handler) endpoint { return h.listAssetChannels }},
	{"POST /v1/assets/{a}/channels/{c}", false, func(h *Handler) endpoint { return h.setAssetChannel }},
	{"POST /v1/assets/{a}:restore", false, func(h *Handler) endpoint { return h.restoreAsset }},
	{"POST /v1/assets/{a}:suspend", false, func(h *Handler) endpoint { return h.suspendAsset }},
	{"GET /v1/assets/{a}/grants", false, func(h *Handler) endpoint { return h.listAssetGrants }},
	{"POST /v1/assets/{a}/grants", false, func(h *Handler) endpoint { return h.grantAsset }},
	{"DELETE /v1/assets/{a}/grants", false, func(h *Handler) endpoint { return h.revokeAssetGrant }},
	{"GET /v1/reviews", false, func(h *Handler) endpoint { return h.listAssetReviews }},
	{"POST /v1/reviews/{v}:decide", false, func(h *Handler) endpoint { return h.decideAssetReview }},
	{"POST /v1/workspaces/{w}/installs", false, func(h *Handler) endpoint { return h.installWorkspaceAsset }},
	{"GET /v1/workspaces/{w}/installs", false, func(h *Handler) endpoint { return h.listWorkspaceInstalls }},
	{"POST /v1/installs", false, func(h *Handler) endpoint { return h.installTenantAsset }},
	{"GET /v1/installs", false, func(h *Handler) endpoint { return h.listTenantInstalls }},
	{"DELETE /v1/installs/{i}", false, func(h *Handler) endpoint { return h.uninstallAsset }},
	{"POST /v1/workspaces/{w}/locks:preview", false, func(h *Handler) endpoint { return h.previewAssetLock }},

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
	{"DELETE /v1/admin/api-keys/{k}", false, func(h *Handler) endpoint { return h.deleteKey }},
	{"POST /v1/admin/service-principals", false, func(h *Handler) endpoint { return h.createServicePrincipal }},
	{"GET /v1/admin/service-principals", false, func(h *Handler) endpoint { return h.listServicePrincipals }},
	{"POST /v1/admin/invitations", false, func(h *Handler) endpoint { return h.createInvitation }},
	{"GET /v1/admin/invitations", false, func(h *Handler) endpoint { return h.listInvitations }},
	{"DELETE /v1/admin/invitations/{i}", false, func(h *Handler) endpoint { return h.deleteInvitation }},
	{"POST /v1/invitations/{token}:accept", false, func(h *Handler) endpoint { return h.acceptInvitation }},
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
	h.Auth.OnCreate = h.provisionIdentity
	mux := http.NewServeMux()
	registered := map[string]bool{}
	for _, route := range routes {
		pattern := route.Pattern
		if i := strings.LastIndex(pattern, "}:"); i >= 0 {
			pattern = pattern[:i+1]
		}
		if registered[pattern] {
			continue
		}
		registered[pattern] = true
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			selected := route
			var fn endpoint
			if pattern != route.Pattern {
				for _, candidate := range routes {
					i := strings.LastIndex(candidate.Pattern, "}:")
					if i < 0 || candidate.Pattern[:i+1] != pattern {
						continue
					}
					suffix := candidate.Pattern[i+1:]
					if !strings.HasSuffix(r.URL.Path, suffix) {
						continue
					}
					start := strings.LastIndex(candidate.Pattern[:i], "{")
					param := candidate.Pattern[start+1 : i]
					value, ok := strings.CutSuffix(r.PathValue(param), suffix)
					if !ok || value == "" {
						continue
					}
					if param != "token" {
						r.SetPathValue(param, value)
					}
					selected = candidate
					fn = candidate.Handle(h)
					break
				}
			} else {
				fn = selected.Handle(h)
			}
			if fn == nil {
				httpx.WriteError(w, httpx.E(404, "NOT_FOUND", "Route not found"))
				return
			}
			ip, _, _ := net.SplitHostPort(r.RemoteAddr)
			r = r.WithContext(audit.Context(r.Context(), ip, r.UserAgent()))
			rec := httpx.NewRecorder()
			var e error
			if selected.Public {
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
	return httpx.Middleware(h.Logger, h.Config.PublicOrigin, h.Config.CORS, mux)
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
		if errors.Is(e, pgx.ErrNoRows) {
			return httpx.E(401, "UNAUTHENTICATED", "Invalid credentials")
		}
		return e
	}
	ctx := auth.Context(r.Context(), p, tenant)
	r = r.WithContext(ctx)
	if r.Pattern == "POST /v1/invitations/{token}" {
		return fn(w, r)
	}
	if tenant == "" {
		if r.URL.Path != "/v1/me" && r.URL.Path != "/v1/tenants" {
			return httpx.E(403, "TENANT_REQUIRED", "X-Orbit-Tenant is required")
		}
		return fn(w, r)
	}
	if r.Pattern == "POST /v1/assets/{a}/versions" {
		slots := h.uploadSlots()
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		case <-ctx.Done():
			return ctx.Err()
		}
		r, e = h.stageUpload(r)
		if e != nil {
			return e
		}
		ctx = r.Context()
	}

	if isRunWrite(r) {
		return fn(w, r)
	}
	return h.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		if e := tenancy.Active(ctx, tx, p.ID); e != nil {
			return e
		}
		ctx = db.Context(ctx, tx)
		r = r.WithContext(ctx)

		return httpx.Idempotent(ctx, tx, tenant, p.ID, h.Auth.Replay, w, r, fn)
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
	p, e := h.Auth.JIT(r.Context(), auth.DevIssuer, input.Email, input.Email, input.Name, true)
	if e != nil {
		return e
	}
	ts, e := tenancy.Tenants(r.Context(), h.DB, p.ID)
	if e != nil {
		return e
	}
	if len(ts) == 0 {
		token, e := h.Auth.Signer.DevToken(p)
		if e != nil {
			return e
		}
		httpx.JSON(w, 200, map[string]any{"access_token": token, "expires_in": 3600})
		return nil
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
		return httpx.Idempotent(r.Context(), tx, tenant, p.ID, h.Auth.Replay, w, r, func(w http.ResponseWriter, r *http.Request) error {
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

// First JIT creation, personal tenant membership and OIDC login audit share one
// transaction. With personal tenants disabled there is no tenant audit to append.
func (h *Handler) provisionIdentity(ctx context.Context, tx pgx.Tx, p auth.Principal, tenant string) error {
	if tenant == "" {
		return nil
	}
	if err := tenancy.EnsurePersonal(ctx, tx, p, tenant); err != nil {
		return err
	}
	if h.Config.AuthMode == "oidc" {
		return audit.Append(ctx, tx, p, tenant, "login", "principal", p.ID, map[string]any{"mode": "oidc", "jit": true})
	}
	return nil
}

func isRunWrite(r *http.Request) bool {
	p := r.URL.Path
	return (r.Method == "PATCH" && strings.HasPrefix(p, "/v1/sessions/")) || (r.Method == "POST" && (strings.HasPrefix(p, "/v1/runs/") || strings.HasPrefix(p, "/v1/interactions/") || strings.HasPrefix(p, "/v1/sessions/")))
}

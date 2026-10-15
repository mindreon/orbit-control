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
	Pattern        string
	Public         bool
	Handle         func(*Handler) endpoint
	TwoPhase       bool
	TenantOptional bool
}

var routes = []route{
	{
		Pattern: "POST /v1/sessions",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.createSession
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/sessions",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listSessions
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/sessions/{s}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.getSession
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "PATCH /v1/sessions/{s}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.patchSession
		},
		TwoPhase:       true,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/sessions/{s}/archive",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.archiveSession
		},
		TwoPhase:       true,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/sessions/{s}/messages",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.submitMessage
		},
		TwoPhase:       true,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/sessions/{s}/messages",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.sessionChildren
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/sessions/{s}/runs",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.sessionChildren
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/sessions/{s}/artifacts",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.sessionChildren
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/artifacts/{a}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.getArtifact
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/runs/{r}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.getRun
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/runs/{r}/segments",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.runSegments
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/runs/{r}/stream-token",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.streamToken
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/runs/{r}/cancel",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.controlRun
		},
		TwoPhase:       true,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/runs/{r}/pause",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.controlRun
		},
		TwoPhase:       true,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/runs/{r}/resume",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.controlRun
		},
		TwoPhase:       true,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/runs/{r}/steer",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.controlRun
		},
		TwoPhase:       true,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/interactions",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listInteractions
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/interactions/{i}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.getInteraction
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/interactions/{i}/resolve",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.resolveInteraction
		},
		TwoPhase:       true,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listAssets
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.createAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets/{a}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.getAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "PATCH /v1/assets/{a}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.patchAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "DELETE /v1/assets/{a}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.archiveAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets/{a}/versions",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.uploadAssetVersion
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets/{a}/versions",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listAssetVersions
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets/{a}/versions/{v}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.getAssetVersion
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets/{a}/versions/{v}/files",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listAssetFiles
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets/{a}/versions/{v}/file",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.getAssetFile
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets/{a}/versions/{v}:submit",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.submitAssetVersion
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets/{a}/versions/{v}:withdraw",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.withdrawAssetVersion
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets/{a}/versions/{v}:transition",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.transitionAssetVersion
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets/{a}/versions/{v}/checks",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listAssetChecks
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets/{a}/channels",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listAssetChannels
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets/{a}/channels/{c}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.setAssetChannel
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets/{a}:restore",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.restoreAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets/{a}:suspend",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.suspendAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/assets/{a}/grants",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listAssetGrants
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/assets/{a}/grants",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.grantAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "DELETE /v1/assets/{a}/grants",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.revokeAssetGrant
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/reviews",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listAssetReviews
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/reviews/{v}:decide",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.decideAssetReview
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/workspaces/{w}/installs",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.installWorkspaceAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/workspaces/{w}/installs",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listWorkspaceInstalls
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/installs",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.installTenantAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/installs",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listTenantInstalls
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "DELETE /v1/installs/{i}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.uninstallAsset
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/workspaces/{w}/locks:preview",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.previewAssetLock
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /health",
		Public:  true,
		Handle: func(h *Handler) endpoint {
			return h.health
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /ready",
		Public:  true,
		Handle: func(h *Handler) endpoint {
			return h.ready
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/auth/config",
		Public:  true,
		Handle: func(h *Handler) endpoint {
			return h.authConfig
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/auth/dev/token",
		Public:  true,
		Handle: func(h *Handler) endpoint {
			return h.devToken
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/me",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.me
		},
		TwoPhase:       false,
		TenantOptional: true,
	},
	{
		Pattern: "GET /v1/tenants",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.tenants
		},
		TwoPhase:       false,
		TenantOptional: true,
	},
	{
		Pattern: "GET /v1/workspaces",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listWorkspaces
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/workspaces",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.createWorkspace
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/workspaces/{w}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.getWorkspace
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "PATCH /v1/workspaces/{w}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.patchWorkspace
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/workspaces/{w}/members",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listWorkspaceMembers
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/workspaces/{w}/members",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.addWorkspaceMember
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "DELETE /v1/workspaces/{w}/members/{p}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.deleteWorkspaceMember
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/admin/members",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listMembers
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/admin/members",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.addMember
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "PATCH /v1/admin/members/{p}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.patchMember
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/admin/api-keys",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listKeys
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/admin/api-keys",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.createKey
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "DELETE /v1/admin/api-keys/{k}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.deleteKey
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/admin/service-principals",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.createServicePrincipal
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/admin/service-principals",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listServicePrincipals
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/admin/invitations",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.createInvitation
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "GET /v1/admin/invitations",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listInvitations
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "DELETE /v1/admin/invitations/{i}",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.deleteInvitation
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
	{
		Pattern: "POST /v1/invitations/{token}:accept",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.acceptInvitation
		},
		TwoPhase:       false,
		TenantOptional: true,
	},
	{
		Pattern: "GET /v1/admin/audit",
		Public:  false,
		Handle: func(h *Handler) endpoint {
			return h.listAudit
		},
		TwoPhase:       false,
		TenantOptional: false,
	},
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
				e = h.protected(rec, r, fn, selected)
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
func (h *Handler) protected(w http.ResponseWriter, r *http.Request, fn endpoint, selected route) error {
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
	if selected.Pattern == "POST /v1/invitations/{token}:accept" {
		return fn(w, r)
	}
	if tenant == "" {
		if !selected.TenantOptional {
			return httpx.E(403, "TENANT_REQUIRED", "X-Orbit-Tenant is required")
		}
		return fn(w, r)
	}
	if r.Pattern == "POST /v1/assets/{a}/versions" {
		slots := h.uploadSlots()
		select {
		case slots <- struct{}{}:
			defer func() {
				<-slots
			}()
		case <-ctx.Done():
			return ctx.Err()
		}
		r, e = h.stageUpload(r)
		if e != nil {
			return e
		}
		ctx = r.Context()
	}

	if selected.TwoPhase {
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
	return h.devTenantToken(w, r, p, tenant)
}
func (h *Handler) devTenantToken(w http.ResponseWriter, r *http.Request, p auth.Principal, tenant string) error {
	return h.DB.WithTenant(
		r.Context(),
		tenant,
		func(tx pgx.Tx) error {
			return httpx.Idempotent(
				r.Context(),
				tx,
				tenant,
				p.ID,
				h.Auth.Replay,
				w,
				r,
				func(
					w http.ResponseWriter,
					r *http.Request,
				) error {
					if e := audit.Append(
						r.Context(),
						tx,
						p,
						tenant,
						"login",
						"principal",
						p.ID,
						map[string]any{"mode": "dev"},
					); e != nil {
						return e
					}
					token, e := h.Auth.Signer.DevToken(p)
					if e != nil {
						return e
					}
					httpx.JSON(w, 200, map[string]any{"access_token": token, "expires_in": 3600})
					return nil
				},
			)
		},
	)
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
	httpx.JSON(
		w,
		200,
		map[string]any{"principal": p, "tenants": ts, "current_tenant": tenant, "roles": roles},
	)
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
		return audit.Append(
			ctx,
			tx,
			p,
			tenant,
			"login",
			"principal",
			p.ID,
			map[string]any{"mode": "oidc", "jit": true},
		)
	}
	return nil
}

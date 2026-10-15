// Package kernelapi serves run-scoped capabilities on the internal listener.
package kernelapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/models"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/secrets"
	"github.com/mindreon/orbit-control/internal/usage"
)

type Handler struct {
	DB                 *db.DB
	Signer             *auth.CapabilitySigner
	Secrets            secrets.Keys
	Obj                objstore.ObjectStore
	ServiceTokenSHA256 string
}
type endpoint func(http.ResponseWriter, *http.Request, auth.CapabilityClaims) error

func (h *Handler) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/v1/tokens", func(w http.ResponseWriter, r *http.Request) {
		rec := httpx.NewRecorder()
		if err := h.tokens(rec, r); err != nil {
			httpx.WriteError(w, err)
			return
		}
		rec.FlushTo(w)
	})
	for _, route := range []struct {
		pattern  string
		purposes string
		fn       endpoint
	}{
		{pattern: "GET /internal/v1/runs/{run}/lock", purposes: "segment sandbox", fn: h.lock},
		{
			pattern:  "GET /internal/v1/runs/{run}/policy",
			purposes: "segment",
			fn:       h.policy,
		},
		{
			pattern:  "GET /internal/v1/runs/{run}/model",
			purposes: "segment",
			fn:       h.model,
		},
		{
			pattern:  "POST /internal/v1/runs/{run}/secret-grants",
			purposes: "segment",
			fn:       h.secretGrants,
		},
		{
			pattern:  "GET /internal/v1/runs/{run}/checkpoint-key",
			purposes: "segment",
			fn:       h.checkpointKey,
		},
		{
			pattern:  "GET /internal/v1/runs/{run}/content",
			purposes: "segment sandbox",
			fn:       h.content,
		},
		{
			pattern:  "POST /internal/v1/runs/{run}/usage",
			purposes: "segment",
			fn:       h.reportUsage,
		},
	} {
		mux.HandleFunc(route.pattern, func(w http.ResponseWriter, r *http.Request) {
			rec := httpx.NewRecorder()
			token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			claims, err := h.Signer.VerifyCapability(token)
			if err != nil {
				httpx.WriteError(w, httpx.E(401, "TOKEN_INVALID", "Invalid capability"))
				return
			}
			if claims.Run != r.PathValue("run") ||
				!strings.Contains(" "+route.purposes+" ", " "+claims.Purpose+" ") {
				httpx.WriteError(w, httpx.E(403, "TOKEN_SUPERSEDED", "Capability does not authorize request"))
				return
			}
			err = h.DB.WithTenant(r.Context(), claims.Tenant, func(tx pgx.Tx) error {
				ctx := db.Context(r.Context(), tx)
				if err := check(ctx, claims); err != nil {
					return err
				}
				return route.fn(rec, r.WithContext(ctx), claims)
			})
			if err != nil {
				httpx.WriteError(w, err)
				return
			}
			rec.FlushTo(w)
		})
	}
	return mux
}
func check(ctx context.Context, c auth.CapabilityClaims) error {
	var valid bool
	if c.Purpose == "segment" && c.Member != "" && c.Seq > 0 && c.Attempt > 0 {
		if err := db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1
   FROM runs r
   JOIN run_segments s ON s.tenant_id=r.tenant_id
   AND s.run_id=r.id
   WHERE r.tenant_id=$1
   AND r.id=$2
   AND r.ended_at IS NULL
   AND s.member=$3
   AND s.seq=$4
   AND s.status='running')`, c.Tenant, c.Run, c.Member, c.Seq).Scan(&valid); err != nil {
			return err
		}
	}
	if c.Purpose == "sandbox" && c.LeaseID != "" {
		if err := db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1
   FROM runs r
   JOIN sandboxes s ON s.tenant_id=r.tenant_id
   AND s.session_id=r.session_id
   WHERE r.tenant_id=$1
   AND r.id=$2
   AND r.ended_at IS NULL
   AND s.isolation_key=$3
   AND s.isolation_key='t-'||r.tenant_id||'-s-'||r.session_id
   AND s.state NOT IN ('destroyed','failed'))`, c.Tenant, c.Run, c.LeaseID).Scan(&valid); err != nil {
			return err
		}
	}
	if !valid {
		return httpx.E(403, "TOKEN_SUPERSEDED", "Run or segment is no longer current")
	}
	return nil
}
func decode(r *http.Request, name string, out any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return httpx.E(413, "BODY_TOO_LARGE", "Request exceeds limit")
	}
	if err = contract.Validate(name, raw); err != nil {
		return httpx.E(422, "INVALID_INPUT", "Invalid "+name)
	}
	if err = json.Unmarshal(raw, out); err != nil {
		return httpx.E(422, "INVALID_INPUT", "Invalid "+name)
	}
	return nil
}
func ServiceCredentialMatches(token, expected string) bool {
	digest := sha256.Sum256([]byte(token))
	want, err := hex.DecodeString(expected)
	return err == nil && len(want) == 32 && subtle.ConstantTimeCompare(digest[:], want) == 1
}
func (h *Handler) tokens(w http.ResponseWriter, r *http.Request) error {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || !ServiceCredentialMatches(token, h.ServiceTokenSHA256) {
		return httpx.E(401, "TOKEN_INVALID", "Invalid service credential")
	}
	var req contract.TokenRequest
	if err := decode(r, "TokenRequest", &req); err != nil {
		return err
	}
	c := auth.CapabilityClaims{Tenant: req.TenantId, Run: req.RunId, Purpose: string(req.Purpose)}
	if req.Member != nil {
		c.Member = string(*req.Member)
	}
	if req.Seq != nil {
		c.Seq = *req.Seq
	}
	if req.Attempt != nil {
		c.Attempt = *req.Attempt
	}
	if req.LeaseId != nil {
		c.LeaseID = *req.LeaseId
	}
	return h.DB.WithTenant(r.Context(), c.Tenant, func(tx pgx.Tx) error {
		ctx := db.Context(r.Context(), tx)
		if err := check(ctx, c); err != nil {
			return err
		}
		signed, err := h.Signer.IssueCapability(c)
		if err != nil {
			return httpx.E(422, "INVALID_INPUT", "Invalid token identity")
		}
		claims, err := h.Signer.VerifyCapability(signed)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, contract.TokenResponse{Token: contract.Secret(signed), ExpiresAt: claims.ExpiresAt.Time})
		return nil
	})
}
func getLock(ctx context.Context, c auth.CapabilityClaims) (contract.RunLock, error) {
	var lock contract.RunLock
	var raw []byte
	err := db.Tx(ctx).QueryRow(ctx, `SELECT l.lock
 FROM run_locks l
 JOIN runs r ON r.lock_id=l.id
 AND r.tenant_id=l.tenant_id
 WHERE r.tenant_id=$1
 AND r.id=$2`, c.Tenant, c.Run).Scan(&raw)
	if err != nil {
		return lock, err
	}
	err = json.Unmarshal(raw, &lock)
	return lock, err
}
func (h *Handler) lock(w http.ResponseWriter, r *http.Request, c auth.CapabilityClaims) error {
	lock, err := getLock(r.Context(), c)
	if err != nil {
		return err
	}
	out := contract.GetLockResponse{Lock: lock}
	for _, entry := range lock.Entries {
		for _, f := range entry.Files {
			item := contract.LockFileContent{
				Kind:    entry.Kind,
				Ref:     entry.Ref,
				Version: entry.Version,
				Path:    f.Path,
				Sha256:  f.Sha256,
			}
			if f.SizeBytes <= 64<<10 {
				data, e := h.Obj.Get(r.Context(), c.Tenant, f.Sha256, 64<<10)
				if e != nil {
					return e
				}
				if utf8.Valid(data) {
					item.Text = new(string(data))
				}
			}
			if item.Text == nil {
				key, e := objstore.Key(c.Tenant, f.Sha256)
				if e != nil {
					return e
				}
				url, e := h.Obj.PresignGet(r.Context(), c.Tenant, key, 5*time.Minute)
				if e != nil {
					return e
				}
				item.BlobUrl = new(contract.Secret(url))
			}
			out.Contents = append(out.Contents, item)
		}
	}
	httpx.JSON(w, 200, out)
	return nil
}
func (h *Handler) model(w http.ResponseWriter, r *http.Request, c auth.CapabilityClaims) error {
	out, profileID, err := models.Credentials(r.Context(), c.Tenant, r.URL.Query().Get("profile"), c.ExpiresAt.Time, h.Secrets)
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.E(404, "MODEL_PROFILE_NOT_FOUND", "Model profile not found")
	}
	if err != nil {
		return err
	}
	if err := models.Bind(r.Context(), c, profileID, out); err != nil {
		return err
	}
	httpx.JSON(w, 200, out)
	return nil
}
func (h *Handler) checkpointKey(w http.ResponseWriter, r *http.Request, c auth.CapabilityClaims) error {
	key, err := h.Secrets.DeriveDEK(c.Tenant, "checkpoints", h.Secrets.Current)
	if err != nil {
		return err
	}
	httpx.JSON(
		w,
		200,
		contract.CheckpointKeyResponse{
			KeyVersion: h.Secrets.Current,
			Key:        contract.Secret(base64.StdEncoding.EncodeToString(key)),
		},
	)
	return nil
}
func (h *Handler) content(w http.ResponseWriter, r *http.Request, c auth.CapabilityClaims) error {
	ref := r.URL.Query().Get("ref")
	if !strings.HasPrefix(ref, "t/"+c.Tenant+"/runs/"+c.Run+"/") || strings.Contains(ref, "..") {
		return httpx.E(403, "CONTENT_FORBIDDEN", "Content is outside this run")
	}
	data, mime, err := h.Obj.GetObject(r.Context(), c.Tenant, ref, 8<<20)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", mime)
	_, err = w.Write(data)
	return err
}
func (h *Handler) secretGrants(w http.ResponseWriter, r *http.Request, c auth.CapabilityClaims) error {
	var req contract.SecretGrantRequest
	if err := decode(r, "SecretGrantRequest", &req); err != nil {
		return err
	}
	lock, err := getLock(r.Context(), c)
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	for _, m := range lock.Members {
		if string(m.Name) == c.Member {
			for _, s := range m.EffectivePermissions.Secrets {
				allowed[s.Name] = true
			}
		}
	}
	for _, name := range req.Names {
		if !allowed[name] {
			return httpx.E(403, "SECRET_NOT_DECLARED", "Secret is not declared by this member")
		}
	}
	out := contract.SecretGrantResponse{}
	for _, name := range req.Names {
		var id string
		if err = db.Tx(r.Context()).QueryRow(r.Context(),
			`SELECT id::text FROM secrets WHERE tenant_id=$1 AND name=$2 AND scope='tenant'`, c.Tenant, name).Scan(&id); err != nil {
			return err
		}
		plain, e := h.Secrets.Get(r.Context(), c.Tenant, id)
		if e != nil {
			return e
		}
		out.Grants = append(out.Grants, contract.SecretGrant{Name: name, Value: contract.Secret(plain), ExpiresAt: c.ExpiresAt.Time})
	}
	var actor, kind string
	if err = db.Tx(r.Context()).QueryRow(r.Context(), `SELECT p.id::text,p.kind
 FROM runs r
 JOIN principals p ON p.id=r.created_by
 WHERE r.tenant_id=$1
 AND r.id=$2`, c.Tenant, c.Run).Scan(&actor, &kind); err != nil {
		return err
	}
	if err = audit.Append(
		r.Context(),
		db.Tx(r.Context()),
		auth.Principal{ID: actor, Kind: kind},
		c.Tenant,
		"secret.grant",
		"run",
		c.Run,
		map[string]any{"names": req.Names},
	); err != nil {
		return err
	}
	httpx.JSON(w, 200, out)
	return nil
}
func (h *Handler) policy(w http.ResponseWriter, r *http.Request, c auth.CapabilityClaims) error {
	out := contract.PolicySnapshot{}
	var session, workspace, principal string
	if err := db.Tx(r.Context()).QueryRow(r.Context(),
		`SELECT r.session_id::text,s.workspace_id::text,r.created_by::text
 FROM runs r
 JOIN sessions s ON s.id=r.session_id
 WHERE r.tenant_id=$1
 AND r.id=$2`, c.Tenant, c.Run).Scan(&session, &workspace, &principal); err != nil {
		return err
	}
	rows, err := db.Tx(r.Context()).Query(r.Context(), `SELECT body
 FROM policies
 WHERE tenant_id=$1
 AND kind='tool_rules'
 AND status='active'
 AND ((scope_type='tenant' AND scope_id=$1) OR (scope_type='workspace' AND scope_id=$2))
 ORDER BY CASE scope_type WHEN 'tenant' THEN 0 ELSE 1 END,version,id`, c.Tenant, workspace)
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		var body struct {
			ToolRules []contract.ToolRule `json:"tool_rules"`
		}
		if err = json.Unmarshal(raw, &body); err != nil {
			rows.Close()
			return err
		}
		out.ToolRules = append(out.ToolRules, body.ToolRules...)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = db.Tx(r.Context()).Query(r.Context(),
		`SELECT tool,arg_pattern,scope_type,interaction_id::text,expires_at
 FROM permission_grants
 WHERE tenant_id=$1
 AND decision='allow'
 AND expires_at>now()
 AND ((scope_type='session' AND scope_id=$2) OR (scope_type='user' AND scope_id=$3) OR (scope_type='workspace' AND scope_id=$4))
 ORDER BY created_at,id`, c.Tenant, session, principal, workspace)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var g contract.PermissionGrant
		if err = rows.Scan(&g.Tool, &g.ArgPattern, &g.Scope, &g.InteractionId, &g.ExpiresAt); err != nil {
			return err
		}
		out.Grants = append(out.Grants, g)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	httpx.JSON(w, 200, out)
	return nil
}
func (h *Handler) reportUsage(w http.ResponseWriter, r *http.Request, c auth.CapabilityClaims) error {
	var req contract.UsageReport
	if err := decode(r, "UsageReport", &req); err != nil {
		return err
	}
	out, err := usage.ReportModelUsage(r.Context(), c, req)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, out)
	return nil
}

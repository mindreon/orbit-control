package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/mail"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

type Service struct {
	// OnCreate provisions tenant membership/audit in the identity transaction.
	OnCreate func(context.Context, pgx.Tx, Principal, string) error
	// OnWorkspaceSync audits a baize project sync from inside its transaction;
	// created/granted report rows actually inserted, not repeat syncs. audit
	// imports auth, so the audit write itself must live outside this package:
	// the hook is nil by design and wired in httpapi Router().
	OnWorkspaceSync func(ctx context.Context, tx pgx.Tx, p Principal, tenant, externalRef, workspace string, created, granted bool) error
	DB              *db.DB
	Signer          *Signer
	Capability      *CapabilitySigner
	Stream          *StreamSigner
	Replay          httpx.ReplayKeys
	Config          config.Config
	verifier        *oidc.IDTokenVerifier
	iam             iamLookup
	projects        *iamProjectMemo
	defaultMu       sync.Mutex
	defaultTenant   string
	Logger          *slog.Logger
	wg              sync.WaitGroup
	lastUsed        sync.Map
}

func New(ctx context.Context, d *db.DB, c config.Config, l *slog.Logger) (*Service, error) {
	s, e := LoadSigner(c.KeyFile)
	if e != nil {
		return nil, e
	}
	capKey, e := LoadSigner(c.CapabilityKeyFile)
	if e != nil {
		return nil, fmt.Errorf("ORBIT_CAPABILITY_KEY_FILE: %w", e)
	}
	if s.key.Equal(capKey.key) {
		return nil, fmt.Errorf("ORBIT_CAPABILITY_KEY_FILE must use a different key from ORBIT_SIGNING_KEY_FILE")
	}
	capability, e := NewCapabilitySigner(capKey.key, c.CapabilityKeyID, c.CapabilityPublicKeys)
	if e != nil {
		return nil, e
	}
	streamKey, e := LoadSigner(c.StreamKeyFile)
	if e != nil {
		return nil, fmt.Errorf("ORBIT_STREAM_KEY_FILE: %w", e)
	}
	if streamKey.key.Equal(s.key) || streamKey.key.Equal(capKey.key) {
		return nil, fmt.Errorf("ORBIT_STREAM_KEY_FILE must use a different key from authentication keys")
	}
	stream, e := NewStreamSigner(streamKey.key, c.StreamKeyID, c.StreamPublicKeys)
	if e != nil {
		return nil, e
	}
	replay, e := httpx.NewReplayKeys(c.SecretsKeyVersion, c.SecretsKEKs)
	if e != nil {
		return nil, e
	}
	a := &Service{DB: d, Signer: s, Capability: capability, Stream: stream, Replay: replay, Config: c, Logger: l}
	switch c.AuthMode {
	case "oidc":
		p, e := oidc.NewProvider(ctx, c.Issuer)
		if e != nil {
			return nil, fmt.Errorf("ORBIT_OIDC_ISSUER discovery failed")
		}
		a.verifier = p.Verifier(&oidc.Config{ClientID: c.Audience})
	case "iam":
		a.iam = newCachedLookup(newHTTPLookup(c.IAMBaseURL, nil))
		a.projects = newProjectMemo()
	default:
		l.Warn("development authentication enabled", "auth_mode", "dev")
	}
	return a, nil
}
func (a *Service) Close() { a.wg.Wait() }
func ValidEmail(s string) bool {
	v, e := mail.ParseAddress(s)
	return e == nil && v.Address == s && len(s) <= 254
}

// DefaultTenantID resolves the configured tenant, allowing it to be provisioned later.
func (a *Service) DefaultTenantID(ctx context.Context) (string, error) {
	id, err := a.resolveDefaultTenant(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// resolveDefaultTenant looks the slug up at login, not at startup: on a fresh
// database the tenant is created by seed-dev after control is already running.
func (a *Service) resolveDefaultTenant(ctx context.Context) (string, error) {
	if a.Config.DefaultTenant == "" {
		return "", nil
	}
	a.defaultMu.Lock()
	defer a.defaultMu.Unlock()
	if a.defaultTenant != "" {
		return a.defaultTenant, nil
	}
	var id string
	err := a.DB.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT id::text FROM tenants WHERE slug=$1", a.Config.DefaultTenant).Scan(&id)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("ORBIT_DEFAULT_TENANT: tenant slug %q not found: %w", a.Config.DefaultTenant, err)
	}
	if err != nil {
		return "", fmt.Errorf("ORBIT_DEFAULT_TENANT lookup: %w", err)
	}
	a.defaultTenant = id
	return id, nil
}

// JIT reads existing identities without writes unless default membership is enabled.
// First creation and its optional tenant/audit commit atomically; concurrent logins converge.
func (a *Service) JIT(ctx context.Context, issuer, subject, email, name string, verified bool) (Principal, error) {
	p := Principal{ID: ids.New(), Kind: "user", Email: email, DisplayName: name}
	if subject == "" {
		return p, fmt.Errorf("empty subject")
	}
	if name == "" {
		p.DisplayName = email
	}
	lookup := func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT id::text,kind,email,display_name,email_verified
 FROM principals
 WHERE idp_subject=$1
 AND status='active'`, issuer+"|"+subject).
			Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName, &p.EmailVerified)
	}
	provision := func(tx pgx.Tx, tenant string) error {
		if a.OnCreate != nil {
			return a.OnCreate(ctx, tx, p, tenant)
		}
		if tenant != "" {
			return fmt.Errorf("tenant provisioner required")
		}
		return nil
	}
	defaultTenant, err := a.resolveDefaultTenant(ctx)
	if err != nil {
		return p, err
	}
	err = a.DB.Global(ctx, lookup)
	if !errors.Is(err, pgx.ErrNoRows) {
		if err == nil && defaultTenant != "" {
			err = a.DB.WithTenant(ctx, defaultTenant, func(tx pgx.Tx) error {
				return provision(tx, defaultTenant)
			})
		}
		return p, err
	}
	tenant := defaultTenant
	if a.Config.PersonalTenants {
		tenant = ids.New()
	}
	create := func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `INSERT INTO principals(id,kind,email,display_name,idp_subject,email_verified)
 VALUES($1,'user',$2,$3,$4,$5)
 ON CONFLICT(idp_subject) DO NOTHING
 RETURNING id::text,kind,email,display_name,email_verified`, p.ID, email, p.DisplayName, issuer+"|"+subject,
			verified).
			Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName, &p.EmailVerified)
		if errors.Is(err, pgx.ErrNoRows) {
			if err = lookup(tx); err != nil {
				return err
			}
			if defaultTenant != "" {
				return provision(tx, tenant)
			}
			return nil
		}
		if err != nil {
			return err
		}
		return provision(tx, tenant)
	}
	if tenant != "" {
		err = a.DB.WithTenant(ctx, tenant, create)
	} else {
		err = a.DB.Global(ctx, create)
	}
	return p, err
}
func (a *Service) Authenticate(ctx context.Context, token, tenant string) (Principal, error) {
	if strings.HasPrefix(token, "orbit_") {
		return a.apiKey(ctx, token, tenant)
	}
	if a.Config.AuthMode == "iam" {
		return a.authenticateIAM(ctx, token)
	}
	if a.Config.AuthMode == "dev" {
		c, e := a.Signer.VerifyDev(token)
		if e != nil {
			return Principal{}, httpx.E(401, "UNAUTHENTICATED", "Invalid token")
		}
		var p Principal
		e = a.DB.Global(ctx, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT id::text,kind,email,display_name,email_verified
 FROM principals
 WHERE id=$1
 AND status='active'`, c.Subject).
				Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName, &p.EmailVerified)
		})
		return p, e
	}
	c, e := a.verifier.Verify(ctx, token)
	if e != nil {
		return Principal{}, httpx.E(401, "UNAUTHENTICATED", "Invalid token")
	}
	var fields struct {
		Email         string `json:"email"`
		Name          string `json:"name"`
		EmailVerified bool   `json:"email_verified"`
	}
	if e = c.Claims(&fields); e != nil {
		return Principal{}, httpx.E(401, "UNAUTHENTICATED", "Invalid token claims")
	}
	return a.JIT(ctx, c.Issuer, c.Subject, fields.Email, fields.Name, fields.EmailVerified)
}

// authenticateIAM validates the bearer token against baize IAM and maps the user
// onto a principal. The issuer is fixed: orbit trusts IAM's user id, not the token.
func (a *Service) authenticateIAM(ctx context.Context, token string) (Principal, error) {
	id, err := a.iam.Lookup(ctx, token)
	if err != nil {
		return Principal{}, iamError(err)
	}
	// principals.email is NOT NULL but unconstrained (migrations/00001_r1.sql):
	// an empty email is stored as "" and idp_subject (baize-iam|<user id>) stays the
	// identity key, so a missing IAM email never collides or fails the insert.
	p, err := a.JIT(ctx, "baize-iam", id.UserID, id.Email, id.Name, id.Verified)
	if err != nil {
		return Principal{}, err
	}
	// Every iam user token is restricted to its current project's workspace; with
	// no project at all there is no workspace data. API keys never set this.
	p.IAMRestricted = true
	if id.ProjectID == "" {
		return p, nil
	}
	workspace, err := a.currentWorkspace(ctx, p, id.ProjectID, id.ProjectName)
	if err != nil {
		return Principal{}, err
	}
	p.IAMWorkspace = workspace
	return p, nil
}

// baizeProjectRef prefixes the baize project id stored in workspaces.external_ref.
func baizeProjectRef(project string) string { return "baize-project:" + project }

// currentWorkspace upserts the workspace mapped from the token's current baize
// project, with the caller as a ws.editor member. A hit in the in-process memo
// skips the write for a minute.
func (a *Service) currentWorkspace(ctx context.Context, p Principal, project, name string) (string, error) {
	if workspace, ok := a.projects.get(p.ID, project); ok {
		return workspace, nil
	}
	tenant, err := a.resolveDefaultTenant(ctx)
	if err != nil || tenant == "" {
		// Without the default tenant there is no tenant to upsert into.
		return "", err
	}
	if name == "" {
		name = project
	}
	var workspace string
	err = a.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		var e error
		workspace, e = a.upsertProjectWorkspace(ctx, tx, p, tenant, project, name)
		return e
	})
	if err != nil {
		return "", err
	}
	a.projects.set(p.ID, project, workspace)
	return workspace, nil
}

// upsertProjectWorkspace creates or renames the workspace for one baize project and
// makes sure the caller is a member with ws.editor. Every write is ON CONFLICT, so
// concurrent first requests for the same project converge on one row. Only actual
// inserts are reported to OnWorkspaceSync; repeat syncs audit nothing.
func (a *Service) upsertProjectWorkspace(ctx context.Context, tx pgx.Tx, p Principal, tenant, project, name string) (string, error) {
	var workspace string
	var created bool
	err := tx.QueryRow(ctx, `INSERT INTO workspaces(id,tenant_id,name,external_ref)
 VALUES($1,$2,$3,$4)
 ON CONFLICT(tenant_id,external_ref) WHERE external_ref IS NOT NULL DO UPDATE
 SET name=EXCLUDED.name
 RETURNING id::text, xmax = 0`, ids.New(), tenant, name, baizeProjectRef(project)).Scan(&workspace, &created)
	if err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO workspace_members(tenant_id,workspace_id,principal_id)
 VALUES($1,$2,$3)
 ON CONFLICT(tenant_id,workspace_id,principal_id) DO NOTHING`, tenant, workspace, p.ID); err != nil {
		return "", err
	}
	binding, err := tx.Exec(ctx, `INSERT INTO role_bindings(id,tenant_id,principal_type,principal_id,role,scope_type,scope_id,created_by)
 VALUES($1,$2,$3,$4,'ws.editor','workspace',$5,$4)
 ON CONFLICT(tenant_id,principal_type,principal_id,role,scope_type,scope_id) DO NOTHING`,
		ids.New(), tenant, p.Kind, p.ID, workspace)
	if err != nil {
		return "", err
	}
	if a.OnWorkspaceSync != nil {
		if err = a.OnWorkspaceSync(ctx, tx, p, tenant, baizeProjectRef(project), workspace, created, binding.RowsAffected() == 1); err != nil {
			return "", err
		}
	}
	return workspace, nil
}
func GenerateAPIKey() (token, prefix string, hash []byte, err error) {
	b := make([]byte, 32)
	if _, err = rand.Read(b); err != nil {
		return
	}
	randomPrefix := make([]byte, 8)
	if _, err = rand.Read(randomPrefix); err != nil {
		return
	}
	prefix = hex.EncodeToString(randomPrefix)
	token = "orbit_" + prefix + "_" + base64.RawURLEncoding.EncodeToString(b)
	h := sha256.Sum256([]byte(token))
	hash = h[:]
	return
}
func (a *Service) apiKey(ctx context.Context, token, tenant string) (Principal, error) {
	if !ids.Valid(tenant) {
		return Principal{}, httpx.E(403, "TENANT_REQUIRED", "API keys require X-Orbit-Tenant")
	}
	h := sha256.Sum256([]byte(token))
	var p Principal
	var keyID string
	e := a.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT p.id::text,p.kind,p.email,p.display_name,k.scopes,k.id::text,p.email_verified
 FROM api_keys k
 JOIN principals p ON p.id=k.principal_id
 JOIN memberships m ON m.principal_id=p.id
 AND m.tenant_id=k.tenant_id
 WHERE k.hash=$1
 AND k.expires_at>now()
 AND p.status='active'
 AND m.status='active'`, h[:]).
			Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName, &p.Scopes, &keyID, &p.EmailVerified)
	})
	if errors.Is(e, pgx.ErrNoRows) {
		return p, httpx.E(401, "UNAUTHENTICATED", "Invalid API key")
	}
	if e != nil {
		return p, e
	}
	p.APIKey = true
	now := time.Now()
	old, loaded := a.lastUsed.LoadOrStore(keyID, now)
	update := !loaded
	if loaded && now.Sub(old.(time.Time)) >= time.Minute {
		update = a.lastUsed.CompareAndSwap(keyID, old, now)
	}
	if update {
		time.AfterFunc(time.Minute, func() { a.lastUsed.CompareAndDelete(keyID, now) })
		a.wg.Go(func() {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			e := a.DB.WithTenant(c, tenant, func(tx pgx.Tx) error {
				_, e := tx.Exec(c, `UPDATE api_keys
 SET last_used_at=now()
 WHERE id=$1
 AND (last_used_at IS NULL OR last_used_at<now()-interval '1 minute')`, keyID)
				return e
			})
			if e != nil {
				a.Logger.Warn("API key last-use update failed")
			}
		})
	}
	return p, nil
}

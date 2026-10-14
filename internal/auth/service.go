package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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
	DB       *db.DB
	Signer   *Signer
	Config   config.Config
	verifier *oidc.IDTokenVerifier
	Logger   *slog.Logger
	wg       sync.WaitGroup
	lastUsed sync.Map
}

func New(ctx context.Context, d *db.DB, c config.Config, l *slog.Logger) (*Service, error) {
	s, e := LoadSigner(c.KeyFile)
	if e != nil {
		return nil, e
	}
	a := &Service{DB: d, Signer: s, Config: c, Logger: l}
	if c.AuthMode == "oidc" {
		p, e := oidc.NewProvider(ctx, c.Issuer)
		if e != nil {
			return nil, fmt.Errorf("ORBIT_OIDC_ISSUER discovery failed")
		}
		a.verifier = p.Verifier(&oidc.Config{ClientID: c.Audience})
	} else {
		l.Warn("development authentication enabled", "auth_mode", "dev")
	}
	return a, nil
}
func (a *Service) Close() { a.wg.Wait() }
func ValidEmail(s string) bool {
	v, e := mail.ParseAddress(s)
	return e == nil && v.Address == s && len(s) <= 254
}

// JIT creates only the identity. Tenant membership is assigned by invitation,
// explicit provisioning or the personal-tenant provisioning path.
func (a *Service) JIT(ctx context.Context, issuer, subject, email, name string) (Principal, error) {
	p := Principal{ID: ids.New(), Kind: "user", Email: email, DisplayName: name}
	if subject == "" {
		return p, fmt.Errorf("empty subject")
	}
	if name == "" {
		p.DisplayName = email
	}
	err := a.DB.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO principals(id,kind,email,display_name,idp_subject) VALUES($1,'user',$2,$3,$4) ON CONFLICT(idp_subject) DO NOTHING RETURNING id::text,kind,email,display_name`, p.ID, email, p.DisplayName, issuer+"|"+subject).Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName)
	})
	if err == pgx.ErrNoRows {
		err = a.DB.Global(ctx, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, "SELECT id::text,kind,email,display_name FROM principals WHERE idp_subject=$1 AND status='active'", issuer+"|"+subject).Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName)
		})
	}
	return p, err
}
func (a *Service) Authenticate(ctx context.Context, token, tenant string) (Principal, error) {
	if strings.HasPrefix(token, "orbit_") {
		return a.apiKey(ctx, token, tenant)
	}
	if a.Config.AuthMode == "dev" {
		c, e := a.Signer.VerifyDev(token)
		if e != nil {
			return Principal{}, httpx.E(401, "UNAUTHENTICATED", "Invalid token")
		}
		var p Principal
		e = a.DB.Global(ctx, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, "SELECT id::text,kind,email,display_name FROM principals WHERE id=$1 AND status='active'", c.Subject).Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName)
		})
		return p, e
	}
	c, e := a.verifier.Verify(ctx, token)
	if e != nil {
		return Principal{}, httpx.E(401, "UNAUTHENTICATED", "Invalid token")
	}
	var fields struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	if e = c.Claims(&fields); e != nil {
		return Principal{}, e
	}
	return a.JIT(ctx, c.Issuer, c.Subject, fields.Email, fields.Name)
}
func GenerateAPIKey() (token, prefix string, hash []byte, err error) {
	b := make([]byte, 32)
	if _, err = rand.Read(b); err != nil {
		return
	}
	prefix = ids.New()[:8]
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
		return tx.QueryRow(ctx, `SELECT p.id::text,p.kind,p.email,p.display_name,k.scopes,k.id::text FROM api_keys k JOIN principals p ON p.id=k.principal_id JOIN memberships m ON m.principal_id=p.id AND m.tenant_id=k.tenant_id WHERE k.hash=$1 AND k.expires_at>now() AND p.status='active' AND m.status='active'`, h[:]).Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName, &p.Scopes, &keyID)
	})
	if e != nil {
		return p, httpx.E(401, "UNAUTHENTICATED", "Invalid API key")
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
				_, e := tx.Exec(c, "UPDATE api_keys SET last_used_at=now() WHERE id=$1 AND (last_used_at IS NULL OR last_used_at<now()-interval '1 minute')", keyID)
				return e
			})
			if e != nil {
				a.Logger.Warn("API key last-use update failed")
			}
		})
	}
	return p, nil
}

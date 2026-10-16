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
	// OnCreate provisions tenant membership/audit in the identity creation transaction.
	OnCreate   func(context.Context, pgx.Tx, Principal, string) error
	DB         *db.DB
	Signer     *Signer
	Capability *CapabilitySigner
	Stream     *StreamSigner
	Replay     httpx.ReplayKeys
	Config     config.Config
	verifier   *oidc.IDTokenVerifier
	Logger     *slog.Logger
	wg         sync.WaitGroup
	lastUsed   sync.Map
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

// JIT reads existing identities without writes. First creation and its optional
// personal tenant/audit commit atomically; concurrent first logins converge.
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
	err := a.DB.Global(ctx, lookup)
	if !errors.Is(err, pgx.ErrNoRows) {
		return p, err
	}
	tenant := ""
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
			return lookup(tx)
		}
		if err != nil {
			return err
		}
		if a.OnCreate != nil {
			return a.OnCreate(ctx, tx, p, tenant)
		}
		if tenant != "" {
			return fmt.Errorf("personal tenant provisioner required")
		}
		return nil
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

package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

const DevIssuer = "orbit:dev"
const CapabilityIssuer = "orbit:control"

type Signer struct{ key ed25519.PrivateKey }

func NewSigner(key ed25519.PrivateKey) *Signer { return &Signer{key: key} }
func LoadSigner(path string) (*Signer, error) {
	b, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		_, k, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			return nil, e
		}
		der, e := x509.MarshalPKCS8PrivateKey(k)
		if e != nil {
			return nil, e
		}
		if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			return nil, e
		}
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if os.IsExist(e) {
			return LoadSigner(path)
		}
		if e != nil {
			return nil, e
		}
		_, e = f.Write(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		ce := f.Close()
		if e != nil {
			return nil, e
		}
		if ce != nil {
			return nil, ce
		}
		return NewSigner(k), nil
	}
	if e != nil {
		return nil, fmt.Errorf("ORBIT_SIGNING_KEY_FILE cannot be read")
	}
	st, e := os.Stat(path)
	if e != nil {
		return nil, e
	}
	if st.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("ORBIT_SIGNING_KEY_FILE must have mode 0600")
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, fmt.Errorf("ORBIT_SIGNING_KEY_FILE invalid PEM")
	}
	k, e := x509.ParsePKCS8PrivateKey(block.Bytes)
	if e != nil {
		return nil, fmt.Errorf("ORBIT_SIGNING_KEY_FILE invalid key")
	}
	key, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("ORBIT_SIGNING_KEY_FILE must contain Ed25519")
	}
	return NewSigner(key), nil
}

type Claims struct {
	jwt.RegisteredClaims
	TenantID    string   `json:"tenant_id,omitempty"`
	Email       string   `json:"email,omitempty"`
	DisplayName string   `json:"display_name,omitempty"`
	Scopes      []string `json:"scopes,omitempty"`
}

func (s *Signer) sign(c Claims) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodEdDSA, c).SignedString(s.key)
}
func registered(iss, sub, aud string, ttl time.Duration) jwt.RegisteredClaims {
	n := time.Now()
	return jwt.RegisteredClaims{Issuer: iss, Subject: sub, Audience: jwt.ClaimStrings{aud}, IssuedAt: jwt.NewNumericDate(n), ExpiresAt: jwt.NewNumericDate(n.Add(ttl)), ID: ids.New()}
}
func (s *Signer) DevToken(p Principal) (string, error) {
	return s.sign(Claims{RegisteredClaims: registered(DevIssuer, p.ID, "orbit-api", time.Hour), Email: p.Email, DisplayName: p.DisplayName})
}
func (s *Signer) IssueCapability(tenantID, runID string, scopes []string) (string, error) {
	if !ids.Valid(tenantID) || !ids.Valid(runID) {
		return "", fmt.Errorf("invalid capability tenant or run")
	}
	return s.sign(Claims{RegisteredClaims: registered(CapabilityIssuer, "run:"+runID, "kernel", 5*time.Minute), TenantID: tenantID, Scopes: scopes})
}
func (s *Signer) verify(token, issuer, audience string) (Claims, error) {
	var c Claims
	_, e := jwt.ParseWithClaims(token, &c, func(t *jwt.Token) (any, error) { return s.key.Public(), nil }, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(issuer), jwt.WithAudience(audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt())
	if e != nil {
		return c, fmt.Errorf("invalid or expired token")
	}
	return c, nil
}
func (s *Signer) VerifyDev(token string) (Claims, error) {
	return s.verify(token, DevIssuer, "orbit-api")
}
func (s *Signer) VerifyCapability(token, tenantID, runID string) (Claims, error) {
	c, e := s.verify(token, CapabilityIssuer, "kernel")
	if e != nil {
		return c, e
	}
	if c.TenantID != tenantID || c.Subject != "run:"+runID || c.IssuedAt == nil || c.ExpiresAt.Sub(c.IssuedAt.Time) > 5*time.Minute {
		return c, fmt.Errorf("capability scope mismatch")
	}
	return c, nil
}

func (s *Signer) ReplayKey() []byte {
	h := sha256.Sum256(append([]byte("orbit:idempotency:v1:"), s.key.Seed()...))
	return h[:]
}

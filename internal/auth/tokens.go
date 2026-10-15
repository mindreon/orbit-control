package auth

import (
	"crypto/ed25519"
	"crypto/rand"
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
const CapabilityIssuer = "orbit-capability"

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

type DevClaims struct {
	jwt.RegisteredClaims
	Email       string `json:"email,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

func (s *Signer) sign(c DevClaims) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodEdDSA, c).SignedString(s.key)
}
func registered(iss, sub, aud string, ttl time.Duration) jwt.RegisteredClaims {
	n := time.Now()
	return jwt.RegisteredClaims{
		Issuer:    iss,
		Subject:   sub,
		Audience:  jwt.ClaimStrings{aud},
		IssuedAt:  jwt.NewNumericDate(n),
		ExpiresAt: jwt.NewNumericDate(n.Add(ttl)),
		ID:        ids.New(),
	}
}
func (s *Signer) DevToken(p Principal) (string, error) {
	return s.sign(
		DevClaims{
			RegisteredClaims: registered(DevIssuer, p.ID, "orbit-api", time.Hour),
			Email:            p.Email,
			DisplayName:      p.DisplayName,
		},
	)
}
func (s *Signer) verify(token, issuer, audience string) (DevClaims, error) {
	var c DevClaims
	_, e := jwt.ParseWithClaims(
		token,
		&c,
		func(t *jwt.Token) (any, error) { return s.key.Public(), nil },
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(issuer),
		jwt.WithAudience(audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
	)
	if e != nil {
		return c, fmt.Errorf("invalid or expired token")
	}
	return c, nil
}
func (s *Signer) VerifyDev(token string) (DevClaims, error) {
	return s.verify(token, DevIssuer, "orbit-api")
}

// CapabilityClaims carries segment or sandbox lease identity; callers compare it with current run
// state after verification. It is deliberately independent of public dev claims.
type CapabilityClaims struct {
	jwt.RegisteredClaims
	Tenant  string `json:"tenant_id"`
	Run     string `json:"run_id"`
	Member  string `json:"member,omitempty"`
	Seq     int    `json:"seq,omitempty"`
	Attempt int    `json:"attempt,omitempty"`
	Purpose string `json:"purpose"`
	LeaseID string `json:"lease,omitempty"`
}
type CapabilitySigner struct {
	key        ed25519.PrivateKey
	kid        string
	publicKeys map[string]ed25519.PublicKey
}

func NewCapabilitySigner(key ed25519.PrivateKey, kid string, publicKeys map[string][]byte) (*CapabilitySigner, error) {
	if len(key) != ed25519.PrivateKeySize || kid == "" {
		return nil, fmt.Errorf("invalid capability signing key or kid")
	}
	keys := map[string]ed25519.PublicKey{}
	for id, k := range publicKeys {
		if id == "" || len(k) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("invalid capability public key")
		}
		keys[id] = ed25519.PublicKey(k)
	}
	if previous, ok := keys[kid]; ok && !previous.Equal(key.Public()) {
		return nil, fmt.Errorf("capability kid conflicts with configured public key")
	}
	keys[kid] = key.Public().(ed25519.PublicKey)
	return &CapabilitySigner{key: key, kid: kid, publicKeys: keys}, nil
}
func (s *CapabilitySigner) IssueCapability(c CapabilityClaims) (string, error) {
	if !ids.Valid(c.Tenant) || !ids.Valid(c.Run) || c.Seq < 0 || c.Attempt < 0 ||
		(c.Purpose == "segment" && (c.Member == "" || c.Seq < 1 || c.Attempt < 1)) ||
		(c.Purpose != "segment" && c.Purpose != "sandbox") ||
		(c.Purpose == "sandbox" && c.LeaseID == "") {
		return "", fmt.Errorf("invalid capability identity")
	}
	c.RegisteredClaims = registered(CapabilityIssuer, "run:"+c.Run, "kernel", 5*time.Minute)
	c.NotBefore = c.IssuedAt
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
	token.Header["kid"] = s.kid
	return token.SignedString(s.key)
}

// VerifyCapability verifies cryptographic and time boundaries only. Run/segment
// status and authorization belong to the caller, never this offline verifier.
func (s *CapabilitySigner) VerifyCapability(token string) (CapabilityClaims, error) {
	var c CapabilityClaims
	_, err := jwt.ParseWithClaims(token, &c, func(t *jwt.Token) (any, error) {
		kid, ok := t.Header["kid"].(string)
		key, known := s.publicKeys[kid]
		if !ok || !known {
			return nil, fmt.Errorf("unknown capability kid")
		}
		return key, nil
	},
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(CapabilityIssuer),
		jwt.WithAudience("kernel"),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt())
	if err != nil || c.NotBefore == nil || c.Subject != "run:"+c.Run || !ids.Valid(c.Tenant) || !ids.Valid(c.Run) ||
		(c.Purpose != "segment" && c.Purpose != "sandbox") {
		return c, fmt.Errorf("invalid or expired capability")
	}
	return c, nil
}

type StreamClaims struct {
	jwt.RegisteredClaims
	TenantID  string `json:"tenant_id"`
	RunID     string `json:"run_id"`
	SessionID string `json:"session_id"`
}

func (s *Signer) IssueStream(tenant, run, session string) (string, error) {
	if !ids.Valid(tenant) || !ids.Valid(run) || !ids.Valid(session) {
		return "", fmt.Errorf("invalid stream identity")
	}
	return jwt.NewWithClaims(jwt.SigningMethodEdDSA,
		StreamClaims{RegisteredClaims: registered("orbit-stream", "run:"+run, "stream", 15*time.Minute),
			TenantID:  tenant,
			RunID:     run,
			SessionID: session}).
		SignedString(s.key)
}
func (s *Signer) VerifyStream(token string) (StreamClaims, error) {
	var c StreamClaims
	_, err := jwt.ParseWithClaims(
		token,
		&c,
		func(t *jwt.Token) (any, error) { return s.key.Public(), nil },
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer("orbit-stream"),
		jwt.WithAudience("stream"),
		jwt.WithExpirationRequired(),
	)
	return c, err
}

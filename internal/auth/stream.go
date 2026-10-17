package auth

import (
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

const StreamIssuer = "orbit-stream"

// StreamClaims carries the tenant, session and optional run identity for a stream subscription.
type StreamClaims struct {
	jwt.RegisteredClaims
	TenantID  string `json:"tenant_id"`
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id,omitempty"`
	Scope     string `json:"scope"`
}

// StreamVerifier verifies stream tokens offline using the configured rotation keys.
type StreamVerifier struct{ keyfunc jwt.Keyfunc }

// StreamSigner issues stream tokens with a dedicated Ed25519 key and key ID.
type StreamSigner struct {
	key ed25519.PrivateKey
	kid string
	*StreamVerifier
}

func NewStreamVerifier(publicKeys map[string][]byte) (*StreamVerifier, error) {
	keyfunc, err := kidKeyfunc(publicKeys, "stream")
	if err != nil {
		return nil, err
	}
	if len(publicKeys) == 0 {
		return nil, fmt.Errorf("stream public keys required")
	}
	return &StreamVerifier{keyfunc: keyfunc}, nil
}
func NewStreamSigner(key ed25519.PrivateKey, kid string, publicKeys map[string][]byte) (*StreamSigner, error) {
	keys, err := signingPublicKeys(key, kid, publicKeys, "stream")
	if err != nil {
		return nil, err
	}
	verifier, err := NewStreamVerifier(keys)
	if err != nil {
		return nil, err
	}
	return &StreamSigner{key: key, kid: kid, StreamVerifier: verifier}, nil
}
func streamSubject(c StreamClaims) (string, error) {
	if !ids.Valid(c.TenantID) || !ids.Valid(c.SessionID) {
		return "", fmt.Errorf("invalid stream identity")
	}
	switch c.Scope {
	case "run":
		if ids.Valid(c.RunID) {
			return "run:" + c.RunID, nil
		}
	case "session":
		if c.RunID == "" {
			return "session:" + c.SessionID, nil
		}
	}
	return "", fmt.Errorf("invalid stream scope or identity")
}
func (s *StreamSigner) Issue(c StreamClaims) (string, error) {
	return s.issue(c, 15*time.Minute)
}
func (s *StreamSigner) issue(c StreamClaims, ttl time.Duration) (string, error) {
	sub, err := streamSubject(c)
	if err != nil {
		return "", err
	}
	c.RegisteredClaims = registered(StreamIssuer, sub, "stream", ttl)
	c.NotBefore = c.IssuedAt
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
	token.Header["kid"] = s.kid
	return token.SignedString(s.key)
}
func (v *StreamVerifier) Verify(token string) (StreamClaims, error) {
	var c StreamClaims
	_, err := jwt.ParseWithClaims(token, &c, v.keyfunc, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(StreamIssuer),
		jwt.WithAudience("stream"), jwt.WithExpirationRequired(), jwt.WithLeeway(60*time.Second))
	sub, identityErr := streamSubject(c)
	if err != nil || identityErr != nil || c.Subject != sub || c.NotBefore == nil || c.IssuedAt == nil ||
		c.IssuedAt.Time.After(time.Now().Add(60*time.Second)) {
		return StreamClaims{}, fmt.Errorf("invalid or expired stream token")
	}
	return c, nil
}

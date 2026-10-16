package auth

import (
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

const StreamIssuer = "orbit-stream"

type StreamClaims struct {
	jwt.RegisteredClaims
	TenantID  string `json:"tenant_id"`
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id,omitempty"`
	Scope     string `json:"scope"`
}

type StreamVerifier struct{ publicKeys map[string]ed25519.PublicKey }
type StreamSigner struct {
	key ed25519.PrivateKey
	kid string
	*StreamVerifier
}

func NewStreamVerifier(publicKeys map[string][]byte) (*StreamVerifier, error) {
	keys := make(map[string]ed25519.PublicKey, len(publicKeys))
	for id, key := range publicKeys {
		if id == "" || len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("invalid stream public key")
		}
		keys[id] = append(ed25519.PublicKey(nil), key...)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("stream public keys required")
	}
	return &StreamVerifier{publicKeys: keys}, nil
}
func NewStreamSigner(key ed25519.PrivateKey, kid string, publicKeys map[string][]byte) (*StreamSigner, error) {
	if len(key) != ed25519.PrivateKeySize || kid == "" {
		return nil, fmt.Errorf("invalid stream signing key or kid")
	}
	keys := make(map[string][]byte, len(publicKeys)+1)
	for id, k := range publicKeys {
		keys[id] = k
	}
	pub := key.Public().(ed25519.PublicKey)
	if previous, ok := keys[kid]; ok && !pub.Equal(ed25519.PublicKey(previous)) {
		return nil, fmt.Errorf("stream kid conflicts with configured public key")
	}
	keys[kid] = pub
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
	sub, err := streamSubject(c)
	if err != nil {
		return "", err
	}
	c.RegisteredClaims = registered(StreamIssuer, sub, "stream", 15*time.Minute)
	c.NotBefore = c.IssuedAt
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
	token.Header["kid"] = s.kid
	return token.SignedString(s.key)
}
func (v *StreamVerifier) Verify(token string) (StreamClaims, error) {
	var c StreamClaims
	_, err := jwt.ParseWithClaims(token, &c, func(t *jwt.Token) (any, error) {
		kid, ok := t.Header["kid"].(string)
		key, known := v.publicKeys[kid]
		if !ok || !known {
			return nil, fmt.Errorf("unknown stream kid")
		}
		return key, nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(StreamIssuer),
		jwt.WithAudience("stream"), jwt.WithExpirationRequired())
	sub, identityErr := streamSubject(c)
	if err != nil || identityErr != nil || c.Subject != sub || c.NotBefore == nil || c.IssuedAt == nil ||
		c.IssuedAt.Time.After(time.Now().Add(60*time.Second)) {
		return StreamClaims{}, fmt.Errorf("invalid or expired stream token")
	}
	return c, nil
}

package auth

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func TestStreamSigner(t *testing.T) {
	old, next := testSigner(t), testSigner(t)
	signer, err := NewStreamSigner(old.key, "old", nil)
	require.NoError(t, err)
	rotated, err := NewStreamSigner(next.key, "next", map[string][]byte{"old": old.key.Public().(ed25519.PublicKey)})
	require.NoError(t, err)
	verifier, err := NewStreamVerifier(map[string][]byte{
		"old":  old.key.Public().(ed25519.PublicKey),
		"next": next.key.Public().(ed25519.PublicKey),
	})
	require.NoError(t, err)
	for _, scope := range []string{"run", "session"} {
		c := StreamClaims{TenantID: ids.New(), SessionID: ids.New(), Scope: scope}
		if scope == "run" {
			c.RunID = ids.New()
		}
		token, err := signer.Issue(c)
		require.NoError(t, err)
		got, err := verifier.Verify(token)
		require.NoError(t, err)
		require.Equal(t, c.TenantID, got.TenantID)
		require.Equal(t, c.SessionID, got.SessionID)
		require.Equal(t, c.RunID, got.RunID)
		require.Equal(t, scope, got.Scope)
		require.Equal(t, 15*time.Minute, got.ExpiresAt.Sub(got.IssuedAt.Time))
		require.Equal(t, got.IssuedAt, got.NotBefore)
		require.NotEmpty(t, got.ID)
		_, err = rotated.Verify(token)
		require.NoError(t, err)
		fresh, err := rotated.Issue(c)
		require.NoError(t, err)
		_, err = verifier.Verify(fresh)
		require.NoError(t, err)
		_, err = signer.Verify(fresh)
		require.Error(t, err)
		_, err = old.VerifyDev(token)
		require.Error(t, err)
	}
	_, err = NewStreamSigner(next.key, "old", map[string][]byte{"old": old.key.Public().(ed25519.PublicKey)})
	require.Error(t, err)
	c := StreamClaims{TenantID: ids.New(), SessionID: ids.New(), RunID: ids.New(), Scope: "run"}
	token, err := signer.Issue(c)
	require.NoError(t, err)
	valid, err := signer.Verify(token)
	require.NoError(t, err)
	for _, name := range []string{
		"expired", "missing-exp", "future-nbf", "near-future-nbf", "missing-nbf", "future-iat", "missing-iat", "issuer",
		"audience", "subject", "scope", "tenant", "session", "run", "signature", "kid", "algorithm",
	} {
		t.Run(name, func(t *testing.T) {
			bad := valid
			key, kid := old.key, "old"
			method := jwt.SigningMethod(jwt.SigningMethodEdDSA)
			switch name {
			case "expired":
				bad.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-2 * time.Minute))
			case "missing-exp":
				bad.ExpiresAt = nil
			case "future-nbf":
				bad.NotBefore = jwt.NewNumericDate(time.Now().Add(2 * time.Minute))
			case "near-future-nbf":
				bad.NotBefore = jwt.NewNumericDate(time.Now().Add(30 * time.Second))
			case "missing-nbf":
				bad.NotBefore = nil
			case "future-iat":
				bad.IssuedAt = jwt.NewNumericDate(time.Now().Add(2 * time.Minute))
			case "missing-iat":
				bad.IssuedAt = nil
			case "issuer":
				bad.Issuer = DevIssuer
			case "audience":
				bad.Audience = jwt.ClaimStrings{"kernel"}
			case "subject":
				bad.Subject = "session:" + bad.SessionID
			case "scope":
				bad.Scope = "all"
			case "tenant":
				bad.TenantID = "invalid"
			case "session":
				bad.SessionID = "invalid"
			case "run":
				bad.RunID = ""
			case "signature":
				key = next.key
			case "kid":
				kid = ""
			case "algorithm":
				method = jwt.SigningMethodHS256
			}
			signed := jwt.NewWithClaims(method, bad)
			signed.Header["kid"] = kid
			var signingKey any = key
			if name == "algorithm" {
				signingKey = []byte("test-key")
			}
			token, err := signed.SignedString(signingKey)
			require.NoError(t, err)
			_, err = verifier.Verify(token)
			if name == "near-future-nbf" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

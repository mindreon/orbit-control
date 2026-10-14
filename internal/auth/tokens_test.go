package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func testSigner(t *testing.T) *Signer {
	t.Helper()
	_, k, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	return NewSigner(k)
}
func TestTokenBoundaries(t *testing.T) {
	s := testSigner(t)
	tenant, run := ids.New(), ids.New()
	token, e := s.IssueCapability(tenant, run, []string{"secret.read"})
	if e != nil {
		t.Fatal(e)
	}
	c, e := s.VerifyCapability(token, tenant, run)
	if e != nil || c.Subject != "run:"+run {
		t.Fatal(c, e)
	}
	for _, v := range []struct{ name, tenant, run string }{{"tenant", ids.New(), run}, {"run", tenant, ids.New()}} {
		t.Run(v.name, func(t *testing.T) {
			if _, e = s.VerifyCapability(token, v.tenant, v.run); e == nil {
				t.Fatal("accepted wrong scope")
			}
		})
	}
	if _, e = s.VerifyDev(token); e == nil {
		t.Fatal("kernel token accepted by public API")
	}
	for _, test := range []string{"expired", "audience", "issuer", "duration", "future", "signature"} {
		t.Run(test, func(t *testing.T) {
			c := Claims{RegisteredClaims: registered(CapabilityIssuer, "run:"+run, "kernel", 5*time.Minute), TenantID: tenant}
			switch test {
			case "expired":
				c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
			case "audience":
				c.Audience = jwt.ClaimStrings{"orbit-api"}
			case "issuer":
				c.Issuer = "attacker"
			case "duration":
				c.ExpiresAt = jwt.NewNumericDate(c.IssuedAt.Add(6 * time.Minute))
			case "future":
				c.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Hour))
			case "signature":
				s = testSigner(t)
			}
			token, e := s.sign(c)
			if e != nil {
				t.Fatal(e)
			}
			verifier := s
			if test == "signature" {
				verifier = testSigner(t)
			}
			if _, e = verifier.VerifyCapability(token, tenant, run); e == nil {
				t.Fatal("accepted invalid token")
			}
		})
	}
}
func TestDevSigningKeyPersistsAndRestricted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "signing.key")
	s, e := LoadSigner(path)
	if e != nil {
		t.Fatal(e)
	}
	token, e := s.DevToken(Principal{ID: ids.New(), Email: "dev@example.test"})
	if e != nil {
		t.Fatal(e)
	}
	s2, e := LoadSigner(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s2.VerifyDev(token); e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(path, 0644); e != nil {
		t.Fatal(e)
	}
	if _, e = LoadSigner(path); e == nil {
		t.Fatal("world-readable key accepted")
	}
}
func TestAPIKeyHash(t *testing.T) {
	token, prefix, hash, e := GenerateAPIKey()
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasPrefix(token, "orbit_"+prefix+"_") {
		t.Fatal("format")
	}
	expected := sha256.Sum256([]byte(token))
	if string(hash) != string(expected[:]) {
		t.Fatal("wrong hash")
	}
	other, _, _, e := GenerateAPIKey()
	if e != nil || other == token {
		t.Fatal("not random")
	}
}
